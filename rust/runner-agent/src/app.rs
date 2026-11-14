use crate::{
    args,
    infrastructure::{Infrastructure, PublicTunnels},
    link::ControlLink,
    logs::Logs,
    metrics::{CopyIdentity, Metrics},
    runtime::{Leadership, PlanAuthority, container_health, fingerprint, name_valid},
    state::Context,
    update,
    workloads::{starting_project, strings},
};
use anyhow::ensure;
use runner_core::{
    fs::{atomic_write, private_directory},
    now_ms, response_json,
};
use serde_json::{Value, json};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{sync::Mutex, task::JoinSet};
use tokio_util::sync::CancellationToken;

pub async fn run(cancel: CancellationToken) -> anyhow::Result<()> {
    let context = Context::load()?;
    private_directory(&context.host.base.join("projects"))?;
    private_directory(&context.host.base.join("deployments"))?;
    join(&context, cancel.clone()).await?;
    let infrastructure = loop {
        if cancel.is_cancelled() {
            return Ok(());
        }
        match Infrastructure::select(context.host.clone()).await {
            Ok(mut infrastructure) => {
                let state = context.state.read().await;
                let projects = state.projects.values().cloned().collect();
                let domain = state.s("domain").to_owned();
                let machine = state.n("machine");
                drop(state);
                match infrastructure.router(projects, &domain, machine).await {
                    Ok(()) => break infrastructure,
                    Err(_) => eprintln!("router unavailable; retrying"),
                }
            }
            Err(_) => eprintln!("Docker infrastructure unavailable; retrying"),
        }
        tokio::select! { _=cancel.cancelled()=>return Ok(()),_=tokio::time::sleep(Duration::from_secs(30))=>() }
    };
    let ready = infrastructure.tunnel_running().await?;
    {
        let mut state = context.state.write().await;
        state.ready = ready;
        state.public_open = ready && state.b("wasReady");
        if ready {
            state.set("wasReady", true);
        }
        state.system.insert(infrastructure.router.clone());
        state.system.insert(infrastructure.tunnel.clone());
    }
    let infrastructure = Arc::new(Mutex::new(infrastructure));
    let public = Arc::new(Mutex::new(PublicTunnels::new(context.host.clone())));
    let mut tasks = JoinSet::new();
    tasks.spawn(reconcile(context.clone(), cancel.clone()));
    tasks.spawn(health(
        context.clone(),
        infrastructure.clone(),
        cancel.clone(),
    ));
    tasks.spawn(sampling(context.clone(), cancel.clone()));
    tasks.spawn(public_loop(context.clone(), public.clone(), cancel.clone()));
    let controller = controller(context.clone(), infrastructure.clone(), cancel.clone());
    let result = tokio::select! {
        _=cancel.cancelled()=>Ok(true),
        result=controller=>result,
        result=tasks.join_next()=>match result { Some(Ok(Err(error)))=>Err(error),_ if cancel.is_cancelled()=>Ok(true),_=>Err(anyhow::anyhow!("agent background task stopped")) },
    };
    // A normal process restart preserves all serving connectors and containers.
    // Only an explicit retirement plan withdraws traffic from this incarnation.
    cancel.cancel();
    tasks.abort_all();
    while tasks.join_next().await.is_some() {}
    let _ = context.save().await;
    if matches!(result, Ok(false)) {
        {
            context.state.write().await.leaving = true;
        }
        let _ = public
            .lock()
            .await
            .reconcile(&json!({}), &[], "", false)
            .await;
        let _ = infrastructure.lock().await.withdraw_machine().await;
    }
    result.map(|_| ())
}

async fn join(context: &Context, cancel: CancellationToken) -> anyhow::Result<()> {
    {
        let state = context.state.read().await;
        if state.n("machine") > 0
            && !state.s("tunnelToken").is_empty()
            && !state.s("agent").is_empty()
            && !state.s("run").is_empty()
        {
            return Ok(());
        }
    }
    let mut agent = context
        .environment
        .get("AGENT_ID")
        .cloned()
        .unwrap_or_default();
    if agent.is_empty() {
        agent = std::fs::read_to_string(context.host.base.join("agent-id"))
            .unwrap_or_default()
            .trim()
            .into();
        if agent.is_empty() {
            agent = format!("host-{}", &runner_core::random_id()[..8]);
            atomic_write(&context.host.base.join("agent-id"), agent.as_bytes(), 0o600)?;
        }
    }
    {
        let mut state = context.state.write().await;
        state.set("agent", agent);
        if state.s("run").is_empty() {
            let run = format!("{}-{}", state.s("agent"), state.n("started"));
            state.set("run", run);
        }
    }
    let mut want = context
        .environment
        .get("MACHINE")
        .and_then(|s| s.parse::<u64>().ok());
    let start = Instant::now();
    let mut delay = 5;
    loop {
        let body = {
            let state = context.state.read().await;
            let mut body = state.describe(&context.environment);
            body["agent"] = json!(state.s("agent"));
            if let Some(want) = want {
                body["want"] = json!(want);
            }
            body
        };
        let attempt = async {
            let response = context
                .host
                .client
                .post(format!("{}/api/join", context.control))
                .bearer_auth(&context.token)
                .json(&body)
                .send()
                .await?;
            let code = response.status().as_u16();
            if matches!(code, 401 | 403) {
                return Ok::<_, anyhow::Error>((code, Value::Null));
            }
            if code == 409 && want.is_some() {
                return Ok((code, Value::Null));
            }
            Ok((code, response_json::<Value>(response, 1 << 20).await?))
        };
        let result = tokio::select! { _=cancel.cancelled()=>return Ok(()),result=attempt=>result };
        match result {
            Ok((401 | 403, _)) => anyhow::bail!("the join token was refused"),
            Ok((409, _)) if want.is_some() => {
                want = None;
                continue;
            }
            Ok((_, body)) => {
                ensure!(
                    body["machine"].as_u64().is_some_and(|n| n > 0)
                        && body["tunnelToken"].as_str().is_some_and(|s| !s.is_empty())
                        && body["domain"].as_str().is_some_and(|s| !s.is_empty()),
                    "invalid controller join response"
                );
                {
                    let mut state = context.state.write().await;
                    for key in ["machine", "tunnelToken", "domain"] {
                        state.set(key, body[key].clone());
                    }
                }
                context.save().await?;
                return Ok(());
            }
            Err(_) => (),
        }
        ensure!(
            context.state.read().await.s("tag") == "standalone"
                || start.elapsed() < Duration::from_secs(600),
            "could not join for 10 minutes"
        );
        eprintln!("controller unavailable; retrying join in {delay}s");
        tokio::select! { _=cancel.cancelled()=>return Ok(()),_=tokio::time::sleep(Duration::from_secs(delay))=>() }
        delay = (delay * 2).min(60);
    }
}

async fn reconcile(context: Context, cancel: CancellationToken) -> anyhow::Result<()> {
    let mut jobs: JoinSet<anyhow::Result<()>> = JoinSet::new();
    let mut timer = tokio::time::interval(Duration::from_secs(2));
    timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! { _=cancel.cancelled()=>break,_=timer.tick()=>(),_=context.changed.notified()=>(),result=jobs.join_next(),if !jobs.is_empty()=>{
            if let Some(result)=result { result??; }
        } }
        let mut state = context.state.write().await;
        if state.leaving {
            continue;
        }
        let now = now_ms();
        let accepted = state.accepted;
        let desired = state.value("desired");
        let obsolete: Vec<_> = if accepted {
            state
                .projects
                .iter()
                .filter(|(key, p)| {
                    desired.get(*key).is_none()
                        && p["busy"] != true
                        && p["removeRetryAt"].as_u64().unwrap_or(0) <= now
                })
                .map(|(key, _)| key.clone())
                .collect()
        } else {
            Vec::new()
        };
        for key in obsolete {
            if jobs.len() >= 3 {
                break;
            }
            let before = state.projects[&key].clone();
            state.projects.get_mut(&key).unwrap()["busy"] = json!(true);
            let context = context.clone();
            jobs.spawn(async move {
                let result = context.host.remove(&key, &before).await;
                {
                    let mut state = context.state.write().await;
                    if result.is_ok() {
                        state.projects.remove(&key);
                    } else if let Some(p) = state.projects.get_mut(&key) {
                        p["busy"] = json!(false);
                        p["removeRetryAt"] = json!(now_ms() + 30_000);
                    }
                }
                context.save().await?;
                context.notify();
                Ok(())
            });
        }
        let removals = state
            .projects
            .keys()
            .any(|key| state.value("desired").get(key).is_none());
        let keys: Vec<String> = state
            .value("desired")
            .as_object()
            .into_iter()
            .flat_map(|d| d.keys().cloned())
            .collect();
        for key in keys {
            if jobs.len() >= 3 {
                break;
            }
            let want = &state.value("desired")[&key];
            let before = state.projects.get(&key).cloned().unwrap_or(Value::Null);
            if before["busy"] == true || (before.is_null() && removals) {
                continue;
            }
            let fingerprint = fingerprint(want)?;
            if !accepted && (before.is_null() || before["fingerprint"] != fingerprint) {
                continue;
            }
            let retry = before["s"] == "failed"
                && now.saturating_sub(before["at"].as_u64().unwrap_or(0))
                    > if state.ready { 180_000 } else { 30_000 };
            let changed = before["fingerprint"] != fingerprint
                || before["replica"].as_u64().unwrap_or(1) != want["replica"].as_u64().unwrap_or(1)
                || before["app"].as_str().unwrap_or(&key) != want["app"].as_str().unwrap_or(&key);
            if !before.is_null() && !changed && !retry {
                let v = want["v"].clone();
                let revision = want["revision"].clone();
                let services = want["services"].clone();
                let oneshot = want["oneshotServices"].clone();
                let p = state.projects.get_mut(&key).unwrap();
                p["v"] = v;
                if !revision.is_null() {
                    p["revision"] = revision;
                }
                if !services.is_null() {
                    p["services"] = services;
                }
                if !oneshot.is_null() {
                    p["oneshotServices"] = oneshot;
                }
                continue;
            }
            let want = want.clone();
            let recreate = retry && before["v"] == want["v"];
            let project = starting_project(&key, &want, &before, recreate)?;
            state.projects.insert(key.clone(), project.clone());
            let domain = state.s("domain").to_owned();
            context.route_changed.notify_one();
            let context = context.clone();
            jobs.spawn(async move {
                let result = context
                    .host
                    .apply(&key, &want, &before, project, &domain, recreate)
                    .await;
                context.state.write().await.projects.insert(key, result);
                context.save().await?;
                context.notify();
                Ok(())
            });
        }
    }
    jobs.abort_all();
    while jobs.join_next().await.is_some() {}
    Ok(())
}

async fn sampling(context: Context, cancel: CancellationToken) -> anyhow::Result<()> {
    let mut timer = tokio::time::interval(Duration::from_secs(10));
    timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {_=cancel.cancelled()=>return Ok(()),_=timer.tick()=>()}
        let (client, projects, system) = {
            let state = context.state.read().await;
            (
                state.metrics.client(),
                state
                    .projects
                    .iter()
                    .map(|(key, p)| {
                        (
                            p["composeProject"].as_str().unwrap_or(key).to_owned(),
                            CopyIdentity {
                                key: key.clone(),
                                app: p["app"].as_str().unwrap_or(key).into(),
                                replica: p["replica"].as_u64().unwrap_or(1),
                            },
                        )
                    })
                    .collect(),
                state.system.clone(),
            )
        };
        let observation = tokio::select! {_=cancel.cancelled()=>return Ok(()),observation=Metrics::collect(&client,&projects,&system)=>observation};
        if let Ok(observation) = observation {
            context.state.write().await.metrics.ingest(observation)?;
        }
    }
}
async fn public_loop(
    context: Context,
    public: Arc<Mutex<PublicTunnels>>,
    cancel: CancellationToken,
) -> anyhow::Result<()> {
    let mut timer = tokio::time::interval(Duration::from_secs(2));
    timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {_=cancel.cancelled()=>return Ok(()),_=timer.tick()=>()}
        let (plan, projects, domain, open) = {
            let state = context.state.read().await;
            (
                state.value("publicTunnels").clone(),
                state.projects.values().cloned().collect::<Vec<_>>(),
                state.s("domain").to_owned(),
                state.public_open && !state.leaving,
            )
        };
        let mut public = public.lock().await;
        if public
            .reconcile(&plan, &projects, &domain, open)
            .await
            .is_err()
        {
            continue;
        }
        let mut state = context.state.write().await;
        let active = serde_json::to_value(&public.active)?;
        if state.public_active != active {
            state.public_active = active;
            context.wake.notify_one();
        }
        let prefix = format!("{}-public-", context.host.owner);
        state.system.retain(|name| !name.starts_with(&prefix));
        state.system.extend(public.system.iter().cloned());
    }
}
async fn health(
    context: Context,
    infrastructure: Arc<Mutex<Infrastructure>>,
    cancel: CancellationToken,
) -> anyhow::Result<()> {
    let mut timer = tokio::time::interval(Duration::from_secs(10));
    timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut last_health = Instant::now() - Duration::from_secs(10);
    let mut tunnel_retry = 0;
    loop {
        tokio::select! {_=cancel.cancelled()=>return Ok(()),_=timer.tick()=>(),_=context.route_changed.notified()=>()}
        let (projects, domain, machine, token, was_ready, open) = {
            let state = context.state.read().await;
            let serves = |key: &str| {
                state
                    .projects
                    .get(key)
                    .is_some_and(|p| p["s"] == "healthy" && p["busy"] != true && p["r"] != false)
            };
            let plan = &state.plan;
            let open = state.accepted
                && plan["hold"] != true
                && if plan["successor"] == true {
                    plan["serve"].as_array().is_some_and(|list| {
                        list.iter().all(|v| v.as_str().is_some_and(serves))
                            && (!list.is_empty() || state.settled(state.value("desired")))
                    })
                } else {
                    state.settled(state.value("desired")) || now_ms() > state.n("started") + 600_000
                };
            (
                state.projects.clone(),
                state.s("domain").to_owned(),
                state.n("machine"),
                state.s("tunnelToken").to_owned(),
                state.b("wasReady"),
                open,
            )
        };
        {
            let mut infrastructure = infrastructure.lock().await;
            if infrastructure
                .router(projects.values().cloned().collect(), &domain, machine)
                .await
                .is_err()
            {
                continue;
            }
            let running = infrastructure.tunnel_running().await.unwrap_or(false);
            context.state.write().await.ready = running;
            if open {
                context.state.write().await.public_open = true;
            }
            if !running && (was_ready || open) && now_ms() >= tunnel_retry {
                tunnel_retry = now_ms() + 60_000;
                if infrastructure.start_tunnel(&token).await.is_ok() {
                    let mut state = context.state.write().await;
                    state.ready = true;
                    state.set("wasReady", true);
                    context.wake.notify_one();
                }
            }
        }
        if last_health.elapsed() < Duration::from_secs(10) {
            continue;
        }
        last_health = Instant::now();
        let mut changed = false;
        for (key, p) in projects {
            if p["busy"] == true || p["s"] != "healthy" {
                continue;
            }
            let issue = match context.host.owned(&key, &p).await {
                Ok(containers) => container_health(
                    &containers,
                    &strings(&p["services"]),
                    &strings(&p["oneshotServices"]),
                ),
                Err(_) => Some("Docker state could not be verified"),
            };
            let answers = issue.is_none()
                && (p["port"].as_u64().unwrap_or(0) == 0
                    || context.host.answers(&p, &domain, Duration::ZERO).await);
            let ready = if answers {
                context.host.ready(&p, &domain).await
            } else {
                None
            };
            let mut state = context.state.write().await;
            let Some(current) = state.projects.get_mut(&key).filter(|current| {
                current["busy"] != true && current["fingerprint"] == p["fingerprint"]
            }) else {
                continue;
            };
            if answers {
                current["misses"] = json!(0);
                if let Some(ready) = ready {
                    changed |= current["r"] != ready;
                    current["r"] = json!(ready);
                }
            } else {
                let misses = current["misses"].as_u64().unwrap_or(0) + 1;
                current["misses"] = json!(misses);
                if misses >= 3 {
                    current["s"] = json!("failed");
                    current["e"] = json!(issue.unwrap_or("application stopped answering"));
                    current["at"] = json!(0);
                    current["misses"] = json!(0);
                    changed = true;
                }
            }
        }
        if changed {
            context.save().await?;
            context.notify();
        }
    }
}

async fn controller(
    context: Context,
    _infrastructure: Arc<Mutex<Infrastructure>>,
    cancel: CancellationToken,
) -> anyhow::Result<bool> {
    let current = context.state.read().await.value("leadership").clone();
    let current: Option<Leadership> = serde_json::from_value(current)?;
    let mut authority = PlanAuthority::new(
        context.control.clone(),
        context.token.clone(),
        current,
        context.host.client.clone(),
    );
    let link = ControlLink::start(
        context.control.clone(),
        context.token.clone(),
        context.wake.clone(),
        cancel.clone(),
    );
    let mut logs = Logs::new(context.clone(), cancel.clone());
    let mut cleaned = false;
    let mut successor_at = 0;
    let mut upgrade_at = 0;
    let mut start_job: Option<tokio::task::JoinHandle<()>> = None;
    loop {
        if cancel.is_cancelled() {
            return Ok(true);
        }
        let body = Arc::new(report(&context).await);
        let started = Instant::now();
        let attempt = async {
            let plan = match link.report(body.clone()).await? {
                Some(plan) => plan,
                None => {
                    response_json(
                        context
                            .host
                            .client
                            .post(format!("{}/api/sync", context.control))
                            .bearer_auth(&context.token)
                            .timeout(Duration::from_secs(15))
                            .json(&body)
                            .send()
                            .await?,
                        16_000_000,
                    )
                    .await?
                }
            };
            authority.accept(&plan).await?;
            Ok::<_, anyhow::Error>(plan)
        };
        let result = tokio::select! {_=cancel.cancelled()=>return Ok(true),result=attempt=>result};
        let mut poll = 20.0;
        if let Ok(mut plan) = result {
            {
                let mut state = context.state.write().await;
                state.rtt = Some(started.elapsed().as_millis() as u64);
                state.metrics.confirm(&body["metrics"]);
            }
            if plan["retire"] == true && plan["hold"] != true {
                return Ok(false);
            }
            if plan["hold"] != true {
                ensure!(
                    plan["desired"]
                        .as_object()
                        .is_some_and(|m| m.iter().all(|(k, v)| name_valid(k, 71) && v.is_object())),
                    "controller sent invalid desired state"
                );
            }
            context.accept(&mut plan).await?;
            if plan["hold"] != true {
                logs.reconcile(&plan["logs"]).await;
                if !cleaned {
                    cleaned = true;
                    if let Ok(entries) = std::fs::read_dir(context.host.base.join("projects")) {
                        let mut state = context.state.write().await;
                        for entry in entries.flatten() {
                            let key = entry.file_name().to_string_lossy().to_string();
                            if name_valid(&key, 71)
                                && state.value("desired").get(&key).is_none()
                                && !state.projects.contains_key(&key)
                            {
                                state.projects.insert(
                                    key.clone(),
                                    json!({"app":key,"s":"leftover","busy":false}),
                                );
                            }
                        }
                    }
                    context.changed.notify_one();
                }
                if plan["handover"] == true {
                    if let Some(command) = context
                        .environment
                        .get("START_CMD")
                        .filter(|s| !s.is_empty())
                    {
                        if now_ms().saturating_sub(successor_at) > 600_000
                            && start_job.as_ref().is_none_or(|j| j.is_finished())
                        {
                            successor_at = now_ms();
                            let context = context.clone();
                            let command = command.clone();
                            start_job = Some(tokio::spawn(async move {
                                let slot = context.state.read().await.n("machine");
                                let mut extra = (*context.environment).clone();
                                extra.insert("SLOT".into(), slot.to_string());
                                let _ = context
                                    .host
                                    .commands
                                    .run(
                                        "sh",
                                        &args!["-c", command],
                                        &extra,
                                        Duration::from_secs(120),
                                        false,
                                    )
                                    .await;
                            }));
                        }
                    } else if now_ms().saturating_sub(upgrade_at) >= 60_000 {
                        upgrade_at = now_ms();
                        let result = if context
                            .environment
                            .get("RUNNER_CODE_MANAGED")
                            .is_some_and(|s| s == "1")
                        {
                            update::install(
                                &context.host.base,
                                &context.control,
                                &context.host.client,
                            )
                            .await
                            .map(|_| ())
                        } else {
                            Ok(())
                        };
                        if result.is_ok() {
                            context.state.write().await.set("restartRequested", true);
                            context.save().await?;
                            return Ok(true);
                        }
                        eprintln!(
                            "agent upgrade unavailable; preserving the current agent and applications"
                        );
                    }
                }
            }
            poll = plan["poll"].as_f64().unwrap_or(20.0).clamp(0.05, 300.0);
        } else {
            eprintln!("controller unavailable; keeping authorized workloads running");
        }
        context.save().await?;
        tokio::select! {_=cancel.cancelled()=>return Ok(true),_=context.wake.notified()=>(),_=tokio::time::sleep(Duration::from_secs_f64(poll))=>()}
    }
}
async fn report(context: &Context) -> Value {
    let edges = if context.state.read().await.ready {
        context.host.tunnel_edges().await
    } else {
        None
    };
    let state = context.state.read().await;
    let mut body = state.describe(&context.environment);
    for key in [
        "machine",
        "run",
        "previousRun",
        "agent",
        "started",
        "recovery",
    ] {
        body[key] = state.value(key).clone();
    }
    body["ready"] = json!(state.ready);
    body["leaving"] = json!(state.leaving);
    body["protocol"] = json!(5);
    body["metrics"] = state.metrics.payload();
    body["rtt"] = json!(state.rtt);
    body["edges"] = json!(edges);
    body["disk"] = disk_space(&context.host.base);
    body["publicTunnels"] = state.public_active.clone();
    body["status"] = Value::Object(
        state
            .projects
            .iter()
            .map(|(key, p)| {
                let mut status = serde_json::Map::new();
                for field in ["v", "revision", "s", "e", "r", "t0", "n"] {
                    if let Some(value) = p.get(field).filter(|v| !v.is_null()) {
                        status.insert(field.into(), value.clone());
                    }
                }
                (key.clone(), Value::Object(status))
            })
            .collect(),
    );
    body["inventory"]=Value::Object(state.projects.iter().map(|(key,p)|(key.clone(),json!({"app":p["app"],"replica":p["replica"],"v":p["v"],"revision":p["revision"],"ports":state.value("desired")[key]["ports"]}))).collect());
    body
}
fn disk_space(path: &std::path::Path) -> Value {
    use std::os::unix::ffi::OsStrExt;
    let Ok(path) = std::ffi::CString::new(path.as_os_str().as_bytes()) else {
        return Value::Null;
    };
    let mut stat = std::mem::MaybeUninit::<libc::statvfs>::uninit();
    if unsafe { libc::statvfs(path.as_ptr(), stat.as_mut_ptr()) } != 0 {
        return Value::Null;
    }
    let stat = unsafe { stat.assume_init() };
    let size = stat.f_frsize as u64;
    json!({"free":(stat.f_bavail as u64).saturating_mul(size),"total":(stat.f_blocks as u64).saturating_mul(size)})
}
