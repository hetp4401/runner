use anyhow::ensure;
use runner_core::auth::sha256;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    path::{Component, Path, PathBuf},
};

pub const OWNER_LABEL: &str = "io.runners.owner";
pub const COPY_LABEL: &str = "io.runners.copy";
pub fn name_valid(name: &str, limit: usize) -> bool {
    !name.is_empty()
        && name.len() <= limit
        && name.as_bytes()[0].is_ascii_alphanumeric()
        && name
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
}
pub fn namespace(control: &str) -> anyhow::Result<String> {
    let url = url::Url::parse(control)?;
    ensure!(
        matches!(url.scheme(), "http" | "https") && url.host_str().is_some(),
        "invalid control URL"
    );
    Ok(format!(
        "runner-{}",
        &sha256(url.origin().ascii_serialization())[..12]
    ))
}
pub fn project_name(owner: &str, key: &str) -> anyhow::Result<String> {
    ensure!(name_valid(key, 71), "invalid application copy name");
    Ok(format!("{owner}-{key}"))
}
pub fn server_tag(env: &BTreeMap<String, String>, saved: &Value) -> anyhow::Result<String> {
    let tag = env
        .get("SERVER_TAG")
        .or_else(|| env.get("POOL"))
        .map(String::as_str)
        .or_else(|| saved["tag"].as_str())
        .or_else(|| saved["pool"].as_str())
        .or_else(|| {
            (saved["run"].as_str().is_some()
                && saved["control"].as_str() == env.get("CONTROL_URL").map(String::as_str))
            .then_some("standalone")
        });
    let tag = tag.unwrap_or("");
    ensure!(
        name_valid(tag, 30),
        "SERVER_TAG is required: use 1-30 lowercase letters, digits or dashes, or standalone"
    );
    Ok(tag.into())
}

// Match Node's localeCompare for the restricted ASCII deployment filenames.
// Keeping the fingerprint prevents a language migration rebuilding every app.
fn filename_cmp(a: &str, b: &str) -> std::cmp::Ordering {
    const ORDER: &[u8] = b"_-.@/+0123456789abcdefghijklmnopqrstuvwxyz";
    let rank = |byte: u8| {
        ORDER
            .iter()
            .position(|x| *x == byte.to_ascii_lowercase())
            .unwrap_or(255)
    };
    let primary = a.bytes().map(rank).cmp(b.bytes().map(rank));
    primary.then_with(|| {
        a.bytes()
            .map(|x| x.is_ascii_uppercase())
            .cmp(b.bytes().map(|x| x.is_ascii_uppercase()))
    })
}
pub fn fingerprint(want: &Value) -> anyhow::Result<String> {
    let mut files: Vec<_> = want["files"]
        .as_object()
        .into_iter()
        .flatten()
        .map(|(name, value)| (name, value))
        .collect();
    files.sort_by(|a, b| filename_cmp(a.0, b.0));
    let mut data = serde_json::Map::new();
    // JSON.stringify omits undefined properties but retains explicit nulls.
    if let Some(app) = want.get("app") {
        data.insert("app".into(), app.clone());
    }
    data.insert(
        "replica".into(),
        want.get("replica")
            .filter(|v| !v.is_null())
            .cloned()
            .unwrap_or(json!(1)),
    );
    for key in ["port", "ready", "incarnation"] {
        data.insert(key.into(), want.get(key).cloned().unwrap_or(Value::Null));
    }
    if let Some(compose) = want.get("compose") {
        data.insert("compose".into(), compose.clone());
    }
    data.insert("files".into(), serde_json::to_value(files)?);
    Ok(sha256(serde_json::to_vec(&data)?))
}

// A controller language change may only change YAML presentation. Adopt the
// new digest only after comparing all effective deployment inputs, including
// the managed Docker document. This never adopts an unverified old inventory.
pub fn equivalent_plan(before: &Value, next: &Value) -> bool {
    if !before.is_object() || !next.is_object() {
        return false;
    }
    for key in ["app", "port", "ready", "incarnation", "files"] {
        if before[key] != next[key] {
            return false;
        }
    }
    if before["replica"].as_u64().unwrap_or(1) != next["replica"].as_u64().unwrap_or(1) {
        return false;
    }
    let document = |v: &Value| -> Option<serde_yaml::Value> {
        let text = v.as_str()?;
        if text.len() > 1_000_000 {
            return None;
        }
        let mut parsed = runner_core::yaml::parse(text).ok()?;
        parsed.apply_merge().ok()?;
        Some(parsed)
    };
    let Some(old) = document(&before["compose"]) else {
        return false;
    };
    let Some(new) = document(&next["compose"]) else {
        return false;
    };
    if old != new {
        return false;
    }
    let json = |v: &Value| {
        v.as_str()
            .and_then(|s| serde_json::from_str::<Value>(s).ok())
    };
    let Some(old) = json(&before["managedCompose"]) else {
        return false;
    };
    let Some(new) = json(&next["managedCompose"]) else {
        return false;
    };
    old == new
        && before["dataFiles"] == next["dataFiles"]
        && before["services"] == next["services"]
        && before["oneshotServices"] == next["oneshotServices"]
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Leadership {
    pub cluster: String,
    pub node: u64,
    pub boot: String,
    pub term: u64,
    pub epoch: u64,
}
#[derive(Clone)]
pub struct PlanAuthority {
    pub current: Option<Leadership>,
    origin: String,
    token: String,
    client: reqwest::Client,
}
impl PlanAuthority {
    pub fn new(
        origin: String,
        token: String,
        current: Option<Leadership>,
        client: reqwest::Client,
    ) -> Self {
        Self {
            origin,
            token,
            current,
            client,
        }
    }
    pub async fn accept(&mut self, plan: &Value) -> anyhow::Result<()> {
        let Some(proof) = plan.get("leadership").filter(|v| !v.is_null()) else {
            ensure!(
                self.current.is_none(),
                "controller omitted required leadership proof"
            );
            return Ok(());
        };
        let next: Leadership = serde_json::from_value(proof.clone())?;
        ensure!(
            (1..(1 << 53)).contains(&next.epoch) && next.term < 1 << 53,
            "invalid leadership proof"
        );
        if let Some(current) = &self.current {
            ensure!(
                next.cluster == current.cluster
                    && next.epoch >= current.epoch
                    && (next.epoch != current.epoch
                        || (next.node == current.node
                            && next.boot == current.boot
                            && next.term == current.term)),
                "obsolete leadership proof"
            );
        }
        let response = self
            .client
            .post(format!("{}/api/leadership/verify", self.origin))
            .bearer_auth(&self.token)
            .timeout(std::time::Duration::from_secs(7))
            .json(&next)
            .send()
            .await?;
        let confirmed: Leadership = runner_core::response_json(response, 4096).await?;
        ensure!(
            confirmed == next,
            "leadership changed during plan verification"
        );
        self.current = Some(next);
        Ok(())
    }
}

pub fn resume(
    saved: &Value,
    boot_id: &str,
    control: &str,
    revision: Option<&str>,
    now: u64,
) -> anyhow::Result<(u64, String, Option<String>)> {
    if saved.is_null() {
        return Ok((now, String::new(), None));
    }
    ensure!(
        saved["control"].as_str() == Some(control),
        "RUNNER_DATA belongs to another control plane; use a separate directory for another fleet"
    );
    let fresh = saved["bootId"].as_str() != Some(boot_id)
        || saved["restartRequested"] == true
        || revision.is_some_and(|r| saved["installedRevision"].as_str() != Some(r));
    let started = if fresh {
        now.max(saved["started"].as_u64().unwrap_or(0) + 1)
    } else {
        saved["started"].as_u64().filter(|n| *n > 0).unwrap_or(now)
    };
    let run = if fresh {
        format!(
            "{}-{}-{}",
            saved["agent"].as_str().unwrap_or(""),
            started,
            &runner_core::random_id()[..8]
        )
    } else {
        saved["run"].as_str().unwrap_or("").into()
    };
    let previous = saved["previousRun"]
        .as_str()
        .filter(|s| !s.is_empty())
        .or_else(|| fresh.then(|| saved["run"].as_str()).flatten())
        .map(String::from);
    Ok((started, run, previous))
}

fn normalize(path: &Path) -> PathBuf {
    let path = if path.is_absolute() {
        path.to_owned()
    } else {
        std::env::current_dir()
            .unwrap_or_else(|_| "/".into())
            .join(path)
    };
    let mut out = PathBuf::new();
    for part in path.components() {
        match part {
            Component::ParentDir => {
                out.pop();
            }
            Component::CurDir => (),
            other => out.push(other),
        }
    }
    out
}
pub fn owns_compose(container: &Value, owner: &str, key: &str, legacy_dir: &Path) -> bool {
    let labels = &container["Config"]["Labels"];
    if labels[OWNER_LABEL] == owner && labels[COPY_LABEL] == key {
        return true;
    }
    if labels[OWNER_LABEL].as_str().is_some_and(|s| !s.is_empty()) {
        return false;
    }
    let files: Vec<_> = labels["com.docker.compose.project.config_files"]
        .as_str()
        .unwrap_or("")
        .split(',')
        .collect();
    normalize(Path::new(
        labels["com.docker.compose.project.working_dir"]
            .as_str()
            .unwrap_or("/"),
    )) == normalize(legacy_dir)
        && files.len() == 1
        && normalize(Path::new(files[0])) == normalize(&legacy_dir.join("compose.yaml"))
}
pub fn owns_infrastructure(
    container: &Value,
    owner: &str,
    kind: &str,
    legacy_source: &Path,
    destination: &str,
) -> bool {
    let labels = &container["Config"]["Labels"];
    if labels[COPY_LABEL].as_str().is_some_and(|s| !s.is_empty()) {
        return false;
    }
    if labels[OWNER_LABEL] == owner && labels["io.runners.component"] == kind {
        return true;
    }
    !labels[OWNER_LABEL].as_str().is_some_and(|s| !s.is_empty())
        && container["Mounts"]
            .as_array()
            .into_iter()
            .flatten()
            .any(|mount| {
                mount["Type"] == "bind"
                    && mount["Destination"] == destination
                    && normalize(Path::new(mount["Source"].as_str().unwrap_or("/")))
                        == normalize(legacy_source)
            })
}
pub fn container_health(
    containers: &[Value],
    services: &[String],
    oneshot: &[String],
) -> Option<&'static str> {
    let normal: Vec<_> = containers
        .iter()
        .filter(|c| c["Config"]["Labels"]["com.docker.compose.oneoff"] != "True")
        .collect();
    if normal.is_empty() {
        return Some("no containers are running");
    }
    if services.iter().any(|s| {
        !normal
            .iter()
            .any(|c| c["Config"]["Labels"]["com.docker.compose.service"] == *s)
    }) {
        return Some("an application service is missing");
    }
    for container in normal {
        let state = &container["State"];
        let labels = &container["Config"]["Labels"];
        if state["Status"] == "exited"
            && state["ExitCode"] == 0
            && (labels["io.runners.oneshot"] == "true"
                || oneshot
                    .iter()
                    .any(|s| labels["com.docker.compose.service"] == *s))
        {
            continue;
        }
        if state["Running"] != true
            || state["Restarting"] == true
            || state["Paused"] == true
            || state["Dead"] == true
        {
            return Some("an application container has stopped");
        }
        if !state["Health"].is_null()
            && !matches!(
                state["Health"]["Status"].as_str(),
                Some("healthy" | "starting")
            )
        {
            return Some("an application container is unhealthy");
        }
    }
    None
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BindSource {
    pub path: String,
    #[serde(default)]
    pub read_only: bool,
}
pub fn bind_layout(sources: &[BindSource]) -> Vec<(String, bool)> {
    let mut layout: Vec<_> = sources
        .iter()
        .map(|s| (s.path.clone(), s.read_only))
        .collect();
    layout.sort_by(|a, b| filename_cmp(&a.0, &b.0).then(a.1.cmp(&b.1)));
    layout
}

pub fn app_routes(projects: impl IntoIterator<Item = Value>, domain: &str) -> Vec<Value> {
    if domain.is_empty() {
        return Vec::new();
    }
    let mut apps: BTreeMap<String, Vec<Value>> = BTreeMap::new();
    let mut routes = Vec::new();
    let proxy = |project: &Value| json!({"handler":"reverse_proxy", "upstreams":[{"dial":format!("127.0.0.1:{}", project["port"].as_u64().unwrap_or(0))}],"flush_interval":-1,"stream_close_delay":"1h"});
    for project in projects {
        if project["port"].as_u64().unwrap_or(0) == 0 {
            continue;
        }
        let app = project["app"].as_str().unwrap_or("");
        let replica = project["replica"].as_u64().unwrap_or(1);
        routes.push(json!({"match":[{"host":[format!("{app}-{replica}.{domain}")]}],"handle":[proxy(&project)]}));
        apps.entry(app.into()).or_default().push(project);
    }
    for (app, copies) in apps {
        let ready = copies
            .iter()
            .filter(|p| p["s"] == "healthy" && p["r"] != false && p["busy"] != true)
            .min_by_key(|p| p["replica"].as_u64().unwrap_or(1));
        let handler = ready.map(proxy).unwrap_or_else(|| json!({"handler":"static_response","status_code":503,"headers":{"X-Runner-Route":["unavailable"],"Retry-After":["2"]},"body":"No healthy copy of this app is available on this machine\n"}));
        routes.push(json!({"match":[{"host":[format!("{app}.{domain}")]}],"handle":[handler]}));
    }
    routes
}
