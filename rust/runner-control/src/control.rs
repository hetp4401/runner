use crate::{
    error::{http, require},
    model::*,
    spec,
    storage::Store,
};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex},
};

#[derive(Default)]
struct VersionCache {
    values: BTreeMap<(String, u64), (Arc<Version>, usize, u64)>,
    bytes: usize,
    clock: u64,
}
pub struct Control {
    pub store: Store,
    pub env: BTreeMap<String, String>,
    pub rt: Runtime,
    pub http: reqwest::Client,
    versions: Mutex<VersionCache>,
}
impl std::ops::Deref for Control {
    type Target = Runtime;
    fn deref(&self) -> &Runtime {
        &self.rt
    }
}
impl std::ops::DerefMut for Control {
    fn deref_mut(&mut self) -> &mut Runtime {
        &mut self.rt
    }
}
impl Control {
    pub fn new(store: Store, mut env: BTreeMap<String, String>, now: u64) -> anyhow::Result<Self> {
        let auth = env
            .get("REQUIRE_DEPLOY_AUTH")
            .map(|s| s.to_lowercase())
            .unwrap_or_else(|| "0".into());
        require(
            ["0", "1", "false", "true"].contains(&auth.as_str()),
            "REQUIRE_DEPLOY_AUTH must be 1/true or 0/false",
        )?;
        env.insert(
            "REQUIRE_DEPLOY_AUTH".into(),
            if auth == "1" || auth == "true" {
                "1"
            } else {
                "0"
            }
            .into(),
        );
        let prefix = env
            .entry("TUNNEL_PREFIX".into())
            .or_insert_with(|| "runner".into());
        require(
            !prefix.is_empty()
                && prefix.len() <= 63
                && prefix.as_bytes()[0].is_ascii_alphanumeric()
                && prefix
                    .bytes()
                    .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == b'-'),
            "invalid TUNNEL_PREFIX",
        )?;
        let recovering_until = if env.get("RECOVER_RUNTIME").is_some_and(|s| s == "1") {
            now + env
                .get("RECOVERY_MS")
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(180_000)
        } else {
            0
        };
        let rt = Runtime {
            boot_at: now,
            settle_at: now,
            last_checkin: now,
            recovering_until,
            tunnels_ready: env.get("RECOVER_TUNNELS").is_none_or(|s| s != "1"),
            dns_dirty: true,
            ..Default::default()
        };
        let mut c = Self {
            store,
            env,
            rt,
            http: runner_core::http_client(std::time::Duration::from_secs(30))?,
            versions: Mutex::new(VersionCache::default()),
        };
        c.reload_rows()?;
        c.quiet_since = c.runs.values().map(|r| r.seen).max().unwrap_or(0);
        c.set_pace(now);
        Ok(c)
    }
    // Rollback restores only rows touched by transactions, without cloning
    // metrics history, large specifications or every runtime observation.
    pub fn reload_rows(&mut self) -> anyhow::Result<()> {
        let c = self;
        let old_runs = std::mem::take(&mut c.runs);
        c.projects.clear();
        c.settings.clear();
        c.copies.clear();
        c.starts.clear();
        c.app_passwords.clear();
        c.app_env.clear();
        c.slots.clear();
        c.agents.clear();
        *c.versions.lock().unwrap() = VersionCache::default();
        for p in c.store.rows("SELECT * FROM projects", &[])? {
            let p: Project = serde_json::from_value(p)?;
            c.projects.insert(p.name.clone(), p);
        }
        for s in c.store.rows("SELECT * FROM settings", &[])? {
            let key = s["key"].as_str().unwrap_or("");
            if !["pools", "pool_asks"].contains(&key) {
                c.settings
                    .insert(key.into(), s["value"].as_str().unwrap_or("").into());
            }
        }
        for r in c.store.rows("SELECT * FROM runs", &[])? {
            let mut run = Self::run_row(r)?;
            if let Some(old) = old_runs.get(&run.id) {
                run.extra = old.extra.clone();
            }
            c.runs.insert(run.id.clone(), run);
        }
        for row in c.store.rows("SELECT * FROM copies ORDER BY since", &[])? {
            let name = row["name"].as_str().unwrap_or("").to_owned();
            c.copies.entry(name).or_default().push(Self::copy_row(row)?);
        }
        for row in c.store.rows("SELECT * FROM starts", &[])? {
            c.starts.insert(n(&row, "machine"), n(&row, "at"));
        }
        for row in c.store.rows("SELECT * FROM app_passwords", &[])? {
            c.app_passwords.insert(s(&row, "name").into(), row);
        }
        for row in c.store.rows("SELECT * FROM app_env ORDER BY key", &[])? {
            c.app_env
                .entry(s(&row, "name").into())
                .or_default()
                .insert(s(&row, "key").into(), s(&row, "value").into());
        }
        for row in c.store.rows("SELECT * FROM slots", &[])? {
            c.slots.insert(n(&row, "n"), row);
        }
        for row in c.store.rows("SELECT * FROM agents", &[])? {
            c.agents.insert(s(&row, "id").into(), row);
        }
        c.recent_events = c
            .store
            .rows("SELECT * FROM events ORDER BY id DESC LIMIT 200", &[])?
            .into_iter()
            .rev()
            .collect();
        Ok(())
    }
    pub fn e(&self, k: &str) -> &str {
        self.env.get(k).map(String::as_str).unwrap_or("")
    }
    pub fn clear_versions(&self, name: &str) {
        let mut cache = self.versions.lock().unwrap();
        cache.values.retain(|(n, _), _| n != name);
        cache.bytes = cache.values.values().map(|(_, size, _)| size).sum();
    }
    pub fn en(&self, k: &str, default: u64) -> u64 {
        self.env
            .get(k)
            .and_then(|s| s.parse().ok())
            .unwrap_or(default)
    }
    pub fn version(&self, name: &str, v: u64) -> anyhow::Result<Option<Arc<Version>>> {
        let key = (name.to_owned(), v);
        let mut cache = self.versions.lock().unwrap();
        cache.clock += 1;
        let clock = cache.clock;
        if let Some((value, _, at)) = cache.values.get_mut(&key) {
            *at = clock;
            return Ok(Some(value.clone()));
        }
        let rows = self.store.rows(
            "SELECT compose,port,files,replicas FROM versions WHERE name=? AND version=?",
            &[json!(name), json!(v)],
        )?;
        let Some(row) = rows.first() else {
            return Ok(None);
        };
        let compose = s(row, "compose").to_owned();
        let files: BTreeMap<String, String> =
            serde_json::from_str(row["files"].as_str().unwrap_or("{}"))?;
        let bytes = compose.len() + files.iter().map(|(k, v)| k.len() + v.len()).sum::<usize>();
        let mut claims = spec::claims(&compose);
        let port = row["port"].as_u64().map(|n| n as u16);
        if let Some(port) = port {
            let c = format!("port {port}");
            if !claims.contains(&c) {
                claims.push(c);
            }
        }
        let value = Arc::new(Version {
            policy: spec::policy(&compose),
            nodup: spec::fixed_ports(&compose, true),
            fixed: spec::fixed_ports(&compose, false),
            spec: spec::Spec {
                compose,
                port,
                replicas: row["replicas"].as_u64().unwrap_or(1) as u32,
                files,
            },
            claims,
        });
        while !cache.values.is_empty()
            && (cache.values.len() >= 64 || cache.bytes + bytes > 8 << 20)
        {
            let key = cache
                .values
                .iter()
                .min_by_key(|(_, (_, _, at))| at)
                .unwrap()
                .0
                .clone();
            let (_, size, _) = cache.values.remove(&key).unwrap();
            cache.bytes -= size;
        }
        if bytes <= 8 << 20 {
            cache.bytes += bytes;
            cache.values.insert(key, (value.clone(), bytes, clock));
        }
        Ok(Some(value))
    }
    pub fn project(&self, name: &str) -> anyhow::Result<Project> {
        self.projects
            .get(name)
            .cloned()
            .ok_or_else(|| http(404, format!("no app called {name}")))
    }
    pub fn wanted_spec(&self, name: &str) -> anyhow::Result<Option<Arc<Version>>> {
        match self.projects.get(name).and_then(Project::wanted) {
            Some(v) => self.version(name, v),
            None => Ok(None),
        }
    }
    pub fn save_project(&mut self, p: Project) -> anyhow::Result<()> {
        self.store.exec("INSERT INTO projects(name,version,stable,rollout,halted,updated,enabled) VALUES(?,?,?,?,?,?,?) ON CONFLICT(name) DO UPDATE SET version=excluded.version,stable=excluded.stable,rollout=excluded.rollout,halted=excluded.halted,updated=excluded.updated,enabled=excluded.enabled",&[json!(p.name),json!(p.version),json!(p.stable),json!(p.rollout),json!(p.halted),json!(p.updated),json!(p.enabled)])?;
        self.projects.insert(p.name.clone(), p);
        Ok(())
    }
    pub fn run_row(mut row: Value) -> anyhow::Result<Run> {
        if row["status"].is_string() {
            row["status"] = serde_json::from_str(s(&row, "status"))?;
        }
        for key in ["ready", "handover", "retire", "drain"] {
            if row[key].is_null() {
                row[key] = json!(0);
            }
        }
        serde_json::from_value(row).map_err(Into::into)
    }
    pub fn copy_row(mut row: Value) -> anyhow::Result<Copy> {
        for key in ["leaving", "ports"] {
            if row[key].is_string() {
                row[key] = serde_json::from_str(s(&row, key))?;
            }
        }
        if row["key"].is_null() {
            row["key"] = row["name"].clone();
        }
        serde_json::from_value(row).map_err(Into::into)
    }
    pub fn save_run(&mut self, mut r: Run) -> anyhow::Result<()> {
        self.store.exec("INSERT INTO runs(id,machine,started,status,ready,handover,retire,seen,agent,kind,label,pool,drain,deadline,evict,quarantine) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET status=excluded.status,ready=excluded.ready,handover=excluded.handover,retire=excluded.retire,seen=excluded.seen,label=excluded.label,pool=excluded.pool,drain=excluded.drain,deadline=excluded.deadline,evict=excluded.evict,quarantine=excluded.quarantine",&[json!(r.id),json!(r.machine),json!(r.started),json!(serde_json::to_string(&r.status)?),json!(r.ready),json!(r.handover),json!(r.retire),json!(r.seen),json!(r.agent),json!(""),json!(r.label),json!(r.pool),json!(r.drain),json!(r.deadline),json!(r.evict),json!(r.quarantine)])?;
        r.set("savedSeen", r.seen);
        self.runs.insert(r.id.clone(), r);
        Ok(())
    }
    pub fn set_setting(&mut self, key: &str, value: impl ToString) -> anyhow::Result<()> {
        let value = value.to_string();
        self.store.exec("INSERT INTO settings(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",&[json!(key),json!(value)])?;
        self.settings.insert(key.into(), value);
        Ok(())
    }
    pub fn set_pace(&mut self, now: u64) {
        let machines = self
            .runs
            .values()
            .filter(|r| r.retire == 0 && now.saturating_sub(r.seen) < 10 * MIN)
            .map(|r| r.machine)
            .collect::<BTreeSet<_>>()
            .len();
        let fixed = self.en("POLL_S", 0);
        self.poll_s = if fixed > 0 {
            fixed.max(5)
        } else {
            ((86400. * machines.max(1) as f64 / 100000.).round() as u64).clamp(20, 300)
        };
        self.live_ms = self.poll_s * 2500;
    }
    pub fn live(&self, r: &Run, now: u64) -> bool {
        now.saturating_sub(r.seen) < self.live_ms
    }
    pub fn live_runs(&self, now: u64) -> Vec<Run> {
        self.runs
            .values()
            .filter(|r| r.retire == 0 && self.live(r, now))
            .cloned()
            .collect()
    }
    pub fn live_machines(&self, now: u64) -> BTreeMap<u64, Run> {
        let mut up: BTreeMap<u64, Run> = BTreeMap::new();
        for r in self.live_runs(now) {
            if up.get(&r.machine).is_none_or(|p| p.started < r.started) {
                up.insert(r.machine, r);
            }
        }
        up
    }
    pub fn groups(&self, now: u64) -> Value {
        let mut groups: BTreeMap<String, BTreeSet<u64>> = BTreeMap::new();
        for r in self.live_runs(now) {
            groups.entry(r.tag().into()).or_default().insert(r.machine);
        }
        json!(
            groups
                .iter()
                .map(|(tag, m)| json!({"tag":tag,"online":m.len()}))
                .collect::<Vec<_>>()
        )
    }
    pub fn recovery_active(&mut self, now: u64) -> bool {
        if !self.tunnels_ready || now < self.recovering_until {
            return true;
        }
        let expired = self
            .recovery_missing
            .iter()
            .filter(|id| {
                self.runs
                    .get(*id)
                    .is_some_and(|r| now.saturating_sub(r.seen) > self.live_ms.max(180_000))
            })
            .cloned()
            .collect::<Vec<_>>();
        for id in expired {
            self.recovery_missing.remove(&id);
        }
        !self.recovery_missing.is_empty()
    }
    pub fn settling(&self, now: u64) -> bool {
        !self.tunnels_ready
            || now < self.recovering_until
            || self.recovery_missing.iter().any(|id| {
                self.runs
                    .get(id)
                    .is_none_or(|r| now.saturating_sub(r.seen) <= self.live_ms.max(180_000))
            })
            || now.saturating_sub(self.settle_at) < 5 * MIN
                && self.runs.values().any(|r| {
                    r.retire == 0
                        && r.seen >= self.quiet_since.saturating_sub(self.live_ms)
                        && r.seen < self.settle_at
                })
    }
    pub fn quiet(&self, now: u64) -> bool {
        self.settling(now) || now.saturating_sub(self.settle_at) < self.en("QUIET_MS", 10 * MIN)
    }
    pub fn copies_of(&self, name: &str) -> &[Copy] {
        self.copies.get(name).map(Vec::as_slice).unwrap_or(&[])
    }
    pub fn copies_on(&self, name: &str, machine: u64) -> Vec<Copy> {
        self.copies_of(name)
            .iter()
            .filter(|x| x.machine == machine)
            .cloned()
            .collect()
    }
    pub fn save_copy(&mut self, name: &str, x: Copy) -> anyhow::Result<()> {
        self.store.exec("INSERT OR REPLACE INTO copies(name,machine,replica,since,reason,leaving,ports,key,cause) VALUES(?,?,?,?,?,?,?,?,?)",&[json!(name),json!(x.machine),json!(x.replica),json!(x.since),json!(x.reason),json!(x.leaving.as_ref().map(Value::to_string)),json!(x.ports.as_ref().map(|v|serde_json::to_string(v).unwrap())),json!(x.key),json!(x.cause)])?;
        let list = self.copies.entry(name.into()).or_default();
        if let Some(i) = list
            .iter()
            .position(|y| y.machine == x.machine && y.replica == x.replica)
        {
            list[i] = x;
        } else {
            list.push(x);
        }
        Ok(())
    }
    pub fn drop_copy(
        &mut self,
        name: &str,
        machine: u64,
        replica: u32,
        cause: Option<&str>,
        now: u64,
    ) -> anyhow::Result<()> {
        if let Some(list) = self.copies.get_mut(name) {
            if let Some(i) = list
                .iter()
                .position(|x| x.machine == machine && x.replica == replica)
            {
                let x = list.remove(i);
                if x.key == name {
                    self.plain_drops
                        .insert(format!("{name}|{machine}"), (now, replica));
                }
            }
        }
        if self.copies.get(name).is_some_and(Vec::is_empty) {
            self.copies.remove(name);
        }
        if !matches!(cause, Some("replicas" | "removed" | "disabled"))
            && !self.copies_of(name).iter().any(|x| x.replica == replica)
        {
            self.missing_at.insert(format!("{name}|{replica}"), now);
        }
        self.store.exec(
            "DELETE FROM copies WHERE name=? AND machine=? AND replica=?",
            &[json!(name), json!(machine), json!(replica)],
        )?;
        if let Some(cause) = cause {
            self.log_event(
                "drop",
                json!({"app":name,"replica":replica,"machine":machine,"cause":cause}),
                now,
            )?;
        }
        Ok(())
    }
    pub fn redact(&self, text: &str) -> String {
        let mut secrets: Vec<&str> = self
            .app_env
            .values()
            .flat_map(|e| e.values().map(String::as_str))
            .chain(
                self.env
                    .iter()
                    .filter(|(k, _)| {
                        ["TOKEN", "PASSWORD", "SECRET", "KEY"]
                            .iter()
                            .any(|s| k.contains(s))
                    })
                    .map(|(_, v)| v.as_str()),
            )
            .chain(self.tunnel_secrets.iter().map(String::as_str))
            .chain(
                self.public_tunnels
                    .values()
                    .filter_map(|v| v["token"].as_str()),
            )
            .flat_map(|v| std::iter::once(v).chain(v.split('\n')))
            .filter(|v| v.len() >= 4)
            .collect();
        secrets.sort_by_key(|s| std::cmp::Reverse(s.len()));
        secrets.dedup();
        let mut out = text.to_owned();
        for secret in secrets {
            out = out.replace(secret, "[hidden]");
        }
        out
    }
    pub fn reported_status(
        &mut self,
        value: &Value,
        run: Option<&str>,
        machine: Option<u64>,
        now: u64,
    ) -> Value {
        let mut out = serde_json::Map::new();
        for (key, state) in value.as_object().into_iter().flatten() {
            if key.is_empty()
                || key.len() > 64
                || !key.as_bytes()[0].is_ascii_alphanumeric()
                || !key
                    .bytes()
                    .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == b'-')
                || !state.is_object()
            {
                continue;
            }
            let mut st = serde_json::Map::new();
            for f in ["v", "t0", "n"] {
                if state[f].is_number() {
                    st.insert(f.into(), state[f].clone());
                }
            }
            let status = s(state, "s");
            st.insert(
                "s".into(),
                json!(if !status.is_empty()
                    && status.len() <= 24
                    && status.bytes().all(|b| b.is_ascii_lowercase() || b == b'-')
                {
                    status
                } else {
                    "unknown"
                }),
            );
            if state["r"].is_boolean() {
                st.insert("r".into(), state["r"].clone());
            }
            let revision = s(state, "revision");
            if revision.len() == 64
                && revision
                    .bytes()
                    .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
            {
                st.insert("revision".into(), json!(revision));
            }
            if !state["e"].is_null() && state["e"] != "" {
                st.insert(
                    "e".into(),
                    json!("Workload failed; authenticated diagnostics contain details"),
                );
                if let Some(run) = run {
                    let app = self
                        .copies
                        .iter()
                        .find(|(_, list)| {
                            list.iter()
                                .any(|x| Some(x.machine) == machine && x.key == *key)
                        })
                        .map(|(n, _)| n.clone())
                        .unwrap_or_else(|| base_app(key));
                    let error = tail(&spec::string(&state["e"]), 4000);
                    if self.diagnostics.len() >= 4096 {
                        if let Some(key) = self
                            .diagnostics
                            .iter()
                            .min_by_key(|(_, v)| n(v, "at"))
                            .map(|(k, _)| k.clone())
                        {
                            self.diagnostics.remove(&key);
                        }
                    }
                    self.diagnostics.insert(format!("{run}|{key}"),json!({"app":app,"key":key,"run":run,"machine":machine,"at":now,"error":error}));
                }
            } else if let Some(run) = run {
                self.diagnostics.remove(&format!("{run}|{key}"));
            }
            out.insert(key.clone(), Value::Object(st));
        }
        Value::Object(out)
    }
    pub fn log_event(&mut self, kind: &str, fields: Value, now: u64) -> anyhow::Result<()> {
        let mut event = json!({"t":fields["t"].as_u64().unwrap_or(now),"kind":kind});
        for key in ["app", "replica", "machine", "other", "run", "cause"] {
            event[key] = fields[key].clone();
        }
        event["detail"] = if fields["detail"].is_null() {
            Value::Null
        } else {
            json!(truncate(
                &self.redact(&spec::string(&fields["detail"])),
                500
            ))
        };
        let params = [
            "t", "kind", "app", "replica", "machine", "other", "run", "cause", "detail",
        ]
        .iter()
        .map(|k| event[*k].clone())
        .collect::<Vec<_>>();
        let rows=self.store.exec("INSERT INTO events(t,kind,app,replica,machine,other,run,cause,detail) VALUES(?,?,?,?,?,?,?,?,?) RETURNING id",&params)?;
        event["id"] = rows[0]["id"].clone();
        self.recent_events.push_back(event);
        while self.recent_events.len() > 200 {
            self.recent_events.pop_front();
        }
        Ok(())
    }
    pub fn mark_start(
        &mut self,
        machine: u64,
        at: u64,
        by: &str,
        pool: Option<String>,
    ) -> anyhow::Result<()> {
        self.store.exec("INSERT INTO starts(machine,at,by) VALUES(?,?,?) ON CONFLICT(machine) DO UPDATE SET at=excluded.at,by=excluded.by",&[json!(machine),json!(at),json!(by)])?;
        self.starts.insert(machine, at);
        self.start_pools.insert(machine, pool);
        Ok(())
    }
    pub fn free_key(&self, name: &str, machine: u64, k: u32, now: u64) -> String {
        let freed = self.plain_drops.get(&format!("{name}|{machine}"));
        if !self.copies_on(name, machine).iter().any(|x| x.key == name)
            && !freed
                .is_some_and(|(at, replica)| now.saturating_sub(*at) < 10 * MIN && *replica != k)
        {
            name.into()
        } else {
            format!("{name}-r{k}")
        }
    }
    pub fn due_at(&self, r: &Run) -> Option<u64> {
        r.deadline.map(|deadline| {
            let mut h = 2166136261u32;
            for c in r.id.encode_utf16() {
                h = (h ^ c as u32).wrapping_mul(16777619);
            }
            (r.started + (240 + (h as u64 % 70)) * MIN).min(deadline.saturating_sub(45 * MIN))
        })
    }
}
pub fn n(v: &Value, k: &str) -> u64 {
    v[k].as_u64().unwrap_or(0)
}
pub fn f(v: &Value, k: &str) -> f64 {
    v[k].as_f64().unwrap_or(0.)
}
pub fn s<'a>(v: &'a Value, k: &str) -> &'a str {
    v[k].as_str().unwrap_or("")
}
pub fn truncate(s: &str, n: usize) -> String {
    s.chars().take(n).collect()
}
pub fn tail(s: &str, n: usize) -> String {
    s.chars()
        .rev()
        .take(n)
        .collect::<String>()
        .chars()
        .rev()
        .collect()
}
pub fn base_app(key: &str) -> String {
    key.rsplit_once("-r")
        .filter(|(_, s)| !s.is_empty() && s.bytes().all(|c| c.is_ascii_digit()))
        .map(|(a, _)| a)
        .unwrap_or(key)
        .into()
}
