use crate::{
    command::Commands,
    metrics::{History, Metrics},
    runtime::{fingerprint, name_valid, namespace, resume, server_tag},
    workloads::DockerHost,
};
use anyhow::{Context as _, ensure};
use runner_core::{
    fs::{atomic_json, private_directory},
    now_ms,
};
use serde::{
    Serialize,
    ser::{SerializeMap, Serializer},
};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
    sync::Arc,
};
use tokio::sync::{Mutex, Notify, RwLock};

pub struct AgentState {
    pub fields: serde_json::Map<String, Value>,
    pub projects: BTreeMap<String, Value>,
    pub metrics: Metrics,
    pub ready: bool,
    pub public_open: bool,
    pub accepted: bool,
    pub plan: Value,
    pub public_active: Value,
    pub system: BTreeSet<String>,
    pub rtt: Option<u64>,
    pub leaving: bool,
}
impl AgentState {
    pub fn s(&self, key: &str) -> &str {
        self.fields.get(key).and_then(Value::as_str).unwrap_or("")
    }
    pub fn n(&self, key: &str) -> u64 {
        self.fields.get(key).and_then(Value::as_u64).unwrap_or(0)
    }
    pub fn b(&self, key: &str) -> bool {
        self.fields
            .get(key)
            .and_then(Value::as_bool)
            .unwrap_or(false)
    }
    pub fn value(&self, key: &str) -> &Value {
        self.fields.get(key).unwrap_or(&Value::Null)
    }
    pub fn set(&mut self, key: &str, value: impl Into<Value>) {
        self.fields.insert(key.into(), value.into());
    }
    pub fn describe(&self, environment: &BTreeMap<String, String>) -> Value {
        let tag = self.s("tag");
        let mut out = json!({"tag":tag,"pool":if tag == "standalone" { Value::Null } else { json!(tag) },"label":environment.get("LABEL"),"starts":environment.get("START_CMD").is_some_and(|s| !s.is_empty()),"cpus":std::thread::available_parallelism().map_or(1,|n| n.get())});
        if let Some(region) = environment.get("REGION").filter(|s| !s.is_empty()) {
            out["region"] = json!(region);
        }
        if let Some(lifetime) = environment
            .get("LIFETIME_MIN")
            .and_then(|s| s.parse::<u64>().ok())
            .filter(|n| *n > 0)
        {
            out["deadline"] = json!(
                self.n("started")
                    .saturating_add(lifetime.saturating_mul(60_000))
            );
        }
        out
    }
    pub fn settled(&self, desired: &Value) -> bool {
        desired.as_object().is_some_and(|desired| {
            desired.iter().all(|(key, want)| {
                self.projects.get(key).is_some_and(|p| {
                    p["v"] == want["v"] && p["busy"] != true && p["s"] == "healthy"
                })
            })
        })
    }
}
struct Saved<'a>(&'a AgentState);
impl Serialize for Saved<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(self.0.fields.len() + 2))?;
        for (key, value) in &self.0.fields {
            map.serialize_entry(key, value)?;
        }
        map.serialize_entry("projects", &self.0.projects.iter().collect::<Vec<_>>())?;
        // Decompress one historical minute at a time directly into the file.
        map.serialize_entry("metrics", &History(&self.0.metrics))?;
        map.end()
    }
}
#[derive(Clone)]
pub struct Context {
    pub state: Arc<RwLock<AgentState>>,
    pub host: DockerHost,
    pub environment: Arc<BTreeMap<String, String>>,
    pub control: String,
    pub token: String,
    pub wake: Arc<Notify>,
    pub changed: Arc<Notify>,
    pub route_changed: Arc<Notify>,
    writer: Arc<Mutex<()>>,
}
impl Context {
    pub async fn accept(&self, plan: &mut Value) -> anyhow::Result<()> {
        let _writer = self.writer.lock().await;
        let mut state = self.state.write().await;
        let mut previous = Vec::new();
        let mut adopted = Vec::new();
        if plan["hold"] != true {
            for (key, next) in plan["desired"].as_object().into_iter().flatten() {
                let old = &state.value("desired")[key];
                let Some(project) = state.projects.get(key) else {
                    continue;
                };
                if project["busy"] == true
                    || project["s"] == "failed"
                    || old.is_null()
                    || old["compose"] == next["compose"]
                {
                    continue;
                }
                let before = fingerprint(old)?;
                let after = fingerprint(next)?;
                if before != after
                    && project["fingerprint"] == before
                    && crate::runtime::equivalent_plan(old, next)
                {
                    adopted.push((key.clone(), project["fingerprint"].clone(), after));
                }
            }
        }
        for (key, _, next) in &adopted {
            state.projects.get_mut(key).unwrap()["fingerprint"] = json!(next);
        }
        for (source, destination) in [
            ("leadership", "leadership"),
            ("recovery", "recovery"),
            ("desired", "desired"),
            ("publicTunnels", "publicTunnels"),
            ("domain", "domain"),
        ] {
            if source != "leadership" && plan["hold"] == true {
                continue;
            }
            if let Some(value) = plan.get_mut(source).filter(|v| !v.is_null()) {
                previous.push((
                    destination,
                    state.fields.insert(destination.into(), value.take()),
                ));
            }
        }
        if state.value("recovery")["payload"]
            .as_str()
            .and_then(|p| serde_json::from_str::<Value>(p).ok())
            .is_some_and(|p| p["run"] == state.s("run"))
        {
            previous.push((
                "previousRun",
                state.fields.insert("previousRun".into(), Value::Null),
            ));
        }
        if let Err(error) = atomic_json(&self.host.base.join("agent-state.json"), &Saved(&state)) {
            for (key, old, _) in adopted {
                state.projects.get_mut(&key).unwrap()["fingerprint"] = old;
            }
            for (key, value) in previous {
                if let Some(value) = value {
                    state.fields.insert(key.into(), value);
                } else {
                    state.fields.remove(key);
                }
            }
            return Err(error);
        }
        if plan["hold"] != true {
            state.accepted = true;
        }
        state.plan = json!({"hold":plan["hold"],"successor":plan["successor"],"serve":plan["serve"],"poll":plan["poll"]});
        drop(state);
        self.changed.notify_one();
        self.route_changed.notify_one();
        Ok(())
    }
    pub fn load() -> anyhow::Result<Self> {
        let environment: BTreeMap<String, String> = std::env::vars().collect();
        let control = environment
            .get("CONTROL_URL")
            .filter(|s| !s.is_empty())
            .context("CONTROL_URL is required")?
            .to_owned();
        let token = environment
            .get("JOIN_TOKEN")
            .filter(|s| !s.is_empty())
            .context("JOIN_TOKEN is required")?
            .to_owned();
        if let Some(region) = environment.get("REGION").filter(|s| !s.is_empty()) {
            ensure!(
                region.len() <= 32
                    && region.as_bytes()[0].is_ascii_lowercase()
                    && region
                        .bytes()
                        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == b'-'),
                "REGION must start with a letter and contain up to 32 lowercase letters, digits or dashes"
            );
        }
        let base = private_directory(&PathBuf::from(
            environment
                .get("RUNNER_DATA")
                .map(String::as_str)
                .unwrap_or("/var/lib/runner"),
        ))?;
        let state_path = base.join("agent-state.json");
        let mut saved: Value = match std::fs::File::open(state_path) {
            Ok(file) => {
                ensure!(
                    file.metadata()?.len() <= 64 << 20,
                    "saved agent inventory is too large"
                );
                serde_json::from_reader(file).context(
                    "cannot read the saved agent inventory; restore it before restarting",
                )?
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Value::Null,
            Err(e) => return Err(e.into()),
        };
        let boot = std::fs::read_to_string("/proc/sys/kernel/random/boot_id")
            .unwrap_or_default()
            .trim()
            .to_owned();
        let revision = environment
            .get("RUNNER_INSTALL_REVISION")
            .map(String::as_str)
            .unwrap_or(env!("CARGO_PKG_VERSION"));
        let (started, run, previous) = resume(&saved, &boot, &control, Some(revision), now_ms())?;
        let tag = server_tag(&environment, &saved)?;
        let owner = namespace(&control)?;
        let history = saved
            .get_mut("metrics")
            .map(Value::take)
            .unwrap_or(json!([]));
        let metrics = Metrics::new(
            environment
                .get("DOCKER_SOCKET")
                .map(String::as_str)
                .unwrap_or("/var/run/docker.sock"),
            history.as_array().map(Vec::as_slice).unwrap_or(&[]),
        )?;
        drop(history);
        let rows = saved
            .get_mut("projects")
            .map(Value::take)
            .unwrap_or(json!([]));
        let mut projects: BTreeMap<String, Value> =
            serde_json::from_value::<Vec<(String, Value)>>(rows)?
                .into_iter()
                .collect();
        for (key, project) in &mut projects {
            ensure!(
                name_valid(key, 71) && project.is_object(),
                "invalid saved application inventory"
            );
            if !project["fingerprint"].is_string() && saved["desired"][key].is_object() {
                project["fingerprint"] = json!(fingerprint(&saved["desired"][key])?);
            }
            if project["busy"] == true {
                project["s"] = json!("failed");
            }
            project["busy"] = json!(false);
        }
        let mut fields = saved
            .as_object_mut()
            .map(std::mem::take)
            .unwrap_or_default();
        fields.remove("metrics");
        fields.remove("projects");
        fields.insert("v".into(), json!(2));
        fields.insert("bootId".into(), json!(boot));
        fields.insert("control".into(), json!(control));
        fields.insert("started".into(), json!(started));
        fields.insert("run".into(), json!(run));
        fields.insert("previousRun".into(), json!(previous));
        fields.insert("restartRequested".into(), json!(false));
        fields.insert("installedRevision".into(), json!(revision));
        fields.insert("tag".into(), json!(tag));
        fields.entry("desired").or_insert(json!({}));
        fields.entry("publicTunnels").or_insert(json!({}));
        let router_port = environment
            .get("ROUTER_PORT")
            .map(|s| s.parse::<u16>())
            .transpose()?
            .unwrap_or(19080);
        ensure!(
            router_port > 0 && router_port < 65533,
            "invalid ROUTER_PORT"
        );
        let engine = metrics.client();
        let host = DockerHost {
            owner: owner.clone(),
            base,
            commands: Commands::new(&environment),
            client: runner_core::http_client(std::time::Duration::from_secs(30))?,
            engine,
            router_port,
        };
        Ok(Self {
            state: Arc::new(RwLock::new(AgentState {
                fields,
                projects,
                metrics,
                ready: false,
                public_open: false,
                accepted: false,
                plan: Value::Null,
                public_active: json!({}),
                system: BTreeSet::from(["runner-agent".into(), format!("{owner}-agent")]),
                rtt: None,
                leaving: false,
            })),
            host,
            environment: Arc::new(environment),
            control,
            token,
            wake: Arc::new(Notify::new()),
            changed: Arc::new(Notify::new()),
            route_changed: Arc::new(Notify::new()),
            writer: Arc::new(Mutex::new(())),
        })
    }
    pub async fn save(&self) -> anyhow::Result<()> {
        // Serialize only after acquiring the writer: concurrent changes never
        // queue multiple copies of the complete inventory in RAM.
        let _writer = self.writer.lock().await;
        let state = self.state.read().await;
        atomic_json(&self.host.base.join("agent-state.json"), &Saved(&state))
    }
    pub fn notify(&self) {
        self.changed.notify_one();
        self.route_changed.notify_one();
        self.wake.notify_one();
    }
}
