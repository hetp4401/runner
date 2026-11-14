use crate::spec::{Policy, Spec};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    sync::Arc,
};
pub const MIN: u64 = 60_000;
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Project {
    pub name: String,
    pub version: u64,
    pub stable: Option<u64>,
    pub rollout: u64,
    pub halted: Option<String>,
    pub updated: u64,
    pub enabled: u64,
}
impl Project {
    pub fn wanted(&self) -> Option<u64> {
        if self.halted.is_some() {
            self.stable
        } else {
            Some(self.version)
        }
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Version {
    #[serde(flatten)]
    pub spec: Spec,
    pub claims: Vec<String>,
    pub policy: Policy,
    pub nodup: Option<String>,
    pub fixed: Option<String>,
}
impl std::ops::Deref for Version {
    type Target = Spec;
    fn deref(&self) -> &Spec {
        &self.spec
    }
}
#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct Run {
    pub id: String,
    pub machine: u64,
    pub started: u64,
    #[serde(default = "empty_arc")]
    pub status: Arc<Value>,
    #[serde(default)]
    pub ready: u64,
    #[serde(default)]
    pub handover: u64,
    #[serde(default)]
    pub retire: u64,
    pub seen: u64,
    #[serde(default)]
    pub agent: String,
    #[serde(default)]
    pub label: Option<String>,
    #[serde(default)]
    pub pool: Option<String>,
    #[serde(default)]
    pub drain: u64,
    #[serde(default)]
    pub deadline: Option<u64>,
    #[serde(default)]
    pub evict: Option<u64>,
    #[serde(default)]
    pub quarantine: Option<u64>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}
fn empty_arc() -> Arc<Value> {
    Arc::new(json!({}))
}
impl Run {
    pub fn n(&self, k: &str) -> u64 {
        self.extra.get(k).and_then(Value::as_u64).unwrap_or(0)
    }
    pub fn b(&self, k: &str) -> bool {
        self.extra.get(k) == Some(&Value::Bool(true))
    }
    pub fn s(&self, k: &str) -> &str {
        self.extra.get(k).and_then(Value::as_str).unwrap_or("")
    }
    pub fn v(&self, k: &str) -> &Value {
        self.extra.get(k).unwrap_or(&Value::Null)
    }
    pub fn set(&mut self, k: &str, v: impl Serialize) {
        self.extra.insert(
            k.into(),
            serde_json::to_value(v).expect("serializable runtime field"),
        );
    }
    pub fn tag(&self) -> &str {
        self.pool.as_deref().unwrap_or("standalone")
    }
    pub fn region(&self) -> Option<&str> {
        self.extra
            .get("region")
            .and_then(Value::as_str)
            .filter(|s| crate::spec::region_valid(s))
            .or_else(|| crate::spec::region_of(self.s("location")))
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Copy {
    pub machine: u64,
    pub replica: u32,
    pub since: u64,
    pub reason: String,
    #[serde(default)]
    pub leaving: Option<Value>,
    #[serde(default)]
    pub ports: Option<BTreeMap<u32, u32>>,
    pub key: String,
    #[serde(default)]
    pub cause: Option<String>,
}
impl Copy {
    pub fn to(&self) -> Option<u64> {
        self.leaving.as_ref().and_then(|v| v["to"].as_u64())
    }
}
#[derive(Clone, Default)]
pub struct Candidate {
    pub machine: u64,
    pub ready: u64,
    pub doubling: usize,
    pub tag_copies: usize,
    pub placed: usize,
    pub h: f64,
    pub roomy: bool,
    pub life_ok: bool,
    pub hot: bool,
    pub reason: String,
}
#[derive(Clone)]
pub struct Room {
    pub cpu: f64,
    pub mem: f64,
    pub h: f64,
    pub measured: bool,
}
#[derive(Default)]
pub struct Runtime {
    pub projects: BTreeMap<String, Project>,
    pub runs: BTreeMap<String, Run>,
    pub starts: BTreeMap<u64, u64>,
    pub start_pools: BTreeMap<u64, Option<String>>,
    pub settings: BTreeMap<String, String>,
    pub blocked: BTreeMap<String, String>,
    pub copies: BTreeMap<String, Vec<Copy>>,
    pub plain_drops: BTreeMap<String, (u64, u32)>,
    pub failed_on: BTreeMap<String, u64>,
    pub unhealthy_since: BTreeMap<String, u64>,
    pub missing_at: BTreeMap<String, u64>,
    pub defer_since: BTreeMap<String, u64>,
    pub capped_until: BTreeMap<String, u64>,
    pub depart_log: Vec<Value>,
    pub loss_log: Vec<u64>,
    pub loss_noted: BTreeSet<String>,
    pub pulled: BTreeSet<String>,
    pub boot_at: u64,
    pub recovering_until: u64,
    pub cold_recovery_complete: bool,
    pub observed_runs: BTreeSet<String>,
    pub recovery_seen: BTreeMap<String, u64>,
    pub recovery_missing: BTreeSet<String>,
    pub recovery_settings_at: u64,
    pub diagnostics: BTreeMap<String, Value>,
    pub settle_at: u64,
    pub quiet_since: u64,
    pub last_checkin: u64,
    pub last_loss_check: u64,
    pub mass_arms: Vec<u64>,
    pub mass_exhausted_at: u64,
    pub dns_synced_at: u64,
    pub poll_s: u64,
    pub live_ms: u64,
    pub samples: BTreeMap<u64, Vec<Value>>,
    pub last_rebalance: u64,
    pub failures: BTreeMap<String, (u64, u64)>,
    pub app_passwords: BTreeMap<String, Value>,
    pub geo: BTreeMap<String, Value>,
    pub geo_pending: BTreeSet<String>,
    pub app_env: BTreeMap<String, BTreeMap<String, String>>,
    pub auto_moved: BTreeMap<String, u64>,
    pub minutes: BTreeMap<String, Vec<Value>>,
    pub last_healthy: BTreeMap<String, u64>,
    pub first_seen: BTreeMap<String, u64>,
    pub ready_since: BTreeMap<String, u64>,
    pub restarts: BTreeMap<String, Value>,
    pub low_disk_since: BTreeMap<String, u64>,
    pub strikes: BTreeMap<String, Vec<Value>>,
    pub sick_why: BTreeMap<String, String>,
    pub sick_drains: BTreeMap<String, u64>,
    pub version_fails: BTreeMap<String, BTreeSet<u64>>,
    pub alone_since: BTreeMap<String, u64>,
    pub noted: BTreeMap<String, Value>,
    pub alerted_at: BTreeMap<String, u64>,
    pub last_sick_check: u64,
    pub last_ready_check: u64,
    pub last_v: BTreeMap<String, u64>,
    pub lit_at: BTreeMap<String, Value>,
    pub dark: BTreeMap<String, Value>,
    pub last_dark_check: u64,
    pub due_wait: BTreeMap<String, Value>,
    pub handover_times: Vec<Value>,
    pub webhook_log: Vec<u64>,
    pub recent_events: VecDeque<Value>,
    pub slots: BTreeMap<u64, Value>,
    pub agents: BTreeMap<String, Value>,
    pub holds: BTreeMap<u64, Value>,
    pub tunnels_ready: bool,
    pub tunnel_recovery_error: Option<String>,
    pub last_cleanup: u64,
    pub live_metrics: BTreeMap<u64, Value>,
    pub pending_metrics: BTreeMap<u64, Value>,
    pub pending_metrics_bytes: usize,
    pub last_rollup: u64,
    pub metrics_cache: BTreeMap<String, (u64, Arc<[u8]>)>,
    pub dns_error: Option<String>,
    pub dns_targets_seen: String,
    pub dns_dirty: bool,
    pub tunnel_secrets: BTreeSet<String>,
    pub public_tunnels: BTreeMap<String, Value>,
    pub public_tunnel_ids: BTreeSet<String>,
    pub log_sessions: BTreeMap<String, Value>,
    pub wakes: BTreeSet<String>,
    pub geo_lookups: Vec<String>,
    pub alerts: Vec<Value>,
    pub evict_stuck: BTreeSet<String>,
    pub last_trim: u64,
    pub dns_notes: Vec<String>,
    pub dns_due: u64,
    pub public_cleanup_due: Option<u64>,
    pub churn_cache: Option<(u64, Value)>,
}
