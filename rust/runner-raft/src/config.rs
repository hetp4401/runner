use crate::model::Member;
use anyhow::{Context, ensure};
use std::{collections::BTreeMap, time::Duration};

#[derive(Clone)]
pub struct Config {
    pub id: u64,
    pub members: Vec<Member>,
    pub peers: BTreeMap<u64, String>,
    pub cluster: String,
    pub secret: String,
    pub tick: Duration,
    pub election_ticks: usize,
    pub heartbeat_ticks: usize,
}
impl Config {
    pub fn from_env() -> anyhow::Result<Self> {
        Self::from_map(&std::env::vars().collect())
    }
    pub fn from_map(values: &BTreeMap<String, String>) -> anyhow::Result<Self> {
        let env =
            |k: &str, fallback: &str| values.get(k).cloned().unwrap_or_else(|| fallback.into());
        let number = |k: &str, fallback: u64| {
            values
                .get(k)
                .map(|v| v.parse::<u64>().with_context(|| format!("invalid {k}")))
                .unwrap_or(Ok(fallback))
        };
        let members: Vec<Member> = serde_json::from_str(&env("CP_MEMBERS", ""))
            .context("CP_MEMBERS must be a JSON member array")?;
        ensure!(
            members.len() >= 3 && members.len() <= 51 && members.len() % 2 == 1,
            "an odd CP_MEMBERS set of at least three is required (maximum 51)"
        );
        let id = number("CP_NODE_ID", 0)?;
        let mut peers = BTreeMap::new();
        for member in &members {
            ensure!(
                member.id > 0
                    && valid_origin(&member.url)
                    && peers.insert(member.id, member.url.clone()).is_none(),
                "invalid members"
            );
        }
        ensure!(peers.contains_key(&id), "CP_NODE_ID is not configured");
        let overrides: BTreeMap<u64, String> =
            serde_json::from_str(&env("CP_PEER_OVERRIDES", "{}"))?;
        for (id, address) in overrides {
            ensure!(
                peers.contains_key(&id) && valid_origin(&address),
                "invalid peer override"
            );
            peers.insert(id, address);
        }
        let cluster = env("CP_CLUSTER_ID", "");
        let secret = env("CP_CLUSTER_TOKEN", "");
        ensure!(
            !cluster.is_empty() && secret.len() >= 32,
            "cluster identity and secret are required"
        );
        ensure!(
            values
                .get("CP_REPLICAS")
                .is_none_or(|n| n.parse::<usize>().ok() == Some(members.len())),
            "CP_REPLICAS and CP_MEMBERS disagree"
        );
        ensure!(
            members
                .iter()
                .map(|m| m.url.trim_end_matches('/'))
                .collect::<std::collections::BTreeSet<_>>()
                .len()
                == members.len(),
            "peer origins must be distinct"
        );
        let tick = number("CP_RAFT_TICK_MS", 500)?;
        let election_ticks = number("CP_RAFT_ELECTION_TICKS", 20)? as usize;
        let heartbeat_ticks = number("CP_RAFT_HEARTBEAT_TICKS", 2)? as usize;
        ensure!(
            (10..=60_000).contains(&tick)
                && heartbeat_ticks > 0
                && election_ticks > heartbeat_ticks,
            "invalid Raft timing"
        );
        Ok(Self {
            id,
            members,
            peers,
            cluster,
            secret,
            tick: Duration::from_millis(tick),
            election_ticks,
            heartbeat_ticks,
        })
    }
}
fn valid_origin(value: &str) -> bool {
    reqwest::Url::parse(value).is_ok_and(|u| {
        matches!(u.scheme(), "http" | "https")
            && u.host_str().is_some()
            && u.username().is_empty()
            && u.password().is_none()
            && u.path() == "/"
            && u.query().is_none()
            && u.fragment().is_none()
    })
}
