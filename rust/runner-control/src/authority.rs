use runner_raft::service::RaftService;
use serde::{Deserialize, Serialize};
use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Proof {
    pub cluster: String,
    pub node: u64,
    pub boot: String,
    pub term: u64,
    pub epoch: u64,
}
struct Lease {
    proof: Proof,
    deadline: Instant,
    wall_deadline: u64,
}
#[derive(Clone)]
pub struct Authority {
    raft: RaftService,
    cluster: String,
    slot: u64,
    current: Arc<Mutex<Option<Lease>>>,
}
impl Authority {
    pub fn new(raft: RaftService, cluster: String, slot: u64) -> Self {
        Self {
            raft,
            cluster,
            slot,
            current: Arc::new(Mutex::new(None)),
        }
    }
    pub fn clear(&self) {
        *self.current.lock().unwrap() = None;
    }
    pub fn live(&self) -> Option<Proof> {
        self.current
            .lock()
            .unwrap()
            .as_ref()
            .filter(|l| Instant::now() < l.deadline && runner_core::now_ms() < l.wall_deadline)
            .map(|l| l.proof.clone())
    }
    pub async fn renew(&self) -> anyhow::Result<Proof> {
        let start = Instant::now();
        let wall = runner_core::now_ms();
        let raft = self.raft.verify().await?;
        anyhow::ensure!(
            raft.id == self.slot && raft.leader == raft.id && raft.identity_valid,
            "Raft leadership unavailable"
        );
        let proof = Proof {
            cluster: self.cluster.clone(),
            node: raft.id,
            boot: raft.boot.clone(),
            term: raft.term,
            epoch: raft.term,
        };
        *self.current.lock().unwrap() = Some(Lease {
            proof: proof.clone(),
            deadline: start + Duration::from_secs(14),
            wall_deadline: wall + 14_000,
        });
        if self.live().is_none() {
            self.clear();
            anyhow::bail!("leadership confirmation expired in transit");
        }
        Ok(proof)
    }
    pub async fn check(&self, expected: Option<&Proof>) -> anyhow::Result<Proof> {
        let before = self
            .live()
            .ok_or_else(|| anyhow::anyhow!("leadership lease unavailable"))?;
        let raft = self.raft.verify().await?;
        anyhow::ensure!(
            self.live().as_ref() == Some(&before)
                && raft.leader == before.node
                && raft.id == before.node
                && raft.boot == before.boot
                && raft.term == before.term
                && raft.identity_valid,
            "leadership changed"
        );
        anyhow::ensure!(
            expected.is_none_or(|p| p == &before),
            "obsolete control-plane authority"
        );
        Ok(before)
    }
}
