use crate::error::http;
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, watch};
use tokio_util::sync::CancellationToken;
struct Run {
    wake: watch::Sender<u64>,
    agent: Option<(String, CancellationToken)>,
    wait: Option<(String, CancellationToken)>,
}
pub struct Subscription {
    pub wake: watch::Receiver<u64>,
    pub cancel: CancellationToken,
    hub: Hub,
    run: String,
    id: String,
    agent: bool,
}
impl Subscription {
    pub fn identity(&self) -> &str {
        &self.run
    }
}
impl Drop for Subscription {
    fn drop(&mut self) {
        let mut runs = self.hub.runs.lock().unwrap();
        if let Some(run) = runs.get_mut(&self.run) {
            let slot = if self.agent {
                &mut run.agent
            } else {
                &mut run.wait
            };
            if slot.as_ref().is_some_and(|(id, _)| *id == self.id) {
                *slot = None;
            }
            if run.agent.is_none() && run.wait.is_none() {
                runs.remove(&self.run);
            }
        }
    }
}
pub struct LogFrame {
    pub text: String,
    _permit: OwnedSemaphorePermit,
}
pub struct Relay {
    pub tx: mpsc::Sender<LogFrame>,
    pub cancel: CancellationToken,
}
#[derive(Clone)]
pub struct Hub {
    runs: Arc<Mutex<BTreeMap<String, Run>>>,
    pub logs: Arc<Mutex<BTreeMap<String, Arc<Relay>>>>,
    log_bytes: Arc<Semaphore>,
    pub cancel: CancellationToken,
}
impl Hub {
    pub fn new(cancel: CancellationToken) -> Self {
        Self {
            runs: Default::default(),
            logs: Default::default(),
            log_bytes: Arc::new(Semaphore::new(512 << 10)),
            cancel,
        }
    }
    pub fn subscribe(&self, run: &str, agent: bool) -> anyhow::Result<Subscription> {
        let mut runs = self.runs.lock().unwrap();
        if runs.len() >= 2048 && !runs.contains_key(run) {
            return Err(http(503, "too many waiting agents"));
        }
        let run_entry = runs.entry(run.into()).or_insert_with(|| Run {
            wake: watch::channel(0).0,
            agent: None,
            wait: None,
        });
        let slot = if agent {
            &mut run_entry.agent
        } else {
            &mut run_entry.wait
        };
        if let Some((_, cancel)) = slot.take() {
            cancel.cancel();
        }
        let cancel = self.cancel.child_token();
        let id = runner_core::random_id();
        *slot = Some((id.clone(), cancel.clone()));
        Ok(Subscription {
            wake: run_entry.wake.subscribe(),
            cancel,
            hub: self.clone(),
            run: run.into(),
            id,
            agent,
        })
    }
    pub fn wake(&self, run: &str) {
        if let Some(r) = self.runs.lock().unwrap().get(run) {
            r.wake.send_modify(|v| *v = v.wrapping_add(1));
        }
    }
    pub fn agents(&self) -> usize {
        self.runs
            .lock()
            .unwrap()
            .values()
            .filter(|r| r.agent.is_some())
            .count()
    }
    pub fn add_viewer(&self, id: &str) -> anyhow::Result<(Arc<Relay>, mpsc::Receiver<LogFrame>)> {
        let mut logs = self.logs.lock().unwrap();
        if logs.contains_key(id) || logs.len() >= 50 {
            return Err(http(409, "log viewer already connected or full"));
        }
        let (tx, rx) = mpsc::channel(16);
        let relay = Arc::new(Relay {
            tx,
            cancel: self.cancel.child_token(),
        });
        logs.insert(id.into(), relay.clone());
        Ok((relay, rx))
    }
    pub fn log(&self, id: &str, text: String) -> bool {
        if text.len() > 128 << 10 {
            return false;
        }
        let relay = self.logs.lock().unwrap().get(id).cloned();
        let Some(relay) = relay else {
            return false;
        };
        let Ok(permit) = self
            .log_bytes
            .clone()
            .try_acquire_many_owned(text.len().max(1) as u32)
        else {
            relay.cancel.cancel();
            return false;
        };
        if relay
            .tx
            .try_send(LogFrame {
                text,
                _permit: permit,
            })
            .is_err()
        {
            relay.cancel.cancel();
            return false;
        }
        true
    }
    pub fn end_log(&self, id: &str) {
        if let Some(relay) = self.logs.lock().unwrap().remove(id) {
            relay.cancel.cancel();
        }
    }
}
