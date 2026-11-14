use crate::{
    authority::Authority,
    client_ip::ProxyConfig,
    control::{n, s},
    service::Service,
    tail::Tail,
};
use runner_raft::{config::Config, service::RaftService};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
pub struct Node {
    pub env: Arc<BTreeMap<String, String>>,
    pub config: Option<Config>,
    pub raft: Option<RaftService>,
    pub authority: Option<Authority>,
    pub current: watch::Sender<Option<Service>>,
    pub tail: Arc<Mutex<Tail>>,
    peers: Mutex<BTreeMap<u64, (u64, Value)>>,
    pub incarnation: String,
    pub started: u64,
    pub draining: AtomicBool,
    pub recovering: AtomicBool,
    pub tunnel_ready: AtomicBool,
    pub error: Mutex<Option<String>>,
    pub cancel: CancellationToken,
    pub client: reqwest::Client,
    pub proxy: ProxyConfig,
    pub bodies: Arc<tokio::sync::Semaphore>,
}
impl Node {
    pub fn new(
        env: BTreeMap<String, String>,
        config: Option<Config>,
        cancel: CancellationToken,
    ) -> anyhow::Result<Arc<Self>> {
        let raft = config
            .as_ref()
            .map(|c| RaftService::start(c.clone(), cancel.child_token()))
            .transpose()?;
        let authority = config
            .as_ref()
            .zip(raft.as_ref())
            .map(|(c, r)| Authority::new(r.clone(), c.cluster.clone(), c.id));
        let proxy = ProxyConfig::new(&env)?;
        Ok(Arc::new(Self {
            env: Arc::new(env),
            config,
            raft,
            authority,
            current: watch::channel(None).0,
            tail: Default::default(),
            peers: Default::default(),
            incarnation: runner_core::random_id(),
            started: runner_core::now_ms(),
            draining: AtomicBool::new(false),
            recovering: AtomicBool::new(false),
            tunnel_ready: AtomicBool::new(false),
            error: Default::default(),
            cancel,
            client: runner_core::http_client(Duration::from_secs(65))?,
            proxy,
            bodies: Arc::new(tokio::sync::Semaphore::new(4)),
        }))
    }
    pub fn e(&self, key: &str) -> &str {
        self.env.get(key).map(String::as_str).unwrap_or("")
    }
    pub fn service(&self) -> Option<Service> {
        if self.draining.load(Ordering::Relaxed)
            || self.raft.as_ref().is_some_and(|r| {
                let v = r.view();
                !v.identity_valid || v.leader != v.id || v.role != "StateLeader"
            })
        {
            return None;
        }
        self.current
            .borrow()
            .as_ref()
            .filter(|s| {
                !s.cancel.is_cancelled()
                    && *s.ready.borrow()
                    && (self.authority.is_none()
                        || self.authority.as_ref().unwrap().live().is_some())
            })
            .cloned()
    }
    pub fn local_view(&self) -> Value {
        let config = self.config.as_ref();
        let own = config.and_then(|c| c.members.iter().find(|m| m.id == c.id));
        let tail = self.tail.lock().unwrap();
        json!({"id":config.map(|c|c.id).unwrap_or(1),"label":own.map(|m|&m.label),"origin":own.map(|m|&m.url),"incarnation":self.incarnation,"started":self.started,"observedAt":runner_core::now_ms(),"raft":self.raft.as_ref().map(|r|r.view()),"primary":self.service().is_some(),"authority":self.authority.as_ref().and_then(|a|a.live()),"recovering":self.recovering.load(Ordering::Relaxed)||self.current.borrow().as_ref().is_some_and(|s|!*s.ready.borrow()),"tunnelReady":self.tunnel_ready.load(Ordering::Relaxed),"error":*self.error.lock().unwrap(),"memoryBytes":runner_core::rss_bytes(),"rlog":tail.stats(),"transitions":tail.history(),"coordinationStorage":"memory","runtime":"rust"})
    }
    pub fn cluster_view(&self) -> Value {
        let Some(c) = &self.config else {
            return json!({"mode":"single","runtime":"rust","self":1,"count":1,"quorum":1});
        };
        let now = runner_core::now_ms();
        let own = self.local_view();
        let peers = self.peers.lock().unwrap();
        let view = self.raft.as_ref().unwrap().view();
        json!({"mode":"raft","self":c.id,"count":c.members.len(),"quorum":c.members.len()/2+1,"leader":view.leader,"term":view.term,"observedAt":now,"replicas":c.members.iter().map(|m|{let row=peers.get(&m.id);let local=m.id==c.id;json!({"id":m.id,"url":m.url,"label":m.label,"reachable":local||row.is_some_and(|(at,_)|now.saturating_sub(*at)<15_000),"lastSeen":if local{Some(now)}else{row.map(|(at,_)|*at)},"view":if local{Some(&own)}else{row.map(|(_,v)|v)}})}).collect::<Vec<_>>(),"transitions":self.tail.lock().unwrap().history(),"persistence":{"configuration":"D1","electionMetadata":"memory","rlog":"bounded compressed memory only","transitions":"latest 30 in memory"}})
    }
    pub fn destination(&self, forwarded: bool) -> Option<String> {
        if forwarded {
            return None;
        }
        let c = self.config.as_ref()?;
        let leader = self.raft.as_ref()?.view().leader;
        if leader == c.id {
            return None;
        }
        c.peers.get(&leader).cloned()
    }
    fn demote(&self) {
        if let Some(s) = self.current.send_replace(None) {
            s.cancel.cancel();
        }
        if let Some(a) = &self.authority {
            a.clear();
        }
    }
    pub fn start(self: &Arc<Self>) {
        let node = self.clone();
        tokio::spawn(async move {
            node.manage().await;
        });
        if self.config.is_some() {
            let node = self.clone();
            tokio::spawn(async move {
                node.poll_peers().await;
            });
        }
    }
    async fn manage(self: Arc<Self>) {
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut next_renew = 0;
        let mut retry = 0;
        let mut transition = String::new();
        let mut start: Option<tokio::task::JoinHandle<anyhow::Result<Service>>> = None;
        let mut startup_cancel: Option<CancellationToken> = None;
        loop {
            tokio::select! {_=self.cancel.cancelled()=>break,_=interval.tick()=>{}}
            let now = runner_core::now_ms();
            let mut leader = !self.draining.load(Ordering::Relaxed);
            if let Some(raft) = &self.raft {
                let view = raft.view();
                if view.leader > 0 {
                    let id = format!(
                        "{}:{}:{}",
                        self.config.as_ref().unwrap().cluster,
                        view.term,
                        view.leader
                    );
                    if id != transition {
                        transition = id.clone();
                        let _=self.tail.lock().unwrap().append("transition",&json!({"id":id,"at":now,"term":view.term,"leader":view.leader,"observedBy":view.id}),now);
                    }
                }
                leader = leader
                    && view.identity_valid
                    && view.role == "StateLeader"
                    && view.leader == view.id;
                if leader && now >= next_renew {
                    next_renew = now + 5000;
                    match self.authority.as_ref().unwrap().renew().await {
                        Ok(proof) => {
                            let changed = self
                                .current
                                .borrow()
                                .as_ref()
                                .and_then(|s| s.authority.as_ref())
                                .is_some_and(|(_, old)| *old != proof);
                            if changed {
                                if let Some(s) = self.current.send_replace(None) {
                                    s.cancel.cancel();
                                }
                            }
                        }
                        Err(_) => leader = false,
                    }
                }
            }
            if !leader {
                self.demote();
                next_renew = 0;
                if let Some(task) = start.take() {
                    task.abort();
                }
                if let Some(cancel) = startup_cancel.take() {
                    cancel.cancel();
                }
                self.recovering.store(false, Ordering::Relaxed);
                continue;
            }
            if start.as_ref().is_some_and(|t| t.is_finished()) {
                match start.take().unwrap().await {
                    Ok(Ok(service)) => {
                        if service.check().await.is_ok() {
                            self.current.send_replace(Some(service));
                            *self.error.lock().unwrap() = None;
                        } else {
                            service.cancel.cancel();
                            retry = now + 5000;
                        }
                    }
                    _ => {
                        retry = now + 10_000;
                        *self.error.lock().unwrap() =
                            Some("configuration recovery unavailable".into());
                    }
                }
                startup_cancel = None;
                self.recovering.store(false, Ordering::Relaxed);
            }
            let stopped = self
                .current
                .borrow()
                .as_ref()
                .is_some_and(|s| s.cancel.is_cancelled());
            if stopped {
                self.current.send_replace(None);
                retry = now + 5000;
            }
            if self.current.borrow().is_none() && start.is_none() && now >= retry {
                let expected = self
                    .authority
                    .as_ref()
                    .and_then(|a| a.live().map(|p| (a.clone(), p)));
                if self.authority.is_some() && expected.is_none() {
                    continue;
                }
                let env = (*self.env).clone();
                let tail = self.tail.clone();
                let cancel = self.cancel.child_token();
                startup_cancel = Some(cancel.clone());
                self.recovering.store(true, Ordering::Relaxed);
                start = Some(tokio::spawn(async move {
                    Service::open(env, expected, tail, cancel).await
                }));
            }
        }
        self.demote();
        if let Some(task) = start {
            task.abort();
        }
        if let Some(cancel) = startup_cancel {
            cancel.cancel();
        }
    }
    async fn peer_json(&self, url: &str, limit: usize) -> anyhow::Result<Value> {
        let c = self.config.as_ref().unwrap();
        runner_core::response_json(
            self.client
                .get(url)
                .header("x-cp-token", &c.secret)
                .header("x-cp-from", c.id)
                .timeout(Duration::from_secs(5))
                .send()
                .await?,
            limit,
        )
        .await
    }
    async fn poll_peers(self: Arc<Self>) {
        use futures_util::{StreamExt, stream};
        let c = self.config.as_ref().unwrap();
        let mut interval = tokio::time::interval(Duration::from_secs(3));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut cursors: BTreeMap<u64, (String, u64)> = BTreeMap::new();
        loop {
            tokio::select! {_=self.cancel.cancelled()=>break,_=interval.tick()=>{}}
            let ids = c
                .members
                .iter()
                .filter(|m| m.id != c.id)
                .map(|m| m.id)
                .collect::<Vec<_>>();
            let responses = stream::iter(ids.into_iter().map(|id| {
                let node = &self;
                async move {
                    (
                        id,
                        node.peer_json(&format!("{}/_cp/status", c.peers[&id]), 128 << 10)
                            .await,
                    )
                }
            }))
            .buffer_unordered(4)
            .collect::<Vec<_>>()
            .await;
            for (id, result) in responses {
                if let Ok(view) = result {
                    if n(&view, "id") != id {
                        continue;
                    }
                    for t in view["transitions"]
                        .as_array()
                        .into_iter()
                        .flatten()
                        .take(30)
                    {
                        self.tail.lock().unwrap().transition(t);
                    }
                    self.peers
                        .lock()
                        .unwrap()
                        .insert(id, (runner_core::now_ms(), view));
                }
            }
            let leader = self.raft.as_ref().unwrap().view().leader;
            if leader == c.id || !c.peers.contains_key(&leader) {
                continue;
            }
            let cursor = cursors.get(&leader).cloned().unwrap_or_default();
            let origin = &c.peers[&leader];
            let result = async {
                let mut batch = self
                    .peer_json(&format!("{origin}/_cp/tail?after={}", cursor.1), 9 << 20)
                    .await?;
                if s(&batch, "incarnation") != cursor.0 {
                    batch = self
                        .peer_json(&format!("{origin}/_cp/tail?after=0"), 9 << 20)
                        .await?;
                }
                let events = batch["events"]
                    .as_array()
                    .ok_or_else(|| anyhow::anyhow!("invalid runtime tail"))?;
                anyhow::ensure!(events.len() <= 64, "oversized runtime tail");
                for e in events {
                    self.tail
                        .lock()
                        .unwrap()
                        .append(s(e, "kind"), &e["data"], n(e, "at"))?;
                }
                if batch["gap"] == true {
                    let recovery = self
                        .peer_json(&format!("{origin}/_cp/recovery"), 9 << 20)
                        .await?;
                    for report in recovery["reports"]
                        .as_array()
                        .into_iter()
                        .flatten()
                        .take(128)
                    {
                        self.tail.lock().unwrap().append(
                            "report",
                            &report["body"],
                            n(report, "at"),
                        )?;
                    }
                }
                Ok::<_, anyhow::Error>((
                    s(&batch, "incarnation").to_owned(),
                    events
                        .last()
                        .map(|e| n(e, "sequence"))
                        .unwrap_or(n(&batch, "sequence")),
                ))
            }
            .await;
            if let Ok(cursor) = result {
                cursors.insert(leader, cursor);
            }
        }
    }
}
