use crate::{
    control::{Control, f, n, s, truncate},
    error::{http, require},
    model::*,
    spec,
};
use serde_json::{Value, json};
use std::{collections::BTreeSet, net::IpAddr, sync::Arc};
impl Control {
    pub fn sync(
        &mut self,
        body: &Value,
        ip: Option<&str>,
        edge: Option<&str>,
        now: u64,
    ) -> anyhow::Result<Value> {
        let machine = n(body, "machine");
        let started = n(body, "started");
        let run = s(body, "run");
        require(
            machine > 0
                && machine < 1 << 53
                && !run.is_empty()
                && run.len() <= 200
                && body["started"].as_u64().is_some(),
            "machine, run and started are required",
        )?;
        require(
            body["region"].is_null()
                || body["region"] == ""
                || body["region"].as_str().is_some_and(spec::region_valid),
            "region must be a lowercase region label",
        )?;
        if let Some(r) = self.runs.get(run) {
            if r.machine != machine || r.started != started {
                return Err(http(
                    409,
                    "run identity does not match its existing incarnation",
                ));
            }
        } else if self.runs.len() >= self.en("MAX_TRACKED_RUNS", 2048) as usize {
            return Err(http(
                503,
                "controller run capacity reached; retry after old incarnations expire",
            ));
        }
        let observed = self.observed_runs.contains(run);
        let receipt = self.read_recovery(body, now)?;
        let previous = self
            .runs
            .get(run)
            .map(|r| json!({"pool":r.pool}))
            .or_else(|| receipt.as_ref().map(|r| r["runRow"].clone()));
        let tag = self.agent_tag(body, previous.as_ref())?;
        if !self.cold_recovery_complete
            && self.recovering_until > 0
            && (!self.tunnels_ready
                || now < self.recovering_until
                || !self.recovery_missing.is_empty())
        {
            self.recover_agent(body, receipt.as_ref(), now)?;
        } else {
            if !observed || self.recovery_missing.contains(run) {
                self.recover_late_agent(body, receipt.as_ref(), now)?;
            }
            if self.cold_recovery_complete {
                self.recover_late_witness(receipt.as_ref(), now)?;
            }
        }
        if now.saturating_sub(self.last_checkin) > self.live_ms {
            self.quiet_since = self.last_checkin;
            self.settle_at = now;
            self.log_event("gap",json!({"cause":"unreachable","detail":format!("no check-in for {}s: the control plane was unreachable; nothing counts as gone until the fleet is back",(now-self.last_checkin+500)/1000)}),now)?;
        }
        self.last_checkin = now;
        let status = self.reported_status(&body["status"], Some(run), Some(machine), now);
        let ready = u64::from(body["ready"] == true);
        let pool = (tag != "standalone").then_some(tag);
        let label = truncate(s(body, "label"), 80);
        let label = (!label.is_empty()).then_some(label);
        let deadline = body["deadline"]
            .as_u64()
            .filter(|d| *d > started + MIN && *d < started + 86400_000);
        let mut r = self.runs.get(run).cloned().unwrap_or_else(|| Run {
            id: run.into(),
            machine,
            started,
            status: Arc::new(json!({})),
            agent: truncate(body["agent"].as_str().unwrap_or(run), 100),
            ..Default::default()
        });
        let existed = self.runs.contains_key(run);
        let changed = ready != r.ready
            || *r.status != status
            || r.pool != pool
            || r.label != label
            || r.deadline != deadline;
        r.status = Arc::new(status);
        r.ready = ready;
        r.seen = now;
        r.pool = pool;
        r.label = label;
        r.deadline = deadline;
        r.set("starts", body["starts"] == true);
        if existed && ready > 0 && r.n("readyAt") == 0 {
            r.set("readyAt", now);
        }
        if let Some(region) = body.get("region") {
            r.set(
                "region",
                region
                    .as_str()
                    .filter(|s| spec::region_valid(s))
                    .map(str::to_owned),
            );
        }
        if !existed || changed {
            self.save_run(r.clone())?;
        } else {
            self.store.exec(
                "UPDATE runs SET seen=? WHERE id=?",
                &[json!(now), json!(r.id)],
            )?;
            self.runs.insert(r.id.clone(), r.clone());
        }
        if let Some(receipt) = receipt
            .as_ref()
            .filter(|receipt| receipt["run"] != run && receipt["run"] == body["previousRun"])
        {
            if let Some(mut previous) = self.runs.get(s(receipt, "run")).cloned().filter(|p| {
                p.agent == r.agent && p.machine == machine && p.started < started && p.retire == 0
            }) {
                previous.retire = 1;
                self.save_run(previous)?;
            }
            self.recovery_missing.remove(s(receipt, "run"));
            self.recovery_missing.remove(run);
        }
        if body["cpus"].as_u64().is_some_and(|v| v > 0 && v <= 1024) {
            r.set("cpus", body["cpus"].clone());
        }
        self.take_metrics(&r, &body["metrics"], now)?;
        self.note_copies(&r, &r.status, now)?;
        if body["disk"].is_object() && f(&body["disk"], "total") > 0. {
            r.set(
                "disk",
                json!({"free":f(&body["disk"],"free"),"total":f(&body["disk"],"total")}),
            );
        }
        if body["edges"].as_i64().is_some() {
            r.set("edges", body["edges"].clone());
        }
        if let Some(rtt) = body["rtt"].as_f64().filter(|v| v.is_finite() && *v >= 0.) {
            r.set("rtt", (rtt.min(60_000.) + 0.5).floor() as u64);
        }
        if let Some(ip) = ip.filter(|s| !s.is_empty()) {
            r.set("ip", ip);
            let location = edge
                .filter(|s| !s.is_empty())
                .map(str::to_owned)
                .or_else(|| self.locate(ip))
                .or_else(|| (!r.s("location").is_empty()).then(|| r.s("location").to_owned()));
            r.set("location", location);
        }
        if body["leaving"] == true && r.retire == 0 {
            r.retire = 1;
            r.set("left", now);
            self.save_run(r.clone())?;
            self.log_event("left",json!({"machine":r.machine,"run":r.id,"cause":"leaving","detail":"the server said it's going away"}),now)?;
        }
        r.set("protocol", n(body, "protocol").max(1));
        let public = body["publicTunnels"]
            .as_object()
            .into_iter()
            .flatten()
            .filter(|(name, id)| {
                self.public_tunnels
                    .get(*name)
                    .is_some_and(|t| t["id"] == **id)
                    && !self.copies_on(name, machine).is_empty()
            })
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect::<serde_json::Map<_, _>>();
        r.set("publicTunnels", public);
        self.observed_runs.insert(run.into());
        if let Some(receipt) = &receipt {
            r.set("recoveryAt", r.n("recoveryAt").max(n(receipt, "at")));
            if !receipt["fleet"].is_null() {
                r.set("witnessAt", r.n("witnessAt").max(n(receipt, "at")));
            }
        }
        self.runs.insert(run.into(), r.clone());
        if self.recovery_active(now) {
            if r.n("protocol") < 2 {
                return Err(http(
                    503,
                    "reconstructing fleet state; keep current workloads",
                ));
            }
            return Ok(
                json!({"hold":true,"domain":self.e("DOMAIN"),"poll":5,"message":"reconstructing fleet state; keep current workloads"}),
            );
        }
        self.cold_recovery_complete = true;
        self.check_mass_loss(now)?;
        self.place(now)?;
        self.settle(now)?;
        self.note_ready(now);
        self.check_sick(now)?;
        self.rebalance(now)?;
        self.check_dark(now)?;
        r = self.runs.get(run).cloned().unwrap_or(r);
        if r.retire == 0 && !self.settling(now) {
            if self.superseded(&r, now) {
                r.retire = 1;
                self.save_run(r.clone())?;
                let successor = self.successor_of(&r, now);
                let mut timing = String::new();
                if r.handover > 0 {
                    if let Some(succ) = successor {
                        let ready_at = succ.n("readyAt");
                        let ready_at = if ready_at == 0 { now } else { ready_at };
                        let queue = succ.started.saturating_sub(r.handover);
                        let build = ready_at.saturating_sub(succ.started);
                        let overlap = now.saturating_sub(ready_at);
                        self.handover_times.push(json!({"t":now,"machine":r.machine,"queue":queue,"build":build,"overlap":overlap}));
                        timing = format!(
                            " (start {}, build {}, overlap {})",
                            short_time(queue),
                            short_time(build),
                            short_time(overlap)
                        );
                    }
                }
                self.log_event("retire",json!({"machine":r.machine,"run":r.id,"cause":"superseded","detail":format!("its successor serves everything it did{timing}")}),now)?;
            } else if r.evict.is_some_and(|v| v > 0) {
                self.progress_evict(&mut r, now)?;
            } else {
                self.retire_stuck_successor(&mut r, now)?;
            }
        }
        self.spread_departures(now);
        let handover = r.retire == 0 && self.wants_handover(&mut r, now)?;
        self.runs.insert(r.id.clone(), r.clone());
        self.follow_copies(now)?;
        self.cleanup(now)?;
        let older = if r.retire == 0 {
            self.predecessor_of(&r, now)
        } else {
            None
        };
        let desired = if r.retire > 0 {
            json!({})
        } else {
            self.desired_for(r.machine, older.is_some(), Some(&r), now)?
        };
        let mut region_pending = Vec::new();
        if let Some(older) = &older {
            for p in self.projects.values().filter(|p| p.enabled > 0) {
                let Some(v) = p.wanted() else {
                    continue;
                };
                let Some(spec) = self.version(&p.name, v)? else {
                    continue;
                };
                for x in self.copies_on(&p.name, r.machine) {
                    let region = spec::replica_region(&spec.policy, x.replica);
                    if region.is_some()
                        && r.region() != region
                        && older.status[&x.key]["s"] == "healthy"
                        && older.status[&x.key]["r"] != false
                    {
                        region_pending.push(x.key);
                    }
                }
            }
        }
        let logs = if r.retire > 0 {
            vec![]
        } else {
            self.log_sessions
                .values()
                .filter(|x| {
                    x["viewer"] == true
                        && x["agent"] != true
                        && x["machine"] == r.machine
                        && r.status.get(s(x, "key")).is_some()
                })
                .map(|x| json!({"session":x["id"],"key":x["key"],"tail":200}))
                .collect()
        };
        let mut plan = json!({"domain":self.e("DOMAIN"),"poll":self.poll_s,"desired":desired,"retire":r.retire>0,"handover":handover,"start":[]});
        if !self.e("STATE_SIGNING_KEY").is_empty() {
            plan["recovery"] = self.recovery_for(&r, now)?;
        }
        if !logs.is_empty() {
            plan["logs"] = json!(logs);
        }
        if r.n("protocol") >= 5 {
            plan["publicTunnels"] = self.public_tunnels_for(&r, &desired, now);
        }
        if let Some(older) = older {
            let mut serve = desired
                .as_object()
                .into_iter()
                .flat_map(|m| m.keys())
                .filter(|key| {
                    older.status[*key]["s"] == "healthy" && older.status[*key]["r"] != false
                })
                .cloned()
                .collect::<Vec<_>>();
            for key in region_pending {
                if !serve.contains(&key) {
                    serve.push(key);
                }
            }
            plan["successor"] = json!(true);
            plan["serve"] = json!(serve);
        }
        Ok(plan)
    }
    pub fn locate(&mut self, ip: &str) -> Option<String> {
        let address = ip.parse::<IpAddr>().ok()?;
        let private = match address {
            IpAddr::V4(ip) => {
                ip.is_private() || ip.is_loopback() || ip.is_link_local() || ip.is_unspecified()
            }
            IpAddr::V6(ip) => {
                ip.is_loopback()
                    || ip.is_unique_local()
                    || ip.is_unicast_link_local()
                    || ip.is_unspecified()
            }
        };
        if private {
            return None;
        }
        if let Some(hit) = self.geo.get(ip) {
            return hit["text"].as_str().map(str::to_owned);
        }
        if self.geo_pending.len() < 20 && self.geo_pending.insert(ip.into()) {
            self.geo_lookups.push(ip.into());
        }
        None
    }
    pub fn record_location(
        &mut self,
        ip: &str,
        location: Option<String>,
        now: u64,
    ) -> anyhow::Result<()> {
        self.geo_pending.remove(ip);
        self.geo
            .insert(ip.into(), json!({"ip":ip,"text":location,"at":now}));
        if let Some(location) = location {
            self.store.exec(
                "INSERT OR REPLACE INTO geo(ip,text,at) VALUES(?,?,?)",
                &[json!(ip), json!(location), json!(now)],
            )?;
            for r in self.runs.values_mut() {
                if r.s("ip") == ip {
                    r.set("location", &location);
                }
            }
        }
        Ok(())
    }
    pub fn cleanup(&mut self, now: u64) -> anyhow::Result<()> {
        self.flush_metrics(now)?;
        self.log_sessions
            .retain(|_, s| s["viewer"] == true || now.saturating_sub(n(s, "created")) <= 30_000);
        if now.saturating_sub(self.last_rollup) > MIN {
            self.last_rollup = now;
            self.rollup_metrics(now)?;
        }
        if now.saturating_sub(self.last_cleanup) < 10 * MIN {
            return Ok(());
        }
        self.last_cleanup = now;
        let memory = self.en("MEMORY_HISTORY_MIN", 0) * MIN;
        let keep = if memory > 0 { memory } else { 48 * 60 * MIN };
        self.store.exec(
            "DELETE FROM metrics_1m WHERE t<?",
            &[json!(now.saturating_sub(keep))],
        )?;
        self.store.exec(
            "DELETE FROM metrics_10m WHERE t<? AND t<?",
            &[
                json!(now.saturating_sub(3 * 24 * 60 * MIN)),
                json!(self.setting_number("rolled60")),
            ],
        )?;
        self.store.exec(
            "DELETE FROM metrics_1h WHERE t<?",
            &[json!(now.saturating_sub(30 * 24 * 60 * MIN))],
        )?;
        if memory > 0 {
            for table in ["metrics_10m", "metrics_1h"] {
                self.store.exec(
                    &format!("DELETE FROM {table} WHERE t<?"),
                    &[json!(now.saturating_sub(memory))],
                )?;
            }
        }
        self.store.exec(
            "DELETE FROM geo WHERE at<?",
            &[json!(now.saturating_sub(30 * 86400_000))],
        )?;
        self.geo
            .retain(|_, v| now.saturating_sub(n(v, "at")) <= 30 * 86400_000);
        self.store.exec(
            "DELETE FROM events WHERE t<?",
            &[json!(now.saturating_sub(if memory > 0 {
                6 * 60 * MIN
            } else {
                30 * 86400_000
            }))],
        )?;
        self.store.exec(
            "DELETE FROM events WHERE id NOT IN (SELECT id FROM events ORDER BY id DESC LIMIT ?)",
            &[json!(self.en("EVENT_HISTORY_ROWS", 20_000))],
        )?;
        let gone = self
            .runs
            .values()
            .filter(|r| now.saturating_sub(r.seen) > 120 * MIN)
            .map(|r| r.id.clone())
            .collect::<Vec<_>>();
        for id in gone {
            self.runs.remove(&id);
            self.observed_runs.remove(&id);
            self.recovery_missing.remove(&id);
            self.store
                .exec("DELETE FROM runs WHERE id=?", &[json!(id)])?;
        }
        let ids = self.runs.keys().cloned().collect::<BTreeSet<_>>();
        self.diagnostics.retain(|_, v| ids.contains(s(v, "run")));
        self.last_healthy
            .retain(|k, _| k.rsplit_once('|').is_some_and(|(id, _)| ids.contains(id)));
        self.first_seen
            .retain(|k, _| k.rsplit_once('|').is_some_and(|(id, _)| ids.contains(id)));
        self.last_v
            .retain(|k, _| k.rsplit_once('|').is_some_and(|(id, _)| ids.contains(id)));
        self.minutes.retain(|id, _| ids.contains(id));
        self.restarts.retain(|id, _| ids.contains(id));
        self.low_disk_since.retain(|id, _| ids.contains(id));
        self.strikes.retain(|id, _| ids.contains(id));
        self.sick_why.retain(|id, _| ids.contains(id));
        self.due_wait.retain(|id, _| ids.contains(id));
        self.loss_noted.retain(|id| ids.contains(id));
        self.pulled.retain(|id| ids.contains(id));
        self.live_metrics.retain(|_, v| ids.contains(s(v, "run")));
        self.handover_times
            .retain(|v| now.saturating_sub(n(v, "t")) < 24 * 60 * MIN);
        let projects = self.projects.clone();
        let before = self.version_fails.len();
        self.version_fails.retain(|k, _| {
            k.rsplit_once('@')
                .and_then(|(name, v)| v.parse::<u64>().ok().map(|v| (name, v)))
                .is_some_and(|(name, v)| {
                    projects
                        .get(name)
                        .is_some_and(|p| p.version == v || p.stable == Some(v))
                })
        });
        if before != self.version_fails.len() {
            self.set_setting("version_fails", serde_json::to_string(&self.version_fails)?)?;
        }
        self.failures
            .retain(|_, (_, at)| now.saturating_sub(*at) < 15 * MIN);
        self.failed_on.retain(|_, until| *until > now);
        self.plain_drops
            .retain(|_, (at, _)| now.saturating_sub(*at) < 10 * MIN);
        self.noted.retain(|_, at| {
            at.as_u64()
                .is_some_and(|at| now.saturating_sub(at) < 30 * MIN)
        });
        self.alerted_at
            .retain(|_, at| now.saturating_sub(*at) < 10 * MIN);
        self.capped_until.retain(|_, at| *at > now);
        self.holds
            .retain(|_, h| now.saturating_sub(n(h, "at")) < 2 * MIN);
        self.samples.retain(|_, samples| {
            samples.retain(|v| now.saturating_sub(n(v, "t")) <= 5 * MIN);
            !samples.is_empty()
        });
        self.store
            .trim_metrics(self.en("METRICS_HISTORY_BYTES", 24 << 20))?;
        Ok(())
    }
}
fn short_time(ms: u64) -> String {
    if ms < 90_000 {
        format!("{} s", (ms + 500) / 1000)
    } else {
        format!("{} min", (ms + MIN / 2) / MIN)
    }
}
