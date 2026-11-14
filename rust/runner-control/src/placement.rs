use crate::{
    control::{Control, f, n, s, truncate},
    error::{HttpError, http},
    model::*,
    spec::replica_region,
};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};

pub enum MoveOutcome {
    Waiting,
    Done,
    Cancel {
        reason: String,
        no_new: bool,
        failed: bool,
    },
}
impl Control {
    pub fn copy_claims(&self, name: &str, x: &Copy) -> anyhow::Result<Vec<String>> {
        let Some(p) = self.projects.get(name) else {
            return Ok(vec![]);
        };
        let Some(spec) = self.version(name, p.version)? else {
            return Ok(vec![]);
        };
        Ok(spec
            .claims
            .iter()
            .map(|c| {
                c.strip_prefix("port ")
                    .and_then(|p| p.parse::<u32>().ok())
                    .and_then(|p| x.ports.as_ref().and_then(|m| m.get(&p)))
                    .map(|p| format!("port {p}"))
                    .unwrap_or_else(|| c.clone())
            })
            .collect())
    }
    pub fn clash(&self, name: &str, machine: u64) -> anyhow::Result<Option<(String, String)>> {
        let Some(p) = self.projects.get(name) else {
            return Ok(None);
        };
        let Some(spec) = self.version(name, p.version)? else {
            return Ok(None);
        };
        for (other, list) in &self.copies {
            for x in list.iter().filter(|x| x.machine == machine) {
                for c in self.copy_claims(other, x)? {
                    if (c.starts_with("container name ") || spec.fixed.is_some())
                        && spec.claims.contains(&c)
                    {
                        return Ok(Some((c, other.clone())));
                    }
                }
            }
        }
        Ok(None)
    }
    pub fn moved_ports(
        &self,
        spec: &Version,
        machine: u64,
        k: u32,
    ) -> anyhow::Result<BTreeMap<u32, u32>> {
        let mut taken = BTreeSet::from([2019, 19080, 19081, 19082]);
        for (other, list) in &self.copies {
            for x in list.iter().filter(|x| x.machine == machine) {
                for c in self.copy_claims(other, x)? {
                    if let Some(p) = c.strip_prefix("port ").and_then(|p| p.parse::<u32>().ok()) {
                        taken.insert(p);
                    }
                }
            }
        }
        let mut ports = BTreeMap::new();
        if spec.fixed.is_some() {
            return Ok(ports);
        }
        for c in &spec.claims {
            let Some(p) = c.strip_prefix("port ").and_then(|p| p.parse::<u32>().ok()) else {
                continue;
            };
            let mut q = p;
            if taken.contains(&q) {
                q = 30000 + ((p as u64 * 131 + k as u64 * 7919) % 25000) as u32;
                let initial = q;
                while taken.contains(&q) || ports.values().any(|p| *p == q) {
                    q = if q + 1 < 55000 { q + 1 } else { 30000 };
                    anyhow::ensure!(q != initial, "no free workload host port");
                }
            }
            ports.insert(p, q);
        }
        Ok(ports)
    }
    pub fn new_copy(
        &self,
        name: &str,
        spec: &Version,
        k: u32,
        best: &Candidate,
        now: u64,
        reason: &str,
        cause: &str,
    ) -> anyhow::Result<Copy> {
        let mut x = Copy {
            machine: best.machine,
            replica: k,
            since: now,
            reason: reason.into(),
            leaving: None,
            ports: None,
            key: self.free_key(name, best.machine, k, now),
            cause: Some(cause.into()),
        };
        let ports = self.moved_ports(spec, best.machine, k)?;
        let moved = ports
            .iter()
            .filter(|(a, b)| a != b)
            .map(|(a, b)| format!("{a}→{b}"))
            .collect::<Vec<_>>();
        if !moved.is_empty() {
            x.reason = format!(
                "{}port{} {} moved aside; {reason}",
                if best.doubling > 0 {
                    format!(
                        "copy {} on this machine, every machine having one; ",
                        best.doubling + 1
                    )
                } else {
                    String::new()
                },
                if moved.len() > 1 { "s" } else { "" },
                moved.join(", ")
            );
            x.ports = Some(ports);
        } else if best.doubling > 0 {
            x.reason = format!(
                "copy {} on this server, every server having one; {reason}",
                best.doubling + 1
            );
        }
        Ok(x)
    }
    pub fn headroom(&self, machine: u64, now: u64) -> Room {
        let samples = self
            .samples
            .get(&machine)
            .map(Vec::as_slice)
            .unwrap_or(&[])
            .iter()
            .filter(|x| now.saturating_sub(n(x, "t")) <= 3 * MIN)
            .collect::<Vec<_>>();
        let m = self.live_metrics.get(&machine);
        let mut cpu = 50.;
        let mut mem = 50.;
        if !samples.is_empty() {
            cpu = samples.iter().map(|x| f(x, "cpu")).sum::<f64>() / samples.len() as f64;
            mem = samples.iter().map(|x| f(x, "mem")).sum::<f64>() / samples.len() as f64;
        } else if let Some(m) = m.filter(|m| {
            now.saturating_sub(n(m, "t")) < 3 * self.live_ms && f(&m["h"], "memTotal") > 0.
        }) {
            cpu = f(&m["h"], "cpu");
            mem = 100. * f(&m["h"], "memUsed") / f(&m["h"], "memTotal");
        }
        Room {
            cpu,
            mem,
            h: (70. - cpu).min(80. - mem),
            measured: !samples.is_empty() || m.is_some(),
        }
    }
    pub fn placement_counts(&self) -> BTreeMap<u64, usize> {
        let mut counts = BTreeMap::new();
        for x in self
            .copies
            .values()
            .flatten()
            .filter(|x| x.leaving.is_none())
        {
            *counts.entry(x.machine).or_default() += 1;
        }
        counts
    }
    pub fn starting_counts(&self, up: &BTreeMap<u64, Run>) -> BTreeMap<u64, usize> {
        let mut counts = BTreeMap::new();
        for (name, list) in &self.copies {
            let Some(p) = self.projects.get(name).filter(|p| p.enabled > 0) else {
                continue;
            };
            let Some(version) = p.wanted() else {
                continue;
            };
            for x in list {
                if let Some(r) = up.get(&x.machine) {
                    let st = &r.status[&x.key];
                    if x.leaving.is_none()
                        && (st.is_null()
                            || st["v"] != version
                            || !["healthy", "failed"].contains(&s(st, "s")))
                    {
                        *counts.entry(x.machine).or_default() += 1;
                    }
                }
            }
        }
        counts
    }
    pub fn best_machine(
        &self,
        name: &str,
        up: &BTreeMap<u64, Run>,
        counts: &BTreeMap<u64, usize>,
        now: u64,
        replica: u32,
    ) -> anyhow::Result<Option<Candidate>> {
        let Some(p) = self.projects.get(name) else {
            return Ok(None);
        };
        let Some(spec) = self.version(name, p.version)? else {
            return Ok(None);
        };
        let region = replica_region(&spec.policy, replica);
        let mut mine: BTreeMap<u64, usize> = BTreeMap::new();
        for x in self.copies_of(name) {
            *mine.entry(x.machine).or_default() += 1;
        }
        let live = self.live_machines(now);
        let mut tags: BTreeMap<String, BTreeSet<u32>> = BTreeMap::new();
        if spec.policy.spread_tags {
            for x in self
                .copies_of(name)
                .iter()
                .filter(|x| x.leaving.is_none() && x.replica != replica)
            {
                if let Some(r) = up.get(&x.machine).or_else(|| live.get(&x.machine)) {
                    tags.entry(r.tag().into()).or_default().insert(x.replica);
                }
            }
        }
        let mut on_slot: BTreeMap<u64, usize> = BTreeMap::new();
        for r in self.live_runs(now) {
            *on_slot.entry(r.machine).or_default() += 1;
        }
        let starting = self.starting_counts(up);
        let mut candidates = Vec::new();
        for r in up.values() {
            let r = self.runs.get(&r.id).unwrap_or(r);
            if region.is_some() && r.region() != region {
                continue;
            }
            let doubling = mine.get(&r.machine).copied().unwrap_or(0);
            if self.clash(name, r.machine)?.is_some() || doubling > 0 && spec.nodup.is_some() {
                continue;
            }
            if r.drain > 0
                || r.handover > 0
                || r.retire > 0
                || r.evict.is_some_and(|v| v > 0)
                || on_slot.get(&r.machine).copied().unwrap_or(0) > 1
            {
                continue;
            }
            if r.quarantine.unwrap_or(0) > now
                || self.failed_here(name, r.machine, now)
                || starting.get(&r.machine).copied().unwrap_or(0) >= 3
            {
                continue;
            }
            let due = self.due_at(r);
            if due.is_some_and(|due| now >= due) {
                continue;
            }
            let room = self.headroom(r.machine, now);
            let placed = counts.get(&r.machine).copied().unwrap_or(0);
            candidates.push(Candidate {
                machine: r.machine,
                ready: r.ready,
                doubling,
                tag_copies: tags.get(r.tag()).map(BTreeSet::len).unwrap_or(0),
                placed,
                h: room.h,
                roomy: room.h > 0.,
                life_ok: r
                    .deadline
                    .unwrap_or(u64::MAX)
                    .min(due.unwrap_or(u64::MAX))
                    .saturating_sub(now)
                    >= 30 * MIN,
                hot: room.cpu > 85. || room.mem > 90.,
                reason: format!(
                    "{}, {placed} other {} placed",
                    if room.measured {
                        format!(
                            "cpu {:.0}%, memory {:.0}%",
                            (room.cpu + 0.5).floor(),
                            (room.mem + 0.5).floor()
                        )
                    } else {
                        "no metrics yet".into()
                    },
                    if placed == 1 { "copy" } else { "copies" }
                ),
            });
        }
        let healthy = candidates.iter().any(|c| c.life_ok && !c.hot);
        let life = candidates.iter().any(|c| c.life_ok);
        candidates.retain(|c| {
            if healthy {
                c.life_ok && !c.hot
            } else if life {
                c.life_ok
            } else {
                true
            }
        });
        candidates.sort_by(|a, b| {
            b.ready
                .cmp(&a.ready)
                .then(a.doubling.cmp(&b.doubling))
                .then(a.tag_copies.cmp(&b.tag_copies))
                .then(b.roomy.cmp(&a.roomy))
                .then(a.placed.cmp(&b.placed))
                .then(b.h.total_cmp(&a.h))
        });
        Ok(candidates.into_iter().next())
    }
    pub fn failed_here(&self, name: &str, machine: u64, now: u64) -> bool {
        self.failed_on
            .get(&format!("{name}|{machine}"))
            .is_some_and(|until| *until > now)
    }
    pub fn copy_healthy(
        &self,
        name: &str,
        x: &Copy,
        run: Option<&Run>,
        version: Option<u64>,
    ) -> bool {
        let Some(run) = run else {
            return false;
        };
        let st = &run.status[&x.key];
        let v = version.or_else(|| self.projects.get(name).and_then(Project::wanted));
        st["s"] == "healthy" && st["r"] != false && v.is_some_and(|v| st["v"] == v)
    }
    pub fn any_ready(&self, name: &str, up: &BTreeMap<u64, Run>, except: Option<u32>) -> bool {
        self.copies_of(name).iter().any(|x| {
            x.leaving.is_none()
                && Some(x.replica) != except
                && self.copy_healthy(name, x, up.get(&x.machine), None)
        })
    }
    pub fn built_at(&self, x: &Copy, r: Option<&Run>) -> u64 {
        x.since.max(
            r.map(|r| {
                r.status[&x.key]["t0"]
                    .as_u64()
                    .filter(|v| *v > 0)
                    .unwrap_or(r.started)
            })
            .unwrap_or(0),
        )
    }
    pub fn budget(&self, name: &str) -> anyhow::Result<usize> {
        Ok(match self.projects.get(name) {
            Some(p) => self
                .version(name, p.version)?
                .map(|v| (v.replicas as usize).div_ceil(5).max(1))
                .unwrap_or(1),
            None => 1,
        })
    }
    pub fn transitions(
        &mut self,
        name: &str,
        now: u64,
        count_missing: bool,
        count_lost: bool,
        exclude: Option<&str>,
        exclude_replica: Option<u32>,
    ) -> anyhow::Result<usize> {
        let Some(p) = self.projects.get(name) else {
            return Ok(0);
        };
        let Some(spec) = self.version(name, p.version)? else {
            return Ok(0);
        };
        let up = self.live_machines(now);
        let mut count = 0;
        for k in 1..=spec.replicas {
            if Some(k) == exclude_replica {
                continue;
            }
            let copies = self
                .copies_of(name)
                .iter()
                .filter(|x| x.replica == k)
                .cloned()
                .collect::<Vec<_>>();
            let key = format!("{name}|{k}");
            if copies.is_empty() {
                if count_missing
                    && self
                        .missing_at
                        .get(&key)
                        .is_some_and(|t| now.saturating_sub(*t) < 30 * MIN)
                {
                    count += 1;
                }
                continue;
            }
            self.missing_at.remove(&key);
            if copies.iter().any(|x| x.leaving.is_some()) {
                count += 1;
                continue;
            }
            let x = &copies[0];
            let Some(run) = up.get(&x.machine) else {
                if count_lost {
                    count += 1;
                }
                continue;
            };
            if !self.copy_healthy(name, x, Some(run), None) {
                let since = *self.unhealthy_since.entry(key).or_insert(now);
                if now.saturating_sub(since) < 30 * MIN {
                    count += 1;
                }
                continue;
            }
            self.unhealthy_since.remove(&key);
            if (run.handover > 0 || run.evict.is_some_and(|v| v > 0))
                && Some(run.id.as_str()) != exclude
            {
                count += 1;
            }
        }
        Ok(count)
    }
    pub fn may_transition(
        &mut self,
        name: &str,
        now: u64,
        exclude: Option<&str>,
        replica: Option<u32>,
    ) -> anyhow::Result<bool> {
        Ok(self.transitions(name, now, true, true, exclude, replica)? < self.budget(name)?)
    }
    pub fn gone(&self, machine: u64, up: &BTreeMap<u64, Run>, now: u64) -> bool {
        !up.contains_key(&machine)
            && now.saturating_sub(self.settle_at) > self.live_ms.max(5 * MIN)
            && !self.settling(now)
    }
    pub fn gone_copy(&self, x: &Copy, up: &BTreeMap<u64, Run>, now: u64) -> bool {
        if up.contains_key(&x.machine) {
            return false;
        }
        if self
            .runs
            .values()
            .filter(|r| r.machine == x.machine)
            .max_by_key(|r| r.seen)
            .is_some_and(|r| r.n("left") > 0)
        {
            return true;
        }
        if self.settling(now)
            || now.saturating_sub(self.settle_at) <= self.live_ms.max(5 * MIN)
            || now.saturating_sub(self.last_seen_of(x.machine)) < (3 * self.live_ms).max(5 * MIN)
        {
            return false;
        }
        self.held_by(x.machine, now).is_none()
    }
    pub fn defer_doubling(&mut self, name: &str, up: &BTreeMap<u64, Run>, now: u64) -> bool {
        let starting = self
            .starts
            .iter()
            .any(|(m, t)| now.saturating_sub(*t) < 8 * MIN && !up.contains_key(m));
        let not_ready = up
            .values()
            .any(|r| r.ready == 0 && self.copies_on(name, r.machine).is_empty());
        if !starting && !not_ready {
            self.defer_since.remove(name);
            return false;
        }
        let at = *self.defer_since.entry(name.into()).or_insert(now);
        now.saturating_sub(at) < 8 * MIN
    }
    pub fn why_blocked(
        &self,
        name: &str,
        spec: &Version,
        up: &BTreeMap<u64, Run>,
        replica: u32,
    ) -> anyhow::Result<String> {
        let region = replica_region(&spec.policy, replica);
        let scope = region.map(|r| format!("{r}: ")).unwrap_or_default();
        let up = up
            .iter()
            .filter(|(_, r)| region.is_none() || r.region() == region)
            .map(|(k, v)| (*k, v.clone()))
            .collect::<BTreeMap<_, _>>();
        if let Some(region) = region {
            if up.is_empty() {
                return Ok(format!(
                    "replica {replica} is waiting for a server in {region}; no live server has a known matching region"
                ));
            }
        }
        let starting = self.starting_counts(&up);
        if !up.is_empty()
            && up
                .keys()
                .all(|m| starting.get(m).copied().unwrap_or(0) >= 3)
        {
            return Ok(format!(
                "{scope}waiting for deployment capacity: servers already have 3 or more copies starting"
            ));
        }
        let empty = up
            .keys()
            .filter(|m| self.copies_on(name, **m).is_empty())
            .copied()
            .collect::<Vec<_>>();
        if !empty.is_empty() {
            for m in empty {
                if let Some((what, other)) = self.clash(name, m)? {
                    return Ok(format!(
                        "{scope}every other server already has an app using {what} ({other})"
                    ));
                }
            }
            return Ok(format!("{scope}no usable server can take it"));
        }
        Ok(format!(
            "{scope}every server already runs it, and it can't run twice on one server ({})",
            spec.nodup.as_deref().unwrap_or("its compose file")
        ))
    }
    pub fn move_outcome(
        &mut self,
        name: &str,
        x: &mut Copy,
        up: &BTreeMap<u64, Run>,
        now: u64,
    ) -> anyhow::Result<MoveOutcome> {
        let Some(lv) = x.leaving.clone() else {
            return Ok(MoveOutcome::Waiting);
        };
        let to = n(&lv, "to");
        let Some(nx) = self
            .copies_of(name)
            .iter()
            .find(|y| y.machine == to && y.replica == x.replica && y.leaving.is_none())
            .cloned()
        else {
            return Ok(MoveOutcome::Cancel {
                reason: "its new copy is gone".into(),
                no_new: true,
                failed: false,
            });
        };
        let run = up.get(&to);
        if let Some(p) = self.projects.get(name) {
            if let Some(spec) = self.version(name, p.version)? {
                if let (Some(region), Some(run)) = (replica_region(&spec.policy, x.replica), run) {
                    if run.region().is_none() {
                        return Ok(MoveOutcome::Waiting);
                    }
                    if run.region() != Some(region) {
                        return Ok(MoveOutcome::Cancel {
                            reason: format!("server {to} is outside the required region {region}"),
                            no_new: false,
                            failed: false,
                        });
                    }
                }
            }
        }
        if self.copy_healthy(name, &nx, run, None) {
            let ok_at = if n(&lv, "okAt") == 0 {
                x.leaving.as_mut().unwrap()["okAt"] = json!(now);
                self.save_copy(name, x.clone())?;
                now
            } else {
                n(&lv, "okAt")
            };
            let port = self.wanted_spec(name)?.and_then(|s| s.port);
            return Ok(
                if port.is_none()
                    || self.e("DNS") == "off"
                    || self.dns_synced_at >= ok_at
                    || now.saturating_sub(ok_at) > 5 * MIN
                {
                    MoveOutcome::Done
                } else {
                    MoveOutcome::Waiting
                },
            );
        }
        if self.gone(to, up, now) {
            return Ok(MoveOutcome::Cancel {
                reason: format!("server {to} went away"),
                no_new: false,
                failed: false,
            });
        }
        let st = run.map(|r| &r.status[&nx.key]).unwrap_or(&Value::Null);
        if st["s"] == "failed" {
            return Ok(MoveOutcome::Cancel {
                reason: truncate(
                    &format!(
                        "it failed on server {to}: {}",
                        s(st, "e").lines().last().unwrap_or("")
                    ),
                    300,
                ),
                no_new: false,
                failed: true,
            });
        }
        let limit = if lv["ext"] == true {
            25 * MIN
        } else {
            15 * MIN
        };
        if now.saturating_sub(n(&lv, "at")) > limit {
            if lv["ext"] != true && st["s"] == "applying" {
                x.leaving.as_mut().unwrap()["ext"] = json!(true);
                self.save_copy(name, x.clone())?;
                return Ok(MoveOutcome::Waiting);
            }
            if self.any_ready(name, up, Some(x.replica)) {
                return Ok(MoveOutcome::Cancel {
                    reason: format!(
                        "its new copy on server {to} wasn't healthy after {} min",
                        limit / MIN
                    ),
                    no_new: false,
                    failed: true,
                });
            }
        }
        Ok(MoveOutcome::Waiting)
    }
    pub fn move_copy(
        &mut self,
        name: &str,
        from: u64,
        to: Option<u64>,
        replica: Option<u32>,
        cause: &str,
        why: Option<&str>,
        now: u64,
    ) -> anyhow::Result<Value> {
        let p = self.project(name)?;
        if self.recovery_active(now) {
            return Err(http(
                503,
                "recovering fleet inventory and tunnels; retry moving shortly",
            ));
        }
        self.cold_recovery_complete = true;
        let copies = self.copies_on(name, from);
        let x = if let Some(k) = replica {
            copies.iter().find(|x| x.replica == k)
        } else {
            copies
                .iter()
                .find(|x| x.leaving.is_none())
                .or_else(|| copies.first())
        }
        .cloned()
        .ok_or_else(|| http(400, format!("{name} isn't placed on server {from}")))?;
        if let Some(lv) = &x.leaving {
            return Err(http(
                409,
                format!(
                    "replica {} of {name} is already moving from server {from} to {}",
                    x.replica,
                    n(lv, "to")
                ),
            ));
        }
        let origin = self
            .copies_of(name)
            .iter()
            .find(|y| y.replica == x.replica && y.to() == Some(from))
            .cloned();
        let spec = self
            .version(name, p.version)?
            .ok_or_else(|| http(409, "application version is missing"))?;
        let up = self.live_machines(now);
        let region = replica_region(&spec.policy, x.replica);
        if let Some((to, r)) = to.and_then(|to| up.get(&to).map(|r| (to, r))) {
            if region.is_some() && r.region() != region {
                return Err(http(
                    409,
                    format!(
                        "{name} replica {} requires {}; server {to} is in {}",
                        x.replica,
                        region.unwrap(),
                        r.region().unwrap_or("an unknown region")
                    ),
                ));
            }
        }
        if let Some(mut origin) = origin.clone().filter(|o| Some(o.machine) == to) {
            self.drop_copy(name, from, x.replica, None, now)?;
            self.log_event("move-cancel",json!({"app":name,"replica":x.replica,"machine":origin.machine,"other":from,"cause":origin.leaving.as_ref().unwrap()["cause"],"detail":format!("sent back to server {}{}",origin.machine,why.map(|s|format!(": {s}")).unwrap_or_else(||" by hand".into()))}),now)?;
            origin.leaving = None;
            self.save_copy(name, origin)?;
            return self.describe(&p);
        }
        let mut dest = if let Some(to) = to {
            if !up.contains_key(&to) {
                return Err(http(400, format!("server {to} isn't up")));
            }
            if to == from {
                return Err(http(
                    400,
                    format!("replica {} of {name} is on server {to} already", x.replica),
                ));
            }
            let doubling = self.copies_on(name, to).len();
            if doubling > 0 && spec.nodup.is_some() {
                return Err(http(
                    409,
                    format!(
                        "server {to} already runs {name}, which can't run twice on one server ({})",
                        spec.nodup.as_ref().unwrap()
                    ),
                ));
            }
            if let Some((what, other)) = self.clash(name, to)? {
                return Err(http(
                    409,
                    format!("server {to} already has {other}, which uses {what} too"),
                ));
            }
            Candidate {
                machine: to,
                doubling,
                reason: format!(
                    "moved here from server {from} {}",
                    why.map(|s| format!("automatically: {s}"))
                        .unwrap_or_else(|| "by hand".into())
                ),
                ..Default::default()
            }
        } else {
            let available = up
                .iter()
                .filter(|(m, _)| {
                    **m != from
                        && Some(**m) != origin.as_ref().map(|o| o.machine)
                        && !self.failed_here(name, **m, now)
                })
                .map(|(m, r)| (*m, r.clone()))
                .collect();
            let mut best = self
                .best_machine(name, &available, &self.placement_counts(), now, x.replica)?
                .ok_or_else(|| {
                    http(
                        409,
                        format!(
                            "no other usable server{} is up for {name}",
                            region.map(|r| format!(" in {r}")).unwrap_or_default()
                        ),
                    )
                })?;
            best.reason = format!(
                "moved here from server {from}{}: {}",
                why.map(|s| format!(" automatically: {s}"))
                    .unwrap_or_default(),
                best.reason
            );
            best
        };
        if let Some(mut origin) = origin {
            self.drop_copy(name, from, x.replica, None, now)?;
            dest.reason = dest.reason.replace(
                &format!("from server {from}"),
                &format!("from server {} (instead of {from})", origin.machine),
            );
            let new = self.new_copy(
                name,
                &spec,
                x.replica,
                &dest,
                now,
                &dest.reason,
                &format!("move:{cause}"),
            )?;
            self.save_copy(name, new)?;
            let old = origin.leaving.take().unwrap();
            let mut lv = json!({"to":dest.machine,"at":now,"cause":cause});
            if old["lost"] == true {
                lv["lost"] = old["lost"].clone();
                lv["fromRun"] = old["fromRun"].clone();
            }
            origin.leaving = Some(lv);
            self.save_copy(name, origin.clone())?;
            self.log_event("move-start",json!({"app":name,"replica":x.replica,"machine":origin.machine,"other":dest.machine,"cause":cause,"detail":format!("redirected from server {from}: {}",why.unwrap_or(&dest.reason))}),now)?;
        } else {
            let new = self.new_copy(
                name,
                &spec,
                x.replica,
                &dest,
                now,
                &dest.reason,
                &format!("move:{cause}"),
            )?;
            self.save_copy(name, new)?;
            let mut source = x.clone();
            source.leaving = Some(json!({"to":dest.machine,"at":now,"cause":cause}));
            self.save_copy(name, source)?;
            self.log_event("move-start",json!({"app":name,"replica":x.replica,"machine":from,"other":dest.machine,"cause":cause,"detail":why.unwrap_or(&dest.reason)}),now)?;
        }
        self.describe(&p)
    }
    pub fn place(&mut self, now: u64) -> anyhow::Result<bool> {
        if self.recovery_active(now) {
            return Ok(false);
        }
        self.cold_recovery_complete = true;
        let up = self.live_machines(now);
        let arrived = now.saturating_sub(self.settle_at) > 3 * MIN
            || up.keys().all(|m| {
                self.live_metrics
                    .get(m)
                    .is_some_and(|v| now.saturating_sub(n(v, "t")) < 3 * self.live_ms)
            });
        let mut changed = false;
        let mut counts: BTreeMap<u64, usize> = BTreeMap::new();
        let snapshot = self.copies.clone();
        for (name, list) in snapshot {
            let p = self.projects.get(&name).cloned();
            let spec = match &p {
                Some(p) => self.version(&name, p.version)?,
                None => None,
            };
            let replicas = spec.as_ref().map(|s| s.replicas).unwrap_or(1);
            for mut x in list {
                if !self
                    .copies_of(&name)
                    .iter()
                    .any(|y| y.machine == x.machine && y.replica == x.replica)
                {
                    continue;
                }
                let why = if p.is_none() {
                    Some("removed")
                } else if p.as_ref().unwrap().enabled == 0 {
                    Some("disabled")
                } else if x.replica > replicas {
                    Some("replicas")
                } else if self.gone_copy(&x, &up, now) {
                    Some("gone")
                } else {
                    None
                };
                if why == Some("gone") {
                    let left = self
                        .runs
                        .values()
                        .filter(|r| r.machine == x.machine)
                        .max_by_key(|r| r.seen)
                        .is_some_and(|r| r.n("left") > 0);
                    self.alert("grace",json!({"machine":x.machine}),&if left{format!("server {} was stopped before it could hand over: its copies are placed elsewhere",x.machine)}else{format!("server {} is gone and no replacement took its slot in time: its copies are placed elsewhere",x.machine)},now)?;
                }
                if let Some(why) = why {
                    self.drop_copy(&name, x.machine, x.replica, Some(why), now)?;
                    changed = true;
                    continue;
                }
                if let Some(lv) = x.leaving.clone() {
                    match self.move_outcome(&name, &mut x, &up, now)? {
                        MoveOutcome::Done => {
                            self.drop_copy(&name, x.machine, x.replica, None, now)?;
                            self.log_event("move-done",json!({"app":name,"replica":x.replica,"machine":x.machine,"other":lv["to"],"cause":lv.get("cause").unwrap_or(&json!("hand"))}),now)?;
                            changed = true;
                        }
                        MoveOutcome::Cancel {
                            reason,
                            no_new,
                            failed,
                        } => {
                            let to = n(&lv, "to");
                            if !no_new {
                                self.drop_copy(&name, to, x.replica, None, now)?;
                            }
                            if failed {
                                self.failed_on
                                    .insert(format!("{name}|{to}"), now + 6 * 3600_000);
                                if let Some(v) = p.as_ref().and_then(Project::wanted) {
                                    self.note_version_fail(&name, v, to, now)?;
                                }
                            }
                            self.log_event("move-cancel",json!({"app":name,"replica":x.replica,"machine":x.machine,"other":to,"cause":lv.get("cause").unwrap_or(&json!("hand")),"detail":reason}),now)?;
                            x.leaving = None;
                            self.save_copy(&name, x.clone())?;
                            *counts.entry(x.machine).or_default() += 1;
                            changed = true;
                        }
                        MoveOutcome::Waiting => (),
                    }
                    continue;
                }
                *counts.entry(x.machine).or_default() += 1;
            }
        }
        let projects = self.projects.values().cloned().collect::<Vec<_>>();
        for p in projects.into_iter().filter(|p| p.enabled > 0) {
            let Some(spec) = self.version(&p.name, p.version)? else {
                continue;
            };
            let have = self
                .copies_of(&p.name)
                .iter()
                .filter(|x| x.leaving.is_none())
                .map(|x| x.replica)
                .collect::<BTreeSet<_>>();
            self.blocked.remove(&p.name);
            if !arrived {
                continue;
            }
            if spec.policy.region.is_some() || !spec.policy.regions.is_empty() {
                for x in self.copies_of(&p.name).to_vec() {
                    let Some(run) = up.get(&x.machine) else {
                        continue;
                    };
                    let region = replica_region(&spec.policy, x.replica);
                    if x.leaving.is_some() || run.region() == region {
                        continue;
                    }
                    let known = !run.s("region").is_empty() || !run.s("location").is_empty();
                    let waiting = self.predecessor_of(run, now).is_some()
                        && now.saturating_sub(run.started) >= 3 * MIN;
                    if !known && !waiting {
                        continue;
                    }
                    let serving = self.live_runs(now).iter().any(|r| {
                        r.machine == x.machine
                            && r.ready > 0
                            && r.status[&x.key]["s"] == "healthy"
                            && r.status[&x.key]["r"] != false
                    });
                    if serving && !self.may_transition(&p.name, now, None, Some(x.replica))? {
                        continue;
                    }
                    let before = self
                        .copies_of(&p.name)
                        .iter()
                        .map(|x| (x.machine, x.replica))
                        .collect::<BTreeSet<_>>();
                    match self.move_copy(
                        &p.name,
                        x.machine,
                        None,
                        Some(x.replica),
                        "region",
                        Some(&format!(
                            "replica {} requires {}",
                            x.replica,
                            region.unwrap_or("unknown")
                        )),
                        now,
                    ) {
                        Ok(_) => {
                            if let Some(dest) = self.copies_of(&p.name).iter().find(|y| {
                                !before.contains(&(y.machine, y.replica)) && y.leaving.is_none()
                            }) {
                                *counts.entry(dest.machine).or_default() += 1;
                            }
                            changed = true;
                        }
                        Err(e)
                            if e.downcast_ref::<HttpError>()
                                .is_some_and(|e| e.status == 409) =>
                        {
                            self.blocked.insert(
                                p.name.clone(),
                                format!(
                                    "replica {} is waiting for a usable server in {}",
                                    x.replica,
                                    region.unwrap_or("unknown")
                                ),
                            );
                        }
                        Err(e) => return Err(e),
                    }
                }
            }
            let missing = (1..=spec.replicas)
                .filter(|k| !have.contains(k))
                .collect::<Vec<_>>();
            if missing.is_empty() {
                self.defer_since.remove(&p.name);
                continue;
            }
            for k in missing {
                let Some(best) = self.best_machine(&p.name, &up, &counts, now, k)? else {
                    if !up.is_empty() || replica_region(&spec.policy, k).is_some() {
                        let why = self.why_blocked(&p.name, &spec, &up, k)?;
                        self.blocked.insert(p.name.clone(), why);
                    }
                    continue;
                };
                if best.doubling > 0 && spec.replicas > 1 && self.defer_doubling(&p.name, &up, now)
                {
                    self.blocked.insert(p.name.clone(),"waiting for a server that's starting, rather than running two copies on one".into());
                    break;
                }
                let ever = !self
                    .store
                    .rows(
                        "SELECT 1 FROM events WHERE app=? AND replica=? AND kind='place' LIMIT 1",
                        &[json!(p.name), json!(k)],
                    )?
                    .is_empty();
                let x = self.new_copy(
                    &p.name,
                    &spec,
                    k,
                    &best,
                    now,
                    &best.reason,
                    if ever { "replace" } else { "new" },
                )?;
                self.save_copy(&p.name, x.clone())?;
                self.log_event("place",json!({"app":p.name,"replica":k,"machine":best.machine,"cause":x.cause,"detail":x.reason}),now)?;
                *counts.entry(best.machine).or_default() += 1;
                changed = true;
            }
        }
        if changed {
            self.dns_dirty = true;
        }
        Ok(changed)
    }
    pub fn evict(&mut self, machine: u64, force: bool, now: u64) -> anyhow::Result<Value> {
        let mut moved = Vec::new();
        let mut failed = Vec::new();
        let copies = self.copies.clone();
        for (name, list) in copies {
            for x in list {
                if x.machine != machine || x.leaving.is_some() {
                    continue;
                }
                if !force && !self.may_transition(&name, now, None, None)? {
                    failed.push(format!("{name} (replica {}): it's already changing as much as it may; try again in a few minutes, or force it",x.replica));
                    continue;
                }
                match self.move_copy(&name, machine, None, Some(x.replica), "evict", None, now) {
                    Ok(_) => moved.push(format!("{name} (replica {})", x.replica)),
                    Err(e) => failed.push(format!("{name} (replica {}): {e}", x.replica)),
                }
            }
        }
        Ok(json!({"machine":machine,"moved":moved,"failed":failed}))
    }
}
