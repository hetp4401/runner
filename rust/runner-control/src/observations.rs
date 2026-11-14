use crate::{
    control::{Control, f, n, s},
    model::*,
    rebalance::failing,
};
use serde_json::{Value, json};
impl Control {
    pub fn note_copies(&mut self, r: &Run, status: &Value, now: u64) -> anyhow::Result<()> {
        let mut rs = self
            .restarts
            .get(&r.id)
            .cloned()
            .unwrap_or_else(|| json!({"last":{},"log":[]}));
        for (key, st) in status.as_object().into_iter().flatten() {
            if !st.is_object() {
                continue;
            }
            let k = format!("{}|{key}", r.id);
            let copy = self.copies.iter().find_map(|(name, list)| {
                list.iter()
                    .find(|x| x.machine == r.machine && x.key == *key)
                    .map(|x| (name.clone(), x.replica))
            });
            if !self.first_seen.contains_key(&k) {
                self.first_seen.insert(k.clone(), now);
                if self.predecessor_of(r, now).is_some_and(|older| {
                    st["s"] != "healthy" && older.status[key]["s"] == "healthy"
                }) {
                    self.log_event("copy-start",json!({"app":copy.as_ref().map(|c|&c.0),"replica":copy.as_ref().map(|c|c.1),"machine":r.machine,"run":r.id,"cause":"successor"}),now)?;
                }
            }
            if let Some(v) = st["v"].as_u64() {
                if let Some(old) = self.last_v.get(&k).copied().filter(|old| *old != v) {
                    self.log_event("copy-start",json!({"app":copy.as_ref().map(|c|&c.0),"replica":copy.as_ref().map(|c|c.1),"machine":r.machine,"run":r.id,"cause":"version","detail":format!("v{old} -> v{v}")}),now)?;
                }
                self.last_v.insert(k.clone(), v);
            }
            if st["s"] == "healthy" && st["r"] != false {
                self.last_healthy.insert(k, now);
            }
            if let Some(count) = st["n"].as_u64() {
                if let Some(before) = rs["last"][key].as_u64().filter(|before| count > *before) {
                    rs["log"]
                        .as_array_mut()
                        .unwrap()
                        .push(json!({"t":now,"key":key,"k":count-before}));
                }
                rs["last"][key] = json!(count);
            }
        }
        rs["log"]
            .as_array_mut()
            .unwrap()
            .retain(|entry| now.saturating_sub(n(entry, "t")) <= 60 * MIN);
        // A pathological restart loop must not accumulate an unbounded event list.
        let log = rs["log"].as_array_mut().unwrap();
        if log.len() > 2048 {
            log.drain(..log.len() - 2048);
        }
        self.restarts.insert(r.id.clone(), rs);
        Ok(())
    }
    pub fn note_ready(&mut self, now: u64) {
        if now.saturating_sub(self.last_ready_check) < 10_000 {
            return;
        }
        self.last_ready_check = now;
        let up = self.live_machines(now);
        for name in self.projects.keys().cloned().collect::<Vec<_>>() {
            if self.copies_of(&name).iter().any(|x| {
                x.leaving.is_none() && self.copy_healthy(&name, x, up.get(&x.machine), None)
            }) {
                self.ready_since.entry(name).or_insert(now);
            } else {
                self.ready_since.remove(&name);
            }
        }
    }
    pub fn sick_signs(
        &mut self,
        r: &Run,
        up: &std::collections::BTreeMap<u64, Run>,
        now: u64,
    ) -> Vec<(String, String)> {
        let mut out = Vec::new();
        let here = self
            .copies
            .iter()
            .flat_map(|(name, list)| {
                list.iter()
                    .filter(|x| x.machine == r.machine && x.leaving.is_none())
                    .map(move |x| (name.clone(), x.clone()))
            })
            .collect::<Vec<_>>();
        let mut bad = Vec::new();
        for (name, x) in &here {
            if failing(&r.status[&x.key]) {
                let elsewhere = self.any_ready(name, up, Some(x.replica));
                bad.push((
                    name.clone(),
                    now.saturating_sub(self.fail_clock(name, x, r, now, false)),
                    if elsewhere {
                        now.saturating_sub(self.fail_clock(name, x, r, now, true))
                    } else {
                        0
                    },
                ));
            }
        }
        let apps = bad
            .iter()
            .filter(|(_, _, aware)| *aware >= 5 * MIN)
            .map(|(name, _, _)| name.clone())
            .collect::<std::collections::BTreeSet<_>>();
        if apps.len() >= 2 {
            out.push((
                "apps".into(),
                format!(
                    "{} failing on it while healthy elsewhere",
                    apps.into_iter().collect::<Vec<_>>().join(" and ")
                ),
            ));
        }
        let long = bad
            .iter()
            .filter(|(_, plain, _)| *plain >= 10 * MIN)
            .collect::<Vec<_>>();
        let multi = long
            .iter()
            .map(|(name, _, _)| name)
            .collect::<std::collections::BTreeSet<_>>()
            .len()
            >= 2;
        let counted = long
            .iter()
            .filter(|(_, _, aware)| multi || *aware >= 10 * MIN)
            .count();
        if counted > 0 && 2 * counted >= here.len() {
            out.push((
                "half".into(),
                format!(
                    "{counted} of its {} copies failing for 10 minutes",
                    here.len()
                ),
            ));
        }
        let d = r.v("disk");
        if f(d, "total") > 0. && f(d, "free") / f(d, "total") < 0.1 {
            let since = *self.low_disk_since.entry(r.id.clone()).or_insert(now);
            if now.saturating_sub(since) >= 5 * MIN {
                out.push((
                    "disk".into(),
                    format!(
                        "{:.0}% of its disk free",
                        100. * f(d, "free") / f(d, "total")
                    ),
                ));
            }
        } else {
            self.low_disk_since.remove(&r.id);
        }
        let restarts = self
            .restarts
            .get(&r.id)
            .into_iter()
            .flat_map(|rs| rs["log"].as_array().into_iter().flatten())
            .filter(|e| {
                now.saturating_sub(n(e, "t")) < 60 * MIN
                    && here
                        .iter()
                        .find(|(_, x)| x.key == s(e, "key"))
                        .is_some_and(|(name, x)| self.any_ready(name, up, Some(x.replica)))
            })
            .map(|e| n(e, "k"))
            .sum::<u64>();
        if restarts >= 5 {
            out.push((
                "restarts".into(),
                format!("{restarts} restarts in the last hour of apps healthy elsewhere"),
            ));
        }
        out
    }
    pub fn strike(&mut self, r: &Run, kind: &str, why: &str, now: u64) -> anyhow::Result<()> {
        let list = self.strikes.entry(r.id.clone()).or_default();
        if list
            .iter()
            .any(|x| x["kind"] == kind && now.saturating_sub(n(x, "t")) < 10 * MIN)
        {
            return Ok(());
        }
        list.push(json!({"t":now,"kind":kind,"why":why}));
        self.log_event(
            "strike",
            json!({"machine":r.machine,"run":r.id,"cause":kind,"detail":why}),
            now,
        )
    }
    pub fn quarantine(
        &mut self,
        r: &mut Run,
        why: &str,
        now: u64,
        sick: bool,
    ) -> anyhow::Result<()> {
        r.quarantine = Some(now + 60 * MIN);
        self.save_run(r.clone())?;
        if sick {
            self.sick_why.insert(r.id.clone(), why.into());
        }
        self.log_event("quarantine",json!({"machine":r.machine,"run":r.id,"cause":if sick{"sick"}else{"hot"},"detail":format!("not a destination for an hour: {why}")}),now)
    }
    pub fn sick_drain(&mut self, r: &mut Run, now: u64) -> anyhow::Result<()> {
        if r.quarantine.unwrap_or(0) <= now
            || !self.sick_why.contains_key(&r.id)
            || r.drain > 0
            || r.handover > 0
            || r.evict.is_some_and(|v| v > 0)
            || r.retire > 0
        {
            return Ok(());
        }
        let why = self.sick_why[&r.id].clone();
        if !r.b("starts") {
            self.alert("sick-server",json!({"machine":r.machine}),&format!("server {} looks sick ({why}); nothing replaces it automatically, so it needs a look",r.machine),now)?;
            return Ok(());
        }
        let pool = r.pool.as_deref().unwrap_or("null");
        if self
            .sick_drains
            .get(pool)
            .is_some_and(|at| now.saturating_sub(*at) < 30 * MIN)
            || self.capped(r.pool.as_deref(), now)
        {
            return Ok(());
        }
        r.drain = now;
        self.save_run(r.clone())?;
        self.sick_drains.insert(pool.into(), now);
        self.set_setting("sick_drains", serde_json::to_string(&self.sick_drains)?)?;
        self.log_event("drain",json!({"machine":r.machine,"run":r.id,"cause":"sick","detail":format!("server {} looks sick ({why}): it asks to leave, for a fresh one",r.machine)}),now)
    }
    pub fn check_sick(&mut self, now: u64) -> anyhow::Result<()> {
        if now.saturating_sub(self.last_sick_check) < 30_000 || self.quiet(now) {
            return Ok(());
        }
        self.last_sick_check = now;
        let up = self.live_machines(now);
        let mut candidates = Vec::new();
        for r in up.values() {
            if r.ready == 0 || now.saturating_sub(r.started) < 5 * MIN {
                continue;
            }
            for (kind, why) in self.sick_signs(r, &up, now) {
                self.strike(r, &kind, &why, now)?;
            }
            let mut recent = self.strikes.get(&r.id).cloned().unwrap_or_default();
            recent.retain(|x| now.saturating_sub(n(x, "t")) < 30 * MIN);
            if recent.is_empty() {
                self.strikes.remove(&r.id);
            } else {
                self.strikes.insert(r.id.clone(), recent.clone());
            }
            if recent.len() >= 2 && r.quarantine.unwrap_or(0) <= now {
                let why = recent
                    .iter()
                    .map(|x| s(x, "why").to_owned())
                    .collect::<std::collections::BTreeSet<_>>()
                    .into_iter()
                    .collect::<Vec<_>>()
                    .join("; ");
                candidates.push((r.clone(), why));
            }
        }
        let mut sick = up
            .values()
            .filter(|r| r.quarantine.unwrap_or(0) > now && self.sick_why.contains_key(&r.id))
            .cloned()
            .collect::<Vec<_>>();
        if sick.len() + candidates.len() >= 3 {
            if !candidates.is_empty() {
                let mut machines = sick
                    .iter()
                    .chain(candidates.iter().map(|(r, _)| r))
                    .map(|r| r.machine)
                    .collect::<Vec<_>>();
                machines.sort();
                self.alert("many-sick",json!({}),&format!("servers {} look sick at once: probably an app, not the servers, so nothing is done automatically",machines.iter().map(u64::to_string).collect::<Vec<_>>().join(", ")),now)?;
            }
            return Ok(());
        }
        for (mut r, why) in candidates {
            self.quarantine(&mut r, &why, now, true)?;
            sick.push(r);
        }
        for mut r in sick {
            self.sick_drain(&mut r, now)?;
        }
        Ok(())
    }
    pub fn check_dark(&mut self, now: u64) -> anyhow::Result<()> {
        if now.saturating_sub(self.last_dark_check) < 10_000 {
            return Ok(());
        }
        self.last_dark_check = now;
        let live = self.live_runs(now);
        let settling = self.settling(now);
        let mut wanted = std::collections::BTreeSet::new();
        let projects = self.projects.values().cloned().collect::<Vec<_>>();
        for p in projects {
            if p.enabled == 0 {
                continue;
            }
            let Some(spec) = self.version(&p.name, p.version)? else {
                continue;
            };
            for k in 1..=spec.replicas {
                let key = format!("{}|{k}", p.name);
                wanted.insert(key.clone());
                let serving = self
                    .copies_of(&p.name)
                    .iter()
                    .filter(|x| x.replica == k)
                    .find_map(|x| {
                        live.iter()
                            .find(|r| {
                                r.machine == x.machine
                                    && r.status[&x.key]["s"] == "healthy"
                                    && r.status[&x.key]["r"] != false
                                    && r.v("edges") != &json!(0)
                            })
                            .map(|r| (r.id.clone(), x.machine))
                    });
                let dark = self.dark.get(&key).cloned();
                if let Some((run, machine)) = serving {
                    if let Some(d) = dark {
                        self.dark.remove(&key);
                        self.log_event("lit",json!({"app":p.name,"replica":k,"machine":machine,"run":d["run"],"cause":d["cause"],"other":(now.saturating_sub(n(&d,"since"))+500)/1000,"detail":format!("dark for {}, after {}",fmt_dur(now.saturating_sub(n(&d,"since"))),s(&d,"why"))}),now)?;
                    }
                    self.lit_at
                        .insert(key, json!({"t":now,"run":run,"machine":machine}));
                } else if dark.is_none() {
                    let Some(last) = self.lit_at.get(&key).cloned() else {
                        continue;
                    };
                    if settling {
                        continue;
                    }
                    let r = self.runs.get(s(&last, "run"));
                    let machine = n(&last, "machine");
                    let (cause, why) = match r {
                        None => ("gone", format!("server {machine} went away")),
                        Some(r) if r.n("left") > 0 => {
                            ("stopped", format!("server {machine} was stopped"))
                        }
                        Some(r) if r.evict.is_some_and(|v| v > 0) => {
                            ("evict", format!("server {machine} moved its copies out"))
                        }
                        Some(r) if r.handover > 0 => {
                            ("handover", format!("server {machine} handed over"))
                        }
                        Some(r) if r.retire > 0 => ("retired", format!("server {machine} retired")),
                        Some(r) if !self.live(r, now) => {
                            ("silent", format!("server {machine} went silent"))
                        }
                        _ => (
                            "copy",
                            format!("its copy on server {machine} stopped serving"),
                        ),
                    };
                    self.dark.insert(key,json!({"since":last["t"],"run":last["run"],"machine":machine,"cause":cause,"why":why}));
                    self.log_event("dark",json!({"app":p.name,"replica":k,"machine":machine,"run":last["run"],"cause":cause,"detail":why}),now)?;
                } else if let Some(d) = dark {
                    if d["alerted"] != true
                        && !settling
                        && now.saturating_sub(n(&d, "since")) >= 5 * MIN
                    {
                        self.dark.get_mut(&key).unwrap()["alerted"] = json!(true);
                        self.alert(
                            "dark",
                            json!({"app":p.name}),
                            &format!(
                                "{} replica {k} has had no healthy copy for {} ({})",
                                p.name,
                                fmt_dur(now.saturating_sub(n(&d, "since"))),
                                s(&d, "why")
                            ),
                            now,
                        )?;
                    }
                }
            }
        }
        self.dark.retain(|k, _| wanted.contains(k));
        self.lit_at.retain(|k, _| wanted.contains(k));
        Ok(())
    }
    pub fn alert(
        &mut self,
        cause: &str,
        mut fields: Value,
        text: &str,
        now: u64,
    ) -> anyhow::Result<()> {
        let key = format!(
            "{cause}|{}|{}",
            fields["app"].as_str().unwrap_or(""),
            fields["machine"]
                .as_u64()
                .map(|n| n.to_string())
                .unwrap_or_default()
        );
        if self
            .alerted_at
            .get(&key)
            .is_some_and(|at| now.saturating_sub(*at) < 10 * MIN)
        {
            return Ok(());
        }
        self.alerted_at.insert(key, now);
        fields["cause"] = json!(cause);
        fields["detail"] = json!(text);
        self.log_event("alert", fields.clone(), now)?;
        self.webhook_log
            .retain(|at| now.saturating_sub(*at) < 60 * MIN);
        if !self.e("ALERT_WEBHOOK").is_empty() && self.webhook_log.len() < 30 {
            self.webhook_log.push(now);
            let message = format!("Runners: {}", self.redact(text));
            self.alerts.push(json!({"text":message,"content":message,"cause":cause,"app":fields["app"],"machine":fields["machine"],"t":now}));
        }
        Ok(())
    }
    pub fn note_version_fail(
        &mut self,
        name: &str,
        version: u64,
        machine: u64,
        now: u64,
    ) -> anyhow::Result<()> {
        let key = format!("{name}@{version}");
        let set = self.version_fails.entry(key).or_default();
        if !set.insert(machine) {
            return Ok(());
        }
        let machines = set.iter().copied().collect::<Vec<_>>();
        self.set_setting("version_fails", serde_json::to_string(&self.version_fails)?)?;
        if machines.len() >= 2 {
            self.alert("fails-everywhere",json!({"app":name}),&format!("{name} version {version} failed on servers {}: that's the app, not the servers, so its copies aren't moved for failing any more",machines.iter().map(u64::to_string).collect::<Vec<_>>().join(" and ")),now)?;
        }
        Ok(())
    }
    pub fn fail_clock(&self, name: &str, x: &Copy, run: &Run, now: u64, outage_aware: bool) -> u64 {
        let key = format!("{}|{}", run.id, x.key);
        let t = self
            .last_healthy
            .get(&key)
            .copied()
            .unwrap_or(0)
            .max(x.since)
            .max(self.first_seen.get(&key).copied().unwrap_or(now));
        if outage_aware {
            t.max(self.ready_since.get(name).copied().unwrap_or(now))
        } else {
            t
        }
    }
    pub fn check_mass_loss(&mut self, now: u64) -> anyhow::Result<()> {
        if now.saturating_sub(self.last_loss_check) < 5000 || self.settling(now) {
            return Ok(());
        }
        self.last_loss_check = now;
        let live = self.live_machines(now);
        let mut lost = std::collections::BTreeSet::new();
        let runs = self.runs.values().cloned().collect::<Vec<_>>();
        for r in &runs {
            if r.retire > 0
                || live.contains_key(&r.machine)
                || r.deadline.is_some_and(|d| now > d.saturating_sub(2 * MIN))
            {
                continue;
            }
            let silent = now.saturating_sub(r.seen);
            if silent >= self.live_ms && silent < 2 * self.live_ms {
                lost.insert(r.machine);
            }
        }
        for r in runs {
            if r.retire > 0
                || r.n("left") > 0
                || self.loss_noted.contains(&r.id)
                || live.contains_key(&r.machine)
                || now.saturating_sub(r.seen) < self.live_ms
                || r.seen < self.settle_at.saturating_sub(self.live_ms)
                || r.deadline.is_some_and(|d| now > d.saturating_sub(2 * MIN))
            {
                continue;
            }
            self.loss_noted.insert(r.id.clone());
            self.loss_log.retain(|t| now.saturating_sub(*t) < 10 * MIN);
            self.loss_log.push(now);
            self.log_event("server-lost",json!({"machine":r.machine,"run":r.id,"cause":"silent","detail":format!("server {} stopped checking in without saying it was leaving",r.machine)}),now)?;
        }
        if lost.len() < 3.max(live.len().div_ceil(10)) {
            return Ok(());
        }
        self.mass_arms.retain(|t| now.saturating_sub(*t) < 60 * MIN);
        if self.mass_arms.len() >= 2 {
            if now.saturating_sub(self.mass_exhausted_at) > 60 * MIN {
                self.mass_exhausted_at = now;
                self.log_event("alert",json!({"cause":"grace-exhausted","detail":format!("{} servers went silent together, the third time this hour: treated as lost",lost.len())}),now)?;
            }
            return Ok(());
        }
        self.mass_arms.push(now);
        self.set_setting("mass_arms", serde_json::to_string(&self.mass_arms)?)?;
        self.quiet_since = now.saturating_sub(self.live_ms);
        self.settle_at = now;
        self.log_event("mass-loss",json!({"cause":"silent","detail":format!("{} servers went silent together ({}): waiting for them before re-placing anything",lost.len(),lost.iter().map(u64::to_string).collect::<Vec<_>>().join(", "))}),now)
    }
}
pub fn fmt_dur(ms: u64) -> String {
    if ms < 90_000 {
        format!("{} s", (ms + 500) / 1000)
    } else if ms < 90 * MIN {
        format!("{} min", (ms + MIN / 2) / MIN)
    } else {
        format!("{:.1} h", ms as f64 / 3_600_000.)
    }
}
