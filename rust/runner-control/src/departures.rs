use crate::{
    control::{Control, n, s, truncate},
    error::{http, require},
    model::*,
    spec,
};
use serde_json::{Value, json};
impl Control {
    pub fn rolled(&self, r: &Run) -> bool {
        ["roll".to_string(), format!("roll_{}", r.machine)]
            .iter()
            .any(|k| {
                self.settings
                    .get(k)
                    .and_then(|v| v.parse::<u64>().ok())
                    .is_some_and(|at| r.started < at)
            })
    }
    pub fn due(&self, r: &Run, now: u64) -> bool {
        r.drain > 0
            || self.rolled(r)
            || self.due_at(r).is_some_and(|at| now >= at)
            || self.pulled.contains(&r.id)
    }
    pub fn roll_only(&self, r: &Run, now: u64) -> bool {
        self.rolled(r)
            && r.drain == 0
            && !self.pulled.contains(&r.id)
            && !self.due_at(r).is_some_and(|at| now >= at)
    }
    pub fn urgent(&self, r: &Run, now: u64) -> bool {
        r.deadline
            .is_some_and(|at| at.saturating_sub(now) < 25 * MIN)
    }
    pub fn due_why(&self, r: &Run, now: u64) -> &'static str {
        if self.urgent(r, now) {
            "urgent"
        } else if r.drain > 0 {
            if self.sick_why.contains_key(&r.id) {
                "sick"
            } else {
                "drain"
            }
        } else if self.rolled(r) {
            "roll"
        } else if self.pulled.contains(&r.id) {
            "spread"
        } else {
            "due"
        }
    }
    pub fn predecessor_of(&self, r: &Run, now: u64) -> Option<Run> {
        self.live_runs(now)
            .into_iter()
            .filter(|x| x.machine == r.machine && x.started < r.started)
            .min_by_key(|r| r.started)
    }
    pub fn successor_of(&self, r: &Run, now: u64) -> Option<Run> {
        self.live_runs(now)
            .into_iter()
            .filter(|x| x.machine == r.machine && x.started > r.started)
            .max_by_key(|r| r.started)
    }
    pub fn capped(&self, pool: Option<&str>, now: u64) -> bool {
        pool.and_then(|p| self.capped_until.get(p))
            .is_some_and(|at| *at > now)
    }
    pub fn mark_capped(&mut self, pool: Option<&str>, now: u64, why: &str) -> anyhow::Result<()> {
        let Some(pool) = pool else {
            return Ok(());
        };
        if !self.capped(Some(pool), now) {
            self.log_event("capped",json!({"cause":"capped","detail":format!("new servers aren't arriving ({why}): departures that would wait for one move their copies out first for 30 min")}),now)?;
            self.alert(
                "capped",
                json!({}),
                &format!(
                    "new servers aren't arriving ({why}): whatever starts them may be at its limit"
                ),
                now,
            )?;
        }
        self.capped_until.insert(pool.into(), now + 30 * MIN);
        Ok(())
    }
    pub fn departing(&self, now: u64) -> Vec<Run> {
        self.live_runs(now)
            .into_iter()
            .filter(|r| {
                (r.handover > 0
                    && now.saturating_sub(r.handover) < 15 * MIN
                    && r.evict.is_none_or(|v| v == 0))
                    || r.evict.is_some_and(|v| v > 0)
            })
            .collect()
    }
    pub fn note_departure(
        &mut self,
        r: &Run,
        kind: &str,
        why: &str,
        now: u64,
    ) -> anyhow::Result<()> {
        self.depart_log
            .retain(|d| now.saturating_sub(n(d, "t")) < 10 * MIN);
        self.depart_log
            .push(json!({"t":now,"kind":kind,"run":r.id}));
        self.log_event("depart",json!({"machine":r.machine,"run":r.id,"cause":format!("{kind}:{why}"),"detail":format!("server {} is leaving ({why}) by {}",r.machine,if kind=="handover"{"handing over to a successor on its slot"}else{"moving its copies out first"})}),now)
    }
    fn busy_app(&mut self, r: &Run, now: u64) -> anyhow::Result<Option<String>> {
        let names = self
            .copies
            .iter()
            .filter(|(_, list)| {
                list.iter()
                    .any(|c| c.machine == r.machine && c.leaving.is_none())
            })
            .map(|(name, _)| name.clone())
            .collect::<Vec<_>>();
        for name in names {
            if !self.may_transition(&name, now, None, None)? {
                return Ok(Some(name));
            }
        }
        Ok(None)
    }
    pub fn departure_gate(
        &mut self,
        r: &Run,
        now: u64,
    ) -> anyhow::Result<Option<(String, Option<String>)>> {
        if r.retire > 0
            || r.evict.is_some_and(|v| v > 0)
            || r.handover > 0
            || self.successor_of(r, now).is_some()
            || !self.due(r, now)
        {
            return Ok(None);
        }
        let urgent = self.urgent(r, now);
        let how = if urgent
            || ((r.deadline.is_some() || r.drain > 0) && !r.b("starts"))
            || self.capped(r.pool.as_deref(), now)
        {
            "evict"
        } else {
            "handover"
        };
        let out = |wait: Option<String>| Ok(Some((how.into(), wait)));
        if urgent {
            return out(None);
        }
        if self.quiet(now) {
            return out(Some(
                "the control plane is in its quiet period after a restart or gap".into(),
            ));
        }
        if self
            .loss_log
            .iter()
            .filter(|t| now.saturating_sub(**t) < 10 * MIN)
            .count()
            >= 3
        {
            return out(Some(
                "3 or more servers were lost in the last 10 minutes".into(),
            ));
        }
        let recent = self
            .depart_log
            .iter()
            .filter(|d| now.saturating_sub(n(d, "t")) < 10 * MIN)
            .collect::<Vec<_>>();
        if recent.len() >= 4 {
            return out(Some("4 departures started in the last 10 minutes".into()));
        }
        if how == "evict" && recent.iter().filter(|d| d["kind"] == "evict").count() >= 2 {
            return out(Some("2 evictions started in the last 10 minutes".into()));
        }
        let in_flight = self.departing(now);
        if in_flight.len() >= 3 {
            return out(Some("3 servers are leaving already".into()));
        }
        let mut queue = self
            .live_runs(now)
            .into_iter()
            .filter(|x| {
                x.evict.is_none_or(|v| v == 0)
                    && x.handover == 0
                    && self.successor_of(x, now).is_none()
                    && self.due(x, now)
            })
            .collect::<Vec<_>>();
        queue.sort_by_key(|r| {
            (
                !self.sick_why.contains_key(&r.id),
                r.deadline.unwrap_or(u64::MAX),
                r.started,
            )
        });
        let mut first = None;
        for x in queue {
            if !in_flight.iter().any(|y| y.pool == x.pool)
                && self.busy_app(&x, now)?.is_none()
                && !(self.roll_only(&x, now) && !in_flight.is_empty())
            {
                first = Some(x);
                break;
            }
        }
        if first.as_ref().is_some_and(|x| x.id == r.id) {
            return out(None);
        }
        if self.roll_only(r, now) && !in_flight.is_empty() {
            return out(Some("a roll replaces one server at a time".into()));
        }
        if in_flight.iter().any(|x| x.pool == r.pool) {
            return out(Some("a server of the same group is leaving".into()));
        }
        if let Some(app) = self.busy_app(r, now)? {
            return out(Some(format!("{app} is already changing as much as it may")));
        }
        out(Some(
            first
                .map(|r| format!("server {} goes first", r.machine))
                .unwrap_or_else(|| "waiting its turn".into()),
        ))
    }
    pub fn drain(&mut self, body: &Value, now: u64) -> anyhow::Result<Value> {
        let runs = self
            .live_runs(now)
            .into_iter()
            .filter(|r| {
                (!s(body, "run").is_empty() && r.id == s(body, "run"))
                    || (!s(body, "agent").is_empty() && r.agent == s(body, "agent"))
                    || body["machine"].as_u64() == Some(r.machine)
            })
            .collect::<Vec<_>>();
        if runs.is_empty() {
            return Err(http(404, "no live run matches that run, agent or server"));
        }
        let out = json!({"draining":runs.iter().map(|r|json!({"run":r.id,"machine":r.machine})).collect::<Vec<_>>()});
        for mut r in runs {
            if r.drain == 0 {
                r.drain = now;
                self.save_run(r)?;
            }
        }
        Ok(out)
    }
    pub fn wants_handover(&mut self, r: &mut Run, now: u64) -> anyhow::Result<bool> {
        if r.retire > 0 || r.evict.is_some_and(|v| v > 0) {
            return Ok(false);
        }
        if r.handover > 0 {
            if self.successor_of(r, now).is_some() {
                return Ok(false);
            }
            if now.saturating_sub(r.handover) > 12 * MIN {
                self.mark_capped(
                    r.pool.as_deref(),
                    now,
                    &format!(
                        "server {}'s successor didn't start within 12 minutes",
                        r.machine
                    ),
                )?;
                self.start_evict(r, now, "no successor came")?;
                return Ok(false);
            }
            return Ok(true);
        }
        if self.settling(now) {
            return Ok(false);
        }
        let gate = self.departure_gate(r, now)?;
        let Some((how, wait)) = gate else {
            self.due_wait.remove(&r.id);
            return Ok(false);
        };
        if let Some(wait) = wait {
            let w = self
                .due_wait
                .entry(r.id.clone())
                .or_insert_with(|| json!({"since":now}));
            w["why"] = json!(wait);
            let since = n(w, "since");
            if now.saturating_sub(since) >= 20 * MIN && !self.roll_only(r, now) {
                self.alert(
                    "departure-waiting",
                    json!({"machine":r.machine}),
                    &format!(
                        "server {} has been due to leave for {} minutes: {wait}",
                        r.machine,
                        (now - since + MIN / 2) / MIN
                    ),
                    now,
                )?;
            }
            return Ok(false);
        }
        self.due_wait.remove(&r.id);
        let why = self.due_why(r, now);
        if how == "evict" {
            self.start_evict(r, now, why)?;
            return Ok(false);
        }
        r.handover = now;
        self.save_run(r.clone())?;
        self.note_departure(r, "handover", why, now)?;
        Ok(true)
    }
    pub fn start_evict(&mut self, r: &mut Run, now: u64, why: &str) -> anyhow::Result<()> {
        r.evict = Some(now);
        self.save_run(r.clone())?;
        self.note_departure(r, "evict", why, now)?;
        self.progress_evict(r, now)
    }
    pub fn progress_evict(&mut self, r: &mut Run, now: u64) -> anyhow::Result<()> {
        let urgent = self.urgent(r, now);
        let mut here = 0;
        for (name, list) in self.copies.clone() {
            for x in list.iter().filter(|x| x.machine == r.machine) {
                here += 1;
                if x.leaving.is_some()
                    || !urgent && !self.may_transition(&name, now, Some(&r.id), None)?
                {
                    continue;
                }
                if let Err(e) = self.move_copy(
                    &name,
                    r.machine,
                    None,
                    Some(x.replica),
                    "evict",
                    Some(&format!("server {} is leaving", r.machine)),
                    now,
                ) {
                    let key = format!("{}|{name}|{}", r.id, x.replica);
                    if self.evict_stuck.insert(key) {
                        self.log_event("rebalance",json!({"app":name,"replica":x.replica,"machine":r.machine,"cause":"evict","detail":format!("couldn't move {name} replica {} off leaving server {}: {e}",x.replica,r.machine)}),now)?;
                    }
                }
            }
        }
        if here == 0 {
            r.retire = 1;
            self.save_run(r.clone())?;
            self.log_event("retire",json!({"machine":r.machine,"run":r.id,"cause":"evicted","detail":"its copies moved out"}),now)?;
        }
        Ok(())
    }
    pub fn superseded(&self, r: &Run, now: u64) -> bool {
        self.successor_of(r, now).is_some_and(|succ| {
            succ.ready > 0
                && r.status.as_object().into_iter().flatten().all(|(key, st)| {
                    st["s"] != "healthy"
                        || st["r"] == false
                        || (succ.status[key]["s"] == "healthy" && succ.status[key]["r"] != false)
                })
        })
    }
    pub fn retire_stuck_successor(&mut self, r: &mut Run, now: u64) -> anyhow::Result<()> {
        let Some(mut older) = self.predecessor_of(r, now) else {
            return Ok(());
        };
        if now.saturating_sub(r.started) < 25 * MIN {
            return Ok(());
        }
        let desired = self.desired_for(r.machine, true, Some(r), now)?;
        let stuck = older
            .status
            .as_object()
            .into_iter()
            .flatten()
            .find(|(key, st)| {
                desired.get(*key).is_some()
                    && st["s"] == "healthy"
                    && st["r"] != false
                    && !(r.status[*key]["s"] == "healthy" && r.status[*key]["r"] != false)
            })
            .map(|(k, _)| k.clone());
        if let Some(key) = stuck {
            r.retire = 1;
            self.save_run(r.clone())?;
            self.log_event("retire",json!({"machine":r.machine,"run":r.id,"cause":"stuck-successor","detail":format!("after 25 minutes it still couldn't run {key}, which its predecessor runs")}),now)?;
            if older.evict.is_none_or(|v| v == 0) {
                self.start_evict(&mut older, now, "its successor couldn't take over")?;
            }
        }
        Ok(())
    }
    pub fn spread_departures(&mut self, now: u64) {
        if now.saturating_sub(self.last_trim) < 10_000 || self.quiet(now) {
            return;
        }
        self.last_trim = now;
        let live = self.live_runs(now);
        let pools = live
            .iter()
            .map(|r| r.pool.clone())
            .collect::<std::collections::BTreeSet<_>>();
        let departing = self.departing(now);
        for pool in pools {
            if self.capped(pool.as_deref(), now) || departing.iter().any(|r| r.pool == pool) {
                continue;
            }
            let members = live
                .iter()
                .filter(|r| r.pool == pool && self.successor_of(r, now).is_none())
                .collect::<Vec<_>>();
            if members.iter().any(|r| self.due(r, now)) {
                continue;
            }
            if members
                .iter()
                .filter(|r| {
                    self.due_at(r)
                        .is_some_and(|at| at.saturating_sub(now) < 60 * MIN)
                })
                .count()
                < 2
            {
                continue;
            }
            let oldest = members
                .into_iter()
                .filter(|r| {
                    self.due_at(r).is_some()
                        && now.saturating_sub(r.started) >= 180 * MIN
                        && !self.pulled.contains(&r.id)
                })
                .min_by_key(|r| r.started)
                .map(|r| r.id.clone());
            if let Some(id) = oldest {
                self.pulled.insert(id);
            }
        }
    }
    pub fn held_by(&self, _machine: u64, _now: u64) -> Option<&str> {
        None
    }
    pub fn last_seen_of(&self, machine: u64) -> u64 {
        self.runs
            .values()
            .filter(|r| r.machine == machine)
            .map(|r| r.seen)
            .max()
            .unwrap_or(0)
    }
    pub fn slot_taken(&self, machine: u64, now: u64, agent: &str) -> bool {
        if self
            .starts
            .get(&machine)
            .is_some_and(|at| now.saturating_sub(*at) < 8 * MIN)
        {
            return true;
        }
        if self
            .holds
            .get(&machine)
            .is_some_and(|h| h["agent"] != agent && now.saturating_sub(n(h, "at")) < 2 * MIN)
        {
            return true;
        }
        self.runs.values().any(|r| {
            r.machine == machine
                && r.agent != agent
                && r.retire == 0
                && (self.live(r, now) || r.pool.is_none() && now.saturating_sub(r.seen) < 30 * MIN)
        })
    }
    pub fn agent_tag(&self, body: &Value, previous: Option<&Value>) -> anyhow::Result<String> {
        let tag = if let Some(tag) = body.get("tag") {
            tag.as_str()
        } else if body["pool"].is_string() {
            body["pool"].as_str()
        } else {
            previous.map(|p| {
                p["pool"]
                    .as_str()
                    .filter(|s| !s.is_empty())
                    .unwrap_or("standalone")
            })
        };
        let tag = tag.unwrap_or("");
        require(
            spec::tag_valid(tag),
            "A server tag is required: use 1-30 lowercase letters, digits or dashes, or standalone.",
        )?;
        Ok(tag.into())
    }
    pub fn reserve_join(&mut self, body: &Value, now: u64) -> anyhow::Result<Value> {
        if self.recovery_active(now) {
            return Err(http(
                503,
                "recovering fleet inventory; retry joining shortly",
            ));
        }
        let agent = s(body, "agent");
        require(
            !agent.is_empty() && agent.len() <= 100,
            "agent (an ID for this agent) is required",
        )?;
        let tag = self.agent_tag(body, self.agents.get(agent))?;
        let pool = (tag != "standalone").then(|| tag.clone());
        let label = truncate(s(body, "label"), 80);
        let label = (!label.is_empty()).then_some(label);
        let max = self.en("MAX_SLOTS", 0);
        let max = if max == 0 { u32::MAX as u64 } else { max };
        let slot = if let Some(want) = body["want"].as_u64().filter(|n| *n >= 1 && *n <= max) {
            let others = self
                .live_runs(now)
                .into_iter()
                .filter(|r| r.machine == want && r.agent != agent)
                .collect::<Vec<_>>();
            let asked = others.iter().all(|r| r.handover > 0)
                || self
                    .starts
                    .get(&want)
                    .is_some_and(|at| now.saturating_sub(*at) < 8 * MIN);
            if !others.is_empty() && !asked {
                return Err(http(
                    409,
                    format!("server {want} is up and hasn't asked for a replacement"),
                ));
            }
            want
        } else {
            let before = self.agents.get(agent).and_then(|a| a["slot"].as_u64());
            if let Some(before) = before.filter(|m| !self.slot_taken(*m, now, agent)) {
                before
            } else {
                (1..=max)
                    .find(|m| !self.slot_taken(*m, now, agent))
                    .ok_or_else(|| http(503, format!("all {max} slots are taken")))?
            }
        };
        let a = json!({"id":agent,"slot":slot,"pool":pool,"label":label,"joined":now});
        self.store.exec("INSERT INTO agents(id,slot,kind,label,joined,pool) VALUES(?,?,'',?,?,?) ON CONFLICT(id) DO UPDATE SET slot=excluded.slot,label=excluded.label,joined=excluded.joined,pool=excluded.pool",&[json!(agent),json!(slot),json!(label),json!(now),json!(pool)])?;
        self.agents.insert(agent.into(), a);
        self.holds.insert(slot, json!({"agent":agent,"at":now}));
        Ok(json!({"machine":slot,"tag":tag,"domain":self.e("DOMAIN")}))
    }
}
