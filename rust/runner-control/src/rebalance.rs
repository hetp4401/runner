use crate::{
    control::{Control, f, n, s, truncate},
    model::*,
};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};
#[derive(Clone)]
struct Hot {
    cpu: f64,
    mem: f64,
    kind: &'static str,
    score: f64,
    list: Vec<Value>,
}
struct Share {
    cpu: f64,
    mem: f64,
    mem_bytes: f64,
    copies: Vec<Copy>,
}
struct Context {
    now: u64,
    up: BTreeMap<u64, Run>,
    settled: BTreeSet<u64>,
    moves24: Vec<Value>,
    moves2h: Vec<Value>,
    counts: BTreeMap<u64, usize>,
    hot_runs: Vec<(Run, Hot)>,
    cap: usize,
    in_flight: usize,
    arriving: BTreeSet<u64>,
}
#[derive(PartialEq)]
enum Step {
    None,
    Moved,
    Paused,
}
impl Context {
    fn free(&self, kind: &str) -> bool {
        self.in_flight
            < if kind == "hot" || self.hot_runs.is_empty() {
                self.cap
            } else {
                self.cap.saturating_sub(1)
            }
    }
    fn dests(&self, from: u64, ok: impl Fn(u64) -> bool) -> BTreeMap<u64, Run> {
        self.up
            .iter()
            .filter(|(m, _)| {
                **m != from && self.settled.contains(m) && !self.arriving.contains(m) && ok(**m)
            })
            .map(|(m, r)| (*m, r.clone()))
            .collect()
    }
}
impl Control {
    pub fn is_hot(&self, r: &Run, now: u64) -> bool {
        self.hotness(r, now).is_some()
    }
    fn hotness(&self, r: &Run, now: u64) -> Option<Hot> {
        let mut list = self
            .minutes
            .get(&r.id)?
            .iter()
            .filter(|m| now.saturating_sub(n(m, "t")) <= 7 * MIN)
            .rev()
            .take(5)
            .cloned()
            .collect::<Vec<_>>();
        list.reverse();
        if list.len() < 4 {
            return None;
        }
        if list
            .iter()
            .filter(|m| f(m, "cpu") - f(m, "steal") > 85. || f(m, "mem") > 90.)
            .count()
            < 4
        {
            return None;
        }
        let cpu = list
            .iter()
            .map(|m| f(m, "cpu") - f(m, "steal"))
            .sum::<f64>()
            / list.len() as f64;
        let mem = list.iter().map(|m| f(m, "mem")).sum::<f64>() / list.len() as f64;
        let cpus = list
            .iter()
            .filter(|m| f(m, "cpu") - f(m, "steal") > 85.)
            .count();
        let mems = list.iter().filter(|m| f(m, "mem") > 90.).count();
        let kind = if cpus != mems {
            if mems > cpus { "mem" } else { "cpu" }
        } else if mem - 90. > cpu - 85. {
            "mem"
        } else {
            "cpu"
        };
        Some(Hot {
            cpu,
            mem,
            kind,
            score: (cpu - 85.).max(mem - 90.),
            list,
        })
    }
    fn app_shares(&self, r: &Run, list: &[Value]) -> BTreeMap<String, Share> {
        let cores = r.n("cpus").max(1);
        let cores = if r.n("cpus") == 0 { 4 } else { cores } as f64;
        let mem_total = list
            .last()
            .map(|v| f(v, "memTotal"))
            .filter(|v| *v > 0.)
            .unwrap_or(1.);
        let mut out = BTreeMap::new();
        for (name, copies) in &self.copies {
            let all = copies
                .iter()
                .filter(|x| x.machine == r.machine)
                .collect::<Vec<_>>();
            let staying = all
                .iter()
                .filter(|x| x.leaving.is_none())
                .map(|x| (*x).clone())
                .collect::<Vec<_>>();
            let rows = list
                .iter()
                .map(|m| &m["a"][name])
                .filter(|m| m.is_object())
                .collect::<Vec<_>>();
            if staying.is_empty() || rows.is_empty() {
                continue;
            }
            let cpu = rows.iter().map(|m| f(m, "cpu")).sum::<f64>()
                / rows.len() as f64
                / cores
                / all.len() as f64;
            let mem_bytes = rows.iter().map(|m| f(m, "mem")).sum::<f64>()
                / rows.len() as f64
                / all.len() as f64;
            out.insert(
                name.clone(),
                Share {
                    cpu,
                    mem: 100. * mem_bytes / mem_total,
                    mem_bytes,
                    copies: staying,
                },
            );
        }
        out
    }
    pub fn has_room(&self, machine: u64, now: u64) -> bool {
        self.live_metrics.get(&machine).is_some_and(|m| {
            now.saturating_sub(n(m, "t")) <= 3 * self.live_ms
                && f(&m["h"], "memTotal") > 0.
                && f(&m["h"], "cpu") < 70.
                && 100. * f(&m["h"], "memUsed") / f(&m["h"], "memTotal") < 80.
        })
    }
    fn never_auto(&self, name: &str) -> anyhow::Result<Option<&'static str>> {
        let Some(p) = self.projects.get(name) else {
            return Ok(Some("it isn't running"));
        };
        Ok(
            if p.enabled == 0 || self.version(name, p.version)?.is_none() {
                Some("it isn't running")
            } else if p.stable != Some(p.version) {
                Some("its latest version hasn't settled yet")
            } else {
                None
            },
        )
    }
    fn auto_moved_at(&mut self, name: &str, now: u64) -> anyhow::Result<()> {
        self.auto_moved.insert(name.into(), now);
        let recent = self
            .auto_moved
            .iter()
            .filter(|(_, t)| now.saturating_sub(**t) < 30 * MIN)
            .collect::<BTreeMap<_, _>>();
        let text = serde_json::to_string(&recent)?;
        self.set_setting("auto_moved", text)
    }
    fn hot_backoff(&self, name: &str, k: u32, moves: &[Value]) -> Option<u64> {
        let mine = moves
            .iter()
            .filter(|e| e["cause"] == "hot" && e["app"] == name && e["replica"] == k)
            .map(|e| n(e, "t"))
            .collect::<Vec<_>>();
        if mine.len() >= 3 {
            None
        } else if mine.is_empty() {
            Some(0)
        } else {
            Some(mine.iter().max().unwrap() + 30 * (1 << (mine.len() - 1)) * MIN)
        }
    }
    fn thrash(
        &self,
        name: &str,
        k: u32,
        from: u64,
        to: u64,
        moves: &[Value],
        now: u64,
    ) -> Option<String> {
        if moves
            .iter()
            .filter(|e| {
                (e["machine"] == from && e["other"] == to)
                    || (e["machine"] == to && e["other"] == from)
            })
            .count()
            >= 3
        {
            return Some(format!(
                "it would be the 4th automatic move between servers {from} and {to} in 2 hours"
            ));
        }
        if moves
            .iter()
            .filter(|e| now.saturating_sub(n(e, "t")) < 60 * MIN)
            .count()
            >= 6
        {
            return Some("it would be the 7th automatic move in an hour".into());
        }
        if moves.iter().any(|e| e["app"] == name && e["replica"] == k) {
            return Some(format!(
                "{name} replica {k} would move automatically a second time in 2 hours"
            ));
        }
        None
    }
    pub fn paused(&self, now: u64) -> bool {
        self.settings
            .get("rebalance_paused_until")
            .and_then(|s| s.parse::<u64>().ok())
            .is_some_and(|at| at > now)
    }
    pub fn pause(&mut self, why: &str, now: u64) -> anyhow::Result<()> {
        self.set_setting("rebalance_paused_until", now + 60 * MIN)?;
        self.log_event(
            "pause",
            json!({"cause":"thrash","detail":format!("automatic moves paused for an hour: {why}")}),
            now,
        )?;
        self.alert(
            "paused",
            json!({}),
            &format!("automatic moves are paused for an hour: {why}"),
            now,
        )
    }
    pub fn note_rebalance(
        &mut self,
        subject: &str,
        text: &str,
        now: u64,
        mut fields: Value,
    ) -> anyhow::Result<()> {
        if self
            .noted
            .get(subject)
            .and_then(Value::as_u64)
            .is_some_and(|at| now.saturating_sub(at) < 30 * MIN)
        {
            return Ok(());
        }
        self.noted.insert(subject.into(), json!(now));
        fields["detail"] = json!(text);
        self.log_event("rebalance", fields, now)
    }
    fn start_move(
        &mut self,
        ctx: &mut Context,
        name: &str,
        x: &Copy,
        dest: &Candidate,
        cause: &str,
        why: &str,
    ) -> anyhow::Result<Step> {
        let now = ctx.now;
        if ["hot", "failed"].contains(&cause) {
            if let Some(t) =
                self.thrash(name, x.replica, x.machine, dest.machine, &ctx.moves2h, now)
            {
                self.pause(&t, now)?;
                return Ok(Step::Paused);
            }
        }
        if let Err(e) = self.move_copy(
            name,
            x.machine,
            Some(dest.machine),
            Some(x.replica),
            cause,
            Some(why),
            now,
        ) {
            self.note_rebalance(
                &format!("fail|{name}|{}", x.replica),
                &format!(
                    "couldn't move {name} replica {} off server {}: {e}",
                    x.replica, x.machine
                ),
                now,
                json!({"app":name,"replica":x.replica,"machine":x.machine}),
            )?;
            return Ok(Step::None);
        }
        if let Some(mut nx) = self
            .copies_on(name, dest.machine)
            .into_iter()
            .find(|y| y.replica == x.replica && y.leaving.is_none())
        {
            nx.reason = format!(
                "moved here from server {} automatically: {why}; {}",
                x.machine, dest.reason
            );
            self.save_copy(name, nx)?;
        }
        ctx.in_flight += 1;
        ctx.arriving.insert(dest.machine);
        *ctx.counts.entry(dest.machine).or_default() += 1;
        if cause != "unstack" {
            self.auto_moved_at(name, now)?;
        }
        if ["hot", "failed"].contains(&cause) {
            ctx.moves2h.push(json!({"app":name,"replica":x.replica,"machine":x.machine,"other":dest.machine,"t":now,"cause":cause}));
        }
        Ok(Step::Moved)
    }
    pub fn rebalance(&mut self, now: u64) -> anyhow::Result<()> {
        if !self.rebalance_on()
            || now.saturating_sub(self.last_rebalance) < 30_000
            || self.quiet(now)
            || self.paused(now)
        {
            return Ok(());
        }
        self.last_rebalance = now;
        let up = self.live_machines(now);
        let settled = up
            .values()
            .filter(|r| {
                r.ready > 0
                    && now.saturating_sub(r.started) > 5 * MIN
                    && self.live_metrics.contains_key(&r.machine)
            })
            .cloned()
            .collect::<Vec<_>>();
        if settled.len() < 2 {
            return Ok(());
        }
        let mut arriving = BTreeSet::new();
        let mut in_flight = 0;
        for x in self.copies.values().flatten() {
            if let Some(to) = x.to() {
                in_flight += 1;
                arriving.insert(to);
            }
        }
        let moves24=self.store.rows("SELECT app,replica,machine,other,t,cause FROM events WHERE kind='move-start' AND cause IN('hot','failed') AND t>?",&[json!(now.saturating_sub(24*60*MIN))])?;
        let moves2h = moves24
            .iter()
            .filter(|e| now.saturating_sub(n(e, "t")) < 120 * MIN)
            .cloned()
            .collect();
        let mut hot_runs = settled
            .iter()
            .filter_map(|r| self.hotness(r, now).map(|h| (r.clone(), h)))
            .collect::<Vec<_>>();
        hot_runs.sort_by(|a, b| b.1.score.total_cmp(&a.1.score));
        let mut ctx = Context {
            now,
            up,
            settled: settled.iter().map(|r| r.machine).collect(),
            moves24,
            moves2h,
            counts: self.placement_counts(),
            hot_runs,
            cap: settled.len().div_ceil(10).clamp(1, 8),
            in_flight,
            arriving,
        };
        if self.rebalance_hot(&mut ctx)? == Step::Paused {
            return Ok(());
        }
        if self.rebalance_failed(&mut ctx)? == Step::Paused {
            return Ok(());
        }
        self.rebalance_unstack(&mut ctx)?;
        Ok(())
    }
    fn rebalance_hot(&mut self, ctx: &mut Context) -> anyhow::Result<Step> {
        let now = ctx.now;
        let cooling = self
            .settings
            .get("last_hot_move")
            .and_then(|s| s.parse::<u64>().ok())
            .is_some_and(|at| now.saturating_sub(at) < 10 * MIN);
        for (mut r, hot) in ctx.hot_runs.clone() {
            let what = format!(
                "server {} is hot ({} {:.0}% over 5 minutes)",
                r.machine,
                if hot.kind == "cpu" { "cpu" } else { "memory" },
                if hot.kind == "cpu" { hot.cpu } else { hot.mem }
            );
            let off = ctx
                .moves24
                .iter()
                .filter(|e| {
                    e["cause"] == "hot"
                        && e["machine"] == r.machine
                        && now.saturating_sub(n(e, "t")) < 60 * MIN
                })
                .collect::<Vec<_>>();
            if off
                .iter()
                .map(|e| n(e, "t"))
                .max()
                .is_some_and(|at| now.saturating_sub(at) >= 10 * MIN)
                && r.quarantine.unwrap_or(0) <= now
            {
                self.quarantine(
                    &mut r,
                    "still hot 10 minutes after an app was moved off it",
                    now,
                    false,
                )?;
                ctx.up.insert(r.machine, r.clone());
            }
            if off.len() >= 2 {
                self.note_rebalance(
                    &format!("hot2|{}", r.id),
                    &format!("{what}, but 2 apps were moved off it this hour already"),
                    now,
                    json!({"machine":r.machine}),
                )?;
                continue;
            }
            let here = self
                .copies
                .iter()
                .flat_map(|(name, list)| {
                    list.iter()
                        .filter(|x| x.machine == r.machine && x.leaving.is_none())
                        .map(move |x| (name.clone(), x.clone()))
                })
                .collect::<Vec<_>>();
            if here.iter().any(|(name, x)| {
                now.saturating_sub(self.built_at(x, Some(&r))) < self.en("YOUNG_MS", 10 * MIN)
                    || !self.copy_healthy(name, x, Some(&r), None)
            }) {
                self.note_rebalance(
                    &format!("hotwait|{}", r.id),
                    &format!("{what}, but a copy on it is new or not healthy: waiting"),
                    now,
                    json!({"machine":r.machine}),
                )?;
                continue;
            }
            let shares = self.app_shares(&r, &hot.list);
            let share = |s: &Share| if hot.kind == "cpu" { s.cpu } else { s.mem };
            if shares.values().map(share).fold(0., f64::max) < 15. {
                self.note_rebalance(
                    &format!("hotapps|{}", r.id),
                    &format!("{what}, but no app on it uses 15% of it: nothing to move"),
                    now,
                    json!({"machine":r.machine}),
                )?;
                continue;
            }
            if cooling || !ctx.free("hot") {
                self.note_rebalance(
                    &format!("hotcool|{}", r.id),
                    &format!(
                        "{what}: waiting, {}",
                        if cooling {
                            "a hot move was made less than 10 minutes ago"
                        } else {
                            "enough moves are under way"
                        }
                    ),
                    now,
                    json!({"machine":r.machine}),
                )?;
                continue;
            }
            let mut candidates = Vec::new();
            for (name, s) in &shares {
                if share(s) < 15.
                    || self
                        .auto_moved
                        .get(name)
                        .is_some_and(|at| now.saturating_sub(*at) < 30 * MIN)
                    || !self.may_transition(name, now, None, None)?
                {
                    continue;
                }
                for x in &s.copies {
                    if self
                        .hot_backoff(name, x.replica, &ctx.moves24)
                        .is_some_and(|at| at <= now)
                        && self.never_auto(name)?.is_none()
                    {
                        candidates.push((name.clone(), x.clone(), s));
                    }
                }
            }
            candidates.sort_by(|a, b| {
                share(b.2)
                    .total_cmp(&share(a.2))
                    .then(b.1.replica.cmp(&a.1.replica))
            });
            let Some((name, x, pick)) = candidates.into_iter().next() else {
                self.note_rebalance(&format!("hotnone|{}",r.id),&format!("{what}, but the apps using it can't move now (cooldown, budget, backoff or policy)"),now,json!({"machine":r.machine}))?;
                continue;
            };
            let took_hot = ctx
                .moves24
                .iter()
                .filter(|e| e["cause"] == "hot" && now.saturating_sub(n(e, "t")) < 30 * MIN)
                .map(|e| n(e, "other"))
                .collect::<BTreeSet<_>>();
            let dests = ctx.dests(r.machine, |m| {
                let d = &ctx.up[&m];
                let room = self.headroom(m, now);
                let total = self
                    .live_metrics
                    .get(&m)
                    .map(|m| f(&m["h"], "memTotal"))
                    .unwrap_or(0.);
                let cores = |r: &Run| {
                    if r.n("cpus") == 0 {
                        4.
                    } else {
                        r.n("cpus") as f64
                    }
                };
                let cpu = room.cpu + pick.cpu * (cores(&r) / cores(d));
                let mem = room.mem
                    + if total > 0. {
                        100. * pick.mem_bytes / total
                    } else {
                        pick.mem
                    };
                !took_hot.contains(&m) && cpu < 70. && mem < 80. && self.hotness(d, now).is_none()
            });
            let Some(dest) = self.best_machine(&name, &dests, &ctx.counts, now, x.replica)? else {
                self.note_rebalance(
                    &format!("hotroom|{}", r.id),
                    &format!(
                        "{what}, but no server has room for {name} ({:.0}% of it)",
                        share(pick)
                    ),
                    now,
                    json!({"machine":r.machine,"app":name}),
                )?;
                continue;
            };
            let result = self.start_move(
                ctx,
                &name,
                &x,
                &dest,
                "hot",
                &format!("{what}; {name} used {:.0}% of it", share(pick)),
            )?;
            if result == Step::Moved {
                self.set_setting("last_hot_move", now)?;
            }
            if result != Step::None {
                return Ok(result);
            }
        }
        Ok(Step::None)
    }
    fn rebalance_failed(&mut self, ctx: &mut Context) -> anyhow::Result<Step> {
        let now = ctx.now;
        for (name, list) in self.copies.clone() {
            let Some(p) = self.projects.get(&name).filter(|p| p.enabled > 0) else {
                continue;
            };
            let Some(v) = p.wanted() else {
                continue;
            };
            for x in list {
                if x.leaving.is_some() || !ctx.settled.contains(&x.machine) {
                    continue;
                }
                let run = &ctx.up[&x.machine];
                let st = &run.status[&x.key];
                if !failing(st) || st["v"] != v || !self.any_ready(&name, &ctx.up, Some(x.replica))
                {
                    continue;
                }
                let since = self.fail_clock(&name, &x, run, now, true);
                if now.saturating_sub(since) < 10 * MIN {
                    continue;
                }
                self.note_version_fail(&name, v, x.machine, now)?;
                if self
                    .version_fails
                    .get(&format!("{name}@{v}"))
                    .is_some_and(|s| s.len() >= 2)
                {
                    continue;
                }
                if !ctx.free("failed") {
                    return Ok(Step::None);
                }
                let no = self.never_auto(&name)?;
                if no.is_some()
                    || self
                        .auto_moved
                        .get(&name)
                        .is_some_and(|at| now.saturating_sub(*at) < 30 * MIN)
                    || !self.may_transition(&name, now, None, Some(x.replica))?
                {
                    if let Some(no) = no {
                        self.note_rebalance(&format!("failno|{name}|{}",x.replica),&format!("{name} replica {} is failing on server {}, but it's left there: {no}",x.replica,x.machine),now,json!({"app":name,"replica":x.replica,"machine":x.machine}))?;
                    }
                    continue;
                }
                let Some(dest) = self.best_machine(
                    &name,
                    &ctx.dests(x.machine, |_| true),
                    &ctx.counts,
                    now,
                    x.replica,
                )?
                else {
                    self.note_rebalance(&format!("failroom|{name}|{}",x.replica),&format!("{name} replica {} is failing on server {}, and no other server can take it",x.replica,x.machine),now,json!({"app":name,"replica":x.replica,"machine":x.machine}))?;
                    continue;
                };
                let error = truncate(s(st, "e").lines().last().unwrap_or(""), 160);
                let why = format!(
                    "it {} on server {} for {} minutes while healthy elsewhere{}",
                    if st["s"] == "failed" {
                        "failed"
                    } else {
                        "wasn't ready"
                    },
                    x.machine,
                    (now - since + MIN / 2) / MIN,
                    if error.is_empty() {
                        String::new()
                    } else {
                        format!(" ({error})")
                    }
                );
                let result = self.start_move(ctx, &name, &x, &dest, "failed", &why)?;
                if result == Step::Paused {
                    return Ok(result);
                }
                if result == Step::Moved {
                    self.failed_on
                        .insert(format!("{name}|{}", x.machine), now + 6 * 60 * MIN);
                }
            }
        }
        Ok(Step::None)
    }
    fn rebalance_unstack(&mut self, ctx: &mut Context) -> anyhow::Result<()> {
        let now = ctx.now;
        for (name, list) in self.copies.clone() {
            if !ctx.free("unstack") {
                break;
            }
            if !self.may_transition(&name, now, None, None)? {
                continue;
            }
            let mut by: BTreeMap<u64, usize> = BTreeMap::new();
            for x in &list {
                if x.leaving.is_none()
                    && now.saturating_sub(self.built_at(x, ctx.up.get(&x.machine)))
                        >= self.en("YOUNG_MS", 10 * MIN)
                {
                    *by.entry(x.machine).or_default() += 1;
                }
            }
            let Some((from, count)) = by
                .into_iter()
                .filter(|(m, n)| *n > 1 && ctx.settled.contains(m))
                .max_by_key(|(_, n)| *n)
            else {
                continue;
            };
            let Some(x) = list
                .iter()
                .filter(|x| x.machine == from && x.leaving.is_none())
                .max_by_key(|x| x.replica)
            else {
                continue;
            };
            if self.never_auto(&name)?.is_some() {
                continue;
            }
            let dests = ctx.dests(from, |m| {
                !list.iter().any(|x| x.machine == m) && self.has_room(m, now)
            });
            if let Some(dest) = self.best_machine(&name, &dests, &ctx.counts, now, x.replica)? {
                self.start_move(
                    ctx,
                    &name,
                    x,
                    &dest,
                    "unstack",
                    &format!(
                        "server {from} ran {count} copies of {name} and server {} none",
                        dest.machine
                    ),
                )?;
            }
        }
        Ok(())
    }
}
pub fn failing(st: &Value) -> bool {
    st.is_object() && (st["s"] == "failed" || (st["s"] == "healthy" && st["r"] == false))
}
