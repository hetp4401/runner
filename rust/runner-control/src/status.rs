use crate::{
    control::{Control, n, s},
    error::{http, require},
    model::*,
};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};
pub fn event_text(e: &Value) -> String {
    let app = s(e, "app");
    let copy = if app.is_empty() {
        String::new()
    } else {
        format!(
            "{app}{}",
            e["replica"]
                .as_u64()
                .map(|n| format!(" replica {n}"))
                .unwrap_or_default()
        )
    };
    let machine = n(e, "machine");
    let other = n(e, "other");
    let detail = s(e, "detail");
    match s(e, "kind") {
        "move-start" => format!(
            "moving {copy} from server {machine} to server {other}{}",
            if detail.is_empty() {
                String::new()
            } else {
                format!(": {detail}")
            }
        ),
        "move-done" => format!("moved {copy} from server {machine} to server {other}"),
        "move-cancel" => format!("kept {copy} on server {machine}: {detail}"),
        "place" => format!("placed {copy} on server {machine}"),
        "drop" => format!("removed {copy} from server {machine} ({})", s(e, "cause")),
        "strike" => format!("strike against server {machine}: {detail}"),
        "copy-start" => format!(
            "{} server {machine} started {copy}",
            if e["cause"] == "successor" {
                "the successor on"
            } else {
                "a new version on"
            }
        ),
        "dns" => format!("{app}-{} now points at server {machine}", n(e, "replica")),
        "dark" => format!("{copy} went dark: {detail}"),
        "lit" => format!("{copy} is back: {detail}"),
        "quarantine" => format!("server {machine} quarantined: {detail}"),
        "alert" => format!("alert: {detail}"),
        _ => {
            if e["detail"].is_null() {
                format!(
                    "{}{}",
                    s(e, "kind"),
                    if copy.is_empty() {
                        String::new()
                    } else {
                        format!(" {copy}")
                    }
                )
            } else {
                detail.into()
            }
        }
    }
}
fn increment(v: &mut Value, k: &str) {
    v[k] = json!(n(v, k) + 1);
}
impl Control {
    pub fn events(&self, q: &BTreeMap<String, String>) -> anyhow::Result<Value> {
        let mut conditions = vec!["t >= ?".to_string()];
        let mut args = vec![json!(
            q.get("since")
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(0)
        )];
        for key in ["app", "kind", "cause"] {
            if let Some(value) = q.get(key).filter(|s| !s.is_empty()) {
                conditions.push(format!("{key}=?"));
                args.push(json!(value));
            }
        }
        let kinds = q
            .get("kinds")
            .map(String::as_str)
            .unwrap_or("")
            .split(',')
            .filter(|s| {
                !s.is_empty()
                    && s.len() <= 20
                    && s.bytes().all(|c| c.is_ascii_lowercase() || c == b'-')
            })
            .take(20)
            .collect::<Vec<_>>();
        if !kinds.is_empty() {
            conditions.push(format!("kind IN ({})", vec!["?"; kinds.len()].join(",")));
            args.extend(kinds.iter().map(|s| json!(s)));
        }
        if let Some(machine) = q.get("machine").filter(|s| !s.is_empty()) {
            let n = machine
                .parse::<u64>()
                .map_err(|_| http(400, "invalid machine"))?;
            conditions.push("(machine=? OR other=?)".into());
            args.extend([json!(n), json!(n)]);
        }
        let requested = q
            .get("limit")
            .map(|s| s.parse::<u64>())
            .transpose()
            .map_err(|_| http(400, "limit must be a positive whole number"))?
            .unwrap_or(200);
        require(requested > 0, "limit must be a positive whole number")?;
        args.push(json!(requested.min(1000)));
        let mut rows = self.store.rows(
            &format!(
                "SELECT * FROM events WHERE {} ORDER BY id DESC LIMIT ?",
                conditions.join(" AND ")
            ),
            &args,
        )?;
        for e in &mut rows {
            e["text"] = json!(event_text(e));
        }
        Ok(json!({"events":rows}))
    }
    pub fn churn(&mut self, now: u64) -> anyhow::Result<Value> {
        if let Some((_, value)) = self
            .churn_cache
            .as_ref()
            .filter(|(at, _)| now.saturating_sub(*at) < 30_000)
        {
            return Ok(value.clone());
        }
        let blank = || json!({"starts":0,"placed":0,"moved":0,"successor":0,"version":0,"dns":0,"moves":{}});
        let mut h1 = blank();
        let mut h24 = blank();
        let mut apps: BTreeMap<String, Value> = BTreeMap::new();
        self.store.scan("SELECT t,kind,cause,app FROM events WHERE t>? AND kind IN('place','move-start','copy-start','dns')",&[json!(now.saturating_sub(24*60*MIN))],|e|{for b in [&mut h1,&mut h24].into_iter().skip(usize::from(now.saturating_sub(n(&e,"t"))>=60*MIN)){if e["kind"]=="dns"{increment(b,"dns");continue;}increment(b,"starts");if e["kind"]=="place"{increment(b,"placed");}else if e["kind"]=="move-start"{increment(b,"moved");let cause=e["cause"].as_str().unwrap_or("hand");increment(&mut b["moves"],cause);}else{increment(b,if e["cause"]=="successor"{"successor"}else{"version"});}}
            if e["kind"]!="dns"&&!s(&e,"app").is_empty(){let app=s(&e,"app");let a=apps.entry(app.into()).or_insert_with(||json!({"app":app,"h1":0,"h24":0}));increment(a,"h24");if now.saturating_sub(n(&e,"t"))<60*MIN{increment(a,"h1");}}Ok(())})?;
        let mut apps = apps.into_values().collect::<Vec<_>>();
        apps.sort_by_key(|a| std::cmp::Reverse(n(a, "h24")));
        let value = json!({"h1":h1,"h24":h24,"apps":apps});
        self.churn_cache = Some((now, value.clone()));
        Ok(value)
    }
    pub fn departures(&mut self, now: u64) -> anyhow::Result<Value> {
        let runs = self
            .live_runs(now)
            .into_iter()
            .filter(|r| {
                self.predecessor_of(r, now).is_none()
                    && (self.due_at(r).is_some()
                        || r.drain > 0
                        || r.handover > 0
                        || r.evict.is_some_and(|v| v > 0)
                        || self.rolled(r))
            })
            .collect::<Vec<_>>();
        let mut list = Vec::new();
        for r in runs {
            let gate = self.departure_gate(&r, now)?;
            let due = self.due(&r, now);
            list.push(json!({"machine":r.machine,"run":r.id,"dueAt":self.due_at(&r),"deadline":r.deadline,"due":due,"why":due.then(||self.due_why(&r,now)),"leaving":if r.handover>0{Some("handover")}else if r.evict.is_some_and(|v|v>0){Some("evict")}else{None},"since":if r.handover>0{Some(r.handover)}else{r.evict.filter(|v|*v>0)},"how":gate.as_ref().map(|g|&g.0),"wait":gate.as_ref().and_then(|g|g.1.as_ref()),"waitingSince":gate.as_ref().filter(|g|g.1.is_some()).and_then(|_|self.due_wait.get(&r.id).map(|v|v["since"].clone()))}));
        }
        list.sort_by_key(|r| {
            (
                r["leaving"].is_null(),
                r["due"] != true,
                r["dueAt"].as_u64().unwrap_or(u64::MAX),
            )
        });
        let events=self.store.rows("SELECT kind,run FROM events WHERE t>? AND kind IN('depart','server-lost','left','dark')",&[json!(now.saturating_sub(24*60*MIN))])?;
        let dark = events
            .iter()
            .filter(|e| e["kind"] == "dark")
            .map(|e| s(e, "run").to_owned())
            .collect::<BTreeSet<_>>();
        let mut day = json!({"planned":0,"plannedCold":0,"stopped":0,"stoppedCold":0,"lost":0,"handover":null});
        for e in events {
            match s(&e, "kind") {
                "depart" => {
                    increment(&mut day, "planned");
                    if dark.contains(s(&e, "run")) {
                        increment(&mut day, "plannedCold");
                    }
                }
                "left" => {
                    increment(&mut day, "stopped");
                    if dark.contains(s(&e, "run")) {
                        increment(&mut day, "stoppedCold");
                    }
                }
                "server-lost" => increment(&mut day, "lost"),
                _ => (),
            }
        }
        if !self.handover_times.is_empty() {
            let median = |key: &str| {
                let mut values = self
                    .handover_times
                    .iter()
                    .map(|h| n(h, key))
                    .collect::<Vec<_>>();
                values.sort();
                values[values.len() / 2]
            };
            day["handover"] = json!({"n":self.handover_times.len(),"queue":median("queue"),"build":median("build"),"overlap":median("overlap")});
        }
        Ok(json!({"list":list,"day":day,"capped":self.capped_until.values().any(|t|*t>now)}))
    }
    pub fn status(&mut self, now: u64) -> anyhow::Result<Value> {
        let live = self.live_machines(now);
        let mut regions =
            BTreeSet::from(["us-west".to_string(), "us-central".into(), "us-east".into()]);
        regions.extend(
            self.runs
                .values()
                .filter_map(|r| r.region().map(str::to_owned)),
        );
        for p in self.projects.values() {
            if let Some(spec) = self.version(&p.name, p.version)? {
                regions.extend(spec.policy.region.iter().cloned());
                regions.extend(spec.policy.regions.iter().cloned());
            }
        }
        let regions=regions.into_iter().map(|name|json!({"name":name,"label":match name.as_str(){"us-west"=>"US West","us-central"=>"US Central","us-east"=>"US East",_=>&name},"machines":live.values().filter(|r|r.region()==Some(name.as_str())).count()})).collect::<Vec<_>>();
        let projects = self
            .projects
            .values()
            .map(|p| self.describe(p))
            .collect::<anyhow::Result<Vec<_>>>()?;
        let mut runs = self
            .runs
            .values()
            .filter(|r| now.saturating_sub(r.seen) < 60 * MIN)
            .cloned()
            .collect::<Vec<_>>();
        runs.sort_by_key(|r| (r.machine, r.started));
        let slots = runs
            .iter()
            .map(|r| r.machine)
            .chain(
                self.starts
                    .iter()
                    .filter(|(_, at)| now.saturating_sub(**at) < 8 * MIN)
                    .map(|(n, _)| *n),
            )
            .collect::<BTreeSet<_>>();
        let left = self
            .live_runs(now)
            .iter()
            .filter(|r| self.predecessor_of(r, now).is_none() && self.rolled(r))
            .count();
        let cold=self.store.rows("SELECT COUNT(*) AS n,COALESCE(SUM(other),0) AS s FROM events WHERE kind='lit' AND t>?",&[json!(now.saturating_sub(24*60*MIN))])?.into_iter().next().unwrap();
        let recent = self.recent_events.iter().rev().collect::<Vec<_>>();
        let output = json!({"now":now,"domain":self.e("DOMAIN"),"requireDeployAuth":self.e("REQUIRE_DEPLOY_AUTH")=="1","regions":regions,"fleetMode":"passive","groups":self.groups(now),"servers":live.len(),"dnsError":self.dns_error,"dnsNotes":self.dns_notes,
            "rebalance":{"on":self.rebalance_on(),"log":recent.iter().filter(|e|["move-start","move-done","move-cancel","rebalance"].contains(&s(e,"kind"))).take(10).map(|e|json!({"t":e["t"],"text":event_text(e)})).collect::<Vec<_>>(),"hot":live.values().filter(|r|self.is_hot(r,now)).map(|r|r.machine).collect::<Vec<_>>(),"paused":self.paused(now).then(||self.setting_number("rebalance_paused_until")),"state":if !self.rebalance_on(){"off"}else if self.paused(now){"paused"}else if self.quiet(now){"quiet"}else{"on"},"inFlight":self.copies.values().flatten().filter(|x|x.leaving.is_some()).count(),"cap":live.values().filter(|r|r.ready>0&&now.saturating_sub(r.started)>5*MIN).count().div_ceil(10).clamp(1,8)},
            "roll":(left>0).then(||json!({"since":(self.setting_number("roll")>0).then(||self.setting_number("roll")),"left":left})),
            "dark":self.dark.iter().map(|(key,d)|{let (app,k)=key.rsplit_once('|').unwrap_or((key,"0"));json!({"app":app,"replica":k.parse::<u32>().unwrap_or(0),"since":d["since"],"machine":d["machine"],"why":d["why"]})}).collect::<Vec<_>>(),
            "cold":{"spells":n(&cold,"n")+self.dark.len() as u64,"seconds":n(&cold,"s")+self.dark.values().map(|d|(now.saturating_sub(n(d,"since"))+500)/1000).sum::<u64>()},
            "alerts":recent.iter().filter(|e|e["kind"]=="alert"&&now.saturating_sub(n(e,"t"))<24*60*MIN).take(20).map(|e|json!({"t":e["t"],"cause":e["cause"],"app":e["app"],"machine":e["machine"],"text":e["detail"]})).collect::<Vec<_>>(),
            "events":recent.iter().take(50).map(|e|{let mut v=(*e).clone();v["text"]=json!(event_text(e));v}).collect::<Vec<_>>(),
            "starting":self.starts.iter().filter(|(m,at)|now.saturating_sub(**at)<8*MIN&&!live.contains_key(m)).map(|(m,_)|*m).collect::<Vec<_>>(),"settling":self.settling(now),"slots":slots,"projects":projects,
            "runs":runs.iter().map(|r|json!({"id":r.id,"machine":r.machine,"started":r.started,"seen":r.seen,"live":self.live(r,now),"ready":r.ready>0,"handover":r.handover>0,"retiring":r.retire>0,"protocol":r.n("protocol").max(1),"publicTunnels":r.extra.get("publicTunnels").cloned().unwrap_or_else(||json!({})),"recoveryAt":r.v("recoveryAt"),"witnessAt":r.v("witnessAt"),"pool":r.pool,"tag":r.tag(),"canReplace":r.b("starts"),"draining":r.drain>0,"label":r.label,"rtt":r.v("rtt"),"quarantine":r.quarantine.filter(|until|*until>now).map(|until|json!({"until":until,"why":self.sick_why.get(&r.id),"sick":self.sick_why.contains_key(&r.id)})),"deadline":r.deadline,"dueAt":self.due_at(r),"departing":if r.handover>0{Some("handover")}else if r.evict.is_some_and(|v|v>0){Some("evict")}else{None},"edges":r.v("edges"),"cpus":r.v("cpus"),"disk":r.v("disk"),"strikes":self.strikes.get(&r.id).into_iter().flatten().filter(|x|now.saturating_sub(n(x,"t"))<30*MIN).map(|x|x["why"].clone()).collect::<Vec<_>>(),"location":r.v("location"),"region":r.region(),"projects":r.status})).collect::<Vec<_>>()});
        let mut output = output;
        output["churn"] = self.churn(now)?;
        output["departures"] = self.departures(now)?;
        Ok(output)
    }
}
