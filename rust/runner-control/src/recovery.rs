use crate::{
    control::{Control, base_app, n, s},
    error::{http, require},
    model::*,
    storage::durable_setting,
};
use serde_json::{Value, json};
use std::collections::BTreeSet;
use std::sync::Arc;
impl Control {
    pub fn recovery_witnesses(&self, now: u64) -> Vec<String> {
        let mut candidates = self
            .live_runs(now)
            .into_iter()
            .filter(|r| r.n("protocol") >= 2 && r.ready > 0)
            .collect::<Vec<_>>();
        candidates.sort_by(|a, b| {
            b.deadline
                .unwrap_or(b.started + 86400_000)
                .cmp(&a.deadline.unwrap_or(a.started + 86400_000))
                .then(a.id.cmp(&b.id))
        });
        let mut chosen = Vec::new();
        let mut regions = BTreeSet::new();
        for r in &candidates {
            let region = r.region().or(r.pool.as_deref()).unwrap_or("unknown");
            if !regions.insert(region) {
                continue;
            }
            chosen.push(r.id.clone());
            if chosen.len() == 3 {
                return chosen;
            }
        }
        for r in &candidates {
            if !chosen.contains(&r.id) {
                chosen.push(r.id.clone());
                if chosen.len() == 3 {
                    break;
                }
            }
        }
        chosen
    }
    pub fn recovery_for(&self, r: &Run, now: u64) -> anyhow::Result<Value> {
        let runtime = self
            .store
            .rows("SELECT * FROM settings", &[])?
            .into_iter()
            .filter(|x| {
                !durable_setting(s(x, "key")) && !["pools", "pool_asks"].contains(&s(x, "key"))
            })
            .collect::<Vec<_>>();
        let row = self
            .store
            .rows("SELECT * FROM runs WHERE id=?", &[json!(r.id)])?
            .into_iter()
            .next()
            .unwrap_or(Value::Null);
        let copies = self
            .store
            .rows("SELECT * FROM copies WHERE machine=?", &[json!(r.machine)])?;
        let starts = self
            .store
            .rows("SELECT * FROM starts WHERE by=?", &[json!(r.id)])?;
        let mut events = self
            .recent_events
            .iter()
            .filter(|e| e["machine"] == r.machine || e["other"] == r.machine)
            .rev()
            .take(40)
            .cloned()
            .collect::<Vec<_>>();
        events.reverse();
        let mut payload = json!({"v":1,"at":now,"agent":r.agent,"run":r.id,"machine":r.machine,"runRow":row,"copies":copies,"starts":starts,"runtime":runtime,"location":r.v("location"),"region":r.v("region"),"events":events});
        if self.recovery_witnesses(now).contains(&r.id) {
            let mut rows = self.store.rows(
                "SELECT * FROM runs WHERE seen>?",
                &[json!(now.saturating_sub(15 * MIN))],
            )?;
            for row in &mut rows {
                row["region"] = self
                    .runs
                    .get(s(row, "id"))
                    .map(|r| r.v("region").clone())
                    .unwrap_or(Value::Null);
            }
            payload["fleet"] = json!({"runs":rows,"copies":self.store.rows("SELECT * FROM copies",&[])?,"starts":self.store.rows("SELECT * FROM starts",&[])?,"startPools":self.start_pools.iter().map(|(n,p)|json!([n,p])).collect::<Vec<_>>(),"events":self.recent_events});
        }
        let payload = serde_json::to_string(&payload)?;
        let signature =
            runner_core::auth::sign(self.e("STATE_SIGNING_KEY").as_bytes(), payload.as_bytes());
        Ok(json!({"payload":payload,"signature":signature}))
    }
    pub fn read_recovery(&self, body: &Value, now: u64) -> anyhow::Result<Option<Value>> {
        let receipt = &body["recovery"];
        if receipt.is_null() || self.e("STATE_SIGNING_KEY").is_empty() {
            return Ok(None);
        }
        let payload = s(receipt, "payload");
        let signature = s(receipt, "signature");
        require(
            signature.len() == 64
                && signature
                    .bytes()
                    .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
                && runner_core::auth::verify(
                    self.e("STATE_SIGNING_KEY").as_bytes(),
                    payload.as_bytes(),
                    signature,
                ),
            "invalid recovery receipt",
        )?;
        let saved: Value = serde_json::from_str(payload)
            .map_err(|_| http(400, "invalid recovery receipt payload"))?;
        let previous = n(body, "protocol") >= 3
            && saved["run"] == body["previousRun"]
            && saved["run"] != body["run"]
            && n(body, "started") > n(&saved["runRow"], "started");
        require(
            saved["v"] == 1
                && (saved["run"] == body["run"] || previous)
                && saved["agent"] == body["agent"]
                && saved["machine"] == body["machine"]
                && saved["at"].as_u64().is_some_and(|at| at <= now + 60_000),
            "recovery receipt does not identify this agent",
        )?;
        Ok(Some(saved))
    }
    pub fn recover_run(
        &mut self,
        row: Value,
        at: u64,
        location: Value,
        now: u64,
    ) -> anyhow::Result<()> {
        let id = s(&row, "id");
        if id.is_empty()
            || at
                <= self
                    .recovery_seen
                    .get(&format!("run:{id}"))
                    .copied()
                    .unwrap_or(0)
        {
            return Ok(());
        }
        let id = id.to_owned();
        let observed = self
            .runs
            .get(&id)
            .filter(|_| self.observed_runs.contains(&id))
            .cloned();
        let mut r = Self::run_row(row)?;
        r.status = Arc::new(self.reported_status(&r.status, None, None, now));
        r.set("savedSeen", r.seen);
        r.set("location", location);
        if let Some(current) = observed {
            r.status = current.status;
            r.seen = current.seen;
            r.ready = current.ready;
            for key in [
                "protocol",
                "recoveryAt",
                "witnessAt",
                "cpus",
                "disk",
                "edges",
                "rtt",
                "location",
                "region",
                "starts",
                "ip",
            ] {
                if let Some(v) = current.extra.get(key) {
                    r.extra.insert(key.into(), v.clone());
                }
            }
        }
        self.recovery_seen.insert(format!("run:{id}"), at);
        self.recovery_missing.remove(&id);
        self.agents.insert(
            r.agent.clone(),
            json!({"id":r.agent,"slot":r.machine,"pool":r.pool,"label":r.label,"joined":r.started}),
        );
        self.save_run(r)
    }
    pub fn recover_copies(&mut self, machine: u64, rows: &[Value], at: u64) -> anyhow::Result<()> {
        let key = format!("machine:{machine}");
        if at <= self.recovery_seen.get(&key).copied().unwrap_or(0) {
            return Ok(());
        }
        self.recovery_seen.insert(key, at);
        for list in self.copies.values_mut() {
            list.retain(|x| x.machine != machine);
        }
        self.store
            .exec("DELETE FROM copies WHERE machine=?", &[json!(machine)])?;
        for row in rows {
            let name = s(row, "name");
            if n(row, "machine") != machine || !self.projects.contains_key(name) {
                continue;
            }
            let copy = Self::copy_row(row.clone())?;
            self.save_copy(name, copy)?;
        }
        Ok(())
    }
    pub fn recover_agent(
        &mut self,
        body: &Value,
        saved: Option<&Value>,
        now: u64,
    ) -> anyhow::Result<()> {
        let Some(saved) = saved else {
            return self.recover_late_agent(body, None, now);
        };
        self.recovery_missing.remove(s(saved, "run"));
        let at = n(saved, "at");
        let fleet = &saved["fleet"];
        if !fleet.is_null() {
            for row in array(&fleet["runs"]) {
                self.recover_run(row.clone(), at, Value::Null, now)?;
            }
            let machines = array(&fleet["copies"])
                .iter()
                .chain(array(&fleet["runs"]))
                .map(|row| n(row, "machine"))
                .chain(self.copies.values().flatten().map(|x| x.machine))
                .collect::<BTreeSet<_>>();
            for machine in machines {
                self.recover_copies(machine, array(&fleet["copies"]), at)?;
            }
        }
        if !saved["runRow"].is_null() {
            let mut row = saved["runRow"].clone();
            row["region"] = saved
                .get("region")
                .filter(|v| !v.is_null())
                .cloned()
                .unwrap_or(row["region"].clone());
            self.recover_run(row, at, saved["location"].clone(), now)?;
        }
        self.recover_copies(n(saved, "machine"), array(&saved["copies"]), at)?;
        if at > self.recovery_settings_at {
            self.recovery_settings_at = at;
            for setting in array(&saved["runtime"]) {
                let key = s(setting, "key");
                if durable_setting(key) || ["pools", "pool_asks"].contains(&key) {
                    continue;
                }
                self.set_setting(key, s(setting, "value"))?;
            }
            self.auto_moved = self
                .settings
                .get("auto_moved")
                .and_then(|v| serde_json::from_str(v).ok())
                .unwrap_or_default();
            self.version_fails = self
                .settings
                .get("version_fails")
                .and_then(|v| serde_json::from_str(v).ok())
                .unwrap_or_default();
            self.mass_arms = self
                .settings
                .get("mass_arms")
                .and_then(|v| serde_json::from_str(v).ok())
                .unwrap_or_default();
            self.sick_drains = self
                .settings
                .get("sick_drains")
                .and_then(|v| serde_json::from_str(v).ok())
                .unwrap_or_default();
        }
        for event in array(&fleet["events"])
            .iter()
            .chain(array(&saved["events"]))
        {
            if now.saturating_sub(n(event, "t")) > 30 * 86400_000
                || self.recent_events.iter().any(|x| {
                    ["t", "kind", "run", "detail"]
                        .iter()
                        .all(|k| x[*k] == event[*k])
                })
            {
                continue;
            }
            self.log_event(s(event, "kind"), event.clone(), now)?;
        }
        self.recent_events
            .make_contiguous()
            .sort_by_key(|e| n(e, "t"));
        Ok(())
    }
    pub fn recover_late_agent(
        &mut self,
        body: &Value,
        saved: Option<&Value>,
        now: u64,
    ) -> anyhow::Result<()> {
        let machine = n(body, "machine");
        let inventory = body["inventory"].as_object();
        let rows = if let Some(saved) = saved {
            array(&saved["copies"]).to_vec()
        } else {
            inventory
                .into_iter()
                .flatten()
                .map(|(key, value)| {
                    let mut row = value.clone();
                    if !row.is_object() {
                        row = json!({});
                    }
                    row["name"] = value["app"].clone();
                    row["machine"] = json!(machine);
                    row["key"] = json!(key);
                    row["replica"] = value["replica"].clone();
                    row["since"] = json!(now);
                    row["reason"] = json!("reported running inventory");
                    row["leaving"] = Value::Null;
                    row
                })
                .collect::<Vec<_>>()
        };
        if let Some(saved) = saved {
            if !saved["runRow"].is_null() && !self.runs.contains_key(s(saved, "run")) {
                let mut row = saved["runRow"].clone();
                row["region"] = saved["region"].clone();
                self.recover_run(row, n(saved, "at"), saved["location"].clone(), now)?;
            }
        }
        for mut row in rows {
            let name = s(&row, "name").to_owned();
            let Some(p) = self.projects.get(&name).filter(|p| p.enabled > 0) else {
                continue;
            };
            let Some(spec) = self.version(&name, p.version)? else {
                continue;
            };
            let key = s(&row, "key");
            let replica = n(&row, "replica");
            if n(&row, "machine") != machine
                || replica < 1
                || replica > spec.replicas as u64
                || key.is_empty()
                || key.len() > 64
                || !key.as_bytes()[0].is_ascii_alphanumeric()
                || !key
                    .bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
            {
                continue;
            }
            if self
                .copies_of(&name)
                .iter()
                .any(|x| x.replica as u64 == replica)
            {
                continue;
            }
            if row["ports"].is_string() {
                let Ok(ports) = serde_json::from_str::<Value>(s(&row, "ports")) else {
                    continue;
                };
                row["ports"] = ports;
            }
            if !row["ports"].is_null()
                && !row["ports"].as_object().is_some_and(|m| {
                    m.iter().all(|(k, v)| {
                        !k.is_empty()
                            && k.bytes().all(|b| b.is_ascii_digit())
                            && v.as_u64().is_some_and(|v| v > 0 && v <= 65535)
                    })
                })
            {
                continue;
            }
            row["since"] = json!(row["since"].as_u64().filter(|v| *v > 0).unwrap_or(now));
            row["reason"] = json!("recovered from agent inventory");
            row["leaving"] = Value::Null;
            row["cause"] = json!("recovery");
            self.save_copy(&name, Self::copy_row(row)?)?;
        }
        let unresolved = body["status"]
            .as_object()
            .into_iter()
            .flat_map(|m| m.keys())
            .any(|key| {
                let name = if self.projects.contains_key(key) {
                    key.clone()
                } else {
                    base_app(key)
                };
                self.projects.get(&name).is_some_and(|p| p.enabled > 0)
                    && saved.is_none()
                    && inventory.is_none()
                    && !self
                        .recovery_seen
                        .contains_key(&format!("run:{}", s(body, "run")))
            });
        if unresolved {
            self.recovery_missing.insert(s(body, "run").into());
        } else {
            self.recovery_missing.remove(s(body, "run"));
        }
        Ok(())
    }
    pub fn recover_late_witness(&mut self, saved: Option<&Value>, now: u64) -> anyhow::Result<()> {
        let Some(saved) = saved.filter(|v| !v["fleet"].is_null()) else {
            return Ok(());
        };
        if self.recovery_missing.is_empty() {
            return Ok(());
        }
        for row in array(&saved["fleet"]["runs"]) {
            if !self.recovery_missing.contains(s(row, "id")) {
                continue;
            }
            let Some(current) = self.runs.get(s(row, "id")).cloned() else {
                continue;
            };
            if !self.observed_runs.contains(&current.id)
                || row["agent"] != current.agent
                || row["machine"] != current.machine
                || row["started"] != current.started
            {
                continue;
            }
            self.recover_late_agent(&json!({"run":current.id,"machine":current.machine,"status":current.status}),Some(&json!({"at":saved["at"],"run":row["id"],"runRow":row,"copies":array(&saved["fleet"]["copies"]).iter().filter(|c|c["machine"]==current.machine).collect::<Vec<_>>()})),now)?;
        }
        Ok(())
    }
}
pub fn array(value: &Value) -> &[Value] {
    value.as_array().map(Vec::as_slice).unwrap_or(&[])
}
