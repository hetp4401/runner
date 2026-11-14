use crate::{
    control::{Control, s, truncate},
    error::{http, require},
    model::*,
    spec,
};
use serde_json::{Value, json};

impl Control {
    pub fn describe(&self, p: &Project) -> anyhow::Result<Value> {
        let latest = self.version(&p.name, p.version)?;
        let mut placed=self.copies_of(&p.name).iter().map(|x|json!({"machine":x.machine,"replica":x.replica,"region":latest.as_ref().and_then(|s|spec::replica_region(&s.policy,x.replica)),"key":x.key,"since":x.since,"reason":x.reason,"cause":x.cause,"leaving":x.leaving,"moved":x.ports.is_some()})).collect::<Vec<_>>();
        placed.sort_by_key(|x| (x["replica"].as_u64().unwrap_or(0), !x["leaving"].is_null()));
        let staying = placed.iter().filter(|x| x["leaving"].is_null()).count();
        let port = latest.as_ref().and_then(|v| v.port);
        Ok(
            json!({"name":p.name,"version":p.version,"stable":p.stable,"port":port,"globalUrl":port.map(|_|format!("https://{}.{}/",p.name,self.e("DOMAIN"))),"replicas":latest.as_ref().map(|s|s.replicas).unwrap_or(1),"region":latest.as_ref().and_then(|s|s.policy.region.as_ref()),"regions":latest.as_ref().map(|s|s.policy.regions.clone()).unwrap_or_default(),"spreadTags":latest.as_ref().is_some_and(|s|s.policy.spread_tags),"placed":placed,"staying":staying,"blocked":self.blocked.get(&p.name),"files":latest.as_ref().map(|s|s.files.keys().cloned().collect::<Vec<_>>()).unwrap_or_default(),"enabled":p.enabled>0,"hasPassword":self.app_passwords.contains_key(&p.name),"env":self.app_env.get(&p.name).map(|e|e.keys().cloned().collect::<Vec<_>>()).unwrap_or_default(),"state":if p.enabled==0{"disabled"}else if p.halted.is_some(){"halted"}else if p.stable==Some(p.version){"live"}else{"deploying"},"halted":p.halted.as_ref().map(|s|self.redact(s)),"updated":p.updated}),
        )
    }
    pub fn get_project(&self, name: &str, version: Option<u64>) -> anyhow::Result<Value> {
        let p = self.project(name)?;
        let v = version.unwrap_or(p.version);
        let spec = self
            .version(name, v)?
            .ok_or_else(|| http(404, format!("{name} has no version {v}")))?;
        let mut out = self.describe(&p)?;
        out["shown"] = json!(v);
        out["compose"] = json!(spec.compose);
        out["files"] = json!(spec.files);
        out["port"] = json!(spec.port);
        out["replicas"] = json!(spec.replicas);
        let mut versions = self.store.rows(
            "SELECT version,port,replicas,created FROM versions WHERE name=? ORDER BY version",
            &[json!(name)],
        )?;
        for v in &mut versions {
            if v["replicas"].is_null() {
                v["replicas"] = json!(1);
            }
        }
        out["versions"] = json!(versions);
        Ok(out)
    }
    pub fn put_project(
        &mut self,
        name: &str,
        mut body: Value,
        trusted: bool,
        now: u64,
    ) -> anyhow::Result<Value> {
        require(
            spec::name_valid(name),
            "app names are lowercase letters, digits and dashes",
        )?;
        if !self.projects.contains_key(name) {
            require(
                ![
                    "control",
                    "runners",
                    self.e("CONTROL_HOST").split('.').next().unwrap_or(""),
                ]
                .contains(&name),
                "that name is reserved for the control plane's public addresses",
            )?;
            require(
                !spec::numbered(name),
                "an app name can't end in -<number> or -m<number>: those are the URLs of its replicas and servers",
            )?;
        }
        let p = self.projects.get(name).cloned();
        let latest = if let Some(p) = &p {
            self.version(name, p.version)?
        } else {
            None
        };
        if let Some(latest) = &latest {
            if body.is_object()
                && ["compose", "dockerfile", "files"]
                    .iter()
                    .all(|k| body[*k].is_null())
            {
                body["compose"] = json!(latest.compose);
                body["files"] = json!(latest.files);
                if body.get("port").is_none() {
                    body["port"] = json!(latest.port);
                }
                if body.get("replicas").is_none() {
                    body["replicas"] = json!(latest.replicas);
                }
            }
        }
        let spec = spec::build(&body, trusted)?;
        let machines = self.live_machines(now).len() as u64;
        let env_changed = if let Some(env) = body.get("env") {
            self.merge_env(name, env, now)?
        } else {
            false
        };
        let same = latest.as_ref().is_some_and(|s| {
            !env_changed
                && s.compose == spec.compose
                && s.port == spec.port
                && s.replicas == spec.replicas
                && s.files == spec.files
        });
        let next = if same {
            let mut p = p.unwrap();
            if p.halted.is_none() && p.stable == Some(p.version) {
                let mut out = self.describe(&p)?;
                out["unchanged"] = json!(true);
                return Ok(out);
            }
            p.halted = None;
            p.rollout = machines;
            p.stable = Some(p.version);
            p.updated = now;
            p
        } else {
            let version = p.as_ref().map(|p| p.version).unwrap_or(0) + 1;
            self.store.exec("INSERT INTO versions(name,version,compose,port,files,replicas,created) VALUES(?,?,?,?,?,?,?)",&[json!(name),json!(version),json!(spec.compose),json!(spec.port),json!(serde_json::to_string(&spec.files)?),json!(spec.replicas),json!(now)])?;
            Project {
                name: name.into(),
                version,
                stable: Some(version),
                rollout: machines,
                halted: None,
                updated: now,
                enabled: p.as_ref().map(|p| p.enabled).unwrap_or(1),
            }
        };
        self.save_project(next.clone())?;
        self.place(now)?;
        self.dns_dirty = true;
        self.describe(&next)
    }
    pub fn set_enabled(&mut self, name: &str, enabled: bool, now: u64) -> anyhow::Result<Value> {
        let mut p = self.project(name)?;
        p.enabled = u64::from(enabled);
        p.updated = now;
        self.save_project(p.clone())?;
        self.place(now)?;
        self.describe(&p)
    }
    pub fn merge_env(&mut self, name: &str, patch: &Value, now: u64) -> anyhow::Result<bool> {
        let patch=patch.as_object().ok_or_else(||http(400,"env must be an object: {\"KEY\": \"value\"} sets a variable, {\"KEY\": null} removes one"))?;
        let mut env = self.app_env.get(name).cloned().unwrap_or_default();
        for (key, value) in patch {
            require(
                !key.is_empty()
                    && key.len() <= 64
                    && (key.as_bytes()[0].is_ascii_alphabetic() || key.starts_with('_'))
                    && key.bytes().all(|c| c.is_ascii_alphanumeric() || c == b'_'),
                format!("{key} isn't a usable variable name (letters, digits and _, up to 64)"),
            )?;
            require(
                !key.starts_with("FLEET_"),
                format!("{key}: FLEET_ variables are set by the fleet"),
            )?;
            if value.is_null() {
                env.remove(key);
            } else {
                let v = value.as_str().ok_or_else(|| {
                    http(400, format!("{key} must be text (or null to remove it)"))
                })?;
                require(
                    v.len() <= 8192,
                    format!("{key} is longer than 8192 characters"),
                )?;
                env.insert(key.clone(), v.into());
            }
        }
        require(env.len() <= 40, "at most 40 variables")?;
        let changed = self.app_env.get(name).is_some_and(|before| *before != env)
            || !self.app_env.contains_key(name) && !env.is_empty();
        if !changed {
            return Ok(false);
        }
        self.store
            .exec("DELETE FROM app_env WHERE name=?", &[json!(name)])?;
        for (key, value) in &env {
            self.store.exec(
                "INSERT INTO app_env(name,key,value,updated) VALUES(?,?,?,?)",
                &[json!(name), json!(key), json!(value), json!(now)],
            )?;
        }
        if env.is_empty() {
            self.app_env.remove(name);
        } else {
            self.app_env.insert(name.into(), env);
        }
        Ok(true)
    }
    pub fn put_env(&mut self, name: &str, body: &Value, now: u64) -> anyhow::Result<Value> {
        let mut p = self.project(name)?;
        require(
            body.as_object().is_some_and(|b| !b.is_empty()),
            "send {\"KEY\": \"value\"} to set a variable and {\"KEY\": null} to remove one",
        )?;
        let changed = self.merge_env(name, body, now)?;
        if changed {
            if let Some(latest) = self.version(name, p.version)? {
                p.version += 1;
                self.store.exec("INSERT INTO versions(name,version,compose,port,files,replicas,created) VALUES(?,?,?,?,?,?,?)",&[json!(name),json!(p.version),json!(latest.compose),json!(latest.port),json!(serde_json::to_string(&latest.files)?),json!(latest.replicas),json!(now)])?;
                p.stable = Some(p.version);
                p.rollout = self.live_machines(now).len() as u64;
                p.halted = None;
                p.updated = now;
                self.save_project(p.clone())?;
                self.place(now)?;
            }
        }
        let mut out = self.describe(&p)?;
        out["keys"] = out["env"].clone();
        if !changed {
            out["unchanged"] = json!(true);
        }
        Ok(out)
    }
    pub fn env_file(&self, name: &str, x: &Copy) -> String {
        let domain = self.e("DOMAIN");
        let mut out = format!(
            "FLEET_APP={name}\nFLEET_REPLICA={}\nFLEET_MACHINE={}\nFLEET_HOST={name}-{}.{domain}\nFLEET_DOMAIN={domain}\n",
            x.replica, x.machine, x.replica
        );
        for (key, value) in self.app_env.get(name).into_iter().flatten() {
            let quote = if !value.contains(['\'', '\n', '\r']) {
                format!("'{value}'")
            } else {
                format!(
                    "\"{}\"",
                    value
                        .replace('\\', "\\\\")
                        .replace('"', "\\\"")
                        .replace('$', "$$")
                        .replace("\r\n", "\n")
                        .replace('\n', "\\n")
                )
            };
            out.push_str(&format!("{key}={quote}\n"));
        }
        out
    }
    pub fn delete_project(&mut self, name: &str) -> anyhow::Result<Value> {
        self.project(name)?;
        for table in ["projects", "versions", "copies", "app_passwords", "app_env"] {
            self.store
                .exec(&format!("DELETE FROM {table} WHERE name=?"), &[json!(name)])?;
        }
        self.projects.remove(name);
        self.copies.remove(name);
        self.blocked.remove(name);
        self.app_passwords.remove(name);
        self.app_env.remove(name);
        self.diagnostics.retain(|_, v| v["app"] != name);
        self.clear_versions(name);
        self.dns_dirty = true;
        Ok(json!({"deleted":name}))
    }
    pub fn put_settings(&mut self, body: &Value, now: u64) -> anyhow::Result<Value> {
        require(
            !["pools", "expected", "serverCount"]
                .iter()
                .any(|k| body.get(*k).is_some()),
            "Server targets are not supported. Set SERVER_TAG on each agent to identify its group.",
        )?;
        if let Some(rebalance) = body.get("rebalance") {
            require(rebalance.is_boolean(), "rebalance must be true or false")?;
            self.set_setting("rebalance", if rebalance == true { "on" } else { "off" })?;
        }
        Ok(json!({"groups":self.groups(now),"rebalance":self.rebalance_on()}))
    }
    pub fn rebalance_on(&self) -> bool {
        self.settings.get("rebalance").is_none_or(|s| s != "off")
    }
    pub fn roll(&mut self, machine: Option<u64>, now: u64) -> anyhow::Result<Value> {
        self.set_setting(
            &machine
                .map(|m| format!("roll_{m}"))
                .unwrap_or_else(|| "roll".into()),
            now,
        )?;
        self.log_event("roll",json!({"machine":machine,"cause":"start","detail":machine.map(|m|format!("server {m} is to be replaced")).unwrap_or_else(||"every server is to be replaced, one at a time".into())}),now)?;
        Ok(json!({"rolling":machine.map(|m|json!([m])).unwrap_or(json!("all")),"since":now}))
    }
    pub fn cancel_roll(&mut self, now: u64) -> anyhow::Result<Value> {
        let keys = self
            .settings
            .keys()
            .filter(|k| {
                k.as_str() == "roll" || k.starts_with("roll_") && crate::storage::durable_setting(k)
            })
            .cloned()
            .collect::<Vec<_>>();
        for k in &keys {
            self.settings.remove(k);
            self.store
                .exec("DELETE FROM settings WHERE key=?", &[json!(k)])?;
        }
        if !keys.is_empty() {
            self.log_event("roll",json!({"cause":"cancelled","detail":"the roll was called off: servers leave on their own schedules again"}),now)?;
        }
        Ok(json!({"cancelled":!keys.is_empty()}))
    }
    pub fn desired_for(
        &self,
        machine: u64,
        successor: bool,
        run: Option<&Run>,
        now: u64,
    ) -> anyhow::Result<Value> {
        let live = self.live_machines(now);
        let run = run.or_else(|| live.get(&machine));
        let mut out = serde_json::Map::new();
        for p in self.projects.values().filter(|p| p.enabled > 0) {
            let Some(v) = p.wanted() else {
                continue;
            };
            let Some(spec) = self.version(&p.name, v)? else {
                continue;
            };
            for x in self.copies_on(&p.name, machine) {
                if successor && x.leaving.is_some() {
                    continue;
                }
                let region = spec::replica_region(&spec.policy, x.replica);
                if region.is_some()
                    && run.and_then(Run::region) != region
                    && run.is_none_or(|r| r.status[&x.key]["s"] != "healthy")
                {
                    continue;
                }
                let (compose, port) = if let Some(ports) = &x.ports {
                    (
                        spec::remap(&spec.compose, ports)?,
                        spec.port
                            .map(|p| ports.get(&(p as u32)).copied().unwrap_or(p as u32) as u16),
                    )
                } else {
                    (spec.compose.clone(), spec.port)
                };
                let mut files = spec.files.clone();
                files.insert(".env".into(), self.env_file(&p.name, &x));
                let mut plan = json!({"v":v,"compose":compose,"port":port,"files":files,"app":p.name,"replica":x.replica,"ports":x.ports});
                if let Some(ready) = &spec.policy.ready {
                    plan["ready"] = json!(ready);
                }
                out.insert(x.key.clone(), spec::agent_plan(plan)?);
            }
        }
        Ok(Value::Object(out))
    }
    pub fn settle(&mut self, now: u64) -> anyhow::Result<()> {
        let up = self.live_machines(now);
        let projects = self.projects.values().cloned().collect::<Vec<_>>();
        for mut p in projects {
            if p.enabled == 0 || p.halted.is_some() || p.stable == Some(p.version) {
                continue;
            }
            let seen = self
                .copies_of(&p.name)
                .iter()
                .filter(|x| x.leaving.is_none() && up.contains_key(&x.machine))
                .map(|x| (x.clone(), up[&x.machine].status[&x.key].clone()))
                .collect::<Vec<_>>();
            if seen.is_empty() {
                continue;
            }
            let mine = seen
                .iter()
                .filter(|(_, st)| st["v"] == p.version)
                .collect::<Vec<_>>();
            let failed = mine
                .iter()
                .filter(|(_, st)| st["s"] == "failed")
                .map(|(x, _)| x.machine)
                .collect::<std::collections::BTreeSet<_>>();
            let healthy = mine
                .iter()
                .any(|(_, st)| st["s"] == "healthy" && st["r"] != false);
            let key = format!("{}@{}", p.name, p.version);
            if !failed.is_empty() && !healthy {
                self.alone_since.entry(key.clone()).or_insert(now);
            } else {
                self.alone_since.remove(&key);
            }
            let halt = if failed.len() >= 2 {
                Some(format!(
                    "it failed on servers {}",
                    failed
                        .iter()
                        .map(u64::to_string)
                        .collect::<Vec<_>>()
                        .join(" and ")
                ))
            } else if !failed.is_empty() && seen.len() == 1 {
                Some(format!(
                    "it failed on server {}, its only server",
                    failed.first().unwrap()
                ))
            } else if !failed.is_empty()
                && self
                    .alone_since
                    .get(&key)
                    .is_some_and(|at| now.saturating_sub(*at) >= 10 * MIN)
            {
                Some(format!(
                    "it failed on server {} and has been healthy nowhere for 10 minutes",
                    failed.first().unwrap()
                ))
            } else {
                None
            };
            if let Some(halt) = halt {
                let error = mine
                    .iter()
                    .find(|(_, st)| st["s"] == "failed")
                    .map(|(_, st)| s(st, "e"))
                    .filter(|s| !s.is_empty())
                    .unwrap_or("failed");
                p.halted = Some(truncate(
                    &format!("{halt}: {}", error.lines().last().unwrap_or("failed")),
                    600,
                ));
                self.alone_since.remove(&key);
                self.alert(
                    "halted",
                    json!({"app":p.name}),
                    &format!(
                        "{} version {} was halted (its copies go back to version {}): {halt}",
                        p.name,
                        p.version,
                        p.stable
                            .map(|n| n.to_string())
                            .unwrap_or_else(|| "none".into())
                    ),
                    now,
                )?;
            } else if seen
                .iter()
                .all(|(_, st)| st["v"] == p.version && st["s"] == "healthy" && st["r"] != false)
            {
                p.stable = Some(p.version);
            } else {
                continue;
            }
            p.updated = now;
            self.save_project(p)?;
        }
        Ok(())
    }
    pub fn save_app_password(
        &mut self,
        name: &str,
        password: &str,
        now: u64,
    ) -> anyhow::Result<()> {
        require(
            (6..=200).contains(&password.encode_utf16().count()),
            "an app's password (\"password\") is 6 to 200 characters",
        )?;
        let salt = runner_core::random_id();
        let hash = runner_core::auth::password_hash(password, &salt)?;
        self.store.exec("INSERT INTO app_passwords(name,salt,hash,updated) VALUES(?,?,?,?) ON CONFLICT(name) DO UPDATE SET salt=excluded.salt,hash=excluded.hash,updated=excluded.updated",&[json!(name),json!(salt),json!(hash),json!(now)])?;
        self.app_passwords
            .insert(name.into(), json!({"name":name,"salt":salt,"hash":hash}));
        Ok(())
    }
}
