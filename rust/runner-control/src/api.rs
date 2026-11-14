use crate::{
    control::{Control, n, s},
    error::{http, require},
    model::MIN,
    storage::EXPORT_TABLES,
};
use axum::http::{HeaderMap, Method};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};

pub struct Request {
    pub method: Method,
    pub path: String,
    pub query: BTreeMap<String, String>,
    pub headers: HeaderMap,
    pub origin: String,
    pub ip: String,
    pub body: bytes::Bytes,
    pub permit: Option<tokio::sync::OwnedSemaphorePermit>,
}
impl Request {
    pub fn header(&self, key: &str) -> Option<&str> {
        self.headers.get(key).and_then(|v| v.to_str().ok())
    }
    pub fn query(&self, key: &str) -> &str {
        self.query.get(key).map(String::as_str).unwrap_or("")
    }
    pub fn number(&self, key: &str) -> anyhow::Result<Option<u64>> {
        self.query
            .get(key)
            .filter(|s| !s.is_empty())
            .map(|s| s.parse().map_err(|_| http(400, format!("invalid {key}"))))
            .transpose()
    }
    pub fn json(&self) -> anyhow::Result<Value> {
        serde_json::from_slice(&self.body).map_err(|_| http(400, "the body must be JSON"))
    }
}
pub enum Output {
    Json(Value),
    Bytes(Vec<u8>),
    Wake(String),
    Restart,
}
fn need(ok: bool, what: &str) -> anyhow::Result<()> {
    if ok {
        Ok(())
    } else {
        Err(http(401, format!("this needs {what}")))
    }
}

impl Control {
    fn refuse_locked(&self, key: &str, now: u64) -> anyhow::Result<()> {
        if let Some(&(count, at)) = self.failures.get(key) {
            if count >= 5 && now.saturating_sub(at) < 15 * MIN {
                return Err(http(
                    429,
                    format!(
                        "too many wrong passwords; try again in {} min",
                        (15 * MIN - now.saturating_sub(at)).div_ceil(MIN)
                    ),
                ));
            }
        }
        Ok(())
    }
    fn password_try(&mut self, key: &str, ok: bool, now: u64) -> anyhow::Result<()> {
        if ok {
            self.failures.remove(key);
            return Ok(());
        }
        if self.failures.len() >= 10_000 && !self.failures.contains_key(key) {
            self.failures
                .retain(|_, (_, at)| now.saturating_sub(*at) < 15 * MIN);
            if self.failures.len() >= 10_000 {
                return Err(http(429, "password checks are busy; retry later"));
            }
        }
        let count = self
            .failures
            .get(key)
            .filter(|(_, at)| now.saturating_sub(*at) < 15 * MIN)
            .map(|(n, _)| n + 1)
            .unwrap_or(1);
        self.failures.insert(key.into(), (count, now));
        Ok(())
    }
    pub fn check_password(&mut self, given: &str, ip: &str, now: u64) -> anyhow::Result<bool> {
        let expected = if self.e("ADMIN_PASSWORD").is_empty() {
            self.e("FLEET_PASSWORD")
        } else {
            self.e("ADMIN_PASSWORD")
        };
        if expected.is_empty() {
            return Ok(false);
        }
        let key = format!("{ip}|fleet");
        self.refuse_locked(&key, now)?;
        let ok = runner_core::auth::equal_secret(given, expected);
        self.password_try(&key, ok, now)?;
        Ok(ok)
    }
    pub fn is_join_token(&self, header: Option<&str>) -> bool {
        !self.e("JOIN_TOKEN").is_empty()
            && header.is_some_and(|h| {
                runner_core::auth::equal_secret(h, &format!("Bearer {}", self.e("JOIN_TOKEN")))
            })
    }

    pub async fn api(&mut self, request: &Request, now: u64) -> anyhow::Result<Output> {
        self.set_pace(now);
        let path = request.path.trim_end_matches('/');
        if path == "/internal/dns" && request.method == Method::GET {
            return Ok(Output::Json(json!({"names":self.dns_targets(now)?})));
        }
        let ui = path.starts_with("/admin/api/");
        let route = if ui {
            path.strip_prefix("/admin/api")
        } else {
            path.strip_prefix("/api")
        }
        .ok_or_else(|| http(404, "not found"))?;
        if ui
            && request.method != Method::GET
            && request
                .header("origin")
                .is_some_and(|o| o != request.origin)
        {
            return Err(http(403, "cross-site request refused"));
        }
        let method = &request.method;
        let parts = route.trim_start_matches('/').split('/').collect::<Vec<_>>();
        let password = request
            .header("x-admin-password")
            .or_else(|| request.header("x-fleet-password"));
        let fleet =
            matches!(
                route,
                "/unlock" | "/settings" | "/roll" | "/join-token" | "/usage"
            ) || parts.first() == Some(&"machines") && parts.len() == 3 && parts[2] == "evict"
                || parts.first() == Some(&"slots") && parts.len() == 2;
        let check = !ui || fleet || method != Method::GET && route.starts_with("/projects/");
        let admin = if check {
            match password {
                Some(p) => self.check_password(p, &request.ip, now)?,
                None => false,
            }
        } else {
            false
        };
        let node = !ui && (admin || self.is_join_token(request.header("authorization")));
        let wrong = password.is_some() && !admin;
        if ui && method == Method::POST && route == "/unlock" {
            if !admin {
                return Err(http(401, "wrong password"));
            }
            return Ok(Output::Json(json!({"ok":true})));
        }
        let value = match (method, route) {
            (&Method::GET, "/status") => self.status(now)?,
            (&Method::GET, "/events") => self.events(&request.query)?,
            (&Method::GET, "/metrics") => {
                return Ok(Output::Bytes(self.metrics_response(
                    if request.query("range").is_empty() {
                        "1h"
                    } else {
                        request.query("range")
                    },
                    now,
                )?));
            }
            (&Method::GET, "/usage") => {
                need(admin, "the admin password")?;
                json!({"enabled":true,"runtime":"rust","configuration":self.store.diagnostics(),"memoryBytes":runner_core::rss_bytes()})
            }
            (&Method::POST, "/join") => {
                need(node, "the join token")?;
                self.join(&request.json()?, now).await?
            }
            (&Method::POST, "/sync") => {
                need(node, "the join token")?;
                let edge = if self.e("USE_EDGE_LOCATION") == "1" {
                    request.header("x-runner-location").map(|v| {
                        url::form_urlencoded::parse(
                            format!("v={}", v.replace('+', "%2B")).as_bytes(),
                        )
                        .next()
                        .map(|(_, s)| s.into_owned())
                        .unwrap_or_default()
                    })
                } else {
                    None
                };
                self.sync(&request.json()?, Some(&request.ip), edge.as_deref(), now)?
            }
            (&Method::POST, "/claim") => {
                need(node, "the join token")?;
                json!({"start":[],"mode":"passive"})
            }
            (&Method::POST, "/drain") => {
                need(node, "the join token")?;
                self.drain(&request.json()?, now)?
            }
            (&Method::GET, "/wake") => {
                need(node, "the join token")?;
                let run = request.query("run");
                require(!run.is_empty() && run.len() <= 200, "run is required")?;
                return Ok(Output::Wake(run.into()));
            }
            (&Method::POST, "/roll") => {
                need(admin || node, "the admin password or the join token")?;
                self.roll(request.number("machine")?, now)?
            }
            (&Method::DELETE, "/roll") => {
                need(admin, "the admin password")?;
                self.cancel_roll(now)?
            }
            (&Method::GET, "/join-token") => {
                need(admin, "the admin password")?;
                json!({"token":self.e("JOIN_TOKEN")})
            }
            (&Method::GET, "/export") => {
                need(admin, "the admin password")?;
                let table = request.query("table");
                require(
                    EXPORT_TABLES.contains(&table),
                    format!("table is one of {}", EXPORT_TABLES.join(", ")),
                )?;
                let limit = request.number("limit")?.unwrap_or(500).clamp(1, 2000);
                let rows = self.store.rows(
                    &format!(
                        "SELECT rowid AS _rowid,* FROM {table} WHERE rowid>? ORDER BY rowid LIMIT ?"
                    ),
                    &[json!(request.number("after")?.unwrap_or(0)), json!(limit)],
                )?;
                let next = if rows.len() == limit as usize {
                    rows.last()
                        .map(|r| r["_rowid"].clone())
                        .unwrap_or(Value::Null)
                } else {
                    Value::Null
                };
                json!({"rows":rows,"next":next})
            }
            (&Method::POST, "/import") => {
                need(admin, "the admin password")?;
                if !request.query("done").is_empty() {
                    return Ok(Output::Restart);
                }
                let body = request.json()?;
                let table = s(&body, "table");
                require(
                    EXPORT_TABLES.contains(&table) && body["rows"].is_array(),
                    "send {table, rows}",
                )?;
                let known = self
                    .store
                    .rows(&format!("PRAGMA table_info({table})"), &[])?
                    .iter()
                    .map(|r| s(r, "name").to_owned())
                    .collect::<BTreeSet<_>>();
                let rows = body["rows"].as_array().unwrap();
                require(rows.len() <= 2000, "at most 2000 rows per import")?;
                for row in rows {
                    let cols = row
                        .as_object()
                        .ok_or_else(|| http(400, "each row must be an object"))?
                        .keys()
                        .filter(|k| known.contains(*k))
                        .cloned()
                        .collect::<Vec<_>>();
                    require(!cols.is_empty(), "row has no recognized columns")?;
                    self.store.exec(
                        &format!(
                            "INSERT OR REPLACE INTO {table} ({}) VALUES ({})",
                            cols.join(","),
                            vec!["?"; cols.len()].join(",")
                        ),
                        &cols.iter().map(|k| row[k].clone()).collect::<Vec<_>>(),
                    )?;
                }
                json!({"imported":rows.len()})
            }
            (&Method::PUT, "/settings") => {
                need(admin, "the admin password")?;
                self.put_settings(&request.json()?, now)?
            }
            _ => {
                if parts.len() == 3
                    && parts[0] == "machines"
                    && parts[2] == "evict"
                    && method == Method::POST
                {
                    need(admin, "the admin password")?;
                    let machine = parts[1].parse().map_err(|_| http(400, "invalid machine"))?;
                    self.evict(machine, request.query("force") == "1", now)?
                } else if parts.len() == 2 && parts[0] == "slots" && method == Method::DELETE {
                    need(admin, "the admin password")?;
                    let machine = parts[1].parse().map_err(|_| http(400, "invalid slot"))?;
                    self.retire_slot(machine, now).await?
                } else if (2..=3).contains(&parts.len()) && parts[0] == "projects" {
                    self.app_api(request, parts[1], parts.get(2).copied(), admin, wrong, now)?
                } else {
                    return Err(http(404, "not found"));
                }
            }
        };
        Ok(Output::Json(value))
    }
    fn app_api(
        &mut self,
        r: &Request,
        name: &str,
        action: Option<&str>,
        admin: bool,
        wrong: bool,
        now: u64,
    ) -> anyhow::Result<Value> {
        let method = &r.method;
        if action.is_none() && method == Method::GET {
            return self.get_project(name, r.number("version")?);
        }
        if action == Some("env") && method == Method::GET {
            return Ok(
                json!({"name":name,"keys":self.app_env.get(name).map(|e|e.keys().cloned().collect::<Vec<_>>()).unwrap_or_default()}),
            );
        }
        let input = if method == Method::PUT {
            r.json()?
        } else {
            Value::Null
        };
        let creating =
            action.is_none() && method == Method::PUT && !self.projects.contains_key(name);
        let password = if action == Some("password") || creating && !input["password"].is_null() {
            let value = input["password"]
                .as_str()
                .ok_or_else(|| http(400, "password must be text"))?;
            require(
                (6..=200).contains(&value.encode_utf16().count()),
                "an app's password (\"password\") is 6 to 200 characters",
            )?;
            Some(value.to_owned())
        } else {
            None
        };
        if self.e("REQUIRE_DEPLOY_AUTH") == "1"
            && (creating || !self.app_passwords.contains_key(name))
        {
            need(admin, "the admin password")?;
        }
        if !creating && !admin {
            self.project(name)?;
            if let Some(secret) = self.app_passwords.get(name) {
                let key = format!("{}|app:{name}", r.ip);
                let given=r.header("x-app-password").ok_or_else(||http(401,if wrong{"wrong admin password".into()}else{format!("{name} is locked: changing it needs its password or the admin password")}))?;
                self.refuse_locked(&key, now)?;
                let hash = runner_core::auth::password_hash(given, s(secret, "salt"))?;
                let ok = runner_core::auth::equal_secret(&hash, s(secret, "hash"));
                self.password_try(&key, ok, now)?;
                if !ok {
                    return Err(http(401, format!("wrong password for {name}")));
                }
            } else if wrong {
                return Err(http(401, "wrong admin password"));
            }
        }
        match (method, action) {
            (&Method::POST, Some("unlock")) => Ok(json!({"ok":true})),
            (&Method::POST, Some("logs")) => {
                need(
                    admin || self.app_passwords.contains_key(name),
                    "the app password or the admin password",
                )?;
                let replica = r.number("replica")?.unwrap_or(1);
                require(replica > 0 && replica <= u32::MAX as u64, "invalid replica")?;
                self.open_log_session(name, replica as u32, now)
            }
            (&Method::POST, Some("diagnostics")) => {
                need(
                    admin || self.app_passwords.contains_key(name),
                    "the admin password",
                )?;
                Ok(
                    json!({"errors":self.diagnostics.values().filter(|x|x["app"]==name).map(|x|{let mut x=x.clone();x["error"]=json!(self.redact(s(&x,"error")));x}).collect::<Vec<_>>()}),
                )
            }
            (&Method::PUT, Some("password")) => {
                self.project(name)?;
                self.save_app_password(name, password.as_deref().unwrap(), now)?;
                Ok(json!({"ok":true}))
            }
            (&Method::PUT, Some("env")) => self.put_env(name, &input, now),
            (&Method::POST, Some("move")) => {
                let replica = r.number("replica")?;
                require(
                    replica.is_none_or(|n| n > 0 && n <= u32::MAX as u64),
                    "invalid replica",
                )?;
                self.move_copy(
                    name,
                    r.number("from")?.unwrap_or(0),
                    r.number("to")?,
                    replica.map(|n| n as u32),
                    "hand",
                    None,
                    now,
                )
            }
            (&Method::POST, Some("enable" | "disable")) => {
                self.set_enabled(name, action == Some("enable"), now)
            }
            (&Method::PUT, None) => {
                let mut result = self.put_project(name, input, admin, now)?;
                if creating {
                    if let Some(password) = password {
                        self.save_app_password(name, &password, now)?;
                        result["hasPassword"] = json!(true);
                    }
                }
                Ok(result)
            }
            (&Method::DELETE, None) => self.delete_project(name),
            _ => Err(http(404, "not found")),
        }
    }
    pub fn open_log_session(
        &mut self,
        name: &str,
        replica: u32,
        now: u64,
    ) -> anyhow::Result<Value> {
        self.project(name)?;
        let copy = self
            .copies_of(name)
            .iter()
            .filter(|c| c.replica == replica)
            .min_by_key(|c| c.leaving.is_some())
            .cloned()
            .ok_or_else(|| http(404, format!("replica {replica} of {name} has no copy")))?;
        self.log_sessions
            .retain(|_, v| v["viewer"] == true || now.saturating_sub(n(v, "created")) <= 30_000);
        if self.log_sessions.len() >= 50 {
            return Err(http(
                429,
                "too many logs are being watched; try again in a minute",
            ));
        }
        let id = uuid::Uuid::new_v4().to_string();
        self.log_sessions.insert(id.clone(),json!({"id":id,"name":name,"replica":replica,"machine":copy.machine,"key":copy.key,"created":now,"viewer":false,"agent":false,"run":null}));
        Ok(json!({"session":id,"machine":copy.machine,"key":copy.key}))
    }
    pub fn attach_log(
        &mut self,
        id: &str,
        run: Option<&str>,
        now: u64,
    ) -> anyhow::Result<(Value, Vec<String>)> {
        let session = self
            .log_sessions
            .get(id)
            .cloned()
            .ok_or_else(|| http(404, "no such session"))?;
        let mut session = session;
        let mut secrets = vec![];
        if let Some(run) = run {
            require(
                session["viewer"] == true && session["agent"] != true,
                "no such session",
            )?;
            let r = self
                .runs
                .get(run)
                .filter(|r| {
                    r.machine == n(&session, "machine")
                        && self.live(r, now)
                        && r.retire == 0
                        && r.status.get(s(&session, "key")).is_some()
                })
                .ok_or_else(|| http(403, "agent does not own this log session"))?;
            session["agent"] = json!(true);
            session["run"] = json!(r.id);
            secrets = self
                .app_env
                .get(s(&session, "name"))
                .into_iter()
                .flat_map(|m| m.values())
                .flat_map(|v| std::iter::once(v.clone()).chain(v.split('\n').map(str::to_owned)))
                .filter(|s| s.len() >= 4)
                .collect();
            secrets.sort_by_key(|s| std::cmp::Reverse(s.len()));
            secrets.dedup();
        } else {
            require(
                session["viewer"] != true && now.saturating_sub(n(&session, "created")) <= 30_000,
                "no such session",
            )?;
            session["viewer"] = json!(true);
            for r in self
                .live_runs(now)
                .into_iter()
                .filter(|r| r.machine == n(&session, "machine"))
            {
                self.wakes.insert(r.id);
            }
        }
        self.log_sessions.insert(id.into(), session.clone());
        Ok((session, secrets))
    }
}
