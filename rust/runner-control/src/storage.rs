use anyhow::{Context, ensure};
use rusqlite::{
    Connection,
    types::{Value as SqlValue, ValueRef},
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};

pub const CONFIG_TABLES: &[&str] = &[
    "projects",
    "versions",
    "settings",
    "app_passwords",
    "app_env",
];
pub const EXPORT_TABLES: &[&str] = &[
    "projects",
    "versions",
    "copies",
    "runs",
    "starts",
    "settings",
    "slots",
    "agents",
    "app_passwords",
    "app_env",
    "events",
    "metrics_1m",
    "metrics_10m",
    "metrics_1h",
];
pub fn durable_setting(key: &str) -> bool {
    matches!(key, "rebalance" | "roll")
        || key
            .strip_prefix("roll_")
            .is_some_and(|s| !s.is_empty() && s.bytes().all(|c| c.is_ascii_digit()))
}
pub struct Table {
    pub columns: &'static [&'static str],
    pub keys: &'static [&'static str],
    pub schema: &'static str,
}
pub fn table(name: &str) -> Table {
    match name {
        "projects" => Table {
            columns: &[
                "name", "version", "stable", "rollout", "halted", "updated", "enabled",
            ],
            keys: &["name"],
            schema: "name TEXT PRIMARY KEY,version INTEGER NOT NULL,stable INTEGER,rollout INTEGER NOT NULL,halted TEXT,updated INTEGER NOT NULL,enabled INTEGER NOT NULL DEFAULT 1",
        },
        "versions" => Table {
            columns: &[
                "name", "version", "compose", "port", "created", "files", "replicas",
            ],
            keys: &["name", "version"],
            schema: "name TEXT NOT NULL,version INTEGER NOT NULL,compose TEXT NOT NULL,port INTEGER,created INTEGER NOT NULL,files TEXT,replicas INTEGER,PRIMARY KEY(name,version)",
        },
        "settings" => Table {
            columns: &["key", "value"],
            keys: &["key"],
            schema: "key TEXT PRIMARY KEY,value TEXT NOT NULL",
        },
        "app_passwords" => Table {
            columns: &["name", "salt", "hash", "updated"],
            keys: &["name"],
            schema: "name TEXT PRIMARY KEY,salt TEXT NOT NULL,hash TEXT NOT NULL,updated INTEGER NOT NULL",
        },
        "app_env" => Table {
            columns: &["name", "key", "value", "updated"],
            keys: &["name", "key"],
            schema: "name TEXT NOT NULL,key TEXT NOT NULL,value TEXT NOT NULL,updated INTEGER NOT NULL,PRIMARY KEY(name,key)",
        },
        _ => panic!("unknown configuration table"),
    }
}
#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct Query {
    pub sql: String,
    pub params: Vec<Value>,
}
pub fn upsert(name: &str, row: &Value) -> Query {
    let t = table(name);
    Query {
        sql: format!(
            "INSERT INTO {name} ({}) VALUES ({}) ON CONFLICT ({}) DO UPDATE SET {}",
            t.columns.join(","),
            vec!["?"; t.columns.len()].join(","),
            t.keys.join(","),
            t.columns
                .iter()
                .filter(|c| !t.keys.contains(c))
                .map(|c| format!("{c}=excluded.{c}"))
                .collect::<Vec<_>>()
                .join(",")
        ),
        params: t.columns.iter().map(|c| row[*c].clone()).collect(),
    }
}
#[derive(Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Usage {
    pub requests: u64,
    pub rows_read: u64,
    pub rows_written: u64,
    pub commits: u64,
    pub revision: u64,
}
pub struct D1 {
    client: reqwest::Client,
    url: String,
    key: String,
    email: String,
    origin: String,
    pub usage: Usage,
}
impl D1 {
    pub fn new(env: &BTreeMap<String, String>) -> anyhow::Result<Self> {
        let get = |k: &str| env.get(k).map(String::as_str).unwrap_or("");
        let account = if get("D1_ACCOUNT_ID").is_empty() {
            get("ACCOUNT_ID")
        } else {
            get("D1_ACCOUNT_ID")
        };
        let database = get("D1_DATABASE_ID");
        let key = if get("D1_API_KEY").is_empty() {
            get("CF_API_KEY")
        } else {
            get("D1_API_KEY")
        };
        let email = if get("D1_API_EMAIL").is_empty() {
            get("CF_API_EMAIL")
        } else {
            get("D1_API_EMAIL")
        };
        ensure!(
            !account.is_empty() && !database.is_empty() && !key.is_empty() && !email.is_empty(),
            "D1 configuration is incomplete"
        );
        let base = env
            .get("D1_API_BASE")
            .map(String::as_str)
            .unwrap_or("https://api.cloudflare.com/client/v4");
        Ok(Self {
            client: runner_core::http_client(std::time::Duration::from_secs(30))?,
            url: format!(
                "{}/accounts/{account}/d1/database/{database}/query",
                base.trim_end_matches('/')
            ),
            key: key.into(),
            email: email.into(),
            origin: get("CONTROL_HOST").into(),
            usage: Usage::default(),
        })
    }
    pub async fn query(&mut self, batch: &[Query]) -> anyhow::Result<Vec<Value>> {
        self.usage.requests += 1;
        let response = self
            .client
            .post(&self.url)
            .header("x-auth-key", &self.key)
            .header("x-auth-email", &self.email)
            .timeout(std::time::Duration::from_secs(30))
            .json(&json!({"batch":batch}))
            .send()
            .await
            .map_err(|_| {
                anyhow::anyhow!("D1 network request failed; configuration was not acknowledged")
            })?;
        let status = response.status();
        // Page limits, not a permanently allocated shared worker buffer, bound reads.
        let body: Value = runner_core::response_json(response, 8 << 20)
            .await
            .map_err(|_| {
                anyhow::anyhow!(
                    "D1 returned an unreadable response (HTTP {})",
                    status.as_u16()
                )
            })?;
        ensure!(
            status.is_success()
                && body["success"] == true
                && body["result"]
                    .as_array()
                    .is_some_and(|a| a.iter().all(|v| v["success"] == true)),
            "D1 request failed (HTTP {})",
            status.as_u16()
        );
        let mut body = body;
        let results = body["result"]
            .take()
            .as_array_mut()
            .map(std::mem::take)
            .unwrap_or_default();
        for r in &results {
            self.usage.rows_read += r["meta"]["rows_read"].as_u64().unwrap_or(0);
            self.usage.rows_written += r["meta"]["rows_written"].as_u64().unwrap_or(0);
        }
        Ok(results)
    }
    async fn metadata(&mut self) -> anyhow::Result<Value> {
        let mut rows = self
            .query(&[Query {
                sql: "SELECT * FROM runner_meta WHERE id=1".into(),
                params: vec![],
            }])
            .await?;
        let row = rows
            .get_mut(0)
            .and_then(|r| r["results"].get_mut(0))
            .map(Value::take)
            .unwrap_or(Value::Null);
        ensure!(
            row["schema_version"] == 1,
            "D1 has no supported fleet configuration; run the migration explicitly"
        );
        Ok(row)
    }
}
type Fingerprints = BTreeMap<String, (String, Vec<Value>)>;
pub struct Store {
    db: Connection,
    pub d1: Option<D1>,
    known: BTreeMap<String, Fingerprints>,
    dirty: BTreeSet<String>,
    transaction: bool,
    pub failed: bool,
    pub authority: Option<(crate::authority::Authority, crate::authority::Proof)>,
}
impl Store {
    pub fn changes(&self) -> u64 {
        self.db.total_changes()
    }
    pub fn diagnostics(&self) -> Value {
        match &self.d1 {
            Some(d1) => json!({"backend":"d1","failed":self.failed,"usage":d1.usage}),
            None => json!({"backend":"sqlite","failed":self.failed}),
        }
    }
    pub fn memory() -> anyhow::Result<Self> {
        Self::connection(Connection::open_in_memory()?, true)
    }
    fn connection(db: Connection, initialize: bool) -> anyhow::Result<Self> {
        db.execute_batch("PRAGMA foreign_keys=ON; PRAGMA temp_store=MEMORY; PRAGMA cache_size=-1024; PRAGMA mmap_size=0;")?;
        if initialize {
            for name in CONFIG_TABLES {
                db.execute_batch(&format!(
                    "CREATE TABLE IF NOT EXISTS {name} ({})",
                    table(name).schema
                ))?;
            }
        }
        let mut store = Self {
            db,
            d1: None,
            known: BTreeMap::new(),
            dirty: BTreeSet::new(),
            transaction: false,
            failed: false,
            authority: None,
        };
        store.runtime_schema()?;
        Ok(store)
    }
    pub async fn open(env: &BTreeMap<String, String>) -> anyhow::Result<Self> {
        if env.get("CONFIG_STORAGE").is_some_and(|s| s == "d1") {
            let mut store = Self::memory()?;
            let mut d1 = D1::new(env)?;
            let meta = d1.metadata().await?;
            ensure!(
                meta["ready"] == 1 && meta["origin"] == d1.origin,
                "D1 fleet is not ready or belongs to another controller"
            );
            d1.usage.revision = meta["revision"].as_u64().context("invalid D1 revision")?;
            for name in CONFIG_TABLES {
                let t = table(name);
                let mut last: Vec<Value> = Vec::new();
                loop {
                    let predicate = if last.is_empty() {
                        String::new()
                    } else {
                        format!(
                            " WHERE ({}) > ({})",
                            t.keys.join(","),
                            vec!["?"; t.keys.len()].join(",")
                        )
                    };
                    let mut result = d1
                        .query(&[Query {
                            sql: format!(
                                "SELECT {} FROM {name}{predicate} ORDER BY {} LIMIT 10",
                                t.columns.join(","),
                                t.keys.join(",")
                            ),
                            params: last,
                        }])
                        .await?;
                    let rows = result[0]["results"]
                        .as_array_mut()
                        .context("invalid D1 rows")?;
                    if rows.is_empty() {
                        break;
                    }
                    last = t
                        .keys
                        .iter()
                        .map(|k| rows.last().unwrap()[*k].clone())
                        .collect();
                    for row in rows.iter() {
                        ensure!(
                            *name != "settings"
                                || durable_setting(row["key"].as_str().unwrap_or("")),
                            "D1 contains non-configuration settings; migration required"
                        );
                        let mut q = upsert(name, row);
                        q.sql = q.sql.replacen(
                            &format!("INTO {name}"),
                            &format!("INTO main.{name}"),
                            1,
                        );
                        store.execute(&q.sql, &q.params)?;
                    }
                    if rows.len() < 10 {
                        break;
                    }
                }
                store
                    .known
                    .insert(name.to_string(), store.fingerprints(name)?);
            }
            let after = d1.metadata().await?;
            ensure!(
                ["revision", "commit_id", "ready", "origin"]
                    .iter()
                    .all(|k| after[*k] == meta[*k]),
                "D1 configuration changed while loading; retry startup"
            );
            store.db.execute_batch(
                "DELETE FROM temp.settings; INSERT INTO temp.settings SELECT * FROM main.settings;",
            )?;
            store.validate()?;
            store.dirty.clear();
            store.d1 = Some(d1);
            Ok(store)
        } else {
            let dir =
                std::path::Path::new(env.get("DATA_DIR").map(String::as_str).unwrap_or("/data"));
            runner_core::fs::private_directory(dir)?;
            let path = dir.join("control.db");
            let initialize = env.get("INIT_CONTROL_DB").is_some_and(|v| v == "1");
            ensure!(
                path.exists() || initialize,
                "configuration database is missing; initialize a new fleet explicitly"
            );
            let db = Connection::open(path)?;
            db.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;")?;
            let store = Self::connection(db, initialize)?;
            store.validate()?;
            Ok(store)
        }
    }
    fn runtime_schema(&mut self) -> anyhow::Result<()> {
        // The durable settings table remains in main; transient values shadow it.
        self.db.execute_batch("CREATE TEMP TABLE settings(key TEXT PRIMARY KEY,value TEXT NOT NULL); INSERT INTO temp.settings SELECT * FROM main.settings;
          CREATE TEMP TABLE runs(id TEXT PRIMARY KEY,machine INTEGER NOT NULL,started INTEGER NOT NULL,status TEXT NOT NULL,ready INTEGER NOT NULL,handover INTEGER NOT NULL,retire INTEGER NOT NULL,seen INTEGER NOT NULL,agent TEXT,kind TEXT NOT NULL DEFAULT '',label TEXT,pool TEXT,drain INTEGER,deadline INTEGER,evict INTEGER,quarantine INTEGER);
          CREATE TEMP TABLE starts(machine INTEGER PRIMARY KEY,at INTEGER NOT NULL,by TEXT NOT NULL);
          CREATE TEMP TABLE copies(name TEXT NOT NULL,machine INTEGER NOT NULL,replica INTEGER NOT NULL,since INTEGER NOT NULL,reason TEXT NOT NULL,leaving TEXT,ports TEXT,key TEXT,cause TEXT,PRIMARY KEY(name,machine,replica));
          CREATE TEMP TABLE slots(n INTEGER PRIMARY KEY,tunnel TEXT NOT NULL,token TEXT NOT NULL,created INTEGER NOT NULL);
          CREATE TEMP TABLE agents(id TEXT PRIMARY KEY,slot INTEGER NOT NULL,kind TEXT NOT NULL,label TEXT,joined INTEGER NOT NULL,pool TEXT);
          CREATE TEMP TABLE geo(ip TEXT PRIMARY KEY,text TEXT,at INTEGER NOT NULL);
          CREATE TEMP TABLE events(id INTEGER PRIMARY KEY AUTOINCREMENT,t INTEGER NOT NULL,kind TEXT NOT NULL,app TEXT,replica INTEGER,machine INTEGER,other INTEGER,run TEXT,cause TEXT,detail TEXT);
          CREATE INDEX temp.events_t ON events(t);
          CREATE TEMP TABLE metrics_1m(t INTEGER PRIMARY KEY,data BLOB NOT NULL);
          CREATE TEMP TABLE metrics_10m(t INTEGER PRIMARY KEY,data BLOB NOT NULL);
          CREATE TEMP TABLE metrics_1h(t INTEGER PRIMARY KEY,data BLOB NOT NULL);")?;
        Ok(())
    }
    pub fn validate(&self) -> anyhow::Result<()> {
        ensure!(
            self.rows("PRAGMA quick_check", &[])?
                .first()
                .and_then(|r| r["quick_check"].as_str())
                == Some("ok"),
            "configuration database integrity check failed"
        );
        for name in CONFIG_TABLES {
            self.rows(
                &format!(
                    "SELECT {} FROM main.{name} LIMIT 0",
                    table(name).columns.join(",")
                ),
                &[],
            )?;
        }
        for row in self.rows("SELECT name,version,stable FROM main.projects", &[])? {
            for version in [&row["version"], &row["stable"]] {
                if version.is_null() {
                    continue;
                }
                let rows = self.rows(
                    "SELECT compose,files FROM main.versions WHERE name=? AND version=?",
                    &[row["name"].clone(), version.clone()],
                )?;
                let spec = rows
                    .first()
                    .context("configuration refers to a missing application version")?;
                let _: serde_yaml::Value =
                    serde_yaml::from_str(spec["compose"].as_str().context("invalid compose")?)?;
                let files: Value = serde_json::from_str(spec["files"].as_str().unwrap_or("{}"))?;
                ensure!(
                    files.is_object(),
                    "invalid application files in configuration"
                );
            }
        }
        Ok(())
    }
    fn parameters(args: &[Value]) -> Vec<SqlValue> {
        args.iter()
            .map(|v| match v {
                Value::Null => SqlValue::Null,
                Value::Bool(v) => SqlValue::Integer(i64::from(*v)),
                Value::Number(n) => n
                    .as_i64()
                    .map(SqlValue::Integer)
                    .unwrap_or_else(|| SqlValue::Real(n.as_f64().unwrap_or(0.))),
                Value::String(s) => SqlValue::Text(s.clone()),
                _ => SqlValue::Text(v.to_string()),
            })
            .collect()
    }
    pub fn rows(&self, sql: &str, args: &[Value]) -> anyhow::Result<Vec<Value>> {
        let mut out = Vec::new();
        self.scan(sql, args, |row| {
            out.push(row);
            Ok(())
        })?;
        Ok(out)
    }
    pub fn scan(
        &self,
        sql: &str,
        args: &[Value],
        mut consume: impl FnMut(Value) -> anyhow::Result<()>,
    ) -> anyhow::Result<()> {
        ensure!(!self.failed, "configuration storage failed; refusing plans");
        let mut statement = self.db.prepare_cached(sql)?;
        let names = statement
            .column_names()
            .into_iter()
            .map(str::to_owned)
            .collect::<Vec<_>>();
        let params = Self::parameters(args);
        let mut rows = statement.query(rusqlite::params_from_iter(params))?;
        while let Some(row) = rows.next()? {
            let mut map = serde_json::Map::new();
            for (i, name) in names.iter().enumerate() {
                let v = match row.get_ref(i)? {
                    ValueRef::Null => Value::Null,
                    ValueRef::Integer(n) => json!(n),
                    ValueRef::Real(n) => json!(n),
                    ValueRef::Text(s) => Value::String(std::str::from_utf8(s)?.into()),
                    ValueRef::Blob(b) => {
                        ensure!(name == "data", "unexpected database binary column");
                        // Every compressed metrics row has an independent decoded-size limit.
                        ensure!(
                            b.len() >= 4 && u32::from_le_bytes(b[..4].try_into()?) <= 8 << 20,
                            "invalid compressed metrics size"
                        );
                        Value::String(String::from_utf8(lz4_flex::decompress_size_prepended(b)?)?)
                    }
                };
                map.insert(name.clone(), v);
            }
            consume(Value::Object(map))?;
        }
        Ok(())
    }
    fn execute(&self, sql: &str, args: &[Value]) -> anyhow::Result<usize> {
        Ok(self
            .db
            .execute(sql, rusqlite::params_from_iter(Self::parameters(args)))?)
    }
    pub fn exec(&mut self, sql: &str, args: &[Value]) -> anyhow::Result<Vec<Value>> {
        ensure!(!self.failed, "configuration storage failed; refusing plans");
        let head = sql.trim_start().to_ascii_lowercase();
        let read = head.starts_with("select") || head.starts_with("pragma");
        if read {
            return self.rows(sql, args);
        }
        let name = head
            .split_whitespace()
            .skip_while(|s| !matches!(*s, "into" | "update" | "from"))
            .nth(1)
            .unwrap_or("")
            .trim_start_matches("main.")
            .split('(')
            .next()
            .unwrap_or("");
        if CONFIG_TABLES.contains(&name) {
            ensure!(
                self.transaction,
                "configuration edits require an explicit transaction"
            );
            self.dirty.insert(name.into());
        }
        let result =
            if head.starts_with("insert") && name.starts_with("metrics_") && args.len() == 2 {
                let text = args[1].as_str().context("metrics must be JSON text")?;
                ensure!(text.len() <= 8 << 20, "metrics row too large");
                self.db.execute(
                    sql,
                    rusqlite::params![
                        args[0].as_i64().unwrap_or(0),
                        lz4_flex::compress_prepend_size(text.as_bytes())
                    ],
                )?;
                Vec::new()
            } else if head.contains("returning ") {
                self.rows(sql, args)?
            } else {
                self.execute(sql, args)?;
                Vec::new()
            };
        if name == "settings" {
            self.db.execute("DELETE FROM main.settings", [])?;
            for row in self.rows("SELECT key,value FROM temp.settings", &[])? {
                if durable_setting(row["key"].as_str().unwrap_or("")) {
                    self.execute(
                        "INSERT INTO main.settings(key,value) VALUES(?,?)",
                        &[row["key"].clone(), row["value"].clone()],
                    )?;
                }
            }
        }
        Ok(result)
    }
    pub fn begin(&mut self) -> anyhow::Result<()> {
        ensure!(
            !self.failed && !self.transaction,
            "invalid configuration transaction"
        );
        self.db.execute_batch("SAVEPOINT runner_config")?;
        self.transaction = true;
        Ok(())
    }
    pub fn rollback(&mut self) -> anyhow::Result<()> {
        if self.transaction {
            self.db
                .execute_batch("ROLLBACK TO runner_config; RELEASE runner_config")?;
            self.transaction = false;
            self.dirty.clear();
        }
        Ok(())
    }
    fn fingerprints(&self, name: &str) -> anyhow::Result<Fingerprints> {
        let t = table(name);
        let mut map = BTreeMap::new();
        self.scan(&format!("SELECT * FROM main.{name}"), &[], |row| {
            let keys = t.keys.iter().map(|c| row[*c].clone()).collect::<Vec<_>>();
            let values = t
                .columns
                .iter()
                .map(|c| row[*c].clone())
                .collect::<Vec<_>>();
            let digest = runner_core::auth::sha256(serde_json::to_vec(&values)?);
            map.insert(serde_json::to_string(&keys)?, (digest, keys));
            Ok(())
        })?;
        Ok(map)
    }
    // Caller must obtain a fresh quorum proof immediately before this call, and
    // must stop issuing plans on any error. A failed remote commit is not replayed.
    pub async fn commit(&mut self) -> anyhow::Result<()> {
        ensure!(self.transaction, "no configuration transaction");
        let result = self.commit_inner().await;
        if result.is_err() {
            let _ = self.rollback();
            self.failed = true;
        }
        result
    }
    async fn commit_inner(&mut self) -> anyhow::Result<()> {
        let mut next = BTreeMap::new();
        let mut batch = Vec::new();
        if self.d1.is_some() {
            for name in &self.dirty {
                let t = table(name);
                let before = self.known.get(name).context("missing D1 fingerprints")?;
                let after = self.fingerprints(name)?;
                self.scan(&format!("SELECT * FROM main.{name}"), &[], |row| {
                    let key = serde_json::to_string(
                        &t.keys.iter().map(|c| row[*c].clone()).collect::<Vec<_>>(),
                    )?;
                    if before.get(&key).map(|x| &x.0) != after.get(&key).map(|x| &x.0) {
                        batch.push(upsert(name, &row));
                    }
                    Ok(())
                })?;
                for (key, (_, keys)) in before {
                    if !after.contains_key(key) {
                        batch.push(Query {
                            sql: format!(
                                "DELETE FROM {name} WHERE {}",
                                t.keys
                                    .iter()
                                    .map(|k| format!("{k}=?"))
                                    .collect::<Vec<_>>()
                                    .join(" AND ")
                            ),
                            params: keys.clone(),
                        });
                    }
                }
                next.insert(name.clone(), after);
            }
        }
        if let Some(d1) = self.d1.as_mut() {
            if !batch.is_empty() {
                let id = uuid::Uuid::new_v4().to_string();
                let revision = d1.usage.revision;
                batch.insert(0,Query {sql:"SELECT CASE WHEN EXISTS (SELECT 1 FROM runner_meta WHERE id=1 AND ready=1 AND origin=? AND revision=?) THEN 1 ELSE json('configuration revision conflict') END AS allowed".into(),params:vec![json!(d1.origin),json!(revision)]});
                batch.push(Query {
                    sql: "UPDATE runner_meta SET revision=revision+1,commit_id=? WHERE id=1".into(),
                    params: vec![json!(id)],
                });
                if let Some((authority, proof)) = &self.authority {
                    authority.check(Some(proof)).await?;
                }
                if let Err(error) = d1.query(&batch).await {
                    let confirmed = d1.metadata().await.unwrap_or(Value::Null);
                    if confirmed["commit_id"] != id
                        || confirmed["revision"] != revision + 1
                        || confirmed["origin"] != d1.origin
                        || confirmed["ready"] != 1
                    {
                        return Err(error);
                    }
                }
                d1.usage.revision += 1;
                d1.usage.commits += 1;
            }
        }
        if self.d1.is_none() && !self.dirty.is_empty() {
            if let Some((authority, proof)) = &self.authority {
                authority.check(Some(proof)).await?;
            }
        }
        self.db.execute_batch("RELEASE runner_config")?;
        self.transaction = false;
        self.known.extend(next);
        self.dirty.clear();
        Ok(())
    }
    pub fn trim_metrics(&self, max_bytes: u64) -> anyhow::Result<()> {
        // Retention has a byte ceiling as well as time limits. Oldest fine-grain
        // history gives way first; already rolled-up history remains available.
        for table in ["metrics_1m", "metrics_10m", "metrics_1h"] {
            loop {
                let used:i64=self.db.query_row("SELECT coalesce((SELECT sum(length(data)) FROM metrics_1m),0)+coalesce((SELECT sum(length(data)) FROM metrics_10m),0)+coalesce((SELECT sum(length(data)) FROM metrics_1h),0)",[],|r|r.get(0))?;
                if used <= max_bytes as i64 {
                    return Ok(());
                }
                if self.db.execute(
                    &format!(
                        "DELETE FROM {table} WHERE t IN (SELECT t FROM {table} ORDER BY t LIMIT 32)"
                    ),
                    [],
                )? == 0
                {
                    break;
                }
            }
        }
        Ok(())
    }
}
