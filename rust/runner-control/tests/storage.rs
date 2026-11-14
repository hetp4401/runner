use axum::{Json, Router, extract::State, http::StatusCode, response::IntoResponse, routing::post};
use runner_control::{
    control::Control,
    storage::{CONFIG_TABLES, Query, Store, table},
};
use rusqlite::{
    Connection,
    types::{Value as SqlValue, ValueRef},
};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

struct Fixture {
    db: Connection,
    lose_response: bool,
    reject: bool,
    requests: usize,
}
async fn query(
    State(state): State<Arc<Mutex<Fixture>>>,
    Json(body): Json<Value>,
) -> axum::response::Response {
    let batch: Vec<Query> = serde_json::from_value(body["batch"].clone()).unwrap();
    let mut state = state.lock().unwrap();
    state.requests += 1;
    let writes = batch
        .iter()
        .any(|q| q.sql.starts_with("UPDATE runner_meta"));
    if writes && state.reject {
        return (StatusCode::OK,Json(json!({"success":false,"errors":[{"code":1,"message":"fixture-rejected-bound-secret"}]}))).into_response();
    }
    state.db.execute_batch("BEGIN").unwrap();
    let result = (|| -> anyhow::Result<Vec<Value>> {
        let mut result = Vec::new();
        for q in &batch {
            let mut statement = state.db.prepare(&q.sql)?;
            let params = q
                .params
                .iter()
                .map(|p| match p {
                    Value::Null => SqlValue::Null,
                    Value::Number(n) => SqlValue::Integer(n.as_i64().unwrap()),
                    Value::String(s) => SqlValue::Text(s.clone()),
                    _ => SqlValue::Text(p.to_string()),
                })
                .collect::<Vec<_>>();
            let names = statement
                .column_names()
                .into_iter()
                .map(str::to_owned)
                .collect::<Vec<_>>();
            let mut values = Vec::new();
            if names.is_empty() {
                statement.execute(rusqlite::params_from_iter(params))?;
            } else {
                let mut rows = statement.query(rusqlite::params_from_iter(params))?;
                while let Some(row) = rows.next()? {
                    let mut value = serde_json::Map::new();
                    for (i, name) in names.iter().enumerate() {
                        value.insert(
                            name.clone(),
                            match row.get_ref(i)? {
                                ValueRef::Null => Value::Null,
                                ValueRef::Integer(n) => json!(n),
                                ValueRef::Real(n) => json!(n),
                                ValueRef::Text(s) => json!(std::str::from_utf8(s)?),
                                _ => Value::Null,
                            },
                        );
                    }
                    values.push(Value::Object(value));
                }
            }
            result.push(json!({"success":true,"results":values,"meta":{"rows_read":values.len(),"rows_written":0}}));
        }
        Ok(result)
    })();
    match result {
        Ok(result) => {
            state.db.execute_batch("COMMIT").unwrap();
            if writes && state.lose_response {
                state.lose_response = false;
                (
                    StatusCode::BAD_GATEWAY,
                    "fixture lost response after commit",
                )
                    .into_response()
            } else {
                Json(json!({"success":true,"result":result})).into_response()
            }
        }
        Err(_) => {
            state.db.execute_batch("ROLLBACK").unwrap();
            Json(json!({"success":false,"errors":[{"code":2,"message":"fixture rejection"}]}))
                .into_response()
        }
    }
}
async fn fixture() -> (
    BTreeMap<String, String>,
    Arc<Mutex<Fixture>>,
    tokio::task::JoinHandle<()>,
) {
    let db = Connection::open_in_memory().unwrap();
    for name in CONFIG_TABLES {
        db.execute_batch(&format!("CREATE TABLE {name} ({})", table(name).schema))
            .unwrap();
    }
    db.execute_batch("CREATE TABLE runner_meta(id INTEGER PRIMARY KEY,schema_version INTEGER,origin TEXT,ready INTEGER,revision INTEGER,commit_id TEXT); INSERT INTO runner_meta VALUES(1,1,'runners.test.example',1,0,'initial');").unwrap();
    let state = Arc::new(Mutex::new(Fixture {
        db,
        lose_response: false,
        reject: false,
        requests: 0,
    }));
    let app = Router::new()
        .route("/accounts/test/d1/database/test/query", post(query))
        .with_state(state.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let env = BTreeMap::from([
        ("CONFIG_STORAGE".into(), "d1".into()),
        ("ACCOUNT_ID".into(), "test".into()),
        ("D1_DATABASE_ID".into(), "test".into()),
        ("D1_API_KEY".into(), "test-key".into()),
        ("D1_API_EMAIL".into(), "test@example.com".into()),
        ("D1_API_BASE".into(), base),
        ("CONTROL_HOST".into(), "runners.test.example".into()),
        ("DOMAIN".into(), "test.example".into()),
        ("DNS".into(), "off".into()),
    ]);
    (env, state, server)
}
#[tokio::test(flavor = "current_thread")]
async fn d1_commits_are_atomic_fenced_and_confirmed_without_replay() -> anyhow::Result<()> {
    let (mut env, fixture, server) = fixture().await;
    let cache_root = tempfile::tempdir()?;
    let cache_root_path = cache_root.path().canonicalize()?;
    env.insert(
        "DATA_DIR".into(),
        cache_root_path.to_string_lossy().into_owned(),
    );
    let now = 1_800_000_000_000;
    let store = Store::open(&env).await?;
    let mut c = Control::new(store, env.clone(), now)?;
    c.store.begin()?;
    c.put_project("web",json!({"compose":"services:\n  web:\n    image: nginx\n    ports: [\"8080:80\"]\n","port":8080,"replicas":2,"env":{"TOKEN":"private-value"}}),false,now)?;
    c.save_app_password("web", "test-password", now)?;
    c.store.commit().await?;
    assert_eq!(c.store.d1.as_ref().unwrap().usage.revision, 1);
    let loaded = Store::open(&env).await?;
    let mut stale = Control::new(loaded, env.clone(), now)?;
    assert_eq!(stale.app_env["web"]["TOKEN"], "private-value");
    assert!(stale.app_passwords.contains_key("web"));
    assert!(stale.runs.is_empty());
    let before = fixture.lock().unwrap().requests;
    c.store.begin()?;
    for i in 0..200 {
        c.set_setting("last_hot_move", i)?;
        c.log_event("test", json!({"detail":"runtime"}), now + i)?;
    }
    c.put_project("web", json!({"replicas":2}), false, now)?;
    c.store.commit().await?;
    assert_eq!(
        fixture.lock().unwrap().requests,
        before,
        "runtime changes must not hit D1"
    );
    let cache_directory = cache_root_path.join(".native-config-cache");
    for entry in std::fs::read_dir(&cache_directory)? {
        use std::os::unix::fs::PermissionsExt;
        let entry = entry?;
        assert_eq!(entry.metadata()?.permissions().mode() & 0o077, 0);
        if entry.path().extension().is_some_and(|s| s == "sqlite") {
            let disk = Connection::open_with_flags(
                entry.path(),
                rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
            )?;
            let tables = disk
                .prepare("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")?
                .query_map([], |row| row.get::<_, String>(0))?
                .collect::<Result<Vec<_>, _>>()?;
            assert_eq!(
                tables,
                [
                    "app_env",
                    "app_passwords",
                    "projects",
                    "settings",
                    "versions"
                ]
            );
            assert_eq!(
                disk.query_row(
                    "SELECT count(*) FROM settings WHERE key='last_hot_move'",
                    [],
                    |r| r.get::<_, u64>(0)
                )?,
                0
            );
        }
    }
    c.store.begin()?;
    c.put_env("web", &json!({"TOKEN":"changed"}), now + 1)?;
    c.store.commit().await?;
    assert_eq!(c.projects["web"].version, 2);
    stale.store.begin()?;
    stale.set_enabled("web", false, now + 2)?;
    assert!(stale.store.commit().await.is_err());
    assert!(stale.store.failed);
    assert_eq!(
        fixture
            .lock()
            .unwrap()
            .db
            .query_row("SELECT enabled FROM projects", [], |r| r.get::<_, u64>(0))?,
        1
    );
    fixture.lock().unwrap().lose_response = true;
    c.store.begin()?;
    c.put_settings(&json!({"rebalance":false}), now)?;
    c.store.commit().await?;
    assert_eq!(c.store.d1.as_ref().unwrap().usage.revision, 3);
    assert_eq!(
        fixture
            .lock()
            .unwrap()
            .db
            .query_row("SELECT revision FROM runner_meta", [], |r| r
                .get::<_, u64>(0))?,
        3,
        "lost response must not repeat an edit"
    );
    fixture.lock().unwrap().reject = true;
    c.store.begin()?;
    c.put_env("web", &json!({"TOKEN":"rejected"}), now + 3)?;
    let error = c.store.commit().await.unwrap_err().to_string();
    assert!(!error.contains("bound-secret"));
    assert!(c.store.failed);
    assert_eq!(
        fixture
            .lock()
            .unwrap()
            .db
            .query_row("SELECT value FROM app_env", [], |r| r.get::<_, String>(0))?,
        "changed"
    );
    assert!(c.store.rows("SELECT * FROM projects", &[]).is_err());
    drop(stale);
    drop(c);
    assert_eq!(
        std::fs::read_dir(cache_directory)?.count(),
        0,
        "disposable configuration caches must be removed on close"
    );
    server.abort();
    Ok(())
}
#[tokio::test(flavor = "current_thread")]
async fn runtime_data_never_survives_a_local_controller_restart() -> anyhow::Result<()> {
    let temp = tempfile::tempdir()?;
    let path = std::fs::canonicalize(temp.path())?;
    let env = BTreeMap::from([
        ("DATA_DIR".into(), path.to_string_lossy().into_owned()),
        ("INIT_CONTROL_DB".into(), "1".into()),
    ]);
    let mut store = Store::open(&env).await?;
    store.begin()?;
    store.exec(
        "INSERT INTO settings(key,value) VALUES(?,?)",
        &[json!("rebalance"), json!("off")],
    )?;
    store.exec(
        "INSERT INTO settings(key,value) VALUES(?,?)",
        &[json!("last_hot_move"), json!("123")],
    )?;
    store.exec(
        "INSERT INTO events(t,kind) VALUES(?,?)",
        &[json!(1), json!("test")],
    )?;
    store.commit().await?;
    drop(store);
    let loaded = Store::open(&env).await?;
    assert_eq!(
        loaded.rows("SELECT * FROM settings", &[])?,
        vec![json!({"key":"rebalance","value":"off"})]
    );
    assert!(loaded.rows("SELECT * FROM events", &[])?.is_empty());
    Ok(())
}
#[test]
fn compressed_metrics_have_independent_size_and_retention_bounds() -> anyhow::Result<()> {
    let mut store = Store::memory()?;
    let text = json!({"host":{"h":{"cpu":12.5},"a":{"application":{"cpu":4.1}}}}).to_string();
    for i in 0..1000 {
        store.exec(
            "INSERT INTO metrics_1m(t,data) VALUES(?,?)",
            &[json!(i), json!(text)],
        )?;
    }
    assert_eq!(
        store.rows("SELECT data FROM metrics_1m LIMIT 1", &[])?[0]["data"],
        text
    );
    store.trim_metrics(2048)?;
    assert!(
        store.rows("SELECT count(*) AS n FROM metrics_1m", &[])?[0]["n"]
            .as_u64()
            .unwrap()
            < 1000
    );
    Ok(())
}
