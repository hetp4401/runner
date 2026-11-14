use axum::{Json, Router, extract::State, routing::get};
use runner_agent::update;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::sync::{Arc, Mutex};

struct Fixture {
    manifest: Value,
    binary: Vec<u8>,
}

#[tokio::test(flavor = "current_thread")]
async fn mirrored_updates_verify_content_and_preserve_the_selected_release_on_failure()
-> anyhow::Result<()> {
    let binary = b"isolated fixture binary".to_vec();
    let digest = hex::encode(Sha256::digest(&binary));
    let fixture = Arc::new(Mutex::new(Fixture {
        manifest: json!({"schema":1,"runtime":"rust","targets":{
            update::target(): {"sha256":digest,"size":binary.len(),"path":"/agent/rust/test-binary"}
        }}),
        binary,
    }));
    let app = Router::new()
        .route(
            "/custom-agent/rust/manifest.json",
            get(async |State(s): State<Arc<Mutex<Fixture>>>| {
                Json(s.lock().unwrap().manifest.clone())
            }),
        )
        .route(
            "/custom-agent/rust/test-binary",
            get(async |State(s): State<Arc<Mutex<Fixture>>>| s.lock().unwrap().binary.clone()),
        )
        .with_state(fixture.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let url = format!("http://{}/custom-agent", listener.local_addr()?);
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let temporary = tempfile::tempdir()?;
    let data = temporary.path().canonicalize()?;
    let client = runner_core::http_client(std::time::Duration::from_secs(5))?;
    assert_eq!(update::install(&data, &url, &client).await?, digest);
    let pointer = data.join("agent-code/rust-current.json");
    let selected = std::fs::read(&pointer)?;
    let releases = data.join("agent-code/releases");
    assert_eq!(
        std::fs::read(releases.join(format!("rust-{digest}/runner-agent")))?,
        b"isolated fixture binary"
    );
    {
        let mut f = fixture.lock().unwrap();
        f.manifest["targets"][update::target()]["sha256"] = json!("a".repeat(64));
    }
    assert!(update::install(&data, &url, &client).await.is_err());
    assert_eq!(std::fs::read(&pointer)?, selected);
    assert_eq!(
        std::fs::read_dir(&releases)?.count(),
        1,
        "bad downloads must not accumulate"
    );
    {
        let mut f = fixture.lock().unwrap();
        f.manifest["targets"][update::target()]["size"] = json!(33 * 1024 * 1024);
    }
    assert!(update::install(&data, &url, &client).await.is_err());
    assert_eq!(std::fs::read(&pointer)?, selected);
    server.abort();
    Ok(())
}
