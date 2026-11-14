use runner_control::{bootstrap, node::Node, server};
use std::{collections::BTreeMap, time::Duration};
use tokio_util::sync::CancellationToken;
#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    unsafe {
        libc::umask(0o077);
    }
    let command = std::env::args().nth(1).unwrap_or_else(|| "start".into());
    if command == "--version" {
        println!(
            "runner-control {} {}",
            env!("CARGO_PKG_VERSION"),
            option_env!("RUNNER_BUILD_REVISION").unwrap_or("development")
        );
        return Ok(());
    }
    if command == "healthcheck" {
        let client = runner_core::http_client(Duration::from_millis(3500))?;
        let base = format!("http://127.0.0.1:{}", runner_core::env("PORT", "8920"));
        let _: serde_json::Value =
            runner_core::response_json(client.get(format!("{base}/healthz")).send().await?, 16384)
                .await?;
        if runner_core::env("CP_REPLICAS", "1") == "1" {
            let _: serde_json::Value = runner_core::response_json(
                client.get(format!("{base}/readyz")).send().await?,
                16384,
            )
            .await?;
            if !matches!(runner_core::env("AUTO_TUNNEL", "1").as_str(), "0" | "false") {
                anyhow::ensure!(
                    client
                        .get(format!(
                            "http://127.0.0.1:{}/ready",
                            runner_core::env("TUNNEL_METRICS_PORT", "49312")
                        ))
                        .send()
                        .await?
                        .status()
                        .is_success(),
                    "tunnel not ready"
                );
            }
        }
        return Ok(());
    }
    let env = bootstrap::settings(std::env::vars().collect::<BTreeMap<_, _>>())?;
    if command == "init" {
        bootstrap::initialize(env).await?;
        println!("Fleet initialized; identity is stored privately in DATA_DIR/identity.json");
        return Ok(());
    }
    let env = bootstrap::identity(env, true, command == "start")?;
    if command == "credentials" {
        println!(
            "{}",
            serde_json::to_string_pretty(
                &serde_json::json!({"ADMIN_PASSWORD":env["ADMIN_PASSWORD"],"JOIN_TOKEN":env["JOIN_TOKEN"]})
            )?
        );
        return Ok(());
    }
    anyhow::ensure!(
        command == "start",
        "command must be start, init or credentials"
    );
    let env = bootstrap::resolve(env).await?;
    let config = if env["CP_REPLICAS"] != "1" {
        Some(runner_raft::config::Config::from_map(&env)?)
    } else {
        None
    };
    let cancel = CancellationToken::new();
    let node = Node::new(env, config, cancel.clone())?;
    let bind = format!("{}:{}", node.e("BIND_HOST"), node.e("PORT"));
    let listener = tokio::net::TcpListener::bind(&bind).await?;
    node.start();
    let connector_node = node.clone();
    let connector = tokio::spawn(async move {
        if bootstrap::connector(connector_node.clone()).await.is_err() {
            eprintln!("controller connector unavailable");
            connector_node.cancel.cancel();
        }
    });
    let signal_node = node.clone();
    let signals = tokio::spawn(async move {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .expect("SIGTERM handler");
        tokio::select! {_=tokio::signal::ctrl_c()=>{},_=terminate.recv()=>{},_=signal_node.cancel.cancelled()=>return}
        signal_node
            .draining
            .store(true, std::sync::atomic::Ordering::Relaxed);
        if let Some(service) = signal_node.current.send_replace(None) {
            service.cancel.cancel();
        }
        if let Some(a) = &signal_node.authority {
            a.clear();
        }
        if let Some(raft) = &signal_node.raft {
            if raft.view().leader == raft.view().id {
                let _ = raft.transfer(0).await;
                for _ in 0..20 {
                    if raft.view().leader != raft.view().id {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(250)).await;
                }
            }
        }
        signal_node.cancel.cancel();
    });
    eprintln!("Rust controller listening on {bind}");
    let shutdown = cancel.clone();
    let result = axum::serve(
        listener,
        server::router(node).into_make_service_with_connect_info::<std::net::SocketAddr>(),
    )
    .with_graceful_shutdown(async move { shutdown.cancelled().await })
    .await;
    cancel.cancel();
    signals.abort();
    let _ = tokio::time::timeout(Duration::from_secs(8), connector).await;
    result?;
    Ok(())
}
