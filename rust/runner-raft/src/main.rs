use runner_raft::{RaftService, config::Config};
use tokio_util::sync::CancellationToken;

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let cancel = CancellationToken::new();
    let service = RaftService::start(Config::from_env()?, cancel.clone())?;
    let port = runner_core::number("CP_RAFT_PORT", 8931)?;
    let listener = tokio::net::TcpListener::bind(format!("127.0.0.1:{port}")).await?;
    let signal = cancel.clone();
    tokio::spawn(async move {
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("SIGTERM handler");
        tokio::select! { _ = tokio::signal::ctrl_c() => (), _ = term.recv() => () }
        signal.cancel();
    });
    axum::serve(listener, service.router())
        .with_graceful_shutdown(cancel.cancelled_owned())
        .await?;
    Ok(())
}
