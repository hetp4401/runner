use tokio_util::sync::CancellationToken;

fn main() -> anyhow::Result<()> {
    if std::env::args().nth(1).as_deref() == Some("--version") {
        println!(
            "runner-agent {} {}",
            env!("CARGO_PKG_VERSION"),
            runner_agent::update::target()
        );
        return Ok(());
    }
    runner_agent::update::launch_cached()?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let cancel = CancellationToken::new();
        let signal = cancel.clone();
        tokio::spawn(async move {
            let mut term =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                    .expect("SIGTERM handler");
            tokio::select! {_=tokio::signal::ctrl_c()=>(),_=term.recv()=>()}
            signal.cancel();
        });
        runner_agent::app::run(cancel).await
    })
}
