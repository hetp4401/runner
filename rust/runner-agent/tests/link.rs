use runner_agent::link::ControlLink;
use serde_json::json;
use std::{sync::Arc, time::Duration};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_util::sync::CancellationToken;

#[tokio::test(flavor = "current_thread")]
async fn unsupported_upgrades_use_authenticated_http_without_hiding_authentication_failures()
-> anyhow::Result<()> {
    for (status, fallback) in [
        ("HTTP/1.0 404 Not Found", true),
        ("HTTP/1.1 426 Upgrade Required", true),
        ("HTTP/1.1 401 Unauthorized", false),
    ] {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let origin = format!("http://{}", listener.local_addr()?);
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = [0u8; 4096];
            let size = socket.read(&mut request).await.unwrap();
            assert!(
                String::from_utf8_lossy(&request[..size])
                    .to_ascii_lowercase()
                    .contains("authorization: bearer test-join")
            );
            socket
                .write_all(
                    format!("{status}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                        .as_bytes(),
                )
                .await
                .unwrap();
        });
        let cancel = CancellationToken::new();
        let link = ControlLink::start(
            origin,
            "test-join".into(),
            Arc::new(tokio::sync::Notify::new()),
            cancel.clone(),
        );
        let result = tokio::time::timeout(
            Duration::from_secs(3),
            link.report(Arc::new(json!({"run":"test"}))),
        )
        .await?;
        if fallback {
            assert!(result?.is_none());
        } else {
            assert!(result.is_err());
        }
        cancel.cancel();
        server.await?;
    }
    Ok(())
}
