use anyhow::{bail, ensure};
use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::{Notify, mpsc, oneshot};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async_with_config,
    tungstenite::{Message, client::IntoClientRequest, protocol::WebSocketConfig},
};
use tokio_util::sync::CancellationToken;
type Socket = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;
struct Report {
    body: Arc<Value>,
    reply: oneshot::Sender<anyhow::Result<Option<Value>>>,
}

#[derive(Clone)]
pub struct ControlLink {
    requests: mpsc::Sender<Report>,
}
impl ControlLink {
    pub fn start(
        origin: String,
        token: String,
        wake: Arc<Notify>,
        cancel: CancellationToken,
    ) -> Self {
        let (requests, receiver) = mpsc::channel(1);
        tokio::spawn(run(origin, token, wake, receiver, cancel));
        Self { requests }
    }
    pub async fn report(&self, body: Arc<Value>) -> anyhow::Result<Option<Value>> {
        let (reply, receive) = oneshot::channel();
        self.requests
            .send(Report { body, reply })
            .await
            .map_err(|_| anyhow::anyhow!("controller link stopped"))?;
        receive
            .await
            .map_err(|_| anyhow::anyhow!("controller link closed"))?
    }
}
pub async fn connect(
    origin: &str,
    path: &str,
    token: &str,
    max_message: usize,
) -> anyhow::Result<Socket> {
    let mut url = url::Url::parse(&format!("{}{}", origin.trim_end_matches('/'), path))?;
    let scheme = if url.scheme() == "https" { "wss" } else { "ws" };
    url.set_scheme(scheme)
        .map_err(|_| anyhow::anyhow!("invalid WebSocket origin"))?;
    let mut request = url.as_str().into_client_request()?;
    request
        .headers_mut()
        .insert("authorization", format!("Bearer {token}").parse()?);
    let config = WebSocketConfig::default()
        .read_buffer_size(8192)
        .write_buffer_size(8192)
        .max_write_buffer_size(2 << 20)
        .max_message_size(Some(max_message))
        .max_frame_size(Some(max_message));
    let (socket, _) = tokio::time::timeout(
        Duration::from_secs(15),
        connect_async_with_config(request, Some(config), true),
    )
    .await??;
    Ok(socket)
}
async fn run(
    origin: String,
    token: String,
    wake: Arc<Notify>,
    mut requests: mpsc::Receiver<Report>,
    cancel: CancellationToken,
) {
    let mut socket: Option<Socket> = None;
    let mut sequence = 0_u64;
    let mut retry_at = Instant::now();
    loop {
        let report = if let Some(connection) = socket.as_mut() {
            tokio::select! {
                _ = cancel.cancelled() => break,
                request = requests.recv() => match request { Some(r) => r, None => break },
                message = connection.next() => {
                    match message {
                        Some(Ok(Message::Text(text))) => match serde_json::from_str::<Value>(&text) {
                            Ok(message) if message["type"] == "wake" => wake.notify_one(),
                            Ok(message) if message.is_object() => (), _ => socket = None,
                        },
                        Some(Ok(Message::Ping(_))) => { if connection.flush().await.is_err() { socket = None; } },
                        Some(Ok(Message::Pong(_))) => (), _ => socket = None,
                    }
                    continue;
                }
            }
        } else {
            tokio::select! { _ = cancel.cancelled() => break, report = requests.recv() => match report { Some(r) => r, None => break } }
        };
        if report.reply.is_closed() {
            continue;
        }
        if socket.is_none() && Instant::now() < retry_at {
            let _ = report.reply.send(Ok(None));
            continue;
        }
        if socket.is_none() {
            let result = tokio::select! { _ = cancel.cancelled() => break, result = connect(&origin,"/api/agent",&token,16_000_000) => result };
            match result {
                Ok(connection) => socket = Some(connection),
                Err(error) => {
                    use tokio_tungstenite::tungstenite::{Error, error::ProtocolError};
                    let unavailable = error.downcast_ref::<Error>().is_some_and(|e| match e {
                        Error::Http(response) => {
                            matches!(response.status().as_u16(), 400 | 404 | 405 | 426 | 501)
                        }
                        Error::Protocol(ProtocolError::WrongHttpVersion) => true,
                        _ => false,
                    });
                    if unavailable {
                        retry_at = Instant::now() + Duration::from_secs(60);
                        let _ = report.reply.send(Ok(None));
                    } else {
                        let _ = report.reply.send(Err(anyhow::anyhow!(
                            "controller WebSocket connection failed"
                        )));
                    }
                    continue;
                }
            }
        }
        sequence += 1;
        let connection = socket.as_mut().unwrap();
        let exchange = async {
            #[derive(serde::Serialize)]
            struct Envelope<'a> {
                #[serde(rename = "type")]
                kind: &'static str,
                id: u64,
                body: &'a Value,
            }
            let payload = serde_json::to_string(&Envelope {
                kind: "sync",
                id: sequence,
                body: &report.body,
            })?;
            ensure!(payload.len() <= 1 << 20, "controller report is too large");
            connection.send(Message::Text(payload.into())).await?;
            loop {
                let Some(message) = connection.next().await else {
                    bail!("controller connection closed");
                };
                match message? {
                    Message::Text(text) => {
                        let mut message: Value = serde_json::from_str(&text)?;
                        ensure!(message.is_object(), "invalid controller message");
                        if message["type"] == "wake" {
                            wake.notify_one();
                            continue;
                        }
                        if message["type"] != "plan" && message["type"] != "error" {
                            continue;
                        }
                        let id = message["id"]
                            .as_u64()
                            .filter(|id| *id > 0 && *id < 1 << 53)
                            .ok_or_else(|| anyhow::anyhow!("invalid controller message ID"))?;
                        ensure!(
                            message["type"] != "plan" || message["plan"].is_object(),
                            "invalid controller plan"
                        );
                        if id != sequence {
                            continue;
                        }
                        if message["type"] == "error" {
                            bail!("controller refused report");
                        }
                        return Ok(Some(message["plan"].take()));
                    }
                    Message::Ping(_) => connection.flush().await?,
                    Message::Pong(_) => (),
                    _ => bail!("controller connection closed"),
                }
            }
        };
        let result = tokio::select! { _ = cancel.cancelled() => break, result = tokio::time::timeout(Duration::from_secs(30),exchange) => result.unwrap_or_else(|_| Err(anyhow::anyhow!("controller response timed out"))) };
        if result.is_err() {
            socket = None;
        }
        let _ = report.reply.send(result);
    }
    if let Some(mut socket) = socket {
        let _ = tokio::time::timeout(Duration::from_millis(250), socket.close(None)).await;
    }
}
