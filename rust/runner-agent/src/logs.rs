use crate::{args, link::connect, state::Context};
use anyhow::ensure;
use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use std::{collections::BTreeMap, time::Duration};
use tokio::{
    io::{AsyncRead, AsyncReadExt},
    sync::mpsc,
    task::JoinSet,
};
use tokio_tungstenite::tungstenite::Message;
use tokio_util::sync::CancellationToken;

pub struct Logs {
    tasks: BTreeMap<String, tokio::task::JoinHandle<()>>,
    context: Context,
    cancel: CancellationToken,
}
impl Logs {
    pub fn new(context: Context, cancel: CancellationToken) -> Self {
        Self {
            tasks: BTreeMap::new(),
            context,
            cancel,
        }
    }
    pub async fn reconcile(&mut self, wanted: &Value) {
        self.tasks.retain(|_, task| !task.is_finished());
        for request in wanted.as_array().into_iter().flatten() {
            if self.tasks.len() >= 4 {
                break;
            }
            let session = request["session"].as_str().unwrap_or("");
            let key = request["key"].as_str().unwrap_or("");
            if session.len() != 36
                || !session.bytes().all(|c| c.is_ascii_hexdigit() || c == b'-')
                || self.tasks.contains_key(session)
            {
                continue;
            }
            let state = self.context.state.read().await;
            let Some(project) = state.projects.get(key).cloned() else {
                continue;
            };
            let run = state.s("run").to_owned();
            drop(state);
            let context = self.context.clone();
            let session_owned = session.to_owned();
            let key = key.to_owned();
            let cancel = self.cancel.clone();
            let tail = request["tail"].as_u64().unwrap_or(200).min(1000);
            self.tasks.insert(
                session.into(),
                tokio::spawn(async move {
                    let _ = stream_logs(context, &session_owned, &run, &key, project, tail, cancel)
                        .await;
                }),
            );
        }
    }
}
impl Drop for Logs {
    fn drop(&mut self) {
        for task in self.tasks.values() {
            task.abort();
        }
    }
}
async fn reader(
    mut stream: impl AsyncRead + Unpin,
    tx: mpsc::Sender<Vec<u8>>,
) -> anyhow::Result<()> {
    let mut buffer = [0u8; 8192];
    loop {
        let count = stream.read(&mut buffer).await?;
        if count == 0 {
            return Ok(());
        }
        if tx.send(buffer[..count].to_vec()).await.is_err() {
            return Ok(());
        }
    }
}
async fn stream_logs(
    context: Context,
    session: &str,
    run: &str,
    key: &str,
    project: Value,
    tail: u64,
    cancel: CancellationToken,
) -> anyhow::Result<()> {
    let mut query = url::Url::parse("http://local/api/logs/stream")?;
    query
        .query_pairs_mut()
        .append_pair("session", session)
        .append_pair("run", run);
    let path = format!("{}?{}", query.path(), query.query().unwrap_or(""));
    let mut connection = connect(&context.control, &path, &context.token, 1 << 20).await?;
    let containers = context.host.owned(key, &project).await?;
    ensure!(
        !containers.is_empty() && containers.len() <= 64,
        "the application's log sources are unavailable or too numerous"
    );
    let (tx, mut rx) = mpsc::channel(8);
    let mut readers = JoinSet::new();
    let mut children = Vec::new();
    for container in containers {
        let id = container["Id"].as_str().unwrap_or("");
        let mut child = context
            .host
            .commands
            .process(
                "docker",
                &args!["logs", "--follow", "--timestamps", "--tail", tail, id],
                &BTreeMap::new(),
            )
            .spawn()?;
        readers.spawn(reader(child.stdout.take().unwrap(), tx.clone()));
        readers.spawn(reader(child.stderr.take().unwrap(), tx.clone()));
        children.push(child);
    }
    drop(tx);
    let mut pending = Vec::with_capacity(64 << 10);
    let mut timer = tokio::time::interval(Duration::from_millis(250));
    let result = async {
        loop {
            let mut flush_all = false;
            tokio::select! {
                _ = cancel.cancelled() => break,
                message = connection.next() => match message { Some(Ok(Message::Ping(_))) => connection.flush().await?, Some(Ok(Message::Pong(_))) => (), _ => break },
                chunk = rx.recv() => match chunk { Some(chunk) => { pending.extend_from_slice(&chunk); if pending.len() < 64_000 { continue; } flush_all = true; }, None => { if !pending.is_empty() { connection.send(Message::Text(String::from_utf8_lossy(&pending).into_owned().into())).await?; } break; } },
                _ = timer.tick() => (),
            }
            let cut = if flush_all { pending.len() } else { pending.iter().rposition(|b| *b == b'\n').map_or(0,|i| i+1) };
            if cut > 0 {
                let text = String::from_utf8_lossy(&pending[..cut]).into_owned();
                tokio::time::timeout(Duration::from_secs(10),connection.send(Message::Text(text.into()))).await??;
                pending.drain(..cut);
            }
        }
        Ok::<_,anyhow::Error>(())
    }.await;
    readers.abort_all();
    for mut child in children {
        let _ = child.kill().await;
    }
    let _ = tokio::time::timeout(Duration::from_millis(250), connection.close(None)).await;
    result
}
