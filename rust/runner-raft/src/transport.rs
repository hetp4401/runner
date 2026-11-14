use crate::{
    config::Config,
    engine::{Event, Packet},
    model::*,
};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use futures_util::future::join_all;
use protobuf::Message as _;
use raft::prelude::Message;
use runner_core::response_json;
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;

fn request(
    client: &reqwest::Client,
    config: &Config,
    method: reqwest::Method,
    url: String,
    generation: &str,
) -> reqwest::RequestBuilder {
    client
        .request(method, url)
        .header("x-cp-token", &config.secret)
        .header("x-cp-from", config.id)
        .header("x-cp-cluster", &config.cluster)
        .header("x-cp-generation", generation)
}
pub(crate) async fn worker(
    config: Arc<Config>,
    identity: Identity,
    peer: u64,
    client: reqwest::Client,
    mut packets: mpsc::Receiver<Arc<Packet>>,
    events: mpsc::Sender<Event>,
    cancel: CancellationToken,
) {
    let address = config.peers[&peer].trim_end_matches('/').to_owned() + "/_cp/raft";
    loop {
        let packet = tokio::select! { _ = cancel.cancelled() => return, packet = packets.recv() => match packet { Some(p) => p, None => return } };
        let send = async {
            let response = request(
                &client,
                &config,
                reqwest::Method::POST,
                address.clone(),
                &packet.generation,
            )
            .header("x-cp-boot", &identity.boot)
            .header("content-type", "application/octet-stream")
            .body(packet.data.clone())
            .send()
            .await?;
            let ok = response.status().is_success();
            // Drain a small response so keep-alive connections can be reused.
            let _ = runner_core::response_bytes(response, 4096).await;
            Ok::<_, anyhow::Error>(ok)
        };
        let ok = tokio::select! { _ = cancel.cancelled() => return, result = send => result.unwrap_or(false) };
        let delivered = Event::Delivered {
            to: packet.to,
            sequence: packet.sequence,
            snapshot: packet.snapshot,
            ok,
        };
        tokio::select! { _ = cancel.cancelled() => return, _ = events.send(delivered) => () }
    }
}

async fn call<T: serde::de::DeserializeOwned>(
    client: &reqwest::Client,
    config: &Config,
    peer: u64,
    path: &str,
    body: Option<&Identity>,
    generation: &str,
    timeout: Duration,
) -> anyhow::Result<T> {
    let method = if body.is_some() {
        reqwest::Method::POST
    } else {
        reqwest::Method::GET
    };
    let mut query = request(
        client,
        config,
        method,
        format!("{}{}", config.peers[&peer].trim_end_matches('/'), path),
        generation,
    )
    .timeout(timeout);
    if let Some(body) = body {
        query = query.json(body);
    }
    response_json(query.send().await?, 1 << 20).await
}

pub(crate) async fn network(
    config: Arc<Config>,
    identity: Identity,
    client: reqwest::Client,
    status: watch::Receiver<Arc<View>>,
    events: mpsc::Sender<Event>,
    cancel: CancellationToken,
) {
    let mut timer = tokio::time::interval(Duration::from_millis(500));
    timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut last_join = Instant::now() - Duration::from_secs(10);
    let mut last_canonical = last_join;
    let mut canonical = false;
    loop {
        tokio::select! { _ = cancel.cancelled() => return, _ = timer.tick() => () }
        let own = status.borrow().clone();
        if own.phase == "retired" {
            continue;
        }
        if (own.generation.is_empty() || own.phase == "joining")
            && last_join.elapsed() > Duration::from_secs(2)
        {
            last_join = Instant::now();
            for peer in &config.members {
                if cancel.is_cancelled() {
                    return;
                }
                let remote: View = match call(
                    &client,
                    &config,
                    peer.id,
                    "/_cp/raft-status",
                    None,
                    "",
                    Duration::from_millis(1200),
                )
                .await
                {
                    Ok(v) => v,
                    Err(_) => continue,
                };
                if !remote.identity_valid
                    || remote.leader_node != remote.node
                    || remote.generation.is_empty()
                {
                    continue;
                }
                let result: anyhow::Result<Registry> = call(
                    &client,
                    &config,
                    peer.id,
                    "/_cp/join",
                    Some(&identity),
                    &remote.generation,
                    Duration::from_secs(5),
                )
                .await;
                if let Ok(registry) = result {
                    if !registry.generation.is_empty() {
                        if events.send(Event::Adopt(registry)).await.is_err() {
                            return;
                        }
                        break;
                    }
                }
            }
        }
        let own = status.borrow().clone();
        if own.generation.is_empty() {
            continue;
        }
        if last_canonical.elapsed() > Duration::from_secs(3) {
            last_canonical = Instant::now();
            let result: anyhow::Result<View> = call(
                &client,
                &config,
                identity.id,
                "/_cp/raft-status",
                None,
                "",
                Duration::from_secs(1),
            )
            .await;
            canonical = result.is_ok_and(|v| v.node == identity.node);
        }
        if canonical && own.identity_valid {
            continue;
        }
        // At most one small pull per configured peer; no timer overlap or task
        // growth while an old hostname still points to the previous process.
        let pulls = config.members.iter().map(|peer| {
            let own = own.clone();
            let identity = &identity;
            let config = &config;
            let client = &client;
            let events = &events;
            async move {
                let path = format!("/_cp/pull?node={}&boot={}", identity.node, identity.boot);
                let result: anyhow::Result<Vec<Envelope>> = call(
                    client,
                    config,
                    peer.id,
                    &path,
                    None,
                    &own.generation,
                    Duration::from_millis(1200),
                )
                .await;
                if let Ok(messages) = result {
                    for item in messages.into_iter().take(64) {
                        let Ok(data) = STANDARD.decode(item.data) else {
                            continue;
                        };
                        let Ok(message) = Message::parse_from_bytes(&data) else {
                            continue;
                        };
                        if message.to != identity.node
                            || !own.registry.nodes.contains_key(&message.from)
                        {
                            continue;
                        }
                        if events.send(Event::Message(message, None)).await.is_err() {
                            return;
                        }
                    }
                }
            }
        });
        tokio::select! { _ = cancel.cancelled() => return, _ = join_all(pulls) => () }
    }
}
