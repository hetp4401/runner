use crate::{
    api::{self, Output},
    control::{n, s},
    error::{HttpError, http},
    node::Node,
    service::Service,
};
use axum::{
    Json, Router,
    body::{Body, to_bytes},
    extract::{
        ConnectInfo, Request, State, WebSocketUpgrade,
        ws::{Message, WebSocket, rejection::WebSocketUpgradeRejection},
    },
    http::{HeaderMap, Method, StatusCode},
    response::{IntoResponse, Response},
};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{collections::BTreeMap, net::SocketAddr, sync::Arc, time::Duration};
pub const PAGE: &str = include_str!("../../../control/src/app.html");
pub fn router(node: Arc<Node>) -> Router {
    Router::new().fallback(endpoint).with_state(node)
}
pub fn json_response(code: u16, value: Value) -> Response {
    let mut response = (
        StatusCode::from_u16(code).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
        Json(value),
    )
        .into_response();
    response
        .headers_mut()
        .insert("cache-control", "no-store".parse().unwrap());
    response
}
pub fn failure(error: anyhow::Error) -> Response {
    let (status, text) = error
        .downcast_ref::<HttpError>()
        .map(|e| (e.status, e.message.clone()))
        .unwrap_or((500, "request failed".into()));
    json_response(status, json!({"error":text}))
}
fn text_response(code: u16, kind: &str, body: impl Into<Body>) -> Response {
    Response::builder()
        .status(code)
        .header("content-type", kind)
        .header("cache-control", "no-store")
        .header("server", "runner-control-rust")
        .body(body.into())
        .unwrap()
}
fn redirect(to: &str) -> Response {
    Response::builder()
        .status(302)
        .header("location", to)
        .header("cache-control", "no-store")
        .body(Body::empty())
        .unwrap()
}
fn bearer(node: &Node, headers: &HeaderMap) -> bool {
    !node.e("JOIN_TOKEN").is_empty()
        && headers
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .is_some_and(|v| {
                runner_core::auth::equal_secret(v, &format!("Bearer {}", node.e("JOIN_TOKEN")))
            })
}
fn header<'a>(headers: &'a HeaderMap, key: &str) -> &'a str {
    headers.get(key).and_then(|v| v.to_str().ok()).unwrap_or("")
}
async fn bounded_body(
    node: &Node,
    body: Body,
    limit: usize,
) -> anyhow::Result<(bytes::Bytes, tokio::sync::OwnedSemaphorePermit)> {
    let permit = node
        .bodies
        .clone()
        .try_acquire_owned()
        .map_err(|_| http(503, "controller busy; retry shortly"))?;
    let bytes = tokio::time::timeout(Duration::from_secs(10), to_bytes(body, limit))
        .await
        .map_err(|_| http(408, "request body timeout"))?
        .map_err(|_| http(413, "the request is too big"))?;
    Ok((bytes, permit))
}

async fn endpoint(
    State(node): State<Arc<Node>>,
    ConnectInfo(remote): ConnectInfo<SocketAddr>,
    ws: Result<WebSocketUpgrade, WebSocketUpgradeRejection>,
    request: Request,
) -> Response {
    match handle(node, remote, ws, request).await {
        Ok(response) => response,
        Err(error) => failure(error),
    }
}
async fn handle(
    node: Arc<Node>,
    remote: SocketAddr,
    ws: Result<WebSocketUpgrade, WebSocketUpgradeRejection>,
    request: Request,
) -> anyhow::Result<Response> {
    let path = request.uri().path().trim_end_matches('/').to_owned();
    let path = if path.is_empty() {
        "/".to_owned()
    } else {
        path
    };
    let query = url::form_urlencoded::parse(request.uri().query().unwrap_or("").as_bytes())
        .into_owned()
        .collect::<BTreeMap<String, String>>();
    if path.starts_with("/_cp/") {
        if !runner_core::auth::equal_secret(
            header(request.headers(), "x-cp-token"),
            node.e("CP_CLUSTER_TOKEN"),
        ) {
            return Err(http(401, "unauthorized"));
        }
        if [
            "/_cp/raft-status",
            "/_cp/join",
            "/_cp/pull",
            "/_cp/bootstrap",
            "/_cp/raft",
            "/_cp/transfer",
        ]
        .contains(&path.as_str())
        {
            return Ok(node
                .raft
                .as_ref()
                .ok_or_else(|| http(503, "voter unavailable"))?
                .handle(request)
                .await);
        }
        let value = match path.as_str() {
            "/_cp/status" => node.local_view(),
            "/_cp/tail" => {
                let after = query.get("after").and_then(|s| s.parse().ok()).unwrap_or(0);
                let mut out = node.tail.lock().unwrap().read(after)?;
                out["incarnation"] = json!(node.incarnation);
                out
            }
            "/_cp/recovery" => json!({"reports":node.tail.lock().unwrap().reports()?}),
            "/_cp/verify" => {
                let expected = if request.method() == Method::POST {
                    let (bytes, _permit) = bounded_body(&node, request.into_body(), 4096).await?;
                    serde_json::from_slice(&bytes).map_err(|_| http(400, "invalid authority"))?
                } else {
                    None
                };
                json!(
                    node.authority
                        .as_ref()
                        .ok_or_else(|| http(503, "authority unavailable"))?
                        .check(expected.as_ref())
                        .await
                        .map_err(|_| http(503, "authority unavailable"))?
                )
            }
            "/_cp/report" if request.method() == Method::POST => {
                let (bytes, _permit) = bounded_body(&node, request.into_body(), 1_000_000).await?;
                let body: Value =
                    serde_json::from_slice(&bytes).map_err(|_| http(400, "invalid report"))?;
                let expected = serde_json::from_value(body["authority"].clone())
                    .map_err(|_| http(400, "invalid authority"))?;
                node.authority
                    .as_ref()
                    .ok_or_else(|| http(503, "authority unavailable"))?
                    .check(Some(&expected))
                    .await
                    .map_err(|_| http(503, "authority unavailable"))?;
                node.tail.lock().unwrap().append(
                    s(&body, "kind"),
                    &body["body"],
                    runner_core::now_ms(),
                )?;
                json!({"ok":true})
            }
            _ => return Err(http(404, "unknown peer endpoint")),
        };
        return Ok(json_response(200, value));
    }
    if request.method() == Method::GET {
        match path.as_str() {
            "/" => return Ok(text_response(200, "text/html; charset=utf-8", PAGE)),
            "/cluster" => return Ok(redirect("/#/fleet")),
            "/admin" => return Ok(redirect("/?from=admin")),
            "/metrics" => return Ok(redirect("/?from=metrics")),
            "/api/cluster" => return Ok(json_response(200, node.cluster_view())),
            "/healthz" => {
                return Ok(json_response(
                    200,
                    json!({"ok":!node.cancel.is_cancelled(),"tunnelsReady":node.tunnel_ready.load(std::sync::atomic::Ordering::Relaxed)||matches!(node.e("AUTO_TUNNEL"),"0"|"false"),"mode":if node.raft.is_some(){"raft"}else{"single"},"runtime":"rust","replica":node.config.as_ref().map(|c|c.id).unwrap_or(1),"primary":node.service().is_some(),"quorum":node.config.as_ref().map(|c|c.members.len()/2+1).unwrap_or(1)}),
                ));
            }
            "/readyz" => {
                let ready = node
                    .raft
                    .as_ref()
                    .map(|r| {
                        let v = r.view();
                        v.identity_valid && v.outgoing.is_empty()
                    })
                    .unwrap_or(node.service().is_some());
                return Ok(json_response(
                    if ready { 200 } else { 503 },
                    json!({"ok":ready,"voterReady":ready,"replacementWaiting":node.raft.as_ref().is_some_and(|r|r.view().phase=="joining"),"bootstrapRequired":node.raft.as_ref().is_some_and(|r|r.view().phase=="uninitialized")}),
                ));
            }
            _ => {}
        }
    }
    if request.method() == Method::GET && (path == "/install.sh" || path.starts_with("/agent/")) {
        return crate::artifacts::serve(&node, &path).await;
    }
    let Some(service) = node.service() else {
        if let Some(target) = node.destination(request.headers().contains_key("x-cp-forwarded")) {
            return if let Ok(ws) = ws {
                proxy_socket(node, ws, request, target, remote).await
            } else {
                proxy_http(node, request, target, remote).await
            };
        }
        return Err(http(
            503,
            "no primary is ready; existing workloads keep running",
        ));
    };
    if path == "/api/leadership/verify" && request.method() == Method::POST {
        if !bearer(&node, request.headers()) {
            return Err(http(401, "unauthorized"));
        }
        let (bytes, _permit) = bounded_body(&node, request.into_body(), 4096).await?;
        let expected: Option<crate::authority::Proof> =
            serde_json::from_slice(&bytes).map_err(|_| http(400, "invalid authority"))?;
        let proof = service.check().await?;
        if expected.is_some() && proof != expected {
            return Err(http(409, "obsolete control-plane authority"));
        }
        return Ok(json_response(200, json!(proof)));
    }
    let ip = request_ip(&node, remote, request.headers());
    let origin = request_origin(&node, request.headers());
    if let Ok(ws) = ws {
        service.check().await?;
        let ws = ws
            .read_buffer_size(16 << 10)
            .write_buffer_size(0)
            .max_write_buffer_size(17 << 20)
            .max_message_size(1 << 20)
            .max_frame_size(1 << 20);
        if path == "/api/agent" {
            if !bearer(&node, request.headers()) {
                return Err(http(401, "unauthorized"));
            }
            let headers = request.headers().clone();
            return Ok(ws.on_upgrade(move |socket| {
                agent_socket(node, service, socket, headers, origin, ip)
            }));
        }
        if let Some(id) = path.strip_prefix("/admin/api/logs/") {
            if uuid::Uuid::parse_str(id).is_err() {
                return Err(http(404, "no such session"));
            }
            if request.headers().contains_key("origin")
                && header(request.headers(), "origin") != origin
            {
                return Err(http(403, "cross-site request refused"));
            }
            let id = id.to_owned();
            let (session, _) = service.attach_log(id.clone(), None).await?;
            let (relay, rx) = service.hub.add_viewer(&id)?;
            let failed = service.clone();
            let failed_id = id.clone();
            return Ok(ws
                .max_message_size(4096)
                .max_frame_size(4096)
                .on_failed_upgrade(move |_| failed.end_log(&failed_id))
                .on_upgrade(move |socket| log_viewer(service, socket, id, session, relay, rx)));
        }
        if path == "/api/logs/stream" {
            if !bearer(&node, request.headers()) {
                return Err(http(401, "unauthorized"));
            }
            let id = query.get("session").cloned().unwrap_or_default();
            let run = query.get("run").cloned().unwrap_or_default();
            let (session, secrets) = service.attach_log(id.clone(), Some(run)).await?;
            let relay = service
                .hub
                .logs
                .lock()
                .unwrap()
                .get(&id)
                .cloned()
                .ok_or_else(|| http(404, "no log viewer"))?;
            let failed = service.clone();
            let failed_id = id.clone();
            return Ok(ws
                .max_message_size(64 << 10)
                .max_frame_size(64 << 10)
                .on_failed_upgrade(move |_| failed.end_log(&failed_id))
                .on_upgrade(move |socket| {
                    log_agent(service, socket, id, session, secrets, relay)
                }));
        }
        return Err(http(404, "not found"));
    }
    if path == "/internal/dns" && request.method() == Method::GET {
        let output = service
            .request(api::Request {
                method: Method::GET,
                path,
                query,
                headers: request.headers().clone(),
                origin,
                ip,
                body: bytes::Bytes::new(),
                permit: None,
            })
            .await?;
        return output_response(service, output).await;
    }
    let (parts, body) = request.into_parts();
    let (bytes, permit) = bounded_body(&node, body, 1_000_000).await?;
    let output = service
        .request(api::Request {
            method: parts.method,
            path,
            query,
            headers: parts.headers,
            origin,
            ip,
            body: bytes,
            permit: Some(permit),
        })
        .await?;
    output_response(service, output).await
}
async fn output_response(service: Service, output: Output) -> anyhow::Result<Response> {
    match output {
        Output::Json(value) => Ok(json_response(200, value)),
        Output::Bytes(bytes) => Ok(text_response(200, "application/json", bytes)),
        Output::Restart => Ok(json_response(200, json!({"restarting":true}))),
        Output::Wake(run) => {
            let mut subscription = service.hub.subscribe(&run, false)?;
            let wake = tokio::select! {_=subscription.cancel.cancelled()=>false,r=subscription.wake.changed()=>r.is_ok(),_=tokio::time::sleep(Duration::from_secs(45))=>false};
            service.check().await?;
            Ok(json_response(200, json!({"wake":wake})))
        }
    }
}
async fn send(socket: &mut WebSocket, value: Value) -> anyhow::Result<()> {
    tokio::time::timeout(
        Duration::from_secs(5),
        socket.send(Message::Text(serde_json::to_string(&value)?.into())),
    )
    .await??;
    Ok(())
}
#[derive(Deserialize)]
struct Envelope<'a> {
    #[serde(rename = "type")]
    kind: &'a str,
    id: u64,
    #[serde(borrow)]
    body: &'a serde_json::value::RawValue,
}
#[derive(Deserialize)]
struct Identity {
    run: String,
}
async fn agent_socket(
    node: Arc<Node>,
    service: Service,
    mut socket: WebSocket,
    headers: HeaderMap,
    origin: String,
    ip: String,
) {
    let mut subscription: Option<crate::hub::Subscription> = None;
    let mut ping = tokio::time::interval(Duration::from_secs(25));
    ping.tick().await;
    let outcome=async{
    loop {let connection_cancel=subscription.as_ref().map(|s|s.cancel.clone());let message=tokio::select!{_=service.cancel.cancelled()=>break,_=async{match &connection_cancel{Some(cancel)=>cancel.cancelled().await,None=>std::future::pending().await}}=>break,_=async{match &mut subscription{Some(s)=>{let _=s.wake.changed().await;},None=>std::future::pending().await}}=>{send(&mut socket,json!({"type":"wake"})).await?;continue;},_=ping.tick()=>{service.check().await?;tokio::time::timeout(Duration::from_secs(5),socket.send(Message::Ping(bytes::Bytes::new()))).await??;continue;},message=socket.next()=>message};
        let Some(message)=message else{break;};let message=message?;let raw=match message{Message::Text(text)=>text,Message::Close(_)=>break,Message::Ping(_)|Message::Pong(_)=>continue,_=>{send(&mut socket,json!({"type":"error","error":"text reports required"})).await?;continue;}};
        let envelope=serde_json::from_str::<Envelope<'_>>(&raw);let envelope=match envelope{Ok(e) if e.kind=="sync"&&e.id>0&&e.id<=9_007_199_254_740_991=>e,_=>{send(&mut socket,json!({"type":"error","error":"invalid report"})).await?;continue;}};let id=envelope.id;let identity:Identity=serde_json::from_str(envelope.body.get())?;if identity.run.is_empty()||identity.run.len()>200{break;}if subscription.as_ref().is_some_and(|s|s.identity()!=identity.run){break;}
        let permit=match node.bodies.clone().try_acquire_owned(){Ok(p)=>p,Err(_)=>{send(&mut socket,json!({"type":"error","id":id,"error":"controller busy; retry shortly"})).await?;continue;}};let body=bytes::Bytes::copy_from_slice(envelope.body.get().as_bytes());drop(raw);
        let result=service.request(api::Request{method:Method::POST,path:"/api/sync".into(),query:BTreeMap::new(),headers:headers.clone(),origin:origin.clone(),ip:ip.clone(),body,permit:Some(permit)}).await;
        match result{Ok(Output::Json(plan))=>{if subscription.is_none(){subscription=Some(service.hub.subscribe(&identity.run,true)?);}send(&mut socket,Value::Object(serde_json::Map::from_iter([("type".into(),json!("plan")),("id".into(),json!(id)),("plan".into(),plan)]))).await?;},Err(error)=>{let message=error.downcast_ref::<HttpError>().map(|e|e.message.as_str()).unwrap_or("scheduler unavailable");send(&mut socket,json!({"type":"error","id":id,"error":message})).await?;},_=>break}
    }Ok::<_,anyhow::Error>(())}.await;
    let _ = outcome;
    let _ = tokio::time::timeout(Duration::from_secs(1), socket.close()).await;
}
async fn log_viewer(
    service: Service,
    mut socket: WebSocket,
    id: String,
    session: Value,
    relay: Arc<crate::hub::Relay>,
    mut rx: tokio::sync::mpsc::Receiver<crate::hub::LogFrame>,
) {
    let outcome=async{send(&mut socket,json!({"t":"info","text":format!("asking server {} to stream {} replica {}",n(&session,"machine"),s(&session,"name"),n(&session,"replica"))})).await?;let mut ping=tokio::time::interval(Duration::from_secs(25));ping.tick().await;loop{tokio::select!{_=relay.cancel.cancelled()=>break,message=socket.next()=>match message{Some(Ok(Message::Close(_)))|None|Some(Err(_))=>break,_=>{}},frame=rx.recv()=>match frame{Some(frame)=>tokio::time::timeout(Duration::from_secs(5),socket.send(Message::Text(frame.text.into()))).await??,None=>break},_=ping.tick()=>{service.check().await?;tokio::time::timeout(Duration::from_secs(5),socket.send(Message::Ping(bytes::Bytes::new()))).await??;}}}Ok::<_,anyhow::Error>(())}.await;
    let _ = outcome;
    service.end_log(&id);
    let _ = tokio::time::timeout(Duration::from_secs(1), socket.close()).await;
}
async fn log_agent(
    service: Service,
    mut socket: WebSocket,
    id: String,
    session: Value,
    secrets: Vec<String>,
    relay: Arc<crate::hub::Relay>,
) {
    service.hub.log(
        &id,
        json!({"t":"open","text":format!("streaming from server {}",n(&session,"machine"))})
            .to_string(),
    );
    let mut ping = tokio::time::interval(Duration::from_secs(25));
    ping.tick().await;
    loop {
        tokio::select! {_=relay.cancel.cancelled()=>break,message=socket.next()=>match message{Some(Ok(Message::Text(text)))=>{let mut text=text.to_string();for secret in &secrets{text=text.replace(secret,"[hidden]");}if !service.hub.log(&id,json!({"t":"log","text":text}).to_string()){break;}},Some(Ok(Message::Close(_)))|None|Some(Err(_))=>break,_=>{}},_=ping.tick()=>{if service.check().await.is_err()||tokio::time::timeout(Duration::from_secs(5),socket.send(Message::Ping(bytes::Bytes::new()))).await.is_err(){break;}}}
    }
    service.end_log(&id);
    let _ = tokio::time::timeout(Duration::from_secs(1), socket.close()).await;
}

fn trusted_peer(node: &Node, headers: &HeaderMap) -> bool {
    headers.contains_key("x-cp-forwarded")
        && runner_core::auth::equal_secret(
            header(headers, "x-cp-token"),
            node.e("CP_CLUSTER_TOKEN"),
        )
}
fn request_origin(node: &Node, headers: &HeaderMap) -> String {
    if trusted_peer(node, headers) {
        let value = header(headers, "x-cp-proxy-origin");
        if url::Url::parse(value).is_ok_and(|u| {
            matches!(u.scheme(), "http" | "https")
                && u.host_str().is_some()
                && u.path() == "/"
                && u.username().is_empty()
                && u.password().is_none()
                && u.query().is_none()
                && u.fragment().is_none()
        }) {
            return value.into();
        }
    }
    let host = header(headers, "host");
    let proto = if header(headers, "x-forwarded-proto") == "https" {
        "https"
    } else {
        "http"
    };
    format!("{proto}://{host}")
}
fn request_ip(node: &Node, remote: SocketAddr, headers: &HeaderMap) -> String {
    if trusted_peer(node, headers) {
        if let Ok(ip) = header(headers, "x-cp-client-ip").parse::<std::net::IpAddr>() {
            return ip.to_string();
        }
    }
    node.proxy.address(remote.ip(), headers)
}
fn forwarded_headers(
    headers: &HeaderMap,
    node: &Node,
    target: &url::Url,
    remote: SocketAddr,
) -> HeaderMap {
    let mut out = headers.clone();
    let origin = request_origin(node, headers);
    let ip = request_ip(node, remote, headers);
    out.insert("x-cp-proxy-origin", origin.parse().unwrap());
    out.insert("x-cp-client-ip", ip.parse().unwrap());
    out.insert("x-cp-token", node.e("CP_CLUSTER_TOKEN").parse().unwrap());
    for name in [
        "host",
        "connection",
        "keep-alive",
        "proxy-authenticate",
        "proxy-authorization",
        "te",
        "trailer",
        "transfer-encoding",
        "upgrade",
    ] {
        out.remove(name);
    }
    if let Some(host) = target.host_str() {
        let host = target
            .port()
            .map(|p| format!("{host}:{p}"))
            .unwrap_or_else(|| host.into());
        if let Ok(value) = host.parse() {
            out.insert("host", value);
        }
    }
    out.insert(
        "x-cp-forwarded",
        node.config
            .as_ref()
            .map(|c| c.id)
            .unwrap_or(1)
            .to_string()
            .parse()
            .unwrap(),
    );
    out
}
async fn proxy_http(
    node: Arc<Node>,
    request: Request,
    target: String,
    remote: SocketAddr,
) -> anyhow::Result<Response> {
    let uri = request
        .uri()
        .path_and_query()
        .map(|u| u.as_str())
        .unwrap_or("/");
    let target = url::Url::parse(&target)?;
    let url = target.join(uri)?;
    let headers = forwarded_headers(request.headers(), &node, &target, remote);
    let method = request.method().clone();
    let (body, _permit) = bounded_body(&node, request.into_body(), 1_000_000).await?;
    let incoming = node
        .client
        .request(method, url)
        .headers(headers)
        .body(body)
        .timeout(Duration::from_secs(65))
        .send()
        .await
        .map_err(|_| http(503, "primary unavailable; retry shortly"))?;
    let status = incoming.status();
    let mut headers = incoming.headers().clone();
    for key in ["connection", "keep-alive", "transfer-encoding", "upgrade"] {
        headers.remove(key);
    }
    let mut response = Response::new(Body::from_stream(incoming.bytes_stream()));
    *response.status_mut() = status;
    *response.headers_mut() = headers;
    Ok(response)
}
async fn proxy_socket(
    node: Arc<Node>,
    ws: WebSocketUpgrade,
    request: Request,
    target: String,
    remote: SocketAddr,
) -> anyhow::Result<Response> {
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;
    let uri = request
        .uri()
        .path_and_query()
        .map(|u| u.as_str())
        .unwrap_or("/");
    let base = url::Url::parse(&target)?;
    let mut url = base.join(uri)?;
    url.set_scheme(if base.scheme() == "https" {
        "wss"
    } else {
        "ws"
    })
    .map_err(|_| http(500, "invalid peer URL"))?;
    let mut outgoing = url.as_str().into_client_request()?;
    for (key, value) in forwarded_headers(request.headers(), &node, &base, remote) {
        if let Some(key) = key {
            if !key.as_str().starts_with("sec-websocket-") {
                outgoing.headers_mut().insert(key, value);
            }
        }
    }
    let config = tokio_tungstenite::tungstenite::protocol::WebSocketConfig::default()
        .read_buffer_size(16 << 10)
        .write_buffer_size(0)
        .max_write_buffer_size(17 << 20)
        .max_message_size(Some(16_000_000))
        .max_frame_size(Some(16_000_000));
    let (upstream, _) = tokio::time::timeout(
        Duration::from_secs(10),
        tokio_tungstenite::connect_async_with_config(outgoing, Some(config), true),
    )
    .await
    .map_err(|_| http(503, "primary unavailable"))?
    .map_err(|_| http(503, "primary unavailable"))?;
    Ok(ws
        .read_buffer_size(16 << 10)
        .write_buffer_size(0)
        .max_write_buffer_size(17 << 20)
        .max_message_size(1 << 20)
        .max_frame_size(1 << 20)
        .on_upgrade(move |socket| async move {
            use tokio_tungstenite::tungstenite::Message as M;
            let (mut upstream_write, mut upstream_read) = upstream.split();
            let (mut write, mut read) = socket.split();
            let outgoing = async {
                while let Some(Ok(m)) = read.next().await {
                    let m = match m {
                        Message::Text(s) => M::Text(s.to_string().into()),
                        Message::Binary(b) => M::Binary(b),
                        Message::Ping(b) => M::Ping(b),
                        Message::Pong(b) => M::Pong(b),
                        Message::Close(_) => M::Close(None),
                    };
                    tokio::time::timeout(Duration::from_secs(5), upstream_write.send(m)).await??;
                }
                Ok::<_, anyhow::Error>(())
            };
            let incoming = async {
                while let Some(Ok(m)) = upstream_read.next().await {
                    let m = match m {
                        M::Text(s) => Message::Text(s.to_string().into()),
                        M::Binary(b) => Message::Binary(b),
                        M::Ping(b) => Message::Ping(b),
                        M::Pong(b) => Message::Pong(b),
                        M::Close(_) => Message::Close(None),
                        _ => continue,
                    };
                    tokio::time::timeout(Duration::from_secs(5), write.send(m)).await??;
                }
                Ok::<_, anyhow::Error>(())
            };
            tokio::select! {_=node.cancel.cancelled()=>{},_=outgoing=>{},_=incoming=>{}}
        }))
}
