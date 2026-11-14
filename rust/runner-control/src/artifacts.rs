use crate::{error::http, node::Node};
use axum::{body::Body, response::Response};
use std::path::{Component, Path};
pub async fn serve(node: &Node, path: &str) -> anyhow::Result<Response> {
    if path == "/install.sh" {
        let quote = |s: &str| format!("'{}'", s.replace('\'', "'\\''"));
        let origin = if node.e("PUBLIC_URL").is_empty() {
            format!("https://{}", node.e("CONTROL_HOST"))
        } else {
            node.e("PUBLIC_URL").into()
        };
        let agent = if node.e("AGENT_URL").is_empty() {
            format!("{origin}/agent")
        } else {
            node.e("AGENT_URL").into()
        };
        let script = include_str!("../../install-agent.sh")
            .replace("@ORIGIN@", &quote(&origin))
            .replace("@AGENT_URL@", &quote(&agent));
        return Ok(Response::builder()
            .status(200)
            .header("content-type", "text/plain; charset=utf-8")
            .header("cache-control", "no-store")
            .body(Body::from(script))?);
    }
    let relative = if path == "/install.sh" {
        "install.sh"
    } else {
        path.strip_prefix("/agent/")
            .ok_or_else(|| http(404, "not found"))?
    };
    let allowed = relative == "install.sh"
        || relative.ends_with(".mjs")
            && relative
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || matches!(b, b'-' | b'.'))
        || [
            "bundle.json",
            "manifest.json",
            "bundle.tar",
            "bundle.sha256",
            "rust/bundle.tar",
            "rust/bundle.sha256",
            "rust/manifest.json",
        ]
        .contains(&relative)
        || relative.strip_prefix("rust/").is_some_and(|p| {
            p.starts_with("runner-agent-linux-")
                && p.bytes()
                    .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.'))
        });
    if !allowed
        || !Path::new(relative)
            .components()
            .all(|c| matches!(c, Component::Normal(_)))
    {
        return Err(http(404, "agent asset unavailable"));
    }
    let root = if node.e("AGENT_ASSET_DIR").is_empty() {
        "/usr/local/share/runner-agent"
    } else {
        node.e("AGENT_ASSET_DIR")
    };
    let file = tokio::fs::File::open(Path::new(root).join(relative))
        .await
        .map_err(|_| http(404, "agent asset unavailable"))?;
    let size = file.metadata().await?.len();
    anyhow::ensure!(size <= 64 << 20, "agent artifact too large");
    let content = if relative.ends_with(".json") {
        "application/json"
    } else if relative.ends_with(".mjs") {
        "text/javascript; charset=utf-8"
    } else if relative.ends_with(".tar") {
        "application/x-tar"
    } else if relative.ends_with(".sh") || relative.ends_with(".sha256") {
        "text/plain; charset=utf-8"
    } else {
        "application/octet-stream"
    };
    Ok(Response::builder()
        .status(200)
        .header("content-type", content)
        .header("content-length", size)
        .header("cache-control", "no-cache")
        .body(Body::from_stream(
            tokio_util::io::ReaderStream::with_capacity(file, 16 << 10),
        ))?)
}
