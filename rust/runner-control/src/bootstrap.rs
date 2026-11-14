use crate::{
    authority::{Authority, Proof},
    control::s,
    node::Node,
    storage::Store,
};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde_json::{Value, json};
use std::{collections::BTreeMap, path::Path, sync::Arc, time::Duration};
fn host(value: &str) -> anyhow::Result<String> {
    let value = value.trim_end_matches('.').to_lowercase();
    anyhow::ensure!(
        value.len() <= 253
            && value.contains('.')
            && value.split('.').all(|s| !s.is_empty()
                && s.len() <= 63
                && s.as_bytes()[0].is_ascii_alphanumeric()
                && s.as_bytes()[s.len() - 1].is_ascii_alphanumeric()
                && s.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-')),
        "invalid DNS hostname"
    );
    Ok(value)
}
fn get<'a>(env: &'a BTreeMap<String, String>, key: &str) -> &'a str {
    env.get(key).map(String::as_str).unwrap_or("")
}
pub fn settings(mut env: BTreeMap<String, String>) -> anyhow::Result<BTreeMap<String, String>> {
    let count = env
        .get("CP_REPLICAS")
        .map(|s| s.parse::<usize>())
        .transpose()?
        .unwrap_or(1);
    anyhow::ensure!(
        (count == 1 || count >= 3 && count % 2 == 1) && count <= 51,
        "CP_REPLICAS must be 1 or an odd number from 3 to 51"
    );
    env.insert("CP_REPLICAS".into(), count.to_string());
    if count > 1 && get(&env, "CP_NODE_ID").is_empty() {
        let offset = get(&env, "CP_NODE_ID_OFFSET").parse::<u64>()?;
        let replica = get(&env, "FLEET_REPLICA").parse::<u64>()?;
        anyhow::ensure!(replica > 0, "FLEET_REPLICA must be positive");
        env.insert(
            "CP_NODE_ID".into(),
            offset
                .checked_add(replica)
                .ok_or_else(|| anyhow::anyhow!("invalid CP_NODE_ID_OFFSET"))?
                .to_string(),
        );
    }
    let domain = host(get(&env, "DOMAIN"))?;
    env.insert("DOMAIN".into(), domain.clone());
    let control = host(if get(&env, "CONTROL_HOST").is_empty() {
        env.insert("CONTROL_HOST".into(), format!("runners.{domain}"));
        get(&env, "CONTROL_HOST")
    } else {
        get(&env, "CONTROL_HOST")
    })?;
    anyhow::ensure!(
        control.ends_with(&format!(".{domain}")),
        "CONTROL_HOST must be under DOMAIN"
    );
    env.insert("CONTROL_HOST".into(), control.clone());
    let public = if get(&env, "PUBLIC_URL").is_empty() {
        format!("https://{control}")
    } else {
        get(&env, "PUBLIC_URL").to_owned()
    };
    let url = url::Url::parse(&public)?;
    anyhow::ensure!(
        matches!(url.scheme(), "http" | "https")
            && url.host_str().is_some()
            && url.username().is_empty()
            && url.password().is_none()
            && url.path() == "/"
            && url.query().is_none()
            && url.fragment().is_none(),
        "PUBLIC_URL must be an HTTP(S) origin"
    );
    env.insert("PUBLIC_URL".into(), url.origin().ascii_serialization());
    for (key, value) in [
        ("PORT", "8920"),
        ("TUNNEL_METRICS_PORT", "49312"),
        ("DATA_DIR", "/data"),
        ("RUNTIME_DIR", "/run/runner"),
        ("BIND_HOST", "0.0.0.0"),
        ("CONTROLLER_NAME", &control),
        ("MEMORY_HISTORY_MIN", "360"),
        ("CONFIG_STORAGE", "sqlite"),
    ] {
        if get(&env, key).is_empty() {
            env.insert(key.into(), value.into());
        }
    }
    for key in ["PORT", "TUNNEL_METRICS_PORT"] {
        anyhow::ensure!(
            get(&env, key).parse::<u16>().is_ok_and(|n| n > 0),
            "invalid {key}"
        );
    }
    anyhow::ensure!(
        env["PORT"] != env["TUNNEL_METRICS_PORT"],
        "tunnel metrics and HTTP ports must differ"
    );
    for (key, default) in [
        ("REQUIRE_DEPLOY_AUTH", "1"),
        ("AUTO_TUNNEL", "1"),
        ("CP_PEER_TUNNEL", "1"),
        ("RECOVER_RUNTIME", "1"),
        ("RECOVER_TUNNELS", "1"),
    ] {
        let v = match env.get(key).map(String::as_str) {
            None => default,
            Some("1" | "true") => "1",
            Some("0" | "false") => "0",
            _ => anyhow::bail!("{key} must be 1/true or 0/false"),
        };
        env.insert(key.into(), v.into());
    }
    anyhow::ensure!(
        ["d1", "sqlite"].contains(&get(&env, "CONFIG_STORAGE")),
        "invalid CONFIG_STORAGE"
    );
    anyhow::ensure!(
        count == 1 || get(&env, "CONFIG_STORAGE") == "d1",
        "replicated controllers require CONFIG_STORAGE=d1"
    );
    env.remove("INIT_CONTROL_DB");
    Ok(env)
}
pub fn identity(
    mut env: BTreeMap<String, String>,
    required: bool,
    persist: bool,
) -> anyhow::Result<BTreeMap<String, String>> {
    let dir = runner_core::fs::private_directory(Path::new(&env["DATA_DIR"]))?;
    let path = dir.join("identity.json");
    let mut saved = if path.exists() {
        anyhow::ensure!(
            !std::fs::symlink_metadata(&path)?.file_type().is_symlink(),
            "identity.json cannot be a symlink"
        );
        let metadata = std::fs::metadata(&path)?;
        anyhow::ensure!(metadata.len() <= 64 << 10, "invalid identity.json");
        serde_json::from_slice::<Value>(&std::fs::read(&path)?)?
    } else {
        json!({})
    };
    anyhow::ensure!(saved.is_object(), "identity.json must be an object");
    let suffix = runner_core::auth::sha256(&env["CONTROL_HOST"]);
    let mut changed = false;
    for (key, default, max) in [
        ("TUNNEL_PREFIX", format!("runner-{}", &suffix[..12]), 63),
        (
            "CONTROLLER_TUNNEL_NAME",
            format!("control-{}", &suffix[..12]),
            100,
        ),
    ] {
        let previous = saved
            .get(key)
            .map(|v| {
                v.as_str()
                    .filter(|s| !s.is_empty())
                    .ok_or_else(|| anyhow::anyhow!("invalid saved namespace"))
            })
            .transpose()?;
        let supplied = env.get(key).filter(|s| !s.is_empty()).map(String::as_str);
        anyhow::ensure!(
            previous.is_none() || supplied.is_none() || previous == supplied,
            "{key} conflicts with saved fleet identity"
        );
        let name = previous.or(supplied).unwrap_or(&default).to_owned();
        anyhow::ensure!(
            name.len() <= max
                && name.as_bytes()[0].is_ascii_alphanumeric()
                && name
                    .bytes()
                    .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == b'-'),
            "invalid {key}"
        );
        if saved[key] != name {
            changed = true;
            saved[key] = json!(name);
        }
        env.insert(key.into(), name);
    }
    let overlap = env["CONTROLLER_TUNNEL_NAME"].strip_prefix(&format!("{}-", env["TUNNEL_PREFIX"]));
    anyhow::ensure!(
        !overlap.is_some_and(|s| !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit())),
        "controller and agent tunnel names overlap"
    );
    for key in ["ADMIN_PASSWORD", "JOIN_TOKEN", "STATE_SIGNING_KEY"] {
        if get(&env, key).is_empty() {
            if let Some(secret) = saved[key].as_str() {
                env.insert(key.into(), secret.into());
            }
        }
        anyhow::ensure!(
            !required || !get(&env, key).is_empty(),
            "{key} is missing; restore fleet identity or initialize explicitly"
        );
    }
    if persist && changed {
        runner_core::fs::atomic_json(&path, &saved)?;
    }
    Ok(env)
}
pub async fn initialize(env: BTreeMap<String, String>) -> anyhow::Result<()> {
    anyhow::ensure!(
        get(&env, "CONFIG_STORAGE") != "d1",
        "initialize D1 by explicitly migrating an initialized local fleet"
    );
    let dir = Path::new(&env["DATA_DIR"]);
    anyhow::ensure!(
        !dir.join("control.db").exists() && !dir.join("identity.json").exists(),
        "fleet data exists; refusing to overwrite it"
    );
    let mut env = identity(env, false, false)?;
    let mut saved = json!({});
    for key in ["ADMIN_PASSWORD", "JOIN_TOKEN", "STATE_SIGNING_KEY"] {
        if get(&env, key).is_empty() {
            env.insert(
                key.into(),
                format!("{}{}", runner_core::random_id(), runner_core::random_id()),
            );
        }
        saved[key] = json!(env[key]);
    }
    for key in ["TUNNEL_PREFIX", "CONTROLLER_TUNNEL_NAME"] {
        saved[key] = json!(env[key]);
    }
    runner_core::fs::atomic_json(&Path::new(&env["DATA_DIR"]).join("identity.json"), &saved)?;
    env.insert("INIT_CONTROL_DB".into(), "1".into());
    let _store = Store::open(&env).await?;
    Ok(())
}

#[derive(Clone)]
struct Cloud {
    env: Arc<BTreeMap<String, String>>,
    client: reqwest::Client,
}
impl Cloud {
    fn new(env: Arc<BTreeMap<String, String>>) -> anyhow::Result<Self> {
        Ok(Self {
            env,
            client: runner_core::http_client(Duration::from_secs(30))?,
        })
    }
    async fn call(
        &self,
        path: &str,
        method: reqwest::Method,
        body: Option<Value>,
        authority: Option<&(Authority, Proof)>,
    ) -> anyhow::Result<Value> {
        let mut req = self.client.request(
            method.clone(),
            format!(
                "{}{}",
                self.env
                    .get("CF_API_BASE")
                    .map(String::as_str)
                    .unwrap_or("https://api.cloudflare.com/client/v4")
                    .trim_end_matches('/'),
                path
            ),
        );
        if !get(&self.env, "CF_API_TOKEN").is_empty() {
            req = req.bearer_auth(&self.env["CF_API_TOKEN"]);
        } else {
            anyhow::ensure!(
                !get(&self.env, "CF_API_KEY").is_empty()
                    && !get(&self.env, "CF_API_EMAIL").is_empty(),
                "Cloudflare credentials unavailable"
            );
            req = req
                .header("x-auth-key", &self.env["CF_API_KEY"])
                .header("x-auth-email", &self.env["CF_API_EMAIL"]);
        }
        if let Some(body) = body {
            req = req.json(&body);
        }
        if method != reqwest::Method::GET {
            if let Some((authority, proof)) = authority {
                authority.check(Some(proof)).await?;
            }
        }
        let response = req
            .send()
            .await
            .map_err(|_| anyhow::anyhow!("Cloudflare request unavailable"))?;
        let status = response.status();
        let body: Value = runner_core::response_json(response, 4 << 20)
            .await
            .map_err(|_| anyhow::anyhow!("Cloudflare request failed ({})", status.as_u16()))?;
        anyhow::ensure!(body["success"] == true, "Cloudflare operation rejected");
        Ok(body["result"].clone())
    }
    async fn tunnel(&self, name: &str) -> anyhow::Result<(String, String)> {
        let account = format!("/accounts/{}/cfd_tunnel", self.env["ACCOUNT_ID"]);
        let query = url::form_urlencoded::Serializer::new(String::new())
            .append_pair("name", name)
            .append_pair("is_deleted", "false")
            .finish();
        let found = self
            .call(
                &format!("{account}?{query}"),
                reqwest::Method::GET,
                None,
                None,
            )
            .await?;
        let matches = found
            .as_array()
            .ok_or_else(|| anyhow::anyhow!("invalid tunnel inventory"))?
            .iter()
            .filter(|t| t["name"] == name && t["deleted_at"].is_null())
            .collect::<Vec<_>>();
        anyhow::ensure!(
            matches.len() <= 1,
            "multiple tunnels share the configured name"
        );
        let tunnel = match matches.first() {
            Some(t) => (*t).clone(),
            None => {
                let bytes = [
                    uuid::Uuid::new_v4().as_bytes().as_slice(),
                    uuid::Uuid::new_v4().as_bytes().as_slice(),
                ]
                .concat();
                self.call(&account,reqwest::Method::POST,Some(json!({"name":name,"config_src":"local","tunnel_secret":STANDARD.encode(bytes)})),None).await?
            }
        };
        let id = s(&tunnel, "id");
        anyhow::ensure!(
            !id.is_empty() && (tunnel["config_src"].is_null() || tunnel["config_src"] == "local"),
            "controller tunnel must be locally managed"
        );
        let token = self
            .call(
                &format!("{account}/{id}/token"),
                reqwest::Method::GET,
                None,
                None,
            )
            .await?;
        let token = token
            .as_str()
            .filter(|s| !s.is_empty())
            .ok_or_else(|| anyhow::anyhow!("tunnel has no token"))?;
        Ok((id.into(), token.into()))
    }
    async fn route(
        &self,
        hostname: &str,
        tunnel: &str,
        comment: &str,
        authority: Option<&(Authority, Proof)>,
    ) -> anyhow::Result<()> {
        let path = format!("/zones/{}/dns_records", self.env["ZONE"]);
        let query = url::form_urlencoded::Serializer::new(String::new())
            .append_pair("name", hostname)
            .append_pair("per_page", "100")
            .finish();
        let records = self
            .call(&format!("{path}?{query}"), reqwest::Method::GET, None, None)
            .await?;
        let records = records
            .as_array()
            .ok_or_else(|| anyhow::anyhow!("invalid DNS inventory"))?;
        let content = format!("{tunnel}.cfargotunnel.com");
        anyhow::ensure!(
            records.len() <= 1 && records.iter().all(|r| r["type"] == "CNAME"),
            "hostname belongs to another DNS record"
        );
        if records
            .iter()
            .any(|r| r["content"] != content && r["comment"] != comment)
        {
            anyhow::ensure!(
                hostname == get(&self.env, "CONTROL_HOST")
                    && comment == format!("runner control {hostname}"),
                "hostname belongs to another DNS record"
            );
            let name = get(&self.env, "CONTROLLER_TUNNEL_NAME");
            let query = url::form_urlencoded::Serializer::new(String::new())
                .append_pair("name", name)
                .append_pair("is_deleted", "false")
                .finish();
            let previous = self
                .call(
                    &format!("/accounts/{}/cfd_tunnel?{query}", self.env["ACCOUNT_ID"]),
                    reqwest::Method::GET,
                    None,
                    None,
                )
                .await?;
            let previous = previous
                .as_array()
                .into_iter()
                .flatten()
                .filter(|t| t["name"] == name && t["deleted_at"].is_null())
                .collect::<Vec<_>>();
            anyhow::ensure!(
                previous.len() == 1
                    && records[0]["content"]
                        == format!("{}.cfargotunnel.com", s(previous[0], "id")),
                "hostname belongs to another DNS record"
            );
        }
        let record = json!({"type":"CNAME","name":hostname,"content":content,"proxied":true,"comment":comment});
        if records.is_empty() {
            self.call(&path, reqwest::Method::POST, Some(record), authority)
                .await?;
        } else if records[0]["content"] != content || records[0]["proxied"] != true {
            self.call(
                &format!("{path}/{}", s(&records[0], "id")),
                reqwest::Method::PATCH,
                Some(record),
                authority,
            )
            .await?;
        }
        Ok(())
    }
}
pub async fn resolve(
    mut env: BTreeMap<String, String>,
) -> anyhow::Result<BTreeMap<String, String>> {
    if !get(&env, "ACCOUNT_ID").is_empty() && !get(&env, "ZONE").is_empty() {
        return Ok(env);
    }
    if get(&env, "AUTO_TUNNEL") == "0"
        && get(&env, "RECOVER_TUNNELS") == "0"
        && get(&env, "DNS") == "off"
    {
        return Ok(env);
    }
    let cloud = Cloud::new(Arc::new(env.clone()))?;
    let query = url::form_urlencoded::Serializer::new(String::new())
        .append_pair("name", &env["DOMAIN"])
        .append_pair("status", "active")
        .append_pair("per_page", "50")
        .finish();
    let zones = cloud
        .call(&format!("/zones?{query}"), reqwest::Method::GET, None, None)
        .await?;
    let matches = zones
        .as_array()
        .into_iter()
        .flatten()
        .filter(|z| {
            z["name"] == env["DOMAIN"]
                && (get(&env, "ACCOUNT_ID").is_empty() || z["account"]["id"] == env["ACCOUNT_ID"])
        })
        .collect::<Vec<_>>();
    anyhow::ensure!(
        matches.len() == 1,
        "DOMAIN must identify one active Cloudflare zone"
    );
    anyhow::ensure!(
        get(&env, "ZONE").is_empty() || matches[0]["id"] == env["ZONE"],
        "ZONE does not match DOMAIN"
    );
    env.insert("ZONE".into(), s(matches[0], "id").into());
    env.insert("ACCOUNT_ID".into(), s(&matches[0]["account"], "id").into());
    Ok(env)
}
pub async fn connector(node: Arc<Node>) -> anyhow::Result<()> {
    if node.e("AUTO_TUNNEL") == "0" {
        return Ok(());
    }
    let cloud = Cloud::new(node.env.clone())?;
    let name = if let Some(c) = &node.config {
        format!("{}-cp-{}", node.e("CONTROLLER_TUNNEL_NAME"), c.id)
    } else {
        node.e("CONTROLLER_TUNNEL_NAME").into()
    };
    let (id, token) = loop {
        let result = tokio::select! {_=node.cancel.cancelled()=>return Ok(()),result=cloud.tunnel(&name)=>result};
        match result {
            Ok(tunnel) => break tunnel,
            Err(_) => {
                *node.error.lock().unwrap() = Some("replica tunnel unavailable".into());
                tokio::select! {_=node.cancel.cancelled()=>return Ok(()),_=tokio::time::sleep(Duration::from_secs(10))=>{}}
            }
        }
    };
    let mut hosts = vec![node.e("CONTROL_HOST").to_owned()];
    if let Some(c) = &node.config {
        let hostname = url::Url::parse(&c.members.iter().find(|m| m.id == c.id).unwrap().url)?
            .host_str()
            .ok_or_else(|| anyhow::anyhow!("invalid peer hostname"))?
            .to_owned();
        anyhow::ensure!(
            hostname != node.e("CONTROL_HOST"),
            "peer origin must differ from the primary hostname"
        );
        if node.e("CP_PEER_TUNNEL") != "0" {
            cloud
                .route(
                    &hostname,
                    &id,
                    &format!("runner peer {} {}", c.cluster, c.id),
                    None,
                )
                .await?;
        }
        hosts.push(hostname);
    } else {
        cloud
            .route(
                node.e("CONTROL_HOST"),
                &id,
                &format!("runner control {}", node.e("CONTROL_HOST")),
                None,
            )
            .await?;
    }
    let dir = runner_core::fs::private_directory(Path::new(node.e("RUNTIME_DIR")))?;
    let file = dir.join("replica-ingress.yml");
    let mut text = "ingress:\n".to_owned();
    hosts.sort();
    hosts.dedup();
    for host in hosts {
        text.push_str(&format!(
            "  - hostname: {}\n    service: http://127.0.0.1:{}\n",
            serde_json::to_string(&host)?,
            node.e("PORT")
        ));
    }
    text.push_str("  - service: http_status:404\n");
    runner_core::fs::atomic_write(&file, text.as_bytes(), 0o600)?;
    let binary = if node.e("CLOUDFLARED_BINARY").is_empty() {
        "cloudflared"
    } else {
        node.e("CLOUDFLARED_BINARY")
    };
    let mut child = tokio::process::Command::new(binary)
        .args(["tunnel", "--no-autoupdate", "--config"])
        .arg(&file)
        .args([
            "--metrics",
            &format!("127.0.0.1:{}", node.e("TUNNEL_METRICS_PORT")),
            "run",
        ])
        .env_clear()
        .env("PATH", std::env::var("PATH").unwrap_or_default())
        .env("TUNNEL_TOKEN", token)
        .stdin(std::process::Stdio::null())
        .kill_on_drop(true)
        .spawn()?;
    node.tunnel_ready
        .store(true, std::sync::atomic::Ordering::Relaxed);
    let mut interval = tokio::time::interval(Duration::from_secs(15));
    loop {
        tokio::select! {_=node.cancel.cancelled()=>break,status=child.wait()=>{let _=status;node.tunnel_ready.store(false,std::sync::atomic::Ordering::Relaxed);node.cancel.cancel();anyhow::bail!("tunnel connector stopped");},_=interval.tick()=>{if let Some(service)=node.service(){if service.authority.is_some(){if cloud.route(node.e("CONTROL_HOST"),&id,&format!("runner control {}",node.e("CONTROL_HOST")),service.authority.as_ref()).await.is_err(){*node.error.lock().unwrap()=Some("primary hostname reconciliation unavailable".into());}}}}}
    }
    node.tunnel_ready
        .store(false, std::sync::atomic::Ordering::Relaxed);
    if let Some(pid) = child.id() {
        unsafe {
            libc::kill(pid as i32, libc::SIGTERM);
        }
    }
    if tokio::time::timeout(Duration::from_secs(6), child.wait())
        .await
        .is_err()
    {
        let _ = child.kill().await;
    }
    Ok(())
}
