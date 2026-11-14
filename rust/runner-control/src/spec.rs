use crate::error::{http, require};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};

pub const MAX_REPLICAS: u32 = 50;
pub fn string(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Null => "null".into(),
        _ => v.to_string(),
    }
}
fn items(v: &Value) -> Vec<&Value> {
    if v.is_null() {
        vec![]
    } else {
        v.as_array().map(|a| a.iter().collect()).unwrap_or(vec![v])
    }
}
pub fn name_valid(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 40
        && name.as_bytes()[0].is_ascii_alphanumeric()
        && name
            .as_bytes()
            .last()
            .is_some_and(u8::is_ascii_alphanumeric)
        && name
            .bytes()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == b'-')
}
pub fn numbered(name: &str) -> bool {
    name.rsplit_once('-').is_some_and(|(_, s)| {
        let s = s.strip_prefix('m').unwrap_or(s);
        !s.is_empty() && s.bytes().all(|c| c.is_ascii_digit())
    })
}
pub fn region_valid(region: &str) -> bool {
    !region.is_empty()
        && region.len() <= 32
        && region.as_bytes()[0].is_ascii_lowercase()
        && region
            .bytes()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == b'-')
}
pub fn tag_valid(tag: &str) -> bool {
    !tag.is_empty()
        && tag.len() <= 30
        && tag.as_bytes()[0].is_ascii_alphanumeric()
        && tag
            .bytes()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == b'-')
}
pub fn join_path(path: &str) -> Option<String> {
    let mut parts = Vec::new();
    for s in path.split('/') {
        match s {
            "" | "." => (),
            ".." => {
                parts.pop()?;
            }
            _ => parts.push(s),
        }
    }
    Some(parts.join("/"))
}
fn inside(path: &Value) -> bool {
    path.as_str().is_some_and(|s| {
        !s.contains('$')
            && !s.starts_with(['/', '~', '\\'])
            && !(s.as_bytes().get(1) == Some(&b':'))
            && join_path(s).is_some()
    })
}
pub fn parse(compose: &str) -> anyhow::Result<Value> {
    let mut doc = runner_core::yaml::parse(compose).map_err(|e| {
        http(
            400,
            format!(
                "the compose file isn't valid YAML: {}",
                e.to_string().lines().next().unwrap_or("invalid YAML")
            ),
        )
    })?;
    doc.apply_merge()
        .map_err(|_| http(400, "the compose file has invalid YAML merge keys"))?;
    serde_json::to_value(doc).map_err(|_| http(400, "compose mappings require string keys"))
}
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Policy {
    pub ready: Option<String>,
    pub spread_tags: bool,
    pub region: Option<String>,
    pub regions: Vec<String>,
}
pub fn policy(compose: &str) -> Policy {
    let doc = parse(compose).unwrap_or(Value::Null);
    let xr = &doc["x-runner"];
    Policy {
        ready: xr["ready"].as_str().map(str::to_owned),
        spread_tags: xr["spread-tags"] == true,
        region: xr["region"]
            .as_str()
            .filter(|s| region_valid(s))
            .map(str::to_owned),
        regions: xr["regions"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|s| s.as_str().filter(|s| region_valid(s)).map(str::to_owned))
            .collect(),
    }
}
pub fn replica_region(policy: &Policy, replica: u32) -> Option<&str> {
    policy.region.as_deref().or_else(|| {
        (!policy.regions.is_empty())
            .then(|| policy.regions[(replica.max(1) as usize - 1) % policy.regions.len()].as_str())
    })
}
pub fn region_of(location: &str) -> Option<&'static str> {
    let before = location.split(", US").next()?;
    if before == location {
        return None;
    }
    let state = before.rsplit(',').next()?.trim().to_ascii_lowercase();
    for (region, states) in [
        (
            "us-west",
            "Alaska|Arizona|California|Colorado|Hawaii|Idaho|Montana|Nevada|New Mexico|Oregon|Utah|Washington|Wyoming",
        ),
        (
            "us-central",
            "Alabama|Arkansas|Illinois|Indiana|Iowa|Kansas|Kentucky|Louisiana|Michigan|Minnesota|Mississippi|Missouri|Nebraska|North Dakota|Ohio|Oklahoma|South Dakota|Tennessee|Texas|Wisconsin",
        ),
        (
            "us-east",
            "Connecticut|Delaware|District of Columbia|Florida|Georgia|Maine|Maryland|Massachusetts|New Hampshire|New Jersey|New York|North Carolina|Pennsylvania|Rhode Island|South Carolina|Vermont|Virginia|West Virginia",
        ),
    ] {
        if states.split('|').any(|s| s.eq_ignore_ascii_case(&state)) {
            return Some(region);
        }
    }
    None
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Bind {
    pub path: String,
    pub read_only: bool,
}
pub fn relative_binds(doc: &Value) -> Vec<Bind> {
    let mut out = Vec::new();
    for s in doc["services"]
        .as_object()
        .into_iter()
        .flat_map(|m| m.values())
    {
        for v in s["volumes"].as_array().into_iter().flatten() {
            let (source, ro) = if let Some(v) = v.as_str() {
                let p = v.split(':').collect::<Vec<_>>();
                if p.len() < 2 || !p[0].starts_with('.') {
                    continue;
                }
                (
                    p[0],
                    p.get(2).is_some_and(|s| s.split(',').any(|s| s == "ro")),
                )
            } else if v["type"] == "bind" {
                (v["source"].as_str().unwrap_or(""), v["read_only"] == true)
            } else {
                continue;
            };
            if source.starts_with('/') || source.contains('$') || source.starts_with('~') {
                continue;
            }
            if let Some(path) = join_path(source) {
                out.push(Bind {
                    path: if path.is_empty() { ".".into() } else { path },
                    read_only: ro,
                });
            }
        }
    }
    out
}
pub fn sandbox_problems(doc: &Value) -> Vec<String> {
    const TOP: &[&str] = &["services", "volumes", "networks", "name", "version"];
    const SERVICE: &[&str] = &[
        "image",
        "build",
        "command",
        "entrypoint",
        "environment",
        "env_file",
        "ports",
        "expose",
        "restart",
        "healthcheck",
        "depends_on",
        "working_dir",
        "user",
        "labels",
        "networks",
        "volumes",
        "tmpfs",
        "init",
        "stop_signal",
        "stop_grace_period",
        "mem_limit",
        "mem_reservation",
        "memswap_limit",
        "cpus",
        "cpu_shares",
        "shm_size",
        "pids_limit",
        "ulimits",
        "read_only",
        "stdin_open",
        "tty",
        "hostname",
        "domainname",
        "dns",
        "dns_search",
        "dns_opt",
        "extra_hosts",
        "platform",
        "pull_policy",
        "logging",
        "deploy",
        "network_mode",
        "cap_drop",
        "links",
        "profiles",
        "scale",
    ];
    const BUILD: &[&str] = &[
        "context",
        "dockerfile",
        "dockerfile_inline",
        "args",
        "target",
        "labels",
        "no_cache",
        "pull",
        "shm_size",
        "tags",
        "platforms",
    ];
    let allowed = |key: &str, set: &[&str]| set.contains(&key) || key.starts_with("x-");
    let mut out = Vec::new();
    for key in doc.as_object().into_iter().flat_map(|d| d.keys()) {
        if !allowed(key, TOP) {
            out.push(key.clone());
        }
    }
    for (key, v) in doc["volumes"].as_object().into_iter().flatten() {
        if v.as_object()
            .is_some_and(|m| m.keys().any(|k| !allowed(k, &["labels", "driver"])))
            || (v["driver"]
                .as_str()
                .is_some_and(|s| !s.is_empty() && s != "local"))
        {
            out.push(format!("volumes.{key} (only plain named volumes)"));
        }
    }
    for (key, v) in doc["networks"].as_object().into_iter().flatten() {
        if v.as_object().is_some_and(|m| {
            m.keys().any(|k| {
                !allowed(
                    k,
                    &["labels", "internal", "attachable", "driver", "enable_ipv6"],
                )
            })
        }) || v["driver"]
            .as_str()
            .is_some_and(|s| !s.is_empty() && s != "bridge")
        {
            out.push(format!("networks.{key} (only plain bridge networks)"));
        }
    }
    let binds = relative_binds(doc);
    for parent in &binds {
        if !parent.read_only
            && binds.iter().any(|child| {
                child.path != parent.path
                    && (parent.path == "." || child.path.starts_with(&format!("{}/", parent.path)))
            })
        {
            out.push("volumes (a writable bind must not contain another bind source)".into());
        }
    }
    for (name, s) in doc["services"].as_object().into_iter().flatten() {
        let Some(service) = s.as_object() else {
            continue;
        };
        let at = format!("services.{name}");
        for key in service.keys() {
            if !allowed(key, SERVICE) {
                out.push(format!("{at}.{key}"));
            }
        }
        if !s["build"].is_null() {
            let b = if s["build"].is_object() {
                s["build"].clone()
            } else {
                json!({"context":s["build"]})
            };
            for key in b.as_object().unwrap().keys() {
                if !allowed(key, BUILD) {
                    out.push(format!("{at}.build.{key}"));
                }
            }
            let context = b["context"].as_str().unwrap_or(".");
            let lower = context.to_ascii_lowercase();
            if !["http://", "https://", "git://", "git@"]
                .iter()
                .any(|p| lower.starts_with(p))
                && !inside(&json!(context))
            {
                out.push(format!(
                    "{at}.build.context (it must be a folder of the app's own files)"
                ));
            }
            if !b["dockerfile"].is_null() && !inside(&json!(string(&b["dockerfile"]))) {
                out.push(format!(
                    "{at}.build.dockerfile (it must be one of the app's own files)"
                ));
            }
        }
        for v in items(&s["volumes"]) {
            if let Some(m) = v.as_str() {
                let p = m.split(':').collect::<Vec<_>>();
                if m.contains('$') {
                    out.push(format!("{at}.volumes ({m}: no variables)"));
                } else if p.len() > 1
                    && (p[0].starts_with(['.', '/', '~', '\\'])
                        || p[0].as_bytes().get(1) == Some(&b':'))
                    && !inside(&json!(p[0]))
                {
                    out.push(format!(
                        "{at}.volumes ({} is outside the app's folder)",
                        p[0]
                    ));
                }
            } else if v.is_object() {
                let kind = v["type"].as_str().unwrap_or("volume");
                if !["volume", "bind", "tmpfs"].contains(&kind) {
                    out.push(format!("{at}.volumes (type {kind})"));
                } else if kind == "bind" && !inside(&v["source"]) {
                    out.push(format!(
                        "{at}.volumes ({} is outside the app's folder)",
                        string(&v["source"])
                    ));
                } else if kind == "volume" && v["source"].as_str().is_some_and(|s| s.contains('$'))
                {
                    out.push(format!(
                        "{at}.volumes ({}: no variables)",
                        string(&v["source"])
                    ));
                }
                if v["bind"].as_object().is_some_and(|m| {
                    m.keys()
                        .any(|s| !matches!(s.as_str(), "propagation" | "create_host_path"))
                }) {
                    out.push(format!("{at}.volumes (bind options)"));
                }
            }
        }
        for f in items(&s["env_file"]) {
            let p = if f.is_object() { &f["path"] } else { f };
            if !inside(p) {
                out.push(format!(
                    "{at}.env_file ({} must be one of the app's own files)",
                    string(p)
                ));
            }
        }
        if !s["network_mode"].is_null() {
            let mode = s["network_mode"].as_str().unwrap_or("");
            if !["bridge", "none"].contains(&mode)
                && !mode
                    .strip_prefix("service:")
                    .is_some_and(|n| doc["services"].get(n).is_some())
            {
                out.push(format!("{at}.network_mode ({mode})"));
            }
        }
        for h in items(&s["extra_hosts"]) {
            if h.to_string().contains("host-gateway") {
                out.push(format!(
                    "{at}.extra_hosts (host-gateway reaches the server)"
                ));
            }
        }
        for p in items(&s["ports"]) {
            if let Some(host) = published(p) {
                if host.contains('$')
                    || host
                        .parse::<u32>()
                        .is_ok_and(|n| [2019, 19080, 19081, 19082].contains(&n))
                {
                    out.push(format!("{at}.ports ({p}: that port is the server's)"));
                }
            }
        }
        if s["logging"]["driver"]
            .as_str()
            .is_some_and(|s| !s.is_empty() && !["json-file", "local", "none"].contains(&s))
        {
            out.push(format!("{at}.logging.driver"));
        }
        for key in s["deploy"].as_object().into_iter().flat_map(|m| m.keys()) {
            if !["resources", "restart_policy", "replicas", "mode"].contains(&key.as_str()) {
                out.push(format!("{at}.deploy.{key}"));
            }
        }
        if s["deploy"]["resources"].to_string().contains("devices") {
            out.push(format!("{at}.deploy.resources (devices)"));
        }
    }
    out
}
fn published(p: &Value) -> Option<String> {
    if p.is_object() {
        (!p["published"].is_null()).then(|| string(&p["published"]))
    } else {
        let text = string(p);
        let plain = text.split('/').next()?;
        plain
            .rsplit_once(':')
            .map(|(s, _)| s.rsplit(':').next().unwrap_or("").into())
    }
}
pub fn port_range(s: &str) -> Option<Vec<u32>> {
    let (a, b) = s.split_once('-').unwrap_or((s, s));
    let a = a.parse::<u32>().ok()?;
    let b = b.parse::<u32>().ok()?;
    if b < a || b - a > 100 {
        None
    } else {
        Some((a..=b).collect())
    }
}
pub fn claims(compose: &str) -> Vec<String> {
    let doc = parse(compose).unwrap_or(Value::Null);
    let mut out = Vec::new();
    for s in doc["services"]
        .as_object()
        .into_iter()
        .flat_map(|s| s.values())
    {
        if let Some(n) = s["container_name"].as_str() {
            let c = format!("container name {n}");
            if !out.contains(&c) {
                out.push(c);
            }
        }
        for p in items(&s["ports"]) {
            if let Some(ports) = published(p).and_then(|s| port_range(&s)) {
                for n in ports {
                    let c = format!("port {n}");
                    if !out.contains(&c) {
                        out.push(c);
                    }
                }
            }
        }
    }
    out
}
fn port_parts(text: &str) -> Option<(&str, &str, &str, &str)> {
    let (plain, proto) = text
        .split_once('/')
        .map(|(a, b)| (a, &text[a.len()..], b))
        .map(|(a, p, b)| {
            if !b.is_empty() && b.bytes().all(|c| c.is_ascii_alphanumeric() || c == b'_') {
                Some((a, p))
            } else {
                None
            }
        })
        .unwrap_or(Some((text, "")))?;
    let (prefix, target) = plain.rsplit_once(':')?;
    let (ip, host) = prefix.rsplit_once(':').unwrap_or(("", prefix));
    port_range(host)?;
    port_range(target)?;
    Some((ip, host, target, proto))
}
pub fn fixed_ports(compose: &str, duplicate: bool) -> Option<String> {
    let Ok(doc) = parse(compose) else {
        return Some("the compose file".into());
    };
    for (name, s) in doc["services"].as_object().into_iter().flatten() {
        if s["network_mode"] == "host" {
            return Some(format!("services.{name}.network_mode: host"));
        }
        if duplicate && !s["container_name"].is_null() {
            return Some(format!("services.{name}.container_name"));
        }
        for p in items(&s["ports"]) {
            if p.to_string().contains('$') {
                return Some(format!("services.{name}.ports: variables"));
            }
            if !p.is_object() {
                let text = string(p);
                if port_parts(&text).is_none()
                    && port_range(text.split('/').next().unwrap_or("")).is_none()
                {
                    return Some(format!("services.{name}.ports: {p}"));
                }
            }
        }
    }
    None
}
pub fn remap(compose: &str, ports: &BTreeMap<u32, u32>) -> anyhow::Result<String> {
    let mut doc = parse(compose)?;
    for s in doc["services"]
        .as_object_mut()
        .into_iter()
        .flat_map(|m| m.values_mut())
    {
        if s["ports"].is_null() {
            continue;
        }
        let mut out = Vec::new();
        for p in items(&s["ports"]) {
            if p.is_object() {
                if let Some(pub_ports) = published(p).and_then(|s| port_range(&s)) {
                    let target = port_range(&string(&p["target"])).unwrap_or(pub_ports.clone());
                    for (i, h) in pub_ports.iter().enumerate() {
                        let mut q = p.clone();
                        q["published"] = json!(ports.get(h).unwrap_or(h).to_string());
                        q["target"] = json!(target[i.min(target.len() - 1)]);
                        out.push(q);
                    }
                } else {
                    out.push(p.clone());
                }
            } else {
                let text = string(p);
                if let Some((ip, host, target, proto)) = port_parts(&text) {
                    let hs = port_range(host).unwrap();
                    let ts = port_range(target).unwrap();
                    for (i, h) in hs.iter().enumerate() {
                        out.push(json!(format!(
                            "{}{}:{}{}",
                            if ip.is_empty() {
                                String::new()
                            } else {
                                format!("{ip}:")
                            },
                            ports.get(h).unwrap_or(h),
                            ts[i.min(ts.len() - 1)],
                            proto
                        )));
                    }
                } else {
                    out.push(p.clone());
                }
            }
        }
        s["ports"] = json!(out);
    }
    Ok(serde_yaml::to_string(&doc)?)
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Spec {
    pub compose: String,
    pub port: Option<u16>,
    pub replicas: u32,
    pub files: BTreeMap<String, String>,
}
fn count(v: &Value, where_: &str) -> anyhow::Result<Option<u32>> {
    if v.is_null() || v == "" {
        return Ok(None);
    }
    let n = v
        .as_u64()
        .or_else(|| v.as_str().and_then(|s| s.parse().ok()));
    require(
        n.is_some_and(|n| (1..=MAX_REPLICAS as u64).contains(&n)),
        format!("{where_} must be a whole number from 1 to {MAX_REPLICAS}"),
    )?;
    Ok(n.map(|n| n as u32))
}
fn valid_port(v: &Value, where_: &str) -> anyhow::Result<Option<u16>> {
    if v.is_null() {
        return Ok(None);
    }
    require(
        v.as_u64().is_some_and(|n| (1..65536).contains(&n)),
        format!("{where_} must be a whole number from 1 to 65535"),
    )?;
    Ok(v.as_u64().map(|n| n as u16))
}
fn generated(port: Option<u16>, replicas: Option<u32>) -> String {
    let mut s = String::new();
    if port.is_some() || replicas.is_some() {
        s.push_str("x-runner:\n");
    }
    if let Some(p) = port {
        s.push_str(&format!("  port: {p}\n"));
    }
    if let Some(n) = replicas {
        s.push_str(&format!("  replicas: {n}\n"));
    }
    s.push_str("services:\n  app:\n    build: .\n");
    if let Some(p) = port {
        s.push_str(&format!("    ports: [\"{p}:{p}\"]\n"));
    }
    s.push_str("    restart: unless-stopped\n");
    s
}
pub fn build(body: &Value, trusted: bool) -> anyhow::Result<Spec> {
    require(body.is_object(), "send a JSON object")?;
    require(
        body.get("routing").is_none(),
        "global and replica addresses are automatic; remove routing",
    )?;
    let mut compose = body
        .get("compose")
        .unwrap_or(&Value::Null)
        .as_str()
        .map(str::to_owned)
        .unwrap_or_default();
    require(
        body.get("compose").is_none() || body["compose"].is_string(),
        "compose must be the compose file as text",
    )?;
    require(
        body.get("files").is_none() || body["files"].is_object(),
        "files must be an object of path: content",
    )?;
    let mut files = BTreeMap::new();
    for (path, content) in body["files"].as_object().into_iter().flatten() {
        files.insert(
            path.clone(),
            content
                .as_str()
                .ok_or_else(|| http(400, format!("{path} must be text")))?
                .to_owned(),
        );
    }
    if !body["dockerfile"].is_null() {
        let f = body["dockerfile"]
            .as_str()
            .filter(|s| !s.trim().is_empty())
            .ok_or_else(|| http(400, "dockerfile must be the Dockerfile as text"))?;
        files.insert("Dockerfile".into(), f.into());
    }
    require(files.len() <= 30, "at most 30 files")?;
    let mut size = compose.len();
    for (path, content) in &files {
        require(
            !path.is_empty()
                && path
                    .bytes()
                    .all(|c| c.is_ascii_alphanumeric() || b"_.@+-/".contains(&c))
                && join_path(path).as_deref() == Some(path)
                && !path.split('/').any(str::is_empty),
            format!("{path} isn't a usable file path"),
        )?;
        require(
            ![
                "compose.yaml",
                "compose.yml",
                "docker-compose.yaml",
                "docker-compose.yml",
            ]
            .contains(&path.as_str()),
            "send the compose file as compose, not as a file",
        )?;
        require(
            path != ".env",
            "the app's .env is made from its env (PUT /api/projects/<name>/env): put the values there, not in a file",
        )?;
        size += path.len() + content.len();
    }
    require(size <= 256_000, "the spec is bigger than 250 KB")?;
    let mut port = valid_port(&body["port"], "port")?;
    let mut replicas = count(&body["replicas"], "replicas")?;
    if files.contains_key("Dockerfile") {
        if let Ok(old) = parse(&compose) {
            let p = valid_port(&old["x-runner"]["port"], "x-runner.port")
                .ok()
                .flatten();
            let r = count(&old["x-runner"]["replicas"], "x-runner.replicas")
                .ok()
                .flatten();
            if compose.trim_end_matches('\n') == generated(p, r).trim_end_matches('\n') {
                port = port.or(p);
                replicas = replicas.or(r);
                compose.clear();
            }
        }
    }
    if compose.trim().is_empty() {
        require(
            files.contains_key("Dockerfile"),
            "send a compose file, a Dockerfile, or both",
        )?;
        compose = generated(port, replicas);
    }
    let doc = parse(&compose)?;
    require(
        doc["services"].as_object().is_some_and(|s| !s.is_empty()),
        "the compose file needs a services: section",
    )?;
    let xr = &doc["x-runner"];
    let declared = valid_port(&xr["port"], "x-runner.port")?;
    require(
        port.is_none() || declared.is_none() || port == declared,
        format!(
            "the port given ({}) isn't the compose file's x-runner.port ({}); change one of them",
            port.unwrap_or(0),
            declared.unwrap_or(0)
        ),
    )?;
    port = port.or(declared);
    let replicas = replicas
        .or(count(&xr["replicas"], "x-runner.replicas")?)
        .unwrap_or(1);
    require(
        xr.get("routing").is_none(),
        "global and replica addresses are automatic; remove x-runner.routing",
    )?;
    require(
        xr["ready"].is_null()
            || xr["ready"].as_str().is_some_and(|s| {
                s.starts_with('/')
                    && s.len() <= 200
                    && s.bytes().all(|b| (0x21..=0x7e).contains(&b))
            }),
        "x-runner.ready is a path, like /healthz",
    )?;
    require(
        xr["region"].is_null() || xr["region"].as_str().is_some_and(region_valid),
        "x-runner.region must be a region label: 1-32 lowercase letters, digits or hyphens, starting with a letter",
    )?;
    require(
        xr["regions"].is_null()
            || xr["regions"].as_array().is_some_and(|s| {
                !s.is_empty()
                    && s.len() <= 50
                    && s.iter().all(|v| v.as_str().is_some_and(region_valid))
            }),
        "x-runner.regions must be a nonempty list of up to 50 region labels",
    )?;
    require(
        xr["region"].is_null() || xr["regions"].is_null(),
        "set x-runner.region or x-runner.regions, not both",
    )?;
    require(
        xr.get("spread-tags").is_none() || xr["spread-tags"].is_boolean(),
        "x-runner.spread-tags must be true or false",
    )?;
    if !trusted {
        let problems = sandbox_problems(&doc);
        if !problems.is_empty() {
            return Err(http(
                400,
                format!(
                    "without the admin password, an app gets an ordinary container only, so its compose file can't use: {}{}",
                    problems
                        .iter()
                        .take(6)
                        .cloned()
                        .collect::<Vec<_>>()
                        .join("; "),
                    if problems.len() > 6 {
                        format!("; and {} more", problems.len() - 6)
                    } else {
                        String::new()
                    }
                ),
            ));
        }
    }
    for (name, s) in doc["services"].as_object().unwrap() {
        if s["build"].is_null() {
            continue;
        }
        let b = if s["build"].is_object() {
            s["build"].clone()
        } else {
            json!({"context":s["build"]})
        };
        if b["dockerfile_inline"]
            .as_str()
            .is_some_and(|s| !s.is_empty())
        {
            continue;
        }
        let context = b["context"].as_str().unwrap_or(".");
        if context.contains("://") || context.starts_with("git@") {
            continue;
        }
        let path = join_path(&format!(
            "{context}/{}",
            b["dockerfile"].as_str().unwrap_or("Dockerfile")
        ))
        .ok_or_else(|| {
            http(
                400,
                format!("service {name} builds from outside the app's folder"),
            )
        })?;
        require(
            files.contains_key(&path),
            format!("service {name} builds from {path}, but no file with that path was sent"),
        )?;
    }
    Ok(Spec {
        compose,
        port,
        replicas,
        files,
    })
}
pub fn agent_plan(mut plan: Value) -> anyhow::Result<Value> {
    let compose = plan["compose"]
        .as_str()
        .ok_or_else(|| http(500, "missing compose"))?;
    let mut doc = parse(compose)?;
    let bind_sources = relative_binds(&doc);
    let mut binds = BTreeSet::new();
    let mut source = |s: &str| {
        if s.is_empty() || s.starts_with(['/', '~']) || s.contains('$') {
            return s.to_owned();
        }
        let relative = join_path(s).unwrap_or_else(|| s.into());
        let relative = if relative.is_empty() {
            ".".into()
        } else {
            relative
        };
        if relative != ".." && !relative.starts_with("../") {
            binds.insert(relative.clone());
        }
        format!("${{RUNNER_APP_DATA}}/{relative}")
    };
    for s in doc["services"]
        .as_object_mut()
        .into_iter()
        .flat_map(|m| m.values_mut())
    {
        for v in s
            .get_mut("volumes")
            .and_then(Value::as_array_mut)
            .into_iter()
            .flatten()
        {
            if let Some(text) = v.as_str() {
                let mut parts = text.split(':').map(str::to_owned).collect::<Vec<_>>();
                if parts.len() > 1 && parts[0].starts_with('.') {
                    parts[0] = source(&parts[0]);
                }
                *v = json!(parts.join(":"));
            } else if v["type"] == "bind" {
                if let Some(s) = v["source"].as_str() {
                    v["source"] = json!(source(s));
                }
            }
        }
    }
    let data_files = plan["files"]
        .as_object()
        .into_iter()
        .flatten()
        .filter(|(path, _)| {
            binds
                .iter()
                .any(|b| b == "." || *path == b || path.starts_with(&format!("{b}/")))
        })
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect::<serde_json::Map<_, _>>();
    let services = doc["services"]
        .as_object()
        .into_iter()
        .flatten()
        .filter(|(_, s)| s["profiles"].as_array().is_none_or(Vec::is_empty))
        .map(|(k, _)| k.clone())
        .collect::<Vec<_>>();
    let mut oneshot = Vec::new();
    for s in doc["services"]
        .as_object()
        .into_iter()
        .flat_map(|m| m.values())
    {
        for (name, d) in s["depends_on"].as_object().into_iter().flatten() {
            if d["condition"] == "service_completed_successfully" && !oneshot.contains(name) {
                oneshot.push(name.clone());
            }
        }
    }
    let mut files = plan["files"]
        .as_object()
        .into_iter()
        .flatten()
        .map(|(k, v)| (k, v))
        .collect::<Vec<_>>();
    files.sort_by(|a, b| runner_core::filename_cmp(a.0, b.0));
    let mut rev = serde_json::Map::new();
    for key in ["app", "replica", "compose", "port"] {
        if let Some(v) = plan.get(key) {
            rev.insert(key.into(), v.clone());
        }
    }
    rev.insert("files".into(), serde_json::to_value(files)?);
    rev.insert("ready".into(), plan["ready"].clone());
    plan["revision"] = json!(runner_core::auth::sha256(serde_json::to_vec(&rev)?));
    plan["managedCompose"] = json!(serde_json::to_string(&doc)?);
    plan["dataFiles"] = json!(data_files);
    plan["services"] = json!(services);
    plan["oneshotServices"] = json!(oneshot);
    plan["bindSources"] = serde_json::to_value(bind_sources)?;
    Ok(plan)
}
