use crate::{
    args,
    runtime::{OWNER_LABEL, app_routes, name_valid, owns_infrastructure},
    workloads::DockerHost,
};
use anyhow::ensure;
use runner_core::{
    auth::sha256,
    fs::{atomic_write, private_directory},
};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    time::Duration,
};

pub struct Infrastructure {
    pub router: String,
    pub tunnel: String,
    host: DockerHost,
    router_config: String,
}
impl Infrastructure {
    pub async fn select(host: DockerHost) -> anyhow::Result<Self> {
        let mut result = Self {
            router: format!("{}-router", host.owner),
            tunnel: format!("{}-tunnel", host.owner),
            host,
            router_config: String::new(),
        };
        result.router = result.select_kind("router").await?;
        result.tunnel = result.select_kind("tunnel").await?;
        let ingress = format!(
            "ingress:\n  - service: http://127.0.0.1:{}\n",
            result.host.router_port
        );
        atomic_write(
            &result.host.base.join("tunnel.yml"),
            ingress.as_bytes(),
            0o600,
        )?;
        Ok(result)
    }
    fn owns(&self, container: &Value, kind: &str) -> bool {
        let source = self.host.base.join(if kind == "router" {
            "router"
        } else {
            "tunnel.yml"
        });
        owns_infrastructure(
            container,
            &self.host.owner,
            kind,
            &source,
            if kind == "router" {
                "/etc/router"
            } else {
                "/etc/cloudflared/config.yml"
            },
        )
    }
    async fn select_kind(&self, kind: &str) -> anyhow::Result<String> {
        let name = format!("{}-{kind}", self.host.owner);
        if let Some(current) = self.host.named(&name).await? {
            ensure!(
                self.owns(&current, kind),
                "the fleet's {kind} container name is already in use"
            );
            return Ok(name);
        }
        Ok(match self.host.named(kind).await? {
            Some(legacy) if self.owns(&legacy, kind) => kind.into(),
            _ => name,
        })
    }
    async fn remove(&self, kind: &str) -> anyhow::Result<()> {
        let name = if kind == "router" {
            &self.router
        } else {
            &self.tunnel
        };
        if let Some(container) = self.host.named(name).await? {
            ensure!(
                self.owns(&container, kind),
                "refusing to remove an unrelated {kind} container"
            );
            ensure!(
                self.host
                    .commands
                    .docker(&args!["rm", "-f", container["Id"].as_str().unwrap_or("")])
                    .await?
                    .ok,
                "could not replace the {kind} container"
            );
        }
        Ok(())
    }
    pub async fn tunnel_running(&self) -> anyhow::Result<bool> {
        Ok(self
            .host
            .named(&self.tunnel)
            .await?
            .is_some_and(|c| self.owns(&c, "tunnel") && c["State"]["Running"] == true))
    }
    pub async fn router(
        &mut self,
        projects: Vec<Value>,
        domain: &str,
        machine: u64,
    ) -> anyhow::Result<()> {
        let directory = private_directory(&self.host.base.join("router"))?;
        let body = router_json(projects, domain, machine, self.host.router_port)?;
        let container = self.host.named(&self.router).await?;
        if let Some(container) = &container {
            ensure!(
                self.owns(container, "router"),
                "refusing to change an unrelated router"
            );
        }
        if container.is_some_and(|c| c["State"]["Running"] == true) {
            if body == self.router_config {
                return Ok(());
            }
            atomic_write(&directory.join("caddy.json"), body.as_bytes(), 0o600)?;
            let mut result = self
                .host
                .commands
                .docker(&args![
                    "exec",
                    &self.router,
                    "caddy",
                    "reload",
                    "--address",
                    format!("127.0.0.1:{}", self.host.router_port + 2),
                    "--config",
                    "/etc/router/caddy.json"
                ])
                .await?;
            if !result.ok && self.router == "router" {
                result = self
                    .host
                    .commands
                    .docker(&args![
                        "exec",
                        &self.router,
                        "caddy",
                        "reload",
                        "--address",
                        "127.0.0.1:2019",
                        "--config",
                        "/etc/router/caddy.json"
                    ])
                    .await?;
            }
            ensure!(result.ok, "router reload failed");
            self.router_config = body;
            return Ok(());
        }
        atomic_write(&directory.join("caddy.json"), body.as_bytes(), 0o600)?;
        self.remove("router").await?;
        let result = self
            .host
            .commands
            .run(
                "docker",
                &args![
                    "run",
                    "-d",
                    "--name",
                    &self.router,
                    "--network",
                    "host",
                    "--restart",
                    "unless-stopped",
                    "--label",
                    format!("{OWNER_LABEL}={}", self.host.owner),
                    "--label",
                    "io.runners.component=router",
                    "-v",
                    format!("{}:/etc/router:ro", directory.display()),
                    "caddy:2",
                    "caddy",
                    "run",
                    "--config",
                    "/etc/router/caddy.json"
                ],
                &BTreeMap::new(),
                Duration::from_secs(300),
                false,
            )
            .await?;
        ensure!(result.ok, "router did not start");
        for _ in 0..30 {
            if self
                .host
                .probe("check", "/", Duration::from_secs(1))
                .await
                .0
                != 0
            {
                self.router_config = body;
                return Ok(());
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        anyhow::bail!("router is not answering")
    }
    pub async fn start_tunnel(&self, token: &str) -> anyhow::Result<()> {
        self.remove("tunnel").await?;
        let result = self
            .host
            .commands
            .run(
                "docker",
                &args![
                    "run",
                    "-d",
                    "--name",
                    &self.tunnel,
                    "--network",
                    "host",
                    "--restart",
                    "unless-stopped",
                    "--label",
                    format!("{OWNER_LABEL}={}", self.host.owner),
                    "--label",
                    "io.runners.component=tunnel",
                    "-e",
                    "TUNNEL_TOKEN",
                    "-v",
                    format!(
                        "{}:/etc/cloudflared/config.yml:ro",
                        self.host.base.join("tunnel.yml").display()
                    ),
                    "cloudflare/cloudflared:latest",
                    "tunnel",
                    "--no-autoupdate",
                    "--metrics",
                    format!("127.0.0.1:{}", self.host.router_port + 1),
                    "--config",
                    "/etc/cloudflared/config.yml",
                    "run"
                ],
                &BTreeMap::from([("TUNNEL_TOKEN".into(), token.into())]),
                Duration::from_secs(300),
                false,
            )
            .await?;
        ensure!(result.ok, "tunnel did not start");
        for _ in 0..45 {
            let logs = self
                .host
                .commands
                .docker(&args!["logs", "--tail", "30", &self.tunnel])
                .await?;
            if logs.out.contains("Registered tunnel connection")
                || logs.stderr.contains("Registered tunnel connection")
            {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
        anyhow::bail!("tunnel did not connect")
    }
    pub async fn withdraw_machine(&self) -> anyhow::Result<()> {
        if let Some(container) = self.host.named(&self.tunnel).await? {
            ensure!(
                self.owns(&container, "tunnel"),
                "refusing to stop an unrelated tunnel"
            );
            ensure!(
                self.host
                    .commands
                    .docker(&args![
                        "stop",
                        "-t",
                        "10",
                        container["Id"].as_str().unwrap_or("")
                    ])
                    .await?
                    .ok,
                "could not stop machine tunnel"
            );
        }
        Ok(())
    }
}
fn router_json(
    projects: Vec<Value>,
    domain: &str,
    machine: u64,
    port: u16,
) -> anyhow::Result<String> {
    let mut routes = app_routes(projects, domain);
    routes.push(json!({"handle":[{"handler":"static_response","status_code":404,"headers":{"X-Runner-Route":["none"]},"body":format!("No such project on machine {machine}\n")}]}));
    Ok(serde_json::to_string(
        &json!({"admin":{"listen":format!("127.0.0.1:{}",port+2)},"apps":{"http":{"servers":{"router":{
            "listen":[format!("127.0.0.1:{port}")],"automatic_https":{"disable":true},"routes":routes,"trusted_proxies":{"source":"static","ranges":["127.0.0.1/32","::1/128"]}
        }}}}}),
    )?)
}

pub struct PublicTunnels {
    host: DockerHost,
    pub active: BTreeMap<String, String>,
    failures: BTreeMap<String, u32>,
    connections: BTreeMap<String, String>,
    pub system: BTreeSet<String>,
}
impl PublicTunnels {
    pub fn new(host: DockerHost) -> Self {
        Self {
            host,
            active: BTreeMap::new(),
            failures: BTreeMap::new(),
            connections: BTreeMap::new(),
            system: BTreeSet::new(),
        }
    }
    fn owns(&self, container: &Value) -> bool {
        container["Config"]["Labels"][OWNER_LABEL] == self.host.owner
            && container["Config"]["Labels"]["io.runners.component"] == "public-tunnel"
    }
    async fn remove(&mut self, container: &Value) -> anyhow::Result<()> {
        ensure!(
            self.owns(container),
            "refusing to remove an unrelated public tunnel"
        );
        let id = container["Id"].as_str().unwrap_or("");
        ensure!(
            self.host.commands.docker(&args!["rm", "-f", id]).await?.ok,
            "cannot remove owned public tunnel"
        );
        self.connections.remove(id);
        Ok(())
    }
    pub async fn reconcile(
        &mut self,
        plan: &Value,
        projects: &[Value],
        domain: &str,
        open: bool,
    ) -> anyhow::Result<()> {
        let empty = serde_json::Map::new();
        let plan = plan.as_object().unwrap_or(&empty);
        let mut containers: BTreeMap<String, Value> = self
            .host
            .labeled(&[
                format!("{OWNER_LABEL}={}", self.host.owner),
                "io.runners.component=public-tunnel".into(),
            ])
            .await?
            .into_iter()
            .filter(|c| self.owns(c))
            .map(|c| {
                (
                    c["Name"]
                        .as_str()
                        .unwrap_or("")
                        .trim_start_matches('/')
                        .into(),
                    c,
                )
            })
            .collect();
        let obsolete: Vec<_> = containers
            .iter()
            .filter(|(_, c)| {
                !plan.contains_key(
                    c["Config"]["Labels"]["io.runners.app"]
                        .as_str()
                        .unwrap_or(""),
                )
            })
            .map(|(name, c)| (name.clone(), c.clone()))
            .collect();
        for (name, container) in obsolete {
            self.remove(&container).await?;
            containers.remove(&name);
            self.system.remove(&name);
        }
        self.failures.retain(|app, _| plan.contains_key(app));
        let mut next = BTreeMap::new();
        for (app, tunnel) in plan {
            let id = tunnel["id"].as_str().unwrap_or("");
            let token = tunnel["token"].as_str().unwrap_or("");
            ensure!(
                name_valid(app, 40) && !id.is_empty() && !token.is_empty(),
                "invalid public tunnel authorization"
            );
            let name = format!("{}-public-{}", self.host.owner, &sha256(app)[..16]);
            let revision = sha256(token);
            self.system.insert(name.clone());
            let mut container = containers.remove(&name);
            let eligible = open
                && projects.iter().any(|p| {
                    p["app"] == *app
                        && p["port"].as_u64().unwrap_or(0) > 0
                        && p["s"] == "healthy"
                        && p["r"] != false
                        && p["busy"] != true
                });
            let healthy = if eligible {
                let path = projects
                    .iter()
                    .find(|p| p["app"] == *app && p["readyPath"].is_string())
                    .and_then(|p| p["readyPath"].as_str())
                    .unwrap_or("/");
                let (code, routed) = self
                    .host
                    .probe(
                        &format!("{app}.{domain}"),
                        path,
                        Duration::from_millis(1500),
                    )
                    .await;
                routed && code > 0 && code < 500
            } else {
                false
            };
            let failures = self.failures.entry(app.clone()).or_default();
            *failures = if healthy {
                0
            } else {
                failures.saturating_add(1)
            };
            if !healthy {
                if let Some(container) = container.filter(|c| c["State"]["Running"] == true) {
                    if !eligible || *failures >= 2 {
                        let cid = container["Id"].as_str().unwrap_or("");
                        ensure!(
                            self.host
                                .commands
                                .docker(&args!["stop", "-t", "0", cid])
                                .await?
                                .ok,
                            "cannot withdraw unhealthy public tunnel"
                        );
                        self.connections.remove(cid);
                    } else if self.active.get(app).is_some_and(|active| active == id) {
                        next.insert(app.clone(), id.into());
                    }
                }
                continue;
            }
            if container.as_ref().is_some_and(|c| {
                c["Config"]["Labels"]["io.runners.tunnel"] != id
                    || c["Config"]["Labels"]["io.runners.token-revision"] != revision
            }) {
                self.remove(container.as_ref().unwrap()).await?;
                container = None;
            }
            if container.is_none() {
                ensure!(
                    self.host.named(&name).await?.is_none(),
                    "public tunnel name belongs to another container"
                );
                let result = self
                    .host
                    .commands
                    .run(
                        "docker",
                        &args![
                            "run",
                            "-d",
                            "--name",
                            &name,
                            "--network",
                            "host",
                            "--restart",
                            "unless-stopped",
                            "--memory",
                            "128m",
                            "--label",
                            format!("{OWNER_LABEL}={}", self.host.owner),
                            "--label",
                            "io.runners.component=public-tunnel",
                            "--label",
                            format!("io.runners.app={app}"),
                            "--label",
                            format!("io.runners.tunnel={id}"),
                            "--label",
                            format!("io.runners.token-revision={revision}"),
                            "-e",
                            "TUNNEL_TOKEN",
                            "-v",
                            format!(
                                "{}:/etc/cloudflared/config.yml:ro",
                                self.host.base.join("tunnel.yml").display()
                            ),
                            "cloudflare/cloudflared:latest",
                            "tunnel",
                            "--no-autoupdate",
                            "--config",
                            "/etc/cloudflared/config.yml",
                            "run"
                        ],
                        &BTreeMap::from([("TUNNEL_TOKEN".into(), token.into())]),
                        Duration::from_secs(300),
                        false,
                    )
                    .await?;
                if !result.ok {
                    continue;
                }
                container = Some(
                    json!({"Id":result.out.trim(),"State":{"Running":true,"StartedAt":"new"}}),
                );
            }
            let container = container.as_mut().unwrap();
            let cid = container["Id"].as_str().unwrap_or("").to_owned();
            if container["State"]["Running"] != true {
                if !self.host.commands.docker(&args!["start", &cid]).await?.ok {
                    continue;
                }
                container["State"] = json!({"Running":true,"StartedAt":"restarted"});
            }
            let started = container["State"]["StartedAt"]
                .as_str()
                .unwrap_or("running");
            if self
                .connections
                .get(&cid)
                .is_none_or(|cached| cached != started)
            {
                let logs = self
                    .host
                    .commands
                    .docker(&args!["logs", "--tail", "30", &cid])
                    .await?;
                if !logs.ok
                    || !(logs.out.contains("Registered tunnel connection")
                        || logs.stderr.contains("Registered tunnel connection"))
                {
                    continue;
                }
                self.connections.insert(cid, started.into());
            }
            next.insert(app.clone(), id.into());
        }
        self.active = next;
        Ok(())
    }
}
