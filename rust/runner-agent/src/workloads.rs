use crate::{
    args,
    command::Commands,
    files::{stage_deployment, validate_bind_sources, write_data_files},
    runtime::*,
};
use anyhow::{Context, ensure};
use futures_util::{StreamExt, TryStreamExt, stream};
use runner_core::{now_ms, response_bytes, response_json};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

#[derive(Clone)]
pub struct DockerHost {
    pub owner: String,
    pub base: PathBuf,
    pub commands: Commands,
    pub client: reqwest::Client,
    pub engine: reqwest::Client,
    pub router_port: u16,
}
impl DockerHost {
    pub async fn named(&self, name: &str) -> anyhow::Result<Option<Value>> {
        ensure!(
            !name.is_empty()
                && name
                    .bytes()
                    .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'-' | b'_' | b'.')),
            "invalid Docker container name"
        );
        let response = self
            .engine
            .get(format!("http://localhost/containers/{name}/json"))
            .send()
            .await?;
        if response.status().as_u16() == 404 {
            return Ok(None);
        }
        Ok(Some(response_json(response, 4 << 20).await?))
    }
    pub async fn containers(&self, project: &str) -> anyhow::Result<Vec<Value>> {
        self.labeled(&[format!("com.docker.compose.project={project}")])
            .await
    }
    pub async fn labeled(&self, labels: &[String]) -> anyhow::Result<Vec<Value>> {
        let filters = serde_json::to_string(&json!({"label":labels}))?;
        let list: Vec<Value> = response_json(
            self.engine
                .get("http://localhost/containers/json")
                .query(&[("all", "true"), ("filters", &filters)])
                .send()
                .await?,
            4 << 20,
        )
        .await?;
        self.inspect(
            list.iter()
                .filter_map(|c| c["Id"].as_str().map(String::from))
                .collect(),
        )
        .await
    }
    pub async fn inspect(&self, ids: Vec<String>) -> anyhow::Result<Vec<Value>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        ensure!(
            ids.len() <= 4096
                && ids
                    .iter()
                    .all(|id| !id.is_empty() && id.bytes().all(|c| c.is_ascii_hexdigit())),
            "invalid container identity"
        );
        stream::iter(ids.into_iter().map(|id| async move {
            response_json(
                self.engine
                    .get(format!("http://localhost/containers/{id}/json"))
                    .send()
                    .await?,
                4 << 20,
            )
            .await
        }))
        .buffer_unordered(4)
        .try_collect()
        .await
    }
    pub fn owns(&self, container: &Value, key: &str) -> bool {
        owns_compose(
            container,
            &self.owner,
            key,
            &self.base.join("projects").join(key),
        )
    }
    pub async fn select_project(&self, key: &str, previous: &Value) -> anyhow::Result<String> {
        let named = project_name(&self.owner, key)?;
        if let Some(saved) = previous["composeProject"].as_str() {
            ensure!(
                saved == key || saved == named,
                "saved Compose identity does not belong to this fleet"
            );
            ensure!(
                self.containers(saved)
                    .await?
                    .iter()
                    .all(|c| self.owns(c, key)),
                "refusing to change a Compose project with unrelated containers"
            );
            return Ok(saved.into());
        }
        let current = self.containers(&named).await?;
        ensure!(
            current.iter().all(|c| self.owns(c, key)),
            "the fleet's Compose name is already owned by another application"
        );
        if !current.is_empty() {
            return Ok(named);
        }
        let legacy = self.containers(key).await?;
        Ok(
            if !legacy.is_empty() && legacy.iter().all(|c| self.owns(c, key)) {
                key.into()
            } else {
                named
            },
        )
    }
    pub async fn owned(&self, key: &str, project: &Value) -> anyhow::Result<Vec<Value>> {
        let compose = if let Some(saved) = project["composeProject"].as_str() {
            ensure!(
                saved == key || saved == project_name(&self.owner, key)?,
                "saved Compose identity does not belong to this fleet"
            );
            saved.into()
        } else {
            self.select_project(key, project).await?
        };
        let containers = self.containers(&compose).await?;
        ensure!(
            containers.iter().all(|c| self.owns(c, key)),
            "refusing to change unrelated Docker containers"
        );
        Ok(containers)
    }
    pub async fn probe(&self, host: &str, path: &str, timeout: Duration) -> (u16, bool) {
        let result = self
            .client
            .get(format!("http://127.0.0.1:{}{}", self.router_port, path))
            .header("host", host)
            .timeout(timeout)
            .send()
            .await;
        if let Ok(response) = result {
            let status = response.status().as_u16();
            let routed = response
                .headers()
                .get("x-runner-route")
                .is_none_or(|value| value != "none");
            // Response bodies may be a large stream. Headers suffice for a probe;
            // dropping the response cancels it instead of buffering the stream.
            return (status, routed);
        }
        (0, false)
    }
    pub async fn answers(&self, project: &Value, domain: &str, duration: Duration) -> bool {
        let end = Instant::now() + duration;
        let hostname = format!(
            "{}-{}.{}",
            project["app"].as_str().unwrap_or(""),
            project["replica"].as_u64().unwrap_or(1),
            domain
        );
        loop {
            let (code, routed) = self.probe(&hostname, "/", Duration::from_secs(5)).await;
            if code > 0 && routed && code != 502 {
                return true;
            }
            if Instant::now() >= end {
                return false;
            }
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
    }
    pub async fn ready(&self, project: &Value, domain: &str) -> Option<bool> {
        let path = project["readyPath"].as_str().filter(|s| !s.is_empty())?;
        let host = format!(
            "{}-{}.{}",
            project["app"].as_str().unwrap_or(""),
            project["replica"].as_u64().unwrap_or(1),
            domain
        );
        let (code, routed) = self.probe(&host, path, Duration::from_secs(5)).await;
        Some(routed && (200..300).contains(&code))
    }
    pub async fn apply(
        &self,
        key: &str,
        want: &Value,
        before: &Value,
        mut project: Value,
        domain: &str,
        recreate: bool,
    ) -> Value {
        let result = self
            .apply_inner(key, want, before, &mut project, domain, recreate)
            .await;
        project["s"] = json!(if result.is_ok() { "healthy" } else { "failed" });
        project["e"] = json!(
            result
                .err()
                .map(|e| tail(&e.to_string(), 4000))
                .unwrap_or_default()
        );
        project["at"] = json!(now_ms());
        project["r"] = if project["s"] == "healthy" {
            json!(self.ready(&project, domain).await)
        } else {
            Value::Null
        };
        if project["sourceDir"].is_string() {
            let prior = (project["s"] != "healthy")
                .then(|| before["sourceDir"].as_str())
                .flatten();
            let _ = self.prune(key, &project, prior).await;
        }
        project["busy"] = json!(false);
        project
    }
    async fn apply_inner(
        &self,
        key: &str,
        want: &Value,
        before: &Value,
        project: &mut Value,
        domain: &str,
        recreate: bool,
    ) -> anyhow::Result<()> {
        project["composeProject"] = json!(self.select_project(key, before).await?);
        let managed = want["managedCompose"].as_str().context(
            "update the control plane to provide safe deployment files before changing this app",
        )?;
        let mut compose: Value = serde_json::from_str(managed)?;
        let oneshot = strings(&want["oneshotServices"]);
        if let Some(services) = compose["services"].as_object_mut() {
            for (service, config) in services {
                let mut labels = match &config["labels"] {
                    Value::Object(labels) => labels.clone(),
                    Value::Array(labels) => labels
                        .iter()
                        .filter_map(|label| label.as_str())
                        .map(|label| {
                            let (k, v) = label.split_once('=').unwrap_or((label, ""));
                            (k.into(), json!(v))
                        })
                        .collect(),
                    _ => serde_json::Map::new(),
                };
                labels.insert(OWNER_LABEL.into(), json!(self.owner));
                labels.insert(COPY_LABEL.into(), json!(key));
                labels.insert(
                    "io.runners.oneshot".into(),
                    json!(if oneshot.contains(service) {
                        "true"
                    } else {
                        "false"
                    }),
                );
                config["labels"] = Value::Object(labels);
            }
        }
        let data_dir = self.base.join("projects").join(key);
        std::fs::create_dir_all(&data_dir)?;
        let sources: Vec<BindSource> =
            serde_json::from_value(want.get("bindSources").cloned().unwrap_or(json!([])))?;
        let previous: Option<Vec<BindSource>> = before
            .get("bindSources")
            .map(|v| serde_json::from_value(v.clone()))
            .transpose()?;
        if previous.is_none_or(|p| bind_layout(&p) != bind_layout(&sources)) {
            let containers = self.owned(key, project).await?;
            let running = containers
                .iter()
                .filter(|c| c["State"]["Running"] == true)
                .filter_map(|c| c["Id"].as_str());
            let mut arguments = args!["stop", "-t", "10"];
            arguments.extend(running.map(String::from));
            if arguments.len() > 3 {
                ensure!(
                    self.commands
                        .run(
                            "docker",
                            &arguments,
                            &BTreeMap::new(),
                            Duration::from_secs(120),
                            false
                        )
                        .await?
                        .ok,
                    "could not safely stop the app before changing its bind mounts"
                );
            }
        }
        let data_files =
            serde_json::from_value(want.get("dataFiles").cloned().unwrap_or(json!({})))?;
        write_data_files(&data_dir, &data_files)?;
        validate_bind_sources(&data_dir, &sources)?;
        project["bindSources"] = serde_json::to_value(&sources)?;
        let files: BTreeMap<String, String> =
            serde_json::from_value(want.get("files").cloned().unwrap_or(json!({})))?;
        let source = stage_deployment(
            &self.base.join("deployments"),
            key,
            &files,
            &serde_json::to_string(&compose)?,
        )?;
        let fresh =
            recreate || (!before.is_null() && before["fingerprint"] != project["fingerprint"]);
        let mut arguments = args![
            "compose",
            "-p",
            project["composeProject"].as_str().unwrap(),
            "-f",
            source.join("compose.yaml").display(),
            "up",
            "-d",
            "--build",
            "--remove-orphans",
            "--wait",
            "--wait-timeout",
            "300"
        ];
        if fresh {
            arguments.push("--force-recreate".into());
        }
        let extra = BTreeMap::from([(
            "RUNNER_APP_DATA".into(),
            data_dir.to_string_lossy().to_string(),
        )]);
        project["sourceDir"] = json!(source);
        let up = self
            .commands
            .run(
                "docker",
                &arguments,
                &extra,
                Duration::from_secs(15 * 60),
                false,
            )
            .await?;
        ensure!(
            up.ok,
            "{}",
            tail(&format!("{}\n{}", up.out, up.stderr), 4000)
        );
        let containers = self.owned(key, project).await?;
        let services: Vec<String> = containers
            .iter()
            .filter_map(|c| {
                c["Config"]["Labels"]["com.docker.compose.service"]
                    .as_str()
                    .map(String::from)
            })
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        project["services"] = json!(services);
        if let Some(problem) = container_health(&containers, &services, &oneshot) {
            anyhow::bail!(problem);
        }
        if want["port"].as_u64().unwrap_or(0) > 0 {
            ensure!(
                self.answers(project, domain, Duration::from_secs(60)).await,
                "nothing answers on the application port through the router"
            );
        }
        Ok(())
    }
    pub async fn remove(&self, key: &str, project: &Value) -> anyhow::Result<()> {
        project_name(&self.owner, key)?;
        let containers = self.owned(key, project).await?;
        let mut running = args!["stop"];
        running.extend(
            containers
                .iter()
                .filter(|c| c["State"]["Running"] == true)
                .filter_map(|c| c["Id"].as_str())
                .map(String::from),
        );
        if running.len() > 1 {
            ensure!(
                self.commands
                    .run(
                        "docker",
                        &running,
                        &BTreeMap::new(),
                        Duration::from_secs(15 * 60),
                        false
                    )
                    .await?
                    .ok,
                "could not stop the application's containers cleanly"
            );
        }
        if !containers.is_empty() {
            let mut arguments = args!["rm"];
            arguments.extend(
                containers
                    .iter()
                    .filter_map(|c| c["Id"].as_str())
                    .map(String::from),
            );
            ensure!(
                self.commands.docker(&arguments).await?.ok,
                "could not remove this application's Docker containers"
            );
        }
        self.remove_dir(&self.base.join("projects").join(key))
            .await?;
        self.remove_dir(&self.base.join("deployments").join(key))
            .await?;
        Ok(())
    }
    async fn remove_dir(&self, path: &Path) -> anyhow::Result<()> {
        match std::fs::remove_dir_all(path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::PermissionDenied => {
                let _ = self
                    .commands
                    .run(
                        "docker",
                        &args![
                            "run",
                            "--rm",
                            "-v",
                            format!("{}:/p", path.display()),
                            "busybox",
                            "sh",
                            "-c",
                            "find /p -mindepth 1 -delete"
                        ],
                        &BTreeMap::new(),
                        Duration::from_secs(300),
                        false,
                    )
                    .await?;
                std::fs::remove_dir_all(path)?;
                Ok(())
            }
            Err(e) => Err(e.into()),
        }
    }
    async fn prune(
        &self,
        key: &str,
        project: &Value,
        previous: Option<&str>,
    ) -> anyhow::Result<()> {
        let root = self.base.join("deployments").join(key);
        let mut keep: BTreeSet<PathBuf> = project["sourceDir"]
            .as_str()
            .into_iter()
            .chain(previous)
            .map(PathBuf::from)
            .collect();
        for container in self.owned(key, project).await? {
            let working =
                container["Config"]["Labels"]["com.docker.compose.project.working_dir"].as_str();
            let sources = container["Mounts"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(|m| m["Source"].as_str());
            for path in working.into_iter().chain(sources) {
                if let Ok(relative) = Path::new(path).strip_prefix(&root) {
                    if let Some(first) = relative.components().next() {
                        keep.insert(root.join(first));
                    }
                }
            }
        }
        for entry in std::fs::read_dir(&root)? {
            let entry = entry?;
            if entry.file_name().to_string_lossy().starts_with("release-")
                && !keep.contains(&entry.path())
            {
                std::fs::remove_dir_all(entry.path())?;
            }
        }
        Ok(())
    }
    pub async fn tunnel_edges(&self) -> Option<u64> {
        let response = self
            .client
            .get(format!("http://127.0.0.1:{}/metrics", self.router_port + 1))
            .timeout(Duration::from_secs(3))
            .send()
            .await
            .ok()?;
        let data = response_bytes(response, 1 << 20).await.ok()?;
        String::from_utf8_lossy(&data).lines().find_map(|line| {
            line.strip_prefix("cloudflared_tunnel_ha_connections ")
                .and_then(|v| v.parse().ok())
        })
    }
}
pub fn starting_project(
    key: &str,
    want: &Value,
    before: &Value,
    recreate: bool,
) -> anyhow::Result<Value> {
    let app = want["app"].as_str().unwrap_or(key);
    let replica = want["replica"].as_u64().unwrap_or(1);
    let same = !before.is_null() && before["replica"] == replica && before["app"] == app;
    Ok(
        json!({"app":app,"replica":replica,"v":want["v"],"port":want["port"],"readyPath":want["ready"],"s":"applying","e":"",
        "fingerprint":fingerprint(want)?,"revision":want["revision"],"busy":true,"at":now_ms(),"misses":0,"oneshotServices":want.get("oneshotServices").cloned().unwrap_or(json!([])),
        "composeProject":before["composeProject"],"sourceDir":before["sourceDir"],"t0":if same { before["t0"].as_u64().unwrap_or(now_ms()) } else { now_ms() },
        "n":if same { before["n"].as_u64().unwrap_or(0) } else { 0 } + u64::from(recreate)}),
    )
}
pub fn strings(value: &Value) -> Vec<String> {
    value
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|v| v.as_str().map(String::from))
        .collect()
}
pub fn tail(text: &str, limit: usize) -> String {
    let offset = text.len().saturating_sub(limit);
    let start = (offset..=text.len())
        .find(|i| text.is_char_boundary(*i))
        .unwrap_or(text.len());
    text[start..].into()
}
