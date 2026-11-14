use futures_util::{StreamExt, stream};
use runner_core::{now_ms, response_json};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    time::{Duration, Instant},
};

type Fields = BTreeMap<String, f64>;
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CopyIdentity {
    pub key: String,
    pub app: String,
    pub replica: u64,
}
struct Host {
    at: Instant,
    cpu: [f64; 8],
    mem_total: f64,
    mem_avail: f64,
    disk_read: f64,
    disk_write: f64,
    rx: f64,
    tx: f64,
}
#[derive(Clone)]
struct Container {
    id: String,
    app: String,
    copy: Option<CopyIdentity>,
    cpu: Option<f64>,
    mem: f64,
    rx: f64,
    tx: f64,
    read: f64,
    write: f64,
}
struct Containers {
    at: Instant,
    list: Vec<Container>,
}
pub struct Observation {
    host: Host,
    containers: Containers,
}
#[derive(Default)]
struct Accumulator {
    n: u64,
    sum: Fields,
    cpu_max: f64,
}
impl Accumulator {
    fn add(&mut self, sample: &Fields) {
        self.n += 1;
        for (key, value) in sample {
            *self.sum.entry(key.clone()).or_default() += value;
        }
        self.cpu_max = self.cpu_max.max(*sample.get("cpu").unwrap_or(&0.0));
    }
    fn summarize(&self) -> Fields {
        let mut out: Fields = self
            .sum
            .iter()
            .map(|(key, value)| (key.clone(), rounded(value / self.n.max(1) as f64)))
            .collect();
        out.insert("cpuMax".into(), rounded(self.cpu_max));
        out
    }
}
struct Minute {
    t: u64,
    host: Accumulator,
    apps: BTreeMap<String, Accumulator>,
}
struct Point {
    t: u64,
    compressed: Vec<u8>,
}
pub struct Metrics {
    client: reqwest::Client,
    host: Option<Host>,
    containers: Option<Containers>,
    latest: Value,
    minute: Option<Minute>,
    retained: VecDeque<Point>,
    retained_bytes: usize,
    confirmed_through: u64,
}
impl Metrics {
    pub fn new(socket: &str, history: &[Value]) -> anyhow::Result<Self> {
        let client = reqwest::Client::builder()
            .unix_socket(socket)
            .timeout(Duration::from_secs(8))
            .pool_max_idle_per_host(4)
            .redirect(reqwest::redirect::Policy::none())
            .build()?;
        let mut metrics = Self {
            client,
            host: None,
            containers: None,
            latest: Value::Null,
            minute: None,
            retained: VecDeque::new(),
            retained_bytes: 0,
            confirmed_through: history
                .iter()
                .filter_map(|p| p["t"].as_u64())
                .max()
                .unwrap_or(0),
        };
        for row in history
            .iter()
            .filter(|row| row["t"].as_u64().unwrap_or(0) > now_ms().saturating_sub(6 * 3600_000))
            .rev()
            .take(360)
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
        {
            metrics.retain(row)?;
        }
        Ok(metrics)
    }
    fn retain(&mut self, summary: &Value) -> anyhow::Result<()> {
        let point = Point {
            t: summary["t"].as_u64().unwrap_or(0),
            compressed: lz4_flex::compress_prepend_size(&serde_json::to_vec(summary)?),
        };
        if point.compressed.len() > 1 << 20 {
            return Ok(());
        }
        self.retained_bytes += point.compressed.len();
        self.retained.push_back(point);
        while self.retained.len() > 360 || self.retained_bytes > 4 << 20 {
            self.retained_bytes -= self.retained.pop_front().unwrap().compressed.len();
        }
        Ok(())
    }
    fn decode(point: &Point) -> anyhow::Result<Value> {
        Ok(serde_json::from_slice(
            &lz4_flex::decompress_size_prepended(&point.compressed)?,
        )?)
    }
    pub fn history(&self) -> Vec<Value> {
        self.retained
            .iter()
            .filter_map(|p| Self::decode(p).ok())
            .collect()
    }
    pub fn payload(&self) -> Value {
        let minutes: Vec<_> = self
            .retained
            .iter()
            .rev()
            .take(30)
            .filter(|p| p.t > self.confirmed_through)
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .filter_map(|p| Self::decode(p).ok())
            .collect();
        let recent: Vec<_> = self
            .retained
            .iter()
            .rev()
            .take(10)
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .filter_map(|p| Self::decode(p).ok())
            .collect();
        json!({"live":self.latest,"minutes":minutes,"recent":recent})
    }
    pub fn confirm(&mut self, sent: &Value) {
        if let Some(t) = sent["minutes"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|p| p["t"].as_u64())
            .max()
        {
            self.confirmed_through = self.confirmed_through.max(t);
        }
    }
    pub fn client(&self) -> reqwest::Client {
        self.client.clone()
    }
    pub async fn collect(
        client: &reqwest::Client,
        projects: &BTreeMap<String, CopyIdentity>,
        system: &BTreeSet<String>,
    ) -> anyhow::Result<Observation> {
        let host = read_host();
        let containers = read_containers(client, projects, system).await?;
        Ok(Observation { host, containers })
    }
    pub fn ingest(&mut self, observation: Observation) -> anyhow::Result<()> {
        let Observation { host, containers } = observation;
        let now = now_ms();
        let bucket = now - now % 60_000;
        if let (Some(old_host), Some(old_containers)) = (&self.host, &self.containers) {
            let host_fields = host_sample(old_host, &host);
            let apps = app_sample(old_containers, &containers, false);
            let replicas = replica_sample(old_containers, &containers);
            if self.minute.as_ref().is_none_or(|m| m.t != bucket) {
                if let Some(minute) = self.minute.take() {
                    let apps: BTreeMap<_, _> = minute
                        .apps
                        .iter()
                        .map(|(key, acc)| (key.clone(), acc.summarize()))
                        .collect();
                    self.retain(&json!({"t":minute.t,"h":minute.host.summarize(),"a":apps}))?;
                }
                self.minute = Some(Minute {
                    t: bucket,
                    host: Accumulator::default(),
                    apps: BTreeMap::new(),
                });
            }
            let minute = self.minute.as_mut().unwrap();
            minute.host.add(&host_fields);
            for (app, fields) in &apps {
                minute.apps.entry(app.clone()).or_default().add(fields);
            }
            self.latest = json!({"t":now,"h":round_fields(host_fields),"a":apps.into_iter().map(|(k, f)| (k, round_fields(f))).collect::<BTreeMap<_,_>>(),"replicas":replicas});
        }
        self.host = Some(host);
        self.containers = Some(containers);
        Ok(())
    }
}
pub struct History<'a>(pub &'a Metrics);
impl serde::Serialize for History<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeSeq;
        let mut sequence = serializer.serialize_seq(Some(self.0.retained.len()))?;
        for point in &self.0.retained {
            let value = Metrics::decode(point).map_err(serde::ser::Error::custom)?;
            sequence.serialize_element(&value)?;
        }
        sequence.end()
    }
}
fn n(value: &Value) -> f64 {
    value.as_f64().unwrap_or(0.0)
}
fn rounded(value: f64) -> f64 {
    if !value.is_finite() {
        0.0
    } else if value.abs() >= 100.0 {
        value.round()
    } else {
        (value * 100.0).round() / 100.0
    }
}
fn round_fields(mut fields: Fields) -> Fields {
    for value in fields.values_mut() {
        *value = rounded(*value);
    }
    fields
}
fn read_host() -> Host {
    let read = |path| std::fs::read_to_string(path).unwrap_or_default();
    let stat = read("/proc/stat");
    let mem = read("/proc/meminfo");
    let disks = read("/proc/diskstats");
    let networks = read("/proc/net/dev");
    let mut cpu = [0.0; 8];
    for (i, value) in stat
        .lines()
        .next()
        .unwrap_or("")
        .split_whitespace()
        .skip(1)
        .take(8)
        .enumerate()
    {
        cpu[i] = value.parse().unwrap_or(0.0);
    }
    let memory: BTreeMap<_, f64> = mem
        .lines()
        .filter_map(|line| {
            let (name, fields) = line.split_once(':')?;
            Some((
                name,
                fields.split_whitespace().next()?.parse::<f64>().ok()? * 1024.0,
            ))
        })
        .collect();
    let (mut disk_read, mut disk_write, mut rx, mut tx) = (0.0, 0.0, 0.0, 0.0);
    for line in disks.lines() {
        let fields: Vec<_> = line.split_whitespace().collect();
        if fields.len() < 10 || !whole_disk(fields[2]) {
            continue;
        }
        disk_read += fields[5].parse::<f64>().unwrap_or(0.0) * 512.0;
        disk_write += fields[9].parse::<f64>().unwrap_or(0.0) * 512.0;
    }
    for line in networks.lines().skip(2) {
        let Some((name, values)) = line.split_once(':') else {
            continue;
        };
        let name = name.trim();
        if ["lo", "docker", "veth", "br-"]
            .iter()
            .any(|prefix| name.starts_with(prefix))
        {
            continue;
        }
        let fields: Vec<_> = values.split_whitespace().collect();
        if fields.len() < 9 {
            continue;
        }
        rx += fields[0].parse::<f64>().unwrap_or(0.0);
        tx += fields[8].parse::<f64>().unwrap_or(0.0);
    }
    Host {
        at: Instant::now(),
        cpu,
        mem_total: *memory.get("MemTotal").unwrap_or(&0.0),
        mem_avail: *memory
            .get("MemAvailable")
            .or_else(|| memory.get("MemFree"))
            .unwrap_or(&0.0),
        disk_read,
        disk_write,
        rx,
        tx,
    }
}
fn whole_disk(name: &str) -> bool {
    for prefix in ["sd", "vd", "xvd"] {
        if let Some(tail) = name.strip_prefix(prefix) {
            return !tail.is_empty() && tail.bytes().all(|c| c.is_ascii_lowercase());
        }
    }
    if let Some(tail) = name.strip_prefix("nvme") {
        return tail.split_once('n').is_some_and(|(a, b)| {
            !a.is_empty() && !b.is_empty() && a.bytes().chain(b.bytes()).all(|c| c.is_ascii_digit())
        });
    }
    name.strip_prefix("mmcblk")
        .is_some_and(|tail| !tail.is_empty() && tail.bytes().all(|c| c.is_ascii_digit()))
}
fn host_sample(a: &Host, b: &Host) -> Fields {
    let seconds = b.at.duration_since(a.at).as_secs_f64().max(0.001);
    let delta = b.cpu.iter().sum::<f64>() - a.cpu.iter().sum::<f64>();
    BTreeMap::from([
        (
            "cpu".into(),
            if delta > 0.0 {
                (100.0 - 100.0 * ((b.cpu[3] + b.cpu[4]) - (a.cpu[3] + a.cpu[4])) / delta)
                    .clamp(0.0, 100.0)
            } else {
                0.0
            },
        ),
        (
            "steal".into(),
            if delta > 0.0 {
                100.0 * (b.cpu[7] - a.cpu[7]).max(0.0) / delta
            } else {
                0.0
            },
        ),
        ("memUsed".into(), (b.mem_total - b.mem_avail).max(0.0)),
        ("memTotal".into(), b.mem_total),
        (
            "diskRead".into(),
            (b.disk_read - a.disk_read).max(0.0) / seconds,
        ),
        (
            "diskWrite".into(),
            (b.disk_write - a.disk_write).max(0.0) / seconds,
        ),
        ("netRx".into(), (b.rx - a.rx).max(0.0) / seconds),
        ("netTx".into(), (b.tx - a.tx).max(0.0) / seconds),
    ])
}
async fn read_containers(
    client: &reqwest::Client,
    projects: &BTreeMap<String, CopyIdentity>,
    system: &BTreeSet<String>,
) -> anyhow::Result<Containers> {
    let list: Vec<Value> = response_json(
        client
            .get("http://localhost/containers/json")
            .send()
            .await?,
        4 << 20,
    )
    .await?;
    let pending = list.into_iter().filter_map(|container| {
        let id = container["Id"].as_str()?.to_owned();
        if id.is_empty() || !id.bytes().all(|c| c.is_ascii_hexdigit()) {
            return None;
        }
        let name = container["Names"][0]
            .as_str()
            .unwrap_or(&id)
            .trim_start_matches('/');
        let project = container["Labels"]["com.docker.compose.project"].as_str();
        let copy = project.and_then(|p| projects.get(p)).cloned();
        let app = copy
            .as_ref()
            .map(|c| c.app.clone())
            .or_else(|| system.contains(name).then(|| "_system".into()))?;
        Some(async move {
            let mut out = Container {
                id: id.clone(),
                app,
                copy,
                cpu: None,
                mem: 0.0,
                rx: 0.0,
                tx: 0.0,
                read: 0.0,
                write: 0.0,
            };
            let fetch = async {
                response_json::<Value>(
                    client
                        .get(format!(
                            "http://localhost/containers/{id}/stats?stream=false&one-shot=true"
                        ))
                        .send()
                        .await?,
                    256 << 10,
                )
                .await
            };
            if let Ok(stats) = fetch.await {
                out.cpu = Some(n(&stats["cpu_stats"]["cpu_usage"]["total_usage"]));
                let memory = &stats["memory_stats"];
                let inactive = memory["stats"]
                    .get("inactive_file")
                    .unwrap_or(&memory["stats"]["total_inactive_file"]);
                out.mem = (n(&memory["usage"]) - n(inactive)).max(0.0);
                for network in stats["networks"]
                    .as_object()
                    .into_iter()
                    .flat_map(|m| m.values())
                {
                    out.rx += n(&network["rx_bytes"]);
                    out.tx += n(&network["tx_bytes"]);
                }
                for io in stats["blkio_stats"]["io_service_bytes_recursive"]
                    .as_array()
                    .into_iter()
                    .flatten()
                {
                    match io["op"]
                        .as_str()
                        .unwrap_or("")
                        .to_ascii_lowercase()
                        .as_str()
                    {
                        "read" => out.read += n(&io["value"]),
                        "write" => out.write += n(&io["value"]),
                        _ => (),
                    }
                }
            }
            out
        })
    });
    let list = stream::iter(pending).buffer_unordered(4).collect().await;
    Ok(Containers {
        at: Instant::now(),
        list,
    })
}
fn app_sample(a: &Containers, b: &Containers, replicas: bool) -> BTreeMap<String, Fields> {
    let seconds = b.at.duration_since(a.at).as_secs_f64().max(0.001);
    let before: BTreeMap<_, _> = a.list.iter().map(|c| (c.id.as_str(), c)).collect();
    let unavailable: BTreeSet<_> = b
        .list
        .iter()
        .filter_map(|c| {
            let copy = c.copy.as_ref()?;
            let old = before.get(c.id.as_str());
            (c.cpu.is_none() || old.is_none_or(|o| o.cpu.is_none() || o.copy != c.copy))
                .then_some(copy.key.as_str())
        })
        .collect();
    let mut apps: BTreeMap<String, Fields> = BTreeMap::new();
    for container in &b.list {
        let key = if replicas {
            let Some(copy) = &container.copy else {
                continue;
            };
            if unavailable.contains(copy.key.as_str()) {
                continue;
            }
            &copy.key
        } else {
            &container.app
        };
        let fields = apps.entry(key.clone()).or_insert_with(|| {
            ["cpu", "mem", "netRx", "netTx", "diskRead", "diskWrite"]
                .into_iter()
                .map(|s| (s.into(), 0.0))
                .collect()
        });
        *fields.get_mut("mem").unwrap() += container.mem;
        let Some(old) = before.get(container.id.as_str()) else {
            continue;
        };
        let (Some(cpu), Some(previous)) = (container.cpu, old.cpu) else {
            continue;
        };
        for (field, value) in [
            ("cpu", (cpu - previous).max(0.0) / 1e7),
            ("netRx", (container.rx - old.rx).max(0.0)),
            ("netTx", (container.tx - old.tx).max(0.0)),
            ("diskRead", (container.read - old.read).max(0.0)),
            ("diskWrite", (container.write - old.write).max(0.0)),
        ] {
            *fields.get_mut(field).unwrap() += value / seconds;
        }
    }
    apps
}
fn replica_sample(a: &Containers, b: &Containers) -> BTreeMap<String, Value> {
    let copies: BTreeMap<_, _> = b
        .list
        .iter()
        .filter_map(|c| c.copy.as_ref().map(|copy| (copy.key.as_str(), copy)))
        .collect();
    app_sample(a, b, true)
        .into_iter()
        .map(|(key, fields)| {
            let copy = copies[key.as_str()];
            let value =
                json!({"app":copy.app,"replica":copy.replica,"values":round_fields(fields)});
            (key, value)
        })
        .collect()
}
