use crate::{
    control::{Control, f, n, s},
    error::http,
    model::*,
    recovery::array,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{collections::BTreeMap, sync::Arc};

#[derive(Default)]
struct Summary(BTreeMap<String, (f64, u64)>);
impl Summary {
    fn add(&mut self, v: &Value) {
        for (key, value) in v.as_object().into_iter().flatten() {
            let Some(value) = value.as_f64().filter(|v| v.is_finite()) else {
                continue;
            };
            let entry = self.0.entry(key.clone()).or_insert((
                if key.ends_with("Max") {
                    f64::NEG_INFINITY
                } else {
                    0.
                },
                0,
            ));
            if key.ends_with("Max") {
                entry.0 = entry.0.max(value);
            } else {
                entry.0 += value;
                entry.1 += 1;
            }
        }
    }
    fn finish(self) -> Value {
        Value::Object(
            self.0
                .into_iter()
                .map(|(k, (sum, count))| {
                    let v = if count > 0 { sum / count as f64 } else { sum };
                    let v = if v.abs() >= 100. {
                        (v + 0.5).floor()
                    } else {
                        (v * 100. + 0.5).floor() / 100.
                    };
                    (k, json!(v))
                })
                .collect(),
        )
    }
}
#[derive(Default)]
struct MachineSummary {
    h: Summary,
    a: BTreeMap<String, Summary>,
}
#[derive(Default)]
struct FleetSummary(BTreeMap<String, MachineSummary>);
impl FleetSummary {
    fn add(&mut self, row: &Value) {
        for (machine, value) in row.as_object().into_iter().flatten() {
            let m = self.0.entry(machine.clone()).or_default();
            m.h.add(&value["h"]);
            for (name, value) in value["a"].as_object().into_iter().flatten() {
                m.a.entry(name.clone()).or_default().add(value);
            }
        }
    }
    fn finish(self) -> Value {
        Value::Object(self.0.into_iter().map(|(k,m)|(k,json!({"h":m.h.finish(),"a":m.a.into_iter().map(|(k,v)|(k,v.finish())).collect::<BTreeMap<_,_>>()}))).collect())
    }
}
pub fn merge_fleet(rows: &[Value]) -> Value {
    let mut summary = FleetSummary::default();
    for row in rows {
        summary.add(row);
    }
    summary.finish()
}
#[derive(Default, Clone)]
pub struct Series(Vec<f64>);
impl Serialize for Series {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.collect_seq(self.0.iter().map(|v| v.is_finite().then_some(v)))
    }
}
impl<'de> Deserialize<'de> for Series {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct Visitor;
        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value = Series;
            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("numeric series")
            }
            fn visit_seq<A: serde::de::SeqAccess<'de>>(
                self,
                mut seq: A,
            ) -> Result<Series, A::Error> {
                let mut out = Vec::with_capacity(seq.size_hint().unwrap_or(0).min(4096));
                while let Some(value) = seq.next_element::<Option<f64>>()? {
                    out.push(value.unwrap_or(f64::NAN));
                }
                Ok(Series(out))
            }
        }
        d.deserialize_seq(Visitor)
    }
}
#[derive(Default, Serialize, Deserialize)]
pub struct Columns {
    h: BTreeMap<String, Series>,
    a: BTreeMap<String, BTreeMap<String, Series>>,
}
#[derive(Serialize, Deserialize)]
pub struct History {
    pub step: u64,
    pub t: Vec<u64>,
    pub m: BTreeMap<String, Columns>,
}
fn range(name: &str) -> anyhow::Result<(u64, u64, &'static str)> {
    Ok(match name {
        "1h" => (60 * MIN, MIN, "metrics_1m"),
        "6h" => (6 * 60 * MIN, 2 * MIN, "metrics_1m"),
        "24h" => (24 * 60 * MIN, 10 * MIN, "metrics_10m"),
        "7d" => (7 * 24 * 60 * MIN, 60 * MIN, "metrics_1h"),
        "30d" => (30 * 24 * 60 * MIN, 240 * MIN, "metrics_1h"),
        _ => return Err(http(400, "range is one of 1h, 6h, 24h, 7d, 30d")),
    })
}
fn numeric(v: &Value) -> Value {
    Value::Object(
        v.as_object()
            .into_iter()
            .flatten()
            .take(128)
            .filter(|(_, v)| v.as_f64().is_some_and(f64::is_finite))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect(),
    )
}
fn applications(v: &Value) -> Value {
    Value::Object(
        v.as_object()
            .into_iter()
            .flatten()
            .take(1024)
            .filter(|(k, v)| k.len() <= 128 && v.is_object())
            .map(|(k, v)| (k.clone(), numeric(v)))
            .collect(),
    )
}
impl Control {
    pub fn speaks_for(&self, r: &Run, now: u64) -> bool {
        let runs = self
            .live_runs(now)
            .into_iter()
            .filter(|x| x.machine == r.machine)
            .collect::<Vec<_>>();
        runs.iter()
            .filter(|x| x.ready > 0)
            .max_by_key(|x| x.started)
            .or_else(|| runs.iter().max_by_key(|x| x.started))
            .is_some_and(|x| x.id == r.id)
    }
    pub fn take_metrics(&mut self, r: &Run, metrics: &Value, now: u64) -> anyhow::Result<()> {
        if !metrics.is_object() || r.retire > 0 || !self.speaks_for(r, now) {
            return Ok(());
        }
        if metrics["live"]["h"].is_object() {
            let h = numeric(&metrics["live"]["h"]);
            let mut live = json!({"run":r.id,"started":r.started,"t":metrics["live"]["t"].as_u64().filter(|t|*t>0&&*t<=now+MIN).unwrap_or(now),"h":h,"a":applications(&metrics["live"]["a"])});
            if metrics["live"]["replicas"].is_object() {
                live["replicas"] = metrics["live"]["replicas"].clone();
            }
            self.live_metrics.insert(r.machine, live);
            let samples = self.samples.entry(r.machine).or_default();
            samples.push(json!({"t":now,"cpu":f(&h,"cpu"),"mem":if f(&h,"memTotal")>0.{100.*f(&h,"memUsed")/f(&h,"memTotal")}else{0.}}));
            samples.retain(|x| now.saturating_sub(n(x, "t")) <= 5 * MIN);
            if samples.len() > 300 {
                samples.drain(..samples.len() - 300);
            }
        }
        let mut mine = self.minutes.get(&r.id).cloned().unwrap_or_default();
        let recent = array(&metrics["recent"]);
        let minutes = array(&metrics["minutes"]);
        let summaries = recent
            .iter()
            .skip(recent.len().saturating_sub(10))
            .chain(minutes.iter().skip(minutes.len().saturating_sub(60)));
        for m in summaries {
            let Some(t) = m["t"]
                .as_u64()
                .filter(|t| *t % MIN == 0 && *t <= now && now - *t <= 48 * 60 * MIN)
            else {
                continue;
            };
            if !m["h"].is_object() {
                continue;
            }
            let h = numeric(&m["h"]);
            let a = applications(&m["a"]);
            let machine = r.machine.to_string();
            let old = self
                .pending_metrics
                .get(&t)
                .map(runner_core::json_size)
                .transpose()?
                .unwrap_or(0);
            let slot = self.pending_metrics.entry(t).or_insert_with(|| json!({}));
            slot[&machine] = json!({"h":h,"a":a});
            let bytes = runner_core::json_size(slot)?;
            self.pending_metrics_bytes = self.pending_metrics_bytes.saturating_sub(old) + bytes;
            if now - t <= 10 * MIN && !mine.iter().any(|m| n(m, "t") == t) {
                let total = f(&h, "memTotal");
                mine.push(json!({"t":t,"cpu":f(&h,"cpu"),"steal":f(&h,"steal"),"mem":if total>0.{100.*f(&h,"memUsed")/total}else{0.},"memTotal":total,"a":a}));
            }
        }
        mine.sort_by_key(|m| n(m, "t"));
        if mine.len() > 10 {
            mine.drain(..mine.len() - 10);
        }
        if !mine.is_empty() {
            self.minutes.insert(r.id.clone(), mine);
        }
        // Flush completed minutes early under pressure; keep pending raw JSON bounded.
        if self.pending_metrics_bytes > 4 << 20 {
            self.flush_metrics(now)?;
            while self.pending_metrics_bytes > 4 << 20 {
                let Some((_, slot)) = self.pending_metrics.pop_first() else {
                    break;
                };
                self.pending_metrics_bytes = self
                    .pending_metrics_bytes
                    .saturating_sub(runner_core::json_size(&slot)?);
            }
        }
        Ok(())
    }
    pub fn flush_metrics(&mut self, now: u64) -> anyhow::Result<()> {
        let times = self
            .pending_metrics
            .keys()
            .filter(|t| now.saturating_sub(**t) >= 150_000)
            .copied()
            .collect::<Vec<_>>();
        if times.is_empty() {
            return Ok(());
        }
        for t in times {
            let slot = self.pending_metrics.remove(&t).unwrap();
            self.pending_metrics_bytes = self
                .pending_metrics_bytes
                .saturating_sub(runner_core::json_size(&slot)?);
            let old = self
                .store
                .rows("SELECT data FROM metrics_1m WHERE t=?", &[json!(t)])?;
            let mut data = old
                .first()
                .map(|r| serde_json::from_str::<Value>(s(r, "data")))
                .transpose()?
                .unwrap_or_else(|| json!({}));
            data.as_object_mut()
                .unwrap()
                .extend(slot.as_object().unwrap().clone());
            self.store.exec("INSERT INTO metrics_1m(t,data) VALUES(?,?) ON CONFLICT(t) DO UPDATE SET data=excluded.data",&[json!(t),json!(serde_json::to_string(&data)?)])?;
            let ten = t - t % (10 * MIN);
            if ten <= self.setting_number("rolled10") {
                self.rollup_bucket("metrics_10m", "metrics_1m", ten, 10 * MIN)?;
                let hour = t - t % (60 * MIN);
                if hour <= self.setting_number("rolled60") {
                    self.rollup_bucket("metrics_1h", "metrics_10m", hour, 60 * MIN)?;
                }
            }
        }
        self.store
            .trim_metrics(self.en("METRICS_HISTORY_BYTES", 24 << 20))?;
        Ok(())
    }
    pub fn setting_number(&self, key: &str) -> u64 {
        self.settings
            .get(key)
            .and_then(|s| s.parse().ok())
            .unwrap_or(0)
    }
    pub fn rollup_bucket(
        &mut self,
        table: &str,
        from: &str,
        b: u64,
        step: u64,
    ) -> anyhow::Result<()> {
        let mut merge = FleetSummary::default();
        let mut count = 0;
        self.store.scan(
            &format!("SELECT data FROM {from} WHERE t>=? AND t<?"),
            &[json!(b), json!(b + step)],
            |row| {
                let value = serde_json::from_str(s(&row, "data"))?;
                merge.add(&value);
                count += 1;
                Ok(())
            },
        )?;
        if count > 0 {
            self.store.exec(&format!("INSERT INTO {table}(t,data) VALUES(?,?) ON CONFLICT(t) DO UPDATE SET data=excluded.data"),&[json!(b),json!(serde_json::to_string(&merge.finish())?)])?;
        }
        Ok(())
    }
    pub fn rollup_metrics(&mut self, now: u64) -> anyhow::Result<()> {
        let ten = 10 * MIN;
        let hour = 60 * MIN;
        let mut last10 = self.setting_number("rolled10");
        if last10 == 0 {
            last10 = (now - now % ten).saturating_sub(2 * ten);
        }
        for _ in 0..300 {
            let b = last10 + ten;
            if b + ten + 150_000 > now {
                break;
            }
            self.rollup_bucket("metrics_10m", "metrics_1m", b, ten)?;
            last10 = b;
        }
        if self.settings.get("rolled10") != Some(&last10.to_string()) {
            self.set_setting("rolled10", last10)?;
        }
        let mut last60 = self.setting_number("rolled60");
        if last60 == 0 {
            let oldest = self
                .store
                .rows("SELECT MIN(t) AS t FROM metrics_10m", &[])?
                .first()
                .and_then(|v| v["t"].as_u64())
                .unwrap_or(now);
            last60 = (oldest - oldest % hour).saturating_sub(hour);
        }
        for _ in 0..300 {
            let b = last60 + hour;
            if b + hour > last10 + ten {
                break;
            }
            self.rollup_bucket("metrics_1h", "metrics_10m", b, hour)?;
            last60 = b;
        }
        if self.settings.get("rolled60") != Some(&last60.to_string()) {
            self.set_setting("rolled60", last60)?;
        }
        self.store
            .trim_metrics(self.en("METRICS_HISTORY_BYTES", 24 << 20))?;
        Ok(())
    }
    pub fn history(&self, name: &str, now: u64) -> anyhow::Result<History> {
        let (span, step, table) = range(name)?;
        let end = now - now % step;
        let from = end.saturating_sub(span);
        let t = (from..=end).step_by(step as usize).collect::<Vec<_>>();
        let mut history = History {
            step,
            t,
            m: BTreeMap::new(),
        };
        let mut bytes = 0usize;
        let budget = self.en("METRICS_QUERY_BYTES", 16 << 20) as usize;
        let mut bucket = None;
        let mut summary = FleetSummary::default();
        let mut single = Value::Null;
        let mut count = 0;
        let flush = |b: u64,
                     summary: &mut FleetSummary,
                     single: &mut Value,
                     count: &mut usize,
                     history: &mut History,
                     bytes: &mut usize|
         -> anyhow::Result<()> {
            let row = if *count == 1 {
                single.take()
            } else {
                std::mem::take(summary).finish()
            };
            *summary = FleetSummary::default();
            *count = 0;
            let i = ((b - from) / step) as usize;
            if i >= history.t.len() {
                return Ok(());
            }
            for (machine, value) in row.as_object().into_iter().flatten() {
                let columns = history.m.entry(machine.clone()).or_default();
                let mut write = |fields: &mut BTreeMap<String, Series>,
                                 data: &Value|
                 -> anyhow::Result<()> {
                    for (key, v) in data.as_object().into_iter().flatten() {
                        let Some(v) = v.as_f64() else {
                            continue;
                        };
                        if !fields.contains_key(key) {
                            *bytes += history.t.len() * 8;
                            if *bytes > budget {
                                return Err(http(
                                    413,
                                    "metrics range exceeds its memory limit; choose a shorter range",
                                ));
                            }
                            fields.insert(key.clone(), Series(vec![f64::NAN; history.t.len()]));
                        }
                        fields.get_mut(key).unwrap().0[i] = v;
                    }
                    Ok(())
                };
                write(&mut columns.h, &value["h"])?;
                for (app, values) in value["a"].as_object().into_iter().flatten() {
                    write(columns.a.entry(app.clone()).or_default(), values)?;
                }
            }
            Ok(())
        };
        self.store.scan(
            &format!("SELECT t,data FROM {table} WHERE t>=? AND t<=? ORDER BY t"),
            &[json!(from), json!(end)],
            |row| {
                let time = n(&row, "t");
                let b = time - time % step;
                if let Some(previous) = bucket {
                    if previous != b {
                        flush(
                            previous,
                            &mut summary,
                            &mut single,
                            &mut count,
                            &mut history,
                            &mut bytes,
                        )?;
                    }
                }
                bucket = Some(b);
                let value: Value = serde_json::from_str(s(&row, "data"))?;
                count += 1;
                if count == 1 {
                    single = value;
                } else {
                    if count == 2 {
                        summary.add(&single);
                        single = Value::Null;
                    }
                    summary.add(&value);
                }
                Ok(())
            },
        )?;
        if let Some(b) = bucket {
            flush(
                b,
                &mut summary,
                &mut single,
                &mut count,
                &mut history,
                &mut bytes,
            )?;
        }
        Ok(history)
    }
    pub fn live_now(&self, now: u64) -> Value {
        Value::Object(
            self.live_metrics
                .iter()
                .filter_map(|(m, x)| {
                    let r = self.runs.get(s(x, "run"))?;
                    if now.saturating_sub(n(x, "t")) > self.live_ms {
                        return None;
                    }
                    let mut value = x.clone();
                    value["ready"] = json!(r.ready > 0);
                    value["status"] = json!(r.status);
                    value["label"] = json!(r.label);
                    Some((m.to_string(), value))
                })
                .collect(),
        )
    }
    pub fn metrics_response(&mut self, name: &str, now: u64) -> anyhow::Result<Vec<u8>> {
        let (_, step, _) = range(name)?;
        let ttl = if step >= 60 * MIN {
            5 * MIN
        } else if step >= 10 * MIN {
            MIN
        } else {
            15_000
        };
        let history = if let Some((_, bytes)) = self
            .metrics_cache
            .get(name)
            .filter(|(at, _)| now.saturating_sub(*at) <= ttl)
        {
            let raw = lz4_flex::decompress_size_prepended(bytes)?;
            serde_json::from_slice::<History>(&raw)?
        } else {
            let history = self.history(name, now)?;
            let raw = serde_json::to_vec(&history)?;
            if raw.len() <= 16 << 20 {
                let compressed = lz4_flex::compress_prepend_size(&raw);
                if compressed.len() <= 2 << 20 {
                    self.metrics_cache.clear();
                    self.metrics_cache
                        .insert(name.into(), (now, Arc::from(compressed)));
                }
            }
            history
        };
        #[derive(Serialize)]
        struct Response<'a> {
            now: u64,
            range: &'a str,
            servers: usize,
            projects: Vec<Value>,
            live: Value,
            #[serde(flatten)]
            history: &'a History,
        }
        let response = Response {
            now,
            range: name,
            servers: self.live_machines(now).len(),
            projects: self
                .projects
                .values()
                .map(|p| self.describe(p))
                .collect::<anyhow::Result<Vec<_>>>()?,
            live: self.live_now(now),
            history: &history,
        };
        Ok(serde_json::to_vec(&response)?)
    }
}
