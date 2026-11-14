use crate::control::{n, s};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, VecDeque},
    sync::Arc,
};
struct Payload {
    compressed: Vec<u8>,
    raw: usize,
}
impl Payload {
    fn value(&self) -> anyhow::Result<Value> {
        Ok(serde_json::from_slice(
            &lz4_flex::decompress_size_prepended(&self.compressed)?,
        )?)
    }
}
struct Entry {
    sequence: u64,
    at: u64,
    kind: String,
    data: Arc<Payload>,
}
struct Report {
    at: u64,
    data: Arc<Payload>,
}
pub struct Tail {
    entries: VecDeque<Entry>,
    reports: BTreeMap<String, Report>,
    transitions: BTreeMap<String, Value>,
    sequence: u64,
    bytes: usize,
    report_bytes: usize,
    max_bytes: usize,
}
impl Default for Tail {
    fn default() -> Self {
        Self {
            entries: VecDeque::new(),
            reports: BTreeMap::new(),
            transitions: BTreeMap::new(),
            sequence: 0,
            bytes: 0,
            report_bytes: 0,
            max_bytes: 8 << 20,
        }
    }
}
impl Tail {
    pub fn append(&mut self, kind: &str, data: &Value, at: u64) -> anyhow::Result<()> {
        if !matches!(kind, "report" | "event" | "transition") {
            return Ok(());
        }
        let raw = serde_json::to_vec(data)?;
        if raw.len() > 1_000_000 {
            return Ok(());
        }
        let payload = Arc::new(Payload {
            compressed: lz4_flex::compress_prepend_size(&raw),
            raw: raw.len(),
        });
        self.sequence += 1;
        self.bytes += payload.compressed.len();
        self.entries.push_back(Entry {
            sequence: self.sequence,
            at,
            kind: kind.into(),
            data: payload.clone(),
        });
        while self.bytes > self.max_bytes || self.entries.len() > 2048 {
            let old = self.entries.pop_front().unwrap();
            self.bytes -= old.data.compressed.len();
        }
        let run = s(data, "run");
        if kind == "report"
            && !run.is_empty()
            && run.len() <= 200
            && self.reports.get(run).is_none_or(|r| at >= r.at)
        {
            if let Some(old) = self.reports.remove(run) {
                self.report_bytes -= old.data.raw;
            }
            self.report_bytes += payload.raw;
            self.reports
                .insert(run.into(), Report { at, data: payload });
            while self.report_bytes > self.max_bytes || self.reports.len() > 128 {
                let key = self
                    .reports
                    .iter()
                    .min_by_key(|(_, r)| r.at)
                    .unwrap()
                    .0
                    .clone();
                let old = self.reports.remove(&key).unwrap();
                self.report_bytes -= old.data.raw;
            }
        }
        if kind == "transition" {
            self.transition(data);
        }
        Ok(())
    }
    pub fn transition(&mut self, data: &Value) {
        let id = s(data, "id");
        if id.is_empty() || id.len() > 200 {
            return;
        }
        self.transitions.insert(id.into(), data.clone());
        while self.transitions.len() > 30 {
            let key = self
                .transitions
                .iter()
                .min_by_key(|(_, v)| n(v, "at"))
                .unwrap()
                .0
                .clone();
            self.transitions.remove(&key);
        }
    }
    pub fn read(&self, after: u64) -> anyhow::Result<Value> {
        let first = self
            .entries
            .front()
            .map(|e| e.sequence)
            .unwrap_or(self.sequence + 1);
        let mut events = vec![];
        let mut bytes = 0;
        for e in self.entries.iter().filter(|e| e.sequence > after).take(64) {
            if !events.is_empty() && bytes + e.data.raw > 2 << 20 {
                break;
            }
            bytes += e.data.raw;
            events.push(
                json!({"sequence":e.sequence,"at":e.at,"kind":e.kind,"data":e.data.value()?}),
            );
        }
        Ok(
            json!({"sequence":self.sequence,"first":first,"gap":after<first.saturating_sub(1),"events":events}),
        )
    }
    pub fn reports(&self) -> anyhow::Result<Vec<Value>> {
        let mut reports = self
            .reports
            .values()
            .map(|r| Ok(json!({"at":r.at,"body":r.data.value()?})))
            .collect::<anyhow::Result<Vec<_>>>()?;
        reports.sort_by_key(|r| n(r, "at"));
        Ok(reports)
    }
    pub fn history(&self) -> Value {
        let mut values = self.transitions.values().cloned().collect::<Vec<_>>();
        values.sort_by_key(|v| std::cmp::Reverse(n(v, "at")));
        json!(values)
    }
    pub fn stats(&self) -> Value {
        json!({"sequence":self.sequence,"entries":self.entries.len(),"bytes":self.bytes,"reportBytes":self.report_bytes,"transitions":self.transitions.len(),"compression":"lz4"})
    }
}
