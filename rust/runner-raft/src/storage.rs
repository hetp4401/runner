//! Volatile log plus an applied-state snapshot. No disk votes survive a process.
use protobuf::Message as _;
use raft::{Error, GetEntriesContext, RaftState, Result, Storage, StorageError, prelude::*};
use std::sync::{Arc, RwLock};

#[derive(Clone)]
pub struct VolatileStorage(Arc<RwLock<State>>);
impl Default for VolatileStorage {
    fn default() -> Self {
        Self::new()
    }
}
#[derive(Default)]
struct State {
    hard: HardState,
    snapshot: Snapshot,
    // The first entry is a sentinel retaining the compacted index and term.
    entries: Vec<Entry>,
}
impl VolatileStorage {
    pub fn new() -> Self {
        Self(Arc::new(RwLock::new(State {
            entries: vec![Entry::default()],
            ..State::default()
        })))
    }
    pub fn hard_state(&self, hard: HardState) {
        self.0.write().unwrap().hard = hard;
    }
    pub fn commit(&self, index: u64) {
        self.0.write().unwrap().hard.commit = index;
    }
    pub fn apply_snapshot(&self, snapshot: Snapshot) -> Result<()> {
        let mut state = self.0.write().unwrap();
        if snapshot.get_metadata().index <= state.snapshot.get_metadata().index {
            return Err(Error::Store(StorageError::SnapshotOutOfDate));
        }
        let mut sentinel = Entry::default();
        sentinel.index = snapshot.get_metadata().index;
        sentinel.term = snapshot.get_metadata().term;
        state.entries = vec![sentinel];
        state.snapshot = snapshot;
        Ok(())
    }
    pub fn append(&self, entries: &[Entry]) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        let mut state = self.0.write().unwrap();
        let first = state.entries[0].index + 1;
        let entries =
            &entries[(first.saturating_sub(entries[0].index) as usize).min(entries.len())..];
        if entries.is_empty() {
            return Ok(());
        }
        let offset = (entries[0].index - state.entries[0].index) as usize;
        if offset > state.entries.len() {
            return Err(Error::Store(StorageError::Unavailable));
        }
        state.entries.truncate(offset);
        state.entries.extend_from_slice(entries);
        Ok(())
    }
    pub fn compact(&self, applied: u64, conf: &ConfState, data: Vec<u8>) -> Result<()> {
        let mut state = self.0.write().unwrap();
        if applied <= state.snapshot.get_metadata().index {
            return Ok(());
        }
        let offset = (applied - state.entries[0].index) as usize;
        if offset >= state.entries.len() {
            return Err(Error::Store(StorageError::Unavailable));
        }
        let term = state.entries[offset].term;
        state.entries.drain(..offset);
        state.entries[0].data.clear();
        state.entries[0].context.clear();
        let mut snapshot = Snapshot::default();
        snapshot.data = data.into();
        snapshot.mut_metadata().index = applied;
        snapshot.mut_metadata().term = term;
        snapshot.mut_metadata().set_conf_state(conf.clone());
        state.snapshot = snapshot;
        // Membership logs are tiny. Reclaim any exceptional high-water capacity.
        if state.entries.capacity() > 256 && state.entries.len() < 32 {
            state.entries.shrink_to(32);
        }
        Ok(())
    }
}
impl Storage for VolatileStorage {
    fn initial_state(&self) -> Result<RaftState> {
        let state = self.0.read().unwrap();
        Ok(RaftState {
            hard_state: state.hard.clone(),
            conf_state: state.snapshot.get_metadata().get_conf_state().clone(),
        })
    }
    fn entries(
        &self,
        low: u64,
        high: u64,
        max_size: impl Into<Option<u64>>,
        _: GetEntriesContext,
    ) -> Result<Vec<Entry>> {
        let state = self.0.read().unwrap();
        let offset = state.entries[0].index;
        if low <= offset {
            return Err(Error::Store(StorageError::Compacted));
        }
        if high > offset + state.entries.len() as u64 || low > high {
            return Err(Error::Store(StorageError::Unavailable));
        }
        let mut total = 0_u64;
        let limit = max_size.into().unwrap_or(u64::MAX);
        let mut out = Vec::new();
        for entry in &state.entries[(low - offset) as usize..(high - offset) as usize] {
            let size = entry.compute_size() as u64;
            if !out.is_empty() && size > limit.saturating_sub(total) {
                break;
            }
            total += size;
            out.push(entry.clone());
        }
        Ok(out)
    }
    fn term(&self, index: u64) -> Result<u64> {
        let state = self.0.read().unwrap();
        let offset = state.entries[0].index;
        if index < offset {
            return Err(Error::Store(StorageError::Compacted));
        }
        state
            .entries
            .get((index - offset) as usize)
            .map(|e| e.term)
            .ok_or(Error::Store(StorageError::Unavailable))
    }
    fn first_index(&self) -> Result<u64> {
        Ok(self.0.read().unwrap().entries[0].index + 1)
    }
    fn last_index(&self) -> Result<u64> {
        Ok(self.0.read().unwrap().entries.last().unwrap().index)
    }
    fn snapshot(&self, request_index: u64, _: u64) -> Result<Snapshot> {
        let state = self.0.read().unwrap();
        if request_index > state.snapshot.get_metadata().index {
            return Err(Error::Store(StorageError::SnapshotTemporarilyUnavailable));
        }
        Ok(state.snapshot.clone())
    }
}
