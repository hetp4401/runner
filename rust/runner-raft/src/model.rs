use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Member {
    pub id: u64,
    pub url: String,
    #[serde(default)]
    pub label: String,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Identity {
    pub node: u64,
    pub id: u64,
    pub boot: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Registry {
    pub generation: String,
    pub slots: BTreeMap<u64, u64>,
    pub nodes: BTreeMap<u64, Identity>,
}

#[derive(Serialize, Deserialize)]
pub struct Manifest {
    pub generation: String,
    pub term: u64,
    pub members: Vec<Identity>,
}

#[derive(Serialize, Deserialize)]
pub struct Change {
    pub kind: String,
    pub member: Identity,
    #[serde(default)]
    pub previous: u64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct View {
    pub id: u64,
    pub node: u64,
    pub leader: u64,
    pub leader_node: u64,
    pub term: u64,
    pub role: String,
    pub applied: u64,
    pub boot: String,
    pub members: usize,
    pub quorum: usize,
    pub identity_valid: bool,
    pub phase: String,
    pub generation: String,
    pub registry: Registry,
    #[serde(default, deserialize_with = "null_vec")]
    pub voters: Vec<u64>,
    #[serde(default, deserialize_with = "null_vec")]
    pub outgoing: Vec<u64>,
    #[serde(default, deserialize_with = "null_vec")]
    pub learners: Vec<u64>,
}

fn null_vec<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Vec<u64>, D::Error> {
    Ok(Option::<Vec<u64>>::deserialize(d)?.unwrap_or_default())
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Envelope {
    pub sequence: u64,
    pub data: String,
}
