use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use crate::store::ProbeId;

pub(crate) fn hash(probe_id: ProbeId) -> u64 {
    let mut hasher = DefaultHasher::new();
    probe_id.hash(&mut hasher);
    return hasher.finish();
}