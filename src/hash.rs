use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use crate::store::Probe;

pub(crate) fn hash(probe: Probe) -> u64 {
    let mut hasher = DefaultHasher::new();
    probe.hash(&mut hasher);
    return hasher.finish();
}