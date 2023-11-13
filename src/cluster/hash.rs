use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use fnv::FnvHasher;

pub fn hash(key: String) -> usize {
    //todo make the hash instances static
    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);

    let mut fnv_hasher = FnvHasher::default();
    hasher.finish().hash(&mut fnv_hasher);

    fnv_hasher.finish() as usize
}