use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use crate::hash::hash;
use crate::store::ProbeId;

#[test]
fn run(){
    println!("from unit test")
}




#[test]
fn should_generate_hash(){
    let probe_id = ProbeId {
        id: "PRB3422242242112300000000000000000000011111112222334445556666123131782131231231233437687687623423412".to_string()
    };

    assert_eq!(11564296154245411618, hash(probe_id))
}

