use std::collections::HashMap;
use std::hash::Hash;
use std::sync::Arc;

use parking_lot::RwLock;

use probe::Probe;

use crate::probe::probe;

type Probes = HashMap<String, Probe>;

#[derive(Clone)]
pub struct Store {
    probes: Arc<RwLock<Probes>>,
}

impl Store {
    pub fn new() -> Self {
        Store {
            probes: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}