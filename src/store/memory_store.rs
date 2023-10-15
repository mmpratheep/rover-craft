use std::collections::HashMap;
use std::hash::Hash;
use std::sync::Arc;

use parking_lot::RwLock;

use probe::Probe;

use crate::probe::probe;

type Probes = HashMap<String, Probe>;

#[derive(Clone)]
pub struct MemoryStore  {
    probes: Arc<RwLock<Probes>>,
}

impl MemoryStore {
    pub fn new() -> Self {
        MemoryStore {
            probes: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn save_probe(&self, probe: &Probe) -> Option<Probe> {
        self.probes.write()
            .insert(probe.get_probe_id().to_string(), probe.clone());

        Some(probe.to_owned())

    }

    pub fn get_probe(&self, probe_id: &String) -> Option<Probe> {
        self.probes.read()
            .get(probe_id)
            .map(|probe | probe.to_owned())
    }
}