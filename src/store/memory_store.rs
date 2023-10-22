use std::sync::Arc;
use dashmap::DashMap;

use probe::Probe;

use crate::probe::probe;

#[derive(Clone,Debug, Default)]
pub struct MemoryStore  {
    probes: DashMap<String, Probe>,
}

impl MemoryStore {
    pub fn new() -> Self {
        MemoryStore {
            probes: DashMap::new(),
        }
    }

    pub fn save_probe(&self, probe: &Probe) -> Option<Probe> {
        self.probes.insert(probe.get_probe_id().to_string(), probe.clone());

        Some(probe.to_owned())

    }

    pub fn get_probe(&self, probe_id: &String) -> Option<Probe> {
        self.probes.get(probe_id)
            .map(|probe | probe.to_owned())
    }
}