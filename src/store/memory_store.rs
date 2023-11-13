use dashmap::DashMap;

use probe::Probe;
use crate::grpc::service::probe_sync::{ProbeProto};

use crate::probe::probe;

#[derive(Clone, Debug, Default)]
pub struct MemoryStore {
    pub(crate) probes: DashMap<String, Probe>,
}

impl MemoryStore {
    pub fn new() -> Self {
        MemoryStore {
            probes: DashMap::new(),
        }
    }

    pub fn save_probe(&self, probe: &Probe){
        self.probes.insert(probe.get_probe_id().to_string(), probe.clone());
    }

    pub fn get_probe(&self, probe_id: &String) -> Option<Probe> {
        self.probes.get(probe_id)
            .map(|probe| probe.to_owned())
    }

    //todo implement zero copy here
    pub fn serialise(&self) -> Vec<ProbeProto> {
        self.probes.iter()
            .map(|entry| entry.value().clone().to_probe_data())
            .collect()
    }

    pub fn de_serialise_and_update(&self, serialised_data: Vec<ProbeProto>) {
        //todo should we need to update only if the current timestamp is greater than the existing timestamp
        for data in serialised_data {
            let probe = Probe::from_probe_proto(data);
            self.probes.insert(probe.probe_id.clone(), probe);
        }
    }
}