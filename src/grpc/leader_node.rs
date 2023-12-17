use std::fmt;
use std::sync::Arc;

use tonic::Status;

use crate::grpc::node::Node;
use crate::probe::probe::Probe;
use crate::store::memory_store::MemoryStore;

#[derive(Clone, Debug)]
pub struct LeaderNode {

    pub(crate) node: Arc<Node>,
    //todo handle delta data removal
    pub(crate) delta_data: Option<MemoryStore>,
}

impl fmt::Display for LeaderNode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", &self.node)
    }
}

impl LeaderNode {
    pub(crate) async fn write_probe_to_store_and_delta(&self, partition_id: usize, probe: &Probe) -> Result<(), Status> {
        self.write_to_delta_if_exists(probe);
        self.node.write_probe_to_store(partition_id, true, probe).await
    }

    pub fn write_to_delta(&self, probe: &Probe) {
        match &self.delta_data {
            None => {
                log::error!("Not writing as delta data wasn't present, {}",probe.event_id);
            }
            Some(store) => {
                store.save_probe(probe)
            }
        }
    }
    pub fn write_to_delta_if_exists(&self, probe: &Probe) {
        if self.delta_data.is_some() {
            log::info!("Written to delta {}",probe.event_id);
            self.write_to_delta(probe);
        }
    }

    pub fn remove_delta_data(&mut self) {
        self.delta_data = None;
    }
}


