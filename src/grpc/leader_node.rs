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

impl LeaderNode {
    pub(crate) async fn write_probe_to_store(&self, probe: &Probe) -> Result<(), Status> {
        self.delta_data.map(|it| it.save_probe(probe));
        self.node.write_probe_to_store(probe)
    }
}


