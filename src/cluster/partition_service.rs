use tonic::{Request, Response, Status};
use crate::cluster::hash::hash;
use crate::grpc::node::Node;
use crate::grpc::service::cluster::{Empty, Partitions};
use crate::grpc::service::cluster::partition_server::Partition;


#[derive(Debug, Default)]
pub(crate) struct PartitionService {
    leader_nodes: Vec<Node>,
    follower_nodes: Vec<Node>,
    partition_size: usize,
}

impl PartitionService {
    pub fn initialise_partitions(nodes: Vec<Node>, size: usize) {
        let mut leader_nodes: Vec<&Node> = Vec::with_capacity(size);
        let mut follower_nodes: Vec<&Node> = Vec::with_capacity(size);
        let mut index: usize = 0;
        for i in 0..size {
            leader_nodes.insert(i, nodes.get(index % size).unwrap());
            index = index + 1;
        }
    }

    pub fn get_partition_nodes(&self, probe_id: &String) -> (Option<&Node>, Option<&Node>) {
        let hash = hash(probe_id.clone());
        let partition_id = hash % self.partition_size;
        (self.leader_nodes.get(partition_id), self.follower_nodes.get(partition_id))
    }
}

#[tonic::async_trait]
impl Partition for PartitionService {
    async fn get_partitions_from_leader(&self, request: Request<Empty>) -> Result<Response<Partitions>, Status> {
        todo!()
    }
}
