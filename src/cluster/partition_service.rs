use std::sync::Arc;
use tonic::{Request, Response, Status};
use crate::cluster::hash::hash;
use crate::grpc::node::Node;
use crate::grpc::service::cluster::{Empty, Partitions};
use crate::grpc::service::cluster::partition_server::Partition;


#[derive(Debug, Default)]
pub(crate) struct PartitionService {
    //todo store the reference of Nodes in leader/follower nodes
    leader_nodes: Vec<Arc<Node>>,
    follower_nodes: Vec<Arc<Node>>,
    partition_size: usize,
}

impl PartitionService {
    pub fn initialise_partitions(&mut self, nodes: Vec<Node>) {
        //distributes partition evenly across nodes with replication factor n-1
        let replica = nodes.len() - 1;
        let size = nodes.len() * replica;
        let mut leader_nodes: Vec<Arc<Node>> = Vec::with_capacity(size);
        let mut follower_nodes: Vec<Arc<Node>> = Vec::with_capacity(size);
        if replica < nodes.len() {
            let mut index: usize = 0;
            for node in &nodes {
                assign_values_for_leader(replica, &mut leader_nodes, node, index);
                let mut temp_nodes = clone_except_given_value(&nodes, &node);
                assign_values_for_replica(replica, &mut follower_nodes, &mut temp_nodes, index);
                index += replica;
            }
        }
        self.leader_nodes = leader_nodes;
        self.follower_nodes = follower_nodes;
    }
    fn remove_node(nodes: &mut Vec<Node>, node: &Node) {
        for i in 0..nodes.len() {
            if node.address == nodes.get(i).unwrap().address {
                nodes.remove(i);
                return;
            }
        }
    }

    pub fn get_partition_nodes(&self, probe_id: &String) -> (Option<&Arc<Node>>, Option<&Arc<Node>>) {
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


fn clone_except_given_value(nodes: &Vec<Node>, node: &Node) -> Vec<Node> {
    let mut temp_nodes = nodes.clone();
    temp_nodes.retain(|n| n.address != node.address);
    return temp_nodes;
}

fn assign_values_for_replica(replica: usize, follower_nodes: &mut Vec<Arc<Node>>, temp_nodes: &mut Vec<Node>, mut index: usize) {
    for _ in 0..replica {
        let remaining_node = temp_nodes.get(0).unwrap().clone();
        follower_nodes.insert(index, Arc::new(remaining_node));
        temp_nodes.remove(0);
        index += 1;
    }
}

fn assign_values_for_leader(replica: usize, leader_nodes: &mut Vec<Arc<Node>>, node: &Node, mut current_index: usize) {
    for _ in 0..replica {
        leader_nodes.insert(current_index, Arc::new(node.clone()));
        current_index += 1;
    }
}
