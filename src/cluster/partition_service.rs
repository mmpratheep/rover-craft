use std::sync::Arc;
use tonic::{Request, Response, Status};
use crate::cluster::hash::hash;
use crate::grpc::leader_node::LeaderNode;
use crate::grpc::node::Node;
use crate::grpc::node_status::NodeStatus;
use crate::grpc::service::cluster::{AliveNotServingRequest, Empty};
use crate::grpc::service::cluster::partition_server::Partition;
use crate::store::memory_store::MemoryStore;


#[derive(Debug, Default)]
pub(crate) struct PartitionService {
    //todo store the reference of Nodes in leader/follower nodes
    leader_nodes: Vec<LeaderNode>,
    follower_nodes: Vec<Arc<Node>>,
    partition_size: usize,
}

impl PartitionService {
    pub fn initialise_partitions(&mut self, nodes: Vec<Node>) {
        //distributes partition evenly across nodes with replication factor n-1
        let replica = nodes.len() - 1;
        let size = nodes.len() * replica;
        let mut leader_nodes: Vec<LeaderNode> = Vec::with_capacity(size);
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

    // fn remove_node(nodes: &mut Vec<Arc<Node>>, node: &Node) {
    //     for i in 0..nodes.len() {
    //         if node.host_name == nodes.get(i).unwrap().host_name {
    //             nodes.remove(i);
    //             return;
    //         }
    //     }
    // }

    pub fn get_partition_nodes(&self, probe_id: &String) -> (Option<&LeaderNode>, Option<&Arc<Node>>) {
        let hash = hash(probe_id.clone());
        let partition_id = hash % self.partition_size;
        (self.leader_nodes.get(partition_id), self.follower_nodes.get(partition_id))
    }

    pub(crate) fn balance_partitions_and_write_delta_data(&mut self) {
        for i in 0..self.leader_nodes.len() {
            let leader_node = self.leader_nodes.get(i).unwrap();
            let follower_node = self.follower_nodes.get(i).unwrap();
            self.reassign_leader_partition_and_write_delta_data(i, leader_node, follower_node);

            self.handle_follower_delta_data_write(i, follower_node);
        }
    }

    fn reassign_leader_partition_and_write_delta_data(&mut self, i: usize, leader_node: &LeaderNode, follower_node: &Arc<Node>) {
        if leader_node.node.node_status == NodeStatus::Dead {
            let mut delta_data: Option<MemoryStore> = None;
            if follower_node.is_current_node() {
                delta_data = Some(MemoryStore::new());
            }
            self.leader_nodes[i] = LeaderNode { node: follower_node.clone(), delta_data: None };
        }
    }

    fn handle_follower_delta_data_write(&mut self, i: usize, follower_node: &Arc<Node>) {
        if follower_node.node_status == NodeStatus::Dead {
            if follower_node.is_current_node() {
                let mut leader_node = self.leader_nodes.get_mut(i).unwrap();
                leader_node.delta_data = Some(Arc::new(MemoryStore::new()));
            }
        }
    }
}

#[tonic::async_trait]
impl Partition for PartitionService {
    async fn make_node_alive_not_serving(&self, request: Request<AliveNotServingRequest>) -> Result<Response<Empty>, Status> {
        todo!()
    }

    async fn make_node_alive_serving(&self, request: Request<Empty>) -> Result<Response<Empty>, Status> {
        todo!()
    }
}


fn clone_except_given_value(nodes: &Vec<Node>, node: &Node) -> Vec<Node> {
    let mut temp_nodes = nodes.clone();
    temp_nodes.retain(|n| n.host_name != node.host_name);
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

fn assign_values_for_leader(replica: usize, leader_nodes: &mut Vec<LeaderNode>, node: &Node, mut current_index: usize) {
    for _ in 0..replica {
        leader_nodes.insert(current_index, LeaderNode {
            node: Arc::from(node.clone()),
            delta_data: None,
        });
        current_index += 1;
    }
}
