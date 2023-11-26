use std::sync::{Arc};
use std::sync::RwLock;
use log::{error};
use crate::cluster::hash::hash;
use crate::grpc::leader_node::LeaderNode;
use crate::grpc::node::Node;
use crate::grpc::node_status::NodeStatus;
use crate::grpc::nodes::NodeManager;
use crate::store::memory_store::MemoryStore;


#[derive(Debug, Default)]
pub(crate) struct PartitionService {
    //todo store the reference of Nodes in leader/follower nodes
    //todo check and remove locks because the partition-service itself has a lock
    pub(crate) leader_nodes: RwLock<Vec<LeaderNode>>,
    pub follower_nodes: RwLock<Vec<Arc<Node>>>,
    pub partition_size: usize,
    pub nodes: Arc<NodeManager>,
}

impl PartitionService {
    pub fn new(nodes: Arc<NodeManager>) -> Self {
        let (leaders, followers, partition_size) =
            Self::initialise_partitions(nodes.nodes.clone());
        println!("leaders: {:?}", leaders);
        println!("followers: {:?}", followers);
        println!("partition size: {:?}", partition_size);
        PartitionService {
            leader_nodes: RwLock::new(leaders),
            follower_nodes: RwLock::new(followers),
            partition_size,
            nodes,
        }
    }

    pub async fn get_leader_node(&self, partition_id: usize) -> Arc<Node> {
        let leaders;
        {
            leaders = self.leader_nodes.read().unwrap();
            let leader_ref = leaders.get(partition_id).expect("No leader to get");
            return leader_ref.node.clone();
        }
    }

    pub fn get_leader_partition_ids(&self, hostname: &String) -> Vec<u32> {
        let leaders;
        {
            leaders = self.leader_nodes.read().unwrap();
            let leader_partitions = leaders.iter()
                .enumerate()
                .filter(|(_,&ref leader_node)| *leader_node.node.host_name == *hostname)
                .map(|(index, _)| index as u32)
                .collect();
            leader_partitions
        }
    }
    pub fn get_follower_partition_ids(&self, hostname: &String) -> Vec<u32> {
        let followers;
        {
            followers = self.follower_nodes.read().unwrap();
            let follower_partitions = followers.iter()
                .enumerate()
                .filter(|(_,&ref follower_node)| *follower_node.host_name == *hostname)
                .map(|(index, _)| index as u32)
                .collect();
            follower_partitions
        }
    }

    pub async fn get_follower_node(&self, partition_id: usize) -> Arc<Node> {
        let followers;
        {
            followers = self.follower_nodes.read().unwrap();
            let followers_ref = followers.get(partition_id).expect("No follower to get");
            //todo remove clone
            return followers_ref.clone();
        }
    }

    pub fn initialise_partitions(mut nodes: Vec<Arc<Node>>) -> (Vec<LeaderNode>, Vec<Arc<Node>>, usize) {
        //distributes partition evenly across nodes with replication factor n-1
        let replica = nodes.len() - 1;
        let partition_size = nodes.len() * replica;
        let mut leader_nodes: Vec<LeaderNode> = Vec::with_capacity(partition_size);
        let mut follower_nodes: Vec<Arc<Node>> = Vec::with_capacity(partition_size);
        if replica < nodes.len() {
            let mut index: usize = 0;
            for node in &nodes {
                assign_values_for_leader(replica, &mut leader_nodes, node, index);
                let mut temp_nodes = clone_except_given_value(&nodes, &node);
                assign_values_for_replica(replica, &mut follower_nodes, &mut temp_nodes, index);
                index += replica;
            }
        }
        return (leader_nodes, follower_nodes, partition_size);
    }

    pub async fn get_leader_delta_data(&self, partition_id: usize) -> Option<Arc<MemoryStore>> {
        let leaders = self.leader_nodes.read().unwrap();
        {
            let leader_ref = leaders.get(partition_id)
                .expect("No leader to get to copy delta data for partition");
            return leader_ref.clone().delta_data;
        }
    }

    pub async fn get_partition_nodes(&self, probe_id: &String) -> (LeaderNode, Arc<Node>, usize) {
        let leaders;
        let followers;
        {
            let hash = hash(probe_id.clone());
            let partition_id = hash % self.partition_size;
            leaders = self.leader_nodes.read().unwrap();
            followers = self.follower_nodes.read().unwrap();
            println!("leader partition id: {}", partition_id);
            println!("follower partition id: {}", partition_id);
            let leader_ref = leaders.get(partition_id).expect("No leader to get");
            let follower_ref = followers.get(partition_id).expect("No follower to get");

            // Release the locks here, ensuring that the references are valid
            (leader_ref.clone(), follower_ref.clone(), partition_id)
        }
    }

    pub(crate) async fn balance_partitions_and_write_delta_data(&mut self) {
        let leader_nodes = self.leader_nodes.read().unwrap();

        let follower_nodes = self.follower_nodes.read().unwrap();

        //todo make sure it's happening only once
        for i in 0..leader_nodes.len() {
            let leader_node = leader_nodes.get(i).unwrap();
            let follower_node = follower_nodes.get(i).unwrap();

            if leader_node.node.node_status == NodeStatus::Dead {
                let mut delta_data: Option<MemoryStore> = None;
                if follower_node.is_current_node() {
                    delta_data = Some(MemoryStore::new());
                }
                self.leader_nodes.write().unwrap()[i] = LeaderNode { node: follower_node.clone(), delta_data: None };
            }

            if follower_node.node_status == NodeStatus::Dead {
                if follower_node.is_current_node() {
                    match self.leader_nodes.write() {
                        Ok(mut nodes) => {
                            nodes[i].delta_data = Some(Arc::new(MemoryStore::new()));
                        }
                        Err(err) => {
                            error!("Failed to acquire write lock to create delta-data map {}",err);
                        }
                    }
                }
            }
        }
    }
}

fn clone_except_given_value(nodes: &Vec<Arc<Node>>, node: &Node) -> Vec<Arc<Node>> {
    let mut temp_nodes = nodes.clone();
    temp_nodes.retain(|n| n.host_name != node.host_name);
    return temp_nodes;
}

fn assign_values_for_replica(replica: usize, follower_nodes: &mut Vec<Arc<Node>>, temp_nodes: &mut Vec<Arc<Node>>, mut index: usize) {
    for _ in 0..replica {
        let remaining_node = temp_nodes.get(0).unwrap().clone();
        follower_nodes.insert(index, remaining_node);
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

