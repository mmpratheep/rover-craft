use std::sync::{Arc, RwLock};
use log::error;
use tonic::{Request, Response, Status};
use crate::cluster::hash::hash;
use crate::grpc::leader_node::LeaderNode;
use crate::grpc::node::Node;
use crate::grpc::node_status::NodeStatus;
use crate::grpc::nodes::NodeManager;
use crate::grpc::service::cluster::{AliveAndServingRequest, AliveNotServingRequest, Empty};
use crate::grpc::service::cluster::partition_server::Partition;
use crate::store::memory_store::MemoryStore;


#[derive(Debug, Default)]
pub(crate) struct PartitionService {
    //todo store the reference of Nodes in leader/follower nodes
    leader_nodes: RwLock<Vec<LeaderNode>>,
    follower_nodes: RwLock<Vec<Arc<Node>>>,
    partition_size: usize,
    nodes: NodeManager,
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
        self.leader_nodes = RwLock::new(leader_nodes);
        self.follower_nodes = RwLock::new(follower_nodes);
    }

    pub fn get_partition_nodes(&self, probe_id: &String) -> (LeaderNode, Arc<Node>) {
        let leaders;
        let followers;
        {
            let hash = hash(probe_id.clone());
            let partition_id = hash % self.partition_size;
            leaders = self.leader_nodes.read().unwrap();
            followers = self.follower_nodes.read().unwrap();
            let leader_ref = leaders.get(partition_id).expect("No leader to get");
            let follower_ref = followers.get(partition_id).expect("No follower to get");

            // Release the locks here, ensuring that the references are valid
            (leader_ref.clone(), follower_ref.clone())
        }
    }

    pub(crate) fn balance_partitions_and_write_delta_data(&mut self) {
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

#[tonic::async_trait]
impl Partition for PartitionService {
    async fn make_node_alive_not_serving(&self, request: Request<AliveNotServingRequest>) -> Result<Response<Empty>, Status> {
        //todo
        let req_data = request.into_inner();
        let node_host_name = req_data.host_name;
        self.nodes.make_node_alive_and_not_serving(&node_host_name);
        for i in req_data.partitions {
            match self.leader_nodes.write() {
                Ok(mut l_nodes) => {
                    match self.follower_nodes.write() {
                        Ok(mut f_nodes) => {
                            let index = i as usize;
                            f_nodes[index] = l_nodes[index].node.clone();
                            l_nodes[index].node = self.nodes.get_node(node_host_name.clone()).unwrap();
                            //todo check whether it handles properly for the second node assignment
                        }
                        Err(err) => {
                            error!("Failed to get write lock for followers {}",err)
                        }
                    }
                }
                Err(err) => {
                    error!("Failed to get write lock for leaders {}",err)
                }
            }
        };
        Ok(Response::new(Empty {}))
    }

    async fn make_node_alive_serving(&self, request: Request<AliveAndServingRequest>) -> Result<Response<Empty>, Status> {
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
