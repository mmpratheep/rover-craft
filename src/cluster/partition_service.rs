use std::ops::Deref;
use std::sync::Arc;
use std::sync::RwLock;

use crate::cluster::hash::hash;
use crate::grpc::leader_node::LeaderNode;
use crate::grpc::node::Node;
use crate::grpc::node_manager::NodeManager;
use crate::grpc::node_ref::NodeRef;
use crate::grpc::service::cluster::{AnnounceAliveNotServingRequest, AnnounceAliveServingRequest};
use crate::probe::probe::Probe;
use crate::store::memory_store::MemoryStore;

#[derive(Debug, Default)]
pub(crate) struct PartitionService {
    //todo store the reference of Nodes in leader/follower nodes
    //todo check and remove locks because the partition-service itself has a lock
    leader_nodes: RwLock<Vec<LeaderNode>>,
    follower_nodes: RwLock<Vec<Arc<Node>>>,
    partition_size: usize,
    nodes: NodeManager,
}

impl PartitionService {
    pub fn new(nodes: NodeManager) -> Self {
        let (leaders, followers, partition_size) =
            Self::initialise_partitions(nodes.nodes.clone());
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
    pub async fn get_leader_and_write_delta(&self, partition_id: usize, probe: &Probe) -> Arc<Node> {
        let leaders;
        {
            leaders = self.leader_nodes.read().unwrap();
            let leader_ref = leaders.get(partition_id).expect("No leader to get");
            leader_ref.write_to_delta_if_exists(probe);
            return leader_ref.node.clone();
        }
    }

    pub fn get_leader_partition_ids(&self, hostname: &String) -> Vec<u32> {
        let leaders;
        {
            leaders = self.leader_nodes.read().unwrap();
            let leader_partitions = leaders.iter()
                .enumerate()
                .filter(|(_, &ref leader_node)| *leader_node.node.node_ref.host_name == *hostname)
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
                .filter(|(_, &ref follower_node)| *follower_node.node_ref.host_name == *hostname)
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

    pub fn initialise_partitions(mut nodes: Vec<Arc<NodeRef>>) -> (Vec<LeaderNode>, Vec<Arc<Node>>, usize) {
        //distributes partition evenly across nodes with replication factor n-1
        let replica = nodes.len() - 1;
        let partition_size = nodes.len() * replica;
        let mut leader_nodes: Vec<LeaderNode> = Vec::with_capacity(partition_size);
        let mut follower_nodes: Vec<Arc<Node>> = Vec::with_capacity(partition_size);
        if replica < nodes.len() {
            let mut index: usize = 0;
            for node in &nodes {
                assign_values_for_leader(replica, &mut leader_nodes, node.clone(), index);
                let mut temp_nodes = clone_except_given_value(&nodes, &node);
                assign_values_for_replica(replica, &mut follower_nodes, &mut temp_nodes, index);
                index += replica;
            }
        }
        return (leader_nodes, follower_nodes, partition_size);
    }

    pub fn get_leader_delta_data(&self, partition_id: usize) -> Option<MemoryStore> {
        log::debug!("Came here...");
        let leaders = self.leader_nodes.read().unwrap();
        {
            log::debug!("Also here...");
            return leaders.get(partition_id).unwrap().delta_data.clone()
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
            log::info!("leader partition id: {}", partition_id);
            log::info!("follower partition id: {}", partition_id);
            let leader_ref = leaders.get(partition_id).expect("No leader to get");
            let follower_ref = followers.get(partition_id).expect("No follower to get");

            // Release the locks here, ensuring that the references are valid
            (leader_ref.clone(), follower_ref.clone(), partition_id)
        }
    }

    fn print_leader(&self, log_text: &str) {
        let leaders: Vec<String> = self.leader_nodes.read().unwrap().iter().map(|it| it.to_string()).collect();
        log::info!("{} leaders: {:?}", log_text, leaders);
    }

    fn print_follower(&self, log_text: &str) {
        let followers: Vec<String> = self.follower_nodes.read().unwrap().iter().map(|it| it.to_string()).collect();
        log::info!("{} followers: {:?}", log_text, followers);
    }

    pub(crate) async fn balance_partitions_and_write_delta_data(&self) {
        self.print_leader("Before rebalance:");
        self.print_follower("Before rebalance:");
        let leader_nodes = self.leader_nodes.read().unwrap().clone();

        let follower_nodes = self.follower_nodes.read().unwrap().clone();

        //todo make sure it's happening only once
        for i in 0..leader_nodes.len() {
            let leader_node = leader_nodes.get(i).unwrap();
            let follower_node = follower_nodes.get(i).unwrap();
            log::info!("leader {}", leader_node.node.node_ref.host_name);
            log::info!("follower {}", follower_node.node_ref.host_name);

            if leader_node.node.is_node_down() {
                log::info!("down leader {}", leader_node.node.node_ref.host_name);
                let  delta_data: Option<MemoryStore> = if follower_node.is_current_node() {Some(MemoryStore::new())} else {None};
                let mut guard = self.leader_nodes.write().unwrap();
                guard[i] = LeaderNode { node: follower_node.clone(), delta_data };
            }

            if follower_node.is_node_down() {
                log::info!("down follower {}", follower_node.node_ref.host_name);
                log::info!("leader {}", leader_node.node.node_ref.host_name);
                if leader_node.node.is_current_node() {
                    match self.leader_nodes.write() {
                        Ok(mut nodes) => {
                            nodes[i].delta_data = Some(MemoryStore::new());
                        }
                        Err(err) => {
                            log::error!("Failed to acquire write lock to create delta-data map {}",err);
                        }
                    }
                }
            }
        }
        self.print_leader("After rebalance:");
        self.print_follower("After rebalance:");
    }

    pub(crate) async fn make_node_alive_and_not_serving(&self, req_data: &AnnounceAliveNotServingRequest) {
        self.print_leader("Before alive and not serving,");
        self.print_follower("Before alive and not serving,");
        let node_host_name = req_data.host_name.clone();
        self.nodes.make_node_alive_and_not_serving(&node_host_name);
        match self.leader_nodes.write() {
            Ok(mut l_nodes) => {
                log::info!("Got write lock for leader nodes");
                match self.follower_nodes.write() {
                    Ok(mut f_nodes) => {
                        log::info!("Got write lock for follower nodes");
                        for i in req_data.leader_partitions.clone() {
                            let index = i as usize;
                            //todo imp I think we can skip this
                            f_nodes[index] = l_nodes[index].node.clone();
                            //when node is back, we need to keep the delta data there in the leader, and move the data to the follower and create connection to from leader partition
                            l_nodes[index].node = Arc::new(Node::new(
                                self.nodes.get_node(&node_host_name).unwrap()));
                            //todo check whether it handles properly for the second node assignment
                        }
                    }
                    Err(err) => {
                        log::error!("Failed to get write lock for followers {}",err)
                    }
                }
            }
            Err(err) => {
                log::error!("Failed to get write lock for leaders {}",err)
            }
        };

        self.print_leader("After alive and not serving,");
        self.print_follower("After alive and not serving,");

    }


    pub(crate) async fn make_node_alive_serving(&self, req_data: &AnnounceAliveServingRequest) {
        self.print_leader("Before alive and serving,");
        self.print_follower("Before alive and serving,");
        let node_host_name = req_data.host_name.clone();
        self.nodes.make_node_alive_and_not_serving(&node_host_name);
        match self.leader_nodes.write() {
            Ok(mut l_nodes) => {
                for i in req_data.leader_partitions.clone() {
                    l_nodes[i as usize].remove_delta_data();
                }
                for i in req_data.follower_partitions.clone() {
                    l_nodes[i as usize].remove_delta_data()
                }
            }
            Err(err) => {
                log::error!("Failed to get write lock for leaders {}",err)
            }
        }
        self.print_leader("After alive and serving,");
        self.print_follower("After alive and serving,");
    }

    pub(crate) async fn get_peers(&self) -> Vec<&Arc<NodeRef>> {
        self.nodes.get_peers()
    }

    pub(crate) fn make_node_dead(&self, node_host: &String) {
        self.nodes.make_node_dead(node_host)
    }

    pub(crate) fn current_node(&self) -> &Arc<NodeRef> {
        self.nodes.get_current_node().unwrap()
    }

    pub (crate) fn is_current_node_dead(&self) -> bool {
        // println!("current node status in health check {:?}", self.nodes.get_current_node().unwrap().node_status.read().unwrap().deref());
        self.nodes.get_current_node().unwrap().is_dead()
    }

    pub (crate) fn  is_current_node_down(&self) -> bool {
        self.nodes.is_current_node_down()
    }

    pub (crate) async fn announce_alive_and_serving(&self) {
        let current_node = self.nodes.get_current_node().unwrap();
        let current_node_leader_partition_ids = self.get_leader_partition_ids(&current_node.host_name);
        let current_node_follower_partition_ids = self.get_follower_partition_ids(&current_node.host_name);
        for peer in self.nodes.get_peers() {
            peer.announce_me_alive_and_serving(
                &current_node.host_name,
                current_node_leader_partition_ids.clone(),
                current_node_follower_partition_ids.clone()).await;
        }
    }

    pub async fn handle_recovery(&self) {
        for peer_node in self.nodes.get_peers() {
            let alive_peer_node = self.nodes.get_node(&peer_node.host_name).unwrap().clone();

            let current_node = self.nodes.get_current_node().unwrap();
            log::info!("making current node alive");
            self.nodes.make_node_alive_and_serving(&current_node.host_name);

            let current_node_leader_partitions = self.get_leader_partition_ids(&current_node.host_name);
            log::info!("Announce alive and not serving, for partitions: {:?}", current_node_leader_partitions);
            alive_peer_node.announce_me_alive_not_serving(&current_node.host_name, &current_node_leader_partitions).await;
        }

        log::info!("Handling recovery");
        self.recover_current_node().await;
        log::info!("Announce alive and serving");
        self.announce_alive_and_serving().await
    }

    pub async fn recover_current_node(&self) {
        let current_node = self.nodes.get_current_node().unwrap();
        let current_node_leader_partition_ids = self.get_leader_partition_ids(&current_node.host_name);
        let current_node_follower_partition_ids = self.get_follower_partition_ids(&current_node.host_name);

        //todo IMP optimise: get delta data in parallel from other nodes
        log::info!("Catching up the leader partitions");

        for partition_id in current_node_leader_partition_ids.clone().iter() {
            let leader_s_follower_partition = self.get_follower_node(partition_id.clone() as usize).await;
            log::info!("leader partition_id: {}, follower partition host: {}", &partition_id, &leader_s_follower_partition.node_ref.host_name);
            let result = leader_s_follower_partition
                .get_delta_data_from_peer(partition_id.clone()).await;

            match result {
                Ok(delta_data) => {
                    log::info!("Received delta data, updating the partition");
                    let leader = self.get_leader_node(partition_id.clone() as usize).await;
                    leader.update_with_delta_data(delta_data.into_inner());
                    log::info!("updated the partition");
                }
                Err(err) => {
                    log::error!("Err Delta data leader: {} : {}", leader_s_follower_partition.node_ref.host_name, err)
                }
            }
        }
        log::info!("Catching up the follower partitions");

        for partition_id in current_node_follower_partition_ids.clone().iter() {
            let follower_s_leader_partitions = self.get_leader_node(partition_id.clone() as usize).await;
            log::info!("partition_id: {}, partition: {}", &partition_id, &follower_s_leader_partitions.node_ref.host_name);
            let result = follower_s_leader_partitions
                .get_delta_data_from_peer(partition_id.clone()).await;

            match result {
                Ok(delta_data) => {
                    log::info!("Received delta data, updating the partition");
                    let follower = self.get_follower_node(partition_id.clone() as usize).await;
                    follower.update_with_delta_data(delta_data.into_inner());
                    log::info!("updated the partition");
                }
                Err(err) => {
                    log::error!("Err Delta data follower: {}", err)
                }
            }
        }
    }

}


fn clone_except_given_value(nodes: &Vec<Arc<NodeRef>>, node: &NodeRef) -> Vec<Arc<NodeRef>> {
    let mut temp_nodes = nodes.clone();
    temp_nodes.retain(|n| n.host_name != node.host_name);
    return temp_nodes;
}

fn assign_values_for_replica(replica: usize, follower_nodes: &mut Vec<Arc<Node>>, temp_nodes: &mut Vec<Arc<NodeRef>>, mut index: usize) {
    for _ in 0..replica {
        let remaining_node = temp_nodes.get(0).unwrap().clone();
        follower_nodes.insert(index, Arc::new(Node::new(remaining_node.clone())));
        temp_nodes.remove(0);
        index += 1;
    }
}

fn assign_values_for_leader(replica: usize, leader_nodes: &mut Vec<LeaderNode>, node: Arc<NodeRef>, mut current_index: usize) {
    for _ in 0..replica {
        leader_nodes.insert(current_index, LeaderNode {
            node: Arc::new(Node::new(node.clone())),
            delta_data: None,
        });
        current_index += 1;
    }
}

