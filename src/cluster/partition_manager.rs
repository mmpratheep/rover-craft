use std::sync::{Arc};
use tokio::sync::RwLock as TrwLock;
use log::error;
use crate::cluster::partition_service::PartitionService;
use crate::grpc::leader_node::LeaderNode;
use crate::grpc::node::Node;
use crate::grpc::node_status::NodeStatus;
use crate::probe::probe::Probe;
use crate::store::memory_store::MemoryStore;

#[derive(Debug, Default)]
pub struct PartitionManager {
    pub(crate) partition_service: Arc<TrwLock<PartitionService>>,
}

impl PartitionManager {
    pub async fn read_probe_from_partition(&self, partition_id: usize, is_leader: bool, probe_id: String) -> Option<Probe> {
        let partition = self.get_partition(partition_id, is_leader);
        partition
            .await.read_probe_from_store(partition_id, true, &probe_id).await.unwrap()
    }
    pub async fn write_probe_to_partition(&self, partition_id: usize, is_leader: bool, probe: Probe) {
        let partition = self.get_partition(partition_id, is_leader);
        partition
            .await.write_probe_to_store(partition_id, true, &probe).await.expect("internal probe write failed");
    }
    async fn get_partition(&self, partition_id: usize, is_leader: bool) -> Arc<Node> {
        if is_leader { self.partition_service.read().await.get_leader_node(partition_id).await } else { self.partition_service.read().await.get_follower_node(partition_id).await }
    }

    // pub async fn make_node_down(&self, node: Node) {
    //     if node.is_node_not_down() {
    //         node.make_node_down();
    //         //todo notice other nodes saying that one node is down, also make sure that the current node is not down
    //         self.partition_service.write().await.balance_partitions_and_write_delta_data().await
    //         //todo once the node is back, need to publish a message to other nodes saying that the node will be the leader for the existing leader partitions
    //         //todo also move it's state to aliveAndServing and it can receive the writes for the partition, post the catchup it will send another request saying moved back to aliveAndServing
    //     }
    // }


    pub async fn read_probe(&self, probe_id: String) -> Option<Probe> {
        //todo clone
        let (leader_node, follower_node, partition_id) = self.partition_service.read().await
            .get_partition_nodes(&probe_id).await;
        println!("leader: {:?}", leader_node);
        println!("follower: {:?}", follower_node);
        //todo if leader_node.unwrap().node.node_status == NodeStatus::AliveServing -> then read from that
        //todo else read from the follower partition
        return match leader_node.node.node_status() {
            NodeStatus::AliveServing => {
                Self::read_from_leader_first(&probe_id, leader_node, follower_node, partition_id).await
            }
            NodeStatus::AliveNotServing=> {
                let result = follower_node.read_probe_from_store(partition_id, false, &probe_id).await;
                return match result {
                    Ok(val) => {
                        val
                    }
                    Err(error) => {
                        println!("Error while getting data from follower when leader is alive and not serving {}", error);
                        None
                    }
                }
            }
            NodeStatus::Dead => {
                let result = follower_node.read_probe_from_store(partition_id, false, &probe_id).await;
                return match result {
                    Ok(val) => {
                        val
                    }
                    Err(error) => {
                        println!("Error while getting data from follower when leader is dead {}", error);
                        None
                    }
                }
            }
        }
    }

    async fn read_from_leader_first(probe_id: &String, leader_node: LeaderNode, follower_node: Arc<Node>, partition_id: usize) -> Option<Probe> {
        let result = leader_node.node.read_probe_from_store(partition_id, true, &probe_id).await;

        return match result {
            Ok(probe) => {
                probe
            }
            Err(err) => {
                error!("Exception {}", err);

                //todo explicitly tell the follower node to rebalance the partition by sending the dead node ip, then make the request to the follower to get the probe from partition
                let fallback_result = follower_node.read_probe_from_store(partition_id, false, &probe_id).await;
                match fallback_result {
                    Ok(fallback_probe) => {
                        return fallback_probe;
                    }
                    Err(_) => {
                        None
                    }
                }
                //todo handle re-balance
            }
        };
        None
    }

    pub async fn upsert_value(&self, probe: Probe) -> Option<Probe> {
        //todo WIP..
        //todo try to send leader and follower write request in parallel
        //todo what happens when leader is down
        //todo if leader_node.unwrap().node.node_status == NodeStatus::AliveServing & AliveNotServing then write
        //todo else read from the follower partition
        let (leader_node, follower_node, partition_id) = self.partition_service.read().await.get_partition_nodes(&probe.probe_id).await;
        println!("leader: {:?}", leader_node);
        println!("follower: {:?}", follower_node);
        let result = leader_node.node.write_probe_to_store(partition_id, true, &probe).await;
        let mut final_result = None;
        match result {
            Ok(_probe_response) => {
                final_result = Some(probe.clone());
            }
            Err(_err) => {
                //todo ideally it should not reach here
                println!("Should not reach here")
            }
        }
        if follower_node.is_node_not_down() {
            //you need to write to delta data in leader if the delta data is present, and the follower node will take care of handling original read and write
            let res = follower_node.write_probe_to_store(partition_id, false, &probe).await;
            //todo handle this res
            match res {
                Ok(_probe_response) => {
                    final_result = Some(probe);
                }
                Err(_err) => {
                    println!("follower down while writing");
                }
            }
        }
        return final_result;
    }

    pub async fn get_delta_data(&self, partition_id: usize) -> Option<MemoryStore> {
        self.partition_service.read().await.get_leader_delta_data(partition_id).await
    }
}
