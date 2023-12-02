use std::sync::{Arc};
use tokio::sync::RwLock as TrwLock;
use log::{error, info};
use tokio::sync::mpsc::Sender;
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
        let partition = self.get_partition_and_write_delta(partition_id, is_leader, &probe);
        partition
            .await.write_probe_to_store(partition_id, true, &probe).await.expect("internal probe write failed");
    }

    pub async fn get_partition_and_write_delta(&self, partition_id: usize, is_leader: bool, probe: &Probe) -> Arc<Node> {
        if is_leader {
            self.partition_service.read().await.get_leader_and_write_delta(partition_id, probe).await
        } else { self.partition_service.read().await.get_follower_node(partition_id).await }
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


    pub async fn read_probe(&self, probe_id: String, tx: Sender<String>) -> Option<Probe> {
        //todo clone
        let (leader_node, follower_node, partition_id) = self.partition_service.read().await
            .get_partition_nodes(&probe_id).await;
        log::info!("read request {:?}", probe_id);
        log::info!("partition-id: {:?}", partition_id);
        //todo if leader_node.unwrap().node.node_status == NodeStatus::AliveServing -> then read from that
        //todo else read from the follower partition
        return match leader_node.node.node_status() {
            NodeStatus::AliveServing => {
                Self::read_from_leader_first(&probe_id, leader_node, follower_node, partition_id, tx).await
            }
            NodeStatus::AliveNotServing => {
                let result = follower_node.read_probe_from_store(partition_id, false, &probe_id).await;
                return match result {
                    Ok(val) => {
                        log::info!("Read from store, {:?}", val);
                        val
                    }
                    Err(error) => {
                        log::warn!("Error while getting data from follower when leader is alive and not serving {}", error);
                        None
                    }
                };
            }
            NodeStatus::Dead => {
                let result = follower_node.read_probe_from_store(partition_id, false, &probe_id).await;
                return match result {
                    Ok(val) => {
                        val
                    }
                    Err(error) => {
                        log::error!("Error while getting data from follower when leader is dead {}", error);
                        None
                    }
                };
            }
        };
    }

    async fn read_from_leader_first(probe_id: &String, leader_node: LeaderNode, follower_node: Arc<Node>, partition_id: usize, tx: Sender<String>) -> Option<Probe> {
        let result = leader_node.node.read_probe_from_store(partition_id, true, &probe_id).await;

        return match result {
            Ok(probe) => {
                log::info!("Read data: {:?}", probe);
                probe
            }
            Err(err) => {
                log::error!("Exception {}", err);

                //todo explicitly tell the follower node to rebalance the partition by sending the dead node ip, then make the request to the follower to get the probe from partition
                let fallback_result = follower_node.read_probe_from_store(partition_id, false, &probe_id).await;
                match fallback_result {
                    Ok(fallback_probe) => {
                        return fallback_probe;
                    }
                    Err(_) => {
                        tx.send(leader_node.node.node_ref.host_name.clone()).await.expect("Err couldn't send leader down message via channel to health check ");
                        None
                    }
                }
                //todo handle re-balance
            }
        };
        None
    }

    pub async fn upsert_value(&self, probe: Probe, tx: Sender<String>) -> Option<Probe> {
        //todo WIP..
        //todo try to send leader and follower write request in parallel
        //todo what happens when leader is down
        //todo if leader_node.unwrap().node.node_status == NodeStatus::AliveServing & AliveNotServing then write
        //todo else read from the follower partition
        // log::info!("partition-service: {:?}", self.partition_service);
        let (leader_node, follower_node, partition_id) = self.partition_service.read().await.get_partition_nodes(&probe.probe_id).await;
        log::info!("write request {:?}", probe);
        log::info!("partition-id: {:?}", partition_id);
        let result = leader_node.write_probe_to_store_and_delta(partition_id, &probe).await;
        let mut final_result = None;
        match result {
            Ok(_probe_response) => {
                log::info!("successfully written to leader");
                final_result = Some(probe.clone());
            }
            Err(_err) => {
                tx.send(leader_node.node.node_ref.host_name.clone()).await.expect("Err couldn't send leader down message via channel to health check ");
                log::warn!("Err: Leader down while writing");
                let result = leader_node.write_probe_to_store_and_delta(partition_id, &probe).await;
                match result {
                    Ok(_probe_response) => {
                        log::info!("successfully written to leader after re-balance");
                        final_result = Some(probe.clone());
                    }
                    Err(_err) => {
                        log::error!("Err: Should not reach here after re-balance");
                    }
                }
            }
        }
        if follower_node.is_node_not_down() {
            //you need to write to delta data in leader if the delta data is present, and the follower node will take care of handling original read and write
            let res = follower_node.write_probe_to_store(partition_id, false, &probe).await;
            //todo handle this res
            match res {
                Ok(_probe_response) => {
                    log::info!("successfully written to follower");
                    final_result = Some(probe);
                }
                Err(_err) => {
                    log::warn!("Err: follower down while writing");
                    tx.send(leader_node.node.node_ref.host_name.clone()).await.expect("Err couldn't send leader down message via channel to health check ");
                    log::warn!("writing to delta in leader");
                    leader_node.write_to_delta(&probe);
                }
            }
        }
        return final_result;
    }

    pub async fn get_delta_data(&self, partition_id: usize) -> Option<MemoryStore> {
        self.partition_service.read().await.get_leader_delta_data(partition_id)
    }
}
