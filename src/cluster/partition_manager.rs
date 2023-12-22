use std::sync::{Arc};
use tokio::sync::{oneshot, RwLock as TrwLock};
use log::{error, info, warn};
use tokio::sync::mpsc::Sender;
use tokio::{join, try_join};
use warp::ws::Message;
use crate::cluster::health_check_service::ThreadMessage;
use crate::cluster::partition_service::PartitionService;
use crate::grpc::leader_node::LeaderNode;
use crate::grpc::node::Node;
use crate::grpc::node_status::NodeStatus;
use crate::grpc::service::probe_sync::ProbeProto;
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

    pub async fn is_current_node_down(&self) -> bool{
        self.partition_service.read().await.is_current_node_dead()
    }

    pub async fn is_current_node_not_serving(&self) -> bool {
        self.partition_service.read().await.is_current_node_not_serving()
    }

    pub async fn read_probe(&self, probe_id: String, tx: Sender<ThreadMessage>) -> Option<Probe> {
        //todo clone
        let (leader_node, follower_node, partition_id) = self.partition_service.read().await
            .get_partition_nodes(&probe_id).await;
        log::info!("read request {}, {}", probe_id, partition_id);

        match join!(Self::read_from_leader(&probe_id, leader_node, partition_id, tx.clone()),
            Self::read_from_follower(&probe_id, follower_node, partition_id, tx.clone())
        ) {
            (Some(leader_probe), Some(follower_probe)) => {
                if leader_probe.event_date_time >= follower_probe.event_date_time {
                    log::info!("returning from leader {}", probe_id);
                    return Some(leader_probe);
                }
                log::info!("returning from follower {}", probe_id);
                Some(follower_probe)
            }
            (Some(leader_probe), None) => {
                Some(leader_probe)
            }
            (None, Some(follower_probe)) => {
                Some(follower_probe)
            }
            (None, None) => {
                None
            }
        }

        //todo if leader_node.unwrap().node.node_status == NodeStatus::AliveServing -> then read from that
        //todo else read from the follower partition
        // return match leader_node.node.node_status() {
        //     NodeStatus::AliveServing => {
        //         Self::read_from_leader(&probe_id, leader_node, follower_node, partition_id, tx).await
        //     }
        //     NodeStatus::AliveNotServing => {
        //         log::info!("Reading from follower as the leader is alive and not serving");
        //         let result = follower_node.read_probe_from_store(partition_id, false, &probe_id).await;
        //         return match result {
        //             Ok(val) => {
        //                 val
        //             }
        //             Err(error) => {
        //                 log::error!("Error while getting data from follower when leader is alive and not serving, so reading from leader now {}", error);
        //                 let result = leader_node.node.read_probe_from_store(partition_id, true, &probe_id).await;
        //                 return match result {
        //                     Ok(probe) => {
        //                         log::info!("Read data: {:?}", probe);
        //                         probe
        //                     }
        //                     Err(err) => {
        //                         log::warn!("Leader went down while reading {}", err);
        //                         None
        //                     }
        //                 }
        //             }
        //         };
        //     }
        //     NodeStatus::Dead => {
        //         log::warn!("leader down while reading, reading from follower");
        //         if leader_node.node.is_current_node(){
        //             log::error!("WARNING!!!!!!!!!, received request when current node is down");
        //         }
        //         let result = follower_node.read_probe_from_store(partition_id, false, &probe_id).await;
        //         return match result {
        //             Ok(val) => {
        //                 val
        //             }
        //             Err(error) => {
        //                 log::error!("!!!!!!!!this should not happen {}", error);
        //                 None
        //             }
        //         };
        //     }
        // };
    }

    async fn read_from_leader(probe_id: &String, leader_node: LeaderNode, partition_id: usize, tx: Sender<ThreadMessage>) -> Option<Probe> {
        let result = leader_node.node.read_probe_from_store(partition_id, true, &probe_id).await;

        return match result {
            Ok(probe) => {
                log::info!("Read data from leader: {:?}", probe);
                probe
            }
            Err(err) => {
                log::warn!("Leader went down while reading {} {}", probe_id, err);
                tx.send(ThreadMessage {
                    hostname: leader_node.node.node_ref.host_name.clone(),
                    response_sender: None,
                }).await.expect("Err couldn't send leader down message via channel to read the data");
                None
            }
        };
    }

    async fn read_from_follower(probe_id: &String, follower_node: Arc<Node>, partition_id: usize, tx: Sender<ThreadMessage>) -> Option<Probe> {
        let result = follower_node.read_probe_from_store(partition_id, false, &probe_id).await;

        return match result {
            Ok(probe) => {
                match probe {
                    Some(value) => {
                        log::info!("Read data from follower: {}", value);
                        Some(value)
                    }
                    None => {
                        None
                    }
                }
            }
            Err(err) => {
                log::warn!("Follower went down while reading {} {}",probe_id, err);
                tx.send(ThreadMessage {
                    hostname: follower_node.node_ref.host_name.clone(),
                    response_sender: None,
                }).await.expect("Err couldn't send leader down message via channel to read the data");
                None
            }
        };
    }

    pub async fn upsert_value(&self, probe: Probe, tx: Sender<ThreadMessage>) -> Option<Probe> {
        //todo WIP..
        //todo what happens when leader is down
        //todo if leader_node.unwrap().node.node_status == NodeStatus::AliveServing & AliveNotServing then write
        //todo else read from the follower partition
        // log::info!("partition-service: {:?}", self.partition_service);
        let (leader_node, follower_node, partition_id) = self.partition_service.read().await.get_partition_nodes(&probe.probe_id).await;
        log::info!("write request {} -> {:?} ", partition_id, probe.event_id);
        match join!(self.write_to_leader(&probe, &tx, leader_node, partition_id),
            self.write_to_follower(&probe, &tx, follower_node, partition_id)) {
            (Some(probe), Some(_)) => {
                Some(probe)
            }
            (Some(probe), None) => {
                Some(probe)
            }
            (None, Some(probe)) => {
                Some(probe)
            }

            (None, None) => {
                None
            }
        }
    }

    async fn write_to_follower(&self, probe: &Probe, tx: &Sender<ThreadMessage>, follower_node: Arc<Node>, partition_id: usize) -> Option<Probe> {

        //you need to write to delta data in leader if the delta data is present, and the follower node will take care of handling original read and write
        if follower_node.is_node_not_down() {
            let res = follower_node.write_probe_to_store(partition_id, false, &probe).await;
            //todo handle this res
            match res {
                Ok(_probe_response) => {
                    log::info!("successfully written to follower {}", probe.event_id);
                    Some(probe.clone())
                }
                Err(_err) => {
                    log::warn!("Err: follower down while writing {}", probe.event_id);
                    Self::initiate_manual_re_balancing(&tx, follower_node.node_ref.host_name.clone()).await;
                    log::warn!("writing to delta in a leader partition");
                    let (leader_node, follower_node, partition_id) = self.partition_service.read().await.get_partition_nodes(&probe.probe_id).await;
                    let result = leader_node.write_probe_to_store_and_delta(partition_id, &probe).await;

                    match result {
                        Ok(_probe_response) => {
                            log::info!("successfully written delta to leader after re-balance");
                            Some(probe.clone())
                        }
                        Err(_err) => {
                            log::error!("Err: Should not reach here after follower re-balance");
                            None
                        }
                    }
                }
            }
        } else {
            None
        }
    }

    async fn write_to_leader(&self, probe: &Probe, tx: &Sender<ThreadMessage>, leader_node: LeaderNode, partition_id: usize) -> Option<Probe> {
        let result = leader_node.write_probe_to_store_and_delta(partition_id, &probe).await;
        let mut final_result = None;
        match result {
            Ok(_probe_response) => {
                log::info!("successfully written to leader {}", probe.event_id);
                final_result = Some(probe.clone());
            }
            Err(_err) => {
                log::warn!("Err: leader down while writing {}", probe.event_id);
                //todo try to keep this creation in common place
                Self::initiate_manual_re_balancing(&tx, leader_node.node.node_ref.host_name.clone()).await;

                let (leader_node, follower_node, partition_id) = self.partition_service.read().await.get_partition_nodes(&probe.probe_id).await;
                let result = leader_node.write_probe_to_store_and_delta(partition_id, &probe).await;
                match result {
                    Ok(_probe_response) => {
                        log::info!("successfully written to leader after re-balance, {}",probe.event_id);
                        final_result = Some(probe.clone());
                    }
                    Err(_err) => {
                        log::error!("Err: Should not reach here after re-balance {}", probe.event_id);
                    }
                }
            }
        }
        final_result
    }

    async fn initiate_manual_re_balancing(tx: &Sender<ThreadMessage>, hostname: String) {
        let (scheduler_response_sender, scheduler_response_receiver) = oneshot::channel();
        log::info!("handler hostname: {}",hostname);
        tx.send(ThreadMessage { hostname, response_sender: Some(scheduler_response_sender) })
            .await.expect("Err couldn't send leader down message via channel to health check ");

        log::warn!("Err: Node down while writing");

        scheduler_response_receiver.await.expect("unable to get the thread response");
    }

    pub async fn get_delta_data(&self, partition_id: usize) -> Vec<ProbeProto> {
        self.partition_service.read().await.get_leader_delta_data(partition_id)
    }
}
