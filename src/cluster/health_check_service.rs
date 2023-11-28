use std::ops::Deref;
use std::sync::{Arc};
use tokio::sync::{RwLock, RwLockReadGuard};
use std::time::Duration;

use log::{debug};
use tokio::time::{interval, sleep};
use tonic::{Request, Response, Status};
use crate::cluster::partition_service::PartitionService;

use crate::grpc::node_status::NodeStatus::Dead;
use crate::grpc::service::cluster::{HealthCheckRequest, HealthCheckResponse};
use crate::grpc::service::cluster::health_check_server::HealthCheck;

#[derive(Debug, Default, Clone)]
pub(crate) struct HealthCheckService {}

#[tonic::async_trait]
impl HealthCheck for HealthCheckService {
    async fn health_check(&self, _: Request<HealthCheckRequest>) -> Result<Response<HealthCheckResponse>, Status> {
        Ok(Response::new(HealthCheckResponse {
            pong: true
        }))
    }
}

impl HealthCheckService {
    pub async fn start_health_check(partition_service: Arc<RwLock<PartitionService>>) {
        sleep(Duration::from_secs(5)).await;
        let mut interval = interval(Duration::from_secs(1));

        loop {
            // Wait for the next tick
            interval.tick().await;
            let mut handle_recovery = false;
            let mut re_balance_partitions = false;
            println!("Starting health check ");
            for node in partition_service.read().await.nodes.get_peers() {
                debug!("Making health check for {}", node.host_name);
                let result = node.do_health_check().await;
                match result {
                    Ok(_) => {
                        debug!("Received response");
                        //todo change to current node status
                        if Self::is_current_node_dead(&partition_service.read().await) {
                            println!("Coming back alive");
                            handle_recovery = true;
                            let partition_service_read_guard = partition_service.read()
                                .await;
                            let alive_peer_node = partition_service_read_guard.nodes.get_node(&node.host_name).unwrap().clone();
                            partition_service_read_guard.nodes.make_node_alive_and_serving(&node.host_name);
                            let current_node = partition_service_read_guard.nodes.get_current_node().unwrap();
                            let current_node_leader_partitions = partition_service_read_guard.get_leader_partition_ids(&current_node.host_name);
                            println!("Announce alive and not serving");
                            alive_peer_node.announce_me_alive_not_serving(&current_node.host_name, &current_node_leader_partitions).await;
                        }
                    }
                    Err(err) => {
                        println!("Couldn't connect to node {}", node.host_name);
                        if *node.node_status.read().unwrap().deref() != Dead {
                            re_balance_partitions = true;
                            debug!("Health check error: {}",err);
                            println!("Making node dead {}", node.host_name);
                            partition_service.read().await.nodes.make_node_dead(&node.host_name);
                        }
                    }
                }
            }
            if re_balance_partitions {
                println!("Seems like node is down");
                if partition_service.read().await.nodes.is_current_node_down() {
                    println!("Marking current node down");
                    interval = tokio::time::interval(Duration::from_millis(100));
                } else {
                    println!("Re-balancing partitions");
                    partition_service.write().await.balance_partitions_and_write_delta_data().await;
                }
            }

            if handle_recovery {
                println!("Handling recovery");
                let partition_service_read_guard = partition_service.read()
                    .await;
                let current_node = partition_service_read_guard.nodes.get_current_node().unwrap();
                let current_node_leader_partition_ids = partition_service_read_guard.get_leader_partition_ids(&current_node.host_name);
                let current_node_follower_partition_ids = partition_service_read_guard.get_follower_partition_ids(&current_node.host_name);

                //todo IMP optimise: get delta data in parallel from other nodes
                println!("Catching up the leader partitions");

                for partition_id in current_node_leader_partition_ids.clone().iter() {
                    let follower_partition = partition_service_read_guard.get_follower_node(partition_id.clone() as usize).await;
                    println!("partition_id: {}, partition: {}", &partition_id, &follower_partition.host_name);
                    let result = follower_partition
                        .get_delta_data_from_peer(partition_id.clone()).await;

                    match result {
                        Ok(delta_data) => {
                            println!("Received delta data, updating the partition");
                            let leader = partition_service_read_guard.get_leader_node(partition_id.clone() as usize).await;
                            leader.update_with_delta_data(delta_data.into_inner());
                            println!("updated the partition");
                        }
                        Err(err) => {
                            println!("Err Delta data leader: {}", err)
                        }
                    }
                }
                println!("Catching up the follower partitions");

                for partition_id in current_node_follower_partition_ids.clone().iter() {
                    let leader_partitions = partition_service_read_guard.get_leader_node(partition_id.clone() as usize).await;
                    println!("partition_id: {}, partition: {}", &partition_id, &leader_partitions.host_name);
                    let result = leader_partitions
                        .get_delta_data_from_peer(partition_id.clone()).await;

                    match result {
                        Ok(delta_data) => {
                            println!("Received delta data, updating the partition");
                            let follower = partition_service_read_guard.get_follower_node(partition_id.clone() as usize).await;
                            follower.update_with_delta_data(delta_data.into_inner());
                            println!("updated the partition");
                        }
                        Err(err) => {
                            println!("Err Delta data follower: {}", err)
                        }
                    }
                }
                println!("Announce alive and serving");
                for peer in partition_service.read().await.nodes.get_peers() {
                    peer.announce_me_alive_and_serving(
                        &current_node.host_name,
                        current_node_leader_partition_ids.clone(),
                        current_node_follower_partition_ids.clone()).await;
                }
            }
        }
    }

    fn is_current_node_dead(partition_service_read_guard: &RwLockReadGuard<PartitionService>) -> bool {
        *partition_service_read_guard.nodes.get_current_node().unwrap().node_status.read().unwrap().deref() == Dead
    }
}
