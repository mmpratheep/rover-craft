use std::sync::{Arc};
use tokio::sync::RwLock;
use std::time::Duration;

use log::{debug};
use tokio::time::interval;
use tokio::try_join;
use tonic::{Request, Response, Status};
use crate::cluster::partition_service::PartitionService;

use crate::grpc::node_status::NodeStatus::Dead;
use crate::grpc::service::cluster::{HealthCheckRequest, HealthCheckResponse};
use crate::grpc::service::cluster::health_check_server::HealthCheck;
use crate::grpc::service::probe_sync::ProbePartition;

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
                        if node.node_status == Dead {
                            handle_recovery = true;
                            let partition_service_read_guard = partition_service.read()
                                .await;
                            let alive_peer_node = partition_service_read_guard.nodes.get_node(&node.host_name);
                            partition_service_read_guard.nodes.make_node_alive_and_serving(&node.host_name);
                            let current_node = partition_service_read_guard.nodes.get_current_node().unwrap();
                            let current_node_leader_partitions = partition_service_read_guard.get_leader_partition_ids(&current_node.host_name);
                            alive_peer_node.unwrap().make_node_alive_not_serving(&current_node.host_name, &current_node_leader_partitions).await;
                        }
                    }
                    Err(err) => {
                        re_balance_partitions = true;
                        debug!("Error occurred while doing healthcheck: {}", err);
                        partition_service.read().await.nodes.make_node_dead(&node.host_name);
                    }
                }
            }
            if re_balance_partitions {
                if partition_service.read().await.nodes.is_current_node_down() {
                    interval = tokio::time::interval(Duration::from_millis(100));
                } else {
                    partition_service.write().await.balance_partitions_and_write_delta_data().await;
                }
            }

            if handle_recovery {
                let partition_service_read_guard = partition_service.read()
                    .await;
                let current_node = partition_service_read_guard.nodes.get_current_node().unwrap();
                let current_node_leader_partition_ids = partition_service_read_guard.get_leader_partition_ids(&current_node.host_name);
                let current_node_follower_partition_ids = partition_service_read_guard.get_follower_partition_ids(&current_node.host_name);

                let peer_delta_leaders: Vec<_> = current_node_leader_partition_ids.into_iter()
                    .map(|partition_id| {
                        partition_service_read_guard.get_follower_node(partition_id.clone() as usize)
                    })
                    .collect();

                let peer_delta_followers: Vec<_> = current_node_follower_partition_ids.clone().into_iter()
                    .map(|partition_id| {
                        partition_service_read_guard.get_leader_node(partition_id.clone() as usize)
                    })
                    .collect();

                //todo perform the below code for every peer_delta_followers and peer_delta_leaders
                let result = partition_service_read_guard.get_leader_node(current_node_follower_partition_ids.get(0).unwrap().clone() as usize).await
                    .get_delta_data_from_peer(current_node_follower_partition_ids[0].clone()).await;

                match result {
                    Ok(delta_data) => {
                        let follower = partition_service_read_guard.get_follower_node(current_node_follower_partition_ids[0].clone() as usize).await;
                        follower.update_with_delta_data(delta_data.into_inner());
                    }
                    Err(_) => {}
                }


                // let change_state: Vec<_> = partition_service_read_guard.nodes.get_peers().into_iter()
                //     .map(|peer|
                //         peer.make_node_alive_and_serving(
                //             &current_node.host_name,
                //             current_node_leader_partition_ids,
                //             current_node_follower_partition_ids)
                //     ).collect();
                //todo catchup the follower partitions
                //todo catchup the leader partitions
                //todo make alive and serving
            }
        }
    }
}
