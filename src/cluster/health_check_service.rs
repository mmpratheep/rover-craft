use std::fs::read;
use std::sync::{Arc};
use tokio::sync::RwLock;
use std::time::Duration;

use log::{debug, info};
use tokio::time::interval;
use tonic::{Request, Response, Status};
use crate::cluster::partition_service::PartitionService;

use crate::grpc::node_status::NodeStatus::Dead;
use crate::grpc::nodes::NodeManager;
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
        let mut interval = interval(Duration::from_secs(1));

        loop {
            // Wait for the next tick
            interval.tick().await;
            println!("Starting health check ");
            for node in partition_service.read().await.nodes.get_peers() {
                debug!("Making health check for {}", node.host_name);
                let result = node.do_health_check().await;
                match result {
                    Ok(_) => {
                        debug!("Received response");
                        if node.node_status == Dead {
                            let read_guard = partition_service.read()
                                .await;
                            let alive_peer_node = read_guard.nodes.get_node(&node.host_name);
                            read_guard.nodes.make_node_alive_and_serving(&node.host_name);
                            let current_node = read_guard.nodes.get_current_node().unwrap();
                            let current_node_leader_partitions = read_guard.get_leader_partitions(&current_node.host_name);
                            //todo give current hostname
                            alive_peer_node.unwrap().make_node_alive_not_serving(&current_node.host_name, current_node_leader_partitions).await;
                            //todo catchup the remaining things
                            //todo make alive and serving
                        }
                    }
                    Err(err) => {
                        debug!("Error occurred while doing healthcheck: {}", err);
                        partition_service.read().await.nodes.make_node_dead(&node.host_name);
                    }
                }
            }
            if partition_service.read().await.nodes.is_current_node_down() {
                interval = tokio::time::interval(Duration::from_millis(100));
            } else {
                partition_service.write().await.balance_partitions_and_write_delta_data().await;
            }
        }
    }
}
