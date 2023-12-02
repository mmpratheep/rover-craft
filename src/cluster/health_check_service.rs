use std::ops::Deref;
use std::sync::Arc;
use std::time::Duration;

use log::debug;
use tokio::sync::RwLock;
use tokio::sync::mpsc::Receiver;
use tokio::time::{interval, Interval, sleep};
use tonic::{Request, Response, Status};

use crate::cluster::partition_service::PartitionService;
use crate::grpc::node_ref::NodeRef;
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
    pub async fn start_health_check(partition_service: Arc<RwLock<PartitionService>>, mut health_check_receiver: Receiver<String>) {
        sleep(Duration::from_secs(5)).await;
        let mut interval = interval(Duration::from_millis(500));
        let read_lock = partition_service.read().await;
        let peer_nodes = read_lock.get_peers().await.clone();

        loop {
            tokio::select! {
                // Wait for the next tick
                _ = interval.tick() => {
                    println!("Health check");
                    Self::health_check(&partition_service, &mut interval,&peer_nodes).await;
                }
                value = health_check_receiver.recv() => {
                    //todo remove already dead peer node
                    let read_lock = partition_service.read().await;
                    let peer_nodes = read_lock.get_peers().await.clone();
                    // External event received, trigger immediate execution
                    println!("external triggered it");
                    Self::health_check(&partition_service, &mut interval,&peer_nodes).await;
                }
            }
        }
    }

    async fn health_check(partition_service: &Arc<RwLock<PartitionService>>, interval: &mut Interval, peer_nodes: &Vec<&Arc<NodeRef>>) {
        let mut handle_recovery = false;
        let mut re_balance_partitions = false;
        println!("Starting health check ");
        for peer_node in peer_nodes {
            println!("Making health check for {}", peer_node.host_name);
            let result = peer_node.do_health_check().await;
            match result {
                Ok(_) => {
                    println!("Received response");
                    //todo change to current node status
                    if partition_service.read().await.is_current_node_dead() {
                        *interval = tokio::time::interval(Duration::from_millis(500));
                        println!("Coming back alive");
                        handle_recovery = true;
                    }
                }
                Err(err) => {
                    println!("Couldn't connect to node {}", peer_node.host_name);
                    if *peer_node.node_status.read().unwrap().deref() != Dead {
                        re_balance_partitions = true;
                        debug!("Health check error: {}",err);
                        println!("Making node dead {}", peer_node.host_name);
                        partition_service.read().await.make_node_dead(&peer_node.host_name);
                    }
                }
            }
        }
        if re_balance_partitions {
            println!("Seems like node is down");
            if partition_service.read().await.is_current_node_dead() {
                println!("Marking current node down");
                *interval = tokio::time::interval(Duration::from_millis(50));
                let partition_service_read_guard = partition_service.read().await;
                partition_service_read_guard.make_node_dead(&partition_service_read_guard.current_node().host_name);
            } else {
                println!("Re-balancing partitions");
                partition_service.read().await.balance_partitions_and_write_delta_data().await;
            }
        }

        if handle_recovery {
            partition_service.read().await.handle_recovery().await;
        }
    }
}
