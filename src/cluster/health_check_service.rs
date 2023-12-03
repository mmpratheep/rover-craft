use std::ops::Deref;
use std::sync::Arc;
use std::time::Duration;

use log::debug;
use tokio::sync::{oneshot, RwLock};
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

pub struct ThreadMessage {
    pub(crate) hostname: String,
    pub(crate) response_sender: Option<oneshot::Sender<()>>,
}


#[tonic::async_trait]
impl HealthCheck for HealthCheckService {
    async fn health_check(&self, _: Request<HealthCheckRequest>) -> Result<Response<HealthCheckResponse>, Status> {
        Ok(Response::new(HealthCheckResponse {
            pong: true
        }))
    }
}

impl HealthCheckService {
    pub async fn start_health_check(partition_service: Arc<RwLock<PartitionService>>, mut health_check_receiver: Receiver<ThreadMessage>) {
        sleep(Duration::from_secs(5)).await;
        let mut interval = interval(Duration::from_millis(500));
        let read_lock = partition_service.read().await;
        let peer_nodes = read_lock.get_peers().await.clone();

        loop {
            tokio::select! {
                // Wait for the next tick
                _ = interval.tick() => {
                    let mut re_balance_partitions = false;
                    log::info!("starting scheduled health check");
                    Self::health_check(&partition_service, &mut interval,&peer_nodes,re_balance_partitions).await;
                }
                thread_message = health_check_receiver.recv() => {
                    //todo remove already dead peer node
                    let mut re_balance_partitions = false;
                    let message = thread_message.unwrap();
                    let hostname = message.hostname;
                    let read_lock = partition_service.read().await;

                    let peer_nodes = read_lock.get_peers().await.clone();
                    let peer_alive_nodes = peer_nodes
                        .iter()
                        .filter(|it| it.host_name != hostname)
                        .map(|arc_ref| &**arc_ref)
                        .collect();
                    let peer_dead_node = peer_nodes
                        .iter()
                        .find(|it| it.host_name == hostname);
                    log::info!("dead node: {}, peer node: {:?}",hostname, peer_alive_nodes);

                    Self::mark_node_down(&mut re_balance_partitions, peer_dead_node.unwrap()).await;
                    // External event received, trigger immediate execution
                    log::info!("external triggered it");
                    Self::health_check(&partition_service, &mut interval,&peer_alive_nodes,re_balance_partitions).await;
                    log::info!("Sending the response");
                    match message.response_sender{
                        None => {}
                        Some(val) =>{
                            val.send(()).expect("the receiver dropped");
                        }
                    };
                }
            }
        }
    }


    async fn health_check(partition_service: &Arc<RwLock<PartitionService>>,
                          interval: &mut Interval,
                          peer_nodes: &Vec<&Arc<NodeRef>>,
                          mut re_balance_partitions: bool,
    ) {
        let mut handle_recovery = false;
        log::debug!("Starting health check ");
        for peer_node in peer_nodes {
            log::debug!("Making health check for {}", peer_node.host_name);
            let result = peer_node.do_health_check().await;
            match result {
                Ok(_) => {
                    log::debug!("Received response");
                    //todo change to current node status
                    if partition_service.read().await.is_current_node_dead() {
                        *interval = tokio::time::interval(Duration::from_millis(500));
                        log::info!("Coming back alive");
                        handle_recovery = true;
                    }
                }
                Err(_err) => {
                    // log::error!("Couldn't connect to node {}", peer_node.host_name);
                    Self::mark_node_down(&mut re_balance_partitions, peer_node).await;
                }
            }
        }
        if re_balance_partitions {
            log::info!("Seems like node is down");
            if partition_service.read().await.is_current_node_down() {
                log::info!("Marking current node down");
                *interval = tokio::time::interval(Duration::from_millis(50));
                let partition_service_read_guard = partition_service.read().await;
                partition_service_read_guard.make_node_dead(&partition_service_read_guard.current_node().host_name);
            } else {
                log::info!("Re-balancing partitions");
                partition_service.read().await.balance_partitions_and_write_delta_data().await;
            }
        }

        if handle_recovery {
            partition_service.read().await.handle_recovery().await;
        }
    }

    async fn mark_node_down(re_balance_partitions: &mut bool, peer_node: &&Arc<NodeRef>) {
        if *peer_node.node_status.read().unwrap().deref() != Dead {
            peer_node.make_node_dead();
            *re_balance_partitions = true;
            log::warn!("Made node dead {}, {:?}", peer_node.host_name, peer_node.node_status);
        }
    }
}
