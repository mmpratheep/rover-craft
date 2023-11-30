use std::ops::Deref;
use std::sync::{Arc};
use tokio::sync::{RwLock, RwLockReadGuard};
use std::time::Duration;

use log::{debug, error, info};
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
        let mut interval = interval(Duration::from_millis(500));

        loop {
            // Wait for the next tick
            interval.tick().await;
            let mut handle_recovery = false;
            let mut re_balance_partitions = false;
            println!("Starting health check ");
            for peer_node in partition_service.read().await.nodes.get_peers() {
                debug!("Making health check for {}", peer_node.host_name);
                let result = peer_node.do_health_check().await;
                match result {
                    Ok(_) => {
                        println!("Received response");
                        //todo change to current node status
                        if Self::is_current_node_dead(&partition_service.read().await) {
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
                            partition_service.read().await.nodes.make_node_dead(&peer_node.host_name);
                        }
                    }
                }
            }
            if re_balance_partitions {
                println!("Seems like node is down");
                if partition_service.read().await.nodes.is_current_node_down() {
                    println!("Marking current node down");
                    interval = tokio::time::interval(Duration::from_millis(500));
                    let partition_service_read_guard = partition_service.read().await;
                    partition_service_read_guard.nodes.make_node_dead(
                        &partition_service_read_guard.nodes.get_current_node().unwrap().host_name
                    );
                } else {
                    println!("Re-balancing partitions");
                    partition_service.write().await.balance_partitions_and_write_delta_data().await;
                }
            }

            if handle_recovery {
                for peer_node in partition_service.read().await.nodes.get_peers() {
                    let partition_service_read_guard = partition_service.read().await;
                    let alive_peer_node = partition_service_read_guard.nodes.get_node(&peer_node.host_name).unwrap().clone();

                    let current_node = partition_service_read_guard.nodes.get_current_node().unwrap();
                    println!("making current node alive");
                    partition_service_read_guard.nodes.make_node_alive_and_serving(&current_node.host_name);

                    let current_node_leader_partitions = partition_service_read_guard.get_leader_partition_ids(&current_node.host_name);
                    println!("Announce alive and not serving, for partitions: {:?}", current_node_leader_partitions);
                    alive_peer_node.announce_me_alive_not_serving(&current_node.host_name, &current_node_leader_partitions).await;
                }

                println!("Handling recovery");
                let partition_service_read_guard = partition_service.read().await;
                let current_node = partition_service_read_guard.nodes.get_current_node().unwrap();
                let current_node_leader_partition_ids = partition_service_read_guard.get_leader_partition_ids(&current_node.host_name);
                let current_node_follower_partition_ids = partition_service_read_guard.get_follower_partition_ids(&current_node.host_name);

                //todo IMP optimise: get delta data in parallel from other nodes
                println!("Catching up the leader partitions");

                for partition_id in current_node_leader_partition_ids.clone().iter() {
                    let leader_s_follower_partition = partition_service_read_guard.get_follower_node(partition_id.clone() as usize).await;
                    println!("leader partition_id: {}, follower partition host: {}", &partition_id, &leader_s_follower_partition.node_ref.host_name);
                    let result = leader_s_follower_partition
                        .get_delta_data_from_peer(partition_id.clone()).await;

                    match result {
                        Ok(delta_data) => {
                            println!("Received delta data, updating the partition");
                            let leader = partition_service_read_guard.get_leader_node(partition_id.clone() as usize).await;
                            leader.update_with_delta_data(delta_data.into_inner());
                            println!("updated the partition");
                        }
                        Err(err) => {
                            println!("Err Delta data leader: {} : {}",leader_s_follower_partition.node_ref.host_name, err)
                        }
                    }
                }
                println!("Catching up the follower partitions");

                for partition_id in current_node_follower_partition_ids.clone().iter() {
                    let follower_s_leader_partitions = partition_service_read_guard.get_leader_node(partition_id.clone() as usize).await;
                    println!("partition_id: {}, partition: {}", &partition_id, &follower_s_leader_partitions.node_ref.host_name);
                    let result = follower_s_leader_partitions
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
        println!("current node status in health check {:?}", *partition_service_read_guard.nodes.get_current_node().unwrap().node_status.read().unwrap().deref());
        *partition_service_read_guard.nodes.get_current_node().unwrap().node_status.read().unwrap().deref() == Dead
    }
}
