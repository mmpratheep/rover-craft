use std::ffi::OsString;
use std::fmt;
use std::ops::Deref;
use std::sync::RwLock;

use log::debug;
use tokio::time::Instant;
use tonic::Status;
use tonic::transport::Channel;

use crate::grpc::node::get_channel;
use crate::grpc::node_status::NodeStatus;
use crate::grpc::node_status::NodeStatus::{AliveServing, Dead};
use crate::grpc::service::cluster::{AnnounceAliveNotServingRequest, AnnounceAliveServingRequest, HealthCheckRequest};
use crate::grpc::service::cluster::health_check_client::HealthCheckClient;
use crate::grpc::service::cluster::partition_proto_client::PartitionProtoClient;
use crate::{ANNOUNCEMENT_TIMEOUT, HEALTH_CHECK_TIMEOUT};

#[derive(Debug)]
pub struct NodeRef {
    pub host_name: String,
    pub node_status: RwLock<NodeStatus>,
    health_check_client: HealthCheckClient<Channel>,
    proto_partition_client: Option<PartitionProtoClient<Channel>>,
}

impl fmt::Display for NodeRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} -> {:?}", &self.host_name, self.node_status.read().unwrap().deref())
    }
}



impl NodeRef {
    pub fn new(host_name: String) -> Self {
        Self {
            host_name: host_name.clone(),
            node_status: RwLock::new(NodeStatus::AliveServing),
            health_check_client: HealthCheckClient::new(get_channel(&host_name, HEALTH_CHECK_TIMEOUT)),
            //todo handle creation of channel to current node
            proto_partition_client: Some(PartitionProtoClient::new(get_channel(&host_name, ANNOUNCEMENT_TIMEOUT))),
        }
    }

    pub fn is_dead(&self) -> bool {
        *self.node_status.read().unwrap().deref() == Dead
    }
    pub fn make_node_dead(&self) {
        *self.node_status.write().unwrap() = Dead;
    }
    pub fn make_node_alive_and_serving(&self) {
        *self.node_status.write().unwrap() = AliveServing;
    }

    pub fn is_current_node(&self) -> bool {
        Self::is_same_node(&self.host_name)
    }

    fn is_same_node(node_ip: &String) -> bool {
        let current_node_host_name = get_current_node_hostname().into_string().unwrap();
        debug!("current {} node {} result {}", current_node_host_name, node_ip, node_ip.contains(&current_node_host_name));
        node_ip.contains(&current_node_host_name)
    }


    pub async fn do_health_check(&self) -> Result<(), Status> {
        let start_time = Instant::now();
        log::debug!("Before calling grpc health check {:?}",start_time);
        let response = self.health_check_client.clone()
            .health_check(HealthCheckRequest { ping: true }).await;
        let end_time = Instant::now();
        log::debug!("After calling grpc health check {:?}",end_time);
        let duration = end_time - start_time;
        log::debug!("Latency {:?}",duration);

        return match response {
            Ok(_val) => {
                Ok(())
            }
            Err(err) => {
                Err(err)
            }
        };
    }

    pub async fn announce_me_alive_not_serving(&self, hostname: &String, leader_partitions: &Vec<u32>) {
        if self.proto_partition_client.is_some() {
            log::info!("Making node alive and not serving... {}", self.host_name);
            let request = tonic::Request::new(AnnounceAliveNotServingRequest {
                host_name: hostname.clone(),
                leader_partitions: leader_partitions.clone(),
            });
            let response = self.proto_partition_client.clone().unwrap().make_node_alive_not_serving(request).await;
            match response {
                Ok(_val) => {
                    log::info!("Made node alive and not serving in: {}",self.host_name);
                }
                Err(err) => {
                    log::error!("error while announcing alive and not serving: {}", err);
                }
            }
        }
    }

    pub async fn announce_me_alive_and_serving(&self, hostname: &String, leader_partitions: Vec<u32>, follower_partitions: Vec<u32>) {
        if self.proto_partition_client.is_some() {
            log::info!("Making node alive and serving... {}", self.host_name);
            let request = tonic::Request::new(AnnounceAliveServingRequest {
                host_name: hostname.clone(),
                leader_partitions,
                follower_partitions,
            });
            let response = self.proto_partition_client.clone().unwrap().make_node_alive_serving(request).await;
            match response {
                Ok(_val) => {
                    log::info!("Made node alive and serving in: {}",self.host_name);
                }
                Err(err) => {
                    log::error!("Error while announcing alive and serving: {}", err);
                }
            }
        }
    }
}

pub fn get_current_node_hostname() -> OsString {
    hostname::get().unwrap().to_os_string()
}