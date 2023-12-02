use std::ffi::OsString;
use std::fmt;
use std::ops::Deref;
use std::sync::RwLock;

use log::debug;
use tonic::Status;
use tonic::transport::Channel;

use crate::grpc::node::get_channel;
use crate::grpc::node_status::NodeStatus;
use crate::grpc::node_status::NodeStatus::Dead;
use crate::grpc::service::cluster::{AnnounceAliveNotServingRequest, AnnounceAliveServingRequest, HealthCheckRequest};
use crate::grpc::service::cluster::health_check_client::HealthCheckClient;
use crate::grpc::service::cluster::partition_proto_client::PartitionProtoClient;

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
            health_check_client: HealthCheckClient::new(get_channel(&host_name)),
            //todo handle creation of channel to current node
            proto_partition_client: Some(PartitionProtoClient::new(get_channel(&host_name))),
        }
    }

    pub fn is_dead(&self) -> bool {
        *self.node_status.read().unwrap().deref() == Dead
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
        let response = self.health_check_client.clone()
            .health_check(HealthCheckRequest { ping: true }).await;

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
            println!("Making node alive and not serving... {}", &hostname);
            let request = tonic::Request::new(AnnounceAliveNotServingRequest {
                host_name: hostname.clone(),
                leader_partitions: leader_partitions.clone(),
            });
            let response = self.proto_partition_client.clone().unwrap().make_node_alive_not_serving(request).await;
            match response {
                Ok(_val) => {
                    println!("Made node alive and not serving in: {}",hostname);
                }
                Err(err) => {
                    println!("error while announcing alive and not serving: {}", err);
                }
            }
        }
    }

    pub async fn announce_me_alive_and_serving(&self, hostname: &String, leader_partitions: Vec<u32>, follower_partitions: Vec<u32>) {
        if self.proto_partition_client.is_some() {
            println!("Making node alive and serving... {}", &hostname);
            let request = tonic::Request::new(AnnounceAliveServingRequest {
                host_name: hostname.clone(),
                leader_partitions,
                follower_partitions,
            });
            let response = self.proto_partition_client.clone().unwrap().make_node_alive_serving(request).await;
            match response {
                Ok(_val) => {
                    println!("Made node alive and serving in: {}",hostname);
                }
                Err(err) => {
                    println!("Error while announcing alive and serving: {}", err);
                }
            }
        }
    }
}

pub fn get_current_node_hostname() -> OsString {
    hostname::get().unwrap().to_os_string()
}