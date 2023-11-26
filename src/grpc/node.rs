use std::ffi::OsString;
use std::io::Chain;
use std::time::Duration;
use log::{debug, log};
use tonic::{Code, Response, Status};
use tonic::codegen::tokio_stream::StreamExt;
use tonic::transport::{Channel, Error};
use warp::hyper::client::connect::Connect;
use crate::cluster::network_node::NetworkNode;
use crate::grpc::node_status::NodeStatus;
use crate::grpc::service::cluster::health_check_client::HealthCheckClient;
use crate::grpc::service::cluster::{AnnounceAliveServingRequest, AnnounceAliveNotServingRequest, HealthCheckRequest, HealthCheckResponse};
use crate::grpc::service::cluster::partition_proto_client::PartitionProtoClient;
use crate::grpc::service::probe_sync::probe_sync_client::ProbeSyncClient;
use crate::grpc::service::probe_sync::{PartitionRequest, ProbePartition, ProbeProto, ReadProbeRequest, WriteProbeRequest, WriteProbeResponse};
use crate::probe::probe::Probe;
use crate::store::memory_store::MemoryStore;

#[derive(Clone, Debug)]
//todo remove this clone
pub struct Node {
    pub host_name: String,
    probe_store: NetworkNode,
    health_check_client: HealthCheckClient<Channel>,
    proto_partition_client: Option<PartitionProtoClient<Channel>>,
    pub node_status: NodeStatus,
}

impl Node {
    pub fn make_node_down(mut self) {
        self.node_status = NodeStatus::Dead
    }

    pub fn is_node_not_down(&self) -> bool {
        self.node_status != NodeStatus::Dead
    }

    pub fn is_current_node(&self) -> bool {
        Self::is_same_node(&self.host_name)
    }

    fn is_same_node(node_ip: &String) -> bool {
        let current_node_host_name = Self::get_current_node_hostname().into_string().unwrap();
        debug!("current {} node {} result {}", current_node_host_name, node_ip, node_ip.contains(&current_node_host_name));
        node_ip.contains(&current_node_host_name)
    }

    fn get_current_node_hostname() -> OsString {
        hostname::get().unwrap().to_os_string()
    }

    pub fn update_with_delta_data(&self, partition : ProbePartition){
        match &self.probe_store {
            NetworkNode::LocalStore(store) => {
                store.de_serialise_and_update(partition.probe_array);
            }
            NetworkNode::RemoteStore(_) => {
                println!("The data is not present in the current node")
            }
        }

    }

    pub(crate) async fn new(node_host_name: String) -> Self {
        if Self::is_same_node(&node_host_name) {
            return
                Self {
                    host_name: node_host_name.clone(),
                    probe_store: NetworkNode::LocalStore(MemoryStore::new()),
                    health_check_client: HealthCheckClient::new(Self::get_channel(&node_host_name).await),
                    proto_partition_client: None,
                    node_status: NodeStatus::AliveServing,
                };
        }
        Self {
            host_name: node_host_name.clone(),
            probe_store: NetworkNode::RemoteStore(ProbeSyncClient::new(Self::get_channel(&node_host_name).await)),
            health_check_client: HealthCheckClient::new(Self::get_channel(&node_host_name).await),
            proto_partition_client: Some(PartitionProtoClient::new(Self::get_channel(&node_host_name).await)),
            node_status: NodeStatus::AliveServing,
        }
    }

    pub async fn get_delta_data_from_peer(&self, partition_id: u32) -> Result<Response<ProbePartition>, Status> {
        //todo making all uint in proto to common type
        match &self.probe_store {
            NetworkNode::RemoteStore(remote_store) => {
                let request = tonic::Request::new(PartitionRequest {
                    partition_id: partition_id as u64,
                });
                remote_store.clone().get_partition_data(request).await
            }
            NetworkNode::LocalStore(_) => {
                Err(Status::internal("The data is already present in the current node"))
            }
        }
    }

    async fn get_channel(address: &String) -> Channel {
        println!("Channel to connect: {}", address);
        let time_out = 500;
        match Channel::from_shared(address.clone()) {
            Ok(endpoint) => endpoint,
            Err(err) => {
                panic!("Unable to parse URI {:?}", err)
            }
        }
            .timeout(Duration::from_millis(time_out))
            .connect_lazy()
    }

    pub async fn make_node_alive_not_serving(&self, hostname: &String, leader_partitions: &Vec<u32>) {
        if self.proto_partition_client.is_some() {
            println!("Making node alive and not serving... {}", &hostname);
            let request = tonic::Request::new(AnnounceAliveNotServingRequest {
                host_name: hostname.clone(),
                leader_partitions: leader_partitions.clone(),
            });
            let response = self.proto_partition_client.clone().unwrap().make_node_alive_not_serving(request).await;
            match response {
                Ok(_val) => {}
                Err(err) => {
                    print!("{}", err);
                }
            }
        }
    }

    pub async fn make_node_alive_and_serving(&self, hostname: &String, leader_partitions: Vec<u32>, follower_partitions: Vec<u32>) {
        if self.proto_partition_client.is_some() {
            println!("Making node alive and serving... {}", &hostname);
            let request = tonic::Request::new(AnnounceAliveServingRequest {
                host_name: hostname.clone(),
                leader_partitions,
                follower_partitions,
            });
            let response = self.proto_partition_client.clone().unwrap().make_node_alive_serving(request).await;
            match response {
                Ok(_val) => {}
                Err(err) => {
                    print!("{}", err);
                }
            }
        }
    }

    pub async fn read_probe_from_store(&self, partition_id: usize, is_leader: bool, probe_id: &String) -> Result<Option<Probe>, Status> {
        return match &self.probe_store {
            NetworkNode::LocalStore(store) => {
                Ok(store.get_probe(&probe_id))
            }
            NetworkNode::RemoteStore(remote_store) => {
                println!("Starting Remote read call");
                let response = Self::read_remote_store(remote_store.clone(), partition_id, is_leader, probe_id).await;
                Self::get_probe_from_response(response)
            }
        };
    }

    pub async fn write_probe_to_store(&self, partition_id: usize, is_leader: bool, probe: &Probe) -> Result<(), Status> {
        return match &self.probe_store {
            NetworkNode::LocalStore(store) => {
                store.save_probe(&probe);
                Ok(())
            }
            NetworkNode::RemoteStore(remote_store) => {
                let response = Self::write_remote_store(remote_store.clone(), partition_id, is_leader, probe).await;
                println!("Starting Remote write call");
                match response {
                    Ok(_val) => {
                        Ok(())
                    }
                    Err(err) => {
                        print!("{}", err);
                        Err(err)
                    }
                }
            }
        };
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


    fn get_probe_from_response(response: Result<Response<ProbeProto>, Status>) -> Result<Option<Probe>, Status> {
        return match response {
            Ok(val) => {
                Ok(Some(Probe::from_probe_proto(val.into_inner())))
            }
            Err(err) => {
                eprintln!("Err from remote read: {}", err);
                return Self::parse_error(err);
            }
        };
    }

    fn parse_error(err: Status) -> Result<Option<Probe>, Status> {
        match err.code() {
            Code::NotFound => {
                Ok(None)
            }
            _ => {
                Err(err)
            }
        }
    }

    pub(crate) async fn read_remote_store(mut channel: ProbeSyncClient<Channel>, partition_id: usize, is_leader: bool, probe_id: &String) -> Result<Response<ProbeProto>, Status> {
        let request = tonic::Request::new(ReadProbeRequest {
            partition_id: partition_id as u64,
            probe_id: probe_id.to_string(),
            is_leader,
        });
        channel.read_probe(request).await
    }

    pub(crate) async fn write_remote_store(mut channel: ProbeSyncClient<Channel>, partition_id: usize, is_leader: bool, probe: &Probe) -> Result<Response<WriteProbeResponse>, Status> {
        //todo try to remote the clone
        let request = tonic::Request::new(WriteProbeRequest {
            partition_id: partition_id as u64,
            probe: Some(probe.clone().to_probe_data()),
            is_leader,
        });
        channel.write_probe(request).await
        //todo handle retry logic
    }
}
