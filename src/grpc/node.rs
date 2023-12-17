use std::fmt;
use std::sync::Arc;
use std::time::{Duration};

use log::debug;
use tokio::time::Instant;
use tonic::{Code, Response, Status};
use tonic::transport::Channel;

use crate::cluster::network_node::NetworkNode;
use crate::grpc::node_ref::{get_current_node_hostname, NodeRef};
use crate::grpc::node_status::NodeStatus;
use crate::grpc::service::probe_sync::{PartitionRequest, ProbePartition, ProbeProto, ReadProbeRequest, WriteProbeRequest, WriteProbeResponse};
use crate::grpc::service::probe_sync::probe_sync_client::ProbeSyncClient;
use crate::probe::probe::Probe;
use crate::PROBE_SYNC_TIMEOUT;
use crate::store::memory_store::MemoryStore;

#[derive(Debug)]
//todo remove this clone
pub struct Node {
    pub node_ref: Arc<NodeRef>,
    probe_store: NetworkNode,
}

impl fmt::Display for Node {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", &self.node_ref)
    }
}


impl Node {
    pub fn node_status(&self) -> NodeStatus {
        self.node_ref.node_status.read().unwrap().clone()
    }

    // pub fn make_node_down(&mut self) {
    //     let mut guard = self.node_status.write().unwrap();
    //     *guard =  NodeStatus::Dead;
    // }

    pub fn is_node_not_down(&self) -> bool {
        self.node_status() != NodeStatus::Dead
    }
    pub fn is_node_down(&self) -> bool {
        self.node_status() == NodeStatus::Dead
    }


    pub fn is_current_node_ip(node_ip: &String) -> bool {
        let current_node_host_name = get_current_node_hostname().into_string().unwrap();
        debug!("current {} node {} result {}", current_node_host_name, node_ip, node_ip.contains(&current_node_host_name));
        node_ip.contains(&current_node_host_name)
    }

    pub fn is_current_node(&self) -> bool{
        Self::is_current_node_ip(&self.node_ref.host_name)
    }

    pub fn update_with_delta_data(&self, partition : ProbePartition){
        match &self.probe_store {
            NetworkNode::LocalStore(store) => {
                store.de_serialise_and_update(partition.probe_array);
            }
            NetworkNode::RemoteStore(_) => {
                log::error!("The data is not present in the current node")
            }
        }

    }

    pub(crate) fn new(node_ref: Arc<NodeRef>) -> Self {
        if Self::is_current_node_ip(&node_ref.host_name) {
            return
                Self {
                    node_ref:node_ref.clone(),
                    probe_store: NetworkNode::LocalStore(MemoryStore::new()),
                };
        }
        Self {
            node_ref:node_ref.clone(),
            probe_store: NetworkNode::RemoteStore(ProbeSyncClient::new(get_channel(&node_ref.host_name, PROBE_SYNC_TIMEOUT))),
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


    pub async fn read_probe_from_store(&self, partition_id: usize, is_leader: bool, probe_id: &String) -> Result<Option<Probe>, Status> {
        return match &self.probe_store {
            NetworkNode::LocalStore(store) => {
                Ok(store.get_probe(&probe_id))
            }
            NetworkNode::RemoteStore(remote_store) => {
                log::info!("Starting Remote read call: {}",probe_id);
                let response = Self::read_remote_store(remote_store.clone(), partition_id, is_leader, probe_id).await;
                Self::get_probe_from_response(response)
            }
        };
    }

    pub async fn write_probe_to_store(&self, partition_id: usize, is_leader: bool, probe: &Probe) -> Result<(), Status> {
        return match &self.probe_store {
            NetworkNode::LocalStore(store) => {
                log::info!("Writing to local: {}",probe.event_id);
                store.save_probe(&probe);
                Ok(())
            }
            NetworkNode::RemoteStore(remote_store) => {
                let response = Self::write_remote_store(remote_store.clone(), partition_id, is_leader, probe).await;
                log::info!("Starting Remote write call {}",probe.event_id);
                match response {
                    Ok(_val) => {
                        Ok(())
                    }
                    Err(err) => {
                        log::error!("Error: {} {}",probe.event_id, err);
                        Err(err)
                    }
                }
            }
        };
    }


    fn get_probe_from_response(response: Result<Response<ProbeProto>, Status>) -> Result<Option<Probe>, Status> {
        return match response {
            Ok(val) => {
                Ok(Some(Probe::from_probe_proto(val.into_inner())))
            }
            Err(err) => {
                log::error!("Err from remote read for: {}", err);
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
        let start_time = Instant::now();
        log::info!("Before calling grpc read {} {:?}",probe_id,start_time);
        let result = channel.read_probe(request).await;
        let end_time = Instant::now();
        let duration = end_time - start_time;
        log::info!("Latency {} {:?}",probe_id,duration);
        return result;
    }

    pub(crate) async fn write_remote_store(mut channel: ProbeSyncClient<Channel>, partition_id: usize, is_leader: bool, probe: &Probe) -> Result<Response<WriteProbeResponse>, Status> {
        //todo try to remote the clone
        let request = tonic::Request::new(WriteProbeRequest {
            partition_id: partition_id as u64,
            probe: Some(probe.clone().to_probe_data()),
            is_leader,
        });
        let start_time = Instant::now();
        log::info!("Before calling grpc write {} {:?}",probe.event_id,start_time);
        let result = channel.write_probe(request).await;
        let end_time = Instant::now();
        let duration = end_time - start_time;
        log::info!("Latency {} {:?}",probe.event_id,duration);
        return result;
        //todo handle retry logic
    }
}

pub  fn get_channel(address: &String, time_out: u64) -> Channel {
    log::info!("Channel to connect: {}", address);
    match Channel::from_shared(address.clone()) {
        Ok(endpoint) => endpoint,
        Err(err) => {
            panic!("Unable to parse URI {:?}", err)
        }
    }
        .timeout(Duration::from_millis(time_out))
        .connect_timeout(Duration::from_millis(50))
        .connect_lazy()
}
