use std::ffi::OsString;
use std::ops::Deref;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use log::{debug, error, info};
use tonic::{Code, Response, Status};
use tonic::transport::{Channel};
use crate::cluster::network_node::NetworkNode;
use crate::grpc::node_ref::{get_current_node_hostname, NodeRef};
use crate::grpc::node_status::NodeStatus;
use crate::grpc::service::probe_sync::probe_sync_client::ProbeSyncClient;
use crate::grpc::service::probe_sync::{PartitionRequest, ProbePartition, ProbeProto, ReadProbeRequest, WriteProbeRequest, WriteProbeResponse};
use crate::probe::probe::Probe;
use crate::store::memory_store::MemoryStore;

#[derive(Debug)]
//todo remove this clone
pub struct Node {
    pub node_ref: Arc<NodeRef>,
    probe_store: NetworkNode,
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
                println!("The data is not present in the current node")
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
            probe_store: NetworkNode::RemoteStore(ProbeSyncClient::new(get_channel(&node_ref.host_name))),
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
                println!("Starting Remote read call: ");
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
                        println!("{}", err);
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
                println!("Err from remote read for: {}", err);
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

pub  fn get_channel(address: &String) -> Channel {
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
