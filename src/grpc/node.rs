use std::ffi::OsString;
use std::time::Duration;
use tonic::{Code, Response, Status};
use tonic::codegen::tokio_stream::StreamExt;
use tonic::transport::{Channel, Error};
use warp::hyper::client::connect::Connect;
use crate::cluster::network_node::NetworkNode;
use crate::grpc::node_status::NodeStatus;
use crate::grpc::service::probe_sync::probe_sync_client::ProbeSyncClient;
use crate::grpc::service::probe_sync::{ProbeProto, ReadProbeRequest, WriteProbeResponse};
use crate::probe::probe::Probe;
use crate::store::memory_store::MemoryStore;

#[derive(Clone, Debug)]
//todo remove this clone
pub struct Node {
    pub host_name: String,
    probe_store: NetworkNode,
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
        Self::get_current_node_hostname().eq_ignore_ascii_case(&node_ip)
    }

    fn get_current_node_hostname() -> OsString {
        hostname::get().unwrap().to_os_string()
    }

    pub(crate) async fn new(node_host_name: String) -> Result<Self, Error> {
        if Self::is_same_node(&node_host_name) {
            return Ok(
                Self {
                    host_name: node_host_name,
                    probe_store: NetworkNode::LocalStore(MemoryStore::new()),
                    node_status: NodeStatus::AliveServing,
                }
            );
        }
        let client = Self::get_channel(&node_host_name).await?;
        Ok(Self {
            host_name: node_host_name,
            probe_store: NetworkNode::RemoteStore(client),
            node_status: NodeStatus::AliveServing,
        })
    }

    async fn get_channel(address: &String) -> Result<ProbeSyncClient<Channel>, Error> {
        let time_out = 500;
        let channel = match Channel::from_shared(address.clone()) {
            Ok(endpoint) => endpoint,
            Err(err) => {
                panic!("Unable to parse URI {:?}", err)
            }
        }
            .timeout(Duration::from_millis(time_out))
            .connect().await?;

        Ok(ProbeSyncClient::new(channel))
    }

    pub async fn read_probe_from_store(&self, probe_id: &String) -> Result<Option<Probe>, Status> {
        return match &self.probe_store {
            NetworkNode::LocalStore(store) => {
                Ok(store.get_probe(&probe_id))
            }
            NetworkNode::RemoteStore(remote_store) => {
                let response = Self::read_remote_store(remote_store.clone(), probe_id).await;
                Self::get_probe_from_response(response)
            }
        };
    }

    pub async fn write_probe_to_store(&self, probe: &Probe) -> Result<(), Status> {
        return match &self.probe_store {
            NetworkNode::LocalStore(store) => {
                store.save_probe(&probe);
                Ok(())
            }
            NetworkNode::RemoteStore(remote_store) => {
                let response = Self::write_remote_store(remote_store.clone(), probe).await;
                match response {
                    Ok(_val) => {
                        Ok(())
                    }
                    Err(err) => {
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

    pub(crate) async fn read_remote_store(mut channel: ProbeSyncClient<Channel>, probe_id: &String) -> Result<Response<ProbeProto>, Status> {
        let request = tonic::Request::new(ReadProbeRequest {
            probe_id: probe_id.to_string()
        });
        channel.read_probe(request).await
    }

    pub(crate) async fn write_remote_store(mut channel: ProbeSyncClient<Channel>, probe: &Probe) -> Result<Response<WriteProbeResponse>, Status> {
        //todo try to remote the clone
        let request = tonic::Request::new(probe.clone().to_probe_data());
        channel.write_probe(request).await
        //todo handle retry logic
    }
}
