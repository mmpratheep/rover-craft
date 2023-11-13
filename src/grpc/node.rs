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
    pub address: String,
    probe_store: NetworkNode,
    pub node_status: NodeStatus,
}

impl Node {

    pub(crate) async fn new(node_ip: String, current_node_ip: String) -> Result<Self, Error> {
        if node_ip == current_node_ip {
            return Ok(
                Self {
                    address: node_ip,
                    probe_store: NetworkNode::LocalStore(MemoryStore::new()),
                    node_status: NodeStatus::AliveServing,
                }
            );
        }
        let client = Self::get_channel(&node_ip).await?;
        Ok(Self {
            address: node_ip,
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
