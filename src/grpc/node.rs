use std::time::Duration;
use tonic::{Response, Status};
use tonic::codegen::tokio_stream::StreamExt;
use tonic::transport::{Channel};
use warp::hyper::client::connect::Connect;
use crate::grpc::node_status::NodeStatus;
use crate::grpc::service::probe_sync::probe_sync_client::ProbeSyncClient;
use crate::grpc::service::probe_sync::{ReadProbeRequest, ProbeData, WriteProbeResponse};
use crate::probe::probe::Probe;

pub struct Node {
    address: String,
    connection: ProbeSyncClient<Channel>,
    node_status: NodeStatus,
}

impl Node {
    pub(crate) async fn new(address: String) -> Result<Self, tonic::transport::Error> {
        let time_out = 1000;
        let address_clone = address.clone();
        let channel = match Channel::from_shared(address_clone) {
            Ok(endpoint) => endpoint,
            Err(err) => {
                panic!("Unable to parse URI {:?}", err)
            }
        }
            .timeout(Duration::from_millis(time_out))
            .connect().await?;
        let client = ProbeSyncClient::new(channel);
        Ok(Self {
            address,
            connection: client,
            node_status: NodeStatus::AliveServing,
        })
    }

    pub(crate) async fn read_probe(mut self, probe_id: String) -> Result<Response<ProbeData>, Status> {
        let request = tonic::Request::new(ReadProbeRequest {
            probe_id
        });
        self.connection.read_probe(request).await
    }

    pub(crate) async fn write_probe(mut self, probe: Probe) -> Result<Response<WriteProbeResponse>, Status> {
        let request = tonic::Request::new(probe.to_probe_data());
        self.connection.write_probe(request).await
        //todo handle retry logic
    }
}