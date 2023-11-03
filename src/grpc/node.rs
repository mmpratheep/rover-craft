use std::error::Error;
use std::time::Duration;
use tonic::{Response, Status};
use tower::timeout::Timeout;
use tonic::transport::Channel;
use crate::grpc::service::probe_sync::probe_sync_client::ProbeSyncClient;
use crate::grpc::service::probe_sync::{ReadProbeRequest, ReadProbeResponse, WriteProbeResponse};
use crate::probe::probe::Probe;

struct Node {
    address: String,
    connection: ProbeSyncClient<Timeout<Result<Channel, dyn Error>>>,
}

impl Node {
    async fn new(address: String) -> Self {
        let channel = Channel::from_static(&address).connect().await;
        let TIME_OUT = 1000;
        let timeout_channel = Timeout::new(channel, Duration::from_millis(TIME_OUT));
        let client = ProbeSyncClient::new(timeout_channel);
        Node {
            address,
            connection: client,
        }
    }

    async fn read_probe(mut self, probe_id: String) -> Result<Response<ReadProbeResponse>, Status> {
        let request = tonic::Request::new(ReadProbeRequest {
            probe_id
        });

        self.connection.read_probe(request).await
    }

    async fn write_probe(mut self, probe: Probe) -> Result<Response<WriteProbeResponse>, Status> {
        let request = tonic::Request::new(probe.to_write_probe_request());

        self.connection.write_probe(request).await
        //todo handle retry logic
    }
}