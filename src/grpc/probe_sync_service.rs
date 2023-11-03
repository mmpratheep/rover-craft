use std::sync::Arc;
use tonic::{Request, Response, Status};
use crate::grpc::service::probe_sync::{ReadProbeRequest, ReadProbeResponse, WriteProbeRequest, WriteProbeResponse};
use crate::grpc::service::probe_sync::probe_sync_server::ProbeSync;
use crate::probe::probe::Probe;
use crate::store::memory_store::MemoryStore;

#[derive(Debug, Default)]
pub struct ProbeSyncService {
    pub(crate) store: Arc<MemoryStore>,
}


#[tonic::async_trait]
impl ProbeSync for ProbeSyncService {
    async fn read_probe(&self, request: Request<ReadProbeRequest>) -> Result<Response<ReadProbeResponse>, Status> {
        let read_probe_req = request.into_inner();

        match self.store.get_probe(&read_probe_req.probe_id) {
            Some(probe) => {
                Ok(Response::new(probe.to_read_probe_response()))
            }
            None => {
                Err(Status::new(tonic::Code::NotFound, "No probe found"))
            }
        }
    }

    async fn write_probe(&self, request: Request<WriteProbeRequest>) -> Result<Response<WriteProbeResponse>, Status> {
        let write_probe_req = request.into_inner();

        self.store.save_probe(&Probe::from_write_probe_request(write_probe_req));
        //todo check whether the above statement will always pass without any error, if so,then handle that scenario
        Ok(Response::new(WriteProbeResponse { confirmation: true }))
    }
}

