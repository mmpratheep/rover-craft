use std::sync::Arc;
use probe_sync::{ReadProbeRequest, ReadProbeResponse, WriteProbeRequest, WriteProbeResponse,
                 probe_sync_server::{ProbeSync}};
use tonic::{Request, Response, Status};
use crate::probe::probe::Probe;
use crate::store::memory_store::MemoryStore;


pub mod probe_sync {
    tonic::include_proto!("probe_sync");
}

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
                Err(Status::new(tonic::Code::NotFound, "Invalid vote provided"))
            }
        }
    }

    async fn write_probe(&self, request: Request<WriteProbeRequest>) -> Result<Response<WriteProbeResponse>, Status> {
        let write_probe_req = request.into_inner();

        self.store.save_probe(&Probe::create_probe_from_write_probe_request(write_probe_req));
        //todo check whether the above statement will always pass without any error, if so,then handle that scenario
        Ok(Response::new(WriteProbeResponse { confirmation: true }))
    }
}

