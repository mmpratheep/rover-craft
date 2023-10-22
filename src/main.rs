use crate::store::memory_store::MemoryStore;
use tonic::transport::Server as GrpcServer;

use probe_sync::{ReadProbeRequest, ReadProbeResponse, WriteProbeRequest, WriteProbeResponse,
                 probe_sync_server::{ProbeSync, ProbeSyncServer}};
use tonic::{Request, Response, Status};
use crate::probe::probe::Probe;


mod probe;
mod store;

mod cluster;
mod http;

pub mod probe_sync {
    tonic::include_proto!("probe_sync");
}


#[tokio::main]
async fn main() {
    let store = MemoryStore::new();
    // http::controller::setup_controller(9000, Arc::clone(&store))
    //     .await;

    let address = "[::1]:8080".parse().unwrap();
    // let voting_service = ProbeSyncService{ store };

    let grpc_server = GrpcServer::builder().add_service(ProbeSyncServer::new(store))
        .serve(address)
        .await;


    // tokio::try_join!(grpc_server, http_server)?;
}


// #[derive(Debug, Default)]
// pub struct ProbeSyncService {
//     store: MemoryStore,
// }

#[tonic::async_trait]
impl ProbeSync for MemoryStore {
    async fn read_probe(&self, request: Request<ReadProbeRequest>) -> Result<Response<ReadProbeResponse>, Status> {
        let read_probe_req = request.into_inner();


        match self.get_probe(&read_probe_req.probe_id) {
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

        self.save_probe(&Probe::create_probe_from_write_probe_request(write_probe_req));
        //todo check whether the above statement will always pass without any error, if so,then handle that scenario
        Ok(Response::new(WriteProbeResponse { confirmation: true }))
    }
}

