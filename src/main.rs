use std::sync::Arc;
use crate::store::memory_store::MemoryStore;
use tonic::transport::Server as GrpcServer;
use crate::grpc::probe_sync_service::probe_sync::probe_sync_server::ProbeSyncServer;
use crate::grpc::probe_sync_service::ProbeSyncService;


mod probe;
mod store;

mod cluster;
mod http;
mod grpc;


#[tokio::main]
async fn main() {
    let store = Arc::new(MemoryStore::new());
    http::controller::setup_controller(9000, store.clone())
        .await;

    let address = "[::1]:8080".parse().unwrap();
    let probe_sync_service = ProbeSyncService{ store: Arc::clone(&store) };

    let grpc_server = GrpcServer::builder().add_service(ProbeSyncServer::new(probe_sync_service))
        .serve(address)
        .await;

    // tokio::try_join!(grpc_server, http_server)?;
}


