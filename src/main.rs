use std::collections::HashMap;
use std::env::Args;
use std::fs::File;
use std::io::Write;
use std::{env, process};
use std::sync::Arc;

use tonic::transport::Server as GrpcServer;

use crate::grpc::probe_sync_service::ProbeSyncService;
use crate::grpc::service::probe_sync::probe_sync_server::ProbeSyncServer;
use crate::http::controller::setup_controller;
use crate::store::memory_store::MemoryStore;

mod probe;
mod store;
mod cluster;
mod http;
mod grpc;

#[tokio::main]
async fn main() {
    write_pid();
    let store = Arc::new(MemoryStore::new());
    let http_server = tokio::spawn(setup_controller(9000, store.clone()));

    let address = "[::1]:9001".parse().unwrap();
    let probe_sync_service = ProbeSyncService{ store: store.clone() };

    let grpc_server = tokio::spawn(GrpcServer::builder()
        .add_service(ProbeSyncServer::new(probe_sync_service))
        .serve(address));

    tokio::try_join!(grpc_server, http_server);
}

fn write_pid() {
    let process_id= format!("{}", process::id());
    println!("Current process Id is {}", &process_id);
    let mut file= File::options().create(true).write(true).append(false).open("./rovercraft.pid").unwrap();
    file.write_all(&process_id.as_bytes())
        .unwrap();
}
