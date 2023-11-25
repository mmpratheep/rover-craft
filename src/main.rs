use std::collections::HashMap;
use std::env::Args;
use std::fs::File;
use std::io::Write;
use std::{env, process};
use std::future::Future;
use std::sync::{Arc};
use std::sync::RwLock;
use tokio::sync::RwLock as TrwLock;
use std::time::Duration;
use tokio::time::interval;

use tonic::transport::Server as GrpcServer;
use crate::cluster::health_check_service;
use crate::cluster::health_check_service::HealthCheckService;
use crate::cluster::partition_manager::PartitionManager;
use crate::cluster::partition_service::PartitionService;
use crate::grpc::nodes::NodeManager;

use crate::grpc::probe_sync_service::ProbeSyncService;
use crate::grpc::service::cluster::health_check_server::HealthCheckServer;
use crate::grpc::service::cluster::partition_proto_server::PartitionProtoServer;
use crate::grpc::service::probe_sync::probe_sync_server::ProbeSyncServer;
use crate::http::controller::setup_controller;

mod probe;
mod store;
mod cluster;
mod http;
mod grpc;

static LISTEN_PEER_URLS: &str = "listen-peer-urls";
static LISTEN_CLIENT_URLS: &str = "listen-client-urls";
static INITIAL_CLUSTER: &str = "initial-cluster";

#[tokio::main]
async fn main() {
    write_pid();
    let parsed_argument = parse_args(env::args());
    print!("Args : {:?}",parsed_argument);
    let listen_port = find_port(parsed_argument.get(LISTEN_CLIENT_URLS).or(Some(&"http://localhost:9000".to_string())).expect("Missing listen-client-urls"));
    let peer_port = find_port(parsed_argument.get(LISTEN_PEER_URLS).or(Some(&"http://localhost:9001".to_string())).expect("Missing listen-peer-urls"));
    let address = format!("0.0.0.0:{}", peer_port).parse().unwrap();
    println!("gRPC local address: {}",address);
    let peer_host_names = get_peer_hostnames(parsed_argument, peer_port);

    println!("{}",peer_host_names[0]);

    let node_manager = Arc::new(NodeManager::initialise_nodes(peer_host_names).await);
    let partition_service = Arc::new(TrwLock::new(PartitionService::new(node_manager.clone())));
    let store = Arc::new(PartitionManager{
        partition_service: partition_service.clone()
    });
    let http_server = tokio::spawn(setup_controller(listen_port, store.clone()));

    let health_check_future = tokio::spawn(HealthCheckService::start_health_check(partition_service.clone()));


    let probe_sync_service = ProbeSyncService { partition_manager: store.clone() };

    let grpc_server = tokio::spawn(GrpcServer::builder()
        .add_service(ProbeSyncServer::new(probe_sync_service))
        .add_service(HealthCheckServer::new(HealthCheckService{}))
        .add_service(PartitionProtoServer::new(partition_service))
        .serve(address));

    print_info(listen_port, peer_port);

    tokio::try_join!(grpc_server, http_server, health_check_future);
}

fn print_info(listen_port: u16, peer_port: u16) {
    println!(
        "
   / __ \\____ _   _____  ___________________ _/ __/ /_
  / /_/ / __ \\ | / / _ \\/ ___/ ___/ ___/ __ `/ /_/ __/
 / _, _/ /_/ / |/ /  __/ /  / /__/ /  / /_/ / __/ /_
/_/ |_|\\____/|___/\\___/_/   \\___/_/   \\__,_/_/  \\__/
"
    );
    println!("Started listener on {}", listen_port);
    println!("Started peer listener on {}", peer_port);
}

fn get_peer_hostnames(parsed_argument: HashMap<String, String>, peer_port: u16) -> Vec<String> {
    parsed_argument.get(INITIAL_CLUSTER).or(Some(&"n1,n2,n3".to_string())).expect("Missing initial cluster")
        .split(",")
        .map(|it| format!("http://{}:{}", it, peer_port))
        .collect()
}

fn write_pid() {
    let process_id = format!("{}", process::id());
    println!("Current process Id is {}", &process_id);
    let mut file = File::options().create(true).write(true).append(false).open("./rovercraft.pid").unwrap();
    file.write_all(&process_id.as_bytes())
        .unwrap();
}

fn parse_args(args: Args) -> HashMap<String, String> {
    let mut map: HashMap<String, String> = HashMap::new();

    let arguments: Vec<String> = args.skip(1).collect();
    for index in (0..arguments.len()).step_by(2) {
        let name = &arguments[index].replace("--", "");
        let value = &arguments[index + 1];
        map.insert(name.to_string(), value.to_string());
    }

    map
}

fn find_port(url: &String) -> u16 {
    let index = url.rfind(":").expect("Incorrect url configured");
    url[index + 1..].parse().unwrap()
}
