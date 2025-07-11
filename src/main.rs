use std::{env, process};
use std::collections::HashMap;
use std::env::Args;
use std::fs::File;
use std::io::Write;
use std::string::String;
use std::sync::Arc;
use std::time::Duration;

use log::LevelFilter;
use tokio::sync::{mpsc, RwLock as TrwLock};
use tonic::transport::Server as GrpcServer;

use crate::cluster::health_check_service::{HealthCheckService, ThreadMessage};
use crate::cluster::partition_manager::PartitionManager;
use crate::cluster::partition_service::PartitionService;
use crate::cluster::proto_partition_service::ProtoPartitionService;
use crate::grpc::node_manager::NodeManager;
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

pub const ANNOUNCEMENT_TIMEOUT: u64 = 150;
pub const HEALTH_CHECK_TIMEOUT: u64 = 100;
const PROBE_SYNC_TIMEOUT: u64 = 150;

#[tokio::main]
async fn main() {
    write_pid();
    let parsed_argument = parse_args(env::args());
    env_logger::builder()
        .format_timestamp_millis()
        .filter_level(LevelFilter::Info).init();
    log::info!("Args : {:?}", parsed_argument);
    let listen_port = find_port(parsed_argument.get(LISTEN_CLIENT_URLS).or(Some(&"http://localhost:9000".to_string())).expect("Missing listen-client-urls"));
    let peer_port = find_port(parsed_argument.get(LISTEN_PEER_URLS).or(Some(&"http://localhost:9001".to_string())).expect("Missing listen-peer-urls"));
    let address = format!("0.0.0.0:{}", peer_port).parse().unwrap();
    log::info!("gRPC local address: {}", address);
    let peer_host_names = get_peer_hostnames(parsed_argument, peer_port);

    log::info!("{}", peer_host_names[0]);

    let (request_tx, mut request_rx) = mpsc::channel::<ThreadMessage>(1);

    let partition_service = Arc::new(TrwLock::new(PartitionService::new(NodeManager::initialise_nodes(peer_host_names))));
    let store = Arc::new(PartitionManager {
        partition_service: partition_service.clone()
    });
    let http_server = tokio::spawn(setup_controller(listen_port, store.clone(),request_tx));

    let health_check_future = tokio::spawn(HealthCheckService::start_health_check(partition_service.clone(), request_rx));
    let probe_sync_service = ProbeSyncService { partition_manager: store.clone() };

    let grpc_server = tokio::spawn(GrpcServer::builder()
        .add_service(ProbeSyncServer::new(probe_sync_service))
        .add_service(HealthCheckServer::new(HealthCheckService {}))
        .add_service(PartitionProtoServer::new(ProtoPartitionService { partition_service }))
        .serve(address));

    print_info(listen_port, peer_port);

    tokio::try_join!(grpc_server, http_server, health_check_future);
}

fn print_info(listen_port: u16, peer_port: u16) {
    log::info!(
        "
   / __ \\____ _   _____  ___________________ _/ __/ /_
  / /_/ / __ \\ | / / _ \\/ ___/ ___/ ___/ __ `/ /_/ __/
 / _, _/ /_/ / |/ /  __/ /  / /__/ /  / /_/ / __/ /_
/_/ |_|\\____/|___/\\___/_/   \\___/_/   \\__,_/_/  \\__/
"
    );
    log::info!("Started listener on {}", listen_port);
    log::info!("Started peer listener on {}", peer_port);
}

fn get_peer_hostnames(parsed_argument: HashMap<String, String>, peer_port: u16) -> Vec<String> {
    parsed_argument.get(INITIAL_CLUSTER).or(Some(&"n1,n2,n3".to_string())).expect("Missing initial cluster")
        .split(",")
        .map(|it| format!("http://{}:{}", it, peer_port))
        .collect()
}

fn write_pid() {
    let process_id = format!("{}", process::id());
    log::info!("Current process Id is {}", &process_id);
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
