use std::sync::Arc;
use std::time::Duration;

use log::info;
use tokio::time::interval;
use tonic::{Request, Response, Status};

use crate::grpc::node_status::NodeStatus::Dead;
use crate::grpc::nodes::NodeManager;
use crate::grpc::service::cluster::{HealthCheckRequest, HealthCheckResponse};
use crate::grpc::service::cluster::health_check_server::HealthCheck;

#[derive(Debug, Default, Clone)]
pub(crate) struct HealthCheckService {
    pub(crate) nodes: Arc<NodeManager>,
}

#[tonic::async_trait]
impl HealthCheck for HealthCheckService {
    async fn health_check(&self, _: Request<HealthCheckRequest>) -> Result<Response<HealthCheckResponse>, Status> {
        Ok(Response::new(HealthCheckResponse {
            pong: true
        }))
    }
}

impl HealthCheckService {
    pub fn new(nodes: Arc<NodeManager>) -> HealthCheckService {
        HealthCheckService {
            nodes
        }
    }
    pub async fn start_health_check(nodes: Arc<NodeManager>) {
        let mut interval = interval(Duration::from_secs(1));

        loop {
            // Wait for the next tick
            interval.tick().await;
            println!("Starting health check ");
            for node in nodes.get_nodes() {
                println!("Making health check for {}", node.host_name);
                let result = node.do_health_check().await;
                match result {
                    Ok(_) => {
                        println!("Received response");
                        if node.node_status == Dead {
                            nodes.make_node_alive_and_serving(&node.host_name)
                        }
                    }
                    Err(err) => {
                        println!("Error occurred while doing healthcheck: {}", err);
                        nodes.make_node_dead(&node.host_name)
                    }
                }

            }
        }
    }
}
