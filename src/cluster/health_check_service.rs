use std::sync::Arc;
use std::time::Duration;

use tokio::time::interval;
use tonic::{Request, Response, Status};

use crate::grpc::nodes::NodeManager;
use crate::grpc::service::cluster::{HealthCheckRequest, HealthCheckResponse};
use crate::grpc::service::cluster::health_check_server::HealthCheck;

#[derive(Debug, Default, Clone)]
pub(crate) struct HealthCheckService {
    pub(crate) nodes: Arc<NodeManager>
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

    pub fn new (nodes: Arc<NodeManager>) -> HealthCheckService {
        HealthCheckService {
            nodes
        }
    }
    pub async fn start_health_check(&self)  {
        let mut interval = interval(Duration::from_secs(1));

        loop {
            // Wait for the next tick
            interval.tick().await;

            let _: Vec<_> = self.nodes.get_nodes()
                .iter()
                .map(|&node| {
                    println!("Making health check for {}", node.host_name);
                }).collect();
        }
    }
}
