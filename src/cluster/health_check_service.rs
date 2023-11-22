use std::sync::Arc;
use std::time::Duration;

use clokwerk::{Scheduler, TimeUnits};
use log::info;
use tonic::{Request, Response, Status};

use crate::grpc::nodes::NodeManager;
use crate::grpc::service::cluster::{HealthCheckRequest, HealthCheckResponse};
use crate::grpc::service::cluster::health_check_server::HealthCheck;

#[derive(Debug, Default, Clone)]
pub(crate) struct HealthCheckService {
    nodes: Arc<NodeManager>,
    scheduler: Scheduler,
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
    pub fn new(nodes: Arc<NodeManager>) -> Self {
        let mut scheduler = Scheduler::new();
        scheduler.every(1.seconds())
            .run(|| println!("Periodic task"));

        HealthCheckService {
            nodes,
            scheduler,
        }
    }

    pub fn start_health_check(&self) -> () {
        let thread_handle = self.scheduler.watch_thread(Duration::from_millis(100));
    }
}
