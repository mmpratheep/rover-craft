use tonic::{Request, Response, Status};
use crate::grpc::service::cluster::health_check_server::HealthCheck;
use crate::grpc::service::cluster::{HealthCheckRequest, HealthCheckResponse};


#[derive(Debug, Default)]
pub(crate) struct HealthCheckService {}

#[tonic::async_trait]
impl HealthCheck for HealthCheckService {
    async fn health_check(&self, request: Request<HealthCheckRequest>) -> Result<Response<HealthCheckResponse>, Status> {
        todo!()
    }
}
