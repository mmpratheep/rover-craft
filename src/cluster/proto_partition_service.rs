use std::sync::Arc;

use tokio::sync::RwLock;
use tonic::{Request, Response, Status};

use crate::cluster::partition_service::PartitionService;
use crate::grpc::service::cluster::{AnnounceAliveNotServingRequest, AnnounceAliveServingRequest, Empty};
use crate::grpc::service::cluster::partition_proto_server::PartitionProto;

#[derive(Debug, Default)]
pub struct ProtoPartitionService {
    pub(crate) partition_service: Arc<RwLock<PartitionService>>
}

#[tonic::async_trait]
impl PartitionProto for ProtoPartitionService {
    async fn make_node_alive_not_serving(&self, request: Request<AnnounceAliveNotServingRequest>) -> Result<Response<Empty>, Status> {
        //todo
        let req_data = request.into_inner();
        println!("Make alive and not serving host: {} : partitions {:?}", req_data.host_name, req_data.leader_partitions);
        self.partition_service.read().await.make_node_alive_and_not_serving(&req_data).await;
        Ok(Response::new(Empty {}))
    }

    async fn make_node_alive_serving(&self, request: Request<AnnounceAliveServingRequest>) -> Result<Response<Empty>, Status> {
        let req_data = request.into_inner();
        self.partition_service.read().await.make_node_alive_serving(&req_data).await;
        Ok(Response::new(Empty {}))
    }
}


