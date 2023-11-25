use std::sync::Arc;
use log::error;
use tokio::sync::RwLock;
use tonic::{Request, Response, Status};
use crate::cluster::partition_service::PartitionService;
use crate::grpc::service::cluster::{AnnounceAliveRequest, Empty};
use crate::grpc::service::cluster::partition_proto_server::PartitionProto;

#[derive(Debug, Default)]
pub struct ProtoPartitionService {
    pub(crate) partition_service: Arc<RwLock<PartitionService>>
}

#[tonic::async_trait]
impl PartitionProto for ProtoPartitionService {
    async fn make_node_alive_not_serving(&self, request: Request<AnnounceAliveRequest>) -> Result<Response<Empty>, Status> {
        //todo
        let req_data = request.into_inner();
        let node_host_name = req_data.host_name;
        let read_guard = self.partition_service.read().await;
        read_guard.nodes.make_node_alive_and_not_serving(&node_host_name);
        match read_guard.leader_nodes.write() {
            Ok(mut l_nodes) => {
                match read_guard.follower_nodes.write() {
                    Ok(mut f_nodes) => {
                        for i in req_data.partitions {
                            let index = i as usize;
                            f_nodes[index] = l_nodes[index].node.clone();
                            l_nodes[index].node = read_guard.nodes.get_node(node_host_name.clone()).unwrap();
                            //todo check whether it handles properly for the second node assignment
                        }
                    }
                    Err(err) => {
                        error!("Failed to get write lock for followers {}",err)
                    }
                }
            }
            Err(err) => {
                error!("Failed to get write lock for leaders {}",err)
            }
        };
        Ok(Response::new(Empty {}))
    }

    async fn make_node_alive_serving(&self, request: Request<AnnounceAliveRequest>) -> Result<Response<Empty>, Status> {
        let req_data = request.into_inner();
        let node_host_name = req_data.host_name;
        let read_guard = self.partition_service.read().await;
        read_guard.nodes.make_node_alive_and_not_serving(&node_host_name);
        match read_guard.leader_nodes.write() {
            Ok(mut l_nodes) => {
                for i in req_data.partitions {
                    l_nodes[i as usize].remove_delta_data();
                }
            }
            Err(err) => {
                error!("Failed to get write lock for leaders {}",err)
            }
        }
        Ok(Response::new(Empty {}))
    }
}


