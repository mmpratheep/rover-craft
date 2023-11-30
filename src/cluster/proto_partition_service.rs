use std::sync::Arc;
use log::error;
use tokio::sync::RwLock;
use tonic::{Request, Response, Status};
use crate::cluster::partition_service::PartitionService;
use crate::grpc::node::Node;
use crate::grpc::service::cluster::{AnnounceAliveServingRequest, AnnounceAliveNotServingRequest, Empty};
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
        println!("Before alive and not serving, leaders {:?}",self.partition_service.read().await.leader_nodes);
        println!("Before alive and not serving, followers {:?}",self.partition_service.read().await.follower_nodes);
        let node_host_name = req_data.host_name;
        let read_guard = self.partition_service.read().await;
        read_guard.nodes.make_node_alive_and_not_serving(&node_host_name);
        match read_guard.leader_nodes.write() {
            Ok(mut l_nodes) => {
                println!("Got write lock for leader nodes");
                match read_guard.follower_nodes.write() {
                    Ok(mut f_nodes) => {
                        println!("Got write lock for follower nodes");
                        for i in req_data.leader_partitions {
                            let index = i as usize;
                            f_nodes[index] = l_nodes[index].node.clone();
                            //when node is back, we need to keep the delta data there in the leader, and move the data to the follower and create connection to from leader partition
                            l_nodes[index].node = Arc::new(Node::new(
                                read_guard.nodes.get_node(&node_host_name).unwrap()));
                            //todo check whether it handles properly for the second node assignment
                        }
                    }
                    Err(err) => {
                        println!("Failed to get write lock for followers {}",err)
                    }
                }
            }
            Err(err) => {
                println!("Failed to get write lock for leaders {}",err)
            }
        };

        println!("Post alive and not serving, leaders {:?}",self.partition_service.read().await.leader_nodes);
        println!("Post alive and not serving, followers {:?}",self.partition_service.read().await.follower_nodes);

        Ok(Response::new(Empty {}))
    }

    async fn make_node_alive_serving(&self, request: Request<AnnounceAliveServingRequest>) -> Result<Response<Empty>, Status> {
        let req_data = request.into_inner();
        println!("Make alive and serving host: {} : leader {:?}, follower: {:?}", req_data.host_name, req_data.leader_partitions,req_data.follower_partitions);
        println!("Before alive and serving, leaders {:?}",self.partition_service.read().await.leader_nodes);
        let node_host_name = req_data.host_name;
        let read_guard = self.partition_service.read().await;
        read_guard.nodes.make_node_alive_and_not_serving(&node_host_name);
        match read_guard.leader_nodes.write() {
            Ok(mut l_nodes) => {
                for i in req_data.leader_partitions {
                    l_nodes[i as usize].remove_delta_data();
                }
                for i in req_data.follower_partitions{
                    l_nodes[i as usize].remove_delta_data()
                }
            }
            Err(err) => {
                println!("Failed to get write lock for leaders {}",err)
            }
        }
        println!("Post alive and serving, leaders {:?}",self.partition_service.read().await.leader_nodes);
        Ok(Response::new(Empty {}))
    }
}


