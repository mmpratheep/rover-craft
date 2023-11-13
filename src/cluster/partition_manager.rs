use log::error;
use tonic::Status;
use crate::cluster::partition_service::PartitionService;
use crate::grpc::node::Node;
use crate::probe::probe::Probe;

struct PartitionManager {
    partition_service: PartitionService,
    nodes: Vec<Node>,
    local_ip: String,
}

impl PartitionManager {

    // fn new() -> Self{
    //     PartitionManager{
    //         current_ip: local_ip().unwrap();
    //     }
    // }


    fn publish_partition_re_balance() {}

    fn get_leader(&mut self) -> Option<&Node> {
        // health_check_and_update_status(&self.nodes);
        self.nodes.sort_by_key(| it | it.address.clone());
        self.nodes.get(0)
    }

    fn get_partition_assignment_from_leader(){

    }

    fn re_balance_partition() {}

    pub async fn get_probe(&self, probe_id: String) -> Option<Probe>{
        //todo clone
        let (leader_node,follower_node) = self.partition_service.get_partition_nodes(&probe_id);
        let result = leader_node.unwrap().read_probe_from_store(&probe_id).await;
        return match result {
            Ok(probe) => {
                probe
            }
            Err(err) => {
                error!("Exception {}",err);
                let fallback_result = follower_node.unwrap().read_probe_from_store(&probe_id).await;
                match fallback_result {
                    Ok(fallback_probe) => {
                        return fallback_probe
                    }
                    Err(_) => {
                        None
                    }
                }
                //todo handle re-balance
            }
        }
    }




    pub async fn upsert_value(&self, probe: Probe) -> Option<Probe> {
        //todo WIP..
        //todo try to send leader and follower write request in parallel
        //todo what happens when leader is down
        let (leader_node,follower_node) = self.partition_service.get_partition_nodes(&probe.probe_id);
        let result = leader_node.unwrap().write_probe_to_store(&probe).await;
        let res = follower_node.unwrap().write_probe_to_store(&probe).await;
        return match result {
            Ok(_probe_response) => {
                Some(probe)
            }
            Err(_err) => {
                None
            }
        };
    }

    fn is_current_node(&self, node_ip: &String) -> bool {
        *node_ip == self.local_ip
    }
}
