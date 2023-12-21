use std::ops::Deref;
use std::sync::{Arc};
use log::{error, info};
use crate::grpc::node_ref::NodeRef;
use crate::grpc::node_status::NodeStatus;

#[derive(Debug, Default)]
pub struct NodeManager {
    pub nodes: Vec<Arc<NodeRef>>,
}

impl NodeManager {
    pub fn initialise_nodes(node_hosts: Vec<String>) -> Self {
        let mut nodes: Vec<Arc<NodeRef>> = Vec::with_capacity(node_hosts.len());
        for node_host in node_hosts {
            nodes.push(
                Arc::new(
                    NodeRef::new(node_host.clone())
                )
            );
        }
        nodes.sort_by_key(|it| it.host_name.clone());
        NodeManager {
            nodes
        }
    }

    fn sort_nodes(&mut self) {
        self.nodes.sort_by(|a, b| a.host_name.cmp(&b.host_name));
    }

    pub fn make_node_alive_and_serving(&self, node_host: &String) -> bool{
        let node = self.get_single_node(node_host);
        if *node.unwrap().node_status.read().unwrap().deref() == NodeStatus::AliveServing {
            return false;
        }
        self.change_node_state(node, NodeStatus::AliveServing)
    }

    pub fn is_current_node_down(&self) -> bool {
        self.get_peers().iter()
            .all(|node| *node.node_status.read().unwrap().deref() == NodeStatus::Dead)
    }

    pub fn make_node_dead(&self, node_host: &String){
        let node = self.get_single_node(node_host);
        self.change_node_state(node, NodeStatus::Dead);
    }

    pub fn make_node_alive_and_not_serving(&self, node_host: &String) -> bool{
        let node = self.get_single_node(node_host);
        if *node.unwrap().node_status.read().unwrap().deref() == NodeStatus::AliveServing {
            return false;
        }
        self.change_node_state(node, NodeStatus::AliveNotServing)
    }

    fn change_node_state(&self, node: Option<&Arc<NodeRef>>, status: NodeStatus) -> bool{
        let mut guard = node.unwrap().node_status.write().unwrap();
        *guard = status;
        //todo remove below while taking to prod
        drop(guard);
        log::info!("After state change host: {}, status: {:?}", node.unwrap().host_name, node.unwrap().node_status.read());
        return true;
    }

    pub fn get_node(&self, node_host: &String) -> Option<Arc<NodeRef>> {
        return match self.get_single_node(node_host) {
            Some(node) => {
                Some(node.clone())
            }
            None => {
                log::error!("Unable to get existing node, So creating new one");
                None
            }
        };
    }

    pub fn get_current_node(&self) -> Option<&Arc<NodeRef>> {
        self.nodes.iter().find(|&node| node.is_current_node())
    }

    pub fn get_peers(&self) -> Vec<&Arc<NodeRef>> {
        self.nodes.iter()
            .filter(|node_ref| !node_ref.is_current_node())
            .collect()
    }

    fn get_single_node(&self, node_host: &String) -> Option<&Arc<NodeRef>> {
        self.nodes.iter()
            .find(|&node|
                node.host_name == *node_host
            )
    }
}