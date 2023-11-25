use std::slice::Iter;
use std::sync::Arc;
use log::error;
use crate::grpc::node::Node;
use crate::grpc::node_status::NodeStatus;

#[derive(Debug, Default)]
pub struct NodeManager {
    pub nodes: Vec<Arc<Node>>,
}

impl NodeManager {
    pub async fn initialise_nodes(node_hosts: Vec<String>) -> Self {
        let mut nodes: Vec<Arc<Node>> = Vec::with_capacity(node_hosts.len());
        for node_host in node_hosts {
            nodes.push(
                Arc::new(
                    Node::new(node_host.clone()).await
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

    pub fn make_node_alive_and_serving(&self, node_host: &String) {
        self.change_node_state(node_host, NodeStatus::AliveServing);
    }

    pub fn make_node_dead(&self, node_host: &String) {
        self.change_node_state(node_host, NodeStatus::Dead);
    }

    pub fn make_node_alive_and_not_serving(&self, node_host: &String) {
        self.change_node_state(node_host, NodeStatus::AliveNotServing);
    }

    fn change_node_state(&self, node_host: &String, status: NodeStatus) {
        self.get_single_node(node_host)
            .map(|it| {
                let mut node = it.clone();
                Arc::make_mut(&mut node).node_status = status;
            }
            );
    }

    pub fn get_node(&self, node_host: String) -> Option<Arc<Node>> {
        return match self.get_single_node(&node_host) {
            Some(node) => {
                Some(Arc::clone(node))
            }
            None => {
                error!("Unable to get existing node, So creating new one");
                None
            }
        };
    }

    pub fn get_nodes(&self) -> Vec<&Arc<Node>> {
        self.nodes.iter()
          //.filter(|node| !node.is_current_node())
            .collect()

    }

    fn get_single_node(&self, node_host: &String) -> Option<&Arc<Node>> {
        self.nodes.iter()
            .find(|&node|
                node.host_name == *node_host
            )
    }


}