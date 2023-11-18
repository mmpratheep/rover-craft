use std::sync::Arc;
use log::error;
use crate::grpc::node::Node;

#[derive(Debug, Default)]
pub struct Nodes {
    nodes: Vec<Arc<Node>>,
}

impl Nodes {
    pub async fn initialise_nodes(node_hosts: Vec<String>) -> Self {
        let mut nodes: Vec<Arc<Node>> = Vec::with_capacity(node_hosts.len());
        for node_host in node_hosts {
            nodes.push(
                Arc::new(
                    Node::new(node_host).await
                        .expect("Unable to make connection to node")
                )
            );
        }
        nodes.sort_by_key(|it| it.host_name.clone());
        Nodes {
            nodes
        }
    }

    fn sort_nodes(&mut self) {
        self.nodes.sort_by_key(|it| it.host_name);
    }

    pub async fn get_node(&self, node_host: String) -> Arc<Node> {
        return match self.nodes.iter().find(|&node| node.host_name == node_host) {
            Some(node) => {
                Arc::clone(node)
            }
            None => {
                error!("Unable to get existing node, So creating new one");
                Arc::new(Node::new(node_host)
                    .await
                    .expect("Unable to connect to node host"))
            }
        };
    }
}