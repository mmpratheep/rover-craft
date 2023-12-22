use std::sync::Arc;
use tokio::sync::mpsc::Sender;
use warp::{Filter, Rejection, Reply};
use warp::http::StatusCode;
use warp::reply::{json, with_status};
use crate::cluster::health_check_service::ThreadMessage;
use crate::cluster::partition_manager::PartitionManager;
use crate::grpc::node_status::NodeStatus;

use crate::http::probe_request::ProbeRequest;
use crate::probe::probe::Probe;
use crate::store::memory_store::MemoryStore;

pub fn post_json() -> impl Filter<Extract=(ProbeRequest, ), Error=Rejection> + Clone {
    warp::body::content_length_limit(1024 * 16).and(warp::body::json())
}

pub async fn update_probe(
    probe_id: String,
    probe_request: ProbeRequest,
    store: Arc<PartitionManager>,
    tx: Sender<ThreadMessage>
) -> Result<impl Reply, Rejection> {
    log::info!("Write request: {}, event: {}", probe_id, probe_request.get_event_id());
    if store.is_current_node_not_serving().await {
        log::info!("Returned 500, since the current node is dead");
        return Ok(with_status(json(&""), StatusCode::INTERNAL_SERVER_ERROR));
    }
    let probe = Probe::create_probe(probe_id, probe_request);
    //todo remove await here
    store.upsert_value(probe.clone(),tx).await;

    Ok(with_status(json(&probe),StatusCode::OK))
}

pub async fn get_probe(
    probe_id: String,
    store: Arc<PartitionManager>,
    tx: Sender<ThreadMessage>
) -> Result<impl Reply, Rejection> {
    log::info!("Read request: {}", probe_id);
    if store.is_current_node_not_serving().await {
        log::info!("Returned 500, since the current node is dead");
        return Ok(with_status(json(&""), StatusCode::INTERNAL_SERVER_ERROR));
    }
    let response = store.read_probe(probe_id, tx).await;
    match response {
        Some(value) => {
            Ok(with_status(json(&value), StatusCode::OK))
        }
        None => {
            Ok(with_status(json(&""), StatusCode::NOT_FOUND))
        }
    }
}
