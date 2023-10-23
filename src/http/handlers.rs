use warp::{Filter, Rejection, Reply};
use warp::http::StatusCode;
use warp::reply::{json, with_status};

use crate::http::probe_request::ProbeRequest;
use crate::probe::probe::Probe;
use crate::store::memory_store::MemoryStore;

pub fn post_json() -> impl Filter<Extract=(ProbeRequest, ), Error=Rejection> + Clone {
    warp::body::content_length_limit(1024 * 16).and(warp::body::json())
}

pub async fn update_probe(
    probe_id: String,
    probe_request: ProbeRequest,
    store: MemoryStore,
) -> Result<impl Reply, Rejection> {
    let response = store.save_probe(&Probe::create_probe(probe_id, probe_request));
    Ok(json(&response.unwrap()))
}

pub async fn get_probe(
    probe_id: String,
    store: MemoryStore,
) -> Result<impl Reply, Rejection> {
    let response = store.get_probe(&probe_id);
    match response {
        Some(value) => {
            Ok(with_status(json(&value), StatusCode::OK))
        }
        None => {
            Ok(with_status(json(&""), StatusCode::NOT_FOUND))
        }
    }
}
