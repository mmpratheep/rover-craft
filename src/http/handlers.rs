use warp::Filter;

use crate::http::probe_request::ProbeRequest;
use crate::probe::probe::Probe;
use crate::store::store::Store;

pub fn post_json() -> impl Filter<Extract=(ProbeRequest,), Error=warp::Rejection> + Clone {
    warp::body::content_length_limit(1024 * 16).and(warp::body::json())
}

pub async fn update_probe(
    probe_id: String,
    probe_request: ProbeRequest,
    store: Store
) -> Result<impl warp::Reply, warp::Rejection> {
    let probe = Probe::create_probe(probe_request);
    Ok(warp::reply::json(&probe))
}

pub async fn get_probe(
    probe_id: String,
    store: Store
) -> Result<impl warp::Reply, warp::Rejection> {
    let probe = Probe::dummy_probe(probe_id);
    Ok(warp::reply::json(&probe))
}