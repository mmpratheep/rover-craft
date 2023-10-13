mod store;
mod test;
mod hash;

use warp::Filter;
use serde::{Serialize, Deserialize};

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
struct ProbeRequest {
    probe_id: String,
    event_id: String,
    data: String,
}

fn post_json() -> impl Filter<Extract=(ProbeRequest,), Error=warp::Rejection> + Clone {
    warp::body::content_length_limit(1024 * 16).and(warp::body::json())
}

async fn update_probe(
    probe_id: String,
    probe_request: ProbeRequest,
    store: store::Store
) -> Result<impl warp::Reply, warp::Rejection> {
    let probe = store::Probe::create_probe(probe_request);
    Ok(warp::reply::json(&probe))
}

async fn get_probe(
    probe_id: String,
    store: store::Store
) -> Result<impl warp::Reply, warp::Rejection> {
    let probe = store::Probe::dummy_probe(probe_id);
    Ok(warp::reply::json(&probe))
}

#[tokio::main]
async fn main() {
    let store = store::Store::new();
    let store_filter = warp::any().map(move || store.clone());

    let update_probe_route = warp::put()
        .and(warp::path("probe"))
        .and(warp::path::param())
        .and(warp::path::end())
        .and(post_json())
        .and(store_filter.clone())
        .and_then(update_probe);

    let get_probe_route = warp::get()
        .and(warp::path("probe"))
        .and(warp::path::param())
        .and(warp::path::end())
        .and(store_filter.clone())
        .and_then(get_probe);

    let routes = update_probe_route.or(get_probe_route);

    warp::serve(routes)
        .run(([127, 0, 0, 1], 9090))
        .await;
}
