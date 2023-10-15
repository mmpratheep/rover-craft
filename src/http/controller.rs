
use warp::Filter;
use crate::http::handlers::{get_probe, post_json, update_probe};
use crate::store;

#[tokio::main]
pub async fn setup_controller(port : u16) {
    let store = store::store::Store::new();
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
        .run(([127, 0, 0, 1], port))
        .await;
}