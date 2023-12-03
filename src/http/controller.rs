use std::sync::Arc;
use tokio::sync::mpsc::Sender;
use warp::{Filter};
use crate::cluster::health_check_service::ThreadMessage;
use crate::cluster::partition_manager::PartitionManager;

use crate::http::handlers::{get_probe, post_json, update_probe};

pub async fn setup_controller(port : u16, store : Arc<PartitionManager>, tx: Sender<ThreadMessage>) {
    let store_filter = warp::any().map(move || store.clone());
    let tx_filter = warp::any().map(move || tx.clone());

    let update_probe_route = warp::put()
        .and(warp::path("probe"))
        .and(warp::path::param())
        .and(warp::path::end())
        .and(post_json())
        .and(store_filter.clone())
        .and(tx_filter.clone())
        .and_then(update_probe);

    let get_probe_route = warp::get()
        .and(warp::path("probe"))
        .and(warp::path::param())
        .and(warp::path::end())
        .and(store_filter.clone())
        .and(tx_filter.clone())
        .and_then(get_probe);

    let routes = update_probe_route
        .or(get_probe_route);

     return warp::serve(routes)
        .run(([0, 0, 0, 0], port))
         .await
}