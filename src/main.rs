use crate::store::memory_store::MemoryStore;

mod probe;
mod store;

mod cluster;
mod http;

#[tokio::main]
async fn main() {
    let store = MemoryStore::new();
    http::controller::setup_controller(9000, store)
        .await;
}
