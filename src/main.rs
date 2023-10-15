mod probe;
mod store;

mod cluster;
mod http;

#[tokio::main]
async fn main() {
    http::controller::setup_controller(9000);
}
