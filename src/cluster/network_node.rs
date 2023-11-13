use tonic::transport::Channel;
use crate::grpc::service::probe_sync::probe_sync_client::ProbeSyncClient;
use crate::store::memory_store::MemoryStore;

#[derive(Debug)]
pub enum NetworkNode {
    RemoteStore(ProbeSyncClient<Channel>),
    LocalStore(MemoryStore),
}