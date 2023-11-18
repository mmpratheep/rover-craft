use std::sync::Arc;
use tonic::{Request, Response, Status};
use crate::cluster::partition_manager::PartitionManager;
use crate::grpc::service::probe_sync::{PartitionRequest, WriteProbeRequest, ProbeProto, ProbePartition, ReadProbeRequest, WriteProbeResponse};
use crate::grpc::service::probe_sync::probe_sync_server::ProbeSync;
use crate::probe::probe::Probe;

#[derive(Debug, Default)]
pub struct ProbeSyncService {
    pub(crate) partition_manager: Arc<PartitionManager>,
}


#[tonic::async_trait]
impl ProbeSync for ProbeSyncService {
    async fn read_probe(&self, request: Request<ReadProbeRequest>) -> Result<Response<ProbeProto>, Status> {
        let req = request.into_inner();

        match self.partition_manager
            .read_probe_from_partition(req.partition_id as usize, req.probe_id)
            .await {
            Some(probe) => {
                Ok(Response::new(probe.to_probe_data()))
            }
            None => {
                Err(Status::new(tonic::Code::NotFound, "No probe found"))
            }
        }
    }

    async fn write_probe(&self, request: Request<WriteProbeRequest>) -> Result<Response<WriteProbeResponse>, Status> {
        let req = request.into_inner();

        self.partition_manager
            .write_probe_to_partition(req.partition_id as usize,
                                      Probe::from_probe_proto(req.probe.unwrap())).await;
        //todo check whether the above statement will always pass without any error, if so,then handle that scenario
        Ok(Response::new(WriteProbeResponse { confirmation: true }))
    }

    async fn get_partition_data(&self, request: Request<PartitionRequest>) -> Result<Response<ProbePartition>, Status> {
        //todo need to check if it can properly send the data without any data loss
        let partition_id = request.into_inner().partition_id;
        let delta_partition = self.partition_manager
            .get_delta_data(partition_id as usize).unwrap();
        Ok(Response::new(ProbePartition { probe_array: delta_partition.serialise() }))
    }
}
