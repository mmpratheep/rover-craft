use std::fmt;
use std::hash::{Hash, Hasher};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use http::probe_request::ProbeRequest;
use crate::grpc::service::probe_sync::{ProbeProto};

use crate::http;

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Probe {
    pub(crate) probe_id: String,
    pub(crate) event_id: String,
    pub(crate) event_received_time: u64,
    pub(crate) data: String,
}

impl Hash for Probe {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.probe_id.hash(state);
    }
}

impl fmt::Display for Probe {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{},{},{}", &self.probe_id, self.event_id, self.event_received_time)
    }
}

impl Probe {

    pub fn get_probe_id(&self) -> &String {
        &self.probe_id
    }

    pub(crate) fn create_probe(probe_id: String, probe_request: ProbeRequest ) -> Probe {
        let event_date_time = SystemTime::now().duration_since(UNIX_EPOCH)
            .expect("Unable to form event_date_time Epoch.")
            .as_millis() as u64;
        Probe {
            probe_id,
            event_id: probe_request.get_event_id().to_string(),
            event_received_time: event_date_time,
            data: probe_request.get_data().to_string(),
        }
    }

    pub(crate) fn from_probe_proto(probe_request: ProbeProto) -> Probe {
        Probe {
            probe_id: probe_request.probe_id,
            event_id: probe_request.event_id,
            event_received_time: probe_request.event_date_time,
            data: probe_request.data,
        }
    }

    pub(crate) fn to_probe_data(self) -> ProbeProto{
        ProbeProto {
            probe_id: self.probe_id,
            data: self.data,
            event_date_time: self.event_received_time,
            event_id: self.event_id
        }
    }

}