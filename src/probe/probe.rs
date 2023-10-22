use std::hash::{Hash, Hasher};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use http::probe_request::ProbeRequest;

use crate::http;

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Probe {
    pub(crate) probe_id: String,
    pub(crate) event_id: String,
    pub(crate) event_date_time: u64,
    pub(crate) data: String,
}

impl Hash for Probe {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.probe_id.hash(state);
    }
}

impl Probe {

    pub fn get_probe_id(&self) -> &String {
        &self.probe_id
    }

    pub(crate) fn create_probe(probe_id: String, probe_request: ProbeRequest ) -> Probe {
        let event_date_time = SystemTime::now().duration_since(UNIX_EPOCH).expect("").as_millis() as u64;
        Probe {
            probe_id,
            event_id: probe_request.get_event_id().to_string(),
            event_date_time,
            data: probe_request.get_data().to_string(),
        }
    }
}