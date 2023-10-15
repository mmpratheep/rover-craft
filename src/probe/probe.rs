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
    pub(crate) event_date_time: u128,
    pub(crate) data: String,
}

impl Hash for Probe {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.probe_id.hash(state);
    }
}

impl Probe {
    pub(crate) fn create_probe(probe_request: ProbeRequest ) -> Probe {
        let event_date_time = SystemTime::now().duration_since(UNIX_EPOCH).expect("").as_millis();
        Probe {
            probe_id: probe_request.get_probe_id().to_string(),
            event_id: probe_request.get_event_id().to_string(),
            event_date_time,
            data: probe_request.get_event_id().to_string(),
        }
    }

    pub(crate) fn dummy_probe(probe_id: String) -> Probe {
        let event_date_time = SystemTime::now().duration_since(UNIX_EPOCH).expect("").as_millis();
        Probe {
            probe_id,
            event_id: String::from("1"),
            event_date_time,
            data: String::from("Dummy data"),
        }
    }
}