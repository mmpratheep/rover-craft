use parking_lot::RwLock;
use std::sync::Arc;
use crate::ProbeRequest;
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};
use serde::{Serialize, Deserialize};

#[derive(Debug, Deserialize, Serialize, Clone)]
pub(crate) struct Probe {
    probe_id: String,
    event_id: String,
    event_date_time: u128,
    data: String,
}

type Probes = HashMap<String, Probe>;

impl Probe {
    pub(crate) fn create_probe(probe_request: ProbeRequest ) -> Probe {
        let event_date_time = SystemTime::now().duration_since(UNIX_EPOCH).expect("").as_millis();
        Probe {
            probe_id: probe_request.probe_id,
            event_id: probe_request.event_id,
            event_date_time,
            data: probe_request.data,
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

#[derive(Clone)]
pub(crate) struct Store {
    probes: Arc<RwLock<Probes>>,
}

impl Store {
    pub(crate) fn new() -> Self {
        Store {
            probes: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}