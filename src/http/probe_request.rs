use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ProbeRequest {
    event_id: String,
    data: String,
}

impl ProbeRequest {
    pub fn get_event_id(&self) -> &String {
        &self.event_id
    }

    pub fn get_data(&self) -> &String {
        &self.data
    }
}
