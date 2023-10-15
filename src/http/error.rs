use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Error {
    message: String
}

impl Error {
    pub fn new(message : String) -> Error {
        Error {
            message
        }
    }
}