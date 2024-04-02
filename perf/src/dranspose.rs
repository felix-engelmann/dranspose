use std::collections::HashMap;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct ConnectedWorker {
    pub(crate) name: String,
    pub(crate) service_uuid: Uuid,
    pub(crate) last_seen: f64,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct IngesterState {
    pub(crate) service_uuid: Uuid,
    pub mapping_uuid: Option<Uuid>,
    pub parameters_hash: Option<String>,
    pub processed_events: u64,
    pub event_rate: f32,
    pub(crate) name: String,
    pub(crate) url: String,
    //#[serde(flatten)]
    pub(crate) connected_workers: HashMap<Uuid, ConnectedWorker>,
    pub(crate) streams: Vec<String>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct ControllerUpdate {
    pub mapping_uuid: Uuid,
    pub target_parameters_hash: Option<String>,
    pub finished: bool,
}



#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct WorkAssignment{
    pub event_number: u64,
    pub assignments: HashMap<String, Vec<String>>
}
