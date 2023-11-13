#[derive(Debug)]
pub enum NodeStatus {
    AliveServing,
    AliveNotServing,
    Dead,
}