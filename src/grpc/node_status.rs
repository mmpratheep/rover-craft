#[derive(Clone, Debug)]
pub enum NodeStatus {
    AliveServing,
    AliveNotServing,
    Dead,
}