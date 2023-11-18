#[derive(Clone, Debug, PartialEq)]
pub enum NodeStatus {
    AliveServing,
    AliveNotServing,
    Dead,
}