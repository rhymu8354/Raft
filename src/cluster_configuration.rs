use serde::{
    Deserialize,
    Serialize,
};
use std::collections::HashSet;

#[derive(Debug, Deserialize, Clone, Eq, PartialEq, Serialize)]
pub enum ClusterConfiguration {
    Single(HashSet<usize>),
    Joint(HashSet<usize>, HashSet<usize>),
}
