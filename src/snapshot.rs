use crate::ClusterConfiguration;
use serde::{
    Deserialize,
    Serialize,
};

#[derive(Debug, Deserialize, Clone, Eq, PartialEq, Serialize)]
pub struct Snapshot<S> {
    pub cluster_configuration: ClusterConfiguration,
    pub state: S,
}

impl<S> AsRef<Snapshot<S>> for Snapshot<S> {
    fn as_ref(&self) -> &Snapshot<S> {
        self
    }
}
