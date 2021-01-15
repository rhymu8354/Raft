use crate::ClusterConfiguration;
use serde::{
    Deserialize,
    Serialize,
};

// TODO: Do we need this structure at all, or can `S` be the snapshot,
// and we just let the host take care of maintaining the `cluster_configuration`
// on its own?
/// This holds the committed state of the cluster, incorporating any
/// compacted log entries, as well as the cluster configuration
/// in effect at the time the last compacted log entry was appended.
#[derive(Debug, Deserialize, Clone, Eq, PartialEq, Serialize)]
pub struct Snapshot<S> {
    /// This holds the identifiers of the other servers in the cluster.
    pub cluster_configuration: ClusterConfiguration,

    /// This holds the host-defined committed state of the cluster,
    /// incorporating any compacted log entries.  It must implement
    /// the [`Debug`], [`Clone`], and [`Eq`] traits, and must also
    /// support serialization ([`serde::Deserialize`] and [`serde::Serialize`]
    /// traits).
    ///
    /// [`Debug`]: https://doc.rust-lang.org/core/fmt/trait.Debug.html
    /// [`Clone`]: https://doc.rust-lang.org/core/clone/trait.Clone.html
    /// [`Eq`]: https://doc.rust-lang.org/core/cmp/trait.Eq.html
    /// [`serde::Deserialize`]: https://docs.rs/serde/latest/serde/trait.Deserialize.html
    /// [`serde::Serialize`]: https://docs.rs/serde/latest/serde/trait.Serialize.html
    pub state: S,
}

impl<S> AsRef<Snapshot<S>> for Snapshot<S> {
    fn as_ref(&self) -> &Snapshot<S> {
        self
    }
}
