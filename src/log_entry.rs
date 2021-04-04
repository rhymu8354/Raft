use serde::{
    Deserialize,
    Serialize,
};
use std::{
    collections::HashSet,
    fmt::Debug,
    hash::Hash,
};

/// This represents a change to the cluster state, replicated to all
/// servers in the cluster.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub enum Command<T, Id>
where
    Id: Eq + Hash,
{
    /// This indicates the cluster should complete the transition from
    /// one set of servers to another in the cluster membership.
    FinishReconfiguration,

    /// This indicates the cluster should begin transitioning to a new
    /// set of servers in the cluster membership.
    StartReconfiguration(HashSet<Id>),

    /// This indicates the host-specific state of the cluster should
    /// be updated in some way.  The contents of the command are also
    /// host-specific.  Raft itself does nothing with it other than to
    /// replicate it to all servers in the cluster.
    Custom(T),
}

/// This holds a single command to change the cluster state, as maintained
/// in the cluster's log.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct LogEntry<T, Id>
where
    Id: Eq + Hash,
{
    /// This is the election term number that was in effect when the leader
    /// of the cluster appended this command to the log.
    pub term: usize,

    /// This is the command held in the entry, that may be applied to
    /// the cluster state once the entry is committed.
    pub command: Option<Command<T, Id>>,
}
