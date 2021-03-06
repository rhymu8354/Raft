use crate::{
    ClusterConfiguration,
    LogEntry,
};
use std::hash::Hash;

/// This represents the set of requirements that a Raft [`Server`]
/// has on its host in terms of maintaining its copy of the cluster state
/// and the log of commands to change that state.
///
/// The cluster state consists of a [`snapshot`] at its base, upon which
/// is built a sequence of log entries.  The implementation must not change
/// any state outside of the calls to the methods of this trait.  The
/// Raft [`Server`] will occasionally call [`update_snapshot`] to give the
/// host an opportunity to apply log compaction (which should be prepared
/// in advance in some other thread/task context).
///
/// [`Server`]: struct.Server.html
/// [`snapshot`]: #tymethod.snapshot
/// [`update_snapshot`]: #tymethod.update_snapshot
/// [`ServerEvent::LogCommitted`]: enum.ServerEvent.html#variant.LogCommitted
pub trait Log<S, Id>: Send
where
    Id: Eq + Hash,
{
    /// This is the type defined by the host to hold any host-specific
    /// command added to the log in order to change the cluster state.
    type Command;

    /// Add a single command to the end of the log.
    ///
    /// The log implementation is responsible for updating the cluster
    /// configuration with the command in this entry, if applicable.
    fn append_one(
        &mut self,
        entry: LogEntry<Self::Command, Id>,
    );

    /// Add commands to the end of the log, provided by the given iterator.
    ///
    /// The log implementation is responsible for updating the cluster
    /// configuration with the commands in these entries, if applicable.
    fn append(
        &mut self,
        entries: Box<dyn Iterator<Item = LogEntry<Self::Command, Id>>>,
    );

    /// Return the cluster leadership term that was in effect when the
    /// last log entry incorporated into the snapshot was created.
    ///
    /// This should be zero in the initial condition of the cluster where no log
    /// entries have yet been incorporated into the snapshot.
    fn base_term(&self) -> usize;

    /// Return the index of the last log entry incorporated into the snapshot.
    ///
    /// This should be zero in the initial condition of the cluster where no log
    /// entries have yet been incorporated into the snapshot.
    fn base_index(&self) -> usize;

    /// Return the identifiers of the servers that are members of the cluster,
    /// along with an indication of whether or not the cluster is currently
    /// in a "joint" configuration.
    ///
    /// The returned configuration should be match the result of taking
    /// the configuration in the snapshot and updating it with each command
    /// in the log in sequence from first to last.
    fn cluster_configuration(&self) -> ClusterConfiguration<Id>;

    /// Return a copy of all entries in the log immediately following the
    /// log entry with the given index.
    fn entries(
        &self,
        prev_log_index: usize,
    ) -> Vec<LogEntry<Self::Command, Id>>;

    /// Return the cluster leadership term that was in effect when the log
    /// entry having the given index was created.
    ///
    /// [`None`] should be returned if the log entry is not known (either
    /// not yet appended, or already compacted into the snapshot and not
    /// the last one compacted.
    ///
    /// [`None`]: https://doc.rust-lang.org/std/option/enum.Option.html#variant.None
    fn entry_term(
        &self,
        index: usize,
    ) -> Option<usize>;

    /// Replace the current cluster state and all log entries with the given
    /// snapshot.
    fn install_snapshot(
        &mut self,
        base_index: usize,
        base_term: usize,
        snapshot: S,
    );

    /// Return the cluster leadership term that was in effect when the
    /// last entry in the log was created.  If the log is empty, the
    /// [`base_term`] should be returned.
    ///
    /// [`base_term`]: #tymethod.base_term
    fn last_term(&self) -> usize;

    /// Return the index of the last entry in the log, including any
    /// log entries compacted into the snapshot.
    fn last_index(&self) -> usize;

    /// Return a copy of the server state which forms the base of the log of
    /// commands.  This state minimally contains a cluster configuration and
    /// the index and term of the last log entry compacted into it (zero if no
    /// log entries have been compacted into it yet).  The host is free to
    /// include any host-specific state at its discretion, as long as such
    /// state is the correct result of compacting any log entries compacted
    /// into the snapshot.
    fn snapshot(&self) -> S;

    /// Remove any log entries not yet compacted into the snapshot whose
    /// index is greater than the given index.
    ///
    /// The log implementation is responsible for updating the cluster
    /// configuration to reflect what it was at the point just before the
    /// oldest log entry removed was added in the first place.
    fn truncate(
        &mut self,
        index: usize,
    );

    /// Optionally update the [`snapshot`], with the [`base_index`] and
    /// [`base_term`] accordingly, to the latest log compaction results
    /// the host has available.  It's up to the host whether or not to
    /// actually update them.  The host should not compute any new snapshot
    /// during this call, as it may unnecessarily stall other Raft
    /// operations.  Instead, the host should compute log compaction results
    /// in the background (another thread or task) and store them somewhere
    /// else, only updating the actual snapshot when this method is called.
    ///
    /// [`snapshot`]: #tymethod.snapshot
    /// [`base_index`]: #tymethod.base_index
    /// [`base_term`]: #tymethod.base_term
    fn update_snapshot(&mut self);
}
