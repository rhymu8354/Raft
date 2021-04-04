use crate::LogEntry;
use serde::{
    Deserialize,
    Serialize,
};
use std::hash::Hash;

/// This holds the information contained in an [`AppendEntries`]
/// message from one server to another.
///
/// [`AppendEntries`]: enum.MessageContent.html#variant.AppendEntries
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct AppendEntriesContent<T, Id>
where
    Id: Eq + Hash,
{
    /// This is the index of the last log entry which the leader of
    /// the cluster has committed at the time this message was sent.
    pub leader_commit: usize,

    /// This is the index of the log entry immediately before the first
    /// log entry in this message.  The index of the first log entry is 1,
    /// so this will be 0 if this message holds the first entry in the log.
    pub prev_log_index: usize,

    /// This is the cluster leadership term that was in effect when the log
    /// entry before the first log entry in this message was created.
    pub prev_log_term: usize,

    /// This holds the entries to be appended to the log.
    pub log: Vec<LogEntry<T, Id>>,
}

/// This holds the content of a message sent from one server to another.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub enum Content<S, T, Id>
where
    Id: Eq + Hash,
{
    /// The sender is running for cluster leadership and is requesting
    /// the receiver vote for the sender.
    RequestVote {
        /// This is the index of the last log entry the candidate for
        /// leadership has in its copy of the log.
        last_log_index: usize,

        /// This is the cluster leadership term that was in effect when the
        /// last log entry the candidate for leadership has in its copy
        /// of the log was created.
        last_log_term: usize,
    },

    /// The sender is responding to a previous [`RequestVote`] sent by the
    /// receiver.
    ///
    /// [`RequestVote`]: #variant.RequestVote
    RequestVoteResponse {
        /// This indicates whether or not the sender is voting for the
        /// receiver.
        vote_granted: bool,
    },

    /// Either append new entries to the receiver's copy of the log,
    /// or (if no log entries are provided) serve as a "heartbeat" to prevent
    /// the receiver from starting a new election.
    AppendEntries(AppendEntriesContent<T, Id>),

    /// The sender is responding to a previous [`AppendEntries`] sent by
    /// the receiver.
    ///
    /// [`AppendEntries`]: #variant.AppendEntries
    AppendEntriesResponse {
        /// Indicate whether or not the new log entries were accepted.
        success: bool,

        /// Indicate the index of the next log entry the receiver should
        /// send back.
        next_log_index: usize,
    },

    /// Replace the server state and compacted log with a given snapshot.
    InstallSnapshot {
        /// This is the index of the last log entry incorporated into the
        /// given snapshot.
        last_included_index: usize,

        /// This is the cluster leadership term that was in effect when the
        /// last log entry incorporated into the given snapshot was created.
        last_included_term: usize,

        /// This is the complete server state and compacted log that
        /// should be accepted by the receiver.
        snapshot: S,
    },

    /// The sender is responding to a previous [`InstallSnapshot`] sent by
    /// the receiver, indicating that it's ready to accept more log entries
    /// based on the received snapshot.
    ///
    /// [`InstallSnapshot`]: #variant.InstallSnapshot
    InstallSnapshotResponse {
        /// Indicate the index of the next log entry the receiver should
        /// send back.
        next_log_index: usize,
    },
}

/// This holds information to be sent from one server to another.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct Message<S, T, Id>
where
    Id: Eq + Hash,
{
    /// This holds information which varies according to the intention
    /// of the message.
    pub content: Content<S, T, Id>,

    /// This is a number used to correlate request messages with responses.
    /// It should be different for each separate request sent from the
    /// cluster leader to any specific follower.
    pub seq: usize,

    /// This is the current cluster leadership term in effect for the
    /// sender of the message, used to inform the receiver in order to
    /// synchronize the servers to be in the same term.
    pub term: usize,
}
