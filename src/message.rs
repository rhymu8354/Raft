use crate::{
    LogEntry,
    Snapshot,
};
use serde::{
    Deserialize,
    Serialize,
};

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct AppendEntriesContent<T> {
    pub leader_commit: usize,
    pub prev_log_index: usize,
    pub prev_log_term: usize,
    pub log: Vec<LogEntry<T>>,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub enum Content<S, T> {
    RequestVote {
        last_log_index: usize,
        last_log_term: usize,
    },
    RequestVoteResponse {
        vote_granted: bool,
    },
    AppendEntries(AppendEntriesContent<T>),
    AppendEntriesResponse {
        success: bool,
        next_log_index: usize,
    },
    InstallSnapshot {
        last_included_index: usize,
        last_included_term: usize,
        snapshot: Snapshot<S>,
    },
    InstallSnapshotResponse,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct Message<S, T> {
    pub content: Content<S, T>,
    pub seq: usize,
    pub term: usize,
}
