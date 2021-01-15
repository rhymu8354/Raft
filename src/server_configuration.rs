use std::{
    ops::Range,
    time::Duration,
};

/// This holds variables which are local to one server in a Raft cluster.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServerConfiguration {
    /// This is the range within which the intervals for the server's
    /// election timer are chosen.
    pub election_timeout: Range<Duration>,

    /// This is the maximum interval the server should allow to elapse
    /// between sending two sequential [`AppendEntries`] messages
    /// when it's the cluster leader.
    ///
    /// [`AppendEntries`]: enum.MessageContent.html#variant.AppendEntries
    pub heartbeat_interval: Duration,

    /// This is the minimum interval the server should wait for a response
    /// back from another server to a normal (not an [`InstallSnapshot`])
    /// request sent to it, before concluding that the request or response
    /// was lost, and retransmitting the request.
    ///
    /// [`InstallSnapshot`]: enum.MessageContent.html#variant.InstallSnapshot
    pub rpc_timeout: Duration,

    /// This is the minimum interval the server should wait for a response
    /// back from another server to an [`InstallSnapshot`] request sent to it,
    /// before concluding that the request or response was lost, and
    /// retransmitting the request.
    ///
    /// [`InstallSnapshot`]: enum.MessageContent.html#variant.InstallSnapshot
    pub install_snapshot_timeout: Duration,
}

impl Default for ServerConfiguration {
    fn default() -> Self {
        Self {
            election_timeout: Duration::from_millis(150)
                ..Duration::from_millis(300),
            heartbeat_interval: Duration::from_millis(75),
            rpc_timeout: Duration::from_millis(15),
            install_snapshot_timeout: Duration::from_secs(10),
        }
    }
}
