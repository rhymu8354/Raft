use std::{
    ops::Range,
    time::Duration,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServerConfiguration {
    pub election_timeout: Range<Duration>,
    pub heartbeat_interval: Duration,
    pub rpc_timeout: Duration,
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
