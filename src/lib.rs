//! This crate implements the [Raft Consensus
//! Algorithm], designed to get multiple servers to
//! agree on a common state for the purposes of replication and
//! fault-tolerance.
//!
//! A group of servers working together using this algorithm is called a
//! "cluster".  Each cluster has a single "leader", or server selected through
//! an election process which is in charge of taking changes to the common
//! state and replicating them to the other servers in the cluster.  State
//! changes are represented by a "log" or sequence of commands to change the
//! state.  Raft handles the replication of this log, while the host is in
//! charge of defining the state structure and the semantics of the commands.
//!
//! To participate in a Raft cluster, the host creates a [`Server`]
//! and interacts with it by sending it commands ([`ServerCommand`]) and
//! receiving back events ([`ServerEvent`]) from it.  The [`Server`] is
//! asynchronous and implements the [`Stream`] and [`Sink`] traits from the
//! [`futures`] crate.
//!
//! [Raft Consensus Algorithm]: https://raft.github.io/
//! [`Server`]: struct.Server.html
//! [`ServerCommand`]: enum.ServerCommand.html
//! [`ServerEvent`]: enum.ServerEvent.html
//! [`Stream`]: https://docs.rs/futures/latest/futures/stream/trait.Stream.html
//! [`Sink`]: https://docs.rs/futures/latest/futures/sink/trait.Sink.html
//! [`futures`]: https://docs.rs/futures

#![warn(clippy::pedantic)]
// This warning falsely triggers when `future::select_all` is used.
#![allow(clippy::mut_mut)]
#![warn(missing_docs)]

mod cluster_configuration;
mod log;
mod log_entry;
mod message;
#[cfg(test)]
mod mock_scheduler;
mod persistent_storage;
mod server;
mod server_configuration;
mod utilities;

pub use self::log::Log;
pub use cluster_configuration::ClusterConfiguration;
pub use log_entry::{
    Command as LogEntryCommand,
    LogEntry,
};
pub use message::{
    AppendEntriesContent,
    Content as MessageContent,
    Message,
};
#[cfg(test)]
use mock_scheduler::{
    ScheduledEvent,
    ScheduledEventReceiver,
    ScheduledEventWithCompleter,
    Scheduler,
};
pub use persistent_storage::PersistentStorage;
pub use server::{
    Command as ServerCommand,
    ElectionState as ServerElectionState,
    Event as ServerEvent,
    Server,
};
pub use server_configuration::ServerConfiguration;

#[cfg(test)]
mod tests {
    use log::LevelFilter;
    use simple_logger::SimpleLogger;
    pub fn assert_logger() {
        let _ = SimpleLogger::new().with_level(LevelFilter::Error).init();
    }
}
