#![warn(clippy::pedantic)]
// TODO: Remove this once ready to publish.
#![allow(clippy::missing_errors_doc)]
// This warning falsely triggers when `future::select_all` is used.
#![allow(clippy::mut_mut)]
// TODO: Uncomment this once ready to publish.
//#![warn(missing_docs)]

mod cluster_configuration;
mod log;
mod log_entry;
mod message;
#[cfg(test)]
mod mock_scheduler;
mod persistent_storage;
mod server;
mod server_configuration;
mod snapshot;
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
pub use snapshot::Snapshot;

#[cfg(test)]
mod tests {
    use log::LevelFilter;
    use simple_logger::SimpleLogger;
    pub fn assert_logger() {
        let _ = SimpleLogger::new().with_level(LevelFilter::Error).init();
    }
}
