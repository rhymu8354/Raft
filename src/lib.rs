mod cluster_configuration;
mod error;
mod log;
mod log_entry;
mod message;
#[cfg(test)]
mod mock_scheduler;
mod persistent_storage;
#[cfg(not(test))]
mod scheduler;
mod server;
mod server_configuration;
mod snapshot;

pub use self::log::Log;
pub use cluster_configuration::ClusterConfiguration;
pub use error::Error;
pub use log_entry::{
    Command as LogEntryCommand,
    CustomCommand as LogEntryCustomCommand,
    LogEntry,
};
pub use message::{
    AppendEntriesContent,
    Message,
    MessageContent,
};
#[cfg(test)]
use mock_scheduler::{
    ScheduledEvent,
    ScheduledEventReceiver,
    ScheduledEventWithCompleter,
    Scheduler,
};
pub use persistent_storage::PersistentStorage;
#[cfg(not(test))]
use scheduler::Scheduler;
pub use server::{
    ElectionState as ServerElectionState,
    Event as ServerEvent,
    MobilizeArgs as ServerMobilizeArgs,
    Server,
    SinkItem as ServerSinkItem,
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
