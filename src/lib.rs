mod configuration;
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

pub use self::log::Log;
pub use configuration::Configuration;
pub use error::Error;
pub use log_entry::{
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

#[cfg(test)]
mod tests {
    use log::LevelFilter;
    use simple_logger::SimpleLogger;
    pub fn assert_logger() {
        let _ = SimpleLogger::new().with_level(LevelFilter::Error).init();
    }
}
