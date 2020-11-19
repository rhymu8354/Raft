#[cfg(test)]
mod tests;

#[cfg(not(test))]
use crate::Scheduler;
use crate::{
    Configuration,
    Log,
    PersistentStorage,
};
use futures::{
    channel::{
        mpsc,
        oneshot,
    },
    executor,
};
use std::{
    collections::HashSet,
    sync::Arc,
    thread,
};

#[cfg(test)]
use crate::{
    ScheduledEventReceiver,
    ScheduledEventSender,
};

pub enum ElectionState {
    Follower,
    Candidate,
    Leader,
}

pub struct MobilizeArgs {
    pub id: usize,
    pub cluster: HashSet<usize>,
    pub log: Arc<dyn Log>,
    pub persistent_storage: Arc<dyn PersistentStorage>,
}

enum ServerCommand {
    Configure(Configuration),
    Demobilize(oneshot::Sender<()>),
    Mobilize(MobilizeArgs),
    Stop,
}

type ServerCommandReceiver = mpsc::UnboundedReceiver<ServerCommand>;
type ServerCommandSender = mpsc::UnboundedSender<ServerCommand>;

async fn server(
    server_command_receiver: ServerCommandReceiver,
    #[cfg(test)] scheduled_event_sender: ScheduledEventSender,
) {
    let mut configuration = Configuration::default();
    #[cfg(not(test))]
    let (scheduler, scheduled_event_sender) = Scheduler::new();
}

pub struct Server {
    #[cfg(test)]
    scheduled_event_receiver: Option<ScheduledEventReceiver>,
    thread_join_handle: Option<thread::JoinHandle<()>>,
    server_command_sender: ServerCommandSender,
}

impl Server {
    pub fn configure<C>(
        &self,
        configuration: C,
    ) where
        C: Into<Configuration>,
    {
        self.server_command_sender
            .unbounded_send(ServerCommand::Configure(configuration.into()))
            .expect("server command receiver dropped prematurely");
    }

    pub async fn demobilize(&self) {
        let (sender, receiver) = oneshot::channel();
        self.server_command_sender
            .unbounded_send(ServerCommand::Demobilize(sender))
            .expect("server command receiver dropped prematurely");
        receiver.await.expect("server dropped demobilize results sender");
    }

    pub fn mobilize(
        &self,
        args: MobilizeArgs,
    ) {
        self.server_command_sender
            .unbounded_send(ServerCommand::Mobilize(args))
            .expect("server command receiver dropped prematurely");
    }

    pub fn new() -> Self {
        let (server_command_sender, server_command_receiver) =
            mpsc::unbounded();
        #[cfg(test)]
        let (scheduled_event_sender, scheduled_event_receiver) =
            mpsc::unbounded();
        Self {
            #[cfg(test)]
            scheduled_event_receiver: Some(scheduled_event_receiver),
            server_command_sender,
            thread_join_handle: Some(thread::spawn(move || {
                executor::block_on(server(
                    server_command_receiver,
                    #[cfg(test)]
                    scheduled_event_sender,
                ))
            })),
        }
    }

    #[cfg(test)]
    pub fn scheduled_event_receiver(&mut self) -> ScheduledEventReceiver {
        self.scheduled_event_receiver
            .take()
            .expect("scheduled event receiver is missing")
    }
}

impl Default for Server {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.server_command_sender.unbounded_send(ServerCommand::Stop);
        self.thread_join_handle
            .take()
            .expect("somehow the server thread join handle got lost before we could take it")
            .join()
            .expect("the server thread panicked before we could join it");
    }
}
