#[cfg(test)]
mod tests;

#[cfg(test)]
use crate::ScheduledEvent;
use crate::{
    Configuration,
    Log,
    LogEntryCustomCommand,
    Message,
    PersistentStorage,
    Scheduler,
};
use async_mutex::Mutex;
use futures::{
    channel::{
        mpsc,
        oneshot,
    },
    executor,
    future,
    FutureExt as _,
    Stream,
    StreamExt as _,
};
use std::{
    cell::RefCell,
    collections::HashSet,
    fmt::Debug,
    pin::Pin,
    rc::Rc,
    thread,
    time::Duration,
};

#[cfg(test)]
use crate::ScheduledEventReceiver;

#[derive(Debug, Eq, PartialEq)]
pub enum ElectionState {
    Follower,
    Candidate,
    Leader,
}

pub struct Mobilization {
    pub id: usize,
    pub cluster: HashSet<usize>,
    pub log: Box<dyn Log>,
    pub persistent_storage: Box<dyn PersistentStorage>,
}

pub enum ServerEvent<T> {
    SendMessage(Message<T>),
    ElectionStateChange {
        election_state: ElectionState,
        term: usize,
        voted_for: Option<usize>,
    },
}

type ServerEventReceiver<T> = mpsc::UnboundedReceiver<ServerEvent<T>>;
type ServerEventSender<T> = mpsc::UnboundedSender<ServerEvent<T>>;

enum ServerCommand {
    Configure(Configuration),
    Demobilize(oneshot::Sender<()>),
    Mobilize(Mobilization),
    Stop,
}

impl Debug for ServerCommand {
    fn fmt(
        &self,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        match self {
            ServerCommand::Configure(_) => write!(f, "Configure"),
            ServerCommand::Demobilize(_) => write!(f, "Demobilize"),
            ServerCommand::Mobilize(_) => write!(f, "Mobilize"),
            ServerCommand::Stop => write!(f, "Stop"),
        }
    }
}

type ServerCommandReceiver = mpsc::UnboundedReceiver<ServerCommand>;
type ServerCommandSender = mpsc::UnboundedSender<ServerCommand>;

type StateChangeSender = mpsc::UnboundedSender<()>;
type StateChangeReceiver = mpsc::UnboundedReceiver<()>;

struct State<T> {
    configuration: Configuration,
    mobilization: Option<Mobilization>,
    server_event_sender: ServerEventSender<T>,
    state_change_sender: StateChangeSender,
}

enum FutureKind {
    ElectionTimeout,
    StateChange(StateChangeReceiver),
}

async fn process_server_commands<T>(
    server_command_receiver: ServerCommandReceiver,
    state: &Mutex<State<T>>,
) {
    server_command_receiver
        .take_while(|command| {
            future::ready(!matches!(command, ServerCommand::Stop))
        })
        .for_each(|message| async {
            let mut state = state.lock().await;
            match message {
                ServerCommand::Configure(new_configuration) => {
                    state.configuration = new_configuration;
                },
                ServerCommand::Demobilize(completed) => {
                    state.mobilization.take();
                    let _ = completed.send(());
                },
                ServerCommand::Mobilize(new_mobilize_args) => {
                    state.mobilization.replace(new_mobilize_args);
                },
                ServerCommand::Stop => unreachable!(),
            }
        })
        .await;
}

async fn process_state_change_receiver(
    mut state_change_receiver: StateChangeReceiver
) -> FutureKind {
    state_change_receiver.next().await;
    FutureKind::StateChange(state_change_receiver)
}

async fn process_futures<T>(
    state_change_receiver: mpsc::UnboundedReceiver<()>,
    scheduler: Scheduler,
    state: &Mutex<State<T>>,
) {
    let mut state_change_receiver = Some(state_change_receiver);
    let mut need_state_change_receiver_future = true;
    let mut futures: Vec<Pin<Box<dyn futures::Future<Output = FutureKind>>>> =
        Vec::new();
    loop {
        // Make state change receiver future if we don't have one.
        if need_state_change_receiver_future {
            futures.push(
                process_state_change_receiver(
                    state_change_receiver
                        .take()
                        .expect("state change receiver unexpectedly dropped"),
                )
                .boxed(),
            );
            need_state_change_receiver_future = false;
        }

        // Wait for the next future to complete.
        let futures_in = futures;
        let (future_kind, _, futures_remaining) =
            future::select_all(futures_in).await;
        match future_kind {
            FutureKind::ElectionTimeout => {
                // TODO: Handle election timeout here.
            },
            FutureKind::StateChange(state_change_receiver_out) => {
                state_change_receiver.replace(state_change_receiver_out);
                need_state_change_receiver_future = true;
            },
        }

        // Move remaining futures back to await again in the next loop.
        futures = futures_remaining;

        // TODO: This is a place-holder for the futures we will await
        // to process timeouts.
        // scheduler
        //     .schedule(
        //         #[cfg(test)]
        //         ScheduledEvent::ElectionTimeout,
        //         Duration::from_millis(150),
        //     )
        //     .await;
    }
}

async fn serve<T>(
    server_command_receiver: ServerCommandReceiver,
    server_event_sender: ServerEventSender<T>,
    scheduler: Scheduler,
) {
    let (state_change_sender, state_change_receiver) = mpsc::unbounded();
    let state = Mutex::new(State {
        configuration: Configuration::default(),
        mobilization: None,
        server_event_sender,
        state_change_sender,
    });
    let server_command_processor =
        process_server_commands(server_command_receiver, &state);
    let futures_processor =
        process_futures(state_change_receiver, scheduler, &state);
    futures::select! {
        _ = server_command_processor.fuse() => {},
        _ = futures_processor.fuse() => {},
    }
}

pub struct Server<T> {
    thread_join_handle: Option<thread::JoinHandle<()>>,
    server_command_sender: ServerCommandSender,
    server_event_receiver: ServerEventReceiver<T>,
}

impl<T> Server<T>
where
    T: LogEntryCustomCommand + Send + Sync + 'static,
{
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
        args: Mobilization,
    ) {
        self.server_command_sender
            .unbounded_send(ServerCommand::Mobilize(args))
            .expect("server command receiver dropped prematurely");
    }

    #[cfg(test)]
    pub fn new() -> (Self, ScheduledEventReceiver) {
        let (scheduler, scheduled_event_receiver) = Scheduler::new();
        (Self::new_with_scheduler(scheduler), scheduled_event_receiver)
    }

    #[cfg(not(test))]
    pub fn new() -> Self {
        let scheduler = Scheduler::new();
        Self::new_with_scheduler(scheduler)
    }

    fn new_with_scheduler(scheduler: Scheduler) -> Self {
        let (server_command_sender, server_command_receiver) =
            mpsc::unbounded();
        let (server_event_sender, server_event_receiver) = mpsc::unbounded();
        Self {
            server_command_sender,
            server_event_receiver,
            thread_join_handle: Some(thread::spawn(move || {
                executor::block_on(serve(
                    server_command_receiver,
                    server_event_sender,
                    scheduler,
                ))
            })),
        }
    }
}

#[cfg(not(test))]
impl<T> Default for Server<T>
where
    T: LogEntryCustomCommand + Send + Sync + 'static,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<T> Drop for Server<T> {
    fn drop(&mut self) {
        let _ = self.server_command_sender.unbounded_send(ServerCommand::Stop);
        self.thread_join_handle
            .take()
            .expect("somehow the server thread join handle got lost before we could take it")
            .join()
            .expect("the server thread panicked before we could join it");
    }
}

impl<T> Stream for Server<T> {
    type Item = ServerEvent<T>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.server_event_receiver.poll_next_unpin(cx)
    }
}
