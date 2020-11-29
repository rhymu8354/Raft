#[cfg(test)]
mod tests;

#[cfg(test)]
use crate::ScheduledEvent;
use crate::{
    Configuration,
    Log,
    LogEntryCustomCommand,
    Message,
    MessageContent,
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
use rand::{
    rngs::StdRng,
    Rng,
    SeedableRng,
};
use std::{
    collections::HashSet,
    fmt::Debug,
    pin::Pin,
    thread,
    time::Duration,
};

#[cfg(test)]
use crate::ScheduledEventReceiver;

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum ElectionState {
    Follower,
    Candidate,
    Leader,
}

pub struct MobilizeArgs {
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
    Mobilize(MobilizeArgs),
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

struct OnlineState {
    cluster: HashSet<usize>,
    election_state: ElectionState,
    id: usize,
    last_log_index: usize,
    last_log_term: usize,
    log: Box<dyn Log>,
    persistent_storage: Box<dyn PersistentStorage>,
}

impl OnlineState {
    fn become_candidate<T>(
        &mut self,
        server_event_sender: &ServerEventSender<T>,
    ) {
        self.change_election_state(
            ElectionState::Candidate,
            server_event_sender,
        );
        for id in &self.cluster {
            if *id == self.id {
                continue;
            }
            let message = Message::<T> {
                content: MessageContent::RequestVote::<T> {
                    candidate_id: self.id,
                    last_log_index: self.last_log_index,
                    last_log_term: self.last_log_term,
                },
                receiver_id: *id,
                seq: 0,
                term: 0,
            };
            let _ = server_event_sender
                .unbounded_send(ServerEvent::SendMessage(message));
        }
    }

    fn change_election_state<T>(
        &mut self,
        new_election_state: ElectionState,
        server_event_sender: &ServerEventSender<T>,
    ) {
        println!(
            "State: {:?} -> {:?}",
            self.election_state, new_election_state
        );
        self.election_state = new_election_state;
        let _ = server_event_sender.unbounded_send(
            ServerEvent::ElectionStateChange {
                election_state: self.election_state,
                term: 0,
                voted_for: Some(self.id),
            },
        );
    }

    fn new(mobilize_args: MobilizeArgs) -> Self {
        Self {
            cluster: mobilize_args.cluster,
            election_state: ElectionState::Follower,
            id: mobilize_args.id,
            last_log_index: mobilize_args.log.base_index(),
            last_log_term: mobilize_args.log.base_term(),
            log: mobilize_args.log,
            persistent_storage: mobilize_args.persistent_storage,
        }
    }
}

struct State<T> {
    configuration: Configuration,
    server_event_sender: ServerEventSender<T>,
    state_change_sender: StateChangeSender,
    online: Option<OnlineState>,
}

impl<T> State<T> {
    fn become_candidate(&mut self) {
        if let Some(state) = &mut self.online {
            state.become_candidate(&self.server_event_sender);
        }
    }

    fn election_state(&self) -> ElectionState {
        if let Some(state) = &self.online {
            state.election_state
        } else {
            ElectionState::Follower
        }
    }

    fn notify_state_change(&mut self) {
        self.state_change_sender
            .unbounded_send(())
            .expect("state change receiver unexpectedly dropped");
    }
}

enum FutureKind {
    Cancelled,
    ElectionTimeout,
    StateChange(StateChangeReceiver),
}

async fn await_cancellation(cancel: oneshot::Receiver<()>) {
    let _ = cancel.await;
}

async fn await_cancellable_timeout(
    future_kind: FutureKind,
    timeout: Pin<Box<dyn futures::Future<Output = ()> + Send>>,
    cancel: oneshot::Receiver<()>,
) -> FutureKind {
    futures::select! {
        _ = timeout.fuse() => future_kind,
        _ = await_cancellation(cancel).fuse() => FutureKind::Cancelled,
    }
}

fn make_cancellable_timeout_future(
    future_kind: FutureKind,
    duration: Duration,
    #[cfg(test)] scheduled_event: ScheduledEvent,
    scheduler: &Scheduler,
) -> (Pin<Box<dyn futures::Future<Output = FutureKind>>>, oneshot::Sender<()>) {
    let (sender, receiver) = oneshot::channel();
    let timeout = scheduler.schedule(
        #[cfg(test)]
        scheduled_event,
        duration,
    );
    let future =
        await_cancellable_timeout(future_kind, timeout, receiver).boxed();
    (future, sender)
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
            let state_change = match message {
                ServerCommand::Configure(configuration) => {
                    println!("Received Configure");
                    state.configuration = configuration;
                    true
                },
                ServerCommand::Demobilize(completed) => {
                    println!("Received Demobilize");
                    state.online = None;
                    let _ = completed.send(());
                    true
                },
                ServerCommand::Mobilize(mobilize_args) => {
                    println!("Received Mobilize");
                    state.online = Some(OnlineState::new(mobilize_args));
                    true
                },
                ServerCommand::Stop => unreachable!(),
            };
            if state_change {
                state.notify_state_change();
            }
        })
        .await;
    println!("Received Stop");
}

async fn process_state_change_receiver(
    mut state_change_receiver: StateChangeReceiver
) -> FutureKind {
    state_change_receiver.next().await;
    println!("Received StateChange");
    FutureKind::StateChange(state_change_receiver)
}

async fn upkeep_election_timeout_future<T>(
    state: &Mutex<State<T>>,
    cancel_election_timeout: &mut Option<oneshot::Sender<()>>,
    rng: &mut StdRng,
    futures: &mut Vec<Pin<Box<dyn futures::Future<Output = FutureKind>>>>,
    scheduler: &Scheduler,
) {
    let state = state.lock().await;
    let is_not_leader =
        !matches!(state.election_state(), ElectionState::Leader);
    if is_not_leader && cancel_election_timeout.is_none() {
        let timeout_duration = rng.gen_range(
            state.configuration.election_timeout.start,
            state.configuration.election_timeout.end,
        );
        println!(
            "Setting election timer to {:?} ({:?})",
            timeout_duration, state.configuration.election_timeout
        );
        let (future, cancel_future) = make_cancellable_timeout_future(
            FutureKind::ElectionTimeout,
            timeout_duration,
            #[cfg(test)]
            ScheduledEvent::ElectionTimeout,
            &scheduler,
        );
        futures.push(future);
        cancel_election_timeout.replace(cancel_future);
    }
}

async fn process_futures<T>(
    state_change_receiver: mpsc::UnboundedReceiver<()>,
    scheduler: Scheduler,
    state: &Mutex<State<T>>,
) {
    let mut state_change_receiver = Some(state_change_receiver);
    let mut need_state_change_receiver_future = true;
    let mut cancel_election_timeout = None;
    let mut rng = StdRng::from_entropy();
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

        // Make election timeout future if we don't have one and we are
        // not the leader of the cluster.
        upkeep_election_timeout_future(
            state,
            &mut cancel_election_timeout,
            &mut rng,
            &mut futures,
            &scheduler,
        )
        .await;

        // Wait for the next future to complete.
        let futures_in = futures;
        let (future_kind, _, futures_remaining) =
            future::select_all(futures_in).await;
        match future_kind {
            FutureKind::Cancelled => {
                println!("Completed canceled future");
            },
            FutureKind::ElectionTimeout => {
                // TODO: Handle election timeout here.
                println!("*** Election timeout! ***");
                cancel_election_timeout.take();
                let mut state = state.lock().await;
                let is_not_leader =
                    !matches!(state.election_state(), ElectionState::Leader);
                if is_not_leader {
                    state.become_candidate();
                }
            },
            FutureKind::StateChange(state_change_receiver_out) => {
                println!("Handling StateChange");
                if let Some(cancel) = cancel_election_timeout.take() {
                    let _ = cancel.send(());
                }
                state_change_receiver.replace(state_change_receiver_out);
                need_state_change_receiver_future = true;
            },
        }

        // Move remaining futures back to await again in the next loop.
        futures = futures_remaining;
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
        server_event_sender,
        state_change_sender,
        online: None,
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
        args: MobilizeArgs,
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
