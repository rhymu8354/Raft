#[cfg(test)]
mod tests;

#[cfg(test)]
use crate::ScheduledEvent;
use crate::{
    AppendEntriesContent,
    Configuration,
    Error,
    Log,
    LogEntry,
    LogEntryCustomCommand,
    Message,
    MessageContent,
    PersistentStorage,
    Scheduler,
};
use futures::{
    channel::{
        mpsc,
        oneshot,
    },
    executor,
    future,
    future::BoxFuture,
    FutureExt as _,
    Sink,
    Stream,
    StreamExt as _,
};
use rand::{
    rngs::StdRng,
    Rng,
    SeedableRng,
};
use std::{
    cmp::Ordering,
    collections::{
        HashMap,
        HashSet,
    },
    fmt::Debug,
    pin::Pin,
    task::Poll,
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
    SendMessage {
        message: Message<T>,
        receiver_id: usize,
    },
    ElectionStateChange {
        election_state: ElectionState,
        term: usize,
        voted_for: Option<usize>,
    },
}

type ServerEventReceiver<T> = mpsc::UnboundedReceiver<ServerEvent<T>>;
type ServerEventSender<T> = mpsc::UnboundedSender<ServerEvent<T>>;

#[derive(Debug)]
pub enum ServerSinkItem<T> {
    ReceiveMessage {
        message: Message<T>,
        sender_id: usize,
        // TODO: Consider using a future instead of an optional channel.
        // The real code could provide a no-op future, whereas
        // test code could provide a future which the test waits on.
        received: Option<oneshot::Sender<()>>,
    },
}

enum ServerCommand<T> {
    Configure(Configuration),
    Demobilize(oneshot::Sender<()>),
    #[cfg(test)]
    FetchElectionTimeoutCounter(oneshot::Sender<usize>),
    Mobilize(MobilizeArgs),
    ProcessSinkItem(ServerSinkItem<T>),
}

#[derive(Debug)]
enum FutureKind<T> {
    #[cfg(test)]
    Cancelled(String),
    #[cfg(not(test))]
    Cancelled,
    ElectionTimeout,
    RpcTimeout(usize),
    ServerCommand {
        command: ServerCommand<T>,
        server_command_receiver: ServerCommandReceiver<T>,
    },
    Stop,
}

impl<T: Debug> Debug for ServerCommand<T> {
    fn fmt(
        &self,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        match self {
            ServerCommand::Configure(_) => write!(f, "Configure"),
            ServerCommand::Demobilize(_) => write!(f, "Demobilize"),
            #[cfg(test)]
            ServerCommand::FetchElectionTimeoutCounter(_) => {
                write!(f, "FetchElectionTimeoutCounter")
            },
            ServerCommand::Mobilize(_) => write!(f, "Mobilize"),
            ServerCommand::ProcessSinkItem(sink_item) => {
                write!(f, "ProcessSinkItem({:?})", sink_item)
            },
        }
    }
}

type ServerCommandReceiver<T> = mpsc::UnboundedReceiver<ServerCommand<T>>;
type ServerCommandSender<T> = mpsc::UnboundedSender<ServerCommand<T>>;

struct PeerState<T> {
    cancel_retransmission: Option<oneshot::Sender<()>>,
    last_message: Option<Message<T>>,
    last_seq: usize,
    retransmission_future: Option<BoxFuture<'static, FutureKind<T>>>,
    vote: Option<bool>,
}

impl<T: 'static + Clone + Debug + Send> PeerState<T> {
    fn send_request(
        &mut self,
        message: Message<T>,
        peer_id: usize,
        server_event_sender: &ServerEventSender<T>,
        rpc_timeout: Duration,
        scheduler: &Scheduler,
    ) {
        self.last_message = Some(message.clone());
        let (future, cancel_future) = make_cancellable_timeout_future(
            FutureKind::RpcTimeout(peer_id),
            rpc_timeout,
            #[cfg(test)]
            ScheduledEvent::Retransmit(peer_id),
            &scheduler,
        );
        self.retransmission_future = Some(future);
        self.cancel_retransmission = Some(cancel_future);
        println!("Sending message to {}: {:?}", peer_id, message);
        let _ = server_event_sender.unbounded_send(ServerEvent::SendMessage {
            message,
            receiver_id: peer_id,
        });
    }

    fn send_new_request(
        &mut self,
        content: MessageContent<T>,
        peer_id: usize,
        term: usize,
        server_event_sender: &ServerEventSender<T>,
        rpc_timeout: Duration,
        scheduler: &Scheduler,
    ) {
        self.vote = None;
        self.last_seq += 1;
        let message = Message {
            content,
            seq: self.last_seq,
            term,
        };
        self.send_request(
            message,
            peer_id,
            server_event_sender,
            rpc_timeout,
            scheduler,
        );
    }
}

// We can't #[derive(Default)] without constraining `T: Default`,
// so let's just implement it ourselves.
impl<T> Default for PeerState<T> {
    fn default() -> Self {
        PeerState {
            cancel_retransmission: None,
            last_message: None,
            last_seq: 0,
            retransmission_future: None,
            vote: None,
        }
    }
}

struct OnlineState<T> {
    cluster: HashSet<usize>,
    election_state: ElectionState,
    id: usize,
    last_log_index: usize,
    last_log_term: usize,
    log: Box<dyn Log>,
    peer_states: HashMap<usize, PeerState<T>>,
    persistent_storage: Box<dyn PersistentStorage>,
}

impl<T: 'static + Clone + Debug + Send> OnlineState<T> {
    fn become_candidate(
        &mut self,
        server_event_sender: &ServerEventSender<T>,
        rpc_timeout: Duration,
        scheduler: &Scheduler,
    ) {
        let term = self.persistent_storage.term() + 1;
        self.persistent_storage.update(term, Some(self.id));
        self.change_election_state(
            ElectionState::Candidate,
            server_event_sender,
        );
        self.send_new_message_broadcast(
            MessageContent::RequestVote {
                candidate_id: self.id,
                last_log_index: self.last_log_index,
                last_log_term: self.last_log_term,
            },
            server_event_sender,
            rpc_timeout,
            scheduler,
        );
    }

    fn become_leader(
        &mut self,
        server_event_sender: &ServerEventSender<T>,
        rpc_timeout: Duration,
        scheduler: &Scheduler,
    ) {
        self.change_election_state(ElectionState::Leader, server_event_sender);
        self.send_new_message_broadcast(
            MessageContent::AppendEntries(AppendEntriesContent {
                leader_commit: 0, // TODO: track last commit
                prev_log_index: self.last_log_index,
                prev_log_term: self.last_log_term,
                log: vec![LogEntry {
                    term: self.persistent_storage.term(),
                    command: None,
                }],
            }),
            server_event_sender,
            rpc_timeout,
            scheduler,
        );
    }

    fn cancel_retransmission(
        &mut self,
        peer_id: usize,
    ) {
        if let Some(cancel_retransmission) = self
            .peer_states
            .get_mut(&peer_id)
            .and_then(|peer_state| peer_state.cancel_retransmission.take())
        {
            let _ = cancel_retransmission.send(());
        }
    }

    fn change_election_state(
        &mut self,
        new_election_state: ElectionState,
        server_event_sender: &ServerEventSender<T>,
    ) {
        let term = self.persistent_storage.term();
        println!(
            "State: {:?} -> {:?} (term {})",
            self.election_state, new_election_state, term
        );
        self.election_state = new_election_state;
        let _ = server_event_sender.unbounded_send(
            ServerEvent::ElectionStateChange {
                election_state: self.election_state,
                term,
                voted_for: self.persistent_storage.voted_for(),
            },
        );
    }

    fn new(mobilize_args: MobilizeArgs) -> Self {
        let peer_states = mobilize_args
            .cluster
            .iter()
            .filter_map(|&id| {
                if id == mobilize_args.id {
                    None
                } else {
                    Some((id, PeerState::default()))
                }
            })
            .collect();
        Self {
            cluster: mobilize_args.cluster,
            election_state: ElectionState::Follower,
            id: mobilize_args.id,
            last_log_index: mobilize_args.log.base_index(),
            last_log_term: mobilize_args.log.base_term(),
            log: mobilize_args.log,
            peer_states,
            persistent_storage: mobilize_args.persistent_storage,
        }
    }

    fn process_request_vote_results(
        &mut self,
        sender_id: usize,
        vote_granted: bool,
        server_event_sender: &ServerEventSender<T>,
        rpc_timeout: Duration,
        scheduler: &Scheduler,
    ) -> bool {
        if self.election_state != ElectionState::Candidate {
            println!(
                "Received unexpected or extra vote {} from {}",
                vote_granted, sender_id
            );
            return false;
        }
        println!("Received vote {} from {}", vote_granted, sender_id);
        if let Some(peer_state) = self.peer_states.get_mut(&sender_id) {
            peer_state.vote = Some(vote_granted);
        } else {
            return false;
        }
        let votes = self
            .peer_states
            .values()
            .filter(|peer_state| matches!(peer_state.vote, Some(vote) if vote))
            .count()
            + 1; // we always vote for ourselves (it's implicit)
        if votes > self.cluster.len() - votes {
            self.become_leader(server_event_sender, rpc_timeout, scheduler);
            true
        } else {
            false
        }
    }

    fn decide_vote_grant(
        &mut self,
        candidate_id: usize,
        candidate_term: usize,
        candidate_last_log_term: usize,
        candidate_last_log_index: usize,
        server_event_sender: &ServerEventSender<T>,
    ) -> bool {
        match candidate_term.cmp(&self.persistent_storage.term()) {
            Ordering::Less => return false,
            Ordering::Equal => {
                if let Some(voted_for_id) = self.persistent_storage.voted_for()
                {
                    if voted_for_id != candidate_id {
                        return false;
                    }
                }
            },
            Ordering::Greater => (),
        };
        match candidate_last_log_term.cmp(&self.log.last_term()) {
            Ordering::Less => return false,
            Ordering::Equal => {
                if candidate_last_log_index < self.log.last_index() {
                    return false;
                }
            },
            Ordering::Greater => (),
        }
        self.persistent_storage.update(candidate_term, Some(candidate_id));
        if self.election_state != ElectionState::Follower {
            self.change_election_state(
                ElectionState::Follower,
                server_event_sender,
            );
        }
        true
    }

    fn process_request_vote(
        &mut self,
        sender_id: usize,
        seq: usize,
        term: usize,
        last_log_index: usize,
        last_log_term: usize,
        server_event_sender: &ServerEventSender<T>,
    ) -> bool {
        let vote_granted = self.decide_vote_grant(
            sender_id,
            term,
            last_log_term,
            last_log_index,
            server_event_sender,
        );
        let message = Message {
            content: MessageContent::RequestVoteResults {
                vote_granted,
            },
            seq,
            term: self.persistent_storage.term(),
        };
        println!("Sending message to {}: {:?}", sender_id, message);
        let _ = server_event_sender.unbounded_send(ServerEvent::SendMessage {
            message,
            receiver_id: sender_id,
        });
        vote_granted
    }

    fn process_append_entries(
        &mut self,
        _sender_id: usize,
        _seq: usize,
        term: usize,
        _append_entries: AppendEntriesContent<T>,
    ) -> bool {
        self.persistent_storage
            .update(term, self.persistent_storage.voted_for());
        true
    }

    fn process_receive_message(
        &mut self,
        message: Message<T>,
        sender_id: usize,
        server_event_sender: &ServerEventSender<T>,
        rpc_timeout: Duration,
        scheduler: &Scheduler,
    ) -> bool {
        match message.content {
            MessageContent::RequestVoteResults {
                vote_granted,
            } => {
                if self
                    .peer_states
                    .get(&sender_id)
                    .filter(|peer_state| peer_state.last_seq == message.seq)
                    .is_none()
                {
                    println!("Received old vote");
                    return false;
                }
                self.cancel_retransmission(sender_id);
                self.process_request_vote_results(
                    sender_id,
                    vote_granted,
                    server_event_sender,
                    rpc_timeout,
                    scheduler,
                )
            },
            MessageContent::RequestVote {
                last_log_index,
                last_log_term,
                ..
            } => self.process_request_vote(
                sender_id,
                message.seq,
                message.term,
                last_log_index,
                last_log_term,
                server_event_sender,
            ),
            MessageContent::AppendEntries(append_entries) => self
                .process_append_entries(
                    sender_id,
                    message.seq,
                    message.term,
                    append_entries,
                ),
            _ => todo!(),
        }
    }

    fn process_sink_item(
        &mut self,
        sink_item: ServerSinkItem<T>,
        server_event_sender: &ServerEventSender<T>,
        rpc_timeout: Duration,
        scheduler: &Scheduler,
    ) -> bool {
        match sink_item {
            ServerSinkItem::ReceiveMessage {
                message,
                sender_id,
                received,
            } => {
                let cancel_election_timer = self.process_receive_message(
                    message,
                    sender_id,
                    server_event_sender,
                    rpc_timeout,
                    scheduler,
                );
                if let Some(received) = received {
                    let _ = received.send(());
                }
                cancel_election_timer
            },
        }
    }

    fn send_new_message_broadcast(
        &mut self,
        content: MessageContent<T>,
        server_event_sender: &ServerEventSender<T>,
        rpc_timeout: Duration,
        scheduler: &Scheduler,
    ) {
        let term = self.persistent_storage.term();
        for (&peer_id, peer_state) in &mut self.peer_states {
            peer_state.send_new_request(
                content.clone(),
                peer_id,
                term,
                server_event_sender,
                rpc_timeout,
                scheduler,
            );
        }
    }
}

struct State<T> {
    configuration: Configuration,
    #[cfg(test)]
    election_timeout_counter: usize,
    online: Option<OnlineState<T>>,
    server_event_sender: ServerEventSender<T>,
}

impl<T: 'static + Clone + Debug + Send> State<T> {
    fn become_candidate(
        &mut self,
        scheduler: &Scheduler,
    ) {
        if let Some(state) = &mut self.online {
            state.become_candidate(
                &self.server_event_sender,
                self.configuration.rpc_timeout,
                scheduler,
            );
        }
    }

    fn election_state(&self) -> ElectionState {
        if let Some(state) = &self.online {
            state.election_state
        } else {
            ElectionState::Follower
        }
    }

    fn process_sink_item(
        &mut self,
        sink_item: ServerSinkItem<T>,
        scheduler: &Scheduler,
    ) -> bool {
        if let Some(state) = &mut self.online {
            state.process_sink_item(
                sink_item,
                &self.server_event_sender,
                self.configuration.rpc_timeout,
                scheduler,
            )
        } else {
            false
        }
    }

    fn retransmit(
        &mut self,
        peer_id: usize,
        scheduler: &Scheduler,
    ) {
        if let Some(state) = &mut self.online {
            if let Some(peer_state) = state.peer_states.get_mut(&peer_id) {
                peer_state.cancel_retransmission.take();
                let message = peer_state
                    .last_message
                    .take()
                    .expect("lost message that needs to be retransmitted");
                peer_state.send_request(
                    message,
                    peer_id,
                    &self.server_event_sender,
                    self.configuration.rpc_timeout,
                    scheduler,
                )
            }
        }
    }
}

async fn await_cancellation(cancel: oneshot::Receiver<()>) {
    let _ = cancel.await;
}

async fn await_cancellable_timeout<T: Debug + Send>(
    future_kind: FutureKind<T>,
    timeout: BoxFuture<'static, ()>,
    cancel: oneshot::Receiver<()>,
) -> FutureKind<T> {
    #[cfg(test)]
    let cancelled = FutureKind::Cancelled(format!("{:?}", future_kind));
    #[cfg(not(test))]
    let cancelled = FutureKind::Cancelled;
    futures::select! {
        _ = timeout.fuse() => future_kind,
        _ = await_cancellation(cancel).fuse() => cancelled,
    }
}

fn make_cancellable_timeout_future<T: 'static + Debug + Send>(
    future_kind: FutureKind<T>,
    duration: Duration,
    #[cfg(test)] scheduled_event: ScheduledEvent,
    scheduler: &Scheduler,
) -> (BoxFuture<'static, FutureKind<T>>, oneshot::Sender<()>) {
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

async fn process_server_command_receiver<T: 'static + Clone + Debug + Send>(
    server_command_receiver: ServerCommandReceiver<T>
) -> FutureKind<T> {
    let (command, server_command_receiver) =
        server_command_receiver.into_future().await;
    if let Some(command) = command {
        FutureKind::ServerCommand {
            command,
            server_command_receiver,
        }
    } else {
        println!("Server command channel closed");
        FutureKind::Stop
    }
}

fn upkeep_election_timeout_future<T: 'static + Clone + Debug + Send>(
    state: &State<T>,
    cancel_election_timeout: &mut Option<oneshot::Sender<()>>,
    rng: &mut StdRng,
    futures: &mut Vec<BoxFuture<'static, FutureKind<T>>>,
    scheduler: &Scheduler,
) {
    if state.online.is_none() {
        return;
    }
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

async fn serve<T>(
    server_command_receiver: ServerCommandReceiver<T>,
    server_event_sender: ServerEventSender<T>,
    scheduler: Scheduler,
) where
    T: 'static + Clone + Debug + Send,
{
    let mut state = State {
        configuration: Configuration::default(),
        #[cfg(test)]
        election_timeout_counter: 0,
        online: None,
        server_event_sender,
    };
    let mut server_command_receiver = Some(server_command_receiver);
    let mut cancel_election_timeout = None;
    let mut rng = StdRng::from_entropy();
    let mut futures = Vec::new();
    loop {
        // Make server command receiver future if we don't have one.
        if let Some(server_command_receiver) = server_command_receiver.take() {
            futures.push(
                process_server_command_receiver(server_command_receiver)
                    .boxed(),
            );
        }

        // Make election timeout future if we don't have one and we are
        // not the leader of the cluster.
        upkeep_election_timeout_future(
            &state,
            &mut cancel_election_timeout,
            &mut rng,
            &mut futures,
            &scheduler,
        );

        // Add any RPC timeout futures that have been set up.
        if let Some(state) = &mut state.online {
            for peer_state in state.peer_states.values_mut() {
                if let Some(future) = peer_state.retransmission_future.take() {
                    futures.push(future);
                }
            }
        }

        // Wait for the next future to complete.
        let futures_in = futures;
        let (future_kind, _, futures_remaining) =
            future::select_all(futures_in).await;
        match future_kind {
            #[cfg(test)]
            FutureKind::Cancelled(future_kind) => {
                println!("Completed canceled future: {}", future_kind);
            },
            #[cfg(not(test))]
            FutureKind::Cancelled => {
                println!("Completed canceled future");
            },
            FutureKind::ElectionTimeout => {
                println!("*** Election timeout! ***");
                cancel_election_timeout.take();
                #[cfg(test)]
                {
                    state.election_timeout_counter += 1;
                }
                let is_not_leader =
                    !matches!(state.election_state(), ElectionState::Leader);
                if is_not_leader {
                    state.become_candidate(&scheduler);
                }
            },
            FutureKind::RpcTimeout(peer_id) => {
                println!("*** RPC timeout ({})! ***", peer_id);
                state.retransmit(peer_id, &scheduler);
            },
            FutureKind::ServerCommand {
                command,
                server_command_receiver: server_command_receiver_out,
            } => {
                println!("ServerCommand: {:?}", command);
                let cancel_election_timer = match command {
                    ServerCommand::Configure(configuration) => {
                        state.configuration = configuration;
                        state.online.is_some()
                    },
                    ServerCommand::Demobilize(completed) => {
                        state.online = None;
                        let _ = completed.send(());
                        true
                    },
                    #[cfg(test)]
                    ServerCommand::FetchElectionTimeoutCounter(
                        response_sender,
                    ) => {
                        let _ = response_sender
                            .send(state.election_timeout_counter);
                        false
                    },
                    ServerCommand::Mobilize(mobilize_args) => {
                        state.online = Some(OnlineState::new(mobilize_args));
                        true
                    },
                    ServerCommand::ProcessSinkItem(sink_item) => {
                        state.process_sink_item(sink_item, &scheduler)
                    },
                };
                if cancel_election_timer {
                    if let Some(cancel) = cancel_election_timeout.take() {
                        let _ = cancel.send(());
                    }
                }
                server_command_receiver.replace(server_command_receiver_out);
            },
            FutureKind::Stop => break,
        }

        // Move remaining futures back to await again in the next loop.
        futures = futures_remaining;
    }
}

pub struct Server<T> {
    thread_join_handle: Option<thread::JoinHandle<()>>,
    server_command_sender: ServerCommandSender<T>,
    server_event_receiver: ServerEventReceiver<T>,
}

impl<T> Server<T>
where
    T: LogEntryCustomCommand + Clone + Debug + Send + Sync + 'static,
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

    #[cfg(test)]
    pub async fn election_timeout_count(&self) -> usize {
        let (sender, receiver) = oneshot::channel();
        self.server_command_sender
            .unbounded_send(ServerCommand::FetchElectionTimeoutCounter(sender))
            .expect("server command receiver dropped prematurely");
        receiver.await.expect(
            "server dropped fetch election timeout counter results sender",
        )
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
    T: LogEntryCustomCommand + Clone + Debug + Send + Sync + 'static,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<T> Drop for Server<T> {
    fn drop(&mut self) {
        let _ = self.server_command_sender.close_channel();
        self.thread_join_handle
            .take()
            .expect("somehow the server thread join handle got lost before we could take it")
            .join()
            .expect("the server thread panicked before we could join it");
    }
}

impl<T: Debug> Stream for Server<T> {
    type Item = ServerEvent<T>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        self.server_event_receiver.poll_next_unpin(cx)
    }
}

impl<T> Sink<ServerSinkItem<T>> for Server<T> {
    type Error = Error;

    fn poll_ready(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        self.server_command_sender.poll_ready(cx).map(|_| Ok(()))
    }

    fn start_send(
        mut self: Pin<&mut Self>,
        item: ServerSinkItem<T>,
    ) -> Result<(), Self::Error> {
        self.server_command_sender
            .start_send(ServerCommand::ProcessSinkItem(item))
            .expect("server command receiver unexpectedly dropped");
        Ok(())
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(
        self: Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
}
