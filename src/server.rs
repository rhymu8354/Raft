#[cfg(test)]
mod tests;

use crate::{
    AppendEntriesContent,
    Configuration,
    Error,
    Log,
    LogEntry,
    Message,
    MessageContent,
    PersistentStorage,
    Scheduler,
};
#[cfg(test)]
use crate::{
    ScheduledEvent,
    ScheduledEventReceiver,
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
    Rng as _,
    SeedableRng as _,
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

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum ElectionState {
    Candidate,
    Follower,
    Leader,
}

impl ElectionState {
    pub fn is_candidate(&self) -> bool {
        matches!(*self, ElectionState::Candidate)
    }

    pub fn is_follower(&self) -> bool {
        matches!(*self, ElectionState::Follower)
    }

    pub fn is_leader(&self) -> bool {
        matches!(*self, ElectionState::Leader)
    }
}

pub struct MobilizeArgs {
    pub cluster: HashSet<usize>,
    pub id: usize,
    pub log: Box<dyn Log>,
    pub persistent_storage: Box<dyn PersistentStorage>,
}

pub enum Event<T> {
    ElectionStateChange {
        election_state: ElectionState,
        term: usize,
        voted_for: Option<usize>,
    },
    SendMessage {
        message: Message<T>,
        receiver_id: usize,
    },
}

type EventReceiver<T> = mpsc::UnboundedReceiver<Event<T>>;
type EventSender<T> = mpsc::UnboundedSender<Event<T>>;

#[derive(Debug)]
pub enum SinkItem<T> {
    ReceiveMessage {
        message: Message<T>,
        // TODO: Consider using a future instead of an optional channel.
        // The real code could provide a no-op future, whereas
        // test code could provide a future which the test waits on.
        received: Option<oneshot::Sender<()>>,
        sender_id: usize,
    },
}

enum Command<T> {
    Configure(Configuration),
    Demobilize(oneshot::Sender<()>),
    #[cfg(test)]
    FetchElectionTimeoutCounter(oneshot::Sender<usize>),
    Mobilize(MobilizeArgs),
    ProcessSinkItem(SinkItem<T>),
}

#[derive(Debug)]
enum WorkItem<T> {
    #[cfg(test)]
    Cancelled(String),
    #[cfg(not(test))]
    Cancelled,
    ElectionTimeout,
    Command {
        command: Command<T>,
        command_receiver: CommandReceiver<T>,
    },
    RpcTimeout(usize),
    Stop,
}

type WorkItemFuture<T> = BoxFuture<'static, WorkItem<T>>;

impl<T> Debug for Command<T>
where
    T: Debug,
{
    fn fmt(
        &self,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        match self {
            Command::Configure(_) => write!(f, "Configure"),
            Command::Demobilize(_) => write!(f, "Demobilize"),
            #[cfg(test)]
            Command::FetchElectionTimeoutCounter(_) => {
                write!(f, "FetchElectionTimeoutCounter")
            },
            Command::Mobilize(_) => write!(f, "Mobilize"),
            Command::ProcessSinkItem(sink_item) => {
                write!(f, "ProcessSinkItem({:?})", sink_item)
            },
        }
    }
}

type CommandReceiver<T> = mpsc::UnboundedReceiver<Command<T>>;
type CommandSender<T> = mpsc::UnboundedSender<Command<T>>;

struct Peer<T> {
    cancel_retransmission: Option<oneshot::Sender<()>>,
    last_message: Option<Message<T>>,
    last_seq: usize,
    retransmission_future: Option<WorkItemFuture<T>>,
    vote: Option<bool>,
}

impl<T> Peer<T>
where
    T: 'static + Clone + Debug + Send,
{
    fn send_request(
        &mut self,
        message: Message<T>,
        peer_id: usize,
        event_sender: &EventSender<T>,
        rpc_timeout: Duration,
        scheduler: &Scheduler,
    ) {
        self.last_message = Some(message.clone());
        let (future, cancel_future) = make_cancellable_timeout_future(
            WorkItem::RpcTimeout(peer_id),
            rpc_timeout,
            #[cfg(test)]
            ScheduledEvent::Retransmit(peer_id),
            &scheduler,
        );
        self.retransmission_future = Some(future);
        self.cancel_retransmission = Some(cancel_future);
        println!("Sending message to {}: {:?}", peer_id, message);
        let _ = event_sender.unbounded_send(Event::SendMessage {
            message,
            receiver_id: peer_id,
        });
    }

    fn send_new_request(
        &mut self,
        content: MessageContent<T>,
        peer_id: usize,
        term: usize,
        event_sender: &EventSender<T>,
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
            event_sender,
            rpc_timeout,
            scheduler,
        );
    }
}

// We can't #[derive(Default)] without constraining `T: Default`,
// so let's just implement it ourselves.
impl<T> Default for Peer<T> {
    fn default() -> Self {
        Peer {
            cancel_retransmission: None,
            last_message: None,
            last_seq: 0,
            retransmission_future: None,
            vote: None,
        }
    }
}

struct Mobilization<T> {
    cancel_election_timeout: Option<oneshot::Sender<()>>,
    cluster: HashSet<usize>,
    election_state: ElectionState,
    #[cfg(test)]
    election_timeout_counter: usize,
    id: usize,
    last_log_index: usize,
    last_log_term: usize,
    log: Box<dyn Log>,
    peers: HashMap<usize, Peer<T>>,
    persistent_storage: Box<dyn PersistentStorage>,
    rng: StdRng,
}

impl<T> Mobilization<T>
where
    T: 'static + Clone + Debug + Send,
{
    fn become_candidate(
        &mut self,
        event_sender: &EventSender<T>,
        rpc_timeout: Duration,
        scheduler: &Scheduler,
    ) {
        let term = self.persistent_storage.term() + 1;
        self.persistent_storage.update(term, Some(self.id));
        self.change_election_state(ElectionState::Candidate, event_sender);
        self.send_new_message_broadcast(
            MessageContent::RequestVote {
                candidate_id: self.id,
                last_log_index: self.last_log_index,
                last_log_term: self.last_log_term,
            },
            event_sender,
            rpc_timeout,
            scheduler,
        );
    }

    fn become_leader(
        &mut self,
        event_sender: &EventSender<T>,
        rpc_timeout: Duration,
        scheduler: &Scheduler,
    ) {
        self.change_election_state(ElectionState::Leader, event_sender);
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
            event_sender,
            rpc_timeout,
            scheduler,
        );
    }

    fn cancel_retransmission(
        &mut self,
        peer_id: usize,
    ) {
        if let Some(cancel_retransmission) = self
            .peers
            .get_mut(&peer_id)
            .and_then(|peer| peer.cancel_retransmission.take())
        {
            let _ = cancel_retransmission.send(());
        }
    }

    fn change_election_state(
        &mut self,
        new_election_state: ElectionState,
        event_sender: &EventSender<T>,
    ) {
        let term = self.persistent_storage.term();
        println!(
            "State: {:?} -> {:?} (term {})",
            self.election_state, new_election_state, term
        );
        self.election_state = new_election_state;
        let _ = event_sender.unbounded_send(Event::ElectionStateChange {
            election_state: self.election_state,
            term,
            voted_for: self.persistent_storage.voted_for(),
        });
    }

    fn new(mobilize_args: MobilizeArgs) -> Self {
        let peers = mobilize_args
            .cluster
            .iter()
            .filter_map(|&id| {
                if id == mobilize_args.id {
                    None
                } else {
                    Some((id, Peer::default()))
                }
            })
            .collect();
        Self {
            cancel_election_timeout: None,
            cluster: mobilize_args.cluster,
            election_state: ElectionState::Follower,
            #[cfg(test)]
            election_timeout_counter: 0,
            id: mobilize_args.id,
            last_log_index: mobilize_args.log.base_index(),
            last_log_term: mobilize_args.log.base_term(),
            log: mobilize_args.log,
            peers,
            persistent_storage: mobilize_args.persistent_storage,
            rng: StdRng::from_entropy(),
        }
    }

    fn process_request_vote_results(
        &mut self,
        sender_id: usize,
        vote_granted: bool,
        event_sender: &EventSender<T>,
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
        if let Some(peer) = self.peers.get_mut(&sender_id) {
            peer.vote = Some(vote_granted);
        } else {
            return false;
        }
        let votes = self
            .peers
            .values()
            .filter(|peer| matches!(peer.vote, Some(vote) if vote))
            .count()
            + 1; // we always vote for ourselves (it's implicit)
        if votes > self.cluster.len() - votes {
            self.become_leader(event_sender, rpc_timeout, scheduler);
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
        event_sender: &EventSender<T>,
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
            self.change_election_state(ElectionState::Follower, event_sender);
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
        event_sender: &EventSender<T>,
    ) -> bool {
        let vote_granted = self.decide_vote_grant(
            sender_id,
            term,
            last_log_term,
            last_log_index,
            event_sender,
        );
        let message = Message {
            content: MessageContent::RequestVoteResults {
                vote_granted,
            },
            seq,
            term: self.persistent_storage.term(),
        };
        println!("Sending message to {}: {:?}", sender_id, message);
        let _ = event_sender.unbounded_send(Event::SendMessage {
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
        event_sender: &EventSender<T>,
        rpc_timeout: Duration,
        scheduler: &Scheduler,
    ) -> bool {
        match message.content {
            MessageContent::RequestVoteResults {
                vote_granted,
            } => {
                if self
                    .peers
                    .get(&sender_id)
                    .filter(|peer| peer.last_seq == message.seq)
                    .is_none()
                {
                    println!("Received old vote");
                    return false;
                }
                self.cancel_retransmission(sender_id);
                self.process_request_vote_results(
                    sender_id,
                    vote_granted,
                    event_sender,
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
                event_sender,
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
        sink_item: SinkItem<T>,
        event_sender: &EventSender<T>,
        rpc_timeout: Duration,
        scheduler: &Scheduler,
    ) -> bool {
        match sink_item {
            SinkItem::ReceiveMessage {
                message,
                sender_id,
                received,
            } => {
                let cancel_election_timer = self.process_receive_message(
                    message,
                    sender_id,
                    event_sender,
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
        event_sender: &EventSender<T>,
        rpc_timeout: Duration,
        scheduler: &Scheduler,
    ) {
        let term = self.persistent_storage.term();
        for (&peer_id, peer) in &mut self.peers {
            peer.send_new_request(
                content.clone(),
                peer_id,
                term,
                event_sender,
                rpc_timeout,
                scheduler,
            );
        }
    }
}

struct Inner<T> {
    configuration: Configuration,
    mobilization: Option<Mobilization<T>>,
    command_receiver: Option<CommandReceiver<T>>,
    event_sender: EventSender<T>,
}

impl<T> Inner<T>
where
    T: 'static + Clone + Debug + Send,
{
    fn process_sink_item(
        &mut self,
        sink_item: SinkItem<T>,
        scheduler: &Scheduler,
    ) -> bool {
        if let Some(mobilization) = &mut self.mobilization {
            mobilization.process_sink_item(
                sink_item,
                &self.event_sender,
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
        if let Some(mobilization) = &mut self.mobilization {
            if let Some(peer) = mobilization.peers.get_mut(&peer_id) {
                peer.cancel_retransmission.take();
                let message = peer
                    .last_message
                    .take()
                    .expect("lost message that needs to be retransmitted");
                peer.send_request(
                    message,
                    peer_id,
                    &self.event_sender,
                    self.configuration.rpc_timeout,
                    scheduler,
                )
            }
        }
    }

    async fn serve(
        mut self,
        scheduler: Scheduler,
    ) where
        T: 'static + Clone + Debug + Send,
    {
        let mut futures = Vec::new();
        loop {
            // Make server command receiver future if we don't have one.
            if let Some(command_receiver) = self.command_receiver.take() {
                futures
                    .push(process_command_receiver(command_receiver).boxed());
            }

            // Make election timeout future if we don't have one and we are
            // not the leader of the cluster.
            self.upkeep_election_timeout_future(&mut futures, &scheduler);

            // Add any RPC timeout futures that have been set up.
            if let Some(mobilization) = &mut self.mobilization {
                for peer in mobilization.peers.values_mut() {
                    if let Some(work_item) = peer.retransmission_future.take() {
                        futures.push(work_item);
                    }
                }
            }

            // Wait for the next future to complete.
            // let futures_in = futures;
            let (work_item, _, futures_remaining) =
                future::select_all(futures).await;
            futures = futures_remaining;
            match work_item {
                #[cfg(test)]
                WorkItem::Cancelled(work_item) => {
                    println!("Completed canceled future: {}", work_item);
                },
                #[cfg(not(test))]
                WorkItem::Cancelled => {
                    println!("Completed canceled future");
                },
                WorkItem::ElectionTimeout => {
                    println!("*** Election timeout! ***");
                    if let Some(mobilization) = &mut self.mobilization {
                        mobilization.cancel_election_timeout.take();
                        #[cfg(test)]
                        {
                            mobilization.election_timeout_counter += 1;
                        }
                        if !mobilization.election_state.is_leader() {
                            mobilization.become_candidate(
                                &self.event_sender,
                                self.configuration.rpc_timeout,
                                &scheduler,
                            );
                        }
                    }
                },
                WorkItem::RpcTimeout(peer_id) => {
                    println!("*** RPC timeout ({})! ***", peer_id);
                    self.retransmit(peer_id, &scheduler);
                },
                WorkItem::Command {
                    command,
                    command_receiver: command_receiver_out,
                } => {
                    println!("Command: {:?}", command);
                    let cancel_election_timer = match command {
                        Command::Configure(configuration) => {
                            self.configuration = configuration;
                            self.mobilization.is_some()
                        },
                        Command::Demobilize(completed) => {
                            self.mobilization = None;
                            let _ = completed.send(());
                            true
                        },
                        #[cfg(test)]
                        Command::FetchElectionTimeoutCounter(
                            response_sender,
                        ) => {
                            if let Some(mobilization) = &self.mobilization {
                                let _ = response_sender.send(
                                    mobilization.election_timeout_counter,
                                );
                            }
                            false
                        },
                        Command::Mobilize(mobilize_args) => {
                            self.mobilization =
                                Some(Mobilization::new(mobilize_args));
                            true
                        },
                        Command::ProcessSinkItem(sink_item) => {
                            self.process_sink_item(sink_item, &scheduler)
                        },
                    };
                    if cancel_election_timer {
                        if let Some(mobilization) = &mut self.mobilization {
                            if let Some(cancel) =
                                mobilization.cancel_election_timeout.take()
                            {
                                let _ = cancel.send(());
                            }
                        }
                    }
                    self.command_receiver.replace(command_receiver_out);
                },
                WorkItem::Stop => break,
            }
        }
    }

    fn upkeep_election_timeout_future(
        &mut self,
        futures: &mut Vec<WorkItemFuture<T>>,
        scheduler: &Scheduler,
    ) {
        if let Some(mobilization) = &mut self.mobilization {
            if !mobilization.election_state.is_leader()
                && mobilization.cancel_election_timeout.is_none()
            {
                let timeout_duration = mobilization.rng.gen_range(
                    self.configuration.election_timeout.start,
                    self.configuration.election_timeout.end,
                );
                println!(
                    "Setting election timer to {:?} ({:?})",
                    timeout_duration, self.configuration.election_timeout
                );
                let (future, cancel_future) = make_cancellable_timeout_future(
                    WorkItem::ElectionTimeout,
                    timeout_duration,
                    #[cfg(test)]
                    ScheduledEvent::ElectionTimeout,
                    &scheduler,
                );
                futures.push(future);
                mobilization.cancel_election_timeout.replace(cancel_future);
            }
        }
    }
}

async fn await_cancellation(cancel: oneshot::Receiver<()>) {
    let _ = cancel.await;
}

async fn await_cancellable_timeout<T: Debug + Send>(
    work_item: WorkItem<T>,
    timeout: BoxFuture<'static, ()>,
    cancel: oneshot::Receiver<()>,
) -> WorkItem<T> {
    #[cfg(test)]
    let cancelled = WorkItem::Cancelled(format!("{:?}", work_item));
    #[cfg(not(test))]
    let cancelled = WorkItem::Cancelled;
    futures::select! {
        _ = timeout.fuse() => work_item,
        _ = await_cancellation(cancel).fuse() => cancelled,
    }
}

fn make_cancellable_timeout_future<T: 'static + Debug + Send>(
    work_item: WorkItem<T>,
    duration: Duration,
    #[cfg(test)] scheduled_event: ScheduledEvent,
    scheduler: &Scheduler,
) -> (WorkItemFuture<T>, oneshot::Sender<()>) {
    let (sender, receiver) = oneshot::channel();
    let timeout = scheduler.schedule(
        #[cfg(test)]
        scheduled_event,
        duration,
    );
    let future =
        await_cancellable_timeout(work_item, timeout, receiver).boxed();
    (future, sender)
}

async fn process_command_receiver<T: 'static + Clone + Debug + Send>(
    command_receiver: CommandReceiver<T>
) -> WorkItem<T> {
    let (command, command_receiver) = command_receiver.into_future().await;
    if let Some(command) = command {
        WorkItem::Command {
            command,
            command_receiver,
        }
    } else {
        println!("Server command channel closed");
        WorkItem::Stop
    }
}

pub struct Server<T> {
    thread_join_handle: Option<thread::JoinHandle<()>>,
    command_sender: CommandSender<T>,
    event_receiver: EventReceiver<T>,
}

impl<T> Server<T>
where
    T: Clone + Debug + Send + 'static,
{
    pub fn configure<C>(
        &self,
        configuration: C,
    ) where
        C: Into<Configuration>,
    {
        self.command_sender
            .unbounded_send(Command::Configure(configuration.into()))
            .expect("server command receiver dropped prematurely");
    }

    pub async fn demobilize(&self) {
        let (sender, receiver) = oneshot::channel();
        self.command_sender
            .unbounded_send(Command::Demobilize(sender))
            .expect("server command receiver dropped prematurely");
        receiver.await.expect("server dropped demobilize results sender");
    }

    #[cfg(test)]
    pub async fn election_timeout_count(&self) -> usize {
        let (sender, receiver) = oneshot::channel();
        self.command_sender
            .unbounded_send(Command::FetchElectionTimeoutCounter(sender))
            .expect("server command receiver dropped prematurely");
        receiver.await.expect(
            "server dropped fetch election timeout counter results sender",
        )
    }

    pub fn mobilize(
        &self,
        args: MobilizeArgs,
    ) {
        self.command_sender
            .unbounded_send(Command::Mobilize(args))
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
        let (command_sender, command_receiver) = mpsc::unbounded();
        let (event_sender, event_receiver) = mpsc::unbounded();
        let inner = Inner {
            configuration: Configuration::default(),
            mobilization: None,
            command_receiver: Some(command_receiver),
            event_sender,
        };
        Self {
            command_sender,
            event_receiver,
            thread_join_handle: Some(thread::spawn(|| {
                executor::block_on(inner.serve(scheduler))
            })),
        }
    }
}

#[cfg(not(test))]
impl<T> Default for Server<T>
where
    T: Clone + Debug + Send + Sync + 'static,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<T> Drop for Server<T> {
    fn drop(&mut self) {
        let _ = self.command_sender.close_channel();
        self.thread_join_handle
            .take()
            .expect("somehow the server thread join handle got lost before we could take it")
            .join()
            .expect("the server thread panicked before we could join it");
    }
}

impl<T> Stream for Server<T>
where
    T: Debug,
{
    type Item = Event<T>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        self.event_receiver.poll_next_unpin(cx)
    }
}

impl<T> Sink<SinkItem<T>> for Server<T> {
    type Error = Error;

    fn poll_ready(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        self.command_sender.poll_ready(cx).map(|_| Ok(()))
    }

    fn start_send(
        mut self: Pin<&mut Self>,
        item: SinkItem<T>,
    ) -> Result<(), Self::Error> {
        self.command_sender
            .start_send(Command::ProcessSinkItem(item))
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
