mod inner;
mod peer;
#[cfg(test)]
mod tests;

use crate::{
    ClusterConfiguration,
    Log,
    Message,
    PersistentStorage,
    ServerConfiguration,
};
#[cfg(test)]
use crate::{
    ScheduledEvent,
    ScheduledEventReceiver,
    Scheduler,
};
use futures::{
    channel::{
        mpsc,
        oneshot,
    },
    executor,
    future::BoxFuture,
    FutureExt as _,
    Sink,
    Stream,
    StreamExt as _,
};
#[cfg(not(test))]
use futures_timer::Delay;
use inner::Inner;
use std::{
    collections::HashSet,
    fmt::Debug,
    pin::Pin,
    task::Poll,
    thread,
    time::Duration,
};

/// This reflects the role of a server in a Raft cluster.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum ElectionState {
    /// The server has started an election for leadership of the cluster,
    /// and is currently soliciting votes from other servers.  If it receives
    /// a positive vote from a majority of the servers in the cluster
    /// (including its own vote for itself) then it will transition to the
    /// [`Leader`] state and begin sending new log entries
    /// (or empty "heartbeat" messages) to the other servers in the cluster.
    /// If, on the other hand, it receives a valid log entry or heartbeat
    /// message from another server claiming to be leader, it will transition
    /// to the [`Follower`] state.  If neither of these
    /// conditions occurs for long enough (a pre-computed randomized
    /// "election timer interval"), the server will increment the cluster
    /// term and ask the other servers in the cluster to vote for it again as
    /// the new leader in the new term.
    ///
    /// [`Candidate`]: #variant.Candidate
    /// [`Leader`]: #variant.Leader
    /// [`Follower`]: #variant.Follower
    Candidate,

    /// The server is neither the leader of the cluster nor currently
    /// a candidate for leadership of the cluster.  It passively awaits
    /// new log entries from the cluster leader.  If no messages are received
    /// from a leader for long enough (a pre-computed randomized "election
    /// timer interval"), the server will increment the cluster term,
    /// transition to the [`Candidate`] state, and ask the other
    /// servers in the cluster to vote for it as the new leader.
    ///
    /// [`Candidate`]: #variant.Candidate
    Follower,

    /// The server has been elected leader of the cluster.  It sends any log
    /// entries it has to other servers which don't have them, and sends
    /// empty "heartbeat" messages to servers which are up to date with the
    /// log. The server also accepts commands from the host to add new
    /// commands to the log or to add/remove servers in the cluster.
    Leader,
}

/// This represents some change to the cluster or request by the server
/// which the host should handle as appropriate.  Values of this type
/// are obtained by the host by using the [`Stream`] trait functions
/// of the [`Server`].
///
/// [`Stream`]: https://docs.rs/futures/latest/futures/stream/trait.Stream.html
/// [`Server`]: struct.Server.html
pub enum Event<S, T> {
    /// This informs the host that the servers with the given identifiers
    /// should be added to the cluster as non-voting members.
    ///
    /// Non-voting members are those whose identifiers are not in the current
    /// cluster configuration, yet are operated and included in log
    /// replication, for the purpose of adding them to the cluster
    /// configuration once they have caught up to the leader in replicating the
    /// log.
    ///
    /// Non-voting members automatically become voting members when they are
    /// added to the cluster configuration, as indicated by the
    /// [`Reconfiguration`] event.
    ///
    /// [`Reconfiguration`]: #variant.Reconfiguration
    AddNonVotingMembers(HashSet<usize>),

    /// This informs the host that any non-voting members should be removed
    /// from the cluster.
    ///
    /// Non-voting members are those whose identifiers are not in the current
    /// cluster configuration, yet are operated and included in log
    /// replication, for the purpose of adding them to the cluster
    /// configuration once they have caught up to the leader in replicating the
    /// log.
    DropNonVotingMembers,

    /// This indicates that the server has transitioned between being
    /// a leader, follower, or candidate for leader in the cluster.
    /// Changes in the current term number of the cluster, and whether
    /// or not the server voted for another server for leadership in the
    /// term are also indicated.
    ElectionStateChange {
        /// This reflects the new role of the server in the current term.
        election_state: ElectionState,

        /// This is the current term number of the cluster.  It is incremented
        /// whenever a new election for cluster leadership is started.
        term: usize,

        /// This indicates whether or not the server voted for a leader
        /// of the cluster in the current term, and if so, what is the
        /// identifier of the server that this server voted for.
        voted_for: Option<usize>,
    },

    /// This indicates that the server has determined all log entries up
    /// to and including the one at the given index in the log have been
    /// successfully replicated to a majority of the servers in the cluster,
    /// and so may be incorporated into the cluster state.
    LogCommitted(usize),

    /// This indicates that the membership of servers in the cluster has
    /// changed.
    ///
    /// Note that any previously added non-voting members should be
    /// automatically removed when this event is received, unless their
    /// identifiers are present in the given new cluster configuration.
    Reconfiguration(ClusterConfiguration),

    /// This is generated by the server in order to instruct the host
    /// to send the given `message` to the other server in the cluster
    /// whose identifier is `receiver_id`.
    SendMessage {
        /// This is the message that needs to be sent to another server.
        message: Message<S, T>,

        /// This is the identifier of the server to which the host
        /// should send the message.
        receiver_id: usize,
    },
}

type EventReceiver<S, T> = mpsc::UnboundedReceiver<Event<S, T>>;
type EventSender<S, T> = mpsc::UnboundedSender<Event<S, T>>;

/// These are the various requests the host can make of the Raft server.
pub enum Command<S, T> {
    /// This requests the server to add the given commands to the cluster log.
    ///
    /// The server must be the leader of the cluster to accept this command,
    /// otherwise it's ignored.
    AddCommands(Vec<T>),

    /// This is sent to provide the server with any message received for it
    /// from another server in the cluster.
    ReceiveMessage {
        /// This is the message received for the server.
        message: Message<S, T>,

        /// This is the identifier of the server which sent the message.
        sender_id: usize,
    },

    /// This requests the server to add or remove servers from the cluster.
    ///
    /// The server must be the leader of the cluster to accept this command,
    /// otherwise it's ignored.
    ///
    /// The configuration change is not immediate.  If servers are to be added,
    /// the leader will first replicate its log to the new servers.  Next,
    /// the server will append a command to the log to change the cluster
    /// to a joint configuration.  Once separate majories of the servers in
    /// both the previous and next configurations have committed this
    /// command, the server appends a command to the log to change the
    /// cluster to a single configuration consisting of only the given
    /// servers.  The host will receive [`ServerEvent::Reconfiguration`]
    /// events during this process whenever the server's configuration
    /// actually changes.
    ///
    /// [`ServerEvent::Reconfiguration`]:
    /// enum.ServerEvent.html#variant.Reconfiguration
    ReconfigureCluster(HashSet<usize>),

    /// This requests the server change its local configuration values
    /// which are server-specific.  The new configuration values take
    /// effect whenever the server needs them, which may not be immediate.
    /// For example, the election timeout interval range only changes
    /// when the next election timer is scheduled.
    ReconfigureServer(ServerConfiguration),

    /// This is sent during testing in order to wait for the server to
    /// process all commands up to and including this command.  The given
    /// oneshot is triggered once the command is processed.
    #[cfg(test)]
    Synchronize(oneshot::Sender<()>),
}

impl<S, T> Debug for Command<S, T>
where
    S: Debug,
    T: Debug,
{
    fn fmt(
        &self,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result where {
        match self {
            Command::AddCommands(commands) => {
                write!(f, "AddCommands({})", commands.len())?;
            },
            Command::ReceiveMessage {
                message,
                sender_id,
            } => {
                write!(
                    f,
                    "ReceiveMessage(from: {:?}, message: {:?})",
                    sender_id, message
                )?;
            },
            Command::ReconfigureCluster(ids) => {
                write!(f, "ReconfigureCluster({:?})", ids)?;
            },
            Command::ReconfigureServer(configuration) => {
                write!(f, "ReconfigureServer({:?})", configuration)?;
            },
            #[cfg(test)]
            Command::Synchronize(_) => {
                write!(f, "Synchronize")?;
            },
        }
        Ok(())
    }
}

type CommandReceiver<S, T> = mpsc::UnboundedReceiver<Command<S, T>>;
type CommandSender<S, T> = mpsc::UnboundedSender<Command<S, T>>;

#[cfg(test)]
type TimeoutArg = oneshot::Sender<()>;
#[cfg(not(test))]
type TimeoutArg = ();

pub struct WorkItem<S, T> {
    content: WorkItemContent<S, T>,
    #[cfg(test)]
    ack: Option<TimeoutArg>,
}

#[derive(Debug)]
pub enum WorkItemContent<S, T> {
    #[cfg(test)]
    Abandoned(String),
    #[cfg(not(test))]
    Abandoned,
    #[cfg(test)]
    Cancelled(String),
    #[cfg(not(test))]
    Cancelled,
    Command {
        command: Command<S, T>,
        command_receiver: CommandReceiver<S, T>,
    },
    ElectionTimeout,
    Heartbeat,
    MinElectionTimeout,
    RpcTimeout(usize),
    Stop,
}

pub type WorkItemFuture<S, T> = BoxFuture<'static, WorkItem<S, T>>;

async fn await_cancellation(cancel: oneshot::Receiver<()>) -> bool {
    cancel.await.is_ok()
}

async fn await_cancellable_timeout<S, T>(
    work_item_content: WorkItemContent<S, T>,
    timeout: BoxFuture<'static, TimeoutArg>,
    cancel: oneshot::Receiver<()>,
) -> WorkItem<S, T>
where
    S: Debug,
    T: Debug,
{
    #[cfg(test)]
    futures::select! {
        ack = timeout.fuse() => WorkItem {
            content: work_item_content,
            ack: Some(ack)
        },
        cancelled = await_cancellation(cancel).fuse() => WorkItem {
            content: if cancelled {
                WorkItemContent::Cancelled(format!("{:?}", work_item_content))
            } else {
                WorkItemContent::Abandoned(format!("{:?}", work_item_content))
            },
            ack: None
        },
    }
    #[cfg(not(test))]
    futures::select! {
        _ = timeout.fuse() => WorkItem {
            content: work_item_content,
        },
        cancelled = await_cancellation(cancel).fuse() => WorkItem {
            content: if cancelled {
                WorkItemContent::Cancelled
            } else {
                WorkItemContent::Abandoned
            },
        },
    }
}

fn make_cancellable_timeout_future<S, T>(
    work_item_content: WorkItemContent<S, T>,
    duration: Duration,
    #[cfg(test)] scheduled_event: ScheduledEvent,
    #[cfg(test)] scheduler: &Scheduler,
) -> (WorkItemFuture<S, T>, oneshot::Sender<()>)
where
    S: 'static + Debug + Send,
    T: 'static + Debug + Send,
{
    let (sender, receiver) = oneshot::channel();
    #[cfg(test)]
    let timeout = scheduler.schedule(scheduled_event, duration);
    #[cfg(not(test))]
    let timeout = Delay::new(duration).boxed();
    let future =
        await_cancellable_timeout(work_item_content, timeout, receiver).boxed();
    (future, sender)
}

/// This is used by a host in order to act as a server in a Raft cluster.
///
/// The server works asynchronously using a dedicated thread that executes
/// for the lifetime of the server.  [`ServerCommand`] values sent to the
/// server via the [`Sink`] trait go through a channel to be handled by
/// the thread.  The host should provide to the sink any Raft messages it
/// receives from other servers as [`ServerCommand::ReceiveMessage`] commands.
/// The server communicates back to the host by sending [`ServerEvent`] values
/// through another channel which is read by the host via the [`Stream`] trait.
/// This includes [`ServerEvent::SendMessage`] events which the host should
/// use in order to send messages to other servers in the cluster.
///
/// The host provides the identifier (id) of the server, various
/// server-specific configuration values when it creates the server, and a
/// "persistent storage" value responsible for maintaining a few Raft server
/// variables that need to persist between server lifetimes (saved whenever
/// changed, and loaded back if the server restarts).  In addition, it provides
/// a "log" which maintains the server state (including any compacted log
/// entries) and the current sequence of log entries being replicated across the
/// cluster.
///
/// Following the Raft algorithm, the server starts in the "follower" state
/// and obtains the cluster configuration from the snapshot in the log provided
/// by the host. What happens next depends on the other servers in the cluster.
/// If no servers are in the "leader" state, at least one server will eventually
/// start an election, which will inevitably lead to one server (not necessarily
/// the first to start the election) to become the leader.  The host is notified
/// of any changes to election state and configuration through
/// [`ServerEvent::ElectionStateChange`] and [`ServerEvent::Reconfiguration`]
/// respectively.
///
/// If the server being hosted becomes the leader of the cluster, the host
/// may use [`ServerCommand::AddCommands`] and
/// [`ServerCommand::ReconfigureCluster`] to add new commands to the cluster log
/// or add/remove servers from the cluster, respectively.  These commands have
/// no effect if the server isn't the leader.  It's expected that if a
/// non-leader server wishes to add new commands or add/remove servers from the
/// cluster, it requests the cluster leader to do these on its behalf, through
/// some means beyond the scope of Raft (for example, by sending a non-Raft
/// request message directly to the host of the leader server).
///
/// Whenever the cluster has successfully replicated one or more new commands
/// in the log, the host will be notified through [`ServerEvent::LogCommitted`].
/// The log is allowed to compact committed log entries into a new snapshot
/// at any time, but should not compact log entries not yet committed.  Until
/// the first [`ServerEvent::LogCommitted`] event is received, the host should
/// not assume any log entries are committed.
///
/// [`ServerCommand`]: enum.ServerCommand.html
/// [`ServerCommand::ReceiveMessage`]:
/// enum.ServerCommand.html#variant.ReceiveMessage
/// [`ServerCommand::AddCommands`]: enum.ServerCommand.html#variant.AddCommands
/// [`ServerCommand::ReconfigureCluster`]:
/// enum.ServerCommand.html#variant.ReconfigureCluster
/// [`ServerEvent`]: enum.ServerEvent.html
/// [`ServerEvent::SendMessage`]: enum.ServerEvent.html#variant.SendMessage
/// [`ServerEvent::ElectionStateChange`]:
/// enum.ServerEvent.html#variant.ElectionStateChange
/// [`ServerEvent::Reconfiguration`]:
/// enum.ServerEvent.html#variant.Reconfiguration
/// [`ServerEvent::LogCommitted`]: enum.ServerEvent.html#variant.LogCommitted
/// [`Stream`]: https://docs.rs/futures/latest/futures/stream/trait.Stream.html
/// [`Sink`]: https://docs.rs/futures/latest/futures/sink/trait.Sink.html
pub struct Server<S, T> {
    thread_join_handle: Option<thread::JoinHandle<()>>,
    command_sender: CommandSender<S, T>,
    event_receiver: EventReceiver<S, T>,
}

impl<S, T> Server<S, T> {
    #[cfg(test)]
    pub fn new_with_scheduler<C>(
        id: usize,
        configuration: C,
        log: Box<dyn Log<S, Command = T>>,
        persistent_storage: Box<dyn PersistentStorage>,
    ) -> (Self, ScheduledEventReceiver)
    where
        C: Into<ServerConfiguration>,
        S: Clone + Debug + Send + 'static,
        T: Clone + Debug + Send + 'static,
    {
        let (scheduler, scheduled_event_receiver) = Scheduler::new();
        (
            Self::new(id, configuration, log, persistent_storage, scheduler),
            scheduled_event_receiver,
        )
    }

    /// Create a new server with the given identifier (`id`), local
    /// `configuration` (server-specific), log provider, and persistent storage
    /// provider.
    ///
    /// The log and persistent storage provider implementations are
    /// host-specific dependencies injected when the server is created.
    ///
    /// Once created, the server operates using its own dedicated worker thread.
    /// The host communicates with the server through the [`Stream`] and
    /// [`Sink`] traits of the server.  The worker thread is joined when
    /// the server is dropped.
    ///
    /// Although the server's identifier, log provider, and persistent storage
    /// provider cannot change during the server's lifetime, the host may
    /// change the local configuration by sending a
    /// [`ServerCommand::ReconfigureServer`] command to its sink at any time.
    ///
    /// [`Stream`]: https://docs.rs/futures/latest/futures/stream/trait.Stream.html
    /// [`Sink`]: https://docs.rs/futures/latest/futures/sink/trait.Sink.html
    /// [`ServerCommand::ReconfigureServer`]:
    /// enum.ServerCommand.html#variant.ReconfigureServer
    pub fn new<C>(
        id: usize,
        configuration: C,
        log: Box<dyn Log<S, Command = T>>,
        persistent_storage: Box<dyn PersistentStorage>,
        #[cfg(test)] scheduler: Scheduler,
    ) -> Self
    where
        C: Into<ServerConfiguration>,
        S: Clone + Debug + Send + 'static,
        T: Clone + Debug + Send + 'static,
    {
        let configuration = configuration.into();
        let (command_sender, command_receiver) = mpsc::unbounded();
        let (event_sender, event_receiver) = mpsc::unbounded();
        let inner = Inner::new(
            id,
            configuration,
            log,
            persistent_storage,
            event_sender,
            #[cfg(test)]
            scheduler,
        );
        Self {
            command_sender,
            event_receiver,
            thread_join_handle: Some(thread::spawn(|| {
                executor::block_on(inner.serve(command_receiver))
            })),
        }
    }
}

impl<S, T> Drop for Server<S, T> {
    fn drop(&mut self) {
        self.command_sender.close_channel();
        self.thread_join_handle
            .take()
            .expect("somehow the server thread join handle got lost before we could take it")
            .join()
            .expect("the server thread panicked before we could join it");
    }
}

impl<S, T> Stream for Server<S, T>
where
    T: Debug,
{
    type Item = Event<S, T>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        self.event_receiver.poll_next_unpin(cx)
    }
}

impl<S, T> Sink<Command<S, T>> for Server<S, T> {
    type Error = ();

    fn poll_ready(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        self.command_sender.poll_ready(cx).map(|_| Ok(()))
    }

    fn start_send(
        mut self: Pin<&mut Self>,
        item: Command<S, T>,
    ) -> Result<(), Self::Error> {
        self.command_sender
            .start_send(item)
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
