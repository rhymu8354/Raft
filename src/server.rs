mod inner;
mod mobilization;
mod peer;
#[cfg(test)]
mod tests;

use crate::{
    Error,
    Log,
    Message,
    PersistentStorage,
    Scheduler,
    ServerConfiguration,
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
    future::BoxFuture,
    FutureExt as _,
    Sink,
    Stream,
    StreamExt as _,
};
use inner::Inner;
use std::{
    collections::HashSet,
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

pub struct MobilizeArgs<S, T> {
    pub cluster: HashSet<usize>,
    pub id: usize,
    pub log: Box<dyn Log<S, Command = T>>,
    pub persistent_storage: Box<dyn PersistentStorage>,
}

pub enum Event<S, T> {
    ElectionStateChange {
        election_state: ElectionState,
        term: usize,
        voted_for: Option<usize>,
    },
    SendMessage {
        message: Message<S, T>,
        receiver_id: usize,
    },
    LogCommitted(usize),
}

type EventReceiver<S, T> = mpsc::UnboundedReceiver<Event<S, T>>;
type EventSender<S, T> = mpsc::UnboundedSender<Event<S, T>>;

pub enum SinkItem<S, T> {
    AddCommands(Vec<T>),
    ReceiveMessage {
        message: Message<S, T>,
        sender_id: usize,
    },
    #[cfg(test)]
    Synchronize(oneshot::Sender<()>),
}

impl<S, T> Debug for SinkItem<S, T>
where
    S: Debug,
    T: Debug,
{
    fn fmt(
        &self,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result where {
        match self {
            SinkItem::AddCommands(commands) => {
                write!(f, "AddCommands({})", commands.len())?;
            },
            SinkItem::ReceiveMessage {
                message,
                sender_id,
            } => {
                write!(
                    f,
                    "ReceiveMessage(from: {:?}, message: {:?})",
                    sender_id, message
                )?;
            },
            #[cfg(test)]
            SinkItem::Synchronize(_) => {
                write!(f, "Synchronize")?;
            },
        }
        Ok(())
    }
}

pub enum Command<S, T> {
    Configure(ServerConfiguration),
    Demobilize(oneshot::Sender<()>),
    Mobilize(MobilizeArgs<S, T>),
    ProcessSinkItem(SinkItem<S, T>),
}

impl<S, T> Debug for Command<S, T>
where
    S: Debug,
    T: Debug,
{
    fn fmt(
        &self,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        match self {
            Command::Configure(_) => write!(f, "Configure"),
            Command::Demobilize(_) => write!(f, "Demobilize"),
            Command::Mobilize(_) => write!(f, "Mobilize"),
            Command::ProcessSinkItem(sink_item) => {
                write!(f, "ProcessSinkItem({:?})", sink_item)
            },
        }
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
    scheduler: &Scheduler,
) -> (WorkItemFuture<S, T>, oneshot::Sender<()>)
where
    S: 'static + Debug + Send,
    T: 'static + Debug + Send,
{
    let (sender, receiver) = oneshot::channel();
    let timeout = scheduler.schedule(
        #[cfg(test)]
        scheduled_event,
        duration,
    );
    let future =
        await_cancellable_timeout(work_item_content, timeout, receiver).boxed();
    (future, sender)
}

pub struct Server<S, T> {
    thread_join_handle: Option<thread::JoinHandle<()>>,
    command_sender: CommandSender<S, T>,
    event_receiver: EventReceiver<S, T>,
}

impl<S, T> Server<S, T> {
    pub fn configure<C>(
        &self,
        configuration: C,
    ) where
        C: Into<ServerConfiguration>,
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

    pub fn mobilize(
        &self,
        args: MobilizeArgs<S, T>,
    ) {
        self.command_sender
            .unbounded_send(Command::Mobilize(args))
            .expect("server command receiver dropped prematurely");
    }

    #[cfg(test)]
    pub fn new() -> (Self, ScheduledEventReceiver)
    where
        S: Clone + Debug + Send + 'static,
        T: Clone + Debug + Send + 'static,
    {
        let (scheduler, scheduled_event_receiver) = Scheduler::new();
        (Self::new_with_scheduler(scheduler), scheduled_event_receiver)
    }

    #[cfg(not(test))]
    pub fn new() -> Self
    where
        S: Clone + Debug + Send + 'static,
        T: Clone + Debug + Send + 'static,
    {
        let scheduler = Scheduler::new();
        Self::new_with_scheduler(scheduler)
    }

    fn new_with_scheduler(scheduler: Scheduler) -> Self
    where
        S: Clone + Debug + Send + 'static,
        T: Clone + Debug + Send + 'static,
    {
        let (command_sender, command_receiver) = mpsc::unbounded();
        let (event_sender, event_receiver) = mpsc::unbounded();
        let inner = Inner::new(event_sender, scheduler);
        Self {
            command_sender,
            event_receiver,
            thread_join_handle: Some(thread::spawn(|| {
                executor::block_on(inner.serve(command_receiver))
            })),
        }
    }
}

#[cfg(not(test))]
impl<S, T> Default for Server<S, T>
where
    S: Clone + Debug + Send + 'static,
    T: Clone + Debug + Send + Sync + 'static,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<S, T> Drop for Server<S, T> {
    fn drop(&mut self) {
        let _ = self.command_sender.close_channel();
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

impl<S, T> Sink<SinkItem<S, T>> for Server<S, T> {
    type Error = Error;

    fn poll_ready(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        self.command_sender.poll_ready(cx).map(|_| Ok(()))
    }

    fn start_send(
        mut self: Pin<&mut Self>,
        item: SinkItem<S, T>,
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
