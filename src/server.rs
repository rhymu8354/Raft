mod inner;
mod mobilization;
mod peer;
#[cfg(test)]
mod tests;

use crate::{
    Configuration,
    Error,
    Log,
    Message,
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

pub struct MobilizeArgs<T> {
    pub cluster: HashSet<usize>,
    pub id: usize,
    pub log: Box<dyn Log<Command = T>>,
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
    LogCommitted(usize),
}

type EventReceiver<T> = mpsc::UnboundedReceiver<Event<T>>;
type EventSender<T> = mpsc::UnboundedSender<Event<T>>;

pub enum SinkItem<T> {
    ReceiveMessage {
        message: Message<T>,
        sender_id: usize,
    },
    #[cfg(test)]
    Synchronize(oneshot::Sender<()>),
}

impl<T> Debug for SinkItem<T>
where
    T: Debug,
{
    fn fmt(
        &self,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result where {
        match self {
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

pub enum Command<T> {
    Configure(Configuration),
    Demobilize(oneshot::Sender<()>),
    #[cfg(test)]
    FetchElectionTimeoutCounter(oneshot::Sender<usize>),
    Mobilize(MobilizeArgs<T>),
    ProcessSinkItem(SinkItem<T>),
}

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

#[cfg(test)]
type TimeoutArg = oneshot::Sender<()>;
#[cfg(not(test))]
type TimeoutArg = ();

pub struct WorkItem<T> {
    content: WorkItemContent<T>,
    #[cfg(test)]
    ack: Option<TimeoutArg>,
}

#[derive(Debug)]
pub enum WorkItemContent<T> {
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

pub type WorkItemFuture<T> = BoxFuture<'static, WorkItem<T>>;

async fn await_cancellation(cancel: oneshot::Receiver<()>) {
    let _ = cancel.await;
}

async fn await_cancellable_timeout<T>(
    work_item_content: WorkItemContent<T>,
    timeout: BoxFuture<'static, TimeoutArg>,
    cancel: oneshot::Receiver<()>,
) -> WorkItem<T>
where
    T: Debug,
{
    #[cfg(test)]
    futures::select! {
        ack = timeout.fuse() => WorkItem {
            content: work_item_content,
            ack: Some(ack)
        },
        _ = await_cancellation(cancel).fuse() => WorkItem {
            content: WorkItemContent::Cancelled(format!("{:?}", work_item_content)),
            ack: None
        },
    }
    #[cfg(not(test))]
    futures::select! {
        _ = timeout.fuse() => WorkItem {
            content: work_item_content,
        },
        _ = await_cancellation(cancel).fuse() => WorkItem {
            content: WorkItemContent::Cancelled,
        },
    }
}

fn make_cancellable_timeout_future<T>(
    work_item_content: WorkItemContent<T>,
    duration: Duration,
    #[cfg(test)] scheduled_event: ScheduledEvent,
    scheduler: &Scheduler,
) -> (WorkItemFuture<T>, oneshot::Sender<()>)
where
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

pub struct Server<T> {
    thread_join_handle: Option<thread::JoinHandle<()>>,
    command_sender: CommandSender<T>,
    event_receiver: EventReceiver<T>,
}

impl<T> Server<T> {
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
        args: MobilizeArgs<T>,
    ) {
        self.command_sender
            .unbounded_send(Command::Mobilize(args))
            .expect("server command receiver dropped prematurely");
    }

    #[cfg(test)]
    pub fn new() -> (Self, ScheduledEventReceiver)
    where
        T: Clone + Debug + Send + 'static,
    {
        let (scheduler, scheduled_event_receiver) = Scheduler::new();
        (Self::new_with_scheduler(scheduler), scheduled_event_receiver)
    }

    #[cfg(not(test))]
    pub fn new() -> Self
    where
        T: Clone + Debug + Send + 'static,
    {
        let scheduler = Scheduler::new();
        Self::new_with_scheduler(scheduler)
    }

    fn new_with_scheduler(scheduler: Scheduler) -> Self
    where
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
