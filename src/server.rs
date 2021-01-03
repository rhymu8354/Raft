mod inner;
mod peer;
#[cfg(test)]
mod tests;

use crate::{
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

pub enum Command<S, T> {
    AddCommands(Vec<T>),
    ReceiveMessage {
        message: Message<S, T>,
        sender_id: usize,
    },
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
