use futures::{
    channel::{
        mpsc,
        oneshot,
    },
    future::BoxFuture,
    FutureExt,
};
use log::trace;
use std::{
    fmt::Debug,
    time::Duration,
};

#[derive(Debug)]
pub enum ScheduledEvent<Id> {
    ElectionTimeout,
    Heartbeat,
    MinElectionTimeout,
    Retransmit(Id),
}

pub struct ScheduledEventWithCompleter<Id> {
    pub scheduled_event: ScheduledEvent<Id>,
    pub duration: Duration,
    pub completer: oneshot::Sender<oneshot::Sender<()>>,
}

pub type ScheduledEventReceiver<Id> =
    mpsc::UnboundedReceiver<ScheduledEventWithCompleter<Id>>;
type ScheduledEventSender<Id> =
    mpsc::UnboundedSender<ScheduledEventWithCompleter<Id>>;

pub struct Scheduler<Id> {
    sender: ScheduledEventSender<Id>,
}

async fn await_receiver(
    receiver: oneshot::Receiver<oneshot::Sender<()>>
) -> oneshot::Sender<()> {
    if let Ok(sender) = receiver.await {
        sender
    } else {
        futures::future::pending().await
    }
}

impl<Id> Scheduler<Id> {
    pub fn new() -> (Self, ScheduledEventReceiver<Id>) {
        let (sender, receiver) = mpsc::unbounded();
        (
            Self {
                sender,
            },
            receiver,
        )
    }

    pub fn schedule(
        &self,
        event: ScheduledEvent<Id>,
        duration: Duration,
    ) -> BoxFuture<'static, oneshot::Sender<()>>
    where
        Id: Debug,
    {
        trace!("Scheduling {:?} in {:?}", event, duration);
        let (sender, receiver) = oneshot::channel();
        let _ = self.sender.unbounded_send(ScheduledEventWithCompleter {
            scheduled_event: event,
            duration,
            completer: sender,
        });
        await_receiver(receiver).boxed()
    }
}
