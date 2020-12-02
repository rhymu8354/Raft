use futures::{
    channel::{
        mpsc,
        oneshot,
    },
    future::BoxFuture,
    FutureExt,
};
use std::time::Duration;

#[derive(Debug)]
pub enum ScheduledEvent {
    ElectionTimeout,
    Heartbeat,
    Retransmit(usize),
}

pub struct ScheduledEventWithCompleter {
    pub scheduled_event: ScheduledEvent,
    pub duration: Duration,
    pub completer: oneshot::Sender<()>,
}

pub type ScheduledEventReceiver =
    mpsc::UnboundedReceiver<ScheduledEventWithCompleter>;
type ScheduledEventSender = mpsc::UnboundedSender<ScheduledEventWithCompleter>;

pub struct Scheduler {
    sender: ScheduledEventSender,
}

async fn await_receiver(receiver: oneshot::Receiver<()>) {
    if receiver.await.is_err() {
        futures::future::pending().await
    }
}

impl Scheduler {
    pub fn new() -> (Self, ScheduledEventReceiver) {
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
        event: ScheduledEvent,
        duration: Duration,
    ) -> BoxFuture<'static, ()> {
        println!("Scheduling {:?} in {:?}", event, duration);
        let (sender, receiver) = oneshot::channel();
        let _ = self.sender.unbounded_send(ScheduledEventWithCompleter {
            scheduled_event: event,
            duration,
            completer: sender,
        });
        await_receiver(receiver).boxed()
    }
}
