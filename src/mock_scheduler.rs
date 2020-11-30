use futures::{
    channel::{
        mpsc,
        oneshot,
    },
    FutureExt,
};
use std::{
    pin::Pin,
    time::Duration,
};

#[derive(Debug)]
pub enum ScheduledEvent {
    ElectionTimeout,
    Heartbeat,
    Retransmit,
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
    let _ = receiver.await;
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
    ) -> Pin<Box<dyn futures::Future<Output = ()> + Send>> {
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