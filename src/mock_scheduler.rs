use futures::channel::{
    mpsc,
    oneshot,
};
use std::time::Duration;

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

    pub async fn schedule(
        &self,
        event: ScheduledEvent,
        duration: Duration,
    ) {
        let (sender, receiver) = oneshot::channel();
        let _ = self.sender.unbounded_send(ScheduledEventWithCompleter {
            scheduled_event: event,
            duration,
            completer: sender,
        });
        let _ = receiver.await;
    }
}
