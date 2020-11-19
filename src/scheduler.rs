use futures::channel::mpsc;
use std::time::Duration;

pub enum ScheduledEvent {
    ElectionTimeout(Duration),
    Heartbeat(Duration),
    Retransmit(Duration),
}

pub type ScheduledEventReceiver = mpsc::UnboundedReceiver<ScheduledEvent>;
pub type ScheduledEventSender = mpsc::UnboundedSender<ScheduledEvent>;

pub struct Scheduler {
    receiver: ScheduledEventReceiver,
}

impl Scheduler {
    pub fn new() -> (Self, ScheduledEventSender) {
        let (sender, receiver) = mpsc::unbounded();
        (
            Self {
                receiver,
            },
            sender,
        )
    }
}
