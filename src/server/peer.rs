use super::{
    make_cancellable_timeout_future,
    Event,
    EventSender,
    WorkItem,
    WorkItemFuture,
};
#[cfg(test)]
use crate::ScheduledEvent;
use crate::{
    Message,
    MessageContent,
    Scheduler,
};
use futures::channel::oneshot;
use std::{
    fmt::Debug,
    time::Duration,
};

pub struct Peer<T> {
    pub cancel_retransmission: Option<oneshot::Sender<()>>,
    pub last_message: Option<Message<T>>,
    pub last_seq: usize,
    pub retransmission_future: Option<WorkItemFuture<T>>,
    pub vote: Option<bool>,
}

impl<T> Peer<T> {
    pub fn send_request(
        &mut self,
        message: Message<T>,
        peer_id: usize,
        event_sender: &EventSender<T>,
        rpc_timeout: Duration,
        scheduler: &Scheduler,
    ) where
        T: 'static + Clone + Debug + Send,
    {
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

    pub fn send_new_request(
        &mut self,
        content: MessageContent<T>,
        peer_id: usize,
        term: usize,
        event_sender: &EventSender<T>,
        rpc_timeout: Duration,
        scheduler: &Scheduler,
    ) where
        T: 'static + Clone + Debug + Send,
    {
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
