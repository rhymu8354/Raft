use super::{
    make_cancellable_timeout_future,
    Event,
    EventSender,
    WorkItemContent,
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
use log::{
    debug,
    trace,
};
use std::{
    fmt::Debug,
    time::Duration,
};

pub struct Peer<S, T> {
    cancel_retransmission: Option<oneshot::Sender<()>>,
    last_message: Option<Message<S, T>>,
    pub last_seq: usize,
    pub match_index: usize,
    pub retransmission_future: Option<WorkItemFuture<S, T>>,
    pub vote: Option<bool>,
}

impl<S, T> Peer<S, T> {
    pub fn awaiting_response(&self) -> bool {
        self.last_message.is_some()
    }

    pub fn cancel_retransmission(&mut self) -> Option<Message<S, T>> {
        if let Some(cancel_retransmission) = self.cancel_retransmission.take() {
            let _ = cancel_retransmission.send(());
            trace!("Cancelling retransmission timer (Peer)");
        }
        self.retransmission_future.take();
        self.last_message.take()
    }

    pub fn send_request(
        &mut self,
        message: Message<S, T>,
        peer_id: usize,
        event_sender: &EventSender<S, T>,
        rpc_timeout: Duration,
        scheduler: &Scheduler,
    ) where
        S: 'static + Clone + Debug + Send,
        T: 'static + Clone + Debug + Send,
    {
        self.last_message = Some(message.clone());
        let (future, cancel_future) = make_cancellable_timeout_future(
            WorkItemContent::RpcTimeout(peer_id),
            rpc_timeout,
            #[cfg(test)]
            ScheduledEvent::Retransmit(peer_id),
            &scheduler,
        );
        self.retransmission_future = Some(future);
        self.cancel_retransmission = Some(cancel_future);
        debug!("Sending request to {}: {:?}", peer_id, message);
        let _ = event_sender.unbounded_send(Event::SendMessage {
            message,
            receiver_id: peer_id,
        });
    }

    pub fn send_new_request(
        &mut self,
        content: MessageContent<S, T>,
        peer_id: usize,
        term: usize,
        event_sender: &EventSender<S, T>,
        rpc_timeout: Duration,
        scheduler: &Scheduler,
    ) where
        S: 'static + Clone + Debug + Send,
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
impl<S, T> Default for Peer<S, T> {
    fn default() -> Self {
        Peer {
            cancel_retransmission: None,
            last_message: None,
            last_seq: 0,
            match_index: 0,
            retransmission_future: None,
            vote: None,
        }
    }
}
