use super::{
    make_cancellable_timeout_future,
    Event,
    EventSender,
    WorkItemContent,
    WorkItemFuture,
};
use crate::{
    Message,
    MessageContent,
};
#[cfg(test)]
use crate::{
    ScheduledEvent,
    Scheduler,
};
use futures::channel::oneshot;
use log::trace;
use std::{
    fmt::{
        Debug,
        Display,
    },
    hash::Hash,
    time::Duration,
};

pub struct Peer<S, T, Id>
where
    Id: Eq + Hash,
{
    cancel_retransmission: Option<oneshot::Sender<()>>,
    last_message: Option<Message<S, T, Id>>,
    pub last_seq: usize,
    pub match_index: usize,
    pub retransmission_future: Option<WorkItemFuture<S, T, Id>>,
    pub vote: Option<bool>,
}

impl<S, T, Id> Peer<S, T, Id>
where
    Id: Eq + Hash,
{
    pub fn awaiting_response(&self) -> bool {
        self.last_message.is_some()
    }

    pub fn cancel_retransmission(&mut self) -> Option<Message<S, T, Id>> {
        if let Some(cancel_retransmission) = self.cancel_retransmission.take() {
            let _ = cancel_retransmission.send(());
            trace!("Cancelling retransmission timer (Peer)");
        }
        self.retransmission_future.take();
        self.last_message.take()
    }

    pub fn send_request(
        &mut self,
        message: Message<S, T, Id>,
        peer_id: Id,
        event_sender: &EventSender<S, T, Id>,
        rpc_timeout: Duration,
        #[cfg(test)] scheduler: &Scheduler<Id>,
    ) where
        S: 'static + Clone + Debug + Send,
        T: 'static + Clone + Debug + Send,
        Id: 'static + Copy + Debug + Display + Send,
    {
        self.last_message = Some(message.clone());
        let (future, cancel_future) = make_cancellable_timeout_future(
            WorkItemContent::RpcTimeout(peer_id),
            rpc_timeout,
            #[cfg(test)]
            ScheduledEvent::Retransmit(peer_id),
            #[cfg(test)]
            &scheduler,
        );
        self.retransmission_future = Some(future);
        self.cancel_retransmission = Some(cancel_future);
        trace!("Sending request to {}: {:?}", peer_id, message);
        let _ = event_sender.unbounded_send(Event::SendMessage {
            message,
            receiver_id: peer_id,
        });
    }

    pub fn send_new_request(
        &mut self,
        content: MessageContent<S, T, Id>,
        peer_id: Id,
        term: usize,
        event_sender: &EventSender<S, T, Id>,
        rpc_timeout: Duration,
        #[cfg(test)] scheduler: &Scheduler<Id>,
    ) where
        S: 'static + Clone + Debug + Send,
        T: 'static + Clone + Debug + Send,
        Id: 'static + Copy + Debug + Display + Send,
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
            #[cfg(test)]
            scheduler,
        );
    }
}

// We can't #[derive(Default)] without constraining `T: Default`,
// so let's just implement it ourselves.
impl<S, T, Id> Default for Peer<S, T, Id>
where
    Id: Eq + Hash,
{
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
