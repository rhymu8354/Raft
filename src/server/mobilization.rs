use super::{
    make_cancellable_timeout_future,
    peer::Peer,
    ElectionState,
    Event,
    EventSender,
    MobilizeArgs,
    SinkItem,
    WorkItem,
    WorkItemFuture,
};
#[cfg(test)]
use crate::ScheduledEvent;
use crate::{
    log_entry::LogEntry,
    AppendEntriesContent,
    Log,
    Message,
    MessageContent,
    PersistentStorage,
    Scheduler,
};
use futures::channel::oneshot;
use rand::{
    rngs::StdRng,
    Rng as _,
    SeedableRng as _,
};
use std::{
    cmp::Ordering,
    collections::{
        HashMap,
        HashSet,
    },
    fmt::Debug,
    ops::Range,
    time::Duration,
};

pub struct Mobilization<T> {
    cancel_election_timeout: Option<oneshot::Sender<()>>,
    cluster: HashSet<usize>,
    election_state: ElectionState,
    #[cfg(test)]
    pub election_timeout_counter: usize,
    id: usize,
    last_log_index: usize,
    last_log_term: usize,
    log: Box<dyn Log>,
    peers: HashMap<usize, Peer<T>>,
    persistent_storage: Box<dyn PersistentStorage>,
    rng: StdRng,
}

impl<T> Mobilization<T> {
    fn become_candidate(
        &mut self,
        event_sender: &EventSender<T>,
        rpc_timeout: Duration,
        scheduler: &Scheduler,
    ) where
        T: Clone + Debug + Send + 'static,
    {
        let term = self.persistent_storage.term() + 1;
        self.persistent_storage.update(term, Some(self.id));
        self.change_election_state(ElectionState::Candidate, event_sender);
        self.send_new_message_broadcast(
            MessageContent::RequestVote {
                candidate_id: self.id,
                last_log_index: self.last_log_index,
                last_log_term: self.last_log_term,
            },
            event_sender,
            rpc_timeout,
            scheduler,
        );
    }

    fn become_leader(
        &mut self,
        event_sender: &EventSender<T>,
        rpc_timeout: Duration,
        scheduler: &Scheduler,
    ) where
        T: Clone + Debug + Send + 'static,
    {
        self.change_election_state(ElectionState::Leader, event_sender);
        self.send_new_message_broadcast(
            MessageContent::AppendEntries(AppendEntriesContent {
                leader_commit: 0, // TODO: track last commit
                prev_log_index: self.last_log_index,
                prev_log_term: self.last_log_term,
                log: vec![LogEntry {
                    term: self.persistent_storage.term(),
                    command: None,
                }],
            }),
            event_sender,
            rpc_timeout,
            scheduler,
        );
    }

    pub fn cancel_election_timer(&mut self) {
        if let Some(cancel) = self.cancel_election_timeout.take() {
            let _ = cancel.send(());
        }
    }

    fn cancel_retransmission(
        &mut self,
        peer_id: usize,
    ) {
        if let Some(cancel_retransmission) = self
            .peers
            .get_mut(&peer_id)
            .and_then(|peer| peer.cancel_retransmission.take())
        {
            let _ = cancel_retransmission.send(());
        }
    }

    fn change_election_state(
        &mut self,
        new_election_state: ElectionState,
        event_sender: &EventSender<T>,
    ) {
        let term = self.persistent_storage.term();
        println!(
            "State: {:?} -> {:?} (term {})",
            self.election_state, new_election_state, term
        );
        self.election_state = new_election_state;
        let _ = event_sender.unbounded_send(Event::ElectionStateChange {
            election_state: self.election_state,
            term,
            voted_for: self.persistent_storage.voted_for(),
        });
    }

    fn decide_vote_grant(
        &mut self,
        candidate_id: usize,
        candidate_term: usize,
        candidate_last_log_term: usize,
        candidate_last_log_index: usize,
        event_sender: &EventSender<T>,
    ) -> bool {
        match candidate_term.cmp(&self.persistent_storage.term()) {
            Ordering::Less => return false,
            Ordering::Equal => {
                if let Some(voted_for_id) = self.persistent_storage.voted_for()
                {
                    if voted_for_id != candidate_id {
                        return false;
                    }
                }
            },
            Ordering::Greater => (),
        };
        match candidate_last_log_term.cmp(&self.log.last_term()) {
            Ordering::Less => return false,
            Ordering::Equal => {
                if candidate_last_log_index < self.log.last_index() {
                    return false;
                }
            },
            Ordering::Greater => (),
        }
        self.persistent_storage.update(candidate_term, Some(candidate_id));
        if self.election_state != ElectionState::Follower {
            self.change_election_state(ElectionState::Follower, event_sender);
        }
        true
    }

    pub fn election_timeout(
        &mut self,
        event_sender: &EventSender<T>,
        rpc_timeout: Duration,
        scheduler: &Scheduler,
    ) where
        T: Clone + Debug + Send + 'static,
    {
        self.cancel_election_timeout.take();
        #[cfg(test)]
        {
            self.election_timeout_counter += 1;
        }
        if self.election_state != ElectionState::Leader {
            self.become_candidate(event_sender, rpc_timeout, &scheduler);
        }
    }

    pub fn new(mobilize_args: MobilizeArgs) -> Self {
        let peers = mobilize_args
            .cluster
            .iter()
            .filter_map(|&id| {
                if id == mobilize_args.id {
                    None
                } else {
                    Some((id, Peer::default()))
                }
            })
            .collect();
        Self {
            cancel_election_timeout: None,
            cluster: mobilize_args.cluster,
            election_state: ElectionState::Follower,
            #[cfg(test)]
            election_timeout_counter: 0,
            id: mobilize_args.id,
            last_log_index: mobilize_args.log.base_index(),
            last_log_term: mobilize_args.log.base_term(),
            log: mobilize_args.log,
            peers,
            persistent_storage: mobilize_args.persistent_storage,
            rng: StdRng::from_entropy(),
        }
    }

    fn process_receive_message(
        &mut self,
        message: Message<T>,
        sender_id: usize,
        event_sender: &EventSender<T>,
        rpc_timeout: Duration,
        scheduler: &Scheduler,
    ) -> bool
    where
        T: Clone + Debug + Send + 'static,
    {
        match message.content {
            MessageContent::RequestVoteResults {
                vote_granted,
            } => {
                if self
                    .peers
                    .get(&sender_id)
                    .filter(|peer| peer.last_seq == message.seq)
                    .is_none()
                {
                    println!("Received old vote");
                    return false;
                }
                self.cancel_retransmission(sender_id);
                self.process_request_vote_results(
                    sender_id,
                    vote_granted,
                    event_sender,
                    rpc_timeout,
                    scheduler,
                )
            },
            MessageContent::RequestVote {
                last_log_index,
                last_log_term,
                ..
            } => self.process_request_vote(
                sender_id,
                message.seq,
                message.term,
                last_log_index,
                last_log_term,
                event_sender,
            ),
            MessageContent::AppendEntries(append_entries) => self
                .process_append_entries(
                    sender_id,
                    message.seq,
                    message.term,
                    append_entries,
                ),
            _ => todo!(),
        }
    }

    fn process_append_entries(
        &mut self,
        _sender_id: usize,
        _seq: usize,
        term: usize,
        _append_entries: AppendEntriesContent<T>,
    ) -> bool {
        self.persistent_storage
            .update(term, self.persistent_storage.voted_for());
        true
    }

    fn process_request_vote(
        &mut self,
        sender_id: usize,
        seq: usize,
        term: usize,
        last_log_index: usize,
        last_log_term: usize,
        event_sender: &EventSender<T>,
    ) -> bool
    where
        T: Debug,
    {
        let vote_granted = self.decide_vote_grant(
            sender_id,
            term,
            last_log_term,
            last_log_index,
            event_sender,
        );
        let message = Message {
            content: MessageContent::RequestVoteResults {
                vote_granted,
            },
            seq,
            term: self.persistent_storage.term(),
        };
        println!("Sending message to {}: {:?}", sender_id, message);
        let _ = event_sender.unbounded_send(Event::SendMessage {
            message,
            receiver_id: sender_id,
        });
        vote_granted
    }

    fn process_request_vote_results(
        &mut self,
        sender_id: usize,
        vote_granted: bool,
        event_sender: &EventSender<T>,
        rpc_timeout: Duration,
        scheduler: &Scheduler,
    ) -> bool
    where
        T: Clone + Debug + Send + 'static,
    {
        if self.election_state != ElectionState::Candidate {
            println!(
                "Received unexpected or extra vote {} from {}",
                vote_granted, sender_id
            );
            return false;
        }
        println!("Received vote {} from {}", vote_granted, sender_id);
        if let Some(peer) = self.peers.get_mut(&sender_id) {
            peer.vote = Some(vote_granted);
        } else {
            return false;
        }
        let votes = self
            .peers
            .values()
            .filter(|peer| matches!(peer.vote, Some(vote) if vote))
            .count()
            + 1; // we always vote for ourselves (it's implicit)
        if votes > self.cluster.len() - votes {
            self.become_leader(event_sender, rpc_timeout, scheduler);
            true
        } else {
            false
        }
    }

    pub fn process_sink_item(
        &mut self,
        sink_item: SinkItem<T>,
        event_sender: &EventSender<T>,
        rpc_timeout: Duration,
        scheduler: &Scheduler,
    ) -> bool
    where
        T: Clone + Debug + Send + 'static,
    {
        match sink_item {
            SinkItem::ReceiveMessage {
                message,
                sender_id,
                received,
            } => {
                let cancel_election_timer = self.process_receive_message(
                    message,
                    sender_id,
                    event_sender,
                    rpc_timeout,
                    scheduler,
                );
                if let Some(received) = received {
                    let _ = received.send(());
                }
                cancel_election_timer
            },
        }
    }

    pub fn retransmit(
        &mut self,
        peer_id: usize,
        event_sender: &EventSender<T>,
        rpc_timeout: Duration,
        scheduler: &Scheduler,
    ) where
        T: 'static + Clone + Debug + Send,
    {
        if let Some(peer) = self.peers.get_mut(&peer_id) {
            peer.cancel_retransmission.take();
            let message = peer
                .last_message
                .take()
                .expect("lost message that needs to be retransmitted");
            peer.send_request(
                message,
                peer_id,
                event_sender,
                rpc_timeout,
                scheduler,
            )
        }
    }

    fn send_new_message_broadcast(
        &mut self,
        content: MessageContent<T>,
        event_sender: &EventSender<T>,
        rpc_timeout: Duration,
        scheduler: &Scheduler,
    ) where
        T: 'static + Clone + Debug + Send,
    {
        let term = self.persistent_storage.term();
        for (&peer_id, peer) in &mut self.peers {
            peer.send_new_request(
                content.clone(),
                peer_id,
                term,
                event_sender,
                rpc_timeout,
                scheduler,
            );
        }
    }

    pub fn take_retransmission_futures(
        &mut self
    ) -> impl IntoIterator<Item = WorkItemFuture<T>> + '_ {
        self.peers
            .values_mut()
            .filter_map(|peer| peer.retransmission_future.take())
    }

    pub fn upkeep_election_timeout_future(
        &mut self,
        election_timeout: &Range<Duration>,
        scheduler: &Scheduler,
    ) -> Option<WorkItemFuture<T>>
    where
        T: 'static + Clone + Debug + Send,
    {
        if self.election_state == ElectionState::Leader
            || self.cancel_election_timeout.is_some()
        {
            return None;
        }
        let timeout_duration =
            self.rng.gen_range(election_timeout.start, election_timeout.end);
        println!(
            "Setting election timer to {:?} ({:?})",
            timeout_duration, election_timeout
        );
        let (future, cancel_future) = make_cancellable_timeout_future(
            WorkItem::ElectionTimeout,
            timeout_duration,
            #[cfg(test)]
            ScheduledEvent::ElectionTimeout,
            &scheduler,
        );
        self.cancel_election_timeout.replace(cancel_future);
        Some(future)
    }
}
