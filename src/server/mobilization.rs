use super::{
    make_cancellable_timeout_future,
    peer::Peer,
    ElectionState,
    Event,
    EventSender,
    MobilizeArgs,
    SinkItem,
    WorkItemContent,
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
use log::{
    debug,
    info,
    warn,
};
use rand::{
    rngs::StdRng,
    Rng as _,
    SeedableRng as _,
};
use std::{
    cmp::Ordering,
    collections::{
        BinaryHeap,
        HashMap,
        HashSet,
    },
    fmt::Debug,
    ops::Range,
    time::Duration,
};

struct ProcessAppendEntriesResultsArgs<'a, 'b, T> {
    event_sender: &'a EventSender<T>,
    install_snapshot_timeout: Duration,
    match_index: usize,
    rpc_timeout: Duration,
    scheduler: &'b Scheduler,
    sender_id: usize,
    term: usize,
}

#[derive(PartialEq)]
enum RequestDecision {
    Accept,
    AcceptAndUpdateTerm,
    Reject,
}

enum HistoryComparison {
    NothingInCommon,
    Same,
    SameUpToPrevious,
}

enum LogEntryDisposition {
    Future,
    Next,
    Possessed,
}

pub struct Mobilization<T> {
    cancel_election_timeout: Option<oneshot::Sender<()>>,
    cancel_heartbeat: Option<oneshot::Sender<()>>,
    #[cfg(test)]
    cancellations_pending: usize,
    cluster: HashSet<usize>,
    commit_index: usize,
    election_state: ElectionState,
    id: usize,
    log: Box<dyn Log<Command = T>>,
    peers: HashMap<usize, Peer<T>>,
    persistent_storage: Box<dyn PersistentStorage>,
    rng: StdRng,
    #[cfg(test)]
    synchronize_ack: Option<oneshot::Sender<()>>,
}

impl<T> Mobilization<T> {
    fn attempt_accept_append_entries(
        &mut self,
        append_entries: AppendEntriesContent<T>,
        event_sender: &EventSender<T>,
    ) -> usize {
        if self.election_state != ElectionState::Follower {
            self.become_follower(event_sender);
        }
        let mut match_index = append_entries.prev_log_index;
        let mut match_term = append_entries.prev_log_term;
        for new_log_entry in append_entries.log {
            let new_log_term = new_log_entry.term;
            match self.determine_log_entry_disposition(match_index) {
                LogEntryDisposition::Possessed => match self
                    .compare_log_history(match_index, match_term, new_log_term)
                {
                    HistoryComparison::NothingInCommon => {
                        match_index = 0;
                        break;
                    },
                    HistoryComparison::SameUpToPrevious => {
                        self.log.truncate(match_index);
                        self.log.append_one(new_log_entry);
                    },
                    HistoryComparison::Same => (),
                },
                LogEntryDisposition::Next => {
                    if match_term != self.log.last_term() {
                        match_index = 0;
                        break;
                    }
                    self.log.append_one(new_log_entry);
                },
                LogEntryDisposition::Future => {
                    match_index = 0;
                    break;
                },
            }
            match_index += 1;
            match_term = new_log_term;
        }
        match_index
    }

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
        for peer in self.peers.values_mut() {
            peer.vote = None;
        }
        self.send_new_message_broadcast(
            MessageContent::RequestVote {
                candidate_id: self.id,
                last_log_index: self.log.last_index(),
                last_log_term: self.log.last_term(),
            },
            event_sender,
            rpc_timeout,
            scheduler,
        );
    }

    fn become_follower(
        &mut self,
        event_sender: &EventSender<T>,
    ) {
        self.change_election_state(ElectionState::Follower, event_sender);
        self.cancel_heartbeat_timer();
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
        let no_op = LogEntry {
            term: self.persistent_storage.term(),
            command: None,
        };
        let prev_log_index = self.log.last_index();
        let prev_log_term = self.log.last_term();
        self.log.append_one(no_op.clone());
        info!("Asserting leadership with no-op entry {}", prev_log_index + 1);
        for peer in self.peers.values_mut() {
            peer.match_index = 0;
        }
        self.send_new_message_broadcast(
            MessageContent::AppendEntries(AppendEntriesContent {
                leader_commit: self.commit_index,
                prev_log_index,
                prev_log_term,
                log: vec![no_op],
            }),
            event_sender,
            rpc_timeout,
            scheduler,
        );
    }

    pub fn cancel_election_timer(&mut self) {
        if let Some(cancel) = self.cancel_election_timeout.take() {
            let _ = cancel.send(());
            #[cfg(test)]
            {
                self.cancellations_pending += 1;
                debug!(
                    "Canceling election timer; {} cancellations pending",
                    self.cancellations_pending
                );
            }
        }
    }

    pub fn cancel_heartbeat_timer(&mut self) {
        if let Some(cancel_heartbeat) = self.cancel_heartbeat.take() {
            let _ = cancel_heartbeat.send(());
            #[cfg(test)]
            {
                self.cancellations_pending += 1;
                debug!(
                    "Cancelling heartbeat timer; {} cancellations pending",
                    self.cancellations_pending
                );
            }
        }
    }

    fn cancel_retransmission(
        &mut self,
        peer_id: usize,
    ) {
        if let Some(peer) = self.peers.get_mut(&peer_id) {
            #[cfg(not(test))]
            peer.cancel_retransmission();
            #[cfg(test)]
            if peer.cancel_retransmission().is_some() {
                self.cancellations_pending += 1;
                debug!(
                    "Cancelling retransmission timer; {} cancellations pending",
                    self.cancellations_pending
                );
            }
        }
    }

    fn change_election_state(
        &mut self,
        new_election_state: ElectionState,
        event_sender: &EventSender<T>,
    ) {
        let term = self.persistent_storage.term();
        info!(
            "State: {:?} -> {:?} (term {})",
            self.election_state, new_election_state, term
        );
        self.election_state = new_election_state;
        #[cfg(test)]
        let mut cancellations = 0;
        for peer in self.peers.values_mut() {
            #[cfg(not(test))]
            peer.cancel_retransmission();
            #[cfg(test)]
            if peer.cancel_retransmission().is_some() {
                cancellations += 1;
                debug!("Cancelling retransmission timer (Mobilization)");
            }
        }
        #[cfg(test)]
        {
            self.cancellations_pending += cancellations;
        }
        let _ = event_sender.unbounded_send(Event::ElectionStateChange {
            election_state: self.election_state,
            term,
            voted_for: self.persistent_storage.voted_for(),
        });
    }

    #[cfg(test)]
    pub fn count_cancellation(&mut self) {
        self.cancellations_pending -= 1;
        if self.cancellations_pending == 0 {
            if let Some(synchronize_ack) = self.synchronize_ack.take() {
                debug!("All cancellations counted; completing synchronization");
                let _ = synchronize_ack.send(());
            }
        }
    }

    fn decide_request_verdict(
        &self,
        term: usize,
    ) -> RequestDecision {
        match term.cmp(&self.persistent_storage.term()) {
            Ordering::Less => RequestDecision::Reject,
            Ordering::Equal => {
                if self.election_state == ElectionState::Leader {
                    RequestDecision::Reject
                } else {
                    RequestDecision::Accept
                }
            },
            Ordering::Greater => RequestDecision::AcceptAndUpdateTerm,
        }
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
            self.become_follower(event_sender);
        }
        true
    }

    fn commit_log(
        &mut self,
        leader_commit: usize,
        event_sender: &EventSender<T>,
    ) {
        let new_commit_index =
            std::cmp::min(leader_commit, self.log.last_index());
        if new_commit_index > self.commit_index {
            info!(
                "{:?}: Committing log from {} to {}",
                self.election_state, self.commit_index, new_commit_index
            );
            self.commit_index = new_commit_index;
            let _ = event_sender
                .unbounded_send(Event::LogCommitted(self.commit_index));
        }
    }

    fn compare_log_history(
        &self,
        prev_log_index: usize,
        prev_log_term: usize,
        new_log_term: usize,
    ) -> HistoryComparison {
        if prev_log_index >= self.log.base_index() {
            if prev_log_term != self.log_entry_term(prev_log_index) {
                return HistoryComparison::NothingInCommon;
            }
            if new_log_term != self.log_entry_term(prev_log_index + 1) {
                return HistoryComparison::SameUpToPrevious;
            }
        }
        HistoryComparison::Same
    }

    fn determine_log_entry_disposition(
        &self,
        index: usize,
    ) -> LogEntryDisposition {
        match index.cmp(&self.log.last_index()) {
            Ordering::Less => LogEntryDisposition::Possessed,
            Ordering::Equal => LogEntryDisposition::Next,
            Ordering::Greater => LogEntryDisposition::Future,
        }
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
        if self.election_state != ElectionState::Leader {
            self.become_candidate(event_sender, rpc_timeout, &scheduler);
        }
    }

    fn find_majority_match_index(&self) -> usize {
        let mut match_indices = self
            .peers
            .values()
            .map(|peer| peer.match_index)
            .collect::<BinaryHeap<_>>();
        let mut n = self.cluster.len() / 2 - 1;
        loop {
            let match_index =
                match_indices.pop().expect("unexpectedly short peer list");
            if n == 0 {
                break match_index;
            }
            n -= 1;
        }
    }

    pub fn heartbeat(
        &mut self,
        event_sender: &EventSender<T>,
        rpc_timeout: Duration,
        scheduler: &Scheduler,
    ) where
        T: Clone + Debug + Send + 'static,
    {
        self.cancel_heartbeat.take();
        if self.election_state == ElectionState::Leader {
            let prev_log_index = self.log.last_index();
            for (&peer_id, peer) in &mut self.peers {
                if peer.match_index == prev_log_index
                    && !peer.awaiting_response()
                {
                    peer.send_new_request(
                        MessageContent::AppendEntries(AppendEntriesContent {
                            leader_commit: self.commit_index,
                            prev_log_index,
                            prev_log_term: self.log.last_term(),
                            log: vec![],
                        }),
                        peer_id,
                        self.persistent_storage.term(),
                        event_sender,
                        rpc_timeout,
                        scheduler,
                    );
                }
            }
        }
    }

    fn log_entry_term(
        &self,
        index: usize,
    ) -> usize {
        self.log.entry_term(index).unwrap_or_else(|| {
            panic!("log entry {} should be in log but isn't", index)
        })
    }

    pub fn new(mobilize_args: MobilizeArgs<T>) -> Self {
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
            cancel_heartbeat: None,
            #[cfg(test)]
            cancellations_pending: 0,
            cluster: mobilize_args.cluster,
            commit_index: 0,
            election_state: ElectionState::Follower,
            id: mobilize_args.id,
            log: mobilize_args.log,
            peers,
            persistent_storage: mobilize_args.persistent_storage,
            rng: StdRng::from_entropy(),
            #[cfg(test)]
            synchronize_ack: None,
        }
    }

    fn process_append_entries(
        &mut self,
        sender_id: usize,
        seq: usize,
        term: usize,
        append_entries: AppendEntriesContent<T>,
        event_sender: &EventSender<T>,
    ) -> bool
    where
        T: 'static + Debug,
    {
        let decision = self.decide_request_verdict(term);
        if decision == RequestDecision::AcceptAndUpdateTerm {
            self.persistent_storage.update(term, None);
        }
        let leader_commit = append_entries.leader_commit;
        let match_index = if decision == RequestDecision::Reject {
            0
        } else {
            self.attempt_accept_append_entries(append_entries, event_sender)
        };
        if leader_commit > self.commit_index {
            self.commit_log(leader_commit, event_sender);
        }
        let message = Message {
            content: MessageContent::AppendEntriesResults {
                match_index,
            },
            seq,
            term: self.persistent_storage.term(),
        };
        debug!(
            "Sending AppendEntriesResults ({}) to {}",
            match_index, sender_id
        );
        let _ = event_sender.unbounded_send(Event::SendMessage {
            message,
            receiver_id: sender_id,
        });
        true
    }

    fn process_append_entries_results(
        &mut self,
        args: ProcessAppendEntriesResultsArgs<T>,
    ) where
        T: 'static + Clone + Debug + Send,
    {
        if args.term > self.persistent_storage.term() {
            self.persistent_storage.update(args.term, None);
            self.become_follower(args.event_sender);
        }
        if self.election_state != ElectionState::Leader {
            warn!(
                "Not processing AppendEntriesResults ({}) from {} because we are {:?}, not Leader",
                args.match_index,
                args.sender_id,
                self.election_state
            );
            return;
        } else {
            debug!(
                "Received AppendEntriesResults ({}) from {}",
                args.match_index, args.sender_id
            );
        }
        if let Some(peer) = self.peers.get_mut(&args.sender_id) {
            let match_index =
                std::cmp::min(args.match_index, self.log.last_index());
            if match_index > peer.match_index {
                info!(
                    "{} advanced match index {} -> {}",
                    args.sender_id, peer.match_index, match_index
                );
                peer.match_index = match_index;
            }
        }
        let majority_match_index = self.find_majority_match_index();
        if majority_match_index > self.commit_index {
            self.commit_log(majority_match_index, args.event_sender);
        }
        if let Some(peer) = self.peers.get_mut(&args.sender_id) {
            if peer.match_index < self.log.last_index() {
                let prev_log_term = match peer
                    .match_index
                    .cmp(&self.log.base_index())
                {
                    Ordering::Less => None,
                    Ordering::Equal => Some(self.log.base_term()),
                    Ordering::Greater => self.log.entry_term(peer.match_index),
                };
                if let Some(prev_log_term) = prev_log_term {
                    peer.send_new_request(
                        MessageContent::AppendEntries(AppendEntriesContent {
                            leader_commit: self.commit_index,
                            prev_log_index: peer.match_index,
                            prev_log_term,
                            log: self.log.entries(peer.match_index),
                        }),
                        args.sender_id,
                        self.persistent_storage.term(),
                        args.event_sender,
                        args.rpc_timeout,
                        args.scheduler,
                    );
                } else {
                    peer.send_new_request(
                        MessageContent::InstallSnapshot {
                            last_included_index: self.log.base_index(),
                            last_included_term: self.log.base_term(),
                            snapshot: self.log.snapshot(),
                        },
                        args.sender_id,
                        self.persistent_storage.term(),
                        args.event_sender,
                        args.install_snapshot_timeout,
                        args.scheduler,
                    );
                }
            }
        }
    }

    fn process_receive_message(
        &mut self,
        message: Message<T>,
        sender_id: usize,
        event_sender: &EventSender<T>,
        rpc_timeout: Duration,
        install_snapshot_timeout: Duration,
        scheduler: &Scheduler,
    ) -> bool
    where
        T: Clone + Debug + Send + 'static,
    {
        match message.content {
            MessageContent::RequestVoteResults {
                vote_granted,
            } => {
                info!(
                    "Received vote {} from {} for term {} (we are {:?} in term {})",
                    vote_granted,
                    sender_id,
                    message.term,
                    self.election_state,
                    self.persistent_storage.term()
                );
                if message.term < self.persistent_storage.term() {
                    return false;
                }
                if self
                    .peers
                    .get(&sender_id)
                    .filter(|peer| peer.last_seq == message.seq)
                    .is_none()
                {
                    return false;
                }
                self.cancel_retransmission(sender_id);
                self.process_request_vote_results(
                    sender_id,
                    vote_granted,
                    message.term,
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
                    event_sender,
                ),
            MessageContent::AppendEntriesResults {
                match_index,
            } => {
                if self
                    .peers
                    .get(&sender_id)
                    .filter(|peer| peer.last_seq == message.seq)
                    .is_none()
                {
                    debug!(
                        "Received old append entries results ({}) from {}",
                        match_index, sender_id
                    );
                    return false;
                }
                self.cancel_retransmission(sender_id);
                self.process_append_entries_results(
                    ProcessAppendEntriesResultsArgs {
                        event_sender,
                        install_snapshot_timeout,
                        match_index,
                        rpc_timeout,
                        scheduler,
                        sender_id,
                        term: message.term,
                    },
                );
                false
            },
            _ => todo!(),
        }
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
        debug!(
            "Sending RequestVoteResults ({}) message to {}",
            vote_granted, sender_id
        );
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
        term: usize,
        event_sender: &EventSender<T>,
        rpc_timeout: Duration,
        scheduler: &Scheduler,
    ) -> bool
    where
        T: Clone + Debug + Send + 'static,
    {
        if term > self.persistent_storage.term() {
            self.persistent_storage.update(term, None);
            self.become_follower(event_sender);
        }
        if self.election_state != ElectionState::Candidate {
            debug!(
                "Received unexpected or extra vote {} from {}",
                vote_granted, sender_id
            );
            return false;
        }
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
        install_snapshot_timeout: Duration,
        scheduler: &Scheduler,
    ) -> bool
    where
        T: Clone + Debug + Send + 'static,
    {
        match sink_item {
            SinkItem::ReceiveMessage {
                message,
                sender_id,
            } => self.process_receive_message(
                message,
                sender_id,
                event_sender,
                rpc_timeout,
                install_snapshot_timeout,
                scheduler,
            ),
            #[cfg(test)]
            SinkItem::Synchronize(received) => {
                if self.cancellations_pending == 0 {
                    debug!(
                        "No cancellations counted; completing synchronization"
                    );
                    let _ = received.send(());
                } else {
                    debug!(
                        "{} cancellations pending; awaiting synchronization",
                        self.cancellations_pending
                    );
                    self.synchronize_ack.replace(received);
                }
                false
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
            if let Some(message) = peer.cancel_retransmission() {
                peer.send_request(
                    message,
                    peer_id,
                    event_sender,
                    rpc_timeout,
                    scheduler,
                )
            }
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
    ) -> impl Iterator<Item = WorkItemFuture<T>> + '_ {
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
        debug!(
            "Setting election timer to {:?} ({:?})",
            timeout_duration, election_timeout
        );
        let (future, cancel_future) = make_cancellable_timeout_future(
            WorkItemContent::ElectionTimeout,
            timeout_duration,
            #[cfg(test)]
            ScheduledEvent::ElectionTimeout,
            &scheduler,
        );
        self.cancel_election_timeout.replace(cancel_future);
        Some(future)
    }

    pub fn upkeep_heartbeat_future(
        &mut self,
        heartbeat_interval: Duration,
        scheduler: &Scheduler,
    ) -> Option<WorkItemFuture<T>>
    where
        T: 'static + Clone + Debug + Send,
    {
        if self.election_state != ElectionState::Leader
            || self.cancel_heartbeat.is_some()
        {
            return None;
        }
        let (future, cancel_future) = make_cancellable_timeout_future(
            WorkItemContent::Heartbeat,
            heartbeat_interval,
            #[cfg(test)]
            ScheduledEvent::Heartbeat,
            &scheduler,
        );
        self.cancel_heartbeat.replace(cancel_future);
        Some(future)
    }
}
