use super::{
    make_cancellable_timeout_future,
    peer::Peer,
    Command,
    CommandReceiver,
    ElectionState,
    Event,
    EventSender,
    WorkItem,
    WorkItemContent,
    WorkItemFuture,
};
use crate::{
    utilities::{
        sorted,
        spread,
    },
    AppendEntriesContent,
    ClusterConfiguration,
    Log,
    LogEntry,
    LogEntryCommand,
    Message,
    MessageContent,
    PersistentStorage,
    ServerConfiguration,
};
#[cfg(test)]
use crate::{
    ScheduledEvent,
    Scheduler,
};
use futures::{
    channel::oneshot,
    future,
    FutureExt as _,
    StreamExt as _,
};
use log::{
    debug,
    info,
    trace,
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
    iter::once,
};

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

#[derive(PartialEq)]
enum RequestDecision {
    Accept,
    AcceptAndUpdateTerm,
    Reject,
}

pub struct Inner<S, T> {
    cancel_election_timeout: Option<oneshot::Sender<()>>,
    cancel_heartbeat: Option<oneshot::Sender<()>>,
    cancel_min_election_timeout: Option<oneshot::Sender<()>>,
    #[cfg(test)]
    cancellations_pending: usize,
    catch_up_index: usize,
    commit_index: usize,
    configuration: ServerConfiguration,
    election_state: ElectionState,
    event_sender: EventSender<S, T>,
    id: usize,
    ignore_vote_requests: bool,
    log: Box<dyn Log<S, Command = T>>,
    peers: HashMap<usize, Peer<S, T>>,
    pending_reconfiguration_ids: Option<HashSet<usize>>,
    persistent_storage: Box<dyn PersistentStorage>,
    rng: StdRng,
    #[cfg(test)]
    scheduler: Scheduler,
    #[cfg(test)]
    synchronize_ack: Option<oneshot::Sender<()>>,
}

impl<S, T> Inner<S, T> {
    fn append_reconfiguration_command(
        &mut self,
        command: LogEntryCommand<T>,
        skip_peer_id: Option<usize>,
    ) where
        S: 'static + Clone + Debug + Send,
        T: 'static + Clone + Debug + Send,
    {
        self.cancel_heartbeat_timer();
        let term = self.persistent_storage.term();
        let prev_log_index = self.log.last_index();
        let prev_log_term = self.log.last_term();
        info!("Adding {:?} command from index {}", command, prev_log_index);
        let new_entry = LogEntry {
            term,
            command: Some(command),
        };
        for (&peer_id, peer) in &mut self.peers {
            if let Some(skip_peer_id) = &skip_peer_id {
                if peer_id == *skip_peer_id {
                    continue;
                }
            }
            if !peer.awaiting_response() {
                peer.send_new_request(
                    MessageContent::AppendEntries(AppendEntriesContent {
                        leader_commit: self.commit_index,
                        prev_log_index,
                        prev_log_term,
                        log: vec![new_entry.clone()],
                    }),
                    peer_id,
                    term,
                    &self.event_sender,
                    self.configuration.rpc_timeout,
                    #[cfg(test)]
                    &self.scheduler,
                )
            }
        }
        self.log.append_one(new_entry);
        let cluster_configuration = self.log.cluster_configuration();
        let _ = self.event_sender.unbounded_send(Event::Reconfiguration(
            cluster_configuration.clone(),
        ));
        info!("Configuration is now {:?}", cluster_configuration);
    }

    fn attempt_accept_append_entries(
        &mut self,
        append_entries: AppendEntriesContent<T>,
    ) -> (bool, usize) {
        if self.election_state != ElectionState::Follower {
            self.become_follower();
        }
        let mut match_index = append_entries.prev_log_index;
        let mut match_term = append_entries.prev_log_term;
        let success = append_entries.log.into_iter().all(|new_log_entry| {
            let new_log_term = new_log_entry.term;
            match self.determine_log_entry_disposition(match_index) {
                LogEntryDisposition::Possessed => match self
                    .compare_log_history(match_index, match_term, new_log_term)
                {
                    HistoryComparison::NothingInCommon => {
                        match_index = 0;
                        return false;
                    },
                    HistoryComparison::SameUpToPrevious => {
                        self.log.truncate(match_index);
                        self.log.append_one(new_log_entry);
                    },
                    HistoryComparison::Same => (),
                },
                LogEntryDisposition::Next => {
                    if match_term != self.log.last_term() {
                        match_index = if match_index > self.log.base_index() {
                            self.log.last_index() - 1
                        } else {
                            0
                        };
                        return false;
                    }
                    self.log.append_one(new_log_entry);
                },
                LogEntryDisposition::Future => {
                    match_index = self.log.last_index();
                    return false;
                },
            }
            match_index += 1;
            match_term = new_log_term;
            true
        });
        (success, match_index + 1)
    }

    fn become_candidate(&mut self)
    where
        S: 'static + Clone + Debug + Send,
        T: 'static + Clone + Debug + Send,
    {
        let term = self.persistent_storage.term() + 1;
        self.persistent_storage.update(term, Some(self.id));
        self.change_election_state(ElectionState::Candidate);
        for peer in self.peers.values_mut() {
            peer.vote = None;
        }
        let last_log_index = self.log.last_index();
        let last_log_term = self.log.last_term();
        info!("Requesting votes ({};{})", last_log_index, last_log_term);
        self.send_new_message_broadcast(MessageContent::RequestVote {
            last_log_index,
            last_log_term,
        });
    }

    fn become_follower(&mut self) {
        if self.pending_reconfiguration_ids.take().is_some() {
            self.cancel_reconfiguration();
        }
        self.change_election_state(ElectionState::Follower);
        self.cancel_heartbeat_timer();
    }

    fn become_leader(&mut self)
    where
        S: 'static + Clone + Debug + Send,
        T: 'static + Clone + Debug + Send,
    {
        self.change_election_state(ElectionState::Leader);
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
        self.send_new_message_broadcast(MessageContent::AppendEntries(
            AppendEntriesContent {
                leader_commit: self.commit_index,
                prev_log_index,
                prev_log_term,
                log: vec![no_op],
            },
        ));
    }

    fn cancel_election_timers(&mut self) {
        if let Some(cancel) = self.cancel_election_timeout.take() {
            let _ = cancel.send(());
            #[cfg(test)]
            {
                self.cancellations_pending += 1;
                trace!(
                    "Canceling election timer; {} cancellations pending",
                    self.cancellations_pending
                );
            }
        }
        if let Some(cancel) = self.cancel_min_election_timeout.take() {
            let _ = cancel.send(());
            #[cfg(test)]
            {
                self.cancellations_pending += 1;
                trace!(
                    "Canceling minimum election timer; {} cancellations pending",
                    self.cancellations_pending
                );
            }
        }
    }

    fn cancel_heartbeat_timer(&mut self) {
        if let Some(cancel_heartbeat) = self.cancel_heartbeat.take() {
            let _ = cancel_heartbeat.send(());
            #[cfg(test)]
            {
                self.cancellations_pending += 1;
                trace!(
                    "Cancelling heartbeat timer; {} cancellations pending",
                    self.cancellations_pending
                );
            }
        }
    }

    fn cancel_reconfiguration(&mut self) {
        info!(
            "Reconfiguration cancelled; back to {:?}",
            self.log.cluster_configuration()
        );
        self.drop_old_peers();
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
                trace!(
                    "Cancelling retransmission timer; {} cancellations pending",
                    self.cancellations_pending
                );
            }
        }
    }

    fn change_election_state(
        &mut self,
        new_election_state: ElectionState,
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
                trace!("Cancelling retransmission timer (Election State)");
            }
        }
        #[cfg(test)]
        {
            self.cancellations_pending += cancellations;
        }
        let _ = self.event_sender.unbounded_send(Event::ElectionStateChange {
            election_state: self.election_state,
            term,
            voted_for: self.persistent_storage.voted_for(),
        });
    }

    fn commit_log(
        &mut self,
        leader_commit: usize,
    ) {
        let new_commit_index =
            std::cmp::min(leader_commit, self.log.last_index());
        if new_commit_index > self.commit_index {
            info!(
                "{:?}: Committing log from {} to {}",
                self.election_state, self.commit_index, new_commit_index
            );
            self.commit_index = new_commit_index;
            let _ = self
                .event_sender
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

    #[cfg(test)]
    fn count_cancellation(&mut self) {
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
            self.become_follower();
        }
        true
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

    fn drop_old_peers(&mut self) {
        let current_configuration = self.log.cluster_configuration();
        if let ClusterConfiguration::Single(current_ids) =
            &current_configuration
        {
            let peers_to_drop = self
                .peers
                .keys()
                .filter(|peer_id| !current_ids.contains(peer_id))
                .copied()
                .collect::<Vec<_>>();
            for id in peers_to_drop {
                self.peers.remove(&id);
            }
        }
    }

    fn election_timeout(&mut self)
    where
        S: 'static + Clone + Debug + Send,
        T: 'static + Clone + Debug + Send,
    {
        self.cancel_election_timeout.take();
        self.ignore_vote_requests = false;
        if self.election_state != ElectionState::Leader {
            self.become_candidate();
        }
    }

    fn find_majority_match_index_among(
        &self,
        ids: &HashSet<usize>,
    ) -> usize {
        let mut match_indices = ids
            .iter()
            .filter_map(|id| {
                if *id == self.id {
                    Some(self.log.last_index())
                } else {
                    self.peers.get(id).map(|peer| peer.match_index)
                }
            })
            .collect::<BinaryHeap<_>>();
        let mut n = (match_indices.len() - 1) / 2;
        loop {
            let match_index = match_indices.pop().unwrap_or(0);
            if n == 0 {
                break match_index;
            }
            n -= 1;
        }
    }

    fn find_majority_match_index(&self) -> usize {
        match &self.log.cluster_configuration() {
            ClusterConfiguration::Single(ids) => {
                self.find_majority_match_index_among(ids)
            },
            ClusterConfiguration::Joint {
                old_ids,
                new_ids,
                ..
            } => self
                .find_majority_match_index_among(old_ids)
                .min(self.find_majority_match_index_among(new_ids)),
        }
    }

    fn finish_reconfiguration(
        &mut self,
        skip_peer_id: usize,
    ) where
        S: 'static + Clone + Debug + Send,
        T: 'static + Clone + Debug + Send,
    {
        self.append_reconfiguration_command(
            LogEntryCommand::FinishReconfiguration,
            Some(skip_peer_id),
        );
        self.drop_old_peers();
        if !self.log.cluster_configuration().contains(self.id) {
            self.cancel_heartbeat_timer();
        }
    }

    fn heartbeat(&mut self)
    where
        S: Clone + Debug + Send + 'static,
        T: Clone + Debug + Send + 'static,
    {
        self.cancel_heartbeat.take();
        if self.election_state == ElectionState::Leader {
            for (&peer_id, peer) in &mut self.peers {
                if !peer.awaiting_response() {
                    debug!("Sending heartbeat to {}", peer_id);
                    peer.send_new_request(
                        MessageContent::AppendEntries(AppendEntriesContent {
                            leader_commit: self.commit_index,
                            prev_log_index: self.log.last_index(),
                            prev_log_term: self.log.last_term(),
                            log: vec![],
                        }),
                        peer_id,
                        self.persistent_storage.term(),
                        &self.event_sender,
                        self.configuration.rpc_timeout,
                        #[cfg(test)]
                        &self.scheduler,
                    );
                }
            }
        }
    }

    fn install_snapshot(
        &mut self,
        (last_included_index, last_included_term, snapshot): (usize, usize, S),
    ) {
        self.log.install_snapshot(
            last_included_index,
            last_included_term,
            snapshot,
        );
    }

    fn log_entry_term(
        &self,
        index: usize,
    ) -> usize {
        self.log.entry_term(index).unwrap_or_else(|| {
            panic!("log entry {} should be in log but isn't", index)
        })
    }

    fn majority_vote(
        &self,
        ids: &HashSet<usize>,
    ) -> bool {
        let votes = ids
            .iter()
            .filter(|id| {
                if **id == self.id {
                    true
                } else if let Some(peer) = self.peers.get(*id) {
                    matches!(peer.vote, Some(vote) if vote)
                } else {
                    false
                }
            })
            .count();
        votes >= ids.len() - votes
    }

    fn minimum_election_timeout(&mut self) {
        self.cancel_min_election_timeout.take();
        self.ignore_vote_requests = false;
    }

    pub fn new(
        id: usize,
        configuration: ServerConfiguration,
        log: Box<dyn Log<S, Command = T>>,
        persistent_storage: Box<dyn PersistentStorage>,
        event_sender: EventSender<S, T>,
        #[cfg(test)] scheduler: Scheduler,
    ) -> Self {
        let peers = log
            .cluster_configuration()
            .peers(id)
            .map(|id| (*id, Peer::default()))
            .collect();
        Self {
            cancel_election_timeout: None,
            cancel_heartbeat: None,
            cancel_min_election_timeout: None,
            #[cfg(test)]
            cancellations_pending: 0,
            catch_up_index: 0,
            commit_index: 0,
            configuration,
            election_state: ElectionState::Follower,
            event_sender,
            id,
            ignore_vote_requests: true,
            log,
            peers,
            pending_reconfiguration_ids: None,
            persistent_storage,
            rng: StdRng::from_entropy(),
            #[cfg(test)]
            scheduler,
            #[cfg(test)]
            synchronize_ack: None,
        }
    }

    fn perform_operation_which_may_change_configuration<O, A, R>(
        &mut self,
        operation: O,
        argument: A,
    ) -> R
    where
        O: FnOnce(&mut Self, A) -> R,
    {
        let cluster_configuration_before = self.log.cluster_configuration();
        let result = operation(self, argument);
        let cluster_configuration_after = self.log.cluster_configuration();
        let new_ids = cluster_configuration_after
            .peers(self.id)
            .copied()
            .collect::<HashSet<_>>();
        let old_ids = self.peers.keys().copied().collect::<HashSet<_>>();
        for old_peer_id in old_ids.difference(&new_ids) {
            self.peers.remove(old_peer_id);
        }
        for new_peer_id in new_ids.difference(&old_ids) {
            self.peers.insert(*new_peer_id, Peer::default());
        }
        if cluster_configuration_before != cluster_configuration_after {
            let _ = self.event_sender.unbounded_send(Event::Reconfiguration(
                cluster_configuration_after.clone(),
            ));
            info!("Configuration is now {:?}", cluster_configuration_after);
        }
        result
    }

    fn process_add_commands(
        &mut self,
        commands: Vec<T>,
    ) where
        S: 'static + Clone + Debug + Send,
        T: 'static + Clone + Debug + Send,
    {
        if self.election_state != ElectionState::Leader {
            warn!(
                "Not adding {} commands because we are {:?}",
                commands.len(),
                self.election_state
            );
            return;
        }
        self.cancel_heartbeat_timer();
        let term = self.persistent_storage.term();
        let prev_log_index = self.log.last_index();
        let prev_log_term = self.log.last_term();
        info!(
            "Adding {} commands from index {}",
            commands.len(),
            prev_log_index
        );
        let new_entries = commands
            .into_iter()
            .map(move |command| LogEntry {
                term,
                command: Some(LogEntryCommand::Custom(command)),
            })
            .collect::<Vec<_>>();
        for (&peer_id, peer) in &mut self.peers {
            if !peer.awaiting_response() {
                info!("Sending new commands to {}", peer_id);
                peer.send_new_request(
                    MessageContent::AppendEntries(AppendEntriesContent {
                        leader_commit: self.commit_index,
                        prev_log_index,
                        prev_log_term,
                        log: new_entries.clone(),
                    }),
                    peer_id,
                    term,
                    &self.event_sender,
                    self.configuration.rpc_timeout,
                    #[cfg(test)]
                    &self.scheduler,
                )
            }
        }
        self.log.append(Box::new(new_entries.into_iter()));
    }

    fn process_append_entries(
        &mut self,
        sender_id: usize,
        seq: usize,
        term: usize,
        append_entries: AppendEntriesContent<T>,
    ) where
        T: 'static + Debug,
    {
        debug!(
            "Received AppendEntries({} on top of {};{}, {} committed) from {} for term {} (we are {:?} in term {})",
            append_entries.log.len(),
            append_entries.prev_log_index,
            append_entries.prev_log_term,
            append_entries.leader_commit,
            sender_id,
            term,
            self.election_state,
            self.persistent_storage.term()
        );
        let decision = self.decide_request_verdict(term);
        if decision == RequestDecision::AcceptAndUpdateTerm {
            self.persistent_storage.update(term, None);
        } else if self.election_state == ElectionState::Leader {
            return;
        }
        let leader_commit = append_entries.leader_commit;
        let (success, next_log_index) = if decision == RequestDecision::Reject {
            (false, 1)
        } else {
            self.perform_operation_which_may_change_configuration(
                Self::attempt_accept_append_entries,
                append_entries,
            )
        };
        if leader_commit > self.commit_index {
            self.commit_log(leader_commit);
        }
        let message = Message {
            content: MessageContent::AppendEntriesResponse {
                success,
                next_log_index,
            },
            seq,
            term: self.persistent_storage.term(),
        };
        debug!(
            "Sending AppendEntriesResponse ({}) to {}",
            next_log_index, sender_id
        );
        let _ = self.event_sender.unbounded_send(Event::SendMessage {
            message,
            receiver_id: sender_id,
        });
        self.cancel_election_timers();
    }

    fn process_append_entries_response(
        &mut self,
        success: bool,
        next_log_index: usize,
        sender_id: usize,
        sender_term: usize,
        seq: usize,
    ) where
        S: 'static + Clone + Debug + Send,
        T: 'static + Clone + Debug + Send,
    {
        let term = self.persistent_storage.term();
        debug!(
            "Received AppendEntriesResponse ({}/{}) from {} for term {} (we are {:?} in term {})",
            success,
            next_log_index,
            sender_id,
            sender_term,
            self.election_state,
            term
        );
        if sender_term < term {
            return;
        }
        if self
            .peers
            .get(&sender_id)
            .filter(|peer| peer.last_seq == seq)
            .is_none()
        {
            return;
        }
        self.cancel_retransmission(sender_id);
        if sender_term > term {
            self.persistent_storage.update(sender_term, None);
            self.become_follower();
        }
        if self.election_state != ElectionState::Leader {
            warn!(
                "Not processing AppendEntriesResponse ({}) from {} because we are {:?}, not Leader",
                next_log_index,
                sender_id,
                self.election_state
            );
            return;
        }
        let next_log_index =
            std::cmp::min(next_log_index, self.log.last_index() + 1);
        if success {
            if let Some(peer) = self.peers.get_mut(&sender_id) {
                let match_index = next_log_index - 1;
                if match_index > peer.match_index {
                    info!(
                        "{} advanced match index {} -> {}",
                        sender_id, peer.match_index, match_index
                    );
                    peer.match_index = match_index;
                }
            }
        }
        let majority_match_index = self.find_majority_match_index();
        if majority_match_index > self.commit_index {
            self.commit_log(majority_match_index);
            if let ClusterConfiguration::Joint {
                index: config_index,
                ..
            } = self.log.cluster_configuration()
            {
                if majority_match_index >= config_index {
                    self.finish_reconfiguration(sender_id);
                }
            }
        }
        if let Some(pending_reconfiguration_ids) =
            self.pending_reconfiguration_ids.take()
        {
            self.start_reconfiguration_if_ready(
                pending_reconfiguration_ids,
                Some(sender_id),
            );
        }
        self.send_more_log_entries_to_peer(sender_id, next_log_index - 1);
    }

    fn process_command(
        &mut self,
        command: Command<S, T>,
    ) where
        S: 'static + Clone + Debug + Send,
        T: 'static + Clone + Debug + Send,
    {
        match command {
            Command::AddCommands(commands) => {
                self.process_add_commands(commands)
            },
            Command::ReceiveMessage {
                message,
                sender_id,
            } => self.process_receive_message(message, sender_id),
            Command::ReconfigureCluster(ids) => {
                self.process_reconfigure_cluster(ids)
            },
            Command::ReconfigureServer(configuration) => {
                self.process_reconfigure_server(configuration)
            },
            #[cfg(test)]
            Command::Synchronize(received) => {
                if self.cancellations_pending == 0 {
                    trace!(
                        "No cancellations counted; completing synchronization"
                    );
                    let _ = received.send(());
                } else {
                    trace!(
                        "{} cancellations pending; awaiting synchronization",
                        self.cancellations_pending
                    );
                    self.synchronize_ack.replace(received);
                }
            },
        }
    }

    fn process_install_snapshot(
        &mut self,
        sender_id: usize,
        seq: usize,
        term: usize,
        last_included_index: usize,
        last_included_term: usize,
        snapshot: S,
    ) where
        S: Debug,
    {
        info!(
            "Received InstallSnapshot({};{}, {:?}) from {} for term {} (we are {:?} in term {})",
            last_included_index,
            last_included_term,
            snapshot,
            sender_id,
            term,
            self.election_state,
            self.persistent_storage.term()
        );
        let decision = self.decide_request_verdict(term);
        if decision == RequestDecision::AcceptAndUpdateTerm {
            self.persistent_storage.update(term, None);
        }
        if decision != RequestDecision::Reject {
            if self.election_state != ElectionState::Follower {
                self.become_follower();
            }
            self.cancel_election_timers();
            self.perform_operation_which_may_change_configuration(
                Self::install_snapshot,
                (last_included_index, last_included_term, snapshot),
            );
        }
        let message = Message {
            content: MessageContent::InstallSnapshotResponse,
            seq,
            term: self.persistent_storage.term(),
        };
        debug!("Sending InstallSnapshotResponse to {}", sender_id);
        let _ = self.event_sender.unbounded_send(Event::SendMessage {
            message,
            receiver_id: sender_id,
        });
    }

    fn process_install_snapshot_response(
        &mut self,
        sender_id: usize,
        term: usize,
        seq: usize,
    ) where
        S: 'static + Clone + Debug + Send,
        T: 'static + Clone + Debug + Send,
    {
        info!(
            "Received InstallSnapshotResponse from {} for term {} (we are {:?} in term {})",
            sender_id,
            term,
            self.election_state,
            self.persistent_storage.term()
        );
        if term < self.persistent_storage.term() {
            return;
        }
        if self
            .peers
            .get(&sender_id)
            .filter(|peer| peer.last_seq == seq)
            .is_none()
        {
            return;
        }
        self.cancel_retransmission(sender_id);
        if term > self.persistent_storage.term() {
            self.persistent_storage.update(term, None);
            self.become_follower();
        }
        if self.election_state != ElectionState::Leader {
            warn!(
                "Not processing InstallSnapshotResponse from {} because we are {:?}, not Leader",
                sender_id,
                self.election_state
            );
            return;
        }
        self.send_more_log_entries_to_peer(sender_id, self.log.base_index());
    }

    fn process_receive_message(
        &mut self,
        message: Message<S, T>,
        sender_id: usize,
    ) where
        S: 'static + Clone + Debug + Send,
        T: 'static + Clone + Debug + Send,
    {
        match message.content {
            MessageContent::RequestVoteResponse {
                vote_granted,
            } => self.process_request_vote_response(
                sender_id,
                vote_granted,
                message.term,
                message.seq,
            ),
            MessageContent::RequestVote {
                last_log_index,
                last_log_term,
            } => self.process_request_vote(
                sender_id,
                message.seq,
                message.term,
                last_log_index,
                last_log_term,
            ),
            MessageContent::AppendEntries(append_entries) => {
                self.process_append_entries(
                    sender_id,
                    message.seq,
                    message.term,
                    append_entries,
                );
            },
            MessageContent::AppendEntriesResponse {
                success,
                next_log_index,
            } => {
                self.process_append_entries_response(
                    success,
                    next_log_index,
                    sender_id,
                    message.term,
                    message.seq,
                );
            },
            MessageContent::InstallSnapshot {
                last_included_index,
                last_included_term,
                snapshot,
            } => {
                self.process_install_snapshot(
                    sender_id,
                    message.seq,
                    message.term,
                    last_included_index,
                    last_included_term,
                    snapshot,
                );
            },
            MessageContent::InstallSnapshotResponse => {
                self.process_install_snapshot_response(
                    sender_id,
                    message.term,
                    message.seq,
                );
            },
        }
    }

    fn process_reconfigure_cluster(
        &mut self,
        ids: HashSet<usize>,
    ) where
        S: 'static + Clone + Debug + Send,
        T: 'static + Clone + Debug + Send,
    {
        let current_configuration = self.log.cluster_configuration();
        match &current_configuration {
            ClusterConfiguration::Single(current_ids)
                if *current_ids == ids =>
            {
                if self.pending_reconfiguration_ids.take().is_some() {
                    self.cancel_reconfiguration();
                } else {
                    warn!(
                        "Ignoring reconfiguration request because configuration would not change ({:?})",
                        sorted(&ids)
                    );
                }
                return;
            }
            ClusterConfiguration::Joint {
                ..
            } => {
                warn!(
                    "Ignoring reconfiguration request because currently in joint configuration ({:?})",
                    current_configuration
                );
                return;
            },
            _ => (),
        }
        info!(
            "Reconfiguring from {:?} to {:?}",
            current_configuration,
            sorted(&ids)
        );
        let self_id = self.id;
        let leader_commit = self.commit_index;
        let prev_log_index = self.log.last_index();
        let prev_log_term = self.log.last_term();
        let term = self.persistent_storage.term();
        let event_sender = &self.event_sender;
        let rpc_timeout = self.configuration.rpc_timeout;
        #[cfg(test)]
        let scheduler = &self.scheduler;
        self.catch_up_index = prev_log_index;
        let old_ids = self
            .peers
            .keys()
            .copied()
            .chain(once(self.id))
            .collect::<HashSet<_>>();
        let new_peer_ids = ids
            .difference(&old_ids)
            .filter(|id| **id != self_id)
            .collect::<Vec<_>>();
        if new_peer_ids.is_empty() {
            self.start_reconfiguration(ids, None);
        } else {
            for new_peer_id in new_peer_ids {
                debug!("Sending heartbeat to new peer {}", *new_peer_id);
                let mut peer = Peer::default();
                peer.send_new_request(
                    MessageContent::AppendEntries(AppendEntriesContent {
                        leader_commit,
                        prev_log_index,
                        prev_log_term,
                        log: vec![],
                    }),
                    *new_peer_id,
                    term,
                    event_sender,
                    rpc_timeout,
                    #[cfg(test)]
                    scheduler,
                );
                self.peers.insert(*new_peer_id, peer);
            }
            self.pending_reconfiguration_ids = Some(ids);
        }
    }

    fn process_reconfigure_server(
        &mut self,
        configuration: ServerConfiguration,
    ) {
        info!("Changed server configuration");
        self.configuration = configuration;
    }

    fn process_request_vote(
        &mut self,
        sender_id: usize,
        seq: usize,
        term: usize,
        last_log_index: usize,
        last_log_term: usize,
    ) where
        T: Debug,
    {
        info!(
            "Received RequestVote({};{}) from {} for term {} (we are {:?} in term {})",
            last_log_index,
            last_log_term,
            sender_id,
            term,
            self.election_state,
            self.persistent_storage.term()
        );
        if !self.log.cluster_configuration().contains(self.id) {
            info!("Ignoring RequestVote because not a voting member");
            return;
        }
        if self.ignore_vote_requests {
            info!("Ignoring RequestVote because minimum election time has not yet elapsed");
            return;
        }
        let vote_granted = self.decide_vote_grant(
            sender_id,
            term,
            last_log_term,
            last_log_index,
        );
        let message = Message {
            content: MessageContent::RequestVoteResponse {
                vote_granted,
            },
            seq,
            term: self.persistent_storage.term(),
        };
        debug!(
            "Sending RequestVoteResponse ({}) message to {}",
            vote_granted, sender_id
        );
        let _ = self.event_sender.unbounded_send(Event::SendMessage {
            message,
            receiver_id: sender_id,
        });
    }

    fn process_request_vote_response(
        &mut self,
        sender_id: usize,
        vote_granted: bool,
        term: usize,
        seq: usize,
    ) where
        S: 'static + Clone + Debug + Send,
        T: 'static + Clone + Debug + Send,
    {
        info!(
            "Received vote {} from {} for term {} (we are {:?} in term {})",
            vote_granted,
            sender_id,
            term,
            self.election_state,
            self.persistent_storage.term()
        );
        if term < self.persistent_storage.term() {
            return;
        }
        if self
            .peers
            .get(&sender_id)
            .filter(|peer| peer.last_seq == seq)
            .is_none()
        {
            return;
        }
        self.cancel_retransmission(sender_id);
        if term > self.persistent_storage.term() {
            self.persistent_storage.update(term, None);
            self.become_follower();
        }
        if self.election_state != ElectionState::Candidate {
            debug!(
                "Received unexpected or extra vote {} from {}",
                vote_granted, sender_id
            );
            return;
        }
        if let Some(peer) = self.peers.get_mut(&sender_id) {
            peer.vote = Some(vote_granted);
        } else {
            return;
        }
        if match &self.log.cluster_configuration() {
            ClusterConfiguration::Joint {
                old_ids,
                new_ids,
                ..
            } => self.majority_vote(old_ids) && self.majority_vote(new_ids),
            ClusterConfiguration::Single(ids) => self.majority_vote(ids),
        } {
            self.become_leader();
            self.cancel_election_timers();
        }
    }

    fn retransmit(
        &mut self,
        peer_id: usize,
    ) where
        S: 'static + Clone + Debug + Send,
        T: 'static + Clone + Debug + Send,
    {
        if let Some(peer) = self.peers.get_mut(&peer_id) {
            if let Some(message) = peer.cancel_retransmission() {
                peer.send_request(
                    message,
                    peer_id,
                    &self.event_sender,
                    self.configuration.rpc_timeout,
                    #[cfg(test)]
                    &self.scheduler,
                )
            }
        }
    }

    fn send_more_log_entries_to_peer(
        &mut self,
        peer_id: usize,
        prev_log_index: usize,
    ) where
        S: 'static + Clone + Debug + Send,
        T: 'static + Clone + Debug + Send,
    {
        if let Some(peer) = self.peers.get_mut(&peer_id) {
            if prev_log_index < self.log.last_index() {
                let term = self.persistent_storage.term();
                let prev_log_term = match prev_log_index
                    .cmp(&self.log.base_index())
                {
                    Ordering::Less => None,
                    Ordering::Equal => Some(self.log.base_term()),
                    Ordering::Greater => self.log.entry_term(prev_log_index),
                };
                if let Some(prev_log_term) = prev_log_term {
                    let log = self.log.entries(prev_log_index);
                    info!(
                        "Sending {} entries from {} to {}",
                        log.len(),
                        prev_log_index,
                        peer_id
                    );
                    peer.send_new_request(
                        MessageContent::AppendEntries(AppendEntriesContent {
                            leader_commit: self.commit_index,
                            prev_log_index,
                            prev_log_term,
                            log,
                        }),
                        peer_id,
                        term,
                        &self.event_sender,
                        self.configuration.rpc_timeout,
                        #[cfg(test)]
                        &self.scheduler,
                    );
                } else {
                    info!("Sending snapshot to {}", peer_id);
                    peer.send_new_request(
                        MessageContent::InstallSnapshot {
                            last_included_index: self.log.base_index(),
                            last_included_term: self.log.base_term(),
                            snapshot: self.log.snapshot(),
                        },
                        peer_id,
                        term,
                        &self.event_sender,
                        self.configuration.install_snapshot_timeout,
                        #[cfg(test)]
                        &self.scheduler,
                    );
                }
            }
        }
    }

    fn send_new_message_broadcast(
        &mut self,
        content: MessageContent<S, T>,
    ) where
        S: 'static + Clone + Debug + Send,
        T: 'static + Clone + Debug + Send,
    {
        let term = self.persistent_storage.term();
        let event_sender = &self.event_sender;
        let rpc_timeout = self.configuration.rpc_timeout;
        #[cfg(test)]
        let scheduler = &self.scheduler;
        spread(&mut self.peers, content, |(&peer_id, peer), content| {
            peer.send_new_request(
                content,
                peer_id,
                term,
                event_sender,
                rpc_timeout,
                #[cfg(test)]
                scheduler,
            )
        });
    }

    pub async fn serve(
        mut self,
        command_receiver: CommandReceiver<S, T>,
    ) where
        S: 'static + Clone + Debug + Send,
        T: 'static + Clone + Debug + Send,
    {
        let mut command_receiver = Some(command_receiver);
        let mut futures = Vec::new();
        loop {
            if let Some(command_receiver) = command_receiver.take() {
                futures
                    .push(process_command_receiver(command_receiver).boxed());
            }
            if let Some(future) = self.upkeep_min_election_timeout_future() {
                futures.push(future);
            }
            if let Some(future) = self.upkeep_election_timeout_future() {
                futures.push(future);
            }
            if let Some(future) = self.upkeep_heartbeat_future() {
                futures.push(future);
            }
            futures.extend(self.take_retransmission_futures());
            let (work_item, _, futures_remaining) =
                future::select_all(futures).await;
            futures = futures_remaining;
            match work_item.content {
                #[cfg(test)]
                WorkItemContent::Abandoned(work_item) => {
                    trace!("Completed abandoned future: {}", work_item);
                },
                #[cfg(not(test))]
                WorkItemContent::Abandoned => {
                    trace!("Completed abandoned future");
                },
                #[cfg(test)]
                WorkItemContent::Cancelled(work_item) => {
                    trace!("Completed canceled future: {}", work_item);
                    self.count_cancellation();
                },
                #[cfg(not(test))]
                WorkItemContent::Cancelled => {
                    trace!("Completed canceled future");
                },
                WorkItemContent::Command {
                    command,
                    command_receiver: command_receiver_out,
                } => {
                    trace!("Command: {:?}", command);
                    self.process_command(command);
                    command_receiver.replace(command_receiver_out);
                },
                WorkItemContent::ElectionTimeout => {
                    info!("*** Election timeout! ***");
                    self.election_timeout();
                },
                WorkItemContent::Heartbeat => {
                    debug!("> Heartbeat <");
                    self.heartbeat();
                },
                WorkItemContent::MinElectionTimeout => {
                    debug!("minimum election timeout");
                    self.minimum_election_timeout();
                },
                WorkItemContent::RpcTimeout(peer_id) => {
                    info!("*** RPC timeout ({})! ***", peer_id);
                    self.retransmit(peer_id);
                },
                WorkItemContent::Stop => {
                    info!("Server worker stopping");
                    break;
                },
            }
            #[cfg(test)]
            if let Some(ack) = work_item.ack {
                let _ = ack.send(());
            }
        }
    }

    fn start_reconfiguration(
        &mut self,
        new_ids: HashSet<usize>,
        skip_peer_id: Option<usize>,
    ) where
        S: 'static + Clone + Debug + Send,
        T: 'static + Clone + Debug + Send,
    {
        self.append_reconfiguration_command(
            LogEntryCommand::StartReconfiguration(new_ids),
            skip_peer_id,
        );
    }

    fn start_reconfiguration_if_ready(
        &mut self,
        pending_reconfiguration_ids: HashSet<usize>,
        skip_peer_id: Option<usize>,
    ) where
        S: 'static + Clone + Debug + Send,
        T: 'static + Clone + Debug + Send,
    {
        let voting_peers = self
            .log
            .cluster_configuration()
            .peers(self.id)
            .copied()
            .collect::<HashSet<_>>();
        let all_peers_voting_or_caught_up =
            self.peers.iter().all(|(peer_id, peer)| {
                voting_peers.contains(peer_id)
                    || peer.match_index >= self.catch_up_index
            });
        if all_peers_voting_or_caught_up {
            self.start_reconfiguration(
                pending_reconfiguration_ids,
                skip_peer_id,
            );
        } else {
            self.pending_reconfiguration_ids
                .replace(pending_reconfiguration_ids);
        }
    }

    fn take_retransmission_futures(
        &mut self
    ) -> impl Iterator<Item = WorkItemFuture<S, T>> + '_ {
        self.peers
            .values_mut()
            .filter_map(|peer| peer.retransmission_future.take())
    }

    fn upkeep_election_timeout_future(&mut self) -> Option<WorkItemFuture<S, T>>
    where
        S: 'static + Clone + Debug + Send,
        T: 'static + Clone + Debug + Send,
    {
        if self.election_state == ElectionState::Leader
            || self.cancel_election_timeout.is_some()
            || !self.log.cluster_configuration().contains(self.id)
        {
            return None;
        }
        let timeout_duration = self.rng.gen_range(
            self.configuration.election_timeout.start,
            self.configuration.election_timeout.end,
        );
        debug!(
            "Setting election timer to {:?} ({:?})",
            timeout_duration, self.configuration.election_timeout
        );
        let (future, cancel_future) = make_cancellable_timeout_future(
            WorkItemContent::ElectionTimeout,
            timeout_duration,
            #[cfg(test)]
            ScheduledEvent::ElectionTimeout,
            #[cfg(test)]
            &self.scheduler,
        );
        self.cancel_election_timeout.replace(cancel_future);
        Some(future)
    }

    fn upkeep_heartbeat_future(&mut self) -> Option<WorkItemFuture<S, T>>
    where
        S: 'static + Clone + Debug + Send,
        T: 'static + Clone + Debug + Send,
    {
        if self.election_state != ElectionState::Leader
            || self.cancel_heartbeat.is_some()
            || self.peers.values().all(Peer::awaiting_response)
            || !self.log.cluster_configuration().contains(self.id)
        {
            return None;
        }
        let (future, cancel_future) = make_cancellable_timeout_future(
            WorkItemContent::Heartbeat,
            self.configuration.heartbeat_interval,
            #[cfg(test)]
            ScheduledEvent::Heartbeat,
            #[cfg(test)]
            &self.scheduler,
        );
        self.cancel_heartbeat.replace(cancel_future);
        Some(future)
    }

    fn upkeep_min_election_timeout_future(
        &mut self
    ) -> Option<WorkItemFuture<S, T>>
    where
        S: 'static + Clone + Debug + Send,
        T: 'static + Clone + Debug + Send,
    {
        if self.election_state == ElectionState::Leader
            || self.cancel_min_election_timeout.is_some()
            || self.cancel_election_timeout.is_some()
            || !self.log.cluster_configuration().contains(self.id)
        {
            return None;
        }
        debug!(
            "Setting minimum election timer to {:?}",
            self.configuration.election_timeout.start
        );
        self.ignore_vote_requests = true;
        let (future, cancel_future) = make_cancellable_timeout_future(
            WorkItemContent::MinElectionTimeout,
            self.configuration.election_timeout.start,
            #[cfg(test)]
            ScheduledEvent::MinElectionTimeout,
            #[cfg(test)]
            &self.scheduler,
        );
        self.cancel_min_election_timeout.replace(cancel_future);
        Some(future)
    }
}

async fn process_command_receiver<S, T>(
    command_receiver: CommandReceiver<S, T>
) -> WorkItem<S, T> {
    let (command, command_receiver) = command_receiver.into_future().await;
    let content = if let Some(command) = command {
        WorkItemContent::Command {
            command,
            command_receiver,
        }
    } else {
        info!("Server command channel closed");
        WorkItemContent::Stop
    };
    WorkItem {
        content,
        #[cfg(test)]
        ack: None,
    }
}
