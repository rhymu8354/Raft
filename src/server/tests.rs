mod common;
mod election_request;
mod election_response;
mod mock_log;
mod mock_persistent_storage;
mod reconfiguration;
mod replication_request;
mod replication_response;

use super::*;
use crate::{
    AppendEntriesContent,
    ClusterConfiguration,
    Log,
    LogEntry,
    LogEntryCommand,
    Message,
    MessageContent,
    PersistentStorage,
    ScheduledEvent,
    ScheduledEventReceiver,
    ScheduledEventWithCompleter,
    ServerConfiguration,
};
use futures::{
    channel::oneshot,
    FutureExt as _,
    SinkExt,
    StreamExt as _,
};
use maplit::hashset;
use mock_log::{
    BackEnd as MockLogBackEnd,
    MockLog,
};
use mock_persistent_storage::{
    BackEnd as MockPersistentStorageBackEnd,
    MockPersistentStorage,
};
use std::{
    borrow::Borrow,
    collections::{
        HashMap,
        HashSet,
    },
    iter::FromIterator,
    time::Duration,
};

struct AwaitElectionTimeoutArgs {
    last_log_term: usize,
    last_log_index: usize,
    term: usize,
}

struct VerifyVoteRequestArgs<'a> {
    message: &'a Message<ClusterConfiguration<usize>, (), usize>,
    expected_last_log_term: usize,
    expected_last_log_index: usize,
    expected_seq: Option<usize>,
    expected_term: usize,
}

struct CastVoteArgs {
    sender_id: usize,
    seq: usize,
    term: usize,
    vote: bool,
}

struct ReceiveVoteRequestArgs {
    sender_id: usize,
    last_log_term: usize,
    last_log_index: usize,
    seq: usize,
    term: usize,
}

struct AwaitVoteArgs {
    expect_state_change: bool,
    receiver_id: usize,
    seq: usize,
    term: usize,
    vote_granted: bool,
}

struct VerifyVoteArgs<'a> {
    seq: usize,
    term: usize,
    vote: bool,
    receiver_id: usize,
    expected: &'a AwaitVoteArgs,
}

#[derive(Clone)]
struct AwaitAppendEntriesArgs {
    term: usize,
    leader_commit: usize,
    prev_log_term: usize,
    prev_log_index: usize,
    log: Vec<LogEntry<(), usize>>,
}

struct VerifyAppendEntriesArgs<'a, 'b> {
    message: &'a Message<ClusterConfiguration<usize>, (), usize>,
    expected_leader_commit: usize,
    expected_prev_log_index: usize,
    expected_prev_log_term: usize,
    expected_log: &'b Vec<LogEntry<(), usize>>,
    expected_seq: Option<usize>,
    expected_term: usize,
}

struct AwaitAppendEntriesResponseArgs {
    commit_index: Option<usize>,
    expect_state_change: bool,
    success: bool,
    next_log_index: usize,
    receiver_id: usize,
    seq: usize,
    term: usize,
}

struct VerifyAppendEntriesResponseArgs<'a> {
    seq: usize,
    term: usize,
    success: bool,
    next_log_index: usize,
    receiver_id: usize,
    expected: &'a AwaitAppendEntriesResponseArgs,
}

struct Fixture {
    configuration: ServerConfiguration,
    id: usize,
    peer_ids: HashSet<usize>,

    // IMPORTANT: `server` must be listed before `scheduled_event_receiver`
    // because the mock scheduler produces futures which will
    // complete immediately if `scheduled_event_receiver` is dropped,
    // causing `server` to get stuck in a constant loop of timeout processing.
    server: Option<Server<ClusterConfiguration<usize>, (), usize>>,
    scheduled_event_receiver: Option<ScheduledEventReceiver<usize>>,
}

impl Fixture {
    fn expect_election_timer_registration_now(
        &mut self
    ) -> (Duration, oneshot::Sender<oneshot::Sender<()>>) {
        loop {
            let event_with_completer = self
                .scheduled_event_receiver
                .as_mut()
                .expect("no server mobilized")
                .next()
                .now_or_never()
                .flatten()
                .expect("no election timer registered");
            if let ScheduledEventWithCompleter {
                scheduled_event: ScheduledEvent::ElectionTimeout,
                duration,
                completer,
            } = event_with_completer
            {
                break (duration, completer);
            }
        }
    }

    fn expect_no_election_timer_registrations_now(&mut self) {
        while let Some(event_with_completer) = self
            .scheduled_event_receiver
            .as_mut()
            .expect("no server mobilized")
            .next()
            .now_or_never()
            .flatten()
        {
            assert!(
                !matches!(event_with_completer, ScheduledEventWithCompleter {
                    scheduled_event: ScheduledEvent::ElectionTimeout,
                    ..
                }),
                "Unexpected election timer registered"
            );
        }
    }

    async fn expect_no_election_timer_registrations(&mut self) {
        self.synchronize().await;
        self.expect_no_election_timer_registrations_now();
    }

    fn expect_election_timer_registrations_now(
        &mut self,
        num_timers_to_await: usize,
    ) -> (Duration, oneshot::Sender<oneshot::Sender<()>>) {
        for _ in 0..num_timers_to_await - 1 {
            self.expect_election_timer_registration_now();
        }
        self.expect_election_timer_registration_now()
    }

    async fn expect_election_timer_registrations(
        &mut self,
        num_timers_to_await: usize,
    ) -> (Duration, oneshot::Sender<oneshot::Sender<()>>) {
        self.synchronize().await;
        let registration =
            self.expect_election_timer_registrations_now(num_timers_to_await);
        self.expect_no_election_timer_registrations_now();
        registration
    }

    fn expect_min_election_timer_registration_now(
        &mut self
    ) -> (Duration, oneshot::Sender<oneshot::Sender<()>>) {
        loop {
            let event_with_completer = self
                .scheduled_event_receiver
                .as_mut()
                .expect("no server mobilized")
                .next()
                .now_or_never()
                .flatten()
                .expect("no minimum election timer registered");
            if let ScheduledEventWithCompleter {
                scheduled_event: ScheduledEvent::MinElectionTimeout,
                duration,
                completer,
            } = event_with_completer
            {
                break (duration, completer);
            }
        }
    }

    fn expect_no_min_election_timer_registrations_now(&mut self) {
        while let Some(event_with_completer) = self
            .scheduled_event_receiver
            .as_mut()
            .expect("no server mobilized")
            .next()
            .now_or_never()
            .flatten()
        {
            assert!(!matches!(
                event_with_completer,
                ScheduledEventWithCompleter {
                    scheduled_event: ScheduledEvent::MinElectionTimeout,
                    ..
                }
            ));
        }
    }

    async fn expect_no_min_election_timer_registrations(&mut self) {
        self.synchronize().await;
        self.expect_no_min_election_timer_registrations_now();
    }

    fn expect_min_election_timer_registrations_now(
        &mut self,
        num_timers_to_await: usize,
    ) -> (Duration, oneshot::Sender<oneshot::Sender<()>>) {
        for _ in 0..num_timers_to_await - 1 {
            self.expect_min_election_timer_registration_now();
        }
        self.expect_min_election_timer_registration_now()
    }

    async fn expect_min_election_timer_registrations(
        &mut self,
        num_timers_to_await: usize,
    ) -> (Duration, oneshot::Sender<oneshot::Sender<()>>) {
        self.synchronize().await;
        let registration = self
            .expect_min_election_timer_registrations_now(num_timers_to_await);
        self.expect_no_min_election_timer_registrations_now();
        registration
    }

    async fn trigger_min_election_timeout(&mut self) {
        let (duration, timeout) =
            self.expect_min_election_timer_registrations(1).await;
        assert_eq!(self.configuration.election_timeout.start, duration);
        let (sender, receiver) = oneshot::channel();
        timeout
            .send(sender)
            .expect("server dropped minimum election timeout future");
        receiver
            .await
            .expect("server dropped minimum election timeout acknowledgment");
    }

    async fn trigger_election_timeout(&mut self) {
        let (election_timeout_duration, election_timeout_completer) =
            self.expect_election_timer_registrations(1).await;
        assert!(
            self.configuration
                .election_timeout
                .contains(&election_timeout_duration),
            "election timeout duration {:?} is not within {:?}",
            election_timeout_duration,
            self.configuration.election_timeout
        );
        let (sender, receiver) = oneshot::channel();
        election_timeout_completer
            .send(sender)
            .expect("server dropped election timeout future");
        receiver.await.expect("server dropped election timeout acknowledgment");
    }

    fn expect_no_heartbeat_timer_registrations_now(&mut self) {
        while let Some(event_with_completer) = self
            .scheduled_event_receiver
            .as_mut()
            .expect("no server mobilized")
            .next()
            .now_or_never()
            .flatten()
        {
            assert!(
                !matches!(event_with_completer, ScheduledEventWithCompleter {
                    scheduled_event: ScheduledEvent::Heartbeat,
                    ..
                }),
                "server registered unexpected heartbeat timer"
            );
        }
    }

    fn expect_heartbeat_timer_registration_now(
        &mut self
    ) -> (Duration, oneshot::Sender<oneshot::Sender<()>>) {
        loop {
            let event_with_completer = self
                .scheduled_event_receiver
                .as_mut()
                .expect("no server mobilized")
                .next()
                .now_or_never()
                .flatten()
                .expect("no heartbeat timer registered");
            if let ScheduledEventWithCompleter {
                scheduled_event: ScheduledEvent::Heartbeat,
                duration,
                completer,
            } = event_with_completer
            {
                break (duration, completer);
            }
        }
    }

    fn expect_heartbeat_timer_registrations_now(
        &mut self,
        num_timers_to_await: usize,
    ) -> (Duration, oneshot::Sender<oneshot::Sender<()>>) {
        for _ in 0..num_timers_to_await - 1 {
            self.expect_heartbeat_timer_registration_now();
        }
        self.expect_heartbeat_timer_registration_now()
    }

    async fn expect_heartbeat_timer_registrations(
        &mut self,
        num_timers_to_await: usize,
    ) -> (Duration, oneshot::Sender<oneshot::Sender<()>>) {
        self.synchronize().await;
        let registration =
            self.expect_heartbeat_timer_registrations_now(num_timers_to_await);
        self.expect_no_heartbeat_timer_registrations_now();
        registration
    }

    async fn trigger_heartbeat_timeout(&mut self) {
        let (heartbeat_timeout_duration, heartbeat_timeout_completer) =
            self.expect_heartbeat_timer_registrations(1).await;
        assert_eq!(
            self.configuration.heartbeat_interval,
            heartbeat_timeout_duration
        );
        let (sender, receiver) = oneshot::channel();
        heartbeat_timeout_completer
            .send(sender)
            .expect("server dropped heartbeat future");
        receiver
            .await
            .expect("server dropped heartbeat timeout acknowledgment");
    }

    fn is_verified_vote_request(args: &VerifyVoteRequestArgs) -> bool {
        if let MessageContent::RequestVote {
            last_log_index,
            last_log_term,
        } = args.message.content
        {
            assert_eq!(
                last_log_term, args.expected_last_log_term,
                "wrong last_log_term in vote request (was {}, should be {})",
                last_log_term, args.expected_last_log_term
            );
            assert_eq!(
                last_log_index, args.expected_last_log_index,
                "wrong last_log_index in vote request (was {}, should be {})",
                last_log_index, args.expected_last_log_index
            );
            assert_eq!(
                args.message.term, args.expected_term,
                "wrong term in vote request (was {}, should be {})",
                args.message.term, args.expected_term
            );
            if let Some(expected_seq) = args.expected_seq {
                assert_eq!(
                    args.message.seq, expected_seq,
                    "wrong sequence number in vote request (was {}, should be {})",
                    args.message.seq, expected_seq
                );
            }
            true
        } else {
            false
        }
    }

    fn expect_vote_request_now(
        &mut self,
        expected_last_log_term: usize,
        expected_last_log_index: usize,
        expected_seq: Option<usize>,
        expected_term: usize,
    ) -> usize {
        loop {
            let event = self
                .server
                .as_mut()
                .expect("no server mobilized")
                .next()
                .now_or_never()
                .flatten()
                .expect("no vote request received");
            match event {
                Event::SendMessage {
                    message,
                    receiver_id,
                } => {
                    if Self::is_verified_vote_request(&VerifyVoteRequestArgs {
                        message: &message,
                        expected_last_log_term,
                        expected_last_log_index,
                        expected_seq,
                        expected_term,
                    }) {
                        break receiver_id;
                    }
                },
                Event::ElectionStateChange {
                    election_state,
                    term,
                    voted_for,
                } => {
                    assert_eq!(ElectionState::Candidate, election_state);
                    assert_eq!(
                        term,
                        expected_term,
                        "wrong term in election state change (was {}, should be {})",
                        term,
                        expected_term
                    );
                    assert!(
                        matches!(voted_for, Some(id) if id == self.id),
                        "server voted for {:?}, not itself ({})",
                        voted_for,
                        self.id
                    );
                },
                Event::LogCommitted(_) => {
                    panic!("Unexpectedly committed log entries")
                },
                Event::Reconfiguration(_) => {
                    panic!("Unexpected reconfiguration")
                },
                Event::AddNonVotingMembers(_) => {
                    panic!("Unexpected non-voting members added")
                },
                Event::DropNonVotingMembers => {
                    panic!("Unexpected non-voting members removed")
                },
                Event::LeadershipChange(_) => {
                    panic!("Unexpected leadership change")
                },
            }
        }
    }

    async fn expect_vote_request(
        &mut self,
        expected_last_log_term: usize,
        expected_last_log_index: usize,
        expected_seq: Option<usize>,
        expected_term: usize,
    ) -> usize {
        self.synchronize().await;
        self.expect_vote_request_now(
            expected_last_log_term,
            expected_last_log_index,
            expected_seq,
            expected_term,
        )
    }

    async fn expect_server_to_start_election(
        &mut self,
        args: AwaitElectionTimeoutArgs,
    ) {
        self.expect_leadership_change(None).await;
        let mut awaiting_vote_requests = self.peer_ids.clone();
        while !awaiting_vote_requests.is_empty() {
            let receiver_id = self
                .expect_vote_request(
                    args.last_log_term,
                    args.last_log_index,
                    None,
                    args.term,
                )
                .await;
            assert!(
                awaiting_vote_requests.remove(&receiver_id),
                "Unexpected vote request from {} sent",
                receiver_id
            );
        }
    }

    async fn expect_election(
        &mut self,
        args: AwaitElectionTimeoutArgs,
    ) {
        // Expect the server to register an election timeout event with a
        // duration within the configured range, and complete it.
        self.trigger_election_timeout().await;

        // Wait on server stream until we receive all the expected
        // vote requests.
        self.expect_server_to_start_election(args).await;
    }

    async fn expect_election_with_defaults(&mut self) {
        self.expect_election(AwaitElectionTimeoutArgs {
            last_log_term: 0,
            last_log_index: 0,
            term: 1,
        })
        .await
    }

    fn expect_assume_leadership_now(
        &mut self,
        expected_term: usize,
    ) {
        let mut got_election_state_change = false;
        loop {
            let event = self
                .server
                .as_mut()
                .expect("no server mobilized")
                .next()
                .now_or_never()
                .flatten()
                .expect("no leadership change");
            match event {
                Event::ElectionStateChange {
                    election_state: new_election_state,
                    term,
                    voted_for,
                } => {
                    assert_eq!(ElectionState::Leader, new_election_state);
                    assert_eq!(
                        term,
                        expected_term,
                        "wrong term in election state change (was {}, should be {})",
                        term,
                        expected_term
                    );
                    assert!(
                        matches!(voted_for, Some(id) if id == self.id),
                        "server voted for {:?}, not itself ({})",
                        voted_for,
                        self.id
                    );
                    got_election_state_change = true;
                },
                Event::LeadershipChange(leader_id) => {
                    assert!(
                        matches!(leader_id, Some(id) if id == self.id),
                        "new leader is {:?}, not self ({})",
                        leader_id,
                        self.id
                    );
                    break;
                },
                _ => {},
            }
        }
        assert!(got_election_state_change);
    }

    async fn expect_assume_leadership(
        &mut self,
        term: usize,
    ) {
        self.synchronize().await;
        self.expect_assume_leadership_now(term);
    }

    fn expect_leadership_change_now(
        &mut self,
        expected_leader_id: Option<usize>,
    ) {
        loop {
            let event = self
                .server
                .as_mut()
                .expect("no server mobilized")
                .next()
                .now_or_never()
                .flatten()
                .expect("no leadership change");
            if let Event::LeadershipChange(leader_id) = event {
                assert!(
                    matches!(leader_id, id if id == expected_leader_id),
                    "new leader is {:?}, not expected ({:?})",
                    leader_id,
                    expected_leader_id
                );
                break;
            }
        }
    }

    async fn expect_leadership_change(
        &mut self,
        leader_id: Option<usize>,
    ) {
        self.synchronize().await;
        self.expect_leadership_change_now(leader_id);
    }

    fn expect_no_leadership_change_now(&mut self) {
        while let Some(event) = self
            .server
            .as_mut()
            .expect("no server mobilized")
            .next()
            .now_or_never()
            .flatten()
        {
            if let Event::LeadershipChange(leader_id) = event {
                panic!("unexpected leadership change to {:?}", leader_id);
            }
        }
    }

    async fn expect_no_leadership_change(&mut self) {
        self.synchronize().await;
        self.expect_no_leadership_change_now();
    }

    fn verify_vote(args: &VerifyVoteArgs) {
        assert_eq!(
            args.vote, args.expected.vote_granted,
            "unexpected vote (was {}, should be {})",
            args.vote, args.expected.vote_granted
        );
        assert_eq!(
            args.term, args.expected.term,
            "wrong term in vote (was {}, should be {})",
            args.term, args.expected.term
        );
        assert_eq!(
            args.seq, args.expected.seq,
            "wrong sequence number in vote (was {}, should be {})",
            args.seq, args.expected.seq
        );
        assert_eq!(
            args.receiver_id, args.expected.receiver_id,
            "vote sent to wrong receiver (was {}, should be {})",
            args.receiver_id, args.expected.receiver_id
        );
    }

    fn is_verified_vote(
        message: &Message<ClusterConfiguration<usize>, (), usize>,
        receiver_id: usize,
        args: &AwaitVoteArgs,
    ) -> bool {
        if let MessageContent::RequestVoteResponse {
            vote_granted,
        } = message.content
        {
            Self::verify_vote(&VerifyVoteArgs {
                seq: message.seq,
                term: message.term,
                vote: vote_granted,
                receiver_id,
                expected: args,
            });
            true
        } else {
            false
        }
    }

    fn expect_vote_now(
        &mut self,
        args: &AwaitVoteArgs,
    ) {
        let mut state_changed = false;
        loop {
            let event = self
                .server
                .as_mut()
                .expect("no server mobilized")
                .next()
                .now_or_never()
                .flatten()
                .expect("no vote sent");
            match event {
                Event::SendMessage {
                    message,
                    receiver_id,
                } => {
                    if Self::is_verified_vote(&message, receiver_id, &args) {
                        break;
                    }
                },
                Event::ElectionStateChange {
                    election_state,
                    term,
                    voted_for,
                } => {
                    state_changed = true;
                    assert_eq!(election_state, ElectionState::Follower);
                    assert_eq!(
                        term, args.term,
                        "wrong term in election state change (was {}, should be {})",
                        term, args.term
                    );
                    assert_eq!(
                        voted_for,
                        Some(args.receiver_id),
                        "server vote was {:?}, while we expected {:?}",
                        voted_for,
                        Some(args.receiver_id)
                    );
                },
                Event::LogCommitted(_) => {
                    panic!("Unexpectedly committed log entries")
                },
                Event::Reconfiguration(_) => {
                    panic!("Unexpected reconfiguration")
                },
                Event::AddNonVotingMembers(_) => {
                    panic!("Unexpected non-voting members added")
                },
                Event::DropNonVotingMembers => {
                    panic!("Unexpected non-voting members removed")
                },
                Event::LeadershipChange(_) => {
                    panic!("Unexpected leadership change")
                },
            }
        }
        if args.expect_state_change {
            assert!(state_changed, "server did not change election state");
        }
    }

    async fn expect_vote(
        &mut self,
        args: &AwaitVoteArgs,
    ) {
        self.synchronize().await;
        self.expect_vote_now(args);
    }

    fn expect_no_vote_now(&mut self) {
        while let Some(event) = self
            .server
            .as_mut()
            .expect("no server mobilized")
            .next()
            .now_or_never()
            .flatten()
        {
            match event {
                Event::SendMessage {
                    message,
                    receiver_id,
                } => {
                    if let MessageContent::RequestVoteResponse {
                        vote_granted,
                    } = message.content
                    {
                        panic!(
                            "unexpected vote ({:?}) sent to {}",
                            vote_granted, receiver_id
                        );
                    }
                },
                Event::ElectionStateChange {
                    election_state,
                    ..
                } => {
                    panic!(
                        "unexpected election state change to {:?}",
                        election_state
                    );
                },
                Event::LogCommitted(_) => {
                    panic!("Unexpectedly committed log entries")
                },
                Event::Reconfiguration(_) => {
                    panic!("Unexpected reconfiguration")
                },
                Event::AddNonVotingMembers(_) => {
                    panic!("Unexpected non-voting members added")
                },
                Event::DropNonVotingMembers => {
                    panic!("Unexpected non-voting members removed")
                },
                Event::LeadershipChange(_) => {
                    panic!("Unexpected leadership change")
                },
            }
        }
    }

    async fn expect_no_vote(&mut self) {
        self.synchronize().await;
        self.expect_no_vote_now();
    }

    fn expect_election_state_change_now(
        &mut self,
        election_state: ElectionState,
        expected_term: usize,
    ) {
        while let Some(event) = self
            .server
            .as_mut()
            .expect("no server mobilized")
            .next()
            .now_or_never()
        {
            if let Event::ElectionStateChange {
                election_state: new_election_state,
                term,
                ..
            } = event.expect("unexpected end of server events")
            {
                assert_eq!(election_state, new_election_state);
                assert_eq!(
                    term,
                    expected_term,
                    "wrong term in election state change (was {}, should be {})",
                    term,
                    expected_term
                );
                return;
            }
        }
        panic!("server did not change election state")
    }

    async fn expect_election_state_change(
        &mut self,
        election_state: ElectionState,
        expected_term: usize,
    ) {
        self.synchronize().await;
        if let ElectionState::Leader = election_state {
            self.expect_assume_leadership_now(expected_term);
        } else {
            self.expect_election_state_change_now(
                election_state,
                expected_term,
            );
        }
    }

    fn expect_no_election_state_changes_now(&mut self) {
        while let Some(event) = self
            .server
            .as_mut()
            .expect("no server mobilized")
            .next()
            .now_or_never()
            .flatten()
        {
            if let Event::ElectionStateChange {
                election_state,
                ..
            } = event
            {
                panic!(
                    "unexpected election state change to {:?}",
                    election_state
                );
            }
        }
    }

    async fn expect_no_election_state_changes(&mut self) {
        self.synchronize().await;
        self.expect_no_election_state_changes_now();
    }

    fn expect_commit_now(
        &mut self,
        expected_index: usize,
    ) {
        while let Some(event) = self
            .server
            .as_mut()
            .expect("no server mobilized")
            .next()
            .now_or_never()
        {
            if let Event::LogCommitted(index) =
                event.expect("unexpected end of server events")
            {
                assert_eq!(
                    expected_index, index,
                    "wrong commit index (expected {}, got {})",
                    expected_index, index
                );
                return;
            }
        }
        panic!("server did not commit log")
    }

    async fn expect_commit(
        &mut self,
        expected_index: usize,
    ) {
        self.synchronize().await;
        self.expect_commit_now(expected_index);
    }

    fn expect_no_commit_now(&mut self) {
        while let Some(event) = self
            .server
            .as_mut()
            .expect("no server mobilized")
            .next()
            .now_or_never()
            .flatten()
        {
            if let Event::LogCommitted(index) = event {
                panic!("unexpected commit to {:?}", index);
            }
        }
    }

    async fn expect_no_commit(&mut self) {
        self.synchronize().await;
        self.expect_no_commit_now();
    }

    fn expect_reconfiguration_now(
        &mut self,
        expected_configuration: &ClusterConfiguration<usize>,
    ) {
        while let Some(event) = self
            .server
            .as_mut()
            .expect("no server mobilized")
            .next()
            .now_or_never()
        {
            if let Event::Reconfiguration(configuration) =
                event.expect("unexpected end of server events")
            {
                assert_eq!(
                    *expected_configuration, configuration,
                    "wrong configuration (expected {:?}, got {:?})",
                    *expected_configuration, configuration
                );
                return;
            }
        }
        panic!("server did not reconfigure")
    }

    async fn expect_reconfiguration(
        &mut self,
        expected_configuration: &ClusterConfiguration<usize>,
    ) {
        self.synchronize().await;
        self.expect_reconfiguration_now(expected_configuration);
    }

    fn expect_no_reconfiguration_now(&mut self) {
        while let Some(event) = self
            .server
            .as_mut()
            .expect("no server mobilized")
            .next()
            .now_or_never()
            .flatten()
        {
            if let Event::Reconfiguration(ids) = event {
                panic!("unexpected reconfiguration to {:?}", ids);
            }
        }
    }

    async fn expect_no_reconfiguration(&mut self) {
        self.synchronize().await;
        self.expect_no_reconfiguration_now();
    }

    fn expect_non_voting_members_added_now(
        &mut self,
        expected_ids: &HashSet<usize>,
    ) {
        while let Some(event) = self
            .server
            .as_mut()
            .expect("no server mobilized")
            .next()
            .now_or_never()
        {
            if let Event::AddNonVotingMembers(ids) =
                event.expect("unexpected end of server events")
            {
                assert_eq!(
                    *expected_ids, ids,
                    "wrong non-voting members added (expected {:?}, got {:?})",
                    *expected_ids, ids
                );
                return;
            }
        }
        panic!("server did not add non-voting members")
    }

    async fn expect_non_voting_members_added(
        &mut self,
        expected_ids: &HashSet<usize>,
    ) {
        self.synchronize().await;
        self.expect_non_voting_members_added_now(expected_ids);
    }

    fn expect_non_voting_members_dropped_now(&mut self) {
        while let Some(event) = self
            .server
            .as_mut()
            .expect("no server mobilized")
            .next()
            .now_or_never()
        {
            if let Event::DropNonVotingMembers =
                event.expect("unexpected end of server events")
            {
                return;
            }
        }
        panic!("server did not drop non-voting members")
    }

    async fn expect_non_voting_members_dropped(&mut self) {
        self.synchronize().await;
        self.expect_non_voting_members_dropped_now();
    }

    async fn cast_vote(
        &mut self,
        args: CastVoteArgs,
    ) {
        cast_vote(self.server.as_mut().expect("no server mobilized"), args)
            .await;
    }

    async fn receive_vote_request(
        &mut self,
        args: ReceiveVoteRequestArgs,
    ) {
        receive_vote_request(
            self.server.as_mut().expect("no server mobilized"),
            args,
        )
        .await;
    }

    async fn cast_votes(
        &mut self,
        seq: usize,
        term: usize,
    ) {
        for &id in &self.peer_ids {
            cast_vote(
                self.server.as_mut().expect("no server mobilized"),
                CastVoteArgs {
                    sender_id: id,
                    seq,
                    term,
                    vote: true,
                },
            )
            .await;
        }
    }

    fn expect_retransmission_timer_registration_now(
        &mut self,
        expected_receiver_id: usize,
    ) -> (Duration, oneshot::Sender<oneshot::Sender<()>>) {
        loop {
            let event_with_completer = self
                .scheduled_event_receiver
                .as_mut()
                .expect("no server mobilized")
                .next()
                .now_or_never()
                .flatten()
                .expect("no retransmit timer registered");
            if let ScheduledEventWithCompleter {
                scheduled_event: ScheduledEvent::Retransmit(peer_id),
                duration,
                completer,
            } = event_with_completer
            {
                if peer_id == expected_receiver_id {
                    return (duration, completer);
                }
            }
        }
    }

    async fn expect_retransmission_timer_registration(
        &mut self,
        expected_receiver_id: usize,
    ) -> (Duration, oneshot::Sender<oneshot::Sender<()>>) {
        self.synchronize().await;
        self.expect_retransmission_timer_registration_now(expected_receiver_id)
    }

    fn expect_retransmission_timer_registrations_now<T>(
        &mut self,
        expected_receiver_ids: T,
    ) -> HashMap<usize, oneshot::Sender<oneshot::Sender<()>>>
    where
        T: IntoIterator<Item = usize>,
    {
        let mut expected_receiver_ids: HashSet<usize> =
            HashSet::from_iter(expected_receiver_ids);
        let mut completers = HashMap::new();
        while !expected_receiver_ids.is_empty() {
            let event_with_completer = self
                .scheduled_event_receiver
                .as_mut()
                .expect("no server mobilized")
                .next()
                .now_or_never()
                .flatten()
                .expect("no retransmit timer registered");
            if let ScheduledEventWithCompleter {
                scheduled_event: ScheduledEvent::Retransmit(peer_id),
                completer,
                ..
            } = event_with_completer
            {
                if expected_receiver_ids.remove(&peer_id) {
                    completers.insert(peer_id, completer);
                }
            }
        }
        completers
    }

    fn expect_message_now(
        &mut self,
        expected_receiver_id: usize,
    ) -> Message<ClusterConfiguration<usize>, (), usize> {
        loop {
            let event = self
                .server
                .as_mut()
                .expect("no server mobilized")
                .next()
                .now_or_never()
                .flatten()
                .expect("no message sent");
            match event {
                Event::SendMessage {
                    message,
                    receiver_id,
                } => {
                    if receiver_id == expected_receiver_id {
                        return message;
                    }
                },
                Event::ElectionStateChange {
                    election_state,
                    ..
                } => {
                    panic!(
                        "Unexpected state transition to {:?}",
                        election_state
                    );
                },
                Event::LogCommitted(_) => {
                    panic!("Unexpectedly committed log entries")
                },
                Event::Reconfiguration(_) => {
                    panic!("Unexpected reconfiguration")
                },
                Event::AddNonVotingMembers(_) => {
                    panic!("Unexpected non-voting members added")
                },
                Event::DropNonVotingMembers => {
                    panic!("Unexpected non-voting members removed")
                },
                Event::LeadershipChange(_) => {
                    panic!("Unexpected leadership change")
                },
            }
        }
    }

    async fn expect_message(
        &mut self,
        receiver_id: usize,
    ) -> Message<ClusterConfiguration<usize>, (), usize> {
        self.synchronize().await;
        self.expect_message_now(receiver_id)
    }

    fn expect_messages_now(
        &mut self,
        mut expected_receiver_ids: HashSet<usize>,
    ) -> HashMap<usize, Message<ClusterConfiguration<usize>, (), usize>> {
        let mut messages = HashMap::new();
        while !expected_receiver_ids.is_empty() {
            let event = self
                .server
                .as_mut()
                .expect("no server mobilized")
                .next()
                .now_or_never()
                .flatten()
                .unwrap_or_else(|| {
                    panic!("no messages sent to {:?}", expected_receiver_ids)
                });
            match event {
                Event::SendMessage {
                    message,
                    receiver_id,
                } => {
                    if expected_receiver_ids.remove(&receiver_id) {
                        messages.insert(receiver_id, message);
                    } else {
                        panic!("Unexpected message sent to {}", receiver_id);
                    }
                },
                Event::ElectionStateChange {
                    election_state,
                    ..
                } => {
                    panic!(
                        "Unexpected state transition to {:?}",
                        election_state
                    );
                },
                Event::LogCommitted(_) => {
                    panic!("Unexpectedly committed log entries")
                },
                Event::Reconfiguration(_) => {
                    panic!("Unexpected reconfiguration")
                },
                Event::AddNonVotingMembers(_) => {
                    panic!("Unexpected non-voting members added")
                },
                Event::DropNonVotingMembers => {
                    panic!("Unexpected non-voting members removed")
                },
                Event::LeadershipChange(_) => {
                    panic!("Unexpected leadership change")
                },
            }
        }
        messages
    }

    async fn expect_messages(
        &mut self,
        receiver_ids: HashSet<usize>,
    ) -> HashMap<usize, Message<ClusterConfiguration<usize>, (), usize>> {
        self.synchronize().await;
        self.expect_messages_now(receiver_ids)
    }

    fn expect_no_messages_now(&mut self) {
        while let Some(event) = self
            .server
            .as_mut()
            .expect("no server mobilized")
            .next()
            .now_or_never()
            .flatten()
        {
            match event {
                Event::SendMessage {
                    message,
                    receiver_id,
                } => {
                    panic!(
                        "Unexpectedly sent {:?} to {}",
                        message, receiver_id
                    );
                },
                Event::ElectionStateChange {
                    election_state,
                    ..
                } => {
                    panic!(
                        "Unexpected state transition to {:?}",
                        election_state
                    );
                },
                Event::LogCommitted(_) => {
                    panic!("Unexpectedly committed log entries")
                },
                Event::Reconfiguration(_) => {
                    panic!("Unexpected reconfiguration")
                },
                Event::AddNonVotingMembers(_) => {
                    panic!("Unexpected non-voting members added")
                },
                Event::DropNonVotingMembers => {
                    panic!("Unexpected non-voting members removed")
                },
                Event::LeadershipChange(_) => {
                    panic!("Unexpected leadership change")
                },
            }
        }
    }

    async fn expect_no_messages(&mut self) {
        self.synchronize().await;
        self.expect_no_messages_now();
    }

    async fn expect_retransmission(
        &mut self,
        expected_receiver_id: usize,
    ) -> Message<ClusterConfiguration<usize>, (), usize> {
        let (retransmit_duration, completer) = self
            .expect_retransmission_timer_registration(expected_receiver_id)
            .await;
        assert_eq!(self.configuration.rpc_timeout, retransmit_duration);
        let (sender, receiver) = oneshot::channel();
        completer.send(sender).expect("server dropped retransmission future");
        receiver
            .await
            .expect("server dropped retransmission timeout acknowledgment");
        self.expect_message(expected_receiver_id).await
    }

    fn new() -> Self {
        Self {
            configuration: ServerConfiguration {
                election_timeout: Duration::from_millis(100)
                    ..Duration::from_millis(200),
                heartbeat_interval: Duration::from_millis(50),
                rpc_timeout: Duration::from_millis(10),
                install_snapshot_timeout: Duration::from_secs(10),
            },
            id: 5,
            peer_ids: HashSet::new(),
            scheduled_event_receiver: None,
            server: None,
        }
    }

    fn mobilize_server(&mut self) {
        let (mock_log, _mock_log_back_end) = MockLog::new();
        self.mobilize_server_with_log(Box::new(mock_log))
    }

    fn mobilize_server_with_log(
        &mut self,
        log: Box<dyn Log<ClusterConfiguration<usize>, usize, Command = ()>>,
    ) {
        let (mock_persistent_storage, _mock_persistent_storage_back_end) =
            MockPersistentStorage::new();
        self.mobilize_server_with_log_and_persistent_storage(
            log,
            Box::new(mock_persistent_storage),
        );
    }

    fn mobilize_server_with_log_and_persistent_storage(
        &mut self,
        log: Box<dyn Log<ClusterConfiguration<usize>, usize, Command = ()>>,
        persistent_storage: Box<dyn PersistentStorage<usize>>,
    ) {
        self.peer_ids = log.cluster_configuration().peers(self.id).collect();
        let mut server = Server::new(
            self.id,
            self.configuration.clone(),
            log,
            persistent_storage,
        );
        let scheduled_event_receiver =
            server.take_scheduled_event_receiver().unwrap();
        self.scheduled_event_receiver.replace(scheduled_event_receiver);
        self.server.replace(server);
    }

    fn mobilize_server_with_persistent_storage(
        &mut self,
        persistent_storage: Box<dyn PersistentStorage<usize>>,
    ) {
        let (mock_log, _mock_log_back_end) = MockLog::new();
        self.mobilize_server_with_log_and_persistent_storage(
            Box::new(mock_log),
            persistent_storage,
        );
    }

    fn is_verified_append_entries(args: &VerifyAppendEntriesArgs) -> bool {
        if let MessageContent::AppendEntries(AppendEntriesContent {
            leader_commit,
            prev_log_index,
            prev_log_term,
            log,
        }) = &args.message.content
        {
            assert_eq!(
                *leader_commit, args.expected_leader_commit,
                "wrong leader commit in append entries (was {}, should be {})",
                *leader_commit, args.expected_leader_commit
            );
            assert_eq!(
                *prev_log_index, args.expected_prev_log_index,
                "wrong previous log index in append entries (was {}, should be {})",
                *prev_log_index, args.expected_prev_log_index
            );
            assert_eq!(
                *prev_log_term, args.expected_prev_log_term,
                "wrong previous log term in append entries (was {}, should be {})",
                *prev_log_term, args.expected_prev_log_term
            );
            assert_eq!(
                log, args.expected_log,
                "wrong log in append entries (was {:?}, should be {:?})",
                log, args.expected_log
            );
            assert_eq!(
                args.message.term, args.expected_term,
                "wrong term in append entries (was {}, should be {})",
                args.message.term, args.expected_term
            );
            if let Some(expected_seq) = args.expected_seq {
                assert_eq!(
                    args.message.seq, expected_seq,
                    "wrong sequence number in append entries (was {}, should be {})",
                    args.message.seq, expected_seq
                );
            }
            true
        } else {
            false
        }
    }

    fn expect_append_entries_now(
        &mut self,
        args: &AwaitAppendEntriesArgs,
    ) -> usize {
        loop {
            let event = self
                .server
                .as_mut()
                .expect("no server mobilized")
                .next()
                .now_or_never()
                .flatten()
                .expect("no append entries sent");
            match event {
                Event::SendMessage {
                    message,
                    receiver_id,
                } => {
                    if Self::is_verified_append_entries(
                        &VerifyAppendEntriesArgs {
                            message: &message,
                            expected_leader_commit: args.leader_commit,
                            expected_prev_log_index: args.prev_log_index,
                            expected_prev_log_term: args.prev_log_term,
                            expected_log: &args.log,
                            expected_seq: None,
                            expected_term: args.term,
                        },
                    ) {
                        break receiver_id;
                    }
                },
                Event::ElectionStateChange {
                    election_state,
                    ..
                } => {
                    panic!(
                        "unexpected election state change to {:?}",
                        election_state
                    );
                },
                Event::LogCommitted(_) => {
                    panic!("Unexpectedly committed log entries")
                },
                Event::Reconfiguration(_) => {
                    panic!("Unexpected reconfiguration")
                },
                Event::AddNonVotingMembers(_) => {
                    panic!("Unexpected non-voting members added")
                },
                Event::DropNonVotingMembers => {
                    panic!("Unexpected non-voting members removed")
                },
                Event::LeadershipChange(_) => {
                    panic!("Unexpected leadership change")
                },
            }
        }
    }

    async fn expect_log_entries_broadcast(
        &mut self,
        args: AwaitAppendEntriesArgs,
    ) {
        self.synchronize().await;
        let mut recipients = self.peer_ids.clone();
        while !recipients.is_empty() {
            let recipient_id = self.expect_append_entries_now(&args);
            assert!(
                recipients.remove(&recipient_id),
                "Unexpected append entries sent to {}",
                recipient_id
            );
        }
    }

    fn verify_append_entries_response(args: &VerifyAppendEntriesResponseArgs) {
        assert_eq!(
            args.success, args.expected.success,
            "unexpected success flag (was {}, should be {})",
            args.success, args.expected.success
        );
        assert_eq!(
            args.next_log_index, args.expected.next_log_index,
            "unexpected next log index (was {}, should be {})",
            args.next_log_index, args.expected.next_log_index
        );
        assert_eq!(
            args.term, args.expected.term,
            "wrong term in append entries (was {}, should be {})",
            args.term, args.expected.term
        );
        assert_eq!(
            args.seq, args.expected.seq,
            "wrong sequence number in append entries (was {}, should be {})",
            args.seq, args.expected.seq
        );
        assert_eq!(
            args.receiver_id, args.expected.receiver_id,
            "append entries sent to wrong receiver (was {}, should be {})",
            args.receiver_id, args.expected.receiver_id
        );
    }

    fn is_verified_append_entries_response(
        message: &Message<ClusterConfiguration<usize>, (), usize>,
        receiver_id: usize,
        args: &AwaitAppendEntriesResponseArgs,
    ) -> bool {
        if let MessageContent::AppendEntriesResponse {
            success,
            next_log_index,
        } = message.content
        {
            Self::verify_append_entries_response(
                &VerifyAppendEntriesResponseArgs {
                    seq: message.seq,
                    term: message.term,
                    success,
                    next_log_index,
                    receiver_id,
                    expected: args,
                },
            );
            true
        } else {
            false
        }
    }

    fn expect_append_entries_response_now(
        &mut self,
        args: &AwaitAppendEntriesResponseArgs,
    ) {
        let mut state_changed = false;
        let mut log_committed = false;
        loop {
            let event = self
                .server
                .as_mut()
                .expect("no server mobilized")
                .next()
                .now_or_never()
                .flatten()
                .expect("no append entries response sent");
            match event {
                Event::SendMessage {
                    message,
                    receiver_id,
                } => {
                    if Self::is_verified_append_entries_response(
                        &message,
                        receiver_id,
                        &args,
                    ) {
                        break;
                    }
                },
                Event::ElectionStateChange {
                    election_state,
                    term,
                    ..
                } => {
                    state_changed = true;
                    assert_eq!(election_state, ElectionState::Follower);
                    assert_eq!(
                        term, args.term,
                        "wrong term in election state change (was {}, should be {})",
                        term, args.term
                    );
                },
                Event::LogCommitted(commit_index) => {
                    log_committed = true;
                    let expected_commit_index = args
                        .commit_index
                        .expect("Unexpectedly committed log entries");
                    assert_eq!(
                        commit_index, expected_commit_index,
                        "wrong log index committed (was {}, should be {})",
                        commit_index, expected_commit_index
                    );
                },
                Event::Reconfiguration(_) => {
                    panic!("Unexpected reconfiguration")
                },
                Event::AddNonVotingMembers(_) => {
                    panic!("Unexpected non-voting members added")
                },
                Event::DropNonVotingMembers => {
                    panic!("Unexpected non-voting members removed")
                },
                Event::LeadershipChange(_) => {
                    panic!("Unexpected leadership change")
                },
            }
        }
        if args.expect_state_change {
            assert!(state_changed, "server did not change election state");
        }
        if args.commit_index.is_some() {
            assert!(log_committed, "server did not commit the log");
        }
    }

    async fn expect_append_entries_response(
        &mut self,
        args: &AwaitAppendEntriesResponseArgs,
    ) {
        self.synchronize().await;
        self.expect_append_entries_response_now(args);
    }

    fn is_verified_install_snapshot_response(
        message: &Message<ClusterConfiguration<usize>, (), usize>,
        receiver_id: usize,
        expected_next_log_index: usize,
        expected_receiver_id: usize,
        expected_seq: usize,
        expected_term: usize,
    ) -> bool {
        if let MessageContent::InstallSnapshotResponse {
            next_log_index,
        } = message.content
        {
            assert_eq!(
                next_log_index, expected_next_log_index,
                "wrong term in install snapshot response (was {}, should be {})",
                next_log_index, expected_next_log_index
            );
            assert_eq!(
                message.term, expected_term,
                "wrong term in install snapshot response (was {}, should be {})",
                message.term, expected_term
            );
            assert_eq!(
                message.seq, expected_seq,
                "wrong sequence number in install snapshot response (was {}, should be {})",
                message.seq, expected_seq
            );
            assert_eq!(
                receiver_id, expected_receiver_id,
                "install snapshot response sent to wrong receiver (was {}, should be {})",
                receiver_id, expected_receiver_id
            );
            true
        } else {
            false
        }
    }

    fn expect_install_snapshot_response_now(
        &mut self,
        expected_next_log_index: usize,
        expected_receiver_id: usize,
        expected_seq: usize,
        expected_term: usize,
    ) {
        let mut log_committed = false;
        loop {
            let event = self
                .server
                .as_mut()
                .expect("no server mobilized")
                .next()
                .now_or_never()
                .flatten()
                .expect("no install snapshot response sent");
            match event {
                Event::SendMessage {
                    message,
                    receiver_id,
                } => {
                    if Self::is_verified_install_snapshot_response(
                        &message,
                        receiver_id,
                        expected_next_log_index,
                        expected_receiver_id,
                        expected_seq,
                        expected_term,
                    ) {
                        break;
                    }
                },
                Event::ElectionStateChange {
                    ..
                } => {
                    panic!("Unexpected election state change");
                },
                Event::LogCommitted(commit_index) => {
                    assert_eq!(
                        commit_index,
                        expected_next_log_index - 1,
                        "Expected commit to {}, got commit to {}",
                        expected_next_log_index - 1,
                        commit_index
                    );
                    log_committed = true;
                },
                Event::Reconfiguration(_) => {
                    panic!("Unexpected reconfiguration")
                },
                Event::AddNonVotingMembers(_) => {
                    panic!("Unexpected non-voting members added")
                },
                Event::DropNonVotingMembers => {
                    panic!("Unexpected non-voting members removed")
                },
                Event::LeadershipChange(_) => {
                    panic!("Unexpected leadership change")
                },
            }
        }
        assert!(log_committed, "Log not committed on snapshot install");
    }

    async fn expect_install_snapshot_response(
        &mut self,
        next_log_index: usize,
        receiver_id: usize,
        seq: usize,
        term: usize,
    ) {
        self.synchronize().await;
        self.expect_install_snapshot_response_now(
            next_log_index,
            receiver_id,
            seq,
            term,
        );
    }

    async fn send_server_message(
        &mut self,
        message: Message<ClusterConfiguration<usize>, (), usize>,
        sender_id: usize,
    ) {
        send_server_message(
            self.server.as_mut().expect("no server mobilized"),
            message,
            sender_id,
        )
        .await
    }

    async fn synchronize(&mut self) {
        synchronize(self.server.as_mut().expect("no server mobilized")).await;
    }
}

fn new_mock_log_with_non_defaults<T>(
    base_term: usize,
    base_index: usize,
    snapshot: T,
) -> (MockLog, MockLogBackEnd)
where
    T: Into<ClusterConfiguration<usize>>,
{
    let (mock_log, mock_log_back_end) = MockLog::new();
    {
        let mut log_shared = mock_log_back_end.shared.lock().unwrap();
        log_shared.base_term = base_term;
        log_shared.base_index = base_index;
        log_shared.last_term = base_term;
        log_shared.last_index = base_index;
        log_shared.snapshot = snapshot.into();
    }
    (mock_log, mock_log_back_end)
}

fn new_mock_persistent_storage_with_non_defaults(
    term: usize,
    voted_for: Option<usize>,
) -> (MockPersistentStorage, MockPersistentStorageBackEnd) {
    let (mock_persistent_storage, mock_persistent_storage_back_end) =
        MockPersistentStorage::new();
    {
        let mut persistent_storage_shared =
            mock_persistent_storage_back_end.shared.lock().unwrap();
        persistent_storage_shared.term = term;
        persistent_storage_shared.voted_for = voted_for;
    }
    (mock_persistent_storage, mock_persistent_storage_back_end)
}

fn verify_log<L, S>(
    mock_log_back_end: &MockLogBackEnd,
    base_term: usize,
    base_index: usize,
    entries: L,
    snapshot: S,
) where
    L: AsRef<[LogEntry<(), usize>]>,
    S: Borrow<ClusterConfiguration<usize>>,
{
    let log_shared = mock_log_back_end.shared.lock().unwrap();
    assert_eq!(
        base_term, log_shared.base_term,
        "log base term should be {} but is {}",
        base_term, log_shared.base_term
    );
    assert_eq!(
        base_index, log_shared.base_index,
        "log base index should be {} but is {}",
        base_index, log_shared.base_index
    );
    let entries = entries.as_ref();
    assert_eq!(
        entries, log_shared.entries,
        "log should be {:?} but is {:?}",
        entries, log_shared.entries
    );
    let snapshot = snapshot.borrow();
    assert_eq!(
        *snapshot, log_shared.snapshot,
        "snapshot should be {:?} but is {:?}",
        *snapshot, log_shared.snapshot
    );
}

fn verify_persistent_storage(
    mock_persistent_storage_back_end: &MockPersistentStorageBackEnd,
    term: usize,
    voted_for: Option<usize>,
) {
    let persistent_storage_shared =
        mock_persistent_storage_back_end.shared.lock().unwrap();
    assert_eq!(
        term, persistent_storage_shared.term,
        "term should be {} but is {}",
        term, persistent_storage_shared.term
    );
    assert_eq!(
        voted_for, persistent_storage_shared.voted_for,
        "should have voted {:?} but actually {:?}",
        voted_for, persistent_storage_shared.voted_for
    );
}

async fn send_server_message(
    server: &mut Server<ClusterConfiguration<usize>, (), usize>,
    message: Message<ClusterConfiguration<usize>, (), usize>,
    sender_id: usize,
) {
    server
        .send(Command::ReceiveMessage {
            message,
            sender_id,
        })
        .await
        .unwrap();
}

async fn synchronize(
    server: &mut Server<ClusterConfiguration<usize>, (), usize>
) {
    let (completed_sender, completed_receiver) = oneshot::channel();
    server.send(Command::Synchronize(completed_sender)).await.unwrap();
    let _ = completed_receiver.await;
}

async fn cast_vote(
    server: &mut Server<ClusterConfiguration<usize>, (), usize>,
    CastVoteArgs {
        sender_id,
        seq,
        term,
        vote,
    }: CastVoteArgs,
) {
    send_server_message(
        server,
        Message {
            content: MessageContent::RequestVoteResponse {
                vote_granted: vote,
            },
            seq,
            term,
        },
        sender_id,
    )
    .await;
}

async fn receive_vote_request(
    server: &mut Server<ClusterConfiguration<usize>, (), usize>,
    ReceiveVoteRequestArgs {
        sender_id,
        last_log_term,
        last_log_index,
        seq,
        term,
    }: ReceiveVoteRequestArgs,
) {
    send_server_message(
        server,
        Message {
            content: MessageContent::RequestVote {
                last_log_index,
                last_log_term,
            },
            seq,
            term,
        },
        sender_id,
    )
    .await;
}
