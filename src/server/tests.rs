mod common;
mod election_request;
mod election_response;
mod mock_log;
mod mock_persistent_storage;
mod replication_request;
mod replication_response;

use super::Server;
use crate::{
    AppendEntriesContent,
    Configuration,
    Log,
    LogEntry,
    LogEntryCustomCommand,
    Message,
    MessageContent,
    PersistentStorage,
    ScheduledEvent,
    ScheduledEventReceiver,
    ScheduledEventWithCompleter,
    ServerElectionState,
    ServerEvent,
    ServerMobilizeArgs,
    ServerSinkItem,
};
use futures::{
    channel::oneshot,
    FutureExt as _,
    SinkExt,
    StreamExt as _,
};
use maplit::hashset;
use mock_log::{
    MockLog,
    MockLogBackEnd,
};
use mock_persistent_storage::{
    MockPersistentStorage,
    MockPersistentStorageBackEnd,
};
use serde::{
    Deserialize,
    Serialize,
};
use serde_json::Value as JsonValue;
use std::{
    collections::{
        HashMap,
        HashSet,
    },
    iter::FromIterator,
    time::Duration,
};

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct DummyCommand {}

impl LogEntryCustomCommand for DummyCommand {
    fn command_type(&self) -> &'static str {
        "POGGERS"
    }

    fn to_json(&self) -> JsonValue {
        JsonValue::Null
    }

    fn from_json(_json: &JsonValue) -> Option<Self>
    where
        Self: Sized,
    {
        None
    }
}

struct AwaitElectionTimeoutArgs {
    last_log_term: usize,
    last_log_index: usize,
    term: usize,
}

struct VerifyVoteRequestArgs<'a> {
    message: &'a Message<DummyCommand>,
    expected_last_log_term: usize,
    expected_last_log_index: usize,
    expected_seq: Option<usize>,
    expected_term: usize,
}

struct AwaitAssumeLeadershipArgs {
    term: usize,
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
    log: Vec<LogEntry<DummyCommand>>,
}

struct VerifyAppendEntriesArgs<'a, 'b> {
    message: &'a Message<DummyCommand>,
    expected_leader_commit: usize,
    expected_prev_log_index: usize,
    expected_prev_log_term: usize,
    expected_log: &'b Vec<LogEntry<DummyCommand>>,
    expected_seq: Option<usize>,
    expected_term: usize,
}

struct AwaitAppendEntriesResponseArgs {
    commit_index: Option<usize>,
    expect_state_change: bool,
    match_index: usize,
    receiver_id: usize,
    seq: usize,
    term: usize,
}

struct VerifyAppendEntriesResponseArgs<'a> {
    seq: usize,
    term: usize,
    match_index: usize,
    receiver_id: usize,
    expected: &'a AwaitAppendEntriesResponseArgs,
}

struct Fixture {
    cluster: HashSet<usize>,
    configuration: Configuration,
    configured: bool,
    id: usize,

    // IMPORTANT: `server` must be listed before `scheduled_event_receiver`
    // because the mock scheduler produces futures which will
    // complete immediately if `scheduled_event_receiver` is dropped,
    // causing `server` to get stuck in a constant loop of timeout processing.
    server: Server<DummyCommand>,
    scheduled_event_receiver: ScheduledEventReceiver,
}

impl Fixture {
    fn expect_election_timer_registration_now(
        &mut self
    ) -> (Duration, oneshot::Sender<()>) {
        loop {
            let event_with_completer = self
                .scheduled_event_receiver
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
        while let Some(event_with_completer) =
            self.scheduled_event_receiver.next().now_or_never().flatten()
        {
            assert!(!matches!(
                event_with_completer,
                ScheduledEventWithCompleter {
                    scheduled_event: ScheduledEvent::ElectionTimeout,
                    ..
                }
            ));
        }
    }

    fn expect_election_timer_registrations_now(
        &mut self,
        num_timers_to_await: usize,
    ) -> (Duration, oneshot::Sender<()>) {
        for _ in 0..num_timers_to_await - 1 {
            self.expect_election_timer_registration_now();
        }
        self.expect_election_timer_registration_now()
    }

    async fn expect_election_timer_registrations(
        &mut self,
        num_timers_to_await: usize,
    ) -> (Duration, oneshot::Sender<()>) {
        self.synchronize().await;
        let registration =
            self.expect_election_timer_registrations_now(num_timers_to_await);
        self.expect_no_election_timer_registrations_now();
        registration
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
        election_timeout_completer
            .send(())
            .expect("server dropped election timeout future");
    }

    fn is_verified_vote_request(
        &self,
        args: VerifyVoteRequestArgs,
    ) -> bool {
        if let MessageContent::RequestVote {
            candidate_id,
            last_log_index,
            last_log_term,
        } = args.message.content
        {
            assert_eq!(
                candidate_id, self.id,
                "wrong candidate_id in vote request (was {}, should be {})",
                candidate_id, self.id
            );
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
                .next()
                .now_or_never()
                .flatten()
                .expect("no vote request received");
            match event {
                ServerEvent::SendMessage {
                    message,
                    receiver_id,
                } => {
                    if self.is_verified_vote_request(VerifyVoteRequestArgs {
                        message: &message,
                        expected_last_log_term,
                        expected_last_log_index,
                        expected_seq,
                        expected_term,
                    }) {
                        break receiver_id;
                    }
                },
                ServerEvent::ElectionStateChange {
                    election_state,
                    term,
                    voted_for,
                } => {
                    assert_eq!(ServerElectionState::Candidate, election_state);
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
                ServerEvent::LogCommitted(_) => {
                    panic!("Unexpectedly committed log entries")
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
        expected_last_log_term: usize,
        expected_last_log_index: usize,
        expected_term: usize,
    ) {
        let mut awaiting_vote_requests = self.cluster.clone();
        awaiting_vote_requests.remove(&self.id);
        while !awaiting_vote_requests.is_empty() {
            let receiver_id = self
                .expect_vote_request(
                    expected_last_log_term,
                    expected_last_log_index,
                    None,
                    expected_term,
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
        self.expect_server_to_start_election(
            args.last_log_term,
            args.last_log_index,
            args.term,
        )
        .await;
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
        args: AwaitAssumeLeadershipArgs,
    ) {
        loop {
            let event = self
                .server
                .next()
                .now_or_never()
                .flatten()
                .expect("no election state change");
            if let ServerEvent::ElectionStateChange {
                election_state: new_election_state,
                term,
                voted_for,
            } = event
            {
                assert_eq!(ServerElectionState::Leader, new_election_state);
                assert_eq!(
                    term,
                    args.term,
                    "wrong term in election state change (was {}, should be {})",
                    term,
                    args.term
                );
                assert!(
                    matches!(voted_for, Some(id) if id == self.id),
                    "server voted for {:?}, not itself ({})",
                    voted_for,
                    self.id
                );
                break;
            }
        }
    }

    async fn expect_assume_leadership(
        &mut self,
        args: AwaitAssumeLeadershipArgs,
    ) {
        self.synchronize().await;
        self.expect_assume_leadership_now(args);
    }

    fn verify_vote(
        &self,
        args: VerifyVoteArgs,
    ) {
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
        &self,
        message: &Message<DummyCommand>,
        receiver_id: usize,
        args: &AwaitVoteArgs,
    ) -> bool {
        if let MessageContent::RequestVoteResults {
            vote_granted,
        } = message.content
        {
            self.verify_vote(VerifyVoteArgs {
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
        args: AwaitVoteArgs,
    ) {
        let mut state_changed = false;
        loop {
            let event = self
                .server
                .next()
                .now_or_never()
                .flatten()
                .expect("no vote sent");
            match event {
                ServerEvent::SendMessage {
                    message,
                    receiver_id,
                } => {
                    if self.is_verified_vote(&message, receiver_id, &args) {
                        break;
                    }
                },
                ServerEvent::ElectionStateChange {
                    election_state,
                    term,
                    voted_for,
                } => {
                    state_changed = true;
                    assert_eq!(election_state, ServerElectionState::Follower);
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
                ServerEvent::LogCommitted(_) => {
                    panic!("Unexpectedly committed log entries")
                },
            }
        }
        if args.expect_state_change {
            assert!(state_changed, "server did not change election state");
        }
    }

    async fn expect_vote(
        &mut self,
        args: AwaitVoteArgs,
    ) {
        self.synchronize().await;
        self.expect_vote_now(args);
    }

    async fn expect_election_state_change_now(
        &mut self,
        election_state: ServerElectionState,
    ) {
        self.synchronize().await;
        while let Some(event) = self.server.next().now_or_never() {
            if let ServerEvent::ElectionStateChange {
                election_state: new_election_state,
                ..
            } = event.expect("unexpected end of server events")
            {
                assert_eq!(election_state, new_election_state);
                return;
            }
        }
        panic!("server did not change election state")
    }

    fn expect_no_election_state_changes_now(&mut self) {
        while let Some(event) = self.server.next().now_or_never().flatten() {
            if let ServerEvent::ElectionStateChange {
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

    async fn cast_vote(
        &mut self,
        args: CastVoteArgs,
    ) {
        cast_vote(&mut self.server, args).await;
    }

    async fn receive_vote_request(
        &mut self,
        args: ReceiveVoteRequestArgs,
    ) {
        receive_vote_request(&mut self.server, args).await;
    }

    async fn cast_votes(
        &mut self,
        seq: usize,
        term: usize,
    ) {
        for &id in self.cluster.iter() {
            if id == self.id {
                continue;
            }
            cast_vote(&mut self.server, CastVoteArgs {
                sender_id: id,
                seq,
                term,
                vote: true,
            })
            .await;
        }
    }

    fn configure_server(&mut self) {
        self.server.configure(self.configuration.clone());
        self.configured = true;
    }

    fn expect_retransmission_timer_registration_now(
        &mut self,
        expected_receiver_id: usize,
    ) -> (Duration, oneshot::Sender<()>) {
        loop {
            let event_with_completer = self
                .scheduled_event_receiver
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
    ) -> (Duration, oneshot::Sender<()>) {
        self.synchronize().await;
        self.expect_retransmission_timer_registration_now(expected_receiver_id)
    }

    fn expect_retransmission_timer_registrations_now<T>(
        &mut self,
        expected_receiver_ids: T,
    ) -> HashMap<usize, oneshot::Sender<()>>
    where
        T: IntoIterator<Item = usize>,
    {
        let mut expected_receiver_ids: HashSet<usize> =
            HashSet::from_iter(expected_receiver_ids);
        let mut completers = HashMap::new();
        while !expected_receiver_ids.is_empty() {
            let event_with_completer = self
                .scheduled_event_receiver
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

    async fn expect_retransmission_timer_registrations<T>(
        &mut self,
        expected_receiver_ids: T,
    ) -> HashMap<usize, oneshot::Sender<()>>
    where
        T: IntoIterator<Item = usize>,
    {
        self.synchronize().await;
        self.expect_retransmission_timer_registrations_now(
            expected_receiver_ids,
        )
    }

    fn expect_message_now(
        &mut self,
        expected_receiver_id: usize,
    ) -> Message<DummyCommand> {
        loop {
            let event = self
                .server
                .next()
                .now_or_never()
                .flatten()
                .expect("no message sent");
            match event {
                ServerEvent::SendMessage {
                    message,
                    receiver_id,
                } => {
                    if receiver_id == expected_receiver_id {
                        return message;
                    }
                },
                ServerEvent::ElectionStateChange {
                    election_state,
                    ..
                } => {
                    panic!(
                        "Unexpected state transition to {:?}",
                        election_state
                    );
                },
                ServerEvent::LogCommitted(_) => {
                    panic!("Unexpectedly committed log entries")
                },
            }
        }
    }

    async fn expect_message(
        &mut self,
        receiver_id: usize,
    ) -> Message<DummyCommand> {
        self.synchronize().await;
        self.expect_message_now(receiver_id)
    }

    async fn expect_retransmission(
        &mut self,
        expected_receiver_id: usize,
    ) -> Message<DummyCommand> {
        let (retransmit_duration, completer) = self
            .expect_retransmission_timer_registration(expected_receiver_id)
            .await;
        assert_eq!(self.configuration.rpc_timeout, retransmit_duration);
        completer.send(()).expect("server dropped retransmission future");
        self.expect_message(expected_receiver_id).await
    }

    fn new() -> Self {
        let (server, scheduled_event_receiver) = Server::new();
        Self {
            cluster: hashset! {2, 5, 6, 7, 11},
            configuration: Configuration {
                election_timeout: Duration::from_millis(100)
                    ..Duration::from_millis(200),
                heartbeat_interval: Duration::from_millis(50),
                rpc_timeout: Duration::from_millis(10),
                install_snapshot_timeout: Duration::from_secs(10),
            },
            configured: false,
            id: 5,
            scheduled_event_receiver,
            server,
        }
    }

    fn mobilize_server(&mut self) {
        let (mock_log, _mock_log_back_end) = MockLog::new();
        self.mobilize_server_with_log(Box::new(mock_log))
    }

    fn mobilize_server_with_log(
        &mut self,
        log: Box<dyn Log<Command = DummyCommand>>,
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
        log: Box<dyn Log<Command = DummyCommand>>,
        persistent_storage: Box<dyn PersistentStorage>,
    ) {
        if !self.configured {
            self.configure_server();
        }
        self.server.mobilize(ServerMobilizeArgs {
            id: self.id,
            cluster: self.cluster.clone(),
            log,
            persistent_storage,
        });
    }

    fn mobilize_server_with_persistent_storage(
        &mut self,
        persistent_storage: Box<dyn PersistentStorage>,
    ) {
        let (mock_log, _mock_log_back_end) = MockLog::new();
        self.mobilize_server_with_log_and_persistent_storage(
            Box::new(mock_log),
            persistent_storage,
        );
    }

    fn is_verified_append_entries(
        &self,
        args: VerifyAppendEntriesArgs,
    ) -> bool {
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
                .next()
                .now_or_never()
                .flatten()
                .expect("no append entries sent");
            match event {
                ServerEvent::SendMessage {
                    message,
                    receiver_id,
                } => {
                    if self.is_verified_append_entries(
                        VerifyAppendEntriesArgs {
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
                ServerEvent::ElectionStateChange {
                    election_state,
                    ..
                } => {
                    panic!(
                        "unexpected election state change to {:?}",
                        election_state
                    );
                },
                ServerEvent::LogCommitted(_) => {
                    panic!("Unexpectedly committed log entries")
                },
            }
        }
    }

    async fn expect_log_entries_broadcast(
        &mut self,
        args: AwaitAppendEntriesArgs,
    ) {
        self.synchronize().await;
        let mut recipients = self.cluster.clone();
        recipients.remove(&self.id);
        while !recipients.is_empty() {
            let recipient_id = self.expect_append_entries_now(&args);
            assert!(
                recipients.remove(&recipient_id),
                "Unexpected append entries sent to {}",
                recipient_id
            );
        }
    }

    fn verify_append_entries_response(
        &self,
        args: VerifyAppendEntriesResponseArgs,
    ) {
        assert_eq!(
            args.match_index, args.expected.match_index,
            "unexpected match index (was {}, should be {})",
            args.match_index, args.expected.match_index
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
        &self,
        message: &Message<DummyCommand>,
        receiver_id: usize,
        args: &AwaitAppendEntriesResponseArgs,
    ) -> bool {
        if let MessageContent::AppendEntriesResults {
            match_index,
        } = message.content
        {
            self.verify_append_entries_response(
                VerifyAppendEntriesResponseArgs {
                    seq: message.seq,
                    term: message.term,
                    match_index,
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
        args: AwaitAppendEntriesResponseArgs,
    ) {
        let mut state_changed = false;
        let mut log_committed = false;
        loop {
            let event = self
                .server
                .next()
                .now_or_never()
                .flatten()
                .expect("no append entries response sent");
            match event {
                ServerEvent::SendMessage {
                    message,
                    receiver_id,
                } => {
                    if self.is_verified_append_entries_response(
                        &message,
                        receiver_id,
                        &args,
                    ) {
                        break;
                    }
                },
                ServerEvent::ElectionStateChange {
                    election_state,
                    term,
                    ..
                } => {
                    state_changed = true;
                    assert_eq!(election_state, ServerElectionState::Follower);
                    assert_eq!(
                        term, args.term,
                        "wrong term in election state change (was {}, should be {})",
                        term, args.term
                    );
                },
                ServerEvent::LogCommitted(commit_index) => {
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
        args: AwaitAppendEntriesResponseArgs,
    ) {
        self.synchronize().await;
        self.expect_append_entries_response_now(args);
    }

    async fn send_server_message(
        &mut self,
        message: Message<DummyCommand>,
        sender_id: usize,
    ) {
        send_server_message(&mut self.server, message, sender_id).await
    }

    async fn synchronize(&mut self) {
        synchronize(&mut self.server).await;
    }
}

fn new_mock_log_with_non_defaults(
    base_term: usize,
    base_index: usize,
) -> (MockLog, MockLogBackEnd) {
    let (mock_log, mock_log_back_end) = MockLog::new();
    {
        let mut log_shared = mock_log_back_end.shared.lock().unwrap();
        log_shared.base_term = base_term;
        log_shared.base_index = base_index;
        log_shared.last_term = base_term;
        log_shared.last_index = base_index;
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

fn verify_log<L>(
    mock_log_back_end: &MockLogBackEnd,
    base_term: usize,
    base_index: usize,
    entries: L,
) where
    L: AsRef<[LogEntry<DummyCommand>]>,
{
    let log_shared = mock_log_back_end.shared.lock().unwrap();
    assert_eq!(base_term, log_shared.base_term);
    assert_eq!(base_index, log_shared.base_index);
    assert_eq!(entries.as_ref(), log_shared.entries);
}

fn verify_persistent_storage(
    mock_persistent_storage_back_end: &MockPersistentStorageBackEnd,
    term: usize,
    voted_for: Option<usize>,
) {
    let persistent_storage_shared =
        mock_persistent_storage_back_end.shared.lock().unwrap();
    assert_eq!(term, persistent_storage_shared.term);
    assert_eq!(voted_for, persistent_storage_shared.voted_for);
}

async fn send_server_message(
    server: &mut Server<DummyCommand>,
    message: Message<DummyCommand>,
    sender_id: usize,
) {
    server
        .send(ServerSinkItem::ReceiveMessage {
            message,
            sender_id,
        })
        .await
        .unwrap();
}

async fn synchronize(server: &mut Server<DummyCommand>) {
    let (completed_sender, completed_receiver) = oneshot::channel();
    server.send(ServerSinkItem::Synchronize(completed_sender)).await.unwrap();
    let _ = completed_receiver.await;
}

async fn cast_vote(
    server: &mut Server<DummyCommand>,
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
            content: MessageContent::RequestVoteResults {
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
    server: &mut Server<DummyCommand>,
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
                candidate_id: sender_id,
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
