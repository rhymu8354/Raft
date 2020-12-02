use super::*;

mod common;
mod elections_follower;
mod elections_leader;
mod mock_log;
mod mock_persistent_storage;

use crate::{
    LogEntryCustomCommand,
    Message,
    MessageContent,
    ScheduledEvent,
    ScheduledEventReceiver,
    ScheduledEventWithCompleter,
    ServerSinkItem,
};
use futures::{
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
    collections::HashSet,
    time::Duration,
};

const REASONABLE_FAST_OPERATION_TIMEOUT: Duration = Duration::from_millis(1000);

pub async fn timeout<F, T>(
    max_time: std::time::Duration,
    f: F,
) -> Result<T, ()>
where
    F: futures::Future<Output = T>,
{
    futures::select! {
        result = f.fuse() => Ok(result),
        _ = futures_timer::Delay::new(max_time).fuse() => Err(())
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
struct DummyCommand {}

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
    expected_cancellations: usize,
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
    message: &'a Message<DummyCommand>,
    expected_seq: usize,
    expected_term: usize,
    expected_vote: bool,
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
    async fn await_election_timer_registrations(
        &mut self,
        mut num_timers_to_await: usize,
        other_timer_completers: &mut Vec<oneshot::Sender<()>>,
    ) -> (Duration, Vec<oneshot::Sender<()>>) {
        let mut election_timeout_completers = Vec::new();
        election_timeout_completers.reserve(num_timers_to_await);
        loop {
            let event_with_completer = self
                .scheduled_event_receiver
                .next()
                .await
                .expect("no election timer registered");
            if let ScheduledEventWithCompleter {
                scheduled_event: ScheduledEvent::ElectionTimeout,
                duration,
                completer,
            } = event_with_completer
            {
                election_timeout_completers.push(completer);
                num_timers_to_await -= 1;
                if num_timers_to_await == 0 {
                    break (duration, election_timeout_completers);
                }
            } else {
                other_timer_completers.push(event_with_completer.completer);
            }
        }
    }

    async fn expect_election_timer_registrations(
        &mut self,
        num_timers_to_await: usize,
        other_timer_completers: &mut Vec<oneshot::Sender<()>>,
    ) -> (Duration, Vec<oneshot::Sender<()>>) {
        timeout(
            REASONABLE_FAST_OPERATION_TIMEOUT,
            self.await_election_timer_registrations(
                num_timers_to_await,
                other_timer_completers,
            ),
        )
        .await
        .expect("timeout waiting for election timer registration")
    }

    fn expect_election_timer_registrations_now(
        &mut self,
        mut num_timers_to_expect: usize,
    ) {
        while let Some(event_with_completer) =
            self.scheduled_event_receiver.next().now_or_never()
        {
            if let ScheduledEventWithCompleter {
                scheduled_event: ScheduledEvent::ElectionTimeout,
                ..
            } =
                event_with_completer.expect("unexpected end of server events")
            {
                let timers_remaining = num_timers_to_expect
                    .checked_sub(1)
                    .expect("too many election timers registered");
                num_timers_to_expect = timers_remaining;
            }
        }
        assert_eq!(
            0, num_timers_to_expect,
            "too few election timers registered"
        );
    }

    async fn trigger_election_timeout(
        &mut self,
        num_timers_to_skip: usize,
        other_completers: &mut Vec<oneshot::Sender<()>>,
    ) {
        let (election_timeout_duration, mut election_timeout_completers) = self
            .expect_election_timer_registrations(
                num_timers_to_skip + 1,
                other_completers,
            )
            .await;
        assert!(
            self.configuration
                .election_timeout
                .contains(&election_timeout_duration),
            "election timeout duration {:?} is not within {:?}",
            election_timeout_duration,
            self.configuration.election_timeout
        );
        election_timeout_completers
            .pop()
            .expect("no election timeout completers received")
            .send(())
            .expect("server dropped election timeout future");
    }

    fn verify_vote_request(
        &self,
        args: VerifyVoteRequestArgs,
    ) -> Result<(), ()> {
        // Prefer '&' over '*' despite what Clippy says, because reasons.
        #[allow(clippy::match_ref_pats)]
        if let &MessageContent::<DummyCommand>::RequestVote {
            candidate_id,
            last_log_index,
            last_log_term,
        } = &args.message.content
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
            Ok(())
        } else {
            Err(())
        }
    }

    async fn await_vote_request(
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
                .await
                .expect("unexpected end of server events");
            match event {
                ServerEvent::SendMessage {
                    message,
                    receiver_id,
                } => {
                    if self
                        .verify_vote_request(VerifyVoteRequestArgs {
                            message: &message,
                            expected_last_log_term,
                            expected_last_log_index,
                            expected_seq,
                            expected_term,
                        })
                        .is_ok()
                    {
                        break receiver_id;
                    }
                },
                ServerEvent::ElectionStateChange {
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
        timeout(
            REASONABLE_FAST_OPERATION_TIMEOUT,
            self.await_vote_request(
                expected_last_log_term,
                expected_last_log_index,
                expected_seq,
                expected_term,
            ),
        )
        .await
        .expect("timeout waiting for vote request message")
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
        let mut other_completers = Vec::new();
        self.trigger_election_timeout(
            args.expected_cancellations,
            &mut other_completers,
        )
        .await;

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
            expected_cancellations: 2,
            last_log_term: 0,
            last_log_index: 0,
            term: 1,
        })
        .await
    }

    async fn await_assume_leadership(
        &mut self,
        args: AwaitAssumeLeadershipArgs,
    ) {
        loop {
            let event = self
                .server
                .next()
                .await
                .expect("unexpected end of server events");
            if let ServerEvent::ElectionStateChange {
                election_state: new_election_state,
                term,
                voted_for,
            } = event
            {
                assert_eq!(ElectionState::Leader, new_election_state);
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
        timeout(
            REASONABLE_FAST_OPERATION_TIMEOUT,
            self.await_assume_leadership(args),
        )
        .await
        .expect("timeout waiting for leadership assumption");
    }

    fn verify_vote(
        &self,
        args: VerifyVoteArgs,
    ) -> Result<(), ()> {
        // Prefer '&' over '*' despite what Clippy says, because reasons.
        #[allow(clippy::match_ref_pats)]
        if let &MessageContent::<DummyCommand>::RequestVoteResults {
            vote_granted,
        } = &args.message.content
        {
            assert_eq!(
                vote_granted, args.expected_vote,
                "unexpected vote (was {}, should be {})",
                vote_granted, args.expected_vote
            );
            assert_eq!(
                args.message.term, args.expected_term,
                "wrong term in vote (was {}, should be {})",
                args.message.term, args.expected_term
            );
            assert_eq!(
                args.message.seq, args.expected_seq,
                "wrong sequence number in vote (was {}, should be {})",
                args.message.seq, args.expected_seq
            );
            Ok(())
        } else {
            Err(())
        }
    }

    async fn await_vote(
        &mut self,
        args: AwaitVoteArgs,
    ) {
        let mut state_changed = false;
        loop {
            let event = self
                .server
                .next()
                .await
                .expect("unexpected end of server events");
            match event {
                ServerEvent::SendMessage {
                    message,
                    receiver_id,
                } => {
                    if self
                        .verify_vote(VerifyVoteArgs {
                            message: &message,
                            expected_seq: args.seq,
                            expected_term: args.term,
                            expected_vote: args.vote_granted,
                        })
                        .is_ok()
                    {
                        assert_eq!(args.receiver_id, receiver_id,
                            "vote sent to wrong receiver (was {}, should be {})",
                            receiver_id, args.receiver_id
                        );
                        break;
                    }
                },
                ServerEvent::ElectionStateChange {
                    election_state: new_election_state,
                    term,
                    voted_for,
                } => {
                    state_changed = true;
                    assert_eq!(ElectionState::Follower, new_election_state);
                    assert_eq!(
                        term,
                        args.term,
                        "wrong term in election state change (was {}, should be {})",
                        term,
                        args.term
                    );
                    if args.vote_granted {
                        assert_eq!(
                            voted_for,
                            Some(args.receiver_id),
                            "server voted for {:?}, not the receiver ({})",
                            voted_for,
                            args.receiver_id
                        );
                    }
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
        timeout(REASONABLE_FAST_OPERATION_TIMEOUT, self.await_vote(args))
            .await
            .expect("timeout waiting for vote");
    }

    fn expect_election_state_change(
        &mut self,
        election_state: ElectionState,
    ) {
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

    fn expect_no_election_state_changes(&mut self) {
        while let Some(event) = self.server.next().now_or_never() {
            if let ServerEvent::ElectionStateChange {
                election_state,
                ..
            } = event.expect("unexpected end of server events")
            {
                panic!(
                    "unexpected election state change to {:?}",
                    election_state
                );
            }
        }
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

    async fn await_retransmission_timer_registration(
        &mut self,
        expected_receiver_id: usize,
        other_completers: &mut Vec<oneshot::Sender<()>>,
    ) -> (Duration, oneshot::Sender<()>) {
        loop {
            let event_with_completer = self
                .scheduled_event_receiver
                .next()
                .await
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
            } else {
                other_completers.push(event_with_completer.completer);
            }
        }
    }

    async fn expect_retransmission_timer_registration(
        &mut self,
        expected_receiver_id: usize,
        other_completers: &mut Vec<oneshot::Sender<()>>,
    ) -> (Duration, oneshot::Sender<()>) {
        timeout(
            REASONABLE_FAST_OPERATION_TIMEOUT,
            self.await_retransmission_timer_registration(
                expected_receiver_id,
                other_completers,
            ),
        )
        .await
        .expect("timeout waiting for retransmission timer registration")
    }

    async fn await_message(
        &mut self,
        expected_receiver_id: usize,
    ) -> Message<DummyCommand> {
        loop {
            let event = self
                .server
                .next()
                .await
                .expect("unexpected end of server events");
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
            }
        }
    }

    async fn expect_message(
        &mut self,
        receiver_id: usize,
    ) -> Message<DummyCommand> {
        timeout(
            REASONABLE_FAST_OPERATION_TIMEOUT,
            self.await_message(receiver_id),
        )
        .await
        .expect("timeout waiting for message")
    }

    async fn await_retransmission(
        &mut self,
        expected_receiver_id: usize,
    ) -> Message<DummyCommand> {
        let mut other_completers = Vec::new();
        let (retransmit_duration, completer) = self
            .expect_retransmission_timer_registration(
                expected_receiver_id,
                &mut other_completers,
            )
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
        log: Box<dyn Log>,
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
        log: Box<dyn Log>,
        persistent_storage: Box<dyn PersistentStorage>,
    ) {
        if !self.configured {
            self.configure_server();
        }
        self.server.mobilize(MobilizeArgs {
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
    let (completed_sender, completed_receiver) = oneshot::channel();
    server
        .send(ServerSinkItem::ReceiveMessage {
            message,
            sender_id,
            received: Some(completed_sender),
        })
        .await
        .unwrap();
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
