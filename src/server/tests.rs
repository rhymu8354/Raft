use super::*;

mod common;
mod elections;
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

const REASONABLE_FAST_OPERATION_TIMEOUT: Duration = Duration::from_millis(200);

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
    term: usize,
    vote: bool,
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

    async fn trigger_election_timeout(
        &mut self,
        num_timers_to_skip: usize,
        other_completers: &mut Vec<oneshot::Sender<()>>,
    ) {
        let (election_timeout_duration, mut election_timeout_completers) =
            timeout(
                REASONABLE_FAST_OPERATION_TIMEOUT,
                self.await_election_timer_registrations(
                    num_timers_to_skip + 1,
                    other_completers,
                ),
            )
            .await
            .expect("timeout waiting for election timer registration");
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

    async fn expect_vote_request(
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

    async fn expect_server_to_start_election(
        &mut self,
        expected_last_log_term: usize,
        expected_last_log_index: usize,
        expected_term: usize,
    ) {
        let mut awaiting_vote_requests = self.cluster.clone();
        awaiting_vote_requests.remove(&self.id);
        while !awaiting_vote_requests.is_empty() {
            let receiver_id = timeout(
                REASONABLE_FAST_OPERATION_TIMEOUT,
                self.expect_vote_request(
                    expected_last_log_term,
                    expected_last_log_index,
                    None,
                    expected_term,
                ),
            )
            .await
            .expect("timeout waiting for vote request message");
            assert!(
                awaiting_vote_requests.remove(&receiver_id),
                "Unexpected vote request from {} sent",
                receiver_id
            );
        }
    }

    async fn await_election(
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

    async fn await_election_with_defaults(&mut self) {
        self.await_election(AwaitElectionTimeoutArgs {
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
        timeout(
            REASONABLE_FAST_OPERATION_TIMEOUT,
            async {
                loop {
                    let event = self
                        .server
                        .next()
                        .await
                        .expect("unexpected end of server events");
                    if let ServerEvent::ElectionStateChange {
                        election_state: new_election_state,
                        term,
                        voted_for
                    } = event {
                        assert_eq!(ElectionState::Leader, new_election_state);
                        assert_eq!(
                            term,
                            args.term,
                            "wrong term in election state change (was {}, should be {})",
                            term,
                            args.term
                        );
                        assert!(matches!(voted_for, Some(id) if id == self.id),
                            "server voted for {:?}, not itself ({})",
                            voted_for,
                            self.id
                        );
                        break;
                    }
                }
            }
            .boxed(),
        )
        .await
        .expect("timeout waiting for leadership assumption");
    }

    async fn cast_vote(
        &mut self,
        args: CastVoteArgs,
    ) {
        cast_vote(&mut self.server, args).await;
    }

    async fn cast_votes(
        &mut self,
        term: usize,
    ) {
        for &id in self.cluster.iter() {
            if id == self.id {
                continue;
            }
            cast_vote(&mut self.server, CastVoteArgs {
                sender_id: id,
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

    async fn await_retransmission(
        &mut self,
        expected_receiver_id: usize,
    ) -> Message<DummyCommand> {
        let mut other_completers = Vec::new();
        let (retransmit_duration, completer) =
            timeout(REASONABLE_FAST_OPERATION_TIMEOUT, async {
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
                        if peer_id == 2 {
                            break (duration, completer);
                        }
                    } else {
                        other_completers.push(event_with_completer.completer);
                    }
                }
            })
            .await
            .expect("timeout waiting for retransmission timer registration");
        assert_eq!(self.configuration.rpc_timeout, retransmit_duration);
        completer.send(()).expect("server dropped retransmission future");
        timeout(REASONABLE_FAST_OPERATION_TIMEOUT, async {
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
                            break message;
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
        })
        .await
        .expect("timeout waiting for retransmission")
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
    server
        .send(ServerSinkItem::ReceiveMessage {
            message,
            sender_id,
        })
        .await
        .unwrap();
}

async fn cast_vote(
    server: &mut Server<DummyCommand>,
    CastVoteArgs {
        sender_id,
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
            seq: 0,
            term,
        },
        sender_id,
    )
    .await;
}
