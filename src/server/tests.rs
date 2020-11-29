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
};
use futures::{
    FutureExt as _,
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

#[derive(Debug, Eq, PartialEq, Serialize, Deserialize)]
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
    async fn await_election_timeout(
        &mut self,
        mut args: AwaitElectionTimeoutArgs,
    ) {
        // Expect the server to register an election timeout event with a
        // duration within the configured range, and complete it.
        let (election_timeout_duration, mut election_timeout_completers) =
            timeout(
                REASONABLE_FAST_OPERATION_TIMEOUT,
                async {
                    let mut completers = Vec::new();
                    completers.reserve(args.expected_cancellations + 1);
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
                            completers.push(completer);
                            if let Some(remaining_expected_cancellations) =
                                args.expected_cancellations.checked_sub(1)
                            {
                                args.expected_cancellations =
                                    remaining_expected_cancellations;
                            } else {
                                break (duration, completers);
                            }
                        }
                    }
                }
                .boxed(),
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

        // Wait on server stream until we receive all the expected
        // vote requests.
        let mut awaiting_vote_requests = self.cluster.clone();
        awaiting_vote_requests.remove(&self.id);
        let mut election_state = ElectionState::Follower;
        while !awaiting_vote_requests.is_empty() {
            let receiver_id = timeout(
                REASONABLE_FAST_OPERATION_TIMEOUT,
                async {
                    loop {
                        let event = self
                            .server
                            .next()
                            .await
                            .expect("unexpected end of server events");
                        match event {
                            ServerEvent::SendMessage(
                                Message::<DummyCommand> {
                                    content:
                                        MessageContent::<DummyCommand>::RequestVote {
                                            candidate_id,
                                            last_log_index,
                                            last_log_term,
                                        },
                                    receiver_id,
                                    ..
                                },
                                ..,
                            ) => {
                                assert_eq!(
                                    last_log_term,
                                    args.last_log_term,
                                    "wrong last_log_term in vote request (was {}, should be {})",
                                    last_log_term,
                                    args.last_log_term
                                );
                                assert_eq!(
                                    last_log_index,
                                    args.last_log_index,
                                    "wrong last_log_index in vote request (was {}, should be {})",
                                    last_log_index,
                                    args.last_log_index
                                );
                                break receiver_id;
                            },
                            ServerEvent::ElectionStateChange {
                                election_state: new_election_state,
                                ..
                            } => {
                                election_state = new_election_state;
                            },
                            _ => {},
                        }
                    }
                }
                .boxed(),
            )
            .await
            .expect("timeout waiting for vote request message");
            assert!(
                awaiting_vote_requests.remove(&receiver_id),
                "Unexpected vote request from {} sent",
                receiver_id
            );
        }

        // Make sure the server is a candidate once all vote requests
        // have been sent.
        assert_eq!(ElectionState::Candidate, election_state);
    }

    fn configure_server(&mut self) {
        self.server.configure(self.configuration.clone());
        self.configured = true;
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
        let (mock_log, mock_log_back_end) = MockLog::new();
        self.mobilize_server_with_log(Box::new(mock_log))
    }

    fn mobilize_server_with_log(
        &mut self,
        log: Box<dyn Log>,
    ) {
        let (mock_persistent_storage, mock_persistent_storage_back_end) =
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
        let (mock_log, mock_log_back_end) = MockLog::new();
        self.mobilize_server_with_log_and_persistent_storage(
            Box::new(mock_log),
            persistent_storage,
        );
    }
}
