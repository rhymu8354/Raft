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

type DummyMessage = Message<DummyCommand>;
type DummyMessageContent = MessageContent<DummyCommand>;

struct MobilizedServerResources {
    mock_log_back_end: MockLogBackEnd,
    mock_persistent_storage_back_end: MockPersistentStorageBackEnd,
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
    async fn await_election_timeout(&mut self) {
        // Expect the server to register an election timeout event with a
        // duration within the configured range, and complete it.
        let (election_timeout_duration, election_timeout_completer) = timeout(
            REASONABLE_FAST_OPERATION_TIMEOUT,
            async {
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
                        break (duration, completer);
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
        election_timeout_completer
            .send(())
            .expect("server dropped election timeout future");

        // Wait on server stream until we receive all the expected
        // vote requests.
        let mut awaiting_vote_requests = self.cluster.clone();
        awaiting_vote_requests.remove(&self.id);
        let mut election_state = ElectionState::Follower;
        while !awaiting_vote_requests.is_empty() {
            let candidate_id = timeout(
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
                                DummyMessage {
                                    content:
                                        DummyMessageContent::RequestVote {
                                            candidate_id,
                                            ..
                                        },
                                    ..
                                },
                                ..,
                            ) => {
                                break candidate_id;
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
                awaiting_vote_requests.remove(&candidate_id),
                "Unexpected vote request from {} sent",
                candidate_id
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

    fn mobilize_server(&mut self) -> MobilizedServerResources {
        let (mock_log, mock_log_back_end) = MockLog::new();
        let (mock_persistent_storage, mock_persistent_storage_back_end) =
            MockPersistentStorage::new();
        if !self.configured {
            self.configure_server();
        }
        self.server.mobilize(MobilizeArgs {
            id: self.id,
            cluster: self.cluster.clone(),
            log: Box::new(mock_log),
            persistent_storage: Box::new(mock_persistent_storage),
        });
        MobilizedServerResources {
            mock_log_back_end,
            mock_persistent_storage_back_end,
        }
    }
}
