use super::*;

mod common;
mod elections;
mod mock_log;
mod mock_persistent_storage;

use crate::{
    ScheduledEvent,
    ScheduledEventReceiver,
};
use futures::{
    FutureExt as _,
    StreamExt as _,
};
use maplit::hashset;
use mock_log::MockLog;
use mock_persistent_storage::MockPersistentStorage;
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

struct MobilizedServerResources {
    log: Arc<MockLog>,
    persistent_storage: Arc<MockPersistentStorage>,
    scheduler_event_receiver: ScheduledEventReceiver,
}

struct Fixture {
    cluster: HashSet<usize>,
    configuration: Configuration,
    configured: bool,
    id: usize,
    server: Server,
}

impl Fixture {
    async fn await_election_timeout(
        &mut self,
        mut scheduler_event_receiver: ScheduledEventReceiver,
    ) {
        let election_timeout_duration = timeout(
            REASONABLE_FAST_OPERATION_TIMEOUT,
            async {
                loop {
                    let event = scheduler_event_receiver
                        .next()
                        .await
                        .expect("no election timer registered");
                    if let ScheduledEvent::ElectionTimeout(duration) = event {
                        break duration;
                    }
                }
            }
            .boxed(),
        )
        .await
        .expect("timeout waiting for election timer registration");
        assert!(self
            .configuration
            .election_timeout
            .contains(&election_timeout_duration));

        let mut others = self.cluster.clone();
        others.remove(&self.id);
        let mut expected_vote_requests = others.len();
        // TODO: Wait on server stream until we receive all the expected
        // vote requests.

        // TODO: Make sure the server is a candidate once all vote requests
        // have been sent.
        //
        // assert_eq!(ElectionState::Candidate, fixture.election_state);
    }

    fn configure_server(&mut self) {
        self.server.configure(self.configuration.clone());
        self.configured = true;
    }

    fn new() -> Self {
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
            server: Server::new(),
        }
    }

    fn mobilize_server(&mut self) -> MobilizedServerResources {
        let log = Arc::new(MockLog::new());
        let persistent_storage = Arc::new(MockPersistentStorage::new());
        let scheduler_event_receiver = self.server.scheduled_event_receiver();
        if !self.configured {
            self.configure_server();
        }
        self.server.mobilize(MobilizeArgs {
            id: self.id,
            cluster: self.cluster.clone(),
            log: log.clone(),
            persistent_storage: persistent_storage.clone(),
        });
        MobilizedServerResources {
            log,
            persistent_storage,
            scheduler_event_receiver,
        }
    }
}
