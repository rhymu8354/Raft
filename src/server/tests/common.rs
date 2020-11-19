use super::*;

#[test]
fn mobilize_twice_does_not_crash() {
    let mut fixture = Fixture::new();
    let mock_log = Arc::new(MockLog::new());
    let mock_persistent_storage = Arc::new(MockPersistentStorage::new());
    fixture.server.mobilize(MobilizeArgs {
        id: fixture.id,
        cluster: fixture.cluster.clone(),
        log: mock_log.clone(),
        persistent_storage: mock_persistent_storage.clone(),
    });
    fixture.server.mobilize(MobilizeArgs {
        id: fixture.id,
        cluster: fixture.cluster.clone(),
        log: mock_log,
        persistent_storage: mock_persistent_storage,
    });
}

#[test]
fn log_keeper_released_on_demobilize() {
    let mut fixture = Fixture::new();
    let log_dropped = {
        let MobilizedServerResources {
            log,
            ..
        } = fixture.mobilize_server();
        log.dropped.clone()
    };
    fixture.server.demobilize();
    assert!(log_dropped.load(std::sync::atomic::Ordering::SeqCst));
}

#[test]
fn persistent_state_released_on_demobilize() {
    let mut fixture = Fixture::new();
    let log_dropped = {
        let MobilizedServerResources {
            persistent_storage,
            ..
        } = fixture.mobilize_server();
        persistent_storage.dropped.clone()
    };
    fixture.server.demobilize();
    assert!(log_dropped.load(std::sync::atomic::Ordering::SeqCst));
}
