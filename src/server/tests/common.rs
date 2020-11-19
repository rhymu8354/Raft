use super::*;

#[test]
fn mobilize_twice_does_not_crash() {
    let fixture = Fixture::new();
    let (mock_log, _) = MockLog::new();
    let (mock_persistent_storage, _) = MockPersistentStorage::new();
    fixture.server.mobilize(Mobilization {
        id: fixture.id,
        cluster: fixture.cluster.clone(),
        log: Box::new(mock_log),
        persistent_storage: Box::new(mock_persistent_storage),
    });
    let (mock_log, _) = MockLog::new();
    let (mock_persistent_storage, _) = MockPersistentStorage::new();
    fixture.server.mobilize(Mobilization {
        id: fixture.id,
        cluster: fixture.cluster.clone(),
        log: Box::new(mock_log),
        persistent_storage: Box::new(mock_persistent_storage),
    });
}

#[test]
fn log_keeper_released_on_demobilize() {
    let mut fixture = Fixture::new();
    let MobilizedServerResources {
        mock_log_back_end,
        ..
    } = fixture.mobilize_server();
    executor::block_on(async {
        timeout(REASONABLE_FAST_OPERATION_TIMEOUT, fixture.server.demobilize())
            .await
            .expect("timeout waiting for demobilize to complete")
    });
    assert!(mock_log_back_end
        .dropped
        .load(std::sync::atomic::Ordering::SeqCst));
}

#[test]
fn persistent_state_released_on_demobilize() {
    let mut fixture = Fixture::new();
    let MobilizedServerResources {
        mock_persistent_storage_back_end,
        ..
    } = fixture.mobilize_server();
    executor::block_on(async {
        timeout(REASONABLE_FAST_OPERATION_TIMEOUT, fixture.server.demobilize())
            .await
            .expect("timeout waiting for demobilize to complete")
    });
    assert!(mock_persistent_storage_back_end
        .dropped
        .load(std::sync::atomic::Ordering::SeqCst));
}
