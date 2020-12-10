use super::*;
use crate::tests::assert_logger;
use futures::executor;

#[test]
fn mobilize_twice_does_not_crash() {
    assert_logger();
    let fixture = Fixture::new();
    let (mock_log, _) = MockLog::new();
    let (mock_persistent_storage, _) = MockPersistentStorage::new();
    fixture.server.mobilize(ServerMobilizeArgs {
        id: fixture.id,
        cluster: fixture.cluster.clone(),
        log: Box::new(mock_log),
        persistent_storage: Box::new(mock_persistent_storage),
    });
    let (mock_log, _) = MockLog::new();
    let (mock_persistent_storage, _) = MockPersistentStorage::new();
    fixture.server.mobilize(ServerMobilizeArgs {
        id: fixture.id,
        cluster: fixture.cluster.clone(),
        log: Box::new(mock_log),
        persistent_storage: Box::new(mock_persistent_storage),
    });
}

#[test]
fn log_keeper_released_on_demobilize() {
    assert_logger();
    executor::block_on(async {
        let mut fixture = Fixture::new();
        let (mock_log, mock_log_back_end) = MockLog::new();
        fixture.mobilize_server_with_log(Box::new(mock_log));
        fixture.server.demobilize().await;
        let mock_log_shared = mock_log_back_end.shared.lock().unwrap();
        assert!(mock_log_shared.dropped);
    });
}

#[test]
fn persistent_state_released_on_demobilize() {
    assert_logger();
    executor::block_on(async {
        let mut fixture = Fixture::new();
        let (mock_persistent_storage, mock_persistent_storage_back_end) =
            MockPersistentStorage::new();
        fixture.mobilize_server_with_persistent_storage(Box::new(
            mock_persistent_storage,
        ));
        fixture.server.demobilize().await;
        let mock_persistent_storage_back_end =
            mock_persistent_storage_back_end.shared.lock().unwrap();
        assert!(mock_persistent_storage_back_end.dropped);
    });
}
