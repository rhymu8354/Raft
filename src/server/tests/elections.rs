use super::*;

#[test]
fn new_election() {
    let mut fixture = Fixture::new();
    let (mock_log, mock_log_back_end) = MockLog::new();
    let (mock_persistent_storage, mock_persistent_storage_back_end) =
        MockPersistentStorage::new();
    {
        let mut log_shared = mock_log_back_end.shared.lock().unwrap();
        log_shared.base_term = 7;
        log_shared.base_index = 42;
    }
    {
        let mut persistent_storage_shared =
            mock_persistent_storage_back_end.shared.lock().unwrap();
        persistent_storage_shared.term = 9;
    }
    fixture.mobilize_server_with_log_and_persistent_storage(
        Box::new(mock_log),
        Box::new(mock_persistent_storage),
    );
    executor::block_on(fixture.await_election_timeout(
        AwaitElectionTimeoutArgs {
            expected_cancellations: 2,
            last_log_term: 7,
            last_log_index: 42,
            term: 10,
        },
    ));
    {
        let persistent_storage_shared =
            mock_persistent_storage_back_end.shared.lock().unwrap();
        assert_eq!(10, persistent_storage_shared.term);
        assert!(matches!(
            persistent_storage_shared.voted_for,
            Some(id) if id == fixture.id
        ));
    }
}

#[test]
fn elected_leader_unanimously() {
    let mut fixture = Fixture::new();
    fixture.mobilize_server();
    executor::block_on(fixture.await_election_timeout(
        AwaitElectionTimeoutArgs {
            expected_cancellations: 2,
            last_log_term: 0,
            last_log_index: 0,
            term: 1,
        },
    ));
    executor::block_on(fixture.cast_votes(1));
    executor::block_on(fixture.await_assume_leadership(
        AwaitAssumeLeadershipArgs {
            term: 1,
        },
    ));
}
