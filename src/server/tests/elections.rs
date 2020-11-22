use super::*;

#[test]
fn new_election() {
    let mut fixture = Fixture::new();
    let (mock_log, mock_log_back_end) = MockLog::new();
    {
        let mut log_shared = mock_log_back_end.shared.lock().unwrap();
        log_shared.base_term = 7;
        log_shared.base_index = 42;
    }
    fixture.mobilize_server_with_log(Box::new(mock_log));
    executor::block_on(fixture.await_election_timeout(
        AwaitElectionTimeoutArgs {
            expected_cancellations: 2,
            last_log_term: 7,
            last_log_index: 42,
        },
    ));
}
