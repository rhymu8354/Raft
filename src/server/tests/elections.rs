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
    executor::block_on(fixture.await_election_timeout_with_defaults());
    executor::block_on(fixture.cast_votes(1));
    executor::block_on(fixture.await_assume_leadership(
        AwaitAssumeLeadershipArgs {
            term: 1,
        },
    ));
}

#[test]
fn elected_leader_non_unanimous_majority() {
    let mut fixture = Fixture::new();
    fixture.mobilize_server();
    executor::block_on(fixture.await_election_timeout_with_defaults());
    executor::block_on(fixture.cast_vote(2, 1, true));
    executor::block_on(fixture.cast_vote(6, 1, true));
    executor::block_on(fixture.await_assume_leadership(
        AwaitAssumeLeadershipArgs {
            term: 1,
        },
    ));
}

#[test]
fn server_retransmits_request_vote_for_slow_voters_in_election() {
    let mut fixture = Fixture::new();
    fixture.mobilize_server();
    executor::block_on(fixture.await_election_timeout_with_defaults());
    let retransmission = executor::block_on(fixture.await_retransmission(2));
    if let MessageContent::<DummyCommand>::RequestVote {
        candidate_id,
        last_log_index,
        last_log_term,
    } = retransmission.content
    {
        assert_eq!(
            candidate_id, fixture.id,
            "wrong candidate_id in vote request (was {}, should be {})",
            candidate_id, fixture.id
        );
        assert_eq!(
            last_log_term, 0,
            "wrong last_log_term in vote request (was {}, should be {})",
            last_log_term, 0
        );
        assert_eq!(
            last_log_index, 0,
            "wrong last_log_index in vote request (was {}, should be {})",
            last_log_index, 0
        );
        assert_eq!(
            retransmission.term, 1,
            "wrong term in vote request (was {}, should be {})",
            retransmission.term, 1
        );
    } else {
        panic!(
            "Expected request vote message, got {:?} instead",
            retransmission
        );
    }
}

#[test]
fn timeout_before_majority_vote_or_new_leader_heart_beat() {
    let mut fixture = Fixture::new();
    fixture.mobilize_server();
    executor::block_on(fixture.await_election_timeout_with_defaults());
    executor::block_on(fixture.cast_vote(6, 1, false));
    executor::block_on(fixture.cast_vote(7, 1, false));
    executor::block_on(fixture.cast_vote(11, 1, true));
    executor::block_on(fixture.await_election_timeout(
        AwaitElectionTimeoutArgs {
            expected_cancellations: 0,
            last_log_term: 0,
            last_log_index: 0,
            term: 2,
        },
    ));
}
