use super::*;

#[test]
fn new_election() {
    executor::block_on(async {
        let mut fixture = Fixture::new();
        let (mock_log, _mock_log_back_end) =
            new_mock_log_with_non_defaults(7, 42);
        let (mock_persistent_storage, mock_persistent_storage_back_end) =
            new_mock_persistent_storage_with_non_defaults(9, None);
        fixture.mobilize_server_with_log_and_persistent_storage(
            Box::new(mock_log),
            Box::new(mock_persistent_storage),
        );
        fixture
            .await_election(AwaitElectionTimeoutArgs {
                expected_cancellations: 2,
                last_log_term: 7,
                last_log_index: 42,
                term: 10,
            })
            .await;
        verify_persistent_storage(
            &mock_persistent_storage_back_end,
            10,
            Some(fixture.id),
        );
    });
}

#[test]
fn elected_leader_unanimously() {
    executor::block_on(async {
        let mut fixture = Fixture::new();
        fixture.mobilize_server();
        fixture.await_election_with_defaults().await;
        fixture.cast_votes(1).await;
        fixture
            .await_assume_leadership(AwaitAssumeLeadershipArgs {
                term: 1,
            })
            .await;
    });
}

#[test]
fn elected_leader_non_unanimous_majority() {
    executor::block_on(async {
        let mut fixture = Fixture::new();
        fixture.mobilize_server();
        fixture.await_election_with_defaults().await;
        fixture.cast_vote(2, 1, true).await;
        fixture.cast_vote(6, 1, true).await;
        fixture
            .await_assume_leadership(AwaitAssumeLeadershipArgs {
                term: 1,
            })
            .await;
    });
}

#[test]
fn server_retransmits_request_vote_for_slow_voters_in_election() {
    executor::block_on(async {
        let mut fixture = Fixture::new();
        fixture.mobilize_server();
        fixture.await_election_with_defaults().await;
        let retransmission = fixture.await_retransmission(2).await;
        fixture
            .verify_vote_request(VerifyVoteRequestArgs {
                message: &retransmission,
                expected_last_log_term: 0,
                expected_last_log_index: 0,
                expected_seq: None,
                expected_term: 1,
            })
            .unwrap_or_else(|_| {
                panic!(
                    "Expected request vote message, got {:?} instead",
                    retransmission
                )
            });
    });
}

#[test]
fn timeout_before_majority_vote_or_new_leader_heart_beat() {
    executor::block_on(async {
        let mut fixture = Fixture::new();
        fixture.mobilize_server();
        fixture.await_election_with_defaults().await;
        fixture.cast_vote(6, 1, false).await;
        fixture.cast_vote(7, 1, false).await;
        fixture.cast_vote(11, 1, true).await;
        fixture
            .await_election(AwaitElectionTimeoutArgs {
                expected_cancellations: 0,
                last_log_term: 0,
                last_log_index: 0,
                term: 2,
            })
            .await;
    });
}
