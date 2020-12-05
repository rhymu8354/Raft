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
            .expect_election(AwaitElectionTimeoutArgs {
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
        fixture.expect_election_with_defaults().await;
        fixture.cast_votes(1, 1).await;
        fixture
            .expect_assume_leadership(AwaitAssumeLeadershipArgs {
                term: 1,
            })
            .await;
        let (_election_timeout_duration, election_timeout_completer) =
            fixture.expect_election_timer_registrations(1).await;
        election_timeout_completer
            .send(())
            .expect_err("server did not cancel last election timeout");
        fixture.cast_votes(2, 2).await;
        fixture.expect_no_election_state_changes();
        assert_eq!(1, fixture.server.election_timeout_count().await);
    });
}

#[test]
fn elected_leader_non_unanimous_majority() {
    executor::block_on(async {
        let mut fixture = Fixture::new();
        fixture.mobilize_server();
        fixture.expect_election_with_defaults().await;
        fixture
            .cast_vote(CastVoteArgs {
                sender_id: 2,
                seq: 1,
                term: 1,
                vote: true,
            })
            .await;
        fixture
            .cast_vote(CastVoteArgs {
                sender_id: 6,
                seq: 1,
                term: 1,
                vote: true,
            })
            .await;
        fixture
            .expect_assume_leadership(AwaitAssumeLeadershipArgs {
                term: 1,
            })
            .await;
    });
}

#[test]
fn elected_leader_does_not_process_extra_votes() {
    executor::block_on(async {
        let mut fixture = Fixture::new();
        fixture.mobilize_server();
        fixture.expect_election_with_defaults().await;
        fixture
            .cast_vote(CastVoteArgs {
                sender_id: 2,
                seq: 1,
                term: 1,
                vote: true,
            })
            .await;
        fixture
            .cast_vote(CastVoteArgs {
                sender_id: 6,
                seq: 1,
                term: 1,
                vote: true,
            })
            .await;
        fixture
            .expect_assume_leadership(AwaitAssumeLeadershipArgs {
                term: 1,
            })
            .await;
        fixture
            .cast_vote(CastVoteArgs {
                sender_id: 11,
                seq: 1,
                term: 1,
                vote: true,
            })
            .await;
        fixture.expect_no_election_state_changes();
    });
}

#[test]
fn not_elected_leader_because_no_majority_votes() {
    executor::block_on(async {
        let mut fixture = Fixture::new();
        fixture.mobilize_server();
        fixture.expect_election_with_defaults().await;
        fixture
            .cast_vote(CastVoteArgs {
                sender_id: 2,
                seq: 1,
                term: 1,
                vote: true,
            })
            .await;
        fixture
            .cast_vote(CastVoteArgs {
                sender_id: 6,
                seq: 1,
                term: 1,
                vote: false,
            })
            .await;
    });
}

#[test]
fn server_retransmits_request_vote_for_slow_voters_in_election() {
    executor::block_on(async {
        let mut fixture = Fixture::new();
        fixture.mobilize_server();
        fixture.expect_election_with_defaults().await;
        let retransmission = fixture.await_retransmission(2).await;
        assert!(
            fixture.is_verified_vote_request(VerifyVoteRequestArgs {
                message: &retransmission,
                expected_last_log_term: 0,
                expected_last_log_index: 0,
                expected_seq: Some(1),
                expected_term: 1,
            }),
            "Expected request vote message, got {:?} instead",
            retransmission
        );
    });
}

#[test]
fn server_retransmits_request_vote_if_vote_had_wrong_seq() {
    executor::block_on(async {
        let mut fixture = Fixture::new();
        fixture.mobilize_server();
        fixture.expect_election_with_defaults().await;
        fixture
            .cast_vote(CastVoteArgs {
                sender_id: 2,
                seq: 42,
                term: 1,
                vote: true,
            })
            .await;
        let retransmission = fixture.await_retransmission(2).await;
        assert!(
            fixture.is_verified_vote_request(VerifyVoteRequestArgs {
                message: &retransmission,
                expected_last_log_term: 0,
                expected_last_log_index: 0,
                expected_seq: Some(1),
                expected_term: 1,
            }),
            "Expected request vote message, got {:?} instead",
            retransmission
        );
    });
}

#[test]
fn timeout_before_majority_vote_or_new_leader_heart_beat() {
    executor::block_on(async {
        let mut fixture = Fixture::new();
        fixture.mobilize_server();
        fixture.expect_election_with_defaults().await;
        fixture
            .cast_vote(CastVoteArgs {
                sender_id: 6,
                seq: 1,
                term: 1,
                vote: false,
            })
            .await;
        fixture
            .cast_vote(CastVoteArgs {
                sender_id: 7,
                seq: 1,
                term: 1,
                vote: false,
            })
            .await;
        fixture
            .cast_vote(CastVoteArgs {
                sender_id: 11,
                seq: 1,
                term: 1,
                vote: true,
            })
            .await;
        fixture
            .expect_election(AwaitElectionTimeoutArgs {
                last_log_term: 0,
                last_log_index: 0,
                term: 2,
            })
            .await;
        fixture.cast_votes(2, 2).await;
        fixture
            .expect_assume_leadership(AwaitAssumeLeadershipArgs {
                term: 2,
            })
            .await;
    });
}
