use super::*;
use crate::{
    tests::assert_logger,
    ClusterConfiguration,
    ServerElectionState,
};
use futures::executor;

#[test]
fn new_election() {
    assert_logger();
    executor::block_on(async {
        let mut fixture = Fixture::new();
        let (mock_log, _mock_log_back_end) = new_mock_log_with_non_defaults(
            7,
            42,
            ClusterConfiguration::Single(hashset![2, 5, 6, 7, 11]),
        );
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
fn new_election_after_new_server_config() {
    assert_logger();
    executor::block_on(async {
        let mut fixture = Fixture::new();
        fixture.mobilize_server();
        fixture.expect_election_timer_registrations(1).await;
        let new_server_configuration = ServerConfiguration {
            election_timeout: Duration::from_millis(300)
                ..Duration::from_millis(400),
            heartbeat_interval: Duration::from_millis(50),
            rpc_timeout: Duration::from_millis(10),
            install_snapshot_timeout: Duration::from_secs(10),
        };
        fixture.configuration = new_server_configuration.clone();
        fixture
            .server
            .as_mut()
            .expect("no server mobilized")
            .send(Command::ReconfigureServer(new_server_configuration))
            .await
            .unwrap();
        fixture
            .send_server_message(
                Message {
                    content: MessageContent::AppendEntries(
                        AppendEntriesContent {
                            leader_commit: 0,
                            prev_log_index: 0,
                            prev_log_term: 0,
                            log: vec![LogEntry {
                                term: 1,
                                command: None,
                            }],
                        },
                    ),
                    seq: 1,
                    term: 1,
                },
                2,
            )
            .await;
        fixture.expect_leadership_change(Some(2)).await;
        fixture
            .expect_election(AwaitElectionTimeoutArgs {
                last_log_term: 1,
                last_log_index: 1,
                term: 2,
            })
            .await;
    });
}

#[test]
fn elected_leader_unanimously() {
    assert_logger();
    executor::block_on(async {
        let mut fixture = Fixture::new();
        fixture.mobilize_server();
        fixture.expect_election_with_defaults().await;
        fixture.cast_votes(1, 1).await;
        fixture.expect_assume_leadership(1).await;
        let (_election_timeout_duration, election_timeout_completer) =
            fixture.expect_election_timer_registrations(1).await;
        let (sender, _receiver) = oneshot::channel();
        election_timeout_completer
            .send(sender)
            .expect_err("server did not cancel last election timeout");
        fixture.cast_votes(2, 1).await;
        fixture.expect_no_election_state_changes().await;
        fixture.expect_no_election_timer_registrations_now();
    });
}

#[test]
fn elected_leader_non_unanimous_majority() {
    assert_logger();
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
        fixture.expect_assume_leadership(1).await;
    });
}

#[test]
fn elected_leader_does_not_process_extra_votes() {
    assert_logger();
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
        fixture.expect_assume_leadership(1).await;
        fixture
            .cast_vote(CastVoteArgs {
                sender_id: 11,
                seq: 1,
                term: 1,
                vote: true,
            })
            .await;
        fixture.expect_no_election_state_changes().await;
    });
}

#[test]
fn not_elected_leader_because_no_majority_votes() {
    assert_logger();
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
        fixture.expect_no_election_state_changes().await;
    });
}

#[test]
fn not_elected_leader_because_request_vote_responaw_old_term() {
    assert_logger();
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
                term: 0,
                vote: true,
            })
            .await;
        fixture.expect_no_election_state_changes().await;
    });
}

#[test]
fn candidate_revert_to_follower_on_request_vote_response_newer_term() {
    assert_logger();
    executor::block_on(async {
        let mut fixture = Fixture::new();
        let (mock_persistent_storage, mock_persistent_storage_back_end) =
            new_mock_persistent_storage_with_non_defaults(0, None);
        fixture.mobilize_server_with_persistent_storage(Box::new(
            mock_persistent_storage,
        ));
        fixture.expect_election_with_defaults().await;
        fixture
            .cast_vote(CastVoteArgs {
                sender_id: 2,
                seq: 1,
                term: 2,
                vote: false,
            })
            .await;
        fixture
            .expect_election_state_change(ServerElectionState::Follower, 2)
            .await;
        let (_duration, completer) =
            fixture.expect_retransmission_timer_registration(6).await;
        let (sender, _receiver) = oneshot::channel();
        assert!(
            completer.send(sender).is_err(),
            "server didn't cancel retransmission timer"
        );
        fixture.expect_election_timer_registration_now();
        verify_persistent_storage(&mock_persistent_storage_back_end, 2, None);
    });
}

#[test]
fn server_retransmits_request_vote_for_slow_voters_in_election() {
    assert_logger();
    executor::block_on(async {
        let mut fixture = Fixture::new();
        fixture.mobilize_server();
        fixture.expect_election_with_defaults().await;
        let retransmission = fixture.expect_retransmission(2).await;
        assert!(
            Fixture::is_verified_vote_request(&VerifyVoteRequestArgs {
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
    assert_logger();
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
        let retransmission = fixture.expect_retransmission(2).await;
        assert!(
            Fixture::is_verified_vote_request(&VerifyVoteRequestArgs {
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
    assert_logger();
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
        fixture.expect_assume_leadership(2).await;
    });
}

#[test]
fn leader_no_retransmit_vote_request_after_election() {
    assert_logger();
    executor::block_on(async {
        let mut fixture = Fixture::new();
        fixture.mobilize_server();
        fixture.expect_election_with_defaults().await;
        let mut completers = fixture
            .expect_retransmission_timer_registrations_now(
                [2, 11].iter().copied(),
            );
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
        fixture.expect_assume_leadership(1).await;
        let (sender, _receiver) = oneshot::channel();
        assert!(
            completers.remove(&2).unwrap().send(sender).is_err(),
            "server didn't cancel retransmission timer for server 2"
        );
        let (sender, _receiver) = oneshot::channel();
        assert!(
            completers.remove(&11).unwrap().send(sender).is_err(),
            "server didn't cancel retransmission timer for server 11"
        );
    });
}

#[test]
fn no_election_if_not_voting_member() {
    assert_logger();
    executor::block_on(async {
        let mut fixture = Fixture::new();
        let (mock_log, _mock_log_back_end) = new_mock_log_with_non_defaults(
            0,
            0,
            ClusterConfiguration::Single(hashset![2, 6, 7, 11]),
        );
        fixture.mobilize_server_with_log(Box::new(mock_log));
        fixture.expect_no_election_timer_registrations().await;
    });
}

#[test]
fn cancel_election_timer_if_become_non_voting_member() {
    assert_logger();
    executor::block_on(async {
        let mut fixture = Fixture::new();
        fixture.mobilize_server();
        let (_election_timeout_duration, election_timeout_completer) =
            fixture.expect_election_timer_registrations(1).await;
        fixture
            .send_server_message(
                Message {
                    content: MessageContent::AppendEntries(
                        AppendEntriesContent {
                            leader_commit: 0,
                            prev_log_index: 0,
                            prev_log_term: 0,
                            log: vec![
                                LogEntry {
                                    term: 1,
                                    command: Some(
                                        LogEntryCommand::StartReconfiguration(
                                            hashset![2, 6, 7, 11],
                                        ),
                                    ),
                                },
                                LogEntry {
                                    term: 1,
                                    command: Some(
                                        LogEntryCommand::FinishReconfiguration,
                                    ),
                                },
                            ],
                        },
                    ),
                    seq: 1,
                    term: 1,
                },
                2,
            )
            .await;
        fixture.synchronize().await;
        let (sender, _receiver) = oneshot::channel();
        election_timeout_completer
            .send(sender)
            .expect_err("server did not cancel last election timeout");
        fixture.expect_no_election_timer_registrations().await;
    });
}

#[test]
fn start_election_timer_if_become_voting_member() {
    assert_logger();
    executor::block_on(async {
        let mut fixture = Fixture::new();
        let (mock_log, _mock_log_back_end) = new_mock_log_with_non_defaults(
            0,
            0,
            ClusterConfiguration::Single(hashset![2, 6, 7, 11]),
        );
        fixture.mobilize_server_with_log(Box::new(mock_log));
        fixture.expect_no_election_timer_registrations().await;
        fixture
            .send_server_message(
                Message {
                    content: MessageContent::AppendEntries(
                        AppendEntriesContent {
                            leader_commit: 0,
                            prev_log_index: 0,
                            prev_log_term: 0,
                            log: vec![
                                LogEntry {
                                    term: 1,
                                    command: Some(
                                        LogEntryCommand::StartReconfiguration(
                                            hashset![2, 5, 6, 7, 11],
                                        ),
                                    ),
                                },
                                LogEntry {
                                    term: 1,
                                    command: Some(
                                        LogEntryCommand::FinishReconfiguration,
                                    ),
                                },
                            ],
                        },
                    ),
                    seq: 1,
                    term: 1,
                },
                2,
            )
            .await;
        fixture.expect_election_timer_registrations(1).await;
    });
}

#[test]
fn election_during_joint_configuration_requires_separate_majorities() {
    assert_logger();
    executor::block_on(async {
        let mut fixture = Fixture::new();
        let (mock_log, _mock_log_back_end) =
            new_mock_log_with_non_defaults(0, 1, ClusterConfiguration::Joint {
                old_ids: hashset![5, 6, 7, 11],
                new_ids: hashset![2, 5, 8, 10],
                index: 1,
            });
        fixture.mobilize_server_with_log(Box::new(mock_log));
        fixture
            .expect_election(AwaitElectionTimeoutArgs {
                last_log_term: 0,
                last_log_index: 1,
                term: 1,
            })
            .await;
        fixture
            .cast_vote(CastVoteArgs {
                sender_id: 2,
                seq: 1,
                term: 1,
                vote: true,
            })
            .await;
        fixture.expect_no_election_state_changes().await;
        fixture
            .cast_vote(CastVoteArgs {
                sender_id: 6,
                seq: 1,
                term: 1,
                vote: true,
            })
            .await;
        fixture.expect_assume_leadership(1).await;
    });
}
