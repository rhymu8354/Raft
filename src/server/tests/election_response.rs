use super::*;
use crate::{
    tests::assert_logger,
    ClusterConfiguration,
};
use futures::executor;

#[test]
fn follower_votes_for_first_candidate() {
    assert_logger();
    executor::block_on(async {
        let mut fixture = Fixture::new();
        let (mock_persistent_storage, mock_persistent_storage_back_end) =
            new_mock_persistent_storage_with_non_defaults(0, None);
        fixture.mobilize_server_with_persistent_storage(Box::new(
            mock_persistent_storage,
        ));
        fixture
            .receive_vote_request(ReceiveVoteRequestArgs {
                sender_id: 6,
                last_log_term: 7,
                last_log_index: 42,
                seq: 1,
                term: 1,
            })
            .await;
        fixture
            .expect_vote(&AwaitVoteArgs {
                expect_state_change: false,
                receiver_id: 6,
                seq: 1,
                term: 1,
                vote_granted: true,
            })
            .await;
        verify_persistent_storage(
            &mock_persistent_storage_back_end,
            1,
            Some(6),
        );
    });
}

#[test]
fn follower_rejects_subsequent_votes_after_first_candidate_same_term() {
    assert_logger();
    executor::block_on(async {
        let mut fixture = Fixture::new();
        let (mock_persistent_storage, mock_persistent_storage_back_end) =
            new_mock_persistent_storage_with_non_defaults(1, Some(11));
        fixture.mobilize_server_with_persistent_storage(Box::new(
            mock_persistent_storage,
        ));
        fixture
            .receive_vote_request(ReceiveVoteRequestArgs {
                sender_id: 6,
                last_log_term: 7,
                last_log_index: 42,
                seq: 1,
                term: 1,
            })
            .await;
        fixture
            .expect_vote(&AwaitVoteArgs {
                expect_state_change: false,
                receiver_id: 6,
                seq: 1,
                term: 1,
                vote_granted: false,
            })
            .await;
        verify_persistent_storage(
            &mock_persistent_storage_back_end,
            1,
            Some(11),
        );
    });
}

#[test]
fn follower_affirm_vote_upon_repeated_request() {
    assert_logger();
    executor::block_on(async {
        let mut fixture = Fixture::new();
        let (mock_persistent_storage, mock_persistent_storage_back_end) =
            new_mock_persistent_storage_with_non_defaults(1, Some(6));
        fixture.mobilize_server_with_persistent_storage(Box::new(
            mock_persistent_storage,
        ));
        fixture
            .receive_vote_request(ReceiveVoteRequestArgs {
                sender_id: 6,
                last_log_term: 7,
                last_log_index: 42,
                seq: 1,
                term: 1,
            })
            .await;
        fixture
            .expect_vote(&AwaitVoteArgs {
                expect_state_change: false,
                receiver_id: 6,
                seq: 1,
                term: 1,
                vote_granted: true,
            })
            .await;
        verify_persistent_storage(
            &mock_persistent_storage_back_end,
            1,
            Some(6),
        );
    });
}

#[test]
fn follower_rejects_vote_from_old_term() {
    assert_logger();
    executor::block_on(async {
        let mut fixture = Fixture::new();
        let (mock_persistent_storage, mock_persistent_storage_back_end) =
            new_mock_persistent_storage_with_non_defaults(2, None);
        fixture.mobilize_server_with_persistent_storage(Box::new(
            mock_persistent_storage,
        ));
        fixture
            .receive_vote_request(ReceiveVoteRequestArgs {
                sender_id: 6,
                last_log_term: 7,
                last_log_index: 42,
                seq: 1,
                term: 1,
            })
            .await;
        fixture
            .expect_vote(&AwaitVoteArgs {
                expect_state_change: false,
                receiver_id: 6,
                seq: 1,
                term: 2,
                vote_granted: false,
            })
            .await;
        verify_persistent_storage(&mock_persistent_storage_back_end, 2, None);
    });
}

#[test]
fn non_follower_rejects_vote_from_same_term() {
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
            .receive_vote_request(ReceiveVoteRequestArgs {
                sender_id: 6,
                last_log_term: 7,
                last_log_index: 42,
                seq: 1,
                term: 1,
            })
            .await;
        fixture
            .expect_vote(&AwaitVoteArgs {
                expect_state_change: false,
                receiver_id: 6,
                seq: 1,
                term: 1,
                vote_granted: false,
            })
            .await;
        verify_persistent_storage(
            &mock_persistent_storage_back_end,
            1,
            Some(fixture.id),
        );
    });
}

#[test]
fn non_follower_revert_to_follower_and_vote_for_new_term_candidate() {
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
            .receive_vote_request(ReceiveVoteRequestArgs {
                sender_id: 6,
                last_log_term: 7,
                last_log_index: 42,
                seq: 1,
                term: 2,
            })
            .await;
        fixture
            .expect_vote(&AwaitVoteArgs {
                expect_state_change: true,
                receiver_id: 6,
                seq: 1,
                term: 2,
                vote_granted: true,
            })
            .await;
        verify_persistent_storage(
            &mock_persistent_storage_back_end,
            2,
            Some(6),
        );
        fixture.expect_election_timer_registrations(1).await;
    });
}

#[test]
fn vote_rejected_if_candidate_log_old() {
    assert_logger();
    executor::block_on(async {
        let combinations: &[(usize, usize, usize, usize)] =
            &[(1, 199, 1, 42), (2, 199, 1, 399), (2, 199, 1, 42)];
        for (our_term, our_index, their_term, their_index) in combinations {
            let mut fixture = Fixture::new();
            let (mock_log, _mock_log_back_end) = new_mock_log_with_non_defaults(
                *our_term,
                *our_index,
                ClusterConfiguration::Single(hashset![2, 5, 6, 7, 11]),
            );
            fixture.mobilize_server_with_log(Box::new(mock_log));
            fixture
                .receive_vote_request(ReceiveVoteRequestArgs {
                    sender_id: 6,
                    last_log_term: *their_term,
                    last_log_index: *their_index,
                    seq: 1,
                    term: 1,
                })
                .await;
            fixture
                .expect_vote(&AwaitVoteArgs {
                    expect_state_change: false,
                    receiver_id: 6,
                    seq: 1,
                    term: 0,
                    vote_granted: false,
                })
                .await;
        }
    });
}

#[test]
fn follower_ignores_vote_request_within_minimum_election_time_if_leader_known()
{
    assert_logger();
    executor::block_on(async {
        let mut fixture = Fixture::new();
        fixture.mobilize_server();
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
        fixture
            .receive_vote_request(ReceiveVoteRequestArgs {
                sender_id: 6,
                last_log_term: 7,
                last_log_index: 42,
                seq: 1,
                term: 1,
            })
            .await;
        fixture.expect_no_vote().await;
    });
}

#[test]
fn follower_does_not_ignore_vote_request_past_minimum_election_time_if_leader_known(
) {
    assert_logger();
    executor::block_on(async {
        let mut fixture = Fixture::new();
        fixture.mobilize_server();
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
        fixture.trigger_min_election_timeout().await;
        fixture
            .receive_vote_request(ReceiveVoteRequestArgs {
                sender_id: 6,
                last_log_term: 7,
                last_log_index: 42,
                seq: 1,
                term: 1,
            })
            .await;
        fixture
            .expect_vote(&AwaitVoteArgs {
                expect_state_change: false,
                receiver_id: 6,
                seq: 1,
                term: 1,
                vote_granted: true,
            })
            .await;
    });
}

#[test]
fn follower_does_not_ignore_vote_request_within_minimum_election_time_if_leader_unknown(
) {
    assert_logger();
    executor::block_on(async {
        let mut fixture = Fixture::new();
        fixture.mobilize_server();
        fixture
            .receive_vote_request(ReceiveVoteRequestArgs {
                sender_id: 6,
                last_log_term: 7,
                last_log_index: 42,
                seq: 1,
                term: 1,
            })
            .await;
        fixture
            .expect_vote(&AwaitVoteArgs {
                expect_state_change: false,
                receiver_id: 6,
                seq: 1,
                term: 1,
                vote_granted: true,
            })
            .await;
    });
}

#[test]
fn no_vote_response_if_not_voting_member() {
    assert_logger();
    executor::block_on(async {
        let mut fixture = Fixture::new();
        let (mock_log, _mock_log_back_end) = new_mock_log_with_non_defaults(
            0,
            0,
            ClusterConfiguration::Single(hashset![2, 6, 7, 11]),
        );
        fixture.mobilize_server_with_log(Box::new(mock_log));
        fixture.expect_no_min_election_timer_registrations().await;
        fixture
            .receive_vote_request(ReceiveVoteRequestArgs {
                sender_id: 6,
                last_log_term: 0,
                last_log_index: 0,
                seq: 1,
                term: 1,
            })
            .await;
        fixture.expect_no_vote().await;
    });
}
