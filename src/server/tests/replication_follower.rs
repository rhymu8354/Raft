use super::*;
use crate::AppendEntriesContent;
use futures::executor;

#[test]
fn follower_receive_append_entries() {
    executor::block_on(async {
        let mut fixture = Fixture::new();
        let (mock_persistent_storage, mock_persistent_storage_back_end) =
            new_mock_persistent_storage_with_non_defaults(0, None);
        let (mock_log, mock_log_back_end) =
            new_mock_log_with_non_defaults(0, 0);
        fixture.mobilize_server_with_log_and_persistent_storage(
            Box::new(mock_log),
            Box::new(mock_persistent_storage),
        );
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
            .expect_append_entries_response(AwaitAppendEntriesResponseArgs {
                expect_state_change: false,
                match_index: 1,
                receiver_id: 2,
                seq: 1,
                term: 1,
            })
            .await;
        let (election_timeout_duration, _election_timeout_completer) =
            fixture.expect_election_timer_registrations(2).await;
        assert!(
            fixture
                .configuration
                .election_timeout
                .contains(&election_timeout_duration),
            "election timeout duration {:?} is not within {:?}",
            election_timeout_duration,
            fixture.configuration.election_timeout
        );
        verify_log(&mock_log_back_end, 0, 0, &vec![LogEntry {
            term: 1,
            command: None,
        }]);
        verify_persistent_storage(&mock_persistent_storage_back_end, 1, None);
    });
}

#[test]
fn leader_revert_to_follower_on_append_entries_new_term() {
    executor::block_on(async {
        let mut fixture = Fixture::new();
        let (mock_persistent_storage, mock_persistent_storage_back_end) =
            new_mock_persistent_storage_with_non_defaults(0, None);
        let (mock_log, mock_log_back_end) =
            new_mock_log_with_non_defaults(0, 0);
        fixture.mobilize_server_with_log_and_persistent_storage(
            Box::new(mock_log),
            Box::new(mock_persistent_storage),
        );
        fixture.expect_election_with_defaults().await;
        fixture.cast_votes(1, 1).await;
        fixture.expect_election_state_change(ServerElectionState::Leader);
        fixture
            .send_server_message(
                Message {
                    content: MessageContent::AppendEntries(
                        AppendEntriesContent {
                            leader_commit: 0,
                            prev_log_index: 0,
                            prev_log_term: 0,
                            log: vec![LogEntry {
                                term: 2,
                                command: None,
                            }],
                        },
                    ),
                    seq: 42,
                    term: 2,
                },
                6,
            )
            .await;
        fixture
            .expect_append_entries_response(AwaitAppendEntriesResponseArgs {
                expect_state_change: true,
                match_index: 2,
                receiver_id: 6,
                seq: 42,
                term: 2,
            })
            .await;
        let (election_timeout_duration, _election_timeout_completer) =
            fixture.expect_election_timer_registrations(1).await;
        assert!(
            fixture
                .configuration
                .election_timeout
                .contains(&election_timeout_duration),
            "election timeout duration {:?} is not within {:?}",
            election_timeout_duration,
            fixture.configuration.election_timeout
        );
        verify_log(&mock_log_back_end, 0, 0, &vec![
            LogEntry {
                term: 1,
                command: None,
            },
            LogEntry {
                term: 2,
                command: None,
            },
        ]);
        verify_persistent_storage(&mock_persistent_storage_back_end, 2, None);
    });
}

#[test]
fn leader_reject_append_entries_same_term() {
    executor::block_on(async {
        let mut fixture = Fixture::new();
        let (mock_persistent_storage, mock_persistent_storage_back_end) =
            new_mock_persistent_storage_with_non_defaults(0, None);
        let (mock_log, mock_log_back_end) =
            new_mock_log_with_non_defaults(0, 0);
        fixture.mobilize_server_with_log_and_persistent_storage(
            Box::new(mock_log),
            Box::new(mock_persistent_storage),
        );
        fixture.expect_election_with_defaults().await;
        fixture.cast_votes(1, 1).await;
        fixture.expect_election_state_change(ServerElectionState::Leader);
        fixture
            .send_server_message(
                Message {
                    content: MessageContent::AppendEntries(
                        AppendEntriesContent {
                            leader_commit: 0,
                            prev_log_index: 1,
                            prev_log_term: 0,
                            log: vec![LogEntry {
                                term: 1,
                                command: None,
                            }],
                        },
                    ),
                    seq: 42,
                    term: 1,
                },
                6,
            )
            .await;
        fixture
            .expect_append_entries_response(AwaitAppendEntriesResponseArgs {
                expect_state_change: false,
                match_index: 0,
                receiver_id: 6,
                seq: 42,
                term: 1,
            })
            .await;
        fixture.expect_no_election_state_changes();
        verify_log(&mock_log_back_end, 0, 0, &vec![LogEntry {
            term: 1,
            command: None,
        }]);
        verify_persistent_storage(
            &mock_persistent_storage_back_end,
            1,
            Some(fixture.id),
        );
    });
}

#[test]
fn candidate_append_entries_same_term() {
    executor::block_on(async {
        let mut fixture = Fixture::new();
        let (mock_persistent_storage, mock_persistent_storage_back_end) =
            new_mock_persistent_storage_with_non_defaults(0, None);
        let (mock_log, mock_log_back_end) =
            new_mock_log_with_non_defaults(0, 0);
        fixture.mobilize_server_with_log_and_persistent_storage(
            Box::new(mock_log),
            Box::new(mock_persistent_storage),
        );
        fixture.expect_election_with_defaults().await;
        fixture
            .send_server_message(
                Message {
                    content: MessageContent::AppendEntries(
                        AppendEntriesContent {
                            leader_commit: 0,
                            prev_log_index: 1,
                            prev_log_term: 0,
                            log: vec![LogEntry {
                                term: 1,
                                command: None,
                            }],
                        },
                    ),
                    seq: 42,
                    term: 1,
                },
                6,
            )
            .await;
        fixture
            .expect_append_entries_response(AwaitAppendEntriesResponseArgs {
                expect_state_change: true,
                match_index: 1,
                receiver_id: 6,
                seq: 42,
                term: 1,
            })
            .await;
        fixture.expect_no_election_state_changes();
        verify_log(&mock_log_back_end, 0, 0, &vec![LogEntry {
            term: 1,
            command: None,
        }]);
        verify_persistent_storage(
            &mock_persistent_storage_back_end,
            1,
            Some(fixture.id),
        );
    });
}
