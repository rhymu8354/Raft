use super::*;
use crate::{
    tests::assert_logger,
    AppendEntriesContent,
};
use futures::executor;

#[test]
fn follower_receive_append_entries() {
    assert_logger();
    executor::block_on(async {
        let mut fixture = Fixture::new();
        let (mock_persistent_storage, mock_persistent_storage_back_end) =
            new_mock_persistent_storage_with_non_defaults(0, None);
        let (mock_log, mock_log_back_end) =
            new_mock_log_with_non_defaults(0, 0, []);
        fixture.mobilize_server_with_log_and_persistent_storage(
            Box::new(mock_log),
            Box::new(mock_persistent_storage),
        );
        fixture
            .send_server_message(
                Message {
                    content: MessageContent::AppendEntries(
                        AppendEntriesContent {
                            leader_commit: 10,
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
                commit_index: Some(1),
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
        verify_log(
            &mock_log_back_end,
            0,
            0,
            [LogEntry {
                term: 1,
                command: None,
            }],
            [],
        );
        verify_persistent_storage(&mock_persistent_storage_back_end, 1, None);
    });
}

#[test]
fn leader_revert_to_follower_on_append_entries_new_term() {
    assert_logger();
    executor::block_on(async {
        let mut fixture = Fixture::new();
        let (mock_persistent_storage, mock_persistent_storage_back_end) =
            new_mock_persistent_storage_with_non_defaults(0, None);
        let (mock_log, mock_log_back_end) =
            new_mock_log_with_non_defaults(0, 0, []);
        fixture.mobilize_server_with_log_and_persistent_storage(
            Box::new(mock_log),
            Box::new(mock_persistent_storage),
        );
        fixture.expect_election_with_defaults().await;
        fixture.expect_election_timer_registrations(1).await;
        fixture.cast_votes(1, 1).await;
        fixture.expect_election_state_change(ServerElectionState::Leader).await;
        fixture
            .send_server_message(
                Message {
                    content: MessageContent::AppendEntries(
                        AppendEntriesContent {
                            leader_commit: 0,
                            prev_log_index: 1,
                            prev_log_term: 1,
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
                commit_index: None,
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
        verify_log(
            &mock_log_back_end,
            0,
            0,
            [
                LogEntry {
                    term: 1,
                    command: None,
                },
                LogEntry {
                    term: 2,
                    command: None,
                },
            ],
            [],
        );
        verify_persistent_storage(&mock_persistent_storage_back_end, 2, None);
    });
}

#[test]
fn leader_reject_append_entries_same_term() {
    assert_logger();
    executor::block_on(async {
        let mut fixture = Fixture::new();
        let (mock_persistent_storage, mock_persistent_storage_back_end) =
            new_mock_persistent_storage_with_non_defaults(0, None);
        let (mock_log, mock_log_back_end) =
            new_mock_log_with_non_defaults(0, 0, []);
        fixture.mobilize_server_with_log_and_persistent_storage(
            Box::new(mock_log),
            Box::new(mock_persistent_storage),
        );
        fixture.expect_election_with_defaults().await;
        fixture.cast_votes(1, 1).await;
        fixture.expect_election_state_change(ServerElectionState::Leader).await;
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
                commit_index: None,
                expect_state_change: false,
                match_index: 0,
                receiver_id: 6,
                seq: 42,
                term: 1,
            })
            .await;
        fixture.expect_no_election_state_changes_now();
        verify_log(
            &mock_log_back_end,
            0,
            0,
            [LogEntry {
                term: 1,
                command: None,
            }],
            [],
        );
        verify_persistent_storage(
            &mock_persistent_storage_back_end,
            1,
            Some(fixture.id),
        );
    });
}

#[test]
fn candidate_append_entries_same_term() {
    assert_logger();
    executor::block_on(async {
        let mut fixture = Fixture::new();
        let (mock_persistent_storage, mock_persistent_storage_back_end) =
            new_mock_persistent_storage_with_non_defaults(0, None);
        let (mock_log, mock_log_back_end) =
            new_mock_log_with_non_defaults(0, 0, []);
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
                            prev_log_index: 0,
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
                commit_index: None,
                expect_state_change: true,
                match_index: 1,
                receiver_id: 6,
                seq: 42,
                term: 1,
            })
            .await;
        fixture.expect_no_election_state_changes_now();
        verify_log(
            &mock_log_back_end,
            0,
            0,
            [LogEntry {
                term: 1,
                command: None,
            }],
            [],
        );
        verify_persistent_storage(
            &mock_persistent_storage_back_end,
            1,
            Some(fixture.id),
        );
    });
}

#[test]
fn follower_match_appended_entries() {
    assert_logger();
    executor::block_on(async {
        let mut fixture = Fixture::new();
        let (mock_persistent_storage, mock_persistent_storage_back_end) =
            new_mock_persistent_storage_with_non_defaults(0, None);
        let (mut mock_log, mock_log_back_end) =
            new_mock_log_with_non_defaults(0, 0, []);
        mock_log.append_one(LogEntry {
            term: 1,
            command: None,
        });
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
                            log: vec![
                                LogEntry {
                                    term: 1,
                                    command: None,
                                },
                                LogEntry {
                                    term: 2,
                                    command: None,
                                },
                            ],
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
                commit_index: None,
                expect_state_change: false,
                match_index: 2,
                receiver_id: 6,
                seq: 42,
                term: 2,
            })
            .await;
        verify_log(
            &mock_log_back_end,
            0,
            0,
            [
                LogEntry {
                    term: 1,
                    command: None,
                },
                LogEntry {
                    term: 2,
                    command: None,
                },
            ],
            [],
        );
        verify_persistent_storage(&mock_persistent_storage_back_end, 2, None);
    });
}

#[test]
fn follower_replaces_mismatched_appended_entries() {
    assert_logger();
    executor::block_on(async {
        let mut fixture = Fixture::new();
        let (mock_persistent_storage, mock_persistent_storage_back_end) =
            new_mock_persistent_storage_with_non_defaults(0, None);
        let (mut mock_log, mock_log_back_end) =
            new_mock_log_with_non_defaults(0, 0, []);
        mock_log.append_one(LogEntry {
            term: 1,
            command: None,
        });
        fixture.mobilize_server_with_log_and_persistent_storage(
            Box::new(mock_log),
            Box::new(mock_persistent_storage),
        );
        fixture
            .send_server_message(
                Message {
                    content: MessageContent::AppendEntries(
                        AppendEntriesContent {
                            leader_commit: 1,
                            prev_log_index: 0,
                            prev_log_term: 0,
                            log: vec![
                                LogEntry {
                                    term: 2,
                                    command: None,
                                },
                                LogEntry {
                                    term: 3,
                                    command: None,
                                },
                            ],
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
                commit_index: Some(1),
                expect_state_change: false,
                match_index: 2,
                receiver_id: 6,
                seq: 42,
                term: 2,
            })
            .await;
        verify_log(
            &mock_log_back_end,
            0,
            0,
            [
                LogEntry {
                    term: 2,
                    command: None,
                },
                LogEntry {
                    term: 3,
                    command: None,
                },
            ],
            [],
        );
        verify_persistent_storage(&mock_persistent_storage_back_end, 2, None);
    });
}

#[test]
fn follower_rejects_appended_entries_with_mismatched_previous_term() {
    assert_logger();
    executor::block_on(async {
        let mut fixture = Fixture::new();
        let (mock_persistent_storage, mock_persistent_storage_back_end) =
            new_mock_persistent_storage_with_non_defaults(0, None);
        let (mut mock_log, mock_log_back_end) =
            new_mock_log_with_non_defaults(0, 0, []);
        mock_log.append(Box::new(
            vec![
                LogEntry {
                    term: 1,
                    command: None,
                },
                LogEntry {
                    term: 2, // <-- leader says this should be 3 not 2
                    command: None,
                },
            ]
            .into_iter(),
        ));
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
                            prev_log_index: 2,
                            prev_log_term: 3,
                            log: vec![
                                LogEntry {
                                    term: 3,
                                    command: None,
                                },
                                LogEntry {
                                    term: 4,
                                    command: None,
                                },
                            ],
                        },
                    ),
                    seq: 42,
                    term: 4,
                },
                6,
            )
            .await;
        fixture
            .expect_append_entries_response(AwaitAppendEntriesResponseArgs {
                commit_index: None,
                expect_state_change: false,
                match_index: 0,
                receiver_id: 6,
                seq: 42,
                term: 4,
            })
            .await;
        verify_log(
            &mock_log_back_end,
            0,
            0,
            [
                LogEntry {
                    term: 1,
                    command: None,
                },
                LogEntry {
                    term: 2, // <-- leader says this should be 3 not 2
                    command: None,
                },
            ],
            [],
        );
        verify_persistent_storage(&mock_persistent_storage_back_end, 4, None);
    });
}

#[test]
fn follower_rejects_appended_entries_with_mismatched_base() {
    assert_logger();
    executor::block_on(async {
        let mut fixture = Fixture::new();
        let (mock_persistent_storage, mock_persistent_storage_back_end) =
            new_mock_persistent_storage_with_non_defaults(0, None);
        let (mock_log, mock_log_back_end) =
            new_mock_log_with_non_defaults(2, 2, []);
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
                            prev_log_index: 2,
                            prev_log_term: 3,
                            log: vec![
                                LogEntry {
                                    term: 3,
                                    command: None,
                                },
                                LogEntry {
                                    term: 4,
                                    command: None,
                                },
                            ],
                        },
                    ),
                    seq: 42,
                    term: 4,
                },
                6,
            )
            .await;
        fixture
            .expect_append_entries_response(AwaitAppendEntriesResponseArgs {
                commit_index: None,
                expect_state_change: false,
                match_index: 0,
                receiver_id: 6,
                seq: 42,
                term: 4,
            })
            .await;
        verify_log(&mock_log_back_end, 2, 2, [], []);
        verify_persistent_storage(&mock_persistent_storage_back_end, 4, None);
    });
}

#[test]
fn follower_rejects_appended_entries_with_no_common_base() {
    assert_logger();
    executor::block_on(async {
        let mut fixture = Fixture::new();
        let (mock_persistent_storage, mock_persistent_storage_back_end) =
            new_mock_persistent_storage_with_non_defaults(0, None);
        let (mock_log, mock_log_back_end) =
            new_mock_log_with_non_defaults(0, 0, []);
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
                            prev_log_index: 2,
                            prev_log_term: 3,
                            log: vec![
                                LogEntry {
                                    term: 3,
                                    command: None,
                                },
                                LogEntry {
                                    term: 4,
                                    command: None,
                                },
                            ],
                        },
                    ),
                    seq: 42,
                    term: 4,
                },
                6,
            )
            .await;
        fixture
            .expect_append_entries_response(AwaitAppendEntriesResponseArgs {
                commit_index: None,
                expect_state_change: false,
                match_index: 0,
                receiver_id: 6,
                seq: 42,
                term: 4,
            })
            .await;
        verify_log(&mock_log_back_end, 0, 0, [], []);
        verify_persistent_storage(&mock_persistent_storage_back_end, 4, None);
    });
}

#[test]
fn follower_installs_snapshot() {
    assert_logger();
    executor::block_on(async {
        let mut fixture = Fixture::new();
        let (mock_persistent_storage, mock_persistent_storage_back_end) =
            new_mock_persistent_storage_with_non_defaults(0, None);
        let (mock_log, mock_log_back_end) =
            new_mock_log_with_non_defaults(0, 0, []);
        fixture.mobilize_server_with_log_and_persistent_storage(
            Box::new(mock_log),
            Box::new(mock_persistent_storage),
        );
        let (_duration, timeout) =
            fixture.expect_election_timer_registrations(1).await;
        fixture
            .send_server_message(
                Message {
                    content: MessageContent::InstallSnapshot {
                        last_included_index: 1,
                        last_included_term: 10,
                        snapshot: vec![1, 2, 3, 4, 5],
                    },
                    seq: 42,
                    term: 11,
                },
                6,
            )
            .await;
        fixture.synchronize().await;
        let (timeout_ack_sender, _timeout_ack_receiver) = oneshot::channel();
        timeout
            .send(timeout_ack_sender)
            .expect_err("server did not cancel election timer");
        fixture.expect_install_snapshot_response(6, 42, 11, false).await;
        verify_log(&mock_log_back_end, 10, 1, [], [1, 2, 3, 4, 5]);
        verify_persistent_storage(&mock_persistent_storage_back_end, 11, None);
        fixture.expect_election_timer_registrations(1).await;
    });
}
