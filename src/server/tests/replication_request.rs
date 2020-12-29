use super::*;
use crate::tests::assert_logger;
use futures::executor;

#[test]
fn leader_sends_no_op_log_entry_upon_election() {
    assert_logger();
    executor::block_on(async {
        let mut fixture = Fixture::new();
        let (mock_log, mock_log_back_end) =
            new_mock_log_with_non_defaults(0, 0, []);
        fixture.mobilize_server_with_log(Box::new(mock_log));
        fixture.expect_election_with_defaults().await;
        fixture.cast_votes(1, 1).await;
        fixture.expect_election_state_change(ServerElectionState::Leader).await;
        fixture
            .expect_log_entries_broadcast(AwaitAppendEntriesArgs {
                term: 1,
                leader_commit: 0,
                prev_log_term: 0,
                prev_log_index: 0,
                log: vec![LogEntry {
                    term: 1,
                    command: None,
                }],
            })
            .await;
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
    });
}

#[test]
fn leader_retransmit_append_entries() {
    assert_logger();
    executor::block_on(async {
        let mut fixture = Fixture::new();
        fixture.mobilize_server();
        fixture.expect_election_with_defaults().await;
        fixture.expect_retransmission_timer_registration(2).await;
        fixture.cast_votes(1, 1).await;
        fixture.expect_election_state_change(ServerElectionState::Leader).await;
        fixture.expect_message(2).await;
        assert_eq!(
            Message {
                content: MessageContent::AppendEntries(AppendEntriesContent {
                    leader_commit: 0,
                    prev_log_term: 0,
                    prev_log_index: 0,
                    log: vec![LogEntry {
                        term: 1,
                        command: None,
                    }],
                }),
                seq: 2,
                term: 1,
            },
            fixture.expect_retransmission(2).await
        );
    });
}

#[test]
fn leader_no_retransmit_append_entries_after_response() {
    assert_logger();
    executor::block_on(async {
        let mut fixture = Fixture::new();
        fixture.mobilize_server();
        fixture.expect_election_with_defaults().await;
        fixture.expect_retransmission_timer_registration(2).await;
        fixture.cast_votes(1, 1).await;
        fixture.expect_election_state_change(ServerElectionState::Leader).await;
        fixture.expect_message(2).await;
        fixture
            .send_server_message(
                Message {
                    content: MessageContent::AppendEntriesResults {
                        match_index: 1,
                    },
                    seq: 2,
                    term: 1,
                },
                2,
            )
            .await;
        let (_, completer) =
            fixture.expect_retransmission_timer_registration(2).await;
        let (sender, _receiver) = oneshot::channel();
        assert!(
            completer.send(sender).is_err(),
            "server didn't cancel retransmission timer"
        );
    });
}

#[test]
fn leader_ignore_append_entries_old_term() {
    assert_logger();
    executor::block_on(async {
        let mut fixture = Fixture::new();
        fixture.mobilize_server();
        fixture.expect_election_with_defaults().await;
        fixture.expect_retransmission_timer_registration(2).await;
        fixture.cast_votes(1, 1).await;
        fixture.expect_election_state_change(ServerElectionState::Leader).await;
        fixture.expect_message(2).await;
        fixture
            .send_server_message(
                Message {
                    content: MessageContent::AppendEntriesResults {
                        match_index: 1,
                    },
                    seq: 2,
                    term: 0,
                },
                2,
            )
            .await;
        assert_eq!(
            Message {
                content: MessageContent::AppendEntries(AppendEntriesContent {
                    leader_commit: 0,
                    prev_log_term: 0,
                    prev_log_index: 0,
                    log: vec![LogEntry {
                        term: 1,
                        command: None,
                    }],
                }),
                seq: 2,
                term: 1,
            },
            fixture.expect_retransmission(2).await
        );
    });
}

#[test]
fn leader_revert_to_follower_when_receive_new_term_append_entries_results() {
    assert_logger();
    executor::block_on(async {
        let mut fixture = Fixture::new();
        let (mock_persistent_storage, mock_persistent_storage_back_end) =
            new_mock_persistent_storage_with_non_defaults(0, None);
        fixture.mobilize_server_with_persistent_storage(Box::new(
            mock_persistent_storage,
        ));
        fixture.expect_election_with_defaults().await;
        fixture.expect_retransmission_timer_registration(6).await;
        fixture.cast_votes(1, 1).await;
        fixture.expect_election_state_change(ServerElectionState::Leader).await;
        fixture
            .send_server_message(
                Message {
                    content: MessageContent::AppendEntriesResults {
                        match_index: 0,
                    },
                    seq: 2,
                    term: 2,
                },
                2,
            )
            .await;
        fixture
            .expect_election_state_change(ServerElectionState::Follower)
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
fn leader_commit_entry_when_majority_match() {
    assert_logger();
    executor::block_on(async {
        let mut fixture = Fixture::new();
        fixture.mobilize_server();
        fixture.expect_election_with_defaults().await;
        fixture.cast_votes(1, 1).await;
        fixture.expect_election_state_change(ServerElectionState::Leader).await;
        fixture
            .send_server_message(
                Message {
                    content: MessageContent::AppendEntriesResults {
                        match_index: 1,
                    },
                    seq: 2,
                    term: 1,
                },
                2,
            )
            .await;
        fixture.expect_no_commit().await;
        fixture
            .send_server_message(
                Message {
                    content: MessageContent::AppendEntriesResults {
                        match_index: 1,
                    },
                    seq: 2,
                    term: 1,
                },
                6,
            )
            .await;
        fixture.expect_commit(1).await;
        fixture.trigger_heartbeat_timeout().await;
        assert_eq!(
            Message {
                content: MessageContent::AppendEntries(AppendEntriesContent {
                    leader_commit: 1,
                    prev_log_term: 1,
                    prev_log_index: 1,
                    log: vec![],
                }),
                seq: 3,
                term: 1,
            },
            fixture.expect_message_now(2)
        );
    });
}

#[test]
fn leader_send_missing_entries_mid_log_on_append_entries_results() {
    assert_logger();
    executor::block_on(async {
        let mut fixture = Fixture::new();
        let (mock_persistent_storage, _mock_persistent_storage_back_end) =
            new_mock_persistent_storage_with_non_defaults(6, None);
        let (mut mock_log, _mock_log_back_end) =
            new_mock_log_with_non_defaults(4, 10, []);
        mock_log.append(Box::new(
            vec![
                LogEntry {
                    term: 5,
                    command: None,
                },
                LogEntry {
                    term: 6,
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
            .expect_election(AwaitElectionTimeoutArgs {
                last_log_term: 6,
                last_log_index: 12,
                term: 7,
            })
            .await;
        fixture.cast_votes(1, 7).await;
        fixture.expect_election_state_change(ServerElectionState::Leader).await;
        fixture.expect_messages(hashset! {2, 6}).await;
        fixture
            .send_server_message(
                Message {
                    content: MessageContent::AppendEntriesResults {
                        match_index: 11,
                    },
                    seq: 2,
                    term: 7,
                },
                2,
            )
            .await;
        assert_eq!(
            Message {
                content: MessageContent::AppendEntries(AppendEntriesContent {
                    leader_commit: 0,
                    prev_log_term: 5,
                    prev_log_index: 11,
                    log: vec![
                        LogEntry {
                            term: 6,
                            command: None,
                        },
                        LogEntry {
                            term: 7,
                            command: None,
                        },
                    ],
                }),
                seq: 3,
                term: 7,
            },
            fixture.expect_message(2).await
        );
        fixture
            .send_server_message(
                Message {
                    content: MessageContent::AppendEntriesResults {
                        match_index: 11,
                    },
                    seq: 2,
                    term: 7,
                },
                6,
            )
            .await;
        fixture.expect_commit(11).await;
        assert_eq!(
            Message {
                content: MessageContent::AppendEntries(AppendEntriesContent {
                    leader_commit: 11,
                    prev_log_term: 5,
                    prev_log_index: 11,
                    log: vec![
                        LogEntry {
                            term: 6,
                            command: None,
                        },
                        LogEntry {
                            term: 7,
                            command: None,
                        },
                    ],
                }),
                seq: 3,
                term: 7,
            },
            fixture.expect_message(6).await
        );
    });
}

#[test]
fn leader_send_missing_entries_all_log_on_append_entries_results() {
    assert_logger();
    executor::block_on(async {
        let mut fixture = Fixture::new();
        let (mock_persistent_storage, _mock_persistent_storage_back_end) =
            new_mock_persistent_storage_with_non_defaults(6, None);
        let (mut mock_log, _mock_log_back_end) =
            new_mock_log_with_non_defaults(4, 10, []);
        mock_log.append(Box::new(
            vec![
                LogEntry {
                    term: 5,
                    command: None,
                },
                LogEntry {
                    term: 6,
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
            .expect_election(AwaitElectionTimeoutArgs {
                last_log_term: 6,
                last_log_index: 12,
                term: 7,
            })
            .await;
        fixture.cast_votes(1, 7).await;
        fixture.expect_election_state_change(ServerElectionState::Leader).await;
        fixture.expect_message(2).await;
        fixture
            .send_server_message(
                Message {
                    content: MessageContent::AppendEntriesResults {
                        match_index: 10,
                    },
                    seq: 2,
                    term: 7,
                },
                2,
            )
            .await;
        assert_eq!(
            Message {
                content: MessageContent::AppendEntries(AppendEntriesContent {
                    leader_commit: 0,
                    prev_log_term: 4,
                    prev_log_index: 10,
                    log: vec![
                        LogEntry {
                            term: 5,
                            command: None,
                        },
                        LogEntry {
                            term: 6,
                            command: None,
                        },
                        LogEntry {
                            term: 7,
                            command: None,
                        },
                    ],
                }),
                seq: 3,
                term: 7,
            },
            fixture.expect_message(2).await
        );
    });
}

#[test]
fn follower_ignore_append_entries_results() {
    assert_logger();
    executor::block_on(async {
        let mut fixture = Fixture::new();
        fixture.mobilize_server();
        fixture.expect_election_with_defaults().await;
        fixture.cast_votes(1, 1).await;
        fixture.expect_election_state_change(ServerElectionState::Leader).await;
        fixture
            .send_server_message(
                Message {
                    content: MessageContent::AppendEntriesResults {
                        match_index: 1,
                    },
                    seq: 2,
                    term: 1,
                },
                2,
            )
            .await;
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
                    seq: 1,
                    term: 2,
                },
                11,
            )
            .await;
        fixture
            .send_server_message(
                Message {
                    content: MessageContent::AppendEntriesResults {
                        match_index: 1,
                    },
                    seq: 2,
                    term: 1,
                },
                6,
            )
            .await;
        fixture.expect_no_commit().await;
    });
}

#[test]
fn leader_send_heartbeat_when_follower_up_to_date() {
    assert_logger();
    executor::block_on(async {
        let mut fixture = Fixture::new();
        fixture.mobilize_server();
        fixture.expect_election_with_defaults().await;
        fixture.cast_votes(1, 1).await;
        fixture.expect_election_state_change(ServerElectionState::Leader).await;
        fixture.expect_message_now(2);
        fixture
            .send_server_message(
                Message {
                    content: MessageContent::AppendEntriesResults {
                        match_index: 1,
                    },
                    seq: 2,
                    term: 1,
                },
                2,
            )
            .await;
        fixture.synchronize().await;
        fixture.trigger_heartbeat_timeout().await;
        assert_eq!(
            Message {
                content: MessageContent::AppendEntries(AppendEntriesContent {
                    leader_commit: 0,
                    prev_log_term: 1,
                    prev_log_index: 1,
                    log: vec![],
                }),
                seq: 3,
                term: 1,
            },
            fixture.expect_message_now(2)
        );
    });
}

#[test]
fn leader_no_op_non_zero_commit_index() {
    assert_logger();
    executor::block_on(async {
        let mut fixture = Fixture::new();
        fixture.mobilize_server();
        fixture.expect_election_timer_registrations(1).await;
        fixture
            .send_server_message(
                Message {
                    content: MessageContent::AppendEntries(
                        AppendEntriesContent {
                            leader_commit: 1,
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
        fixture
            .expect_election(AwaitElectionTimeoutArgs {
                last_log_term: 1,
                last_log_index: 1,
                term: 2,
            })
            .await;
        fixture.cast_votes(1, 2).await;
        fixture.expect_election_state_change(ServerElectionState::Leader).await;
        assert_eq!(
            Message {
                content: MessageContent::AppendEntries(AppendEntriesContent {
                    leader_commit: 1,
                    prev_log_term: 1,
                    prev_log_index: 1,
                    log: vec![LogEntry {
                        term: 2,
                        command: None,
                    }],
                }),
                seq: 2,
                term: 2,
            },
            fixture.expect_message_now(2)
        );
    });
}

#[test]
fn install_snapshot_if_match_index_before_base() {
    assert_logger();
    executor::block_on(async {
        let mut fixture = Fixture::new();
        let (mock_persistent_storage, _mock_persistent_storage_back_end) =
            new_mock_persistent_storage_with_non_defaults(10, None);
        let (mock_log, _mock_log_back_end) =
            new_mock_log_with_non_defaults(10, 1, [1, 2, 3, 4, 5]);
        fixture.mobilize_server_with_log_and_persistent_storage(
            Box::new(mock_log),
            Box::new(mock_persistent_storage),
        );
        fixture
            .expect_election(AwaitElectionTimeoutArgs {
                last_log_index: 1,
                last_log_term: 10,
                term: 11,
            })
            .await;
        fixture.cast_votes(1, 11).await;
        fixture.expect_retransmission_timer_registration(2).await;
        fixture.expect_election_state_change(ServerElectionState::Leader).await;
        fixture.expect_retransmission_timer_registration(2).await;
        fixture.expect_message_now(2);
        fixture
            .send_server_message(
                Message {
                    content: MessageContent::AppendEntriesResults {
                        match_index: 0,
                    },
                    seq: 2,
                    term: 11,
                },
                2,
            )
            .await;
        assert_eq!(
            Message {
                content: MessageContent::InstallSnapshot {
                    last_included_index: 1,
                    last_included_term: 10,
                    snapshot: vec![1, 2, 3, 4, 5],
                },
                seq: 3,
                term: 11,
            },
            fixture.expect_message(2).await
        );
        let (duration, completer) =
            fixture.expect_retransmission_timer_registration(2).await;
        assert_eq!(fixture.configuration.install_snapshot_timeout, duration);
        fixture
            .send_server_message(
                Message {
                    content: MessageContent::InstallSnapshotResults,
                    seq: 3,
                    term: 11,
                },
                2,
            )
            .await;
        fixture.synchronize().await;
        let (sender, _receiver) = oneshot::channel();
        assert!(
            completer.send(sender).is_err(),
            "server didn't cancel retransmission timer"
        );
        assert_eq!(
            Message {
                content: MessageContent::AppendEntries(AppendEntriesContent {
                    leader_commit: 0,
                    prev_log_index: 1,
                    prev_log_term: 10,
                    log: vec![LogEntry {
                        term: 11,
                        command: None
                    }],
                }),
                seq: 4,
                term: 11,
            },
            fixture.expect_message(2).await
        );
    });
}

#[test]
fn install_snapshot_ignore_results_if_term_old() {
    assert_logger();
    executor::block_on(async {
        let mut fixture = Fixture::new();
        let (mock_persistent_storage, _mock_persistent_storage_back_end) =
            new_mock_persistent_storage_with_non_defaults(10, None);
        let (mock_log, _mock_log_back_end) =
            new_mock_log_with_non_defaults(10, 1, [1, 2, 3, 4, 5]);
        fixture.mobilize_server_with_log_and_persistent_storage(
            Box::new(mock_log),
            Box::new(mock_persistent_storage),
        );
        fixture
            .expect_election(AwaitElectionTimeoutArgs {
                last_log_index: 1,
                last_log_term: 10,
                term: 11,
            })
            .await;
        fixture.cast_votes(1, 11).await;
        fixture.expect_retransmission_timer_registration(2).await;
        fixture.expect_election_state_change(ServerElectionState::Leader).await;
        fixture.expect_retransmission_timer_registration(2).await;
        fixture.expect_message_now(2);
        fixture
            .send_server_message(
                Message {
                    content: MessageContent::AppendEntriesResults {
                        match_index: 0,
                    },
                    seq: 2,
                    term: 11,
                },
                2,
            )
            .await;
        fixture.expect_message(2).await;
        let (duration, completer) =
            fixture.expect_retransmission_timer_registration(2).await;
        assert_eq!(fixture.configuration.install_snapshot_timeout, duration);
        fixture
            .send_server_message(
                Message {
                    content: MessageContent::InstallSnapshotResults,
                    seq: 3,
                    term: 10,
                },
                2,
            )
            .await;
        fixture.synchronize().await;
        let (sender, _receiver) = oneshot::channel();
        assert!(
            completer.send(sender).is_ok(),
            "server cancelled retransmission timer"
        );
        assert_eq!(
            Message {
                content: MessageContent::InstallSnapshot {
                    last_included_index: 1,
                    last_included_term: 10,
                    snapshot: vec![1, 2, 3, 4, 5],
                },
                seq: 3,
                term: 11,
            },
            fixture.expect_message(2).await
        );
    });
}

// TODO:
// * Send `AppendEntries` messages and reset heartbeat if more entries are given
//   to the leader from the host.
