use super::*;
use crate::{
    tests::assert_logger,
    ClusterConfiguration,
};
use futures::executor;

#[test]
fn leader_sends_no_op_log_entry_upon_election() {
    assert_logger();
    executor::block_on(async {
        let mut fixture = Fixture::new();
        let (mock_log, mock_log_back_end) = new_mock_log_with_non_defaults(
            0,
            0,
            ClusterConfiguration::Single(hashset![2, 5, 6, 7, 11]),
        );
        fixture.mobilize_server_with_log(Box::new(mock_log));
        fixture.expect_election_with_defaults().await;
        fixture.cast_votes(1, 1).await;
        fixture.expect_election_state_change(ElectionState::Leader).await;
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
            ClusterConfiguration::Single(hashset![2, 5, 6, 7, 11]),
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
        fixture.expect_election_state_change(ElectionState::Leader).await;
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
        fixture.expect_election_state_change(ElectionState::Leader).await;
        fixture.expect_message(2).await;
        fixture
            .send_server_message(
                Message {
                    content: MessageContent::AppendEntriesResponse {
                        success: true,
                        next_log_index: 2,
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
        fixture.expect_election_state_change(ElectionState::Leader).await;
        fixture.expect_message(2).await;
        fixture
            .send_server_message(
                Message {
                    content: MessageContent::AppendEntriesResponse {
                        success: true,
                        next_log_index: 2,
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
fn leader_revert_to_follower_when_receive_new_term_append_entries_response() {
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
        fixture.expect_election_state_change(ElectionState::Leader).await;
        fixture
            .send_server_message(
                Message {
                    content: MessageContent::AppendEntriesResponse {
                        success: false,
                        next_log_index: 1,
                    },
                    seq: 2,
                    term: 2,
                },
                2,
            )
            .await;
        fixture.expect_election_state_change(ElectionState::Follower).await;
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
fn leader_commit_entry_when_majority_match_single_configuration() {
    assert_logger();
    executor::block_on(async {
        let mut fixture = Fixture::new();
        fixture.mobilize_server();
        fixture.expect_election_with_defaults().await;
        fixture.cast_votes(1, 1).await;
        fixture.expect_election_state_change(ElectionState::Leader).await;
        fixture
            .send_server_message(
                Message {
                    content: MessageContent::AppendEntriesResponse {
                        success: true,
                        next_log_index: 2,
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
                    content: MessageContent::AppendEntriesResponse {
                        success: true,
                        next_log_index: 2,
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
fn leader_commit_entry_when_majority_match_joint_configuration() {
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
        fixture.cast_votes(1, 1).await;
        fixture.expect_election_state_change(ElectionState::Leader).await;
        fixture
            .send_server_message(
                Message {
                    content: MessageContent::AppendEntriesResponse {
                        success: true,
                        next_log_index: 3,
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
                    content: MessageContent::AppendEntriesResponse {
                        success: true,
                        next_log_index: 3,
                    },
                    seq: 2,
                    term: 1,
                },
                6,
            )
            .await;
        fixture.expect_commit(2).await;
    });
}

#[test]
#[allow(clippy::too_many_lines)]
fn leader_send_missing_entries_mid_log_on_append_entries_response() {
    assert_logger();
    executor::block_on(async {
        let mut fixture = Fixture::new();
        let (mock_persistent_storage, _mock_persistent_storage_back_end) =
            new_mock_persistent_storage_with_non_defaults(6, None);
        let (mut mock_log, _mock_log_back_end) = new_mock_log_with_non_defaults(
            4,
            10,
            ClusterConfiguration::Single(hashset![2, 5, 6, 7, 11]),
        );
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
        fixture.expect_election_state_change(ElectionState::Leader).await;
        fixture.expect_messages(hashset![2, 6, 7, 11]).await;
        fixture
            .send_server_message(
                Message {
                    content: MessageContent::AppendEntriesResponse {
                        success: true,
                        next_log_index: 12,
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
                    content: MessageContent::AppendEntriesResponse {
                        success: true,
                        next_log_index: 12,
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
fn leader_send_missing_entries_all_log_on_append_entries_response() {
    assert_logger();
    executor::block_on(async {
        let mut fixture = Fixture::new();
        let (mock_persistent_storage, _mock_persistent_storage_back_end) =
            new_mock_persistent_storage_with_non_defaults(6, None);
        let (mut mock_log, _mock_log_back_end) = new_mock_log_with_non_defaults(
            4,
            10,
            ClusterConfiguration::Single(hashset![2, 5, 6, 7, 11]),
        );
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
        fixture.expect_election_state_change(ElectionState::Leader).await;
        fixture.expect_message(2).await;
        fixture
            .send_server_message(
                Message {
                    content: MessageContent::AppendEntriesResponse {
                        success: false,
                        next_log_index: 11,
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
fn follower_ignore_append_entries_response() {
    assert_logger();
    executor::block_on(async {
        let mut fixture = Fixture::new();
        fixture.mobilize_server();
        fixture.expect_election_with_defaults().await;
        fixture.cast_votes(1, 1).await;
        fixture.expect_election_state_change(ElectionState::Leader).await;
        fixture
            .send_server_message(
                Message {
                    content: MessageContent::AppendEntriesResponse {
                        success: true,
                        next_log_index: 2,
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
                    content: MessageContent::AppendEntriesResponse {
                        success: true,
                        next_log_index: 2,
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
        fixture.expect_election_state_change(ElectionState::Leader).await;
        fixture.expect_message_now(2);
        fixture
            .send_server_message(
                Message {
                    content: MessageContent::AppendEntriesResponse {
                        success: true,
                        next_log_index: 2,
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
            .expect_append_entries_response(&AwaitAppendEntriesResponseArgs {
                commit_index: Some(1),
                expect_state_change: false,
                success: true,
                next_log_index: 2,
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
        fixture.expect_election_state_change(ElectionState::Leader).await;
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
        let (mock_log, _mock_log_back_end) = new_mock_log_with_non_defaults(
            10,
            1,
            ClusterConfiguration::Single(hashset![2, 5, 6, 7, 11]),
        );
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
        fixture.expect_election_state_change(ElectionState::Leader).await;
        fixture.expect_retransmission_timer_registration(2).await;
        fixture.expect_message_now(2);
        fixture
            .send_server_message(
                Message {
                    content: MessageContent::AppendEntriesResponse {
                        success: false,
                        next_log_index: 1,
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
                    snapshot: ClusterConfiguration::Single(hashset![
                        2, 5, 6, 7, 11
                    ]),
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
                    content: MessageContent::InstallSnapshotResponse,
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
fn install_snapshot_ignore_response_if_term_old() {
    assert_logger();
    executor::block_on(async {
        let mut fixture = Fixture::new();
        let (mock_persistent_storage, _mock_persistent_storage_back_end) =
            new_mock_persistent_storage_with_non_defaults(10, None);
        let (mock_log, _mock_log_back_end) = new_mock_log_with_non_defaults(
            10,
            1,
            ClusterConfiguration::Single(hashset![2, 5, 6, 7, 11]),
        );
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
        fixture.expect_retransmission_timer_registration(2).await;
        fixture.cast_votes(1, 11).await;
        fixture.expect_election_state_change(ElectionState::Leader).await;
        fixture.expect_retransmission_timer_registration(2).await;
        fixture.expect_message(2).await;
        fixture
            .send_server_message(
                Message {
                    content: MessageContent::AppendEntriesResponse {
                        success: false,
                        next_log_index: 1,
                    },
                    seq: 2,
                    term: 11,
                },
                2,
            )
            .await;
        let (duration, completer) =
            fixture.expect_retransmission_timer_registration(2).await;
        assert_eq!(fixture.configuration.install_snapshot_timeout, duration);
        fixture.expect_message(2).await;
        fixture
            .send_server_message(
                Message {
                    content: MessageContent::InstallSnapshotResponse,
                    seq: 3,
                    term: 10,
                },
                2,
            )
            .await;
        fixture.synchronize().await;
        let (sender, receiver) = oneshot::channel();
        completer.send(sender).expect("server cancelled retransmission timer");
        receiver.await.expect(
            "server dropped retransmission timer acknowledgment sender",
        );
        assert_eq!(
            Message {
                content: MessageContent::InstallSnapshot {
                    last_included_index: 1,
                    last_included_term: 10,
                    snapshot: ClusterConfiguration::Single(hashset![
                        2, 5, 6, 7, 11
                    ]),
                },
                seq: 3,
                term: 11,
            },
            fixture.expect_message(2).await
        );
    });
}

#[test]
#[allow(clippy::too_many_lines)]
fn leader_send_new_log_entries() {
    assert_logger();
    executor::block_on(async {
        let mut fixture = Fixture::new();
        let (mock_log, mock_log_back_end) = MockLog::new();
        fixture.mobilize_server_with_log(Box::new(mock_log));
        fixture.expect_election_with_defaults().await;
        fixture.cast_votes(1, 1).await;
        fixture.expect_election_state_change(ElectionState::Leader).await;
        fixture.expect_messages_now(hashset![2, 6, 7, 11]);
        fixture
            .send_server_message(
                Message {
                    content: MessageContent::AppendEntriesResponse {
                        success: true,
                        next_log_index: 2,
                    },
                    seq: 2,
                    term: 1,
                },
                2,
            )
            .await;
        fixture.synchronize().await;
        let (_duration, timeout) =
            fixture.expect_heartbeat_timer_registrations(1).await;
        fixture
            .server
            .as_mut()
            .expect("no server mobilized")
            .send(Command::AddCommands(vec![()]))
            .await
            .unwrap();
        fixture.synchronize().await;
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
                    term: 1,
                    command: Some(LogEntryCommand::Custom(())),
                },
            ],
            ClusterConfiguration::Single(hashset![2, 5, 6, 7, 11]),
        );
        let (sender, _receiver) = oneshot::channel();
        timeout
            .send(sender)
            .expect_err("server did not cancel heartbeat timer");
        fixture.expect_no_heartbeat_timer_registrations_now();
        assert_eq!(
            Message {
                content: MessageContent::AppendEntries(AppendEntriesContent {
                    leader_commit: 0,
                    prev_log_index: 1,
                    prev_log_term: 1,
                    log: vec![LogEntry {
                        term: 1,
                        command: Some(LogEntryCommand::Custom(()))
                    }]
                }),
                seq: 3,
                term: 1
            },
            fixture.expect_message(2).await
        );
        fixture
            .send_server_message(
                Message {
                    content: MessageContent::AppendEntriesResponse {
                        success: true,
                        next_log_index: 2,
                    },
                    seq: 2,
                    term: 1,
                },
                6,
            )
            .await;
        fixture.expect_commit(1).await;
        assert_eq!(
            Message {
                content: MessageContent::AppendEntries(AppendEntriesContent {
                    leader_commit: 1,
                    prev_log_index: 1,
                    prev_log_term: 1,
                    log: vec![LogEntry {
                        term: 1,
                        command: Some(LogEntryCommand::Custom(()))
                    }]
                }),
                seq: 3,
                term: 1
            },
            fixture.expect_message(6).await
        );
        fixture.expect_no_heartbeat_timer_registrations_now();
        fixture
            .send_server_message(
                Message {
                    content: MessageContent::AppendEntriesResponse {
                        success: true,
                        next_log_index: 3,
                    },
                    seq: 3,
                    term: 1,
                },
                2,
            )
            .await;
        fixture.trigger_heartbeat_timeout().await;
        assert_eq!(
            Message {
                content: MessageContent::AppendEntries(AppendEntriesContent {
                    leader_commit: 1,
                    prev_log_term: 1,
                    prev_log_index: 2,
                    log: vec![],
                }),
                seq: 4,
                term: 1,
            },
            fixture.expect_message(2).await
        );
    });
}

#[test]
fn non_leader_should_not_accept_commands() {
    assert_logger();
    executor::block_on(async {
        let mut fixture = Fixture::new();
        let (mock_log, mock_log_back_end) = MockLog::new();
        fixture.mobilize_server_with_log(Box::new(mock_log));
        fixture
            .server
            .as_mut()
            .expect("no server mobilized")
            .send(Command::AddCommands(vec![()]))
            .await
            .unwrap();
        fixture.synchronize().await;
        verify_log(
            &mock_log_back_end,
            0,
            0,
            [],
            ClusterConfiguration::Single(hashset![2, 5, 6, 7, 11]),
        );
        fixture.expect_no_messages_now();
    });
}
