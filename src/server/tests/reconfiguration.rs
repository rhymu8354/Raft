use super::*;
use crate::{
    tests::assert_logger,
    ClusterConfiguration,
};
use futures::executor;

#[test]
#[allow(clippy::too_many_lines)]
fn delay_start_reconfiguration_until_new_member_catches_up() {
    assert_logger();
    executor::block_on(async {
        let mut fixture = Fixture::new();
        let (mock_log, mock_log_back_end) = MockLog::new();
        fixture.mobilize_server_with_log(Box::new(mock_log));
        fixture.expect_election_with_defaults().await;
        fixture.cast_votes(1, 1).await;
        fixture.expect_election_state_change(ServerElectionState::Leader).await;
        fixture.expect_messages_now(hashset![2, 6, 7, 11]);
        fixture
            .server
            .as_mut()
            .expect("no server mobilized")
            .send(ServerCommand::Reconfigure(hashset![2, 5, 6, 7, 11, 12, 13]))
            .await
            .expect("unable to send command to server");
        let messages = fixture.expect_messages(hashset![12, 13]).await;
        fixture.expect_no_reconfiguration().await;
        assert_eq!(
            Message {
                content: MessageContent::AppendEntries(AppendEntriesContent {
                    leader_commit: 0,
                    prev_log_term: 1,
                    prev_log_index: 1,
                    log: vec![],
                }),
                seq: 1,
                term: 1,
            },
            messages[&12]
        );
        assert_eq!(
            Message {
                content: MessageContent::AppendEntries(AppendEntriesContent {
                    leader_commit: 0,
                    prev_log_term: 1,
                    prev_log_index: 1,
                    log: vec![],
                }),
                seq: 1,
                term: 1,
            },
            messages[&13]
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
                2,
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
        fixture.expect_commit(1).await;
        fixture
            .send_server_message(
                Message {
                    content: MessageContent::AppendEntriesResponse {
                        success: true,
                        next_log_index: 2,
                    },
                    seq: 1,
                    term: 1,
                },
                12,
            )
            .await;
        fixture.expect_no_reconfiguration().await;
        verify_log(
            &mock_log_back_end,
            0,
            0,
            [LogEntry {
                term: 1,
                command: None,
            }],
            Snapshot {
                cluster_configuration: ClusterConfiguration::Single(hashset![
                    2, 5, 6, 7, 11
                ]),
                state: (),
            },
        );
        fixture
            .send_server_message(
                Message {
                    content: MessageContent::AppendEntriesResponse {
                        success: true,
                        next_log_index: 2,
                    },
                    seq: 1,
                    term: 1,
                },
                13,
            )
            .await;
        let messages = fixture.expect_messages(hashset![2, 6, 12]).await;
        assert_eq!(
            Message {
                content: MessageContent::AppendEntries(AppendEntriesContent {
                    leader_commit: 1,
                    prev_log_term: 1,
                    prev_log_index: 1,
                    log: vec![LogEntry {
                        term: 1,
                        command: Some(LogEntryCommand::StartReconfiguration(
                            hashset![2, 5, 6, 7, 11, 12, 13],
                        )),
                    }],
                }),
                seq: 3,
                term: 1,
            },
            messages[&2]
        );
        assert_eq!(
            Message {
                content: MessageContent::AppendEntries(AppendEntriesContent {
                    leader_commit: 1,
                    prev_log_term: 1,
                    prev_log_index: 1,
                    log: vec![LogEntry {
                        term: 1,
                        command: Some(LogEntryCommand::StartReconfiguration(
                            hashset![2, 5, 6, 7, 11, 12, 13],
                        )),
                    }],
                }),
                seq: 3,
                term: 1,
            },
            messages[&6]
        );
        assert_eq!(
            Message {
                content: MessageContent::AppendEntries(AppendEntriesContent {
                    leader_commit: 1,
                    prev_log_term: 1,
                    prev_log_index: 1,
                    log: vec![LogEntry {
                        term: 1,
                        command: Some(LogEntryCommand::StartReconfiguration(
                            hashset![2, 5, 6, 7, 11, 12, 13],
                        )),
                    }],
                }),
                seq: 2,
                term: 1,
            },
            messages[&12]
        );
        fixture
            .expect_reconfiguration(&ClusterConfiguration::Joint(
                hashset![2, 5, 6, 7, 11],
                hashset![2, 5, 6, 7, 11, 12, 13],
            ))
            .await;
        assert_eq!(
            Message {
                content: MessageContent::AppendEntries(AppendEntriesContent {
                    leader_commit: 1,
                    prev_log_term: 1,
                    prev_log_index: 1,
                    log: vec![LogEntry {
                        term: 1,
                        command: Some(LogEntryCommand::StartReconfiguration(
                            hashset![2, 5, 6, 7, 11, 12, 13],
                        )),
                    }],
                }),
                seq: 2,
                term: 1,
            },
            fixture.expect_message(13).await
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
                    term: 1,
                    command: Some(LogEntryCommand::StartReconfiguration(
                        hashset![2, 5, 6, 7, 11, 12, 13],
                    )),
                },
            ],
            Snapshot {
                cluster_configuration: ClusterConfiguration::Single(hashset![
                    2, 5, 6, 7, 11
                ]),
                state: (),
            },
        );
        fixture.expect_no_messages_now();
    });
}

#[test]
fn start_reconfiguration_immediately_if_no_new_members() {
    assert_logger();
    executor::block_on(async {
        let mut fixture = Fixture::new();
        let (mock_log, mock_log_back_end) = MockLog::new();
        fixture.mobilize_server_with_log(Box::new(mock_log));
        fixture.expect_election_with_defaults().await;
        fixture.cast_votes(1, 1).await;
        fixture.expect_election_state_change(ServerElectionState::Leader).await;
        fixture.expect_messages_now(hashset![2, 6, 7, 11]);
        fixture
            .server
            .as_mut()
            .expect("no server mobilized")
            .send(ServerCommand::Reconfigure(hashset![2, 5, 6, 7]))
            .await
            .expect("unable to send command to server");
        fixture
            .expect_reconfiguration(&ClusterConfiguration::Joint(
                hashset![2, 5, 6, 7, 11],
                hashset![2, 5, 6, 7],
            ))
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
                    term: 1,
                    command: Some(LogEntryCommand::StartReconfiguration(
                        hashset![2, 5, 6, 7],
                    )),
                },
            ],
            Snapshot {
                cluster_configuration: ClusterConfiguration::Single(hashset![
                    2, 5, 6, 7, 11
                ]),
                state: (),
            },
        );
    });
}

#[test]
#[allow(clippy::too_many_lines)]
fn reconfiguration_overrides_pending_previous_reconfiguration() {
    assert_logger();
    executor::block_on(async {
        let mut fixture = Fixture::new();
        let (mock_log, mock_log_back_end) = MockLog::new();
        fixture.mobilize_server_with_log(Box::new(mock_log));
        fixture.expect_election_with_defaults().await;
        fixture.cast_votes(1, 1).await;
        fixture.expect_election_state_change(ServerElectionState::Leader).await;
        fixture
            .server
            .as_mut()
            .expect("no server mobilized")
            .send(ServerCommand::Reconfigure(hashset![2, 5, 6, 7, 11, 12]))
            .await
            .expect("unable to send command to server");
        fixture.expect_messages(hashset![2, 6, 7, 11, 12]).await;
        verify_log(
            &mock_log_back_end,
            0,
            0,
            [LogEntry {
                term: 1,
                command: None,
            }],
            Snapshot {
                cluster_configuration: ClusterConfiguration::Single(hashset![
                    2, 5, 6, 7, 11
                ]),
                state: (),
            },
        );
        fixture
            .server
            .as_mut()
            .expect("no server mobilized")
            .send(ServerCommand::AddCommands(vec![DummyCommand {}]))
            .await
            .unwrap();
        fixture
            .server
            .as_mut()
            .expect("no server mobilized")
            .send(ServerCommand::Reconfigure(hashset![2, 5, 6, 7, 11, 12, 13]))
            .await
            .expect("unable to send command to server");
        assert_eq!(
            Message {
                content: MessageContent::AppendEntries(AppendEntriesContent {
                    leader_commit: 0,
                    prev_log_term: 1,
                    prev_log_index: 2,
                    log: vec![],
                }),
                seq: 1,
                term: 1,
            },
            fixture.expect_message(13).await
        );
        fixture
            .send_server_message(
                Message {
                    content: MessageContent::AppendEntriesResponse {
                        success: true,
                        next_log_index: 3,
                    },
                    seq: 1,
                    term: 1,
                },
                13,
            )
            .await;
        fixture.expect_no_reconfiguration().await;
        fixture
            .send_server_message(
                Message {
                    content: MessageContent::AppendEntriesResponse {
                        success: true,
                        next_log_index: 2,
                    },
                    seq: 1,
                    term: 1,
                },
                12,
            )
            .await;
        assert_eq!(
            Message {
                content: MessageContent::AppendEntries(AppendEntriesContent {
                    leader_commit: 0,
                    prev_log_term: 1,
                    prev_log_index: 1,
                    log: vec![LogEntry {
                        term: 1,
                        command: Some(LogEntryCommand::Custom(DummyCommand {})),
                    }],
                }),
                seq: 2,
                term: 1,
            },
            fixture.expect_message(12).await
        );
        fixture.expect_no_reconfiguration().await;
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
                12,
            )
            .await;
        assert_eq!(
            Message {
                content: MessageContent::AppendEntries(AppendEntriesContent {
                    leader_commit: 0,
                    prev_log_term: 1,
                    prev_log_index: 2,
                    log: vec![LogEntry {
                        term: 1,
                        command: Some(LogEntryCommand::StartReconfiguration(
                            hashset![2, 5, 6, 7, 11, 12, 13],
                        )),
                    }],
                }),
                seq: 2,
                term: 1,
            },
            fixture.expect_message(13).await
        );
        fixture
            .expect_reconfiguration(&ClusterConfiguration::Joint(
                hashset![2, 5, 6, 7, 11],
                hashset![2, 5, 6, 7, 11, 12, 13],
            ))
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
                    term: 1,
                    command: Some(LogEntryCommand::Custom(DummyCommand {})),
                },
                LogEntry {
                    term: 1,
                    command: Some(LogEntryCommand::StartReconfiguration(
                        hashset![2, 5, 6, 7, 11, 12, 13],
                    )),
                },
            ],
            Snapshot {
                cluster_configuration: ClusterConfiguration::Single(hashset![
                    2, 5, 6, 7, 11
                ]),
                state: (),
            },
        );
        assert_eq!(
            Message {
                content: MessageContent::AppendEntries(AppendEntriesContent {
                    leader_commit: 0,
                    prev_log_term: 1,
                    prev_log_index: 2,
                    log: vec![LogEntry {
                        term: 1,
                        command: Some(LogEntryCommand::StartReconfiguration(
                            hashset![2, 5, 6, 7, 11, 12, 13],
                        )),
                    }],
                }),
                seq: 3,
                term: 1,
            },
            fixture.expect_message(12).await
        );
        fixture.expect_no_messages_now();
    });
}

#[test]
fn no_reconfiguration_if_reconfiguration_requested_is_current_configuration() {
    assert_logger();
    executor::block_on(async {
        let mut fixture = Fixture::new();
        let (mock_log, mock_log_back_end) = MockLog::new();
        fixture.mobilize_server_with_log(Box::new(mock_log));
        fixture.expect_election_with_defaults().await;
        fixture.cast_votes(1, 1).await;
        fixture.expect_election_state_change(ServerElectionState::Leader).await;
        fixture.expect_messages(hashset![2, 6, 7, 11]).await;
        fixture
            .server
            .as_mut()
            .expect("no server mobilized")
            .send(ServerCommand::Reconfigure(hashset![2, 5, 6, 7, 11]))
            .await
            .expect("unable to send command to server");
        fixture.expect_no_messages().await;
        verify_log(
            &mock_log_back_end,
            0,
            0,
            [LogEntry {
                term: 1,
                command: None,
            }],
            Snapshot {
                cluster_configuration: ClusterConfiguration::Single(hashset![
                    2, 5, 6, 7, 11
                ]),
                state: (),
            },
        );
    });
}

#[test]
fn no_reconfiguration_if_in_joint_configuration() {
    assert_logger();
    executor::block_on(async {
        let mut fixture = Fixture::new();
        let (mock_log, mock_log_back_end) = MockLog::new();
        fixture.mobilize_server_with_log(Box::new(mock_log));
        fixture.expect_election_with_defaults().await;
        fixture.cast_votes(1, 1).await;
        fixture.expect_election_state_change(ServerElectionState::Leader).await;
        fixture
            .server
            .as_mut()
            .expect("no server mobilized")
            .send(ServerCommand::Reconfigure(hashset![2, 5, 6, 7, 11, 12]))
            .await
            .expect("unable to send command to server");
        fixture.expect_messages(hashset![2, 6, 7, 11, 12]).await;
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
        fixture
            .send_server_message(
                Message {
                    content: MessageContent::AppendEntriesResponse {
                        success: true,
                        next_log_index: 2,
                    },
                    seq: 1,
                    term: 1,
                },
                12,
            )
            .await;
        fixture.expect_messages(hashset![2, 6]).await;
        fixture
            .server
            .as_mut()
            .expect("no server mobilized")
            .send(ServerCommand::Reconfigure(hashset![2, 5, 6, 7, 11, 12, 13]))
            .await
            .expect("unable to send command to server");
        fixture
            .expect_reconfiguration(&ClusterConfiguration::Joint(
                hashset![2, 5, 6, 7, 11],
                hashset![2, 5, 6, 7, 11, 12],
            ))
            .await;
        fixture.expect_message(12).await;
        fixture.expect_no_messages_now();
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
                    command: Some(LogEntryCommand::StartReconfiguration(
                        hashset![2, 5, 6, 7, 11, 12],
                    )),
                },
            ],
            Snapshot {
                cluster_configuration: ClusterConfiguration::Single(hashset![
                    2, 5, 6, 7, 11
                ]),
                state: (),
            },
        );
    });
}

#[test]
#[allow(clippy::too_many_lines)]
fn reconfiguration_cancelled_if_reconfigured_back_to_original_configuration() {
    assert_logger();
    executor::block_on(async {
        let mut fixture = Fixture::new();
        let (mock_log, mock_log_back_end) = MockLog::new();
        fixture.mobilize_server_with_log(Box::new(mock_log));
        fixture.expect_election_with_defaults().await;
        fixture.cast_votes(1, 1).await;
        fixture.expect_election_state_change(ServerElectionState::Leader).await;
        fixture
            .server
            .as_mut()
            .expect("no server mobilized")
            .send(ServerCommand::Reconfigure(hashset![2, 5, 6, 7, 11, 12]))
            .await
            .expect("unable to send command to server");
        fixture.expect_messages(hashset![2, 6, 7, 11, 12]).await;
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
        fixture
            .server
            .as_mut()
            .expect("no server mobilized")
            .send(ServerCommand::Reconfigure(hashset![2, 5, 6, 7, 11]))
            .await
            .expect("unable to send command to server");
        fixture.expect_no_messages().await;
        fixture
            .send_server_message(
                Message {
                    content: MessageContent::AppendEntriesResponse {
                        success: true,
                        next_log_index: 2,
                    },
                    seq: 1,
                    term: 1,
                },
                12,
            )
            .await;
        fixture.expect_no_messages().await;
        verify_log(
            &mock_log_back_end,
            0,
            0,
            [LogEntry {
                term: 1,
                command: None,
            }],
            Snapshot {
                cluster_configuration: ClusterConfiguration::Single(hashset![
                    2, 5, 6, 7, 11
                ]),
                state: (),
            },
        );
        fixture
            .server
            .as_mut()
            .expect("no server mobilized")
            .send(ServerCommand::Reconfigure(hashset![2, 5, 6, 7, 11, 12]))
            .await
            .expect("unable to send command to server");
        assert_eq!(
            Message {
                content: MessageContent::AppendEntries(AppendEntriesContent {
                    leader_commit: 1,
                    prev_log_term: 1,
                    prev_log_index: 1,
                    log: vec![],
                }),
                seq: 1,
                term: 1,
            },
            fixture.expect_message(12).await
        );
    });
}

#[test]
#[allow(clippy::too_many_lines)]
fn reconfiguration_cancelled_if_revert_to_follower() {
    assert_logger();
    executor::block_on(async {
        let mut fixture = Fixture::new();
        let (mock_log, mock_log_back_end) = MockLog::new();
        fixture.mobilize_server_with_log(Box::new(mock_log));
        fixture.expect_election_with_defaults().await;
        fixture.cast_votes(1, 1).await;
        fixture.expect_election_timer_registrations(1).await;
        fixture.expect_election_state_change(ServerElectionState::Leader).await;
        fixture
            .server
            .as_mut()
            .expect("no server mobilized")
            .send(ServerCommand::Reconfigure(hashset![2, 5, 6, 7, 11, 12]))
            .await
            .expect("unable to send command to server");
        fixture.expect_messages(hashset![2, 6, 7, 11, 12]).await;
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
                7,
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
                11,
            )
            .await;
        fixture.expect_commit(1).await;
        fixture
            .receive_vote_request(ReceiveVoteRequestArgs {
                sender_id: 6,
                last_log_term: 1,
                last_log_index: 1,
                seq: 1,
                term: 2,
            })
            .await;
        fixture
            .expect_election_state_change(ServerElectionState::Follower)
            .await;
        fixture.expect_message(6).await;
        fixture
            .expect_election(AwaitElectionTimeoutArgs {
                last_log_term: 1,
                last_log_index: 1,
                term: 3,
            })
            .await;
        fixture.expect_no_messages().await;
        fixture.cast_votes(3, 3).await;
        fixture.expect_election_state_change(ServerElectionState::Leader).await;
        fixture.expect_messages(hashset![2, 6, 7, 11]).await;
        fixture.expect_no_messages().await;
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
                    term: 3,
                    command: None,
                },
            ],
            Snapshot {
                cluster_configuration: ClusterConfiguration::Single(hashset![
                    2, 5, 6, 7, 11
                ]),
                state: (),
            },
        );
    });
}

#[test]
fn follower_add_peers_in_joint_configuration() {
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
                            leader_commit: 0,
                            prev_log_index: 0,
                            prev_log_term: 0,
                            log: vec![LogEntry {
                                term: 1,
                                command: Some(
                                    LogEntryCommand::StartReconfiguration(
                                        hashset![2, 5, 6, 7, 11, 12],
                                    ),
                                ),
                            }],
                        },
                    ),
                    seq: 1,
                    term: 1,
                },
                2,
            )
            .await;
        fixture.peer_ids.insert(12);
        fixture
            .expect_election(AwaitElectionTimeoutArgs {
                last_log_term: 1,
                last_log_index: 1,
                term: 2,
            })
            .await;
        fixture.cast_votes(1, 2).await;
        fixture.expect_election_timer_registrations(1).await;
        fixture.expect_election_state_change(ServerElectionState::Leader).await;
        fixture.expect_messages(hashset![2, 6, 7, 11, 12]).await;
        fixture.expect_no_messages_now();
    });
}

#[test]
fn truncate_log_should_remove_old_peers() {
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
                            leader_commit: 0,
                            prev_log_index: 0,
                            prev_log_term: 0,
                            log: vec![LogEntry {
                                term: 1,
                                command: Some(
                                    LogEntryCommand::StartReconfiguration(
                                        hashset![2, 5, 6, 7, 11, 12],
                                    ),
                                ),
                            }],
                        },
                    ),
                    seq: 1,
                    term: 1,
                },
                2,
            )
            .await;
        fixture.expect_election_timer_registrations(1).await;
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
                6,
            )
            .await;
        fixture
            .expect_election(AwaitElectionTimeoutArgs {
                last_log_term: 2,
                last_log_index: 1,
                term: 3,
            })
            .await;
        fixture.cast_votes(1, 3).await;
        fixture.expect_election_timer_registrations(1).await;
        fixture.expect_election_state_change(ServerElectionState::Leader).await;
        fixture.expect_messages(hashset![2, 6, 7, 11]).await;
        fixture.expect_no_messages_now();
    });
}

// TODO:
// * When in joint configuration and the last `StartReconfiguration` command is
//   committed, the leader should append `FinishReconfiguration` command and
//   drop any peers that are not in the new configuration.
// * A "configuration change" event should be generated whenever the
//   configuration changes.
// * Once the current configuration is committed, if the leader was a
//   "non-voting" member, it should step down by no longer appending entries.
//   (At this point we could let it delegate leadership explicitly, or simply
//   let one of the other servers start a new election once its election timer
//   expires.)
// * Election win should require separate majorites of old and new
//   configuration, when in joint configuration.
// * Committing log entries should require separate majorites of old and new
//   configuration, when in joint configuration.
