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
        fixture.expect_messages_now(hashset! {2, 6, 7, 11});
        fixture
            .server
            .as_mut()
            .expect("no server mobilized")
            .send(ServerCommand::Reconfigure(hashset! {2, 5, 6, 7, 11, 12, 13}))
            .await
            .expect("unable to send command to server");
        let messages = fixture.expect_messages(hashset! {12, 13}).await;
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
                    content: MessageContent::AppendEntriesResponse {
                        match_index: 1,
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
                        match_index: 1,
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
                        match_index: 1,
                    },
                    seq: 1,
                    term: 1,
                },
                13,
            )
            .await;
        fixture
            .expect_reconfiguration(&hashset! {2, 5, 6, 7, 11, 12, 13})
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
        let messages = fixture.expect_messages(hashset! {2, 6, 12, 13}).await;
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
            messages[&13]
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
        fixture.expect_messages_now(hashset! {2, 6, 7, 11});
        fixture
            .server
            .as_mut()
            .expect("no server mobilized")
            .send(ServerCommand::Reconfigure(hashset! {2, 5, 6, 7}))
            .await
            .expect("unable to send command to server");
        fixture.expect_reconfiguration(&hashset! {2, 5, 6, 7}).await;
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
            .send(ServerCommand::Reconfigure(hashset! {2, 5, 6, 7, 11, 12}))
            .await
            .expect("unable to send command to server");
        fixture.expect_messages(hashset! {2, 6, 7, 11, 12}).await;
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
            .send(ServerCommand::Reconfigure(hashset! {2, 5, 6, 7, 11, 12, 13}))
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
                        match_index: 2,
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
                        match_index: 1,
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
                        match_index: 2,
                    },
                    seq: 2,
                    term: 1,
                },
                12,
            )
            .await;
        fixture
            .expect_reconfiguration(&hashset! {2, 5, 6, 7, 11, 12, 13})
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
        let messages = fixture.expect_messages(hashset! {12, 13}).await;
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
            messages[&12]
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
                seq: 2,
                term: 1,
            },
            messages[&13]
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
        fixture.expect_messages(hashset! {2, 6, 7, 11}).await;
        fixture
            .server
            .as_mut()
            .expect("no server mobilized")
            .send(ServerCommand::Reconfigure(hashset! {2, 5, 6, 7, 11}))
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
            .send(ServerCommand::Reconfigure(hashset! {2, 5, 6, 7, 11, 12}))
            .await
            .expect("unable to send command to server");
        fixture.expect_messages(hashset! {2, 6, 7, 11, 12}).await;
        fixture
            .send_server_message(
                Message {
                    content: MessageContent::AppendEntriesResponse {
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
                    content: MessageContent::AppendEntriesResponse {
                        match_index: 1,
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
                        match_index: 1,
                    },
                    seq: 1,
                    term: 1,
                },
                12,
            )
            .await;
        fixture.expect_reconfiguration(&hashset! {2, 5, 6, 7, 11, 12}).await;
        fixture.expect_messages(hashset! {2, 6, 12}).await;
        fixture
            .server
            .as_mut()
            .expect("no server mobilized")
            .send(ServerCommand::Reconfigure(hashset! {2, 5, 6, 7, 11, 12, 13}))
            .await
            .expect("unable to send command to server");
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

// TODO:
// * A request to change the configuration back to the current configuration
//   when a new configuration is pending should cancel the pending
//   reconfiguration and drop any peers not in the original configuration.
// * When a `FinishReconfiguration` command is appended, drop any peers that
//   were in the old configuration but not the new configuration.
// * Server should use latest configuration appended to log, even if it hasn't
//   yet been committed.
// * When the last joint configuration log entry is committed, the leader should
//   append log entry for the new configuration.
// * A newly elected leader should inspect its log, from newest backwards to its
//   snapshot, to see what configuration is in effect.  If it differs from the
//   configuration provided by the user, a "configuration change" event should
//   be generated.
// * A server whose own ID is not in the cluster configuration should consider
//   itself a "non-voting" member; it should not respond to vote requests and
//   should not start elections.
// * A "non-voting" member should transition to "voting" member when its own ID
//   becomes present in the latest cluster configuration; it should start its
//   election timer and begin responding to vote requests.
// * A "voting" member should transition to "non-voting" member when its own ID
//   is no longer in the latest cluster configuration; it should cancel its
//   election timer and stop responding to vote requests.
// * Once the current configuration is committed, if the leader was a
//   "non-voting" member, it should step down by no longer appending entries.
//   (At this point we could let it delegate leadership explicitly, or simply
//   let one of the other servers start a new election once its election timer
//   expires.)
// * Servers reverting to follower should drop any pending reconfiguration and
//   forget any non-voting peers.
