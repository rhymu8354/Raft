use super::*;
use futures::executor;

#[test]
fn leader_sends_no_op_log_entry_upon_election() {
    executor::block_on(async {
        let mut fixture = Fixture::new();
        let (mock_log, mock_log_back_end) =
            new_mock_log_with_non_defaults(0, 0);
        fixture.mobilize_server_with_log(Box::new(mock_log));
        fixture.expect_election_with_defaults().await;
        fixture.cast_votes(1, 1).await;
        fixture.expect_election_state_change(ServerElectionState::Leader);
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
        verify_log(&mock_log_back_end, 0, 0, &vec![LogEntry {
            term: 1,
            command: None,
        }]);
    });
}

#[test]
fn leader_retransmit_append_entries() {
    executor::block_on(async {
        let mut fixture = Fixture::new();
        fixture.mobilize_server();
        fixture.expect_election_with_defaults().await;
        fixture.expect_retransmission_timer_registration(2).await;
        fixture.cast_votes(1, 1).await;
        fixture.expect_election_state_change(ServerElectionState::Leader);
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
            fixture.await_retransmission(2).await
        );
    });
}

#[test]
fn leader_no_retransmit_append_entries_after_response() {
    executor::block_on(async {
        let mut fixture = Fixture::new();
        fixture.mobilize_server();
        fixture.expect_election_with_defaults().await;
        fixture.expect_retransmission_timer_registration(2).await;
        fixture.cast_votes(1, 1).await;
        fixture.expect_election_state_change(ServerElectionState::Leader);
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
        assert!(
            completer.send(()).is_err(),
            "server didn't cancel retransmission timer"
        );
    });
}
