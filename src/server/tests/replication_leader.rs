use super::*;

#[test]
fn leader_sends_no_op_log_entry_upon_election() {
    executor::block_on(async {
        let mut fixture = Fixture::new();
        fixture.mobilize_server();
        fixture.expect_election_with_defaults().await;
        fixture.cast_votes(1, 1).await;
        fixture.expect_election_state_change(ElectionState::Leader);
        fixture
            .expect_log_entries_broadcast(AwaitAppendEntriesArgs {
                term: 1,
                leader_commit: 0,
                prev_log_term: 0,
                prev_log_index: 0,
                log: vec![LogEntry::<DummyCommand> {
                    term: 1,
                    command: None,
                }],
            })
            .await;
    });
}
