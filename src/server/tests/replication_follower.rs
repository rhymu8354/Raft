use super::*;
use crate::AppendEntriesContent;
use futures::executor;

#[test]
fn follower_receive_append_entries() {
    executor::block_on(async {
        let mut fixture = Fixture::new();
        let (mock_persistent_storage, mock_persistent_storage_back_end) =
            new_mock_persistent_storage_with_non_defaults(0, None);
        let (mock_log, _mock_log_back_end) =
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
        verify_persistent_storage(&mock_persistent_storage_back_end, 1, None);
    });
}
