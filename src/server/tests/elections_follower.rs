use super::*;

#[test]
fn follower_votes_for_first_candidate() {
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
            .await_vote(AwaitVoteArgs {
                receiver_id: 6,
                seq: 1,
                term: 1,
                vote_granted: true,
            })
            .await;
        fixture.expect_election_state(ElectionState::Follower);
    });
}
