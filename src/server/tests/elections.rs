use super::*;

#[test]
fn election_always_started_within_maximum_timeout_interval() {
    let mut fixture = Fixture::new();
    fixture.mobilize_server();
    executor::block_on(fixture.await_election_timeout());
}
