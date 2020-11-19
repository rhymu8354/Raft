use super::*;

#[test]
fn election_always_started_within_maximum_timeout_interval() {
    let mut fixture = Fixture::new();
    let MobilizedServerResources {
        scheduler_event_receiver,
        ..
    } = fixture.mobilize_server();
    fixture.await_election_timeout(scheduler_event_receiver);
}
