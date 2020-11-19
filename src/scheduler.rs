use futures_timer::Delay;
use std::time::Duration;

pub struct Scheduler {}

impl Scheduler {
    pub fn new() -> Self {
        Self {}
    }

    pub async fn schedule(
        &self,
        duration: Duration,
    ) {
        Delay::new(duration).await
    }
}
