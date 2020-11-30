use futures::{
    future::BoxFuture,
    FutureExt,
};
use futures_timer::Delay;
use std::time::Duration;

pub struct Scheduler {}

impl Scheduler {
    pub fn new() -> Self {
        Self {}
    }

    pub fn schedule(
        &self,
        duration: Duration,
    ) -> BoxFuture<'static, ()> {
        Delay::new(duration).boxed()
    }
}
