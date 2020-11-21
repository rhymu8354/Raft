use futures::FutureExt;
use futures_timer::Delay;
use std::{
    pin::Pin,
    time::Duration,
};

pub struct Scheduler {}

impl Scheduler {
    pub fn new() -> Self {
        Self {}
    }

    pub fn schedule(
        &self,
        duration: Duration,
    ) -> Pin<Box<dyn futures::Future<Output = ()> + Send>> {
        Delay::new(duration).boxed()
    }
}
