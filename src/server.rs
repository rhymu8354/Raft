#[cfg(test)]
mod tests;

use crate::{
    Log,
    PersistentStorage,
    Scheduler,
};
use std::sync::Arc;

#[derive(Default)]
pub struct Server {}

impl Server {
    pub fn new() -> Self {
        Self {}
    }

    pub fn mobilize(
        &mut self,
        _log: Arc<dyn Log>,
        _persistent_storage: Arc<dyn PersistentStorage>,
        _scheduler: Arc<dyn Scheduler>,
    ) {
    }

    pub fn demobilize(&mut self) {}
}
