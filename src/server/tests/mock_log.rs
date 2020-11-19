use crate::Log;
use std::sync::{
    atomic::AtomicBool,
    Arc,
};

pub struct MockLog {
    pub dropped: Arc<AtomicBool>,
}

impl MockLog {
    pub fn new() -> Self {
        Self {
            dropped: Arc::new(AtomicBool::new(false)),
        }
    }
}

impl Log for MockLog {}

impl Drop for MockLog {
    fn drop(&mut self) {
        self.dropped.store(true, std::sync::atomic::Ordering::SeqCst);
    }
}
