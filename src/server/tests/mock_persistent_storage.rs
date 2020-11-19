use crate::PersistentStorage;
use std::sync::{
    atomic::AtomicBool,
    Arc,
};

pub struct MockPersistentStorage {
    pub dropped: Arc<AtomicBool>,
}

impl MockPersistentStorage {
    pub fn new() -> Self {
        Self {
            dropped: Arc::new(AtomicBool::new(false)),
        }
    }
}

impl PersistentStorage for MockPersistentStorage {}

impl Drop for MockPersistentStorage {
    fn drop(&mut self) {
        self.dropped.store(true, std::sync::atomic::Ordering::SeqCst);
    }
}
