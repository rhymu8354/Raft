use crate::PersistentStorage;
use std::sync::{
    atomic::AtomicBool,
    Arc,
};

pub struct MockPersistentStorage {
    pub dropped: Arc<AtomicBool>,
}

pub struct MockPersistentStorageBackEnd {
    pub dropped: Arc<AtomicBool>,
}

impl MockPersistentStorage {
    pub fn new() -> (Self, MockPersistentStorageBackEnd) {
        let dropped = Arc::new(AtomicBool::new(false));
        (
            Self {
                dropped: dropped.clone(),
            },
            MockPersistentStorageBackEnd {
                dropped,
            },
        )
    }
}

impl PersistentStorage for MockPersistentStorage {}

impl Drop for MockPersistentStorage {
    fn drop(&mut self) {
        self.dropped.store(true, std::sync::atomic::Ordering::SeqCst);
    }
}
