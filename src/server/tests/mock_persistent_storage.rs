use crate::PersistentStorage;
use std::sync::{
    Arc,
    Mutex,
};

pub struct MockPersistentStorageShared {
    pub dropped: bool,
    pub term: usize,
    pub voted_for: Option<usize>,
}

pub struct MockPersistentStorage {
    pub shared: Arc<Mutex<MockPersistentStorageShared>>,
}

pub struct MockPersistentStorageBackEnd {
    pub shared: Arc<Mutex<MockPersistentStorageShared>>,
}

impl MockPersistentStorage {
    pub fn new() -> (Self, MockPersistentStorageBackEnd) {
        let shared = Arc::new(Mutex::new(MockPersistentStorageShared {
            dropped: false,
            term: 0,
            voted_for: None,
        }));
        (
            Self {
                shared: shared.clone(),
            },
            MockPersistentStorageBackEnd {
                shared,
            },
        )
    }
}

impl PersistentStorage for MockPersistentStorage {
    fn term(&self) -> usize {
        let shared = self.shared.lock().unwrap();
        shared.term
    }

    fn voted_for(&self) -> Option<usize> {
        let shared = self.shared.lock().unwrap();
        shared.voted_for
    }

    fn update(
        &mut self,
        term: usize,
        voted_vor: Option<usize>,
    ) {
        let mut shared = self.shared.lock().unwrap();
        shared.term = term;
        shared.voted_for = voted_vor;
    }
}

impl Drop for MockPersistentStorage {
    fn drop(&mut self) {
        let mut shared = self.shared.lock().unwrap();
        shared.dropped = true;
    }
}
