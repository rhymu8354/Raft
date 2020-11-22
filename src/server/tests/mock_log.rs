use crate::Log;
use std::sync::{
    Arc,
    Mutex,
};

pub struct MockLogShared {
    pub base_term: usize,
    pub base_index: usize,
    pub dropped: bool,
}

pub struct MockLog {
    pub shared: Arc<Mutex<MockLogShared>>,
}

pub struct MockLogBackEnd {
    pub shared: Arc<Mutex<MockLogShared>>,
}

impl MockLog {
    pub fn new() -> (Self, MockLogBackEnd) {
        let shared = Arc::new(Mutex::new(MockLogShared {
            base_term: 0,
            base_index: 0,
            dropped: false,
        }));
        (
            Self {
                shared: shared.clone(),
            },
            MockLogBackEnd {
                shared,
            },
        )
    }
}

impl Log for MockLog {
    fn base_term(&self) -> usize {
        let shared = self.shared.lock().unwrap();
        shared.base_term
    }

    fn base_index(&self) -> usize {
        let shared = self.shared.lock().unwrap();
        shared.base_index
    }
}

impl Drop for MockLog {
    fn drop(&mut self) {
        let mut shared = self.shared.lock().unwrap();
        shared.dropped = true;
    }
}
