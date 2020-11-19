use crate::Log;
use std::sync::{
    atomic::AtomicBool,
    Arc,
};

pub struct MockLog {
    pub dropped: Arc<AtomicBool>,
}

pub struct MockLogBackEnd {
    pub dropped: Arc<AtomicBool>,
}

impl MockLog {
    pub fn new() -> (Self, MockLogBackEnd) {
        let dropped = Arc::new(AtomicBool::new(false));
        (
            Self {
                dropped: dropped.clone(),
            },
            MockLogBackEnd {
                dropped,
            },
        )
    }
}

impl Log for MockLog {}

impl Drop for MockLog {
    fn drop(&mut self) {
        self.dropped.store(true, std::sync::atomic::Ordering::SeqCst);
    }
}
