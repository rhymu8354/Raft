use super::DummyCommand;
use crate::{
    Log,
    LogEntry,
};
use std::sync::{
    Arc,
    Mutex,
};

pub struct MockLogShared {
    pub base_term: usize,
    pub base_index: usize,
    pub dropped: bool,
    pub entries: Vec<LogEntry<DummyCommand>>,
    pub last_term: usize,
    pub last_index: usize,
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
            entries: vec![],
            last_term: 0,
            last_index: 0,
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
    type Command = DummyCommand;

    fn append(
        &mut self,
        entry: LogEntry<Self::Command>,
    ) {
        let mut shared = self.shared.lock().unwrap();
        shared.last_index += 1;
        shared.last_term = entry.term;
        shared.entries.push(entry);
    }

    fn base_term(&self) -> usize {
        let shared = self.shared.lock().unwrap();
        shared.base_term
    }

    fn base_index(&self) -> usize {
        let shared = self.shared.lock().unwrap();
        shared.base_index
    }

    fn last_term(&self) -> usize {
        let shared = self.shared.lock().unwrap();
        shared.last_term
    }

    fn last_index(&self) -> usize {
        let shared = self.shared.lock().unwrap();
        shared.last_index
    }
}

impl Drop for MockLog {
    fn drop(&mut self) {
        let mut shared = self.shared.lock().unwrap();
        shared.dropped = true;
    }
}
