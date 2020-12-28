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
    pub snapshot: Vec<u8>,
}

impl MockLogShared {
    fn append_one(
        &mut self,
        entry: LogEntry<DummyCommand>,
    ) {
        self.last_index += 1;
        self.last_term = entry.term;
        self.entries.push(entry);
    }

    fn append<T>(
        &mut self,
        entries: T,
    ) where
        T: IntoIterator<Item = LogEntry<DummyCommand>>,
    {
        for entry in entries.into_iter() {
            self.append_one(entry);
        }
    }

    fn entries(
        &self,
        prev_log_index: usize,
    ) -> Vec<LogEntry<DummyCommand>> {
        self.entries
            .iter()
            .skip(prev_log_index - self.base_index)
            .cloned()
            .collect::<Vec<_>>()
    }

    fn entry_term(
        &self,
        index: usize,
    ) -> Option<usize> {
        if index == self.base_index {
            Some(self.base_term)
        } else {
            index
                .checked_sub(self.base_index)
                .and_then(|index| self.entries.get(index - 1))
                .map(|entry| entry.term)
        }
    }

    fn truncate(
        &mut self,
        index: usize,
    ) {
        self.entries.truncate(index.saturating_sub(self.base_index));
        self.last_index = self.base_index + self.entries.len();
        self.last_term = if let Some(last_log_entry) = self.entries.last() {
            last_log_entry.term
        } else {
            self.base_term
        };
    }
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
            snapshot: vec![],
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

    fn append_one(
        &mut self,
        entry: LogEntry<Self::Command>,
    ) {
        let mut shared = self.shared.lock().unwrap();
        shared.append_one(entry)
    }

    fn append(
        &mut self,
        entries: Box<dyn Iterator<Item = LogEntry<Self::Command>>>,
    ) {
        let mut shared = self.shared.lock().unwrap();
        shared.append(entries)
    }

    fn base_term(&self) -> usize {
        let shared = self.shared.lock().unwrap();
        shared.base_term
    }

    fn base_index(&self) -> usize {
        let shared = self.shared.lock().unwrap();
        shared.base_index
    }

    fn entries(
        &self,
        prev_log_index: usize,
    ) -> Vec<LogEntry<Self::Command>> {
        let shared = self.shared.lock().unwrap();
        shared.entries(prev_log_index)
    }

    fn entry_term(
        &self,
        index: usize,
    ) -> Option<usize> {
        let shared = self.shared.lock().unwrap();
        shared.entry_term(index)
    }

    fn last_term(&self) -> usize {
        let shared = self.shared.lock().unwrap();
        shared.last_term
    }

    fn last_index(&self) -> usize {
        let shared = self.shared.lock().unwrap();
        shared.last_index
    }

    fn snapshot(&self) -> Vec<u8> {
        let shared = self.shared.lock().unwrap();
        shared.snapshot.clone()
    }

    fn truncate(
        &mut self,
        index: usize,
    ) {
        let mut shared = self.shared.lock().unwrap();
        shared.truncate(index);
    }
}

impl Drop for MockLog {
    fn drop(&mut self) {
        let mut shared = self.shared.lock().unwrap();
        shared.dropped = true;
    }
}
