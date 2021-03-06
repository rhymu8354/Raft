use crate::{
    ClusterConfiguration,
    Log,
    LogEntry,
};
use maplit::hashset;
use std::sync::{
    Arc,
    Mutex,
};

pub struct Shared {
    pub base_term: usize,
    pub base_index: usize,
    pub dropped: bool,
    pub entries: Vec<LogEntry<(), usize>>,
    pub last_term: usize,
    pub last_index: usize,
    pub on_update_snapshot: Option<Box<dyn FnOnce(&mut Self) + Send>>,
    pub snapshot: ClusterConfiguration<usize>,
}

impl Shared {
    fn append_one(
        &mut self,
        entry: LogEntry<(), usize>,
    ) {
        self.last_index += 1;
        self.last_term = entry.term;
        self.entries.push(entry);
    }

    fn append<T>(
        &mut self,
        entries: T,
    ) where
        T: IntoIterator<Item = LogEntry<(), usize>>,
    {
        for entry in entries {
            self.append_one(entry);
        }
    }

    fn cluster_configuration(&self) -> ClusterConfiguration<usize> {
        let offset = self.base_index + 1;
        self.entries
            .iter()
            .enumerate()
            .map(|(index, entry)| (index + offset, entry))
            .fold(self.snapshot.clone(), ClusterConfiguration::update)
    }

    fn entries(
        &self,
        prev_log_index: usize,
    ) -> Vec<LogEntry<(), usize>> {
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

    fn install_snapshot(
        &mut self,
        base_index: usize,
        base_term: usize,
        snapshot: ClusterConfiguration<usize>,
    ) {
        self.base_index = base_index;
        self.base_term = base_term;
        self.snapshot = snapshot;
        self.entries.clear();
        self.last_index = base_index;
        self.last_term = base_term;
    }

    fn truncate(
        &mut self,
        index: usize,
    ) {
        self.entries.truncate(index.saturating_sub(self.base_index));
        self.last_index = self.base_index + self.entries.len();
        self.last_term = self
            .entries
            .last()
            .map_or(self.base_term, |last_log_entry| last_log_entry.term);
    }

    fn update_snapshot(&mut self) {
        if let Some(on_update_snapshot) = self.on_update_snapshot.take() {
            on_update_snapshot(self);
        }
    }
}

pub struct MockLog {
    pub shared: Arc<Mutex<Shared>>,
}

pub struct BackEnd {
    pub shared: Arc<Mutex<Shared>>,
}

impl MockLog {
    pub fn new() -> (Self, BackEnd) {
        let shared = Arc::new(Mutex::new(Shared {
            base_term: 0,
            base_index: 0,
            dropped: false,
            entries: vec![],
            last_term: 0,
            last_index: 0,
            on_update_snapshot: None,
            snapshot: ClusterConfiguration::Single(hashset![2, 5, 6, 7, 11]),
        }));
        (
            Self {
                shared: shared.clone(),
            },
            BackEnd {
                shared,
            },
        )
    }
}

impl Log<ClusterConfiguration<usize>, usize> for MockLog {
    type Command = ();

    fn append_one(
        &mut self,
        entry: LogEntry<Self::Command, usize>,
    ) {
        let mut shared = self.shared.lock().unwrap();
        shared.append_one(entry)
    }

    fn append(
        &mut self,
        entries: Box<dyn Iterator<Item = LogEntry<Self::Command, usize>>>,
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

    fn cluster_configuration(&self) -> ClusterConfiguration<usize> {
        let shared = self.shared.lock().unwrap();
        shared.cluster_configuration()
    }

    fn entries(
        &self,
        prev_log_index: usize,
    ) -> Vec<LogEntry<Self::Command, usize>> {
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

    fn install_snapshot(
        &mut self,
        base_index: usize,
        base_term: usize,
        snapshot: ClusterConfiguration<usize>,
    ) {
        let mut shared = self.shared.lock().unwrap();
        shared.install_snapshot(base_index, base_term, snapshot);
    }

    fn last_term(&self) -> usize {
        let shared = self.shared.lock().unwrap();
        shared.last_term
    }

    fn last_index(&self) -> usize {
        let shared = self.shared.lock().unwrap();
        shared.last_index
    }

    fn snapshot(&self) -> ClusterConfiguration<usize> {
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

    fn update_snapshot(&mut self) {
        let mut shared = self.shared.lock().unwrap();
        shared.update_snapshot();
    }
}

impl Drop for MockLog {
    fn drop(&mut self) {
        let mut shared = self.shared.lock().unwrap();
        shared.dropped = true;
    }
}
