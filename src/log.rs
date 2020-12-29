use crate::LogEntry;

pub trait Log: Send + Sync {
    type Command;

    fn append_one(
        &mut self,
        entry: LogEntry<Self::Command>,
    );
    fn append(
        &mut self,
        entries: Box<dyn Iterator<Item = LogEntry<Self::Command>>>,
    );
    fn base_term(&self) -> usize;
    fn base_index(&self) -> usize;
    fn entries(
        &self,
        prev_log_index: usize,
    ) -> Vec<LogEntry<Self::Command>>;
    fn entry_term(
        &self,
        index: usize,
    ) -> Option<usize>;
    fn install_snapshot(
        &mut self,
        base_index: usize,
        base_term: usize,
        snapshot: Vec<u8>,
    );
    fn last_term(&self) -> usize;
    fn last_index(&self) -> usize;
    fn snapshot(&self) -> Vec<u8>;
    fn truncate(
        &mut self,
        index: usize,
    );
}
