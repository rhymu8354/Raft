use crate::LogEntry;

pub trait Log: Send + Sync {
    type Command;

    fn append(
        &mut self,
        entry: LogEntry<Self::Command>,
    );
    fn base_term(&self) -> usize;
    fn base_index(&self) -> usize;
    fn last_term(&self) -> usize;
    fn last_index(&self) -> usize;
}
