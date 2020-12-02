pub trait Log: Send + Sync {
    fn base_term(&self) -> usize;
    fn base_index(&self) -> usize;
    fn last_term(&self) -> usize;
    fn last_index(&self) -> usize;
}
