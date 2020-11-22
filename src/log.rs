pub trait Log: Send + Sync {
    fn base_term(&self) -> usize;
    fn base_index(&self) -> usize;
}
