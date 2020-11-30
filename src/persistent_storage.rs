pub trait PersistentStorage: Send + Sync {
    fn term(&self) -> usize;
    fn voted_for(&self) -> Option<usize>;
    fn update(
        &mut self,
        term: usize,
        voted_for: Option<usize>,
    );
}
