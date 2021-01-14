pub trait PersistentStorage: Send {
    fn term(&self) -> usize;
    fn voted_for(&self) -> Option<usize>;
    fn update(
        &mut self,
        term: usize,
        voted_for: Option<usize>,
    );
}
