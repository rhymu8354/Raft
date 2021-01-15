/// This represents the set of requirements that a Raft [`Server`]
/// has on its host in terms of maintaining variables which persist
/// from one lifetime of the server to the next (in other words, when
/// the server is stopped and restarted later).
///
/// [`Server`]: struct.Server.html
pub trait PersistentStorage: Send {
    /// Obtain the current cluster leadership term maintained for the server.
    fn term(&self) -> usize;

    /// Obtain the record of whether or not the server voted for leadership
    /// of the cluster in the current term, and if so, what is the identifier
    /// of the server voted for.
    fn voted_for(&self) -> Option<usize>;

    /// Update the current cluster leadership term and voting record
    /// of the server.
    fn update(
        &mut self,
        term: usize,
        voted_for: Option<usize>,
    );
}
