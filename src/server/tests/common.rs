use super::*;

use crate::{
    Log,
    PersistentStorage,
};
use std::sync::{
    atomic::AtomicBool,
    Arc,
};

struct MockLog {
    dropped: Arc<AtomicBool>,
}

impl MockLog {
    fn new() -> Self {
        Self {
            dropped: Arc::new(AtomicBool::new(false)),
        }
    }
}

impl Log for MockLog {}

impl Drop for MockLog {
    fn drop(&mut self) {
        self.dropped.store(true, std::sync::atomic::Ordering::SeqCst);
    }
}

struct MockPersistentStorage {
    dropped: Arc<AtomicBool>,
}

impl MockPersistentStorage {
    fn new() -> Self {
        Self {
            dropped: Arc::new(AtomicBool::new(false)),
        }
    }
}

impl PersistentStorage for MockPersistentStorage {}

impl Drop for MockPersistentStorage {
    fn drop(&mut self) {
        self.dropped.store(true, std::sync::atomic::Ordering::SeqCst);
    }
}

struct MockScheduler {
    dropped: Arc<AtomicBool>,
}

impl MockScheduler {
    fn new() -> Self {
        Self {
            dropped: Arc::new(AtomicBool::new(false)),
        }
    }
}

impl Scheduler for MockScheduler {}

impl Drop for MockScheduler {
    fn drop(&mut self) {
        self.dropped.store(true, std::sync::atomic::Ordering::SeqCst);
    }
}

fn mobilize_server(server: &mut Server) {
    let mock_log = Arc::new(MockLog::new());
    let mock_persistent_storage = Arc::new(MockPersistentStorage::new());
    let mock_scheduler = Arc::new(MockScheduler::new());
    server.mobilize(mock_log, mock_persistent_storage, mock_scheduler);
}

#[test]
fn mobilize_twice_does_not_crash() {
    let mut server = Server::new();
    mobilize_server(&mut server);
    mobilize_server(&mut server);
}

#[test]
fn log_keeper_released_on_demobilize() {
    let mock_log = MockLog::new();
    let log_dropped = mock_log.dropped.clone();
    let mock_persistent_storage = MockPersistentStorage::new();
    let mock_scheduler = MockScheduler::new();
    let mut server = Server::new();
    server.mobilize(
        Arc::new(mock_log),
        Arc::new(mock_persistent_storage),
        Arc::new(mock_scheduler),
    );
    server.demobilize();
    assert!(log_dropped.load(std::sync::atomic::Ordering::SeqCst));
}

#[test]
fn persistent_state_released_on_demobilize() {
    let mock_log = MockLog::new();
    let mock_persistent_storage = MockPersistentStorage::new();
    let log_dropped = mock_persistent_storage.dropped.clone();
    let mock_scheduler = MockScheduler::new();
    let mut server = Server::new();
    server.mobilize(
        Arc::new(mock_log),
        Arc::new(mock_persistent_storage),
        Arc::new(mock_scheduler),
    );
    server.demobilize();
    assert!(log_dropped.load(std::sync::atomic::Ordering::SeqCst));
}

#[test]
fn scheduler_released_on_demobilize() {
    let mock_log = MockLog::new();
    let mock_persistent_storage = MockPersistentStorage::new();
    let mock_scheduler = MockScheduler::new();
    let log_dropped = mock_scheduler.dropped.clone();
    let mut server = Server::new();
    server.mobilize(
        Arc::new(mock_log),
        Arc::new(mock_persistent_storage),
        Arc::new(mock_scheduler),
    );
    server.demobilize();
    assert!(log_dropped.load(std::sync::atomic::Ordering::SeqCst));
}
