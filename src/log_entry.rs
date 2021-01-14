use serde::{
    Deserialize,
    Serialize,
};
use std::{
    collections::HashSet,
    fmt::Debug,
};

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub enum Command<T> {
    FinishReconfiguration,
    StartReconfiguration(HashSet<usize>),
    Custom(T),
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct LogEntry<T> {
    pub term: usize,
    pub command: Option<Command<T>>,
}
