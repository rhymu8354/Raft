use crate::{
    LogEntry,
    LogEntryCommand,
};
use serde::{
    Deserialize,
    Serialize,
};
use std::collections::HashSet;

#[derive(Debug, Deserialize, Clone, Eq, PartialEq, Serialize)]
pub enum ClusterConfiguration {
    Single(HashSet<usize>),
    Joint(HashSet<usize>, HashSet<usize>),
}

impl ClusterConfiguration {
    pub fn peers(
        &self,
        self_id: usize,
    ) -> Box<dyn Iterator<Item = &usize> + '_> {
        let filter = move |id: &&usize| **id != self_id;
        match self {
            ClusterConfiguration::Single(configuration) => {
                Box::new(configuration.iter().filter(filter))
            },
            ClusterConfiguration::Joint(old, new) => {
                Box::new(old.union(&new).filter(filter))
            },
        }
    }

    pub fn update<T>(
        self,
        log_entry: &LogEntry<T>,
    ) -> Self {
        match &log_entry.command {
            Some(LogEntryCommand::SingleConfiguration {
                configuration,
                ..
            }) => ClusterConfiguration::Single(configuration.clone()),
            Some(LogEntryCommand::JointConfiguration {
                new_configuration,
                ..
            }) => match self {
                ClusterConfiguration::Single(old_configuration) => {
                    ClusterConfiguration::Joint(
                        old_configuration,
                        new_configuration.clone(),
                    )
                },
                ClusterConfiguration::Joint(old_configuration, _) => {
                    ClusterConfiguration::Joint(
                        old_configuration,
                        new_configuration.clone(),
                    )
                },
            },
            _ => self,
        }
    }
}
