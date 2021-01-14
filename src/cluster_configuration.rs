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
    Joint {
        old_ids: HashSet<usize>,
        new_ids: HashSet<usize>,
        index: usize,
    },
}

impl ClusterConfiguration {
    #[must_use]
    pub fn contains(
        &self,
        id: usize,
    ) -> bool {
        match self {
            ClusterConfiguration::Single(ids) => ids.contains(&id),
            ClusterConfiguration::Joint {
                old_ids,
                new_ids,
                ..
            } => old_ids.contains(&id) || new_ids.contains(&id),
        }
    }

    #[must_use]
    pub fn peers(
        &self,
        self_id: usize,
    ) -> Box<dyn Iterator<Item = &usize> + '_> {
        let filter = move |id: &&usize| **id != self_id;
        match self {
            ClusterConfiguration::Single(configuration) => {
                Box::new(configuration.iter().filter(filter))
            },
            ClusterConfiguration::Joint {
                old_ids,
                new_ids,
                ..
            } => Box::new(old_ids.union(&new_ids).filter(filter)),
        }
    }

    pub fn update<T>(
        self,
        (index, log_entry): (usize, &LogEntry<T>),
    ) -> Self {
        match &log_entry.command {
            Some(LogEntryCommand::FinishReconfiguration) => match self {
                ClusterConfiguration::Single(new_ids)
                | ClusterConfiguration::Joint {
                    new_ids,
                    ..
                } => ClusterConfiguration::Single(new_ids),
            },
            Some(LogEntryCommand::StartReconfiguration(new_ids)) => {
                match self {
                    ClusterConfiguration::Single(old_ids)
                    | ClusterConfiguration::Joint {
                        old_ids,
                        ..
                    } => ClusterConfiguration::Joint {
                        old_ids,
                        new_ids: new_ids.clone(),
                        index,
                    },
                }
            },
            _ => self,
        }
    }
}
