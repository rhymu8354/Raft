use crate::{
    LogEntry,
    LogEntryCommand,
};
use serde::{
    Deserialize,
    Serialize,
};
use std::{
    collections::HashSet,
    hash::Hash,
};

/// This holds the identifiers of the servers in the Raft cluster,
/// and indicates whether or not the cluster is in a "joint" configuration
/// (transitioning from one set of servers to another).
#[derive(Debug, Deserialize, Clone, Eq, PartialEq, Serialize)]
pub enum ClusterConfiguration<Id>
where
    Id: Eq + Hash,
{
    /// This holds the set of identifiers of servers in the cluster, and
    /// indicates that the cluster is not transitioning from one set of
    /// servers to another.
    Single(HashSet<Id>),

    /// This holds the sets of identifiers of servers in the cluster, both
    /// before and after the current configuration change, and indicates
    /// that the cluster is transitioning from one set of servers to
    /// another.
    Joint {
        /// This is the set of identifiers of servers in the cluster
        /// before the configuration change.
        old_ids: HashSet<Id>,

        /// This is the set of identifiers of servers in the cluster
        /// after the configuration change.
        new_ids: HashSet<Id>,

        /// This is the index of the log entry which started the change
        /// in cluster configuration.
        index: usize,
    },
}

impl<Id> ClusterConfiguration<Id>
where
    Id: Eq + Hash,
{
    /// Determine if the server with the given identifier is a member
    /// of the cluster.
    #[must_use]
    pub fn contains(
        &self,
        id: Id,
    ) -> bool
    where
        Id: Copy,
    {
        match self {
            ClusterConfiguration::Single(ids) => ids.contains(&id),
            ClusterConfiguration::Joint {
                old_ids,
                new_ids,
                ..
            } => old_ids.contains(&id) || new_ids.contains(&id),
        }
    }

    /// Return an iterator of the identifiers of the servers in the cluster.
    #[must_use]
    pub fn ids(&self) -> Box<dyn Iterator<Item = Id> + '_>
    where
        Id: Copy,
    {
        match self {
            ClusterConfiguration::Single(configuration) => {
                Box::new(configuration.iter().copied())
            },
            ClusterConfiguration::Joint {
                old_ids,
                new_ids,
                ..
            } => Box::new(old_ids.union(&new_ids).copied()),
        }
    }

    /// Return an iterator of the identifiers of the servers in the cluster
    /// which are a peer of the given server (all identifiers *except*
    /// the given one).
    #[must_use]
    pub fn peers(
        &self,
        self_id: Id,
    ) -> Box<dyn Iterator<Item = Id> + '_>
    where
        Id: Copy,
    {
        Box::new(self.ids().filter(move |id| *id != self_id))
    }

    /// Apply the given log entry (if applicable) to a cluster configuration,
    /// returning it after application.
    ///
    /// [`LogEntryCommand::StartReconfiguration`] applied to the configuration
    /// will transition to [`Joint`] configuration.
    ///
    /// [`LogEntryCommand::FinishReconfiguration`] applied to the configuration
    /// will transition to [`Single`] configuration.
    ///
    /// [`LogEntryCommand::StartReconfiguration`]:
    /// enum.LogEntryCommand.html#variant.StartReconfiguration
    /// [`LogEntryCommand::FinishReconfiguration`]:
    /// enum.LogEntryCommand.html#variant.FinishReconfiguration
    /// [`Joint`]: #variant.Joint
    /// [`Single`]: #variant.Single
    pub fn update<T>(
        self,
        (index, log_entry): (usize, &LogEntry<T, Id>),
    ) -> Self
    where
        Id: Clone,
    {
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

#[cfg(test)]
mod tests {
    use super::*;
    use maplit::hashset;
    use serde_json::json;

    #[test]
    fn deserialize_json_single() {
        let serialization = json!({
            "Single": [10, 11, 12]
        });
        let deserialization = serde_json::from_value(serialization).unwrap();
        assert_eq!(
            ClusterConfiguration::Single(hashset![10, 11, 12]),
            deserialization
        );
    }

    #[test]
    fn deserialize_json_joint() {
        let serialization = json!({
            "Joint": {
                "old_ids": [10, 11, 12],
                "new_ids": [10, 11, 12, 13],
                "index": 42
            }
        });
        let deserialization = serde_json::from_value(serialization).unwrap();
        assert_eq!(
            ClusterConfiguration::Joint {
                old_ids: hashset![10, 11, 12],
                new_ids: hashset![10, 11, 12, 13],
                index: 42
            },
            deserialization
        );
    }
}
