use crate::{
    LogEntry,
    Snapshot,
};
use serde::{
    Deserialize,
    Serialize,
};

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct AppendEntriesContent<T> {
    pub leader_commit: usize,
    pub prev_log_index: usize,
    pub prev_log_term: usize,
    pub log: Vec<LogEntry<T>>,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub enum Content<S, T> {
    RequestVote {
        last_log_index: usize,
        last_log_term: usize,
    },
    RequestVoteResponse {
        vote_granted: bool,
    },
    AppendEntries(AppendEntriesContent<T>),
    AppendEntriesResponse {
        success: bool,
        next_log_index: usize,
    },
    InstallSnapshot {
        last_included_index: usize,
        last_included_term: usize,
        snapshot: Snapshot<S>,
    },
    InstallSnapshotResponse,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct Message<S, T> {
    pub content: Content<S, T>,
    pub seq: usize,
    pub term: usize,
}

#[allow(clippy::string_lit_as_bytes)]
#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        log_entry::{
            Command,
            CustomCommand,
        },
        ClusterConfiguration,
    };
    use maplit::hashset;
    use serde_json::Value as JsonValue;
    use serialization::{
        from_bytes,
        to_bytes,
    };
    use std::fmt::Debug;

    // TODO: Extract this out so that it can be shared with the `message`
    // unit tests.
    #[derive(Clone, Serialize, Deserialize)]
    struct DummyCommand {}

    impl CustomCommand for DummyCommand {
        fn command_type(&self) -> &'static str {
            "POGGERS"
        }

        fn to_json(&self) -> JsonValue {
            JsonValue::Null
        }

        fn from_json(_json: &JsonValue) -> Option<Self>
        where
            Self: Sized,
        {
            None
        }
    }

    impl Debug for DummyCommand {
        fn fmt(
            &self,
            f: &mut std::fmt::Formatter<'_>,
        ) -> std::fmt::Result {
            write!(f, "PogChamp")
        }
    }

    impl PartialEq for DummyCommand {
        fn eq(
            &self,
            _other: &Self,
        ) -> bool {
            true
        }
    }

    #[test]
    fn request_vote() {
        let message_in = Message {
            content: Content::RequestVote {
                last_log_index: 11,
                last_log_term: 3,
            },
            seq: 7,
            term: 42,
        };
        let serialized_message = to_bytes(&message_in).unwrap();
        let message_out: Message<(), DummyCommand> =
            from_bytes(&serialized_message).unwrap();
        assert_eq!(message_in, message_out);
    }

    #[test]
    fn request_vote_response() {
        let message_in = Message {
            content: Content::RequestVoteResponse {
                vote_granted: true,
            },
            seq: 8,
            term: 16,
        };
        let serialized_message = to_bytes(&message_in).unwrap();
        let message_out: Message<(), DummyCommand> =
            from_bytes(&serialized_message).unwrap();
        assert_eq!(message_in, message_out);
    }

    #[test]
    fn heart_beat() {
        let message_in = Message {
            content: Content::AppendEntries(AppendEntriesContent {
                leader_commit: 18,
                prev_log_index: 6,
                prev_log_term: 1,
                log: vec![],
            }),
            seq: 7,
            term: 8,
        };
        let serialized_message = to_bytes(&message_in).unwrap();
        let message_out: Message<(), DummyCommand> =
            from_bytes(&serialized_message).unwrap();
        assert_eq!(message_in, message_out);
    }

    #[test]
    fn append_entries_with_content() {
        let entries = vec![
            LogEntry {
                term: 7,
                command: None,
            },
            LogEntry {
                term: 8,
                command: Some(Command::FinishReconfiguration),
            },
        ];
        let message_in = Message {
            content: Content::AppendEntries(AppendEntriesContent {
                leader_commit: 33,
                prev_log_index: 5,
                prev_log_term: 6,
                log: entries,
            }),
            seq: 9,
            term: 8,
        };
        let serialized_message = to_bytes(&message_in).unwrap();
        let message_out: Message<(), DummyCommand> =
            from_bytes(&serialized_message).unwrap();
        assert_eq!(message_in, message_out);
    }

    #[test]
    fn deserialize_garbage() {
        let serialized_message = "PogChamp";
        let message: Result<Message<(), DummyCommand>, _> =
            from_bytes(&serialized_message.as_bytes());
        assert!(message.is_err());
    }

    #[test]
    fn append_entries_response() {
        let message_in = Message {
            content: Content::AppendEntriesResponse {
                success: true,
                next_log_index: 10,
            },
            seq: 4,
            term: 5,
        };
        let serialized_message = to_bytes(&message_in).unwrap();
        let message_out: Message<(), DummyCommand> =
            from_bytes(&serialized_message).unwrap();
        assert_eq!(message_in, message_out);
    }

    #[test]
    fn install_snapshot() {
        let message_in = Message {
            content: Content::InstallSnapshot {
                last_included_index: 2,
                last_included_term: 7,
                snapshot: Snapshot {
                    cluster_configuration: ClusterConfiguration::Single(
                        hashset![2, 5, 6, 7, 11],
                    ),
                    state: (),
                },
            },
            seq: 2,
            term: 8,
        };
        let serialized_message = to_bytes(&message_in).unwrap();
        let message_out: Message<(), DummyCommand> =
            from_bytes(&serialized_message).unwrap();
        assert_eq!(message_in, message_out);
    }

    #[test]
    fn install_snapshot_response() {
        let message_in = Message {
            content: Content::InstallSnapshotResponse,
            seq: 17,
            term: 8,
        };
        let serialized_message = to_bytes(&message_in).unwrap();
        let message_out: Message<(), DummyCommand> =
            from_bytes(&serialized_message).unwrap();
        assert_eq!(message_in, message_out);
    }
}
