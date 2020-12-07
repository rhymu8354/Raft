// TODO: Revisit the way we import/export various parts of the crate.
use super::log_entry::LogEntry;
use serde::{
    Deserialize,
    Serialize,
};
use serde_json::Value as JsonValue;

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct AppendEntriesContent<T> {
    pub leader_commit: usize,
    pub prev_log_index: usize,
    pub prev_log_term: usize,
    pub log: Vec<LogEntry<T>>,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub enum MessageContent<T> {
    RequestVote {
        // TODO: Possibly remove this field, because the receiver
        // already should know the ID of the sender, and the only
        // legal value here is the sender ID.
        candidate_id: usize,
        last_log_index: usize,
        last_log_term: usize,
    },
    RequestVoteResults {
        vote_granted: bool,
    },
    AppendEntries(AppendEntriesContent<T>),
    AppendEntriesResults {
        match_index: usize,
    },
    InstallSnapshot {
        last_included_index: usize,
        last_included_term: usize,
        #[serde(with = "super::json_encoding")]
        snapshot: JsonValue,
    },
    InstallSnapshotResults {
        match_index: usize,
    },
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct Message<T> {
    pub content: MessageContent<T>,
    pub seq: usize,
    pub term: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::log_entry::{
        Command,
        CustomCommand,
    };
    use maplit::hashset;
    use serde_json::json;
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
            content: MessageContent::RequestVote {
                candidate_id: 5,
                last_log_index: 11,
                last_log_term: 3,
            },
            seq: 7,
            term: 42,
        };
        let serialized_message = to_bytes(&message_in).unwrap();
        let message_out: Message<DummyCommand> =
            from_bytes(&serialized_message).unwrap();
        assert_eq!(message_in, message_out);
    }

    #[test]
    fn request_vote_results() {
        let message_in = Message {
            content: MessageContent::RequestVoteResults {
                vote_granted: true,
            },
            seq: 8,
            term: 16,
        };
        let serialized_message = to_bytes(&message_in).unwrap();
        let message_out: Message<DummyCommand> =
            from_bytes(&serialized_message).unwrap();
        assert_eq!(message_in, message_out);
    }

    #[test]
    fn heart_beat() {
        let message_in = Message {
            content: MessageContent::AppendEntries(AppendEntriesContent {
                leader_commit: 18,
                prev_log_index: 6,
                prev_log_term: 1,
                log: vec![],
            }),
            seq: 7,
            term: 8,
        };
        let serialized_message = to_bytes(&message_in).unwrap();
        let message_out: Message<DummyCommand> =
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
                command: Some(Command::SingleConfiguration {
                    old_configuration: hashset!(2, 5, 6, 7, 11),
                    configuration: hashset!(2, 5, 6, 7, 12),
                }),
            },
        ];
        let message_in = Message {
            content: MessageContent::AppendEntries(AppendEntriesContent {
                leader_commit: 33,
                prev_log_index: 5,
                prev_log_term: 6,
                log: entries,
            }),
            seq: 9,
            term: 8,
        };
        let serialized_message = to_bytes(&message_in).unwrap();
        let message_out: Message<DummyCommand> =
            from_bytes(&serialized_message).unwrap();
        assert_eq!(message_in, message_out);
    }

    #[test]
    fn deserialize_garbage() {
        let serialized_message = "PogChamp";
        let message: Result<Message<DummyCommand>, _> =
            from_bytes(&serialized_message.as_bytes());
        assert!(message.is_err());
    }

    #[test]
    fn append_entries_results() {
        let message_in = Message {
            content: MessageContent::AppendEntriesResults {
                match_index: 10,
            },
            seq: 4,
            term: 5,
        };
        let serialized_message = to_bytes(&message_in).unwrap();
        let message_out: Message<DummyCommand> =
            from_bytes(&serialized_message).unwrap();
        assert_eq!(message_in, message_out);
    }

    #[test]
    fn install_snapshot() {
        let message_in = Message {
            content: MessageContent::InstallSnapshot {
                last_included_index: 2,
                last_included_term: 7,
                snapshot: json!({
                    "foo": "bar"
                }),
            },
            seq: 2,
            term: 8,
        };
        let serialized_message = to_bytes(&message_in).unwrap();
        let message_out: Message<DummyCommand> =
            from_bytes(&serialized_message).unwrap();
        assert_eq!(message_in, message_out);
    }

    #[test]
    fn install_snapshot_results() {
        let message_in = Message {
            content: MessageContent::InstallSnapshotResults {
                match_index: 100,
            },
            seq: 17,
            term: 8,
        };
        let serialized_message = to_bytes(&message_in).unwrap();
        let message_out: Message<DummyCommand> =
            from_bytes(&serialized_message).unwrap();
        assert_eq!(message_in, message_out);
    }
}
