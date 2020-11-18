// TODO: Revisit the way we import/export various parts of the crate.
use super::log_entry::{
    CustomCommand,
    LogEntry,
};
use serde::{
    Deserialize,
    Serialize,
};
use serde_json::Value as JsonValue;
use serialization::{
    from_bytes,
    to_bytes,
};

// TODO: I don't know what I want to do with this, but it certainly
// probably maybe doesn't belong here.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("ran out of bytes while deserializing message")]
    MessageTruncated,

    #[error("encountered a message serialized as an unknown version")]
    UnsupportedVersion,

    #[error("encountered a message of an unknown type")]
    UnknownMessageType,

    #[error("error decoding text in serialized message")]
    Utf8(std::str::Utf8Error),

    #[error("error parsing log entry from serialized message")]
    BadLogEntry(serde_json::Error),

    #[error("error parsing snapshot from serialized message")]
    BadSnapshot(serde_json::Error),
}

#[derive(Debug, Serialize, Deserialize)]
enum MessageContent<T> {
    RequestVote {
        candidate_id: usize,
        last_log_index: usize,
        last_log_term: usize,
    },
    RequestVoteResults {
        vote_granted: bool,
    },
    AppendEntries {
        leader_commit: usize,
        prev_log_index: usize,
        prev_log_term: usize,
        log: Vec<LogEntry<T>>,
    },
    AppendEntriesResults {
        match_index: usize,
        success: bool,
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

#[derive(Debug, Serialize, Deserialize)]
struct Message<T> {
    term: usize,
    seq: usize,
    content: MessageContent<T>,
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
        let message_in = Message::<DummyCommand> {
            term: 42,
            seq: 7,
            content: MessageContent::RequestVote {
                candidate_id: 5,
                last_log_index: 11,
                last_log_term: 3,
            },
        };
        let serialized_message = dbg!(to_bytes(&message_in).unwrap());
        let message: Message<DummyCommand> =
            from_bytes(&serialized_message).unwrap();
        assert_eq!(42, message.term);
        assert_eq!(7, message.seq);
        if let MessageContent::RequestVote {
            candidate_id,
            last_log_index,
            last_log_term,
        } = message.content
        {
            assert_eq!(5, candidate_id);
            assert_eq!(11, last_log_index);
            assert_eq!(3, last_log_term);
        } else {
            panic!("RequestVote message expected");
        }
    }

    #[test]
    fn request_vote_results() {
        let message_in = Message::<DummyCommand> {
            term: 16,
            seq: 8,
            content: MessageContent::RequestVoteResults {
                vote_granted: true,
            },
        };
        let serialized_message = to_bytes(&message_in).unwrap();
        let message: Message<DummyCommand> =
            from_bytes(&serialized_message).unwrap();
        assert_eq!(16, message.term);
        assert_eq!(8, message.seq);
        if let MessageContent::RequestVoteResults {
            vote_granted,
        } = message.content
        {
            assert!(vote_granted);
        } else {
            panic!("RequestVoteResults message expected");
        }
    }

    #[test]
    fn heart_beat() {
        let message_in = Message::<DummyCommand> {
            term: 8,
            seq: 7,
            content: MessageContent::AppendEntries {
                leader_commit: 18,
                prev_log_index: 6,
                prev_log_term: 1,
                log: vec![],
            },
        };
        let serialized_message = to_bytes(&message_in).unwrap();
        let message: Message<DummyCommand> =
            from_bytes(&serialized_message).unwrap();
        assert_eq!(8, message.term);
        assert_eq!(7, message.seq);
        if let MessageContent::AppendEntries {
            leader_commit,
            prev_log_index,
            prev_log_term,
            log,
        } = message.content
        {
            assert_eq!(18, leader_commit);
            assert_eq!(6, prev_log_index);
            assert_eq!(1, prev_log_term);
            assert!(log.is_empty());
        } else {
            panic!("RequestVoteResults message expected");
        }
    }

    #[test]
    fn append_entries_with_content() {
        let entries = vec![
            LogEntry::<DummyCommand> {
                term: 7,
                command: None,
            },
            LogEntry::<DummyCommand> {
                term: 8,
                command: Some(Command::SingleConfiguration {
                    old_configuration: hashset!(2, 5, 6, 7, 11),
                    configuration: hashset!(2, 5, 6, 7, 12),
                }),
            },
        ];
        let message_in = Message {
            term: 8,
            seq: 9,
            content: MessageContent::AppendEntries {
                leader_commit: 33,
                prev_log_index: 5,
                prev_log_term: 6,
                log: entries.clone(),
            },
        };
        let serialized_message = to_bytes(&message_in).unwrap();
        let message: Message<DummyCommand> =
            from_bytes(&serialized_message).unwrap();
        assert_eq!(8, message.term);
        assert_eq!(9, message.seq);
        if let MessageContent::AppendEntries {
            leader_commit,
            prev_log_index,
            prev_log_term,
            log,
        } = message.content
        {
            assert_eq!(33, leader_commit);
            assert_eq!(5, prev_log_index);
            assert_eq!(6, prev_log_term);
            assert_eq!(entries, log);
        } else {
            panic!("AppendEntries message expected");
        }
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
        let message_in = Message::<DummyCommand> {
            term: 5,
            seq: 4,
            content: MessageContent::AppendEntriesResults {
                match_index: 10,
                success: false,
            },
        };
        let serialized_message = to_bytes(&message_in).unwrap();
        let message: Message<DummyCommand> =
            from_bytes(&serialized_message).unwrap();
        assert_eq!(5, message.term);
        assert_eq!(4, message.seq);
        if let MessageContent::AppendEntriesResults {
            match_index,
            success,
        } = message.content
        {
            assert_eq!(10, match_index);
            assert!(!success);
        } else {
            panic!("AppendEntriesResults message expected");
        }
    }

    #[test]
    fn install_snapshot() {
        let message_in = Message::<DummyCommand> {
            term: 8,
            seq: 2,
            content: MessageContent::InstallSnapshot {
                last_included_index: 2,
                last_included_term: 7,
                snapshot: json!({
                    "foo": "bar"
                }),
            },
        };
        let serialized_message = dbg!(to_bytes(&message_in).unwrap());
        let message: Message<DummyCommand> =
            from_bytes(&serialized_message).unwrap();
        assert_eq!(8, message.term);
        assert_eq!(2, message.seq);
        if let MessageContent::InstallSnapshot {
            last_included_index,
            last_included_term,
            snapshot,
        } = message.content
        {
            assert_eq!(2, last_included_index);
            assert_eq!(7, last_included_term);
            if let MessageContent::InstallSnapshot {
                snapshot: snapshot_in,
                ..
            } = message_in.content
            {
                assert_eq!(snapshot_in, snapshot);
            } else {
                panic!("message in was not an InstallSnapshot");
            }
        } else {
            panic!("InstallSnapshot message expected");
        }
    }

    #[test]
    fn install_snapshot_results() {
        let message_in = Message::<DummyCommand> {
            term: 8,
            seq: 17,
            content: MessageContent::InstallSnapshotResults {
                match_index: 100,
            },
        };
        let serialized_message = to_bytes(&message_in).unwrap();
        let message: Message<DummyCommand> =
            from_bytes(&serialized_message).unwrap();
        assert_eq!(8, message.term);
        assert_eq!(17, message.seq);
        if let MessageContent::InstallSnapshotResults {
            match_index,
        } = message.content
        {
            assert_eq!(100, match_index);
        } else {
            panic!("InstallSnapshotResults message expected");
        }
    }
}
