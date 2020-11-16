// TODO: Revisit the way we import/export various parts of the crate.
use super::log_entry::{
    CustomCommand,
    LogEntry,
};
use serde_json::Value as JsonValue;

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
}

#[derive(Debug)]
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
        snapshot: JsonValue,
    },
    InstallSnapshotResults {
        match_index: usize,
    },
}

#[derive(Debug)]
struct Message<T> {
    term: usize,
    seq: usize,
    content: MessageContent<T>,
}

impl<T: CustomCommand> Message<T> {
    fn serialize_usize(
        buf: &mut Vec<u8>,
        mut value: usize,
    ) {
        // TODO: This is ripe for optimization!
        let mut stack = Vec::new();
        stack.reserve(8);
        loop {
            stack.push((value & 0x7F) as u8);
            value >>= 7;
            if value == 0 {
                break;
            }
        }
        while !stack.is_empty() {
            let mut next = stack.pop().unwrap();
            if !stack.is_empty() {
                next |= 0x80;
            }
            buf.push(next);
        }
    }

    fn serialize_str<S>(
        buf: &mut Vec<u8>,
        value: S,
    ) where
        S: AsRef<str>,
    {
        let value = value.as_ref();
        Self::serialize_usize(buf, value.len());
        buf.extend(value.as_bytes());
    }

    fn deserialize_str(serialization: &[u8]) -> Result<(&str, &[u8]), Error> {
        let (len, serialization) = Self::deserialize_usize(serialization)?;
        if serialization.len() < len {
            return Err(Error::MessageTruncated);
        }
        let value = &serialization[0..len];
        let serialization = &serialization[len..];
        Ok((std::str::from_utf8(value).map_err(Error::Utf8)?, serialization))
    }

    fn deserialize_usize(
        mut serialization: &[u8]
    ) -> Result<(usize, &[u8]), Error> {
        // TODO: This is ripe for optimization!
        let mut value = 0;
        loop {
            if serialization.is_empty() {
                return Err(Error::MessageTruncated);
            }
            let next = serialization[0];
            serialization = &serialization[1..];
            value <<= 7;
            value |= (next & 0x7F) as usize;
            if (next & 0x80) == 0 {
                break;
            }
        }
        Ok((value, serialization))
    }

    fn deserialize<S>(serialization: S) -> Result<Self, Error>
    where
        S: AsRef<[u8]>,
    {
        let serialization = serialization.as_ref();
        if serialization.is_empty() {
            return Err(Error::MessageTruncated);
        }
        let version = serialization[0];
        let serialization = &serialization[1..];
        if version == 1 {
            let (term, serialization) = Self::deserialize_usize(serialization)?;
            let (seq, serialization) = Self::deserialize_usize(serialization)?;
            if serialization.is_empty() {
                return Err(Error::MessageTruncated);
            }
            let type_encoding = serialization[0];
            let serialization = &serialization[1..];
            Ok(Message {
                term,
                seq,
                content: match type_encoding {
                    1 => {
                        let (candidate_id, serialization) =
                            Self::deserialize_usize(serialization)?;
                        let (last_log_index, serialization) =
                            Self::deserialize_usize(serialization)?;
                        let (last_log_term, _) =
                            Self::deserialize_usize(serialization)?;
                        MessageContent::RequestVote {
                            candidate_id,
                            last_log_index,
                            last_log_term,
                        }
                    },
                    2 => {
                        if serialization.is_empty() {
                            return Err(Error::MessageTruncated);
                        }
                        let vote_granted = serialization[0];
                        MessageContent::RequestVoteResults {
                            vote_granted: vote_granted != 0,
                        }
                    },
                    3 => {
                        let (leader_commit, serialization) =
                            Self::deserialize_usize(serialization)?;
                        let (prev_log_index, serialization) =
                            Self::deserialize_usize(serialization)?;
                        let (prev_log_term, serialization) =
                            Self::deserialize_usize(serialization)?;
                        let (num_log_entries, mut serialization) =
                            Self::deserialize_usize(serialization)?;
                        let mut log = Vec::<LogEntry<T>>::new();
                        log.reserve(num_log_entries);
                        for _ in 0..num_log_entries {
                            let (serialized_log_entry, remainder) =
                                Self::deserialize_str(serialization)?;
                            serialization = remainder;
                            log.push(LogEntry::from(
                                &serde_json::from_str::<JsonValue>(
                                    serialized_log_entry,
                                )
                                .map_err(Error::BadLogEntry)?,
                            ))
                        }
                        MessageContent::AppendEntries {
                            leader_commit,
                            prev_log_index,
                            prev_log_term,
                            log,
                        }
                    },
                    4 => {
                        if serialization.is_empty() {
                            return Err(Error::MessageTruncated);
                        }
                        let success = serialization[0];
                        let serialization = &serialization[1..];
                        let (match_index, _) =
                            Self::deserialize_usize(serialization)?;
                        MessageContent::AppendEntriesResults {
                            match_index,
                            success: success != 0,
                        }
                    },
                    5 => {
                        let (last_included_index, remainder) =
                            Self::deserialize_usize(serialization)?;
                        let (last_included_term, remainder) =
                            Self::deserialize_usize(remainder)?;
                        let (snapshot, _) = Self::deserialize_str(remainder)?;
                        MessageContent::InstallSnapshot {
                            last_included_index,
                            last_included_term,
                            snapshot: serde_json::from_str::<JsonValue>(
                                snapshot,
                            )
                            .map_err(Error::BadLogEntry)?,
                        }
                    },
                    6 => {
                        let (match_index, _) =
                            Self::deserialize_usize(serialization)?;
                        MessageContent::InstallSnapshotResults {
                            match_index,
                        }
                    },
                    _ => return Err(Error::UnknownMessageType),
                },
            })
        } else {
            Err(Error::UnsupportedVersion)
        }
    }

    fn serialize(&self) -> Vec<u8> {
        let mut serialization = Vec::new();
        serialization.reserve(16);
        serialization.push(1); // version
        Self::serialize_usize(&mut serialization, self.term);
        Self::serialize_usize(&mut serialization, self.seq);
        match &self.content {
            MessageContent::RequestVote {
                candidate_id,
                last_log_index,
                last_log_term,
            } => {
                serialization.push(1); // RequestVote
                Self::serialize_usize(&mut serialization, *candidate_id);
                Self::serialize_usize(&mut serialization, *last_log_index);
                Self::serialize_usize(&mut serialization, *last_log_term);
            },
            MessageContent::RequestVoteResults {
                vote_granted,
            } => {
                serialization.push(2); // RequestVoteResults
                serialization.push(if *vote_granted {
                    1
                } else {
                    0
                });
            },
            MessageContent::AppendEntries {
                leader_commit,
                prev_log_index,
                prev_log_term,
                log,
            } => {
                serialization.push(3); // AppendEntries
                Self::serialize_usize(&mut serialization, *leader_commit);
                Self::serialize_usize(&mut serialization, *prev_log_index);
                Self::serialize_usize(&mut serialization, *prev_log_term);
                Self::serialize_usize(&mut serialization, log.len());
                log.iter().for_each(|entry| {
                    Self::serialize_str(
                        &mut serialization,
                        entry.to_json().to_string(),
                    );
                })
            },
            MessageContent::AppendEntriesResults {
                match_index,
                success,
            } => {
                serialization.push(4); // AppendEntriesResults
                serialization.push(if *success {
                    1
                } else {
                    0
                });
                Self::serialize_usize(&mut serialization, *match_index);
            },
            MessageContent::InstallSnapshot {
                last_included_index,
                last_included_term,
                snapshot,
            } => {
                serialization.push(5); // InstallSnapshot
                Self::serialize_usize(&mut serialization, *last_included_index);
                Self::serialize_usize(&mut serialization, *last_included_term);
                Self::serialize_str(&mut serialization, snapshot.to_string());
            },
            MessageContent::InstallSnapshotResults {
                match_index,
            } => {
                serialization.push(6); // InstallSnapshotResults
                Self::serialize_usize(&mut serialization, *match_index);
            },
        };
        serialization
    }
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
    #[derive(Clone)]
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
        let serialized_message = message_in.serialize();
        let message = Message::<DummyCommand>::deserialize(&serialized_message);
        assert!(message.is_ok());
        let message = message.unwrap();
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
        let serialized_message = message_in.serialize();
        let message = Message::<DummyCommand>::deserialize(&serialized_message);
        assert!(message.is_ok());
        let message = message.unwrap();
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
        let serialized_message = message_in.serialize();
        let message = Message::<DummyCommand>::deserialize(&serialized_message);
        assert!(message.is_ok());
        let message = message.unwrap();
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
        let serialized_message = message_in.serialize();
        let message = Message::deserialize(&serialized_message);
        assert!(message.is_ok());
        let message = message.unwrap();
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
        let message = Message::<DummyCommand>::deserialize(serialized_message);
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
        let serialized_message = message_in.serialize();
        let message = Message::<DummyCommand>::deserialize(serialized_message);
        assert!(message.is_ok());
        let message = message.unwrap();
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
        let serialized_message = message_in.serialize();
        let message = Message::<DummyCommand>::deserialize(serialized_message);
        assert!(message.is_ok());
        let message = message.unwrap();
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
        let serialized_message = message_in.serialize();
        let message = Message::<DummyCommand>::deserialize(serialized_message);
        assert!(message.is_ok());
        let message = message.unwrap();
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
