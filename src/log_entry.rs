use serde::{
    Deserialize,
    Serialize,
};
use serde_json::{
    json,
    Value as JsonValue,
};
use std::{
    collections::HashSet,
    convert::TryFrom,
    fmt::Debug,
};

pub trait CustomCommand {
    fn command_type(&self) -> &'static str;
    fn to_json(&self) -> JsonValue;
    fn from_json(json: &JsonValue) -> Option<Self>
    where
        Self: Sized;
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub enum Command<T> {
    FinishReconfiguration,
    StartReconfiguration(HashSet<usize>),
    Custom(T),
}

impl<T: CustomCommand> Command<T> {
    pub fn command_type(&self) -> &str {
        match self {
            Command::FinishReconfiguration {
                ..
            } => "FinishReconfiguration",
            Command::StartReconfiguration {
                ..
            } => "StartReconfiguration",
            Command::Custom(custom_command) => custom_command.command_type(),
        }
    }

    pub fn to_json(&self) -> JsonValue {
        // TODO: Clean this up, once I get better at serde/serde_json.  Either
        // implement serde traits (`Serialize` and `Deserialize`), or become a
        // true serde ninja by using their derive macros.
        match self {
            Command::FinishReconfiguration => JsonValue::Null,
            Command::StartReconfiguration(ids) => {
                let mut ids = ids.iter().copied().collect::<Vec<_>>();
                ids.sort_unstable();
                json!(ids)
            },
            Command::Custom(custom_command) => custom_command.to_json(),
        }
    }
}

impl<T: CustomCommand> TryFrom<&JsonValue> for Command<T> {
    type Error = ();

    fn try_from(json: &JsonValue) -> Result<Self, Self::Error> {
        let command = json.get("command");
        json.get("type")
            .and_then(JsonValue::as_str)
            .and_then(|command_type| match command_type {
                "FinishReconfiguration" => Some(Command::FinishReconfiguration),
                "StartReconfiguration" => command.map(|command| {
                    Command::StartReconfiguration(decode_instance_ids(command))
                }),
                _ => command.and_then(|command| {
                    T::from_json(command).map(Command::Custom)
                }),
            })
            .ok_or(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct LogEntry<T> {
    pub term: usize,
    pub command: Option<Command<T>>,
}

impl<T: CustomCommand> LogEntry<T> {
    pub fn to_json(&self) -> JsonValue {
        let mut json = serde_json::Map::new();
        json.insert(String::from("term"), JsonValue::from(self.term));
        if let Some(command) = &self.command {
            json.insert(
                String::from("type"),
                JsonValue::from(command.command_type()),
            );
            json.insert(String::from("command"), command.to_json());
        }
        JsonValue::Object(json)
    }
}

fn decode_instance_ids(configuration: &JsonValue) -> HashSet<usize> {
    configuration.as_array().map_or_else(HashSet::new, |instance_ids| {
        #[allow(clippy::cast_possible_truncation)]
        instance_ids
            .iter()
            .filter_map(|value| value.as_u64().map(|value| value as usize))
            .collect()
    })
}

impl<T: CustomCommand> From<&JsonValue> for LogEntry<T> {
    fn from(json: &JsonValue) -> Self {
        #[allow(clippy::cast_possible_truncation)]
        Self {
            term: json
                .get("term")
                .and_then(JsonValue::as_u64)
                .map_or(0, |term| term as usize),
            command: Command::try_from(json).ok(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use maplit::hashset;
    use serialization::{
        from_bytes,
        to_bytes,
    };

    #[derive(Debug, Eq, PartialEq, Serialize, Deserialize)]
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

    #[test]
    fn finish_reconfiguration_command_to_json() {
        let entry = LogEntry::<DummyCommand> {
            term: 9,
            command: Some(Command::FinishReconfiguration),
        };
        assert_eq!(
            json!({
                "type": "FinishReconfiguration",
                "term": 9,
                "command": JsonValue::Null,
            }),
            entry.to_json()
        );
    }

    #[test]
    fn finish_reconfiguration_command_from_json() {
        let encoded_entry = json!({
            "type": "FinishReconfiguration",
            "term": 9,
            "command": JsonValue::Null,
        });
        let log_entry = LogEntry::<DummyCommand>::from(&encoded_entry);
        assert_eq!(log_entry, LogEntry {
            term: 9,
            command: Some(Command::FinishReconfiguration),
        });
    }

    #[test]
    fn finish_reconfiguration_command_serialization() {
        let original_entry = LogEntry {
            term: 9,
            command: Some(Command::FinishReconfiguration),
        };
        let encoded_entry = to_bytes(&original_entry).unwrap();
        let decoded_entry: LogEntry<DummyCommand> =
            from_bytes(&encoded_entry).unwrap();
        assert_eq!(decoded_entry, original_entry);
    }

    #[test]
    fn start_reconfiguration_command_to_json() {
        let entry = LogEntry::<DummyCommand> {
            term: 9,
            command: Some(Command::StartReconfiguration(hashset![
                42, 85, 13531, 8354
            ])),
        };
        assert_eq!(
            json!({
                "type": "StartReconfiguration",
                "term": 9,
                "command": [42, 85, 8354, 13531],
            }),
            entry.to_json()
        );
    }

    #[test]
    fn start_reconfiguration_command_from_json() {
        let encoded_entry = json!({
            "type": "StartReconfiguration",
            "term": 9,
            "command": [42, 85, 8354, 13531],
        });
        let log_entry = LogEntry::<DummyCommand>::from(&encoded_entry);
        assert_eq!(log_entry, LogEntry {
            term: 9,
            command: Some(Command::StartReconfiguration(hashset![
                42, 85, 13531, 8354
            ])),
        });
    }

    #[test]
    fn start_reconfiguration_command_serialization() {
        let original_entry = LogEntry {
            term: 9,
            command: Some(Command::StartReconfiguration(hashset![
                42, 85, 13531, 8354
            ])),
        };
        let encoded_entry = to_bytes(&original_entry).unwrap();
        let decoded_entry: LogEntry<DummyCommand> =
            from_bytes(&encoded_entry).unwrap();
        assert_eq!(decoded_entry, original_entry);
    }

    #[test]
    fn to_json_without_command() {
        let entry = LogEntry::<DummyCommand> {
            term: 9,
            command: None,
        };
        assert_eq!(
            json!({
                "term": 9,
            }),
            entry.to_json()
        );
    }

    #[test]
    fn from_json_without_command() {
        let entry_as_json = json!({
            "term": 9,
        });
        let entry = LogEntry::<DummyCommand>::from(&entry_as_json);
        assert_eq!(9, entry.term);
        assert!(entry.command.is_none());
    }

    #[test]
    fn empty_command_serialization() {
        let original_entry = LogEntry {
            term: 9,
            command: None,
        };
        let encoded_entry = to_bytes(&original_entry).unwrap();
        let decoded_entry: LogEntry<DummyCommand> =
            from_bytes(&encoded_entry).unwrap();
        assert_eq!(decoded_entry, original_entry);
    }

    #[test]
    fn custom_command() {
        // Invent a custom command for this test.
        #[derive(Debug, Eq, PartialEq, Serialize, Deserialize)]
        struct PogChamp {
            payload: usize,
        }

        impl CustomCommand for PogChamp {
            fn command_type(&self) -> &'static str {
                "PogChamp"
            }

            fn to_json(&self) -> JsonValue {
                json!({
                    "payload": self.payload,
                })
            }

            fn from_json(json: &JsonValue) -> Option<Self> {
                #[allow(clippy::cast_possible_truncation)]
                json.get("payload")
                    .and_then(JsonValue::as_u64)
                    .map(|payload| payload as usize)
                    .map(|payload| Self {
                        payload,
                    })
            }
        }

        // Create an instance of the custom command.
        let pog_champ = PogChamp {
            payload: 42,
        };
        let pog_champ_entry = LogEntry {
            term: 8,
            command: Some(Command::Custom(pog_champ)),
        };

        // Turn the custom command into JSON, verifying it has
        // the expected rendering.
        let serialized_pog_champ = pog_champ_entry.to_json();
        assert_eq!(
            json!({
                "term": 8,
                "type": "PogChamp",
                "command": {
                    "payload": 42,
                },
            }),
            serialized_pog_champ
        );

        // Take the JSON and turn it around to form the command again.
        let log_entry = LogEntry::<PogChamp>::from(&serialized_pog_champ);

        // Verify the command has all the expected values in it.
        assert_eq!(log_entry, pog_champ_entry);

        // Run the command through the full round-trip of serialization.
        let encoded_entry = to_bytes(&log_entry).unwrap();
        let decoded_entry: LogEntry<PogChamp> =
            from_bytes(&encoded_entry).unwrap();
        assert_eq!(decoded_entry, log_entry);
    }
}
