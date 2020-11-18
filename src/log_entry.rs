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
    SingleConfiguration {
        old_configuration: HashSet<usize>,
        configuration: HashSet<usize>,
    },
    JointConfiguration {
        old_configuration: HashSet<usize>,
        new_configuration: HashSet<usize>,
    },
    Custom(T),
}

impl<T: CustomCommand> Command<T> {
    pub fn command_type(&self) -> &str {
        match self {
            Command::SingleConfiguration {
                ..
            } => "SingleConfiguration",
            Command::JointConfiguration {
                ..
            } => "JointConfiguration",
            Command::Custom(custom_command) => custom_command.command_type(),
        }
    }

    pub fn to_json(&self) -> JsonValue {
        // TODO: Clean this up, once I get better at serde/serde_json.  Either
        // implement serde traits (`Serialize` and `Deserialize`), or become a
        // true serde ninja by using their derive macros.
        match self {
            Command::SingleConfiguration {
                configuration,
                old_configuration,
            } => {
                let mut configuration =
                    configuration.iter().copied().collect::<Vec<_>>();
                configuration.sort_unstable();
                let mut old_configuration =
                    old_configuration.iter().copied().collect::<Vec<_>>();
                old_configuration.sort_unstable();
                json!({
                    "configuration": {
                        "instanceIds": configuration
                    },
                    "oldConfiguration": {
                        "instanceIds": old_configuration
                    },
                })
            },
            Command::JointConfiguration {
                new_configuration,
                old_configuration,
            } => {
                let mut new_configuration =
                    new_configuration.iter().copied().collect::<Vec<_>>();
                new_configuration.sort_unstable();
                let mut old_configuration =
                    old_configuration.iter().copied().collect::<Vec<_>>();
                old_configuration.sort_unstable();
                json!({
                    "newConfiguration": {
                        "instanceIds": new_configuration
                    },
                    "oldConfiguration": {
                        "instanceIds": old_configuration
                    },
                })
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
                "SingleConfiguration" => {
                    command.map(|command| Command::SingleConfiguration {
                        configuration: command
                            .get("configuration")
                            .map(decode_instance_ids)
                            .unwrap_or_else(HashSet::new),
                        old_configuration: command
                            .get("oldConfiguration")
                            .map(decode_instance_ids)
                            .unwrap_or_else(HashSet::new),
                    })
                },
                "JointConfiguration" => {
                    command.map(|command| Command::JointConfiguration {
                        new_configuration: command
                            .get("newConfiguration")
                            .map(decode_instance_ids)
                            .unwrap_or_else(HashSet::new),
                        old_configuration: command
                            .get("oldConfiguration")
                            .map(decode_instance_ids)
                            .unwrap_or_else(HashSet::new),
                    })
                },
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
    configuration
        .get("instanceIds")
        .and_then(JsonValue::as_array)
        .map(|instance_ids| {
            instance_ids
                .iter()
                .filter_map(JsonValue::as_u64)
                .map(|value| value as usize)
                .collect()
        })
        .unwrap_or_else(HashSet::new)
}

impl<T: CustomCommand> From<&JsonValue> for LogEntry<T> {
    fn from(json: &JsonValue) -> Self {
        Self {
            term: json
                .get("term")
                .and_then(JsonValue::as_u64)
                .map(|term| term as usize)
                .unwrap_or(0),
            command: Command::try_from(json).ok(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use maplit::hashset;

    #[derive(Debug, Eq, PartialEq)]
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
    fn single_configuration_command_to_json() {
        let command = Command::<DummyCommand>::SingleConfiguration {
            old_configuration: hashset!(5, 42, 85, 13531, 8354),
            configuration: hashset!(42, 85, 13531, 8354),
        };
        let entry = LogEntry {
            term: 9,
            command: Some(command),
        };
        assert_eq!(
            json!({
                "type": "SingleConfiguration",
                "term": 9,
                "command": {
                    "oldConfiguration": {
                        "instanceIds": [5, 42, 85, 8354, 13531],
                    },
                    "configuration": {
                        "instanceIds": [42, 85, 8354, 13531],
                    },
                }
            }),
            entry.to_json()
        );
    }

    #[test]
    fn single_configuration_command_from_json() {
        let encoded_entry = json!({
            "type": "SingleConfiguration",
            "term": 9,
            "command": {
                "oldConfiguration": {
                    "instanceIds": [5, 42, 85, 8354, 13531],
                },
                "configuration": {
                    "instanceIds": [42, 85, 8354, 13531],
                },
            },
        });
        let log_entry = LogEntry::<DummyCommand>::from(&encoded_entry);
        assert_eq!(log_entry, LogEntry {
            term: 9,
            command: Some(Command::<DummyCommand>::SingleConfiguration {
                old_configuration: hashset!(5, 42, 85, 13531, 8354),
                configuration: hashset!(42, 85, 13531, 8354),
            }),
        });
    }

    #[test]
    fn joint_configuration_command_to_json() {
        let command = Command::<DummyCommand>::JointConfiguration {
            old_configuration: hashset!(5, 42, 85, 13531, 8354),
            new_configuration: hashset!(42, 85, 13531, 8354),
        };
        let entry = LogEntry {
            term: 9,
            command: Some(command),
        };
        assert_eq!(
            json!({
                "type": "JointConfiguration",
                "term": 9,
                "command": {
                    "oldConfiguration": {
                        "instanceIds": [5, 42, 85, 8354, 13531],
                    },
                    "newConfiguration": {
                        "instanceIds": [42, 85, 8354, 13531],
                    },
                }
            }),
            entry.to_json()
        );
    }

    #[test]
    fn joint_configuration_command_from_json() {
        let encoded_entry = json!({
            "type": "JointConfiguration",
            "term": 9,
            "command": {
                "oldConfiguration": {
                    "instanceIds": [5, 42, 85, 8354, 13531],
                },
                "newConfiguration": {
                    "instanceIds": [42, 85, 8354, 13531],
                },
            },
        });
        let log_entry = LogEntry::<DummyCommand>::from(&encoded_entry);
        assert_eq!(log_entry, LogEntry {
            term: 9,
            command: Some(Command::<DummyCommand>::JointConfiguration {
                old_configuration: hashset!(5, 42, 85, 13531, 8354),
                new_configuration: hashset!(42, 85, 13531, 8354),
            }),
        });
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
    fn custom_command() {
        // Invent a custom command for this test.
        #[derive(Debug, Eq, PartialEq)]
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
    }
}
