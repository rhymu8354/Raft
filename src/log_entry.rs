use serde_json::{
    json,
    Value as JsonValue,
};
use std::{
    collections::HashSet,
    convert::TryFrom,
};

enum Command {
    SingleConfiguration {
        old_configuration: HashSet<usize>,
        configuration: HashSet<usize>,
    },
    JointConfiguration {
        old_configuration: HashSet<usize>,
        new_configuration: HashSet<usize>,
    },
}

impl Command {
    fn command_type(&self) -> &str {
        match self {
            Command::SingleConfiguration {
                ..
            } => "SingleConfiguration",
            Command::JointConfiguration {
                ..
            } => "JointConfiguration",
        }
    }

    fn to_json(&self) -> JsonValue {
        // TODO: Clean this up, once I get better at serde/serde_json.
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
        }
    }
}

impl TryFrom<JsonValue> for Command {
    type Error = ();

    fn try_from(json: JsonValue) -> Result<Self, Self::Error> {
        json.get("type")
            .and_then(JsonValue::as_str)
            .and_then(|command| match command {
                "SingleConfiguration" => json.get("command").map(|command| {
                    Command::SingleConfiguration {
                        configuration: command
                            .get("configuration")
                            .map(decode_instance_ids)
                            .unwrap_or_else(HashSet::new),
                        old_configuration: command
                            .get("oldConfiguration")
                            .map(decode_instance_ids)
                            .unwrap_or_else(HashSet::new),
                    }
                }),
                "JointConfiguration" => json.get("command").map(|command| {
                    Command::JointConfiguration {
                        new_configuration: command
                            .get("newConfiguration")
                            .map(decode_instance_ids)
                            .unwrap_or_else(HashSet::new),
                        old_configuration: command
                            .get("oldConfiguration")
                            .map(decode_instance_ids)
                            .unwrap_or_else(HashSet::new),
                    }
                }),
                _ => None,
            })
            .ok_or(())
    }
}

struct LogEntry {
    term: usize,
    command: Option<Command>,
}

impl LogEntry {
    fn to_json(&self) -> JsonValue {
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

impl From<JsonValue> for LogEntry {
    fn from(json: JsonValue) -> Self {
        Self {
            term: json
                .get("term")
                .and_then(JsonValue::as_u64)
                .map(|term| term as usize)
                .unwrap_or(0),
            command: Command::try_from(json).ok(),
        }

        // if let Some(configuration) = json.get("configuration") {
        //     if let Some(instance_ids) = configuration.get("instanceIds") {
        //         if let JsonValue::Array(instance_ids) = instance_ids {
        //         }
        //     }
        // }

        // const auto& instanceIds = json["configuration"]["instanceIds"];
        // for (size_t i = 0; i < instanceIds.GetSize(); ++i) {
        //     (void)configuration.instanceIds.insert(instanceIds[i]);
        // }
        // const auto& oldInstanceIds = json["oldConfiguration"]["instanceIds"];
        // for (size_t i = 0; i < oldInstanceIds.GetSize(); ++i) {
        //     (void)oldConfiguration.instanceIds.insert(oldInstanceIds[i]);
        // }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use maplit::hashset;

    #[test]
    fn encode_single_configuration_command() {
        let command = Command::SingleConfiguration {
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
    fn decode_single_configuration_command() {
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
        let LogEntry {
            term,
            command,
        } = LogEntry::from(encoded_entry);
        assert_eq!(9, term);
        assert!(command.is_some());
        let command = command.unwrap();
        assert_eq!("SingleConfiguration", command.command_type());
        match command {
            Command::SingleConfiguration {
                old_configuration,
                configuration,
            } => {
                assert_eq!(hashset!(42, 85, 13531, 8354), configuration);
                assert_eq!(hashset!(5, 42, 85, 13531, 8354), old_configuration);
            },
            _ => panic!("expected `Command::SingleConfiguration`"),
        }
    }

    #[test]
    fn encode_joint_configuration_command() {
        let command = Command::JointConfiguration {
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
    fn decode_joint_configuration_command() {
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
        let LogEntry {
            term,
            command,
        } = LogEntry::from(encoded_entry);
        assert_eq!(9, term);
        assert!(command.is_some());
        let command = command.unwrap();
        assert_eq!("JointConfiguration", command.command_type());
        match command {
            Command::JointConfiguration {
                old_configuration,
                new_configuration,
            } => {
                assert_eq!(hashset!(42, 85, 13531, 8354), new_configuration);
                assert_eq!(hashset!(5, 42, 85, 13531, 8354), old_configuration);
            },
            _ => panic!("expected `Command::JointConfiguration`"),
        }
    }
}
