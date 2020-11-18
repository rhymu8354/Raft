use serde::{
    de::Error as _,
    ser::Error as _,
    Deserialize,
    Deserializer,
    Serialize,
    Serializer,
};
use serde_json::Value as JsonValue;

pub fn serialize<S>(
    value: &JsonValue,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let encoding = serde_json::to_string(value)
        .map_err(|error| S::Error::custom(error.to_string()))?;
    encoding.serialize(serializer)
}

pub fn deserialize<'de, D>(deserializer: D) -> Result<JsonValue, D::Error>
where
    D: Deserializer<'de>,
{
    let encoding = <&str>::deserialize(deserializer)?;
    serde_json::from_str(encoding)
        .map_err(|error| D::Error::custom(error.to_string()))
}
