use serde::{
    de::Error as _,
    ser::Error as _,
    Deserialize,
    Deserializer,
    Serializer,
};
use serde_json::Value as JsonValue;
use std::borrow::Cow;

pub fn serialize<S>(
    value: &JsonValue,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let encoding = serde_json::to_string(value).map_err(S::Error::custom)?;
    serializer.serialize_str(&encoding)
}

pub fn deserialize<'de, D>(deserializer: D) -> Result<JsonValue, D::Error>
where
    D: Deserializer<'de>,
{
    let encoding = <Cow<'_, str>>::deserialize(deserializer)?;
    serde_json::from_str(&encoding).map_err(D::Error::custom)
}
