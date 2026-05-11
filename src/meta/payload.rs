use bytes::Bytes;
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::meta::repository::{RepositoryError, RepositoryResult};
use crate::meta::{MetaPayload, MetaPayloadEncoding};

pub fn encode_json_payload<T>(schema_version: i32, value: &T) -> RepositoryResult<MetaPayload>
where
    T: Serialize,
{
    let bytes = serde_json::to_vec(value)
        .map_err(|err| RepositoryError::invalid(format!("failed to encode JSON payload: {err}")))?;
    Ok(MetaPayload::json(schema_version, Bytes::from(bytes)))
}

pub fn decode_json_payload<T>(payload: &MetaPayload) -> RepositoryResult<T>
where
    T: DeserializeOwned,
{
    if payload.encoding != MetaPayloadEncoding::Json {
        return Err(RepositoryError::invalid(format!(
            "expected JSON payload, got {:?}",
            payload.encoding
        )));
    }
    serde_json::from_slice(&payload.bytes)
        .map_err(|err| RepositoryError::invalid(format!("failed to decode JSON payload: {err}")))
}
