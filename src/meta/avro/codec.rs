use std::io::Cursor;

use apache_avro::{from_avro_datum, from_value, to_avro_datum, to_value};
use bytes::Bytes;
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::meta::avro::catalog::{AvroSchemaEntry, schema_catalog};
use crate::meta::repository::{RepositoryError, RepositoryResult};
use crate::meta::{MetaPayload, MetaPayloadEncoding};

pub fn encode_payload<T>(subject: &str, value: &T) -> RepositoryResult<MetaPayload>
where
    T: Serialize,
{
    let catalog = schema_catalog()?;
    let entry = catalog.latest(subject)?;
    encode_payload_with_schema(entry, value)
}

pub fn encode_payload_with_schema<T>(
    entry: &AvroSchemaEntry,
    value: &T,
) -> RepositoryResult<MetaPayload>
where
    T: Serialize,
{
    let value = to_value(value).map_err(|err| {
        RepositoryError::invalid(format!("failed to convert value to Avro: {err}"))
    })?;
    let bytes = to_avro_datum(entry.schema(), value).map_err(|err| {
        RepositoryError::invalid(format!(
            "failed to encode Avro payload for subject `{}` schema id {}: {err}",
            entry.subject(),
            entry.id()
        ))
    })?;
    Ok(MetaPayload::avro(
        entry.id(),
        entry.fingerprint().to_string(),
        Bytes::from(bytes),
    ))
}

pub fn decode_payload<T>(subject: &str, payload: &MetaPayload) -> RepositoryResult<T>
where
    T: DeserializeOwned,
{
    if payload.encoding != MetaPayloadEncoding::Avro {
        return Err(RepositoryError::invalid(format!(
            "expected Avro payload, got {:?}",
            payload.encoding
        )));
    }

    let catalog = schema_catalog()?;
    let writer = catalog.entry(subject, payload.schema_id)?;
    if payload.schema_fingerprint != writer.fingerprint() {
        return Err(RepositoryError::provider(format!(
            "Avro schema fingerprint mismatch for subject `{subject}` schema id {}: payload={}, catalog={}",
            payload.schema_id,
            payload.schema_fingerprint,
            writer.fingerprint()
        )));
    }
    let reader = catalog.latest(subject)?;
    let mut cursor = Cursor::new(payload.bytes.as_ref());
    let value = from_avro_datum(writer.schema(), &mut cursor, Some(reader.schema())).map_err(
        |err| {
            RepositoryError::invalid(format!(
                "failed to decode Avro payload for subject `{subject}` writer schema id {} reader schema id {}: {err}",
                writer.id(),
                reader.id()
            ))
        },
    )?;
    if cursor.position() != payload.bytes.len() as u64 {
        return Err(RepositoryError::invalid(format!(
            "failed to decode Avro payload for subject `{subject}`: trailing bytes after datum"
        )));
    }
    from_value::<T>(&value).map_err(|err| {
        RepositoryError::invalid(format!(
            "failed to materialize Avro payload for subject `{subject}`: {err}"
        ))
    })
}
