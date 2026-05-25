use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::meta::avro;
use crate::meta::repository::RepositoryResult;
use crate::meta::MetaPayload;

pub fn encode_record_payload<T>(kind: &str, value: &T) -> RepositoryResult<MetaPayload>
where
    T: Serialize,
{
    avro::encode_payload(kind, value)
}

pub fn decode_payload_for_kind<T>(kind: &str, payload: &MetaPayload) -> RepositoryResult<T>
where
    T: DeserializeOwned,
{
    avro::decode_payload(kind, payload)
}
