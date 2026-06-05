use std::fmt;

use crate::meta::{MetaError, MetaErrorKind};

pub mod dictionary;
pub mod iceberg_catalog;
pub mod iceberg_operation;
pub mod id_scopes;
pub mod job;
pub mod mv;
pub mod mv_contract;
pub mod starrocks_table;
pub mod starrocks_txn;

pub use crate::meta::payload::{decode_payload_for_kind, encode_record_payload};

#[cfg(test)]
pub(crate) mod test_avro_seed {
    use std::fmt;

    use serde::de::{Error as DeError, SeqAccess, Visitor};
    use serde::ser::Serializer;
    use serde::{Deserialize, Serialize};

    use crate::meta::MetaPayload;
    use crate::meta::repository::iceberg_operation::StoredIcebergOperation;
    use crate::meta::repository::job::{StoredEraseJob, StoredIcebergOptimizeJob};
    use crate::meta::repository::starrocks_table::{
        StoredStarRocksColumn, StoredStarRocksDatabase, StoredStarRocksIndex,
        StoredStarRocksPartition, StoredStarRocksTable, StoredStarRocksTablet,
    };
    use crate::meta::repository::starrocks_txn::StoredStarRocksTxn;
    use crate::meta::repository::{RepositoryError, RepositoryResult, encode_record_payload};

    pub(crate) fn encode_seed_payload(
        kind: &str,
        payload: &serde_json::Value,
    ) -> RepositoryResult<MetaPayload> {
        match kind {
            "starrocks.database" => encode_from_json::<StoredStarRocksDatabase>(kind, payload),
            "starrocks.table" => encode_from_json::<StoredStarRocksTable>(kind, payload),
            "starrocks.schema" => encode_from_json::<StoredStarRocksSchemaAvro>(kind, payload),
            "starrocks.column" => encode_from_json::<StoredStarRocksColumn>(kind, payload),
            "starrocks.partition" => encode_from_json::<StoredStarRocksPartition>(kind, payload),
            "starrocks.index" => encode_from_json::<StoredStarRocksIndex>(kind, payload),
            "starrocks.tablet" => encode_from_json::<StoredStarRocksTablet>(kind, payload),
            "starrocks.txn" => encode_from_json::<StoredStarRocksTxn>(kind, payload),
            "job.erase" => encode_from_json::<StoredEraseJob>(kind, payload),
            "job.iceberg_optimize" => encode_from_json::<StoredIcebergOptimizeJob>(kind, payload),
            "iceberg.operation" => encode_from_json::<StoredIcebergOperation>(kind, payload),
            _ => Err(RepositoryError::invalid(format!(
                "unsupported test seed metadata kind `{kind}`"
            ))),
        }
    }

    fn encode_from_json<T>(kind: &str, payload: &serde_json::Value) -> RepositoryResult<MetaPayload>
    where
        T: for<'de> Deserialize<'de> + Serialize,
    {
        let value = serde_json::from_value::<T>(payload.clone()).map_err(|err| {
            RepositoryError::invalid(format!(
                "failed to materialize test seed payload for `{kind}`: {err}"
            ))
        })?;
        encode_record_payload(kind, &value)
    }

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    struct StoredStarRocksSchemaAvro {
        schema_id: i64,
        table_id: i64,
        schema_version: i64,
        #[serde(with = "avro_bytes_vec")]
        tablet_schema_pb: Vec<u8>,
    }

    mod avro_bytes_vec {
        use super::*;

        pub fn serialize<S>(value: &Vec<u8>, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            serializer.serialize_bytes(value)
        }

        pub fn deserialize<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
        where
            D: serde::Deserializer<'de>,
        {
            deserializer.deserialize_byte_buf(BytesVecVisitor)
        }

        struct BytesVecVisitor;

        impl<'de> Visitor<'de> for BytesVecVisitor {
            type Value = Vec<u8>;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("Avro bytes")
            }

            fn visit_bytes<E>(self, value: &[u8]) -> Result<Self::Value, E>
            where
                E: DeError,
            {
                Ok(value.to_vec())
            }

            fn visit_byte_buf<E>(self, value: Vec<u8>) -> Result<Self::Value, E>
            where
                E: DeError,
            {
                Ok(value)
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let mut bytes = Vec::with_capacity(seq.size_hint().unwrap_or(0));
                while let Some(byte) = seq.next_element::<u8>()? {
                    bytes.push(byte);
                }
                Ok(bytes)
            }
        }
    }
}

pub type RepositoryResult<T> = Result<T, RepositoryError>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RepositoryErrorKind {
    Conflict,
    NotFound,
    InvalidRequest,
    Provider,
}

#[derive(Debug)]
pub struct RepositoryError {
    kind: RepositoryErrorKind,
    message: String,
}

impl RepositoryError {
    pub fn conflict(message: impl Into<String>) -> Self {
        Self::new(RepositoryErrorKind::Conflict, message)
    }

    pub fn not_found(message: impl Into<String>) -> Self {
        Self::new(RepositoryErrorKind::NotFound, message)
    }

    pub fn invalid(message: impl Into<String>) -> Self {
        Self::new(RepositoryErrorKind::InvalidRequest, message)
    }

    pub fn provider(message: impl Into<String>) -> Self {
        Self::new(RepositoryErrorKind::Provider, message)
    }

    pub fn kind(&self) -> RepositoryErrorKind {
        self.kind
    }

    fn new(kind: RepositoryErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }
}

impl fmt::Display for RepositoryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let label = match self.kind {
            RepositoryErrorKind::Conflict => "conflict",
            RepositoryErrorKind::NotFound => "not found",
            RepositoryErrorKind::InvalidRequest => "invalid request",
            RepositoryErrorKind::Provider => "provider error",
        };
        write!(f, "metadata repository {label}: {}", self.message)
    }
}

impl std::error::Error for RepositoryError {}

impl From<MetaError> for RepositoryError {
    fn from(err: MetaError) -> Self {
        match err.kind() {
            MetaErrorKind::Conflict | MetaErrorKind::AlreadyExists => {
                Self::conflict(err.to_string())
            }
            MetaErrorKind::NotFound => Self::not_found(err.to_string()),
            MetaErrorKind::InvalidRequest | MetaErrorKind::Unsupported => {
                Self::invalid(err.to_string())
            }
            MetaErrorKind::Transient
            | MetaErrorKind::DefiniteCommitFailure
            | MetaErrorKind::CommitUnknown
            | MetaErrorKind::ProviderCorruption => Self::provider(err.to_string()),
        }
    }
}
