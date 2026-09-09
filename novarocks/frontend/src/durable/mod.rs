// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Shared frontend contract for bounded durable records.
//!
//! A durable record is encoded and budget-checked before it reaches a
//! StateStore transaction.  The encoded value remains opaque outside this
//! module, so callers cannot accidentally replace the checked value with a
//! fresh JSON encoding at the final `put` boundary.

// Design: ADR-0074 (docs/adr/ADR-0074-frontend-durable-record-budget-contract.md)

use std::fmt;
use std::sync::Arc;

use bytes::Bytes;
use novarocks_state_store_api::{
    Key, Precondition, StateStore, StateStoreError, StateStoreLimits, Value, WriteTransaction,
};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// An opaque byte payload with a durable, canonical lowercase-hex encoding.
///
/// The bound is on original bytes. Its encoded JSON representation is exactly
/// two hexadecimal characters per byte, plus enclosing JSON framing.
#[derive(Clone, Eq, PartialEq)]
pub(crate) struct DurableOpaqueBytes<const MAX_BYTES: usize>(Vec<u8>);

impl<const MAX_BYTES: usize> DurableOpaqueBytes<MAX_BYTES> {
    pub(crate) fn try_new(bytes: Vec<u8>) -> Result<Self, DurableRecordError> {
        if bytes.is_empty() || bytes.len() > MAX_BYTES {
            return Err(DurableRecordError::OpaqueBytesOutOfBounds {
                actual_bytes: bytes.len(),
                max_bytes: MAX_BYTES,
            });
        }
        Ok(Self(bytes))
    }

    pub(crate) fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl<const MAX_BYTES: usize> fmt::Debug for DurableOpaqueBytes<MAX_BYTES> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DurableOpaqueBytes")
            .field("len", &self.0.len())
            .finish()
    }
}

impl<const MAX_BYTES: usize> Serialize for DurableOpaqueBytes<MAX_BYTES> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&hex::encode(&self.0))
    }
}

impl<'de, const MAX_BYTES: usize> Deserialize<'de> for DurableOpaqueBytes<MAX_BYTES> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let text = String::deserialize(deserializer)?;
        let bytes = hex::decode(&text).map_err(serde::de::Error::custom)?;
        if hex::encode(&bytes) != text {
            return Err(serde::de::Error::custom(
                "durable opaque bytes must use canonical lowercase hex",
            ));
        }
        Self::try_new(bytes).map_err(serde::de::Error::custom)
    }
}

/// Metadata every frontend durable record declares before encoding.
pub(crate) trait DurableRecord: Serialize {
    const RECORD_KIND: &'static str;
    const SCHEMA_VERSION: u8;
    const ENCODED_LIMIT: usize;
}

/// A checked StateStore value. Only [`DurableRecordStore`] can consume it.
#[derive(Clone)]
pub(crate) struct EncodedRecord(Value);

impl EncodedRecord {
    #[cfg(test)]
    pub(crate) fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

impl fmt::Debug for EncodedRecord {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EncodedRecord")
            .field("len", &self.0.as_bytes().len())
            .finish()
    }
}

/// Errors at the frontend durable-record boundary. Opaque record content is
/// intentionally never retained in an error or its formatted representation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum DurableRecordError {
    OpaqueBytesOutOfBounds {
        actual_bytes: usize,
        max_bytes: usize,
    },
    EncodingFailed {
        record_kind: &'static str,
        schema_version: u8,
    },
    BudgetExceeded {
        record_kind: &'static str,
        schema_version: u8,
        actual_bytes: usize,
        limit_bytes: usize,
    },
    SmallValueBudgetExceeded {
        value_kind: &'static str,
        actual_bytes: usize,
        limit_bytes: usize,
    },
    Store(String),
}

impl fmt::Display for DurableRecordError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::OpaqueBytesOutOfBounds {
                actual_bytes,
                max_bytes,
            } => write!(
                formatter,
                "durable opaque bytes must hold 1..={max_bytes} bytes, found {actual_bytes}"
            ),
            Self::EncodingFailed {
                record_kind,
                schema_version,
            } => write!(
                formatter,
                "failed to encode durable record {record_kind} schema version {schema_version}"
            ),
            Self::BudgetExceeded {
                record_kind,
                schema_version,
                actual_bytes,
                limit_bytes,
            } => write!(
                formatter,
                "durable record {record_kind} schema version {schema_version} encoded to {actual_bytes} bytes, exceeding its {limit_bytes}-byte budget"
            ),
            Self::SmallValueBudgetExceeded {
                value_kind,
                actual_bytes,
                limit_bytes,
            } => write!(
                formatter,
                "durable small value {value_kind} has {actual_bytes} bytes, exceeding its {limit_bytes}-byte budget"
            ),
            Self::Store(message) => formatter.write_str(message),
        }
    }
}

impl std::error::Error for DurableRecordError {}

impl From<StateStoreError> for DurableRecordError {
    fn from(error: StateStoreError) -> Self {
        Self::Store(error.to_string())
    }
}

/// The only frontend durable-record encoder and transaction writer.
///
/// Repositories retain this handle beside their StateStore reference. The
/// StateStore remains available for reads and non-record index values, while
/// the private [`EncodedRecord`] payload makes record writes use this handle.
#[derive(Clone)]
pub(crate) struct DurableRecordStore {
    limits: StateStoreLimits,
}

impl DurableRecordStore {
    pub(crate) fn new(store: Arc<dyn StateStore>) -> Self {
        Self::with_limits(store.limits().clone())
    }

    pub(crate) fn with_limits(limits: StateStoreLimits) -> Self {
        Self { limits }
    }

    pub(crate) fn encode<R: DurableRecord>(
        &self,
        record: &R,
    ) -> Result<EncodedRecord, DurableRecordError> {
        let bytes = serde_json::to_vec(record).map_err(|_| DurableRecordError::EncodingFailed {
            record_kind: R::RECORD_KIND,
            schema_version: R::SCHEMA_VERSION,
        })?;
        let limit_bytes = R::ENCODED_LIMIT.min(self.limits.max_value_bytes);
        if bytes.len() > limit_bytes {
            return Err(DurableRecordError::BudgetExceeded {
                record_kind: R::RECORD_KIND,
                schema_version: R::SCHEMA_VERSION,
                actual_bytes: bytes.len(),
                limit_bytes,
            });
        }
        let value = Value::try_from(Bytes::from(bytes)).map_err(DurableRecordError::from)?;
        Ok(EncodedRecord(value))
    }

    pub(crate) async fn put_record(
        &self,
        transaction: &mut dyn WriteTransaction,
        key: Key,
        record: EncodedRecord,
        precondition: Precondition,
    ) -> Result<(), StateStoreError> {
        transaction.put(key, record.0, precondition).await
    }
}

#[cfg(test)]
mod tests {
    use novarocks_state_store_testkit::testing::InMemoryStateStore;

    use super::*;

    #[derive(Serialize)]
    struct TestRecord {
        payload: DurableOpaqueBytes<3>,
    }

    impl DurableRecord for TestRecord {
        const RECORD_KIND: &'static str = "test-record";
        const SCHEMA_VERSION: u8 = 7;
        const ENCODED_LIMIT: usize = 20;
    }

    /// Builds the encoder against the shared in-memory reference store.
    ///
    /// It goes through `DurableRecordStore::new` on purpose, so the budget the
    /// encoder enforces is the one a real `StateStore` reports. The hand-written
    /// stub this replaces implemented the whole trait for a helper that only
    /// ever called `limits()`, which meant every contract change had to be
    /// mirrored here for no coverage in return.
    fn store(limits: StateStoreLimits) -> DurableRecordStore {
        DurableRecordStore::new(Arc::new(InMemoryStateStore::with_limits(
            "frontend-durable-record",
            limits,
        )))
    }

    #[test]
    fn durable_opaque_bytes_round_trip_as_canonical_hex() {
        let payload = DurableOpaqueBytes::<3>::try_new(vec![0x0a, 0xfe]).expect("bounded payload");
        assert_eq!(
            serde_json::to_string(&payload).expect("serialize"),
            "\"0afe\""
        );
        let decoded: DurableOpaqueBytes<3> = serde_json::from_str("\"0afe\"").expect("decode");
        assert_eq!(decoded.as_bytes(), [0x0a, 0xfe]);
        assert!(serde_json::from_str::<DurableOpaqueBytes<3>>("\"0AFE\"").is_err());
    }

    #[test]
    fn durable_opaque_bytes_debug_is_redacted() {
        let payload = DurableOpaqueBytes::<3>::try_new(b"top".to_vec()).expect("bounded payload");
        let debug = format!("{payload:?}");
        assert!(debug.contains("len: 3"));
        assert!(!debug.contains("top"));
        assert!(!debug.contains("746f70"));
    }

    #[test]
    fn record_at_its_encoded_limit_is_accepted() {
        let record = TestRecord {
            payload: DurableOpaqueBytes::try_new(vec![0xff; 3]).expect("bounded payload"),
        };
        let store = store(StateStoreLimits::default());
        let encoded = store.encode(&record).expect("record is within its budget");
        assert_eq!(encoded.0.as_bytes().len(), TestRecord::ENCODED_LIMIT);
    }

    #[test]
    fn record_one_byte_over_its_encoded_limit_is_rejected_without_content() {
        #[derive(Serialize)]
        struct OneByteOver {
            payload: &'static str,
        }
        impl DurableRecord for OneByteOver {
            const RECORD_KIND: &'static str = "one-byte-over";
            const SCHEMA_VERSION: u8 = 2;
            const ENCODED_LIMIT: usize = 10;
        }

        let error = store(StateStoreLimits::default())
            .encode(&OneByteOver {
                payload: "leakproof",
            })
            .expect_err("record exceeds its budget");
        assert_eq!(
            error,
            DurableRecordError::BudgetExceeded {
                record_kind: "one-byte-over",
                schema_version: 2,
                actual_bytes: 23,
                limit_bytes: 10,
            }
        );
        assert!(!format!("{error:?}").contains("leakproof"));
        assert!(!error.to_string().contains("leakproof"));
    }

    #[test]
    fn state_store_limit_wins_when_it_is_tighter_than_record_limit() {
        let limits = StateStoreLimits {
            max_value_bytes: 19,
            ..StateStoreLimits::default()
        };
        let record = TestRecord {
            payload: DurableOpaqueBytes::try_new(vec![0xff; 3]).expect("bounded payload"),
        };
        let error = store(limits)
            .encode(&record)
            .expect_err("store budget is tighter");
        assert_eq!(
            error,
            DurableRecordError::BudgetExceeded {
                record_kind: "test-record",
                schema_version: 7,
                actual_bytes: 20,
                limit_bytes: 19,
            }
        );
    }
}
