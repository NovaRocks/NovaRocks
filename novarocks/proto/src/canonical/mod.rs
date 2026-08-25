//! Descriptor-driven canonical projection for native protobuf messages.
//!
//! Generated DTOs remain the schema representation. This module turns a
//! decoded DTO into a deterministic semantic byte stream for lifecycle digest
//! consumers: fields are ordered by tag, maps by key, and ordinary repeated
//! fields retain their sequence order.

use std::fmt;

use once_cell::sync::Lazy;
use prost::Message;
use prost_reflect::{DescriptorPool, DynamicMessage, MapKey, Value};
use sha2::{Digest, Sha256};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CanonicalError {
    detail: String,
}

impl CanonicalError {
    fn new(detail: impl Into<String>) -> Self {
        Self {
            detail: detail.into(),
        }
    }

    pub fn detail(&self) -> &str {
        &self.detail
    }
}

impl fmt::Display for CanonicalError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.detail)
    }
}

impl std::error::Error for CanonicalError {}

static NATIVE_DESCRIPTOR_POOL: Lazy<DescriptorPool> = Lazy::new(|| {
    DescriptorPool::decode(crate::FILE_DESCRIPTOR_SET)
        .expect("native protobuf descriptor set is valid")
});

/// Computes a SHA-256 digest over a domain-separated canonical projection of
/// one generated protobuf message.
pub fn digest_message<M: Message>(
    domain: &[u8],
    message_name: &str,
    message: &M,
) -> Result<[u8; 32], CanonicalError> {
    let mut hasher = Sha256::new();
    hasher.update(domain);
    hash_message(&mut hasher, message_name, message)?;
    Ok(hasher.finalize().into())
}

/// Adds a canonical protobuf message projection to an existing digest. This
/// is crate-visible for compound lifecycle digests such as StageDigest.
pub(crate) fn hash_message<M: Message>(
    hasher: &mut Sha256,
    message_name: &str,
    message: &M,
) -> Result<(), CanonicalError> {
    let descriptor = NATIVE_DESCRIPTOR_POOL
        .get_message_by_name(message_name)
        .ok_or_else(|| {
            CanonicalError::new(format!("missing digest descriptor for {message_name}"))
        })?;
    let mut bytes = Vec::with_capacity(message.encoded_len());
    message
        .encode(&mut bytes)
        .expect("Vec has enough capacity for a protobuf message");
    let dynamic = DynamicMessage::decode(descriptor, bytes.as_slice()).map_err(|error| {
        CanonicalError::new(format!(
            "cannot decode {message_name} for canonical digest: {error}"
        ))
    })?;
    canonical_message(hasher, &dynamic)
}

const VALUE_BOOL: u8 = 1;
const VALUE_I32: u8 = 2;
const VALUE_I64: u8 = 3;
const VALUE_U32: u8 = 4;
const VALUE_U64: u8 = 5;
const VALUE_F32: u8 = 6;
const VALUE_F64: u8 = 7;
const VALUE_STRING: u8 = 8;
const VALUE_BYTES: u8 = 9;
const VALUE_ENUM: u8 = 10;
const VALUE_MESSAGE: u8 = 11;
const VALUE_LIST: u8 = 12;
const VALUE_MAP: u8 = 13;

fn canonical_message(hasher: &mut Sha256, message: &DynamicMessage) -> Result<(), CanonicalError> {
    let mut fields = message.fields().collect::<Vec<_>>();
    fields.sort_by_key(|(field, _)| field.number());
    hash_u64(
        hasher,
        u64::try_from(fields.len()).expect("field count fits u64"),
    );
    for (field, value) in fields {
        hash_u32(hasher, field.number());
        canonical_value(hasher, value)?;
    }
    Ok(())
}

fn canonical_value(hasher: &mut Sha256, value: &Value) -> Result<(), CanonicalError> {
    match value {
        Value::Bool(value) => hasher.update([VALUE_BOOL, u8::from(*value)]),
        Value::I32(value) => {
            hasher.update([VALUE_I32]);
            hasher.update(value.to_be_bytes());
        }
        Value::I64(value) => {
            hasher.update([VALUE_I64]);
            hasher.update(value.to_be_bytes());
        }
        Value::U32(value) => {
            hasher.update([VALUE_U32]);
            hasher.update(value.to_be_bytes());
        }
        Value::U64(value) => {
            hasher.update([VALUE_U64]);
            hasher.update(value.to_be_bytes());
        }
        Value::F32(value) => {
            if !value.is_finite() {
                return Err(CanonicalError::new(
                    "canonical digest rejects non-finite float values",
                ));
            }
            hasher.update([VALUE_F32]);
            hasher.update(value.to_bits().to_be_bytes());
        }
        Value::F64(value) => {
            if !value.is_finite() {
                return Err(CanonicalError::new(
                    "canonical digest rejects non-finite double values",
                ));
            }
            hasher.update([VALUE_F64]);
            hasher.update(value.to_bits().to_be_bytes());
        }
        Value::String(value) => {
            hasher.update([VALUE_STRING]);
            hash_bytes(hasher, value.as_bytes());
        }
        Value::Bytes(value) => {
            hasher.update([VALUE_BYTES]);
            hash_bytes(hasher, value);
        }
        Value::EnumNumber(value) => {
            hasher.update([VALUE_ENUM]);
            hasher.update(value.to_be_bytes());
        }
        Value::Message(value) => {
            hasher.update([VALUE_MESSAGE]);
            canonical_message(hasher, value)?;
        }
        Value::List(values) => {
            hasher.update([VALUE_LIST]);
            hash_u64(
                hasher,
                u64::try_from(values.len()).expect("list length fits u64"),
            );
            for value in values {
                canonical_value(hasher, value)?;
            }
        }
        Value::Map(values) => {
            hasher.update([VALUE_MAP]);
            let mut entries = values.iter().collect::<Vec<_>>();
            entries.sort_by_key(|(key, _)| *key);
            hash_u64(
                hasher,
                u64::try_from(entries.len()).expect("map length fits u64"),
            );
            for (key, value) in entries {
                canonical_map_key(hasher, key);
                canonical_value(hasher, value)?;
            }
        }
    }
    Ok(())
}

fn canonical_map_key(hasher: &mut Sha256, key: &MapKey) {
    match key {
        MapKey::Bool(value) => hasher.update([VALUE_BOOL, u8::from(*value)]),
        MapKey::I32(value) => {
            hasher.update([VALUE_I32]);
            hasher.update(value.to_be_bytes());
        }
        MapKey::I64(value) => {
            hasher.update([VALUE_I64]);
            hasher.update(value.to_be_bytes());
        }
        MapKey::U32(value) => {
            hasher.update([VALUE_U32]);
            hasher.update(value.to_be_bytes());
        }
        MapKey::U64(value) => {
            hasher.update([VALUE_U64]);
            hasher.update(value.to_be_bytes());
        }
        MapKey::String(value) => {
            hasher.update([VALUE_STRING]);
            hash_bytes(hasher, value.as_bytes());
        }
    }
}

fn hash_u32(hasher: &mut Sha256, value: u32) {
    hasher.update(value.to_be_bytes());
}

fn hash_u64(hasher: &mut Sha256, value: u64) {
    hasher.update(value.to_be_bytes());
}

fn hash_bytes(hasher: &mut Sha256, bytes: &[u8]) {
    hash_u64(
        hasher,
        u64::try_from(bytes.len()).expect("byte length fits u64"),
    );
    hasher.update(bytes);
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::{common, novarocks};

    use super::digest_message;

    const DOMAIN: &[u8] = b"novarocks.protocol.canonical.test.v1\0";
    const LITERAL_VALUE_DIGEST_GOLDEN_HEX: &str =
        "11fbceccc45ee0bf45e6acc5fcdefe220a95838183c3cedf95e7db5e9e142078";

    fn digest_hex(digest: [u8; 32]) -> String {
        digest.iter().map(|byte| format!("{byte:02x}")).collect()
    }

    #[test]
    fn sorts_maps_but_preserves_repeated_order() {
        let first = novarocks::InstanceParams {
            per_exch_num_senders: HashMap::from([(3, 30), (9, 90)]),
            destinations: vec![
                novarocks::Destination {
                    endpoint: "be-a:9020".into(),
                    ..Default::default()
                },
                novarocks::Destination {
                    endpoint: "be-b:9020".into(),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        let mut second = first.clone();
        second.per_exch_num_senders = HashMap::from([(9, 90), (3, 30)]);
        assert_eq!(
            digest_message(DOMAIN, "novarocks.InstanceParams", &first).expect("digest"),
            digest_message(DOMAIN, "novarocks.InstanceParams", &second).expect("digest")
        );

        second.destinations.reverse();
        assert_ne!(
            digest_message(DOMAIN, "novarocks.InstanceParams", &first).expect("digest"),
            digest_message(DOMAIN, "novarocks.InstanceParams", &second).expect("digest")
        );
    }

    #[test]
    fn preserves_optional_presence_and_rejects_non_finite_values() {
        let absent = novarocks::QueryOptions::default();
        let present = novarocks::QueryOptions {
            runtime_filter_wait_timeout_ms: Some(0),
            ..Default::default()
        };
        assert_ne!(
            digest_message(DOMAIN, "novarocks.QueryOptions", &absent).expect("digest"),
            digest_message(DOMAIN, "novarocks.QueryOptions", &present).expect("digest")
        );

        let non_finite = novarocks::SpillOptions {
            spill_mem_limit_threshold: f64::NAN,
            ..Default::default()
        };
        let error = digest_message(DOMAIN, "novarocks.SpillOptions", &non_finite)
            .expect_err("NaN cannot enter a canonical digest");
        assert!(error.detail().contains("non-finite"));
    }

    #[test]
    fn literal_value_oneof_variants_change_digest_and_are_stable() {
        use common::literal_value::Value;

        let binary = common::LiteralValue {
            value: Some(Value::BinaryValue(vec![0x00, 0xff, 0x2a])),
        };
        let same_binary = binary.clone();
        let string = common::LiteralValue {
            value: Some(Value::StringValue("\0\u{fffd}*".into())),
        };

        let binary_digest = digest_message(DOMAIN, "novarocks.common.LiteralValue", &binary)
            .expect("binary literal digest");
        assert_eq!(
            binary_digest,
            digest_message(DOMAIN, "novarocks.common.LiteralValue", &same_binary,)
                .expect("same binary literal digest")
        );
        assert_ne!(
            binary_digest,
            digest_message(DOMAIN, "novarocks.common.LiteralValue", &string)
                .expect("string literal digest")
        );
    }

    #[test]
    fn literal_value_bytes_and_empty_oneof_presence_change_digest() {
        use common::literal_value::Value;

        let first = common::LiteralValue {
            value: Some(Value::BinaryValue(vec![0x00, 0xff, 0x2a])),
        };
        let changed = common::LiteralValue {
            value: Some(Value::BinaryValue(vec![0x00, 0xfe, 0x2a])),
        };
        let absent = common::LiteralValue::default();
        let present_empty = common::LiteralValue {
            value: Some(Value::BinaryValue(Vec::new())),
        };

        assert_ne!(
            digest_message(DOMAIN, "novarocks.common.LiteralValue", &first).expect("digest"),
            digest_message(DOMAIN, "novarocks.common.LiteralValue", &changed)
                .expect("changed bytes digest")
        );
        assert_ne!(
            digest_message(DOMAIN, "novarocks.common.LiteralValue", &absent)
                .expect("absent oneof digest"),
            digest_message(DOMAIN, "novarocks.common.LiteralValue", &present_empty,)
                .expect("present empty bytes digest")
        );
    }

    #[test]
    fn literal_value_digest_matches_known_hex_golden() {
        use common::literal_value::Value;

        let literal = common::LiteralValue {
            value: Some(Value::BinaryValue(vec![0x00, 0xff, 0x2a])),
        };
        let digest = digest_message(DOMAIN, "novarocks.common.LiteralValue", &literal)
            .expect("literal digest");

        assert_eq!(digest_hex(digest), LITERAL_VALUE_DIGEST_GOLDEN_HEX);
    }
}
