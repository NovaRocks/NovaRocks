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

use serde::{Deserialize, Serialize};

use crate::durable::{DurableRecord, DurableRecordError, DurableRecordStore, EncodedRecord};
use crate::state_family::StateFamily;

/// Catalog attachments have no opaque payload fields. Their complete durable
/// JSON record is capped at the global StateStore value budget before a write
/// transaction is opened.
const CATALOG_ATTACHMENT_ENCODED_LIMIT: usize = novarocks_state_store_api::MAX_VALUE_BYTES;
/// Record version of the catalog desired-state family.
///
/// Declared by the manifest, not here: a second literal could disagree with
/// the version the manifest publishes and nothing would catch it.
pub(crate) const CATALOG_ATTACHMENT_SCHEMA_VERSION: u8 =
    match StateFamily::CatalogDesiredState.record_version() {
        Some(version) => version,
        None => panic!("catalog desired state is a durable external projection"),
    };

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StoredCatalogAttachment {
    pub(crate) schema_version: u8,
    pub(crate) attachment_id: String,
    pub(crate) instance_id: String,
    pub(crate) provider_id: String,
    pub(crate) display_name: String,
    pub(crate) durable_properties: Vec<StoredProperty>,
    pub(crate) credential_bindings: Vec<StoredCredentialBinding>,
    pub(crate) created_at_ms: i64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StoredProperty {
    pub(crate) key: String,
    pub(crate) value: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StoredCredentialBinding {
    pub(crate) purpose: String,
    pub(crate) consumer_role: String,
    pub(crate) mode: String,
    pub(crate) name: Option<String>,
    pub(crate) generation: Option<String>,
}

#[derive(Deserialize)]
struct StoredCatalogAttachmentVersion {
    schema_version: u8,
}

impl DurableRecord for StoredCatalogAttachment {
    const RECORD_KIND: &'static str = "catalog-attachment";
    const SCHEMA_VERSION: u8 = CATALOG_ATTACHMENT_SCHEMA_VERSION;
    const ENCODED_LIMIT: usize = CATALOG_ATTACHMENT_ENCODED_LIMIT;
}

pub(crate) fn encode(
    store: &DurableRecordStore,
    value: &StoredCatalogAttachment,
) -> Result<EncodedRecord, DurableRecordError> {
    store.encode(value)
}

pub(crate) fn decode(value: &[u8]) -> Result<StoredCatalogAttachment, String> {
    let version = serde_json::from_slice::<StoredCatalogAttachmentVersion>(value)
        .map_err(|error| format!("decode catalog attachment durable record version: {error}"))?;
    if version.schema_version != CATALOG_ATTACHMENT_SCHEMA_VERSION {
        return Err(format!(
            "catalog attachment has unsupported schema version {}",
            version.schema_version
        ));
    }
    let stored = serde_json::from_slice::<StoredCatalogAttachment>(value)
        .map_err(|error| format!("decode catalog attachment durable record: {error}"))?;
    Ok(stored)
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use super::*;

    fn stored_attachment() -> StoredCatalogAttachment {
        StoredCatalogAttachment {
            schema_version: CATALOG_ATTACHMENT_SCHEMA_VERSION,
            attachment_id: Uuid::parse_str("01991d9f-939d-7a20-8000-000000000001")
                .expect("fixture UUID")
                .to_string(),
            instance_id: "warehouse".to_string(),
            provider_id: "iceberg".to_string(),
            display_name: "Warehouse".to_string(),
            durable_properties: vec![StoredProperty {
                key: "type".to_string(),
                value: "iceberg".to_string(),
            }],
            credential_bindings: vec![StoredCredentialBinding {
                purpose: "object-store-data".to_string(),
                consumer_role: "frontend-and-backend".to_string(),
                mode: "static".to_string(),
                name: Some("warehouse-data".to_string()),
                generation: Some("blue".to_string()),
            }],
            created_at_ms: 42,
        }
    }

    #[test]
    fn codec_round_trips_v3_with_stable_bytes() {
        let value = stored_attachment();
        let store =
            DurableRecordStore::with_limits(novarocks_state_store_api::StateStoreLimits::default());
        let encoded = encode(&store, &value).expect("encode");
        assert_eq!(decode(encoded.as_bytes()).expect("decode"), value);
        assert_eq!(
            encoded.as_bytes(),
            br#"{"schema_version":3,"attachment_id":"01991d9f-939d-7a20-8000-000000000001","instance_id":"warehouse","provider_id":"iceberg","display_name":"Warehouse","durable_properties":[{"key":"type","value":"iceberg"}],"credential_bindings":[{"purpose":"object-store-data","consumer_role":"frontend-and-backend","mode":"static","name":"warehouse-data","generation":"blue"}],"created_at_ms":42}"#
        );
    }

    #[test]
    fn codec_rejects_v2_instead_of_upgrading_backend_only_bindings() {
        let old = br#"{"schema_version":2,"attachment_id":"01991d9f-939d-7a20-8000-000000000001","instance_id":"warehouse","provider_id":"iceberg","display_name":"Warehouse","durable_properties":[{"key":"type","value":"iceberg"}],"credential_bindings":[{"purpose":"object-store-data","consumer_role":"backend","mode":"static","name":"warehouse-data","generation":"blue"}],"created_at_ms":42}"#;
        let error = decode(old).expect_err("v2 must fail closed");
        assert!(error.contains("unsupported schema version 2"), "{error}");
    }

    #[test]
    fn codec_rejects_an_unknown_schema_version() {
        let mut value = stored_attachment();
        value.schema_version = CATALOG_ATTACHMENT_SCHEMA_VERSION + 1;
        let encoded = serde_json::to_vec(&value).expect("serialize stored attachment");
        assert!(
            decode(&encoded)
                .expect_err("unknown schema version must fail closed")
                .contains("unsupported schema version")
        );
    }

    #[test]
    fn codec_reports_the_typed_record_budget_error() {
        let mut value = stored_attachment();
        value.display_name = "x".repeat(1_024);
        let limits = novarocks_state_store_api::StateStoreLimits {
            max_value_bytes: 256,
            ..novarocks_state_store_api::StateStoreLimits::default()
        };
        let error = encode(&DurableRecordStore::with_limits(limits), &value)
            .expect_err("record beyond the StateStore limit must fail before a write");
        assert!(matches!(
            error,
            DurableRecordError::BudgetExceeded {
                record_kind: "catalog-attachment",
                schema_version: CATALOG_ATTACHMENT_SCHEMA_VERSION,
                actual_bytes,
                limit_bytes: 256,
            } if actual_bytes > 256
        ));
    }
}
