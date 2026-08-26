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

#[cfg(test)]
use novarocks_sql::planning::mv::ApplyKeySource;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};

use crate::common::persisted_query_definition::PersistedQueryDefinition;
use crate::mv::domain::persistence::schema::MvSchemaContract;
use crate::mv::domain::persistence::semantic::{MvDesiredSemantics, MvRefreshDesiredConfiguration};

pub const MV_DESCRIPTOR_V3_VERSION: u16 = 3;
pub const MV_DESCRIPTOR_PACKAGE_ID_PROP: &str = "novarocks.mv.descriptor.package-id";
pub const MV_DESCRIPTOR_HASH_PROP: &str = "novarocks.mv.descriptor.hash";
pub const MV_DESCRIPTOR_INLINE_PROP: &str = "novarocks.mv.descriptor.inline";
// W2 adds `novarocks.mv.descriptor.location` for externalized descriptor payloads.
pub const MV_DESCRIPTOR_INLINE_MAX_BYTES: usize = 64 * 1024;
pub const MV_DESCRIPTOR_RAW_QUERY_SOURCE_MAX_BYTES: usize = 64 * 1024;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DescriptorDependency {
    pub catalog: String,
    pub namespace: String,
    pub name: String,
    pub object_type: String,
    pub storage_engine: String,
}

/// Current lake-owned MV desired-semantics package.
///
/// Unlike the retired v2 shape, every field needed to rebuild user-visible MV
/// semantics is required and typed. The descriptor deliberately excludes the
/// StateStore physical identity, refresh attempt state, and scheduler runtime
/// bookkeeping; those are projections rather than lake authority.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MvDescriptorV3 {
    pub descriptor_version: u16,
    pub package_id: String,
    pub query_definition: PersistedQueryDefinition,
    pub visible_columns: Vec<String>,
    pub hidden_columns: Vec<String>,
    pub base_dependencies: Vec<DescriptorDependency>,
    pub primary_key_columns: Vec<String>,
    pub schema_contract: MvSchemaContract,
    pub refresh: MvRefreshDesiredConfiguration,
    pub created_at_ms: i64,
}

impl MvDescriptorV3 {
    pub fn from_desired_semantics(semantics: MvDesiredSemantics) -> Self {
        Self {
            descriptor_version: MV_DESCRIPTOR_V3_VERSION,
            package_id: semantics.package_id,
            query_definition: semantics.query_definition,
            visible_columns: semantics.visible_columns,
            hidden_columns: semantics.hidden_columns,
            base_dependencies: semantics.base_dependencies,
            primary_key_columns: semantics.primary_key_columns,
            schema_contract: semantics.schema_contract,
            refresh: semantics.refresh,
            created_at_ms: semantics.created_at_ms,
        }
    }

    pub fn desired_semantics(&self) -> Result<MvDesiredSemantics, String> {
        if self.descriptor_version != MV_DESCRIPTOR_V3_VERSION {
            return Err(format!(
                "unsupported MV descriptor version: expected {}, got {}",
                MV_DESCRIPTOR_V3_VERSION, self.descriptor_version
            ));
        }
        let semantics = MvDesiredSemantics::new(
            self.package_id.clone(),
            self.query_definition.clone(),
            self.visible_columns.clone(),
            self.hidden_columns.clone(),
            self.base_dependencies.clone(),
            self.primary_key_columns.clone(),
            self.schema_contract.clone(),
            self.refresh.clone(),
            self.created_at_ms,
        )?;
        if semantics.primary_key_columns != self.primary_key_columns {
            return Err(
                "MV descriptor primary_key_columns must contain canonical identifiers".to_string(),
            );
        }
        Ok(semantics)
    }

    pub fn to_canonical_json(&self) -> Result<String, String> {
        self.desired_semantics()?;
        let value = serde_json::to_value(self)
            .map_err(|err| format!("failed to serialize MV descriptor: {err}"))?;
        serde_json::to_string(&sort_json_value(value))
            .map_err(|err| format!("failed to render canonical MV descriptor JSON: {err}"))
    }

    pub fn content_hash(&self) -> Result<String, String> {
        self.desired_semantics()?;
        let mut value = serde_json::to_value(self)
            .map_err(|err| format!("failed to serialize MV descriptor: {err}"))?;
        if let Some(obj) = value.as_object_mut() {
            obj.remove("created_at_ms");
        }
        let canonical = serde_json::to_string(&sort_json_value(value))
            .map_err(|err| format!("failed to render canonical MV descriptor hash JSON: {err}"))?;
        Ok(hex_encode(&Sha256::digest(canonical.as_bytes())))
    }

    pub fn from_json(value: &str) -> Result<Self, String> {
        let raw: Value = serde_json::from_str(value)
            .map_err(|err| format!("failed to parse MV descriptor JSON: {err}"))?;
        let version = raw
            .get("descriptor_version")
            .and_then(Value::as_u64)
            .ok_or_else(|| "MV descriptor is missing an integer descriptor_version".to_string())?;
        if version != u64::from(MV_DESCRIPTOR_V3_VERSION) {
            return Err(format!(
                "unsupported MV descriptor version: expected {}, got {}",
                MV_DESCRIPTOR_V3_VERSION, version
            ));
        }
        let descriptor: Self = serde_json::from_value(raw)
            .map_err(|err| format!("failed to parse MV descriptor v3 JSON: {err}"))?;
        descriptor.desired_semantics()?;
        Ok(descriptor)
    }

    pub fn to_storage_properties(&self) -> Result<Vec<(String, String)>, String> {
        let inline = self.to_canonical_json()?;
        let inline_bytes = inline.len();
        if inline_bytes > MV_DESCRIPTOR_INLINE_MAX_BYTES {
            return Err(format!(
                "MV descriptor inline payload is {inline_bytes} bytes, exceeds 64KiB cap of {} bytes",
                MV_DESCRIPTOR_INLINE_MAX_BYTES
            ));
        }
        Ok(vec![
            (
                MV_DESCRIPTOR_PACKAGE_ID_PROP.to_string(),
                self.package_id.clone(),
            ),
            (MV_DESCRIPTOR_HASH_PROP.to_string(), self.content_hash()?),
            (MV_DESCRIPTOR_INLINE_PROP.to_string(), inline),
        ])
    }

    pub fn from_storage_properties(
        props: &std::collections::HashMap<String, String>,
    ) -> Result<Self, String> {
        let inline = props.get(MV_DESCRIPTOR_INLINE_PROP).ok_or_else(|| {
            format!(
                "MV table is missing required MV descriptor inline property `{MV_DESCRIPTOR_INLINE_PROP}`"
            )
        })?;
        let descriptor = Self::from_json(inline)?;
        let package_id = props.get(MV_DESCRIPTOR_PACKAGE_ID_PROP).ok_or_else(|| {
            format!(
                "MV table is missing required MV descriptor package property `{MV_DESCRIPTOR_PACKAGE_ID_PROP}`"
            )
        })?;
        if package_id != &descriptor.package_id {
            return Err(format!(
                "MV descriptor package mismatch: storage property has {package_id}, descriptor has {}",
                descriptor.package_id
            ));
        }
        let stored_hash = props.get(MV_DESCRIPTOR_HASH_PROP).ok_or_else(|| {
            format!(
                "MV table is missing required MV descriptor hash property `{MV_DESCRIPTOR_HASH_PROP}`"
            )
        })?;
        let actual_hash = descriptor.content_hash()?;
        if stored_hash != &actual_hash {
            return Err(format!(
                "MV descriptor hash mismatch: storage property has {stored_hash}, descriptor content hash is {actual_hash}"
            ));
        }
        Ok(descriptor)
    }
}

fn sort_json_value(value: Value) -> Value {
    match value {
        Value::Array(values) => Value::Array(values.into_iter().map(sort_json_value).collect()),
        Value::Object(object) => {
            let mut entries = object
                .into_iter()
                .map(|(key, value)| (key, sort_json_value(value)))
                .collect::<Vec<_>>();
            entries.sort_by(|left, right| left.0.cmp(&right.0));

            let mut sorted = Map::new();
            for (key, value) in entries {
                sorted.insert(key, value);
            }
            Value::Object(sorted)
        }
        value => value,
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";

    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(HEX[(byte >> 4) as usize] as char);
        encoded.push(HEX[(byte & 0x0f) as usize] as char);
    }
    encoded
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_schema_contract() -> MvSchemaContract {
        use crate::mv::domain::persistence::schema::{
            BaseContract, BaseFieldRecord, BaseSchemaSnapshot, ExpressionKind, ExpressionLineage,
            HiddenApplyKeyContract, OutputColumnLineage, OutputContract, TargetContract,
            TargetVisibleColumn,
        };
        use bytes::Bytes;
        use novarocks_spi::connector::ConnectorTableObjectId;

        MvSchemaContract {
            contract_version: 1,
            base: BaseContract {
                table_fqn: "ice.sales.orders".to_string(),
                table_object_id: ConnectorTableObjectId::try_new(Bytes::from_static(&[
                    0, 0xff, b'u',
                ]))
                .expect("valid opaque table object ID"),
                alias_at_create: None,
                schema_id_at_create: 0,
                schema_at_create: BaseSchemaSnapshot {
                    fields: vec![BaseFieldRecord {
                        field_id: 1,
                        name_at_create: "id".to_string(),
                        type_signature: "long".to_string(),
                        required: true,
                    }],
                },
            },
            bases: vec![],
            output: OutputContract {
                columns: vec![OutputColumnLineage {
                    expression: ExpressionLineage {
                        kind: ExpressionKind::Column,
                        referenced_base_field_ids: vec![1],
                        referenced_base_fields: vec![],
                    },
                }],
                filter: None,
            },
            join: None,
            aggregate: None,
            branch: None,
            target: TargetContract {
                table_fqn: "ice.sales.mv_orders".to_string(),
                table_uuid: "target-uuid".to_string(),
                schema_id_at_create: 0,
                visible_columns: vec![TargetVisibleColumn {
                    output_name: "id".to_string(),
                    target_field_id: 1,
                    type_signature: "long".to_string(),
                    nullable: false,
                }],
                hidden_apply_key: HiddenApplyKeyContract {
                    column_name: "__nova_base_row_id".to_string(),
                    target_field_id: 2,
                    source: ApplyKeySource::BaseRowId,
                },
                partition: None,
            },
        }
    }

    fn sample_v3() -> MvDescriptorV3 {
        MvDescriptorV3::from_desired_semantics(
            MvDesiredSemantics::new(
                "analytics.mv_orders".to_string(),
                PersistedQueryDefinition::new(
                    "SELECT id FROM ice.sales.orders",
                    crate::common::persisted_query_definition::PersistedQueryDialect::StarRocks,
                    "ice",
                    "sales",
                )
                .expect("valid query definition"),
                vec!["id".to_string()],
                vec!["__nova_base_row_id".to_string()],
                vec![DescriptorDependency {
                    catalog: "ice".to_string(),
                    namespace: "sales".to_string(),
                    name: "orders".to_string(),
                    object_type: "table".to_string(),
                    storage_engine: "iceberg".to_string(),
                }],
                vec!["id".to_string()],
                sample_schema_contract(),
                MvRefreshDesiredConfiguration::new(
                    crate::mv::domain::persistence::definition::StoredMvRefreshPolicy::AsyncInterval,
                    true,
                    Some(60_000),
                    Some(300_000),
                )
                .expect("valid refresh configuration"),
                123,
            )
            .expect("valid desired semantics"),
        )
    }

    #[test]
    fn v3_descriptor_round_trips_with_all_required_semantics() {
        let descriptor = sample_v3();
        let json = descriptor
            .to_canonical_json()
            .expect("canonical v3 descriptor");

        assert_eq!(MvDescriptorV3::from_json(&json).unwrap(), descriptor);
        assert_eq!(
            MvDescriptorV3::from_storage_properties(
                &descriptor
                    .to_storage_properties()
                    .unwrap()
                    .into_iter()
                    .collect()
            )
            .unwrap(),
            descriptor
        );
    }

    #[test]
    fn v3_descriptor_rejects_missing_or_unknown_semantic_fields() {
        let descriptor = sample_v3();
        let mut value = serde_json::to_value(&descriptor).unwrap();
        value.as_object_mut().unwrap().remove("primary_key_columns");
        assert!(
            MvDescriptorV3::from_json(&value.to_string())
                .unwrap_err()
                .contains("primary_key_columns")
        );

        let mut value = serde_json::to_value(&descriptor).unwrap();
        value.as_object_mut().unwrap().remove("schema_contract");
        assert!(
            MvDescriptorV3::from_json(&value.to_string())
                .unwrap_err()
                .contains("schema_contract")
        );

        let mut value = serde_json::to_value(&descriptor).unwrap();
        value
            .as_object_mut()
            .unwrap()
            .insert("unknown".into(), Value::Null);
        assert!(
            MvDescriptorV3::from_json(&value.to_string())
                .unwrap_err()
                .contains("unknown field")
        );
    }

    #[test]
    fn v3_content_hash_includes_refresh_and_primary_key_but_not_created_at() {
        let descriptor = sample_v3();
        let mut changed_created_at = descriptor.clone();
        changed_created_at.created_at_ms = 999;
        assert_eq!(
            descriptor.content_hash().unwrap(),
            changed_created_at.content_hash().unwrap()
        );

        let mut changed_primary_key = descriptor.clone();
        changed_primary_key.primary_key_columns.clear();
        assert_ne!(
            descriptor.content_hash().unwrap(),
            changed_primary_key.content_hash().unwrap()
        );

        let mut changed_refresh = descriptor.clone();
        changed_refresh.refresh.paused = false;
        assert_ne!(
            descriptor.content_hash().unwrap(),
            changed_refresh.content_hash().unwrap()
        );
    }

    #[test]
    fn v3_descriptor_rejects_v2_and_missing_package_or_hash_properties() {
        let v2_json = r#"{"descriptor_version":2}"#;
        assert_eq!(
            MvDescriptorV3::from_json(v2_json).unwrap_err(),
            "unsupported MV descriptor version: expected 3, got 2"
        );

        let descriptor = sample_v3();
        let mut properties = descriptor
            .to_storage_properties()
            .unwrap()
            .into_iter()
            .collect::<std::collections::HashMap<_, _>>();
        properties.remove(MV_DESCRIPTOR_PACKAGE_ID_PROP);
        assert!(
            MvDescriptorV3::from_storage_properties(&properties)
                .unwrap_err()
                .contains("package property")
        );
        let mut properties = descriptor
            .to_storage_properties()
            .unwrap()
            .into_iter()
            .collect::<std::collections::HashMap<_, _>>();
        properties.remove(MV_DESCRIPTOR_HASH_PROP);
        assert!(
            MvDescriptorV3::from_storage_properties(&properties)
                .unwrap_err()
                .contains("hash property")
        );
    }
}
