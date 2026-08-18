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

use crate::mv::domain::persistence::schema::MvSchemaContract;

pub const MV_DESCRIPTOR_VERSION: u16 = 1;
pub const MV_DESCRIPTOR_PACKAGE_ID_PROP: &str = "novarocks.mv.descriptor.package-id";
pub const MV_DESCRIPTOR_HASH_PROP: &str = "novarocks.mv.descriptor.hash";
pub const MV_DESCRIPTOR_INLINE_PROP: &str = "novarocks.mv.descriptor.inline";
// W2 adds `novarocks.mv.descriptor.location` for externalized descriptor payloads.
pub const MV_DESCRIPTOR_INLINE_MAX_BYTES: usize = 64 * 1024;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DescriptorDependency {
    pub catalog: String,
    pub namespace: String,
    pub name: String,
    pub object_type: String,
    pub storage_engine: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MvDescriptorV1 {
    pub descriptor_version: u16,
    pub package_id: String,
    pub logical_sql: String,
    pub dialect: String,
    pub visible_columns: Vec<String>,
    pub hidden_columns: Vec<String>,
    pub base_dependencies: Vec<DescriptorDependency>,
    #[serde(default)]
    pub schema_contract: Option<Value>,
    #[serde(default)]
    pub refresh_contract: Option<Value>,
    pub created_at_ms: i64,
}

impl MvDescriptorV1 {
    pub fn to_canonical_json(&self) -> Result<String, String> {
        let value = serde_json::to_value(self)
            .map_err(|err| format!("failed to serialize MV descriptor: {err}"))?;
        serde_json::to_string(&sort_json_value(value))
            .map_err(|err| format!("failed to render canonical MV descriptor JSON: {err}"))
    }

    pub fn content_hash(&self) -> Result<String, String> {
        let mut value = serde_json::to_value(self)
            .map_err(|err| format!("failed to serialize MV descriptor: {err}"))?;
        // Physical fields excluded from the identity hash so a descriptor rebuilt
        // from the same lake on a fresh cluster hashes identically (W0 acceptance:
        // logical content equality, physical products may differ).
        if let Some(obj) = value.as_object_mut() {
            obj.remove("created_at_ms");
        }
        let canonical = serde_json::to_string(&sort_json_value(value))
            .map_err(|err| format!("failed to render canonical MV descriptor hash JSON: {err}"))?;
        Ok(hex_encode(&Sha256::digest(canonical.as_bytes())))
    }

    pub fn from_json(s: &str) -> Result<Self, String> {
        let descriptor: Self = serde_json::from_str(s)
            .map_err(|err| format!("failed to parse MV descriptor JSON: {err}"))?;
        if descriptor.descriptor_version != MV_DESCRIPTOR_VERSION {
            return Err(format!(
                "unsupported MV descriptor version: expected {}, got {}",
                MV_DESCRIPTOR_VERSION, descriptor.descriptor_version
            ));
        }
        Ok(descriptor)
    }

    pub fn to_storage_properties(&self) -> Result<Vec<(String, String)>, String> {
        let inline = self.to_canonical_json()?;
        let inline_bytes = inline.len();
        if inline_bytes > MV_DESCRIPTOR_INLINE_MAX_BYTES {
            return Err(format!(
                "MV descriptor inline payload is {inline_bytes} bytes, exceeds W1 cap of {} bytes; W2 externalized descriptor storage must be used for larger descriptors",
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

        if let Some(stored_hash) = props.get(MV_DESCRIPTOR_HASH_PROP) {
            let actual_hash = descriptor.content_hash()?;
            if stored_hash != &actual_hash {
                return Err(format!(
                    "MV descriptor hash mismatch: storage property has {stored_hash}, descriptor content hash is {actual_hash}"
                ));
            }
        }

        Ok(descriptor)
    }

    /// Store a typed MV schema contract into this descriptor's
    /// `schema_contract` field, serializing it to `serde_json::Value`.
    pub fn set_schema_contract(&mut self, contract: &MvSchemaContract) -> Result<(), String> {
        self.schema_contract = Some(
            serde_json::to_value(contract)
                .map_err(|e| format!("serialize MV schema contract into descriptor: {e}"))?,
        );
        Ok(())
    }

    /// Parse this descriptor's `schema_contract` field into a typed MV
    /// schema contract, if present.
    pub fn schema_contract_typed(&self) -> Result<Option<MvSchemaContract>, String> {
        match &self.schema_contract {
            None => Ok(None),
            Some(value) => serde_json::from_value::<MvSchemaContract>(value.clone())
                .map(Some)
                .map_err(|e| format!("parse MV schema contract from descriptor: {e}")),
        }
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

    fn sample() -> MvDescriptorV1 {
        MvDescriptorV1 {
            descriptor_version: MV_DESCRIPTOR_VERSION,
            package_id: "analytics.mv_orders".to_string(),
            logical_sql: "SELECT id FROM ice.sales.orders".to_string(),
            dialect: "starrocks".to_string(),
            visible_columns: vec!["id".to_string()],
            hidden_columns: vec!["__nova_base_row_id".to_string()],
            base_dependencies: vec![DescriptorDependency {
                catalog: "ice".to_string(),
                namespace: "sales".to_string(),
                name: "orders".to_string(),
                object_type: "table".to_string(),
                storage_engine: "iceberg".to_string(),
            }],
            schema_contract: Some(serde_json::json!({
                "z": 3,
                "a": {
                    "z": 2,
                    "a": 1
                }
            })),
            refresh_contract: None,
            created_at_ms: 123,
        }
    }

    #[test]
    fn descriptor_json_round_trips() {
        let descriptor = sample();
        let json = descriptor.to_canonical_json().unwrap();

        let parsed = MvDescriptorV1::from_json(&json).unwrap();

        assert_eq!(parsed, descriptor);
    }

    #[test]
    fn descriptor_json_rejects_unsupported_version() {
        let mut descriptor = sample();
        descriptor.descriptor_version = MV_DESCRIPTOR_VERSION + 1;
        let json = descriptor.to_canonical_json().unwrap();

        let err = MvDescriptorV1::from_json(&json).unwrap_err();

        assert!(err.contains("unsupported MV descriptor version"));
        assert!(err.contains(&(MV_DESCRIPTOR_VERSION + 1).to_string()));
    }

    #[test]
    fn canonical_json_has_only_single_table_descriptor_fields() {
        let descriptor = sample();

        let canonical = descriptor.to_canonical_json().unwrap();

        assert_eq!(
            canonical,
            "{\"base_dependencies\":[{\"catalog\":\"ice\",\"name\":\"orders\",\"namespace\":\"sales\",\"object_type\":\"table\",\"storage_engine\":\"iceberg\"}],\"created_at_ms\":123,\"descriptor_version\":1,\"dialect\":\"starrocks\",\"hidden_columns\":[\"__nova_base_row_id\"],\"logical_sql\":\"SELECT id FROM ice.sales.orders\",\"package_id\":\"analytics.mv_orders\",\"refresh_contract\":null,\"schema_contract\":{\"a\":{\"a\":1,\"z\":2},\"z\":3},\"visible_columns\":[\"id\"]}"
        );
    }

    #[test]
    fn canonical_json_is_key_sorted_and_hash_stable() {
        let descriptor = sample();

        let canonical = descriptor.to_canonical_json().unwrap();

        assert_eq!(
            canonical,
            "{\"base_dependencies\":[{\"catalog\":\"ice\",\"name\":\"orders\",\"namespace\":\"sales\",\"object_type\":\"table\",\"storage_engine\":\"iceberg\"}],\"created_at_ms\":123,\"descriptor_version\":1,\"dialect\":\"starrocks\",\"hidden_columns\":[\"__nova_base_row_id\"],\"logical_sql\":\"SELECT id FROM ice.sales.orders\",\"package_id\":\"analytics.mv_orders\",\"refresh_contract\":null,\"schema_contract\":{\"a\":{\"a\":1,\"z\":2},\"z\":3},\"visible_columns\":[\"id\"]}"
        );
        assert_eq!(
            descriptor.content_hash().unwrap(),
            "3388ae5d5cc385e8b9a150bc0fb5417b281c6c2173a844fe23523be9ee28c1c1"
        );
    }

    #[test]
    fn content_hash_excludes_created_at_ms() {
        let mut descriptor_a = sample();
        descriptor_a.created_at_ms = 123;
        let mut descriptor_b = sample();
        descriptor_b.created_at_ms = 999_999;

        assert_eq!(
            descriptor_a.content_hash().unwrap(),
            descriptor_b.content_hash().unwrap(),
            "descriptors differing only in created_at_ms must hash identically"
        );

        let mut descriptor_c = descriptor_b.clone();
        descriptor_c.logical_sql = "SELECT id FROM ice.sales.other".to_string();

        assert_ne!(
            descriptor_b.content_hash().unwrap(),
            descriptor_c.content_hash().unwrap(),
            "descriptors differing in a logical field must hash differently"
        );
    }

    #[test]
    fn descriptor_properties_carry_pkg_hash_inline() {
        let descriptor = sample();

        let props = descriptor.to_storage_properties().unwrap();
        let get = |key: &str| {
            props
                .iter()
                .find(|(prop_key, _)| prop_key == key)
                .map(|(_, value)| value.clone())
        };

        assert_eq!(
            get(MV_DESCRIPTOR_PACKAGE_ID_PROP).as_deref(),
            Some("analytics.mv_orders")
        );
        assert_eq!(
            get(MV_DESCRIPTOR_HASH_PROP),
            Some(descriptor.content_hash().unwrap())
        );
        assert_eq!(
            MvDescriptorV1::from_json(&get(MV_DESCRIPTOR_INLINE_PROP).unwrap()).unwrap(),
            descriptor
        );

        let props_map = props
            .into_iter()
            .collect::<std::collections::HashMap<_, _>>();
        assert_eq!(
            MvDescriptorV1::from_storage_properties(&props_map).unwrap(),
            descriptor
        );
    }

    #[test]
    fn descriptor_properties_reject_hash_mismatch() {
        let descriptor = sample();
        let props = descriptor.to_storage_properties().unwrap();
        let mut props = props
            .into_iter()
            .collect::<std::collections::HashMap<_, _>>();
        props.insert(
            MV_DESCRIPTOR_HASH_PROP.to_string(),
            "not-the-hash".to_string(),
        );

        let err = MvDescriptorV1::from_storage_properties(&props).unwrap_err();

        assert!(err.contains("hash mismatch"), "got: {err}");
    }

    #[test]
    fn descriptor_properties_fail_fast_when_inline_exceeds_w1_cap() {
        let mut descriptor = sample();
        descriptor.logical_sql = "x".repeat(MV_DESCRIPTOR_INLINE_MAX_BYTES + 1);

        let err = descriptor.to_storage_properties().unwrap_err();

        assert!(err.contains("exceeds W1 cap"), "got: {err}");
        assert!(
            err.contains(&MV_DESCRIPTOR_INLINE_MAX_BYTES.to_string()),
            "got: {err}"
        );
        assert!(err.contains("externalized descriptor"), "got: {err}");
    }

    #[test]
    fn schema_contract_typed_preserves_absence() {
        let mut descriptor = sample();
        descriptor.schema_contract = None;

        assert_eq!(descriptor.schema_contract_typed().unwrap(), None);

        let round_trip = MvDescriptorV1::from_json(&descriptor.to_canonical_json().unwrap())
            .expect("round-trip descriptor without schema contract");
        assert_eq!(round_trip.schema_contract_typed().unwrap(), None);
    }

    #[test]
    fn schema_contract_typed_round_trips() {
        use crate::mv::domain::persistence::schema::{
            BaseContract, BaseFieldRecord, BaseSchemaSnapshot, ExpressionKind, ExpressionLineage,
            HiddenApplyKeyContract, MvSchemaContract, OutputColumnLineage, OutputContract,
            TargetContract, TargetVisibleColumn,
        };
        let contract = MvSchemaContract {
            contract_version: 1,
            base: BaseContract {
                table_fqn: "ice.ns.orders".to_string(),
                table_uuid: "u".to_string(),
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
                table_fqn: "ice.ns.mv".to_string(),
                table_uuid: "t".to_string(),
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
        };
        let mut d = sample();
        d.set_schema_contract(&contract).unwrap();
        assert_eq!(d.schema_contract_typed().unwrap().as_ref(), Some(&contract));

        // A descriptor with no contract yields None. `sample()` itself sets
        // `schema_contract` to an arbitrary JSON blob (for canonical-JSON/hash
        // tests elsewhere), so build the `None` case explicitly here rather
        // than relying on `sample()`'s default.
        let mut empty = sample();
        empty.schema_contract = None;
        assert_eq!(empty.schema_contract_typed().unwrap(), None);
    }
}
