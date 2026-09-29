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

//! Canonical provider fact encodings, distinct from raw metadata-file bytes.
//!
//! Only SDK set/map-backed collections are reordered. Schema fields, transform
//! fields, sort fields, snapshot/metadata logs and physical blob lists keep their
//! semantic order. This is an identity projection, not a metadata writer.

use crate::iceberg::spec::{Schema, TableMetadata};
use serde_json::Value;

pub(crate) fn canonical_schema_json(schema: &Schema) -> serde_json::Result<String> {
    let mut value = serde_json::to_value(schema)?;
    normalize_schema(&mut value)?;
    value.sort_all_objects();
    serde_json::to_string(&value)
}

pub(crate) fn canonical_metadata_json(metadata: &TableMetadata) -> serde_json::Result<Vec<u8>> {
    let mut value = serde_json::to_value(metadata)?;
    normalize_metadata(&mut value)?;
    value.sort_all_objects();
    serde_json::to_vec(&value)
}

fn invalid(message: impl std::fmt::Display) -> serde_json::Error {
    <serde_json::Error as serde::ser::Error>::custom(message)
}

fn normalize_schema(schema: &mut Value) -> serde_json::Result<()> {
    if let Some(ids) = schema.get_mut("identifier-field-ids") {
        let ids = ids
            .as_array_mut()
            .ok_or_else(|| invalid("schema identifier IDs are not an array"))?;
        if ids.iter().any(|id| id.as_i64().is_none()) {
            return Err(invalid("schema identifier ID is not an integer"));
        }
        ids.sort_unstable_by_key(Value::as_i64);
    }
    Ok(())
}

fn sort_id_map(metadata: &mut Value, collection: &str, id: &str) -> serde_json::Result<()> {
    if let Some(values) = metadata
        .get_mut(collection)
        .filter(|value| !value.is_null())
    {
        let values = values
            .as_array_mut()
            .ok_or_else(|| invalid(format!("metadata {collection} is not an array")))?;
        if values
            .iter()
            .any(|value| value.get(id).and_then(Value::as_i64).is_none())
        {
            return Err(invalid(format!(
                "metadata {collection} has a noninteger {id}"
            )));
        }
        values.sort_unstable_by_key(|value| value[id].as_i64());
    }
    Ok(())
}

fn normalize_metadata(metadata: &mut Value) -> serde_json::Result<()> {
    // V1 emits both the current schema and the historical schema collection.
    if let Some(schema) = metadata.get_mut("schema").filter(|value| !value.is_null()) {
        normalize_schema(schema)?;
    }
    if let Some(schemas) = metadata.get_mut("schemas").and_then(Value::as_array_mut) {
        for schema in schemas {
            normalize_schema(schema)?;
        }
    }
    // TableMetadata's SDK representation owns these collections as maps.
    // Their serialized vector order is determined by randomized hash state.
    for (collection, id) in [
        ("schemas", "schema-id"),
        ("partition-specs", "spec-id"),
        ("sort-orders", "order-id"),
        ("snapshots", "snapshot-id"),
        ("statistics", "snapshot-id"),
        ("partition-statistics", "snapshot-id"),
    ] {
        sort_id_map(metadata, collection, id)?;
    }
    if let Some(keys) = metadata
        .get_mut("encryption-keys")
        .filter(|value| !value.is_null())
    {
        let keys = keys
            .as_array_mut()
            .ok_or_else(|| invalid("metadata encryption keys are not an array"))?;
        if keys
            .iter()
            .any(|key| key.get("key-id").and_then(Value::as_str).is_none())
        {
            return Err(invalid("metadata encryption key has no string key-id"));
        }
        keys.sort_unstable_by(|a, b| a["key-id"].as_str().cmp(&b["key-id"].as_str()));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::delete_semantics::{PinnedEndpointFacts, ReadDomain, ReadObservationId};
    use crate::iceberg::spec::{NestedField, PartitionSpec, PrimitiveType, Type};
    use serde_json::json;
    use std::collections::HashSet;
    use std::sync::Arc;

    fn schema(ids: impl IntoIterator<Item = i32>) -> Schema {
        Schema::builder()
            .with_schema_id(3)
            .with_fields([
                Arc::new(NestedField::required(
                    11,
                    "first",
                    Type::Primitive(PrimitiveType::Long),
                )),
                Arc::new(NestedField::required(
                    2,
                    "second",
                    Type::Primitive(PrimitiveType::String),
                )),
            ])
            .with_identifier_field_ids(ids.into_iter().collect::<HashSet<_>>())
            .build()
            .unwrap()
    }

    fn domain(schema: &Schema) -> ReadDomain {
        ReadDomain::new(
            ReadObservationId::try_new([3; 16]).unwrap(),
            PinnedEndpointFacts::try_new(
                uuid::Uuid::from_u128(7),
                "exact-metadata",
                19,
                schema,
                &[PartitionSpec::unpartition_spec()],
            )
            .unwrap(),
        )
    }

    #[test]
    fn independent_identifier_sets_and_repeated_domain_roundtrips_have_one_identity() {
        let expected = domain(&schema([11, 2]));
        assert_eq!(
            serde_json::from_str::<Value>(expected.endpoint().schema_json()).unwrap()["identifier-field-ids"],
            json!([2, 11])
        );
        for iteration in 0..64 {
            // Fresh SDK builds allocate independent HashSets, not cloned seeds.
            let rebuilt = schema(if iteration % 2 == 0 { [2, 11] } else { [11, 2] });
            assert_eq!(domain(&rebuilt), expected);
            let mut current = expected.clone();
            for _ in 0..4 {
                let decoded: Schema =
                    serde_json::from_str(current.endpoint().schema_json()).unwrap();
                current = domain(&decoded);
                assert_eq!(current, expected);
            }
        }
    }

    #[test]
    fn schema_field_order_remains_part_of_identity() {
        let original = schema([11, 2]);
        let mut reordered = serde_json::to_value(&original).unwrap();
        reordered["fields"].as_array_mut().unwrap().reverse();
        let reordered: Schema = serde_json::from_value(reordered).unwrap();
        assert_ne!(
            canonical_schema_json(&original).unwrap(),
            canonical_schema_json(&reordered).unwrap()
        );
    }

    #[test]
    fn metadata_normalization_changes_only_declared_set_and_map_orders() {
        let ordered = json!({
            "fields":[{"id":11},{"id":2}],
            "partition-spec":[{"field-id":1001},{"field-id":1000}],
            "snapshot-log":[{"snapshot-id":78},{"snapshot-id":77}],
            "metadata-log":[{"timestamp-ms":2},{"timestamp-ms":1}],
            "schemas":[{"schema-id":2,"identifier-field-ids":[11,2],"fields":[11,2]},{"schema-id":1,"identifier-field-ids":[2,11],"fields":[2,11]}],
            "partition-specs":[{"spec-id":1,"fields":[1001,1000]},{"spec-id":0,"fields":[]}],
            "sort-orders":[{"order-id":1,"fields":[11,2]},{"order-id":0,"fields":[]}],
            "snapshots":[{"snapshot-id":78},{"snapshot-id":77}],
            "statistics":[{"snapshot-id":78,"blob-metadata":[2,1]},{"snapshot-id":77,"blob-metadata":[1,2]}],
            "partition-statistics":[{"snapshot-id":78},{"snapshot-id":77}],
            "encryption-keys":[{"key-id":"b"},{"key-id":"a"}]
        });
        let mut normalized = ordered.clone();
        normalize_metadata(&mut normalized).unwrap();
        for name in ["fields", "partition-spec", "snapshot-log", "metadata-log"] {
            assert_eq!(normalized[name], ordered[name]);
        }
        for (name, id) in [
            ("schemas", "schema-id"),
            ("partition-specs", "spec-id"),
            ("sort-orders", "order-id"),
            ("snapshots", "snapshot-id"),
            ("statistics", "snapshot-id"),
            ("partition-statistics", "snapshot-id"),
        ] {
            let values = normalized[name].as_array().unwrap();
            assert!(values[0][id].as_i64() < values[1][id].as_i64());
        }
        assert_eq!(normalized["schemas"][1]["fields"], json!([11, 2]));
        assert_eq!(
            normalized["partition-specs"][1]["fields"],
            json!([1001, 1000])
        );
        assert_eq!(normalized["sort-orders"][1]["fields"], json!([11, 2]));
        assert_eq!(normalized["statistics"][1]["blob-metadata"], json!([2, 1]));
        assert_eq!(normalized["encryption-keys"][0]["key-id"], "a");
    }
}
