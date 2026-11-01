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

//! Pure revalidation of ONE prepared source endpoint against a fresh observation.
//!
//! These checks acquire no access, discover no manifests and mint no read domain.
//! The original FE split owner owns the fresh observation, frozen Table/FileIO,
//! paid CPU/IO, retained facts and From-before-To scheduling. A successful proof
//! does not retain an external snapshot or guarantee later object access.

use crate::delete_semantics::ReadDomain;
use crate::iceberg::spec::{
    MappedField, NameMapping, PartitionSpec, Schema, Snapshot, TableMetadata, Type,
};
use crate::read_snapshot::{PartitionSourceTypeFailure, partition_source_types_checked};
use crate::schema_mapping::nominal_declaration::{TableNominalDocument, restrict_to_schema};
use novarocks_spi::connector::{
    ConnectorError, ConnectorErrorKind, ConnectorOperationControl,
    ConnectorTableObjectBindingFailure,
};

const NAME_MAPPING_PROPERTY: &str = "schema.name-mapping.default";

/// Check only this endpoint. In a Delta the caller completes the From proof
/// and frozen From discovery before invoking this for To. The domain's query
/// schema is the already-frozen To interpretation, including when checking From.
///
/// No comparison with observed Current, location, or unrelated properties/specs
/// is made. The caller supplies the original borrowed operation control; this
/// is not a new resource or cancellation owner.
pub(crate) fn validate_prepared_endpoint(
    frozen: &TableMetadata,
    observed: &TableMetadata,
    domain: &ReadDomain,
    control: &dyn ConnectorOperationControl,
) -> Result<(), ConnectorError> {
    control.check_active()?;
    validate_uuid(frozen, observed, domain)?;
    let snapshot_id = domain.endpoint().snapshot_id();
    let selected = frozen
        .snapshot_by_id(snapshot_id)
        .ok_or_else(|| corrupt("Iceberg prepared endpoint is absent from its frozen metadata"))?;
    let fresh = observed
        .snapshot_by_id(snapshot_id)
        .ok_or_else(|| not_found("Iceberg prepared endpoint is no longer visible"))?;
    if !same_snapshot_descriptor(selected, fresh) {
        return Err(corrupt("Iceberg prepared endpoint descriptor changed"));
    }

    // Snapshot::schema(observed) would fall back to the NEW current schema
    // when schema-id is absent. Resolve once in the original frozen metadata
    // and require that precise schema identity to remain in the observation.
    let selected_schema = selected
        .schema(frozen)
        .map_err(|_| corrupt("Iceberg frozen endpoint schema cannot be resolved"))?;
    let fresh_schema = observed
        .schema_by_id(selected_schema.schema_id())
        .ok_or_else(|| corrupt("Iceberg prepared endpoint schema is no longer retained"))?;
    if selected_schema.as_ref() != fresh_schema.as_ref() {
        return Err(corrupt("Iceberg prepared endpoint schema changed"));
    }

    let query = domain
        .endpoint()
        .schema()
        .map_err(|_| corrupt("Iceberg prepared read-domain schema cannot be decoded"))?;
    for metadata in [frozen, observed] {
        control.check_active()?;
        let schema = metadata.schema_by_id(query.schema_id()).ok_or_else(|| {
            corrupt("Iceberg prepared read interpretation schema is not retained")
        })?;
        // The original Schema equality includes field IDs, names, types,
        // nullability and initial/write defaults. Do not add another default
        // decoder or reinterpret values under observed current_schema().
        if schema.as_ref() != &query {
            return Err(corrupt(
                "Iceberg prepared read interpretation schema changed",
            ));
        }
    }
    validate_read_interpretation(frozen, observed, &query, control)
}

/// Validate only spec IDs supplied by this endpoint's completed original
/// manifest/data/delete discovery. Do not pass the entire frozen spec inventory
/// and do not do another discovery to obtain this set. Call before publishing
/// that endpoint's splits; retain the original From-before-To error order.
pub(crate) fn validate_referenced_specs(
    frozen: &TableMetadata,
    observed: &TableMetadata,
    domain: &ReadDomain,
    used_spec_ids: &[i32],
    control: &dyn ConnectorOperationControl,
) -> Result<(), ConnectorError> {
    control.check_active()?;
    validate_uuid(frozen, observed, domain)?;
    let query = domain
        .endpoint()
        .schema()
        .map_err(|_| corrupt("Iceberg prepared read-domain schema cannot be decoded"))?;
    for spec_id in used_spec_ids {
        control.check_active()?;
        let selected = frozen
            .partition_spec_by_id(*spec_id)
            .ok_or_else(|| corrupt("Iceberg used partition spec is absent from frozen metadata"))?;
        let fresh = observed
            .partition_spec_by_id(*spec_id)
            .ok_or_else(|| corrupt("Iceberg prepared partition spec is no longer retained"))?;
        let encoded = domain
            .endpoint()
            .partition_spec_jsons()
            .get(spec_id)
            .ok_or_else(|| {
                corrupt("Iceberg used partition spec is absent from the prepared domain")
            })?;
        let domain_spec: PartitionSpec = serde_json::from_str(encoded)
            .map_err(|_| corrupt("Iceberg prepared partition spec cannot be decoded"))?;
        if selected.as_ref() != fresh.as_ref() || selected.as_ref() != &domain_spec {
            return Err(corrupt("Iceberg prepared partition spec changed"));
        }
        let storage = domain
            .endpoint()
            .partition_type(*spec_id)
            .map_err(|_| corrupt("Iceberg prepared partition storage type cannot be decoded"))?;
        if storage.fields().len() != fresh.fields().len() {
            return Err(corrupt("Iceberg prepared partition storage shape changed"));
        }
        for (field, stored) in fresh.fields().iter().zip(storage.fields()) {
            control.check_active()?;
            // Reuse the original retained-type policy, including legal type
            // promotion and caller-control identity. This mirrors the frozen
            // domain author's transform application, not a new promotion rule.
            let (query_source, historical) =
                partition_source_types_checked(observed, &query, field.source_id, || {
                    control.check_active()
                })
                .map_err(|error| match error {
                    PartitionSourceTypeFailure::Original(original) => original,
                    PartitionSourceTypeFailure::Semantic(_) => {
                        corrupt("Iceberg prepared partition source history changed")
                    }
                })?;
            let ty = match field.transform.result_type(&Type::Primitive(query_source)) {
                Ok(ty) => ty,
                Err(_) => field
                    .transform
                    .result_type(&Type::Primitive(historical))
                    .map_err(|_| {
                        corrupt("Iceberg prepared partition transform storage type changed")
                    })?,
            };
            if stored.id != field.field_id
                || stored.name != field.name
                || stored.field_type.as_ref() != &ty
            {
                return Err(corrupt("Iceberg prepared partition storage type changed"));
            }
        }
    }
    Ok(())
}

/// To-stage only: prove the selected window still has its prepared lineage.
/// Both endpoints must have passed their own endpoint checks. A missing parent,
/// unrelated branch or bounded cycle is an execution failure, never a request
/// to prepare a new window or silently select FullRebuild/latest.
pub(crate) fn validate_window_lineage(
    observed: &TableMetadata,
    from: i64,
    to: i64,
    control: &dyn ConnectorOperationControl,
) -> Result<(), ConnectorError> {
    let mut cursor = to;
    // This is the existing inventory-derived traversal bound, not a new cap.
    for _ in 0..=observed.snapshots().len() {
        control.check_active()?;
        let snapshot = observed
            .snapshot_by_id(cursor)
            .ok_or_else(|| not_found("Iceberg prepared window ancestry is no longer visible"))?;
        if cursor == from {
            return Ok(());
        }
        cursor = snapshot.parent_snapshot_id().ok_or_else(|| {
            corrupt("Iceberg prepared endpoints no longer form one ancestry window")
        })?;
    }
    Err(corrupt("Iceberg prepared window ancestry contains a cycle"))
}

fn validate_uuid(
    frozen: &TableMetadata,
    observed: &TableMetadata,
    domain: &ReadDomain,
) -> Result<(), ConnectorError> {
    if domain.endpoint().table_uuid() != frozen.uuid() {
        return Err(corrupt(
            "Iceberg prepared domain differs from its frozen source UUID",
        ));
    }
    if observed.uuid() != frozen.uuid() {
        return Err(ConnectorError::table_object_binding(
            ConnectorTableObjectBindingFailure::Replaced,
            "Iceberg prepared source table UUID changed",
        ));
    }
    Ok(())
}

fn same_snapshot_descriptor(a: &Snapshot, b: &Snapshot) -> bool {
    a.snapshot_id() == b.snapshot_id()
        && a.manifest_list() == b.manifest_list()
        && a.sequence_number() == b.sequence_number()
        && a.parent_snapshot_id() == b.parent_snapshot_id()
        && a.schema_id() == b.schema_id()
        && a.row_range() == b.row_range()
        && a.encryption_key_id() == b.encryption_key_id()
}

fn validate_read_interpretation(
    frozen: &TableMetadata,
    observed: &TableMetadata,
    query: &Schema,
    control: &dyn ConnectorOperationControl,
) -> Result<(), ConnectorError> {
    // These are the existing declaration authors. Restrict the result to the
    // already-selected query schema; new lawful fields are not equality keys.
    let check = |_: crate::scalar_integer_domain::CheckPoint| control.check_active();
    let selected = TableNominalDocument::from_metadata_checked(frozen, &check)?;
    let fresh = TableNominalDocument::from_metadata_checked(observed, &check)?;
    control.check_active()?;
    if restrict_to_schema(query, &selected.nominals)? != restrict_to_schema(query, &fresh.nominals)?
        || crate::scalar_integer_domain::of_schema(query, &selected.integers)?
            != crate::scalar_integer_domain::of_schema(query, &fresh.integers)?
    {
        return Err(corrupt(
            "Iceberg prepared nominal read interpretation changed",
        ));
    }
    // Default values are already part of the selected schema above. Mapping
    // validation stays with its original provider author; project only the
    // selected field IDs and necessary ancestry. A future field's mapping is
    // not a new interpretation of this frozen read.
    let selected_mapping = selected_name_mapping(frozen, query, control)?;
    let fresh_mapping = selected_name_mapping(observed, query, control)?;
    if selected_mapping != fresh_mapping {
        return Err(corrupt("Iceberg prepared name mapping changed"));
    }
    Ok(())
}

fn selected_name_mapping(
    metadata: &TableMetadata,
    query: &Schema,
    control: &dyn ConnectorOperationControl,
) -> Result<Option<NameMapping>, ConnectorError> {
    control.check_active()?;
    let Some(raw) = metadata.properties().get(NAME_MAPPING_PROPERTY) else {
        return Ok(None);
    };
    let canonical = crate::schema_mapping::canonical_name_mapping(raw)
        .map_err(|_| corrupt("Iceberg prepared name mapping cannot be decoded"))?;
    // Reuse the SDK value shape after original validation; do not implement a
    // second name-mapping decoder or accept different malformed declarations.
    let mapping: NameMapping = serde_json::from_str(&canonical)
        .map_err(|_| corrupt("Iceberg prepared name mapping cannot be decoded"))?;
    let fields = selected_mapping_fields(mapping.fields(), query, control)?;
    Ok((!fields.is_empty()).then(|| NameMapping::new(fields)))
}

fn selected_mapping_fields(
    fields: &[MappedField],
    query: &Schema,
    control: &dyn ConnectorOperationControl,
) -> Result<Vec<MappedField>, ConnectorError> {
    let mut selected = Vec::new();
    for field in fields {
        control.check_active()?;
        // Retain ancestry even if only a nested field is selected.
        let mut kept_children = Vec::new();
        for child in field.fields() {
            control.check_active()?;
            kept_children.extend(selected_mapping_fields(
                std::slice::from_ref(child.as_ref()),
                query,
                control,
            )?);
        }
        if field
            .field_id()
            .is_some_and(|id| query.field_by_id(id).is_some())
            || !kept_children.is_empty()
        {
            let mut names = Vec::new();
            for name in field.names() {
                control.check_active()?;
                names.push(name.clone());
            }
            // Sibling/alias order does not change lookup. Original canonical
            // validation has already rejected duplicate IDs and aliases.
            names.sort_unstable();
            selected.push(MappedField::new(field.field_id(), names, kept_children));
        }
    }
    selected.sort_unstable_by_key(MappedField::field_id);
    Ok(selected)
}

fn corrupt(message: &'static str) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::CorruptData, message)
}
fn not_found(message: &'static str) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::NotFound, message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::iceberg::spec::{
        FormatVersion, Literal, NestedField, Operation, PartitionSpec, PrimitiveType, Schema,
        Snapshot, SortOrder, Summary, TableMetadataBuilder, Transform,
    };
    use crate::scalar_integer_domain::ScalarIntegerDomain;
    use crate::schema_mapping::nominal_declaration::{PROPERTY, encode_views};
    use serde_json::{Value, json};
    use std::collections::{BTreeMap, HashMap};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct Active;
    impl ConnectorOperationControl for Active {
        fn check_active(&self) -> Result<(), ConnectorError> {
            Ok(())
        }
    }
    struct RefuseAt {
        at: usize,
        checks: AtomicUsize,
        original: ConnectorError,
    }
    impl ConnectorOperationControl for RefuseAt {
        fn check_active(&self) -> Result<(), ConnectorError> {
            if self.checks.fetch_add(1, Ordering::SeqCst) >= self.at {
                Err(self.original.clone())
            } else {
                Ok(())
            }
        }
    }

    fn snapshot(id: i64, parent: Option<i64>, sequence: i64, base_ms: i64) -> Snapshot {
        Snapshot::builder()
            .with_snapshot_id(id)
            .with_parent_snapshot_id(parent)
            .with_sequence_number(sequence)
            .with_timestamp_ms(base_ms + sequence)
            .with_manifest_list(format!("memory:///prepared/metadata/snap-{id}.avro"))
            .with_schema_id(0)
            .with_row_range(0, 0)
            .with_summary(Summary {
                operation: Operation::Append,
                additional_properties: HashMap::new(),
            })
            .build()
    }
    fn frozen() -> TableMetadata {
        let schema = Schema::builder()
            .with_fields(vec![
                Arc::new(NestedField::required(
                    1,
                    "id",
                    Type::Primitive(PrimitiveType::Int),
                )),
                Arc::new(
                    NestedField::optional(2, "value", Type::Primitive(PrimitiveType::String))
                        .with_initial_default(Literal::string("initial"))
                        .with_write_default(Literal::string("write")),
                ),
            ])
            .build()
            .unwrap();
        let spec = PartitionSpec::builder(Arc::new(schema.clone()))
            .add_partition_field("id", "id_part", Transform::Identity)
            .unwrap()
            .build()
            .unwrap();
        let properties = HashMap::from([(
            NAME_MAPPING_PROPERTY.to_string(),
            r#"[{"field-id":1,"names":["id"]},{"field-id":2,"names":["value"]}]"#.to_string(),
        )]);
        let base = TableMetadataBuilder::new(
            schema,
            spec.into_unbound(),
            SortOrder::unsorted_order(),
            "memory:///prepared".to_string(),
            FormatVersion::V3,
            properties,
        )
        .unwrap()
        .build()
        .unwrap()
        .metadata;
        let base_ms = base.last_updated_ms();
        base.into_builder(None)
            .set_branch_snapshot(snapshot(10, None, 1, base_ms), "main")
            .unwrap()
            .set_branch_snapshot(snapshot(20, Some(10), 2, base_ms), "main")
            .unwrap()
            .build()
            .unwrap()
            .metadata
    }
    fn domain(metadata: &TableMetadata, endpoint: i64) -> Arc<ReadDomain> {
        crate::read_snapshot::mint_read_domain(metadata, endpoint, metadata.current_schema())
            .unwrap()
    }
    fn edit(metadata: &TableMetadata, change: impl FnOnce(&mut Value)) -> TableMetadata {
        let mut json = serde_json::to_value(metadata).unwrap();
        change(&mut json);
        serde_json::from_value(json).unwrap()
    }
    fn endpoint_mut(value: &mut Value, id: i64) -> &mut Value {
        value["snapshots"]
            .as_array_mut()
            .unwrap()
            .iter_mut()
            .find(|snapshot| snapshot["snapshot-id"].as_i64() == Some(id))
            .unwrap()
    }
    fn used() -> [i32; 1] {
        [0]
    }

    #[test]
    fn later_append_current_schema_location_and_unreferenced_spec_do_not_rebind_endpoint() {
        let frozen = frozen();
        let domain = domain(&frozen, 20);
        let observed = edit(&frozen, |value| {
            let mut schema = value["schemas"][0].clone();
            schema["schema-id"] = json!(1);
            schema["fields"][0]["name"] = json!("renamed_after_to");
            value["schemas"].as_array_mut().unwrap().push(schema);
            value["current-schema-id"] = json!(1);
            let mut next = endpoint_mut(value, 20).clone();
            next["snapshot-id"] = json!(30);
            next["parent-snapshot-id"] = json!(20);
            next["sequence-number"] = json!(3);
            next["schema-id"] = json!(1);
            next["manifest-list"] = json!("memory:///future/metadata/snap-30.avro");
            value["snapshots"].as_array_mut().unwrap().push(next);
            value["current-snapshot-id"] = json!(30);
            value["refs"]["main"]["snapshot-id"] = json!(30);
            value["last-sequence-number"] = json!(3);
            value["location"] = json!("memory:///relocated");
            value["properties"]["unrelated.future.property"] = json!("changed");
            value["properties"]["read.split.target-size"] = json!("4096");
            let mut spec = value["partition-specs"][0].clone();
            spec["spec-id"] = json!(1);
            spec["fields"][0]["field-id"] = json!(1001);
            spec["fields"][0]["transform"] = json!("bucket[8]");
            value["last-partition-id"] = json!(1001);
            value["partition-specs"].as_array_mut().unwrap().push(spec);
            value["default-spec-id"] = json!(1);
        });
        validate_prepared_endpoint(&frozen, &observed, &domain, &Active).unwrap();
        validate_referenced_specs(&frozen, &observed, &domain, &used(), &Active).unwrap();
        validate_window_lineage(&observed, 10, 20, &Active).unwrap();
    }

    #[test]
    fn snapshot_without_schema_id_retains_frozen_schema_instead_of_new_current() {
        let frozen = edit(&frozen(), |value| {
            endpoint_mut(value, 20)
                .as_object_mut()
                .unwrap()
                .remove("schema-id");
        });
        let domain = domain(&frozen, 20);
        let observed = edit(&frozen, |value| {
            let mut next = value["schemas"][0].clone();
            next["schema-id"] = json!(1);
            next["fields"][0]["name"] = json!("future_name");
            value["schemas"].as_array_mut().unwrap().push(next);
            value["current-schema-id"] = json!(1);
        });
        validate_prepared_endpoint(&frozen, &observed, &domain, &Active).unwrap();
    }

    #[test]
    fn replaced_uuid_and_missing_selected_endpoint_remain_typed() {
        let frozen = frozen();
        let domain = domain(&frozen, 20);
        let replaced = edit(&frozen, |value| {
            value["table-uuid"] = json!(uuid::Uuid::new_v4())
        });
        assert_eq!(
            validate_prepared_endpoint(&frozen, &replaced, &domain, &Active)
                .unwrap_err()
                .table_object_binding_failure(),
            Some(ConnectorTableObjectBindingFailure::Replaced)
        );
        let expired = edit(&frozen, |value| {
            value["snapshots"]
                .as_array_mut()
                .unwrap()
                .retain(|v| v["snapshot-id"] != json!(20));
            value["current-snapshot-id"] = json!(10);
            value["refs"]["main"]["snapshot-id"] = json!(10);
        });
        assert_eq!(
            validate_prepared_endpoint(&frozen, &expired, &domain, &Active)
                .unwrap_err()
                .kind(),
            ConnectorErrorKind::NotFound
        );
    }

    #[test]
    fn selected_manifest_sequence_parent_and_schema_descriptor_changes_are_rejected() {
        let frozen = frozen();
        let domain = domain(&frozen, 20);
        for (key, value) in [
            ("manifest-list", json!("memory:///different.avro")),
            ("sequence-number", json!(1)),
            ("parent-snapshot-id", json!(None::<i64>)),
            ("schema-id", json!(None::<i32>)),
        ] {
            let observed = edit(&frozen, |json| endpoint_mut(json, 20)[key] = value);
            assert_eq!(
                validate_prepared_endpoint(&frozen, &observed, &domain, &Active)
                    .unwrap_err()
                    .kind(),
                ConnectorErrorKind::CorruptData,
                "{key}"
            );
        }
    }

    #[test]
    fn selected_schema_and_initial_read_default_are_not_reinterpreted() {
        let frozen = frozen();
        let domain = domain(&frozen, 20);
        for (key, value) in [
            ("name", json!("wrong_name")),
            ("initial-default", json!("changed")),
        ] {
            let observed = edit(&frozen, |json| json["schemas"][0]["fields"][1][key] = value);
            assert_eq!(
                validate_prepared_endpoint(&frozen, &observed, &domain, &Active)
                    .unwrap_err()
                    .kind(),
                ConnectorErrorKind::CorruptData
            );
        }
    }

    #[test]
    fn nominal_and_default_name_mapping_use_the_existing_policy_authors() {
        let frozen = frozen();
        let domain = domain(&frozen, 20);
        let integers = BTreeMap::from([(1, ScalarIntegerDomain::Int8)]);
        let declared = frozen
            .clone()
            .into_builder(None)
            .set_properties(HashMap::from([(
                PROPERTY.to_string(),
                encode_views(&BTreeMap::new(), &integers).unwrap(),
            )]))
            .unwrap()
            .build()
            .unwrap()
            .metadata;
        assert_eq!(
            validate_prepared_endpoint(&frozen, &declared, &domain, &Active)
                .unwrap_err()
                .kind(),
            ConnectorErrorKind::CorruptData
        );
        let mapping_changed = edit(&frozen, |json| {
            json["properties"][NAME_MAPPING_PROPERTY] =
                json!(r#"[{"field-id":1,"names":["different"]},{"field-id":2,"names":["value"]}]"#);
        });
        assert_eq!(
            validate_prepared_endpoint(&frozen, &mapping_changed, &domain, &Active)
                .unwrap_err()
                .kind(),
            ConnectorErrorKind::CorruptData
        );
        let reformatted = edit(&frozen, |json| {
            json["properties"][NAME_MAPPING_PROPERTY] = json!(
                r#"[ { "names": [ "id" ], "field-id": 1 }, { "names": ["value"], "field-id": 2 } ]"#
            );
        });
        validate_prepared_endpoint(&frozen, &reformatted, &domain, &Active).unwrap();
    }

    #[test]
    fn future_field_nominal_mapping_and_default_do_not_change_selected_interpretation() {
        let frozen = frozen();
        let domain = domain(&frozen, 20);
        let declaration = encode_views(
            &BTreeMap::from([(
                3,
                crate::schema_mapping::nominal_declaration::NominalSourceDomain::Hll,
            )]),
            &BTreeMap::new(),
        )
        .unwrap();
        let observed = edit(&frozen, |value| {
            let mut schema = value["schemas"][0].clone();
            schema["schema-id"] = json!(1);
            // A future retained field is outside the original To read domain.
            schema["fields"].as_array_mut().unwrap().push(json!({
                "id":3, "name":"future_hll", "required":false, "type":"binary"
            }));
            schema["fields"][1]["initial-default"] = json!("future default");
            value["schemas"].as_array_mut().unwrap().push(schema);
            value["current-schema-id"] = json!(1);
            value["last-column-id"] = json!(3);
            value["properties"][PROPERTY] = json!(declaration);
            value["properties"][NAME_MAPPING_PROPERTY] = json!(
                r#"[{"field-id":3,"names":["future_hll"]},{"field-id":2,"names":["value"]},{"field-id":1,"names":["id"]}]"#
            );
        });
        validate_prepared_endpoint(&frozen, &observed, &domain, &Active).unwrap();
        validate_referenced_specs(&frozen, &observed, &domain, &used(), &Active).unwrap();
    }

    #[test]
    fn referenced_partition_source_history_must_still_prove_frozen_storage() {
        let frozen = frozen();
        let domain = domain(&frozen, 20);
        let observed = edit(&frozen, |value| {
            let mut schema = value["schemas"][0].clone();
            schema["schema-id"] = json!(1);
            schema["fields"][0]["type"] = json!("double");
            value["schemas"].as_array_mut().unwrap().push(schema);
            value["current-schema-id"] = json!(1);
        });
        // The original retained-type author rejects Int -> Double as storage
        // evidence, even though the selected schema/spec documents survived.
        assert_eq!(
            validate_referenced_specs(&frozen, &observed, &domain, &used(), &Active)
                .unwrap_err()
                .kind(),
            ConnectorErrorKind::CorruptData
        );
    }

    #[test]
    fn referenced_spec_changes_fail_without_comparing_the_whole_spec_inventory() {
        let frozen = frozen();
        let domain = domain(&frozen, 20);
        let observed = edit(&frozen, |json| {
            json["partition-specs"][0]["fields"][0]["transform"] = json!("bucket[8]");
        });
        // No object depended on this spec in an empty endpoint discovery.
        validate_referenced_specs(&frozen, &observed, &domain, &[], &Active).unwrap();
        assert_eq!(
            validate_referenced_specs(&frozen, &observed, &domain, &used(), &Active)
                .unwrap_err()
                .kind(),
            ConnectorErrorKind::CorruptData
        );
    }

    #[test]
    fn a_from_check_does_not_validate_the_to_endpoint_early() {
        let frozen = frozen();
        let from = domain(&frozen, 10);
        let to = domain(&frozen, 20);
        let observed = edit(&frozen, |json| {
            endpoint_mut(json, 20)["manifest-list"] = json!("memory:///changed-to.avro");
        });
        validate_prepared_endpoint(&frozen, &observed, &from, &Active).unwrap();
        assert_eq!(
            validate_prepared_endpoint(&frozen, &observed, &to, &Active)
                .unwrap_err()
                .kind(),
            ConnectorErrorKind::CorruptData
        );
        // The actual From manifest failure + To mismatch integration test stays
        // with the caller that owns full discovery; this pure helper cannot
        // manufacture a manifest outcome or certify that scheduling contract.
    }

    #[test]
    fn lineage_checks_selected_endpoints_equal_case_missing_parent_and_cycle() {
        let frozen = frozen();
        validate_window_lineage(&frozen, 10, 20, &Active).unwrap();
        validate_window_lineage(&frozen, 20, 20, &Active).unwrap();
        let unrelated = edit(&frozen, |json| {
            endpoint_mut(json, 20)
                .as_object_mut()
                .unwrap()
                .remove("parent-snapshot-id");
        });
        assert_eq!(
            validate_window_lineage(&unrelated, 10, 20, &Active)
                .unwrap_err()
                .kind(),
            ConnectorErrorKind::CorruptData
        );
        let missing = edit(&frozen, |json| {
            endpoint_mut(json, 20)["parent-snapshot-id"] = json!(999)
        });
        assert_eq!(
            validate_window_lineage(&missing, 10, 20, &Active)
                .unwrap_err()
                .kind(),
            ConnectorErrorKind::NotFound
        );
        let cyclic = edit(&frozen, |json| {
            endpoint_mut(json, 20)["parent-snapshot-id"] = json!(20)
        });
        assert_eq!(
            validate_window_lineage(&cyclic, 10, 20, &Active)
                .unwrap_err()
                .kind(),
            ConnectorErrorKind::CorruptData
        );
    }

    #[test]
    fn original_control_fact_survives_endpoint_spec_and_ancestry_checks() {
        let frozen = frozen();
        let domain = domain(&frozen, 20);
        let original = ConnectorError::compile_interruption(
            novarocks_type_contract::CompileControlError::ResourceExhausted,
        );
        let refuse = || RefuseAt {
            at: 1,
            checks: AtomicUsize::new(0),
            original: original.clone(),
        };
        assert_eq!(
            validate_prepared_endpoint(&frozen, &frozen, &domain, &refuse()).unwrap_err(),
            original
        );
        assert_eq!(
            validate_referenced_specs(&frozen, &frozen, &domain, &used(), &refuse()).unwrap_err(),
            original
        );
        assert_eq!(
            validate_window_lineage(&frozen, 10, 20, &refuse()).unwrap_err(),
            original
        );
    }
}
