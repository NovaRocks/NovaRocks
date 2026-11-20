// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Pure preparation of an evaluated partitioning property on an existing table.

use super::partitioning_binding::{PropertyOutputNames, resolve_property_partition_source};
use crate::catalog_control::catalog_mutation::{
    INITIAL_PARTITION_FIELD_ID, PartitionEvolutionFailure, finalize_existing_partition_change,
    partition_source, partition_transform, validate_partition_transform,
};
use crate::commit::staging::PreparedChange;
use crate::iceberg::spec::{
    FormatVersion, PartitionField, TableMetadata, Transform, UnboundPartitionField,
};
use crate::iceberg::{ErrorKind, TableRequirement};
use novarocks_failure_contract::{FailureFact, FailureProjection};
use novarocks_spi::connector::{
    ConnectorError, ConnectorErrorKind, ConnectorOperationControl, ConnectorPartitionTransform,
    InvalidTableProperty,
};
use std::collections::{HashMap, HashSet};

pub(crate) fn prepare_full_partitioning_change(
    metadata: &TableMetadata,
    fields: &[Option<String>],
    control: &dyn ConnectorOperationControl,
) -> Result<PreparedChange, ConnectorError> {
    prepare_full_partitioning_change_inner(metadata, fields, control)
        .map_err(InvalidTableProperty::from_property_error)
}

fn prepare_full_partitioning_change_inner(
    metadata: &TableMetadata,
    fields: &[Option<String>],
    control: &dyn ConnectorOperationControl,
) -> Result<PreparedChange, ConnectorError> {
    control.check_active()?;
    let schema = metadata.current_schema();
    let current = metadata.default_partition_spec();
    let mut history = Vec::new();
    for spec in metadata.partition_specs_iter() {
        control.check_active()?;
        for field in spec.fields() {
            control.check_active()?;
            if schema.field_by_id(field.source_id).is_some() {
                history.push(field);
            }
        }
    }

    // Parse and bind each source before advancing, then run the shared type
    // checks in request order after name admission.
    let mut names = PropertyOutputNames::new(schema, fields.len())?;
    let mut requested = Vec::with_capacity(fields.len());
    let mut positions = HashMap::with_capacity(fields.len());
    for (index, input) in fields.iter().enumerate() {
        control.check_active()?;
        let (source_id, field) =
            resolve_property_partition_source(schema, input.as_deref(), index, "ALTER", control)?;
        let transform = partition_transform(&field);
        let include_width = needs_width_in_name(&history, &field, transform, control)?;
        let name = names.admit(schema, source_id, &field, include_width, control)?;
        if positions
            .insert((source_id, transform), requested.len())
            .is_some()
        {
            return Err(invalid("duplicate transform in desired partitioning"));
        }
        requested.push((source_id, transform, name, field));
    }
    for (source_id, _, _, field) in &requested {
        control.check_active()?;
        validate_partition_transform(schema, *source_id, field)
            .map_err(|cause| cause.into_connector_error())?;
    }

    let v1 = metadata.format_version() == FormatVersion::V1;
    let mut retained = vec![false; requested.len()];
    let mut output = Vec::new();
    let mut names_in_use = HashSet::new();
    let mut tombstones = HashMap::new();
    for field in current.fields() {
        control.check_active()?;
        if field.transform == Transform::Unknown {
            return Err(unsupported(
                "current partition spec has an unknown transform",
            ));
        }
        if let Some(&index) = positions.get(&(field.source_id, field.transform)) {
            if std::mem::replace(&mut retained[index], true) {
                return Err(unsupported(
                    "current partition transform has multiple identities",
                ));
            }
            names_in_use.insert(field.name.clone());
            output.push(field.clone().into_unbound());
        } else if v1 {
            // V1 removals preserve slot/ID as Void. Keep existing physical
            // tombstones even when the active desired field list is empty.
            let mut removed = field.clone().into_unbound();
            removed.transform = Transform::Void;
            tombstones.insert(removed.name.clone(), output.len());
            names_in_use.insert(removed.name.clone());
            output.push(removed);
        }
    }

    let mut last_id = metadata.last_partition_id();
    for (index, (source_id, transform, name, _)) in requested.into_iter().enumerate() {
        control.check_active()?;
        if retained[index] {
            continue;
        }
        if let Some(position) = tombstones.remove(&name) {
            // Iceberg V1 only renames a tombstone on a real same-name addition.
            let original = &mut output[position];
            let id = original.field_id.expect("existing partition field ID");
            let renamed = format!("{name}_{id}");
            if !names_in_use.insert(renamed.clone()) {
                return Err(unsupported(
                    "retired V1 partition field has an ambiguous name",
                ));
            }
            names_in_use.remove(&name);
            original.name = renamed;
        }
        if !names_in_use.insert(name.clone()) {
            return Err(invalid(format!(
                "partition output name is repeated: {name}"
            )));
        }
        let historical_id = if v1 {
            None
        } else {
            historical_id(&history, source_id, transform, &name, control)?
        };
        let field_id = match historical_id {
            Some(id) => id,
            None => {
                last_id = last_id.checked_add(1).ok_or_else(|| {
                    ConnectorError::new(
                        ConnectorErrorKind::ResourceExhausted,
                        "Iceberg partition field ID overflow",
                    )
                })?;
                last_id
            }
        };
        output.push(UnboundPartitionField {
            source_id,
            field_id: Some(field_id),
            name,
            transform,
        });
    }

    let mut ids = HashSet::with_capacity(output.len());
    for (position, field) in output.iter().enumerate() {
        control.check_active()?;
        let id = field
            .field_id
            .expect("partition preparation assigned field ID");
        if !ids.insert(id) {
            return Err(unsupported(
                "desired partition fields share a historical ID",
            ));
        }
        if v1
            && i32::try_from(position)
                .ok()
                .and_then(|position| INITIAL_PARTITION_FIELD_ID.checked_add(position))
                != Some(id)
        {
            return Err(unsupported(
                "V1 partition field IDs cannot be preserved sequentially",
            ));
        }
    }

    if matching_fields(current.fields(), &output, control)? == FieldMatch::Same {
        return Ok(PreparedChange {
            requirements: vec![TableRequirement::DefaultSpecIdMatch {
                default_spec_id: current.spec_id(),
            }],
            updates: vec![],
        });
    }
    // The SDK finds a shape-compatible spec without considering field IDs.
    // Refuse a conflicting historical shape before that unordered lookup.
    let mut has_compatible_history = false;
    let mut highest_spec_id = current.spec_id();
    for spec in metadata.partition_specs_iter() {
        control.check_active()?;
        highest_spec_id = highest_spec_id.max(spec.spec_id());
        match matching_fields(spec.fields(), &output, control)? {
            FieldMatch::DifferentShape => {}
            FieldMatch::DifferentIds => {
                return Err(unsupported(
                    "compatible historical partition spec has different IDs",
                ));
            }
            FieldMatch::Same => has_compatible_history = true,
        }
    }
    if !has_compatible_history && highest_spec_id == i32::MAX {
        return Err(ConnectorError::new(
            ConnectorErrorKind::ResourceExhausted,
            "Iceberg partition spec ID overflow",
        ));
    }

    // SDK reuse preserves ordered source/transform/name, but its compatibility
    // relation ignores IDs. Keep the intended IDs across the actual SDK call.
    let expected_ids: Vec<_> = output.iter().map(|field| field.field_id).collect();
    let (change, committed) =
        finalize_existing_partition_change(metadata, output).map_err(finalization_error)?;
    if committed.fields().len() != expected_ids.len() {
        return Err(unsupported(
            "Iceberg SDK selected a different partition identity",
        ));
    }
    for (actual, expected) in committed.fields().iter().zip(expected_ids) {
        control.check_active()?;
        if Some(actual.field_id) != expected {
            return Err(unsupported(
                "Iceberg SDK selected a different partition identity",
            ));
        }
    }
    control.check_active()?;
    Ok(change)
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum FieldMatch {
    DifferentShape,
    DifferentIds,
    Same,
}

fn matching_fields(
    actual: &[PartitionField],
    expected: &[UnboundPartitionField],
    control: &dyn ConnectorOperationControl,
) -> Result<FieldMatch, ConnectorError> {
    if actual.len() != expected.len() {
        return Ok(FieldMatch::DifferentShape);
    }
    let mut same_ids = true;
    for (actual, expected) in actual.iter().zip(expected) {
        control.check_active()?;
        if actual.source_id != expected.source_id
            || actual.name != expected.name
            || actual.transform != expected.transform
        {
            return Ok(FieldMatch::DifferentShape);
        }
        same_ids &= Some(actual.field_id) == expected.field_id;
    }
    Ok(if same_ids {
        FieldMatch::Same
    } else {
        FieldMatch::DifferentIds
    })
}

fn needs_width_in_name(
    history: &[&PartitionField],
    field: &ConnectorPartitionTransform,
    transform: Transform,
    control: &dyn ConnectorOperationControl,
) -> Result<bool, ConnectorError> {
    let suffix = match field {
        ConnectorPartitionTransform::Bucket { .. } => "_bucket",
        ConnectorPartitionTransform::Truncate { .. } => "_trunc",
        _ => return Ok(false),
    };
    let name = format!("{}{suffix}", partition_source(field));
    for field in history {
        control.check_active()?;
        if field.name.eq_ignore_ascii_case(&name) && field.transform != transform {
            return Ok(true);
        }
    }
    Ok(false)
}

fn historical_id(
    history: &[&PartitionField],
    source_id: i32,
    transform: Transform,
    name: &str,
    control: &dyn ConnectorOperationControl,
) -> Result<Option<i32>, ConnectorError> {
    let mut id = None;
    for field in history {
        control.check_active()?;
        if field.source_id == source_id && field.transform == transform && field.name == name {
            if id.is_some_and(|existing| existing != field.field_id) {
                return Err(unsupported("historical partition field has multiple IDs"));
            }
            id = Some(field.field_id);
        }
    }
    Ok(id)
}

#[derive(Debug)]
struct UnrepresentableSpec {
    stage: &'static str,
    cause: crate::iceberg::Error,
    kind: ConnectorErrorKind,
}
impl std::fmt::Display for UnrepresentableSpec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} evolved Iceberg partition spec: {}",
            self.stage, self.cause
        )
    }
}
impl std::error::Error for UnrepresentableSpec {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.cause)
    }
}
impl FailureProjection for UnrepresentableSpec {
    fn failure_fact(&self) -> FailureFact {
        self.kind.failure_fact()
    }
}

fn finalization_error(error: PartitionEvolutionFailure) -> ConnectorError {
    match error {
        PartitionEvolutionFailure::Sdk { stage, cause } => {
            let kind = match cause.kind() {
                ErrorKind::DataInvalid | ErrorKind::FeatureUnsupported => {
                    ConnectorErrorKind::Unsupported
                }
                _ => ConnectorErrorKind::Internal,
            };
            ConnectorError::from_domain(kind, UnrepresentableSpec { stage, cause, kind })
        }
        PartitionEvolutionFailure::UnchangedDefault => {
            unsupported("Iceberg SDK could not retain the desired partition identity")
        }
        PartitionEvolutionFailure::MissingCommittedSpec
        | PartitionEvolutionFailure::UnexpectedUpdates => ConnectorError::new(
            ConnectorErrorKind::Internal,
            "Iceberg SDK returned incomplete partition evolution",
        ),
    }
}

fn invalid(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::InvalidRequest, message)
}
fn unsupported(message: &'static str) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::Unsupported, message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::iceberg::TableUpdate;
    use crate::iceberg::spec::{
        NestedField, PrimitiveType, Schema, SortOrder, TableMetadataBuilder, Type,
        UnboundPartitionSpec, UnboundPartitionSpecBuilder,
    };
    use std::sync::Arc;

    struct Active;
    impl ConnectorOperationControl for Active {
        fn check_active(&self) -> Result<(), ConnectorError> {
            Ok(())
        }
    }
    fn field(source_id: i32, id: i32, name: &str, transform: Transform) -> UnboundPartitionField {
        UnboundPartitionField {
            source_id,
            field_id: Some(id),
            name: name.into(),
            transform,
        }
    }
    fn spec(fields: Vec<UnboundPartitionField>) -> UnboundPartitionSpec {
        UnboundPartitionSpecBuilder::new()
            .add_partition_fields(fields)
            .unwrap()
            .build()
    }
    fn metadata(version: FormatVersion, fields: Vec<UnboundPartitionField>) -> TableMetadata {
        let schema = Schema::builder()
            .with_fields(vec![
                Arc::new(NestedField::required(
                    1,
                    "id",
                    Type::Primitive(PrimitiveType::Long),
                )),
                Arc::new(NestedField::required(
                    2,
                    "city",
                    Type::Primitive(PrimitiveType::String),
                )),
                Arc::new(NestedField::required(
                    3,
                    "ts",
                    Type::Primitive(PrimitiveType::Timestamp),
                )),
            ])
            .build()
            .unwrap();
        TableMetadataBuilder::new(
            schema,
            spec(fields),
            SortOrder::unsorted_order(),
            "memory://warehouse/db/t".into(),
            version,
            HashMap::new(),
        )
        .unwrap()
        .build()
        .unwrap()
        .metadata
    }
    fn input(values: &[&str]) -> Vec<Option<String>> {
        values
            .iter()
            .map(|value| Some((*value).to_owned()))
            .collect()
    }
    fn prepared_fields(change: &PreparedChange) -> &[UnboundPartitionField] {
        let [
            TableUpdate::AddSpec { spec },
            TableUpdate::SetDefaultSpec { spec_id: -1 },
        ] = change.updates.as_slice()
        else {
            panic!("one fully frozen REST partition change");
        };
        assert!(spec.spec_id().is_some());
        spec.fields()
    }
    fn check_fence(original: &TableMetadata, change: &PreparedChange) {
        let [TableRequirement::DefaultSpecIdMatch { default_spec_id }] =
            change.requirements.as_slice()
        else {
            panic!("same existing partition fence");
        };
        assert_eq!(*default_spec_id, original.default_partition_spec_id());
    }
    #[test]
    fn same_spec_keeps_current_field_order_and_none_of_the_updates() {
        let original = metadata(
            FormatVersion::V2,
            vec![
                field(2, 1000, "city", Transform::Identity),
                field(1, 1001, "id_bucket_4", Transform::Bucket(4)),
            ],
        );
        let change = prepare_full_partitioning_change(
            &original,
            &input(&["bucket(ID, 4)", "\"city\""]),
            &Active,
        )
        .unwrap();
        check_fence(&original, &change);
        assert!(change.updates.is_empty());
        assert_eq!(
            original.default_partition_spec().fields()[1].name,
            "id_bucket_4"
        );
    }
    #[test]
    fn v2_and_v3_retain_current_slots_then_append_new_fields_and_keep_history() {
        for version in [FormatVersion::V2, FormatVersion::V3] {
            let original = metadata(
                version,
                vec![
                    field(2, 1000, "city", Transform::Identity),
                    field(1, 1001, "id_bucket", Transform::Bucket(4)),
                    field(3, 1002, "ts_day", Transform::Day),
                ],
            );
            let change = prepare_full_partitioning_change(
                &original,
                &input(&["day(ts)", "bucket(id, 8)", "city", "bucket(ts, 4)"]),
                &Active,
            )
            .unwrap();
            check_fence(&original, &change);
            assert_eq!(
                prepared_fields(&change),
                &[
                    field(2, 1000, "city", Transform::Identity),
                    field(3, 1002, "ts_day", Transform::Day),
                    field(1, 1003, "id_bucket_8", Transform::Bucket(8)),
                    field(3, 1004, "ts_bucket", Transform::Bucket(4)),
                ]
            );
            let mut author = TableMetadataBuilder::new_from_metadata(original.clone(), None);
            for update in change.updates {
                author = update.apply(author).unwrap();
            }
            let updated = author.build().unwrap().metadata;
            assert_eq!(
                updated
                    .partition_spec_by_id(original.default_partition_spec_id())
                    .unwrap(),
                original.default_partition_spec()
            );
            assert_eq!(updated.default_partition_spec().fields().len(), 4);
            let emptied = prepare_full_partitioning_change(&updated, &[], &Active).unwrap();
            check_fence(&updated, &emptied);
            assert!(prepared_fields(&emptied).is_empty());
        }
    }
    #[test]
    fn v1_preserves_removed_physical_slot_as_void_or_refuses_unrepresentable_identity() {
        let original = metadata(
            FormatVersion::V1,
            vec![
                field(1, 1000, "id_bucket", Transform::Bucket(4)),
                field(3, 1001, "ts_day", Transform::Day),
            ],
        );
        let change =
            prepare_full_partitioning_change(&original, &input(&["day(ts)", "city"]), &Active)
                .unwrap();
        check_fence(&original, &change);
        assert_eq!(
            prepared_fields(&change),
            &[
                field(1, 1000, "id_bucket", Transform::Void),
                field(3, 1001, "ts_day", Transform::Day),
                field(2, 1002, "city", Transform::Identity),
            ]
        );
        let identity = metadata(
            FormatVersion::V1,
            vec![field(1, 1000, "id", Transform::Identity)],
        );
        // Rust SDK rejects Void with a schema-column name. The real v1 slot
        // may not be dropped or renamed to manufacture a different identity.
        let error = prepare_full_partitioning_change(&identity, &[], &Active)
            .err()
            .unwrap();
        assert_eq!(error.kind(), ConnectorErrorKind::Unsupported);
        assert!(InvalidTableProperty::from_connector_error(&error).is_none());
        assert_eq!(
            identity.default_partition_spec().fields()[0].transform,
            Transform::Identity
        );
    }
    #[test]
    fn history_reuses_actual_named_id_and_refuses_two_ids_for_the_same_named_field() {
        let base = metadata(
            FormatVersion::V2,
            vec![field(1, 1000, "id_bucket", Transform::Bucket(4))],
        );
        let history = TableMetadataBuilder::new_from_metadata(base, None)
            .add_default_partition_spec(spec(vec![field(2, 1001, "city", Transform::Identity)]))
            .unwrap()
            .build()
            .unwrap()
            .metadata;
        let reused =
            prepare_full_partitioning_change(&history, &input(&["city", "bucket(id, 4)"]), &Active)
                .unwrap();
        check_fence(&history, &reused);
        assert_eq!(
            prepared_fields(&reused),
            &[
                field(2, 1001, "city", Transform::Identity),
                field(1, 1000, "id_bucket", Transform::Bucket(4)),
            ]
        );

        // These are native metadata edits with different layouts, so the SDK
        // records both supplied historical identities instead of deduplicating
        // them as a shape-compatible spec.
        let ambiguous = TableMetadataBuilder::new_from_metadata(history, None)
            .add_partition_spec(spec(vec![
                field(1, 1002, "id_bucket", Transform::Bucket(4)),
                field(3, 1003, "ts_day", Transform::Day),
            ]))
            .unwrap()
            .build()
            .unwrap()
            .metadata;
        let error = prepare_full_partitioning_change(
            &ambiguous,
            &input(&["city", "bucket(id, 4)"]),
            &Active,
        )
        .err()
        .unwrap();
        assert_eq!(error.kind(), ConnectorErrorKind::Unsupported);
        assert!(error.message().contains("multiple IDs"));
    }
    #[test]
    fn full_property_preserves_source_data_and_original_control_class() {
        let original = metadata(FormatVersion::V2, vec![]);
        let error = prepare_full_partitioning_change(&original, &input(&["absent", "?"]), &Active)
            .err()
            .unwrap();
        assert!(
            InvalidTableProperty::from_connector_error(&error)
                .unwrap()
                .source_error()
                .message()
                .contains("absent")
        );
        struct Stopped(ConnectorError);
        impl ConnectorOperationControl for Stopped {
            fn check_active(&self) -> Result<(), ConnectorError> {
                Err(self.0.clone())
            }
        }
        let stopped = ConnectorError::compile_interruption(
            novarocks_type_contract::CompileControlError::Cancelled,
        );
        let error =
            prepare_full_partitioning_change(&original, &input(&["id"]), &Stopped(stopped.clone()))
                .err()
                .unwrap();
        assert_eq!(error, stopped);
    }
}
