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

//! Provider-owned Iceberg change-window admission and manifest planning.

use std::collections::BTreeMap;
use std::num::NonZeroU32;
use std::sync::Arc;

use bytes::Bytes;
use novarocks_spi::connector::{
    ConnectorChangePartition, ConnectorChangePartitionField, ConnectorChangePartitionTransform,
    ConnectorChangePartitionValue, ConnectorChangeWindowAdmission,
    ConnectorChangeWindowFullRebuildReason, ConnectorChangeWindowPartitionImpact,
    ConnectorChangeWindowReplaceFailure, ConnectorError, ConnectorErrorKind,
    ConnectorOperationControl, ConnectorRequestContext,
};

use crate::iceberg::spec::{
    FormatVersion, NestedField, Operation, Schema, Snapshot, TableMetadata, Type,
};
use crate::iceberg::table::Table;
use crate::resources::IcebergCatalogRuntime;

#[derive(Debug)]
enum LineageAdmission {
    MetadataOnly,
    Incremental,
    FullRebuild(ConnectorChangeWindowFullRebuildReason),
}

/// Plans one exact Iceberg snapshot window without leaking manifests, file
/// paths, or numeric Iceberg field identities into SPI admission facts.
pub(crate) fn plan_change_window(
    table: &Table,
    from_exclusive: i64,
    to_inclusive: i64,
    runtime: &IcebergCatalogRuntime,
    context: &ConnectorRequestContext,
) -> Result<ConnectorChangeWindowAdmission, ConnectorError> {
    check_active(context)?;
    let metadata = table.metadata();
    if !matches!(
        metadata.format_version(),
        FormatVersion::V2 | FormatVersion::V3
    ) {
        return Err(unsupported(
            "Iceberg change-window scans require table format v2 or v3",
        ));
    }
    match classify_lineage(metadata, from_exclusive, to_inclusive)? {
        LineageAdmission::MetadataOnly => return Ok(ConnectorChangeWindowAdmission::MetadataOnly),
        LineageAdmission::FullRebuild(reason) => {
            return Ok(ConnectorChangeWindowAdmission::FullRebuild(reason));
        }
        LineageAdmission::Incremental => {}
    }
    // Admission and execution observe the same complete endpoint semantics.
    // No manifest event or newly introduced delete artifact is a row delta.
    let schema = resolved_window_schema(metadata, to_inclusive)?;
    let from = build_snapshot_controlled(table, from_exclusive, &schema, runtime, context)?;
    let to = build_snapshot_controlled(table, to_inclusive, &schema, runtime, context)?;
    endpoint_admission(metadata, &from.files, &to.files, context)
}

/// Admission and execution use the same pinned upper-endpoint interpretation.
/// Later table schema changes cannot reinterpret an already selected window.
pub(crate) fn resolved_window_schema(
    metadata: &TableMetadata,
    to: i64,
) -> Result<Arc<Schema>, ConnectorError> {
    metadata
        .snapshot_by_id(to)
        .ok_or_else(|| corrupt("Iceberg change-window To snapshot is absent"))?
        .schema(metadata)
        .map_err(|e| corrupt(e.to_string()))
}

pub(crate) fn same_immutable_data_facts(
    a: &crate::read_model::IcebergReadFile,
    b: &crate::read_model::IcebergReadFile,
) -> bool {
    a.path == b.path
        && a.size == b.size
        && a.record_count == b.record_count
        && a.data_sequence_number == b.data_sequence_number
        && a.first_row_id == b.first_row_id
        && a.partition_spec_id == b.partition_spec_id
        && a.partition_values == b.partition_values
        && a.manifest.file_format == b.manifest.file_format
        && a.manifest.key_metadata == b.manifest.key_metadata
}

fn index_endpoint_files(
    files: &[crate::read_model::IcebergReadFile],
) -> Result<BTreeMap<&str, &crate::read_model::IcebergReadFile>, ConnectorError> {
    let mut indexed = BTreeMap::new();
    for file in files {
        if indexed.insert(file.path.as_str(), file).is_some() {
            return Err(corrupt(format!(
                "Iceberg endpoint contains duplicate data file {}",
                file.path
            )));
        }
    }
    Ok(indexed)
}

fn endpoint_admission(
    metadata: &TableMetadata,
    from: &[crate::read_model::IcebergReadFile],
    to: &[crate::read_model::IcebergReadFile],
    context: &ConnectorRequestContext,
) -> Result<ConnectorChangeWindowAdmission, ConnectorError> {
    let from = index_endpoint_files(from)?;
    let to = index_endpoint_files(to)?;
    let mut added = Vec::new();
    let mut removed = Vec::new();
    let mut row_deletes = false;
    for (path, file) in &to {
        if let Some(previous) = from.get(path) {
            if !same_immutable_data_facts(previous, file) {
                return Err(corrupt(format!(
                    "Iceberg data file {path} changes immutable facts across endpoints"
                )));
            }
            let before = previous.logical_delete_set();
            let after = file.logical_delete_set();
            row_deletes |= !before.same_addresses(after) || !before.same_applications(after);
        } else {
            added.push(*file);
        }
    }
    for (path, file) in &from {
        if !to.contains_key(path) {
            removed.push(*file);
        }
    }
    if added.is_empty() && removed.is_empty() && !row_deletes {
        return Ok(ConnectorChangeWindowAdmission::MetadataOnly);
    }
    Ok(ConnectorChangeWindowAdmission::Incremental {
        has_inserts: !added.is_empty(),
        has_deletes: !removed.is_empty() || row_deletes,
        partition_impact: endpoint_partition_impact(
            metadata,
            &added,
            &removed,
            row_deletes,
            context,
        )?,
    })
}

fn build_snapshot_controlled(
    table: &Table,
    snapshot_id: i64,
    schema: &Schema,
    runtime: &IcebergCatalogRuntime,
    context: &ConnectorRequestContext,
) -> Result<crate::read_model::IcebergReadSnapshot, ConnectorError> {
    check_active(context)?;
    let table = table.clone();
    let control = context.clone();
    let domain = crate::read_snapshot::mint_read_domain(table.metadata(), snapshot_id, schema)
        .map_err(corrupt)?;
    let result = runtime.block_on(async move {
        crate::read_snapshot::build_read_snapshot_in_domain(
            &table,
            domain,
            Some(&control as &dyn ConnectorOperationControl),
        )
        .await
    });
    check_active(context)?;
    result.map_err(unavailable)?.map_err(unavailable)
}

fn classify_lineage(
    metadata: &TableMetadata,
    from_exclusive: i64,
    to_inclusive: i64,
) -> Result<LineageAdmission, ConnectorError> {
    if from_exclusive == to_inclusive {
        return Ok(LineageAdmission::MetadataOnly);
    }
    let Some(mut current) = metadata.snapshot_by_id(to_inclusive) else {
        return Err(corrupt(format!(
            "Iceberg change-window upper snapshot {to_inclusive} is missing from metadata"
        )));
    };
    if metadata.snapshot_by_id(from_exclusive).is_none() {
        return Ok(LineageAdmission::FullRebuild(
            ConnectorChangeWindowFullRebuildReason::LineageBroken {
                from_snapshot_id: from_exclusive,
            },
        ));
    }

    let mut changed = false;
    loop {
        let snapshot = current.as_ref();
        let parent_id = snapshot.parent_snapshot_id();
        let parent = parent_id
            .and_then(|id| metadata.snapshot_by_id(id))
            .map(|value| value.as_ref());
        match classify_snapshot(snapshot, parent)? {
            SnapshotDecision::Changed => changed = true,
            SnapshotDecision::MetadataOnly => {}
            SnapshotDecision::FullRebuild(reason) => {
                return Ok(LineageAdmission::FullRebuild(reason));
            }
        }
        if !matches!(snapshot.summary().operation, Operation::Replace)
            && let Some(parent) = parent
            && snapshot.schema_id() != parent.schema_id()
        {
            let previous_schema = parent.schema(metadata).map_err(|error| {
                corrupt(format!(
                    "resolve Iceberg snapshot {} schema: {error}",
                    parent.snapshot_id()
                ))
            })?;
            let next_schema = snapshot.schema(metadata).map_err(|error| {
                corrupt(format!(
                    "resolve Iceberg snapshot {} schema: {error}",
                    snapshot.snapshot_id()
                ))
            })?;
            if !schema_differs_only_by_field_names(&previous_schema, &next_schema) {
                return Ok(LineageAdmission::FullRebuild(
                    ConnectorChangeWindowFullRebuildReason::UnprovenReplace {
                        snapshot_id: snapshot.snapshot_id(),
                        failure: ConnectorChangeWindowReplaceFailure::SchemaChanged,
                    },
                ));
            }
        }
        match parent_id {
            Some(id) if id == from_exclusive => break,
            Some(id) => {
                let Some(parent) = metadata.snapshot_by_id(id) else {
                    return Ok(LineageAdmission::FullRebuild(
                        ConnectorChangeWindowFullRebuildReason::LineageBroken {
                            from_snapshot_id: from_exclusive,
                        },
                    ));
                };
                current = parent;
            }
            None => {
                return Ok(LineageAdmission::FullRebuild(
                    ConnectorChangeWindowFullRebuildReason::LineageBroken {
                        from_snapshot_id: from_exclusive,
                    },
                ));
            }
        }
    }
    Ok(if !changed {
        LineageAdmission::MetadataOnly
    } else {
        LineageAdmission::Incremental
    })
}

fn schema_differs_only_by_field_names(previous: &Schema, next: &Schema) -> bool {
    previous
        .identifier_field_ids()
        .collect::<std::collections::BTreeSet<_>>()
        == next
            .identifier_field_ids()
            .collect::<std::collections::BTreeSet<_>>()
        && fields_differ_only_by_names(previous.as_struct().fields(), next.as_struct().fields())
}

fn fields_differ_only_by_names(previous: &[Arc<NestedField>], next: &[Arc<NestedField>]) -> bool {
    previous.len() == next.len()
        && previous
            .iter()
            .zip(next)
            .all(|(previous, next)| field_differs_only_by_name(previous, next))
}

fn field_differs_only_by_name(previous: &NestedField, next: &NestedField) -> bool {
    previous.id == next.id
        && previous.required == next.required
        && previous.doc == next.doc
        && previous.initial_default == next.initial_default
        && previous.write_default == next.write_default
        && type_differs_only_by_field_names(&previous.field_type, &next.field_type)
}

fn type_differs_only_by_field_names(previous: &Type, next: &Type) -> bool {
    match (previous, next) {
        (Type::Primitive(previous), Type::Primitive(next)) => previous == next,
        (Type::Struct(previous), Type::Struct(next)) => {
            fields_differ_only_by_names(previous.fields(), next.fields())
        }
        (Type::List(previous), Type::List(next)) => {
            field_differs_only_by_name(&previous.element_field, &next.element_field)
        }
        (Type::Map(previous), Type::Map(next)) => {
            field_differs_only_by_name(&previous.key_field, &next.key_field)
                && field_differs_only_by_name(&previous.value_field, &next.value_field)
        }
        _ => false,
    }
}

enum SnapshotDecision {
    Changed,
    MetadataOnly,
    FullRebuild(ConnectorChangeWindowFullRebuildReason),
}

fn classify_snapshot(
    snapshot: &Snapshot,
    parent: Option<&Snapshot>,
) -> Result<SnapshotDecision, ConnectorError> {
    let snapshot_id = snapshot.snapshot_id();
    Ok(match snapshot.summary().operation {
        Operation::Append | Operation::Delete | Operation::Overwrite => SnapshotDecision::Changed,
        Operation::Replace => {
            let Some(parent) = parent else {
                return Ok(SnapshotDecision::FullRebuild(unproven_replace(
                    snapshot_id,
                    ConnectorChangeWindowReplaceFailure::MissingParent,
                )));
            };
            if let Some(failure) = validate_replace_snapshot(snapshot, parent)? {
                SnapshotDecision::FullRebuild(unproven_replace(snapshot_id, failure))
            } else {
                SnapshotDecision::MetadataOnly
            }
        }
    })
}

fn validate_replace_snapshot(
    snapshot: &Snapshot,
    parent: &Snapshot,
) -> Result<Option<ConnectorChangeWindowReplaceFailure>, ConnectorError> {
    let summary = &snapshot.summary().additional_properties;
    let parent_summary = &parent.summary().additional_properties;
    let records = parse_summary_i64(summary.get("total-records"))?;
    let parent_records = parse_summary_i64(parent_summary.get("total-records"))?;
    let (Some(records), Some(parent_records)) = (records, parent_records) else {
        return Ok(Some(
            ConnectorChangeWindowReplaceFailure::MissingOrInvalidSummary,
        ));
    };
    if records != parent_records {
        return Ok(Some(
            ConnectorChangeWindowReplaceFailure::RecordCountChanged,
        ));
    }
    let Some(added) = parse_summary_i64(summary.get("added-data-files"))? else {
        return Ok(Some(
            ConnectorChangeWindowReplaceFailure::MissingOrInvalidSummary,
        ));
    };
    let Some(removed) = parse_summary_i64(summary.get("deleted-data-files"))? else {
        return Ok(Some(
            ConnectorChangeWindowReplaceFailure::MissingOrInvalidSummary,
        ));
    };
    if added < 0 || removed < 0 {
        return Ok(Some(
            ConnectorChangeWindowReplaceFailure::InvalidDataFileCounts,
        ));
    }
    let valid = (added > 0 && removed > 0)
        || (added == 0 && removed == 0)
        || (records == 0 && added == 0 && removed > 0);
    if !valid {
        return Ok(Some(
            ConnectorChangeWindowReplaceFailure::InvalidDataFileCounts,
        ));
    }
    if snapshot.schema_id() != parent.schema_id() {
        return Ok(Some(ConnectorChangeWindowReplaceFailure::SchemaChanged));
    }
    Ok(None)
}

fn parse_summary_i64(value: Option<&String>) -> Result<Option<i64>, ConnectorError> {
    let Some(value) = value else {
        return Ok(None);
    };
    Ok(value.parse::<i64>().ok())
}

fn unproven_replace(
    snapshot_id: i64,
    failure: ConnectorChangeWindowReplaceFailure,
) -> ConnectorChangeWindowFullRebuildReason {
    ConnectorChangeWindowFullRebuildReason::UnprovenReplace {
        snapshot_id,
        failure,
    }
}

fn endpoint_partition_impact(
    metadata: &TableMetadata,
    added: &[&crate::read_model::IcebergReadFile],
    removed: &[&crate::read_model::IcebergReadFile],
    row_deletes: bool,
    context: &ConnectorRequestContext,
) -> Result<ConnectorChangeWindowPartitionImpact, ConnectorError> {
    if metadata
        .partition_specs_iter()
        .all(|spec| spec.is_unpartitioned())
    {
        return Ok(ConnectorChangeWindowPartitionImpact::Unpartitioned);
    }
    let project = |file: &&crate::read_model::IcebergReadFile| {
        let (Some(spec), Some(values)) = (file.partition_spec_id, file.partition_values.as_ref())
        else {
            return Ok(None);
        };
        connector_partition(
            Some(spec),
            &change_partition_field_values(metadata, spec, values)?,
        )
    };
    let added = added
        .iter()
        .map(project)
        .collect::<Result<Option<Vec<_>>, _>>()?;
    let removed = removed
        .iter()
        .map(project)
        .collect::<Result<Option<Vec<_>>, _>>()?;
    match (added, removed) {
        (Some(added), Some(removed)) => {
            ConnectorChangeWindowPartitionImpact::try_exact(row_deletes, added, removed, context)
        }
        _ => Ok(ConnectorChangeWindowPartitionImpact::Unavailable),
    }
}

fn connector_partition(
    partition_spec_id: Option<i32>,
    values: &[ChangePartitionFieldValue],
) -> Result<Option<ConnectorChangePartition>, ConnectorError> {
    let Some(partition_spec_id) = partition_spec_id else {
        return Ok(None);
    };
    if values.is_empty() {
        return Ok(None);
    }
    let mut fields = Vec::with_capacity(values.len());
    for partition_value in values {
        let Some(source_column) = partition_value.source_column.as_deref() else {
            return Ok(None);
        };
        let Some(transform) = connector_transform(&partition_value.transform) else {
            return Ok(None);
        };
        let value = match &partition_value.value {
            ChangePartitionValue::Null => ConnectorChangePartitionValue::Null,
            ChangePartitionValue::Primitive(value) => {
                ConnectorChangePartitionValue::String(Arc::from(value.as_str()))
            }
            ChangePartitionValue::Unsupported(_) => return Ok(None),
        };
        fields.push(ConnectorChangePartitionField::try_new(
            Bytes::copy_from_slice(&partition_value.source_field_id.to_be_bytes()),
            source_column,
            transform,
            value,
        )?);
    }
    ConnectorChangePartition::try_new(
        crate::storage_inspector::exact_partition_spec_version(partition_spec_id),
        fields,
    )
    .map(Some)
}

fn connector_transform(value: &str) -> Option<ConnectorChangePartitionTransform> {
    match value.to_ascii_lowercase().as_str() {
        "identity" => Some(ConnectorChangePartitionTransform::Identity),
        "year" => Some(ConnectorChangePartitionTransform::Year),
        "month" => Some(ConnectorChangePartitionTransform::Month),
        "day" => Some(ConnectorChangePartitionTransform::Day),
        "hour" => Some(ConnectorChangePartitionTransform::Hour),
        value if value.starts_with("bucket(") && value.ends_with(')') => value[7..value.len() - 1]
            .parse::<u32>()
            .ok()
            .and_then(NonZeroU32::new)
            .map(|buckets| ConnectorChangePartitionTransform::Bucket { buckets }),
        value if value.starts_with("truncate(") && value.ends_with(')') => value
            [9..value.len() - 1]
            .parse::<u32>()
            .ok()
            .and_then(NonZeroU32::new)
            .map(|width| ConnectorChangePartitionTransform::Truncate { width }),
        _ => None,
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct ChangePartitionFieldValue {
    source_field_id: i32,
    source_column: Option<String>,
    field_name: String,
    transform: String,
    value: ChangePartitionValue,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum ChangePartitionValue {
    Null,
    Primitive(String),
    Unsupported(String),
}

fn change_partition_field_values(
    metadata: &crate::iceberg::spec::TableMetadata,
    spec_id: i32,
    partition: &crate::iceberg::spec::Struct,
) -> Result<Vec<ChangePartitionFieldValue>, ConnectorError> {
    let Some(spec) = metadata.partition_spec_by_id(spec_id) else {
        return Err(corrupt(format!(
            "iceberg table metadata missing partition spec id {spec_id}"
        )));
    };
    let schema = metadata.current_schema();
    spec.fields()
        .iter()
        .enumerate()
        .map(|(idx, field)| {
            let literal = partition.fields().get(idx).ok_or_else(|| {
                corrupt(format!(
                    "iceberg partition struct for spec id {spec_id} is missing field {} at index {idx}",
                    field.name
                ))
            })?;
            Ok(ChangePartitionFieldValue {
                source_field_id: field.source_id,
                source_column: schema.field_by_id(field.source_id).map(|source| source.name.clone()),
                field_name: field.name.clone(),
                transform: change_partition_transform_name(&field.transform),
                value: change_partition_value(literal.as_ref()),
            })
        })
        .collect()
}

fn change_partition_transform_name(transform: &crate::iceberg::spec::Transform) -> String {
    match transform {
        crate::iceberg::spec::Transform::Identity => "identity".to_string(),
        other => format!("{other:?}").to_ascii_lowercase(),
    }
}

fn change_partition_value(literal: Option<&crate::iceberg::spec::Literal>) -> ChangePartitionValue {
    let Some(crate::iceberg::spec::Literal::Primitive(value)) = literal else {
        return match literal {
            None => ChangePartitionValue::Null,
            Some(_) => {
                ChangePartitionValue::Unsupported("non-primitive partition value".to_string())
            }
        };
    };
    let value = match value {
        crate::iceberg::spec::PrimitiveLiteral::Boolean(v) => v.to_string(),
        crate::iceberg::spec::PrimitiveLiteral::Int(v) => v.to_string(),
        crate::iceberg::spec::PrimitiveLiteral::Long(v) => v.to_string(),
        crate::iceberg::spec::PrimitiveLiteral::Float(v) => v.0.to_string(),
        crate::iceberg::spec::PrimitiveLiteral::Double(v) => v.0.to_string(),
        crate::iceberg::spec::PrimitiveLiteral::String(v) => {
            return ChangePartitionValue::Primitive(v.clone());
        }
        crate::iceberg::spec::PrimitiveLiteral::Binary(_) => {
            return ChangePartitionValue::Unsupported("binary partition value".to_string());
        }
        crate::iceberg::spec::PrimitiveLiteral::Int128(_) => {
            return ChangePartitionValue::Unsupported("int128 partition value".to_string());
        }
        crate::iceberg::spec::PrimitiveLiteral::UInt128(_) => {
            return ChangePartitionValue::Unsupported("uint128 partition value".to_string());
        }
        crate::iceberg::spec::PrimitiveLiteral::AboveMax => {
            return ChangePartitionValue::Unsupported("above-max partition value".to_string());
        }
        crate::iceberg::spec::PrimitiveLiteral::BelowMin => {
            return ChangePartitionValue::Unsupported("below-min partition value".to_string());
        }
    };
    ChangePartitionValue::Primitive(value)
}

fn check_active(context: &ConnectorRequestContext) -> Result<(), ConnectorError> {
    if context.is_cancelled() {
        return Err(ConnectorError::new(
            ConnectorErrorKind::Cancelled,
            "Iceberg change-window planning was cancelled",
        ));
    }
    if std::time::Instant::now() >= context.deadline() {
        return Err(ConnectorError::new(
            ConnectorErrorKind::DeadlineExceeded,
            "Iceberg change-window planning deadline elapsed",
        ));
    }
    Ok(())
}

fn corrupt(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::CorruptData, message)
}

fn unsupported(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::Unsupported, message)
}

fn unavailable(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::Unavailable, message).with_retryable_before_progress()
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use crate::iceberg::spec::{
        FormatVersion, NestedField, Operation, PartitionSpec, PrimitiveType, Schema, Snapshot,
        SortOrder, Summary, TableMetadata, TableMetadataBuilder, Type,
    };

    use super::*;

    fn snapshot(
        snapshot_id: i64,
        parent_snapshot_id: Option<i64>,
        operation: Operation,
        properties: &[(&str, &str)],
        schema_id: i32,
    ) -> Snapshot {
        Snapshot::builder()
            .with_snapshot_id(snapshot_id)
            .with_parent_snapshot_id(parent_snapshot_id)
            .with_sequence_number(snapshot_id)
            .with_timestamp_ms(1_700_000_000_000 + snapshot_id)
            .with_manifest_list(format!("file:///tmp/manifest-list-{snapshot_id}.avro"))
            .with_summary(Summary {
                operation,
                additional_properties: properties
                    .iter()
                    .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
                    .collect::<HashMap<_, _>>(),
            })
            .with_schema_id(schema_id)
            .build()
    }

    fn metadata_with_snapshots(snapshots: Vec<Snapshot>) -> TableMetadata {
        let schema = Schema::builder()
            .with_fields(vec![Arc::new(NestedField::required(
                1,
                "id",
                Type::Primitive(PrimitiveType::Long),
            ))])
            .build()
            .expect("schema");
        let mut builder = TableMetadataBuilder::new(
            schema,
            PartitionSpec::unpartition_spec(),
            SortOrder::unsorted_order(),
            "/tmp/change-window-test".to_string(),
            FormatVersion::V2,
            HashMap::new(),
        )
        .expect("metadata builder");
        for snapshot in snapshots {
            builder = builder.add_snapshot(snapshot).expect("add snapshot");
        }
        builder.build().expect("metadata").metadata
    }

    fn replace_failure(
        parent: Option<&Snapshot>,
        properties: &[(&str, &str)],
        schema_id: i32,
    ) -> ConnectorChangeWindowReplaceFailure {
        let replace = snapshot(
            2,
            parent.map(Snapshot::snapshot_id),
            Operation::Replace,
            properties,
            schema_id,
        );
        let SnapshotDecision::FullRebuild(
            ConnectorChangeWindowFullRebuildReason::UnprovenReplace { failure, .. },
        ) = classify_snapshot(&replace, parent).expect("replace admission")
        else {
            panic!("expected typed unproven REPLACE admission")
        };
        failure
    }

    #[test]
    fn replace_failures_are_typed_without_provider_reason_strings() {
        let parent = snapshot(1, None, Operation::Append, &[("total-records", "100")], 0);
        assert_eq!(
            replace_failure(None, &[("total-records", "100")], 0),
            ConnectorChangeWindowReplaceFailure::MissingParent
        );
        assert_eq!(
            replace_failure(
                Some(&parent),
                &[
                    ("total-records", "101"),
                    ("added-data-files", "1"),
                    ("deleted-data-files", "1"),
                ],
                0,
            ),
            ConnectorChangeWindowReplaceFailure::RecordCountChanged
        );
        assert_eq!(
            replace_failure(
                Some(&parent),
                &[("added-data-files", "1"), ("deleted-data-files", "1")],
                0,
            ),
            ConnectorChangeWindowReplaceFailure::MissingOrInvalidSummary
        );
        assert_eq!(
            replace_failure(
                Some(&parent),
                &[
                    ("total-records", "100"),
                    ("added-data-files", "0"),
                    ("deleted-data-files", "1"),
                ],
                0,
            ),
            ConnectorChangeWindowReplaceFailure::InvalidDataFileCounts
        );
        assert_eq!(
            replace_failure(
                Some(&parent),
                &[
                    ("total-records", "100"),
                    ("added-data-files", "1"),
                    ("deleted-data-files", "1"),
                ],
                7,
            ),
            ConnectorChangeWindowReplaceFailure::SchemaChanged
        );
    }

    #[test]
    fn valid_replace_is_metadata_only() {
        let parent = snapshot(1, None, Operation::Append, &[("total-records", "100")], 0);
        let replace = snapshot(
            2,
            Some(1),
            Operation::Replace,
            &[
                ("total-records", "100"),
                ("added-data-files", "3"),
                ("deleted-data-files", "2"),
            ],
            0,
        );
        assert!(matches!(
            classify_snapshot(&replace, Some(&parent)).expect("replace admission"),
            SnapshotDecision::MetadataOnly
        ));
    }

    #[test]
    fn partition_transform_projection_is_typed_and_bounded() {
        assert_eq!(
            connector_transform("bucket(16)"),
            Some(ConnectorChangePartitionTransform::Bucket {
                buckets: NonZeroU32::new(16).expect("nonzero")
            })
        );
        assert_eq!(connector_transform("bucket(0)"), None);
        assert_eq!(connector_transform("void"), None);
    }

    #[test]
    fn file_partition_impact_preserves_opaque_source_and_spec_identities() {
        let values = vec![ChangePartitionFieldValue {
            source_field_id: 17,
            source_column: Some("renamed_region".to_string()),
            field_name: "region_bucket".to_string(),
            transform: "bucket(8)".to_string(),
            value: ChangePartitionValue::Primitive("3".to_string()),
        }];
        let partition = connector_partition(Some(9), &values)
            .expect("provider partition projection")
            .expect("exact partition");
        assert_eq!(
            partition.partition_spec_identity(),
            &crate::storage_inspector::exact_partition_spec_version(9)
        );
        assert_eq!(
            partition.fields()[0].source_field_identity().as_ref(),
            &17_i32.to_be_bytes()
        );
        assert_eq!(partition.fields()[0].source_column(), "renamed_region");
        assert!(
            connector_partition(None, &values)
                .expect("missing spec identity remains unavailable")
                .is_none()
        );
    }

    #[test]
    fn equal_endpoints_are_metadata_only_without_ordering_snapshot_identities() {
        let metadata = metadata_with_snapshots(Vec::new());
        assert!(matches!(
            classify_lineage(&metadata, 41, 41).expect("equal endpoint admission"),
            LineageAdmission::MetadataOnly
        ));
    }

    #[test]
    fn missing_from_snapshot_is_a_typed_lineage_full_rebuild() {
        let current = snapshot(2, None, Operation::Append, &[], 0);
        let metadata = metadata_with_snapshots(vec![current]);
        assert!(matches!(
            classify_lineage(&metadata, 1, 2).expect("lineage admission"),
            LineageAdmission::FullRebuild(ConnectorChangeWindowFullRebuildReason::LineageBroken {
                from_snapshot_id: 1
            })
        ));
    }

    #[test]
    fn missing_upper_snapshot_remains_a_hard_corrupt_data_error() {
        let metadata = metadata_with_snapshots(Vec::new());
        let error = classify_lineage(&metadata, 1, 2).expect_err("missing upper snapshot");
        assert_eq!(error.kind(), ConnectorErrorKind::CorruptData);
    }

    #[test]
    fn field_id_preserving_rename_is_the_only_incremental_schema_change() {
        let previous = Schema::builder()
            .with_fields(vec![Arc::new(NestedField::optional(
                7,
                "region",
                Type::Primitive(PrimitiveType::String),
            ))])
            .build()
            .expect("previous schema");
        let renamed = Schema::builder()
            .with_fields(vec![Arc::new(NestedField::optional(
                7,
                "area",
                Type::Primitive(PrimitiveType::String),
            ))])
            .build()
            .expect("renamed schema");
        let widened = Schema::builder()
            .with_fields(vec![Arc::new(NestedField::optional(
                7,
                "area",
                Type::Primitive(PrimitiveType::Long),
            ))])
            .build()
            .expect("widened schema");

        assert!(schema_differs_only_by_field_names(&previous, &renamed));
        assert!(!schema_differs_only_by_field_names(&previous, &widened));
    }
    #[test]
    fn rename_only_identifier_sets_ignore_independent_hash_iteration_order() {
        let schema = |name: &str, ids: &[i32]| {
            Schema::builder()
                .with_fields(vec![
                    Arc::new(NestedField::required(
                        1,
                        name,
                        Type::Primitive(PrimitiveType::Long),
                    )),
                    Arc::new(NestedField::required(
                        2,
                        "second",
                        Type::Primitive(PrimitiveType::String),
                    )),
                ])
                .with_identifier_field_ids(ids.iter().copied())
                .build()
                .unwrap()
        };
        let original = schema("first", &[1, 2]);
        for _ in 0..64 {
            assert!(schema_differs_only_by_field_names(
                &original,
                &schema("renamed", &[2, 1])
            ));
        }
        assert!(!schema_differs_only_by_field_names(
            &original,
            &schema("renamed", &[1])
        ));
    }

    fn request_context() -> ConnectorRequestContext {
        ConnectorRequestContext::try_new(
            std::time::Instant::now() + std::time::Duration::from_secs(30),
            novarocks_spi::connector::ConnectorStopOwner::new().view(),
            256 * 1024,
            1024 * 1024,
        )
        .unwrap()
    }

    fn endpoint_file(
        metadata: &TableMetadata,
        snapshot: i64,
        delete_sequence: i64,
        prune: bool,
    ) -> crate::read_model::IcebergReadFile {
        use crate::delete_semantics::*;
        use crate::iceberg::spec::{DataFileFormat, Datum, Struct};
        let schema = metadata.current_schema();
        let spec = PartitionSpec::unpartition_spec();
        let partition = TypedPartition::bind(&spec, schema, &Struct::empty()).unwrap();
        let metrics = |lo, hi| {
            FileMetrics::new(BTreeMap::from([(
                1,
                FieldMetrics {
                    resolved_type: PrimitiveType::Long,
                    value_count: Some(10),
                    null_count: Some(0),
                    nan_count: None,
                    lower_bound: Some(Datum::long(lo)),
                    upper_bound: Some(Datum::long(hi)),
                },
            )]))
        };
        let data = DataFileFact::try_new(
            "data",
            DataSequenceNumber::try_new(1).unwrap(),
            partition.clone(),
            10,
            metrics(8, 9),
        )
        .unwrap();
        let fact = DeleteFact::try_new(DeleteFactParams {
            address: DeleteContentAddress::file("same.parquet").unwrap(),
            kind: DeleteKind::Equality(EqualityFieldGroup::bind(&[1], schema).unwrap()),
            sequence: DataSequenceNumber::try_new(delete_sequence).unwrap(),
            partition,
            read: DeleteReadFacts {
                format: DeleteFormat::Parquet,
                record_count: 10,
                file_size: 100,
                key_metadata: Arc::from([]),
            },
            metrics: metrics(1, 2),
        })
        .unwrap();
        let index = DeleteCandidateIndex::try_new(
            test_read_domain(schema, &[spec], snapshot),
            DeleteObservation::from_normalized_recall([Arc::new(fact)]).unwrap(),
        )
        .unwrap();
        crate::read_model::IcebergReadFile {
            path: "data".into(),
            size: 100,
            record_count: Some(10),
            column_stats: None,
            partition_spec_id: Some(0),
            partition_key: None,
            partition_values: Some(Struct::empty()),
            manifest_path: None,
            first_row_id: None,
            data_sequence_number: Some(1),
            manifest: Arc::new(crate::read_model::IcebergDataFileMetadata {
                file_format: DataFileFormat::Parquet,
                split_offsets: vec![],
                key_metadata: vec![],
                value_counts: HashMap::new(),
                null_value_counts: HashMap::new(),
                nan_value_counts: HashMap::new(),
                lower_bounds: HashMap::new(),
                upper_bounds: HashMap::new(),
            }),
            deletes: index.for_data(&data).unwrap().load_view(if prune {
                StatisticsPolicy::MetadataBudget {
                    max_candidate_members: 10,
                    max_field_comparisons: 10,
                }
            } else {
                StatisticsPolicy::Disabled
            }),
        }
    }

    #[test]
    fn admission_uses_logical_applications_and_ignores_optional_load_pruning() {
        let metadata = metadata_with_snapshots(vec![]);
        let from = endpoint_file(&metadata, 10, 2, false);
        let pruned = endpoint_file(&metadata, 20, 2, true);
        assert_eq!(from.deletes.member_count(), 1);
        assert_eq!(pruned.deletes.member_count(), 0);
        assert!(matches!(
            endpoint_admission(&metadata, &[from.clone()], &[pruned], &request_context()).unwrap(),
            ConnectorChangeWindowAdmission::MetadataOnly
        ));
        let changed = endpoint_file(&metadata, 20, 3, false);
        assert!(matches!(
            endpoint_admission(&metadata, &[from], &[changed], &request_context()).unwrap(),
            ConnectorChangeWindowAdmission::Incremental {
                has_inserts: false,
                has_deletes: true,
                ..
            }
        ));
    }

    #[test]
    fn admission_endpoint_membership_excludes_transient_files_and_checks_data_identity() {
        let metadata = metadata_with_snapshots(vec![]);
        let file = endpoint_file(&metadata, 10, 2, false);
        assert!(matches!(
            endpoint_admission(&metadata, &[], &[], &request_context()).unwrap(),
            ConnectorChangeWindowAdmission::MetadataOnly
        ));
        assert!(matches!(
            endpoint_admission(&metadata, &[], &[file.clone()], &request_context()).unwrap(),
            ConnectorChangeWindowAdmission::Incremental {
                has_inserts: true,
                has_deletes: false,
                ..
            }
        ));
        assert!(matches!(
            endpoint_admission(&metadata, &[file.clone()], &[], &request_context()).unwrap(),
            ConnectorChangeWindowAdmission::Incremental {
                has_inserts: false,
                has_deletes: true,
                ..
            }
        ));
        let mut mutated = file.clone();
        mutated.record_count = Some(11);
        assert_eq!(
            endpoint_admission(&metadata, &[file], &[mutated], &request_context())
                .unwrap_err()
                .kind(),
            ConnectorErrorKind::CorruptData
        );
    }
}
