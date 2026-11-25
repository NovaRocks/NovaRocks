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

//! Provider-owned Iceberg change-window facts and manifest planning.

// Design: ADR-0271 (docs/adr/ADR-0271-iceberg-change-window-facts-and-complete-enumeration.md)

use std::collections::BTreeMap;
use std::num::NonZeroU32;
use std::sync::Arc;

use bytes::Bytes;
use novarocks_spi::connector::{
    ConnectorChangePartition, ConnectorChangePartitionField, ConnectorChangePartitionTransform,
    ConnectorChangePartitionValue, ConnectorChangeWindowFacts, ConnectorChangeWindowNegativeWeight,
    ConnectorChangeWindowNetZeroBasis, ConnectorChangeWindowNotIncrementalReason,
    ConnectorChangeWindowPartitionImpact, ConnectorChangeWindowPartitionImpactBuilder,
    ConnectorChangeWindowVolume, ConnectorError, ConnectorErrorKind, ConnectorRequestContext,
};

use crate::delete_semantics::{DeleteSet, PositionSource};
use crate::iceberg::spec::{
    FormatVersion, NestedField, PartitionSpec, Schema, Snapshot, Struct, TableMetadata, Type,
};
use crate::read_model::IcebergReadFile;
use crate::snapshot_history::{SnapshotDataEffect, SnapshotHistory};

#[derive(Debug)]
pub(crate) enum ChangeWindowPlan {
    Resolved(ConnectorChangeWindowFacts),
    Endpoints { schema: Arc<Schema> },
}

/// Pure lineage and the one original upper-endpoint schema author.
pub(crate) fn prepare_change_window(
    metadata: &TableMetadata,
    from_exclusive: i64,
    to_inclusive: i64,
    context: &ConnectorRequestContext,
) -> Result<ChangeWindowPlan, ConnectorError> {
    check_active(context)?;
    if !matches!(
        metadata.format_version(),
        FormatVersion::V2 | FormatVersion::V3
    ) {
        return Err(unsupported(
            "Iceberg change-window scans require table format v2 or v3",
        ));
    }
    classify_lineage(metadata, from_exclusive, to_inclusive, context)
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

pub(crate) fn same_immutable_data_facts(a: &IcebergReadFile, b: &IcebergReadFile) -> bool {
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

/// Admission and execution compare the unpruned logical delete facts.
pub(crate) fn delete_closure_changed(from: &IcebergReadFile, to: &IcebergReadFile) -> bool {
    let before = from.logical_delete_set();
    let after = to.logical_delete_set();
    !before.same_addresses(after) || !before.same_applications(after)
}

fn index_endpoint_files<'a>(
    files: &'a [IcebergReadFile],
    context: &ConnectorRequestContext,
) -> Result<BTreeMap<&'a str, &'a IcebergReadFile>, ConnectorError> {
    let mut indexed = BTreeMap::new();
    for file in files {
        check_active(context)?;
        if indexed.insert(file.path.as_str(), file).is_some() {
            return Err(corrupt(format!(
                "Iceberg endpoint contains duplicate data file {}",
                file.path
            )));
        }
    }
    Ok(indexed)
}

/// Compare original endpoints and collect their fixed facts and optional impact
/// independently. A full impact budget cannot stop the comparison.
pub(crate) fn finish_change_window(
    metadata: &TableMetadata,
    from: &[IcebergReadFile],
    to: &[IcebergReadFile],
    context: &ConnectorRequestContext,
) -> Result<ConnectorChangeWindowFacts, ConnectorError> {
    check_active(context)?;
    let from = index_endpoint_files(from, context)?;
    let to = index_endpoint_files(to, context)?;
    let unpartitioned = metadata
        .partition_specs_iter()
        .all(|spec| spec.is_unpartitioned());
    let mut impact =
        (!unpartitioned).then(|| ConnectorChangeWindowPartitionImpactBuilder::new(false, context));
    let mut has_difference = false;
    let mut negative_weight = ConnectorChangeWindowNegativeWeight::Absent;
    let mut volume = ConnectorChangeWindowVolume {
        source_event_rows_upper_bound: 0,
        source_data_file_bytes: 0,
    };

    for (path, file) in &to {
        check_active(context)?;
        if let Some(previous) = from.get(path) {
            if !same_immutable_data_facts(previous, file) {
                return Err(corrupt(format!(
                    "Iceberg data file {path} changes immutable facts across endpoints"
                )));
            }
            if delete_closure_changed(previous, file) {
                has_difference = true;
                negative_weight = ConnectorChangeWindowNegativeWeight::Possible;
                file_partition(metadata, file)?;
                let record_count = required_record_count(file)?;
                let before = dv_cardinality(previous, record_count)?;
                let after = dv_cardinality(file, record_count)?;
                let rows = match (before, after) {
                    (Some(before), Some(after)) => after.checked_sub(before).ok_or_else(|| {
                        corrupt(format!(
                            "Iceberg deletion vector cardinality decreased for data file {path}"
                        ))
                    })?,
                    _ => record_count,
                };
                add_source_volume(&mut volume, file, rows)?;
                if let Some(builder) = &mut impact {
                    builder.mark_row_deletes();
                }
            }
        } else {
            has_difference = true;
            let record_count = required_record_count(file)?;
            add_source_volume(&mut volume, file, visible_rows(file, record_count)?)?;
            collect_partition(
                &mut impact,
                metadata,
                file,
                ConnectorChangeWindowPartitionImpactBuilder::add,
            )?;
        }
    }
    for (path, file) in &from {
        check_active(context)?;
        if !to.contains_key(path) {
            has_difference = true;
            negative_weight = ConnectorChangeWindowNegativeWeight::Possible;
            let record_count = required_record_count(file)?;
            add_source_volume(&mut volume, file, visible_rows(file, record_count)?)?;
            collect_partition(
                &mut impact,
                metadata,
                file,
                ConnectorChangeWindowPartitionImpactBuilder::remove,
            )?;
        }
    }
    if !has_difference {
        return Ok(ConnectorChangeWindowFacts::NetZero(
            ConnectorChangeWindowNetZeroBasis::EqualEndpoints,
        ));
    }
    let partition_impact = if unpartitioned {
        ConnectorChangeWindowPartitionImpact::Unpartitioned
    } else {
        impact.map_or(
            ConnectorChangeWindowPartitionImpact::Unavailable,
            ConnectorChangeWindowPartitionImpactBuilder::finish,
        )
    };
    Ok(ConnectorChangeWindowFacts::Difference {
        negative_weight,
        partition_impact,
        volume,
    })
}

fn required_record_count(file: &IcebergReadFile) -> Result<u64, ConnectorError> {
    file.record_count
        .and_then(|count| u64::try_from(count).ok())
        .ok_or_else(|| {
            corrupt(format!(
                "Iceberg data file {} is missing a non-negative record count",
                file.path
            ))
        })
}

fn add_source_volume(
    volume: &mut ConnectorChangeWindowVolume,
    file: &IcebergReadFile,
    rows: u64,
) -> Result<(), ConnectorError> {
    let size = u128::try_from(file.size).map_err(|_| {
        corrupt(format!(
            "Iceberg data file {} has a negative size",
            file.path
        ))
    })?;
    volume.source_event_rows_upper_bound = volume
        .source_event_rows_upper_bound
        .checked_add(u128::from(rows))
        .ok_or_else(|| corrupt("Iceberg change-window row count overflow"))?;
    volume.source_data_file_bytes = volume
        .source_data_file_bytes
        .checked_add(size)
        .ok_or_else(|| corrupt("Iceberg change-window data file size overflow"))?;
    Ok(())
}

fn visible_rows(file: &IcebergReadFile, record_count: u64) -> Result<u64, ConnectorError> {
    Ok(record_count - dv_cardinality(file, record_count)?.unwrap_or(0))
}

fn dv_cardinality(
    file: &IcebergReadFile,
    record_count: u64,
) -> Result<Option<u64>, ConnectorError> {
    let deletes: &DeleteSet = file.logical_delete_set();
    if !deletes.equality().is_empty() {
        return Ok(None);
    }
    let count = match deletes.position() {
        PositionSource::None => 0,
        PositionSource::Files(_) => return Ok(None),
        PositionSource::OneDv(dv) => dv.read().record_count,
    };
    if count > record_count {
        return Err(corrupt(format!(
            "Iceberg deletion vector cardinality exceeds the record count of data file {}",
            file.path
        )));
    }
    Ok(Some(count))
}

fn classify_lineage(
    metadata: &TableMetadata,
    from_exclusive: i64,
    to_inclusive: i64,
    context: &ConnectorRequestContext,
) -> Result<ChangeWindowPlan, ConnectorError> {
    if from_exclusive == to_inclusive {
        return Ok(ChangeWindowPlan::Resolved(
            ConnectorChangeWindowFacts::NetZero(ConnectorChangeWindowNetZeroBasis::SameSnapshot),
        ));
    }
    if metadata.snapshot_by_id(to_inclusive).is_none() {
        return Err(corrupt(format!(
            "Iceberg change-window upper snapshot {to_inclusive} is missing from metadata"
        )));
    }
    let broken = || {
        ChangeWindowPlan::Resolved(ConnectorChangeWindowFacts::NotIncremental(
            ConnectorChangeWindowNotIncrementalReason::SnapshotAncestryBroken {
                from_snapshot_id: from_exclusive,
            },
        ))
    };
    let Some(from) = metadata.snapshot_by_id(from_exclusive) else {
        return Ok(broken());
    };

    let mut history = SnapshotHistory::new(metadata, to_inclusive);
    let mut previous = None;
    let mut schema_pairs = Vec::new();
    let mut changed = false;
    for _ in 0..metadata.snapshots().len() {
        check_active(context)?;
        let Some(step) = history.next() else {
            break;
        };
        changed |= step.data_effect != SnapshotDataEffect::Preserves;
        if let Some(child) = previous {
            remember_schema_pair(&mut schema_pairs, child, step.snapshot);
        }
        if history.next_id() == Some(from_exclusive) {
            if !changed {
                return Ok(ChangeWindowPlan::Resolved(
                    ConnectorChangeWindowFacts::NetZero(
                        ConnectorChangeWindowNetZeroBasis::ReplaceOnly,
                    ),
                ));
            }
            remember_schema_pair(&mut schema_pairs, step.snapshot, from);
            return classify_proven_change(metadata, to_inclusive, schema_pairs, context);
        }
        previous = Some(step.snapshot);
    }
    Ok(broken())
}

fn remember_schema_pair<'a>(
    pairs: &mut Vec<(&'a Snapshot, &'a Snapshot)>,
    child: &'a Snapshot,
    parent: &'a Snapshot,
) {
    if child.schema_id() != parent.schema_id() {
        pairs.push((child, parent));
    }
}

fn classify_proven_change(
    metadata: &TableMetadata,
    to: i64,
    schema_pairs: Vec<(&Snapshot, &Snapshot)>,
    context: &ConnectorRequestContext,
) -> Result<ChangeWindowPlan, ConnectorError> {
    for (child, parent) in schema_pairs {
        check_active(context)?;
        let resolve = |snapshot: &Snapshot| {
            snapshot.schema(metadata).map_err(|error| {
                corrupt(format!(
                    "resolve Iceberg snapshot {} schema: {error}",
                    snapshot.snapshot_id()
                ))
            })
        };
        if !schema_differs_only_by_field_names(resolve(parent)?.as_ref(), resolve(child)?.as_ref())
        {
            return Ok(ChangeWindowPlan::Resolved(
                ConnectorChangeWindowFacts::NotIncremental(
                    ConnectorChangeWindowNotIncrementalReason::SchemaChanged {
                        snapshot_id: child.snapshot_id(),
                    },
                ),
            ));
        }
    }
    Ok(ChangeWindowPlan::Endpoints {
        schema: resolved_window_schema(metadata, to)?,
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
        (Type::Variant(previous), Type::Variant(next)) => previous == next,
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

fn file_partition<'a>(
    metadata: &'a TableMetadata,
    file: &'a IcebergReadFile,
) -> Result<(&'a PartitionSpec, &'a Struct), ConnectorError> {
    let spec_id = file.partition_spec_id.ok_or_else(|| {
        corrupt(format!(
            "Iceberg data file {} is missing its partition spec id",
            file.path
        ))
    })?;
    let spec = metadata.partition_spec_by_id(spec_id).ok_or_else(|| {
        corrupt(format!(
            "Iceberg table metadata is missing partition spec id {spec_id}"
        ))
    })?;
    let values = file.partition_values.as_ref().ok_or_else(|| {
        corrupt(format!(
            "Iceberg data file {} is missing its partition values",
            file.path
        ))
    })?;
    if values.fields().len() != spec.fields().len() {
        return Err(corrupt(format!(
            "Iceberg data file {} has the wrong partition field count for spec id {spec_id}",
            file.path
        )));
    }
    Ok((spec, values))
}

fn collect_partition(
    impact: &mut Option<ConnectorChangeWindowPartitionImpactBuilder>,
    metadata: &TableMetadata,
    file: &IcebergReadFile,
    retain: impl FnOnce(&mut ConnectorChangeWindowPartitionImpactBuilder, ConnectorChangePartition),
) -> Result<(), ConnectorError> {
    let (spec, values) = file_partition(metadata, file)?;
    let Some(builder) = impact.as_mut().filter(|builder| builder.is_available()) else {
        return Ok(());
    };
    let fields = change_partition_field_values(metadata, spec, values);
    if let Some(partition) = connector_partition(Some(spec.spec_id()), &fields)? {
        retain(builder, partition);
    } else {
        *impact = None;
    }
    Ok(())
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
    metadata: &TableMetadata,
    spec: &PartitionSpec,
    partition: &Struct,
) -> Vec<ChangePartitionFieldValue> {
    let schema = metadata.current_schema();
    spec.fields()
        .iter()
        .zip(partition.fields())
        .map(|(field, literal)| ChangePartitionFieldValue {
            source_field_id: field.source_id,
            source_column: schema
                .field_by_id(field.source_id)
                .map(|source| source.name.clone()),
            field_name: field.name.clone(),
            transform: change_partition_transform_name(&field.transform),
            value: change_partition_value(literal.as_ref()),
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

#[cfg(test)]
mod tests;
