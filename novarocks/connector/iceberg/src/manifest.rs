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

//! Provider-owned manifest facts used by statistics and mutation planning.

use std::collections::HashMap;
use std::sync::Arc;

use crate::iceberg::io::FileIO;
use crate::iceberg::spec::{
    DataContentType, DataFileFormat, Literal as IcebergLiteral, Manifest, ManifestContentType,
    ManifestFile, ManifestList, ManifestStatus, PrimitiveLiteral, Snapshot, TableMetadata,
    Transform,
};
use crate::iceberg::table::Table;
#[cfg(test)]
use crate::read_model::{IcebergReadDeleteFormat, IcebergReadDeleteKind};
use crate::scan_model::{
    IcebergColumnStats, IcebergDataFileInfo, IcebergDeleteFileContent, IcebergDeleteFileFormat,
    IcebergDeleteFileInfo, IcebergPartitionFieldValue, IcebergPartitionValue,
};

pub(crate) fn schema_contains_variant(schema: &crate::iceberg::spec::Schema) -> bool {
    fn contains(data_type: &crate::iceberg::spec::Type) -> bool {
        use crate::iceberg::spec::Type;
        match data_type {
            Type::Variant(_) => true,
            Type::Struct(fields) => fields
                .fields()
                .iter()
                .any(|field| contains(&field.field_type)),
            Type::List(list) => contains(&list.element_field.field_type),
            Type::Map(map) => {
                contains(&map.key_field.field_type) || contains(&map.value_field.field_type)
            }
            Type::Primitive(_) => false,
        }
    }
    schema
        .as_struct()
        .fields()
        .iter()
        .any(|field| contains(&field.field_type))
}

pub(crate) fn table_has_variant_history(metadata: &TableMetadata) -> bool {
    metadata
        .schemas_iter()
        .any(|schema| schema_contains_variant(schema))
}

pub(crate) fn with_variant_statistics_hint(message: String, metadata: &TableMetadata) -> String {
    with_variant_statistics_hint_for_history(message, table_has_variant_history(metadata))
}

pub(crate) fn with_variant_statistics_hint_for_history(
    message: String,
    has_variant_history: bool,
) -> String {
    if has_variant_history {
        format!(
            "{message}; hint: this table has had Variant columns, and NovaRocks cannot read Variant column statistics written by other engines yet, which may be related (docs/guides/iceberg-v3/variant.md)"
        )
    } else {
        message
    }
}

// Design: ADR-0174 (docs/adr/ADR-0174-vendored-iceberg-baseline-and-patch-policy.md)
/// Read a pinned manifest list directly through its admitted FileIO.
/// This preserves the existing unencrypted metadata read boundary.
pub async fn load_manifest_list(
    snapshot: &Snapshot,
    file_io: &FileIO,
    metadata: &TableMetadata,
) -> crate::iceberg::Result<ManifestList> {
    let bytes = file_io.new_input(snapshot.manifest_list())?.read().await?;
    decode_manifest_list(&bytes, metadata)
}

pub(crate) fn decode_manifest_list(
    bytes: &[u8],
    metadata: &TableMetadata,
) -> crate::iceberg::Result<ManifestList> {
    ManifestList::parse_with_version(bytes, metadata.format_version())
}

/// Read physical entries and inherit their snapshot and sequence facts from
/// the manifest-list descriptor. Row-ID assignment remains with the provider's
/// existing row-lineage owner; this read never assigns first_row_id or decrypts.
pub async fn load_manifest(
    descriptor: &ManifestFile,
    file_io: &FileIO,
) -> crate::iceberg::Result<Manifest> {
    let bytes = file_io.new_input(&descriptor.manifest_path)?.read().await?;
    decode_manifest(&bytes, descriptor)
}

pub(crate) fn decode_manifest(
    bytes: &[u8],
    descriptor: &ManifestFile,
) -> crate::iceberg::Result<Manifest> {
    // Iceberg's initial sequence is zero; the SDK keeps this constant private.
    const INITIAL_SEQUENCE_NUMBER: i64 = 0;
    let (entries, metadata) = Manifest::parse_avro(bytes)?.into_parts();
    let entries = entries
        .into_iter()
        .map(|entry| {
            let mut entry = Arc::unwrap_or_clone(entry);
            if entry.snapshot_id.is_none() {
                entry.snapshot_id = Some(descriptor.added_snapshot_id);
            }
            let inherits_sequence = entry.status == ManifestStatus::Added
                || descriptor.sequence_number == INITIAL_SEQUENCE_NUMBER;
            if entry.sequence_number.is_none() && inherits_sequence {
                entry.sequence_number = Some(descriptor.sequence_number);
            }
            if entry.file_sequence_number.is_none() && inherits_sequence {
                entry.file_sequence_number = Some(descriptor.sequence_number);
            }
            entry
        })
        .collect();
    Ok(Manifest::new(metadata, entries))
}

/// Data-file facts extracted from one pinned Iceberg manifest walk.
#[derive(Clone)]
pub struct DataFileWithStats {
    pub path: String,
    pub size: i64,
    pub record_count: Option<i64>,
    pub column_stats: Option<HashMap<String, IcebergColumnStats>>,
    pub partition_spec_id: Option<i32>,
    pub partition_key: Option<String>,
    pub partition_values: Option<crate::iceberg::spec::Struct>,
    pub manifest_path: Option<String>,
    pub partition_field_values: Vec<IcebergPartitionFieldValue>,
    /// Iceberg v3 row-lineage: first row id assigned to this data file.
    pub first_row_id: Option<i64>,
    /// Iceberg v3 row-lineage: data sequence number of the manifest entry.
    pub data_sequence_number: Option<i64>,
    pub delete_files: Vec<IcebergDeleteFileInfo>,
}

pub fn data_file_with_stats_to_iceberg_data_file_info(
    file: DataFileWithStats,
) -> IcebergDataFileInfo {
    IcebergDataFileInfo {
        path: file.path,
        size: file.size,
        row_count: file.record_count,
        column_stats: file.column_stats,
        partition_spec_id: file.partition_spec_id,
        partition_key: file.partition_key,
        first_row_id: file.first_row_id,
        data_sequence_number: file.data_sequence_number,
        ivm_change_op: None,
        included_positions: None,
        delete_files: file.delete_files,
        manifest_path: file.manifest_path,
        partition_values: file.partition_field_values,
    }
}

fn partition_field_values(
    metadata: &TableMetadata,
    schema: &crate::iceberg::spec::Schema,
    spec_id: i32,
    partition: &crate::iceberg::spec::Struct,
) -> Result<Vec<IcebergPartitionFieldValue>, String> {
    partition_field_values_checked(metadata, schema, spec_id, partition, None)
        .map_err(|e| e.to_string())
}
fn partition_field_values_checked(
    metadata: &TableMetadata,
    schema: &crate::iceberg::spec::Schema,
    spec_id: i32,
    partition: &crate::iceberg::spec::Struct,
    capture: Option<&crate::read_snapshot::cow_capture::CaptureScope<'_>>,
) -> Result<Vec<IcebergPartitionFieldValue>, crate::read_snapshot::cow_capture::ReadFailure> {
    let Some(spec) = metadata.partition_spec_by_id(spec_id) else {
        return Err(format!("iceberg table metadata missing partition spec id {spec_id}").into());
    };
    let mut values = Vec::with_capacity(spec.fields().len());
    for (idx, field) in spec.fields().iter().enumerate() {
        if let Some(capture) = capture {
            capture.active()?;
        }
        let source_column = schema
            .field_by_id(field.source_id)
            .map(|source| source.name.clone())
            .unwrap_or_else(|| format!("#{}", field.source_id));
        let value = partition
            .fields()
            .get(idx)
            .and_then(|literal| literal.as_ref())
            .and_then(partition_value_from_literal);
        values.push(IcebergPartitionFieldValue {
            source_column,
            field_name: field.name.clone(),
            transform: partition_transform_name(&field.transform),
            value,
        });
    }
    Ok(values)
}

fn partition_transform_name(transform: &Transform) -> String {
    match transform {
        Transform::Identity => "identity".to_string(),
        other => format!("{other:?}").to_ascii_lowercase(),
    }
}

fn partition_value_from_literal(literal: &IcebergLiteral) -> Option<IcebergPartitionValue> {
    let IcebergLiteral::Primitive(value) = literal else {
        return None;
    };
    match value {
        PrimitiveLiteral::Boolean(value) => Some(IcebergPartitionValue::Boolean(*value)),
        PrimitiveLiteral::Int(value) => Some(IcebergPartitionValue::Int32(*value)),
        PrimitiveLiteral::Long(value) => Some(IcebergPartitionValue::Int64(*value)),
        PrimitiveLiteral::Float(value) => Some(IcebergPartitionValue::Float(value.0)),
        PrimitiveLiteral::Double(value) => Some(IcebergPartitionValue::Double(value.0)),
        PrimitiveLiteral::String(value) => Some(IcebergPartitionValue::String(value.clone())),
        PrimitiveLiteral::Binary(value) => Some(IcebergPartitionValue::Binary(value.clone())),
        PrimitiveLiteral::Int128(_)
        | PrimitiveLiteral::UInt128(_)
        | PrimitiveLiteral::AboveMax
        | PrimitiveLiteral::BelowMin => None,
    }
}

fn equality_delete_column_names_for_field_ids(
    file_path: &str,
    equality_ids: Option<Vec<i32>>,
    field_id_to_name: &HashMap<i32, String>,
) -> Result<Vec<String>, String> {
    let equality_ids = equality_ids
        .ok_or_else(|| format!("iceberg equality-delete file {file_path} missing equality_ids"))?;
    if equality_ids.is_empty() {
        return Err(format!(
            "iceberg equality-delete file {file_path} has empty equality_ids"
        ));
    }
    equality_ids
        .iter()
        .map(|id| {
            field_id_to_name.get(id).cloned().ok_or_else(|| {
                format!("iceberg equality-delete file {file_path} references unknown field id {id}")
            })
        })
        .collect()
}

pub async fn current_equality_delete_column_names(table: &Table) -> Result<Vec<String>, String> {
    current_equality_delete_column_names_with_control(table, None).await
}

pub(crate) async fn current_equality_delete_column_names_with_control(
    table: &Table,
    control: Option<&dyn novarocks_spi::connector::ConnectorOperationControl>,
) -> Result<Vec<String>, String> {
    let check_active = || -> Result<(), String> {
        if let Some(control) = control {
            control.check_active().map_err(|error| error.to_string())?;
        }
        Ok(())
    };
    check_active()?;
    let metadata = table.metadata();
    let Some(snapshot) = metadata.current_snapshot() else {
        return Ok(Vec::new());
    };
    let schema = metadata.current_schema();
    let field_id_to_name: HashMap<i32, String> = schema
        .as_struct()
        .fields()
        .iter()
        .map(|field| (field.id, field.name.clone()))
        .collect();
    let file_io = table.file_io();
    let manifest_list = crate::manifest::load_manifest_list(snapshot, file_io, metadata)
        .await
        .map_err(|error| format!("load manifest list: {error}"))?;
    check_active()?;
    let mut columns = Vec::new();
    for manifest_file in manifest_list.entries() {
        check_active()?;
        if manifest_file.content != ManifestContentType::Deletes {
            continue;
        }
        let manifest = crate::manifest::load_manifest(manifest_file, file_io)
            .await
            .map_err(|error| {
                with_variant_statistics_hint(format!("load manifest: {error}"), metadata)
            })?;
        check_active()?;
        for entry in manifest.entries() {
            check_active()?;
            if entry.status == ManifestStatus::Deleted {
                continue;
            }
            let data_file = entry.data_file();
            if data_file.content_type() != DataContentType::EqualityDeletes {
                continue;
            }
            if data_file.file_format() != DataFileFormat::Parquet {
                return Err(format!(
                    "unsupported iceberg equality-delete file format {:?}: {}",
                    data_file.file_format(),
                    data_file.file_path()
                ));
            }
            columns.extend(equality_delete_column_names_for_field_ids(
                data_file.file_path(),
                data_file.equality_ids(),
                &field_id_to_name,
            )?);
        }
    }
    check_active()?;
    Ok(columns)
}

#[cfg(test)]
fn read_delete_to_catalog_delete(
    delete_file: crate::read_model::IcebergReadDeleteFile,
) -> Result<IcebergDeleteFileInfo, String> {
    let file_format = match delete_file.file_format {
        IcebergReadDeleteFormat::Parquet => IcebergDeleteFileFormat::Parquet,
        IcebergReadDeleteFormat::Puffin => IcebergDeleteFileFormat::Puffin,
    };
    let (file_content, equality_column_names, equality_field_ids) = match delete_file.kind {
        IcebergReadDeleteKind::Position => {
            (IcebergDeleteFileContent::Position, Vec::new(), Vec::new())
        }
        IcebergReadDeleteKind::Equality { equality_field_ids } => {
            if file_format != IcebergDeleteFileFormat::Parquet {
                return Err(format!(
                    "iceberg equality-delete file {} must use Parquet format",
                    delete_file.path
                ));
            }
            (
                IcebergDeleteFileContent::Equality,
                Vec::new(),
                equality_field_ids,
            )
        }
    };
    Ok(IcebergDeleteFileInfo {
        path: delete_file.path,
        record_count: None,
        partition_data_json: None,
        file_format,
        file_content,
        length: delete_file.length,
        content_offset: delete_file.content_offset,
        content_size_in_bytes: delete_file.content_size_in_bytes,
        sequence_number: delete_file.sequence_number,
        partition_spec_id: delete_file.partition_spec_id,
        partition_key: delete_file.partition_key,
        referenced_data_file: delete_file.referenced_data_file,
        equality_column_names,
        equality_field_ids,
    })
}

fn canonical_delete_to_catalog_delete(
    fact: &crate::delete_semantics::DeleteFact,
) -> Result<IcebergDeleteFileInfo, String> {
    use crate::delete_semantics::{DeleteContentAddress, DeleteFormat, DeleteKind};
    let bounded = |value: u64| {
        i64::try_from(value)
            .map_err(|_| "Iceberg delete size exceeds the catalog representation".to_owned())
    };
    let file_format = match fact.read().format {
        DeleteFormat::Parquet => IcebergDeleteFileFormat::Parquet,
        DeleteFormat::Puffin => IcebergDeleteFileFormat::Puffin,
        other => return Err(format!("unsupported Iceberg delete format {other:?}")),
    };
    let (file_content, equality_field_ids, referenced_data_file) = match fact.kind() {
        DeleteKind::Position { exact_target } => (
            IcebergDeleteFileContent::Position,
            Vec::new(),
            exact_target.as_deref().map(str::to_owned),
        ),
        DeleteKind::DeletionVector { exact_target } => (
            IcebergDeleteFileContent::Position,
            Vec::new(),
            Some(exact_target.to_string()),
        ),
        DeleteKind::Equality(group) => (
            IcebergDeleteFileContent::Equality,
            group.fields().iter().map(|(id, _)| *id).collect(),
            None,
        ),
    };
    let (content_offset, content_size_in_bytes) = match fact.address() {
        DeleteContentAddress::File(_) => (None, None),
        DeleteContentAddress::PuffinBlob { offset, length, .. } => {
            (Some(bounded(*offset)?), Some(bounded(*length)?))
        }
    };
    Ok(IcebergDeleteFileInfo {
        path: fact.address().path().to_owned(),
        record_count: Some(bounded(fact.read().record_count)?),
        partition_data_json: Some(fact.partition().to_json_string()),
        file_format,
        file_content,
        length: Some(bounded(fact.read().file_size)?),
        content_offset,
        content_size_in_bytes,
        sequence_number: Some(fact.sequence().get()),
        partition_spec_id: Some(fact.partition().spec_id()),
        partition_key: Some(fact.partition().to_json_string()),
        referenced_data_file,
        equality_column_names: Vec::new(),
        equality_field_ids,
    })
}

pub async fn extract_data_files_with_stats_at(
    table: &Table,
    snapshot_id: i64,
) -> Result<Vec<DataFileWithStats>, String> {
    extract_data_files_with_stats_at_with_control(table, snapshot_id, None).await
}

/// Original statistics and the original typed facts they do not expose.
/// This value is provider-private and never crosses a generic/native boundary.
pub(crate) struct FrozenReadFileWithStats {
    pub stats: DataFileWithStats,
    manifest: std::sync::Arc<crate::read_model::IcebergDataFileMetadata>,
    deletes: crate::delete_semantics::LoadView,
}

impl FrozenReadFileWithStats {
    pub(crate) fn retain(
        stats: DataFileWithStats,
        manifest: std::sync::Arc<crate::read_model::IcebergDataFileMetadata>,
        deletes: crate::delete_semantics::LoadView,
    ) -> Self {
        Self {
            stats,
            manifest,
            deletes,
        }
    }

    /// Move the exact original fields after legacy statistics, match and
    /// branch-source validation completed. No file fact is guessed from its
    /// path, catalog delete DTO, current table, or partition spelling.
    pub(crate) fn into_read_file(self) -> crate::read_model::IcebergReadFile {
        let stats = self.stats;
        crate::read_model::IcebergReadFile {
            path: stats.path,
            size: stats.size,
            record_count: stats.record_count,
            column_stats: stats.column_stats,
            partition_spec_id: stats.partition_spec_id,
            partition_key: stats.partition_key,
            partition_values: stats.partition_values,
            manifest_path: stats.manifest_path,
            first_row_id: stats.first_row_id,
            data_sequence_number: stats.data_sequence_number,
            manifest: self.manifest,
            deletes: self.deletes,
        }
    }
}

pub(crate) async fn extract_data_files_with_stats_at_with_control(
    table: &Table,
    snapshot_id: i64,
    control: Option<&dyn novarocks_spi::connector::ConnectorOperationControl>,
) -> Result<Vec<DataFileWithStats>, String> {
    extract_data_files_with_stats_at_projected(table, snapshot_id, control, |stats, _, _| stats)
        .await
}

/// Run the original statistics author once. The legacy projection returns
/// the original value directly; a frozen source may retain the exact typed
/// remainder by move without cloning or re-decoding a manifest/delete view.
pub(crate) async fn extract_data_files_with_stats_at_projected<O>(
    table: &Table,
    snapshot_id: i64,
    control: Option<&dyn novarocks_spi::connector::ConnectorOperationControl>,
    project: impl FnMut(
        DataFileWithStats,
        std::sync::Arc<crate::read_model::IcebergDataFileMetadata>,
        crate::delete_semantics::LoadView,
    ) -> O,
) -> Result<Vec<O>, String> {
    let read_snapshot =
        crate::read_snapshot::build_read_snapshot_at_with_control(table, snapshot_id, control)
            .await?;
    finish_data_files_with_stats_from_original_files(
        table.metadata(),
        snapshot_id,
        read_snapshot.files,
        control,
        project,
    )
}

/// The original pure projection, shared by scoped CPU continuations after the
/// same complete manifest observation; no I/O or second manifest algorithm.
pub(crate) fn finish_data_files_with_stats_from_original_files<O>(
    metadata: &crate::iceberg::spec::TableMetadata,
    snapshot_id: i64,
    files: Vec<crate::read_model::IcebergReadFile>,
    control: Option<&dyn novarocks_spi::connector::ConnectorOperationControl>,
    mut project: impl FnMut(
        DataFileWithStats,
        std::sync::Arc<crate::read_model::IcebergDataFileMetadata>,
        crate::delete_semantics::LoadView,
    ) -> O,
) -> Result<Vec<O>, String> {
    let snapshot_schema = metadata
        .snapshot_by_id(snapshot_id)
        .ok_or_else(|| format!("Iceberg snapshot {snapshot_id} is absent from table metadata"))?
        .schema(metadata)
        .map_err(|error| format!("resolve Iceberg snapshot {snapshot_id} schema: {error}"))?;
    files
        .into_iter()
        .map(|file| {
            if let Some(control) = control {
                control.check_active().map_err(|error| error.to_string())?;
            }
            let partition_field_values =
                match (file.partition_spec_id, file.partition_values.as_ref()) {
                    (Some(spec_id), Some(partition_values)) => partition_field_values(
                        metadata,
                        snapshot_schema.as_ref(),
                        spec_id,
                        partition_values,
                    )?,
                    _ => Vec::new(),
                };
            let delete_files = file
                .deletes
                .members()
                .map(|fact| canonical_delete_to_catalog_delete(fact))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(project(
                DataFileWithStats {
                    path: file.path,
                    size: file.size,
                    record_count: file.record_count,
                    column_stats: file.column_stats,
                    partition_spec_id: file.partition_spec_id,
                    partition_key: file.partition_key,
                    partition_values: file.partition_values,
                    manifest_path: file.manifest_path,
                    partition_field_values,
                    first_row_id: file.first_row_id,
                    data_sequence_number: file.data_sequence_number,
                    delete_files,
                },
                file.manifest,
                file.deletes,
            ))
        })
        .collect()
}

/// COW extracts the same complete live-file facts without constructing scan-only
/// DataFile clones or the unused IcebergDataFileMetadata graph.
#[cfg(test)]
pub(crate) async fn extract_cow_data_files_with_stats_at(
    table: &Table,
    snapshot_id: i64,
) -> Result<Vec<DataFileWithStats>, String> {
    extract_cow_data_files_with_stats_at_with_control(table, snapshot_id, None).await
}

#[cfg(test)]
pub(crate) async fn extract_cow_data_files_with_stats_at_with_control(
    table: &Table,
    snapshot_id: i64,
    control: Option<&dyn novarocks_spi::connector::ConnectorOperationControl>,
) -> Result<Vec<DataFileWithStats>, String> {
    let metadata = table.metadata();
    let snapshot_schema = metadata
        .snapshot_by_id(snapshot_id)
        .ok_or_else(|| format!("Iceberg snapshot {snapshot_id} is absent from table metadata"))?
        .schema(metadata)
        .map_err(|error| format!("resolve Iceberg snapshot {snapshot_id} schema: {error}"))?;
    let files =
        crate::read_snapshot::build_cow_read_files_at_with_control(table, snapshot_id, control)
            .await?;
    files
        .into_iter()
        .map(|file| {
            if let Some(control) = control {
                control.check_active().map_err(|error| error.to_string())?;
            }
            let partition_field_values =
                match (file.partition_spec_id, file.partition_values.as_ref()) {
                    (Some(spec_id), Some(partition_values)) => partition_field_values(
                        metadata,
                        snapshot_schema.as_ref(),
                        spec_id,
                        partition_values,
                    )?,
                    _ => Vec::new(),
                };
            let delete_files = file
                .deletes
                .members()
                .map(|fact| canonical_delete_to_catalog_delete(fact))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(DataFileWithStats {
                path: file.path,
                size: file.size,
                record_count: file.record_count,
                column_stats: file.column_stats,
                partition_spec_id: file.partition_spec_id,
                partition_key: file.partition_key,
                partition_values: file.partition_values,
                manifest_path: file.manifest_path,
                partition_field_values,
                first_row_id: file.first_row_id,
                data_sequence_number: file.data_sequence_number,
                delete_files,
            })
        })
        .collect()
}

/// Original-window COW capture keeps all outputs under one neutral owner.
pub(crate) struct CheckedCowDataFiles {
    pub files: Vec<FrozenReadFileWithStats>,
    pub _retention_guard: novarocks_spi::connector::ConnectorPayloadRetentionGuard,
}

pub(crate) async fn extract_cow_data_files_with_stats_at_with_original_scope(
    table: &Table,
    snapshot_id: i64,
    scope: &novarocks_spi::connector::ConnectorOriginalResultScope,
) -> Result<CheckedCowDataFiles, novarocks_spi::connector::ConnectorCowBeginCause> {
    use crate::read_snapshot::cow_capture::{CaptureScope, ReadFailure};
    let build = async {
        let capture = CaptureScope::new(scope)?;
        let captured = crate::read_snapshot::build_cow_read_files_at_with_original_scope(
            table,
            snapshot_id,
            &capture,
        )
        .await?;
        finish_checked_cow_data_files_from_original_files(
            table.metadata(),
            snapshot_id,
            captured,
            scope,
            &capture,
        )
    };
    build.await.map_err(ReadFailure::into_cause)
}

/// Pure checked projection of the same original raw files. The caller retains
/// its original operation and the exact captured files.
pub(crate) fn finish_checked_cow_data_files_from_original_files(
    metadata: &crate::iceberg::spec::TableMetadata,
    snapshot_id: i64,
    captured: Vec<crate::read_model::IcebergReadFile>,
    scope: &novarocks_spi::connector::ConnectorOriginalResultScope,
    capture: &crate::read_snapshot::cow_capture::CaptureScope<'_>,
) -> Result<CheckedCowDataFiles, crate::read_snapshot::cow_capture::ReadFailure> {
    use crate::read_snapshot::cow_capture::ReadFailure;
    let snapshot_schema = metadata
        .snapshot_by_id(snapshot_id)
        .ok_or_else(|| format!("Iceberg snapshot {snapshot_id} is absent from table metadata"))?
        .schema(metadata)
        .map_err(|error| format!("resolve Iceberg snapshot {snapshot_id} schema: {error}"))?;
    let mut files = Vec::with_capacity(captured.len());
    for file in captured {
        capture.active()?;
        let partition_field_values = match (file.partition_spec_id, file.partition_values.as_ref())
        {
            (Some(spec_id), Some(values)) => partition_field_values_checked(
                metadata,
                snapshot_schema.as_ref(),
                spec_id,
                values,
                Some(capture),
            )?,
            _ => Vec::new(),
        };
        let mut delete_files = Vec::new();
        for fact in file.deletes.members() {
            capture.active()?;
            delete_files.push(canonical_delete_to_catalog_delete(fact)?);
        }
        let stats = DataFileWithStats {
            path: file.path,
            size: file.size,
            record_count: file.record_count,
            column_stats: file.column_stats,
            partition_spec_id: file.partition_spec_id,
            partition_key: file.partition_key,
            partition_values: file.partition_values,
            manifest_path: file.manifest_path,
            partition_field_values,
            first_row_id: file.first_row_id,
            data_sequence_number: file.data_sequence_number,
            delete_files,
        };
        files.push(FrozenReadFileWithStats::retain(
            stats,
            file.manifest,
            file.deletes,
        ));
    }
    capture.active()?;
    Ok::<_, ReadFailure>(CheckedCowDataFiles {
        files,
        _retention_guard: scope.retention_guard(),
    })
}

pub async fn extract_data_files_with_stats(
    table: &Table,
) -> Result<Vec<DataFileWithStats>, String> {
    extract_data_files_with_stats_with_control(table, None).await
}

pub(crate) async fn extract_data_files_with_stats_with_control(
    table: &Table,
    control: Option<&dyn novarocks_spi::connector::ConnectorOperationControl>,
) -> Result<Vec<DataFileWithStats>, String> {
    if let Some(control) = control {
        control.check_active().map_err(|error| error.to_string())?;
    }
    let files = match table.metadata().current_snapshot() {
        Some(snapshot) => {
            extract_data_files_with_stats_at_with_control(table, snapshot.snapshot_id(), control)
                .await
        }
        None => Ok(Vec::new()),
    }?;
    if let Some(control) = control {
        control.check_active().map_err(|error| error.to_string())?;
    }
    Ok(files)
}

#[cfg(test)]
mod tests {
    use super::{equality_delete_column_names_for_field_ids, read_delete_to_catalog_delete};
    use std::collections::HashMap;

    use crate::read_model::{
        IcebergReadDeleteFile, IcebergReadDeleteFormat, IcebergReadDeleteKind,
    };
    use crate::scan_model::{IcebergDeleteFileContent, IcebergDeleteFileFormat};

    #[test]
    fn variant_manifest_hint_uses_all_historical_schemas_and_preserves_original_text() {
        use crate::iceberg::spec::{
            FormatVersion, NestedField, PartitionSpec, PrimitiveType, Schema, SortOrder,
            TableMetadataBuilder, Type, VariantType,
        };
        let variant_schema = Schema::builder()
            .with_fields(vec![
                NestedField::optional(1, "v", Type::Variant(VariantType)).into(),
            ])
            .build()
            .unwrap();
        let clean_schema = Schema::builder()
            .with_fields(vec![
                NestedField::optional(2, "id", Type::Primitive(PrimitiveType::Long)).into(),
            ])
            .build()
            .unwrap();
        let new = |schema| {
            TableMetadataBuilder::new(
                schema,
                PartitionSpec::unpartition_spec().into_unbound(),
                SortOrder::unsorted_order(),
                "file:///warehouse/t".to_owned(),
                FormatVersion::V3,
                HashMap::new(),
            )
            .unwrap()
        };
        let never_variant = new(clean_schema.clone()).build().unwrap().metadata;
        let old_variant = new(variant_schema)
            .add_current_schema(clean_schema)
            .unwrap()
            .build()
            .unwrap()
            .metadata;
        assert!(!super::schema_contains_variant(
            old_variant.current_schema()
        ));
        for original in [
            "permission denied",
            "network timeout",
            "invalid Avro",
            "Variant statistics",
        ] {
            assert_eq!(
                super::with_variant_statistics_hint(original.to_owned(), &never_variant),
                original
            );
            let hinted = super::with_variant_statistics_hint(original.to_owned(), &old_variant);
            assert!(hinted.starts_with(original));
            assert!(hinted.contains("has had Variant columns"));
        }
    }

    fn read_delete(
        file_format: IcebergReadDeleteFormat,
        kind: IcebergReadDeleteKind,
    ) -> IcebergReadDeleteFile {
        IcebergReadDeleteFile {
            path: "s3://bucket/table/delete-file".to_string(),
            file_format,
            kind,
            length: Some(128),
            content_offset: None,
            content_size_in_bytes: None,
            sequence_number: Some(7),
            partition_spec_id: Some(1),
            partition_key: Some("city=A".to_string()),
            referenced_data_file: None,
        }
    }

    #[test]
    fn parquet_equality_delete_carries_explicit_field_ids() {
        let delete_file = read_delete(
            IcebergReadDeleteFormat::Parquet,
            IcebergReadDeleteKind::Equality {
                equality_field_ids: vec![3, 1],
            },
        );
        let catalog_delete = read_delete_to_catalog_delete(delete_file).expect("convert");

        assert_eq!(catalog_delete.file_format, IcebergDeleteFileFormat::Parquet);
        assert_eq!(
            catalog_delete.file_content,
            IcebergDeleteFileContent::Equality
        );
        assert_eq!(catalog_delete.equality_field_ids, vec![3, 1]);
        assert!(catalog_delete.equality_column_names.is_empty());
    }

    #[test]
    fn puffin_position_delete_preserves_content_range() {
        let mut delete_file = read_delete(
            IcebergReadDeleteFormat::Puffin,
            IcebergReadDeleteKind::Position,
        );
        delete_file.content_offset = Some(64);
        delete_file.content_size_in_bytes = Some(512);
        let catalog_delete = read_delete_to_catalog_delete(delete_file).expect("convert");

        assert_eq!(catalog_delete.file_format, IcebergDeleteFileFormat::Puffin);
        assert_eq!(
            catalog_delete.file_content,
            IcebergDeleteFileContent::Position
        );
        assert_eq!(catalog_delete.content_offset, Some(64));
        assert_eq!(catalog_delete.content_size_in_bytes, Some(512));
        assert!(catalog_delete.equality_field_ids.is_empty());
    }

    #[test]
    fn puffin_equality_delete_is_rejected() {
        let delete_file = read_delete(
            IcebergReadDeleteFormat::Puffin,
            IcebergReadDeleteKind::Equality {
                equality_field_ids: vec![3],
            },
        );
        let err = read_delete_to_catalog_delete(delete_file).expect_err("reject puffin equality");

        assert!(err.contains("must use Parquet format"));
    }

    #[test]
    fn equality_delete_column_names_follow_current_schema_field_ids() {
        let fields = HashMap::from([(1, "id".to_string()), (2, "category".to_string())]);
        let names =
            equality_delete_column_names_for_field_ids("delete.parquet", Some(vec![2, 1]), &fields)
                .expect("column names");

        assert_eq!(names, vec!["category".to_string(), "id".to_string()]);
    }

    #[test]
    fn equality_delete_column_names_reject_unknown_field_id() {
        let fields = HashMap::from([(1, "id".to_string())]);
        let err =
            equality_delete_column_names_for_field_ids("delete.parquet", Some(vec![7]), &fields)
                .expect_err("unknown field id");

        assert!(err.contains("unknown field id 7"));
    }
}
