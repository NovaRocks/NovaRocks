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

//! Errors and (in later PRs) data structures for iceberg snapshot-lineage
//! change planning under IVM Phase 2. This file is the home of the new
//! `plan_changes` entrypoint that PR-2 will introduce; PR-1 only lands the
//! error enum so that CREATE-time PRIMARY KEY validation has a stable type
//! to return.

use crate::connector::iceberg::delta::{
    DeltaSourceFile, DeltaSourceRole, EqualityDeleteTargetData, PositionDeleteFileFormat,
    PositionDeleteSourceData,
};

/// All failure modes the iceberg change-planning and IVM CREATE/REFRESH
/// paths can surface. STRICT fail-fast: every variant is a hard rejection,
/// not a fallback signal. Variants not constructed in this PR are reserved
/// for PR-2 (`plan_changes` lineage walk) and PR-3/4 (runtime checks).
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(dead_code)]
pub(crate) enum ChangeError {
    /// `previous_snapshot` referenced by stored MV state is no longer
    /// reachable from the current snapshot's parent chain (e.g. expired).
    LineageBroken { previous_snapshot: i64 },

    /// Snapshot operation is not understood or not in scope for this phase
    /// (e.g. vendor-specific ops).
    UnsupportedOperation { snapshot_id: i64, op: String },

    /// Schema evolution between `previous_snapshot` and `current_snapshot`
    /// (or any unsupported schema-related rejection at CREATE time).
    SchemaEvolutionUnsupported { detail: String },

    /// REPLACE snapshot failed the compaction-only sanity checks (records
    /// changed / schema-id changed / no added or no removed files).
    ReplaceValidationFailed { snapshot_id: i64, reason: String },

    /// CREATE-time: PRIMARY KEY column does not exist on the iceberg base
    /// table.
    PrimaryKeyMissingFromBase { pk_col: String },

    /// CREATE-time: PRIMARY KEY column is nullable on the base table.
    PrimaryKeyNullable { pk_col: String },

    /// CREATE-time: PRIMARY KEY column has a non-hashable scalar type.
    PrimaryKeyTypeUnsupported { pk_col: String, ty: String },

    /// Runtime: PRIMARY KEY column observed NULL in a base row at refresh
    /// time. Not constructed in PR-1.
    PrimaryKeyValueNull { row_info: String },

    /// CREATE-time: iceberg base table is not format-version 2.
    IcebergFormatUnsupported { format_version: i32 },

    /// Catch-all for invariant violations the codebase should never hit;
    /// constructing one is a bug, not a user error.
    InternalInconsistency(String),
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[allow(dead_code)]
pub(crate) enum IcebergChangePolicySignal {
    Incremental,
    FullRefresh { reason: String },
    Unsupported { reason: String },
}

pub(crate) fn policy_signal_from_change_error(err: &ChangeError) -> IcebergChangePolicySignal {
    match err {
        ChangeError::LineageBroken { .. } => IcebergChangePolicySignal::FullRefresh {
            reason: "previous snapshot is not reachable".to_string(),
        },
        ChangeError::ReplaceValidationFailed { reason, .. } => {
            IcebergChangePolicySignal::FullRefresh {
                reason: format!("replace snapshot is not a provably safe compaction: {reason}"),
            }
        }
        ChangeError::SchemaEvolutionUnsupported { detail } => {
            IcebergChangePolicySignal::Unsupported {
                reason: format!("schema evolution is not supported by IVM: {detail}"),
            }
        }
        other => IcebergChangePolicySignal::Unsupported {
            reason: other.to_string(),
        },
    }
}

impl std::fmt::Display for ChangeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ChangeError::LineageBroken { previous_snapshot } => write!(
                f,
                "iceberg lineage broken: previous snapshot {previous_snapshot} is unreachable from current snapshot"
            ),
            ChangeError::UnsupportedOperation { snapshot_id, op } => {
                write!(
                    f,
                    "iceberg snapshot {snapshot_id} has unsupported operation `{op}`"
                )
            }
            ChangeError::SchemaEvolutionUnsupported { detail } => {
                write!(f, "iceberg schema evolution not supported: {detail}")
            }
            ChangeError::ReplaceValidationFailed {
                snapshot_id,
                reason,
            } => write!(
                f,
                "iceberg REPLACE snapshot {snapshot_id} failed compaction validation: {reason}"
            ),
            ChangeError::PrimaryKeyMissingFromBase { pk_col } => write!(
                f,
                "PRIMARY KEY column `{pk_col}` does not exist on the iceberg base table"
            ),
            ChangeError::PrimaryKeyNullable { pk_col } => write!(
                f,
                "PRIMARY KEY column `{pk_col}` must be NOT NULL on the iceberg base table"
            ),
            ChangeError::PrimaryKeyTypeUnsupported { pk_col, ty } => write!(
                f,
                "PRIMARY KEY column `{pk_col}` has unsupported type `{ty}`; only hashable scalar types are allowed"
            ),
            ChangeError::PrimaryKeyValueNull { row_info } => {
                write!(f, "PRIMARY KEY value is NULL in base row: {row_info}")
            }
            ChangeError::IcebergFormatUnsupported { format_version } => write!(
                f,
                "iceberg base table format-version {format_version} is not supported; IVM requires v2 or v3"
            ),
            ChangeError::InternalInconsistency(detail) => {
                write!(f, "internal inconsistency: {detail}")
            }
        }
    }
}

impl std::error::Error for ChangeError {}

/// Reference to a single data file added to the table by an `Append`
/// snapshot. Row-lineage metadata is preserved so incremental MV refresh can
/// expose Iceberg v3 metadata columns while scanning only the appended files.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct DataFileRef {
    pub path: String,
    pub size: i64,
    pub record_count: Option<i64>,
    pub partition_spec_id: Option<i32>,
    pub partition_key: Option<String>,
    pub partition_values: Vec<ChangePartitionFieldValue>,
    pub first_row_id: Option<i64>,
    pub data_sequence_number: Option<i64>,
    /// Optional IVM-only scan-time filter for explicitly row-preserving
    /// mutation paths. Ordinary `Operation::Overwrite` snapshots must leave
    /// this as `None` so IVM observes overwrite as delete+insert.
    pub row_id_allow_list: Option<std::collections::BTreeSet<i64>>,
}

/// A single Iceberg partition field value carried with an Iceberg change ref.
///
/// This is intentionally local to change planning. Unlike catalog-level
/// `IcebergPartitionFieldValue`, PR2 stores only conservative string
/// representations for primitive values so later MV partition planning can
/// decide whether a value is safe to consume without depending on catalog
/// typed-value semantics.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ChangePartitionFieldValue {
    /// Stable Iceberg source field id from the partition spec.
    pub source_field_id: i32,
    /// Optional diagnostic source table column name resolved from the current
    /// Iceberg schema for `source_field_id`. Consumers that need stability
    /// should use `source_field_id`; this is `None` when the current schema no
    /// longer contains the source field id.
    pub source_column: Option<String>,
    /// Iceberg partition field name from the partition spec.
    pub field_name: String,
    /// Iceberg partition transform name, such as `identity`, `bucket(8)`, or
    /// `month`.
    pub transform: String,
    /// Partition field value. `Null` means the manifest explicitly carries a
    /// null partition value. `Unsupported` means a value exists but PR2 should
    /// not treat it as safely usable for MV partition planning.
    pub value: ChangePartitionValue,
}

/// Conservative partition value representation for Iceberg change planning.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum ChangePartitionValue {
    /// Actual NULL partition value in the manifest partition struct.
    Null,
    /// Primitive partition value rendered as a stable string for planner use.
    Primitive(String),
    /// Value exists, but this change-planning metadata path cannot represent it safely.
    Unsupported(String),
}

impl ChangePartitionValue {
    #[cfg(test)]
    pub(crate) fn as_primitive_str(&self) -> Option<&str> {
        match self {
            ChangePartitionValue::Primitive(value) => Some(value.as_str()),
            ChangePartitionValue::Null | ChangePartitionValue::Unsupported(_) => None,
        }
    }
}

/// Reference to a data file removed by an Iceberg overwrite snapshot. Reading
/// these files back produces the delete side of the logical change stream.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct DeletedDataFileRef {
    pub path: String,
    pub size: i64,
    pub record_count: Option<i64>,
    pub partition_spec_id: Option<i32>,
    pub partition_key: Option<String>,
    pub partition_values: Vec<ChangePartitionFieldValue>,
    pub first_row_id: Option<i64>,
    pub data_sequence_number: Option<i64>,
}

/// Reference to a single position-delete file added to the table by a
/// `Delete` snapshot. PR-2 only reports these on the lineage path; the
/// reverse-projection that turns each (delete_file, pos) pair back into
/// the original base row lives in PR-3.
///
/// `referenced_data_file` carries the iceberg `DataFile.referenced_data_file`
/// field — a position-delete file MAY declare a single data file that all
/// of its rows target, in which case readers can short-circuit the join.
/// When `None`, every delete row carries its own `file_path` cell and the
/// reader must read it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PositionDeleteRef {
    pub delete_file_path: String,
    pub delete_file_size: i64,
    pub record_count: Option<i64>,
    pub referenced_data_file: Option<String>,
    /// `Parquet` for v2 position-delete files, `Puffin` for v3 deletion-vector
    /// files. Other variants are rejected at construction.
    pub file_format: iceberg::spec::DataFileFormat,
    /// Required when `file_format == Puffin`: byte offset of the
    /// `deletion-vector-v1` blob inside the Puffin file. Must be `None` when
    /// `file_format == Parquet`.
    pub content_offset: Option<i64>,
    /// Required when `file_format == Puffin`: byte length of the
    /// `deletion-vector-v1` blob inside the Puffin file. Must be `None` when
    /// `file_format == Parquet`.
    pub content_size_in_bytes: Option<i64>,
    pub partition_values: Vec<ChangePartitionFieldValue>,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum DeleteVisibilityDeleteFileFormat {
    Parquet,
    Puffin,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum DeleteVisibilityDeleteFileContent {
    Position,
    Equality,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct DeleteVisibilityDeleteFileDescriptor {
    pub path: String,
    pub file_format: DeleteVisibilityDeleteFileFormat,
    pub file_content: DeleteVisibilityDeleteFileContent,
    pub length: Option<i64>,
    pub content_offset: Option<i64>,
    pub content_size_in_bytes: Option<i64>,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct DeleteVisibilityDataFileDescriptor {
    pub path: String,
    pub size: i64,
    pub first_row_id: Option<i64>,
    pub data_sequence_number: Option<i64>,
    pub delete_files: Vec<DeleteVisibilityDeleteFileDescriptor>,
}

/// Reference to a single equality-delete file added to the table. Unlike
/// position deletes, equality deletes do not name row positions; reverse
/// projection must scan older data files in the same partition and keep rows
/// whose equality-key tuple appears in the delete file.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct EqualityDeleteRef {
    pub delete_file_path: String,
    pub delete_file_size: i64,
    pub record_count: Option<i64>,
    pub equality_ids: Vec<i32>,
    pub sequence_number: Option<i64>,
    pub partition_spec_id: Option<i32>,
    pub partition_key: Option<String>,
    pub partition_values: Vec<ChangePartitionFieldValue>,
}

fn iceberg_partition_key(partition: &iceberg::spec::Struct) -> Option<String> {
    if partition.fields().is_empty() {
        None
    } else {
        Some(format!("{partition:?}"))
    }
}

pub(crate) fn change_partition_field_values(
    metadata: &iceberg::spec::TableMetadata,
    spec_id: i32,
    partition: &iceberg::spec::Struct,
) -> Result<Vec<ChangePartitionFieldValue>, ChangeError> {
    let Some(spec) = metadata.partition_spec_by_id(spec_id) else {
        return Err(ChangeError::InternalInconsistency(format!(
            "iceberg table metadata missing partition spec id {spec_id}"
        )));
    };
    let schema = metadata.current_schema();
    let mut values = Vec::with_capacity(spec.fields().len());
    for (idx, field) in spec.fields().iter().enumerate() {
        let source_column = schema
            .field_by_id(field.source_id)
            .map(|source| source.name.clone());
        let Some(literal) = partition.fields().get(idx) else {
            return Err(ChangeError::InternalInconsistency(format!(
                "iceberg partition struct for spec id {spec_id} is missing field {} at index {idx}",
                field.name
            )));
        };
        let value = change_partition_value(literal.as_ref());
        values.push(ChangePartitionFieldValue {
            source_field_id: field.source_id,
            source_column,
            field_name: field.name.clone(),
            transform: change_partition_transform_name(&field.transform),
            value,
        });
    }
    Ok(values)
}

pub(crate) fn change_partition_transform_name(transform: &iceberg::spec::Transform) -> String {
    match transform {
        iceberg::spec::Transform::Identity => "identity".to_string(),
        other => format!("{other:?}").to_ascii_lowercase(),
    }
}

fn change_partition_value(literal: Option<&iceberg::spec::Literal>) -> ChangePartitionValue {
    let Some(literal) = literal else {
        return ChangePartitionValue::Null;
    };
    let iceberg::spec::Literal::Primitive(value) = literal else {
        return ChangePartitionValue::Unsupported("non-primitive partition value".to_string());
    };
    match value {
        iceberg::spec::PrimitiveLiteral::Boolean(v) => {
            ChangePartitionValue::Primitive(v.to_string())
        }
        iceberg::spec::PrimitiveLiteral::Int(v) => ChangePartitionValue::Primitive(v.to_string()),
        iceberg::spec::PrimitiveLiteral::Long(v) => ChangePartitionValue::Primitive(v.to_string()),
        iceberg::spec::PrimitiveLiteral::Float(v) => {
            ChangePartitionValue::Primitive(v.0.to_string())
        }
        iceberg::spec::PrimitiveLiteral::Double(v) => {
            ChangePartitionValue::Primitive(v.0.to_string())
        }
        iceberg::spec::PrimitiveLiteral::String(v) => ChangePartitionValue::Primitive(v.clone()),
        iceberg::spec::PrimitiveLiteral::Binary(_) => {
            ChangePartitionValue::Unsupported("binary partition value".to_string())
        }
        iceberg::spec::PrimitiveLiteral::Int128(_) => {
            ChangePartitionValue::Unsupported("int128 partition value".to_string())
        }
        iceberg::spec::PrimitiveLiteral::UInt128(_) => {
            ChangePartitionValue::Unsupported("uint128 partition value".to_string())
        }
        iceberg::spec::PrimitiveLiteral::AboveMax => {
            ChangePartitionValue::Unsupported("above-max partition value".to_string())
        }
        iceberg::spec::PrimitiveLiteral::BelowMin => {
            ChangePartitionValue::Unsupported("below-min partition value".to_string())
        }
    }
}

impl PositionDeleteRef {
    /// Verify the file_format / content_offset / content_size_in_bytes /
    /// referenced_data_file fields are mutually consistent. Returns
    /// `ChangeError::InternalInconsistency` on any mismatch.
    pub(crate) fn validate_invariants(&self) -> Result<(), ChangeError> {
        use iceberg::spec::DataFileFormat;
        match self.file_format {
            DataFileFormat::Parquet => {
                if self.content_offset.is_some() || self.content_size_in_bytes.is_some() {
                    return Err(ChangeError::InternalInconsistency(format!(
                        "PositionDeleteRef {} has Parquet file_format but content_offset/size set",
                        self.delete_file_path
                    )));
                }
            }
            DataFileFormat::Puffin => {
                if self.referenced_data_file.is_none() {
                    return Err(ChangeError::InternalInconsistency(format!(
                        "Puffin DV {} missing referenced_data_file",
                        self.delete_file_path
                    )));
                }
                if self.content_offset.is_none() {
                    return Err(ChangeError::InternalInconsistency(format!(
                        "Puffin DV {} missing content_offset",
                        self.delete_file_path
                    )));
                }
                if self.content_size_in_bytes.is_none() {
                    return Err(ChangeError::InternalInconsistency(format!(
                        "Puffin DV {} missing content_size_in_bytes",
                        self.delete_file_path
                    )));
                }
            }
            other => {
                return Err(ChangeError::InternalInconsistency(format!(
                    "PositionDeleteRef {} has unsupported file_format {:?}",
                    self.delete_file_path, other
                )));
            }
        }
        Ok(())
    }
}

/// Output of `plan_changes`: a flattened, in-order projection of every
/// data-file insert, every row-level delete file, and every overwrite-deleted
/// data file in the lineage from `previous_snapshot_id` (exclusive) to
/// `current_snapshot_id` (inclusive). REPLACE compaction snapshots are
/// validated and skipped; they contribute to no delta vector.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct IcebergChangeBatch {
    pub previous_snapshot_id: i64,
    /// The resolved upper endpoint of the planned lineage. When `plan_changes`
    /// was called with `to_snapshot_id = None`, this equals `table.metadata().current_snapshot()`
    /// at the time of the call. When called with `to_snapshot_id = Some(id)`, this
    /// equals `id`. Do not assume this matches the table's current snapshot
    /// at any later moment; callers that need that invariant must check explicitly.
    pub current_snapshot_id: i64,
    pub inserts: Vec<DataFileRef>,
    pub deletes: Vec<PositionDeleteRef>,
    pub equality_deletes: Vec<EqualityDeleteRef>,
    pub deleted_data_files: Vec<DeletedDataFileRef>,
}

#[allow(dead_code)]
pub(crate) fn delta_source_files_from_change_batch(
    batch: &IcebergChangeBatch,
) -> Result<Vec<DeltaSourceFile>, String> {
    delta_source_files_from_change_batch_with_equality_targets(
        batch,
        &std::collections::HashMap::new(),
    )
}

pub(crate) fn delta_source_files_from_change_batch_with_equality_targets(
    batch: &IcebergChangeBatch,
    equality_targets_by_delete_file: &std::collections::HashMap<
        String,
        Vec<EqualityDeleteTargetData>,
    >,
) -> Result<Vec<DeltaSourceFile>, String> {
    let mut out = Vec::with_capacity(
        batch.inserts.len()
            + batch.deletes.len()
            + batch.equality_deletes.len()
            + batch.deleted_data_files.len(),
    );
    for ins in &batch.inserts {
        out.push(DeltaSourceFile {
            path: ins.path.clone(),
            size: ins.size,
            role: DeltaSourceRole::DataFile,
            partition_spec_id: ins.partition_spec_id,
            partition_key: ins.partition_key.clone(),
            first_row_id: ins.first_row_id,
            data_sequence_number: ins.data_sequence_number,
            row_id_allow_list: ins.row_id_allow_list.clone(),
        });
    }
    let mut position_deletes = Vec::with_capacity(batch.deletes.len());
    for del in &batch.deletes {
        position_deletes.push(PositionDeleteSourceData {
            delete_file_path: del.delete_file_path.clone(),
            delete_file_size: del.delete_file_size,
            referenced_data_file: del.referenced_data_file.clone(),
            file_format: match del.file_format {
                iceberg::spec::DataFileFormat::Parquet => PositionDeleteFileFormat::Parquet,
                iceberg::spec::DataFileFormat::Puffin => PositionDeleteFileFormat::Puffin,
                other => {
                    return Err(format!(
                        "ivm-a1 delta-scan payload: position-delete file {} has unsupported \
                         file_format {:?}; only Parquet and Puffin are supported",
                        del.delete_file_path, other
                    ));
                }
            },
            content_offset: del.content_offset,
            content_size_in_bytes: del.content_size_in_bytes,
        });
    }
    if let Some(first) = position_deletes.first() {
        out.push(DeltaSourceFile {
            path: first.delete_file_path.clone(),
            size: 0,
            role: DeltaSourceRole::PositionDelete {
                deletes: position_deletes,
            },
            partition_spec_id: None,
            partition_key: None,
            first_row_id: None,
            data_sequence_number: None,
            row_id_allow_list: None,
        });
    }
    for eq in &batch.equality_deletes {
        out.push(DeltaSourceFile {
            path: eq.delete_file_path.clone(),
            size: eq.delete_file_size,
            role: DeltaSourceRole::EqualityDelete {
                equality_field_ids: eq.equality_ids.clone(),
                targets: equality_targets_by_delete_file
                    .get(&eq.delete_file_path)
                    .cloned()
                    .unwrap_or_default(),
            },
            partition_spec_id: eq.partition_spec_id,
            partition_key: eq.partition_key.clone(),
            first_row_id: None,
            data_sequence_number: eq.sequence_number,
            row_id_allow_list: None,
        });
    }
    for d in &batch.deleted_data_files {
        out.push(DeltaSourceFile {
            path: d.path.clone(),
            size: d.size,
            role: DeltaSourceRole::DeletedDataFile {
                previous_data_file_visibility: None,
            },
            partition_spec_id: d.partition_spec_id,
            partition_key: d.partition_key.clone(),
            first_row_id: d.first_row_id,
            data_sequence_number: d.data_sequence_number,
            row_id_allow_list: None,
        });
    }
    Ok(out)
}

pub(crate) fn equality_delete_targets_at(
    table: &iceberg::table::Table,
    snapshot_id: i64,
    equality_deletes: &[EqualityDeleteRef],
) -> Result<std::collections::HashMap<String, Vec<EqualityDeleteTargetData>>, String> {
    if equality_deletes.is_empty() {
        return Ok(std::collections::HashMap::new());
    }
    let read_snapshot =
        crate::connector::iceberg::read::build_read_snapshot_at(table, snapshot_id)?;
    let mut out = std::collections::HashMap::new();
    for delete in equality_deletes {
        let delete_file = equality_change_to_read_delete(delete);
        let targets = crate::connector::iceberg::read::data_files_matching_delete(
            &read_snapshot,
            &delete_file,
        )
        .into_iter()
        .map(|data_file| EqualityDeleteTargetData {
            data_file_path: data_file.path.clone(),
            data_file_size: data_file.size,
            data_file_first_row_id: data_file.first_row_id,
            data_file_sequence_number: data_file.data_sequence_number,
        })
        .collect::<Vec<_>>();
        out.insert(delete.delete_file_path.clone(), targets);
    }
    Ok(out)
}

pub(crate) fn delete_visibility_data_files_at(
    table: &iceberg::table::Table,
    snapshot_id: i64,
) -> Result<Vec<DeleteVisibilityDataFileDescriptor>, String> {
    crate::connector::iceberg::catalog::registry::extract_data_files_with_stats_at(
        table,
        snapshot_id,
    )?
    .into_iter()
    .map(delete_visibility_data_file_from_stats)
    .collect()
}

fn delete_visibility_data_file_from_stats(
    file: crate::connector::iceberg::catalog::registry::DataFileWithStats,
) -> Result<DeleteVisibilityDataFileDescriptor, String> {
    let delete_files = file
        .delete_files
        .into_iter()
        .map(|delete| {
            let file_format = match delete.file_format {
                crate::connector::iceberg::scan_model::IcebergDeleteFileFormat::Parquet => {
                    DeleteVisibilityDeleteFileFormat::Parquet
                }
                crate::connector::iceberg::scan_model::IcebergDeleteFileFormat::Puffin => {
                    DeleteVisibilityDeleteFileFormat::Puffin
                }
            };
            let file_content = match delete.file_content {
                crate::connector::iceberg::scan_model::IcebergDeleteFileContent::Position => {
                    DeleteVisibilityDeleteFileContent::Position
                }
                crate::connector::iceberg::scan_model::IcebergDeleteFileContent::Equality => {
                    DeleteVisibilityDeleteFileContent::Equality
                }
            };
            Ok(DeleteVisibilityDeleteFileDescriptor {
                path: delete.path,
                file_format,
                file_content,
                length: delete.length,
                content_offset: delete.content_offset,
                content_size_in_bytes: delete.content_size_in_bytes,
            })
        })
        .collect::<Result<Vec<_>, String>>()?;
    Ok(DeleteVisibilityDataFileDescriptor {
        path: file.path,
        size: file.size,
        first_row_id: file.first_row_id,
        data_sequence_number: file.data_sequence_number,
        delete_files,
    })
}

/// Per-row Change action: this row got inserted or deleted relative to
/// the previous MV refresh state. Carried alongside the row contents
/// through the materialize-changes pipeline so the aggregate path can
/// One unit of work the file-collection phase needs to perform for a
/// single snapshot in the lineage. `Replace` snapshots are validated by
/// `classify_snapshot` itself and never produce a `LineageAction` —
/// they're silently absorbed once the validator passes.
//
// The `Collect*` prefix is intentional: each variant pairs a verb
// ("collect from this snapshot") with a noun describing what to collect.
// Renaming the variants to drop the prefix would make the call sites
// ambiguous (e.g. `LineageAction::Inserts` reads like a value rather than
// a unit of work). Suppress the corresponding clippy lint instead.
#[allow(clippy::enum_variant_names)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum LineageAction {
    /// Walk the snapshot's data manifests, collect entries with
    /// `added_snapshot_id == this`, project to `DataFileRef`.
    CollectInserts { snapshot_id: i64 },
    /// Walk the snapshot's delete manifests, collect row-level delete
    /// files, and also collect any added data files from the same row-delta
    /// snapshot.
    CollectDeletes { snapshot_id: i64 },
    /// Walk the snapshot's data manifests and collect both added data files
    /// and deleted data files. This is the standard Iceberg representation
    /// of full-table overwrite and COW row updates.
    CollectOverwriteDiff { snapshot_id: i64 },
}

/// Output of `classify_lineage`: a chronologically-ordered list of
/// actions to execute against snapshots from `previous_snapshot_id`
/// (exclusive) to `current_snapshot_id` (inclusive). Replace snapshots
/// validated and skipped during classification do not appear here.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct LineagePlan {
    pub previous_snapshot_id: i64,
    pub current_snapshot_id: i64,
    pub actions: Vec<LineageAction>,
}

/// Pure per-snapshot decision. Returns:
/// - `Ok(Some(action))` when the snapshot contributes work to the file
///   collector,
/// - `Ok(None)` when the snapshot is a validated REPLACE compaction and
///   should be silently absorbed,
/// - `Err(ChangeError)` for REPLACE-validation failure, etc.
///
/// `parent` is required for REPLACE (the validator compares
/// `total-records` and `schema_id` against the parent). It can be
/// `None` for any other operation; passing `None` for REPLACE
/// produces a `ReplaceValidationFailed` error.
fn classify_snapshot(
    snapshot: &iceberg::spec::Snapshot,
    parent: Option<&iceberg::spec::Snapshot>,
) -> Result<Option<LineageAction>, ChangeError> {
    use iceberg::spec::Operation;
    let snapshot_id = snapshot.snapshot_id();
    match &snapshot.summary().operation {
        Operation::Append => Ok(Some(LineageAction::CollectInserts { snapshot_id })),
        Operation::Delete => Ok(Some(LineageAction::CollectDeletes { snapshot_id })),
        Operation::Replace => {
            let parent = parent.ok_or_else(|| ChangeError::ReplaceValidationFailed {
                snapshot_id,
                reason: "REPLACE snapshot has no parent reachable for compaction validation"
                    .to_string(),
            })?;
            validate_replace_snapshot(snapshot, parent)?;
            Ok(None)
        }
        Operation::Overwrite => Ok(Some(LineageAction::CollectOverwriteDiff { snapshot_id })),
    }
}

/// Validate that a `Replace` snapshot preserves logical data content.
///
/// Data-file compaction must report positive `added-data-files` and
/// `deleted-data-files` counters. Delete-file-only rewrites must report
/// explicit `0/0` data-file counters. Both forms must leave `total-records`
/// unchanged and keep the same schema id. Anything else is rejected.
fn validate_replace_snapshot(
    snapshot: &iceberg::spec::Snapshot,
    parent: &iceberg::spec::Snapshot,
) -> Result<(), ChangeError> {
    let snap_props = &snapshot.summary().additional_properties;
    let parent_props = &parent.summary().additional_properties;

    let snap_records = snap_props
        .get("total-records")
        .and_then(|s| s.parse::<i64>().ok());
    let parent_records = parent_props
        .get("total-records")
        .and_then(|s| s.parse::<i64>().ok());
    match (snap_records, parent_records) {
        (Some(a), Some(b)) if a == b => {}
        (Some(a), Some(b)) => {
            return Err(ChangeError::ReplaceValidationFailed {
                snapshot_id: snapshot.snapshot_id(),
                reason: format!("total-records changed across REPLACE: parent={b}, replace={a}"),
            });
        }
        _ => {
            return Err(ChangeError::ReplaceValidationFailed {
                snapshot_id: snapshot.snapshot_id(),
                reason:
                    "REPLACE snapshot summary is missing `total-records`; cannot prove compaction"
                        .to_string(),
            });
        }
    }

    let added = required_replace_summary_i64(snapshot, "added-data-files")?;
    let removed = required_replace_summary_i64(snapshot, "deleted-data-files")?;
    let data_file_compaction = added > 0 && removed > 0;
    let delete_file_only_rewrite = added == 0 && removed == 0;
    let zero_row_rewrite =
        matches!((snap_records, parent_records), (Some(0), Some(0))) && added == 0 && removed > 0;
    if !data_file_compaction && !delete_file_only_rewrite && !zero_row_rewrite {
        return Err(ChangeError::ReplaceValidationFailed {
            snapshot_id: snapshot.snapshot_id(),
            reason: format!(
                "REPLACE snapshot must report added-data-files/deleted-data-files as \
                 data-file compaction counts (>0/>0), delete-file-only no-op counts (0/0), \
                 or zero-row rewrite counts (0/>0); \
                 got added={added}, deleted={removed}"
            ),
        });
    }

    if snapshot.schema_id() != parent.schema_id() {
        return Err(ChangeError::ReplaceValidationFailed {
            snapshot_id: snapshot.snapshot_id(),
            reason: format!(
                "REPLACE snapshot schema-id {:?} differs from parent {:?}; schema evolution \
                 across compaction is not in scope",
                snapshot.schema_id(),
                parent.schema_id(),
            ),
        });
    }
    Ok(())
}

fn required_replace_summary_i64(
    snapshot: &iceberg::spec::Snapshot,
    key: &'static str,
) -> Result<i64, ChangeError> {
    let value = snapshot
        .summary()
        .additional_properties
        .get(key)
        .ok_or_else(|| ChangeError::ReplaceValidationFailed {
            snapshot_id: snapshot.snapshot_id(),
            reason: format!(
                "REPLACE snapshot summary is missing `{key}`; cannot prove compaction or delete-file-only no-op"
            ),
        })?;
    value
        .parse::<i64>()
        .map_err(|e| ChangeError::ReplaceValidationFailed {
            snapshot_id: snapshot.snapshot_id(),
            reason: format!("REPLACE snapshot summary `{key}` is invalid `{value}`: {e}"),
        })
}

/// Walk the explicit lineage range from `previous_snapshot_id` (exclusive) to
/// `current_snapshot_id` (inclusive), dispatching each node through
/// `classify_snapshot`. Performs no I/O.
///
/// Errors:
/// - `LineageBroken` when `current_snapshot_id` is missing, or when
///   `previous_snapshot_id` is not an ancestor of `current_snapshot_id`
///   (its metadata entry has been pruned, or the chain runs off its root).
/// - `UnsupportedOperation` / `ReplaceValidationFailed` propagated from
///   `classify_snapshot`.
pub(crate) fn classify_lineage(
    metadata: &iceberg::spec::TableMetadata,
    previous_snapshot_id: i64,
    current_snapshot_id: i64,
) -> Result<LineagePlan, ChangeError> {
    let current_snapshot =
        metadata
            .snapshot_by_id(current_snapshot_id)
            .ok_or(ChangeError::LineageBroken {
                previous_snapshot: previous_snapshot_id,
            })?;

    if current_snapshot_id == previous_snapshot_id {
        return Ok(LineagePlan {
            previous_snapshot_id,
            current_snapshot_id,
            actions: Vec::new(),
        });
    }

    if metadata.snapshot_by_id(previous_snapshot_id).is_none() {
        return Err(ChangeError::LineageBroken {
            previous_snapshot: previous_snapshot_id,
        });
    }

    let mut actions_reversed: Vec<LineageAction> = Vec::new();
    let mut current = current_snapshot;
    loop {
        let snapshot = current.as_ref();
        let parent_id = snapshot.parent_snapshot_id();
        let parent = parent_id
            .and_then(|id| metadata.snapshot_by_id(id))
            .map(|sr| sr.as_ref());

        if let Some(action) = classify_snapshot(snapshot, parent)? {
            actions_reversed.push(action);
        }

        match parent_id {
            Some(id) if id == previous_snapshot_id => break,
            Some(id) => {
                current = metadata
                    .snapshot_by_id(id)
                    .ok_or(ChangeError::LineageBroken {
                        previous_snapshot: previous_snapshot_id,
                    })?;
            }
            None => {
                // Walked off the root without finding previous_snapshot_id.
                return Err(ChangeError::LineageBroken {
                    previous_snapshot: previous_snapshot_id,
                });
            }
        }
    }

    actions_reversed.reverse();
    Ok(LineagePlan {
        previous_snapshot_id,
        current_snapshot_id,
        actions: actions_reversed,
    })
}

/// Public entrypoint for snapshot-lineage change planning. Walks the
/// lineage from `previous_snapshot_id` (exclusive) to `to_snapshot_id`
/// (inclusive). When `to_snapshot_id` is `None`, defaults to the table's
/// current snapshot (preserves legacy behavior).
///
/// The returned `IcebergChangeBatch.current_snapshot_id` field reflects
/// the *resolved* to_snapshot_id (i.e. the actual right endpoint of the
/// walked lineage), which may differ from `table.metadata().current_snapshot()`
/// when the caller pins to a historical snapshot.
///
/// The `_pk_columns` parameter is reserved for future delete-side row-id
/// computation; snapshot lineage planning itself does not need it yet.
pub(crate) fn plan_changes(
    table: &iceberg::table::Table,
    previous_snapshot_id: i64,
    to_snapshot_id: Option<i64>,
    _pk_columns: &[String],
) -> Result<IcebergChangeBatch, ChangeError> {
    let metadata = table.metadata();
    let current_snapshot_id = match to_snapshot_id {
        Some(id) => id,
        None => metadata
            .current_snapshot()
            .map(|s| s.snapshot_id())
            .ok_or_else(|| {
                ChangeError::InternalInconsistency(
                    "plan_changes: table has no current snapshot".to_string(),
                )
            })?,
    };

    let plan = classify_lineage(metadata, previous_snapshot_id, current_snapshot_id)?;
    if plan.actions.is_empty() {
        return Ok(IcebergChangeBatch {
            previous_snapshot_id,
            current_snapshot_id,
            inserts: Vec::new(),
            deletes: Vec::new(),
            equality_deletes: Vec::new(),
            deleted_data_files: Vec::new(),
        });
    }

    let file_io = table.file_io();
    let collect = collect_files(metadata, file_io, &plan.actions);
    let (inserts, deletes, equality_deletes, deleted_data_files) =
        crate::connector::iceberg::catalog::registry::block_on_iceberg(collect).map_err(
            |e| ChangeError::InternalInconsistency(format!("plan_changes runtime: {e}")),
        )??;

    Ok(IcebergChangeBatch {
        previous_snapshot_id,
        current_snapshot_id,
        inserts,
        deletes,
        equality_deletes,
        deleted_data_files,
    })
}

/// Helper for `IcebergDeltaScanOperator`: scan one position-delete file
/// and reverse-project deleted rows from its target data file(s).
///
/// Returns rows with the same projection as a regular base-table scan
/// plus all four Iceberg v3 row-lineage virtual columns
/// (`_file`, `_pos`, `_row_id`, `_last_updated_sequence_number`) so the
/// downstream codegen tuple descriptor's slot count matches. Each row has
/// not yet had `__change_op` injected — the operator will add it.
#[allow(dead_code)]
pub(crate) fn scan_position_delete_rows_for_targets(
    deletes: &[PositionDeleteRef],
    base_data_file_lineage: &std::collections::HashMap<
        String,
        crate::connector::iceberg::delta::BaseDataFileLineage,
    >,
    suppressed_data_files: &std::collections::HashSet<String>,
    previously_deleted_positions_per_file: &std::collections::HashMap<
        String,
        roaring::RoaringTreemap,
    >,
    factory: &novarocks_fs::FsAccessHandle,
    object_store_config: Option<&novarocks_fs::ObjectStoreConfig>,
    expected_object_store_bucket: Option<&str>,
) -> Result<Vec<arrow::record_batch::RecordBatch>, String> {
    let size_lookup = |_path: &str| -> Option<u64> { None };
    crate::connector::iceberg::scan_deletes::scan_deletes_with_lineage_lookup_and_path_normalizer(
        deletes,
        factory,
        size_lookup,
        |path| base_data_file_lineage.get(path).copied(),
        suppressed_data_files,
        previously_deleted_positions_per_file,
        |path| {
            normalize_delete_projection_path(
                path,
                object_store_config,
                expected_object_store_bucket,
            )
        },
    )
    .map_err(|e| e.to_string())
}

/// Helper for `IcebergDeltaScanOperator`: scan one equality-delete file
/// and reverse-project the matching rows from its target data file(s),
/// emitting all four Iceberg v3 row-lineage virtual columns
/// (`_file`, `_pos`, `_row_id`, `_last_updated_sequence_number`) so the
/// downstream codegen tuple descriptor's slot count matches.
#[allow(dead_code)]
pub(crate) fn scan_equality_delete_rows_for_one_with_v3_lineage(
    base_table: &iceberg::table::Table,
    delete: &EqualityDeleteRef,
    factory: &novarocks_fs::FsAccessHandle,
    object_store_config: Option<&novarocks_fs::ObjectStoreConfig>,
) -> Result<Vec<arrow::record_batch::RecordBatch>, String> {
    scan_equality_delete_rows_for_table_with_v3_lineage(
        base_table,
        std::slice::from_ref(delete),
        factory,
        object_store_config,
    )
}

/// Snapshot-pinned variant of
/// `scan_equality_delete_rows_for_one_with_v3_lineage`.
#[allow(dead_code)]
pub(crate) fn scan_equality_delete_rows_for_one_with_v3_lineage_at(
    base_table: &iceberg::table::Table,
    delete: &EqualityDeleteRef,
    snapshot_id: i64,
    factory: &novarocks_fs::FsAccessHandle,
    object_store_config: Option<&novarocks_fs::ObjectStoreConfig>,
) -> Result<Vec<arrow::record_batch::RecordBatch>, String> {
    scan_equality_delete_rows_for_table_with_v3_lineage_at(
        base_table,
        std::slice::from_ref(delete),
        snapshot_id,
        factory,
        object_store_config,
    )
}

pub(crate) fn scan_equality_delete_rows_for_targets_with_v3_lineage(
    delete: &EqualityDeleteRef,
    targets: &[EqualityDeleteTargetData],
    factory: &novarocks_fs::FsAccessHandle,
    object_store_config: Option<&novarocks_fs::ObjectStoreConfig>,
    expected_object_store_bucket: Option<&str>,
) -> Result<Vec<arrow::record_batch::RecordBatch>, String> {
    if targets.is_empty() {
        return Ok(Vec::new());
    }
    let delete_specs = vec![equality_change_to_delete_spec(
        delete,
        object_store_config,
        expected_object_store_bucket,
    )?];
    let sets = crate::connector::iceberg::equality_delete::load_equality_delete_sets(
        &delete_specs,
        factory,
    )?;
    let mut out = Vec::new();
    for target in targets {
        let first_row_id = target.data_file_first_row_id.ok_or_else(|| {
            format!(
                "iceberg MV equality-delete reverse projection requires first_row_id for data file {}; rebuild the MV after enabling Iceberg v3 row-lineage metadata",
                target.data_file_path
            )
        })?;
        let data_sequence_number = target.data_file_sequence_number.ok_or_else(|| {
            format!(
                "iceberg MV equality-delete reverse projection requires data_sequence_number for data file {}; rebuild the MV after enabling Iceberg v3 row-lineage metadata",
                target.data_file_path
            )
        })?;
        out.extend(read_data_file_matching_equality_deletes_with_v3_lineage(
            &target.data_file_path,
            u64::try_from(target.data_file_size).ok(),
            &sets,
            first_row_id,
            data_sequence_number,
            factory,
            |path| {
                normalize_delete_projection_path(
                    path,
                    object_store_config,
                    expected_object_store_bucket,
                )
                .map_err(|e| e.to_string())
            },
        )?);
    }
    Ok(out)
}

/// Helper for `IcebergDeltaScanOperator`: scan one freshly-added data file
/// (snapshot diff INSERT side). Returns raw rows with the base-table physical
/// projection. `__change_op` is injected by the operator.
#[allow(dead_code)]
pub(crate) fn scan_one_added_data_file(
    path: &str,
    size: i64,
    base_table: &iceberg::table::Table,
    object_store_config: Option<&novarocks_fs::ObjectStoreConfig>,
) -> Result<Vec<arrow::record_batch::RecordBatch>, String> {
    let factory = build_factory_for_table(base_table, object_store_config)?;
    let expected_bucket = expected_object_store_bucket_for_table(base_table)?;
    scan_one_added_data_file_with_factory(
        path,
        size,
        &factory,
        object_store_config,
        expected_bucket.as_deref(),
    )
}

pub(crate) fn scan_one_added_data_file_with_factory(
    path: &str,
    size: i64,
    factory: &novarocks_fs::FsAccessHandle,
    object_store_config: Option<&novarocks_fs::ObjectStoreConfig>,
    expected_object_store_bucket: Option<&str>,
) -> Result<Vec<arrow::record_batch::RecordBatch>, String> {
    // Validate the file against the configured storage domain, but preserve
    // its logical URI for `FsAccessHandle::bind_location`. The normalized
    // object-store path is operator-relative and would otherwise parse as a
    // local filesystem path at the binding boundary.
    normalize_delete_projection_path(path, object_store_config, expected_object_store_bucket)
        .map_err(|e| format!("normalize added data file `{path}`: {e}"))?;
    let len = u64::try_from(size).ok();
    read_full_data_file(path, len, factory)
}

/// Helper for `IcebergDeltaScanOperator`: scan one deleted data file
/// (i.e., a file that was present at previous_snapshot and removed in
/// current snapshot). Returns the live rows from that file at the previous
/// snapshot, applying the previous-visibility delete mask. Emits all four
/// Iceberg v3 row-lineage virtual columns so the downstream codegen tuple
/// descriptor's slot count matches.
#[allow(dead_code)]
pub(crate) fn scan_one_deleted_data_file(
    base_table: &iceberg::table::Table,
    deleted_file: &DeletedDataFileRef,
    object_store_config: Option<&novarocks_fs::ObjectStoreConfig>,
    previous_delete_visibility: &crate::connector::iceberg::delete_visibility::ExistingDeleteVisibilityByDataFile,
) -> Result<Vec<arrow::record_batch::RecordBatch>, String> {
    let factory = build_factory_for_table(base_table, object_store_config)?;
    let expected_bucket = expected_object_store_bucket_for_table(base_table)?;
    scan_one_deleted_data_file_with_factory(
        deleted_file,
        &factory,
        object_store_config,
        expected_bucket.as_deref(),
        previous_delete_visibility,
    )
}

pub(crate) fn scan_one_deleted_data_file_with_factory(
    deleted_file: &DeletedDataFileRef,
    factory: &novarocks_fs::FsAccessHandle,
    object_store_config: Option<&novarocks_fs::ObjectStoreConfig>,
    expected_object_store_bucket: Option<&str>,
    previous_delete_visibility: &crate::connector::iceberg::delete_visibility::ExistingDeleteVisibilityByDataFile,
) -> Result<Vec<arrow::record_batch::RecordBatch>, String> {
    scan_deleted_data_file_rows_with_visibility_and_v3_lineage(
        std::slice::from_ref(deleted_file),
        factory,
        object_store_config,
        expected_object_store_bucket,
        previous_delete_visibility,
    )
}

#[allow(dead_code)]
pub(crate) fn scan_equality_delete_rows_for_table(
    table: &iceberg::table::Table,
    equality_deletes: &[EqualityDeleteRef],
    factory: &novarocks_fs::FsAccessHandle,
    object_store_config: Option<&novarocks_fs::ObjectStoreConfig>,
) -> Result<Vec<arrow::record_batch::RecordBatch>, String> {
    if equality_deletes.is_empty() {
        return Ok(Vec::new());
    }
    let read_snapshot = crate::connector::iceberg::read::build_read_snapshot(table)?;
    let expected_bucket = expected_object_store_bucket_for_table(table)?;
    scan_equality_delete_rows_for_snapshot(
        &read_snapshot,
        equality_deletes,
        factory,
        object_store_config,
        expected_bucket.as_deref(),
    )
}

pub(crate) fn scan_equality_delete_rows_for_table_at(
    table: &iceberg::table::Table,
    equality_deletes: &[EqualityDeleteRef],
    snapshot_id: i64,
    factory: &novarocks_fs::FsAccessHandle,
    object_store_config: Option<&novarocks_fs::ObjectStoreConfig>,
) -> Result<Vec<arrow::record_batch::RecordBatch>, String> {
    if equality_deletes.is_empty() {
        return Ok(Vec::new());
    }
    let read_snapshot =
        crate::connector::iceberg::read::build_read_snapshot_at(table, snapshot_id)?;
    let expected_bucket = expected_object_store_bucket_for_table(table)?;
    scan_equality_delete_rows_for_snapshot(
        &read_snapshot,
        equality_deletes,
        factory,
        object_store_config,
        expected_bucket.as_deref(),
    )
}

fn scan_equality_delete_rows_for_snapshot(
    read_snapshot: &crate::connector::iceberg::read::IcebergReadSnapshot,
    equality_deletes: &[EqualityDeleteRef],
    factory: &novarocks_fs::FsAccessHandle,
    object_store_config: Option<&novarocks_fs::ObjectStoreConfig>,
    expected_object_store_bucket: Option<&str>,
) -> Result<Vec<arrow::record_batch::RecordBatch>, String> {
    let mut out = Vec::new();
    for delete in equality_deletes {
        let delete_file = equality_change_to_read_delete(delete);
        let delete_specs = vec![equality_change_to_delete_spec(
            delete,
            object_store_config,
            expected_object_store_bucket,
        )?];
        let sets = crate::connector::iceberg::equality_delete::load_equality_delete_sets(
            &delete_specs,
            factory,
        )?;
        for data_file in crate::connector::iceberg::read::data_files_matching_delete(
            &read_snapshot,
            &delete_file,
        ) {
            // first_row_id is only present on Iceberg v3 row-lineage tables.
            // For v2 base tables we still need the equality-delete reverse
            // projection to power aggregate / projection MV retraction —
            // mirror the position-delete v2 path (no `_row_id` column added).
            out.extend(read_data_file_matching_equality_deletes_with_base_row_id(
                &data_file.path,
                u64::try_from(data_file.size).ok(),
                &sets,
                data_file.first_row_id,
                factory,
                |path| {
                    normalize_delete_projection_path(
                        path,
                        object_store_config,
                        expected_object_store_bucket,
                    )
                    .map_err(|e| e.to_string())
                },
            )?);
        }
    }
    Ok(out)
}

/// IVM-A1 variant of `scan_equality_delete_rows_for_table` that emits the
/// full Iceberg v3 row-lineage virtual column set on each batch.
pub(crate) fn scan_equality_delete_rows_for_table_with_v3_lineage(
    table: &iceberg::table::Table,
    equality_deletes: &[EqualityDeleteRef],
    factory: &novarocks_fs::FsAccessHandle,
    object_store_config: Option<&novarocks_fs::ObjectStoreConfig>,
) -> Result<Vec<arrow::record_batch::RecordBatch>, String> {
    if equality_deletes.is_empty() {
        return Ok(Vec::new());
    }
    let read_snapshot = crate::connector::iceberg::read::build_read_snapshot(table)?;
    let expected_bucket = expected_object_store_bucket_for_table(table)?;
    scan_equality_delete_rows_for_snapshot_with_v3_lineage(
        &read_snapshot,
        equality_deletes,
        factory,
        object_store_config,
        expected_bucket.as_deref(),
    )
}

/// Snapshot-pinned variant of
/// `scan_equality_delete_rows_for_table_with_v3_lineage`.
#[allow(dead_code)]
pub(crate) fn scan_equality_delete_rows_for_table_with_v3_lineage_at(
    table: &iceberg::table::Table,
    equality_deletes: &[EqualityDeleteRef],
    snapshot_id: i64,
    factory: &novarocks_fs::FsAccessHandle,
    object_store_config: Option<&novarocks_fs::ObjectStoreConfig>,
) -> Result<Vec<arrow::record_batch::RecordBatch>, String> {
    if equality_deletes.is_empty() {
        return Ok(Vec::new());
    }
    let read_snapshot =
        crate::connector::iceberg::read::build_read_snapshot_at(table, snapshot_id)?;
    let expected_bucket = expected_object_store_bucket_for_table(table)?;
    scan_equality_delete_rows_for_snapshot_with_v3_lineage(
        &read_snapshot,
        equality_deletes,
        factory,
        object_store_config,
        expected_bucket.as_deref(),
    )
}

fn scan_equality_delete_rows_for_snapshot_with_v3_lineage(
    read_snapshot: &crate::connector::iceberg::read::IcebergReadSnapshot,
    equality_deletes: &[EqualityDeleteRef],
    factory: &novarocks_fs::FsAccessHandle,
    object_store_config: Option<&novarocks_fs::ObjectStoreConfig>,
    expected_object_store_bucket: Option<&str>,
) -> Result<Vec<arrow::record_batch::RecordBatch>, String> {
    let mut out = Vec::new();
    for delete in equality_deletes {
        let delete_file = equality_change_to_read_delete(delete);
        let delete_specs = vec![equality_change_to_delete_spec(
            delete,
            object_store_config,
            expected_object_store_bucket,
        )?];
        let sets = crate::connector::iceberg::equality_delete::load_equality_delete_sets(
            &delete_specs,
            factory,
        )?;
        for data_file in crate::connector::iceberg::read::data_files_matching_delete(
            &read_snapshot,
            &delete_file,
        ) {
            let first_row_id = data_file.first_row_id.ok_or_else(|| {
                format!(
                    "iceberg MV equality-delete reverse projection requires first_row_id for data file {}; rebuild the MV after enabling Iceberg v3 row-lineage metadata",
                    data_file.path
                )
            })?;
            let data_sequence_number = data_file.data_sequence_number.ok_or_else(|| {
                format!(
                    "iceberg MV equality-delete reverse projection requires data_sequence_number for data file {}; rebuild the MV after enabling Iceberg v3 row-lineage metadata",
                    data_file.path
                )
            })?;
            out.extend(read_data_file_matching_equality_deletes_with_v3_lineage(
                &data_file.path,
                u64::try_from(data_file.size).ok(),
                &sets,
                first_row_id,
                data_sequence_number,
                factory,
                |path| {
                    normalize_delete_projection_path(
                        path,
                        object_store_config,
                        expected_object_store_bucket,
                    )
                    .map_err(|e| e.to_string())
                },
            )?);
        }
    }
    Ok(out)
}

#[allow(clippy::too_many_arguments)]
fn read_data_file_matching_equality_deletes_with_v3_lineage<N>(
    data_file_path: &str,
    data_file_size: Option<u64>,
    sets: &[crate::connector::iceberg::equality_delete::EqualityDeleteSet],
    first_row_id: i64,
    data_sequence_number: i64,
    factory: &novarocks_fs::FsAccessHandle,
    normalize_path: N,
) -> Result<Vec<arrow::record_batch::RecordBatch>, String>
where
    N: Fn(&str) -> Result<String, String>,
{
    use arrow::array::BooleanArray;
    use arrow::compute::filter_record_batch;

    if sets.is_empty() {
        return Ok(Vec::new());
    }

    let normalized_path = normalize_path(data_file_path)?;
    let batches = crate::connector::file_execution::read_foundation_parquet_batches(
        factory,
        &normalized_path,
        data_file_size,
        novarocks_fs::FileProjection::All,
    )?;

    let mut out = Vec::new();
    let mut next_position = 0_u64;
    for batch in batches {
        let row_count = batch.num_rows();
        let Some(keep_mask) =
            crate::connector::iceberg::equality_delete::equality_delete_keep_mask(&batch, sets)?
        else {
            next_position = next_position.checked_add(row_count as u64).ok_or_else(|| {
                format!(
                    "row position overflow while scanning equality deletes for {data_file_path}"
                )
            })?;
            continue;
        };

        let mut matched_positions = Vec::new();
        let match_mask = BooleanArray::from(
            keep_mask
                .iter()
                .enumerate()
                .map(|(idx, keep)| {
                    let matched = !*keep;
                    if matched {
                        matched_positions.push(next_position + idx as u64);
                    }
                    matched
                })
                .collect::<Vec<_>>(),
        );
        let filtered = filter_record_batch(&batch, &match_mask).map_err(|e| {
            format!(
                "filter iceberg data file {data_file_path} for equality-delete v3-lineage reverse projection failed: {e}"
            )
        })?;
        if filtered.num_rows() > 0 {
            out.push(
                crate::connector::iceberg::scan_deletes::append_iceberg_v3_row_lineage_columns(
                    &filtered,
                    data_file_path,
                    &matched_positions,
                    first_row_id,
                    data_sequence_number,
                )
                .map_err(|e| e.to_string())?,
            );
        }
        next_position = next_position.checked_add(row_count as u64).ok_or_else(|| {
            format!("row position overflow while scanning equality deletes for {data_file_path}")
        })?;
    }
    Ok(out)
}

fn read_data_file_matching_equality_deletes_with_base_row_id<N>(
    data_file_path: &str,
    data_file_size: Option<u64>,
    sets: &[crate::connector::iceberg::equality_delete::EqualityDeleteSet],
    first_row_id: Option<i64>,
    factory: &novarocks_fs::FsAccessHandle,
    normalize_path: N,
) -> Result<Vec<arrow::record_batch::RecordBatch>, String>
where
    N: Fn(&str) -> Result<String, String>,
{
    use arrow::array::BooleanArray;
    use arrow::compute::filter_record_batch;

    if sets.is_empty() {
        return Ok(Vec::new());
    }

    let normalized_path = normalize_path(data_file_path)?;
    let batches = crate::connector::file_execution::read_foundation_parquet_batches(
        factory,
        &normalized_path,
        data_file_size,
        novarocks_fs::FileProjection::All,
    )?;

    let mut out = Vec::new();
    let mut next_position = 0_u64;
    for batch in batches {
        let row_count = batch.num_rows();
        let Some(keep_mask) =
            crate::connector::iceberg::equality_delete::equality_delete_keep_mask(&batch, sets)?
        else {
            next_position = next_position.checked_add(row_count as u64).ok_or_else(|| {
                format!(
                    "row position overflow while scanning equality deletes for {data_file_path}"
                )
            })?;
            continue;
        };

        let mut matched_positions = Vec::new();
        let match_mask = BooleanArray::from(
            keep_mask
                .iter()
                .enumerate()
                .map(|(idx, keep)| {
                    let matched = !*keep;
                    if matched {
                        matched_positions.push(next_position + idx as u64);
                    }
                    matched
                })
                .collect::<Vec<_>>(),
        );
        let filtered = filter_record_batch(&batch, &match_mask).map_err(|e| {
            format!(
                "filter iceberg data file {data_file_path} for equality-delete row-id reverse projection failed: {e}"
            )
        })?;
        if filtered.num_rows() > 0 {
            // v3 row-lineage tables carry first_row_id, so we synthesise the
            // `_row_id` virtual column for downstream IVM. v2 tables omit it —
            // matching the position-delete v2 path, where the projection just
            // emits the matched data columns.
            let projected = match first_row_id {
                Some(first_row_id) => {
                    crate::connector::iceberg::scan_deletes::append_base_row_id_column(
                        &filtered,
                        first_row_id,
                        &matched_positions,
                    )
                    .map_err(|e| e.to_string())?
                }
                None => filtered,
            };
            out.push(projected);
        }
        next_position = next_position.checked_add(row_count as u64).ok_or_else(|| {
            format!("row position overflow while scanning equality deletes for {data_file_path}")
        })?;
    }
    Ok(out)
}

/// Build a path -> v3 row-lineage index over a specific base table snapshot.
/// Used while planning the typed delta-scan payload to look up
/// `first_row_id` and `data_sequence_number` for each target data file
/// referenced by a position/equality/deleted-data-file role,
/// so the operator can synthesize the four v3 row-lineage virtual columns
/// (`_file`, `_pos`, `_row_id`, `_last_updated_sequence_number`).
pub(crate) fn base_data_file_lineage_index_at(
    table: &iceberg::table::Table,
    snapshot_id: i64,
) -> Result<
    std::collections::HashMap<String, crate::connector::iceberg::delta::BaseDataFileLineage>,
    String,
> {
    let read_snapshot =
        crate::connector::iceberg::read::build_read_snapshot_at(table, snapshot_id)?;
    build_data_file_lineage_index_from_snapshot(&read_snapshot)
}

/// Build a (file_path -> first_row_id / data_sequence_number) index from the
/// data files alive in a specific snapshot. Distinct from
/// `base_data_file_lineage_index_at` at the delta-scan upper endpoint: this
/// is used to look up the original `first_row_id` of a file that was
/// OVERWRITE-deleted between the MV's previous-refresh snapshot and the
/// planned upper snapshot. The deleted file is no longer alive at that upper
/// endpoint, but it WAS alive at `previous_snapshot_id`, where its
/// first_row_id is faithfully readable via iceberg-rust per-manifest
/// inheritance.
pub(crate) fn previous_snapshot_data_file_lineage_index(
    table: &iceberg::table::Table,
    snapshot_id: i64,
) -> Result<
    std::collections::HashMap<String, crate::connector::iceberg::delta::BaseDataFileLineage>,
    String,
> {
    base_data_file_lineage_index_at(table, snapshot_id)
}

fn build_data_file_lineage_index_from_snapshot(
    read_snapshot: &crate::connector::iceberg::read::IcebergReadSnapshot,
) -> Result<
    std::collections::HashMap<String, crate::connector::iceberg::delta::BaseDataFileLineage>,
    String,
> {
    let mut out = std::collections::HashMap::new();
    for file in &read_snapshot.files {
        let first_row_id = file.first_row_id.ok_or_else(|| {
            format!(
                "iceberg MV delete reverse projection requires first_row_id for data file {}; rebuild the MV after enabling Iceberg v3 row-lineage metadata",
                file.path
            )
        })?;
        let data_sequence_number = file.data_sequence_number.ok_or_else(|| {
            format!(
                "iceberg MV delete reverse projection requires data_sequence_number for data file {}; rebuild the MV after enabling Iceberg v3 row-lineage metadata",
                file.path
            )
        })?;
        out.insert(
            file.path.clone(),
            crate::connector::iceberg::delta::BaseDataFileLineage {
                first_row_id,
                data_sequence_number,
            },
        );
    }
    Ok(out)
}

fn equality_change_to_read_delete(
    delete: &EqualityDeleteRef,
) -> crate::connector::iceberg::read::IcebergReadDeleteFile {
    crate::connector::iceberg::read::IcebergReadDeleteFile {
        path: delete.delete_file_path.clone(),
        file_format: crate::connector::iceberg::read::IcebergReadDeleteFormat::Parquet,
        kind: crate::connector::iceberg::read::IcebergReadDeleteKind::Equality {
            equality_field_ids: delete.equality_ids.clone(),
        },
        length: Some(delete.delete_file_size),
        content_offset: None,
        content_size_in_bytes: None,
        sequence_number: delete.sequence_number,
        partition_spec_id: delete.partition_spec_id,
        partition_key: delete.partition_key.clone(),
        referenced_data_file: None,
    }
}

fn equality_change_to_delete_spec(
    delete: &EqualityDeleteRef,
    object_store_config: Option<&novarocks_fs::ObjectStoreConfig>,
    expected_object_store_bucket: Option<&str>,
) -> Result<crate::connector::iceberg::delete_file::IcebergDeleteFileSpec, String> {
    Ok(
        crate::connector::iceberg::delete_file::IcebergDeleteFileSpec {
            path: normalize_delete_projection_path(
                &delete.delete_file_path,
                object_store_config,
                expected_object_store_bucket,
            )
            .map_err(|e| e.to_string())?,
            file_format: crate::connector::iceberg::delete_file::IcebergFileFormat::Parquet,
            file_content:
                crate::connector::iceberg::delete_file::IcebergFileContent::EqualityDeletes,
            length: if delete.delete_file_size > 0 {
                Some(delete.delete_file_size as u64)
            } else {
                None
            },
            content_offset: None,
            content_size_in_bytes: None,
        },
    )
}

/// Read every data file removed by an overwrite snapshot and return its row
/// content as `RecordBatch`es. StarRocks table IVM (`ivm_delta_source.rs`)
/// feeds these into a temp parquet table the MV SELECT then reads from.
/// Iceberg-MV IVM (Task 13 onward) uses the lineage-bearing
/// `scan_deleted_data_file_rows_with_visibility_and_v3_lineage` variant
/// instead.
pub(crate) fn scan_deleted_data_file_rows(
    base_table: &iceberg::table::Table,
    deleted_data_files: &[DeletedDataFileRef],
    object_store_config: Option<&novarocks_fs::ObjectStoreConfig>,
) -> Result<Vec<arrow::record_batch::RecordBatch>, String> {
    if deleted_data_files.is_empty() {
        return Ok(Vec::new());
    }
    let factory = build_factory_for_table(base_table, object_store_config)?;
    let expected_bucket = expected_object_store_bucket_for_table(base_table)?;

    scan_deleted_data_file_rows_with_factory(deleted_data_files, &factory, |path| {
        normalize_delete_projection_path(path, object_store_config, expected_bucket.as_deref())
    })
    .map_err(|e| e.to_string())
}

/// IVM-A1 variant of `scan_deleted_data_file_rows_with_visibility` that
/// emits the full Iceberg v3 row-lineage virtual column set on each batch.
fn scan_deleted_data_file_rows_with_visibility_and_v3_lineage(
    deleted_data_files: &[DeletedDataFileRef],
    factory: &novarocks_fs::FsAccessHandle,
    object_store_config: Option<&novarocks_fs::ObjectStoreConfig>,
    expected_object_store_bucket: Option<&str>,
    existing_deletes_by_file: &crate::connector::iceberg::delete_visibility::ExistingDeleteVisibilityByDataFile,
) -> Result<Vec<arrow::record_batch::RecordBatch>, String> {
    if deleted_data_files.is_empty() {
        return Ok(Vec::new());
    }
    let mut out = Vec::new();
    // Group by logical path (in iceberg manifests the same path could occur
    // in multiple `DeletedDataFileRef`s if a follow-up snapshot also touched
    // the file; defensive de-dup keeps lineage synthesis deterministic).
    let mut seen: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    for file in deleted_data_files {
        if !seen.insert(file.path.clone()) {
            continue;
        }
        let first_row_id = file.first_row_id.ok_or_else(|| {
            format!(
                "iceberg MV deleted-data-file reverse projection requires first_row_id for {}; \
                 rebuild the MV after enabling Iceberg v3 row-lineage metadata",
                file.path
            )
        })?;
        let data_sequence_number = file.data_sequence_number.ok_or_else(|| {
            format!(
                "iceberg MV deleted-data-file reverse projection requires data_sequence_number for {}; \
                 rebuild the MV after enabling Iceberg v3 row-lineage metadata",
                file.path
            )
        })?;
        let normalized = normalize_delete_projection_path(
            &file.path,
            object_store_config,
            expected_object_store_bucket,
        )
        .map_err(|e| format!("normalize deleted data file `{}`: {e}", file.path))?;
        let size = u64::try_from(file.size).ok();
        let batches = read_full_data_file_with_v3_lineage_and_visibility(
            &file.path,
            &normalized,
            size,
            first_row_id,
            data_sequence_number,
            factory,
            Some(existing_deletes_by_file),
        )?;
        out.extend(batches);
    }
    Ok(out)
}

fn read_full_data_file_with_v3_lineage_and_visibility(
    logical_path: &str,
    path: &str,
    size: Option<u64>,
    first_row_id: i64,
    data_sequence_number: i64,
    factory: &novarocks_fs::FsAccessHandle,
    existing_deletes_by_file: Option<
        &crate::connector::iceberg::delete_visibility::ExistingDeleteVisibilityByDataFile,
    >,
) -> Result<Vec<arrow::record_batch::RecordBatch>, String> {
    use arrow::array::BooleanArray;
    use arrow::compute::filter_record_batch;

    let batches = read_full_data_file(path, size, factory)?;
    let mut out = Vec::with_capacity(batches.len());
    let mut next_position = 0_u64;
    for batch in batches {
        let end = next_position
            .checked_add(batch.num_rows() as u64)
            .ok_or_else(|| format!("row position overflow while scanning deleted file {path}"))?;
        let mut positions = Vec::with_capacity(batch.num_rows());
        let mut keep = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            let position = next_position.checked_add(row as u64).ok_or_else(|| {
                format!("row position overflow while scanning deleted file {path}")
            })?;
            let visible = match existing_deletes_by_file {
                Some(deletes) => {
                    let row_position = i64::try_from(position).map_err(|_| {
                        format!("row position {position} is too large for deleted file {path}")
                    })?;
                    crate::connector::iceberg::delete_visibility::data_file_row_is_visible(
                        &batch,
                        row,
                        logical_path,
                        row_position,
                        deletes,
                    )?
                }
                None => true,
            };
            keep.push(visible);
            if visible {
                positions.push(position);
            }
        }
        if positions.is_empty() {
            next_position = end;
            continue;
        }
        let filtered = if positions.len() == batch.num_rows() {
            batch
        } else {
            filter_record_batch(&batch, &BooleanArray::from(keep)).map_err(|e| {
                format!("filter deleted data file {logical_path} by previous delete visibility failed: {e}")
            })?
        };
        let enriched =
            crate::connector::iceberg::scan_deletes::append_iceberg_v3_row_lineage_columns(
                &filtered,
                logical_path,
                &positions,
                first_row_id,
                data_sequence_number,
            )
            .map_err(|e| e.to_string())?;
        out.push(enriched);
        next_position = end;
    }
    Ok(out)
}

pub(crate) fn scan_deleted_data_file_rows_with_factory<N>(
    deleted_data_files: &[DeletedDataFileRef],
    factory: &novarocks_fs::FsAccessHandle,
    normalize_path: N,
) -> Result<Vec<arrow::record_batch::RecordBatch>, ChangeError>
where
    N: Fn(&str) -> Result<String, ChangeError>,
{
    scan_deleted_data_file_rows_with_factory_and_visibility(
        deleted_data_files,
        factory,
        normalize_path,
        None,
    )
}

fn scan_deleted_data_file_rows_with_factory_and_visibility<N>(
    deleted_data_files: &[DeletedDataFileRef],
    factory: &novarocks_fs::FsAccessHandle,
    normalize_path: N,
    existing_deletes_by_file: Option<
        &crate::connector::iceberg::delete_visibility::ExistingDeleteVisibilityByDataFile,
    >,
) -> Result<Vec<arrow::record_batch::RecordBatch>, ChangeError>
where
    N: Fn(&str) -> Result<String, ChangeError>,
{
    let mut old_paths: std::collections::BTreeMap<String, (Option<u64>, i64)> =
        std::collections::BTreeMap::new();
    for file in deleted_data_files {
        let first_row_id = file.first_row_id.ok_or_else(|| {
            ChangeError::InternalInconsistency(format!(
                "iceberg MV deleted-data-file reverse projection requires first_row_id for {}; rebuild the MV after enabling Iceberg v3 row-lineage metadata",
                file.path
            ))
        })?;
        old_paths
            .entry(file.path.clone())
            .or_insert_with(|| (u64::try_from(file.size).ok(), first_row_id));
    }

    let mut out = Vec::new();
    for (path, (size, first_row_id)) in old_paths {
        let normalized = normalize_path(&path).map_err(|e| {
            ChangeError::InternalInconsistency(format!("normalize deleted data file `{path}`: {e}"))
        })?;
        let batches = read_full_data_file_with_base_row_id_and_visibility(
            &path,
            &normalized,
            size,
            first_row_id,
            factory,
            existing_deletes_by_file,
        )
        .map_err(|e| {
            ChangeError::InternalInconsistency(format!("read deleted data file `{path}`: {e}"))
        })?;
        out.extend(batches);
    }
    Ok(out)
}

fn read_full_data_file(
    path: &str,
    size: Option<u64>,
    factory: &novarocks_fs::FsAccessHandle,
) -> Result<Vec<arrow::record_batch::RecordBatch>, String> {
    crate::connector::file_execution::read_foundation_parquet_batches(
        factory,
        path,
        size,
        novarocks_fs::FileProjection::All,
    )
    .map(|batches| {
        batches
            .into_iter()
            .filter(|batch| batch.num_rows() > 0)
            .collect()
    })
}

fn read_full_data_file_with_base_row_id_and_visibility(
    logical_path: &str,
    path: &str,
    size: Option<u64>,
    first_row_id: i64,
    factory: &novarocks_fs::FsAccessHandle,
    existing_deletes_by_file: Option<
        &crate::connector::iceberg::delete_visibility::ExistingDeleteVisibilityByDataFile,
    >,
) -> Result<Vec<arrow::record_batch::RecordBatch>, String> {
    use arrow::array::BooleanArray;
    use arrow::compute::filter_record_batch;

    let batches = read_full_data_file(path, size, factory)?;
    let mut out = Vec::with_capacity(batches.len());
    let mut next_position = 0_u64;
    for batch in batches {
        let end = next_position
            .checked_add(batch.num_rows() as u64)
            .ok_or_else(|| format!("row position overflow while scanning deleted file {path}"))?;
        let mut positions = Vec::with_capacity(batch.num_rows());
        let mut keep = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            let position = next_position.checked_add(row as u64).ok_or_else(|| {
                format!("row position overflow while scanning deleted file {path}")
            })?;
            let visible = match existing_deletes_by_file {
                Some(deletes) => {
                    let row_position = i64::try_from(position).map_err(|_| {
                        format!("row position {position} is too large for deleted file {path}")
                    })?;
                    crate::connector::iceberg::delete_visibility::data_file_row_is_visible(
                        &batch,
                        row,
                        logical_path,
                        row_position,
                        deletes,
                    )?
                }
                None => true,
            };
            keep.push(visible);
            if visible {
                positions.push(position);
            }
        }
        if positions.is_empty() {
            next_position = end;
            continue;
        }
        let filtered = if positions.len() == batch.num_rows() {
            batch
        } else {
            filter_record_batch(&batch, &BooleanArray::from(keep)).map_err(|e| {
                format!("filter deleted data file {logical_path} by previous delete visibility failed: {e}")
            })?
        };
        let enriched = crate::connector::iceberg::scan_deletes::append_base_row_id_column(
            &filtered,
            first_row_id,
            &positions,
        )
        .map_err(|e| e.to_string())?;
        out.push(enriched);
        next_position = end;
    }
    Ok(out)
}

/// Build a filesystem factory that can read planned delta-scan data/delete
/// files for a table location through the Iceberg fs adapter.
pub(crate) fn build_factory_for_table_location(
    location: &str,
    object_store_config: Option<&novarocks_fs::ObjectStoreConfig>,
) -> Result<novarocks_fs::FsAccessHandle, String> {
    crate::connector::iceberg::fs_io::reader_factory_for_table_location(
        location,
        object_store_config,
    )
    .map_err(|e| format!("build iceberg table reader factory for {location}: {e}"))
}

pub(crate) fn expected_object_store_bucket_from_location(
    location: &str,
) -> Result<Option<String>, String> {
    let location = novarocks_fs::FsAccessResolver::new()
        .parse_location(location)
        .map_err(|e| format!("parse iceberg table location {location}: {e}"))?;
    if location.scheme() == novarocks_fs::FsScheme::ObjectStore {
        return location
            .authority()
            .map(|bucket| Some(bucket.to_string()))
            .ok_or_else(|| {
                format!(
                    "object-store iceberg table location missing bucket: {}",
                    location.original()
                )
            });
    }
    Ok(None)
}

pub(crate) fn expected_object_store_bucket_for_table(
    table: &iceberg::table::Table,
) -> Result<Option<String>, String> {
    expected_object_store_bucket_from_location(table.metadata().location())
}

/// Build a filesystem factory that can read both data files and
/// position-delete files for the given iceberg base table. Existing callers
/// outside delta scan still pass a loaded table; delta scan uses
/// `build_factory_for_table_location` to avoid runtime TableMetadata handles.
pub(crate) fn build_factory_for_table(
    table: &iceberg::table::Table,
    object_store_config: Option<&novarocks_fs::ObjectStoreConfig>,
) -> Result<novarocks_fs::FsAccessHandle, String> {
    build_factory_for_table_location(table.metadata().location(), object_store_config)
}

pub(crate) fn normalize_delete_projection_path(
    path: &str,
    object_store_config: Option<&novarocks_fs::ObjectStoreConfig>,
    expected_object_store_bucket: Option<&str>,
) -> Result<String, ChangeError> {
    let parsed = novarocks_fs::FsAccessResolver::new()
        .parse_location(path)
        .map_err(|e| {
            ChangeError::InternalInconsistency(format!(
                "parse iceberg delete reverse projection path {path}: {e}"
            ))
        })?;
    match parsed.scheme() {
        novarocks_fs::FsScheme::Local => Ok(parsed.path().to_string()),
        novarocks_fs::FsScheme::ObjectStore => {
            let access = crate::connector::iceberg::fs_io::resolve_access_for_location(
                path,
                object_store_config,
            )
            .map_err(|e| {
                ChangeError::InternalInconsistency(format!(
                    "normalize object-store delete reverse projection path {path}: {e}"
                ))
            })?;
            let bucket = access.handle().authority().ok_or_else(|| {
                ChangeError::InternalInconsistency(format!(
                    "object-store delete reverse projection path {path} missing bucket"
                ))
            })?;
            if let Some(expected) = expected_object_store_bucket
                && bucket != expected
            {
                return Err(ChangeError::InternalInconsistency(format!(
                    "bucket mismatch for object-store delete reverse projection path {path}: path bucket={bucket} expected bucket={expected}"
                )));
            }
            access
                .single_relative_path()
                .map(str::to_string)
                .map_err(|e| {
                    ChangeError::InternalInconsistency(format!(
                        "normalize object-store delete reverse projection path {path}: {e}"
                    ))
                })
        }
        novarocks_fs::FsScheme::Hdfs => {
            crate::connector::iceberg::fs_io::normalize_hdfs_path_parse_only(path).map_err(|e| {
                ChangeError::InternalInconsistency(format!(
                    "normalize hdfs delete reverse projection path {path}: {e}"
                ))
            })
        }
    }
}

/// Async file collection for one `LineagePlan`. Loads each snapshot's
/// manifest list, walks data manifests for added data rows, walks delete
/// manifests for row-level deletes, and walks overwrite data manifests for
/// deleted data files. Order of the returned vectors matches the lineage
/// order in `actions`.
async fn collect_files(
    metadata: &iceberg::spec::TableMetadata,
    file_io: &iceberg::io::FileIO,
    actions: &[LineageAction],
) -> Result<
    (
        Vec<DataFileRef>,
        Vec<PositionDeleteRef>,
        Vec<EqualityDeleteRef>,
        Vec<DeletedDataFileRef>,
    ),
    ChangeError,
> {
    let mut inserts: Vec<DataFileRef> = Vec::new();
    let mut deletes: Vec<PositionDeleteRef> = Vec::new();
    let mut equality_deletes: Vec<EqualityDeleteRef> = Vec::new();
    let mut deleted_data_files: Vec<DeletedDataFileRef> = Vec::new();

    for action in actions {
        let snapshot_id = match action {
            LineageAction::CollectInserts { snapshot_id }
            | LineageAction::CollectDeletes { snapshot_id }
            | LineageAction::CollectOverwriteDiff { snapshot_id } => *snapshot_id,
        };
        let snapshot = metadata.snapshot_by_id(snapshot_id).ok_or_else(|| {
            ChangeError::InternalInconsistency(format!(
                "collect_files: snapshot {snapshot_id} no longer in metadata"
            ))
        })?;
        let manifest_list = snapshot
            .load_manifest_list(file_io, metadata)
            .await
            .map_err(|e| {
                ChangeError::InternalInconsistency(format!(
                    "load manifest list for snapshot {snapshot_id}: {e}"
                ))
            })?;

        match action {
            LineageAction::CollectInserts { .. } => {
                collect_added_data_files_for_manifest_list(
                    metadata,
                    snapshot_id,
                    file_io,
                    &manifest_list,
                    &mut inserts,
                )
                .await?;
            }
            LineageAction::CollectDeletes { .. } => {
                collect_added_data_files_for_manifest_list(
                    metadata,
                    snapshot_id,
                    file_io,
                    &manifest_list,
                    &mut inserts,
                )
                .await?;
                collect_added_delete_files_for_manifest_list(
                    metadata,
                    snapshot_id,
                    file_io,
                    &manifest_list,
                    &mut deletes,
                    &mut equality_deletes,
                )
                .await?;
            }
            LineageAction::CollectOverwriteDiff { .. } => {
                collect_added_data_files_for_manifest_list(
                    metadata,
                    snapshot_id,
                    file_io,
                    &manifest_list,
                    &mut inserts,
                )
                .await?;
                collect_deleted_data_files_for_manifest_list(
                    metadata,
                    snapshot_id,
                    file_io,
                    &manifest_list,
                    &mut deleted_data_files,
                )
                .await?;
            }
        }
    }

    Ok((inserts, deletes, equality_deletes, deleted_data_files))
}

async fn collect_added_data_files_for_manifest_list(
    metadata: &iceberg::spec::TableMetadata,
    snapshot_id: i64,
    file_io: &iceberg::io::FileIO,
    manifest_list: &iceberg::spec::ManifestList,
    inserts: &mut Vec<DataFileRef>,
) -> Result<(), ChangeError> {
    use iceberg::spec::{DataContentType, ManifestContentType, ManifestStatus};

    for manifest_file in manifest_list.entries() {
        if manifest_file.content != ManifestContentType::Data {
            continue;
        }
        if manifest_file.added_snapshot_id != snapshot_id {
            continue;
        }
        let mut next_manifest_first_row_id = manifest_file
            .first_row_id
            .map(|v| {
                i64::try_from(v).map_err(|_| {
                    ChangeError::InternalInconsistency(format!(
                        "manifest first_row_id too large in snapshot {snapshot_id}: {v}"
                    ))
                })
            })
            .transpose()?;
        let manifest = manifest_file.load_manifest(file_io).await.map_err(|e| {
            ChangeError::InternalInconsistency(format!(
                "load data manifest {} for snapshot {snapshot_id}: {e}",
                manifest_file.manifest_path
            ))
        })?;
        for entry in manifest.entries() {
            // Skip non-Added rows. `Deleted` entries appear here as carry-over
            // bookkeeping when iceberg-rust's writer compacts a prior
            // snapshot's manifest into the new manifest (e.g. an Append
            // immediately following an Overwrite/COW UPDATE). They are not
            // newly-added rows, so this collector ignores them. `Existing`
            // entries are also carry-over and are similarly skipped. Only
            // `Added` entries owned by `snapshot_id` represent inserts
            // produced by this snapshot.
            if entry.status != ManifestStatus::Added {
                continue;
            }
            if entry.snapshot_id() != Some(snapshot_id) {
                continue;
            }
            let df = entry.data_file();
            if df.content_type() != DataContentType::Data {
                continue;
            }
            let record_count = i64::try_from(df.record_count()).unwrap_or(i64::MAX);
            let first_row_id = df.first_row_id().or(next_manifest_first_row_id);
            if let Some(next) = next_manifest_first_row_id.as_mut() {
                *next = next.checked_add(record_count).ok_or_else(|| {
                    ChangeError::InternalInconsistency(format!(
                        "first_row_id overflow in manifest {}",
                        manifest_file.manifest_path
                    ))
                })?;
            }
            inserts.push(DataFileRef {
                path: df.file_path().to_string(),
                size: i64::try_from(df.file_size_in_bytes()).unwrap_or(i64::MAX),
                record_count: Some(record_count),
                partition_spec_id: Some(manifest_file.partition_spec_id),
                partition_key: iceberg_partition_key(df.partition()),
                partition_values: change_partition_field_values(
                    metadata,
                    manifest_file.partition_spec_id,
                    df.partition(),
                )?,
                first_row_id,
                data_sequence_number: Some(
                    entry
                        .sequence_number()
                        .unwrap_or(manifest_file.sequence_number),
                ),
                row_id_allow_list: None,
            });
        }
    }
    Ok(())
}

async fn collect_deleted_data_files_for_manifest_list(
    metadata: &iceberg::spec::TableMetadata,
    snapshot_id: i64,
    file_io: &iceberg::io::FileIO,
    manifest_list: &iceberg::spec::ManifestList,
    deleted_data_files: &mut Vec<DeletedDataFileRef>,
) -> Result<(), ChangeError> {
    use iceberg::spec::{DataContentType, ManifestContentType, ManifestStatus};

    for manifest_file in manifest_list.entries() {
        if manifest_file.content != ManifestContentType::Data {
            continue;
        }
        if manifest_file.added_snapshot_id != snapshot_id {
            continue;
        }
        let manifest = manifest_file.load_manifest(file_io).await.map_err(|e| {
            ChangeError::InternalInconsistency(format!(
                "load data manifest {} for overwrite snapshot {snapshot_id}: {e}",
                manifest_file.manifest_path
            ))
        })?;
        for entry in manifest.entries() {
            if entry.status != ManifestStatus::Deleted {
                continue;
            }
            if entry.snapshot_id() != Some(snapshot_id) {
                continue;
            }
            let df = entry.data_file();
            if df.content_type() != DataContentType::Data {
                continue;
            }
            let record_count = i64::try_from(df.record_count()).unwrap_or(i64::MAX);
            // For deleted entries `df.first_row_id()` is the value carried
            // forward from the original `Added` entry — when the file was
            // first appended. The IcebergDeltaScanOperator's deleted-file
            // scanner falls back to a previous-snapshot lineage lookup if
            // this is `None` (e.g. the original APPEND only set the
            // manifest-level first_row_id and relied on inheritance).
            deleted_data_files.push(DeletedDataFileRef {
                path: df.file_path().to_string(),
                size: i64::try_from(df.file_size_in_bytes()).unwrap_or(i64::MAX),
                record_count: Some(record_count),
                partition_spec_id: Some(manifest_file.partition_spec_id),
                partition_key: iceberg_partition_key(df.partition()),
                partition_values: change_partition_field_values(
                    metadata,
                    manifest_file.partition_spec_id,
                    df.partition(),
                )?,
                first_row_id: df.first_row_id(),
                data_sequence_number: Some(
                    entry
                        .sequence_number()
                        .unwrap_or(manifest_file.sequence_number),
                ),
            });
        }
    }
    Ok(())
}

async fn collect_added_delete_files_for_manifest_list(
    metadata: &iceberg::spec::TableMetadata,
    snapshot_id: i64,
    file_io: &iceberg::io::FileIO,
    manifest_list: &iceberg::spec::ManifestList,
    deletes: &mut Vec<PositionDeleteRef>,
    equality_deletes: &mut Vec<EqualityDeleteRef>,
) -> Result<(), ChangeError> {
    use iceberg::spec::{DataContentType, DataFileFormat, ManifestContentType, ManifestStatus};

    for manifest_file in manifest_list.entries() {
        if manifest_file.content != ManifestContentType::Deletes {
            continue;
        }
        if manifest_file.added_snapshot_id != snapshot_id {
            continue;
        }
        let manifest = manifest_file.load_manifest(file_io).await.map_err(|e| {
            ChangeError::InternalInconsistency(format!(
                "load delete manifest {} for snapshot {snapshot_id}: {e}",
                manifest_file.manifest_path
            ))
        })?;
        for entry in manifest.entries() {
            if entry.status != ManifestStatus::Added {
                continue;
            }
            if entry.snapshot_id() != Some(snapshot_id) {
                continue;
            }
            let df = entry.data_file();
            match df.content_type() {
                DataContentType::PositionDeletes => {
                    let r = match df.file_format() {
                        DataFileFormat::Parquet => PositionDeleteRef {
                            delete_file_path: df.file_path().to_string(),
                            delete_file_size: i64::try_from(df.file_size_in_bytes())
                                .unwrap_or(i64::MAX),
                            record_count: Some(
                                i64::try_from(df.record_count()).unwrap_or(i64::MAX),
                            ),
                            referenced_data_file: df.referenced_data_file(),
                            file_format: DataFileFormat::Parquet,
                            content_offset: None,
                            content_size_in_bytes: None,
                            partition_values: change_partition_field_values(
                                metadata,
                                manifest_file.partition_spec_id,
                                df.partition(),
                            )?,
                        },
                        DataFileFormat::Puffin => {
                            let referenced = df.referenced_data_file().ok_or_else(|| {
                                ChangeError::InternalInconsistency(format!(
                                    "Puffin DV {} in snapshot {snapshot_id} missing referenced_data_file",
                                    df.file_path()
                                ))
                            })?;
                            let offset = df.content_offset().ok_or_else(|| {
                                ChangeError::InternalInconsistency(format!(
                                    "Puffin DV {} in snapshot {snapshot_id} missing content_offset",
                                    df.file_path()
                                ))
                            })?;
                            let length = df.content_size_in_bytes().ok_or_else(|| {
                                ChangeError::InternalInconsistency(format!(
                                    "Puffin DV {} in snapshot {snapshot_id} missing content_size_in_bytes",
                                    df.file_path()
                                ))
                            })?;
                            PositionDeleteRef {
                                delete_file_path: df.file_path().to_string(),
                                delete_file_size: i64::try_from(df.file_size_in_bytes())
                                    .unwrap_or(i64::MAX),
                                record_count: Some(
                                    i64::try_from(df.record_count()).unwrap_or(i64::MAX),
                                ),
                                referenced_data_file: Some(referenced),
                                file_format: DataFileFormat::Puffin,
                                content_offset: Some(offset),
                                content_size_in_bytes: Some(length),
                                partition_values: change_partition_field_values(
                                    metadata,
                                    manifest_file.partition_spec_id,
                                    df.partition(),
                                )?,
                            }
                        }
                        other => {
                            return Err(ChangeError::InternalInconsistency(format!(
                                "delete manifest in snapshot {snapshot_id} has unsupported file_format {:?}: {}",
                                other,
                                df.file_path()
                            )));
                        }
                    };
                    r.validate_invariants()?;
                    deletes.push(r);
                }
                DataContentType::EqualityDeletes => {
                    if df.file_format() != DataFileFormat::Parquet {
                        return Err(ChangeError::InternalInconsistency(format!(
                            "equality-delete file in snapshot {snapshot_id} has unsupported file_format {:?}: {}",
                            df.file_format(),
                            df.file_path()
                        )));
                    }
                    let equality_ids = df.equality_ids().ok_or_else(|| {
                        ChangeError::InternalInconsistency(format!(
                            "equality-delete file {} in snapshot {snapshot_id} missing equality_ids",
                            df.file_path()
                        ))
                    })?;
                    if equality_ids.is_empty() {
                        return Err(ChangeError::InternalInconsistency(format!(
                            "equality-delete file {} in snapshot {snapshot_id} has empty equality_ids",
                            df.file_path()
                        )));
                    }
                    equality_deletes.push(EqualityDeleteRef {
                        delete_file_path: df.file_path().to_string(),
                        delete_file_size: i64::try_from(df.file_size_in_bytes())
                            .unwrap_or(i64::MAX),
                        record_count: Some(i64::try_from(df.record_count()).unwrap_or(i64::MAX)),
                        equality_ids,
                        sequence_number: Some(
                            entry
                                .sequence_number()
                                .unwrap_or(manifest_file.sequence_number),
                        ),
                        partition_spec_id: Some(manifest_file.partition_spec_id),
                        partition_key: iceberg_partition_key(df.partition()),
                        partition_values: change_partition_field_values(
                            metadata,
                            manifest_file.partition_spec_id,
                            df.partition(),
                        )?,
                    });
                }
                DataContentType::Data => {
                    return Err(ChangeError::InternalInconsistency(format!(
                        "delete manifest contains DATA file in snapshot {snapshot_id}: {}",
                        df.file_path()
                    )));
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use arrow::array::{Int32Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
    use arrow::record_batch::RecordBatch;
    use iceberg::spec::{Operation, Snapshot, Summary};
    use parquet::arrow::ArrowWriter;

    use super::{
        ChangeError, DeletedDataFileRef, EqualityDeleteRef, IcebergChangeBatch,
        IcebergChangePolicySignal, LineageAction, build_factory_for_table_location,
        classify_snapshot, delta_source_files_from_change_batch_with_equality_targets,
        expected_object_store_bucket_from_location, normalize_delete_projection_path,
        policy_signal_from_change_error, scan_deleted_data_file_rows_with_factory,
        validate_replace_snapshot,
    };

    use crate::connector::iceberg::catalog::registry::{
        IcebergCatalogEntry, block_on_iceberg, build_catalog_entry, build_hadoop_catalog,
        create_namespace, create_table, insert_rows, load_table,
    };
    use crate::connector::iceberg::commit::{
        CommitCtx, CommitOpKind, IcebergCommitAction, IcebergCommitCollector, OverwriteCommit,
    };
    use crate::sql::parser::ast::IcebergPartitionFieldExpr;
    use crate::sql::{Literal, TableColumnDef};
    use novarocks_catalog::schema::SqlType;
    use novarocks_fs::ObjectStoreConfig;

    use super::plan_changes;

    #[test]
    fn delta_source_files_carries_preplanned_equality_delete_targets() {
        let batch = IcebergChangeBatch {
            previous_snapshot_id: 10,
            current_snapshot_id: 11,
            inserts: Vec::new(),
            deletes: Vec::new(),
            equality_deletes: vec![EqualityDeleteRef {
                delete_file_path: "eq-delete.parquet".to_string(),
                delete_file_size: 123,
                record_count: None,
                equality_ids: vec![1],
                sequence_number: Some(7),
                partition_spec_id: None,
                partition_key: None,
                partition_values: Vec::new(),
            }],
            deleted_data_files: Vec::new(),
        };
        let mut equality_targets = HashMap::new();
        equality_targets.insert(
            "eq-delete.parquet".to_string(),
            vec![crate::connector::iceberg::delta::EqualityDeleteTargetData {
                data_file_path: "data.parquet".to_string(),
                data_file_size: 456,
                data_file_first_row_id: Some(1000),
                data_file_sequence_number: Some(6),
            }],
        );

        let files =
            delta_source_files_from_change_batch_with_equality_targets(&batch, &equality_targets)
                .expect("delta source files");
        assert_eq!(files.len(), 1);
        let crate::connector::iceberg::delta::DeltaSourceRole::EqualityDelete {
            equality_field_ids,
            targets,
        } = &files[0].role
        else {
            panic!("expected equality-delete role");
        };
        assert_eq!(equality_field_ids, &vec![1]);
        assert_eq!(targets.len(), 1);
        assert_eq!(targets[0].data_file_path, "data.parquet");
        assert_eq!(targets[0].data_file_size, 456);
        assert_eq!(targets[0].data_file_first_row_id, Some(1000));
        assert_eq!(targets[0].data_file_sequence_number, Some(6));
    }

    #[test]
    fn replace_validation_policy_signal_is_full_refresh() {
        let err = ChangeError::ReplaceValidationFailed {
            snapshot_id: 1,
            reason: "records changed".to_string(),
        };
        let IcebergChangePolicySignal::FullRefresh { reason } =
            policy_signal_from_change_error(&err)
        else {
            panic!("expected full refresh signal");
        };
        assert!(
            reason.contains("not a provably safe compaction"),
            "{reason}"
        );
    }

    #[test]
    fn deleted_data_file_reverse_projection_appends_base_row_id_sequence() {
        let dir = tempfile::tempdir().expect("tempdir");
        let data_path = dir.path().join("deleted.parquet");
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
        )
        .expect("batch");
        let file = std::fs::File::create(&data_path).expect("create parquet");
        let mut writer = ArrowWriter::try_new(file, schema, None).expect("writer");
        writer.write(&batch).expect("write batch");
        writer.close().expect("close writer");

        let deleted = vec![DeletedDataFileRef {
            path: "deleted.parquet".to_string(),
            size: std::fs::metadata(&data_path).expect("metadata").len() as i64,
            record_count: Some(3),
            partition_spec_id: None,
            partition_key: None,
            partition_values: Vec::new(),
            first_row_id: Some(200),
            data_sequence_number: None,
        }];
        let factory = novarocks_fs::FsAccessResolver::new()
            .resolve_location(dir.path().join("__binding__").to_string_lossy(), None)
            .expect("access");

        let batches = scan_deleted_data_file_rows_with_factory(&deleted, &factory, |path: &str| {
            Ok(path.to_string())
        })
        .expect("scan deleted data file");

        let batch = batches.first().expect("deleted row batch");
        let row_id = batch
            .column(batch.schema().index_of("_row_id").expect("_row_id column"))
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("_row_id int64");
        assert_eq!(row_id.values(), &[200, 201, 202]);
    }

    #[test]
    fn equality_delete_reverse_projection_appends_matching_base_row_ids() {
        let dir = tempfile::tempdir().expect("tempdir");
        let data_path = dir.path().join("data.parquet");
        let data_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let data = RecordBatch::try_new(
            Arc::clone(&data_schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3, 4])),
                Arc::new(StringArray::from(vec!["a", "b", "c", "d"])),
            ],
        )
        .expect("data batch");
        let file = std::fs::File::create(&data_path).expect("create data parquet");
        let mut writer = ArrowWriter::try_new(file, data_schema, None).expect("data writer");
        writer.write(&data).expect("write data");
        writer.close().expect("close data writer");

        let equality_path = dir.path().join("eq.parquet");
        let equality_schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "id",
            DataType::Int32,
            false,
        )]));
        let equality = RecordBatch::try_new(
            Arc::clone(&equality_schema),
            vec![Arc::new(Int32Array::from(vec![2, 4]))],
        )
        .expect("equality batch");
        let file = std::fs::File::create(&equality_path).expect("create equality parquet");
        let mut writer =
            ArrowWriter::try_new(file, equality_schema, None).expect("equality writer");
        writer.write(&equality).expect("write equality");
        writer.close().expect("close equality writer");

        let factory = novarocks_fs::FsAccessResolver::new()
            .resolve_location(dir.path().join("__binding__").to_string_lossy(), None)
            .expect("access");
        let spec = crate::connector::iceberg::delete_file::IcebergDeleteFileSpec {
            path: "eq.parquet".to_string(),
            file_format: crate::connector::iceberg::delete_file::IcebergFileFormat::Parquet,
            file_content:
                crate::connector::iceberg::delete_file::IcebergFileContent::EqualityDeletes,
            length: Some(std::fs::metadata(&equality_path).expect("metadata").len()),
            content_offset: None,
            content_size_in_bytes: None,
        };
        let sets = crate::connector::iceberg::equality_delete::load_equality_delete_sets(
            &[spec],
            &factory,
        )
        .expect("load equality delete sets");

        let batches = super::read_data_file_matching_equality_deletes_with_base_row_id(
            "data.parquet",
            Some(std::fs::metadata(&data_path).expect("metadata").len()),
            &sets,
            Some(300),
            &factory,
            |path| Ok(path.to_string()),
        )
        .expect("scan equality deleted rows");

        let batch = batches.first().expect("deleted row batch");
        let row_id = batch
            .column(batch.schema().index_of("_row_id").expect("_row_id column"))
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("_row_id int64");
        assert_eq!(row_id.values(), &[301, 303]);
    }

    #[test]
    fn data_file_ref_preserves_partition_and_lineage_metadata() {
        let file = super::DataFileRef {
            path: "s3://bucket/t/data.parquet".to_string(),
            size: 10,
            record_count: Some(2),
            partition_spec_id: Some(4),
            partition_key: Some("city=A".to_string()),
            partition_values: vec![super::ChangePartitionFieldValue {
                source_field_id: 7,
                source_column: Some("city".to_string()),
                field_name: "city".to_string(),
                transform: "identity".to_string(),
                value: super::ChangePartitionValue::Primitive("A".to_string()),
            }],
            first_row_id: Some(100),
            data_sequence_number: Some(12),
            row_id_allow_list: None,
        };

        assert_eq!(file.partition_spec_id, Some(4));
        assert_eq!(file.partition_key.as_deref(), Some("city=A"));
        assert_eq!(file.partition_values[0].source_field_id, 7);
        assert_eq!(
            file.partition_values[0].source_column.as_deref(),
            Some("city")
        );
        assert_eq!(file.partition_values[0].field_name, "city");
        assert_eq!(file.partition_values[0].transform, "identity");
        assert_eq!(file.partition_values[0].value.as_primitive_str(), Some("A"));
        assert_eq!(file.first_row_id, Some(100));
        assert_eq!(file.data_sequence_number, Some(12));
    }

    #[test]
    fn change_partition_value_distinguishes_null_from_unsupported() {
        use iceberg::spec::{Literal as IcebergLiteral, PrimitiveLiteral, Struct};

        assert_eq!(
            super::change_partition_value(None),
            super::ChangePartitionValue::Null
        );
        assert_eq!(
            super::change_partition_value(Some(&IcebergLiteral::Primitive(
                PrimitiveLiteral::Binary(vec![1, 2, 3]),
            ))),
            super::ChangePartitionValue::Unsupported("binary partition value".to_string())
        );
        assert_eq!(
            super::change_partition_value(Some(&IcebergLiteral::Struct(Struct::empty()))),
            super::ChangePartitionValue::Unsupported("non-primitive partition value".to_string())
        );
    }

    #[test]
    fn position_delete_ref_preserves_partition_metadata() {
        let delete = super::PositionDeleteRef {
            delete_file_path: "s3://bucket/t/delete.parquet".to_string(),
            delete_file_size: 20,
            record_count: Some(1),
            referenced_data_file: Some("s3://bucket/t/data.parquet".to_string()),
            file_format: iceberg::spec::DataFileFormat::Parquet,
            content_offset: None,
            content_size_in_bytes: None,
            partition_values: vec![super::ChangePartitionFieldValue {
                source_field_id: 7,
                source_column: Some("city".to_string()),
                field_name: "city".to_string(),
                transform: "identity".to_string(),
                value: super::ChangePartitionValue::Primitive("A".to_string()),
            }],
        };

        delete.validate_invariants().expect("valid position delete");
        assert_eq!(delete.partition_values[0].source_field_id, 7);
        assert_eq!(
            delete.partition_values[0].source_column.as_deref(),
            Some("city")
        );
        assert_eq!(delete.partition_values[0].field_name, "city");
        assert_eq!(
            delete.partition_values[0].value.as_primitive_str(),
            Some("A")
        );
    }

    fn test_hadoop_catalog_entry(catalog_name: &str, warehouse_uri: &str) -> IcebergCatalogEntry {
        build_catalog_entry(
            catalog_name,
            &[
                ("type".to_string(), "iceberg".to_string()),
                ("iceberg.catalog.type".to_string(), "hadoop".to_string()),
                (
                    "iceberg.catalog.warehouse".to_string(),
                    warehouse_uri.to_string(),
                ),
            ],
        )
        .expect("catalog entry")
    }

    /// Build a synthetic `Snapshot` whose summary carries the given
    /// operation and properties. `manifest_list` and timestamps get
    /// throwaway-but-positive values; the classifier never reads them.
    /// schema_id is encoded in the summary's `schema_id` only when the
    /// caller passes it via the builder.
    fn snap(
        snapshot_id: i64,
        parent_snapshot_id: Option<i64>,
        operation: Operation,
        properties: &[(&str, &str)],
        schema_id: i32,
    ) -> Snapshot {
        let mut props: HashMap<String, String> = HashMap::new();
        for (k, v) in properties {
            props.insert((*k).to_string(), (*v).to_string());
        }
        // iceberg-rust 0.9 `Snapshot::with_parent_snapshot_id` is generated
        // by typed_builder without `strip_option`, so its setter takes
        // `Option<i64>` directly. We always call it (passing `None` when
        // there's no parent) because TypedBuilder's type-state means we
        // can't reassign the builder across optional setters.
        Snapshot::builder()
            .with_snapshot_id(snapshot_id)
            .with_parent_snapshot_id(parent_snapshot_id)
            .with_sequence_number(snapshot_id)
            .with_timestamp_ms(1_700_000_000_000 + snapshot_id)
            .with_manifest_list(format!("file:///tmp/manifest-list-{snapshot_id}.avro"))
            .with_summary(Summary {
                operation,
                additional_properties: props,
            })
            .with_schema_id(schema_id)
            .build()
    }

    fn replace_props(
        total_records: i64,
        added_files: i64,
        deleted_files: i64,
    ) -> Vec<(&'static str, String)> {
        vec![
            ("total-records", total_records.to_string()),
            ("added-data-files", added_files.to_string()),
            ("deleted-data-files", deleted_files.to_string()),
        ]
    }

    fn replace_props_with_delete_counts(
        total_records: i64,
        added_files: i64,
        deleted_files: i64,
        added_delete_files: i64,
        deleted_delete_files: i64,
    ) -> Vec<(&'static str, String)> {
        let mut props = replace_props(total_records, added_files, deleted_files);
        props.extend([
            ("added-delete-files", added_delete_files.to_string()),
            ("removed-delete-files", deleted_delete_files.to_string()),
        ]);
        props
    }

    fn test_object_store_config() -> ObjectStoreConfig {
        ObjectStoreConfig {
            endpoint: "http://127.0.0.1:9000".to_string(),
            access_key_id: "ak".to_string(),
            access_key_secret: "sk".to_string(),
            session_token: None,
            enable_path_style_access: Some(true),
            region: Some("us-east-1".to_string()),
            retry_max_times: None,
            retry_min_delay_ms: None,
            retry_max_delay_ms: None,
            timeout_ms: None,
            io_timeout_ms: None,
        }
    }

    #[test]
    fn build_factory_for_local_table_location_uses_resolver() {
        let dir = tempfile::tempdir().expect("tempdir");
        let file_path = dir.path().join("data.parquet");
        let location = format!("file://{}", file_path.display());

        let _factory =
            build_factory_for_table_location(&location, None).expect("local reader factory");
    }

    #[test]
    fn build_factory_for_object_store_location_requires_credentials() {
        let err = build_factory_for_table_location("s3://lake/warehouse/db/orders", None)
            .expect_err("object-store location requires credentials");

        assert!(
            err.contains("object-store location requires object store config"),
            "{err}"
        );
    }

    #[test]
    fn expected_object_store_bucket_from_location_ignores_hdfs_authority() {
        let bucket =
            expected_object_store_bucket_from_location("hdfs://nn-1:9000/warehouse/db/orders")
                .expect("parse hdfs location");

        assert_eq!(bucket, None);
    }

    #[test]
    fn normalize_delete_projection_path_keeps_full_local_file_path() {
        let dir = tempfile::tempdir().expect("tempdir");
        let file_path = dir.path().join("nested").join("data.parquet");
        let location = format!("file://{}", file_path.display());

        let normalized =
            normalize_delete_projection_path(&location, None, None).expect("normalize local path");

        assert_eq!(normalized, file_path.display().to_string());
    }

    #[test]
    fn local_table_factory_reads_normalized_full_local_path() {
        let dir = tempfile::tempdir().expect("tempdir");
        let table_location = format!("file://{}", dir.path().join("table").display());
        let data_path = dir.path().join("table").join("data").join("part-0.bin");
        std::fs::create_dir_all(data_path.parent().expect("parent")).expect("mkdir");
        std::fs::write(&data_path, b"abcdef").expect("write data");
        let data_location = format!("file://{}", data_path.display());

        let factory =
            build_factory_for_table_location(&table_location, None).expect("local factory");
        let normalized =
            normalize_delete_projection_path(&data_location, None, None).expect("normalize");
        let file = factory
            .bind_location(
                &normalized,
                novarocks_fs::FileIdentity::new(&normalized, 6, None),
            )
            .expect("bind normalized path");
        let cancellation = novarocks_fs::FileCancellation::new();
        let bytes = crate::runtime::global_async_runtime::data_block_on(async move {
            file.read(
                novarocks_fs::FileReadRange::bounded(1, 3).expect("range"),
                &cancellation,
            )
            .await
        })
        .expect("runtime")
        .expect("read range");

        assert_eq!(bytes, "bcd");
    }

    #[test]
    fn normalize_delete_projection_path_uses_object_store_config_for_s3_uri() {
        let cfg = test_object_store_config();
        let path = normalize_delete_projection_path(
            "s3://lake/warehouse/db/orders/data.parquet",
            Some(&cfg),
            Some("lake"),
        )
        .expect("normalize");
        // Object-store operators are bucket-root scoped in FS-3, so the key
        // passed to OpenDAL must remain relative to the bucket, not to a table
        // warehouse prefix.
        assert_eq!(path, "warehouse/db/orders/data.parquet");
    }

    #[test]
    fn normalize_delete_projection_path_keeps_hdfs_relative_path() {
        let path = normalize_delete_projection_path(
            "hdfs://nn-1:9000/warehouse/db/orders/data.parquet",
            None,
            None,
        )
        .expect("normalize hdfs path");

        assert_eq!(path, "warehouse/db/orders/data.parquet");
    }

    #[test]
    fn normalize_delete_projection_path_rejects_mismatched_s3_bucket() {
        let cfg = test_object_store_config();
        let err = normalize_delete_projection_path(
            "s3://other/warehouse/db/orders/data.parquet",
            Some(&cfg),
            Some("lake"),
        )
        .expect_err("must reject mismatched bucket");
        assert!(format!("{err}").contains("bucket mismatch"), "{err}");
    }

    #[test]
    fn normalize_delete_projection_path_rejects_s3_uri_without_object_store_config() {
        let err = normalize_delete_projection_path(
            "s3://lake/warehouse/db/orders/data.parquet",
            None,
            Some("lake"),
        )
        .expect_err("must reject");
        assert!(
            format!("{err}").contains("object-store location requires object store config"),
            "{err}"
        );
    }

    #[test]
    fn display_primary_key_missing() {
        let e = ChangeError::PrimaryKeyMissingFromBase {
            pk_col: "order_id".to_string(),
        };
        let s = format!("{e}");
        assert!(s.contains("order_id"), "{s}");
        assert!(s.to_lowercase().contains("primary key"), "{s}");
    }

    #[test]
    fn display_iceberg_format_unsupported() {
        let e = ChangeError::IcebergFormatUnsupported { format_version: 1 };
        let s = format!("{e}");
        assert!(s.contains("format-version 1"), "{s}");
        assert!(s.to_lowercase().contains("v2"), "{s}");
    }

    #[test]
    fn classify_snapshot_append_emits_collect_inserts() {
        let s = snap(7, Some(1), Operation::Append, &[], 0);
        let action = classify_snapshot(&s, None).expect("ok");
        assert_eq!(
            action,
            Some(LineageAction::CollectInserts { snapshot_id: 7 })
        );
    }

    #[test]
    fn classify_snapshot_delete_emits_collect_deletes() {
        let s = snap(7, Some(1), Operation::Delete, &[], 0);
        let action = classify_snapshot(&s, None).expect("ok");
        assert_eq!(
            action,
            Some(LineageAction::CollectDeletes { snapshot_id: 7 })
        );
    }

    #[test]
    fn classify_snapshot_overwrite_emits_collect_overwrite_diff() {
        let s = snap(7, Some(1), Operation::Overwrite, &[], 0);
        let action = classify_snapshot(&s, None).expect("ok");
        assert_eq!(
            action,
            Some(LineageAction::CollectOverwriteDiff { snapshot_id: 7 })
        );
    }

    #[test]
    fn classify_marked_cow_update_overwrite_ignores_private_marker() {
        let s = snap(
            7,
            Some(6),
            Operation::Overwrite,
            &[
                ("novarocks.row-level-op", "update"),
                ("novarocks.update.mode", "copy-on-write"),
            ],
            0,
        );
        assert_eq!(
            classify_snapshot(&s, None).expect("classify"),
            Some(LineageAction::CollectOverwriteDiff { snapshot_id: 7 })
        );
    }

    #[test]
    fn classify_marked_mor_update_delete_ignores_private_marker() {
        let s = snap(
            7,
            Some(6),
            Operation::Delete,
            &[
                ("novarocks.row-level-op", "update"),
                ("novarocks.update.mode", "merge-on-read"),
            ],
            0,
        );
        assert_eq!(
            classify_snapshot(&s, None).expect("classify"),
            Some(LineageAction::CollectDeletes { snapshot_id: 7 })
        );
    }

    #[test]
    fn ordinary_overwrite_uses_standard_diff_path() {
        let s = snap(7, Some(6), Operation::Overwrite, &[], 0);
        assert_eq!(
            classify_snapshot(&s, None).expect("classify"),
            Some(LineageAction::CollectOverwriteDiff { snapshot_id: 7 })
        );
    }

    #[test]
    fn ordinary_delete_without_marker_still_maps_to_collect_deletes() {
        let s = snap(7, Some(6), Operation::Delete, &[], 0);
        assert_eq!(
            classify_snapshot(&s, None).expect("classify"),
            Some(LineageAction::CollectDeletes { snapshot_id: 7 })
        );
    }

    #[test]
    fn classify_snapshot_replace_compaction_is_skipped() {
        let parent = snap(1, None, Operation::Append, &[("total-records", "100")], 0);
        let owned = replace_props(100, 3, 5);
        let props: Vec<(&str, &str)> = owned.iter().map(|(k, v)| (*k, v.as_str())).collect();
        let s = snap(2, Some(1), Operation::Replace, &props, 0);
        let action = classify_snapshot(&s, Some(&parent)).expect("ok");
        assert_eq!(action, None);
    }

    #[test]
    fn classify_lineage_skips_delete_eliminating_replace_compaction() {
        let parent = snap(1, None, Operation::Append, &[("total-records", "100")], 0);
        let owned = replace_props_with_delete_counts(100, 3, 5, 0, 2);
        let props: Vec<(&str, &str)> = owned.iter().map(|(k, v)| (*k, v.as_str())).collect();
        let s = snap(2, Some(1), Operation::Replace, &props, 0);

        let action = classify_snapshot(&s, Some(&parent)).expect("ok");
        assert_eq!(action, None);
    }

    #[test]
    fn classify_lineage_skips_rewrite_after_delete_elimination() {
        let parent = snap(1, None, Operation::Delete, &[("total-records", "18")], 0);
        let mut owned = replace_props_with_delete_counts(18, 2, 1, 0, 2);
        owned.extend([
            ("added-records", "18".to_string()),
            ("deleted-records", "23".to_string()),
        ]);
        let props: Vec<(&str, &str)> = owned.iter().map(|(k, v)| (*k, v.as_str())).collect();
        let s = snap(2, Some(1), Operation::Replace, &props, 0);

        let action = classify_snapshot(&s, Some(&parent)).expect("ok");
        assert_eq!(action, None);
    }

    #[test]
    fn classify_lineage_skips_zero_row_rewrite_after_delete_elimination() {
        let parent = snap(1, None, Operation::Delete, &[("total-records", "0")], 0);
        let mut owned = replace_props_with_delete_counts(0, 0, 1, 0, 1);
        owned.extend([
            ("added-records", "0".to_string()),
            ("deleted-records", "23".to_string()),
        ]);
        let props: Vec<(&str, &str)> = owned.iter().map(|(k, v)| (*k, v.as_str())).collect();
        let s = snap(2, Some(1), Operation::Replace, &props, 0);

        let action = classify_snapshot(&s, Some(&parent)).expect("ok");
        assert_eq!(action, None);
    }

    #[test]
    fn classify_lineage_skips_delete_file_only_replace_noop() {
        let parent = snap(1, None, Operation::Append, &[("total-records", "100")], 0);
        let mut owned = replace_props(100, 0, 0);
        owned.extend([
            ("rewritten-delete-files", "2".to_string()),
            ("added-delete-files", "1".to_string()),
            ("rewritten-bytes", "128".to_string()),
            ("added-bytes", "64".to_string()),
        ]);
        let props: Vec<(&str, &str)> = owned.iter().map(|(k, v)| (*k, v.as_str())).collect();
        let s = snap(2, Some(1), Operation::Replace, &props, 0);

        let action = classify_snapshot(&s, Some(&parent)).expect("ok");
        assert_eq!(action, None);
    }

    #[test]
    fn validate_replace_delete_file_only_requires_explicit_data_file_counts() {
        let parent = snap(1, None, Operation::Append, &[("total-records", "100")], 0);
        let s = snap(
            2,
            Some(1),
            Operation::Replace,
            &[
                ("total-records", "100"),
                ("rewritten-delete-files", "2"),
                ("added-delete-files", "1"),
            ],
            0,
        );

        let err = validate_replace_snapshot(&s, &parent).expect_err("err");
        match err {
            ChangeError::ReplaceValidationFailed {
                snapshot_id,
                reason,
            } => {
                assert_eq!(snapshot_id, 2);
                assert!(reason.contains("added-data-files"), "{reason}");
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn validate_replace_delete_file_only_rejects_malformed_data_file_counts() {
        let parent = snap(1, None, Operation::Append, &[("total-records", "100")], 0);
        let s = snap(
            2,
            Some(1),
            Operation::Replace,
            &[
                ("total-records", "100"),
                ("added-data-files", "not-a-number"),
                ("deleted-data-files", "0"),
            ],
            0,
        );

        let err = validate_replace_snapshot(&s, &parent).expect_err("err");
        match err {
            ChangeError::ReplaceValidationFailed {
                snapshot_id,
                reason,
            } => {
                assert_eq!(snapshot_id, 2);
                assert!(reason.contains("added-data-files"), "{reason}");
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn validate_replace_delete_file_only_rejects_malformed_deleted_data_file_counts() {
        let parent = snap(1, None, Operation::Append, &[("total-records", "100")], 0);
        let s = snap(
            2,
            Some(1),
            Operation::Replace,
            &[
                ("total-records", "100"),
                ("added-data-files", "0"),
                ("deleted-data-files", "not-a-number"),
            ],
            0,
        );

        let err = validate_replace_snapshot(&s, &parent).expect_err("err");
        match err {
            ChangeError::ReplaceValidationFailed {
                snapshot_id,
                reason,
            } => {
                assert_eq!(snapshot_id, 2);
                assert!(reason.contains("deleted-data-files"), "{reason}");
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn validate_replace_delete_file_only_schema_id_change_is_rejected() {
        let parent = snap(1, None, Operation::Append, &[("total-records", "100")], 0);
        let owned = replace_props(100, 0, 0);
        let props: Vec<(&str, &str)> = owned.iter().map(|(k, v)| (*k, v.as_str())).collect();
        let s = snap(2, Some(1), Operation::Replace, &props, 7);

        let err = validate_replace_snapshot(&s, &parent).expect_err("err");
        match err {
            ChangeError::ReplaceValidationFailed {
                snapshot_id,
                reason,
            } => {
                assert_eq!(snapshot_id, 2);
                assert!(reason.contains("schema"), "{reason}");
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn classify_lineage_rejects_replace_that_changes_total_records() {
        let parent = snap(1, None, Operation::Append, &[("total-records", "100")], 0);
        let owned = replace_props_with_delete_counts(101, 3, 5, 0, 2);
        let props: Vec<(&str, &str)> = owned.iter().map(|(k, v)| (*k, v.as_str())).collect();
        let s = snap(2, Some(1), Operation::Replace, &props, 0);

        let err = classify_snapshot(&s, Some(&parent)).expect_err("err");
        match err {
            ChangeError::ReplaceValidationFailed {
                snapshot_id,
                reason,
            } => {
                assert_eq!(snapshot_id, 2);
                assert!(reason.contains("total-records"), "{reason}");
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn classify_snapshot_replace_without_parent_is_rejected() {
        let owned = replace_props(100, 3, 5);
        let props: Vec<(&str, &str)> = owned.iter().map(|(k, v)| (*k, v.as_str())).collect();
        let s = snap(2, None, Operation::Replace, &props, 0);
        let err = classify_snapshot(&s, None).expect_err("err");
        match err {
            ChangeError::ReplaceValidationFailed {
                snapshot_id,
                reason,
            } => {
                assert_eq!(snapshot_id, 2);
                assert!(reason.contains("parent"), "{reason}");
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn validate_replace_record_count_change_is_rejected() {
        let parent = snap(1, None, Operation::Append, &[("total-records", "100")], 0);
        let owned = replace_props(101, 3, 5);
        let props: Vec<(&str, &str)> = owned.iter().map(|(k, v)| (*k, v.as_str())).collect();
        let s = snap(2, Some(1), Operation::Replace, &props, 0);
        let err = validate_replace_snapshot(&s, &parent).expect_err("err");
        match err {
            ChangeError::ReplaceValidationFailed {
                snapshot_id,
                reason,
            } => {
                assert_eq!(snapshot_id, 2);
                assert!(reason.contains("total-records"), "{reason}");
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn validate_replace_missing_total_records_is_rejected() {
        // Parent has total-records, REPLACE doesn't. Validator can't prove the
        // record count is unchanged → reject.
        let parent = snap(1, None, Operation::Append, &[("total-records", "100")], 0);
        let s = snap(
            2,
            Some(1),
            Operation::Replace,
            &[("added-data-files", "3"), ("deleted-data-files", "5")],
            0,
        );
        let err = validate_replace_snapshot(&s, &parent).expect_err("err");
        match err {
            ChangeError::ReplaceValidationFailed {
                snapshot_id,
                reason,
            } => {
                assert_eq!(snapshot_id, 2);
                assert!(reason.contains("total-records"), "{reason}");
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn validate_replace_missing_added_or_removed_is_rejected() {
        let parent = snap(1, None, Operation::Append, &[("total-records", "100")], 0);
        let owned = replace_props(100, 0, 5);
        let props: Vec<(&str, &str)> = owned.iter().map(|(k, v)| (*k, v.as_str())).collect();
        let s = snap(2, Some(1), Operation::Replace, &props, 0);
        let err = validate_replace_snapshot(&s, &parent).expect_err("err");
        match err {
            ChangeError::ReplaceValidationFailed {
                snapshot_id,
                reason,
            } => {
                assert_eq!(snapshot_id, 2);
                assert!(reason.contains("added-data-files"), "{reason}");
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn validate_replace_schema_id_change_is_rejected() {
        let parent = snap(1, None, Operation::Append, &[("total-records", "100")], 0);
        let owned = replace_props(100, 3, 5);
        let props: Vec<(&str, &str)> = owned.iter().map(|(k, v)| (*k, v.as_str())).collect();
        // schema_id 7 ≠ parent's 0.
        let s = snap(2, Some(1), Operation::Replace, &props, 7);
        let err = validate_replace_snapshot(&s, &parent).expect_err("err");
        match err {
            ChangeError::ReplaceValidationFailed {
                snapshot_id,
                reason,
            } => {
                assert_eq!(snapshot_id, 2);
                assert!(reason.contains("schema"), "{reason}");
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn plan_changes_collects_inserts_after_previous_snapshot() {
        let dir = tempfile::tempdir().expect("tempdir");
        let warehouse = format!("file://{}", dir.path().join("warehouse").display());
        let entry = test_hadoop_catalog_entry("ice", &warehouse);
        create_namespace(&entry, "ns").expect("namespace");
        create_table(
            &entry,
            "ns",
            "orders",
            &[TableColumnDef {
                name: "k1".to_string(),
                data_type: SqlType::Int,
                nullable: true,
                aggregation: None,
                default: None,
            }],
            None,
            &[IcebergPartitionFieldExpr::Identity {
                column: "k1".to_string(),
            }],
            &[],
        )
        .expect("table");
        insert_rows(&entry, "ns", "orders", &[vec![Literal::Int(1)]]).expect("first insert");
        let loaded = load_table(&entry, "ns", "orders").expect("load first");
        let previous = loaded
            .table
            .metadata()
            .current_snapshot()
            .expect("snapshot")
            .snapshot_id();

        insert_rows(&entry, "ns", "orders", &[vec![Literal::Int(2)]]).expect("second insert");
        let loaded = load_table(&entry, "ns", "orders").expect("load second");
        let batch = plan_changes(&loaded.table, previous, None, &[]).expect("plan");
        assert_eq!(batch.previous_snapshot_id, previous);
        assert_eq!(
            batch.current_snapshot_id,
            loaded
                .table
                .metadata()
                .current_snapshot()
                .unwrap()
                .snapshot_id()
        );
        assert!(!batch.inserts.is_empty());
        assert!(batch.deletes.is_empty());
        assert!(batch.equality_deletes.is_empty());
        let partition_values = &batch.inserts[0].partition_values;
        assert_eq!(partition_values.len(), 1);
        assert_eq!(partition_values[0].source_field_id, 1);
        assert_eq!(partition_values[0].source_column.as_deref(), Some("k1"));
        assert_eq!(partition_values[0].field_name, "k1");
        assert_eq!(partition_values[0].transform, "identity");
        assert_eq!(partition_values[0].value.as_primitive_str(), Some("2"));
        let returned_rows: i64 = batch
            .inserts
            .iter()
            .map(|f| f.record_count.unwrap_or_default())
            .sum();
        assert_eq!(returned_rows, 1);
    }

    #[test]
    fn plan_changes_collects_overwrite_added_and_deleted_data_files() {
        let dir = tempfile::tempdir().expect("tempdir");
        let warehouse = format!("file://{}", dir.path().join("warehouse").display());
        let entry = test_hadoop_catalog_entry("ice", &warehouse);
        create_namespace(&entry, "ns").expect("namespace");
        create_table(
            &entry,
            "ns",
            "orders",
            &[TableColumnDef {
                name: "k1".to_string(),
                data_type: SqlType::Int,
                nullable: true,
                aggregation: None,
                default: None,
            }],
            None,
            &[],
            &[
                ("format-version".to_string(), "3".to_string()),
                ("write.row-lineage".to_string(), "true".to_string()),
            ],
        )
        .expect("table");
        insert_rows(
            &entry,
            "ns",
            "orders",
            &[vec![Literal::Int(1)], vec![Literal::Int(2)]],
        )
        .expect("seed insert");
        let loaded = load_table(&entry, "ns", "orders").expect("load seed");
        let previous = loaded
            .table
            .metadata()
            .current_snapshot()
            .expect("snapshot")
            .snapshot_id();

        let catalog = build_hadoop_catalog(&entry).expect("catalog");
        let batch = RecordBatch::try_new(
            Arc::new(ArrowSchema::new(vec![Field::new(
                "k1",
                DataType::Int32,
                true,
            )])),
            vec![Arc::new(Int32Array::from(vec![3]))],
        )
        .expect("replacement batch");
        let data_files = block_on_iceberg(async {
            crate::connector::iceberg::data_writer::write_record_batches_as_data_files(
                &loaded.table,
                [batch],
            )
            .await
        })
        .expect("write runtime")
        .expect("write data file");

        let metadata = loaded.table.metadata();
        let table_ident = iceberg::TableIdent::from_strs(["ns", "orders"]).expect("ident");
        let collector = Arc::new(
            IcebergCommitCollector::new(
                CommitOpKind::Overwrite,
                table_ident,
                metadata.current_snapshot().map(|s| s.snapshot_id()),
                metadata.last_sequence_number(),
                metadata.current_schema().clone(),
                metadata.default_partition_spec().clone(),
                format!("{}/data/_staging/test-overwrite", metadata.location()),
                novarocks_types::UniqueId::new(0, 0),
            )
            .with_table_metadata(metadata.clone()),
        );
        for df in data_files {
            collector.inject_written_file(
                crate::engine::iceberg_writer::data_file_to_written_file(
                    &df,
                    metadata.default_partition_spec_id(),
                )
                .expect("written file"),
            );
        }
        block_on_iceberg(async {
            let file_io = loaded.table.file_io().clone();
            let snapshot_properties = std::collections::BTreeMap::new();
            let ctx = CommitCtx {
                collector: &collector,
                table: &loaded.table,
                catalog: &catalog,
                file_io: &file_io,
                commit_uuid: uuid::Uuid::new_v4(),
                abort_handle: collector.abort_log.clone(),
                target_ref: "main",
                snapshot_properties: &snapshot_properties,
            };
            OverwriteCommit.commit(ctx).await
        })
        .expect("overwrite runtime")
        .expect("overwrite commit");

        entry.invalidate_table_cache("ns", "orders");
        let loaded = load_table(&entry, "ns", "orders").expect("load overwrite");
        let batch = plan_changes(&loaded.table, previous, None, &[]).expect("plan overwrite");

        assert_eq!(batch.inserts.len(), 1);
        assert_eq!(batch.deleted_data_files.len(), 1);
        assert!(batch.deletes.is_empty());
        assert!(batch.equality_deletes.is_empty());
        assert_eq!(
            batch
                .inserts
                .iter()
                .map(|f| f.record_count.unwrap_or_default())
                .sum::<i64>(),
            1
        );
        assert_eq!(
            batch
                .deleted_data_files
                .iter()
                .map(|f| f.record_count.unwrap_or_default())
                .sum::<i64>(),
            2
        );
        assert!(
            batch.inserts.iter().all(|f| f.row_id_allow_list.is_none()),
            "ordinary overwrite must expose every added row as an insert; inserts={:?}",
            batch.inserts
        );
    }

    #[test]
    fn plan_changes_to_none_equivalent_to_to_some_current() {
        let dir = tempfile::tempdir().expect("tempdir");
        let warehouse = format!("file://{}", dir.path().join("warehouse").display());
        let entry = test_hadoop_catalog_entry("ice", &warehouse);
        create_namespace(&entry, "ns").expect("namespace");
        create_table(
            &entry,
            "ns",
            "orders",
            &[TableColumnDef {
                name: "k1".to_string(),
                data_type: SqlType::Int,
                nullable: true,
                aggregation: None,
                default: None,
            }],
            None,
            &[],
            &[],
        )
        .expect("table");
        insert_rows(&entry, "ns", "orders", &[vec![Literal::Int(1)]]).expect("first insert");
        let loaded = load_table(&entry, "ns", "orders").expect("load first");
        let previous = loaded
            .table
            .metadata()
            .current_snapshot()
            .expect("snapshot")
            .snapshot_id();
        insert_rows(&entry, "ns", "orders", &[vec![Literal::Int(2)]]).expect("second insert");
        let loaded = load_table(&entry, "ns", "orders").expect("load second");
        let current = loaded
            .table
            .metadata()
            .current_snapshot()
            .expect("snapshot")
            .snapshot_id();

        let batch_none = plan_changes(&loaded.table, previous, None, &[]).expect("none");
        let batch_some = plan_changes(&loaded.table, previous, Some(current), &[]).expect("some");

        assert_eq!(
            batch_none.previous_snapshot_id,
            batch_some.previous_snapshot_id
        );
        assert_eq!(
            batch_none.current_snapshot_id,
            batch_some.current_snapshot_id
        );
        assert_eq!(batch_none.inserts.len(), batch_some.inserts.len());
        assert_eq!(batch_none.deletes.len(), batch_some.deletes.len());
    }

    #[test]
    fn plan_changes_to_is_strict_ancestor_of_from_returns_lineage_broken() {
        let dir = tempfile::tempdir().expect("tempdir");
        let warehouse = format!("file://{}", dir.path().join("warehouse").display());
        let entry = test_hadoop_catalog_entry("ice", &warehouse);
        create_namespace(&entry, "ns").expect("namespace");
        create_table(
            &entry,
            "ns",
            "orders",
            &[TableColumnDef {
                name: "k1".to_string(),
                data_type: SqlType::Int,
                nullable: true,
                aggregation: None,
                default: None,
            }],
            None,
            &[],
            &[],
        )
        .expect("table");
        insert_rows(&entry, "ns", "orders", &[vec![Literal::Int(1)]]).expect("snap s0");
        let loaded = load_table(&entry, "ns", "orders").expect("load s0");
        let s0 = loaded
            .table
            .metadata()
            .current_snapshot()
            .unwrap()
            .snapshot_id();
        insert_rows(&entry, "ns", "orders", &[vec![Literal::Int(2)]]).expect("snap s1");
        let loaded = load_table(&entry, "ns", "orders").expect("load s1");
        let s1 = loaded
            .table
            .metadata()
            .current_snapshot()
            .unwrap()
            .snapshot_id();

        let err =
            plan_changes(&loaded.table, s1, Some(s0), &[]).expect_err("ancestor not descendant");
        assert!(
            matches!(err, ChangeError::LineageBroken { previous_snapshot } if previous_snapshot == s1),
            "expected LineageBroken with previous_snapshot={s1}, got {err:?}"
        );
    }

    #[test]
    fn plan_changes_truncates_to_middle_ancestor() {
        let dir = tempfile::tempdir().expect("tempdir");
        let warehouse = format!("file://{}", dir.path().join("warehouse").display());
        let entry = test_hadoop_catalog_entry("ice", &warehouse);
        create_namespace(&entry, "ns").expect("namespace");
        create_table(
            &entry,
            "ns",
            "orders",
            &[TableColumnDef {
                name: "k1".to_string(),
                data_type: SqlType::Int,
                nullable: true,
                aggregation: None,
                default: None,
            }],
            None,
            &[],
            &[],
        )
        .expect("table");
        insert_rows(&entry, "ns", "orders", &[vec![Literal::Int(1)]]).expect("snap s0");
        let loaded = load_table(&entry, "ns", "orders").expect("load s0");
        let s0 = loaded
            .table
            .metadata()
            .current_snapshot()
            .unwrap()
            .snapshot_id();
        insert_rows(&entry, "ns", "orders", &[vec![Literal::Int(2)]]).expect("snap s1 append A");
        let loaded = load_table(&entry, "ns", "orders").expect("load s1");
        let s1 = loaded
            .table
            .metadata()
            .current_snapshot()
            .unwrap()
            .snapshot_id();
        insert_rows(&entry, "ns", "orders", &[vec![Literal::Int(3)]]).expect("snap s2 append B");
        let loaded = load_table(&entry, "ns", "orders").expect("load s2");
        let s2 = loaded
            .table
            .metadata()
            .current_snapshot()
            .unwrap()
            .snapshot_id();
        insert_rows(&entry, "ns", "orders", &[vec![Literal::Int(4)]]).expect("snap s3 append C");
        let loaded = load_table(&entry, "ns", "orders").expect("load s3");
        let s3 = loaded
            .table
            .metadata()
            .current_snapshot()
            .unwrap()
            .snapshot_id();

        assert_ne!(s0, s1);
        assert_ne!(s1, s2);
        assert_ne!(s2, s3);

        let batch_mid = plan_changes(&loaded.table, s0, Some(s2), &[]).expect("truncate");
        assert_eq!(batch_mid.previous_snapshot_id, s0);
        assert_eq!(batch_mid.current_snapshot_id, s2);
        let mid_files: i64 = batch_mid
            .inserts
            .iter()
            .map(|f| f.record_count.unwrap_or_default())
            .sum();

        let batch_full = plan_changes(&loaded.table, s0, Some(s3), &[]).expect("full");
        let full_files: i64 = batch_full
            .inserts
            .iter()
            .map(|f| f.record_count.unwrap_or_default())
            .sum();

        assert!(
            mid_files < full_files,
            "mid-ancestor truncation should yield fewer rows: mid={mid_files} full={full_files}"
        );
    }

    #[test]
    fn plan_changes_to_snapshot_id_expired_returns_lineage_broken() {
        let dir = tempfile::tempdir().expect("tempdir");
        let warehouse = format!("file://{}", dir.path().join("warehouse").display());
        let entry = test_hadoop_catalog_entry("ice", &warehouse);
        create_namespace(&entry, "ns").expect("namespace");
        create_table(
            &entry,
            "ns",
            "orders",
            &[TableColumnDef {
                name: "k1".to_string(),
                data_type: SqlType::Int,
                nullable: true,
                aggregation: None,
                default: None,
            }],
            None,
            &[],
            &[],
        )
        .expect("table");
        insert_rows(&entry, "ns", "orders", &[vec![Literal::Int(1)]]).expect("snap s0");
        let loaded = load_table(&entry, "ns", "orders").expect("load s0");
        let s0 = loaded
            .table
            .metadata()
            .current_snapshot()
            .unwrap()
            .snapshot_id();
        insert_rows(&entry, "ns", "orders", &[vec![Literal::Int(2)]]).expect("snap s1");
        let loaded = load_table(&entry, "ns", "orders").expect("load s1");

        let pruned_metadata = loaded
            .table
            .metadata()
            .clone()
            .into_builder(None)
            .remove_snapshots(&[s0])
            .build()
            .expect("pruned metadata")
            .metadata;
        let pruned_table = iceberg::table::Table::builder()
            .file_io(loaded.table.file_io().clone())
            .metadata(std::sync::Arc::new(pruned_metadata))
            .identifier(loaded.table.identifier().clone())
            .build()
            .expect("pruned table");

        let from = s0 + 1;
        let err = plan_changes(&pruned_table, from, Some(s0), &[]).expect_err("expired to");
        assert!(
            matches!(err, ChangeError::LineageBroken { previous_snapshot } if previous_snapshot == from),
            "expected LineageBroken, got {err:?}"
        );
    }

    #[test]
    fn position_delete_ref_validates_parquet_with_no_content_offset() {
        let r = super::PositionDeleteRef {
            delete_file_path: "/tmp/x.parquet".to_string(),
            delete_file_size: 0,
            record_count: None,
            referenced_data_file: None,
            file_format: iceberg::spec::DataFileFormat::Parquet,
            content_offset: None,
            content_size_in_bytes: None,
            partition_values: Vec::new(),
        };
        r.validate_invariants().expect("ok");
    }

    #[test]
    fn position_delete_ref_rejects_parquet_with_content_offset() {
        let r = super::PositionDeleteRef {
            delete_file_path: "/tmp/x.parquet".to_string(),
            delete_file_size: 0,
            record_count: None,
            referenced_data_file: None,
            file_format: iceberg::spec::DataFileFormat::Parquet,
            content_offset: Some(0),
            content_size_in_bytes: None,
            partition_values: Vec::new(),
        };
        let err = r.validate_invariants().expect_err("must reject");
        assert!(matches!(err, super::ChangeError::InternalInconsistency(_)));
    }

    #[test]
    fn position_delete_ref_rejects_parquet_with_content_size() {
        let r = super::PositionDeleteRef {
            delete_file_path: "/tmp/x.parquet".to_string(),
            delete_file_size: 0,
            record_count: None,
            referenced_data_file: None,
            file_format: iceberg::spec::DataFileFormat::Parquet,
            content_offset: None,
            content_size_in_bytes: Some(120),
            partition_values: Vec::new(),
        };
        let err = r.validate_invariants().expect_err("must reject");
        assert!(matches!(err, super::ChangeError::InternalInconsistency(_)));
    }

    #[test]
    fn position_delete_ref_validates_puffin_with_full_metadata() {
        let r = super::PositionDeleteRef {
            delete_file_path: "/tmp/dv.puffin".to_string(),
            delete_file_size: 0,
            record_count: None,
            referenced_data_file: Some("/tmp/data.parquet".to_string()),
            file_format: iceberg::spec::DataFileFormat::Puffin,
            content_offset: Some(4),
            content_size_in_bytes: Some(120),
            partition_values: Vec::new(),
        };
        r.validate_invariants().expect("ok");
    }

    #[test]
    fn position_delete_ref_rejects_puffin_missing_offset() {
        let r = super::PositionDeleteRef {
            delete_file_path: "/tmp/dv.puffin".to_string(),
            delete_file_size: 0,
            record_count: None,
            referenced_data_file: Some("/tmp/data.parquet".to_string()),
            file_format: iceberg::spec::DataFileFormat::Puffin,
            content_offset: None,
            content_size_in_bytes: Some(120),
            partition_values: Vec::new(),
        };
        let err = r.validate_invariants().expect_err("must reject");
        assert!(matches!(err, super::ChangeError::InternalInconsistency(_)));
    }

    #[test]
    fn position_delete_ref_rejects_puffin_missing_referenced_data_file() {
        let r = super::PositionDeleteRef {
            delete_file_path: "/tmp/dv.puffin".to_string(),
            delete_file_size: 0,
            record_count: None,
            referenced_data_file: None,
            file_format: iceberg::spec::DataFileFormat::Puffin,
            content_offset: Some(4),
            content_size_in_bytes: Some(120),
            partition_values: Vec::new(),
        };
        let err = r.validate_invariants().expect_err("must reject");
        assert!(matches!(err, super::ChangeError::InternalInconsistency(_)));
    }

    #[test]
    fn plan_changes_rejects_pruned_previous_snapshot() {
        let dir = tempfile::tempdir().expect("tempdir");
        let warehouse = format!("file://{}", dir.path().join("warehouse").display());
        let entry = test_hadoop_catalog_entry("ice", &warehouse);
        create_namespace(&entry, "ns").expect("namespace");
        create_table(
            &entry,
            "ns",
            "orders",
            &[TableColumnDef {
                name: "k1".to_string(),
                data_type: SqlType::Int,
                nullable: true,
                aggregation: None,
                default: None,
            }],
            None,
            &[],
            &[],
        )
        .expect("table");
        insert_rows(&entry, "ns", "orders", &[vec![Literal::Int(1)]]).expect("first insert");
        let loaded = load_table(&entry, "ns", "orders").expect("load first");
        let previous = loaded
            .table
            .metadata()
            .current_snapshot()
            .expect("snapshot")
            .snapshot_id();

        insert_rows(&entry, "ns", "orders", &[vec![Literal::Int(2)]]).expect("second insert");
        let loaded = load_table(&entry, "ns", "orders").expect("load second");

        let pruned_metadata = loaded
            .table
            .metadata()
            .clone()
            .into_builder(None)
            .remove_snapshots(&[previous])
            .build()
            .expect("pruned metadata")
            .metadata;
        let pruned_table = iceberg::table::Table::builder()
            .file_io(loaded.table.file_io().clone())
            .metadata(std::sync::Arc::new(pruned_metadata))
            .identifier(loaded.table.identifier().clone())
            .build()
            .expect("pruned table");

        let err = plan_changes(&pruned_table, previous, None, &[]).expect_err("should fail");
        assert!(
            matches!(err, ChangeError::LineageBroken { previous_snapshot } if previous_snapshot == previous)
        );
    }
}
