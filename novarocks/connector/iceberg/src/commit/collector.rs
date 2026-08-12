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

//! Query-scoped state shared between the engine flow and the commit-action.
//!
//! Lifetime: created by Iceberg write preparation or `engine/delete_flow.rs`
//! before lowering, dropped after `run_iceberg_commit` returns.
//!
//! Provider-owned writer reports are injected before commit and converted into
//! [`WrittenFile`]s. Each file path is mirrored into the [`AbortLog`] so that a
//! later commit failure can clean up via OpenDAL.

use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use crate::iceberg::TableIdent;
use crate::iceberg::spec::{
    Datum, PartitionSpecRef, PrimitiveType, SchemaRef, TableMetadata, Type,
};
use std::collections::{BTreeMap, HashMap};

use crate::commit::PositionDeleteGroup;
use crate::commit::abort::AbortLog;
use crate::commit::report::IcebergWriterReport;
use crate::commit::{CommitOpKind, WrittenFile};
use crate::stats_assembler::FileSketchSet;

#[derive(Default)]
struct StagedEffectCounters {
    injected_data_rows: u128,
    appended_data_rows: u128,
    delete_rows: u128,
}

#[allow(dead_code)]
fn staged_row_count(value: u128) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

/// Query-scoped Iceberg INSERT / INSERT OVERWRITE / DELETE state.
pub struct IcebergCommitCollector {
    pub op_kind: CommitOpKind,
    pub table_ident: TableIdent,
    /// `None` for the first write into a fresh table.
    pub base_snapshot_id: Option<i64>,
    pub base_sequence_number: i64,
    pub schema: SchemaRef,
    pub partition_spec: PartitionSpecRef,
    metadata: Option<TableMetadata>,
    pub staging_dir: String,
    pub abort_log: Arc<AbortLog>,
    /// Files supplied by the provider execution/control boundary.
    injected: Mutex<Vec<WrittenFile>>,
    /// Net-new data files (content == Data, NO preserved `_row_id`) that a folded
    /// MERGE not-matched INSERT branch produced. Kept separate from `injected`
    /// (the reuse channel) so the `RowDeltaDvFromFilesCommit` entry can route
    /// them into the action's fresh `appended_files` channel — those rows MUST
    /// draw fresh `_row_id`s, unlike the reuse replacement rows in `injected`.
    /// Drained via [`take_appended_files`]. Empty for every non-folded path,
    /// keeping MOR-UPDATE / DELETE byte-identical.
    appended: Mutex<Vec<WrittenFile>>,
    /// Cumulative logical effect of every validated file/group injected into
    /// this collector. Commit actions drain the concrete channels above, but
    /// MV refresh still needs the exact staged row counts after an external
    /// commit has completed in connector control.
    staged_effect: Mutex<StagedEffectCounters>,
    /// Grouped `(referenced_data_file, positions)` records produced by the
    /// engine-side row-lineage DELETE flow. Only used when
    /// `op_kind == CommitOpKind::RowDeltaDv`. The `RowDeltaDvCommit` action
    /// drains this channel via [`take_delete_groups`].
    delete_groups: Mutex<Vec<PositionDeleteGroup>>,
    /// Per-file Theta sketch sets produced by the sink for Iceberg Puffin
    /// NDV statistics. One entry per written Parquet data file. Optional —
    /// non-Iceberg sinks and tests that do not exercise stats can leave
    /// this empty. Drained by [`take_sketch_sets`] at commit time.
    sketch_sets: Mutex<Vec<FileSketchSet>>,
    /// When set, signals that the engine wrote data files whose `_row_id`
    /// values are already stamped at the reserved field IDs inside the
    /// file (e.g. the OPTIMIZE row-lineage preserve path). The commit
    /// action then skips fresh `next_row_id` allocation, omits the
    /// snapshot's `row_range`, and propagates the per-file row identity
    /// without rebasing it onto a contiguous range.
    preserve_row_lineage: AtomicBool,
    committed: AtomicBool,
    manifest_cleanup_token: Mutex<Option<String>>,
}

impl crate::commit::service::CommitRecoverySource for IcebergCommitCollector {
    fn recovery_table_ident(&self) -> String {
        self.table_ident.to_string()
    }

    fn recovery_op_kind(&self) -> CommitOpKind {
        self.op_kind
    }

    fn recovery_base_snapshot_id(&self) -> Option<i64> {
        self.base_snapshot_id
    }

    fn recovery_base_sequence_number(&self) -> i64 {
        self.base_sequence_number
    }

    fn recovery_staging_dir(&self) -> String {
        self.staging_dir.clone()
    }

    fn recovery_manifest_cleanup_token(&self) -> Option<String> {
        self.manifest_cleanup_token
            .lock()
            .expect("manifest cleanup token poisoned")
            .clone()
    }
}

#[allow(dead_code)]
impl IcebergCommitCollector {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        op_kind: CommitOpKind,
        table_ident: TableIdent,
        base_snapshot_id: Option<i64>,
        base_sequence_number: i64,
        schema: SchemaRef,
        partition_spec: PartitionSpecRef,
        staging_dir: String,
    ) -> Self {
        Self {
            op_kind,
            table_ident,
            base_snapshot_id,
            base_sequence_number,
            schema,
            partition_spec,
            metadata: None,
            staging_dir,
            abort_log: Arc::new(AbortLog::new()),
            injected: Mutex::new(Vec::new()),
            appended: Mutex::new(Vec::new()),
            staged_effect: Mutex::new(StagedEffectCounters::default()),
            delete_groups: Mutex::new(Vec::new()),
            sketch_sets: Mutex::new(Vec::new()),
            preserve_row_lineage: AtomicBool::new(false),
            committed: AtomicBool::new(false),
            manifest_cleanup_token: Mutex::new(None),
        }
    }

    pub(crate) fn set_manifest_cleanup_token(&self, token: String) {
        *self
            .manifest_cleanup_token
            .lock()
            .expect("manifest cleanup token poisoned") = Some(token);
    }

    pub(crate) fn with_table_metadata(mut self, metadata: TableMetadata) -> Self {
        self.metadata = Some(metadata);
        self
    }

    /// Mark that the data files injected via [`inject_written_file`] carry
    /// per-row `_row_id` values stamped at the reserved field IDs. The
    /// rewrite commit-action consumes this signal to skip `next_row_id`
    /// allocation and `row_range` emission. Idempotent.
    pub fn mark_preserve_row_lineage(&self) {
        self.preserve_row_lineage.store(true, Ordering::Release);
    }

    /// Returns true when [`mark_preserve_row_lineage`] was called for this
    /// query.
    pub fn preserve_row_lineage(&self) -> bool {
        self.preserve_row_lineage.load(Ordering::Acquire)
    }

    /// Push a grouped DELETE position vector into the collector. Used by the
    /// engine-side row-lineage DELETE flow so that `RowDeltaDvCommit` can
    /// build the merged Puffin DV files at commit time.
    pub fn inject_delete_group(&self, group: PositionDeleteGroup) {
        let mut effect = self
            .staged_effect
            .lock()
            .expect("collector staged_effect lock poisoned");
        effect.delete_rows = effect
            .delete_rows
            .saturating_add(group.positions.len() as u128);
        self.delete_groups
            .lock()
            .expect("collector delete_groups lock poisoned")
            .push(group);
    }

    /// Drain the grouped DELETE position vectors registered via
    /// [`inject_delete_group`].
    pub fn take_delete_groups(&self) -> Vec<PositionDeleteGroup> {
        let mut guard = self
            .delete_groups
            .lock()
            .expect("collector delete_groups lock poisoned");
        std::mem::take(&mut *guard)
    }

    /// Record a per-file Theta sketch set produced by the sink for Iceberg
    /// Puffin NDV statistics. Used by both the runtime IcebergSink (pipeline
    /// path) and the standalone iceberg_writer path.
    pub fn inject_sketch_set(&self, set: FileSketchSet) {
        self.sketch_sets
            .lock()
            .expect("collector sketch_sets lock poisoned")
            .push(set);
    }

    /// Drain the per-file sketch sets registered via
    /// [`inject_sketch_set`] plus any pushed through the runtime
    /// `sink_commit` side channel for this query's fragment instance.
    /// Each call is destructive — sketches cannot be cloned, so the
    /// caller (typically `StatsAssembler::assemble`) consumes them once.
    pub fn take_sketch_sets(&self) -> Vec<FileSketchSet> {
        {
            let mut guard = self
                .sketch_sets
                .lock()
                .expect("collector sketch_sets lock poisoned");
            std::mem::take(&mut *guard)
        }
    }

    /// Cumulative `record_count` across every injected [`WrittenFile`] with
    /// `content == Data`. Commit actions may already have drained the concrete
    /// file channel; this accounting evidence remains available to the MV
    /// refresh publisher.
    pub fn injected_data_record_count(&self) -> i64 {
        let effect = self
            .staged_effect
            .lock()
            .expect("collector staged_effect lock poisoned");
        staged_row_count(effect.injected_data_rows)
    }

    /// Sum of data rows across both preserved/reuse files and net-new appended
    /// files. Used by change-stream based IMV refresh accounting: both channels
    /// materialize rows into the target snapshot, even though RowDeltaDvFromFiles
    /// keeps them separate for row-lineage assignment.
    pub fn injected_or_appended_data_record_count(&self) -> i64 {
        let effect = self
            .staged_effect
            .lock()
            .expect("collector staged_effect lock poisoned");
        staged_row_count(
            effect
                .injected_data_rows
                .saturating_add(effect.appended_data_rows),
        )
    }

    /// Sum of delete-side rows across coordinator-built delete groups and
    /// BE-written delete files. Non-destructive; used by IVM-A1 refresh
    /// accounting and empty-write gating.
    pub fn injected_delete_record_count(&self) -> i64 {
        let effect = self
            .staged_effect
            .lock()
            .expect("collector staged_effect lock poisoned");
        staged_row_count(effect.delete_rows)
    }

    /// Pre-load a written file into the collector. Used by the standalone
    /// engine when it writes data files via iceberg-rust `DataFileWriter`
    /// directly (no IcebergSink in the loop). Each path is recorded in the
    /// [`AbortLog`] so abort cleanup still works.
    pub fn inject_written_file(&self, wf: WrittenFile) {
        self.inject_written_files(vec![wf]);
    }

    /// Pre-load written files into the collector after they have all been
    /// validated. Each path is recorded in the [`AbortLog`] so abort cleanup
    /// still works.
    pub(crate) fn inject_written_files(&self, files: Vec<WrittenFile>) {
        use crate::iceberg::spec::DataContentType;

        {
            let mut effect = self
                .staged_effect
                .lock()
                .expect("collector staged_effect lock poisoned");
            for file in &files {
                match file.content {
                    DataContentType::Data => {
                        effect.injected_data_rows = effect
                            .injected_data_rows
                            .saturating_add(file.record_count as u128);
                    }
                    DataContentType::PositionDeletes => {
                        effect.delete_rows =
                            effect.delete_rows.saturating_add(file.record_count as u128);
                    }
                    DataContentType::EqualityDeletes => {}
                }
            }
        }
        let mut guard = self
            .injected
            .lock()
            .expect("collector injected lock poisoned");
        for wf in files {
            self.abort_log.record_data_file(wf.path.clone());
            guard.push(wf);
        }
    }

    /// Pre-load net-new INSERT data files (folded MERGE not-matched branch) into
    /// the fresh-row-lineage channel. These rows carry NO preserved `_row_id`
    /// and MUST draw fresh ids at commit time. Kept distinct from
    /// [`inject_written_files`] (the reuse channel). Each path is recorded in the
    /// [`AbortLog`] so abort cleanup still works.
    pub(crate) fn inject_appended_files(&self, files: Vec<WrittenFile>) {
        use crate::iceberg::spec::DataContentType;

        {
            let mut effect = self
                .staged_effect
                .lock()
                .expect("collector staged_effect lock poisoned");
            for file in &files {
                if matches!(file.content, DataContentType::Data) {
                    effect.appended_data_rows = effect
                        .appended_data_rows
                        .saturating_add(file.record_count as u128);
                }
            }
        }
        let mut guard = self
            .appended
            .lock()
            .expect("collector appended lock poisoned");
        for wf in files {
            self.abort_log.record_data_file(wf.path.clone());
            guard.push(wf);
        }
    }

    /// Drain the net-new INSERT data files registered via
    /// [`inject_appended_files`]. The `RowDeltaDvFromFilesCommit` entry routes
    /// these into the action's fresh `appended_files` channel.
    pub(crate) fn take_appended_files(&self) -> Vec<WrittenFile> {
        let mut guard = self
            .appended
            .lock()
            .expect("collector appended lock poisoned");
        std::mem::take(&mut *guard)
    }

    /// Read-only check that no net-new INSERT data files were registered via
    /// [`inject_appended_files`]. Used by `CowUpdateCommit` to assert it routes
    /// appended rows through the rewrite set, NOT this channel. Non-draining, so
    /// it is safe inside `debug_assert!` (no debug/release state divergence).
    pub(crate) fn appended_is_empty(&self) -> bool {
        self.appended
            .lock()
            .expect("collector appended lock poisoned")
            .is_empty()
    }

    pub(crate) fn inject_writer_report(&self, report: IcebergWriterReport) -> Result<(), String> {
        self.inject_writer_reports([report])
    }

    /// Pre-load a batch of writer reports atomically: either every report
    /// converts and becomes visible, or the collector and abort log remain
    /// unchanged.
    pub(crate) fn inject_writer_reports<I>(&self, reports: I) -> Result<(), String>
    where
        I: IntoIterator<Item = IcebergWriterReport>,
    {
        let files = reports
            .into_iter()
            .map(|report| self.convert_writer_report(report))
            .collect::<Result<Vec<_>, _>>()?;
        self.inject_written_files(files);
        Ok(())
    }

    /// Returns the [`WrittenFile`] set produced by this query.
    ///
    /// The distributed writer contract requires provider-owned reports to be
    /// decoded and injected before this point. The collector never reads a
    /// process-global runtime report side channel.
    pub fn take_written_files(&self) -> Result<Vec<WrittenFile>, String> {
        Ok(std::mem::take(
            &mut *self
                .injected
                .lock()
                .expect("collector injected lock poisoned"),
        ))
    }

    /// Reconstruct a [`WrittenFile`] from a writer report.
    ///
    /// As of P6 this is descriptor-authoritative for Iceberg partition values
    /// and lossless against the inject path
    /// (`engine::iceberg_writer::data_file_to_written_file`) for data and
    /// delete files: column statistics (`column_stats`), `first_row_id`,
    /// `equality_ids`, and `key_metadata` all round-trip. Two boundaries
    /// are intentional and handled (or deferred) elsewhere:
    ///
    /// - Column-stat bounds for field-ids absent from the table schema (e.g.
    ///   stats left behind for a dropped column) are skipped rather than
    ///   decoded, matching the inject path's tolerance for stale stats.
    /// - Puffin/NDV sketches are not part of `WrittenFile`; they ride the
    ///   out-of-band sketch channel (`take_sketch_sets` /
    ///   `runtime::sink_commit::take_sketch_sets`), which is in-process today.
    ///   Cross-node sketch transport is required only when multi-BE append is
    ///   cut over and is out of scope for PR-0.
    pub(crate) fn convert_writer_report(
        &self,
        report: IcebergWriterReport,
    ) -> Result<WrittenFile, String> {
        use crate::delete_file::IcebergFileContent;
        use crate::iceberg::spec::{DataContentType, DataFileFormat};

        let IcebergWriterReport { file, .. } = report;
        let format_name = file.format.clone();
        let format = DataFileFormat::from_str(&format_name).map_err(|e| {
            format!("unsupported Iceberg writer report format `{format_name}`: {e}")
        })?;
        let content = match file.content {
            IcebergFileContent::Data => DataContentType::Data,
            IcebergFileContent::PositionDeletes => DataContentType::PositionDeletes,
            IcebergFileContent::EqualityDeletes => DataContentType::EqualityDeletes,
        };
        validate_puffin_dv_descriptor(
            format.clone(),
            content.clone(),
            file.referenced_data_file.as_deref(),
            file.content_offset,
            file.content_size_in_bytes,
            file.cardinality,
        )?;

        let partition = file.partition;
        let stats = file.column_stats.unwrap_or_default();
        let column_sizes = i64_map_to_u64(Some(stats.column_sizes), "column_sizes")?;
        let value_counts = i64_map_to_u64(Some(stats.value_counts), "value_counts")?;
        let null_value_counts = i64_map_to_u64(Some(stats.null_value_counts), "null_value_counts")?;
        let nan_value_counts = i64_map_to_u64(Some(stats.nan_value_counts), "nan_value_counts")?;
        let lower_bounds = self.decode_bounds(Some(stats.lower_bounds), "lower_bounds")?;
        let upper_bounds = self.decode_bounds(Some(stats.upper_bounds), "upper_bounds")?;
        let split_offsets = i64_vec_non_negative(file.split_offsets, "split_offsets")?;
        let first_row_id = i64_option_non_negative(file.first_row_id, "first_row_id")?;
        let content_offset = i64_option_non_negative(file.content_offset, "content_offset")?;
        let content_size_in_bytes =
            i64_option_non_negative(file.content_size_in_bytes, "content_size_in_bytes")?;

        Ok(WrittenFile {
            path: file.path,
            format,
            content,
            partition_values: partition.partition_values,
            partition_spec_id: partition.partition_spec_id,
            record_count: i64_to_u64(file.record_count, "record_count")?,
            file_size_in_bytes: i64_to_u64(file.file_size_in_bytes, "file_size_in_bytes")?,
            split_offsets,
            column_sizes,
            value_counts,
            null_value_counts,
            nan_value_counts,
            lower_bounds,
            upper_bounds,
            key_metadata: file.key_metadata,
            referenced_data_file: file.referenced_data_file,
            equality_ids: file.equality_ids,
            first_row_id,
            content_offset,
            content_size_in_bytes,
            cardinality: file
                .cardinality
                .map(|c| i64_to_u64(c, "cardinality"))
                .transpose()?,
        })
    }

    /// Decode per-column bound bytes (Iceberg single-value binary encoding)
    /// back into `Datum`s, using the table schema to resolve each field id's
    /// primitive type. Inverse of `data_writer::datum_bounds_to_bytes`.
    fn decode_bounds(
        &self,
        bounds: Option<BTreeMap<i32, Vec<u8>>>,
        field: &str,
    ) -> Result<HashMap<i32, Datum>, String> {
        let mut out = HashMap::new();
        for (field_id, bytes) in bounds.unwrap_or_default() {
            // Iceberg writers may leave bounds for retired field-ids in a file
            // after a column is dropped. We cannot decode bytes without the
            // field's type, so skip unknown ids rather than failing the commit.
            // This matches the inject path (`data_file_to_written_file`), which
            // carries bounds through without validating against the schema.
            let Some(schema_field) = self.schema.field_by_id(field_id) else {
                continue;
            };
            let prim = match &*schema_field.field_type {
                Type::Primitive(p) => p.clone(),
                other => {
                    return Err(format!(
                        "column stat {field} field id {field_id} has non-primitive type {other:?}"
                    ));
                }
            };
            if matches!(prim, PrimitiveType::Variant) {
                continue;
            }
            let datum = Datum::try_from_bytes(&bytes, prim)
                .map_err(|e| format!("decode column stat {field}[{field_id}] failed: {e}"))?;
            out.insert(field_id, datum);
        }
        Ok(out)
    }

    pub fn mark_committed(&self) {
        self.committed.store(true, Ordering::SeqCst);
    }

    pub fn is_committed(&self) -> bool {
        self.committed.load(Ordering::SeqCst)
    }
}

fn validate_puffin_dv_descriptor(
    format: crate::iceberg::spec::DataFileFormat,
    content: crate::iceberg::spec::DataContentType,
    referenced_data_file: Option<&str>,
    content_offset: Option<i64>,
    content_size_in_bytes: Option<i64>,
    cardinality: Option<i64>,
) -> Result<(), String> {
    use crate::iceberg::spec::{DataContentType, DataFileFormat};

    if format != DataFileFormat::Puffin || content != DataContentType::PositionDeletes {
        return Ok(());
    }
    match referenced_data_file {
        Some(path) if !path.is_empty() => {}
        _ => {
            return Err(
                "Puffin position-delete DV requires non-empty referenced_data_file".to_string(),
            );
        }
    }
    match content_offset {
        Some(offset) if offset >= 0 => {}
        Some(offset) => {
            return Err(format!(
                "Puffin position-delete DV content_offset must be non-negative, got {offset}"
            ));
        }
        None => {
            return Err("Puffin position-delete DV requires content_offset".to_string());
        }
    }
    match content_size_in_bytes {
        Some(size) if size >= 0 => {}
        Some(size) => {
            return Err(format!(
                "Puffin position-delete DV content_size_in_bytes must be non-negative, got {size}"
            ));
        }
        None => {
            return Err("Puffin position-delete DV requires content_size_in_bytes".to_string());
        }
    }
    match cardinality {
        Some(value) if value >= 0 => Ok(()),
        Some(value) => Err(format!(
            "Puffin position-delete DV cardinality must be non-negative, got {value}"
        )),
        None => Err("Puffin position-delete DV requires cardinality".to_string()),
    }
}

/// Convert signed writer-report counts into the `WrittenFile`
/// `HashMap<i32, u64>` representation.
fn i64_to_u64(value: i64, field: &str) -> Result<u64, String> {
    u64::try_from(value).map_err(|_| format!("iceberg {field} value {value} is negative"))
}

fn i64_option_non_negative(value: Option<i64>, field: &str) -> Result<Option<i64>, String> {
    if let Some(value) = value
        && value < 0
    {
        return Err(format!("iceberg {field} value {value} is negative"));
    }
    Ok(value)
}

fn i64_vec_non_negative(values: Option<Vec<i64>>, field: &str) -> Result<Vec<i64>, String> {
    values
        .unwrap_or_default()
        .into_iter()
        .enumerate()
        .map(|(idx, value)| {
            if value < 0 {
                Err(format!("iceberg {field}[{idx}] value {value} is negative"))
            } else {
                Ok(value)
            }
        })
        .collect()
}

fn i64_map_to_u64(
    map: Option<BTreeMap<i32, i64>>,
    field: &str,
) -> Result<HashMap<i32, u64>, String> {
    map.unwrap_or_default()
        .into_iter()
        .map(|(field_id, value)| {
            i64_to_u64(value, &format!("column stat {field}[{field_id}]"))
                .map(|value| (field_id, value))
        })
        .collect()
}
