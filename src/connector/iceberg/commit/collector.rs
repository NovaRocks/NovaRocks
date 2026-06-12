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
//! Lifetime: created in `engine/insert_flow.rs` or `engine/delete_flow.rs`
//! before lowering, dropped after `run_iceberg_commit` returns.
//!
//! At pipeline finish, [`take_written_files`](IcebergCommitCollector::take_written_files)
//! drains the per-fragment-instance entries from [`runtime::sink_commit`] and
//! converts the recorded `TIcebergDataFile` values into [`WrittenFile`]s. Each
//! file path is mirrored into the [`AbortLog`] so that a later commit failure
//! can clean up via OpenDAL.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use base64::Engine;
use chrono::{DateTime, Datelike, NaiveDate, NaiveDateTime};
use iceberg::TableIdent;
use iceberg::spec::{
    Datum, Literal, PartitionSpecRef, PrimitiveLiteral, PrimitiveType, SchemaRef, Struct,
    Transform, Type,
};
use std::collections::{BTreeMap, HashMap};

use crate::common::types::UniqueId;

use super::abort::AbortLog;
use super::position_delete_writer::PositionDeleteGroup;
use super::types::{CommitOpKind, WrittenFile};
use crate::connector::iceberg::stats_assembler::FileSketchSet;

/// Query-scoped Iceberg INSERT / INSERT OVERWRITE / DELETE state.
pub struct IcebergCommitCollector {
    pub op_kind: CommitOpKind,
    pub table_ident: TableIdent,
    /// `None` for the first write into a fresh table.
    pub base_snapshot_id: Option<i64>,
    pub base_sequence_number: i64,
    pub schema: SchemaRef,
    pub partition_spec: PartitionSpecRef,
    pub staging_dir: String,
    pub finst_id: UniqueId,
    pub abort_log: Arc<AbortLog>,
    /// Files supplied directly by the engine layer when it bypasses the
    /// IcebergSink path (e.g. standalone INSERT/DELETE that uses iceberg-rust
    /// `DataFileWriter` directly, mirroring phase4a). When non-empty,
    /// [`take_written_files`] returns these instead of draining
    /// [`runtime::sink_commit`]. [`AbortLog`] entries are still recorded
    /// because abort cleanup applies regardless of which channel produced
    /// the file.
    injected: Mutex<Vec<WrittenFile>>,
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
}

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
        finst_id: UniqueId,
    ) -> Self {
        Self {
            op_kind,
            table_ident,
            base_snapshot_id,
            base_sequence_number,
            schema,
            partition_spec,
            staging_dir,
            finst_id,
            abort_log: Arc::new(AbortLog::new()),
            injected: Mutex::new(Vec::new()),
            delete_groups: Mutex::new(Vec::new()),
            sketch_sets: Mutex::new(Vec::new()),
            preserve_row_lineage: AtomicBool::new(false),
            committed: AtomicBool::new(false),
        }
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
        let mut sets = {
            let mut guard = self
                .sketch_sets
                .lock()
                .expect("collector sketch_sets lock poisoned");
            std::mem::take(&mut *guard)
        };
        sets.extend(crate::runtime::sink_commit::take_sketch_sets(self.finst_id));
        sets
    }

    /// Sum of `record_count` across all currently-injected
    /// [`WrittenFile`]s with `content == Data`. Non-destructive; used by
    /// IVM-A1 refresh accounting (added-row count for the MV row total).
    pub fn injected_data_record_count(&self) -> i64 {
        use iceberg::spec::DataContentType;
        let guard = self
            .injected
            .lock()
            .expect("collector injected lock poisoned");
        guard
            .iter()
            .filter(|wf| matches!(wf.content, DataContentType::Data))
            .map(|wf| wf.record_count as i64)
            .sum()
    }

    /// Sum of `positions.len()` across all currently-injected
    /// [`PositionDeleteGroup`]s. Non-destructive; used by IVM-A1 refresh
    /// accounting (deleted-row count for the MV row total).
    pub fn injected_delete_record_count(&self) -> i64 {
        let guard = self
            .delete_groups
            .lock()
            .expect("collector delete_groups lock poisoned");
        guard.iter().map(|g| g.positions.len() as i64).sum()
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
        let mut guard = self
            .injected
            .lock()
            .expect("collector injected lock poisoned");
        for wf in files {
            self.abort_log.record_data_file(wf.path.clone());
            guard.push(wf);
        }
    }

    pub(crate) fn has_injected_written_files(&self) -> bool {
        !self
            .injected
            .lock()
            .expect("collector injected lock poisoned")
            .is_empty()
    }

    /// Convert one writer-reported sink commit payload without mutating the
    /// collector. This lets callers validate all writer payloads before any file
    /// becomes visible to the commit-action or abort log.
    pub(crate) fn convert_sink_commit_info(
        &self,
        info: crate::types::TSinkCommitInfo,
    ) -> Result<WrittenFile, String> {
        let df = info
            .iceberg_data_file
            .ok_or_else(|| "sink_commit_info missing iceberg_data_file".to_string())?;
        self.convert(df)
    }

    /// Pre-load one writer-reported sink commit payload without consulting the
    /// process-global `runtime::sink_commit` table. Used by coordinated writes
    /// where the engine already collected all writer reports.
    pub(crate) fn inject_sink_commit_info(
        &self,
        info: crate::types::TSinkCommitInfo,
    ) -> Result<(), String> {
        self.inject_sink_commit_infos([info])
    }

    /// Pre-load a batch of writer-reported sink commit payloads atomically:
    /// either every payload converts and becomes visible, or the collector and
    /// abort log remain unchanged.
    pub(crate) fn inject_sink_commit_infos<I>(&self, infos: I) -> Result<(), String>
    where
        I: IntoIterator<Item = crate::types::TSinkCommitInfo>,
    {
        let files = infos
            .into_iter()
            .map(|info| self.convert_sink_commit_info(info))
            .collect::<Result<Vec<_>, _>>()?;
        self.inject_written_files(files);
        Ok(())
    }

    /// Returns the [`WrittenFile`] set produced by this query.
    ///
    /// If the engine pre-loaded files via [`inject_written_file`], those are
    /// returned and the per-fragment-instance `sink_commit` table is left
    /// untouched. Otherwise the collector drains
    /// [`runtime::sink_commit::list`] and converts each `TIcebergDataFile`
    /// into a [`WrittenFile`].
    pub fn take_written_files(&self) -> Result<Vec<WrittenFile>, String> {
        {
            let mut guard = self
                .injected
                .lock()
                .expect("collector injected lock poisoned");
            if !guard.is_empty() {
                return Ok(std::mem::take(&mut *guard));
            }
        }
        let infos = crate::runtime::sink_commit::list(self.finst_id);
        let mut out = Vec::with_capacity(infos.len());
        for info in infos {
            let df = info
                .iceberg_data_file
                .ok_or_else(|| "sink_commit_info missing iceberg_data_file".to_string())?;
            let wf = self.convert(df)?;
            self.abort_log.record_data_file(wf.path.clone());
            out.push(wf);
        }
        Ok(out)
    }

    /// Reconstruct a [`WrittenFile`] from a writer-reported `TIcebergDataFile`.
    ///
    /// As of PR-0 this is lossless against the inject path
    /// (`engine::iceberg_writer::data_file_to_written_file`) for data and
    /// delete files: column statistics (`column_stats`), `first_row_id`,
    /// `equality_ids`, and `key_metadata` all round-trip. Three boundaries
    /// are intentional and handled (or deferred) elsewhere:
    ///
    /// - Partition values are decoded from `partition_path`; transform
    ///   directory values are converted back to Iceberg partition literals.
    /// - Column-stat bounds for field-ids absent from the table schema (e.g.
    ///   stats left behind for a dropped column) are skipped rather than
    ///   decoded, matching the inject path's tolerance for stale stats.
    /// - Puffin/NDV sketches are not part of `WrittenFile`; they ride the
    ///   out-of-band sketch channel (`take_sketch_sets` /
    ///   `runtime::sink_commit::take_sketch_sets`), which is in-process today.
    ///   Cross-node sketch transport is required only when multi-BE append is
    ///   cut over and is out of scope for PR-0.
    fn convert(&self, df: crate::types::TIcebergDataFile) -> Result<WrittenFile, String> {
        use iceberg::spec::{DataContentType, DataFileFormat};

        let path = df
            .path
            .ok_or_else(|| "TIcebergDataFile missing path".to_string())?;
        let content = match df
            .file_content
            .unwrap_or(crate::types::TIcebergFileContent::DATA)
        {
            crate::types::TIcebergFileContent::DATA => DataContentType::Data,
            crate::types::TIcebergFileContent::POSITION_DELETES => DataContentType::PositionDeletes,
            crate::types::TIcebergFileContent::EQUALITY_DELETES => DataContentType::EqualityDeletes,
            other => {
                return Err(format!(
                    "unexpected TIcebergFileContent variant {other:?} in sink_commit_info"
                ));
            }
        };

        let partition_spec_id = df
            .partition_spec_id
            .unwrap_or(self.partition_spec.spec_id());
        let partition_values = parse_partition_path(
            df.partition_path.as_deref().unwrap_or(""),
            &self.partition_spec,
            &self.schema,
            df.partition_null_fingerprint.as_deref(),
        )?;

        let stats = df.column_stats.unwrap_or_default();
        let column_sizes = i64_map_to_u64(stats.column_sizes, "column_sizes")?;
        let value_counts = i64_map_to_u64(stats.value_counts, "value_counts")?;
        let null_value_counts = i64_map_to_u64(stats.null_value_counts, "null_value_counts")?;
        let lower_bounds = self.decode_bounds(stats.lower_bounds, "lower_bounds")?;
        let upper_bounds = self.decode_bounds(stats.upper_bounds, "upper_bounds")?;

        Ok(WrittenFile {
            path,
            // The standalone sink is Parquet-only (enforced in `sink.rs`), so the
            // wire `df.format` is intentionally not re-read here. Revisit if a
            // non-Parquet sink is added.
            format: DataFileFormat::Parquet,
            content,
            partition_values,
            partition_spec_id,
            record_count: df.record_count.unwrap_or(0).max(0) as u64,
            file_size_in_bytes: df.file_size_in_bytes.unwrap_or(0).max(0) as u64,
            split_offsets: df.split_offsets.unwrap_or_default(),
            column_sizes,
            value_counts,
            null_value_counts,
            lower_bounds,
            upper_bounds,
            key_metadata: df.key_metadata,
            referenced_data_file: df.referenced_data_file,
            equality_ids: df.equality_ids,
            first_row_id: df.first_row_id,
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

/// Convert a thrift `map<i32, i64>` column-stat map into the `WrittenFile`
/// `HashMap<i32, u64>` representation. Inverse of `data_writer::u64_stats_to_i64`.
fn i64_map_to_u64(
    map: Option<BTreeMap<i32, i64>>,
    field: &str,
) -> Result<HashMap<i32, u64>, String> {
    map.unwrap_or_default()
        .into_iter()
        .map(|(field_id, value)| {
            u64::try_from(value)
                .map(|value| (field_id, value))
                .map_err(|_| {
                    format!("iceberg column stat {field}[{field_id}] value {value} is negative")
                })
        })
        .collect()
}

/// Decode an Iceberg v2-style partition path (e.g. `p=1/q=A`) into a
/// [`Struct`] keyed by the partition spec's field order.
fn parse_partition_path(
    path: &str,
    spec: &PartitionSpecRef,
    schema: &SchemaRef,
    null_fingerprint: Option<&str>,
) -> Result<Struct, String> {
    if path.is_empty() {
        if !spec.fields().is_empty() {
            return Err(format!(
                "partition_path is empty but spec expects {} fields",
                spec.fields().len()
            ));
        }
        return Ok(Struct::empty());
    }
    let trimmed = path.trim_matches('/');
    let segments: Vec<&str> = trimmed.split('/').collect();
    if segments.len() != spec.fields().len() {
        return Err(format!(
            "partition_path `{path}` has {} segments but spec expects {}",
            segments.len(),
            spec.fields().len()
        ));
    }
    let null_fingerprint = null_fingerprint.filter(|fp| !fp.is_empty());
    if let Some(fp) = null_fingerprint
        && fp.len() != segments.len()
    {
        return Err(format!(
            "partition_null_fingerprint length {} does not match partition_path segment count {}",
            fp.len(),
            segments.len()
        ));
    }
    let partition_type = spec
        .partition_type(schema.as_ref())
        .map_err(|e| format!("failed to derive iceberg partition type: {e}"))?;
    let partition_fields = partition_type.fields();
    if partition_fields.len() != spec.fields().len() {
        return Err(format!(
            "partition type has {} fields but spec expects {}",
            partition_fields.len(),
            spec.fields().len()
        ));
    }

    let mut values: Vec<Option<Literal>> = Vec::with_capacity(spec.fields().len());
    for (idx, (seg, field)) in segments.iter().zip(spec.fields().iter()).enumerate() {
        let (k, v) = seg
            .split_once('=')
            .ok_or_else(|| format!("partition_path segment `{seg}` is missing `=`"))?;
        if k != field.name {
            return Err(format!(
                "partition_path segment `{seg}` does not match partition field `{}`",
                field.name
            ));
        }
        let partition_field = partition_fields
            .get(idx)
            .ok_or_else(|| format!("partition type missing field at index {idx}"))?;
        let is_null = match null_fingerprint.and_then(|fp| fp.as_bytes().get(idx)) {
            Some(b'0') => false,
            Some(b'1') => true,
            Some(other) => {
                return Err(format!(
                    "partition_null_fingerprint contains invalid byte `{}` at index {}",
                    *other as char, idx
                ));
            }
            None => v == "__HIVE_DEFAULT_PARTITION__" || v == "null",
        };
        let lit = if is_null {
            None
        } else {
            Some(
                parse_literal_for_partition_field(v, &field.transform, &partition_field.field_type)
                    .map_err(|e| format!("partition value `{v}` parse failed: {e}"))?,
            )
        };
        values.push(lit);
    }
    Ok(Struct::from_iter(values))
}

fn parse_literal_for_partition_field(
    raw: &str,
    transform: &Transform,
    ty: &Type,
) -> Result<Literal, String> {
    match transform {
        Transform::Year => parse_year_partition_literal(raw, ty),
        Transform::Month => parse_month_partition_literal(raw, ty),
        Transform::Day => parse_literal_for_type(raw, ty),
        Transform::Hour => parse_hour_partition_literal(raw, ty),
        _ => parse_literal_for_type(raw, ty),
    }
}

fn parse_year_partition_literal(raw: &str, ty: &Type) -> Result<Literal, String> {
    let decoded = decode_partition_value(raw);
    let value = decoded.parse::<i64>().map_err(|e| e.to_string())?;
    if (-999..=999).contains(&value) {
        return partition_integer_literal(value, ty);
    }
    partition_integer_literal(value - 1970, ty)
}

fn parse_month_partition_literal(raw: &str, ty: &Type) -> Result<Literal, String> {
    let decoded = decode_partition_value(raw);
    if let Ok(date) = NaiveDate::parse_from_str(&format!("{decoded}-01"), "%Y-%m-%d") {
        let months = i64::from(date.year() - 1970) * 12 + i64::from(date.month() - 1);
        return partition_integer_literal(months, ty);
    }
    parse_literal_for_type(raw, ty)
}

fn parse_hour_partition_literal(raw: &str, ty: &Type) -> Result<Literal, String> {
    let decoded = decode_partition_value(raw);
    if let Ok(dt) = NaiveDateTime::parse_from_str(&decoded, "%Y-%m-%d-%H") {
        let hours = dt.and_utc().timestamp().div_euclid(3_600);
        return partition_integer_literal(hours, ty);
    }
    parse_literal_for_type(raw, ty)
}

fn partition_integer_literal(value: i64, ty: &Type) -> Result<Literal, String> {
    match ty {
        Type::Primitive(PrimitiveType::Int) => i32::try_from(value)
            .map(Literal::int)
            .map_err(|_| format!("partition integer {value} is out of INT range")),
        Type::Primitive(PrimitiveType::Long) => Ok(Literal::long(value)),
        other => Err(format!(
            "time transform partition type must be INT or BIGINT, got {other:?}"
        )),
    }
}

/// Reverse the percent-escaping that the IcebergSink applies to string
/// partition values when building the partition path. The sink uses the
/// Iceberg-spec subset (`%XX` for filesystem-unsafe characters); decode by
/// walking the input rather than pulling in the `urlencoding` crate.
fn decode_partition_value(raw: &str) -> String {
    String::from_utf8_lossy(&decode_partition_value_bytes(raw)).into_owned()
}

fn decode_partition_value_bytes(raw: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(raw.len());
    let bytes = raw.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'+' {
            out.push(b' ');
            i += 1;
            continue;
        }
        if bytes[i] == b'%'
            && i + 2 < bytes.len()
            && let (Some(h), Some(l)) = (hex_nibble(bytes[i + 1]), hex_nibble(bytes[i + 2]))
        {
            out.push((h << 4) | l);
            i += 3;
            continue;
        }
        out.push(bytes[i]);
        i += 1;
    }
    out
}

fn hex_nibble(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

fn parse_literal_for_type(raw: &str, ty: &Type) -> Result<Literal, String> {
    let prim = match ty {
        Type::Primitive(p) => p,
        _ => {
            return Err(format!(
                "phase 1 only supports primitive partition types, got {ty:?}"
            ));
        }
    };
    match prim {
        PrimitiveType::Int => raw
            .parse::<i32>()
            .map(Literal::int)
            .map_err(|e| e.to_string()),
        PrimitiveType::Long => raw
            .parse::<i64>()
            .map(Literal::long)
            .map_err(|e| e.to_string()),
        PrimitiveType::String => Ok(Literal::string(decode_partition_value(raw))),
        PrimitiveType::Boolean => raw
            .parse::<bool>()
            .map(Literal::bool)
            .map_err(|e| e.to_string()),
        PrimitiveType::Float => raw
            .parse::<f32>()
            .map(Literal::float)
            .map_err(|e| e.to_string()),
        PrimitiveType::Double => raw
            .parse::<f64>()
            .map(Literal::double)
            .map_err(|e| e.to_string()),
        PrimitiveType::Decimal { .. } => Literal::decimal_from_str(raw).map_err(|e| e.to_string()),
        PrimitiveType::Date => parse_date_literal(raw),
        PrimitiveType::Time => parse_time_literal(raw),
        PrimitiveType::Timestamp => parse_timestamp_literal(raw, TimestampUnit::Micros, false),
        PrimitiveType::Timestamptz => parse_timestamp_literal(raw, TimestampUnit::Micros, true),
        PrimitiveType::TimestampNs => parse_timestamp_literal(raw, TimestampUnit::Nanos, false),
        PrimitiveType::TimestamptzNs => parse_timestamp_literal(raw, TimestampUnit::Nanos, true),
        PrimitiveType::Uuid => uuid::Uuid::parse_str(&decode_partition_value(raw))
            .map(Literal::uuid)
            .map_err(|e| e.to_string()),
        PrimitiveType::Fixed(_) | PrimitiveType::Binary => {
            let encoded = decode_partition_value(raw);
            base64::engine::general_purpose::STANDARD
                .decode(encoded.as_bytes())
                .map(Literal::binary)
                .map_err(|e| e.to_string())
        }
        PrimitiveType::Variant => {
            Err("variant primitive type cannot be used in partition paths".to_string())
        }
    }
}

fn parse_date_literal(raw: &str) -> Result<Literal, String> {
    raw.parse::<i32>()
        .map(Literal::date)
        .or_else(|_| Literal::date_from_str(decode_partition_value(raw)).map_err(|e| e.to_string()))
}

fn parse_time_literal(raw: &str) -> Result<Literal, String> {
    raw.parse::<i64>()
        .map(Literal::time)
        .or_else(|_| Literal::time_from_str(decode_partition_value(raw)).map_err(|e| e.to_string()))
}

#[derive(Clone, Copy)]
enum TimestampUnit {
    Micros,
    Nanos,
}

fn parse_timestamp_literal(
    raw: &str,
    unit: TimestampUnit,
    with_timezone: bool,
) -> Result<Literal, String> {
    if let Ok(value) = raw.parse::<i64>() {
        return Ok(timestamp_literal_from_units(value, unit, with_timezone));
    }

    let value = parse_timestamp_string_to_units(&decode_partition_value(raw), unit)?;
    Ok(timestamp_literal_from_units(value, unit, with_timezone))
}

fn timestamp_literal_from_units(value: i64, unit: TimestampUnit, with_timezone: bool) -> Literal {
    match (unit, with_timezone) {
        (TimestampUnit::Micros, false) => Literal::timestamp(value),
        (TimestampUnit::Micros, true) => Literal::timestamptz(value),
        (TimestampUnit::Nanos, _) => Literal::Primitive(PrimitiveLiteral::Long(value)),
    }
}

fn parse_timestamp_string_to_units(raw: &str, unit: TimestampUnit) -> Result<i64, String> {
    if let Ok(dt) = DateTime::parse_from_rfc3339(raw) {
        return datetime_to_units(dt, unit);
    }
    for pattern in ["%Y-%m-%d %H:%M:%S%.f%:z", "%Y-%m-%dT%H:%M:%S%.f%:z"] {
        if let Ok(dt) = DateTime::parse_from_str(raw, pattern) {
            return datetime_to_units(dt, unit);
        }
    }
    for pattern in ["%Y-%m-%d %H:%M:%S%.f", "%Y-%m-%dT%H:%M:%S%.f"] {
        if let Ok(dt) = NaiveDateTime::parse_from_str(raw, pattern) {
            return match unit {
                TimestampUnit::Micros => Ok(dt.and_utc().timestamp_micros()),
                TimestampUnit::Nanos => dt
                    .and_utc()
                    .timestamp_nanos_opt()
                    .ok_or_else(|| format!("timestamp `{raw}` is out of nanosecond range")),
            };
        }
    }
    Err(format!("can't parse timestamp `{raw}`"))
}

fn datetime_to_units<Tz: chrono::TimeZone>(
    dt: DateTime<Tz>,
    unit: TimestampUnit,
) -> Result<i64, String> {
    match unit {
        TimestampUnit::Micros => Ok(dt.timestamp_micros()),
        TimestampUnit::Nanos => dt
            .timestamp_nanos_opt()
            .ok_or_else(|| "timestamp is out of nanosecond range".to_string()),
    }
}

#[cfg(test)]
mod parity_tests {
    use super::*;
    use iceberg::spec::{
        DataContentType, DataFileBuilder, DataFileFormat, Datum, NestedField, PrimitiveType,
        Schema, Struct, Transform, Type,
    };

    fn int_schema() -> SchemaRef {
        Arc::new(
            Schema::builder()
                .with_fields(vec![Arc::new(NestedField::required(
                    1,
                    "k1",
                    Type::Primitive(PrimitiveType::Int),
                ))])
                .build()
                .expect("schema"),
        )
    }

    fn int_variant_schema() -> SchemaRef {
        Arc::new(
            Schema::builder()
                .with_fields(vec![
                    Arc::new(NestedField::required(
                        1,
                        "k1",
                        Type::Primitive(PrimitiveType::Int),
                    )),
                    Arc::new(NestedField::optional(
                        2,
                        "v",
                        Type::Primitive(PrimitiveType::Variant),
                    )),
                ])
                .build()
                .expect("schema"),
        )
    }

    fn unpartitioned_collector(schema: SchemaRef) -> IcebergCommitCollector {
        IcebergCommitCollector::new(
            CommitOpKind::FastAppend,
            TableIdent::from_strs(["db", "t"]).expect("ident"),
            None,
            0,
            schema,
            Arc::new(iceberg::spec::PartitionSpec::unpartition_spec()),
            "file:///tmp/staging".to_string(),
            UniqueId { hi: 0, lo: 0 },
        )
    }

    #[test]
    fn convert_reproduces_inject_path_for_data_file_stats() {
        let mut b = DataFileBuilder::default();
        b.content(DataContentType::Data)
            .file_path("file:///t/data-1.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .partition(Struct::empty())
            .partition_spec_id(0)
            .record_count(1000)
            .file_size_in_bytes(2048);
        b.column_sizes(HashMap::from([(1, 4000u64)]));
        b.value_counts(HashMap::from([(1, 1000u64)]));
        b.null_value_counts(HashMap::from([(1, 0u64)]));
        b.lower_bounds(HashMap::from([(1, Datum::int(1))]));
        b.upper_bounds(HashMap::from([(1, Datum::int(1000))]));
        let df = b.build().expect("data file");

        let expected =
            crate::engine::iceberg_writer::data_file_to_written_file(&df, 0).expect("expected");

        let thrift = crate::connector::iceberg::data_writer::data_file_to_iceberg_thrift(
            &df,
            String::new(),
            String::new(),
            "PARQUET".to_string(),
            crate::types::TIcebergFileContent::DATA,
            Some(0),
        )
        .expect("thrift");

        let collector = unpartitioned_collector(int_schema());
        let actual = collector.convert(thrift).expect("convert");

        assert_eq!(expected, actual);
    }

    #[test]
    fn inject_sink_commit_info_converts_and_drains_written_file() {
        let mut b = DataFileBuilder::default();
        b.content(DataContentType::Data)
            .file_path("file:///t/data-from-sink.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .partition(Struct::empty())
            .partition_spec_id(0)
            .record_count(7)
            .file_size_in_bytes(128);
        let df = b.build().expect("data file");

        let thrift = crate::connector::iceberg::data_writer::data_file_to_iceberg_thrift(
            &df,
            String::new(),
            String::new(),
            "PARQUET".to_string(),
            crate::types::TIcebergFileContent::DATA,
            Some(0),
        )
        .expect("thrift");

        let collector = unpartitioned_collector(int_schema());
        collector
            .inject_sink_commit_info(crate::types::TSinkCommitInfo {
                iceberg_data_file: Some(thrift),
                ..Default::default()
            })
            .expect("inject sink commit info");

        let files = collector.take_written_files().expect("take written files");
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].path, "file:///t/data-from-sink.parquet");
        assert_eq!(files[0].record_count, 7);
        assert!(
            collector
                .take_written_files()
                .expect("second take")
                .is_empty()
        );
    }

    #[test]
    fn inject_sink_commit_info_rejects_missing_data_file() {
        let collector = unpartitioned_collector(int_schema());
        let err = collector
            .inject_sink_commit_info(crate::types::TSinkCommitInfo {
                iceberg_data_file: None,
                ..Default::default()
            })
            .expect_err("missing data file should fail");
        assert!(
            err.contains("sink_commit_info missing iceberg_data_file"),
            "got: {err}"
        );
    }

    #[test]
    fn inject_sink_commit_infos_is_all_or_nothing() {
        let mut b = DataFileBuilder::default();
        b.content(DataContentType::Data)
            .file_path("file:///t/atomic-data.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .partition(Struct::empty())
            .partition_spec_id(0)
            .record_count(7)
            .file_size_in_bytes(128);
        let df = b.build().expect("data file");
        let thrift = crate::connector::iceberg::data_writer::data_file_to_iceberg_thrift(
            &df,
            String::new(),
            String::new(),
            "PARQUET".to_string(),
            crate::types::TIcebergFileContent::DATA,
            Some(0),
        )
        .expect("thrift");

        let collector = unpartitioned_collector(int_schema());
        let err = collector
            .inject_sink_commit_infos([
                crate::types::TSinkCommitInfo {
                    iceberg_data_file: Some(thrift),
                    ..Default::default()
                },
                crate::types::TSinkCommitInfo {
                    iceberg_data_file: None,
                    ..Default::default()
                },
            ])
            .expect_err("batch with invalid sink payload must fail");
        assert!(
            err.contains("sink_commit_info missing iceberg_data_file"),
            "got: {err}"
        );
        assert!(
            collector
                .take_written_files()
                .expect("take written files")
                .is_empty(),
            "failed batch must not inject partially converted files"
        );
        assert!(
            collector.abort_log.drain_data_files().is_empty(),
            "failed batch must not partially populate abort log"
        );
    }

    #[test]
    fn convert_roundtrips_equality_delete_files() {
        let mut b = DataFileBuilder::default();
        b.content(DataContentType::EqualityDeletes)
            .file_path("file:///t/eq-del-1.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .partition(Struct::empty())
            .partition_spec_id(0)
            .record_count(3)
            .file_size_in_bytes(64)
            .equality_ids(Some(vec![1]));
        let df = b.build().expect("eq delete file");

        let expected =
            crate::engine::iceberg_writer::data_file_to_written_file(&df, 0).expect("expected");

        let thrift = crate::connector::iceberg::data_writer::data_file_to_iceberg_thrift(
            &df,
            String::new(),
            String::new(),
            "PARQUET".to_string(),
            crate::types::TIcebergFileContent::EQUALITY_DELETES,
            Some(0),
        )
        .expect("thrift");

        let collector = unpartitioned_collector(int_schema());
        let actual = collector.convert(thrift).expect("convert");

        assert_eq!(expected, actual);
        assert_eq!(actual.equality_ids, Some(vec![1]));
        assert_eq!(actual.partition_spec_id, 0);
    }

    fn identity_partition_spec(schema: &SchemaRef) -> PartitionSpecRef {
        Arc::new(
            iceberg::spec::PartitionSpec::builder(schema.clone())
                .with_spec_id(0)
                .add_partition_field("k1", "k1", Transform::Identity)
                .expect("add partition field")
                .build()
                .expect("partition spec"),
        )
    }

    #[test]
    fn convert_reproduces_identity_partition_values() {
        use iceberg::spec::Literal;

        let schema = int_schema();
        let spec = identity_partition_spec(&schema);

        let partition = Struct::from_iter([Some(Literal::int(5))]);
        let mut b = DataFileBuilder::default();
        b.content(DataContentType::Data)
            .file_path("file:///t/k1=5/data-1.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .partition(partition.clone())
            .partition_spec_id(0)
            .record_count(1)
            .file_size_in_bytes(64);
        let df = b.build().expect("data file");

        let expected =
            crate::engine::iceberg_writer::data_file_to_written_file(&df, 0).expect("expected");

        let thrift = crate::connector::iceberg::data_writer::data_file_to_iceberg_thrift(
            &df,
            "k1=5".to_string(),
            String::new(),
            "PARQUET".to_string(),
            crate::types::TIcebergFileContent::DATA,
            Some(0),
        )
        .expect("thrift");

        let collector = IcebergCommitCollector::new(
            CommitOpKind::FastAppend,
            TableIdent::from_strs(["db", "t"]).expect("ident"),
            None,
            0,
            schema,
            spec,
            "file:///tmp/staging".to_string(),
            UniqueId { hi: 0, lo: 0 },
        );
        let actual = collector.convert(thrift).expect("convert");

        assert_eq!(expected.partition_values, actual.partition_values);
        assert_eq!(expected, actual);
    }

    #[test]
    fn convert_accepts_transform_partition_paths() {
        let schema = int_schema();
        let spec = Arc::new(
            iceberg::spec::PartitionSpec::builder(schema.clone())
                .with_spec_id(0)
                .add_partition_field("k1", "k1_bucket", Transform::Bucket(4))
                .expect("add partition field")
                .build()
                .expect("partition spec"),
        );

        let mut b = DataFileBuilder::default();
        b.content(DataContentType::Data)
            .file_path("file:///t/k1_bucket=2/data-1.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .partition(Struct::from_iter([Some(iceberg::spec::Literal::int(2))]))
            .partition_spec_id(0)
            .record_count(1)
            .file_size_in_bytes(64);
        let df = b.build().expect("data file");

        let expected =
            crate::engine::iceberg_writer::data_file_to_written_file(&df, 0).expect("expected");

        let thrift = crate::connector::iceberg::data_writer::data_file_to_iceberg_thrift(
            &df,
            "k1_bucket=2".to_string(),
            String::new(),
            "PARQUET".to_string(),
            crate::types::TIcebergFileContent::DATA,
            Some(0),
        )
        .expect("thrift");

        let collector = IcebergCommitCollector::new(
            CommitOpKind::FastAppend,
            TableIdent::from_strs(["db", "t"]).expect("ident"),
            None,
            0,
            schema,
            spec,
            "file:///tmp/staging".to_string(),
            UniqueId { hi: 0, lo: 0 },
        );
        let actual = collector.convert(thrift).expect("convert");

        assert_eq!(expected.partition_values, actual.partition_values);
        assert_eq!(expected, actual);
    }

    #[test]
    fn convert_skips_bounds_for_field_ids_absent_from_schema() {
        // int_schema() has only field id 1; a bound for field id 999 simulates
        // stats left behind for a dropped column. convert() must succeed and
        // simply omit field 999 rather than erroring.
        let mut b = DataFileBuilder::default();
        b.content(DataContentType::Data)
            .file_path("file:///t/data-1.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .partition(Struct::empty())
            .partition_spec_id(0)
            .record_count(1)
            .file_size_in_bytes(10);
        b.lower_bounds(HashMap::from([(999, Datum::int(7))]));
        let df = b.build().expect("data file");

        let thrift = crate::connector::iceberg::data_writer::data_file_to_iceberg_thrift(
            &df,
            String::new(),
            String::new(),
            "PARQUET".to_string(),
            crate::types::TIcebergFileContent::DATA,
            Some(0),
        )
        .expect("thrift");

        let collector = unpartitioned_collector(int_schema());
        let actual = collector
            .convert(thrift)
            .expect("convert should skip unknown field-id, not error");
        assert!(
            actual.lower_bounds.is_empty(),
            "stale field-id bound should be skipped, got {:?}",
            actual.lower_bounds
        );
    }

    #[test]
    fn convert_skips_bounds_for_variant_field_ids() {
        let thrift = crate::types::TIcebergDataFile {
            path: Some("file:///t/data-variant.parquet".to_string()),
            format: Some("PARQUET".to_string()),
            record_count: Some(1),
            file_size_in_bytes: Some(10),
            partition_path: Some(String::new()),
            column_stats: Some(crate::types::TIcebergColumnStats {
                column_sizes: Some(BTreeMap::from([(2, 8)])),
                value_counts: Some(BTreeMap::from([(2, 1)])),
                null_value_counts: Some(BTreeMap::from([(2, 0)])),
                nan_value_counts: None,
                lower_bounds: Some(BTreeMap::from([(2, vec![1, 2, 3])])),
                upper_bounds: Some(BTreeMap::from([(2, vec![4, 5, 6])])),
            }),
            partition_null_fingerprint: Some(String::new()),
            file_content: Some(crate::types::TIcebergFileContent::DATA),
            partition_spec_id: Some(0),
            ..Default::default()
        };

        let collector = unpartitioned_collector(int_variant_schema());
        let actual = collector
            .convert(thrift)
            .expect("variant bounds should be skipped, not decoded");
        assert!(
            actual.lower_bounds.is_empty(),
            "variant lower bounds should be skipped, got {:?}",
            actual.lower_bounds
        );
        assert!(
            actual.upper_bounds.is_empty(),
            "variant upper bounds should be skipped, got {:?}",
            actual.upper_bounds
        );
        assert_eq!(actual.column_sizes.get(&2), Some(&8));
        assert_eq!(actual.value_counts.get(&2), Some(&1));
        assert_eq!(actual.null_value_counts.get(&2), Some(&0));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use iceberg::spec::{NestedField, PartitionSpec, PrimitiveType, Schema, Transform, Type};

    fn fixture_schema_and_spec() -> (SchemaRef, PartitionSpecRef) {
        let schema: SchemaRef = Arc::new(
            Schema::builder()
                .with_schema_id(0)
                .with_fields(vec![
                    NestedField::required(1, "p", Type::Primitive(PrimitiveType::Int)).into(),
                    NestedField::required(2, "v", Type::Primitive(PrimitiveType::String)).into(),
                ])
                .build()
                .expect("build schema"),
        );
        let spec = PartitionSpec::builder(schema.clone())
            .with_spec_id(0)
            .add_partition_field("p", "p", Transform::Identity)
            .expect("add partition field")
            .build()
            .expect("build partition spec");
        (schema, Arc::new(spec))
    }

    fn fixture_string_schema_and_spec() -> (SchemaRef, PartitionSpecRef) {
        let schema: SchemaRef = Arc::new(
            Schema::builder()
                .with_schema_id(0)
                .with_fields(vec![
                    NestedField::required(1, "p", Type::Primitive(PrimitiveType::Int)).into(),
                    NestedField::required(2, "v", Type::Primitive(PrimitiveType::String)).into(),
                ])
                .build()
                .expect("build schema"),
        );
        let spec = PartitionSpec::builder(schema.clone())
            .with_spec_id(0)
            .add_partition_field("v", "v", Transform::Identity)
            .expect("add partition field")
            .build()
            .expect("build partition spec");
        (schema, Arc::new(spec))
    }

    #[test]
    fn parse_empty_partition_path_returns_empty_struct() {
        let schema: SchemaRef = Arc::new(
            Schema::builder()
                .with_schema_id(0)
                .with_fields(vec![
                    NestedField::required(1, "p", Type::Primitive(PrimitiveType::Int)).into(),
                ])
                .build()
                .expect("build schema"),
        );
        let spec = Arc::new(
            PartitionSpec::builder(schema.clone())
                .with_spec_id(0)
                .build()
                .expect("build partition spec"),
        );
        let s = parse_partition_path("", &spec, &schema, None).expect("parse empty path");
        assert_eq!(s.fields().len(), 0);
    }

    #[test]
    fn parse_empty_partition_path_rejects_partitioned_spec() {
        let (schema, spec) = fixture_schema_and_spec();
        let err = parse_partition_path("", &spec, &schema, None)
            .expect_err("partitioned empty path must fail");
        assert!(err.contains("partition_path is empty but spec expects 1 fields"));
    }

    #[test]
    fn parse_one_segment_identity_int() {
        let (schema, spec) = fixture_schema_and_spec();
        let s = parse_partition_path("p=42", &spec, &schema, None).expect("parse identity int");
        assert_eq!(s.fields().len(), 1);
        match &s.fields()[0] {
            Some(Literal::Primitive(_)) => {}
            other => panic!("expected primitive literal, got {other:?}"),
        }
    }

    #[test]
    fn parse_partition_path_uses_null_fingerprint() {
        let (schema, spec) = fixture_string_schema_and_spec();

        let literal = parse_partition_path("v=null", &spec, &schema, Some("0"))
            .expect("parse non-null literal");
        match &literal.fields()[0] {
            Some(Literal::Primitive(iceberg::spec::PrimitiveLiteral::String(value))) => {
                assert_eq!(value, "null");
            }
            other => panic!("expected string literal, got {other:?}"),
        }

        let null =
            parse_partition_path("v=null", &spec, &schema, Some("1")).expect("parse null literal");
        assert!(null.fields()[0].is_none());
    }

    #[test]
    fn rejects_segment_count_mismatch() {
        let (schema, spec) = fixture_schema_and_spec();
        let r = parse_partition_path("p=1/q=2", &spec, &schema, None);
        assert!(r.is_err());
    }

    #[test]
    fn rejects_segment_without_equals_sign() {
        let (schema, spec) = fixture_schema_and_spec();
        let r = parse_partition_path("p1", &spec, &schema, None);
        assert!(r.is_err());
    }

    #[test]
    fn parse_month_transform_partition_path_to_offset() {
        let schema: SchemaRef = Arc::new(
            Schema::builder()
                .with_schema_id(0)
                .with_fields(vec![
                    NestedField::required(1, "ts", Type::Primitive(PrimitiveType::Timestamp))
                        .into(),
                ])
                .build()
                .expect("build schema"),
        );
        let spec = Arc::new(
            PartitionSpec::builder(schema.clone())
                .with_spec_id(0)
                .add_partition_field("ts", "ts_month", Transform::Month)
                .expect("add partition field")
                .build()
                .expect("build partition spec"),
        );

        let s = parse_partition_path("ts_month=2024-01", &spec, &schema, None)
            .expect("parse month partition");

        assert_eq!(s.fields().len(), 1);
        match &s.fields()[0] {
            Some(Literal::Primitive(PrimitiveLiteral::Int(value))) => {
                assert_eq!(*value, (2024 - 1970) * 12);
            }
            other => panic!("expected int month offset, got {other:?}"),
        }
    }

    #[test]
    fn parse_day_transform_partition_path_to_date() {
        let schema: SchemaRef = Arc::new(
            Schema::builder()
                .with_schema_id(0)
                .with_fields(vec![
                    NestedField::required(1, "ts", Type::Primitive(PrimitiveType::Timestamp))
                        .into(),
                ])
                .build()
                .expect("build schema"),
        );
        let spec = Arc::new(
            PartitionSpec::builder(schema.clone())
                .with_spec_id(0)
                .add_partition_field("ts", "ts_day", Transform::Day)
                .expect("add partition field")
                .build()
                .expect("build partition spec"),
        );

        let s = parse_partition_path("ts_day=2024-01-15", &spec, &schema, None)
            .expect("parse day partition");

        assert_eq!(s.fields().len(), 1);
        match &s.fields()[0] {
            Some(Literal::Primitive(PrimitiveLiteral::Int(value))) => {
                let date = NaiveDate::from_ymd_opt(2024, 1, 15).expect("date");
                assert_eq!(*value, date.num_days_from_ce() - 719_163);
            }
            other => panic!("expected date literal, got {other:?}"),
        }
    }

    #[test]
    fn collector_round_trips_injected_delete_groups() {
        let (schema, spec) = fixture_schema_and_spec();
        let collector = IcebergCommitCollector::new(
            CommitOpKind::RowDeltaDv,
            iceberg::TableIdent::new(
                iceberg::NamespaceIdent::new("db".to_string()),
                "t".to_string(),
            ),
            None,
            0,
            schema,
            spec,
            "file:///tmp/staging".to_string(),
            crate::common::types::UniqueId { hi: 0, lo: 0 },
        );
        collector.inject_delete_group(PositionDeleteGroup {
            referenced_data_file: "file:///tmp/data.parquet".to_string(),
            partition_spec_id: 0,
            partition_values: iceberg::spec::Struct::empty(),
            positions: vec![1, 3, 5],
        });
        let groups = collector.take_delete_groups();
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].referenced_data_file, "file:///tmp/data.parquet");
        assert_eq!(groups[0].positions, vec![1, 3, 5]);
        // Subsequent take must return an empty vec.
        assert!(collector.take_delete_groups().is_empty());
    }
}
