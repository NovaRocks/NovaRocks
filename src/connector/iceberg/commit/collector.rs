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

use iceberg::TableIdent;
use iceberg::spec::{Literal, PartitionSpecRef, PrimitiveType, SchemaRef, Struct, Transform, Type};

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
        self.abort_log.record_data_file(wf.path.clone());
        self.injected
            .lock()
            .expect("collector injected lock poisoned")
            .push(wf);
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
            crate::types::TIcebergFileContent::EQUALITY_DELETES => {
                return Err(
                    "IcebergSink commit info cannot carry equality_ids for equality-delete files"
                        .to_string(),
                );
            }
            other => {
                return Err(format!(
                    "unexpected TIcebergFileContent variant {other:?} in sink_commit_info"
                ));
            }
        };

        let partition_values = parse_partition_path(
            df.partition_path.as_deref().unwrap_or(""),
            &self.partition_spec,
            &self.schema,
        )?;

        Ok(WrittenFile {
            path,
            format: DataFileFormat::Parquet,
            content,
            partition_values,
            partition_spec_id: self.partition_spec.spec_id(),
            record_count: df.record_count.unwrap_or(0).max(0) as u64,
            file_size_in_bytes: df.file_size_in_bytes.unwrap_or(0).max(0) as u64,
            split_offsets: df.split_offsets.unwrap_or_default(),
            column_sizes: Default::default(),
            value_counts: Default::default(),
            null_value_counts: Default::default(),
            key_metadata: None,
            referenced_data_file: df.referenced_data_file,
            equality_ids: None,
            first_row_id: None,
        })
    }

    pub fn mark_committed(&self) {
        self.committed.store(true, Ordering::SeqCst);
    }

    pub fn is_committed(&self) -> bool {
        self.committed.load(Ordering::SeqCst)
    }
}

/// Decode an Iceberg v2-style partition path (e.g. `p=1/q=A`) into a
/// [`Struct`] keyed by the partition spec's field order.
///
/// Phase 1 only handles identity-transformed partitions on primitive source
/// columns. Anything else is rejected with an explicit error so that the
/// caller can fall back gracefully.
fn parse_partition_path(
    path: &str,
    spec: &PartitionSpecRef,
    schema: &SchemaRef,
) -> Result<Struct, String> {
    if path.is_empty() {
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

    let mut values: Vec<Option<Literal>> = Vec::with_capacity(spec.fields().len());
    for (seg, field) in segments.iter().zip(spec.fields().iter()) {
        let (_k, v) = seg
            .split_once('=')
            .ok_or_else(|| format!("partition_path segment `{seg}` is missing `=`"))?;
        if !matches!(field.transform, Transform::Identity) {
            return Err(format!(
                "phase 1 partition transform `{:?}` not yet supported during \
                 partition_path → Struct decoding",
                field.transform
            ));
        }
        let source_field = schema.field_by_id(field.source_id).ok_or_else(|| {
            format!(
                "partition source field id {} not present in schema",
                field.source_id
            )
        })?;
        let lit = parse_literal_for_type(v, &source_field.field_type)
            .map_err(|e| format!("partition value `{v}` parse failed: {e}"))?;
        values.push(Some(lit));
    }
    Ok(Struct::from_iter(values))
}

/// Reverse the percent-escaping that the IcebergSink applies to string
/// partition values when building the partition path. The sink uses the
/// Iceberg-spec subset (`%XX` for filesystem-unsafe characters); decode by
/// walking the input rather than pulling in the `urlencoding` crate.
fn decode_partition_value(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    let bytes = raw.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%'
            && i + 2 < bytes.len()
            && let (Some(h), Some(l)) = (hex_nibble(bytes[i + 1]), hex_nibble(bytes[i + 2]))
        {
            out.push(((h << 4) | l) as char);
            i += 3;
            continue;
        }
        out.push(bytes[i] as char);
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
    if raw == "__HIVE_DEFAULT_PARTITION__" || raw == "null" {
        return Err("phase 1 does not support null partition values".to_string());
    }
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
        other => Err(format!(
            "phase 1 partition primitive type {other:?} not yet supported"
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use iceberg::spec::{NestedField, PartitionSpec, PrimitiveType, Schema, Type};

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

    #[test]
    fn parse_empty_partition_path_returns_empty_struct() {
        let (schema, spec) = fixture_schema_and_spec();
        let s = parse_partition_path("", &spec, &schema).expect("parse empty path");
        assert_eq!(s.fields().len(), 0);
    }

    #[test]
    fn parse_one_segment_identity_int() {
        let (schema, spec) = fixture_schema_and_spec();
        let s = parse_partition_path("p=42", &spec, &schema).expect("parse identity int");
        assert_eq!(s.fields().len(), 1);
        match &s.fields()[0] {
            Some(Literal::Primitive(_)) => {}
            other => panic!("expected primitive literal, got {other:?}"),
        }
    }

    #[test]
    fn rejects_segment_count_mismatch() {
        let (schema, spec) = fixture_schema_and_spec();
        let r = parse_partition_path("p=1/q=2", &spec, &schema);
        assert!(r.is_err());
    }

    #[test]
    fn rejects_segment_without_equals_sign() {
        let (schema, spec) = fixture_schema_and_spec();
        let r = parse_partition_path("p1", &spec, &schema);
        assert!(r.is_err());
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
