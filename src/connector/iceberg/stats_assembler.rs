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

//! Iceberg Puffin statistics assembler.
//!
//! Given the per-file Theta sketches computed by the sink and the table's
//! prior Puffin statistics (if any), produce the snapshot-level
//! `StatisticsFile` to be registered with the new commit.
//!
//! The assembler implements the commit-type behavior matrix described in the
//! Puffin NDV design spec (section 5.3):
//!
//! | CommitType | Behavior                                       |
//! |------------|-----------------------------------------------|
//! | Append     | union(previous aggregate, new file sketches)   |
//! | Delete     | reuse previous Puffin (returns `None`)         |
//! | Rewrite    | reuse previous Puffin (returns `None`)         |
//! | Overwrite  | full rescan (deferred — returns `None` today)  |
//!
//! "First commit" with no prior Puffin currently follows the Overwrite path
//! and therefore returns `None`. The full rescan logic is left to a follow-up
//! agent that wires file I/O into the read-back path.
//!
//! NOTE: This module's public surface is not yet called from any commit
//! action — Phase 2.3 of the implementation plan (commit hook integration)
//! is owned by the next agent. The `#[allow(dead_code)]` attributes below
//! keep the unused-warning suppressed until then.

#![allow(dead_code)]

use std::collections::HashMap;

use iceberg::io::FileIO;
use iceberg::puffin::{APACHE_DATASKETCHES_THETA_V1, Blob, PuffinReader, PuffinWriter};
use iceberg::spec::{BlobMetadata, StatisticsFile, TableMetadata};
use iceberg::table::Table;

use super::theta_sketch::ThetaSketchHandle;

/// The kind of commit being performed. Determines how the assembler combines
/// new file sketches with the previous snapshot's aggregate Puffin.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommitType {
    /// Append-only commit (e.g. INSERT, fast_append). Aggregate is
    /// `previous_aggregate ∪ union(new_file_sketches)`.
    Append,
    /// Delete-only commit (position-delete, equality-delete). NDV is an upper
    /// bound and remains valid; the assembler returns `None` so the caller
    /// reuses the previous Puffin entry.
    Delete,
    /// INSERT OVERWRITE / REPLACE. Requires a full rescan; deferred for now.
    Overwrite,
    /// Compaction or other rewrite-data-files action that does not change
    /// logical row content. Reuse the previous Puffin.
    Rewrite,
}

/// Per-file Theta sketches produced by the sink, one entry per primitive
/// column keyed by Iceberg field id.
pub struct FileSketchSet {
    pub file_path: String,
    pub sketches: HashMap<i32, ThetaSketchHandle>,
}

/// Orchestrates Puffin statistics assembly during a snapshot commit.
pub struct StatsAssembler;

impl StatsAssembler {
    /// Assemble the Puffin statistics file for the current commit.
    ///
    /// Returns `Some(StatisticsFile)` when a fresh Puffin was written and
    /// should be registered with the metadata. Returns `None` when the caller
    /// should either skip the registration (no prior Puffin) or carry forward
    /// the previous snapshot's Puffin entry unchanged.
    ///
    /// `current_snapshot_id` / `current_sequence_number` describe the snapshot
    /// being committed. `prev_snapshot_id`, when `Some`, identifies the
    /// snapshot whose Puffin we read for incremental APPEND merging.
    pub async fn assemble(
        table: &Table,
        commit_type: CommitType,
        new_file_sketches: Vec<FileSketchSet>,
        current_snapshot_id: i64,
        current_sequence_number: i64,
        prev_snapshot_id: Option<i64>,
        file_io: &FileIO,
    ) -> Result<Option<StatisticsFile>, String> {
        match commit_type {
            CommitType::Delete | CommitType::Rewrite => {
                // Reuse the previous Puffin entry — NDV remains a valid upper
                // bound. The caller is responsible for re-registering the
                // previous StatisticsFile against the new snapshot id if
                // desired.
                Ok(None)
            }
            CommitType::Append => {
                Self::assemble_append(
                    table,
                    new_file_sketches,
                    current_snapshot_id,
                    current_sequence_number,
                    prev_snapshot_id,
                    file_io,
                )
                .await
            }
            CommitType::Overwrite => {
                // Full rescan path is deferred (requires reading every live
                // data file and re-computing Theta sketches). Returning None
                // means "no new Puffin this commit"; the optimizer will fall
                // back to manifest-derived heuristics until a follow-up agent
                // wires the rescan path.
                Self::assemble_overwrite(
                    table,
                    new_file_sketches,
                    current_snapshot_id,
                    current_sequence_number,
                    file_io,
                )
                .await
            }
        }
    }

    /// APPEND path: union the previous snapshot's aggregate sketch with the
    /// new per-file sketches and write a new Puffin file.
    async fn assemble_append(
        table: &Table,
        new_file_sketches: Vec<FileSketchSet>,
        current_snapshot_id: i64,
        current_sequence_number: i64,
        prev_snapshot_id: Option<i64>,
        file_io: &FileIO,
    ) -> Result<Option<StatisticsFile>, String> {
        // 1. Aggregate the new file sketches per field id.
        let per_column = aggregate_per_column(new_file_sketches);
        if per_column.is_empty() {
            // Nothing to write — caller can either keep the previous entry or
            // skip statistics for this snapshot.
            return Ok(None);
        }

        // 2. If a previous Puffin exists, union its sketches into the running
        //    aggregate.
        let merged = if let Some(prev_id) = prev_snapshot_id {
            let previous_sketches =
                read_previous_sketches(table.metadata(), prev_id, file_io).await?;
            merge_with_previous(per_column, previous_sketches)
        } else {
            per_column
        };

        // 3. Serialize and write the Puffin file.
        let puffin_path = puffin_path_for_snapshot(table.metadata(), current_snapshot_id);
        write_puffin(
            file_io,
            &puffin_path,
            current_snapshot_id,
            current_sequence_number,
            &merged,
        )
        .await
    }

    /// OVERWRITE / first-commit path: needs a full rescan over the current
    /// snapshot's data files. Deferred — returns `None` so the caller skips
    /// statistics registration for this commit.
    async fn assemble_overwrite(
        _table: &Table,
        _new_file_sketches: Vec<FileSketchSet>,
        _current_snapshot_id: i64,
        _current_sequence_number: i64,
        _file_io: &FileIO,
    ) -> Result<Option<StatisticsFile>, String> {
        // Full rescan implementation is the responsibility of the follow-up
        // agent that wires file I/O into the commit path. See
        // docs/superpowers/plans/2026-05-20-iceberg-puffin-ndv-stats.md
        // Step 2.2 "OVERWRITE logic".
        Ok(None)
    }
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

/// Combine many per-file sketch sets into a per-column aggregate by taking the
/// union over each field id's individual sketches.
fn aggregate_per_column(new_file_sketches: Vec<FileSketchSet>) -> HashMap<i32, ThetaSketchHandle> {
    let mut by_field: HashMap<i32, Vec<ThetaSketchHandle>> = HashMap::new();
    for set in new_file_sketches {
        for (field_id, sketch) in set.sketches {
            by_field.entry(field_id).or_default().push(sketch);
        }
    }

    let mut out = HashMap::new();
    for (field_id, sketches) in by_field {
        let refs: Vec<&ThetaSketchHandle> = sketches.iter().collect();
        out.insert(field_id, ThetaSketchHandle::union(&refs));
    }
    out
}

/// Merge the previous snapshot's aggregate sketches into the per-column
/// aggregate from this commit. Columns only present in the previous Puffin
/// are kept; columns only present in the new commit are kept; overlapping
/// columns are unioned.
fn merge_with_previous(
    mut new_per_column: HashMap<i32, ThetaSketchHandle>,
    previous: HashMap<i32, ThetaSketchHandle>,
) -> HashMap<i32, ThetaSketchHandle> {
    for (field_id, prev_sketch) in previous {
        match new_per_column.remove(&field_id) {
            Some(new_sketch) => {
                let merged = ThetaSketchHandle::union(&[&new_sketch, &prev_sketch]);
                new_per_column.insert(field_id, merged);
            }
            None => {
                new_per_column.insert(field_id, prev_sketch);
            }
        }
    }
    new_per_column
}

/// Read the previous snapshot's Puffin and decode each Theta blob.
/// Returns an empty map if the snapshot has no statistics entry.
async fn read_previous_sketches(
    table_metadata: &TableMetadata,
    prev_snapshot_id: i64,
    file_io: &FileIO,
) -> Result<HashMap<i32, ThetaSketchHandle>, String> {
    let Some(prev_stats) = table_metadata.statistics_for_snapshot(prev_snapshot_id) else {
        return Ok(HashMap::new());
    };

    let input_file = file_io
        .new_input(&prev_stats.statistics_path)
        .map_err(|e| format!("open previous puffin {}: {e}", prev_stats.statistics_path))?;
    let reader = PuffinReader::new(input_file);
    let file_metadata = reader
        .file_metadata()
        .await
        .map_err(|e| format!("read previous puffin metadata: {e}"))?;

    let mut sketches = HashMap::new();
    for blob_metadata in file_metadata.blobs() {
        if blob_metadata.blob_type() != APACHE_DATASKETCHES_THETA_V1 {
            continue;
        }
        let blob = reader
            .blob(blob_metadata)
            .await
            .map_err(|e| format!("read previous puffin blob: {e}"))?;
        let Some(&field_id) = blob.fields().first() else {
            // Skip blobs without a field id — the optimizer cannot key off
            // an empty column descriptor.
            continue;
        };
        match ThetaSketchHandle::deserialize(blob.data()) {
            Ok(sketch) => {
                sketches.insert(field_id, sketch);
            }
            Err(err) => {
                // Surface deserialization failures as errors rather than
                // silently dropping — the caller can choose to swallow and
                // fall back to a from-scratch rebuild.
                return Err(format!(
                    "decode previous theta sketch for field {field_id}: {err}"
                ));
            }
        }
    }
    Ok(sketches)
}

/// Write a new Puffin file holding one Theta blob per primitive column.
async fn write_puffin(
    file_io: &FileIO,
    puffin_path: &str,
    snapshot_id: i64,
    sequence_number: i64,
    sketches: &HashMap<i32, ThetaSketchHandle>,
) -> Result<Option<StatisticsFile>, String> {
    let output_file = file_io
        .new_output(puffin_path)
        .map_err(|e| format!("open output puffin {puffin_path}: {e}"))?;
    let mut writer = PuffinWriter::new(&output_file, HashMap::new(), false)
        .await
        .map_err(|e| format!("create puffin writer: {e}"))?;

    // Sort by field id so the resulting blob ordering is deterministic across
    // re-commits with the same input.
    let mut sorted_fields: Vec<i32> = sketches.keys().copied().collect();
    sorted_fields.sort_unstable();

    let mut blob_metadata: Vec<BlobMetadata> = Vec::with_capacity(sorted_fields.len());
    for field_id in sorted_fields {
        let sketch = sketches
            .get(&field_id)
            .expect("sketch present for sorted field id");
        let data = sketch.serialize();
        let blob = Blob::builder()
            .r#type(APACHE_DATASKETCHES_THETA_V1.to_string())
            .fields(vec![field_id])
            .snapshot_id(snapshot_id)
            .sequence_number(sequence_number)
            .data(data)
            .properties(HashMap::new())
            .build();
        writer
            .add(blob, iceberg::puffin::CompressionCodec::None)
            .await
            .map_err(|e| format!("write puffin blob field={field_id}: {e}"))?;

        blob_metadata.push(BlobMetadata {
            r#type: APACHE_DATASKETCHES_THETA_V1.to_string(),
            snapshot_id,
            sequence_number,
            fields: vec![field_id],
            properties: HashMap::new(),
        });
    }
    writer
        .close()
        .await
        .map_err(|e| format!("close puffin writer: {e}"))?;

    // Determine total file size and the footer size by reading back the
    // payload-length prefix from the trailing footer struct. This is the
    // canonical Iceberg approach — Puffin's writer does not expose footer
    // size directly, but the file layout makes it cheap to recover.
    let input_file = file_io
        .new_input(puffin_path)
        .map_err(|e| format!("open puffin for sizing {puffin_path}: {e}"))?;
    let file_size = input_file
        .metadata()
        .await
        .map_err(|e| format!("stat puffin {puffin_path}: {e}"))?
        .size;
    let file_footer_size = read_footer_size(&input_file, file_size).await?;

    Ok(Some(StatisticsFile {
        snapshot_id,
        statistics_path: puffin_path.to_string(),
        file_size_in_bytes: file_size as i64,
        file_footer_size_in_bytes: file_footer_size as i64,
        key_metadata: None,
        blob_metadata,
    }))
}

/// Read the footer struct trailer and compute the total footer size.
///
/// Puffin footer layout (from `vendor/iceberg-0.9.0/src/puffin/metadata.rs`):
///   `MAGIC(4) + footer_payload + payload_length(4) + flags(4) + MAGIC(4)`
/// where `payload_length` is the little-endian u32 stored at
/// `file_size - FOOTER_STRUCT_LENGTH = file_size - 12`.
async fn read_footer_size(
    input_file: &iceberg::io::InputFile,
    file_size: u64,
) -> Result<u64, String> {
    const FOOTER_STRUCT_LENGTH: u64 = 12; // payload_length(4) + flags(4) + magic(4)
    const MAGIC_LENGTH: u64 = 4;

    if file_size < FOOTER_STRUCT_LENGTH + MAGIC_LENGTH {
        return Err(format!(
            "puffin file too small to contain footer: {file_size} bytes"
        ));
    }

    let reader = input_file
        .reader()
        .await
        .map_err(|e| format!("open puffin reader: {e}"))?;
    let start = file_size - FOOTER_STRUCT_LENGTH;
    let end = start + 4;
    let bytes = reader
        .read(start..end)
        .await
        .map_err(|e| format!("read footer payload length: {e}"))?;
    let mut buf = [0u8; 4];
    buf.copy_from_slice(&bytes);
    let payload_length = u32::from_le_bytes(buf) as u64;
    Ok(MAGIC_LENGTH + payload_length + FOOTER_STRUCT_LENGTH)
}

/// Canonical Puffin path for a given snapshot id, relative to the table
/// location's metadata folder. Spark/Trino/Iceberg reference engines use the
/// same `snap-<id>-<seq>-<uuid>.stats` pattern, but for NovaRocks we keep
/// things deterministic with a fixed suffix that is recoverable from the
/// snapshot id alone.
fn puffin_path_for_snapshot(table_metadata: &TableMetadata, snapshot_id: i64) -> String {
    let location = table_metadata.location().trim_end_matches('/');
    format!("{location}/metadata/snap-{snapshot_id}-statistics.puffin")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_sketch_with_values(start: i64, count: i64) -> ThetaSketchHandle {
        let mut sketch = ThetaSketchHandle::new(12);
        for v in start..(start + count) {
            sketch.update(v);
        }
        sketch
    }

    #[test]
    fn aggregate_per_column_unions_same_field() {
        let mut a = HashMap::new();
        a.insert(1, make_sketch_with_values(0, 1000));
        let mut b = HashMap::new();
        b.insert(1, make_sketch_with_values(500, 1000));
        let aggregate = aggregate_per_column(vec![
            FileSketchSet {
                file_path: "a.parquet".to_string(),
                sketches: a,
            },
            FileSketchSet {
                file_path: "b.parquet".to_string(),
                sketches: b,
            },
        ]);
        let est = aggregate.get(&1).expect("field 1 present").estimate();
        // Union of [0,999] and [500,1499] = [0,1499] ≈ 1500 distinct.
        assert!(
            (1300.0..1700.0).contains(&est),
            "aggregate estimate {est} out of expected range"
        );
    }

    #[test]
    fn aggregate_per_column_keeps_distinct_fields() {
        let mut a = HashMap::new();
        a.insert(1, make_sketch_with_values(0, 100));
        a.insert(2, make_sketch_with_values(0, 200));
        let aggregate = aggregate_per_column(vec![FileSketchSet {
            file_path: "a.parquet".to_string(),
            sketches: a,
        }]);
        assert_eq!(aggregate.len(), 2);
        assert!(aggregate.contains_key(&1));
        assert!(aggregate.contains_key(&2));
    }

    #[test]
    fn merge_with_previous_unions_overlapping_fields() {
        let mut new_map = HashMap::new();
        new_map.insert(1, make_sketch_with_values(0, 5000));
        let mut prev_map = HashMap::new();
        prev_map.insert(1, make_sketch_with_values(3000, 5000));
        let merged = merge_with_previous(new_map, prev_map);
        let est = merged.get(&1).expect("field 1 present").estimate();
        // Union of [0,4999] and [3000,7999] = [0,7999] ≈ 8000 distinct.
        assert!(
            (7000.0..9500.0).contains(&est),
            "merged estimate {est} out of expected range"
        );
    }

    #[test]
    fn merge_with_previous_keeps_unique_previous_fields() {
        let mut new_map = HashMap::new();
        new_map.insert(1, make_sketch_with_values(0, 100));
        let mut prev_map = HashMap::new();
        prev_map.insert(2, make_sketch_with_values(0, 200));
        let merged = merge_with_previous(new_map, prev_map);
        assert_eq!(merged.len(), 2);
        assert!(merged.contains_key(&1));
        assert!(merged.contains_key(&2));
    }
}
