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

use std::collections::HashMap;

use iceberg::spec::{DataContentType, DataFileFormat, Datum, Struct};

/// Selects which Iceberg commit action to run for a given write transaction.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CommitOpKind {
    FastAppend,
    Overwrite,
    RowDelta,
    /// Iceberg v3 row-lineage DELETE: writes Puffin deletion-vector files and
    /// rewrites touched delete manifests instead of producing v2 Parquet
    /// position-delete files.
    RowDeltaDv,
    /// Iceberg v3 row-lineage DELETE where BE writers already produced merged
    /// Puffin deletion-vector files; the FE commit registers metadata only.
    RowDeltaDvFromFiles,
    /// Iceberg OPTIMIZE whole-table rewrite: replaces all current live data
    /// files with compacted data files and drops all current delete files.
    RewriteDataFiles,
    /// Iceberg v3 row-lineage UPDATE in copy-on-write mode: rewrites touched
    /// data files while preserving `_row_id`.
    CowUpdate,
    /// Iceberg `TRUNCATE TABLE`: writes a single `operation=delete` snapshot
    /// that marks every live data / DV / position-delete / equality-delete
    /// file as DELETED while preserving schema, partition spec, properties,
    /// and other refs.
    Truncate,
    /// Iceberg `INSERT OVERWRITE PARTITIONS`: writes a single
    /// `operation=overwrite` snapshot that marks live files in only the
    /// partitions touched by the new data as DELETED (cross historical
    /// partition specs) and adds the new files. Other partitions are
    /// preserved untouched. v3 row-lineage tables only.
    OverwritePartitions,
    /// Iceberg `ALTER TABLE x REWRITE MANIFESTS`: groups manifests by
    /// (partition_spec_id, content_type) and merges each group into a
    /// single manifest, emitting an `operation=replace` snapshot. No data
    /// files are rewritten; sequence_number is preserved.
    RewriteManifests,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IcebergWriteMode {
    LegacyPositionDeletes,
    RowLineageV3,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IcebergSqlDeleteStrategy {
    PositionDeleteFiles,
    DeletionVectors,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IcebergUpdateMode {
    CopyOnWrite,
    MergeOnRead,
}

impl IcebergUpdateMode {
    pub fn as_property_value(self) -> &'static str {
        match self {
            IcebergUpdateMode::CopyOnWrite => NOVAROCKS_UPDATE_MODE_COW,
            IcebergUpdateMode::MergeOnRead => NOVAROCKS_UPDATE_MODE_MOR,
        }
    }

    pub fn from_property_value(value: &str) -> Option<Self> {
        match value {
            NOVAROCKS_UPDATE_MODE_COW => Some(IcebergUpdateMode::CopyOnWrite),
            NOVAROCKS_UPDATE_MODE_MOR => Some(IcebergUpdateMode::MergeOnRead),
            _ => None,
        }
    }
}

pub const NOVAROCKS_UPDATE_MODE: &str = "novarocks.update.mode";
pub const NOVAROCKS_UPDATE_MODE_COW: &str = "copy-on-write";
pub const NOVAROCKS_UPDATE_MODE_MOR: &str = "merge-on-read";

/// Metadata about a single Parquet file produced by `IcebergSink` during a
/// pipeline run. Mirrors the subset of `TIcebergDataFile` we need for commit
/// and abort flows. Constructed from `TSinkCommitInfo` after pipeline finish.
#[derive(Clone, Debug, PartialEq)]
pub struct WrittenFile {
    pub path: String,
    pub format: DataFileFormat,
    pub content: DataContentType,
    pub partition_values: Struct,
    pub partition_spec_id: i32,
    pub record_count: u64,
    pub file_size_in_bytes: u64,
    pub split_offsets: Vec<i64>,
    pub column_sizes: HashMap<i32, u64>,
    pub value_counts: HashMap<i32, u64>,
    pub null_value_counts: HashMap<i32, u64>,
    /// Per-column lower bounds (Iceberg spec §3.4), keyed by field id. Carried
    /// from the writer's `DataFile` so the committed manifest entry preserves
    /// min bounds for range-predicate selectivity. Empty when the writer did
    /// not produce bounds (e.g. delete files).
    pub lower_bounds: HashMap<i32, Datum>,
    /// Per-column upper bounds (Iceberg spec §3.4), keyed by field id. See
    /// [`Self::lower_bounds`].
    pub upper_bounds: HashMap<i32, Datum>,
    pub key_metadata: Option<Vec<u8>>,
    /// Set only for content == PositionDeletes.
    pub referenced_data_file: Option<String>,
    /// Set only for content == EqualityDeletes.
    pub equality_ids: Option<Vec<i32>>,
    /// For Iceberg v3 row-lineage data files whose rows reuse pre-existing
    /// `_row_id`s (e.g. MOR UPDATE replacement files): the lineage `first_row_id`
    /// to record on the manifest entry. When set, readers prefer this over the
    /// manifest-level first_row_id and `df.first_row_id()` propagates without
    /// triggering fresh row-id allocation. `None` for normal INSERT writes.
    pub first_row_id: Option<i64>,
    /// Puffin DV blob offset (set only for Puffin DV files).
    pub content_offset: Option<i64>,
    /// Puffin DV blob size in bytes (set only for Puffin DV files).
    pub content_size_in_bytes: Option<i64>,
    /// Puffin DV cardinality (set only for Puffin DV files).
    pub cardinality: Option<u64>,
}

/// Result returned by a successful commit action.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CommitOutcome {
    pub new_snapshot_id: i64,
    /// Manifest / manifest-list paths written by the commit-action; consumed by
    /// abort cleanup on failure.
    pub written_manifest_paths: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn written_file_can_be_constructed() {
        let f = WrittenFile {
            path: "s3://x/data/abc.parquet".to_string(),
            format: DataFileFormat::Parquet,
            content: DataContentType::Data,
            partition_values: Struct::empty(),
            partition_spec_id: 0,
            record_count: 100,
            file_size_in_bytes: 4096,
            split_offsets: vec![4],
            column_sizes: Default::default(),
            value_counts: Default::default(),
            null_value_counts: Default::default(),
            lower_bounds: Default::default(),
            upper_bounds: Default::default(),
            key_metadata: None,
            referenced_data_file: None,
            equality_ids: None,
            first_row_id: None,
            content_offset: None,
            content_size_in_bytes: None,
            cardinality: None,
        };
        assert_eq!(f.record_count, 100);
        assert_eq!(f.content, DataContentType::Data);
    }

    #[test]
    fn op_kind_variants_are_distinct() {
        let variants = [
            CommitOpKind::FastAppend,
            CommitOpKind::Overwrite,
            CommitOpKind::RowDelta,
            CommitOpKind::RowDeltaDv,
            CommitOpKind::RowDeltaDvFromFiles,
            CommitOpKind::RewriteDataFiles,
            CommitOpKind::CowUpdate,
            CommitOpKind::Truncate,
            CommitOpKind::OverwritePartitions,
            CommitOpKind::RewriteManifests,
        ];
        for (idx, left) in variants.iter().enumerate() {
            for right in variants.iter().skip(idx + 1) {
                assert_ne!(left, right);
            }
        }
    }

    #[test]
    fn update_mode_property_values_round_trip() {
        assert_eq!(NOVAROCKS_UPDATE_MODE, "novarocks.update.mode");
        assert_eq!(
            IcebergUpdateMode::from_property_value(
                IcebergUpdateMode::MergeOnRead.as_property_value()
            ),
            Some(IcebergUpdateMode::MergeOnRead)
        );
        assert_eq!(
            IcebergUpdateMode::from_property_value(
                IcebergUpdateMode::CopyOnWrite.as_property_value()
            ),
            Some(IcebergUpdateMode::CopyOnWrite)
        );
    }
}
