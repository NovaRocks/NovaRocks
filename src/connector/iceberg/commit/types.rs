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

use iceberg::spec::{DataContentType, DataFileFormat, Struct};

/// Selects which Iceberg commit action to run for a given write transaction.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CommitOpKind {
    FastAppend,
    Overwrite,
    RowDelta,
}

/// Metadata about a single Parquet file produced by `IcebergSink` during a
/// pipeline run. Mirrors the subset of `TIcebergDataFile` we need for commit
/// and abort flows. Constructed from `TSinkCommitInfo` after pipeline finish.
#[derive(Clone, Debug)]
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
    pub key_metadata: Option<Vec<u8>>,
    /// Set only for content == PositionDeletes.
    pub referenced_data_file: Option<String>,
}

/// Result returned by a successful commit action.
#[derive(Clone, Debug)]
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
            key_metadata: None,
            referenced_data_file: None,
        };
        assert_eq!(f.record_count, 100);
        assert_eq!(f.content, DataContentType::Data);
    }

    #[test]
    fn op_kind_variants_are_distinct() {
        assert_ne!(CommitOpKind::FastAppend, CommitOpKind::Overwrite);
        assert_ne!(CommitOpKind::Overwrite, CommitOpKind::RowDelta);
        assert_ne!(CommitOpKind::FastAppend, CommitOpKind::RowDelta);
    }
}
