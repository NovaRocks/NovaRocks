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
// KIND, either except as required by applicable law or agreed
// to in writing, either express or implied.  See the License
// for the specific language governing permissions and
// limitations under the License.

//! `WrittenFile` → `iceberg::spec::DataFile` conversion shared across
//! all three commit-action implementations.
//!
//! `DataFile` fields are `pub(crate)` in iceberg-rust 0.9, so construction
//! goes through `DataFileBuilder`. Phase 1 supplies only the fields the
//! sink already populates; column statistics (`column_sizes`, `value_counts`,
//! `null_value_counts`, `lower_bounds`, `upper_bounds`) are deferred per
//! spec §3.4 and remain at the builder defaults.

use iceberg::spec::{DataFile, DataFileBuilder};

use super::collector::IcebergCommitCollector;
use super::types::WrittenFile;

pub fn written_file_to_iceberg_data_file(
    f: &WrittenFile,
    _collector: &IcebergCommitCollector,
) -> Result<DataFile, String> {
    let mut builder = DataFileBuilder::default();
    builder
        .content(f.content)
        .file_path(f.path.clone())
        .file_format(f.format)
        .partition(f.partition_values.clone())
        .partition_spec_id(f.partition_spec_id)
        .record_count(f.record_count)
        .file_size_in_bytes(f.file_size_in_bytes);

    if !f.split_offsets.is_empty() {
        builder.split_offsets(Some(f.split_offsets.clone()));
    }
    if let Some(km) = &f.key_metadata {
        builder.key_metadata(Some(km.clone()));
    }
    if let Some(ref_path) = &f.referenced_data_file {
        builder.referenced_data_file(Some(ref_path.clone()));
    }
    if !f.column_sizes.is_empty() {
        builder.column_sizes(f.column_sizes.clone());
    }
    if !f.value_counts.is_empty() {
        builder.value_counts(f.value_counts.clone());
    }
    if !f.null_value_counts.is_empty() {
        builder.null_value_counts(f.null_value_counts.clone());
    }

    builder
        .build()
        .map_err(|e| format!("failed to build iceberg DataFile from WrittenFile: {e}"))
}
