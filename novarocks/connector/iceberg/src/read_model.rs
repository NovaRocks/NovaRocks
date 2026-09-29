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

//! Provider-owned Iceberg read-view model and delete applicability.

use std::collections::HashMap;
use std::sync::Arc;

use crate::delete_semantics::{DeleteSet, LoadView, ReadDomain};
use crate::iceberg::spec::{DataFileFormat, Datum};

use crate::scan_model::IcebergColumnStats;

#[cfg(test)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum IcebergReadDeleteFormat {
    Parquet,
    Puffin,
}

#[cfg(test)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum IcebergReadDeleteKind {
    Position,
    Equality { equality_field_ids: Vec<i32> },
}

#[cfg(test)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IcebergReadDeleteFile {
    pub path: String,
    pub file_format: IcebergReadDeleteFormat,
    pub kind: IcebergReadDeleteKind,
    pub length: Option<i64>,
    pub content_offset: Option<i64>,
    pub content_size_in_bytes: Option<i64>,
    pub sequence_number: Option<i64>,
    pub partition_spec_id: Option<i32>,
    pub partition_key: Option<String>,
    pub referenced_data_file: Option<String>,
}

/// Complete data-file facts from the same manifest observation as its deletes.
#[derive(Clone, Debug)]
pub struct IcebergDataFileMetadata {
    pub file_format: DataFileFormat,
    pub split_offsets: Vec<i64>,
    pub key_metadata: Vec<u8>,
    pub value_counts: HashMap<i32, u64>,
    pub null_value_counts: HashMap<i32, u64>,
    pub nan_value_counts: HashMap<i32, u64>,
    pub lower_bounds: HashMap<i32, Datum>,
    pub upper_bounds: HashMap<i32, Datum>,
}

#[derive(Clone, Debug)]
pub struct IcebergReadFile {
    pub path: String,
    pub size: i64,
    pub record_count: Option<i64>,
    pub column_stats: Option<HashMap<String, IcebergColumnStats>>,
    pub partition_spec_id: Option<i32>,
    pub partition_key: Option<String>,
    pub partition_values: Option<crate::iceberg::spec::Struct>,
    pub manifest_path: Option<String>,
    pub first_row_id: Option<i64>,
    pub data_sequence_number: Option<i64>,
    pub manifest: Arc<IcebergDataFileMetadata>,
    /// Statistics may reduce required loads, but never the logical closure.
    pub deletes: LoadView,
}

impl IcebergReadFile {
    pub fn logical_delete_set(&self) -> &DeleteSet {
        self.deletes.logical()
    }

    pub fn read_domain(&self) -> &Arc<ReadDomain> {
        self.logical_delete_set().domain()
    }
}

#[derive(Clone, Debug)]
pub struct IcebergReadSnapshot {
    pub snapshot_id: Option<i64>,
    pub files: Vec<IcebergReadFile>,
}

pub fn iceberg_partition_key(partition: &crate::iceberg::spec::Struct) -> Option<String> {
    if partition.fields().is_empty() {
        None
    } else {
        Some(format!("{partition:?}"))
    }
}
