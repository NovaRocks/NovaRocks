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
//! StarRocks write-side format helpers.

pub mod bundle_meta;
pub mod io;
pub mod layout;
pub mod parquet;
pub mod segment_data;
pub mod segment_meta;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StarRocksWriteFormat {
    Native,
    Parquet,
}

impl StarRocksWriteFormat {
    pub fn parse(raw: &str) -> Result<Self, String> {
        let normalized = raw.trim().to_ascii_lowercase();
        match normalized.as_str() {
            "native" => Ok(Self::Native),
            "parquet" => Ok(Self::Parquet),
            other => Err(format!(
                "unsupported starrocks.lake_data_write_format={other} (expected native|parquet)"
            )),
        }
    }
}

pub use layout::{
    build_data_file_name, build_txn_data_file_name, bundle_meta_file_path,
    combined_txn_log_file_path, join_tablet_path, txn_log_file_path,
    txn_log_file_path_with_load_id, txn_vlog_file_path,
};
pub use parquet::{read_bundle_parquet_snapshot_if_any, write_parquet_file};
pub use segment_data::build_starrocks_native_segment_bytes;
pub use segment_meta::{build_single_segment_metadata, sort_batch_for_native_write};
