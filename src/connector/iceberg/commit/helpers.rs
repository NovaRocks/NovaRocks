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

//! Shared utilities for the self-implemented commit-actions
//! (`RowDeltaCommit` and `OverwriteCommit`).

use iceberg::io::FileIO;
use iceberg::spec::{FormatVersion, ManifestFile, ManifestListWriter};

/// Generate an Iceberg-spec-compliant random positive snapshot id.
pub fn generate_snapshot_id() -> i64 {
    use rand::Rng;
    let mut rng = rand::thread_rng();
    rng.r#gen::<i64>().abs()
}

/// Current wall-clock time in milliseconds since the Unix epoch.
pub fn now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Resolve the metadata directory for a table — i.e. the directory containing
/// `metadata.json`, manifest-list, and manifest avro files.
pub fn metadata_dir(table: &iceberg::table::Table) -> String {
    format!("{}/metadata", table.metadata().location())
}

/// Write a manifest list (avro) to `out_path` containing the supplied entries.
/// Caller is responsible for `abort_handle.record_manifest(out_path)` before
/// invoking this function so that a later failure can clean up.
pub async fn write_manifest_list(
    file_io: &FileIO,
    out_path: &str,
    entries: Vec<ManifestFile>,
    snap_id: i64,
    parent_snap_id: Option<i64>,
    sequence_number: i64,
    format_version: FormatVersion,
    first_row_id: Option<u64>,
) -> Result<Option<u64>, String> {
    let output = file_io
        .new_output(out_path)
        .map_err(|e| format!("FileIO::new_output({out_path}) failed: {e}"))?;
    let mut writer = match format_version {
        FormatVersion::V1 => ManifestListWriter::v1(output, snap_id, parent_snap_id),
        FormatVersion::V2 => {
            ManifestListWriter::v2(output, snap_id, parent_snap_id, sequence_number)
        }
        FormatVersion::V3 => ManifestListWriter::v3(
            output,
            snap_id,
            parent_snap_id,
            sequence_number,
            first_row_id,
        ),
    };
    writer
        .add_manifests(entries.into_iter())
        .map_err(|e| format!("ManifestListWriter::add_manifests failed: {e}"))?;
    let next_row_id = writer.next_row_id();
    writer
        .close()
        .await
        .map_err(|e| format!("ManifestListWriter::close failed: {e}"))?;
    Ok(next_row_id)
}

/// Read the manifest list referenced by `current_snapshot()` and return its
/// `ManifestFile` entries. Returns an empty Vec if the table has no current
/// snapshot.
pub async fn read_base_manifest_list(
    table: &iceberg::table::Table,
    file_io: &FileIO,
) -> Result<Vec<ManifestFile>, String> {
    let m = table.metadata();
    let snap = match m.current_snapshot() {
        Some(s) => s,
        None => return Ok(Vec::new()),
    };
    let bytes = file_io
        .new_input(snap.manifest_list())
        .map_err(|e| format!("FileIO::new_input({}) failed: {e}", snap.manifest_list()))?
        .read()
        .await
        .map_err(|e| format!("read manifest_list failed: {e}"))?;
    let list = iceberg::spec::ManifestList::parse_with_version(&bytes, m.format_version())
        .map_err(|e| format!("parse manifest_list failed: {e}"))?;
    Ok(list.entries().to_vec())
}
