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

//! Resolution-time manifest walk that materialises `$files` / `$manifests` /
//! `$entries` metadata-table rows as a JSON payload. Runs on the server thread
//! (async); the sync pipeline scan op cannot perform manifest I/O.
//!
//! The JSON row field names emitted here MUST match the analyzer column names
//! for these metadata tables so that the downstream scan-op builders can read
//! them back by name.

use std::collections::HashMap;

use iceberg::io::FileIO;
use iceberg::spec::{
    DataContentType, DataFile, DataFileFormat, ManifestContentType, ManifestStatus,
};
use iceberg::table::Table;
use serde_json::{Value, json};

use crate::connector::iceberg::IcebergMetadataTableType;

fn partition_string(df: &DataFile) -> String {
    crate::connector::iceberg::read::iceberg_partition_key(df.partition())
        .unwrap_or_else(|| "Struct([])".to_string())
}

fn content_code(df: &DataFile) -> i32 {
    match df.content_type() {
        DataContentType::Data => 0,
        DataContentType::PositionDeletes => 1,
        DataContentType::EqualityDeletes => 2,
    }
}

fn file_format_str(fmt: DataFileFormat) -> &'static str {
    match fmt {
        DataFileFormat::Parquet => "PARQUET",
        DataFileFormat::Orc => "ORC",
        DataFileFormat::Avro => "AVRO",
        DataFileFormat::Puffin => "PUFFIN",
    }
}

fn int_map(m: &HashMap<i32, u64>) -> Value {
    let mut pairs: Vec<(i32, u64)> = m.iter().map(|(k, v)| (*k, *v)).collect();
    pairs.sort_by_key(|(k, _)| *k);
    json!(pairs.iter().map(|(k, v)| json!([k, v])).collect::<Vec<_>>())
}

fn bytes_map(m: &HashMap<i32, Vec<u8>>) -> Value {
    let mut pairs: Vec<(i32, Vec<u8>)> = m.iter().map(|(k, v)| (*k, v.clone())).collect();
    pairs.sort_by_key(|(k, _)| *k);
    json!(pairs.iter().map(|(k, v)| json!([k, v])).collect::<Vec<_>>())
}

// `spec_id` is sourced from the enclosing `ManifestFile.partition_spec_id`:
// the iceberg-rust `DataFile::partition_spec_id` is crate-private with no
// public accessor, and every data file in a manifest shares the manifest's
// partition spec.
fn file_row(df: &DataFile, spec_id: i32, entry_cols: Option<Value>) -> Result<Value, String> {
    let mut lower = HashMap::new();
    for (k, datum) in df.lower_bounds() {
        if let Ok(b) = datum.to_bytes() {
            lower.insert(*k, b.to_vec());
        }
    }
    let mut upper = HashMap::new();
    for (k, datum) in df.upper_bounds() {
        if let Ok(b) = datum.to_bytes() {
            upper.insert(*k, b.to_vec());
        }
    }
    let base = json!({
        "content": content_code(df),
        "file_path": df.file_path(),
        "file_format": file_format_str(df.file_format()),
        "spec_id": spec_id,
        "record_count": df.record_count(),
        "file_size_in_bytes": df.file_size_in_bytes(),
        "column_sizes": int_map(df.column_sizes()),
        "value_counts": int_map(df.value_counts()),
        "null_value_counts": int_map(df.null_value_counts()),
        "nan_value_counts": int_map(df.nan_value_counts()),
        "lower_bounds": bytes_map(&lower),
        "upper_bounds": bytes_map(&upper),
        "split_offsets": df.split_offsets(),
        "equality_ids": df.equality_ids(),
        "sort_order_id": df.sort_order_id(),
        "key_metadata": df.key_metadata().map(|b| b.to_vec()),
        "first_row_id": df.first_row_id(),
        "partition": partition_string(df),
    });
    match entry_cols {
        None => Ok(base),
        Some(Value::Object(mut entry)) => {
            if let Value::Object(b) = base {
                entry.extend(b);
            }
            Ok(Value::Object(entry))
        }
        Some(_) => Err("entry columns must be a JSON object".to_string()),
    }
}

pub async fn read_metadata_table_rows(
    table: &Table,
    file_io: &FileIO,
    ty: IcebergMetadataTableType,
) -> Result<String, String> {
    let metadata = table.metadata();
    let Some(snapshot) = metadata.current_snapshot() else {
        return Ok(json!({ "version": 1, "rows": [] }).to_string());
    };
    let manifest_list = snapshot
        .load_manifest_list(file_io, metadata)
        .await
        .map_err(|e| format!("load manifest list: {e}"))?;
    let mut rows: Vec<Value> = Vec::new();
    for mf in manifest_list.entries() {
        if ty == IcebergMetadataTableType::Manifests {
            let partition_summaries = mf
                .partitions
                .iter()
                .flatten()
                .map(|p| {
                    json!({
                        "contains_null": p.contains_null,
                        "contains_nan": p.contains_nan,
                        "lower_bound": p.lower_bound.as_ref().map(|b| format!("{b:?}")),
                        "upper_bound": p.upper_bound.as_ref().map(|b| format!("{b:?}")),
                    })
                })
                .collect::<Vec<_>>();
            rows.push(json!({
                "content": match mf.content {
                    ManifestContentType::Data => 0,
                    ManifestContentType::Deletes => 1,
                },
                "path": mf.manifest_path,
                "length": mf.manifest_length,
                "partition_spec_id": mf.partition_spec_id,
                "added_snapshot_id": mf.added_snapshot_id,
                "added_data_files_count": mf.added_files_count,
                "existing_data_files_count": mf.existing_files_count,
                "deleted_data_files_count": mf.deleted_files_count,
                "added_rows_count": mf.added_rows_count,
                "existing_rows_count": mf.existing_rows_count,
                "deleted_rows_count": mf.deleted_rows_count,
                "partition_summaries": partition_summaries,
            }));
            continue;
        }
        let manifest = mf
            .load_manifest(file_io)
            .await
            .map_err(|e| format!("load manifest {}: {e}", mf.manifest_path))?;
        for entry in manifest.entries() {
            let df = entry.data_file();
            match ty {
                IcebergMetadataTableType::Files => {
                    if entry.status() == ManifestStatus::Deleted {
                        continue;
                    }
                    rows.push(file_row(df, mf.partition_spec_id, None)?);
                }
                IcebergMetadataTableType::LogicalIcebergMetadata => {
                    let status = match entry.status() {
                        ManifestStatus::Existing => 0,
                        ManifestStatus::Added => 1,
                        ManifestStatus::Deleted => 2,
                    };
                    let entry_cols = json!({
                        "status": status,
                        "snapshot_id": entry.snapshot_id(),
                        "sequence_number": entry.sequence_number(),
                        "file_sequence_number": entry.file_sequence_number,
                        "first_row_id": df.first_row_id(),
                    });
                    rows.push(file_row(df, mf.partition_spec_id, Some(entry_cols))?);
                }
                _ => unreachable!("manifests handled above"),
            }
        }
    }
    Ok(json!({ "version": 1, "rows": rows }).to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn read_metadata_table_rows_empty_snapshot_returns_empty() {
        let fixture =
            crate::connector::iceberg::commit::test_helpers::empty_v3_iceberg_table().await;
        let file_io = fixture.table.file_io().clone();
        let payload =
            read_metadata_table_rows(&fixture.table, &file_io, IcebergMetadataTableType::Files)
                .await
                .expect("read_metadata_table_rows on empty table should succeed");
        // Compare as parsed JSON values: serde_json serialises object keys in
        // alphabetical order, so a raw string equality on the payload is
        // ordering-sensitive while the contract is purely structural.
        let actual: Value = serde_json::from_str(&payload).expect("payload must be valid JSON");
        let expected: Value = json!({ "version": 1, "rows": [] });
        assert_eq!(actual, expected);
    }
}
