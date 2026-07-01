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

use std::fs;
use std::io::Cursor;
use std::path::PathBuf;

use arrow::array::{Array, ArrayRef};
use arrow::compute::{cast, concat};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;

use crate::formats::starrocks::fs_access::resolve_format_path;
use crate::formats::starrocks::metadata::StarRocksTabletSnapshot;
use crate::fs::access::FsScheme;

pub fn read_bundle_parquet_snapshot_if_any(
    snapshot: &StarRocksTabletSnapshot,
    output_schema: SchemaRef,
) -> Result<Option<RecordBatch>, String> {
    if snapshot.segment_files.is_empty() {
        return Ok(None);
    }
    if snapshot
        .segment_files
        .iter()
        .any(|seg| seg.bundle_file_offset.is_some())
    {
        return Ok(None);
    }
    if snapshot
        .segment_files
        .iter()
        .any(|seg| !seg.name.to_ascii_lowercase().ends_with(".parquet"))
    {
        return Ok(None);
    }

    let mut batches = Vec::new();
    for seg in &snapshot.segment_files {
        let segment_batches = read_parquet_file(&seg.path)?;
        for batch in segment_batches {
            if batch.num_rows() == 0 {
                continue;
            }
            batches.push(align_batch_to_output_schema(batch, &output_schema)?);
        }
    }
    concat_batches(output_schema, batches)
}

pub fn write_parquet_file(path: &str, batch: &RecordBatch) -> Result<u64, String> {
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();
    reject_hdfs_path(path, "write_parquet_file")?;
    let access = resolve_format_path(path)?;

    match access.scheme() {
        FsScheme::Local => {
            let path_buf = PathBuf::from(path);
            if let Some(parent) = path_buf.parent() {
                fs::create_dir_all(parent)
                    .map_err(|e| format!("create parquet dir failed: {}", e))?;
            }
            let file = fs::File::create(&path_buf)
                .map_err(|e| format!("create parquet file failed: {}", e))?;
            let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props))
                .map_err(|e| format!("create parquet writer failed: {}", e))?;
            writer
                .write(batch)
                .map_err(|e| format!("write parquet batch failed: {}", e))?;
            writer
                .close()
                .map_err(|e| format!("close parquet writer failed: {}", e))?;
            let meta =
                fs::metadata(&path_buf).map_err(|e| format!("stat parquet failed: {}", e))?;
            Ok(meta.len())
        }
        FsScheme::ObjectStore => {
            let rel = access.single_relative_path()?.to_string();
            let mut bytes = Vec::new();
            {
                let cursor = Cursor::new(&mut bytes);
                let mut writer = ArrowWriter::try_new(cursor, batch.schema(), Some(props))
                    .map_err(|e| format!("create parquet writer failed: {}", e))?;
                writer
                    .write(batch)
                    .map_err(|e| format!("write parquet batch failed: {}", e))?;
                writer
                    .close()
                    .map_err(|e| format!("close parquet writer failed: {}", e))?;
            }
            let size = bytes.len() as u64;
            let write_result =
                crate::fs::object_store::oss_block_on(access.operator().write(&rel, bytes))?;
            write_result.map_err(|e| format!("write parquet object failed: {}", e))?;
            Ok(size)
        }
        FsScheme::Hdfs => Err(format!(
            "write_parquet_file does not support hdfs path yet: {}",
            path
        )),
    }
}

pub fn read_parquet_file(path: &str) -> Result<Vec<RecordBatch>, String> {
    reject_hdfs_path(path, "read_parquet_file")?;
    let access = resolve_format_path(path)?;
    match access.scheme() {
        FsScheme::Local => {
            let file = fs::File::open(path).map_err(|e| format!("open parquet failed: {}", e))?;
            let reader = ParquetRecordBatchReaderBuilder::try_new(file)
                .map_err(|e| format!("create parquet reader failed: {}", e))?
                .build()
                .map_err(|e| format!("build parquet reader failed: {}", e))?;
            let mut out = Vec::new();
            for batch in reader {
                out.push(batch.map_err(|e| format!("read parquet batch failed: {}", e))?);
            }
            Ok(out)
        }
        FsScheme::ObjectStore => {
            let rel = access.single_relative_path()?.to_string();
            let read_result =
                crate::fs::object_store::oss_block_on(access.operator().read(&rel))?;
            let bytes = read_result.map_err(|e| format!("read parquet object failed: {}", e))?;
            let reader = ParquetRecordBatchReaderBuilder::try_new(bytes.to_bytes())
                .map_err(|e| format!("create parquet reader failed: {}", e))?
                .build()
                .map_err(|e| format!("build parquet reader failed: {}", e))?;
            let mut out = Vec::new();
            for batch in reader {
                out.push(batch.map_err(|e| format!("read parquet batch failed: {}", e))?);
            }
            Ok(out)
        }
        FsScheme::Hdfs => Err(format!(
            "read_parquet_file does not support hdfs path yet: {}",
            path
        )),
    }
}

fn reject_hdfs_path(path: &str, function_name: &str) -> Result<(), String> {
    let trimmed = path.trim();
    if trimmed
        .split_once("://")
        .is_some_and(|(scheme, _)| scheme.eq_ignore_ascii_case("hdfs"))
    {
        return Err(format!(
            "{function_name} does not support hdfs path yet: {path}"
        ));
    }
    Ok(())
}

fn align_batch_to_output_schema(
    batch: RecordBatch,
    output_schema: &SchemaRef,
) -> Result<RecordBatch, String> {
    let mut name_to_index: std::collections::HashMap<String, usize> =
        std::collections::HashMap::new();
    for (idx, field) in batch.schema().fields().iter().enumerate() {
        let normalized_name = normalize_column_name(field.name());
        if !normalized_name.is_empty() {
            name_to_index.entry(normalized_name).or_insert(idx);
        }
    }

    let mut arrays = Vec::with_capacity(output_schema.fields().len());
    for (idx, field) in output_schema.fields().iter().enumerate() {
        let source_idx = *name_to_index
            .get(&normalize_column_name(field.name()))
            .ok_or_else(|| {
                format!(
                    "parquet output column '{}' not found in source schema by normalized name; source_fields={}",
                    field.name(),
                    debug_schema_fields(batch.schema().as_ref())
                )
            })?;
        let src = batch.column(source_idx).clone();
        let out = if src.data_type() == field.data_type() {
            src
        } else {
            cast(src.as_ref(), field.data_type()).map_err(|e| {
                format!(
                    "cast parquet column failed: output_idx={} output_name={} source_idx={} from {:?} to {:?}: {}",
                    idx,
                    field.name(),
                    source_idx,
                    src.data_type(),
                    field.data_type(),
                    e
                )
            })?
        };
        arrays.push(out);
    }
    RecordBatch::try_new(output_schema.clone(), arrays)
        .map_err(|e| format!("build aligned batch failed: {}", e))
}

fn normalize_column_name(name: &str) -> String {
    name.trim()
        .trim_matches('`')
        .trim_matches('"')
        .to_ascii_lowercase()
}

fn debug_schema_fields(schema: &arrow::datatypes::Schema) -> String {
    schema
        .fields()
        .iter()
        .enumerate()
        .map(|(idx, field)| format!("#{idx}:{}:{:?}", field.name(), field.data_type()))
        .collect::<Vec<_>>()
        .join(", ")
}

fn concat_batches(
    output_schema: SchemaRef,
    batches: Vec<RecordBatch>,
) -> Result<Option<RecordBatch>, String> {
    if batches.is_empty() {
        return Ok(None);
    }
    let num_cols = output_schema.fields().len();
    let mut by_col: Vec<Vec<ArrayRef>> = (0..num_cols).map(|_| Vec::new()).collect();
    let mut total_rows = 0usize;
    for batch in batches {
        if batch.num_rows() == 0 {
            continue;
        }
        total_rows = total_rows.saturating_add(batch.num_rows());
        for (col_idx, columns) in by_col.iter_mut().enumerate().take(num_cols) {
            columns.push(batch.column(col_idx).clone());
        }
    }
    if total_rows == 0 {
        return Ok(None);
    }

    let mut merged = Vec::with_capacity(num_cols);
    for arrays in by_col {
        if arrays.is_empty() {
            return Err("empty column arrays while concatenating".to_string());
        }
        if arrays.len() == 1 {
            merged.push(arrays[0].clone());
            continue;
        }
        let refs: Vec<&dyn Array> = arrays.iter().map(|a| a.as_ref()).collect();
        let arr = concat(&refs).map_err(|e| format!("concat arrays failed: {}", e))?;
        merged.push(arr);
    }

    let out = RecordBatch::try_new(output_schema, merged)
        .map_err(|e| format!("build batch failed: {}", e))?;
    Ok(Some(out))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};

    fn sample_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int32, false),
            Field::new("v", DataType::Utf8, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef,
                Arc::new(StringArray::from(vec![Some("a"), None])) as ArrayRef,
            ],
        )
        .expect("build sample batch")
    }

    #[test]
    fn parquet_helpers_round_trip_local_path() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let path = temp_dir
            .path()
            .join("nested")
            .join("data.parquet")
            .to_string_lossy()
            .to_string();
        let batch = sample_batch();

        let size = write_parquet_file(&path, &batch).expect("write parquet");
        let batches = read_parquet_file(&path).expect("read parquet");

        assert!(size > 0);
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 2);
        let keys = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("int column");
        assert_eq!(keys.value(0), 1);
        assert_eq!(keys.value(1), 2);
        let values = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("string column");
        assert_eq!(values.value(0), "a");
        assert!(values.is_null(1));
    }

    #[test]
    fn parquet_helpers_use_format_path_resolver_for_object_store_credentials() {
        let _guard = crate::connector::starrocks::lake::context::lock_runtime_test_state();
        let path = "s3://missing-bucket/warehouse/tablet-1/data.parquet";

        let read_err = read_parquet_file(path).expect_err("missing runtime S3 config must fail");
        assert!(
            read_err.contains("missing S3 config for StarRocks object-store path="),
            "{read_err}"
        );

        let batch = sample_batch();
        let write_err =
            write_parquet_file(path, &batch).expect_err("missing runtime S3 config must fail");
        assert!(
            write_err.contains("missing S3 config for StarRocks object-store path="),
            "{write_err}"
        );
    }

    #[test]
    fn parquet_helpers_reject_malformed_hdfs_with_function_specific_errors() {
        let path = "hdfs://nn:9000";

        let batch = sample_batch();
        let write_err = write_parquet_file(path, &batch).expect_err("hdfs parquet write must fail");
        assert!(
            write_err.contains("write_parquet_file does not support hdfs path yet"),
            "{write_err}"
        );

        let read_err = read_parquet_file(path).expect_err("hdfs parquet read must fail");
        assert!(
            read_err.contains("read_parquet_file does not support hdfs path yet"),
            "{read_err}"
        );
    }
}
