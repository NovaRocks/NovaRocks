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

use crate::common::ids::SlotId;
use crate::exec::chunk::field_slot_id;
use crate::formats::starrocks::metadata::StarRocksTabletSnapshot;
use crate::fs::path::{ScanPathScheme, classify_scan_paths};

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
    let scheme = classify_scan_paths([path])?;

    match scheme {
        ScanPathScheme::Local => {
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
        ScanPathScheme::Oss => {
            let (op, rel) = crate::fs::oss::resolve_oss_operator_and_path(path)?;

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
            let write_result = crate::fs::oss::oss_block_on(op.write(&rel, bytes))?;
            write_result.map_err(|e| format!("write parquet object failed: {}", e))?;
            Ok(size)
        }
    }
}

pub fn read_parquet_file(path: &str) -> Result<Vec<RecordBatch>, String> {
    let scheme = classify_scan_paths([path])?;
    match scheme {
        ScanPathScheme::Local => {
            let file = fs::File::open(path).map_err(|e| format!("open parquet failed: {}", e))?;
            let mut reader = ParquetRecordBatchReaderBuilder::try_new(file)
                .map_err(|e| format!("create parquet reader failed: {}", e))?
                .build()
                .map_err(|e| format!("build parquet reader failed: {}", e))?;
            let mut out = Vec::new();
            while let Some(batch) = reader.next() {
                out.push(batch.map_err(|e| format!("read parquet batch failed: {}", e))?);
            }
            Ok(out)
        }
        ScanPathScheme::Oss => {
            let (op, rel) = crate::fs::oss::resolve_oss_operator_and_path(path)?;
            let read_result = crate::fs::oss::oss_block_on(op.read(&rel))?;
            let bytes = read_result.map_err(|e| format!("read parquet object failed: {}", e))?;
            let mut reader = ParquetRecordBatchReaderBuilder::try_new(bytes.to_bytes())
                .map_err(|e| format!("create parquet reader failed: {}", e))?
                .build()
                .map_err(|e| format!("build parquet reader failed: {}", e))?;
            let mut out = Vec::new();
            while let Some(batch) = reader.next() {
                out.push(batch.map_err(|e| format!("read parquet batch failed: {}", e))?);
            }
            Ok(out)
        }
    }
}

fn align_batch_to_output_schema(
    batch: RecordBatch,
    output_schema: &SchemaRef,
) -> Result<RecordBatch, String> {
    let mut name_to_index: std::collections::HashMap<String, usize> =
        std::collections::HashMap::new();
    let mut slot_id_to_index: std::collections::HashMap<SlotId, usize> =
        std::collections::HashMap::new();
    for (idx, field) in batch.schema().fields().iter().enumerate() {
        let normalized_name = normalize_column_name(field.name());
        if !normalized_name.is_empty() {
            name_to_index.entry(normalized_name).or_insert(idx);
        }
        if let Ok(Some(slot_id)) = field_slot_id(field.as_ref()) {
            slot_id_to_index.entry(slot_id).or_insert(idx);
        } else if let Some(slot_id) = parse_slot_id_from_field_name(field.name()) {
            slot_id_to_index.entry(slot_id).or_insert(idx);
        }
    }

    let mut arrays = Vec::with_capacity(output_schema.fields().len());
    for (idx, field) in output_schema.fields().iter().enumerate() {
        let source_idx = if let Some(named_idx) =
            name_to_index.get(&normalize_column_name(field.name()))
        {
            *named_idx
        } else if let Some(slot_id) = field_slot_id(field.as_ref()).map_err(|e| {
            format!(
                "parse output field slot id failed: output_name={} error={e}",
                field.name()
            )
        })? {
            *slot_id_to_index.get(&slot_id).ok_or_else(|| {
                format!(
                    "parquet output column '{}' (slot_id={}) not found in source schema by name/slot_id; source_fields={}",
                    field.name(),
                    slot_id,
                    debug_schema_fields(batch.schema().as_ref())
                )
            })?
        } else {
            return Err(format!(
                "parquet output column '{}' not found in source schema by name and output has no slot_id metadata; source_fields={}",
                field.name(),
                debug_schema_fields(batch.schema().as_ref())
            ));
        };
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

fn parse_slot_id_from_field_name(name: &str) -> Option<SlotId> {
    let trimmed = name.trim().trim_matches('`').trim_matches('"');
    trimmed.parse::<SlotId>().ok()
}

fn debug_schema_fields(schema: &arrow::datatypes::Schema) -> String {
    schema
        .fields()
        .iter()
        .enumerate()
        .map(|(idx, field)| {
            let slot = field
                .metadata()
                .get(crate::exec::chunk::FIELD_META_SLOT_ID)
                .cloned()
                .unwrap_or_else(|| "<none>".to_string());
            format!(
                "#{idx}:{}:{:?}:slot={slot}",
                field.name(),
                field.data_type()
            )
        })
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
        for col_idx in 0..num_cols {
            by_col[col_idx].push(batch.column(col_idx).clone());
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
