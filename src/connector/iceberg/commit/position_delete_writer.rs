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

//! Minimal v2-compatible position-delete Parquet writer.
//!
//! iceberg-rust 0.9 ships `DataFileWriter` and `EqualityDeleteFileWriter` but
//! does not yet expose a public `PositionDeleteFileWriter` (see the comment
//! at [`vendor/iceberg-0.9.0/src/writer/base_writer/mod.rs:18`]). This module
//! fills the gap with the minimum needed by `RowDeltaCommit` (Plan Task 14B):
//! given a list of `(data_file_path, pos)` tuples already grouped by data
//! file, write one Parquet file per group with the v2 position-delete schema
//! (`[file_path STRING, pos BIGINT]` plus reserved field-ids 2147483546 /
//! 2147483545) and return a [`super::types::WrittenFile`] with
//! `content = PositionDeletes` and `referenced_data_file = <data file path>`.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema, SchemaRef as ArrowSchemaRef};
use iceberg::io::FileIO;
use iceberg::spec::{DataContentType, DataFileFormat};
use parquet::arrow::{ArrowWriter, PARQUET_FIELD_ID_META_KEY};
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use uuid::Uuid;

use super::types::WrittenFile;

/// Reserved Iceberg v2 field-id for the `file_path` column of a position
/// delete file.
const FIELD_ID_FILE_PATH: i32 = 2147483546;

/// Reserved Iceberg v2 field-id for the `pos` column of a position delete file.
const FIELD_ID_POS: i32 = 2147483545;

/// Group of position-delete tuples that all reference the same data file.
pub struct PositionDeleteGroup {
    pub referenced_data_file: String,
    /// Sorted in ascending `pos` order (the writer enforces this).
    pub positions: Vec<i64>,
}

/// Write each group to a single position-delete Parquet file under
/// `<table_metadata_dir>/data/_staging/<query_uuid>/`. Returns the
/// [`WrittenFile`] entries ready to be injected into
/// [`super::collector::IcebergCommitCollector`].
pub async fn write_position_delete_files(
    file_io: &FileIO,
    staging_dir: &str,
    partition_spec_id: i32,
    groups: Vec<PositionDeleteGroup>,
) -> Result<Vec<WrittenFile>, String> {
    let mut out = Vec::with_capacity(groups.len());
    for (idx, group) in groups.into_iter().enumerate() {
        if group.positions.is_empty() {
            continue;
        }
        let mut sorted = group.positions;
        sorted.sort_unstable();
        let n = sorted.len();
        let path = format!(
            "{staging_dir}/position-delete-{idx:08x}-{}.parquet",
            Uuid::new_v4()
        );
        let bytes = encode_position_delete_parquet(&group.referenced_data_file, &sorted)?;
        let file_size = bytes.len() as u64;
        write_bytes_via_file_io(file_io, &path, bytes).await?;

        out.push(WrittenFile {
            path,
            format: DataFileFormat::Parquet,
            content: DataContentType::PositionDeletes,
            partition_values: iceberg::spec::Struct::empty(),
            partition_spec_id,
            record_count: n as u64,
            file_size_in_bytes: file_size,
            split_offsets: vec![],
            column_sizes: HashMap::new(),
            value_counts: HashMap::new(),
            null_value_counts: HashMap::new(),
            key_metadata: None,
            referenced_data_file: Some(group.referenced_data_file),
        });
    }
    Ok(out)
}

/// Build the v2 position-delete schema with the reserved field-ids attached
/// as Parquet field-id metadata so iceberg readers route the columns
/// correctly.
fn position_delete_schema() -> ArrowSchemaRef {
    let mut file_path_meta = HashMap::new();
    file_path_meta.insert(
        PARQUET_FIELD_ID_META_KEY.to_string(),
        FIELD_ID_FILE_PATH.to_string(),
    );
    let mut pos_meta = HashMap::new();
    pos_meta.insert(
        PARQUET_FIELD_ID_META_KEY.to_string(),
        FIELD_ID_POS.to_string(),
    );
    Arc::new(ArrowSchema::new(vec![
        Field::new("file_path", DataType::Utf8, false).with_metadata(file_path_meta),
        Field::new("pos", DataType::Int64, false).with_metadata(pos_meta),
    ]))
}

fn encode_position_delete_parquet(
    referenced_data_file: &str,
    positions: &[i64],
) -> Result<Vec<u8>, String> {
    let schema = position_delete_schema();
    let file_path_array: ArrayRef = Arc::new(StringArray::from(vec![
        referenced_data_file;
        positions.len()
    ]));
    let pos_array: ArrayRef = Arc::new(Int64Array::from(positions.to_vec()));
    let batch = RecordBatch::try_new(schema.clone(), vec![file_path_array, pos_array])
        .map_err(|e| format!("position-delete RecordBatch::try_new failed: {e}"))?;

    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();
    let mut buf = Vec::with_capacity(positions.len() * 16 + 1024);
    {
        let mut writer = ArrowWriter::try_new(&mut buf, schema, Some(props))
            .map_err(|e| format!("ArrowWriter::try_new failed for position-delete: {e}"))?;
        writer
            .write(&batch)
            .map_err(|e| format!("ArrowWriter::write failed for position-delete: {e}"))?;
        writer
            .close()
            .map_err(|e| format!("ArrowWriter::close failed for position-delete: {e}"))?;
    }
    Ok(buf)
}

async fn write_bytes_via_file_io(
    file_io: &FileIO,
    path: &str,
    bytes: Vec<u8>,
) -> Result<(), String> {
    let output = file_io
        .new_output(path)
        .map_err(|e| format!("FileIO::new_output({path}) failed: {e}"))?;
    let mut w = output
        .writer()
        .await
        .map_err(|e| format!("FileIO::writer({path}) failed: {e}"))?;
    w.write(bytes.into())
        .await
        .map_err(|e| format!("write position-delete bytes to {path} failed: {e}"))?;
    w.close()
        .await
        .map_err(|e| format!("close position-delete output {path} failed: {e}"))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_has_reserved_field_ids() {
        let schema = position_delete_schema();
        assert_eq!(schema.fields().len(), 2);
        let file_field = schema.field(0);
        assert_eq!(file_field.name(), "file_path");
        assert_eq!(file_field.data_type(), &DataType::Utf8);
        let id = file_field
            .metadata()
            .get(PARQUET_FIELD_ID_META_KEY)
            .expect("file_path field-id meta");
        assert_eq!(id.parse::<i32>().unwrap(), FIELD_ID_FILE_PATH);

        let pos_field = schema.field(1);
        assert_eq!(pos_field.name(), "pos");
        assert_eq!(pos_field.data_type(), &DataType::Int64);
        let id = pos_field
            .metadata()
            .get(PARQUET_FIELD_ID_META_KEY)
            .expect("pos field-id meta");
        assert_eq!(id.parse::<i32>().unwrap(), FIELD_ID_POS);
    }

    #[test]
    fn encode_emits_non_empty_bytes() {
        let bytes = encode_position_delete_parquet("s3://x/y/data.parquet", &[0, 1, 5, 10])
            .expect("encode");
        assert!(
            bytes.len() > 200,
            "parquet body unexpectedly small: {}",
            bytes.len()
        );
    }
}
