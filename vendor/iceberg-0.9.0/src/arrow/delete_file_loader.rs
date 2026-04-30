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

use std::io::{Cursor, Read};
use std::sync::Arc;

use futures::{StreamExt, TryStreamExt};
use parquet::arrow::ParquetRecordBatchStreamBuilder;
use roaring::RoaringBitmap;

use crate::arrow::ArrowReader;
use crate::arrow::reader::ParquetReadOptions;
use crate::arrow::record_batch_transformer::RecordBatchTransformerBuilder;
use crate::delete_vector::DeleteVector;
use crate::io::FileIO;
use crate::scan::{ArrowRecordBatchStream, FileScanTaskDeleteFile};
use crate::spec::{Schema, SchemaRef};
use crate::{Error, ErrorKind, Result};

/// Magic bytes that prefix the body of an Iceberg `deletion-vector-v1`
/// payload, immediately after the 4-byte big-endian declared length.
const DELETION_VECTOR_V1_MAGIC: [u8; 4] = [0xD1, 0xD3, 0x39, 0x64];

/// Delete File Loader
#[allow(unused)]
#[async_trait::async_trait]
pub trait DeleteFileLoader {
    /// Read the delete file referred to in the task
    ///
    /// Returns the contents of the delete file as a RecordBatch stream. Applies schema evolution.
    async fn read_delete_file(
        &self,
        task: &FileScanTaskDeleteFile,
        schema: SchemaRef,
    ) -> Result<ArrowRecordBatchStream>;
}

#[derive(Clone, Debug)]
pub(crate) struct BasicDeleteFileLoader {
    file_io: FileIO,
}

#[allow(unused_variables)]
impl BasicDeleteFileLoader {
    pub fn new(file_io: FileIO) -> Self {
        BasicDeleteFileLoader { file_io }
    }
    /// Loads a RecordBatchStream for a given datafile.
    pub(crate) async fn parquet_to_batch_stream(
        &self,
        data_file_path: &str,
        file_size_in_bytes: u64,
    ) -> Result<ArrowRecordBatchStream> {
        /*
           Essentially a super-cut-down ArrowReader. We can't use ArrowReader directly
           as that introduces a circular dependency.
        */
        let parquet_read_options = ParquetReadOptions::builder().build();

        let (parquet_file_reader, arrow_metadata, _missing_field_ids) =
            ArrowReader::open_parquet_file(
            data_file_path,
            &self.file_io,
            file_size_in_bytes,
            parquet_read_options,
            None,
            Vec::new(),
        )
        .await?;

        let record_batch_stream =
            ParquetRecordBatchStreamBuilder::new_with_metadata(parquet_file_reader, arrow_metadata)
                .build()?
                .map_err(|e| Error::new(ErrorKind::Unexpected, format!("{e}")));

        Ok(Box::pin(record_batch_stream) as ArrowRecordBatchStream)
    }

    /// Read the Puffin `deletion-vector-v1` blob at
    /// `[content_offset, content_offset + content_size_in_bytes)` from `path`
    /// and decode it into a [`DeleteVector`].
    ///
    /// The framed payload is `4 BE length || 4 magic D1 D3 39 64 || body || 4
    /// BE CRC32`, where `body = 8 LE bitmap_count || (4 LE high32 || roaring
    /// portable bitmap)*` (Iceberg v3 deletion-vector-v1).
    pub(crate) async fn puffin_dv_to_delete_vector(
        &self,
        path: &str,
        content_offset: i64,
        content_size_in_bytes: i64,
    ) -> Result<DeleteVector> {
        if content_offset < 0 {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!("Puffin deletion-vector content_offset must be non-negative, got {content_offset} for {path}"),
            ));
        }
        if content_size_in_bytes < 0 {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!("Puffin deletion-vector content_size_in_bytes must be non-negative, got {content_size_in_bytes} for {path}"),
            ));
        }
        let start = content_offset as u64;
        let end = start
            .checked_add(content_size_in_bytes as u64)
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!("Puffin deletion-vector byte range overflows u64 for {path}"),
                )
            })?;

        let input = self.file_io.new_input(path)?;
        let reader = input.reader().await?;
        let bytes = reader.read(start..end).await?;
        decode_iceberg_deletion_vector_v1(bytes.as_ref())
    }

    /// Evolves the schema of the RecordBatches from an equality delete file.
    ///
    /// Per the [Iceberg spec](https://iceberg.apache.org/spec/#equality-delete-files),
    /// only evolves the specified `equality_ids` columns, not all table columns.
    pub(crate) async fn evolve_schema(
        record_batch_stream: ArrowRecordBatchStream,
        target_schema: Arc<Schema>,
        equality_ids: &[i32],
    ) -> Result<ArrowRecordBatchStream> {
        let mut record_batch_transformer =
            RecordBatchTransformerBuilder::new(target_schema.clone(), equality_ids).build();

        let record_batch_stream = record_batch_stream.map(move |record_batch| {
            record_batch.and_then(|record_batch| {
                record_batch_transformer.process_record_batch(record_batch)
            })
        });

        Ok(Box::pin(record_batch_stream) as ArrowRecordBatchStream)
    }
}

/// Decode an Iceberg `deletion-vector-v1` framed payload into a
/// [`DeleteVector`]. CRC verification is best-effort: a CRC mismatch is
/// reported as a `DataInvalid` error.
fn decode_iceberg_deletion_vector_v1(payload: &[u8]) -> Result<DeleteVector> {
    if payload.len() < 4 + DELETION_VECTOR_V1_MAGIC.len() + 8 + 4 {
        return Err(Error::new(
            ErrorKind::DataInvalid,
            format!(
                "Puffin deletion-vector-v1 payload too short ({} bytes)",
                payload.len()
            ),
        ));
    }
    let declared_len = u32::from_be_bytes(payload[..4].try_into().unwrap()) as usize;
    if 4 + declared_len + 4 != payload.len() {
        return Err(Error::new(
            ErrorKind::DataInvalid,
            format!(
                "Puffin deletion-vector-v1 length mismatch: declared {} payload {} total",
                declared_len,
                payload.len()
            ),
        ));
    }
    let body = &payload[4..4 + declared_len];
    if body.len() < DELETION_VECTOR_V1_MAGIC.len()
        || body[..DELETION_VECTOR_V1_MAGIC.len()] != DELETION_VECTOR_V1_MAGIC
    {
        return Err(Error::new(
            ErrorKind::DataInvalid,
            "Puffin deletion-vector-v1 magic mismatch",
        ));
    }

    let mut cursor = Cursor::new(&body[DELETION_VECTOR_V1_MAGIC.len()..]);
    let mut count_buf = [0u8; 8];
    cursor.read_exact(&mut count_buf).map_err(|e| {
        Error::new(
            ErrorKind::DataInvalid,
            format!("Puffin deletion-vector-v1: failed to read bitmap count: {e}"),
        )
    })?;
    let bitmap_count = u64::from_le_bytes(count_buf);

    let mut dv = DeleteVector::default();
    for _ in 0..bitmap_count {
        let mut key_buf = [0u8; 4];
        cursor.read_exact(&mut key_buf).map_err(|e| {
            Error::new(
                ErrorKind::DataInvalid,
                format!("Puffin deletion-vector-v1: failed to read bitmap key: {e}"),
            )
        })?;
        let high = u64::from(u32::from_le_bytes(key_buf));
        let bitmap = RoaringBitmap::deserialize_from(&mut cursor).map_err(|e| {
            Error::new(
                ErrorKind::DataInvalid,
                format!("Puffin deletion-vector-v1: failed to deserialize bitmap: {e}"),
            )
        })?;
        for low in &bitmap {
            dv.insert((high << 32) | u64::from(low));
        }
    }
    Ok(dv)
}

#[async_trait::async_trait]
impl DeleteFileLoader for BasicDeleteFileLoader {
    async fn read_delete_file(
        &self,
        task: &FileScanTaskDeleteFile,
        schema: SchemaRef,
    ) -> Result<ArrowRecordBatchStream> {
        let raw_batch_stream = self
            .parquet_to_batch_stream(&task.file_path, task.file_size_in_bytes)
            .await?;

        // For equality deletes, only evolve the equality_ids columns.
        // For positional deletes (equality_ids is None), use all field IDs.
        let field_ids = match &task.equality_ids {
            Some(ids) => ids.clone(),
            None => schema.field_id_to_name_map().keys().cloned().collect(),
        };

        Self::evolve_schema(raw_batch_stream, schema, &field_ids).await
    }
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;
    use crate::arrow::delete_filter::tests::setup;

    #[tokio::test]
    async fn test_basic_delete_file_loader_read_delete_file() {
        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path();
        let file_io = FileIO::new_with_fs();

        let delete_file_loader = BasicDeleteFileLoader::new(file_io.clone());

        let file_scan_tasks = setup(table_location);

        let result = delete_file_loader
            .read_delete_file(
                &file_scan_tasks[0].deletes[0],
                file_scan_tasks[0].schema_ref(),
            )
            .await
            .unwrap();

        let result = result.try_collect::<Vec<_>>().await.unwrap();

        assert_eq!(result.len(), 1);
    }
}
