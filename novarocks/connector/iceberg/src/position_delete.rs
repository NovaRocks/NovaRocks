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

//! Physical Iceberg position-delete application for provider batch readers.

use arrow::array::{Array, Int64Array, StringArray};
use novarocks_fs::{FileProjection, FileReadContext, FileReadRange, FsAccessHandle};
use roaring::RoaringTreemap;

use crate::commit::DeletionVector;
use crate::delete_file::{IcebergDeleteFileSpec, IcebergFileContent, IcebergFileFormat};

pub const FILE_PATH_COLUMN: &str = "file_path";
pub const POS_COLUMN: &str = "pos";

/// Loads the delete positions attached to one provider-frozen data file using
/// the exact cancellation, deadline, and runtime resources of its reader.
pub fn load_position_deletes_with_context(
    specs: &[IcebergDeleteFileSpec],
    data_file_path: &str,
    access: &FsAccessHandle,
    context: &FileReadContext,
) -> Result<RoaringTreemap, String> {
    let mut deleted = RoaringTreemap::new();
    for spec in specs {
        if spec.file_content != IcebergFileContent::PositionDeletes {
            continue;
        }
        match plan_position_delete(spec, data_file_path)? {
            PositionDeleteRead::DeletionVector(range) => {
                let payload = crate::file_reader::read_bytes(
                    access,
                    &spec.path,
                    spec.length,
                    range,
                    context,
                )?;
                apply_deletion_vector(spec, &payload, &mut deleted)?;
            }
            PositionDeleteRead::Parquet => {
                let batches = crate::file_reader::read_parquet_batches(
                    access,
                    &spec.path,
                    spec.length,
                    position_delete_projection(),
                    context.clone(),
                )?;
                apply_position_delete_batches(spec, data_file_path, batches, &mut deleted)?;
            }
        }
    }
    Ok(deleted)
}

/// Awaited [`load_position_deletes_with_context`]: the same reads, awaited
/// through the source's range service.
pub async fn load_position_deletes_async(
    specs: &[IcebergDeleteFileSpec],
    data_file_path: &str,
    access: &FsAccessHandle,
    context: &FileReadContext,
) -> Result<RoaringTreemap, String> {
    let mut deleted = RoaringTreemap::new();
    for spec in specs {
        if spec.file_content != IcebergFileContent::PositionDeletes {
            continue;
        }
        match plan_position_delete(spec, data_file_path)? {
            PositionDeleteRead::DeletionVector(range) => {
                let payload = crate::file_reader::read_bytes_async(
                    access,
                    &spec.path,
                    spec.length,
                    range,
                    context,
                )
                .await?;
                apply_deletion_vector(spec, &payload, &mut deleted)?;
            }
            PositionDeleteRead::Parquet => {
                let batches = crate::file_reader::read_parquet_batches_async(
                    access,
                    &spec.path,
                    spec.length,
                    position_delete_projection(),
                    context.clone(),
                )
                .await?;
                apply_position_delete_batches(spec, data_file_path, batches, &mut deleted)?;
            }
        }
    }
    Ok(deleted)
}

/// What one position-delete file needs read before it can be applied.
enum PositionDeleteRead {
    /// A deletion vector: one byte range of its Puffin container.
    DeletionVector(FileReadRange),
    /// A Parquet position-delete file, read whole.
    Parquet,
}

fn plan_position_delete(
    spec: &IcebergDeleteFileSpec,
    data_file_path: &str,
) -> Result<PositionDeleteRead, String> {
    if let Some(referenced_data_file) = spec.referenced_data_file.as_deref()
        && referenced_data_file != data_file_path
    {
        return Err(format!(
            "iceberg position-delete file {} belongs to data file {referenced_data_file}, not {data_file_path}",
            spec.path
        ));
    }
    if spec.content_offset.is_some() || spec.content_size_in_bytes.is_some() {
        let offset = spec.content_offset.ok_or_else(|| {
            format!(
                "Puffin deletion vector {} missing content_offset",
                spec.path
            )
        })?;
        let size = spec.content_size_in_bytes.ok_or_else(|| {
            format!(
                "Puffin deletion vector {} missing content_size_in_bytes",
                spec.path
            )
        })?;
        let start = u64::try_from(offset)
            .map_err(|_| format!("Puffin deletion vector {} has negative offset", spec.path))?;
        let length = u64::try_from(size)
            .map_err(|_| format!("Puffin deletion vector {} size is too large", spec.path))?;
        return FileReadRange::bounded(start, length)
            .map(PositionDeleteRead::DeletionVector)
            .map_err(|error| error.to_string());
    }
    if spec.file_format != IcebergFileFormat::Parquet {
        return Err(format!(
            "iceberg position-delete file {} has unsupported format {:?}; only PARQUET is supported",
            spec.path, spec.file_format
        ));
    }
    Ok(PositionDeleteRead::Parquet)
}

fn position_delete_projection() -> FileProjection {
    FileProjection::RootNames(vec![FILE_PATH_COLUMN.to_string(), POS_COLUMN.to_string()])
}

fn apply_deletion_vector(
    spec: &IcebergDeleteFileSpec,
    payload: &[u8],
    deleted: &mut RoaringTreemap,
) -> Result<(), String> {
    let dv = DeletionVector::from_iceberg_payload(payload).map_err(|error| {
        format!(
            "decode Puffin deletion vector {} failed: {error}",
            spec.path
        )
    })?;
    *deleted |= dv.to_roaring_treemap();
    Ok(())
}

fn apply_position_delete_batches(
    spec: &IcebergDeleteFileSpec,
    data_file_path: &str,
    batches: Vec<novarocks_fs::FileBatch>,
    deleted: &mut RoaringTreemap,
) -> Result<(), String> {
    for batch in batches {
        let batch = batch.batch;
        let schema = batch.schema();
        let file_path_index = schema.index_of(FILE_PATH_COLUMN).map_err(|error| {
            format!(
                "projected batch from {} missing `{FILE_PATH_COLUMN}`: {error}",
                spec.path
            )
        })?;
        let pos_index = schema.index_of(POS_COLUMN).map_err(|error| {
            format!(
                "projected batch from {} missing `{POS_COLUMN}`: {error}",
                spec.path
            )
        })?;
        let file_paths = batch
            .column(file_path_index)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                format!(
                    "iceberg position-delete file {} column `{FILE_PATH_COLUMN}` is not STRING",
                    spec.path
                )
            })?;
        let positions = batch
            .column(pos_index)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| {
                format!(
                    "iceberg position-delete file {} column `{POS_COLUMN}` is not BIGINT",
                    spec.path
                )
            })?;
        for row in 0..batch.num_rows() {
            if file_paths.is_null(row)
                || positions.is_null(row)
                || file_paths.value(row) != data_file_path
            {
                continue;
            }
            let position = positions.value(row);
            if position < 0 {
                return Err(format!(
                    "iceberg position-delete file {} has negative pos {} for data file {data_file_path}",
                    spec.path, position
                ));
            }
            deleted.insert(position as u64);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use novarocks_fs::{
        FileCancellation, FileIoRuntime, FileTaskSpawner, FsAccessResolver, TokioFileIoRuntime,
        TokioFileTaskSpawner,
    };
    use parquet::arrow::ArrowWriter;

    use super::*;
    use crate::access_binding::IcebergReadBinding;

    #[test]
    fn context_loader_uses_provider_owned_file_resources() {
        let directory = tempfile::tempdir().expect("create temporary directory");
        let delete_path = directory.path().join("deletes.parquet");
        write_delete_parquet(
            &delete_path,
            &["/data/a.parquet", "/data/b.parquet", "/data/a.parquet"],
            &[2, 3, 5],
        );

        let runtime = tokio::runtime::Runtime::new().expect("build Tokio runtime");
        let file_runtime: Arc<dyn FileIoRuntime> =
            Arc::new(TokioFileIoRuntime::new(runtime.handle().clone()));
        let task_spawner: Arc<dyn FileTaskSpawner> =
            Arc::new(TokioFileTaskSpawner::new(runtime.handle().clone()));
        let binding = IcebergReadBinding::new(
            None,
            FsAccessResolver::new(),
            Arc::clone(&file_runtime),
            Arc::clone(&task_spawner),
        );
        let access = binding
            .resolve_access(&format!(
                "file://{}",
                directory.path().join("__binding__").display()
            ))
            .expect("resolve local access");
        let context = FileReadContext {
            cancellation: FileCancellation::new(),
            deadline: Some(Instant::now() + Duration::from_secs(1)),
            runtime: file_runtime,
            task_spawner,
            range: None,
        };
        let spec = IcebergDeleteFileSpec {
            path: delete_path
                .file_name()
                .expect("delete file name")
                .to_string_lossy()
                .to_string(),
            file_format: IcebergFileFormat::Parquet,
            file_content: IcebergFileContent::PositionDeletes,
            length: None,
            content_offset: None,
            content_size_in_bytes: None,
            referenced_data_file: Some("/data/a.parquet".to_string()),
        };

        let deleted = load_position_deletes_with_context(
            &[spec.clone()],
            "/data/a.parquet",
            &access,
            &context,
        )
        .expect("read position deletes");
        assert_eq!(deleted.iter().collect::<Vec<_>>(), vec![2, 5]);

        let foreign = IcebergDeleteFileSpec {
            referenced_data_file: Some("/data/b.parquet".to_string()),
            ..spec
        };
        let error =
            load_position_deletes_with_context(&[foreign], "/data/a.parquet", &access, &context)
                .expect_err("a delete file for another data file must fail before it is read");
        assert!(error.contains("belongs to data file /data/b.parquet"));
    }

    fn write_delete_parquet(path: &std::path::Path, file_paths: &[&str], positions: &[i64]) {
        let schema = Arc::new(Schema::new(vec![
            Field::new(FILE_PATH_COLUMN, DataType::Utf8, false),
            Field::new(POS_COLUMN, DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(file_paths.to_vec())),
                Arc::new(Int64Array::from(positions.to_vec())),
            ],
        )
        .expect("build delete batch");
        let file = fs::File::create(path).expect("create delete file");
        let mut writer = ArrowWriter::try_new(file, schema, None).expect("create parquet writer");
        writer.write(&batch).expect("write delete batch");
        writer.close().expect("close parquet writer");
    }
}
