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

//! Iceberg v2 position-delete support used by the HDFS scan runner's
//! merge-on-read path. A position-delete file is a Parquet file with two
//! required columns:
//!
//! * `file_path: string` — the data-file path that each delete row targets
//! * `pos: bigint` — the 0-based row ordinal within that data file
//!
//! The loader opens every delete file attached to a scan range, filters rows
//! whose `file_path` matches the current data-file path, and collects the
//! matching `pos` values into a [`RoaringTreemap`]. The caller then consults
//! that set to drop deleted rows from each scanned chunk.

use arrow::array::{Array, Int64Array, StringArray};
use roaring::RoaringTreemap;

use crate::connector::file_execution::{read_foundation_bytes, read_foundation_parquet_batches};
use crate::connector::iceberg::delete_file::{
    IcebergDeleteFileSpec, IcebergFileContent, IcebergFileFormat,
};
use novarocks_fs::FileReadContext;
use novarocks_fs::{FileProjection, FileReadRange, FsAccessHandle};

/// The only two column names a position-delete Parquet file is allowed to
/// have (equality-delete files carry a different schema and are rejected in
/// lowering).
pub(crate) const FILE_PATH_COLUMN: &str = "file_path";
pub(crate) const POS_COLUMN: &str = "pos";

/// Load every position-delete Parquet file in `specs`, keep only the rows
/// whose `file_path` equals `data_file_path`, and collect the matching `pos`
/// values into a [`RoaringTreemap`]. Returns an empty set when no delete
/// row targets the data file.
pub fn load_position_deletes(
    specs: &[IcebergDeleteFileSpec],
    data_file_path: &str,
    access: &FsAccessHandle,
) -> Result<RoaringTreemap, String> {
    let mut deleted = RoaringTreemap::new();
    for spec in specs {
        if spec.file_content != IcebergFileContent::PositionDeletes {
            continue;
        }
        accumulate_deletes_from_file(spec, data_file_path, access, &mut deleted)?;
    }
    Ok(deleted)
}

/// Provider-reader variant that carries the same cancellation, deadline, and
/// runtime binding through delete and deletion-vector I/O as data-file I/O.
pub(crate) fn load_position_deletes_with_context(
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
        accumulate_deletes_from_file_with_context(
            spec,
            data_file_path,
            access,
            context,
            &mut deleted,
        )?;
    }
    Ok(deleted)
}

fn accumulate_deletes_from_file_with_context(
    spec: &IcebergDeleteFileSpec,
    data_file_path: &str,
    access: &FsAccessHandle,
    context: &FileReadContext,
    deleted: &mut RoaringTreemap,
) -> Result<(), String> {
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
        let payload = crate::connector::iceberg::file_reader::read_bytes(
            access,
            &spec.path,
            spec.length,
            FileReadRange::bounded(start, length).map_err(|error| error.to_string())?,
            context,
        )?;
        let dv = crate::connector::iceberg::commit::DeletionVector::from_iceberg_payload(
            payload.as_ref(),
        )
        .map_err(|error| {
            format!(
                "decode Puffin deletion vector {} failed: {error}",
                spec.path
            )
        })?;
        let _ = data_file_path;
        *deleted |= dv.to_roaring_treemap();
        return Ok(());
    }
    if spec.file_format != IcebergFileFormat::Parquet {
        return Err(format!(
            "iceberg position-delete file {} has unsupported format {:?}; only PARQUET is supported",
            spec.path, spec.file_format
        ));
    }
    for batch in crate::connector::iceberg::file_reader::read_parquet_batches(
        access,
        &spec.path,
        spec.length,
        FileProjection::RootNames(vec![FILE_PATH_COLUMN.to_string(), POS_COLUMN.to_string()]),
        context.clone(),
    )? {
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
                || !paths_match(file_paths.value(row), data_file_path)
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

fn accumulate_deletes_from_file(
    spec: &IcebergDeleteFileSpec,
    data_file_path: &str,
    access: &FsAccessHandle,
    deleted: &mut RoaringTreemap,
) -> Result<(), String> {
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
        let payload = read_foundation_bytes(
            access,
            &spec.path,
            spec.length,
            FileReadRange::bounded(start, length).map_err(|error| error.to_string())?,
        )
        .map_err(|e| {
            format!(
                "read Puffin deletion vector {} at {}+{} failed: {}",
                spec.path, offset, size, e
            )
        })?;
        let dv = crate::connector::iceberg::commit::DeletionVector::from_iceberg_payload(
            payload.as_ref(),
        )
        .map_err(|e| format!("decode Puffin deletion vector {} failed: {e}", spec.path))?;
        let _ = data_file_path;
        *deleted |= dv.to_roaring_treemap();
        return Ok(());
    }
    if spec.file_format != IcebergFileFormat::Parquet {
        return Err(format!(
            "iceberg position-delete file {} has unsupported format {:?}; only PARQUET is supported",
            spec.path, spec.file_format
        ));
    }

    let batches = read_foundation_parquet_batches(
        access,
        &spec.path,
        spec.length,
        FileProjection::RootNames(vec![FILE_PATH_COLUMN.to_string(), POS_COLUMN.to_string()]),
    )?;

    for batch in batches {
        // After projection the two columns retain their original schema
        // order, so we resolve them by name against the projected batch schema
        // (which is what `index_of` sees).
        let batch_schema = batch.schema();
        let fp_pos_in_batch = batch_schema.index_of(FILE_PATH_COLUMN).map_err(|e| {
            format!(
                "projected batch from {} missing `{}`: {}",
                spec.path, FILE_PATH_COLUMN, e
            )
        })?;
        let pos_pos_in_batch = batch_schema.index_of(POS_COLUMN).map_err(|e| {
            format!(
                "projected batch from {} missing `{}`: {}",
                spec.path, POS_COLUMN, e
            )
        })?;

        let fp_array = batch
            .column(fp_pos_in_batch)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                format!(
                    "iceberg position-delete file {} column `{}` is not STRING",
                    spec.path, FILE_PATH_COLUMN
                )
            })?;
        let pos_array = batch
            .column(pos_pos_in_batch)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| {
                format!(
                    "iceberg position-delete file {} column `{}` is not BIGINT",
                    spec.path, POS_COLUMN
                )
            })?;

        for row in 0..batch.num_rows() {
            if fp_array.is_null(row) || pos_array.is_null(row) {
                continue;
            }
            let row_file_path = fp_array.value(row);
            if !paths_match(row_file_path, data_file_path) {
                continue;
            }
            let pos = pos_array.value(row);
            if pos < 0 {
                return Err(format!(
                    "iceberg position-delete file {} has negative pos {} for data file {}",
                    spec.path, pos, data_file_path
                ));
            }
            deleted.insert(pos as u64);
        }
    }

    Ok(())
}

/// Iceberg records delete `file_path` values verbatim; it is the writer's
/// responsibility to keep them consistent with the data-file paths recorded
/// in the manifest. We match byte-for-byte here to mirror Iceberg spec and
/// the reference StarRocks reader; any path normalization is expected to
/// happen before the delete file is written.
fn paths_match(candidate: &str, target: &str) -> bool {
    candidate == target
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::fs;
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;

    fn write_delete_parquet(path: &std::path::Path, file_paths: &[&str], positions: &[i64]) {
        let schema = Arc::new(Schema::new(vec![
            Field::new(FILE_PATH_COLUMN, DataType::Utf8, false),
            Field::new(POS_COLUMN, DataType::Int64, false),
        ]));
        let fp_array = StringArray::from(file_paths.to_vec());
        let pos_array = Int64Array::from(positions.to_vec());
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(fp_array), Arc::new(pos_array)],
        )
        .expect("record batch");
        let file = fs::File::create(path).expect("create");
        let mut writer = ArrowWriter::try_new(file, schema, None).expect("writer");
        writer.write(&batch).expect("write");
        writer.close().expect("close");
    }

    fn temp_dir_for(name: &str) -> std::path::PathBuf {
        let mut dir = std::env::temp_dir();
        dir.push(format!(
            "novarocks_position_delete_tests_{}_{}",
            name,
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).expect("create tmp dir");
        dir
    }

    fn factory_for_dir(dir: &std::path::Path) -> novarocks_fs::FsAccessHandle {
        novarocks_fs::FsAccessResolver::new()
            .resolve_location(dir.join("__binding__").to_string_lossy(), None)
            .expect("access")
    }

    #[test]
    fn collects_positions_for_matching_file() {
        let dir = temp_dir_for("collects");
        let del = dir.join("deletes.parquet");
        write_delete_parquet(
            &del,
            &[
                "/data/file_a.parquet",
                "/data/file_a.parquet",
                "/data/file_b.parquet",
                "/data/file_a.parquet",
            ],
            &[2, 5, 7, 10],
        );

        let spec = IcebergDeleteFileSpec {
            path: del.file_name().unwrap().to_string_lossy().to_string(),
            file_format: IcebergFileFormat::Parquet,
            file_content: IcebergFileContent::PositionDeletes,
            length: None,
            content_offset: None,
            content_size_in_bytes: None,
        };
        let factory = factory_for_dir(&dir);
        let deleted =
            load_position_deletes(&[spec], "/data/file_a.parquet", &factory).expect("load");
        assert_eq!(deleted.iter().collect::<Vec<_>>(), vec![2, 5, 10]);
    }

    #[test]
    fn ignores_rows_for_other_files() {
        let dir = temp_dir_for("ignores");
        let del = dir.join("deletes.parquet");
        write_delete_parquet(&del, &["/x.parquet", "/y.parquet"], &[1, 2]);

        let spec = IcebergDeleteFileSpec {
            path: del.file_name().unwrap().to_string_lossy().to_string(),
            file_format: IcebergFileFormat::Parquet,
            file_content: IcebergFileContent::PositionDeletes,
            length: None,
            content_offset: None,
            content_size_in_bytes: None,
        };
        let factory = factory_for_dir(&dir);
        let deleted = load_position_deletes(&[spec], "/unrelated.parquet", &factory).expect("load");
        assert!(deleted.is_empty());
    }

    #[test]
    fn merges_multiple_delete_files() {
        let dir = temp_dir_for("merges");
        let del_a = dir.join("del_a.parquet");
        let del_b = dir.join("del_b.parquet");
        write_delete_parquet(&del_a, &["/t.parquet", "/t.parquet"], &[1, 3]);
        write_delete_parquet(&del_b, &["/t.parquet"], &[2]);

        let specs = vec![
            IcebergDeleteFileSpec {
                path: del_a.file_name().unwrap().to_string_lossy().to_string(),
                file_format: IcebergFileFormat::Parquet,
                file_content: IcebergFileContent::PositionDeletes,
                length: None,
                content_offset: None,
                content_size_in_bytes: None,
            },
            IcebergDeleteFileSpec {
                path: del_b.file_name().unwrap().to_string_lossy().to_string(),
                file_format: IcebergFileFormat::Parquet,
                file_content: IcebergFileContent::PositionDeletes,
                length: None,
                content_offset: None,
                content_size_in_bytes: None,
            },
        ];
        let factory = factory_for_dir(&dir);
        let deleted = load_position_deletes(&specs, "/t.parquet", &factory).expect("load");
        assert_eq!(deleted.iter().collect::<Vec<_>>(), vec![1, 2, 3]);
    }

    #[test]
    fn rejects_non_parquet_format() {
        let dir = temp_dir_for("rejects");
        let spec = IcebergDeleteFileSpec {
            path: "irrelevant".to_string(),
            file_format: IcebergFileFormat::Unknown,
            file_content: IcebergFileContent::PositionDeletes,
            length: None,
            content_offset: None,
            content_size_in_bytes: None,
        };
        let factory = factory_for_dir(&dir);
        let err = load_position_deletes(&[spec], "/foo", &factory).unwrap_err();
        assert!(err.contains("only PARQUET"), "error was: {err}");
    }
}
