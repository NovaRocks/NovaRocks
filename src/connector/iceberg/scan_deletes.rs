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

//! Position-delete reverse projection for IVM Phase 2.
//!
//! Reads `PositionDeleteRef`s produced by `plan_changes` and, for each
//! deleted `(data_file, pos)` pair, projects the *original* base row
//! out of the source data file. The output is a `Vec<RecordBatch>` of
//! the deleted rows in the base table's full schema, ready for WHERE
//! re-application (which `materialize_changes` does in SQL by
//! registering these as a temp parquet table and running the MV's
//! SELECT).
//!
//! This is the inverse of `iceberg::position_delete`'s scan-time
//! filtering: that module *removes* deleted rows from a scan; we keep
//! only the deleted rows.

use std::collections::HashMap;

use arrow::array::Array;
use arrow::array::BooleanArray;
use arrow::compute::filter_record_batch;
use arrow::record_batch::RecordBatch;
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use roaring::RoaringTreemap;

use crate::connector::iceberg::changes::{ChangeError, PositionDeleteRef};

/// Constants matching the iceberg position-delete file schema (file_path, pos).
const FILE_PATH_COLUMN: &str = "file_path";
const POS_COLUMN: &str = "pos";

/// Strip the `file://` URL scheme so the path can be passed to a local-FS
/// opendal operator (which expects bare filesystem paths). Other schemes
/// (s3://, hdfs://, …) are returned unchanged because PR-3 only supports
/// the local-FS path; cloud handling is PR-4.
fn normalize_local_fs_path(path: &str) -> &str {
    path.strip_prefix("file://").unwrap_or(path)
}

// TODO(ivm-phase-2 follow-up): every failure path here funnels into
// ChangeError::InternalInconsistency, but operationally several classes
// of failure (I/O errors, corrupt delete-file schema, negative pos)
// are *external* — not invariants of NovaRocks. Re-classify into
// distinct ChangeError variants (e.g. DeleteFileIoError /
// DeleteFileSchemaInvalid) once the orchestrator (Task 5+) provides
// caller context to disambiguate.

/// Read every position-delete file in `delete_files` and return, per
/// referenced data file, the set of positions deleted by those files.
///
/// Equivalent to `iceberg::position_delete::load_position_deletes` run
/// once per distinct `data_file_path`, but reads each delete file only
/// once.
pub(crate) fn read_delete_positions_per_data_file(
    delete_files: &[PositionDeleteRef],
    factory: &crate::fs::opendal::OpendalRangeReaderFactory,
) -> Result<HashMap<String, RoaringTreemap>, ChangeError> {
    use crate::cache::CachedRangeReader;
    use crate::formats::parquet::{ParquetCachedReader, ParquetReadCachePolicy};
    use arrow::array::{Int64Array, StringArray};

    let mut positions_per_file: HashMap<String, RoaringTreemap> = HashMap::new();

    for delete_file in delete_files {
        let length = if delete_file.delete_file_size > 0 {
            Some(delete_file.delete_file_size as u64)
        } else {
            None
        };
        let reader = factory
            .open_with_len(
                normalize_local_fs_path(&delete_file.delete_file_path),
                length,
            )
            .map_err(|e| {
                ChangeError::InternalInconsistency(format!(
                    "open iceberg position-delete file {} failed: {e}",
                    delete_file.delete_file_path
                ))
            })?;
        let reader = ParquetCachedReader::new(
            CachedRangeReader::new(reader, None),
            ParquetReadCachePolicy::with_flags(false, false, None),
        );
        let builder = ParquetRecordBatchReaderBuilder::try_new(reader).map_err(|e| {
            ChangeError::InternalInconsistency(format!(
                "read position-delete file {} metadata failed: {e}",
                delete_file.delete_file_path
            ))
        })?;
        let arrow_schema = builder.schema();
        let file_path_idx = arrow_schema.index_of(FILE_PATH_COLUMN).map_err(|e| {
            ChangeError::InternalInconsistency(format!(
                "position-delete file {} missing `{}`: {e}",
                delete_file.delete_file_path, FILE_PATH_COLUMN
            ))
        })?;
        let pos_idx = arrow_schema.index_of(POS_COLUMN).map_err(|e| {
            ChangeError::InternalInconsistency(format!(
                "position-delete file {} missing `{}`: {e}",
                delete_file.delete_file_path, POS_COLUMN
            ))
        })?;
        let projection = ProjectionMask::leaves(
            builder.parquet_schema(),
            [file_path_idx, pos_idx].iter().copied(),
        );
        let reader = builder.with_projection(projection).build().map_err(|e| {
            ChangeError::InternalInconsistency(format!(
                "build position-delete reader for {} failed: {e}",
                delete_file.delete_file_path
            ))
        })?;
        for batch_result in reader {
            let batch = batch_result.map_err(|e| {
                ChangeError::InternalInconsistency(format!(
                    "read position-delete file {} batch failed: {e}",
                    delete_file.delete_file_path
                ))
            })?;
            let batch_schema = batch.schema();
            let fp_idx = batch_schema.index_of(FILE_PATH_COLUMN).map_err(|e| {
                ChangeError::InternalInconsistency(format!(
                    "projected position-delete batch from {} missing `{}`: {e}",
                    delete_file.delete_file_path, FILE_PATH_COLUMN
                ))
            })?;
            let pos_idx_in_batch = batch_schema.index_of(POS_COLUMN).map_err(|e| {
                ChangeError::InternalInconsistency(format!(
                    "projected position-delete batch from {} missing `{}`: {e}",
                    delete_file.delete_file_path, POS_COLUMN
                ))
            })?;
            let fp_array = batch
                .column(fp_idx)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    ChangeError::InternalInconsistency(format!(
                        "position-delete file {} column `{}` is not STRING",
                        delete_file.delete_file_path, FILE_PATH_COLUMN
                    ))
                })?;
            let pos_array = batch
                .column(pos_idx_in_batch)
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| {
                    ChangeError::InternalInconsistency(format!(
                        "position-delete file {} column `{}` is not BIGINT",
                        delete_file.delete_file_path, POS_COLUMN
                    ))
                })?;
            for row in 0..batch.num_rows() {
                if fp_array.is_null(row) || pos_array.is_null(row) {
                    continue;
                }
                let pos = pos_array.value(row);
                if pos < 0 {
                    return Err(ChangeError::InternalInconsistency(format!(
                        "position-delete file {} has negative pos {} for data file {}",
                        delete_file.delete_file_path,
                        pos,
                        fp_array.value(row)
                    )));
                }
                let entry = positions_per_file
                    .entry(fp_array.value(row).to_string())
                    .or_default();
                entry.insert(pos as u64);
            }
        }
    }

    Ok(positions_per_file)
}

/// Open a single data file and project the rows at the positions
/// listed in `positions`. Returns one `RecordBatch` per parquet
/// `RecordBatch` boundary that contained at least one matching row.
/// Empty if the file has no matching rows (which would be a bug;
/// `read_delete_positions_per_data_file` only emits keys for files that
/// actually had deletions, but defensive empty-handling avoids surprise).
///
/// `data_file_path` is in iceberg's path format (e.g. `file:///...` or
/// `s3://...`). The `factory` knows how to dispatch.
pub(crate) fn read_data_file_at_positions(
    data_file_path: &str,
    data_file_size: Option<u64>,
    positions: &RoaringTreemap,
    factory: &crate::fs::opendal::OpendalRangeReaderFactory,
) -> Result<Vec<RecordBatch>, ChangeError> {
    use crate::cache::CachedRangeReader;
    use crate::formats::parquet::{ParquetCachedReader, ParquetReadCachePolicy};

    if positions.is_empty() {
        return Ok(Vec::new());
    }

    let reader = factory
        .open_with_len(normalize_local_fs_path(data_file_path), data_file_size)
        .map_err(|e| {
            ChangeError::InternalInconsistency(format!(
                "open iceberg data file {data_file_path} for delete reverse projection: {e}"
            ))
        })?;
    let reader = ParquetCachedReader::new(
        CachedRangeReader::new(reader, None),
        ParquetReadCachePolicy::with_flags(false, false, None),
    );
    let builder = ParquetRecordBatchReaderBuilder::try_new(reader).map_err(|e| {
        ChangeError::InternalInconsistency(format!(
            "read iceberg data file {data_file_path} metadata for delete reverse projection: {e}"
        ))
    })?;
    let reader = builder.build().map_err(|e| {
        ChangeError::InternalInconsistency(format!(
            "build parquet reader for {data_file_path}: {e}"
        ))
    })?;

    let mut out: Vec<RecordBatch> = Vec::new();
    let mut row_offset: u64 = 0;
    for batch_result in reader {
        let batch = batch_result.map_err(|e| {
            ChangeError::InternalInconsistency(format!(
                "read iceberg data file {data_file_path} batch for delete reverse projection: {e}"
            ))
        })?;
        let n = batch.num_rows() as u64;
        if n == 0 {
            continue;
        }
        let mut mask = Vec::with_capacity(batch.num_rows());
        let mut any_kept = false;
        for local in 0..n {
            let global = row_offset + local;
            let keep = positions.contains(global);
            mask.push(keep);
            if keep {
                any_kept = true;
            }
        }
        if any_kept {
            let mask_array = BooleanArray::from(mask);
            let projected = filter_record_batch(&batch, &mask_array).map_err(|e| {
                ChangeError::InternalInconsistency(format!("filter rows in {data_file_path}: {e}"))
            })?;
            out.push(projected);
        }
        row_offset += n;
    }

    Ok(out)
}

/// Top-level: take a slice of `PositionDeleteRef`s and produce
/// `Vec<RecordBatch>` containing the original deleted base rows in the
/// data files' full schema (no projection / no WHERE applied — those
/// are SQL-level concerns layered on top of this function).
///
/// `data_file_size_lookup` returns the on-disk size in bytes for a given
/// `data_file_path`. iceberg-rust's `DataFile::file_size_in_bytes` is
/// the canonical source. Caller must provide a closure since iceberg
/// table state isn't carried into this module to keep the dependency
/// graph minimal.
pub(crate) fn scan_deletes<F>(
    delete_files: &[PositionDeleteRef],
    factory: &crate::fs::opendal::OpendalRangeReaderFactory,
    data_file_size_lookup: F,
) -> Result<Vec<RecordBatch>, ChangeError>
where
    F: Fn(&str) -> Option<u64>,
{
    if delete_files.is_empty() {
        return Ok(Vec::new());
    }

    let positions_per_file = read_delete_positions_per_data_file(delete_files, factory)?;
    let mut out: Vec<RecordBatch> = Vec::new();
    // Sort keys for deterministic output ordering — useful for tests
    // and downstream equality assertions.
    let mut data_file_paths: Vec<&String> = positions_per_file.keys().collect();
    data_file_paths.sort();
    for data_file_path in data_file_paths {
        let positions = &positions_per_file[data_file_path];
        let size = data_file_size_lookup(data_file_path);
        let batches = read_data_file_at_positions(data_file_path, size, positions, factory)?;
        out.extend(batches);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::sync::Arc;

    use arrow::array::{Int32Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;
    use roaring::RoaringTreemap;

    use super::{
        FILE_PATH_COLUMN, POS_COLUMN, read_data_file_at_positions,
        read_delete_positions_per_data_file, scan_deletes,
    };
    use crate::connector::iceberg::changes::PositionDeleteRef;
    use crate::fs::opendal::{OpendalRangeReaderFactory, build_fs_operator};

    fn write_data_parquet(path: &std::path::Path, ids: &[i32], names: &[&str]) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let id_array = Int32Array::from(ids.to_vec());
        let name_array = StringArray::from(names.to_vec());
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(id_array), Arc::new(name_array)],
        )
        .expect("data batch");
        let file = fs::File::create(path).expect("create data parquet");
        let mut writer = ArrowWriter::try_new(file, schema, None).expect("data writer");
        writer.write(&batch).expect("write data batch");
        writer.close().expect("close data writer");
    }

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
        .expect("delete batch");
        let file = fs::File::create(path).expect("create delete parquet");
        let mut writer = ArrowWriter::try_new(file, schema, None).expect("delete writer");
        writer.write(&batch).expect("write delete batch");
        writer.close().expect("close delete writer");
    }

    /// Build a factory rooted at `dir`; relative paths inside the parquet
    /// fixtures are resolved against this root, mirroring
    /// `position_delete::tests::factory_for_dir`.
    fn factory_for_dir(dir: &std::path::Path) -> OpendalRangeReaderFactory {
        let op = build_fs_operator(dir.to_str().expect("utf8 dir")).expect("fs operator");
        OpendalRangeReaderFactory::from_operator(op).expect("factory")
    }

    #[test]
    fn read_delete_positions_groups_by_file_path() {
        let dir = tempfile::tempdir().expect("tempdir");
        let delete_path = dir.path().join("deletes.parquet");
        write_delete_parquet(
            &delete_path,
            &["data1.parquet", "data1.parquet", "data2.parquet"],
            &[0, 2, 5],
        );
        let refs = vec![PositionDeleteRef {
            delete_file_path: "deletes.parquet".to_string(),
            delete_file_size: 0,
            record_count: Some(3),
            referenced_data_file: None,
        }];
        let map =
            read_delete_positions_per_data_file(&refs, &factory_for_dir(dir.path())).expect("ok");
        assert_eq!(map.len(), 2);
        let one = &map["data1.parquet"];
        assert_eq!(one.len(), 2);
        assert!(one.contains(0) && one.contains(2));
        let two = &map["data2.parquet"];
        assert_eq!(two.len(), 1);
        assert!(two.contains(5));
    }

    #[test]
    fn read_data_file_at_positions_keeps_only_listed_rows() {
        let dir = tempfile::tempdir().expect("tempdir");
        let data_path = dir.path().join("data.parquet");
        write_data_parquet(&data_path, &[10, 20, 30, 40], &["a", "b", "c", "d"]);
        let mut positions = RoaringTreemap::new();
        positions.insert(1);
        positions.insert(3);
        let batches = read_data_file_at_positions(
            "data.parquet",
            None,
            &positions,
            &factory_for_dir(dir.path()),
        )
        .expect("ok");
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 2);
        let id = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("id col");
        let name = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("name col");
        assert_eq!(id.value(0), 20);
        assert_eq!(id.value(1), 40);
        assert_eq!(name.value(0), "b");
        assert_eq!(name.value(1), "d");
    }

    #[test]
    fn scan_deletes_returns_empty_for_empty_input() {
        let dir = tempfile::tempdir().expect("tempdir");
        let batches = scan_deletes(&[], &factory_for_dir(dir.path()), |_| None).expect("ok");
        assert!(batches.is_empty());
    }

    #[test]
    fn scan_deletes_projects_rows_for_single_data_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        let data_path = dir.path().join("data.parquet");
        write_data_parquet(&data_path, &[10, 20, 30, 40], &["a", "b", "c", "d"]);
        let delete_path = dir.path().join("deletes.parquet");
        let data_uri = "data.parquet";
        write_delete_parquet(&delete_path, &[data_uri, data_uri], &[1, 3]);
        let refs = vec![PositionDeleteRef {
            delete_file_path: "deletes.parquet".to_string(),
            delete_file_size: 0,
            record_count: Some(2),
            referenced_data_file: Some(data_uri.to_string()),
        }];
        let batches = scan_deletes(&refs, &factory_for_dir(dir.path()), |_| None).expect("ok");
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 2);
    }

    #[test]
    fn scan_deletes_projects_across_multiple_data_files() {
        let dir = tempfile::tempdir().expect("tempdir");
        let data1_path = dir.path().join("data1.parquet");
        write_data_parquet(&data1_path, &[1, 2, 3], &["x", "y", "z"]);
        let data2_path = dir.path().join("data2.parquet");
        write_data_parquet(&data2_path, &[100, 200], &["p", "q"]);
        let delete_path = dir.path().join("deletes.parquet");
        let d1 = "data1.parquet";
        let d2 = "data2.parquet";
        write_delete_parquet(&delete_path, &[d1, d2], &[0, 1]);
        let refs = vec![PositionDeleteRef {
            delete_file_path: "deletes.parquet".to_string(),
            delete_file_size: 0,
            record_count: Some(2),
            referenced_data_file: None,
        }];
        let batches = scan_deletes(&refs, &factory_for_dir(dir.path()), |_| None).expect("ok");
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        // 1 row from data1 (id=1) + 1 row from data2 (id=200) = 2 rows total.
        assert_eq!(total, 2);
    }
}
