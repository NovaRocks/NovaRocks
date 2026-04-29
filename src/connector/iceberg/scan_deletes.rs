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
// removed when scan_deletes lands in Task 5
#[allow(dead_code)]
pub(crate) fn read_delete_positions_per_data_file(
    delete_files: &[PositionDeleteRef],
    factory: &crate::fs::opendal::OpendalRangeReaderFactory,
) -> Result<HashMap<String, RoaringTreemap>, ChangeError> {
    use arrow::array::{Int64Array, StringArray};
    use crate::cache::CachedRangeReader;
    use crate::formats::parquet::{ParquetCachedReader, ParquetReadCachePolicy};

    let mut positions_per_file: HashMap<String, RoaringTreemap> = HashMap::new();

    for delete_file in delete_files {
        let length = if delete_file.delete_file_size > 0 {
            Some(delete_file.delete_file_size as u64)
        } else {
            None
        };
        let reader = factory
            .open_with_len(&delete_file.delete_file_path, length)
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
// removed when scan_deletes lands in Task 5
#[allow(dead_code)]
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
        .open_with_len(data_file_path, data_file_size)
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
                ChangeError::InternalInconsistency(format!(
                    "filter rows in {data_file_path}: {e}"
                ))
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
// removed when materialize_changes lands in Task 7
#[allow(dead_code)]
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
