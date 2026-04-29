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
#[allow(unused_imports)] // removed in Task 5 once scan_deletes lands
use arrow::array::{ArrayRef, BooleanArray};
#[allow(unused_imports)] // removed in Task 5 once scan_deletes lands
use arrow::compute::filter_record_batch;
#[allow(unused_imports)] // removed in Task 5 once scan_deletes lands
use arrow::record_batch::RecordBatch;
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use roaring::RoaringTreemap;

use crate::connector::iceberg::changes::{ChangeError, PositionDeleteRef};

/// Constants matching the iceberg position-delete file schema (file_path, pos).
// removed when scan_deletes lands in Task 5
#[allow(dead_code)]
const FILE_PATH_COLUMN: &str = "file_path";
// removed when scan_deletes lands in Task 5
#[allow(dead_code)]
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

// TODO PR-3 Task 4: implement read_data_file_at_positions
// TODO PR-3 Task 5: implement scan_deletes (top-level orchestrator)
