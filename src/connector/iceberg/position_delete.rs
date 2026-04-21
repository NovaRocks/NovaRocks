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
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use roaring::RoaringTreemap;

use crate::cache::CachedRangeReader;
use crate::descriptors::THdfsFileFormat;
use crate::formats::parquet::{ParquetCachedReader, ParquetReadCachePolicy};
use crate::fs::opendal::OpendalRangeReaderFactory;
use crate::plan_nodes::THdfsScanRange;
use crate::types::TIcebergFileContent;

/// The only two column names a position-delete Parquet file is allowed to
/// have (equality-delete files carry a different schema and are rejected in
/// lowering).
const FILE_PATH_COLUMN: &str = "file_path";
const POS_COLUMN: &str = "pos";

/// Rust-side view of a single `TIcebergDeleteFile` filtered down to the
/// fields we actually need for merge-on-read. Equality-delete files are
/// rejected during lowering, so this struct always refers to a
/// position-delete Parquet file.
#[derive(Clone, Debug)]
pub struct IcebergDeleteFileSpec {
    pub path: String,
    pub file_format: THdfsFileFormat,
    pub length: Option<u64>,
}

/// Convert the `THdfsScanRange.delete_files` list attached to a scan range
/// into [`IcebergDeleteFileSpec`]. Only POSITION_DELETES in PARQUET format
/// are accepted today; anything else is rejected with a descriptive error
/// that names the scan-node id so operators can trace the rejection back
/// to the originating fragment.
pub fn convert_scan_range_delete_files(
    scan_node_label: &str,
    hdfs_range: &THdfsScanRange,
) -> Result<Vec<IcebergDeleteFileSpec>, String> {
    let Some(delete_files) = hdfs_range.delete_files.as_ref() else {
        return Ok(Vec::new());
    };
    let mut out = Vec::with_capacity(delete_files.len());
    for del in delete_files {
        let file_content = del.file_content.ok_or_else(|| {
            format!("{scan_node_label} iceberg delete file is missing file_content")
        })?;
        match file_content {
            TIcebergFileContent::POSITION_DELETES => {}
            TIcebergFileContent::EQUALITY_DELETES => {
                return Err(format!(
                    "{scan_node_label} does not yet support iceberg equality-delete files; \
                     only POSITION_DELETES is supported on the reader side"
                ));
            }
            other => {
                return Err(format!(
                    "{scan_node_label} received unexpected iceberg delete file_content {other:?}"
                ));
            }
        }
        let path = del
            .full_path
            .as_ref()
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .ok_or_else(|| {
                format!("{scan_node_label} iceberg position-delete file has empty full_path")
            })?
            .to_string();
        // The FE sometimes omits `file_format` because Iceberg writers emit
        // delete files in Parquet by default. Fall back to PARQUET when the
        // field is absent; the subsequent read still validates the schema.
        let file_format = del.file_format.unwrap_or(THdfsFileFormat::PARQUET);
        if file_format != THdfsFileFormat::PARQUET {
            return Err(format!(
                "{scan_node_label} iceberg position-delete file {path} has unsupported format \
                 {file_format:?}; only PARQUET is supported"
            ));
        }
        let length = del
            .length
            .and_then(|v| if v > 0 { Some(v as u64) } else { None });
        out.push(IcebergDeleteFileSpec {
            path,
            file_format,
            length,
        });
    }
    Ok(out)
}

/// Load every position-delete Parquet file in `specs`, keep only the rows
/// whose `file_path` equals `data_file_path`, and collect the matching `pos`
/// values into a [`RoaringTreemap`]. Returns an empty set when no delete
/// row targets the data file.
pub fn load_position_deletes(
    specs: &[IcebergDeleteFileSpec],
    data_file_path: &str,
    factory: &OpendalRangeReaderFactory,
) -> Result<RoaringTreemap, String> {
    let mut deleted = RoaringTreemap::new();
    for spec in specs {
        accumulate_deletes_from_file(spec, data_file_path, factory, &mut deleted)?;
    }
    Ok(deleted)
}

fn accumulate_deletes_from_file(
    spec: &IcebergDeleteFileSpec,
    data_file_path: &str,
    factory: &OpendalRangeReaderFactory,
    deleted: &mut RoaringTreemap,
) -> Result<(), String> {
    if spec.file_format != THdfsFileFormat::PARQUET {
        return Err(format!(
            "iceberg position-delete file {} has unsupported format {:?}; only PARQUET is supported",
            spec.path, spec.file_format
        ));
    }

    let reader = factory
        .open_with_len(&spec.path, spec.length)
        .map_err(|e| {
            format!(
                "open iceberg position-delete file {} failed: {}",
                spec.path, e
            )
        })?;

    // Position-delete files are small enough that we just skip the parquet data
    // cache — every byte is consumed exactly once per scan.
    let reader = ParquetCachedReader::new(
        CachedRangeReader::new(reader, None),
        ParquetReadCachePolicy::with_flags(false, false, None),
    );

    let builder = ParquetRecordBatchReaderBuilder::try_new(reader).map_err(|e| {
        format!(
            "read iceberg position-delete file {} metadata failed: {}",
            spec.path, e
        )
    })?;

    // Resolve the two required columns by name against the Arrow schema the
    // parquet reader exposes; this avoids pulling in the parquet
    // `schema::types::Type` API just for name-to-index lookup.
    let arrow_schema = builder.schema();
    let file_path_field_idx = arrow_schema.index_of(FILE_PATH_COLUMN).map_err(|e| {
        format!(
            "iceberg position-delete file {} missing `{}`: {}",
            spec.path, FILE_PATH_COLUMN, e
        )
    })?;
    let pos_field_idx = arrow_schema.index_of(POS_COLUMN).map_err(|e| {
        format!(
            "iceberg position-delete file {} missing `{}`: {}",
            spec.path, POS_COLUMN, e
        )
    })?;

    // `ProjectionMask::leaves` takes leaf (physical) indices, which map 1:1 to
    // top-level fields for the `(string, bigint)` schema used by Iceberg
    // position-delete files.
    let projection = ProjectionMask::leaves(
        builder.parquet_schema(),
        [file_path_field_idx, pos_field_idx].iter().copied(),
    );

    let reader = builder.with_projection(projection).build().map_err(|e| {
        format!(
            "build iceberg position-delete reader for {} failed: {}",
            spec.path, e
        )
    })?;

    for batch_result in reader {
        let batch: arrow::record_batch::RecordBatch = batch_result.map_err(|e| {
            format!(
                "read iceberg position-delete file {} batch failed: {}",
                spec.path, e
            )
        })?;
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

    use crate::fs::opendal::build_fs_operator;

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

    fn factory_for_dir(dir: &std::path::Path) -> OpendalRangeReaderFactory {
        let op = build_fs_operator(dir.to_str().expect("utf8 dir")).expect("operator");
        OpendalRangeReaderFactory::from_operator(op).expect("factory")
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
            file_format: THdfsFileFormat::PARQUET,
            length: None,
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
            file_format: THdfsFileFormat::PARQUET,
            length: None,
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
                file_format: THdfsFileFormat::PARQUET,
                length: None,
            },
            IcebergDeleteFileSpec {
                path: del_b.file_name().unwrap().to_string_lossy().to_string(),
                file_format: THdfsFileFormat::PARQUET,
                length: None,
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
            file_format: THdfsFileFormat::ORC,
            length: None,
        };
        let factory = factory_for_dir(&dir);
        let err = load_position_deletes(&[spec], "/foo", &factory).unwrap_err();
        assert!(err.contains("only PARQUET"), "error was: {err}");
    }
}
