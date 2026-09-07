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

//! Provider-owned physical Iceberg writer I/O.

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::fs;
use std::io::Cursor;
use std::path::PathBuf;

use arrow::array::{Array, RecordBatch, StringArray};
use arrow::datatypes::SchemaRef;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::data_type::AsBytes;
use parquet::file::metadata::ParquetMetaData;
use parquet::file::properties::WriterProperties;
use parquet::file::statistics::{Statistics, ValueStatistics};

use crate::access_binding::IcebergReadBinding;
use crate::commit::report::IcebergColumnStats;

/// Parquet facts returned to the provider's writer report adapter.
pub struct ParquetWriteResult {
    pub file_size: u64,
    pub split_offsets: Option<Vec<i64>>,
    pub column_stats: Option<IcebergColumnStats>,
}

/// Return the sole referenced data file when every position-delete row names
/// the same file; empty or mixed batches have no single reference.
pub fn unique_file_path(batch: &RecordBatch) -> Result<Option<String>, String> {
    if batch.num_rows() == 0 {
        return Ok(None);
    }
    let file_path_col = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| "position-delete batch missing file_path Utf8 column".to_string())?;
    let first = file_path_col.value(0);
    for row in 1..batch.num_rows() {
        if file_path_col.value(row) != first {
            return Ok(None);
        }
    }
    Ok(Some(first.to_string()))
}

/// Build a FileIO from one request-scoped storage binding.
pub fn build_staged_file_io(
    binding: &IcebergReadBinding,
    data_location: &str,
) -> Result<crate::iceberg::io::FileIO, String> {
    binding
        .resolve_access(data_location)
        .map_err(|error| format!("resolve Iceberg staged output {data_location}: {error}"))?;
    Ok(crate::fs_io::build_file_io_for_location(
        data_location,
        binding.clone(),
    ))
}

/// Encode and persist one Parquet file through one request-scoped storage binding.
pub async fn write_parquet_file(
    binding: &IcebergReadBinding,
    path: &str,
    schema: SchemaRef,
    batch: &RecordBatch,
    compression: Compression,
) -> Result<ParquetWriteResult, String> {
    let props = WriterProperties::builder()
        .set_compression(compression)
        .build();
    if binding.is_object_store_location(path)? {
        let (data, write_result) = write_parquet_to_bytes(schema, batch, props)?;
        let access = binding
            .resolve_access(path)
            .map_err(|error| format!("resolve Iceberg parquet output {path}: {error}"))?;
        let relative_paths = access.operator_relative_paths();
        let [relative_path] = relative_paths.as_slice() else {
            return Err(format!(
                "resolve Iceberg parquet output path {path}: expected exactly one path"
            ));
        };
        access
            .operator()
            .write(relative_path, data)
            .await
            .map_err(|error| format!("opendal write failed: {error}"))?;
        return Ok(write_result);
    }

    let local_path = normalize_path(path)?;
    let path_buf = PathBuf::from(&local_path);
    if let Some(parent) = path_buf.parent() {
        fs::create_dir_all(parent)
            .map_err(|error| format!("create parquet dir failed: {error}"))?;
    }
    let file = fs::File::create(&path_buf)
        .map_err(|error| format!("create parquet file failed: {error}"))?;
    let mut writer = ArrowWriter::try_new(file, schema, Some(props))
        .map_err(|error| format!("create parquet writer failed: {error}"))?;
    writer
        .write(batch)
        .map_err(|error| format!("write parquet batch failed: {error}"))?;
    let parquet_metadata = writer
        .close()
        .map_err(|error| format!("close parquet writer failed: {error}"))?;
    let metadata =
        fs::metadata(&path_buf).map_err(|error| format!("stat parquet file failed: {error}"))?;
    build_parquet_write_result(metadata.len(), &parquet_metadata)
}

fn normalize_path(path: &str) -> Result<String, String> {
    if path.starts_with("file:") {
        let url = url::Url::parse(path).map_err(|error| format!("invalid file url: {error}"))?;
        let path = url
            .to_file_path()
            .map_err(|_| "file url is not a valid local path".to_string())?;
        return Ok(path.to_string_lossy().to_string());
    }
    Ok(path.to_string())
}

fn write_parquet_to_bytes(
    schema: SchemaRef,
    batch: &RecordBatch,
    props: WriterProperties,
) -> Result<(Vec<u8>, ParquetWriteResult), String> {
    let mut buffer = Vec::new();
    let parquet_metadata;
    {
        let cursor = Cursor::new(&mut buffer);
        let mut writer = ArrowWriter::try_new(cursor, schema, Some(props))
            .map_err(|error| format!("create parquet writer failed: {error}"))?;
        writer
            .write(batch)
            .map_err(|error| format!("write parquet batch failed: {error}"))?;
        parquet_metadata = writer
            .close()
            .map_err(|error| format!("close parquet writer failed: {error}"))?;
    }
    let write_result = build_parquet_write_result(buffer.len() as u64, &parquet_metadata)?;
    Ok((buffer, write_result))
}

fn build_parquet_write_result(
    file_size: u64,
    metadata: &ParquetMetaData,
) -> Result<ParquetWriteResult, String> {
    Ok(ParquetWriteResult {
        file_size,
        split_offsets: collect_split_offsets(metadata),
        column_stats: collect_iceberg_column_stats(metadata),
    })
}

fn collect_split_offsets(metadata: &ParquetMetaData) -> Option<Vec<i64>> {
    let mut offsets = Vec::with_capacity(metadata.row_groups().len());
    for row_group in metadata.row_groups() {
        if row_group.num_columns() == 0 {
            continue;
        }
        let first_column = row_group.column(0);
        let data_page_offset = first_column.data_page_offset();
        let split_offset = match first_column.dictionary_page_offset() {
            Some(dictionary_page_offset)
                if dictionary_page_offset > 0 && dictionary_page_offset < data_page_offset =>
            {
                dictionary_page_offset
            }
            _ => data_page_offset,
        };
        offsets.push(split_offset);
    }
    (!offsets.is_empty()).then_some(offsets)
}

fn collect_iceberg_column_stats(metadata: &ParquetMetaData) -> Option<IcebergColumnStats> {
    let mut accumulators: BTreeMap<i32, ColumnStatsAccumulator> = BTreeMap::new();
    for row_group in metadata.row_groups() {
        for column in row_group.columns() {
            let basic_info = column.column_descr().self_type().get_basic_info();
            if !basic_info.has_id() {
                continue;
            }
            let field_id = basic_info.id();
            let accumulator = accumulators.entry(field_id).or_default();
            accumulator.column_size += column.compressed_size();
            let Some(stats) = column.statistics() else {
                continue;
            };
            accumulator.has_statistics = true;
            accumulator.value_count += column.num_values();
            if let Some(null_count) = stats.null_count_opt() {
                accumulator.null_value_count += null_count as i64;
            }
            if let Some(merged) = accumulator.merged_statistics.as_mut() {
                merge_statistics(merged, stats);
            } else {
                accumulator.merged_statistics = Some(stats.clone());
            }
        }
    }
    if accumulators.is_empty() {
        return None;
    }

    let mut column_sizes = BTreeMap::new();
    let mut value_counts = BTreeMap::new();
    let mut null_value_counts = BTreeMap::new();
    let mut lower_bounds = BTreeMap::new();
    let mut upper_bounds = BTreeMap::new();
    for (field_id, accumulator) in accumulators {
        column_sizes.insert(field_id, accumulator.column_size);
        if !accumulator.has_statistics {
            continue;
        }
        value_counts.insert(field_id, accumulator.value_count);
        null_value_counts.insert(field_id, accumulator.null_value_count);
        if let Some(stats) = accumulator.merged_statistics.as_ref() {
            if let Some(min) = stats.min_bytes_opt() {
                lower_bounds.insert(field_id, min.to_vec());
            }
            if let Some(max) = stats.max_bytes_opt() {
                upper_bounds.insert(field_id, max.to_vec());
            }
        }
    }
    Some(IcebergColumnStats {
        column_sizes,
        value_counts,
        null_value_counts,
        nan_value_counts: BTreeMap::new(),
        lower_bounds,
        upper_bounds,
    })
}

#[derive(Default)]
struct ColumnStatsAccumulator {
    column_size: i64,
    value_count: i64,
    null_value_count: i64,
    has_statistics: bool,
    merged_statistics: Option<Statistics>,
}

fn merge_statistics(current: &mut Statistics, next: &Statistics) {
    match (current, next) {
        (Statistics::Boolean(current), Statistics::Boolean(next)) => {
            *current = merge_typed_statistics(current, next, PartialOrd::partial_cmp);
        }
        (Statistics::Int32(current), Statistics::Int32(next)) => {
            *current = merge_typed_statistics(current, next, PartialOrd::partial_cmp);
        }
        (Statistics::Int64(current), Statistics::Int64(next)) => {
            *current = merge_typed_statistics(current, next, PartialOrd::partial_cmp);
        }
        (Statistics::Int96(current), Statistics::Int96(next)) => {
            *current = merge_typed_statistics(current, next, PartialOrd::partial_cmp);
        }
        (Statistics::Float(current), Statistics::Float(next)) => {
            *current = merge_typed_statistics(current, next, PartialOrd::partial_cmp);
        }
        (Statistics::Double(current), Statistics::Double(next)) => {
            *current = merge_typed_statistics(current, next, PartialOrd::partial_cmp);
        }
        (Statistics::ByteArray(current), Statistics::ByteArray(next)) => {
            *current = merge_typed_statistics(current, next, PartialOrd::partial_cmp);
        }
        (Statistics::FixedLenByteArray(current), Statistics::FixedLenByteArray(next)) => {
            *current = merge_typed_statistics(current, next, PartialOrd::partial_cmp);
        }
        _ => {}
    }
}

fn merge_typed_statistics<T, F>(
    current: &ValueStatistics<T>,
    next: &ValueStatistics<T>,
    compare: F,
) -> ValueStatistics<T>
where
    T: Clone + AsBytes,
    F: Fn(&T, &T) -> Option<Ordering>,
{
    let min = choose_min(current.min_opt(), next.min_opt(), &compare);
    let max = choose_max(current.max_opt(), next.max_opt(), &compare);
    let null_count =
        Some(current.null_count_opt().unwrap_or(0) + next.null_count_opt().unwrap_or(0));
    let min_is_exact = match (current.min_opt(), next.min_opt()) {
        (Some(_), Some(_)) => current.min_is_exact() && next.min_is_exact(),
        (Some(_), None) => current.min_is_exact(),
        (None, Some(_)) => next.min_is_exact(),
        (None, None) => false,
    };
    let max_is_exact = match (current.max_opt(), next.max_opt()) {
        (Some(_), Some(_)) => current.max_is_exact() && next.max_is_exact(),
        (Some(_), None) => current.max_is_exact(),
        (None, Some(_)) => next.max_is_exact(),
        (None, None) => false,
    };
    ValueStatistics::new(min, max, None, null_count, false)
        .with_backwards_compatible_min_max(
            current.is_min_max_backwards_compatible() && next.is_min_max_backwards_compatible(),
        )
        .with_min_is_exact(min_is_exact)
        .with_max_is_exact(max_is_exact)
}

fn choose_min<T, F>(left: Option<&T>, right: Option<&T>, compare: &F) -> Option<T>
where
    T: Clone,
    F: Fn(&T, &T) -> Option<Ordering>,
{
    match (left, right) {
        (Some(left), Some(right)) => match compare(left, right) {
            Some(Ordering::Greater) => Some(right.clone()),
            _ => Some(left.clone()),
        },
        (Some(left), None) => Some(left.clone()),
        (None, Some(right)) => Some(right.clone()),
        (None, None) => None,
    }
}

fn choose_max<T, F>(left: Option<&T>, right: Option<&T>, compare: &F) -> Option<T>
where
    T: Clone,
    F: Fn(&T, &T) -> Option<Ordering>,
{
    match (left, right) {
        (Some(left), Some(right)) => match compare(left, right) {
            Some(Ordering::Less) => Some(right.clone()),
            _ => Some(left.clone()),
        },
        (Some(left), None) => Some(left.clone()),
        (None, Some(right)) => Some(right.clone()),
        (None, None) => None,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use novarocks_fs::{FsAccessResolver, TokioFileIoRuntime, TokioFileTaskSpawner};
    use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

    use super::{unique_file_path, write_parquet_file};
    use crate::access_binding::IcebergReadBinding;

    #[test]
    fn unique_file_path_requires_one_reference() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "file_path",
            DataType::Utf8,
            false,
        )]));
        let one = arrow::record_batch::RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(StringArray::from(vec!["a", "a"])) as ArrayRef],
        )
        .expect("batch");
        assert_eq!(
            unique_file_path(&one).expect("one reference"),
            Some("a".to_string())
        );

        let mixed = arrow::record_batch::RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(vec!["a", "b"])) as ArrayRef],
        )
        .expect("batch");
        assert_eq!(unique_file_path(&mixed).expect("mixed reference"), None);

        let wrong = arrow::record_batch::RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "position",
                DataType::Int64,
                false,
            )])),
            vec![Arc::new(Int64Array::from(vec![1_i64])) as ArrayRef],
        )
        .expect("batch");
        assert!(unique_file_path(&wrong).is_err());
    }

    #[tokio::test]
    async fn local_parquet_write_returns_provider_stats() {
        let runtime = tokio::runtime::Handle::current();
        let binding = IcebergReadBinding::new(
            None,
            FsAccessResolver::new(),
            Arc::new(TokioFileIoRuntime::new(runtime.clone())),
            Arc::new(TokioFileTaskSpawner::new(runtime)),
        );
        let mut metadata = HashMap::new();
        metadata.insert(PARQUET_FIELD_ID_META_KEY.to_string(), "3".to_string());
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true).with_metadata(metadata),
        ]));
        let batch = arrow::record_batch::RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![Some(1_i64), None, Some(2)])) as ArrayRef],
        )
        .expect("batch");
        let directory = tempfile::tempdir().expect("temporary output");
        let path = url::Url::from_file_path(directory.path().join("data.parquet"))
            .expect("file URL")
            .to_string();

        let result = write_parquet_file(
            &binding,
            &path,
            schema,
            &batch,
            parquet::basic::Compression::UNCOMPRESSED,
        )
        .await
        .expect("write parquet");

        assert!(result.file_size > 0);
        assert!(result.split_offsets.is_some());
        let stats = result.column_stats.expect("column stats");
        assert_eq!(stats.value_counts.get(&3), Some(&3));
        assert_eq!(stats.null_value_counts.get(&3), Some(&1));
    }
}
