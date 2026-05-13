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

use std::collections::HashSet;

use arrow::array::{
    Array, BinaryArray, BooleanArray, Date32Array, Date64Array, Decimal128Array, Float32Array,
    Float64Array, Int8Array, Int16Array, Int32Array, Int64Array, LargeBinaryArray,
    LargeStringArray, RecordBatch, StringArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt8Array,
    UInt16Array, UInt32Array, UInt64Array,
};
use arrow::compute::filter_record_batch;
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use parquet::arrow::{PARQUET_FIELD_ID_META_KEY, arrow_reader::ParquetRecordBatchReaderBuilder};

use crate::cache::CachedRangeReader;
use crate::connector::iceberg::position_delete::IcebergDeleteFileSpec;
use crate::descriptors::THdfsFileFormat;
use crate::formats::parquet::{ParquetCachedReader, ParquetReadCachePolicy};
use crate::fs::opendal::OpendalRangeReaderFactory;
use crate::types::TIcebergFileContent;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
enum EqualityValue {
    Null,
    Bool(bool),
    I64(i64),
    U64(u64),
    F64(u64),
    Utf8(String),
    Binary(Vec<u8>),
    Decimal128(i128, i8),
    Date32(i32),
    Date64(i64),
    Timestamp(i64, TimeUnit),
}

#[derive(Clone, Debug)]
pub struct EqualityDeleteSet {
    columns: Vec<EqualityColumnRef>,
    keys: HashSet<Vec<EqualityValue>>,
}

#[derive(Clone, Debug)]
struct EqualityColumnRef {
    name: String,
    field_id: Option<i32>,
}

pub(crate) fn load_equality_delete_sets(
    specs: &[IcebergDeleteFileSpec],
    factory: &OpendalRangeReaderFactory,
) -> Result<Vec<EqualityDeleteSet>, String> {
    let mut sets = Vec::new();
    for spec in specs {
        if spec.file_content != TIcebergFileContent::EQUALITY_DELETES {
            continue;
        }
        if spec.file_format != THdfsFileFormat::PARQUET {
            return Err(format!(
                "iceberg equality-delete file {} has unsupported format {:?}; only PARQUET is supported",
                spec.path, spec.file_format
            ));
        }
        if spec.content_offset.is_some() || spec.content_size_in_bytes.is_some() {
            return Err(format!(
                "iceberg equality-delete file {} must not carry Puffin content offsets",
                spec.path
            ));
        }
        let reader = factory
            .open_with_len(&spec.path, spec.length)
            .map_err(|e| {
                format!(
                    "open iceberg equality-delete file {} failed: {e}",
                    spec.path
                )
            })?;
        let reader = ParquetCachedReader::new(
            CachedRangeReader::new(reader, None),
            ParquetReadCachePolicy::with_flags(false, false, None),
        );
        let builder = ParquetRecordBatchReaderBuilder::try_new(reader).map_err(|e| {
            format!(
                "read iceberg equality-delete file {} metadata failed: {e}",
                spec.path
            )
        })?;
        let schema = builder.schema();
        if schema.fields().is_empty() {
            return Err(format!(
                "iceberg equality-delete file {} has no equality columns",
                spec.path
            ));
        }
        let columns = schema
            .fields()
            .iter()
            .map(|field| equality_column_ref(field.as_ref()))
            .collect::<Result<Vec<_>, _>>()?;
        let reader = builder.build().map_err(|e| {
            format!(
                "build iceberg equality-delete reader for {} failed: {e}",
                spec.path
            )
        })?;
        let mut keys = HashSet::new();
        for batch in reader {
            let batch = batch.map_err(|e| {
                format!(
                    "read iceberg equality-delete file {} batch failed: {e}",
                    spec.path
                )
            })?;
            if batch.num_columns() != columns.len() {
                return Err(format!(
                    "equality-delete batch from {} has {} columns, expected {}",
                    spec.path,
                    batch.num_columns(),
                    columns.len()
                ));
            }
            for row in 0..batch.num_rows() {
                keys.insert(equality_key_for_row(&batch, row, &columns)?);
            }
        }
        sets.push(EqualityDeleteSet { columns, keys });
    }
    Ok(sets)
}

pub(crate) fn equality_delete_keep_mask(
    batch: &RecordBatch,
    sets: &[EqualityDeleteSet],
) -> Result<Option<Vec<bool>>, String> {
    if sets.is_empty() || batch.num_rows() == 0 {
        return Ok(None);
    }
    let mut keep = Vec::with_capacity(batch.num_rows());
    let mut deleted_count = 0usize;
    for row in 0..batch.num_rows() {
        let deleted = row_matches_any_equality_delete(batch, row, sets)?;
        if deleted {
            deleted_count += 1;
        }
        keep.push(!deleted);
    }
    if deleted_count == 0 {
        return Ok(None);
    }
    Ok(Some(keep))
}

#[allow(dead_code)]
pub(crate) fn read_data_file_matching_equality_deletes_with_path_normalizer<N>(
    data_file_path: &str,
    data_file_size: Option<u64>,
    sets: &[EqualityDeleteSet],
    factory: &OpendalRangeReaderFactory,
    normalize_path: N,
) -> Result<Vec<RecordBatch>, String>
where
    N: Fn(&str) -> Result<String, String>,
{
    if sets.is_empty() {
        return Ok(Vec::new());
    }

    let normalized_path = normalize_path(data_file_path)?;
    let reader = factory
        .open_with_len(&normalized_path, data_file_size)
        .map_err(|e| {
            format!(
                "open iceberg data file {data_file_path} for equality-delete reverse projection failed: {e}"
            )
        })?;
    let reader = ParquetCachedReader::new(
        CachedRangeReader::new(reader, None),
        ParquetReadCachePolicy::with_flags(false, false, None),
    );
    let reader = ParquetRecordBatchReaderBuilder::try_new(reader)
        .map_err(|e| {
            format!(
                "read iceberg data file {data_file_path} metadata for equality-delete reverse projection failed: {e}"
            )
        })?
        .build()
        .map_err(|e| {
            format!(
                "build iceberg data reader for equality-delete reverse projection {data_file_path} failed: {e}"
            )
        })?;

    let mut out = Vec::new();
    for batch in reader {
        let batch = batch.map_err(|e| {
            format!(
                "read iceberg data file {data_file_path} batch for equality-delete reverse projection failed: {e}"
            )
        })?;
        let Some(keep_mask) = equality_delete_keep_mask(&batch, sets)? else {
            continue;
        };
        let match_mask =
            BooleanArray::from(keep_mask.into_iter().map(|keep| !keep).collect::<Vec<_>>());
        let filtered = filter_record_batch(&batch, &match_mask).map_err(|e| {
            format!(
                "filter iceberg data file {data_file_path} for equality-delete reverse projection failed: {e}"
            )
        })?;
        if filtered.num_rows() > 0 {
            out.push(filtered);
        }
    }
    Ok(out)
}

fn row_matches_any_equality_delete(
    batch: &RecordBatch,
    row: usize,
    sets: &[EqualityDeleteSet],
) -> Result<bool, String> {
    for set in sets {
        let key = equality_key_for_row(batch, row, &set.columns)?;
        if set.keys.contains(&key) {
            return Ok(true);
        }
    }
    Ok(false)
}

pub(crate) fn equality_delete_row_is_deleted(
    batch: &RecordBatch,
    row: usize,
    sets: &[EqualityDeleteSet],
) -> Result<bool, String> {
    if sets.is_empty() {
        return Ok(false);
    }
    row_matches_any_equality_delete(batch, row, sets)
}

fn equality_key_for_row(
    batch: &RecordBatch,
    row: usize,
    columns: &[EqualityColumnRef],
) -> Result<Vec<EqualityValue>, String> {
    let schema = batch.schema();
    let mut key = Vec::with_capacity(columns.len());
    for column in columns {
        let idx = find_equality_column_index(schema.as_ref(), column)?;
        key.push(equality_value(batch.column(idx).as_ref(), row)?);
    }
    Ok(key)
}

fn equality_column_ref(field: &Field) -> Result<EqualityColumnRef, String> {
    Ok(EqualityColumnRef {
        name: field.name().to_ascii_lowercase(),
        field_id: parse_parquet_field_id(field)?,
    })
}

fn find_equality_column_index(
    schema: &Schema,
    column: &EqualityColumnRef,
) -> Result<usize, String> {
    if let Some(target_field_id) = column.field_id {
        let mut schema_has_field_ids = false;
        for (idx, field) in schema.fields().iter().enumerate() {
            let field_id = parse_parquet_field_id(field.as_ref())?;
            schema_has_field_ids |= field_id.is_some();
            if field_id == Some(target_field_id) {
                return Ok(idx);
            }
        }
        if schema_has_field_ids {
            return Err(equality_column_missing_error(schema, column));
        }
    }

    schema
        .fields()
        .iter()
        .position(|field| field.name().eq_ignore_ascii_case(&column.name))
        .ok_or_else(|| equality_column_missing_error(schema, column))
}

fn equality_column_missing_error(schema: &Schema, column: &EqualityColumnRef) -> String {
    let field_id = column
        .field_id
        .map(|id| format!(" field_id={id}"))
        .unwrap_or_default();
    format!(
        "equality-delete column `{}`{} is not available in data batch schema {:?}",
        column.name,
        field_id,
        schema.fields().iter().map(|f| f.name()).collect::<Vec<_>>()
    )
}

fn parse_parquet_field_id(field: &Field) -> Result<Option<i32>, String> {
    let Some(raw) = field.metadata().get(PARQUET_FIELD_ID_META_KEY) else {
        return Ok(None);
    };
    raw.parse::<i32>().map(Some).map_err(|e| {
        format!(
            "invalid parquet field_id metadata: field={} key={} value={} error={}",
            field.name(),
            PARQUET_FIELD_ID_META_KEY,
            raw,
            e
        )
    })
}

fn equality_value(array: &dyn Array, row: usize) -> Result<EqualityValue, String> {
    if array.is_null(row) {
        return Ok(EqualityValue::Null);
    }
    match array.data_type() {
        DataType::Boolean => {
            let a = array_as::<BooleanArray>(array)?;
            Ok(EqualityValue::Bool(a.value(row)))
        }
        DataType::Int8 => {
            let a = array_as::<Int8Array>(array)?;
            Ok(EqualityValue::I64(i64::from(a.value(row))))
        }
        DataType::Int16 => {
            let a = array_as::<Int16Array>(array)?;
            Ok(EqualityValue::I64(i64::from(a.value(row))))
        }
        DataType::Int32 => {
            let a = array_as::<Int32Array>(array)?;
            Ok(EqualityValue::I64(i64::from(a.value(row))))
        }
        DataType::Int64 => {
            let a = array_as::<Int64Array>(array)?;
            Ok(EqualityValue::I64(a.value(row)))
        }
        DataType::UInt8 => {
            let a = array_as::<UInt8Array>(array)?;
            Ok(EqualityValue::U64(u64::from(a.value(row))))
        }
        DataType::UInt16 => {
            let a = array_as::<UInt16Array>(array)?;
            Ok(EqualityValue::U64(u64::from(a.value(row))))
        }
        DataType::UInt32 => {
            let a = array_as::<UInt32Array>(array)?;
            Ok(EqualityValue::U64(u64::from(a.value(row))))
        }
        DataType::UInt64 => {
            let a = array_as::<UInt64Array>(array)?;
            Ok(EqualityValue::U64(a.value(row)))
        }
        DataType::Float32 => {
            let a = array_as::<Float32Array>(array)?;
            Ok(EqualityValue::F64(f64::from(a.value(row)).to_bits()))
        }
        DataType::Float64 => {
            let a = array_as::<Float64Array>(array)?;
            Ok(EqualityValue::F64(a.value(row).to_bits()))
        }
        DataType::Utf8 => {
            let a = array_as::<StringArray>(array)?;
            Ok(EqualityValue::Utf8(a.value(row).to_string()))
        }
        DataType::LargeUtf8 => {
            let a = array_as::<LargeStringArray>(array)?;
            Ok(EqualityValue::Utf8(a.value(row).to_string()))
        }
        DataType::Binary => {
            let a = array_as::<BinaryArray>(array)?;
            Ok(EqualityValue::Binary(a.value(row).to_vec()))
        }
        DataType::LargeBinary => {
            let a = array_as::<LargeBinaryArray>(array)?;
            Ok(EqualityValue::Binary(a.value(row).to_vec()))
        }
        DataType::Decimal128(_, scale) => {
            let a = array_as::<Decimal128Array>(array)?;
            Ok(EqualityValue::Decimal128(a.value(row), *scale))
        }
        DataType::Date32 => {
            let a = array_as::<Date32Array>(array)?;
            Ok(EqualityValue::Date32(a.value(row)))
        }
        DataType::Date64 => {
            let a = array_as::<Date64Array>(array)?;
            Ok(EqualityValue::Date64(a.value(row)))
        }
        DataType::Timestamp(TimeUnit::Second, _) => {
            let a = array_as::<TimestampSecondArray>(array)?;
            Ok(EqualityValue::Timestamp(a.value(row), TimeUnit::Second))
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            let a = array_as::<TimestampMillisecondArray>(array)?;
            Ok(EqualityValue::Timestamp(
                a.value(row),
                TimeUnit::Millisecond,
            ))
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            let a = array_as::<TimestampMicrosecondArray>(array)?;
            Ok(EqualityValue::Timestamp(
                a.value(row),
                TimeUnit::Microsecond,
            ))
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            let a = array_as::<TimestampNanosecondArray>(array)?;
            Ok(EqualityValue::Timestamp(a.value(row), TimeUnit::Nanosecond))
        }
        other => Err(format!(
            "unsupported equality-delete column type for row filtering: {other:?}"
        )),
    }
}

fn array_as<T: 'static>(array: &dyn Array) -> Result<&T, String> {
    array.as_any().downcast_ref::<T>().ok_or_else(|| {
        format!(
            "array downcast failed for equality-delete filtering: {:?}",
            array.data_type()
        )
    })
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::sync::Arc;

    use arrow::array::{Decimal128Array, Float32Array, Float64Array, Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::{ArrowWriter, PARQUET_FIELD_ID_META_KEY};

    use crate::connector::iceberg::position_delete::IcebergDeleteFileSpec;
    use crate::descriptors::THdfsFileFormat;
    use crate::fs::opendal::{OpendalRangeReaderFactory, build_fs_operator};
    use crate::types::TIcebergFileContent;

    fn temp_dir_for(name: &str) -> std::path::PathBuf {
        let mut dir = std::env::temp_dir();
        dir.push(format!(
            "novarocks_equality_delete_tests_{}_{}",
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

    fn field_with_id(name: &str, data_type: DataType, nullable: bool, field_id: i32) -> Field {
        Field::new(name, data_type, nullable).with_metadata(std::collections::HashMap::from([(
            PARQUET_FIELD_ID_META_KEY.to_string(),
            field_id.to_string(),
        )]))
    }

    fn write_eq_delete_parquet(path: &std::path::Path) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("category", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![2, 4])),
                Arc::new(StringArray::from(vec!["B", "A"])),
            ],
        )
        .expect("record batch");
        let file = fs::File::create(path).expect("create");
        let mut writer = ArrowWriter::try_new(file, schema, None).expect("writer");
        writer.write(&batch).expect("write");
        writer.close().expect("close");
    }

    fn write_eq_delete_parquet_with_old_field_name(path: &std::path::Path) {
        let schema = Arc::new(Schema::new(vec![field_with_id(
            "amount",
            DataType::Int32,
            false,
            2,
        )]));
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vec![20]))])
                .expect("record batch");
        let file = fs::File::create(path).expect("create");
        let mut writer = ArrowWriter::try_new(file, schema, None).expect("writer");
        writer.write(&batch).expect("write");
        writer.close().expect("close");
    }

    fn write_float32_eq_delete_parquet(path: &std::path::Path) {
        let schema = Arc::new(Schema::new(vec![field_with_id(
            "ratio",
            DataType::Float32,
            false,
            1,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Float32Array::from(vec![1.5_f32]))],
        )
        .expect("record batch");
        let file = fs::File::create(path).expect("create");
        let mut writer = ArrowWriter::try_new(file, schema, None).expect("writer");
        writer.write(&batch).expect("write");
        writer.close().expect("close");
    }

    fn write_decimal_eq_delete_parquet(path: &std::path::Path) {
        let schema = Arc::new(Schema::new(vec![field_with_id(
            "amount",
            DataType::Decimal128(10, 2),
            false,
            1,
        )]));
        let amount = Decimal128Array::from(vec![1234_i128])
            .with_precision_and_scale(10, 2)
            .expect("decimal delete array");
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(amount)]).expect("record batch");
        let file = fs::File::create(path).expect("create");
        let mut writer = ArrowWriter::try_new(file, schema, None).expect("writer");
        writer.write(&batch).expect("write");
        writer.close().expect("close");
    }

    fn write_data_parquet(path: &std::path::Path) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("category", DataType::Utf8, false),
            Field::new("amount", DataType::Int32, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3, 4])),
                Arc::new(StringArray::from(vec!["A", "B", "B", "A"])),
                Arc::new(Int32Array::from(vec![10, 20, 30, 40])),
            ],
        )
        .expect("data batch");
        let file = fs::File::create(path).expect("create data");
        let mut writer = ArrowWriter::try_new(file, schema, None).expect("writer");
        writer.write(&batch).expect("write data");
        writer.close().expect("close data");
    }

    #[test]
    fn equality_delete_keep_mask_drops_matching_rows() {
        let dir = temp_dir_for("mask");
        let delete_path = dir.join("eq-delete.parquet");
        write_eq_delete_parquet(&delete_path);
        let spec = IcebergDeleteFileSpec {
            path: delete_path
                .file_name()
                .unwrap()
                .to_string_lossy()
                .to_string(),
            file_format: THdfsFileFormat::PARQUET,
            file_content: TIcebergFileContent::EQUALITY_DELETES,
            length: None,
            content_offset: None,
            content_size_in_bytes: None,
        };
        let sets = super::load_equality_delete_sets(&[spec], &factory_for_dir(&dir)).expect("load");

        let data_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("category", DataType::Utf8, false),
            Field::new("amount", DataType::Int32, false),
        ]));
        let data = RecordBatch::try_new(
            data_schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3, 4])),
                Arc::new(StringArray::from(vec!["A", "B", "B", "A"])),
                Arc::new(Int32Array::from(vec![10, 20, 30, 40])),
            ],
        )
        .expect("data batch");

        let mask = super::equality_delete_keep_mask(&data, &sets).expect("mask");

        assert_eq!(mask, Some(vec![true, false, true, false]));
    }

    #[test]
    fn equality_delete_matches_renamed_data_column_by_field_id() {
        let dir = temp_dir_for("field_id_rename");
        let delete_path = dir.join("eq-delete-renamed.parquet");
        write_eq_delete_parquet_with_old_field_name(&delete_path);
        let spec = IcebergDeleteFileSpec {
            path: delete_path
                .file_name()
                .unwrap()
                .to_string_lossy()
                .to_string(),
            file_format: THdfsFileFormat::PARQUET,
            file_content: TIcebergFileContent::EQUALITY_DELETES,
            length: None,
            content_offset: None,
            content_size_in_bytes: None,
        };
        let sets = super::load_equality_delete_sets(&[spec], &factory_for_dir(&dir)).expect("load");

        let data_schema = Arc::new(Schema::new(vec![
            field_with_id("id", DataType::Int32, false, 1),
            field_with_id("total_amount", DataType::Int32, false, 2),
        ]));
        let data = RecordBatch::try_new(
            data_schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(Int32Array::from(vec![10, 20, 30])),
            ],
        )
        .expect("data batch");

        let mask = super::equality_delete_keep_mask(&data, &sets).expect("mask");

        assert_eq!(mask, Some(vec![true, false, true]));
    }

    #[test]
    fn equality_delete_rejects_same_name_with_different_field_id() {
        let dir = temp_dir_for("field_id_readd");
        let delete_path = dir.join("eq-delete-old-id.parquet");
        write_eq_delete_parquet_with_old_field_name(&delete_path);
        let spec = IcebergDeleteFileSpec {
            path: delete_path
                .file_name()
                .unwrap()
                .to_string_lossy()
                .to_string(),
            file_format: THdfsFileFormat::PARQUET,
            file_content: TIcebergFileContent::EQUALITY_DELETES,
            length: None,
            content_offset: None,
            content_size_in_bytes: None,
        };
        let sets = super::load_equality_delete_sets(&[spec], &factory_for_dir(&dir)).expect("load");

        let data_schema = Arc::new(Schema::new(vec![field_with_id(
            "amount",
            DataType::Int32,
            false,
            3,
        )]));
        let data = RecordBatch::try_new(data_schema, vec![Arc::new(Int32Array::from(vec![20]))])
            .expect("data batch");

        let err = super::equality_delete_keep_mask(&data, &sets).expect_err("field-id mismatch");

        assert!(err.contains("field_id=2"), "{err}");
    }

    #[test]
    fn equality_delete_matches_float_promoted_to_double() {
        let dir = temp_dir_for("float_promotion");
        let delete_path = dir.join("eq-delete-float.parquet");
        write_float32_eq_delete_parquet(&delete_path);
        let spec = IcebergDeleteFileSpec {
            path: delete_path
                .file_name()
                .unwrap()
                .to_string_lossy()
                .to_string(),
            file_format: THdfsFileFormat::PARQUET,
            file_content: TIcebergFileContent::EQUALITY_DELETES,
            length: None,
            content_offset: None,
            content_size_in_bytes: None,
        };
        let sets = super::load_equality_delete_sets(&[spec], &factory_for_dir(&dir)).expect("load");

        let data_schema = Arc::new(Schema::new(vec![field_with_id(
            "ratio",
            DataType::Float64,
            false,
            1,
        )]));
        let data = RecordBatch::try_new(
            data_schema,
            vec![Arc::new(Float64Array::from(vec![1.5_f64, 2.5_f64]))],
        )
        .expect("data batch");

        let mask = super::equality_delete_keep_mask(&data, &sets).expect("mask");

        assert_eq!(mask, Some(vec![false, true]));
    }

    #[test]
    fn equality_delete_matches_decimal_precision_promotion() {
        let dir = temp_dir_for("decimal_precision_promotion");
        let delete_path = dir.join("eq-delete-decimal.parquet");
        write_decimal_eq_delete_parquet(&delete_path);
        let spec = IcebergDeleteFileSpec {
            path: delete_path
                .file_name()
                .unwrap()
                .to_string_lossy()
                .to_string(),
            file_format: THdfsFileFormat::PARQUET,
            file_content: TIcebergFileContent::EQUALITY_DELETES,
            length: None,
            content_offset: None,
            content_size_in_bytes: None,
        };
        let sets = super::load_equality_delete_sets(&[spec], &factory_for_dir(&dir)).expect("load");

        let data_schema = Arc::new(Schema::new(vec![field_with_id(
            "amount",
            DataType::Decimal128(20, 2),
            false,
            1,
        )]));
        let amounts = Decimal128Array::from(vec![1234_i128, 5678_i128])
            .with_precision_and_scale(20, 2)
            .expect("decimal data array");
        let data = RecordBatch::try_new(data_schema, vec![Arc::new(amounts)]).expect("data batch");

        let mask = super::equality_delete_keep_mask(&data, &sets).expect("mask");

        assert_eq!(mask, Some(vec![false, true]));
    }

    #[test]
    fn equality_delete_reverse_projection_returns_matching_data_rows() {
        let dir = temp_dir_for("reverse");
        let delete_path = dir.join("eq-delete.parquet");
        let data_path = dir.join("data.parquet");
        write_eq_delete_parquet(&delete_path);
        write_data_parquet(&data_path);
        let factory = factory_for_dir(&dir);
        let spec = IcebergDeleteFileSpec {
            path: delete_path
                .file_name()
                .unwrap()
                .to_string_lossy()
                .to_string(),
            file_format: THdfsFileFormat::PARQUET,
            file_content: TIcebergFileContent::EQUALITY_DELETES,
            length: None,
            content_offset: None,
            content_size_in_bytes: None,
        };
        let sets = super::load_equality_delete_sets(&[spec], &factory).expect("load");

        let batches = super::read_data_file_matching_equality_deletes_with_path_normalizer(
            &data_path.file_name().unwrap().to_string_lossy(),
            None,
            &sets,
            &factory,
            |path| Ok(path.to_string()),
        )
        .expect("reverse projection");

        assert_eq!(batches.len(), 1);
        let ids = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("id array");
        assert_eq!(ids.values(), &[2, 4]);
    }
}
