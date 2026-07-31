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

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BinaryArray, BinaryBuilder, BooleanArray, Date32Array, Decimal128Array,
    FixedSizeBinaryArray, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array,
    Int64Array, LargeBinaryBuilder, StringArray, Time64MicrosecondArray, TimestampMicrosecondArray,
    TimestampNanosecondArray, new_empty_array, new_null_array,
};
use arrow::buffer::{OffsetBuffer, ScalarBuffer};
use arrow::compute::concat;
use arrow::datatypes::{DataType, Field, Fields, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use novarocks::engine::insert_engine::{InsertValue, QueryInsertBatch};
use novarocks::exec::expr::cast_with_special_rules;
use novarocks_catalog::identifier::normalize_identifier;
use novarocks_catalog::schema::{ColumnDef, ColumnDefault, validate_column_default};

/// Reorder literal rows into target-column order and materialize omitted values.
pub fn reorder_insert_rows(
    rows: &[Vec<InsertValue>],
    insert_columns: &[String],
    target_columns: &[ColumnDef],
) -> Result<Vec<Vec<InsertValue>>, String> {
    if insert_columns.is_empty() {
        for row in rows {
            if row.len() != target_columns.len() {
                return Err(format!(
                    "insert column count mismatch: expected {} values, got {}",
                    target_columns.len(),
                    row.len()
                ));
            }
        }
        return Ok(rows.to_vec());
    }

    for row in rows {
        if row.len() != insert_columns.len() {
            return Err(format!(
                "insert column count mismatch: expected {} values for column list, got {}",
                insert_columns.len(),
                row.len()
            ));
        }
    }
    let mapping = build_insert_column_mapping(insert_columns, target_columns)?;
    rows.iter()
        .map(|row| {
            mapping
                .iter()
                .enumerate()
                .map(|(target_idx, source_idx)| match source_idx {
                    Some(source_idx) => row.get(*source_idx).cloned().ok_or_else(|| {
                        format!(
                            "insert value for column position {} is missing",
                            source_idx + 1
                        )
                    }),
                    None => omitted_insert_value(&target_columns[target_idx]),
                })
                .collect()
        })
        .collect()
}

/// Align query output to target order, materializing defaults and applying casts.
pub fn align_query_batch_to_target(
    result: &QueryInsertBatch,
    insert_columns: &[String],
    target_columns: &[ColumnDef],
) -> Result<RecordBatch, String> {
    let source_column_count = result.columns.len();
    let mapping = build_query_column_mapping(insert_columns, target_columns, source_column_count)?;

    for (target_idx, source_idx) in mapping.iter().enumerate() {
        if source_idx.is_none() {
            validate_omitted_column(&target_columns[target_idx])?;
        }
    }

    let normalized_types = target_columns
        .iter()
        .map(|column| normalize_nested_type(&column.data_type))
        .collect::<Vec<_>>();
    let target_schema = Arc::new(Schema::new(
        target_columns
            .iter()
            .zip(&normalized_types)
            .map(|(column, data_type)| Field::new(&column.name, data_type.clone(), column.nullable))
            .collect::<Vec<_>>(),
    ));

    let mut per_target_columns: Vec<Vec<ArrayRef>> = vec![Vec::new(); target_columns.len()];
    for batch in &result.batches {
        if batch.num_columns() != source_column_count {
            return Err(format!(
                "INSERT SELECT batch has {} columns but query returns {}",
                batch.num_columns(),
                source_column_count
            ));
        }
        for (target_idx, source_idx) in mapping.iter().enumerate() {
            let target_column = &target_columns[target_idx];
            let target_type = &normalized_types[target_idx];
            let array = match source_idx {
                Some(source_idx) => {
                    cast_source_array(batch.column(*source_idx), target_type, &target_column.name)?
                }
                None => omitted_column_array(target_column, target_type, batch.num_rows())?,
            };
            per_target_columns[target_idx].push(array);
        }
    }

    let mut final_columns = Vec::with_capacity(target_columns.len());
    for (target_idx, arrays) in per_target_columns.into_iter().enumerate() {
        let array = match arrays.len() {
            0 => new_empty_array(&normalized_types[target_idx]),
            1 => arrays.into_iter().next().expect("one array"),
            _ => {
                let refs = arrays
                    .iter()
                    .map(|array| array.as_ref())
                    .collect::<Vec<&dyn Array>>();
                concat(&refs).map_err(|error| {
                    format!(
                        "INSERT SELECT failed to concat batches for column `{}`: {error}",
                        target_columns[target_idx].name
                    )
                })?
            }
        };
        final_columns.push(array);
    }

    RecordBatch::try_new(target_schema, final_columns)
        .map_err(|error| format!("build INSERT SELECT batch failed: {error}"))
}

fn build_insert_column_mapping(
    insert_columns: &[String],
    target_columns: &[ColumnDef],
) -> Result<Vec<Option<usize>>, String> {
    let mut insert_index_by_name = normalized_insert_columns(insert_columns)?;
    let mut mapping = Vec::with_capacity(target_columns.len());
    for column in target_columns {
        mapping.push(insert_index_by_name.remove(&normalize_identifier(&column.name)?));
    }
    if let Some((name, _)) = insert_index_by_name.into_iter().next() {
        return Err(format!("unknown INSERT column `{name}`"));
    }
    Ok(mapping)
}

fn build_query_column_mapping(
    insert_columns: &[String],
    target_columns: &[ColumnDef],
    source_column_count: usize,
) -> Result<Vec<Option<usize>>, String> {
    if insert_columns.is_empty() {
        if source_column_count != target_columns.len() {
            return Err(format!(
                "INSERT SELECT column count mismatch: target has {} columns, SELECT produces {}",
                target_columns.len(),
                source_column_count
            ));
        }
        return Ok((0..target_columns.len()).map(Some).collect());
    }
    if insert_columns.len() != source_column_count {
        return Err(format!(
            "INSERT SELECT column count mismatch: INSERT lists {} columns, SELECT produces {}",
            insert_columns.len(),
            source_column_count
        ));
    }
    build_insert_column_mapping(insert_columns, target_columns)
}

fn normalized_insert_columns(insert_columns: &[String]) -> Result<HashMap<String, usize>, String> {
    let mut indices = HashMap::with_capacity(insert_columns.len());
    for (index, column) in insert_columns.iter().enumerate() {
        let normalized = normalize_identifier(column)?;
        if indices.insert(normalized, index).is_some() {
            return Err(format!("duplicate INSERT column `{column}`"));
        }
    }
    Ok(indices)
}

fn omitted_insert_value(column: &ColumnDef) -> Result<InsertValue, String> {
    if let Some(default) = &column.write_default {
        validate_column_default(default)
            .map_err(|error| format!("INSERT write-default for `{}`: {error}", column.name))?;
        return default_to_insert_value(default, &column.data_type)
            .map_err(|error| format!("INSERT write-default for `{}`: {error}", column.name));
    }
    if column.nullable {
        Ok(InsertValue::Null)
    } else {
        Err(format!(
            "INSERT omits required column `{}` without a write default",
            column.name
        ))
    }
}

fn validate_omitted_column(column: &ColumnDef) -> Result<(), String> {
    if let Some(default) = &column.write_default {
        validate_column_default(default)
            .map_err(|error| format!("INSERT write-default for `{}`: {error}", column.name))?;
        scalar_default_array(default, &normalize_nested_type(&column.data_type), 0)
            .map(|_| ())
            .map_err(|error| format!("INSERT write-default for `{}`: {error}", column.name))
    } else if column.nullable {
        Ok(())
    } else {
        Err(format!(
            "INSERT omits required column `{}` without a write default",
            column.name
        ))
    }
}

fn default_to_insert_value(
    default: &ColumnDefault,
    data_type: &DataType,
) -> Result<InsertValue, String> {
    Ok(match (default, data_type) {
        (ColumnDefault::Boolean(value), DataType::Boolean) => InsertValue::Bool(*value),
        (
            ColumnDefault::Int32(value),
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64,
        ) => InsertValue::Int(i64::from(*value)),
        (ColumnDefault::Int64(value), DataType::Int64) => InsertValue::Int(*value),
        (ColumnDefault::Float32 { bits }, DataType::Float32) => {
            InsertValue::Float(f64::from(f32::from_bits(*bits)))
        }
        (ColumnDefault::Float64 { bits }, DataType::Float64) => {
            InsertValue::Float(f64::from_bits(*bits))
        }
        (
            ColumnDefault::Decimal {
                unscaled, scale, ..
            },
            DataType::Decimal128(_, target_scale),
        ) if *scale == *target_scale => InsertValue::String(format_decimal(*unscaled, *scale)?),
        (ColumnDefault::String(value), DataType::Utf8 | DataType::LargeUtf8) => {
            InsertValue::String(value.clone())
        }
        (ColumnDefault::Binary(value), DataType::Binary | DataType::LargeBinary) => {
            InsertValue::String(bytes_to_latin1(value))
        }
        (ColumnDefault::Date { days_since_epoch }, DataType::Date32) => {
            InsertValue::Date(format_date(*days_since_epoch)?)
        }
        (
            ColumnDefault::TimestampMicros { micros_since_epoch },
            DataType::Timestamp(TimeUnit::Microsecond, _),
        )
        | (
            ColumnDefault::TimestamptzMicros { micros_since_epoch },
            DataType::Timestamp(TimeUnit::Microsecond, _),
        ) => InsertValue::String(format_timestamp_micros(*micros_since_epoch)?),
        (
            ColumnDefault::TimestampNanos { nanos_since_epoch },
            DataType::Timestamp(TimeUnit::Nanosecond, _),
        )
        | (
            ColumnDefault::TimestamptzNanos { nanos_since_epoch },
            DataType::Timestamp(TimeUnit::Nanosecond, _),
        ) => InsertValue::String(format_timestamp_nanos(*nanos_since_epoch)),
        (ColumnDefault::Array(values), DataType::List(field)) => InsertValue::Array(
            values
                .iter()
                .map(|value| {
                    if matches!(value, ColumnDefault::Null) {
                        Ok(InsertValue::Null)
                    } else {
                        default_to_insert_value(value, field.data_type())
                    }
                })
                .collect::<Result<Vec<_>, _>>()?,
        ),
        (ColumnDefault::Map(entries), DataType::Map(field, _)) => {
            let DataType::Struct(fields) = field.data_type() else {
                return Err(format!(
                    "MAP has unexpected entries type {:?}",
                    field.data_type()
                ));
            };
            if fields.len() != 2 {
                return Err("MAP entries struct must contain key and value".to_string());
            }
            InsertValue::Map(
                entries
                    .iter()
                    .map(|(key, value)| {
                        Ok((
                            default_to_insert_value(key, fields[0].data_type())?,
                            if matches!(value, ColumnDefault::Null) {
                                InsertValue::Null
                            } else {
                                default_to_insert_value(value, fields[1].data_type())?
                            },
                        ))
                    })
                    .collect::<Result<Vec<_>, String>>()?,
            )
        }
        (ColumnDefault::Struct(values), DataType::Struct(fields))
            if values.len() == fields.len() =>
        {
            InsertValue::Struct(
                values
                    .iter()
                    .zip(fields)
                    .map(|((_, value), field)| {
                        if matches!(value, ColumnDefault::Null) {
                            Ok(InsertValue::Null)
                        } else {
                            default_to_insert_value(value, field.data_type())
                        }
                    })
                    .collect::<Result<Vec<_>, _>>()?,
            )
        }
        (value, data_type) => {
            return Err(format!(
                "write-default literal type does not match column type: literal={value:?} column={data_type:?}"
            ));
        }
    })
}

fn cast_source_array(
    source: &ArrayRef,
    target_type: &DataType,
    target_column_name: &str,
) -> Result<ArrayRef, String> {
    let cast_input = remote_binary_text_cast_input(source, target_type, target_column_name)?;
    if cast_input.data_type() == target_type {
        return Ok(cast_input);
    }
    cast_with_special_rules(&cast_input, target_type).map_err(|error| {
        format!(
            "INSERT SELECT cannot cast column `{target_column_name}` from {:?} to {target_type:?}: {error}",
            source.data_type()
        )
    })
}

fn remote_binary_text_cast_input(
    source: &ArrayRef,
    target_type: &DataType,
    target_column_name: &str,
) -> Result<ArrayRef, String> {
    if matches!(target_type, DataType::Binary | DataType::LargeBinary) {
        return Ok(Arc::clone(source));
    }
    let Some(binary) = source.as_any().downcast_ref::<BinaryArray>() else {
        return Ok(Arc::clone(source));
    };
    let values = (0..binary.len())
        .map(|row| {
            if binary.is_null(row) {
                Ok(None)
            } else {
                std::str::from_utf8(binary.value(row))
                    .map(|value| Some(value.to_string()))
                    .map_err(|error| {
                        format!(
                            "INSERT SELECT column `{target_column_name}` contains non-UTF8 remote text: {error}"
                        )
                    })
            }
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Arc::new(StringArray::from(values)))
}

fn omitted_column_array(
    column: &ColumnDef,
    target_type: &DataType,
    len: usize,
) -> Result<ArrayRef, String> {
    let Some(default) = &column.write_default else {
        if column.nullable {
            return Ok(new_null_array(target_type, len));
        }
        return Err(format!(
            "INSERT omits required column `{}` without a write default",
            column.name
        ));
    };
    validate_column_default(default)
        .map_err(|error| format!("INSERT write-default for `{}`: {error}", column.name))?;
    scalar_default_array(default, target_type, len)
        .map_err(|error| format!("INSERT write-default for `{}`: {error}", column.name))
}

fn scalar_default_array(
    default: &ColumnDefault,
    data_type: &DataType,
    len: usize,
) -> Result<ArrayRef, String> {
    let array: ArrayRef = match (default, data_type) {
        (ColumnDefault::Boolean(value), DataType::Boolean) => {
            Arc::new(BooleanArray::from(vec![Some(*value); len]))
        }
        (ColumnDefault::Int32(value), DataType::Int8) => Arc::new(Int8Array::from(vec![
            Some(
                i8::try_from(*value)
                    .map_err(|_| format!("DEFAULT value {value} is out of range for TINYINT"))?
            );
            len
        ])),
        (ColumnDefault::Int32(value), DataType::Int16) => Arc::new(Int16Array::from(vec![
            Some(
                i16::try_from(*value)
                    .map_err(|_| format!("DEFAULT value {value} is out of range for SMALLINT"))?
            );
            len
        ])),
        (ColumnDefault::Int32(value), DataType::Int32) => {
            Arc::new(Int32Array::from(vec![Some(*value); len]))
        }
        (ColumnDefault::Int32(value), DataType::Int64) => {
            Arc::new(Int64Array::from(vec![Some(i64::from(*value)); len]))
        }
        (ColumnDefault::Int64(value), DataType::Int64) => {
            Arc::new(Int64Array::from(vec![Some(*value); len]))
        }
        (ColumnDefault::Float32 { bits }, DataType::Float32) => {
            Arc::new(Float32Array::from(vec![Some(f32::from_bits(*bits)); len]))
        }
        (ColumnDefault::Float64 { bits }, DataType::Float64) => {
            Arc::new(Float64Array::from(vec![Some(f64::from_bits(*bits)); len]))
        }
        (
            ColumnDefault::Decimal {
                unscaled,
                precision,
                scale,
            },
            DataType::Decimal128(target_precision, target_scale),
        ) if precision == target_precision && scale == target_scale => Arc::new(
            Decimal128Array::from(vec![Some(*unscaled); len])
                .with_precision_and_scale(*target_precision, *target_scale)
                .map_err(|error| format!("build DECIMAL default failed: {error}"))?,
        ),
        (ColumnDefault::String(value), DataType::Utf8) => {
            Arc::new(StringArray::from(vec![Some(value.as_str()); len]))
        }
        (ColumnDefault::Binary(value), DataType::Binary) => {
            let mut builder = BinaryBuilder::new();
            for _ in 0..len {
                builder.append_value(value);
            }
            Arc::new(builder.finish())
        }
        (ColumnDefault::Binary(value), DataType::LargeBinary) => {
            let mut builder = LargeBinaryBuilder::new();
            for _ in 0..len {
                builder.append_value(value);
            }
            Arc::new(builder.finish())
        }
        (ColumnDefault::Date { days_since_epoch }, DataType::Date32) => {
            Arc::new(Date32Array::from(vec![Some(*days_since_epoch); len]))
        }
        (
            ColumnDefault::TimeMicros {
                micros_since_midnight,
            },
            DataType::Time64(TimeUnit::Microsecond),
        ) => Arc::new(Time64MicrosecondArray::from(vec![
            Some(
                *micros_since_midnight
            );
            len
        ])),
        (
            ColumnDefault::TimestampMicros { micros_since_epoch }
            | ColumnDefault::TimestamptzMicros { micros_since_epoch },
            DataType::Timestamp(TimeUnit::Microsecond, timezone),
        ) => Arc::new(
            TimestampMicrosecondArray::from(vec![Some(*micros_since_epoch); len])
                .with_timezone_opt(timezone.clone()),
        ),
        (
            ColumnDefault::TimestampNanos { nanos_since_epoch }
            | ColumnDefault::TimestamptzNanos { nanos_since_epoch },
            DataType::Timestamp(TimeUnit::Nanosecond, timezone),
        ) => Arc::new(
            TimestampNanosecondArray::from(vec![Some(*nanos_since_epoch); len])
                .with_timezone_opt(timezone.clone()),
        ),
        (ColumnDefault::Fixed { size, bytes }, DataType::FixedSizeBinary(width))
            if *size == u64::try_from(*width).unwrap_or(u64::MAX) =>
        {
            Arc::new(
                FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                    std::iter::repeat_n(Some(bytes.as_slice()), len),
                    *width,
                )
                .map_err(|error| format!("build FIXED default failed: {error}"))?,
            )
        }
        (ColumnDefault::Uuid(bytes), DataType::FixedSizeBinary(16)) => Arc::new(
            FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                std::iter::repeat_n(Some(bytes.as_slice()), len),
                16,
            )
            .map_err(|error| format!("build UUID default failed: {error}"))?,
        ),
        (ColumnDefault::Array(values), DataType::List(field)) if values.is_empty() => {
            Arc::new(arrow::array::ListArray::new(
                Arc::clone(field),
                OffsetBuffer::new(ScalarBuffer::from(vec![0_i32; len + 1])),
                new_empty_array(field.data_type()),
                None,
            ))
        }
        (ColumnDefault::Map(entries), DataType::Map(field, ordered)) if entries.is_empty() => {
            let DataType::Struct(fields) = field.data_type() else {
                return Err(format!(
                    "MAP has unexpected entries type {:?}",
                    field.data_type()
                ));
            };
            Arc::new(arrow::array::MapArray::new(
                Arc::clone(field),
                OffsetBuffer::new(ScalarBuffer::from(vec![0_i32; len + 1])),
                arrow::array::StructArray::new(
                    fields.clone(),
                    fields
                        .iter()
                        .map(|field| new_empty_array(field.data_type()))
                        .collect(),
                    None,
                ),
                None,
                *ordered,
            ))
        }
        (value, data_type) => {
            return Err(format!(
                "write-default literal type does not match column type: literal={value:?} column={data_type:?}"
            ));
        }
    };
    Ok(array)
}

fn normalize_nested_type(data_type: &DataType) -> DataType {
    match data_type {
        DataType::List(field) => DataType::List(Arc::new(Field::new(
            field.name(),
            normalize_nested_type(field.data_type()),
            field.is_nullable(),
        ))),
        DataType::LargeList(field) => DataType::LargeList(Arc::new(Field::new(
            field.name(),
            normalize_nested_type(field.data_type()),
            field.is_nullable(),
        ))),
        DataType::FixedSizeList(field, size) => DataType::FixedSizeList(
            Arc::new(Field::new(
                field.name(),
                normalize_nested_type(field.data_type()),
                field.is_nullable(),
            )),
            *size,
        ),
        DataType::Struct(fields) => DataType::Struct(Fields::from(
            fields
                .iter()
                .map(|field| {
                    Arc::new(Field::new(
                        field.name(),
                        normalize_nested_type(field.data_type()),
                        field.is_nullable(),
                    ))
                })
                .collect::<Vec<_>>(),
        )),
        DataType::Map(field, ordered) => {
            let normalized = match field.data_type() {
                DataType::Struct(fields) => DataType::Struct(Fields::from(
                    fields
                        .iter()
                        .enumerate()
                        .map(|(index, child)| {
                            Arc::new(Field::new(
                                child.name(),
                                normalize_nested_type(child.data_type()),
                                index != 0 && child.is_nullable(),
                            ))
                        })
                        .collect::<Vec<_>>(),
                )),
                other => normalize_nested_type(other),
            };
            DataType::Map(
                Arc::new(Field::new(field.name(), normalized, false)),
                *ordered,
            )
        }
        other => other.clone(),
    }
}

fn format_decimal(unscaled: i128, scale: i8) -> Result<String, String> {
    if scale < 0 {
        return Err(format!("negative DECIMAL scale {scale} is not supported"));
    }
    let negative = unscaled.is_negative();
    let digits = unscaled.unsigned_abs().to_string();
    let scale = usize::try_from(scale).expect("non-negative scale");
    let value = if scale == 0 {
        digits
    } else if digits.len() <= scale {
        format!("0.{}{}", "0".repeat(scale - digits.len()), digits)
    } else {
        let split = digits.len() - scale;
        format!("{}.{}", &digits[..split], &digits[split..])
    };
    Ok(if negative { format!("-{value}") } else { value })
}

fn format_date(days_since_epoch: i32) -> Result<String, String> {
    const UNIX_EPOCH_DAY_OFFSET: i32 = 719_163;
    let ce_days = UNIX_EPOCH_DAY_OFFSET
        .checked_add(days_since_epoch)
        .ok_or_else(|| format!("write-default date value {days_since_epoch} is out of range"))?;
    chrono::NaiveDate::from_num_days_from_ce_opt(ce_days)
        .map(|date| date.format("%Y-%m-%d").to_string())
        .ok_or_else(|| format!("write-default date value {days_since_epoch} is out of range"))
}

fn format_timestamp_micros(value: i64) -> Result<String, String> {
    chrono::DateTime::from_timestamp_micros(value)
        .map(|datetime| datetime.naive_utc().format("%Y-%m-%d %H:%M:%S").to_string())
        .ok_or_else(|| format!("write-default datetime value {value} is out of range"))
}

fn format_timestamp_nanos(value: i64) -> String {
    chrono::DateTime::from_timestamp_nanos(value)
        .naive_utc()
        .format("%Y-%m-%d %H:%M:%S%.9f")
        .to_string()
}

fn bytes_to_latin1(bytes: &[u8]) -> String {
    bytes.iter().copied().map(char::from).collect()
}
