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

use arrow::datatypes::{DataType, TimeUnit};
use novarocks::engine::insert_engine::InsertValue;
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
