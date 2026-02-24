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

use arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Decimal256Array,
    FixedSizeBinaryArray, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array,
    Int64Array, LargeListArray, ListArray, MapArray, RecordBatch, StringArray, StructArray,
    TimestampMicrosecondArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow::compute::cast;
use arrow::datatypes::Schema;
use arrow_buffer::i256;
use chrono::{DateTime, Datelike, NaiveDate, NaiveTime};
use crc32c::crc32c;
use prost::Message;
use std::collections::HashMap;
use std::sync::Arc;

use crate::service::grpc_client::proto::starrocks::{ColumnPb, TabletSchemaPb};

const SEGMENT_TRAILER_MAGIC: &[u8; 4] = b"D0R1";
const PAGE_TYPE_DATA: i32 = 1;
const PAGE_TYPE_SHORT_KEY: i32 = 4;
const ENCODING_PLAIN: i32 = 2;
const COMPRESSION_LZ4_FRAME: i32 = 5;
const NULL_ENCODING_LZ4: i32 = 1;
const LOGICAL_TYPE_TINYINT: i32 = 1;
const LOGICAL_TYPE_SMALLINT: i32 = 3;
const LOGICAL_TYPE_INT: i32 = 5;
const LOGICAL_TYPE_BIGINT: i32 = 7;
const LOGICAL_TYPE_LARGEINT: i32 = 9;
const LOGICAL_TYPE_FLOAT: i32 = 10;
const LOGICAL_TYPE_DOUBLE: i32 = 11;
const LOGICAL_TYPE_VARCHAR: i32 = 17;
const LOGICAL_TYPE_STRUCT: i32 = 18;
const LOGICAL_TYPE_ARRAY: i32 = 19;
const LOGICAL_TYPE_MAP: i32 = 20;
const LOGICAL_TYPE_HLL: i32 = 23;
const LOGICAL_TYPE_OBJECT: i32 = 25;
const LOGICAL_TYPE_DECIMAL256: i32 = 26;
const LOGICAL_TYPE_BOOLEAN: i32 = 24;
const LOGICAL_TYPE_BINARY: i32 = 45;
const LOGICAL_TYPE_VARBINARY: i32 = 46;
const LOGICAL_TYPE_DECIMAL32: i32 = 47;
const LOGICAL_TYPE_DECIMAL64: i32 = 48;
const LOGICAL_TYPE_DECIMAL128: i32 = 49;
const LOGICAL_TYPE_DATE: i32 = 50;
const LOGICAL_TYPE_DATETIME: i32 = 51;
const COLUMN_INDEX_TYPE_ORDINAL_INDEX: i32 = 1;
const COLUMN_INDEX_TYPE_ZONE_MAP_INDEX: i32 = 2;
const SHORT_KEY_NULL_FIRST_MARKER: u8 = 0x01;
const SHORT_KEY_NORMAL_MARKER: u8 = 0x02;
const DEFAULT_SHORT_KEY_NUM_ROWS_PER_BLOCK: i32 = 1024;
const DATE_UNIX_EPOCH_JULIAN: i64 = 2_440_588;
const DATE32_UNIX_EPOCH_DAY_OFFSET: i32 = 719_163; // 1970-01-01 in proleptic Gregorian days
const TIMESTAMP_BITS: u32 = 40;
const TIMESTAMP_TIME_MASK: u64 = (1_u64 << TIMESTAMP_BITS) - 1;
const USECS_PER_DAY_I64: i64 = 86_400_000_000;

fn standardize_date_literal_for_native_writer(value: i64) -> Option<i64> {
    const YY_PART_YEAR: i64 = 70;
    if value <= 0 {
        return None;
    }
    if value >= 10000101000000 {
        if value > 99999999999999 {
            return None;
        }
        return Some(value);
    }
    if value < 101 {
        return None;
    }
    if value <= (YY_PART_YEAR - 1) * 10000 + 1231 {
        return Some((value + 20000000) * 1000000);
    }
    if value < YY_PART_YEAR * 10000 + 101 {
        return None;
    }
    if value <= 991231 {
        return Some((value + 19000000) * 1000000);
    }
    if value < 10000101 {
        return None;
    }
    if value <= 99991231 {
        return Some(value * 1000000);
    }
    if value < 101000000 {
        return None;
    }
    if value <= (YY_PART_YEAR - 1) * 10000000000 + 1231235959 {
        return Some(value + 20000000000000);
    }
    if value < YY_PART_YEAR * 10000000000 + 101000000 {
        return None;
    }
    if value <= 991231235959 {
        return Some(value + 19000000000000);
    }
    Some(value)
}

fn date_literal_to_date32_for_native_writer(value: i64) -> Option<i32> {
    let standardized = standardize_date_literal_for_native_writer(value)?;
    let date_part = standardized / 1_000_000;
    let time_part = standardized % 1_000_000;
    let year = (date_part / 10000) as i32;
    let month = ((date_part / 100) % 100) as u32;
    let day = (date_part % 100) as u32;
    let hour = (time_part / 10000) as i32;
    let minute = ((time_part / 100) % 100) as i32;
    let second = (time_part % 100) as i32;
    if hour > 23 || minute > 59 || second > 59 {
        return None;
    }
    let date = NaiveDate::from_ymd_opt(year, month, day)?;
    Some(date.num_days_from_ce() - DATE32_UNIX_EPOCH_DAY_OFFSET)
}

fn datetime_literal_to_micros_for_native_writer(value: i64) -> Option<i64> {
    let standardized = standardize_date_literal_for_native_writer(value)?;
    let date_part = standardized / 1_000_000;
    let time_part = standardized % 1_000_000;
    let year = (date_part / 10000) as i32;
    let month = ((date_part / 100) % 100) as u32;
    let day = (date_part % 100) as u32;
    let hour = (time_part / 10000) as u32;
    let minute = ((time_part / 100) % 100) as u32;
    let second = (time_part % 100) as u32;
    let date = NaiveDate::from_ymd_opt(year, month, day)?;
    let time = NaiveTime::from_hms_opt(hour, minute, second)?;
    Some(date.and_time(time).and_utc().timestamp_micros())
}

fn cast_column_to_date32_for_native_writer(
    column: &ArrayRef,
    column_index: usize,
    column_name: &str,
    context: &str,
) -> Result<ArrayRef, String> {
    if let Some(fixed) = column.as_any().downcast_ref::<FixedSizeBinaryArray>()
        && fixed.value_length() == crate::common::largeint::LARGEINT_BYTE_WIDTH
    {
        let values = decode_largeint_values(column, column_index, column_name, context)?;
        let mut out = Vec::with_capacity(values.len());
        for value in values {
            let days = value
                .and_then(|v| i64::try_from(v).ok())
                .and_then(date_literal_to_date32_for_native_writer);
            out.push(days);
        }
        return Ok(Arc::new(Date32Array::from(out)) as ArrayRef);
    }
    cast(column.as_ref(), &arrow::datatypes::DataType::Date32).map_err(|e| {
        format!(
            "cast column to Date32 failed for {}: column_index={}, column_name={}, error={}",
            context, column_index, column_name, e
        )
    })
}

fn cast_column_to_timestamp_micros_for_native_writer(
    column: &ArrayRef,
    column_index: usize,
    column_name: &str,
    context: &str,
) -> Result<ArrayRef, String> {
    if let Some(fixed) = column.as_any().downcast_ref::<FixedSizeBinaryArray>()
        && fixed.value_length() == crate::common::largeint::LARGEINT_BYTE_WIDTH
    {
        let values = decode_largeint_values(column, column_index, column_name, context)?;
        let mut out = Vec::with_capacity(values.len());
        for value in values {
            let micros = value
                .and_then(|v| i64::try_from(v).ok())
                .and_then(datetime_literal_to_micros_for_native_writer);
            out.push(micros);
        }
        return Ok(Arc::new(TimestampMicrosecondArray::from(out)) as ArrayRef);
    }
    cast(
        column.as_ref(),
        &arrow::datatypes::DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
    )
    .map_err(|e| {
        format!(
            "cast column to Timestamp(Microsecond,None) failed for {}: column_index={}, column_name={}, error={}",
            context, column_index, column_name, e
        )
    })
}

fn decimal128_to_i128_literal_for_native_writer(value: i128, scale: i8) -> Option<i128> {
    if scale == 0 {
        return Some(value);
    }
    if scale > 0 {
        let factor = 10_i128.checked_pow(scale as u32)?;
        return Some(value / factor);
    }
    let factor = 10_i128.checked_pow((-scale) as u32)?;
    value.checked_mul(factor)
}

fn decimal128_to_f64_literal_for_native_writer(value: i128, scale: i8) -> Option<f64> {
    let mut out = value as f64;
    if scale > 0 {
        out /= 10_f64.powi(scale as i32);
    } else if scale < 0 {
        out *= 10_f64.powi((-scale) as i32);
    }
    Some(out)
}

fn numeric_values_as_i128_for_native_writer(
    column: &ArrayRef,
    column_index: usize,
    column_name: &str,
    context: &str,
) -> Result<Option<Vec<Option<i128>>>, String> {
    let values =
        match column.data_type() {
            arrow::datatypes::DataType::Boolean => {
                let typed = column
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| {
                    format!(
                        "downcast Boolean column failed for {}: column_index={}, column_name={}",
                        context, column_index, column_name
                    )
                })?;
                let mut out = Vec::with_capacity(typed.len());
                for row in 0..typed.len() {
                    out.push(if typed.is_null(row) {
                        None
                    } else if typed.value(row) {
                        Some(1)
                    } else {
                        Some(0)
                    });
                }
                out
            }
            arrow::datatypes::DataType::Int8 => {
                let typed = column.as_any().downcast_ref::<Int8Array>().ok_or_else(|| {
                    format!(
                        "downcast Int8 column failed for {}: column_index={}, column_name={}",
                        context, column_index, column_name
                    )
                })?;
                let mut out = Vec::with_capacity(typed.len());
                for row in 0..typed.len() {
                    out.push(if typed.is_null(row) {
                        None
                    } else {
                        Some(i128::from(typed.value(row)))
                    });
                }
                out
            }
            arrow::datatypes::DataType::Int16 => {
                let typed = column
                    .as_any()
                    .downcast_ref::<Int16Array>()
                    .ok_or_else(|| {
                        format!(
                            "downcast Int16 column failed for {}: column_index={}, column_name={}",
                            context, column_index, column_name
                        )
                    })?;
                let mut out = Vec::with_capacity(typed.len());
                for row in 0..typed.len() {
                    out.push(if typed.is_null(row) {
                        None
                    } else {
                        Some(i128::from(typed.value(row)))
                    });
                }
                out
            }
            arrow::datatypes::DataType::Int32 => {
                let typed = column
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .ok_or_else(|| {
                        format!(
                            "downcast Int32 column failed for {}: column_index={}, column_name={}",
                            context, column_index, column_name
                        )
                    })?;
                let mut out = Vec::with_capacity(typed.len());
                for row in 0..typed.len() {
                    out.push(if typed.is_null(row) {
                        None
                    } else {
                        Some(i128::from(typed.value(row)))
                    });
                }
                out
            }
            arrow::datatypes::DataType::Int64 => {
                let typed = column
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| {
                        format!(
                            "downcast Int64 column failed for {}: column_index={}, column_name={}",
                            context, column_index, column_name
                        )
                    })?;
                let mut out = Vec::with_capacity(typed.len());
                for row in 0..typed.len() {
                    out.push(if typed.is_null(row) {
                        None
                    } else {
                        Some(i128::from(typed.value(row)))
                    });
                }
                out
            }
            arrow::datatypes::DataType::UInt8 => {
                let typed = column
                    .as_any()
                    .downcast_ref::<UInt8Array>()
                    .ok_or_else(|| {
                        format!(
                            "downcast UInt8 column failed for {}: column_index={}, column_name={}",
                            context, column_index, column_name
                        )
                    })?;
                let mut out = Vec::with_capacity(typed.len());
                for row in 0..typed.len() {
                    out.push(if typed.is_null(row) {
                        None
                    } else {
                        Some(i128::from(typed.value(row)))
                    });
                }
                out
            }
            arrow::datatypes::DataType::UInt16 => {
                let typed = column
                    .as_any()
                    .downcast_ref::<UInt16Array>()
                    .ok_or_else(|| {
                        format!(
                            "downcast UInt16 column failed for {}: column_index={}, column_name={}",
                            context, column_index, column_name
                        )
                    })?;
                let mut out = Vec::with_capacity(typed.len());
                for row in 0..typed.len() {
                    out.push(if typed.is_null(row) {
                        None
                    } else {
                        Some(i128::from(typed.value(row)))
                    });
                }
                out
            }
            arrow::datatypes::DataType::UInt32 => {
                let typed = column
                    .as_any()
                    .downcast_ref::<UInt32Array>()
                    .ok_or_else(|| {
                        format!(
                            "downcast UInt32 column failed for {}: column_index={}, column_name={}",
                            context, column_index, column_name
                        )
                    })?;
                let mut out = Vec::with_capacity(typed.len());
                for row in 0..typed.len() {
                    out.push(if typed.is_null(row) {
                        None
                    } else {
                        Some(i128::from(typed.value(row)))
                    });
                }
                out
            }
            arrow::datatypes::DataType::UInt64 => {
                let typed = column
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .ok_or_else(|| {
                        format!(
                            "downcast UInt64 column failed for {}: column_index={}, column_name={}",
                            context, column_index, column_name
                        )
                    })?;
                let mut out = Vec::with_capacity(typed.len());
                for row in 0..typed.len() {
                    out.push(if typed.is_null(row) {
                        None
                    } else {
                        Some(i128::from(typed.value(row)))
                    });
                }
                out
            }
            arrow::datatypes::DataType::Float32 => {
                let typed = column.as_any().downcast_ref::<Float32Array>().ok_or_else(|| {
                format!(
                    "downcast Float32 column failed for {}: column_index={}, column_name={}",
                    context, column_index, column_name
                )
            })?;
                let mut out = Vec::with_capacity(typed.len());
                for row in 0..typed.len() {
                    let value = if typed.is_null(row) {
                        None
                    } else {
                        let raw = typed.value(row);
                        if !raw.is_finite() || raw < i128::MIN as f32 || raw > i128::MAX as f32 {
                            None
                        } else {
                            Some(raw.trunc() as i128)
                        }
                    };
                    out.push(value);
                }
                out
            }
            arrow::datatypes::DataType::Float64 => {
                let typed = column.as_any().downcast_ref::<Float64Array>().ok_or_else(|| {
                format!(
                    "downcast Float64 column failed for {}: column_index={}, column_name={}",
                    context, column_index, column_name
                )
            })?;
                let mut out = Vec::with_capacity(typed.len());
                for row in 0..typed.len() {
                    let value = if typed.is_null(row) {
                        None
                    } else {
                        let raw = typed.value(row);
                        if !raw.is_finite() || raw < i128::MIN as f64 || raw > i128::MAX as f64 {
                            None
                        } else {
                            Some(raw.trunc() as i128)
                        }
                    };
                    out.push(value);
                }
                out
            }
            arrow::datatypes::DataType::Decimal128(_, scale) => {
                let typed = column
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .ok_or_else(|| {
                    format!(
                        "downcast Decimal128 column failed for {}: column_index={}, column_name={}",
                        context, column_index, column_name
                    )
                })?;
                let mut out = Vec::with_capacity(typed.len());
                for row in 0..typed.len() {
                    out.push(if typed.is_null(row) {
                        None
                    } else {
                        decimal128_to_i128_literal_for_native_writer(typed.value(row), *scale)
                    });
                }
                out
            }
            arrow::datatypes::DataType::FixedSizeBinary(width)
                if *width == crate::common::largeint::LARGEINT_BYTE_WIDTH =>
            {
                decode_largeint_values(column, column_index, column_name, context)?
            }
            _ => return Ok(None),
        };
    Ok(Some(values))
}

fn numeric_values_as_f64_for_native_writer(
    column: &ArrayRef,
    column_index: usize,
    column_name: &str,
    context: &str,
) -> Result<Option<Vec<Option<f64>>>, String> {
    let values = match column.data_type() {
        arrow::datatypes::DataType::Boolean => {
            let typed = column
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| {
                    format!(
                        "downcast Boolean column failed for {}: column_index={}, column_name={}",
                        context, column_index, column_name
                    )
                })?;
            let mut out = Vec::with_capacity(typed.len());
            for row in 0..typed.len() {
                out.push(if typed.is_null(row) {
                    None
                } else if typed.value(row) {
                    Some(1.0)
                } else {
                    Some(0.0)
                });
            }
            out
        }
        arrow::datatypes::DataType::Int8 => {
            let typed = column.as_any().downcast_ref::<Int8Array>().ok_or_else(|| {
                format!(
                    "downcast Int8 column failed for {}: column_index={}, column_name={}",
                    context, column_index, column_name
                )
            })?;
            let mut out = Vec::with_capacity(typed.len());
            for row in 0..typed.len() {
                out.push((!typed.is_null(row)).then(|| typed.value(row) as f64));
            }
            out
        }
        arrow::datatypes::DataType::Int16 => {
            let typed = column
                .as_any()
                .downcast_ref::<Int16Array>()
                .ok_or_else(|| {
                    format!(
                        "downcast Int16 column failed for {}: column_index={}, column_name={}",
                        context, column_index, column_name
                    )
                })?;
            let mut out = Vec::with_capacity(typed.len());
            for row in 0..typed.len() {
                out.push((!typed.is_null(row)).then(|| typed.value(row) as f64));
            }
            out
        }
        arrow::datatypes::DataType::Int32 => {
            let typed = column
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| {
                    format!(
                        "downcast Int32 column failed for {}: column_index={}, column_name={}",
                        context, column_index, column_name
                    )
                })?;
            let mut out = Vec::with_capacity(typed.len());
            for row in 0..typed.len() {
                out.push((!typed.is_null(row)).then(|| typed.value(row) as f64));
            }
            out
        }
        arrow::datatypes::DataType::Int64 => {
            let typed = column
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| {
                    format!(
                        "downcast Int64 column failed for {}: column_index={}, column_name={}",
                        context, column_index, column_name
                    )
                })?;
            let mut out = Vec::with_capacity(typed.len());
            for row in 0..typed.len() {
                out.push((!typed.is_null(row)).then(|| typed.value(row) as f64));
            }
            out
        }
        arrow::datatypes::DataType::UInt8 => {
            let typed = column
                .as_any()
                .downcast_ref::<UInt8Array>()
                .ok_or_else(|| {
                    format!(
                        "downcast UInt8 column failed for {}: column_index={}, column_name={}",
                        context, column_index, column_name
                    )
                })?;
            let mut out = Vec::with_capacity(typed.len());
            for row in 0..typed.len() {
                out.push((!typed.is_null(row)).then(|| typed.value(row) as f64));
            }
            out
        }
        arrow::datatypes::DataType::UInt16 => {
            let typed = column
                .as_any()
                .downcast_ref::<UInt16Array>()
                .ok_or_else(|| {
                    format!(
                        "downcast UInt16 column failed for {}: column_index={}, column_name={}",
                        context, column_index, column_name
                    )
                })?;
            let mut out = Vec::with_capacity(typed.len());
            for row in 0..typed.len() {
                out.push((!typed.is_null(row)).then(|| typed.value(row) as f64));
            }
            out
        }
        arrow::datatypes::DataType::UInt32 => {
            let typed = column
                .as_any()
                .downcast_ref::<UInt32Array>()
                .ok_or_else(|| {
                    format!(
                        "downcast UInt32 column failed for {}: column_index={}, column_name={}",
                        context, column_index, column_name
                    )
                })?;
            let mut out = Vec::with_capacity(typed.len());
            for row in 0..typed.len() {
                out.push((!typed.is_null(row)).then(|| typed.value(row) as f64));
            }
            out
        }
        arrow::datatypes::DataType::UInt64 => {
            let typed = column
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or_else(|| {
                    format!(
                        "downcast UInt64 column failed for {}: column_index={}, column_name={}",
                        context, column_index, column_name
                    )
                })?;
            let mut out = Vec::with_capacity(typed.len());
            for row in 0..typed.len() {
                out.push((!typed.is_null(row)).then(|| typed.value(row) as f64));
            }
            out
        }
        arrow::datatypes::DataType::Float32 => {
            let typed = column
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| {
                    format!(
                        "downcast Float32 column failed for {}: column_index={}, column_name={}",
                        context, column_index, column_name
                    )
                })?;
            let mut out = Vec::with_capacity(typed.len());
            for row in 0..typed.len() {
                out.push((!typed.is_null(row)).then(|| typed.value(row) as f64));
            }
            out
        }
        arrow::datatypes::DataType::Float64 => {
            let typed = column
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| {
                    format!(
                        "downcast Float64 column failed for {}: column_index={}, column_name={}",
                        context, column_index, column_name
                    )
                })?;
            let mut out = Vec::with_capacity(typed.len());
            for row in 0..typed.len() {
                out.push((!typed.is_null(row)).then(|| typed.value(row)));
            }
            out
        }
        arrow::datatypes::DataType::Decimal128(_, scale) => {
            let typed = column
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .ok_or_else(|| {
                    format!(
                        "downcast Decimal128 column failed for {}: column_index={}, column_name={}",
                        context, column_index, column_name
                    )
                })?;
            let mut out = Vec::with_capacity(typed.len());
            for row in 0..typed.len() {
                out.push(if typed.is_null(row) {
                    None
                } else {
                    decimal128_to_f64_literal_for_native_writer(typed.value(row), *scale)
                });
            }
            out
        }
        arrow::datatypes::DataType::FixedSizeBinary(width)
            if *width == crate::common::largeint::LARGEINT_BYTE_WIDTH =>
        {
            let values = decode_largeint_values(column, column_index, column_name, context)?;
            let mut out = Vec::with_capacity(values.len());
            for value in values {
                out.push(value.map(|v| v as f64));
            }
            out
        }
        _ => return Ok(None),
    };
    Ok(Some(values))
}

fn cast_column_to_int8_for_native_writer(
    column: &ArrayRef,
    column_index: usize,
    column_name: &str,
    context: &str,
) -> Result<ArrayRef, String> {
    if let Some(values) =
        numeric_values_as_i128_for_native_writer(column, column_index, column_name, context)?
    {
        let mut out = Vec::with_capacity(values.len());
        for value in values.iter().copied() {
            out.push(value.and_then(|v| i8::try_from(v).ok()));
        }
        return Ok(Arc::new(Int8Array::from(out)) as ArrayRef);
    }
    cast(column.as_ref(), &arrow::datatypes::DataType::Int8).map_err(|e| {
        format!(
            "cast column to Int8 failed for {}: column_index={}, column_name={}, error={}",
            context, column_index, column_name, e
        )
    })
}

fn cast_column_to_int16_for_native_writer(
    column: &ArrayRef,
    column_index: usize,
    column_name: &str,
    context: &str,
) -> Result<ArrayRef, String> {
    if let Some(values) =
        numeric_values_as_i128_for_native_writer(column, column_index, column_name, context)?
    {
        let mut out = Vec::with_capacity(values.len());
        for value in values {
            out.push(value.and_then(|v| i16::try_from(v).ok()));
        }
        return Ok(Arc::new(Int16Array::from(out)) as ArrayRef);
    }
    cast(column.as_ref(), &arrow::datatypes::DataType::Int16).map_err(|e| {
        format!(
            "cast column to Int16 failed for {}: column_index={}, column_name={}, error={}",
            context, column_index, column_name, e
        )
    })
}

fn cast_column_to_int32_for_native_writer(
    column: &ArrayRef,
    column_index: usize,
    column_name: &str,
    context: &str,
) -> Result<ArrayRef, String> {
    if let Some(values) =
        numeric_values_as_i128_for_native_writer(column, column_index, column_name, context)?
    {
        let mut out = Vec::with_capacity(values.len());
        for value in values {
            out.push(value.and_then(|v| i32::try_from(v).ok()));
        }
        return Ok(Arc::new(Int32Array::from(out)) as ArrayRef);
    }
    cast(column.as_ref(), &arrow::datatypes::DataType::Int32).map_err(|e| {
        format!(
            "cast column to Int32 failed for {}: column_index={}, column_name={}, error={}",
            context, column_index, column_name, e
        )
    })
}

fn cast_column_to_int64_for_native_writer(
    column: &ArrayRef,
    column_index: usize,
    column_name: &str,
    context: &str,
) -> Result<ArrayRef, String> {
    if let Some(values) =
        numeric_values_as_i128_for_native_writer(column, column_index, column_name, context)?
    {
        let mut out = Vec::with_capacity(values.len());
        for value in values {
            out.push(value.and_then(|v| i64::try_from(v).ok()));
        }
        return Ok(Arc::new(Int64Array::from(out)) as ArrayRef);
    }
    cast(column.as_ref(), &arrow::datatypes::DataType::Int64).map_err(|e| {
        format!(
            "cast column to Int64 failed for {}: column_index={}, column_name={}, error={}",
            context, column_index, column_name, e
        )
    })
}

fn cast_column_to_float32_for_native_writer(
    column: &ArrayRef,
    column_index: usize,
    column_name: &str,
    context: &str,
) -> Result<ArrayRef, String> {
    if let Some(values) =
        numeric_values_as_f64_for_native_writer(column, column_index, column_name, context)?
    {
        let mut out = Vec::with_capacity(values.len());
        for value in values {
            let converted = value.map(|v| v as f32);
            out.push(converted);
        }
        return Ok(Arc::new(Float32Array::from(out)) as ArrayRef);
    }
    cast(column.as_ref(), &arrow::datatypes::DataType::Float32).map_err(|e| {
        format!(
            "cast column to Float32 failed for {}: column_index={}, column_name={}, error={}",
            context, column_index, column_name, e
        )
    })
}

fn cast_column_to_float64_for_native_writer(
    column: &ArrayRef,
    column_index: usize,
    column_name: &str,
    context: &str,
) -> Result<ArrayRef, String> {
    if let Some(values) =
        numeric_values_as_f64_for_native_writer(column, column_index, column_name, context)?
    {
        let mut out = Vec::with_capacity(values.len());
        for value in values {
            out.push(value);
        }
        return Ok(Arc::new(Float64Array::from(out)) as ArrayRef);
    }
    cast(column.as_ref(), &arrow::datatypes::DataType::Float64).map_err(|e| {
        format!(
            "cast column to Float64 failed for {}: column_index={}, column_name={}, error={}",
            context, column_index, column_name, e
        )
    })
}

fn cast_column_to_boolean_for_native_writer(
    column: &ArrayRef,
    column_index: usize,
    column_name: &str,
    context: &str,
) -> Result<ArrayRef, String> {
    if let Some(values) =
        numeric_values_as_i128_for_native_writer(column, column_index, column_name, context)?
    {
        let mut out = Vec::with_capacity(values.len());
        for value in values {
            out.push(value.map(|v| v != 0));
        }
        return Ok(Arc::new(BooleanArray::from(out)) as ArrayRef);
    }
    cast(column.as_ref(), &arrow::datatypes::DataType::Boolean).map_err(|e| {
        format!(
            "cast column to Boolean failed for {}: column_index={}, column_name={}, error={}",
            context, column_index, column_name, e
        )
    })
}

fn is_positional_generated_field_name(field_name: &str, index: usize) -> bool {
    field_name
        .strip_prefix("col_")
        .and_then(|suffix| suffix.parse::<usize>().ok())
        .is_some_and(|generated_index| generated_index == index)
}

fn align_batch_columns_to_schema_for_native_writer(
    batch: &RecordBatch,
    tablet_schema: &TabletSchemaPb,
) -> Result<RecordBatch, String> {
    if tablet_schema.column.is_empty() {
        return Ok(batch.clone());
    }
    if batch.num_columns() < tablet_schema.column.len() {
        return Err(format!(
            "batch/schema column mismatch for native segment writer: batch_columns={} schema_columns={}",
            batch.num_columns(),
            tablet_schema.column.len()
        ));
    }

    let mut name_to_batch_idx = HashMap::new();
    for (idx, field) in batch.schema().fields().iter().enumerate() {
        name_to_batch_idx.insert(field.name().to_ascii_lowercase(), idx);
    }

    let mut selected_indices = Vec::with_capacity(tablet_schema.column.len());
    for (schema_idx, schema_col) in tablet_schema.column.iter().enumerate() {
        let schema_name = schema_col
            .name
            .as_deref()
            .unwrap_or("")
            .trim()
            .to_ascii_lowercase();

        let batch_schema = batch.schema();
        let indexed_field = batch_schema.fields().get(schema_idx);
        let index_matches_name =
            indexed_field.is_some_and(|field| field.name().eq_ignore_ascii_case(&schema_name));
        let index_is_generated = indexed_field
            .is_some_and(|field| is_positional_generated_field_name(field.name(), schema_idx));

        let batch_idx = if index_matches_name || schema_name.is_empty() || index_is_generated {
            schema_idx
        } else if let Some(idx) = name_to_batch_idx.get(&schema_name) {
            *idx
        } else if schema_idx < batch.num_columns() {
            schema_idx
        } else {
            return Err(format!(
                "schema column not found in batch for native segment writer: schema_index={}, schema_name={}",
                schema_idx, schema_name
            ));
        };
        selected_indices.push(batch_idx);
    }

    let identity = selected_indices
        .iter()
        .enumerate()
        .all(|(idx, selected)| idx == *selected);
    if identity {
        return Ok(batch.clone());
    }

    let mut aligned_columns = Vec::with_capacity(selected_indices.len());
    let mut aligned_fields = Vec::with_capacity(selected_indices.len());
    for idx in selected_indices {
        aligned_columns.push(batch.column(idx).clone());
        aligned_fields.push(batch.schema().field(idx).clone());
    }

    let aligned_schema = Arc::new(Schema::new_with_metadata(
        aligned_fields,
        batch.schema().metadata().clone(),
    ));
    RecordBatch::try_new(aligned_schema, aligned_columns).map_err(|e| {
        format!("build schema-aligned record batch for native segment writer failed: {e}")
    })
}
pub fn build_starrocks_native_segment_bytes(
    batch: &RecordBatch,
    tablet_schema: &TabletSchemaPb,
) -> Result<Vec<u8>, String> {
    let batch = align_batch_columns_to_schema_for_native_writer(batch, tablet_schema)?;
    if batch.num_rows() == 0 {
        return Err("cannot encode empty batch into starrocks native segment".to_string());
    }
    if batch.num_columns() != tablet_schema.column.len() {
        return Err(format!(
            "batch/schema column mismatch for native segment writer: batch_columns={} schema_columns={}",
            batch.num_columns(),
            tablet_schema.column.len()
        ));
    }

    let mut segment_payload = Vec::new();
    let mut column_metas = Vec::with_capacity(batch.num_columns());
    let num_rows_u32 = u32::try_from(batch.num_rows())
        .map_err(|_| format!("native segment row count overflow: {}", batch.num_rows()))?;

    for (idx, schema_column) in tablet_schema.column.iter().enumerate() {
        let root_unique_id = u32::try_from(schema_column.unique_id).map_err(|_| {
            format!(
                "invalid column unique_id for native segment writer: column_index={}, unique_id={}",
                idx, schema_column.unique_id
            )
        })?;
        let column_id = u32::try_from(idx)
            .map_err(|_| format!("column index overflow for native segment writer: {}", idx))?;
        let column = batch.column(idx).clone();
        let schema_column_name = schema_column.name.as_deref().unwrap_or("<unknown>");
        let batch_field_name = batch.schema().field(idx).name().to_string();
        let column_name = if batch_field_name.eq_ignore_ascii_case(schema_column_name) {
            schema_column_name.to_string()
        } else {
            format!("{}(batch_field={})", schema_column_name, batch_field_name)
        };
        let column_meta = build_native_column_meta_recursive(
            &column,
            schema_column,
            column_id,
            root_unique_id,
            idx,
            column_name.as_str(),
            &mut segment_payload,
        )?;
        column_metas.push(column_meta);
    }

    let short_key_page = build_short_key_index_page(&batch, tablet_schema)?;
    let short_key_index_page = if let Some(page) = short_key_page {
        let offset = u64::try_from(segment_payload.len())
            .map_err(|_| "short key page offset overflow".to_string())?;
        let size = u32::try_from(page.len())
            .map_err(|_| format!("short key page size overflow: {}", page.len()))?;
        segment_payload.extend_from_slice(&page);
        Some(PagePointerWriterPb {
            offset: Some(offset),
            size: Some(size),
        })
    } else {
        None
    };

    let segment_footer = SegmentFooterWriterPb {
        version: Some(1),
        columns: column_metas,
        num_rows: Some(num_rows_u32),
        short_key_index_page,
    };
    let footer_bytes = segment_footer.encode_to_vec();
    let footer_size = u32::try_from(footer_bytes.len()).map_err(|_| {
        format!(
            "native segment footer size overflow: {}",
            footer_bytes.len()
        )
    })?;
    let footer_checksum = crc32c(&footer_bytes);

    let mut out = Vec::with_capacity(
        segment_payload
            .len()
            .saturating_add(footer_bytes.len())
            .saturating_add(12),
    );
    out.extend_from_slice(&segment_payload);
    out.extend_from_slice(&footer_bytes);
    out.extend_from_slice(&footer_size.to_le_bytes());
    out.extend_from_slice(&footer_checksum.to_le_bytes());
    out.extend_from_slice(SEGMENT_TRAILER_MAGIC);
    Ok(out)
}

fn build_short_key_index_page(
    batch: &RecordBatch,
    tablet_schema: &TabletSchemaPb,
) -> Result<Option<Vec<u8>>, String> {
    let short_key_indexes = resolve_short_key_indexes(tablet_schema, batch.num_columns())?;
    if short_key_indexes.is_empty() {
        return Ok(None);
    }

    let num_rows_per_block = tablet_schema
        .num_rows_per_row_block
        .unwrap_or(DEFAULT_SHORT_KEY_NUM_ROWS_PER_BLOCK)
        .max(1);
    let num_rows_per_block_usize = usize::try_from(num_rows_per_block).map_err(|_| {
        format!(
            "num_rows_per_row_block overflow for short key page: {}",
            num_rows_per_block
        )
    })?;

    let mut key_buf = Vec::new();
    let mut offset_buf = Vec::new();
    let mut num_items: u32 = 0;
    for row_idx in (0..batch.num_rows()).step_by(num_rows_per_block_usize) {
        let offset = u32::try_from(key_buf.len()).map_err(|_| {
            format!(
                "short key offset overflow before row {}: {}",
                row_idx,
                key_buf.len()
            )
        })?;
        append_varint_u32(&mut offset_buf, offset);
        let key_bytes =
            encode_short_key_for_row(batch, tablet_schema, &short_key_indexes, row_idx)?;
        key_buf.extend_from_slice(&key_bytes);
        num_items = num_items.saturating_add(1);
    }
    if num_items == 0 {
        return Ok(None);
    }

    let mut body = Vec::with_capacity(key_buf.len().saturating_add(offset_buf.len()));
    body.extend_from_slice(&key_buf);
    body.extend_from_slice(&offset_buf);
    let key_bytes = u32::try_from(key_buf.len())
        .map_err(|_| format!("short key key-bytes overflow: {}", key_buf.len()))?;
    let offset_bytes = u32::try_from(offset_buf.len())
        .map_err(|_| format!("short key offset-bytes overflow: {}", offset_buf.len()))?;
    let num_segment_rows = u32::try_from(batch.num_rows())
        .map_err(|_| format!("short key num_segment_rows overflow: {}", batch.num_rows()))?;
    let page_footer = PageFooterWriterPb {
        r#type: Some(PAGE_TYPE_SHORT_KEY),
        uncompressed_size: Some(
            u32::try_from(body.len())
                .map_err(|_| format!("short key page body too large: {}", body.len()))?,
        ),
        data_page_footer: None,
        short_key_page_footer: Some(ShortKeyFooterWriterPb {
            num_items: Some(num_items),
            key_bytes: Some(key_bytes),
            offset_bytes: Some(offset_bytes),
            segment_id: Some(0),
            num_rows_per_block: Some(u32::try_from(num_rows_per_block).map_err(|_| {
                format!(
                    "short key num_rows_per_block overflow: {}",
                    num_rows_per_block
                )
            })?),
            num_segment_rows: Some(num_segment_rows),
        }),
    };
    encode_page_with_footer(body, page_footer).map(Some)
}

fn resolve_short_key_indexes(
    tablet_schema: &TabletSchemaPb,
    num_columns: usize,
) -> Result<Vec<usize>, String> {
    let short_key_count = tablet_schema.num_short_key_columns.unwrap_or(0).max(0) as usize;
    if short_key_count == 0 {
        return Ok(Vec::new());
    }
    if tablet_schema.sort_key_idxes.is_empty() {
        return Err("tablet schema missing sort_key_idxes for short key index".to_string());
    }
    let mut indexes = Vec::with_capacity(tablet_schema.sort_key_idxes.len());
    for idx in &tablet_schema.sort_key_idxes {
        let idx_usize = usize::try_from(*idx)
            .map_err(|_| format!("invalid sort_key_idx for short key index: {}", idx))?;
        if idx_usize >= num_columns {
            return Err(format!(
                "sort_key_idx out of range for short key index: idx={} num_columns={}",
                idx_usize, num_columns
            ));
        }
        indexes.push(idx_usize);
    }
    let take = short_key_count.min(indexes.len());
    Ok(indexes.into_iter().take(take).collect())
}

fn encode_short_key_for_row(
    batch: &RecordBatch,
    tablet_schema: &TabletSchemaPb,
    short_key_indexes: &[usize],
    row_idx: usize,
) -> Result<Vec<u8>, String> {
    let mut out = Vec::new();
    for col_idx in short_key_indexes {
        let schema_col = tablet_schema.column.get(*col_idx).ok_or_else(|| {
            format!(
                "short key column index out of range in tablet schema: idx={} columns={}",
                col_idx,
                tablet_schema.column.len()
            )
        })?;
        let writer_type = map_schema_type_to_writer_type(schema_col)?;
        let column = batch.column(*col_idx);
        let column_name = schema_col.name.as_deref().unwrap_or("<unknown>");
        if column.is_null(row_idx) {
            out.push(SHORT_KEY_NULL_FIRST_MARKER);
            continue;
        }
        out.push(SHORT_KEY_NORMAL_MARKER);
        let value =
            encode_short_key_value_bytes(column, row_idx, writer_type, *col_idx, column_name)?;
        let value_len = u32::try_from(value.len()).map_err(|_| {
            format!(
                "short key value too large at column_index={} row_index={} size={}",
                col_idx,
                row_idx,
                value.len()
            )
        })?;
        append_varint_u32(&mut out, value_len);
        out.extend_from_slice(&value);
    }
    Ok(out)
}

fn encode_short_key_value_bytes(
    column: &ArrayRef,
    row_idx: usize,
    writer_type: NativeWriterType,
    column_index: usize,
    column_name: &str,
) -> Result<Vec<u8>, String> {
    match writer_type {
        NativeWriterType::TinyInt => {
            let casted = cast_column_to_int8_for_native_writer(
                column,
                column_index,
                column_name,
                "short key writer",
            )?;
            let typed = casted.as_any().downcast_ref::<Int8Array>().ok_or_else(|| {
                format!(
                    "downcast short key Int8 column failed: column_index={}, column_name={}",
                    column_index, column_name
                )
            })?;
            let v = typed.value(row_idx);
            let sortable = (v as u8) ^ 0x80;
            Ok(vec![sortable])
        }
        NativeWriterType::SmallInt => {
            let casted = cast_column_to_int16_for_native_writer(
                column,
                column_index,
                column_name,
                "short key writer",
            )?;
            let typed = casted
                .as_any()
                .downcast_ref::<Int16Array>()
                .ok_or_else(|| {
                    format!(
                        "downcast short key Int16 column failed: column_index={}, column_name={}",
                        column_index, column_name
                    )
                })?;
            let v = typed.value(row_idx);
            let sortable = (v as u16) ^ 0x8000;
            Ok(sortable.to_be_bytes().to_vec())
        }
        NativeWriterType::Int => {
            let casted = cast_column_to_int32_for_native_writer(
                column,
                column_index,
                column_name,
                "short key writer",
            )?;
            let typed = casted
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| {
                    format!(
                        "downcast short key Int32 column failed: column_index={}, column_name={}",
                        column_index, column_name
                    )
                })?;
            let v = typed.value(row_idx);
            let sortable = (v as u32) ^ 0x8000_0000;
            Ok(sortable.to_be_bytes().to_vec())
        }
        NativeWriterType::Date => {
            let casted = cast_column_to_date32_for_native_writer(
                column,
                column_index,
                column_name,
                "short key writer",
            )?;
            let typed = casted
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(|| {
                    format!(
                        "downcast short key Date32 column failed: column_index={}, column_name={}",
                        column_index, column_name
                    )
                })?;
            let encoded = encode_date_storage_from_unix_days(typed.value(row_idx))?;
            let sortable = (encoded as u32) ^ 0x8000_0000;
            Ok(sortable.to_be_bytes().to_vec())
        }
        NativeWriterType::BigInt => {
            let casted = cast_column_to_int64_for_native_writer(
                column,
                column_index,
                column_name,
                "short key writer",
            )?;
            let typed = casted
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| {
                    format!(
                        "downcast short key Int64 column failed: column_index={}, column_name={}",
                        column_index, column_name
                    )
                })?;
            let v = typed.value(row_idx);
            let sortable = (v as u64) ^ 0x8000_0000_0000_0000;
            Ok(sortable.to_be_bytes().to_vec())
        }
        NativeWriterType::LargeInt => {
            let value = if let Some(typed) = column.as_any().downcast_ref::<FixedSizeBinaryArray>()
            {
                if typed.value_length() != crate::common::largeint::LARGEINT_BYTE_WIDTH {
                    return Err(format!(
                        "invalid LARGEINT fixed-size binary width for short key writer: column_index={}, expected={}, actual={}",
                        column_index,
                        crate::common::largeint::LARGEINT_BYTE_WIDTH,
                        typed.value_length()
                    ));
                }
                crate::common::largeint::i128_from_be_bytes(typed.value(row_idx)).map_err(|e| {
                    format!(
                        "decode LARGEINT fixed-size binary failed for short key writer: column_index={}, row_index={}, error={}",
                        column_index, row_idx, e
                    )
                })?
            } else {
                let casted = cast(column.as_ref(), &arrow::datatypes::DataType::Decimal128(38, 0))
                    .map_err(|e| {
                    format!(
                        "cast short key column to Decimal128(38,0) for LARGEINT failed: column_index={}, error={}",
                        column_index, e
                    )
                })?;
                let typed = casted
                    .as_any()
                    .downcast_ref::<Decimal128Array>()
                    .ok_or_else(|| {
                        format!(
                            "downcast short key Decimal128(38,0) column for LARGEINT failed: column_index={}",
                            column_index
                        )
                    })?;
                typed.value(row_idx)
            };
            let sortable = (value as u128) ^ (1_u128 << 127);
            Ok(sortable.to_be_bytes().to_vec())
        }
        NativeWriterType::Decimal32
        | NativeWriterType::Decimal64
        | NativeWriterType::Decimal128
        | NativeWriterType::Decimal256 => match writer_type {
            NativeWriterType::Decimal256 => {
                let typed = column
                    .as_any()
                    .downcast_ref::<Decimal256Array>()
                    .ok_or_else(|| {
                        format!(
                            "downcast short key Decimal256 column failed: column_index={}",
                            column_index
                        )
                    })?;
                let mut sortable = typed.value(row_idx).to_be_bytes();
                sortable[0] ^= 0x80;
                Ok(sortable.to_vec())
            }
            NativeWriterType::Decimal32
            | NativeWriterType::Decimal64
            | NativeWriterType::Decimal128 => {
                let typed = column
                    .as_any()
                    .downcast_ref::<Decimal128Array>()
                    .ok_or_else(|| {
                        format!(
                            "downcast short key Decimal128 column failed: column_index={}",
                            column_index
                        )
                    })?;
                let value = typed.value(row_idx);
                match writer_type {
                    NativeWriterType::Decimal32 => {
                        let narrowed = i32::try_from(value).map_err(|_| {
                            format!(
                                "short key decimal32 overflow: column_index={} row_index={} value={}",
                                column_index, row_idx, value
                            )
                        })?;
                        let sortable = (narrowed as u32) ^ 0x8000_0000;
                        Ok(sortable.to_be_bytes().to_vec())
                    }
                    NativeWriterType::Decimal64 => {
                        let narrowed = i64::try_from(value).map_err(|_| {
                            format!(
                                "short key decimal64 overflow: column_index={} row_index={} value={}",
                                column_index, row_idx, value
                            )
                        })?;
                        let sortable = (narrowed as u64) ^ 0x8000_0000_0000_0000;
                        Ok(sortable.to_be_bytes().to_vec())
                    }
                    NativeWriterType::Decimal128 => {
                        let sortable = (value as u128) ^ (1u128 << 127);
                        Ok(sortable.to_be_bytes().to_vec())
                    }
                    _ => unreachable!("invalid decimal short key writer type"),
                }
            }
            _ => unreachable!("invalid decimal short key writer type"),
        },
        NativeWriterType::Datetime => {
            let casted = cast_column_to_timestamp_micros_for_native_writer(
                column,
                column_index,
                column_name,
                "short key writer",
            )?;
            let typed = casted
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .ok_or_else(|| {
                    format!(
                        "downcast short key Timestamp(Microsecond,None) column failed: column_index={}, column_name={}",
                        column_index, column_name
                    )
                })?;
            let encoded = encode_datetime_storage_from_unix_micros(typed.value(row_idx))?;
            let sortable = (encoded as u64) ^ 0x8000_0000_0000_0000;
            Ok(sortable.to_be_bytes().to_vec())
        }
        NativeWriterType::Float => {
            let casted = cast_column_to_float32_for_native_writer(
                column,
                column_index,
                column_name,
                "short key writer",
            )?;
            let typed = casted
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| {
                    format!(
                        "downcast short key Float32 column failed: column_index={}, column_name={}",
                        column_index, column_name
                    )
                })?;
            let bits = typed.value(row_idx).to_bits();
            let sortable = if (bits & 0x8000_0000) != 0 {
                !bits
            } else {
                bits ^ 0x8000_0000
            };
            Ok(sortable.to_be_bytes().to_vec())
        }
        NativeWriterType::Double => {
            let casted = cast_column_to_float64_for_native_writer(
                column,
                column_index,
                column_name,
                "short key writer",
            )?;
            let typed = casted
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| {
                    format!(
                        "downcast short key Float64 column failed: column_index={}, column_name={}",
                        column_index, column_name
                    )
                })?;
            let bits = typed.value(row_idx).to_bits();
            let sortable = if (bits & 0x8000_0000_0000_0000) != 0 {
                !bits
            } else {
                bits ^ 0x8000_0000_0000_0000
            };
            Ok(sortable.to_be_bytes().to_vec())
        }
        NativeWriterType::Boolean => {
            let casted = cast_column_to_boolean_for_native_writer(
                column,
                column_index,
                column_name,
                "short key writer",
            )?;
            let typed = casted
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| {
                    format!(
                        "downcast short key Boolean column failed: column_index={}, column_name={}",
                        column_index, column_name
                    )
                })?;
            Ok(vec![u8::from(typed.value(row_idx))])
        }
        NativeWriterType::Varchar => {
            let casted = cast(column.as_ref(), &arrow::datatypes::DataType::Utf8).map_err(|e| {
                format!(
                    "cast short key column to Utf8 failed: column_index={}, error={}",
                    column_index, e
                )
            })?;
            let typed = casted
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    format!(
                        "downcast short key Utf8 column failed: column_index={}",
                        column_index
                    )
                })?;
            Ok(typed.value(row_idx).as_bytes().to_vec())
        }
        NativeWriterType::Binary
        | NativeWriterType::VarBinary
        | NativeWriterType::Hll
        | NativeWriterType::Object => {
            let casted =
                cast(column.as_ref(), &arrow::datatypes::DataType::Binary).map_err(|e| {
                    format!(
                        "cast short key column to Binary failed: column_index={}, error={}",
                        column_index, e
                    )
                })?;
            let typed = casted
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| {
                    format!(
                        "downcast short key Binary column failed: column_index={}",
                        column_index
                    )
                })?;
            Ok(typed.value(row_idx).to_vec())
        }
    }
}

fn append_varint_u32(out: &mut Vec<u8>, mut value: u32) {
    while value >= 0x80 {
        out.push((value as u8) | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
}

fn encode_date_storage_from_unix_days(unix_days: i32) -> Result<i32, String> {
    let julian = DATE_UNIX_EPOCH_JULIAN
        .checked_add(i64::from(unix_days))
        .ok_or_else(|| {
            format!(
                "date julian day overflow: unix_days={}, epoch_julian={}",
                unix_days, DATE_UNIX_EPOCH_JULIAN
            )
        })?;
    i32::try_from(julian).map_err(|_| {
        format!(
            "date julian conversion out of i32 range: unix_days={}, julian_day={}",
            unix_days, julian
        )
    })
}

fn encode_datetime_storage_from_unix_micros(unix_micros: i64) -> Result<i64, String> {
    let days_since_epoch = unix_micros.div_euclid(USECS_PER_DAY_I64);
    let micros_of_day = unix_micros.rem_euclid(USECS_PER_DAY_I64);
    if micros_of_day < 0 || micros_of_day >= USECS_PER_DAY_I64 {
        return Err(format!(
            "invalid datetime micros-of-day after conversion: unix_micros={}, micros_of_day={}",
            unix_micros, micros_of_day
        ));
    }
    let julian_day = DATE_UNIX_EPOCH_JULIAN
        .checked_add(days_since_epoch)
        .ok_or_else(|| {
            format!(
                "datetime julian day overflow: unix_micros={}, days_since_epoch={}",
                unix_micros, days_since_epoch
            )
        })?;
    if julian_day < 0 {
        return Err(format!(
            "datetime julian day underflow: unix_micros={}, julian_day={}",
            unix_micros, julian_day
        ));
    }
    let julian_u64 = u64::try_from(julian_day).map_err(|_| {
        format!(
            "datetime julian conversion failed: julian_day={}",
            julian_day
        )
    })?;
    let micros_u64 = u64::try_from(micros_of_day).map_err(|_| {
        format!(
            "datetime micros-of-day conversion failed: micros_of_day={}",
            micros_of_day
        )
    })?;
    let encoded = (julian_u64 << TIMESTAMP_BITS) | (micros_u64 & TIMESTAMP_TIME_MASK);
    i64::try_from(encoded).map_err(|_| {
        format!(
            "datetime encoded value overflow: unix_micros={}, encoded={}",
            unix_micros, encoded
        )
    })
}

fn format_date_zone_map(days_since_epoch: i32) -> Result<Vec<u8>, String> {
    let days_from_ce = DATE32_UNIX_EPOCH_DAY_OFFSET
        .checked_add(days_since_epoch)
        .ok_or_else(|| {
            format!(
                "date zone-map conversion overflow: days_since_epoch={}",
                days_since_epoch
            )
        })?;
    let date = NaiveDate::from_num_days_from_ce_opt(days_from_ce).ok_or_else(|| {
        format!(
            "date zone-map conversion failed: invalid days_since_epoch={}",
            days_since_epoch
        )
    })?;
    Ok(date.format("%Y-%m-%d").to_string().into_bytes())
}

fn format_datetime_zone_map(unix_micros: i64) -> Result<Vec<u8>, String> {
    let dt = DateTime::from_timestamp_micros(unix_micros).ok_or_else(|| {
        format!(
            "datetime zone-map conversion failed: invalid unix micros {}",
            unix_micros
        )
    })?;
    Ok(dt
        .naive_utc()
        .format("%Y-%m-%d %H:%M:%S%.6f")
        .to_string()
        .into_bytes())
}

fn build_native_column_meta_recursive(
    column: &ArrayRef,
    schema_column: &ColumnPb,
    column_id: u32,
    unique_id_fallback: u32,
    column_index: usize,
    column_path: &str,
    segment_payload: &mut Vec<u8>,
) -> Result<ColumnMetaWriterPb, String> {
    let type_name = schema_column.r#type.trim().to_ascii_uppercase();
    if type_name == "ARRAY" {
        return build_native_array_column_meta(
            column,
            schema_column,
            column_id,
            unique_id_fallback,
            column_index,
            column_path,
            segment_payload,
        );
    }
    if type_name == "MAP" {
        return build_native_map_column_meta(
            column,
            schema_column,
            column_id,
            unique_id_fallback,
            column_index,
            column_path,
            segment_payload,
        );
    }
    if type_name == "STRUCT" {
        return build_native_struct_column_meta(
            column,
            schema_column,
            column_id,
            unique_id_fallback,
            column_index,
            column_path,
            segment_payload,
        );
    }
    build_native_scalar_column_meta(
        column,
        schema_column,
        column_id,
        unique_id_fallback,
        column_index,
        segment_payload,
    )
}

fn build_native_scalar_column_meta(
    column: &ArrayRef,
    schema_column: &ColumnPb,
    column_id: u32,
    unique_id_fallback: u32,
    column_index: usize,
    segment_payload: &mut Vec<u8>,
) -> Result<ColumnMetaWriterPb, String> {
    let writer_type = map_schema_type_to_writer_type(schema_column)?;
    let logical_type = writer_type.logical_type();
    if column.is_empty() {
        return Ok(ColumnMetaWriterPb {
            column_id: Some(column_id),
            unique_id: Some(resolve_column_unique_id(schema_column, unique_id_fallback)),
            r#type: Some(logical_type),
            encoding: Some(ENCODING_PLAIN),
            compression: Some(COMPRESSION_LZ4_FRAME),
            is_nullable: Some(schema_column.is_nullable.unwrap_or(true)),
            indexes: Vec::new(),
            children_columns: Vec::new(),
            num_rows: Some(0),
        });
    }
    let zone_map =
        build_segment_zone_map_for_column(column, column_index, schema_column, writer_type)?;
    let data_page =
        encode_native_data_page_for_column(column, column_index, schema_column, writer_type)?;
    let page_offset = u64::try_from(segment_payload.len()).map_err(|_| {
        format!(
            "native segment page offset overflow at column_index={}",
            column_index
        )
    })?;
    let page_size = u32::try_from(data_page.len()).map_err(|_| {
        format!(
            "native segment page size overflow at column_index={}, page_size={}",
            column_index,
            data_page.len()
        )
    })?;
    segment_payload.extend_from_slice(&data_page);

    let unique_id = resolve_column_unique_id(schema_column, unique_id_fallback);
    let ordinal_index = ColumnIndexMetaWriterPb {
        r#type: Some(COLUMN_INDEX_TYPE_ORDINAL_INDEX),
        ordinal_index: Some(OrdinalIndexWriterPb {
            root_page: Some(BTreeMetaWriterPb {
                root_page: Some(PagePointerWriterPb {
                    offset: Some(page_offset),
                    size: Some(page_size),
                }),
                is_root_data_page: Some(true),
            }),
        }),
        zone_map_index: None,
    };
    let zone_map_index = ColumnIndexMetaWriterPb {
        r#type: Some(COLUMN_INDEX_TYPE_ZONE_MAP_INDEX),
        ordinal_index: None,
        zone_map_index: Some(ZoneMapIndexWriterPb {
            segment_zone_map: Some(zone_map),
            page_zone_maps: None,
        }),
    };

    Ok(ColumnMetaWriterPb {
        column_id: Some(column_id),
        unique_id: Some(unique_id),
        r#type: Some(logical_type),
        encoding: Some(ENCODING_PLAIN),
        compression: Some(COMPRESSION_LZ4_FRAME),
        is_nullable: Some(schema_column.is_nullable.unwrap_or(true)),
        indexes: vec![ordinal_index, zone_map_index],
        children_columns: Vec::new(),
        num_rows: Some(column.len() as u64),
    })
}

fn build_native_array_column_meta(
    column: &ArrayRef,
    schema_column: &ColumnPb,
    column_id: u32,
    unique_id_fallback: u32,
    column_index: usize,
    column_path: &str,
    segment_payload: &mut Vec<u8>,
) -> Result<ColumnMetaWriterPb, String> {
    let is_nullable = schema_column.is_nullable.unwrap_or(true);
    let column_name = schema_column.name.as_deref().unwrap_or("<unknown>");
    ensure_nullable_compatible(column.as_ref(), column_index, column_name, is_nullable)?;
    if schema_column.children_columns.len() != 1 {
        return Err(format!(
            "ARRAY schema child count mismatch for native segment writer: column_index={}, column_name={}, schema_children={}, expected=1",
            column_index,
            column_name,
            schema_column.children_columns.len()
        ));
    }

    let (element_values, lengths, null_flags) =
        extract_array_storage(column, column_index, column_path)?;
    let element_schema = &schema_column.children_columns[0];
    let element_meta = build_native_column_meta_recursive(
        &element_values,
        element_schema,
        column_id,
        unique_id_fallback,
        column_index,
        &format!("{column_path}.item"),
        segment_payload,
    )?;

    let mut children = Vec::with_capacity(if is_nullable { 3 } else { 2 });
    children.push(element_meta);
    if is_nullable {
        let null_schema = build_synthetic_child_column(
            unique_id_fallback,
            format!("{column_path}.__array_nulls"),
            "TINYINT",
            false,
        );
        let null_array: ArrayRef = std::sync::Arc::new(Int8Array::from(
            null_flags.into_iter().map(|v| v as i8).collect::<Vec<i8>>(),
        ));
        let null_meta = build_native_scalar_column_meta(
            &null_array,
            &null_schema,
            column_id,
            unique_id_fallback,
            column_index,
            segment_payload,
        )?;
        children.push(null_meta);
    }

    let length_schema = build_synthetic_child_column(
        unique_id_fallback,
        format!("{column_path}.__array_lengths"),
        "INT",
        false,
    );
    let length_array: ArrayRef = std::sync::Arc::new(Int32Array::from(lengths));
    let length_meta = build_native_scalar_column_meta(
        &length_array,
        &length_schema,
        column_id,
        unique_id_fallback,
        column_index,
        segment_payload,
    )?;
    children.push(length_meta);

    Ok(ColumnMetaWriterPb {
        column_id: Some(column_id),
        unique_id: Some(resolve_column_unique_id(schema_column, unique_id_fallback)),
        r#type: Some(LOGICAL_TYPE_ARRAY),
        encoding: Some(ENCODING_PLAIN),
        compression: Some(COMPRESSION_LZ4_FRAME),
        is_nullable: Some(is_nullable),
        indexes: Vec::new(),
        children_columns: children,
        num_rows: Some(column.len() as u64),
    })
}

fn build_native_map_column_meta(
    column: &ArrayRef,
    schema_column: &ColumnPb,
    column_id: u32,
    unique_id_fallback: u32,
    column_index: usize,
    column_path: &str,
    segment_payload: &mut Vec<u8>,
) -> Result<ColumnMetaWriterPb, String> {
    let is_nullable = schema_column.is_nullable.unwrap_or(true);
    let column_name = schema_column.name.as_deref().unwrap_or("<unknown>");
    ensure_nullable_compatible(column.as_ref(), column_index, column_name, is_nullable)?;
    if schema_column.children_columns.len() != 2 {
        return Err(format!(
            "MAP schema child count mismatch for native segment writer: column_index={}, column_name={}, schema_children={}, expected=2",
            column_index,
            column_name,
            schema_column.children_columns.len()
        ));
    }

    let (key_values, value_values, lengths, null_flags) =
        extract_map_storage(column, column_index, column_path)?;
    let key_meta = build_native_column_meta_recursive(
        &key_values,
        &schema_column.children_columns[0],
        column_id,
        unique_id_fallback,
        column_index,
        &format!("{column_path}.key"),
        segment_payload,
    )?;
    let value_meta = build_native_column_meta_recursive(
        &value_values,
        &schema_column.children_columns[1],
        column_id,
        unique_id_fallback,
        column_index,
        &format!("{column_path}.value"),
        segment_payload,
    )?;

    let mut children = Vec::with_capacity(if is_nullable { 4 } else { 3 });
    children.push(key_meta);
    children.push(value_meta);

    if is_nullable {
        let null_schema = build_synthetic_child_column(
            unique_id_fallback,
            format!("{column_path}.__map_nulls"),
            "TINYINT",
            false,
        );
        let null_array: ArrayRef = std::sync::Arc::new(Int8Array::from(
            null_flags.into_iter().map(|v| v as i8).collect::<Vec<i8>>(),
        ));
        let null_meta = build_native_scalar_column_meta(
            &null_array,
            &null_schema,
            column_id,
            unique_id_fallback,
            column_index,
            segment_payload,
        )?;
        children.push(null_meta);
    }

    let length_schema = build_synthetic_child_column(
        unique_id_fallback,
        format!("{column_path}.__map_lengths"),
        "INT",
        false,
    );
    let length_array: ArrayRef = std::sync::Arc::new(Int32Array::from(lengths));
    let length_meta = build_native_scalar_column_meta(
        &length_array,
        &length_schema,
        column_id,
        unique_id_fallback,
        column_index,
        segment_payload,
    )?;
    children.push(length_meta);

    Ok(ColumnMetaWriterPb {
        column_id: Some(column_id),
        unique_id: Some(resolve_column_unique_id(schema_column, unique_id_fallback)),
        r#type: Some(LOGICAL_TYPE_MAP),
        encoding: Some(ENCODING_PLAIN),
        compression: Some(COMPRESSION_LZ4_FRAME),
        is_nullable: Some(is_nullable),
        indexes: Vec::new(),
        children_columns: children,
        num_rows: Some(column.len() as u64),
    })
}

fn build_native_struct_column_meta(
    column: &ArrayRef,
    schema_column: &ColumnPb,
    column_id: u32,
    unique_id_fallback: u32,
    column_index: usize,
    column_path: &str,
    segment_payload: &mut Vec<u8>,
) -> Result<ColumnMetaWriterPb, String> {
    let is_nullable = schema_column.is_nullable.unwrap_or(true);
    let column_name = schema_column.name.as_deref().unwrap_or("<unknown>");
    ensure_nullable_compatible(column.as_ref(), column_index, column_name, is_nullable)?;
    let struct_array = match column.data_type() {
        arrow::datatypes::DataType::Struct(_) => {
            column.as_any().downcast_ref::<StructArray>().ok_or_else(|| {
                format!(
                    "downcast StructArray failed for native segment writer: column_index={}, column_path={}",
                    column_index, column_path
                )
            })?
        }
        other => {
            return Err(format!(
                "STRUCT column data type mismatch for native segment writer: column_index={}, column_path={}, actual_arrow_type={:?}",
                column_index, column_path, other
            ));
        }
    };
    if schema_column.children_columns.len() != struct_array.num_columns() {
        return Err(format!(
            "STRUCT schema child count mismatch for native segment writer: column_index={}, column_name={}, schema_children={}, struct_children={}",
            column_index,
            column_name,
            schema_column.children_columns.len(),
            struct_array.num_columns()
        ));
    }

    let mut children =
        Vec::with_capacity(schema_column.children_columns.len() + usize::from(is_nullable));
    for (child_idx, child_schema) in schema_column.children_columns.iter().enumerate() {
        let child = struct_array.column(child_idx).clone();
        let child_name = child_schema
            .name
            .as_deref()
            .filter(|name| !name.is_empty())
            .map(str::to_string)
            .unwrap_or_else(|| format!("field_{child_idx}"));
        let child_meta = build_native_column_meta_recursive(
            &child,
            child_schema,
            column_id,
            unique_id_fallback,
            column_index,
            &format!("{column_path}.{child_name}"),
            segment_payload,
        )?;
        children.push(child_meta);
    }

    if is_nullable {
        let null_schema = build_synthetic_child_column(
            unique_id_fallback,
            format!("{column_path}.__struct_nulls"),
            "TINYINT",
            false,
        );
        let mut null_flags = Vec::with_capacity(struct_array.len());
        for row in 0..struct_array.len() {
            null_flags.push(if struct_array.is_null(row) {
                1_i8
            } else {
                0_i8
            });
        }
        let null_array: ArrayRef = std::sync::Arc::new(Int8Array::from(null_flags));
        let null_meta = build_native_scalar_column_meta(
            &null_array,
            &null_schema,
            column_id,
            unique_id_fallback,
            column_index,
            segment_payload,
        )?;
        children.push(null_meta);
    }

    Ok(ColumnMetaWriterPb {
        column_id: Some(column_id),
        unique_id: Some(resolve_column_unique_id(schema_column, unique_id_fallback)),
        r#type: Some(LOGICAL_TYPE_STRUCT),
        encoding: Some(ENCODING_PLAIN),
        compression: Some(COMPRESSION_LZ4_FRAME),
        is_nullable: Some(is_nullable),
        indexes: Vec::new(),
        children_columns: children,
        num_rows: Some(column.len() as u64),
    })
}

fn resolve_column_unique_id(column: &ColumnPb, fallback: u32) -> u32 {
    u32::try_from(column.unique_id).unwrap_or(fallback)
}

fn build_synthetic_child_column(
    unique_id: u32,
    name: String,
    type_name: &str,
    is_nullable: bool,
) -> ColumnPb {
    ColumnPb {
        unique_id: i32::try_from(unique_id).unwrap_or(i32::MAX),
        name: Some(name),
        r#type: type_name.to_string(),
        is_nullable: Some(is_nullable),
        ..Default::default()
    }
}

fn extract_array_storage(
    column: &ArrayRef,
    column_index: usize,
    column_path: &str,
) -> Result<(ArrayRef, Vec<i32>, Vec<u8>), String> {
    match column.data_type() {
        arrow::datatypes::DataType::List(_) => {
            let typed = column.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
                format!(
                    "downcast ListArray failed for native segment writer: column_index={}, column_path={}",
                    column_index, column_path
                )
            })?;
            let offsets = typed.value_offsets();
            let start = offsets.first().copied().unwrap_or(0);
            let end = offsets.last().copied().unwrap_or(0);
            let total = end.checked_sub(start).ok_or_else(|| {
                format!(
                    "invalid ARRAY offsets for native segment writer: column_index={}, column_path={}, start={}, end={}",
                    column_index, column_path, start, end
                )
            })?;
            let start_usize = usize::try_from(start).map_err(|_| {
                format!(
                    "ARRAY offsets start overflow for native segment writer: column_index={}, column_path={}, start={}",
                    column_index, column_path, start
                )
            })?;
            let total_usize = usize::try_from(total).map_err(|_| {
                format!(
                    "ARRAY offsets length overflow for native segment writer: column_index={}, column_path={}, length={}",
                    column_index, column_path, total
                )
            })?;
            let elements = typed.values().slice(start_usize, total_usize);
            let mut lengths = Vec::with_capacity(typed.len());
            let mut null_flags = Vec::with_capacity(typed.len());
            for row in 0..typed.len() {
                let row_start = offsets[row];
                let row_end = offsets[row + 1];
                let len = row_end.checked_sub(row_start).ok_or_else(|| {
                    format!(
                        "negative ARRAY row length in native segment writer: column_index={}, column_path={}, row_index={}, start={}, end={}",
                        column_index, column_path, row, row_start, row_end
                    )
                })?;
                lengths.push(len);
                null_flags.push(u8::from(typed.is_null(row)));
            }
            Ok((elements, lengths, null_flags))
        }
        arrow::datatypes::DataType::LargeList(_) => {
            let typed = column
                .as_any()
                .downcast_ref::<LargeListArray>()
                .ok_or_else(|| {
                    format!(
                        "downcast LargeListArray failed for native segment writer: column_index={}, column_path={}",
                        column_index, column_path
                    )
                })?;
            let offsets = typed.value_offsets();
            let start = offsets.first().copied().unwrap_or(0);
            let end = offsets.last().copied().unwrap_or(0);
            let total = end.checked_sub(start).ok_or_else(|| {
                format!(
                    "invalid LargeList offsets for native segment writer: column_index={}, column_path={}, start={}, end={}",
                    column_index, column_path, start, end
                )
            })?;
            let start_usize = usize::try_from(start).map_err(|_| {
                format!(
                    "LargeList offsets start overflow for native segment writer: column_index={}, column_path={}, start={}",
                    column_index, column_path, start
                )
            })?;
            let total_usize = usize::try_from(total).map_err(|_| {
                format!(
                    "LargeList offsets length overflow for native segment writer: column_index={}, column_path={}, length={}",
                    column_index, column_path, total
                )
            })?;
            let elements = typed.values().slice(start_usize, total_usize);
            let mut lengths = Vec::with_capacity(typed.len());
            let mut null_flags = Vec::with_capacity(typed.len());
            for row in 0..typed.len() {
                let row_start = offsets[row];
                let row_end = offsets[row + 1];
                let len_i64 = row_end.checked_sub(row_start).ok_or_else(|| {
                    format!(
                        "negative LargeList row length in native segment writer: column_index={}, column_path={}, row_index={}, start={}, end={}",
                        column_index, column_path, row, row_start, row_end
                    )
                })?;
                let len = i32::try_from(len_i64).map_err(|_| {
                    format!(
                        "LargeList row length overflow for native segment writer: column_index={}, column_path={}, row_index={}, length={}",
                        column_index, column_path, row, len_i64
                    )
                })?;
                lengths.push(len);
                null_flags.push(u8::from(typed.is_null(row)));
            }
            Ok((elements, lengths, null_flags))
        }
        other => Err(format!(
            "ARRAY column data type mismatch for native segment writer: column_index={}, column_path={}, actual_arrow_type={:?}",
            column_index, column_path, other
        )),
    }
}

fn extract_map_storage(
    column: &ArrayRef,
    column_index: usize,
    column_path: &str,
) -> Result<(ArrayRef, ArrayRef, Vec<i32>, Vec<u8>), String> {
    match column.data_type() {
        arrow::datatypes::DataType::Map(_, _) => {
            let typed = column.as_any().downcast_ref::<MapArray>().ok_or_else(|| {
                format!(
                    "downcast MapArray failed for native segment writer: column_index={}, column_path={}",
                    column_index, column_path
                )
            })?;
            let offsets = typed.value_offsets();
            let start = offsets.first().copied().unwrap_or(0);
            let end = offsets.last().copied().unwrap_or(0);
            let total = end.checked_sub(start).ok_or_else(|| {
                format!(
                    "invalid MAP offsets for native segment writer: column_index={}, column_path={}, start={}, end={}",
                    column_index, column_path, start, end
                )
            })?;
            let start_usize = usize::try_from(start).map_err(|_| {
                format!(
                    "MAP offsets start overflow for native segment writer: column_index={}, column_path={}, start={}",
                    column_index, column_path, start
                )
            })?;
            let total_usize = usize::try_from(total).map_err(|_| {
                format!(
                    "MAP offsets length overflow for native segment writer: column_index={}, column_path={}, length={}",
                    column_index, column_path, total
                )
            })?;
            let keys = typed.keys().slice(start_usize, total_usize);
            let values = typed.values().slice(start_usize, total_usize);
            let mut lengths = Vec::with_capacity(typed.len());
            let mut null_flags = Vec::with_capacity(typed.len());
            for row in 0..typed.len() {
                let row_start = offsets[row];
                let row_end = offsets[row + 1];
                let len = row_end.checked_sub(row_start).ok_or_else(|| {
                    format!(
                        "negative MAP row length in native segment writer: column_index={}, column_path={}, row_index={}, start={}, end={}",
                        column_index, column_path, row, row_start, row_end
                    )
                })?;
                lengths.push(len);
                null_flags.push(u8::from(typed.is_null(row)));
            }
            Ok((keys, values, lengths, null_flags))
        }
        other => Err(format!(
            "MAP column data type mismatch for native segment writer: column_index={}, column_path={}, actual_arrow_type={:?}",
            column_index, column_path, other
        )),
    }
}

fn build_segment_zone_map_for_column(
    column: &ArrayRef,
    column_index: usize,
    schema_column: &ColumnPb,
    writer_type: NativeWriterType,
) -> Result<ZoneMapWriterPb, String> {
    let column_name = schema_column.name.as_deref().unwrap_or("<unknown>");
    let has_null = column.null_count() > 0;
    let has_not_null = column.null_count() < column.len();
    if !has_not_null {
        return Ok(ZoneMapWriterPb {
            min: None,
            max: None,
            has_null: Some(has_null),
            has_not_null: Some(false),
        });
    }

    match writer_type {
        NativeWriterType::TinyInt => {
            let casted = cast_column_to_int8_for_native_writer(
                column,
                column_index,
                column_name,
                "zone map writer",
            )?;
            let typed = casted.as_any().downcast_ref::<Int8Array>().ok_or_else(|| {
                format!(
                    "downcast Int8 column failed for zone map writer: column_index={}, column_name={}",
                    column_index, column_name
                )
            })?;
            let (min, max) = min_max_int8(typed)?;
            Ok(ZoneMapWriterPb {
                min: Some(min.to_string().into_bytes()),
                max: Some(max.to_string().into_bytes()),
                has_null: Some(has_null),
                has_not_null: Some(true),
            })
        }
        NativeWriterType::SmallInt => {
            let casted = cast_column_to_int16_for_native_writer(
                column,
                column_index,
                column_name,
                "zone map writer",
            )?;
            let typed = casted.as_any().downcast_ref::<Int16Array>().ok_or_else(|| {
                format!(
                    "downcast Int16 column failed for zone map writer: column_index={}, column_name={}",
                    column_index, column_name
                )
            })?;
            let (min, max) = min_max_int16(typed)?;
            Ok(ZoneMapWriterPb {
                min: Some(min.to_string().into_bytes()),
                max: Some(max.to_string().into_bytes()),
                has_null: Some(has_null),
                has_not_null: Some(true),
            })
        }
        NativeWriterType::Int => {
            let casted = cast_column_to_int32_for_native_writer(
                column,
                column_index,
                column_name,
                "zone map writer",
            )?;
            let typed = casted.as_any().downcast_ref::<Int32Array>().ok_or_else(|| {
                format!(
                    "downcast Int32 column failed for zone map writer: column_index={}, column_name={}",
                    column_index, column_name
                )
            })?;
            let (min, max) = min_max_int32(typed)?;
            Ok(ZoneMapWriterPb {
                min: Some(min.to_string().into_bytes()),
                max: Some(max.to_string().into_bytes()),
                has_null: Some(has_null),
                has_not_null: Some(true),
            })
        }
        NativeWriterType::Date => {
            let casted = cast_column_to_date32_for_native_writer(
                column,
                column_index,
                column_name,
                "zone map writer",
            )?;
            let typed = casted.as_any().downcast_ref::<Date32Array>().ok_or_else(|| {
                format!(
                    "downcast Date32 column failed for zone map writer: column_index={}, column_name={}",
                    column_index, column_name
                )
            })?;
            let (min, max) = min_max_date32(typed)?;
            Ok(ZoneMapWriterPb {
                min: Some(format_date_zone_map(min)?),
                max: Some(format_date_zone_map(max)?),
                has_null: Some(has_null),
                has_not_null: Some(true),
            })
        }
        NativeWriterType::BigInt => {
            let casted = cast_column_to_int64_for_native_writer(
                column,
                column_index,
                column_name,
                "zone map writer",
            )?;
            let typed = casted.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                format!(
                    "downcast Int64 column failed for zone map writer: column_index={}, column_name={}",
                    column_index, column_name
                )
            })?;
            let (min, max) = min_max_int64(typed)?;
            Ok(ZoneMapWriterPb {
                min: Some(min.to_string().into_bytes()),
                max: Some(max.to_string().into_bytes()),
                has_null: Some(has_null),
                has_not_null: Some(true),
            })
        }
        NativeWriterType::LargeInt => {
            let values =
                decode_largeint_values(column, column_index, column_name, "zone map encoder")?;
            let (min, max) = min_max_largeint(&values)?;
            Ok(ZoneMapWriterPb {
                min: Some(min.to_string().into_bytes()),
                max: Some(max.to_string().into_bytes()),
                has_null: Some(has_null),
                has_not_null: Some(true),
            })
        }
        NativeWriterType::Decimal32
        | NativeWriterType::Decimal64
        | NativeWriterType::Decimal128
        | NativeWriterType::Decimal256 => {
            let (precision, scale) =
                decimal_meta_from_schema_column(schema_column, column_index, column_name)?;
            if writer_type == NativeWriterType::Decimal256 {
                let casted = cast(
                    column.as_ref(),
                    &arrow::datatypes::DataType::Decimal256(precision, scale),
                )
                .map_err(|e| {
                    format!(
                        "cast column to Decimal256 failed for zone map writer: column_index={}, column_name={}, precision={}, scale={}, error={}",
                        column_index, column_name, precision, scale, e
                    )
                })?;
                let typed = casted
                    .as_any()
                    .downcast_ref::<Decimal256Array>()
                    .ok_or_else(|| {
                        format!(
                            "downcast Decimal256 column failed for zone map writer: column_index={}, column_name={}",
                            column_index, column_name
                        )
                    })?;
                if typed.null_count() == typed.len() {
                    return Ok(ZoneMapWriterPb {
                        min: None,
                        max: None,
                        has_null: Some(true),
                        has_not_null: Some(false),
                    });
                }
                let (min, max) = min_max_decimal256(typed)?;
                Ok(ZoneMapWriterPb {
                    min: Some(format_decimal256_zone_map_value(min, scale)),
                    max: Some(format_decimal256_zone_map_value(max, scale)),
                    has_null: Some(has_null),
                    has_not_null: Some(true),
                })
            } else {
                let casted = cast(
                    column.as_ref(),
                    &arrow::datatypes::DataType::Decimal128(precision, scale),
                )
                .map_err(|e| {
                    format!(
                        "cast column to Decimal128 failed for zone map writer: column_index={}, column_name={}, precision={}, scale={}, error={}",
                        column_index, column_name, precision, scale, e
                    )
                })?;
                let typed = casted
                    .as_any()
                    .downcast_ref::<Decimal128Array>()
                    .ok_or_else(|| {
                        format!(
                            "downcast Decimal128 column failed for zone map writer: column_index={}, column_name={}",
                            column_index, column_name
                        )
                    })?;
                if typed.null_count() == typed.len() {
                    return Ok(ZoneMapWriterPb {
                        min: None,
                        max: None,
                        has_null: Some(true),
                        has_not_null: Some(false),
                    });
                }
                let (min, max) = min_max_decimal128(typed)?;
                Ok(ZoneMapWriterPb {
                    min: Some(format_decimal_zone_map_value(min, scale)),
                    max: Some(format_decimal_zone_map_value(max, scale)),
                    has_null: Some(has_null),
                    has_not_null: Some(true),
                })
            }
        }
        NativeWriterType::Datetime => {
            let casted = cast_column_to_timestamp_micros_for_native_writer(
                column,
                column_index,
                column_name,
                "zone map writer",
            )?;
            let typed = casted
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .ok_or_else(|| {
                    format!(
                        "downcast Timestamp(Microsecond,None) column failed for zone map writer: column_index={}, column_name={}",
                        column_index, column_name
                    )
                })?;
            let (min, max) = min_max_timestamp_microsecond(typed)?;
            Ok(ZoneMapWriterPb {
                min: Some(format_datetime_zone_map(min)?),
                max: Some(format_datetime_zone_map(max)?),
                has_null: Some(has_null),
                has_not_null: Some(true),
            })
        }
        NativeWriterType::Float => {
            let casted = cast_column_to_float32_for_native_writer(
                column,
                column_index,
                column_name,
                "zone map writer",
            )?;
            let typed = casted.as_any().downcast_ref::<Float32Array>().ok_or_else(|| {
                format!(
                    "downcast Float32 column failed for zone map writer: column_index={}, column_name={}",
                    column_index, column_name
                )
            })?;
            let (min, max) = min_max_float32(typed)?;
            Ok(ZoneMapWriterPb {
                min: Some(min.to_string().into_bytes()),
                max: Some(max.to_string().into_bytes()),
                has_null: Some(has_null),
                has_not_null: Some(true),
            })
        }
        NativeWriterType::Double => {
            let casted = cast_column_to_float64_for_native_writer(
                column,
                column_index,
                column_name,
                "zone map writer",
            )?;
            let typed = casted.as_any().downcast_ref::<Float64Array>().ok_or_else(|| {
                format!(
                    "downcast Float64 column failed for zone map writer: column_index={}, column_name={}",
                    column_index, column_name
                )
            })?;
            let (min, max) = min_max_float64(typed)?;
            Ok(ZoneMapWriterPb {
                min: Some(min.to_string().into_bytes()),
                max: Some(max.to_string().into_bytes()),
                has_null: Some(has_null),
                has_not_null: Some(true),
            })
        }
        NativeWriterType::Boolean => {
            let casted = cast_column_to_boolean_for_native_writer(
                column,
                column_index,
                column_name,
                "zone map writer",
            )?;
            let typed = casted.as_any().downcast_ref::<BooleanArray>().ok_or_else(|| {
                format!(
                    "downcast Boolean column failed for zone map writer: column_index={}, column_name={}",
                    column_index, column_name
                )
            })?;
            let (min, max) = min_max_bool(typed)?;
            Ok(ZoneMapWriterPb {
                min: Some(if min { b"1".to_vec() } else { b"0".to_vec() }),
                max: Some(if max { b"1".to_vec() } else { b"0".to_vec() }),
                has_null: Some(has_null),
                has_not_null: Some(true),
            })
        }
        NativeWriterType::Varchar => {
            let casted =
                cast(column.as_ref(), &arrow::datatypes::DataType::Utf8).map_err(|e| {
                    format!(
                        "cast column to Utf8 failed for zone map writer: column_index={}, column_name={}, error={}",
                        column_index, column_name, e
                    )
                })?;
            let typed = casted.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                format!(
                    "downcast Utf8 column failed for zone map writer: column_index={}, column_name={}",
                    column_index, column_name
                )
            })?;
            let (min, max) = min_max_utf8(typed)?;
            Ok(ZoneMapWriterPb {
                min: Some(min),
                max: Some(max),
                has_null: Some(has_null),
                has_not_null: Some(true),
            })
        }
        NativeWriterType::Binary
        | NativeWriterType::VarBinary
        | NativeWriterType::Hll
        | NativeWriterType::Object => {
            let casted = cast(column.as_ref(), &arrow::datatypes::DataType::Binary).map_err(
                |e| {
                    format!(
                        "cast column to Binary failed for zone map writer: column_index={}, column_name={}, error={}",
                        column_index, column_name, e
                    )
                },
            )?;
            let typed = casted.as_any().downcast_ref::<BinaryArray>().ok_or_else(|| {
                format!(
                    "downcast Binary column failed for zone map writer: column_index={}, column_name={}",
                    column_index, column_name
                )
            })?;
            let (min, max) = min_max_binary(typed)?;
            Ok(ZoneMapWriterPb {
                min: Some(min),
                max: Some(max),
                has_null: Some(has_null),
                has_not_null: Some(true),
            })
        }
    }
}

fn min_max_int8(array: &Int8Array) -> Result<(i8, i8), String> {
    let mut min = None;
    let mut max = None;
    for row in 0..array.len() {
        if array.is_null(row) {
            continue;
        }
        let value = array.value(row);
        min = Some(min.map_or(value, |v: i8| v.min(value)));
        max = Some(max.map_or(value, |v: i8| v.max(value)));
    }
    match (min, max) {
        (Some(min), Some(max)) => Ok((min, max)),
        _ => Err("zone map Int8 has no non-null values".to_string()),
    }
}

fn min_max_int16(array: &Int16Array) -> Result<(i16, i16), String> {
    let mut min = None;
    let mut max = None;
    for row in 0..array.len() {
        if array.is_null(row) {
            continue;
        }
        let value = array.value(row);
        min = Some(min.map_or(value, |v: i16| v.min(value)));
        max = Some(max.map_or(value, |v: i16| v.max(value)));
    }
    match (min, max) {
        (Some(min), Some(max)) => Ok((min, max)),
        _ => Err("zone map Int16 has no non-null values".to_string()),
    }
}

fn min_max_int32(array: &Int32Array) -> Result<(i32, i32), String> {
    let mut min = None;
    let mut max = None;
    for row in 0..array.len() {
        if array.is_null(row) {
            continue;
        }
        let value = array.value(row);
        min = Some(min.map_or(value, |v: i32| v.min(value)));
        max = Some(max.map_or(value, |v: i32| v.max(value)));
    }
    match (min, max) {
        (Some(min), Some(max)) => Ok((min, max)),
        _ => Err("zone map Int32 has no non-null values".to_string()),
    }
}

fn min_max_date32(array: &Date32Array) -> Result<(i32, i32), String> {
    let mut min = None;
    let mut max = None;
    for row in 0..array.len() {
        if array.is_null(row) {
            continue;
        }
        let value = array.value(row);
        min = Some(min.map_or(value, |v: i32| v.min(value)));
        max = Some(max.map_or(value, |v: i32| v.max(value)));
    }
    match (min, max) {
        (Some(min), Some(max)) => Ok((min, max)),
        _ => Err("zone map Date32 has no non-null values".to_string()),
    }
}

fn min_max_int64(array: &Int64Array) -> Result<(i64, i64), String> {
    let mut min = None;
    let mut max = None;
    for row in 0..array.len() {
        if array.is_null(row) {
            continue;
        }
        let value = array.value(row);
        min = Some(min.map_or(value, |v: i64| v.min(value)));
        max = Some(max.map_or(value, |v: i64| v.max(value)));
    }
    match (min, max) {
        (Some(min), Some(max)) => Ok((min, max)),
        _ => Err("zone map Int64 has no non-null values".to_string()),
    }
}

fn min_max_largeint(values: &[Option<i128>]) -> Result<(i128, i128), String> {
    let mut min = None;
    let mut max = None;
    for value in values.iter().flatten().copied() {
        min = Some(min.map_or(value, |v: i128| v.min(value)));
        max = Some(max.map_or(value, |v: i128| v.max(value)));
    }
    match (min, max) {
        (Some(min), Some(max)) => Ok((min, max)),
        _ => Err("zone map LargeInt has no non-null values".to_string()),
    }
}

fn min_max_timestamp_microsecond(array: &TimestampMicrosecondArray) -> Result<(i64, i64), String> {
    let mut min = None;
    let mut max = None;
    for row in 0..array.len() {
        if array.is_null(row) {
            continue;
        }
        let value = array.value(row);
        min = Some(min.map_or(value, |v: i64| v.min(value)));
        max = Some(max.map_or(value, |v: i64| v.max(value)));
    }
    match (min, max) {
        (Some(min), Some(max)) => Ok((min, max)),
        _ => Err("zone map Timestamp(Microsecond,None) has no non-null values".to_string()),
    }
}

fn min_max_float32(array: &Float32Array) -> Result<(f32, f32), String> {
    let mut min = None;
    let mut max = None;
    for row in 0..array.len() {
        if array.is_null(row) {
            continue;
        }
        let value = array.value(row);
        min = Some(min.map_or(value, |v: f32| {
            if value.total_cmp(&v).is_lt() {
                value
            } else {
                v
            }
        }));
        max = Some(max.map_or(value, |v: f32| {
            if value.total_cmp(&v).is_gt() {
                value
            } else {
                v
            }
        }));
    }
    match (min, max) {
        (Some(min), Some(max)) => Ok((min, max)),
        _ => Err("zone map Float32 has no non-null values".to_string()),
    }
}

fn min_max_float64(array: &Float64Array) -> Result<(f64, f64), String> {
    let mut min = None;
    let mut max = None;
    for row in 0..array.len() {
        if array.is_null(row) {
            continue;
        }
        let value = array.value(row);
        min = Some(min.map_or(value, |v: f64| {
            if value.total_cmp(&v).is_lt() {
                value
            } else {
                v
            }
        }));
        max = Some(max.map_or(value, |v: f64| {
            if value.total_cmp(&v).is_gt() {
                value
            } else {
                v
            }
        }));
    }
    match (min, max) {
        (Some(min), Some(max)) => Ok((min, max)),
        _ => Err("zone map Float64 has no non-null values".to_string()),
    }
}

fn min_max_bool(array: &BooleanArray) -> Result<(bool, bool), String> {
    let mut min = None;
    let mut max = None;
    for row in 0..array.len() {
        if array.is_null(row) {
            continue;
        }
        let value = array.value(row);
        min = Some(min.map_or(value, |v: bool| v && value));
        max = Some(max.map_or(value, |v: bool| v || value));
    }
    match (min, max) {
        (Some(min), Some(max)) => Ok((min, max)),
        _ => Err("zone map Boolean has no non-null values".to_string()),
    }
}

fn min_max_utf8(array: &StringArray) -> Result<(Vec<u8>, Vec<u8>), String> {
    let mut min: Option<Vec<u8>> = None;
    let mut max: Option<Vec<u8>> = None;
    for row in 0..array.len() {
        if array.is_null(row) {
            continue;
        }
        let value = array.value(row).as_bytes();
        match min.as_ref() {
            Some(current) => {
                if value < current.as_slice() {
                    min = Some(value.to_vec());
                }
            }
            None => min = Some(value.to_vec()),
        }
        match max.as_ref() {
            Some(current) => {
                if value > current.as_slice() {
                    max = Some(value.to_vec());
                }
            }
            None => max = Some(value.to_vec()),
        }
    }
    match (min, max) {
        (Some(min), Some(max)) => Ok((min, max)),
        _ => Err("zone map Utf8 has no non-null values".to_string()),
    }
}

fn min_max_binary(array: &BinaryArray) -> Result<(Vec<u8>, Vec<u8>), String> {
    let mut min: Option<Vec<u8>> = None;
    let mut max: Option<Vec<u8>> = None;
    for row in 0..array.len() {
        if array.is_null(row) {
            continue;
        }
        let value = array.value(row);
        match min.as_ref() {
            Some(current) => {
                if value < current.as_slice() {
                    min = Some(value.to_vec());
                }
            }
            None => min = Some(value.to_vec()),
        }
        match max.as_ref() {
            Some(current) => {
                if value > current.as_slice() {
                    max = Some(value.to_vec());
                }
            }
            None => max = Some(value.to_vec()),
        }
    }
    match (min, max) {
        (Some(min), Some(max)) => Ok((min, max)),
        _ => Err("zone map Binary has no non-null values".to_string()),
    }
}

fn decimal_meta_from_schema_column(
    schema_column: &ColumnPb,
    column_index: usize,
    column_name: &str,
) -> Result<(u8, i8), String> {
    let precision_i32 = schema_column.precision.ok_or_else(|| {
        format!(
            "missing decimal precision for native segment writer: column_index={}, column_name={}, schema_type={}",
            column_index, column_name, schema_column.r#type
        )
    })?;
    let frac_i32 = schema_column.frac.ok_or_else(|| {
        format!(
            "missing decimal scale for native segment writer: column_index={}, column_name={}, schema_type={}",
            column_index, column_name, schema_column.r#type
        )
    })?;
    let precision = u8::try_from(precision_i32).map_err(|_| {
        format!(
            "decimal precision overflow for native segment writer: column_index={}, column_name={}, precision={}",
            column_index, column_name, precision_i32
        )
    })?;
    let scale = i8::try_from(frac_i32).map_err(|_| {
        format!(
            "decimal scale overflow for native segment writer: column_index={}, column_name={}, scale={}",
            column_index, column_name, frac_i32
        )
    })?;
    if precision == 0 {
        return Err(format!(
            "decimal precision must be > 0 for native segment writer: column_index={}, column_name={}",
            column_index, column_name
        ));
    }
    if scale < 0 {
        return Err(format!(
            "decimal scale must be >= 0 for native segment writer: column_index={}, column_name={}, scale={}",
            column_index, column_name, scale
        ));
    }
    Ok((precision, scale))
}

fn min_max_decimal128(array: &Decimal128Array) -> Result<(i128, i128), String> {
    let mut min: Option<i128> = None;
    let mut max: Option<i128> = None;
    for row in 0..array.len() {
        if array.is_null(row) {
            continue;
        }
        let value = array.value(row);
        min = Some(min.map_or(value, |v| v.min(value)));
        max = Some(max.map_or(value, |v| v.max(value)));
    }
    match (min, max) {
        (Some(min), Some(max)) => Ok((min, max)),
        _ => Err("zone map Decimal128 has no non-null values".to_string()),
    }
}

fn min_max_decimal256(array: &Decimal256Array) -> Result<(i256, i256), String> {
    let mut min: Option<i256> = None;
    let mut max: Option<i256> = None;
    for row in 0..array.len() {
        if array.is_null(row) {
            continue;
        }
        let value = array.value(row);
        min = Some(min.map_or(value, |v| v.min(value)));
        max = Some(max.map_or(value, |v| v.max(value)));
    }
    match (min, max) {
        (Some(min), Some(max)) => Ok((min, max)),
        _ => Err("zone map Decimal256 has no non-null values".to_string()),
    }
}

fn format_decimal_zone_map_value(unscaled: i128, scale: i8) -> Vec<u8> {
    if scale <= 0 {
        return unscaled.to_string().into_bytes();
    }
    let scale_usize = scale as usize;
    let sign = if unscaled < 0 { "-" } else { "" };
    let digits = unscaled.abs().to_string();
    if digits.len() <= scale_usize {
        let mut frac = String::with_capacity(scale_usize);
        frac.extend(std::iter::repeat('0').take(scale_usize - digits.len()));
        frac.push_str(&digits);
        return format!("{sign}0.{frac}").into_bytes();
    }
    let split = digits.len() - scale_usize;
    let integer = &digits[..split];
    let frac = &digits[split..];
    format!("{sign}{integer}.{frac}").into_bytes()
}

fn format_decimal256_zone_map_value(unscaled: i256, scale: i8) -> Vec<u8> {
    if scale <= 0 {
        return unscaled.to_string().into_bytes();
    }
    let scale_usize = scale as usize;
    let negative = unscaled.is_negative();
    let abs = if negative {
        unscaled.checked_neg().unwrap_or(unscaled)
    } else {
        unscaled
    };
    let sign = if negative { "-" } else { "" };
    let digits = abs.to_string();
    if digits.len() <= scale_usize {
        let mut frac = String::with_capacity(scale_usize);
        frac.extend(std::iter::repeat('0').take(scale_usize - digits.len()));
        frac.push_str(&digits);
        return format!("{sign}0.{frac}").into_bytes();
    }
    let split = digits.len() - scale_usize;
    let integer = &digits[..split];
    let frac = &digits[split..];
    format!("{sign}{integer}.{frac}").into_bytes()
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum NativeWriterType {
    TinyInt,
    SmallInt,
    Int,
    BigInt,
    LargeInt,
    Decimal32,
    Decimal64,
    Decimal128,
    Decimal256,
    Date,
    Datetime,
    Float,
    Double,
    Varchar,
    Binary,
    VarBinary,
    Hll,
    Object,
    Boolean,
}

impl NativeWriterType {
    fn logical_type(self) -> i32 {
        match self {
            Self::TinyInt => LOGICAL_TYPE_TINYINT,
            Self::SmallInt => LOGICAL_TYPE_SMALLINT,
            Self::Int => LOGICAL_TYPE_INT,
            Self::BigInt => LOGICAL_TYPE_BIGINT,
            Self::LargeInt => LOGICAL_TYPE_LARGEINT,
            Self::Decimal32 => LOGICAL_TYPE_DECIMAL32,
            Self::Decimal64 => LOGICAL_TYPE_DECIMAL64,
            Self::Decimal128 => LOGICAL_TYPE_DECIMAL128,
            Self::Decimal256 => LOGICAL_TYPE_DECIMAL256,
            Self::Date => LOGICAL_TYPE_DATE,
            Self::Datetime => LOGICAL_TYPE_DATETIME,
            Self::Float => LOGICAL_TYPE_FLOAT,
            Self::Double => LOGICAL_TYPE_DOUBLE,
            Self::Varchar => LOGICAL_TYPE_VARCHAR,
            Self::Binary => LOGICAL_TYPE_BINARY,
            Self::VarBinary => LOGICAL_TYPE_VARBINARY,
            Self::Hll => LOGICAL_TYPE_HLL,
            Self::Object => LOGICAL_TYPE_OBJECT,
            Self::Boolean => LOGICAL_TYPE_BOOLEAN,
        }
    }
}

fn map_schema_type_to_writer_type(column: &ColumnPb) -> Result<NativeWriterType, String> {
    let type_name = column.r#type.trim().to_ascii_uppercase();
    let base_type = type_name.split('(').next().unwrap_or(type_name.as_str());
    match base_type {
        "TINYINT" => Ok(NativeWriterType::TinyInt),
        "SMALLINT" => Ok(NativeWriterType::SmallInt),
        "INT" => Ok(NativeWriterType::Int),
        "BIGINT" => Ok(NativeWriterType::BigInt),
        "LARGEINT" => Ok(NativeWriterType::LargeInt),
        "DECIMAL32" => Ok(NativeWriterType::Decimal32),
        "DECIMAL64" => Ok(NativeWriterType::Decimal64),
        "DECIMAL128" => Ok(NativeWriterType::Decimal128),
        "DECIMAL256" => Ok(NativeWriterType::Decimal256),
        "DATE" | "DATE_V2" => Ok(NativeWriterType::Date),
        "DATETIME" | "DATETIME_V2" | "TIMESTAMP" => Ok(NativeWriterType::Datetime),
        "FLOAT" => Ok(NativeWriterType::Float),
        "DOUBLE" => Ok(NativeWriterType::Double),
        "BOOLEAN" => Ok(NativeWriterType::Boolean),
        "CHAR" | "VARCHAR" | "STRING" => Ok(NativeWriterType::Varchar),
        "BINARY" => Ok(NativeWriterType::Binary),
        "VARBINARY" => Ok(NativeWriterType::VarBinary),
        "HLL" => Ok(NativeWriterType::Hll),
        "BITMAP" | "OBJECT" | "JSON" => Ok(NativeWriterType::Object),
        _ => Err(format!(
            "unsupported schema type for native segment writer: {}",
            column.r#type
        )),
    }
}

fn decode_largeint_values(
    column: &ArrayRef,
    column_index: usize,
    column_name: &str,
    context: &str,
) -> Result<Vec<Option<i128>>, String> {
    if let Some(typed) = column.as_any().downcast_ref::<FixedSizeBinaryArray>() {
        if typed.value_length() != crate::common::largeint::LARGEINT_BYTE_WIDTH {
            return Err(format!(
                "invalid LARGEINT fixed-size binary width for native segment writer {}: column_index={}, column_name={}, expected={}, actual={}",
                context,
                column_index,
                column_name,
                crate::common::largeint::LARGEINT_BYTE_WIDTH,
                typed.value_length()
            ));
        }
        let mut values = Vec::with_capacity(typed.len());
        for row in 0..typed.len() {
            if typed.is_null(row) {
                values.push(None);
                continue;
            }
            let value = crate::common::largeint::i128_from_be_bytes(typed.value(row)).map_err(|e| {
                format!(
                    "decode LARGEINT fixed-size binary failed for native segment writer {}: column_index={}, column_name={}, row={}, error={}",
                    context, column_index, column_name, row, e
                )
            })?;
            values.push(Some(value));
        }
        return Ok(values);
    }

    let casted = cast(column.as_ref(), &arrow::datatypes::DataType::Decimal128(38, 0)).map_err(
        |e| {
            format!(
                "cast column to Decimal128(38,0) for LARGEINT failed for native segment writer {}: column_index={}, column_name={}, error={}",
                context, column_index, column_name, e
            )
        },
    )?;
    let typed = casted
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .ok_or_else(|| {
            format!(
                "downcast Decimal128(38,0) for LARGEINT failed for native segment writer {}: column_index={}, column_name={}",
                context, column_index, column_name
            )
        })?;
    let mut values = Vec::with_capacity(typed.len());
    for row in 0..typed.len() {
        if typed.is_null(row) {
            values.push(None);
        } else {
            values.push(Some(typed.value(row)));
        }
    }
    Ok(values)
}

fn encode_native_data_page_for_column(
    column: &ArrayRef,
    column_index: usize,
    schema_column: &ColumnPb,
    writer_type: NativeWriterType,
) -> Result<Vec<u8>, String> {
    let column_name = schema_column.name.as_deref().unwrap_or("<unknown>");
    let is_nullable = schema_column.is_nullable.unwrap_or(true);
    ensure_nullable_compatible(column.as_ref(), column_index, column_name, is_nullable)?;

    match writer_type {
        NativeWriterType::TinyInt => {
            let casted = cast_column_to_int8_for_native_writer(
                column,
                column_index,
                column_name,
                "native segment writer",
            )?;
            let typed = casted.as_any().downcast_ref::<Int8Array>().ok_or_else(|| {
                format!(
                    "downcast casted Int8 column failed for native segment writer: column_index={}, column_name={}",
                    column_index, column_name
                )
            })?;
            let writer_type_label = format!("TinyInt(source={:?})", column.data_type());
            let (value_bytes, null_flags) = encode_fixed_values_with_nulls(
                typed.len(),
                typed.null_count(),
                is_nullable,
                1,
                column_index,
                column_name,
                writer_type_label.as_str(),
                |row| typed.is_null(row),
                |row, dst| dst.push(typed.value(row) as u8),
            )?;
            encode_plain_fixed_data_page(value_bytes, typed.len(), null_flags)
        }
        NativeWriterType::SmallInt => {
            let casted = cast_column_to_int16_for_native_writer(
                column,
                column_index,
                column_name,
                "native segment writer",
            )?;
            let typed = casted.as_any().downcast_ref::<Int16Array>().ok_or_else(|| {
                format!(
                    "downcast casted Int16 column failed for native segment writer: column_index={}, column_name={}",
                    column_index, column_name
                )
            })?;
            let (value_bytes, null_flags) = encode_fixed_values_with_nulls(
                typed.len(),
                typed.null_count(),
                is_nullable,
                2,
                column_index,
                column_name,
                "SmallInt",
                |row| typed.is_null(row),
                |row, dst| dst.extend_from_slice(&typed.value(row).to_le_bytes()),
            )?;
            encode_plain_fixed_data_page(value_bytes, typed.len(), null_flags)
        }
        NativeWriterType::Int => {
            let casted = cast_column_to_int32_for_native_writer(
                column,
                column_index,
                column_name,
                "native segment writer",
            )?;
            let typed = casted.as_any().downcast_ref::<Int32Array>().ok_or_else(|| {
                format!(
                    "downcast casted Int32 column failed for native segment writer: column_index={}, column_name={}",
                    column_index, column_name
                )
            })?;
            let (value_bytes, null_flags) = encode_fixed_values_with_nulls(
                typed.len(),
                typed.null_count(),
                is_nullable,
                4,
                column_index,
                column_name,
                "Int",
                |row| typed.is_null(row),
                |row, dst| dst.extend_from_slice(&typed.value(row).to_le_bytes()),
            )?;
            encode_plain_fixed_data_page(value_bytes, typed.len(), null_flags)
        }
        NativeWriterType::Date => {
            let casted = cast_column_to_date32_for_native_writer(
                column,
                column_index,
                column_name,
                "native segment writer",
            )?;
            let typed = casted.as_any().downcast_ref::<Date32Array>().ok_or_else(|| {
                format!(
                    "downcast casted Date32 column failed for native segment writer: column_index={}, column_name={}",
                    column_index, column_name
                )
            })?;
            let mut value_bytes = Vec::with_capacity(typed.len() * 4);
            let mut null_flags = if typed.null_count() > 0 {
                Some(Vec::with_capacity(typed.len()))
            } else {
                None
            };
            for row in 0..typed.len() {
                if typed.is_null(row) {
                    if let Some(flags) = null_flags.as_mut() {
                        flags.push(1);
                    }
                    value_bytes.extend_from_slice(&0_i32.to_le_bytes());
                    continue;
                }
                if let Some(flags) = null_flags.as_mut() {
                    flags.push(0);
                }
                let encoded = encode_date_storage_from_unix_days(typed.value(row)).map_err(|e| {
                    format!(
                        "encode DATE value failed for native segment writer: column_index={}, column_name={}, row={}, error={}",
                        column_index, column_name, row, e
                    )
                })?;
                value_bytes.extend_from_slice(&encoded.to_le_bytes());
            }
            encode_plain_fixed_data_page(value_bytes, typed.len(), null_flags)
        }
        NativeWriterType::BigInt => {
            let casted = cast_column_to_int64_for_native_writer(
                column,
                column_index,
                column_name,
                "native segment writer",
            )?;
            let typed = casted.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                format!(
                    "downcast casted Int64 column failed for native segment writer: column_index={}, column_name={}",
                    column_index, column_name
                )
            })?;
            let (value_bytes, null_flags) = encode_fixed_values_with_nulls(
                typed.len(),
                typed.null_count(),
                is_nullable,
                8,
                column_index,
                column_name,
                "BigInt",
                |row| typed.is_null(row),
                |row, dst| dst.extend_from_slice(&typed.value(row).to_le_bytes()),
            )?;
            encode_plain_fixed_data_page(value_bytes, typed.len(), null_flags)
        }
        NativeWriterType::LargeInt => {
            let values =
                decode_largeint_values(column, column_index, column_name, "data page encoder")?;
            let null_count = values.iter().filter(|v| v.is_none()).count();
            let (value_bytes, null_flags) = encode_fixed_values_with_nulls(
                values.len(),
                null_count,
                is_nullable,
                16,
                column_index,
                column_name,
                "LargeInt",
                |row| values[row].is_none(),
                |row, dst| {
                    let value = values[row].expect(
                        "encode_fixed_values_with_nulls only calls write_non_null for non-null rows",
                    );
                    dst.extend_from_slice(&value.to_le_bytes());
                },
            )?;
            encode_plain_fixed_data_page(value_bytes, values.len(), null_flags)
        }
        NativeWriterType::Decimal32
        | NativeWriterType::Decimal64
        | NativeWriterType::Decimal128
        | NativeWriterType::Decimal256 => {
            let (precision, scale) =
                decimal_meta_from_schema_column(schema_column, column_index, column_name)?;
            if writer_type == NativeWriterType::Decimal256 {
                let casted = cast(
                    column.as_ref(),
                    &arrow::datatypes::DataType::Decimal256(precision, scale),
                )
                .map_err(|e| {
                    format!(
                        "cast column to Decimal256 failed for native segment writer: column_index={}, column_name={}, precision={}, scale={}, error={}",
                        column_index, column_name, precision, scale, e
                    )
                })?;
                let typed = casted
                    .as_any()
                    .downcast_ref::<Decimal256Array>()
                    .ok_or_else(|| {
                        format!(
                            "downcast casted Decimal256 column failed for native segment writer: column_index={}, column_name={}",
                            column_index, column_name
                        )
                    })?;
                let fixed_width = 32usize;
                let mut value_bytes = Vec::with_capacity(typed.len().saturating_mul(fixed_width));
                let mut null_flags = if typed.null_count() > 0 {
                    Some(Vec::with_capacity(typed.len()))
                } else {
                    None
                };
                for row in 0..typed.len() {
                    if typed.is_null(row) {
                        if let Some(flags) = null_flags.as_mut() {
                            flags.push(1);
                        }
                        value_bytes.extend(std::iter::repeat(0_u8).take(fixed_width));
                        continue;
                    }
                    if let Some(flags) = null_flags.as_mut() {
                        flags.push(0);
                    }
                    value_bytes.extend_from_slice(&typed.value(row).to_le_bytes());
                }
                if null_flags.is_some() && !is_nullable {
                    return Err(format!(
                        "NULL values are not allowed for non-nullable native decimal column: column_index={}, column_name={}",
                        column_index, column_name
                    ));
                }
                encode_plain_fixed_data_page(value_bytes, typed.len(), null_flags)
            } else {
                let casted = cast(
                    column.as_ref(),
                    &arrow::datatypes::DataType::Decimal128(precision, scale),
                )
                .map_err(|e| {
                    format!(
                        "cast column to Decimal128 failed for native segment writer: column_index={}, column_name={}, precision={}, scale={}, error={}",
                        column_index, column_name, precision, scale, e
                    )
                })?;
                let typed = casted
                    .as_any()
                    .downcast_ref::<Decimal128Array>()
                    .ok_or_else(|| {
                        format!(
                            "downcast casted Decimal128 column failed for native segment writer: column_index={}, column_name={}",
                            column_index, column_name
                        )
                    })?;

                let fixed_width = match writer_type {
                    NativeWriterType::Decimal32 => 4,
                    NativeWriterType::Decimal64 => 8,
                    NativeWriterType::Decimal128 => 16,
                    _ => unreachable!("invalid decimal writer type"),
                };

                let mut value_bytes = Vec::with_capacity(typed.len().saturating_mul(fixed_width));
                let mut null_flags = if typed.null_count() > 0 {
                    Some(Vec::with_capacity(typed.len()))
                } else {
                    None
                };
                for row in 0..typed.len() {
                    if typed.is_null(row) {
                        if let Some(flags) = null_flags.as_mut() {
                            flags.push(1);
                        }
                        value_bytes.extend(std::iter::repeat(0_u8).take(fixed_width));
                        continue;
                    }
                    if let Some(flags) = null_flags.as_mut() {
                        flags.push(0);
                    }
                    let value = typed.value(row);
                    match writer_type {
                        NativeWriterType::Decimal32 => {
                            let v = i32::try_from(value).map_err(|_| {
                                format!(
                                    "decimal32 overflow for native segment writer: column_index={}, column_name={}, row={}, value={}",
                                    column_index, column_name, row, value
                                )
                            })?;
                            value_bytes.extend_from_slice(&v.to_le_bytes());
                        }
                        NativeWriterType::Decimal64 => {
                            let v = i64::try_from(value).map_err(|_| {
                                format!(
                                    "decimal64 overflow for native segment writer: column_index={}, column_name={}, row={}, value={}",
                                    column_index, column_name, row, value
                                )
                            })?;
                            value_bytes.extend_from_slice(&v.to_le_bytes());
                        }
                        NativeWriterType::Decimal128 => {
                            value_bytes.extend_from_slice(&value.to_le_bytes());
                        }
                        _ => unreachable!("invalid decimal writer type"),
                    }
                }
                if null_flags.is_some() && !is_nullable {
                    return Err(format!(
                        "NULL values are not allowed for non-nullable native decimal column: column_index={}, column_name={}",
                        column_index, column_name
                    ));
                }
                encode_plain_fixed_data_page(value_bytes, typed.len(), null_flags)
            }
        }
        NativeWriterType::Datetime => {
            let casted = cast_column_to_timestamp_micros_for_native_writer(
                column,
                column_index,
                column_name,
                "native segment writer",
            )?;
            let typed = casted
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .ok_or_else(|| {
                    format!(
                        "downcast casted Timestamp(Microsecond,None) column failed for native segment writer: column_index={}, column_name={}",
                        column_index, column_name
                    )
                })?;
            let mut value_bytes = Vec::with_capacity(typed.len() * 8);
            let mut null_flags = if typed.null_count() > 0 {
                Some(Vec::with_capacity(typed.len()))
            } else {
                None
            };
            for row in 0..typed.len() {
                if typed.is_null(row) {
                    if let Some(flags) = null_flags.as_mut() {
                        flags.push(1);
                    }
                    value_bytes.extend_from_slice(&0_i64.to_le_bytes());
                    continue;
                }
                if let Some(flags) = null_flags.as_mut() {
                    flags.push(0);
                }
                let encoded = encode_datetime_storage_from_unix_micros(typed.value(row)).map_err(
                    |e| {
                        format!(
                            "encode DATETIME value failed for native segment writer: column_index={}, column_name={}, row={}, error={}",
                            column_index, column_name, row, e
                        )
                    },
                )?;
                value_bytes.extend_from_slice(&encoded.to_le_bytes());
            }
            encode_plain_fixed_data_page(value_bytes, typed.len(), null_flags)
        }
        NativeWriterType::Float => {
            let casted = cast_column_to_float32_for_native_writer(
                column,
                column_index,
                column_name,
                "native segment writer",
            )?;
            let typed = casted
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| {
                    format!(
                        "downcast casted Float32 column failed for native segment writer: column_index={}, column_name={}",
                        column_index, column_name
                    )
                })?;
            let (value_bytes, null_flags) = encode_fixed_values_with_nulls(
                typed.len(),
                typed.null_count(),
                is_nullable,
                4,
                column_index,
                column_name,
                "Float",
                |row| typed.is_null(row),
                |row, dst| dst.extend_from_slice(&typed.value(row).to_le_bytes()),
            )?;
            encode_plain_fixed_data_page(value_bytes, typed.len(), null_flags)
        }
        NativeWriterType::Double => {
            let casted = cast_column_to_float64_for_native_writer(
                column,
                column_index,
                column_name,
                "native segment writer",
            )?;
            let typed = casted
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| {
                    format!(
                        "downcast casted Float64 column failed for native segment writer: column_index={}, column_name={}",
                        column_index, column_name
                    )
                })?;
            let (value_bytes, null_flags) = encode_fixed_values_with_nulls(
                typed.len(),
                typed.null_count(),
                is_nullable,
                8,
                column_index,
                column_name,
                "Double",
                |row| typed.is_null(row),
                |row, dst| dst.extend_from_slice(&typed.value(row).to_le_bytes()),
            )?;
            encode_plain_fixed_data_page(value_bytes, typed.len(), null_flags)
        }
        NativeWriterType::Boolean => {
            let casted = cast_column_to_boolean_for_native_writer(
                column,
                column_index,
                column_name,
                "native segment writer",
            )?;
            let typed = casted
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| {
                    format!(
                        "downcast casted Boolean column failed for native segment writer: column_index={}, column_name={}",
                        column_index, column_name
                    )
                })?;
            let (value_bytes, null_flags) = encode_fixed_values_with_nulls(
                typed.len(),
                typed.null_count(),
                is_nullable,
                1,
                column_index,
                column_name,
                "Boolean",
                |row| typed.is_null(row),
                |row, dst| dst.push(u8::from(typed.value(row))),
            )?;
            encode_plain_fixed_data_page(value_bytes, typed.len(), null_flags)
        }
        NativeWriterType::Varchar => {
            let casted = cast(column.as_ref(), &arrow::datatypes::DataType::Utf8).map_err(|e| {
                format!(
                    "cast column to Utf8 failed for native segment writer: column_index={}, column_name={}, error={}",
                    column_index, column_name, e
                )
            })?;
            let typed = casted
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    format!(
                        "downcast casted Utf8 column failed for native segment writer: column_index={}, column_name={}",
                        column_index, column_name
                    )
                })?;
            let mut values = Vec::with_capacity(typed.len());
            let mut null_flags = if typed.null_count() > 0 {
                Some(Vec::with_capacity(typed.len()))
            } else {
                None
            };
            for row in 0..typed.len() {
                if typed.is_null(row) {
                    if let Some(flags) = null_flags.as_mut() {
                        flags.push(1);
                    }
                    values.push(Vec::new());
                    continue;
                }
                if let Some(flags) = null_flags.as_mut() {
                    flags.push(0);
                }
                values.push(typed.value(row).as_bytes().to_vec());
            }
            encode_plain_binary_data_page(values, null_flags)
        }
        NativeWriterType::Binary
        | NativeWriterType::VarBinary
        | NativeWriterType::Hll
        | NativeWriterType::Object => {
            let casted = cast(column.as_ref(), &arrow::datatypes::DataType::Binary).map_err(
                |e| {
                    format!(
                        "cast column to Binary failed for native segment writer: column_index={}, column_name={}, error={}",
                        column_index, column_name, e
                    )
                },
            )?;
            let typed = casted
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| {
                    format!(
                        "downcast casted Binary column failed for native segment writer: column_index={}, column_name={}",
                        column_index, column_name
                    )
                })?;
            let mut values = Vec::with_capacity(typed.len());
            let mut null_flags = if typed.null_count() > 0 {
                Some(Vec::with_capacity(typed.len()))
            } else {
                None
            };
            for row in 0..typed.len() {
                if typed.is_null(row) {
                    if let Some(flags) = null_flags.as_mut() {
                        flags.push(1);
                    }
                    values.push(Vec::new());
                    continue;
                }
                if let Some(flags) = null_flags.as_mut() {
                    flags.push(0);
                }
                values.push(typed.value(row).to_vec());
            }
            encode_plain_binary_data_page(values, null_flags)
        }
    }
}

fn ensure_nullable_compatible(
    column: &dyn Array,
    column_index: usize,
    column_name: &str,
    is_nullable: bool,
) -> Result<(), String> {
    if is_nullable || column.null_count() == 0 {
        return Ok(());
    }
    let first_null = (0..column.len())
        .find(|row| column.is_null(*row))
        .unwrap_or(0);
    Err(format!(
        "native segment writer found NULL in non-nullable column: column_index={}, column_name={}, first_null_row={}",
        column_index, column_name, first_null
    ))
}

fn encode_fixed_values_with_nulls<FNull, FValue>(
    num_rows: usize,
    null_count: usize,
    is_nullable: bool,
    null_value_width: usize,
    column_index: usize,
    column_name: &str,
    writer_type: &str,
    mut is_null_at: FNull,
    mut write_non_null: FValue,
) -> Result<(Vec<u8>, Option<Vec<u8>>), String>
where
    FNull: FnMut(usize) -> bool,
    FValue: FnMut(usize, &mut Vec<u8>),
{
    if num_rows == 0 {
        return Err("cannot encode empty native column page".to_string());
    }
    if null_value_width == 0 {
        return Err("native fixed-width null value width must be > 0".to_string());
    }
    if null_count > 0 && !is_nullable {
        let first_null_row = (0..num_rows).find(|row| is_null_at(*row)).unwrap_or(0);
        return Err(format!(
            "NULL values are not allowed for non-nullable native column: column_index={}, column_name={}, writer_type={}, first_null_row={}",
            column_index, column_name, writer_type, first_null_row
        ));
    }

    let mut value_bytes = Vec::with_capacity(num_rows.saturating_mul(null_value_width));
    let mut null_flags = if null_count > 0 {
        Some(Vec::with_capacity(num_rows))
    } else {
        None
    };

    for row in 0..num_rows {
        if is_null_at(row) {
            if let Some(flags) = null_flags.as_mut() {
                flags.push(1);
            }
            value_bytes.extend(std::iter::repeat(0_u8).take(null_value_width));
            continue;
        }
        if let Some(flags) = null_flags.as_mut() {
            flags.push(0);
        }
        write_non_null(row, &mut value_bytes);
    }
    let expected_bytes = num_rows.saturating_mul(null_value_width);
    if value_bytes.len() != expected_bytes {
        return Err(format!(
            "native fixed-width value byte size mismatch: expected={}, actual={}",
            expected_bytes,
            value_bytes.len()
        ));
    }
    Ok((value_bytes, null_flags))
}

fn encode_plain_fixed_data_page(
    value_bytes: Vec<u8>,
    num_values: usize,
    null_flags: Option<Vec<u8>>,
) -> Result<Vec<u8>, String> {
    let num_values_u32 = u32::try_from(num_values)
        .map_err(|_| format!("native page value count overflow: {}", num_values))?;
    let mut body = Vec::with_capacity(4 + value_bytes.len());
    body.extend_from_slice(&num_values_u32.to_le_bytes());
    body.extend_from_slice(&value_bytes);
    encode_plain_data_page_body(body, num_values, null_flags)
}

fn encode_plain_binary_data_page(
    values: Vec<Vec<u8>>,
    null_flags: Option<Vec<u8>>,
) -> Result<Vec<u8>, String> {
    if values.is_empty() {
        return Err("cannot encode empty binary native page".to_string());
    }
    let mut body = Vec::new();
    let mut offsets = Vec::with_capacity(values.len());
    for value in &values {
        let offset = u32::try_from(body.len())
            .map_err(|_| format!("binary value offset overflow: {}", body.len()))?;
        offsets.push(offset);
        body.extend_from_slice(value);
    }
    for offset in offsets {
        body.extend_from_slice(&offset.to_le_bytes());
    }
    let value_count = u32::try_from(values.len())
        .map_err(|_| format!("binary value count overflow: {}", values.len()))?;
    body.extend_from_slice(&value_count.to_le_bytes());
    encode_plain_data_page_body(body, values.len(), null_flags)
}

fn encode_plain_data_page_body(
    mut value_body: Vec<u8>,
    num_values: usize,
    null_flags: Option<Vec<u8>>,
) -> Result<Vec<u8>, String> {
    let (nullmap_bytes, nullmap_size_u32) = if let Some(flags) = null_flags {
        if flags.len() != num_values {
            return Err(format!(
                "native null flag length mismatch: expected={}, actual={}",
                num_values,
                flags.len()
            ));
        }
        let compressed = lz4_flex::block::compress(&flags);
        let size = u32::try_from(compressed.len())
            .map_err(|_| format!("compressed null map too large: {}", compressed.len()))?;
        (compressed, size)
    } else {
        (Vec::new(), 0)
    };
    value_body.extend_from_slice(&nullmap_bytes);

    let page_footer = PageFooterWriterPb {
        r#type: Some(PAGE_TYPE_DATA),
        uncompressed_size: Some(
            u32::try_from(value_body.len())
                .map_err(|_| format!("native data page body too large: {}", value_body.len()))?,
        ),
        data_page_footer: Some(DataPageFooterWriterPb {
            first_ordinal: Some(0),
            num_values: Some(num_values as u64),
            nullmap_size: Some(nullmap_size_u32),
            format_version: Some(2),
            null_encoding: Some(NULL_ENCODING_LZ4),
        }),
        short_key_page_footer: None,
    };
    encode_page_with_footer(value_body, page_footer)
}

fn encode_page_with_footer(
    page_body: Vec<u8>,
    page_footer: PageFooterWriterPb,
) -> Result<Vec<u8>, String> {
    let footer_bytes = page_footer.encode_to_vec();
    let footer_size = u32::try_from(footer_bytes.len())
        .map_err(|_| format!("native page footer too large: {}", footer_bytes.len()))?;

    let mut out = Vec::with_capacity(
        page_body
            .len()
            .saturating_add(footer_bytes.len())
            .saturating_add(8),
    );
    out.extend_from_slice(&page_body);
    out.extend_from_slice(&footer_bytes);
    out.extend_from_slice(&footer_size.to_le_bytes());
    let checksum = crc32c(&out);
    out.extend_from_slice(&checksum.to_le_bytes());
    Ok(out)
}

#[derive(Clone, PartialEq, Message)]
struct SegmentFooterWriterPb {
    #[prost(uint32, optional, tag = "1")]
    version: Option<u32>,
    #[prost(message, repeated, tag = "2")]
    columns: Vec<ColumnMetaWriterPb>,
    #[prost(uint32, optional, tag = "3")]
    num_rows: Option<u32>,
    #[prost(message, optional, tag = "9")]
    short_key_index_page: Option<PagePointerWriterPb>,
}

#[derive(Clone, PartialEq, Message)]
struct ColumnMetaWriterPb {
    #[prost(uint32, optional, tag = "1")]
    column_id: Option<u32>,
    #[prost(uint32, optional, tag = "2")]
    unique_id: Option<u32>,
    #[prost(int32, optional, tag = "3")]
    r#type: Option<i32>,
    #[prost(int32, optional, tag = "5")]
    encoding: Option<i32>,
    #[prost(int32, optional, tag = "6")]
    compression: Option<i32>,
    #[prost(bool, optional, tag = "7")]
    is_nullable: Option<bool>,
    #[prost(message, repeated, tag = "8")]
    indexes: Vec<ColumnIndexMetaWriterPb>,
    #[prost(message, repeated, tag = "10")]
    children_columns: Vec<ColumnMetaWriterPb>,
    #[prost(uint64, optional, tag = "11")]
    num_rows: Option<u64>,
}

#[derive(Clone, PartialEq, Message)]
struct ColumnIndexMetaWriterPb {
    #[prost(int32, optional, tag = "1")]
    r#type: Option<i32>,
    #[prost(message, optional, tag = "7")]
    ordinal_index: Option<OrdinalIndexWriterPb>,
    #[prost(message, optional, tag = "8")]
    zone_map_index: Option<ZoneMapIndexWriterPb>,
}

#[derive(Clone, PartialEq, Message)]
struct OrdinalIndexWriterPb {
    #[prost(message, optional, tag = "1")]
    root_page: Option<BTreeMetaWriterPb>,
}

#[derive(Clone, PartialEq, Message)]
struct BTreeMetaWriterPb {
    #[prost(message, optional, tag = "1")]
    root_page: Option<PagePointerWriterPb>,
    #[prost(bool, optional, tag = "2")]
    is_root_data_page: Option<bool>,
}

#[derive(Clone, PartialEq, Message)]
struct PagePointerWriterPb {
    #[prost(uint64, optional, tag = "1")]
    offset: Option<u64>,
    #[prost(uint32, optional, tag = "2")]
    size: Option<u32>,
}

#[derive(Clone, PartialEq, Message)]
struct ZoneMapWriterPb {
    #[prost(bytes, optional, tag = "1")]
    min: Option<Vec<u8>>,
    #[prost(bytes, optional, tag = "2")]
    max: Option<Vec<u8>>,
    #[prost(bool, optional, tag = "3")]
    has_null: Option<bool>,
    #[prost(bool, optional, tag = "4")]
    has_not_null: Option<bool>,
}

#[derive(Clone, PartialEq, Message)]
struct ZoneMapIndexWriterPb {
    #[prost(message, optional, tag = "1")]
    segment_zone_map: Option<ZoneMapWriterPb>,
    #[prost(message, optional, tag = "2")]
    page_zone_maps: Option<IndexedColumnMetaWriterPb>,
}

#[derive(Clone, PartialEq, Message)]
struct IndexedColumnMetaWriterPb {
    #[prost(int32, optional, tag = "1")]
    data_type: Option<i32>,
    #[prost(int32, optional, tag = "2")]
    encoding: Option<i32>,
    #[prost(int64, optional, tag = "3")]
    num_values: Option<i64>,
    #[prost(message, optional, tag = "4")]
    ordinal_index_meta: Option<BTreeMetaWriterPb>,
    #[prost(int32, optional, tag = "6")]
    compression: Option<i32>,
    #[prost(uint64, optional, tag = "7")]
    size: Option<u64>,
}

#[derive(Clone, PartialEq, Message)]
struct DataPageFooterWriterPb {
    #[prost(uint64, optional, tag = "1")]
    first_ordinal: Option<u64>,
    #[prost(uint64, optional, tag = "2")]
    num_values: Option<u64>,
    #[prost(uint32, optional, tag = "3")]
    nullmap_size: Option<u32>,
    #[prost(uint32, optional, tag = "20")]
    format_version: Option<u32>,
    #[prost(int32, optional, tag = "21")]
    null_encoding: Option<i32>,
}

#[derive(Clone, PartialEq, Message)]
struct ShortKeyFooterWriterPb {
    #[prost(uint32, optional, tag = "1")]
    num_items: Option<u32>,
    #[prost(uint32, optional, tag = "2")]
    key_bytes: Option<u32>,
    #[prost(uint32, optional, tag = "3")]
    offset_bytes: Option<u32>,
    #[prost(uint32, optional, tag = "4")]
    segment_id: Option<u32>,
    #[prost(uint32, optional, tag = "5")]
    num_rows_per_block: Option<u32>,
    #[prost(uint32, optional, tag = "6")]
    num_segment_rows: Option<u32>,
}

#[derive(Clone, PartialEq, Message)]
struct PageFooterWriterPb {
    #[prost(int32, optional, tag = "1")]
    r#type: Option<i32>,
    #[prost(uint32, optional, tag = "2")]
    uncompressed_size: Option<u32>,
    #[prost(message, optional, tag = "7")]
    data_page_footer: Option<DataPageFooterWriterPb>,
    #[prost(message, optional, tag = "10")]
    short_key_page_footer: Option<ShortKeyFooterWriterPb>,
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{
        Array, Date32Array, Int32Array, Int32Builder, Int64Array, Int64Builder, ListBuilder,
        MapBuilder, StructArray,
    };
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use arrow_buffer::NullBufferBuilder;
    use prost::Message;

    use super::{
        COLUMN_INDEX_TYPE_ZONE_MAP_INDEX, LOGICAL_TYPE_ARRAY, LOGICAL_TYPE_BIGINT,
        LOGICAL_TYPE_DATE, LOGICAL_TYPE_INT, LOGICAL_TYPE_LARGEINT, LOGICAL_TYPE_MAP,
        LOGICAL_TYPE_STRUCT, LOGICAL_TYPE_TINYINT, PAGE_TYPE_SHORT_KEY, PageFooterWriterPb,
        SegmentFooterWriterPb, align_batch_columns_to_schema_for_native_writer,
        build_starrocks_native_segment_bytes,
    };
    use crate::common::largeint;
    use crate::service::grpc_client::proto::starrocks::{ColumnPb, KeysType, TabletSchemaPb};

    fn test_tablet_schema(schema_id: i64) -> TabletSchemaPb {
        TabletSchemaPb {
            keys_type: Some(KeysType::DupKeys as i32),
            column: vec![ColumnPb {
                unique_id: 1,
                name: Some("c1".to_string()),
                r#type: "BIGINT".to_string(),
                is_key: Some(true),
                aggregation: None,
                is_nullable: Some(false),
                default_value: None,
                precision: None,
                frac: None,
                length: None,
                index_length: None,
                is_bf_column: None,
                referenced_column_id: None,
                referenced_column: None,
                has_bitmap_index: None,
                visible: None,
                children_columns: Vec::new(),
                is_auto_increment: Some(false),
                agg_state_desc: None,
            }],
            num_short_key_columns: Some(1),
            num_rows_per_row_block: None,
            bf_fpp: None,
            next_column_unique_id: Some(2),
            deprecated_is_in_memory: None,
            deprecated_id: None,
            compression_type: None,
            sort_key_idxes: vec![0],
            schema_version: Some(0),
            sort_key_unique_ids: vec![1],
            table_indices: Vec::new(),
            compression_level: None,
            id: Some(schema_id),
        }
    }

    #[test]
    fn native_segment_alignment_keeps_positional_generated_columns() {
        let schema = TabletSchemaPb {
            keys_type: Some(KeysType::DupKeys as i32),
            column: vec![
                ColumnPb {
                    unique_id: 1,
                    name: Some("col_1".to_string()),
                    r#type: "INT".to_string(),
                    is_key: Some(true),
                    is_nullable: Some(false),
                    ..Default::default()
                },
                ColumnPb {
                    unique_id: 2,
                    name: Some("col_2".to_string()),
                    r#type: "INT".to_string(),
                    is_key: Some(false),
                    is_nullable: Some(false),
                    ..Default::default()
                },
                ColumnPb {
                    unique_id: 3,
                    name: Some("col_3".to_string()),
                    r#type: "INT".to_string(),
                    is_key: Some(false),
                    is_nullable: Some(false),
                    ..Default::default()
                },
            ],
            num_short_key_columns: Some(1),
            sort_key_idxes: vec![0],
            sort_key_unique_ids: vec![1],
            ..Default::default()
        };
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("col_0", DataType::Int32, false),
                Field::new("col_1", DataType::Int32, false),
                Field::new("col_2", DataType::Int32, false),
            ])),
            vec![
                Arc::new(Int32Array::from(vec![10, 20])),
                Arc::new(Int32Array::from(vec![30, 40])),
                Arc::new(Int32Array::from(vec![50, 60])),
            ],
        )
        .expect("build generated-name batch");

        let aligned = align_batch_columns_to_schema_for_native_writer(&batch, &schema)
            .expect("align generated-name batch");
        let c0 = aligned
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("aligned column0");
        let c1 = aligned
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("aligned column1");
        let c2 = aligned
            .column(2)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("aligned column2");

        assert_eq!(c0.value(0), 10);
        assert_eq!(c1.value(0), 30);
        assert_eq!(c2.value(0), 50);
    }

    #[test]
    fn native_segment_footer_contains_short_key_and_zone_map_index() {
        let schema = test_tablet_schema(1);
        let arrow_schema = Arc::new(Schema::new(vec![Field::new("c1", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            arrow_schema,
            vec![Arc::new(Int64Array::from(vec![1_i64, 5_i64, 9_i64]))],
        )
        .expect("build record batch");

        let bytes = build_starrocks_native_segment_bytes(&batch, &schema).expect("build segment");
        let trailer_offset = bytes.len().saturating_sub(12);
        let footer_size = u32::from_le_bytes(
            bytes[trailer_offset..trailer_offset + 4]
                .try_into()
                .expect("footer size bytes"),
        ) as usize;
        let footer_start = trailer_offset.saturating_sub(footer_size);
        let footer = SegmentFooterWriterPb::decode(&bytes[footer_start..trailer_offset])
            .expect("decode segment footer");

        let short_key_page = footer
            .short_key_index_page
            .as_ref()
            .expect("short key page pointer");
        let short_key_offset = short_key_page.offset.unwrap_or(0) as usize;
        let short_key_size = short_key_page.size.unwrap_or(0) as usize;
        assert!(short_key_size > 0, "short key page should not be empty");
        assert!(
            short_key_offset.saturating_add(short_key_size) <= footer_start,
            "short key page pointer out of range"
        );

        let short_key_page_bytes = &bytes[short_key_offset..short_key_offset + short_key_size];
        let short_key_footer_size = u32::from_le_bytes(
            short_key_page_bytes[short_key_page_bytes.len() - 8..short_key_page_bytes.len() - 4]
                .try_into()
                .expect("short key footer size bytes"),
        ) as usize;
        let short_key_footer_start = short_key_page_bytes
            .len()
            .saturating_sub(8)
            .saturating_sub(short_key_footer_size);
        let short_key_footer = PageFooterWriterPb::decode(
            &short_key_page_bytes[short_key_footer_start..short_key_page_bytes.len() - 8],
        )
        .expect("decode short key page footer");
        assert_eq!(short_key_footer.r#type, Some(PAGE_TYPE_SHORT_KEY));
        assert!(
            short_key_footer
                .short_key_page_footer
                .as_ref()
                .and_then(|v| v.num_items)
                .unwrap_or(0)
                > 0,
            "short key page should include index items"
        );

        let zone_map = footer.columns[0]
            .indexes
            .iter()
            .find(|idx| idx.r#type == Some(COLUMN_INDEX_TYPE_ZONE_MAP_INDEX))
            .and_then(|idx| idx.zone_map_index.as_ref())
            .and_then(|idx| idx.segment_zone_map.as_ref())
            .expect("segment zone map index");
        assert_eq!(zone_map.has_not_null, Some(true));
        assert_eq!(zone_map.has_null, Some(false));
        assert_eq!(zone_map.min.as_deref(), Some("1".as_bytes()));
        assert_eq!(zone_map.max.as_deref(), Some("9".as_bytes()));
    }

    #[test]
    fn native_segment_supports_date_column_zone_map() {
        let schema = TabletSchemaPb {
            keys_type: Some(KeysType::DupKeys as i32),
            column: vec![ColumnPb {
                unique_id: 1,
                name: Some("d1".to_string()),
                r#type: "DATE".to_string(),
                is_key: Some(true),
                is_nullable: Some(false),
                ..Default::default()
            }],
            num_short_key_columns: Some(1),
            sort_key_idxes: vec![0],
            sort_key_unique_ids: vec![1],
            ..Default::default()
        };
        let arrow_schema = Arc::new(Schema::new(vec![Field::new("d1", DataType::Date32, false)]));
        let batch = RecordBatch::try_new(
            arrow_schema,
            vec![Arc::new(Date32Array::from(vec![0_i32, 1_i32, 2_i32]))],
        )
        .expect("build date batch");

        let bytes = build_starrocks_native_segment_bytes(&batch, &schema).expect("build segment");
        let trailer_offset = bytes.len().saturating_sub(12);
        let footer_size = u32::from_le_bytes(
            bytes[trailer_offset..trailer_offset + 4]
                .try_into()
                .expect("footer size bytes"),
        ) as usize;
        let footer_start = trailer_offset.saturating_sub(footer_size);
        let footer = SegmentFooterWriterPb::decode(&bytes[footer_start..trailer_offset])
            .expect("decode segment footer");

        assert_eq!(footer.columns[0].r#type, Some(LOGICAL_TYPE_DATE));
        let zone_map = footer.columns[0]
            .indexes
            .iter()
            .find(|idx| idx.r#type == Some(COLUMN_INDEX_TYPE_ZONE_MAP_INDEX))
            .and_then(|idx| idx.zone_map_index.as_ref())
            .and_then(|idx| idx.segment_zone_map.as_ref())
            .expect("segment zone map index");
        assert_eq!(zone_map.min.as_deref(), Some("1970-01-01".as_bytes()));
        assert_eq!(zone_map.max.as_deref(), Some("1970-01-03".as_bytes()));
    }

    #[test]
    fn native_segment_supports_largeint_column_zone_map() {
        let schema = TabletSchemaPb {
            keys_type: Some(KeysType::DupKeys as i32),
            column: vec![ColumnPb {
                unique_id: 1,
                name: Some("l1".to_string()),
                r#type: "LARGEINT".to_string(),
                is_key: Some(false),
                is_nullable: Some(false),
                ..Default::default()
            }],
            num_short_key_columns: Some(0),
            ..Default::default()
        };
        let arrow_schema = Arc::new(Schema::new(vec![Field::new(
            "l1",
            DataType::FixedSizeBinary(largeint::LARGEINT_BYTE_WIDTH),
            false,
        )]));
        let batch = RecordBatch::try_new(
            arrow_schema,
            vec![
                largeint::array_from_i128(&[Some(-5_i128), Some(0_i128), Some(7_i128)])
                    .expect("build largeint array"),
            ],
        )
        .expect("build largeint batch");

        let bytes = build_starrocks_native_segment_bytes(&batch, &schema).expect("build segment");
        let trailer_offset = bytes.len().saturating_sub(12);
        let footer_size = u32::from_le_bytes(
            bytes[trailer_offset..trailer_offset + 4]
                .try_into()
                .expect("footer size bytes"),
        ) as usize;
        let footer_start = trailer_offset.saturating_sub(footer_size);
        let footer = SegmentFooterWriterPb::decode(&bytes[footer_start..trailer_offset])
            .expect("decode segment footer");

        assert_eq!(footer.columns[0].r#type, Some(LOGICAL_TYPE_LARGEINT));
        let zone_map = footer.columns[0]
            .indexes
            .iter()
            .find(|idx| idx.r#type == Some(COLUMN_INDEX_TYPE_ZONE_MAP_INDEX))
            .and_then(|idx| idx.zone_map_index.as_ref())
            .and_then(|idx| idx.segment_zone_map.as_ref())
            .expect("segment zone map index");
        assert_eq!(zone_map.min.as_deref(), Some("-5".as_bytes()));
        assert_eq!(zone_map.max.as_deref(), Some("7".as_bytes()));
    }

    #[test]
    fn native_segment_supports_nullable_array_column_metadata() {
        let item = ColumnPb {
            unique_id: -1,
            name: Some("item".to_string()),
            r#type: "BIGINT".to_string(),
            is_nullable: Some(true),
            ..Default::default()
        };
        let schema = TabletSchemaPb {
            keys_type: Some(KeysType::DupKeys as i32),
            column: vec![ColumnPb {
                unique_id: 10,
                name: Some("arr".to_string()),
                r#type: "ARRAY".to_string(),
                is_nullable: Some(true),
                children_columns: vec![item],
                ..Default::default()
            }],
            num_short_key_columns: Some(0),
            ..Default::default()
        };
        let arrow_schema = Arc::new(Schema::new(vec![Field::new(
            "arr",
            DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
            true,
        )]));
        let mut builder = ListBuilder::new(Int64Builder::new());
        builder.values().append_value(1_i64);
        builder.values().append_value(2_i64);
        builder.append(true);
        builder.append(false);
        builder.values().append_value(7_i64);
        builder.append(true);
        let batch = RecordBatch::try_new(arrow_schema, vec![Arc::new(builder.finish())])
            .expect("build array batch");

        let bytes = build_starrocks_native_segment_bytes(&batch, &schema).expect("build segment");
        let trailer_offset = bytes.len().saturating_sub(12);
        let footer_size = u32::from_le_bytes(
            bytes[trailer_offset..trailer_offset + 4]
                .try_into()
                .expect("footer size bytes"),
        ) as usize;
        let footer_start = trailer_offset.saturating_sub(footer_size);
        let footer = SegmentFooterWriterPb::decode(&bytes[footer_start..trailer_offset])
            .expect("decode segment footer");

        assert_eq!(footer.columns.len(), 1);
        let root = &footer.columns[0];
        assert_eq!(root.r#type, Some(LOGICAL_TYPE_ARRAY));
        assert!(
            root.indexes.is_empty(),
            "ARRAY root should not own scalar indexes"
        );
        assert_eq!(root.children_columns.len(), 3);
        assert_eq!(root.children_columns[0].r#type, Some(LOGICAL_TYPE_BIGINT));
        assert_eq!(root.children_columns[1].r#type, Some(LOGICAL_TYPE_TINYINT));
        assert_eq!(root.children_columns[2].r#type, Some(LOGICAL_TYPE_INT));
        assert_eq!(root.num_rows, Some(3));
        assert_eq!(root.children_columns[0].num_rows, Some(3));
        assert_eq!(root.children_columns[1].num_rows, Some(3));
        assert_eq!(root.children_columns[2].num_rows, Some(3));
    }

    #[test]
    fn native_segment_supports_nullable_map_column_metadata() {
        let key = ColumnPb {
            unique_id: -1,
            name: Some("key".to_string()),
            r#type: "INT".to_string(),
            is_nullable: Some(false),
            ..Default::default()
        };
        let value = ColumnPb {
            unique_id: -1,
            name: Some("value".to_string()),
            r#type: "INT".to_string(),
            is_nullable: Some(true),
            ..Default::default()
        };
        let schema = TabletSchemaPb {
            keys_type: Some(KeysType::DupKeys as i32),
            column: vec![ColumnPb {
                unique_id: 11,
                name: Some("m".to_string()),
                r#type: "MAP".to_string(),
                is_nullable: Some(true),
                children_columns: vec![key, value],
                ..Default::default()
            }],
            num_short_key_columns: Some(0),
            ..Default::default()
        };

        let mut builder = MapBuilder::new(None, Int32Builder::new(), Int32Builder::new());
        builder.keys().append_value(1_i32);
        builder.values().append_value(11_i32);
        builder.keys().append_value(2_i32);
        builder.values().append_value(22_i32);
        builder.append(true).expect("append non-null map row");
        builder.append(false).expect("append null map row");
        builder.keys().append_value(3_i32);
        builder.values().append_value(33_i32);
        builder.append(true).expect("append non-null map row");
        let map_array = Arc::new(builder.finish());
        let arrow_schema = Arc::new(Schema::new(vec![Field::new(
            "m",
            map_array.data_type().clone(),
            true,
        )]));
        let batch = RecordBatch::try_new(arrow_schema, vec![map_array]).expect("build map batch");

        let bytes = build_starrocks_native_segment_bytes(&batch, &schema).expect("build segment");
        let trailer_offset = bytes.len().saturating_sub(12);
        let footer_size = u32::from_le_bytes(
            bytes[trailer_offset..trailer_offset + 4]
                .try_into()
                .expect("footer size bytes"),
        ) as usize;
        let footer_start = trailer_offset.saturating_sub(footer_size);
        let footer = SegmentFooterWriterPb::decode(&bytes[footer_start..trailer_offset])
            .expect("decode segment footer");

        assert_eq!(footer.columns.len(), 1);
        let root = &footer.columns[0];
        assert_eq!(root.r#type, Some(LOGICAL_TYPE_MAP));
        assert!(
            root.indexes.is_empty(),
            "MAP root should not own scalar indexes"
        );
        assert_eq!(root.children_columns.len(), 4);
        assert_eq!(root.children_columns[0].r#type, Some(LOGICAL_TYPE_INT));
        assert_eq!(root.children_columns[1].r#type, Some(LOGICAL_TYPE_INT));
        assert_eq!(root.children_columns[2].r#type, Some(LOGICAL_TYPE_TINYINT));
        assert_eq!(root.children_columns[3].r#type, Some(LOGICAL_TYPE_INT));
        assert_eq!(root.num_rows, Some(3));
        assert_eq!(root.children_columns[0].num_rows, Some(3));
        assert_eq!(root.children_columns[1].num_rows, Some(3));
        assert_eq!(root.children_columns[2].num_rows, Some(3));
        assert_eq!(root.children_columns[3].num_rows, Some(3));
    }

    #[test]
    fn native_segment_supports_nullable_struct_column_metadata() {
        let field_a = ColumnPb {
            unique_id: -1,
            name: Some("a".to_string()),
            r#type: "INT".to_string(),
            is_nullable: Some(false),
            ..Default::default()
        };
        let field_b = ColumnPb {
            unique_id: -1,
            name: Some("b".to_string()),
            r#type: "BIGINT".to_string(),
            is_nullable: Some(true),
            ..Default::default()
        };
        let schema = TabletSchemaPb {
            keys_type: Some(KeysType::DupKeys as i32),
            column: vec![ColumnPb {
                unique_id: 12,
                name: Some("s".to_string()),
                r#type: "STRUCT".to_string(),
                is_nullable: Some(true),
                children_columns: vec![field_a, field_b],
                ..Default::default()
            }],
            num_short_key_columns: Some(0),
            ..Default::default()
        };

        let mut struct_nulls = NullBufferBuilder::new(3);
        struct_nulls.append_non_null();
        struct_nulls.append_null();
        struct_nulls.append_non_null();
        let struct_array = Arc::new(StructArray::new(
            vec![
                Field::new("a", DataType::Int32, false),
                Field::new("b", DataType::Int64, true),
            ]
            .into(),
            vec![
                Arc::new(Int32Array::from(vec![Some(1), Some(0), Some(3)])),
                Arc::new(Int64Array::from(vec![Some(10), None, Some(30)])),
            ],
            struct_nulls.finish(),
        ));
        let arrow_schema = Arc::new(Schema::new(vec![Field::new(
            "s",
            struct_array.data_type().clone(),
            true,
        )]));
        let batch =
            RecordBatch::try_new(arrow_schema, vec![struct_array]).expect("build struct batch");

        let bytes = build_starrocks_native_segment_bytes(&batch, &schema).expect("build segment");
        let trailer_offset = bytes.len().saturating_sub(12);
        let footer_size = u32::from_le_bytes(
            bytes[trailer_offset..trailer_offset + 4]
                .try_into()
                .expect("footer size bytes"),
        ) as usize;
        let footer_start = trailer_offset.saturating_sub(footer_size);
        let footer = SegmentFooterWriterPb::decode(&bytes[footer_start..trailer_offset])
            .expect("decode segment footer");

        assert_eq!(footer.columns.len(), 1);
        let root = &footer.columns[0];
        assert_eq!(root.r#type, Some(LOGICAL_TYPE_STRUCT));
        assert!(
            root.indexes.is_empty(),
            "STRUCT root should not own scalar indexes"
        );
        assert_eq!(root.children_columns.len(), 3);
        assert_eq!(root.children_columns[0].r#type, Some(LOGICAL_TYPE_INT));
        assert_eq!(root.children_columns[1].r#type, Some(LOGICAL_TYPE_BIGINT));
        assert_eq!(root.children_columns[2].r#type, Some(LOGICAL_TYPE_TINYINT));
        assert_eq!(root.num_rows, Some(3));
        assert_eq!(root.children_columns[0].num_rows, Some(3));
        assert_eq!(root.children_columns[1].num_rows, Some(3));
        assert_eq!(root.children_columns[2].num_rows, Some(3));
    }
}
