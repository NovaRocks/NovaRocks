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
use crate::exec::chunk::Chunk;
use crate::exec::expr::{ExprArena, ExprId};
use arrow::array::{
    Array, ArrayRef, Date32Array, Int16Array, Int32Array, Int64Array, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray,
};
use arrow::datatypes::{DataType, TimeUnit};
use chrono::{DateTime, Datelike, NaiveDate};
use std::sync::Arc;

// YEAR function for Arrow arrays
pub fn eval_year(
    arena: &ExprArena,
    year_expr_id: ExprId,
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    // Get the child expression (input date/timestamp)
    // For FunctionCall, the first argument is the input
    let child_expr_id = match arena.node(year_expr_id) {
        Some(crate::exec::expr::ExprNode::FunctionCall { args, .. }) => args
            .first()
            .copied()
            .ok_or_else(|| "year: missing argument".to_string())?,
        _ => return Err("Year expression node type mismatch".to_string()),
    };

    let array = arena.eval(child_expr_id, chunk)?;
    let len = array.len();

    // Get expected return type from arena (for the Year expression itself)
    let expected_type = arena
        .data_type(year_expr_id)
        .cloned()
        .unwrap_or(DataType::Int64);

    let result: ArrayRef = match array.data_type() {
        DataType::Date32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(|| "failed to downcast to Date32Array".to_string())?;

            let values: Vec<Option<i32>> = (0..len)
                .map(|i| {
                    if arr.is_null(i) {
                        None
                    } else {
                        let days_since_epoch = arr.value(i);
                        // Date32 represents days since 1970-01-01
                        // 719163 is the Julian day number for 1970-01-01
                        let date =
                            NaiveDate::from_num_days_from_ce_opt(719163 + days_since_epoch as i32)
                                .unwrap_or_else(|| NaiveDate::from_ymd_opt(1970, 1, 1).unwrap());
                        Some(date.year())
                    }
                })
                .collect();

            // Build result array based on expected type
            match expected_type {
                DataType::Int16 => {
                    let int16_values: Vec<Option<i16>> = values
                        .iter()
                        .map(|opt_year| opt_year.and_then(|y| i16::try_from(y).ok()))
                        .collect();
                    Arc::new(Int16Array::from(int16_values))
                }
                DataType::Int32 => Arc::new(Int32Array::from(values)),
                _ => {
                    let int64_values: Vec<Option<i64>> = values
                        .iter()
                        .map(|opt_year| opt_year.map(|y| y as i64))
                        .collect();
                    Arc::new(Int64Array::from(int64_values))
                }
            }
        }
        DataType::Timestamp(unit, _tz) => {
            let values: Vec<Option<i32>> = match unit {
                TimeUnit::Second => {
                    let arr = array
                        .as_any()
                        .downcast_ref::<TimestampSecondArray>()
                        .ok_or_else(|| "failed to downcast to TimestampSecondArray".to_string())?;
                    (0..len)
                        .map(|i| {
                            if arr.is_null(i) {
                                None
                            } else {
                                let seconds = arr.value(i);
                                let dt_utc = DateTime::from_timestamp(seconds, 0)
                                    .unwrap_or_else(|| DateTime::from_timestamp(0, 0).unwrap());
                                Some(dt_utc.year())
                            }
                        })
                        .collect()
                }
                TimeUnit::Millisecond => {
                    let arr = array
                        .as_any()
                        .downcast_ref::<TimestampMillisecondArray>()
                        .ok_or_else(|| {
                            "failed to downcast to TimestampMillisecondArray".to_string()
                        })?;
                    (0..len)
                        .map(|i| {
                            if arr.is_null(i) {
                                None
                            } else {
                                let millis = arr.value(i);
                                let seconds = millis / 1000;
                                let nanos = ((millis % 1000) * 1_000_000) as u32;
                                let dt_utc = DateTime::from_timestamp(seconds, nanos)
                                    .unwrap_or_else(|| DateTime::from_timestamp(0, 0).unwrap());
                                Some(dt_utc.year())
                            }
                        })
                        .collect()
                }
                TimeUnit::Microsecond => {
                    let arr = array
                        .as_any()
                        .downcast_ref::<TimestampMicrosecondArray>()
                        .ok_or_else(|| {
                            "failed to downcast to TimestampMicrosecondArray".to_string()
                        })?;
                    (0..len)
                        .map(|i| {
                            if arr.is_null(i) {
                                None
                            } else {
                                let micros = arr.value(i);
                                let seconds = micros / 1_000_000;
                                let nanos = ((micros % 1_000_000) * 1000) as u32;
                                let dt_utc = DateTime::from_timestamp(seconds, nanos)
                                    .unwrap_or_else(|| DateTime::from_timestamp(0, 0).unwrap());
                                Some(dt_utc.year())
                            }
                        })
                        .collect()
                }
                TimeUnit::Nanosecond => {
                    let arr = array
                        .as_any()
                        .downcast_ref::<TimestampNanosecondArray>()
                        .ok_or_else(|| {
                            "failed to downcast to TimestampNanosecondArray".to_string()
                        })?;
                    (0..len)
                        .map(|i| {
                            if arr.is_null(i) {
                                None
                            } else {
                                let nanos_total = arr.value(i);
                                let seconds = nanos_total / 1_000_000_000;
                                let nanos = (nanos_total % 1_000_000_000) as u32;
                                let dt_utc = DateTime::from_timestamp(seconds, nanos)
                                    .unwrap_or_else(|| DateTime::from_timestamp(0, 0).unwrap());
                                Some(dt_utc.year())
                            }
                        })
                        .collect()
                }
            };

            // Build result array based on expected type
            match expected_type {
                DataType::Int16 => {
                    let int16_values: Vec<Option<i16>> = values
                        .iter()
                        .map(|opt_year| opt_year.and_then(|y| i16::try_from(y).ok()))
                        .collect();
                    Arc::new(Int16Array::from(int16_values))
                }
                DataType::Int32 => Arc::new(Int32Array::from(values)),
                _ => {
                    let int64_values: Vec<Option<i64>> = values
                        .iter()
                        .map(|opt_year| opt_year.map(|y| y as i64))
                        .collect();
                    Arc::new(Int64Array::from(int64_values))
                }
            }
        }
        DataType::Utf8 => {
            // Parse string dates/timestamps
            let arr = array
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .ok_or_else(|| "failed to downcast to StringArray".to_string())?;

            let values: Vec<Option<i32>> = (0..len)
                .map(|i| {
                    if arr.is_null(i) {
                        None
                    } else {
                        let s = arr.value(i);
                        // Try to parse as date or timestamp
                        if let Ok(date) = NaiveDate::parse_from_str(s, "%Y-%m-%d") {
                            Some(date.year())
                        } else if let Ok(dt) =
                            chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
                        {
                            Some(dt.date().year())
                        } else if let Ok(dt) =
                            chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f")
                        {
                            Some(dt.date().year())
                        } else {
                            None // Return null for unparseable strings
                        }
                    }
                })
                .collect();

            // Build result array based on expected type
            match expected_type {
                DataType::Int16 => {
                    let int16_values: Vec<Option<i16>> = values
                        .iter()
                        .map(|opt_year| opt_year.and_then(|y| i16::try_from(y).ok()))
                        .collect();
                    Arc::new(Int16Array::from(int16_values))
                }
                DataType::Int32 => Arc::new(Int32Array::from(values)),
                _ => {
                    let int64_values: Vec<Option<i64>> = values
                        .iter()
                        .map(|opt_year| opt_year.map(|y| y as i64))
                        .collect();
                    Arc::new(Int64Array::from(int64_values))
                }
            }
        }
        _ => {
            return Err(format!(
                "year: unsupported input type: {:?}",
                array.data_type()
            ));
        }
    };

    Ok(result)
}
