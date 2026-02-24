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
    Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float32Array,
    Float64Array, Int8Array, Int16Array, Int32Array, Int64Array, StringArray,
    TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray,
};
use arrow::datatypes::{DataType, TimeUnit};
use chrono::{DateTime, NaiveDate};

use super::AggKernelEntry;

pub enum AggInputView<'a> {
    None,
    Any(&'a ArrayRef),
    Int(IntArrayView<'a>),
    Float(FloatArrayView<'a>),
    Bool(&'a BooleanArray),
    Utf8(Utf8ArrayView<'a>),
    Binary(&'a BinaryArray),
    AvgState(AvgStateView<'a>),
    AvgDecimalState(AvgDecimalStateView<'a>),
}

pub struct AvgStateView<'a> {
    pub(crate) sums: FloatArrayView<'a>,
    pub(crate) counts: IntArrayView<'a>,
}

impl<'a> AvgStateView<'a> {
    pub(crate) fn value_at(&self, row: usize) -> Option<(f64, i64)> {
        let sum = self.sums.value_at(row)?;
        let count = self.counts.value_at(row)?;
        Some((sum, count))
    }
}

pub struct AvgDecimalStateView<'a> {
    pub(crate) sums: &'a Decimal128Array,
    pub(crate) counts: IntArrayView<'a>,
}

impl<'a> AvgDecimalStateView<'a> {
    pub(crate) fn value_at(&self, row: usize) -> Option<(i128, i64)> {
        if self.sums.is_null(row) {
            return None;
        }
        let sum = self.sums.value(row);
        let count = self.counts.value_at(row)?;
        Some((sum, count))
    }
}

pub fn build_agg_input_views_from_kernels<'a>(
    kernels: &[AggKernelEntry],
    arrays: &'a [Option<ArrayRef>],
) -> Result<Vec<AggInputView<'a>>, String> {
    let mut views = Vec::with_capacity(kernels.len());
    for (idx, kernel) in kernels.iter().enumerate() {
        let array = arrays
            .get(idx)
            .ok_or_else(|| "aggregate input missing".to_string())?;
        views.push(kernel.build_input_view(array)?);
    }
    Ok(views)
}

pub fn build_agg_merge_views_from_kernels<'a>(
    kernels: &[AggKernelEntry],
    arrays: &'a [Option<ArrayRef>],
) -> Result<Vec<AggInputView<'a>>, String> {
    let mut views = Vec::with_capacity(kernels.len());
    for (idx, kernel) in kernels.iter().enumerate() {
        let array = arrays
            .get(idx)
            .ok_or_else(|| "aggregate input missing".to_string())?;
        views.push(kernel.build_merge_view(array)?);
    }
    Ok(views)
}

#[derive(Clone, Debug)]
pub enum IntArrayView<'a> {
    Int64(&'a Int64Array),
    Int32(&'a Int32Array),
    Int16(&'a Int16Array),
    Int8(&'a Int8Array),
}

impl<'a> IntArrayView<'a> {
    pub fn new(array: &'a ArrayRef) -> Result<Self, String> {
        match array.data_type() {
            DataType::Int64 => array
                .as_any()
                .downcast_ref::<Int64Array>()
                .map(Self::Int64)
                .ok_or_else(|| "failed to downcast to Int64Array".to_string()),
            DataType::Int32 => array
                .as_any()
                .downcast_ref::<Int32Array>()
                .map(Self::Int32)
                .ok_or_else(|| "failed to downcast to Int32Array".to_string()),
            DataType::Int16 => array
                .as_any()
                .downcast_ref::<Int16Array>()
                .map(Self::Int16)
                .ok_or_else(|| "failed to downcast to Int16Array".to_string()),
            DataType::Int8 => array
                .as_any()
                .downcast_ref::<Int8Array>()
                .map(Self::Int8)
                .ok_or_else(|| "failed to downcast to Int8Array".to_string()),
            other => Err(format!("unsupported int input type: {:?}", other)),
        }
    }

    pub fn value_at(&self, row: usize) -> Option<i64> {
        match self {
            IntArrayView::Int64(arr) => (!arr.is_null(row)).then(|| arr.value(row)),
            IntArrayView::Int32(arr) => (!arr.is_null(row)).then(|| arr.value(row) as i64),
            IntArrayView::Int16(arr) => (!arr.is_null(row)).then(|| arr.value(row) as i64),
            IntArrayView::Int8(arr) => (!arr.is_null(row)).then(|| arr.value(row) as i64),
        }
    }
}

#[derive(Clone, Debug)]
pub enum FloatArrayView<'a> {
    Float64(&'a Float64Array),
    Float32(&'a Float32Array),
}

impl<'a> FloatArrayView<'a> {
    pub fn new(array: &'a ArrayRef) -> Result<Self, String> {
        match array.data_type() {
            DataType::Float64 => array
                .as_any()
                .downcast_ref::<Float64Array>()
                .map(Self::Float64)
                .ok_or_else(|| "failed to downcast to Float64Array".to_string()),
            DataType::Float32 => array
                .as_any()
                .downcast_ref::<Float32Array>()
                .map(Self::Float32)
                .ok_or_else(|| "failed to downcast to Float32Array".to_string()),
            other => Err(format!("unsupported float input type: {:?}", other)),
        }
    }

    pub fn value_at(&self, row: usize) -> Option<f64> {
        match self {
            FloatArrayView::Float64(arr) => (!arr.is_null(row)).then(|| arr.value(row)),
            FloatArrayView::Float32(arr) => (!arr.is_null(row)).then(|| arr.value(row) as f64),
        }
    }
}

#[derive(Clone, Debug)]
pub enum Utf8ArrayView<'a> {
    Utf8(&'a StringArray),
    Date32(&'a Date32Array),
    TimestampSecond(&'a TimestampSecondArray, Option<String>),
    TimestampMillisecond(&'a TimestampMillisecondArray, Option<String>),
    TimestampMicrosecond(&'a TimestampMicrosecondArray, Option<String>),
    TimestampNanosecond(&'a TimestampNanosecondArray, Option<String>),
    Decimal128(&'a Decimal128Array, i8),
}

impl<'a> Utf8ArrayView<'a> {
    pub fn len(&self) -> usize {
        match self {
            Utf8ArrayView::Utf8(arr) => arr.len(),
            Utf8ArrayView::Date32(arr) => arr.len(),
            Utf8ArrayView::TimestampSecond(arr, _) => arr.len(),
            Utf8ArrayView::TimestampMillisecond(arr, _) => arr.len(),
            Utf8ArrayView::TimestampMicrosecond(arr, _) => arr.len(),
            Utf8ArrayView::TimestampNanosecond(arr, _) => arr.len(),
            Utf8ArrayView::Decimal128(arr, _) => arr.len(),
        }
    }

    pub fn new(array: &'a ArrayRef) -> Result<Self, String> {
        match array.data_type() {
            DataType::Utf8 => array
                .as_any()
                .downcast_ref::<StringArray>()
                .map(Self::Utf8)
                .ok_or_else(|| "failed to downcast to StringArray".to_string()),
            DataType::Date32 => array
                .as_any()
                .downcast_ref::<Date32Array>()
                .map(Self::Date32)
                .ok_or_else(|| "failed to downcast to Date32Array".to_string()),
            DataType::Timestamp(unit, tz) => {
                let tz_string = tz.as_deref().map(|s| s.to_string());
                match unit {
                    TimeUnit::Second => array
                        .as_any()
                        .downcast_ref::<TimestampSecondArray>()
                        .map(|arr| Self::TimestampSecond(arr, tz_string))
                        .ok_or_else(|| "failed to downcast to TimestampSecondArray".to_string()),
                    TimeUnit::Millisecond => array
                        .as_any()
                        .downcast_ref::<TimestampMillisecondArray>()
                        .map(|arr| Self::TimestampMillisecond(arr, tz_string))
                        .ok_or_else(|| {
                            "failed to downcast to TimestampMillisecondArray".to_string()
                        }),
                    TimeUnit::Microsecond => array
                        .as_any()
                        .downcast_ref::<TimestampMicrosecondArray>()
                        .map(|arr| Self::TimestampMicrosecond(arr, tz_string))
                        .ok_or_else(|| {
                            "failed to downcast to TimestampMicrosecondArray".to_string()
                        }),
                    TimeUnit::Nanosecond => array
                        .as_any()
                        .downcast_ref::<TimestampNanosecondArray>()
                        .map(|arr| Self::TimestampNanosecond(arr, tz_string))
                        .ok_or_else(|| {
                            "failed to downcast to TimestampNanosecondArray".to_string()
                        }),
                }
            }
            DataType::Decimal128(_, scale) => array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .map(|arr| Self::Decimal128(arr, *scale))
                .ok_or_else(|| "failed to downcast to Decimal128Array".to_string()),
            other => Err(format!("unsupported utf8 input type: {:?}", other)),
        }
    }

    pub fn value_at(&self, row: usize) -> Option<String> {
        match self {
            Utf8ArrayView::Utf8(arr) => (!arr.is_null(row)).then(|| arr.value(row).to_string()),
            Utf8ArrayView::Date32(arr) => {
                if arr.is_null(row) {
                    None
                } else {
                    let days = arr.value(row);
                    Some(format_date32(days))
                }
            }
            Utf8ArrayView::TimestampSecond(arr, tz) => {
                if arr.is_null(row) {
                    None
                } else {
                    let seconds = arr.value(row);
                    Some(format_timestamp(TimeUnit::Second, seconds, tz.as_deref()))
                }
            }
            Utf8ArrayView::TimestampMillisecond(arr, tz) => {
                if arr.is_null(row) {
                    None
                } else {
                    let millis = arr.value(row);
                    Some(format_timestamp(
                        TimeUnit::Millisecond,
                        millis,
                        tz.as_deref(),
                    ))
                }
            }
            Utf8ArrayView::TimestampMicrosecond(arr, tz) => {
                if arr.is_null(row) {
                    None
                } else {
                    let micros = arr.value(row);
                    Some(format_timestamp(
                        TimeUnit::Microsecond,
                        micros,
                        tz.as_deref(),
                    ))
                }
            }
            Utf8ArrayView::TimestampNanosecond(arr, tz) => {
                if arr.is_null(row) {
                    None
                } else {
                    let nanos = arr.value(row);
                    Some(format_timestamp(TimeUnit::Nanosecond, nanos, tz.as_deref()))
                }
            }
            Utf8ArrayView::Decimal128(arr, scale) => {
                if arr.is_null(row) {
                    None
                } else {
                    Some(format_decimal(arr.value(row), *scale))
                }
            }
        }
    }
}

fn format_date32(days_since_epoch: i32) -> String {
    let date = NaiveDate::from_num_days_from_ce_opt(719163 + days_since_epoch)
        .unwrap_or_else(|| NaiveDate::from_ymd_opt(1970, 1, 1).unwrap());
    date.format("%Y-%m-%d").to_string()
}

fn format_timestamp(unit: TimeUnit, value: i64, tz: Option<&str>) -> String {
    let timestamp_str = match unit {
        TimeUnit::Second => {
            let dt = DateTime::from_timestamp(value, 0)
                .unwrap_or_else(|| DateTime::from_timestamp(0, 0).unwrap());
            dt.naive_utc().format("%Y-%m-%d %H:%M:%S").to_string()
        }
        TimeUnit::Millisecond => {
            let seconds = value / 1000;
            let nanos = ((value % 1000) * 1_000_000) as u32;
            let dt = DateTime::from_timestamp(seconds, nanos)
                .unwrap_or_else(|| DateTime::from_timestamp(0, 0).unwrap());
            dt.naive_utc().format("%Y-%m-%d %H:%M:%S%.3f").to_string()
        }
        TimeUnit::Microsecond => {
            let seconds = value / 1_000_000;
            let nanos = ((value % 1_000_000) * 1000) as u32;
            let dt = DateTime::from_timestamp(seconds, nanos)
                .unwrap_or_else(|| DateTime::from_timestamp(0, 0).unwrap());
            dt.naive_utc().format("%Y-%m-%d %H:%M:%S%.6f").to_string()
        }
        TimeUnit::Nanosecond => {
            let seconds = value / 1_000_000_000;
            let nanos = (value % 1_000_000_000) as u32;
            let dt = DateTime::from_timestamp(seconds, nanos)
                .unwrap_or_else(|| DateTime::from_timestamp(0, 0).unwrap());
            dt.naive_utc().format("%Y-%m-%d %H:%M:%S%.9f").to_string()
        }
    };
    if let Some(tz) = tz {
        format!("{} {}", timestamp_str, tz)
    } else {
        timestamp_str
    }
}

fn format_decimal(unscaled: i128, scale: i8) -> String {
    let scale = scale as i32;
    if scale <= 0 {
        return unscaled.to_string();
    }

    let unscaled_str = unscaled.abs().to_string();
    let scale_usize = scale as usize;

    if unscaled_str.len() <= scale_usize {
        let padded = format!("{:0>width$}", unscaled_str, width = scale_usize);
        if unscaled < 0 {
            format!("-0.{}", padded)
        } else {
            format!("0.{}", padded)
        }
    } else {
        let split_pos = unscaled_str.len() - scale_usize;
        let integer_part = &unscaled_str[..split_pos];
        let fractional_part = &unscaled_str[split_pos..];
        if unscaled < 0 {
            format!("-{}.{}", integer_part, fractional_part)
        } else {
            format!("{}.{}", integer_part, fractional_part)
        }
    }
}
