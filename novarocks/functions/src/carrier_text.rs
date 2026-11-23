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
//! Original primitive-to-VARCHAR computation shared by selected casts and v1.
use arrow_array::*;
use arrow_schema::{DataType, TimeUnit};
use chrono::DateTime;

/// Render one already evaluated, non-NULL primitive value. The caller owns
/// address, NULL and control validation; no arena or runtime policy is read.
pub fn render(array: &dyn Array, row: usize) -> Result<String, CarrierTextFailure> {
    macro_rules! integer {
        ($ty:ty) => {
            array
                .as_any()
                .downcast_ref::<$ty>()
                .ok_or_else(|| CarrierTextFailure::new("text cast carrier downcast failed"))?
                .value(row)
                .to_string()
        };
    }
    Ok(match array.data_type() {
        DataType::Boolean => integer!(BooleanArray),
        DataType::Int8 => integer!(Int8Array),
        DataType::Int16 => integer!(Int16Array),
        DataType::Int32 => integer!(Int32Array),
        DataType::Int64 => integer!(Int64Array),
        DataType::UInt8 => integer!(UInt8Array),
        DataType::UInt16 => integer!(UInt16Array),
        DataType::UInt32 => integer!(UInt32Array),
        DataType::UInt64 => integer!(UInt64Array),
        DataType::Float32 => format_float32_for_varchar(
            array
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| CarrierTextFailure::new("text cast carrier downcast failed"))?
                .value(row),
        ),
        DataType::Float64 => format_float64_for_varchar(
            array
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| CarrierTextFailure::new("text cast carrier downcast failed"))?
                .value(row),
        ),
        DataType::Timestamp(unit, tz) => {
            let value = match unit {
                TimeUnit::Second => array
                    .as_any()
                    .downcast_ref::<TimestampSecondArray>()
                    .ok_or_else(|| CarrierTextFailure::new("text cast carrier downcast failed"))?
                    .value(row),
                TimeUnit::Millisecond => array
                    .as_any()
                    .downcast_ref::<TimestampMillisecondArray>()
                    .ok_or_else(|| CarrierTextFailure::new("text cast carrier downcast failed"))?
                    .value(row),
                TimeUnit::Microsecond => array
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .ok_or_else(|| CarrierTextFailure::new("text cast carrier downcast failed"))?
                    .value(row),
                TimeUnit::Nanosecond => array
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .ok_or_else(|| CarrierTextFailure::new("text cast carrier downcast failed"))?
                    .value(row),
            };
            format_timestamp_for_varchar(unit, value, tz.as_deref())
        }
        _ => {
            return Err(CarrierTextFailure::new(
                "text cast requires an exact primitive carrier",
            ));
        }
    })
}

#[derive(Debug)]
pub struct CarrierTextFailure(novarocks_failure_contract::BoundedDetail);
impl CarrierTextFailure {
    fn new(detail: impl std::fmt::Display) -> Self {
        Self(novarocks_failure_contract::BoundedDetail::truncating_display(detail))
    }
}
impl std::fmt::Display for CarrierTextFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0.as_str())
    }
}
impl std::error::Error for CarrierTextFailure {}
impl novarocks_failure_contract::FailureProjection for CarrierTextFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use novarocks_failure_contract::{FailureClass, FailureFact, FailureReason, domain};
        FailureFact::new(
            FailureClass::Invariant,
            FailureReason::new(domain::KERNEL, std::num::NonZeroU16::new(206).unwrap()),
        )
    }
}
impl novarocks_failure_contract::OuterFailureProjection for CarrierTextFailure {}
#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    #[test]
    fn primitive_integer_text_matches_original_arrow_rendering() {
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(Int8Array::from(vec![Some(i8::MIN), None, Some(i8::MAX)])),
            Arc::new(Int16Array::from(vec![Some(i16::MIN), None, Some(i16::MAX)])),
            Arc::new(Int32Array::from(vec![Some(i32::MIN), None, Some(i32::MAX)])),
            Arc::new(Int64Array::from(vec![Some(i64::MIN), None, Some(i64::MAX)])),
            Arc::new(UInt8Array::from(vec![Some(0), None, Some(u8::MAX)])),
            Arc::new(UInt16Array::from(vec![Some(0), None, Some(u16::MAX)])),
            Arc::new(UInt32Array::from(vec![Some(0), None, Some(u32::MAX)])),
            Arc::new(UInt64Array::from(vec![Some(0), None, Some(u64::MAX)])),
            Arc::new(BooleanArray::from(vec![Some(true), None, Some(false)])),
        ];
        for array in arrays {
            let original = arrow_cast::cast(&array, &DataType::Utf8).unwrap();
            let original = original.as_any().downcast_ref::<StringArray>().unwrap();
            for row in [0, 2] {
                assert_eq!(render(array.as_ref(), row).unwrap(), original.value(row));
            }
        }
    }
}

pub fn format_float64_for_varchar(value: f64) -> String {
    if value == 0.0 {
        return "0".to_string();
    }
    if value.is_nan() {
        return "nan".to_string();
    }
    if value.is_infinite() {
        return if value.is_sign_negative() {
            "-inf".to_string()
        } else {
            "inf".to_string()
        };
    }
    let mut buf = ryu::Buffer::new();
    let formatted = buf.format(value);
    normalize_float_string_for_varchar(formatted)
}

pub fn format_float32_for_varchar(value: f32) -> String {
    if value == 0.0 {
        return "0".to_string();
    }
    if value.is_nan() {
        return "nan".to_string();
    }
    if value.is_infinite() {
        return if value.is_sign_negative() {
            "-inf".to_string()
        } else {
            "inf".to_string()
        };
    }
    let mut buf = ryu::Buffer::new();
    let formatted = buf.format(value);
    normalize_float_string_for_varchar(formatted)
}

fn normalize_float_string_for_varchar(formatted: &str) -> String {
    let stripped = formatted.strip_suffix(".0").unwrap_or(formatted);
    if let Some(exp_pos) = stripped.find('e') {
        let mut out = String::with_capacity(stripped.len() + 1);
        out.push_str(&stripped[..=exp_pos]);
        if let Some(sign_or_digit) = stripped.as_bytes().get(exp_pos + 1) {
            if *sign_or_digit == b'+' || *sign_or_digit == b'-' {
                out.push_str(&stripped[exp_pos + 1..]);
            } else {
                out.push('+');
                out.push_str(&stripped[exp_pos + 1..]);
            }
        }
        out
    } else {
        stripped.to_string()
    }
}

pub fn format_timestamp_for_varchar(unit: &TimeUnit, value: i64, tz: Option<&str>) -> String {
    let timestamp_str = match unit {
        TimeUnit::Second => {
            let dt = DateTime::from_timestamp(value, 0)
                .unwrap_or_else(|| DateTime::from_timestamp(0, 0).unwrap());
            dt.naive_utc().format("%Y-%m-%d %H:%M:%S").to_string()
        }
        TimeUnit::Millisecond => {
            let seconds = value.div_euclid(1_000);
            let millis = value.rem_euclid(1_000) as u32;
            let dt = DateTime::from_timestamp(seconds, millis * 1_000_000)
                .unwrap_or_else(|| DateTime::from_timestamp(0, 0).unwrap());
            if millis == 0 {
                dt.naive_utc().format("%Y-%m-%d %H:%M:%S").to_string()
            } else {
                dt.naive_utc().format("%Y-%m-%d %H:%M:%S%.3f").to_string()
            }
        }
        TimeUnit::Microsecond => {
            let seconds = value.div_euclid(1_000_000);
            let micros = value.rem_euclid(1_000_000) as u32;
            let dt = DateTime::from_timestamp(seconds, micros * 1_000)
                .unwrap_or_else(|| DateTime::from_timestamp(0, 0).unwrap());
            if micros == 0 {
                dt.naive_utc().format("%Y-%m-%d %H:%M:%S").to_string()
            } else {
                dt.naive_utc().format("%Y-%m-%d %H:%M:%S%.6f").to_string()
            }
        }
        TimeUnit::Nanosecond => {
            let seconds = value.div_euclid(1_000_000_000);
            let nanos = value.rem_euclid(1_000_000_000) as u32;
            let dt = DateTime::from_timestamp(seconds, nanos)
                .unwrap_or_else(|| DateTime::from_timestamp(0, 0).unwrap());
            if nanos == 0 {
                dt.naive_utc().format("%Y-%m-%d %H:%M:%S").to_string()
            } else {
                dt.naive_utc().format("%Y-%m-%d %H:%M:%S%.9f").to_string()
            }
        }
    };
    if let Some(tz) = tz {
        format!("{timestamp_str} {tz}")
    } else {
        timestamp_str
    }
}
