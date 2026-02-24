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
    Array, ArrayRef, Date32Array, Decimal128Array, Float32Array, Float64Array, Int8Array,
    Int16Array, Int32Array, Int64Array, StringArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt8Array,
    UInt16Array, UInt32Array, UInt64Array,
};
use arrow::datatypes::{DataType, TimeUnit};
use chrono::{DateTime, Datelike, NaiveDate, NaiveDateTime, NaiveTime, TimeZone, Timelike, Utc};

pub const UNIX_EPOCH_DAY_OFFSET: i32 = 719163; // 1970-01-01 in Julian days
pub const BC_EPOCH_JULIAN: i32 = 1721060; // from StarRocks time_types.h

pub fn date32_to_naive(days: i32) -> Option<NaiveDate> {
    NaiveDate::from_num_days_from_ce_opt(UNIX_EPOCH_DAY_OFFSET + days)
}

pub fn naive_to_date32(date: NaiveDate) -> i32 {
    date.num_days_from_ce() - UNIX_EPOCH_DAY_OFFSET
}

pub fn timestamp_to_naive(unit: &TimeUnit, value: i64) -> Option<NaiveDateTime> {
    match unit {
        TimeUnit::Second => DateTime::<Utc>::from_timestamp(value, 0).map(|dt| dt.naive_utc()),
        TimeUnit::Millisecond => {
            let secs = value.div_euclid(1_000);
            let nanos = (value.rem_euclid(1_000) as u32) * 1_000_000;
            DateTime::<Utc>::from_timestamp(secs, nanos).map(|dt| dt.naive_utc())
        }
        TimeUnit::Microsecond => {
            let secs = value.div_euclid(1_000_000);
            let nanos = (value.rem_euclid(1_000_000) as u32) * 1_000;
            DateTime::<Utc>::from_timestamp(secs, nanos).map(|dt| dt.naive_utc())
        }
        TimeUnit::Nanosecond => {
            let secs = value.div_euclid(1_000_000_000);
            let nanos = value.rem_euclid(1_000_000_000) as u32;
            DateTime::<Utc>::from_timestamp(secs, nanos).map(|dt| dt.naive_utc())
        }
    }
}

pub fn naive_to_timestamp_micros(dt: NaiveDateTime) -> i64 {
    dt.and_utc().timestamp_micros()
}

pub fn parse_date(s: &str) -> Option<NaiveDate> {
    NaiveDate::parse_from_str(s, "%Y-%m-%d")
        .or_else(|_| NaiveDate::parse_from_str(s, "%Y%m%d"))
        .ok()
}

pub fn parse_datetime(s: &str) -> Option<NaiveDateTime> {
    NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
        .or_else(|_| NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f"))
        .or_else(|_| NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S"))
        .or_else(|_| NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f"))
        .ok()
}

pub fn parse_time(s: &str) -> Option<NaiveTime> {
    NaiveTime::parse_from_str(s, "%H:%M:%S")
        .or_else(|_| NaiveTime::parse_from_str(s, "%H:%M:%S%.f"))
        .ok()
}

fn float_to_i64(v: f64) -> Option<i64> {
    if !v.is_finite() || v < i64::MIN as f64 || v > i64::MAX as f64 {
        return None;
    }
    Some(v.trunc() as i64)
}

fn pow10_i128(scale: u32) -> Option<i128> {
    let mut out: i128 = 1;
    for _ in 0..scale {
        out = out.checked_mul(10)?;
    }
    Some(out)
}

fn decimal128_to_i64(value: i128, scale: i8) -> Option<i64> {
    let integral = if scale >= 0 {
        let divisor = pow10_i128(scale as u32)?;
        value / divisor
    } else {
        let factor = pow10_i128((-scale) as u32)?;
        value.checked_mul(factor)?
    };
    i64::try_from(integral).ok()
}

fn parse_i64_from_utf8(s: &str) -> Option<i64> {
    let trimmed = s.trim();
    if trimmed.is_empty() {
        return None;
    }
    if let Ok(v) = trimmed.parse::<i64>() {
        return Some(v);
    }
    let float_v = trimmed.parse::<f64>().ok()?;
    float_to_i64(float_v)
}

pub fn extract_i64_array(array: &ArrayRef, func_name: &str) -> Result<Vec<Option<i64>>, String> {
    match array.data_type() {
        DataType::Int8 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int8Array>()
                .ok_or_else(|| "failed to downcast to Int8Array".to_string())?;
            Ok((0..arr.len())
                .map(|i| (!arr.is_null(i)).then(|| arr.value(i) as i64))
                .collect())
        }
        DataType::Int16 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int16Array>()
                .ok_or_else(|| "failed to downcast to Int16Array".to_string())?;
            Ok((0..arr.len())
                .map(|i| (!arr.is_null(i)).then(|| arr.value(i) as i64))
                .collect())
        }
        DataType::Int32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| "failed to downcast to Int32Array".to_string())?;
            Ok((0..arr.len())
                .map(|i| (!arr.is_null(i)).then(|| arr.value(i) as i64))
                .collect())
        }
        DataType::Int64 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| "failed to downcast to Int64Array".to_string())?;
            Ok((0..arr.len())
                .map(|i| (!arr.is_null(i)).then(|| arr.value(i)))
                .collect())
        }
        DataType::UInt8 => {
            let arr = array
                .as_any()
                .downcast_ref::<UInt8Array>()
                .ok_or_else(|| "failed to downcast to UInt8Array".to_string())?;
            Ok((0..arr.len())
                .map(|i| (!arr.is_null(i)).then(|| arr.value(i) as i64))
                .collect())
        }
        DataType::UInt16 => {
            let arr = array
                .as_any()
                .downcast_ref::<UInt16Array>()
                .ok_or_else(|| "failed to downcast to UInt16Array".to_string())?;
            Ok((0..arr.len())
                .map(|i| (!arr.is_null(i)).then(|| arr.value(i) as i64))
                .collect())
        }
        DataType::UInt32 => {
            let arr = array
                .as_any()
                .downcast_ref::<UInt32Array>()
                .ok_or_else(|| "failed to downcast to UInt32Array".to_string())?;
            Ok((0..arr.len())
                .map(|i| (!arr.is_null(i)).then(|| arr.value(i) as i64))
                .collect())
        }
        DataType::UInt64 => {
            let arr = array
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or_else(|| "failed to downcast to UInt64Array".to_string())?;
            Ok((0..arr.len())
                .map(|i| {
                    if arr.is_null(i) {
                        None
                    } else {
                        i64::try_from(arr.value(i)).ok()
                    }
                })
                .collect())
        }
        DataType::Float32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| "failed to downcast to Float32Array".to_string())?;
            Ok((0..arr.len())
                .map(|i| {
                    if arr.is_null(i) {
                        None
                    } else {
                        float_to_i64(arr.value(i) as f64)
                    }
                })
                .collect())
        }
        DataType::Float64 => {
            let arr = array
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| "failed to downcast to Float64Array".to_string())?;
            Ok((0..arr.len())
                .map(|i| {
                    if arr.is_null(i) {
                        None
                    } else {
                        float_to_i64(arr.value(i))
                    }
                })
                .collect())
        }
        DataType::Decimal128(_, scale) => {
            let arr = array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .ok_or_else(|| "failed to downcast to Decimal128Array".to_string())?;
            Ok((0..arr.len())
                .map(|i| {
                    if arr.is_null(i) {
                        None
                    } else {
                        decimal128_to_i64(arr.value(i), *scale)
                    }
                })
                .collect())
        }
        DataType::Utf8 => {
            let arr = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| "failed to downcast to StringArray".to_string())?;
            Ok((0..arr.len())
                .map(|i| {
                    if arr.is_null(i) {
                        None
                    } else {
                        parse_i64_from_utf8(arr.value(i))
                    }
                })
                .collect())
        }
        DataType::Null => Ok(vec![None; array.len()]),
        _ => Err(format!("{func_name} expects int")),
    }
}

pub fn extract_datetime_array(array: &ArrayRef) -> Result<Vec<Option<NaiveDateTime>>, String> {
    match array.data_type() {
        DataType::Date32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(|| "failed to downcast to Date32Array".to_string())?;
            let mut out = Vec::with_capacity(arr.len());
            for i in 0..arr.len() {
                if arr.is_null(i) {
                    out.push(None);
                } else {
                    let date = date32_to_naive(arr.value(i));
                    out.push(date.map(|d| d.and_hms_opt(0, 0, 0).unwrap()));
                }
            }
            Ok(out)
        }
        DataType::Timestamp(unit, _) => {
            let mut out = Vec::with_capacity(array.len());
            match unit {
                TimeUnit::Second => {
                    let arr = array
                        .as_any()
                        .downcast_ref::<TimestampSecondArray>()
                        .ok_or_else(|| "failed to downcast to TimestampSecondArray".to_string())?;
                    for i in 0..arr.len() {
                        if arr.is_null(i) {
                            out.push(None);
                        } else {
                            out.push(timestamp_to_naive(unit, arr.value(i)));
                        }
                    }
                }
                TimeUnit::Millisecond => {
                    let arr = array
                        .as_any()
                        .downcast_ref::<TimestampMillisecondArray>()
                        .ok_or_else(|| {
                            "failed to downcast to TimestampMillisecondArray".to_string()
                        })?;
                    for i in 0..arr.len() {
                        if arr.is_null(i) {
                            out.push(None);
                        } else {
                            out.push(timestamp_to_naive(unit, arr.value(i)));
                        }
                    }
                }
                TimeUnit::Microsecond => {
                    let arr = array
                        .as_any()
                        .downcast_ref::<TimestampMicrosecondArray>()
                        .ok_or_else(|| {
                            "failed to downcast to TimestampMicrosecondArray".to_string()
                        })?;
                    for i in 0..arr.len() {
                        if arr.is_null(i) {
                            out.push(None);
                        } else {
                            out.push(timestamp_to_naive(unit, arr.value(i)));
                        }
                    }
                }
                TimeUnit::Nanosecond => {
                    let arr = array
                        .as_any()
                        .downcast_ref::<TimestampNanosecondArray>()
                        .ok_or_else(|| {
                            "failed to downcast to TimestampNanosecondArray".to_string()
                        })?;
                    for i in 0..arr.len() {
                        if arr.is_null(i) {
                            out.push(None);
                        } else {
                            out.push(timestamp_to_naive(unit, arr.value(i)));
                        }
                    }
                }
            }
            Ok(out)
        }
        DataType::Utf8 => {
            let arr = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| "failed to downcast to StringArray".to_string())?;
            let mut out = Vec::with_capacity(arr.len());
            for i in 0..arr.len() {
                if arr.is_null(i) {
                    out.push(None);
                } else {
                    let s = arr.value(i);
                    let dt = parse_datetime(s)
                        .or_else(|| parse_date(s).map(|d| d.and_hms_opt(0, 0, 0).unwrap()));
                    out.push(dt);
                }
            }
            Ok(out)
        }
        other => Err(format!("unsupported datetime input type: {:?}", other)),
    }
}

pub fn extract_date_array(array: &ArrayRef) -> Result<Vec<Option<NaiveDate>>, String> {
    match array.data_type() {
        DataType::Date32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(|| "failed to downcast to Date32Array".to_string())?;
            let mut out = Vec::with_capacity(arr.len());
            for i in 0..arr.len() {
                if arr.is_null(i) {
                    out.push(None);
                } else {
                    out.push(date32_to_naive(arr.value(i)));
                }
            }
            Ok(out)
        }
        DataType::Timestamp(_, _) | DataType::Utf8 => {
            let dts = extract_datetime_array(array)?;
            Ok(dts.into_iter().map(|dt| dt.map(|d| d.date())).collect())
        }
        other => Err(format!("unsupported date input type: {:?}", other)),
    }
}

pub fn datetime_from_local_now() -> NaiveDateTime {
    chrono::Local::now().naive_local()
}

pub fn datetime_from_utc_now() -> NaiveDateTime {
    chrono::Utc::now().naive_utc()
}

pub fn time_from_local_now() -> NaiveTime {
    chrono::Local::now().naive_local().time()
}

pub fn time_from_utc_now() -> NaiveTime {
    chrono::Utc::now().naive_utc().time()
}

pub fn julian_from_date(date: NaiveDate) -> i32 {
    // Julian day number for proleptic Gregorian calendar
    let y = date.year();
    let m = date.month() as i32;
    let d = date.day() as i32;
    let a = (14 - m) / 12;
    let y = y + 4800 - a;
    let m = m + 12 * a - 3;
    d + ((153 * m + 2) / 5) + 365 * y + y / 4 - y / 100 + y / 400 - 32045
}

pub fn date_from_julian(julian: i32) -> Option<NaiveDate> {
    // Inverse of julian day number to date
    let a = julian + 32044;
    let b = (4 * a + 3) / 146097;
    let c = a - (146097 * b) / 4;
    let d = (4 * c + 3) / 1461;
    let e = c - (1461 * d) / 4;
    let m = (5 * e + 2) / 153;
    let day = e - (153 * m + 2) / 5 + 1;
    let month = m + 3 - 12 * (m / 10);
    let year = 100 * b + d - 4800 + (m / 10);
    NaiveDate::from_ymd_opt(year, month as u32, day as u32)
}

pub fn time_to_seconds(time: NaiveTime) -> i64 {
    (time.hour() as i64) * 3600 + (time.minute() as i64) * 60 + (time.second() as i64)
}

pub fn seconds_to_time(seconds: i64) -> NaiveTime {
    let mut secs = seconds % 86400;
    if secs < 0 {
        secs += 86400;
    }
    let h = (secs / 3600) as u32;
    let m = ((secs % 3600) / 60) as u32;
    let s = (secs % 60) as u32;
    NaiveTime::from_hms_opt(h, m, s).unwrap_or_else(|| NaiveTime::from_hms_opt(0, 0, 0).unwrap())
}

const DATE_UNIX_EPOCH_JULIAN: i64 = 2_440_588;
const USECS_PER_DAY_I64: i64 = 86_400_000_000;
const TIMESTAMP_BITS: u32 = 40;
const TIMESTAMP_TIME_MASK: u64 = (1_u64 << TIMESTAMP_BITS) - 1;

fn is_starrocks_datetime_encodable(unix_micros: i64) -> bool {
    let days_since_epoch = unix_micros.div_euclid(USECS_PER_DAY_I64);
    let micros_of_day = unix_micros.rem_euclid(USECS_PER_DAY_I64);
    if !(0..USECS_PER_DAY_I64).contains(&micros_of_day) {
        return false;
    }

    let Some(julian_day) = DATE_UNIX_EPOCH_JULIAN.checked_add(days_since_epoch) else {
        return false;
    };
    if julian_day < 0 {
        return false;
    }

    let Ok(julian_u64) = u64::try_from(julian_day) else {
        return false;
    };
    let Ok(micros_u64) = u64::try_from(micros_of_day) else {
        return false;
    };
    let encoded = (julian_u64 << TIMESTAMP_BITS) | (micros_u64 & TIMESTAMP_TIME_MASK);
    i64::try_from(encoded).is_ok()
}

pub fn to_timestamp_value(dt: NaiveDateTime, output_type: &DataType) -> Result<i64, String> {
    let year = dt.date().year();
    if !(0..=9999).contains(&year) {
        return Err("timestamp out of StarRocks DATETIME year range".to_string());
    }

    let micros = dt.and_utc().timestamp_micros();
    if !is_starrocks_datetime_encodable(micros) {
        return Err("timestamp out of StarRocks DATETIME encoding range".to_string());
    }

    match output_type {
        DataType::Timestamp(TimeUnit::Microsecond, _) => Ok(micros),
        DataType::Timestamp(TimeUnit::Millisecond, _) => Ok(dt.and_utc().timestamp_millis()),
        DataType::Timestamp(TimeUnit::Second, _) => Ok(dt.and_utc().timestamp()),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => dt
            .and_utc()
            .timestamp_nanos_opt()
            .ok_or_else(|| "timestamp out of range".to_string()),
        _ => Err("expected timestamp output type".to_string()),
    }
}

pub fn format_datetime_with_pattern(dt: NaiveDateTime, pattern: &str) -> String {
    dt.format(pattern).to_string()
}

pub fn parse_datetime_with_pattern(s: &str, pattern: &str) -> Option<NaiveDateTime> {
    NaiveDateTime::parse_from_str(s, pattern).ok()
}

pub fn mysql_format_to_chrono(fmt: &str) -> String {
    // Basic mapping for common MySQL format tokens
    let mut out = String::new();
    let mut chars = fmt.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '%' {
            if let Some(n) = chars.next() {
                match n {
                    'Y' => out.push_str("%Y"),
                    'y' => out.push_str("%y"),
                    'm' => out.push_str("%m"),
                    'c' => out.push_str("%m"),
                    'd' => out.push_str("%d"),
                    'e' => out.push_str("%d"),
                    'H' => out.push_str("%H"),
                    'h' | 'I' => out.push_str("%I"),
                    'i' => out.push_str("%M"),
                    's' | 'S' => out.push_str("%S"),
                    'f' => out.push_str("%f"),
                    'T' => out.push_str("%H:%M:%S"),
                    _ => {
                        out.push('%');
                        out.push(n);
                    }
                }
            }
        } else {
            out.push(c);
        }
    }
    out
}

pub fn convert_tz_fixed(
    dt: NaiveDateTime,
    from: chrono::FixedOffset,
    to: chrono::FixedOffset,
) -> NaiveDateTime {
    let dt_from = from.from_local_datetime(&dt).unwrap();
    let utc = dt_from.with_timezone(&Utc);
    utc.with_timezone(&to).naive_local()
}
