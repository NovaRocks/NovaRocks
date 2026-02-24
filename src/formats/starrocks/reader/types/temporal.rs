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
//! Temporal conversion helpers for StarRocks native storage values.
//!
//! Conversion rules follow StarRocks native page storage:
//! - DATE is stored as Julian day number.
//! - DATETIME is stored as packed `(julian_day << 40) | micros_of_day`.
//!
//! Current limitations:
//! - Only DATE/DATETIME conversions required by scalar reader are implemented.
//! - Timezone-aware or string temporal encodings are not handled here.

use super::super::constants::{
    DATE_UNIX_EPOCH_JULIAN, TIMESTAMP_BITS, TIMESTAMP_TIME_MASK, USECS_PER_DAY_I64,
};

pub(crate) fn convert_date32_julian_to_unix_days(
    julian: i32,
    segment_path: &str,
    output_name: &str,
) -> Result<i32, String> {
    let unix_days = i64::from(julian) - i64::from(DATE_UNIX_EPOCH_JULIAN);
    i32::try_from(unix_days).map_err(|_| {
        format!(
            "Date32 conversion overflow for rust native reader: output_column={}, segment={}, julian_day={}, unix_days={}",
            output_name, segment_path, julian, unix_days
        )
    })
}

pub(crate) fn convert_datetime_storage_to_unix_micros(
    encoded: i64,
    segment_path: &str,
    output_name: &str,
) -> Result<i64, String> {
    if encoded < 0 {
        return Err(format!(
            "invalid DATETIME internal value for rust native reader: output_column={}, segment={}, value={}",
            output_name, segment_path, encoded
        ));
    }
    let value = encoded as u64;
    let julian_day = (value >> TIMESTAMP_BITS) as i64;
    let micros_of_day = (value & TIMESTAMP_TIME_MASK) as i64;
    if micros_of_day >= USECS_PER_DAY_I64 {
        return Err(format!(
            "invalid DATETIME microseconds-of-day for rust native reader: output_column={}, segment={}, value={}, microseconds_of_day={}, max_valid={}",
            output_name,
            segment_path,
            encoded,
            micros_of_day,
            USECS_PER_DAY_I64 - 1
        ));
    }
    let days_since_epoch = julian_day - i64::from(DATE_UNIX_EPOCH_JULIAN);
    days_since_epoch
        .checked_mul(USECS_PER_DAY_I64)
        .and_then(|base| base.checked_add(micros_of_day))
        .ok_or_else(|| {
            format!(
                "DATETIME conversion overflow for rust native reader: output_column={}, segment={}, value={}, julian_day={}, microseconds_of_day={}",
                output_name, segment_path, encoded, julian_day, micros_of_day
            )
        })
}
