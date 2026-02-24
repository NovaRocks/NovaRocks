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
use super::common::{BC_EPOCH_JULIAN, date_from_julian, naive_to_date32};
use crate::exec::chunk::Chunk;
use crate::exec::expr::{ExprArena, ExprId};
use arrow::array::{Array, ArrayRef, Date32Array, Int32Array, Int64Array};
use std::sync::Arc;

const FROM_DAYS_MAX_VALID: i64 = 3_652_424;
const ZERO_DATE_TO_DAYS: i64 = -32;

pub fn eval_from_days(
    arena: &ExprArena,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let arr = arena.eval(args[0], chunk)?;
    let mut out = Vec::with_capacity(arr.len());
    if let Some(arr) = arr.as_any().downcast_ref::<Int32Array>() {
        for i in 0..arr.len() {
            if arr.is_null(i) {
                out.push(None);
            } else {
                out.push(from_days_value(arr.value(i) as i64));
            }
        }
    } else if let Some(arr) = arr.as_any().downcast_ref::<Int64Array>() {
        for i in 0..arr.len() {
            if arr.is_null(i) {
                out.push(None);
            } else {
                out.push(from_days_value(arr.value(i)));
            }
        }
    } else {
        return Err("from_days expects int".to_string());
    }
    Ok(Arc::new(Date32Array::from(out)) as ArrayRef)
}

fn zero_date_sentinel_date32() -> i32 {
    let julian = BC_EPOCH_JULIAN + ZERO_DATE_TO_DAYS as i32;
    match date_from_julian(julian) {
        Some(date) => naive_to_date32(date),
        None => 0,
    }
}

fn from_days_value(days: i64) -> Option<i32> {
    if days < i32::MIN as i64 || days > i32::MAX as i64 {
        return None;
    }

    if (0..=FROM_DAYS_MAX_VALID).contains(&days) {
        let julian = BC_EPOCH_JULIAN as i64 + days;
        let julian = i32::try_from(julian).ok()?;
        return date_from_julian(julian).map(naive_to_date32);
    }

    Some(zero_date_sentinel_date32())
}
#[cfg(test)]
mod tests {
    use super::{FROM_DAYS_MAX_VALID, from_days_value, zero_date_sentinel_date32};
    use crate::exec::expr::function::date::test_utils::assert_date_function_logic;

    #[test]
    fn test_from_days_logic() {
        assert_date_function_logic("from_days");
    }

    #[test]
    fn test_from_days_out_of_calendar_range_returns_zero_date_sentinel() {
        let sentinel = zero_date_sentinel_date32();
        assert_eq!(from_days_value(-1), Some(sentinel));
        assert_eq!(from_days_value(FROM_DAYS_MAX_VALID + 1), Some(sentinel));
        assert_eq!(from_days_value(i32::MAX as i64), Some(sentinel));
        assert_eq!(from_days_value(i32::MIN as i64), Some(sentinel));
    }

    #[test]
    fn test_from_days_out_of_i32_range_returns_null() {
        assert_eq!(from_days_value(i32::MAX as i64 + 1), None);
        assert_eq!(from_days_value(i32::MIN as i64 - 1), None);
    }
}
