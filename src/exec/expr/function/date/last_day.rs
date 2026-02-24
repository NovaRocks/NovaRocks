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
use super::common::{extract_date_array, naive_to_date32};
use crate::exec::chunk::Chunk;
use crate::exec::expr::{ExprArena, ExprId};
use arrow::array::{Array, ArrayRef, Date32Array, StringArray};
use chrono::{Datelike, Duration, NaiveDate};
use std::sync::Arc;

#[derive(Clone, Copy)]
enum LastDayUnit {
    Month,
    Quarter,
    Year,
}

#[inline]
fn parse_last_day_unit(unit: &str) -> Result<LastDayUnit, String> {
    match unit.to_ascii_lowercase().as_str() {
        "month" => Ok(LastDayUnit::Month),
        "quarter" => Ok(LastDayUnit::Quarter),
        "year" => Ok(LastDayUnit::Year),
        _ => Err("avaiable data_part parameter is year/month/quarter".to_string()),
    }
}

#[inline]
fn end_of_month(year: i32, month: u32) -> Option<NaiveDate> {
    let (next_year, next_month) = if month == 12 {
        (year + 1, 1)
    } else {
        (year, month + 1)
    };
    let first_next = NaiveDate::from_ymd_opt(next_year, next_month, 1)?;
    first_next.checked_sub_signed(Duration::days(1))
}

#[inline]
fn eval_last_day_value(date: NaiveDate, unit: LastDayUnit) -> Option<i32> {
    let out = match unit {
        LastDayUnit::Month => end_of_month(date.year(), date.month())?,
        LastDayUnit::Quarter => {
            let month = ((date.month() - 1) / 3 + 1) * 3;
            end_of_month(date.year(), month)?
        }
        LastDayUnit::Year => NaiveDate::from_ymd_opt(date.year(), 12, 31)?,
    };
    Some(naive_to_date32(out))
}

pub fn eval_last_day(
    arena: &ExprArena,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let arr = arena.eval(args[0], chunk)?;
    let dates = extract_date_array(&arr)?;
    let mut out = Vec::with_capacity(dates.len());
    if args.len() == 1 {
        for date in dates {
            out.push(date.and_then(|d| eval_last_day_value(d, LastDayUnit::Month)));
        }
        return Ok(Arc::new(Date32Array::from(out)) as ArrayRef);
    }

    let unit_arr = arena.eval(args[1], chunk)?;
    let unit_arr = unit_arr
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| "last_day expects string".to_string())?;

    for i in 0..dates.len() {
        if unit_arr.is_null(i) {
            out.push(None);
            continue;
        }
        let Some(date) = dates[i] else {
            out.push(None);
            continue;
        };
        let unit = parse_last_day_unit(unit_arr.value(i))?;
        out.push(eval_last_day_value(date, unit));
    }
    Ok(Arc::new(Date32Array::from(out)) as ArrayRef)
}
#[cfg(test)]
mod tests {
    use crate::exec::expr::function::date::test_utils::assert_date_function_logic;

    #[test]
    fn test_last_day_logic() {
        assert_date_function_logic("last_day");
    }
}
