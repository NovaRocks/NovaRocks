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
use chrono::{Datelike, Duration};
use std::sync::Arc;

#[inline]
fn parse_weekday_token(target: &str, func_name: &str) -> Result<chrono::Weekday, String> {
    match target {
        "Mo" | "Mon" | "Monday" => Ok(chrono::Weekday::Mon),
        "Tu" | "Tue" | "Tuesday" => Ok(chrono::Weekday::Tue),
        "We" | "Wed" | "Wednesday" => Ok(chrono::Weekday::Wed),
        "Th" | "Thu" | "Thursday" => Ok(chrono::Weekday::Thu),
        "Fr" | "Fri" | "Friday" => Ok(chrono::Weekday::Fri),
        "Sa" | "Sat" | "Saturday" => Ok(chrono::Weekday::Sat),
        "Su" | "Sun" | "Sunday" => Ok(chrono::Weekday::Sun),
        _ => Err(format!(
            "{} not supported in {} dow_string backend",
            target, func_name
        )),
    }
}

#[inline]
fn eval_next_previous_day_inner(
    arena: &ExprArena,
    args: &[ExprId],
    chunk: &Chunk,
    next: bool,
) -> Result<ArrayRef, String> {
    let date_arr = arena.eval(args[0], chunk)?;
    let day_arr = arena.eval(args[1], chunk)?;
    let dates = extract_date_array(&date_arr)?;
    let day_arr = day_arr
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| "next_day expects string".to_string())?;
    let mut out = Vec::with_capacity(dates.len());
    let func_name = if next { "next_day" } else { "previous_day" };
    for i in 0..dates.len() {
        if date_arr.is_null(i) || day_arr.is_null(i) {
            out.push(None);
            continue;
        }
        let target = day_arr.value(i);
        let target_weekday = parse_weekday_token(target, func_name)?;
        let date = match dates[i] {
            Some(d) => d,
            None => {
                out.push(None);
                continue;
            }
        };
        let current = date.weekday().num_days_from_monday() as i64;
        let target = target_weekday.num_days_from_monday() as i64;
        let delta_days = if next {
            let mut d = (target - current + 7) % 7;
            if d == 0 {
                d = 7;
            }
            d
        } else {
            let mut d = (current - target + 7) % 7;
            if d == 0 {
                d = 7;
            }
            -d
        };
        let shifted = date.checked_add_signed(Duration::days(delta_days));
        let shifted = shifted.filter(|d| (0..=9999).contains(&d.year()));
        out.push(shifted.map(naive_to_date32));
    }
    Ok(Arc::new(Date32Array::from(out)) as ArrayRef)
}

pub fn eval_next_day(
    arena: &ExprArena,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_next_previous_day_inner(arena, args, chunk, true)
}

pub fn eval_previous_day(
    arena: &ExprArena,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_next_previous_day_inner(arena, args, chunk, false)
}

#[cfg(test)]
mod tests {
    use crate::exec::expr::function::date::test_utils::assert_date_function_logic;

    #[test]
    fn test_next_day_logic() {
        assert_date_function_logic("next_day");
    }

    #[test]
    fn test_previous_day_logic() {
        assert_date_function_logic("previous_day");
    }
}
