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
use super::common::naive_to_date32;
use crate::exec::chunk::Chunk;
use crate::exec::expr::{ExprArena, ExprId};
use arrow::array::{Array, ArrayRef, Date32Array, Int64Array};
use arrow::compute::cast;
use arrow::datatypes::DataType;
use chrono::{Days, NaiveDate};
use std::sync::Arc;

pub fn eval_makedate(
    arena: &ExprArena,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let year_arr = arena.eval(args[0], chunk)?;
    let day_arr = arena.eval(args[1], chunk)?;
    let year_arr = cast(&year_arr, &DataType::Int64).map_err(|e| e.to_string())?;
    let day_arr = cast(&day_arr, &DataType::Int64).map_err(|e| e.to_string())?;
    let year_arr = year_arr
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| "makedate expects int".to_string())?;
    let day_arr = day_arr
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| "makedate expects int".to_string())?;
    let mut out = Vec::with_capacity(year_arr.len());
    for i in 0..year_arr.len() {
        if year_arr.is_null(i) || day_arr.is_null(i) {
            out.push(None);
            continue;
        }
        let year = i32::try_from(year_arr.value(i)).ok();
        let day = i32::try_from(day_arr.value(i)).ok();
        let date = match (year, day) {
            (Some(year), Some(day)) if day > 0 && (0..=9999).contains(&year) => {
                let base = NaiveDate::from_ymd_opt(year, 1, 1);
                let leap = NaiveDate::from_ymd_opt(year, 2, 29).is_some();
                let max_day = if leap { 366 } else { 365 };
                if day > max_day {
                    None
                } else {
                    base.and_then(|d| d.checked_add_days(Days::new((day - 1) as u64)))
                }
            }
            _ => None,
        };
        out.push(date.map(naive_to_date32));
    }
    Ok(Arc::new(Date32Array::from(out)) as ArrayRef)
}