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
use super::common::{NumericArrayView, value_at_i64};
use crate::exec::chunk::Chunk;
use crate::exec::expr::{ExprArena, ExprId};
use arrow::array::{Array, ArrayRef, StringArray};
use std::sync::Arc;

pub fn eval_conv(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let _ = expr;
    let num_arr = arena.eval(args[0], chunk)?;
    let from_arr = arena.eval(args[1], chunk)?;
    let to_arr = arena.eval(args[2], chunk)?;
    let num_len = num_arr.len();

    let from_view = NumericArrayView::new(&from_arr)?;
    let to_view = NumericArrayView::new(&to_arr)?;

    let len = num_len;
    let mut values = Vec::with_capacity(len);
    for row in 0..len {
        let num_str = if num_arr.is_null(row) {
            None
        } else if let Some(s) = num_arr.as_any().downcast_ref::<StringArray>() {
            Some(s.value(row).to_string())
        } else {
            let view = NumericArrayView::new(&num_arr)?;
            value_at_i64(&view, row, len).map(|v| v.to_string())
        };
        let from_base = value_at_i64(&from_view, row, len);
        let to_base = value_at_i64(&to_view, row, len);
        let out = match (num_str, from_base, to_base) {
            (Some(s), Some(fb), Some(tb)) => convert_base(&s, fb as i32, tb as i32),
            _ => None,
        };
        values.push(out);
    }
    Ok(Arc::new(StringArray::from(values)) as ArrayRef)
}

fn convert_base(s: &str, from_base: i32, to_base: i32) -> Option<String> {
    let from_base_abs = from_base.unsigned_abs();
    let to_base_abs = to_base.unsigned_abs();
    if !(2..=36).contains(&from_base_abs) || !(2..=36).contains(&to_base_abs) {
        return None;
    }
    let negative = s.starts_with('-');
    let num_str = s.trim_start_matches('-');
    let value = i64::from_str_radix(num_str, from_base_abs).ok()?;
    let mut v = if negative { -value } else { value };
    let mut out = if to_base_abs == 10 {
        v.to_string()
    } else {
        let mut res = String::new();
        let base = i64::from(to_base_abs);
        if v == 0 {
            res.push('0');
        } else {
            let sign = v < 0;
            if sign {
                v = -v;
            }
            while v > 0 {
                let digit = (v % base) as u8;
                res.push("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ".as_bytes()[digit as usize] as char);
                v /= base;
            }
            if sign {
                res.push('-');
            }
            res = res.chars().rev().collect();
        }
        res
    };
    if to_base < 0 {
        out = out.to_string();
    }
    Some(out)
}
