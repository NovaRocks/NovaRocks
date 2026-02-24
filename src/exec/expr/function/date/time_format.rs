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
use super::common::{extract_datetime_array, parse_time, time_to_seconds};
use crate::exec::chunk::Chunk;
use crate::exec::expr::{ExprArena, ExprId, ExprNode};
use arrow::array::{Array, ArrayRef, StringArray};
use arrow::datatypes::DataType;
use std::sync::Arc;

fn format_time_pattern(fmt: &str, seconds_of_day: i64) -> String {
    let mut out = String::new();
    let mut chars = fmt.chars().peekable();
    let clamped_seconds = seconds_of_day.clamp(0, 86_399);
    while let Some(ch) = chars.next() {
        if ch != '%' {
            out.push(ch);
            continue;
        }
        let Some(token) = chars.next() else {
            out.push('%');
            break;
        };
        match token {
            '%' => out.push('%'),
            'H' | 'i' | 's' | 'S' => out.push_str("00"),
            'h' => out.push_str("12"),
            'f' => out.push_str(&format!("{clamped_seconds:06}")),
            _ => {
                out.push('%');
                out.push(token);
            }
        }
    }
    out
}

pub fn eval_time_format(
    arena: &ExprArena,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let time_seconds_override = if let Some(ExprNode::Cast(child)) = arena.node(args[0]) {
        if matches!(arena.data_type(*child), Some(DataType::Utf8)) {
            let raw_arr = arena.eval(*child, chunk)?;
            let raw_utf8 = raw_arr
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| "time_format expects string".to_string())?;
            let mut out = Vec::with_capacity(raw_utf8.len());
            for i in 0..raw_utf8.len() {
                if raw_utf8.is_null(i) {
                    out.push(None);
                } else {
                    out.push(parse_time(raw_utf8.value(i)).map(time_to_seconds));
                }
            }
            Some(out)
        } else {
            None
        }
    } else {
        None
    };

    let time_arr = arena.eval(args[0], chunk)?;
    let fmt_arr = arena.eval(args[1], chunk)?;
    let fmt_arr = fmt_arr
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| "time_format expects string format".to_string())?;

    let time_seconds: Vec<Option<i64>> = if let Some(override_v) = time_seconds_override {
        override_v
    } else {
        match time_arr.data_type() {
            DataType::Date32 | DataType::Timestamp(_, _) => extract_datetime_array(&time_arr)?
                .into_iter()
                .map(|dt| dt.map(|v| time_to_seconds(v.time())))
                .collect(),
            DataType::Utf8 => {
                let arr = time_arr
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| "time_format expects string".to_string())?;
                let mut out = Vec::with_capacity(arr.len());
                for i in 0..arr.len() {
                    if arr.is_null(i) {
                        out.push(None);
                    } else {
                        out.push(parse_time(arr.value(i)).map(time_to_seconds));
                    }
                }
                out
            }
            DataType::Null => vec![None; time_arr.len()],
            _ => return Err("time_format expects time".to_string()),
        }
    };

    let len = chunk.len();
    if time_seconds.len() != len && time_seconds.len() != 1 {
        return Err("time_format argument length mismatch".to_string());
    }
    if fmt_arr.len() != len && fmt_arr.len() != 1 {
        return Err("time_format format length mismatch".to_string());
    }
    let mut out = Vec::with_capacity(len);
    for i in 0..len {
        let time_v = if time_seconds.len() == 1 {
            time_seconds[0]
        } else {
            time_seconds[i]
        };
        if fmt_arr.is_null(if fmt_arr.len() == 1 { 0 } else { i }) {
            out.push(None);
            continue;
        }
        let Some(seconds_of_day) = time_v else {
            out.push(None);
            continue;
        };
        let fmt = fmt_arr.value(if fmt_arr.len() == 1 { 0 } else { i });
        out.push(Some(format_time_pattern(fmt, seconds_of_day)));
    }
    Ok(Arc::new(StringArray::from(out)) as ArrayRef)
}

#[cfg(test)]
mod tests {
    use crate::exec::expr::function::date::test_utils::assert_date_function_logic;

    #[test]
    fn test_time_format_logic() {
        assert_date_function_logic("time_format");
    }
}
