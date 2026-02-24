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
use crate::exec::chunk::Chunk;
use crate::exec::expr::function::date::common::{extract_datetime_array, naive_to_date32};
use crate::exec::expr::{ExprArena, ExprId};
use arrow::array::{
    Array, ArrayRef, Date32Array, Int64Array, ListArray, StringArray, TimestampMicrosecondArray,
};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field};
use arrow_buffer::{NullBufferBuilder, OffsetBuffer};
use chrono::{Datelike, Duration, NaiveDate, NaiveDateTime};
use std::sync::Arc;

fn cast_to_i64(array: ArrayRef, arg_name: &str) -> Result<ArrayRef, String> {
    if array.data_type() == &DataType::Int64 {
        return Ok(array);
    }
    cast(&array, &DataType::Int64).map_err(|e| {
        format!(
            "array_generate failed to cast {} to BIGINT: {}",
            arg_name, e
        )
    })
}

fn is_datetime_like(ty: &DataType) -> bool {
    matches!(
        ty,
        DataType::Date32 | DataType::Timestamp(_, _) | DataType::Utf8
    )
}

fn last_day_of_month(year: i32, month: u32) -> u32 {
    let (next_year, next_month) = if month == 12 {
        (year + 1, 1)
    } else {
        (year, month + 1)
    };
    let first_next = NaiveDate::from_ymd_opt(next_year, next_month, 1).unwrap();
    (first_next - Duration::days(1)).day()
}

fn add_months_to_datetime(dt: NaiveDateTime, months: i32) -> Option<NaiveDateTime> {
    let date = dt.date();
    let mut year = date.year();
    let mut month = date.month() as i32 - 1 + months;
    year += month.div_euclid(12);
    month = month.rem_euclid(12) + 1;
    let day = date.day().min(last_day_of_month(year, month as u32));
    NaiveDate::from_ymd_opt(year, month as u32, day).map(|d| d.and_time(dt.time()))
}

fn add_datetime_unit(dt: NaiveDateTime, step: i64, unit: &str) -> Option<NaiveDateTime> {
    match unit {
        "year" => add_months_to_datetime(dt, i32::try_from(step).ok()?.checked_mul(12)?),
        "quarter" => add_months_to_datetime(dt, i32::try_from(step).ok()?.checked_mul(3)?),
        "month" => add_months_to_datetime(dt, i32::try_from(step).ok()?),
        "week" => dt.checked_add_signed(Duration::weeks(step)),
        "day" => dt.checked_add_signed(Duration::days(step)),
        "hour" => dt.checked_add_signed(Duration::hours(step)),
        "minute" => dt.checked_add_signed(Duration::minutes(step)),
        "second" => dt.checked_add_signed(Duration::seconds(step)),
        "millisecond" => dt.checked_add_signed(Duration::milliseconds(step)),
        "microsecond" => dt.checked_add_signed(Duration::microseconds(step)),
        _ => None,
    }
}

fn extract_datetime_array_lenient(array: &ArrayRef) -> Result<Vec<Option<NaiveDateTime>>, String> {
    if is_datetime_like(array.data_type()) {
        return extract_datetime_array(array);
    }
    let as_utf8 = cast(array.as_ref(), &DataType::Utf8).map_err(|e| {
        format!(
            "array_generate failed to cast datetime input to VARCHAR: {}",
            e
        )
    })?;
    extract_datetime_array(&as_utf8)
}

fn build_datetime_values_array(
    values: Vec<NaiveDateTime>,
    target_type: &DataType,
) -> Result<ArrayRef, String> {
    match target_type {
        DataType::Date32 => {
            let out = values
                .into_iter()
                .map(|dt| naive_to_date32(dt.date()))
                .collect::<Vec<_>>();
            Ok(Arc::new(Date32Array::from(out)) as ArrayRef)
        }
        DataType::Timestamp(_, _) => {
            let micros = values
                .into_iter()
                .map(|dt| dt.and_utc().timestamp_micros())
                .collect::<Vec<_>>();
            let arr = Arc::new(TimestampMicrosecondArray::from(micros)) as ArrayRef;
            if arr.data_type() == target_type {
                Ok(arr)
            } else {
                cast(&arr, target_type).map_err(|e| {
                    format!(
                        "array_generate failed to cast datetime output {:?} -> {:?}: {}",
                        arr.data_type(),
                        target_type,
                        e
                    )
                })
            }
        }
        other => Err(format!(
            "array_generate date mode unsupported output element type: {:?}",
            other
        )),
    }
}

fn eval_array_generate_numeric(
    arena: &ExprArena,
    output_field: Arc<Field>,
    target_item_type: DataType,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let stop_arr = if args.len() == 1 {
        cast_to_i64(arena.eval(args[0], chunk)?, "stop")?
    } else {
        cast_to_i64(arena.eval(args[1], chunk)?, "stop")?
    };
    let stop = stop_arr
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| "array_generate internal stop downcast failure".to_string())?;

    let start_arr = if args.len() == 1 {
        None
    } else {
        Some(cast_to_i64(arena.eval(args[0], chunk)?, "start")?)
    };
    let start = start_arr
        .as_ref()
        .map(|arr| {
            arr.as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| "array_generate internal start downcast failure".to_string())
        })
        .transpose()?;

    let step_arr = if args.len() == 3 {
        Some(cast_to_i64(arena.eval(args[2], chunk)?, "step")?)
    } else {
        None
    };
    let step = step_arr
        .as_ref()
        .map(|arr| {
            arr.as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| "array_generate internal step downcast failure".to_string())
        })
        .transpose()?;

    let mut values = Vec::<i64>::new();
    let mut offsets = Vec::with_capacity(chunk.len() + 1);
    offsets.push(0_i32);
    let mut current_offset: i64 = 0;
    let mut null_builder = NullBufferBuilder::new(chunk.len());

    for row in 0..chunk.len() {
        let stop_row = super::common::row_index(row, stop.len());
        if stop.is_null(stop_row) {
            null_builder.append_null();
            offsets.push(current_offset as i32);
            continue;
        }
        let stop_v = stop.value(stop_row);

        let start_v = if let Some(start_arr) = start {
            let start_row = super::common::row_index(row, start_arr.len());
            if start_arr.is_null(start_row) {
                null_builder.append_null();
                offsets.push(current_offset as i32);
                continue;
            }
            start_arr.value(start_row)
        } else {
            1_i64
        };

        let step_v = if let Some(step_arr) = step {
            let step_row = super::common::row_index(row, step_arr.len());
            if step_arr.is_null(step_row) {
                null_builder.append_null();
                offsets.push(current_offset as i32);
                continue;
            }
            step_arr.value(step_row)
        } else if start_v <= stop_v {
            1_i64
        } else {
            -1_i64
        };

        if step_v == 0 {
            null_builder.append_non_null();
            offsets.push(current_offset as i32);
            continue;
        }

        if step_v > 0 && start_v <= stop_v {
            let mut cur = start_v;
            while cur <= stop_v {
                values.push(cur);
                current_offset += 1;
                if current_offset > i32::MAX as i64 {
                    return Err("array_generate offset overflow".to_string());
                }
                let Some(next) = cur.checked_add(step_v) else {
                    break;
                };
                cur = next;
            }
        } else if step_v < 0 && start_v >= stop_v {
            let mut cur = start_v;
            while cur >= stop_v {
                values.push(cur);
                current_offset += 1;
                if current_offset > i32::MAX as i64 {
                    return Err("array_generate offset overflow".to_string());
                }
                let Some(next) = cur.checked_add(step_v) else {
                    break;
                };
                cur = next;
            }
        }

        null_builder.append_non_null();
        offsets.push(current_offset as i32);
    }

    let mut values_array = Arc::new(Int64Array::from(values)) as ArrayRef;
    if values_array.data_type() != &target_item_type {
        values_array = cast(&values_array, &target_item_type).map_err(|e| {
            format!(
                "array_generate failed to cast output values to {:?}: {}",
                target_item_type, e
            )
        })?;
    }

    let out = ListArray::new(
        output_field,
        OffsetBuffer::new(offsets.into()),
        values_array,
        null_builder.finish(),
    );
    Ok(Arc::new(out) as ArrayRef)
}

fn eval_array_generate_datetime(
    arena: &ExprArena,
    output_field: Arc<Field>,
    target_item_type: DataType,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let start_arr = arena.eval(args[0], chunk)?;
    let stop_arr = arena.eval(args[1], chunk)?;
    let start_values = extract_datetime_array_lenient(&start_arr)?;
    let stop_values = extract_datetime_array_lenient(&stop_arr)?;

    let step_arr = if args.len() >= 3 {
        Some(cast_to_i64(arena.eval(args[2], chunk)?, "step")?)
    } else {
        None
    };
    let step_values = step_arr
        .as_ref()
        .map(|arr| {
            arr.as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| "array_generate internal step downcast failure".to_string())
        })
        .transpose()?;

    let unit_arr = if args.len() == 4 {
        let unit = arena.eval(args[3], chunk)?;
        if unit.data_type() == &DataType::Utf8 {
            unit
        } else {
            cast(&unit, &DataType::Utf8).map_err(|e| {
                format!(
                    "array_generate failed to cast unit {:?} -> VARCHAR: {}",
                    unit.data_type(),
                    e
                )
            })?
        }
    } else {
        Arc::new(StringArray::from(vec!["day"])) as ArrayRef
    };
    let units = unit_arr
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| "array_generate internal unit downcast failure".to_string())?;

    let mut values = Vec::<NaiveDateTime>::new();
    let mut offsets = Vec::with_capacity(chunk.len() + 1);
    offsets.push(0_i32);
    let mut current_offset: i64 = 0;
    let mut null_builder = NullBufferBuilder::new(chunk.len());

    for row in 0..chunk.len() {
        let start_row = super::common::row_index(row, start_values.len());
        let stop_row = super::common::row_index(row, stop_values.len());
        let Some(start) = start_values[start_row] else {
            null_builder.append_null();
            offsets.push(current_offset as i32);
            continue;
        };
        let Some(stop) = stop_values[stop_row] else {
            null_builder.append_null();
            offsets.push(current_offset as i32);
            continue;
        };

        let step_v = if let Some(steps) = step_values {
            let step_row = super::common::row_index(row, steps.len());
            if steps.is_null(step_row) {
                null_builder.append_null();
                offsets.push(current_offset as i32);
                continue;
            }
            steps.value(step_row)
        } else {
            1_i64
        };
        if step_v < 0 {
            return Err("array_generate requires step parameter must be non-negative.".to_string());
        }

        let unit_row = super::common::row_index(row, units.len());
        if units.is_null(unit_row) {
            null_builder.append_null();
            offsets.push(current_offset as i32);
            continue;
        }
        let unit = units.value(unit_row).trim().to_ascii_lowercase();
        if step_v == 0 {
            null_builder.append_non_null();
            offsets.push(current_offset as i32);
            continue;
        }

        let forward = start <= stop;
        let actual_step = if forward { step_v } else { -step_v };
        let mut current = start;
        loop {
            if forward {
                if current > stop {
                    break;
                }
            } else if current < stop {
                break;
            }
            values.push(current);
            current_offset += 1;
            if current_offset > i32::MAX as i64 {
                return Err("array_generate offset overflow".to_string());
            }
            let Some(next) = add_datetime_unit(current, actual_step, unit.as_str()) else {
                return Err(format!("array_generate unsupported time unit: {}", unit));
            };
            if next == current {
                break;
            }
            current = next;
        }

        null_builder.append_non_null();
        offsets.push(current_offset as i32);
    }

    let values_array = build_datetime_values_array(values, &target_item_type)?;
    let out = ListArray::new(
        output_field,
        OffsetBuffer::new(offsets.into()),
        values_array,
        null_builder.finish(),
    );
    Ok(Arc::new(out) as ArrayRef)
}

pub fn eval_array_generate(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    if args.is_empty() || args.len() > 4 {
        return Err("array_generate expects 1 to 4 arguments".to_string());
    }

    let output_field = match arena.data_type(expr) {
        Some(DataType::List(field)) => field.clone(),
        _ => Arc::new(Field::new("item", DataType::Int64, true)),
    };
    let target_item_type = output_field.data_type().clone();
    let is_date_mode = if args.len() == 4 {
        true
    } else if args.len() >= 2 {
        let arg0_type = arena
            .data_type(args[0])
            .ok_or_else(|| "array_generate missing arg0 type".to_string())?;
        let arg1_type = arena
            .data_type(args[1])
            .ok_or_else(|| "array_generate missing arg1 type".to_string())?;
        is_datetime_like(arg0_type) || is_datetime_like(arg1_type)
    } else {
        false
    };

    if is_date_mode {
        eval_array_generate_datetime(arena, output_field, target_item_type, args, chunk)
    } else {
        eval_array_generate_numeric(arena, output_field, target_item_type, args, chunk)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exec::expr::function::array::eval_array_function;
    use crate::exec::expr::function::array::test_utils::{chunk_len_1, literal_i64, typed_null};
    use arrow::array::ListArray;
    use arrow::datatypes::{DataType, Field, TimeUnit};

    #[test]
    fn test_array_generate_one_arg() {
        let mut arena = ExprArena::default();
        let chunk = chunk_len_1();
        let list_type = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));
        let expr = typed_null(&mut arena, list_type);
        let stop = literal_i64(&mut arena, 3);

        let out = eval_array_function("array_generate", &arena, expr, &[stop], &chunk).unwrap();
        let list = out.as_any().downcast_ref::<ListArray>().unwrap();
        let values = list.values().as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(values.values(), &[1, 2, 3]);
    }

    #[test]
    fn test_array_generate_two_args_desc() {
        let mut arena = ExprArena::default();
        let chunk = chunk_len_1();
        let list_type = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));
        let expr = typed_null(&mut arena, list_type);
        let start = literal_i64(&mut arena, 3);
        let stop = literal_i64(&mut arena, 1);

        let out =
            eval_array_function("array_generate", &arena, expr, &[start, stop], &chunk).unwrap();
        let list = out.as_any().downcast_ref::<ListArray>().unwrap();
        let values = list.values().as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(values.values(), &[3, 2, 1]);
    }

    #[test]
    fn test_array_generate_three_args_and_empty() {
        let mut arena = ExprArena::default();
        let chunk = chunk_len_1();
        let list_type = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));
        let expr = typed_null(&mut arena, list_type.clone());
        let start = literal_i64(&mut arena, 1);
        let stop = literal_i64(&mut arena, 5);
        let step = literal_i64(&mut arena, 2);

        let out = eval_array_function("array_generate", &arena, expr, &[start, stop, step], &chunk)
            .unwrap();
        let list = out.as_any().downcast_ref::<ListArray>().unwrap();
        let values = list.values().as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(values.values(), &[1, 3, 5]);

        let expr2 = typed_null(&mut arena, list_type);
        let start2 = literal_i64(&mut arena, 3);
        let stop2 = literal_i64(&mut arena, 2);
        let step2 = literal_i64(&mut arena, 1);
        let out2 = eval_array_function(
            "array_generate",
            &arena,
            expr2,
            &[start2, stop2, step2],
            &chunk,
        )
        .unwrap();
        let list2 = out2.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(list2.value_length(0), 0);
    }

    #[test]
    fn test_array_generate_date_with_unit_arg() {
        let mut arena = ExprArena::default();
        let chunk = chunk_len_1();
        let list_type = DataType::List(Arc::new(Field::new(
            "item",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        )));
        let expr = typed_null(&mut arena, list_type);
        let start = arena.push_typed(
            crate::exec::expr::ExprNode::Literal(crate::exec::expr::LiteralValue::Utf8(
                "2025-10-01".to_string(),
            )),
            DataType::Utf8,
        );
        let stop = arena.push_typed(
            crate::exec::expr::ExprNode::Literal(crate::exec::expr::LiteralValue::Utf8(
                "2025-10-05".to_string(),
            )),
            DataType::Utf8,
        );
        let step = literal_i64(&mut arena, 1);
        let unit = arena.push_typed(
            crate::exec::expr::ExprNode::Literal(crate::exec::expr::LiteralValue::Utf8(
                "day".to_string(),
            )),
            DataType::Utf8,
        );
        let out = eval_array_function(
            "array_generate",
            &arena,
            expr,
            &[start, stop, step, unit],
            &chunk,
        )
        .unwrap();
        let list = out.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(list.value_length(0), 5);
    }
}
