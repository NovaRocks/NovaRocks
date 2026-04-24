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
use crate::exec::expr::{ExprArena, ExprId};
use arrow::array::{Array, ArrayRef, BooleanArray, UInt32Array};
use arrow::compute::take;

pub fn eval_element_at(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let map_arr = arena.eval(args[0], chunk)?;
    let key_arr = arena.eval(args[1], chunk)?;
    let check_arr = if args.len() == 3 {
        Some(arena.eval(args[2], chunk)?)
    } else {
        None
    };
    let map = map_arr
        .as_any()
        .downcast_ref::<arrow::array::MapArray>()
        .ok_or_else(|| format!("element_at expects MapArray, got {:?}", map_arr.data_type()))?;
    let check_arr = check_arr
        .as_ref()
        .map(|a| {
            a.as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| "element_at check flag must be BOOLEAN".to_string())
        })
        .transpose()?;

    if key_arr.len() != 1 && key_arr.len() != map.len() {
        return Err(format!(
            "element_at key length mismatch: map rows={}, key rows={}",
            map.len(),
            key_arr.len()
        ));
    }
    if let Some(flags) = check_arr {
        if flags.len() != 1 && flags.len() != map.len() {
            return Err(format!(
                "element_at check flag length mismatch: map rows={}, check rows={}",
                map.len(),
                flags.len()
            ));
        }
    }

    let keys = map.keys();
    let values = map.values();
    let offsets = map.value_offsets();

    // Short-circuit when the underlying entries are empty: every lookup
    // yields NULL (or errors if the caller requested strict bounds-checking
    // and there is any non-null row). Avoiding arrow's `take` here side-steps
    // a known panic in `take_fixed_size_binary` when the underlying values
    // are length 0 — it reads `values.value(0)` even for indices flagged as
    // null.
    if values.is_empty() {
        if let Some(flags) = check_arr {
            for row in 0..map.len() {
                let check_idx = super::common::row_index(row, flags.len());
                let strict = !flags.is_null(check_idx) && flags.value(check_idx);
                if strict && !map.is_null(row) {
                    return Err("Key not present in map".to_string());
                }
            }
        }
        let out = arrow::array::new_null_array(values.data_type(), map.len());
        return super::common::cast_output(out, arena.data_type(expr), "element_at");
    }

    let mut indices = Vec::with_capacity(map.len());
    for row in 0..map.len() {
        let key_idx = super::common::row_index(row, key_arr.len());
        let check_idx = check_arr.map(|flags| super::common::row_index(row, flags.len()));
        let check_out_of_bounds = check_idx
            .map(|idx| {
                let flags = check_arr.expect("check_arr exists when check_idx exists");
                !flags.is_null(idx) && flags.value(idx)
            })
            .unwrap_or(false);

        if map.is_null(row) {
            indices.push(None);
            continue;
        }

        let start = offsets[row] as usize;
        let end = offsets[row + 1] as usize;
        let mut found = None;
        if key_arr.is_null(key_idx) {
            for i in (start..end).rev() {
                if keys.is_null(i) {
                    found = Some(i as u32);
                    break;
                }
            }
        } else {
            for i in (start..end).rev() {
                if super::common::compare_keys_at(keys, i, &key_arr, key_idx)? {
                    found = Some(i as u32);
                    break;
                }
            }
        }

        if found.is_none() && check_out_of_bounds {
            return Err("Key not present in map".to_string());
        }
        if let Some(v) = found {
            indices.push(Some(v));
        } else {
            indices.push(None);
        }
    }

    let indices = UInt32Array::from(indices);
    // arrow's `take_fixed_size_binary` does not honour the indices array null
    // bitmap: it dereferences the placeholder slot (0) for null indices even
    // when `values` is non-nullable. Merge the indices null bitmap into the
    // output so LARGEINT lookups return NULL rather than the value at slot 0.
    let out = take(values.as_ref(), &indices, None).map_err(|e| e.to_string())?;
    let out = apply_indices_nulls(&out, &indices);
    super::common::cast_output(out, arena.data_type(expr), "element_at")
}

fn apply_indices_nulls(out: &ArrayRef, indices: &UInt32Array) -> ArrayRef {
    let Some(idx_nulls) = indices.nulls() else {
        return out.clone();
    };
    if idx_nulls.null_count() == 0 {
        return out.clone();
    }
    let combined_nulls = match out.nulls() {
        Some(existing) => arrow_buffer::NullBuffer::union(Some(existing), Some(idx_nulls))
            .expect("combined null buffer is always Some"),
        None => idx_nulls.clone(),
    };
    let data = out.to_data().into_builder().nulls(Some(combined_nulls));
    arrow::array::make_array(unsafe { data.build_unchecked() })
}
