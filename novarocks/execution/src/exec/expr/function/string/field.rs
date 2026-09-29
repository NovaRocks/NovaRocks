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
use arrow::array::{Array, ArrayRef, BooleanArray, Float32Array, Float64Array, Int32Array};
use arrow::compute::kernels::cmp::eq;
use arrow::datatypes::DataType;
use std::sync::Arc;

// Arrow's equality uses IEEE totalOrder for floats (-0 != +0 and NaN == NaN).
// FIELD uses scalar equality, matching its typed comparison contract.
fn field_equal(first: &ArrayRef, candidate: &ArrayRef) -> Result<BooleanArray, String> {
    macro_rules! float_equal {
        ($array:ty) => {{
            let left = first
                .as_any()
                .downcast_ref::<$array>()
                .ok_or_else(|| "field float downcast failed".to_string())?;
            let right = candidate
                .as_any()
                .downcast_ref::<$array>()
                .ok_or_else(|| "field float downcast failed".to_string())?;
            Ok(BooleanArray::from_iter((0..left.len()).map(|row| {
                (!left.is_null(row) && !right.is_null(row))
                    .then(|| left.value(row) == right.value(row))
            })))
        }};
    }
    match first.data_type() {
        DataType::Float32 => float_equal!(Float32Array),
        DataType::Float64 => float_equal!(Float64Array),
        _ => eq(
            &first.as_ref() as &dyn arrow::array::Datum,
            &candidate.as_ref() as &dyn arrow::array::Datum,
        )
        .map_err(|error| error.to_string()),
    }
}

pub fn eval_field(
    arena: &ExprArena,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    if args.len() < 2 || args.len() - 1 > i32::MAX as usize {
        return Err("field requires a value and an INT-bounded candidate list".to_string());
    }
    let first = arena.eval(args[0], chunk)?;
    let mut indices = vec![0_i32; first.len()];
    for (index, arg) in args[1..].iter().enumerate() {
        let candidate = arena.eval(*arg, chunk)?;
        if candidate.len() != first.len() {
            return Err("field frozen argument length mismatch".to_string());
        }
        if first.data_type() == &DataType::Null {
            // Untyped NULL has no comparable values. Candidate evaluation still
            // occurs; ordinary SQL binding normally retags NULL to the common type.
            continue;
        }
        if candidate.data_type() != first.data_type() {
            return Err(format!(
                "field frozen argument mismatch: {:?}/{} vs {:?}/{}",
                first.data_type(),
                first.len(),
                candidate.data_type(),
                candidate.len()
            ));
        }
        let equal = field_equal(&first, &candidate)?;
        for (row, output) in indices.iter_mut().enumerate() {
            if *output == 0 && !equal.is_null(row) && equal.value(row) {
                *output = (index + 1) as i32;
            }
        }
    }
    Ok(Arc::new(Int32Array::from(indices)))
}
