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
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BinaryArray, BinaryBuilder, Int64Array, Int64Builder, LargeBinaryArray,
    LargeStringArray, StringArray,
};

use crate::exec::chunk::Chunk;
use crate::exec::expr::agg::{AggStateArena, build_kernel_set};
use crate::exec::expr::{ExprArena, ExprId};
use crate::exec::node::aggregate::{AggFunction, AggTypeSignature};

const HLL_DATA_EMPTY: u8 = 0;

pub fn eval_hll_empty(
    _arena: &ExprArena,
    _expr: ExprId,
    _args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let mut builder = BinaryBuilder::new();
    for _ in 0..chunk.len() {
        builder.append_value([HLL_DATA_EMPTY]);
    }
    Ok(Arc::new(builder.finish()) as ArrayRef)
}

pub fn eval_hll_serialize(
    arena: &ExprArena,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    if args.len() != 1 {
        return Err(format!(
            "hll_serialize expects 1 argument, got {}",
            args.len()
        ));
    }
    let input = arena.eval(args[0], chunk)?;
    copy_as_binary(&input)
}

pub fn eval_hll_deserialize(
    arena: &ExprArena,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    if args.len() != 1 {
        return Err(format!(
            "hll_deserialize expects 1 argument, got {}",
            args.len()
        ));
    }
    let input = arena.eval(args[0], chunk)?;
    copy_as_binary(&input)
}

pub fn eval_hll_cardinality(
    arena: &ExprArena,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    if args.len() != 1 {
        return Err(format!(
            "hll_cardinality expects 1 argument, got {}",
            args.len()
        ));
    }

    let input = arena.eval(args[0], chunk)?;
    let kernels = build_kernel_set(
        &[AggFunction {
            name: "hll_union_agg".to_string(),
            inputs: vec![args[0]],
            input_is_intermediate: false,
            types: Some(AggTypeSignature {
                intermediate_type: None,
                output_type: Some(arrow::datatypes::DataType::Int64),
                input_arg_type: Some(input.data_type().clone()),
            }),
        }],
        &[Some(input.data_type().clone())],
    )?;
    let kernel = kernels
        .entries
        .first()
        .ok_or_else(|| "hll_cardinality failed to build aggregate kernel".to_string())?;

    let mut state_arena = AggStateArena::new(8 * 1024);
    let mut state_ptrs = Vec::with_capacity(input.len());
    for _ in 0..input.len() {
        let ptr = state_arena.alloc(kernels.layout.total_size, kernel.state_align());
        kernel.init_state(ptr);
        state_ptrs.push(ptr);
    }

    let input_opt = Some(input.clone());
    let view = kernel.build_input_view(&input_opt)?;
    kernel.update_batch(&state_ptrs, &view)?;

    let raw = kernel.build_array(&state_ptrs, false)?;
    let raw_int = raw
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| "hll_cardinality expected Int64 aggregate output".to_string())?;

    let mut out = Int64Builder::with_capacity(raw_int.len());
    for row in 0..raw_int.len() {
        if input.is_null(row) || raw_int.is_null(row) {
            out.append_null();
        } else {
            out.append_value(raw_int.value(row));
        }
    }

    for state_ptr in state_ptrs {
        kernel.drop_state(state_ptr);
    }

    Ok(Arc::new(out.finish()) as ArrayRef)
}

fn copy_as_binary(input: &ArrayRef) -> Result<ArrayRef, String> {
    if let Some(arr) = input.as_any().downcast_ref::<BinaryArray>() {
        let mut builder = BinaryBuilder::new();
        for row in 0..arr.len() {
            if arr.is_null(row) {
                builder.append_null();
            } else {
                builder.append_value(arr.value(row));
            }
        }
        return Ok(Arc::new(builder.finish()) as ArrayRef);
    }

    if let Some(arr) = input.as_any().downcast_ref::<StringArray>() {
        let mut builder = BinaryBuilder::new();
        for row in 0..arr.len() {
            if arr.is_null(row) {
                builder.append_null();
            } else {
                builder.append_value(arr.value(row).as_bytes());
            }
        }
        return Ok(Arc::new(builder.finish()) as ArrayRef);
    }

    if let Some(arr) = input.as_any().downcast_ref::<LargeBinaryArray>() {
        let mut builder = BinaryBuilder::new();
        for row in 0..arr.len() {
            if arr.is_null(row) {
                builder.append_null();
            } else {
                builder.append_value(arr.value(row));
            }
        }
        return Ok(Arc::new(builder.finish()) as ArrayRef);
    }

    if let Some(arr) = input.as_any().downcast_ref::<LargeStringArray>() {
        let mut builder = BinaryBuilder::new();
        for row in 0..arr.len() {
            if arr.is_null(row) {
                builder.append_null();
            } else {
                builder.append_value(arr.value(row).as_bytes());
            }
        }
        return Ok(Arc::new(builder.finish()) as ArrayRef);
    }

    Err(format!(
        "hll_serialize/hll_deserialize expects VARCHAR/BINARY input, got {:?}",
        input.data_type()
    ))
}
