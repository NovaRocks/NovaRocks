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
use arrow::array::{ArrayRef, LargeBinaryArray};

use crate::exec::chunk::Chunk;
use crate::exec::expr::{ExprArena, ExprId};

pub(super) fn eval_variant_arg<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    args: &[ExprId],
    chunk: &Chunk,
    fn_name: &str,
) -> Result<ArrayRef, __Eval::Error> {
    if args.len() != 1 {
        return Err((format!("{} expects 1 argument, got {}", fn_name, args.len())).into());
    }
    Ok((arena.eval(args[0], chunk))?)
}

pub(super) fn downcast_variant_arg<'a>(
    variant_array: &'a ArrayRef,
    fn_name: &str,
) -> Result<&'a LargeBinaryArray, String> {
    variant_array
        .as_any()
        .downcast_ref::<LargeBinaryArray>()
        .ok_or_else(|| format!("{} expects LargeBinary for variant argument", fn_name))
}

pub(super) fn binary_value_at_any<'a>(
    arr: &'a ArrayRef,
    row: usize,
    fn_name: &str,
) -> Result<Option<&'a [u8]>, String> {
    use arrow::array::{BinaryArray, BinaryViewArray};
    if arr.is_null(row) {
        return Ok(None);
    }
    if let Some(a) = arr.as_any().downcast_ref::<BinaryArray>() {
        return Ok(Some(a.value(row)));
    }
    if let Some(a) = arr.as_any().downcast_ref::<LargeBinaryArray>() {
        return Ok(Some(a.value(row)));
    }
    if let Some(a) = arr.as_any().downcast_ref::<BinaryViewArray>() {
        return Ok(Some(a.value(row)));
    }
    Err(format!(
        "{fn_name}: expected binary metadata/value arrays, got {:?}",
        arr.data_type()
    ))
}
