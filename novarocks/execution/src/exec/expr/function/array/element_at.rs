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
use arrow::array::{Array, ArrayRef};
use novarocks_functions::builtin::array_access_core::{
    AccessFailure, ArrayAccessInputs, lookup_observed,
};
use std::convert::Infallible;
fn legacy_error(error: AccessFailure<Infallible>) -> String {
    match error {
        AccessFailure::Data(message) => message,
        AccessFailure::Control(never) => match never {},
    }
}
pub fn eval_element_at<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    let arr = arena.eval(args[0], chunk)?;
    let subscript_arr = arena.eval(args[1], chunk)?;
    let check_arr = if args.len() == 3 {
        Some(arena.eval(args[2], chunk)?)
    } else {
        None
    };
    let mut observer = |_| Ok::<(), Infallible>(());
    let inputs = ArrayAccessInputs::new(arr, subscript_arr, check_arr, &mut observer)
        .map_err(legacy_error)?;
    inputs.validate_legacy_lengths()?;
    Ok((lookup_observed(
        &inputs,
        novarocks_functions::Selection::all(inputs.list().len()),
        |_, row| Ok::<_, Infallible>(inputs.legacy_rows(row)),
        arena.data_type(expr),
        |_, _| Ok::<(), Infallible>(()),
        &mut observer,
    )
    .map_err(legacy_error))?)
}
