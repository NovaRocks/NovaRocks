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
use arrow::array::ArrayRef;
use arrow::datatypes::DataType;
use novarocks_functions::builtin::array_append_core::{
    AppendFailure, AppendOutputFacts, ArrayAppendInputs, append_observed,
};
use std::convert::Infallible;
fn legacy_error(error: AppendFailure<Infallible>) -> String {
    match error {
        AppendFailure::Data(message) => message,
        AppendFailure::Control(never) => match never {},
    }
}
pub fn eval_array_append<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    let arr = arena.eval(args[0], chunk)?;
    let target = arena.eval(args[1], chunk)?;
    let output = match arena.data_type(expr) {
        Some(DataType::List(field)) => AppendOutputFacts::FrozenList(field),
        _ => AppendOutputFacts::OriginalInputList,
    };
    let mut observer = |_| Ok::<(), Infallible>(());
    let inputs =
        ArrayAppendInputs::new(arr, target, output, &mut observer).map_err(legacy_error)?;
    Ok((append_observed(
        &inputs,
        novarocks_functions::Selection::all(chunk.len()),
        |_, row| Ok::<_, Infallible>(inputs.legacy_rows(row)),
        |_, _, _, _| Ok::<(), Infallible>(()),
        &mut observer,
    )
    .map_err(legacy_error))?)
}
