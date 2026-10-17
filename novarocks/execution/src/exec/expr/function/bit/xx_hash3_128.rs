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
use novarocks_functions::builtin::xx_hash3_128;
pub fn eval_xx_hash3_128<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    // Preserve interleaved evaluation and admission, including the original
    // first unsupported argument before any later child is evaluated.
    let mut inputs = Vec::with_capacity(args.len());
    for (idx, arg) in args.iter().enumerate() {
        inputs.push(xx_hash3_128::prepare_legacy_input(
            arena.eval(*arg, chunk)?,
            idx,
        )?);
    }
    Ok((xx_hash3_128::evaluate_legacy(&inputs, chunk.len(), arena.data_type(expr)))?)
}
#[cfg(test)]
#[path = "legacy_xx_hash3_128_baseline_tests.rs"]
mod legacy_xx_hash3_128_baseline_tests;
