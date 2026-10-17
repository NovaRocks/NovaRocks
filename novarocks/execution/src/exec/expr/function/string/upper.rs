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
use novarocks_functions::builtin::string_case::{StringCaseOp, evaluate_legacy_case};
/// V1 retains child evaluation; the shared core owns whole-string calculation.
pub fn eval_upper<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    value_expr: ExprId,
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    let input = arena.eval(value_expr, chunk)?;
    Ok((evaluate_legacy_case(StringCaseOp::Upper, &input))?)
}
