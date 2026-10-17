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
//! Original arena demand order around ONE shared FIELD state computation.
use crate::exec::chunk::Chunk;
use crate::exec::expr::{ExprArena, ExprId};
use arrow::array::ArrayRef;
use novarocks_functions::field_shared::{FieldState, validate_arity};
pub fn eval_field<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    validate_arity(args.len())?;
    let first = arena.eval(args[0], chunk)?;
    let mut state = FieldState::new(&first, args.len())?;
    for (index, arg) in args[1..].iter().enumerate() {
        let candidate = arena.eval(*arg, chunk)?;
        state.step(index, &candidate)?;
    }
    Ok(state.finish())
}
