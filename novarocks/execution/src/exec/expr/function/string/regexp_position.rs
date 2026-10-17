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
use novarocks_functions::builtin::regexp_position::{
    evaluate_legacy, validate_legacy_position, validate_legacy_strings,
};

pub fn eval_regexp_position<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    let _ = expr;
    let str_arr = arena.eval(args[0], chunk)?;
    let pat_arr = arena.eval(args[1], chunk)?;
    validate_legacy_strings(&str_arr, &pat_arr)?;
    let mut arrays = vec![str_arr, pat_arr];
    if args.len() >= 3 {
        let start = arena.eval(args[2], chunk)?;
        validate_legacy_position(&start)?;
        arrays.push(start);
    }
    if args.len() >= 4 {
        let occurrence = arena.eval(args[3], chunk)?;
        validate_legacy_position(&occurrence)?;
        arrays.push(occurrence);
    }
    Ok((evaluate_legacy(&arrays, chunk.len()))?)
}
