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
use novarocks_functions::{
    Selection,
    builtin::map_size_core::{MapSizeFailure, count_observed, map_input},
};
pub fn eval_map_size<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    let _ = chunk;
    let map_arr = arena.eval(args[0], chunk)?;
    let map = map_input(map_arr.as_ref())?;
    let out = count_observed(
        map,
        Selection::all(map.len()),
        |_, row| Ok::<_, String>(row),
        &mut |_| Ok(()),
    )
    .map_err(|failure| match failure {
        MapSizeFailure::Data(message) | MapSizeFailure::Control(message) => message,
    })?;
    Ok((super::common::cast_output(out, arena.data_type(expr), "map_size"))?)
}
