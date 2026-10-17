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
use novarocks_functions::{
    Selection,
    builtin::{
        collection_cardinality_core::count_observed, collection_offset_count::OffsetCountFailure,
    },
};
pub fn eval_cardinality<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    let arr = arena.eval(args[0], chunk)?;
    let out = count_observed(
        arr.as_ref(),
        Selection::all(arr.len()),
        |_, row| Ok::<_, String>(row),
        &mut |_| Ok(()),
    )
    .map_err(|failure| match failure {
        OffsetCountFailure::Data(message) | OffsetCountFailure::Control(message) => message,
    })?;
    Ok((super::common::cast_output(out, arena.data_type(expr), "cardinality"))?)
}
