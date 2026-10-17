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
use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field};
use novarocks_functions::builtin::array_repeat_core::{cast_repeat_count, repeat_values};
use std::sync::Arc;

pub fn eval_array_repeat<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    let source = arena.eval(args[0], chunk)?;
    let count_array = cast_repeat_count(arena.eval(args[1], chunk)?)?;
    let counts = count_array
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| "array_repeat internal count downcast failure".to_string())?;

    let output_field = match arena.data_type(expr) {
        Some(DataType::List(field)) => field.clone(),
        _ => Arc::new(Field::new("item", source.data_type().clone(), true)),
    };
    Ok((repeat_values(source, counts, chunk.len(), output_field))?)
}
