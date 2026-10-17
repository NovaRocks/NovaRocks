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
use arrow::array::{Array, ArrayRef, StringArray};
use novarocks_functions::builtin::string_substring_index_core::{
    ForwardMatchAccess, forward_match_indices, substring_index_span,
};
use std::{convert::Infallible, sync::Arc};

use super::common::downcast_int_arg_array;

pub fn eval_substring_index<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    let _ = expr;
    let str_arr = arena.eval(args[0], chunk)?;
    let delim_arr = arena.eval(args[1], chunk)?;
    let count_arr = arena.eval(args[2], chunk)?;
    let s_arr = str_arr
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| "substring_index expects string".to_string())?;
    let d_arr = delim_arr
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| "substring_index expects string".to_string())?;
    let c_arr = downcast_int_arg_array(&count_arr, "substring_index")?;
    let len = s_arr.len();
    let mut out = Vec::with_capacity(len);
    for i in 0..len {
        if s_arr.is_null(i) || d_arr.is_null(i) || c_arr.is_null(i) {
            out.push(None);
            continue;
        }
        let s = s_arr.value(i);
        let delim = d_arr.value(i);
        let count = c_arr.value(i);
        let mut access = LegacyMatchAccess {
            text: s,
            positions: Vec::new(),
        };
        let span = match substring_index_span(&mut access, delim, count) {
            Ok(span) => span,
            Err(never) => match never {},
        };
        out.push(span.map(|span| span.to_string()));
    }
    Ok(Arc::new(StringArray::from(out)) as ArrayRef)
}

struct LegacyMatchAccess<'text> {
    text: &'text str,
    positions: Vec<usize>,
}
impl<'text> ForwardMatchAccess<'text> for LegacyMatchAccess<'text> {
    type Error = Infallible;
    fn text(&self) -> &'text str {
        self.text
    }
    fn first_pass(
        &mut self,
        text: &str,
        delimiter: &str,
        _: Option<usize>,
    ) -> Result<usize, Infallible> {
        self.positions = forward_match_indices(text, delimiter)
            .map(|(idx, _)| idx)
            .collect::<Vec<usize>>();
        Ok(self.positions.len())
    }
    fn prefix_cut(&mut self, _: &str, _: &str, ordinal: usize) -> Result<usize, Infallible> {
        Ok(self.positions[ordinal - 1])
    }
    fn suffix_cut(&mut self, _: &str, _: &str, ordinal: usize) -> Result<usize, Infallible> {
        Ok(self.positions[ordinal - 1])
    }
}
