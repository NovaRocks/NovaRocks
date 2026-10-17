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
use crate::exec::expr::{ExprArena, ExprId, ExprNode, LiteralValue};
use arrow::array::ArrayRef;
use novarocks_functions::builtin::regexp_count::{self, PatternSource};

pub fn eval_regexp_count<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    let len = chunk.len();
    let str_arr = arena.eval(args[0], chunk)?;
    let pat_arr = arena.eval(args[1], chunk)?;
    // Preserve both original type admissions before observing the source node.
    regexp_count::string_or_null(&str_arr)?;
    regexp_count::string_or_null(&pat_arr)?;
    let literal_policy = if let Some(source) = arena.arena().regexp_count_pattern_source(expr) {
        arena
            .arena()
            .validate_regexp_count_pattern_source(expr, source)?;
        source.invalid_pattern_is_error()
    } else {
        // Raw reference construction preserves its historical Literal vs pool
        // distinction. Only real emitted-source authors install a receipt.
        matches!(
            arena.node(args[1]),
            Some(ExprNode::Literal(LiteralValue::Utf8(_)))
        )
    };
    let source = if literal_policy {
        PatternSource::Utf8Literal
    } else {
        PatternSource::Other
    };
    Ok((regexp_count::evaluate_legacy(&[str_arr, pat_arr], len, source))?)
}
