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
use crate::exec::expr::function::FunctionKind;
use crate::exec::expr::{ExprArena, ExprId, ExprNode};
use arrow::array::{ArrayRef, StringArray};
use arrow::datatypes::DataType;
use novarocks_functions::EvaluationCheckpoints;
pub use novarocks_functions::builtin::calendar_time_text_shared::parse_hms_duration_to_seconds;
use novarocks_functions::builtin::calendar_time_text_shared::{
    self as shared, LegacyControl, MergeMode, TimeTextError,
};

fn parse_from_strings(string_arr: &StringArray) -> Result<Vec<Option<i64>>, String> {
    shared::duration_strings(string_arr, &mut EvaluationCheckpoints::new(&LegacyControl))
        .map_err(TimeTextError::into_legacy)
}

fn parse_direct_string_array(array: &ArrayRef) -> Result<Option<Vec<Option<i64>>>, String> {
    let Some(str_arr) = array.as_any().downcast_ref::<StringArray>() else {
        return Ok(None);
    };
    parse_from_strings(str_arr).map(Some)
}

fn strip_cast_wrappers<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    mut expr_id: ExprId,
) -> ExprId {
    loop {
        match arena.node(expr_id) {
            Some(
                ExprNode::Cast(child, _)
                | ExprNode::CastTime(child, _)
                | ExprNode::CastTimeFromDatetime(child, _),
            ) => expr_id = *child,
            _ => return expr_id,
        }
    }
}

fn parse_from_sec_to_time_source<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    arg_expr: ExprId,
    chunk: &Chunk,
) -> Result<Option<Vec<Option<i64>>>, __Eval::Error> {
    let inner = strip_cast_wrappers(arena, arg_expr);
    let Some(ExprNode::FunctionCall { kind, args }) = arena.node(inner) else {
        return Ok(None);
    };
    let FunctionKind::Date(name) = kind else {
        return Ok(None);
    };
    if *name != "sec_to_time" || args.len() != 1 {
        return Ok(None);
    }

    let source = arena.eval(args[0], chunk)?;
    Ok(
        (shared::sec_to_time_source(&source, &mut EvaluationCheckpoints::new(&LegacyControl))
            .map(Some)
            .map_err(TimeTextError::into_legacy))?,
    )
}

fn parse_from_immediate_cast_string_source<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    arg_expr: ExprId,
    chunk: &Chunk,
) -> Result<Option<Vec<Option<i64>>>, __Eval::Error> {
    let child = match arena.node(arg_expr) {
        Some(
            ExprNode::Cast(child, _)
            | ExprNode::CastTime(child, _)
            | ExprNode::CastTimeFromDatetime(child, _),
        ) => *child,
        _ => return Ok(None),
    };
    let source = arena.eval(child, chunk)?;
    Ok((parse_direct_string_array(&source))?)
}

pub fn parse_from_cast_source<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    arg_expr: ExprId,
    chunk: &Chunk,
) -> Result<Option<Vec<Option<i64>>>, __Eval::Error> {
    let mut current = arg_expr;
    let mut seen_cast = false;
    while let Some(
        ExprNode::Cast(child, _)
        | ExprNode::CastTime(child, _)
        | ExprNode::CastTimeFromDatetime(child, _),
    ) = arena.node(current)
    {
        current = *child;
        seen_cast = true;
    }
    if !seen_cast {
        return Ok(None);
    }
    let source = arena.eval(current, chunk)?;
    Ok(
        (shared::duration_cast_source(&source, &mut EvaluationCheckpoints::new(&LegacyControl))
            .map_err(TimeTextError::into_legacy))?,
    )
}

pub fn eval_time_to_sec<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    let arg_expr = args[0];
    let mut work = EvaluationCheckpoints::new(&LegacyControl);
    if let Some(out) = parse_from_sec_to_time_source(arena, arg_expr, chunk)? {
        return Ok((shared::seconds_output(out, &mut work).map_err(TimeTextError::into_legacy))?);
    }
    let arr = arena.eval(arg_expr, chunk)?;
    if let Some(out) = parse_direct_string_array(&arr)? {
        return Ok((shared::seconds_output(out, &mut work).map_err(TimeTextError::into_legacy))?);
    }
    let mut out = shared::datetime_seconds(&arr, &mut work).map_err(TimeTextError::into_legacy)?;
    if let Some(source_out) = parse_from_immediate_cast_string_source(arena, arg_expr, chunk)? {
        shared::merge_source(&mut out, &source_out, MergeMode::Override, &mut work)
            .map_err(TimeTextError::into_legacy)?;
        return Ok((shared::seconds_output(out, &mut work).map_err(TimeTextError::into_legacy))?);
    }
    if shared::any_null(&out, &mut work).map_err(TimeTextError::into_legacy)?
        && let Some(source_out) = parse_from_cast_source(arena, arg_expr, chunk)?
    {
        shared::merge_source(&mut out, &source_out, MergeMode::FillNull, &mut work)
            .map_err(TimeTextError::into_legacy)?;
    }
    Ok((shared::seconds_output(out, &mut work).map_err(TimeTextError::into_legacy))?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exec::chunk::ChunkSchema;
    use crate::exec::expr::function::FunctionKind;
    use crate::exec::expr::{ExprArena, ExprNode, LiteralValue};
    use arrow::array::{Array, Int32Array, Int64Array};
    use arrow::datatypes::{Field, Schema};
    use arrow::record_batch::RecordBatch;
    use novarocks_types::SlotId;
    use std::sync::Arc;

    fn one_row_chunk() -> Chunk {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "dummy",
            DataType::Int32,
            true,
        )]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1]))]).unwrap();
        let chunk_schema =
            ChunkSchema::try_ref_from_schema_and_slot_ids(batch.schema().as_ref(), &[SlotId(1)])
                .expect("chunk schema");
        Chunk::new_with_chunk_schema(batch, chunk_schema)
    }

    #[test]
    fn time_to_sec_preserves_sec_to_time_negative_roundtrip() {
        let mut arena = ExprArena::default();
        let arg = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(-1)), DataType::Int64);
        let sec_to_time = arena.push_typed(
            ExprNode::FunctionCall {
                kind: FunctionKind::Date("sec_to_time"),
                args: vec![arg],
            },
            DataType::Utf8,
        );
        let time_to_sec = arena.push_typed(
            ExprNode::FunctionCall {
                kind: FunctionKind::Date("time_to_sec"),
                args: vec![sec_to_time],
            },
            DataType::Int64,
        );

        let out = arena.eval(time_to_sec, &one_row_chunk()).expect("eval");
        let out = out.as_any().downcast_ref::<Int64Array>().expect("int64");
        assert_eq!(out.value(0), -1);
    }
}
