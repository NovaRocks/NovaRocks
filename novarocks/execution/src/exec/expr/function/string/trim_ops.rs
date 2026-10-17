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
use novarocks_functions::builtin::string_trim_core::{StringTrimOp, trimmed_observed};
use std::{convert::Infallible, sync::Arc};

fn eval_trim_impl<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    args: &[ExprId],
    chunk: &Chunk,
    mode: StringTrimOp,
) -> Result<ArrayRef, __Eval::Error> {
    let str_arr = arena.eval(args[0], chunk)?;
    let s_arr = str_arr
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| "trim expects string".to_string())?;
    let len = s_arr.len();
    let mut out = Vec::with_capacity(len);
    for i in 0..len {
        if s_arr.is_null(i) {
            out.push(None);
            continue;
        }
        let s = s_arr.value(i);
        let selected = match trimmed_observed(mode, s, || Ok::<(), Infallible>(())) {
            Ok(value) => value,
            Err(never) => match never {},
        };
        let v = selected.to_string();
        out.push(Some(v));
    }
    Ok(Arc::new(StringArray::from(out)) as ArrayRef)
}

pub fn eval_ltrim<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    Ok((eval_trim_impl(arena, args, chunk, StringTrimOp::Left))?)
}

pub fn eval_rtrim<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    Ok((eval_trim_impl(arena, args, chunk, StringTrimOp::Right))?)
}

pub fn eval_trim<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    Ok((eval_trim_impl(arena, args, chunk, StringTrimOp::Both))?)
}

#[cfg(test)]
mod legacy_trim_contract_tests {
    use super::*;
    use crate::exec::{chunk::ChunkSchema, expr::ExprNode};
    use arrow::{
        array::Int64Array,
        datatypes::{Field, Schema},
        record_batch::RecordBatch,
    };
    use novarocks_types::SlotId;

    fn evaluate(values: ArrayRef, mode: StringTrimOp) -> Result<ArrayRef, String> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "s",
            values.data_type().clone(),
            true,
        )]));
        let batch = RecordBatch::try_new(schema.clone(), vec![values.clone()]).unwrap();
        let slots = [SlotId::new(1)];
        let chunk = Chunk::new_with_chunk_schema(
            batch,
            ChunkSchema::try_ref_from_schema_and_slot_ids(&schema, &slots).unwrap(),
        );
        let mut arena = ExprArena::default();
        let arg = arena.push_typed(ExprNode::SlotId(slots[0]), values.data_type().clone());
        let frozen = arena.into_immutable().unwrap();
        eval_trim_impl(
            &ExprArena::from_immutable(&frozen).unwrap(),
            &[arg],
            &chunk,
            mode,
        )
    }

    #[test]
    fn legacy_trim_contract_tests_fixed_unicode_null_and_sliced_std_oracle() {
        let corpus = [
            "",
            " \t\n\r",
            "\u{2003}x\u{a0}",
            " \0 ",
            "\u{200b}x\u{feff}",
            "\u{180e}\u{1c}",
            " 你好👩‍💻 ",
        ];
        let backing = Arc::new(StringArray::from(
            vec![Some("unused"), None]
                .into_iter()
                .chain(corpus.iter().copied().map(Some))
                .chain([Some("unused")])
                .collect::<Vec<_>>(),
        )) as ArrayRef;
        let values = backing.slice(1, corpus.len() + 1);
        for mode in [StringTrimOp::Left, StringTrimOp::Right, StringTrimOp::Both] {
            let expected = corpus
                .iter()
                .map(|s| {
                    Some(match mode {
                        StringTrimOp::Left => s.trim_start(),
                        StringTrimOp::Right => s.trim_end(),
                        StringTrimOp::Both => s.trim(),
                    })
                })
                .collect::<Vec<_>>();
            let actual = evaluate(values.clone(), mode).unwrap();
            assert_eq!(actual.data_type(), &arrow::datatypes::DataType::Utf8);
            assert_eq!(
                actual
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .iter()
                    .collect::<Vec<_>>(),
                [vec![None], expected].concat()
            );
        }
        let fixed = Arc::new(StringArray::from(vec![Some("\u{2003}x\u{a0}")])) as ArrayRef;
        for (mode, expected) in [
            (StringTrimOp::Both, "x"),
            (StringTrimOp::Left, "x\u{a0}"),
            (StringTrimOp::Right, "\u{2003}x"),
        ] {
            assert_eq!(
                evaluate(fixed.clone(), mode)
                    .unwrap()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .value(0),
                expected
            );
        }
    }
    #[test]
    fn legacy_trim_contract_tests_empty_carrier_and_exact_raw_downcast_message() {
        for mode in [StringTrimOp::Left, StringTrimOp::Right, StringTrimOp::Both] {
            let wrong = Arc::new(Int64Array::from(vec![Some(1), None])) as ArrayRef;
            assert_eq!(evaluate(wrong, mode).unwrap_err(), "trim expects string");
        }
        for mode in [StringTrimOp::Left, StringTrimOp::Right, StringTrimOp::Both] {
            let empty = Arc::new(StringArray::from(Vec::<Option<&str>>::new())) as ArrayRef;
            assert_eq!(evaluate(empty, mode).unwrap().len(), 0);
        }
    }
}
