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
//! The original default Percentile-to-Binary CAST leaf, after exact frozen
//! source/target validation. No kernel state, buffer or array is constructed.
use crate::exec::{
    chunk::Chunk,
    expr::{ExprArena, ExprId},
};
use arrow::array::ArrayRef;
#[cfg(test)]
use arrow::datatypes::DataType;
use novarocks_functions::builtin::value_conversion::{
    is_default_json_text_keep, is_default_percentile_binary_keep, share_same_structure_keep,
};

pub(in crate::exec::expr) fn eval_percentile_to_binary<
    __Eval: crate::exec::expr::ExprEvaluation + ?Sized,
>(
    arena: &__Eval,
    expression: ExprId,
    arguments: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    Ok((eval_same_structure_keep(
        arena,
        expression,
        arguments,
        chunk,
        is_default_percentile_binary_keep,
        [
            "opaque Binary conversion requires one argument",
            "opaque Binary conversion lost its complete source declaration",
            "opaque Binary conversion lost its complete target declaration",
            "opaque Binary conversion differs from its complete Percentile source or Physical target",
            "opaque Binary conversion input differs from its complete declared carrier",
            "opaque Binary conversion received NULL in a required source",
        ],
    ))?)
}

pub(in crate::exec::expr) fn eval_json_to_text<
    __Eval: crate::exec::expr::ExprEvaluation + ?Sized,
>(
    arena: &__Eval,
    expression: ExprId,
    arguments: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    Ok((eval_same_structure_keep(
        arena,
        expression,
        arguments,
        chunk,
        is_default_json_text_keep,
        [
            "JSON text conversion requires one argument",
            "JSON text conversion lost its complete source declaration",
            "JSON text conversion lost its complete target declaration",
            "JSON text conversion differs from its exact complete source or target",
            "JSON text conversion input differs from its complete declared carrier",
            "JSON text conversion received NULL in a required source",
        ],
    ))?)
}

fn eval_same_structure_keep<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expression: ExprId,
    arguments: &[ExprId],
    chunk: &Chunk,
    supported: fn(
        &novarocks_type_contract::FunctionValueType,
        &novarocks_type_contract::FunctionValueType,
    ) -> bool,
    errors: [&'static str; 6],
) -> Result<ArrayRef, __Eval::Error> {
    let [child] = arguments else {
        return Err(errors[0].into());
    };
    let source = arena.value_type(*child).ok_or(errors[1])?;
    let target = arena.value_type(expression).ok_or(errors[2])?;
    if !supported(source, target) {
        return Err(errors[3].into());
    }
    // The original arena evaluates this sole child eagerly and propagates its
    // error. Both existing CAST operations share precisely that array,
    // including slice offsets/NULL bits; the family matcher authorizes the
    // complete target and no type is inferred from the observed payload.
    let value = arena.eval(*child, chunk)?;
    // Reuse the existing default carrier owner: legal string dictionaries
    // are format choices, while the complete logical source/target above are
    // unchanged. Keep does not retag or decode the existing payload.
    if value.len() != chunk.len()
        || crate::exec::chunk::type_compatibility::check_exact(
            source.data_type(),
            value.data_type(),
        )
        .is_err()
    {
        return Err(errors[4].into());
    }
    if !source.nullable() && value.null_count() != 0 {
        return Err(errors[5].into());
    }
    Ok(share_same_structure_keep(&value))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exec::chunk::{ChunkSchema, ChunkSlotSchema};
    use crate::exec::expr::{ExprNode, function::FunctionKind};
    use arrow::array::{BinaryArray, RecordBatch};
    use arrow::datatypes::Field;
    use novarocks_type_contract::{FunctionValueType, ValueLogicalType, ValueTypeStorageLifetime};
    use novarocks_types::SlotId;
    use std::sync::Arc;

    #[test]
    fn percentile_binary_keep_freeze_thaw_preserves_array_nulls_slices_and_empty() {
        // These are explicit pure carrier fixtures. The production actor is
        // reached through the real FE frozen request in Native roundtrip tests.
        // No successful test-only allocation grant is installed here.
        let array: ArrayRef = Arc::new(BinaryArray::from(vec![
            Some(b"unused".as_slice()),
            Some(b"opaque".as_slice()),
            None,
        ]));
        let all_null: ArrayRef = Arc::new(BinaryArray::from(vec![None::<&[u8]>, None]));
        let cases = [
            (array.slice(1, 1), false),
            (array.slice(1, 2), true),
            (all_null, true),
            (array.slice(2, 0), true),
        ];
        for (array, nullable) in cases {
            let source = FunctionValueType::try_with_logical_type(
                DataType::Binary,
                nullable,
                ValueLogicalType::Percentile,
            )
            .unwrap();
            let target = FunctionValueType::new(DataType::Binary, nullable);
            let field = Arc::new(Field::new("percentile", DataType::Binary, nullable));
            let slot = ChunkSlotSchema::try_new_typed(
                SlotId(7),
                field,
                source.typed_binding().clone(),
                None,
                ValueTypeStorageLifetime::pure(),
            )
            .unwrap();
            let schema = Arc::new(
                ChunkSchema::try_new_typed(vec![slot], ValueTypeStorageLifetime::pure()).unwrap(),
            );
            let batch =
                RecordBatch::try_new(schema.arrow_schema_ref(), vec![array.clone()]).unwrap();
            let chunk = Chunk::try_new_with_chunk_schema(batch, schema).unwrap();
            let mut arena = ExprArena::default();
            let child = arena.push_value_typed(ExprNode::SlotId(SlotId(7)), source.clone());
            let id = arena.push_value_typed(
                ExprNode::FunctionCall {
                    kind: FunctionKind::PercentileToBinary,
                    args: vec![child],
                },
                target.clone(),
            );
            let frozen = arena.into_immutable().unwrap();
            assert!(matches!(
                frozen.nodes()[id.0].kind(),
                novarocks_local_program::StaticExprKind::FunctionCall {
                    kind: novarocks_local_program::StaticFunctionKind::PercentileToBinary,
                    ..
                }
            ));
            let thawed = ExprArena::from_immutable(&frozen).unwrap();
            assert_eq!(thawed.value_type(child), Some(&source));
            assert_eq!(thawed.value_type(id), Some(&target));
            let result = thawed.eval(id, &chunk).unwrap();
            // This is the actual returned array, not merely an equal buffer:
            // the leaf makes no new array/header and preserves original offset.
            assert!(Arc::ptr_eq(&result, &array));
            assert_eq!(result.null_count(), array.null_count());
            assert_eq!(result.len(), array.len());
            assert_eq!(
                result.as_any().downcast_ref::<BinaryArray>().unwrap(),
                array.as_any().downcast_ref::<BinaryArray>().unwrap()
            );
        }
    }
}
