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
use super::common::cast_output;
use crate::exec::chunk::Chunk;
use crate::exec::expr::{ExprArena, ExprId};
use arrow::array::{Array, ArrayRef, Float64Array, ListArray};
use arrow::compute::cast;
use arrow::datatypes::DataType;
use std::sync::Arc;

fn row_index(row: usize, len: usize, out_len: usize) -> usize {
    if len == 1 && out_len > 1 { 0 } else { row }
}

fn ensure_row_count(
    len: usize,
    out_len: usize,
    fn_name: &str,
    arg_name: &str,
) -> Result<(), String> {
    if out_len == 0 || len == out_len || len == 1 {
        return Ok(());
    }
    Err(format!(
        "{} requires array arguments with row count 1 or {}, but {} array size is {}",
        fn_name, out_len, arg_name, len
    ))
}

fn require_list_arg<'a>(arg: &'a ArrayRef, fn_name: &str) -> Result<&'a ListArray, String> {
    arg.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
        format!(
            "{} expects ListArray arguments, got {:?}",
            fn_name,
            arg.data_type()
        )
    })
}

fn cast_list_values_to_f64(
    list: &ListArray,
    fn_name: &str,
    arg_name: &str,
) -> Result<Arc<Float64Array>, String> {
    let values = list.values();
    let casted = cast(&values, &DataType::Float64).map_err(|e| {
        format!(
            "{} expects numeric array elements for {} argument: {}",
            fn_name, arg_name, e
        )
    })?;
    let out = casted
        .as_any()
        .downcast_ref::<Float64Array>()
        .ok_or_else(|| format!("{} failed to cast {} values to float64", fn_name, arg_name))?;
    if out.null_count() > 0 {
        return Err(format!(
            "{} does not support null values. {} array has null value.",
            fn_name, arg_name
        ));
    }
    Ok(Arc::new(out.clone()))
}

fn eval_cosine_impl<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
    normalized: bool,
    fn_name: &'static str,
) -> Result<ArrayRef, __Eval::Error> {
    let left = arena.eval(args[0], chunk)?;
    let right = arena.eval(args[1], chunk)?;
    let left_list = require_list_arg(&left, fn_name)?;
    let right_list = require_list_arg(&right, fn_name)?;

    if left_list.null_count() > 0 {
        return Err((format!(
            "{} does not support null values. base array has null value.",
            fn_name
        ))
        .into());
    }
    if right_list.null_count() > 0 {
        return Err((format!(
            "{} does not support null values. target array has null value.",
            fn_name
        ))
        .into());
    }

    let out_len = chunk.len();
    ensure_row_count(left_list.len(), out_len, fn_name, "base")?;
    ensure_row_count(right_list.len(), out_len, fn_name, "target")?;

    let left_values = cast_list_values_to_f64(left_list, fn_name, "base")?;
    let right_values = cast_list_values_to_f64(right_list, fn_name, "target")?;
    let left_offsets = left_list.value_offsets();
    let right_offsets = right_list.value_offsets();

    let mut out = Vec::with_capacity(out_len);
    for row in 0..out_len {
        let left_row = row_index(row, left_list.len(), out_len);
        let right_row = row_index(row, right_list.len(), out_len);

        let left_start = left_offsets[left_row] as usize;
        let left_end = left_offsets[left_row + 1] as usize;
        let right_start = right_offsets[right_row] as usize;
        let right_end = right_offsets[right_row + 1] as usize;

        let left_dim = left_end - left_start;
        let right_dim = right_end - right_start;
        if left_dim != right_dim {
            return Err((format!(
                "{} requires equal length arrays in each row. base array dimension size is {}, target array dimension size is {}.",
                fn_name, left_dim, right_dim
            )).into());
        }
        if left_dim == 0 {
            return Err((format!("{} requires non-empty arrays in each row", fn_name)).into());
        }

        let mut dot = 0.0_f64;
        let mut left_sq = 0.0_f64;
        let mut right_sq = 0.0_f64;
        for idx in 0..left_dim {
            let lv = left_values.value(left_start + idx);
            let rv = right_values.value(right_start + idx);
            dot += lv * rv;
            if !normalized {
                left_sq += lv * lv;
                right_sq += rv * rv;
            }
        }

        let value = if normalized {
            dot
        } else if left_sq == 0.0 || right_sq == 0.0 {
            0.0
        } else {
            dot / (left_sq.sqrt() * right_sq.sqrt())
        };
        out.push(Some(value));
    }

    Ok((cast_output(
        Arc::new(Float64Array::from(out)) as ArrayRef,
        arena.data_type(expr),
    ))?)
}

pub fn eval_cosine_similarity<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    Ok((eval_cosine_impl(arena, expr, args, chunk, false, "cosine_similarity"))?)
}

pub fn eval_cosine_similarity_norm<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    Ok((eval_cosine_impl(arena, expr, args, chunk, true, "cosine_similarity_norm"))?)
}

pub fn eval_l2_distance<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    let left = arena.eval(args[0], chunk)?;
    let right = arena.eval(args[1], chunk)?;
    let left_list = require_list_arg(&left, "l2_distance")?;
    let right_list = require_list_arg(&right, "l2_distance")?;

    if left_list.null_count() > 0 {
        return Err(
            ("l2_distance does not support null values. base array has null value.".to_string())
                .into(),
        );
    }
    if right_list.null_count() > 0 {
        return Err(
            ("l2_distance does not support null values. target array has null value.".to_string())
                .into(),
        );
    }

    let out_len = chunk.len();
    ensure_row_count(left_list.len(), out_len, "l2_distance", "base")?;
    ensure_row_count(right_list.len(), out_len, "l2_distance", "target")?;

    let left_values = cast_list_values_to_f64(left_list, "l2_distance", "base")?;
    let right_values = cast_list_values_to_f64(right_list, "l2_distance", "target")?;
    let left_offsets = left_list.value_offsets();
    let right_offsets = right_list.value_offsets();

    let mut out = Vec::with_capacity(out_len);
    for row in 0..out_len {
        let left_row = row_index(row, left_list.len(), out_len);
        let right_row = row_index(row, right_list.len(), out_len);

        let left_start = left_offsets[left_row] as usize;
        let left_end = left_offsets[left_row + 1] as usize;
        let right_start = right_offsets[right_row] as usize;
        let right_end = right_offsets[right_row + 1] as usize;

        let left_dim = left_end - left_start;
        let right_dim = right_end - right_start;
        if left_dim != right_dim {
            return Err((format!(
                "l2_distance requires equal length arrays in each row. base array dimension size is {}, target array dimension size is {}.",
                left_dim, right_dim
            )).into());
        }
        if left_dim == 0 {
            return Err(("l2_distance requires non-empty arrays in each row".to_string()).into());
        }

        let mut distance = 0.0_f64;
        for idx in 0..left_dim {
            let delta = left_values.value(left_start + idx) - right_values.value(right_start + idx);
            distance += delta * delta;
        }
        out.push(Some(distance));
    }

    Ok((cast_output(
        Arc::new(Float64Array::from(out)) as ArrayRef,
        arena.data_type(expr),
    ))?)
}

#[cfg(test)]
mod legacy_vector_contract_tests {
    use super::*;
    use crate::exec::chunk::ChunkSchema;
    use crate::exec::expr::ExprNode;
    use crate::exec::expr::function::lookup_function;
    use arrow::array::{
        BinaryArray, BooleanArray, Decimal128Array, FixedSizeListArray, Int8Array, StringArray,
        UInt32Array, UnionArray,
    };
    use arrow::buffer::OffsetBuffer;
    use arrow::datatypes::{Field, Schema, UnionFields};
    use arrow::record_batch::RecordBatch;
    use novarocks_types::SlotId;

    fn list(values: ArrayRef, offsets: Vec<i32>, nullable: Option<Vec<bool>>) -> ArrayRef {
        let nulls = nullable.map(|valid| {
            Int8Array::from(
                valid
                    .into_iter()
                    .map(|valid| valid.then_some(0))
                    .collect::<Vec<_>>(),
            )
            .nulls()
            .unwrap()
            .clone()
        });
        Arc::new(
            ListArray::try_new(
                Arc::new(Field::new("item", values.data_type().clone(), true)),
                OffsetBuffer::new(offsets.into()),
                values,
                nulls,
            )
            .unwrap(),
        )
    }
    fn vector(values: &[Option<f64>]) -> ArrayRef {
        list(
            Arc::new(Float64Array::from(values.to_vec())),
            vec![0, values.len() as i32],
            None,
        )
    }
    fn evaluate(name: &str, left: ArrayRef, right: ArrayRef) -> Result<ArrayRef, String> {
        let types = [left.data_type().clone(), right.data_type().clone()];
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("left", types[0].clone(), true),
                Field::new("right", types[1].clone(), true),
            ])),
            vec![left, right],
        )
        .unwrap();
        let schema = ChunkSchema::try_ref_from_schema_and_slot_ids(
            batch.schema().as_ref(),
            &[SlotId::new(1), SlotId::new(2)],
        )
        .unwrap();
        let chunk = Chunk::new_with_chunk_schema(batch, schema);
        let mut arena = ExprArena::default();
        let left = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), types[0].clone());
        let right = arena.push_typed(ExprNode::SlotId(SlotId::new(2)), types[1].clone());
        // Aliases go through the same real lookup used to freeze the canonical
        // function kind; there is no fabricated alias metadata entry.
        let call = arena.push_typed(
            ExprNode::FunctionCall {
                kind: lookup_function(name).unwrap(),
                args: vec![left, right],
            },
            DataType::Float64,
        );
        let frozen = arena.into_immutable().unwrap();
        ExprArena::from_immutable(&frozen)
            .expect("legacy frozen expression fixture")
            .eval(call, &chunk)
    }
    fn assert_values(array: &ArrayRef, expected: &[Option<f64>]) {
        assert_eq!(array.data_type(), &DataType::Float64);
        let values = array.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(values.len(), expected.len());
        for (row, expected) in expected.iter().enumerate() {
            match expected {
                Some(expected) => {
                    assert!(!values.is_null(row));
                    assert!(
                        (values.value(row) - expected).abs() <= expected.abs().max(1.0) * 1e-14
                    );
                }
                None => assert!(values.is_null(row)),
            }
        }
    }
    #[test]
    fn vector_names_preserve_cosine_normalized_dot_and_squared_l2() {
        for (name, expected) in [
            ("cosine_similarity", 1.0),
            ("approx_cosine_similarity", 1.0),
            ("cosine_similarity_norm", 50.0),
            ("l2_distance", 25.0),
            ("approx_l2_distance", 25.0),
        ] {
            assert_values(
                &evaluate(
                    name,
                    vector(&[Some(3.0), Some(4.0)]),
                    vector(&[Some(6.0), Some(8.0)]),
                )
                .unwrap(),
                &[Some(expected)],
            );
        }
        assert_values(
            &evaluate(
                "cosine_similarity",
                vector(&[Some(0.0), Some(0.0)]),
                vector(&[Some(3.0), Some(4.0)]),
            )
            .unwrap(),
            &[Some(0.0)],
        );
        assert_values(
            &evaluate(
                "cosine_similarity_norm",
                vector(&[Some(1e16), Some(1.0), Some(-1e16)]),
                vector(&[Some(1.0); 3]),
            )
            .unwrap(),
            &[Some(0.0)],
        );
    }
    #[test]
    fn vector_final_nonfinite_values_are_successful_nulls() {
        for (name, left, right) in [
            (
                "cosine_similarity",
                vec![Some(f64::INFINITY)],
                vec![Some(1.0)],
            ),
            (
                "cosine_similarity_norm",
                vec![Some(f64::MAX)],
                vec![Some(2.0)],
            ),
            ("l2_distance", vec![Some(f64::MAX)], vec![Some(-f64::MAX)]),
            ("l2_distance", vec![Some(f64::NAN)], vec![Some(0.0)]),
        ] {
            assert_values(
                &evaluate(name, vector(&left), vector(&right)).unwrap(),
                &[None],
            );
        }
    }
    #[test]
    fn vector_required_outer_child_cast_null_empty_and_dimension_mismatch_are_errors() {
        let required_nulls: Vec<ArrayRef> = vec![
            list(
                Arc::new(Float64Array::from(vec![Some(1.0)])),
                vec![0, 1],
                Some(vec![false]),
            ),
            vector(&[None]),
            list(
                Arc::new(StringArray::from(vec!["invalid"])),
                vec![0, 1],
                None,
            ),
        ];
        for name in ["cosine_similarity", "cosine_similarity_norm", "l2_distance"] {
            for left in &required_nulls {
                let error = evaluate(name, left.clone(), vector(&[Some(1.0)])).unwrap_err();
                assert!(error.contains("does not support null values"), "{error}");
            }
            assert!(
                evaluate(name, vector(&[]), vector(&[]))
                    .unwrap_err()
                    .contains("requires non-empty arrays")
            );
            assert!(
                evaluate(name, vector(&[Some(1.0)]), vector(&[Some(1.0), Some(2.0)]))
                    .unwrap_err()
                    .contains("requires equal length arrays")
            );
        }
    }
    #[test]
    fn vector_old_whole_pool_cast_rejects_unreferenced_prefix_and_suffix_nulls() {
        let unused_nulls = list(
            Arc::new(Float64Array::from(vec![None, Some(3.0), Some(4.0), None])),
            vec![1, 3],
            None,
        );
        for name in ["cosine_similarity", "cosine_similarity_norm", "l2_distance"] {
            let error =
                evaluate(name, unused_nulls.clone(), vector(&[Some(0.0), Some(0.0)])).unwrap_err();
            assert!(error.contains("base array has null value"), "{error}");
        }
    }
    #[test]
    fn vector_fixed_size_list_child_parent_null_does_not_mask_raw_numeric_values() {
        let mask = Int8Array::from(vec![Some(0), None]);
        let child: ArrayRef = Arc::new(
            FixedSizeListArray::try_new(
                Arc::new(Field::new("scalar", DataType::Float64, true)),
                1,
                Arc::new(Float64Array::from(vec![Some(3.0), Some(4.0)])),
                mask.nulls().cloned(),
            )
            .unwrap(),
        );
        let casted = cast(&child, &DataType::Float64).unwrap();
        assert_values(&casted, &[Some(3.0), Some(4.0)]);
        assert_values(
            &evaluate(
                "l2_distance",
                list(child, vec![0, 2], None),
                vector(&[Some(0.0), Some(0.0)]),
            )
            .unwrap(),
            &[Some(25.0)],
        );
    }
    #[test]
    fn vector_sparse_union_nonchosen_fixed_list_tag_retains_shadow_numeric_values() {
        let child: ArrayRef = Arc::new(
            FixedSizeListArray::try_new(
                Arc::new(Field::new("scalar", DataType::Float64, true)),
                1,
                Arc::new(Float64Array::from(vec![Some(3.0), Some(4.0)])),
                None,
            )
            .unwrap(),
        );
        let union: ArrayRef = Arc::new(
            UnionArray::try_new(
                UnionFields::try_new(
                    [2, 7],
                    [
                        Field::new("scalar_list", child.data_type().clone(), true),
                        Field::new("binary", DataType::Binary, true),
                    ],
                )
                .unwrap(),
                vec![2_i8, 7].into(),
                None,
                vec![
                    child,
                    Arc::new(BinaryArray::from(vec![None, Some(b"ignored".as_slice())])),
                ],
            )
            .unwrap(),
        );
        // Sparse extraction adds a parent NULL to FSL1 for tag 7. Arrow's
        // FSL1->Float64 caster ignores that mask and reads its numeric shadow.
        let casted = cast(&union, &DataType::Float64).unwrap();
        assert_values(&casted, &[Some(3.0), Some(4.0)]);
        assert_values(
            &evaluate(
                "l2_distance",
                list(union, vec![0, 2], None),
                vector(&[Some(0.0), Some(0.0)]),
            )
            .unwrap(),
            &[Some(25.0)],
        );
    }
    #[test]
    fn vector_child_casts_preserve_real_uint_boolean_text_and_decimal_capabilities() {
        let children: Vec<ArrayRef> = vec![
            Arc::new(UInt32Array::from(vec![3, 4])),
            Arc::new(BooleanArray::from(vec![true, false])),
            Arc::new(StringArray::from(vec!["3", "4"])),
            Arc::new(
                Decimal128Array::from(vec![30, 40])
                    .with_precision_and_scale(38, 1)
                    .unwrap(),
            ),
        ];
        for (index, child) in children.into_iter().enumerate() {
            assert_values(
                &evaluate(
                    "l2_distance",
                    list(child, vec![0, 2], None),
                    vector(&[Some(0.0), Some(0.0)]),
                )
                .unwrap(),
                &[Some(if index == 1 { 1.0 } else { 25.0 })],
            );
        }
    }
    #[test]
    fn vector_dense_union_fixed_list_shadow_result_depends_on_actual_batch_split() {
        let child: ArrayRef = Arc::new(
            FixedSizeListArray::try_new(
                Arc::new(Field::new("scalar", DataType::Float64, true)),
                1,
                Arc::new(Float64Array::from(vec![Some(3.0), Some(4.0)])),
                None,
            )
            .unwrap(),
        );
        let union: ArrayRef = Arc::new(
            UnionArray::try_new(
                UnionFields::try_new(
                    [2, 7],
                    [
                        Field::new("scalar_list", child.data_type().clone(), true),
                        Field::new("binary", DataType::Binary, true),
                    ],
                )
                .unwrap(),
                vec![2_i8, 7].into(),
                Some(vec![0_i32, 0].into()),
                vec![
                    child,
                    Arc::new(BinaryArray::from(vec![Some(b"ignored".as_slice())])),
                ],
            )
            .unwrap(),
        );
        let whole = list(union.clone(), vec![0, 1, 2], None);
        let zeroes = list(
            Arc::new(Float64Array::from(vec![0.0, 0.0])),
            vec![0, 1, 2],
            None,
        );
        // Mixed tags force Arrow take with a NULL index. FSL's primitive
        // child then contains a real NULL, so the old whole-pool gate fails.
        assert!(
            evaluate("l2_distance", whole, zeroes)
                .unwrap_err()
                .contains("base array has null value")
        );
        // Rebuilding each batch with an actually sliced Union selects a
        // different Arrow branch. The second, wholly nonchosen tag gets a
        // FSL parent mask on target.slice(0,1); casting drops that mask and
        // reads the target's first shadow child, 3 rather than 4.
        for row in 0..2 {
            assert_values(
                &evaluate(
                    "l2_distance",
                    list(union.slice(row, 1), vec![0, 1], None),
                    vector(&[Some(0.0)]),
                )
                .unwrap(),
                &[Some(9.0)],
            );
        }
    }
}
