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

//! Locked Arrow pipeline oracles. These intentionally distinguish legacy
//! Arrow NULL behavior from ROUND's accepted Strict selected reader. They do
//! not register a new Arrow-mode runtime kernel or adjudicate that behavior.

use super::*;
use crate::builtin::binding_control;
use arrow_schema::{Field, UnionFields};
use novarocks_type_contract::CompilePhase;
use std::sync::{Arc, Mutex};

fn fsl(values: ArrayRef, valid: &[bool]) -> ArrayRef {
    Arc::new(FixedSizeListArray::new(
        Arc::new(Field::new("item", values.data_type().clone(), true)),
        1,
        values,
        Some(valid.to_vec().into()),
    ))
}
fn floats(values: &[Option<f64>]) -> ArrayRef {
    Arc::new(Float64Array::from(values.to_vec()))
}
fn dictionary(keys: &[Option<i8>], values: ArrayRef) -> ArrayRef {
    Arc::new(DictionaryArray::<Int8Type>::try_new(Int8Array::from(keys.to_vec()), values).unwrap())
}
fn runs(ends: &[i32], values: ArrayRef) -> ArrayRef {
    Arc::new(
        RunArray::<Int32Type>::try_new(&Int32Array::from(ends.to_vec()), values.as_ref()).unwrap(),
    )
}
fn union(tags: &[i8], offsets: Option<&[i32]>, selected: ArrayRef, other_len: usize) -> ArrayRef {
    let fields = UnionFields::try_new(
        [7, 1],
        [
            Field::new("selected", selected.data_type().clone(), true),
            Field::new("other", DataType::Binary, true),
        ],
    )
    .unwrap();
    let other: ArrayRef = Arc::new(BinaryArray::from(vec![Some(&b"x"[..]); other_len]));
    Arc::new(
        UnionArray::try_new(
            fields,
            tags.to_vec().into(),
            offsets.map(|o| o.to_vec().into()),
            vec![selected, other],
        )
        .unwrap(),
    )
}
fn arrow_values(array: &ArrayRef) -> Vec<Option<f64>> {
    let output = arrow_cast::cast(array.as_ref(), &DataType::Float64).unwrap();
    let output = output.as_any().downcast_ref::<Float64Array>().unwrap();
    output.iter().collect()
}
fn recipe(array: &ArrayRef) -> CastRecipe {
    let source =
        FunctionValueType::from_legacy_declared_projection(array.data_type().clone(), true);
    binding_control::scope(crate::binding_test_control(), |work| {
        binding_control::value_type(&source, work)?;
        // Keep actual encoding separate from complete authored semantics.
        CastRecipe::prepare_typed(
            array.data_type(),
            source.value_type(),
            CastTarget::Float64,
            work,
        )
    })
    .unwrap()
}
fn strict_values(array: &ArrayRef) -> Vec<Option<f64>> {
    struct Control;
    impl crate::KernelEvaluationControl for Control {
        fn checkpoint(&self, _: u32) -> Result<(), KernelFailure> {
            Ok(())
        }
        fn wait(&self, _: std::time::Duration) -> Result<(), KernelFailure> {
            panic!("cast cannot wait")
        }
    }
    let recipe = recipe(array);
    let mut work = EvaluationCheckpoints::new(&Control);
    let values = (0..array.len())
        .map(
            |row| match recipe.read(array.as_ref(), row, &mut work).unwrap() {
                CastValue::Null => None,
                CastValue::Float(value) => Some(value),
                _ => panic!("wrong Float64 recipe"),
            },
        )
        .collect();
    work.finish().unwrap();
    values
}

#[test]
fn original_arrow_singleton_list_cast_ignores_parent_null_and_retains_child_null() {
    let array = fsl(
        floats(&[Some(3.0), None, Some(-0.0)]),
        &[false, true, false],
    );
    let actual = arrow_values(&array);
    assert_eq!(actual, [Some(3.0), None, Some(-0.0)]);
    assert_eq!(actual[2].unwrap().to_bits(), (-0.0_f64).to_bits());
    assert_eq!(strict_values(&array), [None, None, None]);
    let slice = array.slice(2, 1);
    assert_eq!(
        arrow_values(&slice)[0].unwrap().to_bits(),
        (-0.0_f64).to_bits()
    );
}

#[test]
fn dictionary_casts_values_before_taking_keys_while_null_keys_stay_null() {
    let values = fsl(floats(&[Some(3.0), Some(4.0)]), &[false, true]);
    let array = dictionary(&[Some(0), None, Some(1), Some(0)], values);
    assert_eq!(
        arrow_values(&array),
        [Some(3.0), None, Some(4.0), Some(3.0)]
    );
    assert_eq!(strict_values(&array), [None, None, Some(4.0), None]);
    assert_eq!(arrow_values(&array.slice(1, 2)), [None, Some(4.0)]);
}

#[test]
fn singleton_list_before_dictionary_drops_only_list_null_and_keeps_key_null() {
    let values = dictionary(&[Some(0), None, Some(1)], floats(&[Some(3.0), Some(4.0)]));
    let array = fsl(values, &[false, true, false]);
    assert_eq!(arrow_values(&array), [Some(3.0), None, Some(4.0)]);
    assert_eq!(strict_values(&array), [None, None, None]);
}

#[test]
fn run_cast_takes_logical_values_before_cast_and_retains_sliced_run_offsets() {
    let values = fsl(floats(&[Some(3.0), Some(4.0), None]), &[false, true, true]);
    let array = runs(&[2, 4, 5], values);
    assert_eq!(
        arrow_values(&array),
        [Some(3.0), Some(3.0), Some(4.0), Some(4.0), None]
    );
    assert_eq!(
        arrow_values(&array.slice(1, 3)),
        [Some(3.0), Some(4.0), Some(4.0)]
    );
    assert_eq!(
        strict_values(&array),
        [None, None, Some(4.0), Some(4.0), None]
    );
    let nullable = runs(&[2, 4], floats(&[None, Some(7.0)]));
    assert_eq!(arrow_values(&nullable), [None, None, Some(7.0), Some(7.0)]);
}

#[test]
fn sparse_union_mask_on_list_parent_does_not_mask_selected_child_shadow_rows() {
    let selected = fsl(floats(&[Some(3.0), Some(4.0)]), &[true, true]);
    let array = union(&[7, 1], None, selected, 2);
    assert_eq!(arrow_values(&array), [Some(3.0), Some(4.0)]);
    assert_eq!(strict_values(&array), [Some(3.0), None]);
    let all_other = array.slice(1, 1);
    assert_eq!(arrow_values(&all_other), [Some(4.0)]);
}

#[test]
fn dense_union_list_cast_changes_on_batch_partition_due_to_extract_fast_paths() {
    let selected = fsl(floats(&[Some(3.0)]), &[true]);
    let array = union(&[7, 1], Some(&[0, 0]), selected, 1);
    assert_eq!(arrow_values(&array), [Some(3.0), None]);
    // A mixed parent uses nullable take, giving the child a NULL. Slicing to
    // all-other with a target of equal length overlays only the parent mask.
    assert_eq!(arrow_values(&array.slice(1, 1)), [Some(3.0)]);
    assert_eq!(strict_values(&array.slice(1, 1)), [None]);
}

#[test]
fn dense_union_all_null_list_parent_fast_path_ignores_dense_offsets() {
    let selected = fsl(floats(&[Some(3.0), Some(4.0)]), &[false, false]);
    let array = union(&[7, 1], Some(&[1, 0]), selected, 1);
    // target.null_count == target.len returns the original child when its
    // length equals the union length. FSL casting subsequently drops bitmap.
    assert_eq!(arrow_values(&array), [Some(3.0), Some(4.0)]);
    assert_eq!(strict_values(&array), [None, None]);
    let selected = fsl(floats(&[Some(3.0)]), &[true]);
    let none = union(&[1, 1], Some(&[0, 1]), selected, 2);
    assert_eq!(arrow_values(&none), [None, None]);
}

#[test]
fn sparse_union_masks_dictionary_keys_but_list_before_dictionary_drops_that_mask() {
    let selected = dictionary(
        &[Some(0), Some(1)],
        fsl(floats(&[Some(3.0), Some(4.0)]), &[true, true]),
    );
    assert_eq!(
        arrow_values(&union(&[7, 1], None, selected, 2)),
        [Some(3.0), None]
    );
    let selected = fsl(
        dictionary(&[Some(0), Some(1)], floats(&[Some(3.0), Some(4.0)])),
        &[true, true],
    );
    assert_eq!(
        arrow_values(&union(&[7, 1], None, selected, 2)),
        [Some(3.0), Some(4.0)]
    );
}

#[test]
fn dense_union_nullable_take_into_runs_ignores_hidden_index_under_null_tag() {
    let selected = runs(&[1, 2], floats(&[Some(3.0), Some(4.0)]));
    let array = union(&[7, 1], Some(&[0, 1]), selected, 2);
    // Arrow 60.1 take_run respects the nullable index created by the tag
    // mask. The inactive run's physical value is not the selected value.
    assert_eq!(arrow_values(&array), [Some(3.0), None]);
    assert_eq!(strict_values(&array), [Some(3.0), None]);
}

#[test]
fn taking_run_values_before_nested_union_cast_changes_child_population() {
    let child = union(
        &[7, 1],
        Some(&[0, 0]),
        fsl(floats(&[Some(3.0)]), &[true]),
        1,
    );
    let array = runs(&[1, 3], child);
    assert_eq!(arrow_values(&array), [Some(3.0), None, None]);
    // Sliced REE expands an all-other union domain before the union cast.
    // Its chosen FSL child becomes empty, so the new-null branch is used.
    assert_eq!(arrow_values(&array.slice(1, 2)), [None, None]);
    assert_eq!(arrow_values(&array.slice(1, 1)), [None]);
}

#[test]
fn borrowed_prepare_type_preserves_exact_recipe_and_checkpoint_trace_without_clone() {
    let array = dictionary(&[Some(0)], fsl(floats(&[Some(3.0)]), &[true]));
    let source =
        FunctionValueType::from_legacy_declared_projection(array.data_type().clone(), true);
    struct Trace(Mutex<Vec<u32>>);
    impl novarocks_type_contract::PureCompileControl for Trace {
        fn checkpoint(
            &self,
            _: CompilePhase,
            work: u32,
        ) -> Result<(), novarocks_type_contract::CompileControlError> {
            self.0.lock().unwrap().push(work);
            Ok(())
        }
    }
    let first = Trace(Mutex::new(Vec::new()));
    let second = Trace(Mutex::new(Vec::new()));
    let a = binding_control::scope(&first, |work| {
        CastRecipe::prepare(&source, CastTarget::Float64, work)
    })
    .unwrap();
    let b = binding_control::scope(&second, |work| {
        CastRecipe::prepare_type(
            source.data_type(),
            source.logical_type(),
            CastTarget::Float64,
            work,
        )
    })
    .unwrap();
    assert_eq!(format!("{a:?}"), format!("{b:?}"));
    assert_eq!(*first.0.lock().unwrap(), *second.0.lock().unwrap());
    assert_eq!(std::mem::size_of_val(&a), std::mem::size_of_val(&b));
    assert!(
        binding_control::scope(crate::binding_test_control(), |work| {
            CastRecipe::prepare_type(
                &DataType::FixedSizeBinary(16),
                ValueLogicalType::LargeInt,
                CastTarget::Float64,
                work,
            )
        })
        .is_err()
    );
}

#[test]
fn borrowed_prepare_preserves_original_control_at_entry_quantum_and_tail() {
    use novarocks_type_contract::{CompileControlError, PureCompileControl};
    struct Refusal {
        calls: Mutex<Vec<u32>>,
        refusal: Option<(usize, CompileControlError)>,
    }
    impl PureCompileControl for Refusal {
        fn checkpoint(&self, _: CompilePhase, units: u32) -> Result<(), CompileControlError> {
            assert!(units <= novarocks_type_contract::MAX_UNOBSERVED_COMPILE_WORK);
            let mut calls = self.calls.lock().unwrap();
            let at = calls.len();
            calls.push(units);
            if let Some((when, error)) = self.refusal.clone()
                && at == when
            {
                return Err(error);
            }
            Ok(())
        }
    }
    // There are 128 valid Union tags. Both unsuccessful exact/family passes
    // are genuine owner work before the third pass picks the first FSL child.
    let chosen = DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float64, true)), 1);
    let fields = UnionFields::try_new(
        0_i8..=127,
        (0..128).map(|id| {
            Field::new(
                format!("field-{id}"),
                if id == 0 {
                    chosen.clone()
                } else {
                    DataType::Binary
                },
                true,
            )
        }),
    )
    .unwrap();
    let source = FunctionValueType::from_legacy_declared_projection(
        DataType::Union(fields, arrow_schema::UnionMode::Sparse),
        true,
    );
    let drive = |control: &Refusal| {
        binding_control::scope(control, |work| {
            binding_control::value_type(&source, work)?;
            CastRecipe::prepare_type(
                source.data_type(),
                source.logical_type(),
                CastTarget::Float64,
                work,
            )
        })
    };
    let baseline = Refusal {
        calls: Mutex::new(Vec::new()),
        refusal: None,
    };
    drive(&baseline).unwrap();
    let calls = baseline.calls.lock().unwrap().clone();
    assert!(calls.contains(&256));
    assert!(calls.last().is_some_and(|units| *units > 0 && *units < 256));
    for error in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        for (at, _) in calls.iter().enumerate() {
            let control = Refusal {
                calls: Mutex::new(Vec::new()),
                refusal: Some((at, error.clone())),
            };
            assert_eq!(
                drive(&control).unwrap_err().control_error(),
                Some(error.clone())
            );
            assert_eq!(control.calls.lock().unwrap().len(), at + 1);
        }
    }
}
