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

use super::{validate_argument_observed, visit_selected_nulls};
use crate::{
    ConstantPolicy, ConstantPool, EvaluatedArgument, FunctionValueType, KernelDiagnostic,
    KernelEvaluationControl, KernelFailure, RowDataError, SelectedValues, Selection,
};
use arrow_array::types::{Int8Type, Int16Type};
use arrow_array::{
    Array, ArrayRef, DictionaryArray, Int8Array, Int16Array, Int64Array, RunArray, StringArray,
    UnionArray,
};
use arrow_schema::{DataType, Field, UnionFields};
use novarocks_type_contract::{CompileControlError, CompilePhase, PureCompileControl};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

#[derive(Default)]
struct Control {
    checks: Mutex<Vec<u32>>,
    refusal: Option<(usize, KernelFailure)>,
}
impl KernelEvaluationControl for Control {
    fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
        assert!(units <= crate::MAX_UNOBSERVED_KERNEL_WORK);
        let mut checks = self.checks.lock().unwrap();
        let index = checks.len();
        checks.push(units);
        if let Some((at, failure)) = &self.refusal
            && index == *at
        {
            return Err(failure.clone());
        }
        Ok(())
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("NULL inspection must not wait");
    }
}
struct ConstructionControl;
impl PureCompileControl for ConstructionControl {
    fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
        Ok(())
    }
}
fn refusals() -> [KernelFailure; 8] {
    [
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
        KernelFailure::InvalidProgram(KernelDiagnostic::new("original invalid-program refusal")),
        KernelFailure::Internal(KernelDiagnostic::new("original internal refusal")),
        KernelFailure::Operational(KernelDiagnostic::new("original operational refusal")),
        KernelFailure::InstanceFailed,
        KernelFailure::from_domain(
            novarocks_type_contract::owned_resources::layout::LayoutResourceError::SourceModel,
        ),
    ]
}

#[test]
fn public_provider_work_scope_preserves_quantum_and_every_originating_refusal() {
    use crate::{EvaluationCheckpoints, KernelControlObservation};
    let success = Control::default();
    let observer = KernelControlObservation::new(&success);
    let mut work = EvaluationCheckpoints::new(&observer);
    for _ in 0..513 {
        work.step().unwrap();
    }
    assert_eq!(work.finish_result(Ok(17)), Ok(17));
    assert_eq!(*success.checks.lock().unwrap(), [256, 256, 1]);

    for failure in refusals() {
        for at in 0..3 {
            let control = Control {
                refusal: Some((at, failure.clone())),
                ..Default::default()
            };
            let observer = KernelControlObservation::new(&control);
            let mut work = EvaluationCheckpoints::new(&observer);
            let outcome = (|| {
                for _ in 0..513 {
                    work.step()?;
                }
                Ok(17)
            })();
            assert_eq!(work.finish_result(outcome), Err(failure.clone()));
            let expected = [256, 256, 1];
            assert_eq!(*control.checks.lock().unwrap(), expected[..=at]);
            assert_eq!(observer.checkpoint(0), Err(failure.clone()));
            assert_eq!(observer.finish(Ok(99)), Err(failure.clone()));
            assert_eq!(control.checks.lock().unwrap().len(), at + 1);
        }

        // A nested ABI observes the same original controller. An ordinary body
        // error cannot override its diagnostic refusal or issue a new tail.
        let nested = Control {
            refusal: Some((0, failure.clone())),
            ..Default::default()
        };
        let observer = KernelControlObservation::new(&nested);
        let mut work = EvaluationCheckpoints::new(&observer);
        work.step().unwrap();
        assert_eq!(observer.checkpoint(0), Err(failure.clone()));
        let body: Result<(), KernelFailure> = Err(KernelFailure::Internal(KernelDiagnostic::new(
            "ordinary body failure",
        )));
        // The actual enclosing observation owner, rather than a new
        // checkpoint, retains the refusal it observed before this body error.
        assert_eq!(observer.finish(work.finish_result(body)), Err(failure));
        assert_eq!(*nested.checks.lock().unwrap(), [0]);
    }

    let ordinary = Control::default();
    let mut work = EvaluationCheckpoints::new(&ordinary);
    work.step().unwrap();
    let body: Result<(), KernelFailure> = Err(KernelFailure::Operational(KernelDiagnostic::new(
        "ordinary lifecycle data failure",
    )));
    assert_eq!(work.finish_result(body.clone()), body);
    assert!(ordinary.checks.lock().unwrap().is_empty());

    for failure in refusals() {
        let control = Control {
            refusal: Some((
                0,
                KernelFailure::Internal(KernelDiagnostic::new("later tail")),
            )),
            ..Default::default()
        };
        let mut work = EvaluationCheckpoints::new(&control);
        work.step().unwrap();
        let result: Result<(), KernelFailure> = Err(failure.clone());
        assert_eq!(work.finish_result(result), Err(failure));
        assert!(control.checks.lock().unwrap().is_empty());
    }
}

fn pool(array: ArrayRef) -> ConstantPool {
    let ty = FunctionValueType::from_legacy_declared_projection(array.data_type().clone(), true);
    ConstantPool::try_new(
        Arc::new(
            ty.try_to_field("selected-null-input")
                .unwrap()
                .with_data_type(array.data_type().clone()),
        ),
        ty,
        array.to_data(),
        ConstantPolicy {
            max_rows: 32,
            max_array_nodes: 512,
            max_logical_elements: 4096,
            max_retained_buffer_bytes: 8 * 1024 * 1024,
            max_type_depth: 64,
            max_type_nodes: 4096,
            max_dictionary_depth: 8,
            max_metadata_bytes: 4 * 1024 * 1024,
            max_library_validation_work: 128 * 1024 * 1024,
            max_library_validation_bytes: 64 * 1024 * 1024,
        },
        CompilePhase::Validate,
        &ConstructionControl,
    )
    .unwrap()
}
fn inspect(argument: EvaluatedArgument<'_>, selection: Selection<'_>) -> Vec<(usize, usize, bool)> {
    let mut rows = Vec::new();
    visit_selected_nulls(
        argument,
        selection,
        &Control::default(),
        |ordinal, row, null| {
            rows.push((ordinal, row, null));
            Ok(())
        },
    )
    .unwrap();
    rows
}

#[test]
fn constant_inspection_broadcasts_exact_nonzero_pool_ordinal() {
    let p = pool(Arc::new(Int64Array::from(vec![
        None,
        Some(41),
        None,
        Some(99),
    ])));
    let selection = Selection::try_sparse(100, &[2, 51, 99]).unwrap();
    let nonnull = p.value(1).unwrap();
    let null = p.value(2).unwrap();
    assert_eq!(
        inspect(EvaluatedArgument::Constant(&nonnull), selection),
        [(0, 2, false), (1, 51, false), (2, 99, false)]
    );
    assert_eq!(
        inspect(EvaluatedArgument::Constant(&null), selection),
        [(0, 2, true), (1, 51, true), (2, 99, true)]
    );
    assert!(Arc::ptr_eq(nonnull.pool().array(), p.array()));
}

#[test]
fn dictionary_inspection_uses_selected_keys_and_not_unselected_pool_nulls() {
    let dictionary: ArrayRef = Arc::new(
        DictionaryArray::<Int8Type>::try_new(
            Int8Array::from(vec![Some(1), Some(0), None, Some(1)]),
            Arc::new(StringArray::from(vec![
                Some("present"),
                None,
                Some("unused"),
            ])),
        )
        .unwrap(),
    );
    let selection = Selection::try_sparse(4, &[1, 2, 3]).unwrap();
    assert_eq!(
        inspect(EvaluatedArgument::Column(&dictionary), selection),
        [(0, 1, false), (1, 2, true), (2, 3, true)]
    );
    // The first pool row is NULL, while the constant ordinal selects key 0.
    let p = pool(dictionary);
    let value = p.value(1).unwrap();
    assert_eq!(
        inspect(EvaluatedArgument::Constant(&value), Selection::all(2)),
        [(0, 0, false), (1, 1, false)]
    );
}

#[test]
fn union_inspection_uses_actual_dense_tags_offsets_and_slice() {
    let union = UnionArray::try_new(
        UnionFields::try_new(
            [7, 2],
            [
                Field::new("text", DataType::Utf8, true),
                Field::new("number", DataType::Int64, true),
            ],
        )
        .unwrap(),
        vec![7_i8, 2, 7, 2].into(),
        Some(vec![0_i32, 1, 1, 0].into()),
        vec![
            Arc::new(StringArray::from(vec![Some("unused"), None])),
            Arc::new(Int64Array::from(vec![None, Some(42)])),
        ],
    )
    .unwrap();
    let sliced: ArrayRef = Arc::new(union.slice(1, 3));
    assert_eq!(
        inspect(EvaluatedArgument::Column(&sliced), Selection::all(3)),
        [(0, 0, false), (1, 1, true), (2, 2, true)]
    );
}

#[test]
fn run_end_inspection_uses_logical_offsets_of_sliced_runs() {
    let runs = RunArray::<Int16Type>::try_new(
        &Int16Array::from(vec![2, 5, 7]),
        &Int64Array::from(vec![Some(11), None, Some(77)]),
    )
    .unwrap();
    let sliced: ArrayRef = Arc::new(runs.slice(1, 5));
    assert_eq!(
        inspect(
            EvaluatedArgument::Column(&sliced),
            Selection::try_sparse(5, &[0, 2, 4]).unwrap()
        ),
        [(0, 0, false), (1, 2, true), (2, 4, false)]
    );
}

#[test]
fn selected_error_nulls_can_be_inspected_but_cannot_enter_ordinary_kernel_arguments() {
    let rows = [1, 4, 8];
    let selection = Selection::try_sparse(10, &rows).unwrap();
    let values: ArrayRef = Arc::new(Int64Array::from(vec![Some(3), None, Some(9)]));
    let output = SelectedValues::try_new(
        selection,
        &DataType::Int64,
        values,
        vec![RowDataError::new(1, "required child failed")].into_boxed_slice(),
    )
    .unwrap();
    let argument = EvaluatedArgument::SelectedColumn(&output);
    assert_eq!(
        inspect(argument, selection),
        [(0, 1, false), (1, 4, true), (2, 8, false)]
    );
    assert!(matches!(
        validate_argument_observed(
            argument,
            selection,
            &FunctionValueType::new(DataType::Int64, true),
            &Control::default()
        ),
        Err(ref cause) if crate::kernel_control::test_contract::input(cause)
    ));
    let wrong = Selection::try_sparse(10, &[1, 5, 8]).unwrap();
    let mut visits = 0;
    assert!(matches!(
        visit_selected_nulls(argument, wrong, &Control::default(), |_, _, _| {
            visits += 1;
            Ok(())
        }),
        Err(ref cause) if crate::kernel_control::test_contract::input(cause)
    ));
    assert_eq!(visits, 0);
}

#[test]
fn callback_refusal_is_primary_and_has_no_later_visit_or_checkpoint() {
    let values: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), None, Some(3)]));
    for failure in refusals() {
        let control = Control::default();
        let mut visits = Vec::new();
        let result = visit_selected_nulls(
            EvaluatedArgument::Column(&values),
            Selection::all(3),
            &control,
            |ordinal, _, null| {
                visits.push((ordinal, null));
                if ordinal == 1 {
                    Err(failure.clone())
                } else {
                    Ok(())
                }
            },
        );
        assert_eq!(result, Err(failure));
        assert_eq!(visits, [(0, false), (1, true)]);
        assert_eq!(*control.checks.lock().unwrap(), [0]);
    }
}

#[test]
fn original_controls_refuse_entry_quantum_and_completed_tail_without_retry() {
    let values: ArrayRef = Arc::new(Int64Array::from(vec![Some(1); 257]));
    let baseline = Control::default();
    visit_selected_nulls(
        EvaluatedArgument::Column(&values),
        Selection::all(257),
        &baseline,
        |_, _, _| Ok(()),
    )
    .unwrap();
    let trace = [0, 256, 256, 2];
    assert_eq!(*baseline.checks.lock().unwrap(), trace);
    for failure in refusals() {
        for (check, expected_visits) in [(0, 0), (1, 128), (2, 256), (3, 257)] {
            let control = Control {
                checks: Mutex::default(),
                refusal: Some((check, failure.clone())),
            };
            let mut visits = 0;
            let result = visit_selected_nulls(
                EvaluatedArgument::Column(&values),
                Selection::all(257),
                &control,
                |_, _, _| {
                    visits += 1;
                    Ok(())
                },
            );
            assert_eq!(result, Err(failure.clone()));
            assert_eq!(visits, expected_visits);
            assert_eq!(*control.checks.lock().unwrap(), trace[..=check]);
        }
    }
}

#[test]
fn ordinary_shape_rejection_is_primary_without_a_comparison_footer() {
    let selection = Selection::try_sparse(10, &[1, 4, 8]).unwrap();
    let wrong = Selection::try_sparse(10, &[1, 5, 8]).unwrap();
    let values: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3]));
    let output =
        SelectedValues::try_new(selection, &DataType::Int64, values, Box::default()).unwrap();
    let baseline = Control::default();
    let mut visits = 0;
    assert!(matches!(
        visit_selected_nulls(
            EvaluatedArgument::SelectedColumn(&output),
            wrong,
            &baseline,
            |_, _, _| {
                visits += 1;
                Ok(())
            }
        ),
        Err(KernelFailure::InvalidProgram(_))
    ));
    assert_eq!(visits, 0);
    assert_eq!(*baseline.checks.lock().unwrap(), [0]);
    for refusal in refusals() {
        let control = Control {
            checks: Mutex::default(),
            refusal: Some((1, refusal.clone())),
        };
        let result = visit_selected_nulls(
            EvaluatedArgument::SelectedColumn(&output),
            wrong,
            &control,
            |_, _, _| panic!("shape rejection must precede any visitor"),
        );
        assert!(matches!(result, Err(KernelFailure::InvalidProgram(_))));
        assert_eq!(*control.checks.lock().unwrap(), [0]);
    }
}

#[test]
fn ordinary_address_rejection_preserves_the_typed_source_without_a_footer() {
    // Valid Arrow dictionaries can exceed the kernel's checked addressing
    // depth. Preserve that original domain's refusal before a later callback.
    let mut values: ArrayRef = Arc::new(Int64Array::from(vec![1]));
    for _ in 0..novarocks_type_contract::MAX_VALUE_TYPE_DEPTH {
        values = Arc::new(
            DictionaryArray::<Int8Type>::try_new(Int8Array::from(vec![0]), values).unwrap(),
        );
    }
    let baseline = Control::default();
    let failure = visit_selected_nulls(
        EvaluatedArgument::Column(&values),
        Selection::all(1),
        &baseline,
        |_, _, _| panic!("invalid depth cannot reach visitor"),
    )
    .unwrap_err();
    use novarocks_failure_contract::{FailureClass, FailureProjection};
    let fact = failure.failure_fact();
    assert_eq!(fact.class(), FailureClass::Invariant);
    assert_eq!(
        fact.reason().domain(),
        novarocks_failure_contract::domain::KERNEL.get()
    );
    assert_eq!(fact.reason().code(), 160);
    let KernelFailure::Source(source) = failure else {
        panic!("expected original addressing cause")
    };
    assert_eq!(
        std::error::Error::source(&source)
            .unwrap()
            .downcast_ref::<crate::logical_null::NullAddressViolation>(),
        Some(&crate::logical_null::NullAddressViolation::NestedAddress)
    );
    assert_eq!(*baseline.checks.lock().unwrap(), [0]);
    for refusal in refusals() {
        let control = Control {
            checks: Mutex::default(),
            refusal: Some((1, refusal.clone())),
        };
        let failure = visit_selected_nulls(
            EvaluatedArgument::Column(&values),
            Selection::all(1),
            &control,
            |_, _, _| panic!("invalid depth cannot reach visitor"),
        )
        .unwrap_err();
        assert_eq!(failure.failure_fact(), fact);
        let KernelFailure::Source(source) = failure else {
            panic!("expected original addressing cause")
        };
        assert_eq!(
            std::error::Error::source(&source)
                .unwrap()
                .downcast_ref::<crate::logical_null::NullAddressViolation>(),
            Some(&crate::logical_null::NullAddressViolation::NestedAddress)
        );
        assert_eq!(*control.checks.lock().unwrap(), [0]);
    }
}

#[test]
fn ordinary_length_rejection_is_primary_without_a_zero_work_footer() {
    let values: ArrayRef = Arc::new(Int64Array::from(vec![1]));
    let baseline = Control::default();
    assert!(
        matches!(visit_selected_nulls(EvaluatedArgument::Column(&values), Selection::all(2), &baseline, |_, _, _| panic!("invalid length cannot reach visitor")), Err(ref cause) if crate::kernel_control::test_contract::input(cause))
    );
    assert_eq!(*baseline.checks.lock().unwrap(), [0]);
    let control = Control {
        checks: Mutex::default(),
        refusal: Some((1, KernelFailure::DeadlineExceeded)),
    };
    assert!(
        matches!(visit_selected_nulls(EvaluatedArgument::Column(&values), Selection::all(2), &control, |_, _, _| panic!("invalid length cannot reach visitor")), Err(ref cause) if crate::kernel_control::test_contract::input(cause))
    );
    assert_eq!(*control.checks.lock().unwrap(), [0]);
}

#[test]
fn bounded_row_projection_shares_null_traversal_observation_instead_of_a_parallel_counter() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    #[derive(Default)]
    struct ProjectionControl {
        visited: AtomicUsize,
        positions: Mutex<Vec<usize>>,
    }
    impl KernelEvaluationControl for ProjectionControl {
        fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
            assert!(units <= crate::MAX_UNOBSERVED_KERNEL_WORK);
            self.positions
                .lock()
                .unwrap()
                .push(self.visited.load(Ordering::Relaxed));
            Ok(())
        }
        fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
            panic!("NULL projection must not wait");
        }
    }
    let array: ArrayRef = Arc::new(Int64Array::from(vec![Some(17); 320]));
    let control = ProjectionControl::default();
    visit_selected_nulls(
        EvaluatedArgument::Column(&array),
        Selection::all(320),
        &control,
        |_, _, null| {
            assert!(!null);
            control.visited.fetch_add(1, Ordering::Relaxed);
            Ok(())
        },
    )
    .unwrap();
    assert_eq!(control.visited.load(Ordering::Relaxed), 320);
    let mut previous = 0;
    for &position in control.positions.lock().unwrap().iter() {
        // Each shallow source visit and its completed projection share the
        // 256-unit limit. A separate 256-row counter would exceed this gap.
        assert!(position - previous <= crate::MAX_UNOBSERVED_KERNEL_WORK as usize / 2);
        previous = position;
    }
    assert_eq!(previous, 320);
}

#[test]
fn runtime_map64_argument_keeps_selected_values_original_nulls_and_control() {
    use arrow_array::{Int32Array, MapArray, StructArray};
    use arrow_buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
    use novarocks_failure_contract::FailureProjection;
    fn maps(mut values: ArrayRef) -> ArrayRef {
        for _ in 1..64 {
            let fields = vec![
                Arc::new(Field::new("key", DataType::Int32, false)),
                Arc::new(Field::new("value", values.data_type().clone(), false)),
            ]
            .into();
            let entries = StructArray::new(
                fields,
                vec![Arc::new(Int32Array::from(vec![1, 2])), values],
                None,
            );
            values = Arc::new(MapArray::new(
                Arc::new(Field::new("entries", entries.data_type().clone(), false)),
                OffsetBuffer::new(ScalarBuffer::from(vec![0_i32, 1, 2])),
                entries,
                None,
                false,
            ));
        }
        values
    }
    let values = maps(Arc::new(Int32Array::from(vec![7, 11])));
    let value_type = FunctionValueType::new(values.data_type().clone(), false);
    assert_eq!(value_type.value_type().depth(), 64);
    // Preserve the raw producer's original failure identity; only this typed
    // runtime seam chooses the value profile after the original constant branch.
    let raw = novarocks_type_contract::arrow_data_types_exact_observed::<
        novarocks_type_contract::ValueTypeError,
    >(values.data_type(), values.data_type(), || Ok(()))
    .unwrap_err();
    assert_eq!(
        raw.failure_fact(),
        novarocks_type_contract::ValueTypeError::TooDeep.failure_fact()
    );
    let control = Control::default();
    validate_argument_observed(
        EvaluatedArgument::Column(&values),
        Selection::all(2),
        &value_type,
        &control,
    )
    .unwrap();
    let trace = control.checks.lock().unwrap().clone();
    assert!(trace.iter().any(|&units| units > 0));
    for at in 0..trace.len() {
        for failure in [
            KernelFailure::Cancelled,
            KernelFailure::DeadlineExceeded,
            KernelFailure::ResourceExhausted,
        ] {
            let refusing = Control {
                refusal: Some((at, failure.clone())),
                ..Default::default()
            };
            assert_eq!(
                validate_argument_observed(
                    EvaluatedArgument::Column(&values),
                    Selection::all(2),
                    &value_type,
                    &refusing
                ),
                Err(failure)
            );
            assert_eq!(*refusing.checks.lock().unwrap(), trace[..=at]);
        }
    }
    let other = maps(Arc::new(Int64Array::from(vec![7, 11])));
    let other_type = FunctionValueType::new(other.data_type().clone(), false);
    assert!(matches!(
        validate_argument_observed(
            EvaluatedArgument::Column(&values),
            Selection::all(2),
            &other_type,
            &Control::default()
        ),
        Err(KernelFailure::InvalidProgram(_))
    ));
    let map = values.as_any().downcast_ref::<MapArray>().unwrap().clone();
    let (field, offsets, entries, _, ordered) = map.into_parts();
    let nullable: ArrayRef = Arc::new(MapArray::new(
        field,
        offsets,
        entries,
        Some(NullBuffer::from(vec![true, false])),
        ordered,
    ));
    assert!(matches!(
        validate_argument_observed(
            EvaluatedArgument::Column(&nullable),
            Selection::all(2),
            &value_type,
            &Control::default()
        ),
        Err(KernelFailure::InvalidProgram(_))
    ));
    validate_argument_observed(
        EvaluatedArgument::Column(&nullable),
        Selection::try_sparse(2, &[0]).unwrap(),
        &value_type,
        &Control::default(),
    )
    .unwrap();
}

#[test]
fn permissive_runtime_struct_preserves_active_requiredness_and_selected_row_mapping() {
    use arrow_array::{Int32Array, StructArray};
    use arrow_buffer::NullBuffer;
    use novarocks_failure_contract::FailureProjection;
    let logical = FunctionValueType::new(
        DataType::Struct(vec![Arc::new(Field::new("n", DataType::Int32, false))].into()),
        true,
    );
    let array = |values: Vec<Option<i32>>, parents: Vec<bool>| -> ArrayRef {
        Arc::new(StructArray::new(
            vec![Arc::new(Field::new("n", DataType::Int32, true))].into(),
            vec![Arc::new(Int32Array::from(values))],
            Some(NullBuffer::from(parents)),
        ))
    };
    let valid = array(vec![None, Some(7)], vec![false, true]);
    validate_argument_observed(
        EvaluatedArgument::Column(&valid),
        Selection::all(2),
        &logical,
        &Control::default(),
    )
    .unwrap();
    let DataType::Struct(fields) = valid.data_type() else {
        panic!("original Struct carrier")
    };
    assert!(
        fields[0].is_nullable(),
        "validation must not tighten the original Arrow carrier"
    );
    let invalid = array(vec![None, None], vec![false, true]);
    let error = validate_argument_observed(
        EvaluatedArgument::Column(&invalid),
        Selection::all(2),
        &logical,
        &Control::default(),
    )
    .unwrap_err();
    assert_eq!(
        error.failure_fact(),
        novarocks_value_arrays::ValueError::Invariant(
            novarocks_value_arrays::ValueViolation::RequiredNull
        )
        .failure_fact()
    );
    validate_argument_observed(
        EvaluatedArgument::Column(&invalid),
        Selection::try_sparse(2, &[0]).unwrap(),
        &logical,
        &Control::default(),
    )
    .unwrap();
    // Compact selected values keep original batch coordinates in Selection, but
    // the borrowed requiredness author must receive compact physical ordinals.
    let selected = Selection::try_sparse(6, &[2, 5]).unwrap();
    let compact =
        SelectedValues::try_new(selected, valid.data_type(), valid.clone(), Box::default())
            .unwrap();
    validate_argument_observed(
        EvaluatedArgument::SelectedColumn(&compact),
        selected,
        &logical,
        &Control::default(),
    )
    .unwrap();
    let renamed = FunctionValueType::new(
        DataType::Struct(vec![Arc::new(Field::new("different", DataType::Int32, false))].into()),
        true,
    );
    assert!(matches!(
        validate_argument_observed(
            EvaluatedArgument::Column(&valid),
            Selection::all(2),
            &renamed,
            &Control::default()
        ),
        Err(KernelFailure::InvalidProgram(_))
    ));
    let control = Control::default();
    validate_argument_observed(
        EvaluatedArgument::Column(&valid),
        Selection::all(2),
        &logical,
        &control,
    )
    .unwrap();
    let trace = control.checks.lock().unwrap().clone();
    for at in 0..trace.len() {
        for failure in [
            KernelFailure::Cancelled,
            KernelFailure::DeadlineExceeded,
            KernelFailure::ResourceExhausted,
        ] {
            let refused = Control {
                refusal: Some((at, failure.clone())),
                ..Default::default()
            };
            assert_eq!(
                validate_argument_observed(
                    EvaluatedArgument::Column(&valid),
                    Selection::all(2),
                    &logical,
                    &refused
                ),
                Err(failure)
            );
            assert_eq!(*refused.checks.lock().unwrap(), trace[..=at]);
        }
    }
}

#[test]
fn permissive_runtime_list_ignores_parent_null_and_unused_child_nulls() {
    use arrow_array::{Int32Array, ListArray};
    use arrow_buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
    use novarocks_failure_contract::FailureProjection;
    let logical = FunctionValueType::new(
        DataType::List(Arc::new(Field::new("item", DataType::Int32, false))),
        true,
    );
    let array = |last: Option<i32>| -> ArrayRef {
        Arc::new(ListArray::new(
            Arc::new(Field::new("item", DataType::Int32, true)),
            OffsetBuffer::new(ScalarBuffer::from(vec![1_i32, 2, 3, 4])),
            Arc::new(Int32Array::from(vec![None, Some(11), None, last, None])),
            Some(NullBuffer::from(vec![true, false, true])),
        ))
    };
    let valid = array(Some(13));
    validate_argument_observed(
        EvaluatedArgument::Column(&valid),
        Selection::all(3),
        &logical,
        &Control::default(),
    )
    .unwrap();
    let invalid = array(None);
    let error = validate_argument_observed(
        EvaluatedArgument::Column(&invalid),
        Selection::all(3),
        &logical,
        &Control::default(),
    )
    .unwrap_err();
    assert_eq!(
        error.failure_fact(),
        novarocks_value_arrays::ValueError::Invariant(
            novarocks_value_arrays::ValueViolation::RequiredNull
        )
        .failure_fact()
    );
    validate_argument_observed(
        EvaluatedArgument::Column(&invalid),
        Selection::try_sparse(3, &[0, 1]).unwrap(),
        &logical,
        &Control::default(),
    )
    .unwrap();
    let DataType::List(child) = valid.data_type() else {
        panic!("original List")
    };
    assert!(child.is_nullable());
}

#[test]
fn permissive_argument_does_not_admit_foreign_metadata_or_runtime_encoding_wrappers() {
    use arrow_array::types::Int32Type;
    use arrow_array::{Int32Array, StructArray};
    let expected = FunctionValueType::new(
        DataType::Struct(vec![Arc::new(Field::new("n", DataType::Utf8, false))].into()),
        true,
    );
    let dictionary: ArrayRef = Arc::new(
        DictionaryArray::<Int32Type>::try_new(
            Int32Array::from(vec![0, 1]),
            Arc::new(StringArray::from(vec!["x", "y"])),
        )
        .unwrap(),
    );
    let run: ArrayRef = Arc::new(
        RunArray::<Int32Type>::try_new(&Int32Array::from(vec![2]), &StringArray::from(vec!["x"]))
            .unwrap(),
    );
    for child in [dictionary, run] {
        let source: ArrayRef = Arc::new(StructArray::new(
            vec![Arc::new(Field::new("n", child.data_type().clone(), true))].into(),
            vec![child],
            None,
        ));
        assert!(matches!(
            validate_argument_observed(
                EvaluatedArgument::Column(&source),
                Selection::all(2),
                &expected,
                &Control::default()
            ),
            Err(KernelFailure::InvalidProgram(_))
        ));
    }
    let source: ArrayRef = Arc::new(StructArray::new(
        vec![Arc::new(
            Field::new("n", DataType::Utf8, true).with_metadata(std::collections::HashMap::from([
                ("foreign".into(), "annotation".into()),
            ])),
        )]
        .into(),
        vec![Arc::new(StringArray::from(vec!["x", "y"]))],
        None,
    ));
    assert!(matches!(
        validate_argument_observed(
            EvaluatedArgument::Column(&source),
            Selection::all(2),
            &expected,
            &Control::default()
        ),
        Err(KernelFailure::InvalidProgram(_))
    ));
}
