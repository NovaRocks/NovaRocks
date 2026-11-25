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

use std::{
    alloc::Layout,
    ptr::NonNull,
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};

use arrow_array::{
    Array, ArrayRef, BooleanArray, Int32Array, Int64Array, ListArray, MapArray, NullArray,
    StructArray, types::Int64Type,
};
use arrow_buffer::{NullBuffer, OffsetBuffer};
use arrow_schema::Field;
use novarocks_type_contract::{
    CompileControlError, DecimalOverflowPolicy, EvaluationRegionId, ExpressionEffectContext,
    ExpressionEffects, ExpressionUseId, SemanticParameters,
};

use super::*;
use crate::{
    AggregateStateAllocator, CallArgumentUses, DataFailureReason, EvaluatedArgument,
    HigherOrderEvaluationInstance, HigherOrderEvaluationStep, HigherOrderPreparationOptions,
    KernelEvaluationControl, LambdaBodyBatch, RowDataError, ScopedExpressionEffects,
    SelectedValues, Selection, UnaccountedAggregateStateAllocator,
    opaque_memory::OpaqueAllocationHost, specialize_higher_order,
};

struct Control;
impl PureCompileControl for Control {
    fn checkpoint(&self, _: CompilePhase, work: u32) -> Result<(), CompileControlError> {
        assert!(work <= 256);
        Ok(())
    }
}
impl KernelEvaluationControl for Control {
    fn checkpoint(&self, work: u32) -> Result<(), KernelFailure> {
        assert!(work <= 256);
        Ok(())
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("collection lambda evaluation must use the original control checkpoints")
    }
}

#[derive(Default)]
struct Ledger {
    live: AtomicUsize,
    peak: AtomicUsize,
}
impl Ledger {
    fn charge(&self, bytes: usize) {
        let total = self.live.fetch_add(bytes, Ordering::Relaxed) + bytes;
        self.peak.fetch_max(total, Ordering::Relaxed);
    }
    fn refund(&self, bytes: usize) {
        let old = self.live.fetch_sub(bytes, Ordering::Relaxed);
        assert!(old >= bytes);
    }
}
impl AggregateStateAllocator for Ledger {
    fn allocate(&self, layout: Layout) -> Result<NonNull<u8>, KernelFailure> {
        let block = UnaccountedAggregateStateAllocator.allocate(layout)?;
        self.charge(layout.size());
        Ok(block)
    }
    unsafe fn release(&self, block: NonNull<u8>, layout: Layout) {
        // SAFETY: the caller returns the exact block and originating layout.
        unsafe { UnaccountedAggregateStateAllocator.release(block, layout) };
        self.refund(layout.size());
    }
    fn opaque_allocation_host(&self) -> Option<&dyn OpaqueAllocationHost> {
        Some(self)
    }
}
impl OpaqueAllocationHost for Ledger {
    fn reserve_opaque(&self, bytes: usize) -> Result<(), KernelFailure> {
        self.charge(bytes);
        Ok(())
    }
    fn release_opaque(&self, bytes: usize) {
        self.refund(bytes);
    }
}

fn prepared(
    name: &str,
    collection: &ArrayRef,
    result_type: FunctionValueType,
    effects: ExpressionEffects,
) -> Arc<dyn PreparedHigherOrderKernel> {
    let signature = signature(name).unwrap();
    let ordinary = FunctionArgument::Value {
        value_type: FunctionValueType::new(collection.data_type().clone(), true),
        constant: None,
    };
    let candidate = signature
        .begin(0, &ordinary, 1, signature.arity(), &Control)
        .unwrap();
    let arguments = [
        ordinary,
        FunctionArgument::Lambda {
            parameter_types: candidate.parameter_types,
            result_type,
        },
    ];
    let request = FunctionBindingRequest {
        logical_argument_count: arguments.len(),
        arguments: &arguments,
        expected_result_type: None,
    };
    let selected = Arc::new(signature.select(request, &Control).unwrap());
    let function = signature.function();
    let owner = CollectionLambdaOwner {
        signature,
        declaration: FunctionBindingDeclaration::try_new(
            function.clone(),
            FunctionKind::Scalar,
            [signature.declaration()],
        )
        .unwrap(),
        implementations: Box::from([signature.implementation().unwrap()]),
        prepare: Some(prepare_kernel),
    };
    let outer = ExpressionEffectContext {
        use_id: ExpressionUseId::new(0),
        region: EvaluationRegionId::new(0),
        demand: EvaluationDemand::Value,
    };
    let inner = ExpressionEffectContext {
        use_id: ExpressionUseId::new(3),
        region: EvaluationRegionId::new(1),
        demand: EvaluationDemand::Value,
    };
    let parameters = SemanticParameters::default();
    specialize_higher_order(
        &owner,
        CallEffectInput {
            context: outer.into(),
            argument_uses: CallArgumentUses::SelectedChannels(&[
                Some(ExpressionUseId::new(1)),
                Some(ExpressionUseId::new(2)),
            ]),
            function_id: &function,
            kind: FunctionKind::Scalar,
            selected: &selected,
            request,
            environment: &[],
            parameters: &parameters,
            decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
            proof_scope: CallProofScope::Region(outer.region),
        },
        Arc::clone(&selected),
        HigherOrderPreparationOptions {
            arguments: ScopedExpressionEffects::primitive(outer, ExpressionEffects::PURE_VALUE),
            body_context: inner,
            body_effects: ScopedExpressionEffects::primitive(inner, effects),
            capture_types: Box::default(),
        },
        &Control,
    )
    .unwrap()
    .into_prepared()
}

type BatchRecord = Vec<(usize, Vec<usize>)>;
struct Trial {
    output: ArrayRef,
    errors: Box<[RowDataError]>,
    batches: Vec<BatchRecord>,
    ledger: Arc<Ledger>,
}
impl Trial {
    fn check_release(self) {
        assert!(self.ledger.live.load(Ordering::Relaxed) > 0);
        assert!(self.ledger.peak.load(Ordering::Relaxed) > 0);
        let ledger = Arc::clone(&self.ledger);
        let alias = self.output.slice(0, self.output.len());
        drop(self);
        assert!(ledger.live.load(Ordering::Relaxed) > 0);
        drop(alias);
        assert_eq!(ledger.live.load(Ordering::Relaxed), 0);
    }
}

fn run(
    kernel: Arc<dyn PreparedHigherOrderKernel>,
    input: EvaluatedArgument<'_>,
    selection: Selection<'_>,
    mut evaluate: impl FnMut(&LambdaBodyBatch) -> Result<(ArrayRef, Vec<RowDataError>), KernelFailure>,
) -> Result<Trial, KernelFailure> {
    let body = Arc::clone(kernel.body_contract());
    let mut instance = HigherOrderEvaluationInstance::instantiate(kernel, &Control)?;
    let ledger = Arc::new(Ledger::default());
    let host: Arc<dyn AggregateStateAllocator> = ledger.clone();
    let arguments = [input];
    let mut execution = instance.begin(selection, &arguments, &host, &Control)?;
    let mut batches = vec![];
    loop {
        match execution.advance(&Control)? {
            HigherOrderEvaluationStep::Request(obligation) => {
                obligation.run_with(&Control, |demand| {
                    let batch = demand.batch();
                    batches.push(
                        batch
                            .parent_ordinals()
                            .iter()
                            .enumerate()
                            .map(|(row, &parent)| {
                                let positions = (0..batch.parameter_sources().len())
                                    .map(|param| batch.parameter_position(param, row).unwrap())
                                    .collect();
                                (parent, positions)
                            })
                            .collect(),
                    );
                    let (values, errors) = evaluate(batch)?;
                    let selected = SelectedValues::try_new(
                        demand.row_map().all_elements(),
                        body.result_type().data_type(),
                        values,
                        errors.into_boxed_slice(),
                    )
                    .map_err(KernelFailure::from)?;
                    demand.complete(&body, selected)
                })?
            }
            HigherOrderEvaluationStep::Complete(output) => {
                let (_, output, errors) = output.into_parts();
                return Ok(Trial {
                    output,
                    errors,
                    batches,
                    ledger,
                });
            }
        }
    }
}

fn list(rows: impl IntoIterator<Item = Option<Vec<Option<i64>>>>) -> ArrayRef {
    Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>(rows))
}

fn row(values: impl IntoIterator<Item = i64>) -> Option<Vec<Option<i64>>> {
    Some(values.into_iter().map(Some).collect())
}

fn value(batch: &LambdaBodyBatch, parameter: usize, row: usize) -> Option<i64> {
    let values = batch.parameter_sources()[parameter]
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let physical = batch.parameter_position(parameter, row).unwrap();
    (!values.is_null(physical)).then(|| values.value(physical))
}

#[test]
fn array_and_map_construction_preserve_shape_nulls_and_entry_identity() {
    let array = list([row([99]), None, row([]), Some(vec![Some(1), None, Some(2)])]).slice(1, 3);
    let transformed = run(
        prepared(
            "transform",
            &array,
            FunctionValueType::new(DataType::Int64, true),
            ExpressionEffects::PURE_VALUE,
        ),
        EvaluatedArgument::Column(&array),
        Selection::all(3),
        |batch| {
            Ok((
                Arc::new(Int64Array::from_iter(
                    (0..batch.parent_ordinals().len())
                        .map(|row| value(batch, 0, row).map(|x| x * 2)),
                )),
                vec![],
            ))
        },
    )
    .unwrap();
    assert_eq!(
        &*transformed.output,
        &*list([None, row([]), Some(vec![Some(2), None, Some(4)])])
    );
    assert_eq!(
        transformed.batches[0],
        vec![(2, vec![1]), (2, vec![2]), (2, vec![3])]
    );
    transformed.check_release();

    let array = array.slice(2, 1);
    let filtered = run(
        prepared(
            "filter",
            &array,
            FunctionValueType::new(DataType::Boolean, true),
            ExpressionEffects::PURE_VALUE,
        ),
        EvaluatedArgument::Scalar(&array),
        Selection::all(2),
        |batch| {
            Ok((
                Arc::new(BooleanArray::from_iter(
                    (0..batch.parent_ordinals().len())
                        .map(|row| value(batch, 0, row).map(|x| x % 2 == 0)),
                )),
                vec![],
            ))
        },
    )
    .unwrap();
    assert_eq!(&*filtered.output, &*list([row([2]), row([2])]));
    assert_eq!(
        filtered.batches[0]
            .iter()
            .map(|entry| entry.0)
            .collect::<Vec<_>>(),
        [0, 0, 0, 1, 1, 1]
    );
    filtered.check_release();

    let map = source_map();
    let filtered = run(
        prepared(
            "map_filter",
            &map,
            FunctionValueType::new(DataType::Boolean, true),
            ExpressionEffects::PURE_VALUE,
        ),
        EvaluatedArgument::Column(&map),
        Selection::all(3),
        |batch| {
            Ok((
                Arc::new(BooleanArray::from_iter(
                    (0..batch.parent_ordinals().len())
                        .map(|row| value(batch, 1, row).map(|x| x > 10)),
                )),
                vec![],
            ))
        },
    )
    .unwrap();
    let actual = filtered.output.as_any().downcast_ref::<MapArray>().unwrap();
    assert_eq!(actual.data_type(), map.data_type());
    assert_eq!(actual.value_offsets(), [0, 1, 1, 1]);
    assert!(actual.is_null(1) && !actual.is_null(2));
    assert_eq!(actual.keys().as_ref(), &Int64Array::from(vec![3]));
    assert_eq!(actual.values().as_ref(), &Int64Array::from(vec![30]));
    filtered.check_release();

    let transformed = run(
        prepared(
            "transform_values",
            &map,
            FunctionValueType::new(DataType::Int64, true),
            ExpressionEffects::PURE_VALUE,
        ),
        EvaluatedArgument::Column(&map),
        Selection::all(3),
        |batch| {
            Ok((
                Arc::new(Int64Array::from_iter(
                    (0..batch.parent_ordinals().len())
                        .map(|row| value(batch, 1, row).map(|x| x + value(batch, 0, row).unwrap())),
                )),
                vec![],
            ))
        },
    )
    .unwrap();
    let actual = transformed
        .output
        .as_any()
        .downcast_ref::<MapArray>()
        .unwrap();
    assert_eq!(actual.data_type(), map.data_type());
    assert_eq!(actual.value_offsets(), [0, 3, 3, 3]);
    assert!(actual.is_null(1) && !actual.is_null(2));
    assert_eq!(actual.keys().as_ref(), &Int64Array::from(vec![1, 2, 3]));
    assert_eq!(
        actual.values().as_ref(),
        &Int64Array::from(vec![Some(11), None, Some(33)])
    );
    transformed.check_release();
}

fn source_map() -> ArrayRef {
    let fields = Fields::from(vec![
        Field::new("key", DataType::Int64, false),
        Field::new("value", DataType::Int64, true),
    ]);
    let map = MapArray::try_new(
        Arc::new(Field::new(
            "entries",
            DataType::Struct(fields.clone()),
            false,
        )),
        OffsetBuffer::new(vec![0, 3, 3, 3].into()),
        StructArray::try_new(
            fields,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(Int64Array::from(vec![Some(10), None, Some(30)])),
            ],
            None,
        )
        .unwrap(),
        Some(NullBuffer::from(vec![true, false, true])),
        false,
    )
    .unwrap();
    Arc::new(map)
}

#[test]
fn matches_grow_actual_work_and_keep_the_first_terminal_value_or_error() {
    let array = list([
        row([0, 1, 2, 3, 4, 5]),
        row([2, 1, 0]),
        None,
        row([]),
        row([6]),
    ]);
    for (name, decisive) in [("any_match", true), ("all_match", false)] {
        let kernel = prepared(
            name,
            &array,
            FunctionValueType::new(DataType::Boolean, true),
            ExpressionEffects {
                may_raise_row_error: true,
                ..ExpressionEffects::PURE_VALUE
            },
        );
        let trial = run(
            kernel,
            EvaluatedArgument::Column(&array),
            Selection::all(5),
            |batch| {
                let mut errors = vec![];
                let values =
                    BooleanArray::from_iter((0..batch.parent_ordinals().len()).map(|row| {
                        match value(batch, 0, row).unwrap() {
                            2 => {
                                errors.push(RowDataError::with_reason(
                                    row,
                                    DataFailureReason::DivisionByZero,
                                    "actual body zero divisor",
                                ));
                                None
                            }
                            1 => Some(decisive),
                            6 => None,
                            _ => Some(!decisive),
                        }
                    }));
                Ok((Arc::new(values), errors))
            },
        )
        .unwrap();
        let actual = trial
            .output
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert_eq!(
            actual.iter().collect::<Vec<_>>(),
            [Some(decisive), None, None, Some(!decisive), None]
        );
        assert_eq!(trial.errors.len(), 1);
        assert_eq!(trial.errors[0].selected_ordinal(), 1);
        assert_eq!(trial.errors[0].reason(), DataFailureReason::DivisionByZero);
        assert_eq!(
            trial.batches[0]
                .iter()
                .map(|(parent, _)| *parent)
                .collect::<Vec<_>>(),
            [0, 1, 4]
        );
        assert_eq!(trial.batches[1], [(0, vec![1]), (0, vec![2])]);
        assert_eq!(trial.batches.len(), 2);
        trial.check_release();
    }
}

#[test]
fn actual_outer_refusal_from_a_speculative_tail_is_preserved() {
    let array = list([row([0, 1, 2, 3, 4, 5])]);
    let kernel = prepared(
        "any_match",
        &array,
        FunctionValueType::new(DataType::Boolean, true),
        ExpressionEffects::PURE_VALUE,
    );
    let mut invocations = 0;
    let original =
        KernelFailure::Operational(crate::KernelDiagnostic::new("original staged body refusal"));
    let error = run(
        kernel,
        EvaluatedArgument::Column(&array),
        Selection::all(1),
        |batch| {
            invocations += 1;
            if invocations == 2 {
                assert_eq!(batch.parent_ordinals(), [0, 0]);
                return Err(original.clone());
            }
            Ok((Arc::new(BooleanArray::from(vec![false])), vec![]))
        },
    )
    .err()
    .unwrap();
    assert_eq!(invocations, 2);
    assert_eq!(error, original);
}

#[test]
fn key_transform_uses_identical_and_defers_duplicates_until_body_completion() {
    let map = source_map();
    let row_fields = Fields::from(vec![
        Field::new("actual", DataType::Int64, false),
        Field::new("nullable", DataType::Int64, true),
    ]);
    let row_type = FunctionValueType::new(DataType::Struct(row_fields.clone()), false);
    let trial = run(
        prepared(
            "transform_keys",
            &map,
            row_type.clone(),
            ExpressionEffects::PURE_VALUE,
        ),
        EvaluatedArgument::Column(&map),
        Selection::all(3),
        |batch| {
            let size = batch.parent_ordinals().len();
            let keys = Int64Array::from_iter_values(
                (0..size).map(|ordinal| value(batch, 0, ordinal).unwrap()),
            );
            let array = StructArray::try_new(
                row_fields.clone(),
                vec![Arc::new(keys), Arc::new(Int64Array::new_null(size))],
                None,
            )
            .unwrap();
            Ok((Arc::new(array), vec![]))
        },
    )
    .unwrap();
    let actual = trial.output.as_any().downcast_ref::<MapArray>().unwrap();
    assert_eq!(actual.value_offsets(), [0, 3, 3, 3]);
    assert_eq!(actual.keys().data_type(), row_type.data_type());
    assert!(actual.is_null(1) && !actual.is_null(2));
    trial.check_release();

    let trial = run(
        prepared(
            "transform_keys",
            &map,
            row_type,
            ExpressionEffects::PURE_VALUE,
        ),
        EvaluatedArgument::Column(&map),
        Selection::all(3),
        |batch| {
            let size = batch.parent_ordinals().len();
            let array = StructArray::try_new(
                row_fields.clone(),
                vec![
                    Arc::new(Int64Array::from_value(0, size)),
                    Arc::new(Int64Array::new_null(size)),
                ],
                None,
            )
            .unwrap();
            Ok((Arc::new(array), vec![]))
        },
    )
    .unwrap();
    assert_eq!(
        trial.errors[0].message(),
        "Duplicate map keys are not allowed"
    );
    trial.check_release();

    let trial = run(
        prepared(
            "transform_keys",
            &map,
            FunctionValueType::new(DataType::Int64, true),
            ExpressionEffects {
                may_raise_row_error: true,
                ..ExpressionEffects::PURE_VALUE
            },
        ),
        EvaluatedArgument::Column(&map),
        Selection::all(3),
        |batch| {
            let mut errors = vec![];
            let output = Int64Array::from_iter((0..batch.parent_ordinals().len()).map(|row| {
                if value(batch, 0, row) == Some(3) {
                    errors.push(RowDataError::with_reason(
                        row,
                        DataFailureReason::DivisionByZero,
                        "later actual body error",
                    ));
                    None
                } else {
                    Some(1)
                }
            }));
            Ok((Arc::new(output), errors))
        },
    )
    .unwrap();
    assert_eq!(trial.errors[0].reason(), DataFailureReason::DivisionByZero);
    trial.check_release();
}

#[test]
fn effectful_keys_stop_at_null_and_unknown_keys_have_no_demand() {
    let map = source_map();
    let trial = run(
        prepared(
            "transform_keys",
            &map,
            FunctionValueType::new(DataType::Int64, true),
            ExpressionEffects {
                observable_effects: ObservableEffects {
                    warnings: true,
                    ..ObservableEffects::NONE
                },
                ..ExpressionEffects::PURE_VALUE
            },
        ),
        EvaluatedArgument::Column(&map),
        Selection::all(3),
        |batch| {
            assert_eq!(batch.parent_ordinals(), [0]);
            assert_eq!(batch.parameter_position(0, 0), Some(0));
            Ok((Arc::new(Int64Array::new_null(1)), vec![]))
        },
    )
    .unwrap();
    assert_eq!(trial.batches.len(), 1);
    assert_eq!(trial.errors[0].message(), "map key cannot be null");
    trial.check_release();

    let unknown = run(
        prepared(
            "transform_keys",
            &map,
            FunctionValueType::new(DataType::Null, true),
            ExpressionEffects::PURE_VALUE,
        ),
        EvaluatedArgument::Column(&map),
        Selection::all(3),
        |_| -> Result<(ArrayRef, Vec<RowDataError>), KernelFailure> {
            panic!("an unknown map key has no body demand")
        },
    )
    .unwrap();
    let actual = unknown.output.as_any().downcast_ref::<MapArray>().unwrap();
    assert!(actual.is_null(0) && actual.is_null(1) && !actual.is_null(2));
    assert_eq!(actual.value_offsets(), [0, 0, 0, 0]);
    assert!(actual.keys().as_any().is::<NullArray>());
    assert_eq!(unknown.errors[0].message(), "map key cannot be null");
    unknown.check_release();
}

#[test]
fn one_long_comparator_row_batches_independent_runs_and_keeps_ties_stable() {
    let values = (0..129).rev().collect::<Vec<_>>();
    let input = list([row(values.iter().copied())]);
    let trial = run(
        prepared(
            "array_sort",
            &input,
            FunctionValueType::new(DataType::Int32, false),
            ExpressionEffects::PURE_VALUE,
        ),
        EvaluatedArgument::Column(&input),
        Selection::all(1),
        |batch| {
            let results =
                Int32Array::from_iter_values((0..batch.parent_ordinals().len()).map(|row| {
                    (value(batch, 0, row).unwrap() % 7).cmp(&(value(batch, 1, row).unwrap() % 7))
                        as i32
                }));
            Ok((Arc::new(results), vec![]))
        },
    )
    .unwrap();
    let mut expected = values;
    expected.sort_by_key(|value| value % 7);
    assert_eq!(&*trial.output, &*list([row(expected)]));
    assert_eq!(trial.batches[0].len(), 64);
    for (index, (parent, pair)) in trial.batches[0].iter().enumerate() {
        assert_eq!(
            (*parent, pair.as_slice()),
            (0, &[index * 2, index * 2 + 1][..])
        );
    }
    trial.check_release();
}

#[test]
fn effectful_comparator_does_not_request_tail_after_invalid_result() {
    let array = list([row(0..30)]);
    let trial = run(
        prepared(
            "array_sort",
            &array,
            FunctionValueType::new(DataType::Int32, true),
            ExpressionEffects {
                has_instance_state: true,
                ..ExpressionEffects::PURE_VALUE
            },
        ),
        EvaluatedArgument::Column(&array),
        Selection::all(1),
        |batch| {
            assert_eq!(batch.parent_ordinals(), &[0]);
            Ok((Arc::new(Int32Array::from(vec![Some(2)])), vec![]))
        },
    )
    .unwrap();
    assert_eq!(trial.batches, [vec![(0, vec![0, 1])]]);
    assert_eq!(
        trial.errors[0].reason(),
        DataFailureReason::Domain(
            novarocks_failure_contract::kernel_reason::INVALID_FUNCTION_ARGUMENT
        )
    );
    assert!(trial.output.is_null(0));
    trial.check_release();
}
