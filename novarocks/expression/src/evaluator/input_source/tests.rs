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

//! Observe the Slot source selected by a real frame and its ownership after evaluation.
use super::{BorrowedInputSource, FrameInput, SlotSourceObserver};
use crate::testkit::{
    CountingAllocator, HarnessControl, PassThroughCallHost, TestInputSourceStorage,
    checked_constants, compile_fixture_unit, fixture_scope,
    guarded_fixtures::{fixture_constant_policy, unit_draft},
};
use crate::{
    CompiledExprSet, CompiledExpressionInstance, EvaluationEnvironment, EvaluationInput,
    ExprEvaluationFailure, ScopeLayouts,
};
use arrow::{
    array::{
        Array, ArrayRef, FixedSizeBinaryArray, Int32Array, StructArray,
        builder::FixedSizeBinaryBuilder,
    },
    buffer::NullBuffer,
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use novarocks_constant_contract::{ConstantPools, ConstantValue};
use novarocks_expression_contract::{
    BinaryOperator, ExprKind, ExprPort, RootOrdinal, RootScope, ValueId,
};
use novarocks_functions::{
    KernelDiagnostic, KernelEvaluationControl, KernelFailure, ScalarInvocationActivation,
    ScalarInvocationScope, SelectedValues, Selection,
    opaque_memory::{OpaqueReservation, OpaqueRetainedCharge},
};
use novarocks_type_contract::{
    CompileCheckpoints, CompilePhase, DecimalOverflowPolicy, EvaluationDemand, FunctionValueType,
    SemanticParameters, ValueTypeStorageHold, ValueTypeStorageLifetime,
};
use std::{
    collections::BTreeMap,
    convert::Infallible,
    sync::{Arc, Mutex},
    time::Duration,
};

struct Control;

impl KernelEvaluationControl for Control {
    fn checkpoint(&self, _: u32) -> Result<(), KernelFailure> {
        Ok(())
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("Slot must not wait")
    }
}

fn evaluate<'a>(
    instance: &mut CompiledExpressionInstance,
    input: EvaluationInput<'_>,
    selection: Selection<'a>,
) -> Result<SelectedValues<'a>, ExprEvaluationFailure<Infallible>> {
    instance.evaluate(
        input,
        ScalarInvocationScope::new(selection, selection),
        if selection.is_empty() {
            ScalarInvocationActivation::ValidateOnly
        } else {
            ScalarInvocationActivation::Activated
        },
        &Control,
        &mut PassThroughCallHost,
    )
}

// The SQL/LocalCompiler CASE shape has a separate seam test. This fixture
// authors that searched CASE over the same raw, Struct and Int32 port order.
fn fixed_binary_case_unit() -> (Arc<CompiledExprSet>, [RootOrdinal; 2]) {
    let nested = DataType::Struct(
        vec![Arc::new(Field::new(
            "f",
            DataType::FixedSizeBinary(4),
            false,
        ))]
        .into(),
    );
    let fields = [
        Field::new("raw", DataType::FixedSizeBinary(16), false),
        Field::new("st", nested, true),
        Field::new("n", DataType::Int32, false),
    ];
    let values = std::array::from_fn::<_, 3, _>(|ordinal| {
        let field = &fields[ordinal];
        (
            ValueId::new(u32::try_from(ordinal).unwrap()),
            FunctionValueType::new(field.data_type().clone(), field.is_nullable()),
        )
    });
    let scope = fixture_scope(
        &ExprPort {
            values: Box::from(values.clone()),
        },
        Arc::new(Schema::new(fields.into_iter().collect::<Vec<_>>())),
    );
    let mut builder = unit_draft();
    let mut constants = ConstantPools::empty();
    let (_, integer) = &values[2];
    let input = builder
        .add(None, integer.clone(), ExprKind::Value(values[2].0))
        .unwrap();
    let ten = ConstantValue::from_i32(
        Arc::new(integer.try_to_field("fixture").unwrap()),
        integer.clone(),
        10,
        fixture_constant_policy(),
        CompilePhase::Validate,
        &HarnessControl,
    )
    .unwrap();
    let ten = builder
        .add(
            None,
            integer.clone(),
            checked_constants::kind(&mut constants, ten),
        )
        .unwrap();
    let condition = builder
        .add(
            None,
            FunctionValueType::new(DataType::Boolean, false),
            ExprKind::Binary {
                left: input,
                op: BinaryOperator::Eq,
                right: ten,
                decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
                allow_throw_exception: None,
            },
        )
        .unwrap();
    let roots = std::array::from_fn(|i| {
        let (value, input_ty) = &values[i];
        let result_ty = input_ty.clone().with_nullable(true).unwrap();
        let input = builder
            .add(None, input_ty.clone(), ExprKind::Value(*value))
            .unwrap();
        let null = checked_constants::null(&result_ty, fixture_constant_policy());
        let null = builder
            .add(
                None,
                result_ty.clone(),
                checked_constants::kind(&mut constants, null),
            )
            .unwrap();
        let case = builder
            .add(
                None,
                result_ty,
                ExprKind::Case {
                    ieee_float_equality: false,
                    operand: None,
                    when_then: Box::from([(condition, input)]),
                    else_expr: Some(null),
                },
            )
            .unwrap();
        builder
            .root(case, EvaluationDemand::Value, RootScope::Port(0))
            .unwrap()
    });
    let mut work = CompileCheckpoints::try_new(&HarnessControl, CompilePhase::Validate).unwrap();
    let sealed = builder
        .seal(
            vec![ExprPort {
                values: Box::from(values),
            }],
            &mut work,
        )
        .unwrap();
    let unit = sealed.finish(BTreeMap::new(), &mut work).unwrap();
    work.finish().unwrap();
    let parameters = SemanticParameters::try_new([]).unwrap();
    (
        compile_fixture_unit(
            &unit,
            EvaluationEnvironment {
                constants: &constants,
                parameters: &parameters,
            },
            ScopeLayouts {
                ports: std::slice::from_ref(&scope),
                all_ports: None,
            },
            fixture_constant_policy(),
            &novarocks_functions::testkit::installed_builtin_owner_catalogue(),
        ),
        roots,
    )
}

// The neutral source retains an actual reservation for the fixture's Arrow batch.
struct SourceHold {
    _reservation: OpaqueReservation,
}
impl ValueTypeStorageHold for SourceHold {}

#[test]
fn fixed_binary_case_roots_preserve_values_and_input_lifetime() {
    let (unit, roots) = fixed_binary_case_unit();
    let schema = unit.scope(RootScope::Port(0)).unwrap().schema();
    let sparse = [1usize];
    let empty = [];
    for (expression, root) in roots.into_iter().enumerate() {
        for selection in [
            Selection::all(2),
            Selection::try_sparse(2, &sparse).unwrap(),
            Selection::try_sparse(2, &empty).unwrap(),
        ] {
            let mut raw = FixedSizeBinaryBuilder::with_capacity(2, 16);
            raw.append_value([0x11; 16]).unwrap();
            raw.append_value([0x22; 16]).unwrap();
            let mut child = FixedSizeBinaryBuilder::with_capacity(2, 4);
            child.append_value([1, 2, 3, 4]).unwrap();
            child.append_null();
            let DataType::Struct(nested_fields) = schema.field(1).data_type() else {
                panic!("actual frozen Struct")
            };
            assert!(!nested_fields[0].is_nullable());
            let st: ArrayRef = Arc::new(
                StructArray::try_new(
                    nested_fields.clone(),
                    vec![Arc::new(child.finish())],
                    Some(NullBuffer::from(vec![true, false])),
                )
                .unwrap(),
            );
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(raw.finish()),
                    st,
                    Arc::new(Int32Array::from(vec![10, 20])),
                ],
            )
            .unwrap();
            let source_tracker = Arc::new(CountingAllocator::new(usize::MAX));
            let hold = OpaqueRetainedCharge::try_new(source_tracker.clone()).unwrap();
            // The three fixture columns and nested validity have distinct buffers.
            let source_bytes = batch
                .columns()
                .iter()
                .map(|array| array.to_data().get_buffer_memory_size())
                .sum();
            let reservation = hold.reserve_operation(source_bytes).unwrap();
            assert!(source_bytes > 0);
            assert_eq!(source_tracker.outstanding(), source_bytes);
            let input_storage =
                TestInputSourceStorage::new(ValueTypeStorageLifetime::held(Arc::new(SourceHold {
                    _reservation: reservation,
                })));
            drop(hold);
            let original_storage = std::ptr::from_ref(&input_storage).addr();
            let actual_array = batch.column(expression).clone();
            let condition_array = batch.column(2).clone();
            let seen = Arc::new(Mutex::new([0usize; 2]));
            let observed = seen.clone();
            let observer: Arc<SlotSourceObserver> = Arc::new(move |column| {
                let (array, source) = column.into_parts();
                let ordinal = if Arc::ptr_eq(array, &condition_array) {
                    0
                } else {
                    assert!(
                        Arc::ptr_eq(array, &actual_array),
                        "CASE observes its own payload or condition Slot only"
                    );
                    1
                };
                let BorrowedInputSource::Storage(source) = source else {
                    panic!("actual input source must survive")
                };
                assert_eq!(std::ptr::from_ref(source).addr(), original_storage);
                observed.lock().unwrap()[ordinal] += 1;
            });
            let host_tracker = Arc::new(CountingAllocator::new(usize::MAX));
            let mut instance = CompiledExpressionInstance::try_new_with_allocator(
                unit.clone(),
                root,
                &Control,
                Some(host_tracker.clone()),
            )
            .unwrap();
            instance.observe_slot_source(observer);
            let input = EvaluationInput::Batch {
                batch: &batch,
                source: Some(&input_storage),
            };
            assert!(FrameInput::new(input).checked_column(3).is_none());
            let row_input = EvaluationInput::Batch {
                batch: &batch,
                source: None,
            };
            assert!(matches!(
                FrameInput::new(row_input)
                    .checked_column(expression)
                    .unwrap()
                    .into_parts()
                    .1,
                BorrowedInputSource::NoStorage,
            ));
            let output = evaluate(&mut instance, input, selection).expect(
                "the real whole CASE must prepare and materialize with its exact declaration",
            );
            assert!(output.errors().is_empty());
            assert_eq!(output.values().len(), selection.len());
            assert_eq!(
                *seen.lock().unwrap(),
                [
                    usize::from(!selection.is_empty()),
                    usize::from(selection.iter().any(|row| row == 0))
                ],
                "condition is read once; the THEN payload is read only for the true row"
            );
            if expression == 0 {
                let raw = output
                    .values()
                    .as_any()
                    .downcast_ref::<FixedSizeBinaryArray>()
                    .unwrap();
                assert_eq!(raw.value_length(), 16);
                for (i, row) in selection.iter().enumerate() {
                    assert_eq!(raw.is_null(i), row == 1);
                    if row == 0 {
                        assert_eq!(raw.value(i), &[0x11; 16]);
                    }
                }
            } else {
                let st = output
                    .values()
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .unwrap();
                let DataType::Struct(fields) = st.data_type() else {
                    unreachable!()
                };
                assert!(!fields[0].is_nullable());
                for (i, row) in selection.iter().enumerate() {
                    assert_eq!(st.is_null(i), row == 1);
                    if row == 0 {
                        assert_eq!(
                            st.column(0)
                                .as_any()
                                .downcast_ref::<FixedSizeBinaryArray>()
                                .unwrap()
                                .value(i),
                            &[1, 2, 3, 4]
                        );
                    }
                }
            }
            let data = output.values().to_data();
            let last = if expression == 0 {
                data.buffers()[0].clone()
            } else {
                data.child_data()[0].buffers()[0].clone()
            };
            drop(data);
            drop(output);
            drop(instance);
            drop((batch, input_storage));
            if !selection.is_empty() {
                assert_eq!(
                    source_tracker.outstanding(),
                    source_bytes,
                    "the output Buffer retains the exact original source"
                );
            }
            std::thread::scope(|scope| scope.spawn(move || drop(last)).join().unwrap());
            assert_eq!(source_tracker.outstanding(), 0);
            assert_eq!(host_tracker.outstanding(), 0);
        }
    }
}

#[test]
fn compiled_canonical_empty_result_keeps_its_own_creator_independent_of_equal_source() {
    use novarocks_type_contract::ValueLayout;
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct Creator(Arc<AtomicUsize>);
    impl ValueTypeStorageHold for Creator {}
    impl Drop for Creator {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    let data_type = DataType::Struct(
        vec![Arc::new(arrow::datatypes::Field::new(
            "canonical_child",
            DataType::Int64,
            true,
        ))]
        .into(),
    );
    let semantic = FunctionValueType::new(data_type.clone(), true);
    let source_drops = Arc::new(AtomicUsize::new(0));
    let canonical_drops = Arc::new(AtomicUsize::new(0));
    let source = FunctionValueType::try_from_value(
        semantic.value_type().clone(),
        ValueLayout::try_new(
            &data_type,
            ValueTypeStorageLifetime::held(Arc::new(Creator(source_drops.clone()))),
        )
        .unwrap(),
    )
    .unwrap();
    let canonical = FunctionValueType::try_from_value(
        semantic.value_type().clone(),
        ValueLayout::try_new(
            &data_type,
            ValueTypeStorageLifetime::held(Arc::new(Creator(canonical_drops.clone()))),
        )
        .unwrap(),
    )
    .unwrap();
    assert_eq!(source, canonical);
    assert!(
        !source
            .typed_binding()
            .layout()
            .shares_storage(canonical.typed_binding().layout())
    );
    let raw = arrow::array::new_empty_array(canonical.data_type());
    let missing = super::retain_result_type(raw.clone(), &canonical, None, &Control).unwrap_err();
    assert_eq!(
        missing,
        KernelFailure::InvalidProgram(KernelDiagnostic::new(
            "compiled result Arrow creator requires its actual allocation host"
        ))
    );
    let tracker = Arc::new(CountingAllocator::new(usize::MAX));
    let host: Arc<dyn novarocks_functions::AggregateStateAllocator> = tracker.clone();
    let result = super::retain_result_type(raw, &canonical, Some(&host), &Control).unwrap();
    assert_eq!(result.len(), 0);
    drop((source, canonical, semantic));
    assert_eq!(
        source_drops.load(Ordering::SeqCst),
        1,
        "equal source is not the canonical creator"
    );
    assert_eq!(
        canonical_drops.load(Ordering::SeqCst),
        0,
        "the empty result has actual independent canonical custody"
    );
    let last = result.clone();
    drop(result);
    assert_eq!(canonical_drops.load(Ordering::SeqCst), 0);
    std::thread::scope(|scope| scope.spawn(move || drop(last)).join().unwrap());
    assert_eq!(canonical_drops.load(Ordering::SeqCst), 1);
    assert_eq!(tracker.outstanding(), 0);
}
