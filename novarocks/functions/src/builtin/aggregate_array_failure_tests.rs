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

//! Actual host-owned ARRAY graphs, constructor refusal and failed-state latch.
use super::*;

use arrow_array::{LargeBinaryArray, StringArray};

use novarocks_type_contract::{
    CallProofScope, CompileControlError, CompilePhase, DecimalOverflowPolicy, EvaluationDemand,
    EvaluationRegionId, ExpressionEffectContext, ExpressionUseId, PureCompileControl,
    SemanticParameters, ValueKind, ValueLayout, ValueTypeStorageLifetime,
};
use std::{alloc::Layout, ptr::NonNull, sync::Mutex, time::Duration};
struct Compile;
impl PureCompileControl for Compile {
    fn checkpoint(&self, _: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert!(units <= 256);
        Ok(())
    }
}
#[derive(Default)]
struct Control {
    trace: Mutex<Vec<u32>>,
    refusal: Option<(usize, KernelFailure)>,
}
impl KernelEvaluationControl for Control {
    fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        if let Some((stop, _)) = &self.refusal {
            assert!(at <= *stop, "callback after primary refusal");
        }
        trace.push(units);
        match &self.refusal {
            Some((stop, cause)) if *stop == at => Err(cause.clone()),
            _ => Ok(()),
        }
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("ARRAY never waits")
    }
}
#[derive(Default)]
struct Ledger {
    attempts: usize,
    bytes: usize,
    peak: usize,
    live: Vec<(usize, Layout)>,
    metadata: Option<(usize, Layout)>,
}
#[derive(Default)]
struct Host {
    ledger: Mutex<Ledger>,
    refusal: Mutex<Option<(usize, KernelFailure)>>,
}
impl AggregateStateAllocator for Host {
    fn allocate(&self, layout: Layout) -> Result<NonNull<u8>, KernelFailure> {
        assert_ne!(layout.size(), 0);
        let mut ledger = self.ledger.lock().unwrap();
        let at = ledger.attempts;
        ledger.attempts += 1;
        if let Some((stop, cause)) = &*self.refusal.lock().unwrap()
            && *stop == at
        {
            return Err(cause.clone());
        }
        let pointer = NonNull::new(unsafe { std::alloc::alloc(layout) })
            .ok_or(KernelFailure::ResourceExhausted)?;
        ledger.bytes += layout.size();
        ledger.peak = ledger.peak.max(ledger.bytes);
        let block = (pointer.as_ptr().addr(), layout);
        if ledger.metadata.is_none() {
            ledger.metadata = Some(block);
        }
        ledger.live.push(block);
        Ok(pointer)
    }
    unsafe fn release(&self, pointer: NonNull<u8>, layout: Layout) {
        let mut ledger = self.ledger.lock().unwrap();
        let at = ledger
            .live
            .iter()
            .position(|(address, actual)| *address == pointer.as_ptr().addr() && *actual == layout)
            .expect("exact block released once");
        ledger.live.swap_remove(at);
        ledger.bytes -= layout.size();
        unsafe { std::alloc::dealloc(pointer.as_ptr(), layout) };
    }
}
fn arm_refusal(host: &Host, offset: usize, cause: KernelFailure) {
    let next = host.ledger.lock().unwrap().attempts;
    *host.refusal.lock().unwrap() = Some((next + offset, cause));
}
fn assert_metadata_only(host: &Host) {
    let ledger = host.ledger.lock().unwrap();
    let metadata = ledger
        .metadata
        .expect("successful constructor allocated metadata");
    assert_eq!(ledger.live.as_slice(), &[metadata]);
    assert_eq!(ledger.bytes, metadata.1.size());
}
fn causes() -> [KernelFailure; 7] {
    [
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
        invalid("host original"),
        internal("host original"),
        KernelFailure::Operational(KernelDiagnostic::new("host original")),
        KernelFailure::InstanceFailed,
    ]
}

use arrow_array::{Array, Int64Array, ListArray};
use arrow_buffer::OffsetBuffer;
use arrow_schema::Field;
fn kernel_source(
    name: &str,
    source: FunctionValueType,
    phase: AggregateKernelPhase,
    receipt: bool,
) -> Result<ArrayKernel, FunctionSpecializationFailure> {
    let catalog = super::super::catalogue::build_builtin_engine_function_catalog().unwrap();
    let args = [FunctionArgument::Value {
        value_type: source,
        constant: None,
    }];
    let request = FunctionBindingRequest {
        arguments: &args,
        logical_argument_count: 1,
        expected_result_type: None,
    };
    let bound = catalog
        .resolve_bound_trusted(name, FunctionKind::Aggregate, request, &Compile)
        .unwrap();
    let selected = Arc::new(bound.selected);
    let context = novarocks_type_contract::RelationEffectContext::call(
        novarocks_type_contract::RelationCallId::new(641),
    );
    let state_context = ExpressionEffectContext {
        use_id: ExpressionUseId::new(644),
        region: EvaluationRegionId::new(70),
        demand: EvaluationDemand::Value,
    };
    let state_type = selected
        .aggregate
        .as_ref()
        .unwrap()
        .intermediate_type
        .clone();
    let uses = [Some(ExpressionUseId::new(642))];
    let argument_uses = if phase.consumes_logical_arguments() {
        CallArgumentUses::SelectedChannels(&uses)
    } else {
        CallArgumentUses::AggregateMerge {
            phase,
            state_context: state_context.into(),
            state_input_type: &state_type,
        }
    };
    let parameters = SemanticParameters::try_new([]).unwrap();
    let prepared = catalog.prepare_fresh_selected(
        CallEffectInput {
            function_id: &bound.function_id,
            kind: FunctionKind::Aggregate,
            selected: &selected,
            request,
            argument_uses,
            context: context.into(),
            parameters: &parameters,
            environment: &[],
            decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
            proof_scope: CallProofScope::Relation(context.identity),
        },
        selected.clone(),
        PureCallPreparation::Aggregate {
            arguments: ScopedRelationalEffects::pure_value(context),
            options: AggregatePreparationOptions {
                state_interpretation: receipt.then(|| {
                    Arc::new(novarocks_type_contract::AggregateStateInterpretation {
                        distinct: false,
                        order_keys:
                            novarocks_type_contract::AggregateStateOrderKeys::from_pure_boxed(
                                Box::new([]),
                            ),
                    })
                }),
                phase,
                distinct: false,
                order_keys: Arc::from([]),
                state_input_type: (!phase.consumes_logical_arguments())
                    .then_some(state_type.clone()),
            },
        },
        &Compile,
    )?;
    let PreparedPureKernel::Aggregate(handle) = prepared.prepared() else {
        panic!("ARRAY handle")
    };
    Ok(ArrayKernel {
        contract: handle.contract().clone(),
        operation: match name {
            "array_agg" => Operation::Array,
            "array_agg_distinct" => Operation::Distinct,
            "array_unique_agg" => Operation::Unique,
            _ => unreachable!(),
        },
        ascending: Box::new([]),
        nulls_first: Box::new([]),
    })
}
fn nested() -> ArrayRef {
    Arc::new(ListArray::new(
        Arc::new(Field::new("item", DataType::Utf8, true)),
        OffsetBuffer::new(vec![0i32, 3].into()),
        Arc::new(StringArray::from(vec![
            Some("nested-value"),
            None,
            Some("another-value"),
        ])),
        None,
    ))
}
fn source_type() -> FunctionValueType {
    FunctionValueType::from_legacy_declared_projection(nested().data_type().clone(), false)
}
fn kernel(name: &str, phase: AggregateKernelPhase) -> ArrayKernel {
    kernel_source(name, source_type(), phase, true).unwrap()
}
fn new_state(kernel: &ArrayKernel, host: Arc<Host>) -> ArrayState {
    kernel
        .create_state_with_allocator(Some(host), &Control::default())
        .unwrap()
}
fn update(
    kernel: &ArrayKernel,
    state: &mut ArrayState,
    values: &ArrayRef,
    control: &dyn KernelEvaluationControl,
) -> Result<(), KernelFailure> {
    let args = [EvaluatedArgument::Column(values)];
    let setup = Control::default();
    let input = SelectedAggregateUpdateInput::try_new(
        &kernel.contract,
        Selection::all(1),
        &args,
        &[],
        &setup,
    )
    .unwrap();
    let input = kernel.prepare_update(input, &setup).unwrap();
    kernel.update_row(state, &input, 0, control)
}
fn merge(
    kernel: &ArrayKernel,
    state: &mut ArrayState,
    values: &ArrayRef,
    control: &dyn KernelEvaluationControl,
) -> Result<(), KernelFailure> {
    let setup = Control::default();
    let input = SelectedAggregateMergeInput::try_new(
        &kernel.contract,
        Selection::all(1),
        EvaluatedArgument::Column(values),
        &setup,
    )
    .unwrap();
    let input = kernel.prepare_merge(input, &setup).unwrap();
    kernel.merge_row(state, &input, 0, control)
}
fn assert_released(host: &Host) {
    let ledger = host.ledger.lock().unwrap();
    assert_eq!(ledger.bytes, 0);
    assert!(ledger.live.is_empty());
}
fn assert_failed(kernel: &ArrayKernel, state: &ArrayState) {
    assert!(state.failed);
    assert!(state.values.is_empty());
    assert_eq!(
        kernel.retained_bytes(state),
        state.values.allocator.metadata_bytes()
    );
    assert!(matches!(
        kernel.build_final(std::iter::once(state), &Control::default()),
        Err(KernelFailure::InstanceFailed)
    ));
}
#[test]
fn array_actual_owner_all_four_phases_keep_original_nested_state_codec() {
    let json = FunctionValueType::try_with_logical_type(
        DataType::LargeBinary,
        true,
        ValueLogicalType::Json,
    )
    .unwrap();
    let jsons: ArrayRef = Arc::new(ListArray::new(
        Arc::new(Field::new("item", DataType::LargeBinary, true)),
        OffsetBuffer::new(vec![0_i32, 3].into()),
        Arc::new(LargeBinaryArray::from(vec![
            Some(&[0x01, 0x20][..]),
            None,
            Some(&[0x01, 0x44, 0x01, b'x'][..]),
        ])),
        None,
    ));
    let storage = ValueTypeStorageLifetime::pure();
    let json_list = FunctionValueType::try_from_value(
        ValueTypeRef::try_new(
            ValueKind::List(json.value_type().clone()),
            false,
            storage.clone(),
        )
        .unwrap(),
        ValueLayout::try_new(jsons.data_type(), storage).unwrap(),
    )
    .unwrap();
    assert_eq!(
        json_list.child_at(0).unwrap().logical_type(),
        ValueLogicalType::Json
    );
    for (name, source, values) in [
        ("array_agg", source_type(), nested()),
        ("array_agg_distinct", source_type(), nested()),
        ("array_agg", json_list, jsons),
    ] {
        let kernel = |phase| kernel_source(name, source.clone(), phase, true).unwrap();
        let single = kernel(AggregateKernelPhase::Single);
        let host = Arc::new(Host::default());
        let mut original = new_state(&single, host.clone());
        update(&single, &mut original, &values, &Control::default()).unwrap();
        assert_eq!(
            single.retained_bytes(&original),
            host.ledger.lock().unwrap().bytes
        );
        let expected = single
            .build_final(std::iter::once(&original), &Control::default())
            .unwrap();
        let expected = expected.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(expected.values().to_data(), values.to_data());
        let partial = kernel(AggregateKernelPhase::Partial);
        let wire = partial
            .build_intermediate(std::iter::once(&original), &Control::default())
            .unwrap();
        let middle = kernel(AggregateKernelPhase::Intermediate);
        let mut state = new_state(&middle, host.clone());
        merge(&middle, &mut state, &wire, &Control::default()).unwrap();
        let again = middle
            .build_intermediate(std::iter::once(&state), &Control::default())
            .unwrap();
        assert_eq!(wire.to_data(), again.to_data());
        let last = kernel(AggregateKernelPhase::Final);
        let mut final_state = new_state(&last, host.clone());
        merge(&last, &mut final_state, &again, &Control::default()).unwrap();
        let actual = last
            .build_final(std::iter::once(&final_state), &Control::default())
            .unwrap();
        assert_eq!(expected.to_data(), actual.to_data());
        drop((original, state, final_state));
        assert_released(&host);
    }
}
#[test]
fn array_every_real_nested_update_allocation_refusal_keeps_typed_cause_and_rolls_back() {
    for name in ["array_agg", "array_agg_distinct"] {
        let kernel = kernel(name, AggregateKernelPhase::Single);
        let probe = Arc::new(Host::default());
        let mut state = new_state(&kernel, probe.clone());
        let first = probe.ledger.lock().unwrap().attempts;
        update(&kernel, &mut state, &nested(), &Control::default()).unwrap();
        let allocations = probe.ledger.lock().unwrap().attempts - first;
        assert!(allocations >= 4);
        drop(state);
        assert_released(&probe);
        for stop in 0..allocations {
            for cause in causes() {
                let host = Arc::new(Host::default());
                let mut state = new_state(&kernel, host.clone());
                arm_refusal(&host, stop, cause.clone());
                assert_eq!(
                    update(&kernel, &mut state, &nested(), &Control::default()),
                    Err(cause)
                );
                assert_failed(&kernel, &state);
                assert_metadata_only(&host);
                let attempts = host.ledger.lock().unwrap().attempts;
                assert_eq!(
                    update(&kernel, &mut state, &nested(), &Control::default()),
                    Err(KernelFailure::InstanceFailed)
                );
                assert_eq!(host.ledger.lock().unwrap().attempts, attempts);
                drop(state);
                assert_released(&host);
            }
        }
    }
}
#[test]
fn array_every_real_nested_merge_allocation_refusal_keeps_typed_cause_and_rolls_back() {
    for name in ["array_agg", "array_agg_distinct"] {
        let partial = kernel(name, AggregateKernelPhase::Partial);
        let source_host = Arc::new(Host::default());
        let mut source = new_state(&partial, source_host.clone());
        update(&partial, &mut source, &nested(), &Control::default()).unwrap();
        let wire = partial
            .build_intermediate(std::iter::once(&source), &Control::default())
            .unwrap();
        drop(source);
        assert_released(&source_host);
        let last = kernel(name, AggregateKernelPhase::Final);
        let probe = Arc::new(Host::default());
        let mut state = new_state(&last, probe.clone());
        let first = probe.ledger.lock().unwrap().attempts;
        merge(&last, &mut state, &wire, &Control::default()).unwrap();
        let allocations = probe.ledger.lock().unwrap().attempts - first;
        assert!(allocations >= 4);
        drop(state);
        assert_released(&probe);
        for stop in 0..allocations {
            for cause in causes() {
                let host = Arc::new(Host::default());
                let mut state = new_state(&last, host.clone());
                arm_refusal(&host, stop, cause.clone());
                assert_eq!(
                    merge(&last, &mut state, &wire, &Control::default()),
                    Err(cause)
                );
                assert_failed(&last, &state);
                assert_metadata_only(&host);
                drop(state);
                assert_released(&host);
            }
        }
    }
}
#[test]
fn array_every_update_checkpoint_preserves_seven_causes_and_failed_latch() {
    for name in ["array_agg", "array_agg_distinct"] {
        let kernel = kernel(name, AggregateKernelPhase::Single);
        let probe = Arc::new(Host::default());
        let mut state = new_state(&kernel, probe.clone());
        let trace = Control::default();
        update(&kernel, &mut state, &nested(), &trace).unwrap();
        let checkpoints = trace.trace.lock().unwrap().len();
        drop(state);
        assert_released(&probe);
        for stop in 0..checkpoints {
            for cause in causes() {
                let host = Arc::new(Host::default());
                let mut state = new_state(&kernel, host.clone());
                let control = Control {
                    trace: Mutex::new(vec![]),
                    refusal: Some((stop, cause.clone())),
                };
                assert_eq!(update(&kernel, &mut state, &nested(), &control), Err(cause));
                assert_failed(&kernel, &state);
                assert_metadata_only(&host);
                drop(state);
                assert_released(&host);
            }
        }
    }
}
#[test]
fn array_missing_host_and_real_constructor_refusals_never_publish_state() {
    for name in ["array_agg", "array_agg_distinct"] {
        let kernel = kernel(name, AggregateKernelPhase::Single);
        assert!(matches!(
            kernel.create_state(&Control::default()),
            Err(KernelFailure::InvalidProgram(_))
        ));
        for cause in causes() {
            let host = Arc::new(Host::default());
            arm_refusal(&host, 0, cause.clone());
            assert!(
                matches!(kernel.create_state_with_allocator(Some(host.clone()),&Control::default()),Err(actual)if actual==cause)
            );
            assert_released(&host);
        }
    }
}
#[test]
fn array_all_phases_require_actual_producer_receipt() {
    for name in ["array_agg", "array_agg_distinct"] {
        for phase in [
            AggregateKernelPhase::Single,
            AggregateKernelPhase::Partial,
            AggregateKernelPhase::Intermediate,
            AggregateKernelPhase::Final,
        ] {
            let error = kernel_source(name, source_type(), phase, false).unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("ARRAY aggregate requires its original state interpretation facts"),
                "{error}"
            );
        }
    }
}

#[test]
fn array_every_merge_and_output_checkpoint_preserves_original_typed_cause() {
    for name in ["array_agg", "array_agg_distinct"] {
        let partial = kernel(name, AggregateKernelPhase::Partial);
        let source_host = Arc::new(Host::default());
        let mut source = new_state(&partial, source_host.clone());
        update(&partial, &mut source, &nested(), &Control::default()).unwrap();
        let wire = partial
            .build_intermediate(std::iter::once(&source), &Control::default())
            .unwrap();
        let last = kernel(name, AggregateKernelPhase::Final);
        let probe = Arc::new(Host::default());
        let mut state = new_state(&last, probe.clone());
        let trace = Control::default();
        merge(&last, &mut state, &wire, &trace).unwrap();
        let checkpoints = trace.trace.lock().unwrap().len();
        drop(state);
        assert_released(&probe);
        for stop in 0..checkpoints {
            for cause in causes() {
                let host = Arc::new(Host::default());
                let mut state = new_state(&last, host.clone());
                let control = Control {
                    trace: Mutex::new(vec![]),
                    refusal: Some((stop, cause.clone())),
                };
                assert_eq!(merge(&last, &mut state, &wire, &control), Err(cause));
                assert_failed(&last, &state);
                assert_metadata_only(&host);
                drop(state);
                assert_released(&host);
            }
        }
        for intermediate in [false, true] {
            let trace = Control::default();
            if intermediate {
                partial
                    .build_intermediate(std::iter::once(&source), &trace)
                    .unwrap();
            } else {
                partial
                    .build_final(std::iter::once(&source), &trace)
                    .unwrap();
            }
            let checkpoints = trace.trace.lock().unwrap().len();
            for stop in 0..checkpoints {
                for cause in causes() {
                    let control = Control {
                        trace: Mutex::new(vec![]),
                        refusal: Some((stop, cause.clone())),
                    };
                    let result = if intermediate {
                        partial.build_intermediate(std::iter::once(&source), &control)
                    } else {
                        partial.build_final(std::iter::once(&source), &control)
                    };
                    assert!(matches!(result,Err(actual)if actual==cause));
                }
            }
        }
        drop(source);
        assert_released(&source_host);
    }
}

#[test]
fn array_original_fingerprint_keeps_nan_and_distinct_decimal256_encodings() {
    use crate::aggregate_scalar::{AggScalarValue as V, TrackedAggScalarValue as T};
    use crate::aggregate_scalar_fingerprint::{key_fingerprint, tracked_key_fingerprint};
    use arrow_buffer::i256;
    let host = Arc::new(Host::default());
    let allocator = HostAggregateAllocator::try_new(host.clone()).unwrap();
    let value = i256::from_i128(-123);
    let tracked = tracked_key_fingerprint(
        &T::Decimal256(value),
        &allocator,
        &mut ScalarWork::new(None),
    )
    .unwrap();
    let mut expected = vec![11];
    expected.extend_from_slice(&value.to_le_bytes());
    assert_eq!(tracked.as_slice(), expected);
    let mut expected_text = vec![11];
    expected_text.extend_from_slice(&4u32.to_le_bytes());
    expected_text.extend_from_slice(b"-123");
    assert_eq!(
        key_fingerprint(&V::Decimal256(value), &mut ScalarWork::new(None)).unwrap(),
        expected_text
    );
    let nan1 = V::Float64(f64::from_bits(0x7ff8000000000041));
    let nan2 = V::Float64(f64::from_bits(0x7ff8000000000042));
    assert_eq!(
        key_fingerprint(&nan1, &mut ScalarWork::new(None)).unwrap(),
        key_fingerprint(&nan2, &mut ScalarWork::new(None)).unwrap()
    );
    assert_ne!(
        key_fingerprint(&V::Float64(-0.0), &mut ScalarWork::new(None)).unwrap(),
        key_fingerprint(&V::Float64(0.0), &mut ScalarWork::new(None)).unwrap()
    );
    drop((tracked, allocator));
    assert_released(&host);
}

fn ordered_kernel_source(
    name: &str,
    source: FunctionValueType,
    phase: AggregateKernelPhase,
    receipt: bool,
) -> Result<ArrayKernel, FunctionSpecializationFailure> {
    let catalog = super::super::catalogue::build_builtin_engine_function_catalog().unwrap();
    let args = [
        FunctionArgument::Value {
            value_type: source,
            constant: None,
        },
        FunctionArgument::Value {
            value_type: FunctionValueType::new(DataType::Int64, false),
            constant: None,
        },
    ];
    let request = FunctionBindingRequest {
        arguments: &args,
        logical_argument_count: 1,
        expected_result_type: None,
    };
    let bound = catalog
        .resolve_bound_trusted(name, FunctionKind::Aggregate, request, &Compile)
        .unwrap();
    let selected = Arc::new(bound.selected);
    let context = novarocks_type_contract::RelationEffectContext::call(
        novarocks_type_contract::RelationCallId::new(641),
    );
    let state_context = ExpressionEffectContext {
        use_id: ExpressionUseId::new(644),
        region: EvaluationRegionId::new(70),
        demand: EvaluationDemand::Value,
    };
    let FunctionResultType::Scalar(state_type) = &selected.result_type else {
        unreachable!("array_agg selects a scalar result")
    };
    let uses = [
        Some(ExpressionUseId::new(642)),
        Some(ExpressionUseId::new(643)),
    ];
    let argument_uses = if phase.consumes_logical_arguments() {
        CallArgumentUses::SelectedChannels(&uses)
    } else {
        CallArgumentUses::AggregateMerge {
            phase,
            state_context: state_context.into(),
            state_input_type: state_type,
        }
    };
    let parameters = SemanticParameters::try_new([]).unwrap();
    let prepared = catalog.prepare_fresh_selected(
        CallEffectInput {
            function_id: &bound.function_id,
            kind: FunctionKind::Aggregate,
            selected: &selected,
            request,
            argument_uses,
            context: context.into(),
            parameters: &parameters,
            environment: &[],
            decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
            proof_scope: CallProofScope::Relation(context.identity),
        },
        selected.clone(),
        PureCallPreparation::Aggregate {
            arguments: ScopedRelationalEffects::pure_value(context),
            options: AggregatePreparationOptions {
                state_interpretation: receipt.then(|| {
                    Arc::new(novarocks_type_contract::AggregateStateInterpretation {
                        distinct: false,
                        order_keys:
                            novarocks_type_contract::AggregateStateOrderKeys::from_pure_boxed(
                                Box::new([novarocks_type_contract::AggregateStateOrderKey {
                                    ascending: true,
                                    nulls_first: false,
                                }]),
                            ),
                    })
                }),
                phase,
                distinct: false,
                order_keys: if phase.consumes_logical_arguments() {
                    Arc::from([AggregateOrderKey {
                        ascending: true,
                        nulls_first: false,
                    }])
                } else {
                    Arc::from([])
                },
                state_input_type: (!phase.consumes_logical_arguments())
                    .then_some(state_type.clone()),
            },
        },
        &Compile,
    )?;
    let PreparedPureKernel::Aggregate(handle) = prepared.prepared() else {
        panic!("ARRAY handle")
    };
    Ok(ArrayKernel {
        contract: handle.contract().clone(),
        operation: match name {
            "array_agg" => Operation::Array,
            "array_agg_distinct" => Operation::Distinct,
            "array_unique_agg" => Operation::Unique,
            _ => unreachable!(),
        },
        ascending: Box::new([true]),
        nulls_first: Box::new([false]),
    })
}
#[test]
fn array_ordered_single_rejects_merge_phases_and_legacy_receipts() {
    for phase in [
        AggregateKernelPhase::Partial,
        AggregateKernelPhase::Intermediate,
        AggregateKernelPhase::Final,
    ] {
        assert!(ordered_kernel_source("array_agg_distinct", source_type(), phase, false).is_err());
    }
    assert!(
        ordered_kernel_source(
            "array_agg_distinct",
            source_type(),
            AggregateKernelPhase::Single,
            true,
        )
        .is_err()
    );
}

// Complete installed-owner evidence preserves the original phases and order
// channels, rejects full-type drift, and keeps the primary control cause.
fn full_list(item: &FunctionValueType, nullable: bool) -> FunctionValueType {
    use novarocks_type_contract::{ValueKind, ValueLayout, ValueTypeRef, ValueTypeStorageLifetime};
    let storage = ValueTypeStorageLifetime::pure();
    let layout = DataType::List(Arc::new(Field::new(
        "item",
        item.data_type().clone(),
        item.nullable(),
    )));
    FunctionValueType::try_from_value(
        ValueTypeRef::try_new(
            ValueKind::List(item.value_type().clone()),
            nullable,
            storage.clone(),
        )
        .unwrap(),
        ValueLayout::try_new(&layout, storage).unwrap(),
    )
    .unwrap()
}
fn full_largeint(nullable: bool) -> FunctionValueType {
    FunctionValueType::try_with_logical_type(
        DataType::FixedSizeBinary(16),
        nullable,
        novarocks_type_contract::ValueLogicalType::LargeInt,
    )
    .unwrap()
}
#[test]
fn array_complete_largeint_installed_all_phases_preserve_original_state_and_order_channels() {
    let expected = full_list(&full_largeint(true), true);
    for name in ["array_agg", "array_agg_distinct"] {
        for nullable in [false, true] {
            for phase in [
                AggregateKernelPhase::Single,
                AggregateKernelPhase::Partial,
                AggregateKernelPhase::Intermediate,
                AggregateKernelPhase::Final,
            ] {
                let kernel = kernel_source(name, full_largeint(nullable), phase, true).unwrap();
                assert_eq!(kernel.contract.final_type(), &expected);
                assert_eq!(kernel.contract.intermediate_type(), &expected);
                let DataType::List(item) = expected.data_type() else {
                    panic!("ARRAY result")
                };
                assert!(item.metadata().as_arc().is_none());
                assert!(item.is_nullable());
            }
            let ordered = ordered_kernel_source(
                name,
                full_largeint(nullable),
                AggregateKernelPhase::Single,
                false,
            )
            .unwrap();
            assert_eq!(ordered.contract.final_type(), &expected);
            assert!(ordered.contract.merge_state().is_none());
            assert_eq!(
                ordered.contract.order_keys(),
                [AggregateOrderKey {
                    ascending: true,
                    nulls_first: false
                },]
            );
        }
    }
}
#[test]
fn array_complete_selected_ports_refuse_same_carrier_wrong_domain_child_null_and_ordered_state() {
    let setup = |operation,
                 arguments: &[FunctionArgumentType],
                 output: &FunctionValueType,
                 state: &FunctionValueType| {
        let mut work = novarocks_type_contract::CompileCheckpoints::try_new(
            &Compile,
            CompilePhase::FunctionSpecialization,
        )
        .unwrap();
        validate_full_selected_types(operation, arguments, output, Some(state), &mut work)
    };
    let actual = kernel_source(
        "array_agg",
        full_largeint(false),
        AggregateKernelPhase::Single,
        true,
    )
    .unwrap();
    let arguments = &actual.contract.call().selected().argument_types;
    let independent = full_list(&full_largeint(true), true);
    assert!(setup(Operation::Array, arguments, &independent, &independent).is_ok());
    for item in [
        FunctionValueType::new(DataType::FixedSizeBinary(16), true),
        FunctionValueType::try_with_logical_type(
            DataType::FixedSizeBinary(16),
            true,
            novarocks_type_contract::ValueLogicalType::Uuid,
        )
        .unwrap(),
    ] {
        let wrong = full_list(&item, true);
        assert_eq!(wrong.data_type(), independent.data_type());
        for (output, state) in [(&wrong, &independent), (&independent, &wrong)] {
            assert_eq!(
                setup(Operation::Array, arguments, output, state).unwrap_err(),
                invalid(
                    "ARRAY aggregate selected output or state differs from its original full signature"
                )
            );
        }
    }
    let wrong_child_null = full_list(&full_largeint(false), true);
    assert!(setup(Operation::Array, arguments, &wrong_child_null, &independent).is_err());
    assert!(setup(Operation::Array, arguments, &independent, &wrong_child_null).is_err());
    let wrong_root_null = independent.with_nullable(false).unwrap();
    assert!(setup(Operation::Array, arguments, &wrong_root_null, &independent).is_err());
    for child_nullable in [false, true] {
        let source = full_list(&full_largeint(child_nullable), false);
        let kernel = kernel_source(
            "array_agg",
            source.clone(),
            AggregateKernelPhase::Single,
            true,
        )
        .unwrap();
        let expected = full_list(&source.with_nullable(true).unwrap(), true);
        assert_eq!(kernel.contract.final_type(), &expected);
        let wrong_source_child = full_list(&full_largeint(!child_nullable), true);
        let wrong = full_list(&wrong_source_child, true);
        assert!(
            setup(
                Operation::Array,
                &kernel.contract.call().selected().argument_types,
                &wrong,
                &expected
            )
            .is_err()
        );
    }
    let ordered = ordered_kernel_source(
        "array_agg",
        full_largeint(true),
        AggregateKernelPhase::Single,
        false,
    )
    .unwrap();
    let order_arguments = &ordered.contract.call().selected().argument_types;
    let mut work = novarocks_type_contract::CompileCheckpoints::try_new(
        &Compile,
        CompilePhase::FunctionSpecialization,
    )
    .unwrap();
    assert!(
        validate_full_selected_types(
            Operation::Array,
            order_arguments,
            &independent,
            None,
            &mut work
        )
        .is_ok()
    );
    assert!(
        validate_full_selected_types(
            Operation::Array,
            order_arguments,
            &wrong_child_null,
            None,
            &mut work
        )
        .is_err()
    );
    assert!(
        setup(
            Operation::Array,
            order_arguments,
            &independent,
            &independent
        )
        .is_err()
    );
}
#[test]
fn array_complete_selected_port_control_is_primary_and_has_no_tail() {
    struct Trace {
        trace: Mutex<Vec<u32>>,
        refuse: Option<usize>,
    }
    impl PureCompileControl for Trace {
        fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
            assert_eq!(phase, CompilePhase::FunctionSpecialization);
            assert!(units <= 256);
            let mut trace = self.trace.lock().unwrap();
            let at = trace.len();
            trace.push(units);
            if self.refuse == Some(at) {
                Err(CompileControlError::Cancelled)
            } else {
                Ok(())
            }
        }
    }
    let kernel = ordered_kernel_source(
        "array_agg",
        full_largeint(true),
        AggregateKernelPhase::Single,
        false,
    )
    .unwrap();
    let execute = |control: &Trace| -> Result<(), KernelFailure> {
        let mut work = novarocks_type_contract::CompileCheckpoints::try_new(
            control,
            CompilePhase::FunctionSpecialization,
        )
        .map_err(compile_failure)?;
        validate_full_selected_types(
            Operation::Array,
            &kernel.contract.call().selected().argument_types,
            kernel.contract.final_type(),
            None,
            &mut work,
        )?;
        work.finish().map_err(compile_failure)
    };
    let success = Trace {
        trace: Mutex::new(Vec::new()),
        refuse: None,
    };
    execute(&success).unwrap();
    let trace = success.trace.into_inner().unwrap();
    for at in 0..trace.len() {
        let control = Trace {
            trace: Mutex::new(Vec::new()),
            refuse: Some(at),
        };
        assert_eq!(
            execute(&control).unwrap_err(),
            KernelFailure::CompileControl(CompileControlError::Cancelled)
        );
        assert_eq!(control.trace.into_inner().unwrap(), trace[..=at]);
    }
}
