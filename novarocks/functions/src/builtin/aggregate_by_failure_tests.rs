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

//! Actual selected BY refusal behavior; register as a cfg(test) aggregate_by child.
use super::*;
use crate::aggregate_scalar::AggScalarValue as V;
use arrow_array::StringArray;
use arrow_schema::{Field, Fields};
use novarocks_type_contract::{
    CallProofScope, CompileControlError, CompilePhase, DecimalOverflowPolicy, EvaluationDemand,
    EvaluationDomainId, ExpressionEffectContext, ExpressionUseId, PureCompileControl,
    SemanticParameters,
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
        panic!("MAX_BY/MIN_BY never waits")
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
        if let Some((stop, cause)) = &*self.refusal.lock().unwrap() {
            if *stop == at {
                return Err(cause.clone());
            }
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
fn kernel(
    name: &str,
    value: FunctionValueType,
    key: FunctionValueType,
    phase: AggregateKernelPhase,
) -> ByKernel {
    let catalog = super::super::catalogue::build_builtin_engine_function_catalog().unwrap();
    kernel_with_catalog(name, value, key, phase, &catalog)
}
fn kernel_with_catalog(
    name: &str,
    value: FunctionValueType,
    key: FunctionValueType,
    phase: AggregateKernelPhase,
    catalog: &EngineFunctionCatalog,
) -> ByKernel {
    let prepared = prepare_with_catalog(name, value, key, phase, catalog, false).unwrap();
    let PreparedPureKernel::Aggregate(handle) = prepared.prepared() else {
        panic!("aggregate handle")
    };
    ByKernel {
        contract: handle.contract().clone(),
        direction: if name == "max_by" {
            ByDirection::Maximum
        } else {
            ByDirection::Minimum
        },
    }
}
fn window_kernel_with_catalog(
    name: &str,
    value: FunctionValueType,
    key: FunctionValueType,
    catalog: &EngineFunctionCatalog,
) -> Arc<dyn PreparedWindowKernel> {
    let prepared = prepare_with_catalog(
        name,
        value,
        key,
        AggregateKernelPhase::Single,
        catalog,
        true,
    )
    .unwrap();
    let PreparedPureKernel::Window(handle) = prepared.prepared() else {
        panic!("window handle")
    };
    handle.clone()
}
fn prepare_with_catalog(
    name: &str,
    value: FunctionValueType,
    key: FunctionValueType,
    phase: AggregateKernelPhase,
    catalog: &EngineFunctionCatalog,
    window: bool,
) -> Result<PureCallSpecialization, FunctionSpecializationFailure> {
    let args = [
        FunctionArgument::Value {
            value_type: value,
            constant: None,
        },
        FunctionArgument::Value {
            value_type: key,
            constant: None,
        },
    ];
    let request = FunctionBindingRequest {
        arguments: &args,
        logical_argument_count: 2,
        expected_result_type: None,
    };
    let bound = catalog
        .resolve_bound_user(name, FunctionKind::Aggregate, request, &Compile)
        .unwrap();
    let selected = Arc::new(bound.selected);
    let context = ExpressionEffectContext {
        use_id: ExpressionUseId::new(431),
        domain: EvaluationDomainId::new(29),
        demand: EvaluationDemand::Value,
    };
    let state_context = ExpressionEffectContext {
        use_id: ExpressionUseId::new(434),
        domain: EvaluationDomainId::new(30),
        demand: EvaluationDemand::Value,
    };
    let state_type = FunctionValueType::new(DataType::Binary, true);
    let uses = [
        Some(ExpressionUseId::new(432)),
        Some(ExpressionUseId::new(433)),
    ];
    let argument_uses = if phase.consumes_logical_arguments() {
        CallArgumentUses::SelectedChannels(&uses)
    } else {
        CallArgumentUses::AggregateMerge {
            phase,
            state_context,
            state_input_type: &state_type,
        }
    };
    let parameters = SemanticParameters::try_new([]).unwrap();
    let options = AggregatePreparationOptions {
        state_interpretation: None,
        phase,
        distinct: false,
        order_keys: Arc::from([]),
        state_input_type: (!phase.consumes_logical_arguments()).then_some(state_type.clone()),
    };
    let preparation = if window {
        PureCallPreparation::AggregateWindow {
            arguments: ScopedExpressionEffects::pure_value(context),
            options: AggregateWindowPreparationOptions {
                aggregate: options,
                window: WindowCallOptions::try_new(None, false, &Compile).unwrap(),
            },
        }
    } else {
        PureCallPreparation::Aggregate {
            arguments: ScopedExpressionEffects::pure_value(context),
            options,
        }
    };
    catalog.prepare_fresh_selected(
        CallEffectInput {
            function_id: &bound.function_id,
            kind: FunctionKind::Aggregate,
            selected: &selected,
            request,
            argument_uses,
            context,
            parameters: &parameters,
            environment: &[],
            decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
            proof_scope: CallProofScope::Domain(context.domain),
        },
        selected.clone(),
        preparation,
        &Compile,
    )
}

fn text(value: &str) -> ArrayRef {
    Arc::new(StringArray::from(vec![value]))
}
fn nested() -> (FunctionValueType, ArrayRef) {
    let ty = DataType::Struct(Fields::from(vec![
        Field::new("first", DataType::Utf8, false),
        Field::new("second", DataType::Utf8, true),
    ]));
    let values = scalar::build_scalar_array(
        &ty,
        vec![Some(V::Struct(vec![
            Some(V::Utf8("a".into())),
            Some(V::Utf8("z".repeat(513))),
        ]))],
        &mut ScalarWork::new(None),
    )
    .unwrap();
    (FunctionValueType::new(ty, false), values)
}
fn text_type() -> FunctionValueType {
    FunctionValueType::new(DataType::Utf8, false)
}
fn new_state(kernel: &ByKernel, host: Arc<Host>) -> ByState<HostAggregateAllocator> {
    kernel
        .create_state_with_allocator(Some(host), &Control::default())
        .unwrap()
}
fn update(
    kernel: &ByKernel,
    state: &mut ByState<HostAggregateAllocator>,
    values: &ArrayRef,
    keys: &ArrayRef,
    control: &dyn KernelEvaluationControl,
) -> Result<(), KernelFailure> {
    let args = [
        EvaluatedArgument::Column(values),
        EvaluatedArgument::Column(keys),
    ];
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
    kernel: &ByKernel,
    state: &mut ByState<HostAggregateAllocator>,
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
fn assert_failed(kernel: &ByKernel, state: &ByState<HostAggregateAllocator>) {
    assert!(state.failed);
    assert!(state.key.is_none());
    assert!(state.value.is_none());
    assert_eq!(
        kernel.retained_bytes(state),
        state.allocator.metadata_bytes()
    );
    assert!(matches!(
        kernel.build_final(std::iter::once(state), &Control::default()),
        Err(KernelFailure::InstanceFailed)
    ));
}
#[test]
fn by_missing_host_and_every_state_construction_refusal_keep_exact_typed_cause() {
    for name in ["max_by", "min_by"] {
        let kernel = kernel(name, text_type(), text_type(), AggregateKernelPhase::Single);
        for absent in [
            kernel.create_state(&Control::default()),
            kernel.create_state_with_allocator(None, &Control::default()),
        ] {
            assert!(
                matches!(absent,Err(KernelFailure::InvalidProgram(error)) if error.message()=="allocation-tracked max_by/min_by requires a host allocator")
            );
        }
        for cause in causes() {
            for stop in [0, 1] {
                let host = Arc::new(Host::default());
                let control = Control {
                    refusal: Some((stop, cause.clone())),
                    ..Control::default()
                };
                assert!(
                    matches!(kernel.create_state_with_allocator(Some(host.clone()),&control),Err(error) if error==cause)
                );
                assert_released(&host);
            }
        }
    }
}
#[test]
fn by_losing_and_tied_keys_still_allocate_full_value_before_comparison() {
    for name in ["max_by", "min_by"] {
        let kernel = kernel(name, text_type(), text_type(), AggregateKernelPhase::Single);
        for key in [if name == "max_by" { "a" } else { "z" }, "mm"] {
            let host = Arc::new(Host::default());
            let mut state = new_state(&kernel, host.clone());
            let initial = host.ledger.lock().unwrap().attempts;
            let metadata = state.allocator.metadata_bytes();
            update(
                &kernel,
                &mut state,
                &text("aa"),
                &text("mm"),
                &Control::default(),
            )
            .unwrap();
            assert_eq!(host.ledger.lock().unwrap().attempts, initial + 2);
            update(
                &kernel,
                &mut state,
                &text("012345678"),
                &text(key),
                &Control::default(),
            )
            .unwrap();
            let ledger = host.ledger.lock().unwrap();
            assert_eq!(ledger.attempts, initial + 4);
            assert_eq!(ledger.bytes, metadata + 4);
            assert_eq!(ledger.peak, metadata + 4 + key.len() + 9);
            drop(ledger);
            assert_eq!(kernel.retained_bytes(&state), metadata + 4);
            let out = kernel
                .build_final(std::iter::once(&state), &Control::default())
                .unwrap();
            assert_eq!(
                out.as_any().downcast_ref::<StringArray>().unwrap().value(0),
                "aa"
            );
            drop(state);
            assert_released(&host);
        }
    }
}
#[test]
fn by_losing_or_tied_key_value_actual_refusal_releases_and_failed_latch_is_permanent() {
    for name in ["max_by", "min_by"] {
        let kernel = kernel(name, text_type(), text_type(), AggregateKernelPhase::Single);
        for key in [if name == "max_by" { "a" } else { "z" }, "mm"] {
            for stop in [0, 1] {
                for cause in causes() {
                    let host = Arc::new(Host::default());
                    let mut state = new_state(&kernel, host.clone());
                    update(
                        &kernel,
                        &mut state,
                        &text("aa"),
                        &text("mm"),
                        &Control::default(),
                    )
                    .unwrap();
                    arm_refusal(&host, stop, cause.clone());
                    assert_eq!(
                        update(
                            &kernel,
                            &mut state,
                            &text("012345678"),
                            &text(key),
                            &Control::default()
                        ),
                        Err(cause)
                    );
                    assert_failed(&kernel, &state);
                    assert_metadata_only(&host);
                    let attempts = host.ledger.lock().unwrap().attempts;
                    assert_eq!(
                        update(
                            &kernel,
                            &mut state,
                            &text("retry"),
                            &text("mm"),
                            &Control::default()
                        ),
                        Err(KernelFailure::InstanceFailed)
                    );
                    assert_eq!(host.ledger.lock().unwrap().attempts, attempts);
                    drop(state);
                    assert_released(&host);
                }
            }
        }
    }
}
fn nested_wire(name: &str) -> (FunctionValueType, ArrayRef) {
    let (source, values) = nested();
    let partial = kernel(
        name,
        source.clone(),
        text_type(),
        AggregateKernelPhase::Partial,
    );
    let host = Arc::new(Host::default());
    let mut state = new_state(&partial, host.clone());
    update(
        &partial,
        &mut state,
        &values,
        &text("m"),
        &Control::default(),
    )
    .unwrap();
    let wire = partial
        .build_intermediate(std::iter::once(&state), &Control::default())
        .unwrap();
    drop(state);
    assert_released(&host);
    (source, wire)
}
#[test]
fn by_recursive_decode_actual_refusal_restores_each_original_cause_and_partial_graph() {
    for name in ["max_by", "min_by"] {
        let (source, wire) = nested_wire(name);
        let kernel = kernel(name, source, text_type(), AggregateKernelPhase::Final);
        for stop in 0..4 {
            for cause in causes() {
                let host = Arc::new(Host::default());
                let mut state = new_state(&kernel, host.clone());
                let initial = host.ledger.lock().unwrap().attempts;
                arm_refusal(&host, stop, cause.clone());
                assert_eq!(
                    merge(&kernel, &mut state, &wire, &Control::default()),
                    Err(cause)
                );
                assert_failed(&kernel, &state);
                assert_eq!(host.ledger.lock().unwrap().attempts, initial + stop + 1);
                assert_metadata_only(&host);
                drop(state);
                assert_released(&host);
            }
        }
        let valid = wire.as_any().downcast_ref::<BinaryArray>().unwrap();
        let mut malformed = valid.value(0).to_vec();
        malformed.push(99);
        let malformed: ArrayRef = Arc::new(BinaryArray::from(vec![Some(malformed.as_slice())]));
        let host = Arc::new(Host::default());
        let mut state = new_state(&kernel, host.clone());
        let initial = host.ledger.lock().unwrap().attempts;
        assert!(
            matches!(merge(&kernel,&mut state,&malformed,&Control::default()),Err(KernelFailure::Operational(error)) if error.message()=="max_by/min_by merge input has trailing bytes")
        );
        assert_eq!(host.ledger.lock().unwrap().attempts, initial + 4);
        assert_failed(&kernel, &state);
        assert_metadata_only(&host);
        drop(state);
        assert_released(&host);
    }
}
#[test]
fn by_every_serialization_temporary_allocation_refusal_releases_only_the_temp() {
    for name in ["max_by", "min_by"] {
        let (source, values) = nested();
        let kernel = kernel(name, source, text_type(), AggregateKernelPhase::Partial);
        let host = Arc::new(Host::default());
        let mut state = new_state(&kernel, host.clone());
        update(
            &kernel,
            &mut state,
            &values,
            &text("m"),
            &Control::default(),
        )
        .unwrap();
        let initial = host.ledger.lock().unwrap().attempts;
        let stable = host.ledger.lock().unwrap().bytes;
        kernel
            .build_intermediate(std::iter::once(&state), &Control::default())
            .unwrap();
        let allocations = host.ledger.lock().unwrap().attempts - initial;
        assert!(allocations > 0);
        assert_eq!(host.ledger.lock().unwrap().bytes, stable);
        drop(state);
        assert_released(&host);
        for stop in 0..allocations {
            for cause in causes() {
                let host = Arc::new(Host::default());
                let mut state = new_state(&kernel, host.clone());
                update(
                    &kernel,
                    &mut state,
                    &values,
                    &text("m"),
                    &Control::default(),
                )
                .unwrap();
                arm_refusal(&host, stop, cause.clone());
                assert!(
                    matches!(kernel.build_intermediate(std::iter::once(&state),&Control::default()),Err(error) if error==cause)
                );
                assert!(!state.failed);
                assert_eq!(kernel.retained_bytes(&state), stable);
                assert_eq!(host.ledger.lock().unwrap().bytes, stable);
                drop(state);
                assert_released(&host);
            }
        }
    }
}
#[test]
fn by_every_update_merge_serialize_and_final_checkpoint_preserves_original_cause() {
    for name in ["max_by", "min_by"] {
        let (source, values) = nested();
        let partial = kernel(
            name,
            source.clone(),
            text_type(),
            AggregateKernelPhase::Partial,
        );
        let final_kernel = kernel(name, source, text_type(), AggregateKernelPhase::Final);
        let host = Arc::new(Host::default());
        let mut first = new_state(&partial, host.clone());
        let control = Control::default();
        update(&partial, &mut first, &values, &text("m"), &control).unwrap();
        let update_steps = control.trace.lock().unwrap().len();
        assert!(control.trace.lock().unwrap().contains(&256));
        let control = Control::default();
        let wire = partial
            .build_intermediate(std::iter::once(&first), &control)
            .unwrap();
        let serialize_steps = control.trace.lock().unwrap().len();
        assert!(control.trace.lock().unwrap().contains(&256));
        let control = Control::default();
        partial
            .build_final(std::iter::once(&first), &control)
            .unwrap();
        let final_steps = control.trace.lock().unwrap().len();
        assert!(control.trace.lock().unwrap().contains(&256));
        let mut merged = new_state(&final_kernel, host.clone());
        let control = Control::default();
        merge(&final_kernel, &mut merged, &wire, &control).unwrap();
        let merge_steps = control.trace.lock().unwrap().len();
        assert!(control.trace.lock().unwrap().contains(&256));
        let retained = partial.retained_bytes(&first);
        assert!(retained > 0);
        drop(merged);
        assert_eq!(host.ledger.lock().unwrap().bytes, retained);
        for operation in 0..4 {
            let steps = [update_steps, merge_steps, serialize_steps, final_steps][operation];
            for stop in 0..steps {
                for cause in causes() {
                    let control = Control {
                        refusal: Some((stop, cause.clone())),
                        ..Control::default()
                    };
                    if operation < 2 {
                        let host = Arc::new(Host::default());
                        let kernel = if operation == 0 {
                            &partial
                        } else {
                            &final_kernel
                        };
                        let mut fresh = new_state(kernel, host.clone());
                        let error = if operation == 0 {
                            update(kernel, &mut fresh, &values, &text("m"), &control)
                        } else {
                            merge(kernel, &mut fresh, &wire, &control)
                        };
                        assert_eq!(error, Err(cause));
                        assert_failed(kernel, &fresh);
                        assert_metadata_only(&host);
                        drop(fresh);
                        assert_released(&host);
                    } else {
                        let error = if operation == 2 {
                            partial.build_intermediate(std::iter::once(&first), &control)
                        } else {
                            partial.build_final(std::iter::once(&first), &control)
                        };
                        assert!(matches!(error,Err(error) if error==cause));
                        assert!(!first.failed);
                        assert_eq!(host.ledger.lock().unwrap().bytes, retained);
                    }
                }
            }
        }
        drop(first);
        assert_released(&host);
    }
}

#[test]
fn by_actual_metadata_constructor_refusal_is_typed_and_never_published() {
    for name in ["min_by", "max_by"] {
        let kernel = kernel(name, text_type(), text_type(), AggregateKernelPhase::Single);
        for cause in causes() {
            let host = Arc::new(Host::default());
            arm_refusal(&host, 0, cause.clone());
            assert!(
                matches!(kernel.create_state_with_allocator(Some(host.clone()), &Control::default()), Err(error) if error == cause)
            );
            assert_eq!(host.ledger.lock().unwrap().attempts, 1);
            assert_released(&host);
        }
    }
}

#[path = "aggregate_by_window_tests.rs"]
mod window_tests;
