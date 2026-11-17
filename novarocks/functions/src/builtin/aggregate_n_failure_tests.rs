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

//! Actual selected N refusal behavior; register as a cfg(test) aggregate_n child.
use super::*;
use arrow_array::{Int64Array, StringArray};
use novarocks_type_contract::{
    CallProofScope, CompileControlError, CompilePhase, DecimalOverflowPolicy, EvaluationDemand,
    EvaluationRegionId, ExpressionEffectContext, ExpressionUseId, PureCompileControl,
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
        panic!("MIN_N/MAX_N never waits")
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
fn kernel(
    name: &str,
    value: FunctionValueType,
    key: FunctionValueType,
    phase: AggregateKernelPhase,
) -> NKernel {
    let catalog = super::super::catalogue::build_builtin_engine_function_catalog().unwrap();
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
    let context = novarocks_type_contract::RelationEffectContext::call(
        novarocks_type_contract::RelationCallId::new(431),
    );
    let state_context = ExpressionEffectContext {
        use_id: ExpressionUseId::new(434),
        region: EvaluationRegionId::new(30),
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
            state_context: state_context.into(),
            state_input_type: &state_type,
        }
    };
    let parameters = SemanticParameters::try_new([]).unwrap();
    let prepared = catalog
        .prepare_fresh_selected(
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
                    state_interpretation: None,
                    phase,
                    distinct: false,
                    order_keys: Arc::from([]),
                    state_input_type: (!phase.consumes_logical_arguments())
                        .then_some(state_type.clone()),
                },
            },
            &Compile,
        )
        .unwrap();
    let PreparedPureKernel::Aggregate(handle) = prepared.prepared() else {
        panic!("aggregate handle")
    };
    NKernel {
        contract: handle.contract().clone(),
        keep_smallest: name == "min_n",
    }
}
fn text(value: &str) -> ArrayRef {
    Arc::new(StringArray::from(vec![value]))
}
fn text_type() -> FunctionValueType {
    FunctionValueType::new(DataType::Utf8, false)
}
fn new_state(kernel: &NKernel, host: Arc<Host>) -> NState<HostAggregateAllocator> {
    kernel
        .create_state_with_allocator(Some(host), &Control::default())
        .unwrap()
}
fn update(
    kernel: &NKernel,
    state: &mut NState<HostAggregateAllocator>,
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
    kernel: &NKernel,
    state: &mut NState<HostAggregateAllocator>,
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
fn assert_failed(kernel: &NKernel, state: &NState<HostAggregateAllocator>) {
    assert!(state.failed);
    assert!(state.values.is_empty());
    assert_eq!(
        kernel.retained_bytes(state),
        state.allocator.metadata_bytes()
    );
    assert!(matches!(
        kernel.build_final(std::iter::once(state), &Control::default()),
        Err(KernelFailure::InstanceFailed)
    ));
}

fn limit() -> ArrayRef {
    Arc::new(Int64Array::from(vec![7]))
}
fn limit_type() -> FunctionValueType {
    FunctionValueType::new(DataType::Int64, false)
}
#[test]
fn n_missing_host_and_construction_control_refusal_keep_actual_cause() {
    for name in ["min_n", "max_n"] {
        let kernel = kernel(
            name,
            text_type(),
            limit_type(),
            AggregateKernelPhase::Single,
        );
        for missing in [
            kernel.create_state(&Control::default()),
            kernel.create_state_with_allocator(None, &Control::default()),
        ] {
            assert!(
                matches!(missing,Err(KernelFailure::InvalidProgram(e)) if e.message()=="allocation-tracked min_n/max_n requires a host allocator")
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
                    matches!(kernel.create_state_with_allocator(Some(host.clone()),&control),Err(e) if e==cause)
                );
                assert_released(&host);
            }
        }
    }
}
#[test]
fn n_every_actual_update_allocation_refusal_releases_child_and_root_and_latches() {
    for name in ["min_n", "max_n"] {
        let kernel = kernel(
            name,
            text_type(),
            limit_type(),
            AggregateKernelPhase::Partial,
        );
        let host = Arc::new(Host::default());
        let mut state = new_state(&kernel, host.clone());
        let initial = host.ledger.lock().unwrap().attempts;
        update(
            &kernel,
            &mut state,
            &text(&"x".repeat(513)),
            &limit(),
            &Control::default(),
        )
        .unwrap();
        let allocations = host.ledger.lock().unwrap().attempts - initial;
        assert!(allocations >= 2);
        drop(state);
        assert_released(&host);
        for stop in 0..allocations {
            for cause in causes() {
                let host = Arc::new(Host::default());
                let mut state = new_state(&kernel, host.clone());
                arm_refusal(&host, stop, cause.clone());
                assert_eq!(
                    update(
                        &kernel,
                        &mut state,
                        &text(&"x".repeat(513)),
                        &limit(),
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
                        &limit(),
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
#[test]
fn n_every_decode_and_serialization_allocation_refusal_keeps_typed_journal_and_rollback() {
    for name in ["min_n", "max_n"] {
        let partial = kernel(
            name,
            text_type(),
            limit_type(),
            AggregateKernelPhase::Partial,
        );
        let final_kernel = kernel(name, text_type(), limit_type(), AggregateKernelPhase::Final);
        let host = Arc::new(Host::default());
        let mut state = new_state(&partial, host.clone());
        update(
            &partial,
            &mut state,
            &text(&"x".repeat(513)),
            &limit(),
            &Control::default(),
        )
        .unwrap();
        let initial = host.ledger.lock().unwrap().attempts;
        let retained = partial.retained_bytes(&state);
        let wire = partial
            .build_intermediate(std::iter::once(&state), &Control::default())
            .unwrap();
        let serial_allocations = host.ledger.lock().unwrap().attempts - initial;
        assert!(serial_allocations > 0);
        drop(state);
        assert_released(&host);
        for stop in 0..serial_allocations {
            for cause in causes() {
                let host = Arc::new(Host::default());
                let mut state = new_state(&partial, host.clone());
                update(
                    &partial,
                    &mut state,
                    &text(&"x".repeat(513)),
                    &limit(),
                    &Control::default(),
                )
                .unwrap();
                arm_refusal(&host, stop, cause.clone());
                assert!(
                    matches!(partial.build_intermediate(std::iter::once(&state),&Control::default()),Err(e) if e==cause)
                );
                assert!(!state.failed);
                assert_eq!(partial.retained_bytes(&state), retained);
                assert_eq!(host.ledger.lock().unwrap().bytes, retained);
                drop(state);
                assert_released(&host);
            }
        }
        let host = Arc::new(Host::default());
        let mut state = new_state(&final_kernel, host.clone());
        let initial = host.ledger.lock().unwrap().attempts;
        merge(&final_kernel, &mut state, &wire, &Control::default()).unwrap();
        let allocations = host.ledger.lock().unwrap().attempts - initial;
        assert!(allocations >= 3);
        drop(state);
        assert_released(&host);
        for stop in 0..allocations {
            for cause in causes() {
                let host = Arc::new(Host::default());
                let mut state = new_state(&final_kernel, host.clone());
                arm_refusal(&host, stop, cause.clone());
                assert_eq!(
                    merge(&final_kernel, &mut state, &wire, &Control::default()),
                    Err(cause)
                );
                assert_failed(&final_kernel, &state);
                assert_metadata_only(&host);
                drop(state);
                assert_released(&host);
            }
        }
    }
}
#[test]
fn n_every_update_merge_serialize_final_checkpoint_is_bounded_and_preserves_cause() {
    for name in ["min_n", "max_n"] {
        let partial = kernel(
            name,
            text_type(),
            limit_type(),
            AggregateKernelPhase::Partial,
        );
        let final_kernel = kernel(name, text_type(), limit_type(), AggregateKernelPhase::Final);
        let value = text(&"x".repeat(513));
        let host = Arc::new(Host::default());
        let mut state = new_state(&partial, host.clone());
        let control = Control::default();
        update(&partial, &mut state, &value, &limit(), &control).unwrap();
        let update_steps = control.trace.lock().unwrap().len();
        assert!(control.trace.lock().unwrap().contains(&256));
        let control = Control::default();
        let wire = partial
            .build_intermediate(std::iter::once(&state), &control)
            .unwrap();
        let serial_steps = control.trace.lock().unwrap().len();
        let control = Control::default();
        partial
            .build_final(std::iter::once(&state), &control)
            .unwrap();
        let final_steps = control.trace.lock().unwrap().len();
        let mut merged = new_state(&final_kernel, host.clone());
        let control = Control::default();
        merge(&final_kernel, &mut merged, &wire, &control).unwrap();
        let merge_steps = control.trace.lock().unwrap().len();
        drop(merged);
        let retained = host.ledger.lock().unwrap().bytes;
        for operation in 0..4 {
            for stop in 0..[update_steps, merge_steps, serial_steps, final_steps][operation] {
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
                        let mut state = new_state(kernel, host.clone());
                        let result = if operation == 0 {
                            update(kernel, &mut state, &value, &limit(), &control)
                        } else {
                            merge(kernel, &mut state, &wire, &control)
                        };
                        assert_eq!(result, Err(cause));
                        assert_failed(kernel, &state);
                        assert_metadata_only(&host);
                        drop(state);
                        assert_released(&host);
                    } else {
                        let result = if operation == 2 {
                            partial.build_intermediate(std::iter::once(&state), &control)
                        } else {
                            partial.build_final(std::iter::once(&state), &control)
                        };
                        assert!(matches!(result,Err(e) if e==cause));
                        assert!(!state.failed);
                        assert_eq!(host.ledger.lock().unwrap().bytes, retained);
                    }
                }
            }
        }
        drop(state);
        assert_released(&host);
    }
}

#[test]
fn n_actual_metadata_constructor_refusal_is_typed_and_never_published() {
    for name in ["min_n", "max_n"] {
        let kernel = kernel(
            name,
            text_type(),
            limit_type(),
            AggregateKernelPhase::Single,
        );
        for cause in causes() {
            let host = Arc::new(Host::default());
            arm_refusal(&host, 0, cause.clone());
            assert!(
                matches!(kernel.create_state_with_allocator(Some(host.clone()),&Control::default()),Err(error) if error==cause)
            );
            assert_eq!(host.ledger.lock().unwrap().attempts, 1);
            assert_released(&host);
        }
    }
}
