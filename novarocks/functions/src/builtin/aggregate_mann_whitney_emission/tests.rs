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
use super::super::aggregate_mann_whitney_core::{MannWhitneyInput, MannWhitneyState};
use super::super::aggregate_mann_whitney_resources::MannSourceParams;
use super::*;
use crate::opaque_memory::OpaqueAllocationHost;
use crate::source_custody_resources_tests::{measured, measured_live_bytes, without_recording};
use crate::*;
use arrow_array::{ArrayRef, BooleanArray, Int64Array, StringArray};
use arrow_schema::DataType;
use novarocks_type_contract::{
    ArgumentControl, CallEffects, CallProofScope, CompileControlError, CompilePhase,
    DecimalOverflowPolicy, EvaluationDemand, EvaluationDomainId, ExpressionEffectContext,
    ExpressionUseId, FunctionEffectDeclaration, FunctionInstanceState, FunctionNullBehavior,
    ObservableEffects, PureCompileControl, SemanticParameters,
};
use std::{
    alloc::Layout,
    ptr::NonNull,
    sync::{
        Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

struct Compile;
impl PureCompileControl for Compile {
    fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
        Ok(())
    }
}
// This fixture uses the actual catalogue binding without installing an owner.
// Its effect witness only constructs the Data-domain identity under test.
struct Effects {
    id: FunctionId,
    selected: Arc<FunctionBindingSelection>,
    declaration: FunctionEffectDeclaration,
}
impl FunctionEffectOwner for Effects {
    type Error = FunctionBindingError;
    fn declaration(
        &self,
        id: &FunctionId,
        selected: &FunctionBindingSelection,
    ) -> Result<&FunctionEffectDeclaration, Self::Error> {
        if id != &self.id || selected != self.selected.as_ref() {
            return Err(FunctionBindingError::UnknownFunction);
        }
        Ok(&self.declaration)
    }
    fn validate_and_refine(
        &self,
        input: CallEffectInput<'_>,
        _: &dyn PureCompileControl,
    ) -> Result<CallEffects, FunctionEffectOwnerError<Self::Error>> {
        self.declaration(input.function_id, input.selected)?;
        Ok(CallEffects {
            value_stability: self.declaration.value_stability,
            own_row_error: self.declaration.own_row_error,
            failure_behavior: self.declaration.failure_behavior,
            null_behavior: self.declaration.null_behavior,
            argument_control: self.declaration.argument_control,
            instance_state: self.declaration.instance_state,
            observable_effects: self.declaration.observable_effects,
            environment: Box::new([]),
            proof_scope: input.proof_scope,
        })
    }
}
fn contract(phase: AggregateKernelPhase) -> Arc<AggregateCallContract> {
    let catalog = super::super::catalogue::build_builtin_engine_function_catalog().unwrap();
    let arguments: Vec<_> = [DataType::Int64, DataType::Boolean, DataType::Utf8]
        .into_iter()
        .map(|data_type| FunctionArgument::Value {
            value_type: FunctionValueType::new(data_type, true),
            constant: None,
        })
        .collect();
    let request = FunctionBindingRequest {
        arguments: &arguments,
        logical_argument_count: 3,
        expected_result_type: None,
    };
    let bound = catalog
        .resolve_bound_user(
            "mann_whitney_u_test",
            FunctionKind::Aggregate,
            request,
            &Compile,
        )
        .unwrap();
    let selected = Arc::new(bound.selected);
    let owner = Effects {
        id: bound.function_id,
        selected: selected.clone(),
        declaration: FunctionEffectDeclaration {
            value_stability: FunctionVolatility::Immutable,
            own_row_error: FunctionIntrinsicRowError::NotRowEvaluated,
            failure_behavior: FunctionFailureBehavior::Propagate,
            null_behavior: FunctionNullBehavior::CalledOnNull,
            argument_control: ArgumentControl::Aggregate,
            instance_state: FunctionInstanceState::AggregateInstance,
            observable_effects: ObservableEffects::NONE,
            environment_dependencies: Box::new([]),
        },
    };
    let uses = [
        Some(ExpressionUseId::new(2)),
        Some(ExpressionUseId::new(3)),
        Some(ExpressionUseId::new(4)),
    ];
    let context = ExpressionEffectContext {
        use_id: ExpressionUseId::new(1),
        domain: EvaluationDomainId::new(11),
        demand: EvaluationDemand::Value,
    };
    let params = SemanticParameters::default();
    let input = CallEffectInput {
        function_id: &owner.id,
        kind: FunctionKind::Aggregate,
        selected: &selected,
        request,
        argument_uses: CallArgumentUses::SelectedChannels(&uses),
        context,
        parameters: &params,
        environment: &[],
        decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
        proof_scope: CallProofScope::Unconditional,
    };
    let receipt = refine_call_effects(&owner, input, &Compile).unwrap();
    let call = Arc::new(
        FunctionCallContract::from_refined(input, &receipt, selected.clone(), &Compile).unwrap(),
    );
    Arc::new(
        AggregateCallContract::try_new(call, phase, false, Arc::from([]), None, &Compile).unwrap(),
    )
}
#[derive(Default)]
struct Ledger {
    opaque: usize,
    opaque_calls: usize,
    typed_calls: usize,
    live: Vec<(usize, Layout)>,
    reservations: Vec<usize>,
    released: Vec<usize>,
}
#[derive(Default)]
struct Host {
    ledger: Mutex<Ledger>,
    opaque_refusal: Mutex<Option<(usize, KernelFailure)>>,
    typed_refusal: Mutex<Option<(usize, KernelFailure)>>,
    assert_global_clear: AtomicBool,
}
impl AggregateStateAllocator for Host {
    fn opaque_allocation_host(&self) -> Option<&dyn OpaqueAllocationHost> {
        Some(self)
    }
    fn allocate(&self, layout: Layout) -> Result<NonNull<u8>, KernelFailure> {
        let at = without_recording(|| {
            let mut ledger = self.ledger.lock().unwrap();
            let at = ledger.typed_calls;
            ledger.typed_calls += 1;
            at
        });
        if let Some((stop, cause)) = &*self.typed_refusal.lock().unwrap() {
            assert!(at <= *stop, "typed allocation after refusal");
            if at == *stop {
                return Err(cause.clone());
            }
        }
        let pointer = without_recording(|| NonNull::new(unsafe { std::alloc::alloc(layout) }))
            .ok_or(KernelFailure::ResourceExhausted)?;
        without_recording(|| {
            self.ledger
                .lock()
                .unwrap()
                .live
                .push((pointer.as_ptr().addr(), layout))
        });
        Ok(pointer)
    }
    unsafe fn release(&self, pointer: NonNull<u8>, layout: Layout) {
        without_recording(|| {
            let mut ledger = self.ledger.lock().unwrap();
            let index = ledger
                .live
                .iter()
                .position(|(address, actual)| {
                    *address == pointer.as_ptr().addr() && *actual == layout
                })
                .expect("exact typed block released once");
            ledger.live.swap_remove(index);
            unsafe { std::alloc::dealloc(pointer.as_ptr(), layout) };
        });
    }
}
impl OpaqueAllocationHost for Host {
    fn reserve_opaque(&self, bytes: usize) -> Result<(), KernelFailure> {
        let at = without_recording(|| {
            let mut ledger = self.ledger.lock().unwrap();
            let at = ledger.opaque_calls;
            ledger.opaque_calls += 1;
            at
        });
        if let Some((stop, cause)) = &*self.opaque_refusal.lock().unwrap() {
            assert!(at <= *stop, "opaque admission after refusal");
            if at == *stop {
                return Err(cause.clone());
            }
        }
        without_recording(|| {
            let mut ledger = self.ledger.lock().unwrap();
            ledger.opaque += bytes;
            ledger.reservations.push(bytes);
        });
        Ok(())
    }
    fn release_opaque(&self, bytes: usize) {
        if self.assert_global_clear.load(Ordering::SeqCst) {
            assert_eq!(
                measured_live_bytes(),
                0,
                "original Global backing still live at final emission release"
            );
        }
        without_recording(|| {
            let mut ledger = self.ledger.lock().unwrap();
            ledger.opaque = ledger.opaque.checked_sub(bytes).unwrap();
            ledger.released.push(bytes);
        });
    }
}
#[derive(Default)]
struct Control {
    trace: Mutex<Vec<u32>>,
    refusal: Option<(usize, KernelFailure)>,
}
impl KernelEvaluationControl for Control {
    fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
        let at = without_recording(|| {
            let mut trace = self.trace.lock().unwrap();
            let at = trace.len();
            trace.push(units);
            at
        });
        if let Some((stop, cause)) = &self.refusal {
            assert!(at <= *stop, "control footer after refusal");
            if at == *stop {
                return Err(cause.clone());
            }
        }
        Ok(())
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        unreachable!()
    }
}
fn causes() -> [KernelFailure; 7] {
    [
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
        invalid("host refusal"),
        internal("host refusal"),
        KernelFailure::Operational(crate::KernelDiagnostic::new("host refusal")),
        KernelFailure::InstanceFailed,
    ]
}

struct State {
    core: std::cell::RefCell<MannWhitneyState<HostAggregateAllocator>>,
    visits: std::cell::Cell<usize>,
    renders: std::cell::Cell<usize>,
}
impl MannEmissionAccess for State {
    fn validate_host(&self, host: &Arc<dyn AggregateStateAllocator>) -> Result<(), KernelFailure> {
        let core = self
            .core
            .try_borrow()
            .map_err(|_| internal("test state borrow conflict"))?;
        if !core.allocator.has_host_authority(host) {
            return Err(invalid("test state emission has another host authority"));
        }
        Ok(())
    }
    fn extents(&self) -> Result<MannStateExtents, KernelFailure> {
        self.visits.set(self.visits.get() + 1);
        let core = self
            .core
            .try_borrow()
            .map_err(|_| internal("test state borrow conflict"))?;
        Ok(core.extents())
    }
    fn with_core(
        &self,
        operation: impl FnOnce(
            &mut MannWhitneyState<HostAggregateAllocator>,
        ) -> Result<(), ScalarStateError>,
    ) -> Result<(), ScalarStateError> {
        self.renders.set(self.renders.get() + 1);
        let mut core = self
            .core
            .try_borrow_mut()
            .map_err(|_| internal("test state borrow conflict"))?;
        operation(&mut core)
    }
}
fn new_host() -> Arc<Host> {
    let host = Arc::new(Host::default());
    // Warm test bookkeeping exclusively before recording the real operation.
    drop(host.ledger.lock().unwrap());
    drop(host.opaque_refusal.lock().unwrap());
    drop(host.typed_refusal.lock().unwrap());
    host
}
fn state(host: &Arc<Host>, values: &[i64], treatment: &[Option<bool>]) -> State {
    let allocator = HostAggregateAllocator::try_new_with_journal(host.clone()).unwrap();
    let mut core = MannWhitneyState::new(allocator);
    if !values.is_empty() {
        let arguments: [ArrayRef; 3] = [
            Arc::new(Int64Array::from(values.to_vec())),
            Arc::new(BooleanArray::from(treatment.to_vec())),
            Arc::new(StringArray::from(vec!["two-sided"; values.len()])),
        ];
        let input = MannWhitneyInput::try_new(&arguments).unwrap();
        for row in 0..values.len() {
            input
                .update_row(&mut core, [row; 4], &mut ScalarWork::new(None))
                .unwrap();
        }
    }
    State {
        core: std::cell::RefCell::new(core),
        visits: std::cell::Cell::new(0),
        renders: std::cell::Cell::new(0),
    }
}
fn emit<'a>(
    host: &Arc<Host>,
    contract: &Arc<AggregateCallContract>,
    states: impl ExactSizeIterator<Item = &'a State> + Clone,
    indices: &[usize],
    control: &Control,
) -> Result<ArrayRef, EvaluationFailure> {
    let allocator: Arc<dyn AggregateStateAllocator> = host.clone();
    let context =
        AggregateEmissionContext::from_host(contract, indices, indices.len(), Some(&allocator));
    emit_mann(
        &MannSourceParams::try_new().unwrap(),
        contract,
        &context,
        states,
        OpaqueRetainedCharge::try_new(host.clone()).unwrap(),
        control,
    )
}
fn fail(result: Result<ArrayRef, EvaluationFailure>, cause: &KernelFailure) {
    assert!(matches!(result, Err(EvaluationFailure::Kernel(actual)) if actual == *cause));
}

#[test]
fn mann_whitney_emission_zero_rows_keeps_real_original_builders_and_last_alias_custody() {
    for phase in [AggregateKernelPhase::Single, AggregateKernelPhase::Partial] {
        let host = new_host();
        let contract = contract(phase);
        let control = Control::default();
        let states: [State; 0] = [];
        let (result, requests) = measured(|| emit(&host, &contract, states.iter(), &[], &control));
        let result = result.unwrap();
        assert_eq!(result.len(), 0);
        assert_eq!(
            result.data_type(),
            if phase == AggregateKernelPhase::Single {
                &DataType::Utf8
            } else {
                &DataType::Binary
            }
        );
        // Exact original default capacity exists even with no rows.
        assert!(requests.0 >= 5124);
        assert!(requests.3 >= 5124);
        let ledger = host.ledger.lock().unwrap();
        assert_eq!(ledger.live.len(), 2, "cold Inner and actual direct holder");
        assert_eq!(ledger.reservations[0], 2 * MAX_ROW_ERROR_MESSAGE_BYTES);
        let model = MannEmissionResources::new(
            if phase == AggregateKernelPhase::Single {
                FlatByteKind::Utf8
            } else {
                FlatByteKind::Binary
            },
            0,
        );
        let facts = model.finish(&MannSourceParams::try_new().unwrap()).unwrap();
        assert_eq!(
            ledger.reservations[2],
            facts.operation_global_peak - 2 * MAX_ROW_ERROR_MESSAGE_BYTES
        );
        assert!(ledger.opaque >= result.get_array_memory_size());
        drop(ledger);
        let data = result.to_data();
        let buffer = data.buffers()[0].clone();
        drop(data);
        drop(result);
        assert_eq!(host.ledger.lock().unwrap().live.len(), 2);
        std::thread::spawn(move || drop(buffer)).join().unwrap();
        let ledger = host.ledger.lock().unwrap();
        assert_eq!(ledger.opaque, 0);
        assert!(ledger.live.is_empty());
    }
}

#[test]
fn mann_whitney_emission_original_final_null_gate_results_and_repeated_state_order() {
    let host = new_host();
    let unknown = state(&host, &[], &[]);
    let initialized_empty = state(&host, &[7], &[None]);
    let samples = state(
        &host,
        &[9, 1, 6, 2],
        &[Some(false), Some(false), Some(true), Some(true)],
    );
    assert!(!samples.core.borrow().extents().sorted);
    let oracle = state(
        &host,
        &[9, 1, 6, 2],
        &[Some(false), Some(false), Some(true), Some(true)],
    );
    let expected = oracle.core.borrow_mut().build_result();
    drop(oracle);
    let states = [&samples, &unknown, &initialized_empty, &samples];
    let result = emit(
        &host,
        &contract(AggregateKernelPhase::Single),
        states.into_iter(),
        &[2, 0, 1, 2],
        &Control::default(),
    )
    .unwrap();
    let array = result.as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(array.value(0), expected);
    assert!(array.is_null(1));
    assert!(array.is_null(2));
    assert_eq!(array.value(3), expected);
    assert_eq!(samples.visits.get(), 2);
    assert_eq!(samples.renders.get(), 2);
    assert_eq!(unknown.renders.get(), 1);
    assert_eq!(initialized_empty.renders.get(), 1);
    drop(result);
    drop(samples);
    drop(unknown);
    drop(initialized_empty);
    assert_eq!(host.ledger.lock().unwrap().opaque, 0);
    assert!(host.ledger.lock().unwrap().live.is_empty());
}

#[test]
fn mann_whitney_emission_intermediate_serializes_uninitialized_and_exact_original_bytes() {
    let host = new_host();
    let states = [
        state(&host, &[], &[]),
        state(&host, &[9, 1, 6], &[Some(false), Some(false), Some(true)]),
    ];
    let expected: Vec<_> = states
        .iter()
        .map(|s| s.core.borrow_mut().serialize())
        .collect();
    let result = emit(
        &host,
        &contract(AggregateKernelPhase::Partial),
        states.iter(),
        &[0, 1],
        &Control::default(),
    )
    .unwrap();
    let result = result
        .as_any()
        .downcast_ref::<arrow_array::BinaryArray>()
        .unwrap();
    assert_eq!(result.null_count(), 0);
    assert_eq!(result.value(0), expected[0]);
    assert_eq!(result.value(1), expected[1]);
    assert_eq!(result.value(0).len(), 17);
}

#[test]
fn mann_whitney_emission_each_admission_frontier_keeps_seven_causes_before_body() {
    // bootstrap, cold journal, complete extra peak, in actual order.
    for offset in 0..3 {
        for cause in causes() {
            let host = new_host();
            let states = [state(&host, &[9, 1], &[Some(false), Some(true)])];
            let prior = host.ledger.lock().unwrap().opaque;
            let start = host.ledger.lock().unwrap().opaque_calls;
            *host.opaque_refusal.lock().unwrap() = Some((start + offset, cause.clone()));
            let result = emit(
                &host,
                &contract(AggregateKernelPhase::Single),
                states.iter(),
                &[0],
                &Control::default(),
            );
            fail(result, &cause);
            assert_eq!(states[0].renders.get(), 0);
            assert_eq!(host.ledger.lock().unwrap().opaque, prior);
            assert_eq!(host.ledger.lock().unwrap().live.len(), 3);
        }
    }
}

#[test]
fn mann_whitney_emission_every_actual_control_frontier_preserves_cause_without_footer() {
    for phase in [AggregateKernelPhase::Single, AggregateKernelPhase::Partial] {
        let callbacks = {
            let host = new_host();
            let states = [state(
                &host,
                &[9, 1, 6, 2],
                &[Some(false), Some(false), Some(true), Some(true)],
            )];
            let control = Control::default();
            drop(emit(&host, &contract(phase), states.iter(), &[0], &control).unwrap());
            control.trace.lock().unwrap().len()
        };
        assert!(callbacks > 8);
        for at in 0..callbacks {
            for cause in causes() {
                let host = new_host();
                let states = [state(
                    &host,
                    &[9, 1, 6, 2],
                    &[Some(false), Some(false), Some(true), Some(true)],
                )];
                let prior = host.ledger.lock().unwrap().opaque;
                let prior_typed = host.ledger.lock().unwrap().live.len();
                let control = Control {
                    trace: Mutex::new(Vec::new()),
                    refusal: Some((at, cause.clone())),
                };
                drop(control.trace.lock().unwrap());
                let c = contract(phase);
                let ((), requests) = measured(|| {
                    fail(emit(&host, &c, states.iter(), &[0], &control), &cause);
                });
                assert_eq!(control.trace.lock().unwrap().len(), at + 1);
                assert_eq!(host.ledger.lock().unwrap().opaque, prior);
                assert_eq!(host.ledger.lock().unwrap().live.len(), prior_typed);
                assert_eq!(
                    requests.3, 0,
                    "original body/custody Global backing cleaned after returned cause drop"
                );
            }
        }
    }
}

#[test]
fn mann_whitney_emission_every_actual_typed_frontier_cleans_partial_custody() {
    for phase in [AggregateKernelPhase::Single, AggregateKernelPhase::Partial] {
        let count = {
            let host = new_host();
            let states = [state(
                &host,
                &[9, 1, 6, 2],
                &[Some(false), Some(false), Some(true), Some(true)],
            )];
            let start = host.ledger.lock().unwrap().typed_calls;
            drop(
                emit(
                    &host,
                    &contract(phase),
                    states.iter(),
                    &[0],
                    &Control::default(),
                )
                .unwrap(),
            );
            host.ledger.lock().unwrap().typed_calls - start
        };
        assert_eq!(count, 2, "actual cold Inner then direct custody holder");
        for at in 0..count {
            for cause in causes() {
                let host = new_host();
                let states = [state(
                    &host,
                    &[9, 1, 6, 2],
                    &[Some(false), Some(false), Some(true), Some(true)],
                )];
                let prior = host.ledger.lock().unwrap().opaque;
                let prior_typed = host.ledger.lock().unwrap().live.len();
                let start = host.ledger.lock().unwrap().typed_calls;
                *host.typed_refusal.lock().unwrap() = Some((start + at, cause.clone()));
                let c = contract(phase);
                let ((), requests) = measured(|| {
                    fail(
                        emit(&host, &c, states.iter(), &[0], &Control::default()),
                        &cause,
                    );
                });
                assert_eq!(host.ledger.lock().unwrap().opaque, prior);
                assert_eq!(host.ledger.lock().unwrap().live.len(), prior_typed);
                assert_eq!(requests.3, 0);
            }
        }
    }
}

#[test]
fn mann_whitney_emission_rejects_foreign_contract_host_missing_allocator_and_row_domain() {
    let host = new_host();
    let other = new_host();
    let states = [state(&host, &[], &[])];
    let c = contract(AggregateKernelPhase::Single);
    let foreign = contract(AggregateKernelPhase::Single);
    let host_dyn: Arc<dyn AggregateStateAllocator> = host.clone();
    let foreign_dyn: Arc<dyn AggregateStateAllocator> = other.clone();
    for (contract, capacity, indices, allocator) in [
        (&foreign, 1, &[0][..], Some(&host_dyn)),
        (&c, 0, &[0][..], Some(&host_dyn)),
        (&c, 1, &[][..], Some(&host_dyn)),
        (&c, 1, &[0][..], None),
        (&c, 1, &[0][..], Some(&foreign_dyn)),
    ] {
        let context = AggregateEmissionContext::from_host(contract, indices, capacity, allocator);
        let result = emit_mann(
            &MannSourceParams::try_new().unwrap(),
            &c,
            &context,
            states.iter(),
            OpaqueRetainedCharge::try_new(host.clone()).unwrap(),
            &Control::default(),
        );
        assert!(matches!(
            result,
            Err(EvaluationFailure::Kernel(KernelFailure::InvalidProgram(_)))
        ));
        assert_eq!(states[0].visits.get(), 0);
        assert_eq!(states[0].renders.get(), 0);
        assert_eq!(other.ledger.lock().unwrap().opaque, 0);
        assert!(other.ledger.lock().unwrap().live.is_empty());
    }
}

#[test]
fn mann_whitney_emission_panic_drops_actual_builder_before_operation_loans() {
    struct PanicControl {
        calls: std::sync::atomic::AtomicUsize,
    }
    impl KernelEvaluationControl for PanicControl {
        fn checkpoint(&self, _: u32) -> Result<(), KernelFailure> {
            if self.calls.fetch_add(1, Ordering::SeqCst) == 2 {
                without_recording(|| std::panic::panic_any("original emission checkpoint panic"));
            }
            Ok(())
        }
        fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
            unreachable!()
        }
    }
    let host = new_host();
    let states = [state(
        &host,
        &[9, 1, 6, 2],
        &[Some(false), Some(false), Some(true), Some(true)],
    )];
    let c = contract(AggregateKernelPhase::Single);
    let host_dyn: Arc<dyn AggregateStateAllocator> = host.clone();
    let context = AggregateEmissionContext::from_host(&c, &[0], 1, Some(&host_dyn));
    let source = MannSourceParams::try_new().unwrap();
    let control = PanicControl {
        calls: std::sync::atomic::AtomicUsize::new(0),
    };
    let prior = host.ledger.lock().unwrap().opaque;
    let typed_prior = host.ledger.lock().unwrap().live.len();
    let (result, requests) = measured(|| {
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            emit_mann(
                &source,
                &c,
                &context,
                states.iter(),
                OpaqueRetainedCharge::try_new(host.clone()).unwrap(),
                &control,
            )
        }))
    });
    let payload = result.unwrap_err();
    without_recording(|| drop(payload));
    assert_eq!(control.calls.load(Ordering::SeqCst), 3);
    assert_eq!(requests.3, 0);
    assert_eq!(host.ledger.lock().unwrap().opaque, prior);
    assert_eq!(host.ledger.lock().unwrap().live.len(), typed_prior);
}

#[test]
fn mann_whitney_emission_actual_peak_and_retained_stock_fit_source_authors() {
    for phase in [AggregateKernelPhase::Single, AggregateKernelPhase::Partial] {
        for rows in [0, 3, 1027] {
            let host = new_host();
            let states = [
                state(
                    &host,
                    &[9, 1, 6, 2],
                    &[Some(false), Some(false), Some(true), Some(true)],
                ),
                state(&host, &[], &[]),
            ];
            let c = contract(phase);
            let source = MannSourceParams::try_new().unwrap();
            let kind = if phase == AggregateKernelPhase::Single {
                FlatByteKind::Utf8
            } else {
                FlatByteKind::Binary
            };
            let mut model = MannEmissionResources::new(kind, rows);
            for at in 0..rows {
                model
                    .add_state(states[at % 2].extents().unwrap(), &source)
                    .unwrap();
            }
            let facts = model.finish(&source).unwrap();
            let indices: Vec<_> = (0..rows).map(|at| at % 2).collect();
            let authority: Arc<dyn AggregateStateAllocator> = host.clone();
            let context = AggregateEmissionContext::from_host(&c, &indices, rows, Some(&authority));
            let control = Control::default();
            drop(control.trace.lock().unwrap());
            let prior = host.ledger.lock().unwrap().opaque;
            let start = host.ledger.lock().unwrap().opaque_calls;
            let typed_prior = host.ledger.lock().unwrap().live.len();
            let ((), measured_facts) = measured(|| {
                let output = emit_mann(
                    &source,
                    &c,
                    &context,
                    indices.iter().map(|&i| &states[i]),
                    OpaqueRetainedCharge::try_new(host.clone()).unwrap(),
                    &control,
                )
                .unwrap();
                assert_eq!(output.len(), rows);
                let alias = output.slice(0, rows);
                let data = alias.to_data();
                drop(output);
                drop(alias);
                assert!(host.ledger.lock().unwrap().opaque > prior);
                drop(data);
            });
            let ledger = host.ledger.lock().unwrap();
            let journal = ledger.reservations[ledger.reservations.len() - 2];
            let calls = ledger.opaque_calls - start;
            let opaque = ledger.opaque;
            let typed = ledger.live.len();
            drop(ledger);
            assert_eq!(calls, 3);
            assert!(measured_facts.2 <= facts.operation_global_peak + journal);
            assert_eq!(measured_facts.3, 0);
            assert_eq!(opaque, prior);
            assert_eq!(typed, typed_prior);
            drop(states);
            assert_eq!(host.ledger.lock().unwrap().opaque, 0);
            assert!(host.ledger.lock().unwrap().live.is_empty());
        }
    }
}
