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
use super::super::aggregate_mann_whitney_core::MannWhitneyState;
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
fn contract() -> Arc<AggregateCallContract> {
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
        AggregateCallContract::try_new(
            call,
            AggregateKernelPhase::Single,
            false,
            Arc::from([]),
            None,
            &Compile,
        )
        .unwrap(),
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
                "original Global backing still live when call loan released"
            );
        }
        without_recording(|| {
            let mut ledger = self.ledger.lock().unwrap();
            ledger.opaque = ledger.opaque.checked_sub(bytes).unwrap();
            ledger.released.push(bytes);
        });
    }
}
struct Fixture {
    host: Arc<Host>,
    allocator: HostAggregateAllocator,
    charge: OpaqueRetainedCharge,
    source: MannSourceParams,
    contract: Arc<AggregateCallContract>,
}
impl Fixture {
    fn new() -> Self {
        let host = Arc::new(Host::default());
        let allocator = HostAggregateAllocator::try_new_with_journal(host.clone()).unwrap();
        Self {
            charge: OpaqueRetainedCharge::try_new(host.clone()).unwrap(),
            host,
            allocator,
            source: MannSourceParams::try_new().unwrap(),
            contract: contract(),
        }
    }
    fn domain<'a>(
        &'a self,
        phase: AggregateInvocationPhase,
        selection: Selection<'a>,
        mapping: &'a [usize],
    ) -> MannDataDomain<'a> {
        MannDataDomain {
            allocator: &self.allocator,
            contract: &self.contract,
            phase,
            selection,
            mapping,
        }
    }
    fn arm_opaque(&self, offset: usize, cause: KernelFailure) {
        let at = self.host.ledger.lock().unwrap().opaque_calls;
        *self.host.opaque_refusal.lock().unwrap() = Some((at + offset, cause));
    }
    fn arm_typed(&self, offset: usize, cause: KernelFailure) {
        let at = self.host.ledger.lock().unwrap().typed_calls;
        *self.host.typed_refusal.lock().unwrap() = Some((at + offset, cause));
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
fn typed(result: Result<(), EvaluationFailure>, cause: &KernelFailure) {
    assert!(matches!(result, Err(EvaluationFailure::Kernel(actual)) if actual == *cause));
}
fn arrays(alt: &str, treatment: Option<bool>) -> [ArrayRef; 3] {
    [
        Arc::new(Int64Array::from(vec![1])),
        Arc::new(BooleanArray::from(vec![treatment])),
        Arc::new(StringArray::from(vec![alt])),
    ]
}

#[test]
fn mann_whitney_call_loans_actual_long_data_drops_global_before_release_and_retains_typed_aliases()
{
    let fixture = Fixture::new();
    let control = Control::default();
    let alt = "INVALID-".repeat(150);
    let args = arrays(&alt, Some(true));
    let input = MannWhitneyInput::try_new(&args).unwrap();
    let mut state = MannWhitneyState::new(fixture.allocator.clone());
    let rows = [2, 7];
    let selection = Selection::try_sparse(10, &rows).unwrap();
    let mapping = [4, 1];
    let prior = fixture.host.ledger.lock().unwrap().opaque;
    fixture
        .host
        .assert_global_clear
        .store(true, Ordering::SeqCst);
    let (result, facts) = measured(|| {
        call_unit(
            &fixture.source,
            &fixture.charge,
            fixture.domain(AggregateInvocationPhase::Update, selection, &mapping),
            &control,
            |work, resources| input.update_row_with_resources(&mut state, [0; 4], work, resources),
        )
    });
    assert_eq!(facts.3, 0);
    let Err(EvaluationFailure::InvocationData(data)) = result else {
        panic!("full original Data expected")
    };
    assert_eq!(
        data.message(),
        format!(
            "update aggregate state: Logical Error: invalid alternative `{}`.",
            alt.to_lowercase()
        )
    );
    assert!(data.message().len() > crate::MAX_ROW_ERROR_MESSAGE_BYTES);
    assert_eq!(data.input_rows(), rows);
    assert_eq!(data.state_indices(), mapping);
    assert_eq!(data.aggregate_phase(), AggregateInvocationPhase::Update);
    assert_eq!(fixture.host.ledger.lock().unwrap().opaque, prior);
    fixture
        .host
        .assert_global_clear
        .store(false, Ordering::SeqCst);
    let alias = data.clone();
    let host = fixture.host.clone();
    drop(data);
    drop(state);
    drop(fixture);
    assert!(!host.ledger.lock().unwrap().live.is_empty());
    std::thread::spawn(move || drop(alias)).join().unwrap();
    assert!(host.ledger.lock().unwrap().live.is_empty());
    assert_eq!(host.ledger.lock().unwrap().opaque, 0);
}

#[test]
fn mann_whitney_call_loans_prefix_and_event_refuse_before_body_all_seven_causes() {
    for cause in causes() {
        for offset in [0, 1, 2] {
            let fixture = Fixture::new();
            let prior = fixture.host.ledger.lock().unwrap().opaque;
            fixture.arm_opaque(offset, cause.clone());
            let args = arrays("LESS", Some(true));
            let input = MannWhitneyInput::try_new(&args).unwrap();
            let mut state = MannWhitneyState::new(fixture.allocator.clone());
            let control = Control::default();
            let selection = Selection::all(1);
            let mapping = [0];
            typed(
                call_unit(
                    &fixture.source,
                    &fixture.charge,
                    fixture.domain(AggregateInvocationPhase::Update, selection, &mapping),
                    &control,
                    |work, resources| {
                        input.update_row_with_resources(&mut state, [0; 4], work, resources)
                    },
                ),
                &cause,
            );
            assert!(!state.has_samples());
            assert_eq!(fixture.host.ledger.lock().unwrap().opaque, prior);
            if offset == 0 {
                assert!(control.trace.lock().unwrap().is_empty());
            }
        }
    }
}

#[test]
fn mann_whitney_call_loans_actual_empty_setup_and_decode_keep_original_update_merge_domains() {
    let fixture = Fixture::new();
    let control = Control::default();
    let mapping = [];
    let result = call_input(
        &fixture.source,
        &fixture.charge,
        fixture.domain(
            AggregateInvocationPhase::Update,
            Selection::all(0),
            &mapping,
        ),
        &control,
        |work, resources| {
            MannWhitneyInput::try_new_channels_with_resources([None; 4], 0, work, resources)
        },
    );
    let Err(EvaluationFailure::InvocationData(data)) = result else {
        panic!("original empty setup Data")
    };
    assert_eq!(
        data.message(),
        "update aggregate state: mann_whitney_u_test expects 2 to 4 arguments"
    );
    assert!(data.input_rows().is_empty());
    drop(data);
    let result = call_unit(
        &fixture.source,
        &fixture.charge,
        fixture.domain(AggregateInvocationPhase::Merge, Selection::all(1), &[0]),
        &control,
        |work, resources| {
            let _other = MannWhitneyState::deserialize_in_with_resources(
                &[],
                fixture.allocator.clone(),
                work,
                resources,
            )?;
            Ok(())
        },
    );
    let Err(EvaluationFailure::InvocationData(data)) = result else {
        panic!("original decode Data")
    };
    assert_eq!(
        data.message(),
        "merge aggregate state: mann_whitney_u_test deserialize: buffer too short"
    );
}

#[test]
fn mann_whitney_call_loans_pending_work_flush_precedes_scalar_geometry() {
    for cause in causes() {
        let fixture = Fixture::new();
        let args = arrays("LESS", Some(true));
        let input = MannWhitneyInput::try_new(&args).unwrap();
        let mut state = MannWhitneyState::new(fixture.allocator.clone());
        let control = Control {
            refusal: Some((0, cause.clone())),
            ..Control::default()
        };
        typed(
            call_unit(
                &fixture.source,
                &fixture.charge,
                fixture.domain(AggregateInvocationPhase::Update, Selection::all(1), &[0]),
                &control,
                |work, resources| {
                    input.update_row_with_resources(&mut state, [0; 4], work, resources)
                },
            ),
            &cause,
        );
        assert_eq!(*control.trace.lock().unwrap(), vec![1]);
        assert!(!state.has_samples());
    }
}

#[test]
fn mann_whitney_call_loans_typed_data_refusal_keeps_original_cause_without_error_footer() {
    let baseline = Fixture::new();
    let args = arrays("INVALID", Some(true));
    let input = MannWhitneyInput::try_new(&args).unwrap();
    let mut state = MannWhitneyState::new(baseline.allocator.clone());
    let first = baseline.host.ledger.lock().unwrap().typed_calls;
    let result = call_unit(
        &baseline.source,
        &baseline.charge,
        baseline.domain(AggregateInvocationPhase::Update, Selection::all(1), &[0]),
        &Control::default(),
        |work, resources| input.update_row_with_resources(&mut state, [0; 4], work, resources),
    );
    let Err(EvaluationFailure::InvocationData(data)) = result else {
        panic!("actual original data projection")
    };
    drop(data);
    let requests = baseline.host.ledger.lock().unwrap().typed_calls - first;
    assert!(requests > 0);
    for cause in causes() {
        for allocation in 0..requests {
            let fixture = Fixture::new();
            fixture.arm_typed(allocation, cause.clone());
            let args = arrays("INVALID", Some(true));
            let input = MannWhitneyInput::try_new(&args).unwrap();
            let mut state = MannWhitneyState::new(fixture.allocator.clone());
            let result = call_unit(
                &fixture.source,
                &fixture.charge,
                fixture.domain(AggregateInvocationPhase::Update, Selection::all(1), &[0]),
                &Control::default(),
                |work, resources| {
                    input.update_row_with_resources(&mut state, [0; 4], work, resources)
                },
            );
            typed(result, &cause);
            let ledger = fixture.host.ledger.lock().unwrap();
            assert_eq!(
                ledger.live.len(),
                1,
                "only actual allocator metadata remains after partial Data teardown"
            );
        }
    }
}

#[test]
fn mann_whitney_call_loans_reject_foreign_host_and_fourth_frontier_without_overwrite() {
    let fixture = Fixture::new();
    let foreign = Fixture::new();
    let control = Control::default();
    let called = std::cell::Cell::new(false);
    assert!(matches!(
        call_unit(
            &fixture.source,
            &fixture.charge,
            foreign.domain(AggregateInvocationPhase::Update, Selection::all(1), &[0]),
            &control,
            |_, _| {
                called.set(true);
                Ok(())
            }
        ),
        Err(EvaluationFailure::Kernel(KernelFailure::InvalidProgram(_)))
    ));
    assert!(!called.get());
    assert!(control.trace.lock().unwrap().is_empty());
    let prior = fixture.host.ledger.lock().unwrap().opaque;
    let result = call_unit(
        &fixture.source,
        &fixture.charge,
        fixture.domain(AggregateInvocationPhase::Update, Selection::all(1), &[0]),
        &control,
        |work, resources| {
            for _ in 0..4 {
                resources.before(
                    MannResourceEvent::StaticDiagnostic {
                        message: "test",
                        treatment_replacement: false,
                    },
                    work,
                )?;
            }
            Ok(())
        },
    );
    assert!(matches!(
        result,
        Err(EvaluationFailure::Kernel(KernelFailure::Internal(_)))
    ));
    assert_eq!(fixture.host.ledger.lock().unwrap().opaque, prior);
}

#[test]
fn mann_whitney_call_loans_unwind_destroys_actual_original_temporaries_before_release() {
    let fixture = Fixture::new();
    let prior = fixture.host.ledger.lock().unwrap().opaque;
    fixture
        .host
        .assert_global_clear
        .store(true, Ordering::SeqCst);
    // Deliberate panic is a teardown probe, not an allocation source or kernel.
    let (_, facts) = measured(|| {
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            call_unit(
                &fixture.source,
                &fixture.charge,
                fixture.domain(AggregateInvocationPhase::Update, Selection::all(1), &[0]),
                &Control::default(),
                |work, resources| {
                    resources.before(
                        MannResourceEvent::FirstAlternative { source: "INVALID" },
                        work,
                    )?;
                    let _actual = "INVALID".to_lowercase();
                    without_recording(|| panic!("Mann call teardown probe"))
                },
            )
        }))
    });
    fixture
        .host
        .assert_global_clear
        .store(false, Ordering::SeqCst);
    assert_eq!(facts.3, 0);
    assert_eq!(fixture.host.ledger.lock().unwrap().opaque, prior);
}

#[test]
fn mann_whitney_call_loans_borrowed_success_update_and_decode_merge_use_same_host() {
    let fixture = Fixture::new();
    let control = Control::default();
    let args = arrays("LESS", Some(true));
    let prior = fixture.host.ledger.lock().unwrap().opaque;
    fixture
        .host
        .assert_global_clear
        .store(true, Ordering::SeqCst);
    let (input, stock) = measured(|| {
        call_input(
            &fixture.source,
            &fixture.charge,
            fixture.domain(AggregateInvocationPhase::Update, Selection::all(1), &[0]),
            &control,
            |work, resources| {
                MannWhitneyInput::try_new_channels_with_resources(
                    std::array::from_fn(|i| args.get(i)),
                    args.len(),
                    work,
                    resources,
                )
            },
        )
        .unwrap()
    });
    assert_eq!(stock.3, 0);
    assert_eq!(fixture.host.ledger.lock().unwrap().opaque, prior);
    let mut state = MannWhitneyState::new(fixture.allocator.clone());
    let (result, stock) = measured(|| {
        call_unit(
            &fixture.source,
            &fixture.charge,
            fixture.domain(AggregateInvocationPhase::Update, Selection::all(1), &[0]),
            &control,
            |work, resources| input.update_row_with_resources(&mut state, [0; 4], work, resources),
        )
    });
    result.unwrap();
    assert_eq!(stock.3, 0);
    assert!(state.has_samples());
    assert_eq!(state.extents().sample_lengths, [0, 1]);
    assert_eq!(fixture.host.ledger.lock().unwrap().opaque, prior);
    fixture
        .host
        .assert_global_clear
        .store(false, Ordering::SeqCst);
    let encoded = state.serialize();
    let mut destination = MannWhitneyState::new(fixture.allocator.clone());
    fixture
        .host
        .assert_global_clear
        .store(true, Ordering::SeqCst);
    let (result, stock) = measured(|| {
        call_unit(
            &fixture.source,
            &fixture.charge,
            fixture.domain(AggregateInvocationPhase::Merge, Selection::all(1), &[0]),
            &control,
            |work, resources| {
                let other = MannWhitneyState::deserialize_in_with_resources(
                    &encoded,
                    fixture.allocator.clone(),
                    work,
                    resources,
                )?;
                destination.merge_with_resources(other, work, resources)
            },
        )
    });
    result.unwrap();
    assert_eq!(stock.3, 0);
    assert_eq!(destination.extents().sample_lengths, [0, 1]);
    assert_eq!(fixture.host.ledger.lock().unwrap().opaque, prior);
    fixture
        .host
        .assert_global_clear
        .store(false, Ordering::SeqCst);
}

#[test]
fn mann_whitney_call_loans_every_actual_core_and_data_callback_preserves_seven_causes() {
    let baseline = Fixture::new();
    let args = arrays(&"INVALID-".repeat(150), Some(true));
    let input = MannWhitneyInput::try_new(&args).unwrap();
    let mut state = MannWhitneyState::new(baseline.allocator.clone());
    let control = Control::default();
    let result = call_unit(
        &baseline.source,
        &baseline.charge,
        baseline.domain(AggregateInvocationPhase::Update, Selection::all(1), &[0]),
        &control,
        |work, resources| input.update_row_with_resources(&mut state, [0; 4], work, resources),
    );
    assert!(matches!(result, Err(EvaluationFailure::InvocationData(_))));
    let count = control.trace.lock().unwrap().len();
    assert!(count > 4);
    for at in 0..count {
        for cause in causes() {
            let fixture = Fixture::new();
            let prior = fixture.host.ledger.lock().unwrap().opaque;
            let args = arrays(&"INVALID-".repeat(150), Some(true));
            let input = MannWhitneyInput::try_new(&args).unwrap();
            let mut state = MannWhitneyState::new(fixture.allocator.clone());
            let control = Control {
                refusal: Some((at, cause.clone())),
                ..Control::default()
            };
            typed(
                call_unit(
                    &fixture.source,
                    &fixture.charge,
                    fixture.domain(AggregateInvocationPhase::Update, Selection::all(1), &[0]),
                    &control,
                    |work, resources| {
                        input.update_row_with_resources(&mut state, [0; 4], work, resources)
                    },
                ),
                &cause,
            );
            assert_eq!(control.trace.lock().unwrap().len(), at + 1);
            assert_eq!(fixture.host.ledger.lock().unwrap().opaque, prior);
            assert_eq!(fixture.host.ledger.lock().unwrap().live.len(), 1);
        }
    }
}

#[test]
fn mann_whitney_call_loans_missing_capability_and_foreign_phase_reject_before_body() {
    struct Bare;
    impl AggregateStateAllocator for Bare {
        fn allocate(&self, _: Layout) -> Result<NonNull<u8>, KernelFailure> {
            panic!("missing capability cannot allocate")
        }
        unsafe fn release(&self, _: NonNull<u8>, _: Layout) {
            panic!("no allocation exists")
        }
    }
    assert!(matches!(
        OpaqueRetainedCharge::try_new(Arc::new(Bare)),
        Err(KernelFailure::InvalidProgram(_))
    ));
    assert!(matches!(
        HostAggregateAllocator::try_new_with_journal(Arc::new(Bare)),
        Err(KernelFailure::InvalidProgram(_))
    ));
    let fixture = Fixture::new();
    let control = Control::default();
    let called = std::cell::Cell::new(false);
    assert!(matches!(
        call_unit(
            &fixture.source,
            &fixture.charge,
            fixture.domain(AggregateInvocationPhase::Final, Selection::all(1), &[0]),
            &control,
            |_, _| {
                called.set(true);
                Ok(())
            }
        ),
        Err(EvaluationFailure::Kernel(KernelFailure::InvalidProgram(_)))
    ));
    assert!(!called.get());
    assert!(control.trace.lock().unwrap().is_empty());
}
