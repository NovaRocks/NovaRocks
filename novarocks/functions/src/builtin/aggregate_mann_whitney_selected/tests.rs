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
// This fixture uses the actual catalogue binding with a focused effect witness.
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
    contract_types(phase, &[DataType::Int64, DataType::Boolean, DataType::Utf8])
}
fn contract_types(phase: AggregateKernelPhase, types: &[DataType]) -> Arc<AggregateCallContract> {
    let catalog = super::super::catalogue::build_builtin_engine_function_catalog().unwrap();
    let arguments: Vec<_> = types
        .iter()
        .cloned()
        .map(|data_type| FunctionArgument::Value {
            value_type: FunctionValueType::new(data_type, true),
            constant: None,
        })
        .collect();
    let request = FunctionBindingRequest {
        arguments: &arguments,
        logical_argument_count: arguments.len(),
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

fn args(values: &[i64], treatment: &[Option<bool>], alternatives: &[&str]) -> [ArrayRef; 3] {
    [
        Arc::new(Int64Array::from(values.to_vec())),
        Arc::new(BooleanArray::from(treatment.to_vec())),
        Arc::new(StringArray::from(alternatives.to_vec())),
    ]
}
fn selected<'a>(
    kernel: &'a MannKernel,
    arguments: &'a [EvaluatedArgument<'a>],
    selection: Selection<'a>,
) -> SelectedAggregateUpdateInput<'a, 'a> {
    SelectedAggregateUpdateInput::try_new(
        kernel.contract(),
        selection,
        arguments,
        &[],
        &Control::default(),
    )
    .unwrap()
}
fn kernel(phase: AggregateKernelPhase) -> Arc<MannKernel> {
    Arc::new(MannKernel::try_new(contract(phase), &Compile).unwrap())
}

#[test]
fn mann_whitney_selected_real_framework_state_iterator_keeps_order_and_last_alias() {
    let host = new_host();
    let kernel = kernel(AggregateKernelPhase::Single);
    let handle = PreparedAggregateHandle::from_typed(kernel.clone(), &Compile).unwrap();
    assert!(kernel.requires_helper_memory());
    let mut column = AggregateStateColumn::try_new(
        handle,
        host.clone(),
        std::num::NonZeroUsize::new(4).unwrap(),
    )
    .unwrap();
    let control = Control::default();
    column.push(&control).unwrap();
    column.push(&control).unwrap();
    let arrays = args(
        &[9, 1, 6, 2],
        &[Some(false), Some(false), Some(true), Some(true)],
        &["two-sided"; 4],
    );
    let evaluated = arrays.each_ref().map(|a| EvaluatedArgument::Column(a));
    let input = selected(&kernel, &evaluated, Selection::all(4));
    {
        let mut frame = column
            .prepare_update_batch_evaluation(&[0, 0, 0, 0], input, &control)
            .unwrap();
        frame.run(&control).unwrap();
        assert_eq!(frame.rows_processed(), 4);
    }
    let output = column.emit_evaluation(&[0, 1, 0], 3, &control).unwrap();
    let array = output.as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(array.value(0), array.value(2));
    assert!(array.is_null(1));
    let data = output.to_data();
    let buffer = data.buffers()[0].clone();
    drop(data);
    drop(output);
    drop(column);
    assert_eq!(
        host.ledger.lock().unwrap().live.len(),
        2,
        "only output cold Inner and actual H remain"
    );
    std::thread::spawn(move || drop(buffer)).join().unwrap();
    assert!(host.ledger.lock().unwrap().live.is_empty());
    assert_eq!(host.ledger.lock().unwrap().opaque, 0);
    let catalog = super::super::catalogue::build_builtin_engine_function_catalog().unwrap();
    let request_types =
        [DataType::Int64, DataType::Boolean, DataType::Utf8].map(|t| FunctionArgument::Value {
            value_type: FunctionValueType::new(t, true),
            constant: None,
        });
    let bound = catalog
        .resolve_bound_user(
            "mann_whitney_u_test",
            FunctionKind::Aggregate,
            FunctionBindingRequest {
                arguments: &request_types,
                logical_argument_count: 3,
                expected_result_type: None,
            },
            &Compile,
        )
        .unwrap();
    let declaration = catalog
        .pure_overload_declaration_observed(
            &bound.function_id,
            FunctionKind::Aggregate,
            &bound.selected.overload,
            &Compile,
        )
        .unwrap();
    assert_eq!(declaration.implementation().abi, PureKernelAbi::AggregateV1);
    assert_eq!(
        declaration.implementation().implementation.as_str(),
        "builtin.aggregate/mann_whitney_u_test/selected-v1"
    );
}

#[test]
fn mann_whitney_selected_sparse_scalar_channels_keep_original_addresses() {
    let host = new_host();
    let kernel = kernel(AggregateKernelPhase::Single);
    let control = Control::default();
    let arrays = args(
        &[99, 99, 99, 9, 99, 2],
        &[None, None, None, Some(false), None, Some(true)],
        &["two-sided"],
    );
    let evaluated = [
        EvaluatedArgument::Column(&arrays[0]),
        EvaluatedArgument::Column(&arrays[1]),
        EvaluatedArgument::Scalar(&arrays[2]),
    ];
    let rows = [3, 5];
    let input = selected(
        &kernel,
        &evaluated,
        Selection::try_sparse(6, &rows).unwrap(),
    );
    let prepared = kernel
        .prepare_update_evaluation(input, &[0, 0], Some(host.clone()), &control)
        .unwrap();
    let mut state = kernel
        .create_state_with_allocator(Some(host.clone()), &control)
        .unwrap();
    kernel
        .update_row_evaluation(&mut state, &prepared, 0, &control)
        .unwrap();
    kernel
        .update_row_evaluation(&mut state, &prepared, 1, &control)
        .unwrap();
    assert_eq!(state.extents().unwrap().sample_lengths, [1, 1]);
    assert_eq!(prepared.mapping.as_slice(), &[0, 0]);
    drop(prepared);
    drop(state);
    assert!(host.ledger.lock().unwrap().live.is_empty());
    assert_eq!(host.ledger.lock().unwrap().opaque, 0);
}

#[test]
fn mann_whitney_selected_owns_mapping_and_original_full_update_data() {
    let host = new_host();
    let kernel = kernel(AggregateKernelPhase::Single);
    let control = Control::default();
    let invalid_alt = "INVALID-".repeat(150);
    let arrays = args(&[1], &[Some(true)], &[&invalid_alt]);
    let evaluated = arrays.each_ref().map(|a| EvaluatedArgument::Column(a));
    let input = selected(&kernel, &evaluated, Selection::all(1));
    let mut mapping = vec![7];
    let prepared = kernel
        .prepare_update_evaluation(input, &mapping, Some(host.clone()), &control)
        .unwrap();
    mapping[0] = 2;
    drop(mapping);
    let mut state = kernel
        .create_state_with_allocator(Some(host.clone()), &control)
        .unwrap();
    let result = kernel.update_row_evaluation(&mut state, &prepared, 0, &control);
    let Err(EvaluationFailure::InvocationData(data)) = result else {
        panic!("full original invocation data expected")
    };
    assert!(data.message().len() > 512);
    assert_eq!(
        data.message(),
        format!(
            "update aggregate state: Logical Error: invalid alternative `{}`.",
            invalid_alt.to_lowercase()
        )
    );
    assert_eq!(data.aggregate_phase(), AggregateInvocationPhase::Update);
    assert_eq!(data.state_indices(), &[7]);
    assert!(state.failed.get());
    assert_eq!(state.retained.get(), state.base_retained);
    assert!(matches!(
        kernel.update_row_evaluation(&mut state, &prepared, 0, &control),
        Err(EvaluationFailure::Kernel(KernelFailure::InstanceFailed))
    ));
    let alias = data.clone();
    drop(data);
    drop(prepared);
    drop(state);
    assert!(!host.ledger.lock().unwrap().live.is_empty());
    std::thread::spawn(move || drop(alias)).join().unwrap();
    assert!(host.ledger.lock().unwrap().live.is_empty());
    assert_eq!(host.ledger.lock().unwrap().opaque, 0);
}

#[test]
fn mann_whitney_selected_empty_update_validates_original_setup() {
    let host = new_host();
    let kernel = kernel(AggregateKernelPhase::Single);
    let control = Control::default();
    let arrays = args(&[], &[], &[]);
    let evaluated = arrays.each_ref().map(|a| EvaluatedArgument::Column(a));
    let input = selected(&kernel, &evaluated, Selection::all(0));
    let prepared = kernel
        .prepare_update_evaluation(input, &[], Some(host.clone()), &control)
        .unwrap();
    assert!(prepared.mapping.is_empty());
    drop(prepared);
    let arrays: [ArrayRef; 3] = [
        Arc::new(Int64Array::from(Vec::<i64>::new())),
        Arc::new(BooleanArray::from(Vec::<bool>::new())),
        Arc::new(Int64Array::from(Vec::<i64>::new())),
    ];
    let malformed = MannKernel::try_new(
        contract_types(
            AggregateKernelPhase::Single,
            &[DataType::Int64, DataType::Boolean, DataType::Int64],
        ),
        &Compile,
    )
    .unwrap();
    let channels = arrays.each_ref().map(|a| EvaluatedArgument::Column(a));
    let input = selected(&malformed, &channels, Selection::all(0));
    let result = malformed.prepare_update_evaluation(input, &[], Some(host.clone()), &control);
    let Err(EvaluationFailure::InvocationData(data)) = result else {
        panic!("original empty setup Data expected");
    };
    assert_eq!(
        data.message(),
        "update aggregate state: mann_whitney_u_test expects string alternative"
    );
    assert!(data.input_rows().is_empty());
    drop(data);
    assert!(host.ledger.lock().unwrap().live.is_empty());
    assert_eq!(host.ledger.lock().unwrap().opaque, 0);
}

#[test]
fn mann_whitney_selected_original_decode_merge_and_phase_clones_use_target_allocator() {
    let host = new_host();
    let single = kernel(AggregateKernelPhase::Single);
    let control = Control::default();
    let (partial_contract, final_contract) =
        AggregateCallContract::local_stages(single.contract(), &Compile).unwrap();
    let partial = single
        .clone_for_local_phase(partial_contract, &Compile)
        .unwrap();
    let final_kernel = single
        .clone_for_local_phase(final_contract, &Compile)
        .unwrap();
    let arrays = args(
        &[9, 1, 6, 2],
        &[Some(false), Some(false), Some(true), Some(true)],
        &["two-sided"; 4],
    );
    let evaluated = arrays.each_ref().map(|a| EvaluatedArgument::Column(a));
    let prepared = partial
        .prepare_update_evaluation(
            selected(&partial, &evaluated, Selection::all(4)),
            &[0; 4],
            Some(host.clone()),
            &control,
        )
        .unwrap();
    let mut state = partial
        .create_state_with_allocator(Some(host.clone()), &control)
        .unwrap();
    for ordinal in 0..4 {
        partial
            .update_row_evaluation(&mut state, &prepared, ordinal, &control)
            .unwrap();
    }
    let authority: Arc<dyn AggregateStateAllocator> = host.clone();
    let context =
        AggregateEmissionContext::from_host(partial.contract(), &[0], 1, Some(&authority));
    let binary = partial
        .build_intermediate_evaluation_with_context(std::iter::once(&state), &context, &control)
        .unwrap();
    let merge_input = SelectedAggregateMergeInput::try_new(
        final_kernel.contract(),
        Selection::all(1),
        EvaluatedArgument::Column(&binary),
        &control,
    )
    .unwrap();
    let merged = final_kernel
        .prepare_merge_evaluation(merge_input, &[0], Some(host.clone()), &control)
        .unwrap();
    let mut target = final_kernel
        .create_state_with_allocator(Some(host.clone()), &control)
        .unwrap();
    let base = target.base_retained;
    final_kernel
        .merge_row_evaluation(&mut target, &merged, 0, &control)
        .unwrap();
    assert_eq!(target.base_retained, base);
    assert_eq!(target.extents().unwrap().sample_lengths, [2, 2]);
    let context =
        AggregateEmissionContext::from_host(final_kernel.contract(), &[0], 1, Some(&authority));
    let output = final_kernel
        .build_final_evaluation_with_context(std::iter::once(&target), &context, &control)
        .unwrap();
    assert_eq!(output.null_count(), 0);
    drop(output);
    drop(target);
    drop(merged);
    drop(binary);
    drop(state);
    drop(prepared);
    assert!(host.ledger.lock().unwrap().live.is_empty());
    assert_eq!(host.ledger.lock().unwrap().opaque, 0);
}

fn new_host() -> Arc<Host> {
    let host = Arc::new(Host::default());
    drop(host.ledger.lock().unwrap());
    drop(host.opaque_refusal.lock().unwrap());
    drop(host.typed_refusal.lock().unwrap());
    host
}

#[test]
fn mann_whitney_selected_emission_rejects_foreign_host_and_latches_every_early_exit() {
    for exit in 0..4 {
        let host = new_host();
        let other = new_host();
        let kernel = kernel(AggregateKernelPhase::Single);
        let control = Control::default();
        let state = kernel
            .create_state_with_allocator(Some(host.clone()), &control)
            .unwrap();
        let correct: Arc<dyn AggregateStateAllocator> = host.clone();
        let foreign: Arc<dyn AggregateStateAllocator> = other.clone();
        struct NoOpaque;
        impl AggregateStateAllocator for NoOpaque {
            fn allocate(&self, _: Layout) -> Result<NonNull<u8>, KernelFailure> {
                panic!("missing capability must reject before allocation")
            }
            unsafe fn release(&self, _: NonNull<u8>, _: Layout) {
                unreachable!()
            }
        }
        let absent: Arc<dyn AggregateStateAllocator> = Arc::new(NoOpaque);
        let allocator = match exit {
            0 => Some(&foreign),
            1 => None,
            2 => Some(&absent),
            _ => Some(&correct),
        };
        let context = AggregateEmissionContext::from_host(kernel.contract(), &[0], 1, allocator);
        let result = if exit == 3 {
            kernel.build_intermediate_evaluation_with_context(
                std::iter::once(&state),
                &context,
                &control,
            )
        } else {
            kernel.build_final_evaluation_with_context(std::iter::once(&state), &context, &control)
        };
        assert!(matches!(
            result,
            Err(EvaluationFailure::Kernel(KernelFailure::InvalidProgram(_)))
        ));
        assert!(state.failed.get());
        assert_eq!(state.retained.get(), state.base_retained);
        assert_eq!(other.ledger.lock().unwrap().opaque, 0);
        assert!(other.ledger.lock().unwrap().live.is_empty());
        drop(state);
        assert!(host.ledger.lock().unwrap().live.is_empty());
        assert_eq!(host.ledger.lock().unwrap().opaque, 0);
    }
}

#[test]
fn mann_whitney_selected_borrow_conflict_keeps_physical_samples_until_actual_drop() {
    let host = new_host();
    let kernel = kernel(AggregateKernelPhase::Single);
    let control = Control::default();
    let arrays = args(&[1], &[Some(true)], &["two-sided"]);
    let evaluated = arrays.each_ref().map(|a| EvaluatedArgument::Column(a));
    let prepared = kernel
        .prepare_update_evaluation(
            selected(&kernel, &evaluated, Selection::all(1)),
            &[0],
            Some(host.clone()),
            &control,
        )
        .unwrap();
    let mut state = kernel
        .create_state_with_allocator(Some(host.clone()), &control)
        .unwrap();
    kernel
        .update_row_evaluation(&mut state, &prepared, 0, &control)
        .unwrap();
    let prior = state.retained.get();
    assert!(prior > state.base_retained);
    let authority: Arc<dyn AggregateStateAllocator> = host.clone();
    let context = AggregateEmissionContext::from_host(kernel.contract(), &[0], 1, Some(&authority));
    let borrowed = state.core.borrow_mut();
    let result =
        kernel.build_final_evaluation_with_context(std::iter::once(&state), &context, &control);
    assert!(matches!(
        result,
        Err(EvaluationFailure::Kernel(KernelFailure::Internal(_)))
    ));
    assert!(state.failed.get());
    assert_eq!(state.retained.get(), prior);
    drop(borrowed);
    drop(state);
    drop(prepared);
    assert_eq!(host.ledger.lock().unwrap().opaque, 0);
    assert!(host.ledger.lock().unwrap().live.is_empty());
}

#[test]
fn mann_whitney_selected_each_real_prepare_row_and_emit_callback_keeps_seven_causes() {
    for operation in 0..3 {
        let count = {
            let host = new_host();
            let kernel = kernel(AggregateKernelPhase::Single);
            let initial = Control::default();
            let arrays = args(&[9], &[Some(true)], &["two-sided"]);
            let evaluated = arrays.each_ref().map(|a| EvaluatedArgument::Column(a));
            let input = selected(&kernel, &evaluated, Selection::all(1));
            let control = Control::default();
            if operation == 0 {
                drop(
                    kernel
                        .prepare_update_evaluation(input, &[0], Some(host.clone()), &control)
                        .unwrap(),
                );
            } else {
                let prepared = kernel
                    .prepare_update_evaluation(input, &[0], Some(host.clone()), &initial)
                    .unwrap();
                let mut state = kernel
                    .create_state_with_allocator(Some(host.clone()), &initial)
                    .unwrap();
                if operation == 1 {
                    kernel
                        .update_row_evaluation(&mut state, &prepared, 0, &control)
                        .unwrap();
                } else {
                    kernel
                        .update_row_evaluation(&mut state, &prepared, 0, &initial)
                        .unwrap();
                    let authority: Arc<dyn AggregateStateAllocator> = host.clone();
                    let context = AggregateEmissionContext::from_host(
                        kernel.contract(),
                        &[0],
                        1,
                        Some(&authority),
                    );
                    drop(
                        kernel
                            .build_final_evaluation_with_context(
                                std::iter::once(&state),
                                &context,
                                &control,
                            )
                            .unwrap(),
                    );
                }
            }
            control.trace.lock().unwrap().len()
        };
        assert!(count > 0);
        for at in 0..count {
            for cause in causes() {
                let host = new_host();
                let kernel = kernel(AggregateKernelPhase::Single);
                let initial = Control::default();
                let arrays = args(&[9], &[Some(true)], &["two-sided"]);
                let evaluated = arrays.each_ref().map(|a| EvaluatedArgument::Column(a));
                let input = selected(&kernel, &evaluated, Selection::all(1));
                let control = Control {
                    trace: Mutex::new(Vec::new()),
                    refusal: Some((at, cause.clone())),
                };
                if operation == 0 {
                    let result =
                        kernel.prepare_update_evaluation(input, &[0], Some(host.clone()), &control);
                    assert!(
                        matches!(result,Err(EvaluationFailure::Kernel(actual)) if actual==cause)
                    );
                } else {
                    let prepared = kernel
                        .prepare_update_evaluation(input, &[0], Some(host.clone()), &initial)
                        .unwrap();
                    let mut state = kernel
                        .create_state_with_allocator(Some(host.clone()), &initial)
                        .unwrap();
                    let result = if operation == 1 {
                        kernel.update_row_evaluation(&mut state, &prepared, 0, &control)
                    } else {
                        kernel
                            .update_row_evaluation(&mut state, &prepared, 0, &initial)
                            .unwrap();
                        let authority: Arc<dyn AggregateStateAllocator> = host.clone();
                        let context = AggregateEmissionContext::from_host(
                            kernel.contract(),
                            &[0],
                            1,
                            Some(&authority),
                        );
                        kernel
                            .build_final_evaluation_with_context(
                                std::iter::once(&state),
                                &context,
                                &control,
                            )
                            .map(|array| drop(array))
                    };
                    assert!(
                        matches!(result,Err(EvaluationFailure::Kernel(actual)) if actual==cause)
                    );
                    assert!(state.failed.get());
                    assert_eq!(state.retained.get(), state.base_retained);
                }
                assert_eq!(control.trace.lock().unwrap().len(), at + 1);
                assert_eq!(host.ledger.lock().unwrap().opaque, 0);
                assert!(host.ledger.lock().unwrap().live.is_empty());
            }
        }
    }
}

#[test]
fn mann_whitney_selected_all_real_host_frontiers_keep_seven_causes_and_cleanup() {
    fn attempt(stage: usize, refusal: Option<(bool, usize, KernelFailure)>) -> (usize, usize) {
        let host = new_host();
        let single = kernel(AggregateKernelPhase::Single);
        let control = Control::default();
        let arrays = args(
            &[9, 1, 6, 2],
            &[Some(false), Some(false), Some(true), Some(true)],
            &["two-sided"; 4],
        );
        let evaluated = arrays.each_ref().map(|a| EvaluatedArgument::Column(a));
        let mut state = None;
        let mut update = None;
        let mut merge = None;
        let mut binary = None;
        let mut merge_kernel = None;
        if stage == 2 {
            update = Some(
                single
                    .prepare_update_evaluation(
                        selected(&single, &evaluated, Selection::all(4)),
                        &[0; 4],
                        Some(host.clone()),
                        &control,
                    )
                    .unwrap(),
            );
            state = Some(
                single
                    .create_state_with_allocator(Some(host.clone()), &control)
                    .unwrap(),
            );
        }
        if stage >= 3 {
            let (_, final_contract) =
                AggregateCallContract::local_stages(single.contract(), &Compile).unwrap();
            merge_kernel = Some(
                single
                    .clone_for_local_phase(final_contract, &Compile)
                    .unwrap(),
            );
            let mut original = MannWhitneyState::new(
                HostAggregateAllocator::try_new_with_journal(host.clone()).unwrap(),
            );
            let view = MannWhitneyInput::try_new(&arrays).unwrap();
            for row in 0..4 {
                view.update_row(&mut original, [row; 4], &mut ScalarWork::new(None))
                    .unwrap();
            }
            let bytes = original.serialize();
            drop(original);
            binary = Some(Arc::new(BinaryArray::from(vec![Some(bytes.as_slice())])) as ArrayRef);
            state = Some(
                merge_kernel
                    .as_ref()
                    .unwrap()
                    .create_state_with_allocator(Some(host.clone()), &control)
                    .unwrap(),
            );
            if stage == 4 {
                let state = state.as_ref().unwrap();
                let mut core = state.core.borrow_mut();
                view.update_row(&mut core, [0; 4], &mut ScalarWork::new(None))
                    .unwrap();
                state.account(&core).unwrap();
            }
        }
        // Declare after the Kernel and Binary source: all borrowed preparation
        // dies before those actual owners, even on a refused merge frontier.
        if let Some(kernel) = merge_kernel.as_ref() {
            let input = SelectedAggregateMergeInput::try_new(
                kernel.contract(),
                Selection::all(1),
                EvaluatedArgument::Column(binary.as_ref().unwrap()),
                &control,
            )
            .unwrap();
            merge = Some(
                kernel
                    .prepare_merge_evaluation(input, &[0], Some(host.clone()), &control)
                    .unwrap(),
            );
        }
        let (typed_start, opaque_start) = {
            let ledger = host.ledger.lock().unwrap();
            (ledger.typed_calls, ledger.opaque_calls)
        };
        if let Some((typed, at, cause)) = &refusal {
            if *typed {
                *host.typed_refusal.lock().unwrap() = Some((typed_start + *at, cause.clone()));
            } else {
                *host.opaque_refusal.lock().unwrap() = Some((opaque_start + *at, cause.clone()));
            }
        }
        let result = match stage {
            0 => single
                .create_state_with_allocator(Some(host.clone()), &control)
                .map(drop)
                .map_err(EvaluationFailure::from),
            1 => single
                .prepare_update_evaluation(
                    selected(&single, &evaluated, Selection::all(4)),
                    &[0; 4],
                    Some(host.clone()),
                    &control,
                )
                .map(drop),
            2 => single.update_row_evaluation(
                state.as_mut().unwrap(),
                update.as_ref().unwrap(),
                0,
                &control,
            ),
            _ => merge_kernel.as_ref().unwrap().merge_row_evaluation(
                state.as_mut().unwrap(),
                merge.as_ref().unwrap(),
                0,
                &control,
            ),
        };
        if let Some((_, _, cause)) = &refusal {
            assert!(matches!(result,Err(EvaluationFailure::Kernel(actual)) if actual==*cause));
            if let Some(state) = &state {
                assert!(state.failed.get());
                assert_eq!(state.retained.get(), state.base_retained);
            }
        } else {
            result.unwrap();
        }
        let count = {
            let ledger = host.ledger.lock().unwrap();
            (
                ledger.typed_calls - typed_start,
                ledger.opaque_calls - opaque_start,
            )
        };
        drop(merge);
        drop(merge_kernel);
        drop(binary);
        drop(update);
        drop(state);
        assert_eq!(host.ledger.lock().unwrap().opaque, 0);
        assert!(host.ledger.lock().unwrap().live.is_empty());
        count
    }
    for stage in 0..5 {
        let (typed, opaque) = attempt(stage, None);
        assert!(typed > 0);
        assert!(opaque > 0);
        for (is_typed, count) in [(true, typed), (false, opaque)] {
            for at in 0..count {
                for cause in causes() {
                    attempt(stage, Some((is_typed, at, cause)));
                }
            }
        }
    }
}
