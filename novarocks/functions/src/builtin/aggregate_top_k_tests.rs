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

//! Real host, original-data and failure-lifecycle ApproxTopK consumer probes.
use super::*;
use crate::opaque_memory::OpaqueAllocationHost;
use arrow_array::{Int32Array, StringArray};
use novarocks_type_contract::{
    CallProofScope, CompileControlError, CompilePhase, DecimalOverflowPolicy, EvaluationDemand,
    EvaluationRegionId, ExpressionEffectContext, ExpressionUseId, PureCompileControl,
    SemanticParameters,
};
use std::{
    alloc::Layout,
    ptr::NonNull,
    sync::{Mutex, OnceLock},
    time::Duration,
};
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
        panic!("approx_top_k never waits")
    }
}
#[derive(Default)]
struct Ledger {
    opaque_attempts: usize,
    opaque_bytes: usize,
    attempts: usize,
    bytes: usize,
    peak: usize,
    live: Vec<(usize, Layout)>,
    metadata: Option<(usize, Layout)>,
}
#[derive(Default)]
struct Host {
    opaque_refusal: Mutex<Option<(usize, KernelFailure)>>,
    ledger: Mutex<Ledger>,
    refusal: Mutex<Option<(usize, KernelFailure)>>,
}
impl AggregateStateAllocator for Host {
    fn opaque_allocation_host(&self) -> Option<&dyn OpaqueAllocationHost> {
        Some(self)
    }
    fn allocate(&self, layout: Layout) -> Result<NonNull<u8>, KernelFailure> {
        assert_ne!(layout.size(), 0);
        let mut ledger = self.ledger.lock().unwrap();
        let at = ledger.attempts;
        ledger.attempts += 1;
        if let Some((stop, cause)) = &*self.refusal.lock().unwrap() {
            assert!(at <= *stop, "owned allocation after first refusal");
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

impl OpaqueAllocationHost for Host {
    fn reserve_opaque(&self, bytes: usize) -> Result<(), KernelFailure> {
        let mut ledger = self.ledger.lock().unwrap();
        let at = ledger.opaque_attempts;
        ledger.opaque_attempts += 1;
        if let Some((stop, cause)) = &*self.opaque_refusal.lock().unwrap() {
            assert!(at <= *stop, "opaque reservation after first refusal");
            if *stop == at {
                return Err(cause.clone());
            }
        }
        ledger.opaque_bytes = ledger.opaque_bytes.checked_add(bytes).unwrap();
        Ok(())
    }
    fn release_opaque(&self, bytes: usize) {
        let mut ledger = self.ledger.lock().unwrap();
        ledger.opaque_bytes = ledger
            .opaque_bytes
            .checked_sub(bytes)
            .expect("actual opaque charge released once");
    }
}
fn empty(host: &Host) {
    let ledger = host.ledger.lock().unwrap();
    assert_eq!(ledger.bytes, 0);
    assert_eq!(ledger.opaque_bytes, 0);
    assert!(ledger.live.is_empty());
}

fn kernel(types: &[DataType], phase: AggregateKernelPhase) -> TopKKernel {
    static CATALOG: OnceLock<crate::EngineFunctionCatalog> = OnceLock::new();
    let catalog = CATALOG
        .get_or_init(|| super::super::catalogue::build_builtin_engine_function_catalog().unwrap());
    let args: Vec<_> = types
        .iter()
        .map(|ty| FunctionArgument::Value {
            value_type: FunctionValueType::from_legacy_declared_projection(ty.clone(), true),
            constant: None,
        })
        .collect();
    let request = FunctionBindingRequest {
        arguments: &args,
        logical_argument_count: args.len(),
        expected_result_type: None,
    };
    let bound = catalog
        .resolve_bound_trusted("approx_top_k", FunctionKind::Aggregate, request, &Compile)
        .unwrap();
    let selected = Arc::new(bound.selected);
    let context = novarocks_type_contract::RelationEffectContext::call(
        novarocks_type_contract::RelationCallId::new(1942),
    );
    let state_context = ExpressionEffectContext {
        use_id: ExpressionUseId::new(1946),
        region: EvaluationRegionId::new(130),
        demand: EvaluationDemand::Value,
    };
    let state_type = FunctionValueType::new(DataType::Binary, true);
    let uses: Vec<_> = (0..args.len())
        .map(|i| Some(ExpressionUseId::new(1943 + i as u32)))
        .collect();
    let argument_uses = if phase.consumes_logical_arguments() {
        CallArgumentUses::SelectedChannels(&uses)
    } else {
        CallArgumentUses::AggregateMerge {
            phase,
            state_context: state_context.into(),
            state_input_type: &state_type,
        }
    };
    let params = SemanticParameters::try_new([]).unwrap();
    let prepared = catalog
        .prepare_fresh_selected(
            CallEffectInput {
                function_id: &bound.function_id,
                kind: FunctionKind::Aggregate,
                selected: &selected,
                request,
                argument_uses,
                context: context.into(),
                parameters: &params,
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
        panic!("actual aggregate handle")
    };
    TopKKernel {
        contract: handle.contract().clone(),
    }
}
fn column(k: TopKKernel, host: Arc<Host>, groups: usize) -> AggregateStateColumn {
    let handle = PreparedAggregateHandle::from_typed(Arc::new(k), &Compile).unwrap();
    let mut column = AggregateStateColumn::try_new(
        handle,
        host,
        std::num::NonZeroUsize::new(groups.max(1)).unwrap(),
    )
    .unwrap();
    for _ in 0..groups {
        column.push(&Control::default()).unwrap();
    }
    column
}
fn update(
    c: &mut AggregateStateColumn,
    arrays: &[ArrayRef],
    selection: Selection<'_>,
    mapping: &[usize],
    control: &dyn KernelEvaluationControl,
) -> Result<(), EvaluationFailure> {
    let contract = c.handle().contract().clone();
    let args: Vec<_> = arrays.iter().map(EvaluatedArgument::Column).collect();
    let setup = Control::default();
    let input =
        SelectedAggregateUpdateInput::try_new(&contract, selection, &args, &[], &setup).unwrap();
    let mut frame = c.prepare_update_batch_evaluation(mapping, input, &setup)?;
    let result = frame.run(control);
    if result.is_err() {
        let reentry = Control::default();
        assert!(matches!(
            frame.run(&reentry),
            Err(EvaluationFailure::Kernel(KernelFailure::InstanceFailed))
        ));
        assert!(reentry.trace.lock().unwrap().is_empty());
    }
    drop(frame);
    result
}
fn control_run(
    control: &dyn KernelEvaluationControl,
    emission: bool,
) -> Result<(), EvaluationFailure> {
    let host = Arc::new(Host::default());
    let mut c = column(
        kernel(&[DataType::Utf8], AggregateKernelPhase::Single),
        host.clone(),
        1,
    );
    let arrays = [Arc::new(StringArray::from(vec![
        "abcdefgh".repeat(40),
        "xyz".repeat(100),
    ])) as ArrayRef];
    let result = if emission {
        update(
            &mut c,
            &arrays,
            Selection::all(2),
            &[0, 0],
            &Control::default(),
        )
        .unwrap();
        c.emit_evaluation(&[0], 1, control).map(|_| ())
    } else {
        update(&mut c, &arrays, Selection::all(2), &[0, 0], control)
    };
    drop(c);
    empty(&host);
    result
}

#[test]
fn approx_top_k_owner_same_original_core_final_and_binary_roundtrip_real_custody() {
    for arity in 1..=3 {
        let host = Arc::new(Host::default());
        let mut types = vec![DataType::Int32];
        types.resize(arity, DataType::Int32);
        let mut c = column(
            kernel(&types, AggregateKernelPhase::Single),
            host.clone(),
            3,
        );
        let values = Arc::new(Int32Array::from(vec![Some(7), None, Some(2), Some(7)])) as ArrayRef;
        let mut arrays = vec![values];
        for _ in 1..arity {
            arrays.push(Arc::new(Int32Array::from(vec![3; 4])) as ArrayRef);
        }
        update(
            &mut c,
            &arrays,
            Selection::all(4),
            &[2, 0, 2, 2],
            &Control::default(),
        )
        .unwrap();
        let out = c
            .emit_evaluation(&[2, 0, 1], 3, &Control::default())
            .unwrap();
        assert_eq!(out.len(), 3);
        assert_eq!(out.null_count(), 0);
        let list = out
            .as_any()
            .downcast_ref::<arrow_array::ListArray>()
            .unwrap();
        assert_eq!(list.value_length(0), 2);
        assert_eq!(list.value_length(1), 1);
        assert_eq!(list.value_length(2), 0);
        drop(c);
        assert!(
            host.ledger.lock().unwrap().opaque_bytes > 0,
            "actual result backing retains its grant"
        );
        drop(out);
        empty(&host);
    }
}
#[test]
fn approx_top_k_owner_contract_actual_sparse_domain_and_failed_reentry() {
    let host = Arc::new(Host::default());
    let mut c = column(
        kernel(&[DataType::UInt32], AggregateKernelPhase::Single),
        host.clone(),
        2,
    );
    let arrays = [Arc::new(arrow_array::UInt32Array::from(vec![
        None,
        Some(7),
        None,
        Some(8),
    ])) as ArrayRef];
    let rows = [3];
    let err = update(
        &mut c,
        &arrays,
        Selection::try_sparse(4, &rows).unwrap(),
        &[1],
        &Control::default(),
    )
    .unwrap_err();
    let EvaluationFailure::Kernel(cause) = err else {
        panic!("unsupported frozen carrier is outer")
    };
    use novarocks_failure_contract::{FailureClass, FailureProjection};
    assert_eq!(cause.failure_fact().class(), FailureClass::Invariant);
    assert_eq!(cause.failure_fact().reason().code(), 313);
    assert_eq!(cause.to_string(), "unsupported tracked scalar type: UInt32");
    assert!(matches!(
        c.emit_evaluation(&[1], 2, &Control::default()),
        Err(EvaluationFailure::Kernel(KernelFailure::InstanceFailed))
    ));
    drop(cause);
    drop(c);
    empty(&host);
}
#[test]
fn approx_top_k_owner_seven_update_and_final_control_causes_prefix_no_tail() {
    for emission in [false, true] {
        let control = Control::default();
        control_run(&control, emission).unwrap();
        let trace = control.trace.lock().unwrap().clone();
        assert!(trace.len() > 2);
        for stop in 0..trace.len() {
            for cause in causes() {
                let control = Control {
                    trace: Mutex::new(vec![]),
                    refusal: Some((stop, cause.clone())),
                };
                assert_eq!(
                    control_run(&control, emission).unwrap_err(),
                    EvaluationFailure::Kernel(cause)
                );
                assert_eq!(*control.trace.lock().unwrap(), trace[..=stop]);
            }
        }
    }
}
#[test]
fn approx_top_k_owner_real_tracked_and_opaque_refusal_causes_drop_and_no_reentry() {
    for opaque in [false, true] {
        for cause in causes() {
            let host = Arc::new(Host::default());
            let mut c = column(
                kernel(&[DataType::Utf8], AggregateKernelPhase::Single),
                host.clone(),
                1,
            );
            let arrays = [Arc::new(StringArray::from(vec!["long".repeat(100)])) as ArrayRef];
            let contract = c.handle().contract().clone();
            let args = [EvaluatedArgument::Column(&arrays[0])];
            let setup = Control::default();
            let input = SelectedAggregateUpdateInput::try_new(
                &contract,
                Selection::all(1),
                &args,
                &[],
                &setup,
            )
            .unwrap();
            let mut frame = c
                .prepare_update_batch_evaluation(&[0], input, &setup)
                .unwrap();
            if opaque {
                let at = host.ledger.lock().unwrap().opaque_attempts;
                *host.opaque_refusal.lock().unwrap() = Some((at, cause.clone()));
            } else {
                arm_refusal(&host, 0, cause.clone());
            }
            assert_eq!(
                frame.run(&Control::default()).unwrap_err(),
                EvaluationFailure::Kernel(cause)
            );
            assert!(matches!(
                frame.run(&Control::default()),
                Err(EvaluationFailure::Kernel(KernelFailure::InstanceFailed))
            ));
            drop(frame);
            assert!(matches!(
                c.emit_evaluation(&[0], 1, &Control::default()),
                Err(EvaluationFailure::Kernel(KernelFailure::InstanceFailed))
            ));
            drop(c);
            empty(&host);
        }
    }
}
#[test]
fn approx_top_k_owner_first_failed_emit_no_owner_callback_or_host_tail() {
    for intermediate in [false, true] {
        let phase = if intermediate {
            AggregateKernelPhase::Partial
        } else {
            AggregateKernelPhase::Single
        };
        let k = kernel(&[DataType::Int32], phase);
        let host = Arc::new(Host::default());
        let mut state = k
            .create_state_with_allocator(Some(host.clone()), &Control::default())
            .unwrap();
        state.latch();
        let indices = [0];
        let erased: Arc<dyn AggregateStateAllocator> = host.clone();
        let context = AggregateEmissionContext::from_host(&k.contract, &indices, 1, Some(&erased));
        let control = Control {
            trace: Mutex::new(vec![]),
            refusal: Some((0, KernelFailure::Cancelled)),
        };
        let attempts = host.ledger.lock().unwrap().attempts;
        let result = if intermediate {
            k.build_intermediate_evaluation_with_context(
                std::iter::once(&state),
                &context,
                &control,
            )
        } else {
            k.build_final_evaluation_with_context(std::iter::once(&state), &context, &control)
        };
        assert!(matches!(
            result,
            Err(EvaluationFailure::Kernel(KernelFailure::InstanceFailed))
        ));
        assert!(control.trace.lock().unwrap().is_empty());
        assert_eq!(host.ledger.lock().unwrap().attempts, attempts);
        drop(state);
        empty(&host);
    }
}
struct CompileRefusal(CompileControlError);
impl PureCompileControl for CompileRefusal {
    fn checkpoint(&self, _: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert_eq!(units, 0);
        Err(self.0.clone())
    }
}
#[test]
fn approx_top_k_owner_three_original_compile_causes() {
    let k = kernel(&[DataType::Int32], AggregateKernelPhase::Single);
    for cause in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        assert!(
            matches!(k.clone_for_local_phase(k.contract.clone(),&CompileRefusal(cause.clone())),Err(actual)if actual==compile_failure(cause))
        );
    }
}

#[test]
fn approx_top_k_owner_intermediate_final_codec_and_full_merge_data_domain() {
    let host = Arc::new(Host::default());
    let mut p = column(
        kernel(&[DataType::Int32], AggregateKernelPhase::Partial),
        host.clone(),
        2,
    );
    let arrays = [Arc::new(Int32Array::from(vec![Some(3), Some(3), None, Some(9)])) as ArrayRef];
    update(
        &mut p,
        &arrays,
        Selection::all(4),
        &[1, 1, 0, 1],
        &Control::default(),
    )
    .unwrap();
    let encoded = p.emit_evaluation(&[1, 0], 2, &Control::default()).unwrap();
    assert_eq!(encoded.data_type(), &DataType::Binary);
    assert_eq!(encoded.null_count(), 0);
    let mut f = column(
        kernel(&[DataType::Int32], AggregateKernelPhase::Final),
        host.clone(),
        2,
    );
    let contract = f.handle().contract().clone();
    let setup = Control::default();
    let arg = EvaluatedArgument::Column(&encoded);
    let input =
        SelectedAggregateMergeInput::try_new(&contract, Selection::all(2), arg, &setup).unwrap();
    f.prepare_merge_batch_evaluation(&[0, 1], input, &setup)
        .unwrap()
        .run(&Control::default())
        .unwrap();
    let out = f.emit_evaluation(&[0, 1], 2, &Control::default()).unwrap();
    let list = out
        .as_any()
        .downcast_ref::<arrow_array::ListArray>()
        .unwrap();
    assert_eq!(list.value_length(0), 2);
    assert_eq!(list.value_length(1), 1);
    drop(out);
    drop(encoded);
    drop(p);
    drop(f);
    empty(&host);
    let host = Arc::new(Host::default());
    let mut f = column(
        kernel(&[DataType::Int32], AggregateKernelPhase::Final),
        host.clone(),
        2,
    );
    let array = Arc::new(BinaryArray::from(vec![Some(b"bad".as_slice()), None])) as ArrayRef;
    let contract = f.handle().contract().clone();
    let arg = EvaluatedArgument::Column(&array);
    let rows = [0];
    let selection = Selection::try_sparse(2, &rows).unwrap();
    let input = SelectedAggregateMergeInput::try_new(&contract, selection, arg, &setup).unwrap();
    let err = f
        .prepare_merge_batch_evaluation(&[1], input, &setup)
        .unwrap()
        .run(&Control::default())
        .unwrap_err();
    let EvaluationFailure::InvocationData(data) = err else {
        panic!("original codec whole invocation Data")
    };
    assert_eq!(
        data.message(),
        "merge aggregate state: approx_top_k merge payload too short"
    );
    assert_eq!(
        data.projected_fact(),
        novarocks_failure_contract::FailureProjection::failure_fact(
            &DataFailureReason::InvalidEncoding
        )
    );
    assert!(data.original_source().is_some());
    assert_eq!(data.input_rows(), &rows);
    assert_eq!(data.state_indices(), &[1]);
    assert_eq!(data.aggregate_phase(), AggregateInvocationPhase::Merge);
    drop(data);
    drop(f);
    empty(&host);
}

#[test]
fn approx_top_k_owner_empty_struct_setup_is_original_whole_data_before_rows() {
    let host = Arc::new(Host::default());
    let mut c = column(
        kernel(
            &[DataType::Struct(arrow_schema::Fields::empty())],
            AggregateKernelPhase::Single,
        ),
        host.clone(),
        1,
    );
    let arrays = [Arc::new(arrow_array::StructArray::new_empty_fields(0, None)) as ArrayRef];
    let err = update(&mut c, &arrays, Selection::all(0), &[], &Control::default()).unwrap_err();
    let EvaluationFailure::InvocationData(data) = err else {
        panic!("original empty Struct setup Data")
    };
    assert_eq!(
        data.message(),
        "update aggregate state: approx_top_k struct input must have at least 1 field"
    );
    assert_eq!(
        data.projected_fact(),
        novarocks_failure_contract::FailureProjection::failure_fact(
            &DataFailureReason::InvalidValue
        )
    );
    assert!(data.original_source().is_some());
    assert_eq!(data.batch_rows(), 0);
    assert!(data.input_rows().is_empty());
    assert!(data.state_indices().is_empty());
    drop(data);
    drop(c);
    empty(&host);
}

#[test]
fn topk_real_codec_sources_do_not_retain_payload_or_host() {
    use novarocks_failure_contract::FailureProjection;
    use std::error::Error;
    let host = Arc::new(Host::default());
    let host_owner = Arc::downgrade(&host);
    let allocator = HostAggregateAllocator::try_new(host.clone()).unwrap();
    for (bytes, utf8) in [
        (vec![1, 4, 1, 0, 0, 0, 0xff], true),
        (vec![1, 11, 1, 0, 0, 0, b'x'], false),
    ] {
        let source = core::decode_optional_scalar_tracked_observed(
            &bytes,
            &allocator,
            &mut ScalarWork::new(None),
        )
        .unwrap_err();
        assert_eq!(
            source.failure_fact(),
            DataFailureReason::InvalidEncoding.failure_fact()
        );
        let KernelFailure::Source(source) = source else {
            panic!("actual codec source required")
        };
        let actual = source.source().unwrap().source().unwrap();
        if utf8 {
            assert!(actual.is::<std::str::Utf8Error>());
        } else {
            assert!(actual.is::<<arrow_buffer::i256 as std::str::FromStr>::Err>());
        }
    }
    drop(allocator);
    empty(&host);
    drop(host);
    assert!(host_owner.upgrade().is_none());
}

#[test]
fn topk_bad_child_counts_refuse_before_original_scratch_allocation() {
    use novarocks_failure_contract::FailureProjection;
    let host = Arc::new(Host::default());
    let allocator = HostAggregateAllocator::try_new(host.clone()).unwrap();
    let before = host.ledger.lock().unwrap().attempts;
    for tag in [8, 9, 10] {
        let mut bytes = vec![1, tag];
        bytes.extend_from_slice(&u32::MAX.to_le_bytes());
        let error = core::decode_optional_scalar_tracked_observed(
            &bytes,
            &allocator,
            &mut ScalarWork::new(None),
        )
        .unwrap_err();
        assert_eq!(
            error.failure_fact(),
            DataFailureReason::InvalidEncoding.failure_fact()
        );
        assert_eq!(host.ledger.lock().unwrap().attempts, before);
    }
    let mut header = vec![];
    header.extend_from_slice(&5_u32.to_le_bytes());
    header.extend_from_slice(&100_u32.to_le_bytes());
    header.extend_from_slice(&100_u32.to_le_bytes());
    let error =
        core::deserialize_state_tracked_observed(&header, &allocator, &mut ScalarWork::new(None))
            .err()
            .expect("missing entries must fail");
    assert_eq!(
        error.failure_fact(),
        DataFailureReason::InvalidEncoding.failure_fact()
    );
    assert_eq!(host.ledger.lock().unwrap().attempts, before);
    drop(allocator);
    empty(&host);
}

#[test]
fn topk_value_decoder_checks_the_original_value_type_depth_boundary() {
    use novarocks_failure_contract::FailureProjection;
    fn nested(levels: usize) -> Vec<u8> {
        let mut bytes = vec![1, 2];
        bytes.extend_from_slice(&0_i64.to_le_bytes());
        for _ in 0..levels {
            let mut parent = vec![1, 10, 1, 0, 0, 0];
            parent.extend(bytes);
            bytes = parent;
        }
        bytes
    }
    let host = Arc::new(Host::default());
    let allocator = HostAggregateAllocator::try_new(host.clone()).unwrap();
    let maximum = novarocks_type_contract::MAX_VALUE_TYPE_DEPTH;
    let value = core::decode_optional_scalar_tracked_observed(
        &nested(maximum),
        &allocator,
        &mut ScalarWork::new(None),
    )
    .unwrap();
    drop(value);
    let error = core::decode_optional_scalar_tracked_observed(
        &nested(maximum + 1),
        &allocator,
        &mut ScalarWork::new(None),
    )
    .unwrap_err();
    assert_eq!(
        error.failure_fact(),
        DataFailureReason::InvalidEncoding.failure_fact()
    );
    drop(allocator);
    empty(&host);
}

#[test]
fn topk_known_codec_origin_survives_real_diagnostic_refusal_and_host_exit() {
    use novarocks_failure_contract::FailureProjection;
    use std::error::Error;
    let kernel = kernel(&[DataType::Int64], AggregateKernelPhase::Final);
    let host = Arc::new(Host::default());
    let host_owner = Arc::downgrade(&host);
    let allocator = HostAggregateAllocator::try_new(host.clone()).unwrap();
    let control = Control {
        refusal: Some((0, KernelFailure::DeadlineExceeded)),
        ..Default::default()
    };
    let (input_owner, expected, error) = {
        let input: ArrayRef = Arc::new(BinaryArray::from(vec![b"bad".as_slice()]));
        let input_owner = Arc::downgrade(&input);
        let bytes = input
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap()
            .value(0);
        let origin =
            core::deserialize_state_tracked_observed(bytes, &allocator, &mut ScalarWork::new(None))
                .err()
                .expect("actual bad payload must fail");
        let expected = origin.clone();
        let mut sink = FailureSink {
            allocator: &allocator,
            domain: FailureDomain::Mutation {
                contract: &kernel.contract,
                phase: AggregateInvocationPhase::Merge,
                selection: Selection::all(1),
                mapping: &[0],
            },
            control: &control,
        };
        let EvaluationFailure::Kernel(error) = topk_error(origin, &mut sink) else {
            panic!("diagnostic cannot complete")
        };
        assert_eq!(error.failure_fact(), expected.failure_fact());
        let KernelFailure::Source(source) = &error else {
            panic!("actual diagnostic refusal required")
        };
        let source = source
            .source()
            .unwrap()
            .downcast_ref::<TopKDiagnosticFailure>()
            .unwrap();
        assert_eq!(source.origin, expected);
        assert_eq!(source._diagnostic, KernelFailure::DeadlineExceeded);
        assert_eq!(source.phase, AggregateInvocationPhase::Merge);
        assert_eq!(control.trace.lock().unwrap().len(), 1);
        (input_owner, expected, error)
    };
    assert!(input_owner.upgrade().is_none());
    drop(allocator);
    empty(&host);
    drop(host);
    assert!(host_owner.upgrade().is_none());
    assert_eq!(error.failure_fact(), expected.failure_fact());
}
