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

//! Actual COUNT DISTINCT host/control failures at observed ledger positions.
use super::*;
use arrow_array::{StringArray, StructArray};
use arrow_schema::{Field, Fields};
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
        assert!(
            units <= 256,
            "COUNT DISTINCT exceeds the frozen checkpoint extent"
        );
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
        panic!("COUNT DISTINCT never waits")
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
struct Snapshot {
    attempts: usize,
    bytes: usize,
    peak: usize,
    live: Vec<(usize, Layout)>,
    layouts: Vec<Layout>,
}
#[derive(Default)]
struct Ledger {
    attempts: usize,
    bytes: usize,
    peak: usize,
    live: Vec<(usize, Layout)>,
    layouts: Vec<Layout>,
}
#[derive(Default)]
struct Host {
    ledger: Mutex<Ledger>,
    refusal: Mutex<Option<(usize, KernelFailure)>>,
}
impl Host {
    fn snapshot(&self) -> Snapshot {
        let ledger = self.ledger.lock().unwrap();
        Snapshot {
            attempts: ledger.attempts,
            bytes: ledger.bytes,
            peak: ledger.peak,
            live: ledger.live.clone(),
            layouts: ledger.layouts.clone(),
        }
    }
    fn arm(&self, actual_next: usize, offset: usize, cause: KernelFailure) {
        assert_eq!(
            actual_next,
            self.snapshot().attempts,
            "arm only against the actual ledger snapshot"
        );
        *self.refusal.lock().unwrap() = Some((actual_next + offset, cause));
    }
}
impl AggregateStateAllocator for Host {
    fn allocate(&self, layout: Layout) -> Result<NonNull<u8>, KernelFailure> {
        assert_ne!(layout.size(), 0);
        let mut ledger = self.ledger.lock().unwrap();
        let at = ledger.attempts;
        ledger.attempts += 1;
        ledger.layouts.push(layout);
        if let Some((stop, cause)) = &*self.refusal.lock().unwrap()
            && *stop == at
        {
            return Err(cause.clone());
        }
        let pointer = NonNull::new(unsafe { std::alloc::alloc(layout) })
            .ok_or(KernelFailure::ResourceExhausted)?;
        ledger.bytes += layout.size();
        ledger.peak = ledger.peak.max(ledger.bytes);
        ledger.live.push((pointer.as_ptr().addr(), layout));
        Ok(pointer)
    }
    unsafe fn release(&self, pointer: NonNull<u8>, layout: Layout) {
        let mut ledger = self.ledger.lock().unwrap();
        let at = ledger
            .live
            .iter()
            .position(|(address, actual)| *address == pointer.as_ptr().addr() && *actual == layout)
            .expect("exact live block released once");
        ledger.live.swap_remove(at);
        ledger.bytes -= layout.size();
        unsafe { std::alloc::dealloc(pointer.as_ptr(), layout) };
    }
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
fn kernel(types: &[FunctionValueType], phase: AggregateKernelPhase) -> CountDistinctKernel {
    let catalog = super::super::catalogue::build_builtin_engine_function_catalog().unwrap();
    let args = types
        .iter()
        .cloned()
        .map(|value_type| FunctionArgument::Value {
            value_type,
            constant: None,
        })
        .collect::<Vec<_>>();
    let request = FunctionBindingRequest {
        arguments: &args,
        logical_argument_count: args.len(),
        expected_result_type: None,
    };
    let bound = catalog
        .resolve_distinct_aggregate(
            crate::aggregate_types::DistinctAggregate::Count,
            request,
            &Compile,
        )
        .unwrap();
    let selected = Arc::new(bound.selected);
    let context = novarocks_type_contract::RelationEffectContext::call(
        novarocks_type_contract::RelationCallId::new(731),
    );
    let state_context = ExpressionEffectContext {
        use_id: ExpressionUseId::new(734),
        region: EvaluationRegionId::new(50),
        demand: EvaluationDemand::Value,
    };
    let state_type = selected
        .aggregate
        .as_ref()
        .expect("selected count state facts")
        .intermediate_type
        .clone();
    let uses = (0..types.len())
        .map(|n| Some(ExpressionUseId::new(740 + n as u32)))
        .collect::<Vec<_>>();
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
    CountDistinctKernel {
        contract: handle.contract().clone(),
    }
}
fn ty(data_type: DataType) -> FunctionValueType {
    FunctionValueType::from_legacy_declared_projection(data_type, false)
}
fn text(value: &str) -> ArrayRef {
    Arc::new(StringArray::from(vec![value]))
}
fn new_state(kernel: &CountDistinctKernel, host: Arc<Host>) -> CountState {
    kernel
        .create_state_with_allocator(Some(host), &Control::default())
        .unwrap()
}
fn update(
    kernel: &CountDistinctKernel,
    state: &mut CountState,
    values: &[ArrayRef],
    control: &dyn KernelEvaluationControl,
) -> Result<(), KernelFailure> {
    let args = values
        .iter()
        .map(EvaluatedArgument::Column)
        .collect::<Vec<_>>();
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
    kernel: &CountDistinctKernel,
    state: &mut CountState,
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
fn released(host: &Host) {
    let state = host.snapshot();
    assert_eq!(state.bytes, 0);
    assert!(state.live.is_empty());
}
fn metadata_only(host: &Host, initial: &Snapshot) {
    let state = host.snapshot();
    assert_eq!(state.bytes, initial.bytes);
    assert_eq!(state.live, initial.live);
}
fn failed(kernel: &CountDistinctKernel, state: &CountState, host: &Host, initial: &Snapshot) {
    assert!(state.failed);
    assert_eq!(state.values.len(), 0);
    metadata_only(host, initial);
    assert_eq!(
        kernel.retained_bytes(state),
        state.values.allocator.metadata_bytes()
    );
    assert!(matches!(
        kernel.build_final(std::iter::once(state), &Control::default()),
        Err(KernelFailure::InstanceFailed)
    ));
    assert!(matches!(
        kernel.build_intermediate(std::iter::once(state), &Control::default()),
        Err(KernelFailure::InstanceFailed)
    ));
}
fn wire(keys: &[&[u8]], trailing: &[u8]) -> ArrayRef {
    let mut bytes = (keys.len() as u32).to_le_bytes().to_vec();
    for key in keys {
        bytes.extend_from_slice(&(key.len() as u32).to_le_bytes());
        bytes.extend_from_slice(key);
    }
    bytes.extend_from_slice(trailing);
    Arc::new(BinaryArray::from(vec![bytes.as_slice()]))
}
fn nested_tuple() -> Vec<ArrayRef> {
    let inner = Arc::new(StructArray::new(
        Fields::from(vec![
            Field::new("first", DataType::Utf8, false),
            Field::new("second", DataType::Utf8, false),
        ]),
        vec![text("a"), text(&"z".repeat(513))],
        None,
    )) as ArrayRef;
    vec![inner, text(&"y".repeat(513))]
}
#[test]
fn count_actual_metadata_constructor_refusal_and_every_control_point_preserve_seven_causes() {
    let kernel = kernel(&[ty(DataType::Utf8)], AggregateKernelPhase::Single);
    assert!(matches!(
        kernel.create_state_with_allocator(None, &Control::default()),
        Err(KernelFailure::InvalidProgram(_))
    ));
    let probe = Arc::new(Host::default());
    let control = Control::default();
    let state = kernel
        .create_state_with_allocator(Some(probe.clone()), &control)
        .unwrap();
    let initial = probe.snapshot();
    assert_eq!(initial.live.len(), 1);
    assert_eq!(initial.bytes, state.values.allocator.metadata_bytes());
    let checkpoints = control.trace.lock().unwrap().len();
    assert!(checkpoints > 0);
    drop(state);
    released(&probe);
    for cause in causes() {
        let host = Arc::new(Host::default());
        let actual_next = host.snapshot().attempts;
        host.arm(actual_next, 0, cause.clone());
        assert!(
            matches!(kernel.create_state_with_allocator(Some(host.clone()),&Control::default()),Err(error) if error==cause)
        );
        released(&host);
        for stop in 0..checkpoints {
            let host = Arc::new(Host::default());
            let control = Control {
                refusal: Some((stop, cause.clone())),
                ..Control::default()
            };
            assert!(
                matches!(kernel.create_state_with_allocator(Some(host.clone()),&control),Err(error) if error==cause)
            );
            released(&host);
        }
    }
}
#[test]
fn count_actual_borrowed_lookup_refusal_keeps_first_cause_and_releases_prior_keys() {
    let kernel = kernel(&[ty(DataType::Utf8)], AggregateKernelPhase::Single);
    for query in ["abc", "missing"] {
        let values = [text(query)];
        let host = Arc::new(Host::default());
        let mut state = new_state(&kernel, host.clone());
        for key in ["abc", "def", ""] {
            update(&kernel, &mut state, &[text(key)], &Control::default()).unwrap();
        }
        let control = Control::default();
        update(&kernel, &mut state, &values, &control).unwrap();
        let extent = control.trace.lock().unwrap().len();
        assert!(extent > 0);
        drop(state);
        released(&host);

        for stop in 0..extent {
            for cause in causes() {
                let host = Arc::new(Host::default());
                let mut state = new_state(&kernel, host.clone());
                let initial = host.snapshot();
                for key in ["abc", "def", ""] {
                    update(&kernel, &mut state, &[text(key)], &Control::default()).unwrap();
                }
                let stable = host.snapshot();
                let control = Control {
                    refusal: Some((stop, cause.clone())),
                    ..Control::default()
                };
                assert_eq!(update(&kernel, &mut state, &values, &control), Err(cause));
                if query == "abc" {
                    assert_eq!(
                        host.snapshot().attempts,
                        stable.attempts,
                        "duplicate lookup does not reserve even when refused"
                    );
                }
                failed(&kernel, &state, &host, &initial);
                let attempts = host.snapshot().attempts;
                let secondary = Control {
                    refusal: Some((0, KernelFailure::ResourceExhausted)),
                    ..Control::default()
                };
                assert_eq!(
                    update(&kernel, &mut state, &values, &secondary),
                    Err(KernelFailure::InstanceFailed)
                );
                assert!(secondary.trace.lock().unwrap().is_empty());
                assert_eq!(host.snapshot().attempts, attempts);
                drop(state);
                released(&host);
            }
        }
    }
}
#[test]
fn count_actual_hash_reserve_precedes_key_reserve_and_duplicate_insert_never_charges() {
    let kernel = kernel(&[ty(DataType::Utf8)], AggregateKernelPhase::Single);
    let values = [text("abc")];
    let host = Arc::new(Host::default());
    let mut state = new_state(&kernel, host.clone());
    let initial = host.snapshot();
    update(&kernel, &mut state, &values, &Control::default()).unwrap();
    let stable = host.snapshot();
    let operation = &stable.layouts[initial.attempts..];
    assert_eq!(operation.len(), 2);
    assert!(operation[0].size() > 3);
    assert_eq!(operation[1].size(), 3);
    state
        .values
        .insert_with_work(b"abc", &mut ScalarWork::new(None))
        .unwrap();
    assert_eq!(
        host.snapshot(),
        stable,
        "shared state duplicate never reserves or charges"
    );
    update(&kernel, &mut state, &values, &Control::default()).unwrap();
    assert_eq!(
        host.snapshot(),
        stable,
        "flat raw key projection borrows the source without scratch charge"
    );
    drop(state);
    released(&host);
    for stop in 0..operation.len() {
        for cause in causes() {
            let host = Arc::new(Host::default());
            let mut state = new_state(&kernel, host.clone());
            let initial = host.snapshot();
            host.arm(initial.attempts, stop, cause.clone());
            assert_eq!(
                update(&kernel, &mut state, &values, &Control::default()),
                Err(cause)
            );
            failed(&kernel, &state, &host, &initial);
            let attempts = host.snapshot().attempts;
            let control = Control {
                refusal: Some((0, KernelFailure::Cancelled)),
                ..Control::default()
            };
            assert_eq!(
                update(&kernel, &mut state, &values, &control),
                Err(KernelFailure::InstanceFailed)
            );
            assert!(control.trace.lock().unwrap().is_empty());
            assert_eq!(host.snapshot().attempts, attempts);
            drop(state);
            released(&host);
        }
    }
}
#[test]
fn count_actual_tuple_graph_temporary_peak_releases_and_every_allocation_refusal_latches() {
    let values = nested_tuple();
    let types = values
        .iter()
        .map(|value| ty(value.data_type().clone()))
        .collect::<Vec<_>>();
    let kernel = kernel(&types, AggregateKernelPhase::Single);
    let probe = Arc::new(Host::default());
    let mut state = new_state(&kernel, probe.clone());
    let initial = probe.snapshot();
    update(&kernel, &mut state, &values, &Control::default()).unwrap();
    let stable = probe.snapshot();
    let allocations = stable.attempts - initial.attempts;
    assert!(allocations > 2);
    assert!(
        stable.peak > stable.bytes,
        "recursive scalar graph and encoded key were actually temporary"
    );
    assert_eq!(state.values.len(), 1);
    // Re-encoding a nested duplicate has real scratch allocations, but retains
    // no extra set/table/key bytes. The test reports both facts separately.
    update(&kernel, &mut state, &values, &Control::default()).unwrap();
    let duplicate = probe.snapshot();
    assert_eq!(duplicate.bytes, stable.bytes);
    assert_eq!(duplicate.live, stable.live);
    assert!(duplicate.attempts > stable.attempts);
    drop(state);
    released(&probe);
    for stop in 0..allocations {
        for cause in causes() {
            let host = Arc::new(Host::default());
            let mut state = new_state(&kernel, host.clone());
            let initial = host.snapshot();
            host.arm(initial.attempts, stop, cause.clone());
            assert_eq!(
                update(&kernel, &mut state, &values, &Control::default()),
                Err(cause)
            );
            failed(&kernel, &state, &host, &initial);
            drop(state);
            released(&host);
        }
    }
}
#[test]
fn count_actual_decoder_trailing_is_accepted_and_every_temporary_refusal_rolls_back() {
    let kernel = kernel(&[ty(DataType::Utf8)], AggregateKernelPhase::Final);
    let large = "z".repeat(513);
    let values = wire(
        &[b"prefix", large.as_bytes(), b"prefix", b""],
        b"deliberately ignored trailing bytes",
    );
    let probe = Arc::new(Host::default());
    let mut state = new_state(&kernel, probe.clone());
    let initial = probe.snapshot();
    merge(&kernel, &mut state, &values, &Control::default()).unwrap();
    let stable = probe.snapshot();
    let allocations = stable.attempts - initial.attempts;
    assert!(allocations > 2);
    assert_eq!(state.values.len(), 3);
    assert!(stable.peak > stable.bytes);
    drop(state);
    released(&probe);
    for stop in 0..allocations {
        for cause in causes() {
            let host = Arc::new(Host::default());
            let mut state = new_state(&kernel, host.clone());
            let initial = host.snapshot();
            host.arm(initial.attempts, stop, cause.clone());
            assert_eq!(
                merge(&kernel, &mut state, &values, &Control::default()),
                Err(cause)
            );
            failed(&kernel, &state, &host, &initial);
            let attempts = host.snapshot().attempts;
            let control = Control {
                refusal: Some((0, KernelFailure::Cancelled)),
                ..Control::default()
            };
            assert_eq!(
                merge(&kernel, &mut state, &values, &control),
                Err(KernelFailure::InstanceFailed)
            );
            assert!(control.trace.lock().unwrap().is_empty());
            assert_eq!(host.snapshot().attempts, attempts);
            drop(state);
            released(&host);
        }
    }
}
#[test]
fn count_actual_malformed_decoder_drops_earlier_state_and_never_replays_a_valid_prefix() {
    let kernel = kernel(&[ty(DataType::Utf8)], AggregateKernelPhase::Final);
    let host = Arc::new(Host::default());
    let mut state = new_state(&kernel, host.clone());
    let initial = host.snapshot();
    merge(
        &kernel,
        &mut state,
        &wire(&[b"old"], &[]),
        &Control::default(),
    )
    .unwrap();
    assert_eq!(state.values.len(), 1);
    let mut malformed = 2_u32.to_le_bytes().to_vec();
    malformed.extend_from_slice(&6_u32.to_le_bytes());
    malformed.extend_from_slice(b"prefix");
    let input = Arc::new(BinaryArray::from(vec![malformed.as_slice()])) as ArrayRef;
    let weak_input = Arc::downgrade(&input);
    let failure = merge(&kernel, &mut state, &input, &Control::default()).unwrap_err();
    assert_eq!(failure.to_string(), "invalid distinct set encoding");
    {
        use novarocks_failure_contract::FailureProjection;
        assert_eq!(
            failure.failure_fact(),
            crate::DataFailureReason::InvalidEncoding.failure_fact()
        );
    }
    drop(input);
    drop(malformed);
    assert!(weak_input.upgrade().is_none());
    failed(&kernel, &state, &host, &initial);
    let attempts = host.snapshot().attempts;
    assert_eq!(
        merge(
            &kernel,
            &mut state,
            &wire(&[b"later"], &[]),
            &Control::default()
        ),
        Err(KernelFailure::InstanceFailed)
    );
    assert_eq!(host.snapshot().attempts, attempts);
    drop(state);
    released(&host);
}
#[test]
fn count_actual_serializer_temporary_allocations_and_typed_refusal_preserve_only_live_state() {
    let kernel = kernel(&[ty(DataType::Utf8)], AggregateKernelPhase::Partial);
    let values = [text(&"z".repeat(513))];
    let probe = Arc::new(Host::default());
    let mut state = new_state(&kernel, probe.clone());
    update(&kernel, &mut state, &values, &Control::default()).unwrap();
    let stable = probe.snapshot();
    let output = kernel
        .build_intermediate(std::iter::once(&state), &Control::default())
        .unwrap();
    let after = probe.snapshot();
    let allocations = after.attempts - stable.attempts;
    assert!(allocations > 0);
    assert_eq!(after.bytes, stable.bytes);
    assert_eq!(after.live, stable.live);
    assert!(after.peak > stable.bytes);
    let binary = output.as_any().downcast_ref::<BinaryArray>().unwrap();
    assert_eq!(
        u32::from_le_bytes(binary.value(0)[..4].try_into().unwrap()),
        1
    );
    drop(state);
    released(&probe);
    for stop in 0..allocations {
        for cause in causes() {
            let host = Arc::new(Host::default());
            let mut state = new_state(&kernel, host.clone());
            update(&kernel, &mut state, &values, &Control::default()).unwrap();
            let stable = host.snapshot();
            host.arm(stable.attempts, stop, cause.clone());
            assert!(
                matches!(kernel.build_intermediate(std::iter::once(&state),&Control::default()),Err(error) if error==cause)
            );
            let after = host.snapshot();
            assert_eq!(after.bytes, stable.bytes);
            assert_eq!(after.live, stable.live);
            assert!(
                !state.failed,
                "immutable materialization does not mutate a state; the caller owns the call failure latch"
            );
            drop(state);
            released(&host);
        }
    }
}
#[test]
fn count_every_actual_update_merge_serialize_and_final_checkpoint_preserves_seven_typed_causes() {
    let tuple = nested_tuple();
    let types = tuple
        .iter()
        .map(|value| ty(value.data_type().clone()))
        .collect::<Vec<_>>();
    let partial = kernel(&types, AggregateKernelPhase::Partial);
    let final_kernel = kernel(&types, AggregateKernelPhase::Final);
    let host = Arc::new(Host::default());
    let mut state = new_state(&partial, host.clone());
    let update_trace = Control::default();
    update(&partial, &mut state, &tuple, &update_trace).unwrap();
    let serialize_trace = Control::default();
    let encoded = partial
        .build_intermediate(std::iter::once(&state), &serialize_trace)
        .unwrap();
    let final_trace = Control::default();
    partial
        .build_final(std::iter::once(&state), &final_trace)
        .unwrap();
    drop(state);
    released(&host);
    let host = Arc::new(Host::default());
    let mut state = new_state(&final_kernel, host.clone());
    let merge_trace = Control::default();
    merge(&final_kernel, &mut state, &encoded, &merge_trace).unwrap();
    drop(state);
    released(&host);
    let extents = [
        update_trace.trace.lock().unwrap().len(),
        merge_trace.trace.lock().unwrap().len(),
        serialize_trace.trace.lock().unwrap().len(),
        final_trace.trace.lock().unwrap().len(),
    ];
    assert!(extents.iter().all(|extent| *extent > 0));
    for (operation, extent) in extents.into_iter().enumerate() {
        for stop in 0..extent {
            for cause in causes() {
                let host = Arc::new(Host::default());
                let kernel = if operation == 1 {
                    &final_kernel
                } else {
                    &partial
                };
                let mut state = new_state(kernel, host.clone());
                let initial = host.snapshot();
                if operation >= 2 {
                    update(kernel, &mut state, &tuple, &Control::default()).unwrap();
                }
                let stable = host.snapshot();
                let control = Control {
                    refusal: Some((stop, cause.clone())),
                    ..Control::default()
                };
                match operation {
                    0 => assert_eq!(update(kernel, &mut state, &tuple, &control), Err(cause)),
                    1 => assert_eq!(merge(kernel, &mut state, &encoded, &control), Err(cause)),
                    2 => assert!(
                        matches!(kernel.build_intermediate(std::iter::once(&state),&control),Err(error) if error==cause)
                    ),
                    3 => assert!(
                        matches!(kernel.build_final(std::iter::once(&state),&control),Err(error) if error==cause)
                    ),
                    _ => unreachable!(),
                }
                if operation < 2 {
                    failed(kernel, &state, &host, &initial);
                } else {
                    let after = host.snapshot();
                    assert_eq!(after.bytes, stable.bytes);
                    assert_eq!(after.live, stable.live);
                }
                drop(state);
                released(&host);
            }
        }
    }
}
