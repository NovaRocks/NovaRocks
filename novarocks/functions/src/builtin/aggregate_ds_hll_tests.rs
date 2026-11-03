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

//! Actual full-data DS HLL consumer witnesses; root runs after reviewed wiring.
use super::*;
use crate::opaque_memory::OpaqueAllocationHost;
use arrow_array::StringArray;
use arrow_schema::Field;
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
        panic!("DS HLL never waits")
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
fn kernel(name: &str, values: &[FunctionValueType], phase: AggregateKernelPhase) -> DsHllKernel {
    let catalog = super::super::catalogue::ds_hll_host_test_catalog();
    let args = values
        .iter()
        .cloned()
        .map(|value_type| FunctionArgument::Value {
            value_type,
            constant: None,
        })
        .collect::<Vec<_>>();
    let request = FunctionBindingRequest {
        arguments: &args,
        logical_argument_count: values.len(),
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
    let state_type =
        FunctionValueType::new(DataType::Binary, name == "ds_hll_count_distinct_union");
    let uses = (0..values.len())
        .map(|i| Some(ExpressionUseId::new(432 + i as u32)))
        .collect::<Vec<_>>();
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
    let prepared = catalog
        .prepare_fresh_selected(
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
            PureCallPreparation::Aggregate {
                arguments: ScopedExpressionEffects::pure_value(context),
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
    DsHllKernel {
        contract: handle.contract().clone(),
        operation: match name {
            "ds_hll_count_distinct" | "approx_count_distinct_hll_sketch" => DsHllOperation::Hash,
            "ds_hll_count_distinct_merge" => DsHllOperation::Count,
            "ds_hll_count_distinct_union" => DsHllOperation::Union,
            _ => panic!("unknown test fixture identity"),
        },
    }
}

fn update(
    kernel: &DsHllKernel,
    state: &mut DsHllState,
    host: Arc<Host>,
    values: &[ArrayRef],
    selection: Selection<'_>,
    mapping: &[usize],
    control: &dyn KernelEvaluationControl,
) -> Result<(), EvaluationFailure> {
    let values = values
        .iter()
        .zip(kernel.contract.logical_argument_types())
        .map(|(array, binding)| {
            crate::test_value_ingress::array_for_binding(array, binding).unwrap()
        })
        .collect::<Vec<_>>();
    let args = values
        .iter()
        .map(EvaluatedArgument::Column)
        .collect::<Vec<_>>();
    let setup = Control::default();
    let input =
        SelectedAggregateUpdateInput::try_new(&kernel.contract, selection, &args, &[], &setup)
            .unwrap();
    let prepared = kernel.prepare_update_evaluation(input, mapping, Some(host), &setup)?;
    for ordinal in 0..selection.len() {
        kernel.update_row_evaluation(state, &prepared, ordinal, control)?;
    }
    Ok(())
}
fn emit(
    kernel: &DsHllKernel,
    state: &DsHllState,
    host: Arc<Host>,
    control: &dyn KernelEvaluationControl,
) -> Result<ArrayRef, EvaluationFailure> {
    let allocator: Arc<dyn AggregateStateAllocator> = host;
    let indices = [19];
    let context =
        AggregateEmissionContext::from_host(&kernel.contract, &indices, 64, Some(&allocator));
    kernel.build_final_evaluation_with_context(std::iter::once(state), &context, control)
}
#[test]
fn ds_hll_host_all_four_identities_original_null_empty_and_full_data() {
    for name in [
        "ds_hll_count_distinct",
        "approx_count_distinct_hll_sketch",
        "ds_hll_count_distinct_merge",
        "ds_hll_count_distinct_union",
    ] {
        let hash = matches!(
            name,
            "ds_hll_count_distinct" | "approx_count_distinct_hll_sketch"
        );
        let ty = if hash {
            DataType::Int64
        } else {
            DataType::Binary
        };
        let kernel = kernel(
            name,
            &[FunctionValueType::from_legacy_declared_projection(
                ty.clone(),
                true,
            )],
            AggregateKernelPhase::Single,
        );
        let host = Arc::new(Host::default());
        let mut state = kernel
            .create_state_with_allocator(Some(host.clone()), &Control::default())
            .unwrap();
        let values = arrow_array::new_null_array(&ty, 321);
        update(
            &kernel,
            &mut state,
            host.clone(),
            &[values],
            Selection::all(321),
            &vec![19; 321],
            &Control::default(),
        )
        .unwrap();
        let output = emit(&kernel, &state, host.clone(), &Control::default()).unwrap();
        assert!(!output.is_null(0));
        if name == "ds_hll_count_distinct_union" {
            assert_eq!(
                crate::datasketches_hll::hll_estimate(
                    output
                        .as_any()
                        .downcast_ref::<arrow_array::BinaryArray>()
                        .unwrap()
                        .value(0)
                )
                .unwrap(),
                0
            );
        } else {
            assert_eq!(
                output
                    .as_any()
                    .downcast_ref::<arrow_array::Int64Array>()
                    .unwrap()
                    .value(0),
                0
            );
        }
        drop(output);
        drop(state);
        empty(&host);
    }
}
#[test]
fn ds_hll_host_sparse_frozen_shape_is_bounded_and_does_not_retain_the_type_owner() {
    let field = Arc::new(Field::new(
        "item",
        DataType::Struct(vec![Field::new("x".repeat(900), DataType::UInt32, true)].into()),
        true,
    ));
    let weak = Arc::downgrade(&field);
    let values = Arc::new(arrow_array::ListArray::new_null(field, 4)) as ArrayRef;
    let kernel = kernel(
        "ds_hll_count_distinct",
        &[FunctionValueType::from_legacy_declared_projection(
            values.data_type().clone(),
            true,
        )],
        AggregateKernelPhase::Single,
    );
    let host = Arc::new(Host::default());
    let mut state = kernel
        .create_state_with_allocator(Some(host.clone()), &Control::default())
        .unwrap();
    let rows = [2];
    let selection = Selection::try_sparse(4, &rows).unwrap();
    let failure = update(
        &kernel,
        &mut state,
        host.clone(),
        std::slice::from_ref(&values),
        selection,
        &[7],
        &Control::default(),
    )
    .unwrap_err();
    use novarocks_failure_contract::{FailureClass, FailureProjection};
    use std::error::Error;
    let EvaluationFailure::Kernel(KernelFailure::Source(ref source)) = failure else {
        panic!("frozen shape must remain an outer typed cause")
    };
    assert_eq!(source.failure_fact().class(), FailureClass::Invariant);
    let expected =
        crate::sketch_hash::prehash_array_value(&values, 2, "ds_hll_count_distinct").unwrap_err();
    assert_eq!(source.failure_fact(), expected.failure_fact());
    let original = source
        .source()
        .unwrap()
        .downcast_ref::<crate::sketch_hash::SketchHashContractFailure>()
        .unwrap();
    assert_eq!(original.failure_fact(), expected.failure_fact());
    assert!(
        source
            .detail()
            .starts_with("ds_hll_count_distinct: unsupported sketch hash input type List")
    );
    assert!(source.detail().len() <= novarocks_failure_contract::DIAGNOSTIC_MAX_BYTES);
    assert!(state.failed);
    assert!(state.accumulator.is_none());
    assert_eq!(state.charge.bytes(), 0);
    assert_eq!(
        update(
            &kernel,
            &mut state,
            host.clone(),
            std::slice::from_ref(&values),
            selection,
            &[7],
            &Control::default()
        )
        .unwrap_err(),
        EvaluationFailure::Kernel(KernelFailure::InstanceFailed)
    );
    drop(state);
    drop(kernel);
    drop(values);
    assert!(weak.upgrade().is_none());
    assert_eq!(source.failure_fact().class(), FailureClass::Invariant);
    empty(&host);
}
#[test]
fn ds_hll_host_every_observed_hash_checkpoint_all_seven_causes_no_tail() {
    let name = "ds_hll_count_distinct";
    let values = Arc::new(StringArray::from(vec!["é中🙂".repeat(1000)])) as ArrayRef;
    let kernel = kernel(
        name,
        &[FunctionValueType::new(DataType::Utf8, false)],
        AggregateKernelPhase::Single,
    );
    let baseline = Control::default();
    let host = Arc::new(Host::default());
    let mut state = kernel
        .create_state_with_allocator(Some(host.clone()), &Control::default())
        .unwrap();
    update(
        &kernel,
        &mut state,
        host.clone(),
        std::slice::from_ref(&values),
        Selection::all(1),
        &[0],
        &baseline,
    )
    .unwrap();
    let checkpoints = baseline.trace.lock().unwrap().len();
    drop(state);
    empty(&host);
    assert!(checkpoints > 5);
    for cause in causes() {
        for at in 0..checkpoints {
            let host = Arc::new(Host::default());
            let mut state = kernel
                .create_state_with_allocator(Some(host.clone()), &Control::default())
                .unwrap();
            let control = Control {
                trace: Mutex::new(vec![]),
                refusal: Some((at, cause.clone())),
            };
            assert_eq!(
                update(
                    &kernel,
                    &mut state,
                    host.clone(),
                    std::slice::from_ref(&values),
                    Selection::all(1),
                    &[0],
                    &control
                )
                .unwrap_err(),
                EvaluationFailure::Kernel(cause.clone())
            );
            assert_eq!(control.trace.lock().unwrap().len(), at + 1);
            assert!(state.failed);
            assert!(state.accumulator.is_none());
            assert_eq!(state.charge.bytes(), 0);
            drop(state);
            empty(&host);
        }
    }
}
#[test]
fn ds_hll_host_every_actual_opaque_refusal_preserves_cause_and_release() {
    let kernel = kernel(
        "ds_hll_count_distinct",
        &[FunctionValueType::new(DataType::Int64, false)],
        AggregateKernelPhase::Single,
    );
    let values = Arc::new(arrow_array::Int64Array::from_iter_values(0..321)) as ArrayRef;
    let host = Arc::new(Host::default());
    let mut state = kernel
        .create_state_with_allocator(Some(host.clone()), &Control::default())
        .unwrap();
    let base = host.ledger.lock().unwrap().opaque_attempts;
    update(
        &kernel,
        &mut state,
        host.clone(),
        std::slice::from_ref(&values),
        Selection::all(321),
        &vec![0; 321],
        &Control::default(),
    )
    .unwrap();
    let frontier = host.ledger.lock().unwrap().opaque_attempts - base;
    drop(state);
    empty(&host);
    assert!(frontier > 1);
    for cause in causes() {
        for at in 0..frontier {
            let host = Arc::new(Host::default());
            let mut state = kernel
                .create_state_with_allocator(Some(host.clone()), &Control::default())
                .unwrap();
            let start = host.ledger.lock().unwrap().opaque_attempts;
            *host.opaque_refusal.lock().unwrap() = Some((start + at, cause.clone()));
            assert_eq!(
                update(
                    &kernel,
                    &mut state,
                    host.clone(),
                    std::slice::from_ref(&values),
                    Selection::all(321),
                    &vec![0; 321],
                    &Control::default()
                )
                .unwrap_err(),
                EvaluationFailure::Kernel(cause.clone())
            );
            assert_eq!(state.charge.bytes(), 0);
            drop(state);
            empty(&host);
        }
    }
}
#[test]
fn ds_hll_host_final_merge_data_actual_domain_first_message() {
    for name in ["ds_hll_count_distinct_merge", "ds_hll_count_distinct_union"] {
        let kernel = kernel(
            name,
            &[FunctionValueType::new(DataType::Binary, true)],
            AggregateKernelPhase::Final,
        );
        let host = Arc::new(Host::default());
        let mut state = kernel
            .create_state_with_allocator(Some(host.clone()), &Control::default())
            .unwrap();
        let values = Arc::new(arrow_array::BinaryArray::from(vec![
            None,
            Some(&[1u8, 2, 3][..]),
            None,
        ])) as ArrayRef;
        let rows = [1];
        let selection = Selection::try_sparse(3, &rows).unwrap();
        let setup = Control::default();
        let input = SelectedAggregateMergeInput::try_new(
            &kernel.contract,
            selection,
            EvaluatedArgument::Column(&values),
            &setup,
        )
        .unwrap();
        let prepared = kernel
            .prepare_merge_evaluation(input, &[8], Some(host.clone()), &setup)
            .unwrap();
        let failure = kernel
            .merge_row_evaluation(&mut state, &prepared, 0, &setup)
            .unwrap_err();
        let EvaluationFailure::InvocationData(data) = failure else {
            panic!("real payload Data")
        };
        assert_eq!(
            data.message(),
            "merge aggregate state: ds_hll preflight: HLL payload requires 8 bytes, got 3"
        );
        assert_eq!(data.input_rows(), &[1]);
        assert_eq!(data.state_indices(), &[8]);
        assert_eq!(data.aggregate_phase(), AggregateInvocationPhase::Merge);
        drop(data);
        drop(prepared);
        drop(state);
        empty(&host);
    }
}
#[test]
fn ds_hll_host_serialization_real_lease_outlives_payload_and_output_control() {
    let kernel = kernel(
        "ds_hll_count_distinct_union",
        &[FunctionValueType::new(DataType::Binary, false)],
        AggregateKernelPhase::Single,
    );
    let host = Arc::new(Host::default());
    let mut state = kernel
        .create_state_with_allocator(Some(host.clone()), &Control::default())
        .unwrap();
    let mut original = HllAccumulator::new_unreserved(4, HllTargetType::Hll4).unwrap();
    for hash in 0..321 {
        original.update_hash_unreserved(hash).unwrap();
    }
    let payload = original.serialize().unwrap();
    let values = Arc::new(arrow_array::BinaryArray::from(vec![payload.as_slice()])) as ArrayRef;
    update(
        &kernel,
        &mut state,
        host.clone(),
        std::slice::from_ref(&values),
        Selection::all(1),
        &[0],
        &Control::default(),
    )
    .unwrap();
    let retained = host.ledger.lock().unwrap().opaque_bytes;
    assert!(retained > 0);
    let trace = Control::default();
    let out = emit(&kernel, &state, host.clone(), &trace).unwrap();
    assert_eq!(
        crate::datasketches_hll::hll_estimate(
            out.as_any()
                .downcast_ref::<arrow_array::BinaryArray>()
                .unwrap()
                .value(0)
        )
        .unwrap(),
        original.estimate().unwrap()
    );
    assert_eq!(host.ledger.lock().unwrap().opaque_bytes, retained);
    drop(out);
    let checkpoints = trace.trace.lock().unwrap().len();
    for cause in causes() {
        for at in 0..checkpoints {
            let control = Control {
                trace: Mutex::new(vec![]),
                refusal: Some((at, cause.clone())),
            };
            assert_eq!(
                emit(&kernel, &state, host.clone(), &control).unwrap_err(),
                EvaluationFailure::Kernel(cause.clone())
            );
            assert_eq!(control.trace.lock().unwrap().len(), at + 1);
            assert_eq!(host.ledger.lock().unwrap().opaque_bytes, retained);
        }
    }
    drop(state);
    empty(&host);
}
#[test]
fn ds_hll_host_missing_capability_refuses_before_accumulator_or_metadata() {
    let kernel = kernel(
        "ds_hll_count_distinct",
        &[FunctionValueType::new(DataType::Int64, false)],
        AggregateKernelPhase::Single,
    );
    assert!(matches!(
        kernel.create_state_with_allocator(None, &Control::default()),
        Err(KernelFailure::InvalidProgram(_))
    ));
    assert!(matches!(
        kernel.create_state_with_allocator(
            Some(Arc::new(UnaccountedAggregateStateAllocator)),
            &Control::default()
        ),
        Err(KernelFailure::InvalidProgram(_))
    ));
}

fn duplicate_set_payload() -> Vec<u8> {
    let mut payload = vec![3, 1, 7, 8, 5, 8, 0, 9];
    payload.extend_from_slice(&2u32.to_le_bytes());
    let coupon = (1u32 << 26) | 1;
    payload.extend_from_slice(&coupon.to_le_bytes());
    payload.extend_from_slice(&coupon.to_le_bytes());
    payload
}
#[test]
fn ds_hll_host_semantic_decoder_data_is_reserved_and_has_no_success_footer() {
    let kernel = kernel(
        "ds_hll_count_distinct_merge",
        &[FunctionValueType::new(DataType::Binary, false)],
        AggregateKernelPhase::Final,
    );
    let payload = duplicate_set_payload();
    let values = Arc::new(arrow_array::BinaryArray::from(vec![payload.as_slice()])) as ArrayRef;
    let run = |control: &Control| {
        let host = Arc::new(Host::default());
        let mut state = kernel
            .create_state_with_allocator(Some(host.clone()), &Control::default())
            .unwrap();
        let setup = Control::default();
        let input = SelectedAggregateMergeInput::try_new(
            &kernel.contract,
            Selection::all(1),
            EvaluatedArgument::Column(&values),
            &setup,
        )
        .unwrap();
        let prepared = kernel
            .prepare_merge_evaluation(input, &[14], Some(host.clone()), &setup)
            .unwrap();
        let result = kernel
            .merge_row_evaluation(&mut state, &prepared, 0, control)
            .unwrap_err();
        assert!(host.ledger.lock().unwrap().opaque_attempts > 0);
        let EvaluationFailure::InvocationData(data) = result else {
            panic!("semantic decoder must publish whole Data")
        };
        assert_eq!(
            data.message(),
            "merge aggregate state: ds_hll: failed to deserialize HLL payload: InvalidData => SET mode contains duplicate coupons"
        );
        assert_eq!(data.state_indices(), &[14]);
        assert!(state.accumulator.is_none());
        assert_eq!(state.charge.bytes(), 0);
        drop(data);
        drop(prepared);
        drop(state);
        empty(&host);
    };
    let trace = Control::default();
    run(&trace);
    let next = trace.trace.lock().unwrap().len();
    run(&Control {
        trace: Mutex::new(vec![]),
        refusal: Some((next, KernelFailure::Cancelled)),
    });
}
#[test]
fn ds_hll_host_scalar_and_constant_carriers_use_actual_sparse_addresses() {
    let kernel = kernel(
        "ds_hll_count_distinct",
        &[FunctionValueType::new(DataType::Int64, false)],
        AggregateKernelPhase::Single,
    );
    let scalar = Arc::new(arrow_array::Int64Array::from(vec![19])) as ArrayRef;
    let source_type = FunctionValueType::new(DataType::Int64, false);
    let pool_source = arrow_array::Int64Array::from(vec![77, 19]);
    let pool = ConstantPool::try_new(
        Arc::new(source_type.try_to_field("original-ds-hll-source").unwrap()),
        source_type,
        pool_source.to_data(),
        ConstantPolicy {
            max_rows: 8,
            max_array_nodes: 1,
            max_logical_elements: 8,
            max_retained_buffer_bytes: 4096,
            max_type_depth: 1,
            max_type_nodes: 1,
            max_dictionary_depth: 0,
            max_metadata_bytes: 4096,
            max_library_validation_work: 65536,
            max_library_validation_bytes: 65536,
        },
        CompilePhase::Validate,
        &Compile,
    )
    .unwrap();
    let constant = pool.value(1).unwrap();
    let rows = [1usize, 4, 8];
    let selection = Selection::try_sparse(9, &rows).unwrap();
    for argument in [
        EvaluatedArgument::Scalar(&scalar),
        EvaluatedArgument::Constant(&constant),
    ] {
        let host = Arc::new(Host::default());
        let mut state = kernel
            .create_state_with_allocator(Some(host.clone()), &Control::default())
            .unwrap();
        let args = [argument];
        let setup = Control::default();
        let input =
            SelectedAggregateUpdateInput::try_new(&kernel.contract, selection, &args, &[], &setup)
                .unwrap();
        let prepared = kernel
            .prepare_update_evaluation(input, &[0, 0, 0], Some(host.clone()), &setup)
            .unwrap();
        for ordinal in 0..selection.len() {
            kernel
                .update_row_evaluation(&mut state, &prepared, ordinal, &setup)
                .unwrap();
        }
        assert_eq!(state.accumulator.as_ref().unwrap().estimate().unwrap(), 1);
        drop(prepared);
        drop(state);
        empty(&host);
    }
}

#[test]
fn ds_hll_host_specialization_observer_all_three_control_causes_no_tail() {
    struct CompileCounter {
        trace: Mutex<Vec<u32>>,
        refusal: Option<(usize, CompileControlError)>,
    }
    impl PureCompileControl for CompileCounter {
        fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
            assert_eq!(phase, CompilePhase::FunctionSpecialization);
            assert!(units <= 256);
            let mut trace = self.trace.lock().unwrap();
            let at = trace.len();
            if let Some((stop, _)) = self.refusal {
                assert!(at <= stop, "callback after compilation refusal");
            }
            trace.push(units);
            match self.refusal.clone() {
                Some((stop, cause)) if at == stop => Err(cause),
                _ => Ok(()),
            }
        }
    }
    let kernel = kernel(
        "ds_hll_count_distinct",
        &[
            FunctionValueType::new(DataType::Int64, true),
            FunctionValueType::new(DataType::Int64, true),
            FunctionValueType::new(DataType::Utf8, true),
        ],
        AggregateKernelPhase::Single,
    );
    let run = |control: &dyn PureCompileControl| {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)
            .map_err(compile_failure)?;
        validate_contract(&kernel.contract, &mut work)?;
        work.finish().map_err(compile_failure)
    };
    let baseline = CompileCounter {
        trace: Mutex::new(vec![]),
        refusal: None,
    };
    run(&baseline).unwrap();
    for at in 0..baseline.trace.lock().unwrap().len() {
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let control = CompileCounter {
                trace: Mutex::new(vec![]),
                refusal: Some((at, cause.clone())),
            };
            assert_eq!(run(&control).unwrap_err(), compile_failure(cause));
            assert_eq!(control.trace.lock().unwrap().len(), at + 1);
        }
    }
}

#[test]
fn ds_hll_host_every_actual_owned_frontier_all_seven_causes_and_last_drop() {
    let value = Arc::new(arrow_array::BinaryArray::from(vec![Some(&[1u8, 2, 3][..])])) as ArrayRef;
    let kernel = kernel(
        "ds_hll_count_distinct_union",
        &[FunctionValueType::new(DataType::Binary, false)],
        AggregateKernelPhase::Single,
    );
    for cause in causes() {
        let host = Arc::new(Host::default());
        arm_refusal(&host, 0, cause.clone());
        let error = kernel
            .create_state_with_allocator(Some(host.clone()), &Control::default())
            .err()
            .expect("real metadata refusal");
        assert_eq!(error, cause);
        empty(&host);
    }
    let baseline = Arc::new(Host::default());
    let mut state = kernel
        .create_state_with_allocator(Some(baseline.clone()), &Control::default())
        .unwrap();
    let start = baseline.ledger.lock().unwrap().attempts;
    let error = update(
        &kernel,
        &mut state,
        baseline.clone(),
        std::slice::from_ref(&value),
        Selection::all(1),
        &[7],
        &Control::default(),
    )
    .unwrap_err();
    assert!(matches!(error, EvaluationFailure::InvocationData(_)));
    let frontiers = baseline.ledger.lock().unwrap().attempts - start;
    assert!(frontiers > 2, "actual prepare, diagnostic, domain backing");
    drop(error);
    drop(state);
    empty(&baseline);
    for cause in causes() {
        for offset in 0..frontiers {
            let host = Arc::new(Host::default());
            let mut state = kernel
                .create_state_with_allocator(Some(host.clone()), &Control::default())
                .unwrap();
            arm_refusal(&host, offset, cause.clone());
            let error = update(
                &kernel,
                &mut state,
                host.clone(),
                std::slice::from_ref(&value),
                Selection::all(1),
                &[7],
                &Control::default(),
            )
            .unwrap_err();
            assert_eq!(error, EvaluationFailure::Kernel(cause.clone()));
            drop(error);
            drop(state);
            empty(&host);
        }
    }
}

#[cfg(test)]
#[path = "aggregate_ds_hll_local_stage_tests.rs"]
mod aggregate_ds_hll_local_stage_tests;
