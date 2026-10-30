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

//! Actual host-ledger and all checkpoint refusal probes for the NDV lifecycle.
use super::*;
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
        panic!("NDV never waits")
    }
}
#[derive(Default)]
struct Ledger {
    attempts: usize,
    bytes: usize,
    live: Vec<(usize, Layout)>,
    metadata: Option<(usize, Layout)>,
    registers_allocated: usize,
    registers_released: usize,
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
        let block = (pointer.as_ptr().addr(), layout);
        if ledger.metadata.is_none() {
            ledger.metadata = Some(block);
        }
        ledger.live.push(block);
        if layout.size() == core::HLL_REGISTERS_COUNT {
            ledger.registers_allocated += 1;
        }
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
        if layout.size() == core::HLL_REGISTERS_COUNT {
            ledger.registers_released += 1;
        }
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
fn kernel(name: &str, source: FunctionValueType, phase: AggregateKernelPhase) -> NdvKernel {
    let catalog = super::super::catalogue::ndv_invocation_data_test_catalog();
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
        .resolve_bound_user(name, FunctionKind::Aggregate, request, &Compile)
        .unwrap();
    let selected = Arc::new(bound.selected);
    let context = ExpressionEffectContext {
        use_id: ExpressionUseId::new(331),
        domain: EvaluationDomainId::new(19),
        demand: EvaluationDemand::Value,
    };
    let parameters = SemanticParameters::try_new([]).unwrap();
    let uses = [Some(ExpressionUseId::new(332))];
    let state_type = FunctionValueType::new(DataType::Binary, true);
    let state_context = ExpressionEffectContext {
        use_id: ExpressionUseId::new(333),
        domain: EvaluationDomainId::new(20),
        demand: EvaluationDemand::Value,
    };
    let argument_uses = if phase.consumes_logical_arguments() {
        CallArgumentUses::SelectedChannels(&uses)
    } else {
        CallArgumentUses::AggregateMerge {
            phase,
            state_context,
            state_input_type: &state_type,
        }
    };
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
    NdvKernel {
        contract: handle.contract().clone(),
    }
}
fn column(kernel: NdvKernel, host: Arc<Host>) -> AggregateStateColumn {
    let handle = PreparedAggregateHandle::from_typed(Arc::new(kernel), &Compile).unwrap();
    let mut column =
        AggregateStateColumn::try_new(handle, host, std::num::NonZeroUsize::new(2).unwrap())
            .unwrap();
    column.push(&Control::default()).unwrap();
    column
}
fn run_column(
    column: &mut AggregateStateColumn,
    values: &ArrayRef,
    selection: Selection<'_>,
    control: &dyn KernelEvaluationControl,
) -> Result<usize, EvaluationFailure> {
    let contract = column.handle().contract().clone();
    let arguments = [EvaluatedArgument::Column(values)];
    let mapping = vec![0; selection.len()];
    let input =
        SelectedAggregateUpdateInput::try_new(&contract, selection, &arguments, &[], control)?;
    let mut frame = column.prepare_update_batch_evaluation(&mapping, input, control)?;
    frame.run(control)?;
    Ok(frame.rows_processed())
}
fn assert_released(host: &Host) {
    assert_eq!(host.ledger.lock().unwrap().bytes, 0);
}
#[test]
fn ndv_actual_column_all_null_and_supported_inputs_allocate_no_diagnostic() {
    for source in [DataType::UInt32, DataType::Int64] {
        let host = Arc::new(Host::default());
        let mut column = column(
            kernel(
                "ndv",
                FunctionValueType::from_legacy_declared_projection(source.clone(), true),
                AggregateKernelPhase::Single,
            ),
            host.clone(),
        );
        let values: ArrayRef = if source == DataType::UInt32 {
            Arc::new(arrow_array::UInt32Array::from(vec![None, None, None]))
        } else {
            Arc::new(Int64Array::from(vec![None, None, None]))
        };
        let before = host.ledger.lock().unwrap().attempts;
        assert_eq!(
            run_column(&mut column, &values, Selection::all(3), &Control::default()).unwrap(),
            3
        );
        assert_eq!(host.ledger.lock().unwrap().attempts, before);
        let empty: ArrayRef = Arc::new(arrow_array::UInt32Array::from(Vec::<Option<u32>>::new()));
        if source == DataType::UInt32 {
            assert_eq!(
                run_column(&mut column, &empty, Selection::all(0), &Control::default()).unwrap(),
                0
            );
        }
        drop(column);
        assert_released(&host);
    }
}
#[test]
fn ndv_actual_column_metadata_contract_is_bounded_and_releases_the_actual_input() {
    use arrow_schema::{Field, Fields};
    let fields: Fields = (0..96)
        .map(|i| {
            Arc::new(
                Field::new(format!("original_field_{i}"), DataType::Int64, true).with_metadata(
                    std::collections::HashMap::<String, String>::from([(
                        format!("key_{i}"),
                        format!("value_{i}"),
                    )]),
                ),
            )
        })
        .collect();
    let values: ArrayRef = Arc::new(arrow_array::StructArray::new(
        fields.clone(),
        fields
            .iter()
            .map(|_| Arc::new(Int64Array::from(vec![None, Some(7), Some(8)])) as ArrayRef)
            .collect(),
        Some(arrow_buffer::NullBuffer::from(vec![false, true, true])),
    ));
    let values = crate::test_value_ingress::array_for_binding(
        &values,
        &FunctionValueType::new(values.data_type().clone(), true),
    )
    .unwrap();
    let original_core = format!(
        "hll_raw does not support input type {:?}",
        values.data_type()
    );
    let original = crate::aggregate_format::AggregateFailureStage::Update
        .message(&original_core)
        .to_string();
    assert!(original.len() > 512);
    let host = Arc::new(Host::default());
    let mut column = column(
        kernel(
            "ndv",
            FunctionValueType::from_legacy_declared_projection(values.data_type().clone(), true),
            AggregateKernelPhase::Partial,
        ),
        host.clone(),
    );
    let contract = column.handle().contract().clone();
    let arguments = [EvaluatedArgument::Column(&values)];
    let input = SelectedAggregateUpdateInput::try_new(
        &contract,
        Selection::all(3),
        &arguments,
        &[],
        &Control::default(),
    )
    .unwrap();
    let control = Control::default();
    let mut frame = column
        .prepare_update_batch_evaluation(&[0, 0, 0], input, &control)
        .unwrap();
    let error = frame.run(&control).unwrap_err();
    assert_eq!(frame.rows_processed(), 1);
    assert_eq!(
        frame.run(&Control::default()),
        Err(KernelFailure::InstanceFailed.into())
    );
    assert_hll_input_contract(&error);
    assert!(error.to_string().len() <= novarocks_failure_contract::DIAGNOSTIC_MAX_BYTES);
    assert!(
        error
            .to_string()
            .starts_with("hll_raw does not support input type")
    );
    let weak = Arc::downgrade(&values);
    let attempts = host.ledger.lock().unwrap().attempts;
    let clone = error.clone();
    assert_eq!(error, clone);
    assert_eq!(host.ledger.lock().unwrap().attempts, attempts);
    assert_eq!(frame.retained_preparation_bytes(), 0);
    drop(frame);
    drop(column);
    drop(contract);
    drop(values);
    assert!(weak.upgrade().is_none());
    assert_released(&host);
    assert_hll_input_contract(&clone);
}
#[test]
fn ndv_selected_null_excludes_unsupported_non_null_without_preparing_a_diagnostic() {
    let values: ArrayRef = Arc::new(arrow_array::UInt32Array::from(vec![Some(7), None, Some(9)]));
    let host = Arc::new(Host::default());
    let mut column = column(
        kernel(
            "ndv",
            FunctionValueType::new(DataType::UInt32, true),
            AggregateKernelPhase::Single,
        ),
        host.clone(),
    );
    let before = host.ledger.lock().unwrap().attempts;
    let rows = [1];
    let selection = Selection::try_sparse(3, &rows).unwrap();
    assert_eq!(
        run_column(&mut column, &values, selection, &Control::default()).unwrap(),
        1
    );
    assert_eq!(host.ledger.lock().unwrap().attempts, before);
    drop(column);
    assert_released(&host);
}
#[test]
fn ndv_actual_column_carrier_contract_precedes_every_armed_host_refusal() {
    let values: ArrayRef = Arc::new(arrow_array::UInt32Array::from(vec![Some(7)]));
    for cause in causes() {
        let host = Arc::new(Host::default());
        let mut column = column(
            kernel(
                "ndv",
                FunctionValueType::new(DataType::UInt32, false),
                AggregateKernelPhase::Single,
            ),
            host.clone(),
        );
        let before = host.ledger.lock().unwrap().attempts;
        let baseline = host.ledger.lock().unwrap().bytes;
        arm_refusal(&host, 0, cause);
        let error =
            run_column(&mut column, &values, Selection::all(1), &Control::default()).unwrap_err();
        assert_hll_input_contract(&error);
        assert_eq!(host.ledger.lock().unwrap().attempts, before);
        assert_eq!(host.ledger.lock().unwrap().bytes, baseline);
        drop(column);
        assert_released(&host);
    }
}

#[test]
fn ndv_actual_column_every_preparation_and_run_control_refusal_stops_without_tail() {
    let values: ArrayRef = Arc::new(arrow_array::UInt32Array::from(vec![None, Some(7), Some(8)]));
    let successful = Arc::new(Host::default());
    let mut probe = column(
        kernel(
            "ndv",
            FunctionValueType::new(DataType::UInt32, true),
            AggregateKernelPhase::Single,
        ),
        successful.clone(),
    );
    let trace = Control::default();
    assert_hll_input_contract(
        &run_column(&mut probe, &values, Selection::all(3), &trace).unwrap_err(),
    );
    let callbacks = trace.trace.lock().unwrap().len();
    drop(probe);
    assert_released(&successful);
    for cause in causes() {
        for stop in 0..callbacks {
            let host = Arc::new(Host::default());
            let mut column = column(
                kernel(
                    "ndv",
                    FunctionValueType::new(DataType::UInt32, true),
                    AggregateKernelPhase::Single,
                ),
                host.clone(),
            );
            let control = Control {
                refusal: Some((stop, cause.clone())),
                ..Control::default()
            };
            assert_eq!(
                run_column(&mut column, &values, Selection::all(3), &control),
                Err(cause.clone().into())
            );
            assert_eq!(control.trace.lock().unwrap().len(), stop + 1);
            drop(column);
            assert_released(&host);
        }
    }
}

#[test]
fn ndv_contract_skips_work_completion_and_real_payload_data_skips_optional_footer() {
    let values: ArrayRef = Arc::new(arrow_array::UInt32Array::from(vec![None, Some(7), Some(8)]));
    let host = Arc::new(Host::default());
    let mut column = column(
        kernel(
            "ndv",
            FunctionValueType::new(DataType::UInt32, true),
            AggregateKernelPhase::Single,
        ),
        host.clone(),
    );
    let contract = column.handle().contract().clone();
    let arguments = [EvaluatedArgument::Column(&values)];
    let setup = Control::default();
    let input = SelectedAggregateUpdateInput::try_new(
        &contract,
        Selection::all(3),
        &arguments,
        &[],
        &setup,
    )
    .unwrap();
    let mut frame = column
        .prepare_update_batch_evaluation(&[0, 0, 0], input, &setup)
        .unwrap();
    assert_eq!(frame.retained_preparation_bytes(), 0);
    let run = Control::default();
    let error = frame.run(&run).unwrap_err();
    assert_hll_input_contract(&error);
    // The successful first NULL row retains its work tail. The following
    // originating carrier refusal starts no new cooperative checkpoint.
    assert_eq!(run.trace.lock().unwrap().as_slice(), &[0, 0, 0, 1, 0, 0]);
    let footers = std::cell::Cell::new(0);
    let finished: Result<(), EvaluationFailure> =
        crate::evaluation_failure::finish_evaluation_lifecycle(Err(error.clone()), || {
            footers.set(footers.get() + 1);
            Ok(())
        });
    // Accepted first-cause semantics also protect ordinary outer Kernel
    // failures; this is not resource cleanup or state destruction.
    assert_eq!(footers.get(), 0);
    assert_eq!(finished, Err(error));
    drop(finished);
    drop(frame);
    drop(column);
    assert_released(&host);

    // Exercise the no-footer property with a real value failure, retaining
    // the same host-backed invocation capsule and its exact merge domain.
    let host = Arc::new(Host::default());
    let mut column = self::column(
        kernel(
            "ndv",
            FunctionValueType::new(DataType::Int64, false),
            AggregateKernelPhase::Final,
        ),
        host.clone(),
    );
    let values = malformed_merge_values(&[1]);
    let data = run_merge_column(&mut column, &values, &Control::default()).unwrap_err();
    let EvaluationFailure::InvocationData(capsule) = &data else {
        panic!("malformed HLL bytes are value Data")
    };
    assert_eq!(
        novarocks_failure_contract::FailureProjection::failure_fact(capsule),
        novarocks_failure_contract::FailureProjection::failure_fact(
            &crate::DataFailureReason::InvalidEncoding
        )
    );
    let finished: Result<(), EvaluationFailure> =
        crate::evaluation_failure::finish_evaluation_lifecycle(Err(data.clone()), || {
            panic!("value Data cannot run an optional footer")
        });
    assert_eq!(finished, Err(data));
    drop(finished);
    drop(column);
    assert_released(&host);
}

#[test]
fn ndv_kernel_only_wrapper_refuses_only_the_new_data_declaration_before_mutation() {
    let host = Arc::new(Host::default());
    let mut column = column(
        kernel(
            "ndv",
            FunctionValueType::new(DataType::Int64, false),
            AggregateKernelPhase::Single,
        ),
        host.clone(),
    );
    let values: ArrayRef = Arc::new(Int64Array::from(vec![7]));
    let contract = column.handle().contract().clone();
    let arguments = [EvaluatedArgument::Column(&values)];
    let control = Control::default();
    let input = SelectedAggregateUpdateInput::try_new(
        &contract,
        Selection::all(1),
        &arguments,
        &[],
        &control,
    )
    .unwrap();
    let failure = match column.prepare_update_batch(&[0], input, &control) {
        Err(failure) => failure,
        Ok(_) => panic!("Kernel-only protocol cannot accept Data owner"),
    };
    assert_eq!(
        failure,
        invalid("aggregate requires its invocation Data protocol")
    );
    assert_eq!(
        run_column(&mut column, &values, Selection::all(1), &control).unwrap(),
        1
    );
    let out = column.emit_evaluation(&[0], 1, &control).unwrap();
    assert_eq!(
        out.as_any().downcast_ref::<Int64Array>().unwrap().value(0),
        1
    );
    drop(column);
    assert_released(&host);
}

fn run_merge_column(
    column: &mut AggregateStateColumn,
    values: &ArrayRef,
    control: &dyn KernelEvaluationControl,
) -> Result<usize, EvaluationFailure> {
    let contract = column.handle().contract().clone();
    let input = SelectedAggregateMergeInput::try_new(
        &contract,
        Selection::all(values.len()),
        EvaluatedArgument::Column(values),
        control,
    )?;
    let mapping = vec![0; values.len()];
    let mut frame = column.prepare_merge_batch_evaluation(&mapping, input, control)?;
    frame.run(control)?;
    Ok(frame.rows_processed())
}
fn malformed_merge_values(bad: &[u8]) -> ArrayRef {
    let good = [1, 1, 1, 0, 0, 0, 0, 0, 0, 0];
    Arc::new(BinaryArray::from(vec![
        None,
        Some(good.as_slice()),
        Some(bad),
        Some(good.as_slice()),
    ]))
}
#[test]
fn ndv_actual_merge_all_original_reachable_data_branches_preserve_full_message_and_prefix() {
    for (bad, expected) in [
        (b"".as_slice(), "hll_raw merge payload is empty"),
        ([1].as_slice(), "hll_raw EXPLICIT payload is malformed"),
        (
            [2, 0, 0, 0].as_slice(),
            "hll_raw SPARSE payload is malformed",
        ),
    ] {
        let original =
            core::merge_hll_bytes(&mut HllRawState::default(), bad, &mut HllWork::new(None))
                .unwrap_err();
        assert_eq!(original.to_string(), expected);
        let host = Arc::new(Host::default());
        let mut column = column(
            kernel(
                "ndv",
                FunctionValueType::new(DataType::Int64, false),
                AggregateKernelPhase::Final,
            ),
            host.clone(),
        );
        let values = malformed_merge_values(bad);
        let contract = column.handle().contract().clone();
        let setup = Control::default();
        let input = SelectedAggregateMergeInput::try_new(
            &contract,
            Selection::all(4),
            EvaluatedArgument::Column(&values),
            &setup,
        )
        .unwrap();
        let mut frame = column
            .prepare_merge_batch_evaluation(&[0, 0, 0, 0], input, &setup)
            .unwrap();
        assert!(frame.retained_preparation_bytes() > 0);
        let cause = frame.run(&Control::default()).unwrap_err();
        assert_eq!(frame.rows_processed(), 2);
        // A real valid prefix allocated registers; the original production
        // latch_failure -> core.clear released them while the state is alive.
        {
            let ledger = host.ledger.lock().unwrap();
            assert_eq!(ledger.registers_allocated, 1);
            assert_eq!(ledger.registers_released, 1);
        }
        let EvaluationFailure::InvocationData(data) = cause else {
            panic!("whole Data");
        };
        assert!(std::ptr::eq(data.aggregate_contract(), contract.as_ref()));
        assert_eq!(
            novarocks_failure_contract::FailureProjection::failure_fact(&data),
            novarocks_failure_contract::FailureProjection::failure_fact(
                &crate::DataFailureReason::InvalidEncoding
            )
        );
        assert_eq!(
            data.message(),
            crate::aggregate_format::AggregateFailureStage::Merge
                .message(expected)
                .to_string()
        );
        assert_eq!(
            data.to_string(),
            crate::aggregate_format::AggregateFailureStage::Merge
                .message(&original)
                .to_string()
        );
        assert_eq!(data.aggregate_phase(), AggregateInvocationPhase::Merge);
        assert_eq!(data.aggregate_call_phase(), AggregateKernelPhase::Final);
        assert_eq!(data.input_rows(), &[0, 1, 2, 3]);
        assert_eq!(
            frame.run(&Control::default()),
            Err(KernelFailure::InstanceFailed.into())
        );
        drop(frame);
        drop(column);
        assert_eq!(host.ledger.lock().unwrap().bytes, data.retained_bytes());
        let alias = data.clone();
        assert!(alias.same_backing(&data));
        drop(data);
        // The real shared capsule still owns its actual HostAggregateAllocator
        // reference. Only its final owner drop returns the remaining blocks.
        assert_eq!(host.ledger.lock().unwrap().bytes, alias.retained_bytes());
        drop(alias);
        assert_released(&host);
    }
}
#[test]
fn ndv_actual_valid_and_all_null_merge_prepares_no_diagnostic_backing() {
    for values in [
        Arc::new(BinaryArray::from(vec![None, None])) as ArrayRef,
        Arc::new(BinaryArray::from(vec![None, Some([0].as_slice())])) as ArrayRef,
    ] {
        let host = Arc::new(Host::default());
        let mut column = column(
            kernel(
                "ndv",
                FunctionValueType::new(DataType::Int64, false),
                AggregateKernelPhase::Final,
            ),
            host.clone(),
        );
        let before = host.ledger.lock().unwrap().attempts;
        assert_eq!(
            run_merge_column(&mut column, &values, &Control::default()).unwrap(),
            2
        );
        assert_eq!(host.ledger.lock().unwrap().attempts, before);
        drop(column);
        assert_released(&host);
    }
}
#[test]
fn ndv_actual_merge_each_diagnostic_and_prefix_register_allocation_preserves_seven_causes() {
    let values = malformed_merge_values(&[]);
    let successful = Arc::new(Host::default());
    let mut probe = column(
        kernel(
            "ndv",
            FunctionValueType::new(DataType::Int64, false),
            AggregateKernelPhase::Final,
        ),
        successful.clone(),
    );
    let before = successful.ledger.lock().unwrap().attempts;
    assert!(matches!(
        run_merge_column(&mut probe, &values, &Control::default()),
        Err(EvaluationFailure::InvocationData(_))
    ));
    let attempts = successful.ledger.lock().unwrap().attempts - before;
    drop(probe);
    assert_released(&successful);
    for cause in causes() {
        for offset in 0..attempts {
            let host = Arc::new(Host::default());
            let mut column = column(
                kernel(
                    "ndv",
                    FunctionValueType::new(DataType::Int64, false),
                    AggregateKernelPhase::Final,
                ),
                host.clone(),
            );
            let baseline = host.ledger.lock().unwrap().bytes;
            arm_refusal(&host, offset, cause.clone());
            assert_eq!(
                run_merge_column(&mut column, &values, &Control::default()),
                Err(cause.clone().into())
            );
            assert_eq!(host.ledger.lock().unwrap().bytes, baseline);
            drop(column);
            assert_released(&host);
        }
    }
}
#[test]
fn ndv_actual_merge_each_control_refusal_and_data_first_prefix_has_no_tail() {
    let values = malformed_merge_values(&[1]);
    let successful = Arc::new(Host::default());
    let mut probe = column(
        kernel(
            "ndv",
            FunctionValueType::new(DataType::Int64, false),
            AggregateKernelPhase::Final,
        ),
        successful.clone(),
    );
    let trace = Control::default();
    assert!(matches!(
        run_merge_column(&mut probe, &values, &trace),
        Err(EvaluationFailure::InvocationData(_))
    ));
    let callbacks = trace.trace.lock().unwrap().len();
    drop(probe);
    assert_released(&successful);
    for cause in causes() {
        for stop in 0..callbacks {
            let host = Arc::new(Host::default());
            let mut column = column(
                kernel(
                    "ndv",
                    FunctionValueType::new(DataType::Int64, false),
                    AggregateKernelPhase::Final,
                ),
                host.clone(),
            );
            let control = Control {
                refusal: Some((stop, cause.clone())),
                ..Control::default()
            };
            assert_eq!(
                run_merge_column(&mut column, &values, &control),
                Err(cause.clone().into())
            );
            assert_eq!(control.trace.lock().unwrap().len(), stop + 1);
            drop(column);
            assert_released(&host);
        }
    }
}

#[path = "aggregate_emission_context_tests.rs"]
mod emission_context_tests;

fn assert_hll_input_contract(error: &EvaluationFailure) {
    use novarocks_failure_contract::{FailureClass, FailureProjection};
    let EvaluationFailure::Kernel(error) = error else {
        panic!("HLL carrier metadata must be an outer failure")
    };
    let fact = error.failure_fact();
    assert_eq!(fact.class(), FailureClass::Invariant);
    assert_eq!(fact.reason().code(), 327);
}
