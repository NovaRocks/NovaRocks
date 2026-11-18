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

//! Real HLL payload phase/state/whole Data/host lifecycle; shared original core only.
use super::*;
use crate::opaque_memory::OpaqueAllocationHost;
use arrow_array::StringArray;
use arrow_schema::Field;
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
        panic!("bitmap union never waits")
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
fn kernel(name: &str, source: FunctionValueType, phase: AggregateKernelPhase) -> PayloadKernel {
    let catalog = super::super::catalogue::hll_payload_aggregate_test_catalog();
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
    let context = novarocks_type_contract::RelationEffectContext::call(
        novarocks_type_contract::RelationCallId::new(331),
    );
    let parameters = SemanticParameters::try_new([]).unwrap();
    let uses = [Some(ExpressionUseId::new(332))];
    let state_type = FunctionValueType::try_with_logical_type(
        DataType::Binary,
        true,
        novarocks_type_contract::ValueLogicalType::Hll,
    )
    .unwrap();
    let state_context = ExpressionEffectContext {
        use_id: ExpressionUseId::new(333),
        region: EvaluationRegionId::new(20),
        demand: EvaluationDemand::Value,
    };
    let argument_uses = if phase.consumes_logical_arguments() {
        CallArgumentUses::SelectedChannels(&uses)
    } else {
        CallArgumentUses::AggregateMerge {
            phase,
            state_context: state_context.into(),
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
    PayloadKernel {
        contract: handle.contract().clone(),
        projection: if name == "hll_union_agg" {
            PayloadProjection::Cardinality
        } else {
            PayloadProjection::Bytes
        },
    }
}
fn column(kernel: PayloadKernel, host: Arc<Host>, groups: usize) -> AggregateStateColumn {
    let handle = PreparedAggregateHandle::from_typed(Arc::new(kernel), &Compile).unwrap();
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
fn update_column(
    column: &mut AggregateStateColumn,
    values: &ArrayRef,
    selection: Selection<'_>,
    mapping: &[usize],
    control: &dyn KernelEvaluationControl,
) -> Result<usize, EvaluationFailure> {
    let contract = column.handle().contract().clone();
    let arguments = [EvaluatedArgument::Column(values)];
    let input =
        SelectedAggregateUpdateInput::try_new(&contract, selection, &arguments, &[], control)?;
    let mut frame = column.prepare_update_batch_evaluation(mapping, input, control)?;
    frame.run(control)?;
    Ok(frame.rows_processed())
}
fn merge_column(
    column: &mut AggregateStateColumn,
    values: &ArrayRef,
    selection: Selection<'_>,
    mapping: &[usize],
    control: &dyn KernelEvaluationControl,
) -> Result<usize, EvaluationFailure> {
    let contract = column.handle().contract().clone();
    let argument = EvaluatedArgument::Column(values);
    let input = SelectedAggregateMergeInput::try_new(&contract, selection, argument, control)?;
    let mut frame = column.prepare_merge_batch_evaluation(mapping, input, control)?;
    frame.run(control)?;
    Ok(frame.rows_processed())
}

fn make(
    name: &str,
    phase: AggregateKernelPhase,
    ty: FunctionValueType,
    host: Arc<Host>,
    groups: usize,
) -> AggregateStateColumn {
    column(kernel(name, ty, phase), host, groups)
}
fn count(array: &ArrayRef) -> Vec<Option<i64>> {
    array
        .as_any()
        .downcast_ref::<arrow_array::Int64Array>()
        .unwrap()
        .iter()
        .collect()
}
#[test]
fn hll_payload_owner_four_phases_three_projections_and_null_empty_groups() {
    for name in ["hll_union", "hll_raw_agg", "hll_union_agg"] {
        let host = Arc::new(Host::default());
        let ty = FunctionValueType::try_with_logical_type(
            DataType::Binary,
            true,
            novarocks_type_contract::ValueLogicalType::Hll,
        )
        .unwrap();
        let mut single = make(
            name,
            AggregateKernelPhase::Single,
            ty.clone(),
            host.clone(),
            3,
        );
        let mut partial = make(
            name,
            AggregateKernelPhase::Partial,
            ty.clone(),
            host.clone(),
            3,
        );
        let input = Arc::new(BinaryArray::from(vec![
            None,
            Some(&[0][..]),
            Some(&[1, 1, 1, 0, 0, 0, 0, 0, 0, 0][..]),
        ])) as ArrayRef;
        for c in [&mut single, &mut partial] {
            update_column(
                c,
                &input,
                Selection::all(3),
                &[0, 1, 2],
                &Control::default(),
            )
            .unwrap();
        }
        let state = partial
            .emit_evaluation(&[2, 0, 1], 3, &Control::default())
            .unwrap();
        let a = state.as_any().downcast_ref::<BinaryArray>().unwrap();
        assert_eq!(a.value(0), &[2, 1, 0, 0, 0, 1, 0, 51]);
        assert!(a.is_null(1));
        assert_eq!(a.value(2), &[0]);
        let mut intermediate = make(
            name,
            AggregateKernelPhase::Intermediate,
            ty.clone(),
            host.clone(),
            3,
        );
        merge_column(
            &mut intermediate,
            &state,
            Selection::all(3),
            &[2, 0, 1],
            &Control::default(),
        )
        .unwrap();
        let merged = intermediate
            .emit_evaluation(&[0, 1, 2], 3, &Control::default())
            .unwrap();
        let mut final_ = make(name, AggregateKernelPhase::Final, ty, host.clone(), 3);
        merge_column(
            &mut final_,
            &merged,
            Selection::all(3),
            &[0, 1, 2],
            &Control::default(),
        )
        .unwrap();
        let output = final_
            .emit_evaluation(&[0, 1, 2], 3, &Control::default())
            .unwrap();
        let original = single
            .emit_evaluation(&[0, 1, 2], 3, &Control::default())
            .unwrap();
        assert_eq!(output.to_data(), original.to_data());
        if name == "hll_union_agg" {
            assert_eq!(count(&output), [None, Some(0), Some(1)]);
        }
        assert_eq!(host.ledger.lock().unwrap().opaque_bytes, 0);
        drop((single, partial, intermediate, final_));
        empty(&host);
    }
}
#[test]
fn hll_payload_owner_unsupported_empty_and_null_are_carrier_contracts() {
    for name in ["hll_union", "hll_raw_agg", "hll_union_agg"] {
        for ty in [
            DataType::Null,
            DataType::Int32,
            DataType::Float64,
            DataType::Decimal128(38, 2),
            DataType::FixedSizeBinary(16),
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
        ] {
            for rows in [0usize, 3] {
                let host = Arc::new(Host::default());
                let mut c = make(
                    name,
                    AggregateKernelPhase::Single,
                    FunctionValueType::from_legacy_declared_projection(ty.clone(), true),
                    host.clone(),
                    1,
                );
                let a = arrow_array::new_null_array(&ty, rows);
                let mapping = vec![0; rows];
                let result = update_column(
                    &mut c,
                    &a,
                    Selection::all(rows),
                    &mapping,
                    &Control::default(),
                );
                let error = result.unwrap_err();
                assert_hll_payload_contract(&error);
                assert_eq!(
                    error.to_string(),
                    format!("hll aggregate expects HLL/BINARY payload input, got {ty:?}")
                );
                drop((error, c));
                empty(&host);
            }
        }
    }
}
#[test]
fn hll_payload_owner_update_merge_original_delayed_recipe_phase_and_mapping() {
    for (phase, stage) in [
        (AggregateKernelPhase::Single, "update aggregate state: "),
        (AggregateKernelPhase::Final, "merge aggregate state: "),
    ] {
        let host = Arc::new(Host::default());
        let mut c = make(
            "hll_union_agg",
            phase,
            FunctionValueType::try_with_logical_type(
                DataType::Binary,
                true,
                novarocks_type_contract::ValueLogicalType::Hll,
            )
            .unwrap(),
            host.clone(),
            2,
        );
        let input = Arc::new(BinaryArray::from(vec![
            None,
            Some(&[1, 1, 1, 0, 0, 0, 0, 0, 0, 0][..]),
            None,
            Some(&[][..]),
        ])) as ArrayRef;
        let rows = [1usize, 3];
        let selection = Selection::try_sparse(4, &rows).unwrap();
        let result = if phase == AggregateKernelPhase::Single {
            update_column(&mut c, &input, selection, &[0, 1], &Control::default())
        } else {
            merge_column(&mut c, &input, selection, &[0, 1], &Control::default())
        };
        let Err(EvaluationFailure::InvocationData(data)) = result else {
            panic!("original delayed payload error")
        };
        assert_eq!(
            data.message(),
            format!("{stage}hll_raw merge payload is empty")
        );
        assert_eq!(data.input_rows(), rows);
        assert_eq!(data.state_indices(), [0, 1]);
        assert_eq!(
            count(&c.emit_evaluation(&[0], 2, &Control::default()).unwrap()),
            [Some(1)]
        );
        assert!(matches!(
            c.emit_evaluation(&[1], 2, &Control::default()),
            Err(EvaluationFailure::Kernel(KernelFailure::InstanceFailed))
        ));
        drop((data, c));
        empty(&host);
    }
}
fn control_run(control: &Control) -> Result<(), EvaluationFailure> {
    let host = Arc::new(Host::default());
    let mut c = make(
        "hll_union_agg",
        AggregateKernelPhase::Single,
        FunctionValueType::try_with_logical_type(
            DataType::Binary,
            true,
            novarocks_type_contract::ValueLogicalType::Hll,
        )
        .unwrap(),
        host.clone(),
        1,
    );
    let mut payload = vec![1, 128];
    for h in 1u64..=128 {
        payload.extend_from_slice(&h.to_le_bytes());
    }
    let input = Arc::new(BinaryArray::from(vec![Some(payload.as_slice()); 3])) as ArrayRef;
    let setup = Control::default();
    let contract = c.handle().contract().clone();
    let args = [EvaluatedArgument::Column(&input)];
    let mapping = [0, 0, 0];
    let selected =
        SelectedAggregateUpdateInput::try_new(&contract, Selection::all(3), &args, &[], &setup)
            .unwrap();
    let mut frame = c
        .prepare_update_batch_evaluation(&mapping, selected, &setup)
        .unwrap();
    let result = frame.run(control);
    if result.is_err() {
        assert!(matches!(
            frame.run(&setup),
            Err(EvaluationFailure::Kernel(KernelFailure::InstanceFailed))
        ))
    }
    drop(frame);
    drop(c);
    empty(&host);
    result
}
#[test]
fn hll_payload_owner_seven_controls_no_tail_real_frame_latch() {
    let trace = Control::default();
    control_run(&trace).unwrap();
    let expected = trace.trace.lock().unwrap().clone();
    assert!(expected.contains(&256));
    for cause in causes() {
        for stop in 0..expected.len() {
            let control = Control {
                refusal: Some((stop, cause.clone())),
                ..Control::default()
            };
            assert_eq!(
                control_run(&control),
                Err(EvaluationFailure::Kernel(cause.clone()))
            );
            assert_eq!(*control.trace.lock().unwrap(), expected[..=stop]);
        }
    }
}
#[test]
fn hll_payload_owner_register_allocation_and_original_serialization_real_host_refusal() {
    for cause in causes() {
        let host = Arc::new(Host::default());
        let mut c = make(
            "hll_union",
            AggregateKernelPhase::Partial,
            FunctionValueType::try_with_logical_type(
                DataType::Binary,
                true,
                novarocks_type_contract::ValueLogicalType::Hll,
            )
            .unwrap(),
            host.clone(),
            1,
        );
        let input = Arc::new(BinaryArray::from(vec![Some(
            &[1, 1, 1, 0, 0, 0, 0, 0, 0, 0][..],
        )])) as ArrayRef;
        let setup = Control::default();
        let contract = c.handle().contract().clone();
        let args = [EvaluatedArgument::Column(&input)];
        let mapping = [0];
        let selected =
            SelectedAggregateUpdateInput::try_new(&contract, Selection::all(1), &args, &[], &setup)
                .unwrap();
        let mut frame = c
            .prepare_update_batch_evaluation(&mapping, selected, &setup)
            .unwrap();
        arm_refusal(&host, 0, cause.clone());
        assert_eq!(
            frame.run(&setup),
            Err(EvaluationFailure::Kernel(cause.clone()))
        );
        drop(frame);
        drop(c);
        empty(&host);
        let host = Arc::new(Host::default());
        let mut c = make(
            "hll_union",
            AggregateKernelPhase::Partial,
            FunctionValueType::try_with_logical_type(
                DataType::Binary,
                true,
                novarocks_type_contract::ValueLogicalType::Hll,
            )
            .unwrap(),
            host.clone(),
            1,
        );
        update_column(&mut c, &input, Selection::all(1), &[0], &setup).unwrap();
        let at = host.ledger.lock().unwrap().opaque_attempts;
        *host.opaque_refusal.lock().unwrap() = Some((at, cause.clone()));
        assert_eq!(
            c.emit_evaluation(&[0], 1, &setup).unwrap_err(),
            EvaluationFailure::Kernel(cause.clone())
        );
        assert_eq!(host.ledger.lock().unwrap().opaque_bytes, 0);
        drop(c);
        empty(&host);
    }
}
struct CompileStop(CompileControlError);
impl PureCompileControl for CompileStop {
    fn checkpoint(&self, _: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert!(units <= 256);
        Err(self.0.clone())
    }
}
#[test]
fn hll_payload_owner_compile_three_causes_and_exact_declared_n1_profiles() {
    let catalog = super::super::catalogue::hll_payload_aggregate_test_catalog();
    for name in ["hll_union", "hll_raw_agg", "hll_union_agg"] {
        for dtype in [
            DataType::Binary,
            DataType::Utf8,
            DataType::Int32,
            DataType::Null,
        ] {
            let arguments = [FunctionArgument::Value {
                value_type: FunctionValueType::from_legacy_declared_projection(dtype, true),
                constant: None,
            }];
            let request = FunctionBindingRequest {
                arguments: &arguments,
                logical_argument_count: 1,
                expected_result_type: None,
            };
            let bound = catalog
                .resolve_bound_user(name, FunctionKind::Aggregate, request, &Compile)
                .unwrap();
            let declaration = catalog
                .pure_overload_declaration_observed(
                    &bound.function_id,
                    FunctionKind::Aggregate,
                    &bound.selected.overload,
                    &Compile,
                )
                .unwrap();
            declaration
                .admit_selected_profile_observed(&bound.selected, 1, &Compile)
                .unwrap();
            for cause in [
                CompileControlError::Cancelled,
                CompileControlError::DeadlineExceeded,
                CompileControlError::ResourceExhausted,
            ] {
                assert!(
                    matches!(catalog.pure_overload_declaration_observed(&bound.function_id, FunctionKind::Aggregate, &bound.selected.overload, &CompileStop(cause.clone())), Err(FunctionSpecializationFailure::Control(actual)) if actual == cause)
                );
            }
        }
    }
}
#[test]
fn hll_payload_owner_four_original_payload_carriers_not_numeric_rehash() {
    let source = [
        None,
        Some(&[0][..]),
        Some(&[1, 1, 1, 0, 0, 0, 0, 0, 0, 0][..]),
    ];
    let text: Vec<Option<&str>> = source
        .iter()
        .map(|v| v.map(|v| std::str::from_utf8(v).unwrap()))
        .collect();
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(BinaryArray::from(source.to_vec())),
        Arc::new(arrow_array::LargeBinaryArray::from(source.to_vec())),
        Arc::new(StringArray::from(text.clone())),
        Arc::new(arrow_array::LargeStringArray::from(text)),
    ];
    for input in arrays {
        let host = Arc::new(Host::default());
        let mut c = make(
            "hll_union_agg",
            AggregateKernelPhase::Single,
            FunctionValueType::from_legacy_declared_projection(input.data_type().clone(), true),
            host.clone(),
            3,
        );
        update_column(
            &mut c,
            &input,
            Selection::all(3),
            &[0, 1, 2],
            &Control::default(),
        )
        .unwrap();
        assert_eq!(
            count(
                &c.emit_evaluation(&[0, 1, 2], 3, &Control::default())
                    .unwrap()
            ),
            [None, Some(0), Some(1)]
        );
        drop(c);
        empty(&host);
    }
}

fn hll_payload_empty_unsupported_probe(
    host: Arc<Host>,
    control: &Control,
) -> Result<usize, EvaluationFailure> {
    let mut column = make(
        "hll_union",
        AggregateKernelPhase::Single,
        FunctionValueType::new(DataType::Null, true),
        host,
        1,
    );
    let input = arrow_array::new_null_array(&DataType::Null, 0);
    update_column(&mut column, &input, Selection::all(0), &[], control)
}
#[test]
fn hll_payload_empty_update_setup_real_host_and_seven_control_first_causes() {
    let host = Arc::new(Host::default());
    let baseline = Control::default();
    let result = hll_payload_empty_unsupported_probe(host.clone(), &baseline);
    let expected_trace = baseline.trace.lock().unwrap().clone();
    assert_hll_payload_contract(result.as_ref().unwrap_err());
    drop(result);
    empty(&host);
    for cause in causes() {
        for stop in 0..expected_trace.len() {
            let host = Arc::new(Host::default());
            let control = Control {
                refusal: Some((stop, cause.clone())),
                ..Control::default()
            };
            assert_eq!(
                hll_payload_empty_unsupported_probe(host.clone(), &control),
                Err(EvaluationFailure::Kernel(cause.clone()))
            );
            assert_eq!(*control.trace.lock().unwrap(), expected_trace[..=stop]);
            empty(&host);
        }
        let host = Arc::new(Host::default());
        let mut column = make(
            "hll_union",
            AggregateKernelPhase::Single,
            FunctionValueType::new(DataType::Null, true),
            host.clone(),
            1,
        );
        let input = arrow_array::new_null_array(&DataType::Null, 0);
        // Carrier rejection is bounded metadata and needs no diagnostic host request.
        let before = host.ledger.lock().unwrap().attempts;
        arm_refusal(&host, 0, cause.clone());
        let error = update_column(
            &mut column,
            &input,
            Selection::all(0),
            &[],
            &Control::default(),
        )
        .unwrap_err();
        assert_hll_payload_contract(&error);
        assert_eq!(host.ledger.lock().unwrap().attempts, before);
        drop(column);
        empty(&host);
    }
}
#[test]
fn hll_payload_empty_supported_four_carriers_update_and_binary_merge_preserve_null() {
    for name in ["hll_union", "hll_raw_agg", "hll_union_agg"] {
        for ty in [
            DataType::Binary,
            DataType::LargeBinary,
            DataType::Utf8,
            DataType::LargeUtf8,
        ] {
            let host = Arc::new(Host::default());
            let mut column = make(
                name,
                AggregateKernelPhase::Single,
                FunctionValueType::from_legacy_declared_projection(ty.clone(), true),
                host.clone(),
                1,
            );
            let a = arrow_array::new_null_array(&ty, 0);
            assert_eq!(
                update_column(&mut column, &a, Selection::all(0), &[], &Control::default())
                    .unwrap(),
                0
            );
            let out = column
                .emit_evaluation(&[0], 1, &Control::default())
                .unwrap();
            assert!(out.is_null(0));
            drop((out, column));
            empty(&host);
        }
        let host = Arc::new(Host::default());
        let mut column = make(
            name,
            AggregateKernelPhase::Final,
            FunctionValueType::try_with_logical_type(
                DataType::Binary,
                true,
                novarocks_type_contract::ValueLogicalType::Hll,
            )
            .unwrap(),
            host.clone(),
            1,
        );
        let a = arrow_array::new_null_array(&DataType::Binary, 0);
        assert_eq!(
            merge_column(&mut column, &a, Selection::all(0), &[], &Control::default()).unwrap(),
            0
        );
        let out = column
            .emit_evaluation(&[0], 1, &Control::default())
            .unwrap();
        assert!(out.is_null(0));
        drop((out, column));
        empty(&host);
    }
}

#[test]
fn hll_payload_failed_emission_all_positions_preserves_real_healthy_prefix_only() {
    for name in ["hll_union", "hll_raw_agg", "hll_union_agg"] {
        for failed_at in [0usize, 1, 256, 319] {
            let host = Arc::new(Host::default());
            let erased: Arc<dyn AggregateStateAllocator> = host.clone();
            let k = kernel(
                name,
                FunctionValueType::try_with_logical_type(
                    DataType::Binary,
                    true,
                    novarocks_type_contract::ValueLogicalType::Hll,
                )
                .unwrap(),
                AggregateKernelPhase::Single,
            );
            let setup = Control::default();
            let mut states: Vec<_> = (0..320)
                .map(|_| {
                    k.create_state_with_allocator(Some(erased.clone()), &setup)
                        .unwrap()
                })
                .collect();
            let input = Arc::new(BinaryArray::from(vec![Some(
                &[1, 1, 1, 0, 0, 0, 0, 0, 0, 0][..],
            )])) as ArrayRef;
            let args = [EvaluatedArgument::Column(&input)];
            let mapping = [failed_at];
            let selected = SelectedAggregateUpdateInput::try_new(
                &k.contract,
                Selection::all(1),
                &args,
                &[],
                &setup,
            )
            .unwrap();
            let prepared = k
                .prepare_update_evaluation(selected, &mapping, Some(erased.clone()), &setup)
                .unwrap();
            arm_refusal(&host, 0, KernelFailure::ResourceExhausted);
            assert!(matches!(
                k.update_row_evaluation(&mut states[failed_at], &prepared, 0, &setup),
                Err(EvaluationFailure::Kernel(KernelFailure::ResourceExhausted))
            ));
            assert!(states[failed_at].failed);
            drop(prepared);
            // This is a new emission probe, while the genuine row failure and
            // the latched state remain unchanged. Forbid all further backing.
            *host.refusal.lock().unwrap() = None;
            arm_refusal(&host, 0, KernelFailure::ResourceExhausted);
            let before = {
                let l = host.ledger.lock().unwrap();
                (l.attempts, l.opaque_attempts)
            };
            for intermediate in [false, true] {
                let control = Control {
                    refusal: (failed_at == 0).then_some((0, KernelFailure::Cancelled)),
                    ..Control::default()
                };
                let result = if intermediate {
                    k.build_intermediate(states.iter(), &control)
                } else {
                    k.build_final(states.iter(), &control)
                };
                assert!(matches!(result, Err(KernelFailure::InstanceFailed)));
                let expected = match failed_at {
                    0 => vec![],
                    1 => vec![0, 0, 0],
                    _ => vec![0, 0, 0, 256],
                };
                assert_eq!(*control.trace.lock().unwrap(), expected);
                let after = {
                    let l = host.ledger.lock().unwrap();
                    (l.attempts, l.opaque_attempts)
                };
                assert_eq!(before, after);
            }
            drop(states);
            drop(k);
            drop(erased);
            empty(&host);
        }
    }
}

fn assert_hll_payload_contract(error: &EvaluationFailure) {
    use novarocks_failure_contract::{FailureClass, FailureProjection};
    let EvaluationFailure::Kernel(error) = error else {
        panic!("HLL payload carrier must be an outer contract failure")
    };
    assert_eq!(error.failure_fact().class(), FailureClass::Invariant);
    assert_eq!(error.failure_fact().reason().code(), 328);
}
