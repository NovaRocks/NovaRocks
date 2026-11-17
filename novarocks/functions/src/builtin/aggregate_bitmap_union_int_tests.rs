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

//! Actual bitmap union state-column, whole Data, host/opaque refusals and exact lifecycle witnesses.
use super::*;
use crate::opaque_memory::OpaqueAllocationHost;
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
fn kernel(name: &str, source: FunctionValueType, phase: AggregateKernelPhase) -> BitmapKernel {
    static CATALOG: std::sync::OnceLock<crate::EngineFunctionCatalog> = std::sync::OnceLock::new();
    let catalog = CATALOG.get_or_init(super::super::catalogue::bitmap_union_int_test_catalog);
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
        novarocks_type_contract::ValueLogicalType::Bitmap,
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
    BitmapKernel {
        contract: handle.contract().clone(),
        projection: match &handle.contract().call().selected().result_type {
            FunctionResultType::Scalar(ty) if ty.data_type() == &DataType::Int64 => {
                BitmapResult::Cardinality
            }
            FunctionResultType::Scalar(ty) if ty.data_type() == &DataType::Binary => {
                BitmapResult::Encoded
            }
            _ => panic!("original bitmap result author"),
        },
    }
}
fn column(kernel: BitmapKernel, host: Arc<Host>, groups: usize) -> AggregateStateColumn {
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
    phase: AggregateKernelPhase,
    ty: FunctionValueType,
    host: Arc<Host>,
    groups: usize,
) -> AggregateStateColumn {
    column(kernel("bitmap_union_int", ty, phase), host, groups)
}
fn counts(output: &ArrayRef) -> Vec<Option<i64>> {
    output
        .as_any()
        .downcast_ref::<arrow_array::Int64Array>()
        .unwrap()
        .iter()
        .collect()
}
#[test]
fn bitmap_union_int_owner_real_all_four_phases_and_fixed_original_state_bytes() {
    let ty = FunctionValueType::new(DataType::Int32, true);
    let values = Arc::new(arrow_array::Int32Array::from(vec![
        Some(-1),
        Some(7),
        Some(7),
        None,
    ])) as ArrayRef;
    let host = Arc::new(Host::default());
    let mut single = make(AggregateKernelPhase::Single, ty.clone(), host.clone(), 3);
    update_column(
        &mut single,
        &values,
        Selection::all(4),
        &[0, 0, 0, 1],
        &Control::default(),
    )
    .unwrap();
    let out = single
        .emit_evaluation(&[2, 0, 1], 3, &Control::default())
        .unwrap();
    assert_eq!(counts(&out), vec![None, Some(2), None]);
    drop(out);
    let mut partial = make(AggregateKernelPhase::Partial, ty.clone(), host.clone(), 3);
    update_column(
        &mut partial,
        &values,
        Selection::all(4),
        &[0, 0, 0, 1],
        &Control::default(),
    )
    .unwrap();
    let p = partial
        .emit_evaluation(&[0, 1, 2], 3, &Control::default())
        .unwrap();
    let binary = p.as_any().downcast_ref::<BinaryArray>().unwrap();
    let set = std::collections::BTreeSet::from([7, u64::MAX]);
    assert_eq!(
        binary.value(0),
        crate::bitmap_value::encode_bitmap_aggregate(&set).unwrap()
    );
    assert!(binary.is_null(1) && binary.is_null(2));
    let mut intermediate = make(
        AggregateKernelPhase::Intermediate,
        ty.clone(),
        host.clone(),
        3,
    );
    merge_column(
        &mut intermediate,
        &p,
        Selection::all(3),
        &[0, 1, 2],
        &Control::default(),
    )
    .unwrap();
    let i = intermediate
        .emit_evaluation(&[0, 1, 2], 3, &Control::default())
        .unwrap();
    assert_eq!(i.to_data(), p.to_data());
    let mut final_column = make(AggregateKernelPhase::Final, ty, host.clone(), 3);
    merge_column(
        &mut final_column,
        &i,
        Selection::all(3),
        &[0, 1, 2],
        &Control::default(),
    )
    .unwrap();
    let f = final_column
        .emit_evaluation(&[2, 0, 1], 3, &Control::default())
        .unwrap();
    assert_eq!(counts(&f), vec![None, Some(2), None]);
    drop((f, i, p, final_column, intermediate, partial, single));
    empty(&host);
}
#[test]
fn bitmap_union_int_owner_full_carriers_nominals_and_null_group_contract() {
    use novarocks_type_contract::ValueLogicalType;
    let mut sources: Vec<(FunctionValueType, ArrayRef)> = vec![];
    macro_rules! integers {
        ($array:ty,$dtype:expr) => {
            sources.push((
                FunctionValueType::from_legacy_declared_projection($dtype, true),
                Arc::new(<$array>::from(vec![Some(1), None, Some(1), Some(7)])),
            ));
        };
    }
    integers!(arrow_array::Int8Array, DataType::Int8);
    integers!(arrow_array::Int16Array, DataType::Int16);
    integers!(arrow_array::Int32Array, DataType::Int32);
    integers!(arrow_array::Int64Array, DataType::Int64);
    integers!(arrow_array::UInt8Array, DataType::UInt8);
    integers!(arrow_array::UInt16Array, DataType::UInt16);
    integers!(arrow_array::UInt32Array, DataType::UInt32);
    integers!(arrow_array::UInt64Array, DataType::UInt64);
    sources.push((
        FunctionValueType::new(DataType::Boolean, true),
        Arc::new(arrow_array::BooleanArray::from(vec![
            Some(true),
            None,
            Some(true),
            Some(false),
        ])),
    ));
    sources.push((
        FunctionValueType::new(DataType::Utf8, true),
        Arc::new(arrow_array::StringArray::from(vec![
            Some("1"),
            None,
            Some("bad"),
            Some("7"),
        ])),
    ));
    sources.push((
        FunctionValueType::new(DataType::LargeUtf8, true),
        Arc::new(arrow_array::LargeStringArray::from(vec![
            Some("1"),
            None,
            Some("bad"),
            Some("7"),
        ])),
    ));
    let small = Arc::new(BinaryArray::from(vec![
        Some(b"1".as_slice()),
        None,
        Some(b"bad".as_slice()),
        Some(b"7".as_slice()),
    ])) as ArrayRef;
    let large = Arc::new(arrow_array::LargeBinaryArray::from(vec![
        Some(b"1".as_slice()),
        None,
        Some(b"bad".as_slice()),
        Some(b"7".as_slice()),
    ])) as ArrayRef;
    sources.push((
        FunctionValueType::try_with_logical_type(
            DataType::Binary,
            true,
            novarocks_type_contract::ValueLogicalType::Bitmap,
        )
        .unwrap(),
        small.clone(),
    ));
    sources.push((
        FunctionValueType::new(DataType::LargeBinary, true),
        large.clone(),
    ));
    sources.push((
        FunctionValueType::try_with_logical_type(DataType::Utf8, true, ValueLogicalType::Json)
            .unwrap(),
        Arc::new(arrow_array::StringArray::from(vec![
            Some("1"),
            None,
            Some("bad"),
            Some("7"),
        ])),
    ));
    sources.push((
        FunctionValueType::try_with_logical_type(
            DataType::LargeBinary,
            true,
            ValueLogicalType::Variant,
        )
        .unwrap(),
        large.clone(),
    ));
    for logical in [
        ValueLogicalType::Hll,
        ValueLogicalType::Bitmap,
        ValueLogicalType::Object,
        ValueLogicalType::Percentile,
    ] {
        sources.push((
            FunctionValueType::try_with_logical_type(DataType::Binary, true, logical).unwrap(),
            small.clone(),
        ));
        sources.push((
            FunctionValueType::try_with_logical_type(DataType::LargeBinary, true, logical).unwrap(),
            large.clone(),
        ));
    }
    for (ty, a) in sources {
        let host = Arc::new(Host::default());
        let mut c = make(AggregateKernelPhase::Single, ty, host.clone(), 3);
        update_column(
            &mut c,
            &a,
            Selection::all(4),
            &[0, 1, 0, 0],
            &Control::default(),
        )
        .unwrap();
        let out = c
            .emit_evaluation(&[2, 0, 1], 3, &Control::default())
            .unwrap();
        assert_eq!(counts(&out), vec![None, Some(2), None]);
        drop((out, c));
        empty(&host);
    }
}
#[test]
fn bitmap_union_int_owner_merge_whole_long_original_data_actual_domain_and_latch() {
    let host = Arc::new(Host::default());
    let mut c = make(
        AggregateKernelPhase::Final,
        FunctionValueType::new(DataType::Int32, true),
        host.clone(),
        2,
    );
    let long = "雪".repeat(600);
    let good = crate::bitmap_value::encode_bitmap_aggregate(&std::collections::BTreeSet::from([7]))
        .unwrap();
    let values = Arc::new(BinaryArray::from(vec![
        None,
        Some(good.as_slice()),
        None,
        Some(long.as_bytes()),
        Some(good.as_slice()),
    ])) as ArrayRef;
    let rows = [1usize, 3];
    let s = Selection::try_sparse(5, &rows).unwrap();
    let failure = merge_column(&mut c, &values, s, &[0, 1], &Control::default()).unwrap_err();
    let EvaluationFailure::InvocationData(data) = failure else {
        panic!("whole real merge Data")
    };
    let expected = format!(
        "{}",
        AggregateFailureStage::Merge.message(&format_args!(
            "bitmap string contains invalid value: {long}"
        ))
    );
    assert_eq!(data.message(), expected);
    assert_eq!(
        novarocks_failure_contract::FailureProjection::failure_fact(&data),
        novarocks_failure_contract::FailureProjection::failure_fact(
            &crate::DataFailureReason::InvalidEncoding
        )
    );
    assert!(data.message().len() > 512);
    assert_eq!(data.input_rows(), &rows);
    assert_eq!(data.state_indices(), &[0, 1]);
    assert_eq!(data.batch_rows(), 5);
    assert_eq!(data.aggregate_phase(), AggregateInvocationPhase::Merge);
    assert!(std::ptr::eq(
        data.aggregate_contract(),
        c.handle().contract().as_ref()
    ));
    assert!(matches!(
        c.emit_evaluation(&[1], 2, &Control::default()),
        Err(EvaluationFailure::Kernel(KernelFailure::InstanceFailed))
    ));
    drop((data, c));
    empty(&host);
}
fn control_run(control: &Control) -> Result<(), EvaluationFailure> {
    let host = Arc::new(Host::default());
    let mut c = make(
        AggregateKernelPhase::Single,
        FunctionValueType::try_with_logical_type(
            DataType::Binary,
            true,
            novarocks_type_contract::ValueLogicalType::Bitmap,
        )
        .unwrap(),
        host.clone(),
        1,
    );
    let payload = crate::bitmap_value::encode_bitmap_aggregate(&(0..320).collect()).unwrap();
    let values = Arc::new(BinaryArray::from(vec![Some(payload.as_slice()); 3])) as ArrayRef;
    let setup = Control::default();
    let contract = c.handle().contract().clone();
    let args = [EvaluatedArgument::Column(&values)];
    let mapping = [0, 0, 0];
    let input =
        SelectedAggregateUpdateInput::try_new(&contract, Selection::all(3), &args, &[], &setup)
            .unwrap();
    let mut frame = c
        .prepare_update_batch_evaluation(&mapping, input, &setup)
        .unwrap();
    let result = frame.run(control);
    if result.is_err() {
        assert!(matches!(
            frame.run(&Control::default()),
            Err(EvaluationFailure::Kernel(KernelFailure::InstanceFailed))
        ));
    }
    drop(frame);

    drop(c);
    empty(&host);
    result
}
#[test]
fn bitmap_union_int_owner_seven_checkpoint_causes_exact_prefix_latch_no_tail() {
    let trace = Control::default();
    control_run(&trace).unwrap();
    let callbacks = trace.trace.lock().unwrap().len();
    assert!(callbacks > 2);
    for stop in 0..callbacks {
        for cause in causes() {
            let control = Control {
                trace: Mutex::new(vec![]),
                refusal: Some((stop, cause.clone())),
            };
            assert_eq!(
                control_run(&control).unwrap_err(),
                EvaluationFailure::Kernel(cause)
            );
            assert_eq!(control.trace.lock().unwrap().len(), stop + 1);
        }
    }
}
#[test]
fn bitmap_union_int_owner_all_tracked_and_opaque_host_refusals_preserve_origin_and_drop() {
    for opaque in [false, true] {
        for cause in causes() {
            let host = Arc::new(Host::default());
            let mut c = make(
                AggregateKernelPhase::Single,
                FunctionValueType::try_with_logical_type(
                    DataType::Binary,
                    true,
                    novarocks_type_contract::ValueLogicalType::Bitmap,
                )
                .unwrap(),
                host.clone(),
                1,
            );
            let payload =
                crate::bitmap_value::encode_bitmap_aggregate(&(0..128).collect()).unwrap();
            let values = Arc::new(BinaryArray::from(vec![
                Some(payload.as_slice()),
                Some(payload.as_slice()),
            ])) as ArrayRef;
            let setup = Control::default();
            let contract = c.handle().contract().clone();
            let args = [EvaluatedArgument::Column(&values)];
            let mapping = [0, 0];
            let input = SelectedAggregateUpdateInput::try_new(
                &contract,
                Selection::all(2),
                &args,
                &[],
                &setup,
            )
            .unwrap();
            let mut frame = c
                .prepare_update_batch_evaluation(&mapping, input, &setup)
                .unwrap();
            if opaque {
                let next = host.ledger.lock().unwrap().opaque_attempts;
                *host.opaque_refusal.lock().unwrap() = Some((next, cause.clone()));
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
fn bitmap_union_int_owner_singleton_second_tree_real_opaque_admission_and_output() {
    for value in [7u64, u64::MAX] {
        let host = Arc::new(Host::default());
        let mut c = make(
            AggregateKernelPhase::Partial,
            FunctionValueType::new(DataType::UInt64, false),
            host.clone(),
            1,
        );
        let values = Arc::new(arrow_array::UInt64Array::from(vec![value])) as ArrayRef;
        update_column(
            &mut c,
            &values,
            Selection::all(1),
            &[0],
            &Control::default(),
        )
        .unwrap();
        let before = host.ledger.lock().unwrap().opaque_attempts;
        let output = c.emit_evaluation(&[0], 1, &Control::default()).unwrap();
        let grants = host.ledger.lock().unwrap().opaque_attempts - before;
        assert_eq!(
            grants, 3,
            "sorted original tree, singleton second tree, original fixed-width Vec"
        );
        assert_eq!(
            output
                .as_any()
                .downcast_ref::<BinaryArray>()
                .unwrap()
                .value(0),
            crate::bitmap_value::encode_bitmap_aggregate(&std::collections::BTreeSet::from([
                value
            ]))
            .unwrap()
        );
        assert_eq!(
            host.ledger.lock().unwrap().opaque_bytes,
            0,
            "all original temporaries destroyed before returning output"
        );
        drop((output, c));
        empty(&host);
    }
}
struct CompileRefusal(CompileControlError);
impl PureCompileControl for CompileRefusal {
    fn checkpoint(&self, _: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert!(units <= 256);
        Err(self.0.clone())
    }
}
#[test]
fn bitmap_union_int_owner_original_profile_admission_and_compile_three_causes() {
    let catalog = super::super::catalogue::bitmap_union_int_test_catalog();
    for (dtype, supported) in [
        (DataType::Int32, true),
        (DataType::Binary, true),
        (DataType::Null, false),
        (DataType::Float64, false),
        (DataType::FixedSizeBinary(16), false),
        (
            DataType::List(Arc::new(arrow_schema::Field::new(
                "item",
                DataType::Int32,
                true,
            ))),
            false,
        ),
    ] {
        let args = [FunctionArgument::Value {
            value_type: FunctionValueType::from_legacy_declared_projection(dtype, true),
            constant: None,
        }];
        let request = FunctionBindingRequest {
            arguments: &args,
            logical_argument_count: 1,
            expected_result_type: None,
        };
        let bound = catalog
            .resolve_bound_user(
                "bitmap_union_int",
                FunctionKind::Aggregate,
                request,
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
        assert_eq!(
            declaration
                .admit_selected_profile_observed(&bound.selected, 1, &Compile)
                .is_ok(),
            supported
        );
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            assert!(
                matches!(declaration.admit_selected_profile_observed(&bound.selected,1,&CompileRefusal(cause.clone())),Err(FunctionBindingError::Control(actual)) if actual==cause)
            );
        }
    }
    let k = kernel(
        "bitmap_union_int",
        FunctionValueType::new(DataType::Int32, true),
        AggregateKernelPhase::Single,
    );
    for cause in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        assert!(
            matches!(k.clone_for_local_phase(Arc::clone(&k.contract),&CompileRefusal(cause.clone())),Err(actual) if actual==compile_failure(cause))
        );
    }
}
#[test]
fn bitmap_union_int_owner_nonnull_empty_bitmap_is_zero_count_and_one_byte_state() {
    let host = Arc::new(Host::default());
    let mut c = make(
        AggregateKernelPhase::Partial,
        FunctionValueType::try_with_logical_type(
            DataType::Binary,
            true,
            novarocks_type_contract::ValueLogicalType::Bitmap,
        )
        .unwrap(),
        host.clone(),
        2,
    );
    let values = Arc::new(BinaryArray::from(vec![Some([0u8].as_slice()), None])) as ArrayRef;
    update_column(
        &mut c,
        &values,
        Selection::all(2),
        &[0, 1],
        &Control::default(),
    )
    .unwrap();
    let before = host.ledger.lock().unwrap().opaque_attempts;
    let output = c.emit_evaluation(&[0, 1], 2, &Control::default()).unwrap();
    let array = output.as_any().downcast_ref::<BinaryArray>().unwrap();
    assert_eq!(array.value(0), [0]);
    assert!(array.is_null(1));
    assert_eq!(
        host.ledger.lock().unwrap().opaque_attempts - before,
        1,
        "only the original one-byte Vec requests backing; empty sorted projection allocates nothing"
    );
    let mut f = make(
        AggregateKernelPhase::Final,
        FunctionValueType::try_with_logical_type(
            DataType::Binary,
            true,
            novarocks_type_contract::ValueLogicalType::Bitmap,
        )
        .unwrap(),
        host.clone(),
        2,
    );
    merge_column(
        &mut f,
        &output,
        Selection::all(2),
        &[0, 1],
        &Control::default(),
    )
    .unwrap();
    let out = f.emit_evaluation(&[0, 1], 2, &Control::default()).unwrap();
    assert_eq!(counts(&out), vec![Some(0), None]);
    drop((out, output, f, c));
    empty(&host);
}
#[test]
fn bitmap_union_int_owner_every_singleton_emit_opaque_refusal_no_tail_and_drop() {
    for stop in 0..3 {
        for cause in causes() {
            let host = Arc::new(Host::default());
            let mut c = make(
                AggregateKernelPhase::Partial,
                FunctionValueType::new(DataType::UInt64, false),
                host.clone(),
                1,
            );
            let values = Arc::new(arrow_array::UInt64Array::from(vec![u64::MAX])) as ArrayRef;
            update_column(
                &mut c,
                &values,
                Selection::all(1),
                &[0],
                &Control::default(),
            )
            .unwrap();
            let next = host.ledger.lock().unwrap().opaque_attempts;
            *host.opaque_refusal.lock().unwrap() = Some((next + stop, cause.clone()));
            assert_eq!(
                c.emit_evaluation(&[0], 1, &Control::default()).unwrap_err(),
                EvaluationFailure::Kernel(cause)
            );
            assert_eq!(host.ledger.lock().unwrap().opaque_attempts, next + stop + 1);
            assert_eq!(host.ledger.lock().unwrap().opaque_bytes, 0);
            drop(c);
            empty(&host);
        }
    }
}

#[test]
fn bitmap_union_int_failed_emission_all_positions_keeps_only_real_healthy_prefix() {
    for failed_at in [0usize, 1, 256, 319] {
        let host = Arc::new(Host::default());
        let erased: Arc<dyn AggregateStateAllocator> = host.clone();
        let k = kernel(
            "bitmap_union_int",
            FunctionValueType::new(DataType::Int32, true),
            AggregateKernelPhase::Partial,
        );
        let setup = Control::default();
        let mut states: Vec<_> = (0..320)
            .map(|_| {
                k.create_state_with_allocator(Some(erased.clone()), &setup)
                    .unwrap()
            })
            .collect();
        let values = Arc::new(arrow_array::Int32Array::from(vec![7])) as ArrayRef;
        let args = [EvaluatedArgument::Column(&values)];
        let mapping = [failed_at];
        let input = SelectedAggregateUpdateInput::try_new(
            &k.contract,
            Selection::all(1),
            &args,
            &[],
            &setup,
        )
        .unwrap();
        let prepared = k
            .prepare_update_evaluation(input, &mapping, Some(erased.clone()), &setup)
            .unwrap();
        arm_refusal(&host, 0, KernelFailure::ResourceExhausted);
        assert!(matches!(
            k.update_row_evaluation(&mut states[failed_at], &prepared, 0, &setup),
            Err(EvaluationFailure::Kernel(KernelFailure::ResourceExhausted))
        ));
        assert!(states[failed_at].failed);
        drop(prepared);
        // A new invocation forbids all backing. The original refusal above is
        // already asserted; resetting this fixture journal does not reset state.
        *host.refusal.lock().unwrap() = None;
        arm_refusal(&host, 0, KernelFailure::ResourceExhausted);
        let before = {
            let l = host.ledger.lock().unwrap();
            (l.attempts, l.opaque_attempts)
        };
        let indices: Vec<_> = (0..320).collect();
        let context =
            AggregateEmissionContext::from_host(&k.contract, &indices, 320, Some(&erased));
        let control = Control {
            refusal: (failed_at == 0).then_some((0, KernelFailure::Cancelled)),
            ..Control::default()
        };
        assert!(matches!(
            k.emit(states.iter(), &context, &control, true),
            Err(EvaluationFailure::Kernel(KernelFailure::InstanceFailed))
        ));
        let expected = match failed_at {
            0 => vec![],
            1 => vec![0],
            _ => vec![0, 256],
        };
        assert_eq!(*control.trace.lock().unwrap(), expected);
        let after = {
            let l = host.ledger.lock().unwrap();
            (l.attempts, l.opaque_attempts)
        };
        assert_eq!(before, after);
        drop(states);
        drop(k);
        drop(erased);
        empty(&host);
    }
}
fn bitmap_agg_column(
    phase: AggregateKernelPhase,
    host: Arc<Host>,
    groups: usize,
) -> AggregateStateColumn {
    column(
        kernel(
            "bitmap_agg",
            FunctionValueType::new(DataType::Int32, true),
            phase,
        ),
        host,
        groups,
    )
}
fn bitmap_agg_bytes(output: &ArrayRef) -> Vec<Option<Vec<u8>>> {
    output
        .as_any()
        .downcast_ref::<BinaryArray>()
        .unwrap()
        .iter()
        .map(|value| value.map(<[u8]>::to_vec))
        .collect()
}
#[test]
fn bitmap_agg_owner_all_four_real_phases_ignore_negative_and_preserve_binary_null_state() {
    let host = Arc::new(Host::default());
    let values = Arc::new(arrow_array::Int32Array::from(vec![
        Some(-1),
        Some(7),
        Some(7),
        None,
    ])) as ArrayRef;
    let bytes =
        crate::bitmap_value::encode_bitmap_aggregate(&std::collections::BTreeSet::from([7]))
            .unwrap();
    let mut single = bitmap_agg_column(AggregateKernelPhase::Single, host.clone(), 3);
    update_column(
        &mut single,
        &values,
        Selection::all(4),
        &[0, 0, 0, 1],
        &Control::default(),
    )
    .unwrap();
    assert_eq!(
        bitmap_agg_bytes(
            &single
                .emit_evaluation(&[2, 0, 1], 3, &Control::default())
                .unwrap()
        ),
        vec![None, Some(bytes.clone()), None]
    );
    let mut partial = bitmap_agg_column(AggregateKernelPhase::Partial, host.clone(), 3);
    update_column(
        &mut partial,
        &values,
        Selection::all(4),
        &[0, 0, 0, 1],
        &Control::default(),
    )
    .unwrap();
    let state = partial
        .emit_evaluation(&[0, 1, 2], 3, &Control::default())
        .unwrap();
    assert_eq!(
        bitmap_agg_bytes(&state),
        vec![Some(bytes.clone()), None, None]
    );
    let mut intermediate = bitmap_agg_column(AggregateKernelPhase::Intermediate, host.clone(), 3);
    merge_column(
        &mut intermediate,
        &state,
        Selection::all(3),
        &[0, 1, 2],
        &Control::default(),
    )
    .unwrap();
    let next = intermediate
        .emit_evaluation(&[0, 1, 2], 3, &Control::default())
        .unwrap();
    assert_eq!(
        bitmap_agg_bytes(&next),
        vec![Some(bytes.clone()), None, None]
    );
    let mut final_column = bitmap_agg_column(AggregateKernelPhase::Final, host.clone(), 3);
    merge_column(
        &mut final_column,
        &next,
        Selection::all(3),
        &[0, 1, 2],
        &Control::default(),
    )
    .unwrap();
    assert_eq!(
        bitmap_agg_bytes(
            &final_column
                .emit_evaluation(&[2, 0, 1], 3, &Control::default())
                .unwrap()
        ),
        vec![None, Some(bytes), None]
    );
    drop((next, state, final_column, intermediate, partial, single));
    empty(&host);
}
fn bitmap_agg_control_probe(control: &Control, emission: bool) -> Result<(), EvaluationFailure> {
    let host = Arc::new(Host::default());
    let mut c = bitmap_agg_column(AggregateKernelPhase::Single, host.clone(), 1);
    let values = Arc::new(arrow_array::Int32Array::from(vec![
        Some(-1),
        Some(7),
        Some(7),
    ])) as ArrayRef;
    let setup = Control::default();
    let result = if emission {
        update_column(&mut c, &values, Selection::all(3), &[0, 0, 0], &setup).unwrap();
        c.emit_evaluation(&[0], 1, control).map(|_| ())
    } else {
        let contract = c.handle().contract().clone();
        let args = [EvaluatedArgument::Column(&values)];
        let input =
            SelectedAggregateUpdateInput::try_new(&contract, Selection::all(3), &args, &[], &setup)
                .unwrap();
        let mapping = [0, 0, 0];
        let mut frame = c
            .prepare_update_batch_evaluation(&mapping, input, &setup)
            .unwrap();
        let result = frame.run(control);
        if result.is_err() {
            assert!(matches!(
                frame.run(&Control::default()),
                Err(EvaluationFailure::Kernel(KernelFailure::InstanceFailed))
            ));
        }
        drop(frame);
        result
    };
    drop(c);
    empty(&host);
    result
}
#[test]
fn bitmap_agg_owner_update_and_final_encoder_seven_causes_prefix_latch_no_tail() {
    for emission in [false, true] {
        let baseline = Control::default();
        bitmap_agg_control_probe(&baseline, emission).unwrap();
        let trace = baseline.trace.lock().unwrap().clone();
        assert!(!trace.is_empty());
        for stop in 0..trace.len() {
            for cause in causes() {
                let control = Control {
                    trace: Mutex::new(vec![]),
                    refusal: Some((stop, cause.clone())),
                };
                assert_eq!(
                    bitmap_agg_control_probe(&control, emission).unwrap_err(),
                    EvaluationFailure::Kernel(cause)
                );
                assert_eq!(*control.trace.lock().unwrap(), trace[..=stop]);
            }
        }
    }
}
#[test]
fn bitmap_agg_owner_final_encoded_singleton_all_real_opaque_refusals_drop() {
    for stop in 0..3 {
        for cause in causes() {
            let host = Arc::new(Host::default());
            let mut c = column(
                kernel(
                    "bitmap_agg",
                    FunctionValueType::new(DataType::UInt64, false),
                    AggregateKernelPhase::Single,
                ),
                host.clone(),
                1,
            );
            let values = Arc::new(arrow_array::UInt64Array::from(vec![u64::MAX])) as ArrayRef;
            update_column(
                &mut c,
                &values,
                Selection::all(1),
                &[0],
                &Control::default(),
            )
            .unwrap();
            let next = host.ledger.lock().unwrap().opaque_attempts;
            *host.opaque_refusal.lock().unwrap() = Some((next + stop, cause.clone()));
            assert_eq!(
                c.emit_evaluation(&[0], 1, &Control::default()).unwrap_err(),
                EvaluationFailure::Kernel(cause)
            );
            assert_eq!(host.ledger.lock().unwrap().opaque_attempts, next + stop + 1);
            assert_eq!(host.ledger.lock().unwrap().opaque_bytes, 0);
            drop(c);
            empty(&host);
        }
    }
}
#[test]
fn bitmap_agg_owner_original_profile_admission_and_compile_three_causes() {
    let catalog = super::super::catalogue::bitmap_union_int_test_catalog();
    for (dtype, supported) in [
        (DataType::Int32, true),
        (DataType::Binary, true),
        (DataType::Null, false),
        (DataType::Float64, false),
        (DataType::FixedSizeBinary(16), false),
        (
            DataType::List(Arc::new(arrow_schema::Field::new(
                "item",
                DataType::Int32,
                true,
            ))),
            false,
        ),
    ] {
        let args = [FunctionArgument::Value {
            value_type: FunctionValueType::from_legacy_declared_projection(dtype, true),
            constant: None,
        }];
        let request = FunctionBindingRequest {
            arguments: &args,
            logical_argument_count: 1,
            expected_result_type: None,
        };
        let bound = catalog
            .resolve_bound_user("bitmap_agg", FunctionKind::Aggregate, request, &Compile)
            .unwrap();
        let declaration = catalog
            .pure_overload_declaration_observed(
                &bound.function_id,
                FunctionKind::Aggregate,
                &bound.selected.overload,
                &Compile,
            )
            .unwrap();
        assert_eq!(
            declaration
                .admit_selected_profile_observed(&bound.selected, 1, &Compile)
                .is_ok(),
            supported
        );
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            assert!(
                matches!(declaration.admit_selected_profile_observed(&bound.selected,1,&CompileRefusal(cause.clone())),Err(FunctionBindingError::Control(actual)) if actual==cause)
            );
        }
    }
    let k = kernel(
        "bitmap_agg",
        FunctionValueType::new(DataType::Int32, true),
        AggregateKernelPhase::Single,
    );
    for cause in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        assert!(
            matches!(k.clone_for_local_phase(Arc::clone(&k.contract),&CompileRefusal(cause.clone())),Err(actual) if actual==compile_failure(cause))
        );
    }
}
#[test]
fn bitmap_shared_output_validation_final_count_seven_causes_keep_origin_no_footer() {
    fn run(control: &Control) -> Result<(), EvaluationFailure> {
        let host = Arc::new(Host::default());
        let mut c = make(
            AggregateKernelPhase::Single,
            FunctionValueType::new(DataType::Int32, true),
            host.clone(),
            1,
        );
        let a = Arc::new(arrow_array::Int32Array::from(vec![7])) as ArrayRef;
        update_column(&mut c, &a, Selection::all(1), &[0], &Control::default()).unwrap();
        let result = c
            .emit_evaluation(&[0], 1, control)
            .map(|value| assert_eq!(counts(&value), vec![Some(1)]));
        drop(c);
        empty(&host);
        result
    }
    let baseline = Control::default();
    run(&baseline).unwrap();
    let trace = baseline.trace.lock().unwrap().clone();
    assert!(!trace.is_empty());
    for stop in 0..trace.len() {
        for cause in causes() {
            let control = Control {
                trace: Mutex::new(vec![]),
                refusal: Some((stop, cause.clone())),
            };
            assert_eq!(run(&control).unwrap_err(), EvaluationFailure::Kernel(cause));
            assert_eq!(*control.trace.lock().unwrap(), trace[..=stop]);
        }
    }
}
