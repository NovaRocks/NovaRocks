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

use super::*;
use crate::{
    FunctionArgument, FunctionSpecializationFailure, FunctionValueType, ScopedExpressionEffects,
    specialize_scalar,
};
use arrow_schema::DataType;
use novarocks_type_contract::{
    DecimalOverflowPolicy, EvaluationDemand, EvaluationRegionId, ExpressionEffectContext,
    ExpressionUseId, SemanticParameters,
};
fn owner(name: &str) -> DsHllStateOwner {
    let signatures = super::super::registry::builtin_scalar_declarations()
        .into_iter()
        .find(|(candidate, _)| candidate == name)
        .unwrap()
        .1;
    let (declaration, resolver) = super::super::catalogue::scalar_definition_parts(
        name,
        &signatures,
        novarocks_type_contract::FunctionKind::Scalar,
    )
    .unwrap();
    DsHllStateOwner::new(name, declaration, resolver).unwrap()
}
fn context() -> ExpressionEffectContext {
    ExpressionEffectContext {
        use_id: ExpressionUseId::new(41),
        region: EvaluationRegionId::new(7),
        demand: EvaluationDemand::Value,
    }
}

fn request(arguments: &[FunctionArgument]) -> FunctionBindingRequest<'_> {
    FunctionBindingRequest {
        expected_result_type: None,
        arguments,
        logical_argument_count: arguments.len(),
    }
}

fn input<'a>(
    owner: &'a DsHllStateOwner,
    selected: &'a FunctionBindingSelection,
    arguments: &'a [FunctionArgument],
    parameters: &'a SemanticParameters,
    uses: &'a [Option<ExpressionUseId>],
) -> CallEffectInput<'a> {
    CallEffectInput {
        context: context(),
        argument_uses: crate::CallArgumentUses::SelectedChannels(uses),
        function_id: owner.declaration.function_id(),
        kind: FunctionKind::Scalar,
        selected,
        request: request(arguments),
        environment: &[],
        parameters,
        decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
        proof_scope: CallProofScope::Unconditional,
    }
}

fn argument(ty: FunctionValueType) -> FunctionArgument {
    FunctionArgument::Value {
        value_type: ty,
        constant: None,
    }
}

pub(crate) fn prepared_for_test(
    name: &str,
    sources: &[FunctionValueType],
) -> Result<Arc<dyn PreparedScalarKernel>, FunctionSpecializationFailure> {
    prepared_with_control(name, sources, crate::binding_test_control())
}
pub(crate) fn prepared_with_control(
    name: &str,
    sources: &[FunctionValueType],
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<Arc<dyn PreparedScalarKernel>, FunctionSpecializationFailure> {
    if !operation(name) {
        return Err(FunctionSpecializationFailure::Binding(
            FunctionBindingError::UnknownFunction,
        ));
    }
    let owner = owner(name);
    let arguments = sources.iter().cloned().map(argument).collect::<Vec<_>>();
    let selected = Arc::new(owner.resolve(request(&arguments), control)?);
    let parameters = SemanticParameters::try_new([]).unwrap();
    let uses = sources
        .iter()
        .enumerate()
        .map(|(i, _)| Some(ExpressionUseId::new(i as u32 + 42)))
        .collect::<Vec<_>>();
    specialize_scalar(
        &owner,
        input(&owner, &selected, &arguments, &parameters, &uses),
        selected.clone(),
        ScopedExpressionEffects::pure_value(context()),
        control,
    )
    .map(|call| call.into_prepared())
}

use crate::kernel_control::{internal, invalid};
use crate::opaque_memory::OpaqueAllocationHost;
use crate::{
    AggregateStateAllocator, EvaluatedArgument, KernelDiagnostic, ScalarEvaluationInstance,
    Selection,
};
use arrow_array::{Array, ArrayRef, BinaryArray, Int64Array, StringArray};
use std::{alloc::Layout, ptr::NonNull, sync::Mutex, time::Duration};
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

fn prepared(arrays: &[ArrayRef]) -> Arc<dyn PreparedScalarKernel> {
    let types = arrays
        .iter()
        .map(|a| FunctionValueType::from_legacy_declared_projection(a.data_type().clone(), true))
        .collect::<Vec<_>>();
    prepared_for_test("ds_hll_count_distinct_state", &types).unwrap()
}
fn instance(arrays: &[ArrayRef], host: &Arc<Host>) -> ScalarEvaluationInstance {
    ScalarEvaluationInstance::instantiate_with_allocator(prepared(arrays), Some(host.clone()))
        .unwrap()
}
fn args(arrays: &[ArrayRef]) -> Vec<EvaluatedArgument<'_>> {
    arrays.iter().map(EvaluatedArgument::Column).collect()
}
#[test]
fn ds_scalar_all_three_arity_same_original_payload_null_and_tuning_default() {
    for arity in 1..=3 {
        let mut arrays =
            vec![Arc::new(StringArray::from(vec![Some("a"), None, Some("b")])) as ArrayRef];
        if arity >= 2 {
            arrays.push(Arc::new(Int64Array::from(vec![None, Some(999), Some(10)])));
        }
        if arity >= 3 {
            arrays.push(Arc::new(StringArray::from(vec![
                None,
                Some("bad"),
                Some("hll_8"),
            ])));
        }
        let original = super::super::ds_hll_state_core::evaluate(
            &arrays[0],
            arrays.get(1).cloned(),
            arrays.get(2).cloned(),
        )
        .unwrap();
        let host = Arc::new(Host::default());
        let mut kernel = instance(&arrays, &host);
        let arguments = args(&arrays);
        let output = kernel
            .evaluate(Selection::all(3), &arguments, &Control::default())
            .unwrap();
        assert!(output.errors().is_empty());
        let original = original.as_any().downcast_ref::<BinaryArray>().unwrap();
        let actual = output
            .values()
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        for row in 0..3 {
            assert_eq!(actual.is_null(row), original.is_null(row));
            if !actual.is_null(row) {
                assert_eq!(actual.value(row), original.value(row));
            }
        }
        drop(output);
        drop(kernel);
        empty(&host);
    }
}
fn long_target() -> ArrayRef {
    use arrow_array::builder::{ListBuilder, StringBuilder};
    let mut b = ListBuilder::new(StringBuilder::new());
    for _ in 0..90 {
        b.values().append_value("unbounded-original-诊断");
    }
    b.append(true);
    b.append(false);
    Arc::new(b.finish())
}
#[test]
fn ds_scalar_numeric_value_cause_keeps_reason_remap_clone_and_last_drop() {
    use novarocks_failure_contract::{FailureClass, FailureProjection};
    let arrays = vec![
        Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef,
        Arc::new(Int64Array::from(vec![1024, 17])),
    ];
    let original =
        super::super::ds_hll_state_core::evaluate(&arrays[0], Some(arrays[1].clone()), None)
            .unwrap_err();
    assert_eq!(original.failure_fact().class(), FailureClass::Data);
    assert_eq!(
        original.failure_fact(),
        crate::DataFailureReason::NumericOutOfRange.failure_fact()
    );
    let mut source: &(dyn std::error::Error + 'static) = &original;
    let mut actual_range_source = false;
    while let Some(next) = source.source() {
        actual_range_source |= next.is::<std::num::TryFromIntError>();
        source = next;
    }
    assert!(
        actual_range_source,
        "raw required call retains the actual conversion cause"
    );
    let original = original.to_string();
    let host = Arc::new(Host::default());
    let mut kernel = instance(&arrays, &host);
    let arguments = args(&arrays);
    let rows = [0];
    let output = kernel
        .evaluate(
            Selection::try_sparse(2, &rows).unwrap(),
            &arguments,
            &Control::default(),
        )
        .unwrap();
    assert!(output.values().is_null(0));
    assert_eq!(output.errors()[0].message(), original);
    assert_eq!(
        output.errors()[0].reason(),
        crate::DataFailureReason::NumericOutOfRange
    );
    let remapped = output.errors()[0].with_selected_ordinal(9);
    assert_eq!(remapped.selected_ordinal(), 9);
    assert_eq!(remapped.message(), original);
    assert_eq!(
        remapped.reason(),
        crate::DataFailureReason::NumericOutOfRange
    );
    assert!(host.ledger.lock().unwrap().bytes > original.len());
    drop(output);
    drop(kernel);
    assert!(
        host.ledger.lock().unwrap().bytes > 0,
        "a real Data alias retains its diagnostic backing"
    );
    drop(remapped);
    empty(&host);
}
#[test]
fn ds_scalar_tuning_carrier_contract_is_bounded_latched_and_drops_actual_input() {
    use novarocks_failure_contract::{FailureClass, FailureProjection};
    let arrays = vec![
        Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef,
        Arc::new(Int64Array::from(vec![17, 17])),
        long_target(),
    ];
    let weak = Arc::downgrade(&arrays[2]);
    let original = super::super::ds_hll_state_core::evaluate(
        &arrays[0],
        Some(arrays[1].clone()),
        Some(arrays[2].clone()),
    )
    .unwrap_err();
    assert_eq!(original.failure_fact().class(), FailureClass::Invariant);
    assert!(original.to_string().len() <= 512);
    let host = Arc::new(Host::default());
    let mut kernel = instance(&arrays, &host);
    let before_opaque = host.ledger.lock().unwrap().opaque_attempts;
    let arguments = args(&arrays);
    let rows = [0];
    let failure = kernel
        .evaluate(
            Selection::try_sparse(2, &rows).unwrap(),
            &arguments,
            &Control::default(),
        )
        .unwrap_err();
    assert_eq!(failure.failure_fact(), original.failure_fact());
    assert_eq!(
        host.ledger.lock().unwrap().opaque_attempts,
        before_opaque,
        "tuning type refusal must precede nested value copies and sketch preparation"
    );
    assert_eq!(failure.to_string(), original.to_string());
    assert_eq!(
        kernel
            .evaluate(
                Selection::try_sparse(2, &rows).unwrap(),
                &arguments,
                &Control::default()
            )
            .unwrap_err(),
        KernelFailure::InstanceFailed
    );
    drop(arguments);
    drop(arrays);
    drop(kernel);
    assert!(weak.upgrade().is_none());
    empty(&host);
}
#[test]
fn ds_scalar_target_hint_borrows_long_text_without_additional_opaque_work() {
    fn run(target: &str) -> (Vec<u8>, usize) {
        let arrays = vec![
            Arc::new(Int64Array::from(vec![1])) as ArrayRef,
            Arc::new(Int64Array::from(vec![17])),
            Arc::new(StringArray::from(vec![target])),
        ];
        let host = Arc::new(Host::default());
        let mut kernel = instance(&arrays, &host);
        let before = host.ledger.lock().unwrap().opaque_attempts;
        let arguments = args(&arrays);
        let output = kernel
            .evaluate(Selection::all(1), &arguments, &Control::default())
            .unwrap();
        assert!(output.errors().is_empty());
        let values = output
            .values()
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        let bytes = values.value(0).to_vec();
        let requests = host.ledger.lock().unwrap().opaque_attempts - before;
        drop(output);
        drop(kernel);
        empty(&host);
        (bytes, requests)
    }
    let expected = run("HLL_6");
    let long = run(&"x".repeat(1_000_000));
    assert_eq!(
        long, expected,
        "same HLL_6 hint result and actual opaque operation count"
    );
}
#[test]
fn ds_scalar_all_null_key_skips_entire_owned_tuning_reader_and_diagnostic() {
    let arrays = vec![
        Arc::new(Int64Array::from(vec![None, None])) as ArrayRef,
        long_target(),
        long_target(),
    ];
    let host = Arc::new(Host::default());
    let mut kernel = instance(&arrays, &host);
    let attempts = host.ledger.lock().unwrap().attempts;
    let opaque = host.ledger.lock().unwrap().opaque_attempts;
    let arguments = args(&arrays);
    let output = kernel
        .evaluate(Selection::all(2), &arguments, &Control::default())
        .unwrap();
    assert!(output.errors().is_empty());
    assert_eq!(output.values().null_count(), 2);
    assert_eq!(host.ledger.lock().unwrap().attempts, attempts);
    assert_eq!(host.ledger.lock().unwrap().opaque_attempts, opaque);
    drop(output);
    drop(kernel);
    empty(&host);
}
#[test]
fn ds_scalar_missing_host_is_named_and_does_not_fallback() {
    let arrays = vec![Arc::new(Int64Array::from(vec![1])) as ArrayRef];
    let expected = invalid(
        "builtin.scalar/ds_hll_count_distinct_state/v1 requires an actual scalar opaque allocation host",
    );
    assert!(
        matches!(ScalarEvaluationInstance::instantiate(prepared(&arrays)), Err(actual) if actual == expected)
    );
    assert!(
        matches!(ScalarEvaluationInstance::instantiate_with_allocator(
        prepared(&arrays), Some(Arc::new(crate::UnaccountedAggregateStateAllocator)),
    ), Err(actual) if actual == expected)
    );
}
#[test]
fn ds_scalar_every_callback_seven_causes_has_no_tail_and_failure_latch() {
    let arrays = vec![
        Arc::new(StringArray::from(vec!["some long original row".repeat(30)])) as ArrayRef,
        Arc::new(Int64Array::from(vec![17])),
        Arc::new(StringArray::from(vec!["HLL_4"])),
    ];
    let good = Control::default();
    let host = Arc::new(Host::default());
    let mut kernel = instance(&arrays, &host);
    let arguments = args(&arrays);
    drop(
        kernel
            .evaluate(Selection::all(1), &arguments, &good)
            .unwrap(),
    );
    drop(kernel);
    empty(&host);
    let count = good.trace.lock().unwrap().len();
    assert!(count > 3);
    for cause in causes() {
        for stop in 0..count {
            let host = Arc::new(Host::default());
            let mut kernel = instance(&arrays, &host);
            let control = Control {
                trace: Mutex::new(vec![]),
                refusal: Some((stop, cause.clone())),
            };
            let failure = kernel
                .evaluate(Selection::all(1), &arguments, &control)
                .unwrap_err();
            assert_eq!(failure, cause);
            assert_eq!(control.trace.lock().unwrap().len(), stop + 1);
            assert_eq!(
                kernel
                    .evaluate(Selection::all(1), &arguments, &Control::default())
                    .unwrap_err(),
                KernelFailure::InstanceFailed
            );
            drop(kernel);
            empty(&host);
        }
    }
}
#[test]
fn ds_scalar_every_owned_allocation_and_opaque_reservation_seven_causes_rollback() {
    // One successful sketch row exercises actual opaque copies/SDK work;
    // the second row's genuine numeric Data exercises owned diagnostics.
    let arrays = vec![
        Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef,
        Arc::new(Int64Array::from(vec![17, 1024])),
        Arc::new(StringArray::from(vec!["HLL_4", "HLL_4"])) as ArrayRef,
    ];
    let good = Arc::new(Host::default());
    let mut kernel = instance(&arrays, &good);
    let arguments = args(&arrays);
    let initial_owned = good.ledger.lock().unwrap().attempts;
    let initial_opaque = good.ledger.lock().unwrap().opaque_attempts;
    drop(
        kernel
            .evaluate(Selection::all(2), &arguments, &Control::default())
            .unwrap(),
    );
    let owned = good.ledger.lock().unwrap().attempts - initial_owned;
    let opaque = good.ledger.lock().unwrap().opaque_attempts - initial_opaque;
    drop(kernel);
    empty(&good);
    assert!(owned > 0);
    assert!(opaque > 0);
    for cause in causes() {
        for stop in 0..owned {
            let host = Arc::new(Host::default());
            let mut kernel = instance(&arrays, &host);
            arm_refusal(&host, stop, cause.clone());
            assert_eq!(
                kernel
                    .evaluate(Selection::all(2), &arguments, &Control::default())
                    .unwrap_err(),
                cause
            );
            drop(kernel);
            empty(&host);
        }
        for stop in 0..opaque {
            let host = Arc::new(Host::default());
            let mut kernel = instance(&arrays, &host);
            let base = host.ledger.lock().unwrap().opaque_attempts;
            *host.opaque_refusal.lock().unwrap() = Some((base + stop, cause.clone()));
            assert_eq!(
                kernel
                    .evaluate(Selection::all(2), &arguments, &Control::default())
                    .unwrap_err(),
                cause
            );
            drop(kernel);
            empty(&host);
        }
    }
}
#[test]
fn ds_scalar_constructor_box_and_metadata_seven_causes_rollback() {
    let arrays = vec![Arc::new(Int64Array::from(vec![1])) as ArrayRef];
    for cause in causes() {
        let host = Arc::new(Host::default());
        *host.opaque_refusal.lock().unwrap() = Some((0, cause.clone()));
        assert!(
            matches!(ScalarEvaluationInstance::instantiate_with_allocator(prepared(&arrays),Some(host.clone())),Err(actual) if actual==cause)
        );
        empty(&host);
        let host = Arc::new(Host::default());
        *host.refusal.lock().unwrap() = Some((0, cause.clone()));
        assert!(
            matches!(ScalarEvaluationInstance::instantiate_with_allocator(prepared(&arrays),Some(host.clone())),Err(actual) if actual==cause)
        );
        empty(&host);
    }
}

#[test]
fn ds_scalar_every_sketch_create_update_serialize_reservation_preserves_seven_causes() {
    let arrays = vec![
        Arc::new(Int64Array::from(vec![9])) as ArrayRef,
        Arc::new(Int64Array::from(vec![21])),
        Arc::new(StringArray::from(vec!["HLL_8"])),
    ];
    let arguments = args(&arrays);
    let host = Arc::new(Host::default());
    let mut kernel = instance(&arrays, &host);
    let base = host.ledger.lock().unwrap().opaque_attempts;
    let output = kernel
        .evaluate(Selection::all(1), &arguments, &Control::default())
        .unwrap();
    assert!(output.errors().is_empty());
    let values = output
        .values()
        .as_any()
        .downcast_ref::<BinaryArray>()
        .unwrap();
    assert_eq!(
        crate::datasketches_hll::hll_estimate(values.value(0)).unwrap(),
        1
    );
    drop(output);
    let count = host.ledger.lock().unwrap().opaque_attempts - base;
    // Capacity authorization is required for these real SDK phases. The old
    // minimum also counted two tuning copies which borrowed reads eliminated.
    let funded_sdk_phases = ["create", "update", "serialize"];
    assert!(
        count >= funded_sdk_phases.len(),
        "only {count} actual grants for {funded_sdk_phases:?}"
    );
    drop(kernel);
    empty(&host);
    for cause in causes() {
        for stop in 0..count {
            let host = Arc::new(Host::default());
            let mut kernel = instance(&arrays, &host);
            let base = host.ledger.lock().unwrap().opaque_attempts;
            *host.opaque_refusal.lock().unwrap() = Some((base + stop, cause.clone()));
            assert_eq!(
                kernel
                    .evaluate(Selection::all(1), &arguments, &Control::default())
                    .unwrap_err(),
                cause
            );
            assert_eq!(host.ledger.lock().unwrap().opaque_attempts, base + stop + 1);
            drop(kernel);
            empty(&host);
        }
    }
}
#[test]
fn ds_scalar_explicit_scalar_constant_ordinal_and_selected_column_mapping() {
    use crate::{ConstantPolicy, ConstantPool, SelectedValues};
    use arrow_schema::Field;
    let values: ArrayRef = Arc::new(Int64Array::from(vec![9, 10]));
    let log: ArrayRef = Arc::new(Int64Array::from(vec![10]));
    let source: ArrayRef = Arc::new(StringArray::from(vec![
        "wrong outside ordinal",
        "HLL_4",
        "HLL_8",
    ]));
    let ty = FunctionValueType::new(DataType::Utf8, true);
    let pool = ConstantPool::try_new(
        Arc::new(Field::new("actual-target", DataType::Utf8, true)),
        ty.clone(),
        source.to_data(),
        ConstantPolicy {
            max_rows: 16,
            max_array_nodes: 64,
            max_logical_elements: 65536,
            max_retained_buffer_bytes: 1024 * 1024,
            max_type_depth: 64,
            max_type_nodes: 4096,
            max_dictionary_depth: 16,
            max_metadata_bytes: 65536,
            max_library_validation_work: 4 * 1024 * 1024,
            max_library_validation_bytes: 4 * 1024 * 1024,
        },
        CompilePhase::Validate,
        crate::binding_test_control(),
    )
    .unwrap();
    let constant = pool.value(1).unwrap();
    let host = Arc::new(Host::default());
    let mut kernel = ScalarEvaluationInstance::instantiate_with_allocator(
        prepared_for_test(
            "ds_hll_count_distinct_state",
            &[
                FunctionValueType::new(DataType::Int64, true),
                FunctionValueType::new(DataType::Int64, true),
                ty,
            ],
        )
        .unwrap(),
        Some(host.clone()),
    )
    .unwrap();
    let rows = [1, 3];
    let selection = Selection::try_sparse(4, &rows).unwrap();
    let mapped =
        SelectedValues::try_new(selection, &DataType::Int64, values, Box::default()).unwrap();
    let arguments = [
        EvaluatedArgument::SelectedColumn(&mapped),
        EvaluatedArgument::Scalar(&log),
        EvaluatedArgument::Constant(&constant),
    ];
    let output = kernel
        .evaluate(selection, &arguments, &Control::default())
        .unwrap();
    assert!(output.errors().is_empty());
    let bytes = output
        .values()
        .as_any()
        .downcast_ref::<BinaryArray>()
        .unwrap();
    for row in 0..2 {
        assert_eq!(bytes.value(row)[3], 10);
        assert_eq!((bytes.value(row)[7] >> 2) & 3, 0);
    }
    drop(output);
    drop(kernel);
    empty(&host);
}
