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
fn owner(name: &str) -> PercentileApproxRawOwner {
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
    PercentileApproxRawOwner::new(name, declaration, resolver).unwrap()
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
    owner: &'a PercentileApproxRawOwner,
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
use arrow_array::{Array, ArrayRef, BinaryArray, Float64Array};
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
        panic!("percentile never waits")
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
    prepared_for_test("percentile_approx_raw", &types).unwrap()
}
fn instance(arrays: &[ArrayRef], host: &Arc<Host>) -> ScalarEvaluationInstance {
    ScalarEvaluationInstance::instantiate_with_allocator(prepared(arrays), Some(host.clone()))
        .unwrap()
}
fn args(arrays: &[ArrayRef]) -> Vec<EvaluatedArgument<'_>> {
    arrays.iter().map(EvaluatedArgument::Column).collect()
}

fn payload() -> Vec<u8> {
    let mut state = crate::approx_percentile_core::PercentileState::default();
    for value in [1.0, 2.0, 3.0, 4.0] {
        crate::approx_percentile_core::add_value(&mut state, value).unwrap();
    }
    crate::approx_percentile_core::encode_state(&state).unwrap()
}
fn data_rows(n: usize) -> Vec<ArrayRef> {
    let p = payload();
    vec![
        Arc::new(BinaryArray::from(
            (0..n).map(|_| Some(p.as_slice())).collect::<Vec<_>>(),
        )) as ArrayRef,
        Arc::new(Float64Array::from(vec![0.75; n])),
    ]
}
#[test]
fn percentile_raw_owner_full_any_null_and_original_value_errors() {
    let arrays = data_rows(4);
    let expected =
        crate::percentile_approx_raw_core::evaluate_required(&arrays[0], &arrays[1]).unwrap();
    let host = Arc::new(Host::default());
    let mut kernel = instance(&arrays, &host);
    let arguments = args(&arrays);
    let output = kernel
        .evaluate(Selection::all(4), &arguments, &Control::default())
        .unwrap();
    assert_eq!(output.values().to_data(), expected.to_data());
    assert!(output.errors().is_empty());
    drop(output);
    drop(kernel);
    empty(&host);
    for ty in [
        DataType::Null,
        DataType::UInt32,
        DataType::Boolean,
        DataType::Decimal256(76, 6),
    ] {
        for n in [0, 3] {
            let arrays = vec![
                arrow_array::new_null_array(&ty, n),
                Arc::new(Float64Array::from(vec![0.5; n])) as ArrayRef,
            ];
            let host = Arc::new(Host::default());
            let mut kernel = instance(&arrays, &host);
            let arguments = args(&arrays);
            let result = kernel.evaluate(Selection::all(n), &arguments, &Control::default());
            if n == 0 {
                let output = result.unwrap();
                assert!(output.errors().is_empty());
                drop(output);
            } else {
                let cause = result.unwrap_err();
                let expected =
                    crate::percentile_approx_raw_core::row(&arrays[0], 0, &arrays[1]).unwrap_err();
                assert_eq!(
                    novarocks_failure_contract::FailureProjection::failure_fact(&cause).class(),
                    novarocks_failure_contract::FailureClass::Invariant
                );
                assert_eq!(
                    novarocks_failure_contract::FailureProjection::failure_fact(&cause),
                    novarocks_failure_contract::FailureProjection::failure_fact(&expected)
                );
                assert_eq!(cause.to_string(), expected.to_string());
            }
            drop(kernel);
            empty(&host);
        }
    }
}
#[test]
fn percentile_raw_owner_unsupported_type_is_bounded_and_keeps_no_field_or_array() {
    use novarocks_failure_contract::{FailureClass, FailureProjection};
    let field = Arc::new(arrow_schema::Field::new(
        "actual-original-unsupported".repeat(28),
        DataType::UInt32,
        true,
    ));
    let weak_field = Arc::downgrade(&field);
    let ty = DataType::Struct(vec![field].into());
    let arrays = vec![
        arrow_array::new_null_array(&ty, 2),
        Arc::new(Float64Array::from(vec![0.5; 2])) as ArrayRef,
    ];
    let weak_array = Arc::downgrade(&arrays[0]);
    let host = Arc::new(Host::default());
    let mut kernel = instance(&arrays, &host);
    let arguments = args(&arrays);
    let selected = [1];
    let cause = kernel
        .evaluate(
            Selection::try_sparse(2, &selected).unwrap(),
            &arguments,
            &Control::default(),
        )
        .unwrap_err();
    assert_eq!(cause.failure_fact().class(), FailureClass::Invariant);
    assert!(
        cause
            .to_string()
            .starts_with("percentile_approx_raw: unsupported percentile payload type")
    );
    assert!(cause.to_string().len() <= 512);
    let clone = cause.clone();
    drop(cause);
    drop(kernel);
    drop(arguments);
    drop(arrays);
    drop(ty);
    assert!(weak_field.upgrade().is_none());
    assert!(weak_array.upgrade().is_none());
    assert_eq!(clone.failure_fact().class(), FailureClass::Invariant);
    empty(&host);
}

#[test]
fn percentile_raw_owner_qnull_skips_bad_json_and_sparse_excludes_bad_rows() {
    let good = payload();
    let bad = b"invalid payload";
    let arrays = vec![
        Arc::new(BinaryArray::from(vec![
            Some(bad.as_slice()),
            Some(good.as_slice()),
            None,
        ])) as ArrayRef,
        Arc::new(Float64Array::from(vec![None, Some(0.5), Some(0.5)])),
    ];
    let host = Arc::new(Host::default());
    let mut kernel = instance(&arrays, &host);
    let arguments = args(&arrays);
    let output = kernel
        .evaluate(Selection::all(3), &arguments, &Control::default())
        .unwrap();
    assert!(output.errors().is_empty());
    assert!(output.values().is_null(0));
    // Independent median: the fixture has equal-weight [1,2,3,4], so q=0.5
    // lies midway between 2 and 3. Do not ask the core to author the oracle.
    assert_eq!(
        output
            .values()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(1),
        2.5
    );
    assert!(output.values().is_null(2));
    drop(output);
    drop(kernel);
    empty(&host);
}
#[test]
fn percentile_raw_owner_every_callback_seven_causes_no_tail_and_latch() {
    let mut state = crate::approx_percentile_core::PercentileState::default();
    for value in 0..321 {
        crate::approx_percentile_core::add_value(&mut state, value as f64).unwrap();
    }
    let encoded = crate::approx_percentile_core::encode_state(&state).unwrap();
    let arrays = vec![
        Arc::new(BinaryArray::from(vec![Some(encoded.as_slice())])) as ArrayRef,
        Arc::new(Float64Array::from(vec![0.75])) as ArrayRef,
    ];
    let prepared = prepared(&arrays);
    let arguments = args(&arrays);
    let host = Arc::new(Host::default());
    let mut instance =
        ScalarEvaluationInstance::instantiate_with_allocator(prepared.clone(), Some(host.clone()))
            .unwrap();
    let successful = Control::default();
    let out = instance
        .evaluate(Selection::all(1), &arguments, &successful)
        .unwrap();
    let trace = successful.trace.lock().unwrap().clone();
    assert!(trace.contains(&256));
    drop(out);
    drop(instance);
    empty(&host);
    for stop in 0..trace.len() {
        for cause in causes() {
            let host = Arc::new(Host::default());
            let mut instance = ScalarEvaluationInstance::instantiate_with_allocator(
                prepared.clone(),
                Some(host.clone()),
            )
            .unwrap();
            let control = Control {
                refusal: Some((stop, cause.clone())),
                ..Control::default()
            };
            let failure = instance
                .evaluate(Selection::all(1), &arguments, &control)
                .unwrap_err();
            assert_eq!(failure, cause);
            assert_eq!(control.trace.lock().unwrap().len(), stop + 1);
            assert_eq!(
                instance
                    .evaluate(Selection::all(1), &arguments, &Control::default())
                    .unwrap_err(),
                KernelFailure::InstanceFailed
            );
            drop(instance);
            empty(&host);
        }
    }
}
#[test]
fn percentile_raw_owner_every_owned_allocation_seven_causes_rollback() {
    let arrays = data_rows(1);
    let prepared = prepared(&arrays);
    let arguments = args(&arrays);
    let host = Arc::new(Host::default());
    let mut instance =
        ScalarEvaluationInstance::instantiate_with_allocator(prepared.clone(), Some(host.clone()))
            .unwrap();
    let before = host.ledger.lock().unwrap().attempts;
    let output = instance
        .evaluate(Selection::all(1), &arguments, &Control::default())
        .unwrap();
    let count = host.ledger.lock().unwrap().attempts - before;
    assert!(count > 0);
    drop(output);
    drop(instance);
    empty(&host);
    for stop in 0..count {
        for cause in causes() {
            let host = Arc::new(Host::default());
            let mut instance = ScalarEvaluationInstance::instantiate_with_allocator(
                prepared.clone(),
                Some(host.clone()),
            )
            .unwrap();
            arm_refusal(&host, stop, cause.clone());
            assert_eq!(
                instance
                    .evaluate(Selection::all(1), &arguments, &Control::default())
                    .unwrap_err(),
                cause
            );
            drop(instance);
            empty(&host);
        }
    }
}
#[test]
fn percentile_raw_owner_missing_host_named_and_constructor_causes_no_leak() {
    let arrays = data_rows(1);
    let prepared = prepared(&arrays);
    match ScalarEvaluationInstance::instantiate(prepared.clone()) {
        Err(KernelFailure::InvalidProgram(message)) => assert_eq!(
            message.message(),
            "builtin.scalar/percentile_approx_raw/v1 requires an actual scalar opaque allocation host"
        ),
        _ => panic!("missing true host must refuse"),
    }
    for cause in causes() {
        let host = Arc::new(Host::default());
        *host.refusal.lock().unwrap() = Some((0, cause.clone()));
        assert!(
            matches!(ScalarEvaluationInstance::instantiate_with_allocator(prepared.clone(),Some(host.clone())),Err(error) if error==cause)
        );
        empty(&host);
    }
}

fn v3_payload() -> Vec<u8> {
    // The original validated raw fixture's version-3 vocabulary, with no v4 caps.
    let metadata = br#"{"quantiles":null,"compression":10000}"#;
    let digest = br#"{"compression":10000.0,"min":10.0,"max":10.0,"max_processed":20000,"max_unprocessed":80000,"processed_weight":0.0,"unprocessed_weight":1.0,"processed":[],"unprocessed":[{"mean":10.0,"weight":1.0}],"cumulative":[]}"#;
    let mut bytes = vec![0xa2, 3];
    bytes.extend_from_slice(&(metadata.len() as u32).to_le_bytes());
    bytes.extend_from_slice(metadata);
    bytes.extend_from_slice(digest);
    bytes
}
#[test]
fn percentile_raw_owner_v3_actual_serde_and_unbounded_fields_match_original() {
    for bytes in [v3_payload(), {
        let mut bytes = v3_payload();
        let metadata_len = u32::from_le_bytes(bytes[2..6].try_into().unwrap()) as usize;
        let original = String::from_utf8(bytes.split_off(6 + metadata_len)).unwrap();
        // Replace fields on the original JSON vocabulary; the shared decoder is
        // the sole interpreter. This larger compression is legal only in v3.
        bytes.extend_from_slice(original.replace("10000.0", "10001.0").as_bytes());
        bytes
    }] {
        let arrays = [
            Arc::new(BinaryArray::from(vec![Some(bytes.as_slice())])) as ArrayRef,
            Arc::new(Float64Array::from(vec![0.5])) as ArrayRef,
        ];
        let expected =
            crate::percentile_approx_raw_core::evaluate_required(&arrays[0], &arrays[1]).unwrap();
        let host = Arc::new(Host::default());
        let mut kernel = instance(&arrays, &host);
        let arguments = args(&arrays);
        let output = kernel
            .evaluate(Selection::all(1), &arguments, &Control::default())
            .unwrap();
        assert!(output.errors().is_empty());
        assert_eq!(output.values().to_data(), expected.to_data());
        drop(output);
        drop(kernel);
        empty(&host);
    }
}
#[test]
fn percentile_raw_owner_v3_all_opaque_reservations_preserve_seven_causes() {
    let bytes = v3_payload();
    let arrays = [
        Arc::new(BinaryArray::from(vec![Some(bytes.as_slice())])) as ArrayRef,
        Arc::new(Float64Array::from(vec![0.5])) as ArrayRef,
    ];
    let prepared = prepared(&arrays);
    let arguments = args(&arrays);
    let host = Arc::new(Host::default());
    let mut kernel =
        ScalarEvaluationInstance::instantiate_with_allocator(prepared.clone(), Some(host.clone()))
            .unwrap();
    let initial = host.ledger.lock().unwrap().opaque_attempts;
    let output = kernel
        .evaluate(Selection::all(1), &arguments, &Control::default())
        .unwrap();
    let calls = host.ledger.lock().unwrap().opaque_attempts - initial;
    assert!(
        calls >= 3,
        "original two JSON decodes and stable sort are actually admitted"
    );
    drop(output);
    drop(kernel);
    empty(&host);
    for stop in 0..calls {
        for cause in causes() {
            let host = Arc::new(Host::default());
            let mut kernel = ScalarEvaluationInstance::instantiate_with_allocator(
                prepared.clone(),
                Some(host.clone()),
            )
            .unwrap();
            let initial = host.ledger.lock().unwrap().opaque_attempts;
            *host.opaque_refusal.lock().unwrap() = Some((initial + stop, cause.clone()));
            let failure = kernel
                .evaluate(Selection::all(1), &arguments, &Control::default())
                .unwrap_err();
            assert_eq!(failure, cause);
            assert_eq!(
                host.ledger.lock().unwrap().opaque_attempts,
                initial + stop + 1
            );
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
fn percentile_raw_owner_full_fixed_size_binary_widths_lazy_null_failure() {
    for width in [0, 8, 16, 32] {
        use arrow_array::builder::FixedSizeBinaryBuilder;
        let mut builder = FixedSizeBinaryBuilder::with_capacity(2, width);
        builder.append_null();
        builder.append_value(vec![0; width as usize]).unwrap();
        let arrays = [
            Arc::new(BinaryArray::from(vec![Some(payload().as_slice()), None])) as ArrayRef,
            Arc::new(builder.finish()) as ArrayRef,
        ];
        let host = Arc::new(Host::default());
        let mut kernel = instance(&arrays, &host);
        let arguments = args(&arrays);
        let result = kernel.evaluate(Selection::all(2), &arguments, &Control::default());
        if width == 16 {
            let output = result.unwrap();
            assert!(output.values().is_null(0));
            assert!(output.errors().is_empty());
            drop(output);
        } else {
            let cause = result.unwrap_err();
            assert_eq!(
                novarocks_failure_contract::FailureProjection::failure_fact(&cause).class(),
                novarocks_failure_contract::FailureClass::Invariant
            );
            let expected = crate::percentile_input::numeric_value_at_with_failure(
                &arrays[1],
                0,
                &mut |source| {
                    source
                        .into_failure(crate::percentile_input::PercentileInputDiagnostic::ApproxRaw)
                },
            )
            .unwrap_err();
            assert_eq!(cause.to_string(), expected.to_string());
            assert_eq!(
                novarocks_failure_contract::FailureProjection::failure_fact(&cause),
                novarocks_failure_contract::FailureProjection::failure_fact(&expected)
            );
        }
        drop(kernel);
        empty(&host);
    }
}
#[derive(Default)]
struct CompileTrace {
    trace: Mutex<Vec<(CompilePhase, u32)>>,
    refusal: Option<(usize, novarocks_type_contract::CompileControlError)>,
}
impl PureCompileControl for CompileTrace {
    fn checkpoint(
        &self,
        phase: CompilePhase,
        units: u32,
    ) -> Result<(), novarocks_type_contract::CompileControlError> {
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        if let Some((stop, _)) = self.refusal {
            assert!(at <= stop, "compile callback after primary failure");
        }
        trace.push((phase, units));
        match self.refusal.clone() {
            Some((stop, cause)) if stop == at => Err(cause),
            _ => Ok(()),
        }
    }
}
#[test]
fn percentile_raw_owner_compile_all_callbacks_three_causes_no_tail() {
    use novarocks_type_contract::CompileControlError;
    let sources = [
        FunctionValueType::new(DataType::Binary, true),
        FunctionValueType::new(DataType::Float64, true),
    ];
    let trace = CompileTrace::default();
    prepared_with_control("percentile_approx_raw", &sources, &trace).unwrap();
    let callbacks = trace.trace.lock().unwrap().len();
    for stop in 0..callbacks {
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let control = CompileTrace {
                refusal: Some((stop, cause.clone())),
                ..CompileTrace::default()
            };
            let error =
                prepared_with_control("percentile_approx_raw", &sources, &control).unwrap_err();
            match error {
                FunctionSpecializationFailure::Control(actual) => assert_eq!(actual, cause),
                FunctionSpecializationFailure::Kernel(actual) => {
                    assert_eq!(actual, compile_failure(cause))
                }
                FunctionSpecializationFailure::Binding(FunctionBindingError::Control(actual)) => {
                    assert_eq!(actual, cause)
                }
                other => panic!("compile cause lost: {other:?}"),
            }
            assert_eq!(control.trace.lock().unwrap().len(), stop + 1);
        }
    }
}

#[test]
fn percentile_raw_owner_rows_quantum_321_success_and_all_quantum_refusals() {
    let arrays = vec![
        Arc::new(BinaryArray::from(vec![None::<&[u8]>; 321])) as ArrayRef,
        Arc::new(Float64Array::from(vec![None::<f64>; 321])) as ArrayRef,
    ];
    let prepared = prepared(&arrays);
    let arguments = args(&arrays);
    let host = Arc::new(Host::default());
    let mut kernel =
        ScalarEvaluationInstance::instantiate_with_allocator(prepared.clone(), Some(host.clone()))
            .unwrap();
    let control = Control::default();
    let out = kernel
        .evaluate(Selection::all(321), &arguments, &control)
        .unwrap();
    assert!(out.errors().is_empty());
    assert_eq!(out.values().null_count(), 321);
    let trace = control.trace.lock().unwrap().clone();
    assert!(trace.contains(&256));
    drop(out);
    drop(kernel);
    empty(&host);
    for (stop, units) in trace.into_iter().enumerate() {
        if units != 256 {
            continue;
        }
        for cause in causes() {
            let host = Arc::new(Host::default());
            let mut kernel = ScalarEvaluationInstance::instantiate_with_allocator(
                prepared.clone(),
                Some(host.clone()),
            )
            .unwrap();
            let control = Control {
                refusal: Some((stop, cause.clone())),
                ..Control::default()
            };
            assert_eq!(
                kernel
                    .evaluate(Selection::all(321), &arguments, &control)
                    .unwrap_err(),
                cause
            );
            assert_eq!(control.trace.lock().unwrap().len(), stop + 1);
            drop(kernel);
            empty(&host);
        }
    }
}
#[test]
fn percentile_raw_owner_original_v3_long_parser_data_is_not_clipped() {
    let metadata = format!(
        "{{\"quantiles\":{{\"{}\":[]}},\"compression\":10000}}",
        "unknown".repeat(200)
    );
    let mut encoded = vec![0xa2, 3];
    encoded.extend_from_slice(&(metadata.len() as u32).to_le_bytes());
    encoded.extend_from_slice(metadata.as_bytes());
    let arrays = vec![
        Arc::new(BinaryArray::from(vec![Some(encoded.as_slice())])) as ArrayRef,
        Arc::new(Float64Array::from(vec![0.5])) as ArrayRef,
    ];
    let expected = crate::percentile_approx_raw_core::row(&arrays[0], 0, &arrays[1]).unwrap_err();
    let expected = original_serde_diagnostic(&expected);
    assert!(expected.len() > 512);
    let host = Arc::new(Host::default());
    let mut kernel = instance(&arrays, &host);
    let arguments = args(&arrays);
    let out = kernel
        .evaluate(Selection::all(1), &arguments, &Control::default())
        .unwrap();
    assert_eq!(out.errors().len(), 1);
    assert_eq!(out.errors()[0].message(), expected);
    assert_eq!(
        out.errors()[0].reason(),
        crate::DataFailureReason::InvalidEncoding
    );
    let diagnostic = out.errors()[0].clone();
    drop(out);
    drop(kernel);
    assert!(host.ledger.lock().unwrap().bytes > 0);
    assert_eq!(diagnostic.message(), expected);
    drop(diagnostic);
    empty(&host);
}

#[test]
fn percentile_raw_owner_lossless_diagnostic_all_control_and_owned_frontiers() {
    // Exercise full value diagnostics through a real malformed JSON value.
    // Unsupported physical shapes have their separate unmaskable oracle.
    let encoded = long_parser_payload();
    let arrays = vec![
        Arc::new(BinaryArray::from(vec![Some(encoded.as_slice())])) as ArrayRef,
        Arc::new(Float64Array::from(vec![0.5])) as ArrayRef,
    ];
    let prepared = prepared(&arrays);
    let arguments = args(&arrays);
    let host = Arc::new(Host::default());
    let mut kernel =
        ScalarEvaluationInstance::instantiate_with_allocator(prepared.clone(), Some(host.clone()))
            .unwrap();
    let initial = host.ledger.lock().unwrap().attempts;
    let control = Control::default();
    let out = kernel
        .evaluate(Selection::all(1), &arguments, &control)
        .unwrap();
    assert_eq!(out.errors().len(), 1);
    assert!(out.errors()[0].message().len() > 512);
    let trace = control.trace.lock().unwrap().clone();
    let allocations = host.ledger.lock().unwrap().attempts - initial;
    drop(out);
    drop(kernel);
    empty(&host);
    for stop in 0..trace.len() {
        for cause in causes() {
            let host = Arc::new(Host::default());
            let mut kernel = ScalarEvaluationInstance::instantiate_with_allocator(
                prepared.clone(),
                Some(host.clone()),
            )
            .unwrap();
            let control = Control {
                refusal: Some((stop, cause.clone())),
                ..Control::default()
            };
            assert_eq!(
                kernel
                    .evaluate(Selection::all(1), &arguments, &control)
                    .unwrap_err(),
                cause
            );
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
    for stop in 0..allocations {
        for cause in causes() {
            let host = Arc::new(Host::default());
            let mut kernel = ScalarEvaluationInstance::instantiate_with_allocator(
                prepared.clone(),
                Some(host.clone()),
            )
            .unwrap();
            arm_refusal(&host, stop, cause.clone());
            assert_eq!(
                kernel
                    .evaluate(Selection::all(1), &arguments, &Control::default())
                    .unwrap_err(),
                cause
            );
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

fn original_serde_diagnostic(cause: &KernelFailure) -> String {
    let mut source: &(dyn std::error::Error + 'static) = cause;
    loop {
        if let Some(source) = source.downcast_ref::<serde_json::Error>() {
            return source.to_string();
        }
        source = source
            .source()
            .expect("original JSON parser source retained");
    }
}
fn long_parser_payload() -> Vec<u8> {
    let metadata = format!(
        "{{\"quantiles\":{{\"{}\":[]}},\"compression\":10000}}",
        "unknown".repeat(200)
    );
    let mut encoded = vec![0xa2, 3];
    encoded.extend_from_slice(&(metadata.len() as u32).to_le_bytes());
    encoded.extend_from_slice(metadata.as_bytes());
    encoded
}
