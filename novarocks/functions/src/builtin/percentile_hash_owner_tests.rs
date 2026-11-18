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

//! Original Native N1 profile and genuine host/control lifecycle failures.
use super::*;
use crate::kernel_control::{internal, invalid};
use crate::opaque_memory::OpaqueAllocationHost;
use crate::{
    AggregateStateAllocator, ConstantPolicy, ConstantPool, EvaluatedArgument, FunctionArgument,
    FunctionSpecializationFailure, FunctionValueType, KernelDiagnostic, ScalarEvaluationInstance,
    ScopedExpressionEffects, SelectedValues, Selection, specialize_scalar,
};
use arrow_array::{Array, ArrayRef, BinaryArray, Int32Array};
use arrow_schema::DataType;
use novarocks_type_contract::{
    CompileControlError, DecimalOverflowPolicy, EvaluationDemand, EvaluationRegionId,
    ExpressionEffectContext, ExpressionUseId, SemanticParameters,
};
use std::{alloc::Layout, ptr::NonNull, sync::Mutex, time::Duration};
fn owner() -> PercentileHashOwner {
    let signatures = super::super::registry::builtin_scalar_declarations()
        .into_iter()
        .find(|(n, _)| n == "percentile_hash")
        .unwrap()
        .1;
    let (declaration, resolver) = super::super::catalogue::scalar_definition_parts(
        "percentile_hash",
        &signatures,
        FunctionKind::Scalar,
    )
    .unwrap();
    PercentileHashOwner::new(declaration, resolver).unwrap()
}
fn context() -> ExpressionEffectContext {
    ExpressionEffectContext {
        use_id: ExpressionUseId::new(41),
        region: EvaluationRegionId::new(7),
        demand: EvaluationDemand::Value,
    }
}
fn arguments(n: usize) -> Vec<FunctionArgument> {
    (0..n)
        .map(|_| FunctionArgument::Value {
            value_type: FunctionValueType::new(DataType::Int32, true),
            constant: None,
        })
        .collect()
}
fn request(a: &[FunctionArgument]) -> FunctionBindingRequest<'_> {
    FunctionBindingRequest {
        expected_result_type: None,
        arguments: a,
        logical_argument_count: a.len(),
    }
}
fn prepare_with(
    n: usize,
    uses: &[Option<ExpressionUseId>],
    control: &dyn PureCompileControl,
) -> Result<Arc<dyn PreparedScalarKernel>, FunctionSpecializationFailure> {
    let owner = owner();
    let args = arguments(n);
    let selected = Arc::new(owner.resolve(request(&args), control)?);
    let parameters = SemanticParameters::try_new([]).unwrap();
    specialize_scalar(
        &owner,
        CallEffectInput {
            context: (context()).into(),
            argument_uses: crate::CallArgumentUses::SelectedChannels(uses),
            function_id: owner.declaration.function_id(),
            kind: FunctionKind::Scalar,
            selected: &selected,
            request: request(&args),
            environment: &[],
            parameters: &parameters,
            decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
            proof_scope: CallProofScope::Unconditional,
        },
        selected.clone(),
        ScopedExpressionEffects::pure_value(context()),
        control,
    )
    .map(|s| s.into_prepared())
}
fn prepare(n: usize) -> Arc<dyn PreparedScalarKernel> {
    let mut uses = vec![None; n];
    uses[0] = Some(ExpressionUseId::new(42));
    prepare_with(n, &uses, crate::binding_test_control()).unwrap()
}
fn instance(n: usize, host: &Arc<Host>) -> ScalarEvaluationInstance {
    ScalarEvaluationInstance::instantiate_with_allocator(
        prepare(n),
        Some(host.clone() as Arc<dyn AggregateStateAllocator>),
    )
    .unwrap()
}
fn ints(values: Vec<Option<i32>>) -> ArrayRef {
    Arc::new(Int32Array::from(values))
}
fn binary(out: &ArrayRef) -> Vec<Vec<u8>> {
    assert_eq!(out.null_count(), 0);
    out.as_any()
        .downcast_ref::<BinaryArray>()
        .unwrap()
        .iter()
        .map(|b| b.unwrap().to_vec())
        .collect()
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
        panic!("HASH never waits")
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

#[test]
fn hash_native_n1_profile_rejects_deferred_variadic_without_changing_binding() {
    let owner = owner();
    for n in [1, 2, 5, 321] {
        let args = arguments(n);
        let selected = owner
            .resolve(request(&args), crate::binding_test_control())
            .unwrap();
        assert_eq!(selected.argument_types.len(), n);
        let admitted =
            owner.admit_selected_profile_observed(&selected, n, crate::binding_test_control());
        if n == 1 {
            admitted.unwrap();
            let prepared = prepare(1);
            assert_eq!(
                prepared.contract().effects().argument_control,
                ArgumentControl::Eager
            );
        } else {
            assert!(
                matches!(admitted, Err(FunctionBindingError::UnavailableImplementation(ref o))
                if o == &selected.overload)
            );
        }
    }
    for ty in [
        DataType::Utf8,
        DataType::Binary,
        DataType::Decimal256(76, 0),
        DataType::Null,
    ] {
        let args = [FunctionArgument::Value {
            value_type: FunctionValueType::from_legacy_declared_projection(ty, true),
            constant: None,
        }];
        // The actual original selected-domain author rejects these carriers
        // before any selected profile loan exists. Do not fabricate one.
        assert!(matches!(
            owner.resolve(request(&args), crate::binding_test_control()),
            Err(FunctionBindingError::NoMatchingOverload)
        ));
    }
}
#[test]
fn hash_native_n1_sparse_slice_compact_and_nonzero_constant_pool() {
    let host = Arc::new(Host::default());
    let array = ints(vec![Some(99), Some(11), None, Some(17), Some(88)]).slice(1, 3);
    let rows = [0, 2];
    let selection = Selection::try_sparse(3, &rows).unwrap();
    let compact = SelectedValues::try_new(
        selection,
        &DataType::Int32,
        ints(vec![Some(11), Some(17)]),
        Box::default(),
    )
    .unwrap();
    for arg in [
        EvaluatedArgument::Column(&array),
        EvaluatedArgument::SelectedColumn(&compact),
    ] {
        let mut kernel = instance(1, &host);
        let arguments = [arg];
        let out = kernel
            .evaluate(selection, &arguments, &Control::default())
            .unwrap();
        assert_eq!(
            binary(out.values()),
            vec![
                crate::percentile_hash_core::encode_numeric(Some(11.0)).unwrap(),
                crate::percentile_hash_core::encode_numeric(Some(17.0)).unwrap()
            ]
        );
        drop(out);
        drop(kernel);
        empty(&host);
    }
    let ty = FunctionValueType::new(DataType::Int32, true);
    let a = ints(vec![None, Some(17)]);
    let pool = ConstantPool::try_new(
        Arc::new(ty.try_to_field("hash").unwrap()),
        ty,
        a.to_data(),
        ConstantPolicy {
            max_rows: 2,
            max_array_nodes: 1,
            max_logical_elements: 2,
            max_retained_buffer_bytes: 4096,
            max_type_depth: 1,
            max_type_nodes: 1,
            max_dictionary_depth: 0,
            max_metadata_bytes: 1024,
            max_library_validation_work: 8192,
            max_library_validation_bytes: 8192,
        },
        CompilePhase::Validate,
        crate::binding_test_control(),
    )
    .unwrap();
    for ordinal in [0, 1] {
        let value = pool.value(ordinal).unwrap();
        let mut kernel = instance(1, &host);
        let arguments = [EvaluatedArgument::Constant(&value)];
        let out = kernel
            .evaluate(Selection::all(3), &arguments, &Control::default())
            .unwrap();
        let expected =
            crate::percentile_hash_core::encode_numeric((ordinal == 1).then_some(17.0)).unwrap();
        assert_eq!(binary(out.values()), vec![expected; 3]);
        drop(out);
        drop(kernel);
        empty(&host);
    }
}
#[test]
fn hash_native_n1_control_seven_causes_exact_prefix_latch_and_real_row_quantum() {
    let host = Arc::new(Host::default());
    let array = ints(vec![Some(17); 700]);
    let control = Control::default();
    let mut kernel = instance(1, &host);
    let arguments = [EvaluatedArgument::Column(&array)];
    let out = kernel
        .evaluate(Selection::all(700), &arguments, &control)
        .unwrap();
    let trace = control.trace.lock().unwrap().clone();
    assert!(trace.contains(&256));
    drop(out);
    drop(kernel);
    empty(&host);
    for cause in causes() {
        for stop in [0, trace.len() / 2, trace.len() - 1] {
            let control = Control {
                trace: Mutex::new(Vec::new()),
                refusal: Some((stop, cause.clone())),
            };
            let mut kernel = instance(1, &host);
            assert_eq!(
                kernel
                    .evaluate(
                        Selection::all(700),
                        &[EvaluatedArgument::Column(&array)],
                        &control
                    )
                    .unwrap_err(),
                cause
            );
            assert_eq!(*control.trace.lock().unwrap(), trace[..=stop]);
            let count = control.trace.lock().unwrap().len();
            assert_eq!(
                kernel
                    .evaluate(
                        Selection::all(700),
                        &[EvaluatedArgument::Column(&array)],
                        &control
                    )
                    .unwrap_err(),
                KernelFailure::InstanceFailed
            );
            assert_eq!(control.trace.lock().unwrap().len(), count);
            drop(kernel);
            empty(&host);
        }
    }
}
struct HostNoTail<'a>(&'a Host);
impl KernelEvaluationControl for HostNoTail<'_> {
    fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
        assert!(units <= 256);
        let ledger = self.0.ledger.lock().unwrap();
        if let Some((at, _)) = &*self.0.refusal.lock().unwrap() {
            assert!(
                ledger.attempts <= *at,
                "control footer after allocator refusal"
            );
        }
        if let Some((at, _)) = &*self.0.opaque_refusal.lock().unwrap() {
            assert!(
                ledger.opaque_attempts <= *at,
                "control footer after opaque refusal"
            );
        }
        Ok(())
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("HASH never waits")
    }
}
#[test]
fn hash_native_n1_true_host_centroid_and_codec_refusals_preserve_seven_causes() {
    let array = ints(vec![Some(17); 3]);
    for opaque in [false, true] {
        for cause in causes() {
            let host = Arc::new(Host::default());
            let mut kernel = instance(1, &host);
            if opaque {
                let next = host.ledger.lock().unwrap().opaque_attempts;
                *host.opaque_refusal.lock().unwrap() = Some((next, cause.clone()));
            } else {
                arm_refusal(&host, 0, cause.clone());
            }
            assert_eq!(
                kernel
                    .evaluate(
                        Selection::all(3),
                        &[EvaluatedArgument::Column(&array)],
                        &HostNoTail(&host)
                    )
                    .unwrap_err(),
                cause
            );
            let before = {
                let l = host.ledger.lock().unwrap();
                (l.attempts, l.opaque_attempts)
            };
            assert_eq!(
                kernel
                    .evaluate(
                        Selection::all(3),
                        &[EvaluatedArgument::Column(&array)],
                        &HostNoTail(&host)
                    )
                    .unwrap_err(),
                KernelFailure::InstanceFailed
            );
            let after = {
                let l = host.ledger.lock().unwrap();
                (l.attempts, l.opaque_attempts)
            };
            assert_eq!(before, after);
            drop(kernel);
            empty(&host);
        }
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
fn hash_native_n1_compile_three_causes_are_not_row_data() {
    for (cause, expected) in [
        (CompileControlError::Cancelled, KernelFailure::Cancelled),
        (
            CompileControlError::DeadlineExceeded,
            KernelFailure::DeadlineExceeded,
        ),
        (
            CompileControlError::ResourceExhausted,
            KernelFailure::ResourceExhausted,
        ),
    ] {
        let uses = [Some(ExpressionUseId::new(42))];
        assert!(
            matches!(prepare_with(1,&uses,&CompileRefusal(cause.clone())),
            Err(FunctionSpecializationFailure::Control(c)) if c==cause)
        );
        let original_fact = novarocks_failure_contract::FailureProjection::failure_fact(&cause);
        let actual = crate::kernel_control::compile_failure(cause.clone());
        assert_eq!(
            novarocks_failure_contract::FailureProjection::failure_fact(&actual),
            original_fact
        );
        assert_eq!(
            original_fact.class(),
            novarocks_failure_contract::FailureProjection::failure_fact(&expected).class()
        );
        assert_eq!(actual, KernelFailure::CompileControl(cause));
    }
}
