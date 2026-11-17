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
use arrow_array::{Array, ArrayRef, BinaryArray};
use arrow_schema::DataType;
use novarocks_type_contract::{
    CompileControlError, DecimalOverflowPolicy, EvaluationDemand, EvaluationRegionId,
    ExpressionEffectContext, ExpressionUseId, SemanticParameters,
};
use std::{alloc::Layout, ptr::NonNull, sync::Mutex, time::Duration};
fn owner() -> BitmapToStringOwner {
    let signatures = super::super::registry::builtin_scalar_declarations()
        .into_iter()
        .find(|(n, _)| n == "bitmap_to_string")
        .unwrap()
        .1;
    let (declaration, resolver) = super::super::catalogue::scalar_definition_parts(
        "bitmap_to_string",
        &signatures,
        FunctionKind::Scalar,
    )
    .unwrap();
    BitmapToStringOwner::new(declaration, resolver).unwrap()
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
            value_type: FunctionValueType::new(DataType::Binary, true),
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
fn binary_array(values: Vec<Option<&[u8]>>) -> ArrayRef {
    Arc::new(BinaryArray::from(values))
}
fn strings(out: &ArrayRef) -> Vec<Option<String>> {
    out.as_any()
        .downcast_ref::<arrow_array::StringArray>()
        .unwrap()
        .iter()
        .map(|s| s.map(str::to_owned))
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
        panic!("BITMAP never waits")
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
fn bitmap_selected_host_historical_formats_and_nulls() {
    let host = Arc::new(Host::default());
    let set = std::collections::BTreeSet::from([0, 1, 4294967296, u64::MAX]);
    let internal = crate::bitmap_value::encode_internal_bitmap(&set).unwrap();
    let external = crate::bitmap_value::encode_external_bitmap(&set).unwrap();
    let aggregate = crate::bitmap_value::encode_bitmap_aggregate(&set).unwrap();
    let array = binary_array(vec![
        Some(&internal),
        Some(&external),
        Some(&aggregate),
        None,
        Some(b"1,0,1,18446744073709551615,4294967296"),
    ]);
    let mut kernel = instance(1, &host);
    let args = [EvaluatedArgument::Column(&array)];
    let out = kernel
        .evaluate(Selection::all(5), &args, &Control::default())
        .unwrap();
    assert!(out.errors().is_empty());
    assert_eq!(
        strings(out.values()),
        vec![
            Some("0,1,4294967296,18446744073709551615".into()),
            Some("0,1,4294967296,18446744073709551615".into()),
            Some("0,1,4294967296,18446744073709551615".into()),
            None,
            Some("0,1,4294967296,18446744073709551615".into())
        ]
    );
    drop(out);
    drop(kernel);
    empty(&host);
}
#[test]
fn bitmap_selected_long_original_error_exact_true_ordinal_not_bounded() {
    let host = Arc::new(Host::default());
    let invalid = "雪".repeat(600);
    let array = binary_array(vec![
        Some(b"bad-unselected"),
        Some(b"1"),
        Some(invalid.as_bytes()),
        Some(b"2"),
    ]);
    let rows = [1, 2, 3];
    let selection = Selection::try_sparse(4, &rows).unwrap();
    let mut kernel = instance(1, &host);
    let args = [EvaluatedArgument::Column(&array)];
    let out = kernel
        .evaluate(selection, &args, &Control::default())
        .unwrap();
    let expected = format!("bitmap string contains invalid value: {invalid}");
    assert!(expected.len() > 512);
    assert_eq!(out.errors().len(), 1);
    assert_eq!(out.errors()[0].selected_ordinal(), 1);
    assert_eq!(out.errors()[0].message(), expected);
    assert_eq!(
        out.errors()[0].reason(),
        crate::DataFailureReason::InvalidEncoding
    );
    assert_eq!(
        strings(out.values()),
        vec![Some("1".into()), None, Some("2".into())]
    );
    drop(out);
    drop(kernel);
    empty(&host);
}
#[test]
fn bitmap_selected_sparse_compact_and_nonzero_pool_exact_original() {
    let host = Arc::new(Host::default());
    let array = binary_array(vec![
        Some(b"bad"),
        Some(b"2,1"),
        None,
        Some(b"3"),
        Some(b"tail"),
    ])
    .slice(1, 3);
    let rows = [0, 2];
    let selection = Selection::try_sparse(3, &rows).unwrap();
    let compact = SelectedValues::try_new(
        selection,
        &DataType::Binary,
        binary_array(vec![Some(b"2,1"), Some(b"3")]),
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
            strings(out.values()),
            vec![Some("1,2".into()), Some("3".into())]
        );
        drop(out);
        drop(kernel);
        empty(&host);
    }
    let ty = FunctionValueType::new(DataType::Binary, true);
    let array = binary_array(vec![None, Some(b"2,1")]);
    let pool = ConstantPool::try_new(
        Arc::new(ty.try_to_field("bitmap").unwrap()),
        ty,
        array.to_data(),
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
        assert_eq!(
            strings(out.values()),
            vec![(ordinal == 1).then(|| "1,2".to_owned()); 3]
        );
        drop(out);
        drop(kernel);
        empty(&host);
    }
}
#[test]
fn bitmap_selected_control_seven_causes_exact_prefix_latch_real_rows() {
    let host = Arc::new(Host::default());
    let array = binary_array(vec![Some(b""); 700]);
    let args = [EvaluatedArgument::Column(&array)];
    let control = Control::default();
    let mut kernel = instance(1, &host);
    let out = kernel
        .evaluate(Selection::all(700), &args, &control)
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
                    .evaluate(Selection::all(700), &args, &control)
                    .unwrap_err(),
                cause
            );
            assert_eq!(*control.trace.lock().unwrap(), trace[..=stop]);
            let count = control.trace.lock().unwrap().len();
            assert_eq!(
                kernel
                    .evaluate(Selection::all(700), &args, &control)
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
        if let Some((at, _)) = &*self.0.opaque_refusal.lock().unwrap() {
            assert!(
                ledger.opaque_attempts <= *at,
                "control after opaque host primary refusal"
            );
        }
        if let Some((at, _)) = &*self.0.refusal.lock().unwrap() {
            assert!(
                ledger.attempts <= *at,
                "control after diagnostic host primary refusal"
            );
        }
        Ok(())
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("bitmap host does not wait")
    }
}
#[test]
fn bitmap_selected_opaque_and_diagnostic_host_seven_causes_preserved() {
    for cause in causes() {
        let host = Arc::new(Host::default());
        let array = binary_array(vec![Some(b"1,2")]);
        let mut kernel = instance(1, &host);
        let args = [EvaluatedArgument::Column(&array)];
        let next = host.ledger.lock().unwrap().opaque_attempts;
        *host.opaque_refusal.lock().unwrap() = Some((next, cause.clone()));
        assert_eq!(
            kernel
                .evaluate(Selection::all(1), &args, &HostNoTail(&host))
                .unwrap_err(),
            cause
        );
        drop(kernel);
        empty(&host);
        let host = Arc::new(Host::default());
        let array = binary_array(vec![Some(b"bad")]);
        let mut kernel = instance(1, &host);
        let args = [EvaluatedArgument::Column(&array)];
        arm_refusal(&host, 0, cause.clone());
        assert_eq!(
            kernel
                .evaluate(Selection::all(1), &args, &HostNoTail(&host))
                .unwrap_err(),
            cause
        );
        drop(kernel);
        empty(&host);
    }
}
#[test]
fn bitmap_selected_roaring_run_full_expansion_and_empty_host_release() {
    let host = Arc::new(Host::default());
    let set = (0..=u16::MAX)
        .map(u64::from)
        .collect::<std::collections::BTreeSet<_>>();
    let encoded = crate::bitmap_value::encode_external_bitmap(&set).unwrap();
    assert!(encoded.len() < 32);
    let array = binary_array(vec![Some(&encoded)]);
    let mut kernel = instance(1, &host);
    let args = [EvaluatedArgument::Column(&array)];
    let out = kernel
        .evaluate(Selection::all(1), &args, &Control::default())
        .unwrap();
    let expected = set
        .into_iter()
        .map(|v| v.to_string())
        .collect::<Vec<_>>()
        .join(",");
    assert_eq!(strings(out.values()), vec![Some(expected)]);
    drop(out);
    drop(kernel);
    empty(&host);
}

struct CompileRefusal(CompileControlError);
impl PureCompileControl for CompileRefusal {
    fn checkpoint(&self, _: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert!(units <= 256);
        Err(self.0.clone())
    }
}
#[test]
fn bitmap_selected_compile_three_causes_are_not_row_data() {
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

#[test]
fn bitmap_nominal_selection_preserves_domain_and_refuses_plain_overload_replay() {
    use novarocks_type_contract::ValueLogicalType;
    let owner = owner();
    let control = crate::binding_test_control();
    let plain = arguments(1);
    let bitmap = [FunctionArgument::Value {
        value_type: FunctionValueType::try_with_logical_type(
            DataType::Binary,
            true,
            ValueLogicalType::Bitmap,
        )
        .unwrap(),
        constant: None,
    }];
    let plain_selected = owner.resolve(request(&plain), control).unwrap();
    let selected = owner.resolve(request(&bitmap), control).unwrap();
    assert_ne!(plain_selected.overload, selected.overload);
    let [crate::FunctionArgumentType::Value(input)] = selected.argument_types.as_ref() else {
        panic!("one bitmap input")
    };
    assert_eq!(input.logical_type(), ValueLogicalType::Bitmap);
    assert_eq!(input.data_type(), &DataType::Binary);
    owner
        .validate_selected(&selected, request(&bitmap), control)
        .unwrap();
    owner
        .admit_selected_profile_observed(&selected, 1, control)
        .unwrap();
    assert!(
        owner
            .select_at_overload_observed(&plain_selected.overload, request(&bitmap), control)
            .is_err()
    );
    assert!(
        owner
            .select_at_overload_observed(&selected.overload, request(&plain), control)
            .is_err()
    );
    let hll = [FunctionArgument::Value {
        value_type: FunctionValueType::try_with_logical_type(
            DataType::Binary,
            true,
            ValueLogicalType::Hll,
        )
        .unwrap(),
        constant: None,
    }];
    assert!(owner.resolve(request(&hll), control).is_err());
}
