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

//! Real whole-invocation owner, host authority and independently retained output.
use super::*;
use crate::kernel_control::{internal, invalid};
use crate::opaque_memory::OpaqueAllocationHost;
use crate::{
    AggregateStateAllocator, EvaluatedArgument, FunctionArgument, FunctionValueType,
    InvocationScalarEvaluationInstance, KernelDiagnostic, ScalarInvocationActivation,
    ScalarInvocationFailure, ScopedExpressionEffects, Selection, specialize_invocation_scalar,
};
use arrow_array::{Array, ArrayRef, StringArray, UInt32Array};
use arrow_schema::DataType;
use novarocks_type_contract::{
    DecimalOverflowPolicy, EvaluationDemand, EvaluationDomainId, ExpressionEffectContext,
    ExpressionUseId, SemanticParameters,
};
use std::{alloc::Layout, ptr::NonNull, sync::Mutex, time::Duration};

fn prepared(
    types: &[DataType],
    policy: DecimalOverflowPolicy,
) -> Arc<dyn PreparedInvocationScalarKernel> {
    let signatures = super::super::registry::builtin_scalar_declarations()
        .into_iter()
        .find(|(n, _)| n == "json_object")
        .unwrap()
        .1;
    let (declaration, resolver) = super::super::catalogue::scalar_definition_parts(
        "json_object",
        &signatures,
        FunctionKind::Scalar,
    )
    .unwrap();
    let ids = declaration
        .overloads()
        .iter()
        .map(|o| o.identity.clone())
        .collect::<Vec<_>>();
    let declaration = declaration
        .with_scalar_invocation_data_overloads(ids)
        .unwrap();
    let owner = JsonObjectOwner::new(declaration, resolver).unwrap();
    let arguments = types
        .iter()
        .map(|t| FunctionArgument::Value {
            value_type: FunctionValueType::new(t.clone(), true),
            constant: None,
        })
        .collect::<Vec<_>>();
    let request = FunctionBindingRequest {
        expected_result_type: None,
        arguments: &arguments,
        logical_argument_count: arguments.len(),
    };
    let selected = Arc::new(
        owner
            .resolve(request, crate::binding_test_control())
            .unwrap(),
    );
    let context = ExpressionEffectContext {
        use_id: ExpressionUseId::new(41),
        domain: EvaluationDomainId::new(7),
        demand: EvaluationDemand::Value,
    };
    let uses = types
        .iter()
        .enumerate()
        .map(|(i, _)| Some(ExpressionUseId::new(42 + i as u32)))
        .collect::<Vec<_>>();
    let parameters = SemanticParameters::try_new([]).unwrap();
    specialize_invocation_scalar(
        &owner,
        CallEffectInput {
            context,
            argument_uses: crate::CallArgumentUses::SelectedChannels(&uses),
            function_id: owner.declaration.function_id(),
            kind: FunctionKind::Scalar,
            selected: &selected,
            request,
            environment: &[],
            parameters: &parameters,
            decimal_overflow_policy: policy,
            proof_scope: CallProofScope::Unconditional,
        },
        selected.clone(),
        ScopedExpressionEffects::pure_value(context),
        crate::binding_test_control(),
    )
    .unwrap()
    .into_prepared()
}
fn instance(
    types: &[DataType],
    policy: DecimalOverflowPolicy,
    host: &Arc<Host>,
) -> InvocationScalarEvaluationInstance {
    InvocationScalarEvaluationInstance::instantiate_with_allocator(
        prepared(types, policy),
        Some(host.clone()),
    )
    .unwrap()
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
        panic!("JSON_OBJECT never waits")
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
fn json_object_actual_owner_sparse_scalar_output_keeps_host_until_last_arrow_alias() {
    let host = Arc::new(Host::default());
    let mut kernel = instance(
        &[DataType::Utf8, DataType::Utf8],
        DecimalOverflowPolicy::OutputNull,
        &host,
    );
    let keys: ArrayRef = Arc::new(StringArray::from(vec!["guard", "a", "guard", "b"]));
    let value: ArrayRef = Arc::new(StringArray::from(vec!["{\"z\":2,\"a\":1}"]));
    let selection = Selection::try_sparse(4, &[1, 3]).unwrap();
    let arguments = [
        EvaluatedArgument::Column(&keys),
        EvaluatedArgument::Scalar(&value),
    ];
    let out = kernel
        .evaluate(
            selection,
            &arguments,
            ScalarInvocationActivation::Activated,
            &Control::default(),
        )
        .unwrap();
    let text = out.values().as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(text.value(0), "{\"a\": {\"a\": 1, \"z\": 2}}");
    assert_eq!(text.value(1), "{\"b\": {\"a\": 1, \"z\": 2}}");
    let alias = out.values().slice(1, 1);
    drop(out);
    drop(kernel);
    assert!(host.ledger.lock().unwrap().opaque_bytes > 0);
    assert!(host.ledger.lock().unwrap().bytes > 0);
    assert_eq!(
        alias
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "{\"b\": {\"a\": 1, \"z\": 2}}"
    );
    drop(alias);
    empty(&host);
}

#[test]
fn json_object_actual_whole_data_preserves_original_text_under_both_policies_and_last_alias() {
    for policy in [
        DecimalOverflowPolicy::OutputNull,
        DecimalOverflowPolicy::ReportError,
    ] {
        let host = Arc::new(Host::default());
        let mut kernel = instance(&[DataType::Utf8, DataType::UInt32], policy, &host);
        let keys: ArrayRef = Arc::new(StringArray::from(vec!["guard", "a", "guard", "b"]));
        let value: ArrayRef = Arc::new(UInt32Array::from(vec![0, 1, 2, 3]));
        let control = Control::default();
        let error = kernel
            .evaluate(
                Selection::try_sparse(4, &[1, 3]).unwrap(),
                &[
                    EvaluatedArgument::Column(&keys),
                    EvaluatedArgument::Column(&value),
                ],
                ScalarInvocationActivation::Activated,
                &control,
            )
            .unwrap_err();
        let ScalarInvocationFailure::Data(data) = error else {
            panic!("original whole-call Data");
        };
        assert_eq!(data.message(), "unsupported scalar type: UInt32");
        assert_eq!(data.batch_rows(), 4);
        assert_eq!(data.source_row(0), Some(1));
        assert_eq!(data.source_row(1), Some(3));
        let alias = data.clone();
        drop(data);
        let callbacks = control.trace.lock().unwrap().len();
        assert_eq!(
            kernel
                .evaluate(
                    Selection::all(0),
                    &[],
                    ScalarInvocationActivation::Activated,
                    &control
                )
                .unwrap_err(),
            ScalarInvocationFailure::Kernel(KernelFailure::InstanceFailed)
        );
        assert_eq!(control.trace.lock().unwrap().len(), callbacks);
        drop(kernel);
        assert!(host.ledger.lock().unwrap().opaque_bytes > 0);
        assert_eq!(alias.message(), "unsupported scalar type: UInt32");
        drop(alias);
        empty(&host);
    }
}

#[test]
fn json_object_actual_body_host_refusal_precedes_parser_and_latches_all_seven_causes() {
    let key: ArrayRef = Arc::new(StringArray::from(vec!["a"]));
    let value: ArrayRef = Arc::new(StringArray::from(vec!["[1,2]"]));
    for cause in causes() {
        let host = Arc::new(Host::default());
        let mut kernel = instance(
            &[DataType::Utf8, DataType::Utf8],
            DecimalOverflowPolicy::ReportError,
            &host,
        );
        let next = host.ledger.lock().unwrap().opaque_attempts;
        *host.opaque_refusal.lock().unwrap() = Some((next, cause.clone()));
        crate::parse_json_core::PARSE_ENTRIES.with(|entries| entries.set(0));
        let control = Control::default();
        assert_eq!(
            kernel
                .evaluate(
                    Selection::all(1),
                    &[
                        EvaluatedArgument::Column(&key),
                        EvaluatedArgument::Column(&value)
                    ],
                    ScalarInvocationActivation::Activated,
                    &control
                )
                .unwrap_err(),
            ScalarInvocationFailure::Kernel(cause)
        );
        crate::parse_json_core::PARSE_ENTRIES.with(|entries| assert_eq!(entries.get(), 0));
        let callbacks = control.trace.lock().unwrap().len();
        assert_eq!(
            kernel
                .evaluate(
                    Selection::all(1),
                    &[
                        EvaluatedArgument::Column(&key),
                        EvaluatedArgument::Column(&value)
                    ],
                    ScalarInvocationActivation::Activated,
                    &control
                )
                .unwrap_err(),
            ScalarInvocationFailure::Kernel(KernelFailure::InstanceFailed)
        );
        assert_eq!(control.trace.lock().unwrap().len(), callbacks);
        drop(kernel);
        empty(&host);
    }
}

fn replay_is_latched(
    kernel: &mut InvocationScalarEvaluationInstance,
    selection: Selection<'_>,
    arguments: &[EvaluatedArgument<'_>],
    control: &Control,
) {
    let before = control.trace.lock().unwrap().len();
    assert_eq!(
        kernel
            .evaluate(
                selection,
                arguments,
                ScalarInvocationActivation::Activated,
                control
            )
            .unwrap_err(),
        ScalarInvocationFailure::Kernel(KernelFailure::InstanceFailed)
    );
    assert_eq!(control.trace.lock().unwrap().len(), before);
}

#[test]
fn json_object_actual_every_small_control_boundary_preserves_seven_primary_causes() {
    let keys: ArrayRef = Arc::new(StringArray::from(vec!["guard", "a", "guard", "b"]));
    for value in [
        Arc::new(StringArray::from(vec![
            Some("guard"),
            Some("{bad"),
            None,
            Some("{\"z\":[1,2],\"a\":\"é\"}"),
        ])) as ArrayRef,
        Arc::new(UInt32Array::from(vec![0, 1, 2, 3])) as ArrayRef,
    ] {
        let types = [DataType::Utf8, value.data_type().clone()];
        let arguments = [
            EvaluatedArgument::Column(&keys),
            EvaluatedArgument::Column(&value),
        ];
        let selection = Selection::try_sparse(4, &[1, 3]).unwrap();
        let host = Arc::new(Host::default());
        let mut kernel = instance(&types, DecimalOverflowPolicy::OutputNull, &host);
        let control = Control::default();
        let baseline = kernel.evaluate(
            selection,
            &arguments,
            ScalarInvocationActivation::Activated,
            &control,
        );
        assert!(baseline.is_ok() || matches!(&baseline, Err(ScalarInvocationFailure::Data(_))));
        let trace = control.trace.lock().unwrap().clone();
        assert!(!trace.is_empty());
        drop(baseline);
        drop(kernel);
        empty(&host);
        for cause in causes() {
            for stop in 0..trace.len() {
                let host = Arc::new(Host::default());
                let mut kernel = instance(&types, DecimalOverflowPolicy::OutputNull, &host);
                let control = Control {
                    trace: Mutex::new(Vec::new()),
                    refusal: Some((stop, cause.clone())),
                };
                assert_eq!(
                    kernel
                        .evaluate(
                            selection,
                            &arguments,
                            ScalarInvocationActivation::Activated,
                            &control
                        )
                        .unwrap_err(),
                    ScalarInvocationFailure::Kernel(cause.clone()),
                    "boundary {stop}"
                );
                assert_eq!(*control.trace.lock().unwrap(), trace[..=stop]);
                replay_is_latched(&mut kernel, selection, &arguments, &control);
                drop(kernel);
                empty(&host);
            }
        }
    }
}

#[test]
fn json_object_actual_every_host_request_releases_all_partial_custody() {
    let keys: ArrayRef = Arc::new(StringArray::from(vec!["guard", "a", "guard", "b"]));
    let value: ArrayRef = Arc::new(StringArray::from(vec!["guard", "[1,2]", "guard", "null"]));
    let arguments = [
        EvaluatedArgument::Column(&keys),
        EvaluatedArgument::Column(&value),
    ];
    let selection = Selection::try_sparse(4, &[1, 3]).unwrap();
    let types = [DataType::Utf8, DataType::Utf8];
    let host = Arc::new(Host::default());
    let mut kernel = instance(&types, DecimalOverflowPolicy::ReportError, &host);
    let before = host.ledger.lock().unwrap().attempts;
    let output = kernel
        .evaluate(
            selection,
            &arguments,
            ScalarInvocationActivation::Activated,
            &Control::default(),
        )
        .unwrap();
    let requests = host.ledger.lock().unwrap().attempts - before;
    assert!(
        requests > 1,
        "direct header and output custody are real host requests"
    );
    drop(output);
    drop(kernel);
    empty(&host);
    for cause in causes() {
        for stop in 0..requests {
            let host = Arc::new(Host::default());
            let mut kernel = instance(&types, DecimalOverflowPolicy::ReportError, &host);
            arm_refusal(&host, stop, cause.clone());
            let control = Control::default();
            assert_eq!(
                kernel
                    .evaluate(
                        selection,
                        &arguments,
                        ScalarInvocationActivation::Activated,
                        &control
                    )
                    .unwrap_err(),
                ScalarInvocationFailure::Kernel(cause.clone()),
                "request {stop}"
            );
            replay_is_latched(&mut kernel, selection, &arguments, &control);
            drop(kernel);
            empty(&host);
        }
    }
}

#[test]
fn json_object_actual_body_diagnostic_and_sparse_grants_refuse_before_core_entry() {
    let keys: ArrayRef = Arc::new(StringArray::from(vec!["guard", "a", "guard", "b"]));
    let value: ArrayRef = Arc::new(StringArray::from(vec!["[1,2]"]));
    let arguments = [
        EvaluatedArgument::Column(&keys),
        EvaluatedArgument::Scalar(&value),
    ];
    let selection = Selection::try_sparse(4, &[1, 3]).unwrap();
    for cause in causes() {
        for stop in 0..3 {
            let host = Arc::new(Host::default());
            let mut kernel = instance(
                &[DataType::Utf8, DataType::Utf8],
                DecimalOverflowPolicy::ReportError,
                &host,
            );
            let next = host.ledger.lock().unwrap().opaque_attempts;
            *host.opaque_refusal.lock().unwrap() = Some((next + stop, cause.clone()));
            crate::parse_json_core::PARSE_ENTRIES.with(|entries| entries.set(0));
            let control = Control::default();
            assert_eq!(
                kernel
                    .evaluate(
                        selection,
                        &arguments,
                        ScalarInvocationActivation::Activated,
                        &control
                    )
                    .unwrap_err(),
                ScalarInvocationFailure::Kernel(cause.clone())
            );
            crate::parse_json_core::PARSE_ENTRIES.with(|entries| assert_eq!(entries.get(), 0));
            replay_is_latched(&mut kernel, selection, &arguments, &control);
            drop(kernel);
            empty(&host);
        }
    }
}

#[test]
fn json_object_actual_empty_activation_and_selected_addresses_keep_distinct_custody() {
    for activation in [
        ScalarInvocationActivation::Activated,
        ScalarInvocationActivation::ValidateOnly,
    ] {
        let host = Arc::new(Host::default());
        let mut kernel = instance(
            &[DataType::Utf8, DataType::Utf8],
            DecimalOverflowPolicy::OutputNull,
            &host,
        );
        let array: ArrayRef = Arc::new(StringArray::from(Vec::<&str>::new()));
        let arguments = [
            EvaluatedArgument::Column(&array),
            EvaluatedArgument::Column(&array),
        ];
        let output = kernel
            .evaluate(
                Selection::all(0),
                &arguments,
                activation,
                &Control::default(),
            )
            .unwrap();
        assert!(output.values().is_empty());
        let alias = output.values().clone();
        drop(output);
        drop(kernel);
        if activation == ScalarInvocationActivation::Activated {
            assert!(
                host.ledger.lock().unwrap().opaque_bytes > 0,
                "activated empty builder still has real output custody"
            );
        } else {
            empty(&host);
        }
        drop(alias);
        empty(&host);
    }
    let host = Arc::new(Host::default());
    let mut kernel = instance(
        &[DataType::Utf8, DataType::Utf8],
        DecimalOverflowPolicy::OutputNull,
        &host,
    );
    let selection = Selection::try_sparse(6, &[1, 4]).unwrap();
    let keys = crate::SelectedValues::try_new(
        selection,
        &DataType::Utf8,
        Arc::new(StringArray::from(vec!["first", "second"])),
        Box::default(),
    )
    .unwrap();
    let scalar: ArrayRef = Arc::new(StringArray::from(vec!["[1,2]"]));
    let arguments = [
        EvaluatedArgument::SelectedColumn(&keys),
        EvaluatedArgument::Scalar(&scalar),
    ];
    let output = kernel
        .evaluate(
            selection,
            &arguments,
            ScalarInvocationActivation::Activated,
            &Control::default(),
        )
        .unwrap();
    let strings = output
        .values()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(strings.value(0), "{\"first\": [1, 2]}");
    assert_eq!(strings.value(1), "{\"second\": [1, 2]}");
    drop(output);
    drop(kernel);
    empty(&host);
}

#[test]
fn json_object_actual_wide_quantum_entry_and_tail_refuse_without_footer() {
    let key: ArrayRef = Arc::new(StringArray::from(vec!["large"]));
    let source = format!("[{}]", "1,".repeat(1024) + "2");
    let value: ArrayRef = Arc::new(StringArray::from(vec![source]));
    let arguments = [
        EvaluatedArgument::Column(&key),
        EvaluatedArgument::Column(&value),
    ];
    let types = [DataType::Utf8, DataType::Utf8];
    let host = Arc::new(Host::default());
    let mut kernel = instance(&types, DecimalOverflowPolicy::ReportError, &host);
    let control = Control::default();
    let out = kernel
        .evaluate(
            Selection::all(1),
            &arguments,
            ScalarInvocationActivation::Activated,
            &control,
        )
        .unwrap();
    let trace = control.trace.lock().unwrap().clone();
    let quantum = trace
        .iter()
        .position(|units| *units == 256)
        .expect("real owned-work quantum");
    let stops = [0, quantum, trace.len() - 1];
    drop(out);
    drop(kernel);
    empty(&host);
    for cause in causes() {
        for stop in stops {
            let host = Arc::new(Host::default());
            let mut kernel = instance(&types, DecimalOverflowPolicy::ReportError, &host);
            let control = Control {
                trace: Mutex::new(Vec::new()),
                refusal: Some((stop, cause.clone())),
            };
            assert_eq!(
                kernel
                    .evaluate(
                        Selection::all(1),
                        &arguments,
                        ScalarInvocationActivation::Activated,
                        &control
                    )
                    .unwrap_err(),
                ScalarInvocationFailure::Kernel(cause.clone())
            );
            assert_eq!(*control.trace.lock().unwrap(), trace[..=stop]);
            drop(kernel);
            empty(&host);
        }
    }
}

#[test]
fn json_object_actual_constant_pool_uses_its_real_nonzero_ordinal() {
    let host = Arc::new(Host::default());
    let mut kernel = instance(
        &[DataType::Utf8, DataType::Utf8],
        DecimalOverflowPolicy::OutputNull,
        &host,
    );
    let ty = FunctionValueType::new(DataType::Utf8, false);
    let backing: ArrayRef = Arc::new(StringArray::from(vec!["wrong ordinal", "[1,2]"]));
    let pool = crate::ConstantPool::try_new(
        Arc::new(ty.try_to_field("value").unwrap()),
        ty,
        backing.to_data(),
        crate::ConstantPolicy {
            max_rows: 8,
            max_array_nodes: 8,
            max_logical_elements: 64,
            max_retained_buffer_bytes: 4096,
            max_type_depth: 8,
            max_type_nodes: 64,
            max_dictionary_depth: 4,
            max_metadata_bytes: 1024,
            max_library_validation_work: 4096,
            max_library_validation_bytes: 8192,
        },
        CompilePhase::Validate,
        crate::binding_test_control(),
    )
    .unwrap();
    let constant = pool.value(1).unwrap();
    let keys: ArrayRef = Arc::new(StringArray::from(vec!["guard", "a", "guard", "b"]));
    let arguments = [
        EvaluatedArgument::Column(&keys),
        EvaluatedArgument::Constant(&constant),
    ];
    let output = kernel
        .evaluate(
            Selection::try_sparse(4, &[1, 3]).unwrap(),
            &arguments,
            ScalarInvocationActivation::Activated,
            &Control::default(),
        )
        .unwrap();
    let text = output
        .values()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(text.value(0), "{\"a\": [1, 2]}");
    assert_eq!(text.value(1), "{\"b\": [1, 2]}");
    drop(output);
    drop(kernel);
    empty(&host);
}

#[test]
fn json_object_actual_original_date_error_and_raw_panic_frontier_are_preserved() {
    for policy in [
        DecimalOverflowPolicy::OutputNull,
        DecimalOverflowPolicy::ReportError,
    ] {
        for key in [false, true] {
            let host = Arc::new(Host::default());
            let types = if key {
                [DataType::Date32, DataType::Utf8]
            } else {
                [DataType::Utf8, DataType::Date32]
            };
            let mut kernel = instance(&types, policy, &host);
            let text: ArrayRef = Arc::new(StringArray::from(vec!["day"]));
            let invalid: ArrayRef = Arc::new(arrow_array::Date32Array::from(vec![-1_000_000_000]));
            let arguments = if key {
                [
                    EvaluatedArgument::Column(&invalid),
                    EvaluatedArgument::Column(&text),
                ]
            } else {
                [
                    EvaluatedArgument::Column(&text),
                    EvaluatedArgument::Column(&invalid),
                ]
            };
            let Err(ScalarInvocationFailure::Data(data)) = kernel.evaluate(
                Selection::all(1),
                &arguments,
                ScalarInvocationActivation::Activated,
                &Control::default(),
            ) else {
                panic!("original date Data");
            };
            assert_eq!(
                data.message(),
                "json_object invalid date32 day value: -1000000000"
            );
            drop(data);
            drop(kernel);
            empty(&host);
            let mut kernel = instance(&types, policy, &host);
            let overflow: ArrayRef = Arc::new(arrow_array::Date32Array::from(vec![i32::MAX]));
            let arguments = if key {
                [
                    EvaluatedArgument::Column(&overflow),
                    EvaluatedArgument::Column(&text),
                ]
            } else {
                [
                    EvaluatedArgument::Column(&text),
                    EvaluatedArgument::Column(&overflow),
                ]
            };
            let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                kernel.evaluate(
                    Selection::all(1),
                    &arguments,
                    ScalarInvocationActivation::Activated,
                    &Control::default(),
                )
            }))
            .unwrap_err();
            let message = panic
                .downcast_ref::<&str>()
                .copied()
                .or_else(|| panic.downcast_ref::<String>().map(String::as_str))
                .unwrap();
            assert_eq!(message, "attempt to add with overflow");
            drop(panic);
            drop(kernel);
            empty(&host);
        }
    }
}
