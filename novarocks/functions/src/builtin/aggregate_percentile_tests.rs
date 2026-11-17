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

//! Actual full-data percentile consumer witnesses; root runs after owner wiring.
use super::*;
use crate::aggregate_scalar::AggScalarValue as V;
use arrow_array::StringArray;
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
    attempts: usize,
    bytes: usize,
    peak: usize,
    live: Vec<(usize, Layout)>,
    metadata: Option<(usize, Layout)>,
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
fn kernel(
    name: &str,
    value: FunctionValueType,
    key: FunctionValueType,
    phase: AggregateKernelPhase,
) -> PercentileKernel {
    let catalog = super::super::catalogue::build_builtin_engine_function_catalog().unwrap();
    let args = [
        FunctionArgument::Value {
            value_type: value,
            constant: None,
        },
        FunctionArgument::Value {
            value_type: key,
            constant: None,
        },
    ];
    let request = FunctionBindingRequest {
        arguments: &args,
        logical_argument_count: 2,
        expected_result_type: None,
    };
    let bound = catalog
        .resolve_bound_user(name, FunctionKind::Aggregate, request, &Compile)
        .unwrap();
    let selected = Arc::new(bound.selected);
    let context = ExpressionEffectContext {
        use_id: ExpressionUseId::new(431),
        region: EvaluationRegionId::new(29),
        demand: EvaluationDemand::Value,
    };
    let state_context = ExpressionEffectContext {
        use_id: ExpressionUseId::new(434),
        region: EvaluationRegionId::new(30),
        demand: EvaluationDemand::Value,
    };
    let state_type = FunctionValueType::new(DataType::Binary, true);
    let uses = [
        Some(ExpressionUseId::new(432)),
        Some(ExpressionUseId::new(433)),
    ];
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
                proof_scope: CallProofScope::Region(context.region),
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
    PercentileKernel {
        contract: handle.contract().clone(),
        operation: if name == "percentile_cont" {
            PercentileOperation::Continuous
        } else {
            PercentileOperation::Discrete
        },
    }
}

fn update_selected(
    kernel: &PercentileKernel,
    state: &mut PercentileState,
    host: Arc<Host>,
    values: &ArrayRef,
    rates: &ArrayRef,
    selection: Selection<'_>,
    mapping: &[usize],
    control: &dyn KernelEvaluationControl,
) -> Result<(), EvaluationFailure> {
    let args = [
        EvaluatedArgument::Column(values),
        EvaluatedArgument::Column(rates),
    ];
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
#[test]
fn exact_percentile_owner_rate_data_has_actual_sparse_domain_and_latch() {
    for name in ["percentile_cont", "percentile_disc", "percentile_disc_lc"] {
        let kernel = kernel(
            name,
            FunctionValueType::new(DataType::Float64, true),
            FunctionValueType::new(DataType::Float64, false),
            AggregateKernelPhase::Single,
        );
        let host = Arc::new(Host::default());
        let mut state = kernel
            .create_state_with_allocator(Some(host.clone()), &Control::default())
            .unwrap();
        let values = Arc::new(arrow_array::Float64Array::from(vec![
            None, None, None, None,
        ])) as ArrayRef;
        let rates = Arc::new(arrow_array::Float64Array::from(vec![
            0.5,
            0.5,
            f64::NAN,
            0.5,
        ])) as ArrayRef;
        let rows = [2usize];
        let selection = Selection::try_sparse(4, &rows).unwrap();
        let failure = update_selected(
            &kernel,
            &mut state,
            host.clone(),
            &values,
            &rates,
            selection,
            &[7],
            &Control::default(),
        )
        .unwrap_err();
        let EvaluationFailure::InvocationData(data) = failure else {
            panic!("whole data")
        };
        assert_eq!(
            data.message(),
            "update aggregate state: Percentile rate must be between 0 and 1"
        );
        assert_eq!(data.input_rows(), &[2]);
        assert_eq!(data.state_indices(), &[7]);
        assert!(std::ptr::eq(
            data.aggregate_contract(),
            kernel.contract.as_ref()
        ));
        assert_eq!(data.aggregate_phase(), AggregateInvocationPhase::Update);
        assert!(state.failed);
        assert!(state.core.values.is_empty());
        assert!(matches!(
            update_selected(
                &kernel,
                &mut state,
                host.clone(),
                &values,
                &rates,
                selection,
                &[7],
                &Control::default()
            ),
            Err(EvaluationFailure::Kernel(KernelFailure::InstanceFailed))
        ));
        drop(data);
        drop(state);
        assert_eq!(host.ledger.lock().unwrap().bytes, 0);
    }
}
#[test]
fn exact_percentile_owner_final_error_has_real_emission_domain_and_original_message() {
    let kernel = kernel(
        "percentile_cont",
        FunctionValueType::new(DataType::Int64, false),
        FunctionValueType::new(DataType::Float64, false),
        AggregateKernelPhase::Single,
    );
    let host = Arc::new(Host::default());
    let mut state = kernel
        .create_state_with_allocator(Some(host.clone()), &Control::default())
        .unwrap();
    let values = Arc::new(arrow_array::Int64Array::from(vec![1, 4])) as ArrayRef;
    let rates = Arc::new(arrow_array::Float64Array::from(vec![0.5, 0.5])) as ArrayRef;
    update_selected(
        &kernel,
        &mut state,
        host.clone(),
        &values,
        &rates,
        Selection::all(2),
        &[9, 9],
        &Control::default(),
    )
    .unwrap();
    let allocator: Arc<dyn AggregateStateAllocator> = host.clone();
    let indices = [9];
    let context =
        AggregateEmissionContext::from_host(&kernel.contract, &indices, 32, Some(&allocator));
    let failure = kernel
        .build_final_evaluation_with_context(std::iter::once(&state), &context, &Control::default())
        .unwrap_err();
    let EvaluationFailure::Kernel(cause) = failure else {
        panic!("output contract failure");
    };
    assert_eq!(
        novarocks_failure_contract::FailureProjection::failure_fact(&cause).class(),
        novarocks_failure_contract::FailureClass::Invariant
    );
    assert_eq!(
        cause.to_string(),
        "unsupported percentile_cont output type Int64"
    );
    drop(cause);
    drop(state);
    assert_eq!(host.ledger.lock().unwrap().bytes, 0);
}
#[test]
fn exact_percentile_owner_selected_unicode_all_seven_control_causes_no_tail() {
    for cause in causes() {
        let kernel = kernel(
            "percentile_disc_lc",
            FunctionValueType::new(DataType::Utf8, false),
            FunctionValueType::new(DataType::Float64, false),
            AggregateKernelPhase::Single,
        );
        let host = Arc::new(Host::default());
        let mut state = kernel
            .create_state_with_allocator(Some(host.clone()), &Control::default())
            .unwrap();
        let text = "é中🙂".repeat(1000);
        let values = Arc::new(StringArray::from(vec![text.as_str()])) as ArrayRef;
        let rates = Arc::new(arrow_array::Float64Array::from(vec![0.5])) as ArrayRef;
        let control = Control {
            trace: Mutex::new(vec![]),
            refusal: Some((1, cause.clone())),
        };
        let failure = update_selected(
            &kernel,
            &mut state,
            host.clone(),
            &values,
            &rates,
            Selection::all(1),
            &[0],
            &control,
        )
        .unwrap_err();
        assert_eq!(failure, EvaluationFailure::Kernel(cause));
        assert_eq!(control.trace.lock().unwrap().len(), 2);
        assert!(state.failed);
        assert!(state.core.values.is_empty());
        drop(state);
        assert_eq!(host.ledger.lock().unwrap().bytes, 0);
    }
}
#[test]
fn exact_percentile_owner_merge_serde_full_error_and_resource_journal_are_distinct() {
    for resource in [false, true] {
        let kernel = kernel(
            "percentile_disc",
            FunctionValueType::new(DataType::Utf8, true),
            FunctionValueType::new(DataType::Float64, false),
            AggregateKernelPhase::Final,
        );
        let host = Arc::new(Host::default());
        let mut state = kernel
            .create_state_with_allocator(Some(host.clone()), &Control::default())
            .unwrap();
        let mut payload = vec![0xc3, 1];
        payload.extend_from_slice(br#"{"values":[{"Utf8":"abc"}],"rate":0.5}"#);
        if !resource {
            payload.truncate(payload.len() - 1);
        }
        let values = Arc::new(arrow_array::BinaryArray::from(vec![payload.as_slice()])) as ArrayRef;
        let setup = Control::default();
        let input = SelectedAggregateMergeInput::try_new(
            &kernel.contract,
            Selection::all(1),
            EvaluatedArgument::Column(&values),
            &setup,
        )
        .unwrap();
        let prepared = kernel
            .prepare_merge_evaluation(input, &[3], Some(host.clone()), &setup)
            .unwrap();
        if resource {
            arm_refusal(&host, 0, KernelFailure::DeadlineExceeded);
        }
        let result = kernel
            .merge_row_evaluation(&mut state, &prepared, 0, &setup)
            .unwrap_err();
        if resource {
            assert_eq!(
                result,
                EvaluationFailure::Kernel(KernelFailure::DeadlineExceeded)
            );
        } else {
            let expected = core::decode_state_into(
                &payload,
                &PercentileAllocator(HostAggregateAllocator::try_new(host.clone()).unwrap()),
            )
            .unwrap_err();
            let EvaluationFailure::InvocationData(data) = result else {
                panic!("serde data")
            };
            assert_eq!(
                data.message(),
                crate::aggregate_format::AggregateFailureStage::Merge
                    .message(&expected)
                    .to_string()
            );
            assert_eq!(data.state_indices(), &[3]);
            assert_eq!(data.aggregate_phase(), AggregateInvocationPhase::Merge);
        }
        drop(prepared);
        drop(state);
        assert_eq!(host.ledger.lock().unwrap().bytes, 0);
    }
}

#[test]
fn exact_percentile_owner_unsupported_nested_scalar_is_bounded_contract() {
    let ty = DataType::Struct(Fields::from(vec![Field::new(
        "payload",
        DataType::Utf8,
        false,
    )]));
    let kernel = kernel(
        "percentile_disc",
        FunctionValueType::from_legacy_declared_projection(ty.clone(), false),
        FunctionValueType::new(DataType::Float64, false),
        AggregateKernelPhase::Single,
    );
    let host = Arc::new(Host::default());
    let mut state = kernel
        .create_state_with_allocator(Some(host.clone()), &Control::default())
        .unwrap();
    let values = scalar::build_scalar_array(
        &ty,
        vec![Some(V::Struct(vec![Some(V::Utf8(
            "original long unicode 中🙂".repeat(100),
        ))]))],
        &mut ScalarWork::new(None),
    )
    .unwrap();
    let rates = Arc::new(arrow_array::Float64Array::from(vec![0.5])) as ArrayRef;
    let legacy_value = scalar::tracked_scalar_from_array(
        &values,
        0,
        &state.core.allocator,
        &mut ScalarWork::new(None),
    )
    .unwrap()
    .unwrap();
    let expected = format!(
        "unsupported percentile_disc/cont input scalar {:?}",
        legacy_value
    );
    let failure = update_selected(
        &kernel,
        &mut state,
        host.clone(),
        &values,
        &rates,
        Selection::all(1),
        &[19],
        &Control::default(),
    )
    .unwrap_err();
    let EvaluationFailure::Kernel(cause) = failure else {
        panic!("unsupported frozen scalar contract");
    };
    assert!(expected.len() > 512);
    assert_eq!(
        novarocks_failure_contract::FailureProjection::failure_fact(&cause).class(),
        novarocks_failure_contract::FailureClass::Invariant
    );
    assert!(
        cause
            .to_string()
            .starts_with("unsupported percentile_disc/cont input scalar")
    );
    assert!(cause.to_string().len() <= 512);
    drop(legacy_value);
    drop(cause);
    drop(state);
    assert_eq!(host.ledger.lock().unwrap().bytes, 0);
}
#[test]
fn exact_percentile_owner_compile_three_causes_preserved() {
    struct Refuse(CompileControlError);
    impl PureCompileControl for Refuse {
        fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
            Err(self.0.clone())
        }
    }
    let catalog = super::super::catalogue::build_builtin_engine_function_catalog().unwrap();
    let args = std::array::from_fn::<_, 2, _>(|_| FunctionArgument::Value {
        value_type: FunctionValueType::new(DataType::Float64, false),
        constant: None,
    });
    for cause in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        let result = catalog.resolve_bound_user(
            "percentile_cont",
            FunctionKind::Aggregate,
            FunctionBindingRequest {
                arguments: &args,
                logical_argument_count: 2,
                expected_result_type: None,
            },
            &Refuse(cause.clone()),
        );
        assert!(matches!(result,Err(FunctionBindingError::Control(actual)) if actual==cause));
    }
}

#[path = "aggregate_lossless_emission_no_footer_tests.rs"]
mod lossless_emission_no_footer_tests;
