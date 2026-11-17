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

//! Actual approximate percentile full-data/host consumers; root runs after owner wiring.
use super::*;
use arrow_array::{BinaryArray, Decimal128Array, Float64Array, Int64Array, ListArray};
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
    types: Vec<FunctionValueType>,
    phase: AggregateKernelPhase,
) -> ApproxPercentileKernel {
    let catalog = if name == "percentile_union" {
        super::super::catalogue::percentile_union_private_test_catalog()
    } else {
        super::super::catalogue::build_builtin_engine_function_catalog().unwrap()
    };
    let args = types
        .into_iter()
        .map(|value_type| FunctionArgument::Value {
            value_type,
            constant: None,
        })
        .collect::<Vec<_>>();
    let request = FunctionBindingRequest {
        arguments: &args,
        logical_argument_count: args.len(),
        expected_result_type: None,
    };
    let bound = catalog
        .resolve_bound_user(name, FunctionKind::Aggregate, request, &Compile)
        .unwrap();
    let selected = Arc::new(bound.selected);
    let context = ExpressionEffectContext {
        use_id: ExpressionUseId::new(901),
        region: EvaluationRegionId::new(59),
        demand: EvaluationDemand::Value,
    };
    let state_context = ExpressionEffectContext {
        use_id: ExpressionUseId::new(910),
        region: EvaluationRegionId::new(60),
        demand: EvaluationDemand::Value,
    };
    let state_type = selected
        .aggregate
        .as_ref()
        .unwrap()
        .intermediate_type
        .clone();
    let uses = (0..args.len())
        .map(|n| Some(ExpressionUseId::new(902 + u32::try_from(n).unwrap())))
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
        panic!("actual aggregate owner")
    };
    ApproxPercentileKernel {
        contract: handle.contract().clone(),
        operation: match name {
            "percentile_approx" => ApproxPercentileOperation::Unweighted,
            "percentile_approx_weighted" => ApproxPercentileOperation::Weighted,
            "percentile_union" => ApproxPercentileOperation::Union,
            _ => panic!("test requested an unknown immutable owner"),
        },
    }
}
fn types(values: &[ArrayRef]) -> Vec<FunctionValueType> {
    values
        .iter()
        .map(|value| {
            FunctionValueType::from_legacy_declared_projection(value.data_type().clone(), true)
        })
        .collect()
}
fn update_selected(
    kernel: &ApproxPercentileKernel,
    state: &mut ApproxPercentileState,
    host: Arc<Host>,
    values: &[ArrayRef],
    selection: Selection<'_>,
    mapping: &[usize],
    control: &dyn KernelEvaluationControl,
) -> Result<(), EvaluationFailure> {
    let ingress_values = values
        .iter()
        .zip(kernel.contract.call().selected().argument_types.iter())
        .map(|(array, argument)| {
            let crate::FunctionArgumentType::Value(binding) = argument else {
                panic!("value fixture argument");
            };
            crate::test_value_ingress::array_for_binding(array, binding).unwrap()
        })
        .collect::<Vec<_>>();
    let args = ingress_values
        .iter()
        .map(EvaluatedArgument::Column)
        .collect::<Vec<_>>();
    let setup = Control::default();
    let input =
        SelectedAggregateUpdateInput::try_new(&kernel.contract, selection, &args, &[], &setup)
            .unwrap();
    let prepared = kernel.prepare_update_evaluation(input, mapping, Some(host), &setup)?;
    for ordinal in 0..selection.len() {
        kernel.update_row_evaluation(state, &prepared, ordinal, control)?
    }
    Ok(())
}
fn merge_selected(
    kernel: &ApproxPercentileKernel,
    state: &mut ApproxPercentileState,
    host: Arc<Host>,
    array: &ArrayRef,
    selection: Selection<'_>,
    mapping: &[usize],
    control: &dyn KernelEvaluationControl,
) -> Result<(), EvaluationFailure> {
    let setup = Control::default();
    let input = SelectedAggregateMergeInput::try_new(
        &kernel.contract,
        selection,
        EvaluatedArgument::Column(array),
        &setup,
    )
    .unwrap();
    let prepared = kernel.prepare_merge_evaluation(input, mapping, Some(host), &setup)?;
    for ordinal in 0..selection.len() {
        kernel.merge_row_evaluation(state, &prepared, ordinal, control)?
    }
    Ok(())
}
fn final_output(
    kernel: &ApproxPercentileKernel,
    state: &ApproxPercentileState,
    host: Arc<Host>,
) -> Result<ArrayRef, EvaluationFailure> {
    let host: Arc<dyn AggregateStateAllocator> = host;
    let indices = [17];
    let context = AggregateEmissionContext::from_host(&kernel.contract, &indices, 32, Some(&host));
    kernel.build_final_evaluation_with_context(
        std::iter::once(state),
        &context,
        &Control::default(),
    )
}
#[test]
fn approximate_percentile_owner_all_four_arities_native_decimal_and_list_rates() {
    let values = Arc::new(Int64Array::from(vec![Some(2), None, Some(4)])) as ArrayRef;
    let weights = Arc::new(Int64Array::from(vec![Some(1), Some(100), Some(3)])) as ArrayRef;
    let rate = Arc::new(
        Decimal128Array::from(vec![5i128; 3])
            .with_precision_and_scale(1, 1)
            .unwrap(),
    ) as ArrayRef;
    let compression = Arc::new(Int64Array::from(vec![2048; 3])) as ArrayRef;
    for (name, weighted) in [
        ("percentile_approx", false),
        ("percentile_approx_weighted", true),
    ] {
        for optional in [false, true] {
            for list in [false, true] {
                let rate = if list {
                    Arc::new(ListArray::from_iter_primitive::<
                        arrow_array::types::Float64Type,
                        _,
                        _,
                    >(
                        (0..3).map(|_| Some(vec![Some(0.), Some(0.5), Some(1.)]))
                    )) as ArrayRef
                } else {
                    rate.clone()
                };
                let mut args = vec![values.clone()];
                if weighted {
                    args.push(weights.clone())
                }
                args.push(rate);
                if optional {
                    args.push(compression.clone())
                }
                let kernel = kernel(name, types(&args), AggregateKernelPhase::Single);
                let host = Arc::new(Host::default());
                let mut state = kernel
                    .create_state_with_allocator(Some(host.clone()), &Control::default())
                    .unwrap();
                update_selected(
                    &kernel,
                    &mut state,
                    host.clone(),
                    &args,
                    Selection::all(3),
                    &[17; 3],
                    &Control::default(),
                )
                .unwrap();
                let actual = final_output(&kernel, &state, host.clone()).unwrap();
                let expected = core::scalar_output(
                    &state.core,
                    if list {
                        core::ScalarOutput::List
                    } else {
                        core::ScalarOutput::Float64
                    },
                )
                .unwrap();
                let expected = scalar::build_scalar_array(
                    actual.data_type(),
                    vec![expected],
                    &mut ScalarWork::new(None),
                )
                .unwrap();
                assert_eq!(actual.to_data(), expected.to_data());
                drop(actual);
                drop(state);
                assert_eq!(host.ledger.lock().unwrap().bytes, 0);
            }
        }
    }
}
#[test]
fn approximate_percentile_owner_full_data_rate_first_sparse_domain_latch() {
    for (name, weighted) in [
        ("percentile_approx", false),
        ("percentile_approx_weighted", true),
    ] {
        let values = Arc::new(Float64Array::from(vec![None; 4])) as ArrayRef;
        let rate = Arc::new(Float64Array::from(vec![0.5, 0.5, f64::NAN, 0.5])) as ArrayRef;
        let mut args = vec![values];
        if weighted {
            args.push(Arc::new(Int64Array::from(vec![-1; 4])) as ArrayRef)
        }
        args.push(rate);
        let kernel = kernel(name, types(&args), AggregateKernelPhase::Single);
        let host = Arc::new(Host::default());
        let mut state = kernel
            .create_state_with_allocator(Some(host.clone()), &Control::default())
            .unwrap();
        let rows = [2];
        let selection = Selection::try_sparse(4, &rows).unwrap();
        let control = Control::default();
        let failure = update_selected(
            &kernel,
            &mut state,
            host.clone(),
            &args,
            selection,
            &[17],
            &control,
        )
        .unwrap_err();
        let EvaluationFailure::InvocationData(data) = failure else {
            panic!("actual full rate Data")
        };
        assert_eq!(
            data.message(),
            format!(
                "update aggregate state: {name}: percentile parameter must be between 0 and 1, got NaN"
            )
        );
        assert_eq!(data.input_rows(), &[2]);
        assert_eq!(data.state_indices(), &[17]);
        assert!(std::ptr::eq(
            data.aggregate_contract(),
            kernel.contract.as_ref()
        ));
        assert_eq!(data.aggregate_phase(), AggregateInvocationPhase::Update);
        assert!(state.failed);
        let retry = Control::default();
        assert_eq!(
            update_selected(
                &kernel,
                &mut state,
                host.clone(),
                &args,
                selection,
                &[17],
                &retry
            )
            .unwrap_err(),
            EvaluationFailure::Kernel(KernelFailure::InstanceFailed)
        );
        assert!(retry.trace.lock().unwrap().is_empty());
        drop(data);
        drop(state);
        assert_eq!(host.ledger.lock().unwrap().bytes, 0);
    }
}
#[test]
fn approximate_percentile_owner_unsupported_type_is_bounded_contract() {
    let ty = DataType::Struct(Fields::from(vec![Field::new(
        "long original nested field 中🙂".repeat(24),
        DataType::Utf8,
        true,
    )]));
    let values = scalar::build_scalar_array(&ty, vec![None], &mut ScalarWork::new(None)).unwrap();
    let rates = Arc::new(Float64Array::from(vec![0.5])) as ArrayRef;
    let args = vec![values.clone(), rates];
    let kernel = kernel(
        "percentile_approx",
        types(&args),
        AggregateKernelPhase::Single,
    );
    let host = Arc::new(Host::default());
    let mut state = kernel
        .create_state_with_allocator(Some(host.clone()), &Control::default())
        .unwrap();
    let expected = crate::percentile_input::numeric_value_at(
        &values,
        0,
        crate::percentile_input::PercentileInputDiagnostic::LegacyLabel("percentile_approx"),
    )
    .unwrap_err();
    assert_eq!(
        novarocks_failure_contract::FailureProjection::failure_fact(&expected).class(),
        novarocks_failure_contract::FailureClass::Invariant
    );
    assert!(format!("{ty:?}").len() > 512);
    let failure = update_selected(
        &kernel,
        &mut state,
        host.clone(),
        &args,
        Selection::all(1),
        &[9],
        &Control::default(),
    )
    .unwrap_err();
    let EvaluationFailure::Kernel(cause) = failure else {
        panic!("physical input contract refusal");
    };
    assert_eq!(
        novarocks_failure_contract::FailureProjection::failure_fact(&cause).class(),
        novarocks_failure_contract::FailureClass::Invariant
    );
    assert!(
        cause
            .to_string()
            .starts_with("percentile_approx: unsupported numeric input type Struct")
    );
    assert!(cause.to_string().len() <= 512);
    drop(cause);
    drop(state);
    assert_eq!(host.ledger.lock().unwrap().bytes, 0);
}
#[test]
fn approximate_percentile_owner_actual_v4_bug_merge_data_and_emission_domain() {
    let args = vec![
        Arc::new(Float64Array::from(vec![3.])) as ArrayRef,
        Arc::new(Float64Array::from(vec![0.5])) as ArrayRef,
    ];
    let partial = kernel(
        "percentile_approx",
        types(&args),
        AggregateKernelPhase::Partial,
    );
    let host = Arc::new(Host::default());
    let mut state = partial
        .create_state_with_allocator(Some(host.clone()), &Control::default())
        .unwrap();
    update_selected(
        &partial,
        &mut state,
        host.clone(),
        &args,
        Selection::all(1),
        &[19],
        &Control::default(),
    )
    .unwrap();
    let host_dyn: Arc<dyn AggregateStateAllocator> = host.clone();
    let indices = [19];
    let context =
        AggregateEmissionContext::from_host(&partial.contract, &indices, 32, Some(&host_dyn));
    let output = partial
        .build_intermediate_evaluation_with_context(
            std::iter::once(&state),
            &context,
            &Control::default(),
        )
        .unwrap();
    let payload = output
        .as_any()
        .downcast_ref::<BinaryArray>()
        .unwrap()
        .value(0);
    // The original bounded v4 decoder omits the magic check. Its actual
    // emitted bytes remain successful; a wrong first magic byte is accepted.
    let final_kernel = kernel(
        "percentile_approx",
        types(&args),
        AggregateKernelPhase::Final,
    );
    let mut final_state = final_kernel
        .create_state_with_allocator(Some(host.clone()), &Control::default())
        .unwrap();
    merge_selected(
        &final_kernel,
        &mut final_state,
        host.clone(),
        &output,
        Selection::all(1),
        &[7],
        &Control::default(),
    )
    .unwrap();
    assert_eq!(payload, digest::encode_state(&state.core).unwrap());
    let mut wrong_magic = payload.to_vec();
    wrong_magic[0] = 0;
    let wrong_magic = Arc::new(BinaryArray::from(vec![wrong_magic.as_slice()])) as ArrayRef;
    merge_selected(
        &final_kernel,
        &mut final_state,
        host.clone(),
        &wrong_magic,
        Selection::all(1),
        &[7],
        &Control::default(),
    )
    .unwrap();
    let malformed = Arc::new(BinaryArray::from(vec![&[0xa2u8, 3][..]])) as ArrayRef;
    let expected = core::merge_row(
        &mut digest::PercentileState::new_in(
            digest::DEFAULT_COMPRESSION_FACTOR,
            allocator_api2::alloc::Global,
        ),
        &malformed,
        0,
        Diagnostic::UnweightedMerge,
    )
    .unwrap_err();
    let failure = merge_selected(
        &final_kernel,
        &mut final_state,
        host.clone(),
        &malformed,
        Selection::all(1),
        &[7],
        &Control::default(),
    )
    .unwrap_err();
    let EvaluationFailure::InvocationData(data) = failure else {
        panic!("original actual codec Data")
    };
    assert_eq!(
        data.message(),
        crate::aggregate_format::AggregateFailureStage::Merge
            .message(&expected)
            .to_string()
    );
    assert_eq!(data.input_rows(), &[0]);
    assert_eq!(data.state_indices(), &[7]);
    // Successful final emission borrows its real final-domain receipt. No
    // fabricated invalid state is used to invent a semantic emission error.
    let single = kernel(
        "percentile_approx",
        types(&args),
        AggregateKernelPhase::Single,
    );
    let mut single_state = single
        .create_state_with_allocator(Some(host.clone()), &Control::default())
        .unwrap();
    update_selected(
        &single,
        &mut single_state,
        host.clone(),
        &args,
        Selection::all(1),
        &[17],
        &Control::default(),
    )
    .unwrap();
    let final_output = final_output(&single, &single_state, host.clone()).unwrap();
    assert_eq!(final_output.len(), 1);
    drop(final_output);
    drop(single_state);
    drop(data);
    drop(final_state);
    drop(state);
    assert_eq!(host.ledger.lock().unwrap().bytes, 0);
}
#[test]
fn approximate_percentile_owner_actual_host_refusals_all_seven_origins() {
    for cause in causes() {
        let args = vec![
            Arc::new(Float64Array::from(vec![3.])) as ArrayRef,
            Arc::new(Float64Array::from(vec![0.5])) as ArrayRef,
        ];
        let kernel = kernel(
            "percentile_approx",
            types(&args),
            AggregateKernelPhase::Single,
        );
        let host = Arc::new(Host::default());
        let mut state = kernel
            .create_state_with_allocator(Some(host.clone()), &Control::default())
            .unwrap();
        let setup = Control::default();
        let eval = args
            .iter()
            .map(EvaluatedArgument::Column)
            .collect::<Vec<_>>();
        let input = SelectedAggregateUpdateInput::try_new(
            &kernel.contract,
            Selection::all(1),
            &eval,
            &[],
            &setup,
        )
        .unwrap();
        let prepared = kernel
            .prepare_update_evaluation(input, &[0], Some(host.clone()), &setup)
            .unwrap();
        arm_refusal(&host, 0, cause.clone());
        assert_eq!(
            kernel
                .update_row_evaluation(&mut state, &prepared, 0, &setup)
                .unwrap_err(),
            EvaluationFailure::Kernel(cause)
        );
        assert!(state.failed);
        drop(prepared);
        drop(state);
        assert_eq!(host.ledger.lock().unwrap().bytes, 0);
    }
}
#[test]
fn approximate_percentile_owner_every_runtime_callback_seven_cause_prefix_no_tail() {
    for invalid_rate in [false, true] {
        let args = vec![
            Arc::new(Float64Array::from(vec![3.])) as ArrayRef,
            Arc::new(Float64Array::from(vec![if invalid_rate {
                f64::NAN
            } else {
                0.5
            }])) as ArrayRef,
        ];
        let kernel = kernel(
            "percentile_approx",
            types(&args),
            AggregateKernelPhase::Single,
        );
        let run = |control: &Control| {
            let host = Arc::new(Host::default());
            let mut state = kernel
                .create_state_with_allocator(Some(host.clone()), &Control::default())
                .unwrap();
            let result = update_selected(
                &kernel,
                &mut state,
                host.clone(),
                &args,
                Selection::all(1),
                &[7],
                control,
            );
            drop(state);
            (result, host)
        };
        let success = Control::default();
        let (original, host) = run(&success);
        if invalid_rate {
            assert!(matches!(
                &original,
                Err(EvaluationFailure::InvocationData(_))
            ));
            drop(original)
        } else {
            original.unwrap()
        };
        assert_eq!(host.ledger.lock().unwrap().bytes, 0);
        let trace = success.trace.into_inner().unwrap();
        for at in 0..trace.len() {
            for cause in causes() {
                let control = Control {
                    trace: Mutex::new(vec![]),
                    refusal: Some((at, cause.clone())),
                };
                let (actual, host) = run(&control);
                assert_eq!(actual.unwrap_err(), EvaluationFailure::Kernel(cause));
                assert_eq!(host.ledger.lock().unwrap().bytes, 0);
                assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
            }
        }
    }
}

#[test]
fn approximate_percentile_owner_compile_three_causes_preserved() {
    struct Refuse(CompileControlError);
    impl PureCompileControl for Refuse {
        fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
            Err(self.0.clone())
        }
    }
    let catalog = super::super::catalogue::build_builtin_engine_function_catalog().unwrap();
    let args = [
        FunctionArgument::Value {
            value_type: FunctionValueType::new(DataType::Float64, false),
            constant: None,
        },
        FunctionArgument::Value {
            value_type: FunctionValueType::new(DataType::Decimal128(1, 1), false),
            constant: None,
        },
    ];
    for cause in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        let result = catalog.resolve_bound_user(
            "percentile_approx",
            FunctionKind::Aggregate,
            FunctionBindingRequest {
                arguments: &args,
                logical_argument_count: 2,
                expected_result_type: None,
            },
            &Refuse(cause.clone()),
        );
        assert!(matches!(result,Err(FunctionBindingError::Control(actual))if actual==cause));
    }
}

#[test]
fn approximate_percentile_owner_mixed_constant_column_compact_addresses_and_empty() {
    let ty = FunctionValueType::new(DataType::Float64, true);
    let values = Arc::new(Float64Array::from(vec![900., 3., 999.])) as ArrayRef;
    let pool = ConstantPool::try_new(
        Arc::new(ty.try_to_field("authored-value").unwrap()),
        ty.clone(),
        values.to_data(),
        ConstantPolicy {
            max_rows: 64,
            max_array_nodes: 1024,
            max_logical_elements: 4096,
            max_retained_buffer_bytes: 1 << 20,
            max_type_depth: 64,
            max_type_nodes: 4096,
            max_dictionary_depth: 64,
            max_metadata_bytes: 1 << 20,
            max_library_validation_work: 1 << 20,
            max_library_validation_bytes: 1 << 20,
        },
        CompilePhase::FunctionSpecialization,
        &Compile,
    )
    .unwrap();
    let value = pool.value(1).unwrap();
    let rates = Arc::new(Float64Array::from(vec![
        f64::NAN,
        0.5,
        f64::NAN,
        f64::NAN,
        f64::NAN,
        0.5,
        f64::NAN,
    ])) as ArrayRef;
    let rows = [1usize, 5];
    let selection = Selection::try_sparse(7, &rows).unwrap();
    let compact_array = Arc::new(Int64Array::from(vec![2048, 2048])) as ArrayRef;
    let compact =
        SelectedValues::try_new(selection, &DataType::Int64, compact_array, Box::new([])).unwrap();
    let args = [
        EvaluatedArgument::Constant(&value),
        EvaluatedArgument::Column(&rates),
        EvaluatedArgument::SelectedColumn(&compact),
    ];
    let kernel = kernel(
        "percentile_approx",
        vec![
            ty.clone(),
            ty,
            FunctionValueType::new(DataType::Int64, true),
        ],
        AggregateKernelPhase::Single,
    );
    let host = Arc::new(Host::default());
    let setup = Control::default();
    let mut state = kernel
        .create_state_with_allocator(Some(host.clone()), &setup)
        .unwrap();
    let input =
        SelectedAggregateUpdateInput::try_new(&kernel.contract, selection, &args, &[], &setup)
            .unwrap();
    let prepared = kernel
        .prepare_update_evaluation(input, &[17, 17], Some(host.clone()), &setup)
        .unwrap();
    for ordinal in 0..2 {
        kernel
            .update_row_evaluation(&mut state, &prepared, ordinal, &setup)
            .unwrap()
    }
    let actual = final_output(&kernel, &state, host.clone()).unwrap();
    assert_eq!(
        actual
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0),
        3.
    );
    drop(prepared);
    let empty_args = [
        EvaluatedArgument::Constant(&value),
        EvaluatedArgument::Column(&rates),
        EvaluatedArgument::Column(&rates),
    ];
    // Empty selection prepares no row computation, while canonical input
    // type validation still rejects a wrong third channel before preparation.
    assert!(
        SelectedAggregateUpdateInput::try_new(
            &kernel.contract,
            Selection::try_sparse(7, &[]).unwrap(),
            &empty_args,
            &[],
            &setup
        )
        .is_err()
    );
    let empty_selection = Selection::try_sparse(7, &[]).unwrap();
    let empty_compression_array = Arc::new(Int64Array::from(Vec::<i64>::new())) as ArrayRef;
    let empty_compression = SelectedValues::try_new(
        empty_selection,
        &DataType::Int64,
        empty_compression_array,
        Box::new([]),
    )
    .unwrap();
    let valid_empty_args = [
        EvaluatedArgument::Constant(&value),
        EvaluatedArgument::Column(&rates),
        EvaluatedArgument::SelectedColumn(&empty_compression),
    ];
    let empty_input = SelectedAggregateUpdateInput::try_new(
        &kernel.contract,
        empty_selection,
        &valid_empty_args,
        &[],
        &setup,
    )
    .unwrap();
    let empty_prepared = kernel
        .prepare_update_evaluation(empty_input, &[], Some(host.clone()), &setup)
        .unwrap();
    drop(empty_prepared);
    drop(state);
    assert_eq!(host.ledger.lock().unwrap().bytes, 0);
}

#[test]
fn approximate_percentile_owner_fallible_final_clone_preserves_all_host_causes() {
    for list in [false, true] {
        let run = |refusal: Option<(usize, KernelFailure)>| {
            let rate = if list {
                let mut b = arrow_array::builder::ListBuilder::new(
                    arrow_array::builder::Float64Builder::new(),
                );
                b.values().append_value(0.5);
                b.append(true);
                Arc::new(b.finish()) as ArrayRef
            } else {
                Arc::new(Float64Array::from(vec![0.5])) as ArrayRef
            };
            let args = vec![Arc::new(Float64Array::from(vec![3.])) as ArrayRef, rate];
            let kernel = kernel(
                "percentile_approx",
                types(&args),
                AggregateKernelPhase::Single,
            );
            let host = Arc::new(Host::default());
            let mut state = kernel
                .create_state_with_allocator(Some(host.clone()), &Control::default())
                .unwrap();
            update_selected(
                &kernel,
                &mut state,
                host.clone(),
                &args,
                Selection::all(1),
                &[17],
                &Control::default(),
            )
            .unwrap();
            let start = host.ledger.lock().unwrap().attempts;
            if let Some((offset, cause)) = refusal {
                arm_refusal(&host, offset, cause);
            }
            let result = final_output(&kernel, &state, host.clone());
            let attempts = host.ledger.lock().unwrap().attempts - start;
            drop(state);
            (result, attempts, host)
        };
        let (result, attempts, host) = run(None);
        assert!(
            attempts > 1,
            "actual emit metadata and original clone allocate through the same host"
        );
        drop(result.unwrap());
        assert_eq!(host.ledger.lock().unwrap().bytes, 0);
        for offset in 0..attempts {
            for cause in causes() {
                let (result, _, host) = run(Some((offset, cause.clone())));
                assert_eq!(result.unwrap_err(), EvaluationFailure::Kernel(cause));
                assert_eq!(host.ledger.lock().unwrap().bytes, 0);
            }
        }
    }
}
#[test]
fn approximate_percentile_owner_final_callback_seven_causes_no_tail() {
    let args = vec![
        Arc::new(Float64Array::from(vec![3.])) as ArrayRef,
        Arc::new(Float64Array::from(vec![0.5])) as ArrayRef,
    ];
    let kernel = kernel(
        "percentile_approx",
        types(&args),
        AggregateKernelPhase::Single,
    );
    let run = |control: &Control| {
        let host = Arc::new(Host::default());
        let mut state = kernel
            .create_state_with_allocator(Some(host.clone()), &Control::default())
            .unwrap();
        update_selected(
            &kernel,
            &mut state,
            host.clone(),
            &args,
            Selection::all(1),
            &[17],
            &Control::default(),
        )
        .unwrap();
        let allocator: Arc<dyn AggregateStateAllocator> = host.clone();
        let indices = [17];
        let context =
            AggregateEmissionContext::from_host(&kernel.contract, &indices, 32, Some(&allocator));
        let output =
            kernel.build_final_evaluation_with_context(std::iter::once(&state), &context, control);
        drop(state);
        (output, host)
    };
    let success = Control::default();
    let (output, host) = run(&success);
    drop(output.unwrap());
    assert_eq!(host.ledger.lock().unwrap().bytes, 0);
    let trace = success.trace.into_inner().unwrap();
    for at in 0..trace.len() {
        for cause in causes() {
            let control = Control {
                trace: Mutex::new(vec![]),
                refusal: Some((at, cause.clone())),
            };
            let (output, host) = run(&control);
            assert_eq!(output.unwrap_err(), EvaluationFailure::Kernel(cause));
            assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
            assert_eq!(host.ledger.lock().unwrap().bytes, 0);
        }
    }
}

#[path = "aggregate_approx_percentile_intermediate_tests.rs"]
mod intermediate_supplement;

#[path = "aggregate_percentile_union_private_tests.rs"]
mod percentile_union_private_tests;

#[path = "aggregate_percentile_union_core_control_tests.rs"]
mod percentile_union_core_control_tests;
