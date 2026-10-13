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
//! Private profile/component tests, separate from later catalogue installation.
//! Inputs come from current source and actual canonical declarations. The local
//! refinement fixture validates those declarations; it does not stand in for
//! a successful public NOwner prepare. N maintains these on profile changes.
use super::*;
use crate::builtin::aggregate_n_largeint_resources::tests::{Host, causes, clean, control};
use crate::builtin::catalogue::builtin_engine_function_catalog;
use arrow_array::{Int64Array, ListArray};
use novarocks_type_contract::{
    CallEffects, CallProofScope, CompileControlError, DecimalOverflowPolicy, EvaluationDemand,
    EvaluationDomainId, ExpressionEffectContext, ExpressionUseId, FunctionEffectDeclaration,
    SemanticParameters,
};

struct Compile;
impl PureCompileControl for Compile {
    fn checkpoint(&self, _: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert!(units <= 256);
        Ok(())
    }
}
struct Fixture {
    id: FunctionId,
    selected: Arc<FunctionBindingSelection>,
    arguments: Vec<FunctionArgument>,
    declaration: FunctionEffectDeclaration,
}
impl FunctionEffectOwner for Fixture {
    type Error = FunctionBindingError;
    fn declaration(
        &self,
        id: &FunctionId,
        selected: &FunctionBindingSelection,
    ) -> Result<&FunctionEffectDeclaration, Self::Error> {
        if id != &self.id || selected != self.selected.as_ref() {
            return Err(FunctionBindingError::UnknownFunction);
        }
        Ok(&self.declaration)
    }
    fn validate_and_refine(
        &self,
        input: CallEffectInput<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<CallEffects, FunctionEffectOwnerError<Self::Error>> {
        if input.function_id != &self.id
            || input.kind != FunctionKind::Aggregate
            || input.selected != self.selected.as_ref()
            || input.request.logical_argument_count != 2
            || input.request.arguments.len() != 2
            || !input.environment.is_empty()
        {
            return Err(FunctionBindingError::InvalidBinding(
                "N component fixture exact call differs".into(),
            )
            .into());
        }
        for (actual, expected) in input.request.arguments.iter().zip(&self.arguments) {
            if !actual.equals_observed(expected, CompilePhase::FunctionSpecialization, control)? {
                return Err(FunctionBindingError::InvalidBinding(
                    "N component fixture argument differs".into(),
                )
                .into());
            }
        }
        Ok(CallEffects {
            value_stability: self.declaration.value_stability,
            own_row_error: self.declaration.own_row_error,
            failure_behavior: self.declaration.failure_behavior,
            null_behavior: self.declaration.null_behavior,
            argument_control: self.declaration.argument_control,
            instance_state: self.declaration.instance_state,
            observable_effects: self.declaration.observable_effects,
            environment: Box::new([]),
            proof_scope: input.proof_scope,
        })
    }
}
fn source(
    name: &str,
    value_nullable: bool,
    limit: DataType,
    limit_nullable: bool,
) -> Arc<FunctionCallContract> {
    source_with(name, value_nullable, limit, limit_nullable, |_| {})
}
fn source_with(
    name: &str,
    value_nullable: bool,
    limit: DataType,
    limit_nullable: bool,
    adjust: impl FnOnce(&mut Fixture),
) -> Arc<FunctionCallContract> {
    let arguments = vec![
        FunctionArgument::Value {
            value_type: FunctionValueType::try_with_logical_type(
                DataType::FixedSizeBinary(16),
                value_nullable,
                ValueLogicalType::LargeInt,
            )
            .unwrap(),
            constant: None,
        },
        FunctionArgument::Value {
            value_type: FunctionValueType::new(limit, limit_nullable),
            constant: None,
        },
    ];
    let catalog = builtin_engine_function_catalog();
    let request = FunctionBindingRequest {
        arguments: &arguments,
        logical_argument_count: 2,
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
        .unwrap()
        .effects()
        .clone();
    let mut fixture = Fixture {
        id: bound.function_id,
        selected: Arc::new(bound.selected),
        arguments: arguments.clone(),
        declaration,
    };
    adjust(&mut fixture);
    let uses = [
        Some(ExpressionUseId::new(43)),
        Some(ExpressionUseId::new(44)),
    ];
    let parameters = SemanticParameters::default();
    let context = ExpressionEffectContext {
        use_id: ExpressionUseId::new(42),
        domain: EvaluationDomainId::new(42),
        demand: EvaluationDemand::Value,
    };
    let input = CallEffectInput {
        function_id: &fixture.id,
        kind: FunctionKind::Aggregate,
        selected: &fixture.selected,
        request,
        argument_uses: CallArgumentUses::SelectedChannels(&uses),
        context,
        parameters: &parameters,
        environment: &[],
        decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
        proof_scope: CallProofScope::Domain(context.domain),
    };
    let receipt = refine_call_effects(&fixture, input, &Compile).unwrap();
    Arc::new(
        FunctionCallContract::from_refined(
            input,
            &receipt,
            Arc::clone(&fixture.selected),
            &Compile,
        )
        .unwrap(),
    )
}
fn phase(
    call: Arc<FunctionCallContract>,
    phase: AggregateKernelPhase,
    smallest: bool,
) -> NLargeIntKernel {
    let state = (!phase.consumes_logical_arguments())
        .then(|| FunctionValueType::new(DataType::Binary, true));
    let contract = Arc::new(
        AggregateCallContract::try_new(call, phase, false, Arc::from([]), state, &Compile).unwrap(),
    );
    NLargeIntKernel::try_new(contract, smallest, &Compile).unwrap()
}
fn kernel(phase_id: AggregateKernelPhase) -> NLargeIntKernel {
    phase(source("min_n", true, DataType::Int64, true), phase_id, true)
}
fn values(values: &[Option<i128>]) -> ArrayRef {
    crate::largeint::array_from_i128(values).unwrap()
}
fn update(
    kernel: &NLargeIntKernel,
    state: &mut NLargeIntState,
    values: &ArrayRef,
    limits: &ArrayRef,
    selected: Selection<'_>,
    host: &Arc<Host>,
    constant_limit: bool,
) -> Result<(), EvaluationFailure> {
    let args = [
        EvaluatedArgument::Column(values),
        if constant_limit {
            EvaluatedArgument::Scalar(limits)
        } else {
            EvaluatedArgument::Column(limits)
        },
    ];
    let control = control(host, None);
    let input =
        SelectedAggregateUpdateInput::try_new(&kernel.contract, selected, &args, &[], &control)
            .unwrap();
    let mapping = vec![0; selected.len()];
    let prepared = kernel.prepare_update_evaluation(
        input,
        &mapping,
        Some(Arc::clone(host) as Arc<dyn AggregateStateAllocator>),
        &control,
    )?;
    for row in 0..selected.len() {
        kernel.update_row_evaluation(state, &prepared, row, &control)?;
    }
    Ok(())
}
fn merge(
    kernel: &NLargeIntKernel,
    state: &mut NLargeIntState,
    bytes: &ArrayRef,
    host: &Arc<Host>,
) -> Result<(), EvaluationFailure> {
    let control = control(host, None);
    let input = SelectedAggregateMergeInput::try_new(
        &kernel.contract,
        Selection::all(bytes.len()),
        EvaluatedArgument::Column(bytes),
        &control,
    )
    .unwrap();
    let mapping = vec![0; bytes.len()];
    let prepared = kernel.prepare_merge_evaluation(
        input,
        &mapping,
        Some(Arc::clone(host) as Arc<dyn AggregateStateAllocator>),
        &control,
    )?;
    for row in 0..bytes.len() {
        kernel.merge_row_evaluation(state, &prepared, row, &control)?;
    }
    Ok(())
}
fn emit(
    kernel: &NLargeIntKernel,
    state: &NLargeIntState,
    host: &Arc<Host>,
) -> Result<ArrayRef, EvaluationFailure> {
    let authority = Arc::clone(host) as Arc<dyn AggregateStateAllocator>;
    let context = AggregateEmissionContext::from_host(&kernel.contract, &[0], 1, Some(&authority));
    let control = control(host, None);
    if kernel.contract.phase().produces_final_result() {
        kernel.build_final_evaluation_with_context(std::iter::once(state), &context, &control)
    } else {
        kernel.build_intermediate_evaluation_with_context(
            std::iter::once(state),
            &context,
            &control,
        )
    }
}
fn state(kernel: &NLargeIntKernel, host: &Arc<Host>) -> NLargeIntState {
    kernel
        .create_state_with_allocator(
            Some(Arc::clone(host) as Arc<dyn AggregateStateAllocator>),
            &control(host, None),
        )
        .unwrap()
}
fn read_list(array: &ArrayRef) -> Vec<i128> {
    let list = array.as_any().downcast_ref::<ListArray>().unwrap();
    let child = list.value(0);
    let child = child
        .as_any()
        .downcast_ref::<arrow_array::FixedSizeBinaryArray>()
        .unwrap();
    (0..child.len())
        .map(|row| i128::from_be_bytes(child.value(row).try_into().unwrap()))
        .collect()
}

#[test]
fn private_profile_validates_all_declarations_and_actual_four_phase_contracts() {
    for name in ["min_n", "max_n"] {
        for nullable in [false, true] {
            for limit in [
                DataType::Int8,
                DataType::Int16,
                DataType::Int32,
                DataType::Int64,
            ] {
                for limit_nullable in [false, true] {
                    let call = source(name, nullable, limit.clone(), limit_nullable);
                    for phase_id in [
                        AggregateKernelPhase::Single,
                        AggregateKernelPhase::Partial,
                        AggregateKernelPhase::Intermediate,
                        AggregateKernelPhase::Final,
                    ] {
                        let kernel = phase(Arc::clone(&call), phase_id, name == "min_n");
                        assert_eq!(kernel.contract.phase(), phase_id);
                        assert!(
                            kernel.has_invocation_data()
                                && kernel.requires_helper_memory()
                                && kernel.requires_emission_context()
                        );
                        assert!(kernel.requires_empty_update_preparation());
                        let DataType::List(field) = &kernel.contract.final_type().data_type else {
                            panic!("List result")
                        };
                        assert_eq!(field.data_type(), &DataType::FixedSizeBinary(16));
                        assert!(field.is_nullable());
                        assert_eq!(
                            field.metadata(),
                            &std::collections::HashMap::from([(
                                "nr_logical_type".to_owned(),
                                "largeint".to_owned(),
                            )])
                        );
                        assert_eq!(
                            kernel.contract.intermediate_type(),
                            &FunctionValueType::new(DataType::Binary, true)
                        );
                    }
                }
            }
        }
    }
}
#[test]
fn sparse_selection_and_independent_scalar_limit_preserve_original_result() {
    let host = Arc::new(Host::default());
    {
        let kernel = kernel(AggregateKernelPhase::Single);
        let mut state = state(&kernel, &host);
        let input = values(&[Some(999), Some(-7), Some(888), Some(3)]);
        let limits: ArrayRef = Arc::new(Int64Array::from(vec![2]));
        update(
            &kernel,
            &mut state,
            &input,
            &limits,
            Selection::try_sparse(4, &[1, 3]).unwrap(),
            &host,
            true,
        )
        .unwrap();
        let output = emit(&kernel, &state, &host).unwrap();
        assert_eq!(read_list(&output), vec![-7, 3]);
        assert_eq!(output.data_type(), &kernel.contract.final_type().data_type);
    }
    clean(&host);
}
#[test]
fn null_limit_before_null_value_is_full_data_and_retains_its_domain_until_last_clone() {
    let host = Arc::new(Host::default());
    let data = {
        let kernel = kernel(AggregateKernelPhase::Single);
        let mut state = state(&kernel, &host);
        let input = values(&[Some(77), None]);
        let limits: ArrayRef = Arc::new(Int64Array::from(vec![None]));
        let error = update(
            &kernel,
            &mut state,
            &input,
            &limits,
            Selection::try_sparse(2, &[1]).unwrap(),
            &host,
            true,
        )
        .unwrap_err();
        let EvaluationFailure::InvocationData(data) = error else {
            panic!("full Data required")
        };
        assert_eq!(
            data.message(),
            "update aggregate state: min_n/max_n limit cannot be null"
        );
        assert_eq!(data.input_rows(), &[1]);
        assert_eq!(data.state_indices(), &[0]);
        assert_eq!(data.batch_rows(), 2);
        assert!(state.failed.get());
        assert_eq!(kernel.retained_bytes(&state), state.base_retained);
        data
    };
    assert!(!host.ledger.lock().unwrap().blocks.is_empty());
    let alias = data.clone();
    drop(data);
    assert!(!host.ledger.lock().unwrap().blocks.is_empty());
    std::thread::spawn(move || drop(alias)).join().unwrap();
    clean(&host);
}
#[test]
fn partial_intermediate_final_use_one_codec_and_exact_largeint_result() {
    let host = Arc::new(Host::default());
    {
        let call = source("max_n", true, DataType::Int64, true);
        let partial = phase(Arc::clone(&call), AggregateKernelPhase::Partial, false);
        let intermediate = phase(Arc::clone(&call), AggregateKernelPhase::Intermediate, false);
        let final_kernel = phase(call, AggregateKernelPhase::Final, false);
        let mut left = state(&partial, &host);
        let limits: ArrayRef = Arc::new(Int64Array::from(vec![2]));
        update(
            &partial,
            &mut left,
            &values(&[Some(i128::MIN), Some(8), None, Some(i128::MAX)]),
            &limits,
            Selection::all(4),
            &host,
            true,
        )
        .unwrap();
        let bytes = emit(&partial, &left, &host).unwrap();
        let mut middle = state(&intermediate, &host);
        merge(&intermediate, &mut middle, &bytes, &host).unwrap();
        let bytes = emit(&intermediate, &middle, &host).unwrap();
        let mut target = state(&final_kernel, &host);
        merge(&final_kernel, &mut target, &bytes, &host).unwrap();
        assert_eq!(
            read_list(&emit(&final_kernel, &target, &host).unwrap()),
            vec![8, i128::MAX]
        );
        let changed = phase(
            source("min_n", true, DataType::Int64, true),
            AggregateKernelPhase::Final,
            true,
        );
        assert!(
            partial
                .clone_for_local_phase(Arc::clone(&changed.contract), &Compile)
                .is_err()
        );
    }
    clean(&host);
}
#[test]
fn empty_and_nonempty_child_aliases_hold_original_output_until_cross_thread_last_drop() {
    for empty in [false, true] {
        let host = Arc::new(Host::default());
        let child = {
            let kernel = kernel(AggregateKernelPhase::Single);
            let mut state = state(&kernel, &host);
            if !empty {
                let limits: ArrayRef = Arc::new(Int64Array::from(vec![2]));
                update(
                    &kernel,
                    &mut state,
                    &values(&[Some(-1), Some(4)]),
                    &limits,
                    Selection::all(2),
                    &host,
                    true,
                )
                .unwrap();
            }
            let result = emit(&kernel, &state, &host).unwrap();
            let list = result.as_any().downcast_ref::<ListArray>().unwrap();
            assert!(!list.is_null(0));
            assert_eq!(list.value_length(0), if empty { 0 } else { 2 });
            Arc::clone(list.values())
        };
        assert!(!host.ledger.lock().unwrap().loans.is_empty());
        std::thread::spawn(move || drop(child)).join().unwrap();
        clean(&host);
    }
}
#[test]
fn zero_rows_final_and_intermediate_are_real_outputs_with_prior_transfer_funding() {
    for phase_id in [AggregateKernelPhase::Single, AggregateKernelPhase::Partial] {
        let host = Arc::new(Host::default());
        let output = {
            let kernel = kernel(phase_id);
            let authority = Arc::clone(&host) as Arc<dyn AggregateStateAllocator>;
            let context =
                AggregateEmissionContext::from_host(&kernel.contract, &[], 0, Some(&authority));
            let control = control(&host, None);
            let result = if phase_id.produces_final_result() {
                kernel.build_final_evaluation_with_context(std::iter::empty(), &context, &control)
            } else {
                kernel.build_intermediate_evaluation_with_context(
                    std::iter::empty(),
                    &context,
                    &control,
                )
            };
            let output = result.unwrap();
            assert_eq!(output.len(), 0);
            assert_eq!(
                output.data_type(),
                if phase_id.produces_final_result() {
                    &kernel.contract.final_type().data_type
                } else {
                    &kernel.contract.intermediate_type().data_type
                }
            );
            output
        };
        assert!(!host.ledger.lock().unwrap().loans.is_empty());
        drop(output);
        clean(&host);
    }
}

fn payload(tag: u8, body: &[u8]) -> ArrayRef {
    let mut bytes = Vec::from(4_u32.to_le_bytes());
    bytes.extend_from_slice(&1_u32.to_le_bytes());
    bytes.push(tag);
    bytes.extend_from_slice(body);
    Arc::new(BinaryArray::from(vec![bytes.as_slice()]))
}

#[test]
fn all_original_merge_tags_survive_intermediate_and_incompatible_final_is_late_data() {
    let cases = [
        (1, vec![1]),
        (2, 7_i64.to_le_bytes().to_vec()),
        (3, 1.25_f64.to_bits().to_le_bytes().to_vec()),
        (4, [3_u32.to_le_bytes().as_slice(), b"abc"].concat()),
        (5, 2_i32.to_le_bytes().to_vec()),
        (6, 3_i64.to_le_bytes().to_vec()),
        (7, (-9_i128).to_le_bytes().to_vec()),
        (11, [2_u32.to_le_bytes().as_slice(), b"-8"].concat()),
    ];
    for (tag, body) in cases {
        let host = Arc::new(Host::default());
        {
            let call = source("min_n", true, DataType::Int64, true);
            let intermediate = phase(Arc::clone(&call), AggregateKernelPhase::Intermediate, true);
            let final_kernel = phase(call, AggregateKernelPhase::Final, true);
            let input = payload(tag, &body);
            let mut middle = state(&intermediate, &host);
            merge(&intermediate, &mut middle, &input, &host).unwrap();
            let output = emit(&intermediate, &middle, &host).unwrap();
            assert_eq!(
                output
                    .as_any()
                    .downcast_ref::<BinaryArray>()
                    .unwrap()
                    .value(0),
                input
                    .as_any()
                    .downcast_ref::<BinaryArray>()
                    .unwrap()
                    .value(0)
            );
            let mut target = state(&final_kernel, &host);
            merge(&final_kernel, &mut target, &output, &host).unwrap();
            if tag == 7 {
                assert_eq!(
                    read_list(&emit(&final_kernel, &target, &host).unwrap()),
                    vec![-9]
                );
            } else {
                let attempts_before = host.ledger.lock().unwrap().attempts;
                let error = emit(&final_kernel, &target, &host).unwrap_err();
                let EvaluationFailure::InvocationData(data) = error else {
                    panic!("late full Data required")
                };
                assert_eq!(
                    data.message(),
                    "build aggregate final output: scalar output type mismatch for LargeInt"
                );
                assert_eq!(data.state_indices(), &[0]);
                assert!(target.failed.get());
                // The original conversion runs only after whole-emission admission.
                assert!(host.ledger.lock().unwrap().attempts >= attempts_before + 2);
            }
        }
        clean(&host);
    }
}

#[test]
fn empty_preparation_is_real_and_foreign_host_or_state_contract_is_rejected() {
    let host = Arc::new(Host::default());
    let other = Arc::new(Host::default());
    {
        let own_kernel = kernel(AggregateKernelPhase::Single);
        let input_values = values(&[]);
        let limits: ArrayRef = Arc::new(Int64Array::from(vec![2]));
        let args = [
            EvaluatedArgument::Column(&input_values),
            EvaluatedArgument::Scalar(&limits),
        ];
        let ctl = control(&host, None);
        let input = SelectedAggregateUpdateInput::try_new(
            &own_kernel.contract,
            Selection::all(0),
            &args,
            &[],
            &ctl,
        )
        .unwrap();
        let prepared = own_kernel
            .prepare_update_evaluation(
                input,
                &[],
                Some(Arc::clone(&host) as Arc<dyn AggregateStateAllocator>),
                &ctl,
            )
            .unwrap();
        assert!(own_kernel.prepared_update_retained_bytes(&prepared) > 0);
        assert!(!host.ledger.lock().unwrap().blocks.is_empty());
        drop(prepared);
        let mut own = state(&own_kernel, &host);
        let error = update(
            &own_kernel,
            &mut own,
            &values(&[Some(3)]),
            &limits,
            Selection::all(1),
            &other,
            true,
        )
        .unwrap_err();
        assert!(matches!(
            error,
            EvaluationFailure::Kernel(KernelFailure::InvalidProgram(_))
        ));
        assert!(own.failed.get());
        let fresh = kernel(AggregateKernelPhase::Single);
        let foreign_state = state(&fresh, &host);
        assert!(matches!(
            emit(&own_kernel, &foreign_state, &host),
            Err(EvaluationFailure::Kernel(KernelFailure::InvalidProgram(_)))
        ));
        assert!(foreign_state.failed.get());
    }
    clean(&host);
    clean(&other);
}

#[test]
fn local_phase_clone_keeps_one_source_call_and_revalidates_actual_phase_contract() {
    let host = Arc::new(Host::default());
    {
        let call = source("min_n", true, DataType::Int64, true);
        let original = phase(Arc::clone(&call), AggregateKernelPhase::Single, true);
        for phase_id in [
            AggregateKernelPhase::Single,
            AggregateKernelPhase::Partial,
            AggregateKernelPhase::Intermediate,
            AggregateKernelPhase::Final,
        ] {
            let target = phase(Arc::clone(&call), phase_id, true);
            let cloned = original
                .clone_for_local_phase(Arc::clone(&target.contract), &Compile)
                .unwrap();
            assert!(Arc::ptr_eq(cloned.contract.call(), &call));
            assert_eq!(cloned.contract.phase(), phase_id);
            drop(state(&cloned, &host));
        }
    }
    clean(&host);
}

// Every frontier below is a real private-profile operation. Preparation and
// source input creation use a separate non-refusing control, so the injected
// checkpoint index refers only to the operation under test.
fn run_control_stage(
    stage: usize,
    host: &Arc<Host>,
    ctl: &dyn KernelEvaluationControl,
) -> Result<(), EvaluationFailure> {
    run_stage(stage, host, ctl, || {})
}
fn run_stage(
    stage: usize,
    host: &Arc<Host>,
    ctl: &dyn KernelEvaluationControl,
    start: impl FnOnce(),
) -> Result<(), EvaluationFailure> {
    let mut start = Some(start);
    let mut start = || start.take().expect("one actual operation frontier")();
    let phase_id = match stage {
        3 | 4 | 5 => AggregateKernelPhase::Final,
        6 => AggregateKernelPhase::Partial,
        _ => AggregateKernelPhase::Single,
    };
    let kernel = kernel(phase_id);
    if stage == 0 {
        start();
        drop(kernel.create_state_with_allocator(
            Some(Arc::clone(host) as Arc<dyn AggregateStateAllocator>),
            ctl,
        )?);
        return Ok(());
    }
    let mut target = state(&kernel, host);
    let setup = control(host, None);
    let input_values = values(&(0..64).map(|i| Some(i as i128)).collect::<Vec<_>>());
    let limits: ArrayRef = Arc::new(Int64Array::from(vec![64]));
    if stage == 1 || stage == 2 {
        let args = [
            EvaluatedArgument::Column(&input_values),
            EvaluatedArgument::Scalar(&limits),
        ];
        let input = SelectedAggregateUpdateInput::try_new(
            &kernel.contract,
            Selection::all(64),
            &args,
            &[],
            &setup,
        )
        .unwrap();
        let mapping = [0; 64];
        if stage == 1 {
            start();
        }
        let batch = kernel.prepare_update_evaluation(
            input,
            &mapping,
            Some(Arc::clone(host) as Arc<dyn AggregateStateAllocator>),
            if stage == 1 { ctl } else { &setup },
        )?;
        if stage == 2 {
            for row in 0..63 {
                kernel.update_row_evaluation(&mut target, &batch, row, &setup)?;
            }
            start();
            let result = kernel.update_row_evaluation(&mut target, &batch, 63, ctl);
            if result.is_err() {
                assert!(target.failed.get());
            }
            result?;
        }
        return Ok(());
    }
    let mut encoded = Vec::from(64_u32.to_le_bytes());
    encoded.extend_from_slice(&64_u32.to_le_bytes());
    for i in 0..64_i128 {
        encoded.push(7);
        encoded.extend_from_slice(&i.to_le_bytes());
    }
    let bytes: ArrayRef = Arc::new(BinaryArray::from(vec![encoded.as_slice()]));
    if stage == 3 || stage == 4 {
        let input = SelectedAggregateMergeInput::try_new(
            &kernel.contract,
            Selection::all(1),
            EvaluatedArgument::Column(&bytes),
            &setup,
        )
        .unwrap();
        if stage == 3 {
            start();
        }
        let batch = kernel.prepare_merge_evaluation(
            input,
            &[0],
            Some(Arc::clone(host) as Arc<dyn AggregateStateAllocator>),
            if stage == 3 { ctl } else { &setup },
        )?;
        if stage == 4 {
            start();
            let result = kernel.merge_row_evaluation(&mut target, &batch, 0, ctl);
            if result.is_err() {
                assert!(target.failed.get());
            }
            result?;
        }
        return Ok(());
    }
    if stage == 5 {
        merge(&kernel, &mut target, &bytes, host)?;
    } else {
        update(
            &kernel,
            &mut target,
            &input_values,
            &limits,
            Selection::all(64),
            host,
            true,
        )?;
    }
    let authority = Arc::clone(host) as Arc<dyn AggregateStateAllocator>;
    let context = AggregateEmissionContext::from_host(&kernel.contract, &[0], 1, Some(&authority));
    start();
    let result = if stage == 5 {
        kernel.build_final_evaluation_with_context(std::iter::once(&target), &context, ctl)
    } else {
        kernel.build_intermediate_evaluation_with_context(std::iter::once(&target), &context, ctl)
    };
    if result.is_err() {
        assert!(target.failed.get());
    }
    drop(result?);
    Ok(())
}

#[test]
fn every_actual_profile_checkpoint_preserves_seven_causes_without_footer_and_cleans() {
    for stage in 0..7 {
        let host = Arc::new(Host::default());
        let baseline = control(&host, None);
        run_control_stage(stage, &host, &baseline).unwrap();
        let count = baseline.checks.lock().unwrap().len();
        assert!(count > 0);
        clean(&host);
        for stop in 0..count {
            for cause in causes() {
                let host = Arc::new(Host::default());
                let ctl = control(&host, Some((stop, cause.clone())));
                let failure = run_control_stage(stage, &host, &ctl).unwrap_err();
                let EvaluationFailure::Kernel(actual) = failure else {
                    panic!("typed cause required")
                };
                assert_eq!(actual, cause);
                assert_eq!(ctl.checks.lock().unwrap().len(), stop + 1);
                clean(&host);
            }
        }
    }
}

#[test]
fn private_profile_rejects_legal_contracts_with_foreign_overload_or_effects() {
    for alternate_effect in [false, true] {
        let call = source_with("min_n", true, DataType::Int64, true, |fixture| {
            if alternate_effect {
                fixture.declaration.value_stability = FunctionVolatility::Stable;
            } else {
                Arc::make_mut(&mut fixture.selected).overload =
                    FunctionOverloadId::try_new("builtin.aggregate/min_n/foreign-v1").unwrap();
            }
        });
        for phase_id in [
            AggregateKernelPhase::Single,
            AggregateKernelPhase::Partial,
            AggregateKernelPhase::Intermediate,
            AggregateKernelPhase::Final,
        ] {
            let state_type = (!phase_id.consumes_logical_arguments())
                .then(|| FunctionValueType::new(DataType::Binary, true));
            let contract = Arc::new(
                AggregateCallContract::try_new(
                    Arc::clone(&call),
                    phase_id,
                    false,
                    Arc::from([]),
                    state_type,
                    &Compile,
                )
                .unwrap(),
            );
            assert!(matches!(
                NLargeIntKernel::try_new(contract, true, &Compile),
                Err(KernelFailure::InvalidProgram(_))
            ));
        }
    }
}

#[test]
fn every_actual_opaque_and_typed_admission_preserves_seven_causes_and_releases() {
    for stage in 0..7 {
        for typed in [false, true] {
            let host = Arc::new(Host::default());
            let begin = Cell::new(0);
            run_stage(stage, &host, &control(&host, None), || {
                let ledger = host.ledger.lock().unwrap();
                begin.set(if typed {
                    ledger.typed_attempts
                } else {
                    ledger.attempts
                });
            })
            .unwrap();
            let count = {
                let ledger = host.ledger.lock().unwrap();
                (if typed {
                    ledger.typed_attempts
                } else {
                    ledger.attempts
                }) - begin.get()
            };
            clean(&host);
            // An update can reuse existing typed capacity. Zero reached typed
            // allocations are a real fact, not a fabricated refusal frontier.
            if !typed {
                assert!(count > 0);
            }
            for stop in 0..count {
                for cause in causes() {
                    let host = Arc::new(Host::default());
                    let begin = Cell::new(0);
                    let result = run_stage(stage, &host, &control(&host, None), || {
                        let ledger = host.ledger.lock().unwrap();
                        let at = if typed {
                            ledger.typed_attempts
                        } else {
                            ledger.attempts
                        };
                        begin.set(at);
                        let port = if typed {
                            &host.armed_typed
                        } else {
                            &host.armed_opaque
                        };
                        *port.lock().unwrap() = Some((at + stop, cause.clone()));
                    });
                    assert_eq!(result, Err(EvaluationFailure::Kernel(cause)));
                    let at = {
                        let ledger = host.ledger.lock().unwrap();
                        if typed {
                            ledger.typed_attempts
                        } else {
                            ledger.attempts
                        }
                    };
                    assert_eq!(at, begin.get() + stop + 1);
                    clean(&host);
                }
            }
        }
    }
}

#[test]
fn original_empty_binary_builder_capacities_are_an_upgrade_sensitive_budget_fact() {
    // This private policy prices the current original Arrow builder. An Arrow
    // upgrade that changes its public retained payload requires re-pricing.
    let mut builder = arrow_array::builder::BinaryBuilder::new();
    let array = builder.finish();
    let data = array.to_data();
    assert_eq!(data.buffers()[0].capacity(), 1025 * size_of::<i32>());
    assert_eq!(data.buffers()[1].capacity(), 1024);
    assert_eq!(array.len(), 0);
}
