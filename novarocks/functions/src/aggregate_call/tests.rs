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
    AggregateBindingSelection, CallEffectInput, FunctionArgument, FunctionBindingError,
    FunctionBindingRequest, FunctionBindingSelection, FunctionEffectOwner,
    FunctionEffectOwnerError, FunctionId, FunctionOverloadId, RowDataError, SelectedValues,
    refine_call_effects,
};
use arrow_array::{ArrayRef, Int32Array, Int64Array};
use arrow_schema::DataType;
use novarocks_type_contract::{
    CallEffects, CallProofScope, CompileControlError, DecimalOverflowPolicy, EvaluationDemand,
    EvaluationRegionId, ExpressionEffectContext, ExpressionUseId, FunctionEffectDeclaration,
    FunctionFailureBehavior, FunctionInstanceState, FunctionIntrinsicRowError,
    FunctionNullBehavior, FunctionVolatility, ObservableEffects, SemanticParameters,
    ValueLogicalType,
};
use std::time::Duration;

struct CompileControl(Option<CompileControlError>);
impl PureCompileControl for CompileControl {
    fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
        self.0.clone().map_or(Ok(()), Err)
    }
}
struct EvaluationControl(Option<KernelFailure>);
impl KernelEvaluationControl for EvaluationControl {
    fn checkpoint(&self, _: u32) -> Result<(), KernelFailure> {
        self.0.clone().map_or(Ok(()), Err)
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("aggregate input validation must not wait")
    }
}

struct Fixture {
    id: FunctionId,
    selected: Arc<FunctionBindingSelection>,
    arguments: Vec<FunctionArgument>,
    argument_uses: Vec<Option<ExpressionUseId>>,
    logical: usize,
    declaration: FunctionEffectDeclaration,
    parameters: SemanticParameters,
}
impl Fixture {
    fn new(
        logical: &[FunctionValueType],
        order: &[FunctionValueType],
        state: FunctionValueType,
    ) -> Self {
        let arguments = logical
            .iter()
            .chain(order)
            .cloned()
            .map(|value_type| FunctionArgument::Value {
                value_type,
                constant: None,
            })
            .collect::<Vec<_>>();
        Self {
            id: FunctionId::try_new("fixture/aggregate/exact-owner").unwrap(),
            selected: Arc::new(FunctionBindingSelection {
                overload: FunctionOverloadId::try_new("fixture/aggregate/exact-logical-signature")
                    .unwrap(),
                argument_types: novarocks_type_contract::FunctionArgumentTypes::from_pure_boxed(
                    arguments
                        .iter()
                        .map(FunctionArgument::argument_type)
                        .collect::<Box<[_]>>(),
                ),
                result_type: FunctionResultType::Scalar(i64_type(false)),
                aggregate: Some(AggregateBindingSelection {
                    state_argument_contract:
                        novarocks_type_contract::AggregateStateArgumentContract::ExactSignature,
                    intermediate_type: state,
                    state_format: AggregateStateFormatIdentity::try_new(
                        "fixture/aggregate/state-v1",
                    )
                    .unwrap(),
                }),
            }),
            argument_uses: (0..arguments.len())
                .map(|ordinal| Some(ExpressionUseId::new(ordinal as u32 + 1)))
                .collect(),
            arguments,
            logical: logical.len(),
            declaration: FunctionEffectDeclaration {
                value_stability: FunctionVolatility::Immutable,
                own_row_error: FunctionIntrinsicRowError::NotRowEvaluated,
                failure_behavior: FunctionFailureBehavior::Propagate,
                null_behavior: FunctionNullBehavior::CalledOnNull,
                argument_control: ArgumentControl::Aggregate,
                instance_state: FunctionInstanceState::AggregateInstance,
                observable_effects: ObservableEffects::NONE,
                environment_dependencies: Box::new([]),
            },
            parameters: SemanticParameters::default(),
        }
    }
    fn input(&self) -> CallEffectInput<'_> {
        CallEffectInput {
            context: ExpressionEffectContext {
                use_id: ExpressionUseId::new(0),
                region: EvaluationRegionId::new(9),
                demand: EvaluationDemand::Value,
            },
            argument_uses: crate::CallArgumentUses::SelectedChannels(&self.argument_uses),
            function_id: &self.id,
            kind: FunctionKind::Aggregate,
            selected: self.selected.as_ref(),
            request: FunctionBindingRequest {
                expected_result_type: None,
                arguments: &self.arguments,
                logical_argument_count: self.logical,
            },
            environment: &[],
            parameters: &self.parameters,
            decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
            proof_scope: CallProofScope::Unconditional,
        }
    }
    fn call(&self) -> Arc<FunctionCallContract> {
        let input = self.input();
        let receipt = refine_call_effects(self, input, &CompileControl(None)).unwrap();
        Arc::new(
            FunctionCallContract::from_refined(
                input,
                &receipt,
                self.selected.clone(),
                &CompileControl(None),
            )
            .unwrap(),
        )
    }
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
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)
            .map_err(FunctionEffectOwnerError::Control)?;
        if input.function_id != &self.id
            || input.kind != FunctionKind::Aggregate
            || input.selected != self.selected.as_ref()
            || input.request.logical_argument_count != self.logical
            || input.request.arguments.len() != self.arguments.len()
            || !input.environment.is_empty()
        {
            return Err(FunctionBindingError::InvalidBinding(
                "fixture exact aggregate call differs".into(),
            )
            .into());
        }
        for (argument, expected) in input.request.arguments.iter().zip(&self.arguments) {
            if !argument.equals_observed(expected, CompilePhase::FunctionSpecialization, control)? {
                return Err(FunctionBindingError::InvalidBinding(
                    "fixture aggregate argument differs".into(),
                )
                .into());
            }
            work.step().map_err(FunctionEffectOwnerError::Control)?;
        }
        work.finish().map_err(FunctionEffectOwnerError::Control)?;
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

fn i64_type(nullable: bool) -> FunctionValueType {
    FunctionValueType::new(DataType::Int64, nullable)
}
fn ordered_fixture() -> Fixture {
    Fixture::new(
        &[i64_type(true)],
        &[FunctionValueType::new(DataType::Int32, true)],
        i64_type(false),
    )
}
fn keys(count: usize) -> Arc<[AggregateOrderKey]> {
    vec![
        AggregateOrderKey {
            ascending: false,
            nulls_first: true
        };
        count
    ]
    .into()
}
fn aggregate(
    call: Arc<FunctionCallContract>,
    phase: AggregateKernelPhase,
) -> AggregateCallContract {
    let (order, state) = if phase.consumes_logical_arguments() {
        (
            keys(call.selected().argument_types.len() - call.logical_argument_count()),
            None,
        )
    } else {
        (
            keys(0),
            Some(
                call.selected()
                    .aggregate
                    .as_ref()
                    .unwrap()
                    .intermediate_type
                    .clone(),
            ),
        )
    };
    AggregateCallContract::try_new(call, phase, false, order, state, &CompileControl(None)).unwrap()
}
fn invalid_program<T: std::fmt::Debug>(result: Result<T, KernelFailure>) {
    assert!(
        matches!(result, Err(ref cause) if crate::kernel_control::test_contract::input(cause)),
        "{result:?}"
    );
}
fn selected<'a>(selection: Selection<'a>, values: ArrayRef) -> SelectedValues<'a> {
    SelectedValues::try_new(selection, values.data_type(), values.clone(), Box::new([])).unwrap()
}

#[test]
fn all_four_phases_preserve_one_exact_logical_binding_and_state_format() {
    let fixture = ordered_fixture();
    let call = fixture.call();
    for phase in [
        AggregateKernelPhase::Single,
        AggregateKernelPhase::Partial,
        AggregateKernelPhase::Intermediate,
        AggregateKernelPhase::Final,
    ] {
        let contract = aggregate(call.clone(), phase);
        assert!(Arc::ptr_eq(contract.call(), &call));
        assert!(std::ptr::eq(
            contract.call().selected(),
            fixture.selected.as_ref()
        ));
        assert_eq!(contract.phase(), phase);
        assert_eq!(
            contract
                .logical_argument_types()
                .cloned()
                .collect::<Vec<_>>(),
            [i64_type(true)]
        );
        assert_eq!(
            contract.order_argument_types().cloned().collect::<Vec<_>>(),
            [FunctionValueType::new(DataType::Int32, true)]
        );
        assert_eq!(contract.intermediate_type(), &i64_type(false));
        assert_eq!(contract.final_type(), &i64_type(false));
        assert_eq!(
            contract.state_format(),
            &fixture.selected.aggregate.as_ref().unwrap().state_format
        );
        assert_eq!(
            contract.order_keys().len(),
            usize::from(phase.consumes_logical_arguments())
        );
        assert_eq!(
            contract.state_input_type().is_some(),
            !phase.consumes_logical_arguments()
        );
        assert_eq!(
            phase.produces_final_result(),
            matches!(
                phase,
                AggregateKernelPhase::Single | AggregateKernelPhase::Final
            )
        );
    }
    let input = fixture.input();
    let receipt = refine_call_effects(&fixture, input, &CompileControl(None)).unwrap();
    // Equal contents do not replace the exact selected Arc's input borrow.
    invalid_program(FunctionCallContract::from_refined(
        input,
        &receipt,
        Arc::new((*fixture.selected).clone()),
        &CompileControl(None),
    ));
}

#[test]
fn zero_argument_count_shape_needs_only_selection_for_update() {
    let fixture = Fixture::new(&[], &[], i64_type(false));
    let call = fixture.call();
    assert_eq!(call.logical_argument_count(), 0);
    assert!(call.selected().argument_types.is_empty());
    let rows = [1, 4];
    for phase in [AggregateKernelPhase::Single, AggregateKernelPhase::Partial] {
        let contract = aggregate(call.clone(), phase);
        for selection in [Selection::try_sparse(6, &rows).unwrap(), Selection::all(0)] {
            let input = SelectedAggregateUpdateInput::try_new(
                &contract,
                selection,
                &[],
                &[],
                &EvaluationControl(None),
            )
            .unwrap();
            assert_eq!(input.selection(), selection);
            assert!(input.logical_arguments().is_empty());
            assert!(input.order_arguments().is_empty());
            assert!(std::ptr::eq(input.contract(), &contract));
        }
    }
}

#[test]
fn logical_and_order_carriers_share_exact_selection_but_stay_independent() {
    let call = ordered_fixture().call();
    let contract = aggregate(call, AggregateKernelPhase::Partial);
    let rows = [1, 4];
    let other_rows = [0, 4];
    let selection = Selection::try_sparse(6, &rows).unwrap();
    let logical = selected(selection, Arc::new(Int64Array::from(vec![11, 44])));
    let order = selected(selection, Arc::new(Int32Array::from(vec![4, 1])));
    let logical_args = [EvaluatedArgument::SelectedColumn(&logical)];
    let order_args = [EvaluatedArgument::SelectedColumn(&order)];
    let input = SelectedAggregateUpdateInput::try_new(
        &contract,
        selection,
        &logical_args,
        &order_args,
        &EvaluationControl(None),
    )
    .unwrap();
    assert_eq!(input.selection(), selection);
    assert!(std::ptr::eq(
        input.logical_arguments(),
        logical_args.as_slice()
    ));
    assert!(std::ptr::eq(input.order_arguments(), order_args.as_slice()));
    let other = Selection::try_sparse(6, &other_rows).unwrap();
    let wrong_logical = selected(other, Arc::new(Int64Array::from(vec![11, 44])));
    let wrong_order = selected(other, Arc::new(Int32Array::from(vec![4, 1])));
    invalid_program(SelectedAggregateUpdateInput::try_new(
        &contract,
        selection,
        &[EvaluatedArgument::SelectedColumn(&wrong_logical)],
        &order_args,
        &EvaluationControl(None),
    ));
    invalid_program(SelectedAggregateUpdateInput::try_new(
        &contract,
        selection,
        &logical_args,
        &[EvaluatedArgument::SelectedColumn(&wrong_order)],
        &EvaluationControl(None),
    ));
    invalid_program(SelectedAggregateUpdateInput::try_new(
        &contract,
        selection,
        &logical_args,
        &[],
        &EvaluationControl(None),
    ));
    invalid_program(SelectedAggregateUpdateInput::try_new(
        &contract,
        selection,
        &order_args,
        &logical_args,
        &EvaluationControl(None),
    ));
}

#[test]
fn update_and_merge_inputs_reject_opposite_phases() {
    let fixture = Fixture::new(&[i64_type(true)], &[], i64_type(false));
    let call = fixture.call();
    let array: ArrayRef = Arc::new(Int64Array::from(vec![1, 2]));
    let selection = Selection::all(2);
    let args = [EvaluatedArgument::Column(&array)];
    for phase in [AggregateKernelPhase::Single, AggregateKernelPhase::Partial] {
        let contract = aggregate(call.clone(), phase);
        SelectedAggregateUpdateInput::try_new(
            &contract,
            selection,
            &args,
            &[],
            &EvaluationControl(None),
        )
        .unwrap();
        invalid_program(SelectedAggregateMergeInput::try_new(
            &contract,
            selection,
            args[0],
            &EvaluationControl(None),
        ));
    }
    for phase in [
        AggregateKernelPhase::Intermediate,
        AggregateKernelPhase::Final,
    ] {
        let contract = aggregate(call.clone(), phase);
        let input = SelectedAggregateMergeInput::try_new(
            &contract,
            selection,
            args[0],
            &EvaluationControl(None),
        )
        .unwrap();
        assert_eq!(input.selection(), selection);
        assert!(std::ptr::eq(input.contract(), &contract));
        assert!(std::ptr::eq(input.state().array(), &array));
        invalid_program(SelectedAggregateUpdateInput::try_new(
            &contract,
            selection,
            &args,
            &[],
            &EvaluationControl(None),
        ));
    }
}

#[test]
fn merge_nullable_widening_is_allowed_but_reverse_and_domain_changes_are_rejected() {
    for phase in [
        AggregateKernelPhase::Intermediate,
        AggregateKernelPhase::Final,
    ] {
        for (emitted, input, allowed) in [
            (false, true, true),
            (false, false, true),
            (true, true, true),
            (true, false, false),
        ] {
            let call = Fixture::new(&[i64_type(true)], &[], i64_type(emitted)).call();
            let result = AggregateCallContract::try_new(
                call,
                phase,
                false,
                keys(0),
                Some(i64_type(input)),
                &CompileControl(None),
            );
            if allowed {
                assert_eq!(result.unwrap().state_input_type(), Some(&i64_type(input)));
            } else {
                invalid_program(result);
            }
        }
        let largeint = FunctionValueType::try_with_logical_type(
            DataType::FixedSizeBinary(16),
            false,
            ValueLogicalType::LargeInt,
        )
        .unwrap();
        let physical = FunctionValueType::new(DataType::FixedSizeBinary(16), true);
        for (emission, input) in [(largeint.clone(), physical.clone()), (physical, largeint)] {
            let call = Fixture::new(&[], &[], emission).call();
            invalid_program(AggregateCallContract::try_new(
                call,
                phase,
                false,
                keys(0),
                Some(input),
                &CompileControl(None),
            ));
        }
    }
}

#[test]
fn merge_nested_semantic_domain_ignores_annotations_but_preserves_input_carrier_identity() {
    fn state(annotation: &str, nullable: bool, logical: ValueLogicalType) -> FunctionValueType {
        let mut metadata = std::collections::HashMap::from([(
            "fixture.annotation".to_owned(),
            annotation.to_owned(),
        )]);
        if let Some(value) = logical.metadata_value() {
            metadata.insert(
                novarocks_type_contract::NR_LOGICAL_TYPE_KEY.into(),
                value.into(),
            );
        }
        let payload =
            arrow_schema::Field::new("payload", DataType::Utf8, nullable).with_metadata(metadata);
        let nested =
            arrow_schema::Field::new("nested", DataType::Struct(vec![payload].into()), false);
        FunctionValueType::from_legacy_declared_projection(
            DataType::Struct(vec![nested].into()),
            false,
        )
    }
    fn array(layout: &FunctionValueType) -> ArrayRef {
        let DataType::Struct(outer) = layout.data_type() else {
            unreachable!();
        };
        let DataType::Struct(inner) = outer[0].data_type() else {
            unreachable!();
        };
        let payload: ArrayRef = Arc::new(arrow_array::StringArray::from(vec!["one", "two"]));
        let nested: ArrayRef = Arc::new(arrow_array::StructArray::new(
            inner.clone(),
            vec![payload],
            None,
        ));
        Arc::new(arrow_array::StructArray::new(
            outer.clone(),
            vec![nested],
            None,
        ))
    }
    let emitted = state("emission annotation", true, ValueLogicalType::Json);
    let input = state("input annotation", true, ValueLogicalType::Json);
    assert!(emitted.same_value_domain(&input));
    assert!(novarocks_type_contract::arrow_data_types_exact(
        emitted.data_type(),
        input.data_type()
    ));
    let call = Fixture::new(&[], &[], emitted.clone()).call();
    let input_array = array(&input);
    let emitted_array = array(&emitted);
    for phase in [
        AggregateKernelPhase::Intermediate,
        AggregateKernelPhase::Final,
    ] {
        let contract = AggregateCallContract::try_new(
            call.clone(),
            phase,
            false,
            keys(0),
            Some(input.clone()),
            &CompileControl(None),
        )
        .unwrap();
        assert_eq!(contract.state_input_type(), Some(&input));
        SelectedAggregateMergeInput::try_new(
            &contract,
            Selection::all(2),
            EvaluatedArgument::Column(&input_array),
            &EvaluationControl(None),
        )
        .unwrap();
        // External annotations have been removed before the value port.
        SelectedAggregateMergeInput::try_new(
            &contract,
            Selection::all(2),
            EvaluatedArgument::Column(&emitted_array),
            &EvaluationControl(None),
        )
        .unwrap();
        for (emission, incompatible_input) in [
            (
                state("same", false, ValueLogicalType::Json),
                state("same", true, ValueLogicalType::Json),
            ),
            (
                state("same", true, ValueLogicalType::Json),
                state("same", false, ValueLogicalType::Json),
            ),
            (
                state("same", true, ValueLogicalType::Json),
                state("same", true, ValueLogicalType::Physical),
            ),
            (
                state("same", true, ValueLogicalType::Physical),
                state("same", true, ValueLogicalType::Json),
            ),
        ] {
            assert!(!emission.same_value_domain(&incompatible_input));
            invalid_program(AggregateCallContract::try_new(
                Fixture::new(&[], &[], emission).call(),
                phase,
                false,
                keys(0),
                Some(incompatible_input),
                &CompileControl(None),
            ));
        }
    }
}

#[test]
fn phase_contract_rejects_repeated_distinct_order_and_wrong_state_channels() {
    let call = ordered_fixture().call();
    for phase in [AggregateKernelPhase::Single, AggregateKernelPhase::Partial] {
        let legal = AggregateCallContract::try_new(
            call.clone(),
            phase,
            true,
            keys(1),
            None,
            &CompileControl(None),
        )
        .unwrap();
        assert!(legal.distinct());
        assert_eq!(legal.order_keys(), keys(1).as_ref());
        invalid_program(AggregateCallContract::try_new(
            call.clone(),
            phase,
            false,
            keys(0),
            None,
            &CompileControl(None),
        ));
        invalid_program(AggregateCallContract::try_new(
            call.clone(),
            phase,
            false,
            keys(1),
            Some(i64_type(false)),
            &CompileControl(None),
        ));
    }
    for phase in [
        AggregateKernelPhase::Intermediate,
        AggregateKernelPhase::Final,
    ] {
        invalid_program(AggregateCallContract::try_new(
            call.clone(),
            phase,
            true,
            keys(0),
            Some(i64_type(false)),
            &CompileControl(None),
        ));
        invalid_program(AggregateCallContract::try_new(
            call.clone(),
            phase,
            false,
            keys(1),
            Some(i64_type(false)),
            &CompileControl(None),
        ));
        invalid_program(AggregateCallContract::try_new(
            call.clone(),
            phase,
            false,
            keys(0),
            None,
            &CompileControl(None),
        ));
    }
}

#[test]
fn row_errors_wrong_selection_types_lengths_and_nonnull_state_are_rejected() {
    let fixture = Fixture::new(&[i64_type(true)], &[], i64_type(false));
    let call = fixture.call();
    let rows = [1, 4];
    let wrong_rows = [0, 4];
    let selection = Selection::try_sparse(6, &rows).unwrap();
    let error_values: ArrayRef = Arc::new(Int64Array::from(vec![None, Some(4)]));
    let errored = SelectedValues::try_new(
        selection,
        error_values.data_type(),
        error_values.clone(),
        vec![RowDataError::new(0, "fixture child row error")].into_boxed_slice(),
    )
    .unwrap();
    let wrong = selected(
        Selection::try_sparse(6, &wrong_rows).unwrap(),
        Arc::new(Int64Array::from(vec![1, 4])),
    );
    let wrong_type: ArrayRef = Arc::new(Int32Array::from(vec![1; 6]));
    let short: ArrayRef = Arc::new(Int64Array::from(vec![1]));
    let args = [
        EvaluatedArgument::SelectedColumn(&errored),
        EvaluatedArgument::SelectedColumn(&wrong),
        EvaluatedArgument::Column(&wrong_type),
        EvaluatedArgument::Column(&short),
    ];
    for phase in [AggregateKernelPhase::Single, AggregateKernelPhase::Partial] {
        let contract = aggregate(call.clone(), phase);
        for argument in args {
            invalid_program(SelectedAggregateUpdateInput::try_new(
                &contract,
                selection,
                &[argument],
                &[],
                &EvaluationControl(None),
            ));
        }
        SelectedAggregateUpdateInput::try_new(
            &contract,
            selection,
            &[EvaluatedArgument::Scalar(&short)],
            &[],
            &EvaluationControl(None),
        )
        .unwrap();
    }
    let nulls: ArrayRef = Arc::new(Int64Array::from(vec![
        Some(0),
        None,
        Some(2),
        Some(3),
        Some(4),
        Some(5),
    ]));
    for phase in [
        AggregateKernelPhase::Intermediate,
        AggregateKernelPhase::Final,
    ] {
        let contract = aggregate(call.clone(), phase);
        for argument in args {
            invalid_program(SelectedAggregateMergeInput::try_new(
                &contract,
                selection,
                argument,
                &EvaluationControl(None),
            ));
        }
        invalid_program(SelectedAggregateMergeInput::try_new(
            &contract,
            selection,
            EvaluatedArgument::Column(&nulls),
            &EvaluationControl(None),
        ));
        let widened = AggregateCallContract::try_new(
            call.clone(),
            phase,
            false,
            keys(0),
            Some(i64_type(true)),
            &CompileControl(None),
        )
        .unwrap();
        SelectedAggregateMergeInput::try_new(
            &widened,
            selection,
            EvaluatedArgument::Column(&nulls),
            &EvaluationControl(None),
        )
        .unwrap();
    }
}

#[test]
fn outer_control_failures_propagate_from_phase_preparation_update_and_merge() {
    let call = Fixture::new(&[i64_type(true)], &[], i64_type(false)).call();
    let update = aggregate(call.clone(), AggregateKernelPhase::Partial);
    let merge = aggregate(call.clone(), AggregateKernelPhase::Final);
    let array: ArrayRef = Arc::new(Int64Array::from(vec![1, 2]));
    let arguments = [EvaluatedArgument::Column(&array)];
    for (compile_error, kernel_error) in [
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
        for phase in [
            AggregateKernelPhase::Single,
            AggregateKernelPhase::Partial,
            AggregateKernelPhase::Intermediate,
            AggregateKernelPhase::Final,
        ] {
            let state = (!phase.consumes_logical_arguments()).then(|| i64_type(false));
            assert_eq!(
                AggregateCallContract::try_new(
                    call.clone(),
                    phase,
                    false,
                    keys(0),
                    state,
                    &CompileControl(Some(compile_error.clone()))
                )
                .unwrap_err(),
                KernelFailure::CompileControl(compile_error.clone())
            );
        }
        let control = EvaluationControl(Some(kernel_error.clone()));
        assert_eq!(
            SelectedAggregateUpdateInput::try_new(
                &update,
                Selection::all(2),
                &arguments,
                &[],
                &control
            )
            .unwrap_err(),
            kernel_error
        );
        assert_eq!(
            SelectedAggregateMergeInput::try_new(&merge, Selection::all(2), arguments[0], &control)
                .unwrap_err(),
            kernel_error
        );
    }
}

struct PositiveEvaluationControl {
    failure: KernelFailure,
    work: std::sync::Mutex<Vec<u32>>,
}
impl PositiveEvaluationControl {
    fn new(failure: KernelFailure) -> Self {
        Self {
            failure,
            work: std::sync::Mutex::default(),
        }
    }
    fn assert_first_work_failure(&self) {
        let work = self.work.lock().unwrap();
        assert!(work.len() >= 2);
        assert!(work[..work.len() - 1].iter().all(|units| *units == 0));
        assert_eq!(work.last(), Some(&crate::MAX_UNOBSERVED_KERNEL_WORK));
    }
}
impl KernelEvaluationControl for PositiveEvaluationControl {
    fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
        assert!(units <= crate::MAX_UNOBSERVED_KERNEL_WORK);
        self.work.lock().unwrap().push(units);
        if units > 0 {
            Err(self.failure.clone())
        } else {
            Ok(())
        }
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("aggregate input validation must not wait")
    }
}

#[test]
fn independent_equal_sparse_selection_comparison_is_interruptible_mid_work() {
    let rows = (0..300).map(|row| row * 2).collect::<Vec<_>>();
    let invocation_rows = rows.clone();
    assert_ne!(rows.as_ptr(), invocation_rows.as_ptr());
    let selected_rows = Selection::try_sparse(600, &rows).unwrap();
    let invoked_rows = Selection::try_sparse(600, &invocation_rows).unwrap();
    assert_eq!(selected_rows, invoked_rows);
    let compact = selected(
        selected_rows,
        Arc::new(Int64Array::from(vec![1; rows.len()])),
    );
    let argument = EvaluatedArgument::SelectedColumn(&compact);
    let args = [argument];
    let call = Fixture::new(&[i64_type(true)], &[], i64_type(true)).call();
    let update = aggregate(call.clone(), AggregateKernelPhase::Partial);
    let merge = aggregate(call, AggregateKernelPhase::Final);
    SelectedAggregateUpdateInput::try_new(
        &update,
        invoked_rows,
        &args,
        &[],
        &EvaluationControl(None),
    )
    .unwrap();
    SelectedAggregateMergeInput::try_new(&merge, invoked_rows, argument, &EvaluationControl(None))
        .unwrap();
    for failure in [
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
    ] {
        let control = PositiveEvaluationControl::new(failure.clone());
        assert_eq!(
            SelectedAggregateUpdateInput::try_new(&update, invoked_rows, &args, &[], &control)
                .unwrap_err(),
            failure
        );
        control.assert_first_work_failure();
        let control = PositiveEvaluationControl::new(failure.clone());
        assert_eq!(
            SelectedAggregateMergeInput::try_new(&merge, invoked_rows, argument, &control)
                .unwrap_err(),
            failure
        );
        control.assert_first_work_failure();
    }
}

#[test]
fn nonnullable_selected_update_and_merge_rows_are_interruptible_mid_work() {
    let rows = (0..300).map(|row| row * 2).collect::<Vec<_>>();
    let selection = Selection::try_sparse(600, &rows).unwrap();
    let array: ArrayRef = Arc::new(Int64Array::from(vec![1; 600]));
    let argument = EvaluatedArgument::Column(&array);
    let args = [argument];
    let call = Fixture::new(&[i64_type(false)], &[], i64_type(false)).call();
    let update = aggregate(call.clone(), AggregateKernelPhase::Single);
    let merge = aggregate(call, AggregateKernelPhase::Intermediate);
    SelectedAggregateUpdateInput::try_new(&update, selection, &args, &[], &EvaluationControl(None))
        .unwrap();
    SelectedAggregateMergeInput::try_new(&merge, selection, argument, &EvaluationControl(None))
        .unwrap();
    for failure in [
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
    ] {
        let control = PositiveEvaluationControl::new(failure.clone());
        assert_eq!(
            SelectedAggregateUpdateInput::try_new(&update, selection, &args, &[], &control)
                .unwrap_err(),
            failure
        );
        control.assert_first_work_failure();
        let control = PositiveEvaluationControl::new(failure.clone());
        assert_eq!(
            SelectedAggregateMergeInput::try_new(&merge, selection, argument, &control)
                .unwrap_err(),
            failure
        );
        control.assert_first_work_failure();
    }
}

struct ThresholdCompileControl {
    failure: CompileControlError,
    fail_at: u32,
    work: std::sync::Mutex<Vec<u32>>,
}
impl PureCompileControl for ThresholdCompileControl {
    fn checkpoint(&self, _: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert!(units <= novarocks_type_contract::MAX_UNOBSERVED_COMPILE_WORK);
        let mut work = self.work.lock().unwrap();
        work.push(units);
        if work.iter().sum::<u32>() >= self.fail_at {
            Err(self.failure.clone())
        } else {
            Ok(())
        }
    }
}

#[test]
fn wide_merge_semantic_comparison_preserves_mid_work_control_failures() {
    let state = FunctionValueType::from_legacy_declared_projection(
        DataType::Struct(
            (0..300)
                .map(|ordinal| {
                    arrow_schema::Field::new(format!("field_{ordinal}"), DataType::Int64, false)
                })
                .collect::<Vec<_>>()
                .into(),
        ),
        false,
    );
    let mut validation_units = 0u32;
    novarocks_type_contract::validate_value_type_structure_observed::<KernelFailure>(
        state.data_type(),
        |_| {
            validation_units += 1;
            Ok(())
        },
    )
    .unwrap();
    let quantum = novarocks_type_contract::MAX_UNOBSERVED_COMPILE_WORK;
    // With no logical channels or field metadata, this first failing quantum
    // is strictly after the input visitor and inside the shared domain check.
    let fail_at = (validation_units / quantum + 1) * quantum;
    let call = Fixture::new(&[], &[], state.clone()).call();
    for phase in [
        AggregateKernelPhase::Intermediate,
        AggregateKernelPhase::Final,
    ] {
        AggregateCallContract::try_new(
            call.clone(),
            phase,
            false,
            keys(0),
            Some(state.clone()),
            &CompileControl(None),
        )
        .unwrap();
        for failure in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let control = ThresholdCompileControl {
                failure,
                fail_at,
                work: std::sync::Mutex::default(),
            };
            assert_eq!(
                AggregateCallContract::try_new(
                    call.clone(),
                    phase,
                    false,
                    keys(0),
                    Some(state.clone()),
                    &control
                )
                .unwrap_err(),
                KernelFailure::CompileControl(control.failure.clone())
            );
            let work = control.work.lock().unwrap();
            assert_eq!(work.first(), Some(&0));
            assert_eq!(work.iter().sum::<u32>(), fail_at);
            assert!(work.iter().all(|units| *units <= quantum));
        }
    }
}

#[test]
fn call_preparation_bounds_typed_names_and_timezones_at_checked_author() {
    use novarocks_type_contract::{
        MAX_ARROW_FIELD_NAME_BYTES, MAX_ARROW_TIMESTAMP_TIMEZONE_BYTES, ValueLogicalType,
    };
    for (boundary, oversized) in [
        (
            DataType::Struct(
                vec![arrow_schema::Field::new(
                    "n".repeat(MAX_ARROW_FIELD_NAME_BYTES),
                    DataType::Int64,
                    false,
                )]
                .into(),
            ),
            DataType::Struct(
                vec![arrow_schema::Field::new(
                    "n".repeat(MAX_ARROW_FIELD_NAME_BYTES + 1),
                    DataType::Int64,
                    false,
                )]
                .into(),
            ),
        ),
        (
            DataType::Timestamp(
                arrow_schema::TimeUnit::Nanosecond,
                Some("z".repeat(MAX_ARROW_TIMESTAMP_TIMEZONE_BYTES).into()),
            ),
            DataType::Timestamp(
                arrow_schema::TimeUnit::Nanosecond,
                Some("z".repeat(MAX_ARROW_TIMESTAMP_TIMEZONE_BYTES + 1).into()),
            ),
        ),
    ] {
        let state =
            FunctionValueType::try_with_logical_type(boundary, false, ValueLogicalType::Physical)
                .unwrap();
        let fixture = Fixture::new(&[], &[], state);
        let input = fixture.input();
        let receipt = refine_call_effects(&fixture, input, &CompileControl(None)).unwrap();
        FunctionCallContract::from_refined(
            input,
            &receipt,
            fixture.selected.clone(),
            &CompileControl(None),
        )
        .unwrap();
        assert!(
            FunctionValueType::try_with_logical_type(oversized, false, ValueLogicalType::Physical)
                .is_err()
        );
    }
}
