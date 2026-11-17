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
    FunctionArgument, FunctionBindingError, FunctionBindingRequest, FunctionEffectOwner,
    FunctionEffectOwnerError, FunctionId, FunctionOverloadId, RowDataError, refine_call_effects,
};
use arrow_array::{Array, ArrayRef, BooleanArray, Int32Array, Int64Array};
use arrow_schema::DataType;
use novarocks_type_contract::{
    CallEffects, CallProofScope, CompileControlError, DecimalOverflowPolicy, EvaluationRegionId,
    ExpressionEffects, FunctionEffectDeclaration, FunctionFailureBehavior, FunctionInstanceState,
    FunctionIntrinsicRowError, FunctionNullBehavior, FunctionVolatility, ObservableEffects,
    SemanticParameters,
};
use std::{
    sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

#[derive(Default)]
struct CompileControl {
    failure: Option<CompileControlError>,
    positive_only: bool,
    work: Mutex<Vec<u32>>,
}
impl PureCompileControl for CompileControl {
    fn checkpoint(&self, phase: CompilePhase, work: u32) -> Result<(), CompileControlError> {
        assert_eq!(phase, CompilePhase::FunctionSpecialization);
        assert!(work <= 256);
        self.work.lock().unwrap().push(work);
        if !self.positive_only || work > 0 {
            self.failure.clone().map_or(Ok(()), Err)
        } else {
            Ok(())
        }
    }
}
#[derive(Default)]
struct RuntimeControl {
    failure: Option<KernelFailure>,
    positive_only: bool,
    work: Mutex<Vec<u32>>,
}
impl KernelEvaluationControl for RuntimeControl {
    fn checkpoint(&self, work: u32) -> Result<(), KernelFailure> {
        assert!(work <= 256);
        self.work.lock().unwrap().push(work);
        if !self.positive_only || work > 0 {
            self.failure.clone().map_or(Ok(()), Err)
        } else {
            Ok(())
        }
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("fixture does not wait")
    }
}
struct Owner {
    id: FunctionId,
    selected: Arc<FunctionBindingSelection>,
    arguments: Vec<FunctionArgument>,
    uses: Vec<Option<ExpressionUseId>>,
    parameters: SemanticParameters,
    declaration: FunctionEffectDeclaration,
    refines: AtomicUsize,
}
fn i64_type(nullable: bool) -> FunctionValueType {
    FunctionValueType::new(DataType::Int64, nullable)
}
fn i32_type(nullable: bool) -> FunctionValueType {
    FunctionValueType::new(DataType::Int32, nullable)
}
impl Owner {
    fn new(demand: EvaluationDemand) -> Self {
        let result = if demand == EvaluationDemand::TruthOnly {
            FunctionValueType::new(DataType::Boolean, false)
        } else {
            i64_type(false)
        };
        let arguments = vec![
            FunctionArgument::Value {
                value_type: i64_type(false),
                constant: None,
            },
            FunctionArgument::Lambda {
                parameter_types: novarocks_type_contract::FunctionParameterTypes::from_pure_boxed(
                    Box::from([i64_type(false), i32_type(false)]),
                ),
                result_type: result,
            },
        ];
        Self {
            id: FunctionId::try_new("fixture/lambda-body/exact-owner").unwrap(),
            selected: Arc::new(FunctionBindingSelection {
                overload: FunctionOverloadId::try_new("fixture/lambda-body/exact-signature")
                    .unwrap(),
                argument_types: novarocks_type_contract::FunctionArgumentTypes::from_pure_boxed(
                    arguments
                        .iter()
                        .map(FunctionArgument::argument_type)
                        .collect::<Vec<_>>()
                        .into_boxed_slice(),
                ),
                result_type: FunctionResultType::Scalar(i64_type(false)),
                aggregate: None,
            }),
            uses: vec![Some(ExpressionUseId::new(1)), Some(ExpressionUseId::new(2))],
            arguments,
            parameters: SemanticParameters::default(),
            declaration: FunctionEffectDeclaration {
                value_stability: FunctionVolatility::Immutable,
                own_row_error: FunctionIntrinsicRowError::NoRowError,
                failure_behavior: FunctionFailureBehavior::Propagate,
                null_behavior: FunctionNullBehavior::CalledOnNull,
                argument_control: ArgumentControl::HigherOrder {
                    body_ordinal: 1,
                    body_demand: demand,
                },
                instance_state: FunctionInstanceState::None,
                observable_effects: ObservableEffects::NONE,
                environment_dependencies: Box::new([]),
            },
            refines: AtomicUsize::new(0),
        }
    }
    fn input(&self) -> CallEffectInput<'_> {
        CallEffectInput {
            context: ExpressionEffectContext {
                use_id: ExpressionUseId::new(0),
                region: EvaluationRegionId::new(7),
                demand: EvaluationDemand::Value,
            },
            argument_uses: crate::CallArgumentUses::SelectedChannels(&self.uses),
            function_id: &self.id,
            kind: FunctionKind::Scalar,
            selected: &self.selected,
            request: FunctionBindingRequest {
                expected_result_type: None,
                arguments: &self.arguments,
                logical_argument_count: self.arguments.len(),
            },
            environment: &[],
            parameters: &self.parameters,
            decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
            proof_scope: CallProofScope::Region(EvaluationRegionId::new(7)),
        }
    }
    fn call(&self) -> Arc<HigherOrderCallContract> {
        let input = self.input();
        let receipt = refine_call_effects(self, input, &CompileControl::default()).unwrap();
        Arc::new(
            HigherOrderCallContract::from_refined(
                input,
                &receipt,
                self.selected.clone(),
                &CompileControl::default(),
            )
            .unwrap(),
        )
    }
}
impl FunctionEffectOwner for Owner {
    type Error = FunctionBindingError;
    fn declaration(
        &self,
        id: &FunctionId,
        selected: &FunctionBindingSelection,
    ) -> Result<&FunctionEffectDeclaration, Self::Error> {
        if id != &self.id || !std::ptr::eq(selected, self.selected.as_ref()) {
            return Err(FunctionBindingError::UnknownFunction);
        }
        Ok(&self.declaration)
    }
    fn validate_and_refine(
        &self,
        input: CallEffectInput<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<CallEffects, FunctionEffectOwnerError<Self::Error>> {
        self.refines.fetch_add(1, Ordering::Relaxed);
        if input.function_id != &self.id
            || input.kind != FunctionKind::Scalar
            || !std::ptr::eq(input.selected, self.selected.as_ref())
            || !crate::binding::arguments_equal_for_test(
                input.request.arguments,
                &self.arguments,
                control,
            )?
            || input.request.logical_argument_count != self.arguments.len()
            || !input.environment.is_empty()
            || !std::ptr::eq(input.parameters, &self.parameters)
        {
            return Err(FunctionBindingError::UnknownFunction.into());
        }
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)
            .map_err(FunctionEffectOwnerError::Control)?;
        for _ in input.request.arguments {
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
fn context(call: &HigherOrderCallContract) -> ExpressionEffectContext {
    ExpressionEffectContext {
        use_id: ExpressionUseId::new(10),
        region: EvaluationRegionId::new(11),
        demand: call.body_demand(),
    }
}
fn checked_body(call: Arc<HigherOrderCallContract>, may_raise: bool) -> Arc<LambdaBodyContract> {
    let context = context(&call);
    let effects = ScopedExpressionEffects::primitive(
        context,
        ExpressionEffects {
            may_raise_invocation_data: false,
            value_stability: FunctionVolatility::Stable,
            may_raise_row_error: may_raise,
            has_instance_state: true,
            observable_effects: ObservableEffects::NONE,
        },
    );
    Arc::new(
        LambdaBodyContract::try_new(
            call,
            context,
            effects,
            Box::from([i32_type(false), i64_type(false)]),
            &CompileControl::default(),
        )
        .unwrap(),
    )
}
#[derive(Default)]
enum Mode {
    #[default]
    Good,
    ForeignSelection,
    WrongType,
    SuccessNull,
    Drift(Option<KernelFailure>),
    Failure(KernelFailure),
}
type Sample = (usize, usize, Vec<i64>, Vec<i64>);

struct Evaluator {
    contract: Arc<LambdaBodyContract>,
    mode: Mode,
    calls: usize,
    samples: Vec<Sample>,
}
impl Evaluator {
    fn new(contract: Arc<LambdaBodyContract>) -> Self {
        Self {
            contract,
            mode: Mode::Good,
            calls: 0,
            samples: vec![],
        }
    }
}
fn number(argument: EvaluatedArgument<'_>, ordinal: usize, row: usize) -> i64 {
    let index = argument.value_row(ordinal, row);
    match argument.array().data_type() {
        DataType::Int64 => argument
            .array()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(index),
        DataType::Int32 => i64::from(
            argument
                .array()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(index),
        ),
        other => panic!("unexpected fixture carrier {other:?}"),
    }
}
impl BoundLambdaBodyEvaluator for Evaluator {
    fn contract(&self) -> &Arc<LambdaBodyContract> {
        &self.contract
    }
    fn evaluate<'a>(
        &mut self,
        input: LambdaBodyInput<'_, 'a>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<SelectedValues<'a>, KernelFailure> {
        self.calls += 1;
        control.checkpoint(0)?;
        if let Mode::Drift(error) = &self.mode {
            self.contract = Arc::new((*self.contract).clone());
            if let Some(error) = error {
                return Err(error.clone());
            }
        }
        if let Mode::Failure(error) = &self.mode {
            return Err(error.clone());
        }
        let mut values = Vec::with_capacity(input.selection().len());
        let mut errors = Vec::new();
        let mut work = EvaluationCheckpoints::new(control);
        for ordinal in 0..input.selection().len() {
            let row = input.selection().row(ordinal).unwrap();
            let parameters = input
                .parameters()
                .iter()
                .map(|argument| number(*argument, ordinal, row))
                .collect::<Vec<_>>();
            let captures = input
                .captures()
                .iter()
                .map(|argument| number(*argument, ordinal, row))
                .collect::<Vec<_>>();
            self.samples.push((
                row,
                input.row_map().parent_row(row).unwrap(),
                parameters.clone(),
                captures.clone(),
            ));
            if parameters[0] < 0 {
                values.push(None);
                errors.push(RowDataError::new(ordinal, "negative lambda element"));
            } else {
                values.push(Some(parameters.iter().chain(&captures).sum::<i64>()));
            }
            work.step()?;
        }
        work.finish()?;
        let selection = if matches!(self.mode, Mode::ForeignSelection) {
            Selection::try_sparse(input.selection().batch_rows(), &[0, 4]).unwrap()
        } else {
            input.selection()
        };
        let array: ArrayRef = if matches!(self.mode, Mode::WrongType) {
            Arc::new(Int32Array::from(vec![0; selection.len()]))
        } else if matches!(self.mode, Mode::SuccessNull) {
            Arc::new(Int64Array::from(vec![None; selection.len()]))
        } else if self.contract.result_type().data_type() == &DataType::Boolean {
            Arc::new(BooleanArray::from(
                values
                    .iter()
                    .map(|value| value.map(|value| value > 0))
                    .collect::<Vec<_>>(),
            ))
        } else {
            Arc::new(Int64Array::from(values))
        };
        SelectedValues::try_new(
            selection,
            array.data_type(),
            array.clone(),
            errors.into_boxed_slice(),
        )
        .map_err(|_| internal("fixture malformed selected output"))
    }
}
fn carriers() -> [ArrayRef; 4] {
    [
        Arc::new(Int64Array::from(vec![99, -2, 77, 4, 88])),
        Arc::new(Int32Array::from(vec![0, 10, 0, 20, 0])),
        Arc::new(Int32Array::from(vec![0, 100, 0, 200, 0])),
        Arc::new(Int64Array::from(vec![7])),
    ]
}
fn operational() -> KernelFailure {
    KernelFailure::Operational(crate::KernelDiagnostic::new("fixture outer failure"))
}

#[test]
fn parent_no_row_error_keeps_complete_child_element_errors_and_exact_sparse_parameter_capture_mapping()
 {
    let owner = Owner::new(EvaluationDemand::Value);
    let call = owner.call();
    assert_eq!(
        call.call().effects().own_row_error,
        FunctionIntrinsicRowError::NoRowError
    );
    assert_eq!(call.body_ordinal(), 1);
    assert_eq!(call.body_edge_use(), ExpressionUseId::new(2));
    let body = checked_body(call, true);
    assert!(
        body.effects()
            .for_use(body.context())
            .unwrap()
            .may_raise_row_error
    );
    let columns = carriers();
    let parameters = [
        EvaluatedArgument::Column(&columns[0]),
        EvaluatedArgument::Column(&columns[1]),
    ];
    let captures = [
        EvaluatedArgument::Column(&columns[2]),
        EvaluatedArgument::Scalar(&columns[3]),
    ];
    let outer_rows = [2, 6];
    let parents = [0, 0, 1, 1, 1];
    let rows = [1, 3];
    let runtime = RuntimeControl::default();
    let row_map = LambdaElementRowMap::try_new(
        Selection::try_sparse(8, &outer_rows).unwrap(),
        &parents,
        &runtime,
    )
    .unwrap();
    let selection = Selection::try_sparse(5, &rows).unwrap();
    let input =
        LambdaBodyInput::try_new(&body, row_map, selection, &parameters, &captures, &runtime)
            .unwrap();
    let mut evaluator = Evaluator::new(body.clone());
    let mut invocation = LambdaBodyInvocation::try_bind(&mut evaluator, input).unwrap();
    let output = invocation.run(&runtime).unwrap();
    assert_eq!(output.values().selection(), selection);
    assert_eq!(output.values().errors()[0].selected_ordinal(), 0);
    assert_eq!(
        output
            .values()
            .values()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(1),
        231
    );
    assert!(output.values().values().is_null(0));
    let required = output.required_parent_error(0).unwrap().unwrap();
    assert!(Arc::ptr_eq(required.contract(), &body));
    assert!(Arc::ptr_eq(output.contract(), &body));
    assert_eq!(required.error().selected_ordinal(), 0);
    assert_eq!(required.original_parent_row(), Some(2));
    assert_eq!(required.error().message(), "negative lambda element");
    assert_eq!(required.outer_selection(), row_map.outer_selection());
    assert!(output.required_parent_error(1).unwrap().is_none());
    assert_eq!(
        output.row_map().selected_parent_row(selection, 0).unwrap(),
        Some(2)
    );
    assert_eq!(
        output.row_map().selected_parent_row(selection, 1).unwrap(),
        Some(6)
    );
    assert_eq!(
        invocation.run(&runtime).unwrap_err(),
        KernelFailure::InstanceFailed
    );
    assert_eq!(evaluator.calls, 1);
    assert_eq!(
        evaluator.samples,
        [
            (1, 2, vec![-2, 10], vec![100, 7]),
            (3, 6, vec![4, 20], vec![200, 7])
        ]
    );
    assert_eq!(owner.refines.load(Ordering::Relaxed), 1);
}

fn body_input<'a>(
    body: &'a LambdaBodyContract,
    selection: Selection<'a>,
    parents: &'a [usize],
    parameters: &'a [EvaluatedArgument<'a>],
    captures: &'a [EvaluatedArgument<'a>],
) -> LambdaBodyInput<'a, 'a> {
    let map = LambdaElementRowMap::try_new(Selection::all(2), parents, &RuntimeControl::default())
        .unwrap();
    LambdaBodyInput::try_new(
        body,
        map,
        selection,
        parameters,
        captures,
        &RuntimeControl::default(),
    )
    .unwrap()
}
#[test]
fn never_failing_body_error_and_successful_nonnull_null_are_internal_but_error_null_is_legal() {
    let columns = carriers();
    let parameters = [
        EvaluatedArgument::Column(&columns[0]),
        EvaluatedArgument::Column(&columns[1]),
    ];
    let captures = [
        EvaluatedArgument::Column(&columns[2]),
        EvaluatedArgument::Scalar(&columns[3]),
    ];
    let parents = [0, 0, 1, 1, 1];
    let rows = [1, 3];
    let selection = Selection::try_sparse(5, &rows).unwrap();
    let runtime = RuntimeControl::default();
    for (may_raise, mode) in [
        (false, Mode::Good),
        (true, Mode::SuccessNull),
        (true, Mode::WrongType),
        (true, Mode::ForeignSelection),
    ] {
        let owner = Owner::new(EvaluationDemand::Value);
        let body = checked_body(owner.call(), may_raise);
        let input = body_input(&body, selection, &parents, &parameters, &captures);
        let mut evaluator = Evaluator::new(body.clone());
        evaluator.mode = mode;
        let mut invocation = LambdaBodyInvocation::try_bind(&mut evaluator, input).unwrap();
        assert!(matches!(
            invocation.run(&runtime),
            Err(KernelFailure::Internal(_))
        ));
        assert_eq!(
            invocation.run(&runtime).unwrap_err(),
            KernelFailure::InstanceFailed
        );
        assert_eq!(evaluator.calls, 1);
    }
    let owner = Owner::new(EvaluationDemand::Value);
    let body = checked_body(owner.call(), true);
    let input = body_input(&body, selection, &parents, &parameters, &captures);
    let mut evaluator = Evaluator::new(body.clone());
    let output = LambdaBodyInvocation::try_bind(&mut evaluator, input)
        .unwrap()
        .run(&runtime)
        .unwrap();
    assert!(output.values().values().is_null(0));
    assert_eq!(output.values().errors().len(), 1);
}

#[test]
fn successful_nullable_null_mints_no_parent_error_and_truth_only_has_exact_boolean_carrier() {
    let runtime = RuntimeControl::default();
    let columns = carriers();
    let parameters = [
        EvaluatedArgument::Column(&columns[0]),
        EvaluatedArgument::Column(&columns[1]),
    ];
    let captures = [
        EvaluatedArgument::Column(&columns[2]),
        EvaluatedArgument::Scalar(&columns[3]),
    ];
    let parents = [0, 0, 1, 1, 1];
    let rows = [3];
    let selection = Selection::try_sparse(5, &rows).unwrap();
    let mut owner = Owner::new(EvaluationDemand::Value);
    if let FunctionArgument::Lambda { result_type, .. } = &mut owner.arguments[1] {
        *result_type = result_type
            .with_nullable(true)
            .expect("valid function nullability declaration");
    }
    let selected = Arc::get_mut(&mut owner.selected).unwrap();
    let mut argument_types = selected.argument_types.as_slice().to_vec();
    argument_types[1] = owner.arguments[1].argument_type();
    selected.argument_types = novarocks_type_contract::FunctionArgumentTypes::from_pure_boxed(
        argument_types.into_boxed_slice(),
    );
    let body = checked_body(owner.call(), true);
    let input = body_input(&body, selection, &parents, &parameters, &captures);
    let mut evaluator = Evaluator::new(body.clone());
    evaluator.mode = Mode::SuccessNull;
    let output = LambdaBodyInvocation::try_bind(&mut evaluator, input)
        .unwrap()
        .run(&runtime)
        .unwrap();
    assert!(output.values().values().is_null(0));
    assert!(output.values().errors().is_empty());
    assert!(output.required_parent_error(0).unwrap().is_none());
    let owner = Owner::new(EvaluationDemand::TruthOnly);
    let body = checked_body(owner.call(), true);
    assert_eq!(body.context().demand, EvaluationDemand::TruthOnly);
    assert_eq!(body.result_type().data_type().clone(), DataType::Boolean);
    let input = body_input(&body, selection, &parents, &parameters, &captures);
    let mut evaluator = Evaluator::new(body.clone());
    let output = LambdaBodyInvocation::try_bind(&mut evaluator, input)
        .unwrap()
        .run(&runtime)
        .unwrap();
    assert!(
        output
            .values()
            .values()
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .value(0)
    );
}

#[test]
fn equal_foreign_body_arc_cannot_bind_and_post_callback_contract_drift_is_checked() {
    let owner = Owner::new(EvaluationDemand::Value);
    let body = checked_body(owner.call(), true);
    let columns = carriers();
    let parameters = [
        EvaluatedArgument::Column(&columns[0]),
        EvaluatedArgument::Column(&columns[1]),
    ];
    let captures = [
        EvaluatedArgument::Column(&columns[2]),
        EvaluatedArgument::Scalar(&columns[3]),
    ];
    let parents = [0, 0, 1, 1, 1];
    let rows = [3];
    let input = body_input(
        &body,
        Selection::try_sparse(5, &rows).unwrap(),
        &parents,
        &parameters,
        &captures,
    );
    let runtime = RuntimeControl::default();
    let foreign = Arc::new((*body).clone());
    assert_eq!(foreign, body);
    let mut evaluator = Evaluator::new(foreign);
    assert!(matches!(
        LambdaBodyInvocation::try_bind(&mut evaluator, input),
        Err(KernelFailure::InvalidProgram(_))
    ));
    assert_eq!(evaluator.calls, 0);
    for failure in [
        None,
        Some(operational()),
        Some(actual_lifecycle_source()),
        Some(KernelFailure::Cancelled),
        Some(KernelFailure::DeadlineExceeded),
        Some(KernelFailure::ResourceExhausted),
    ] {
        let mut evaluator = Evaluator::new(body.clone());
        evaluator.mode = Mode::Drift(failure.clone());
        let mut invocation = LambdaBodyInvocation::try_bind(&mut evaluator, input).unwrap();
        let result = invocation.run(&runtime);
        if let Some(error) = failure {
            assert_eq!(result.unwrap_err(), error);
        } else {
            assert!(matches!(result, Err(KernelFailure::Internal(_))));
        }
        assert_eq!(
            invocation.run(&runtime).unwrap_err(),
            KernelFailure::InstanceFailed
        );
        assert_eq!(evaluator.calls, 1);
        assert_eq!(*evaluator.contract, *body);
        assert!(!Arc::ptr_eq(&evaluator.contract, &body));
    }
}

#[test]
fn zero_selection_skips_callback_and_success_error_cancel_consume_each_frame_once() {
    let owner = Owner::new(EvaluationDemand::Value);
    let body = checked_body(owner.call(), true);
    let columns = carriers();
    let parameters = [
        EvaluatedArgument::Column(&columns[0]),
        EvaluatedArgument::Column(&columns[1]),
    ];
    let captures = [
        EvaluatedArgument::Column(&columns[2]),
        EvaluatedArgument::Scalar(&columns[3]),
    ];
    let parents = [0, 0, 1, 1, 1];
    let runtime = RuntimeControl::default();
    let rows = [];
    let empty = body_input(
        &body,
        Selection::try_sparse(5, &rows).unwrap(),
        &parents,
        &parameters,
        &captures,
    );
    let mut evaluator = Evaluator::new(body.clone());
    let mut invocation = LambdaBodyInvocation::try_bind(&mut evaluator, empty).unwrap();
    assert_eq!(invocation.run(&runtime).unwrap().values().values().len(), 0);
    assert_eq!(
        invocation.run(&runtime).unwrap_err(),
        KernelFailure::InstanceFailed
    );
    assert_eq!(evaluator.calls, 0);
    let rows = [3];
    let input = body_input(
        &body,
        Selection::try_sparse(5, &rows).unwrap(),
        &parents,
        &parameters,
        &captures,
    );
    for failure in [
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
        operational(),
        internal("fixture fault"),
    ] {
        let mut evaluator = Evaluator::new(body.clone());
        evaluator.mode = Mode::Failure(failure.clone());
        let mut invocation = LambdaBodyInvocation::try_bind(&mut evaluator, input).unwrap();
        assert_eq!(invocation.run(&runtime).unwrap_err(), failure);
        assert_eq!(
            invocation.run(&runtime).unwrap_err(),
            KernelFailure::InstanceFailed
        );
        assert_eq!(evaluator.calls, 1);
    }
    for failure in [
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
    ] {
        let mut evaluator = Evaluator::new(body.clone());
        let mut invocation = LambdaBodyInvocation::try_bind(&mut evaluator, input).unwrap();
        let control = RuntimeControl {
            failure: Some(failure.clone()),
            ..RuntimeControl::default()
        };
        assert_eq!(invocation.run(&control).unwrap_err(), failure);
        assert_eq!(
            invocation.run(&runtime).unwrap_err(),
            KernelFailure::InstanceFailed
        );
        assert_eq!(evaluator.calls, 0);
    }
}

#[test]
fn invocation_shape_ordered_types_and_selected_nonnull_are_checked_before_callback() {
    let owner = Owner::new(EvaluationDemand::Value);
    let body = checked_body(owner.call(), true);
    let columns = carriers();
    let parameters = [
        EvaluatedArgument::Column(&columns[0]),
        EvaluatedArgument::Column(&columns[1]),
    ];
    let captures = [
        EvaluatedArgument::Column(&columns[2]),
        EvaluatedArgument::Scalar(&columns[3]),
    ];
    let parents = [0, 0, 1, 1, 1];
    let runtime = RuntimeControl::default();
    let map = LambdaElementRowMap::try_new(Selection::all(2), &parents, &runtime).unwrap();
    let rows = [1, 3];
    let selection = Selection::try_sparse(5, &rows).unwrap();
    for (parameters, captures) in [
        (&parameters[..1], &captures[..]),
        (&parameters[..], &captures[..1]),
        (&parameters[..], &[][..]),
    ] {
        assert!(matches!(
            LambdaBodyInput::try_new(&body, map, selection, parameters, captures, &runtime),
            Err(KernelFailure::InvalidProgram(_))
        ));
    }
    let reversed_params = [parameters[1], parameters[0]];
    let reversed_captures = [captures[1], captures[0]];
    assert!(matches!(
        LambdaBodyInput::try_new(&body, map, selection, &reversed_params, &captures, &runtime),
        Err(KernelFailure::InvalidProgram(_))
    ));
    assert!(matches!(
        LambdaBodyInput::try_new(
            &body,
            map,
            selection,
            &parameters,
            &reversed_captures,
            &runtime
        ),
        Err(KernelFailure::InvalidProgram(_))
    ));
    let null: ArrayRef = Arc::new(Int64Array::from(vec![
        Some(1),
        None,
        Some(1),
        Some(1),
        Some(1),
    ]));
    let null_parameters = [EvaluatedArgument::Column(&null), parameters[1]];
    assert!(matches!(
        LambdaBodyInput::try_new(&body, map, selection, &null_parameters, &captures, &runtime),
        Err(KernelFailure::InvalidProgram(_))
    ));
    let rows = [3];
    assert!(
        LambdaBodyInput::try_new(
            &body,
            map,
            Selection::try_sparse(5, &rows).unwrap(),
            &null_parameters,
            &captures,
            &runtime
        )
        .is_ok()
    );
    assert!(matches!(
        LambdaBodyInput::try_new(
            &body,
            map,
            Selection::all(6),
            &parameters,
            &captures,
            &runtime
        ),
        Err(KernelFailure::InvalidProgram(_))
    ));
}

fn body_effects(context: ExpressionEffectContext) -> ScopedExpressionEffects {
    ScopedExpressionEffects::primitive(
        context,
        ExpressionEffects {
            may_raise_invocation_data: false,
            value_stability: FunctionVolatility::Stable,
            may_raise_row_error: true,
            has_instance_state: true,
            observable_effects: ObservableEffects::NONE,
        },
    )
}

#[test]
fn body_context_capture_domain_and_compile_observation_are_exact() {
    let owner = Owner::new(EvaluationDemand::Value);
    let call = owner.call();
    let exact = context(&call);
    let different_use = ExpressionEffectContext {
        use_id: ExpressionUseId::new(12),
        ..exact
    };
    let wrong_demand = ExpressionEffectContext {
        demand: EvaluationDemand::TruthOnly,
        ..exact
    };
    let outer_domain = ExpressionEffectContext {
        region: call.call().context().region,
        ..exact
    };
    for (context, effects) in [
        (wrong_demand, body_effects(wrong_demand)),
        (outer_domain, body_effects(outer_domain)),
        (exact, body_effects(different_use)),
    ] {
        assert!(matches!(
            LambdaBodyContract::try_new(
                call.clone(),
                context,
                effects,
                Box::new([]),
                &CompileControl::default()
            ),
            Err(KernelFailure::InvalidProgram(_))
        ));
    }
    assert!(
        FunctionValueType::try_with_logical_type(
            DataType::Int64,
            false,
            novarocks_type_contract::ValueLogicalType::LargeInt
        )
        .is_err()
    );
    let different_domain = FunctionValueType::try_with_logical_type(
        DataType::FixedSizeBinary(16),
        false,
        novarocks_type_contract::ValueLogicalType::LargeInt,
    )
    .unwrap();
    let body = LambdaBodyContract::try_new(
        call.clone(),
        exact,
        body_effects(exact),
        Box::from([different_domain.clone()]),
        &CompileControl::default(),
    )
    .unwrap();
    assert!(body.capture_types()[0].same_semantics(&different_domain));
    assert_eq!(
        LambdaBodyContract::try_new(
            call.clone(),
            exact,
            body_effects(exact),
            vec![i64_type(false); crate::MAX_CALL_EFFECT_ARGUMENTS + 1].into_boxed_slice(),
            &CompileControl::default()
        )
        .unwrap_err(),
        KernelFailure::ResourceExhausted
    );
    assert!(
        LambdaBodyContract::try_new(
            call.clone(),
            exact,
            body_effects(exact),
            vec![i64_type(false); crate::MAX_CALL_EFFECT_ARGUMENTS].into_boxed_slice(),
            &CompileControl::default()
        )
        .is_ok()
    );
    for failure in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        for positive_only in [false, true] {
            let control = CompileControl {
                failure: Some(failure.clone()),
                positive_only,
                work: Mutex::default(),
            };
            let error = LambdaBodyContract::try_new(
                call.clone(),
                exact,
                body_effects(exact),
                vec![i64_type(false); 300].into_boxed_slice(),
                &control,
            )
            .unwrap_err();
            assert_eq!(error, compile_failure(failure.clone()));
            let work = control.work.lock().unwrap();
            if positive_only {
                assert!(work.contains(&256));
            } else {
                assert_eq!(*work, [0]);
            }
        }
    }
    assert_eq!(owner.refines.load(Ordering::Relaxed), 1);
}

#[test]
fn higher_order_entry_control_precedes_wrong_kind_or_non_higher_order_rejection() {
    let mut owner = Owner::new(EvaluationDemand::Value);
    let input = owner.input();
    let receipt = refine_call_effects(&owner, input, &CompileControl::default()).unwrap();
    let wrong_kind = CallEffectInput {
        kind: FunctionKind::Aggregate,
        ..input
    };
    for failure in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        let control = CompileControl {
            failure: Some(failure.clone()),
            ..Default::default()
        };
        assert_eq!(
            HigherOrderCallContract::from_refined(
                wrong_kind,
                &receipt,
                owner.selected.clone(),
                &control
            )
            .unwrap_err(),
            compile_failure(failure)
        );
        assert_eq!(*control.work.lock().unwrap(), [0]);
    }
    assert!(matches!(
        HigherOrderCallContract::from_refined(
            input,
            &receipt,
            Arc::new((*owner.selected).clone()),
            &CompileControl::default()
        ),
        Err(KernelFailure::InvalidProgram(_))
    ));
    assert_eq!(owner.refines.load(Ordering::Relaxed), 1);

    // A genuine eagerly evaluated scalar receipt with no lambda is legal, but
    // cannot be promoted to this higher-order interface.
    owner.arguments.truncate(1);
    owner.uses.truncate(1);
    Arc::get_mut(&mut owner.selected).unwrap().argument_types =
        novarocks_type_contract::FunctionArgumentTypes::from_pure_boxed(Box::from([owner
            .arguments[0]
            .argument_type()]));
    owner.declaration.argument_control = ArgumentControl::Eager;
    let input = owner.input();
    let receipt = refine_call_effects(&owner, input, &CompileControl::default()).unwrap();
    let cancelled = CompileControl {
        failure: Some(CompileControlError::Cancelled),
        ..Default::default()
    };
    assert_eq!(
        HigherOrderCallContract::from_refined(input, &receipt, owner.selected.clone(), &cancelled)
            .unwrap_err(),
        KernelFailure::CompileControl(CompileControlError::Cancelled)
    );
    assert!(matches!(
        HigherOrderCallContract::from_refined(
            input,
            &receipt,
            owner.selected.clone(),
            &CompileControl::default()
        ),
        Err(KernelFailure::InvalidProgram(_))
    ));
    assert_eq!(owner.refines.load(Ordering::Relaxed), 2);
}

#[test]
fn full_selected_arguments_and_private_body_work_stop_at_positive_quantum_without_replay() {
    let owner = Owner::new(EvaluationDemand::Value);
    let body = checked_body(owner.call(), true);
    let columns: [ArrayRef; 4] = [
        Arc::new(Int64Array::from(vec![1; 300])),
        Arc::new(Int32Array::from(vec![2; 300])),
        Arc::new(Int32Array::from(vec![3; 300])),
        Arc::new(Int64Array::from(vec![4])),
    ];
    let parameters = [
        EvaluatedArgument::Column(&columns[0]),
        EvaluatedArgument::Column(&columns[1]),
    ];
    let captures = [
        EvaluatedArgument::Column(&columns[2]),
        EvaluatedArgument::Scalar(&columns[3]),
    ];
    let parents = vec![0; 300];
    let map = LambdaElementRowMap::try_new(Selection::all(1), &parents, &RuntimeControl::default())
        .unwrap();
    let selection = Selection::all(300);
    for failure in [
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
    ] {
        let control = RuntimeControl {
            failure: Some(failure.clone()),
            positive_only: true,
            work: Mutex::default(),
        };
        assert_eq!(
            LambdaBodyInput::try_new(&body, map, selection, &parameters, &captures, &control)
                .unwrap_err(),
            failure
        );
        assert!(control.work.lock().unwrap().contains(&256));

        let input = LambdaBodyInput::try_new(
            &body,
            map,
            selection,
            &parameters,
            &captures,
            &RuntimeControl::default(),
        )
        .unwrap();
        let mut evaluator = Evaluator::new(body.clone());
        let control = RuntimeControl {
            failure: Some(failure.clone()),
            positive_only: true,
            work: Mutex::default(),
        };
        let mut frame = LambdaBodyInvocation::try_bind(&mut evaluator, input).unwrap();
        assert_eq!(frame.run(&control).unwrap_err(), failure);
        assert_eq!(
            frame.run(&RuntimeControl::default()).unwrap_err(),
            KernelFailure::InstanceFailed
        );
        assert_eq!(evaluator.calls, 1);
        assert_eq!(evaluator.samples.len(), 256);
        assert!(control.work.lock().unwrap().contains(&256));
    }
    let input = LambdaBodyInput::try_new(
        &body,
        map,
        selection,
        &parameters,
        &captures,
        &RuntimeControl::default(),
    )
    .unwrap();
    let mut evaluator = Evaluator::new(body.clone());
    let mut frame = LambdaBodyInvocation::try_bind(&mut evaluator, input).unwrap();
    let result = frame.run(&RuntimeControl::default()).unwrap();
    let values = result
        .values()
        .values()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(values.len(), 300);
    assert!(values.values().iter().all(|value| *value == 10));
}

#[test]
fn required_error_bridge_maps_nonzero_compact_element_and_parent_ordinals() {
    let owner = Owner::new(EvaluationDemand::Value);
    let body = checked_body(owner.call(), true);
    let columns: [ArrayRef; 4] = [
        Arc::new(Int64Array::from(vec![99, 2, 99, -3])),
        Arc::new(Int32Array::from(vec![0, 10, 0, 20])),
        Arc::new(Int32Array::from(vec![0, 100, 0, 200])),
        Arc::new(Int64Array::from(vec![7])),
    ];
    let parameters = [
        EvaluatedArgument::Column(&columns[0]),
        EvaluatedArgument::Column(&columns[1]),
    ];
    let captures = [
        EvaluatedArgument::Column(&columns[2]),
        EvaluatedArgument::Scalar(&columns[3]),
    ];
    let outer_rows = [2, 7];
    let parents = [0, 0, 1, 1];
    let elements = [1, 3];
    let outer = Selection::try_sparse(8, &outer_rows).unwrap();
    let selection = Selection::try_sparse(4, &elements).unwrap();
    let runtime = RuntimeControl::default();
    let map = LambdaElementRowMap::try_new(outer, &parents, &runtime).unwrap();
    let input =
        LambdaBodyInput::try_new(&body, map, selection, &parameters, &captures, &runtime).unwrap();
    let mut evaluator = Evaluator::new(body.clone());
    let mut frame = LambdaBodyInvocation::try_bind(&mut evaluator, input).unwrap();
    let output = frame.run(&runtime).unwrap();
    assert_eq!(output.values().errors()[0].selected_ordinal(), 1);
    let required = output.required_parent_error(0).unwrap().unwrap();
    assert!(Arc::ptr_eq(required.contract(), &body));
    assert!(Arc::ptr_eq(output.contract(), &body));
    assert_eq!(required.error().selected_ordinal(), 1);
    assert_eq!(required.original_parent_row(), Some(7));
    assert_eq!(required.outer_selection(), outer);
    assert_eq!(required.error().message(), "negative lambda element");
    assert!(output.required_parent_error(1).unwrap().is_none());
    assert_eq!(evaluator.samples[1], (3, 7, vec![-3, 20], vec![200, 7]));
}

// Only the returned outer borrow can escape this function. The expansion's
// columns, element rows, parent map, evaluator and output all end here.
fn expanded_error_rebound_to_outer<'outer>(
    body: &Arc<LambdaBodyContract>,
    source_outer: Selection<'_>,
    target_outer: Selection<'outer>,
    control: &dyn KernelEvaluationControl,
) -> Result<RequiredLambdaBodyError<'outer>, KernelFailure> {
    let local_outer_rows = source_outer.iter().collect::<Vec<_>>();
    let local_outer = Selection::try_sparse(source_outer.batch_rows(), &local_outer_rows).unwrap();
    let columns: [ArrayRef; 4] = [
        Arc::new(Int64Array::from(vec![2, -3])),
        Arc::new(Int32Array::from(vec![10, 20])),
        Arc::new(Int32Array::from(vec![100, 200])),
        Arc::new(Int64Array::from(vec![7])),
    ];
    let parameters = [
        EvaluatedArgument::Column(&columns[0]),
        EvaluatedArgument::Column(&columns[1]),
    ];
    let captures = [
        EvaluatedArgument::Column(&columns[2]),
        EvaluatedArgument::Scalar(&columns[3]),
    ];
    let parents = [0, source_outer.len() - 1];
    let elements = [0, 1];
    let selection = Selection::try_sparse(2, &elements).unwrap();
    let runtime = RuntimeControl::default();
    let map = LambdaElementRowMap::try_new(local_outer, &parents, &runtime).unwrap();
    let input =
        LambdaBodyInput::try_new(body, map, selection, &parameters, &captures, &runtime).unwrap();
    let mut evaluator = Evaluator::new(body.clone());
    let mut frame = LambdaBodyInvocation::try_bind(&mut evaluator, input).unwrap();
    let output = frame.run(&runtime).unwrap();
    assert_eq!(output.values().errors()[0].selected_ordinal(), 1);
    let required = output.required_parent_error(0).unwrap().unwrap();
    required.into_outer_selection(target_outer, control)
}

#[test]
fn required_error_reborrow_outlives_real_expansion_scope_and_keeps_original_outer_rows() {
    let owner = Owner::new(EvaluationDemand::Value);
    let body = checked_body(owner.call(), true);
    let outer_rows = [2, 7];
    let outer = Selection::try_sparse(8, &outer_rows).unwrap();
    let required =
        expanded_error_rebound_to_outer(&body, outer, outer, &RuntimeControl::default()).unwrap();
    // The helper's expansion locals have dropped; the borrowed enclosing rows
    // and owned exact body/diagnostic remain usable here.
    assert_eq!(required.outer_selection(), outer);
    assert!(Arc::ptr_eq(required.contract(), &body));
    assert_eq!(required.error().selected_ordinal(), 1);
    assert_eq!(required.original_parent_row(), Some(7));
    assert_eq!(required.error().message(), "negative lambda element");

    let wrong_rows = [2, 6];
    let wrong = Selection::try_sparse(8, &wrong_rows).unwrap();
    assert!(matches!(
        expanded_error_rebound_to_outer(&body, outer, wrong, &RuntimeControl::default()),
        Err(KernelFailure::InvalidProgram(_))
    ));
}

#[test]
fn required_error_reborrow_observes_complete_independent_sparse_outer_rows() {
    let owner = Owner::new(EvaluationDemand::Value);
    let body = checked_body(owner.call(), true);
    let outer_rows = (0..300).map(|row| 2 * row + 1).collect::<Vec<_>>();
    let outer = Selection::try_sparse(1000, &outer_rows).unwrap();
    let required =
        expanded_error_rebound_to_outer(&body, outer, outer, &RuntimeControl::default()).unwrap();
    assert_eq!(required.original_parent_row(), Some(599));
    assert_eq!(required.error().selected_ordinal(), 299);
    assert!(Arc::ptr_eq(required.contract(), &body));
    for failure in [
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
    ] {
        for positive_only in [false, true] {
            let control = RuntimeControl {
                failure: Some(failure.clone()),
                positive_only,
                work: Mutex::default(),
            };
            assert_eq!(
                expanded_error_rebound_to_outer(&body, outer, outer, &control).unwrap_err(),
                failure
            );
            let work = control.work.lock().unwrap();
            if positive_only {
                let (quantum, entries) = work.split_last().expect("observed control failure");
                assert_eq!(*quantum, 256);
                assert!(!entries.is_empty());
                assert!(entries.iter().all(|units| *units == 0));
            } else {
                assert_eq!(*work, [0]);
            }
        }
    }
    let mut changed = outer_rows.clone();
    changed[299] = 600;
    let wrong = Selection::try_sparse(1000, &changed).unwrap();
    let control = RuntimeControl::default();
    assert!(matches!(
        expanded_error_rebound_to_outer(&body, outer, wrong, &control),
        Err(KernelFailure::InvalidProgram(_))
    ));
    // A mismatch in the suffix cannot avoid observation of its earlier rows.
    assert!(control.work.lock().unwrap().contains(&256));
}

fn actual_lifecycle_source() -> KernelFailure {
    let source = crate::aggregate_scalar::build_scalar_array(
        &DataType::Boolean,
        vec![Some(crate::aggregate_scalar::AggScalarValue::Int64(1))],
        &mut crate::aggregate_scalar::ScalarWork::new(None),
    )
    .unwrap_err();
    let fact = novarocks_failure_contract::FailureProjection::failure_fact(&source);
    assert_eq!(
        fact.class(),
        novarocks_failure_contract::FailureClass::Invariant
    );
    assert_eq!(fact.reason().code(), 315);
    assert!(matches!(&source, KernelFailure::Source(_)));
    source
}
