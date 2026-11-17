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
    FunctionArgument, FunctionBindingError, FunctionBindingRequest, FunctionBindingResolver,
    FunctionEffectOwner, FunctionEffectOwnerError, FunctionId, FunctionOverloadId, RowDataError,
};
use crate::{FunctionKind, FunctionResultType};
use arrow_array::{Array, ArrayRef, BooleanArray, Int32Array, Int64Array};
use arrow_schema::DataType;
use novarocks_type_contract::{ArgumentControl, EvaluationDemand, ExpressionUseId};
use novarocks_type_contract::{
    CallEffects, CallProofScope, CompileControlError, DecimalOverflowPolicy, EvaluationRegionId,
    ExpressionEffects, FunctionEffectDeclaration, FunctionFailureBehavior, FunctionInstanceState,
    FunctionIntrinsicRowError, FunctionNullBehavior, FunctionVolatility, ObservableEffects,
    SemanticParameters,
};
use novarocks_type_contract::{CompileCheckpoints, CompilePhase, ExpressionEffectContext};
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
    prepares: AtomicUsize,
    config: Arc<Config>,
    replace: u8,
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
            FunctionArgument::Value {
                value_type: i32_type(false),
                constant: None,
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
            uses: vec![
                Some(ExpressionUseId::new(1)),
                Some(ExpressionUseId::new(2)),
                Some(ExpressionUseId::new(3)),
            ],
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
                instance_state: FunctionInstanceState::ScalarInstance,
                observable_effects: ObservableEffects::NONE,
                environment_dependencies: Box::new([]),
            },
            refines: AtomicUsize::new(0),
            prepares: AtomicUsize::new(0),
            config: Arc::new(Config::default()),
            replace: 0,
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
    fn options(&self) -> HigherOrderPreparationOptions {
        let outer = self.input().context;
        let inner = ExpressionEffectContext {
            use_id: ExpressionUseId::new(10),
            region: EvaluationRegionId::new(11),
            demand: EvaluationDemand::Value,
        };
        HigherOrderPreparationOptions {
            arguments: ScopedExpressionEffects::primitive(outer, ExpressionEffects::PURE_VALUE),
            body_context: inner,
            body_effects: ScopedExpressionEffects::primitive(
                inner,
                ExpressionEffects {
                    may_raise_invocation_data: false,
                    value_stability: FunctionVolatility::Stable,
                    may_raise_row_error: true,
                    has_instance_state: true,
                    observable_effects: ObservableEffects::NONE,
                },
            ),
            capture_types: Box::from([i32_type(false), i64_type(false)]),
        }
    }
    fn fresh(&self) -> HigherOrderSpecialization {
        specialize_higher_order(
            self,
            self.input(),
            self.selected.clone(),
            self.options(),
            &CompileControl::default(),
        )
        .unwrap()
    }
    fn frozen(&self) -> CallEffects {
        CallEffects {
            value_stability: self.declaration.value_stability,
            own_row_error: self.declaration.own_row_error,
            failure_behavior: self.declaration.failure_behavior,
            null_behavior: self.declaration.null_behavior,
            argument_control: self.declaration.argument_control,
            instance_state: self.declaration.instance_state,
            observable_effects: self.declaration.observable_effects,
            environment: Box::new([]),
            proof_scope: self.input().proof_scope,
        }
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

impl FunctionBindingResolver for Owner {
    fn resolve(
        &self,
        _: FunctionBindingRequest<'_>,
        _control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<FunctionBindingSelection, FunctionBindingError> {
        panic!("exact specialization must not reselect")
    }
    fn validate_selected(
        &self,
        selected: &FunctionBindingSelection,
        request: FunctionBindingRequest<'_>,
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<(), FunctionBindingError> {
        if !std::ptr::eq(selected, self.selected.as_ref())
            || !crate::binding::arguments_equal_for_test(
                request.arguments,
                &self.arguments,
                control,
            )?
            || request.logical_argument_count != self.arguments.len()
        {
            Err(FunctionBindingError::UnknownFunction)
        } else {
            Ok(())
        }
    }
}
#[derive(Default)]
enum BodyMode {
    #[default]
    Good,
    ForeignSelection,
    WrongType,
    SuccessNull,
    Drift(Option<KernelFailure>),
    Failure(KernelFailure),
}
type Sample = (usize, usize, Vec<i64>, Vec<i64>);

struct BodyEvaluator {
    contract: Arc<LambdaBodyContract>,
    mode: BodyMode,
    calls: usize,
    entered: Arc<AtomicUsize>,
    samples: Vec<Sample>,
}
impl BodyEvaluator {
    fn new(contract: Arc<LambdaBodyContract>) -> Self {
        Self {
            contract,
            mode: BodyMode::Good,
            calls: 0,
            entered: Arc::new(AtomicUsize::new(0)),
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
impl BoundLambdaBodyEvaluator for BodyEvaluator {
    fn contract(&self) -> &Arc<LambdaBodyContract> {
        &self.contract
    }
    fn evaluate<'a>(
        &mut self,
        input: LambdaBodyInput<'_, 'a>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<SelectedValues<'a>, KernelFailure> {
        self.calls += 1;
        self.entered.fetch_add(1, Ordering::Relaxed);
        control.checkpoint(0)?;
        if let BodyMode::Drift(error) = &self.mode {
            self.contract = Arc::new((*self.contract).clone());
            if let Some(error) = error {
                return Err(error.clone());
            }
        }
        if let BodyMode::Failure(error) = &self.mode {
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
        let selection = if matches!(self.mode, BodyMode::ForeignSelection) {
            Selection::try_sparse(input.selection().batch_rows() + 1, &[0, 1, 2, 4]).unwrap()
        } else {
            input.selection()
        };
        let array: ArrayRef = if matches!(self.mode, BodyMode::WrongType) {
            Arc::new(Int32Array::from(vec![0; selection.len()]))
        } else if matches!(self.mode, BodyMode::SuccessNull) {
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

#[derive(Clone, Debug, Default)]
enum Mode {
    #[default]
    Good,
    OwnError,
    WrongBody,
    WrongOuter,
    WrongDiagnostic,
    Duplicate,
    Unused,
    Null,
    WrongType,
    Swallow(bool),
    Grow(Option<KernelFailure>),
    Drift(Option<KernelFailure>),
    Failure(KernelFailure),
}
#[derive(Clone, Debug, Default)]
enum Init {
    #[default]
    Good,
    Failure(KernelFailure),
    Grow,
    Drift,
}
#[derive(Debug)]
struct Config {
    mode: Mode,
    init: Init,
    bound: AtomicUsize,
    creates: AtomicUsize,
    drops: AtomicUsize,
    evaluations: AtomicUsize,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            mode: Mode::Good,
            init: Init::Good,
            bound: AtomicUsize::new(4096),
            creates: AtomicUsize::new(0),
            drops: AtomicUsize::new(0),
            evaluations: AtomicUsize::new(0),
        }
    }
}
#[derive(Debug)]
struct Prepared {
    contract: Arc<HigherOrderCallContract>,
    body: Arc<LambdaBodyContract>,
    config: Arc<Config>,
}
impl PureHigherOrderImplementation for Owner {
    fn prepare_higher_order(
        &self,
        input: CallEffectInput<'_>,
        contract: Arc<HigherOrderCallContract>,
        body: Arc<LambdaBodyContract>,
        control: &dyn PureCompileControl,
    ) -> Result<Arc<dyn PreparedHigherOrderKernel>, KernelFailure> {
        control
            .checkpoint(CompilePhase::FunctionSpecialization, 0)
            .map_err(crate::kernel_control::compile_failure)?;
        self.validate_selected(input.selected, input.request, control)
            .map_err(|_| crate::kernel_control::invalid("fixture changed selected binding"))?;
        self.prepares.fetch_add(1, Ordering::Relaxed);
        let contract = if self.replace == 1 {
            Arc::new((*contract).clone())
        } else {
            contract
        };
        let body = if self.replace == 2 {
            Arc::new((*body).clone())
        } else {
            body
        };
        Ok(Arc::new(Prepared {
            contract,
            body,
            config: self.config.clone(),
        }))
    }
}
impl PreparedHigherOrderKernel for Prepared {
    fn contract(&self) -> &Arc<HigherOrderCallContract> {
        &self.contract
    }
    fn body_contract(&self) -> &Arc<LambdaBodyContract> {
        &self.body
    }
    fn instance_retained_upper_bound(&self) -> usize {
        self.config.bound.load(Ordering::Relaxed)
    }
    fn create_instance(
        &self,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Box<dyn HigherOrderKernelInstance>, KernelFailure> {
        control.checkpoint(0)?;
        self.config.creates.fetch_add(1, Ordering::Relaxed);
        let mut instance = Instance {
            config: self.config.clone(),
            body: self.body.clone(),
            calls: 0,
            retained: 64,
        };
        match &self.config.init {
            Init::Good => {}
            Init::Failure(error) => return Err(error.clone()),
            Init::Grow => instance.retained = self.instance_retained_upper_bound() + 1,
            Init::Drift => {
                self.config.bound.fetch_add(1, Ordering::Relaxed);
            }
        }
        Ok(Box::new(instance))
    }
}
struct Instance {
    config: Arc<Config>,
    body: Arc<LambdaBodyContract>,
    calls: usize,
    retained: usize,
}
impl Drop for Instance {
    fn drop(&mut self) {
        self.config.drops.fetch_add(1, Ordering::Relaxed);
    }
}
fn kernel_error() -> KernelFailure {
    KernelFailure::Operational(crate::KernelDiagnostic::new("fixture owner failure"))
}
impl HigherOrderKernelInstance for Instance {
    fn retained_bytes(&self) -> usize {
        self.retained
    }
    fn evaluate<'input>(
        &mut self,
        input: HigherOrderCallInput<'_, 'input>,
        executor: &mut LambdaBodyExecutor<'_, 'input>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<HigherOrderOutput<'input>, KernelFailure> {
        self.calls += 1;
        self.config.evaluations.fetch_add(1, Ordering::Relaxed);
        if let Mode::Failure(error) = &self.config.mode {
            return Err(error.clone());
        }
        if let Mode::Grow(error) = &self.config.mode {
            self.retained = self.config.bound.load(Ordering::Relaxed) + 1;
            if let Some(error) = error {
                return Err(error.clone());
            }
        }
        if let Mode::Drift(error) = &self.config.mode {
            self.config.bound.fetch_add(1, Ordering::Relaxed);
            if let Some(error) = error {
                return Err(error.clone());
            }
        }
        let mut local_outer_rows = input.selection().iter().collect::<Vec<_>>();
        if matches!(self.config.mode, Mode::WrongOuter) {
            local_outer_rows[0] = 0;
        }
        let local_outer =
            Selection::try_sparse(input.selection().batch_rows(), &local_outer_rows).unwrap();
        let mut parents = vec![];
        let mut p0 = vec![];
        let mut p1 = vec![];
        let mut c0 = vec![];
        let mut c1 = vec![];
        for ordinal in 0..input.selection().len() {
            let row = input.selection().row(ordinal).unwrap();
            for offset in 0..2 {
                parents.push(ordinal);
                p0.push(number(input.arguments()[0], ordinal, row) + offset);
                p1.push(number(input.arguments()[1], ordinal, row) as i32);
                c0.push(number(input.captures()[0], ordinal, row) as i32);
                c1.push(number(input.captures()[1], ordinal, row));
            }
        }
        let columns: [ArrayRef; 4] = [
            Arc::new(Int64Array::from(p0)),
            Arc::new(Int32Array::from(p1)),
            Arc::new(Int32Array::from(c0)),
            Arc::new(Int64Array::from(c1)),
        ];
        let parameters = [
            EvaluatedArgument::Column(&columns[0]),
            EvaluatedArgument::Column(&columns[1]),
        ];
        let captures = [
            EvaluatedArgument::Column(&columns[2]),
            EvaluatedArgument::Column(&columns[3]),
        ];
        let map = LambdaElementRowMap::try_new(local_outer, &parents, control)?;
        let elements = Selection::all(parents.len());
        let child = if matches!(self.config.mode, Mode::WrongBody | Mode::WrongOuter) {
            // Fault injection mints genuine evidence through a checked body,
            // then deliberately returns it to an unrelated authority/domain.
            let contract = if matches!(self.config.mode, Mode::WrongBody) {
                Arc::new((*self.body).clone())
            } else {
                self.body.clone()
            };
            let mut wrong = BodyEvaluator::new(contract.clone());
            let input = LambdaBodyInput::try_new(
                &contract,
                map,
                elements,
                &parameters,
                &captures,
                control,
            )?;
            LambdaBodyInvocation::try_bind(&mut wrong, input)?.run(control)?
        } else {
            match executor.evaluate(map, elements, &parameters, &captures, control) {
                Ok(child) => child,
                Err(error) => {
                    if matches!(self.config.mode, Mode::Swallow(_)) {
                        // Retrying the same failed body executor cannot call
                        // private body code a second time in this invocation.
                        assert_eq!(
                            executor
                                .evaluate(
                                    map,
                                    elements,
                                    &parameters,
                                    &captures,
                                    &RuntimeControl::default()
                                )
                                .unwrap_err(),
                            KernelFailure::InstanceFailed
                        );
                        if matches!(self.config.mode, Mode::Swallow(true)) {
                            return Err(kernel_error());
                        }
                        let array: ArrayRef =
                            Arc::new(Int64Array::from(vec![1; input.selection().len()]));
                        return Ok(HigherOrderOutput::new(
                            SelectedValues::try_new(
                                input.selection(),
                                &DataType::Int64,
                                array,
                                Box::new([]),
                            )
                            .unwrap(),
                            Box::new([]),
                        ));
                    }
                    return Err(error);
                }
            }
        };
        let mut evidence = vec![];
        let mut errors = vec![];
        let mut values = vec![Some(0i64); input.selection().len()];
        let child_values = child
            .values()
            .values()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for ordinal in 0..input.selection().len() {
            values[ordinal] = Some(
                child_values.value(ordinal * 2)
                    + child_values.value(ordinal * 2 + 1)
                    + (self.calls - 1) as i64,
            );
            if let Some((index, _)) = child
                .values()
                .errors()
                .iter()
                .enumerate()
                .find(|(_, error)| error.selected_ordinal() / 2 == ordinal)
            {
                values[ordinal] = None;
                let target = if matches!(self.config.mode, Mode::WrongOuter) {
                    Selection::try_sparse(5, &[0, 3]).unwrap()
                } else {
                    input.selection()
                };
                let required = child
                    .required_parent_error(index)?
                    .unwrap()
                    .into_outer_selection(target, control)?;
                errors.push(RowDataError::new(
                    ordinal,
                    if matches!(self.config.mode, Mode::WrongDiagnostic) {
                        "forged diagnostic"
                    } else {
                        required.error().message()
                    },
                ));
                evidence.push(required);
                if matches!(self.config.mode, Mode::Duplicate) {
                    errors.push(RowDataError::new(ordinal + 1, "negative lambda element"));
                    values[ordinal + 1] = None;
                    evidence.push(
                        child
                            .required_parent_error(index)?
                            .unwrap()
                            .into_outer_selection(target, control)?,
                    );
                    break;
                }
            }
        }
        if matches!(self.config.mode, Mode::Unused) {
            errors.clear();
            values.fill(Some(1));
        }
        if matches!(self.config.mode, Mode::OwnError) {
            errors.push(RowDataError::new(0, "forged own error"));
            values[0] = None;
        }
        if matches!(self.config.mode, Mode::Null) {
            values[0] = None;
        }
        let array: ArrayRef = if matches!(self.config.mode, Mode::WrongType) {
            Arc::new(Int32Array::from(vec![0; values.len()]))
        } else {
            Arc::new(Int64Array::from(values))
        };
        let values = SelectedValues::try_new(
            input.selection(),
            array.data_type(),
            array.clone(),
            errors.into_boxed_slice(),
        )
        .unwrap();
        Ok(HigherOrderOutput::new(values, evidence.into_boxed_slice()))
    }
}
fn columns(negative: bool) -> [ArrayRef; 4] {
    [
        Arc::new(Int64Array::from(vec![
            99,
            if negative { -3 } else { 2 },
            77,
            4,
            88,
        ])),
        Arc::new(Int32Array::from(vec![0, 10, 0, 20, 0])),
        Arc::new(Int32Array::from(vec![0, 100, 0, 200, 0])),
        Arc::new(Int64Array::from(vec![7])),
    ]
}
fn values_args(columns: &[ArrayRef; 4]) -> [EvaluatedArgument<'_>; 2] {
    [
        EvaluatedArgument::Column(&columns[0]),
        EvaluatedArgument::Column(&columns[1]),
    ]
}
fn values_captures(columns: &[ArrayRef; 4]) -> [EvaluatedArgument<'_>; 2] {
    [
        EvaluatedArgument::Column(&columns[2]),
        EvaluatedArgument::Scalar(&columns[3]),
    ]
}
#[test]
fn real_sparse_collection_expansion_preserves_ordinary_order_capture_samples_and_independent_state()
{
    let owner = Owner::new(EvaluationDemand::Value);
    let specialization = owner.fresh();
    assert!(
        specialization
            .effects()
            .for_use(owner.input().context)
            .unwrap()
            .may_raise_row_error
    );
    assert!(
        specialization
            .effects()
            .for_use(owner.input().context)
            .unwrap()
            .has_instance_state
    );
    let prepared = specialization.into_prepared();
    let mut first =
        HigherOrderEvaluationInstance::instantiate(prepared.clone(), &RuntimeControl::default())
            .unwrap();
    let mut second =
        HigherOrderEvaluationInstance::instantiate(prepared.clone(), &RuntimeControl::default())
            .unwrap();
    let mut body1 = BodyEvaluator::new(prepared.body_contract().clone());
    let mut body2 = BodyEvaluator::new(prepared.body_contract().clone());
    let columns = columns(false);
    let args = values_args(&columns);
    let captures = values_captures(&columns);
    let rows = [1, 3];
    let selection = Selection::try_sparse(5, &rows).unwrap();
    for (use_second, expected) in [(false, [239, 463]), (false, [240, 464]), (true, [239, 463])] {
        let (instance, body) = if use_second {
            (&mut second, &mut body2)
        } else {
            (&mut first, &mut body1)
        };
        let output = instance
            .evaluate(
                selection,
                &args,
                &captures,
                body,
                &RuntimeControl::default(),
            )
            .unwrap();
        assert_eq!(
            output
                .values()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values(),
            &expected
        );
        assert!(output.errors().is_empty());
    }
    assert_eq!(
        body2.samples,
        [
            (0, 1, vec![2, 10], vec![100, 7]),
            (1, 1, vec![3, 10], vec![100, 7]),
            (2, 3, vec![4, 20], vec![200, 7]),
            (3, 3, vec![5, 20], vec![200, 7])
        ]
    );
    assert_eq!(owner.refines.load(Ordering::Relaxed), 1);
    assert_eq!(owner.prepares.load(Ordering::Relaxed), 1);
    assert_eq!(owner.config.creates.load(Ordering::Relaxed), 2);
}

fn owner_with(mode: Mode, init: Init) -> Owner {
    let mut owner = Owner::new(EvaluationDemand::Value);
    let config = Arc::get_mut(&mut owner.config).unwrap();
    config.mode = mode;
    config.init = init;
    owner
}
fn instantiate(owner: &Owner) -> (HigherOrderEvaluationInstance, BodyEvaluator) {
    let prepared = owner.fresh().into_prepared();
    let body = BodyEvaluator::new(prepared.body_contract().clone());
    (
        HigherOrderEvaluationInstance::instantiate(prepared, &RuntimeControl::default()).unwrap(),
        body,
    )
}
#[test]
fn fresh_frozen_specialization_refines_once_joins_body_full_effects_and_owns_no_instance() {
    let owner = Owner::new(EvaluationDemand::Value);
    let fresh = owner.fresh();
    assert_eq!(owner.refines.load(Ordering::Relaxed), 1);
    assert_eq!(owner.prepares.load(Ordering::Relaxed), 1);
    assert_eq!(owner.config.creates.load(Ordering::Relaxed), 0);
    owner.refines.store(0, Ordering::Relaxed);
    owner.prepares.store(0, Ordering::Relaxed);
    let frozen = specialize_frozen_higher_order(
        &owner,
        owner.input(),
        owner.selected.clone(),
        &owner.frozen(),
        owner.options(),
        &CompileControl::default(),
    )
    .unwrap();
    assert_eq!(fresh.effects(), frozen.effects());
    let effects = frozen.effects().for_use(owner.input().context).unwrap();
    assert!(effects.may_raise_row_error);
    assert!(effects.has_instance_state);
    assert_eq!(effects.value_stability, FunctionVolatility::Stable);
    assert_eq!(
        frozen.prepared().contract().call().effects().own_row_error,
        FunctionIntrinsicRowError::NoRowError
    );
    assert!(Arc::ptr_eq(
        frozen.prepared().body_contract().call(),
        frozen.prepared().contract()
    ));
    assert_eq!(owner.refines.load(Ordering::Relaxed), 1);
    assert_eq!(owner.prepares.load(Ordering::Relaxed), 1);
    assert_eq!(owner.config.creates.load(Ordering::Relaxed), 0);
    owner.prepares.store(0, Ordering::Relaxed);
    let mut forged = owner.frozen();
    forged.own_row_error = FunctionIntrinsicRowError::MayRaise;
    assert!(
        specialize_frozen_higher_order(
            &owner,
            owner.input(),
            owner.selected.clone(),
            &forged,
            owner.options(),
            &CompileControl::default()
        )
        .is_err()
    );
    assert_eq!(owner.prepares.load(Ordering::Relaxed), 0);
}
#[test]
fn prepare_cannot_replace_value_equal_call_or_body_authority_and_compile_entry_is_observed() {
    for replace in [1, 2] {
        let mut owner = Owner::new(EvaluationDemand::Value);
        owner.replace = replace;
        assert!(matches!(
            specialize_higher_order(
                &owner,
                owner.input(),
                owner.selected.clone(),
                owner.options(),
                &CompileControl::default()
            ),
            Err(FunctionSpecializationFailure::Kernel(
                KernelFailure::Internal(_)
            ))
        ));
        assert_eq!(owner.refines.load(Ordering::Relaxed), 1);
        assert_eq!(owner.prepares.load(Ordering::Relaxed), 1);
    }
    for failure in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        let owner = Owner::new(EvaluationDemand::Value);
        let control = CompileControl {
            failure: Some(failure.clone()),
            ..Default::default()
        };
        assert!(
            matches!(specialize_higher_order(&owner,owner.input(),owner.selected.clone(),owner.options(),&control),Err(FunctionSpecializationFailure::Control(error)) if error==failure)
        );
        assert_eq!(owner.refines.load(Ordering::Relaxed), 0);
        assert_eq!(owner.prepares.load(Ordering::Relaxed), 0);
    }
}
#[test]
fn empty_outer_skips_owner_and_body_and_exact_input_shape_is_required() {
    let owner = Owner::new(EvaluationDemand::Value);
    let (mut instance, mut body) = instantiate(&owner);
    let columns = columns(false);
    let args = values_args(&columns);
    let captures = values_captures(&columns);
    let empty = Selection::try_sparse(5, &[]).unwrap();
    assert!(
        instance
            .evaluate(
                empty,
                &args,
                &captures,
                &mut body,
                &RuntimeControl::default()
            )
            .unwrap()
            .values()
            .is_empty()
    );
    assert_eq!(body.calls, 0);
    assert_eq!(owner.config.evaluations.load(Ordering::Relaxed), 0);
    let rows = [1, 3];
    let selection = Selection::try_sparse(5, &rows).unwrap();
    for shape in 0..4 {
        let owner = Owner::new(EvaluationDemand::Value);
        let (mut instance, mut body) = instantiate(&owner);
        let reversed = [args[1], args[0]];
        let reversed_captures = [captures[1], captures[0]];
        let (arguments, captures) = match shape {
            0 => (&args[..1], &captures[..]),
            1 => (&args[..], &captures[..1]),
            2 => (&reversed[..], &captures[..]),
            _ => (&args[..], &reversed_captures[..]),
        };
        assert!(matches!(
            instance.evaluate(
                selection,
                arguments,
                captures,
                &mut body,
                &RuntimeControl::default()
            ),
            Err(KernelFailure::InvalidProgram(_))
        ));
        assert_eq!(owner.config.evaluations.load(Ordering::Relaxed), 0);
        assert_eq!(body.calls, 0);
        assert_eq!(
            instance
                .evaluate(
                    selection,
                    &args,
                    &values_captures(&columns),
                    &mut body,
                    &RuntimeControl::default()
                )
                .unwrap_err(),
            KernelFailure::InstanceFailed
        );
    }
    let owner = Owner::new(EvaluationDemand::Value);
    let (mut instance, mut body) = instantiate(&owner);
    body.contract = Arc::new((*body.contract).clone());
    assert!(matches!(
        instance.evaluate(
            selection,
            &args,
            &captures,
            &mut body,
            &RuntimeControl::default()
        ),
        Err(KernelFailure::InvalidProgram(_))
    ));
    assert_eq!(owner.config.evaluations.load(Ordering::Relaxed), 0);
    assert_eq!(body.calls, 0);
}
#[test]
fn required_body_error_survives_expansion_locals_and_never_failing_parent_nonnull_result() {
    let owner = Owner::new(EvaluationDemand::Value);
    let (mut instance, mut body) = instantiate(&owner);
    let columns = columns(true);
    let args = values_args(&columns);
    let captures = values_captures(&columns);
    let rows = [1, 3];
    let selection = Selection::try_sparse(5, &rows).unwrap();
    // Instance.evaluate has dropped every expanded column/map/frame before
    // its parent result is checked and returned with the enclosing row borrow.
    let output = instance
        .evaluate(
            selection,
            &args,
            &captures,
            &mut body,
            &RuntimeControl::default(),
        )
        .unwrap();
    assert_eq!(output.selection(), selection);
    assert_eq!(output.errors().len(), 1);
    assert_eq!(output.errors()[0].selected_ordinal(), 0);
    assert_eq!(output.errors()[0].message(), "negative lambda element");
    assert!(output.values().is_null(0));
    assert_eq!(
        output
            .values()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(1),
        463
    );
    assert_eq!(body.calls, 1);
    assert_eq!(body.samples[1].1, 1);
}
#[test]
fn forged_own_errors_and_unrelated_duplicate_or_unused_child_evidence_are_internal() {
    for mode in [
        Mode::OwnError,
        Mode::WrongBody,
        Mode::WrongOuter,
        Mode::WrongDiagnostic,
        Mode::Duplicate,
        Mode::Unused,
        Mode::Null,
        Mode::WrongType,
    ] {
        let negative = matches!(
            mode,
            Mode::WrongBody
                | Mode::WrongOuter
                | Mode::WrongDiagnostic
                | Mode::Duplicate
                | Mode::Unused
        );
        let owner = owner_with(mode, Init::Good);
        let (mut instance, mut body) = instantiate(&owner);
        let columns = columns(negative);
        let args = values_args(&columns);
        let captures = values_captures(&columns);
        let rows = [1, 3];
        let selection = Selection::try_sparse(5, &rows).unwrap();
        assert!(matches!(
            instance.evaluate(
                selection,
                &args,
                &captures,
                &mut body,
                &RuntimeControl::default()
            ),
            Err(KernelFailure::Internal(_))
        ));
        let calls = body.calls;
        assert_eq!(
            instance
                .evaluate(
                    selection,
                    &args,
                    &captures,
                    &mut body,
                    &RuntimeControl::default()
                )
                .unwrap_err(),
            KernelFailure::InstanceFailed
        );
        assert_eq!(body.calls, calls);
        assert_eq!(owner.config.evaluations.load(Ordering::Relaxed), 1);
    }
}
#[test]
fn private_owner_cannot_swallow_body_originating_failures_or_replace_them_with_unrelated_errors() {
    for unrelated_error in [false, true] {
        for failure in [
            KernelFailure::Cancelled,
            KernelFailure::DeadlineExceeded,
            KernelFailure::ResourceExhausted,
            kernel_error(),
            crate::kernel_control::internal("body internal"),
        ] {
            let owner = owner_with(Mode::Swallow(unrelated_error), Init::Good);
            let (mut instance, mut body) = instantiate(&owner);
            body.mode = BodyMode::Failure(failure.clone());
            let columns = columns(false);
            let args = values_args(&columns);
            let captures = values_captures(&columns);
            let rows = [1, 3];
            let selection = Selection::try_sparse(5, &rows).unwrap();
            assert_eq!(
                instance
                    .evaluate(
                        selection,
                        &args,
                        &captures,
                        &mut body,
                        &RuntimeControl::default()
                    )
                    .unwrap_err(),
                failure
            );
            assert_eq!(
                instance
                    .evaluate(
                        selection,
                        &args,
                        &captures,
                        &mut body,
                        &RuntimeControl::default()
                    )
                    .unwrap_err(),
                KernelFailure::InstanceFailed
            );
            assert_eq!(body.calls, 1);
            assert_eq!(owner.config.evaluations.load(Ordering::Relaxed), 1);
        }
    }
}
#[test]
fn body_output_and_immutable_identity_failure_latch_entire_owner_instance() {
    for mode in [
        BodyMode::ForeignSelection,
        BodyMode::WrongType,
        BodyMode::SuccessNull,
        BodyMode::Drift(None),
        BodyMode::Drift(Some(KernelFailure::Cancelled)),
    ] {
        let owner = Owner::new(EvaluationDemand::Value);
        let (mut instance, mut body) = instantiate(&owner);
        body.mode = mode;
        let columns = columns(false);
        let args = values_args(&columns);
        let captures = values_captures(&columns);
        let rows = [1, 3];
        let selection = Selection::try_sparse(5, &rows).unwrap();
        let error = instance
            .evaluate(
                selection,
                &args,
                &captures,
                &mut body,
                &RuntimeControl::default(),
            )
            .unwrap_err();
        if matches!(body.mode, BodyMode::Drift(Some(_))) {
            assert_eq!(error, KernelFailure::Cancelled);
        } else {
            assert!(matches!(error, KernelFailure::Internal(_)));
        }
        assert_eq!(
            instance
                .evaluate(
                    selection,
                    &args,
                    &captures,
                    &mut body,
                    &RuntimeControl::default()
                )
                .unwrap_err(),
            KernelFailure::InstanceFailed
        );
        assert_eq!(body.calls, 1);
    }
}
#[test]
fn initialization_cleanup_retained_limits_metadata_and_overflow_are_checked_before_publish() {
    for init in [
        Init::Failure(kernel_error()),
        Init::Failure(KernelFailure::Cancelled),
        Init::Grow,
        Init::Drift,
    ] {
        let owner = owner_with(Mode::Good, init.clone());
        let prepared = owner.fresh().into_prepared();
        let error =
            HigherOrderEvaluationInstance::instantiate(prepared, &RuntimeControl::default())
                .err()
                .unwrap();
        match init {
            Init::Failure(expected) => assert_eq!(error, expected),
            _ => assert!(matches!(error, KernelFailure::Internal(_))),
        }
        assert_eq!(owner.config.creates.load(Ordering::Relaxed), 1);
        assert_eq!(owner.config.drops.load(Ordering::Relaxed), 1);
    }
    let owner = Owner::new(EvaluationDemand::Value);
    owner.config.bound.store(usize::MAX, Ordering::Relaxed);
    assert_eq!(
        HigherOrderEvaluationInstance::instantiate(
            owner.fresh().into_prepared(),
            &RuntimeControl::default()
        )
        .err()
        .unwrap(),
        KernelFailure::ResourceExhausted
    );
    assert_eq!(owner.config.creates.load(Ordering::Relaxed), 0);
}
#[test]
fn retained_success_error_growth_and_immutable_bound_drift_preserve_primary_controls() {
    for mode in [
        Mode::Grow(None),
        Mode::Grow(Some(kernel_error())),
        Mode::Drift(None),
        Mode::Drift(Some(kernel_error())),
        Mode::Failure(kernel_error()),
        Mode::Grow(Some(actual_lifecycle_source())),
        Mode::Drift(Some(actual_lifecycle_source())),
    ] {
        let owner = owner_with(mode.clone(), Init::Good);
        let (mut instance, mut body) = instantiate(&owner);
        let columns = columns(false);
        let args = values_args(&columns);
        let captures = values_captures(&columns);
        let rows = [1, 3];
        let selection = Selection::try_sparse(5, &rows).unwrap();
        let error = instance
            .evaluate(
                selection,
                &args,
                &captures,
                &mut body,
                &RuntimeControl::default(),
            )
            .unwrap_err();
        match &mode {
            Mode::Failure(original) | Mode::Grow(Some(original)) | Mode::Drift(Some(original)) => {
                assert_eq!(&error, original)
            }
            _ => assert!(matches!(error, KernelFailure::Internal(_))),
        }
        assert_eq!(
            instance
                .evaluate(
                    selection,
                    &args,
                    &captures,
                    &mut body,
                    &RuntimeControl::default()
                )
                .unwrap_err(),
            KernelFailure::InstanceFailed
        );
        drop(instance);
        assert_eq!(owner.config.drops.load(Ordering::Relaxed), 1);
    }
    for failure in [
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
    ] {
        for mode in [
            Mode::Grow(Some(failure.clone())),
            Mode::Drift(Some(failure.clone())),
        ] {
            let owner = owner_with(mode, Init::Good);
            let (mut instance, mut body) = instantiate(&owner);
            let columns = columns(false);
            let args = values_args(&columns);
            let captures = values_captures(&columns);
            let rows = [1, 3];
            let selection = Selection::try_sparse(5, &rows).unwrap();
            assert_eq!(
                instance
                    .evaluate(
                        selection,
                        &args,
                        &captures,
                        &mut body,
                        &RuntimeControl::default()
                    )
                    .unwrap_err(),
                failure
            );
            drop(instance);
            assert_eq!(owner.config.drops.load(Ordering::Relaxed), 1);
        }
    }
}

struct AfterCreateControl {
    config: Arc<Config>,
    failure: KernelFailure,
}
impl KernelEvaluationControl for AfterCreateControl {
    fn checkpoint(&self, _: u32) -> Result<(), KernelFailure> {
        if self.config.creates.load(Ordering::Relaxed) > 0 {
            Err(self.failure.clone())
        } else {
            Ok(())
        }
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("no wait")
    }
}
struct BodyQuantumControl {
    entered: Arc<AtomicUsize>,
    failure: KernelFailure,
    work: Mutex<Vec<u32>>,
}
impl KernelEvaluationControl for BodyQuantumControl {
    fn checkpoint(&self, work: u32) -> Result<(), KernelFailure> {
        assert!(work <= 256);
        self.work.lock().unwrap().push(work);
        if self.entered.load(Ordering::Relaxed) > 0 && work == 256 {
            Err(self.failure.clone())
        } else {
            Ok(())
        }
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("no wait")
    }
}
struct CompileQuantumControl(CompileControlError);
impl PureCompileControl for CompileQuantumControl {
    fn checkpoint(&self, phase: CompilePhase, work: u32) -> Result<(), CompileControlError> {
        assert_eq!(phase, CompilePhase::FunctionSpecialization);
        assert!(work <= 256);
        if work == 256 {
            Err(self.0.clone())
        } else {
            Ok(())
        }
    }
}
#[test]
fn compile_capture_quantum_and_initialization_entry_postcreate_controls_preserve_categories() {
    for (compile_failure, runtime_failure) in [
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
        let owner = Owner::new(EvaluationDemand::Value);
        let mut options = owner.options();
        options.capture_types = vec![i32_type(false); 300].into_boxed_slice();
        assert!(
            matches!(specialize_higher_order(&owner,owner.input(),owner.selected.clone(),options,&CompileQuantumControl(compile_failure.clone())),Err(FunctionSpecializationFailure::Control(error)) if error==compile_failure)
        );
        assert_eq!(owner.prepares.load(Ordering::Relaxed), 0);
        let owner = Owner::new(EvaluationDemand::Value);
        let prepared = owner.fresh().into_prepared();
        let control = RuntimeControl {
            failure: Some(runtime_failure.clone()),
            ..Default::default()
        };
        assert_eq!(
            HigherOrderEvaluationInstance::instantiate(prepared.clone(), &control)
                .err()
                .unwrap(),
            runtime_failure
        );
        assert_eq!(owner.config.creates.load(Ordering::Relaxed), 0);
        let control = AfterCreateControl {
            config: owner.config.clone(),
            failure: runtime_failure.clone(),
        };
        assert_eq!(
            HigherOrderEvaluationInstance::instantiate(prepared, &control)
                .err()
                .unwrap(),
            runtime_failure
        );
        assert_eq!(owner.config.creates.load(Ordering::Relaxed), 1);
        assert_eq!(owner.config.drops.load(Ordering::Relaxed), 1);
    }
}
#[test]
fn positive_private_body_quantum_control_survives_owner_swallow_and_latches_whole_instance() {
    let columns: [ArrayRef; 4] = [
        Arc::new(Int64Array::from(vec![1; 300])),
        Arc::new(Int32Array::from(vec![2; 300])),
        Arc::new(Int32Array::from(vec![3; 300])),
        Arc::new(Int64Array::from(vec![4])),
    ];
    let args = values_args(&columns);
    let captures = values_captures(&columns);
    let rows = (0..150).map(|row| row * 2).collect::<Vec<_>>();
    let selection = Selection::try_sparse(300, &rows).unwrap();
    for failure in [
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
    ] {
        let owner = owner_with(Mode::Swallow(true), Init::Good);
        let (mut instance, mut body) = instantiate(&owner);
        let control = BodyQuantumControl {
            entered: body.entered.clone(),
            failure: failure.clone(),
            work: Mutex::default(),
        };
        assert_eq!(
            instance
                .evaluate(selection, &args, &captures, &mut body, &control)
                .unwrap_err(),
            failure
        );
        assert_eq!(body.calls, 1);
        assert_eq!(body.samples.len(), 256);
        assert!(control.work.lock().unwrap().contains(&256));
        assert_eq!(
            instance
                .evaluate(
                    selection,
                    &args,
                    &captures,
                    &mut body,
                    &RuntimeControl::default()
                )
                .unwrap_err(),
            KernelFailure::InstanceFailed
        );
        assert_eq!(body.calls, 1);
        assert_eq!(owner.config.evaluations.load(Ordering::Relaxed), 1);
    }
}
#[test]
fn selected_capture_null_and_full_argument_quantum_fail_before_owner_private_work() {
    let columns = columns(false);
    let args = values_args(&columns);
    let null: ArrayRef = Arc::new(Int32Array::from(vec![
        Some(0),
        None,
        Some(0),
        Some(20),
        Some(0),
    ]));
    let captures = [
        EvaluatedArgument::Column(&null),
        EvaluatedArgument::Scalar(&columns[3]),
    ];
    let rows = [1, 3];
    let selection = Selection::try_sparse(5, &rows).unwrap();
    let owner = Owner::new(EvaluationDemand::Value);
    let (mut instance, mut body) = instantiate(&owner);
    assert!(matches!(
        instance.evaluate(
            selection,
            &args,
            &captures,
            &mut body,
            &RuntimeControl::default()
        ),
        Err(KernelFailure::InvalidProgram(_))
    ));
    assert_eq!(owner.config.evaluations.load(Ordering::Relaxed), 0);
    assert_eq!(body.calls, 0);
    let large: [ArrayRef; 4] = [
        Arc::new(Int64Array::from(vec![1; 300])),
        Arc::new(Int32Array::from(vec![2; 300])),
        Arc::new(Int32Array::from(vec![3; 300])),
        Arc::new(Int64Array::from(vec![4])),
    ];
    let args = values_args(&large);
    let captures = values_captures(&large);
    for failure in [
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
    ] {
        for positive_only in [false, true] {
            let owner = Owner::new(EvaluationDemand::Value);
            let (mut instance, mut body) = instantiate(&owner);
            let control = RuntimeControl {
                failure: Some(failure.clone()),
                positive_only,
                work: Mutex::default(),
            };
            assert_eq!(
                instance
                    .evaluate(Selection::all(300), &args, &captures, &mut body, &control)
                    .unwrap_err(),
                failure
            );
            assert_eq!(owner.config.evaluations.load(Ordering::Relaxed), 0);
            assert_eq!(body.calls, 0);
            if positive_only {
                assert!(control.work.lock().unwrap().contains(&256));
            } else {
                assert_eq!(*control.work.lock().unwrap(), [0]);
            }
        }
    }
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

#[test]
fn actual_source_higher_order_error_has_no_late_cancel_callback_or_replay() {
    struct LaterCancel<'a> {
        config: &'a Config,
        after: AtomicUsize,
    }
    impl KernelEvaluationControl for LaterCancel<'_> {
        fn checkpoint(&self, _: u32) -> Result<(), KernelFailure> {
            if self.config.evaluations.load(Ordering::Relaxed) != 0 {
                self.after.fetch_add(1, Ordering::Relaxed);
                Err(KernelFailure::Cancelled)
            } else {
                Ok(())
            }
        }
        fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
            panic!("no higher-order wait")
        }
    }
    let original = actual_lifecycle_source();
    let owner = owner_with(Mode::Failure(original.clone()), Init::Good);
    let (mut instance, mut body) = instantiate(&owner);
    let columns = columns(false);
    let arguments = values_args(&columns);
    let captures = values_captures(&columns);
    let selection = Selection::try_sparse(5, &[1, 3]).unwrap();
    let late = LaterCancel {
        config: &owner.config,
        after: AtomicUsize::new(0),
    };
    assert_eq!(
        instance
            .evaluate(selection, &arguments, &captures, &mut body, &late)
            .unwrap_err(),
        original
    );
    assert_eq!(late.after.load(Ordering::Relaxed), 0);
    assert_eq!(
        instance
            .evaluate(selection, &arguments, &captures, &mut body, &late)
            .unwrap_err(),
        KernelFailure::InstanceFailed
    );
    assert_eq!(owner.config.evaluations.load(Ordering::Relaxed), 1);
    drop(instance);
    assert_eq!(owner.config.drops.load(Ordering::Relaxed), 1);
}
