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

//! Original selected-owner fixture migrated from resolved_calls expression tests.
use super::*;
use arrow::array::{BooleanArray, Int64Array};
use novarocks_expression_contract::UnitError;
use novarocks_functions::*;
use std::sync::{
    Mutex,
    atomic::{AtomicUsize, Ordering},
};
const FUNCTION: &str = "fixture/local-resolved/function-v1";
const TYPE_FUNCTION: &str = "fixture/local-resolved/type-function-v1";
const HIGHER_FUNCTION: &str = "fixture/local-resolved/higher-function-v1";
const CONTROL_FUNCTION: &str = "fixture/local-resolved/control-v1";
const SCALAR: &str = "fixture/local-resolved/compare-v1";
const STRICT: &str = "fixture/local-resolved/strict-compare-v1";
const ORDERED: &str = "fixture/local-resolved/ordered-compare-v1";
const TYPE_ONLY: &str = "fixture/local-resolved/type-v1";
const HIGHER: &str = "fixture/local-resolved/higher-v1";
const IF: &str = "fixture/local-resolved/if-v1";

fn fid(value: &str) -> FunctionId {
    FunctionId::try_new(value).unwrap()
}
fn oid(value: &str) -> FunctionOverloadId {
    FunctionOverloadId::try_new(value).unwrap()
}
fn code(value: &str) -> PureImplementationId {
    PureImplementationId::try_new(value).unwrap()
}
fn result_type() -> FunctionValueType {
    FunctionValueType::new(DataType::Boolean, true)
}
fn input_type() -> FunctionValueType {
    FunctionValueType::new(DataType::Int64, false)
}
fn effects(control: ArgumentControl) -> FunctionEffectDeclaration {
    FunctionEffectDeclaration {
        value_stability: FunctionVolatility::Immutable,
        own_row_error: if control == ArgumentControl::OrderedNull {
            FunctionIntrinsicRowError::MayRaise
        } else {
            FunctionIntrinsicRowError::NoRowError
        },
        failure_behavior: FunctionFailureBehavior::Propagate,
        null_behavior: if matches!(
            control,
            ArgumentControl::If
                | ArgumentControl::OrderedNull
                | ArgumentControl::HigherOrder { .. }
        ) {
            FunctionNullBehavior::ControlDefined
        } else {
            FunctionNullBehavior::CalledOnNull
        },
        argument_control: control,
        instance_state: FunctionInstanceState::None,
        observable_effects: ObservableEffects::NONE,
        environment_dependencies: Box::default(),
    }
}
fn argument_types(overload: &FunctionOverloadId) -> Vec<FunctionArgument> {
    let value = || FunctionArgument::Value {
        value_type: input_type(),
        constant: None,
    };
    if *overload == oid(STRICT) || *overload == oid(ORDERED) {
        vec![
            FunctionArgument::Value {
                value_type: input_type().with_nullable(true).unwrap(),
                constant: None,
            };
            2
        ]
    } else if *overload == oid(HIGHER) {
        vec![
            value(),
            FunctionArgument::Lambda {
                parameter_types: novarocks_type_contract::FunctionParameterTypes::from_pure_boxed(
                    Box::from([input_type()]),
                ),
                result_type: result_type(),
            },
        ]
    } else if *overload == oid(IF) {
        vec![
            FunctionArgument::Value {
                value_type: result_type(),
                constant: None
            };
            3
        ]
    } else {
        vec![value(), value()]
    }
}
struct Owner {
    declaration: FunctionBindingDeclaration,
    implementations: Vec<PureImplementationDeclaration>,
    instances: Arc<AtomicUsize>,
    ordered_rows: Arc<Mutex<Vec<Vec<usize>>>>,
    explicit_type_only: Option<[FunctionValueType; 2]>,
    expected_constraint: Option<FunctionValueType>,
    explicit_higher_parameters: Option<Box<[FunctionValueType]>>,
}
impl Owner {
    fn for_overload(overload: &str) -> Self {
        let (function, control, abi) = match overload {
            SCALAR | STRICT => (FUNCTION, ArgumentControl::Eager, PureKernelAbi::ScalarV1),
            ORDERED => (
                FUNCTION,
                ArgumentControl::OrderedNull,
                PureKernelAbi::ScalarV1,
            ),
            TYPE_ONLY => (
                TYPE_FUNCTION,
                ArgumentControl::TypeOnly,
                PureKernelAbi::ScalarV1,
            ),
            HIGHER => (
                HIGHER_FUNCTION,
                ArgumentControl::HigherOrder {
                    body_ordinal: 1,
                    body_demand: EvaluationDemand::Value,
                },
                PureKernelAbi::HigherOrderV1,
            ),
            IF => (
                CONTROL_FUNCTION,
                ArgumentControl::If,
                PureKernelAbi::ControlIntrinsicV1,
            ),
            _ => unreachable!("fixture exact owner"),
        };
        let controls = [(overload, control, abi)];
        Self {
            declaration: FunctionBindingDeclaration::try_new_complete(
                fid(function),
                FunctionKind::Scalar,
                controls.iter().map(|(overload, control, _)| {
                    let mut declared = effects(*control);
                    if *overload == STRICT {
                        declared.null_behavior = FunctionNullBehavior::Strict;
                    }
                    FunctionOverloadDeclaration::from_effects(
                        oid(overload),
                        "fixture exact channels",
                        "BOOLEAN",
                        None,
                        declared,
                    )
                }),
            )
            .unwrap(),
            implementations: controls
                .iter()
                .map(|(overload, _, abi)| PureImplementationDeclaration {
                    overload: oid(overload),
                    implementation: code(overload),
                    abi: *abi,
                })
                .collect(),
            instances: Arc::new(AtomicUsize::new(0)),
            ordered_rows: Arc::new(Mutex::new(Vec::new())),
            explicit_type_only: None,
            expected_constraint: None,
            explicit_higher_parameters: None,
        }
    }
    fn arguments(&self, overload: &FunctionOverloadId) -> Vec<FunctionArgument> {
        if *overload == oid(TYPE_ONLY)
            && let Some(types) = &self.explicit_type_only
        {
            types
                .iter()
                .map(|ty| FunctionArgument::Value {
                    value_type: ty.clone(),
                    constant: None,
                })
                .collect()
        } else if *overload == oid(HIGHER)
            && let Some(types) = &self.explicit_higher_parameters
        {
            vec![
                FunctionArgument::Value {
                    value_type: input_type(),
                    constant: None,
                },
                FunctionArgument::Lambda {
                    parameter_types:
                        novarocks_type_contract::FunctionParameterTypes::from_pure_boxed(
                            types.clone(),
                        ),
                    result_type: result_type(),
                },
            ]
        } else {
            argument_types(overload)
        }
    }
    fn frozen(
        &self,
        context: ExpressionEffectContext,
        selected: &FunctionBindingSelection,
    ) -> CallEffects {
        let base = self
            .declaration
            .effect_declaration(&selected.overload)
            .unwrap();
        CallEffects {
            value_stability: base.value_stability,
            own_row_error: base.own_row_error,
            failure_behavior: base.failure_behavior,
            null_behavior: base.null_behavior,
            argument_control: base.argument_control,
            instance_state: base.instance_state,
            observable_effects: base.observable_effects,
            environment: Box::default(),
            proof_scope: CallProofScope::Region(context.region),
        }
    }
}
impl PureFunctionMetadataOwner for Owner {
    fn binding_declaration(&self) -> &FunctionBindingDeclaration {
        &self.declaration
    }
    fn implementation_declarations(&self) -> &[PureImplementationDeclaration] {
        &self.implementations
    }
}
impl FunctionBindingResolver for Owner {
    fn resolve(
        &self,
        request: FunctionBindingRequest<'_>,
        _control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<FunctionBindingSelection, FunctionBindingError> {
        let overload = self
            .declaration
            .overloads()
            .first()
            .unwrap()
            .identity
            .clone();
        let selected = FunctionBindingSelection {
            overload,
            argument_types: novarocks_type_contract::FunctionArgumentTypes::from_pure_boxed(
                request
                    .arguments
                    .iter()
                    .map(FunctionArgument::argument_type)
                    .collect::<Box<[_]>>(),
            ),
            result_type: FunctionResultType::Scalar(result_type()),
            aggregate: None,
        };
        self.validate_selected(&selected, request, _control)?;
        Ok(selected)
    }
    fn select_at_overload_observed(
        &self,
        overload: &FunctionOverloadId,
        request: FunctionBindingRequest<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<FunctionBindingSelection, FunctionBindingError> {
        self.declaration.effect_declaration(overload)?;
        let selected = self.resolve(request, control)?;
        if &selected.overload != overload {
            return Err(FunctionBindingError::InvalidBinding(
                "fixture selected a different overload".into(),
            ));
        }
        Ok(selected)
    }
    fn validate_selected(
        &self,
        selected: &FunctionBindingSelection,
        request: FunctionBindingRequest<'_>,
        _control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<(), FunctionBindingError> {
        self.declaration.effect_declaration(&selected.overload)?;
        let expected = self.arguments(&selected.overload);
        let mut arguments_equal = request.arguments.len() == expected.len();
        if arguments_equal {
            for (actual, expected) in request.arguments.iter().zip(&expected) {
                if !actual.equals_observed(
                    expected,
                    novarocks_type_contract::CompilePhase::Validate,
                    _control,
                )? {
                    arguments_equal = false;
                    break;
                }
            }
        }
        if !arguments_equal
            || request.expected_result_type != self.expected_constraint.as_ref()
            || request.logical_argument_count != expected.len()
            || selected.argument_types.as_slice()
                != expected
                    .iter()
                    .map(FunctionArgument::argument_type)
                    .collect::<Box<[_]>>()
                    .as_ref()
            || selected.result_type != FunctionResultType::Scalar(result_type())
            || selected.aggregate.is_some()
        {
            return Err(FunctionBindingError::InvalidBinding(
                "fixture signature differs".into(),
            ));
        }
        Ok(())
    }
}
impl FunctionEffectOwner for Owner {
    type Error = FunctionBindingError;
    fn declaration(
        &self,
        function: &FunctionId,
        selected: &FunctionBindingSelection,
    ) -> Result<&FunctionEffectDeclaration, Self::Error> {
        if function != self.declaration.function_id() {
            return Err(FunctionBindingError::UnknownFunction);
        }
        self.declaration.effect_declaration(&selected.overload)
    }
    fn validate_and_refine(
        &self,
        input: CallEffectInput<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<CallEffects, FunctionEffectOwnerError<Self::Error>> {
        control
            .checkpoint(CompilePhase::FunctionSpecialization, 0)
            .map_err(FunctionEffectOwnerError::Control)?;
        self.declaration(input.function_id, input.selected)?;
        self.validate_selected(input.selected, input.request, control)?;
        if !input.environment.is_empty() {
            return Err(
                FunctionBindingError::InvalidBinding("fixture has no environment".into()).into(),
            );
        }
        let CallEffectContext::Expression(context) = input.context else {
            return Err(FunctionBindingError::InvalidBinding(
                "fixture requires expression context".into(),
            )
            .into());
        };
        Ok(self.frozen(context, input.selected))
    }
}
impl PureScalarImplementation for Owner {
    fn prepare_scalar(
        &self,
        _: CallEffectInput<'_>,
        contract: Arc<ScalarCallContract>,
        _: &dyn PureCompileControl,
    ) -> Result<Arc<dyn PreparedScalarKernel>, KernelFailure> {
        Ok(Arc::new(Scalar {
            contract,
            instances: self.instances.clone(),
            ordered_rows: self.ordered_rows.clone(),
        }))
    }
}
impl PureHigherOrderImplementation for Owner {
    fn prepare_higher_order(
        &self,
        _: CallEffectInput<'_>,
        call: Arc<HigherOrderCallContract>,
        body: Arc<LambdaBodyContract>,
        _: &dyn PureCompileControl,
    ) -> Result<Arc<dyn PreparedHigherOrderKernel>, KernelFailure> {
        Ok(Arc::new(PreparedLambdaFixture { call, body }))
    }
}

#[derive(Debug)]
struct PreparedLambdaFixture {
    call: Arc<HigherOrderCallContract>,
    body: Arc<LambdaBodyContract>,
}

impl PreparedHigherOrderKernel for PreparedLambdaFixture {
    fn contract(&self) -> &Arc<HigherOrderCallContract> {
        &self.call
    }

    fn body_contract(&self) -> &Arc<LambdaBodyContract> {
        &self.body
    }

    fn instance_retained_upper_bound(&self) -> usize {
        0
    }

    fn create_instance(
        &self,
        _: &dyn KernelEvaluationControl,
    ) -> Result<Box<dyn HigherOrderKernelInstance>, KernelFailure> {
        panic!("the compilation fixture must not create a runtime instance")
    }
}
#[derive(Debug)]
struct Scalar {
    contract: Arc<ScalarCallContract>,
    instances: Arc<AtomicUsize>,
    ordered_rows: Arc<Mutex<Vec<Vec<usize>>>>,
}
impl PreparedScalarKernel for Scalar {
    fn contract(&self) -> &Arc<ScalarCallContract> {
        &self.contract
    }
    fn instance_retained_upper_bound(&self) -> usize {
        0
    }
    fn create_instance(&self) -> Result<Box<dyn ScalarKernelInstance>, KernelFailure> {
        self.instances.fetch_add(1, Ordering::Relaxed);
        Ok(Box::new(ScalarInstance {
            ordered_rows: self.ordered_rows.clone(),
        }))
    }
}
struct ScalarInstance {
    ordered_rows: Arc<Mutex<Vec<Vec<usize>>>>,
}
impl ScalarKernelInstance for ScalarInstance {
    fn retained_bytes(&self) -> usize {
        0
    }
    fn evaluate<'input>(
        &mut self,
        input: ScalarCallInput<'_, 'input>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<SelectedValues<'input>, KernelFailure> {
        let ordered = input.contract().effects().argument_control == ArgumentControl::OrderedNull;
        if ordered {
            self.ordered_rows
                .lock()
                .unwrap()
                .push(input.selection().iter().collect());
        }
        let mut output = Vec::new();
        let mut row_errors = Vec::new();
        for ordinal in 0..input.selection().len() {
            control.checkpoint(1)?;
            if input.contract().effects().argument_control == ArgumentControl::TypeOnly {
                output.push(Some(true));
                continue;
            }
            let read = |argument: EvaluatedArgument<'_>| {
                let array = argument
                    .array()
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                let row = input.selection().row(ordinal).unwrap();
                let physical_row = argument.value_row(ordinal, row);
                if ordered {
                    use arrow::array::Array;
                    assert!(!array.is_null(physical_row));
                }
                array.value(physical_row)
            };
            let left = read(input.arguments()[0]);
            if ordered && left == 98 {
                return Err(KernelFailure::Cancelled);
            }
            if ordered && left == 99 {
                return Err(KernelFailure::ResourceExhausted);
            }
            if ordered && left == 97 {
                row_errors.push(RowDataError::with_reason(
                    ordinal,
                    DataFailureReason::Domain(
                        novarocks_failure_contract::kernel_reason::INVALID_FUNCTION_ARGUMENT,
                    ),
                    "fixture ordered leaf refused its value",
                ));
                output.push(None);
            } else {
                output.push(Some(left > read(input.arguments()[1])));
            }
        }
        SelectedValues::try_new(
            input.selection(),
            &DataType::Boolean,
            Arc::new(BooleanArray::from(output)),
            row_errors.into_boxed_slice(),
        )
        .map_err(|_| KernelFailure::Internal(KernelDiagnostic::new("fixture output differs")))
    }
}

fn catalog(owner: Arc<Owner>) -> PureEngineFunctionCatalog {
    let mut builder = EngineFunctionCatalogBuilder::new();
    let definition = match owner.implementations[0].abi {
        PureKernelAbi::HigherOrderV1 => FunctionDefinition::try_new_pure_higher_order(
            "unit_fixture",
            FunctionVisibility::Public,
            owner.clone(),
        ),
        PureKernelAbi::ControlIntrinsicV1 => FunctionDefinition::try_new_pure_control(
            "unit_fixture",
            FunctionVisibility::Public,
            owner.clone(),
        ),
        _ => FunctionDefinition::try_new_pure_scalar(
            "unit_fixture",
            FunctionVisibility::Public,
            owner.clone(),
        ),
    }
    .unwrap();
    builder.register(definition).unwrap();
    let mut installed = owner
        .implementations
        .iter()
        .cloned()
        .map(|implementation| InstalledPureKernel {
            function: owner.declaration.function_id().clone(),
            kind: FunctionKind::Scalar,
            implementation,
            aggregate_state_format: None,
        })
        .collect::<Vec<_>>();
    let additional_builtin = if owner.implementations[0].overload == oid(ORDERED) {
        Some("try")
    } else if owner.implementations[0].overload == oid(HIGHER) {
        Some("rand")
    } else {
        None
    };
    if let Some(name) = additional_builtin {
        let builtin = novarocks_functions::testkit::shared_installed_builtin_owner_catalogue();
        let metadata = builtin.metadata();
        let definition = metadata.definition(name, FunctionKind::Scalar).unwrap();
        let declaration = definition.binding_declaration().unwrap();
        for overload in declaration.overloads() {
            let prepared = metadata
                .pure_overload_declaration_observed(
                    declaration.function_id(),
                    declaration.kind(),
                    &overload.identity,
                    &Control::good(),
                )
                .unwrap();
            installed.push(InstalledPureKernel {
                function: declaration.function_id().clone(),
                kind: declaration.kind(),
                implementation: prepared.implementation().clone(),
                aggregate_state_format: None,
            });
        }
        builder.register(definition.clone()).unwrap();
    }
    builder.seal_pure(installed).unwrap()
}

pub(super) fn lambda_compilation_fixture() -> (
    BoundFunction,
    PhysicalCallRequest,
    PureEngineFunctionCatalog,
) {
    let owner = Arc::new(Owner::for_overload(HIGHER));
    let overload = oid(HIGHER);
    let arguments = owner.arguments(&overload);
    let binding = BoundFunction::from_exact_signature(
        owner.declaration.function_id().clone(),
        overload,
        FunctionKind::Scalar,
        FunctionArgumentTypes::from_pure_boxed(
            arguments
                .iter()
                .map(FunctionArgument::argument_type)
                .collect(),
        ),
        result_type(),
    );
    let request = PhysicalCallRequest {
        logical_argument_count: arguments.len(),
        arguments: arguments
            .into_iter()
            .map(|argument| match argument {
                FunctionArgument::Value {
                    value_type,
                    constant: None,
                } => novarocks_function_contract::FunctionArgument::Value {
                    value_type,
                    constant: None,
                },
                FunctionArgument::Lambda {
                    parameter_types,
                    result_type,
                } => novarocks_function_contract::FunctionArgument::Lambda {
                    parameter_types,
                    result_type,
                },
                _ => panic!("fixture uses only original nonconstant channels"),
            })
            .collect(),
        expected_result_type: None,
        constant_policy: policy(),
    };
    (binding, request, catalog(owner))
}
fn unit(owner: &Owner, control: ArgumentControl, hidden_call: bool) -> BoundExprSet {
    let mut builder = builder();
    let a = builder
        .add(None, input_type(), ExprKind::Value(ValueId::new(1)))
        .unwrap();
    let b = builder
        .add(None, input_type(), ExprKind::Value(ValueId::new(2)))
        .unwrap();
    let a = if hidden_call {
        let call = builder
            .add(
                None,
                input_type(),
                ExprKind::FunctionCall {
                    function: BoundFunction::from_exact_signature(
                        fid("fixture/uninstalled/dead"),
                        oid("fixture/uninstalled/dead/i64"),
                        FunctionKind::Scalar,
                        FunctionArgumentTypes::from_pure_boxed(Box::new([
                            FunctionArgumentType::Value(input_type()),
                        ])),
                        input_type(),
                    ),
                    args: Box::new([a]),
                },
            )
            .unwrap();
        builder
            .request(
                call,
                PhysicalCallRequest {
                    arguments: Box::new([novarocks_function_contract::FunctionArgument::Value {
                        value_type: input_type(),
                        constant: None,
                    }]),
                    logical_argument_count: 1,
                    expected_result_type: None,
                    constant_policy: policy(),
                },
            )
            .unwrap();
        builder
            .definition_control(
                call,
                DefinitionControl {
                    shape: ControlShape::Eager,
                    temporal_sources: None,
                },
            )
            .unwrap();
        call
    } else {
        a
    };
    let function = BoundFunction::from_exact_signature(
        owner.declaration.function_id().clone(),
        owner.implementations[0].overload.clone(),
        FunctionKind::Scalar,
        FunctionArgumentTypes::from_pure_boxed(Box::new([
            FunctionArgumentType::Value(input_type()),
            FunctionArgumentType::Value(input_type()),
        ])),
        result_type(),
    );
    let call = builder
        .add(
            None,
            result_type(),
            ExprKind::FunctionCall {
                function,
                args: Box::new([a, b]),
            },
        )
        .unwrap();
    builder
        .request(
            call,
            PhysicalCallRequest {
                arguments: owner
                    .arguments(&owner.implementations[0].overload)
                    .into_iter()
                    .map(|argument| match argument {
                        FunctionArgument::Value {
                            value_type,
                            constant: None,
                        } => novarocks_function_contract::FunctionArgument::Value {
                            value_type,
                            constant: None,
                        },
                        _ => panic!("fixture request has only original nonconstant value channels"),
                    })
                    .collect::<Box<[_]>>(),
                logical_argument_count: 2,
                expected_result_type: owner.expected_constraint.clone(),
                constant_policy: policy(),
            },
        )
        .unwrap();
    builder
        .definition_control(
            call,
            DefinitionControl {
                shape: if control == ArgumentControl::TypeOnly {
                    ControlShape::TypeOnly
                } else {
                    ControlShape::Eager
                },
                temporal_sources: None,
            },
        )
        .unwrap();
    for _ in 0..2 {
        builder
            .root(call, EvaluationDemand::Value, RootScope::Port(0))
            .unwrap();
    }
    let c = Control::good();
    let mut work = CompileCheckpoints::try_new(&c, CompilePhase::Validate).unwrap();
    let sealed = builder
        .seal(
            vec![ExprPort {
                values: Box::new([
                    (ValueId::new(1), input_type()),
                    (ValueId::new(2), input_type()),
                ]),
            }],
            &mut work,
        )
        .unwrap();
    let selected = FunctionBindingSelection {
        overload: owner.implementations[0].overload.clone(),
        argument_types: FunctionArgumentTypes::from_pure_boxed(Box::new([
            FunctionArgumentType::Value(input_type()),
            FunctionArgumentType::Value(input_type()),
        ])),
        result_type: FunctionResultType::Scalar(result_type()),
        aggregate: None,
    };
    let claims = sealed
        .uses()
        .iter()
        .filter(|(_, u)| u.definition == call)
        .map(|(&id, u)| {
            (
                id,
                ExpressionCallClaim {
                    context: u.context,
                    effects: owner.frozen(u.context, &selected),
                    decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
                    temporal_source: None,
                },
            )
        })
        .collect();
    let unit = sealed.finish(claims, &mut work).unwrap();
    work.finish().unwrap();
    unit
}
#[test]
fn frozen_scalar_and_type_only_keep_original_owner_templates_context_and_no_instances() {
    for (overload, control) in [
        (SCALAR, ArgumentControl::Eager),
        (TYPE_ONLY, ArgumentControl::TypeOnly),
    ] {
        let owner = Arc::new(Owner::for_overload(overload));
        let unit = unit(&owner, control, false);
        let catalog = self::catalog(owner.clone());
        let scopes = [layouts(&[
            (ValueId::new(1), input_type()),
            (ValueId::new(2), input_type()),
        ])];
        let c = Control::good();
        let mut work = CompileCheckpoints::try_new(&c, CompilePhase::LowerProgram).unwrap();
        let p = compile_expr_set_pure(
            &unit,
            EvaluationEnvironment {
                constants: &ConstantPools::empty(),
                parameters: &SemanticParameters::default(),
            },
            ScopeLayouts {
                ports: &scopes,
                all_ports: None,
            },
            policy(),
            &catalog,
            &mut work,
        )
        .unwrap();
        work.finish().unwrap();
        assert_eq!(owner.instances.load(Ordering::Relaxed), 0);
        assert_eq!(p.calls().len(), 2);
        for (ordinal, root) in p.roots().iter().enumerate() {
            let call = &p.calls()[&root.use_id];
            assert_eq!(call.source(), PurePreparationSource::Frozen);
            assert_eq!(
                call.call_contract().context(),
                CallEffectContext::Expression(unit.control_flow().uses()[&root.use_id].context)
            );
            assert_eq!(call.implementation(), &owner.implementations[0]);
            let evaluator_view::UnitStateTemplate::Scalar { scope, kernel } =
                evaluator_view::state_template(&p, root.use_id).unwrap()
            else {
                panic!("actual scalar")
            };
            assert_eq!(scope.root, RootOrdinal::new(ordinal as u32));
            assert_eq!(scope.occurrence, root.use_id);
            let PreparedPureKernel::Scalar(original) = call.prepared() else {
                panic!("scalar")
            };
            assert!(Arc::ptr_eq(kernel, original));
            assert_eq!(kernel.contract().as_ref().call(), call.call_contract());
            assert_eq!(
                p.control_flow().uses()[&root.use_id].arguments.len(),
                if control == ArgumentControl::TypeOnly {
                    0
                } else {
                    2
                }
            );
        }
        if control == ArgumentControl::TypeOnly {
            assert!(p.input(ExpressionUseId::new(0)).is_none());
            assert_eq!(p.definitions().nodes().len(), 3);
            assert_eq!(p.types().len(), 3);
        }
    }
}

struct Runtime;
impl KernelEvaluationControl for Runtime {
    fn checkpoint(&self, _: u32) -> Result<(), KernelFailure> {
        Ok(())
    }
    fn wait(&self, _: std::time::Duration) -> Result<(), KernelFailure> {
        Ok(())
    }
}
#[test]
fn prepared_scalar_view_runs_original_kernel_and_type_only_dead_calls_still_validate() {
    let owner = Arc::new(Owner::for_overload(SCALAR));
    let u = unit(&owner, ArgumentControl::Eager, false);
    let catalog = self::catalog(owner.clone());
    let scopes = [layouts(&[
        (ValueId::new(1), input_type()),
        (ValueId::new(2), input_type()),
    ])];
    let c = Control::good();
    let mut w = CompileCheckpoints::try_new(&c, CompilePhase::LowerProgram).unwrap();
    let p = compile_expr_set_pure(
        &u,
        EvaluationEnvironment {
            constants: &ConstantPools::empty(),
            parameters: &SemanticParameters::default(),
        },
        ScopeLayouts {
            ports: &scopes,
            all_ports: None,
        },
        policy(),
        &catalog,
        &mut w,
    )
    .unwrap();
    w.finish().unwrap();
    let evaluator_view::UnitStateTemplate::Scalar { kernel, .. } =
        evaluator_view::state_template(&p, p.roots()[0].use_id).unwrap()
    else {
        panic!("scalar")
    };
    let mut instance = ScalarEvaluationInstance::instantiate(kernel.clone()).unwrap();
    let left: arrow::array::ArrayRef = Arc::new(Int64Array::from(vec![4, 1]));
    let right: arrow::array::ArrayRef = Arc::new(Int64Array::from(vec![2, 2]));
    let arguments = [
        EvaluatedArgument::Column(&left),
        EvaluatedArgument::Column(&right),
    ];
    let result = instance
        .evaluate(Selection::all(2), &arguments, &Runtime)
        .unwrap();
    assert_eq!(
        result
            .values()
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap(),
        &BooleanArray::from(vec![true, false])
    );
    assert_eq!(owner.instances.load(Ordering::Relaxed), 1);
    let owner = Arc::new(Owner::for_overload(TYPE_ONLY));
    let u = unit(&owner, ArgumentControl::TypeOnly, true);
    assert_eq!(u.control_flow().uses().len(), 2);
    let catalog = self::catalog(owner.clone());
    let mut w = CompileCheckpoints::try_new(&c, CompilePhase::LowerProgram).unwrap();
    let e = compile_expr_set_pure(
        &u,
        EvaluationEnvironment {
            constants: &ConstantPools::empty(),
            parameters: &SemanticParameters::default(),
        },
        ScopeLayouts {
            ports: &scopes,
            all_ports: None,
        },
        policy(),
        &catalog,
        &mut w,
    )
    .unwrap_err();
    assert!(matches!(
        e,
        ExprCompileError::Binding(FunctionBindingError::UnknownFunction)
    ));
    assert_eq!(owner.instances.load(Ordering::Relaxed), 0);
}

#[test]
fn real_frozen_call_creation_refusals_keep_first_host_and_final_template_holds() {
    let (unit, catalog, parameters) = rand_unit();
    let scopes = [];
    let run = |refuse| {
        let c = Arc::new(Control::good());
        let f = Factory {
            events: Arc::new(Mutex::default()),
            holds: Mutex::default(),
            refuse,
            control: c.clone(),
            stopped: Arc::new(Mutex::default()),
        };
        let mut temporary = f.scope();
        let mut retained = f.scope();
        let mut work = CompileCheckpoints::try_new(c.as_ref(), CompilePhase::LowerProgram).unwrap();
        let result = compile_expr_set(
            &unit,
            EvaluationEnvironment {
                constants: &ConstantPools::empty(),
                parameters: &parameters,
            },
            ScopeLayouts {
                ports: &scopes,
                all_ports: None,
            },
            policy(),
            &catalog,
            ExprCreation {
                temporary: &mut temporary,
                retained: &mut retained,
                factory: &f,
            },
            &mut work,
        );
        let count = f.events.lock().unwrap().len();
        if refuse.is_some() {
            assert!(matches!(
                result,
                Err(ExprCompileFailure::Creator(HostRefusal))
            ));
            assert_eq!(f.stopped.lock().unwrap().unwrap(), c.trace().len());
            drop(temporary);
            drop(retained);
            assert!(
                f.holds
                    .lock()
                    .unwrap()
                    .iter()
                    .all(|h| h.upgrade().is_none())
            );
        } else {
            let p = result.unwrap();
            let late = p.types().clone();
            drop(temporary);
            drop(retained);
            assert!(
                f.holds
                    .lock()
                    .unwrap()
                    .iter()
                    .any(|h| h.upgrade().is_some())
            );
            assert_eq!(p.calls().len(), 2);
            drop(p);
            assert!(
                f.holds
                    .lock()
                    .unwrap()
                    .iter()
                    .any(|h| h.upgrade().is_some())
            );
            drop(late);
            assert!(
                f.holds
                    .lock()
                    .unwrap()
                    .iter()
                    .all(|h| h.upgrade().is_none())
            );
        }
        count
    };
    let count = run(None);
    assert!(count > 8);
    for at in 0..count {
        assert_eq!(run(Some(at)), at + 1);
    }
}

#[test]
fn intrinsic_controller_keeps_hand_checked_branch_guards_without_scalar_instance() {
    let owner = Arc::new(Owner::for_overload(IF));
    let mut b = builder();
    let leaf = b
        .add(None, result_type(), ExprKind::Value(ValueId::new(9)))
        .unwrap();
    let arguments = owner.arguments(&oid(IF));
    let selected = FunctionBindingSelection {
        overload: oid(IF),
        argument_types: FunctionArgumentTypes::from_pure_boxed(
            arguments
                .iter()
                .map(FunctionArgument::argument_type)
                .collect(),
        ),
        result_type: FunctionResultType::Scalar(result_type()),
        aggregate: None,
    };
    let function = BoundFunction::from_exact_signature(
        fid(CONTROL_FUNCTION),
        selected.overload.clone(),
        FunctionKind::Scalar,
        selected.argument_types.clone(),
        result_type(),
    );
    let call = b
        .add(
            None,
            result_type(),
            ExprKind::FunctionCall {
                function,
                args: Box::new([leaf, leaf, leaf]),
            },
        )
        .unwrap();
    b.request(
        call,
        PhysicalCallRequest {
            arguments: Box::new([
                novarocks_function_contract::FunctionArgument::Value {
                    value_type: result_type(),
                    constant: None,
                },
                novarocks_function_contract::FunctionArgument::Value {
                    value_type: result_type(),
                    constant: None,
                },
                novarocks_function_contract::FunctionArgument::Value {
                    value_type: result_type(),
                    constant: None,
                },
            ]),
            logical_argument_count: 3,
            expected_result_type: None,
            constant_policy: policy(),
        },
    )
    .unwrap();
    b.definition_control(
        call,
        DefinitionControl {
            shape: ControlShape::If,
            temporal_sources: None,
        },
    )
    .unwrap();
    for demand in [EvaluationDemand::Value, EvaluationDemand::TruthOnly] {
        b.root(call, demand, RootScope::Port(0)).unwrap();
    }
    let c = Control::good();
    let mut w = CompileCheckpoints::try_new(&c, CompilePhase::Validate).unwrap();
    let sealed = b
        .seal(
            vec![ExprPort {
                values: Box::new([(ValueId::new(9), result_type())]),
            }],
            &mut w,
        )
        .unwrap();
    let claims = sealed
        .uses()
        .iter()
        .filter(|(_, u)| u.definition == call)
        .map(|(&id, u)| {
            (
                id,
                ExpressionCallClaim {
                    context: u.context,
                    effects: owner.frozen(u.context, &selected),
                    decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
                    temporal_source: None,
                },
            )
        })
        .collect();
    let unit = sealed.finish(claims, &mut w).unwrap();
    w.finish().unwrap();
    let catalog = self::catalog(owner.clone());
    let scopes = [layouts(&[(ValueId::new(9), result_type())])];
    let mut w = CompileCheckpoints::try_new(&c, CompilePhase::LowerProgram).unwrap();
    let p = compile_expr_set_pure(
        &unit,
        EvaluationEnvironment {
            constants: &ConstantPools::empty(),
            parameters: &SemanticParameters::default(),
        },
        ScopeLayouts {
            ports: &scopes,
            all_ports: None,
        },
        policy(),
        &catalog,
        &mut w,
    )
    .unwrap();
    w.finish().unwrap();
    assert_eq!(owner.instances.load(Ordering::Relaxed), 0);
    assert_eq!(p.calls().len(), 2);
    assert_eq!(p.control_flow().uses().len(), 8);
    for (i, root) in p.roots().iter().enumerate() {
        let evaluator_view::UnitStateTemplate::ControlIntrinsic { scope, call } =
            evaluator_view::state_template(&p, root.use_id).unwrap()
        else {
            panic!("controller")
        };
        assert_eq!(scope.root, RootOrdinal::new(i as u32));
        assert_eq!(call.effects().argument_control, ArgumentControl::If);
        let invocation = &p.control_flow().uses()[&root.use_id];
        let condition = &p.control_flow().uses()[&invocation.arguments[0]];
        assert_eq!(condition.context.region, invocation.context.region);
        assert_eq!(condition.context.demand, EvaluationDemand::TruthOnly);
        for (ordinal, kind) in [(1, GuardKind::IfThen), (2, GuardKind::IfElse)] {
            let child = &p.control_flow().uses()[&invocation.arguments[ordinal]];
            assert_eq!(child.context.demand, root.demand);
            let region = &p.control_flow().regions()[&child.context.region];
            assert_eq!(region.parent, Some(invocation.context.region));
            assert_eq!(
                region.guard,
                Some(RegionGuard {
                    owner: root.use_id,
                    kind
                })
            );
        }
    }
}

#[test]
fn original_expected_constraint_stays_some_and_is_not_inferred_from_selected_result() {
    let mut owner = Owner::for_overload(SCALAR);
    owner.expected_constraint = Some(result_type());
    let owner = Arc::new(owner);
    let unit = unit(&owner, ArgumentControl::Eager, false);
    let catalog = self::catalog(owner.clone());
    let scopes = [layouts(&[
        (ValueId::new(1), input_type()),
        (ValueId::new(2), input_type()),
    ])];
    let run = |unit: &BoundExprSet| {
        let c = Control::good();
        let mut w = CompileCheckpoints::try_new(&c, CompilePhase::LowerProgram).unwrap();
        compile_expr_set_pure(
            unit,
            EvaluationEnvironment {
                constants: &ConstantPools::empty(),
                parameters: &SemanticParameters::default(),
            },
            ScopeLayouts {
                ports: &scopes,
                all_ports: None,
            },
            policy(),
            &catalog,
            &mut w,
        )
    };
    run(&unit).unwrap();
    assert!(
        unit.requests()
            .values()
            .all(|r| r.expected_result_type == Some(result_type()))
    );
    let mut requests = unit.requests().clone();
    for request in requests.values_mut() {
        request.expected_result_type = None;
    }
    let parts = UnitParts::new(
        unit.ports().into(),
        unit.definitions().into(),
        unit.roots().into(),
        unit.root_use_ids().into(),
        unit.control_flow().clone(),
        unit.claims().clone(),
        requests,
    );
    let c = Control::good();
    let mut w = CompileCheckpoints::try_new(&c, CompilePhase::Validate).unwrap();
    let missing = BoundExprSet::try_from_parts_in(parts, limits(), &mut w).unwrap();
    w.finish().unwrap();
    assert_eq!(missing.definitions(), unit.definitions());
    assert!(
        matches!(run(&missing),Err(ExprCompileError::Binding(FunctionBindingError::InvalidBinding(message))) if message.as_ref()=="fixture signature differs")
    );
    assert_eq!(owner.instances.load(Ordering::Relaxed), 0);
}

fn seal_type_only_fixture(
    mut b: BoundExprSetBuilder,
    inputs: [(ExprId, ValueType); 2],
    ports: Vec<ExprPort>,
) -> Result<(BoundExprSet, Arc<Owner>), UnitError> {
    let mut owner = Owner::for_overload(TYPE_ONLY);
    let [(first, first_type), (second, second_type)] = inputs;
    owner.explicit_type_only = Some([first_type.clone(), second_type.clone()]);
    let owner = Arc::new(owner);
    let arguments = owner.arguments(&owner.implementations[0].overload);
    let request = FunctionBindingRequest {
        arguments: &arguments,
        logical_argument_count: 2,
        expected_result_type: None,
    };
    let selected = owner.resolve(request, &Control::good()).unwrap();
    let call = b
        .add(
            None,
            result_type(),
            ExprKind::FunctionCall {
                function: BoundFunction::from_exact_signature(
                    owner.declaration.function_id().clone(),
                    selected.overload.clone(),
                    FunctionKind::Scalar,
                    selected.argument_types.clone(),
                    result_type(),
                ),
                args: Box::from([first, second]),
            },
        )
        .unwrap();
    b.request(
        call,
        PhysicalCallRequest {
            arguments: Box::new([first_type, second_type].map(|value_type| {
                novarocks_function_contract::FunctionArgument::Value {
                    value_type,
                    constant: None,
                }
            })),
            logical_argument_count: 2,
            expected_result_type: None,
            constant_policy: policy(),
        },
    )
    .unwrap();
    b.definition_control(
        call,
        DefinitionControl {
            shape: ControlShape::TypeOnly,
            temporal_sources: None,
        },
    )
    .unwrap();
    b.root(call, EvaluationDemand::Value, RootScope::Port(0))
        .unwrap();
    let c = Control::good();
    let mut work = CompileCheckpoints::try_new(&c, CompilePhase::Validate).unwrap();
    let sealed = b.seal(ports, &mut work)?;
    assert_eq!(sealed.uses().len(), 1);
    let claims = sealed
        .uses()
        .iter()
        .map(|(&id, u)| {
            (
                id,
                ExpressionCallClaim {
                    context: u.context,
                    effects: owner.frozen(u.context, &selected),
                    decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
                    temporal_source: None,
                },
            )
        })
        .collect();
    let unit = sealed.finish(claims, &mut work).unwrap();
    work.finish().unwrap();
    Ok((unit, owner))
}

pub(super) fn type_only_fixture(
    builder: BoundExprSetBuilder,
    arguments: [(ExprId, ValueType); 2],
    ports: Vec<ExprPort>,
) -> (BoundExprSet, PureEngineFunctionCatalog) {
    let (unit, owner) = seal_type_only_fixture(builder, arguments, ports).unwrap();
    (unit, catalog(owner))
}

type HiddenNativeFixture = (BoundExprSet, Arc<Owner>, Vec<(ValueId, ValueType)>);

fn type_only_hidden_native_fixture(kind: &str) -> Result<HiddenNativeFixture, UnitError> {
    let mut b = builder();
    let input_type = match kind {
        "in_float" => FunctionValueType::new(DataType::Float64, false),
        "in_text_array" => FunctionValueType::new(
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, false))),
            false,
        ),
        "like_json" | "between_json" => FunctionValueType::try_with_logical_type(
            DataType::LargeBinary,
            false,
            ValueLogicalType::Json,
        )
        .unwrap(),
        _ => unreachable!(),
    };
    let mut ports = vec![
        (ValueId::new(10), input_type.clone()),
        (ValueId::new(20), result_type()),
    ];
    let input = b
        .add(None, input_type.clone(), ExprKind::Value(ValueId::new(10)))
        .unwrap();
    let other = b
        .add(None, result_type(), ExprKind::Value(ValueId::new(20)))
        .unwrap();
    let hidden_kind = match kind {
        "like_json" => {
            let pattern_type = FunctionValueType::new(DataType::Utf8, false);
            ports.push((ValueId::new(30), pattern_type.clone()));
            let pattern = b
                .add(None, pattern_type, ExprKind::Value(ValueId::new(30)))
                .unwrap();
            ExprKind::Like {
                expr: input,
                pattern,
                negated: false,
            }
        }
        "in_float" | "in_text_array" => ExprKind::InList {
            expr: input,
            list: Box::from([input]),
            negated: false,
        },
        "between_json" => ExprKind::Between {
            expr: input,
            low: input,
            high: input,
            negated: false,
        },
        _ => unreachable!(),
    };
    let hidden = b.add(None, result_type(), hidden_kind).unwrap();
    let (unit, owner) = seal_type_only_fixture(
        b,
        [(hidden, result_type()), (other, result_type())],
        vec![ExprPort {
            values: ports.clone().into_boxed_slice(),
        }],
    )?;
    Ok((unit, owner, ports))
}

fn compile_type_only_hidden_native_profile(
    kind: &str,
) -> Result<Arc<CompiledExprSet>, ExprCompileError> {
    let (unit, owner, ports) = type_only_hidden_native_fixture(kind).unwrap();
    let catalogue = catalog(owner.clone());
    let c = Control::good();
    let mut work = CompileCheckpoints::try_new(&c, CompilePhase::LowerProgram).unwrap();
    let layout = layouts(&ports);
    let parameters = SemanticParameters::try_new([]).unwrap();
    let compiled = compile_expr_set_pure(
        &unit,
        EvaluationEnvironment {
            constants: &ConstantPools::empty(),
            parameters: &parameters,
        },
        ScopeLayouts {
            ports: &[layout],
            all_ports: None,
        },
        policy(),
        &catalogue,
        &mut work,
    );
    work.finish().unwrap();
    assert_eq!(owner.instances.load(Ordering::SeqCst), 0);
    compiled
}

fn check_type_only_hidden_native_profile_refusal(kind: &str) {
    let error = compile_type_only_hidden_native_profile(kind).unwrap_err();
    assert!(
        matches!(
            error,
            ExprCompileError::Comparison(ComparisonPrepareError::Unsupported)
        ),
        "{kind}: {error:?}"
    );
    assert_eq!(error.failure_fact().class(), FailureClass::Unsupported);
}

#[test]
fn type_only_hidden_json_like_is_rejected_by_the_unit_verifier() {
    let Err(error) = type_only_hidden_native_fixture("like_json") else {
        panic!("a JSON operand is not UTF-8 text");
    };
    assert!(matches!(
        error,
        UnitError::Invalid { definition: Some(_), root: None, message }
            if message.ends_with("LIKE operands must use a UTF-8 type")
    ));
}
#[test]
fn type_only_hidden_float_in_compiles_without_evaluating_arguments() {
    let compiled = compile_type_only_hidden_native_profile("in_float").unwrap();
    let [root] = compiled.roots() else {
        panic!("expected one type-only root");
    };
    let uses = compiled.control_flow().uses();
    assert_eq!(uses.len(), 1);
    let invocation = &uses[&root.use_id];
    assert_eq!(invocation.control, ControlShape::TypeOnly);
    assert!(invocation.arguments.is_empty());

    let definitions = compiled.definitions();
    let StaticExprKind::BoundCall { args, .. } = definitions.node(root.definition).unwrap().kind()
    else {
        panic!("expected the original bound call");
    };
    let StaticExprKind::PreparedInList { child, values, .. } =
        definitions.node(args[0]).unwrap().kind()
    else {
        panic!("expected the original floating-point membership definition");
    };
    assert_eq!(values.as_slice(), &[*child]);
    assert_eq!(
        definitions.node(*child).unwrap().value_type(),
        Some(&FunctionValueType::new(DataType::Float64, false))
    );
}
#[test]
fn type_only_hidden_text_array_in_compiles_without_evaluating_arguments() {
    let compiled = compile_type_only_hidden_native_profile("in_text_array").unwrap();
    let [root] = compiled.roots() else {
        panic!("expected one type-only root");
    };
    let uses = compiled.control_flow().uses();
    assert_eq!(uses.len(), 1);
    let invocation = &uses[&root.use_id];
    assert_eq!(invocation.control, ControlShape::TypeOnly);
    assert!(invocation.arguments.is_empty());
}
#[test]
fn type_only_hidden_json_between_keeps_the_original_profile_refusal() {
    check_type_only_hidden_native_profile_refusal("between_json");
}

#[test]
fn zero_port_strict_null_skips_kernel_creation() {
    use crate::zero_port;
    use novarocks_expression::testkit::{
        ClaimAuthor, HarnessControl, add_call, compile_fixture_unit, declare_call_controls,
        finish_unit, register_call_requests,
    };

    let owner = Arc::new(Owner::for_overload(STRICT));
    let functions = catalog(owner.clone());
    let nullable = input_type().with_nullable(true).unwrap();
    let parameters = SemanticParameters::default();
    assert_eq!(
        owner
            .declaration
            .effect_declaration(&oid(STRICT))
            .unwrap()
            .null_behavior,
        FunctionNullBehavior::Strict,
    );
    for (value, expected, created) in [(None, None, 0), (Some(23), Some(true), 1)] {
        let mut draft = builder();
        let mut constants = ConstantPools::empty();
        let children = [value, Some(19)]
            .map(|value| {
                zero_port::add_constant(
                    &mut draft,
                    &mut constants,
                    Arc::new(Int64Array::from(vec![value])),
                    nullable.clone(),
                )
            })
            .to_vec();
        let claim = ClaimAuthor::with_exact_eager_binding(
            &functions,
            "unit_fixture",
            owner.arguments(&oid(STRICT)),
            policy(),
            &HarnessControl,
        );
        let mut authors = BTreeMap::new();
        let call = add_call(&mut draft, &mut authors, claim, children);
        draft
            .root(call, EvaluationDemand::Value, RootScope::AllPorts)
            .unwrap();
        register_call_requests(&mut draft, &authors, &mut constants);
        let mut work =
            CompileCheckpoints::try_new(&HarnessControl, CompilePhase::Validate).unwrap();
        declare_call_controls(
            &mut draft,
            &functions,
            &constants,
            &parameters,
            policy(),
            &mut work,
        );
        let sealed = draft.seal(Vec::new(), &mut work).unwrap();
        let unit = finish_unit(
            &functions,
            sealed,
            &authors,
            &constants,
            &parameters,
            policy(),
            &mut work,
        );
        work.finish().unwrap();
        let compiled = compile_fixture_unit(
            &unit,
            EvaluationEnvironment {
                constants: &constants,
                parameters: &parameters,
            },
            ScopeLayouts {
                ports: &[],
                all_ports: None,
            },
            policy(),
            &functions,
        );
        let result = zero_port::evaluate(compiled);
        assert!(result.errors().is_empty());
        assert_eq!(
            result
                .values()
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap()
                .iter()
                .next(),
            Some(expected),
        );
        assert_eq!(owner.instances.load(Ordering::SeqCst), created);
    }
}

#[test]
fn ordered_null_demands_only_surviving_rows_and_keeps_original_error_scope() {
    use arrow::{array::StringArray, datatypes::Schema, record_batch::RecordBatch};
    use novarocks_expression::testkit::{
        HarnessControl,
        expression_text::{Consistency, TextColumn, TextContext, TextError, type_expression_text},
    };
    use novarocks_type_contract::{
        SemanticParameterId, SemanticParameterKey, SemanticParameterRef,
    };

    let owner = Arc::new(Owner::for_overload(ORDERED));
    let functions = catalog(owner.clone());
    let parameters = SemanticParameters::default();
    let context = TextContext {
        functions: &functions,
        constant_policy: policy(),
        decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
        parameters: &parameters,
        allow_throw_exception: SemanticParameterRef {
            id: SemanticParameterId::new(0),
            expected_key: SemanticParameterKey::AllowThrowException,
        },
    };
    let columns = ["a", "b"].map(|name| TextColumn {
        name: name.into(),
        value: ValueId::new(if name == "a" { 1 } else { 2 }),
        value_type: FunctionValueType::new(DataType::Utf8, true),
    });
    let schema = Arc::new(Schema::new(
        columns
            .iter()
            .map(|column| column.value_type.try_to_field(&column.name).unwrap())
            .collect::<Vec<_>>(),
    ));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(vec![
                None,
                Some("bad-left"),
                Some("5"),
                Some("6"),
                Some("1"),
                Some("9"),
                Some("97"),
            ])),
            Arc::new(StringArray::from(vec![
                Some("bad-right"),
                None,
                Some("bad-right"),
                Some("3"),
                Some("3"),
                None,
                Some("3"),
            ])),
        ],
    )
    .unwrap();
    let call = "unit_fixture(CAST(a AS BIGINT), CAST(b AS BIGINT))";
    let expression = type_expression_text(call, &columns, &context, &HarnessControl).unwrap();
    let flow = expression.unit().control_flow();
    let first_use = &flow.uses()[&expression.unit().root_use_ids()[0]];
    assert_eq!(first_use.control, ControlShape::OrderedNull);
    assert_eq!(first_use.arguments.len(), 2);
    let second = &flow.uses()[&first_use.arguments[1]];
    assert_eq!(
        flow.regions()[&second.context.region]
            .guard
            .as_ref()
            .unwrap()
            .kind,
        GuardKind::OrderedNullAfterArgument { ordinal: 0 },
    );
    let checked = expression
        .check_rows(&batch, &HarnessControl, &Runtime)
        .unwrap();
    assert!(matches!(checked.consistency, Consistency::Compared { .. }));
    assert_eq!(
        checked
            .batch
            .values
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap(),
        &BooleanArray::from(vec![None, None, None, Some(true), Some(false), None, None]),
    );
    assert_eq!(
        checked
            .batch
            .errors
            .iter()
            .map(|error| (error.selected_ordinal(), error.failure_fact().reason()))
            .collect::<Vec<_>>(),
        [
            (
                1,
                novarocks_failure_contract::kernel_reason::INVALID_CAST_ARGUMENT
            ),
            (
                2,
                novarocks_failure_contract::kernel_reason::INVALID_CAST_ARGUMENT
            ),
            (
                6,
                novarocks_failure_contract::kernel_reason::INVALID_FUNCTION_ARGUMENT
            ),
        ],
    );
    assert_eq!(
        *owner.ordered_rows.lock().unwrap(),
        [vec![3, 4, 6], vec![0], vec![0], vec![0]]
    );

    let expression =
        type_expression_text(&format!("TRY({call})"), &columns, &context, &HarnessControl).unwrap();
    let checked = expression
        .check_rows(&batch, &HarnessControl, &Runtime)
        .unwrap();
    assert!(checked.batch.errors.is_empty());
    assert_eq!(
        checked
            .batch
            .values
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap(),
        &BooleanArray::from(vec![None, None, None, Some(true), Some(false), None, None]),
    );
    for (left, failure) in [
        ("98", KernelFailure::Cancelled),
        ("99", KernelFailure::ResourceExhausted),
    ] {
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec![left])),
                Arc::new(StringArray::from(vec!["3"])),
            ],
        )
        .unwrap();
        let error = expression
            .check_rows(&batch, &HarnessControl, &Runtime)
            .unwrap_err();
        let TextError::Evaluation(error) = error else {
            panic!("expected original evaluation failure, got {error:?}");
        };
        match (*error, failure) {
            (
                novarocks_expression::ExprEvaluationFailure::Kernel(KernelFailure::Cancelled),
                KernelFailure::Cancelled,
            )
            | (
                novarocks_expression::ExprEvaluationFailure::Kernel(
                    KernelFailure::ResourceExhausted,
                ),
                KernelFailure::ResourceExhausted,
            ) => {}
            (actual, expected) => panic!("actual {actual:?}, expected {expected:?}"),
        }
    }
}
