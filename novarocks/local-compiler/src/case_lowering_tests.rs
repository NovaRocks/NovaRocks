// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

use super::*;
use arrow_schema::DataType;
use novarocks_functions::{
    CallEffectInput, ConstantPolicy, ConstantValue, EngineFunctionCatalogBuilder, FunctionArgument,
    FunctionArgumentType, FunctionBindingRequest, FunctionBindingSelection, FunctionId,
    FunctionKind, FunctionOverloadId, FunctionResultType, InstalledPureKernel, PureCallPreparation,
    PureEngineFunctionCatalog, PureImplementationDeclaration, PureImplementationId, PureKernelAbi,
    ScopedExpressionEffects,
};
use novarocks_local_program::{
    KernelAbiVersion, ProgramExpressionArena, ProgramExpressionRootSite, ProgramNodeExpressionRole,
    ProgramNodeId, StaticExprKind,
};
use novarocks_physical_plan::{
    BoundFunction, ExprId, ExprKind, Fragment, FragmentBuilder, FragmentCuts, FragmentId,
    FragmentPackageInput, FragmentSink, FrozenFragmentCalls, FrozenFragmentPruning,
    FrozenPhysicalCall, LiteralValue, PhysicalCallSite, PhysicalExpressionRoots, PhysicalRootUses,
    PipelineDopDomain, PlanVersionId, RequiredContracts, ResultField, ResultPort,
    RootUseBindingError, ValidationErrors, ValueOrigin,
};
use novarocks_type_contract::{
    ArgumentControl, CallProofScope, ControlShape, DecimalOverflowPolicy, DomainGuard,
    EvaluationDemand, EvaluationDomainId, ExpressionControlFlow, ExpressionControlFlowError,
    ExpressionEffectContext, ExpressionEvaluationDomain, ExpressionInvocation, ExpressionUseId,
    FunctionIntrinsicRowError, FunctionValueType, FunctionVolatility, GuardKind,
    SemanticParameters, ValueLogicalType, control_argument_semantics,
};
use std::{num::NonZeroUsize, sync::Mutex};

struct Control;
impl PureCompileControl for Control {
    fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
        Ok(())
    }
}
// Actual owners and independently authored records only; this is not the Server seal.
fn functions() -> PureEngineFunctionCatalog {
    let actual =
        novarocks_functions::builtin::catalogue::build_builtin_engine_function_catalog().unwrap();
    let mut builder = EngineFunctionCatalogBuilder::new();
    let mut manifest = vec![];
    for (name, overloads, abi) in [
        (
            "rand",
            &["()->f64;strict;legacy", "(i64)->f64;strict;legacy"][..],
            PureKernelAbi::ScalarV1,
        ),
        (
            "if",
            &["(bool,any<T>,any<T>)->any<T>;widen;legacy"][..],
            PureKernelAbi::ControlIntrinsicV1,
        ),
        ("round", &["dynamic-v1"][..], PureKernelAbi::ScalarV1),
    ] {
        builder
            .register(
                actual
                    .definition(name, FunctionKind::Scalar)
                    .unwrap()
                    .clone(),
            )
            .unwrap();
        for overload in overloads {
            manifest.push(InstalledPureKernel {
                function: FunctionId::try_new(format!("builtin.scalar/{name}/v1")).unwrap(),
                kind: FunctionKind::Scalar,
                implementation: PureImplementationDeclaration {
                    overload: FunctionOverloadId::try_new(format!(
                        "builtin.scalar/{name}/{overload}"
                    ))
                    .unwrap(),
                    implementation: PureImplementationId::try_new(format!(
                        "builtin.scalar/{name}/selected-v1"
                    ))
                    .unwrap(),
                    abi,
                },
                aggregate_state_format: None,
            });
        }
    }
    builder.seal_pure(manifest).unwrap()
}
fn options() -> LocalCompileOptions {
    LocalCompileOptions {
        pipeline_dop: NonZeroUsize::new(1).unwrap(),
        root_sink_dop: Some(NonZeroUsize::new(1).unwrap()),
        kernel_abi: KernelAbiVersion::CURRENT,
        constants: ConstantPolicy {
            max_rows: 16,
            max_array_nodes: 128,
            max_logical_elements: 1024,
            max_retained_buffer_bytes: 1 << 20,
            max_type_depth: 64,
            max_type_nodes: 4096,
            max_dictionary_depth: 64,
            max_metadata_bytes: 1 << 20,
            max_library_validation_work: 1 << 20,
            max_library_validation_bytes: 1 << 20,
        },
    }
}

struct Author {
    function: BoundFunction,
    selected: Arc<FunctionBindingSelection>,
    arguments: Vec<FunctionArgument>,
    logical_argument_count: usize,
    constant_policy: ConstantPolicy,
    shape: ControlShape,
}
impl Author {
    fn request(&self) -> FunctionBindingRequest<'_> {
        FunctionBindingRequest {
            arguments: &self.arguments,
            logical_argument_count: self.logical_argument_count,
            expected_result_type: None,
        }
    }
    fn result(&self) -> FunctionValueType {
        let FunctionResultType::Scalar(result) = &self.selected.result_type else {
            panic!("scalar owner")
        };
        result.clone()
    }
}
// Transfer the first resolver request; physical child shapes are not request sources.
fn original_request_sources(
    fragment: Fragment,
    authors: &BTreeMap<ExprId, Author>,
) -> (Fragment, novarocks_physical_plan::ConstantPools) {
    use novarocks_physical_plan::{
        ConstantPoolId, ConstantPools, ConstantReference, PhysicalCallDefinition,
        PhysicalCallRequest, StaticFunctionArgument,
    };
    let mut pools = ConstantPools::empty();
    let mut backing_ids = BTreeMap::new();
    let mut entries = vec![];
    for (&definition, owner) in authors {
        let original = owner.request();
        let arguments = original
            .arguments
            .iter()
            .map(|argument| match argument {
                FunctionArgument::Value {
                    value_type,
                    constant,
                } => {
                    let constant = constant.as_ref().map(|value| {
                        let identity = value.pool().backing_identity();
                        let pool = *backing_ids.entry(identity).or_insert_with(|| {
                            let id =
                                ConstantPoolId::new(u32::try_from(pools.entries().len()).unwrap());
                            pools.insert(id, value.pool().clone()).unwrap();
                            id
                        });
                        ConstantReference {
                            pool,
                            ordinal: value.ordinal(),
                        }
                    });
                    StaticFunctionArgument::Value {
                        value_type: value_type.clone(),
                        constant,
                    }
                }
                FunctionArgument::Lambda {
                    parameter_types,
                    result_type,
                } => StaticFunctionArgument::Lambda {
                    parameter_types: parameter_types.clone(),
                    result_type: result_type.clone(),
                },
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();
        entries.push((
            PhysicalCallDefinition::Expression(definition),
            PhysicalCallRequest {
                arguments,
                logical_argument_count: original.logical_argument_count,
                expected_result_type: original.expected_result_type.cloned(),
                constant_policy: owner.constant_policy,
            },
        ));
    }
    (
        fragment
            .with_call_requests_observed(entries, &Control)
            .unwrap(),
        pools,
    )
}
fn integer_constant(ty: &FunctionValueType, value: i64) -> ConstantValue {
    ConstantValue::from_i64(
        Arc::new(ty.try_to_field("fixture").unwrap()),
        ty.clone(),
        value,
        options().constants,
        CompilePhase::FunctionSpecialization,
        &Control,
    )
    .unwrap()
}
fn integer_argument(ty: FunctionValueType, value: i64) -> FunctionArgument {
    let constant = integer_constant(&ty, value);
    argument(ty, Some(constant))
}
fn argument(ty: FunctionValueType, constant: Option<ConstantValue>) -> FunctionArgument {
    FunctionArgument::Value {
        value_type: ty,
        constant,
    }
}
fn author(
    functions: &PureEngineFunctionCatalog,
    name: &str,
    arguments: Vec<FunctionArgument>,
    shape: ControlShape,
) -> Author {
    let logical_argument_count = arguments.len();
    let request = FunctionBindingRequest {
        arguments: &arguments,
        logical_argument_count,
        expected_result_type: None,
    };
    let bound = functions
        .metadata()
        .resolve_bound_user(name, FunctionKind::Scalar, request, &Control)
        .unwrap();
    let selected = Arc::new(bound.selected.clone());
    let FunctionResultType::Scalar(result) = &selected.result_type else {
        panic!("scalar owner")
    };
    let function = BoundFunction {
        function_id: bound.function_id,
        overload: selected.overload.clone(),
        kind: bound.kind,
        argument_types: selected.argument_types.clone(),
        result_type: result.clone(),
        legacy_metadata: Some(novarocks_physical_plan::LegacyBindingMetadata {
            volatility: bound.semantics.volatility,
            argument_evaluation: bound.semantics.argument_evaluation,
            failure_behavior: bound.semantics.failure_behavior,
            intrinsic_row_error: bound.semantics.intrinsic_row_error,
            semantic_parameters: Box::default(),
        }),
    };
    Author {
        function,
        selected,
        arguments,
        logical_argument_count,
        constant_policy: options().constants,
        shape,
    }
}
fn call(
    builder: &mut FragmentBuilder,
    authors: &mut BTreeMap<ExprId, Author>,
    owner: Author,
    args: Vec<ExprId>,
) -> ExprId {
    let expr = builder
        .add_expression(
            NodeId::new(0),
            owner.result(),
            ExprKind::FunctionCall {
                function: owner.function.clone(),
                args: args.into_boxed_slice(),
            },
        )
        .unwrap();
    authors.insert(expr, owner);
    expr
}

#[derive(Clone, Copy)]
enum Form {
    Searched,
    SimpleInteger,
    SimpleUuid,
}
#[derive(Clone, Copy)]
enum Mode {
    Project,
    Filter,
    IfArgument,
}
#[derive(Clone, Copy)]
enum Effects {
    Pure,
    Random,
    Decimal,
}
struct Fixture {
    fragment: Fragment,
    authors: BTreeMap<ExprId, Author>,
    case: ExprId,
    ordered: Vec<ExprId>,
}
fn add_input(
    builder: &mut FragmentBuilder,
    ty: FunctionValueType,
) -> (ExprId, novarocks_physical_plan::ValueId) {
    let expr = builder
        .add_expression(
            NodeId::new(44),
            ty.clone(),
            ExprKind::Literal(LiteralValue::Null),
        )
        .unwrap();
    let value = builder
        .add_value(
            ty.clone(),
            ValueOrigin::Expr {
                node: NodeId::new(44),
                expr,
            },
        )
        .unwrap();
    (expr, value)
}
fn literal(builder: &mut FragmentBuilder, ty: FunctionValueType, value: LiteralValue) -> ExprId {
    builder
        .add_expression(NodeId::new(0), ty, ExprKind::Literal(value))
        .unwrap()
}
fn fixture(
    functions: &PureEngineFunctionCatalog,
    form: Form,
    mode: Mode,
    has_else: bool,
    effects: Effects,
    narrow_no_else: bool,
) -> Result<Fixture, ValidationErrors> {
    let mut builder = FragmentBuilder::new(FragmentId::new(97));
    builder
        .add_values(
            NodeId::new(u32::MAX),
            Box::from([Box::default()]),
            Box::default(),
        )
        .unwrap();
    let boolean = FunctionValueType::new(DataType::Boolean, true);
    let fixed_boolean = FunctionValueType::new(DataType::Boolean, false);
    let mut inputs = vec![];
    let (flag, flag_value) = add_input(&mut builder, boolean.clone());
    inputs.push((flag, flag_value));
    let operand_type = match form {
        Form::Searched => None,
        Form::SimpleInteger => Some(FunctionValueType::new(DataType::Int64, true)),
        Form::SimpleUuid => Some(
            FunctionValueType::try_from_legacy_declared_projection(
                DataType::FixedSizeBinary(16),
                true,
                ValueLogicalType::Uuid,
            )
            .expect("valid temporary legacy value projection"),
        ),
    };
    let operand_value = operand_type.as_ref().map(|ty| {
        let input = add_input(&mut builder, ty.clone());
        inputs.push(input);
        input.1
    });
    let decimal = FunctionValueType::new(DataType::Decimal128(38, 0), true);
    let decimal_value = if matches!(effects, Effects::Decimal) {
        let input = add_input(&mut builder, decimal.clone());
        inputs.push(input);
        Some(input.1)
    } else {
        None
    };
    builder
        .add_project(
            NodeId::new(44),
            NodeId::new(u32::MAX),
            inputs.clone().into_boxed_slice(),
            inputs
                .iter()
                .map(|(_, value)| *value)
                .collect::<Vec<_>>()
                .into_boxed_slice(),
        )
        .unwrap();
    let flag_reference = if matches!(form, Form::Searched) || matches!(mode, Mode::IfArgument) {
        Some(
            builder
                .add_expression(NodeId::new(0), boolean.clone(), ExprKind::Value(flag_value))
                .unwrap(),
        )
    } else {
        None
    };
    let (operand, when) = if let Some(ty) = operand_type {
        let operand = builder
            .add_expression(
                NodeId::new(0),
                ty.clone(),
                ExprKind::Value(operand_value.unwrap()),
            )
            .unwrap();
        let values = if matches!(form, Form::SimpleInteger) {
            [LiteralValue::Int64(7), LiteralValue::Int64(9)]
        } else {
            [LiteralValue::Null, LiteralValue::Null]
        };
        // The flag is a real source output; avoid authoring an unreferenced root definition.
        (
            Some(operand),
            [
                literal(&mut builder, ty.clone(), values[0].clone()),
                literal(&mut builder, ty, values[1].clone()),
            ],
        )
    } else {
        (
            None,
            [
                flag_reference.unwrap(),
                literal(
                    &mut builder,
                    fixed_boolean.clone(),
                    LiteralValue::Boolean(false),
                ),
            ],
        )
    };
    let mut authors = BTreeMap::new();
    let (then, else_expr, mut result_type) = match effects {
        Effects::Pure => {
            let first = literal(
                &mut builder,
                fixed_boolean.clone(),
                LiteralValue::Boolean(true),
            );
            let second = literal(
                &mut builder,
                fixed_boolean.clone(),
                LiteralValue::Boolean(false),
            );
            let otherwise = has_else.then(|| {
                literal(
                    &mut builder,
                    fixed_boolean.clone(),
                    LiteralValue::Boolean(true),
                )
            });
            (
                [first, second],
                otherwise,
                FunctionValueType::new(DataType::Boolean, !has_else),
            )
        }
        Effects::Random => {
            let first = call(
                &mut builder,
                &mut authors,
                author(functions, "rand", vec![], ControlShape::Eager),
                vec![],
            );
            let second = call(
                &mut builder,
                &mut authors,
                author(functions, "rand", vec![], ControlShape::Eager),
                vec![],
            );
            let ty = FunctionValueType::new(DataType::Float64, true);
            let otherwise = has_else.then(|| {
                literal(
                    &mut builder,
                    ty.clone(),
                    LiteralValue::Float64Bits(0f64.to_bits()),
                )
            });
            ([first, second], otherwise, ty)
        }
        Effects::Decimal => {
            let value = builder
                .add_expression(
                    NodeId::new(0),
                    decimal.clone(),
                    ExprKind::Value(decimal_value.unwrap()),
                )
                .unwrap();
            let integer = FunctionValueType::new(DataType::Int64, false);
            let digits = literal(&mut builder, integer.clone(), LiteralValue::Int64(-1));
            let round = author(
                functions,
                "round",
                vec![
                    argument(decimal.clone(), None),
                    integer_argument(integer, -1),
                ],
                ControlShape::Eager,
            );
            let result_type = round.result();
            let rounded = call(&mut builder, &mut authors, round, vec![value, digits]);
            let otherwise =
                has_else.then(|| literal(&mut builder, result_type.clone(), LiteralValue::Null));
            ([rounded, rounded], otherwise, result_type)
        }
    };
    if narrow_no_else {
        result_type = result_type.with_nullable(false).unwrap();
    }
    let case = builder
        .add_expression(
            NodeId::new(0),
            result_type.clone(),
            ExprKind::Case {
                operand,
                when_then: Box::from([(when[0], then[0]), (when[1], then[1])]),
                else_expr,
            },
        )
        .unwrap();
    let mut ordered = Vec::new();
    ordered.extend(operand);
    ordered.extend([when[0], then[0], when[1], then[1]]);
    ordered.extend(else_expr);
    let root_expr = if matches!(mode, Mode::IfArgument) {
        call(
            &mut builder,
            &mut authors,
            author(
                functions,
                "if",
                vec![
                    argument(boolean.clone(), None),
                    argument(result_type.clone(), None),
                    argument(result_type.clone(), None),
                ],
                ControlShape::If,
            ),
            vec![flag_reference.unwrap(), case, case],
        )
    } else {
        case
    };
    match mode {
        Mode::Project | Mode::IfArgument => {
            let ty = authors
                .get(&root_expr)
                .map(Author::result)
                .unwrap_or(result_type);
            let value = builder
                .add_value(
                    ty,
                    ValueOrigin::Expr {
                        node: NodeId::new(0),
                        expr: root_expr,
                    },
                )
                .unwrap();
            builder
                .add_project(
                    NodeId::new(0),
                    NodeId::new(44),
                    Box::from([(root_expr, value)]),
                    Box::from([value]),
                )
                .unwrap();
        }
        Mode::Filter => {
            builder
                .add_filter(NodeId::new(0), NodeId::new(44), Box::from([root_expr]))
                .unwrap();
        }
    }
    let fragment = builder.finish_definition(
        NodeId::new(0),
        FragmentSink::Result,
        PipelineDopDomain {
            min: 1,
            max: 1,
            requires_power_of_two: false,
        },
        novarocks_physical_plan::StagePlacementPolicy::Singleton,
    )?;
    Ok(Fixture {
        fragment,
        authors,
        case,
        ordered,
    })
}

#[derive(Clone, Copy)]
enum FlowClaim {
    Accurate,
    WrongControl,
    SwappedArms,
    WrongGuard,
}
#[derive(Debug)]
enum FlowError {
    Flow(ExpressionControlFlowError),
    Roots(RootUseBindingError),
}
fn uses(fixture: &Fixture, claim: FlowClaim) -> Result<PhysicalRootUses, FlowError> {
    struct FlowAuthor {
        next_use: u32,
        next_domain: u32,
        uses: Vec<ExpressionInvocation<ExprId>>,
        domains: Vec<ExpressionEvaluationDomain>,
    }
    impl FlowAuthor {
        fn visit(
            &mut self,
            fixture: &Fixture,
            expr: ExprId,
            domain: EvaluationDomainId,
            demand: EvaluationDemand,
            claim: FlowClaim,
        ) -> ExpressionUseId {
            let id = ExpressionUseId::new(self.next_use);
            self.next_use += 17;
            let (mut shape, mut args) =
                match &fixture.fragment.expressions().get(expr).unwrap().kind {
                    ExprKind::Case {
                        operand,
                        when_then,
                        else_expr,
                    } => {
                        let mut args = Vec::new();
                        args.extend(*operand);
                        args.extend(when_then.iter().flat_map(|(when, then)| [*when, *then]));
                        args.extend(*else_expr);
                        (
                            ControlShape::Case {
                                simple: operand.is_some(),
                                arms: when_then.len() as u32,
                                has_else: else_expr.is_some(),
                            },
                            args,
                        )
                    }
                    ExprKind::FunctionCall { args, .. } => {
                        (fixture.authors[&expr].shape, args.to_vec())
                    }
                    ExprKind::Value(_) | ExprKind::Literal(_) => (ControlShape::Eager, vec![]),
                    other => panic!("no exact fixture control for {other:?}"),
                };
            if expr == fixture.case {
                if matches!(claim, FlowClaim::WrongControl) {
                    shape = ControlShape::Eager;
                }
                if matches!(claim, FlowClaim::SwappedArms) {
                    let offset =
                        usize::from(matches!(shape, ControlShape::Case { simple: true, .. }));
                    args.swap(offset, offset + 2);
                    args.swap(offset + 1, offset + 3);
                }
            }
            let mut children = vec![];
            for (ordinal, child) in args.iter().enumerate() {
                let (child_demand, guard) =
                    control_argument_semantics(shape, args.len(), ordinal, demand).unwrap();
                let child_domain = if let Some(kind) = guard {
                    let child_domain = EvaluationDomainId::new(self.next_domain);
                    self.next_domain += 13;
                    self.domains.push(ExpressionEvaluationDomain {
                        id: child_domain,
                        parent: Some(domain),
                        guard: Some(DomainGuard { owner: id, kind }),
                    });
                    child_domain
                } else {
                    domain
                };
                children.push(self.visit(fixture, *child, child_domain, child_demand, claim));
            }
            self.uses.push(ExpressionInvocation {
                context: ExpressionEffectContext {
                    use_id: id,
                    domain,
                    demand,
                },
                definition: expr,
                control: shape,
                arguments: children.into_boxed_slice(),
            });
            id
        }
    }
    let domain = EvaluationDomainId::new(u32::MAX);
    let mut author = FlowAuthor {
        next_use: 7,
        next_domain: 8,
        uses: vec![],
        domains: vec![ExpressionEvaluationDomain {
            id: domain,
            parent: None,
            guard: None,
        }],
    };
    let roots = PhysicalExpressionRoots::try_new(&fixture.fragment, &Control).unwrap();
    let bindings = roots
        .sites()
        .iter()
        .map(|(&site, root)| {
            (
                site,
                author.visit(fixture, root.expr, domain, root.demand, claim),
            )
        })
        .collect();
    if matches!(claim, FlowClaim::WrongGuard) {
        author
            .domains
            .iter_mut()
            .find(|d| d.guard.is_some())
            .unwrap()
            .guard
            .as_mut()
            .unwrap()
            .owner = ExpressionUseId::new(65_500);
    }
    let flow = ExpressionControlFlow::try_new(
        author.domains,
        author.uses,
        fixture.fragment.expressions(),
        CompilePhase::Validate,
        &Control,
    )
    .map_err(FlowError::Flow)?;
    PhysicalRootUses::try_new(&fixture.fragment, flow, bindings, &Control).map_err(FlowError::Roots)
}
fn package(functions: &PureEngineFunctionCatalog, fixture: Fixture) -> Arc<FragmentPackage> {
    let Fixture {
        fragment,
        authors,
        case,
        ordered,
    } = fixture;
    let (fragment, constants) = original_request_sources(fragment, &authors);
    let fixture = Fixture {
        fragment,
        authors,
        case,
        ordered,
    };
    let expression_uses = uses(&fixture, FlowClaim::Accurate).unwrap();
    let flow = expression_uses.flow();
    let parameters = SemanticParameters::try_new([]).unwrap();
    let mut summaries = BTreeMap::new();
    let mut frozen = vec![];
    fn summary(
        functions: &PureEngineFunctionCatalog,
        fixture: &Fixture,
        flow: &ExpressionControlFlow<ExprId>,
        id: ExpressionUseId,
        parameters: &SemanticParameters,
        summaries: &mut BTreeMap<ExpressionUseId, ScopedExpressionEffects>,
        frozen: &mut Vec<FrozenPhysicalCall>,
    ) -> ScopedExpressionEffects {
        if let Some(summary) = summaries.get(&id) {
            return *summary;
        }
        let invocation = &flow.uses()[&id];
        let mut children = ScopedExpressionEffects::pure_value(invocation.context);
        for (ordinal, &child) in invocation.arguments.iter().enumerate() {
            children = children
                .join_control_argument(
                    summary(
                        functions, fixture, flow, child, parameters, summaries, frozen,
                    ),
                    flow,
                    ordinal,
                )
                .unwrap();
        }
        let result = if let Some(owner) = fixture.authors.get(&invocation.definition) {
            let args = invocation
                .arguments
                .iter()
                .copied()
                .map(Some)
                .collect::<Vec<_>>();
            let preparation = if owner.shape == ControlShape::If {
                PureCallPreparation::ControlIntrinsic {
                    arguments: children,
                }
            } else {
                PureCallPreparation::Scalar {
                    arguments: children,
                }
            };
            let token = functions
                .prepare_fresh(
                    CallEffectInput {
                        context: invocation.context,
                        argument_uses: novarocks_functions::CallArgumentUses::SelectedChannels(
                            &args,
                        ),
                        function_id: &owner.function.function_id,
                        kind: owner.function.kind,
                        selected: owner.selected.as_ref(),
                        request: owner.request(),
                        environment: &[],
                        parameters,
                        decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
                        proof_scope: CallProofScope::Domain(invocation.context.domain),
                    },
                    owner.selected.clone(),
                    preparation,
                    &Control,
                )
                .unwrap();
            frozen.push(FrozenPhysicalCall {
                materialized_writer_input: None,
                regexp_count_pattern_source: None,
                to_base64_byte_source: None,
                temporal_source: None,
                site: PhysicalCallSite::Expression(id),
                context: invocation.context,
                effects: token.call_contract().effects().clone(),
                decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
            });
            token.effects()
        } else {
            children
        };
        summaries.insert(id, result);
        result
    }
    for &root in flow.root_use_ids() {
        summary(
            functions,
            &fixture,
            flow,
            root,
            &parameters,
            &mut summaries,
            &mut frozen,
        );
    }
    let calls = FrozenFragmentCalls::try_new(&fixture.fragment, &expression_uses, frozen, &Control)
        .unwrap();
    let root = &fixture.fragment.nodes()[&fixture.fragment.root()];
    let result = ResultPort {
        scalar_schema: None,
        fragment: fixture.fragment.id(),
        output: root.output.clone(),
        fields: root
            .output
            .columns
            .iter()
            .enumerate()
            .map(|(ordinal, value)| ResultField {
                domain: novarocks_physical_plan::ResultValueDomain::Plain,
                name: format!("case_{ordinal}").into(),
                alias: None,
                value: *value,
                ty: fixture.fragment.values()[value].ty.clone(),
            })
            .collect::<Vec<_>>()
            .into_boxed_slice(),
    };
    let fragment_id = fixture.fragment.id();
    Arc::new(
        FragmentPackage::try_new(
            FragmentPackageInput {
                version: PlanVersionId::try_new([97; 16]).unwrap(),
                required: RequiredContracts::default(),
                constants,
                fragment: fixture.fragment,
                expression_uses,
                calls,
                cuts: FragmentCuts::default(),
                result: Some(result),
                parameters,
                scans: BTreeMap::new(),
                writes: BTreeMap::new(),
                annotations: Box::default(),
                pruning: FrozenFragmentPruning::try_new(fragment_id, vec![], &Control).unwrap(),
            },
            package_admission(),
            &Control,
        )
        .unwrap(),
    )
}
fn root(mode: Mode) -> ProgramExpressionRootSite {
    ProgramExpressionRootSite::Node {
        node: ProgramNodeId::new(2),
        role: match mode {
            Mode::Filter => ProgramNodeExpressionRole::FilterPredicate { predicate: 0 },
            _ => ProgramNodeExpressionRole::ProjectOutput { expression: 0 },
        },
    }
}
fn compile(
    source: Arc<FragmentPackage>,
    functions: &PureEngineFunctionCatalog,
    control: &dyn PureCompileControl,
) -> Result<novarocks_local_program::LocalProgram, FragmentCompileError> {
    let providers =
        PureProviderProgramCatalog::<std::io::Error>::try_new(&[], vec![], &Control).unwrap();
    compile_fragment(
        validate_fragment_providers(source, &providers, &Control).unwrap(),
        functions,
        options(),
        control,
    )
}
struct RefusingControl {
    cause: CompileControlError,
    stop_at: usize,
    trace: Mutex<Vec<(CompilePhase, u32)>>,
    refused: Mutex<bool>,
}
impl RefusingControl {
    fn new(cause: CompileControlError, stop_at: usize) -> Self {
        Self {
            cause,
            stop_at,
            trace: Mutex::new(vec![]),
            refused: Mutex::new(false),
        }
    }
}
impl PureCompileControl for RefusingControl {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        let mut refused = self.refused.lock().unwrap();
        assert!(!*refused, "no callback after original control refusal");
        let mut trace = self.trace.lock().unwrap();
        trace.push((phase, units));
        if trace.len() == self.stop_at {
            *refused = true;
            Err(self.cause.clone())
        } else {
            Ok(())
        }
    }
}

#[test]
fn searched_and_simple_case_keep_ordered_children_flags_root_types_and_demands() {
    let functions = functions();
    for form in [Form::Searched, Form::SimpleInteger] {
        for mode in [Mode::Project, Mode::Filter] {
            for has_else in [false, true] {
                let fixture =
                    fixture(&functions, form, mode, has_else, Effects::Pure, false).unwrap();
                let ordered = fixture.ordered.clone();
                let physical_case = fixture.case;
                let source = package(&functions, fixture);
                let program = compile(source.clone(), &functions, &Control).unwrap();
                let typed = program.checked().channels().expressions();
                let calls = typed.resolved_calls();
                let snapshot = calls.snapshot();
                let flow = &snapshot.flows()[&ProgramExpressionArena::Main];
                let arena = &snapshot.roots().arenas()[&ProgramExpressionArena::Main];
                let invocation = &flow.uses()[&snapshot.bindings()[&root(mode)]];
                let simple = matches!(form, Form::SimpleInteger);
                let parent_demand = if matches!(mode, Mode::Filter) {
                    EvaluationDemand::TruthOnly
                } else {
                    EvaluationDemand::Value
                };
                assert_eq!(
                    invocation.control,
                    ControlShape::Case {
                        simple,
                        arms: 2,
                        has_else
                    }
                );
                assert_eq!(invocation.context.demand, parent_demand);
                assert_eq!(invocation.context.domain, EvaluationDomainId::new(u32::MAX));
                assert_eq!(
                    source.expression_uses().flow().uses()[&invocation.context.use_id].definition,
                    physical_case
                );
                let StaticExprKind::Case {
                    has_case_expr,
                    has_else_expr,
                    children,
                } = arena.node(invocation.definition).unwrap().kind()
                else {
                    panic!("direct CASE definition")
                };
                assert_eq!(*has_case_expr, simple);
                assert_eq!(*has_else_expr, has_else);
                assert_eq!(
                    children.len(),
                    4 + usize::from(simple) + usize::from(has_else)
                );
                assert_eq!(children.len(), invocation.arguments.len());
                for (ordinal, (&child_id, &child_use)) in
                    children.iter().zip(&invocation.arguments).enumerate()
                {
                    let child = &flow.uses()[&child_use];
                    assert_eq!(child.definition, child_id);
                    assert_eq!(
                        source.expression_uses().flow().uses()[&child_use].definition,
                        ordered[ordinal]
                    );
                    if simple && ordinal == 0 {
                        assert_eq!(child.context.demand, EvaluationDemand::Value);
                        assert_eq!(child.context.domain, invocation.context.domain);
                        assert!(matches!(
                            arena.node(child_id).unwrap().kind(),
                            StaticExprKind::SlotId(_)
                        ));
                        continue;
                    }
                    let position = ordinal - usize::from(simple);
                    let (demand, guard) = if position == 4 {
                        (parent_demand, GuardKind::CaseElse)
                    } else if position.is_multiple_of(2) {
                        (
                            if simple {
                                EvaluationDemand::Value
                            } else {
                                EvaluationDemand::TruthOnly
                            },
                            GuardKind::CaseWhen {
                                arm: (position / 2) as u32,
                            },
                        )
                    } else {
                        (
                            parent_demand,
                            GuardKind::CaseThen {
                                arm: (position / 2) as u32,
                            },
                        )
                    };
                    assert_eq!(child.context.demand, demand);
                    let domain = &flow.domains()[&child.context.domain];
                    assert_eq!(domain.parent, Some(invocation.context.domain));
                    assert_eq!(
                        domain.guard,
                        Some(DomainGuard {
                            owner: invocation.context.use_id,
                            kind: guard
                        })
                    );
                    assert_ne!(child.context.domain, invocation.context.domain);
                }
                assert_eq!(
                    typed.definition_type(ProgramExpressionArena::Main, invocation.definition),
                    Some(&FunctionArgumentType::Value(FunctionValueType::new(
                        DataType::Boolean,
                        !has_else
                    )))
                );
                assert!(calls.calls().is_empty());
                assert!(source.calls().entries().is_empty());
                assert_eq!(program.graph().nodes().len(), 3);
                assert_eq!(
                    program.graph().nodes()[2]
                        .output_layout()
                        .schema()
                        .field(0)
                        .name(),
                    "case_0"
                );
            }
        }
    }
}

#[test]
fn simple_case_retains_exact_nominal_operand_and_when_types_for_later_equality_author() {
    let functions = functions();
    let source = package(
        &functions,
        fixture(
            &functions,
            Form::SimpleUuid,
            Mode::Project,
            true,
            Effects::Pure,
            false,
        )
        .unwrap(),
    );
    let program = compile(source, &functions, &Control).unwrap();
    let channels = program.checked().channels();
    let typed = channels.expressions();
    let snapshot = typed.resolved_calls().snapshot();
    let flow = &snapshot.flows()[&ProgramExpressionArena::Main];
    let invocation = &flow.uses()[&snapshot.bindings()[&root(Mode::Project)]];
    let uuid = FunctionValueType::try_from_legacy_declared_projection(
        DataType::FixedSizeBinary(16),
        true,
        ValueLogicalType::Uuid,
    )
    .expect("valid temporary legacy value projection");
    for ordinal in [0, 1, 3] {
        let child = &flow.uses()[&invocation.arguments[ordinal]];
        assert_eq!(child.context.demand, EvaluationDemand::Value);
        assert_eq!(
            typed.definition_type(ProgramExpressionArena::Main, child.definition),
            Some(&FunctionArgumentType::Value(uuid.clone()))
        );
    }
    assert_eq!(
        channels.channel_type(novarocks_local_program::ProgramChannelSite::Layout {
            node: ProgramNodeId::new(1),
            role: novarocks_local_program::ProgramChannelLayoutRole::NodeOutput,
            ordinal: 1,
        }),
        Some(&uuid)
    );
    assert!(typed.resolved_calls().calls().is_empty());
}

#[test]
fn case_as_actual_if_argument_is_nonconstant_and_preserves_rand_and_round_full_effects() {
    let functions = functions();
    for effects in [Effects::Pure, Effects::Random, Effects::Decimal] {
        let fixture = fixture(
            &functions,
            Form::Searched,
            Mode::IfArgument,
            true,
            effects,
            false,
        )
        .unwrap();
        let physical_case = fixture.case;
        let source = package(&functions, fixture);
        let program = compile(source.clone(), &functions, &Control).unwrap();
        let calls = program.checked().channels().expressions().resolved_calls();
        let snapshot = calls.snapshot();
        let flow = &snapshot.flows()[&ProgramExpressionArena::Main];
        let arena = &snapshot.roots().arenas()[&ProgramExpressionArena::Main];
        let parent = &flow.uses()[&snapshot.bindings()[&root(Mode::IfArgument)]];
        assert_eq!(parent.control, ControlShape::If);
        let left = &flow.uses()[&parent.arguments[1]];
        let right = &flow.uses()[&parent.arguments[2]];
        assert_eq!(left.definition, right.definition);
        assert_ne!(left.context.use_id, right.context.use_id);
        assert_ne!(left.context.domain, right.context.domain);
        for case in [left, right] {
            assert_eq!(
                case.control,
                ControlShape::Case {
                    simple: false,
                    arms: 2,
                    has_else: true
                }
            );
            assert_eq!(case.context.demand, EvaluationDemand::Value);
            assert!(matches!(
                arena.node(case.definition).unwrap().kind(),
                StaticExprKind::Case { .. }
            ));
            assert_eq!(
                source.expression_uses().flow().uses()[&case.context.use_id].definition,
                physical_case
            );
            assert!(
                !source
                    .calls()
                    .entries()
                    .contains_key(&PhysicalCallSite::Expression(case.context.use_id))
            );
        }
        let parent_call = calls
            .calls()
            .values()
            .find(|call| call.call_contract().context() == parent.context)
            .unwrap();
        assert_eq!(
            parent_call.implementation().abi,
            PureKernelAbi::ControlIntrinsicV1
        );
        assert_eq!(
            parent_call.call_contract().effects().argument_control,
            ArgumentControl::If
        );
        let summary = parent_call.effects().for_use(parent.context).unwrap();
        match effects {
            Effects::Pure => {
                assert_eq!(calls.calls().len(), 1);
                assert_eq!(summary.value_stability, FunctionVolatility::Immutable);
                assert!(!summary.may_raise_row_error);
                assert!(!summary.has_instance_state);
            }
            Effects::Random => {
                assert_eq!(calls.calls().len(), 5);
                assert_eq!(summary.value_stability, FunctionVolatility::Volatile);
                assert!(summary.has_instance_state);
                assert!(summary.observable_effects.rng_sampling);
            }
            Effects::Decimal => {
                assert_eq!(calls.calls().len(), 5);
                assert!(summary.may_raise_row_error);
                assert!(!summary.has_instance_state);
            }
        }
        assert_eq!(calls.calls().len(), source.calls().entries().len());
        for call in calls.calls().values() {
            let contract = call.call_contract();
            let frozen =
                &source.calls().entries()[&PhysicalCallSite::Expression(contract.context().use_id)];
            assert_eq!(contract.effects(), &frozen.effects);
            assert_eq!(contract.context(), frozen.context);
            assert_eq!(
                contract.decimal_overflow_policy(),
                DecimalOverflowPolicy::ReportError
            );
            if contract.function_id().as_str() == "builtin.scalar/round/v1" {
                assert_eq!(
                    contract.effects().own_row_error,
                    FunctionIntrinsicRowError::MayRaise
                );
                assert_eq!(
                    contract.selected().result_type,
                    FunctionResultType::Scalar(FunctionValueType::new(
                        DataType::Decimal128(38, 0),
                        true
                    ))
                );
            }
        }
    }
}

#[test]
fn case_wrong_control_arm_order_and_guard_are_rejected_by_the_original_flow_authors() {
    let functions = functions();
    for form in [Form::Searched, Form::SimpleInteger] {
        let fixture = fixture(&functions, form, Mode::Project, true, Effects::Pure, false).unwrap();
        assert!(matches!(
            uses(&fixture, FlowClaim::WrongControl),
            Err(FlowError::Roots(RootUseBindingError::WrongControl))
        ));
        assert!(matches!(
            uses(&fixture, FlowClaim::SwappedArms),
            Err(FlowError::Roots(RootUseBindingError::WrongArguments))
        ));
        let error = uses(&fixture, FlowClaim::WrongGuard).unwrap_err();
        let FlowError::Flow(cause) = error else {
            panic!("invalid guard must fail in the shared control author")
        };
        assert!(!cause.to_string().is_empty());
    }
}

#[test]
fn case_without_else_cannot_claim_a_nonnullable_result() {
    let functions = functions();
    for form in [Form::Searched, Form::SimpleInteger] {
        assert!(
            match fixture(&functions, form, Mode::Project, false, Effects::Pure, true) {
                Err(error) =>
                    error.is_producer_defect()
                        && error
                            .to_string()
                            .contains("CASE result stops admitting null"),
                Ok(_) => false,
            }
        );
    }
}

#[test]
fn case_compile_preserves_all_original_typed_control_refusals_and_stops_callbacks() {
    let functions = functions();
    let source = package(
        &functions,
        fixture(
            &functions,
            Form::Searched,
            Mode::IfArgument,
            true,
            Effects::Decimal,
            false,
        )
        .unwrap(),
    );
    let recorder = RefusingControl::new(CompileControlError::Cancelled, usize::MAX);
    let _ = compile(source.clone(), &functions, &recorder).unwrap();
    let trace = recorder.trace.lock().unwrap().clone();
    assert!(!trace.is_empty());
    assert!(trace.iter().all(|(_, units)| *units <= 256));
    for stop_at in 1..=trace.len() {
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let control = RefusingControl::new(cause.clone(), stop_at);
            assert!(matches!(compile(source.clone(), &functions, &control),
                Err(FragmentCompileError::Control(actual)) if actual == cause));
            assert_eq!(*control.trace.lock().unwrap(), trace[..stop_at]);
        }
    }
}

// Conservative retained-source invoice and independent projection ceilings for
// these small fixtures only; this is not a production default or a MEM grant.
fn package_admission() -> novarocks_physical_plan::FragmentPackageAdmission {
    novarocks_physical_plan::FragmentPackageAdmission {
        plan_limits: novarocks_physical_plan::PlanLimits::FROZEN,
        source_retained_bytes: 64 * 1024 * 1024,
        property_projection_limits: novarocks_physical_plan::PropertyProofProjectionLimits {
            max_request_bytes: 16 * 1024 * 1024,
            max_coexisting_bytes: 256 * 1024 * 1024,
            max_projection_work: 16 * 1024 * 1024,
        },
    }
}
