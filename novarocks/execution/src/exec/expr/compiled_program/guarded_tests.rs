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

// Full checked compiler fixtures use actual builtin owners. They do not thaw
// ExprArena or claim Server catalogue, host allocation, or native integration.
use super::CompiledExpressionInstance;
use arrow::{
    array::{Array, BooleanArray, Decimal128Array, Float64Array, Int64Array},
    datatypes::DataType,
    record_batch::RecordBatch,
};
use novarocks_connector_contract::PureProviderProgramCatalog;
use novarocks_functions::{
    CallEffectInput, ConstantPolicy, ConstantValue, EngineFunctionCatalogBuilder, FunctionArgument,
    FunctionBindingRequest, FunctionBindingSelection, FunctionId, FunctionKind, FunctionOverloadId,
    FunctionResultType, InstalledPureKernel, KernelDiagnostic, KernelEvaluationControl,
    KernelFailure, PureCallPreparation, PureEngineFunctionCatalog, PureImplementationDeclaration,
    PureImplementationId, PureKernelAbi, ScopedExpressionEffects, Selection,
};
use novarocks_local_compiler::{
    LocalCompileOptions, compile_fragment, validate_fragment_providers,
};
use novarocks_local_program::{
    KernelAbiVersion, LocalProgram, ProgramExpressionRootSite, ProgramNodeExpressionRole,
    ProgramNodeId,
};
use novarocks_physical_plan::{
    BoundFunction, ExprId, ExprKind, Fragment, FragmentBuilder, FragmentCuts, FragmentId,
    FragmentPackage, FragmentPackageInput, FragmentSink, FrozenFragmentCalls,
    FrozenFragmentPruning, FrozenPhysicalCall, LiteralValue, NodeId, PhysicalCallSite,
    PhysicalExpressionRoots, PhysicalRootUses, PipelineDopDomain, PlanVersionId, RequiredContracts,
    ResultField, ResultPort, ValueDef, ValueId, ValueOrigin,
};
use novarocks_type_contract::{
    CallProofScope, CompileControlError, CompilePhase, ControlShape, DecimalOverflowPolicy,
    DomainGuard, EvaluationDemand, EvaluationDomainId, ExpressionControlFlow,
    ExpressionEffectContext, ExpressionEvaluationDomain, ExpressionInvocation, ExpressionUseId,
    FunctionValueType, PureCompileControl, SemanticParameters, control_argument_semantics,
};
use std::{
    collections::BTreeMap,
    num::NonZeroUsize,
    sync::{Arc, Mutex},
    time::Duration,
};

struct Control;
impl PureCompileControl for Control {
    fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
        Ok(())
    }
}
impl KernelEvaluationControl for Control {
    fn checkpoint(&self, _: u32) -> Result<(), KernelFailure> {
        Ok(())
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("guarded numeric calls must not wait")
    }
}

#[derive(Clone, Copy)]
enum Shape {
    Random,
    Decimal,
}
fn catalogue(shape: Shape) -> PureEngineFunctionCatalog {
    let actual =
        novarocks_functions::builtin::catalogue::build_builtin_engine_function_catalog().unwrap();
    let mut builder = EngineFunctionCatalogBuilder::new();
    let mut manifest = vec![];
    for (name, overloads, abi) in [
        (
            "if",
            &["(bool,any<T>,any<T>)->any<T>;widen;legacy"][..],
            PureKernelAbi::ControlIntrinsicV1,
        ),
        (
            "coalesce",
            &["(any<T>...)->any<T>;widen;legacy"][..],
            PureKernelAbi::ControlIntrinsicV1,
        ),
        (
            "rand",
            &["()->f64;strict;legacy", "(i64)->f64;strict;legacy"][..],
            PureKernelAbi::ScalarV1,
        ),
        ("round", &["dynamic-v1"][..], PureKernelAbi::ScalarV1),
    ] {
        if name == "round" && matches!(shape, Shape::Random) {
            continue;
        }
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
    // Independent exact installed records seal only these actual test owners.
    builder.seal_pure(manifest).unwrap()
}
fn options() -> LocalCompileOptions {
    LocalCompileOptions {
        pipeline_dop: NonZeroUsize::new(1).unwrap(),
        root_sink_dop: Some(NonZeroUsize::new(1).unwrap()),
        kernel_abi: KernelAbiVersion::CURRENT,
        exchange_wait: std::time::Duration::from_secs(120),
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
pub(crate) struct Author {
    function: BoundFunction,
    selected: Arc<FunctionBindingSelection>,
    arguments: Vec<FunctionArgument>,
    logical_argument_count: usize,
    constant_policy: ConstantPolicy,
    expected_result_type: Option<FunctionValueType>,
    pub(crate) shape: ControlShape,
}
impl Author {
    fn request(&self) -> FunctionBindingRequest<'_> {
        FunctionBindingRequest {
            arguments: &self.arguments,
            logical_argument_count: self.logical_argument_count,
            expected_result_type: self.expected_result_type.as_ref(),
        }
    }
    pub(crate) fn result(&self) -> FunctionValueType {
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
pub(crate) fn author(
    functions: &PureEngineFunctionCatalog,
    name: &str,
    arguments: Vec<FunctionArgument>,
    shape: ControlShape,
) -> Author {
    author_with_target(functions, name, arguments, shape, None)
}

fn trusted_author(
    functions: &PureEngineFunctionCatalog,
    name: &str,
    arguments: Vec<FunctionArgument>,
    shape: ControlShape,
    expected_result_type: FunctionValueType,
) -> Author {
    author_with_target(
        functions,
        name,
        arguments,
        shape,
        Some(expected_result_type),
    )
}

fn author_with_target(
    functions: &PureEngineFunctionCatalog,
    name: &str,
    arguments: Vec<FunctionArgument>,
    shape: ControlShape,
    expected_result_type: Option<FunctionValueType>,
) -> Author {
    let logical_argument_count = arguments.len();
    let request = FunctionBindingRequest {
        arguments: &arguments,
        logical_argument_count,
        expected_result_type: expected_result_type.as_ref(),
    };
    let bound = if expected_result_type.is_some() {
        functions
            .metadata()
            .resolve_bound_trusted(name, FunctionKind::Scalar, request, &Control)
    } else {
        functions
            .metadata()
            .resolve_bound_user(name, FunctionKind::Scalar, request, &Control)
    }
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
        expected_result_type,
        shape,
    }
}
pub(crate) fn call(
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

struct FlowAuthor {
    next_use: u32,
    next_domain: u32,
    uses: Vec<ExpressionInvocation<ExprId>>,
    domains: Vec<ExpressionEvaluationDomain>,
}
impl FlowAuthor {
    fn new() -> Self {
        Self {
            next_use: 7,
            next_domain: 8,
            uses: vec![],
            domains: vec![ExpressionEvaluationDomain {
                id: EvaluationDomainId::new(u32::MAX),
                parent: None,
                guard: None,
            }],
        }
    }
    fn visit(
        &mut self,
        fragment: &Fragment,
        authors: &BTreeMap<ExprId, Author>,
        expr: ExprId,
        domain: EvaluationDomainId,
        demand: EvaluationDemand,
    ) -> ExpressionUseId {
        let id = ExpressionUseId::new(self.next_use);
        self.next_use += 17;
        let definition = fragment.expressions().get(expr).unwrap();
        let mut case_args = Vec::new();
        let temporal = if let ExprKind::FunctionCall { args, .. } = &definition.kind {
            if let ControlShape::TemporalSource(shape) = authors[&expr].shape {
                let mut work = novarocks_type_contract::CompileCheckpoints::try_new(
                    &Control,
                    CompilePhase::FunctionSpecialization,
                )
                .unwrap();
                Some(
                    novarocks_physical_plan::temporal_source_definitions_observed(
                        shape.kind(),
                        fragment.expressions(),
                        args,
                        &mut work,
                    )
                    .unwrap(),
                )
            } else {
                None
            }
        } else {
            None
        };
        let (shape, args) = match &definition.kind {
            ExprKind::FunctionCall { .. } if authors[&expr].shape == ControlShape::NoArguments => {
                (ControlShape::NoArguments, &[][..])
            }
            ExprKind::FunctionCall { args, .. } => {
                temporal
                    .as_ref()
                    .map_or((authors[&expr].shape, args.as_ref()), |source| {
                        (
                            ControlShape::TemporalSource(source.facts.shape()),
                            source.definitions.as_ref(),
                        )
                    })
            }
            ExprKind::Value(_) | ExprKind::Literal(_) | ExprKind::Constant(_) => {
                (ControlShape::Eager, &[][..])
            }
            ExprKind::Binary { left, right, .. } => {
                case_args.extend([*left, *right]);
                (ControlShape::Eager, case_args.as_slice())
            }
            ExprKind::Conjunction { args } => (ControlShape::Conjunction, args.as_ref()),
            ExprKind::Disjunction { args } => (ControlShape::Disjunction, args.as_ref()),
            ExprKind::Unary {
                expr,
                op: novarocks_physical_plan::UnaryOperator::Not,
            }
            | ExprKind::IsNull { expr, .. }
            | ExprKind::Cast { expr, .. } => (ControlShape::Eager, std::slice::from_ref(expr)),
            ExprKind::Case {
                operand,
                when_then,
                else_expr,
            } => {
                if let Some(operand) = operand {
                    case_args.push(*operand);
                }
                for (when, then) in when_then {
                    case_args.extend([*when, *then]);
                }
                if let Some(otherwise) = else_expr {
                    case_args.push(*otherwise);
                }
                (
                    ControlShape::Case {
                        simple: operand.is_some(),
                        arms: u32::try_from(when_then.len()).unwrap(),
                        has_else: else_expr.is_some(),
                    },
                    case_args.as_slice(),
                )
            }
            other => panic!("fixture lacks exact control author for {other:?}"),
        };
        let mut children = vec![];
        for (ordinal, &child) in args.iter().enumerate() {
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
            children.push(self.visit(fragment, authors, child, child_domain, child_demand));
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

fn program(shape: Shape) -> Arc<LocalProgram> {
    let functions = catalogue(shape);
    let fragment_id = FragmentId::new(193);
    let source = NodeId::new(u32::MAX);
    let input = NodeId::new(41);
    let output = NodeId::new(0);
    let flag = ValueId::new(901);
    let seed = ValueId::new(3);
    let decimal_value = ValueId::new(71);
    let fallback = ValueId::new(72);
    let bool_type = FunctionValueType::new(DataType::Boolean, true);
    let integer = FunctionValueType::new(DataType::Int64, true);
    let scalar_integer = FunctionValueType::new(DataType::Int64, false);
    let floating = FunctionValueType::new(DataType::Float64, true);
    let decimal = FunctionValueType::new(DataType::Decimal128(38, 0), true);
    let mut builder = FragmentBuilder::new(fragment_id);
    builder
        .add_values(source, Box::from([Box::default()]), Box::default())
        .unwrap();
    let mut columns = vec![(flag, bool_type.clone()), (seed, integer.clone())];
    if matches!(shape, Shape::Decimal) {
        columns.extend([
            (decimal_value, decimal.clone()),
            (fallback, decimal.clone()),
        ]);
    }
    let mut items = vec![];
    let mut output_values = vec![];
    for (id, ty) in columns {
        // Checked typed NULL source definitions establish the actual input
        // layout; evaluation receives the complete matching Arrow input port.
        let expr = builder
            .add_expression(input, ty.clone(), ExprKind::Literal(LiteralValue::Null))
            .unwrap();
        builder
            .insert_value(ValueDef {
                id,
                ty,
                origin: ValueOrigin::Expr { node: input, expr },
            })
            .unwrap();
        items.push((expr, id));
        output_values.push(id);
    }
    builder
        .add_project(
            input,
            source,
            items.into_boxed_slice(),
            output_values.into_boxed_slice(),
        )
        .unwrap();
    let mut authors = BTreeMap::new();
    let flag_expr = builder
        .add_expression(output, bool_type.clone(), ExprKind::Value(flag))
        .unwrap();
    let root = if matches!(shape, Shape::Random) {
        let seed_expr = builder
            .add_expression(output, integer.clone(), ExprKind::Value(seed))
            .unwrap();
        let from_column = call(
            &mut builder,
            &mut authors,
            author(
                &functions,
                "rand",
                vec![argument(integer, None)],
                ControlShape::Eager,
            ),
            vec![seed_expr],
        );
        let null = builder
            .add_expression(
                output,
                floating.clone(),
                ExprKind::Literal(LiteralValue::Null),
            )
            .unwrap();
        let conditional = call(
            &mut builder,
            &mut authors,
            author(
                &functions,
                "if",
                vec![
                    argument(bool_type, None),
                    argument(floating.clone(), None),
                    argument(floating.clone(), None),
                ],
                ControlShape::If,
            ),
            vec![flag_expr, from_column, null],
        );
        let literal_seed = builder
            .add_expression(
                output,
                scalar_integer.clone(),
                ExprKind::Literal(LiteralValue::Int64(42)),
            )
            .unwrap();
        let from_constant = call(
            &mut builder,
            &mut authors,
            author(
                &functions,
                "rand",
                vec![integer_argument(scalar_integer, 42)],
                ControlShape::Eager,
            ),
            vec![literal_seed],
        );
        call(
            &mut builder,
            &mut authors,
            author(
                &functions,
                "coalesce",
                vec![argument(floating.clone(), None), argument(floating, None)],
                ControlShape::Coalesce,
            ),
            vec![conditional, from_constant],
        )
    } else {
        let value = builder
            .add_expression(output, decimal.clone(), ExprKind::Value(decimal_value))
            .unwrap();
        let fallback_expr = builder
            .add_expression(output, decimal.clone(), ExprKind::Value(fallback))
            .unwrap();
        let digits = builder
            .add_expression(
                output,
                scalar_integer.clone(),
                ExprKind::Literal(LiteralValue::Int64(-1)),
            )
            .unwrap();
        let rounded = call(
            &mut builder,
            &mut authors,
            author(
                &functions,
                "round",
                vec![
                    argument(decimal.clone(), None),
                    integer_argument(scalar_integer, -1),
                ],
                ControlShape::Eager,
            ),
            vec![value, digits],
        );
        let coalesced = call(
            &mut builder,
            &mut authors,
            author(
                &functions,
                "coalesce",
                vec![
                    argument(decimal.clone(), None),
                    argument(decimal.clone(), None),
                ],
                ControlShape::Coalesce,
            ),
            vec![rounded, fallback_expr],
        );
        call(
            &mut builder,
            &mut authors,
            author(
                &functions,
                "if",
                vec![
                    argument(bool_type, None),
                    argument(decimal.clone(), None),
                    argument(decimal, None),
                ],
                ControlShape::If,
            ),
            vec![flag_expr, coalesced, fallback_expr],
        )
    };
    let result_type = authors[&root].result();
    let result_value = builder
        .add_value(
            result_type.clone(),
            ValueOrigin::Expr {
                node: output,
                expr: root,
            },
        )
        .unwrap();
    builder
        .add_project(
            output,
            input,
            Box::from([(root, result_value)]),
            Box::from([result_value]),
        )
        .unwrap();
    let fragment = builder
        .finish_definition(
            output,
            FragmentSink::Result,
            PipelineDopDomain {
                min: 1,
                max: 1,
                requires_power_of_two: false,
            },
        )
        .unwrap();
    let result = ResultPort {
        scalar_schema: None,
        fragment: fragment_id,
        output: fragment.nodes()[&output].output.clone(),
        fields: Box::from([ResultField {
            domain: crate::test_result_domain::result_value_domain(&result_type),
            name: "guarded_result".into(),
            alias: None,
            value: result_value,
            ty: result_type,
        }]),
    };
    compile_checked_fragment(&functions, fragment, &authors, result)
}

fn compile_checked_fragment(
    functions: &PureEngineFunctionCatalog,
    fragment: Fragment,
    authors: &BTreeMap<ExprId, Author>,
    result: ResultPort,
) -> Arc<LocalProgram> {
    compile_checked_fragment_with_parameters(
        functions,
        fragment,
        authors,
        result,
        SemanticParameters::try_new([]).unwrap(),
    )
}
pub(crate) fn compile_checked_fragment_with_parameters(
    functions: &PureEngineFunctionCatalog,
    fragment: Fragment,
    authors: &BTreeMap<ExprId, Author>,
    result: ResultPort,
    parameters: SemanticParameters,
) -> Arc<LocalProgram> {
    let (fragment, constants) = original_request_sources(fragment, authors);
    let fragment_id = fragment.id();
    let roots = PhysicalExpressionRoots::try_new(&fragment, &Control).unwrap();
    let mut flow_author = FlowAuthor::new();
    let mut bindings = vec![];
    for (&site, root) in roots.sites() {
        let id = flow_author.visit(
            &fragment,
            authors,
            root.expr,
            EvaluationDomainId::new(u32::MAX),
            root.demand,
        );
        bindings.push((site, id));
    }
    let ordered_uses = flow_author.uses;
    let flow = ExpressionControlFlow::try_new(
        flow_author.domains,
        ordered_uses.clone(),
        fragment.expressions(),
        CompilePhase::Validate,
        &Control,
    )
    .unwrap();
    let uses = PhysicalRootUses::try_new(&fragment, flow.clone(), bindings, &Control).unwrap();
    let mut summaries = BTreeMap::new();
    let mut frozen = vec![];
    for invocation in ordered_uses {
        let context = invocation.context;
        let scoped = if let Some(owner) = authors.get(&invocation.definition) {
            let mut children = ScopedExpressionEffects::pure_value(context);
            for (ordinal, child) in invocation.arguments.iter().enumerate() {
                children = if matches!(
                    owner.shape,
                    ControlShape::If | ControlShape::Coalesce | ControlShape::TemporalSource(_)
                ) {
                    children
                        .join_control_argument(summaries[child], &flow, ordinal)
                        .unwrap()
                } else {
                    children.join_same_domain(summaries[child]).unwrap()
                };
            }
            let argument_uses = if owner.shape == ControlShape::NoArguments {
                vec![None; owner.logical_argument_count]
            } else {
                invocation
                    .arguments
                    .iter()
                    .map(|id| Some(*id))
                    .collect::<Vec<_>>()
            };
            let preparation = if matches!(
                owner.shape,
                ControlShape::If | ControlShape::Coalesce | ControlShape::TemporalSource(_)
            ) {
                PureCallPreparation::ControlIntrinsic {
                    arguments: children,
                }
            } else {
                PureCallPreparation::Scalar {
                    arguments: children,
                }
            };
            let source_plan = if let ControlShape::TemporalSource(shape) = invocation.control {
                let source = fragment.expressions().get(invocation.definition).unwrap();
                let ExprKind::FunctionCall { args, .. } = &source.kind else {
                    panic!("source call");
                };
                let mut work = novarocks_type_contract::CompileCheckpoints::try_new(
                    &Control,
                    CompilePhase::FunctionSpecialization,
                )
                .unwrap();
                let projected = novarocks_physical_plan::temporal_source_definitions_observed(
                    shape.kind(),
                    fragment.expressions(),
                    args,
                    &mut work,
                )
                .unwrap();
                Some(novarocks_type_contract::TemporalSourcePlan {
                    facts: projected.facts,
                    sources: projected
                        .definitions
                        .iter()
                        .zip(&invocation.arguments)
                        .enumerate()
                        .map(|(ordinal, (&definition, &use_id))| {
                            novarocks_type_contract::TemporalSourceOccurrence {
                                role: shape.roles()[ordinal].unwrap(),
                                definition,
                                use_id,
                            }
                        })
                        .collect(),
                })
            } else {
                None
            };
            let source_channels = source_plan
                .as_ref()
                .map(|source| {
                    source
                        .sources
                        .iter()
                        .map(|channel| novarocks_functions::TemporalSourceChannel {
                            role: channel.role,
                            context: flow.uses()[&channel.use_id].context,
                            value_type: &fragment.expressions().get(channel.definition).unwrap().ty,
                        })
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            let regexp_count_pattern_source =
                if owner.function.function_id.as_str() == "builtin.scalar/regexp_count/v1" {
                    let definition = fragment.expressions().get(invocation.definition).unwrap();
                    let ExprKind::FunctionCall { args, .. } = &definition.kind else {
                        panic!("count actual call source")
                    };
                    let mut work = novarocks_type_contract::CompileCheckpoints::try_new(
                        &Control,
                        CompilePhase::FunctionSpecialization,
                    )
                    .unwrap();
                    let source = novarocks_physical_plan::regexp_count_pattern_source_observed(
                        fragment.expressions(),
                        args,
                        &mut work,
                    )
                    .unwrap();
                    work.finish().unwrap();
                    Some(source)
                } else {
                    None
                };
            let to_base64_byte_source =
                if owner.function.function_id.as_str() == "builtin.scalar/to_base64/v1" {
                    let definition = fragment.expressions().get(invocation.definition).unwrap();
                    let ExprKind::FunctionCall { args, .. } = &definition.kind else {
                        panic!("base64 actual call source")
                    };
                    let mut work = novarocks_type_contract::CompileCheckpoints::try_new(
                        &Control,
                        CompilePhase::FunctionSpecialization,
                    )
                    .unwrap();
                    let source = novarocks_physical_plan::to_base64_byte_source_observed(
                        fragment.expressions(),
                        args,
                        &mut work,
                    )
                    .unwrap();
                    work.finish().unwrap();
                    Some(source)
                } else {
                    None
                };
            let token = functions
                .prepare_fresh(
                    CallEffectInput {
                        context,
                        argument_uses: match &source_plan {
                            Some(source) => {
                                novarocks_functions::CallArgumentUses::TemporalSources {
                                    facts: &source.facts,
                                    channels: &source_channels,
                                }
                            }
                            None => match regexp_count_pattern_source {
                                Some(source) => {
                                    novarocks_functions::CallArgumentUses::RegexpCountPattern {
                                        source,
                                        channels: &argument_uses,
                                    }
                                }
                                None => match to_base64_byte_source {
                                    Some(source) => {
                                        novarocks_functions::CallArgumentUses::ToBase64Bytes {
                                            source,
                                            channels: &argument_uses,
                                        }
                                    }
                                    None => {
                                        novarocks_functions::CallArgumentUses::SelectedChannels(
                                            &argument_uses,
                                        )
                                    }
                                },
                            },
                        },
                        function_id: &owner.function.function_id,
                        kind: FunctionKind::Scalar,
                        selected: owner.selected.as_ref(),
                        request: owner.request(),
                        environment: &[],
                        parameters: &parameters,
                        decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
                        proof_scope: CallProofScope::Domain(context.domain),
                    },
                    owner.selected.clone(),
                    preparation,
                    &Control,
                )
                .unwrap();
            frozen.push(FrozenPhysicalCall {
                site: PhysicalCallSite::Expression(context.use_id),
                context,
                effects: token.call_contract().effects().clone(),
                decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
                regexp_count_pattern_source,
                to_base64_byte_source,
                temporal_source: source_plan,
            });
            token.effects()
        } else {
            let mut joined = ScopedExpressionEffects::pure_value(context);
            for (ordinal, child) in invocation.arguments.iter().enumerate() {
                joined = joined
                    .join_control_argument(summaries[child], &flow, ordinal)
                    .unwrap();
            }
            joined
        };
        summaries.insert(context.use_id, scoped);
    }
    let calls = FrozenFragmentCalls::try_new(&fragment, &uses, frozen, &Control).unwrap();
    let package = Arc::new(
        FragmentPackage::try_new(
            FragmentPackageInput {
                version: PlanVersionId::try_new([193; 16]).unwrap(),
                required: RequiredContracts::default(),
                constants,
                fragment,
                expression_uses: uses,
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
    );
    let providers =
        PureProviderProgramCatalog::<std::io::Error>::try_new(&[], vec![], &Control).unwrap();
    Arc::new(
        compile_fragment(
            validate_fragment_providers(package, &providers, &Control).unwrap(),
            functions,
            options(),
            &Control,
        )
        .unwrap(),
    )
}
fn root() -> ProgramExpressionRootSite {
    ProgramExpressionRootSite::Node {
        node: ProgramNodeId::new(2),
        role: ProgramNodeExpressionRole::ProjectOutput { expression: 0 },
    }
}
fn instance(program: &Arc<LocalProgram>) -> CompiledExpressionInstance {
    CompiledExpressionInstance::try_new(program.clone(), root(), &Control).unwrap()
}
fn batch(program: &LocalProgram, flags: Vec<Option<bool>>, seeds: Vec<Option<i64>>) -> RecordBatch {
    RecordBatch::try_new(
        program.graph().nodes()[1].output_layout().schema().clone(),
        vec![
            Arc::new(BooleanArray::from(flags)),
            Arc::new(Int64Array::from(seeds)),
        ],
    )
    .unwrap()
}
fn decimal_batch(
    program: &LocalProgram,
    flags: Vec<Option<bool>>,
    values: Vec<Option<i128>>,
    fallback: Vec<Option<i128>>,
) -> RecordBatch {
    let rows = flags.len();
    RecordBatch::try_new(
        program.graph().nodes()[1].output_layout().schema().clone(),
        vec![
            Arc::new(BooleanArray::from(flags)),
            Arc::new(Int64Array::from(vec![Some(42); rows])),
            Arc::new(
                Decimal128Array::from(values)
                    .with_precision_and_scale(38, 0)
                    .unwrap(),
            ),
            Arc::new(
                Decimal128Array::from(fallback)
                    .with_precision_and_scale(38, 0)
                    .unwrap(),
            ),
        ],
    )
    .unwrap()
}

// Frozen independent rand 0.8.5 StdRng seed_from_u64 oracle, not evaluator output.
const SEED_42: [u64; 6] = [
    0x3fe0d98eec6444e4,
    0x3fe15e014267f5aa,
    0x3fe45dec0e3bca26,
    0x3fd9fa4b5e3f5d8c,
    0x3fa19561bff02330,
    0x3fda8ea728e783e0,
];
const SEED_1_FIRST: u64 = 0x3fef2d034c9a6603;

#[test]
fn nested_if_coalesce_maps_sparse_original_rows_and_constant_versus_column_rng() {
    let program = program(Shape::Random);
    let input = batch(
        &program,
        vec![
            Some(false),
            Some(true),
            Some(false),
            Some(false),
            Some(true),
            None,
        ],
        vec![Some(42), Some(1), None, Some(99), Some(42), None],
    );
    let rows = [1, 3, 5];
    let selection = Selection::try_sparse(6, &rows).unwrap();
    let mut evaluator = instance(&program);
    let output = evaluator.evaluate(&input, selection, &Control).unwrap();
    assert_eq!(output.selection(), selection);
    assert!(output.errors().is_empty());
    let values = output
        .values()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(
        values
            .iter()
            .map(|v| v.unwrap().to_bits())
            .collect::<Vec<_>>(),
        vec![SEED_1_FIRST, SEED_42[0], SEED_42[1]]
    );
    let next = batch(
        &program,
        vec![Some(true), Some(false), None],
        vec![Some(1), None, None],
    );
    let output = evaluator
        .evaluate(&next, Selection::all(3), &Control)
        .unwrap();
    let values = output
        .values()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(
        values
            .iter()
            .map(|v| v.unwrap().to_bits())
            .collect::<Vec<_>>(),
        vec![SEED_1_FIRST, SEED_42[2], SEED_42[3]]
    );
}
#[test]
fn guarded_constant_rng_stays_uninstantiated_until_coalesce_really_needs_it() {
    let program = program(Shape::Random);
    let mut evaluator = instance(&program);
    let true_only = batch(&program, vec![Some(true); 3], vec![Some(42); 3]);
    let output = evaluator
        .evaluate(&true_only, Selection::all(3), &Control)
        .unwrap();
    let values = output
        .values()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert!(values.iter().all(|v| v.unwrap().to_bits() == SEED_42[0]));
    assert_eq!(evaluator.instances.len(), 1);
    let false_only = batch(&program, vec![Some(false); 2], vec![None; 2]);
    let output = evaluator
        .evaluate(&false_only, Selection::all(2), &Control)
        .unwrap();
    let values = output
        .values()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(
        values
            .iter()
            .map(|v| v.unwrap().to_bits())
            .collect::<Vec<_>>(),
        SEED_42[..2]
    );
    assert_eq!(evaluator.instances.len(), 2);
}
#[test]
fn empty_guarded_selection_creates_no_kernel_instance_and_does_not_advance_rng() {
    let program = program(Shape::Random);
    let mut evaluator = instance(&program);
    let input = batch(&program, vec![Some(false); 3], vec![None; 3]);
    let rows = [];
    let output = evaluator
        .evaluate(&input, Selection::try_sparse(3, &rows).unwrap(), &Control)
        .unwrap();
    assert!(output.values().is_empty());
    assert!(output.errors().is_empty());
    assert!(evaluator.instances.is_empty());
    let one = batch(&program, vec![None], vec![None]);
    let output = evaluator
        .evaluate(&one, Selection::all(1), &Control)
        .unwrap();
    assert_eq!(
        output
            .values()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0)
            .to_bits(),
        SEED_42[0]
    );
    assert_eq!(evaluator.instances.len(), 1);
}
#[test]
fn guarded_round_skips_inactive_overflow_and_remaps_required_error_without_null_fallback() {
    let program = program(Shape::Decimal);
    let max38 = 10_i128.pow(38) - 1;
    let input = decimal_batch(
        &program,
        vec![
            Some(true),
            None,
            Some(true),
            Some(true),
            Some(true),
            Some(true),
            Some(false),
        ],
        vec![
            Some(max38),
            Some(max38),
            Some(max38),
            None,
            Some(25),
            Some(max38),
            Some(max38),
        ],
        vec![Some(71); 7],
    );
    let rows = [1, 3, 5, 6];
    let selection = Selection::try_sparse(7, &rows).unwrap();
    let mut evaluator = instance(&program);
    let output = evaluator.evaluate(&input, selection, &Control).unwrap();
    let values = output
        .values()
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    assert_eq!(values.value(0), 71);
    assert_eq!(values.value(1), 71);
    assert!(values.is_null(2));
    assert_eq!(values.value(3), 71);
    assert_eq!(output.errors().len(), 1);
    assert_eq!(output.errors()[0].selected_ordinal(), 2);
    assert!(output.errors()[0].message().contains("overflow"));
    let clean = decimal_batch(&program, vec![Some(true)], vec![Some(25)], vec![Some(71)]);
    let output = evaluator
        .evaluate(&clean, Selection::all(1), &Control)
        .unwrap();
    assert!(output.errors().is_empty());
    assert_eq!(
        output
            .values()
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap()
            .value(0),
        30
    );
}
#[test]
fn only_false_or_null_if_rows_never_instantiate_guarded_round_even_with_valid_overflow_values() {
    let program = program(Shape::Decimal);
    let mut evaluator = instance(&program);
    let input = decimal_batch(
        &program,
        vec![Some(false), None],
        vec![Some(10_i128.pow(38) - 1); 2],
        vec![Some(17); 2],
    );
    let output = evaluator
        .evaluate(&input, Selection::all(2), &Control)
        .unwrap();
    assert!(output.errors().is_empty());
    assert!(evaluator.instances.is_empty());
    assert_eq!(
        output
            .values()
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some(17), Some(17)]
    );
}

struct CallbackControl {
    cause: KernelFailure,
    stop_at: usize,
    trace: Mutex<Vec<u32>>,
    refused: Mutex<bool>,
}
impl CallbackControl {
    fn new(cause: KernelFailure, stop_at: usize) -> Self {
        Self {
            cause,
            stop_at,
            trace: Mutex::new(vec![]),
            refused: Mutex::new(false),
        }
    }
}
impl KernelEvaluationControl for CallbackControl {
    fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
        let mut refused = self.refused.lock().unwrap();
        assert!(!*refused, "no callback after primary refusal");
        let mut trace = self.trace.lock().unwrap();
        trace.push(units);
        if trace.len() == self.stop_at {
            *refused = true;
            Err(self.cause.clone())
        } else {
            Ok(())
        }
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("guarded numeric calls must not wait")
    }
}
fn causes() -> [KernelFailure; 7] {
    [
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
        KernelFailure::InvalidProgram(KernelDiagnostic::new("guarded-origin-invalid")),
        KernelFailure::Internal(KernelDiagnostic::new("guarded-origin-internal")),
        KernelFailure::Operational(KernelDiagnostic::new("guarded-origin-operational")),
        KernelFailure::InstanceFailed,
    ]
}
#[test]
fn every_guarded_constructor_callback_preserves_all_original_failure_categories() {
    let program = program(Shape::Random);
    let recorder = CallbackControl::new(KernelFailure::Cancelled, usize::MAX);
    let _ = CompiledExpressionInstance::try_new(program.clone(), root(), &recorder).unwrap();
    let trace = recorder.trace.lock().unwrap().clone();
    assert!(!trace.is_empty());
    for index in 1..=trace.len() {
        for cause in causes() {
            let control = CallbackControl::new(cause.clone(), index);
            assert!(
                matches!(CompiledExpressionInstance::try_new(program.clone(), root(), &control), Err(actual) if actual == cause)
            );
            assert_eq!(*control.trace.lock().unwrap(), trace[..index]);
        }
    }
}
#[test]
fn every_guarded_evaluation_callback_preserves_primary_failure_and_never_replays_state() {
    let program = program(Shape::Random);
    let input = batch(
        &program,
        (0..320)
            .map(|row| {
                if row % 3 == 0 {
                    None
                } else {
                    Some(row % 2 == 0)
                }
            })
            .collect(),
        vec![Some(42); 320],
    );
    let recorder = CallbackControl::new(KernelFailure::Cancelled, usize::MAX);
    let _ = instance(&program)
        .evaluate(&input, Selection::all(320), &recorder)
        .unwrap();
    let trace = recorder.trace.lock().unwrap().clone();
    assert!(trace.contains(&256));
    assert!(trace.iter().all(|units| *units <= 256));
    for index in 1..=trace.len() {
        for cause in causes() {
            let mut evaluator = instance(&program);
            let control = CallbackControl::new(cause.clone(), index);
            assert!(
                matches!(evaluator.evaluate(&input, Selection::all(320), &control), Err(actual) if actual == cause)
            );
            assert_eq!(*control.trace.lock().unwrap(), trace[..index]);
            let after = CallbackControl::new(KernelFailure::Cancelled, usize::MAX);
            assert!(matches!(
                evaluator.evaluate(&input, Selection::all(320), &after),
                Err(KernelFailure::InstanceFailed)
            ));
            assert!(after.trace.lock().unwrap().is_empty());
        }
    }
}

#[path = "unary_tests.rs"]
mod unary_tests;

#[path = "case_tests.rs"]
mod case_tests;

#[path = "byte_guarded_tests.rs"]
mod byte_guarded_tests;

#[path = "equality_tests.rs"]
mod equality_tests;

#[path = "arithmetic_tests.rs"]
mod arithmetic_tests;

#[path = "value_conversion_tests.rs"]
mod value_conversion_tests;

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

#[path = "temporal_tests.rs"]
mod temporal_tests;

/// A real executed InvocationData phase, captured before bounded row projection.
/// The journal is thread local and enabled only by an independent differential.
#[derive(Clone, Debug)]
pub(crate) struct TemporalInvocationDataProbe {
    pub shape: novarocks_type_contract::TemporalSourceShape,
    pub stage: usize,
    pub message: String,
    pub invocation_rows: Vec<usize>,
    pub affected_rows: Vec<usize>,
    pub prior_errors: Vec<(usize, String)>,
}
thread_local! {
    static TEMPORAL_DATA_PROBES: std::cell::RefCell<Option<Vec<TemporalInvocationDataProbe>>> =
        const { std::cell::RefCell::new(None) };
}
pub(crate) struct TemporalProbeScope(Option<Vec<TemporalInvocationDataProbe>>);
impl TemporalProbeScope {
    pub(crate) fn enter() -> Self {
        Self(TEMPORAL_DATA_PROBES.with(|slot| slot.replace(Some(Vec::new()))))
    }
    pub(crate) fn take(&self) -> Vec<TemporalInvocationDataProbe> {
        TEMPORAL_DATA_PROBES.with(|slot| std::mem::take(slot.borrow_mut().as_mut().unwrap()))
    }
}
impl Drop for TemporalProbeScope {
    fn drop(&mut self) {
        TEMPORAL_DATA_PROBES.with(|slot| {
            slot.replace(self.0.take());
        });
    }
}
pub(crate) fn record_temporal_invocation_data(
    shape: novarocks_type_contract::TemporalSourceShape,
    stage: usize,
    message: &str,
    invocation_rows: &[usize],
    affected_ordinals: &[usize],
    prior_errors: &BTreeMap<usize, novarocks_functions::RowDataError>,
) {
    TEMPORAL_DATA_PROBES.with(|slot| {
        if let Some(journal) = slot.borrow_mut().as_mut() {
            journal.push(TemporalInvocationDataProbe {
                shape,
                stage,
                message: message.to_owned(),
                invocation_rows: invocation_rows.to_vec(),
                affected_rows: affected_ordinals
                    .iter()
                    .map(|i| invocation_rows[*i])
                    .collect(),
                prior_errors: prior_errors
                    .iter()
                    .map(|(i, e)| (invocation_rows[*i], e.message().to_owned()))
                    .collect(),
            });
        }
    });
}
#[path = "regexp_count_tests.rs"]
mod regexp_count_tests;

#[path = "parse_url_tests.rs"]
mod parse_url_tests;

#[path = "to_base64_tests.rs"]
mod to_base64_tests;

#[path = "to_binary_after_tests.rs"]
mod to_binary_after_tests;

#[path = "ds_hll_state_frame_tests.rs"]
mod ds_hll_state_frame_tests;

#[path = "ds_hll_state_private_frame_tests.rs"]
mod ds_hll_state_private_frame_tests;

#[path = "percentile_raw_private_frame_tests.rs"]
mod percentile_raw_private_frame_tests;

#[path = "scalar_invocation_frame_tests.rs"]
mod scalar_invocation_frame_tests;
