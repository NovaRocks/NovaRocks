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
    CallEffectInput, ConstantPolicy, EngineFunctionCatalogBuilder, FunctionArgument,
    FunctionArgumentType, FunctionBindingRequest, FunctionBindingSelection, FunctionId,
    FunctionKind, FunctionOverloadId, FunctionResultType, InstalledPureKernel, PureCallPreparation,
    PureEngineFunctionCatalog, PureImplementationDeclaration, PureImplementationId, PureKernelAbi,
    ScopedExpressionEffects,
};
use novarocks_local_program::{
    KernelAbiVersion, ProgramChannelLayoutRole, ProgramChannelSite, ProgramExpressionArena,
    ProgramExpressionRootSite, ProgramLexicalSource, ProgramNodeExpressionRole, ProgramNodeId,
    ProgramUseRef, StaticExprKind,
};
use novarocks_physical_plan::{
    BoundFunction, ExprId, ExprKind, Fragment, FragmentBuilder, FragmentCuts, FragmentId,
    FragmentPackageInput, FragmentSink, FrozenFragmentCalls, FrozenFragmentPruning,
    FrozenPhysicalCall, LiteralValue, PhysicalCallSite, PhysicalExpressionRoots, PhysicalRootUses,
    PipelineDopDomain, PlanVersionId, RequiredContracts, ResultField, ResultPort,
    RootUseBindingError, UnaryOperator, ValidationErrors, ValueOrigin,
};
use novarocks_type_contract::{
    ArgumentControl, CallProofScope, ControlShape, DecimalOverflowPolicy, DomainGuard,
    EvaluationDemand, EvaluationDomainId, ExpressionControlFlow, ExpressionEffectContext,
    ExpressionEvaluationDomain, ExpressionInvocation, ExpressionUseId, FunctionValueType,
    SemanticParameters, ValueLogicalType, control_argument_semantics,
};
use std::{num::NonZeroUsize, sync::Mutex};

struct Control;
impl PureCompileControl for Control {
    fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
        Ok(())
    }
}
fn functions() -> PureEngineFunctionCatalog {
    functions_with_if(true)
}
fn functions_with_if(include_if: bool) -> PureEngineFunctionCatalog {
    let actual =
        novarocks_functions::builtin::catalogue::build_builtin_engine_function_catalog().unwrap();
    let mut builder = EngineFunctionCatalogBuilder::new();
    builder
        .register(
            actual
                .definition("rand", FunctionKind::Scalar)
                .unwrap()
                .clone(),
        )
        .unwrap();
    // The catalogue requires a nonempty seal. This actual unused RAND subset
    // grants no function-call identity to the intrinsic Boolean package.
    let mut manifest = ["()->f64;strict;legacy", "(i64)->f64;strict;legacy"]
        .into_iter()
        .map(|overload| InstalledPureKernel {
            function: FunctionId::try_new("builtin.scalar/rand/v1").unwrap(),
            kind: FunctionKind::Scalar,
            implementation: PureImplementationDeclaration {
                overload: FunctionOverloadId::try_new(format!("builtin.scalar/rand/{overload}"))
                    .unwrap(),
                implementation: PureImplementationId::try_new("builtin.scalar/rand/selected-v1")
                    .unwrap(),
                abi: PureKernelAbi::ScalarV1,
            },
            aggregate_state_format: None,
        })
        .collect::<Vec<_>>();
    if include_if {
        builder
            .register(
                actual
                    .definition("if", FunctionKind::Scalar)
                    .unwrap()
                    .clone(),
            )
            .unwrap();
        manifest.push(InstalledPureKernel {
            function: FunctionId::try_new("builtin.scalar/if/v1").unwrap(),
            kind: FunctionKind::Scalar,
            implementation: PureImplementationDeclaration {
                overload: FunctionOverloadId::try_new(
                    "builtin.scalar/if/(bool,any<T>,any<T>)->any<T>;widen;legacy",
                )
                .unwrap(),
                implementation: PureImplementationId::try_new("builtin.scalar/if/selected-v1")
                    .unwrap(),
                abi: PureKernelAbi::ControlIntrinsicV1,
            },
            aggregate_state_format: None,
        });
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

#[derive(Clone, Copy, Debug)]
enum Operation {
    Not,
    IsNull,
    IsNotNull,
}
impl Operation {
    fn kind(self, expr: ExprId) -> ExprKind {
        match self {
            Self::Not => ExprKind::Unary {
                op: UnaryOperator::Not,
                expr,
            },
            Self::IsNull | Self::IsNotNull => ExprKind::IsNull {
                expr,
                negated: matches!(self, Self::IsNotNull),
            },
        }
    }
    fn result(self, input: &FunctionValueType) -> FunctionValueType {
        FunctionValueType::new(
            DataType::Boolean,
            matches!(self, Self::Not) && input.nullable(),
        )
    }
}
#[derive(Clone, Copy)]
enum Mode {
    Project,
    Filter,
    IfArgument,
}
#[derive(Clone, Copy)]
enum Claim {
    Accurate,
    WrongResult,
}
struct IfAuthor {
    selected: Arc<FunctionBindingSelection>,
    arguments: Vec<FunctionArgument>,
    logical_argument_count: usize,
    constant_policy: ConstantPolicy,
    function: BoundFunction,
}
impl IfAuthor {
    fn request(&self) -> FunctionBindingRequest<'_> {
        FunctionBindingRequest {
            arguments: &self.arguments,
            logical_argument_count: self.logical_argument_count,
            expected_result_type: None,
        }
    }
}
// Transfer the first resolver request; physical child shapes are not request sources.
fn original_request_sources(
    fragment: Fragment,
    call: &Option<(ExprId, IfAuthor)>,
) -> (Fragment, novarocks_physical_plan::ConstantPools) {
    use novarocks_physical_plan::{
        ConstantPoolId, ConstantPools, ConstantReference, PhysicalCallDefinition,
        PhysicalCallRequest, StaticFunctionArgument,
    };
    let mut pools = ConstantPools::empty();
    let mut backing_ids = std::collections::BTreeMap::new();
    let mut entries = vec![];
    for (definition, owner) in call.iter() {
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
            PhysicalCallDefinition::Expression(*definition),
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
struct Fixture {
    fragment: Fragment,
    wrapper: ExprId,
    input: ExprId,
    call: Option<(ExprId, IfAuthor)>,
}
fn fixture(
    functions: &PureEngineFunctionCatalog,
    operation: Operation,
    mode: Mode,
    input_type: FunctionValueType,
    claim: Claim,
) -> Result<Fixture, ValidationErrors> {
    let source = NodeId::new(u32::MAX);
    let input = NodeId::new(44);
    let root = NodeId::new(0);
    let mut builder = FragmentBuilder::new(FragmentId::new(93));
    builder
        .add_values(source, Box::from([Box::default()]), Box::default())
        .unwrap();
    let literal = builder
        .add_expression(
            input,
            input_type.clone(),
            ExprKind::Literal(LiteralValue::Null),
        )
        .unwrap();
    let value = builder
        .add_value(
            input_type.clone(),
            ValueOrigin::Expr {
                node: input,
                expr: literal,
            },
        )
        .unwrap();
    builder
        .add_project(
            input,
            source,
            Box::from([(literal, value)]),
            Box::from([value]),
        )
        .unwrap();
    let reference = builder
        .add_expression(root, input_type.clone(), ExprKind::Value(value))
        .unwrap();
    let mut result_type = operation.result(&input_type);
    if matches!(claim, Claim::WrongResult) {
        result_type = result_type.with_nullable(!result_type.nullable()).unwrap();
    }
    let wrapper = builder
        .add_expression(root, result_type.clone(), operation.kind(reference))
        .unwrap();
    let call = if matches!(mode, Mode::IfArgument) {
        let arguments = vec![
            FunctionArgument::Value {
                value_type: result_type.clone(),
                constant: None
            };
            3
        ];
        let logical_argument_count = arguments.len();
        let request = FunctionBindingRequest {
            arguments: &arguments,
            logical_argument_count,
            expected_result_type: None,
        };
        let bound = functions
            .metadata()
            .resolve_bound_user("if", FunctionKind::Scalar, request, &Control)
            .unwrap();
        let selected = Arc::new(bound.selected.clone());
        let FunctionResultType::Scalar(result) = &selected.result_type else {
            panic!("real IF scalar owner")
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
        let call = builder
            .add_expression(
                root,
                result.clone(),
                ExprKind::FunctionCall {
                    function: function.clone(),
                    args: Box::from([wrapper; 3]),
                },
            )
            .unwrap();
        Some((
            call,
            IfAuthor {
                selected,
                arguments,
                logical_argument_count,
                constant_policy: options().constants,
                function,
            },
        ))
    } else {
        None
    };
    let root_expr = call.as_ref().map(|(id, _)| *id).unwrap_or(wrapper);
    match mode {
        Mode::Project | Mode::IfArgument => {
            let output_type = if let Some((_, author)) = &call {
                author.function.result_type.clone()
            } else {
                result_type.clone()
            };
            let output = builder
                .add_value(
                    output_type,
                    ValueOrigin::Expr {
                        node: root,
                        expr: root_expr,
                    },
                )
                .unwrap();
            builder
                .add_project(
                    root,
                    input,
                    Box::from([(root_expr, output)]),
                    Box::from([output]),
                )
                .unwrap();
        }
        Mode::Filter => {
            builder
                .add_filter(root, input, Box::from([root_expr]))
                .unwrap();
        }
    }
    let fragment = builder.finish_definition(
        root,
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
        wrapper,
        input: reference,
        call,
    })
}
#[derive(Clone, Copy)]
enum Flow {
    Accurate,
    WrongControl,
}
fn uses(fixture: &Fixture, claim: Flow) -> Result<PhysicalRootUses, RootUseBindingError> {
    struct Author {
        next_use: u32,
        next_domain: u32,
        uses: Vec<ExpressionInvocation<ExprId>>,
        domains: Vec<ExpressionEvaluationDomain>,
    }
    impl Author {
        fn visit(
            &mut self,
            fixture: &Fixture,
            expr: ExprId,
            domain: EvaluationDomainId,
            demand: EvaluationDemand,
            claim: Flow,
        ) -> ExpressionUseId {
            let id = ExpressionUseId::new(self.next_use);
            self.next_use += 17;
            let (mut shape, args) = match &fixture.fragment.expressions().get(expr).unwrap().kind {
                ExprKind::Unary { expr, .. } | ExprKind::IsNull { expr, .. } => {
                    (ControlShape::Eager, std::slice::from_ref(expr))
                }
                ExprKind::FunctionCall { args, .. } => (ControlShape::If, args.as_ref()),
                ExprKind::Value(_) | ExprKind::Literal(_) => (ControlShape::Eager, &[][..]),
                other => panic!("fixture has no exact control projection for {other:?}"),
            };
            if expr == fixture.wrapper && matches!(claim, Flow::WrongControl) {
                shape = ControlShape::Conjunction;
            }
            let mut arguments = vec![];
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
                arguments.push(self.visit(fixture, *child, child_domain, child_demand, claim));
            }
            self.uses.push(ExpressionInvocation {
                context: ExpressionEffectContext {
                    use_id: id,
                    domain,
                    demand,
                },
                definition: expr,
                control: shape,
                arguments: arguments.into_boxed_slice(),
            });
            id
        }
    }
    let domain = EvaluationDomainId::new(u32::MAX);
    let mut author = Author {
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
    let mut bindings = vec![];
    for (&site, root) in roots.sites() {
        bindings.push((
            site,
            author.visit(fixture, root.expr, domain, root.demand, claim),
        ));
    }
    let flow = ExpressionControlFlow::try_new(
        author.domains,
        author.uses,
        fixture.fragment.expressions(),
        CompilePhase::Validate,
        &Control,
    )
    .unwrap();
    PhysicalRootUses::try_new(&fixture.fragment, flow, bindings, &Control)
}
fn package(functions: &PureEngineFunctionCatalog, fixture: Fixture) -> Arc<FragmentPackage> {
    let Fixture {
        fragment,
        wrapper,
        input,
        call,
    } = fixture;
    let (fragment, constants) = original_request_sources(fragment, &call);
    let fixture = Fixture {
        fragment,
        wrapper,
        input,
        call,
    };
    let expression_uses = uses(&fixture, Flow::Accurate).unwrap();
    let mut frozen = vec![];
    let parameters = SemanticParameters::try_new([]).unwrap();
    if let Some((id, author)) = &fixture.call {
        let flow = expression_uses.flow();
        let invocation = flow
            .uses()
            .values()
            .find(|invocation| invocation.definition == *id)
            .unwrap();
        let context = invocation.context;
        let mut children = ScopedExpressionEffects::pure_value(context);
        for (ordinal, child) in invocation.arguments.iter().enumerate() {
            children = children
                .join_control_argument(
                    ScopedExpressionEffects::pure_value(flow.uses()[child].context),
                    flow,
                    ordinal,
                )
                .unwrap();
        }
        let argument_uses = invocation
            .arguments
            .iter()
            .copied()
            .map(Some)
            .collect::<Vec<_>>();
        let token = functions
            .prepare_fresh(
                CallEffectInput {
                    context,
                    argument_uses: novarocks_functions::CallArgumentUses::SelectedChannels(
                        &argument_uses,
                    ),
                    function_id: &author.function.function_id,
                    kind: author.function.kind,
                    selected: author.selected.as_ref(),
                    request: author.request(),
                    environment: &[],
                    parameters: &parameters,
                    decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
                    proof_scope: CallProofScope::Domain(context.domain),
                },
                author.selected.clone(),
                PureCallPreparation::ControlIntrinsic {
                    arguments: children,
                },
                &Control,
            )
            .unwrap();
        frozen.push(FrozenPhysicalCall {
            materialized_writer_input: None,
            regexp_count_pattern_source: None,
            to_base64_byte_source: None,
            temporal_source: None,
            site: PhysicalCallSite::Expression(context.use_id),
            context,
            effects: token.call_contract().effects().clone(),
            decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
        });
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
                name: format!("unary_{ordinal}").into(),
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
                version: PlanVersionId::try_new([93; 16]).unwrap(),
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
fn root(mode: Mode) -> ProgramExpressionRootSite {
    ProgramExpressionRootSite::Node {
        node: ProgramNodeId::new(2),
        role: match mode {
            Mode::Project | Mode::IfArgument => {
                ProgramNodeExpressionRole::ProjectOutput { expression: 0 }
            }
            Mode::Filter => ProgramNodeExpressionRole::FilterPredicate { predicate: 0 },
        },
    }
}

fn unary_child(
    operation: Operation,
    kind: &StaticExprKind,
) -> novarocks_local_program::ProgramExprId {
    match (operation, kind) {
        (Operation::Not, StaticExprKind::Not(child))
        | (Operation::IsNull, StaticExprKind::IsNull(child))
        | (Operation::IsNotNull, StaticExprKind::IsNotNull(child)) => *child,
        _ => panic!("the exact unary opcode must survive lowering"),
    }
}

#[test]
fn unary_project_and_filter_keep_exact_slots_and_value_child_demand_without_call_tokens() {
    let functions = functions();
    for operation in [Operation::Not, Operation::IsNull, Operation::IsNotNull] {
        for mode in [Mode::Project, Mode::Filter] {
            let input_type = FunctionValueType::new(DataType::Boolean, true);
            let fixture = fixture(
                &functions,
                operation,
                mode,
                input_type.clone(),
                Claim::Accurate,
            )
            .unwrap();
            let physical_input = fixture.input;
            let physical_wrapper = fixture.wrapper;
            let source = package(&functions, fixture);
            let program = compile(source.clone(), &functions, &Control).unwrap();
            let checked = program.checked();
            let channels = checked.channels();
            let typed = channels.expressions();
            let calls = typed.resolved_calls();
            let snapshot = calls.snapshot();
            let flow = &snapshot.flows()[&ProgramExpressionArena::Main];
            let arena = &snapshot.roots().arenas()[&ProgramExpressionArena::Main];
            let invocation = &flow.uses()[&snapshot.bindings()[&root(mode)]];
            assert_eq!(invocation.control, ControlShape::Eager);
            assert_eq!(invocation.arguments.len(), 1);
            assert_eq!(invocation.context.domain, EvaluationDomainId::new(u32::MAX));
            assert_eq!(
                invocation.context.demand,
                match mode {
                    Mode::Filter => EvaluationDemand::TruthOnly,
                    _ => EvaluationDemand::Value,
                }
            );
            assert_eq!(
                source.expression_uses().flow().uses()[&invocation.context.use_id].definition,
                physical_wrapper
            );
            let child_id =
                unary_child(operation, arena.node(invocation.definition).unwrap().kind());
            let child = &flow.uses()[&invocation.arguments[0]];
            assert_eq!(child.definition, child_id);
            // A NULL predicate and NOT need the operand value even under a filter demand.
            assert_eq!(child.context.demand, EvaluationDemand::Value);
            assert_eq!(child.context.domain, invocation.context.domain);
            assert_eq!(
                source.expression_uses().flow().uses()[&child.context.use_id].definition,
                physical_input
            );
            let StaticExprKind::SlotId(slot) = arena.node(child_id).unwrap().kind() else {
                panic!("the operand must retain its exact input channel")
            };
            let site = ProgramChannelSite::Layout {
                node: ProgramNodeId::new(1),
                role: ProgramChannelLayoutRole::NodeOutput,
                ordinal: 0,
            };
            assert_eq!(channels.channel_slot(site), Some(*slot));
            assert_eq!(channels.channel_type(site), Some(&input_type));
            assert_eq!(
                checked.slots().get(&ProgramUseRef {
                    arena: ProgramExpressionArena::Main,
                    use_id: child.context.use_id,
                }),
                Some(&ProgramLexicalSource::Input(site))
            );
            assert_eq!(
                typed.definition_type(ProgramExpressionArena::Main, child_id),
                Some(&FunctionArgumentType::Value(input_type.clone()))
            );
            assert_eq!(
                typed.definition_type(ProgramExpressionArena::Main, invocation.definition),
                Some(&FunctionArgumentType::Value(operation.result(&input_type)))
            );
            assert!(calls.calls().is_empty());
            assert!(source.calls().entries().is_empty());
            assert_eq!(arena.nodes().len(), 3);
            assert_eq!(program.graph().nodes().len(), 3);
            assert_eq!(
                program.graph().nodes()[2]
                    .output_layout()
                    .schema()
                    .field(0)
                    .name(),
                "unary_0"
            );
        }
    }
}

#[test]
fn actual_if_arguments_keep_distinct_unary_occurrences_and_guarded_value_children() {
    let functions = functions();
    for operation in [Operation::Not, Operation::IsNull, Operation::IsNotNull] {
        let fixture = fixture(
            &functions,
            operation,
            Mode::IfArgument,
            FunctionValueType::new(DataType::Boolean, true),
            Claim::Accurate,
        )
        .unwrap();
        let wrapper = fixture.wrapper;
        let source = package(&functions, fixture);
        let program = compile(source.clone(), &functions, &Control).unwrap();
        let typed = program.checked().channels().expressions();
        let calls = typed.resolved_calls();
        let snapshot = calls.snapshot();
        let flow = &snapshot.flows()[&ProgramExpressionArena::Main];
        let arena = &snapshot.roots().arenas()[&ProgramExpressionArena::Main];
        let invocation = &flow.uses()[&snapshot.bindings()[&root(Mode::IfArgument)]];
        assert_eq!(invocation.control, ControlShape::If);
        assert_eq!(invocation.arguments.len(), 3);
        let StaticExprKind::BoundCall { args } = arena.node(invocation.definition).unwrap().kind()
        else {
            panic!("the actual IF owner must be a resolved call")
        };
        assert_eq!(args.len(), 3);
        assert_eq!(args[0], args[1]);
        assert_eq!(args[1], args[2]);
        for (ordinal, use_id) in invocation.arguments.iter().enumerate() {
            let unary = &flow.uses()[use_id];
            assert_eq!(unary.definition, args[ordinal]);
            assert_eq!(
                source.expression_uses().flow().uses()[use_id].definition,
                wrapper
            );
            assert_eq!(unary.control, ControlShape::Eager);
            assert_eq!(
                unary.context.demand,
                if ordinal == 0 {
                    EvaluationDemand::TruthOnly
                } else {
                    EvaluationDemand::Value
                }
            );
            if ordinal == 0 {
                assert_eq!(unary.context.domain, invocation.context.domain);
            } else {
                assert_ne!(unary.context.domain, invocation.context.domain);
                assert_ne!(*use_id, invocation.arguments[ordinal - 1]);
            }
            let operand = &flow.uses()[&unary.arguments[0]];
            assert_eq!(operand.context.domain, unary.context.domain);
            assert_eq!(operand.context.demand, EvaluationDemand::Value);
            assert_eq!(
                operand.definition,
                unary_child(operation, arena.node(unary.definition).unwrap().kind())
            );
            assert!(matches!(
                arena.node(operand.definition).unwrap().kind(),
                StaticExprKind::SlotId(_)
            ));
        }
        assert_ne!(
            flow.uses()[&invocation.arguments[1]].context.domain,
            flow.uses()[&invocation.arguments[2]].context.domain
        );
        assert_eq!(source.calls().entries().len(), 1);
        assert_eq!(calls.calls().len(), 1);
        let resolved = calls.calls().values().next().unwrap();
        assert_eq!(
            resolved.implementation().abi,
            PureKernelAbi::ControlIntrinsicV1
        );
        assert_eq!(resolved.call_contract().context(), invocation.context);
        assert_eq!(
            resolved.call_contract().effects().argument_control,
            ArgumentControl::If
        );
    }
}

#[test]
fn null_predicates_preserve_authored_uuid_and_variant_operand_domains_in_exact_channels() {
    let functions = functions();
    for input_type in [
        FunctionValueType::try_from_legacy_declared_projection(
            DataType::FixedSizeBinary(16),
            true,
            ValueLogicalType::Uuid,
        )
        .expect("valid temporary legacy value projection"),
        FunctionValueType::try_from_legacy_declared_projection(
            DataType::LargeBinary,
            true,
            ValueLogicalType::Variant,
        )
        .expect("valid temporary legacy value projection"),
    ] {
        for operation in [Operation::IsNull, Operation::IsNotNull] {
            let source = package(
                &functions,
                fixture(
                    &functions,
                    operation,
                    Mode::Project,
                    input_type.clone(),
                    Claim::Accurate,
                )
                .unwrap(),
            );
            let program = compile(source, &functions, &Control).unwrap();
            let channels = program.checked().channels();
            let typed = channels.expressions();
            let snapshot = typed.resolved_calls().snapshot();
            let flow = &snapshot.flows()[&ProgramExpressionArena::Main];
            let invocation = &flow.uses()[&snapshot.bindings()[&root(Mode::Project)]];
            let child = &flow.uses()[&invocation.arguments[0]];
            assert_eq!(
                typed.definition_type(ProgramExpressionArena::Main, child.definition),
                Some(&FunctionArgumentType::Value(input_type.clone()))
            );
            assert_eq!(
                channels.channel_type(ProgramChannelSite::Layout {
                    node: ProgramNodeId::new(1),
                    role: ProgramChannelLayoutRole::NodeOutput,
                    ordinal: 0,
                }),
                Some(&input_type)
            );
            assert_eq!(
                typed.definition_type(ProgramExpressionArena::Main, invocation.definition),
                Some(&FunctionArgumentType::Value(FunctionValueType::new(
                    DataType::Boolean,
                    false
                )))
            );
        }
    }
}

#[test]
fn physical_unary_type_and_control_contradictions_cannot_publish_a_local_program() {
    let functions = functions();
    // Numeric physical NOT is supported; raw text and nominal JSON are not.
    for input_type in [
        FunctionValueType::new(DataType::Utf8, true),
        FunctionValueType::try_with_logical_type(DataType::Utf8, true, ValueLogicalType::Json)
            .unwrap(),
    ] {
        assert!(match fixture(
            &functions,
            Operation::Not,
            Mode::Project,
            input_type,
            Claim::Accurate
        ) {
            Err(error) => error.is_producer_defect(),
            Ok(_) => false,
        });
    }
    for operation in [Operation::Not, Operation::IsNull, Operation::IsNotNull] {
        assert!(match fixture(
            &functions,
            operation,
            Mode::Project,
            FunctionValueType::new(DataType::Boolean, true),
            Claim::WrongResult
        ) {
            Err(error) => error.is_producer_defect(),
            Ok(_) => false,
        });
        let fixture = fixture(
            &functions,
            operation,
            Mode::Filter,
            FunctionValueType::new(DataType::Boolean, true),
            Claim::Accurate,
        )
        .unwrap();
        assert!(matches!(
            uses(&fixture, Flow::WrongControl),
            Err(RootUseBindingError::WrongControl)
        ));
    }
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
fn unary_if_compilation_preserves_every_original_control_refusal_without_later_callbacks() {
    let functions = functions();
    let source = package(
        &functions,
        fixture(
            &functions,
            Operation::IsNull,
            Mode::IfArgument,
            FunctionValueType::new(DataType::Boolean, true),
            Claim::Accurate,
        )
        .unwrap(),
    );
    let recorder = RefusingControl::new(CompileControlError::Cancelled, usize::MAX);
    let _ = compile(source.clone(), &functions, &recorder).unwrap();
    let trace = recorder.trace.lock().unwrap().clone();
    assert!(!trace.is_empty());
    assert!(trace.iter().all(|(_, units)| *units <= 256));
    for index in 1..=trace.len() {
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let control = RefusingControl::new(cause.clone(), index);
            assert!(matches!(compile(source.clone(), &functions, &control),
                Err(FragmentCompileError::Control(actual)) if actual == cause));
            assert_eq!(*control.trace.lock().unwrap(), trace[..index]);
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
