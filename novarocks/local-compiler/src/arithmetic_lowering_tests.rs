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
    ArithmeticPrepareError, ConstantPolicy, EngineFunctionCatalogBuilder, FunctionId, FunctionKind,
    FunctionOverloadId, InstalledPureKernel, PureEngineFunctionCatalog,
    PureImplementationDeclaration, PureImplementationId, PureKernelAbi,
};
use novarocks_local_program::{
    KernelAbiVersion, ProgramChannelLayoutRole, ProgramChannelSite, ProgramExpressionArena,
    ProgramExpressionRootSite, ProgramLexicalSource, ProgramNodeExpressionRole, ProgramNodeId,
    ProgramUseRef, StaticExprKind,
};
use novarocks_physical_plan::{
    BinaryOperator, ExprId, ExprKind, ExprNode, Fragment, FragmentBuilder, FragmentCuts,
    FragmentId, FragmentPackageError, FragmentPackageInput, FragmentSink, FrozenFragmentCalls,
    FrozenFragmentPruning, LiteralValue, NodeId, PhysicalExpressionRoots, PhysicalRootUses,
    PipelineDopDomain, PlanVersionId, RequiredContracts, ResultField, ResultPort, ValueDef,
    ValueId, ValueOrigin,
};
use novarocks_type_contract::{
    ArithmeticOperator, ControlShape, DecimalOverflowPolicy, EvaluationDemand, EvaluationDomainId,
    ExpressionControlFlow, ExpressionEffectContext, ExpressionEvaluationDomain,
    ExpressionInvocation, ExpressionUseId, FunctionValueType, SemanticParameterError,
    SemanticParameterId, SemanticParameterKey, SemanticParameterRef, SemanticParameterValue,
    SemanticParameters, arithmetic_result_value_type_with_op,
};
use std::num::NonZeroUsize;

struct Control;
impl PureCompileControl for Control {
    fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
        Ok(())
    }
}
fn functions() -> PureEngineFunctionCatalog {
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
    // Independent actual RAND records; no comparison function or Server seal is invented.
    builder
        .seal_pure(
            ["()->f64;strict;legacy", "(i64)->f64;strict;legacy"]
                .into_iter()
                .map(|overload| InstalledPureKernel {
                    function: FunctionId::try_new("builtin.scalar/rand/v1").unwrap(),
                    kind: FunctionKind::Scalar,
                    implementation: PureImplementationDeclaration {
                        overload: FunctionOverloadId::try_new(format!(
                            "builtin.scalar/rand/{overload}"
                        ))
                        .unwrap(),
                        implementation: PureImplementationId::try_new(
                            "builtin.scalar/rand/selected-v1",
                        )
                        .unwrap(),
                        abi: PureKernelAbi::ScalarV1,
                    },
                    aggregate_state_format: None,
                }),
        )
        .unwrap()
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

fn physical_operator(op: ArithmeticOperator) -> BinaryOperator {
    match op {
        ArithmeticOperator::Add => BinaryOperator::Add,
        ArithmeticOperator::Subtract => BinaryOperator::Subtract,
        ArithmeticOperator::Multiply => BinaryOperator::Multiply,
        ArithmeticOperator::Divide => BinaryOperator::Divide,
        ArithmeticOperator::Modulo => BinaryOperator::Modulo,
    }
}
fn reference(id: u32) -> SemanticParameterRef {
    SemanticParameterRef {
        id: SemanticParameterId::new(id),
        expected_key: SemanticParameterKey::AllowThrowException,
    }
}
struct Fixture {
    fragment: Fragment,
    root: ExprId,
    left: ExprId,
    right: ExprId,
    child: Option<ExprId>,
    allow: bool,
    policy: DecimalOverflowPolicy,
}
fn fixture(
    left_type: FunctionValueType,
    right_type: FunctionValueType,
    op: ArithmeticOperator,
    allow: bool,
    policy: DecimalOverflowPolicy,
    result_nullable: bool,
    nested_mod: bool,
) -> Fixture {
    let mut builder = FragmentBuilder::new(FragmentId::new(113));
    builder
        .add_values(
            NodeId::new(u32::MAX),
            Box::from([Box::default()]),
            Box::default(),
        )
        .unwrap();
    // Physical literals expose Int64, not narrower signed payload variants.
    // Narrow inputs therefore use accurately declared nullable NULL sources.
    // Nonmonotonic definition and node IDs cannot supply child order or local slots.
    let mut outputs = Vec::new();
    let mut projected = Vec::new();
    for (expr, value, ty, number) in [
        (
            901,
            100,
            FunctionValueType::new(DataType::Int64, false),
            999,
        ),
        (17, 103, left_type.clone(), 12),
        (402, 109, right_type.clone(), 0),
    ] {
        let expr = ExprId::new(expr);
        let value = ValueId::new(value);
        builder
            .insert_expression(ExprNode {
                id: expr,
                owner: NodeId::new(44),
                lambda_scope: None,
                ty: ty.clone(),
                kind: ExprKind::Literal(if ty.nullable() {
                    LiteralValue::Null
                } else {
                    LiteralValue::Int64(number)
                }),
            })
            .unwrap();
        builder
            .insert_value(ValueDef {
                id: value,
                ty,
                origin: ValueOrigin::Expr {
                    node: NodeId::new(44),
                    expr,
                },
            })
            .unwrap();
        projected.push((expr, value));
        outputs.push(value);
    }
    builder
        .add_project(
            NodeId::new(44),
            NodeId::new(u32::MAX),
            projected.into_boxed_slice(),
            outputs.into_boxed_slice(),
        )
        .unwrap();
    let left = ExprId::new(300);
    let right = ExprId::new(31);
    for (id, ty, value) in [
        (left, left_type.clone(), ValueId::new(103)),
        (right, right_type.clone(), ValueId::new(109)),
    ] {
        builder
            .insert_expression(ExprNode {
                id,
                owner: NodeId::new(0),
                lambda_scope: None,
                ty,
                kind: ExprKind::Value(value),
            })
            .unwrap();
    }
    let (effective_left, effective_type, child) = if nested_mod {
        let child = ExprId::new(932);
        let mut ty = arithmetic_result_value_type_with_op(
            &left_type,
            &right_type,
            ArithmeticOperator::Modulo,
        )
        .unwrap();
        ty = ty.with_nullable(true).unwrap();
        builder
            .insert_expression(ExprNode {
                id: child,
                owner: NodeId::new(0),
                lambda_scope: None,
                ty: ty.clone(),
                kind: ExprKind::Binary {
                    left,
                    right,
                    op: BinaryOperator::Modulo,
                    decimal_overflow_policy: policy,
                    allow_throw_exception: Some(reference(0)),
                },
            })
            .unwrap();
        (child, ty, Some(child))
    } else {
        (left, left_type, None)
    };
    let mut result =
        arithmetic_result_value_type_with_op(&effective_type, &right_type, op).unwrap();
    result = result.with_nullable(result_nullable).unwrap();
    let root = ExprId::new(940);
    builder
        .insert_expression(ExprNode {
            id: root,
            owner: NodeId::new(0),
            lambda_scope: None,
            ty: result.clone(),
            kind: ExprKind::Binary {
                left: effective_left,
                right,
                op: physical_operator(op),
                decimal_overflow_policy: policy,
                allow_throw_exception: Some(reference(u32::MAX)),
            },
        })
        .unwrap();
    let output = ValueId::new(117);
    builder
        .insert_value(ValueDef {
            id: output,
            ty: result,
            origin: ValueOrigin::Expr {
                node: NodeId::new(0),
                expr: root,
            },
        })
        .unwrap();
    builder
        .add_project(
            NodeId::new(0),
            NodeId::new(44),
            Box::from([(root, output)]),
            Box::from([output]),
        )
        .unwrap();
    let fragment = builder
        .finish_definition(
            NodeId::new(0),
            FragmentSink::Result,
            PipelineDopDomain {
                min: 1,
                max: 1,
                requires_power_of_two: false,
            },
            novarocks_physical_plan::StagePlacementPolicy::Singleton,
        )
        .unwrap();
    Fixture {
        fragment,
        root,
        left,
        right,
        child,
        allow,
        policy,
    }
}
fn uses(fragment: &Fragment) -> PhysicalRootUses {
    fn visit(
        fragment: &Fragment,
        definition: ExprId,
        domain: EvaluationDomainId,
        demand: EvaluationDemand,
        next: &mut u32,
        output: &mut Vec<ExpressionInvocation<ExprId>>,
    ) -> ExpressionUseId {
        let use_id = ExpressionUseId::new(*next);
        *next += 19;
        let children = match &fragment.expressions().get(definition).unwrap().kind {
            ExprKind::Binary { left, right, .. } => vec![*left, *right],
            ExprKind::Literal(_) | ExprKind::Value(_) => vec![],
            _ => panic!("fixture declares only exact eager intrinsic definitions"),
        };
        let arguments = children
            .into_iter()
            .map(|child| {
                visit(
                    fragment,
                    child,
                    domain,
                    EvaluationDemand::Value,
                    next,
                    output,
                )
            })
            .collect::<Vec<_>>();
        output.push(ExpressionInvocation {
            context: ExpressionEffectContext {
                use_id,
                domain,
                demand,
            },
            definition,
            control: ControlShape::Eager,
            arguments: arguments.into_boxed_slice(),
        });
        use_id
    }
    let domain = EvaluationDomainId::new(u32::MAX);
    let mut next = 7;
    let mut invocations = Vec::new();
    let roots = PhysicalExpressionRoots::try_new(fragment, &Control).unwrap();
    let bindings = roots
        .sites()
        .iter()
        .map(|(&site, root)| {
            (
                site,
                visit(
                    fragment,
                    root.expr,
                    domain,
                    root.demand,
                    &mut next,
                    &mut invocations,
                ),
            )
        })
        .collect();
    let flow = ExpressionControlFlow::try_new(
        vec![ExpressionEvaluationDomain {
            id: domain,
            parent: None,
            guard: None,
        }],
        invocations,
        fragment.expressions(),
        CompilePhase::Validate,
        &Control,
    )
    .unwrap();
    PhysicalRootUses::try_new(fragment, flow, bindings, &Control).unwrap()
}
fn package_input(fixture: &Fixture) -> FragmentPackageInput {
    let expression_uses = uses(&fixture.fragment);
    let calls = FrozenFragmentCalls::try_new(&fixture.fragment, &expression_uses, vec![], &Control)
        .unwrap();
    let mut parameters = vec![(
        SemanticParameterId::new(u32::MAX),
        SemanticParameterValue::AllowThrowException(fixture.allow),
    )];
    if fixture.child.is_some() {
        // The public table preserves exact scoped values. Legacy v1 has its
        // separate homogeneous-root gate; this is direct immutable compilation.
        parameters.push((
            SemanticParameterId::new(0),
            SemanticParameterValue::AllowThrowException(!fixture.allow),
        ));
    }
    let root = &fixture.fragment.nodes()[&fixture.fragment.root()];
    let result = ResultPort {
        scalar_schema: None,
        fragment: fixture.fragment.id(),
        output: root.output.clone(),
        fields: root
            .output
            .columns
            .iter()
            .map(|value| ResultField {
                domain: novarocks_physical_plan::ResultValueDomain::Plain,
                name: "arithmetic".into(),
                alias: None,
                value: *value,
                ty: fixture.fragment.values()[value].ty.clone(),
            })
            .collect::<Vec<_>>()
            .into_boxed_slice(),
    };
    FragmentPackageInput {
        version: PlanVersionId::try_new([113; 16]).unwrap(),
        required: RequiredContracts::default(),
        constants: novarocks_physical_plan::ConstantPools::empty(),
        fragment: fixture.fragment.clone(),
        expression_uses,
        calls,
        pruning: FrozenFragmentPruning::try_new(fixture.fragment.id(), vec![], &Control).unwrap(),
        cuts: FragmentCuts::default(),
        result: Some(result),
        parameters: SemanticParameters::try_new(parameters).unwrap(),
        scans: BTreeMap::new(),
        writes: BTreeMap::new(),
        annotations: Box::default(),
    }
}
fn package(fixture: &Fixture) -> Arc<FragmentPackage> {
    Arc::new(
        FragmentPackage::try_new(package_input(fixture), package_admission(), &Control).unwrap(),
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
fn root() -> ProgramExpressionRootSite {
    ProgramExpressionRootSite::Node {
        node: ProgramNodeId::new(2),
        role: ProgramNodeExpressionRole::ProjectOutput { expression: 0 },
    }
}
fn occurrence(program: &novarocks_local_program::LocalProgram) -> ProgramUseRef {
    ProgramUseRef {
        arena: ProgramExpressionArena::Main,
        use_id: program
            .checked()
            .channels()
            .expressions()
            .resolved_calls()
            .snapshot()
            .bindings()[&root()],
    }
}
#[test]
fn signed_arithmetic_compiles_exact_ordered_types_policies_scopes_and_lexical_sources() {
    let functions = functions();
    let widths = [
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
    ];
    for (left, right) in [
        (0, 0),
        (0, 1),
        (1, 0),
        (1, 1),
        (2, 2),
        (2, 3),
        (3, 2),
        (3, 3),
    ] {
        for op in [
            ArithmeticOperator::Add,
            ArithmeticOperator::Subtract,
            ArithmeticOperator::Multiply,
            ArithmeticOperator::Divide,
            ArithmeticOperator::Modulo,
        ] {
            for (allow, policy) in [
                (false, DecimalOverflowPolicy::OutputNull),
                (true, DecimalOverflowPolicy::ReportError),
            ] {
                let left_type = FunctionValueType::new(widths[left].clone(), left != 3);
                let right_type = FunctionValueType::new(widths[right].clone(), right != 3);
                let fixture = fixture(
                    left_type.clone(),
                    right_type.clone(),
                    op,
                    allow,
                    policy,
                    true,
                    false,
                );
                let source = package(&fixture);
                let program = compile(source.clone(), &functions, &Control).unwrap();
                let occurrence = occurrence(&program);
                let snapshot = program
                    .checked()
                    .channels()
                    .expressions()
                    .resolved_calls()
                    .snapshot();
                let flow = &snapshot.flows()[&ProgramExpressionArena::Main];
                let arena = &snapshot.roots().arenas()[&ProgramExpressionArena::Main];
                let invocation = &flow.uses()[&occurrence.use_id];
                assert_eq!(invocation.context.demand, EvaluationDemand::Value);
                assert_eq!(invocation.control, ControlShape::Eager);
                assert_eq!(invocation.arguments.len(), 2);
                assert_eq!(
                    source.expression_uses().flow().uses()[&occurrence.use_id].definition,
                    fixture.root
                );
                let StaticExprKind::PreparedArithmetic {
                    operator,
                    left: a,
                    right: b,
                    decimal_overflow_policy,
                    allow_throw_exception,
                } = arena.node(invocation.definition).unwrap().kind()
                else {
                    panic!("arithmetic retains an exact prepared opcode")
                };
                assert_eq!(*operator, op);
                assert_eq!(*decimal_overflow_policy, fixture.policy);
                assert_eq!(*allow_throw_exception, fixture.allow);
                let recipe = program.arithmetic_recipe(occurrence).unwrap();
                let mut expected =
                    arithmetic_result_value_type_with_op(&left_type, &right_type, op).unwrap();
                expected = expected.with_nullable(true).unwrap();
                assert_eq!(recipe.operator(), op);
                assert_eq!(recipe.left_type(), &left_type);
                assert_eq!(recipe.right_type(), &right_type);
                assert_eq!(recipe.result_type(), &expected);
                assert_eq!(recipe.decimal_overflow_policy(), policy);
                assert_eq!(recipe.allow_throw_exception(), allow);
                let own = recipe
                    .own_effects(invocation.context)
                    .for_use(invocation.context)
                    .unwrap();
                assert!(own.may_raise_row_error);
                assert!(!own.has_instance_state);
                for (ordinal, local, physical, source_ordinal, physical_value) in [
                    (0, *a, fixture.left, 1, 103),
                    (1, *b, fixture.right, 2, 109),
                ] {
                    let child = &flow.uses()[&invocation.arguments[ordinal]];
                    assert_eq!(child.definition, local);
                    assert_eq!(child.context.domain, invocation.context.domain);
                    assert_eq!(child.context.demand, EvaluationDemand::Value);
                    assert_eq!(
                        source.expression_uses().flow().uses()[&child.context.use_id].definition,
                        physical
                    );
                    let expected_source = ProgramChannelSite::Layout {
                        node: ProgramNodeId::new(1),
                        role: ProgramChannelLayoutRole::NodeOutput,
                        ordinal: source_ordinal,
                    };
                    assert_eq!(
                        program.checked().slots().get(&ProgramUseRef {
                            arena: ProgramExpressionArena::Main,
                            use_id: child.context.use_id
                        }),
                        Some(&ProgramLexicalSource::Input(expected_source))
                    );
                    let StaticExprKind::SlotId(slot) = arena.node(local).unwrap().kind() else {
                        panic!("the selected operand is a checked incoming slot")
                    };
                    assert_eq!(
                        program.checked().channels().channel_slot(expected_source),
                        Some(*slot)
                    );
                    assert_ne!(slot.as_u32(), physical_value);
                    assert!(
                        program
                            .arithmetic_recipe(ProgramUseRef {
                                arena: ProgramExpressionArena::Main,
                                use_id: child.context.use_id
                            })
                            .is_none()
                    );
                }
                assert!(snapshot.roots().sites().contains_key(&root()));
                assert!(
                    snapshot.flows()[&ProgramExpressionArena::Main].uses()[&occurrence.use_id]
                        .arguments[0]
                        != snapshot.flows()[&ProgramExpressionArena::Main].uses()
                            [&occurrence.use_id]
                            .arguments[1]
                );
                assert!(source.calls().entries().is_empty());
                assert!(
                    snapshot.roots().arenas()[&ProgramExpressionArena::Main]
                        .node(invocation.definition)
                        .is_some()
                );
            }
        }
    }
}

#[test]
fn nested_signed_arithmetic_preserves_independent_scoped_switches_and_child_row_error() {
    let functions = functions();
    let ty = FunctionValueType::new(DataType::Int8, true);
    for allow in [false, true] {
        let fixture = fixture(
            ty.clone(),
            ty.clone(),
            ArithmeticOperator::Add,
            allow,
            DecimalOverflowPolicy::ReportError,
            true,
            true,
        );
        let source = package(&fixture);
        let program = compile(source.clone(), &functions, &Control).unwrap();
        let occurrence = occurrence(&program);
        let snapshot = program
            .checked()
            .channels()
            .expressions()
            .resolved_calls()
            .snapshot();
        let flow = &snapshot.flows()[&ProgramExpressionArena::Main];
        let invocation = &flow.uses()[&occurrence.use_id];
        let child = &flow.uses()[&invocation.arguments[0]];
        assert_eq!(
            source.expression_uses().flow().uses()[&child.context.use_id].definition,
            fixture.child.unwrap()
        );
        let outer = program.arithmetic_recipe(occurrence).unwrap();
        let inner = program
            .arithmetic_recipe(ProgramUseRef {
                arena: ProgramExpressionArena::Main,
                use_id: child.context.use_id,
            })
            .unwrap();
        assert_eq!(outer.allow_throw_exception(), allow);
        assert_eq!(inner.allow_throw_exception(), !allow);
        assert_eq!(outer.left_type().data_type(), &DataType::Int8);
        assert!(outer.left_type().nullable());
        assert_eq!(outer.result_type().data_type(), &DataType::Int8);
        let parent = outer.own_effects(invocation.context);
        let argument = inner.own_effects(child.context);
        assert!(
            parent
                .for_use(invocation.context)
                .unwrap()
                .may_raise_row_error
        );
        assert!(argument.for_use(child.context).unwrap().may_raise_row_error);
        // Use the actual checked occurrence edge. Both arithmetic operations
        // retain their own errors without fabricating a call token.
        let composed = parent
            .join_control_argument(argument, flow.shared_flow(), 0)
            .unwrap();
        assert!(
            composed
                .for_use(invocation.context)
                .unwrap()
                .may_raise_row_error
        );
        assert!(
            !composed
                .for_use(invocation.context)
                .unwrap()
                .has_instance_state
        );
        assert_ne!(child.context.use_id, invocation.arguments[1]);
        assert_eq!(
            flow.uses()[&child.arguments[1]].definition,
            flow.uses()[&invocation.arguments[1]].definition
        );
        assert_ne!(child.arguments[1], invocation.arguments[1]);
    }
}

fn assert_arithmetic_failure(error: FragmentCompileError, expected: ArithmeticPrepareError) {
    assert_eq!(
        error.is_unsupported(),
        expected == ArithmeticPrepareError::Unsupported,
        "the actual compilation path must preserve the typed capability category"
    );
    let FragmentCompileError::Owner {
        phase: "expressions",
        error,
    } = error
    else {
        panic!("the original arithmetic owner must reject the exact source")
    };
    let error = error
        .downcast_ref::<crate::expressions::ExpressionLoweringError>()
        .unwrap();
    assert!(
        matches!(error, crate::expressions::ExpressionLoweringError::Arithmetic(actual) if actual == &expected)
    );
}
#[test]
fn nullable_false_division_preserves_the_existing_arithmetic_result_contract() {
    let functions = functions();
    let ty = FunctionValueType::new(DataType::Int64, false);
    let fixture = fixture(
        ty.clone(),
        ty,
        ArithmeticOperator::Divide,
        false,
        DecimalOverflowPolicy::OutputNull,
        false,
        false,
    );
    // Integer division now reports row errors, but this slice deliberately
    // retains the existing nullable frozen binary arithmetic output contract.
    let source = package(&fixture);
    assert!(
        !source
            .fragment()
            .expressions()
            .get(fixture.root)
            .unwrap()
            .ty
            .nullable()
    );
    assert_arithmetic_failure(
        compile(source, &functions, &Control).unwrap_err(),
        ArithmeticPrepareError::TypeMismatch,
    );
}

#[test]
fn floating_arithmetic_preserves_exact_prepared_types_and_policies() {
    let functions = functions();
    for carrier in [DataType::Float32, DataType::Float64] {
        let ty = FunctionValueType::new(carrier, true);
        let fixture = fixture(
            ty.clone(),
            ty.clone(),
            ArithmeticOperator::Add,
            true,
            DecimalOverflowPolicy::ReportError,
            true,
            false,
        );
        let program = compile(package(&fixture), &functions, &Control).unwrap();
        let layout = program.graph().nodes().last().unwrap().output_layout();
        assert!(layout.is_typed());
        assert!(matches!(
            layout.value_binding_at(0).unwrap().layout().data_type(),
            DataType::Float64
        ));
        let recipe = program.arithmetic_recipe(occurrence(&program)).unwrap();
        assert_eq!(recipe.operator(), ArithmeticOperator::Add);
        assert_eq!(recipe.left_type(), &ty);
        assert_eq!(recipe.right_type(), &ty);
        assert_eq!(
            recipe.result_type(),
            &FunctionValueType::new(DataType::Float64, true)
        );
        assert_eq!(
            recipe.decimal_overflow_policy(),
            DecimalOverflowPolicy::ReportError
        );
        assert!(recipe.allow_throw_exception());
    }
}

#[test]
fn unsupported_largeint_float_domains_do_not_gain_a_recipe_from_their_carrier() {
    let functions = functions();
    for carrier in [DataType::Float32, DataType::Float64] {
        let float = FunctionValueType::new(carrier, true);
        let largeint = FunctionValueType::try_with_logical_type(
            DataType::FixedSizeBinary(16),
            true,
            novarocks_type_contract::ValueLogicalType::LargeInt,
        )
        .unwrap();
        // The frozen source can author this pair, but the original Float owner
        // explicitly does not claim the LARGEINT/Float invocation-wide data port.
        for (left, right) in [(float.clone(), largeint.clone()), (largeint, float)] {
            let fixture = fixture(
                left,
                right,
                ArithmeticOperator::Add,
                true,
                DecimalOverflowPolicy::ReportError,
                true,
                false,
            );
            assert_arithmetic_failure(
                compile(package(&fixture), &functions, &Control).unwrap_err(),
                ArithmeticPrepareError::Unsupported,
            );
        }
    }
}

#[test]
fn checked_arithmetic_package_refuses_missing_or_wrong_parameter_authority() {
    let ty = FunctionValueType::new(DataType::Int16, true);
    let fixture = fixture(
        ty.clone(),
        ty,
        ArithmeticOperator::Multiply,
        true,
        DecimalOverflowPolicy::OutputNull,
        true,
        false,
    );
    let mut input = package_input(&fixture);
    input.parameters = SemanticParameters::default();
    assert_eq!(
        FragmentPackage::try_new(input, package_admission(), &Control).unwrap_err(),
        FragmentPackageError::Parameter(SemanticParameterError::MissingId(
            SemanticParameterId::new(u32::MAX)
        ))
    );
    let mut input = package_input(&fixture);
    input.parameters = SemanticParameters::try_new([(
        SemanticParameterId::new(u32::MAX),
        SemanticParameterValue::GroupConcatLegacy(true),
    )])
    .unwrap();
    assert_eq!(
        FragmentPackage::try_new(input, package_admission(), &Control).unwrap_err(),
        FragmentPackageError::Parameter(SemanticParameterError::KeyMismatch(reference(u32::MAX)))
    );
}

struct RefusingArithmeticControl {
    cause: CompileControlError,
    stop_at: usize,
    trace: std::sync::Mutex<Vec<(CompilePhase, u32)>>,
    refused: std::sync::Mutex<bool>,
}
impl RefusingArithmeticControl {
    fn new(cause: CompileControlError, stop_at: usize) -> Self {
        Self {
            cause,
            stop_at,
            trace: std::sync::Mutex::new(vec![]),
            refused: std::sync::Mutex::new(false),
        }
    }
}
impl PureCompileControl for RefusingArithmeticControl {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        let mut refused = self.refused.lock().unwrap();
        assert!(
            !*refused,
            "no callback after original arithmetic control refusal"
        );
        assert!(units <= 256);
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
fn mandatory_arithmetic_compilation_preserves_every_control_prefix_and_ordinary_error_tail() {
    let functions = functions();
    let ty = FunctionValueType::new(DataType::Int64, false);
    for nullable in [true, false] {
        let fixture = fixture(
            ty.clone(),
            ty.clone(),
            ArithmeticOperator::Divide,
            true,
            DecimalOverflowPolicy::ReportError,
            nullable,
            false,
        );
        let source = package(&fixture);
        let recorder = RefusingArithmeticControl::new(CompileControlError::Cancelled, usize::MAX);
        let result = compile(source.clone(), &functions, &recorder);
        if nullable {
            assert!(result.is_ok());
        } else {
            assert_arithmetic_failure(result.unwrap_err(), ArithmeticPrepareError::TypeMismatch);
        }
        let trace = recorder.trace.lock().unwrap().clone();
        assert!(!trace.is_empty());
        for stop_at in 1..=trace.len() {
            for cause in [
                CompileControlError::Cancelled,
                CompileControlError::DeadlineExceeded,
                CompileControlError::ResourceExhausted,
            ] {
                let control = RefusingArithmeticControl::new(cause.clone(), stop_at);
                assert!(
                    matches!(compile(source.clone(), &functions, &control), Err(FragmentCompileError::Control(actual)) if actual == cause)
                );
                assert_eq!(*control.trace.lock().unwrap(), trace[..stop_at]);
            }
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
