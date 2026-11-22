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

use novarocks_expression::StaticExprKind;
use novarocks_expression_contract::{
    BinaryOperator, BoundFunction, DefinitionControl, ExprId, ExprKind, ExpressionCallClaim,
    PhysicalCallRequest, RootScope, UnitError,
};

use super::*;
use crate::test_units::PureUnitFixture;
use arrow_schema::DataType;
use novarocks_expression::ComparisonSite as ProgramComparisonSite;
use novarocks_functions::{
    CallEffectInput, ConstantPolicy, EngineFunctionCatalogBuilder, FunctionBindingRequest,
    FunctionBindingSelection, FunctionId, FunctionKind, FunctionOverloadId, FunctionResultType,
    InstalledPureKernel, PureCallPreparation, PureEngineFunctionCatalog,
    PureImplementationDeclaration, PureImplementationId, PureKernelAbi, ScopedExpressionEffects,
};
use novarocks_local_program::KernelAbiVersion;
use novarocks_local_program::ProgramNodeKind;
use novarocks_physical_plan::{
    Fragment, FragmentBuilder, FragmentCuts, FragmentId, FragmentPackageInput, FragmentSink,
    FrozenFragmentPruning, PipelineDopDomain, PlanVersionId, RequiredContracts, ResultField,
    ResultPort, ValidationErrors, ValueOrigin,
};
use novarocks_type_contract::{
    CallProofScope, ControlShape, DecimalOverflowPolicy, EvaluationDemand, ExpressionUseId,
    FunctionValueType, FunctionVolatility, SemanticParameters, ValueLogicalType,
};
use std::{num::NonZeroUsize, sync::Mutex};

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

#[derive(Clone, Copy)]
enum Shape {
    Binary(BinaryOperator),
    SimpleCase,
    CarrierCast(Option<novarocks_type_contract::ArrayCastFailureMode>),
}
#[derive(Clone, Copy)]
enum Mode {
    Project,
    Filter,
}
#[derive(Clone, Copy)]
enum OutputClaim {
    Accurate,
    NonBoolean,
    Narrow,
}
#[derive(Debug)]
enum FixtureError {
    Unit(UnitError),
    Physical(ValidationErrors),
}
impl FixtureError {
    fn is_producer_defect(&self) -> bool {
        match self {
            Self::Unit(UnitError::Invalid { .. }) => true,
            Self::Physical(e) => e.is_producer_defect(),
            _ => false,
        }
    }
}
impl From<UnitError> for FixtureError {
    fn from(e: UnitError) -> Self {
        Self::Unit(e)
    }
}
impl From<ValidationErrors> for FixtureError {
    fn from(e: ValidationErrors) -> Self {
        Self::Physical(e)
    }
}
fn seal_units(
    builder: &mut FragmentBuilder,
    functions: &PureEngineFunctionCatalog,
    selected: Option<&Arc<FunctionBindingSelection>>,
    request: Option<FunctionBindingRequest<'_>>,
    rand_definitions: &[ExprId],
    policy: ConstantPolicy,
) -> Result<(), FixtureError> {
    if let Some(selected) = selected {
        let declaration = functions
            .metadata()
            .definition("rand", FunctionKind::Scalar)
            .unwrap();
        assert!(
            declaration
                .binding_declaration()
                .unwrap()
                .effect_declaration(&selected.overload)
                .unwrap()
                .argument_control
                .matches_scalar_shape(ControlShape::Eager)
        );
        let draft = builder.unit(NodeId::new(0)).unwrap();
        for &definition in rand_definitions {
            let original = request.unwrap();
            assert!(
                original.arguments.is_empty(),
                "this actual RAND overload is zero-argument"
            );
            draft.request(
                definition,
                PhysicalCallRequest {
                    arguments: Box::default(),
                    logical_argument_count: original.logical_argument_count,
                    expected_result_type: original.expected_result_type.cloned(),
                    constant_policy: policy,
                },
            )?;
            draft.definition_control(
                definition,
                DefinitionControl {
                    shape: ControlShape::Eager,
                    temporal_sources: None,
                },
            )?;
        }
    }
    let nodes = builder
        .construction_nodes()
        .filter_map(|(&id, _)| builder.construction_unit(id).is_some().then_some(id))
        .collect::<Vec<_>>();
    let parameters = SemanticParameters::default();
    for node in nodes {
        let ports = builder.node_ports(node).unwrap();
        let draft = builder.take_unit_builder(node).unwrap();
        let mut work = CompileCheckpoints::try_new(&Control, CompilePhase::Validate)
            .map_err(UnitError::from)?;
        let sealed = draft.seal(ports, &mut work)?;
        let mut claims = BTreeMap::new();
        for (&use_id, invocation) in sealed.uses() {
            let ExprKind::FunctionCall { function, .. } =
                &sealed.definition(invocation.definition).unwrap().kind
            else {
                continue;
            };
            let selected = selected.unwrap();
            let token = functions
                .prepare_fresh(
                    CallEffectInput {
                        context: invocation.context.into(),
                        argument_uses: novarocks_functions::CallArgumentUses::SelectedChannels(&[]),
                        function_id: &function.function_id,
                        kind: function.kind,
                        selected: selected.as_ref(),
                        request: request.unwrap(),
                        environment: &[],
                        parameters: &parameters,
                        decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
                        proof_scope: CallProofScope::Region(invocation.context.region),
                    },
                    selected.clone(),
                    PureCallPreparation::Scalar {
                        arguments: ScopedExpressionEffects::pure_value(invocation.context),
                    },
                    &Control,
                )
                .unwrap();
            claims.insert(
                use_id,
                ExpressionCallClaim {
                    context: invocation.context,
                    effects: token.call_contract().effects().clone(),
                    decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
                    temporal_source: None,
                    regexp_count_pattern_source: None,
                    to_base64_byte_source: None,
                },
            );
        }
        let unit = sealed.finish(claims, &mut work)?;
        work.finish().map_err(UnitError::from)?;
        builder.install_unit(node, Arc::new(unit)).unwrap();
    }
    Ok(())
}
fn unit(program: &novarocks_local_program::LocalProgram) -> &novarocks_expression::CompiledExprSet {
    let root = &program.graph().nodes()[program.graph().root().index()];
    let reference = match root.kind() {
        ProgramNodeKind::Project { exprs, .. } => exprs[0],
        ProgramNodeKind::Filter { predicates, .. } => predicates[0],
        _ => panic!("actual consumer root"),
    };
    program.unit(reference.unit).unwrap()
}
fn source_unit(source: &FragmentPackage) -> &novarocks_expression_contract::BoundExprSet {
    source.fragment().nodes()[&NodeId::new(0)]
        .expressions
        .as_ref()
        .unwrap()
}
fn occurrence(program: &novarocks_local_program::LocalProgram) -> ExpressionUseId {
    unit(program).roots()[0].use_id
}
struct Fixture {
    constants: novarocks_physical_plan::ConstantPools,
    fragment: Fragment,
    expression: ExprId,
    ordered: Vec<ExprId>,
    rand_selected: Option<Arc<FunctionBindingSelection>>,
    rand_request: Option<FunctionBindingRequest<'static>>,
    rand_definitions: Vec<ExprId>,
    constant_policy: ConstantPolicy,
    carrier_cast: bool,
}
fn input_constant(
    pools: &mut novarocks_physical_plan::ConstantPools,
    ty: &FunctionValueType,
    floating_bits: Option<u64>,
) -> ExprKind {
    if ty.nullable() {
        return crate::test_constants::null(pools, ty, options().constants);
    }
    match ty.data_type() {
        DataType::Boolean => crate::test_constants::boolean(pools, ty, true, options().constants),
        DataType::Int64 => crate::test_constants::i64(pools, ty, 7, options().constants),
        DataType::Float64 => crate::test_constants::f64_bits(
            pools,
            ty,
            floating_bits.unwrap_or(1f64.to_bits()),
            options().constants,
        ),
        _ => panic!("a nonnullable fixture requires an exact checked constant factory"),
    }
}
fn fixture(
    functions: &PureEngineFunctionCatalog,
    shape: Shape,
    mode: Mode,
    left: FunctionValueType,
    right: FunctionValueType,
    output: OutputClaim,
    bits: Option<[u64; 2]>,
) -> Result<Fixture, FixtureError> {
    let mut constants = novarocks_physical_plan::ConstantPools::empty();
    let mut builder = FragmentBuilder::new(FragmentId::new(101));
    builder
        .add_values(
            NodeId::new(u32::MAX),
            Box::from([Box::default()]),
            Box::default(),
        )
        .unwrap();
    let mut inputs = vec![];
    for (ordinal, ty) in [&left, &right].into_iter().enumerate() {
        let literal = builder
            .fixture_value_root(
                NodeId::new(44),
                ty.clone(),
                input_constant(&mut constants, ty, bits.map(|bits| bits[ordinal])),
            )
            .unwrap();
        let value = builder
            .add_value(
                ty.clone(),
                ValueOrigin::Expr {
                    node: NodeId::new(44),
                    root: literal,
                },
            )
            .unwrap();
        inputs.push((literal, value));
    }
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
    let a = builder
        .fixture_definition(NodeId::new(0), left.clone(), ExprKind::Value(inputs[0].1))
        .unwrap();
    let b = if matches!(shape, Shape::CarrierCast(_)) {
        a
    } else {
        builder
            .fixture_definition(NodeId::new(0), right.clone(), ExprKind::Value(inputs[1].1))
            .unwrap()
    };
    let mut rand_selected = None;
    let mut rand_request = None;
    let mut rand_definitions = vec![];
    let (kind, result, ordered) = match shape {
        Shape::Binary(op) => {
            let ty = match output {
                OutputClaim::Accurate => FunctionValueType::new(
                    DataType::Boolean,
                    op != BinaryOperator::EqForNull && (left.nullable() || right.nullable()),
                ),
                OutputClaim::NonBoolean => FunctionValueType::new(DataType::Int64, true),
                OutputClaim::Narrow => FunctionValueType::new(DataType::Boolean, false),
            };
            (
                ExprKind::Binary {
                    left: a,
                    op,
                    right: b,
                    decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
                    allow_throw_exception: None,
                },
                ty,
                vec![a, b],
            )
        }
        Shape::CarrierCast(array_failure_mode) => (
            ExprKind::Cast {
                expr: a,
                target: right.data_type().clone(),
                decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
                allow_throw_exception: novarocks_type_contract::SemanticParameterRef {
                    id: novarocks_type_contract::SemanticParameterId::new(7),
                    expected_key:
                        novarocks_type_contract::SemanticParameterKey::AllowThrowException,
                },
                array_failure_mode,
            },
            right.clone(),
            vec![a],
        ),
        Shape::SimpleCase => {
            let request = FunctionBindingRequest {
                arguments: &[],
                logical_argument_count: 0,
                expected_result_type: None,
            };
            rand_request = Some(request);
            let rand = functions
                .metadata()
                .resolve_bound_user("rand", FunctionKind::Scalar, request, &Control)
                .unwrap();
            let selected = Arc::new(rand.selected.clone());
            let FunctionResultType::Scalar(result) = &selected.result_type else {
                panic!("actual RAND scalar")
            };
            let result = result.clone();
            rand_selected = Some(selected.clone());
            let function = BoundFunction {
                function_id: rand.function_id,
                overload: selected.overload.clone(),
                kind: rand.kind,
                argument_types: selected.argument_types.clone(),
                result_type: result.clone(),
                legacy_metadata: Some(novarocks_expression_contract::LegacyBindingMetadata {
                    volatility: rand.semantics.volatility,
                    argument_evaluation: rand.semantics.argument_evaluation,
                    failure_behavior: rand.semantics.failure_behavior,
                    intrinsic_row_error: rand.semantics.intrinsic_row_error,
                    semantic_parameters: Box::default(),
                }),
            };
            let first = builder
                .fixture_definition(
                    NodeId::new(0),
                    result.clone(),
                    ExprKind::FunctionCall {
                        function: function.clone(),
                        args: Box::default(),
                    },
                )
                .unwrap();
            let second = builder
                .fixture_definition(
                    NodeId::new(0),
                    result.clone(),
                    ExprKind::FunctionCall {
                        function,
                        args: Box::default(),
                    },
                )
                .unwrap();
            rand_definitions.extend([first, second]);
            let otherwise = builder
                .fixture_definition(
                    NodeId::new(0),
                    result.clone(),
                    crate::test_constants::f64_bits(
                        &mut constants,
                        &result,
                        0f64.to_bits(),
                        options().constants,
                    ),
                )
                .unwrap();
            (
                ExprKind::Case {
                    ieee_float_equality: false,
                    operand: Some(a),
                    when_then: Box::from([(b, first), (a, second)]),
                    else_expr: Some(otherwise),
                },
                result,
                vec![a, b, first, a, second, otherwise],
            )
        }
    };
    let expression = builder
        .fixture_definition(NodeId::new(0), result.clone(), kind)
        .unwrap();
    let ordinal = builder
        .fixture_occurrence(
            NodeId::new(0),
            expression,
            if matches!(mode, Mode::Filter) {
                EvaluationDemand::TruthOnly
            } else {
                EvaluationDemand::Value
            },
            RootScope::Port(0),
        )
        .unwrap();
    match mode {
        Mode::Project => {
            let value = builder
                .add_value(
                    result,
                    ValueOrigin::Expr {
                        node: NodeId::new(0),
                        root: ordinal,
                    },
                )
                .unwrap();
            builder
                .add_project(
                    NodeId::new(0),
                    NodeId::new(44),
                    Box::from([(ordinal, value)]),
                    Box::from([value]),
                )
                .unwrap();
        }
        Mode::Filter => {
            builder
                .add_filter(NodeId::new(0), NodeId::new(44), Box::from([ordinal]))
                .unwrap();
        }
    }
    seal_units(
        &mut builder,
        functions,
        rand_selected.as_ref(),
        rand_request,
        &rand_definitions,
        options().constants,
    )?;
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
        constants,
        fragment,
        expression,
        ordered,
        rand_selected,
        rand_request,
        rand_definitions,
        constant_policy: options().constants,
        carrier_cast: matches!(shape, Shape::CarrierCast(_)),
    })
}
fn package(functions: &PureEngineFunctionCatalog, fixture: Fixture) -> Arc<FragmentPackage> {
    let parameters = SemanticParameters::try_new(if fixture.carrier_cast {
        vec![(
            novarocks_type_contract::SemanticParameterId::new(7),
            novarocks_type_contract::SemanticParameterValue::AllowThrowException(true),
        )]
    } else {
        vec![]
    })
    .unwrap();
    let constants = fixture.constants.clone();
    let _ = (
        functions,
        &fixture.rand_selected,
        &fixture.rand_request,
        &fixture.rand_definitions,
    );
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
                name: format!("equality_{ordinal}").into(),
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
                version: PlanVersionId::try_new([101; 16]).unwrap(),
                required: RequiredContracts::default(),
                constants,
                fragment: fixture.fragment,
                statement_constant_policy: fixture.constant_policy,
                cuts: FragmentCuts::default(),
                result: Some(result),
                parameters,
                scans: BTreeMap::new(),
                writes: BTreeMap::new(),
                annotations: Box::default(),
                pruning: FrozenFragmentPruning::try_new(fragment_id, vec![], &Control).unwrap(),
                layouts: Default::default(),
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
fn binary_eq_and_not_eq_have_mandatory_recipes_for_exact_ordered_child_uses() {
    let functions = functions();
    for op in [BinaryOperator::Eq, BinaryOperator::NotEq] {
        for mode in [Mode::Project, Mode::Filter] {
            for carrier in [DataType::Boolean, DataType::Int64, DataType::Float64] {
                for (left_null, right_null) in
                    [(false, false), (true, false), (false, true), (true, true)]
                {
                    let left = FunctionValueType::new(carrier.clone(), left_null);
                    let right = FunctionValueType::new(carrier.clone(), right_null);
                    let fixture = fixture(
                        &functions,
                        Shape::Binary(op),
                        mode,
                        left.clone(),
                        right.clone(),
                        OutputClaim::Accurate,
                        None,
                    )
                    .unwrap();
                    let ordered = fixture.ordered.clone();
                    let physical_root = fixture.expression;
                    let source = package(&functions, fixture);
                    let program = compile(source.clone(), &functions, &Control).unwrap();
                    let compiled = unit(&program);
                    let calls = compiled;
                    let flow = compiled.control_flow();
                    let arena = compiled.definitions();
                    let use_id = occurrence(&program);
                    let invocation = &flow.uses()[&use_id];
                    assert_eq!(invocation.control, ControlShape::Eager);
                    assert_eq!(invocation.arguments.len(), 2);
                    assert_eq!(
                        invocation.context.demand,
                        if matches!(mode, Mode::Filter) {
                            EvaluationDemand::TruthOnly
                        } else {
                            EvaluationDemand::Value
                        }
                    );
                    assert_eq!(
                        source_unit(&source).control_flow().uses()[&use_id].definition,
                        physical_root
                    );
                    let (a, b) = match (op, arena.node(invocation.definition).unwrap().kind()) {
                        (BinaryOperator::Eq, StaticExprKind::Eq(a, b))
                        | (BinaryOperator::NotEq, StaticExprKind::Ne(a, b)) => (*a, *b),
                        _ => panic!("the exact ordinary comparison opcode must survive"),
                    };
                    for (ordinal, definition) in [a, b].into_iter().enumerate() {
                        let child = &flow.uses()[&invocation.arguments[ordinal]];
                        assert_eq!(child.definition, definition);
                        assert_eq!(child.context.demand, EvaluationDemand::Value);
                        assert_eq!(child.context.region, invocation.context.region);
                        assert_eq!(
                            source_unit(&source).control_flow().uses()[&child.context.use_id]
                                .definition,
                            ordered[ordinal]
                        );
                        assert!(matches!(
                            arena.node(definition).unwrap().kind(),
                            StaticExprKind::Input
                        ));
                    }
                    assert_ne!(invocation.arguments[0], invocation.arguments[1]);
                    let occurrence = use_id;
                    let recipe = unit(&program)
                        .recipes()
                        .comparison(ProgramComparisonSite::Binary(occurrence))
                        .unwrap();
                    assert_eq!(recipe.left_type(), &left);
                    assert_eq!(recipe.right_type(), &right);
                    assert_eq!(recipe.nullable_result(), left_null || right_null);
                    assert!(
                        unit(&program)
                            .recipes()
                            .comparison(ProgramComparisonSite::CaseWhen { occurrence, arm: 0 })
                            .is_none()
                    );
                    assert!(calls.calls().is_empty());
                    assert!(source_unit(&source).claims().is_empty());
                }
            }
        }
    }
}

#[test]
fn floating_comparison_recipe_preserves_exact_source_types_and_literal_bit_patterns() {
    let functions = functions();
    // These are source material, not a runtime comparison oracle: no numeric normalization is permitted.
    let pairs = [
        [0f64.to_bits(), (-0f64).to_bits()],
        [0x7ff8_0000_0000_0042, 0x7ff8_0000_0000_0043],
    ];
    for bits in pairs {
        let ty = FunctionValueType::new(DataType::Float64, false);
        let source = package(
            &functions,
            fixture(
                &functions,
                Shape::Binary(BinaryOperator::Eq),
                Mode::Project,
                ty.clone(),
                ty.clone(),
                OutputClaim::Accurate,
                Some(bits),
            )
            .unwrap(),
        );
        let program = compile(source, &functions, &Control).unwrap();
        let constants = program
            .units()
            .iter()
            .flat_map(|unit| unit.definitions().nodes().iter())
            .filter_map(|node| {
                if let StaticExprKind::Constant(value) = node.kind() {
                    Some(value.try_f64_bits().unwrap().unwrap())
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        assert_eq!(constants, bits);
        let occurrence = occurrence(&program);
        let recipe = unit(&program)
            .recipes()
            .comparison(ProgramComparisonSite::Binary(occurrence))
            .unwrap();
        assert_eq!(recipe.left_type(), &ty);
        assert_eq!(recipe.right_type(), &ty);
        assert!(!recipe.nullable_result());
    }
}

#[test]
fn simple_case_has_one_exact_recipe_per_ordered_when_and_keeps_actual_rand_effects() {
    let functions = functions();
    let left = FunctionValueType::new(DataType::Int64, true);
    let right = FunctionValueType::new(DataType::Int64, false);
    let fixture = fixture(
        &functions,
        Shape::SimpleCase,
        Mode::Project,
        left.clone(),
        right.clone(),
        OutputClaim::Accurate,
        None,
    )
    .unwrap();
    let ordered = fixture.ordered.clone();
    let source = package(&functions, fixture);
    let program = compile(source.clone(), &functions, &Control).unwrap();
    let compiled = unit(&program);
    let calls = compiled;
    let flow = compiled.control_flow();
    let arena = compiled.definitions();
    let use_id = occurrence(&program);
    let invocation = &flow.uses()[&use_id];
    assert_eq!(
        invocation.control,
        ControlShape::Case {
            simple: true,
            arms: 2,
            has_else: true
        }
    );
    let StaticExprKind::Case {
        has_case_expr: true,
        has_else_expr: true,
        children,
        ..
    } = arena.node(invocation.definition).unwrap().kind()
    else {
        panic!("simple CASE keeps its actual ordered children")
    };
    assert_eq!(children.len(), 6);
    for (ordinal, child_use) in invocation.arguments.iter().enumerate() {
        let child = &flow.uses()[child_use];
        assert_eq!(child.definition, children[ordinal]);
        assert_eq!(
            source_unit(&source).control_flow().uses()[child_use].definition,
            ordered[ordinal]
        );
        assert_eq!(child.context.demand, EvaluationDemand::Value);
    }
    assert_eq!(children[0], children[3]);
    assert_ne!(invocation.arguments[0], invocation.arguments[3]);
    let occurrence = use_id;
    for (arm, rhs) in [(0, &right), (1, &left)] {
        let recipe = unit(&program)
            .recipes()
            .comparison(ProgramComparisonSite::CaseWhen { occurrence, arm })
            .unwrap();
        assert_eq!(recipe.left_type(), &left);
        assert_eq!(recipe.right_type(), rhs);
        assert!(recipe.nullable_result());
    }
    assert!(
        unit(&program)
            .recipes()
            .comparison(ProgramComparisonSite::CaseWhen { occurrence, arm: 2 })
            .is_none()
    );
    assert!(
        unit(&program)
            .recipes()
            .comparison(ProgramComparisonSite::Binary(occurrence))
            .is_none()
    );
    assert!(
        unit(&program)
            .recipes()
            .comparison(ProgramComparisonSite::CaseWhen {
                occurrence: invocation.arguments[1],
                arm: 0
            })
            .is_none()
    );
    assert_eq!(calls.calls().len(), 2);
    assert_eq!(source_unit(&source).claims().len(), 2);
    assert!(!source_unit(&source).claims().contains_key(&use_id));
    for call in calls.calls().values() {
        let context = call.call_contract().context().expression().unwrap();
        assert_eq!(
            call.call_contract().function_id().as_str(),
            "builtin.scalar/rand/v1"
        );
        let summary = call.effects().for_use(context).unwrap();
        assert_eq!(summary.value_stability, FunctionVolatility::Volatile);
        assert!(summary.has_instance_state);
        assert!(summary.observable_effects.rng_sampling);
        let frozen = &source_unit(&source).claims()[&context.use_id];
        assert_eq!(call.call_contract().effects(), &frozen.effects);
        assert_eq!(frozen.context, context);
    }
}

#[test]
fn original_physical_authors_reject_foreign_domains_bad_results_and_nullable_narrowing() {
    let functions = functions();
    let integer = FunctionValueType::new(DataType::Int64, true);
    for op in [BinaryOperator::Eq, BinaryOperator::NotEq] {
        for claim in [OutputClaim::NonBoolean, OutputClaim::Narrow] {
            assert!(match fixture(
                &functions,
                Shape::Binary(op),
                Mode::Project,
                integer.clone(),
                integer.clone(),
                claim,
                None
            ) {
                Err(error) => error.is_producer_defect(),
                Ok(_) => false,
            });
        }
        let uuid = FunctionValueType::try_from_legacy_declared_projection(
            DataType::FixedSizeBinary(16),
            true,
            ValueLogicalType::Uuid,
        )
        .expect("valid temporary legacy value projection");
        let opaque = FunctionValueType::new(DataType::FixedSizeBinary(16), true);
        assert!(match fixture(
            &functions,
            Shape::Binary(op),
            Mode::Project,
            uuid.clone(),
            opaque.clone(),
            OutputClaim::Accurate,
            None
        ) {
            Err(error) => error.is_producer_defect(),
            Ok(_) => false,
        });
        assert!(match fixture(
            &functions,
            Shape::SimpleCase,
            Mode::Project,
            uuid,
            opaque,
            OutputClaim::Accurate,
            None
        ) {
            Err(error) => error.is_producer_defect(),
            Ok(_) => false,
        });
    }
}

#[test]
fn mandatory_equality_preparation_preserves_every_original_control_cause_without_callback_retry() {
    let functions = functions();
    for shape in [Shape::Binary(BinaryOperator::NotEq), Shape::SimpleCase] {
        let ty = FunctionValueType::new(DataType::Int64, true);
        let source = package(
            &functions,
            fixture(
                &functions,
                shape,
                Mode::Project,
                ty.clone(),
                ty,
                OutputClaim::Accurate,
                None,
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
                assert!(
                    matches!(compile(source.clone(), &functions, &control), Err(FragmentCompileError::Control(actual)) if actual == cause)
                );
                assert_eq!(*control.trace.lock().unwrap(), trace[..stop_at]);
            }
        }
    }
}

#[test]
fn nullable_null_domain_keeps_a_checked_recipe_instead_of_a_nonnullable_boolean_claim() {
    let functions = functions();
    let ty = FunctionValueType::new(DataType::Null, true);
    for shape in [
        Shape::Binary(BinaryOperator::Eq),
        Shape::Binary(BinaryOperator::NotEq),
        Shape::SimpleCase,
    ] {
        let source = package(
            &functions,
            fixture(
                &functions,
                shape,
                Mode::Project,
                ty.clone(),
                ty.clone(),
                OutputClaim::Accurate,
                None,
            )
            .unwrap(),
        );
        let program = compile(source, &functions, &Control).unwrap();
        let _compiled = unit(&program);
        let occurrence = occurrence(&program);
        let sites = match shape {
            Shape::Binary(_) => vec![ProgramComparisonSite::Binary(occurrence)],
            Shape::CarrierCast(_) => {
                unreachable!("this matrix only contains comparison nodes")
            }
            Shape::SimpleCase => vec![
                ProgramComparisonSite::CaseWhen { occurrence, arm: 0 },
                ProgramComparisonSite::CaseWhen { occurrence, arm: 1 },
            ],
        };
        for site in sites {
            let recipe = unit(&program).recipes().comparison(site).unwrap();
            assert_eq!(recipe.left_type(), &ty);
            assert_eq!(recipe.right_type(), &ty);
            assert!(recipe.nullable_result());
        }
    }
}

#[path = "ordered_comparison_lowering_tests.rs"]
mod ordered_comparison_lowering_tests;

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

#[test]
fn actual_fragment_compilation_preserves_primitive_capability_refusals() {
    let functions = functions();
    let nested = FunctionValueType::new(
        DataType::List(Arc::new(arrow_schema::Field::new(
            "item",
            DataType::Float32,
            true,
        ))),
        true,
    );
    for operator in [
        BinaryOperator::Eq,
        BinaryOperator::Lt,
        BinaryOperator::EqForNull,
    ] {
        let source = package(
            &functions,
            fixture(
                &functions,
                Shape::Binary(operator),
                Mode::Project,
                nested.clone(),
                nested.clone(),
                OutputClaim::Accurate,
                None,
            )
            .unwrap(),
        );
        let error = compile(source, &functions, &Control).unwrap_err();
        assert!(
            error.is_unsupported(),
            "actual comparison capability refusal must reach the public category: {error}"
        );
        let FragmentCompileError::Owner {
            error,
            phase: "expressions",
        } = error
        else {
            panic!("expected original comparison owner");
        };
        assert!(matches!(
            error.downcast_ref::<crate::expressions::ExpressionLoweringError>(),
            Some(crate::expressions::ExpressionLoweringError::Unit(
                novarocks_expression::ExprCompileError::Comparison(
                    novarocks_functions::ComparisonPrepareError::Unsupported
                )
            ))
        ));
    }
    let source = package(
        &functions,
        fixture(
            &functions,
            Shape::CarrierCast(None),
            Mode::Project,
            FunctionValueType::new(DataType::Date32, true),
            FunctionValueType::new(DataType::Int8, true),
            OutputClaim::Accurate,
            None,
        )
        .unwrap(),
    );
    let error = compile(source, &functions, &Control).unwrap_err();
    assert!(
        error.is_unsupported(),
        "actual cast capability refusal must reach the public category: {error}"
    );
    let FragmentCompileError::Owner {
        error,
        phase: "expressions",
    } = error
    else {
        panic!("expected original cast owner");
    };
    assert!(matches!(
        error.downcast_ref::<crate::expressions::ExpressionLoweringError>(),
        Some(crate::expressions::ExpressionLoweringError::Unit(
            novarocks_expression::ExprCompileError::Cast(
                novarocks_functions::CastPrepareError::Unsupported
            )
        ))
    ));
}

#[test]
fn final_program_primitive_errors_preserve_only_typed_unsupported() {
    use novarocks_expression::PrimitiveError as P;
    use novarocks_functions::{
        ArithmeticPrepareError as A, CastPrepareError as C, ComparisonPrepareError as B,
    };
    for (primitive, expected) in [
        (P::Arithmetic(A::Unsupported), true),
        (P::Cast(C::Unsupported), true),
        (P::Comparison(B::Unsupported), true),
        (P::Arithmetic(A::TypeMismatch), false),
        (P::Cast(C::TypeMismatch), false),
        (P::Comparison(B::TypeMismatch), false),
        (P::Invalid("invalid recipe"), false),
        (P::Control(CompileControlError::Cancelled), false),
    ] {
        let error = FragmentCompileError::Owner {
            phase: "expressions",
            error: novarocks_failure_contract::SharedFailureCause::new(
                crate::expressions::ExpressionLoweringError::Unit(match primitive {
                    P::Arithmetic(e) => novarocks_expression::ExprCompileError::Arithmetic(e),
                    P::Cast(e) => novarocks_expression::ExprCompileError::Cast(e),
                    P::Comparison(e) => novarocks_expression::ExprCompileError::Comparison(e),
                    P::Control(e) => novarocks_expression::ExprCompileError::Control(e),
                    P::Invalid(e) => novarocks_expression::ExprCompileError::Invalid(e),
                }),
            ),
        };
        assert_eq!(error.is_unsupported(), expected);
    }
}

#[path = "public_recipe_tests.rs"]
mod public_recipe_tests;
