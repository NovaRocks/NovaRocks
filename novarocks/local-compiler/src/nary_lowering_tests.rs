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
    CallEffectInput, ConstantPolicy, EngineFunctionCatalogBuilder, FunctionArgumentType,
    FunctionBindingRequest, FunctionId, FunctionKind, FunctionOverloadId, InstalledPureKernel,
    PureCallPreparation, PureEngineFunctionCatalog, PureImplementationDeclaration,
    PureImplementationId, PureKernelAbi, ScopedExpressionEffects,
};
use novarocks_local_program::{
    KernelAbiVersion, ProgramExpressionArena, ProgramExpressionRootSite, ProgramNodeExpressionRole,
    ProgramNodeKind, StaticExprKind,
};
use novarocks_physical_plan::{
    ExprId, ExprKind, Fragment, FragmentBuilder, FragmentCuts, FragmentId, FragmentPackageInput,
    FragmentSink, FrozenCallError, FrozenFragmentCalls, FrozenFragmentPruning, FrozenPhysicalCall,
    LiteralValue, NodeId, PhysicalCallSite, PhysicalExpressionRoots, PhysicalRootUses,
    PipelineDopDomain, PlanVersionId, RequiredContracts, ResultField, ResultPort,
    RootUseBindingError, ValidationErrors, ValueDef, ValueId, ValueOrigin,
};
use novarocks_type_contract::{
    CallProofScope, CompileControlError, CompilePhase, ControlShape, DecimalOverflowPolicy,
    EvaluationDemand, EvaluationDomainId, ExpressionControlFlow, ExpressionEffectContext,
    ExpressionEvaluationDomain, ExpressionInvocation, ExpressionUseId, FunctionValueType,
    PureCompileControl, SemanticParameters, ValueLogicalType,
};
use std::{
    collections::BTreeMap,
    num::NonZeroUsize,
    sync::{Arc, Mutex},
};

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
    // The catalogue requires a nonempty seal. This actual unused RAND subset
    // grants no function-call identity to the intrinsic Boolean package.
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
enum Connective {
    And,
    Or,
}
impl Connective {
    fn shape(self) -> ControlShape {
        match self {
            Self::And => ControlShape::Conjunction,
            Self::Or => ControlShape::Disjunction,
        }
    }
    fn kind(self, args: Box<[ExprId]>) -> ExprKind {
        match self {
            Self::And => ExprKind::Conjunction { args },
            Self::Or => ExprKind::Disjunction { args },
        }
    }
}
#[derive(Clone, Copy)]
enum Use {
    Value,
    TruthOnly,
}
#[derive(Clone, Copy)]
enum Shape {
    Accurate,
    IntegerChild,
    NarrowResult,
}
struct Fixture {
    fragment: Fragment,
    connective: ExprId,
}
fn fixture(
    op: Connective,
    mode: Use,
    width: usize,
    shape: Shape,
) -> Result<Fixture, ValidationErrors> {
    let fragment_id = FragmentId::new(91);
    let source = NodeId::new(u32::MAX);
    let input = NodeId::new(41);
    let root = NodeId::new(0);
    let boolean = FunctionValueType::new(DataType::Boolean, true);
    // Leave room for the builder's generated Project result identities.
    let values = [
        ValueId::new(3),
        ValueId::new(100),
        ValueId::new(u32::MAX - 32),
    ];
    let mut builder = FragmentBuilder::new(fragment_id);
    builder
        .add_values(source, Box::from([Box::default()]), Box::default())
        .unwrap();
    let mut items = vec![];
    let mut operands = vec![];
    for (ordinal, &value) in values.iter().enumerate() {
        let ty = if ordinal == 1 && matches!(shape, Shape::IntegerChild) {
            FunctionValueType::new(DataType::Int64, true)
        } else {
            boolean.clone()
        };
        let definition = builder
            .add_expression(input, ty.clone(), ExprKind::Literal(LiteralValue::Null))
            .unwrap();
        builder
            .insert_value(ValueDef {
                id: value,
                ty: ty.clone(),
                origin: ValueOrigin::Expr {
                    node: input,
                    expr: definition,
                },
            })
            .unwrap();
        items.push((definition, value));
        operands.push(
            builder
                .add_expression(root, ty, ExprKind::Value(value))
                .unwrap(),
        );
    }
    builder
        .add_project(input, source, items.into_boxed_slice(), Box::from(values))
        .unwrap();
    // Neither ValueId nor definition order establishes the argument order.
    let order = [operands[2], operands[0], operands[1]];
    let args = (0..width)
        .map(|ordinal| order[ordinal % 3])
        .collect::<Vec<_>>()
        .into_boxed_slice();
    let result_type =
        FunctionValueType::new(DataType::Boolean, !matches!(shape, Shape::NarrowResult));
    let connective = builder
        .add_expression(root, result_type.clone(), op.kind(args))
        .unwrap();
    match mode {
        Use::Value => {
            let result = builder
                .add_value(
                    result_type,
                    ValueOrigin::Expr {
                        node: root,
                        expr: connective,
                    },
                )
                .unwrap();
            builder
                .add_project(
                    root,
                    input,
                    Box::from([(connective, result)]),
                    Box::from([result]),
                )
                .unwrap();
        }
        Use::TruthOnly => {
            builder
                .add_filter(root, input, Box::from([connective]))
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
        connective,
    })
}
#[derive(Clone, Copy)]
enum Flow {
    Accurate,
    Eager,
    Swapped,
}
fn uses(fixture: &Fixture, style: Flow) -> Result<PhysicalRootUses, RootUseBindingError> {
    fn visit(
        fragment: &Fragment,
        id: ExprId,
        demand: EvaluationDemand,
        next: &mut u32,
        output: &mut Vec<ExpressionInvocation<ExprId>>,
        style: Flow,
        connective: ExprId,
    ) -> ExpressionUseId {
        let use_id = ExpressionUseId::new(*next);
        *next += 17;
        let (control, args) = match &fragment.expressions().get(id).unwrap().kind {
            ExprKind::Conjunction { args } => (ControlShape::Conjunction, args.as_ref()),
            ExprKind::Disjunction { args } => (ControlShape::Disjunction, args.as_ref()),
            ExprKind::Value(_) | ExprKind::Literal(_) => (ControlShape::Eager, &[][..]),
            other => panic!("fixture lacks control projection for {other:?}"),
        };
        let mut arguments = args
            .iter()
            .map(|child| visit(fragment, *child, demand, next, output, style, connective))
            .collect::<Vec<_>>();
        if id == connective && matches!(style, Flow::Swapped) {
            arguments.swap(0, 1);
        }
        output.push(ExpressionInvocation {
            context: ExpressionEffectContext {
                use_id,
                domain: EvaluationDomainId::new(u32::MAX),
                demand,
            },
            definition: id,
            control: if id == connective && matches!(style, Flow::Eager) {
                ControlShape::Eager
            } else {
                control
            },
            arguments: arguments.into_boxed_slice(),
        });
        use_id
    }
    let roots = PhysicalExpressionRoots::try_new(&fixture.fragment, &Control).unwrap();
    let mut output = vec![];
    let mut bindings = vec![];
    let mut next = 7;
    for (&site, root) in roots.sites() {
        let id = visit(
            &fixture.fragment,
            root.expr,
            root.demand,
            &mut next,
            &mut output,
            style,
            fixture.connective,
        );
        bindings.push((site, id));
    }
    let flow = ExpressionControlFlow::try_new(
        vec![ExpressionEvaluationDomain {
            id: EvaluationDomainId::new(u32::MAX),
            parent: None,
            guard: None,
        }],
        output,
        fixture.fragment.expressions(),
        CompilePhase::Validate,
        &Control,
    )
    .unwrap();
    PhysicalRootUses::try_new(&fixture.fragment, flow, bindings, &Control)
}
fn package(fixture: Fixture) -> Arc<FragmentPackage> {
    let expression_uses = uses(&fixture, Flow::Accurate).unwrap();
    let calls = FrozenFragmentCalls::try_new(&fixture.fragment, &expression_uses, vec![], &Control)
        .unwrap();
    assert!(calls.entries().is_empty());
    let root = fixture
        .fragment
        .nodes()
        .get(&fixture.fragment.root())
        .unwrap();
    let result = ResultPort {
        scalar_schema: None,
        fragment: fixture.fragment.id(),
        output: root.output.clone(),
        fields: root
            .output
            .columns
            .iter()
            .enumerate()
            .map(|(ordinal, &value)| ResultField {
                domain: novarocks_physical_plan::ResultValueDomain::Plain,
                name: format!("result_{ordinal}").into_boxed_str(),
                alias: None,
                value,
                ty: fixture.fragment.values()[&value].ty.clone(),
            })
            .collect::<Vec<_>>()
            .into_boxed_slice(),
    };
    let fragment_id = fixture.fragment.id();
    Arc::new(
        FragmentPackage::try_new(
            FragmentPackageInput {
                version: PlanVersionId::try_new([91; 16]).unwrap(),
                required: RequiredContracts::default(),
                constants: novarocks_physical_plan::ConstantPools::empty(),
                fragment: fixture.fragment,
                expression_uses,
                calls,
                cuts: FragmentCuts::default(),
                result: Some(result),
                parameters: SemanticParameters::try_new([]).unwrap(),
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
    package: Arc<FragmentPackage>,
    functions: &PureEngineFunctionCatalog,
    control: &dyn PureCompileControl,
) -> Result<novarocks_local_program::LocalProgram, FragmentCompileError> {
    let providers =
        PureProviderProgramCatalog::<std::io::Error>::try_new(&[], vec![], &Control).unwrap();
    compile_fragment(
        validate_fragment_providers(package, &providers, &Control).unwrap(),
        functions,
        options(),
        control,
    )
}

#[test]
fn ordered_nullable_nary_three_and_wide_320_lower_into_one_intrinsic_definition_without_call_tokens()
 {
    let functions = functions();
    for op in [Connective::And, Connective::Or] {
        for mode in [Use::Value, Use::TruthOnly] {
            for width in [3, 320] {
                let source = package(fixture(op, mode, width, Shape::Accurate).unwrap());
                let program = compile(source.clone(), &functions, &Control).unwrap();
                let typed = program.checked().channels().expressions();
                let calls = typed.resolved_calls();
                assert!(calls.calls().is_empty());
                assert!(source.calls().entries().is_empty());
                let arena = &calls.snapshot().roots().arenas()[&ProgramExpressionArena::Main];
                assert_eq!(arena.nodes().len(), 7); // Three source literals, three references, one n-ary root.
                assert!(!arena.nodes().iter().any(|node| matches!(
                    node.kind(),
                    StaticExprKind::And(..)
                        | StaticExprKind::Or(..)
                        | StaticExprKind::BoundCall { .. }
                )));
                let flow = &calls.snapshot().flows()[&ProgramExpressionArena::Main];
                let roots = calls.snapshot().bindings();
                let (site, root_use) = roots
                    .iter()
                    .find(|(site, _)| {
                        matches!(site, ProgramExpressionRootSite::Node { node, .. } if node.index() == 2)
                    })
                    .unwrap();
                let invocation = &flow.uses()[root_use];
                assert_eq!(invocation.control, op.shape());
                assert_eq!(invocation.context.domain, EvaluationDomainId::new(u32::MAX));
                let expected_demand = match mode {
                    Use::Value => EvaluationDemand::Value,
                    Use::TruthOnly => EvaluationDemand::TruthOnly,
                };
                assert_eq!(invocation.context.demand, expected_demand);
                assert_eq!(invocation.arguments.len(), width);
                match (mode, site) {
                    (
                        Use::Value,
                        ProgramExpressionRootSite::Node {
                            role: ProgramNodeExpressionRole::ProjectOutput { expression: 0 },
                            ..
                        },
                    ) => {}
                    (
                        Use::TruthOnly,
                        ProgramExpressionRootSite::Node {
                            role: ProgramNodeExpressionRole::FilterPredicate { predicate: 0 },
                            ..
                        },
                    ) => {}
                    _ => panic!("exact use site"),
                }
                let args = match arena.node(invocation.definition).unwrap().kind() {
                    StaticExprKind::NaryAnd { args } if matches!(op, Connective::And) => args,
                    StaticExprKind::NaryOr { args } if matches!(op, Connective::Or) => args,
                    _ => panic!("direct n-ary definition"),
                };
                assert_eq!(args.len(), width);
                assert_ne!(args[0], args[1]);
                assert_ne!(args[1], args[2]);
                if width > 3 {
                    assert_eq!(args[0], args[3]);
                }
                let physical_root = source.expression_uses().flow().uses()[root_use].definition;
                let physical_args = match &source
                    .fragment()
                    .expressions()
                    .get(physical_root)
                    .unwrap()
                    .kind
                {
                    ExprKind::Conjunction { args } | ExprKind::Disjunction { args } => args,
                    _ => panic!("actual physical n-ary root"),
                };
                for (ordinal, (&definition, child_use)) in
                    args.iter().zip(&invocation.arguments).enumerate()
                {
                    let child = &flow.uses()[child_use];
                    assert_eq!(child.definition, definition);
                    assert_eq!(child.context.demand, expected_demand);
                    assert_eq!(child.context.domain, invocation.context.domain);
                    assert_eq!(
                        source.expression_uses().flow().uses()[child_use].definition,
                        physical_args[ordinal]
                    );
                    if ordinal > 0 {
                        assert_ne!(*child_use, invocation.arguments[ordinal - 1]);
                    }
                    let FunctionArgumentType::Value(ty) = typed
                        .definition_type(ProgramExpressionArena::Main, definition)
                        .unwrap()
                    else {
                        panic!("Boolean child")
                    };
                    assert_eq!(ty, &FunctionValueType::new(DataType::Boolean, true));
                }
                let FunctionArgumentType::Value(ty) = typed
                    .definition_type(ProgramExpressionArena::Main, invocation.definition)
                    .unwrap()
                else {
                    panic!("Boolean result")
                };
                assert_eq!(ty.data_type(), &DataType::Boolean);
                assert!(ty.nullable());
                assert_eq!(ty.logical_type(), ValueLogicalType::Physical);
                assert_eq!(program.graph().nodes().len(), 3);
                assert_eq!(program.provenance().operators().len(), 3);
                assert!(matches!(
                    program.graph().nodes()[0].kind(),
                    ProgramNodeKind::Values { .. }
                ));
                assert_eq!(
                    program.graph().nodes()[2]
                        .output_layout()
                        .schema()
                        .field(0)
                        .name(),
                    "result_0"
                );
            }
        }
    }
}
#[test]
fn physical_nary_source_rejects_integer_children_and_nullable_result_narrowing() {
    for op in [Connective::And, Connective::Or] {
        for shape in [Shape::IntegerChild, Shape::NarrowResult] {
            let error = match fixture(op, Use::Value, 3, shape) {
                Err(error) => error,
                Ok(_) => panic!("invalid source must not publish"),
            };
            assert!(error.is_producer_defect());
            match shape {
                Shape::IntegerChild => assert!(error.to_string().contains("is not boolean")),
                Shape::NarrowResult => {
                    assert!(error.to_string().contains("result type is inconsistent"))
                }
                Shape::Accurate => unreachable!(),
            }
        }
    }
}
#[test]
fn physical_nary_flow_rejects_eager_relabel_and_changed_order_before_local_publication() {
    for op in [Connective::And, Connective::Or] {
        let source = fixture(op, Use::Value, 3, Shape::Accurate).unwrap();
        assert!(matches!(
            uses(&source, Flow::Eager),
            Err(RootUseBindingError::WrongControl)
        ));
        assert!(matches!(
            uses(&source, Flow::Swapped),
            Err(RootUseBindingError::WrongArguments)
        ));
    }
}
#[test]
fn a_frozen_function_call_cannot_be_attached_to_a_physical_nary_intrinsic() {
    let functions = functions();
    let fixture = fixture(Connective::And, Use::Value, 3, Shape::Accurate).unwrap();
    let uses = uses(&fixture, Flow::Accurate).unwrap();
    let invocation = uses
        .flow()
        .uses()
        .values()
        .find(|invocation| invocation.definition == fixture.connective)
        .unwrap();
    let request = FunctionBindingRequest {
        arguments: &[],
        logical_argument_count: 0,
        expected_result_type: None,
    };
    let bound = functions
        .metadata()
        .resolve_bound_user("rand", FunctionKind::Scalar, request, &Control)
        .unwrap();
    let selected = Arc::new(bound.selected.clone());
    let parameters = SemanticParameters::try_new([]).unwrap();
    let token = functions
        .prepare_fresh(
            CallEffectInput {
                context: invocation.context,
                argument_uses: novarocks_functions::CallArgumentUses::SelectedChannels(&[]),
                function_id: &bound.function_id,
                kind: bound.kind,
                selected: selected.as_ref(),
                request,
                environment: &[],
                parameters: &parameters,
                decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
                proof_scope: CallProofScope::Domain(invocation.context.domain),
            },
            selected.clone(),
            PureCallPreparation::Scalar {
                arguments: ScopedExpressionEffects::pure_value(invocation.context),
            },
            &Control,
        )
        .unwrap();
    assert!(matches!(
        FrozenFragmentCalls::try_new(
            &fixture.fragment,
            &uses,
            vec![FrozenPhysicalCall {
                materialized_writer_input: None,
                regexp_count_pattern_source: None,
                to_base64_byte_source: None,
                temporal_source: None,
                site: PhysicalCallSite::Expression(invocation.context.use_id),
                context: invocation.context,
                effects: token.call_contract().effects().clone(),
                decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
            }],
            &Control
        ),
        Err(FrozenCallError::InvalidSite)
    ));
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
fn wide_nary_compile_observes_actual_entry_interior_and_tail_with_original_typed_control() {
    let functions = functions();
    let source = package(fixture(Connective::And, Use::Value, 320, Shape::Accurate).unwrap());
    let recorder = RefusingControl::new(CompileControlError::Cancelled, usize::MAX);
    let _ = compile(source.clone(), &functions, &recorder).unwrap();
    let trace = recorder.trace.lock().unwrap().clone();
    assert!(trace.iter().any(|(_, units)| *units == 256));
    assert!(trace.iter().all(|(_, units)| *units <= 256));
    for index in 1..=trace.len() {
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let control = RefusingControl::new(cause.clone(), index);
            assert!(
                matches!(compile(source.clone(), &functions, &control), Err(FragmentCompileError::Control(actual)) if actual == cause)
            );
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
