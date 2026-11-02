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

// These full checked compiler fixtures exercise intrinsic Boolean roots only.
// The unused real RAND subset does not attest Server catalogue coverage.
use super::CompiledExpressionInstance;
use arrow::{array::BooleanArray, datatypes::DataType, record_batch::RecordBatch};
use novarocks_connector_contract::PureProviderProgramCatalog;
use novarocks_functions::{
    CallEffectInput, ConstantPolicy, EngineFunctionCatalogBuilder, FunctionArgument,
    FunctionBindingRequest, FunctionId, FunctionKind, FunctionOverloadId, FunctionResultType,
    InstalledPureKernel, KernelDiagnostic, KernelEvaluationControl, KernelFailure,
    PureCallPreparation, PureEngineFunctionCatalog, PureImplementationDeclaration,
    PureImplementationId, PureKernelAbi, ScopedExpressionEffects, Selection,
};
use novarocks_local_compiler::{
    LocalCompileOptions, compile_fragment, validate_fragment_providers,
};
use novarocks_local_program::{
    KernelAbiVersion, LocalProgram, ProgramExpressionArena, ProgramExpressionRootSite,
    ProgramNodeExpressionRole, ProgramNodeId, StaticExprKind,
};
use novarocks_physical_plan::{
    BoundFunction, ExprId, ExprKind, Fragment, FragmentBuilder, FragmentCuts, FragmentId,
    FragmentPackage, FragmentPackageInput, FragmentSink, FrozenFragmentCalls,
    FrozenFragmentPruning, FrozenPhysicalCall, LiteralValue, NodeId, PhysicalCallSite,
    PhysicalExpressionRoots, PhysicalRootUses, PipelineDopDomain, PlanVersionId, RequiredContracts,
    ResultField, ResultPort, RootUseBindingError, ValidationErrors, ValueDef, ValueId, ValueOrigin,
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
fn functions() -> PureEngineFunctionCatalog {
    functions_with_if(false)
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
struct Fixture {
    fragment: Fragment,
}
fn source_columns(
    builder: &mut FragmentBuilder,
    source: NodeId,
    input: NodeId,
    root: NodeId,
) -> Vec<ExprId> {
    // Leave room for the builder's generated Project result identities.
    let values = [
        ValueId::new(3),
        ValueId::new(100),
        ValueId::new(u32::MAX - 32),
    ];
    builder
        .add_values(source, Box::from([Box::default()]), Box::default())
        .unwrap();
    let mut items = vec![];
    let mut operands = vec![];
    for &value in &values {
        let ty = FunctionValueType::new(DataType::Boolean, true);
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
    operands
}
fn fixture(op: Connective, mode: Use, width: usize) -> Result<Fixture, ValidationErrors> {
    let fragment_id = FragmentId::new(91);
    let source = NodeId::new(u32::MAX);
    let input = NodeId::new(41);
    let root = NodeId::new(0);
    let boolean = FunctionValueType::new(DataType::Boolean, true);
    let mut builder = FragmentBuilder::new(fragment_id);
    let operands = source_columns(&mut builder, source, input, root);
    // Neither ValueId nor definition order establishes the argument order.
    let order = [operands[2], operands[0], operands[1]];
    let args = (0..width)
        .map(|ordinal| order[ordinal % 3])
        .collect::<Vec<_>>()
        .into_boxed_slice();
    let result_type = boolean;
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
    Ok(Fixture { fragment })
}
fn uses(fixture: &Fixture) -> Result<PhysicalRootUses, RootUseBindingError> {
    struct FlowAuthor {
        next: u32,
        next_domain: u32,
        output: Vec<ExpressionInvocation<ExprId>>,
        domains: Vec<ExpressionEvaluationDomain>,
    }
    impl FlowAuthor {
        fn visit(
            &mut self,
            fragment: &Fragment,
            id: ExprId,
            domain: EvaluationDomainId,
            demand: EvaluationDemand,
        ) -> ExpressionUseId {
            let use_id = ExpressionUseId::new(self.next);
            self.next += 17;
            let (control, args) = match &fragment.expressions().get(id).unwrap().kind {
                ExprKind::Conjunction { args } => (ControlShape::Conjunction, args.as_ref()),
                ExprKind::Disjunction { args } => (ControlShape::Disjunction, args.as_ref()),
                ExprKind::FunctionCall { function, args } => {
                    assert_eq!(function.function_id.as_str(), "builtin.scalar/if/v1");
                    (ControlShape::If, args.as_ref())
                }
                ExprKind::Value(_) | ExprKind::Literal(_) => (ControlShape::Eager, &[][..]),
                other => panic!("fixture lacks control projection for {other:?}"),
            };
            let mut arguments = vec![];
            for (ordinal, child) in args.iter().enumerate() {
                let (child_demand, guard) =
                    control_argument_semantics(control, args.len(), ordinal, demand).unwrap();
                let child_domain = if let Some(kind) = guard {
                    let child_domain = EvaluationDomainId::new(self.next_domain);
                    self.next_domain += 13;
                    self.domains.push(ExpressionEvaluationDomain {
                        id: child_domain,
                        parent: Some(domain),
                        guard: Some(DomainGuard {
                            owner: use_id,
                            kind,
                        }),
                    });
                    child_domain
                } else {
                    domain
                };
                arguments.push(self.visit(fragment, *child, child_domain, child_demand));
            }
            self.output.push(ExpressionInvocation {
                context: ExpressionEffectContext {
                    use_id,
                    domain,
                    demand,
                },
                definition: id,
                control,
                arguments: arguments.into_boxed_slice(),
            });
            use_id
        }
    }
    let roots = PhysicalExpressionRoots::try_new(&fixture.fragment, &Control).unwrap();
    let domain = EvaluationDomainId::new(u32::MAX);
    let mut author = FlowAuthor {
        next: 7,
        next_domain: 8,
        output: vec![],
        domains: vec![ExpressionEvaluationDomain {
            id: domain,
            parent: None,
            guard: None,
        }],
    };
    let mut bindings = vec![];
    for (&site, root) in roots.sites() {
        bindings.push((
            site,
            author.visit(&fixture.fragment, root.expr, domain, root.demand),
        ));
    }
    let flow = ExpressionControlFlow::try_new(
        author.domains,
        author.output,
        fixture.fragment.expressions(),
        CompilePhase::Validate,
        &Control,
    )
    .unwrap();
    PhysicalRootUses::try_new(&fixture.fragment, flow, bindings, &Control)
}

fn package(fixture: Fixture) -> Arc<FragmentPackage> {
    let expression_uses = uses(&fixture).unwrap();
    let calls = FrozenFragmentCalls::try_new(&fixture.fragment, &expression_uses, vec![], &Control)
        .unwrap();
    assert!(calls.entries().is_empty());
    package_with_calls(fixture, expression_uses, calls)
}
fn package_with_calls(
    fixture: Fixture,
    expression_uses: PhysicalRootUses,
    calls: FrozenFragmentCalls,
) -> Arc<FragmentPackage> {
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
                domain: crate::test_result_domain::result_value_domain(
                    &fixture.fragment.values()[&value].ty,
                ),
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
fn program(op: Connective, mode: Use, width: usize) -> Arc<LocalProgram> {
    let source = package(fixture(op, mode, width).unwrap());
    let providers =
        PureProviderProgramCatalog::<std::io::Error>::try_new(&[], vec![], &Control).unwrap();
    let program = compile_fragment(
        validate_fragment_providers(source, &providers, &Control).unwrap(),
        &functions(),
        options(),
        &Control,
    )
    .unwrap();
    assert!(
        program
            .checked()
            .channels()
            .expressions()
            .resolved_calls()
            .calls()
            .is_empty()
    );
    Arc::new(program)
}
impl KernelEvaluationControl for Control {
    fn checkpoint(&self, _: u32) -> Result<(), KernelFailure> {
        Ok(())
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("intrinsic Boolean roots must not wait")
    }
}
fn root(mode: Use) -> ProgramExpressionRootSite {
    ProgramExpressionRootSite::Node {
        node: ProgramNodeId::new(2),
        role: match mode {
            Use::Value => ProgramNodeExpressionRole::ProjectOutput { expression: 0 },
            Use::TruthOnly => ProgramNodeExpressionRole::FilterPredicate { predicate: 0 },
        },
    }
}
fn instance(program: &Arc<LocalProgram>, mode: Use) -> CompiledExpressionInstance {
    CompiledExpressionInstance::try_new(program.clone(), root(mode), &Control).unwrap()
}
fn batch(program: &LocalProgram, columns: [Vec<Option<bool>>; 3]) -> RecordBatch {
    RecordBatch::try_new(
        program.graph().nodes()[1].output_layout().schema().clone(),
        columns
            .into_iter()
            .map(|values| Arc::new(BooleanArray::from(values)) as arrow::array::ArrayRef)
            .collect(),
    )
    .unwrap()
}

// Rows are authored in physical column order A,B,C. The connective evaluates
// C,A,B, and the SQL truth tables below are independent of scheduler output.
const ROWS: [[Option<bool>; 3]; 9] = [
    [Some(true), Some(true), Some(true)],
    [Some(true), Some(false), Some(true)],
    [Some(true), None, Some(true)],
    [Some(true), Some(false), None],
    [None, Some(true), Some(false)],
    [Some(true), Some(true), None],
    [Some(false), Some(false), Some(false)],
    [None, None, None],
    [Some(false), None, Some(true)],
];
const AND: [Option<bool>; 9] = [
    Some(true),
    Some(false),
    None,
    Some(false),
    Some(false),
    None,
    Some(false),
    None,
    Some(false),
];
const OR: [Option<bool>; 9] = [
    Some(true),
    Some(true),
    Some(true),
    Some(true),
    Some(true),
    Some(true),
    Some(false),
    None,
    Some(true),
];
fn sparse_batch(program: &LocalProgram) -> (RecordBatch, [usize; 9]) {
    let rows = [1, 3, 5, 7, 9, 11, 13, 15, 17];
    let mut columns = [vec![None; 24], vec![None; 24], vec![None; 24]];
    for (original, row) in rows.iter().zip(ROWS) {
        for (column, value) in columns.iter_mut().zip(row) {
            column[*original] = value;
        }
    }
    (batch(program, columns), rows)
}
#[test]
fn nullable_nary_three_and_320_have_independent_sql_truth_results_on_sparse_original_rows() {
    for op in [Connective::And, Connective::Or] {
        for width in [3, 320] {
            let program = program(op, Use::Value, width);
            let (input, rows) = sparse_batch(&program);
            let selection = Selection::try_sparse(input.num_rows(), &rows).unwrap();
            let mut evaluator = instance(&program, Use::Value);
            let output = evaluator.evaluate(&input, selection, &Control).unwrap();
            assert_eq!(output.selection(), selection);
            assert!(output.errors().is_empty());
            let values = output
                .values()
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap();
            let expected = match op {
                Connective::And => AND,
                Connective::Or => OR,
            };
            assert_eq!(values.iter().collect::<Vec<_>>(), expected);
            assert!(evaluator.instances.is_empty());
            // Definitions can be shared by occurrences, but computed values
            // cannot be cached across input batches by definition identity.
            let next = batch(
                &program,
                [
                    vec![Some(false); 2],
                    vec![Some(false); 2],
                    vec![Some(false); 2],
                ],
            );
            let output = evaluator
                .evaluate(&next, Selection::all(2), &Control)
                .unwrap();
            assert_eq!(
                output
                    .values()
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .unwrap()
                    .iter()
                    .collect::<Vec<_>>(),
                vec![Some(false); 2]
            );
            assert!(evaluator.instances.is_empty());
        }
    }
}
#[test]
fn filter_truth_only_nary_preserves_exact_keep_set_without_claiming_null_and_false_are_distinct() {
    for op in [Connective::And, Connective::Or] {
        for width in [3, 320] {
            let program = program(op, Use::TruthOnly, width);
            let (input, rows) = sparse_batch(&program);
            let selection = Selection::try_sparse(input.num_rows(), &rows).unwrap();
            let output = instance(&program, Use::TruthOnly)
                .evaluate(&input, selection, &Control)
                .unwrap();
            assert!(output.errors().is_empty());
            let expected = match op {
                Connective::And => AND,
                Connective::Or => OR,
            };
            assert_eq!(
                output
                    .values()
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .unwrap()
                    .iter()
                    .map(|value| value == Some(true))
                    .collect::<Vec<_>>(),
                expected
                    .into_iter()
                    .map(|value| value == Some(true))
                    .collect::<Vec<_>>()
            );
            let resolved = program.checked().channels().expressions().resolved_calls();
            let snapshot = resolved.snapshot();
            let root_use = snapshot.bindings()[&root(Use::TruthOnly)];
            let flow = &snapshot.flows()[&ProgramExpressionArena::Main];
            let invocation = &flow.uses()[&root_use];
            assert_eq!(invocation.context.demand, EvaluationDemand::TruthOnly);
            assert_eq!(invocation.control, op.shape());
            let arena = &snapshot.roots().arenas()[&ProgramExpressionArena::Main];
            let args = match arena.node(invocation.definition).unwrap().kind() {
                StaticExprKind::NaryAnd { args } | StaticExprKind::NaryOr { args } => args,
                _ => panic!("actual intrinsic definition"),
            };
            assert_eq!(args.len(), width);
            for (definition, child) in args.iter().zip(&invocation.arguments) {
                assert_eq!(flow.uses()[child].definition, *definition);
                assert_eq!(
                    flow.uses()[child].context.demand,
                    EvaluationDemand::TruthOnly
                );
            }
            assert!(resolved.calls().is_empty());
        }
    }
}
#[test]
fn empty_nary_selection_publishes_exact_boolean_empty_without_creating_instances() {
    for op in [Connective::And, Connective::Or] {
        for mode in [Use::Value, Use::TruthOnly] {
            let program = program(op, mode, 320);
            let (input, _) = sparse_batch(&program);
            let rows = [];
            let selection = Selection::try_sparse(input.num_rows(), &rows).unwrap();
            let mut evaluator = instance(&program, mode);
            let output = evaluator.evaluate(&input, selection, &Control).unwrap();
            assert_eq!(output.selection(), selection);
            assert_eq!(output.values().data_type(), &DataType::Boolean);
            assert_eq!(output.values().len(), 0);
            assert!(output.errors().is_empty());
            assert!(evaluator.instances.is_empty());
        }
    }
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
        panic!("intrinsic Boolean roots must not wait")
    }
}
fn causes() -> [KernelFailure; 7] {
    [
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
        KernelFailure::InvalidProgram(KernelDiagnostic::new("nary-original-invalid")),
        KernelFailure::Internal(KernelDiagnostic::new("nary-original-internal")),
        KernelFailure::Operational(KernelDiagnostic::new("nary-original-operational")),
        KernelFailure::InstanceFailed,
    ]
}
#[test]
fn every_nary_constructor_callback_preserves_all_seven_original_failure_categories() {
    let program = program(Connective::And, Use::Value, 320);
    let recorder = CallbackControl::new(KernelFailure::Cancelled, usize::MAX);
    let _ =
        CompiledExpressionInstance::try_new(program.clone(), root(Use::Value), &recorder).unwrap();
    let trace = recorder.trace.lock().unwrap().clone();
    assert!(trace.contains(&256));
    assert!(trace.iter().all(|units| *units <= 256));
    for index in 1..=trace.len() {
        for cause in causes() {
            let control = CallbackControl::new(cause.clone(), index);
            assert!(
                matches!(CompiledExpressionInstance::try_new(program.clone(), root(Use::Value), &control), Err(actual) if actual == cause)
            );
            assert_eq!(*control.trace.lock().unwrap(), trace[..index]);
        }
    }
}
#[test]
fn every_nary_evaluation_callback_preserves_primary_failure_and_refuses_replay() {
    let program = program(Connective::Or, Use::Value, 3);
    let input = batch(
        &program,
        [
            vec![None; 320],
            vec![Some(false); 320],
            vec![Some(true); 320],
        ],
    );
    assert_all_nary_evaluation_callbacks(program, input, Some(true));
}

#[test]
fn every_nary_later_child_callback_preserves_primary_failure_and_refuses_replay() {
    let program = program(Connective::Or, Use::Value, 3);
    // The actual ordered children are C,A,B. C=false cannot decide OR;
    // A=NULL must remain pending through B=false, leaving a successful NULL.
    // Thus every row crosses both later-child continuation boundaries.
    let input = batch(
        &program,
        [
            vec![None; 320],
            vec![Some(false); 320],
            vec![Some(false); 320],
        ],
    );
    assert_all_nary_evaluation_callbacks(program, input, None);
}

fn assert_all_nary_evaluation_callbacks(
    program: Arc<LocalProgram>,
    input: RecordBatch,
    expected: Option<bool>,
) {
    let recorder = CallbackControl::new(KernelFailure::Cancelled, usize::MAX);
    let output = instance(&program, Use::Value)
        .evaluate(&input, Selection::all(320), &recorder)
        .unwrap();
    assert!(output.errors().is_empty());
    assert_eq!(
        output
            .values()
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![expected; 320],
    );
    let trace = recorder.trace.lock().unwrap().clone();
    assert!(trace.contains(&256));
    assert!(trace.iter().all(|units| *units <= 256));
    for index in 1..=trace.len() {
        for cause in causes() {
            let mut evaluator = instance(&program, Use::Value);
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
            assert!(evaluator.instances.is_empty());
        }
    }
}

fn dual_demand_program() -> Arc<LocalProgram> {
    let functions = functions_with_if(true);
    let boolean = FunctionValueType::new(DataType::Boolean, true);
    let arguments = [
        FunctionArgument::Value {
            value_type: boolean.clone(),
            constant: None,
        },
        FunctionArgument::Value {
            value_type: FunctionValueType::new(DataType::Boolean, false),
            constant: None,
        },
        FunctionArgument::Value {
            value_type: boolean.clone(),
            constant: None,
        },
    ];
    let request = FunctionBindingRequest {
        arguments: &arguments,
        logical_argument_count: 3,
        expected_result_type: None,
    };
    let bound = functions
        .metadata()
        .resolve_bound_user("if", FunctionKind::Scalar, request, &Control)
        .unwrap();
    let selected = Arc::new(bound.selected.clone());
    let FunctionResultType::Scalar(result_type) = &selected.result_type else {
        panic!("actual IF scalar result")
    };
    let function = BoundFunction {
        function_id: bound.function_id.clone(),
        overload: selected.overload.clone(),
        kind: bound.kind,
        argument_types: selected.argument_types.clone(),
        result_type: result_type.clone(),
        legacy_metadata: Some(novarocks_physical_plan::LegacyBindingMetadata {
            volatility: bound.semantics.volatility,
            argument_evaluation: bound.semantics.argument_evaluation,
            failure_behavior: bound.semantics.failure_behavior,
            intrinsic_row_error: bound.semantics.intrinsic_row_error,
            semantic_parameters: Box::default(),
        }),
    };
    let source = NodeId::new(u32::MAX);
    let input = NodeId::new(41);
    let root_node = NodeId::new(0);
    let mut builder = FragmentBuilder::new(FragmentId::new(92));
    let operands = source_columns(&mut builder, source, input, root_node);
    let shared = builder
        .add_expression(
            root_node,
            boolean,
            ExprKind::Conjunction {
                args: Box::from([operands[2], operands[0], operands[1]]),
            },
        )
        .unwrap();
    let yes = builder
        .add_expression(
            root_node,
            FunctionValueType::new(DataType::Boolean, false),
            ExprKind::Literal(LiteralValue::Boolean(true)),
        )
        .unwrap();
    // The same physical definition is required first as an IF condition
    // (TruthOnly), and then as its ELSE result (Value) in another domain.
    let result = builder
        .add_expression(
            root_node,
            result_type.clone(),
            ExprKind::FunctionCall {
                function,
                args: Box::from([shared, yes, shared]),
            },
        )
        .unwrap();
    let value = builder
        .add_value(
            result_type.clone(),
            ValueOrigin::Expr {
                node: root_node,
                expr: result,
            },
        )
        .unwrap();
    builder
        .add_project(
            root_node,
            input,
            Box::from([(result, value)]),
            Box::from([value]),
        )
        .unwrap();
    let fixture = Fixture {
        fragment: builder
            .finish_definition(
                root_node,
                FragmentSink::Result,
                PipelineDopDomain {
                    min: 1,
                    max: 1,
                    requires_power_of_two: false,
                },
                novarocks_physical_plan::StagePlacementPolicy::Singleton,
            )
            .unwrap()
            .with_call_requests_observed(
                vec![(
                    novarocks_physical_plan::PhysicalCallDefinition::Expression(result),
                    novarocks_physical_plan::PhysicalCallRequest {
                        arguments: request
                            .arguments
                            .iter()
                            .map(|argument| {
                                let FunctionArgument::Value {
                                    value_type,
                                    constant: None,
                                } = argument
                                else {
                                    panic!("original IF fixture has nonconstant Value channels")
                                };
                                novarocks_physical_plan::StaticFunctionArgument::Value {
                                    value_type: value_type.clone(),
                                    constant: None,
                                }
                            })
                            .collect::<Vec<_>>()
                            .into_boxed_slice(),
                        logical_argument_count: request.logical_argument_count,
                        expected_result_type: request.expected_result_type.cloned(),
                        constant_policy: options().constants,
                    },
                )],
                &Control,
            )
            .unwrap(),
    };
    let expression_uses = uses(&fixture).unwrap();
    let flow = expression_uses.flow();
    let invocation = flow
        .uses()
        .values()
        .find(|invocation| invocation.definition == result)
        .unwrap();
    let context = invocation.context;
    let mut effects = ScopedExpressionEffects::pure_value(context);
    for (ordinal, child) in invocation.arguments.iter().enumerate() {
        effects = effects
            .join_control_argument(
                ScopedExpressionEffects::pure_value(flow.uses()[child].context),
                flow,
                ordinal,
            )
            .unwrap();
    }
    let child_uses = invocation
        .arguments
        .iter()
        .copied()
        .map(Some)
        .collect::<Vec<_>>();
    let parameters = SemanticParameters::try_new([]).unwrap();
    let token = functions
        .prepare_fresh(
            CallEffectInput {
                context,
                argument_uses: novarocks_functions::CallArgumentUses::SelectedChannels(&child_uses),
                function_id: &bound.function_id,
                kind: bound.kind,
                selected: selected.as_ref(),
                request,
                environment: &[],
                parameters: &parameters,
                decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
                proof_scope: CallProofScope::Domain(context.domain),
            },
            selected.clone(),
            PureCallPreparation::ControlIntrinsic { arguments: effects },
            &Control,
        )
        .unwrap();
    let calls = FrozenFragmentCalls::try_new(
        &fixture.fragment,
        &expression_uses,
        vec![FrozenPhysicalCall {
            materialized_writer_input: None,
            regexp_count_pattern_source: None,
            to_base64_byte_source: None,
            temporal_source: None,
            site: PhysicalCallSite::Expression(context.use_id),
            context,
            effects: token.call_contract().effects().clone(),
            decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
        }],
        &Control,
    )
    .unwrap();
    let package = package_with_calls(fixture, expression_uses, calls);
    let providers =
        PureProviderProgramCatalog::<std::io::Error>::try_new(&[], vec![], &Control).unwrap();
    Arc::new(
        compile_fragment(
            validate_fragment_providers(package, &providers, &Control).unwrap(),
            &functions,
            options(),
            &Control,
        )
        .unwrap(),
    )
}
#[test]
fn same_nary_definition_in_if_truth_only_and_value_domains_does_not_cache_null_as_false() {
    let program = dual_demand_program();
    let resolved = program.checked().channels().expressions().resolved_calls();
    assert_eq!(resolved.calls().len(), 1); // Only the actual IF control token.
    let snapshot = resolved.snapshot();
    let flow = &snapshot.flows()[&ProgramExpressionArena::Main];
    let invocation = &flow.uses()[&snapshot.bindings()[&root(Use::Value)]];
    assert_eq!(invocation.control, ControlShape::If);
    let condition = &flow.uses()[&invocation.arguments[0]];
    let otherwise = &flow.uses()[&invocation.arguments[2]];
    assert_eq!(condition.definition, otherwise.definition);
    assert_ne!(condition.context.use_id, otherwise.context.use_id);
    assert_ne!(condition.context.domain, otherwise.context.domain);
    assert_eq!(condition.context.demand, EvaluationDemand::TruthOnly);
    assert_eq!(otherwise.context.demand, EvaluationDemand::Value);
    let input = batch(
        &program,
        [
            vec![Some(true); 6],
            vec![Some(true); 6],
            vec![Some(false), None, Some(true), Some(false), None, Some(true)],
        ],
    );
    let rows = [1, 3, 5];
    let selection = Selection::try_sparse(6, &rows).unwrap();
    let mut evaluator = instance(&program, Use::Value);
    let output = evaluator.evaluate(&input, selection, &Control).unwrap();
    assert_eq!(output.selection(), selection);
    assert!(output.errors().is_empty());
    assert_eq!(
        output
            .values()
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![None, Some(false), Some(true)]
    );
    assert!(evaluator.instances.is_empty());
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

mod filter_conjunction_list_tests;
