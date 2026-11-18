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
use crate::exec::expr::compiled_program::checked_constants;
use arrow::{array::BooleanArray, datatypes::DataType, record_batch::RecordBatch};
use novarocks_connector_contract::PureProviderProgramCatalog;
use novarocks_expression::StaticExprKind;
use novarocks_expression::testkit::PassThroughCallHost;
use novarocks_expression::{EvaluationInput, ExprEvaluationFailure};
use novarocks_expression_contract::{ExprId, ExprKind, RootScope};
use novarocks_functions::{
    ConstantPolicy, EngineFunctionCatalogBuilder, FunctionArgument, FunctionId, FunctionKind,
    FunctionOverloadId, InstalledPureKernel, KernelEvaluationControl, KernelFailure,
    PureEngineFunctionCatalog, PureImplementationDeclaration, PureImplementationId, PureKernelAbi,
    Selection,
};
use novarocks_functions::{ScalarInvocationActivation, ScalarInvocationScope};
use novarocks_local_compiler::{
    LocalCompileOptions, compile_fragment, validate_fragment_providers,
};
use novarocks_local_program::{KernelAbiVersion, LocalProgram, ProgramNodeKind, ProgramRootRef};
use novarocks_physical_plan::{
    Fragment, FragmentBuilder, FragmentCuts, FragmentId, FragmentPackage, FragmentPackageInput,
    FragmentSink, FrozenFragmentPruning, NodeId, PipelineDopDomain, PlanVersionId,
    RequiredContracts, ResultField, ResultPort, ValidationErrors, ValueDef, ValueId, ValueOrigin,
};
use novarocks_type_contract::{
    CompileControlError, CompilePhase, ControlShape, EvaluationDemand, FunctionValueType,
    PureCompileControl, SemanticParameters,
};
use std::{collections::BTreeMap, num::NonZeroUsize, sync::Arc, time::Duration};

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
    constants: novarocks_physical_plan::ConstantPools,
}
fn source_columns(
    builder: &mut FragmentBuilder,
    source: NodeId,
    input: NodeId,
    root: NodeId,
    source_pools: &mut novarocks_physical_plan::ConstantPools,
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
            .unit(input)
            .unwrap()
            .add(
                None,
                ty.clone(),
                checked_constants::kind(
                    source_pools,
                    checked_constants::null(&(ty.clone()), options().constants),
                ),
            )
            .unwrap();
        let input_root = builder
            .unit(input)
            .unwrap()
            .root(definition, EvaluationDemand::Value, RootScope::Port(0))
            .unwrap();
        builder
            .insert_value(ValueDef {
                id: value,
                ty: ty.clone(),
                origin: ValueOrigin::Expr {
                    node: input,
                    root: input_root,
                },
            })
            .unwrap();
        items.push((input_root, value));
        operands.push(
            builder
                .unit(root)
                .unwrap()
                .add(None, ty, ExprKind::Value(value))
                .unwrap(),
        );
    }
    builder
        .add_project(input, source, items.into_boxed_slice(), Box::from(values))
        .unwrap();
    operands
}
fn fixture(op: Connective, mode: Use, width: usize) -> Result<Fixture, ValidationErrors> {
    let mut source_pools = novarocks_physical_plan::ConstantPools::empty();
    let fragment_id = FragmentId::new(91);
    let source = NodeId::new(u32::MAX);
    let input = NodeId::new(41);
    let root = NodeId::new(0);
    let boolean = FunctionValueType::new(DataType::Boolean, true);
    let mut builder = FragmentBuilder::new(fragment_id);
    let operands = source_columns(&mut builder, source, input, root, &mut source_pools);
    // Neither ValueId nor definition order establishes the argument order.
    let order = [operands[2], operands[0], operands[1]];
    let args = (0..width)
        .map(|ordinal| order[ordinal % 3])
        .collect::<Vec<_>>()
        .into_boxed_slice();
    let result_type = boolean;
    let connective = builder
        .unit(root)
        .unwrap()
        .add(None, result_type.clone(), op.kind(args))
        .unwrap();
    let connective = builder
        .unit(root)
        .unwrap()
        .root(
            connective,
            match mode {
                Use::Value => EvaluationDemand::Value,
                Use::TruthOnly => EvaluationDemand::TruthOnly,
            },
            RootScope::Port(0),
        )
        .unwrap();
    match mode {
        Use::Value => {
            let result = builder
                .add_value(
                    result_type,
                    ValueOrigin::Expr {
                        node: root,
                        root: connective,
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
    let source_pools = super::guarded_tests::finish_fixture_units(
        &functions(),
        &mut builder,
        root,
        &BTreeMap::new(),
        &SemanticParameters::try_new([]).unwrap(),
        source_pools,
        |_, _, children| children,
    );
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
        constants: source_pools,
    })
}
fn package(fixture: Fixture) -> Arc<FragmentPackage> {
    assert!(fixture.fragment.nodes().values().all(|node| {
        node.expressions
            .as_ref()
            .is_none_or(|unit| unit.claims().is_empty())
    }));
    package_with_units(fixture)
}
fn package_with_units(fixture: Fixture) -> Arc<FragmentPackage> {
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
                constants: fixture.constants,
                fragment: fixture.fragment,
                statement_constant_policy: options().constants,
                cuts: FragmentCuts::default(),
                result: Some(result),
                parameters: SemanticParameters::try_new([]).unwrap(),
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
    assert!(program.units().iter().all(|unit| unit.calls().is_empty()));
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
fn root(program: &LocalProgram, mode: Use) -> ProgramRootRef {
    match (
        mode,
        program.graph().nodes()[program.graph().root().index()].kind(),
    ) {
        (Use::Value, ProgramNodeKind::Project { exprs, .. }) => exprs[0],
        (Use::TruthOnly, ProgramNodeKind::Filter { predicates, .. }) => predicates[0],
        _ => panic!("actual root node differs from the fixture's declared demand"),
    }
}
fn instance(program: &Arc<LocalProgram>, mode: Use) -> CompiledExpressionInstance {
    let reference = root(program, mode);
    CompiledExpressionInstance::try_new(
        program.unit(reference.unit).unwrap().clone(),
        reference.root,
        &Control,
    )
    .unwrap()
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
            let output = evaluator
                .evaluate(
                    EvaluationInput::Batch {
                        batch: &input,
                        source: None,
                    },
                    ScalarInvocationScope::new(selection, selection),
                    ScalarInvocationActivation::Activated,
                    &Control,
                    &mut PassThroughCallHost,
                )
                .unwrap();
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
                .evaluate(
                    EvaluationInput::Batch {
                        batch: &next,
                        source: None,
                    },
                    ScalarInvocationScope::new(Selection::all(2), Selection::all(2)),
                    ScalarInvocationActivation::Activated,
                    &Control,
                    &mut PassThroughCallHost,
                )
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
                .evaluate(
                    EvaluationInput::Batch {
                        batch: &input,
                        source: None,
                    },
                    ScalarInvocationScope::new(selection, selection),
                    ScalarInvocationActivation::Activated,
                    &Control,
                    &mut PassThroughCallHost,
                )
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
            let reference = root(&program, Use::TruthOnly);
            let unit = program.unit(reference.unit).unwrap();
            let root_use = unit.root(reference.root).unwrap().use_id;
            let flow = unit.control_flow();
            let invocation = &flow.uses()[&root_use];
            assert_eq!(invocation.context.demand, EvaluationDemand::TruthOnly);
            assert_eq!(invocation.control, op.shape());
            let arena = unit.definitions();
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
            assert!(unit.calls().is_empty());
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
            let output = evaluator
                .evaluate(
                    EvaluationInput::Batch {
                        batch: &input,
                        source: None,
                    },
                    ScalarInvocationScope::new(selection, selection),
                    ScalarInvocationActivation::Activated,
                    &Control,
                    &mut PassThroughCallHost,
                )
                .unwrap();
            assert_eq!(output.selection(), selection);
            assert_eq!(output.values().data_type(), &DataType::Boolean);
            assert_eq!(output.values().len(), 0);
            assert!(output.errors().is_empty());
            assert!(evaluator.instances.is_empty());
        }
    }
}
use novarocks_expression::testkit::{CallbackControl, callback_causes};

fn causes() -> [KernelFailure; 7] {
    callback_causes([
        "nary-original-invalid",
        "nary-original-internal",
        "nary-original-operational",
    ])
}

#[test]
fn every_nary_constructor_callback_preserves_all_seven_original_failure_categories() {
    let program = program(Connective::And, Use::Value, 320);
    let recorder = CallbackControl::new(KernelFailure::Cancelled, usize::MAX);
    let _ = CompiledExpressionInstance::try_new(
        program
            .unit(root(&program, Use::Value).unit)
            .unwrap()
            .clone(),
        root(&program, Use::Value).root,
        &recorder,
    )
    .unwrap();
    let trace = recorder.trace();
    assert!(trace.contains(&256));
    assert!(trace.iter().all(|units| *units <= 256));
    for index in 1..=trace.len() {
        for cause in causes() {
            let control = CallbackControl::new(cause.clone(), index);
            assert!(
                matches!(CompiledExpressionInstance::try_new(program.unit(root(&program, Use::Value).unit).unwrap().clone(), root(&program, Use::Value).root, &control), Err(actual) if actual == cause)
            );
            assert_eq!(control.trace(), trace[..index]);
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
        .evaluate(
            EvaluationInput::Batch {
                batch: &input,
                source: None,
            },
            ScalarInvocationScope::new(Selection::all(320), Selection::all(320)),
            ScalarInvocationActivation::Activated,
            &recorder,
            &mut PassThroughCallHost,
        )
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
    let trace = recorder.trace();
    assert!(trace.contains(&256));
    assert!(trace.iter().all(|units| *units <= 256));
    for index in 1..=trace.len() {
        for cause in causes() {
            let mut evaluator = instance(&program, Use::Value);
            let control = CallbackControl::new(cause.clone(), index);
            assert!(
                matches!(evaluator.evaluate(EvaluationInput::Batch { batch: &input, source: None }, ScalarInvocationScope::new(Selection::all(320), Selection::all(320)), ScalarInvocationActivation::Activated, &control, &mut PassThroughCallHost), Err(ExprEvaluationFailure::Kernel(actual)) if actual == cause)
            );
            assert_eq!(control.trace(), trace[..index]);
            let after = CallbackControl::new(KernelFailure::Cancelled, usize::MAX);
            assert!(matches!(
                evaluator.evaluate(
                    EvaluationInput::Batch {
                        batch: &input,
                        source: None
                    },
                    ScalarInvocationScope::new(Selection::all(320), Selection::all(320)),
                    ScalarInvocationActivation::Activated,
                    &after,
                    &mut PassThroughCallHost
                ),
                Err(ExprEvaluationFailure::Kernel(KernelFailure::InstanceFailed))
            ));
            assert!(after.trace().is_empty());
            assert!(evaluator.instances.is_empty());
        }
    }
}

fn dual_demand_program() -> Arc<LocalProgram> {
    let mut source_pools = novarocks_physical_plan::ConstantPools::empty();
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
    let owner =
        super::guarded_tests::author(&functions, "if", arguments.to_vec(), ControlShape::If)
            .with_decimal_policy(novarocks_type_contract::DecimalOverflowPolicy::OutputNull);
    let result_type = owner.result();
    let source = NodeId::new(u32::MAX);
    let input = NodeId::new(41);
    let root_node = NodeId::new(0);
    let mut builder = FragmentBuilder::new(FragmentId::new(92));
    let operands = source_columns(&mut builder, source, input, root_node, &mut source_pools);
    let shared = builder
        .unit(root_node)
        .unwrap()
        .add(
            None,
            boolean,
            ExprKind::Conjunction {
                args: Box::from([operands[2], operands[0], operands[1]]),
            },
        )
        .unwrap();
    let yes = builder
        .unit(root_node)
        .unwrap()
        .add(
            None,
            (FunctionValueType::new(DataType::Boolean, false)).clone(),
            checked_constants::kind(
                &mut source_pools,
                checked_constants::boolean(
                    &(FunctionValueType::new(DataType::Boolean, false)),
                    true,
                    options().constants,
                ),
            ),
        )
        .unwrap();
    // The same physical definition is required first as an IF condition
    // (TruthOnly), and then as its ELSE result (Value) in another domain.
    let mut authors = BTreeMap::new();
    let result =
        super::guarded_tests::call(&mut builder, &mut authors, owner, vec![shared, yes, shared]);
    let ExprKind::FunctionCall { function, .. } = &builder
        .construction_unit(root_node)
        .unwrap()
        .definition(result)
        .unwrap()
        .kind
    else {
        panic!("the original IF call definition")
    };
    assert_eq!(function.function_id.as_str(), "builtin.scalar/if/v1");
    let output_root = builder
        .unit(root_node)
        .unwrap()
        .root(result, EvaluationDemand::Value, RootScope::Port(0))
        .unwrap();
    let value = builder
        .add_value(
            result_type.clone(),
            ValueOrigin::Expr {
                node: root_node,
                root: output_root,
            },
        )
        .unwrap();
    builder
        .add_project(
            root_node,
            input,
            Box::from([(output_root, value)]),
            Box::from([value]),
        )
        .unwrap();
    let source_pools = super::guarded_tests::finish_fixture_units(
        &functions,
        &mut builder,
        root_node,
        &authors,
        &SemanticParameters::try_new([]).unwrap(),
        source_pools,
        |_, _, children| children,
    );
    let fixture = Fixture {
        constants: source_pools,
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
            .unwrap(),
    };
    let package = package_with_units(fixture);
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
    let reference = root(&program, Use::Value);
    let unit = program.unit(reference.unit).unwrap();
    assert_eq!(unit.calls().len(), 1); // Only the actual IF control token.
    let flow = unit.control_flow();
    let invocation = &flow.uses()[&unit.root(reference.root).unwrap().use_id];
    assert_eq!(invocation.control, ControlShape::If);
    let condition = &flow.uses()[&invocation.arguments[0]];
    let otherwise = &flow.uses()[&invocation.arguments[2]];
    assert_eq!(condition.definition, otherwise.definition);
    assert_ne!(condition.context.use_id, otherwise.context.use_id);
    assert_ne!(condition.context.region, otherwise.context.region);
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
    let output = evaluator
        .evaluate(
            EvaluationInput::Batch {
                batch: &input,
                source: None,
            },
            ScalarInvocationScope::new(selection, selection),
            ScalarInvocationActivation::Activated,
            &Control,
            &mut PassThroughCallHost,
        )
        .unwrap();
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
