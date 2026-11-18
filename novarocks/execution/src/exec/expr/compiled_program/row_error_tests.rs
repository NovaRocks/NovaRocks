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

use super::CompiledExpressionInstance;
use crate::exec::expr::compiled_program::checked_constants;
use arrow::{
    array::{Array, Decimal128Array, Float64Array, Int64Array},
    datatypes::DataType,
    record_batch::RecordBatch,
};
use novarocks_expression::{
    CompiledExprSet, EvaluationEnvironment, ScopeColumn, ScopeLayout, ScopeLayouts,
    compile_expr_set_pure,
};
use novarocks_expression_contract::{
    BoundExprSetBuilder, BoundFunction, DefinitionControl, ExprId, ExprKind, ExprPort,
    ExpressionCallClaim, PhysicalCallRequest, RootOrdinal, RootScope, UnitLimits, ValueId,
};
use novarocks_functions::{
    CallEffectInput, ConstantPolicy, ConstantValue, EngineFunctionCatalogBuilder, FunctionArgument,
    FunctionBindingRequest, FunctionBindingSelection, FunctionId, FunctionKind, FunctionOverloadId,
    FunctionResultType, InstalledPureKernel, KernelEvaluationControl, KernelFailure,
    PureCallPreparation, PureEngineFunctionCatalog, PureImplementationDeclaration,
    PureImplementationId, PureKernelAbi, ScopedExpressionEffects, Selection,
};
use novarocks_physical_plan::StaticFunctionArgument;
use novarocks_type_contract::{
    ArgumentControl, CallProofScope, CompileCheckpoints, CompileControlError, CompilePhase,
    ControlShape, DecimalOverflowPolicy, EvaluationDemand, ExpressionUseId, FunctionValueType,
    MAX_CONTROL_DEPTH, PureCompileControl, SemanticParameters, ValueTypeStorageLifetime,
};
use std::{collections::BTreeMap, sync::Arc, time::Duration};

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
        panic!("ROUND must not wait")
    }
}

fn catalogue() -> PureEngineFunctionCatalog {
    let actual =
        novarocks_functions::builtin::catalogue::build_builtin_engine_function_catalog().unwrap();
    let mut builder = EngineFunctionCatalogBuilder::new();
    for name in ["round", "rand"] {
        builder
            .register(
                actual
                    .definition(name, FunctionKind::Scalar)
                    .unwrap()
                    .clone(),
            )
            .unwrap();
    }
    // This is an explicitly sealed two-family test subset, not Server coverage.
    let mut manifest = vec![];
    for (name, overloads) in [
        ("round", vec!["builtin.scalar/round/dynamic-v1"]),
        (
            "rand",
            vec![
                "builtin.scalar/rand/()->f64;strict;legacy",
                "builtin.scalar/rand/(i64)->f64;strict;legacy",
            ],
        ),
    ] {
        for overload in overloads {
            manifest.push(InstalledPureKernel {
                function: FunctionId::try_new(format!("builtin.scalar/{name}/v1")).unwrap(),
                kind: FunctionKind::Scalar,
                implementation: PureImplementationDeclaration {
                    overload: FunctionOverloadId::try_new(overload).unwrap(),
                    implementation: PureImplementationId::try_new(format!(
                        "builtin.scalar/{name}/selected-v1"
                    ))
                    .unwrap(),
                    abi: PureKernelAbi::ScalarV1,
                },
                aggregate_state_format: None,
            });
        }
    }
    builder.seal_pure(manifest).unwrap()
}

struct Author {
    function: BoundFunction,
    selected: Arc<FunctionBindingSelection>,
    arguments: Vec<FunctionArgument>,
    logical_argument_count: usize,
    constant_policy: ConstantPolicy,
    control: ControlShape,
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
fn author(
    functions: &PureEngineFunctionCatalog,
    name: &str,
    arguments: Vec<FunctionArgument>,
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
    let declaration = functions
        .metadata()
        .pure_overload_declaration_observed(
            &bound.function_id,
            bound.kind,
            &selected.overload,
            &Control,
        )
        .unwrap();
    let control = match declaration.effects().argument_control {
        ArgumentControl::Eager => ControlShape::Eager,
        _ => panic!("the actual ROUND/RAND owners must declare eager control"),
    };
    let function = BoundFunction::from_exact_signature(
        bound.function_id,
        selected.overload.clone(),
        bound.kind,
        selected.argument_types.clone(),
        result.clone(),
    );
    Author {
        function,
        selected,
        arguments,
        logical_argument_count,
        constant_policy: constant_policy(),
        control,
    }
}
fn constant_policy() -> ConstantPolicy {
    ConstantPolicy {
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
    }
}
// Transfer the first resolver request; child shapes never reconstruct it.
fn original_request_sources(
    builder: &mut BoundExprSetBuilder,
    authors: &BTreeMap<ExprId, Author>,
    pools: &mut novarocks_physical_plan::ConstantPools,
) {
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
                        crate::exec::expr::test_support::register_constant(pools, value)
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
        builder
            .request(
                definition,
                PhysicalCallRequest {
                    arguments,
                    logical_argument_count: original.logical_argument_count,
                    expected_result_type: original.expected_result_type.cloned(),
                    constant_policy: owner.constant_policy,
                },
            )
            .unwrap();
        builder
            .definition_control(
                definition,
                DefinitionControl {
                    shape: owner.control,
                    temporal_sources: None,
                },
            )
            .unwrap();
    }
}
fn integer_constant(ty: &FunctionValueType, value: i64) -> ConstantValue {
    ConstantValue::from_i64(
        Arc::new(ty.try_to_field("fixture").unwrap()),
        ty.clone(),
        value,
        constant_policy(),
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
fn call(
    builder: &mut BoundExprSetBuilder,
    authors: &mut BTreeMap<ExprId, Author>,
    author: Author,
    args: Vec<ExprId>,
) -> ExprId {
    let id = builder
        .add(
            None,
            author.result(),
            ExprKind::FunctionCall {
                function: author.function.clone(),
                args: args.into_boxed_slice(),
            },
        )
        .unwrap();
    authors.insert(id, author);
    id
}

#[derive(Clone, Copy)]
enum Shape {
    Single,
    Nested,
    ObservableSibling,
}

struct Program {
    unit: Arc<CompiledExprSet>,
    roots: Vec<RootOrdinal>,
}

fn program(shape: Shape) -> Program {
    let mut constants = novarocks_physical_plan::ConstantPools::empty();
    let functions = catalogue();
    let value = ValueId::new(333);
    let digits_value = ValueId::new(901);
    let decimal = FunctionValueType::new(DataType::Decimal128(38, 0), true);
    let integer = FunctionValueType::new(DataType::Int64, true);
    let port_values = vec![(value, decimal.clone()), (digits_value, integer.clone())];
    let mut builder = BoundExprSetBuilder::new(UnitLimits {
        expression_semantic_depth: MAX_CONTROL_DEPTH,
    });
    let mut authors = BTreeMap::new();
    let first = if matches!(shape, Shape::ObservableSibling) {
        let ty = FunctionValueType::new(DataType::Int64, false);
        let seed = builder
            .add(
                None,
                ty.clone(),
                checked_constants::kind(
                    &mut constants,
                    checked_constants::i64(&ty, 42, constant_policy()),
                ),
            )
            .unwrap();
        call(
            &mut builder,
            &mut authors,
            author(&functions, "rand", vec![integer_argument(ty, 42)]),
            vec![seed],
        )
    } else {
        builder
            .add(None, decimal.clone(), ExprKind::Value(value))
            .unwrap()
    };
    let source_type = if matches!(shape, Shape::ObservableSibling) {
        authors[&first].result()
    } else {
        decimal.clone()
    };
    let digit = builder
        .add(
            None,
            integer.clone(),
            if matches!(shape, Shape::ObservableSibling) {
                ExprKind::Value(digits_value)
            } else {
                checked_constants::kind(
                    &mut constants,
                    checked_constants::i64(&integer, -1, constant_policy()),
                )
            },
        )
        .unwrap();
    let first_round = call(
        &mut builder,
        &mut authors,
        author(
            &functions,
            "round",
            vec![
                argument(source_type, None),
                argument(
                    integer.clone(),
                    if matches!(shape, Shape::ObservableSibling) {
                        None
                    } else {
                        Some(integer_constant(&integer, -1))
                    },
                ),
            ],
        ),
        vec![first, digit],
    );
    let root = if matches!(shape, Shape::Nested) {
        let ty = authors[&first_round].result();
        let zero = builder
            .add(
                None,
                integer.clone(),
                checked_constants::kind(
                    &mut constants,
                    checked_constants::i64(&integer, 0, constant_policy()),
                ),
            )
            .unwrap();
        call(
            &mut builder,
            &mut authors,
            author(
                &functions,
                "round",
                vec![argument(ty, None), integer_argument(integer, 0)],
            ),
            vec![first_round, zero],
        )
    } else {
        first_round
    };
    let mut roots = vec![
        builder
            .root(root, EvaluationDemand::Value, RootScope::Port(0))
            .unwrap(),
    ];
    if matches!(shape, Shape::ObservableSibling) {
        roots.push(
            builder
                .root(first, EvaluationDemand::Value, RootScope::Port(0))
                .unwrap(),
        );
    }
    original_request_sources(&mut builder, &authors, &mut constants);
    let parameters = SemanticParameters::try_new([]).unwrap();
    let mut work = CompileCheckpoints::try_new(&Control, CompilePhase::Validate).unwrap();
    let sealed = builder
        .seal(
            vec![ExprPort {
                values: port_values.clone().into_boxed_slice(),
            }],
            &mut work,
        )
        .unwrap();
    let mut effects = BTreeMap::<ExpressionUseId, ScopedExpressionEffects>::new();
    let mut claims = BTreeMap::new();
    // These fixed definitions were authored after their actual children. Check
    // that fixture premise instead of interpreting use numbers as a schedule.
    for definition in sealed.definitions() {
        for (&use_id, invocation) in sealed
            .uses()
            .iter()
            .filter(|(_, item)| item.definition == definition.id)
        {
            let context = invocation.context;
            let mut children = ScopedExpressionEffects::pure_value(context);
            for (ordinal, child) in invocation.arguments.iter().enumerate() {
                assert!(sealed.uses()[child].definition.get() < definition.id.get());
                children = children
                    .join_control_argument(effects[child], sealed.control_flow(), ordinal)
                    .unwrap();
            }
            let summary = if let Some(owner) = authors.get(&definition.id) {
                let argument_uses = invocation
                    .arguments
                    .iter()
                    .map(|id| Some(*id))
                    .collect::<Vec<_>>();
                let token = functions
                    .prepare_fresh(
                        CallEffectInput {
                            context: context.into(),
                            argument_uses: novarocks_functions::CallArgumentUses::SelectedChannels(
                                &argument_uses,
                            ),
                            function_id: &owner.function.function_id,
                            kind: FunctionKind::Scalar,
                            selected: owner.selected.as_ref(),
                            request: owner.request(),
                            environment: &[],
                            parameters: &parameters,
                            decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
                            proof_scope: CallProofScope::Region(context.region),
                        },
                        owner.selected.clone(),
                        PureCallPreparation::Scalar {
                            arguments: children,
                        },
                        &Control,
                    )
                    .unwrap();
                claims.insert(
                    use_id,
                    ExpressionCallClaim {
                        context,
                        effects: token.call_contract().effects().clone(),
                        decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
                        temporal_source: None,
                        regexp_count_pattern_source: None,
                        to_base64_byte_source: None,
                    },
                );
                ScopedExpressionEffects::primitive(
                    context,
                    token.effects().for_use(context).unwrap(),
                )
            } else {
                children
            };
            effects.insert(use_id, summary);
        }
    }
    let unit = sealed.finish(claims, &mut work).unwrap();
    work.finish().unwrap();
    let mut work = CompileCheckpoints::try_new(&Control, CompilePhase::LowerProgram).unwrap();
    let schema = Arc::new(arrow::datatypes::Schema::new(
        port_values
            .iter()
            .enumerate()
            .map(|(ordinal, (_, ty))| ty.try_to_field(format!("input_{ordinal}")).unwrap())
            .collect::<Vec<_>>(),
    ));
    let scope = ScopeLayout::try_new(
        schema,
        port_values.iter().map(|(_, ty)| ty.clone()).collect(),
        port_values
            .iter()
            .enumerate()
            .map(|(ordinal, (value, ty))| ScopeColumn {
                value: *value,
                ordinal: u32::try_from(ordinal).unwrap(),
                ty: ty.clone(),
            })
            .collect(),
        ValueTypeStorageLifetime::pure(),
        &mut work,
    )
    .unwrap();
    let compiled = compile_expr_set_pure(
        &unit,
        EvaluationEnvironment {
            constants: &constants,
            parameters: &parameters,
        },
        ScopeLayouts {
            ports: &[scope],
            all_ports: None,
        },
        constant_policy(),
        &functions,
        &mut work,
    )
    .unwrap();
    work.finish().unwrap();
    Program {
        unit: compiled,
        roots,
    }
}
fn instance(program: &Program, expression: usize) -> CompiledExpressionInstance {
    CompiledExpressionInstance::try_new(program.unit.clone(), program.roots[expression], &Control)
        .unwrap()
}
fn batch(program: &Program, values: Vec<Option<i128>>, digits: Vec<Option<i64>>) -> RecordBatch {
    let root = program.unit.root(program.roots[0]).unwrap();
    RecordBatch::try_new(
        program.unit.scope(root.scope).unwrap().schema().clone(),
        vec![
            Arc::new(
                Decimal128Array::from(values)
                    .with_precision_and_scale(38, 0)
                    .unwrap(),
            ),
            Arc::new(Int64Array::from(digits)),
        ],
    )
    .unwrap()
}

// This operator caller owns a real Values -> Project graph and publishes the
// same actual unit table through LocalProgram's original final author.
fn operator_program(source: &Program) -> novarocks_local_program::LocalProgram {
    use novarocks_local_program::*;
    use novarocks_type_contract::FunctionArgumentType;
    use std::{collections::BTreeSet, num::NonZeroUsize};
    let root = source.unit.root(source.roots[0]).unwrap();
    let scope = source.unit.scope(root.scope).unwrap();
    let input = StaticLayout::try_new_typed_for_compile(
        scope.schema().clone(),
        Arc::from([
            novarocks_types::SlotId::new(333),
            novarocks_types::SlotId::new(901),
        ]),
        scope
            .ordinal_types()
            .iter()
            .map(|ty| (ty.typed_binding().clone(), None))
            .collect(),
        ValueTypeStorageLifetime::pure(),
        |_| Ok(()),
        &Control,
    )
    .unwrap();
    let values = StaticValues::try_new(
        RecordBatch::try_new(
            input.schema().clone(),
            scope
                .ordinal_types()
                .iter()
                .map(|ty| arrow::array::new_null_array(ty.data_type(), 0))
                .collect(),
        )
        .unwrap(),
        input.clone(),
    )
    .unwrap();
    let FunctionArgumentType::Value(output_type) = &source.unit.types()[root.definition.index()]
    else {
        panic!("the actual Project root is scalar")
    };
    let output = StaticLayout::try_new_typed_for_compile(
        Arc::new(arrow::datatypes::Schema::new(vec![
            output_type.try_to_field("result").unwrap(),
        ])),
        Arc::from([novarocks_types::SlotId::new(77)]),
        vec![(output_type.typed_binding().clone(), None)],
        ValueTypeStorageLifetime::pure(),
        |_| Ok(()),
        &Control,
    )
    .unwrap();
    let site = ProgramRootRef {
        unit: ProgramUnitId::new(0),
        root: source.roots[0],
    };
    let nodes = vec![
        ProgramNode::new_local(
            ProgramNodeId::new(0),
            vec![DiagnosticSourceNodeId::new(17)],
            ProgramNodeKind::Values { values },
            input,
        ),
        ProgramNode::new_local(
            ProgramNodeId::new(1),
            vec![DiagnosticSourceNodeId::new(2)],
            ProgramNodeKind::Project {
                input: ProgramNodeId::new(0),
                is_subordinate: false,
                validate_final_result_input: false,
                exprs: vec![site],
                expr_slot_ids: vec![novarocks_types::SlotId::new(77)],
                expr_slot_schemas: None,
                output_indices: None,
            },
            output.clone(),
        ),
    ];
    let graph = LocalProgramGraph::try_new_with_sink(
        nodes,
        ProgramNodeId::new(1),
        std::slice::from_ref(&source.unit),
        CompileProfile::new(
            NonZeroUsize::new(1).unwrap(),
            None,
            output.identity().unwrap(),
            KernelAbiVersion::CURRENT,
        ),
        BindingRequirements::try_new(vec![BindingRequirement::ResultSink { layout: output }])
            .unwrap(),
        Some(StaticSinkProgram::Result),
    )
    .unwrap();
    let channels = [
        (0, 0, scope.ordinal_types()[0].clone()),
        (0, 1, scope.ordinal_types()[1].clone()),
        (1, 0, output_type.clone()),
    ]
    .into_iter()
    .map(|(node, ordinal, ty)| {
        (
            ProgramChannelSite::Layout {
                node: ProgramNodeId::new(node),
                role: ProgramChannelLayoutRole::NodeOutput,
                ordinal,
            },
            ty,
        )
    })
    .collect();
    let operators = [(0, 17), (1, 2)]
        .into_iter()
        .map(|(index, source)| LocalOperatorProvenance {
            id: LocalOperatorId::new(index),
            lowered_nodes: Box::from([ProgramNodeId::new(index as usize)]),
            sources: Box::from([DiagnosticSourceNodeId::new(source)]),
            origin: LocalOperatorOrigin::Direct,
            cost_owner: LocalOperatorId::new(index),
            metrics: OperatorMetricAggregation {
                cpu_time: MetricAggregation::Sum,
                wall_time: MetricAggregation::Maximum,
                peak_retained_bytes: MetricAggregation::Maximum,
            },
        })
        .collect();
    LocalProgram::try_new(
        graph,
        vec![source.unit.clone()],
        vec![],
        channels,
        operators,
        &BTreeSet::from([
            DiagnosticSourceNodeId::new(17),
            DiagnosticSourceNodeId::new(2),
        ]),
        CompiledProgramFacts::default(),
        &Control,
    )
    .unwrap()
}

#[test]
fn strict_null_and_real_decimal_overflow_remap_to_parent_selected_ordinal() {
    let program = program(Shape::Single);
    let max38 = 10_i128.pow(38) - 1;
    let input = batch(
        &program,
        vec![Some(123), None, Some(max38), Some(25), Some(15)],
        vec![None; 5],
    );
    let rows = [1, 2, 4];
    let selection = Selection::try_sparse(5, &rows).unwrap();
    let mut evaluator = instance(&program, 0);
    let result = evaluator.evaluate(&input, selection, &Control).unwrap();
    let values = result
        .values()
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    assert_eq!(result.selection(), selection);
    assert_eq!(values.len(), 3);
    assert!(values.is_null(0));
    assert!(values.is_null(1));
    assert_eq!(values.value(2), 20);
    assert_eq!(result.errors().len(), 1);
    assert_eq!(result.errors()[0].selected_ordinal(), 1);
    assert!(result.errors()[0].message().contains("overflow"));
    // Row errors do not poison a successful evaluator instance.
    let clean = batch(&program, vec![Some(25)], vec![None]);
    let next = evaluator
        .evaluate(&clean, Selection::all(1), &Control)
        .unwrap();
    assert!(next.errors().is_empty());
}

#[test]
fn nested_round_keeps_required_child_error_and_successful_null_distinct() {
    let program = program(Shape::Nested);
    let input = batch(
        &program,
        vec![Some(15), None, Some(10_i128.pow(38) - 1), Some(25)],
        vec![None; 4],
    );
    let rows = [1, 2, 3];
    let result = instance(&program, 0)
        .evaluate(&input, Selection::try_sparse(4, &rows).unwrap(), &Control)
        .unwrap();
    let values = result
        .values()
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    assert!(values.is_null(0));
    assert!(values.is_null(1));
    assert_eq!(values.value(2), 30);
    assert_eq!(result.errors().len(), 1);
    assert_eq!(result.errors()[0].selected_ordinal(), 1);
}

#[test]
fn eager_rng_child_advances_even_when_parent_strict_digits_are_null() {
    let program = program(Shape::ObservableSibling);
    let mut parent = instance(&program, 0);
    let mut reference = instance(&program, 1);
    let null_digits = batch(&program, vec![None; 2], vec![None; 2]);
    let skipped = parent
        .evaluate(&null_digits, Selection::all(2), &Control)
        .unwrap();
    assert_eq!(skipped.values().null_count(), 2);
    assert!(skipped.errors().is_empty());
    let first_two = reference
        .evaluate(&null_digits, Selection::all(2), &Control)
        .unwrap();
    assert_eq!(first_two.values().null_count(), 0);
    let next = batch(&program, vec![None], vec![Some(15)]);
    let actual = parent.evaluate(&next, Selection::all(1), &Control).unwrap();
    let reference = reference
        .evaluate(&next, Selection::all(1), &Control)
        .unwrap();
    let source = reference
        .values()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap()
        .value(0);
    // Independent formula for this finite, positive, small-magnitude digit case.
    let expected = (source * 1e15).round() / 1e15;
    assert_eq!(
        actual
            .values()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0)
            .to_bits(),
        expected.to_bits()
    );
    assert!(actual.errors().is_empty());
}

// The operator helper behind ChangeEventExpand assignments evaluates only the
// selected rows: an overflow on an unselected row raises nothing, and a
// selected row's overflow is a required error reported at its batch row.
#[test]
fn operator_selected_evaluation_skips_unselected_row_errors_and_reports_batch_rows() {
    use crate::exec::operators::compiled_expression::evaluate_selected;
    let program = program(Shape::Single);
    let max38 = 10_i128.pow(38) - 1;
    let input = batch(
        &program,
        vec![Some(15), Some(max38), Some(25)],
        vec![None; 3],
    );
    let local = operator_program(&program);
    let node = &local.graph().nodes()[local.graph().root().index()];
    let novarocks_local_program::ProgramNodeKind::Project { exprs, .. } = node.kind() else {
        panic!("the actual caller is a Project")
    };
    let site = exprs[0];
    let actual_node = node.local_id().unwrap();
    let unit = local.unit(site.unit).unwrap();
    assert!(Arc::ptr_eq(unit, &program.unit));
    assert_eq!(site.root, program.roots[0]);
    let mut evaluator =
        CompiledExpressionInstance::try_new(unit.clone(), site.root, &Control).unwrap();
    let control =
        crate::exec::operators::compiled_expression::RuntimeKernelControl::new(Arc::default());
    let values = evaluate_selected(&mut evaluator, site, &input, &[0, 2], &control)
        .map_err(|cause| cause.at_expression_node(actual_node))
        .unwrap();
    let values = values.as_any().downcast_ref::<Decimal128Array>().unwrap();
    assert_eq!(values.len(), 2);
    assert_eq!((values.value(0), values.value(1)), (20, 30));
    let original = evaluate_selected(&mut instance(&program, 0), site, &input, &[1, 2], &control)
        .expect_err("the selected overflow row is required");
    let error = original.clone().at_expression_node(actual_node);
    assert!(error.same_occurrence(&original));
    assert_eq!(
        novarocks_failure_contract::FailureProjection::failure_fact(&error),
        novarocks_failure_contract::FailureProjection::failure_fact(&original),
    );
    assert_eq!(error.expression_node(), Some(actual_node));
    let crate::runtime::task::execution_failure::ExecutionFailureCause::RequiredRow(required) =
        error.cause()
    else {
        panic!("the selected ROUND overflow must retain its typed row error")
    };
    assert_eq!(required.root(), site.root);
    assert_eq!(required.batch_row(), 1);
    assert_eq!(required.error().selected_ordinal(), 0);
    assert!(required.error().message().contains("overflow"));
    assert!(std::ptr::eq(
        std::error::Error::source(required)
            .unwrap()
            .downcast_ref::<novarocks_functions::RowDataError>()
            .unwrap(),
        required.error(),
    ));
    let message = error.to_string();
    assert!(
        message.contains("failed at batch row 1 (selected ordinal 0)"),
        "{message}"
    );
    assert!(message.contains("overflow"), "{message}");
    let unordered = evaluate_selected(&mut instance(&program, 0), site, &input, &[2, 0], &control)
        .map_err(|cause| cause.at_expression_node(actual_node))
        .expect_err("rows must be strictly increasing");
    assert_eq!(unordered.expression_node(), None);
    assert!(unordered.to_string().contains("not ordered"), "{unordered}");
}
