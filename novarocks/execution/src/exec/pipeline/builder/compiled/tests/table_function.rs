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

//! Compiled TableFunction over the real builtin UNNEST: a physical plan
//! authored with the real builder and binding, frozen as the FE freezes it,
//! compiled by local-compiler and run through the compiled pipeline. The
//! oracle is the longest-zip expansion of each row's arrays, in row order,
//! with one NULL-extended row per empty LEFT OUTER parent.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::ListArray;
use arrow::datatypes::{DataType, Field, Int64Type};
use novarocks_expression::{
    EvaluationEnvironment, ScopeColumn, ScopeLayout, ScopeLayouts, compile_expr_set_pure,
};
use novarocks_expression_contract::{
    BinaryOperator, ExprKind, LegacyBindingMetadata, PhysicalCallRequest, RootScope,
};
use novarocks_functions::{
    CallEffectInput, ConstantPool, EngineFunctionCatalogBuilder, FunctionArgument,
    FunctionBindingRequest, FunctionBindingSelection, FunctionId, FunctionKind, FunctionOverloadId,
    FunctionResultType, InstalledPureKernel, PureCallPreparation, PureEngineFunctionCatalog,
    PureImplementationDeclaration, PureImplementationId, PureKernelAbi, ScopedRelationalEffects,
};
use novarocks_local_program::{LocalProgram, ProgramNodeKind};
use novarocks_physical_plan::{
    BoundTableFunction, ConstantPoolId, ConstantPools, ConstantReference, Distribution, EdgeId,
    ExprId, Fragment, FragmentId, FragmentPackage, FragmentPackageAdmission, FragmentSink,
    FrozenFragmentPruning, FrozenPhysicalCall, NodeId, NodeKind, NullOrdering, OutputPort,
    PhysicalCallSite, PhysicalNode, PhysicalPlan, PhysicalProperties, PipelineDopDomain,
    PlanLimits, PlanVersionId, PropertyProofProjectionLimits, RequiredInputs, ResultField,
    ResultPort, SetOperationKind, SortDirection, SortExpr, SortMode, StaticFunctionArgument,
    TableFunctionOutput, ValueId, ValueOrigin, extract_fragment_packages, passthrough_requirement,
};
use novarocks_type_contract::{
    CallProofScope, CompileCheckpoints, CompilePhase, DecimalOverflowPolicy, EvaluationDemand,
    ExpressionEffects, FunctionValueType, RelationCallId, RelationEffectContext,
    SemanticParameterId, SemanticParameterKey, SemanticParameterRef, SemanticParameterValue,
    SemanticParameters, ValueTypeStorageLifetime, arithmetic_result_value_type_with_op,
};

use novarocks_types::UniqueId;

use super::super::aggregate_fixture::{
    LoopbackTransmitter, destination, edge, hash, receive, register, result_sink, stream_sink,
    try_compile, try_run_in_fixture,
};
use super::super::family_fixture::{FixtureControl, constant_policy, int64, int64_rows};
use crate::exec::chunk::Chunk;
use crate::exec::operators::{ResultSinkBuffer, ResultSinkFactory};
use crate::exec::pipeline::binding::ExchangeBindings;
use crate::exec::pipeline::executor::prepare_compiled_program_pipeline_execution;
use crate::runtime::query_options::QueryOptions;
use crate::runtime::runtime_state::RuntimeState;
use crate::runtime::task::io::NoopTaskEventSink;
use crate::runtime::task::io::exchange::test_support::Fixture;

const SOLE: FragmentId = FragmentId::new(70);
const SOURCE: FragmentId = FragmentId::new(71);
const CONSUMER: FragmentId = FragmentId::new(72);
const TO_CONSUMER: EdgeId = EdgeId::new(9);
const SOURCE_FINST: UniqueId = UniqueId::new(0xa1, 0x01);
const CONSUMER_FINST: UniqueId = UniqueId::new(0xa2, 0x01);
const POLICY: DecimalOverflowPolicy = DecimalOverflowPolicy::ReportError;
const ALLOW: SemanticParameterRef = SemanticParameterRef {
    id: SemanticParameterId::new(3),
    expected_key: SemanticParameterKey::AllowThrowException,
};

/// One row: its key and one optional array per list column.
type Row = (i64, Vec<Option<Vec<Option<i64>>>>);

/// The sealed builtin UNNEST owner with its actually installed TableV1 kernel.
fn unnest_catalog() -> PureEngineFunctionCatalog {
    let actual =
        novarocks_functions::builtin::catalogue::build_builtin_engine_function_catalog().unwrap();
    let mut builder = EngineFunctionCatalogBuilder::new();
    builder
        .register(
            actual
                .definition("unnest", FunctionKind::Table)
                .unwrap()
                .clone(),
        )
        .unwrap();
    builder
        .seal_pure([InstalledPureKernel {
            function: FunctionId::try_new("builtin.table/unnest/v1").unwrap(),
            kind: FunctionKind::Table,
            implementation: PureImplementationDeclaration {
                overload: FunctionOverloadId::try_new("builtin.table/unnest/array-variadic-v1")
                    .unwrap(),
                implementation: PureImplementationId::try_new("builtin.table/unnest/selected-v1")
                    .unwrap(),
                abi: PureKernelAbi::TableV1,
            },
            aggregate_state_format: None,
        }])
        .unwrap()
}

fn list_type() -> FunctionValueType {
    FunctionValueType::new(
        DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
        true,
    )
}

/// One call argument: a list column, or a list column behind a CASE whose
/// condition overflows (`CASE WHEN k + i64::MAX IS NULL THEN l ELSE l END`),
/// which is a guarded List-valued root.
#[derive(Clone, Copy)]
enum Arg {
    List(usize),
    Faulty(usize),
}

/// One output occurrence of the table function.
#[derive(Clone, Copy)]
enum Out {
    Key,
    Result(u32),
}

struct Spec {
    lists: usize,
    args: Vec<Arg>,
    outputs: Vec<Out>,
    left_outer: bool,
}
impl Spec {
    fn unnest(left_outer: bool) -> Self {
        Self {
            lists: 1,
            args: vec![Arg::List(0)],
            outputs: vec![Out::Key, Out::Result(0)],
            left_outer,
        }
    }
}

/// Plan-level constant pools of every authored list cell.
struct Pools(ConstantPools, u32);
impl Pools {
    fn new() -> Self {
        Self(ConstantPools::empty(), 0)
    }
    /// One pool holding `arrays` as rows.
    fn add(&mut self, arrays: impl Iterator<Item = Option<Vec<Option<i64>>>>) -> ConstantPoolId {
        let array = ListArray::from_iter_primitive::<Int64Type, _, _>(arrays);
        let ty = list_type();
        let pool = ConstantPool::try_new(
            Arc::new(ty.try_to_field("analyzed").unwrap()),
            ty,
            arrow::array::Array::to_data(&array),
            constant_policy(),
            CompilePhase::Validate,
            &FixtureControl,
        )
        .unwrap();
        let id = ConstantPoolId::new(self.1);
        self.1 += 1;
        self.0.insert(id, pool).unwrap();
        id
    }
}

/// `Values(k, l0, l1, ...)` at `node`, every list cell a constant reference.
fn values(
    builder: &mut crate::exec::operators::family_fixture::constant_sources::FragmentBuilder,
    pools: &mut Pools,
    node: NodeId,
    rows: &[Row],
    lists: usize,
) -> (ValueId, Vec<ValueId>) {
    let mut types = vec![int64(false)];
    types.extend((0..lists).map(|_| list_type()));
    let columns = types
        .iter()
        .enumerate()
        .map(|(ordinal, ty)| {
            builder
                .add_value(
                    ty.clone(),
                    ValueOrigin::NodeOutput {
                        node,
                        output_ordinal: u32::try_from(ordinal).unwrap(),
                    },
                )
                .unwrap()
        })
        .collect::<Vec<_>>();
    let pool_ids = (0..lists)
        .map(|list| pools.add(rows.iter().map(|(_, arrays)| arrays[list].clone())))
        .collect::<Vec<_>>();
    let mut cells = Vec::new();
    for (row, (k, _)) in rows.iter().enumerate() {
        let mut cell = vec![builder.constant(
            node,
            int64(false),
            crate::exec::operators::family_fixture::constant_sources::i64(*k),
        )];
        for pool in &pool_ids {
            cell.push(
                builder
                    .unit(node)
                    .unwrap()
                    .add(
                        None,
                        list_type(),
                        ExprKind::Constant(ConstantReference {
                            pool: *pool,
                            ordinal: u32::try_from(row).unwrap(),
                        }),
                    )
                    .unwrap(),
            );
        }
        let cell = cell
            .into_iter()
            .map(|definition| {
                builder
                    .unit(node)
                    .unwrap()
                    .root(definition, EvaluationDemand::Value, RootScope::AllPorts)
                    .unwrap()
            })
            .collect::<Vec<_>>();
        cells.push(cell.into_boxed_slice());
    }
    builder
        .add_values(
            node,
            cells.into_boxed_slice(),
            columns.clone().into_boxed_slice(),
        )
        .unwrap();
    (columns[0], columns[1..].to_vec())
}

/// Add `UNNEST(args)` over `input`, whose key and list columns are given, with
/// the spec's outputs and the output properties the physical law derives.
struct TableSource {
    functions: PureEngineFunctionCatalog,
    selected: Arc<FunctionBindingSelection>,
    arguments: Vec<FunctionArgument>,
}

fn add_table_function(
    builder: &mut crate::exec::operators::family_fixture::constant_sources::FragmentBuilder,
    input: NodeId,
    key: ValueId,
    lists: &[ValueId],
    spec: &Spec,
) -> (NodeId, Vec<ValueId>, TableSource) {
    let node = builder.reserve_node_id().unwrap();
    let request = spec
        .args
        .iter()
        .map(|_| FunctionArgument::Value {
            value_type: list_type(),
            constant: None,
        })
        .collect::<Vec<_>>();
    let functions = unnest_catalog();
    let bound = functions
        .metadata()
        .resolve_bound_user(
            "unnest",
            FunctionKind::Table,
            FunctionBindingRequest {
                arguments: &request,
                logical_argument_count: request.len(),
                expected_result_type: None,
            },
            &FixtureControl,
        )
        .unwrap();
    let FunctionResultType::Relation(results) = &bound.selected.result_type else {
        panic!("UNNEST binds a relation")
    };
    let mut function = BoundTableFunction::from_exact_signature(
        bound.function_id.clone(),
        bound.selected.overload.clone(),
        bound.selected.argument_types.clone(),
        results.clone(),
    );
    function.legacy_metadata = Some(LegacyBindingMetadata {
        volatility: bound.semantics.volatility,
        argument_evaluation: bound.semantics.argument_evaluation,
        failure_behavior: bound.semantics.failure_behavior,
        intrinsic_row_error: bound.semantics.intrinsic_row_error,
        semantic_parameters: Box::default(),
    });
    let arguments = spec
        .args
        .iter()
        .map(|arg| match *arg {
            Arg::List(list) => builder
                .unit(node)
                .unwrap()
                .add(None, list_type(), ExprKind::Value(lists[list]))
                .unwrap(),
            Arg::Faulty(list) => faulty_argument(builder, node, key, lists[list]),
        })
        .collect::<Vec<_>>();
    let arguments = arguments
        .into_iter()
        .map(|definition| {
            builder
                .unit(node)
                .unwrap()
                .root(definition, EvaluationDemand::Value, RootScope::Port(0))
                .unwrap()
        })
        .collect::<Vec<_>>();
    let mut outputs = Vec::new();
    let mut columns = Vec::new();
    for (ordinal, out) in spec.outputs.iter().enumerate() {
        let output = match out {
            Out::Key => TableFunctionOutput::PassThrough(key),
            Out::Result(result) => {
                let mut ty = results[*result as usize].clone();
                ty = ty
                    .with_nullable(ty.nullable() || spec.left_outer)
                    .expect("valid test root nullability");
                let value = builder
                    .add_value(
                        ty,
                        ValueOrigin::NodeOutput {
                            node,
                            output_ordinal: u32::try_from(ordinal).unwrap(),
                        },
                    )
                    .unwrap();
                TableFunctionOutput::FunctionResult {
                    result_ordinal: *result,
                    value,
                }
            }
        };
        columns.push(output.value());
        outputs.push(output);
    }
    // A hash placement survives only when every key passes through; the
    // ordering keeps its pass-through prefix.
    let input_properties = builder.node_output_properties(input).unwrap().clone();
    let passes = |value: &ValueId| outputs.contains(&TableFunctionOutput::PassThrough(*value));
    let distribution = match &input_properties.distribution {
        Distribution::Hash { keys, .. } if !keys.iter().all(passes) => Distribution::Unconstrained,
        distribution => distribution.clone(),
    };
    let ordering = input_properties
        .ordering
        .iter()
        .take_while(|key| passes(&key.value))
        .cloned()
        .collect::<Vec<_>>();
    builder
        .insert_node_unchecked(PhysicalNode {
            expressions: None,
            id: node,
            inputs: Box::from([input]),
            required_inputs: Box::from([passthrough_requirement(&input_properties)]),
            output_properties: PhysicalProperties {
                distribution,
                row_multiplicity: input_properties.row_multiplicity,
                ordering: ordering.into_boxed_slice(),
            },
            output: OutputPort {
                node,
                columns: columns.clone().into_boxed_slice(),
            },
            kind: NodeKind::TableFunction {
                function,
                arguments: arguments.into_boxed_slice(),
                outputs: outputs.into_boxed_slice(),
                left_outer: spec.left_outer,
            },
        })
        .unwrap();
    (
        node,
        columns,
        TableSource {
            functions,
            selected: Arc::new(bound.selected.clone()),
            arguments: request,
        },
    )
}

/// `CASE WHEN k + i64::MAX IS NULL THEN l ELSE l END`: a List argument root
/// whose condition raises an arithmetic overflow for every positive key.
fn faulty_argument(
    builder: &mut crate::exec::operators::family_fixture::constant_sources::FragmentBuilder,
    node: NodeId,
    key: ValueId,
    list: ValueId,
) -> ExprId {
    let operand = int64(false);
    // The prepared arithmetic owner publishes a nullable result.
    let mut sum_type = arithmetic_result_value_type_with_op(
        &operand,
        &operand,
        novarocks_type_contract::ArithmeticOperator::Add,
    )
    .unwrap();
    sum_type = sum_type
        .with_nullable(true)
        .expect("valid test root nullability");
    let left = builder
        .unit(node)
        .unwrap()
        .add(None, operand.clone(), ExprKind::Value(key))
        .unwrap();
    let right = builder.constant(
        node,
        operand,
        crate::exec::operators::family_fixture::constant_sources::i64(i64::MAX),
    );
    let sum = builder
        .unit(node)
        .unwrap()
        .add(
            None,
            sum_type,
            ExprKind::Binary {
                op: BinaryOperator::Add,
                left,
                right,
                decimal_overflow_policy: POLICY,
                allow_throw_exception: Some(ALLOW),
            },
        )
        .unwrap();
    let condition = builder
        .unit(node)
        .unwrap()
        .add(
            None,
            FunctionValueType::new(DataType::Boolean, false),
            ExprKind::IsNull {
                expr: sum,
                negated: false,
            },
        )
        .unwrap();
    let then = builder
        .unit(node)
        .unwrap()
        .add(None, list_type(), ExprKind::Value(list))
        .unwrap();
    let otherwise = builder
        .unit(node)
        .unwrap()
        .add(None, list_type(), ExprKind::Value(list))
        .unwrap();
    builder
        .unit(node)
        .unwrap()
        .add(
            None,
            list_type(),
            ExprKind::Case {
                operand: None,
                when_then: Box::from([(condition, then)]),
                else_expr: Some(otherwise),
            },
        )
        .unwrap()
}

/// Finish the original scalar units before authoring relation requests/effects.
fn finish(
    mut builder: crate::exec::operators::family_fixture::constant_sources::FragmentBuilder,
    root: NodeId,
    sink: FragmentSink,
    max_dop: u32,
    sources: BTreeMap<NodeId, TableSource>,
    source_pools: &ConstantPools,
    parameters: &SemanticParameters,
) -> crate::exec::operators::family_fixture::constant_sources::Fragment {
    super::super::family_fixture::seal_no_call_units(&mut builder);
    let fragment = builder
        .finish_structure(
            root,
            sink,
            PipelineDopDomain {
                min: 1,
                max: max_dop,
                requires_power_of_two: false,
            },
            PlanLimits::FROZEN,
            &FixtureControl,
            novarocks_physical_plan::StagePlacementPolicy::Singleton,
        )
        .unwrap();
    let (fragment, constants) = fragment.into_parts();
    let mut evaluation_pools = source_pools.clone();
    super::super::family_fixture::constant_sources::merge(&mut evaluation_pools, &constants);
    let entries = sources
        .iter()
        .map(|(&node, source)| {
            (
                PhysicalCallSite::Table { node },
                PhysicalCallRequest {
                    arguments: source
                        .arguments
                        .iter()
                        .map(|argument| match argument {
                            FunctionArgument::Value {
                                value_type,
                                constant: None,
                            } => StaticFunctionArgument::Value {
                                value_type: value_type.clone(),
                                constant: None,
                            },
                            _ => panic!("UNNEST original requests are nonconstant Values"),
                        })
                        .collect(),
                    logical_argument_count: source.arguments.len(),
                    expected_result_type: None,
                    constant_policy: constant_policy(),
                },
            )
        })
        .collect();
    let fragment = fragment
        .with_call_requests_observed(entries, &FixtureControl)
        .unwrap();
    let mut frozen = Vec::new();
    for (ordinal, (&node, source)) in sources.iter().enumerate() {
        let NodeKind::TableFunction {
            function,
            arguments,
            ..
        } = &fragment.nodes()[&node].kind
        else {
            panic!("actual UNNEST node")
        };
        let unit = fragment.unit(node).unwrap();
        let input = &fragment.nodes()[&fragment.nodes()[&node].inputs[0]]
            .output
            .columns;
        // Keep the complete physical input order. Canonical value reads select
        // their first actual publication; repeated ordinals retain full facts.
        let ordinal_types = input
            .iter()
            .map(|value| fragment.values()[value].ty.clone())
            .collect::<Vec<_>>();
        let schema = Arc::new(arrow::datatypes::Schema::new(
            ordinal_types
                .iter()
                .enumerate()
                .map(|(ordinal, ty)| ty.try_to_field(format!("input_{ordinal}")).unwrap())
                .collect::<Vec<_>>(),
        ));
        let mut work =
            CompileCheckpoints::try_new(&FixtureControl, CompilePhase::LowerProgram).unwrap();
        assert_eq!(unit.ports().len(), 1);
        let scope = ScopeLayout::try_new(
            schema,
            ordinal_types.into(),
            unit.ports()[0]
                .values
                .iter()
                .map(|(value, ty)| ScopeColumn {
                    value: *value,
                    ordinal: u32::try_from(
                        input.iter().position(|actual| actual == value).unwrap(),
                    )
                    .unwrap(),
                    ty: ty.clone(),
                })
                .collect(),
            ValueTypeStorageLifetime::pure(),
            &mut work,
        )
        .unwrap();
        let compiled = compile_expr_set_pure(
            unit,
            EvaluationEnvironment {
                constants: &evaluation_pools,
                parameters,
            },
            ScopeLayouts {
                ports: &[scope],
                all_ports: None,
            },
            constant_policy(),
            &source.functions,
            &mut work,
        )
        .unwrap();
        work.finish().unwrap();
        let context =
            RelationEffectContext::call(RelationCallId::new(u32::try_from(ordinal).unwrap()));
        let mut children = ExpressionEffects::PURE_VALUE;
        let argument_uses = arguments
            .iter()
            .map(|root| {
                let root = compiled.root(*root).unwrap();
                let invocation = &compiled.control_flow().uses()[&root.use_id];
                children = children.join(
                    compiled.effects()[&root.use_id]
                        .for_use(invocation.context)
                        .unwrap(),
                );
                Some(root.use_id)
            })
            .collect::<Vec<_>>();
        let token = source
            .functions
            .prepare_fresh(
                CallEffectInput {
                    context: context.into(),
                    argument_uses: novarocks_functions::CallArgumentUses::SelectedChannels(
                        &argument_uses,
                    ),
                    function_id: &function.function_id,
                    kind: FunctionKind::Table,
                    selected: source.selected.as_ref(),
                    request: FunctionBindingRequest {
                        arguments: &source.arguments,
                        logical_argument_count: source.arguments.len(),
                        expected_result_type: None,
                    },
                    environment: &[],
                    parameters,
                    decimal_overflow_policy: POLICY,
                    proof_scope: CallProofScope::Relation(context.identity),
                },
                source.selected.clone(),
                PureCallPreparation::Table {
                    arguments: ScopedRelationalEffects::primitive(context, children),
                },
                &FixtureControl,
            )
            .unwrap_or_else(|error| panic!("fixture table call prepares: {error}"));
        frozen.push(FrozenPhysicalCall {
            materialized_writer_input: None,
            site: PhysicalCallSite::Table { node },
            context,
            effects: token.call_contract().effects().clone(),
            decimal_overflow_policy: POLICY,
        });
    }
    let fragment = fragment
        .with_calls_observed(frozen, &FixtureControl)
        .unwrap();
    crate::exec::operators::family_fixture::constant_sources::Fragment::from_parts(
        fragment, constants,
    )
}

// Explicit small-fixture admission; these are test inputs, not defaults.
fn admission() -> FragmentPackageAdmission {
    FragmentPackageAdmission {
        plan_limits: PlanLimits::FROZEN,
        source_retained_bytes: 64 * 1024 * 1024,
        property_projection_limits: PropertyProofProjectionLimits {
            max_request_bytes: 16 * 1024 * 1024,
            max_coexisting_bytes: 256 * 1024 * 1024,
            max_projection_work: 16 * 1024 * 1024,
        },
    }
}

/// The authored parameter table: ALLOW only when a faulty argument cites it.
fn parameters(spec: &Spec) -> SemanticParameters {
    if spec.args.iter().any(|arg| matches!(arg, Arg::Faulty(_))) {
        SemanticParameters::try_new([(ALLOW.id, SemanticParameterValue::AllowThrowException(true))])
            .unwrap()
    } else {
        SemanticParameters::try_new([]).unwrap()
    }
}

/// Every fragment of `plan` as a checked package with its frozen calls.
fn packages(plan: &PhysicalPlan) -> BTreeMap<FragmentId, FragmentPackage> {
    let mut pruning = BTreeMap::new();
    let mut admissions = BTreeMap::new();
    for id in plan.fragments().keys() {
        pruning.insert(
            *id,
            FrozenFragmentPruning::try_new(*id, vec![], &FixtureControl).unwrap(),
        );
        admissions.insert(*id, admission());
    }
    extract_fragment_packages(
        plan,
        &BTreeMap::new(),
        &BTreeMap::new(),
        &pruning,
        &admissions,
        &FixtureControl,
    )
    .unwrap_or_else(|error| panic!("table function packages extract: {error:?}"))
}

fn result_port(fragment: &Fragment, root: NodeId) -> ResultPort {
    let output = fragment.nodes()[&root].output.clone();
    ResultPort {
        scalar_schema: None,
        fragment: fragment.id(),
        fields: output
            .columns
            .iter()
            .enumerate()
            .map(|(ordinal, value)| ResultField {
                domain: crate::test_result_domain::result_value_domain(
                    &fragment.values()[value].ty,
                ),
                name: format!("c{ordinal}").into_boxed_str(),
                alias: None,
                value: *value,
                ty: fragment.values()[value].ty.clone(),
            })
            .collect::<Vec<_>>()
            .into_boxed_slice(),
        output,
    }
}

fn version() -> PlanVersionId {
    PlanVersionId::try_new([71; 16]).unwrap()
}

/// `Values -> [global Sort by k DESC] -> UNNEST -> Result` in one fragment.
fn single_program(rows: &[Row], spec: &Spec, sorted: bool) -> Result<Arc<LocalProgram>, String> {
    let fixture_sources = crate::exec::operators::family_fixture::constant_sources::Sources::new();

    let mut pools = Pools::new();
    let mut builder =
        crate::exec::operators::family_fixture::constant_sources::FragmentBuilder::new(
            SOLE,
            &fixture_sources,
        );
    let values_node = builder.reserve_node_id().unwrap();
    let (key, lists) = values(&mut builder, &mut pools, values_node, rows, spec.lists);
    let input = if sorted {
        let sort = builder.reserve_node_id().unwrap();
        let expr = builder
            .unit(sort)
            .unwrap()
            .add(None, int64(false), ExprKind::Value(key))
            .unwrap();
        let expr = builder
            .unit(sort)
            .unwrap()
            .root(expr, EvaluationDemand::Value, RootScope::Port(0))
            .unwrap();
        builder
            .add_sort(
                sort,
                values_node,
                Box::from([SortExpr {
                    expr,
                    direction: SortDirection::Descending,
                    null_ordering: NullOrdering::Last,
                }]),
                SortMode::Global,
            )
            .unwrap();
        sort
    } else {
        values_node
    };
    let (table, _, source_call) = add_table_function(&mut builder, input, key, &lists, spec);
    let actual_parameters = parameters(spec);
    let fragment = finish(
        builder,
        table,
        FragmentSink::Result,
        1,
        BTreeMap::from([(table, source_call)]),
        &pools.0,
        &actual_parameters,
    );
    let port = result_port(&fragment, table);
    let mut plan = crate::exec::operators::family_fixture::constant_sources::PlanBuilder::new(
        version(),
        &fixture_sources,
    )
    .with_constant_pools(pools.0)
    .with_semantic_parameters(actual_parameters);
    plan.add_fragment(fragment).unwrap();
    plan.set_result_port(port).unwrap();
    let plan = plan
        .finish_observed(&FixtureControl)
        .unwrap_or_else(|error| panic!("the table function plan validates: {error:?}"));
    let package = packages(&plan).remove(&SOLE).unwrap();
    try_compile(package, &unnest_catalog(), 1, true)
}

/// Prepare and run the program with an explicit chunk size; a preparation
/// or execution failure retains its typed source.
fn try_run_single(
    program: &Arc<LocalProgram>,
    batch_size: i32,
) -> crate::runtime::task::ExecutionResult<Vec<Chunk>> {
    let state = Arc::new(RuntimeState::for_local_execution(
        Some(QueryOptions {
            batch_size: Some(batch_size),
            ..QueryOptions::default()
        }),
        None,
        None,
        None,
        None,
        Some(crate::runtime::mem_tracker::MemTracker::new_root(
            "compiled table function tests",
        )),
        Some(crate::runtime::execution_runtime::test_execution_runtime()),
    ));
    let output = ResultSinkBuffer::new();
    let dop = i32::try_from(program.graph().profile().pipeline_dop().get()).unwrap();
    let prepared = prepare_compiled_program_pipeline_execution(
        Arc::clone(program),
        Duration::from_millis(10),
        Box::new(ResultSinkFactory::new(output.clone())),
        ExchangeBindings::default(),
        None,
        dop,
        state,
        Arc::new(NoopTaskEventSink),
    )?;
    prepared.start().join()?;
    Ok(output.take_chunks())
}

fn run_single(rows: &[Row], spec: &Spec, sorted: bool, batch_size: i32) -> Vec<Chunk> {
    let program = single_program(rows, spec, sorted)
        .unwrap_or_else(|error| panic!("table function fragment compiles: {error}"));
    try_run_single(&program, batch_size)
        .unwrap_or_else(|error| panic!("table function fragment runs: {error}"))
}

/// The longest-zip oracle: for each row in order, one output per array
/// position up to its longest array (shorter and NULL arrays pad NULL), or
/// one NULL-extended row for a LEFT OUTER row whose arrays are all NULL or
/// empty. Each output is `(k, results...)` in spec order.
fn oracle(rows: &[Row], spec: &Spec) -> Vec<Vec<Option<i64>>> {
    let mut output = Vec::new();
    for (k, arrays) in rows {
        let arguments = spec
            .args
            .iter()
            .map(|arg| match arg {
                Arg::List(list) | Arg::Faulty(list) => arrays[*list].clone().unwrap_or_default(),
            })
            .collect::<Vec<_>>();
        let longest = arguments.iter().map(Vec::len).max().unwrap_or(0);
        let positions = if longest == 0 && spec.left_outer {
            vec![None]
        } else {
            (0..longest).map(Some).collect()
        };
        for position in positions {
            output.push(
                spec.outputs
                    .iter()
                    .map(|out| match out {
                        Out::Key => Some(*k),
                        Out::Result(result) => position.and_then(|position| {
                            arguments[*result as usize].get(position).copied().flatten()
                        }),
                    })
                    .collect(),
            );
        }
    }
    output
}

fn rows() -> Vec<Row> {
    vec![
        (1, vec![Some(vec![Some(10), Some(11), Some(12)])]),
        (2, vec![None]),
        (3, vec![Some(vec![])]),
        (4, vec![Some(vec![Some(40), None])]),
    ]
}

#[test]
fn compiled_table_function_lowers_to_an_argument_project_feeding_a_table_cursor() {
    let program = single_program(&rows(), &Spec::unnest(false), false).unwrap();
    let nodes = program.graph().nodes();
    let root = program.graph().root();
    let ProgramNodeKind::TableFunction { input, .. } = nodes[root.index()].kind() else {
        panic!("the table function is the root");
    };
    assert!(matches!(
        nodes[input.index()].kind(),
        ProgramNodeKind::Project {
            is_subordinate: true,
            ..
        }
    ));
}

#[test]
fn inner_unnest_expands_each_row_in_order_and_drops_null_and_empty_arrays() {
    let spec = Spec::unnest(false);
    let output = int64_rows(&run_single(&rows(), &spec, false, 4096));
    assert_eq!(
        output,
        vec![
            vec![Some(1), Some(10)],
            vec![Some(1), Some(11)],
            vec![Some(1), Some(12)],
            vec![Some(4), Some(40)],
            vec![Some(4), None],
        ]
    );
    assert_eq!(output, oracle(&rows(), &spec));
}

#[test]
fn left_outer_unnest_null_extends_null_and_empty_arrays_in_row_order() {
    let spec = Spec {
        outputs: vec![Out::Result(0), Out::Key],
        ..Spec::unnest(true)
    };
    let output = int64_rows(&run_single(&rows(), &spec, false, 4096));
    assert_eq!(
        output,
        vec![
            vec![Some(10), Some(1)],
            vec![Some(11), Some(1)],
            vec![Some(12), Some(1)],
            vec![None, Some(2)],
            vec![None, Some(3)],
            vec![Some(40), Some(4)],
            vec![None, Some(4)],
        ]
    );
    assert_eq!(output, oracle(&rows(), &spec));
}

#[test]
fn several_arguments_zip_to_the_longest_array_for_inner_and_left_outer() {
    let rows = vec![
        (
            1,
            vec![
                Some(vec![Some(1), Some(2), Some(3)]),
                Some(vec![Some(10), Some(20)]),
            ],
        ),
        (2, vec![None, Some(vec![Some(30)])]),
        (3, vec![Some(vec![]), Some(vec![])]),
        (4, vec![Some(vec![Some(4)]), None]),
        (5, vec![None, None]),
    ];
    for left_outer in [false, true] {
        let spec = Spec {
            lists: 2,
            args: vec![Arg::List(0), Arg::List(1)],
            outputs: vec![Out::Key, Out::Result(0), Out::Result(1)],
            left_outer,
        };
        let output = int64_rows(&run_single(&rows, &spec, false, 4096));
        assert_eq!(output, oracle(&rows, &spec), "left_outer={left_outer}");
        assert_eq!(output.len(), if left_outer { 7 } else { 5 });
    }
}

#[test]
fn a_parent_larger_than_one_chunk_spans_bounded_chunks_in_order() {
    let rows = vec![
        (1, vec![Some((1..=7).map(Some).collect())]),
        (2, vec![Some(vec![Some(8)])]),
        (3, vec![None]),
    ];
    let spec = Spec::unnest(true);
    let chunks = run_single(&rows, &spec, false, 2);
    assert!(chunks.iter().all(|chunk| chunk.len() <= 2));
    assert_eq!(chunks.len(), 5, "nine rows in chunks of at most two");
    assert_eq!(int64_rows(&chunks), oracle(&rows, &spec));
}

#[test]
fn unnest_over_a_global_sort_keeps_the_pass_through_ordering() {
    let spec = Spec::unnest(true);
    let program = single_program(&rows(), &spec, true).unwrap();
    let root = program.graph().root();
    let ProgramNodeKind::TableFunction { input, .. } = program.graph().nodes()[root.index()].kind()
    else {
        panic!("the table function is the root");
    };
    let ProgramNodeKind::Project { input, .. } = program.graph().nodes()[input.index()].kind()
    else {
        panic!("argument Project");
    };
    assert!(matches!(
        program.graph().nodes()[input.index()].kind(),
        ProgramNodeKind::Sort { .. }
    ));
    let output = int64_rows(&try_run_single(&program, 3).unwrap());
    let mut sorted = rows();
    sorted.reverse();
    assert_eq!(output, oracle(&sorted, &spec));
}

/// A guarded List argument is evaluated by its argument Project. Its CASE
/// condition overflows on the first input row, and that required row error
/// keeps the original expression root, selected row and pipeline operation.
#[test]
fn a_guarded_list_argument_preserves_its_required_project_row_error() {
    use crate::runtime::task::{ExecutionFailureCause, OperatorFailureSite, PipelineOperation};
    let spec = Spec {
        args: vec![Arg::Faulty(0)],
        ..Spec::unnest(false)
    };
    let program = single_program(&rows(), &spec, false).unwrap();
    let root = program.graph().root();
    let ProgramNodeKind::TableFunction { input, .. } = program.graph().nodes()[root.index()].kind()
    else {
        panic!("the table function is the root");
    };
    let ProgramNodeKind::Project { exprs, .. } = program.graph().nodes()[input.index()].kind()
    else {
        panic!("argument Project");
    };
    assert_eq!(exprs.len(), 2, "the key read and the guarded argument root");
    let error = try_run_single(&program, 4096).unwrap_err();
    assert_eq!(
        error.site(),
        Some(OperatorFailureSite {
            operator_ordinal: 1,
            operation: PipelineOperation::Push,
        }),
        "{error:?}"
    );
    assert_eq!(error.expression_node(), Some(*input));
    let ExecutionFailureCause::RequiredRow(required) = error.cause() else {
        panic!("the argument Project must preserve its required row error: {error:?}");
    };
    assert_eq!(required.root(), exprs[1].root);
    assert_eq!(required.batch_row(), 0);
    assert_eq!(required.error().selected_ordinal(), 0);
    assert_eq!(
        required.error().message(),
        "Arithmetic overflow: Overflow happened on: 1 + 9223372036854775807"
    );
}

/// `UnionAll(Values_b) -> Stream(Hash[k])` | `ExchangeSource -> UNNEST ->
/// Result` at `dop` drivers: each union branch arrives as its own chunk.
fn exchanged_programs(
    branches: &[Vec<Row>],
    spec: &Spec,
    dop: usize,
) -> (Arc<LocalProgram>, Arc<LocalProgram>) {
    let fixture_sources = crate::exec::operators::family_fixture::constant_sources::Sources::new();

    let mut pools = Pools::new();
    let mut source = crate::exec::operators::family_fixture::constant_sources::FragmentBuilder::new(
        SOURCE,
        &fixture_sources,
    );
    let union = source.reserve_node_id().unwrap();
    let mut inputs = Vec::new();
    let mut mappings = Vec::new();
    for rows in branches {
        let node = source.reserve_node_id().unwrap();
        let (key, lists) = values(&mut source, &mut pools, node, rows, spec.lists);
        let mut mapping = vec![key];
        mapping.extend(lists);
        inputs.push(node);
        mappings.push(mapping.into_boxed_slice());
    }
    let mut types = vec![int64(false)];
    types.extend((0..spec.lists).map(|_| list_type()));
    let columns = types
        .iter()
        .enumerate()
        .map(|(ordinal, ty)| {
            source
                .add_value(
                    ty.clone(),
                    ValueOrigin::NodeOutput {
                        node: union,
                        output_ordinal: u32::try_from(ordinal).unwrap(),
                    },
                )
                .unwrap()
        })
        .collect::<Vec<_>>();
    source
        .add_row_consuming(
            union,
            inputs.into_boxed_slice(),
            RequiredInputs::Singleton,
            Distribution::Singleton,
            columns.clone().into_boxed_slice(),
            NodeKind::SetOp {
                kind: SetOperationKind::UnionAll,
                input_mappings: mappings.into_boxed_slice(),
            },
        )
        .unwrap();
    let actual_parameters = parameters(spec);
    let source = finish(
        source,
        union,
        FragmentSink::Stream { edge: TO_CONSUMER },
        1,
        BTreeMap::new(),
        &pools.0,
        &actual_parameters,
    );

    let mut consumer =
        crate::exec::operators::family_fixture::constant_sources::FragmentBuilder::new(
            CONSUMER,
            &fixture_sources,
        );
    let receiver = consumer.reserve_node_id().unwrap();
    let imports = receive(
        &mut consumer,
        receiver,
        TO_CONSUMER,
        &columns
            .iter()
            .zip(&types)
            .map(|(value, ty)| (*value, ty.clone()))
            .collect::<Vec<_>>(),
        |imports| hash(&imports[..1]),
    );
    let (table, _, source_call) =
        add_table_function(&mut consumer, receiver, imports[0], &imports[1..], spec);
    let consumer = finish(
        consumer,
        table,
        FragmentSink::Result,
        u32::try_from(dop).unwrap(),
        BTreeMap::from([(table, source_call)]),
        &pools.0,
        &actual_parameters,
    );
    let port = result_port(&consumer, table);

    let mut plan = crate::exec::operators::family_fixture::constant_sources::PlanBuilder::new(
        version(),
        &fixture_sources,
    )
    .with_constant_pools(pools.0)
    .with_semantic_parameters(actual_parameters);
    plan.add_fragment(source).unwrap();
    plan.add_fragment(consumer).unwrap();
    plan.add_edge(edge(
        TO_CONSUMER,
        (SOURCE, &columns),
        (CONSUMER, receiver, &imports),
        hash(&columns[..1]),
        hash(&imports[..1]),
    ))
    .unwrap();
    plan.set_result_port(port).unwrap();
    let plan = plan
        .finish_observed(&FixtureControl)
        .unwrap_or_else(|error| panic!("the exchanged table function plan validates: {error:?}"));
    let mut packages = packages(&plan);
    let catalog = unnest_catalog();
    let source = try_compile(packages.remove(&SOURCE).unwrap(), &catalog, 1, false)
        .unwrap_or_else(|error| panic!("source compiles: {error}"));
    let consumer = try_compile(packages.remove(&CONSUMER).unwrap(), &catalog, dop, true)
        .unwrap_or_else(|error| panic!("consumer compiles: {error}"));
    (source, consumer)
}

#[test]
fn unnest_at_four_drivers_keeps_every_row_and_each_parent_in_order() {
    const DOP: usize = 4;
    let branches = (0..6)
        .map(|branch| {
            (0..3)
                .map(|row| {
                    let k = branch * 10 + row;
                    let array = match row {
                        0 => Some((0..k % 5 + 3).map(|v| Some(k * 100 + v)).collect()),
                        1 => None,
                        _ => Some(vec![]),
                    };
                    (k, vec![array])
                })
                .collect::<Vec<Row>>()
        })
        .collect::<Vec<_>>();
    let spec = Spec::unnest(true);
    let (source, consumer) = exchanged_programs(&branches, &spec, DOP);
    assert_eq!(consumer.graph().profile().pipeline_dop().get(), DOP);

    let fixture = Fixture::new();
    let port = fixture.port.clone();
    let transmitter = LoopbackTransmitter::new(fixture.transport.clone());
    let bindings = register(&fixture, &consumer, CONSUMER_FINST, &[SOURCE_FINST], &port);
    let sink = stream_sink(
        &fixture,
        &source,
        SOURCE_FINST,
        vec![destination(CONSUMER_FINST, SOURCE_FINST, 0, 1)],
        &transmitter,
    );
    try_run_in_fixture(
        &fixture,
        &source,
        sink,
        ExchangeBindings::default(),
        SOURCE_FINST,
    )
    .unwrap_or_else(|error| panic!("source runs: {error}"));
    let output = ResultSinkBuffer::new();
    try_run_in_fixture(
        &fixture,
        &consumer,
        result_sink(&consumer, CONSUMER_FINST, &output),
        bindings,
        CONSUMER_FINST,
    )
    .unwrap_or_else(|error| panic!("consumer runs: {error}"));
    let rows = int64_rows(&output.take_chunks());
    let all = branches.concat();
    let expected = oracle(&all, &spec);
    // Drivers interleave parents; each parent keeps its own row order.
    let mut actual_sorted = rows.clone();
    let mut expected_sorted = expected.clone();
    actual_sorted.sort();
    expected_sorted.sort();
    assert_eq!(actual_sorted, expected_sorted);
    for (k, _) in &all {
        let of = |rows: &[Vec<Option<i64>>]| {
            rows.iter()
                .filter(|row| row[0] == Some(*k))
                .cloned()
                .collect::<Vec<_>>()
        };
        assert_eq!(of(&rows), of(&expected), "parent {k}");
    }
}

#[path = "compiled_all_definition_source_tests.rs"]
mod all_definition_source_tests;

#[test]
fn compiled_table_schema_prepare_and_output_last_reference_use_real_owner() {
    use crate::exec::chunk::ChunkSchema;
    use crate::exec::operators::compiled_table_function::CompiledTableFunctionProcessorFactory;
    use crate::exec::pipeline::operator::ConstructionFailure;
    use crate::exec::pipeline::operator_factory::OperatorFactory;
    use crate::runtime::mem_tracker::MemTracker;
    use crate::runtime::runtime_state::RuntimeErrorState;
    use crate::runtime::task::ExecutionFailureCause;
    use arrow::array::builder::{Int64Builder, ListBuilder};
    use arrow::array::{ArrayRef, Int64Array};
    let program = single_program(&rows(), &Spec::unnest(false), false).unwrap();
    let weak = Arc::downgrade(&program);
    let root = program.graph().root();
    let factory = CompiledTableFunctionProcessorFactory::try_new(
        program.clone(),
        root,
        Arc::new(RuntimeErrorState::default()),
    )
    .unwrap();
    let mut missing = factory.create(1, 0);
    assert!(matches!(
        missing.prepare().unwrap_err().cause(),
        ExecutionFailureCause::Construction(ConstructionFailure::MissingOwner)
    ));
    drop(missing);
    let tracker = MemTracker::new_root("table actual schema and output creator");
    let mut operator = factory.create(1, 0);
    operator.set_construction_factory(super::super::aggregate_fixture::runtime_construction_factory(
        tracker.clone(),
        Arc::new(RuntimeErrorState::default()),
    ));
    operator.prepare().unwrap();
    let ProgramNodeKind::TableFunction { input, .. } = program.graph().nodes()[root.index()].kind()
    else {
        panic!("table root");
    };
    let layout = program.graph().nodes()[input.index()].output_layout();
    // Actual input author: one key and one canonical List<Int64> channel.
    let columns = layout
        .schema()
        .fields()
        .iter()
        .map(|field| match field.data_type() {
            DataType::Int64 => Arc::new(Int64Array::from(vec![7])) as ArrayRef,
            DataType::List(_) => {
                let mut values = ListBuilder::new(Int64Builder::new());
                values.values().append_value(10);
                values.values().append_value(20);
                values.append(true);
                Arc::new(values.finish()) as ArrayRef
            }
            ty => panic!("unexpected actual UNNEST input {ty:?}"),
        })
        .collect();
    let input = Chunk::try_new_with_chunk_schema(
        arrow::record_batch::RecordBatch::try_new(layout.schema().clone(), columns).unwrap(),
        ChunkSchema::from_compiled_layout(layout).unwrap(),
    )
    .unwrap();
    drop(factory);
    drop(program);
    assert!(weak.upgrade().is_none());
    use crate::exec::pipeline::{
        driver_step::{DriverStep, TurnWork},
        input::RawInput,
        step::{InputResult, NextStep},
    };
    let state = RuntimeState::for_local_execution(
        Some(QueryOptions {
            batch_size: Some(1),
            ..QueryOptions::default()
        }),
        None,
        None,
        None,
        None,
        None,
        None,
    );
    let control = state.error_state();
    operator.bind_runtime_state(&state).unwrap();
    let duplicate = input.clone();
    let original = duplicate.columns()[0].clone();
    let mut work = TurnWork::new(std::num::NonZeroU64::MIN, 64);
    let mut value_conversions = novarocks_value_arrays::decode::ValueConversionContext::default();
    let mut ctx = DriverStep::new(&control, &mut work, &mut value_conversions);
    let step = operator.as_mut();
    assert!(matches!(
        step.try_push(&mut ctx, RawInput::for_test(input)),
        InputResult::Accepted(Ok(_))
    ));
    let installed = step.drive(&mut ctx).unwrap();
    assert!(installed.output.is_none());
    assert_eq!(installed.committed_rows, 1);
    let InputResult::Deferred {
        input: returned, ..
    } = step.try_push(&mut ctx, RawInput::for_test(duplicate))
    else {
        panic!("the current parent relation still owns the receiver");
    };
    assert!(Arc::ptr_eq(&original, &returned.original().columns()[0]));
    step.finish(&mut ctx).unwrap();
    let mut outputs = Vec::new();
    for _ in 0..2 {
        let result = step.drive(&mut ctx).unwrap();
        assert!(matches!(result.next, NextStep::Runnable));
        let output = result.output.expect("one granted output page");
        assert_eq!(output.len(), 1);
        outputs.push(output);
    }
    let ended = step.drive(&mut ctx).unwrap();
    assert!(ended.output.is_none());
    assert!(matches!(ended.next, NextStep::Finished));
    assert!(step.drive(&mut ctx).unwrap().output.is_none());
    assert_eq!(
        int64_rows(&outputs),
        vec![vec![Some(7), Some(10)], vec![Some(7), Some(20)]]
    );
    let schema = outputs[0].chunk_schema_ref();
    drop(returned);
    drop(original);
    assert!(schema.is_typed());
    assert!(
        schema
            .slots()
            .iter()
            .all(|slot| slot.field_ref().metadata().as_arc().is_none())
    );
    drop(operator);
    drop(outputs);
    assert!(tracker.current() > 0);
    drop(schema);
    assert_eq!(tracker.current(), 0);
}

#[test]
fn compiled_table_prepare_preserves_real_memory_refusal_before_input() {
    use crate::exec::operators::compiled_table_function::CompiledTableFunctionProcessorFactory;
    use crate::exec::pipeline::operator::ConstructionFailure;
    use crate::exec::pipeline::operator_factory::OperatorFactory;
    use crate::runtime::mem_tracker::MemTracker;
    use crate::runtime::runtime_state::RuntimeErrorState;
    use crate::runtime::task::ExecutionFailureCause;
    let program = single_program(&rows(), &Spec::unnest(false), false).unwrap();
    let factory = CompiledTableFunctionProcessorFactory::try_new(
        program.clone(),
        program.graph().root(),
        Arc::new(RuntimeErrorState::default()),
    )
    .unwrap();
    let tracker = MemTracker::new_root("table actual prepare refusal");
    let mut operator = factory.create(1, 0);
    operator.set_construction_factory(super::super::aggregate_fixture::runtime_construction_factory(
        tracker.clone(),
        Arc::new(RuntimeErrorState::default()),
    ));
    let baseline = tracker.current();
    assert!(baseline > 0);
    tracker.install_limit_once(baseline + 1).unwrap();
    let first = operator.prepare().unwrap_err();
    assert!(matches!(
        first.cause(),
        ExecutionFailureCause::Construction(ConstructionFailure::Memory(_))
    ));
    assert_eq!(operator.prepare().unwrap_err(), first);
    assert!(
        operator
            .drive_for_test(&crate::runtime::runtime_state::RuntimeState::default())
            .unwrap_err()
            .same_occurrence(&first)
    );
    assert_eq!(tracker.current(), baseline);
    drop(operator);
    assert_eq!(tracker.current(), 0);
}
