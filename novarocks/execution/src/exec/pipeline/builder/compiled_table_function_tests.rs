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
use novarocks_functions::{
    CallEffectInput, ConstantPool, EngineFunctionCatalogBuilder, FunctionArgument,
    FunctionBindingRequest, FunctionBindingSelection, FunctionId, FunctionKind, FunctionOverloadId,
    FunctionResultType, InstalledPureKernel, PureCallPreparation, PureEngineFunctionCatalog,
    PureImplementationDeclaration, PureImplementationId, PureKernelAbi, ScopedExpressionEffects,
};
use novarocks_local_program::{LocalProgram, ProgramNodeKind};
use novarocks_physical_plan::{
    BinaryOperator, BoundTableFunction, ConstantPoolId, ConstantPools, ConstantReference,
    Distribution, EdgeId, ExprId, ExprKind, ExpressionRootRole, ExpressionRootSite, Fragment,
    FragmentBuilder, FragmentId, FragmentPackage, FragmentPackageAdmission, FragmentSink,
    FrozenCallError, FrozenFragmentCalls, FrozenFragmentPruning, FrozenPhysicalCall,
    FunctionArgumentType, LegacyBindingMetadata, LiteralValue, NodeId, NodeKind, NullOrdering,
    OutputPort, PhysicalCallBinding, PhysicalCallDefinition, PhysicalCallRequest, PhysicalCallSite,
    PhysicalExpressionRoots, PhysicalNode, PhysicalPlan, PhysicalProperties, PhysicalRootUses,
    PipelineDopDomain, PlanBuilder, PlanLimits, PlanVersionId, PropertyProofProjectionLimits,
    RequiredInputs, ResultField, ResultPort, SetOperationKind, SortDirection, SortExpr, SortMode,
    StaticFunctionArgument, TableFunctionOutput, ValueId, ValueOrigin, extract_fragment_packages,
    passthrough_requirement,
};
use novarocks_type_contract::{
    CallProofScope, CompileCheckpoints, CompilePhase, DecimalOverflowPolicy, DomainGuard,
    EvaluationDemand, EvaluationDomainId, ExpressionControlFlow, ExpressionEffectContext,
    ExpressionEffects, ExpressionEvaluationDomain, ExpressionInvocation, ExpressionUseId,
    FunctionValueType, SemanticParameterId, SemanticParameterKey, SemanticParameterRef,
    SemanticParameterValue, SemanticParameters, arithmetic_result_value_type_with_op,
    control_argument_semantics,
};
use novarocks_types::UniqueId;

use super::aggregate_fixture::{
    LoopbackTransmitter, destination, edge, hash, receive, register, result_sink, stream_sink,
    try_compile, try_run,
};
use super::family_fixture::{FixtureControl, constant_policy, int64, int64_rows};
use crate::exec::chunk::Chunk;
use crate::exec::operators::{ResultSinkFactory, ResultSinkHandle};
use crate::exec::pipeline::binding::ExchangeBindings;
use crate::exec::pipeline::executor::prepare_compiled_program_pipeline_execution;
use crate::runtime::fragment::io::NoopFragmentEventSink;
use crate::runtime::fragment::io::exchange::in_process_test_exchange_receiver_port;
use crate::runtime::query_options::QueryOptions;
use crate::runtime::runtime_state::RuntimeState;

const SOLE: FragmentId = FragmentId::new(70);
const SOURCE: FragmentId = FragmentId::new(71);
const CONSUMER: FragmentId = FragmentId::new(72);
const TO_CONSUMER: EdgeId = EdgeId::new(9);
const SOURCE_FINST: UniqueId = UniqueId::new(0xa1, 0x01);
const CONSUMER_FINST: UniqueId = UniqueId::new(0xa2, 0x01);
/// Relational use IDs and domains start far above every expression use.
const RELATIONAL_USE: u32 = 1_000_000;
const RELATIONAL_DOMAIN: u32 = 500_000;
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
    builder: &mut FragmentBuilder,
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
        let mut cell = vec![
            builder
                .add_expression(
                    node,
                    int64(false),
                    ExprKind::Literal(LiteralValue::Int64(*k)),
                )
                .unwrap(),
        ];
        for pool in &pool_ids {
            cell.push(
                builder
                    .add_expression(
                        node,
                        list_type(),
                        ExprKind::Constant(ConstantReference {
                            pool: *pool,
                            ordinal: u32::try_from(row).unwrap(),
                        }),
                    )
                    .unwrap(),
            );
        }
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
fn add_table_function(
    builder: &mut FragmentBuilder,
    input: NodeId,
    key: ValueId,
    lists: &[ValueId],
    spec: &Spec,
) -> (NodeId, Vec<ValueId>) {
    let node = builder.reserve_node_id().unwrap();
    let request = spec
        .args
        .iter()
        .map(|_| FunctionArgument::Value {
            value_type: list_type(),
            constant: None,
        })
        .collect::<Vec<_>>();
    let bound = unnest_catalog()
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
                .add_expression(node, list_type(), ExprKind::Value(lists[list]))
                .unwrap(),
            Arg::Faulty(list) => faulty_argument(builder, node, key, lists[list]),
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
    (node, columns)
}

/// `CASE WHEN k + i64::MAX IS NULL THEN l ELSE l END`: a List argument root
/// whose condition raises an arithmetic overflow for every positive key.
fn faulty_argument(
    builder: &mut FragmentBuilder,
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
        .add_expression(node, operand.clone(), ExprKind::Value(key))
        .unwrap();
    let right = builder
        .add_expression(
            node,
            operand,
            ExprKind::Literal(LiteralValue::Int64(i64::MAX)),
        )
        .unwrap();
    let sum = builder
        .add_expression(
            node,
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
        .add_expression(
            node,
            FunctionValueType::new(DataType::Boolean, false),
            ExprKind::IsNull {
                expr: sum,
                negated: false,
            },
        )
        .unwrap();
    let then = builder
        .add_expression(node, list_type(), ExprKind::Value(list))
        .unwrap();
    let otherwise = builder
        .add_expression(node, list_type(), ExprKind::Value(list))
        .unwrap();
    builder
        .add_expression(
            node,
            list_type(),
            ExprKind::Case {
                operand: None,
                when_then: Box::from([(condition, then)]),
                else_expr: Some(otherwise),
            },
        )
        .unwrap()
}

/// Finish a fragment and attach the original request of each table call: its
/// bound Value arguments.
fn finish(builder: FragmentBuilder, root: NodeId, sink: FragmentSink, max_dop: u32) -> Fragment {
    let fragment = builder
        .finish_definition(
            root,
            sink,
            PipelineDopDomain {
                min: 1,
                max: max_dop,
                requires_power_of_two: false,
            },
        )
        .unwrap();
    let mut entries = fragment
        .call_requests()
        .entries()
        .iter()
        .map(|(definition, request)| (*definition, request.clone()))
        .collect::<Vec<_>>();
    let mut work = CompileCheckpoints::try_new(&FixtureControl, CompilePhase::Validate).unwrap();
    novarocks_physical_plan::visit_relational_calls_observed::<FrozenCallError>(
        &fragment,
        &mut work,
        |site, binding, _| {
            let PhysicalCallBinding::Table(function) = binding else {
                panic!("fixture relational calls are table calls")
            };
            entries.push((
                PhysicalCallDefinition::Relational(site),
                PhysicalCallRequest {
                    arguments: function
                        .argument_types
                        .iter()
                        .map(|argument| match argument {
                            FunctionArgumentType::Value(value_type) => {
                                StaticFunctionArgument::Value {
                                    value_type: value_type.clone(),
                                    constant: None,
                                }
                            }
                            FunctionArgumentType::Lambda { .. } => panic!("UNNEST takes values"),
                        })
                        .collect(),
                    logical_argument_count: function.argument_types.len(),
                    expected_result_type: None,
                    constant_policy: constant_policy(),
                },
            ));
            Ok(())
        },
    )
    .unwrap();
    work.finish().unwrap();
    fragment
        .with_call_requests_observed(entries, &FixtureControl)
        .unwrap()
}

/// One use per actual occurrence; a control child gets its own guarded domain
/// exactly as its shape's argument semantics require.
struct Author<'a> {
    fragment: &'a Fragment,
    next_use: u32,
    next_domain: u32,
    uses: Vec<ExpressionInvocation<ExprId>>,
    domains: Vec<ExpressionEvaluationDomain>,
}
impl Author<'_> {
    fn visit(
        &mut self,
        expr: ExprId,
        domain: EvaluationDomainId,
        demand: EvaluationDemand,
    ) -> ExpressionUseId {
        let id = ExpressionUseId::new(self.next_use);
        self.next_use += 1;
        let kind = &self.fragment.expressions().get(expr).unwrap().kind;
        let shape = kind
            .intrinsic_control_shape()
            .unwrap()
            .expect("fixture authors no function call");
        let mut children = Vec::new();
        kind.expression_references_observed::<std::convert::Infallible>(|child| {
            children.push(child);
            Ok(())
        })
        .unwrap();
        let mut arguments = Vec::new();
        for (ordinal, child) in children.iter().enumerate() {
            let (child_demand, guard) =
                control_argument_semantics(shape, children.len(), ordinal, demand).unwrap();
            let child_domain = match guard {
                Some(kind) => {
                    let child_domain = EvaluationDomainId::new(self.next_domain);
                    self.next_domain += 1;
                    self.domains.push(ExpressionEvaluationDomain {
                        id: child_domain,
                        parent: Some(domain),
                        guard: Some(DomainGuard { owner: id, kind }),
                    });
                    child_domain
                }
                None => domain,
            };
            arguments.push(self.visit(*child, child_domain, child_demand));
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

/// Root uses and frozen calls of one fragment. Expression roots share domain
/// 0; table call `k` owns an unguarded domain and a relational use of its own,
/// with the effects a fresh preparation by the installed owner refines.
fn freeze(
    fragment: &Fragment,
    parameters: &SemanticParameters,
) -> (PhysicalRootUses, FrozenFragmentCalls) {
    let roots = PhysicalExpressionRoots::try_new(fragment, &FixtureControl).unwrap();
    let domain = EvaluationDomainId::new(0);
    let mut author = Author {
        fragment,
        next_use: 0,
        next_domain: 1,
        uses: Vec::new(),
        domains: vec![ExpressionEvaluationDomain {
            id: domain,
            parent: None,
            guard: None,
        }],
    };
    let mut bindings = Vec::new();
    for (site, root) in roots.sites() {
        bindings.push((*site, author.visit(root.expr, domain, root.demand)));
    }
    let Author {
        uses, mut domains, ..
    } = author;
    let mut sites = Vec::new();
    for node in fragment.nodes().values() {
        if matches!(node.kind, NodeKind::TableFunction { .. }) {
            let k = u32::try_from(sites.len()).unwrap();
            let context = ExpressionEffectContext {
                use_id: ExpressionUseId::new(RELATIONAL_USE + k),
                domain: EvaluationDomainId::new(RELATIONAL_DOMAIN + k),
                demand: EvaluationDemand::Value,
            };
            domains.push(ExpressionEvaluationDomain {
                id: context.domain,
                parent: None,
                guard: None,
            });
            sites.push((node.id, context));
        }
    }
    let flow = ExpressionControlFlow::try_new(
        domains,
        uses,
        fragment.expressions(),
        CompilePhase::Validate,
        &FixtureControl,
    )
    .unwrap();
    let root_uses = PhysicalRootUses::try_new(fragment, flow, bindings, &FixtureControl).unwrap();
    let catalog = unnest_catalog();
    let mut frozen = Vec::new();
    for (node, context) in sites {
        let NodeKind::TableFunction {
            function,
            arguments,
            ..
        } = &fragment.nodes()[&node].kind
        else {
            unreachable!()
        };
        let selection = Arc::new(FunctionBindingSelection {
            overload: function.overload.clone(),
            argument_types: function.argument_types.clone(),
            result_type: FunctionResultType::Relation(function.result_types.clone()),
            aggregate: None,
        });
        let request = function
            .argument_types
            .iter()
            .map(|argument| match argument {
                FunctionArgumentType::Value(value_type) => FunctionArgument::Value {
                    value_type: value_type.clone(),
                    constant: None,
                },
                FunctionArgumentType::Lambda { .. } => panic!("UNNEST takes values"),
            })
            .collect::<Vec<_>>();
        let argument_uses = (0..arguments.len())
            .map(|argument| {
                Some(
                    root_uses.bindings()[&ExpressionRootSite {
                        node,
                        role: ExpressionRootRole::TableFunctionArgument {
                            argument: u32::try_from(argument).unwrap(),
                        },
                    }],
                )
            })
            .collect::<Vec<_>>();
        let token = catalog
            .prepare_fresh(
                CallEffectInput {
                    context,
                    argument_uses: novarocks_functions::CallArgumentUses::SelectedChannels(
                        &argument_uses,
                    ),
                    function_id: &function.function_id,
                    kind: FunctionKind::Table,
                    selected: selection.as_ref(),
                    request: FunctionBindingRequest {
                        arguments: &request,
                        logical_argument_count: request.len(),
                        expected_result_type: None,
                    },
                    environment: &[],
                    parameters,
                    decimal_overflow_policy: POLICY,
                    proof_scope: CallProofScope::Domain(context.domain),
                },
                Arc::clone(&selection),
                PureCallPreparation::Table {
                    arguments: ScopedExpressionEffects::primitive(
                        context,
                        ExpressionEffects::PURE_VALUE,
                    ),
                },
                &FixtureControl,
            )
            .unwrap_or_else(|error| panic!("fixture table call prepares: {error}"));
        frozen.push(FrozenPhysicalCall {
            materialized_writer_input: None,
            regexp_count_pattern_source: None,
            to_base64_byte_source: None,
            temporal_source: None,
            site: PhysicalCallSite::Table { node },
            context,
            effects: token.call_contract().effects().clone(),
            decimal_overflow_policy: POLICY,
        });
    }
    let calls = FrozenFragmentCalls::try_new(fragment, &root_uses, frozen, &FixtureControl)
        .unwrap_or_else(|error| panic!("frozen table calls validate: {error}"));
    (root_uses, calls)
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
    // The table call's own environment is empty; no parameter reaches it.
    let parameters = SemanticParameters::try_new([]).unwrap();
    let mut uses = BTreeMap::new();
    let mut calls = BTreeMap::new();
    let mut pruning = BTreeMap::new();
    let mut admissions = BTreeMap::new();
    for (id, fragment) in plan.fragments() {
        let (root_uses, frozen) = freeze(fragment, &parameters);
        uses.insert(*id, root_uses);
        calls.insert(*id, frozen);
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
        &uses,
        &calls,
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
    let mut pools = Pools::new();
    let mut builder = FragmentBuilder::new(SOLE);
    let values_node = builder.reserve_node_id().unwrap();
    let (key, lists) = values(&mut builder, &mut pools, values_node, rows, spec.lists);
    let input = if sorted {
        let sort = builder.reserve_node_id().unwrap();
        let expr = builder
            .add_expression(sort, int64(false), ExprKind::Value(key))
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
    let (table, _) = add_table_function(&mut builder, input, key, &lists, spec);
    let fragment = finish(builder, table, FragmentSink::Result, 1);
    let port = result_port(&fragment, table);
    let mut plan = PlanBuilder::new(version())
        .with_constant_pools(pools.0)
        .with_semantic_parameters(parameters(spec));
    plan.add_fragment(fragment).unwrap();
    plan.set_result_port(port).unwrap();
    let plan = plan
        .finish_observed(&FixtureControl)
        .unwrap_or_else(|error| panic!("the table function plan validates: {error:?}"));
    let package = packages(&plan).remove(&SOLE).unwrap();
    try_compile(package, &unnest_catalog(), 1, true)
}

/// Prepare and run the program with an explicit chunk size; a preparation
/// or execution failure retains its original typed cause.
fn try_run_single(
    program: &Arc<LocalProgram>,
    batch_size: i32,
) -> crate::runtime::fragment::ExecutionResult<Vec<Chunk>> {
    let state = Arc::new(RuntimeState::new(
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
    let output = ResultSinkHandle::new();
    let dop = i32::try_from(program.graph().profile().pipeline_dop().get()).unwrap();
    let prepared = prepare_compiled_program_pipeline_execution(
        Arc::clone(program),
        Duration::from_millis(10),
        Box::new(ResultSinkFactory::new(output.clone())),
        ExchangeBindings::default(),
        None,
        dop,
        state,
        Arc::new(NoopFragmentEventSink),
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
    use crate::runtime::fragment::{
        ExecutionFailureCause, ExecutionFailureContext, PipelineOperation,
    };
    use novarocks_local_program::{ProgramExpressionRootSite, ProgramNodeExpressionRole};
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
        error.context(),
        Some(ExecutionFailureContext {
            operator_ordinal: 1,
            operation: PipelineOperation::Push,
        }),
        "{error:?}"
    );
    let ExecutionFailureCause::RequiredRow(required) = error.cause() else {
        panic!("the argument Project must preserve its required row error: {error:?}");
    };
    assert_eq!(
        required.root(),
        ProgramExpressionRootSite::Node {
            node: *input,
            role: ProgramNodeExpressionRole::ProjectOutput { expression: 1 },
        }
    );
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
    let mut pools = Pools::new();
    let mut source = FragmentBuilder::new(SOURCE);
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
    let source = finish(source, union, FragmentSink::Stream { edge: TO_CONSUMER }, 1);

    let mut consumer = FragmentBuilder::new(CONSUMER);
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
    let (table, _) = add_table_function(&mut consumer, receiver, imports[0], &imports[1..], spec);
    let consumer = finish(
        consumer,
        table,
        FragmentSink::Result,
        u32::try_from(dop).unwrap(),
    );
    let port = result_port(&consumer, table);

    let mut plan = PlanBuilder::new(version())
        .with_constant_pools(pools.0)
        .with_semantic_parameters(parameters(spec));
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

    let port = in_process_test_exchange_receiver_port();
    let transmitter = LoopbackTransmitter::new(Arc::clone(&port));
    let bindings = register(&consumer, CONSUMER_FINST, 1, &port);
    let sink = stream_sink(
        &source,
        SOURCE_FINST,
        vec![destination(CONSUMER_FINST, SOURCE_FINST, 0, 1)],
        &transmitter,
    );
    try_run(&source, sink, ExchangeBindings::default(), SOURCE_FINST)
        .unwrap_or_else(|error| panic!("source runs: {error}"));
    let output = ResultSinkHandle::new();
    try_run(
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
    use crate::runtime::fragment::ExecutionFailureCause;
    use crate::runtime::mem_tracker::MemTracker;
    use crate::runtime::runtime_state::RuntimeErrorState;
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
    operator.set_construction_factory(super::aggregate_fixture::runtime_construction_factory(
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
    let state = RuntimeState::default();
    let processor = operator.as_processor_mut().unwrap();
    processor.push_chunk(&state, input).unwrap();
    let output = processor.pull_chunk(&state).unwrap().unwrap();
    assert_eq!(
        int64_rows(std::slice::from_ref(&output)),
        vec![vec![Some(7), Some(10)], vec![Some(7), Some(20)]]
    );
    let schema = output.chunk_schema_ref();
    assert!(schema.is_typed());
    assert!(
        schema
            .slots()
            .iter()
            .all(|slot| slot.field_ref().metadata().capacity() == 0)
    );
    drop(operator);
    drop(output);
    assert!(tracker.current() > 0);
    drop(schema);
    assert_eq!(tracker.current(), 0);
}

#[test]
fn compiled_table_prepare_preserves_real_memory_refusal_before_input() {
    use crate::exec::operators::compiled_table_function::CompiledTableFunctionProcessorFactory;
    use crate::exec::pipeline::operator::ConstructionFailure;
    use crate::exec::pipeline::operator_factory::OperatorFactory;
    use crate::runtime::fragment::ExecutionFailureCause;
    use crate::runtime::mem_tracker::MemTracker;
    use crate::runtime::runtime_state::RuntimeErrorState;
    let program = single_program(&rows(), &Spec::unnest(false), false).unwrap();
    let factory = CompiledTableFunctionProcessorFactory::try_new(
        program.clone(),
        program.graph().root(),
        Arc::new(RuntimeErrorState::default()),
    )
    .unwrap();
    let tracker = MemTracker::new_root("table actual prepare refusal");
    let mut operator = factory.create(1, 0);
    operator.set_construction_factory(super::aggregate_fixture::runtime_construction_factory(
        tracker.clone(),
        Arc::new(RuntimeErrorState::default()),
    ));
    let baseline = tracker.current();
    assert!(baseline > 0);
    tracker.install_limit_once(baseline + 1).unwrap();
    assert!(matches!(
        operator.prepare().unwrap_err().cause(),
        ExecutionFailureCause::Construction(ConstructionFailure::Memory(_))
    ));
    assert!(matches!(
        operator.prepare().unwrap_err().cause(),
        ExecutionFailureCause::Kernel(novarocks_functions::KernelFailure::InstanceFailed)
    ));
    assert!(operator.is_finished());
    assert_eq!(tracker.current(), baseline);
    drop(operator);
    assert_eq!(tracker.current(), 0);
}
