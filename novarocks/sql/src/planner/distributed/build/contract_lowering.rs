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

//! Direct SQL physical-plan lowering into the final physical-plan contract.
//!
//! This visitor deliberately consumes SQL's planner-private physical tree. It
//! never accepts the legacy sealed distributed representation, so extending
//! operator coverage cannot accidentally turn that carrier into a second plan
//! authority.

#![allow(dead_code)]

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use arrow::datatypes::DataType;
#[cfg(test)]
use novarocks_physical_plan::ProviderReadOccurrenceId;
use novarocks_physical_plan::{
    AggregateBinding, AggregateCall as ContractAggregateCall, AggregateCallId, AggregateGrouping,
    AggregatePhase, AggregateSequenceId, ArtifactRefId, BinaryOperator, BoundFunction,
    BoundTableFunction, BucketOrdinalDomainProof, BuildError, ChangeEventSpec,
    ChangeStreamRoute as ContractChangeStreamRoute, DataRelation, Distribution, Edge,
    EdgeDestination, EdgeId, EdgeKind, EdgePartitioning, EdgeSource, ExprId,
    ExprKind as ContractExprKind, Fragment, FragmentBuilder, FragmentId, FragmentSink,
    FunctionArgumentType, GroupingOutput, HashDefinition, HashPartitionScheme,
    JoinDistribution as ContractJoinDistribution, JoinKey, JoinKind as ContractJoinKind,
    JoinSide as ContractJoinSide, LiteralValue as ContractLiteralValue, MetadataRelation,
    MetadataRelationKind, NestLoopJoinDistribution, NodeId, NodeKind, NullOrdering, OrderingKey,
    OutputPort, PartitionCountDomain, PartitionCountParameter, PartitionTopNType,
    PhysicalProperties, PipelineDopDomain, PlanAnnotation, PlanBuilder, PlanVersionId,
    PredicateGuarantee, PredicateGuaranteeKind, ProviderReadReference,
    ROOT_WRITE_RESULT_SCHEMA_REVISION, Relation, RelationField, RequiredInputs, ResultField,
    ResultPort, RowCountAssertion, RowCountAssertionSpec, RowMultiplicity, RuntimeFilter,
    RuntimeFilterArtifactCapability, RuntimeFilterCompletion, RuntimeFilterConsumer,
    RuntimeFilterConsumerActivation, RuntimeFilterConsumerTarget, RuntimeFilterContributionKind,
    RuntimeFilterCoverage, RuntimeFilterCoverageNode, RuntimeFilterDomain, RuntimeFilterEndpoint,
    RuntimeFilterEqualityWitness, RuntimeFilterEqualityWitnessId, RuntimeFilterId,
    RuntimeFilterKind, RuntimeFilterLifecycle, RuntimeFilterLineageStep,
    RuntimeFilterNullSemantics, RuntimeFilterOrderKey, RuntimeFilterPolicy, RuntimeFilterProducer,
    RuntimeFilterProducerProgress, RuntimeFilterProducerTarget, RuntimeFilterReduction,
    RuntimeFilterWitnessId, SealedArtifactRef, SetOperationKind, SortDirection, SortExpr, SortMode,
    TableFunctionOutput, TopNPhase as ContractTopNPhase, TopNSequenceId, UnaryOperator,
    UnpivotConstant as ContractUnpivotConstant, UnpivotSpec, UnpivotValueMapping, ValidationErrors,
    ValueId, ValueOrigin, ValueType, WRITER_MULTIPLEX_SCHEMA_REVISION,
    WindowBound as ContractWindowBound, WindowExpression, WindowFrame as ContractWindowFrame,
    WindowFrameExclusion, WindowFrameUnits, WindowSpec, WriterAggregateCall, WriterDerivedKind,
    WriterFinishSpec, WriterGroupedUnpivotMapping, WriterGroupedUnpivotSpec, WriterRelationField,
    WriterRelationFieldRole, WriterRelationSchema, WriterTarget, WriterTargetField,
};
use novarocks_spi::connector::read_stack::ConnectorReadRelationKind;
use novarocks_spi::connector::write_stack::{RootWriteResultSchema, WriteTargetOrdinal};
use novarocks_type_contract::{
    OrderedComparisonAlgorithm, PartitionCountParameterId, PartitionSpaceId,
};
use sha2::{Digest, Sha256};

use crate::analysis::cte::CteId;
use crate::analysis::{BinOp, ExprKind, LiteralValue, OutputColumn, TypedExpr, UnOp};
use crate::column_id::ColumnId;
use crate::compiler::{
    FinalizedProviderRead, FinalizedProviderReadSet, ProviderBucketPartitionScheme,
    ProviderHashPartitionScheme, ProviderReadDistribution, ProviderReadProperties,
    ProviderReadRelationNeed,
};
use crate::planner::distributed::write::auxiliary::WriterAuxiliaryPlan;
use crate::planner::distributed::write::change_stream::ChangeStreamWriteDagSpec;
use crate::planner::distributed::write::contract::{
    ConnectorWriteInputBinding, FinalizedWriteTargetSet, SqlWritePlanInput,
};
use crate::planner::physical::{
    AggMode, JoinDistribution as SqlJoinDistribution, JoinExecutionMode, PhysicalHashJoinBuildSide,
    PhysicalPlanKind, PhysicalPlanNode, PlanSetOpKind, RedistributeMode, TopNPhase as SqlTopNPhase,
};

const ROOT_FRAGMENT_ID: FragmentId = FragmentId::new(0);

/// Lower the finalized SQL physical tree directly into the immutable contract.
///
/// `version` and `dop_domain` are frozen inputs owned by the caller. This
/// lowering never derives either fact from process state or topology. The
/// covered family includes no-I/O relational nodes, stream redistribution,
/// split TopN, nested-loop joins with proven placement, and set operations.
/// Every other physical operator fails with its concrete kind or missing fact.
pub(crate) fn lower_final_physical_plan(
    plan: &PhysicalPlanNode,
    version: PlanVersionId,
    dop_domain: PipelineDopDomain,
) -> Result<PlanBuilder, ContractLoweringError> {
    lower_final_physical_plan_inner(plan, version, dop_domain, None)
}

pub(crate) fn lower_final_physical_plan_with_provider_reads(
    plan: &PhysicalPlanNode,
    version: PlanVersionId,
    dop_domain: PipelineDopDomain,
    reads: FinalizedProviderReadSet,
) -> Result<PlanBuilder, ContractLoweringError> {
    lower_final_physical_plan_inner(plan, version, dop_domain, Some(reads))
}

/// Lower one admitted SQL write directly into the final physical-plan
/// contract. The provider handle is consumed by exact target ordinal here;
/// neither the logical tree nor the legacy distributed-plan carrier can own or
/// reconstruct it.
pub(crate) struct FinalWriteLowering<'a> {
    pub(crate) reads: Option<FinalizedProviderReadSet>,
    pub(crate) write: SqlWritePlanInput,
    pub(crate) write_target_ordinal: WriteTargetOrdinal,
    pub(crate) auxiliary: &'a WriterAuxiliaryPlan,
    pub(crate) targets: FinalizedWriteTargetSet,
}

pub(crate) fn lower_final_physical_write_plan(
    plan: &PhysicalPlanNode,
    version: PlanVersionId,
    dop_domain: PipelineDopDomain,
    input: FinalWriteLowering<'_>,
) -> Result<PlanBuilder, ContractLoweringError> {
    let FinalWriteLowering {
        reads,
        write,
        write_target_ordinal,
        auxiliary,
        mut targets,
    } = input;
    let mut visitor = ContractLoweringVisitor::new(version, dop_domain, reads);
    // A write states the runtime filters it can name, for the same reason a
    // read does: a filter whose probe is not one value of one type is a filter
    // the plan cannot say anything exact about.
    visitor.unstatable_runtime_filters = unstatable_runtime_filters(plan);
    let source = visitor.lower_node(plan)?;
    let sequences = visitor.allocate_writer_aggregate_sequences(auxiliary)?;
    let writer = visitor.lower_table_writer(
        source,
        write,
        write_target_ordinal,
        auxiliary,
        &sequences,
        targets.take(write_target_ordinal).map_err(invalid_write)?,
    )?;
    targets.ensure_consumed().map_err(invalid_write)?;
    visitor.lower_single_writer_finish(writer, write_target_ordinal, auxiliary, &sequences)
}

pub(crate) struct FinalChangeStreamWriteLowering<'a> {
    pub(crate) reads: Option<FinalizedProviderReadSet>,
    pub(crate) dag: ChangeStreamWriteDagSpec,
    pub(crate) auxiliary: &'a WriterAuxiliaryPlan,
    pub(crate) targets: FinalizedWriteTargetSet,
}

pub(crate) fn lower_final_change_stream_write_plan(
    plan: &PhysicalPlanNode,
    version: PlanVersionId,
    dop_domain: PipelineDopDomain,
    input: FinalChangeStreamWriteLowering<'_>,
) -> Result<PlanBuilder, ContractLoweringError> {
    let FinalChangeStreamWriteLowering {
        reads,
        dag,
        auxiliary,
        mut targets,
    } = input;
    dag.validate().map_err(invalid_write)?;
    if !matches!(plan.kind, PhysicalPlanKind::ChangeEventExpand(_)) {
        return Err(invalid_write(
            "change-stream router source is not ChangeEventExpand".into(),
        ));
    }
    let mut visitor = ContractLoweringVisitor::new(version, dop_domain, reads);
    // A write states the runtime filters it can name, for the same reason a
    // read does: a filter whose probe is not one value of one type is a filter
    // the plan cannot say anything exact about.
    visitor.unstatable_runtime_filters = unstatable_runtime_filters(plan);
    let source = visitor.lower_node(plan)?;
    let sequences = visitor.allocate_writer_aggregate_sequences(auxiliary)?;
    let (writers, ordinals) =
        visitor.lower_change_stream_writers(source, dag, auxiliary, &sequences, &mut targets)?;
    targets.ensure_consumed().map_err(invalid_write)?;
    visitor.lower_writer_finish(writers, ordinals, auxiliary, &sequences)
}

fn lower_final_physical_plan_inner(
    plan: &PhysicalPlanNode,
    version: PlanVersionId,
    dop_domain: PipelineDopDomain,
    reads: Option<FinalizedProviderReadSet>,
) -> Result<PlanBuilder, ContractLoweringError> {
    let mut visitor = ContractLoweringVisitor::new(version, dop_domain, reads);
    visitor.unstatable_runtime_filters = unstatable_runtime_filters(plan);
    let root = visitor.lower_node(plan)?;

    // What the statement delivers is the type each value actually carries,
    // not the type the statement was analyzed to expect: a plan's nullability
    // widens on the way out, and the client is told what arrives.
    let result_types = root
        .output
        .iter()
        .map(|value| visitor.value_declared_type(*value))
        .collect::<Result<Vec<_>, _>>()?;
    let result_fields = result_fields(plan, &root.output, &result_types, &root.display_names)?;
    let result_output = OutputPort {
        node: root.node,
        columns: root.output.clone(),
    };
    let result_port = ResultPort {
        fragment: root.fragment,
        output: result_output,
        fields: result_fields,
    };
    visitor.complete_fragment(root.fragment, root.node, FragmentSink::Result)?;
    visitor.finish_draft(result_port)
}

struct ContractLoweringVisitor {
    current_fragment: FragmentId,
    next_fragment: u32,
    /// The one hash partition space each destination fragment receives in.
    exchange_hash_schemes: BTreeMap<FragmentId, HashPartitionScheme>,
    next_topn_sequence: u32,
    next_aggregate_sequence: u32,
    next_aggregate_call: u32,
    pending_aggregate_sequences: Option<Box<[AggregateSequenceId]>>,
    pending_aggregate_sequence_used: bool,
    /// The sequence a final TopN offers to a partial one below it.
    ///
    /// A split the planner performed itself -- a partial TopN that prunes an
    /// aggregate's groups before they are shuffled -- reaches the lowering as
    /// two nodes rather than one, and they are paired here.
    pending_topn_sequence: Option<TopNSequenceId>,
    pending_topn_sequence_used: bool,
    /// Runtime filters this plan cannot state, decided once before anything
    /// is lowered so that a filter's producer and its consumer make the same
    /// decision wherever each of them is reached.
    unstatable_runtime_filters: BTreeSet<i32>,
    /// The innermost lambda whose body is being lowered, and which parameter
    /// each of its bound names stands at. Every expression built while this
    /// is set belongs to that lambda's scope.
    /// The lambdas open at this point, outermost first.
    lambda_scope: Vec<LoweringLambdaScope>,
    fragments: BTreeMap<FragmentId, FragmentBuilder>,
    completions: BTreeMap<FragmentId, (NodeId, FragmentSink)>,
    plan_version: PlanVersionId,
    plan_builder: PlanBuilder,
    artifact_refs: BTreeMap<ArtifactRefId, SealedArtifactRef>,
    provider_partition_definitions: BTreeMap<PartitionSpaceId, ProviderPartitionDefinition>,
    dop_domain: PipelineDopDomain,
    provider_reads: Option<FinalizedProviderReadSet>,
    cte_producers: BTreeMap<CteId, CteProducer>,
    annotated_nodes: BTreeSet<(FragmentId, NodeId)>,
    /// What each value is already called, so a column keeps one name.
    annotated_values: BTreeMap<(FragmentId, ValueId), String>,
    runtime_filter_builds: BTreeMap<i32, PendingRuntimeFilterBuild>,
    runtime_filter_probes: BTreeMap<i32, Vec<PendingRuntimeFilterProbe>>,
    runtime_filter_attachments: BTreeSet<(FragmentId, RuntimeFilterId)>,
    edges: BTreeMap<EdgeId, Edge>,
}

struct CteProducer {
    fragment: FragmentId,
    root: NodeId,
    outputs: BTreeMap<ColumnId, (ValueId, ValueType)>,
    row_multiplicity: RowMultiplicity,
    edges: Vec<novarocks_physical_plan::EdgeId>,
}

#[derive(Clone)]
enum PendingRuntimeFilterBuild {
    Join {
        fragment: FragmentId,
        node: NodeId,
        key_ordinal: u32,
        build_side: ContractJoinSide,
        execution_mode: crate::planner::physical::JoinExecutionMode,
        build_value: ValueId,
        probe_value: ValueId,
        null_semantics: RuntimeFilterNullSemantics,
    },
    AggregateTopN {
        fragment: FragmentId,
        node: NodeId,
        group_key_ordinal: u32,
        input_value: ValueId,
        limit: u32,
        direction: SortDirection,
        null_ordering: NullOrdering,
    },
}

#[derive(Clone, Copy)]
struct PendingRuntimeFilterProbe {
    fragment: FragmentId,
    node: NodeId,
    value: ValueId,
    scan_source: bool,
}

/// The plan identity of one runtime filter the placement numbered.
///
/// Placement numbers its filters from zero within one statement. A plan's
/// runtime filter identity is the channel a deployment addresses, and zero is
/// reserved there so an absent wire field cannot read back as a real channel,
/// so the plan's space starts where placement's zero lands.
fn runtime_filter_id(id: i32) -> Result<RuntimeFilterId, ContractLoweringError> {
    u32::try_from(id)
        .ok()
        .and_then(|id| id.checked_add(1))
        .map(RuntimeFilterId::new)
        .ok_or_else(|| ContractLoweringError::InvalidRuntimeFilter {
            id,
            detail: "identity is negative or exhausts the plan identity space".to_string(),
        })
}

fn all_of_runtime_filter_witness(witness: RuntimeFilterWitnessId) -> RuntimeFilterCoverage {
    RuntimeFilterCoverage {
        nodes: Box::from([
            RuntimeFilterCoverageNode::Witness(witness),
            RuntimeFilterCoverageNode::AllOf {
                children: Box::from([0]),
            },
        ]),
        root: 1,
    }
}

fn leaf_runtime_filter_witness(witness: RuntimeFilterWitnessId) -> RuntimeFilterCoverage {
    RuntimeFilterCoverage {
        nodes: Box::from([RuntimeFilterCoverageNode::Witness(witness)]),
        root: 0,
    }
}

fn any_of_runtime_filter_witness(witness: RuntimeFilterWitnessId) -> RuntimeFilterCoverage {
    RuntimeFilterCoverage {
        nodes: Box::from([
            RuntimeFilterCoverageNode::Witness(witness),
            RuntimeFilterCoverageNode::AnyOf {
                children: Box::from([0]),
            },
        ]),
        root: 1,
    }
}

fn join_runtime_filter_coverage(
    witness: RuntimeFilterWitnessId,
    execution_mode: crate::planner::physical::JoinExecutionMode,
) -> RuntimeFilterCoverage {
    match execution_mode {
        crate::planner::physical::JoinExecutionMode::Broadcast => {
            any_of_runtime_filter_witness(witness)
        }
        crate::planner::physical::JoinExecutionMode::Partitioned => {
            all_of_runtime_filter_witness(witness)
        }
        crate::planner::physical::JoinExecutionMode::Colocate
        | crate::planner::physical::JoinExecutionMode::Singleton => {
            leaf_runtime_filter_witness(witness)
        }
    }
}

fn fragment_inbound_edges(fragment: &Fragment) -> BTreeSet<EdgeId> {
    fragment
        .nodes()
        .values()
        .filter_map(|node| match node.kind {
            NodeKind::ExchangeSource { edge, .. } => Some(edge),
            _ => None,
        })
        .collect()
}

fn subtree_inbound_edges(fragment: &Fragment, root: NodeId) -> BTreeSet<EdgeId> {
    let mut edges = BTreeSet::new();
    let mut pending = vec![root];
    let mut visited = BTreeSet::new();
    while let Some(node_id) = pending.pop() {
        if !visited.insert(node_id) {
            continue;
        }
        let Some(node) = fragment.nodes().get(&node_id) else {
            continue;
        };
        if let NodeKind::ExchangeSource { edge, .. } = node.kind {
            edges.insert(edge);
        }
        pending.extend(node.inputs.iter().copied());
    }
    edges
}

fn direct_expression_value(fragment: &Fragment, expression: ExprId) -> Option<ValueId> {
    match fragment.expressions().get(expression)?.kind {
        ContractExprKind::Value(value) => Some(value),
        _ => None,
    }
}

/// Following one input of a join that republishes the value unchanged.
///
/// Sound where removing rows from that input can only remove rows from the
/// join's output: every output row it removes carries the value the filter
/// rejected, so the filter would have removed it anyway.
fn join_output_pass_through(
    fragment: &Fragment,
    node: &novarocks_physical_plan::PhysicalNode,
    position: (FragmentId, NodeId, ValueId),
    kind: novarocks_physical_plan::JoinKind,
) -> impl Iterator<Item = (RuntimeFilterLineageStep, (FragmentId, NodeId, ValueId))> {
    [ContractJoinSide::Left, ContractJoinSide::Right]
        .into_iter()
        .filter(move |side| kind.side_only_loses_rows(*side))
        .filter_map(move |side| {
            let ordinal = u32::try_from(side.input_ordinal()).ok()?;
            let input = *node.inputs.get(usize::try_from(ordinal).ok()?)?;
            fragment
                .nodes()
                .get(&input)?
                .output
                .columns
                .contains(&position.2)
                .then_some((
                    RuntimeFilterLineageStep::JoinOutputPassThrough {
                        fragment: position.0,
                        node: position.1,
                        input_ordinal: ordinal,
                    },
                    (position.0, input, position.2),
                ))
        })
}

/// What one lowered node is called, for a message that names it.
const fn lowered_node_kind_name(kind: &NodeKind) -> &'static str {
    match kind {
        NodeKind::Scan { .. } => "Scan",
        NodeKind::Values { .. } => "Values",
        NodeKind::Filter { .. } => "Filter",
        NodeKind::Project { .. } => "Project",
        NodeKind::Aggregate { .. } => "Aggregate",
        NodeKind::Sort { .. } => "Sort",
        NodeKind::TopN { .. } => "TopN",
        NodeKind::Limit { .. } => "Limit",
        NodeKind::Window(_) => "Window",
        NodeKind::Repeat { .. } => "Repeat",
        NodeKind::Unpivot { .. } => "Unpivot",
        NodeKind::GenerateSeries { .. } => "GenerateSeries",
        NodeKind::TableFunction { .. } => "TableFunction",
        NodeKind::HashJoin { .. } => "HashJoin",
        NodeKind::NestLoopJoin { .. } => "NestLoopJoin",
        NodeKind::SetOp { .. } => "SetOp",
        NodeKind::AssertOneRow(_) => "AssertOneRow",
        NodeKind::ExchangeSource { .. } => "ExchangeSource",
        NodeKind::TableWriter { .. } => "TableWriter",
        NodeKind::TableFinish(_) => "TableFinish",
        NodeKind::ChangeEventExpand { .. } => "ChangeEventExpand",
    }
}

/// Where a lineage walk ran out, so a refusal can say what stopped it.
///
/// The walk is a search, so "where it stopped" is the deepest node it reached
/// that carries the value no further -- that is the node a filter would have
/// to be pushed past, and naming it is the whole diagnosis.
struct LineageDeadEnd {
    fragment: FragmentId,
    node: NodeId,
    kind: String,
    depth: usize,
}

fn runtime_filter_scan_lineage(
    fragments: &BTreeMap<FragmentId, Fragment>,
    edges: &BTreeMap<EdgeId, Edge>,
    start: (FragmentId, NodeId, ValueId),
    target: (FragmentId, NodeId, ValueId),
    dead_end: &mut Option<LineageDeadEnd>,
) -> Option<Box<[RuntimeFilterLineageStep]>> {
    #[allow(clippy::too_many_arguments)]
    fn walk(
        fragments: &BTreeMap<FragmentId, Fragment>,
        edges: &BTreeMap<EdgeId, Edge>,
        position: (FragmentId, NodeId, ValueId),
        target: (FragmentId, NodeId, ValueId),
        visited: &mut BTreeSet<(FragmentId, NodeId, ValueId)>,
        depth: usize,
        dead_end: &mut Option<LineageDeadEnd>,
    ) -> Option<Vec<RuntimeFilterLineageStep>> {
        if position == target {
            return Some(Vec::new());
        }
        if !visited.insert(position) {
            return None;
        }
        let fragment = fragments.get(&position.0)?;
        let node = fragment.nodes().get(&position.1)?;
        let candidates = match &node.kind {
            NodeKind::Filter { .. } if node.inputs.len() == 1 => vec![(
                RuntimeFilterLineageStep::FilterPassThrough {
                    fragment: position.0,
                    node: position.1,
                    input_ordinal: 0,
                },
                (position.0, node.inputs[0], position.2),
            )],
            NodeKind::Sort {
                mode: SortMode::Global | SortMode::Analytic { .. },
                ..
            } if node.inputs.len() == 1 => vec![(
                RuntimeFilterLineageStep::SortPassThrough {
                    fragment: position.0,
                    node: position.1,
                    input_ordinal: 0,
                },
                (position.0, node.inputs[0], position.2),
            )],
            NodeKind::Project { expressions } if node.inputs.len() == 1 => node
                .output
                .columns
                .iter()
                .enumerate()
                .filter(|(_, output)| **output == position.2)
                .filter_map(|(ordinal, _)| {
                    let (expression, output) = expressions.get(ordinal)?;
                    if *output != position.2 {
                        return None;
                    }
                    let source = direct_expression_value(fragment, *expression)?;
                    Some((
                        RuntimeFilterLineageStep::ProjectIdentity {
                            fragment: position.0,
                            node: position.1,
                            output_ordinal: u32::try_from(ordinal).ok()?,
                        },
                        (position.0, node.inputs[0], source),
                    ))
                })
                .collect(),
            NodeKind::HashJoin { kind, keys, .. }
                if node.inputs.len() == 2
                    && (kind.key_filter_reaches_side(ContractJoinSide::Left)
                        || kind.key_filter_reaches_side(ContractJoinSide::Right)) =>
            {
                let into_left = kind.key_filter_reaches_side(ContractJoinSide::Left);
                let into_right = kind.key_filter_reaches_side(ContractJoinSide::Right);
                keys.iter()
                    .enumerate()
                    .filter(|(_, key)| !key.null_safe)
                    .flat_map(move |(ordinal, key)| {
                        let left = novarocks_physical_plan::join_key_source_value(
                            fragment.expressions(),
                            key.left,
                        );
                        let right = novarocks_physical_plan::join_key_source_value(
                            fragment.expressions(),
                            key.right,
                        );
                        [
                            (ContractJoinSide::Left, left),
                            (ContractJoinSide::Right, right),
                        ]
                        .into_iter()
                        .filter(move |(_, value)| *value == Some(position.2))
                        .flat_map(move |(source_side, _)| {
                            [
                                (ContractJoinSide::Left, left),
                                (ContractJoinSide::Right, right),
                            ]
                            .into_iter()
                            .filter(move |(target_side, _)| match target_side {
                                ContractJoinSide::Left => into_left,
                                ContractJoinSide::Right => into_right,
                            })
                            .filter_map(
                                move |(target_side, target_value)| {
                                    Some((
                                        RuntimeFilterLineageStep::JoinEquality {
                                            fragment: position.0,
                                            node: position.1,
                                            key_ordinal: u32::try_from(ordinal).ok()?,
                                            source_side,
                                            target_side,
                                        },
                                        (
                                            position.0,
                                            node.inputs[usize::try_from(
                                                target_side.input_ordinal(),
                                            )
                                            .ok()?],
                                            target_value?,
                                        ),
                                    ))
                                },
                            )
                        })
                    })
                    .chain(join_output_pass_through(fragment, node, position, *kind))
                    .collect()
            }
            // A join that admits no key filter still republishes its inputs'
            // values, and following one of those is a weaker claim.
            NodeKind::HashJoin { kind, .. } | NodeKind::NestLoopJoin { kind, .. }
                if node.inputs.len() == 2 =>
            {
                join_output_pass_through(fragment, node, position, *kind).collect()
            }
            NodeKind::Aggregate { group_by, .. } if node.inputs.len() == 1 => group_by
                .iter()
                .enumerate()
                .filter(|(_, (_, output))| *output == position.2)
                .filter_map(|(ordinal, (expression, _))| {
                    Some((
                        RuntimeFilterLineageStep::AggregateGroupKey {
                            fragment: position.0,
                            node: position.1,
                            group_key_ordinal: u32::try_from(ordinal).ok()?,
                        },
                        (
                            position.0,
                            node.inputs[0],
                            direct_expression_value(fragment, *expression)?,
                        ),
                    ))
                })
                .collect(),
            NodeKind::SetOp {
                kind: SetOperationKind::UnionAll,
                input_mappings,
            } => node
                .output
                .columns
                .iter()
                .enumerate()
                .filter(|(_, output)| **output == position.2)
                .flat_map(|(output_ordinal, _)| {
                    node.inputs
                        .iter()
                        .zip(input_mappings)
                        .enumerate()
                        .filter_map(move |(input_ordinal, (input, mapping))| {
                            Some((
                                RuntimeFilterLineageStep::UnionAllBranch {
                                    fragment: position.0,
                                    node: position.1,
                                    input_ordinal: u32::try_from(input_ordinal).ok()?,
                                    output_ordinal: u32::try_from(output_ordinal).ok()?,
                                },
                                (position.0, *input, *mapping.get(output_ordinal)?),
                            ))
                        })
                })
                .collect(),
            NodeKind::ExchangeSource { edge, imports } => imports
                .iter()
                .enumerate()
                .filter(|(_, (_, destination))| *destination == position.2)
                .filter_map(|(ordinal, (source, _))| {
                    let edge_contract = edges.get(edge)?;
                    let source_fragment = fragments.get(&edge_contract.source.fragment)?;
                    Some((
                        RuntimeFilterLineageStep::ExchangeMapping {
                            edge: *edge,
                            mapping_ordinal: u32::try_from(ordinal).ok()?,
                        },
                        (source_fragment.id(), source_fragment.root(), *source),
                    ))
                })
                .collect(),
            _ => Vec::new(),
        };
        if candidates.is_empty()
            && dead_end
                .as_ref()
                .is_none_or(|deepest| depth > deepest.depth)
        {
            *dead_end = Some(LineageDeadEnd {
                fragment: position.0,
                node: position.1,
                kind: match &node.kind {
                    // A join's kind is what decides whether a filter may be
                    // carried past it, so the message says which one.
                    NodeKind::HashJoin { kind, .. } | NodeKind::NestLoopJoin { kind, .. } => {
                        format!("{} {kind:?}", lowered_node_kind_name(&node.kind))
                    }
                    other => lowered_node_kind_name(other).to_string(),
                },
                depth,
            });
        }
        for (step, next) in candidates {
            let mut candidate_visited = visited.clone();
            if let Some(mut suffix) = walk(
                fragments,
                edges,
                next,
                target,
                &mut candidate_visited,
                depth + 1,
                dead_end,
            ) {
                suffix.insert(0, step);
                return Some(suffix);
            }
        }
        None
    }

    walk(
        fragments,
        edges,
        start,
        target,
        &mut BTreeSet::new(),
        0,
        dead_end,
    )
    .map(Vec::into_boxed_slice)
}

fn materialize_runtime_filter(
    legacy_id: i32,
    build: &PendingRuntimeFilterBuild,
    probes: &[PendingRuntimeFilterProbe],
    fragments: &BTreeMap<FragmentId, Fragment>,
    edges: &BTreeMap<EdgeId, Edge>,
) -> Result<RuntimeFilter, ContractLoweringError> {
    let id = runtime_filter_id(legacy_id)?;
    let witness = RuntimeFilterWitnessId::new(id.get());
    let policy = RuntimeFilterPolicy {
        max_contribution_bytes: 1024,
        max_artifact_bytes: 4096,
        deadline_ms: 30_000,
        max_retries: 3,
    };
    let invalid = |detail: String| ContractLoweringError::InvalidRuntimeFilter {
        id: legacy_id,
        detail,
    };

    match build {
        PendingRuntimeFilterBuild::Join {
            fragment,
            node,
            key_ordinal,
            build_side,
            execution_mode,
            build_value,
            probe_value,
            null_semantics,
        } => {
            let producer_fragment = fragments
                .get(fragment)
                .ok_or_else(|| invalid("producer fragment is absent".to_string()))?;
            let join = producer_fragment
                .nodes()
                .get(node)
                .ok_or_else(|| invalid("producer join is absent".to_string()))?;
            let build_root = join.inputs[usize::try_from(build_side.input_ordinal()).unwrap()];
            let probe_root =
                join.inputs[usize::try_from(build_side.opposite().input_ordinal()).unwrap()];
            let build_edges = subtree_inbound_edges(producer_fragment, build_root);
            let non_build_edges = fragment_inbound_edges(producer_fragment)
                .difference(&build_edges)
                .copied()
                .collect::<BTreeSet<_>>();
            let equality = RuntimeFilterEqualityWitnessId::new(id.get());
            let coverage = join_runtime_filter_coverage(witness, *execution_mode);
            let mut consumers = Vec::new();
            let mut seen_scans = BTreeSet::new();
            let mut needs_join_consumer = false;
            for probe in probes {
                if !probe.scan_source {
                    needs_join_consumer = true;
                    continue;
                }
                if !seen_scans.insert((probe.fragment, probe.node, probe.value)) {
                    continue;
                }
                let mut dead_end = None;
                let lineage = runtime_filter_scan_lineage(
                    fragments,
                    edges,
                    (*fragment, probe_root, *probe_value),
                    (probe.fragment, probe.node, probe.value),
                    &mut dead_end,
                )
                .ok_or_else(|| {
                    invalid(match dead_end {
                        Some(stop) => format!(
                            "scan probe lacks an exact ValueId lineage: it stops at fragment {} node {} ({})",
                            stop.fragment.get(),
                            stop.node.get(),
                            stop.kind
                        ),
                        None => "scan probe lacks an exact ValueId lineage".to_string(),
                    })
                })?;
                consumers.push(RuntimeFilterConsumer {
                    endpoint: RuntimeFilterEndpoint {
                        fragment: probe.fragment,
                        node: probe.node,
                        values: Box::from([probe.value]),
                    },
                    apply_point: novarocks_physical_plan::RuntimeFilterApplyPoint::ScanSource,
                    capabilities: Box::from([
                        RuntimeFilterArtifactCapability::Membership,
                        RuntimeFilterArtifactCapability::EmptyDomain,
                    ]),
                    activation: RuntimeFilterConsumerActivation::BlockingSnapshot,
                    target: RuntimeFilterConsumerTarget::ScanField { equality, lineage },
                });
            }
            if needs_join_consumer || consumers.is_empty() {
                consumers.push(RuntimeFilterConsumer {
                    endpoint: RuntimeFilterEndpoint {
                        fragment: *fragment,
                        node: *node,
                        values: Box::from([*probe_value]),
                    },
                    apply_point: novarocks_physical_plan::RuntimeFilterApplyPoint::NodeInput {
                        input_ordinal: build_side.opposite().input_ordinal(),
                    },
                    capabilities: Box::from([
                        RuntimeFilterArtifactCapability::Membership,
                        RuntimeFilterArtifactCapability::EmptyDomain,
                    ]),
                    activation: RuntimeFilterConsumerActivation::BlockingSnapshot,
                    target: RuntimeFilterConsumerTarget::JoinProbeKey { equality },
                });
            }
            let ty = producer_fragment
                .values()
                .get(build_value)
                .ok_or_else(|| invalid("producer ValueId is absent".to_string()))?
                .ty
                .clone();
            Ok(RuntimeFilter {
                id,
                kind: RuntimeFilterKind::InList,
                domain: RuntimeFilterDomain::Membership {
                    ty,
                    null_semantics: *null_semantics,
                },
                lifecycle: RuntimeFilterLifecycle::CompleteOnce,
                reduction: RuntimeFilterReduction::SetUnion,
                availability_coverage: coverage.clone(),
                terminal_coverage: coverage,
                equality_witnesses: Box::from([RuntimeFilterEqualityWitness {
                    id: equality,
                    fragment: *fragment,
                    join: *node,
                    key_ordinal: *key_ordinal,
                    domain_side: *build_side,
                }]),
                producers: Box::from([RuntimeFilterProducer {
                    witness,
                    endpoint: RuntimeFilterEndpoint {
                        fragment: *fragment,
                        node: *node,
                        values: Box::from([*build_value]),
                    },
                    apply_point: novarocks_physical_plan::RuntimeFilterApplyPoint::NodeInput {
                        input_ordinal: build_side.input_ordinal(),
                    },
                    contribution_kinds: Box::from([
                        RuntimeFilterContributionKind::ValueDomainDelta,
                        RuntimeFilterContributionKind::ProducerClosed,
                    ]),
                    completion: RuntimeFilterCompletion::ProducerClosed,
                    progress: RuntimeFilterProducerProgress {
                        build_edges: build_edges.into_iter().collect(),
                        non_build_edges: non_build_edges.into_iter().collect(),
                    },
                    target: RuntimeFilterProducerTarget::JoinBuildKey { equality },
                }]),
                consumers: consumers.into_boxed_slice(),
                policy,
            })
        }
        PendingRuntimeFilterBuild::AggregateTopN {
            fragment,
            node,
            group_key_ordinal,
            input_value,
            limit,
            direction,
            null_ordering,
        } => {
            let coverage = leaf_runtime_filter_witness(witness);
            let producer_fragment = fragments
                .get(fragment)
                .ok_or_else(|| invalid("producer fragment is absent".to_string()))?;
            let aggregate = producer_fragment
                .nodes()
                .get(node)
                .ok_or_else(|| invalid("producer aggregate is absent".to_string()))?;
            let NodeKind::Aggregate { group_by, .. } = &aggregate.kind else {
                return Err(invalid("producer node is not an aggregate".to_string()));
            };
            let group_output = group_by
                .get(usize::try_from(*group_key_ordinal).unwrap_or(usize::MAX))
                .map(|(_, output)| *output)
                .ok_or_else(|| invalid("producer aggregate group key is absent".to_string()))?;
            let matching_topn = producer_fragment
                .nodes()
                .values()
                .filter_map(|candidate| {
                    let NodeKind::TopN {
                        order_by,
                        limit: frozen_limit,
                        offset,
                        phase,
                    } = &candidate.kind
                    else {
                        return None;
                    };
                    (candidate.inputs.as_ref() == [*node]
                        && matches!(phase, ContractTopNPhase::Partial { .. })
                        && order_by.len() == 1
                        && direct_expression_value(producer_fragment, order_by[0].expr)
                            == Some(group_output)
                        && *frozen_limit == u64::from(*limit)
                        && *offset == 0
                        && order_by[0].direction == *direction
                        && order_by[0].null_ordering == *null_ordering)
                        .then_some((candidate.id, *phase))
                })
                .collect::<Vec<_>>();
            let [(topn, phase)] = matching_topn.as_slice() else {
                return Err(invalid(
                    "Aggregate TopN producer lacks one exact partial TopN parent".to_string(),
                ));
            };
            let input_root = *aggregate
                .inputs
                .first()
                .ok_or_else(|| invalid("producer aggregate has no input".to_string()))?;
            let input_edges = subtree_inbound_edges(producer_fragment, input_root);
            let mut consumers = Vec::new();
            let mut seen = BTreeSet::new();
            for probe in probes {
                if !probe.scan_source || !seen.insert((probe.fragment, probe.node, probe.value)) {
                    if !probe.scan_source {
                        return Err(invalid(
                            "Aggregate TopN consumer is not an exact scan source".to_string(),
                        ));
                    }
                    continue;
                }
                let mut dead_end = None;
                let lineage = runtime_filter_scan_lineage(
                    fragments,
                    edges,
                    (*fragment, input_root, *input_value),
                    (probe.fragment, probe.node, probe.value),
                    &mut dead_end,
                )
                .ok_or_else(|| {
                    invalid(match dead_end {
                        Some(stop) => format!(
                            "Aggregate TopN probe lacks exact ValueId lineage: it stops at fragment {} node {} ({})",
                            stop.fragment.get(),
                            stop.node.get(),
                            stop.kind
                        ),
                        None => "Aggregate TopN probe lacks exact ValueId lineage".to_string(),
                    })
                })?;
                consumers.push(RuntimeFilterConsumer {
                    endpoint: RuntimeFilterEndpoint {
                        fragment: probe.fragment,
                        node: probe.node,
                        values: Box::from([probe.value]),
                    },
                    apply_point: novarocks_physical_plan::RuntimeFilterApplyPoint::ScanSource,
                    capabilities: Box::from([RuntimeFilterArtifactCapability::OrderedRange]),
                    activation: RuntimeFilterConsumerActivation::NonBlockingLive {
                        late_apply: novarocks_physical_plan::LateApplyGranularity::Batch,
                    },
                    target: RuntimeFilterConsumerTarget::AggregateTopNScanField {
                        producer: witness,
                        lineage,
                    },
                });
            }
            let ty = producer_fragment
                .values()
                .get(input_value)
                .ok_or_else(|| invalid("Aggregate TopN producer ValueId is absent".to_string()))?
                .ty
                .clone();
            Ok(RuntimeFilter {
                id,
                kind: RuntimeFilterKind::MinMax,
                domain: RuntimeFilterDomain::Ordered {
                    key: RuntimeFilterOrderKey {
                        ty,
                        direction: *direction,
                        null_ordering: *null_ordering,
                    },
                    inclusive: true,
                    comparator: OrderedComparisonAlgorithm::NativeScalarOrderV1,
                },
                lifecycle: RuntimeFilterLifecycle::MonotonicUpdates,
                reduction: RuntimeFilterReduction::TightenOrderedBound,
                availability_coverage: coverage.clone(),
                terminal_coverage: coverage,
                equality_witnesses: Box::default(),
                producers: Box::from([RuntimeFilterProducer {
                    witness,
                    endpoint: RuntimeFilterEndpoint {
                        fragment: *fragment,
                        node: *node,
                        values: Box::from([*input_value]),
                    },
                    apply_point: novarocks_physical_plan::RuntimeFilterApplyPoint::NodeInput {
                        input_ordinal: 0,
                    },
                    contribution_kinds: Box::from([
                        RuntimeFilterContributionKind::OrderedBoundUpdate,
                        RuntimeFilterContributionKind::ProducerClosed,
                    ]),
                    completion: RuntimeFilterCompletion::ProducerClosed,
                    progress: RuntimeFilterProducerProgress {
                        build_edges: input_edges.into_iter().collect(),
                        non_build_edges: Box::default(),
                    },
                    target: RuntimeFilterProducerTarget::AggregateTopNKey {
                        group_key_ordinal: *group_key_ordinal,
                        topn: *topn,
                        phase: *phase,
                        order_key_ordinal: 0,
                        limit: u64::from(*limit),
                        offset: 0,
                        direction: *direction,
                        null_ordering: *null_ordering,
                    },
                }]),
                consumers: consumers.into_boxed_slice(),
                policy,
            })
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum RuntimeFilterWaitNode {
    Physical(FragmentId, NodeId),
    Filter(RuntimeFilterId),
}

fn resolve_runtime_filter_activations(
    filters: &mut [RuntimeFilter],
    fragments: &BTreeMap<FragmentId, Fragment>,
    edges: &BTreeMap<EdgeId, Edge>,
) {
    let mut dependencies =
        BTreeMap::<RuntimeFilterWaitNode, BTreeSet<RuntimeFilterWaitNode>>::new();
    for fragment in fragments.values() {
        for node in fragment.nodes().values() {
            let current = RuntimeFilterWaitNode::Physical(fragment.id(), node.id);
            let current_dependencies = dependencies.entry(current).or_default();
            current_dependencies.extend(
                node.inputs
                    .iter()
                    .map(|input| RuntimeFilterWaitNode::Physical(fragment.id(), *input)),
            );
            if let NodeKind::ExchangeSource { edge, .. } = node.kind
                && let Some(edge) = edges.get(&edge)
                && let Some(source) = fragments.get(&edge.source.fragment)
            {
                current_dependencies
                    .insert(RuntimeFilterWaitNode::Physical(source.id(), source.root()));
            }
        }
    }
    for filter in filters.iter() {
        let filter_node = RuntimeFilterWaitNode::Filter(filter.id);
        dependencies.entry(filter_node).or_default();
        for producer in &filter.producers {
            let Some(fragment) = fragments.get(&producer.endpoint.fragment) else {
                continue;
            };
            let Some(node) = fragment.nodes().get(&producer.endpoint.node) else {
                continue;
            };
            let producer_root = match (&producer.target, &node.kind) {
                (
                    RuntimeFilterProducerTarget::JoinBuildKey { .. },
                    NodeKind::HashJoin { build_side, .. },
                ) => usize::try_from(build_side.input_ordinal())
                    .ok()
                    .and_then(|ordinal| node.inputs.get(ordinal))
                    .copied(),
                (
                    RuntimeFilterProducerTarget::AggregateTopNKey { .. },
                    NodeKind::Aggregate { .. },
                ) => node.inputs.first().copied(),
                _ => None,
            };
            if let Some(producer_root) = producer_root {
                dependencies.entry(filter_node).or_default().insert(
                    RuntimeFilterWaitNode::Physical(fragment.id(), producer_root),
                );
            }
        }
        for consumer in &filter.consumers {
            if consumer.activation == RuntimeFilterConsumerActivation::BlockingSnapshot {
                dependencies
                    .entry(RuntimeFilterWaitNode::Physical(
                        consumer.endpoint.fragment,
                        consumer.endpoint.node,
                    ))
                    .or_default()
                    .insert(filter_node);
            }
        }
    }
    let downstream = edges.values().fold(
        BTreeMap::<FragmentId, BTreeSet<FragmentId>>::new(),
        |mut index, edge| {
            index
                .entry(edge.source.fragment)
                .or_default()
                .insert(edge.destination.fragment);
            index
        },
    );
    for source in fragments.values() {
        let FragmentSink::Multicast { edges: branches } = source.sink() else {
            continue;
        };
        if branches.len() < 2 {
            continue;
        }
        let mut reachable = BTreeSet::new();
        let mut pending = branches
            .iter()
            .filter_map(|edge| edges.get(edge).map(|edge| edge.destination.fragment))
            .collect::<Vec<_>>();
        while let Some(fragment) = pending.pop() {
            if !reachable.insert(fragment) {
                continue;
            }
            pending.extend(downstream.get(&fragment).into_iter().flatten().copied());
        }
        for consumer in filters
            .iter()
            .flat_map(|filter| &filter.consumers)
            .filter(|consumer| {
                consumer.activation == RuntimeFilterConsumerActivation::BlockingSnapshot
                    && reachable.contains(&consumer.endpoint.fragment)
            })
        {
            dependencies
                .entry(RuntimeFilterWaitNode::Physical(source.id(), source.root()))
                .or_default()
                .insert(RuntimeFilterWaitNode::Physical(
                    consumer.endpoint.fragment,
                    consumer.endpoint.node,
                ));
        }
    }

    let mut reverse = BTreeMap::<RuntimeFilterWaitNode, BTreeSet<RuntimeFilterWaitNode>>::new();
    for (node, node_dependencies) in &dependencies {
        reverse.entry(*node).or_default();
        for dependency in node_dependencies {
            reverse.entry(*dependency).or_default().insert(*node);
        }
    }
    let nodes = reverse.keys().copied().collect::<Vec<_>>();
    let mut visited = BTreeSet::new();
    let mut finish_order = Vec::with_capacity(nodes.len());
    for start in nodes {
        if visited.contains(&start) {
            continue;
        }
        let mut stack = vec![(start, false)];
        while let Some((node, exiting)) = stack.pop() {
            if exiting {
                finish_order.push(node);
                continue;
            }
            if !visited.insert(node) {
                continue;
            }
            stack.push((node, true));
            if let Some(next) = dependencies.get(&node) {
                stack.extend(next.iter().rev().map(|dependency| (*dependency, false)));
            }
        }
    }
    let mut component_by_node = BTreeMap::new();
    let mut next_component = 0_u32;
    while let Some(start) = finish_order.pop() {
        if component_by_node.contains_key(&start) {
            continue;
        }
        let mut stack = vec![start];
        while let Some(node) = stack.pop() {
            if component_by_node.contains_key(&node) {
                continue;
            }
            component_by_node.insert(node, next_component);
            if let Some(next) = reverse.get(&node) {
                stack.extend(next.iter().rev().copied());
            }
        }
        next_component = next_component.saturating_add(1);
    }

    for filter in filters {
        let filter_component = component_by_node
            .get(&RuntimeFilterWaitNode::Filter(filter.id))
            .copied();
        for consumer in &mut filter.consumers {
            if consumer.activation != RuntimeFilterConsumerActivation::BlockingSnapshot {
                continue;
            }
            let consumer_component = component_by_node
                .get(&RuntimeFilterWaitNode::Physical(
                    consumer.endpoint.fragment,
                    consumer.endpoint.node,
                ))
                .copied();
            if filter_component.is_some() && filter_component == consumer_component {
                if matches!(
                    consumer.target,
                    RuntimeFilterConsumerTarget::AggregateTopNScanField { .. }
                ) {
                    continue;
                }
                // A membership filter is applied a batch at a time, by every
                // operator that applies one: a scan reading through it, an
                // exchange source standing where that scan's rows arrive, and
                // a join probe all see rows in batches. Finer granularities
                // belong to the ordered filters, which narrow what is read
                // rather than which rows survive.
                consumer.activation =
                    RuntimeFilterConsumerActivation::StartUnfilteredThenApplyComplete {
                        late_apply: novarocks_physical_plan::LateApplyGranularity::Batch,
                    };
            }
        }
    }
}

impl ContractLoweringVisitor {
    fn new(
        version: PlanVersionId,
        dop_domain: PipelineDopDomain,
        provider_reads: Option<FinalizedProviderReadSet>,
    ) -> Self {
        Self {
            current_fragment: ROOT_FRAGMENT_ID,
            next_fragment: 1,
            exchange_hash_schemes: BTreeMap::new(),
            next_topn_sequence: 1,
            next_aggregate_sequence: 1,
            next_aggregate_call: 0,
            pending_aggregate_sequences: None,
            pending_aggregate_sequence_used: false,
            pending_topn_sequence: None,
            pending_topn_sequence_used: false,
            unstatable_runtime_filters: BTreeSet::new(),
            lambda_scope: Vec::new(),
            fragments: BTreeMap::from([(ROOT_FRAGMENT_ID, FragmentBuilder::new(ROOT_FRAGMENT_ID))]),
            completions: BTreeMap::new(),
            plan_version: version,
            plan_builder: PlanBuilder::new(version),
            artifact_refs: BTreeMap::new(),
            provider_partition_definitions: BTreeMap::new(),
            dop_domain,
            provider_reads,
            cte_producers: BTreeMap::new(),
            annotated_nodes: BTreeSet::new(),
            annotated_values: BTreeMap::new(),
            runtime_filter_builds: BTreeMap::new(),
            runtime_filter_probes: BTreeMap::new(),
            runtime_filter_attachments: BTreeSet::new(),
            edges: BTreeMap::new(),
        }
    }

    fn fragment_mut(&mut self) -> &mut FragmentBuilder {
        self.fragments
            .get_mut(&self.current_fragment)
            .expect("the current fragment is allocated before lowering")
    }

    /// The type a value declares in the fragment that defines it.
    fn value_declared_type_in(
        &mut self,
        fragment: FragmentId,
        value: ValueId,
    ) -> Result<ValueType, ContractLoweringError> {
        self.fragments
            .get_mut(&fragment)
            .and_then(|fragment| fragment.value(value))
            .map(|definition| definition.ty.clone())
            .ok_or(ContractLoweringError::IdentitySpaceExhausted("value"))
    }

    fn attach_runtime_filter(
        &mut self,
        fragment: FragmentId,
        filter: RuntimeFilterId,
    ) -> Result<(), ContractLoweringError> {
        if self.runtime_filter_attachments.insert((fragment, filter)) {
            self.fragments
                .get_mut(&fragment)
                .expect("runtime-filter endpoint fragment has been allocated")
                .attach_runtime_filter(filter)?;
        }
        Ok(())
    }

    fn register_provider_artifacts(
        &mut self,
        artifacts: &[SealedArtifactRef],
    ) -> Result<(), ContractLoweringError> {
        for artifact in artifacts {
            if let Some(existing) = self.artifact_refs.get(&artifact.id) {
                if existing != artifact {
                    return Err(ContractLoweringError::ProviderRead {
                        detail: format!(
                            "provider reads carry conflicting sealed artifact reference {}",
                            artifact.id.get()
                        ),
                    });
                }
                continue;
            }
            self.plan_builder.add_artifact_ref(artifact.clone())?;
            self.artifact_refs.insert(artifact.id, artifact.clone());
        }
        Ok(())
    }

    fn lower_provider_properties(
        &mut self,
        properties: &ProviderReadProperties,
        read: &ProviderReadReference,
        values_by_request_ordinal: &BTreeMap<u32, ValueId>,
    ) -> Result<PhysicalProperties, ContractLoweringError> {
        let values = |ordinals: &[u32]| {
            ordinals
                .iter()
                .map(|ordinal| {
                    values_by_request_ordinal.get(ordinal).copied().ok_or(
                        ContractLoweringError::ProviderRead {
                            detail: format!(
                                "provider property references unknown request ordinal {ordinal}"
                            ),
                        },
                    )
                })
                .collect::<Result<Vec<_>, _>>()
                .map(Vec::into_boxed_slice)
        };
        let distribution = match &properties.distribution {
            ProviderReadDistribution::Unconstrained => Distribution::Unconstrained,
            ProviderReadDistribution::Singleton => Distribution::Singleton,
            ProviderReadDistribution::RoundRobin => Distribution::RoundRobin,
            ProviderReadDistribution::Hash { keys, scheme } => Distribution::Hash {
                keys: values(keys)?,
                scheme: self.lower_provider_hash_scheme(read, scheme)?,
            },
            ProviderReadDistribution::BucketShuffle { keys, scheme } => {
                Distribution::BucketShuffle {
                    keys: values(keys)?,
                    scheme: self.lower_provider_bucket_scheme(read, scheme)?,
                }
            }
        };
        let ordering = properties
            .ordering
            .iter()
            .map(|key| {
                Ok(OrderingKey {
                    value: values_by_request_ordinal
                        .get(&key.request_ordinal)
                        .copied()
                        .ok_or(ContractLoweringError::ProviderRead {
                            detail: format!(
                                "provider ordering references unknown request ordinal {}",
                                key.request_ordinal
                            ),
                        })?,
                    direction: key.direction,
                    null_ordering: key.null_ordering,
                })
            })
            .collect::<Result<Vec<_>, ContractLoweringError>>()?
            .into_boxed_slice();
        Ok(PhysicalProperties {
            distribution,
            row_multiplicity: RowMultiplicity::SingleCopy,
            ordering,
        })
    }

    fn lower_provider_hash_scheme(
        &mut self,
        read: &ProviderReadReference,
        scheme: &ProviderHashPartitionScheme,
    ) -> Result<HashPartitionScheme, ContractLoweringError> {
        let space = PartitionSpaceId::try_new(provider_partition_identity_digest(
            b"novarocks.uea5.partition-space.v1",
            self.plan_version,
            read,
            &[scheme.space.as_bytes()],
        ))
        .map_err(|error| ContractLoweringError::InvalidPlanIdentity {
            detail: error.to_string(),
        })?;
        self.register_provider_partition_definition(
            space,
            ProviderPartitionDefinition::Hash(scheme.clone()),
        )?;
        let count = PartitionCountParameterId::try_new(provider_partition_identity_digest(
            b"novarocks.uea5.partition-count.v1",
            self.plan_version,
            read,
            &[scheme.space.as_bytes(), scheme.count.as_bytes()],
        ))
        .map_err(|error| ContractLoweringError::InvalidPlanIdentity {
            detail: error.to_string(),
        })?;
        Ok(HashPartitionScheme {
            space,
            count: PartitionCountParameter {
                id: count,
                admissible: PartitionCountDomain {
                    min: scheme.admissible.min,
                    max: scheme.admissible.max,
                    requires_power_of_two: scheme.admissible.requires_power_of_two,
                },
            },
            definition: HashDefinition {
                algorithm: scheme.algorithm,
            },
        })
    }

    fn lower_provider_bucket_scheme(
        &mut self,
        read: &ProviderReadReference,
        scheme: &ProviderBucketPartitionScheme,
    ) -> Result<novarocks_physical_plan::BucketPartitionScheme, ContractLoweringError> {
        let space = PartitionSpaceId::try_new(provider_partition_identity_digest(
            b"novarocks.uea5.partition-space.v1",
            self.plan_version,
            read,
            &[scheme.space.as_bytes()],
        ))
        .map_err(|error| ContractLoweringError::InvalidPlanIdentity {
            detail: error.to_string(),
        })?;
        self.register_provider_partition_definition(
            space,
            ProviderPartitionDefinition::Bucket(scheme.clone()),
        )?;
        Ok(novarocks_physical_plan::BucketPartitionScheme {
            space,
            bucket_count: scheme.bucket_count,
            hash: scheme.hash,
            layout: scheme.layout,
            ordinal_domain: BucketOrdinalDomainProof {
                first_ordinal: scheme.ordinal_domain.first_ordinal,
                ordinal_count: scheme.ordinal_domain.ordinal_count,
                evidence_digest: scheme.ordinal_domain.evidence_digest,
            },
        })
    }

    fn register_provider_partition_definition(
        &mut self,
        space: PartitionSpaceId,
        definition: ProviderPartitionDefinition,
    ) -> Result<(), ContractLoweringError> {
        if let Some(existing) = self.provider_partition_definitions.get(&space) {
            if existing != &definition {
                return Err(ContractLoweringError::ProviderRead {
                    detail: format!(
                        "provider partition token changes definition within plan space {:?}",
                        space
                    ),
                });
            }
            return Ok(());
        }
        self.provider_partition_definitions
            .insert(space, definition);
        Ok(())
    }

    fn complete_fragment(
        &mut self,
        fragment: FragmentId,
        root: NodeId,
        sink: FragmentSink,
    ) -> Result<(), ContractLoweringError> {
        if self.completions.insert(fragment, (root, sink)).is_some() {
            return Err(ContractLoweringError::DuplicateFragmentCompletion { fragment });
        }
        Ok(())
    }

    fn finish_draft(
        mut self,
        result_port: ResultPort,
    ) -> Result<PlanBuilder, ContractLoweringError> {
        self.plan_builder.set_result_port(result_port)?;
        let mut finished_fragments = BTreeMap::new();
        for (fragment_id, builder) in std::mem::take(&mut self.fragments) {
            let (root, sink) = self.completions.remove(&fragment_id).ok_or(
                ContractLoweringError::IncompleteFragment {
                    fragment: fragment_id,
                },
            )?;
            // A fragment that can only run one driver says so. The plan's
            // domain is what a fragment may widen to, and a fragment whose
            // root delivers one stream -- a metadata read the provider hands
            // to one reader, a gather, a finish -- never does.
            let dop_domain = fragment_dop_domain(&builder, root, self.dop_domain);
            let fragment = builder.finish_definition(root, sink, dop_domain)?;
            finished_fragments.insert(fragment_id, fragment);
        }
        if let Some((&fragment, _)) = self.completions.first_key_value() {
            return Err(ContractLoweringError::UnknownFragmentCompletion { fragment });
        }
        if let Some(reads) = self.provider_reads.take() {
            reads
                .ensure_consumed()
                .map_err(|error| ContractLoweringError::ProviderRead {
                    detail: error.to_string(),
                })?;
        }
        for filter in self.materialize_runtime_filters(&finished_fragments)? {
            self.plan_builder.add_runtime_filter(filter)?;
        }
        for fragment in finished_fragments.into_values() {
            self.plan_builder.add_fragment(fragment)?;
        }
        Ok(self.plan_builder)
    }

    fn materialize_runtime_filters(
        &self,
        fragments: &BTreeMap<FragmentId, Fragment>,
    ) -> Result<Vec<RuntimeFilter>, ContractLoweringError> {
        for id in self.runtime_filter_probes.keys() {
            if !self.runtime_filter_builds.contains_key(id) {
                return Err(ContractLoweringError::InvalidRuntimeFilter {
                    id: *id,
                    detail: "probe has no static producer".to_string(),
                });
            }
        }
        let mut filters = self
            .runtime_filter_builds
            .iter()
            .map(|(legacy_id, build)| {
                let probes = self.runtime_filter_probes.get(legacy_id).ok_or_else(|| {
                    ContractLoweringError::InvalidRuntimeFilter {
                        id: *legacy_id,
                        detail: "producer has no static consumer witness".to_string(),
                    }
                })?;
                materialize_runtime_filter(*legacy_id, build, probes, fragments, &self.edges)
            })
            .collect::<Result<Vec<_>, _>>()?;
        resolve_runtime_filter_activations(&mut filters, fragments, &self.edges);
        Ok(filters)
    }

    fn lower_node(
        &mut self,
        plan: &PhysicalPlanNode,
    ) -> Result<LoweredNode, ContractLoweringError> {
        let lowered = match &plan.kind {
            PhysicalPlanKind::Scan(scan) => self.lower_scan(plan, scan),
            PhysicalPlanKind::Values(values) => self.lower_values(plan, values),
            PhysicalPlanKind::Filter(filter) => self.lower_filter(plan, &filter.predicate),
            PhysicalPlanKind::Project(project) => self.lower_project(plan, &project.items),
            PhysicalPlanKind::Unpivot(unpivot) => self.lower_unpivot(plan, unpivot),
            PhysicalPlanKind::Limit(limit) => self.lower_limit(plan, limit),
            PhysicalPlanKind::Sort(sort) => self.lower_sort(plan, sort),
            PhysicalPlanKind::TopN(topn) => self.lower_topn(plan, topn),
            PhysicalPlanKind::AssertOneRow(assertion) => self.lower_assertion(plan, assertion),
            PhysicalPlanKind::GenerateSeries(series) => self.lower_generate_series(plan, series),
            PhysicalPlanKind::Repeat(repeat) => self.lower_repeat(plan, repeat),
            PhysicalPlanKind::Window(window) => self.lower_window(plan, window),
            PhysicalPlanKind::TableFunction(function) => self.lower_table_function(plan, function),
            PhysicalPlanKind::Redistribute(redistribute) => {
                self.lower_redistribute(plan, redistribute, None)
            }
            PhysicalPlanKind::HashJoin(join) => self.lower_hash_join(plan, join),
            PhysicalPlanKind::NestLoopJoin(join) => self.lower_nest_loop_join(plan, join),
            PhysicalPlanKind::SetOp(set_op) => self.lower_set_op(plan, set_op),
            PhysicalPlanKind::HashAggregate(aggregate) => {
                self.lower_hash_aggregate(plan, aggregate)
            }
            PhysicalPlanKind::CTEAnchor(anchor) => self.lower_cte_anchor(plan, anchor),
            PhysicalPlanKind::CTEProduce(_) => Err(ContractLoweringError::InvalidCte {
                detail: "CTEProduce is only valid as the first child of its CTEAnchor".into(),
            }),
            PhysicalPlanKind::CTEConsume(consume) => self.lower_cte_consume(plan, consume),
            PhysicalPlanKind::ChangeEventExpand(expand) => {
                self.lower_change_event_expand(plan, expand)
            }
        }?;
        self.record_runtime_filter_probes(plan, &lowered)?;
        self.annotate_node(plan, &lowered)?;
        Ok(lowered)
    }

    fn record_runtime_filter_probes(
        &mut self,
        plan: &PhysicalPlanNode,
        lowered: &LoweredNode,
    ) -> Result<(), ContractLoweringError> {
        for intent in &plan.probe_runtime_filters {
            if self.unstatable_runtime_filters.contains(&intent.filter_id) {
                continue;
            }
            let column = identity_column_ref(&intent.probe_expr).ok_or_else(|| {
                ContractLoweringError::InvalidRuntimeFilter {
                    id: intent.filter_id,
                    detail: format!(
                        "probe expression is {}, not one exact physical value",
                        expression_kind_name(&intent.probe_expr.kind)
                    ),
                }
            })?;
            let value = lowered.columns.get(&column).copied().ok_or_else(|| {
                ContractLoweringError::InvalidRuntimeFilter {
                    id: intent.filter_id,
                    detail: format!("probe ColumnId {} has no lowered ValueId", column.0),
                }
            })?;
            self.attach_runtime_filter(lowered.fragment, runtime_filter_id(intent.filter_id)?)?;
            self.runtime_filter_probes
                .entry(intent.filter_id)
                .or_default()
                .push(PendingRuntimeFilterProbe {
                    fragment: lowered.fragment,
                    node: lowered.node,
                    value,
                    scan_source: matches!(plan.kind, PhysicalPlanKind::Scan(_)),
                });
        }
        Ok(())
    }

    fn annotate_node(
        &mut self,
        plan: &PhysicalPlanNode,
        lowered: &LoweredNode,
    ) -> Result<(), ContractLoweringError> {
        if !self
            .annotated_nodes
            .insert((lowered.fragment, lowered.node))
        {
            return Ok(());
        }
        let subject =
            novarocks_physical_plan::AnnotationSubject::Node(lowered.fragment, lowered.node);
        let value = match &plan.stats.cost_estimate {
            Some(cost) => format!(
                "rows={}, cpu={}, memory={}, network={}",
                plan.stats.output_row_count, cost.cpu_cost, cost.memory_cost, cost.network_cost
            ),
            None => format!("rows={}", plan.stats.output_row_count),
        };
        self.plan_builder.add_annotation(PlanAnnotation {
            subject,
            key: "optimizer.statistics".into(),
            value: value.into_boxed_str(),
        });
        if let Some(decision) = &plan.stats.broadcast_decision {
            self.plan_builder.add_annotation(PlanAnnotation {
                subject,
                key: "optimizer.broadcast".into(),
                value: format!(
                    "verdict={}, forced={}, build_bytes={}, hash_table_bytes={}, backends={}, fanout_bytes={}, per_node_budget_bytes={}, risk_multiplier={}",
                    if decision.feasible { "feasible" } else { "infeasible" },
                    decision.forced,
                    decision.build_bytes,
                    decision.hash_table_bytes,
                    decision.effective_backend_count,
                    decision.risk_adj_fanout_bytes,
                    decision.per_node_budget_bytes,
                    decision.risk_multiplier
                )
                .into_boxed_str(),
            });
        }
        if let PhysicalPlanKind::Scan(scan) = &plan.kind {
            let relation = match &scan.alias {
                Some(alias) => format!("{}.{} (alias={alias})", scan.database, scan.table.name),
                None => format!("{}.{}", scan.database, scan.table.name),
            };
            self.plan_builder.add_annotation(PlanAnnotation {
                subject,
                key: "sql.relation".into(),
                value: relation.into_boxed_str(),
            });
            if let Some(materialized_view) = &scan.mv_rewritten_from {
                // Two facts, two annotations. `rewritten with mv:` promises
                // the view the scan reads, and a reader who wants to know
                // which view answered their query has nowhere else to look
                // for it; the publication provenance is what pins that answer
                // to an exact published state, and it belongs on its own line
                // rather than standing where the name was promised.
                self.plan_builder.add_annotation(PlanAnnotation {
                    subject,
                    key: "sql.mv_rewritten_from".into(),
                    value: materialized_view.name().into(),
                });
                self.plan_builder.add_annotation(PlanAnnotation {
                    subject,
                    key: "sql.mv_rewrite_provenance".into(),
                    value: mv_rewrite_provenance_annotation(materialized_view)?,
                });
            }
        }
        self.annotate_output_display_names(lowered);
        Ok(())
    }

    /// Publish a value's frozen display name where that value is introduced.
    /// An ExchangeImport is a new identity even when its SQL column is unchanged.
    fn annotate_output_display_names(&mut self, lowered: &LoweredNode) {
        for (value, display_name) in lowered.output.iter().zip(&lowered.display_names) {
            if let std::collections::btree_map::Entry::Vacant(entry) =
                self.annotated_values.entry((lowered.fragment, *value))
            {
                entry.insert(display_name.clone());
                self.plan_builder.add_annotation(PlanAnnotation {
                    subject: novarocks_physical_plan::AnnotationSubject::Value(
                        lowered.fragment,
                        *value,
                    ),
                    key: "sql.display_name".into(),
                    value: bounded_display_name(display_name),
                });
            }
        }
    }

    fn allocate_fragment(&mut self) -> Result<FragmentId, ContractLoweringError> {
        let id = FragmentId::new(self.next_fragment);
        self.next_fragment = self
            .next_fragment
            .checked_add(1)
            .ok_or(ContractLoweringError::IdentitySpaceExhausted("fragment"))?;
        self.fragments.insert(id, FragmentBuilder::new(id));
        Ok(id)
    }

    fn lower_change_event_expand(
        &mut self,
        plan: &PhysicalPlanNode,
        expand: &crate::planner::physical::DistributedChangeEventExpandNode,
    ) -> Result<LoweredNode, ContractLoweringError> {
        expect_children(plan, 1)?;
        require_output_shape(
            "ChangeEventExpand",
            &plan.output_columns,
            &expand.output_columns,
        )?;
        let child = self.lower_node(&plan.children[0])?;
        require_single_copy_input("ChangeEventExpand", &child.properties)?;
        let node = self.fragment_mut().reserve_node_id()?;
        let mut output = Vec::with_capacity(plan.output_columns.len());
        let mut columns = BTreeMap::new();
        for (ordinal, column) in plan.output_columns.iter().enumerate() {
            let value = self.fragment_mut().add_value(
                value_type(column),
                ValueOrigin::NodeOutput {
                    node,
                    output_ordinal: checked_ordinal("ChangeEventExpand output", ordinal)?,
                },
            )?;
            if columns.insert(column.column_id, value).is_some() {
                return Err(ContractLoweringError::DuplicateColumnDefinition {
                    node: "ChangeEventExpand",
                    column: column.column_id,
                });
            }
            output.push(value);
        }
        let effect_ordinal = plan
            .output_columns
            .iter()
            .position(|column| column.column_id == expand.effect_column_id)
            .ok_or_else(|| invalid_write("change-event effect column is absent".into()))?;
        let effect_output = output[effect_ordinal];
        let events = expand
            .events
            .iter()
            .map(|event| {
                let predicate = event
                    .predicate
                    .as_ref()
                    .map(|predicate| self.lower_expression(node, predicate, &child.columns))
                    .transpose()?;
                let mut assigned = BTreeSet::new();
                let assignments = event
                    .assignments
                    .iter()
                    .map(|assignment| {
                        if assignment.output_column_id == expand.effect_column_id
                            || !assigned.insert(assignment.output_column_id)
                        {
                            return Err(invalid_write(
                                "change-event assignment repeats or targets the effect output"
                                    .into(),
                            ));
                        }
                        let value = columns
                            .get(&assignment.output_column_id)
                            .copied()
                            .ok_or_else(|| {
                                invalid_write(
                                    "change-event assignment targets an unknown output".into(),
                                )
                            })?;
                        let expression = assignment
                            .expr
                            .as_ref()
                            .map(|expression| {
                                self.lower_expression(node, expression, &child.columns)
                            })
                            .transpose()?;
                        Ok((value, expression))
                    })
                    .collect::<Result<Vec<_>, ContractLoweringError>>()?;
                Ok(ChangeEventSpec {
                    predicate,
                    effect: event.effect,
                    assignments: assignments.into_boxed_slice(),
                })
            })
            .collect::<Result<Vec<_>, ContractLoweringError>>()?;
        self.fragment_mut().add_row_rewriting(
            node,
            child.node,
            None,
            output.clone().into_boxed_slice(),
            NodeKind::ChangeEventExpand {
                events: events.into_boxed_slice(),
                effect_output,
            },
        )?;
        let properties = self
            .fragment_mut()
            .node_output_properties(node)
            .expect("the node was just inserted")
            .clone();
        Ok(LoweredNode {
            fragment: self.current_fragment,
            node,
            output: output.into_boxed_slice(),
            columns,
            properties,
            display_names: plan
                .output_columns
                .iter()
                .map(|column| column.name.clone())
                .collect::<Vec<_>>()
                .into_boxed_slice(),
        })
    }

    fn lower_cte_anchor(
        &mut self,
        plan: &PhysicalPlanNode,
        anchor: &crate::planner::payload::PlanCTEAnchorNode,
    ) -> Result<LoweredNode, ContractLoweringError> {
        expect_children(plan, 2)?;
        let produce_plan = &plan.children[0];
        let PhysicalPlanKind::CTEProduce(produce) = &produce_plan.kind else {
            return Err(ContractLoweringError::InvalidCte {
                detail: "CTEAnchor first child is not CTEProduce".into(),
            });
        };
        if anchor.cte_id != produce.cte_id {
            return Err(ContractLoweringError::InvalidCte {
                detail: format!(
                    "CTEAnchor id {} differs from producer id {}",
                    anchor.cte_id, produce.cte_id
                ),
            });
        }
        if self.cte_producers.contains_key(&anchor.cte_id) {
            return Err(ContractLoweringError::InvalidCte {
                detail: format!("duplicate active CTE definition {}", anchor.cte_id),
            });
        }
        expect_children(produce_plan, 1)?;
        require_output_shape(
            "CTEProduce",
            &produce_plan.output_columns,
            &produce.output_columns,
        )?;
        require_output_shape(
            "CTEProduce child",
            &produce.output_columns,
            &produce_plan.children[0].output_columns,
        )?;

        let destination = self.current_fragment;
        let producer_fragment = self.allocate_fragment()?;
        self.current_fragment = producer_fragment;
        let producer_root = self.lower_node(&produce_plan.children[0])?;
        if producer_root.fragment != producer_fragment {
            return Err(ContractLoweringError::UnexpectedFragment {
                node: "CTEProduce",
                expected: producer_fragment,
                actual: producer_root.fragment,
            });
        }
        require_single_copy_input("CTEProduce", &producer_root.properties)?;
        if producer_root.output.len() != produce.output_columns.len() {
            return Err(ContractLoweringError::ArityMismatch {
                context: "CTEProduce output",
                expected: produce.output_columns.len(),
                actual: producer_root.output.len(),
            });
        }
        let mut outputs = BTreeMap::new();
        for (column, value) in produce.output_columns.iter().zip(&producer_root.output) {
            if outputs
                .insert(column.column_id, (*value, value_type(column)))
                .is_some()
            {
                return Err(ContractLoweringError::InvalidCte {
                    detail: format!(
                        "CTE producer {} repeats output column {}",
                        anchor.cte_id, column.column_id
                    ),
                });
            }
        }
        self.cte_producers.insert(
            anchor.cte_id,
            CteProducer {
                fragment: producer_fragment,
                root: producer_root.node,
                outputs,
                row_multiplicity: producer_root.properties.row_multiplicity,
                edges: Vec::new(),
            },
        );

        self.current_fragment = destination;
        let mut body = self.lower_node(&plan.children[1])?;
        require_output_shape(
            "CTEAnchor",
            &plan.output_columns,
            &plan.children[1].output_columns,
        )?;
        let producer = self
            .cte_producers
            .remove(&anchor.cte_id)
            .expect("the active CTE producer was inserted before lowering its body");
        if producer.edges.is_empty() {
            return Err(ContractLoweringError::InvalidCte {
                detail: format!("CTE producer {} has no consumers", anchor.cte_id),
            });
        }
        self.complete_fragment(
            producer.fragment,
            producer.root,
            FragmentSink::Multicast {
                edges: producer.edges.into_boxed_slice(),
            },
        )?;
        body.display_names = plan
            .output_columns
            .iter()
            .map(|column| column.name.clone())
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Ok(body)
    }

    fn lower_cte_consume(
        &mut self,
        plan: &PhysicalPlanNode,
        consume: &crate::planner::payload::PlanCTEConsumeNode,
    ) -> Result<LoweredNode, ContractLoweringError> {
        expect_children(plan, 0)?;
        require_output_shape("CTEConsume", &plan.output_columns, &consume.output_columns)?;
        if consume.output_columns.len() != consume.producer_column_ids.len() {
            return Err(ContractLoweringError::ArityMismatch {
                context: "CTEConsume producer mapping",
                expected: consume.output_columns.len(),
                actual: consume.producer_column_ids.len(),
            });
        }
        let mut output_ids = BTreeSet::new();
        for column in &consume.output_columns {
            if !output_ids.insert(column.column_id) {
                return Err(ContractLoweringError::InvalidCte {
                    detail: format!(
                        "CTE consumer {} repeats output column {}",
                        consume.cte_id, column.column_id
                    ),
                });
            }
        }
        let producer = self.cte_producers.get(&consume.cte_id).ok_or_else(|| {
            ContractLoweringError::InvalidCte {
                detail: format!(
                    "CTE consumer references unknown or out-of-scope producer {}",
                    consume.cte_id
                ),
            }
        })?;
        let producer_fragment = producer.fragment;
        let producer_multiplicity = producer.row_multiplicity;
        let selected_outputs = consume
            .output_columns
            .iter()
            .zip(&consume.producer_column_ids)
            .enumerate()
            .map(|(ordinal, (consumer_column, producer_column_id))| {
                let (producer_value, producer_type) = producer
                    .outputs
                    .get(producer_column_id)
                    .ok_or_else(|| ContractLoweringError::InvalidCte {
                        detail: format!(
                            "CTE consumer {} references missing producer column {} at ordinal {}",
                            consume.cte_id, producer_column_id, ordinal
                        ),
                    })?;
                let consumer_type = value_type(consumer_column);
                if &consumer_type != producer_type {
                    return Err(ContractLoweringError::OutputColumnMismatch {
                        node: "CTEConsume",
                        ordinal,
                        detail: format!(
                            "producer column {} has type {:?}, consumer output {} has type {:?}",
                            producer_column_id,
                            producer_type,
                            consumer_column.column_id,
                            consumer_type
                        ),
                    });
                }
                Ok((*producer_value, consumer_type))
            })
            .collect::<Result<Vec<_>, _>>()?;

        let destination = self.current_fragment;
        if destination == producer_fragment {
            return Err(ContractLoweringError::InvalidCte {
                detail: format!(
                    "CTE consumer {} cannot receive in its producer fragment",
                    consume.cte_id
                ),
            });
        }
        let edge = self.plan_builder.reserve_edge_id()?;
        let receiver = self.fragment_mut().reserve_node_id()?;
        let mut projection = Vec::with_capacity(consume.output_columns.len());
        let mut receive_mapping = Vec::with_capacity(consume.output_columns.len());
        let mut output = Vec::with_capacity(consume.output_columns.len());
        let mut columns = BTreeMap::new();
        for (consumer_column, (producer_value, consumer_type)) in
            consume.output_columns.iter().zip(selected_outputs)
        {
            let imported = self.fragment_mut().add_value(
                consumer_type,
                ValueOrigin::CteImport {
                    edge,
                    producer_fragment,
                    producer_value,
                },
            )?;
            projection.push(producer_value);
            receive_mapping.push((producer_value, imported));
            output.push(imported);
            columns.insert(consumer_column.column_id, imported);
        }
        self.fragment_mut().add_exchange_source(
            receiver,
            edge,
            receive_mapping.clone().into_boxed_slice(),
            output.clone().into_boxed_slice(),
            Distribution::Unconstrained,
            producer_multiplicity,
        )?;
        let properties = self
            .fragment_mut()
            .node_output_properties(receiver)
            .expect("the exchange source was just inserted")
            .clone();
        let edge_contract = Edge {
            id: edge,
            kind: EdgeKind::CteMulticast,
            source: EdgeSource {
                fragment: producer_fragment,
                projection: projection.into_boxed_slice(),
            },
            destination: EdgeDestination {
                fragment: destination,
                node: receiver,
                receive_mapping: receive_mapping.into_boxed_slice(),
            },
            partitioning: EdgePartitioning {
                source: Distribution::Unconstrained,
                source_multiplicity: producer_multiplicity,
                destination: Distribution::Unconstrained,
                destination_multiplicity: producer_multiplicity,
            },
        };
        self.plan_builder.add_edge(edge_contract.clone())?;
        self.edges.insert(edge, edge_contract);
        self.cte_producers
            .get_mut(&consume.cte_id)
            .expect("the CTE producer stays active through body lowering")
            .edges
            .push(edge);
        Ok(LoweredNode {
            fragment: destination,
            node: receiver,
            output: output.into_boxed_slice(),
            columns,
            properties,
            display_names: consume
                .output_columns
                .iter()
                .map(|column| column.name.clone())
                .collect::<Vec<_>>()
                .into_boxed_slice(),
        })
    }

    /// The hash partition space a fragment receives in.
    ///
    /// A space is one placement rule -- one hash function over one destination
    /// count -- and the keys fed to it are stated beside it, not in it. Every
    /// exchange into one fragment divides the same hash by the same count, so
    /// they are one space, and two inputs that arrived through different
    /// exchanges are co-partitioned exactly when they were hashed on the keys
    /// being compared. Minting a space per exchange instead would have made
    /// that depend on the order the lowering happened to visit them in.
    fn allocate_hash_scheme(&mut self) -> Result<HashPartitionScheme, ContractLoweringError> {
        let destination = self.current_fragment;
        if let Some(scheme) = self.exchange_hash_schemes.get(&destination) {
            return Ok(scheme.clone());
        }
        let destination_bytes = destination.get().to_be_bytes();
        let space_bytes = partition_identity_digest(
            b"novarocks.uea5.partition-space.v1",
            self.plan_version,
            &[b"sql-exchange", &destination_bytes],
        );
        let count_bytes = partition_identity_digest(
            b"novarocks.uea5.partition-count.v1",
            self.plan_version,
            &[b"sql-exchange", &destination_bytes],
        );
        let scheme = HashPartitionScheme {
            space: PartitionSpaceId::try_new(space_bytes).map_err(|error| {
                ContractLoweringError::InvalidPlanIdentity {
                    detail: error.to_string(),
                }
            })?,
            count: PartitionCountParameter {
                id: PartitionCountParameterId::try_new(count_bytes).map_err(|error| {
                    ContractLoweringError::InvalidPlanIdentity {
                        detail: error.to_string(),
                    }
                })?,
                admissible: PartitionCountDomain {
                    min: self.dop_domain.min,
                    max: self.dop_domain.max,
                    requires_power_of_two: self.dop_domain.requires_power_of_two,
                },
            },
            definition: HashDefinition::native_exchange(),
        };
        self.exchange_hash_schemes
            .insert(destination, scheme.clone());
        Ok(scheme)
    }

    fn allocate_topn_sequence(&mut self) -> Result<TopNSequenceId, ContractLoweringError> {
        let sequence = TopNSequenceId::new(self.next_topn_sequence);
        self.next_topn_sequence = self.next_topn_sequence.checked_add(1).ok_or(
            ContractLoweringError::IdentitySpaceExhausted("TopN sequence"),
        )?;
        Ok(sequence)
    }

    fn lower_scan(
        &mut self,
        plan: &PhysicalPlanNode,
        scan: &crate::planner::physical::PhysicalScanNode,
    ) -> Result<LoweredNode, ContractLoweringError> {
        expect_children(plan, 0)?;
        let crate::planner::table::ScanSource::Sql(source) = &scan.table.source;
        let scan_occurrence =
            scan.provider_read_occurrence()
                .ok_or(ContractLoweringError::MissingPlannerFact {
                    node: "Scan",
                    fact: "provider read occurrence",
                })?;
        let FinalizedProviderRead {
            binding,
            contract,
            read_budget,
        } = self
            .provider_reads
            .as_mut()
            .ok_or(ContractLoweringError::MissingPlannerFact {
                node: "Scan",
                fact: "finalized provider read side table",
            })?
            .take(source.binding, scan_occurrence)
            .map_err(|error| ContractLoweringError::ProviderRead {
                detail: error.to_string(),
            })?;
        if binding != source.binding || contract.sql_binding != source.binding {
            return Err(ContractLoweringError::ProviderRead {
                detail: format!(
                    "scan occurrence {} provider contract carries a different SQL binding",
                    scan_occurrence.get()
                ),
            });
        }
        let mut variant_by_output = BTreeMap::new();
        for descriptor in &scan.variant_columns {
            if descriptor.synthetic_column_id == descriptor.source_column_id
                || variant_by_output
                    .insert(descriptor.synthetic_column_id, descriptor)
                    .is_some()
            {
                return Err(ContractLoweringError::InvalidFunctionBinding {
                    detail:
                        "scan VARIANT descriptor has a repeated or self-referential output identity"
                            .to_string(),
                });
            }
        }
        let provider_columns = plan
            .output_columns
            .iter()
            .filter(|column| !variant_by_output.contains_key(&column.column_id))
            .collect::<Vec<_>>();
        if contract.schema.len() != provider_columns.len() {
            return Err(ContractLoweringError::ArityMismatch {
                context: "Scan provider projection",
                expected: provider_columns.len(),
                actual: contract.schema.len(),
            });
        }

        let node = self.fragment_mut().reserve_node_id()?;
        let mut output = Vec::with_capacity(plan.output_columns.len());
        let mut columns = BTreeMap::new();
        let mut provider_outputs = Vec::with_capacity(contract.schema.len());
        let mut relation_schema = Vec::with_capacity(contract.schema.len());
        let mut provider_values_by_request_ordinal = BTreeMap::new();
        for (ordinal, (column, field)) in provider_columns
            .iter()
            .copied()
            .zip(contract.schema.iter())
            .enumerate()
        {
            let request_ordinal =
                u32::try_from(ordinal).map_err(|_| ContractLoweringError::OrdinalOverflow {
                    context: "Scan provider projection",
                    ordinal,
                })?;
            if field.request_ordinal() != request_ordinal {
                return Err(ContractLoweringError::ProviderRead {
                    detail: format!(
                        "scan provider field {} carries request ordinal {}",
                        ordinal,
                        field.request_ordinal()
                    ),
                });
            }
            // The provider names its own nested fields; the plan states the
            // type without that decoration, and this is where the two meet.
            let engine_type = ValueType::new(
                novarocks_types::undecorated_nested_type(&field.engine_type().data_type),
                field.engine_type().nullable,
            );
            if engine_type != value_type(column) {
                return Err(ContractLoweringError::OutputColumnMismatch {
                    node: "Scan",
                    ordinal,
                    detail: format!(
                        "provider field {:?} differs from the published {:?}",
                        engine_type,
                        value_type(column)
                    ),
                });
            }
            let provider_column = field.column().clone();
            let value = self.fragment_mut().add_value(
                engine_type.clone(),
                ValueOrigin::ProviderField {
                    scan_node: node,
                    field: provider_column.clone(),
                },
            )?;
            if provider_values_by_request_ordinal
                .insert(field.request_ordinal(), value)
                .is_some()
            {
                return Err(ContractLoweringError::ProviderRead {
                    detail: format!(
                        "scan provider contract repeats request ordinal {}",
                        field.request_ordinal()
                    ),
                });
            }
            insert_output_column("Scan", ordinal, column, value, &mut columns)?;
            provider_outputs.push((provider_column.clone(), value));
            relation_schema.push(RelationField {
                column: provider_column,
                ty: engine_type,
            });
        }

        let mut derived_values = Vec::with_capacity(scan.variant_columns.len());
        for descriptor in &scan.variant_columns {
            let output_ordinal = plan
                .output_columns
                .iter()
                .position(|column| column.column_id == descriptor.synthetic_column_id)
                .ok_or(ContractLoweringError::UnknownColumnReference(
                    descriptor.synthetic_column_id,
                ))?;
            let output_column = &plan.output_columns[output_ordinal];
            if output_column.name != descriptor.synthetic_column
                || output_column.data_type != descriptor.requested_type
                || !output_column.nullable
            {
                return Err(ContractLoweringError::OutputColumnMismatch {
                    node: "Scan",
                    ordinal: output_ordinal,
                    detail: "derived VARIANT output differs from its exact descriptor".to_string(),
                });
            }
            let source_column = plan
                .output_columns
                .iter()
                .find(|column| column.column_id == descriptor.source_column_id)
                .ok_or(ContractLoweringError::UnknownColumnReference(
                    descriptor.source_column_id,
                ))?;
            if source_column.name != descriptor.source_column {
                return Err(ContractLoweringError::OutputColumnMismatch {
                    node: "Scan",
                    ordinal: output_ordinal,
                    detail: "derived VARIANT source display identity differs from its descriptor"
                        .to_string(),
                });
            }
            let source_value = columns.get(&descriptor.source_column_id).copied().ok_or(
                ContractLoweringError::UnknownColumnReference(descriptor.source_column_id),
            )?;
            let binding = &descriptor.binding;
            let expected_result = value_type(output_column);
            let expected_arguments = Box::from([
                FunctionArgumentType::Value(value_type(source_column)),
                FunctionArgumentType::Value(ValueType::new(DataType::Utf8, false)),
                FunctionArgumentType::Value(ValueType::new(DataType::Utf8, false)),
            ]);
            if novarocks_types::value::variant::variant_get_target_type(
                &descriptor.requested_type_literal,
            )
            .as_ref()
                != Ok(&descriptor.requested_type)
            {
                return Err(ContractLoweringError::InvalidFunctionBinding {
                    detail: format!(
                        "derived VARIANT output {} has inconsistent requested type metadata",
                        descriptor.synthetic_column_id
                    ),
                });
            }
            if binding.kind != novarocks_physical_plan::FunctionKind::Scalar
                || binding.logical_argument_count != 3
                || binding.selected.argument_types != expected_arguments
                || binding.selected.aggregate.is_some()
                || binding.selected.result_type
                    != novarocks_functions::FunctionResultType::Scalar(expected_result.clone())
            {
                return Err(ContractLoweringError::InvalidFunctionBinding {
                    detail: format!(
                        "derived VARIANT output {} has an inconsistent exact scalar binding",
                        descriptor.synthetic_column_id
                    ),
                });
            }
            let source_expression = self.fragment_mut().add_expression(
                node,
                value_type(source_column),
                ContractExprKind::Value(source_value),
            )?;
            let path_expression = self.fragment_mut().add_expression(
                node,
                ValueType::new(DataType::Utf8, false),
                ContractExprKind::Literal(ContractLiteralValue::Utf8(
                    descriptor.canonical_path.clone().into_boxed_str(),
                )),
            )?;
            let type_expression = self.fragment_mut().add_expression(
                node,
                ValueType::new(DataType::Utf8, false),
                ContractExprKind::Literal(ContractLiteralValue::Utf8(
                    descriptor.requested_type_literal.clone().into_boxed_str(),
                )),
            )?;
            let expression = self.fragment_mut().add_expression(
                node,
                expected_result.clone(),
                ContractExprKind::FunctionCall {
                    function: bound_function_from_resolved(binding, &expected_result),
                    args: Box::from([source_expression, path_expression, type_expression]),
                },
            )?;
            let value = self.fragment_mut().add_value(
                expected_result,
                ValueOrigin::Expr {
                    node,
                    expr: expression,
                },
            )?;
            insert_output_column("Scan", output_ordinal, output_column, value, &mut columns)?;
            derived_values.push(value);
        }
        for (ordinal, column) in plan.output_columns.iter().enumerate() {
            output.push(columns.get(&column.column_id).copied().ok_or(
                ContractLoweringError::OutputColumnMismatch {
                    node: "Scan",
                    ordinal,
                    detail:
                        "output has neither a provider field nor a derived expression".to_string(),
                },
            )?);
        }

        let mut predicate_expressions = Vec::with_capacity(scan.predicates.len());
        for predicate in &scan.predicates {
            if predicate.data_type != DataType::Boolean {
                return Err(ContractLoweringError::PredicateIsNotBoolean {
                    actual: predicate.data_type.clone(),
                });
            }
            predicate_expressions.push(self.lower_expression(node, predicate, &columns)?);
        }
        let mut guarantee_by_occurrence = BTreeMap::new();
        for predicate in &contract.predicates {
            let occurrence = predicate.occurrence().get();
            if guarantee_by_occurrence
                .insert(occurrence, predicate.guarantee())
                .is_some()
            {
                return Err(ContractLoweringError::ProviderRead {
                    detail: format!(
                        "scan provider contract repeats predicate occurrence {occurrence}"
                    ),
                });
            }
        }
        let mut guarantees = Vec::with_capacity(guarantee_by_occurrence.len());
        let mut residuals = Vec::new();
        for (ordinal, expression) in predicate_expressions.iter().copied().enumerate() {
            let occurrence =
                u32::try_from(ordinal).map_err(|_| ContractLoweringError::OrdinalOverflow {
                    context: "Scan predicate occurrence",
                    ordinal,
                })?;
            match guarantee_by_occurrence.remove(&occurrence) {
                Some(kind) => {
                    guarantees.push(PredicateGuarantee {
                        predicate: expression,
                        kind,
                    });
                    if kind == PredicateGuaranteeKind::PruningOnly {
                        residuals.push(expression);
                    }
                }
                None => residuals.push(expression),
            }
        }
        if let Some((occurrence, _)) = guarantee_by_occurrence.first_key_value() {
            return Err(ContractLoweringError::ProviderRead {
                detail: format!(
                    "scan provider contract references unknown predicate occurrence {occurrence}"
                ),
            });
        }

        let properties = self.lower_provider_properties(
            &contract.provided_properties,
            &contract.read,
            &provider_values_by_request_ordinal,
        )?;
        self.register_provider_artifacts(&contract.artifact_refs)?;
        let relation = match contract.request.relation() {
            ProviderReadRelationNeed::Metadata { kind, .. } => {
                Relation::Metadata(MetadataRelation {
                    kind: metadata_relation_kind(*kind)?,
                    read: contract.read,
                    work_source: contract.work_source,
                    selection_digest: contract.selection_digest,
                    schema: relation_schema.into_boxed_slice(),
                    predicate_guarantees: guarantees.into_boxed_slice(),
                    provided_properties: properties.clone(),
                    coverage_evidence: contract.coverage_evidence,
                    artifact_inputs: contract.artifact_inputs,
                })
            }
            _ => Relation::Data(DataRelation {
                read: contract.read,
                work_source: contract.work_source,
                selection_digest: contract.selection_digest,
                schema: relation_schema.into_boxed_slice(),
                predicate_guarantees: guarantees.into_boxed_slice(),
                provided_properties: properties.clone(),
                artifact_inputs: contract.artifact_inputs,
            }),
        };
        self.fragment_mut().add_scan(
            node,
            NodeKind::Scan {
                occurrence: scan_occurrence,
                relation: Box::new(relation),
                read_budget,
                provider_outputs: provider_outputs.into_boxed_slice(),
                residuals: residuals.into_boxed_slice(),
                derived_values: derived_values.into_boxed_slice(),
            },
            output.clone().into_boxed_slice(),
        )?;
        // A scan's columns are called what the statement calls them. Where
        // the statement gave the relation a name, that name is part of it: a
        // self-join reads as `a.k = b.k` only because each side says which one
        // it is. Where it gave none, there is nothing to say and the column
        // is called what it is called.
        Ok(LoweredNode {
            fragment: self.current_fragment,
            node,
            output: output.into_boxed_slice(),
            columns,
            properties,
            display_names: plan
                .output_columns
                .iter()
                .map(|column| match &scan.alias {
                    Some(alias) if !column.is_internal => format!("{alias}.{}", column.name),
                    _ => column.name.clone(),
                })
                .collect::<Vec<_>>()
                .into_boxed_slice(),
        })
    }

    fn allocate_aggregate_sequence(
        &mut self,
    ) -> Result<AggregateSequenceId, ContractLoweringError> {
        let sequence = AggregateSequenceId::new(self.next_aggregate_sequence);
        self.next_aggregate_sequence = self.next_aggregate_sequence.checked_add(1).ok_or(
            ContractLoweringError::IdentitySpaceExhausted("aggregate sequence"),
        )?;
        Ok(sequence)
    }

    fn allocate_aggregate_call(&mut self) -> Result<AggregateCallId, ContractLoweringError> {
        let call = AggregateCallId::new(self.next_aggregate_call);
        self.next_aggregate_call = self.next_aggregate_call.checked_add(1).ok_or(
            ContractLoweringError::IdentitySpaceExhausted("aggregate call"),
        )?;
        Ok(call)
    }

    fn allocate_writer_aggregate_sequences(
        &mut self,
        auxiliary: &WriterAuxiliaryPlan,
    ) -> Result<BTreeMap<u32, AggregateSequenceId>, ContractLoweringError> {
        auxiliary
            .final_plan()
            .calls()
            .iter()
            .map(|call| {
                Ok((
                    call.intermediate_input_slot_id(),
                    self.allocate_aggregate_sequence()?,
                ))
            })
            .collect()
    }

    fn lower_table_writer(
        &mut self,
        source: LoweredNode,
        write: SqlWritePlanInput,
        ordinal: WriteTargetOrdinal,
        auxiliary: &WriterAuxiliaryPlan,
        sequences: &BTreeMap<u32, AggregateSequenceId>,
        handle: novarocks_spi::connector::ConnectorEncodedPayload,
    ) -> Result<LoweredNode, ContractLoweringError> {
        if source.fragment != self.current_fragment {
            return Err(ContractLoweringError::UnexpectedFragment {
                node: "TableWriter",
                expected: self.current_fragment,
                actual: source.fragment,
            });
        }
        require_single_copy_input("TableWriter", &source.properties)?;
        if source.properties.distribution == Distribution::Broadcast {
            return Err(invalid_write(
                "table writer cannot consume a replicated input distribution".to_string(),
            ));
        }
        let projected = self.lower_writer_projection(source, &write)?;
        if projected.output.len() != write.contract.target.fields.len()
            || projected.output.len() != write.contract.input_columns.len()
        {
            return Err(invalid_write(format!(
                "write target/input/projected arity differs: target={}, input={}, projected={}",
                write.contract.target.fields.len(),
                write.contract.input_columns.len(),
                projected.output.len()
            )));
        }

        let node = self.fragment_mut().reserve_node_id()?;
        let relation = self.writer_relation_schema(
            node,
            auxiliary.schema().contract_version(),
            auxiliary.schema().arrow_schema().fields(),
            &auxiliary.schema().slot_ids(),
        )?;
        let LoweredWriterRelation {
            schema: output_schema,
            output,
            values_by_slot: output_slots,
        } = relation;
        if output_schema.revision != WRITER_MULTIPLEX_SCHEMA_REVISION {
            return Err(invalid_write(
                "writer multiplex schema revision differs".into(),
            ));
        }

        let target_fields = write
            .contract
            .target
            .fields
            .iter()
            .zip(&write.contract.input_columns)
            .zip(projected.output.iter())
            .enumerate()
            .map(|(field_ordinal, ((target, input), value))| {
                if target.column.data_type != input.data_type
                    || target.column.nullable != input.nullable
                {
                    return Err(invalid_write(format!(
                        "write target field {field_ordinal} type differs from its input contract"
                    )));
                }
                Ok(WriterTargetField {
                    token: target.token,
                    input: *value,
                    // Stated in the plan's own vocabulary, like every other
                    // type it carries: the provider's decoration belongs to
                    // the contract the field was frozen from, not to the plan.
                    ty: column_value_type(input),
                    hidden: target.is_hidden,
                })
            })
            .collect::<Result<Vec<_>, _>>()?;

        let partial = auxiliary.partial_for(ordinal).map_err(invalid_write)?;
        let partial_aggregates = partial
            .calls()
            .iter()
            .map(|call| {
                let input_index = usize::try_from(call.input_slot_id().saturating_sub(1))
                    .map_err(|_| invalid_write("writer input slot is outside host range".into()))?;
                if call.input_slot_id() == 0 {
                    return Err(invalid_write("writer input slot is zero".into()));
                }
                let input = projected.output.get(input_index).copied().ok_or_else(|| {
                    invalid_write(format!(
                        "writer aggregate input slot {} is outside the exact input",
                        call.input_slot_id()
                    ))
                })?;
                let output = output_slots
                    .get(&call.intermediate_slot_id())
                    .copied()
                    .ok_or_else(|| {
                        invalid_write("writer aggregate output slot is unknown".into())
                    })?;
                let sequence = sequences
                    .get(&call.intermediate_slot_id())
                    .copied()
                    .ok_or_else(|| invalid_write("writer aggregate sequence is missing".into()))?;
                Ok(WriterAggregateCall {
                    input,
                    binding: lower_writer_aggregate_binding(
                        call.resolved(),
                        AggregatePhase::Partial { sequence },
                    )?,
                    output,
                })
            })
            .collect::<Result<Vec<_>, ContractLoweringError>>()?;

        self.fragment_mut().add_row_consuming(
            node,
            Box::from([projected.node]),
            RequiredInputs::AsProduced,
            Distribution::Unconstrained,
            output.clone(),
            NodeKind::TableWriter {
                target: WriterTarget {
                    handle,
                    write_target_ordinal: ordinal,
                    input: projected.output,
                    required_distribution: projected.properties.distribution.clone(),
                    target_fields: target_fields.into_boxed_slice(),
                    output_schema,
                    partial_aggregates: partial_aggregates.into_boxed_slice(),
                },
            },
        )?;
        let properties = self
            .fragment_mut()
            .node_output_properties(node)
            .expect("the writer was just inserted")
            .clone();
        Ok(LoweredNode {
            fragment: self.current_fragment,
            node,
            output,
            columns: BTreeMap::new(),
            properties,
            display_names: auxiliary
                .schema()
                .arrow_schema()
                .fields()
                .iter()
                .map(|field| field.name().clone())
                .collect::<Vec<_>>()
                .into_boxed_slice(),
        })
    }

    fn lower_writer_projection(
        &mut self,
        source: LoweredNode,
        write: &SqlWritePlanInput,
    ) -> Result<LoweredNode, ContractLoweringError> {
        let width = write.contract.input_columns.len();
        // A writer publishes rows the target accepts. Nullability is checked
        // there for each row; it does not change the value carried across a
        // router edge. A different data type needs an explicit conversion.
        let mut source_matches_target = source.output.len() == width;
        if source_matches_target {
            for (value, column) in source.output.iter().zip(&write.contract.input_columns) {
                let ty = self.value_declared_type_in(source.fragment, *value)?;
                if ty.data_type != column_value_type(column).data_type {
                    source_matches_target = false;
                    break;
                }
            }
        }
        if write.root_output_exprs.is_none()
            && matches!(write.input, ConnectorWriteInputBinding::RootOutputByOrdinal)
            && source_matches_target
        {
            return Ok(source);
        }
        let node = self.fragment_mut().reserve_node_id()?;
        let mut expressions = Vec::with_capacity(width);
        let mut output = Vec::with_capacity(width);
        if let Some(root_expressions) = &write.root_output_exprs {
            if root_expressions.len() != width {
                return Err(ContractLoweringError::ArityMismatch {
                    context: "TableWriter root projection",
                    expected: width,
                    actual: root_expressions.len(),
                });
            }
            for (ordinal, (expression, column)) in root_expressions
                .iter()
                .zip(&write.contract.input_columns)
                .enumerate()
            {
                let expected = ValueType::new(column.data_type.clone(), column.nullable);
                let actual = expression_type(expression);
                if actual != expected {
                    return Err(ContractLoweringError::ExpressionTypeMismatch {
                        context: format!("TableWriter root projection {ordinal}"),
                        expected,
                        actual,
                    });
                }
                let expression_id = self.lower_expression(node, expression, &source.columns)?;
                let value = match identity_column_ref(expression) {
                    Some(column_id) => source
                        .columns
                        .get(&column_id)
                        .copied()
                        .ok_or(ContractLoweringError::UnknownColumnReference(column_id))?,
                    None => {
                        let ty = published_value_type(
                            &expression_type(expression),
                            &self.expression_value_type(expression_id)?,
                        );
                        self.fragment_mut().add_value(
                            ty,
                            ValueOrigin::Expr {
                                node,
                                expr: expression_id,
                            },
                        )?
                    }
                };
                expressions.push((expression_id, value));
                output.push(value);
            }
        } else {
            let ordinals: Vec<usize> = match &write.input {
                ConnectorWriteInputBinding::RootOutputByOrdinal => {
                    (0..source.output.len()).collect()
                }
                ConnectorWriteInputBinding::OutputOrdinals(ordinals) => ordinals.clone(),
            };
            if ordinals.len() != width {
                return Err(ContractLoweringError::ArityMismatch {
                    context: "TableWriter ordinal projection",
                    expected: width,
                    actual: ordinals.len(),
                });
            }
            for (field_ordinal, source_ordinal) in ordinals.into_iter().enumerate() {
                let value = source.output.get(source_ordinal).copied().ok_or_else(|| {
                    invalid_write(format!(
                        "writer input source ordinal {source_ordinal} is outside {} outputs",
                        source.output.len()
                    ))
                })?;
                // The plan states its types in one vocabulary, so the target's
                // column is read the way every other column is -- with the
                // provider's decoration off its nested fields.
                let ty = column_value_type(&write.contract.input_columns[field_ordinal]);
                let source_ty = self.value_declared_type_in(source.fragment, value)?;
                // A conversion is where the two types meet, whichever of them
                // differs: a column that admits a null the value never holds
                // is stated the same way a narrower one is.
                if source_ty == ty {
                    let expression = self.fragment_mut().add_expression(
                        node,
                        ty,
                        ContractExprKind::Value(value),
                    )?;
                    expressions.push((expression, value));
                    output.push(value);
                } else {
                    // The target's type is not the statement's, so the plan
                    // says how one becomes the other rather than leaving the
                    // writer to decide.
                    let read = self.fragment_mut().add_expression(
                        node,
                        source_ty.clone(),
                        ContractExprKind::Value(value),
                    )?;
                    // A conversion changes what a value is, not whether it is
                    // there: a null converts to a null. So the converted value
                    // admits one wherever the source did, and whether the
                    // target column accepts that is the writer's to enforce.
                    let converted_ty =
                        ValueType::new(ty.data_type.clone(), source_ty.nullable || ty.nullable);
                    let expression = self.fragment_mut().add_expression(
                        node,
                        converted_ty.clone(),
                        ContractExprKind::Cast {
                            expr: read,
                            target: ty.data_type.clone(),
                        },
                    )?;
                    let converted = self.fragment_mut().add_value(
                        converted_ty,
                        ValueOrigin::Expr {
                            node,
                            expr: expression,
                        },
                    )?;
                    expressions.push((expression, converted));
                    output.push(converted);
                }
            }
        }
        self.fragment_mut().add_project(
            node,
            source.node,
            expressions.into_boxed_slice(),
            output.clone().into_boxed_slice(),
        )?;
        let properties = self
            .fragment_mut()
            .node_output_properties(node)
            .expect("the writer projection was just inserted")
            .clone();
        Ok(LoweredNode {
            fragment: source.fragment,
            node,
            output: output.into_boxed_slice(),
            columns: BTreeMap::new(),
            properties,
            display_names: write
                .contract
                .input_columns
                .iter()
                .map(|column| column.name.clone())
                .collect::<Vec<_>>()
                .into_boxed_slice(),
        })
    }

    fn writer_relation_schema(
        &mut self,
        owner: NodeId,
        revision: u32,
        fields: &arrow::datatypes::Fields,
        slot_ids: &[u32],
    ) -> Result<LoweredWriterRelation, ContractLoweringError> {
        if fields.len() != slot_ids.len() {
            return Err(invalid_write(
                "writer schema field/slot arity differs".into(),
            ));
        }
        let mut output = Vec::with_capacity(fields.len());
        let mut by_slot = BTreeMap::new();
        let mut relation_fields = Vec::with_capacity(fields.len());
        for (index, (field, slot)) in fields.iter().zip(slot_ids).enumerate() {
            let role = match index {
                0 => WriterRelationFieldRole::Kind,
                1 => WriterRelationFieldRole::TargetOrdinal,
                2 => WriterRelationFieldRole::RowCount,
                3 => WriterRelationFieldRole::CommitFragment,
                _ => WriterRelationFieldRole::Auxiliary,
            };
            let kind = match role {
                WriterRelationFieldRole::Kind => WriterDerivedKind::RelationKind,
                WriterRelationFieldRole::TargetOrdinal => WriterDerivedKind::WriteTargetOrdinal,
                WriterRelationFieldRole::RowCount => WriterDerivedKind::AffectedRows,
                WriterRelationFieldRole::CommitFragment => WriterDerivedKind::CommitFragment,
                WriterRelationFieldRole::Auxiliary => WriterDerivedKind::RelationAuxiliary,
            };
            let ty = ValueType::new(field.data_type().clone(), field.is_nullable());
            let value = self.fragment_mut().add_value(
                ty.clone(),
                ValueOrigin::WriterDerived {
                    writer_node: owner,
                    kind,
                },
            )?;
            if by_slot.insert(*slot, value).is_some() {
                return Err(invalid_write("writer schema repeats a slot".into()));
            }
            output.push(value);
            relation_fields.push(WriterRelationField {
                value,
                name: field.name().clone().into_boxed_str(),
                ty,
                role,
            });
        }
        Ok(LoweredWriterRelation {
            schema: WriterRelationSchema {
                revision,
                fields: relation_fields.into_boxed_slice(),
            },
            output: output.into_boxed_slice(),
            values_by_slot: by_slot,
        })
    }

    fn lower_change_stream_writers(
        &mut self,
        source: LoweredNode,
        dag: ChangeStreamWriteDagSpec,
        auxiliary: &WriterAuxiliaryPlan,
        sequences: &BTreeMap<u32, AggregateSequenceId>,
        targets: &mut FinalizedWriteTargetSet,
    ) -> Result<(Vec<LoweredNode>, Box<[WriteTargetOrdinal]>), ContractLoweringError> {
        let effect = source
            .output
            .get(dag.effect_output_ordinal)
            .copied()
            .ok_or_else(|| invalid_write("change-stream effect ordinal is out of range".into()))?;
        let source_fragment = source.fragment;
        let mut router_routes = Vec::with_capacity(dag.routes.len());
        let mut writers = Vec::with_capacity(dag.routes.len());
        let mut ordinals = Vec::with_capacity(dag.routes.len());
        for mut route in dag.routes {
            let edge = self.plan_builder.reserve_edge_id()?;
            let writer_fragment = self.allocate_fragment()?;
            let mut projection = Vec::with_capacity(route.input_ordinals.len());
            let mut route_mapping = Vec::with_capacity(route.input_ordinals.len());
            let mut typed_sources = Vec::with_capacity(route.input_ordinals.len());
            for (route_input_ordinal, input) in route.input_ordinals.iter().enumerate() {
                let source_ordinal = usize::try_from(input.input_ordinal()).map_err(|_| {
                    invalid_write("change-stream input ordinal is outside host range".into())
                })?;
                let source_value = source.output.get(source_ordinal).copied().ok_or_else(|| {
                    invalid_write(format!(
                        "change-stream route input ordinal {source_ordinal} is out of range"
                    ))
                })?;
                let input_column = route
                    .sink
                    .contract
                    .input_columns
                    .get(route_input_ordinal)
                    .ok_or_else(|| {
                        invalid_write("change-stream route input/schema arity differs".into())
                    })?;
                projection.push(source_value);
                route_mapping.push((input.token(), source_value));
                // The exchange carries the producer's value. A sink may
                // require a non-null field even when the producer admits
                // null; the writer checks that requirement for each row.
                let source_type = self.value_declared_type_in(source_fragment, source_value)?;
                if source_type.data_type != column_value_type(input_column).data_type {
                    return Err(invalid_write(format!(
                        "change-stream route input {route_input_ordinal} source ordinal {source_ordinal} `{}` type {:?} differs from writer field `{}` type {:?}; producer=[{}], writer=[{}]",
                        source
                            .display_names
                            .get(source_ordinal)
                            .map(String::as_str)
                            .unwrap_or("?"),
                        source_type.data_type,
                        input_column.name,
                        column_value_type(input_column).data_type,
                        source.display_names.join(", "),
                        route
                            .sink
                            .contract
                            .input_columns
                            .iter()
                            .map(|column| column.name.as_str())
                            .collect::<Vec<_>>()
                            .join(", ")
                    )));
                }
                typed_sources.push((source_value, source_type));
            }
            if route
                .sink
                .contract
                .target
                .fields
                .iter()
                .map(|field| field.token)
                .ne(route.input_ordinals.iter().map(|input| input.token()))
            {
                return Err(invalid_write(
                    "change-stream route token order differs from its writer target".into(),
                ));
            }
            let partition_sources = route
                .partition_input_positions
                .iter()
                .map(|position| {
                    projection.get(*position).copied().ok_or_else(|| {
                        invalid_write(
                            "change-stream partition input position is outside the route input"
                                .into(),
                        )
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;

            self.current_fragment = writer_fragment;
            let receiver = self.fragment_mut().reserve_node_id()?;
            let mut imports = Vec::with_capacity(projection.len());
            let mut imported = Vec::with_capacity(projection.len());
            let mut source_to_import = BTreeMap::new();
            for (source_value, ty) in typed_sources {
                let destination = match source_to_import.get(&source_value) {
                    Some((destination, existing_type)) if existing_type == &ty => *destination,
                    Some(_) => {
                        return Err(invalid_write(
                            "one change-stream source value is bound with conflicting target types"
                                .into(),
                        ));
                    }
                    None => {
                        let destination = self.fragment_mut().add_value(
                            ty.clone(),
                            ValueOrigin::ExchangeImport { edge, source_value },
                        )?;
                        source_to_import.insert(source_value, (destination, ty));
                        destination
                    }
                };
                imports.push((source_value, destination));
                imported.push(destination);
            }
            let (source_distribution, destination_distribution) = if partition_sources.is_empty() {
                (Distribution::Singleton, Distribution::Singleton)
            } else {
                let scheme = self.allocate_hash_scheme()?;
                let destination_keys = route
                    .partition_input_positions
                    .iter()
                    .map(|position| {
                        imported.get(*position).copied().ok_or_else(|| {
                            invalid_write(
                                "change-stream partition import position is outside the route input"
                                    .into(),
                            )
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                (
                    Distribution::Hash {
                        keys: partition_sources.clone().into_boxed_slice(),
                        scheme: scheme.clone(),
                    },
                    Distribution::Hash {
                        keys: destination_keys.into_boxed_slice(),
                        scheme,
                    },
                )
            };
            self.fragment_mut().add_exchange_source(
                receiver,
                edge,
                imports.clone().into_boxed_slice(),
                imported.clone().into_boxed_slice(),
                destination_distribution.clone(),
                RowMultiplicity::SingleCopy,
            )?;
            let receiver_properties = self
                .fragment_mut()
                .node_output_properties(receiver)
                .expect("the exchange source was just inserted")
                .clone();
            self.plan_builder.add_edge(Edge {
                id: edge,
                kind: EdgeKind::ChangeStreamRouter,
                source: EdgeSource {
                    fragment: source_fragment,
                    projection: projection.clone().into_boxed_slice(),
                },
                destination: EdgeDestination {
                    fragment: writer_fragment,
                    node: receiver,
                    receive_mapping: imports.into_boxed_slice(),
                },
                partitioning: EdgePartitioning {
                    source: source_distribution,
                    source_multiplicity: RowMultiplicity::SingleCopy,
                    destination: destination_distribution,
                    destination_multiplicity: RowMultiplicity::SingleCopy,
                },
            })?;
            router_routes.push(ContractChangeStreamRoute {
                route_id: route.route_id,
                write_target_ordinal: route.write_target_ordinal,
                accepted_effects: route.accepted_effects.into_boxed_slice(),
                input_mapping: route_mapping.into_boxed_slice(),
                partition_by: partition_sources.into_boxed_slice(),
                edge,
            });
            let ordinal = route.write_target_ordinal;
            let writer_source = LoweredNode {
                fragment: writer_fragment,
                node: receiver,
                output: imported.into_boxed_slice(),
                columns: BTreeMap::new(),
                properties: receiver_properties,
                display_names: route
                    .sink
                    .contract
                    .input_columns
                    .iter()
                    .map(|column| column.name.clone())
                    .collect::<Vec<_>>()
                    .into_boxed_slice(),
            };
            if route.sink.root_output_exprs.is_some() {
                return Err(invalid_write(
                    "change-stream route cannot apply a second writer projection".into(),
                ));
            }
            // The router edge has already projected the exact ordered target
            // occurrences. The writer must consume its ExchangeSource directly;
            // applying the producer-relative ordinal binding again would insert
            // a second projection and sever the route-to-writer contract.
            route.sink.input = ConnectorWriteInputBinding::RootOutputByOrdinal;
            let handle = targets.take(ordinal).map_err(invalid_write)?;
            let writer = self.lower_table_writer(
                writer_source,
                route.sink,
                ordinal,
                auxiliary,
                sequences,
                handle,
            )?;
            writers.push(writer);
            ordinals.push(ordinal);
        }
        self.current_fragment = source_fragment;
        self.complete_fragment(
            source_fragment,
            source.node,
            FragmentSink::Router {
                effect,
                routes: router_routes.into_boxed_slice(),
            },
        )?;
        Ok((writers, ordinals.into_boxed_slice()))
    }

    fn lower_single_writer_finish(
        self,
        writer: LoweredNode,
        ordinal: WriteTargetOrdinal,
        auxiliary: &WriterAuxiliaryPlan,
        sequences: &BTreeMap<u32, AggregateSequenceId>,
    ) -> Result<PlanBuilder, ContractLoweringError> {
        self.lower_writer_finish(vec![writer], Box::from([ordinal]), auxiliary, sequences)
    }

    fn lower_writer_finish(
        mut self,
        writers: Vec<LoweredNode>,
        ordinals: Box<[WriteTargetOrdinal]>,
        auxiliary: &WriterAuxiliaryPlan,
        sequences: &BTreeMap<u32, AggregateSequenceId>,
    ) -> Result<PlanBuilder, ContractLoweringError> {
        if writers.is_empty() || writers.len() != ordinals.len() {
            return Err(invalid_write(
                "writer finish has no exact writer set".into(),
            ));
        }
        let finish_fragment = self.allocate_fragment()?;
        self.current_fragment = finish_fragment;
        let mut exchange_nodes = Vec::with_capacity(writers.len());
        let mut exchange_outputs = Vec::with_capacity(writers.len());
        for writer in writers {
            let edge = self.plan_builder.reserve_edge_id()?;
            self.complete_fragment(writer.fragment, writer.node, FragmentSink::Stream { edge })?;
            let exchange = self.fragment_mut().reserve_node_id()?;
            let mut imports = Vec::with_capacity(writer.output.len());
            let mut imported = Vec::with_capacity(writer.output.len());
            for (source, field) in writer
                .output
                .iter()
                .zip(auxiliary.schema().arrow_schema().fields())
            {
                let destination = self.fragment_mut().add_value(
                    ValueType::new(field.data_type().clone(), field.is_nullable()),
                    ValueOrigin::ExchangeImport {
                        edge,
                        source_value: *source,
                    },
                )?;
                imports.push((*source, destination));
                imported.push(destination);
            }
            self.fragment_mut().add_exchange_source(
                exchange,
                edge,
                imports.clone().into_boxed_slice(),
                imported.clone().into_boxed_slice(),
                Distribution::Singleton,
                RowMultiplicity::SingleCopy,
            )?;
            self.plan_builder.add_edge(Edge {
                id: edge,
                kind: EdgeKind::Stream,
                source: EdgeSource {
                    fragment: writer.fragment,
                    projection: writer.output,
                },
                destination: EdgeDestination {
                    fragment: finish_fragment,
                    node: exchange,
                    receive_mapping: imports.into_boxed_slice(),
                },
                partitioning: EdgePartitioning {
                    source: Distribution::Singleton,
                    source_multiplicity: RowMultiplicity::SingleCopy,
                    destination: Distribution::Singleton,
                    destination_multiplicity: RowMultiplicity::SingleCopy,
                },
            })?;
            exchange_nodes.push(exchange);
            exchange_outputs.push(imported.into_boxed_slice());
        }
        let (finish_input, imported) = if exchange_nodes.len() == 1 {
            (
                exchange_nodes[0],
                exchange_outputs.pop().unwrap().into_vec(),
            )
        } else {
            let union = self.fragment_mut().reserve_node_id()?;
            let mut output = Vec::with_capacity(auxiliary.schema().arrow_schema().fields().len());
            for (output_ordinal, field) in auxiliary
                .schema()
                .arrow_schema()
                .fields()
                .iter()
                .enumerate()
            {
                output.push(self.fragment_mut().add_value(
                    ValueType::new(field.data_type().clone(), field.is_nullable()),
                    ValueOrigin::NodeOutput {
                        node: union,
                        output_ordinal: checked_ordinal("writer UnionAll output", output_ordinal)?,
                    },
                )?);
            }
            self.fragment_mut().add_row_consuming(
                union,
                exchange_nodes.into_boxed_slice(),
                RequiredInputs::Singleton,
                Distribution::Singleton,
                output.clone().into_boxed_slice(),
                NodeKind::SetOp {
                    kind: SetOperationKind::UnionAll,
                    input_mappings: exchange_outputs.into_boxed_slice(),
                },
            )?;
            (union, output)
        };

        let finish = self.fragment_mut().reserve_node_id()?;
        let root_schema = RootWriteResultSchema::new();
        let relation = self.writer_relation_schema(
            finish,
            root_schema.contract_version(),
            root_schema.arrow_schema().fields(),
            &root_schema.slot_ids(),
        )?;
        let LoweredWriterRelation {
            schema: output_schema,
            output,
            values_by_slot: output_slots,
        } = relation;
        if output_schema.revision != ROOT_WRITE_RESULT_SCHEMA_REVISION {
            return Err(invalid_write(
                "root write-result schema revision differs".into(),
            ));
        }
        let input_schema = writer_import_schema(auxiliary, &imported)?;
        let input_slots = auxiliary
            .schema()
            .slot_ids()
            .into_iter()
            .zip(imported.iter().copied())
            .collect::<BTreeMap<_, _>>();
        let mut final_values_by_slot = BTreeMap::new();
        let final_aggregates = auxiliary
            .final_plan()
            .calls()
            .iter()
            .map(|call| {
                let input = input_slots
                    .get(&call.intermediate_input_slot_id())
                    .copied()
                    .ok_or_else(|| invalid_write("final aggregate input slot is unknown".into()))?;
                let output = self.fragment_mut().add_value(
                    writer_aggregate_result_type(call.resolved())?,
                    ValueOrigin::WriterDerived {
                        writer_node: finish,
                        kind: WriterDerivedKind::RelationAuxiliary,
                    },
                )?;
                if final_values_by_slot
                    .insert(call.final_output_slot_id(), output)
                    .is_some()
                {
                    return Err(invalid_write(
                        "final aggregate output slot is duplicated".into(),
                    ));
                }
                let sequence = sequences
                    .get(&call.intermediate_input_slot_id())
                    .copied()
                    .ok_or_else(|| invalid_write("final aggregate sequence is missing".into()))?;
                Ok(WriterAggregateCall {
                    input,
                    binding: lower_writer_aggregate_binding(
                        call.resolved(),
                        AggregatePhase::Final { sequence },
                    )?,
                    output,
                })
            })
            .collect::<Result<Vec<_>, ContractLoweringError>>()?;
        let grouped_unpivot = auxiliary
            .final_plan()
            .unpivot()
            .map(
                |unpivot| -> Result<WriterGroupedUnpivotSpec, ContractLoweringError> {
                    let grouping_input = input_slots
                        .get(&unpivot.grouping_input_slot_id())
                        .copied()
                        .ok_or_else(|| {
                            invalid_write("writer grouping input slot is unknown".into())
                        })?;
                    let grouping_output = self.fragment_mut().add_value(
                        ValueType::new(DataType::Int32, false),
                        ValueOrigin::WriterDerived {
                            writer_node: finish,
                            kind: WriterDerivedKind::GroupingKey,
                        },
                    )?;
                    let passthrough_output = output_slots
                        .get(&unpivot.passthrough_output_slot_id())
                        .copied()
                        .ok_or_else(|| {
                            invalid_write("writer passthrough output slot is unknown".into())
                        })?;
                    let value_output = output_slots
                        .get(&unpivot.value_output_slot_id())
                        .copied()
                        .ok_or_else(|| {
                        invalid_write("writer value output slot is unknown".into())
                    })?;
                    let literal_outputs = unpivot
                        .literal_output_slot_ids()
                        .iter()
                        .map(|slot| {
                            output_slots.get(slot).copied().ok_or_else(|| {
                                invalid_write("writer literal output slot is unknown".into())
                            })
                        })
                        .collect::<Result<Vec<_>, _>>()?;
                    let mappings = unpivot
                        .mappings()
                        .iter()
                        .map(|mapping| {
                            let input = final_values_by_slot
                                .get(&mapping.input_value_slot_id())
                                .copied()
                                .ok_or_else(|| {
                                    invalid_write("writer Unpivot input slot is unknown".into())
                                })?;
                            let constants = mapping
                                .constants()
                                .iter()
                                .map(|constant| match constant {
                                    crate::analysis::UnpivotConstant::Scalar(expression) => {
                                        Ok(ContractUnpivotConstant::Scalar(self.lower_expression(
                                            finish,
                                            expression,
                                            &BTreeMap::new(),
                                        )?))
                                    }
                                    crate::analysis::UnpivotConstant::Int32List(values) => {
                                        Ok(ContractUnpivotConstant::Int32List(
                                            values.clone().into_boxed_slice(),
                                        ))
                                    }
                                    crate::analysis::UnpivotConstant::Utf8Map(entries) => {
                                        Ok(ContractUnpivotConstant::Utf8Map(
                                            entries
                                                .iter()
                                                .map(|(key, value)| {
                                                    (
                                                        key.clone().into_boxed_str(),
                                                        value.clone().into_boxed_str(),
                                                    )
                                                })
                                                .collect::<Vec<_>>()
                                                .into_boxed_slice(),
                                        ))
                                    }
                                })
                                .collect::<Result<Vec<_>, ContractLoweringError>>()?;
                            Ok(WriterGroupedUnpivotMapping {
                                write_target_ordinal: mapping.target(),
                                input,
                                constants: constants.into_boxed_slice(),
                            })
                        })
                        .collect::<Result<Vec<_>, ContractLoweringError>>()?;
                    let statistics_target_ordinals = unpivot
                        .mappings()
                        .iter()
                        .map(|mapping| mapping.target())
                        .collect::<BTreeSet<_>>()
                        .into_iter()
                        .collect::<Vec<_>>()
                        .into_boxed_slice();
                    Ok(WriterGroupedUnpivotSpec {
                        statistics_target_ordinals,
                        grouping_input,
                        grouping_output,
                        passthrough_output,
                        value_output,
                        literal_outputs: literal_outputs.into_boxed_slice(),
                        mappings: mappings.into_boxed_slice(),
                        max_output_rows: u64::try_from(unpivot.max_output_rows()).map_err(
                            |_| invalid_write("writer Unpivot row bound is outside u64".into()),
                        )?,
                        max_output_bytes: u64::try_from(unpivot.max_output_bytes()).map_err(
                            |_| invalid_write("writer Unpivot byte bound is outside u64".into()),
                        )?,
                    })
                },
            )
            .transpose()?;
        self.fragment_mut().add_row_consuming(
            finish,
            Box::from([finish_input]),
            RequiredInputs::Singleton,
            Distribution::Singleton,
            output.clone(),
            NodeKind::TableFinish(WriterFinishSpec {
                expected_target_ordinals: ordinals,
                input_schema,
                output_schema,
                final_aggregates: final_aggregates.into_boxed_slice(),
                grouped_unpivot,
            }),
        )?;
        let fields = root_schema
            .arrow_schema()
            .fields()
            .iter()
            .zip(output.iter())
            .map(|(field, value)| ResultField {
                name: field.name().clone().into_boxed_str(),
                alias: None,
                value: *value,
                ty: ValueType::new(field.data_type().clone(), field.is_nullable()),
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();
        self.complete_fragment(finish_fragment, finish, FragmentSink::Result)?;
        self.finish_draft(ResultPort {
            fragment: finish_fragment,
            output: OutputPort {
                node: finish,
                columns: output,
            },
            fields,
        })
    }

    fn lower_redistribute(
        &mut self,
        plan: &PhysicalPlanNode,
        redistribute: &crate::planner::physical::RedistributeNode,
        shared_hash_scheme: Option<HashPartitionScheme>,
    ) -> Result<LoweredNode, ContractLoweringError> {
        expect_children(plan, 1)?;
        require_output_shape(
            "Redistribute",
            &plan.output_columns,
            &redistribute.output_columns,
        )?;
        require_output_shape(
            "Redistribute",
            &plan.output_columns,
            &plan.children[0].output_columns,
        )?;

        let destination = self.current_fragment;
        let source_fragment = self.allocate_fragment()?;
        self.current_fragment = source_fragment;
        let source = self.lower_node(&plan.children[0])?;
        if source.fragment != source_fragment {
            return Err(ContractLoweringError::UnexpectedFragment {
                node: "Redistribute",
                expected: source_fragment,
                actual: source.fragment,
            });
        }
        self.current_fragment = destination;

        let layout = match &redistribute.mode {
            RedistributeMode::Gather => {
                require_no_partition_expressions("Gather", &redistribute.partition_exprs)?;
                ExchangeLayout::Gather
            }
            RedistributeMode::Broadcast => {
                require_no_partition_expressions("Broadcast", &redistribute.partition_exprs)?;
                ExchangeLayout::Broadcast
            }
            RedistributeMode::Hash { cols, .. } => {
                if cols.is_empty() {
                    return Err(ContractLoweringError::EmptyPartitionKeys {
                        node: "Redistribute",
                    });
                }
                if redistribute.partition_exprs.len() != cols.len()
                    || redistribute
                        .partition_exprs
                        .iter()
                        .zip(cols)
                        .any(|(expression, column)| {
                            identity_column_ref(expression) != Some(*column)
                        })
                {
                    return Err(ContractLoweringError::InvalidPartitionExpressions {
                        node: "Redistribute",
                        detail: "hash partition expressions are not the exact ordered key columns",
                    });
                }
                let scheme = match shared_hash_scheme {
                    Some(scheme) => scheme,
                    None => self.allocate_hash_scheme()?,
                };
                ExchangeLayout::Hash {
                    keys: cols.clone().into_boxed_slice(),
                    scheme,
                }
            }
        };
        self.append_exchange(source, &plan.output_columns, layout)
    }

    fn append_exchange(
        &mut self,
        source: LoweredNode,
        output_columns: &[OutputColumn],
        layout: ExchangeLayout,
    ) -> Result<LoweredNode, ContractLoweringError> {
        if source.properties.row_multiplicity != RowMultiplicity::SingleCopy {
            return Err(ContractLoweringError::PropertyRequirementUnsatisfied {
                node: "Redistribute",
                detail: "exchange source is not single-copy",
            });
        }
        if output_columns.len() != source.output.len() {
            return Err(ContractLoweringError::ArityMismatch {
                context: "exchange output",
                expected: source.output.len(),
                actual: output_columns.len(),
            });
        }
        let destination = self.current_fragment;
        if destination == source.fragment {
            return Err(ContractLoweringError::UnexpectedFragment {
                node: "ExchangeSource",
                expected: destination,
                actual: source.fragment,
            });
        }
        let edge = self.plan_builder.reserve_edge_id()?;
        let receiver = self.fragment_mut().reserve_node_id()?;
        let mut imported_by_source = BTreeMap::new();
        let mut receive_mapping = Vec::with_capacity(source.output.len());
        let mut output = Vec::with_capacity(source.output.len());
        let mut columns = BTreeMap::new();
        for (ordinal, (source_value, column)) in
            source.output.iter().zip(output_columns).enumerate()
        {
            let imported = match imported_by_source.get(source_value).copied() {
                Some(imported) => imported,
                None => {
                    // An edge carries the column as it stands on the other
                    // side: what arrives is what was sent, so the import
                    // admits what the source value admits even where the
                    // statement was analyzed to expect less.
                    let sent = self.value_declared_type_in(source.fragment, *source_value)?;
                    let ty = published_value_type(&value_type(column), &sent);
                    let imported = self.fragment_mut().add_value(
                        ty,
                        ValueOrigin::ExchangeImport {
                            edge,
                            source_value: *source_value,
                        },
                    )?;
                    imported_by_source.insert(*source_value, imported);
                    imported
                }
            };
            if let Some(previous) = columns.insert(column.column_id, imported)
                && previous != imported
            {
                return Err(ContractLoweringError::OutputColumnMismatch {
                    node: "ExchangeSource",
                    ordinal,
                    detail: "one SQL column identity maps to different imported values".into(),
                });
            }
            receive_mapping.push((*source_value, imported));
            output.push(imported);
        }

        let (source_distribution, destination_distribution, destination_multiplicity) = match layout
        {
            ExchangeLayout::Gather => (
                Distribution::Singleton,
                Distribution::Singleton,
                RowMultiplicity::SingleCopy,
            ),
            ExchangeLayout::Broadcast => (
                Distribution::Broadcast,
                Distribution::Broadcast,
                RowMultiplicity::Replicated,
            ),
            ExchangeLayout::Hash { keys, scheme } => {
                let source_keys = keys
                    .iter()
                    .map(|column| {
                        source
                            .columns
                            .get(column)
                            .copied()
                            .ok_or(ContractLoweringError::UnknownColumnReference(*column))
                    })
                    .collect::<Result<Box<[_]>, _>>()?;
                let destination_keys = keys
                    .iter()
                    .map(|column| {
                        columns
                            .get(column)
                            .copied()
                            .ok_or(ContractLoweringError::UnknownColumnReference(*column))
                    })
                    .collect::<Result<Box<[_]>, _>>()?;
                (
                    Distribution::Hash {
                        keys: source_keys,
                        scheme: scheme.clone(),
                    },
                    Distribution::Hash {
                        keys: destination_keys,
                        scheme,
                    },
                    RowMultiplicity::SingleCopy,
                )
            }
        };
        self.fragment_mut().add_exchange_source(
            receiver,
            edge,
            receive_mapping.clone().into_boxed_slice(),
            output.clone().into_boxed_slice(),
            destination_distribution.clone(),
            destination_multiplicity,
        )?;
        let properties = self
            .fragment_mut()
            .node_output_properties(receiver)
            .expect("the exchange source was just inserted")
            .clone();
        let edge_contract = Edge {
            id: edge,
            kind: EdgeKind::Stream,
            source: EdgeSource {
                fragment: source.fragment,
                projection: source.output.clone(),
            },
            destination: EdgeDestination {
                fragment: destination,
                node: receiver,
                receive_mapping: receive_mapping.into_boxed_slice(),
            },
            partitioning: EdgePartitioning {
                source: source_distribution,
                source_multiplicity: RowMultiplicity::SingleCopy,
                destination: destination_distribution,
                destination_multiplicity,
            },
        };
        self.plan_builder.add_edge(edge_contract.clone())?;
        self.edges.insert(edge, edge_contract);
        self.complete_fragment(source.fragment, source.node, FragmentSink::Stream { edge })?;
        let lowered = LoweredNode {
            fragment: destination,
            node: receiver,
            output: output.into_boxed_slice(),
            columns,
            properties,
            // A column keeps its name across the edge: what arrives is
            // what was sent, and the sender already said what it calls it.
            display_names: source
                .output
                .iter()
                .zip(source.display_names.iter())
                .map(|(value, fallback)| {
                    self.annotated_values
                        .get(&(source.fragment, *value))
                        .cloned()
                        .unwrap_or_else(|| fallback.clone())
                })
                .collect::<Vec<_>>()
                .into_boxed_slice(),
        };
        // Shared-scheme joins lower Redistribute directly, so they do not
        // pass through lower_node's annotation step. Publish every receiver
        // value now, including keys hidden by SEMI/ANTI or null extension.
        self.annotate_output_display_names(&lowered);
        Ok(lowered)
    }

    fn ensure_singleton(
        &mut self,
        child: LoweredNode,
        output_columns: &[OutputColumn],
    ) -> Result<LoweredNode, ContractLoweringError> {
        if child.properties.distribution == Distribution::Singleton
            && child.properties.row_multiplicity == RowMultiplicity::SingleCopy
        {
            return Ok(child);
        }
        let destination = self.allocate_fragment()?;
        self.current_fragment = destination;
        self.append_exchange(child, output_columns, ExchangeLayout::Gather)
    }

    fn lower_hash_join(
        &mut self,
        plan: &PhysicalPlanNode,
        join: &crate::planner::physical::PhysicalHashJoinNode,
    ) -> Result<LoweredNode, ContractLoweringError> {
        expect_children(plan, 2)?;
        require_output_shape("HashJoin", &plan.output_columns, &join.output_columns)?;
        if join.eq_conditions.is_empty() {
            return Err(ContractLoweringError::InvalidJoin {
                node: "HashJoin",
                detail: "hash join has no equality keys",
            });
        }
        let kind = lower_join_kind(join.join_type);
        if kind == ContractJoinKind::Cross {
            return Err(ContractLoweringError::InvalidJoin {
                node: "HashJoin",
                detail: "cross join cannot use hash-join lowering",
            });
        }
        let build_side = match join.build_side {
            PhysicalHashJoinBuildSide::Left => ContractJoinSide::Left,
            PhysicalHashJoinBuildSide::Right => ContractJoinSide::Right,
        };
        let distribution = resolve_hash_join_distribution(join)?;

        // A partitioned join's two inputs stand in one partition space. An
        // input that already stands in one names it -- two subqueries
        // aggregated on the key they are joined by arrive that way -- and the
        // side still to be shuffled is sent into that same space. Only when
        // neither side arrives partitioned does the join open a space of its
        // own.
        let mut lowered_children: [Option<LoweredNode>; 2] = [None, None];
        let mut shared_hash_scheme = None;
        if distribution == ContractJoinDistribution::Partitioned {
            for (ordinal, child) in plan.children.iter().enumerate() {
                if matches!(
                    &child.kind,
                    PhysicalPlanKind::Redistribute(redistribute)
                        if matches!(redistribute.mode, RedistributeMode::Hash { .. })
                ) {
                    continue;
                }
                let lowered = self.lower_node(child)?;
                if let Distribution::Hash { scheme, .. } = &lowered.properties.distribution {
                    if shared_hash_scheme
                        .as_ref()
                        .is_some_and(|expected| expected != scheme)
                    {
                        return Err(ContractLoweringError::InvalidJoin {
                            node: "HashJoin",
                            detail: "partitioned inputs arrive in different hash partition spaces",
                        });
                    }
                    shared_hash_scheme = Some(scheme.clone());
                }
                lowered_children[ordinal] = Some(lowered);
            }
            if shared_hash_scheme.is_none() {
                shared_hash_scheme = Some(self.allocate_hash_scheme()?);
            }
        }
        let mut inputs = Vec::with_capacity(2);
        for (ordinal, child) in plan.children.iter().enumerate() {
            let lowered = match lowered_children[ordinal].take() {
                Some(lowered) => lowered,
                None => match (&child.kind, &shared_hash_scheme) {
                    (PhysicalPlanKind::Redistribute(redistribute), Some(scheme))
                        if matches!(redistribute.mode, RedistributeMode::Hash { .. }) =>
                    {
                        self.lower_redistribute(child, redistribute, Some(scheme.clone()))?
                    }
                    _ => self.lower_node(child)?,
                },
            };
            if lowered.fragment != self.current_fragment {
                return Err(ContractLoweringError::UnexpectedFragment {
                    node: "HashJoin",
                    expected: self.current_fragment,
                    actual: lowered.fragment,
                });
            }
            inputs.push(lowered);
        }
        let [left, right] = inputs.as_slice() else {
            unreachable!("hash join arity is checked before lowering children")
        };

        let node = self.fragment_mut().reserve_node_id()?;
        let mut keys = Vec::with_capacity(join.eq_conditions.len());
        let mut left_key_values = Vec::with_capacity(join.eq_conditions.len());
        let mut right_key_values = Vec::with_capacity(join.eq_conditions.len());
        for condition in &join.eq_conditions {
            // Both sides are read in the plan's own vocabulary, where a
            // list's element is `item` and a map's entries are `entries`/
            // `key`/`value`. One side coming from a provider and the other
            // from a projection would otherwise differ over decoration
            // neither of them compares by.
            let left_key_type = novarocks_types::undecorated_nested_type(&condition.left.data_type);
            let right_key_type =
                novarocks_types::undecorated_nested_type(&condition.right.data_type);
            // The values the exchange partitioned by are the ones below any
            // conversion: the partition hash already widens every narrow
            // integer to the same eight bytes, so a side converted here still
            // meets the other where it was sent.
            let left_value = direct_join_key_value(&condition.left, left);
            let right_value = direct_join_key_value(&condition.right, right);
            let left_expr = self.lower_expression(node, &condition.left, &left.columns)?;
            let right_expr = self.lower_expression(node, &condition.right, &right.columns)?;
            // A join compares one type. Its two keys were reconciled while
            // the statement was analyzed -- a narrower integer on one side --
            // and the plan states the comparison it performs rather than two
            // sides the reader has to reconcile again.
            let (left_expr, right_expr) = if left_key_type == right_key_type {
                (left_expr, right_expr)
            } else {
                // Two integers of different widths meet above both of them --
                // an INT against a TINYINT is compared as BIGINT -- so the
                // type the join states is not required to be one of the two.
                // What it is required to be is a type both sides reach.
                let compared = novarocks_types::wider_type(&left_key_type, &right_key_type);
                if novarocks_types::wider_type(&left_key_type, &compared) != compared
                    || novarocks_types::wider_type(&right_key_type, &compared) != compared
                {
                    return Err(ContractLoweringError::InvalidJoinKeys {
                        node: "HashJoin",
                        detail: format!(
                            "equality key pair compares {left_key_type:?} against {right_key_type:?}, which meet at neither"
                        ),
                    });
                }
                (
                    self.cast_expression_to(node, left_expr, &compared)?,
                    self.cast_expression_to(node, right_expr, &compared)?,
                )
            };
            left_key_values.push(left_value);
            right_key_values.push(right_value);
            keys.push(JoinKey {
                left: left_expr,
                right: right_expr,
                null_safe: condition.null_safe,
            });
        }

        let required_inputs = hash_join_required_inputs(HashJoinRequirement {
            plan,
            kind,
            build_side,
            distribution,
            left,
            right,
            left_keys: &left_key_values,
            right_keys: &right_key_values,
        })?;
        let visible = merge_visible_columns("HashJoin", &left.columns, &right.columns)?;
        let residual = join
            .other_condition
            .as_ref()
            .map(|predicate| {
                if predicate.data_type != DataType::Boolean {
                    return Err(ContractLoweringError::JoinPredicateIsNotBoolean {
                        node: "HashJoin",
                        actual: predicate.data_type.clone(),
                    });
                }
                self.lower_expression(node, predicate, &visible)
            })
            .transpose()?;
        let lowered_output = self.lower_join_outputs(
            node,
            JoinOutputRequest {
                kind,
                requested: &plan.output_columns,
                left,
                left_columns: &plan.children[0].output_columns,
                right,
                right_columns: &plan.children[1].output_columns,
            },
        )?;
        let JoinOutput {
            output,
            columns,
            null_extended,
        } = lowered_output;
        let output_distribution = if distribution == ContractJoinDistribution::Singleton {
            Distribution::Singleton
        } else {
            hash_join_output_distribution(kind, build_side, left, right)
        };
        self.fragment_mut().add_join(
            node,
            [left.node, right.node],
            required_inputs,
            output.clone(),
            output_distribution,
            NodeKind::HashJoin {
                kind,
                keys: keys.into_boxed_slice(),
                build_side,
                distribution,
                residual,
                null_extended,
            },
        )?;
        let properties = self
            .fragment_mut()
            .node_output_properties(node)
            .expect("the join was just inserted")
            .clone();
        for intent in &join.build_runtime_filters {
            if self.unstatable_runtime_filters.contains(&intent.filter_id) {
                continue;
            }
            if join.execution_mode != Some(intent.execution_mode) {
                return Err(ContractLoweringError::InvalidRuntimeFilter {
                    id: intent.filter_id,
                    detail: "producer execution mode differs from the frozen join mode".to_string(),
                });
            }
            let key_ordinal = u32::try_from(intent.expr_order).map_err(|_| {
                ContractLoweringError::InvalidRuntimeFilter {
                    id: intent.filter_id,
                    detail: "join equality ordinal exceeds u32".to_string(),
                }
            })?;
            let (build_value, probe_value) = match build_side {
                ContractJoinSide::Left => (
                    left_key_values.get(intent.expr_order).copied().flatten(),
                    right_key_values.get(intent.expr_order).copied().flatten(),
                ),
                ContractJoinSide::Right => (
                    right_key_values.get(intent.expr_order).copied().flatten(),
                    left_key_values.get(intent.expr_order).copied().flatten(),
                ),
            };
            let (Some(build_value), Some(probe_value)) = (build_value, probe_value) else {
                return Err(ContractLoweringError::InvalidRuntimeFilter {
                    id: intent.filter_id,
                    detail: "join runtime filter key is not one direct lowered ValueId".to_string(),
                });
            };
            let (expected_build_expr, expected_probe_expr) = match build_side {
                ContractJoinSide::Left => (
                    &join.eq_conditions[intent.expr_order].left,
                    &join.eq_conditions[intent.expr_order].right,
                ),
                ContractJoinSide::Right => (
                    &join.eq_conditions[intent.expr_order].right,
                    &join.eq_conditions[intent.expr_order].left,
                ),
            };
            if identity_column_ref(&intent.build_expr) != identity_column_ref(expected_build_expr)
                || identity_column_ref(&intent.probe_expr)
                    != identity_column_ref(expected_probe_expr)
            {
                return Err(ContractLoweringError::InvalidRuntimeFilter {
                    id: intent.filter_id,
                    detail: "producer intent differs from the exact frozen join key".to_string(),
                });
            }
            let filter_id = runtime_filter_id(intent.filter_id)?;
            self.attach_runtime_filter(self.current_fragment, filter_id)?;
            let pending = PendingRuntimeFilterBuild::Join {
                fragment: self.current_fragment,
                node,
                key_ordinal,
                build_side,
                execution_mode: intent.execution_mode,
                build_value,
                probe_value,
                null_semantics: match intent.null_semantics {
                    crate::planner::runtime_filter::contract::NullSemantics::NeverMatches => {
                        RuntimeFilterNullSemantics::NeverMatches
                    }
                    crate::planner::runtime_filter::contract::NullSemantics::NullSafeEqual => {
                        RuntimeFilterNullSemantics::NullSafeEqual
                    }
                },
            };
            if self
                .runtime_filter_builds
                .insert(intent.filter_id, pending)
                .is_some()
            {
                return Err(ContractLoweringError::InvalidRuntimeFilter {
                    id: intent.filter_id,
                    detail: "more than one producer owns the filter".to_string(),
                });
            }
        }
        Ok(LoweredNode {
            fragment: self.current_fragment,
            node,
            output,
            columns,
            properties,
            display_names: plan
                .output_columns
                .iter()
                .map(|column| column.name.clone())
                .collect::<Vec<_>>()
                .into_boxed_slice(),
        })
    }

    fn lower_nest_loop_join(
        &mut self,
        plan: &PhysicalPlanNode,
        join: &crate::planner::physical::PhysicalNestLoopJoinNode,
    ) -> Result<LoweredNode, ContractLoweringError> {
        expect_children(plan, 2)?;
        require_output_shape("NestLoopJoin", &plan.output_columns, &join.output_columns)?;
        let left = self.lower_node(&plan.children[0])?;
        let right = self.lower_node(&plan.children[1])?;
        require_same_fragment("NestLoopJoin", &left, &right)?;
        let (distribution, required_inputs, output_distribution) = if left.properties.distribution
            == Distribution::Singleton
            && right.properties.distribution == Distribution::Singleton
            && left.properties.row_multiplicity == RowMultiplicity::SingleCopy
            && right.properties.row_multiplicity == RowMultiplicity::SingleCopy
        {
            (
                NestLoopJoinDistribution::Singleton,
                Box::from([singleton_requirement(), singleton_requirement()]),
                Distribution::Singleton,
            )
        } else if right.properties.distribution == Distribution::Broadcast
            && right.properties.row_multiplicity == RowMultiplicity::Replicated
            && left.properties.row_multiplicity == RowMultiplicity::SingleCopy
        {
            (
                NestLoopJoinDistribution::BroadcastRight,
                Box::from([
                    PhysicalProperties {
                        distribution: left.properties.distribution.clone(),
                        row_multiplicity: RowMultiplicity::SingleCopy,
                        ordering: Box::default(),
                    },
                    PhysicalProperties {
                        distribution: Distribution::Broadcast,
                        row_multiplicity: RowMultiplicity::Replicated,
                        ordering: Box::default(),
                    },
                ]),
                left.properties.distribution.clone(),
            )
        } else {
            return Err(ContractLoweringError::PropertyRequirementUnsatisfied {
                node: "NestLoopJoin",
                detail: "children prove neither singleton/singleton nor single-copy-left/broadcast-right placement",
            });
        };
        let kind = lower_join_kind(join.join_type);
        if distribution == NestLoopJoinDistribution::BroadcastRight
            && !matches!(
                kind,
                ContractJoinKind::Cross
                    | ContractJoinKind::Inner
                    | ContractJoinKind::LeftOuter
                    | ContractJoinKind::LeftSemi
                    | ContractJoinKind::LeftAnti
                    | ContractJoinKind::NullAwareLeftAnti
            )
        {
            return Err(ContractLoweringError::InvalidJoin {
                node: "NestLoopJoin",
                detail: "right-broadcast placement is unsafe for this join kind",
            });
        }
        if kind == ContractJoinKind::Cross && join.condition.is_some() {
            return Err(ContractLoweringError::InvalidJoin {
                node: "NestLoopJoin",
                detail: "cross join carries a predicate",
            });
        }
        let node = self.fragment_mut().reserve_node_id()?;
        let visible = merge_visible_columns("NestLoopJoin", &left.columns, &right.columns)?;
        let predicate = join
            .condition
            .as_ref()
            .map(|predicate| {
                if predicate.data_type != DataType::Boolean {
                    return Err(ContractLoweringError::JoinPredicateIsNotBoolean {
                        node: "NestLoopJoin",
                        actual: predicate.data_type.clone(),
                    });
                }
                self.lower_expression(node, predicate, &visible)
            })
            .transpose()?;
        let JoinOutput {
            output,
            columns,
            null_extended,
        } = self.lower_join_outputs(
            node,
            JoinOutputRequest {
                kind,
                requested: &plan.output_columns,
                left: &left,
                left_columns: &plan.children[0].output_columns,
                right: &right,
                right_columns: &plan.children[1].output_columns,
            },
        )?;
        self.fragment_mut().add_join(
            node,
            [left.node, right.node],
            required_inputs,
            output.clone(),
            output_distribution,
            NodeKind::NestLoopJoin {
                kind,
                distribution,
                predicate,
                null_extended,
            },
        )?;
        let properties = self
            .fragment_mut()
            .node_output_properties(node)
            .expect("the join was just inserted")
            .clone();
        Ok(LoweredNode {
            fragment: self.current_fragment,
            node,
            output,
            columns,
            properties,
            display_names: plan
                .output_columns
                .iter()
                .map(|column| column.name.clone())
                .collect::<Vec<_>>()
                .into_boxed_slice(),
        })
    }

    fn lower_join_outputs(
        &mut self,
        node: NodeId,
        request: JoinOutputRequest<'_>,
    ) -> Result<JoinOutput, ContractLoweringError> {
        let null_sides = match request.kind {
            ContractJoinKind::LeftOuter => (false, true),
            ContractJoinKind::RightOuter => (true, false),
            ContractJoinKind::FullOuter => (true, true),
            _ => (false, false),
        };
        let mut extensions = BTreeMap::new();
        let mut output = Vec::with_capacity(request.requested.len());
        let mut columns = BTreeMap::new();
        let mut null_extended = Vec::new();
        for (ordinal, column) in request.requested.iter().enumerate() {
            let matches = [
                request
                    .left
                    .columns
                    .get(&column.column_id)
                    .copied()
                    .map(|value| (0, value)),
                request
                    .right
                    .columns
                    .get(&column.column_id)
                    .copied()
                    .map(|value| (1, value)),
            ]
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
            let [(side, source)] = matches.as_slice() else {
                return Err(ContractLoweringError::OutputColumnMismatch {
                    node: "Join",
                    ordinal,
                    detail: "column is absent from both inputs or ambiguous across inputs".into(),
                });
            };
            let source_column = if *side == 0 {
                find_output_column(request.left_columns, column.column_id)
            } else {
                find_output_column(request.right_columns, column.column_id)
            };
            let source_ty = source_column.map(value_type).ok_or(
                ContractLoweringError::OutputColumnMismatch {
                    node: "Join",
                    ordinal,
                    detail: "input column type is unavailable".into(),
                },
            )?;
            // Both sides are read in the plan's own vocabulary, where a list's
            // element is `item` and a map's entries are `entries`/`key`/
            // `value`. Reading one of them raw would make a nested column
            // differ from itself.
            if source_ty.data_type != value_type(column).data_type {
                return Err(ContractLoweringError::OutputColumnMismatch {
                    node: "Join",
                    ordinal,
                    detail: format!(
                        "output {:?} differs from its input value {:?}",
                        value_type(column).data_type,
                        source_ty.data_type
                    ),
                });
            }
            let nullable_side = if *side == 0 {
                null_sides.0
            } else {
                null_sides.1
            };
            let value = if nullable_side {
                if !column.nullable {
                    return Err(ContractLoweringError::OutputColumnMismatch {
                        node: "Join",
                        ordinal,
                        detail: "NULL-extended side publishes a non-nullable output".into(),
                    });
                }
                match extensions.get(source).copied() {
                    Some(value) => value,
                    None => {
                        let value = self.fragment_mut().add_value(
                            value_type(column),
                            ValueOrigin::NullExtended { node, of: *source },
                        )?;
                        extensions.insert(*source, value);
                        null_extended.push(value);
                        value
                    }
                }
            } else if source_ty.nullable == column.nullable {
                *source
            } else {
                return Err(ContractLoweringError::OutputColumnMismatch {
                    node: "Join",
                    ordinal,
                    detail: "output nullability is not justified by the join kind".into(),
                });
            };
            if let Some(previous) = columns.insert(column.column_id, value)
                && previous != value
            {
                return Err(ContractLoweringError::OutputColumnMismatch {
                    node: "Join",
                    ordinal,
                    detail: "one SQL column identity maps to different output values".into(),
                });
            }
            output.push(value);
        }
        Ok(JoinOutput {
            output: output.into_boxed_slice(),
            columns,
            null_extended: null_extended.into_boxed_slice(),
        })
    }

    fn lower_set_op(
        &mut self,
        plan: &PhysicalPlanNode,
        set_op: &crate::planner::physical::PhysicalSetOpNode,
    ) -> Result<LoweredNode, ContractLoweringError> {
        if plan.children.len() < 2 {
            return Err(ContractLoweringError::ArityMismatch {
                context: "SetOp inputs",
                expected: 2,
                actual: plan.children.len(),
            });
        }
        require_output_shape("SetOp", &plan.output_columns, &set_op.output_columns)?;
        if set_op.child_output_columns.len() != plan.children.len() {
            return Err(ContractLoweringError::ArityMismatch {
                context: "SetOp child mappings",
                expected: plan.children.len(),
                actual: set_op.child_output_columns.len(),
            });
        }
        let kind = match set_op.kind {
            PlanSetOpKind::UnionAll => SetOperationKind::UnionAll,
            PlanSetOpKind::Intersect => SetOperationKind::Intersect,
            PlanSetOpKind::Except => SetOperationKind::Except,
            PlanSetOpKind::UnionDistinct => {
                return Err(ContractLoweringError::UnsupportedSetOp {
                    detail: "UnionDistinct must be represented as UnionAll plus an explicit distinct aggregate",
                });
            }
        };
        for (child, mapped_columns) in plan.children.iter().zip(&set_op.child_output_columns) {
            require_output_shape("SetOp child", &child.output_columns, mapped_columns)?;
        }
        // A set operation that compares its branches needs them in one
        // partition space. A branch that arrives already partitioned brings
        // the space with it -- a chained `EXCEPT` feeding an `INTERSECT` is
        // one -- and the shuffles beside it join that space; only when every
        // branch is a shuffle of its own is a new space minted here.
        let shuffles_itself = |child: &PhysicalPlanNode| {
            matches!(
                &child.kind,
                PhysicalPlanKind::Redistribute(redistribute)
                    if matches!(redistribute.mode, RedistributeMode::Hash { .. })
            )
        };
        let mut lowered_children = plan
            .children
            .iter()
            .map(|_| None)
            .collect::<Vec<Option<LoweredNode>>>();
        let mut shared_hash_scheme = None;
        if kind != SetOperationKind::UnionAll {
            for (ordinal, child) in plan.children.iter().enumerate() {
                if shuffles_itself(child) {
                    continue;
                }
                let lowered = self.lower_node(child)?;
                if let Distribution::Hash { scheme, .. } = &lowered.properties.distribution {
                    if shared_hash_scheme
                        .as_ref()
                        .is_some_and(|expected| expected != scheme)
                    {
                        return Err(ContractLoweringError::InvalidSetOp {
                            detail: "inputs arrive in different hash partition spaces",
                        });
                    }
                    shared_hash_scheme = Some(scheme.clone());
                }
                lowered_children[ordinal] = Some(lowered);
            }
            if shared_hash_scheme.is_none() && plan.children.iter().all(shuffles_itself) {
                shared_hash_scheme = Some(self.allocate_hash_scheme()?);
            }
        }
        let mut inputs = Vec::with_capacity(plan.children.len());
        for (ordinal, (child, mapped_columns)) in plan
            .children
            .iter()
            .zip(&set_op.child_output_columns)
            .enumerate()
        {
            let lowered = match lowered_children[ordinal].take() {
                Some(lowered) => lowered,
                None => match (&child.kind, &shared_hash_scheme) {
                    (PhysicalPlanKind::Redistribute(redistribute), Some(scheme)) => {
                        self.lower_redistribute(child, redistribute, Some(scheme.clone()))?
                    }
                    _ => self.lower_node(child)?,
                },
            };
            if lowered.fragment != self.current_fragment {
                return Err(ContractLoweringError::UnexpectedFragment {
                    node: "SetOp",
                    expected: self.current_fragment,
                    actual: lowered.fragment,
                });
            }
            // A set operation's column is one type, which its branches were
            // reconciled to while the statement was analyzed: a decimal beside
            // a double answers as a double. Carry that reconciliation here, so
            // the branches this node reads already agree with what it
            // publishes.
            let lowered =
                self.align_set_op_branch(lowered, mapped_columns, &plan.output_columns)?;
            inputs.push(lowered);
        }
        let node = self.fragment_mut().reserve_node_id()?;
        let mut published_types = plan
            .output_columns
            .iter()
            .map(value_type)
            .collect::<Vec<_>>();
        let mut mappings = Vec::with_capacity(inputs.len());
        for (input, mapped_columns) in inputs.iter().zip(&set_op.child_output_columns) {
            let mut mapping = Vec::with_capacity(mapped_columns.len());
            for (ordinal, column) in mapped_columns.iter().enumerate() {
                let value = input.columns.get(&column.column_id).copied().ok_or(
                    ContractLoweringError::UnknownColumnReference(column.column_id),
                )?;
                // Physical join lowering can widen a branch after the
                // optimizer published the set operation's logical columns.
                // The final output admits null if any actual branch does.
                let branch = self.value_declared_type(value)?;
                let published = &mut published_types[ordinal];
                if branch.data_type != published.data_type {
                    return Err(ContractLoweringError::OutputColumnMismatch {
                        node: "SetOp",
                        ordinal,
                        detail: format!("branch {branch:?} does not fit output {published:?}"),
                    });
                }
                published.nullable |= branch.nullable;
                mapping.push(value);
            }
            mappings.push(mapping.into_boxed_slice());
        }
        let mappings = mappings.into_boxed_slice();
        let mut output = Vec::with_capacity(plan.output_columns.len());
        let mut columns = BTreeMap::new();
        for (ordinal, column) in plan.output_columns.iter().enumerate() {
            let value = match columns.get(&column.column_id).copied() {
                Some(value) => value,
                None => {
                    let value = self.fragment_mut().add_value(
                        published_types[ordinal].clone(),
                        ValueOrigin::NodeOutput {
                            node,
                            output_ordinal: checked_ordinal("SetOp output", ordinal)?,
                        },
                    )?;
                    columns.insert(column.column_id, value);
                    value
                }
            };
            output.push(value);
        }
        let all_singleton = inputs.iter().all(|input| {
            input.properties.distribution == Distribution::Singleton
                && input.properties.row_multiplicity == RowMultiplicity::SingleCopy
        });
        let (required_inputs, output_distribution) = match kind {
            SetOperationKind::UnionAll => (
                inputs
                    .iter()
                    .map(|input| passthrough_requirement(&input.properties))
                    .collect::<Vec<_>>()
                    .into_boxed_slice(),
                if all_singleton {
                    Distribution::Singleton
                } else {
                    Distribution::Unconstrained
                },
            ),
            SetOperationKind::Intersect | SetOperationKind::Except if all_singleton => (
                vec![singleton_requirement(); inputs.len()].into_boxed_slice(),
                Distribution::Singleton,
            ),
            SetOperationKind::Intersect | SetOperationKind::Except => {
                let pattern = occurrence_representatives(&mappings[0]);
                if mappings
                    .iter()
                    .skip(1)
                    .any(|mapping| occurrence_representatives(mapping) != pattern)
                {
                    return Err(ContractLoweringError::InvalidSetOp {
                        detail: "child mappings have different duplicate-occurrence equivalence",
                    });
                }
                let representatives = pattern
                    .iter()
                    .enumerate()
                    .filter_map(|(ordinal, representative)| {
                        (*representative == ordinal).then_some(ordinal)
                    })
                    .collect::<Vec<_>>();
                let mut scheme = None;
                let mut required = Vec::with_capacity(inputs.len());
                for (input, mapping) in inputs.iter().zip(mappings.iter()) {
                    let Distribution::Hash {
                        keys,
                        scheme: input_scheme,
                    } = &input.properties.distribution
                    else {
                        return Err(ContractLoweringError::MissingPlannerFact {
                            node: "SetOp",
                            fact: "singleton inputs or one exact shared hash partition scheme",
                        });
                    };
                    let expected_keys = representatives
                        .iter()
                        .map(|ordinal| mapping[*ordinal])
                        .collect::<Vec<_>>();
                    if keys.as_ref() != expected_keys {
                        return Err(ContractLoweringError::InvalidSetOp {
                            detail: "hash keys do not cover each comparison equivalence class",
                        });
                    }
                    if scheme
                        .as_ref()
                        .is_some_and(|expected| expected != input_scheme)
                    {
                        return Err(ContractLoweringError::InvalidSetOp {
                            detail: "inputs use different hash partition schemes",
                        });
                    }
                    scheme = Some(input_scheme.clone());
                    required.push(PhysicalProperties {
                        distribution: input.properties.distribution.clone(),
                        row_multiplicity: RowMultiplicity::SingleCopy,
                        ordering: Box::default(),
                    });
                }
                let scheme = scheme.ok_or(ContractLoweringError::MissingPlannerFact {
                    node: "SetOp",
                    fact: "a non-empty exact input partition scheme",
                })?;
                (
                    required.into_boxed_slice(),
                    Distribution::Hash {
                        keys: representatives
                            .iter()
                            .map(|ordinal| output[*ordinal])
                            .collect(),
                        scheme,
                    },
                )
            }
        };
        self.fragment_mut().add_row_consuming(
            node,
            inputs.iter().map(|input| input.node).collect(),
            RequiredInputs::Exact(required_inputs),
            output_distribution,
            output.clone().into_boxed_slice(),
            NodeKind::SetOp {
                kind,
                input_mappings: mappings,
            },
        )?;
        let properties = self
            .fragment_mut()
            .node_output_properties(node)
            .expect("the node was just inserted")
            .clone();
        Ok(LoweredNode {
            fragment: self.current_fragment,
            node,
            output: output.into_boxed_slice(),
            columns,
            properties,
            display_names: plan
                .output_columns
                .iter()
                .map(|column| column.name.clone())
                .collect::<Vec<_>>()
                .into_boxed_slice(),
        })
    }

    fn lower_hash_aggregate(
        &mut self,
        plan: &PhysicalPlanNode,
        aggregate: &crate::planner::physical::PhysicalHashAggregateNode,
    ) -> Result<LoweredNode, ContractLoweringError> {
        expect_children(plan, 1)?;
        require_output_shape(
            "HashAggregate",
            &plan.output_columns,
            &aggregate.output_columns,
        )?;
        // The layout is what this aggregate produces; the output columns are
        // what stands above it. A grouping-set aggregate groups by a grouping
        // id it never publishes, so the layout is a superset and every visible
        // column has to be found in it rather than stand at the same ordinal.
        require_outputs_within_layout(
            "HashAggregate layout",
            &plan.output_columns,
            &aggregate.output_layout,
        )?;
        if aggregate.group_by.len() != aggregate.output_layout.group_key_columns.len()
            || aggregate.aggregates.len() != aggregate.output_layout.aggregate_columns.len()
            || aggregate.is_merge.len() != aggregate.aggregates.len()
        {
            return Err(ContractLoweringError::InvalidAggregate {
                detail: "group/call/merge arities differ from the output layout",
            });
        }

        let (phases, child) = match aggregate.mode {
            AggMode::Single => {
                if aggregate.is_merge.iter().any(|merge| *merge) {
                    return Err(ContractLoweringError::InvalidAggregate {
                        detail: "single aggregate consumes an intermediate state",
                    });
                }
                let child = self.lower_node(&plan.children[0])?;
                let child = if aggregate.group_by.is_empty() {
                    self.ensure_singleton(child, &plan.children[0].output_columns)?
                } else {
                    child
                };
                (
                    vec![AggregatePhase::Single; aggregate.aggregates.len()],
                    child,
                )
            }
            AggMode::Global => {
                // A node that finishes its groups need not be merging every
                // call: `count(distinct x), sum(y)` splits into one that merges
                // the sum's state while it still reads x's values. Only the
                // merging calls pair with a phase below, so only they take a
                // sequence.
                let sequences = aggregate
                    .is_merge
                    .iter()
                    .filter(|merge| **merge)
                    .map(|_| self.allocate_aggregate_sequence())
                    .collect::<Result<Vec<_>, _>>()?
                    .into_boxed_slice();
                let merges = !sequences.is_empty();
                let previous_sequences =
                    self.pending_aggregate_sequences.replace(sequences.clone());
                let previous_used =
                    std::mem::replace(&mut self.pending_aggregate_sequence_used, false);
                let child_result = self.lower_node(&plan.children[0]);
                let used = self.pending_aggregate_sequence_used;
                self.pending_aggregate_sequences = previous_sequences;
                self.pending_aggregate_sequence_used = previous_used;
                let child = child_result?;
                if merges && !used {
                    return Err(ContractLoweringError::MissingPlannerFact {
                        node: "HashAggregate",
                        fact: "a structurally connected Local producer for every Global call",
                    });
                }
                let child = if aggregate.group_by.is_empty() {
                    self.ensure_singleton(child, &plan.children[0].output_columns)?
                } else {
                    child
                };
                let mut taken = sequences.iter().copied();
                let phases = aggregate
                    .is_merge
                    .iter()
                    .map(|merge| {
                        if *merge {
                            taken
                                .next()
                                .map(|sequence| AggregatePhase::Final { sequence })
                                .ok_or(ContractLoweringError::InvalidAggregate {
                                    detail: "global aggregate has more merging calls than sequences",
                                })
                        } else {
                            Ok(AggregatePhase::Single)
                        }
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                (phases, child)
            }
            AggMode::Local => {
                if aggregate.is_merge.iter().any(|merge| *merge) {
                    return Err(ContractLoweringError::InvalidAggregate {
                        detail: "local aggregate consumes an intermediate state",
                    });
                }
                let sequences = self.pending_aggregate_sequences.clone().ok_or(
                    ContractLoweringError::MissingPlannerFact {
                        node: "HashAggregate",
                        fact: "the exact downstream final sequence for a Local producer",
                    },
                )?;
                if sequences.len() != aggregate.aggregates.len() {
                    return Err(ContractLoweringError::InvalidAggregate {
                        detail: "Local and Global aggregate call arities differ",
                    });
                }
                self.pending_aggregate_sequence_used = true;
                let child = self.lower_node(&plan.children[0])?;
                (
                    sequences
                        .iter()
                        .copied()
                        .map(|sequence| AggregatePhase::Partial { sequence })
                        .collect(),
                    child,
                )
            }
            AggMode::DistinctGlobal | AggMode::DistinctLocal => {
                // A dedup phase, and the per-instance rollup that reads it,
                // stand in the middle of a chain: no call here finishes, so
                // every one either merges the state below it or starts the
                // state above it. The sequences are the ones the finishing
                // node allocated, one per call, and this node keeps its own
                // while handing the merging half further down.
                let sequences = self.pending_aggregate_sequences.clone().ok_or(
                    ContractLoweringError::MissingPlannerFact {
                        node: "HashAggregate",
                        fact: "the exact downstream final sequence for a DISTINCT producer",
                    },
                )?;
                if sequences.len() != aggregate.aggregates.len() {
                    return Err(ContractLoweringError::InvalidAggregate {
                        detail: "DISTINCT and downstream aggregate call arities differ",
                    });
                }
                self.pending_aggregate_sequence_used = true;
                let below = aggregate
                    .is_merge
                    .iter()
                    .zip(sequences.iter().copied())
                    .filter_map(|(merge, sequence)| merge.then_some(sequence))
                    .collect::<Vec<_>>()
                    .into_boxed_slice();
                let merges = !below.is_empty();
                let previous_sequences = self.pending_aggregate_sequences.replace(below);
                let previous_used =
                    std::mem::replace(&mut self.pending_aggregate_sequence_used, false);
                let child_result = self.lower_node(&plan.children[0]);
                let used = self.pending_aggregate_sequence_used;
                self.pending_aggregate_sequences = previous_sequences;
                self.pending_aggregate_sequence_used = previous_used;
                let child = child_result?;
                if merges && !used {
                    return Err(ContractLoweringError::MissingPlannerFact {
                        node: "HashAggregate",
                        fact: "a structurally connected producer for every merging DISTINCT call",
                    });
                }
                // The dedup phase reads a shuffle on its own keys and so
                // finishes them; the rollup above it is per instance. Only
                // the one that finishes its groups needs them gathered when
                // it groups by nothing at all.
                let child =
                    if aggregate.mode == AggMode::DistinctGlobal && aggregate.group_by.is_empty() {
                        self.ensure_singleton(child, &plan.children[0].output_columns)?
                    } else {
                        child
                    };
                let phases = aggregate
                    .is_merge
                    .iter()
                    .zip(sequences.iter().copied())
                    .map(|(merge, sequence)| {
                        if *merge {
                            AggregatePhase::Intermediate { sequence }
                        } else {
                            AggregatePhase::Partial { sequence }
                        }
                    })
                    .collect();
                (phases, child)
            }
        };
        if child.fragment != self.current_fragment {
            return Err(ContractLoweringError::UnexpectedFragment {
                node: "HashAggregate",
                expected: self.current_fragment,
                actual: child.fragment,
            });
        }

        // An aggregate groups by values that reach it, never by an expression
        // it evaluates itself: its own port carries only what its calls
        // produce and what its input passed through. A statement that groups
        // by an expression gets that expression materialized below it.
        let group_keys = aggregate.group_by.iter().collect::<Vec<_>>();
        let (child, materialized_keys) = self.materialize_derived_keys(child, &group_keys)?;

        let node = self.fragment_mut().reserve_node_id()?;
        let mut group_by = Vec::with_capacity(aggregate.group_by.len());
        let mut group_input_values = Vec::with_capacity(aggregate.group_by.len());
        let mut output = Vec::with_capacity(plan.output_columns.len());
        let mut columns = BTreeMap::new();
        for (ordinal, (expression, column)) in aggregate
            .group_by
            .iter()
            .zip(&aggregate.output_layout.group_key_columns)
            .enumerate()
        {
            if expression_type(expression) != value_type(column) {
                return Err(ContractLoweringError::ExpressionTypeMismatch {
                    context: format!("HashAggregate group key {ordinal}"),
                    expected: value_type(column),
                    actual: expression_type(expression),
                });
            }
            let input_value = match materialized_keys[ordinal] {
                Some(value) => Some(value),
                None => identity_column_ref(expression)
                    .map(|column_id| {
                        child
                            .columns
                            .get(&column_id)
                            .copied()
                            .ok_or(ContractLoweringError::UnknownColumnReference(column_id))
                    })
                    .transpose()?,
            };
            let expression_id = match input_value {
                Some(value) => {
                    let ty = self.value_declared_type(value)?;
                    self.fragment_mut()
                        .add_expression(node, ty, ContractExprKind::Value(value))?
                }
                None => self.lower_expression(node, expression, &child.columns)?,
            };
            let value = match input_value {
                Some(value) => value,
                None => {
                    let ty = published_value_type(
                        &value_type(column),
                        &self.expression_value_type(expression_id)?,
                    );
                    self.fragment_mut().add_value(
                        ty,
                        ValueOrigin::Expr {
                            node,
                            expr: expression_id,
                        },
                    )?
                }
            };
            insert_output_column("HashAggregate", ordinal, column, value, &mut columns)?;
            group_by.push((expression_id, value));
            group_input_values.push(input_value);
            output.push(value);
        }

        // The states this node merges stand at the tail of its child's
        // columns, after that child's own grouping keys, in the order this
        // node's merging calls are written. The count is what pairs them: a
        // child produces exactly one state per merging call above it, which
        // is also what the sequence threading above relies on. This node's
        // other calls read values and have no state below them, so its own
        // call ordinals do not index the child.
        let merging_calls = aggregate.is_merge.iter().filter(|merge| **merge).count();
        let state_base = plan.children[0]
            .output_columns
            .len()
            .checked_sub(merging_calls)
            .ok_or(ContractLoweringError::InvalidAggregate {
                detail: "state-consuming aggregates outnumber the states in their child",
            })?;
        let mut merge_ordinal = 0usize;
        let mut calls = Vec::with_capacity(aggregate.aggregates.len());
        for (call_ordinal, ((call, column), phase)) in aggregate
            .aggregates
            .iter()
            .zip(&aggregate.output_layout.aggregate_columns)
            .zip(phases)
            .enumerate()
        {
            if call.output_column_id != column.column_id {
                return Err(ContractLoweringError::OutputColumnMismatch {
                    node: "HashAggregate",
                    ordinal: call_ordinal + aggregate.group_by.len(),
                    detail: "call output identity differs from the output layout".into(),
                });
            }
            let binding = lower_aggregate_binding(call, phase)?;
            let mut arguments = Vec::new();
            let mut order_by = Vec::new();
            if phase.consumes_logical_arguments() {
                arguments = call
                    .args
                    .iter()
                    .map(|argument| self.lower_expression(node, argument, &child.columns))
                    .collect::<Result<Vec<_>, _>>()?;
                order_by = call
                    .order_by
                    .iter()
                    .map(|item| {
                        Ok(SortExpr {
                            expr: self.lower_expression(node, &item.expr, &child.columns)?,
                            direction: if item.asc {
                                SortDirection::Ascending
                            } else {
                                SortDirection::Descending
                            },
                            null_ordering: if item.nulls_first {
                                NullOrdering::First
                            } else {
                                NullOrdering::Last
                            },
                        })
                    })
                    .collect::<Result<Vec<_>, ContractLoweringError>>()?;
            } else {
                // A state-consuming phase reads what the phase before it
                // produced, not the arguments that phase was given: the
                // state standing at this aggregate's own ordinal among the
                // child's aggregate outputs. Ordering was settled while the
                // values were still there, so a phase that only merges
                // states cannot carry one. DISTINCT it may still be written
                // with -- the planner keeps the flag on every phase of a
                // `count(distinct x)` because it is how the call is named --
                // and the phase simply does not apply it again.
                if !call.order_by.is_empty() {
                    return Err(ContractLoweringError::InvalidAggregate {
                        detail: "state-consuming aggregate carries no ORDER BY",
                    });
                }
                let state_column = plan.children[0]
                    .output_columns
                    .get(state_base + merge_ordinal)
                    .ok_or(ContractLoweringError::InvalidAggregate {
                        detail: "state-consuming aggregate has no state input in its child",
                    })?;
                merge_ordinal += 1;
                let state = child.columns.get(&state_column.column_id).copied().ok_or(
                    ContractLoweringError::UnknownColumnReference(state_column.column_id),
                )?;
                let state_type = self.value_declared_type(state)?;
                // The state must be the one this very aggregate produces, so
                // an ordinal that lines up against the wrong column is caught
                // here rather than reaching the backend as a merge of another
                // aggregate's state.
                if state_type.data_type != binding.intermediate_type.data_type {
                    return Err(ContractLoweringError::InvalidAggregate {
                        detail: "state-consuming aggregate reads a state of another type",
                    });
                }
                arguments.push(self.fragment_mut().add_expression(
                    node,
                    state_type,
                    ContractExprKind::Value(state),
                )?);
            }
            let call_id = self.allocate_aggregate_call()?;
            let expected_output_type = if phase.produces_final_result() {
                binding.function.result_type.clone()
            } else {
                binding.intermediate_type.clone()
            };
            // The column may admit null this phase's output never produces
            // -- a count standing where the statement types a nullable
            // integer is sound. It may not claim the reverse, and the type
            // itself must be the one this phase produces.
            let layout = value_type(column);
            if layout.data_type != expected_output_type.data_type
                || (expected_output_type.nullable && !layout.nullable)
            {
                return Err(ContractLoweringError::OutputColumnMismatch {
                    node: "HashAggregate",
                    ordinal: call_ordinal + aggregate.group_by.len(),
                    detail: format!(
                        "aggregate {} phase output type {expected_output_type:?} differs from layout {layout:?}",
                        call.name
                    ),
                });
            }
            let origin = if phase.produces_final_result() {
                ValueOrigin::AggregateResult { call: call_id }
            } else {
                ValueOrigin::AggregateState {
                    call: call_id,
                    phase,
                }
            };
            let value = self
                .fragment_mut()
                .add_value(expected_output_type, origin)?;
            insert_output_column(
                "HashAggregate",
                call_ordinal + aggregate.group_by.len(),
                column,
                value,
                &mut columns,
            )?;
            output.push(value);
            calls.push(ContractAggregateCall {
                id: call_id,
                binding,
                arguments: arguments.into_boxed_slice(),
                // DISTINCT is applied where the values are, by the phase
                // that reads them. What a merging phase reads is a state
                // whose function identity already says it was built from
                // distinct values, so it does not dedup again.
                distinct: call.distinct && phase.consumes_logical_arguments(),
                order_by: order_by.into_boxed_slice(),
                output: value,
            });
        }

        let completes_groups = mode_completes_groups(aggregate.mode);
        let required_distribution = if completes_groups {
            if group_by.is_empty() {
                if child.properties.distribution != Distribution::Singleton {
                    return Err(ContractLoweringError::PropertyRequirementUnsatisfied {
                        node: "HashAggregate",
                        detail: "global scalar aggregate input is not singleton",
                    });
                }
            } else {
                let keys = group_input_values
                    .iter()
                    .map(|value| {
                        value.ok_or(ContractLoweringError::PropertyRequirementUnsatisfied {
                            node: "HashAggregate",
                            detail: "final grouping expression is not a direct input value",
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                if !distribution_colocates(&child.properties.distribution, &keys) {
                    return Err(ContractLoweringError::PropertyRequirementUnsatisfied {
                        node: "HashAggregate",
                        detail: "final grouping keys lack exact co-location",
                    });
                }
            }
            child.properties.distribution.clone()
        } else {
            Distribution::Unconstrained
        };
        let required = PhysicalProperties {
            distribution: required_distribution,
            row_multiplicity: RowMultiplicity::SingleCopy,
            ordering: Box::default(),
        };
        require_single_copy_input("HashAggregate", &child.properties)?;
        let output_distribution = aggregate_output_distribution(&child.properties, &output);
        self.fragment_mut().add_row_consuming(
            node,
            Box::from([child.node]),
            RequiredInputs::Exact(Box::from([required])),
            output_distribution,
            output.clone().into_boxed_slice(),
            NodeKind::Aggregate {
                group_by: group_by.into_boxed_slice(),
                calls: calls.into_boxed_slice(),
                grouping: if completes_groups {
                    AggregateGrouping::Complete
                } else {
                    AggregateGrouping::Partial
                },
            },
        )?;
        let properties = self
            .fragment_mut()
            .node_output_properties(node)
            .expect("the node was just inserted")
            .clone();
        for intent in &aggregate.topn_runtime_filter_builds {
            if self.unstatable_runtime_filters.contains(&intent.filter_id) {
                continue;
            }
            let group_key_ordinal = u32::try_from(intent.group_key_ordinal).map_err(|_| {
                ContractLoweringError::InvalidRuntimeFilter {
                    id: intent.filter_id,
                    detail: "Aggregate TopN group-key ordinal exceeds u32".to_string(),
                }
            })?;
            let input_value = group_input_values
                .get(intent.group_key_ordinal)
                .copied()
                .flatten()
                .ok_or_else(|| ContractLoweringError::InvalidRuntimeFilter {
                    id: intent.filter_id,
                    detail: "Aggregate TopN key is not one direct lowered ValueId".to_string(),
                })?;
            let intent_input = identity_column_ref(&intent.group_key_expr)
                .and_then(|column| child.columns.get(&column))
                .copied();
            if intent_input != Some(input_value) {
                return Err(ContractLoweringError::InvalidRuntimeFilter {
                    id: intent.filter_id,
                    detail: "Aggregate TopN intent differs from the exact frozen group key"
                        .to_string(),
                });
            }
            let filter_id = runtime_filter_id(intent.filter_id)?;
            self.attach_runtime_filter(self.current_fragment, filter_id)?;
            let pending = PendingRuntimeFilterBuild::AggregateTopN {
                fragment: self.current_fragment,
                node,
                group_key_ordinal,
                input_value,
                limit: intent.limit.get(),
                direction: match intent.direction {
                    crate::planner::runtime_filter::contract::SortDirection::Ascending => {
                        SortDirection::Ascending
                    }
                    crate::planner::runtime_filter::contract::SortDirection::Descending => {
                        SortDirection::Descending
                    }
                },
                null_ordering: match intent.null_order {
                    crate::planner::runtime_filter::contract::NullOrder::First => {
                        NullOrdering::First
                    }
                    crate::planner::runtime_filter::contract::NullOrder::Last => NullOrdering::Last,
                },
            };
            if self
                .runtime_filter_builds
                .insert(intent.filter_id, pending)
                .is_some()
            {
                return Err(ContractLoweringError::InvalidRuntimeFilter {
                    id: intent.filter_id,
                    detail: "more than one producer owns the filter".to_string(),
                });
            }
        }
        // What stands above this aggregate reads its layout, not the shorter
        // list it publishes: the planner writes a pass-through node's columns
        // against what the operator materializes, and narrows only at a
        // projection, which selects by column identity anyway.
        Ok(LoweredNode {
            fragment: self.current_fragment,
            node,
            output: output.into_boxed_slice(),
            columns,
            properties,
            display_names: aggregate
                .output_layout
                .group_key_columns
                .iter()
                .chain(&aggregate.output_layout.aggregate_columns)
                .map(|column| column.name.clone())
                .collect::<Vec<_>>()
                .into_boxed_slice(),
        })
    }

    fn lower_values(
        &mut self,
        plan: &PhysicalPlanNode,
        values: &crate::planner::payload::PlanValuesNode,
    ) -> Result<LoweredNode, ContractLoweringError> {
        expect_children(plan, 0)?;
        require_output_shape("Values", &plan.output_columns, &values.columns)?;

        let node = self.fragment_mut().reserve_node_id()?;
        let mut column_values = BTreeMap::new();
        let mut output = Vec::with_capacity(plan.output_columns.len());
        for (ordinal, column) in plan.output_columns.iter().enumerate() {
            if column_values.contains_key(&column.column_id) {
                return Err(ContractLoweringError::DuplicateColumnDefinition {
                    node: "Values",
                    column: column.column_id,
                });
            }
            let value = self.fragment_mut().add_value(
                value_type(column),
                ValueOrigin::NodeOutput {
                    node,
                    output_ordinal: checked_ordinal("Values output", ordinal)?,
                },
            )?;
            column_values.insert(column.column_id, value);
            output.push(value);
        }

        let mut rows = Vec::with_capacity(values.rows.len());
        for (row_ordinal, row) in values.rows.iter().enumerate() {
            if row.len() != plan.output_columns.len() {
                return Err(ContractLoweringError::ArityMismatch {
                    context: "Values row",
                    expected: plan.output_columns.len(),
                    actual: row.len(),
                });
            }
            let mut lowered_row = Vec::with_capacity(row.len());
            for (column_ordinal, expression) in row.iter().enumerate() {
                let expected = value_type(&plan.output_columns[column_ordinal]);
                lowered_row.push(self.lower_values_expression(
                    node,
                    expression,
                    &expected,
                    row_ordinal,
                    column_ordinal,
                )?);
            }
            rows.push(lowered_row.into_boxed_slice());
        }

        let _properties = singleton_properties();
        self.fragment_mut().add_values(
            node,
            rows.into_boxed_slice(),
            output.clone().into_boxed_slice(),
        )?;
        let properties = self
            .fragment_mut()
            .node_output_properties(node)
            .expect("the node was just inserted")
            .clone();
        Ok(LoweredNode {
            fragment: self.current_fragment,
            node,
            output: output.into_boxed_slice(),
            columns: column_values,
            properties,
            display_names: plan
                .output_columns
                .iter()
                .map(|column| column.name.clone())
                .collect::<Vec<_>>()
                .into_boxed_slice(),
        })
    }

    fn lower_filter(
        &mut self,
        plan: &PhysicalPlanNode,
        predicate: &TypedExpr,
    ) -> Result<LoweredNode, ContractLoweringError> {
        expect_children(plan, 1)?;
        let child = self.lower_node(&plan.children[0])?;
        require_passthrough_shape("Filter", &plan.output_columns, &plan.children[0])?;
        if predicate.data_type != DataType::Boolean {
            return Err(ContractLoweringError::PredicateIsNotBoolean {
                actual: predicate.data_type.clone(),
            });
        }

        let node = self.fragment_mut().reserve_node_id()?;
        // Split the top-level conjunction here rather than lowering one nested
        // expression: the filter owns the conjunct list, so consumers that
        // reason per conjunct never have to re-split a tree.
        let predicates =
            self.lower_boolean_connective(node, predicate, BinOp::And, &child.columns)?;
        self.fragment_mut()
            .add_filter(node, child.node, predicates)?;
        let properties = self
            .fragment_mut()
            .node_output_properties(node)
            .expect("the filter was just inserted")
            .clone();
        Ok(LoweredNode {
            fragment: self.current_fragment,
            node,
            output: child.output,
            columns: child.columns,
            properties,
            display_names: child.display_names,
        })
    }

    /// Cast one set-operation branch's columns to the types the operation
    /// publishes.
    ///
    /// Returns the branch unchanged where every column already answers with
    /// the published type.
    fn align_set_op_branch(
        &mut self,
        branch: LoweredNode,
        mapped_columns: &[OutputColumn],
        output_columns: &[OutputColumn],
    ) -> Result<LoweredNode, ContractLoweringError> {
        // Both sides read in the plan's own vocabulary, where a map's entries
        // are named `entries`/`key`/`value`: a branch that came from a
        // provider would otherwise differ from a branch that came from a
        // projection over decoration neither of them compares by.
        let mut needs_cast = false;
        for (column, published) in mapped_columns.iter().zip(output_columns) {
            let value = branch.columns.get(&column.column_id).copied().ok_or(
                ContractLoweringError::UnknownColumnReference(column.column_id),
            )?;
            if self.value_declared_type(value)?.data_type != value_type(published).data_type {
                needs_cast = true;
                break;
            }
        }
        if !needs_cast {
            return Ok(branch);
        }

        let node = self.fragment_mut().reserve_node_id()?;
        let mut expressions = Vec::with_capacity(mapped_columns.len());
        let mut output = Vec::with_capacity(mapped_columns.len());
        let mut columns = BTreeMap::new();
        for (column, published) in mapped_columns.iter().zip(output_columns) {
            let source = branch.columns.get(&column.column_id).copied().ok_or(
                ContractLoweringError::UnknownColumnReference(column.column_id),
            )?;
            let source_type = self.value_declared_type(source)?;
            let expression = self.fragment_mut().add_expression(
                node,
                source_type.clone(),
                ContractExprKind::Value(source),
            )?;
            let published_type = value_type(published).data_type;
            let (expression, value) = if source_type.data_type == published_type {
                (expression, source)
            } else {
                let expression = self.cast_expression_to(node, expression, &published_type)?;
                let value = self.fragment_mut().add_value(
                    ValueType::new(published_type, source_type.nullable),
                    ValueOrigin::Expr {
                        node,
                        expr: expression,
                    },
                )?;
                (expression, value)
            };
            expressions.push((expression, value));
            output.push(value);
            columns.insert(column.column_id, value);
        }
        self.fragment_mut().add_project(
            node,
            branch.node,
            expressions.into_boxed_slice(),
            output.clone().into_boxed_slice(),
        )?;
        let properties = self
            .fragment_mut()
            .node_output_properties(node)
            .expect("the projection was just inserted")
            .clone();
        Ok(LoweredNode {
            fragment: self.current_fragment,
            node,
            output: output.into_boxed_slice(),
            columns,
            properties,
            display_names: branch.display_names,
        })
    }

    /// Materialize the group keys this aggregate cannot evaluate itself.
    ///
    /// Returns the input the aggregate should read and, per group-by ordinal,
    /// the value that now carries that key -- `None` where the key already
    /// reached the aggregate as a column of its input.
    fn materialize_derived_keys(
        &mut self,
        child: LoweredNode,
        keys: &[&TypedExpr],
    ) -> Result<(LoweredNode, Vec<Option<ValueId>>), ContractLoweringError> {
        let derived = keys
            .iter()
            .map(|expression| {
                identity_column_ref(expression)
                    .is_none_or(|column| !child.columns.contains_key(&column))
            })
            .collect::<Vec<_>>();
        if !derived.iter().any(|derived| *derived) {
            return Ok((child, vec![None; keys.len()]));
        }

        let node = self.fragment_mut().reserve_node_id()?;
        let mut expressions = Vec::with_capacity(child.columns.len() + keys.len());
        let mut output = Vec::with_capacity(child.columns.len() + keys.len());
        let mut passed = BTreeSet::new();
        for value in child.columns.values().copied() {
            if !passed.insert(value) {
                continue;
            }
            let ty = self.value_declared_type(value)?;
            let expression =
                self.fragment_mut()
                    .add_expression(node, ty, ContractExprKind::Value(value))?;
            expressions.push((expression, value));
            output.push(value);
        }

        let mut materialized = Vec::with_capacity(keys.len());
        for (expression, derived) in keys.iter().zip(&derived) {
            if !derived {
                materialized.push(None);
                continue;
            }
            let expression_id = self.lower_expression(node, expression, &child.columns)?;
            let ty = published_value_type(
                &expression_type(expression),
                &self.expression_value_type(expression_id)?,
            );
            let value = self.fragment_mut().add_value(
                ty,
                ValueOrigin::Expr {
                    node,
                    expr: expression_id,
                },
            )?;
            expressions.push((expression_id, value));
            output.push(value);
            materialized.push(Some(value));
        }

        self.fragment_mut().add_project(
            node,
            child.node,
            expressions.into_boxed_slice(),
            output.clone().into_boxed_slice(),
        )?;
        let properties = self
            .fragment_mut()
            .node_output_properties(node)
            .expect("the projection was just inserted")
            .clone();
        Ok((
            LoweredNode {
                fragment: self.current_fragment,
                node,
                output: output.into_boxed_slice(),
                columns: child.columns,
                properties,
                display_names: child.display_names,
            },
            materialized,
        ))
    }

    fn lower_project(
        &mut self,
        plan: &PhysicalPlanNode,
        items: &[crate::analysis::ProjectItem],
    ) -> Result<LoweredNode, ContractLoweringError> {
        expect_children(plan, 1)?;
        if items.len() != plan.output_columns.len() {
            return Err(ContractLoweringError::ArityMismatch {
                context: "Project output",
                expected: plan.output_columns.len(),
                actual: items.len(),
            });
        }
        let child = self.lower_node(&plan.children[0])?;
        let node = self.fragment_mut().reserve_node_id()?;
        let mut expressions = Vec::with_capacity(items.len());
        let mut output = Vec::with_capacity(items.len());
        let mut columns = BTreeMap::new();

        for (ordinal, (item, column)) in items.iter().zip(&plan.output_columns).enumerate() {
            if item.output_column_id != column.column_id {
                return Err(ContractLoweringError::OutputColumnMismatch {
                    node: "Project",
                    ordinal,
                    detail: format!(
                        "item column {} differs from output column {}",
                        item.output_column_id, column.column_id
                    ),
                });
            }
            // The column a projection publishes may admit null the expression
            // filling it never writes, the way every other column of the plan
            // may. It may not admit less.
            let expected = value_type(column);
            let actual = expression_type(&item.expr);
            if actual.data_type != expected.data_type || (actual.nullable && !expected.nullable) {
                return Err(ContractLoweringError::ExpressionTypeMismatch {
                    context: format!("Project output {ordinal}"),
                    expected,
                    actual,
                });
            }

            let expression = self.lower_expression(node, &item.expr, &child.columns)?;
            let value = match identity_column_ref(&item.expr) {
                Some(column_id) => child
                    .columns
                    .get(&column_id)
                    .copied()
                    .ok_or(ContractLoweringError::UnknownColumnReference(column_id))?,
                None => {
                    let ty = published_value_type(
                        &value_type(column),
                        &self.expression_value_type(expression)?,
                    );
                    self.fragment_mut().add_value(
                        ty,
                        ValueOrigin::Expr {
                            node,
                            expr: expression,
                        },
                    )?
                }
            };
            match columns.insert(column.column_id, value) {
                Some(previous) if previous != value => {
                    return Err(ContractLoweringError::DuplicateColumnDefinition {
                        node: "Project",
                        column: column.column_id,
                    });
                }
                _ => {}
            }
            expressions.push((expression, value));
            output.push(value);
        }

        self.fragment_mut().add_project(
            node,
            child.node,
            expressions.into_boxed_slice(),
            output.clone().into_boxed_slice(),
        )?;
        let properties = self
            .fragment_mut()
            .node_output_properties(node)
            .expect("the projection was just inserted")
            .clone();
        Ok(LoweredNode {
            fragment: self.current_fragment,
            node,
            output: output.into_boxed_slice(),
            columns,
            properties,
            display_names: items
                .iter()
                .map(|item| item.output_name.clone())
                .collect::<Vec<_>>()
                .into_boxed_slice(),
        })
    }

    fn lower_unpivot(
        &mut self,
        plan: &PhysicalPlanNode,
        unpivot: &crate::planner::payload::PlanUnpivotNode,
    ) -> Result<LoweredNode, ContractLoweringError> {
        expect_children(plan, 1)?;
        require_output_shape("Unpivot", &plan.output_columns, &unpivot.output_columns)?;
        unpivot
            .validate_against(&plan.children[0].output_columns)
            .map_err(|detail| ContractLoweringError::InvalidUnpivot { detail })?;

        let child = self.lower_node(&plan.children[0])?;
        let node = self.fragment_mut().reserve_node_id()?;
        let mut output = Vec::with_capacity(plan.output_columns.len());
        let mut columns = BTreeMap::new();
        for (ordinal, column) in plan.output_columns.iter().enumerate() {
            let value = self.fragment_mut().add_value(
                value_type(column),
                ValueOrigin::NodeOutput {
                    node,
                    output_ordinal: checked_ordinal("Unpivot output", ordinal)?,
                },
            )?;
            insert_output_column("Unpivot", ordinal, column, value, &mut columns)?;
            output.push(value);
        }

        let passthrough = unpivot
            .passthrough_columns
            .iter()
            .map(|mapping| {
                Ok((
                    child.columns.get(&mapping.input_column_id).copied().ok_or(
                        ContractLoweringError::UnknownColumnReference(mapping.input_column_id),
                    )?,
                    columns.get(&mapping.output_column_id).copied().ok_or(
                        ContractLoweringError::UnknownColumnReference(mapping.output_column_id),
                    )?,
                ))
            })
            .collect::<Result<Vec<_>, ContractLoweringError>>()?;
        let value_output = columns
            .get(&unpivot.value_output_column_id)
            .copied()
            .ok_or(ContractLoweringError::UnknownColumnReference(
                unpivot.value_output_column_id,
            ))?;
        let literal_outputs = unpivot
            .literal_output_column_ids
            .iter()
            .map(|column| {
                columns
                    .get(column)
                    .copied()
                    .ok_or(ContractLoweringError::UnknownColumnReference(*column))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let mut mappings = Vec::with_capacity(unpivot.value_mappings.len());
        for mapping in &unpivot.value_mappings {
            let input = child
                .columns
                .get(&mapping.input_value_column_id)
                .copied()
                .ok_or(ContractLoweringError::UnknownColumnReference(
                    mapping.input_value_column_id,
                ))?;
            let mut constants = Vec::with_capacity(mapping.constants.len());
            for constant in &mapping.constants {
                constants.push(match constant {
                    crate::analysis::UnpivotConstant::Scalar(expression) => {
                        ContractUnpivotConstant::Scalar(self.lower_expression(
                            node,
                            expression,
                            &BTreeMap::new(),
                        )?)
                    }
                    crate::analysis::UnpivotConstant::Int32List(values) => {
                        ContractUnpivotConstant::Int32List(values.clone().into_boxed_slice())
                    }
                    crate::analysis::UnpivotConstant::Utf8Map(entries) => {
                        ContractUnpivotConstant::Utf8Map(
                            entries
                                .iter()
                                .map(|(key, value)| {
                                    (key.clone().into_boxed_str(), value.clone().into_boxed_str())
                                })
                                .collect::<Vec<_>>()
                                .into_boxed_slice(),
                        )
                    }
                });
            }
            mappings.push(UnpivotValueMapping {
                input,
                constants: constants.into_boxed_slice(),
            });
        }
        let max_output_rows = u64::try_from(unpivot.max_output_rows).map_err(|_| {
            ContractLoweringError::InvalidUnpivot {
                detail: "max_output_rows exceeds u64".to_string(),
            }
        })?;
        let max_output_bytes = u64::try_from(unpivot.max_output_bytes).map_err(|_| {
            ContractLoweringError::InvalidUnpivot {
                detail: "max_output_bytes exceeds u64".to_string(),
            }
        })?;
        let passthrough_map = passthrough.iter().copied().collect::<BTreeMap<_, _>>();
        self.fragment_mut().add_row_rewriting(
            node,
            child.node,
            Some(&passthrough_map),
            output.clone().into_boxed_slice(),
            NodeKind::Unpivot {
                spec: UnpivotSpec {
                    passthrough: passthrough.into_boxed_slice(),
                    value_output,
                    literal_outputs: literal_outputs.into_boxed_slice(),
                    mappings: mappings.into_boxed_slice(),
                    max_output_rows,
                    max_output_bytes,
                },
            },
        )?;
        let properties = self
            .fragment_mut()
            .node_output_properties(node)
            .expect("the node was just inserted")
            .clone();
        Ok(LoweredNode {
            fragment: self.current_fragment,
            node,
            output: output.into_boxed_slice(),
            columns,
            properties,
            display_names: plan
                .output_columns
                .iter()
                .map(|column| column.name.clone())
                .collect::<Vec<_>>()
                .into_boxed_slice(),
        })
    }

    fn lower_limit(
        &mut self,
        plan: &PhysicalPlanNode,
        limit: &crate::planner::payload::PlanLimitNode,
    ) -> Result<LoweredNode, ContractLoweringError> {
        expect_children(plan, 1)?;
        require_passthrough_shape("Limit", &plan.output_columns, &plan.children[0])?;
        let child = self.lower_node(&plan.children[0])?;
        let child = self.ensure_singleton(child, &plan.output_columns)?;
        let offset = limit
            .offset
            .map(|value| non_negative_row_count("Limit offset", value))
            .transpose()?
            .unwrap_or(0);
        let limit = limit
            .limit
            .map(|value| non_negative_row_count("Limit limit", value))
            .transpose()?;
        if limit.is_none() && offset == 0 {
            return Err(ContractLoweringError::InvalidRowCount {
                context: "Limit",
                value: 0,
                detail: "node has neither a limit nor an offset",
            });
        }
        self.append_limit(child, limit, offset)
    }

    fn append_limit(
        &mut self,
        child: LoweredNode,
        limit: Option<u64>,
        offset: u64,
    ) -> Result<LoweredNode, ContractLoweringError> {
        let node = self.fragment_mut().reserve_node_id()?;
        self.fragment_mut()
            .add_limit(node, child.node, limit, offset)?;
        let properties = self
            .fragment_mut()
            .node_output_properties(node)
            .expect("the limit was just inserted")
            .clone();
        Ok(LoweredNode {
            fragment: self.current_fragment,
            node,
            output: child.output,
            columns: child.columns,
            properties,
            display_names: child.display_names,
        })
    }

    fn lower_sort(
        &mut self,
        plan: &PhysicalPlanNode,
        sort: &crate::planner::payload::PlanSortNode,
    ) -> Result<LoweredNode, ContractLoweringError> {
        expect_children(plan, 1)?;
        // A sort reorders rows; it does not choose columns. Its payload lists
        // the columns the statement reads above it, which can be fewer than
        // the node carries -- a helper expression materialized below it for
        // the sort's own keys is not published upward -- so the payload has
        // to name a subsequence of what arrives, and what leaves is what
        // arrived.
        require_published_subsequence("Sort", &plan.output_columns, &sort.output_columns)?;
        require_passthrough_shape("Sort", &plan.output_columns, &plan.children[0])?;
        let partitioned = !sort.analytic_partition_by.is_empty();
        if sort.items.is_empty() && !partitioned {
            return Err(ContractLoweringError::EmptyOrdering { node: "Sort" });
        }
        if !partitioned && (sort.partition_limit.is_some() || sort.topn_type.is_some()) {
            return Err(ContractLoweringError::UnsupportedSortMode {
                detail: "a per-partition limit belongs to a sort that partitions",
            });
        }
        let offset = sort
            .offset
            .map(|value| non_negative_row_count("Sort offset", value))
            .transpose()?
            .unwrap_or(0);
        let child = self.lower_node(&plan.children[0])?;
        // A sort that partitions runs where the rows already are -- the
        // planner shuffled them by the partition keys -- while a global sort
        // needs the one stream it orders.
        let child = if partitioned {
            child
        } else {
            self.ensure_singleton(child, &plan.output_columns)?
        };
        // A sort placed before a window sorts by its partition keys and then
        // by the window's own order, and states the partition keys again
        // beside them. The plan says the two parts once each, so the leading
        // items are checked against the partition keys and then dropped rather
        // than repeated -- a sort whose items do not begin with them is not
        // the shape this node is documented to be.
        let within_partition =
            &sort.items[sort.analytic_partition_by.len().min(sort.items.len())..];
        let node = self.fragment_mut().reserve_node_id()?;
        if partitioned {
            let leading = &sort.items[..sort.analytic_partition_by.len().min(sort.items.len())];
            if leading.len() != sort.analytic_partition_by.len()
                || leading
                    .iter()
                    .zip(&sort.analytic_partition_by)
                    .any(|(item, key)| {
                        !item.asc
                            || !item.nulls_first
                            || identity_column_ref(&item.expr).is_none()
                            || identity_column_ref(&item.expr) != identity_column_ref(key)
                    })
            {
                return Err(ContractLoweringError::UnsupportedSortMode {
                    detail: "an analytic sort's leading keys are not its partition keys",
                });
            }
        }
        let LoweredOrdering {
            expressions: order_by,
            ..
        } = self.lower_ordering(node, within_partition, &child.columns)?;
        // A partition key states no direction in the statement and none on the
        // wire; the executor groups partitions ascending with nulls first, so
        // that is what the plan says the rows come out in.
        let partition_by = sort
            .analytic_partition_by
            .iter()
            .map(|expression| {
                Ok(SortExpr {
                    expr: self.lower_expression(node, expression, &child.columns)?,
                    direction: SortDirection::Ascending,
                    null_ordering: NullOrdering::First,
                })
            })
            .collect::<Result<Vec<_>, ContractLoweringError>>()?
            .into_boxed_slice();
        let mode = match (partitioned, sort.partition_limit) {
            (false, _) => SortMode::Global,
            (true, None) => SortMode::Analytic { partition_by },
            (true, Some(limit)) => SortMode::PartitionTopN {
                partition_by,
                limit: u64::try_from(limit)
                    .map_err(|_| ContractLoweringError::RowCountOverflow { node: "Sort" })?,
                kind: match sort.topn_type {
                    None | Some(crate::common::SqlTopNType::RowNumber) => {
                        PartitionTopNType::RowNumber
                    }
                    Some(crate::common::SqlTopNType::Rank) => PartitionTopNType::Rank,
                    Some(crate::common::SqlTopNType::DenseRank) => PartitionTopNType::DenseRank,
                },
            },
        };
        self.fragment_mut()
            .add_sort(node, child.node, order_by, mode)?;
        let properties = self
            .fragment_mut()
            .node_output_properties(node)
            .expect("the sort was just inserted")
            .clone();
        let sorted = LoweredNode {
            fragment: self.current_fragment,
            node,
            output: child.output,
            columns: child.columns,
            properties,
            display_names: child.display_names,
        };
        if offset == 0 {
            Ok(sorted)
        } else {
            self.append_limit(sorted, None, offset)
        }
    }

    fn lower_topn(
        &mut self,
        plan: &PhysicalPlanNode,
        topn: &crate::planner::physical::PhysicalTopNNode,
    ) -> Result<LoweredNode, ContractLoweringError> {
        expect_children(plan, 1)?;
        require_passthrough_shape("TopN", &plan.output_columns, &plan.children[0])?;
        if topn.items.is_empty() {
            return Err(ContractLoweringError::EmptyOrdering { node: "TopN" });
        }
        if topn.phase == SqlTopNPhase::Partial {
            // One half of a split the planner performed itself. Its final half
            // is already lowering above it and left the sequence that pairs
            // them here.
            let sequence =
                self.pending_topn_sequence
                    .ok_or(ContractLoweringError::UnsupportedTopNShape {
                        detail: "partial TopN lacks a final-contract TopN sequence identity",
                    })?;
            let limit = topn
                .limit
                .ok_or(ContractLoweringError::InvalidRowCount {
                    context: "TopN limit",
                    value: -1,
                    detail: "TopN has no finite limit",
                })
                .and_then(|value| non_negative_row_count("TopN limit", value))?;
            if topn.offset.unwrap_or(0) != 0 {
                return Err(ContractLoweringError::UnsupportedTopNShape {
                    detail: "partial TopN carries an offset",
                });
            }
            self.pending_topn_sequence_used = true;
            let child = self.lower_node(&plan.children[0])?;
            return self.append_topn(
                child,
                &topn.items,
                limit,
                0,
                ContractTopNPhase::Partial { sequence },
            );
        }
        let limit = topn
            .limit
            .ok_or(ContractLoweringError::InvalidRowCount {
                context: "TopN limit",
                value: -1,
                detail: "TopN has no finite limit",
            })
            .and_then(|value| non_negative_row_count("TopN limit", value))?;
        let offset = topn
            .offset
            .map(|value| non_negative_row_count("TopN offset", value))
            .transpose()?
            .unwrap_or(0);
        limit
            .checked_add(offset)
            .ok_or(ContractLoweringError::RowCountOverflow { node: "TopN" })?;

        // A final TopN pairs with the partial one the planner placed below it,
        // wherever that is: directly under it when the split was of this TopN
        // itself, and further down when the partial was pushed past an
        // aggregate to prune its groups. The sequence offered here is what
        // pairs them; a TopN nothing takes it from is the only one there is.
        let adjacent_partial = matches!(
            &plan.children[0].kind,
            PhysicalPlanKind::TopN(child) if child.phase == SqlTopNPhase::Partial
        );
        let sequence = self.allocate_topn_sequence()?;
        let destination = self.current_fragment;
        if adjacent_partial {
            // The partial prunes in its own fragment and this final finishes
            // what that fragment gathers.
            self.current_fragment = self.allocate_fragment()?;
        }
        let previous_sequence = self.pending_topn_sequence.replace(sequence);
        let previous_used = std::mem::replace(&mut self.pending_topn_sequence_used, false);
        let child_result = self.lower_node(&plan.children[0]);
        let used = self.pending_topn_sequence_used;
        self.pending_topn_sequence = previous_sequence;
        self.pending_topn_sequence_used = previous_used;
        self.current_fragment = destination;
        let child = child_result?;
        if !used {
            let child = self.ensure_singleton(child, &plan.output_columns)?;
            return self.append_topn(child, &topn.items, limit, offset, ContractTopNPhase::Single);
        }
        let child = if adjacent_partial {
            self.append_exchange(child, &plan.output_columns, ExchangeLayout::Gather)?
        } else {
            child
        };
        self.append_topn(
            child,
            &topn.items,
            limit,
            offset,
            ContractTopNPhase::Final { sequence },
        )
    }

    fn append_topn(
        &mut self,
        child: LoweredNode,
        items: &[crate::analysis::SortItem],
        limit: u64,
        offset: u64,
        phase: ContractTopNPhase,
    ) -> Result<LoweredNode, ContractLoweringError> {
        let node = self.fragment_mut().reserve_node_id()?;
        let LoweredOrdering {
            expressions: order_by,
            ..
        } = self.lower_ordering(node, items, &child.columns)?;
        self.fragment_mut()
            .add_top_n(node, child.node, order_by, limit, offset, phase)?;
        let properties = self
            .fragment_mut()
            .node_output_properties(node)
            .expect("the top-n was just inserted")
            .clone();
        Ok(LoweredNode {
            fragment: self.current_fragment,
            node,
            output: child.output,
            columns: child.columns,
            properties,
            display_names: child.display_names,
        })
    }

    fn lower_assertion(
        &mut self,
        plan: &PhysicalPlanNode,
        assertion: &crate::planner::payload::PlanAssertOneRowNode,
    ) -> Result<LoweredNode, ContractLoweringError> {
        expect_children(plan, 1)?;
        require_output_shape(
            "AssertOneRow",
            &plan.output_columns,
            &plan.children[0].output_columns,
        )?;
        let child = self.lower_node(&plan.children[0])?;
        require_single_copy_input("AssertOneRow", &child.properties)?;
        let spec = if assertion.group_key_column_ids.is_empty() {
            require_singleton_input("AssertOneRow", &child.properties)?;
            if !assertion.group_key_labels.is_empty() || assertion.keyed_message_prefix.is_some() {
                return Err(ContractLoweringError::InvalidAssertion {
                    detail: "global assertion carries keyed labels or message",
                });
            }
            let desired = assertion
                .desired_num_rows
                .ok_or(ContractLoweringError::InvalidAssertion {
                    detail: "global assertion has no desired row count",
                })
                .and_then(|value| non_negative_row_count("assertion desired rows", value))?;
            if assertion.subquery_text.is_empty() {
                return Err(ContractLoweringError::InvalidAssertion {
                    detail: "global assertion subject is empty",
                });
            }
            RowCountAssertionSpec::Global {
                subject: assertion.subquery_text.clone().into_boxed_str(),
                desired_rows: desired,
                comparison: lower_row_count_assertion(assertion.assertion),
            }
        } else {
            if assertion.assertion != crate::planner::payload::PlanRowCountAssertion::Le
                || assertion.desired_num_rows != Some(1)
            {
                return Err(ContractLoweringError::InvalidAssertion {
                    detail: "keyed assertion is not exactly per-key at-most-one",
                });
            }
            if assertion.group_key_labels.len() != assertion.group_key_column_ids.len() {
                return Err(ContractLoweringError::InvalidAssertion {
                    detail: "keyed assertion key and label arities differ",
                });
            }
            if assertion.group_key_labels.iter().any(String::is_empty) {
                return Err(ContractLoweringError::InvalidAssertion {
                    detail: "keyed assertion contains an empty key label",
                });
            }
            let message = assertion
                .keyed_message_prefix
                .as_deref()
                .filter(|message| !message.is_empty())
                .ok_or(ContractLoweringError::InvalidAssertion {
                    detail: "keyed assertion message is empty",
                })?;
            let keys = assertion
                .group_key_column_ids
                .iter()
                .map(|column| {
                    child
                        .columns
                        .get(column)
                        .copied()
                        .ok_or(ContractLoweringError::UnknownColumnReference(*column))
                })
                .collect::<Result<Vec<_>, _>>()?
                .into_boxed_slice();
            RowCountAssertionSpec::PerKeyAtMostOne {
                keys,
                labels: assertion
                    .group_key_labels
                    .iter()
                    .map(|label| label.clone().into_boxed_str())
                    .collect::<Vec<_>>()
                    .into_boxed_slice(),
                message: message.into(),
            }
        };
        let node = self.fragment_mut().reserve_node_id()?;
        self.fragment_mut()
            .add_assert_one_row(node, child.node, spec)?;
        Ok(LoweredNode {
            fragment: self.current_fragment,
            node,
            output: child.output,
            columns: child.columns,
            properties: child.properties,
            display_names: child.display_names,
        })
    }

    /// Give independently nullifiable grouping columns their own value identity
    /// when an identity projection shares their input with another column.
    /// Repeat replaces values, so sharing one value across distinct grouping
    /// domains would also null an original aggregate input or another key.
    fn materialize_repeat_input_identities(
        &mut self,
        child: LoweredNode,
        input_columns: &[OutputColumn],
        nullable_columns: &BTreeSet<ColumnId>,
    ) -> Result<LoweredNode, ContractLoweringError> {
        if input_columns.len() != child.output.len() {
            return Err(ContractLoweringError::ArityMismatch {
                context: "Repeat input",
                expected: input_columns.len(),
                actual: child.output.len(),
            });
        }
        let mut value_columns = BTreeMap::<ValueId, BTreeSet<ColumnId>>::new();
        for (value, column) in child.output.iter().zip(input_columns) {
            value_columns
                .entry(*value)
                .or_default()
                .insert(column.column_id);
        }
        let independent_columns = child
            .output
            .iter()
            .zip(input_columns)
            .filter(|(value, column)| {
                nullable_columns.contains(&column.column_id) && value_columns[*value].len() > 1
            })
            .map(|(_, column)| column.column_id)
            .collect::<BTreeSet<_>>();
        if independent_columns.is_empty() {
            return Ok(child);
        }

        let node = self.fragment_mut().reserve_node_id()?;
        let mut expressions = Vec::with_capacity(child.output.len());
        let mut output = Vec::with_capacity(child.output.len());
        let mut columns = child.columns.clone();
        let mut materialized = BTreeMap::new();
        for (source, column) in child.output.iter().zip(input_columns) {
            let (expression, value) = if let Some(pair) = materialized.get(&column.column_id) {
                *pair
            } else {
                let ty = self.value_declared_type(*source)?;
                let expression = self.fragment_mut().add_expression(
                    node,
                    ty.clone(),
                    ContractExprKind::Value(*source),
                )?;
                let value = if independent_columns.contains(&column.column_id) {
                    self.fragment_mut().add_value(
                        ty,
                        ValueOrigin::Expr {
                            node,
                            expr: expression,
                        },
                    )?
                } else {
                    *source
                };
                let pair = (expression, value);
                materialized.insert(column.column_id, pair);
                pair
            };
            columns.insert(column.column_id, value);
            expressions.push((expression, value));
            output.push(value);
        }
        self.fragment_mut().add_project(
            node,
            child.node,
            expressions.into_boxed_slice(),
            output.clone().into_boxed_slice(),
        )?;
        let properties = self
            .fragment_mut()
            .node_output_properties(node)
            .expect("the projection was just inserted")
            .clone();
        Ok(LoweredNode {
            fragment: child.fragment,
            node,
            output: output.into_boxed_slice(),
            columns,
            properties,
            display_names: child.display_names,
        })
    }

    fn lower_repeat(
        &mut self,
        plan: &PhysicalPlanNode,
        repeat: &crate::planner::payload::PlanRepeatNode,
    ) -> Result<LoweredNode, ContractLoweringError> {
        expect_children(plan, 1)?;
        if repeat.repeat_column_ref_ids.is_empty()
            || repeat.repeat_column_ref_ids.len() != repeat.grouping_ids.len()
        {
            return Err(ContractLoweringError::InvalidRepeat {
                detail: "grouping sets and grouping ids must be non-empty and have equal width"
                    .to_string(),
            });
        }
        if repeat.all_rollup_column_ids.len() != repeat.all_rollup_columns.len()
            || repeat.grouping_fn_args.len() != repeat.grouping_fn_arg_ids.len()
            || repeat.grouping_fn_args.len() != repeat.grouping_fn_ids.len()
        {
            return Err(ContractLoweringError::InvalidRepeat {
                detail: "display and identity metadata have different arity".to_string(),
            });
        }
        if repeat.all_rollup_column_ids.len() > u64::BITS as usize {
            return Err(ContractLoweringError::InvalidRepeat {
                detail: "grouping key width exceeds the u64 grouping-id domain".to_string(),
            });
        }

        let nullable_columns = repeat
            .all_rollup_column_ids
            .iter()
            .copied()
            .filter(|column| {
                repeat
                    .repeat_column_ref_ids
                    .iter()
                    .any(|set| !set.contains(column))
            })
            .collect::<BTreeSet<_>>();
        let child = self.lower_node(&plan.children[0])?;
        let child = self.materialize_repeat_input_identities(
            child,
            &plan.children[0].output_columns,
            &nullable_columns,
        )?;
        let mut grouping_sets = Vec::with_capacity(repeat.repeat_column_ref_ids.len());
        for (set_ordinal, (set, grouping_id)) in repeat
            .repeat_column_ref_ids
            .iter()
            .zip(&repeat.grouping_ids)
            .enumerate()
        {
            let mut seen = std::collections::BTreeSet::new();
            let mut values = Vec::with_capacity(set.len());
            for column in set {
                if !repeat.all_rollup_column_ids.contains(column) || !seen.insert(*column) {
                    return Err(ContractLoweringError::InvalidRepeat {
                        detail: format!(
                            "grouping set {set_ordinal} is not an exact subset of the rollup keys"
                        ),
                    });
                }
                values.push(
                    child
                        .columns
                        .get(column)
                        .copied()
                        .ok_or(ContractLoweringError::UnknownColumnReference(*column))?,
                );
            }
            let mut expected_id = 0_u64;
            for (index, column) in repeat.all_rollup_column_ids.iter().enumerate() {
                if !seen.contains(column) {
                    expected_id |= 1_u64 << (repeat.all_rollup_column_ids.len() - 1 - index);
                }
            }
            if *grouping_id != expected_id {
                return Err(ContractLoweringError::InvalidRepeat {
                    detail: format!(
                        "grouping set {set_ordinal} bitmap {grouping_id} differs from exact bitmap {expected_id}"
                    ),
                });
            }
            grouping_sets.push(values.into_boxed_slice());
        }

        let expected_output_len = child
            .output
            .len()
            .checked_add(repeat.grouping_fn_ids.len())
            .ok_or_else(|| ContractLoweringError::InvalidRepeat {
                detail: "output arity overflowed".to_string(),
            })?;
        if plan.output_columns.len() != expected_output_len {
            return Err(ContractLoweringError::ArityMismatch {
                context: "Repeat output",
                expected: expected_output_len,
                actual: plan.output_columns.len(),
            });
        }
        let node = self.fragment_mut().reserve_node_id()?;
        let mut grouping_values = Vec::with_capacity(nullable_columns.len());
        let mut grouping_replacements = BTreeMap::new();
        let mut output = Vec::with_capacity(expected_output_len);
        let mut columns = child.columns.clone();
        for (ordinal, (input, output_column)) in
            child.output.iter().zip(&plan.output_columns).enumerate()
        {
            let child_column = &plan.children[0].output_columns[ordinal];
            if child_column.column_id != output_column.column_id
                || child_column.data_type != output_column.data_type
            {
                return Err(ContractLoweringError::OutputColumnMismatch {
                    node: "Repeat",
                    ordinal,
                    detail: "child occurrence identity or type changed".to_string(),
                });
            }
            let value = if nullable_columns.contains(&output_column.column_id) {
                if !output_column.nullable {
                    return Err(ContractLoweringError::OutputColumnMismatch {
                        node: "Repeat",
                        ordinal,
                        detail: "nullable grouping output is declared non-null".to_string(),
                    });
                }
                let value = if let Some(value) = grouping_replacements.get(input) {
                    *value
                } else {
                    let value = self.fragment_mut().add_value(
                        value_type(output_column),
                        ValueOrigin::NullExtended { node, of: *input },
                    )?;
                    grouping_replacements.insert(*input, value);
                    grouping_values.push((*input, value));
                    value
                };
                columns.insert(output_column.column_id, value);
                value
            } else {
                if child_column.nullable != output_column.nullable {
                    return Err(ContractLoweringError::OutputColumnMismatch {
                        node: "Repeat",
                        ordinal,
                        detail: "non-grouping passthrough nullability changed".to_string(),
                    });
                }
                *input
            };
            output.push(value);
        }

        let mut grouping_outputs = Vec::with_capacity(repeat.grouping_fn_ids.len());
        for (index, (((function_name, display_args), argument_ids), (output_name, output_id))) in
            repeat
                .grouping_fn_args
                .iter()
                .zip(&repeat.grouping_fn_arg_ids)
                .zip(&repeat.grouping_fn_ids)
                .enumerate()
        {
            if function_name != output_name || display_args.len() != argument_ids.len() {
                return Err(ContractLoweringError::InvalidRepeat {
                    detail: format!("GROUPING output {index} metadata is inconsistent"),
                });
            }
            let ordinal = child.output.len() + index;
            let output_column = &plan.output_columns[ordinal];
            if output_column.column_id != *output_id
                || output_column.data_type != DataType::Int64
                || output_column.nullable
            {
                return Err(ContractLoweringError::OutputColumnMismatch {
                    node: "Repeat",
                    ordinal,
                    detail: "GROUPING output must be its exact non-null Int64 column".to_string(),
                });
            }
            let value = self.fragment_mut().add_value(
                value_type(output_column),
                ValueOrigin::NodeOutput {
                    node,
                    output_ordinal: checked_ordinal("Repeat output", ordinal)?,
                },
            )?;
            insert_output_column("Repeat", ordinal, output_column, value, &mut columns)?;
            let arguments = argument_ids
                .iter()
                .map(|column| {
                    child
                        .columns
                        .get(column)
                        .copied()
                        .ok_or(ContractLoweringError::UnknownColumnReference(*column))
                })
                .collect::<Result<Vec<_>, _>>()?;
            grouping_outputs.push(GroupingOutput {
                output: value,
                arguments: arguments.into_boxed_slice(),
            });
            output.push(value);
        }

        let _passthrough_map = child
            .output
            .iter()
            .copied()
            .zip(output.iter().copied())
            .filter(|(input, output)| input == output)
            .collect::<BTreeMap<_, _>>();
        // The keys are the domain every set is read against, in the order the
        // planner writes their presence into a grouping id.
        let rollup_keys = repeat
            .all_rollup_column_ids
            .iter()
            .map(|column| {
                child
                    .columns
                    .get(column)
                    .copied()
                    .ok_or(ContractLoweringError::UnknownColumnReference(*column))
            })
            .collect::<Result<Vec<_>, _>>()?;
        self.fragment_mut().add_repeat(
            node,
            child.node,
            rollup_keys.into_boxed_slice(),
            grouping_sets.into_boxed_slice(),
            grouping_values.into_boxed_slice(),
            grouping_outputs.into_boxed_slice(),
            output.clone().into_boxed_slice(),
        )?;
        let properties = self
            .fragment_mut()
            .node_output_properties(node)
            .expect("the repeat was just inserted")
            .clone();
        Ok(LoweredNode {
            fragment: self.current_fragment,
            node,
            output: output.into_boxed_slice(),
            columns,
            properties,
            display_names: plan
                .output_columns
                .iter()
                .map(|column| column.name.clone())
                .collect::<Vec<_>>()
                .into_boxed_slice(),
        })
    }

    fn lower_table_function(
        &mut self,
        plan: &PhysicalPlanNode,
        table_function: &crate::planner::payload::PlanTableFunctionNode,
    ) -> Result<LoweredNode, ContractLoweringError> {
        expect_children(plan, 1)?;
        let child = self.lower_node(&plan.children[0])?;
        let binding = &table_function.binding;
        if binding.kind != novarocks_physical_plan::FunctionKind::Table
            || binding.logical_argument_count != table_function.args.len()
            || binding.selected.argument_types.len() != table_function.args.len()
        {
            return Err(ContractLoweringError::InvalidTableFunction {
                detail: "exact table-function kind or argument arity is inconsistent".to_string(),
            });
        }
        let novarocks_functions::FunctionResultType::Relation(result_types) =
            &binding.selected.result_type
        else {
            return Err(ContractLoweringError::InvalidTableFunction {
                detail: "binding does not carry a relation result schema".to_string(),
            });
        };
        if result_types.len() != table_function.output_columns.len() {
            return Err(ContractLoweringError::ArityMismatch {
                context: "TableFunction relation result",
                expected: result_types.len(),
                actual: table_function.output_columns.len(),
            });
        }
        let expected_plan_outputs = child
            .output
            .len()
            .checked_add(result_types.len())
            .ok_or_else(|| ContractLoweringError::InvalidTableFunction {
                detail: "output arity overflowed".to_string(),
            })?;
        if plan.output_columns.len() != expected_plan_outputs {
            return Err(ContractLoweringError::ArityMismatch {
                context: "TableFunction output",
                expected: expected_plan_outputs,
                actual: plan.output_columns.len(),
            });
        }
        require_output_shape(
            "TableFunction",
            &plan.output_columns[..child.output.len()],
            &plan.children[0].output_columns,
        )?;

        let node = self.fragment_mut().reserve_node_id()?;
        let arguments = table_function
            .args
            .iter()
            .map(|argument| self.lower_expression(node, argument, &child.columns))
            .collect::<Result<Vec<_>, _>>()?;
        let mut output = child.output.to_vec();
        let mut outputs = child
            .output
            .iter()
            .copied()
            .map(TableFunctionOutput::PassThrough)
            .collect::<Vec<_>>();
        let mut columns = child.columns.clone();
        for (result_ordinal, ((column, plan_column), result_type)) in table_function
            .output_columns
            .iter()
            .zip(&plan.output_columns[child.output.len()..])
            .zip(result_types.iter())
            .enumerate()
        {
            if column.column_id != plan_column.column_id
                || column.data_type != plan_column.data_type
                || column.nullable != plan_column.nullable
            {
                return Err(ContractLoweringError::OutputColumnMismatch {
                    node: "TableFunction",
                    ordinal: child.output.len() + result_ordinal,
                    detail: "payload and physical output schema differ".to_string(),
                });
            }
            // The binding's relation schema is derived from the arguments it
            // was resolved against, so a nested column carries the provider's
            // decoration into it while the plan reads every type in its own
            // vocabulary.
            let mut expected = undecorated(result_type);
            expected.nullable |= table_function.is_left_join;
            if value_type(plan_column) != expected {
                return Err(ContractLoweringError::OutputColumnMismatch {
                    node: "TableFunction",
                    ordinal: child.output.len() + result_ordinal,
                    detail: "output type differs from the bound relation schema".to_string(),
                });
            }
            let output_ordinal = child.output.len() + result_ordinal;
            let value = self.fragment_mut().add_value(
                expected,
                ValueOrigin::NodeOutput {
                    node,
                    output_ordinal: checked_ordinal("TableFunction output", output_ordinal)?,
                },
            )?;
            insert_output_column(
                "TableFunction",
                output_ordinal,
                plan_column,
                value,
                &mut columns,
            )?;
            outputs.push(TableFunctionOutput::FunctionResult {
                result_ordinal: checked_ordinal("TableFunction result", result_ordinal)?,
                value,
            });
            output.push(value);
        }
        let distribution = match &child.properties.distribution {
            Distribution::Broadcast
                if binding.semantics.volatility
                    != novarocks_physical_plan::FunctionVolatility::Immutable
                    || table_function
                        .args
                        .iter()
                        .any(|argument| !expression_is_replica_deterministic(argument)) =>
            {
                Distribution::Unconstrained
            }
            distribution => distribution.clone(),
        };
        self.fragment_mut().add_row_expanding(
            node,
            child.node,
            distribution,
            output.clone().into_boxed_slice(),
            NodeKind::TableFunction {
                function: BoundTableFunction {
                    function_id: binding.function_id.clone(),
                    overload: binding.selected.overload.clone(),
                    argument_types: binding
                        .selected
                        .argument_types
                        .iter()
                        .map(undecorated_argument)
                        .collect(),
                    // The schema the plan carries is the one its own values
                    // are read in; the binding's came from the arguments it
                    // was resolved against, decoration and all.
                    result_types: result_types.iter().map(undecorated).collect(),
                    volatility: binding.semantics.volatility,
                    argument_evaluation: binding.semantics.argument_evaluation,
                    failure_behavior: binding.semantics.failure_behavior,
                },
                arguments: arguments.into_boxed_slice(),
                outputs: outputs.into_boxed_slice(),
                left_outer: table_function.is_left_join,
            },
        )?;
        let properties = self
            .fragment_mut()
            .node_output_properties(node)
            .expect("the table function was just inserted")
            .clone();
        Ok(LoweredNode {
            fragment: self.current_fragment,
            node,
            output: output.into_boxed_slice(),
            columns,
            properties,
            display_names: plan
                .output_columns
                .iter()
                .map(|column| column.name.clone())
                .collect::<Vec<_>>()
                .into_boxed_slice(),
        })
    }

    fn lower_window(
        &mut self,
        plan: &PhysicalPlanNode,
        window: &crate::planner::payload::PlanWindowNode,
    ) -> Result<LoweredNode, ContractLoweringError> {
        expect_children(plan, 1)?;
        if window.window_exprs.is_empty() {
            return Err(ContractLoweringError::InvalidWindow {
                detail: "window expression list is empty".to_string(),
            });
        }
        require_output_shape("Window", &plan.output_columns, &window.output_columns)?;
        // A window node produces what reached it plus what its functions
        // answered, and publishes some of that -- a statement that filters on
        // a rank and then selects two columns publishes neither everything it
        // read nor everything it ranked. Each function's own output column is
        // found among the published ones, which is where its identity, type
        // and nullability are stated.
        let window_columns = window
            .window_exprs
            .iter()
            .map(|expression| {
                plan.output_columns
                    .iter()
                    .find(|column| column.column_id == expression.output_column_id)
                    .cloned()
                    .ok_or(ContractLoweringError::UnknownColumnReference(
                        expression.output_column_id,
                    ))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let mut current = self.lower_node(&plan.children[0])?;

        let mut current_output_columns = plan.children[0].output_columns.clone();
        let mut start = 0;
        while start < window.window_exprs.len() {
            let first = &window.window_exprs[start];
            let mut end = start + 1;
            while end < window.window_exprs.len()
                && same_window_signature(&window.window_exprs[end], first)
            {
                end += 1;
            }
            current = self.lower_window_group(
                current,
                &current_output_columns,
                &window.window_exprs[start..end],
                &window_columns[start..end],
                start == 0,
            )?;
            current_output_columns.extend_from_slice(&window_columns[start..end]);
            start = end;
        }
        // What stands above reads the columns the statement published, in the
        // order it published them.
        let visible = plan
            .output_columns
            .iter()
            .map(|column| {
                current.columns.get(&column.column_id).copied().ok_or(
                    ContractLoweringError::UnknownColumnReference(column.column_id),
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        current.output = visible.into_boxed_slice();
        current.display_names = plan
            .output_columns
            .iter()
            .map(|column| column.name.clone())
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Ok(current)
    }

    fn lower_window_group(
        &mut self,
        mut child: LoweredNode,
        child_output_columns: &[OutputColumn],
        expressions: &[crate::planner::payload::WindowExpr],
        output_columns: &[OutputColumn],
        first_group: bool,
    ) -> Result<LoweredNode, ContractLoweringError> {
        let first = expressions
            .first()
            .ok_or_else(|| ContractLoweringError::InvalidWindow {
                detail: "window group is empty".to_string(),
            })?;
        if first.partition_by.is_empty() {
            child = self.ensure_singleton(child, child_output_columns)?;
        } else if !distribution_colocates_columns(
            &child.properties.distribution,
            &first.partition_by,
            &child.columns,
        )? {
            return Err(ContractLoweringError::PropertyRequirementUnsatisfied {
                node: "Window",
                detail: "input distribution does not colocate the exact partition keys",
            });
        }

        let needs_sort = match window_ordering_keys(first, &child.columns)? {
            Some(expected_ordering) => {
                if expected_ordering.is_empty() {
                    false
                } else {
                    !ordering_has_prefix(&child.properties.ordering, &expected_ordering)
                }
            }
            // The logical builder places the first group's expression sort
            // below the Window. Its ordering cannot be named as a ValueId,
            // so a later group must sort again for its own distinct keys.
            None => !first_group,
        };
        if needs_sort {
            child = self.append_analytic_sort(child, first)?;
        }

        let node = self.fragment_mut().reserve_node_id()?;
        let partition_by =
            lower_window_partition_expressions(self, node, &first.partition_by, &child.columns)?;
        let order_by = self
            .lower_ordering(node, &first.order_by, &child.columns)?
            .expressions;
        let mut output = child.output.to_vec();
        let mut columns = child.columns.clone();
        let mut window_expressions = Vec::with_capacity(expressions.len());
        for (index, (expression, output_column)) in
            expressions.iter().zip(output_columns).enumerate()
        {
            if !same_window_signature(expression, first)
                || expression.output_column_id != output_column.column_id
                || expression.result_type != output_column.data_type
            {
                return Err(ContractLoweringError::OutputColumnMismatch {
                    node: "Window",
                    ordinal: child.output.len() + index,
                    detail: "window group signature or output identity differs".to_string(),
                });
            }
            let expression_id = self.lower_window_call(node, expression, &child.columns)?;
            let expression_type = window_result_type(&expression.binding)?;
            if expression_type != value_type(output_column) {
                return Err(ContractLoweringError::OutputColumnMismatch {
                    node: "Window",
                    ordinal: child.output.len() + index,
                    detail: "window output type differs from its exact function binding"
                        .to_string(),
                });
            }
            let value = self.fragment_mut().add_value(
                expression_type,
                ValueOrigin::Expr {
                    node,
                    expr: expression_id,
                },
            )?;
            insert_output_column(
                "Window",
                child.output.len() + index,
                output_column,
                value,
                &mut columns,
            )?;
            window_expressions.push(WindowExpression {
                expression: expression_id,
                output: value,
            });
            output.push(value);
        }
        let properties = child.properties.clone();
        self.fragment_mut().add_row_widening(
            node,
            child.node,
            output.clone().into_boxed_slice(),
            NodeKind::Window(WindowSpec {
                partition_by,
                order_by,
                expressions: window_expressions.into_boxed_slice(),
            }),
        )?;
        let mut display_names = child.display_names.to_vec();
        display_names.extend(output_columns.iter().map(|column| column.name.clone()));
        Ok(LoweredNode {
            fragment: self.current_fragment,
            node,
            output: output.into_boxed_slice(),
            columns,
            properties,
            display_names: display_names.into_boxed_slice(),
        })
    }

    fn append_analytic_sort(
        &mut self,
        child: LoweredNode,
        window: &crate::planner::payload::WindowExpr,
    ) -> Result<LoweredNode, ContractLoweringError> {
        let node = self.fragment_mut().reserve_node_id()?;
        let partition_by =
            lower_window_partition_expressions(self, node, &window.partition_by, &child.columns)?;
        let order_by = self
            .lower_ordering(node, &window.order_by, &child.columns)?
            .expressions;
        let mode = if partition_by.is_empty() {
            SortMode::Global
        } else {
            SortMode::Analytic { partition_by }
        };
        self.fragment_mut()
            .add_sort(node, child.node, order_by, mode)?;
        let properties = self
            .fragment_mut()
            .node_output_properties(node)
            .expect("the sort was just inserted")
            .clone();
        Ok(LoweredNode {
            fragment: self.current_fragment,
            node,
            output: child.output,
            columns: child.columns,
            properties,
            display_names: child.display_names,
        })
    }

    fn lower_window_call(
        &mut self,
        owner: NodeId,
        window: &crate::planner::payload::WindowExpr,
        visible: &BTreeMap<ColumnId, ValueId>,
    ) -> Result<ExprId, ContractLoweringError> {
        let binding = &window.binding;
        if binding.logical_argument_count != window.args.len()
            || binding.selected.argument_types.len()
                != window.args.len() + window.function_order_by.len()
        {
            return Err(ContractLoweringError::InvalidWindow {
                detail: "function binding arity differs from arguments and function ORDER BY"
                    .to_string(),
            });
        }
        let result_type = window_result_type(binding)?;
        // Both sides in the plan's own vocabulary: the payload's type comes
        // from the statement as written, the binding's from a provider's
        // column, and they differ over decoration the window does not
        // compute with.
        if result_type.data_type != novarocks_types::undecorated_nested_type(&window.result_type) {
            return Err(ContractLoweringError::InvalidWindow {
                detail: "window payload result type differs from its exact binding".to_string(),
            });
        }
        let function = bound_function_from_resolved(binding, &result_type);
        let aggregate_binding = match binding.kind {
            novarocks_physical_plan::FunctionKind::Window => {
                if window.aggregate_binding.is_some()
                    || window.distinct
                    || !window.function_order_by.is_empty()
                {
                    return Err(ContractLoweringError::InvalidWindow {
                        detail: "window function carries aggregate-only semantics".to_string(),
                    });
                }
                None
            }
            novarocks_physical_plan::FunctionKind::Aggregate => {
                let aggregate = window.aggregate_binding.as_ref().ok_or_else(|| {
                    ContractLoweringError::InvalidWindow {
                        detail: "aggregate window lacks its exact aggregate binding".to_string(),
                    }
                })?;
                if aggregate != binding {
                    return Err(ContractLoweringError::InvalidWindow {
                        detail: "aggregate window carries two different function bindings"
                            .to_string(),
                    });
                }
                Some(lower_resolved_aggregate_binding(
                    aggregate,
                    window.args.len(),
                    window.function_order_by.len(),
                    AggregatePhase::Single,
                )?)
            }
            other => {
                return Err(ContractLoweringError::InvalidWindow {
                    detail: format!("window call carries {other:?} function kind"),
                });
            }
        };
        let args = window
            .args
            .iter()
            .zip(function.argument_types.iter())
            .map(|(argument, expected)| {
                self.lower_bound_argument(owner, argument, visible, expected)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let function_order_by = self
            .lower_ordering(owner, &window.function_order_by, visible)?
            .expressions;
        let frame = window
            .window_frame
            .as_ref()
            .map(|frame| self.lower_window_frame(owner, frame))
            .transpose()?;
        Ok(self.fragment_mut().add_expression(
            owner,
            result_type,
            ContractExprKind::WindowCall {
                function,
                distinct: window.distinct,
                args: args.into_boxed_slice(),
                function_order_by,
                frame,
                ignore_nulls: window.ignore_nulls,
                aggregate_binding,
            },
        )?)
    }

    fn lower_window_frame(
        &mut self,
        owner: NodeId,
        frame: &crate::analysis::WindowFrame,
    ) -> Result<ContractWindowFrame, ContractLoweringError> {
        let units = match frame.frame_type {
            crate::analysis::WindowFrameType::Rows => WindowFrameUnits::Rows,
            crate::analysis::WindowFrameType::Range => WindowFrameUnits::Range,
        };
        if units == WindowFrameUnits::Range
            && (matches!(
                frame.start,
                crate::analysis::WindowBound::Preceding(_)
                    | crate::analysis::WindowBound::Following(_)
            ) || matches!(
                frame.end,
                crate::analysis::WindowBound::Preceding(_)
                    | crate::analysis::WindowBound::Following(_)
            ))
        {
            return Err(ContractLoweringError::InvalidWindow {
                detail: "RANGE offsets require typed order-key arithmetic unavailable in contract revision 1"
                    .to_string(),
            });
        }
        Ok(ContractWindowFrame {
            units,
            start: self.lower_window_bound(owner, &frame.start)?,
            end: self.lower_window_bound(owner, &frame.end)?,
            exclusion: WindowFrameExclusion::NoOthers,
        })
    }

    fn lower_window_bound(
        &mut self,
        owner: NodeId,
        bound: &crate::analysis::WindowBound,
    ) -> Result<ContractWindowBound, ContractLoweringError> {
        let offset = |visitor: &mut Self, value: i64| {
            if value < 0 {
                return Err(ContractLoweringError::InvalidWindow {
                    detail: "window frame offset is negative".to_string(),
                });
            }
            if value == 0 {
                return Ok(None);
            }
            // A frame offset is a non-negative count, already checked above.
            // It is stated as Int64 because that is what the contract's own
            // window bound reads and what the wire carries; an unsigned
            // literal would say the same number in a type neither accepts.
            let expression = visitor.fragment_mut().add_expression(
                owner,
                ValueType::new(DataType::Int64, false),
                ContractExprKind::Literal(ContractLiteralValue::Int64(value)),
            )?;
            Ok(Some(expression))
        };
        Ok(match bound {
            crate::analysis::WindowBound::UnboundedPreceding => {
                ContractWindowBound::UnboundedPreceding
            }
            crate::analysis::WindowBound::Preceding(value) => match offset(self, *value)? {
                Some(expression) => ContractWindowBound::Preceding(expression),
                None => ContractWindowBound::CurrentRow,
            },
            crate::analysis::WindowBound::CurrentRow => ContractWindowBound::CurrentRow,
            crate::analysis::WindowBound::Following(value) => match offset(self, *value)? {
                Some(expression) => ContractWindowBound::Following(expression),
                None => ContractWindowBound::CurrentRow,
            },
            crate::analysis::WindowBound::UnboundedFollowing => {
                ContractWindowBound::UnboundedFollowing
            }
        })
    }

    fn lower_generate_series(
        &mut self,
        plan: &PhysicalPlanNode,
        series: &crate::planner::payload::PlanGenerateSeriesNode,
    ) -> Result<LoweredNode, ContractLoweringError> {
        expect_children(plan, 0)?;
        if plan.output_columns.len() != 1 {
            return Err(ContractLoweringError::ArityMismatch {
                context: "GenerateSeries output",
                expected: 1,
                actual: plan.output_columns.len(),
            });
        }
        let column = &plan.output_columns[0];
        if column.column_id != series.output_column_id
            || column.name != series.column_name
            || column.data_type != DataType::Int64
            || column.nullable
        {
            return Err(ContractLoweringError::OutputColumnMismatch {
                node: "GenerateSeries",
                ordinal: 0,
                detail: "payload requires its exact non-nullable Int64 output column".to_string(),
            });
        }
        if series.step == 0 {
            return Err(ContractLoweringError::InvalidGenerateSeries {
                detail: "step is zero",
            });
        }
        let node = self.fragment_mut().reserve_node_id()?;
        let start = self.literal_i64_expression(node, series.start)?;
        let stop = self.literal_i64_expression(node, series.end)?;
        let step = self.literal_i64_expression(node, series.step)?;
        let value = self.fragment_mut().add_value(
            value_type(column),
            ValueOrigin::NodeOutput {
                node,
                output_ordinal: 0,
            },
        )?;
        let properties = singleton_properties();
        self.fragment_mut()
            .add_generate_series(node, start, stop, Some(step), value)?;
        Ok(LoweredNode {
            fragment: self.current_fragment,
            node,
            output: Box::from([value]),
            columns: BTreeMap::from([(column.column_id, value)]),
            properties,
            display_names: Box::from([column.name.clone()]),
        })
    }

    fn literal_i64_expression(
        &mut self,
        owner: NodeId,
        value: i64,
    ) -> Result<ExprId, ContractLoweringError> {
        Ok(self.fragment_mut().add_expression(
            owner,
            ValueType::new(DataType::Int64, false),
            ContractExprKind::Literal(ContractLiteralValue::Int64(value)),
        )?)
    }

    fn lower_values_expression(
        &mut self,
        owner: NodeId,
        expression: &TypedExpr,
        target: &ValueType,
        row_ordinal: usize,
        column_ordinal: usize,
    ) -> Result<ExprId, ContractLoweringError> {
        let source = expression_type(expression);
        if source.nullable && !target.nullable
            || novarocks_types::wider_type(&source.data_type, &target.data_type) != target.data_type
        {
            return Err(ContractLoweringError::ExpressionTypeMismatch {
                context: format!("Values row {row_ordinal} column {column_ordinal}"),
                expected: target.clone(),
                actual: source,
            });
        }

        let mut lowered = self.lower_expression(owner, expression, &BTreeMap::new())?;
        let mut lowered_type = source;
        if lowered_type.data_type != target.data_type {
            lowered_type = ValueType::new(target.data_type.clone(), lowered_type.nullable);
            lowered = self.fragment_mut().add_expression(
                owner,
                lowered_type.clone(),
                ContractExprKind::Cast {
                    expr: lowered,
                    target: target.data_type.clone(),
                },
            )?;
        }
        if lowered_type == *target {
            return Ok(lowered);
        }

        let condition = self.fragment_mut().add_expression(
            owner,
            ValueType::new(DataType::Boolean, false),
            ContractExprKind::Literal(ContractLiteralValue::Boolean(true)),
        )?;
        let otherwise = self.fragment_mut().add_expression(
            owner,
            target.clone(),
            ContractExprKind::Literal(ContractLiteralValue::Null),
        )?;
        Ok(self.fragment_mut().add_expression(
            owner,
            target.clone(),
            ContractExprKind::Case {
                operand: None,
                when_then: Box::from([(condition, lowered)]),
                else_expr: Some(otherwise),
            },
        )?)
    }

    /// Lowers one ordering, and the ordering property it establishes.
    ///
    /// A node sorts by whatever the statement wrote, expression or column.
    /// What it can hand downstream is narrower: an ordering property names
    /// values, so a key the statement wrote as an expression leaves nothing
    /// for a reader above to rely on. The property is therefore claimed only
    /// where every key is a value, which is the same all-or-nothing rule the
    /// contract reads a node's ordering by.
    fn lower_ordering(
        &mut self,
        owner: NodeId,
        items: &[crate::analysis::SortItem],
        visible: &BTreeMap<ColumnId, ValueId>,
    ) -> Result<LoweredOrdering, ContractLoweringError> {
        let mut expressions = Vec::with_capacity(items.len());
        let mut ordering = Vec::with_capacity(items.len());
        let mut claimable = true;
        for item in items {
            let value = match identity_column_ref(&item.expr) {
                Some(column) => Some(
                    visible
                        .get(&column)
                        .copied()
                        .ok_or(ContractLoweringError::UnknownColumnReference(column))?,
                ),
                None => {
                    claimable = false;
                    None
                }
            };
            let expression = self.lower_expression(owner, &item.expr, visible)?;
            let direction = if item.asc {
                SortDirection::Ascending
            } else {
                SortDirection::Descending
            };
            let null_ordering = if item.nulls_first {
                NullOrdering::First
            } else {
                NullOrdering::Last
            };
            expressions.push(SortExpr {
                expr: expression,
                direction,
                null_ordering,
            });
            if let Some(value) = value {
                ordering.push(OrderingKey {
                    value,
                    direction,
                    null_ordering,
                });
            }
        }
        Ok(LoweredOrdering {
            expressions: expressions.into_boxed_slice(),
            properties: if claimable {
                ordering.into_boxed_slice()
            } else {
                Box::default()
            },
        })
    }

    /// Flattens a same-operator `AND`/`OR` chain into one ordered argument list.
    ///
    /// The optimizer builds these connectives as binary trees, so a filter
    /// panel emitting N conditions arrives as an N-deep chain. Collecting the
    /// operands iteratively keeps both the lowering cost and the resulting
    /// contract expression proportional to N in width rather than depth, which
    /// is what stops an ordinary wide predicate from reading as a pathological
    /// one. Traversal preserves left-to-right order, so short-circuit and any
    /// argument failure stay exactly where SQL put them.
    fn lower_boolean_connective(
        &mut self,
        owner: NodeId,
        root: &TypedExpr,
        connective: BinOp,
        visible: &BTreeMap<ColumnId, ValueId>,
    ) -> Result<Box<[ExprId]>, ContractLoweringError> {
        let mut operands: Vec<&TypedExpr> = Vec::new();
        let mut pending: Vec<&TypedExpr> = vec![root];
        while let Some(current) = pending.pop() {
            match &current.kind {
                ExprKind::BinaryOp { left, op, right } if *op == connective => {
                    pending.push(right);
                    pending.push(left);
                }
                _ => operands.push(current),
            }
        }
        let mut args = Vec::with_capacity(operands.len());
        for operand in operands {
            args.push(self.lower_expression(owner, operand, visible)?);
        }
        Ok(args.into_boxed_slice())
    }

    fn lower_expression(
        &mut self,
        owner: NodeId,
        expression: &TypedExpr,
        visible: &BTreeMap<ColumnId, ValueId>,
    ) -> Result<ExprId, ContractLoweringError> {
        if let ExprKind::Literal(literal) = &expression.kind {
            return self.lower_literal_expression(owner, literal, expression);
        }
        let mut ty = expression_type(expression);
        let mut kind = match &expression.kind {
            ExprKind::ColumnRef { column_id, .. } => {
                // A reference is the value, so it is typed by the value's own
                // definition rather than by what the statement was analyzed
                // to say about it. The two differ where an operator between
                // the two changed it: a grouping set nulls a key the analyzer
                // had already read as never null.
                let value = visible
                    .get(column_id)
                    .copied()
                    .ok_or(ContractLoweringError::UnknownColumnReference(*column_id))?;
                ty = self.value_declared_type(value)?;
                ContractExprKind::Value(value)
            }
            ExprKind::Literal(_) => unreachable!("literal expressions return before dispatch"),
            ExprKind::BinaryOp {
                op: op @ (BinOp::And | BinOp::Or),
                ..
            } => {
                let args = self.lower_boolean_connective(owner, expression, *op, visible)?;
                if matches!(op, BinOp::And) {
                    ContractExprKind::Conjunction { args }
                } else {
                    ContractExprKind::Disjunction { args }
                }
            }
            ExprKind::BinaryOp { left, op, right } => {
                let lowered_left = self.lower_expression(owner, left, visible)?;
                let lowered_right = self.lower_expression(owner, right, visible)?;
                // A comparison answers about one type. Its operands were
                // reconciled while the statement was analyzed -- an untyped
                // NULL, a narrower integer -- and the plan states the
                // comparison it actually performs rather than two sides the
                // reader has to reconcile again. Arithmetic keeps its own
                // operands: its result type is derived from the pair.
                let (lowered_left, lowered_right) = if is_comparison_operator(*op) {
                    let compared = novarocks_types::wider_type(
                        &self.expression_value_type(lowered_left)?.data_type,
                        &self.expression_value_type(lowered_right)?.data_type,
                    );
                    (
                        self.cast_expression_to(owner, lowered_left, &compared)?,
                        self.cast_expression_to(owner, lowered_right, &compared)?,
                    )
                } else {
                    (lowered_left, lowered_right)
                };
                ContractExprKind::Binary {
                    left: lowered_left,
                    op: lower_binary_operator(*op),
                    right: lowered_right,
                }
            }
            ExprKind::UnaryOp { op, expr } => ContractExprKind::Unary {
                op: lower_unary_operator(*op),
                expr: self.lower_expression(owner, expr, visible)?,
            },
            ExprKind::Cast { expr, target } => ContractExprKind::Cast {
                expr: self.lower_expression(owner, expr, visible)?,
                // The type a conversion produces is stated in the plan's own
                // vocabulary -- a list's element is named `item` there -- and
                // that is the vocabulary the expression's own type is read in.
                // The two are compared, so they are written the same way.
                target: novarocks_types::undecorated_nested_type(target),
            },
            ExprKind::IsNull { expr, negated } => ContractExprKind::IsNull {
                expr: self.lower_expression(owner, expr, visible)?,
                negated: *negated,
            },
            ExprKind::InList {
                expr,
                list,
                negated,
            } => {
                // A membership test compares one value against many, so all
                // of them are compared as one type. The engine widens them at
                // run time; the contract states the comparison it performs,
                // so the widening is written down here -- the same way a
                // range test and a comparison operator already do.
                let input = self.lower_expression(owner, expr, visible)?;
                let candidates = list
                    .iter()
                    .map(|item| self.lower_expression(owner, item, visible))
                    .collect::<Result<Vec<_>, _>>()?;
                let mut compared = self.expression_value_type(input)?.data_type;
                for candidate in &candidates {
                    let other = self.expression_value_type(*candidate)?.data_type;
                    compared = novarocks_types::wider_type(&compared, &other);
                }
                let list = candidates
                    .into_iter()
                    .map(|candidate| self.cast_expression_to(owner, candidate, &compared))
                    .collect::<Result<Vec<_>, _>>()?;
                ContractExprKind::InList {
                    expr: self.cast_expression_to(owner, input, &compared)?,
                    list: list.into_boxed_slice(),
                    negated: *negated,
                }
            }
            ExprKind::Between {
                expr,
                low,
                high,
                negated,
            } => {
                // A range test compares one value against two bounds, so all
                // three are compared as one type. The engine widens them at
                // run time; the contract states the comparison it performs,
                // so the widening is written down here.
                let input = self.lower_expression(owner, expr, visible)?;
                let low = self.lower_expression(owner, low, visible)?;
                let high = self.lower_expression(owner, high, visible)?;
                let mut compared = self.expression_value_type(input)?.data_type;
                for operand in [low, high] {
                    let other = self.expression_value_type(operand)?.data_type;
                    compared = novarocks_types::wider_type(&compared, &other);
                }
                ContractExprKind::Between {
                    expr: self.cast_expression_to(owner, input, &compared)?,
                    low: self.cast_expression_to(owner, low, &compared)?,
                    high: self.cast_expression_to(owner, high, &compared)?,
                    negated: *negated,
                }
            }
            ExprKind::Like {
                expr,
                pattern,
                negated,
            } => ContractExprKind::Like {
                expr: self.lower_expression(owner, expr, visible)?,
                pattern: self.lower_expression(owner, pattern, visible)?,
                negated: *negated,
            },
            ExprKind::Case {
                operand,
                when_then,
                else_expr,
            } => {
                // Every branch answers with the type the whole expression
                // answers with. A branch that says only NULL, or says a
                // narrower number than its siblings, was reconciled while the
                // statement was analyzed; carry that reconciliation rather
                // than leaving each branch its own type.
                let mut operand = operand
                    .as_deref()
                    .map(|item| self.lower_expression(owner, item, visible))
                    .transpose()?;
                let mut whens = Vec::with_capacity(when_then.len());
                for (when, _) in when_then {
                    whens.push(self.lower_expression(owner, when, visible)?);
                }
                // A simple CASE compares its operand against every label, so
                // the plan states the one type it compares them in the same
                // way a comparison operator does -- an INT operand against a
                // BIGINT label is one comparison, not two types.
                if let Some(subject) = operand {
                    let mut compared = self.expression_value_type(subject)?.data_type;
                    for when in &whens {
                        compared = novarocks_types::wider_type(
                            &compared,
                            &self.expression_value_type(*when)?.data_type,
                        );
                    }
                    operand = Some(self.cast_expression_to(owner, subject, &compared)?);
                    for when in &mut whens {
                        *when = self.cast_expression_to(owner, *when, &compared)?;
                    }
                }
                let mut branches = Vec::with_capacity(when_then.len());
                for ((_, then), when) in when_then.iter().zip(whens) {
                    let then = self.lower_expression(owner, then, visible)?;
                    branches.push((when, self.cast_expression_to(owner, then, &ty.data_type)?));
                }
                let else_expr = match else_expr.as_deref() {
                    Some(item) => {
                        let item = self.lower_expression(owner, item, visible)?;
                        Some(self.cast_expression_to(owner, item, &ty.data_type)?)
                    }
                    None => None,
                };
                ContractExprKind::Case {
                    operand,
                    when_then: branches.into_boxed_slice(),
                    else_expr,
                }
            }
            ExprKind::IsTruthValue {
                expr,
                value,
                negated,
            } => ContractExprKind::IsTruthValue {
                expr: self.lower_expression(owner, expr, visible)?,
                value: *value,
                negated: *negated,
            },
            ExprKind::FunctionCall {
                args,
                distinct,
                binding,
                volatility,
                name,
            } => {
                if *distinct {
                    return Err(ContractLoweringError::InvalidFunctionBinding {
                        detail: "scalar call carries DISTINCT".to_string(),
                    });
                }
                if binding.kind != novarocks_physical_plan::FunctionKind::Scalar {
                    return Err(ContractLoweringError::InvalidFunctionBinding {
                        detail: format!("scalar expression carries {:?} binding", binding.kind),
                    });
                }
                if binding.logical_argument_count != args.len()
                    || binding.selected.argument_types.len() != args.len()
                {
                    return Err(ContractLoweringError::InvalidFunctionBinding {
                        detail: format!(
                            "binding arity logical={} typed={} differs from expression arity {}",
                            binding.logical_argument_count,
                            binding.selected.argument_types.len(),
                            args.len()
                        ),
                    });
                }
                if *volatility != binding.semantics.volatility {
                    return Err(ContractLoweringError::InvalidFunctionBinding {
                        detail: "legacy volatility differs from the exact binding".to_string(),
                    });
                }
                let novarocks_functions::FunctionResultType::Scalar(result_type) =
                    &binding.selected.result_type
                else {
                    return Err(ContractLoweringError::InvalidFunctionBinding {
                        detail: "scalar expression carries a relation result".to_string(),
                    });
                };
                let result_type = undecorated(result_type);
                if result_type != ty {
                    return Err(ContractLoweringError::InvalidFunctionBinding {
                        detail: format!(
                            "function {name}: binding result {result_type:?} differs from expression result {ty:?}"
                        ),
                    });
                }
                let argument_types = binding
                    .selected
                    .argument_types
                    .iter()
                    .map(undecorated_argument)
                    .collect::<Box<[_]>>();
                let lowered_args = args
                    .iter()
                    .zip(argument_types.iter())
                    .map(|(argument, expected)| {
                        self.lower_bound_argument(owner, argument, visible, expected)
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                ContractExprKind::FunctionCall {
                    function: BoundFunction {
                        function_id: binding.function_id.clone(),
                        overload: binding.selected.overload.clone(),
                        kind: binding.kind,
                        argument_types,
                        result_type,
                        volatility: binding.semantics.volatility,
                        argument_evaluation: binding.semantics.argument_evaluation,
                        failure_behavior: binding.semantics.failure_behavior,
                    },
                    args: lowered_args.into_boxed_slice(),
                }
            }
            ExprKind::LambdaFunction { params, body } => {
                // The lambda's identity exists before its body, because every
                // parameter reference inside names it.
                let lambda = self.fragment_mut().reserve_expression_id()?;
                // A parameter's type is the plan's, the same way every other
                // type it states is: a provider's decoration on a nested
                // field would make the declaration and the references to it
                // read as two different types.
                let parameter_types = params
                    .iter()
                    .map(|param| {
                        undecorated(&ValueType::new(param.data_type.clone(), param.nullable))
                    })
                    .collect::<Vec<_>>();
                let enclosing = self.lambda_scope.last().map(|scope| scope.lambda);
                self.lambda_scope.push(LoweringLambdaScope {
                    lambda,
                    parameter_slots: params.iter().map(|param| param.slot_id).collect(),
                    parameter_types: parameter_types.clone(),
                });
                let body_result = self.lower_expression(owner, body, visible);
                self.lambda_scope.pop();
                let body_id = body_result?;
                let body_type = self.expression_value_type(body_id)?;
                self.fragment_mut()
                    .insert_expression(novarocks_physical_plan::ExprNode {
                        id: lambda,
                        owner,
                        lambda_scope: enclosing,
                        ty: body_type,
                        kind: ContractExprKind::Lambda {
                            parameter_types: parameter_types.into_boxed_slice(),
                            body: body_id,
                        },
                    })?;
                return Ok(lambda);
            }
            ExprKind::LambdaParamRef { name, slot_id } => {
                if self.lambda_scope.is_empty() {
                    return Err(ContractLoweringError::InvalidLambda {
                        detail: format!("lambda parameter `{name}` stands outside a lambda"),
                    });
                }
                // A name is resolved by the innermost lambda that declares
                // it, so an inner body reading an outer parameter names the
                // lambda that owns it rather than the one it stands in.
                let (lambda, ordinal, declared) = self
                    .lambda_scope
                    .iter()
                    .rev()
                    .find_map(|scope| {
                        let ordinal = scope
                            .parameter_slots
                            .iter()
                            .position(|slot| slot == slot_id)?;
                        Some((
                            scope.lambda,
                            ordinal,
                            scope.parameter_types.get(ordinal)?.clone(),
                        ))
                    })
                    .ok_or_else(|| ContractLoweringError::InvalidLambda {
                        detail: format!(
                            "lambda parameter `{name}` is not one any open lambda declares"
                        ),
                    })?;
                // The reference is the parameter, so it is typed by the
                // declaration it names -- not by what the statement was
                // analyzed to say at this position.
                ty = declared;
                ContractExprKind::LambdaParameter {
                    lambda,
                    ordinal: u32::try_from(ordinal).map_err(|_| {
                        ContractLoweringError::InvalidLambda {
                            detail: "lambda parameter ordinal exceeds u32".to_string(),
                        }
                    })?,
                }
            }
            ExprKind::Nested(inner) => return self.lower_expression(owner, inner, visible),
            other => {
                return Err(ContractLoweringError::UnsupportedExpression {
                    kind: expression_kind_name(other),
                });
            }
        };
        // A binding records the arguments it was resolved with, not something
        // the function demands: an overload is chosen by data type, and the
        // engine reads validity off every array it is handed. So where a value
        // has since passed through something that nulls it, the record says
        // so rather than describing a call this plan is not making.
        if let ContractExprKind::FunctionCall { function, args } = &mut kind {
            let mut nullable = Vec::with_capacity(args.len());
            for argument in args.iter() {
                nullable.push(
                    self.fragment_mut()
                        .expressions()
                        .get(*argument)
                        .is_some_and(|expression| expression.ty.nullable),
                );
            }
            for (expected, nullable) in function.argument_types.iter_mut().zip(nullable) {
                if let FunctionArgumentType::Value(value) = expected {
                    value.nullable = value.nullable || nullable;
                }
            }
        }
        // Nullability widens on the way out. An operator whose operand may be
        // null may answer null, whatever the statement was analyzed to say
        // about an operand that has since passed through something that nulls
        // it. Only the kinds that answer about their operands' values widen;
        // `IS NULL` answers about the absence itself and never does.
        if !ty.nullable && kind_follows_operand_nullability(&kind) {
            let mut operands = Vec::new();
            collect_operand_expressions(&kind, &mut operands);
            for operand in operands {
                if self.expression_value_type(operand)?.nullable {
                    ty.nullable = true;
                    break;
                }
            }
        }
        self.add_scoped_expression(owner, ty, kind)
    }

    /// Builds an expression in the scope that is open.
    ///
    /// An expression written inside a lambda body belongs to that lambda, not
    /// to the node: that is what lets a parameter reference resolve, and what
    /// keeps the body from reading a value the node has but the lambda does
    /// not.
    fn add_scoped_expression(
        &mut self,
        owner: NodeId,
        ty: ValueType,
        kind: ContractExprKind,
    ) -> Result<ExprId, ContractLoweringError> {
        let scope = self.lambda_scope.last().map(|scope| scope.lambda);
        Ok(self
            .fragment_mut()
            .add_expression_in_scope(owner, scope, ty, kind)?)
    }

    /// Lowers one argument in the type its binding takes.
    ///
    /// A bare `NULL` has no type of its own: it is typed by where it stands,
    /// and where it stands is an argument position the binding already names.
    /// Left untyped, the plan would state a call whose signature and argument
    /// disagree about a shape neither of them is wrong about.
    fn lower_bound_argument(
        &mut self,
        owner: NodeId,
        argument: &TypedExpr,
        visible: &BTreeMap<ColumnId, ValueId>,
        expected: &FunctionArgumentType,
    ) -> Result<ExprId, ContractLoweringError> {
        if let FunctionArgumentType::Value(expected) = expected
            && argument.data_type == DataType::Null
            && expected.data_type != DataType::Null
        {
            return Ok(self.fragment_mut().add_expression(
                owner,
                ValueType::new(expected.data_type.clone(), true),
                ContractExprKind::Literal(ContractLiteralValue::Null),
            )?);
        }
        self.lower_expression(owner, argument, visible)
    }

    /// The type one already-defined value declares.
    fn value_declared_type(&mut self, value: ValueId) -> Result<ValueType, ContractLoweringError> {
        self.fragment_mut()
            .value(value)
            .map(|definition| definition.ty.clone())
            .ok_or(ContractLoweringError::IdentitySpaceExhausted("value"))
    }

    /// The type one already-lowered expression declares.
    fn expression_value_type(&mut self, expr: ExprId) -> Result<ValueType, ContractLoweringError> {
        self.fragment_mut()
            .expressions()
            .get(expr)
            .map(|expression| expression.ty.clone())
            .ok_or(ContractLoweringError::IdentitySpaceExhausted("expression"))
    }

    /// The same expression, compared as `target`.
    ///
    /// An expression that already has the type is returned as it is: a
    /// conversion that converts nothing is not written down.
    fn cast_expression_to(
        &mut self,
        owner: NodeId,
        expr: ExprId,
        target: &DataType,
    ) -> Result<ExprId, ContractLoweringError> {
        let current = self.expression_value_type(expr)?;
        if current.data_type == *target {
            return Ok(expr);
        }
        self.add_scoped_expression(
            owner,
            ValueType::new(target.clone(), current.nullable),
            ContractExprKind::Cast {
                expr,
                target: target.clone(),
            },
        )
    }

    fn lower_literal_expression(
        &mut self,
        owner: NodeId,
        literal: &LiteralValue,
        expression: &TypedExpr,
    ) -> Result<ExprId, ContractLoweringError> {
        let target = expression_type(expression);
        let (literal, source) = lower_literal(literal, &target)?;
        // A literal is an exact value; the position it stands in states the
        // type, because everything that reads this expression was typed
        // against the same position -- the contract lets a carrier admit null
        // its value never will, and refuses only the reverse. So the carrier
        // takes the position's type, widened where the literal is a NULL the
        // position was analyzed not to expect.
        let carrier = ValueType::new(target.data_type.clone(), target.nullable || source.nullable);
        if source.data_type == target.data_type {
            return self.add_scoped_expression(owner, carrier, ContractExprKind::Literal(literal));
        }
        let literal_id =
            self.add_scoped_expression(owner, source, ContractExprKind::Literal(literal))?;
        self.add_scoped_expression(
            owner,
            carrier,
            ContractExprKind::Cast {
                expr: literal_id,
                target: target.data_type,
            },
        )
    }
}

/// One lambda being lowered: its expression identity, and the ordinal each
/// of its parameters is bound at.
struct LoweringLambdaScope {
    lambda: ExprId,
    /// Analyzer slot id of each parameter, in declaration order.
    parameter_slots: Vec<i32>,
    /// The type each parameter is declared with, in the plan's vocabulary.
    parameter_types: Vec<ValueType>,
}

struct LoweredNode {
    fragment: FragmentId,
    node: NodeId,
    output: Box<[ValueId]>,
    columns: BTreeMap<ColumnId, ValueId>,
    properties: PhysicalProperties,
    display_names: Box<[String]>,
}

struct LoweredWriterRelation {
    schema: WriterRelationSchema,
    output: Box<[ValueId]>,
    values_by_slot: BTreeMap<u32, ValueId>,
}

struct JoinOutputRequest<'a> {
    kind: ContractJoinKind,
    requested: &'a [OutputColumn],
    left: &'a LoweredNode,
    left_columns: &'a [OutputColumn],
    right: &'a LoweredNode,
    right_columns: &'a [OutputColumn],
}

struct JoinOutput {
    output: Box<[ValueId]>,
    columns: BTreeMap<ColumnId, ValueId>,
    null_extended: Box<[ValueId]>,
}

struct LoweredOrdering {
    expressions: Box<[SortExpr]>,
    properties: Box<[OrderingKey]>,
}

struct HashJoinRequirement<'a> {
    plan: &'a PhysicalPlanNode,
    kind: ContractJoinKind,
    build_side: ContractJoinSide,
    distribution: ContractJoinDistribution,
    left: &'a LoweredNode,
    right: &'a LoweredNode,
    left_keys: &'a [Option<ValueId>],
    right_keys: &'a [Option<ValueId>],
}

enum ExchangeLayout {
    Gather,
    Broadcast,
    Hash {
        keys: Box<[ColumnId]>,
        scheme: HashPartitionScheme,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum ProviderPartitionDefinition {
    Hash(ProviderHashPartitionScheme),
    Bucket(ProviderBucketPartitionScheme),
}

fn writer_import_schema(
    auxiliary: &WriterAuxiliaryPlan,
    values: &[ValueId],
) -> Result<WriterRelationSchema, ContractLoweringError> {
    let fields = auxiliary.schema().arrow_schema();
    if fields.fields().len() != values.len() {
        return Err(invalid_write("writer import schema arity differs".into()));
    }
    Ok(WriterRelationSchema {
        revision: auxiliary.schema().contract_version(),
        fields: fields
            .fields()
            .iter()
            .zip(values)
            .enumerate()
            .map(|(index, (field, value))| WriterRelationField {
                value: *value,
                name: field.name().clone().into_boxed_str(),
                ty: ValueType::new(field.data_type().clone(), field.is_nullable()),
                role: match index {
                    0 => WriterRelationFieldRole::Kind,
                    1 => WriterRelationFieldRole::TargetOrdinal,
                    2 => WriterRelationFieldRole::RowCount,
                    3 => WriterRelationFieldRole::CommitFragment,
                    _ => WriterRelationFieldRole::Auxiliary,
                },
            })
            .collect::<Vec<_>>()
            .into_boxed_slice(),
    })
}

fn lower_writer_aggregate_binding(
    resolved: &novarocks_functions::ResolvedFunctionBinding,
    phase: AggregatePhase,
) -> Result<AggregateBinding, ContractLoweringError> {
    if resolved.kind != novarocks_physical_plan::FunctionKind::Aggregate
        || resolved.logical_argument_count != 1
    {
        return Err(invalid_write(
            "writer aggregate binding is not an exact unary aggregate".into(),
        ));
    }
    let novarocks_functions::FunctionResultType::Scalar(result) = &resolved.selected.result_type
    else {
        return Err(invalid_write("writer aggregate returns a relation".into()));
    };
    let aggregate = resolved
        .selected
        .aggregate
        .as_ref()
        .ok_or_else(|| invalid_write("writer aggregate has no state contract".into()))?;
    Ok(AggregateBinding {
        function: bound_function_from_resolved(resolved, result),
        phase,
        logical_argument_count: 1,
        intermediate_type: aggregate.intermediate_type.clone(),
        state_format: aggregate.state_format.clone(),
    })
}

fn writer_aggregate_result_type(
    resolved: &novarocks_functions::ResolvedFunctionBinding,
) -> Result<ValueType, ContractLoweringError> {
    match &resolved.selected.result_type {
        novarocks_functions::FunctionResultType::Scalar(result) => Ok(result.clone()),
        novarocks_functions::FunctionResultType::Relation(_) => {
            Err(invalid_write("writer aggregate returns a relation".into()))
        }
    }
}

fn invalid_write(detail: String) -> ContractLoweringError {
    ContractLoweringError::InvalidWrite { detail }
}

fn mv_rewrite_provenance_annotation(
    selection: &crate::planner::payload::MvRewriteSelection,
) -> Result<Box<str>, ContractLoweringError> {
    let publication_id =
        selection
            .publication_id()
            .ok_or(ContractLoweringError::MissingPlannerFact {
                node: "Scan",
                fact: "MV rewrite publication identity",
            })?;
    let definition_fingerprint =
        selection
            .definition_fingerprint()
            .ok_or(ContractLoweringError::MissingPlannerFact {
                node: "Scan",
                fact: "MV rewrite definition fingerprint",
            })?;
    let publication_state_digest = mv_rewrite_publication_state_digest(selection)?;
    Ok(format!(
        "v2;publication_id={};definition_fingerprint={};publication_state_digest={}",
        hex_bytes(&publication_id),
        hex_bytes(&definition_fingerprint),
        hex_bytes(&publication_state_digest)
    )
    .into_boxed_str())
}

fn mv_rewrite_publication_state_digest(
    selection: &crate::planner::payload::MvRewriteSelection,
) -> Result<[u8; 32], ContractLoweringError> {
    let target =
        selection
            .publication_target()
            .ok_or(ContractLoweringError::MissingPlannerFact {
                node: "Scan",
                fact: "MV rewrite publication target revision",
            })?;
    if selection.publication_inputs().is_empty() {
        return Err(ContractLoweringError::MissingPlannerFact {
            node: "Scan",
            fact: "MV rewrite publication input revisions",
        });
    }

    let mut digest = Sha256::new();
    update_digest_part(
        &mut digest,
        b"novarocks.mv-rewrite-publication-provenance.v2",
    );
    for (fact, revision) in [
        (
            "MV rewrite definition revision",
            selection.definition_revision(),
        ),
        (
            "MV rewrite interpretation revision",
            selection.interpretation_revision(),
        ),
    ] {
        update_digest_part(
            &mut digest,
            &revision.ok_or(ContractLoweringError::MissingPlannerFact { node: "Scan", fact })?,
        );
    }
    update_digest_part(
        &mut digest,
        selection
            .publication_provenance()
            .ok_or(ContractLoweringError::MissingPlannerFact {
                node: "Scan",
                fact: "MV rewrite publication provenance",
            })?
            .as_bytes(),
    );
    update_digest_part(
        &mut digest,
        &(selection.publication_inputs().len() as u64).to_be_bytes(),
    );
    for input in selection.publication_inputs() {
        update_digest_part(&mut digest, &input.occurrence_id().get().to_be_bytes());
        update_mv_publication_relation_digest(&mut digest, b"input", input.relation());
    }
    update_mv_publication_relation_digest(&mut digest, b"target", target);
    Ok(digest.finalize().into())
}

fn update_mv_publication_relation_digest(
    digest: &mut Sha256,
    role: &[u8],
    relation: &crate::compiler::SqlMvRewritePublicationRelation,
) {
    update_digest_part(digest, role);
    update_digest_part(digest, relation.table_fqn().as_bytes());
    update_mv_semantic_fact_digest(digest, relation.revision().object_identity());
    update_mv_semantic_fact_digest(digest, relation.revision().data_version());
}

fn update_mv_semantic_fact_digest(
    digest: &mut Sha256,
    fact: &novarocks_spi::connector::ConnectorSemanticFact,
) {
    update_digest_part(digest, fact.provider().as_str().as_bytes());
    update_digest_part(digest, fact.format().as_bytes());
    update_digest_part(digest, &fact.version().to_be_bytes());
    update_digest_part(digest, fact.value());
}

fn update_digest_part(digest: &mut Sha256, value: &[u8]) {
    digest.update((value.len() as u64).to_be_bytes());
    digest.update(value);
}

fn hex_bytes(bytes: &[u8]) -> String {
    use std::fmt::Write as _;

    let mut encoded = String::with_capacity(bytes.len().saturating_mul(2));
    for byte in bytes {
        write!(&mut encoded, "{byte:02x}").expect("writing to a String cannot fail");
    }
    encoded
}

fn provider_partition_identity_digest(
    label: &[u8],
    version: PlanVersionId,
    read: &ProviderReadReference,
    tokens: &[&[u8]],
) -> [u8; 32] {
    let descriptor = read.binding.descriptor();
    let relation_kind = match read.relation.kind() {
        ConnectorReadRelationKind::Table => b"table".as_slice(),
        ConnectorReadRelationKind::TableFunction => b"table-function".as_slice(),
        ConnectorReadRelationKind::ChangeWindow => b"change-window".as_slice(),
        ConnectorReadRelationKind::SystemTable => b"system-table".as_slice(),
        ConnectorReadRelationKind::TableExecute => b"table-execute".as_slice(),
        ConnectorReadRelationKind::MergeTable => b"merge-table".as_slice(),
    };
    let catalog_version = read.binding.catalog_handle().version();
    let mut parts = vec![
        b"provider".as_slice(),
        descriptor.provider_id.as_str().as_bytes(),
        descriptor.instance_id.as_str().as_bytes(),
        catalog_version.as_bytes(),
        relation_kind,
    ];
    parts.extend_from_slice(tokens);
    partition_identity_digest(label, version, &parts)
}

fn partition_identity_digest(label: &[u8], version: PlanVersionId, parts: &[&[u8]]) -> [u8; 32] {
    let mut digest = Sha256::new();
    for part in std::iter::once(label)
        .chain(std::iter::once(version.as_bytes().as_slice()))
        .chain(parts.iter().copied())
    {
        digest.update((part.len() as u64).to_be_bytes());
        digest.update(part);
    }
    digest.finalize().into()
}

fn result_fields(
    plan: &PhysicalPlanNode,
    values: &[ValueId],
    types: &[ValueType],
    display_names: &[String],
) -> Result<Box<[ResultField]>, ContractLoweringError> {
    let columns = &plan.output_columns;
    if columns.len() != values.len() {
        return Err(ContractLoweringError::ArityMismatch {
            context: "result port",
            expected: columns.len(),
            actual: values.len(),
        });
    }
    if columns.len() != display_names.len() {
        return Err(ContractLoweringError::ArityMismatch {
            context: "result field names",
            expected: columns.len(),
            actual: display_names.len(),
        });
    }
    let identities = result_field_identities(plan);
    if columns.len() != types.len() {
        return Err(ContractLoweringError::ArityMismatch {
            context: "result field types",
            expected: columns.len(),
            actual: types.len(),
        });
    }
    Ok(columns
        .iter()
        .zip(values.iter().zip(display_names))
        .zip(types)
        .enumerate()
        .map(|(ordinal, ((column, (value, _display_name)), ty))| {
            let identity = identities.as_ref().and_then(|items| items.get(ordinal));
            let name = identity
                .map(|identity| identity.name.as_str())
                .unwrap_or(&column.name);
            // An alias is a name the statement gave the field. A display name
            // that is this same column said with the relation it came from is
            // not one: `SELECT id FROM t1` delivers `id`, however the plan
            // reaches it.
            // An alias is a name the statement gave this field, which is what
            // the projection at the root of the plan records. A display name
            // is what the column is called wherever it is read, and a
            // statement that aliased nothing still has one.
            let alias = identity.and_then(|identity| identity.alias.as_deref());
            ResultField {
                name: name.into(),
                alias: alias.map(Into::into),
                value: *value,
                ty: ty.clone(),
            }
        })
        .collect::<Vec<_>>()
        .into_boxed_slice())
}

struct ResultFieldIdentity {
    name: String,
    alias: Option<String>,
}

fn result_field_identities(plan: &PhysicalPlanNode) -> Option<Vec<ResultFieldIdentity>> {
    if let PhysicalPlanKind::Project(project) = &plan.kind {
        return Some(
            project
                .items
                .iter()
                .map(|item| {
                    let name = crate::analysis::expr_display::typed_expr_display_name(&item.expr);
                    let alias = (name != item.output_name).then(|| item.output_name.clone());
                    ResultFieldIdentity { name, alias }
                })
                .collect(),
        );
    }
    let passthrough_child = if plan.children.len() == 1 {
        plan.children.first()
    } else if matches!(&plan.kind, PhysicalPlanKind::CTEAnchor(_)) {
        plan.children.get(1)
    } else {
        None
    }?;
    let preserves_occurrences = passthrough_child.output_columns.len() == plan.output_columns.len()
        && passthrough_child
            .output_columns
            .iter()
            .zip(&plan.output_columns)
            .all(|(child, output)| child.column_id == output.column_id);
    preserves_occurrences
        .then(|| result_field_identities(passthrough_child))
        .flatten()
}

fn singleton_properties() -> PhysicalProperties {
    PhysicalProperties {
        distribution: Distribution::Singleton,
        row_multiplicity: RowMultiplicity::SingleCopy,
        ordering: Box::default(),
    }
}

fn find_output_column(columns: &[OutputColumn], id: ColumnId) -> Option<&OutputColumn> {
    columns.iter().find(|column| column.column_id == id)
}

fn merge_visible_columns(
    node: &'static str,
    left: &BTreeMap<ColumnId, ValueId>,
    right: &BTreeMap<ColumnId, ValueId>,
) -> Result<BTreeMap<ColumnId, ValueId>, ContractLoweringError> {
    let mut visible = left.clone();
    for (column, value) in right {
        if visible.insert(*column, *value).is_some() {
            return Err(ContractLoweringError::AmbiguousInputColumn {
                node,
                column: *column,
            });
        }
    }
    Ok(visible)
}

fn require_same_fragment(
    node: &'static str,
    left: &LoweredNode,
    right: &LoweredNode,
) -> Result<(), ContractLoweringError> {
    if left.fragment == right.fragment {
        Ok(())
    } else {
        Err(ContractLoweringError::UnexpectedFragment {
            node,
            expected: left.fragment,
            actual: right.fragment,
        })
    }
}

fn require_no_partition_expressions(
    mode: &'static str,
    expressions: &[TypedExpr],
) -> Result<(), ContractLoweringError> {
    if expressions.is_empty() {
        Ok(())
    } else {
        Err(ContractLoweringError::InvalidPartitionExpressions {
            node: "Redistribute",
            detail: match mode {
                "Gather" => "gather carries partition expressions",
                "Broadcast" => "broadcast carries partition expressions",
                _ => "non-hash exchange carries partition expressions",
            },
        })
    }
}

fn lower_join_kind(kind: crate::common::JoinKind) -> ContractJoinKind {
    match kind {
        crate::common::JoinKind::Cross => ContractJoinKind::Cross,
        crate::common::JoinKind::Inner => ContractJoinKind::Inner,
        crate::common::JoinKind::LeftOuter => ContractJoinKind::LeftOuter,
        crate::common::JoinKind::RightOuter => ContractJoinKind::RightOuter,
        crate::common::JoinKind::FullOuter => ContractJoinKind::FullOuter,
        crate::common::JoinKind::LeftSemi => ContractJoinKind::LeftSemi,
        crate::common::JoinKind::RightSemi => ContractJoinKind::RightSemi,
        crate::common::JoinKind::LeftAnti => ContractJoinKind::LeftAnti,
        crate::common::JoinKind::RightAnti => ContractJoinKind::RightAnti,
        crate::common::JoinKind::NullAwareLeftAnti => ContractJoinKind::NullAwareLeftAnti,
    }
}

fn resolve_hash_join_distribution(
    join: &crate::planner::physical::PhysicalHashJoinNode,
) -> Result<ContractJoinDistribution, ContractLoweringError> {
    let distribution = match join.distribution {
        SqlJoinDistribution::Unknown => {
            return Err(ContractLoweringError::MissingPlannerFact {
                node: "HashJoin",
                fact: "exact distribution mode",
            });
        }
        SqlJoinDistribution::Shuffle => ContractJoinDistribution::Partitioned,
        SqlJoinDistribution::Broadcast => ContractJoinDistribution::BroadcastBuild,
        SqlJoinDistribution::Colocate => ContractJoinDistribution::Colocated,
        SqlJoinDistribution::Singleton => ContractJoinDistribution::Singleton,
    };
    let execution_distribution = join.execution_mode.map(|mode| match mode {
        JoinExecutionMode::Broadcast => ContractJoinDistribution::BroadcastBuild,
        JoinExecutionMode::Partitioned => ContractJoinDistribution::Partitioned,
        JoinExecutionMode::Colocate => ContractJoinDistribution::Colocated,
        JoinExecutionMode::Singleton => ContractJoinDistribution::Singleton,
    });
    if execution_distribution.is_some_and(|actual| actual != distribution) {
        return Err(ContractLoweringError::InvalidJoin {
            node: "HashJoin",
            detail: "planner distribution and execution mode disagree",
        });
    }
    Ok(distribution)
}

fn direct_join_key_value(expression: &TypedExpr, input: &LoweredNode) -> Option<ValueId> {
    identity_column_ref(expression).and_then(|column| input.columns.get(&column).copied())
}

fn hash_join_required_inputs(
    requirement: HashJoinRequirement<'_>,
) -> Result<Box<[PhysicalProperties]>, ContractLoweringError> {
    let HashJoinRequirement {
        plan,
        kind,
        build_side,
        distribution,
        left,
        right,
        left_keys,
        right_keys,
    } = requirement;
    let exact_keys = |side: &'static str, keys: &[Option<ValueId>]| {
        keys.iter()
            .copied()
            .collect::<Option<Vec<_>>>()
            .ok_or(ContractLoweringError::InvalidJoin {
                node: "HashJoin",
                detail: match side {
                    "left" => "left partition key is not an exact input value",
                    _ => "right partition key is not an exact input value",
                },
            })
    };
    let requirement = |input: &LoweredNode, multiplicity| PhysicalProperties {
        distribution: input.properties.distribution.clone(),
        row_multiplicity: multiplicity,
        ordering: Box::default(),
    };

    match distribution {
        ContractJoinDistribution::BroadcastBuild => {
            let build_ordinal = match build_side {
                ContractJoinSide::Left => 0,
                ContractJoinSide::Right => 1,
            };
            if !matches!(
                &plan.children[build_ordinal].kind,
                PhysicalPlanKind::Redistribute(redistribute)
                    if redistribute.mode == RedistributeMode::Broadcast
            ) {
                return Err(ContractLoweringError::MissingPlannerFact {
                    node: "HashJoin",
                    fact: "explicit broadcast exchange on the build input",
                });
            }
            let safe = match kind {
                ContractJoinKind::Inner => true,
                ContractJoinKind::LeftOuter
                | ContractJoinKind::LeftSemi
                | ContractJoinKind::LeftAnti
                | ContractJoinKind::NullAwareLeftAnti => build_side == ContractJoinSide::Right,
                ContractJoinKind::RightOuter
                | ContractJoinKind::RightSemi
                | ContractJoinKind::RightAnti => build_side == ContractJoinSide::Left,
                ContractJoinKind::FullOuter | ContractJoinKind::Cross => false,
            };
            if !safe {
                return Err(ContractLoweringError::InvalidJoin {
                    node: "HashJoin",
                    detail: "broadcast build would duplicate build-side output rows",
                });
            }
            let (left_multiplicity, right_multiplicity) = match build_side {
                ContractJoinSide::Left => {
                    if left.properties.distribution != Distribution::Broadcast
                        || left.properties.row_multiplicity != RowMultiplicity::Replicated
                        || right.properties.row_multiplicity != RowMultiplicity::SingleCopy
                    {
                        return Err(ContractLoweringError::PropertyRequirementUnsatisfied {
                            node: "HashJoin",
                            detail: "left build is not explicitly broadcast over a single-copy probe",
                        });
                    }
                    (RowMultiplicity::Replicated, RowMultiplicity::SingleCopy)
                }
                ContractJoinSide::Right => {
                    if right.properties.distribution != Distribution::Broadcast
                        || right.properties.row_multiplicity != RowMultiplicity::Replicated
                        || left.properties.row_multiplicity != RowMultiplicity::SingleCopy
                    {
                        return Err(ContractLoweringError::PropertyRequirementUnsatisfied {
                            node: "HashJoin",
                            detail: "right build is not explicitly broadcast over a single-copy probe",
                        });
                    }
                    (RowMultiplicity::SingleCopy, RowMultiplicity::Replicated)
                }
            };
            Ok(Box::from([
                requirement(left, left_multiplicity),
                requirement(right, right_multiplicity),
            ]))
        }
        ContractJoinDistribution::Partitioned => {
            // What a partitioned join needs is that both inputs are already
            // hash-partitioned on its keys under one scheme, which is what
            // the properties below state. An exchange is how an input most
            // often gets there, not what the join requires: two aggregated
            // subqueries joined on the key they grouped by arrive already
            // co-partitioned, and a colocated join is checked the same way.
            let left_keys = exact_keys("left", left_keys)?;
            let right_keys = exact_keys("right", right_keys)?;
            let (
                Distribution::Hash {
                    keys: actual_left,
                    scheme: left_scheme,
                },
                Distribution::Hash {
                    keys: actual_right,
                    scheme: right_scheme,
                },
            ) = (
                &left.properties.distribution,
                &right.properties.distribution,
            )
            else {
                return Err(ContractLoweringError::PropertyRequirementUnsatisfied {
                    node: "HashJoin",
                    detail: "partitioned inputs do not carry exact hash distributions",
                });
            };
            if left.properties.row_multiplicity != RowMultiplicity::SingleCopy
                || right.properties.row_multiplicity != RowMultiplicity::SingleCopy
                || left_scheme != right_scheme
                || actual_left.as_ref() != left_keys
                || actual_right.as_ref() != right_keys
            {
                return Err(ContractLoweringError::PropertyRequirementUnsatisfied {
                    node: "HashJoin",
                    detail: "partitioned inputs do not share one exact key-aligned hash scheme",
                });
            }
            Ok(Box::from([
                requirement(left, RowMultiplicity::SingleCopy),
                requirement(right, RowMultiplicity::SingleCopy),
            ]))
        }
        ContractJoinDistribution::Colocated => {
            let left_keys = exact_keys("left", left_keys)?;
            let right_keys = exact_keys("right", right_keys)?;
            let (
                Distribution::BucketShuffle {
                    keys: actual_left,
                    scheme: left_scheme,
                },
                Distribution::BucketShuffle {
                    keys: actual_right,
                    scheme: right_scheme,
                },
            ) = (
                &left.properties.distribution,
                &right.properties.distribution,
            )
            else {
                return Err(ContractLoweringError::MissingPlannerFact {
                    node: "HashJoin",
                    fact: "matching key-aligned bucket partition schemes on both inputs",
                });
            };
            if left.properties.row_multiplicity != RowMultiplicity::SingleCopy
                || right.properties.row_multiplicity != RowMultiplicity::SingleCopy
                || left_scheme != right_scheme
                || actual_left.as_ref() != left_keys
                || actual_right.as_ref() != right_keys
            {
                return Err(ContractLoweringError::InvalidJoin {
                    node: "HashJoin",
                    detail: "colocated inputs have different bucket spaces or misaligned keys",
                });
            }
            Ok(Box::from([
                requirement(left, RowMultiplicity::SingleCopy),
                requirement(right, RowMultiplicity::SingleCopy),
            ]))
        }
        ContractJoinDistribution::Singleton => {
            if left.properties.distribution != Distribution::Singleton
                || right.properties.distribution != Distribution::Singleton
                || left.properties.row_multiplicity != RowMultiplicity::SingleCopy
                || right.properties.row_multiplicity != RowMultiplicity::SingleCopy
            {
                return Err(ContractLoweringError::PropertyRequirementUnsatisfied {
                    node: "HashJoin",
                    detail: "singleton inputs are not both single-copy singleton streams",
                });
            }
            Ok(Box::from([
                requirement(left, RowMultiplicity::SingleCopy),
                requirement(right, RowMultiplicity::SingleCopy),
            ]))
        }
    }
}

fn hash_join_output_distribution(
    kind: ContractJoinKind,
    build_side: ContractJoinSide,
    left: &LoweredNode,
    right: &LoweredNode,
) -> Distribution {
    match (kind, build_side) {
        (ContractJoinKind::Inner, ContractJoinSide::Left) => right.properties.distribution.clone(),
        (
            ContractJoinKind::Inner
            | ContractJoinKind::LeftOuter
            | ContractJoinKind::LeftSemi
            | ContractJoinKind::LeftAnti
            | ContractJoinKind::NullAwareLeftAnti,
            _,
        ) => left.properties.distribution.clone(),
        (
            ContractJoinKind::RightOuter
            | ContractJoinKind::RightSemi
            | ContractJoinKind::RightAnti,
            _,
        ) => right.properties.distribution.clone(),
        (ContractJoinKind::FullOuter | ContractJoinKind::Cross, _) => Distribution::Unconstrained,
    }
}

fn occurrence_representatives(values: &[ValueId]) -> Vec<usize> {
    let mut first = BTreeMap::new();
    values
        .iter()
        .enumerate()
        .map(|(ordinal, value)| *first.entry(*value).or_insert(ordinal))
        .collect()
}

fn insert_output_column(
    node: &'static str,
    ordinal: usize,
    column: &OutputColumn,
    value: ValueId,
    columns: &mut BTreeMap<ColumnId, ValueId>,
) -> Result<(), ContractLoweringError> {
    if let Some(previous) = columns.insert(column.column_id, value)
        && previous != value
    {
        return Err(ContractLoweringError::OutputColumnMismatch {
            node,
            ordinal,
            detail: "one SQL column identity maps to different output values".into(),
        });
    }
    Ok(())
}

fn require_lowered_output_shape(
    node: &'static str,
    lowered: &LoweredNode,
    columns: &[OutputColumn],
) -> Result<(), ContractLoweringError> {
    if lowered.output.len() != columns.len() {
        return Err(ContractLoweringError::ArityMismatch {
            context: "lowered node output",
            expected: columns.len(),
            actual: lowered.output.len(),
        });
    }
    for (ordinal, (value, column)) in lowered.output.iter().zip(columns).enumerate() {
        if lowered.columns.get(&column.column_id) != Some(value) {
            return Err(ContractLoweringError::OutputColumnMismatch {
                node,
                ordinal,
                detail: "output occurrence differs from its SQL column identity".to_string(),
            });
        }
    }
    Ok(())
}

/// The partition keys a window groups by, in the order it groups them.
///
/// A key may be an expression -- the merged column a FULL OUTER `USING`
/// produces is the `COALESCE` of its two sides -- and the window evaluates it
/// over the rows it receives. What such a key cannot do is prove a layout
/// colocates it, which is decided where the node's properties are.
fn lower_window_partition_expressions(
    visitor: &mut ContractLoweringVisitor,
    owner: NodeId,
    expressions: &[TypedExpr],
    visible: &BTreeMap<ColumnId, ValueId>,
) -> Result<Box<[SortExpr]>, ContractLoweringError> {
    expressions
        .iter()
        .map(|expression| {
            Ok(SortExpr {
                expr: visitor.lower_expression(owner, expression, visible)?,
                direction: SortDirection::Ascending,
                null_ordering: NullOrdering::First,
            })
        })
        .collect::<Result<Vec<_>, _>>()
        .map(Vec::into_boxed_slice)
}

fn same_window_signature(
    left: &crate::planner::payload::WindowExpr,
    right: &crate::planner::payload::WindowExpr,
) -> bool {
    left.partition_by.len() == right.partition_by.len()
        && left
            .partition_by
            .iter()
            .zip(&right.partition_by)
            .all(|(left, right)| {
                identity_column_ref(left) == identity_column_ref(right)
                    && expression_type(left) == expression_type(right)
            })
        && left.order_by.len() == right.order_by.len()
        && left
            .order_by
            .iter()
            .zip(&right.order_by)
            .all(|(left, right)| {
                identity_column_ref(&left.expr) == identity_column_ref(&right.expr)
                    && expression_type(&left.expr) == expression_type(&right.expr)
                    && left.asc == right.asc
                    && left.nulls_first == right.nulls_first
            })
}

/// The ordering a window requires of its input, when it can be stated.
///
/// An ordering property names values. A window written over an expression --
/// a FULL OUTER `USING` column is the `COALESCE` of its two sides -- requires
/// an ordering the plan has no value to name, and `None` says exactly that:
/// the sort the planner placed below is carried as it wrote it, and nothing
/// above it claims an ordering it cannot prove.
fn window_ordering_keys(
    window: &crate::planner::payload::WindowExpr,
    visible: &BTreeMap<ColumnId, ValueId>,
) -> Result<Option<Vec<OrderingKey>>, ContractLoweringError> {
    let mut ordering = Vec::with_capacity(window.partition_by.len() + window.order_by.len());
    for expression in &window.partition_by {
        let Some(column) = identity_column_ref(expression) else {
            return Ok(None);
        };
        ordering.push(OrderingKey {
            value: visible
                .get(&column)
                .copied()
                .ok_or(ContractLoweringError::UnknownColumnReference(column))?,
            direction: SortDirection::Ascending,
            null_ordering: NullOrdering::First,
        });
    }
    for item in &window.order_by {
        let Some(column) = identity_column_ref(&item.expr) else {
            return Ok(None);
        };
        ordering.push(OrderingKey {
            value: visible
                .get(&column)
                .copied()
                .ok_or(ContractLoweringError::UnknownColumnReference(column))?,
            direction: if item.asc {
                SortDirection::Ascending
            } else {
                SortDirection::Descending
            },
            null_ordering: if item.nulls_first {
                NullOrdering::First
            } else {
                NullOrdering::Last
            },
        });
    }
    Ok(Some(ordering))
}

fn ordering_has_prefix(actual: &[OrderingKey], required: &[OrderingKey]) -> bool {
    actual.len() >= required.len() && actual[..required.len()] == *required
}

fn distribution_colocates_columns(
    distribution: &Distribution,
    partition_by: &[TypedExpr],
    visible: &BTreeMap<ColumnId, ValueId>,
) -> Result<bool, ContractLoweringError> {
    // One stream holds every partition whole, so there is nothing to compare
    // the keys against -- which is also the only case where a key written as
    // an expression can be colocated at all, since the plan has no value to
    // name it by.
    if *distribution == Distribution::Singleton {
        return Ok(true);
    }
    let mut keys = std::collections::BTreeSet::new();
    for expression in partition_by {
        let Some(column) = identity_column_ref(expression) else {
            return Ok(false);
        };
        keys.insert(
            visible
                .get(&column)
                .copied()
                .ok_or(ContractLoweringError::UnknownColumnReference(column))?,
        );
    }
    Ok(match distribution {
        Distribution::Singleton => true,
        Distribution::Hash {
            keys: distribution_keys,
            ..
        }
        | Distribution::BucketShuffle {
            keys: distribution_keys,
            ..
        } => distribution_keys.iter().all(|key| keys.contains(key)),
        Distribution::Unconstrained | Distribution::RoundRobin | Distribution::Broadcast => false,
    })
}

fn window_result_type(
    binding: &novarocks_functions::ResolvedFunctionBinding,
) -> Result<ValueType, ContractLoweringError> {
    let novarocks_functions::FunctionResultType::Scalar(result) = &binding.selected.result_type
    else {
        return Err(ContractLoweringError::InvalidWindow {
            detail: "window binding carries a relation result".to_string(),
        });
    };
    // The plan reads every type in its own vocabulary, where a list's element
    // is named `item`; a binding resolved against a provider's column carries
    // that provider's naming, and the two would differ over decoration the
    // window does not compute with.
    Ok(undecorated(result))
}

fn bound_function_from_resolved(
    binding: &novarocks_functions::ResolvedFunctionBinding,
    result_type: &ValueType,
) -> BoundFunction {
    BoundFunction {
        function_id: binding.function_id.clone(),
        overload: binding.selected.overload.clone(),
        kind: binding.kind,
        argument_types: binding
            .selected
            .argument_types
            .iter()
            .map(undecorated_argument)
            .collect(),
        result_type: undecorated(result_type),
        volatility: binding.semantics.volatility,
        argument_evaluation: binding.semantics.argument_evaluation,
        failure_behavior: binding.semantics.failure_behavior,
    }
}

fn lower_resolved_aggregate_binding(
    resolved: &novarocks_functions::ResolvedFunctionBinding,
    logical_argument_count: usize,
    order_by_count: usize,
    phase: AggregatePhase,
) -> Result<AggregateBinding, ContractLoweringError> {
    if resolved.kind != novarocks_physical_plan::FunctionKind::Aggregate
        || resolved.logical_argument_count != logical_argument_count
        || resolved.selected.argument_types.len() != logical_argument_count + order_by_count
    {
        return Err(ContractLoweringError::InvalidWindow {
            detail: "aggregate window binding arity or kind is inconsistent".to_string(),
        });
    }
    let result_type = window_result_type(resolved)?;
    let aggregate = resolved.selected.aggregate.as_ref().ok_or_else(|| {
        ContractLoweringError::InvalidWindow {
            detail: "aggregate window binding lacks state metadata".to_string(),
        }
    })?;
    Ok(AggregateBinding {
        function: bound_function_from_resolved(resolved, &result_type),
        phase,
        logical_argument_count: u32::try_from(logical_argument_count).map_err(|_| {
            ContractLoweringError::InvalidWindow {
                detail: "aggregate window logical argument count exceeds u32".to_string(),
            }
        })?,
        intermediate_type: aggregate.intermediate_type.clone(),
        state_format: aggregate.state_format.clone(),
    })
}

fn expression_is_replica_deterministic(expression: &TypedExpr) -> bool {
    match &expression.kind {
        ExprKind::ColumnRef { .. } | ExprKind::Literal(_) | ExprKind::LambdaParamRef { .. } => true,
        ExprKind::BinaryOp { left, right, .. } => {
            expression_is_replica_deterministic(left) && expression_is_replica_deterministic(right)
        }
        ExprKind::UnaryOp { expr, .. }
        | ExprKind::Cast { expr, .. }
        | ExprKind::IsNull { expr, .. }
        | ExprKind::IsTruthValue { expr, .. }
        | ExprKind::Nested(expr) => expression_is_replica_deterministic(expr),
        ExprKind::FunctionCall { args, binding, .. } => {
            binding.semantics.volatility == novarocks_physical_plan::FunctionVolatility::Immutable
                && args.iter().all(expression_is_replica_deterministic)
        }
        ExprKind::InList { expr, list, .. } => {
            expression_is_replica_deterministic(expr)
                && list.iter().all(expression_is_replica_deterministic)
        }
        ExprKind::Between {
            expr, low, high, ..
        } => {
            expression_is_replica_deterministic(expr)
                && expression_is_replica_deterministic(low)
                && expression_is_replica_deterministic(high)
        }
        ExprKind::Like { expr, pattern, .. } => {
            expression_is_replica_deterministic(expr)
                && expression_is_replica_deterministic(pattern)
        }
        ExprKind::Case {
            operand,
            when_then,
            else_expr,
        } => {
            operand
                .as_deref()
                .is_none_or(expression_is_replica_deterministic)
                && when_then.iter().all(|(when, then)| {
                    expression_is_replica_deterministic(when)
                        && expression_is_replica_deterministic(then)
                })
                && else_expr
                    .as_deref()
                    .is_none_or(expression_is_replica_deterministic)
        }
        ExprKind::LambdaFunction { body, .. } | ExprKind::Lambda { body, .. } => {
            expression_is_replica_deterministic(body)
        }
        ExprKind::AggregateCall { .. }
        | ExprKind::WindowCall { .. }
        | ExprKind::SubqueryPlaceholder { .. } => false,
    }
}

fn lower_aggregate_binding(
    call: &crate::planner::payload::AggregateCall,
    phase: AggregatePhase,
) -> Result<AggregateBinding, ContractLoweringError> {
    let resolved = &call.resolved;
    if resolved.kind != novarocks_physical_plan::FunctionKind::Aggregate {
        return Err(ContractLoweringError::InvalidAggregate {
            detail: "call carries a non-aggregate function binding",
        });
    }
    if resolved.logical_argument_count != call.args.len()
        || resolved.selected.argument_types.len() != call.args.len() + call.order_by.len()
    {
        return Err(ContractLoweringError::InvalidAggregate {
            detail: "binding logical/ORDER BY arity differs from the call",
        });
    }
    let novarocks_functions::FunctionResultType::Scalar(result_type) =
        &resolved.selected.result_type
    else {
        return Err(ContractLoweringError::InvalidAggregate {
            detail: "aggregate binding has a relation result",
        });
    };
    let aggregate =
        resolved
            .selected
            .aggregate
            .as_ref()
            .ok_or(ContractLoweringError::InvalidAggregate {
                detail: "aggregate binding has no intermediate state contract",
            })?;
    // A call's own `result_type` is the type its phase's carrier column has,
    // not the aggregate's SQL result -- a partial `avg` carries its state
    // there. The phase carrier is checked against this binding where the
    // output layout is, so there is nothing to compare here.
    Ok(AggregateBinding {
        function: BoundFunction {
            function_id: resolved.function_id.clone(),
            overload: resolved.selected.overload.clone(),
            kind: resolved.kind,
            argument_types: resolved
                .selected
                .argument_types
                .iter()
                .map(undecorated_argument)
                .collect(),
            result_type: undecorated(result_type),
            volatility: resolved.semantics.volatility,
            argument_evaluation: resolved.semantics.argument_evaluation,
            failure_behavior: resolved.semantics.failure_behavior,
        },
        phase,
        logical_argument_count: u32::try_from(resolved.logical_argument_count).map_err(|_| {
            ContractLoweringError::InvalidAggregate {
                detail: "logical argument count exceeds u32",
            }
        })?,
        intermediate_type: undecorated(&aggregate.intermediate_type),
        state_format: aggregate.state_format.clone(),
    })
}

/// Whether this aggregate's groups are complete when it is done with them.
///
/// A phase says so, and a `SELECT DISTINCT`-shaped aggregate has no call to
/// read a phase from -- so an aggregate that carries none answers from the
/// mode it was planned in, where a partial phase completes nothing.
/// Whether an aggregate in this mode emits each group finished.
///
/// This is the node's own fact, not its calls': a dedup phase reads a shuffle
/// on its keys and so finishes every group it emits, while still handing the
/// values on as state for the rollup above it to read.
const fn mode_completes_groups(mode: AggMode) -> bool {
    matches!(
        mode,
        AggMode::Single | AggMode::Global | AggMode::DistinctGlobal
    )
}

fn distribution_colocates(distribution: &Distribution, keys: &[ValueId]) -> bool {
    match distribution {
        Distribution::Singleton => true,
        Distribution::Hash {
            keys: distribution_keys,
            ..
        }
        | Distribution::BucketShuffle {
            keys: distribution_keys,
            ..
        } => distribution_keys.iter().all(|key| keys.contains(key)),
        Distribution::Unconstrained | Distribution::RoundRobin | Distribution::Broadcast => false,
    }
}

fn aggregate_output_distribution(input: &PhysicalProperties, output: &[ValueId]) -> Distribution {
    match &input.distribution {
        Distribution::Singleton => Distribution::Singleton,
        Distribution::Hash { keys, .. } | Distribution::BucketShuffle { keys, .. }
            if keys.iter().all(|key| output.contains(key)) =>
        {
            input.distribution.clone()
        }
        Distribution::Unconstrained
        | Distribution::RoundRobin
        | Distribution::Broadcast
        | Distribution::Hash { .. }
        | Distribution::BucketShuffle { .. } => Distribution::Unconstrained,
    }
}

/// Delegates to the contract so the rule has one definition while the
/// remaining node families move onto typed constructors.
fn passthrough_requirement(input: &PhysicalProperties) -> PhysicalProperties {
    novarocks_physical_plan::passthrough_requirement(input)
}

fn singleton_requirement() -> PhysicalProperties {
    PhysicalProperties {
        distribution: Distribution::Singleton,
        row_multiplicity: RowMultiplicity::SingleCopy,
        ordering: Box::default(),
    }
}

fn require_singleton_input(
    node: &'static str,
    properties: &PhysicalProperties,
) -> Result<(), ContractLoweringError> {
    require_single_copy_input(node, properties)?;
    if properties.distribution == Distribution::Singleton {
        Ok(())
    } else {
        Err(ContractLoweringError::PropertyRequirementUnsatisfied {
            node,
            detail: "requires singleton distribution",
        })
    }
}

fn require_single_copy_input(
    node: &'static str,
    properties: &PhysicalProperties,
) -> Result<(), ContractLoweringError> {
    if properties.row_multiplicity == RowMultiplicity::SingleCopy {
        Ok(())
    } else {
        Err(ContractLoweringError::PropertyRequirementUnsatisfied {
            node,
            detail: "requires single-copy row ownership",
        })
    }
}

fn non_negative_row_count(context: &'static str, value: i64) -> Result<u64, ContractLoweringError> {
    u64::try_from(value).map_err(|_| ContractLoweringError::InvalidRowCount {
        context,
        value,
        detail: "value is negative",
    })
}

fn identity_column_ref(expression: &TypedExpr) -> Option<ColumnId> {
    match &expression.kind {
        ExprKind::ColumnRef { column_id, .. } => Some(*column_id),
        ExprKind::Nested(inner) if expression_type(inner) == expression_type(expression) => {
            identity_column_ref(inner)
        }
        _ => None,
    }
}

fn lower_row_count_assertion(
    assertion: crate::planner::payload::PlanRowCountAssertion,
) -> RowCountAssertion {
    match assertion {
        crate::planner::payload::PlanRowCountAssertion::Eq => RowCountAssertion::Eq,
        crate::planner::payload::PlanRowCountAssertion::Ne => RowCountAssertion::Ne,
        crate::planner::payload::PlanRowCountAssertion::Lt => RowCountAssertion::Lt,
        crate::planner::payload::PlanRowCountAssertion::Le => RowCountAssertion::Le,
        crate::planner::payload::PlanRowCountAssertion::Gt => RowCountAssertion::Gt,
        crate::planner::payload::PlanRowCountAssertion::Ge => RowCountAssertion::Ge,
    }
}

/// The type a plan states for one column.
///
/// A plan's value type is a logical SQL type, so a provider's own decoration
/// on its nested fields -- Iceberg's Parquet field ids, its `element` naming --
/// does not travel in it. That decoration belongs to the provider's frozen
/// read, the way Trino keeps field ids on `IcebergColumnHandle` and out of
/// `io.trino.spi.type.Type`; a plan that repeated it would disagree with every
/// function signature the value is fed to. What a field admits is untouched:
/// nullability is a fact about the values, not decoration.
/// The same, for a write target's own column definition.
fn column_value_type(column: &novarocks_types::schema::ColumnDef) -> ValueType {
    ValueType::new(
        novarocks_types::undecorated_nested_type(&column.data_type),
        column.nullable,
    )
}

/// The widest this fragment may run.
///
/// A fragment that reads a relation the provider hands to one reader whole has
/// no split to spread, so it runs exactly one driver however wide the plan may
/// otherwise go. Everything else takes the plan's own domain.
fn fragment_dop_domain(
    builder: &FragmentBuilder,
    _root: NodeId,
    plan: PipelineDopDomain,
) -> PipelineDopDomain {
    if builder.reads_a_whole_relation() {
        PipelineDopDomain {
            min: 1,
            max: 1,
            requires_power_of_two: plan.requires_power_of_two,
        }
    } else {
        plan
    }
}

fn value_type(column: &OutputColumn) -> ValueType {
    ValueType::new(
        novarocks_types::undecorated_nested_type(&column.data_type),
        column.nullable,
    )
}

/// The same value type with the provider's decoration off its nested fields.
///
/// A binding's types are derived from the arguments it was resolved against,
/// so a nested argument carries the decoration into the result and the
/// intermediate state. See [`value_type`].
fn undecorated(ty: &ValueType) -> ValueType {
    ValueType::new(
        novarocks_types::undecorated_nested_type(&ty.data_type),
        ty.nullable,
    )
}

/// The same argument type with the provider's decoration off its nested
/// fields. See [`undecorated`].
fn undecorated_argument(argument: &FunctionArgumentType) -> FunctionArgumentType {
    match argument {
        FunctionArgumentType::Value(value) => FunctionArgumentType::Value(undecorated(value)),
        FunctionArgumentType::Lambda {
            parameter_types,
            result_type,
        } => FunctionArgumentType::Lambda {
            parameter_types: parameter_types.iter().map(undecorated).collect(),
            result_type: undecorated(result_type),
        },
    }
}

/// The type a plan states for one expression. See [`value_type`].
fn expression_type(expression: &TypedExpr) -> ValueType {
    ValueType::new(
        novarocks_types::undecorated_nested_type(&expression.data_type),
        expression.nullable,
    )
}

fn expect_children(plan: &PhysicalPlanNode, expected: usize) -> Result<(), ContractLoweringError> {
    if plan.children.len() == expected {
        Ok(())
    } else {
        Err(ContractLoweringError::ArityMismatch {
            context: physical_kind_name(&plan.kind),
            expected,
            actual: plan.children.len(),
        })
    }
}

/// Every column one node publishes stands in its own operator's layout.
///
/// Unlike an output shape, this does not fix an ordinal: a layout may carry
/// what the operator produces and never publishes.
fn require_outputs_within_layout(
    node: &'static str,
    outputs: &[OutputColumn],
    layout: &crate::planner::physical::AggregateOutputLayout,
) -> Result<(), ContractLoweringError> {
    let produced = layout
        .group_key_columns
        .iter()
        .chain(&layout.aggregate_columns)
        .map(|column| (column.column_id, column))
        .collect::<BTreeMap<_, _>>();
    for (ordinal, output) in outputs.iter().enumerate() {
        let Some(produced) = produced.get(&output.column_id) else {
            return Err(ContractLoweringError::OutputColumnMismatch {
                node,
                ordinal,
                detail: format!("{} is not produced by this operator", output.column_id),
            });
        };
        if produced.data_type != output.data_type || produced.nullable != output.nullable {
            return Err(ContractLoweringError::OutputColumnMismatch {
                node,
                ordinal,
                detail: format!(
                    "produced {:?} nullable={}, published {:?} nullable={}",
                    produced.data_type, produced.nullable, output.data_type, output.nullable
                ),
            });
        }
    }
    Ok(())
}

/// Checks a node that passes its child's rows through unchanged.
///
/// Every operator publishes the columns it declares, with one exception: an
/// aggregate emits its whole layout and narrows only where a projection above
/// it reads less, so a node standing between the two is written against the
/// layout rather than against the shorter list the aggregate publishes.
fn require_passthrough_shape(
    node: &'static str,
    outputs: &[OutputColumn],
    child: &PhysicalPlanNode,
) -> Result<(), ContractLoweringError> {
    let PhysicalPlanKind::HashAggregate(aggregate) = &child.kind else {
        return require_output_shape(node, outputs, &child.output_columns);
    };
    let produced = aggregate
        .output_layout
        .group_key_columns
        .iter()
        .chain(&aggregate.output_layout.aggregate_columns)
        .cloned()
        .collect::<Vec<_>>();
    require_output_shape(node, outputs, &produced)
}

/// The runtime filters this plan cannot state.
///
/// A filter prunes rows by a value that stands in the data flow, so a probe
/// written as an expression -- a join key one side had to convert -- has
/// nothing for the plan to name.
///
/// One filter also carries one type. Its artifact is built from the values
/// its producer sees and read against the values its consumers see, and the
/// contract binding those two ends names a single type, so a join whose two
/// keys are declared at different widths has no one type to state the filter
/// in. Sending the wider type would have the consumer read a column it does
/// not have.
///
/// A filter is an optimization: a plan that cannot state one states the query
/// without it, which is the same answer read from more rows. The decision is
/// made once, over the whole plan, because a filter's producer and its
/// consumers are lowered apart and have to agree.
fn unstatable_runtime_filters(plan: &PhysicalPlanNode) -> BTreeSet<i32> {
    let mut unstatable = BTreeSet::new();
    let mut built_types = BTreeMap::new();
    let mut probes: Vec<(i32, DataType)> = Vec::new();
    let mut pending = vec![plan];
    while let Some(node) = pending.pop() {
        for intent in &node.probe_runtime_filters {
            match identity_column_ref(&intent.probe_expr) {
                Some(_) => probes.push((
                    intent.filter_id,
                    novarocks_types::undecorated_nested_type(&intent.probe_expr.data_type),
                )),
                None => {
                    unstatable.insert(intent.filter_id);
                }
            }
        }
        // A join's filter is built and probed on the two halves of one
        // equality, and each half has to be a value of the filter's one type
        // for the plan to name it.
        if let PhysicalPlanKind::HashJoin(join) = &node.kind {
            for intent in &join.build_runtime_filters {
                let stated = join
                    .eq_conditions
                    .get(intent.expr_order)
                    .filter(|condition| {
                        identity_column_ref(&condition.left).is_some()
                            && identity_column_ref(&condition.right).is_some()
                    })
                    .and_then(|condition| {
                        let left =
                            novarocks_types::undecorated_nested_type(&condition.left.data_type);
                        let right =
                            novarocks_types::undecorated_nested_type(&condition.right.data_type);
                        (left == right).then_some(left)
                    });
                match stated {
                    Some(ty) => {
                        built_types.insert(intent.filter_id, ty);
                    }
                    None => {
                        unstatable.insert(intent.filter_id);
                    }
                }
            }
        }
        if let PhysicalPlanKind::HashAggregate(aggregate) = &node.kind {
            for intent in &aggregate.topn_runtime_filter_builds {
                built_types.insert(
                    intent.filter_id,
                    novarocks_types::undecorated_nested_type(&intent.group_key_expr.data_type),
                );
            }
        }
        pending.extend(node.children.iter());
    }
    for (filter_id, probe_type) in probes {
        if built_types
            .get(&filter_id)
            .is_some_and(|built| *built != probe_type)
        {
            unstatable.insert(filter_id);
        }
    }
    unstatable
}

/// The type a value takes when it publishes an expression's answer.
///
/// The declaration says what the statement published; the expression says
/// what it can answer. Where the expression may answer null the declaration
/// did not expect -- because something below it nulls a column the analyzer
/// had read as never null -- the value admits it too. A value that admitted
/// less would state something its own definition disproves.
fn published_value_type(declared: &ValueType, expression: &ValueType) -> ValueType {
    ValueType::new(
        declared.data_type.clone(),
        declared.nullable || expression.nullable,
    )
}

/// Whether an expression of this kind answers null wherever an operand does.
///
/// These are the kinds that answer about their operands' values. `IS NULL`,
/// `IS TRUE` and null-safe equality answer about the absence itself and are
/// total; a function decides for itself, which is why `coalesce` over a null
/// argument is not null; and a literal or a value says what it says.
const fn kind_follows_operand_nullability(kind: &ContractExprKind) -> bool {
    if let ContractExprKind::Binary { op, .. } = kind {
        return !matches!(op, BinaryOperator::EqForNull);
    }
    matches!(
        kind,
        ContractExprKind::Unary { .. }
            | ContractExprKind::Cast { .. }
            | ContractExprKind::InList { .. }
            | ContractExprKind::Between { .. }
            | ContractExprKind::Like { .. }
            | ContractExprKind::Case { .. }
            | ContractExprKind::Conjunction { .. }
            | ContractExprKind::Disjunction { .. }
    )
}

/// The expressions one of those kinds answers about.
fn collect_operand_expressions(kind: &ContractExprKind, operands: &mut Vec<ExprId>) {
    match kind {
        ContractExprKind::Unary { expr, .. } | ContractExprKind::Cast { expr, .. } => {
            operands.push(*expr);
        }
        ContractExprKind::Binary { left, right, .. } => {
            operands.push(*left);
            operands.push(*right);
        }
        ContractExprKind::InList { expr, list, .. } => {
            operands.push(*expr);
            operands.extend(list.iter().copied());
        }
        ContractExprKind::Between {
            expr, low, high, ..
        } => {
            operands.extend([*expr, *low, *high]);
        }
        ContractExprKind::Like { expr, pattern, .. } => {
            operands.push(*expr);
            operands.push(*pattern);
        }
        ContractExprKind::Conjunction { args } | ContractExprKind::Disjunction { args } => {
            operands.extend(args.iter().copied());
        }
        ContractExprKind::Case {
            operand,
            when_then,
            else_expr,
        } => {
            operands.extend(operand.iter().copied());
            operands.extend(when_then.iter().map(|(_, then)| *then));
            operands.extend(else_expr.iter().copied());
        }
        _ => {}
    }
}

/// Whether a node's payload names a subsequence of the columns it carries.
///
/// A node that passes its rows through carries whatever its input carried.
/// What the payload lists is what the statement reads above it, in the order
/// it reads them, which is why the two can differ in width but never in order
/// or identity.
fn require_published_subsequence(
    node: &'static str,
    carried: &[OutputColumn],
    published: &[OutputColumn],
) -> Result<(), ContractLoweringError> {
    let mut carried = carried.iter();
    for (ordinal, column) in published.iter().enumerate() {
        let found = carried.any(|candidate| {
            candidate.column_id == column.column_id
                && candidate.data_type == column.data_type
                && candidate.nullable == column.nullable
        });
        if !found {
            return Err(ContractLoweringError::OutputColumnMismatch {
                node,
                ordinal,
                detail: format!(
                    "published column {} {:?} nullable={} is not carried in that order",
                    column.column_id, column.data_type, column.nullable
                ),
            });
        }
    }
    Ok(())
}

fn require_output_shape(
    node: &'static str,
    actual: &[OutputColumn],
    expected: &[OutputColumn],
) -> Result<(), ContractLoweringError> {
    if actual.len() != expected.len() {
        return Err(ContractLoweringError::ArityMismatch {
            context: node,
            expected: expected.len(),
            actual: actual.len(),
        });
    }
    for (ordinal, (actual, expected)) in actual.iter().zip(expected).enumerate() {
        if actual.column_id != expected.column_id
            || actual.data_type != expected.data_type
            || actual.nullable != expected.nullable
        {
            return Err(ContractLoweringError::OutputColumnMismatch {
                node,
                ordinal,
                detail: format!(
                    "expected {} {:?} nullable={}, got {} {:?} nullable={}",
                    expected.column_id,
                    expected.data_type,
                    expected.nullable,
                    actual.column_id,
                    actual.data_type,
                    actual.nullable
                ),
            });
        }
    }
    Ok(())
}

fn checked_ordinal(context: &'static str, ordinal: usize) -> Result<u32, ContractLoweringError> {
    u32::try_from(ordinal).map_err(|_| ContractLoweringError::OrdinalOverflow { context, ordinal })
}

/// Maps the non-connective binary operators. `AND`/`OR` never reach here:
/// they lower to n-ary connectives through `lower_boolean_connective`.
/// Whether this operator answers about two values of one type.
///
/// A comparison does; arithmetic derives its result from the pair it is given
/// and a boolean connective takes booleans only.
const fn is_comparison_operator(op: BinOp) -> bool {
    matches!(
        op,
        BinOp::Eq | BinOp::Ne | BinOp::Lt | BinOp::Le | BinOp::Gt | BinOp::Ge | BinOp::EqForNull
    )
}

fn lower_binary_operator(operator: BinOp) -> BinaryOperator {
    match operator {
        BinOp::And | BinOp::Or => {
            unreachable!("boolean connectives lower to n-ary Conjunction/Disjunction")
        }
        BinOp::Add => BinaryOperator::Add,
        BinOp::Sub => BinaryOperator::Subtract,
        BinOp::Mul => BinaryOperator::Multiply,
        BinOp::Div => BinaryOperator::Divide,
        BinOp::Mod => BinaryOperator::Modulo,
        BinOp::Eq => BinaryOperator::Eq,
        BinOp::Ne => BinaryOperator::NotEq,
        BinOp::Lt => BinaryOperator::Lt,
        BinOp::Le => BinaryOperator::LtEq,
        BinOp::Gt => BinaryOperator::Gt,
        BinOp::Ge => BinaryOperator::GtEq,
        BinOp::EqForNull => BinaryOperator::EqForNull,
    }
}

fn lower_unary_operator(operator: UnOp) -> UnaryOperator {
    match operator {
        UnOp::Not => UnaryOperator::Not,
        UnOp::Negate => UnaryOperator::Minus,
        UnOp::BitwiseNot => UnaryOperator::BitwiseNot,
    }
}

fn lower_literal(
    literal: &LiteralValue,
    target: &ValueType,
) -> Result<(ContractLiteralValue, ValueType), ContractLoweringError> {
    match literal {
        // A null literal admits null because it is one. Where the statement
        // was analyzed to say the slot it fills never carries one, the
        // annotation is what is wrong, and everything above this reads the
        // expression rather than the annotation.
        LiteralValue::Null => Ok((
            ContractLiteralValue::Null,
            ValueType::new(target.data_type.clone(), true),
        )),
        LiteralValue::Bool(value) => Ok((
            ContractLiteralValue::Boolean(*value),
            ValueType::new(DataType::Boolean, false),
        )),
        // A date, a time and a timestamp all reach the planner as the integer
        // they are stored as, and only the position says which. Reading the
        // position is what makes them that value: an integer converted to a
        // date instead reads its digits as `YYYYMMDD`, so `DATE '2024-01-10'`
        // would arrive as a null.
        LiteralValue::Int(value) if matches!(target.data_type, DataType::Date32) => {
            let days =
                i32::try_from(*value).map_err(|_| ContractLoweringError::InvalidLiteral {
                    kind: "Date32",
                    detail: format!("{value} is outside the day range a date is stored in"),
                })?;
            Ok((
                ContractLiteralValue::Date32(days),
                ValueType::new(DataType::Date32, false),
            ))
        }
        LiteralValue::Int(value) if matches!(target.data_type, DataType::Time64(_)) => Ok((
            ContractLiteralValue::Time64(*value),
            ValueType::new(target.data_type.clone(), false),
        )),
        LiteralValue::Int(value) if matches!(target.data_type, DataType::Timestamp(_, _)) => Ok((
            ContractLiteralValue::Timestamp(*value),
            ValueType::new(target.data_type.clone(), false),
        )),
        LiteralValue::Int(value) => Ok((
            ContractLiteralValue::Int64(*value),
            ValueType::new(DataType::Int64, false),
        )),
        LiteralValue::LargeInt(value)
            if novarocks_type_contract::is_largeint_data_type(&target.data_type) =>
        {
            Ok((
                ContractLiteralValue::LargeInt(*value),
                ValueType::new(target.data_type.clone(), false),
            ))
        }
        LiteralValue::LargeInt(_) => Err(ContractLoweringError::InvalidLiteral {
            kind: "LargeInt",
            detail: format!(
                "requires FixedSizeBinary({}), got {:?}",
                novarocks_type_contract::LARGEINT_BYTE_WIDTH,
                target.data_type,
            ),
        }),
        LiteralValue::Float(value) => Ok((
            ContractLiteralValue::Float64Bits(value.to_bits()),
            ValueType::new(DataType::Float64, false),
        )),
        LiteralValue::String(value) => Ok((
            ContractLiteralValue::Utf8(value.clone().into_boxed_str()),
            ValueType::new(DataType::Utf8, false),
        )),
        LiteralValue::Binary(value) => Ok((
            ContractLiteralValue::Binary(value.clone().into_boxed_slice()),
            ValueType::new(DataType::Binary, false),
        )),
        LiteralValue::Decimal(value) => match &target.data_type {
            DataType::Decimal128(precision, scale) => {
                let unscaled = parse_decimal128(value, *scale)?;
                require_decimal_precision(unscaled, *precision, "Decimal")?;
                Ok((
                    ContractLiteralValue::Decimal128(unscaled),
                    ValueType::new(target.data_type.clone(), false),
                ))
            }
            DataType::Decimal256(precision, scale) => {
                let unscaled = parse_decimal256(value, *scale)?;
                require_decimal256_precision(unscaled, *precision)?;
                Ok((
                    ContractLiteralValue::Decimal256(unscaled.to_be_bytes()),
                    ValueType::new(target.data_type.clone(), false),
                ))
            }
            other => Err(ContractLoweringError::InvalidLiteral {
                kind: "Decimal",
                detail: format!("requires a decimal type, got {other:?}"),
            }),
        },
    }
}

fn require_decimal_precision(
    unscaled: i128,
    precision: u8,
    kind: &'static str,
) -> Result<(), ContractLoweringError> {
    let digits = if unscaled == 0 {
        1
    } else {
        unscaled.unsigned_abs().ilog10() + 1
    };
    if digits <= u32::from(precision) {
        Ok(())
    } else {
        Err(ContractLoweringError::InvalidLiteral {
            kind,
            detail: format!(
                "unscaled value requires {digits} digits, exceeding precision {precision}"
            ),
        })
    }
}

/// One display name, cut to what an annotation carries.
///
/// A display name is what EXPLAIN calls a column, and SQL puts no bound on
/// it: a generated name spells out the expression that produced it, which a
/// long literal makes arbitrarily long. The name a reader wants is at the
/// front, so the rest is elided rather than refused -- the result set takes
/// its own column names from the output layout and is unaffected.
fn bounded_display_name(name: &str) -> Box<str> {
    const ELISION: &str = "...";
    let budget = novarocks_physical_plan::MAX_ANNOTATION_VALUE_BYTES;
    if name.len() <= budget {
        return name.into();
    }
    let mut kept = budget - ELISION.len();
    while kept > 0 && !name.is_char_boundary(kept) {
        kept -= 1;
    }
    format!("{}{ELISION}", &name[..kept]).into_boxed_str()
}

fn require_decimal256_precision(
    unscaled: arrow::datatypes::i256,
    precision: u8,
) -> Result<(), ContractLoweringError> {
    // A 256-bit value has no cheap base-ten logarithm, and its digits are what
    // its decimal spelling says they are.
    let digits = unscaled
        .to_string()
        .trim_start_matches('-')
        .trim_start_matches('0')
        .len()
        .max(1);
    if digits <= usize::from(precision) {
        Ok(())
    } else {
        Err(ContractLoweringError::InvalidLiteral {
            kind: "Decimal",
            detail: format!(
                "unscaled value requires {digits} digits, exceeding precision {precision}"
            ),
        })
    }
}

/// Reads one decimal literal as the unscaled 256-bit value it stands for.
///
/// Same reading as the 128-bit case beside it, in the only integer wide enough
/// to hold it: the digits are shifted to the declared scale, and a shift that
/// would drop a digit is refused rather than rounded.
fn parse_decimal256(
    value: &str,
    scale: i8,
) -> Result<arrow::datatypes::i256, ContractLoweringError> {
    use arrow::datatypes::i256;

    let invalid = |detail: String| ContractLoweringError::InvalidLiteral {
        kind: "Decimal",
        detail,
    };
    let (negative, unsigned) = match value.strip_prefix('-') {
        Some(unsigned) => (true, unsigned),
        None => (false, value.strip_prefix('+').unwrap_or(value)),
    };
    let mut parts = unsigned.split('.');
    let integer = parts.next().unwrap_or_default();
    let fraction = parts.next().unwrap_or_default();
    if parts.next().is_some()
        || (integer.is_empty() && fraction.is_empty())
        || !integer.bytes().all(|byte| byte.is_ascii_digit())
        || !fraction.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err(invalid(format!("{value:?} is not a plain decimal literal")));
    }
    let digits = format!("{integer}{fraction}");
    let signed = if negative {
        format!("-{digits}")
    } else {
        digits
    };
    let mut unscaled = i256::from_string(&signed)
        .ok_or_else(|| invalid(format!("{value:?} exceeds Decimal256")))?;
    let fraction_digits = i32::try_from(fraction.len())
        .map_err(|_| invalid(format!("{value:?} has too many fractional digits")))?;
    let adjustment = i32::from(scale) - fraction_digits;
    let ten = i256::from_i128(10);
    let power = |exponent: u32| -> Result<i256, ContractLoweringError> {
        let mut power = i256::ONE;
        for _ in 0..exponent {
            power = power.checked_mul(ten).ok_or_else(|| {
                invalid(format!("{value:?} cannot be represented at scale {scale}"))
            })?;
        }
        Ok(power)
    };
    if adjustment >= 0 {
        unscaled = unscaled
            .checked_mul(power(adjustment.unsigned_abs())?)
            .ok_or_else(|| invalid(format!("{value:?} exceeds Decimal256 at scale {scale}")))?;
    } else {
        let power = power(adjustment.unsigned_abs())?;
        if unscaled.checked_rem(power) != Some(i256::ZERO) {
            return Err(invalid(format!(
                "{value:?} loses precision at scale {scale}"
            )));
        }
        unscaled = unscaled
            .checked_div(power)
            .ok_or_else(|| invalid(format!("{value:?} cannot be represented at scale {scale}")))?;
    }
    Ok(unscaled)
}

fn parse_decimal128(value: &str, scale: i8) -> Result<i128, ContractLoweringError> {
    let (negative, unsigned) = match value.strip_prefix('-') {
        Some(unsigned) => (true, unsigned),
        None => (false, value.strip_prefix('+').unwrap_or(value)),
    };
    let mut parts = unsigned.split('.');
    let integer = parts.next().unwrap_or_default();
    let fraction = parts.next().unwrap_or_default();
    if parts.next().is_some()
        || (integer.is_empty() && fraction.is_empty())
        || !integer.bytes().all(|byte| byte.is_ascii_digit())
        || !fraction.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err(ContractLoweringError::InvalidLiteral {
            kind: "Decimal",
            detail: format!("{value:?} is not a plain decimal literal"),
        });
    }

    let digits = format!("{integer}{fraction}");
    let signed = if negative {
        format!("-{digits}")
    } else {
        digits
    };
    let mut unscaled =
        signed
            .parse::<i128>()
            .map_err(|error| ContractLoweringError::InvalidLiteral {
                kind: "Decimal",
                detail: format!("{value:?} exceeds Decimal128: {error}"),
            })?;
    let fraction_digits =
        i32::try_from(fraction.len()).map_err(|_| ContractLoweringError::InvalidLiteral {
            kind: "Decimal",
            detail: format!("{value:?} has too many fractional digits"),
        })?;
    let adjustment = i32::from(scale) - fraction_digits;
    if adjustment >= 0 {
        let power = 10_i128.checked_pow(adjustment as u32).ok_or_else(|| {
            ContractLoweringError::InvalidLiteral {
                kind: "Decimal",
                detail: format!("{value:?} cannot be represented at scale {scale}"),
            }
        })?;
        unscaled =
            unscaled
                .checked_mul(power)
                .ok_or_else(|| ContractLoweringError::InvalidLiteral {
                    kind: "Decimal",
                    detail: format!("{value:?} exceeds Decimal128 at scale {scale}"),
                })?;
    } else {
        let power = 10_i128
            .checked_pow(adjustment.unsigned_abs())
            .ok_or_else(|| ContractLoweringError::InvalidLiteral {
                kind: "Decimal",
                detail: format!("{value:?} cannot be represented at scale {scale}"),
            })?;
        if unscaled % power != 0 {
            return Err(ContractLoweringError::InvalidLiteral {
                kind: "Decimal",
                detail: format!("{value:?} loses precision at scale {scale}"),
            });
        }
        unscaled /= power;
    }
    Ok(unscaled)
}

fn physical_kind_name(kind: &PhysicalPlanKind) -> &'static str {
    match kind {
        PhysicalPlanKind::Scan(_) => "Scan",
        PhysicalPlanKind::Filter(_) => "Filter",
        PhysicalPlanKind::Project(_) => "Project",
        PhysicalPlanKind::Unpivot(_) => "Unpivot",
        PhysicalPlanKind::Sort(_) => "Sort",
        PhysicalPlanKind::Limit(_) => "Limit",
        PhysicalPlanKind::Values(_) => "Values",
        PhysicalPlanKind::Repeat(_) => "Repeat",
        PhysicalPlanKind::Window(_) => "Window",
        PhysicalPlanKind::GenerateSeries(_) => "GenerateSeries",
        PhysicalPlanKind::TableFunction(_) => "TableFunction",
        PhysicalPlanKind::AssertOneRow(_) => "AssertOneRow",
        PhysicalPlanKind::TopN(_) => "TopN",
        PhysicalPlanKind::HashAggregate(_) => "HashAggregate",
        PhysicalPlanKind::HashJoin(_) => "HashJoin",
        PhysicalPlanKind::NestLoopJoin(_) => "NestLoopJoin",
        PhysicalPlanKind::SetOp(_) => "SetOp",
        PhysicalPlanKind::ChangeEventExpand(_) => "ChangeEventExpand",
        PhysicalPlanKind::CTEAnchor(_) => "CTEAnchor",
        PhysicalPlanKind::CTEProduce(_) => "CTEProduce",
        PhysicalPlanKind::CTEConsume(_) => "CTEConsume",
        PhysicalPlanKind::Redistribute(_) => "Redistribute",
    }
}

fn expression_kind_name(kind: &ExprKind) -> &'static str {
    match kind {
        ExprKind::ColumnRef { .. } => "ColumnRef",
        ExprKind::LambdaParamRef { .. } => "LambdaParamRef",
        ExprKind::Literal(_) => "Literal",
        ExprKind::BinaryOp { .. } => "BinaryOp",
        ExprKind::UnaryOp { .. } => "UnaryOp",
        ExprKind::FunctionCall { .. } => "FunctionCall",
        ExprKind::LambdaFunction { .. } => "LambdaFunction",
        ExprKind::AggregateCall { .. } => "AggregateCall",
        ExprKind::Cast { .. } => "Cast",
        ExprKind::IsNull { .. } => "IsNull",
        ExprKind::InList { .. } => "InList",
        ExprKind::Between { .. } => "Between",
        ExprKind::Like { .. } => "Like",
        ExprKind::Case { .. } => "Case",
        ExprKind::IsTruthValue { .. } => "IsTruthValue",
        ExprKind::Nested(_) => "Nested",
        ExprKind::WindowCall { .. } => "WindowCall",
        ExprKind::SubqueryPlaceholder { .. } => "SubqueryPlaceholder",
        ExprKind::Lambda { .. } => "Lambda",
    }
}

fn metadata_relation_kind(
    kind: crate::planner::table::SqlMetadataTableKind,
) -> Result<MetadataRelationKind, ContractLoweringError> {
    let name = match kind {
        crate::planner::table::SqlMetadataTableKind::Snapshots => "iceberg.snapshots",
        crate::planner::table::SqlMetadataTableKind::History => "iceberg.history",
        crate::planner::table::SqlMetadataTableKind::Refs => "iceberg.refs",
        crate::planner::table::SqlMetadataTableKind::Files => "iceberg.files",
        crate::planner::table::SqlMetadataTableKind::Manifests => "iceberg.manifests",
        crate::planner::table::SqlMetadataTableKind::Partitions => "iceberg.partitions",
        crate::planner::table::SqlMetadataTableKind::Entries => "iceberg.entries",
    };
    MetadataRelationKind::try_new(name).map_err(|error| ContractLoweringError::ProviderRead {
        detail: error.to_string(),
    })
}

#[derive(Debug)]
pub(crate) enum ContractLoweringError {
    IdentitySpaceExhausted(&'static str),
    InvalidPlanIdentity {
        detail: String,
    },
    UnexpectedFragment {
        node: &'static str,
        expected: FragmentId,
        actual: FragmentId,
    },
    EmptyPartitionKeys {
        node: &'static str,
    },
    InvalidPartitionExpressions {
        node: &'static str,
        detail: &'static str,
    },
    MissingPlannerFact {
        node: &'static str,
        fact: &'static str,
    },
    InvalidJoin {
        node: &'static str,
        detail: &'static str,
    },
    InvalidJoinKeys {
        node: &'static str,
        detail: String,
    },
    JoinPredicateIsNotBoolean {
        node: &'static str,
        actual: DataType,
    },
    AmbiguousInputColumn {
        node: &'static str,
        column: ColumnId,
    },
    UnsupportedSetOp {
        detail: &'static str,
    },
    InvalidSetOp {
        detail: &'static str,
    },
    InvalidAggregate {
        detail: &'static str,
    },
    InvalidUnpivot {
        detail: String,
    },
    InvalidRepeat {
        detail: String,
    },
    InvalidWindow {
        detail: String,
    },
    InvalidTableFunction {
        detail: String,
    },
    InvalidCte {
        detail: String,
    },
    InvalidWrite {
        detail: String,
    },
    DuplicateFragmentCompletion {
        fragment: FragmentId,
    },
    IncompleteFragment {
        fragment: FragmentId,
    },
    UnknownFragmentCompletion {
        fragment: FragmentId,
    },
    UnsupportedNode {
        kind: &'static str,
    },
    UnsupportedExpression {
        kind: &'static str,
    },
    InvalidLambda {
        detail: String,
    },
    UnsupportedRuntimeFilters {
        node: &'static str,
        count: usize,
    },
    InvalidRuntimeFilter {
        id: i32,
        detail: String,
    },
    UnsupportedSortMode {
        detail: &'static str,
    },
    UnsupportedTopNShape {
        detail: &'static str,
    },
    ArityMismatch {
        context: &'static str,
        expected: usize,
        actual: usize,
    },
    OutputColumnMismatch {
        node: &'static str,
        ordinal: usize,
        detail: String,
    },
    DuplicateColumnDefinition {
        node: &'static str,
        column: ColumnId,
    },
    UnknownColumnReference(ColumnId),
    PredicateIsNotBoolean {
        actual: DataType,
    },
    ExpressionTypeMismatch {
        context: String,
        expected: ValueType,
        actual: ValueType,
    },
    InvalidLiteral {
        kind: &'static str,
        detail: String,
    },
    InvalidFunctionBinding {
        detail: String,
    },
    ProviderRead {
        detail: String,
    },
    InvalidAssertion {
        detail: &'static str,
    },
    InvalidGenerateSeries {
        detail: &'static str,
    },
    InvalidRowCount {
        context: &'static str,
        value: i64,
        detail: &'static str,
    },
    RowCountOverflow {
        node: &'static str,
    },
    EmptyOrdering {
        node: &'static str,
    },
    OrderingExpressionIsNotValue {
        ordinal: usize,
    },
    PropertyRequirementUnsatisfied {
        node: &'static str,
        detail: &'static str,
    },
    OrdinalOverflow {
        context: &'static str,
        ordinal: usize,
    },
    Build(BuildError),
    Validation(ValidationErrors),
}

impl fmt::Display for ContractLoweringError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::IdentitySpaceExhausted(kind) => {
                write!(formatter, "{kind} identity space exhausted")
            }
            Self::InvalidPlanIdentity { detail } => {
                write!(formatter, "invalid plan-local identity: {detail}")
            }
            Self::UnexpectedFragment {
                node,
                expected,
                actual,
            } => write!(
                formatter,
                "{node} expected fragment {}, got fragment {}",
                expected.get(),
                actual.get()
            ),
            Self::EmptyPartitionKeys { node } => {
                write!(formatter, "{node} has no partition keys")
            }
            Self::InvalidPartitionExpressions { node, detail } => {
                write!(formatter, "invalid {node} partition expressions: {detail}")
            }
            Self::MissingPlannerFact { node, fact } => {
                write!(formatter, "{node} lacks planner fact: {fact}")
            }
            Self::InvalidJoin { node, detail } => write!(formatter, "invalid {node}: {detail}"),
            Self::InvalidJoinKeys { node, detail } => {
                write!(formatter, "invalid {node}: {detail}")
            }
            Self::JoinPredicateIsNotBoolean { node, actual } => {
                write!(
                    formatter,
                    "{node} predicate has non-Boolean type {actual:?}"
                )
            }
            Self::AmbiguousInputColumn { node, column } => {
                write!(formatter, "{node} input column {column} is ambiguous")
            }
            Self::UnsupportedSetOp { detail } => {
                write!(formatter, "unsupported final SetOp: {detail}")
            }
            Self::InvalidSetOp { detail } => write!(formatter, "invalid SetOp: {detail}"),
            Self::InvalidAggregate { detail } => {
                write!(formatter, "invalid HashAggregate: {detail}")
            }
            Self::InvalidUnpivot { detail } => write!(formatter, "invalid Unpivot: {detail}"),
            Self::InvalidRepeat { detail } => write!(formatter, "invalid Repeat: {detail}"),
            Self::InvalidWindow { detail } => write!(formatter, "invalid Window: {detail}"),
            Self::InvalidTableFunction { detail } => {
                write!(formatter, "invalid TableFunction: {detail}")
            }
            Self::InvalidCte { detail } => write!(formatter, "invalid CTE: {detail}"),
            Self::InvalidWrite { detail } => write!(formatter, "invalid final write: {detail}"),
            Self::DuplicateFragmentCompletion { fragment } => write!(
                formatter,
                "fragment {} has more than one root/sink completion",
                fragment.get()
            ),
            Self::IncompleteFragment { fragment } => write!(
                formatter,
                "fragment {} has no root/sink completion",
                fragment.get()
            ),
            Self::UnknownFragmentCompletion { fragment } => write!(
                formatter,
                "completion references unknown fragment {}",
                fragment.get()
            ),
            Self::UnsupportedNode { kind } => {
                write!(
                    formatter,
                    "final physical-plan lowering does not support {kind}"
                )
            }
            Self::InvalidLambda { detail } => write!(formatter, "invalid lambda: {detail}"),
            Self::UnsupportedExpression { kind } => write!(
                formatter,
                "final physical-plan lowering does not support expression {kind}"
            ),
            Self::UnsupportedRuntimeFilters { node, count } => write!(
                formatter,
                "final physical-plan lowering does not yet bind {count} runtime filter(s) on {node}"
            ),
            Self::InvalidRuntimeFilter { id, detail } => {
                write!(formatter, "runtime filter {id} is invalid: {detail}")
            }
            Self::UnsupportedSortMode { detail } => {
                write!(formatter, "unsupported final Sort mode: {detail}")
            }
            Self::UnsupportedTopNShape { detail } => {
                write!(formatter, "unsupported final TopN shape: {detail}")
            }
            Self::ArityMismatch {
                context,
                expected,
                actual,
            } => write!(
                formatter,
                "{context} expected {expected} item(s), got {actual}"
            ),
            Self::OutputColumnMismatch {
                node,
                ordinal,
                detail,
            } => write!(
                formatter,
                "{node} output column {ordinal} is inconsistent: {detail}"
            ),
            Self::DuplicateColumnDefinition { node, column } => {
                write!(formatter, "{node} defines column {column} more than once")
            }
            Self::UnknownColumnReference(column) => {
                write!(formatter, "expression references invisible column {column}")
            }
            Self::PredicateIsNotBoolean { actual } => {
                write!(
                    formatter,
                    "Filter predicate has non-Boolean type {actual:?}"
                )
            }
            Self::ExpressionTypeMismatch {
                context,
                expected,
                actual,
            } => write!(
                formatter,
                "{context} expected expression type {expected:?}, got {actual:?}"
            ),
            Self::InvalidLiteral { kind, detail } => {
                write!(formatter, "invalid {kind} literal: {detail}")
            }
            Self::InvalidFunctionBinding { detail } => {
                write!(formatter, "invalid exact function binding: {detail}")
            }
            Self::ProviderRead { detail } => {
                write!(formatter, "invalid finalized provider read: {detail}")
            }
            Self::InvalidAssertion { detail } => {
                write!(formatter, "invalid row-count assertion: {detail}")
            }
            Self::InvalidGenerateSeries { detail } => {
                write!(formatter, "invalid GenerateSeries: {detail}")
            }
            Self::InvalidRowCount {
                context,
                value,
                detail,
            } => write!(formatter, "invalid {context} {value}: {detail}"),
            Self::RowCountOverflow { node } => {
                write!(formatter, "{node} limit plus offset exceeds u64")
            }
            Self::EmptyOrdering { node } => write!(formatter, "{node} has no ordering keys"),
            Self::OrderingExpressionIsNotValue { ordinal } => write!(
                formatter,
                "ordering expression {ordinal} is not a materialized value"
            ),
            Self::PropertyRequirementUnsatisfied { node, detail } => {
                write!(
                    formatter,
                    "{node} input property proof is incomplete: {detail}"
                )
            }
            Self::OrdinalOverflow { context, ordinal } => {
                write!(formatter, "{context} ordinal {ordinal} exceeds u32")
            }
            Self::Build(error) => write!(formatter, "final physical-plan build failed: {error}"),
            Self::Validation(error) => {
                write!(formatter, "final physical-plan validation failed: {error}")
            }
        }
    }
}

impl std::error::Error for ContractLoweringError {}

impl From<BuildError> for ContractLoweringError {
    fn from(error: BuildError) -> Self {
        Self::Build(error)
    }
}

impl From<ValidationErrors> for ContractLoweringError {
    fn from(error: ValidationErrors) -> Self {
        Self::Validation(error)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use novarocks_physical_plan::{
        ArtifactFormat, ArtifactFormatId, ArtifactInputRequirement, ArtifactKind,
        ArtifactSourceBinding, CoverageRange, CoverageSet, ExactInputVersion, NodeKind,
        PhysicalPlan, ProviderColumnReference, ProviderReadReference, ValueOrigin,
    };
    use novarocks_spi::connector::read_stack::{
        ConnectorReadBinding, ConnectorReadRelationKind, ConnectorReadWorkSource,
        ConnectorValueType,
    };
    use novarocks_spi::connector::{
        CatalogHandle, CatalogVersion, ConnectorCodecCategory, ConnectorCodecRevision,
        ConnectorEncodedPayload, ConnectorEnvelopeHeader, ConnectorInstanceDescriptor,
        ConnectorInstanceId, ConnectorProviderId, ConnectorReadRelationPayload,
    };
    use novarocks_type_contract::{BucketLayoutAlgorithm, PartitionHashAlgorithm};
    use novarocks_types::naming::TableIdentity;

    use super::*;
    use crate::analysis::{ProjectItem, SortItem};
    use crate::compiler::{
        ProviderReadColumnFact, ProviderReadColumnNeed, ProviderReadDistribution,
        ProviderReadLimitFact, ProviderReadNeed, ProviderReadOrderingKey, ProviderReadProperties,
        ProviderReadRequestBinding, ProviderReadStaticContract, ProviderReadVersionNeed,
    };
    use crate::planner::payload::{
        PlanAssertOneRowNode, PlanCTEAnchorNode, PlanCTEConsumeNode, PlanCTEProduceNode,
        PlanFilterNode, PlanGenerateSeriesNode, PlanLimitNode, PlanProjectNode, PlanRepeatNode,
        PlanRowCountAssertion, PlanSortNode, PlanTableFunctionNode, PlanUnpivotNode,
        PlanUnpivotPassthroughColumn, PlanUnpivotValueMapping, PlanValuesNode, PlanWindowNode,
        WindowExpr,
    };
    use crate::planner::physical::DistributedChangeEventExpandNode;
    use crate::planner::physical::{
        PhysicalPlanStats, PhysicalTopNNode, PlannerConfidence, PlannerCostEstimate,
    };

    fn version() -> PlanVersionId {
        PlanVersionId::try_new([41; 16]).unwrap()
    }

    fn dop() -> PipelineDopDomain {
        PipelineDopDomain {
            min: 1,
            max: 8,
            requires_power_of_two: true,
        }
    }

    #[test]
    fn mv_rewrite_annotation_uses_exact_publication_identity() {
        let publication = crate::compiler::SqlMvRewriteSelectionFacts::try_new(
            [7; 16],
            [9; 32],
            vec!["ice.db.orders".to_string()],
        )
        .unwrap();
        let selection = crate::planner::payload::MvRewriteSelection::selected(
            "display_name_is_not_identity".to_string(),
            [7; 16],
            [9; 32],
            publication.definition_revision(),
            publication.interpretation_revision(),
            std::sync::Arc::from(publication.publication_provenance()),
            Vec::new(),
            publication.publication_inputs().to_vec(),
            publication.publication_target().clone(),
        );

        let annotation = mv_rewrite_provenance_annotation(&selection).unwrap();
        assert!(annotation.starts_with(concat!(
            "v2;publication_id=07070707070707070707070707070707;",
            "definition_fingerprint=",
            "0909090909090909090909090909090909090909090909090909090909090909;",
            "publication_state_digest="
        )));
        let state_digest = annotation
            .strip_prefix(concat!(
                "v2;publication_id=07070707070707070707070707070707;",
                "definition_fingerprint=",
                "0909090909090909090909090909090909090909090909090909090909090909;",
                "publication_state_digest="
            ))
            .unwrap();
        assert_eq!(state_digest.len(), 64);
        assert!(state_digest.bytes().all(|byte| byte.is_ascii_hexdigit()));
        assert!(!annotation.contains("display_name_is_not_identity"));

        let other_publication = crate::compiler::SqlMvRewriteSelectionFacts::try_new(
            [7; 16],
            [9; 32],
            vec!["ice.db.other_orders".to_string()],
        )
        .unwrap();
        let other_selection = crate::planner::payload::MvRewriteSelection::selected(
            "display_name_is_not_identity".to_string(),
            [7; 16],
            [9; 32],
            other_publication.definition_revision(),
            other_publication.interpretation_revision(),
            std::sync::Arc::from(other_publication.publication_provenance()),
            Vec::new(),
            other_publication.publication_inputs().to_vec(),
            other_publication.publication_target().clone(),
        );
        assert_ne!(
            annotation,
            mv_rewrite_provenance_annotation(&other_selection).unwrap()
        );
    }

    #[test]
    fn mv_rewrite_provenance_binds_document_revisions_occurrence_ids_and_order() {
        let publication = crate::compiler::SqlMvRewriteSelectionFacts::try_new(
            [7; 16],
            [9; 32],
            vec!["ice.db.orders".to_string()],
        )
        .unwrap();
        let input = |id| {
            crate::compiler::SqlMvRewritePublicationInput::try_new(
                crate::compiler::SqlMvRelationOccurrenceId::new(id),
                publication.publication_inputs()[0].relation().clone(),
            )
            .unwrap()
        };
        let digest = |definition, interpretation, inputs| {
            let selection = crate::planner::payload::MvRewriteSelection::selected(
                "display".to_string(),
                [7; 16],
                [9; 32],
                definition,
                interpretation,
                std::sync::Arc::from(publication.publication_provenance()),
                Vec::new(),
                inputs,
                publication.publication_target().clone(),
            );
            mv_rewrite_publication_state_digest(&selection).unwrap()
        };
        let expected = digest([11; 32], [12; 32], vec![input(7), input(42)]);
        assert_ne!(
            expected,
            digest([13; 32], [12; 32], vec![input(7), input(42)])
        );
        assert_ne!(
            expected,
            digest([11; 32], [13; 32], vec![input(7), input(42)])
        );
        assert_ne!(
            expected,
            digest([11; 32], [12; 32], vec![input(42), input(7)])
        );
        assert_ne!(
            expected,
            digest([11; 32], [12; 32], vec![input(0), input(1)])
        );
    }

    #[test]
    fn mv_rewrite_annotation_rejects_unverified_name_only_marker() {
        let selection =
            crate::planner::payload::MvRewriteSelection::unverified("name_only".to_string());

        assert!(matches!(
            mv_rewrite_provenance_annotation(&selection),
            Err(ContractLoweringError::MissingPlannerFact {
                node: "Scan",
                fact: "MV rewrite publication identity",
            })
        ));
    }

    fn provider_hash_scheme(seed: u8) -> ProviderHashPartitionScheme {
        ProviderHashPartitionScheme {
            space: crate::compiler::ProviderPartitionSpaceToken::from_bytes([seed; 32]),
            count: crate::compiler::ProviderPartitionCountToken::from_bytes(
                [seed.wrapping_add(1); 32],
            ),
            admissible: crate::compiler::ProviderPartitionCountDomain {
                min: 1,
                max: 8,
                requires_power_of_two: true,
            },
            algorithm: PartitionHashAlgorithm::NativeExchangeV1,
        }
    }

    fn provider_bucket_scheme(seed: u8) -> ProviderBucketPartitionScheme {
        ProviderBucketPartitionScheme {
            space: crate::compiler::ProviderPartitionSpaceToken::from_bytes([seed; 32]),
            bucket_count: 16,
            hash: PartitionHashAlgorithm::NativeBucketCrc32V1,
            layout: BucketLayoutAlgorithm::DenseZeroBasedV1,
            ordinal_domain: crate::compiler::ProviderBucketOrdinalDomainProof {
                first_ordinal: 0,
                ordinal_count: 16,
                evidence_digest: [seed.wrapping_add(1); 32],
            },
        }
    }

    fn column(id: u32, name: &str, data_type: DataType, nullable: bool) -> OutputColumn {
        OutputColumn {
            column_id: ColumnId(id),
            name: name.to_string(),
            data_type,
            nullable,
            is_internal: false,
        }
    }

    fn literal_int(value: i64) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::Literal(LiteralValue::Int(value)),
            data_type: DataType::Int64,
            nullable: false,
        }
    }

    fn literal_int32(value: i64) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::Literal(LiteralValue::Int(value)),
            data_type: DataType::Int32,
            nullable: false,
        }
    }

    fn literal_float(value: f64) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::Literal(LiteralValue::Float(value)),
            data_type: DataType::Float64,
            nullable: false,
        }
    }

    fn literal_bool(value: bool) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::Literal(LiteralValue::Bool(value)),
            data_type: DataType::Boolean,
            nullable: false,
        }
    }

    fn literal_largeint(value: i128) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::Literal(LiteralValue::LargeInt(value)),
            data_type: DataType::FixedSizeBinary(novarocks_type_contract::LARGEINT_BYTE_WIDTH),
            nullable: false,
        }
    }

    fn column_ref(column: &OutputColumn) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: column.column_id,
                qualifier: None,
                column: column.name.clone(),
            },
            data_type: column.data_type.clone(),
            nullable: column.nullable,
        }
    }

    fn stats() -> PhysicalPlanStats {
        PhysicalPlanStats {
            output_row_count: 1.0,
            row_count_confidence: PlannerConfidence::Exact,
            column_statistics: HashMap::new(),
            cost_estimate: None,
            broadcast_decision: None,
        }
    }

    fn values(columns: Vec<OutputColumn>, rows: Vec<Vec<TypedExpr>>) -> PhysicalPlanNode {
        PhysicalPlanNode {
            kind: PhysicalPlanKind::Values(PlanValuesNode {
                rows,
                columns: columns.clone(),
            }),
            children: Vec::new(),
            output_columns: columns,
            stats: stats(),
            probe_runtime_filters: Vec::new(),
        }
    }

    fn redistribute(child: PhysicalPlanNode, mode: RedistributeMode) -> PhysicalPlanNode {
        let output_columns = child.output_columns.clone();
        let partition_exprs = match &mode {
            RedistributeMode::Hash { cols, .. } => cols
                .iter()
                .map(|column_id| {
                    let column = output_columns
                        .iter()
                        .find(|column| column.column_id == *column_id)
                        .expect("redistribution key belongs to its child");
                    column_ref(column)
                })
                .collect(),
            RedistributeMode::Gather | RedistributeMode::Broadcast => Vec::new(),
        };
        PhysicalPlanNode {
            kind: PhysicalPlanKind::Redistribute(crate::planner::physical::RedistributeNode {
                mode,
                partition_exprs,
                output_columns: output_columns.clone(),
            }),
            children: vec![child],
            output_columns,
            stats: stats(),
            probe_runtime_filters: Vec::new(),
        }
    }

    fn scalar_call(
        name: &str,
        args: Vec<TypedExpr>,
        data_type: DataType,
        volatility: novarocks_functions::FunctionVolatility,
    ) -> TypedExpr {
        let binding = crate::analysis::test_function_binding(
            name,
            &args,
            data_type.clone(),
            false,
            volatility,
        );
        TypedExpr {
            kind: ExprKind::FunctionCall {
                name: name.to_string(),
                args,
                distinct: false,
                binding,
                volatility,
            },
            data_type,
            nullable: false,
        }
    }

    fn hash_join(
        join_type: crate::common::JoinKind,
        build_side: PhysicalHashJoinBuildSide,
        distribution: SqlJoinDistribution,
        execution_mode: Option<JoinExecutionMode>,
        left: PhysicalPlanNode,
        right: PhysicalPlanNode,
        output_columns: Vec<OutputColumn>,
    ) -> PhysicalPlanNode {
        let left_key = left.output_columns[0].clone();
        let right_key = right.output_columns[0].clone();
        PhysicalPlanNode {
            kind: PhysicalPlanKind::HashJoin(Box::new(
                crate::planner::physical::PhysicalHashJoinNode {
                    join_type,
                    eq_conditions: vec![crate::planner::physical::PhysicalHashJoinEqCondition {
                        left: column_ref(&left_key),
                        right: column_ref(&right_key),
                        null_safe: false,
                    }],
                    other_condition: None,
                    build_side,
                    distribution,
                    execution_mode,
                    build_runtime_filters: Vec::new(),
                    output_columns: output_columns.clone(),
                },
            )),
            children: vec![left, right],
            output_columns,
            stats: stats(),
            probe_runtime_filters: Vec::new(),
        }
    }

    fn attach_singleton_join_runtime_filter(
        plan: &mut PhysicalPlanNode,
        filter_id: i32,
        null_safe: bool,
        include_probe: bool,
    ) {
        let left_key = plan.children[0].output_columns[0].clone();
        let right_key = plan.children[1].output_columns[0].clone();
        if include_probe {
            plan.children[0].probe_runtime_filters.push(
                crate::planner::physical::runtime_filter::RuntimeFilterProbeIntent {
                    filter_id,
                    probe_expr: column_ref(&left_key),
                },
            );
        }
        let PhysicalPlanKind::HashJoin(join) = &mut plan.kind else {
            unreachable!("hash_join fixture always constructs HashJoin")
        };
        join.eq_conditions[0].null_safe = null_safe;
        join.build_runtime_filters.push(
            crate::planner::physical::runtime_filter::RuntimeFilterBuildIntent {
                filter_id,
                build_expr: column_ref(&right_key),
                probe_expr: column_ref(&left_key),
                expr_order: 0,
                execution_mode: JoinExecutionMode::Singleton,
                null_semantics: if null_safe {
                    crate::planner::runtime_filter::contract::NullSemantics::NullSafeEqual
                } else {
                    crate::planner::runtime_filter::contract::NullSemantics::NeverMatches
                },
            },
        );
    }

    fn cte_produce(
        cte_id: CteId,
        output_columns: Vec<OutputColumn>,
        child: PhysicalPlanNode,
    ) -> PhysicalPlanNode {
        PhysicalPlanNode {
            kind: PhysicalPlanKind::CTEProduce(PlanCTEProduceNode {
                cte_id,
                output_columns: output_columns.clone(),
            }),
            children: vec![child],
            output_columns,
            stats: stats(),
            probe_runtime_filters: Vec::new(),
        }
    }

    fn cte_consume(
        cte_id: CteId,
        alias: &str,
        output_columns: Vec<OutputColumn>,
        producer_column_ids: Vec<ColumnId>,
    ) -> PhysicalPlanNode {
        PhysicalPlanNode {
            kind: PhysicalPlanKind::CTEConsume(PlanCTEConsumeNode {
                cte_id,
                alias: alias.to_string(),
                output_columns: output_columns.clone(),
                producer_column_ids,
            }),
            children: Vec::new(),
            output_columns,
            stats: stats(),
            probe_runtime_filters: Vec::new(),
        }
    }

    fn cte_anchor(
        cte_id: CteId,
        produce: PhysicalPlanNode,
        body: PhysicalPlanNode,
    ) -> PhysicalPlanNode {
        PhysicalPlanNode {
            kind: PhysicalPlanKind::CTEAnchor(PlanCTEAnchorNode { cte_id }),
            children: vec![produce, body.clone()],
            output_columns: body.output_columns,
            stats: stats(),
            probe_runtime_filters: Vec::new(),
        }
    }

    fn finish_for_test(plan: &PhysicalPlanNode) -> Result<PhysicalPlan, ContractLoweringError> {
        Ok(lower_final_physical_plan(plan, version(), dop())?.finish()?)
    }

    fn write_handle() -> ConnectorEncodedPayload {
        let provider = ConnectorProviderId::parse("iceberg").unwrap();
        let instance = ConnectorInstanceId::parse("warehouse").unwrap();
        ConnectorEncodedPayload::new(
            ConnectorEnvelopeHeader::new(
                provider,
                CatalogHandle::new(instance, CatalogVersion::from_bytes([9; 32])),
                ConnectorCodecCategory::WriteHandle,
                ConnectorCodecRevision::try_new(1).unwrap(),
            ),
            vec![7].into(),
        )
    }

    #[test]
    fn ordinary_write_lowers_to_writer_stream_finish_and_result_port() {
        use crate::planner::distributed::write::contract::test_support::simple_sql_write_plan_input;

        let input = column(1, "order_id", DataType::Int64, false);
        let source = values(vec![input], vec![vec![literal_int(7)]]);
        let ordinal = WriteTargetOrdinal::try_new(0).unwrap();
        let auxiliary = WriterAuxiliaryPlan::without_requirements([ordinal]).unwrap();
        let targets = FinalizedWriteTargetSet::try_new([(ordinal, write_handle())]).unwrap();
        let plan = lower_final_physical_write_plan(
            &source,
            version(),
            dop(),
            FinalWriteLowering {
                reads: None,
                write: simple_sql_write_plan_input(ConnectorWriteInputBinding::RootOutputByOrdinal),
                write_target_ordinal: ordinal,
                auxiliary: &auxiliary,
                targets,
            },
        )
        .unwrap()
        .finish()
        .unwrap();

        assert_eq!(plan.fragments().len(), 2);
        assert_eq!(plan.edges().len(), 1);
        assert!(matches!(
            plan.edges().values().next().unwrap().kind,
            EdgeKind::Stream
        ));
        assert_eq!(
            plan.fragments()
                .values()
                .filter(|fragment| fragment
                    .nodes()
                    .values()
                    .any(|node| matches!(node.kind, NodeKind::TableWriter { .. })))
                .count(),
            1
        );
        assert_eq!(
            plan.fragments()
                .values()
                .filter(|fragment| fragment
                    .nodes()
                    .values()
                    .any(|node| matches!(node.kind, NodeKind::TableFinish(_))))
                .count(),
            1
        );
        assert_eq!(plan.result_port().unwrap().fields.len(), 8);
    }

    #[test]
    fn ordinary_write_preserves_repeated_source_output_occurrences() {
        use crate::planner::distributed::write::contract::test_support::repeated_source_sql_write_plan_input;

        let source = values(
            vec![column(1, "order_id", DataType::Int64, false)],
            vec![vec![literal_int(7)]],
        );
        let ordinal = WriteTargetOrdinal::try_new(0).unwrap();
        let auxiliary = WriterAuxiliaryPlan::without_requirements([ordinal]).unwrap();
        let plan = lower_final_physical_write_plan(
            &source,
            version(),
            dop(),
            FinalWriteLowering {
                reads: None,
                write: repeated_source_sql_write_plan_input(),
                write_target_ordinal: ordinal,
                auxiliary: &auxiliary,
                targets: FinalizedWriteTargetSet::try_new([(ordinal, write_handle())]).unwrap(),
            },
        )
        .unwrap()
        .finish()
        .unwrap();
        let target = plan
            .fragments()
            .values()
            .flat_map(|fragment| fragment.nodes().values())
            .find_map(|node| match &node.kind {
                NodeKind::TableWriter { target } => Some(target),
                _ => None,
            })
            .unwrap();

        assert_eq!(target.input.len(), 2);
        assert_eq!(target.input[0], target.input[1]);
        assert_eq!(target.target_fields[0].input, target.target_fields[1].input);
    }

    #[test]
    fn ordinary_write_preserves_two_phase_auxiliary_statistics_contract() {
        use crate::planner::distributed::write::auxiliary::{
            WriterStatisticsTargetInput, plan_writer_statistics,
        };
        use crate::planner::distributed::write::contract::test_support::simple_sql_write_plan_input;
        use novarocks_functions::{AggregateOverloadMetadata, FunctionVisibility};
        use novarocks_spi::connector::{
            StatisticsArtifactIdentity, StatisticsRequiredAggregation, StatisticsScanColumn,
        };

        let ordinal = WriteTargetOrdinal::try_new(0).unwrap();
        let input_schema = arrow::datatypes::Schema::new(vec![arrow::datatypes::Field::new(
            "order_id",
            DataType::Int64,
            false,
        )]);
        let requirement = StatisticsRequiredAggregation::try_new(
            StatisticsScanColumn::try_new(0, "order_id", DataType::Int64, false).unwrap(),
            "$test_writer_stat",
            StatisticsArtifactIdentity::try_new(vec![1], "test-writer-stat-v1").unwrap(),
        )
        .unwrap();
        let functions = crate::functions::test_exact_aggregate_catalog(
            "$test_writer_stat",
            FunctionVisibility::Hidden,
            [AggregateOverloadMetadata::try_new(
                "test/writer-stat/i64/v1",
                [DataType::Int64],
                DataType::Binary,
                DataType::Binary,
                "test/writer-stat-state/v1",
            )
            .unwrap()],
        );
        let auxiliary = plan_writer_statistics(
            &[WriterStatisticsTargetInput {
                target: ordinal,
                input_schema: &input_schema,
                requirements: &[requirement],
            }],
            &functions,
        )
        .unwrap();
        let input = column(1, "order_id", DataType::Int64, false);
        let source = values(vec![input], vec![vec![literal_int(7)]]);
        let plan = lower_final_physical_write_plan(
            &source,
            version(),
            dop(),
            FinalWriteLowering {
                reads: None,
                write: simple_sql_write_plan_input(ConnectorWriteInputBinding::RootOutputByOrdinal),
                write_target_ordinal: ordinal,
                auxiliary: &auxiliary,
                targets: FinalizedWriteTargetSet::try_new([(ordinal, write_handle())]).unwrap(),
            },
        )
        .unwrap()
        .finish()
        .unwrap();
        let finish = plan
            .fragments()
            .values()
            .flat_map(|fragment| fragment.nodes().values())
            .find_map(|node| match &node.kind {
                NodeKind::TableFinish(finish) => Some(finish),
                _ => None,
            })
            .unwrap();
        assert_eq!(finish.final_aggregates.len(), 1);
        assert!(finish.grouped_unpivot.is_some());
    }

    #[test]
    fn change_stream_lowers_router_routes_to_exact_writers_and_one_finish() {
        use crate::planner::distributed::write::change_stream::ChangeStreamWriteRouteSpec;
        use crate::planner::distributed::write::contract::test_support::simple_sql_write_plan_input;
        use crate::planner::physical::{
            DistributedChangeEventOutputExpr, DistributedChangeEventSpec,
        };
        use novarocks_spi::connector::{
            ConnectorMutationRouteInput, ConnectorRowMutationEffect, ConnectorWriteFieldToken,
            ConnectorWriteRouteId,
        };

        let input = column(1, "order_id", DataType::Int64, false);
        let data = column(2, "order_id", DataType::Int64, false);
        let effect = column(3, "effect", DataType::Int8, false);
        let expanded = PhysicalPlanNode {
            kind: PhysicalPlanKind::ChangeEventExpand(DistributedChangeEventExpandNode {
                events: vec![DistributedChangeEventSpec {
                    predicate: None,
                    effect: ConnectorRowMutationEffect::Replace,
                    assignments: vec![DistributedChangeEventOutputExpr {
                        output_column_id: data.column_id,
                        expr: Some(column_ref(&input)),
                    }],
                }],
                output_columns: vec![data.clone(), effect.clone()],
                effect_column_id: effect.column_id,
            }),
            children: vec![values(vec![input], vec![vec![literal_int(7)]])],
            output_columns: vec![data, effect],
            stats: stats(),
            probe_runtime_filters: Vec::new(),
        };
        let ordinals = [
            WriteTargetOrdinal::try_new(0).unwrap(),
            WriteTargetOrdinal::try_new(1).unwrap(),
        ];
        let token = ConnectorWriteFieldToken::from_bytes([1; 32]);
        let routes = ordinals
            .iter()
            .enumerate()
            .map(|(index, ordinal)| ChangeStreamWriteRouteSpec {
                route_id: ConnectorWriteRouteId::from_bytes([index as u8 + 1; 32]),
                write_target_ordinal: *ordinal,
                accepted_effects: vec![ConnectorRowMutationEffect::Replace],
                input_ordinals: vec![ConnectorMutationRouteInput::new(token, 0)],
                partition_input_positions: Vec::new(),
                output_partition_ordinals: Vec::new(),
                sink: simple_sql_write_plan_input(ConnectorWriteInputBinding::RootOutputByOrdinal),
            })
            .collect();
        let auxiliary = WriterAuxiliaryPlan::without_requirements(ordinals).unwrap();
        let targets = FinalizedWriteTargetSet::try_new([
            (ordinals[0], write_handle()),
            (ordinals[1], write_handle()),
        ])
        .unwrap();
        let plan = lower_final_change_stream_write_plan(
            &expanded,
            version(),
            dop(),
            FinalChangeStreamWriteLowering {
                reads: None,
                dag: ChangeStreamWriteDagSpec::for_test(1, routes),
                auxiliary: &auxiliary,
                targets,
            },
        )
        .unwrap()
        .finish()
        .unwrap();

        assert_eq!(plan.fragments().len(), 4);
        assert_eq!(
            plan.edges()
                .values()
                .filter(|edge| edge.kind == EdgeKind::ChangeStreamRouter)
                .count(),
            2
        );
        assert_eq!(
            plan.fragments()
                .values()
                .filter(|fragment| fragment
                    .nodes()
                    .values()
                    .any(|node| matches!(node.kind, NodeKind::TableWriter { .. })))
                .count(),
            2
        );
        assert_eq!(
            plan.fragments()
                .values()
                .flat_map(|fragment| fragment.nodes().values())
                .filter(|node| matches!(node.kind, NodeKind::TableFinish(_)))
                .count(),
            1
        );
    }

    #[test]
    fn change_stream_reuses_one_import_for_repeated_source_value_occurrences() {
        use crate::planner::distributed::write::change_stream::ChangeStreamWriteRouteSpec;
        use crate::planner::distributed::write::contract::test_support::repeated_source_sql_write_plan_input;
        use crate::planner::physical::{
            DistributedChangeEventOutputExpr, DistributedChangeEventSpec,
        };
        use novarocks_spi::connector::{
            ConnectorMutationRouteInput, ConnectorRowMutationEffect, ConnectorWriteFieldToken,
            ConnectorWriteRouteId,
        };

        let input = column(1, "order_id", DataType::Int64, false);
        let data = column(2, "order_id", DataType::Int64, false);
        let effect = column(3, "effect", DataType::Int8, false);
        let expanded = PhysicalPlanNode {
            kind: PhysicalPlanKind::ChangeEventExpand(DistributedChangeEventExpandNode {
                events: vec![DistributedChangeEventSpec {
                    predicate: None,
                    effect: ConnectorRowMutationEffect::Replace,
                    assignments: vec![DistributedChangeEventOutputExpr {
                        output_column_id: data.column_id,
                        expr: Some(column_ref(&input)),
                    }],
                }],
                output_columns: vec![data.clone(), effect.clone()],
                effect_column_id: effect.column_id,
            }),
            children: vec![values(vec![input], vec![vec![literal_int(7)]])],
            output_columns: vec![data, effect],
            stats: stats(),
            probe_runtime_filters: Vec::new(),
        };
        let ordinal = WriteTargetOrdinal::try_new(0).unwrap();
        let route = ChangeStreamWriteRouteSpec {
            route_id: ConnectorWriteRouteId::from_bytes([1; 32]),
            write_target_ordinal: ordinal,
            accepted_effects: vec![ConnectorRowMutationEffect::Replace],
            input_ordinals: vec![
                ConnectorMutationRouteInput::new(ConnectorWriteFieldToken::from_bytes([1; 32]), 0),
                ConnectorMutationRouteInput::new(ConnectorWriteFieldToken::from_bytes([2; 32]), 0),
            ],
            partition_input_positions: vec![1],
            output_partition_ordinals: vec![0],
            sink: repeated_source_sql_write_plan_input(),
        };
        let auxiliary = WriterAuxiliaryPlan::without_requirements([ordinal]).unwrap();
        let plan = lower_final_change_stream_write_plan(
            &expanded,
            version(),
            dop(),
            FinalChangeStreamWriteLowering {
                reads: None,
                dag: ChangeStreamWriteDagSpec::for_test(1, vec![route]),
                auxiliary: &auxiliary,
                targets: FinalizedWriteTargetSet::try_new([(ordinal, write_handle())]).unwrap(),
            },
        )
        .unwrap()
        .finish()
        .unwrap();
        let edge = plan
            .edges()
            .values()
            .find(|edge| edge.kind == EdgeKind::ChangeStreamRouter)
            .unwrap();
        let target = plan
            .fragments()
            .get(&edge.destination.fragment)
            .and_then(|fragment| fragment.nodes().get(&fragment.root()))
            .and_then(|node| match &node.kind {
                NodeKind::TableWriter { target } => Some(target),
                _ => None,
            })
            .unwrap();

        assert_eq!(edge.source.projection[0], edge.source.projection[1]);
        assert_eq!(
            edge.destination.receive_mapping[0].1,
            edge.destination.receive_mapping[1].1
        );
        assert_eq!(target.input[0], target.input[1]);
        assert!(matches!(
            edge.partitioning.source,
            Distribution::Hash { .. }
        ));
    }

    fn encoded_provider_payload(
        binding: &ConnectorReadBinding,
        category: ConnectorCodecCategory,
    ) -> ConnectorEncodedPayload {
        ConnectorEncodedPayload::new(
            ConnectorEnvelopeHeader::new(
                binding.descriptor().provider_id.clone(),
                binding.catalog_handle().clone(),
                category,
                ConnectorCodecRevision::try_new(1).unwrap(),
            ),
            vec![category as u8 + 1].into(),
        )
    }

    fn sort_item(column: &OutputColumn, asc: bool, nulls_first: bool) -> SortItem {
        SortItem {
            expr: column_ref(column),
            asc,
            nulls_first,
        }
    }

    fn aggregate_call(
        argument: TypedExpr,
        output_column_id: ColumnId,
    ) -> crate::planner::payload::AggregateCall {
        crate::planner::payload::AggregateCall {
            name: "sum".into(),
            args: vec![argument],
            distinct: false,
            result_type: DataType::Int64,
            order_by: Vec::new(),
            resolved: crate::functions::test_resolved_aggregate("sum", &[DataType::Int64], false),
            output_column_id,
        }
    }

    #[test]
    fn lowers_values_filter_project_directly_into_the_final_contract() {
        let input = column(1, "number", DataType::Int64, false);
        let predicate = TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(column_ref(&input)),
                op: BinOp::Gt,
                right: Box::new(literal_int(0)),
            },
            data_type: DataType::Boolean,
            nullable: false,
        };
        let filter = PhysicalPlanNode {
            kind: PhysicalPlanKind::Filter(PlanFilterNode { predicate }),
            children: vec![values(vec![input.clone()], vec![vec![literal_int(7)]])],
            output_columns: vec![input.clone()],
            stats: stats(),
            probe_runtime_filters: Vec::new(),
        };
        let computed = column(2, "next", DataType::Int64, false);
        let project = PhysicalPlanNode {
            kind: PhysicalPlanKind::Project(PlanProjectNode {
                items: vec![
                    ProjectItem {
                        expr: column_ref(&input),
                        output_name: "first".to_string(),
                        output_column_id: input.column_id,
                    },
                    ProjectItem {
                        expr: TypedExpr {
                            kind: ExprKind::BinaryOp {
                                left: Box::new(column_ref(&input)),
                                op: BinOp::Add,
                                right: Box::new(literal_int(1)),
                            },
                            data_type: DataType::Int64,
                            nullable: false,
                        },
                        output_name: "next".to_string(),
                        output_column_id: computed.column_id,
                    },
                ],
                output_qualifier: None,
            }),
            children: vec![filter],
            output_columns: vec![input, computed],
            stats: stats(),
            probe_runtime_filters: Vec::new(),
        };

        let final_plan = finish_for_test(&project).unwrap();

        assert_eq!(final_plan.version(), version());
        assert!(final_plan.edges().is_empty());
        let fragment = final_plan.fragments().get(&ROOT_FRAGMENT_ID).unwrap();
        assert_eq!(fragment.nodes().len(), 3);
        assert_eq!(fragment.values().len(), 2);
        assert_eq!(final_plan.result_port().unwrap().fields.len(), 2);
        assert!(matches!(
            fragment
                .nodes()
                .get(&fragment.root())
                .unwrap()
                .required_inputs[0]
                .distribution,
            Distribution::Unconstrained
        ));
        assert!(matches!(
            fragment.nodes().get(&fragment.root()).unwrap().kind,
            NodeKind::Project { .. }
        ));
    }

    #[test]
    fn identity_project_reuses_value_identity_for_repeated_occurrences() {
        let input = column(1, "number", DataType::Int64, false);
        let project = PhysicalPlanNode {
            kind: PhysicalPlanKind::Project(PlanProjectNode {
                items: vec![
                    ProjectItem {
                        expr: column_ref(&input),
                        output_name: "left".to_string(),
                        output_column_id: input.column_id,
                    },
                    ProjectItem {
                        expr: column_ref(&input),
                        output_name: "right".to_string(),
                        output_column_id: input.column_id,
                    },
                ],
                output_qualifier: None,
            }),
            children: vec![values(vec![input.clone()], vec![vec![literal_int(7)]])],
            output_columns: vec![input.clone(), input],
            stats: stats(),
            probe_runtime_filters: Vec::new(),
        };

        let final_plan = finish_for_test(&project).unwrap();
        let fragment = final_plan.fragments().get(&ROOT_FRAGMENT_ID).unwrap();
        let root = fragment.nodes().get(&fragment.root()).unwrap();
        assert_eq!(root.output.columns[0], root.output.columns[1]);
        assert_eq!(fragment.values().len(), 1);
        assert!(matches!(
            fragment
                .values()
                .get(&root.output.columns[0])
                .unwrap()
                .origin,
            ValueOrigin::NodeOutput { .. }
        ));
        assert_eq!(
            final_plan.result_port().unwrap().fields[0].value,
            root.output.columns[0]
        );
        assert_eq!(
            final_plan.result_port().unwrap().fields[1].value,
            root.output.columns[0]
        );
        assert_eq!(
            final_plan.result_port().unwrap().fields[0].name.as_ref(),
            "number"
        );
        assert_eq!(
            final_plan.result_port().unwrap().fields[1].name.as_ref(),
            "number"
        );
        assert_eq!(
            final_plan.result_port().unwrap().fields[0].alias.as_deref(),
            Some("left")
        );
        assert_eq!(
            final_plan.result_port().unwrap().fields[1].alias.as_deref(),
            Some("right")
        );
    }

    #[test]
    fn scan_finish_preserves_interleaved_provider_and_derived_occurrences() {
        let binding = crate::binding::SqlTableBindingId::new_for_test(71);
        let payload = column(1, "payload", DataType::Utf8, false);
        let mut synthetic = column(2, "__variant_payload_0", DataType::Utf8, true);
        synthetic.is_internal = true;
        let mut row_id = column(3, "_row_id", DataType::Int64, false);
        row_id.is_internal = true;
        let identity = crate::planner::table::SqlTableIdentity::try_new(
            "iceberg".to_string(),
            "db".to_string(),
            "events".to_string(),
        )
        .unwrap();
        let table = crate::planner::table::TableDef {
            name: "events".to_string(),
            columns: vec![novarocks_types::schema::ColumnDef {
                name: payload.name.clone(),
                data_type: payload.data_type.clone(),
                nullable: payload.nullable,
                write_default: None,
                logical_type: None,
            }],
            iceberg_row_lineage_metadata_columns: vec![novarocks_types::schema::ColumnDef {
                name: row_id.name.clone(),
                data_type: row_id.data_type.clone(),
                nullable: row_id.nullable,
                write_default: None,
                logical_type: None,
            }],
            source: crate::planner::table::ScanSource::Sql(
                crate::planner::table::SqlScanSource::new(
                    binding,
                    identity,
                    crate::planner::table::SqlScanKind::Data {
                        version: crate::planner::table::SqlTableVersionSelector::Current,
                    },
                ),
            ),
        };
        let string_literal = |value: &str| TypedExpr {
            kind: ExprKind::Literal(LiteralValue::String(value.to_string())),
            data_type: DataType::Utf8,
            nullable: false,
        };
        let binding_args = [
            column_ref(&payload),
            string_literal("$.k"),
            string_literal("string"),
        ];
        let plan = PhysicalPlanNode {
            kind: PhysicalPlanKind::Scan(
                crate::planner::physical::PhysicalScanNode::from(
                    crate::planner::payload::PlanScanNode {
                        database: "db".to_string(),
                        table,
                        alias: None,
                        columns: vec![payload.clone(), synthetic.clone(), row_id.clone()],
                        predicates: Vec::new(),
                        required_columns: Some(vec![
                            payload.column_id,
                            synthetic.column_id,
                            row_id.column_id,
                        ]),
                        variant_columns: vec![crate::common::ScanVariantColumn {
                            source_column_id: payload.column_id,
                            source_column: payload.name.clone(),
                            synthetic_column_id: synthetic.column_id,
                            synthetic_column: synthetic.name.clone(),
                            canonical_path: "$.k".to_string(),
                            requested_type: DataType::Utf8,
                            requested_type_literal: "string".to_string(),
                            strict: true,
                            binding: crate::analysis::test_function_binding(
                                "variant_get",
                                &binding_args,
                                DataType::Utf8,
                                true,
                                novarocks_functions::FunctionVolatility::Immutable,
                            ),
                        }],
                        mv_rewritten_from: None,
                    },
                )
                .finalize_provider_read_occurrence(ProviderReadOccurrenceId::new(0))
                .expect("test scan occurrence"),
            ),
            children: Vec::new(),
            output_columns: vec![payload.clone(), synthetic, row_id.clone()],
            stats: stats(),
            probe_runtime_filters: Vec::new(),
        };
        let relation = TableIdentity::new("iceberg", "db", "events");
        let needs = vec![
            ProviderReadColumnNeed::for_test(
                0,
                payload.name.clone(),
                value_type(&payload),
                ConnectorValueType::Varchar,
            ),
            ProviderReadColumnNeed::for_test(
                1,
                row_id.name.clone(),
                value_type(&row_id),
                ConnectorValueType::BigInt,
            ),
        ];
        let need = ProviderReadNeed::exact_projection_for_test(
            binding,
            ProviderReadRelationNeed::Data {
                relation,
                version: ProviderReadVersionNeed::Current,
            },
            needs,
        );
        let provider_id = ConnectorProviderId::parse("iceberg").unwrap();
        let instance_id = ConnectorInstanceId::parse("lakehouse").unwrap();
        let provider_binding = ConnectorReadBinding::new(
            ConnectorInstanceDescriptor {
                provider_id,
                instance_id: instance_id.clone(),
            },
            CatalogHandle::new(instance_id, CatalogVersion::from_bytes([3; 32])),
        );
        let provider_column = |category| ProviderColumnReference {
            column_payload: encoded_provider_payload(&provider_binding, category),
        };
        let read = ProviderReadReference {
            binding: provider_binding.clone(),
            input_version: ExactInputVersion::try_new([9]).unwrap(),
            relation: ConnectorReadRelationPayload::new(
                ConnectorReadRelationKind::Table,
                encoded_provider_payload(&provider_binding, ConnectorCodecCategory::ReadTable),
                encoded_provider_payload(&provider_binding, ConnectorCodecCategory::ReadView),
            ),
        };
        let artifact = ArtifactRefId::new(7);
        let artifact_kind = ArtifactKind::try_new("split-directory").unwrap();
        let artifact_format = ArtifactFormat {
            id: ArtifactFormatId::try_new("uea5.provider-artifact").unwrap(),
            revision: 1,
        };
        let artifact_schema: Box<[ValueType]> = Box::from([value_type(&row_id)]);
        let artifact_source = ArtifactSourceBinding {
            source: read.clone(),
            selection_digest: [8; 32],
        };
        let artifact_coverage = CoverageSet {
            domain: "manifest-entry".into(),
            selection_digest: [8; 32],
            ranges: Box::from([CoverageRange {
                start: None,
                end: None,
            }]),
            complete_input: true,
        };
        let contract = ProviderReadStaticContract {
            sql_binding: binding,
            request: ProviderReadRequestBinding::from_need(&need),
            read,
            work_source: ConnectorReadWorkSource::RuntimeSplits,
            selection_digest: [8; 32],
            schema: vec![
                ProviderReadColumnFact::new(
                    0,
                    provider_column(ConnectorCodecCategory::ReadColumn),
                    value_type(&payload),
                ),
                ProviderReadColumnFact::new(
                    1,
                    provider_column(ConnectorCodecCategory::ReadColumn),
                    value_type(&row_id),
                ),
            ]
            .into_boxed_slice(),
            predicates: Box::default(),
            limit: ProviderReadLimitFact::NotRequested,
            provided_properties: ProviderReadProperties {
                distribution: ProviderReadDistribution::Hash {
                    keys: Box::from([1]),
                    scheme: provider_hash_scheme(51),
                },
                ordering: Box::from([
                    ProviderReadOrderingKey {
                        request_ordinal: 0,
                        direction: SortDirection::Ascending,
                        null_ordering: NullOrdering::First,
                    },
                    ProviderReadOrderingKey {
                        request_ordinal: 1,
                        direction: SortDirection::Descending,
                        null_ordering: NullOrdering::Last,
                    },
                ]),
            },
            artifact_inputs: Box::from([ArtifactInputRequirement {
                artifact,
                kind: artifact_kind.clone(),
                format: artifact_format.clone(),
                schema: artifact_schema.clone(),
                source: artifact_source.clone(),
                required_coverage: artifact_coverage.clone(),
            }]),
            artifact_refs: Box::from([SealedArtifactRef {
                id: artifact,
                kind: artifact_kind,
                format: artifact_format,
                schema: artifact_schema,
                source: artifact_source,
                coverage: artifact_coverage,
                location: "s3://warehouse/artifacts/7".into(),
                content_digest: [62; 32],
                schema_digest: [63; 32],
                object_count: 1,
                row_count: 3,
            }]),
            coverage_evidence: Box::default(),
        };
        let mut bucket_contract = contract.clone();
        bucket_contract.provided_properties.distribution =
            ProviderReadDistribution::BucketShuffle {
                keys: Box::from([1]),
                scheme: provider_bucket_scheme(52),
            };
        let reads = FinalizedProviderReadSet::single_for_test(
            binding,
            ProviderReadOccurrenceId::new(0),
            contract,
            novarocks_physical_plan::ScanReadBudget {
                max_batch_rows: 1024,
                max_batch_bytes: 1 << 20,
            },
        );

        let final_plan =
            lower_final_physical_plan_with_provider_reads(&plan, version(), dop(), reads)
                .unwrap()
                .finish()
                .expect("interleaved scan output must finish validation");
        let fragment = final_plan.fragments().get(&ROOT_FRAGMENT_ID).unwrap();
        let root = fragment.nodes().get(&fragment.root()).unwrap();
        assert_eq!(final_plan.artifact_refs().len(), 1);
        assert_eq!(root.output.columns.len(), 3);
        assert!(matches!(
            fragment
                .values()
                .get(&root.output.columns[0])
                .unwrap()
                .origin,
            ValueOrigin::ProviderField { .. }
        ));
        assert!(matches!(
            fragment
                .values()
                .get(&root.output.columns[1])
                .unwrap()
                .origin,
            ValueOrigin::Expr { .. }
        ));
        assert!(matches!(
            fragment
                .values()
                .get(&root.output.columns[2])
                .unwrap()
                .origin,
            ValueOrigin::ProviderField { .. }
        ));
        let Distribution::Hash { keys, .. } = &root.output_properties.distribution else {
            panic!("expected provider hash distribution");
        };
        assert_eq!(keys.as_ref(), &[root.output.columns[2]]);
        assert_eq!(
            root.output_properties
                .ordering
                .iter()
                .map(|key| key.value)
                .collect::<Vec<_>>(),
            vec![root.output.columns[0], root.output.columns[2]]
        );

        let bucket_reads = FinalizedProviderReadSet::single_for_test(
            binding,
            ProviderReadOccurrenceId::new(0),
            bucket_contract,
            novarocks_physical_plan::ScanReadBudget {
                max_batch_rows: 1024,
                max_batch_bytes: 1 << 20,
            },
        );
        let bucket_plan =
            lower_final_physical_plan_with_provider_reads(&plan, version(), dop(), bucket_reads)
                .unwrap()
                .finish()
                .expect("interleaved bucket scan output must finish validation");
        let bucket_fragment = bucket_plan.fragments().get(&ROOT_FRAGMENT_ID).unwrap();
        let bucket_root = bucket_fragment
            .nodes()
            .get(&bucket_fragment.root())
            .unwrap();
        let Distribution::BucketShuffle { keys, .. } = &bucket_root.output_properties.distribution
        else {
            panic!("expected provider bucket-shuffle distribution");
        };
        assert_eq!(keys.as_ref(), &[bucket_root.output.columns[2]]);
        assert_eq!(
            bucket_root
                .output_properties
                .ordering
                .iter()
                .map(|key| key.value)
                .collect::<Vec<_>>(),
            vec![bucket_root.output.columns[0], bucket_root.output.columns[2]]
        );
    }

    #[test]
    fn provider_partition_tokens_are_isolated_from_sql_exchange_and_definition_drift() {
        let provider_id = ConnectorProviderId::parse("iceberg").unwrap();
        let instance_id = ConnectorInstanceId::parse("lakehouse").unwrap();
        let binding = ConnectorReadBinding::new(
            ConnectorInstanceDescriptor {
                provider_id,
                instance_id: instance_id.clone(),
            },
            CatalogHandle::new(instance_id, CatalogVersion::from_bytes([3; 32])),
        );
        let read = ProviderReadReference {
            binding: binding.clone(),
            input_version: ExactInputVersion::try_new([9]).unwrap(),
            relation: ConnectorReadRelationPayload::new(
                ConnectorReadRelationKind::Table,
                encoded_provider_payload(&binding, ConnectorCodecCategory::ReadTable),
                encoded_provider_payload(&binding, ConnectorCodecCategory::ReadView),
            ),
        };
        let mut old_low_space = [0; 32];
        old_low_space[28..].copy_from_slice(&1_u32.to_be_bytes());
        let mut old_low_count = [0; 32];
        old_low_count[0] = 1;
        old_low_count[28..].copy_from_slice(&1_u32.to_be_bytes());
        let scheme = ProviderHashPartitionScheme {
            space: crate::compiler::ProviderPartitionSpaceToken::from_bytes(old_low_space),
            count: crate::compiler::ProviderPartitionCountToken::from_bytes(old_low_count),
            admissible: crate::compiler::ProviderPartitionCountDomain {
                min: 1,
                max: 8,
                requires_power_of_two: true,
            },
            algorithm: PartitionHashAlgorithm::NativeExchangeV1,
        };

        let mut visitor = ContractLoweringVisitor::new(version(), dop(), None);
        let sql_exchange = visitor.allocate_hash_scheme().unwrap();
        let provider = visitor.lower_provider_hash_scheme(&read, &scheme).unwrap();
        assert_ne!(provider.space, sql_exchange.space);
        assert_ne!(provider.count.id, sql_exchange.count.id);

        let mut drifted = scheme;
        drifted.algorithm = PartitionHashAlgorithm::NativeBucketCrc32V1;
        assert!(matches!(
            visitor.lower_provider_hash_scheme(&read, &drifted),
            Err(ContractLoweringError::ProviderRead { detail })
                if detail.contains("changes definition")
        ));
    }

    #[test]
    fn narrowed_integer_literal_has_an_explicit_contract_cast() {
        let output = column(1, "small", DataType::Int32, false);
        let final_plan =
            finish_for_test(&values(vec![output], vec![vec![literal_int32(7)]])).unwrap();
        let fragment = final_plan.fragments().get(&ROOT_FRAGMENT_ID).unwrap();
        let root = fragment.nodes().get(&fragment.root()).unwrap();
        let NodeKind::Values { rows } = &root.kind else {
            panic!("expected Values root");
        };
        let cast = fragment.expressions().get(rows[0][0]).unwrap();
        let ContractExprKind::Cast {
            expr: source,
            target,
        } = &cast.kind
        else {
            panic!("expected explicit cast for narrowed integer literal");
        };
        assert_eq!(target, &DataType::Int32);
        let source = fragment.expressions().get(*source).unwrap();
        assert_eq!(source.ty, ValueType::new(DataType::Int64, false));
        assert!(matches!(
            source.kind,
            ContractExprKind::Literal(ContractLiteralValue::Int64(7))
        ));
    }

    #[test]
    fn values_materializes_the_analyzed_common_type_and_nullability() {
        let output = column(1, "number", DataType::Float64, true);
        let final_plan = finish_for_test(&values(
            vec![output],
            vec![vec![literal_int(7)], vec![literal_float(1.5)]],
        ))
        .unwrap();
        let fragment = final_plan.fragments().get(&ROOT_FRAGMENT_ID).unwrap();
        let root = fragment.nodes().get(&fragment.root()).unwrap();
        let NodeKind::Values { rows } = &root.kind else {
            panic!("expected Values root");
        };
        for expression in rows.iter().map(|row| row[0]) {
            let expression = fragment.expressions().get(expression).unwrap();
            assert_eq!(expression.ty, ValueType::new(DataType::Float64, true));
            assert!(matches!(expression.kind, ContractExprKind::Case { .. }));
        }
    }

    #[test]
    fn completed_plan_freezes_costs_as_node_annotations() {
        let output = column(1, "number", DataType::Int64, false);
        let mut plan = values(vec![output], vec![vec![literal_int(7)]]);
        plan.stats.output_row_count = 3.0;
        plan.stats.cost_estimate = Some(PlannerCostEstimate {
            cpu_cost: 1.0,
            memory_cost: 2.0,
            network_cost: 4.0,
        });

        let final_plan = finish_for_test(&plan).unwrap();
        let fragment = final_plan.fragments().get(&ROOT_FRAGMENT_ID).unwrap();
        let annotation = final_plan
            .annotations()
            .iter()
            .find(|annotation| {
                annotation.subject
                    == novarocks_physical_plan::AnnotationSubject::Node(
                        ROOT_FRAGMENT_ID,
                        fragment.root(),
                    )
            })
            .expect("root cost annotation");

        assert_eq!(annotation.key.as_ref(), "optimizer.statistics");
        assert_eq!(
            annotation.value.as_ref(),
            "rows=3, cpu=1, memory=2, network=4"
        );
    }

    #[test]
    fn filter_preserves_repeated_input_occurrences_and_display_names() {
        let input = column(1, "number", DataType::Int64, false);
        let repeated_project = PhysicalPlanNode {
            kind: PhysicalPlanKind::Project(PlanProjectNode {
                items: vec![
                    ProjectItem {
                        expr: column_ref(&input),
                        output_name: "first".to_string(),
                        output_column_id: input.column_id,
                    },
                    ProjectItem {
                        expr: column_ref(&input),
                        output_name: "second".to_string(),
                        output_column_id: input.column_id,
                    },
                ],
                output_qualifier: None,
            }),
            children: vec![values(vec![input.clone()], vec![vec![literal_int(7)]])],
            output_columns: vec![input.clone(), input.clone()],
            stats: stats(),
            probe_runtime_filters: Vec::new(),
        };
        let filter = PhysicalPlanNode {
            kind: PhysicalPlanKind::Filter(PlanFilterNode {
                predicate: TypedExpr {
                    kind: ExprKind::BinaryOp {
                        left: Box::new(column_ref(&input)),
                        op: BinOp::Gt,
                        right: Box::new(literal_int(0)),
                    },
                    data_type: DataType::Boolean,
                    nullable: false,
                },
            }),
            children: vec![repeated_project],
            output_columns: vec![input.clone(), input],
            stats: stats(),
            probe_runtime_filters: Vec::new(),
        };

        let final_plan = finish_for_test(&filter).unwrap();
        let fragment = final_plan.fragments().get(&ROOT_FRAGMENT_ID).unwrap();
        let root = fragment.nodes().get(&fragment.root()).unwrap();
        assert_eq!(root.output.columns[0], root.output.columns[1]);
        assert_eq!(
            final_plan.result_port().unwrap().fields[0].name.as_ref(),
            "number"
        );
        assert_eq!(
            final_plan.result_port().unwrap().fields[1].name.as_ref(),
            "number"
        );
        assert_eq!(
            final_plan.result_port().unwrap().fields[0].alias.as_deref(),
            Some("first")
        );
        assert_eq!(
            final_plan.result_port().unwrap().fields[1].alias.as_deref(),
            Some("second")
        );
    }

    #[test]
    fn unpivot_preserves_exact_mapping_constants_and_budget() {
        let passthrough_input = column(1, "tenant", DataType::Int64, false);
        let value_input = column(2, "metric", DataType::Int64, true);
        let passthrough_output = column(3, "tenant_out", DataType::Int64, false);
        let value_output = column(4, "metric_value", DataType::Int64, true);
        let literal_output = column(5, "metric_name", DataType::Utf8, false);
        let child = values(
            vec![passthrough_input.clone(), value_input.clone()],
            vec![vec![
                literal_int(7),
                TypedExpr {
                    kind: ExprKind::Literal(LiteralValue::Null),
                    data_type: DataType::Int64,
                    nullable: true,
                },
            ]],
        );
        let payload = PlanUnpivotNode::try_new(
            &child.output_columns,
            vec![PlanUnpivotPassthroughColumn {
                input_column_id: passthrough_input.column_id,
                output_column_id: passthrough_output.column_id,
            }],
            value_output.column_id,
            vec![literal_output.column_id],
            vec![PlanUnpivotValueMapping {
                input_value_column_id: value_input.column_id,
                constants: vec![crate::analysis::UnpivotConstant::Scalar(TypedExpr {
                    kind: ExprKind::Literal(LiteralValue::String("metric".to_string())),
                    data_type: DataType::Utf8,
                    nullable: false,
                })],
            }],
            vec![
                passthrough_output.clone(),
                value_output.clone(),
                literal_output.clone(),
            ],
            512,
            65_536,
        )
        .unwrap();
        let plan = PhysicalPlanNode {
            kind: PhysicalPlanKind::Unpivot(payload),
            children: vec![child],
            output_columns: vec![passthrough_output, value_output, literal_output],
            stats: stats(),
            probe_runtime_filters: Vec::new(),
        };

        let final_plan = finish_for_test(&plan).unwrap();
        let fragment = final_plan.fragments().get(&ROOT_FRAGMENT_ID).unwrap();
        let root = fragment.nodes().get(&fragment.root()).unwrap();
        let NodeKind::Unpivot { spec } = &root.kind else {
            panic!("expected Unpivot");
        };
        assert_eq!(spec.max_output_rows, 512);
        assert_eq!(spec.max_output_bytes, 65_536);
        assert_eq!(spec.passthrough.len(), 1);
        assert_eq!(spec.mappings.len(), 1);
        assert!(matches!(
            spec.mappings[0].constants[0],
            ContractUnpivotConstant::Scalar(_)
        ));
    }

    fn repeat_over_identity_aliases(
        projected_columns: Vec<OutputColumn>,
        keys: Vec<OutputColumn>,
        sets: Vec<Vec<ColumnId>>,
        grouping_ids: Vec<u64>,
    ) -> PhysicalPlanNode {
        let source = column(1, "source", DataType::Int64, false);
        let project = PhysicalPlanNode {
            kind: PhysicalPlanKind::Project(PlanProjectNode {
                items: projected_columns
                    .iter()
                    .map(|column| ProjectItem {
                        expr: column_ref(&source),
                        output_name: column.name.clone(),
                        output_column_id: column.column_id,
                    })
                    .collect(),
                output_qualifier: None,
            }),
            children: vec![values(vec![source], vec![vec![literal_int(7)]])],
            output_columns: projected_columns.clone(),
            stats: stats(),
            probe_runtime_filters: Vec::new(),
        };
        let grouping = column(4, "grouping", DataType::Int64, false);
        let mut output_columns = projected_columns;
        for column in &mut output_columns {
            if keys.iter().any(|key| key.column_id == column.column_id) {
                column.nullable = true;
            }
        }
        output_columns.push(grouping.clone());
        PhysicalPlanNode {
            kind: PhysicalPlanKind::Repeat(PlanRepeatNode {
                repeat_column_ref_list: sets
                    .iter()
                    .map(|set| {
                        set.iter()
                            .map(|id| {
                                keys.iter()
                                    .find(|key| key.column_id == *id)
                                    .unwrap()
                                    .name
                                    .clone()
                            })
                            .collect()
                    })
                    .collect(),
                repeat_column_ref_ids: sets,
                grouping_ids,
                all_rollup_columns: keys.iter().map(|key| key.name.clone()).collect(),
                all_rollup_column_ids: keys.iter().map(|key| key.column_id).collect(),
                grouping_key_aliases: Vec::new(),
                grouping_fn_args: vec![(
                    "grouping".to_string(),
                    keys.iter().map(|key| key.name.clone()).collect(),
                )],
                grouping_fn_arg_ids: vec![keys.iter().map(|key| key.column_id).collect()],
                grouping_fn_ids: vec![("grouping".to_string(), grouping.column_id)],
                virtual_tuple_id: None,
            }),
            children: vec![project],
            output_columns,
            stats: stats(),
            probe_runtime_filters: Vec::new(),
        }
    }

    #[test]
    fn repeat_keeps_original_input_and_repeated_alias_in_independent_null_domains() {
        let source = column(1, "source", DataType::Int64, false);
        let key = column(2, "key", DataType::Int64, false);
        let plan = repeat_over_identity_aliases(
            vec![source, key.clone(), key.clone()],
            vec![key.clone()],
            vec![vec![key.column_id], Vec::new()],
            vec![0, 1],
        );
        let final_plan = finish_for_test(&plan).unwrap();
        let fragment = final_plan.fragments().get(&ROOT_FRAGMENT_ID).unwrap();
        let repeat = fragment.nodes().get(&fragment.root()).unwrap();
        let copied = fragment.nodes().get(&repeat.inputs[0]).unwrap();
        let original = fragment.nodes().get(&copied.inputs[0]).unwrap();
        assert!(matches!(copied.kind, NodeKind::Project { .. }));
        assert_eq!(original.output.columns[0], original.output.columns[1]);
        assert_eq!(original.output.columns[1], original.output.columns[2]);
        assert_eq!(copied.output.columns[0], original.output.columns[0]);
        assert_ne!(copied.output.columns[0], copied.output.columns[1]);
        assert_eq!(copied.output.columns[1], copied.output.columns[2]);
        assert_eq!(repeat.output.columns[0], copied.output.columns[0]);
        assert_ne!(repeat.output.columns[1], copied.output.columns[1]);
        assert_eq!(repeat.output.columns[1], repeat.output.columns[2]);
        assert!(!fragment.values()[&repeat.output.columns[0]].ty.nullable);
        assert!(fragment.values()[&repeat.output.columns[1]].ty.nullable);
        assert!(matches!(
            fragment.values()[&copied.output.columns[1]].origin,
            ValueOrigin::Expr { node, .. } if node == copied.id
        ));
        let NodeKind::Repeat {
            rollup_keys,
            grouping_sets,
            grouping_values,
            grouping_outputs,
        } = &repeat.kind
        else {
            panic!("expected Repeat");
        };
        assert_eq!(rollup_keys.as_ref(), &[copied.output.columns[1]]);
        assert_eq!(grouping_sets[0].as_ref(), &[copied.output.columns[1]]);
        assert!(grouping_sets[1].is_empty());
        assert_eq!(
            grouping_values.as_ref(),
            &[(copied.output.columns[1], repeat.output.columns[1])]
        );
        assert_eq!(
            grouping_outputs[0].arguments.as_ref(),
            &[copied.output.columns[1]]
        );
    }

    #[test]
    fn repeat_same_source_keys_with_different_presence_have_independent_values() {
        let source = column(1, "source", DataType::Int64, false);
        let left = column(2, "left_key", DataType::Int64, false);
        let right = column(3, "right_key", DataType::Int64, false);
        let plan = repeat_over_identity_aliases(
            vec![source, left.clone(), right.clone()],
            vec![left.clone(), right.clone()],
            vec![vec![left.column_id], vec![right.column_id], Vec::new()],
            vec![1, 2, 3],
        );
        let final_plan = finish_for_test(&plan).unwrap();
        let fragment = final_plan.fragments().get(&ROOT_FRAGMENT_ID).unwrap();
        let repeat = fragment.nodes().get(&fragment.root()).unwrap();
        let copied = fragment.nodes().get(&repeat.inputs[0]).unwrap();
        assert_ne!(copied.output.columns[0], copied.output.columns[1]);
        assert_ne!(copied.output.columns[0], copied.output.columns[2]);
        assert_ne!(copied.output.columns[1], copied.output.columns[2]);
        assert_eq!(repeat.output.columns[0], copied.output.columns[0]);
        assert_ne!(repeat.output.columns[1], repeat.output.columns[2]);
        let NodeKind::Repeat {
            grouping_sets,
            grouping_values,
            grouping_outputs,
            ..
        } = &repeat.kind
        else {
            panic!("expected Repeat");
        };
        assert_eq!(grouping_sets[0].as_ref(), &[copied.output.columns[1]]);
        assert_eq!(grouping_sets[1].as_ref(), &[copied.output.columns[2]]);
        assert!(grouping_sets[2].is_empty());
        assert_eq!(
            grouping_values.as_ref(),
            &[
                (copied.output.columns[1], repeat.output.columns[1]),
                (copied.output.columns[2], repeat.output.columns[2])
            ]
        );
        assert_eq!(
            grouping_outputs[0].arguments.as_ref(),
            &copied.output.columns[1..3]
        );
    }

    #[test]
    fn repeat_reuses_one_nullable_value_for_repeated_grouping_occurrences() {
        let input = column(1, "k", DataType::Int64, false);
        let repeated = PhysicalPlanNode {
            kind: PhysicalPlanKind::Project(PlanProjectNode {
                items: vec![
                    ProjectItem {
                        expr: column_ref(&input),
                        output_name: "k_first".to_string(),
                        output_column_id: input.column_id,
                    },
                    ProjectItem {
                        expr: column_ref(&input),
                        output_name: "k_second".to_string(),
                        output_column_id: input.column_id,
                    },
                ],
                output_qualifier: None,
            }),
            children: vec![values(vec![input.clone()], vec![vec![literal_int(1)]])],
            output_columns: vec![input.clone(), input.clone()],
            stats: stats(),
            probe_runtime_filters: Vec::new(),
        };
        let nullable_key = column(1, "k", DataType::Int64, true);
        let grouping = column(2, "grouping", DataType::Int64, false);
        let plan = PhysicalPlanNode {
            kind: PhysicalPlanKind::Repeat(PlanRepeatNode {
                repeat_column_ref_list: vec![vec!["k".to_string()], Vec::new()],
                repeat_column_ref_ids: vec![vec![input.column_id], Vec::new()],
                grouping_ids: vec![0, 1],
                all_rollup_columns: vec!["k".to_string()],
                all_rollup_column_ids: vec![input.column_id],
                grouping_key_aliases: Vec::new(),
                grouping_fn_args: vec![("grouping".to_string(), vec!["k".to_string()])],
                grouping_fn_arg_ids: vec![vec![input.column_id]],
                grouping_fn_ids: vec![("grouping".to_string(), grouping.column_id)],
                virtual_tuple_id: None,
            }),
            children: vec![repeated],
            output_columns: vec![nullable_key.clone(), nullable_key, grouping],
            stats: stats(),
            probe_runtime_filters: Vec::new(),
        };

        let final_plan = finish_for_test(&plan).unwrap();
        let fragment = final_plan.fragments().get(&ROOT_FRAGMENT_ID).unwrap();
        let root = fragment.nodes().get(&fragment.root()).unwrap();
        let NodeKind::Repeat {
            grouping_values,
            grouping_outputs,
            ..
        } = &root.kind
        else {
            panic!("expected Repeat");
        };
        assert_eq!(root.output.columns[0], root.output.columns[1]);
        assert_eq!(grouping_values.len(), 1);
        assert_eq!(grouping_outputs.len(), 1);
        assert!(matches!(
            fragment.values().get(&root.output.columns[0]).unwrap().origin,
            ValueOrigin::NullExtended { node, .. } if node == root.id
        ));
    }

    #[test]
    fn table_function_separates_outer_passthrough_from_relation_results() {
        let outer = column(1, "outer", DataType::Int64, false);
        let result = column(2, "item", DataType::Utf8, true);
        let result_types = [novarocks_functions::FunctionValueType::new(
            DataType::Utf8,
            false,
        )];
        let binding = crate::optimizer::scalar::test_table_binding(
            &crate::optimizer::scalar::ScalarArena::new(),
            "unnest",
            &[],
            &result_types,
        );
        let plan = PhysicalPlanNode {
            kind: PhysicalPlanKind::TableFunction(PlanTableFunctionNode {
                function_name: "unnest".to_string(),
                args: Vec::new(),
                binding,
                output_columns: vec![result.clone()],
                alias: None,
                is_left_join: true,
            }),
            children: vec![values(vec![outer.clone()], vec![vec![literal_int(3)]])],
            output_columns: vec![outer, result],
            stats: stats(),
            probe_runtime_filters: Vec::new(),
        };

        let final_plan = finish_for_test(&plan).unwrap();
        let fragment = final_plan.fragments().get(&ROOT_FRAGMENT_ID).unwrap();
        let root = fragment.nodes().get(&fragment.root()).unwrap();
        let NodeKind::TableFunction {
            outputs,
            left_outer,
            function,
            ..
        } = &root.kind
        else {
            panic!("expected TableFunction");
        };
        assert!(*left_outer);
        assert_eq!(function.result_types.as_ref(), result_types);
        assert!(matches!(outputs[0], TableFunctionOutput::PassThrough(_)));
        assert!(matches!(
            outputs[1],
            TableFunctionOutput::FunctionResult {
                result_ordinal: 0,
                ..
            }
        ));
    }

    #[test]
    fn window_preserves_exact_function_order_and_frame() {
        let input = column(1, "k", DataType::Int64, false);
        let result = column(2, "rn", DataType::Int64, false);
        let binding =
            crate::analysis::test_window_binding("row_number", &[], DataType::Int64, false);
        let plan = PhysicalPlanNode {
            kind: PhysicalPlanKind::Window(PlanWindowNode {
                window_exprs: vec![WindowExpr {
                    name: "row_number".to_string(),
                    args: Vec::new(),
                    distinct: false,
                    binding: binding.clone(),
                    function_order_by: Vec::new(),
                    aggregate_binding: None,
                    partition_by: Vec::new(),
                    order_by: vec![sort_item(&input, false, true)],
                    window_frame: Some(crate::analysis::WindowFrame {
                        frame_type: crate::analysis::WindowFrameType::Rows,
                        start: crate::analysis::WindowBound::UnboundedPreceding,
                        end: crate::analysis::WindowBound::CurrentRow,
                    }),
                    result_type: DataType::Int64,
                    output_name: result.name.clone(),
                    output_column_id: result.column_id,
                    ignore_nulls: false,
                }],
                output_columns: vec![input.clone(), result.clone()],
            }),
            children: vec![values(vec![input], vec![vec![literal_int(9)]])],
            output_columns: vec![column(1, "k", DataType::Int64, false), result],
            stats: stats(),
            probe_runtime_filters: Vec::new(),
        };

        let final_plan = finish_for_test(&plan).unwrap();
        let fragment = final_plan.fragments().get(&ROOT_FRAGMENT_ID).unwrap();
        let root = fragment.nodes().get(&fragment.root()).unwrap();
        let NodeKind::Window(spec) = &root.kind else {
            panic!("expected Window");
        };
        let sort = fragment.nodes().get(&root.inputs[0]).unwrap();
        assert!(matches!(
            &sort.kind,
            NodeKind::Sort {
                mode: SortMode::Global,
                order_by,
            } if order_by.len() == 1
        ));
        assert_eq!(spec.order_by.len(), 1);
        assert_eq!(spec.order_by[0].direction, SortDirection::Descending);
        assert_eq!(spec.order_by[0].null_ordering, NullOrdering::First);
        let expression = fragment
            .expressions()
            .get(spec.expressions[0].expression)
            .unwrap();
        let ContractExprKind::WindowCall {
            function, frame, ..
        } = &expression.kind
        else {
            panic!("expected WindowCall");
        };
        assert_eq!(function.function_id, binding.function_id);
        assert!(matches!(
            frame,
            Some(ContractWindowFrame {
                units: WindowFrameUnits::Rows,
                start: ContractWindowBound::UnboundedPreceding,
                end: ContractWindowBound::CurrentRow,
                ..
            })
        ));
    }

    #[test]
    fn relational_nodes_reject_inexact_identity_and_mapping_facts() {
        let input = column(1, "k", DataType::Int64, false);

        let unpivot_output = column(2, "value", DataType::Int64, false);
        let unpivot = PhysicalPlanNode {
            kind: PhysicalPlanKind::Unpivot(PlanUnpivotNode {
                passthrough_columns: Vec::new(),
                value_output_column_id: unpivot_output.column_id,
                literal_output_column_ids: Vec::new(),
                value_mappings: vec![PlanUnpivotValueMapping {
                    input_value_column_id: ColumnId::new_for_test(99),
                    constants: Vec::new(),
                }],
                output_columns: vec![unpivot_output.clone()],
                max_output_rows: 1,
                max_output_bytes: 1,
            }),
            children: vec![values(vec![input.clone()], vec![vec![literal_int(1)]])],
            output_columns: vec![unpivot_output],
            stats: stats(),
            probe_runtime_filters: Vec::new(),
        };
        assert!(matches!(
            lower_final_physical_plan(&unpivot, version(), dop()),
            Err(ContractLoweringError::InvalidUnpivot { .. })
        ));

        let repeat = PhysicalPlanNode {
            kind: PhysicalPlanKind::Repeat(PlanRepeatNode {
                repeat_column_ref_list: vec![vec!["k".to_string()]],
                repeat_column_ref_ids: vec![vec![input.column_id]],
                grouping_ids: vec![1],
                all_rollup_columns: vec!["k".to_string()],
                all_rollup_column_ids: vec![input.column_id],
                grouping_key_aliases: Vec::new(),
                grouping_fn_args: Vec::new(),
                grouping_fn_arg_ids: Vec::new(),
                grouping_fn_ids: Vec::new(),
                virtual_tuple_id: None,
            }),
            children: vec![values(vec![input.clone()], vec![vec![literal_int(1)]])],
            output_columns: vec![input.clone()],
            stats: stats(),
            probe_runtime_filters: Vec::new(),
        };
        assert!(matches!(
            lower_final_physical_plan(&repeat, version(), dop()),
            Err(ContractLoweringError::InvalidRepeat { .. })
        ));

        let table_result = column(3, "item", DataType::Int64, false);
        let scalar_binding = crate::analysis::test_function_binding(
            "not_a_table_function",
            &[],
            DataType::Int64,
            false,
            novarocks_functions::FunctionVolatility::Immutable,
        );
        let table_function = PhysicalPlanNode {
            kind: PhysicalPlanKind::TableFunction(PlanTableFunctionNode {
                function_name: "not_a_table_function".to_string(),
                args: Vec::new(),
                binding: scalar_binding.clone(),
                output_columns: vec![table_result.clone()],
                alias: None,
                is_left_join: false,
            }),
            children: vec![values(vec![input.clone()], vec![vec![literal_int(1)]])],
            output_columns: vec![input.clone(), table_result],
            stats: stats(),
            probe_runtime_filters: Vec::new(),
        };
        assert!(matches!(
            lower_final_physical_plan(&table_function, version(), dop()),
            Err(ContractLoweringError::InvalidTableFunction { .. })
        ));

        let window_result = column(4, "rn", DataType::Int64, false);
        let window = PhysicalPlanNode {
            kind: PhysicalPlanKind::Window(PlanWindowNode {
                window_exprs: vec![WindowExpr {
                    name: "not_a_window_function".to_string(),
                    args: Vec::new(),
                    distinct: false,
                    binding: scalar_binding,
                    function_order_by: Vec::new(),
                    aggregate_binding: None,
                    partition_by: Vec::new(),
                    order_by: Vec::new(),
                    window_frame: None,
                    result_type: DataType::Int64,
                    output_name: window_result.name.clone(),
                    output_column_id: window_result.column_id,
                    ignore_nulls: false,
                }],
                output_columns: vec![input.clone(), window_result.clone()],
            }),
            children: vec![values(vec![input.clone()], vec![vec![literal_int(1)]])],
            output_columns: vec![input, window_result],
            stats: stats(),
            probe_runtime_filters: Vec::new(),
        };
        assert!(matches!(
            lower_final_physical_plan(&window, version(), dop()),
            Err(ContractLoweringError::InvalidWindow { .. })
        ));
    }

    #[test]
    fn malformed_change_event_expand_fails_closed() {
        let unsupported = PhysicalPlanNode {
            kind: PhysicalPlanKind::ChangeEventExpand(DistributedChangeEventExpandNode {
                events: Vec::new(),
                output_columns: Vec::new(),
                effect_column_id: ColumnId::new_for_test(1),
            }),
            children: Vec::new(),
            output_columns: Vec::new(),
            stats: stats(),
            probe_runtime_filters: Vec::new(),
        };

        assert!(lower_final_physical_plan(&unsupported, version(), dop()).is_err());
    }

    #[test]
    fn hash_join_unknown_distribution_fails_closed() {
        let left = column(1, "left_key", DataType::Int64, false);
        let right = column(2, "right_key", DataType::Int64, false);
        let hash_join = hash_join(
            crate::common::JoinKind::Inner,
            PhysicalHashJoinBuildSide::Right,
            SqlJoinDistribution::Unknown,
            None,
            values(vec![left.clone()], vec![vec![literal_int(1)]]),
            values(vec![right.clone()], vec![vec![literal_int(1)]]),
            vec![left, right],
        );
        assert!(matches!(
            lower_final_physical_plan(&hash_join, version(), dop())
                .err()
                .expect("unknown distribution must fail"),
            ContractLoweringError::MissingPlannerFact {
                node: "HashJoin",
                fact: "exact distribution mode",
            }
        ));
    }

    #[test]
    fn colocated_hash_join_requires_matching_bucket_proof() {
        let left = column(1, "left_key", DataType::Int64, false);
        let right = column(2, "right_key", DataType::Int64, false);
        let hash_join = hash_join(
            crate::common::JoinKind::Inner,
            PhysicalHashJoinBuildSide::Right,
            SqlJoinDistribution::Colocate,
            Some(JoinExecutionMode::Colocate),
            values(vec![left.clone()], vec![vec![literal_int(1)]]),
            values(vec![right.clone()], vec![vec![literal_int(1)]]),
            vec![left, right],
        );
        assert!(matches!(
            lower_final_physical_plan(&hash_join, version(), dop())
                .err()
                .expect("colocate without bucket proof must fail"),
            ContractLoweringError::MissingPlannerFact {
                node: "HashJoin",
                fact: "matching key-aligned bucket partition schemes on both inputs",
            }
        ));
    }

    #[test]
    fn broadcast_hash_join_uses_an_explicit_build_exchange() {
        let left = column(1, "left_key", DataType::Int64, false);
        let right = column(2, "right_key", DataType::Int64, false);
        let hash_join = hash_join(
            crate::common::JoinKind::Inner,
            PhysicalHashJoinBuildSide::Right,
            SqlJoinDistribution::Broadcast,
            Some(JoinExecutionMode::Broadcast),
            values(vec![left.clone()], vec![vec![literal_int(1)]]),
            redistribute(
                values(vec![right.clone()], vec![vec![literal_int(1)]]),
                RedistributeMode::Broadcast,
            ),
            vec![left, right],
        );
        let final_plan = finish_for_test(&hash_join).unwrap();
        let fragment = final_plan.fragments().get(&ROOT_FRAGMENT_ID).unwrap();
        let root = fragment.nodes().get(&fragment.root()).unwrap();
        assert!(matches!(
            root.kind,
            NodeKind::HashJoin {
                build_side: ContractJoinSide::Right,
                distribution: ContractJoinDistribution::BroadcastBuild,
                ..
            }
        ));
        assert!(final_plan.edges().values().any(|edge| {
            edge.partitioning.destination == Distribution::Broadcast
                && edge.partitioning.destination_multiplicity == RowMultiplicity::Replicated
        }));
    }

    #[test]
    fn shuffle_hash_join_allocates_one_shared_key_aligned_scheme() {
        let left = column(1, "left_key", DataType::Int64, false);
        let right = column(2, "right_key", DataType::Int64, false);
        let hash_join = hash_join(
            crate::common::JoinKind::Inner,
            PhysicalHashJoinBuildSide::Right,
            SqlJoinDistribution::Shuffle,
            Some(JoinExecutionMode::Partitioned),
            redistribute(
                values(vec![left.clone()], vec![vec![literal_int(1)]]),
                RedistributeMode::Hash {
                    cols: vec![left.column_id],
                    source: crate::planner::physical::HashSource::ShuffleJoin,
                },
            ),
            redistribute(
                values(vec![right.clone()], vec![vec![literal_int(1)]]),
                RedistributeMode::Hash {
                    cols: vec![right.column_id],
                    source: crate::planner::physical::HashSource::ShuffleJoin,
                },
            ),
            vec![left, right],
        );
        let final_plan = finish_for_test(&hash_join).unwrap();
        let fragment = final_plan.fragments().get(&ROOT_FRAGMENT_ID).unwrap();
        let root = fragment.nodes().get(&fragment.root()).unwrap();
        let NodeKind::HashJoin {
            distribution: ContractJoinDistribution::Partitioned,
            ..
        } = &root.kind
        else {
            panic!("expected partitioned hash join")
        };
        let left_scheme = match &root.required_inputs[0].distribution {
            Distribution::Hash { scheme, .. } => scheme,
            other => panic!("expected left hash distribution, got {other:?}"),
        };
        let right_scheme = match &root.required_inputs[1].distribution {
            Distribution::Hash { scheme, .. } => scheme,
            other => panic!("expected right hash distribution, got {other:?}"),
        };
        assert_eq!(left_scheme, right_scheme);
    }

    #[test]
    fn shared_shuffle_imports_preserve_hidden_join_key_display_names() {
        for kind in [
            crate::common::JoinKind::Inner,
            crate::common::JoinKind::LeftOuter,
            crate::common::JoinKind::LeftSemi,
            crate::common::JoinKind::LeftAnti,
        ] {
            let left = column(1, "l.k", DataType::Int64, false);
            let right = column(2, "r.k", DataType::Int64, false);
            let mut output = vec![left.clone()];
            if matches!(
                kind,
                crate::common::JoinKind::Inner | crate::common::JoinKind::LeftOuter
            ) {
                let mut visible_right = right.clone();
                visible_right.nullable = kind == crate::common::JoinKind::LeftOuter;
                output.push(visible_right);
            }
            let plan = hash_join(
                kind,
                PhysicalHashJoinBuildSide::Right,
                SqlJoinDistribution::Shuffle,
                Some(JoinExecutionMode::Partitioned),
                redistribute(
                    values(vec![left.clone()], vec![vec![literal_int(1)]]),
                    RedistributeMode::Hash {
                        cols: vec![left.column_id],
                        source: crate::planner::physical::HashSource::ShuffleJoin,
                    },
                ),
                redistribute(
                    values(vec![right.clone()], vec![vec![literal_int(1)]]),
                    RedistributeMode::Hash {
                        cols: vec![right.column_id],
                        source: crate::planner::physical::HashSource::ShuffleJoin,
                    },
                ),
                output,
            );
            let completed = finish_for_test(&plan).unwrap();
            let text = crate::explain::completed_tree::render_completed_plan_tree(
                &completed,
                crate::explain::ExplainLevel::Verbose,
            )
            .unwrap()
            .join("\n");
            assert!(text.contains("eq: [l.k = r.k]"), "{kind:?}: {text}");
            // Check the import contract itself rather than just the final text:
            // each new receiver identity carries exactly its sender's name.
            for edge in completed.edges().values() {
                for (source, destination) in edge.destination.receive_mapping.iter() {
                    let name = |fragment, value| {
                        completed
                            .annotations()
                            .iter()
                            .find(|annotation| {
                                annotation.subject
                                    == novarocks_physical_plan::AnnotationSubject::Value(
                                        fragment, value,
                                    )
                                    && annotation.key.as_ref() == "sql.display_name"
                            })
                            .map(|annotation| annotation.value.as_ref())
                            .expect("named exchange value")
                    };
                    assert_eq!(
                        name(edge.source.fragment, *source),
                        name(edge.destination.fragment, *destination)
                    );
                }
            }
        }
    }

    #[test]
    fn singleton_outer_hash_join_finishes_with_explicit_cast_expression_key() {
        for kind in [
            crate::common::JoinKind::RightOuter,
            crate::common::JoinKind::FullOuter,
        ] {
            let left = column(1, "left_key", DataType::Int64, false);
            let right = column(2, "right_key", DataType::Int32, false);
            let mut output_left = left.clone();
            output_left.nullable = true;
            let mut output_right = right.clone();
            if kind == crate::common::JoinKind::FullOuter {
                output_right.nullable = true;
            }
            let mut hash_join = hash_join(
                kind,
                PhysicalHashJoinBuildSide::Right,
                SqlJoinDistribution::Singleton,
                Some(JoinExecutionMode::Singleton),
                values(vec![left], vec![vec![literal_int(1)]]),
                values(vec![right.clone()], vec![vec![literal_int32(1)]]),
                vec![output_left, output_right],
            );
            let PhysicalPlanKind::HashJoin(join) = &mut hash_join.kind else {
                unreachable!("hash_join fixture always constructs HashJoin")
            };
            join.eq_conditions[0].right = TypedExpr {
                kind: ExprKind::Cast {
                    expr: Box::new(column_ref(&right)),
                    target: DataType::Int64,
                },
                data_type: DataType::Int64,
                nullable: false,
            };

            let final_plan = finish_for_test(&hash_join).expect("singleton outer join must finish");
            let fragment = final_plan.fragments().get(&ROOT_FRAGMENT_ID).unwrap();
            let root = fragment.nodes().get(&fragment.root()).unwrap();
            assert!(matches!(
                root.kind,
                NodeKind::HashJoin {
                    distribution: ContractJoinDistribution::Singleton,
                    ..
                }
            ));
            assert!(root.required_inputs.iter().all(|required| {
                required.distribution == Distribution::Singleton
                    && required.row_multiplicity == RowMultiplicity::SingleCopy
            }));
            assert_eq!(root.output_properties.distribution, Distribution::Singleton);
            assert_eq!(
                root.output_properties.row_multiplicity,
                RowMultiplicity::SingleCopy
            );
        }
    }

    #[test]
    fn singleton_right_outer_runtime_filter_finishes_with_exact_null_safe_witness() {
        let left = column(1, "left_key", DataType::Int64, false);
        let right = column(2, "right_key", DataType::Int64, false);
        let mut nullable_left = left.clone();
        nullable_left.nullable = true;
        let mut hash_join = hash_join(
            crate::common::JoinKind::RightOuter,
            PhysicalHashJoinBuildSide::Right,
            SqlJoinDistribution::Singleton,
            Some(JoinExecutionMode::Singleton),
            values(vec![left], vec![vec![literal_int(1)]]),
            values(vec![right.clone()], vec![vec![literal_int(1)]]),
            vec![nullable_left, right],
        );
        attach_singleton_join_runtime_filter(&mut hash_join, 7, true, true);

        let final_plan = finish_for_test(&hash_join).expect("runtime filter must finish");
        let filter = final_plan
            .runtime_filters()
            .get(&RuntimeFilterId::new(8))
            .expect("runtime filter minted one past the placement's own number");
        assert!(matches!(
            filter.domain,
            RuntimeFilterDomain::Membership {
                null_semantics: RuntimeFilterNullSemantics::NullSafeEqual,
                ..
            }
        ));
        assert!(matches!(
            filter.producers[0].target,
            RuntimeFilterProducerTarget::JoinBuildKey { .. }
        ));
        assert!(matches!(
            filter.consumers[0].target,
            RuntimeFilterConsumerTarget::JoinProbeKey { .. }
        ));
        assert_eq!(
            filter.consumers[0].activation,
            RuntimeFilterConsumerActivation::BlockingSnapshot
        );
        assert_eq!(
            filter.equality_witnesses[0].domain_side,
            ContractJoinSide::Right
        );
    }

    #[test]
    fn runtime_filter_wait_cycle_downgrades_only_the_cyclic_consumer() {
        let left = column(1, "left_key", DataType::Int64, false);
        let nested_build = column(2, "nested_build", DataType::Int64, false);
        let outer_probe = column(3, "outer_probe", DataType::Int64, false);
        let inner = hash_join(
            crate::common::JoinKind::Inner,
            PhysicalHashJoinBuildSide::Right,
            SqlJoinDistribution::Singleton,
            Some(JoinExecutionMode::Singleton),
            values(vec![left.clone()], vec![vec![literal_int(1)]]),
            values(vec![nested_build.clone()], vec![vec![literal_int(2)]]),
            vec![left.clone(), nested_build.clone()],
        );
        let outer = hash_join(
            crate::common::JoinKind::Inner,
            PhysicalHashJoinBuildSide::Left,
            SqlJoinDistribution::Singleton,
            Some(JoinExecutionMode::Singleton),
            inner,
            values(vec![outer_probe.clone()], vec![vec![literal_int(3)]]),
            vec![left, nested_build, outer_probe],
        );
        let plan = finish_for_test(&outer).expect("nested singleton joins must finish");
        let fragment = plan.fragments().get(&ROOT_FRAGMENT_ID).unwrap();
        let outer_node = fragment.nodes().get(&fragment.root()).unwrap();
        let inner_node = fragment.nodes().get(&outer_node.inputs[0]).unwrap();
        let filter_id = RuntimeFilterId::new(44);
        let witness = RuntimeFilterWitnessId::new(44);
        let equality = RuntimeFilterEqualityWitnessId::new(44);
        let mut filters = vec![RuntimeFilter {
            id: filter_id,
            kind: RuntimeFilterKind::InList,
            domain: RuntimeFilterDomain::Membership {
                ty: novarocks_physical_plan::ValueType::new(DataType::Int64, false),
                null_semantics: RuntimeFilterNullSemantics::NeverMatches,
            },
            lifecycle: RuntimeFilterLifecycle::CompleteOnce,
            reduction: RuntimeFilterReduction::SetUnion,
            availability_coverage: leaf_runtime_filter_witness(witness),
            terminal_coverage: leaf_runtime_filter_witness(witness),
            equality_witnesses: Box::default(),
            producers: Box::from([RuntimeFilterProducer {
                witness,
                endpoint: RuntimeFilterEndpoint {
                    fragment: ROOT_FRAGMENT_ID,
                    node: outer_node.id,
                    values: Box::from([inner_node.output.columns[0]]),
                },
                apply_point: novarocks_physical_plan::RuntimeFilterApplyPoint::NodeInput {
                    input_ordinal: 0,
                },
                contribution_kinds: Box::default(),
                completion: RuntimeFilterCompletion::ProducerClosed,
                progress: RuntimeFilterProducerProgress {
                    build_edges: Box::default(),
                    non_build_edges: Box::default(),
                },
                target: RuntimeFilterProducerTarget::JoinBuildKey { equality },
            }]),
            consumers: Box::from([RuntimeFilterConsumer {
                endpoint: RuntimeFilterEndpoint {
                    fragment: ROOT_FRAGMENT_ID,
                    node: inner_node.id,
                    values: Box::from([inner_node.output.columns[0]]),
                },
                apply_point: novarocks_physical_plan::RuntimeFilterApplyPoint::NodeInput {
                    input_ordinal: 0,
                },
                capabilities: Box::default(),
                activation: RuntimeFilterConsumerActivation::BlockingSnapshot,
                target: RuntimeFilterConsumerTarget::JoinProbeKey { equality },
            }]),
            policy: RuntimeFilterPolicy {
                max_contribution_bytes: 1024,
                max_artifact_bytes: 4096,
                deadline_ms: 30_000,
                max_retries: 3,
            },
        }];

        resolve_runtime_filter_activations(&mut filters, plan.fragments(), plan.edges());

        assert_eq!(
            filters[0].consumers[0].activation,
            RuntimeFilterConsumerActivation::StartUnfilteredThenApplyComplete {
                late_apply: novarocks_physical_plan::LateApplyGranularity::Batch,
            }
        );
    }

    #[test]
    fn runtime_filter_producer_without_probe_witness_fails_before_publication() {
        let left = column(1, "left_key", DataType::Int64, false);
        let right = column(2, "right_key", DataType::Int64, false);
        let mut hash_join = hash_join(
            crate::common::JoinKind::Inner,
            PhysicalHashJoinBuildSide::Right,
            SqlJoinDistribution::Singleton,
            Some(JoinExecutionMode::Singleton),
            values(vec![left.clone()], vec![vec![literal_int(1)]]),
            values(vec![right.clone()], vec![vec![literal_int(1)]]),
            vec![left, right],
        );
        attach_singleton_join_runtime_filter(&mut hash_join, 9, false, false);

        assert!(matches!(
            finish_for_test(&hash_join),
            Err(ContractLoweringError::InvalidRuntimeFilter { id: 9, detail })
                if detail == "producer has no static consumer witness"
        ));
    }

    #[test]
    fn right_semi_and_anti_hash_joins_preserve_explicit_left_build_side() {
        for kind in [
            crate::common::JoinKind::RightSemi,
            crate::common::JoinKind::RightAnti,
        ] {
            let left = column(1, "left_key", DataType::Int64, false);
            let right = column(2, "right_key", DataType::Int64, false);
            let hash_join = hash_join(
                kind,
                PhysicalHashJoinBuildSide::Left,
                SqlJoinDistribution::Shuffle,
                Some(JoinExecutionMode::Partitioned),
                redistribute(
                    values(vec![left.clone()], vec![vec![literal_int(1)]]),
                    RedistributeMode::Hash {
                        cols: vec![left.column_id],
                        source: crate::planner::physical::HashSource::ShuffleJoin,
                    },
                ),
                redistribute(
                    values(vec![right.clone()], vec![vec![literal_int(1)]]),
                    RedistributeMode::Hash {
                        cols: vec![right.column_id],
                        source: crate::planner::physical::HashSource::ShuffleJoin,
                    },
                ),
                vec![right],
            );
            let final_plan = finish_for_test(&hash_join).unwrap();
            let fragment = final_plan.fragments().get(&ROOT_FRAGMENT_ID).unwrap();
            let root = fragment.nodes().get(&fragment.root()).unwrap();
            assert!(matches!(
                root.kind,
                NodeKind::HashJoin {
                    build_side: ContractJoinSide::Left,
                    ..
                }
            ));
        }
    }

    #[test]
    fn left_outer_hash_join_defines_a_new_nullable_right_value() {
        let left = column(1, "left_key", DataType::Int64, false);
        let right = column(2, "right_key", DataType::Int64, false);
        let nullable_right = column(2, "right_key", DataType::Int64, true);
        let hash_join = hash_join(
            crate::common::JoinKind::LeftOuter,
            PhysicalHashJoinBuildSide::Right,
            SqlJoinDistribution::Broadcast,
            Some(JoinExecutionMode::Broadcast),
            values(vec![left.clone()], vec![vec![literal_int(1)]]),
            redistribute(
                values(vec![right], vec![vec![literal_int(1)]]),
                RedistributeMode::Broadcast,
            ),
            vec![left, nullable_right],
        );
        let final_plan = finish_for_test(&hash_join).unwrap();
        let fragment = final_plan.fragments().get(&ROOT_FRAGMENT_ID).unwrap();
        let root = fragment.nodes().get(&fragment.root()).unwrap();
        let NodeKind::HashJoin { null_extended, .. } = &root.kind else {
            panic!("expected hash join")
        };
        assert_eq!(null_extended.len(), 1);
        let definition = fragment.values().get(&null_extended[0]).unwrap();
        assert!(definition.ty.nullable);
        assert!(matches!(
            definition.origin,
            ValueOrigin::NullExtended { node, .. } if node == root.id
        ));
    }

    #[test]
    fn lowers_global_sort_and_limit_with_exact_order_and_row_counts() {
        let input = column(1, "number", DataType::Int64, false);
        let sort = PhysicalPlanNode {
            kind: PhysicalPlanKind::Sort(PlanSortNode {
                items: vec![sort_item(&input, false, true)],
                analytic_partition_by: Vec::new(),
                output_columns: vec![input.clone()],
                offset: None,
                partition_limit: None,
                topn_type: None,
            }),
            children: vec![values(vec![input.clone()], vec![vec![literal_int(7)]])],
            output_columns: vec![input.clone()],
            stats: stats(),
            probe_runtime_filters: Vec::new(),
        };
        let limit = PhysicalPlanNode {
            kind: PhysicalPlanKind::Limit(PlanLimitNode {
                limit: Some(5),
                offset: Some(2),
            }),
            children: vec![sort],
            output_columns: vec![input],
            stats: stats(),
            probe_runtime_filters: Vec::new(),
        };

        let final_plan = finish_for_test(&limit).unwrap();
        let fragment = final_plan.fragments().get(&ROOT_FRAGMENT_ID).unwrap();
        let root = fragment.nodes().get(&fragment.root()).unwrap();
        assert!(matches!(
            root.kind,
            NodeKind::Limit {
                limit: Some(5),
                offset: 2
            }
        ));
        let sort = fragment.nodes().get(&root.inputs[0]).unwrap();
        let NodeKind::Sort { order_by, mode } = &sort.kind else {
            panic!("expected Sort below Limit");
        };
        assert_eq!(mode, &SortMode::Global);
        assert_eq!(order_by[0].direction, SortDirection::Descending);
        assert_eq!(order_by[0].null_ordering, NullOrdering::First);
        assert_eq!(
            sort.output_properties.ordering[0].value,
            sort.output.columns[0]
        );
    }

    #[test]
    fn lowers_unsplit_final_topn_as_a_single_exact_phase() {
        let input = column(1, "number", DataType::Int64, false);
        let topn = PhysicalPlanNode {
            kind: PhysicalPlanKind::TopN(PhysicalTopNNode {
                items: vec![sort_item(&input, true, false)],
                limit: Some(10),
                offset: Some(3),
                phase: SqlTopNPhase::Final,
                is_split: false,
            }),
            children: vec![values(vec![input.clone()], vec![vec![literal_int(7)]])],
            output_columns: vec![input],
            stats: stats(),
            probe_runtime_filters: Vec::new(),
        };

        let final_plan = finish_for_test(&topn).unwrap();
        let fragment = final_plan.fragments().get(&ROOT_FRAGMENT_ID).unwrap();
        let root = fragment.nodes().get(&fragment.root()).unwrap();
        assert!(matches!(
            root.kind,
            NodeKind::TopN {
                limit: 10,
                offset: 3,
                phase: ContractTopNPhase::Single,
                ..
            }
        ));
    }

    #[test]
    fn split_topn_uses_one_shared_sequence_across_a_gather_edge() {
        let input = column(1, "number", DataType::Int64, false);
        // The planner splits a TopN into two nodes of its own, the way
        // `SplitTopN` does: a partial that prunes and a final above it.
        let partial = PhysicalPlanNode {
            kind: PhysicalPlanKind::TopN(PhysicalTopNNode {
                items: vec![sort_item(&input, true, false)],
                limit: Some(10),
                offset: Some(0),
                phase: SqlTopNPhase::Partial,
                is_split: false,
            }),
            children: vec![values(vec![input.clone()], vec![vec![literal_int(7)]])],
            output_columns: vec![input.clone()],
            stats: stats(),
            probe_runtime_filters: Vec::new(),
        };
        let split = PhysicalPlanNode {
            kind: PhysicalPlanKind::TopN(PhysicalTopNNode {
                items: vec![sort_item(&input, true, false)],
                limit: Some(10),
                offset: None,
                phase: SqlTopNPhase::Final,
                is_split: true,
            }),
            children: vec![partial],
            output_columns: vec![input.clone()],
            stats: stats(),
            probe_runtime_filters: Vec::new(),
        };
        let final_plan = finish_for_test(&split).unwrap();
        assert_eq!(final_plan.fragments().len(), 2);
        assert_eq!(final_plan.edges().len(), 1);
        let edge = final_plan.edges().values().next().unwrap();
        assert_eq!(edge.partitioning.destination, Distribution::Singleton);
        let source = final_plan.fragments().get(&edge.source.fragment).unwrap();
        let destination = final_plan
            .fragments()
            .get(&edge.destination.fragment)
            .unwrap();
        let NodeKind::TopN {
            limit: 10,
            offset: 0,
            phase: ContractTopNPhase::Partial { sequence: partial },
            ..
        } = source.nodes().get(&source.root()).unwrap().kind
        else {
            panic!("expected partial TopN at source fragment root");
        };
        let NodeKind::TopN {
            limit: 10,
            offset: 0,
            phase:
                ContractTopNPhase::Final {
                    sequence: final_sequence,
                },
            ..
        } = destination.nodes().get(&destination.root()).unwrap().kind
        else {
            panic!("expected final TopN at destination fragment root");
        };
        assert_eq!(partial, final_sequence);
    }

    #[test]
    fn partition_sort_fails_without_exact_ordering_facts() {
        let input = column(1, "number", DataType::Int64, false);

        let partition_sort = PhysicalPlanNode {
            kind: PhysicalPlanKind::Sort(PlanSortNode {
                items: vec![sort_item(&input, true, false)],
                analytic_partition_by: vec![column_ref(&input)],
                output_columns: vec![input.clone()],
                offset: None,
                partition_limit: None,
                topn_type: None,
            }),
            children: vec![values(vec![input.clone()], vec![vec![literal_int(7)]])],
            output_columns: vec![input],
            stats: stats(),
            probe_runtime_filters: Vec::new(),
        };
        assert!(matches!(
            lower_final_physical_plan(&partition_sort, version(), dop())
                .err()
                .expect("partition sort must fail"),
            ContractLoweringError::UnsupportedSortMode { .. }
        ));
    }

    #[test]
    fn gather_edge_preserves_repeated_occurrences_and_import_identity() {
        let input = column(1, "number", DataType::Int64, false);
        let repeated = PhysicalPlanNode {
            kind: PhysicalPlanKind::Project(PlanProjectNode {
                items: vec![
                    ProjectItem {
                        expr: column_ref(&input),
                        output_name: "left".into(),
                        output_column_id: input.column_id,
                    },
                    ProjectItem {
                        expr: column_ref(&input),
                        output_name: "right".into(),
                        output_column_id: input.column_id,
                    },
                ],
                output_qualifier: None,
            }),
            children: vec![values(vec![input.clone()], vec![vec![literal_int(7)]])],
            output_columns: vec![input.clone(), input.clone()],
            stats: stats(),
            probe_runtime_filters: Vec::new(),
        };
        let gather = PhysicalPlanNode {
            kind: PhysicalPlanKind::Redistribute(crate::planner::physical::RedistributeNode {
                mode: RedistributeMode::Gather,
                partition_exprs: Vec::new(),
                output_columns: vec![input.clone(), input.clone()],
            }),
            children: vec![repeated],
            output_columns: vec![input.clone(), input],
            stats: stats(),
            probe_runtime_filters: Vec::new(),
        };
        let final_plan = finish_for_test(&gather).unwrap();
        let edge = final_plan.edges().values().next().unwrap();
        assert_eq!(edge.source.projection[0], edge.source.projection[1]);
        assert_eq!(
            edge.destination.receive_mapping[0].1,
            edge.destination.receive_mapping[1].1
        );
        let result = final_plan.result_port().unwrap();
        assert_eq!(result.fields[0].name.as_ref(), "number");
        assert_eq!(result.fields[1].name.as_ref(), "number");
        assert_eq!(result.fields[0].alias.as_deref(), Some("left"));
        assert_eq!(result.fields[1].alias.as_deref(), Some("right"));
        assert_eq!(result.fields[0].value, result.fields[1].value);
    }

    #[test]
    fn hash_edge_carries_one_exact_plan_local_partition_scheme() {
        let input = column(1, "number", DataType::Int64, false);
        let hash = PhysicalPlanNode {
            kind: PhysicalPlanKind::Redistribute(crate::planner::physical::RedistributeNode {
                mode: RedistributeMode::Hash {
                    cols: vec![input.column_id],
                    source: crate::planner::physical::HashSource::ShuffleJoin,
                },
                partition_exprs: vec![column_ref(&input)],
                output_columns: vec![input.clone()],
            }),
            children: vec![values(vec![input.clone()], vec![vec![literal_int(7)]])],
            output_columns: vec![input],
            stats: stats(),
            probe_runtime_filters: Vec::new(),
        };
        let final_plan = finish_for_test(&hash).unwrap();
        let edge = final_plan.edges().values().next().unwrap();
        let Distribution::Hash {
            keys: source_keys,
            scheme: source_scheme,
        } = &edge.partitioning.source
        else {
            panic!("expected hash source partitioning");
        };
        let Distribution::Hash {
            keys: destination_keys,
            scheme: destination_scheme,
        } = &edge.partitioning.destination
        else {
            panic!("expected hash destination partitioning");
        };
        assert_eq!(source_scheme, destination_scheme);
        assert_eq!(source_scheme.count.admissible.min, dop().min);
        assert_eq!(source_scheme.count.admissible.max, dop().max);
        assert_eq!(source_keys[0], edge.destination.receive_mapping[0].0);
        assert_eq!(destination_keys[0], edge.destination.receive_mapping[0].1);
    }

    #[test]
    fn broadcast_project_and_filter_require_replica_deterministic_expressions() {
        let input = column(1, "number", DataType::Int64, false);
        let broadcast = || {
            redistribute(
                values(vec![input.clone()], vec![vec![literal_int(7)]]),
                RedistributeMode::Broadcast,
            )
        };
        let project = |volatility| {
            let output = column(2, "computed", DataType::Int64, false);
            PhysicalPlanNode {
                kind: PhysicalPlanKind::Project(PlanProjectNode {
                    items: vec![ProjectItem {
                        expr: scalar_call(
                            "project_value",
                            vec![column_ref(&input)],
                            DataType::Int64,
                            volatility,
                        ),
                        output_name: output.name.clone(),
                        output_column_id: output.column_id,
                    }],
                    output_qualifier: None,
                }),
                children: vec![broadcast()],
                output_columns: vec![output],
                stats: stats(),
                probe_runtime_filters: Vec::new(),
            }
        };
        let filter = |volatility| PhysicalPlanNode {
            kind: PhysicalPlanKind::Filter(PlanFilterNode {
                predicate: scalar_call("filter_value", Vec::new(), DataType::Boolean, volatility),
            }),
            children: vec![broadcast()],
            output_columns: vec![input.clone()],
            stats: stats(),
            probe_runtime_filters: Vec::new(),
        };
        let root_distribution = |plan: PhysicalPlanNode| {
            let mut visitor = ContractLoweringVisitor::new(version(), dop(), None);
            visitor
                .lower_node(&plan)
                .expect("tested operator must lower")
                .properties
                .distribution
        };

        assert_eq!(
            root_distribution(project(novarocks_functions::FunctionVolatility::Immutable)),
            Distribution::Broadcast
        );
        assert_eq!(
            root_distribution(project(novarocks_functions::FunctionVolatility::Stable)),
            Distribution::Unconstrained
        );
        assert_eq!(
            root_distribution(filter(novarocks_functions::FunctionVolatility::Immutable)),
            Distribution::Broadcast
        );
        assert_eq!(
            root_distribution(filter(novarocks_functions::FunctionVolatility::Volatile)),
            Distribution::Unconstrained
        );
    }

    #[test]
    fn project_drops_incomplete_hash_keys_and_keeps_the_ordering_prefix() {
        let first = column(1, "first", DataType::Int64, false);
        let second = column(2, "second", DataType::Int64, false);
        let project_first = |child| PhysicalPlanNode {
            kind: PhysicalPlanKind::Project(PlanProjectNode {
                items: vec![ProjectItem {
                    expr: column_ref(&first),
                    output_name: first.name.clone(),
                    output_column_id: first.column_id,
                }],
                output_qualifier: None,
            }),
            children: vec![child],
            output_columns: vec![first.clone()],
            stats: stats(),
            probe_runtime_filters: Vec::new(),
        };
        let hash_child = redistribute(
            values(
                vec![first.clone(), second.clone()],
                vec![vec![literal_int(1), literal_int(2)]],
            ),
            RedistributeMode::Hash {
                cols: vec![first.column_id, second.column_id],
                source: crate::planner::physical::HashSource::ShuffleJoin,
            },
        );
        let mut visitor = ContractLoweringVisitor::new(version(), dop(), None);
        let hash_project = visitor.lower_node(&project_first(hash_child)).unwrap();
        assert_eq!(
            hash_project.properties.distribution,
            Distribution::Unconstrained
        );

        let sort = PhysicalPlanNode {
            kind: PhysicalPlanKind::Sort(PlanSortNode {
                items: vec![
                    sort_item(&first, true, false),
                    sort_item(&second, false, true),
                ],
                offset: None,
                analytic_partition_by: Vec::new(),
                output_columns: vec![first.clone(), second.clone()],
                partition_limit: None,
                topn_type: None,
            }),
            children: vec![values(
                vec![first.clone(), second.clone()],
                vec![vec![literal_int(1), literal_int(2)]],
            )],
            output_columns: vec![first.clone(), second],
            stats: stats(),
            probe_runtime_filters: Vec::new(),
        };
        let mut visitor = ContractLoweringVisitor::new(version(), dop(), None);
        let ordered_project = visitor.lower_node(&project_first(sort)).unwrap();
        assert_eq!(ordered_project.properties.ordering.len(), 1);
        assert_eq!(
            ordered_project.properties.ordering[0].value,
            ordered_project.output[0]
        );
    }

    #[test]
    fn broadcast_nested_loop_left_outer_mints_nullable_value_identity() {
        let left = column(1, "left_key", DataType::Int64, false);
        let right = column(2, "right_key", DataType::Int64, false);
        let nullable_right = column(2, "right_key", DataType::Int64, true);
        let broadcast_right = PhysicalPlanNode {
            kind: PhysicalPlanKind::Redistribute(crate::planner::physical::RedistributeNode {
                mode: RedistributeMode::Broadcast,
                partition_exprs: Vec::new(),
                output_columns: vec![right.clone()],
            }),
            children: vec![values(vec![right.clone()], vec![vec![literal_int(9)]])],
            output_columns: vec![right],
            stats: stats(),
            probe_runtime_filters: Vec::new(),
        };
        let join = PhysicalPlanNode {
            kind: PhysicalPlanKind::NestLoopJoin(
                crate::planner::physical::PhysicalNestLoopJoinNode {
                    join_type: crate::common::JoinKind::LeftOuter,
                    condition: Some(literal_bool(true)),
                    output_columns: vec![left.clone(), nullable_right.clone()],
                },
            ),
            children: vec![
                values(vec![left.clone()], vec![vec![literal_int(7)]]),
                broadcast_right,
            ],
            output_columns: vec![left, nullable_right],
            stats: stats(),
            probe_runtime_filters: Vec::new(),
        };
        let final_plan = finish_for_test(&join).unwrap();
        let fragment = final_plan
            .fragments()
            .get(&final_plan.result_port().unwrap().fragment)
            .unwrap();
        let root = fragment.nodes().get(&fragment.root()).unwrap();
        let NodeKind::NestLoopJoin {
            distribution: NestLoopJoinDistribution::BroadcastRight,
            null_extended,
            ..
        } = &root.kind
        else {
            panic!("expected broadcast-right nested-loop join");
        };
        assert_eq!(null_extended.len(), 1);
        let ValueOrigin::NullExtended { node, of } = fragment
            .values()
            .get(&root.output.columns[1])
            .unwrap()
            .origin
        else {
            panic!("expected nullable join output identity");
        };
        assert_eq!(node, root.id);
        assert_ne!(of, root.output.columns[1]);
    }

    #[test]
    fn union_all_preserves_duplicate_output_occurrences() {
        let output = column(1, "number", DataType::Int64, false);
        let repeated_child = || PhysicalPlanNode {
            kind: PhysicalPlanKind::Project(PlanProjectNode {
                items: vec![
                    ProjectItem {
                        expr: column_ref(&output),
                        output_name: "first".into(),
                        output_column_id: output.column_id,
                    },
                    ProjectItem {
                        expr: column_ref(&output),
                        output_name: "second".into(),
                        output_column_id: output.column_id,
                    },
                ],
                output_qualifier: None,
            }),
            children: vec![values(vec![output.clone()], vec![vec![literal_int(7)]])],
            output_columns: vec![output.clone(), output.clone()],
            stats: stats(),
            probe_runtime_filters: Vec::new(),
        };
        let set_op = PhysicalPlanNode {
            kind: PhysicalPlanKind::SetOp(crate::planner::physical::PhysicalSetOpNode {
                kind: PlanSetOpKind::UnionAll,
                output_columns: vec![output.clone(), output.clone()],
                child_output_columns: vec![
                    vec![output.clone(), output.clone()],
                    vec![output.clone(), output.clone()],
                ],
            }),
            children: vec![repeated_child(), repeated_child()],
            output_columns: vec![output.clone(), output],
            stats: stats(),
            probe_runtime_filters: Vec::new(),
        };
        let final_plan = finish_for_test(&set_op).unwrap();
        let fragment = final_plan.fragments().get(&ROOT_FRAGMENT_ID).unwrap();
        let root = fragment.nodes().get(&fragment.root()).unwrap();
        let NodeKind::SetOp { input_mappings, .. } = &root.kind else {
            panic!("expected SetOp root");
        };
        assert_eq!(root.output.columns[0], root.output.columns[1]);
        assert_eq!(input_mappings[0][0], input_mappings[0][1]);
        assert_eq!(input_mappings[1][0], input_mappings[1][1]);
    }

    #[test]
    fn union_all_widens_output_after_a_physical_branch_becomes_nullable() {
        let left = column(1, "left", DataType::Int64, false);
        let right = column(2, "right", DataType::Int64, true);
        let published = column(3, "result", DataType::Int64, false);
        let set_op = PhysicalPlanNode {
            kind: PhysicalPlanKind::SetOp(crate::planner::physical::PhysicalSetOpNode {
                kind: PlanSetOpKind::UnionAll,
                output_columns: vec![published.clone()],
                child_output_columns: vec![vec![left.clone()], vec![right.clone()]],
            }),
            children: vec![
                values(vec![left], vec![vec![literal_int(7)]]),
                values(
                    vec![right],
                    vec![vec![TypedExpr {
                        kind: ExprKind::Literal(LiteralValue::Null),
                        data_type: DataType::Int64,
                        nullable: true,
                    }]],
                ),
            ],
            output_columns: vec![published],
            stats: stats(),
            probe_runtime_filters: Vec::new(),
        };
        let final_plan = finish_for_test(&set_op).unwrap();
        let fragment = final_plan.fragments().get(&ROOT_FRAGMENT_ID).unwrap();
        let root = fragment.nodes().get(&fragment.root()).unwrap();
        let NodeKind::SetOp { .. } = &root.kind else {
            panic!("expected SetOp root");
        };
        assert!(
            fragment
                .values()
                .get(&root.output.columns[0])
                .unwrap()
                .ty
                .nullable
        );
    }

    #[test]
    fn lowers_single_aggregate_with_exact_result_binding() {
        let input = column(1, "number", DataType::Int64, true);
        let resolved = crate::functions::test_resolved_aggregate("sum", &[DataType::Int64], false);
        let novarocks_functions::FunctionResultType::Scalar(result) =
            &resolved.selected.result_type
        else {
            unreachable!();
        };
        let output = column(2, "total", result.data_type.clone(), result.nullable);
        let aggregate = PhysicalPlanNode {
            kind: PhysicalPlanKind::HashAggregate(Box::new(
                crate::planner::physical::PhysicalHashAggregateNode {
                    mode: AggMode::Single,
                    group_by: Vec::new(),
                    aggregates: vec![aggregate_call(column_ref(&input), output.column_id)],
                    is_merge: vec![false],
                    output_layout: crate::planner::physical::AggregateOutputLayout::new(
                        Vec::new(),
                        vec![output.clone()],
                    ),
                    output_columns: vec![output.clone()],
                    topn_runtime_filter_builds: Vec::new(),
                },
            )),
            children: vec![values(
                vec![input],
                vec![vec![TypedExpr {
                    kind: ExprKind::Literal(LiteralValue::Null),
                    data_type: DataType::Int64,
                    nullable: true,
                }]],
            )],
            output_columns: vec![output],
            stats: stats(),
            probe_runtime_filters: Vec::new(),
        };
        let final_plan = finish_for_test(&aggregate).unwrap();
        let fragment = final_plan.fragments().get(&ROOT_FRAGMENT_ID).unwrap();
        let root = fragment.nodes().get(&fragment.root()).unwrap();
        let NodeKind::Aggregate { calls, .. } = &root.kind else {
            panic!("expected Aggregate root");
        };
        assert!(matches!(calls[0].binding.phase, AggregatePhase::Single));
        assert!(matches!(
            fragment.values().get(&calls[0].output).unwrap().origin,
            ValueOrigin::AggregateResult { call } if call == calls[0].id
        ));
    }

    #[test]
    fn local_and_global_aggregate_share_exact_per_call_sequence() {
        let input = column(1, "number", DataType::Int64, true);
        let resolved = crate::functions::test_resolved_aggregate("sum", &[DataType::Int64], false);
        let aggregate_selection = resolved.selected.aggregate.as_ref().unwrap();
        let state = column(
            2,
            "sum_state",
            aggregate_selection.intermediate_type.data_type.clone(),
            aggregate_selection.intermediate_type.nullable,
        );
        let novarocks_functions::FunctionResultType::Scalar(result) =
            &resolved.selected.result_type
        else {
            unreachable!();
        };
        let output = column(3, "total", result.data_type.clone(), result.nullable);
        let local = PhysicalPlanNode {
            kind: PhysicalPlanKind::HashAggregate(Box::new(
                crate::planner::physical::PhysicalHashAggregateNode {
                    mode: AggMode::Local,
                    group_by: Vec::new(),
                    aggregates: vec![aggregate_call(column_ref(&input), state.column_id)],
                    is_merge: vec![false],
                    output_layout: crate::planner::physical::AggregateOutputLayout::new(
                        Vec::new(),
                        vec![state.clone()],
                    ),
                    output_columns: vec![state.clone()],
                    topn_runtime_filter_builds: Vec::new(),
                },
            )),
            children: vec![values(
                vec![input],
                vec![vec![TypedExpr {
                    kind: ExprKind::Literal(LiteralValue::Null),
                    data_type: DataType::Int64,
                    nullable: true,
                }]],
            )],
            output_columns: vec![state.clone()],
            stats: stats(),
            probe_runtime_filters: Vec::new(),
        };
        let gather = PhysicalPlanNode {
            kind: PhysicalPlanKind::Redistribute(crate::planner::physical::RedistributeNode {
                mode: RedistributeMode::Gather,
                partition_exprs: Vec::new(),
                output_columns: vec![state.clone()],
            }),
            children: vec![local],
            output_columns: vec![state.clone()],
            stats: stats(),
            probe_runtime_filters: Vec::new(),
        };
        let global = PhysicalPlanNode {
            kind: PhysicalPlanKind::HashAggregate(Box::new(
                crate::planner::physical::PhysicalHashAggregateNode {
                    mode: AggMode::Global,
                    group_by: Vec::new(),
                    aggregates: vec![aggregate_call(column_ref(&state), output.column_id)],
                    is_merge: vec![true],
                    output_layout: crate::planner::physical::AggregateOutputLayout::new(
                        Vec::new(),
                        vec![output.clone()],
                    ),
                    output_columns: vec![output.clone()],
                    topn_runtime_filter_builds: Vec::new(),
                },
            )),
            children: vec![gather],
            output_columns: vec![output],
            stats: stats(),
            probe_runtime_filters: Vec::new(),
        };
        let final_plan = finish_for_test(&global).unwrap();
        let phases = final_plan
            .fragments()
            .values()
            .flat_map(|fragment| fragment.nodes().values())
            .filter_map(|node| match &node.kind {
                NodeKind::Aggregate { calls, .. } => Some(calls[0].binding.phase),
                _ => None,
            })
            .collect::<Vec<_>>();
        let partial = phases.iter().find_map(|phase| match phase {
            AggregatePhase::Partial { sequence } => Some(*sequence),
            _ => None,
        });
        let final_sequence = phases.iter().find_map(|phase| match phase {
            AggregatePhase::Final { sequence } => Some(*sequence),
            _ => None,
        });
        assert_eq!(partial, final_sequence);
        assert!(partial.is_some());
    }

    #[test]
    fn lowers_global_and_keyed_assertions_without_losing_their_subjects() {
        let input = column(1, "account_id", DataType::Int64, false);
        let global = PhysicalPlanNode {
            kind: PhysicalPlanKind::AssertOneRow(PlanAssertOneRowNode {
                subquery_text: "select account_id from accounts".to_string(),
                desired_num_rows: Some(1),
                assertion: PlanRowCountAssertion::Le,
                group_key_column_ids: Vec::new(),
                group_key_labels: Vec::new(),
                keyed_message_prefix: None,
            }),
            children: vec![values(vec![input.clone()], vec![vec![literal_int(7)]])],
            output_columns: vec![input.clone()],
            stats: stats(),
            probe_runtime_filters: Vec::new(),
        };
        let final_plan = finish_for_test(&global).unwrap();
        let fragment = final_plan.fragments().get(&ROOT_FRAGMENT_ID).unwrap();
        assert!(matches!(
            &fragment.nodes().get(&fragment.root()).unwrap().kind,
            NodeKind::AssertOneRow(RowCountAssertionSpec::Global {
                subject,
                desired_rows: 1,
                comparison: RowCountAssertion::Le,
            }) if subject.as_ref() == "select account_id from accounts"
        ));

        let keyed = PhysicalPlanNode {
            kind: PhysicalPlanKind::AssertOneRow(PlanAssertOneRowNode::per_key_at_most_one(
                "mutation",
                vec![input.column_id],
                vec!["account_id".to_string()],
                "duplicate mutation match",
            )),
            children: vec![values(vec![input.clone()], vec![vec![literal_int(7)]])],
            output_columns: vec![input],
            stats: stats(),
            probe_runtime_filters: Vec::new(),
        };
        let final_plan = finish_for_test(&keyed).unwrap();
        let fragment = final_plan.fragments().get(&ROOT_FRAGMENT_ID).unwrap();
        assert!(matches!(
            &fragment.nodes().get(&fragment.root()).unwrap().kind,
            NodeKind::AssertOneRow(RowCountAssertionSpec::PerKeyAtMostOne {
                labels,
                message,
                ..
            }) if labels[0].as_ref() == "account_id" && message.as_ref() == "duplicate mutation match"
        ));
    }

    #[test]
    fn lowers_generate_series_with_explicit_start_stop_and_step() {
        let output = column(1, "n", DataType::Int64, false);
        let series = PhysicalPlanNode {
            kind: PhysicalPlanKind::GenerateSeries(PlanGenerateSeriesNode {
                start: 2,
                end: 8,
                step: 2,
                column_name: "n".to_string(),
                alias: Some("series".to_string()),
                output_column_id: output.column_id,
            }),
            children: Vec::new(),
            output_columns: vec![output],
            stats: stats(),
            probe_runtime_filters: Vec::new(),
        };

        let final_plan = finish_for_test(&series).unwrap();
        let fragment = final_plan.fragments().get(&ROOT_FRAGMENT_ID).unwrap();
        let root = fragment.nodes().get(&fragment.root()).unwrap();
        let NodeKind::GenerateSeries { start, stop, step } = &root.kind else {
            panic!("expected GenerateSeries root");
        };
        let literal = |expression| {
            let expression = fragment.expressions().get(expression).unwrap();
            let ContractExprKind::Literal(ContractLiteralValue::Int64(value)) = expression.kind
            else {
                panic!("expected Int64 literal");
            };
            value
        };
        assert_eq!(literal(*start), 2);
        assert_eq!(literal(*stop), 8);
        assert_eq!(literal(step.unwrap()), 2);
    }

    #[test]
    fn cte_consume_preserves_aliases_and_repeated_producer_occurrences() {
        let cte_id = 7;
        let producer_key = column(1, "producer_key", DataType::Int64, false);
        let producer_value = column(2, "producer_value", DataType::Utf8, false);
        let producer_columns = vec![producer_key, producer_value.clone()];
        let produce = cte_produce(
            cte_id,
            producer_columns.clone(),
            values(
                producer_columns,
                vec![vec![
                    literal_int(3),
                    TypedExpr {
                        kind: ExprKind::Literal(LiteralValue::String("payload".into())),
                        data_type: DataType::Utf8,
                        nullable: false,
                    },
                ]],
            ),
        );
        let first_alias = column(10, "first_alias", DataType::Utf8, false);
        let second_alias = column(11, "second_alias", DataType::Utf8, false);
        let consume = cte_consume(
            cte_id,
            "renamed_cte",
            vec![first_alias.clone(), second_alias.clone()],
            vec![producer_value.column_id, producer_value.column_id],
        );

        let final_plan = finish_for_test(&cte_anchor(cte_id, produce, consume)).unwrap();
        assert_eq!(final_plan.fragments().len(), 2);
        assert_eq!(final_plan.edges().len(), 1);
        let producer = final_plan.fragments().get(&FragmentId::new(1)).unwrap();
        let FragmentSink::Multicast { edges } = producer.sink() else {
            panic!("expected CTE producer multicast sink");
        };
        assert_eq!(edges.len(), 1);
        let producer_root = producer.nodes().get(&producer.root()).unwrap();
        let repeated_value = producer_root.output.columns[1];
        let edge = final_plan.edges().get(&edges[0]).unwrap();
        assert_eq!(edge.kind, EdgeKind::CteMulticast);
        assert_eq!(
            edge.source.projection.as_ref(),
            &[repeated_value, repeated_value]
        );
        assert_eq!(edge.destination.receive_mapping[0].0, repeated_value);
        assert_eq!(edge.destination.receive_mapping[1].0, repeated_value);
        assert_ne!(
            edge.destination.receive_mapping[0].1,
            edge.destination.receive_mapping[1].1
        );
        let consumer = final_plan.fragments().get(&ROOT_FRAGMENT_ID).unwrap();
        for (_, imported) in &edge.destination.receive_mapping {
            assert!(matches!(
                consumer.values().get(imported).unwrap().origin,
                ValueOrigin::CteImport {
                    edge: import_edge,
                    producer_fragment,
                    producer_value,
                } if import_edge == edge.id
                    && producer_fragment == FragmentId::new(1)
                    && producer_value == repeated_value
            ));
        }
        let fields = &final_plan.result_port().unwrap().fields;
        assert_eq!(fields[0].name.as_ref(), first_alias.name);
        assert_eq!(fields[1].name.as_ref(), second_alias.name);
    }

    #[test]
    fn cte_multicast_has_one_exact_edge_per_consumer() {
        let cte_id = 8;
        let producer_column = column(1, "producer", DataType::Int64, false);
        let produce = cte_produce(
            cte_id,
            vec![producer_column.clone()],
            values(vec![producer_column.clone()], vec![vec![literal_int(9)]]),
        );
        let left_column = column(10, "left_alias", DataType::Int64, false);
        let right_column = column(20, "right_alias", DataType::Int64, false);
        let left = cte_consume(
            cte_id,
            "left_cte",
            vec![left_column.clone()],
            vec![producer_column.column_id],
        );
        let right = cte_consume(
            cte_id,
            "right_cte",
            vec![right_column.clone()],
            vec![producer_column.column_id],
        );
        let result_column = column(30, "combined", DataType::Int64, false);
        let body = PhysicalPlanNode {
            kind: PhysicalPlanKind::SetOp(crate::planner::physical::PhysicalSetOpNode {
                kind: PlanSetOpKind::UnionAll,
                output_columns: vec![result_column.clone()],
                child_output_columns: vec![vec![left_column], vec![right_column]],
            }),
            children: vec![left, right],
            output_columns: vec![result_column],
            stats: stats(),
            probe_runtime_filters: Vec::new(),
        };

        let final_plan = finish_for_test(&cte_anchor(cte_id, produce, body)).unwrap();
        let producer = final_plan.fragments().get(&FragmentId::new(1)).unwrap();
        let FragmentSink::Multicast { edges } = producer.sink() else {
            panic!("expected CTE producer multicast sink");
        };
        assert_eq!(edges.len(), 2);
        assert_eq!(final_plan.edges().len(), 2);
        let producer_value = producer
            .nodes()
            .get(&producer.root())
            .unwrap()
            .output
            .columns[0];
        let mut destination_nodes = BTreeSet::new();
        for edge_id in edges {
            let edge = final_plan.edges().get(edge_id).unwrap();
            assert_eq!(edge.kind, EdgeKind::CteMulticast);
            assert_eq!(edge.source.fragment, FragmentId::new(1));
            assert_eq!(edge.source.projection.as_ref(), &[producer_value]);
            assert!(destination_nodes.insert(edge.destination.node));
        }
    }

    #[test]
    fn cte_consume_rejects_unknown_duplicate_and_inexact_mappings() {
        let unknown = cte_consume(
            99,
            "missing",
            vec![column(10, "x", DataType::Int64, false)],
            vec![ColumnId(1)],
        );
        assert!(
            finish_for_test(&unknown)
                .unwrap_err()
                .to_string()
                .contains("unknown or out-of-scope producer 99")
        );

        let cte_id = 9;
        let producer_column = column(1, "producer", DataType::Int64, false);
        let produce = || {
            cte_produce(
                cte_id,
                vec![producer_column.clone()],
                values(vec![producer_column.clone()], vec![vec![literal_int(1)]]),
            )
        };
        let arity = cte_consume(
            cte_id,
            "arity",
            vec![column(10, "x", DataType::Int64, false)],
            Vec::new(),
        );
        assert!(
            finish_for_test(&cte_anchor(cte_id, produce(), arity))
                .unwrap_err()
                .to_string()
                .contains("CTEConsume producer mapping expected 1 item(s), got 0")
        );

        let duplicate = cte_consume(
            cte_id,
            "duplicate",
            vec![
                column(10, "x", DataType::Int64, false),
                column(10, "y", DataType::Int64, false),
            ],
            vec![producer_column.column_id, producer_column.column_id],
        );
        assert!(
            finish_for_test(&cte_anchor(cte_id, produce(), duplicate))
                .unwrap_err()
                .to_string()
                .contains("repeats output column c10")
        );

        let missing_column = cte_consume(
            cte_id,
            "missing_column",
            vec![column(10, "x", DataType::Int64, false)],
            vec![ColumnId(404)],
        );
        assert!(
            finish_for_test(&cte_anchor(cte_id, produce(), missing_column))
                .unwrap_err()
                .to_string()
                .contains("missing producer column c404")
        );

        let wrong_type = cte_consume(
            cte_id,
            "wrong_type",
            vec![column(10, "x", DataType::Utf8, false)],
            vec![producer_column.column_id],
        );
        assert!(
            finish_for_test(&cte_anchor(cte_id, produce(), wrong_type))
                .unwrap_err()
                .to_string()
                .contains("producer column c1 has type")
        );
    }

    #[test]
    fn cte_anchor_rejects_duplicate_active_definition() {
        let cte_id = 10;
        let outer_column = column(1, "outer", DataType::Int64, false);
        let inner_column = column(2, "inner", DataType::Int64, false);
        let inner_consume = cte_consume(
            cte_id,
            "inner",
            vec![column(20, "value", DataType::Int64, false)],
            vec![inner_column.column_id],
        );
        let inner = cte_anchor(
            cte_id,
            cte_produce(
                cte_id,
                vec![inner_column.clone()],
                values(vec![inner_column], vec![vec![literal_int(2)]]),
            ),
            inner_consume,
        );
        let outer = cte_anchor(
            cte_id,
            cte_produce(
                cte_id,
                vec![outer_column.clone()],
                values(vec![outer_column], vec![vec![literal_int(1)]]),
            ),
            inner,
        );

        assert!(
            finish_for_test(&outer)
                .unwrap_err()
                .to_string()
                .contains("duplicate active CTE definition 10")
        );
    }

    #[test]
    fn decimal_literals_are_lowered_to_exact_unscaled_values() {
        assert_eq!(parse_decimal128("12.30", 2).unwrap(), 1_230);
        assert_eq!(parse_decimal128("1200", -2).unwrap(), 12);
        assert!(parse_decimal128("12.34", 1).is_err());
        assert!(
            lower_literal(
                &LiteralValue::Decimal("999".to_string()),
                &ValueType::new(DataType::Decimal128(2, 0), false),
            )
            .is_err()
        );
    }

    #[test]
    fn largeint_literals_keep_their_semantic_contract_carrier() {
        let output = column(
            1,
            "large_number",
            DataType::FixedSizeBinary(novarocks_type_contract::LARGEINT_BYTE_WIDTH),
            false,
        );
        let final_plan = finish_for_test(&values(
            vec![output],
            vec![vec![literal_largeint(i128::MIN)]],
        ))
        .unwrap();
        let fragment = final_plan.fragments().get(&ROOT_FRAGMENT_ID).unwrap();
        let root = fragment.nodes().get(&fragment.root()).unwrap();
        let NodeKind::Values { rows } = &root.kind else {
            panic!("expected Values root");
        };
        assert!(matches!(
            fragment.expressions().get(rows[0][0]).unwrap().kind,
            ContractExprKind::Literal(ContractLiteralValue::LargeInt(i128::MIN))
        ));
    }
}
