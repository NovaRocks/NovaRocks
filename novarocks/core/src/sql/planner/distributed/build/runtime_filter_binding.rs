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

use std::collections::{BTreeMap, BTreeSet, HashMap};

use crate::runtime_filter::model::contract::{
    ArtifactCapability, BindingId, ChannelId, CompletionRequirement, ConsumerActivation,
    ContributionKind, CoverageWitnessId, NullSemantics, PlanFragmentId, PlanNodeId,
    ReductionRequirement, RuntimeFilterLifecycle, RuntimeFilterLogicalDomain,
    RuntimeFilterPolicyRequirement,
};
use crate::runtime_filter::model::coverage::Coverage;
use crate::runtime_filter::model::graph::{
    ApplyPoint, ConsumerBindingTarget, ConsumerRequirement, PlanLocation, ProducerRequirement,
    RuntimeFilterBindingRole, RuntimeFilterBindingSpec, RuntimeFilterChannelSpec,
    RuntimeFilterGraph,
};
use crate::sql::analysis::expr_display::typed_expr_display_name;
use crate::sql::analysis::{ExprKind, OutputColumn, SortItem, TypedExpr};
use crate::sql::column_id::ColumnId;

use crate::sql::planner::distributed::{
    DistributedNode, DistributedNodeKind, FragmentId, PlanFragment,
};
use crate::sql::planner::physical::runtime_filter::{
    RuntimeFilterBuildIntent, RuntimeFilterProbeIntent,
};
use crate::sql::planner::physical::{PhysicalPlanKind, PhysicalPlanNode};

#[derive(Clone)]
pub(super) struct RuntimeFilterBuildBinding {
    pub(super) node_id: i32,
    pub(super) fragment_id: FragmentId,
    pub(super) intent: RuntimeFilterBuildIntent,
}

#[derive(Clone)]
pub(super) struct RuntimeFilterProbeBinding {
    pub(super) node_id: i32,
    pub(super) fragment_id: FragmentId,
    pub(super) intent: RuntimeFilterProbeIntent,
}

pub(super) struct RuntimeFilterBindings {
    pub(super) builds: Vec<RuntimeFilterBuildBinding>,
    pub(super) probes: Vec<RuntimeFilterProbeBinding>,
    pub(super) node_input_columns: BTreeMap<(FragmentId, i32), Vec<Vec<OutputColumn>>>,
}

#[derive(Clone)]
pub(super) struct RuntimeFilterProducerCandidate {
    pub(super) location: PlanLocation,
    pub(super) expression: TypedExpr,
    pub(super) join_key_ordinal: usize,
    pub(super) contribution_kinds: BTreeSet<ContributionKind>,
    pub(super) completion_requirement: CompletionRequirement,
}

#[derive(Clone)]
pub(super) struct RuntimeFilterConsumerCandidate {
    pub(super) location: PlanLocation,
    pub(super) expression: TypedExpr,
    pub(super) target: ConsumerBindingTarget,
    pub(super) capabilities: BTreeSet<ArtifactCapability>,
    pub(super) activation: ConsumerActivation,
}

#[derive(Clone)]
pub(super) struct RuntimeFilterChannelCandidate {
    pub(super) legacy_filter_id: i32,
    pub(super) channel: RuntimeFilterChannelSpec,
    pub(super) producer: RuntimeFilterProducerCandidate,
    pub(super) consumers: Vec<RuntimeFilterConsumerCandidate>,
}

impl RuntimeFilterBindings {
    pub(super) fn new() -> Self {
        Self {
            builds: Vec::new(),
            probes: Vec::new(),
            node_input_columns: BTreeMap::new(),
        }
    }

    pub(super) fn record(
        &mut self,
        node_id: i32,
        fragment_id: FragmentId,
        physical: &PhysicalPlanNode,
        distributed_payload: &DistributedNodeKind,
    ) {
        self.node_input_columns.insert(
            (fragment_id, node_id),
            physical
                .children
                .iter()
                .map(|child| child.output_columns.clone())
                .collect(),
        );
        for intent in &physical.probe_runtime_filters {
            self.probes.push(RuntimeFilterProbeBinding {
                node_id,
                fragment_id,
                intent: intent.clone(),
            });
        }
        if matches!(distributed_payload, DistributedNodeKind::HashJoin(_))
            && let PhysicalPlanKind::HashJoin(join) = &physical.kind
        {
            for intent in &join.build_runtime_filters {
                self.builds.push(RuntimeFilterBuildBinding {
                    node_id,
                    fragment_id,
                    intent: intent.clone(),
                });
            }
        }
    }
}

pub(super) fn populate_runtime_filter_candidates(
    fragments: &mut [PlanFragment],
    graph: &mut RuntimeFilterGraph,
    mut candidates: Vec<RuntimeFilterChannelCandidate>,
) -> Result<(), String> {
    candidates.sort_by_key(|candidate| candidate.legacy_filter_id);
    candidates.dedup_by_key(|candidate| candidate.legacy_filter_id);
    let mut binding_ids_by_node = HashMap::<(FragmentId, i32), Vec<BindingId>>::new();
    let mut next_channel_id = u32::try_from(graph.channel_count())
        .map_err(|_| "runtime filter channel count does not fit u32".to_string())?;
    let mut next_binding_id = u32::try_from(graph.binding_count())
        .map_err(|_| "runtime filter binding count does not fit u32".to_string())?;

    for mut candidate in candidates {
        if candidate.consumers.is_empty()
            || candidate.consumers.iter().any(|consumer| {
                consumer.expression.data_type != candidate.producer.expression.data_type
            })
        {
            continue;
        }
        candidate.consumers.sort_by_key(|consumer| {
            (
                consumer.location.fragment_id,
                consumer.location.node_id,
                expression_column_ids(&consumer.expression),
                typed_expr_display_name(&consumer.expression),
            )
        });
        let channel_id = ChannelId::new(next_channel_id);
        next_channel_id = next_channel_id
            .checked_add(1)
            .ok_or_else(|| "runtime filter channel id overflow".to_string())?;
        let witness_id = CoverageWitnessId::new(channel_id.get());
        candidate.channel.channel_id = channel_id;
        candidate.channel.availability_coverage = Coverage::Leaf(witness_id);
        candidate.channel.terminal_coverage = Coverage::Leaf(witness_id);
        graph.insert_channel(candidate.channel).map_err(|error| {
            format!(
                "runtime filter {} graph channel insertion failed: {error:?}",
                candidate.legacy_filter_id
            )
        })?;

        let producer_binding_id = BindingId::new(next_binding_id);
        next_binding_id = next_binding_id
            .checked_add(1)
            .ok_or_else(|| "runtime filter binding id overflow".to_string())?;
        graph
            .insert_binding(RuntimeFilterBindingSpec {
                binding_id: producer_binding_id,
                channel_id,
                coverage_witness_id: Some(witness_id),
                location: candidate.producer.location,
                expression: candidate.producer.expression,
                apply_point: ApplyPoint::NodeOutput,
                role: RuntimeFilterBindingRole::Producer(ProducerRequirement {
                    contribution_kinds: candidate.producer.contribution_kinds,
                    completion_requirement: candidate.producer.completion_requirement,
                    join_key_ordinal: candidate.producer.join_key_ordinal,
                }),
            })
            .map_err(|error| {
                format!(
                    "runtime filter {} producer binding insertion failed: {error:?}",
                    candidate.legacy_filter_id
                )
            })?;
        binding_ids_by_node
            .entry((
                candidate.producer.location.fragment_id.get(),
                candidate.producer.location.node_id.get(),
            ))
            .or_default()
            .push(producer_binding_id);

        for consumer in candidate.consumers {
            let binding_id = BindingId::new(next_binding_id);
            next_binding_id = next_binding_id
                .checked_add(1)
                .ok_or_else(|| "runtime filter binding id overflow".to_string())?;
            graph
                .insert_binding(RuntimeFilterBindingSpec {
                    binding_id,
                    channel_id,
                    coverage_witness_id: None,
                    location: consumer.location,
                    expression: consumer.expression,
                    apply_point: ApplyPoint::NodeInput,
                    role: RuntimeFilterBindingRole::Consumer(ConsumerRequirement {
                        capabilities: consumer.capabilities,
                        activation: consumer.activation,
                        target: consumer.target,
                    }),
                })
                .map_err(|error| {
                    format!(
                        "runtime filter {} consumer binding insertion failed: {error:?}",
                        candidate.legacy_filter_id
                    )
                })?;
            binding_ids_by_node
                .entry((
                    consumer.location.fragment_id.get(),
                    consumer.location.node_id.get(),
                ))
                .or_default()
                .push(binding_id);
        }
    }
    for fragment in fragments {
        attach_binding_ids(&mut fragment.root, &mut binding_ids_by_node);
    }
    Ok(())
}

pub(super) fn populate_runtime_filter_graph(
    fragments: &mut [PlanFragment],
    graph: &mut RuntimeFilterGraph,
    bindings: &RuntimeFilterBindings,
) -> Result<(), String> {
    let mut builds_by_filter = BTreeMap::new();
    for build in &bindings.builds {
        builds_by_filter
            .entry(build.intent.filter_id)
            .or_insert_with(|| build.clone());
    }
    let mut probes_by_filter: BTreeMap<i32, Vec<RuntimeFilterProbeBinding>> = BTreeMap::new();
    for probe in &bindings.probes {
        let probes = probes_by_filter.entry(probe.intent.filter_id).or_default();
        if !probes.iter().any(|candidate| {
            candidate.node_id == probe.node_id && candidate.fragment_id == probe.fragment_id
        }) {
            probes.push(probe.clone());
        }
    }

    let mut candidates = Vec::new();
    for (legacy_filter_id, build) in builds_by_filter {
        let Some(probes) = probes_by_filter.get(&legacy_filter_id) else {
            continue;
        };
        let mut resolved_probes = Vec::new();
        let mut exact = true;
        for probe in probes {
            match resolve_consumer_binding(fragments, bindings, probe) {
                Ok(mut resolved) => resolved_probes.append(&mut resolved),
                Err(()) => {
                    exact = false;
                    break;
                }
            }
        }
        resolved_probes.sort_by_key(|probe| (probe.fragment_id, probe.node_id, probe.stable_key()));
        resolved_probes.dedup_by(|left, right| {
            left.fragment_id == right.fragment_id
                && left.node_id == right.node_id
                && left.stable_key() == right.stable_key()
        });
        if !exact || resolved_probes.is_empty() {
            continue;
        }

        let witness_id = CoverageWitnessId::new(0);
        let contributions = BTreeSet::from([
            ContributionKind::ValueDomainDelta,
            ContributionKind::ProducerClosed,
        ]);
        let capabilities = BTreeSet::from([
            ArtifactCapability::Membership,
            ArtifactCapability::EmptyDomain,
        ]);
        candidates.push(RuntimeFilterChannelCandidate {
            legacy_filter_id,
            channel: RuntimeFilterChannelSpec {
                channel_id: ChannelId::new(0),
                logical_domain: RuntimeFilterLogicalDomain::Membership {
                    value_type: build.intent.build_expr.data_type.clone(),
                    null_semantics: NullSemantics::NeverMatches,
                },
                lifecycle: RuntimeFilterLifecycle::CompleteOnce,
                availability_coverage: Coverage::Leaf(witness_id),
                terminal_coverage: Coverage::Leaf(witness_id),
                reduction_requirement: ReductionRequirement::SetUnion,
                allowed_contribution_kinds: contributions.clone(),
                required_consumer_capabilities: capabilities.clone(),
                policy: RuntimeFilterPolicyRequirement {
                    max_contribution_bytes: 1024,
                    max_artifact_bytes: 4096,
                    deadline_ms: 30_000,
                    max_retries: 3,
                },
            },
            producer: RuntimeFilterProducerCandidate {
                location: PlanLocation {
                    fragment_id: PlanFragmentId::new(build.fragment_id),
                    node_id: PlanNodeId::new(build.node_id),
                },
                expression: build.intent.build_expr.clone(),
                join_key_ordinal: build.intent.expr_order,
                contribution_kinds: contributions,
                completion_requirement: CompletionRequirement::ProducerClosed,
            },
            consumers: resolved_probes
                .into_iter()
                .map(|probe| RuntimeFilterConsumerCandidate {
                    location: PlanLocation {
                        fragment_id: PlanFragmentId::new(probe.fragment_id),
                        node_id: PlanNodeId::new(probe.node_id),
                    },
                    expression: probe.expression,
                    target: probe.target,
                    capabilities: capabilities.clone(),
                    activation: ConsumerActivation::BlockingSnapshot,
                })
                .collect(),
        });
    }
    populate_runtime_filter_candidates(fragments, graph, candidates)
}
#[derive(Clone)]
struct ResolvedConsumerBinding {
    node_id: i32,
    fragment_id: FragmentId,
    expression: TypedExpr,
    target: ConsumerBindingTarget,
}

impl ResolvedConsumerBinding {
    fn stable_key(&self) -> (Vec<ColumnId>, String) {
        (
            expression_column_ids(&self.expression),
            typed_expr_display_name(&self.expression),
        )
    }
}

fn resolve_consumer_binding(
    fragments: &[PlanFragment],
    bindings: &RuntimeFilterBindings,
    probe: &RuntimeFilterProbeBinding,
) -> Result<Vec<ResolvedConsumerBinding>, ()> {
    let node = find_node(fragments, probe.fragment_id, probe.node_id).ok_or(())?;
    match &node.payload {
        DistributedNodeKind::Project(project) => {
            let replacements = project
                .items
                .iter()
                .map(|item| (item.output_column_id, item.expr.clone()))
                .collect::<BTreeMap<_, _>>();
            let expression = rewrite_expr_by_column(&probe.intent.probe_expr, &replacements)?;
            Ok(vec![resolved_consumer_binding(bindings, node, expression)?])
        }
        DistributedNodeKind::HashAggregate(aggregate) => {
            let referenced = expression_column_ids(&probe.intent.probe_expr);
            if referenced.iter().any(|column_id| {
                aggregate
                    .output_layout
                    .aggregate_columns
                    .iter()
                    .any(|column| column.column_id == *column_id)
            }) || aggregate.output_layout.group_key_columns.len() != aggregate.group_by.len()
            {
                return Err(());
            }
            let replacements = aggregate
                .output_layout
                .group_key_columns
                .iter()
                .zip(&aggregate.group_by)
                .map(|(column, expression)| (column.column_id, expression.clone()))
                .collect::<BTreeMap<_, _>>();
            let expression = rewrite_expr_by_column(&probe.intent.probe_expr, &replacements)?;
            Ok(vec![resolved_consumer_binding(bindings, node, expression)?])
        }
        DistributedNodeKind::SetOp(set_op)
            if matches!(
                set_op.kind,
                crate::sql::planner::physical::PlanSetOpKind::UnionAll
            ) =>
        {
            if set_op.output_columns.is_empty()
                || set_op.child_output_columns.len() != node.children.len()
            {
                return Err(());
            }
            let mut resolved = Vec::with_capacity(node.children.len());
            for (child, child_columns) in node.children.iter().zip(&set_op.child_output_columns) {
                if child_columns.len() != set_op.output_columns.len() {
                    return Err(());
                }
                let replacements = set_op
                    .output_columns
                    .iter()
                    .zip(child_columns)
                    .map(|(output, input)| {
                        exact_column_mapping(output, input).map(|expr| (output.column_id, expr))
                    })
                    .collect::<Result<BTreeMap<_, _>, _>>()?;
                let expression = rewrite_expr_by_column(&probe.intent.probe_expr, &replacements)?;
                resolved.push(resolved_consumer_binding(bindings, child, expression)?);
            }
            Ok(resolved)
        }
        DistributedNodeKind::Exchange(exchange) => {
            let source = fragments
                .iter()
                .find(|fragment| fragment.fragment_id == exchange.source_fragment_id)
                .ok_or(())?;
            if exchange.output_columns.len() != source.output_columns.len() {
                return Err(());
            }
            let replacements = exchange
                .output_columns
                .iter()
                .zip(&source.output_columns)
                .map(|(output, input)| {
                    exact_column_mapping(output, input).map(|expr| (output.column_id, expr))
                })
                .collect::<Result<BTreeMap<_, _>, _>>()?;
            let expression = rewrite_expr_by_column(&probe.intent.probe_expr, &replacements)?;
            Ok(vec![resolved_consumer_binding(
                bindings,
                &source.root,
                expression,
            )?])
        }
        _ => Ok(vec![resolved_consumer_binding(
            bindings,
            node,
            probe.intent.probe_expr.clone(),
        )?]),
    }
}

fn resolved_consumer_binding(
    bindings: &RuntimeFilterBindings,
    node: &DistributedNode,
    expression: TypedExpr,
) -> Result<ResolvedConsumerBinding, ()> {
    let target = if node.children.is_empty() {
        ConsumerBindingTarget::SourceBoundary
    } else {
        let inputs = bindings
            .node_input_columns
            .get(&(node.fragment_id, node.node_id))
            .ok_or(())?;
        if inputs.len() != node.children.len() {
            return Err(());
        }
        let referenced = expression_column_ids(&expression);
        if referenced.is_empty() {
            return Err(());
        }
        let matches = inputs
            .iter()
            .enumerate()
            .filter_map(|(input_ordinal, columns)| {
                let available = columns
                    .iter()
                    .map(|column| column.column_id)
                    .collect::<BTreeSet<_>>();
                referenced
                    .iter()
                    .all(|column_id| available.contains(column_id))
                    .then_some(input_ordinal)
            })
            .collect::<Vec<_>>();
        let [input_ordinal] = matches.as_slice() else {
            return Err(());
        };
        ConsumerBindingTarget::DirectInput {
            input_ordinal: *input_ordinal,
        }
    };
    Ok(ResolvedConsumerBinding {
        node_id: node.node_id,
        fragment_id: node.fragment_id,
        expression,
        target,
    })
}

fn find_node(
    fragments: &[PlanFragment],
    fragment_id: FragmentId,
    node_id: i32,
) -> Option<&DistributedNode> {
    fn visit(node: &DistributedNode, node_id: i32) -> Option<&DistributedNode> {
        if node.node_id == node_id {
            return Some(node);
        }
        node.children.iter().find_map(|child| visit(child, node_id))
    }
    fragments
        .iter()
        .find(|fragment| fragment.fragment_id == fragment_id)
        .and_then(|fragment| visit(&fragment.root, node_id))
}

fn exact_column_mapping(output: &OutputColumn, input: &OutputColumn) -> Result<TypedExpr, ()> {
    if output.data_type != input.data_type || output.nullable != input.nullable {
        return Err(());
    }
    Ok(TypedExpr {
        kind: ExprKind::ColumnRef {
            column_id: input.column_id,
            qualifier: None,
            column: input.name.clone(),
        },
        data_type: input.data_type.clone(),
        nullable: input.nullable,
    })
}

fn expression_column_ids(expr: &TypedExpr) -> Vec<ColumnId> {
    fn collect(expr: &TypedExpr, ids: &mut Vec<ColumnId>) {
        match &expr.kind {
            ExprKind::ColumnRef { column_id, .. } => ids.push(*column_id),
            ExprKind::BinaryOp { left, right, .. } => {
                collect(left, ids);
                collect(right, ids);
            }
            ExprKind::UnaryOp { expr, .. }
            | ExprKind::Cast { expr, .. }
            | ExprKind::IsNull { expr, .. }
            | ExprKind::IsTruthValue { expr, .. }
            | ExprKind::Nested(expr) => collect(expr, ids),
            ExprKind::FunctionCall { args, .. } => {
                for arg in args {
                    collect(arg, ids);
                }
            }
            ExprKind::LambdaFunction { body, .. } | ExprKind::Lambda { body, .. } => {
                collect(body, ids)
            }
            ExprKind::AggregateCall { args, order_by, .. } => {
                for arg in args {
                    collect(arg, ids);
                }
                for item in order_by {
                    collect(&item.expr, ids);
                }
            }
            ExprKind::InList { expr, list, .. } => {
                collect(expr, ids);
                for item in list {
                    collect(item, ids);
                }
            }
            ExprKind::Between {
                expr, low, high, ..
            } => {
                collect(expr, ids);
                collect(low, ids);
                collect(high, ids);
            }
            ExprKind::Like { expr, pattern, .. } => {
                collect(expr, ids);
                collect(pattern, ids);
            }
            ExprKind::Case {
                operand,
                when_then,
                else_expr,
            } => {
                if let Some(operand) = operand {
                    collect(operand, ids);
                }
                for (when, then) in when_then {
                    collect(when, ids);
                    collect(then, ids);
                }
                if let Some(else_expr) = else_expr {
                    collect(else_expr, ids);
                }
            }
            ExprKind::WindowCall {
                args,
                partition_by,
                order_by,
                ..
            } => {
                for arg in args {
                    collect(arg, ids);
                }
                for item in partition_by {
                    collect(item, ids);
                }
                for item in order_by {
                    collect(&item.expr, ids);
                }
            }
            ExprKind::Literal(_)
            | ExprKind::LambdaParamRef { .. }
            | ExprKind::SubqueryPlaceholder { .. } => {}
        }
    }
    let mut ids = Vec::new();
    collect(expr, &mut ids);
    ids.sort_unstable();
    ids.dedup();
    ids
}

fn rewrite_expr_by_column(
    expr: &TypedExpr,
    replacements: &BTreeMap<ColumnId, TypedExpr>,
) -> Result<TypedExpr, ()> {
    if let ExprKind::ColumnRef { column_id, .. } = &expr.kind
        && let Some(replacement) = replacements.get(column_id)
    {
        if replacement.data_type != expr.data_type || replacement.nullable != expr.nullable {
            return Err(());
        }
        return Ok(replacement.clone());
    }
    let mut rewritten = expr.clone();
    match &mut rewritten.kind {
        ExprKind::BinaryOp { left, right, .. } => {
            **left = rewrite_expr_by_column(left, replacements)?;
            **right = rewrite_expr_by_column(right, replacements)?;
        }
        ExprKind::UnaryOp { expr, .. }
        | ExprKind::Cast { expr, .. }
        | ExprKind::IsNull { expr, .. }
        | ExprKind::IsTruthValue { expr, .. }
        | ExprKind::Nested(expr) => **expr = rewrite_expr_by_column(expr, replacements)?,
        ExprKind::FunctionCall { args, .. } => rewrite_exprs(args, replacements)?,
        ExprKind::LambdaFunction { body, .. } | ExprKind::Lambda { body, .. } => {
            **body = rewrite_expr_by_column(body, replacements)?;
        }
        ExprKind::AggregateCall { args, order_by, .. } => {
            rewrite_exprs(args, replacements)?;
            rewrite_sort_items(order_by, replacements)?;
        }
        ExprKind::InList { expr, list, .. } => {
            **expr = rewrite_expr_by_column(expr, replacements)?;
            rewrite_exprs(list, replacements)?;
        }
        ExprKind::Between {
            expr, low, high, ..
        } => {
            **expr = rewrite_expr_by_column(expr, replacements)?;
            **low = rewrite_expr_by_column(low, replacements)?;
            **high = rewrite_expr_by_column(high, replacements)?;
        }
        ExprKind::Like { expr, pattern, .. } => {
            **expr = rewrite_expr_by_column(expr, replacements)?;
            **pattern = rewrite_expr_by_column(pattern, replacements)?;
        }
        ExprKind::Case {
            operand,
            when_then,
            else_expr,
        } => {
            if let Some(operand) = operand {
                **operand = rewrite_expr_by_column(operand, replacements)?;
            }
            for (when, then) in when_then {
                *when = rewrite_expr_by_column(when, replacements)?;
                *then = rewrite_expr_by_column(then, replacements)?;
            }
            if let Some(else_expr) = else_expr {
                **else_expr = rewrite_expr_by_column(else_expr, replacements)?;
            }
        }
        ExprKind::WindowCall {
            args,
            partition_by,
            order_by,
            ..
        } => {
            rewrite_exprs(args, replacements)?;
            rewrite_exprs(partition_by, replacements)?;
            rewrite_sort_items(order_by, replacements)?;
        }
        ExprKind::ColumnRef { .. }
        | ExprKind::Literal(_)
        | ExprKind::LambdaParamRef { .. }
        | ExprKind::SubqueryPlaceholder { .. } => {}
    }
    Ok(rewritten)
}

fn rewrite_exprs(
    expressions: &mut [TypedExpr],
    replacements: &BTreeMap<ColumnId, TypedExpr>,
) -> Result<(), ()> {
    for expression in expressions {
        *expression = rewrite_expr_by_column(expression, replacements)?;
    }
    Ok(())
}

fn rewrite_sort_items(
    items: &mut [SortItem],
    replacements: &BTreeMap<ColumnId, TypedExpr>,
) -> Result<(), ()> {
    for item in items {
        item.expr = rewrite_expr_by_column(&item.expr, replacements)?;
    }
    Ok(())
}

fn attach_binding_ids(
    node: &mut DistributedNode,
    binding_ids_by_node: &mut HashMap<(FragmentId, i32), Vec<BindingId>>,
) {
    if let Some(mut binding_ids) = binding_ids_by_node.remove(&(node.fragment_id, node.node_id)) {
        node.runtime_filter_binding_ids.append(&mut binding_ids);
        node.runtime_filter_binding_ids.sort_unstable();
        node.runtime_filter_binding_ids.dedup();
    }
    for child in &mut node.children {
        attach_binding_ids(child, binding_ids_by_node);
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;

    use crate::runtime_filter::model::contract::{
        ComparatorDigest, CompletionFenceKind, LateApplyGranularity, NullOrder, OrderContract,
        OrderKeyContract, SortDirection,
    };
    use crate::runtime_filter::model::validation::GraphValidationErrorKind;
    use crate::sql::analysis::{ExprKind, LiteralValue};
    use crate::sql::planner::distributed::{
        DataPartition, DataSink, ExchangeFlavor, ExchangeReceiver,
    };
    use crate::sql::planner::payload::{PlanProjectNode, PlanValuesNode};
    use crate::sql::planner::physical::{
        AggMode, AggregateOutputLayout, PhysicalHashAggregateNode, PhysicalPlanStats,
        PhysicalSetOpNode, PlanSetOpKind, PlannerConfidence,
    };

    use super::*;

    fn expression() -> TypedExpr {
        TypedExpr {
            kind: ExprKind::Literal(LiteralValue::Int(1)),
            data_type: DataType::Int64,
            nullable: false,
        }
    }

    fn column(column_id: u32, name: &str) -> OutputColumn {
        OutputColumn {
            column_id: ColumnId::new_for_test(column_id),
            name: name.to_string(),
            data_type: DataType::Int64,
            nullable: false,
            is_internal: false,
        }
    }

    fn column_expression(column_id: u32, name: &str) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: ColumnId::new_for_test(column_id),
                qualifier: None,
                column: name.to_string(),
            },
            data_type: DataType::Int64,
            nullable: false,
        }
    }

    fn node(
        node_id: i32,
        fragment_id: FragmentId,
        payload: DistributedNodeKind,
        children: Vec<DistributedNode>,
    ) -> DistributedNode {
        DistributedNode {
            node_id,
            fragment_id,
            tuple_ids: Vec::new(),
            nullable_tuple_ids: Vec::new(),
            limit: -1,
            runtime_filter_binding_ids: Vec::new(),
            children,
            stats: PhysicalPlanStats {
                output_row_count: 1.0,
                row_count_confidence: PlannerConfidence::Exact,
                column_statistics: Default::default(),
                cost_estimate: None,
                broadcast_decision: None,
            },
            payload,
        }
    }

    fn values_node(node_id: i32, fragment_id: FragmentId) -> DistributedNode {
        node(
            node_id,
            fragment_id,
            DistributedNodeKind::Values(PlanValuesNode {
                rows: Vec::new(),
                columns: Vec::new(),
            }),
            Vec::new(),
        )
    }

    fn fragment(
        fragment_id: FragmentId,
        root: DistributedNode,
        output_columns: Vec<OutputColumn>,
    ) -> PlanFragment {
        PlanFragment {
            fragment_id,
            root,
            data_partition: DataPartition::unpartitioned(),
            output_partition: DataPartition::unpartitioned(),
            sink: if fragment_id == 0 {
                DataSink::Result
            } else {
                DataSink::Noop
            },
            output_exprs: None,
            output_columns,
            cte_id: None,
            cte_exchange_nodes: Vec::new(),
        }
    }

    fn fragments() -> Vec<PlanFragment> {
        vec![fragment(
            0,
            node(
                1,
                0,
                DistributedNodeKind::Values(PlanValuesNode {
                    rows: Vec::new(),
                    columns: Vec::new(),
                }),
                vec![values_node(2, 0)],
            ),
            Vec::new(),
        )]
    }

    fn policy() -> RuntimeFilterPolicyRequirement {
        RuntimeFilterPolicyRequirement {
            max_contribution_bytes: 1024,
            max_artifact_bytes: 4096,
            deadline_ms: 30_000,
            max_retries: 3,
        }
    }

    #[test]
    fn rfd_5a_generic_topn_and_aggregate_feedback_candidates_are_non_blocking_live() {
        let location = |node_id| PlanLocation {
            fragment_id: PlanFragmentId::new(0),
            node_id: PlanNodeId::new(node_id),
        };
        let live = ConsumerActivation::NonBlockingLive {
            late_apply: LateApplyGranularity::Batch,
        };
        let topn_contributions = BTreeSet::from([
            ContributionKind::OrderedBoundUpdate,
            ContributionKind::ProducerClosed,
        ]);
        let aggregate_contributions = BTreeSet::from([
            ContributionKind::FinalDomainShard,
            ContributionKind::ProducerClosed,
        ]);
        let mut fragments = fragments();
        let mut graph = RuntimeFilterGraph::default();
        populate_runtime_filter_candidates(
            &mut fragments,
            &mut graph,
            vec![
                RuntimeFilterChannelCandidate {
                    legacy_filter_id: 20,
                    channel: RuntimeFilterChannelSpec {
                        channel_id: ChannelId::new(99),
                        logical_domain: RuntimeFilterLogicalDomain::OrderedBound(OrderContract {
                            keys: vec![OrderKeyContract {
                                data_type: DataType::Int64,
                                direction: SortDirection::Descending,
                                null_order: NullOrder::First,
                            }],
                            inclusive: true,
                            comparator_digest: ComparatorDigest::new([7; 32]),
                        }),
                        lifecycle: RuntimeFilterLifecycle::MonotonicUpdates,
                        availability_coverage: Coverage::Leaf(CoverageWitnessId::new(99)),
                        terminal_coverage: Coverage::Leaf(CoverageWitnessId::new(99)),
                        reduction_requirement: ReductionRequirement::TightenOrderedBound,
                        allowed_contribution_kinds: topn_contributions.clone(),
                        required_consumer_capabilities: BTreeSet::from([
                            ArtifactCapability::OrderedRange,
                        ]),
                        policy: policy(),
                    },
                    producer: RuntimeFilterProducerCandidate {
                        location: location(1),
                        expression: expression(),
                        join_key_ordinal: 0,
                        contribution_kinds: topn_contributions,
                        completion_requirement: CompletionRequirement::ProducerClosed,
                    },
                    consumers: vec![RuntimeFilterConsumerCandidate {
                        location: location(2),
                        expression: expression(),
                        target: ConsumerBindingTarget::SourceBoundary,
                        capabilities: BTreeSet::from([ArtifactCapability::OrderedRange]),
                        activation: live,
                    }],
                },
                RuntimeFilterChannelCandidate {
                    legacy_filter_id: 30,
                    channel: RuntimeFilterChannelSpec {
                        channel_id: ChannelId::new(99),
                        logical_domain: RuntimeFilterLogicalDomain::Membership {
                            value_type: DataType::Int64,
                            null_semantics: NullSemantics::NullSafeEqual,
                        },
                        lifecycle: RuntimeFilterLifecycle::CompleteOnce,
                        availability_coverage: Coverage::Leaf(CoverageWitnessId::new(99)),
                        terminal_coverage: Coverage::Leaf(CoverageWitnessId::new(99)),
                        reduction_requirement: ReductionRequirement::SetUnion,
                        allowed_contribution_kinds: aggregate_contributions.clone(),
                        required_consumer_capabilities: BTreeSet::from([
                            ArtifactCapability::Membership,
                            ArtifactCapability::EmptyDomain,
                        ]),
                        policy: policy(),
                    },
                    producer: RuntimeFilterProducerCandidate {
                        location: location(1),
                        expression: expression(),
                        join_key_ordinal: 0,
                        contribution_kinds: aggregate_contributions,
                        completion_requirement: CompletionRequirement::FencedFinalDomain(
                            CompletionFenceKind::CommittedDomainFrozen,
                        ),
                    },
                    consumers: vec![RuntimeFilterConsumerCandidate {
                        location: location(2),
                        expression: expression(),
                        target: ConsumerBindingTarget::SourceBoundary,
                        capabilities: BTreeSet::from([
                            ArtifactCapability::Membership,
                            ArtifactCapability::EmptyDomain,
                        ]),
                        activation: live,
                    }],
                },
            ],
        )
        .expect("populate generic RFD-5A candidates");

        graph.validate().expect("generic graph must validate");
        assert_eq!(graph.channel_count(), 2);
        assert_eq!(graph.binding_count(), 4);
        assert!(
            graph
                .bindings()
                .filter_map(|binding| match &binding.role {
                    RuntimeFilterBindingRole::Consumer(consumer) => Some(consumer.activation),
                    RuntimeFilterBindingRole::Producer(_) => None,
                })
                .all(|activation| activation == live)
        );
        assert_eq!(fragments[0].root.runtime_filter_binding_ids.len(), 2);
        assert_eq!(
            fragments[0].root.children[0]
                .runtime_filter_binding_ids
                .len(),
            2
        );
        for binding_id in [BindingId::new(1), BindingId::new(3)] {
            let mut invalid = graph.clone();
            let RuntimeFilterBindingRole::Consumer(consumer) = &mut invalid
                .binding_mut_for_test(binding_id)
                .expect("consumer binding")
                .role
            else {
                panic!("expected consumer binding");
            };
            consumer.activation = ConsumerActivation::BlockingSnapshot;
            assert_eq!(
                invalid
                    .validate()
                    .expect_err("feedback must not block")
                    .kind,
                GraphValidationErrorKind::BlockingFeedbackConsumer
            );
        }
    }

    #[test]
    fn rfd_5a_project_rewrites_consumer_to_exact_input_expression() {
        let project = node(
            1,
            0,
            DistributedNodeKind::Project(PlanProjectNode {
                items: vec![crate::sql::analysis::ProjectItem {
                    expr: column_expression(1, "input"),
                    output_name: "projected".to_string(),
                    output_column_id: ColumnId::new_for_test(9),
                }],
                output_qualifier: None,
            }),
            vec![values_node(2, 0)],
        );
        let fragments = vec![fragment(0, project, vec![column(9, "projected")])];
        let mut bindings = RuntimeFilterBindings::new();
        bindings
            .node_input_columns
            .insert((0, 1), vec![vec![column(1, "input")]]);
        let resolved = resolve_consumer_binding(
            &fragments,
            &bindings,
            &RuntimeFilterProbeBinding {
                node_id: 1,
                fragment_id: 0,
                intent: RuntimeFilterProbeIntent {
                    filter_id: 1,
                    probe_expr: column_expression(9, "projected"),
                },
            },
        )
        .expect("resolve Project consumer");
        assert_eq!(resolved.len(), 1);
        assert_eq!(resolved[0].node_id, 1);
        assert_eq!(
            expression_column_ids(&resolved[0].expression),
            vec![ColumnId::new_for_test(1)]
        );
    }

    #[test]
    fn rfd_5a_aggregate_accepts_group_key_and_rejects_aggregate_value() {
        let aggregate = node(
            1,
            0,
            DistributedNodeKind::HashAggregate(Box::new(PhysicalHashAggregateNode {
                mode: AggMode::Single,
                group_by: vec![column_expression(1, "input_key")],
                aggregates: Vec::new(),
                is_merge: Vec::new(),
                output_layout: AggregateOutputLayout::new(
                    vec![column(9, "group_key")],
                    vec![column(10, "aggregate_value")],
                ),
                output_columns: vec![column(9, "group_key"), column(10, "aggregate_value")],
            })),
            vec![values_node(2, 0)],
        );
        let fragments = vec![fragment(
            0,
            aggregate,
            vec![column(9, "group_key"), column(10, "aggregate_value")],
        )];
        let mut bindings = RuntimeFilterBindings::new();
        bindings
            .node_input_columns
            .insert((0, 1), vec![vec![column(1, "input_key")]]);
        let probe = |column_id, name| RuntimeFilterProbeBinding {
            node_id: 1,
            fragment_id: 0,
            intent: RuntimeFilterProbeIntent {
                filter_id: 1,
                probe_expr: column_expression(column_id, name),
            },
        };
        let group = resolve_consumer_binding(&fragments, &bindings, &probe(9, "group_key"))
            .expect("group key resolves");
        assert_eq!(
            expression_column_ids(&group[0].expression),
            vec![ColumnId::new_for_test(1)]
        );
        assert!(
            resolve_consumer_binding(&fragments, &bindings, &probe(10, "aggregate_value")).is_err()
        );
    }

    #[test]
    fn rfd_5a_union_creates_one_consumer_per_branch_mapping() {
        let union = node(
            1,
            0,
            DistributedNodeKind::SetOp(PhysicalSetOpNode {
                kind: PlanSetOpKind::UnionAll,
                output_columns: vec![column(9, "union_key")],
                child_output_columns: vec![
                    vec![column(1, "left_key")],
                    vec![column(2, "right_key")],
                ],
            }),
            vec![values_node(2, 0), values_node(3, 0)],
        );
        let fragments = vec![fragment(0, union, vec![column(9, "union_key")])];
        let bindings = RuntimeFilterBindings::new();
        let resolved = resolve_consumer_binding(
            &fragments,
            &bindings,
            &RuntimeFilterProbeBinding {
                node_id: 1,
                fragment_id: 0,
                intent: RuntimeFilterProbeIntent {
                    filter_id: 1,
                    probe_expr: column_expression(9, "union_key"),
                },
            },
        )
        .expect("resolve Union consumers");
        assert_eq!(resolved.len(), 2);
        assert_eq!(
            resolved
                .iter()
                .map(|binding| binding.node_id)
                .collect::<Vec<_>>(),
            vec![2, 3]
        );
        assert_eq!(
            expression_column_ids(&resolved[0].expression),
            vec![ColumnId::new_for_test(1)]
        );
        assert_eq!(
            expression_column_ids(&resolved[1].expression),
            vec![ColumnId::new_for_test(2)]
        );
    }

    #[test]
    fn rfd_5a_exchange_records_source_fragment_and_exact_source_expression() {
        let source = fragment(0, values_node(2, 0), vec![column(1, "source_key")]);
        let exchange = node(
            1,
            1,
            DistributedNodeKind::Exchange(ExchangeReceiver {
                partition: DataPartition::unpartitioned(),
                source_fragment_id: 0,
                output_columns: vec![column(9, "exchange_key")],
                output_qualifier: None,
                flavor: ExchangeFlavor::Distribution,
            }),
            Vec::new(),
        );
        let target = fragment(1, exchange, vec![column(9, "exchange_key")]);
        let bindings = RuntimeFilterBindings::new();
        let resolved = resolve_consumer_binding(
            &[source, target],
            &bindings,
            &RuntimeFilterProbeBinding {
                node_id: 1,
                fragment_id: 1,
                intent: RuntimeFilterProbeIntent {
                    filter_id: 1,
                    probe_expr: column_expression(9, "exchange_key"),
                },
            },
        )
        .expect("resolve Exchange consumer");
        assert_eq!(resolved.len(), 1);
        assert_eq!((resolved[0].fragment_id, resolved[0].node_id), (0, 2));
        assert_eq!(
            expression_column_ids(&resolved[0].expression),
            vec![ColumnId::new_for_test(1)]
        );
    }
}
