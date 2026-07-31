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

//! Structural validation of a distributed plan.
//!
//! The checks in this module run inside `seal_draft`, after the minimal
//! empty/root checks and before the immutable `DistributedPlan` is constructed.
//! They enforce the structural invariants that fragment/edge lowering relies on
//! (global node-id uniqueness, fragment/edge ownership, sink placement, and
//! finalized stream/router/CTE edge contracts) so codegen only ever observes a
//! plan whose shape is already sound.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt;

use crate::runtime_filter::model::contract::{
    BindingId, PlanFragmentId, PlanNodeId, RuntimeFilterLogicalDomain,
};
use crate::runtime_filter::model::graph::RuntimeFilterGraphData;
use crate::sql::analysis::{ExprKind, OutputColumn, TypedExpr};
use crate::sql::column_id::ColumnId;
use crate::sql::common::ChangeStreamBranchKind;

use super::{
    DataPartition, DataSink, DistributedNode, DistributedNodeKind, ExchangeFlavor,
    ExchangeReceiver, FragmentEdge, FragmentEdgeKind, FragmentId, FragmentStreamKind,
    PartitionKind, PlanFragment,
};

/// Structural validation failure carrying the exact diagnostic string.
///
/// The message text is load-bearing (fragment/edge lowering diagnostics and
/// regression tests match on it), so this type is a thin wrapper whose
/// `Display` reproduces the string verbatim. It is bridged into
/// `DistributedPlanSealError::Structural` at the seal boundary.
#[derive(Debug, PartialEq, Eq)]
pub(in crate::sql::planner::distributed) struct DistributedPlanValidationError(String);

impl fmt::Display for DistributedPlanValidationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

#[derive(Debug, PartialEq, Eq)]
pub(in crate::sql::planner::distributed) enum RuntimeFilterPlanValidationError {
    UnknownFragment(PlanFragmentId),
    UnknownNode {
        fragment_id: PlanFragmentId,
        node_id: PlanNodeId,
    },
    BindingNotAttached(BindingId),
    AttachedBindingUnknown(BindingId),
    BindingLocationMismatch(BindingId),
    ExpressionTypeMismatch(BindingId),
    DuplicateNodeBinding(BindingId),
}

impl fmt::Display for RuntimeFilterPlanValidationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownFragment(fragment_id) => write!(
                formatter,
                "runtime filter binding references unknown fragment id={}",
                fragment_id.get()
            ),
            Self::UnknownNode {
                fragment_id,
                node_id,
            } => write!(
                formatter,
                "runtime filter binding references unknown node id={} in fragment id={}",
                node_id.get(),
                fragment_id.get()
            ),
            Self::BindingNotAttached(binding_id) => write!(
                formatter,
                "runtime filter binding id={} is not attached to its plan node",
                binding_id.get()
            ),
            Self::AttachedBindingUnknown(binding_id) => write!(
                formatter,
                "plan node references unknown runtime filter binding id={}",
                binding_id.get()
            ),
            Self::BindingLocationMismatch(binding_id) => write!(
                formatter,
                "runtime filter binding id={} is attached outside its declared location",
                binding_id.get()
            ),
            Self::ExpressionTypeMismatch(binding_id) => write!(
                formatter,
                "runtime filter binding id={} expression type does not match its channel domain",
                binding_id.get()
            ),
            Self::DuplicateNodeBinding(binding_id) => write!(
                formatter,
                "runtime filter binding id={} is attached more than once",
                binding_id.get()
            ),
        }
    }
}

pub(in crate::sql::planner::distributed) fn validate_runtime_filter_graph_against_plan<A>(
    graph: &RuntimeFilterGraphData<A>,
    fragments: &[PlanFragment],
) -> Result<(), RuntimeFilterPlanValidationError> {
    let fragments_by_id = fragments
        .iter()
        .map(|fragment| (fragment.fragment_id, fragment))
        .collect::<BTreeMap<_, _>>();
    let mut attachments = BTreeMap::<BindingId, (FragmentId, i32)>::new();

    fn collect_attachments(
        node: &DistributedNode,
        fragment_id: FragmentId,
        attachments: &mut BTreeMap<BindingId, (FragmentId, i32)>,
    ) -> Result<(), RuntimeFilterPlanValidationError> {
        let mut local = BTreeSet::new();
        for binding_id in &node.runtime_filter_binding_ids {
            if !local.insert(*binding_id) || attachments.contains_key(binding_id) {
                return Err(RuntimeFilterPlanValidationError::DuplicateNodeBinding(
                    *binding_id,
                ));
            }
            attachments.insert(*binding_id, (fragment_id, node.node_id));
        }
        for child in &node.children {
            collect_attachments(child, fragment_id, attachments)?;
        }
        Ok(())
    }

    for fragment in fragments {
        collect_attachments(&fragment.root, fragment.fragment_id, &mut attachments)?;
    }

    for (binding_id, (fragment_id, node_id)) in &attachments {
        let Some(binding) = graph.binding(*binding_id) else {
            return Err(RuntimeFilterPlanValidationError::AttachedBindingUnknown(
                *binding_id,
            ));
        };
        if binding.location.fragment_id.get() != *fragment_id
            || binding.location.node_id.get() != *node_id
        {
            return Err(RuntimeFilterPlanValidationError::BindingLocationMismatch(
                *binding_id,
            ));
        }
    }

    for binding in graph.bindings() {
        let fragment_id = binding.location.fragment_id.get();
        let Some(fragment) = fragments_by_id.get(&fragment_id) else {
            return Err(RuntimeFilterPlanValidationError::UnknownFragment(
                binding.location.fragment_id,
            ));
        };
        fn contains_node(node: &DistributedNode, node_id: i32) -> bool {
            node.node_id == node_id
                || node
                    .children
                    .iter()
                    .any(|child| contains_node(child, node_id))
        }
        if !contains_node(&fragment.root, binding.location.node_id.get()) {
            return Err(RuntimeFilterPlanValidationError::UnknownNode {
                fragment_id: binding.location.fragment_id,
                node_id: binding.location.node_id,
            });
        }
        if !attachments.contains_key(&binding.binding_id) {
            return Err(RuntimeFilterPlanValidationError::BindingNotAttached(
                binding.binding_id,
            ));
        }
        let channel = graph
            .channel(binding.channel_id)
            .expect("structural graph validation guarantees channel ownership");
        let type_matches = match &channel.logical_domain {
            RuntimeFilterLogicalDomain::Membership { value_type, .. } => {
                value_type == &binding.expression.data_type
            }
            RuntimeFilterLogicalDomain::OrderedBound(order) => {
                order.keys.len() == 1 && order.keys[0].data_type == binding.expression.data_type
            }
        };
        if !type_matches {
            return Err(RuntimeFilterPlanValidationError::ExpressionTypeMismatch(
                binding.binding_id,
            ));
        }
    }
    Ok(())
}

/// Enforce the distributed-plan structural invariants over a draft's fragments,
/// resolved root fragment id, and edges.
///
/// Checks run in strict first-error order so diagnostics are deterministic.
pub(in crate::sql::planner::distributed) fn validate_distributed_structure(
    fragments: &[PlanFragment],
    root_fragment_id: FragmentId,
    edges: &[FragmentEdge],
) -> Result<(), DistributedPlanValidationError> {
    validate_structure(fragments, root_fragment_id, edges).map_err(DistributedPlanValidationError)
}

fn validate_structure(
    fragments: &[PlanFragment],
    root_fragment_id: FragmentId,
    edges: &[FragmentEdge],
) -> Result<(), String> {
    validate_global_node_ids(fragments)?;

    let mut fragments_by_id = BTreeMap::new();
    for fragment in fragments {
        if fragments_by_id
            .insert(fragment.fragment_id, fragment)
            .is_some()
        {
            return Err(format!(
                "lower_distributed_plan duplicate fragment id={}",
                fragment.fragment_id
            ));
        }
    }

    for fragment in fragments {
        ensure_unpartitioned("data_partition", &fragment.data_partition)?;
        if fragment.output_exprs.is_some() {
            return Err(format!(
                "lower_distributed_plan does not support fragment output_exprs for fragment id={}",
                fragment.fragment_id
            ));
        }
        validate_node_fragment_ownership(fragment.fragment_id, &fragment.root)?;

        if fragment.fragment_id == root_fragment_id {
            let root_sink_supported = match fragment.sink {
                DataSink::Result
                | DataSink::Statistics(_)
                | DataSink::ConnectorWrite(_)
                | DataSink::ChangeStreamRouter(_) => true,
                _ => false,
            };
            if !root_sink_supported {
                return Err(format!(
                    "lower_distributed_plan root fragment id={} must use result, statistics, connector write, or change-stream router sink",
                    fragment.fragment_id
                ));
            }
            ensure_unpartitioned("root output_partition", &fragment.output_partition)?;
        } else {
            let non_root_sink_supported = match fragment.sink {
                DataSink::Noop | DataSink::ConnectorWrite(_) => true,
                _ => false,
            };
            if non_root_sink_supported {
                continue;
            }
            return Err(format!(
                "lower_distributed_plan non-root fragment id={} must use noop or Iceberg write sink",
                fragment.fragment_id
            ));
        }
    }

    validate_source_edge_shape(edges)?;

    let mut router_target_partitions = BTreeMap::new();
    for edge in edges {
        if !fragments_by_id.contains_key(&edge.source_fragment_id) {
            return Err(format!(
                "lower_distributed_plan edge references missing source fragment id={}",
                edge.source_fragment_id
            ));
        }
        if !fragments_by_id.contains_key(&edge.target_fragment_id) {
            return Err(format!(
                "lower_distributed_plan edge references missing target fragment id={}",
                edge.target_fragment_id
            ));
        }
        let exchange = target_exchange_for_edge(&fragments_by_id, edge)?;
        validate_edge_stream_partition(edge)?;
        validate_finalized_edge(
            &fragments_by_id,
            edge,
            exchange,
            &mut router_target_partitions,
        )?;
    }
    Ok(())
}

fn validate_global_node_ids(fragments: &[PlanFragment]) -> Result<(), String> {
    fn visit(
        node: &DistributedNode,
        fragment_id: FragmentId,
        owners: &mut HashMap<i32, FragmentId>,
    ) -> Result<(), String> {
        if let Some(previous_fragment_id) = owners.insert(node.node_id, fragment_id) {
            return Err(format!(
                "DistributedPlan contains duplicate node_id={} in fragments {} and {}",
                node.node_id, previous_fragment_id, fragment_id
            ));
        }
        for child in &node.children {
            visit(child, fragment_id, owners)?;
        }
        Ok(())
    }

    let mut owners = HashMap::new();
    for fragment in fragments {
        visit(&fragment.root, fragment.fragment_id, &mut owners)?;
    }
    Ok(())
}

/// Validate the per-source shape of the edge multiset.
///
/// These are graph-level invariants that the per-edge finalized checks cannot
/// see because they inspect one edge at a time. A plain `Stream` source has a
/// single sink, so it may drive at most one plain stream edge and must not also
/// drive a router edge; CTE multicast is exempt because fanning one producer out
/// to many receivers is its entire purpose. A router source addresses exactly
/// one router group, and within that group every branch edge must be uniquely
/// addressable per field: no two edges may repeat the same `branch_id`, the same
/// `branch_kind`, or the same target exchange (`target_fragment_id`,
/// `target_exchange_node_id`). This per-field uniqueness is strictly stronger
/// than full-tuple route uniqueness (it also rejects, e.g., two edges that share
/// a `branch_id` but differ in `branch_kind` or target), so the execution
/// coordinator can trust the sealed shape instead of re-validating any of these
/// facts at dispatch time.
fn validate_source_edge_shape(edges: &[FragmentEdge]) -> Result<(), String> {
    let mut plain_stream_sources: BTreeSet<FragmentId> = BTreeSet::new();
    let mut router_sources: BTreeSet<FragmentId> = BTreeSet::new();
    let mut router_groups_by_source: BTreeMap<FragmentId, BTreeSet<i32>> = BTreeMap::new();
    // Per-(source, group) uniqueness ledgers. Keying on the router group id keeps
    // this aligned with the router sink template even though a source is limited
    // to a single group by the check above.
    let mut branch_ids_by_group: BTreeMap<(FragmentId, i32), BTreeSet<i32>> = BTreeMap::new();
    let mut branch_kinds_by_group: BTreeMap<(FragmentId, i32), BTreeSet<ChangeStreamBranchKind>> =
        BTreeMap::new();
    let mut target_exchanges_by_group: BTreeMap<(FragmentId, i32), BTreeSet<(FragmentId, i32)>> =
        BTreeMap::new();

    for edge in edges {
        match &edge.edge_kind {
            FragmentEdgeKind::Stream => {
                if !plain_stream_sources.insert(edge.source_fragment_id) {
                    return Err(format!(
                        "lower_distributed_plan source fragment id={} has ambiguous plain stream fan-out (more than one outgoing stream edge)",
                        edge.source_fragment_id
                    ));
                }
            }
            // CTE multicast is an intentional one-producer-to-many-receivers
            // fan-out, so it is exempt from the plain-stream fan-out rule.
            FragmentEdgeKind::CteMulticast { .. } => {}
            FragmentEdgeKind::ChangeStreamRouter {
                router_group_id,
                branch_id,
                branch_kind,
            } => {
                router_sources.insert(edge.source_fragment_id);
                let groups = router_groups_by_source
                    .entry(edge.source_fragment_id)
                    .or_default();
                groups.insert(*router_group_id);
                if groups.len() > 1 {
                    return Err(format!(
                        "lower_distributed_plan source fragment id={} uses more than one router group",
                        edge.source_fragment_id
                    ));
                }
                // Per-field uniqueness within the (source, group). Each check is
                // strictly stronger than full-tuple route uniqueness, so the
                // coordinator can trust the sealed shape rather than re-checking
                // branch id / kind / target exchange at dispatch time.
                let group_key = (edge.source_fragment_id, *router_group_id);
                if !branch_ids_by_group
                    .entry(group_key)
                    .or_default()
                    .insert(*branch_id)
                {
                    return Err(format!(
                        "lower_distributed_plan router group source_fragment_id={} router_group_id={} repeats branch_id={}",
                        edge.source_fragment_id, router_group_id, branch_id
                    ));
                }
                if !branch_kinds_by_group
                    .entry(group_key)
                    .or_default()
                    .insert(*branch_kind)
                {
                    return Err(format!(
                        "lower_distributed_plan router group source_fragment_id={} router_group_id={} repeats branch_kind={:?}",
                        edge.source_fragment_id, router_group_id, branch_kind
                    ));
                }
                let target_exchange = (edge.target_fragment_id, edge.target_exchange_node_id);
                if !target_exchanges_by_group
                    .entry(group_key)
                    .or_default()
                    .insert(target_exchange)
                {
                    return Err(format!(
                        "lower_distributed_plan router group source_fragment_id={} router_group_id={} repeats target exchange target_fragment_id={} target_exchange_node_id={}",
                        edge.source_fragment_id,
                        router_group_id,
                        edge.target_fragment_id,
                        edge.target_exchange_node_id
                    ));
                }
            }
        }
        if plain_stream_sources.contains(&edge.source_fragment_id)
            && router_sources.contains(&edge.source_fragment_id)
        {
            return Err(format!(
                "lower_distributed_plan source fragment id={} mixes plain stream and router edges",
                edge.source_fragment_id
            ));
        }
    }
    Ok(())
}

fn validate_finalized_edge(
    fragments_by_id: &BTreeMap<FragmentId, &PlanFragment>,
    edge: &FragmentEdge,
    exchange: &ExchangeReceiver,
    router_target_partitions: &mut BTreeMap<(FragmentId, i32), DataPartition>,
) -> Result<(), String> {
    let source = fragments_by_id
        .get(&edge.source_fragment_id)
        .copied()
        .ok_or_else(|| edge_error(edge, "references a missing source fragment"))?;
    match &edge.edge_kind {
        FragmentEdgeKind::Stream => {
            validate_partition_shape(&edge.output_partition, edge, "edge.output_partition")?;
            ensure_partition_equivalent(
                edge,
                "edge.output_partition",
                &edge.output_partition,
                "target Exchange.partition",
                &exchange.partition,
            )?;
            ensure_partition_equivalent(
                edge,
                "source fragment.output_partition",
                &source.output_partition,
                "edge.output_partition",
                &edge.output_partition,
            )
        }
        FragmentEdgeKind::CteMulticast {
            receive_producer_column_ids,
            ..
        } => {
            ensure_partition_equivalent(
                edge,
                "edge.output_partition",
                &edge.output_partition,
                "target Exchange.partition",
                &exchange.partition,
            )?;
            if receive_producer_column_ids.len() != exchange.output_columns.len() {
                return Err(edge_error(
                    edge,
                    &format!(
                        "CTE receive/output arity mismatch: receive_producer_column_ids={} Exchange.output_columns={}",
                        receive_producer_column_ids.len(),
                        exchange.output_columns.len()
                    ),
                ));
            }
            for (index, (producer_id, output)) in receive_producer_column_ids
                .iter()
                .zip(&exchange.output_columns)
                .enumerate()
            {
                if *producer_id != output.column_id {
                    return Err(edge_error(
                        edge,
                        &format!(
                            "CTE Exchange output mapping mismatch at index {index}: receive producer column {} Exchange output column {}",
                            producer_id.0, output.column_id.0
                        ),
                    ));
                }
            }
            let expected_slots = checked_output_slot_ids(
                receive_producer_column_ids,
                edge,
                "CTE receive producer columns",
            )?;
            if edge.output_slot_ids != expected_slots {
                return Err(edge_error(
                    edge,
                    &format!(
                        "CTE output_slot_ids mismatch: edge={:?} expected={expected_slots:?}",
                        edge.output_slot_ids
                    ),
                ));
            }
            Ok(())
        }
        FragmentEdgeKind::ChangeStreamRouter {
            router_group_id,
            branch_id,
            branch_kind,
        } => {
            let DataSink::ChangeStreamRouter(router) = &source.sink else {
                return Err(edge_error(
                    edge,
                    "router source fragment does not use ChangeStreamRouter sink",
                ));
            };
            let route = router
                .branches
                .iter()
                .find(|route| {
                    router.group_id == *router_group_id
                        && route.branch_id == *branch_id
                        && route.branch_kind == *branch_kind
                        && route.target_fragment_id == edge.target_fragment_id
                        && route.target_exchange_node_id == edge.target_exchange_node_id
                })
                .ok_or_else(|| edge_error(edge, "has no exact matching router branch route"))?;
            let expected_slots = checked_output_slot_ids_for_ordinals(
                &source.output_columns,
                &route.output_ordinals,
                edge,
                "router output",
            )?;
            if edge.output_slot_ids != expected_slots {
                return Err(edge_error(
                    edge,
                    &format!(
                        "router output_slot_ids mismatch: edge={:?} expected={expected_slots:?}",
                        edge.output_slot_ids
                    ),
                ));
            }
            let route_partition = partition_for_output_ordinals(
                &source.output_columns,
                &route.output_partition_ordinals,
                edge,
                "router output partition",
            )?;
            ensure_partition_equivalent(
                edge,
                "edge.output_partition",
                &edge.output_partition,
                "router route partition",
                &route_partition,
            )?;
            // Defense-in-depth only: this conflicting-partition backstop is now
            // subsumed by `validate_source_edge_shape`'s per-(source, router
            // group) target-exchange uniqueness. An exchange node has a single
            // fixed source and a source drives at most one router group, so two
            // router edges can never reach one target exchange to begin with;
            // the shape check rejects that up front, before this per-edge loop
            // runs. Retained inside the seal so a future reordering cannot let a
            // partition conflict slip through unnoticed.
            let target_key = (edge.target_fragment_id, edge.target_exchange_node_id);
            if let Some(existing) = router_target_partitions.get(&target_key)
                && !partition_equivalent(existing, &edge.output_partition)
            {
                return Err(edge_error(
                    edge,
                    "router edges have conflicting partitions for the same target Exchange",
                ));
            }
            router_target_partitions.insert(target_key, edge.output_partition.clone());
            ensure_partition_equivalent(
                edge,
                "edge.output_partition",
                &edge.output_partition,
                "target Exchange.partition",
                &exchange.partition,
            )
        }
    }
}

fn validate_partition_shape(
    partition: &DataPartition,
    edge: &FragmentEdge,
    label: &str,
) -> Result<(), String> {
    if matches!(partition.kind, PartitionKind::Hash) && partition.exprs.is_empty() {
        return Err(edge_error(
            edge,
            &format!("{label} HASH expressions are empty"),
        ));
    }
    Ok(())
}

fn ensure_partition_equivalent(
    edge: &FragmentEdge,
    left_label: &str,
    left: &DataPartition,
    right_label: &str,
    right: &DataPartition,
) -> Result<(), String> {
    if partition_equivalent(left, right) {
        return Ok(());
    }
    Err(edge_error(
        edge,
        &format!("partition mismatch: {left_label} is not equivalent to {right_label}"),
    ))
}

/// Planner-local semantic equivalence of two data partitions.
///
/// This replaces the previous protobuf round-trip comparison
/// (`encode_data_partition(a) == encode_data_partition(b)`) so structural
/// validation no longer depends on the codegen proto encoder. It reproduces
/// exactly the fields the proto comparison distinguished: the partition kind
/// plus, for each ordered key expression, its data type, nullability, and (for
/// the column references partition keys always are) column id, qualifier, and
/// column name.
fn partition_equivalent(left: &DataPartition, right: &DataPartition) -> bool {
    std::mem::discriminant(&left.kind) == std::mem::discriminant(&right.kind)
        && left.exprs.len() == right.exprs.len()
        && left
            .exprs
            .iter()
            .zip(&right.exprs)
            .all(|(left_expr, right_expr)| partition_expr_equivalent(left_expr, right_expr))
}

fn partition_expr_equivalent(left: &TypedExpr, right: &TypedExpr) -> bool {
    if left.nullable != right.nullable || left.data_type != right.data_type {
        return false;
    }
    match (&left.kind, &right.kind) {
        (
            ExprKind::ColumnRef {
                column_id: left_id,
                qualifier: left_qualifier,
                column: left_column,
            },
            ExprKind::ColumnRef {
                column_id: right_id,
                qualifier: right_qualifier,
                column: right_column,
            },
        ) => {
            left_id == right_id && left_qualifier == right_qualifier && left_column == right_column
        }
        // Partition key expressions are always column references in NovaRocks
        // distributed planning (see `partition_for_output_ordinals` and the
        // join-reorder fixtures). Fall back to a conservative structural
        // comparison for any other shape so the check never silently treats
        // distinct expressions as equivalent. The fallback compares
        // `format!("{:?}")` rather than `==` because `TypedExpr`/`ExprKind`
        // derive only `Clone, Debug` and do not implement `PartialEq` (see
        // `src/sql/analysis/mod.rs`).
        (left_kind, right_kind) => format!("{left_kind:?}") == format!("{right_kind:?}"),
    }
}

fn checked_output_slot_ids(
    column_ids: &[ColumnId],
    edge: &FragmentEdge,
    label: &str,
) -> Result<Vec<i32>, String> {
    column_ids
        .iter()
        .map(|column_id| {
            i32::try_from(column_id.0).map_err(|_| {
                edge_error(
                    edge,
                    &format!(
                        "{label} column {} cannot convert to output slot id",
                        column_id.0
                    ),
                )
            })
        })
        .collect()
}

fn checked_output_slot_ids_for_ordinals(
    columns: &[OutputColumn],
    ordinals: &[usize],
    edge: &FragmentEdge,
    label: &str,
) -> Result<Vec<i32>, String> {
    let ids = ordinals
        .iter()
        .map(|ordinal| {
            columns
                .get(*ordinal)
                .map(|column| column.column_id)
                .ok_or_else(|| {
                    edge_error(edge, &format!("{label} ordinal {ordinal} is out of range"))
                })
        })
        .collect::<Result<Vec<_>, _>>()?;
    checked_output_slot_ids(&ids, edge, label)
}

fn partition_for_output_ordinals(
    columns: &[OutputColumn],
    ordinals: &[usize],
    edge: &FragmentEdge,
    label: &str,
) -> Result<DataPartition, String> {
    if ordinals.is_empty() {
        return Ok(DataPartition::unpartitioned());
    }
    let exprs = ordinals
        .iter()
        .map(|ordinal| {
            let column = columns.get(*ordinal).ok_or_else(|| {
                edge_error(edge, &format!("{label} ordinal {ordinal} is out of range"))
            })?;
            Ok(TypedExpr {
                kind: ExprKind::ColumnRef {
                    column_id: column.column_id,
                    qualifier: None,
                    column: column.name.clone(),
                },
                data_type: column.data_type.clone(),
                nullable: column.nullable,
            })
        })
        .collect::<Result<Vec<_>, String>>()?;
    Ok(DataPartition {
        kind: PartitionKind::Hash,
        exprs,
    })
}

fn edge_error(edge: &FragmentEdge, detail: &str) -> String {
    format!(
        "lower_distributed_plan {} edge source_fragment_id={} target_fragment_id={} target_exchange_node_id={}: {detail}",
        fragment_edge_kind_label(&edge.edge_kind),
        edge.source_fragment_id,
        edge.target_fragment_id,
        edge.target_exchange_node_id,
    )
}

fn validate_edge_stream_partition(edge: &FragmentEdge) -> Result<(), String> {
    let valid = matches!(
        (edge.output_partition.kind, edge.stream_kind),
        (
            PartitionKind::Unpartitioned,
            FragmentStreamKind::Gather | FragmentStreamKind::Broadcast
        ) | (PartitionKind::Random, FragmentStreamKind::Other)
            | (PartitionKind::Hash, FragmentStreamKind::Partitioned)
    );
    if valid {
        return Ok(());
    }
    Err(format!(
        "{} edge source_fragment_id={} target_fragment_id={} target_exchange_node_id={} has invalid stream/partition combination: partition_kind={:?} stream_kind={:?}",
        fragment_edge_kind_label(&edge.edge_kind),
        edge.source_fragment_id,
        edge.target_fragment_id,
        edge.target_exchange_node_id,
        edge.output_partition.kind,
        edge.stream_kind,
    ))
}

fn validate_node_fragment_ownership(
    fragment_id: FragmentId,
    node: &DistributedNode,
) -> Result<(), String> {
    if node.fragment_id != fragment_id {
        return Err(format!(
            "lower_distributed_plan fragment id={} contains node_id={} with fragment_id={}",
            fragment_id, node.node_id, node.fragment_id
        ));
    }
    for child in &node.children {
        validate_node_fragment_ownership(fragment_id, child)?;
    }
    Ok(())
}

fn ensure_unpartitioned(label: &str, partition: &DataPartition) -> Result<(), String> {
    if !matches!(partition.kind, PartitionKind::Unpartitioned) || !partition.exprs.is_empty() {
        return Err(format!(
            "lower_distributed_plan supports only unpartitioned {label}"
        ));
    }
    Ok(())
}

fn target_exchange_for_edge<'a>(
    fragments_by_id: &BTreeMap<FragmentId, &'a PlanFragment>,
    edge: &FragmentEdge,
) -> Result<&'a ExchangeReceiver, String> {
    let target = fragments_by_id
        .get(&edge.target_fragment_id)
        .ok_or_else(|| {
            format!(
                "lower_distributed_plan edge references missing target fragment id={}",
                edge.target_fragment_id
            )
        })?;
    let exchange = find_exchange_node(&target.root, edge.target_exchange_node_id).ok_or_else(|| {
        format!(
            "lower_distributed_plan edge target_exchange_node_id={} not found in target fragment id={}",
            edge.target_exchange_node_id, edge.target_fragment_id
        )
    })?;
    let DistributedNodeKind::Exchange(exchange) = &exchange.payload else {
        return Err(format!(
            "lower_distributed_plan edge target_exchange_node_id={} in target fragment id={} must target Exchange",
            edge.target_exchange_node_id, edge.target_fragment_id
        ));
    };
    if edge.source_fragment_id != exchange.source_fragment_id {
        return Err(format!(
            "lower_distributed_plan {} edge source_fragment_id={} does not match Exchange source_fragment_id={} for target_exchange_node_id={} in target fragment id={}",
            fragment_edge_kind_label(&edge.edge_kind),
            edge.source_fragment_id,
            exchange.source_fragment_id,
            edge.target_exchange_node_id,
            edge.target_fragment_id
        ));
    }
    validate_exchange_partition(&exchange.partition)?;
    match (&edge.edge_kind, &exchange.flavor) {
        (FragmentEdgeKind::Stream, ExchangeFlavor::Distribution)
        | (FragmentEdgeKind::Stream, ExchangeFlavor::LimitOffset { .. })
        | (FragmentEdgeKind::Stream, ExchangeFlavor::TopNSplit { .. }) => {}
        (
            FragmentEdgeKind::CteMulticast {
                cte_id,
                receive_producer_column_ids,
            },
            ExchangeFlavor::CteMulticast {
                cte_id: exchange_cte_id,
                receive_producer_column_ids: exchange_ids,
            },
        ) => {
            if cte_id != exchange_cte_id || receive_producer_column_ids != exchange_ids {
                return Err(format!(
                    "lower_distributed_plan CTE multicast edge metadata does not match Exchange metadata for target_exchange_node_id={} in target fragment id={}",
                    edge.target_exchange_node_id, edge.target_fragment_id
                ));
            }
        }
        (FragmentEdgeKind::ChangeStreamRouter { .. }, ExchangeFlavor::Distribution) => {}
        (FragmentEdgeKind::Stream, _) => {
            return Err(format!(
                "lower_distributed_plan stream edge target_exchange_node_id={} in target fragment id={} must target stream Exchange",
                edge.target_exchange_node_id, edge.target_fragment_id
            ));
        }
        (FragmentEdgeKind::CteMulticast { .. }, _) => {
            return Err(format!(
                "lower_distributed_plan CTE multicast edge target_exchange_node_id={} in target fragment id={} must target Exchange(CteMulticast)",
                edge.target_exchange_node_id, edge.target_fragment_id
            ));
        }
        (FragmentEdgeKind::ChangeStreamRouter { .. }, _) => {
            return Err(format!(
                "lower_distributed_plan Iceberg change-stream router edge target_exchange_node_id={} in target fragment id={} must target Exchange(Distribution)",
                edge.target_exchange_node_id, edge.target_fragment_id
            ));
        }
    }
    Ok(exchange)
}

fn fragment_edge_kind_label(edge_kind: &FragmentEdgeKind) -> &'static str {
    match edge_kind {
        FragmentEdgeKind::Stream => "stream",
        FragmentEdgeKind::CteMulticast { .. } => "CTE multicast",
        FragmentEdgeKind::ChangeStreamRouter { .. } => "Iceberg change-stream router",
    }
}

fn validate_exchange_partition(partition: &DataPartition) -> Result<(), String> {
    if matches!(partition.kind, PartitionKind::Hash) && partition.exprs.is_empty() {
        return Err(
            "DistributedPlan HASH Exchange has no native partition expressions".to_string(),
        );
    }
    Ok(())
}

fn find_exchange_node(node: &DistributedNode, node_id: i32) -> Option<&DistributedNode> {
    if node.node_id == node_id {
        return Some(node);
    }
    for child in &node.children {
        if let Some(found) = find_exchange_node(child, node_id) {
            return Some(found);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;

    use crate::sql::analysis::cte::CteId;
    use crate::sql::analysis::{ExprKind, OutputColumn as AnalysisOutputColumn, TypedExpr};
    use crate::sql::column_id::ColumnId;
    use crate::sql::common::ChangeStreamBranchKind;
    use crate::sql::planner::distributed::test_support::{
        DistributedPlanDraftBuilder, distributed_plan_draft_builder_for_test,
        distributed_plan_for_test, draft_builder_from_plan,
    };
    use crate::sql::planner::distributed::{
        DataPartition, DataSink, DistributedNode, DistributedNodeKind, DistributedPlan,
        ExchangeFlavor, ExchangeReceiver, FragmentEdge, FragmentEdgeKind, FragmentStreamKind,
        PartitionKind, PlanFragment,
    };
    use crate::sql::planner::payload::PlanValuesNode;
    use crate::sql::planner::physical::{PhysicalPlanStats, PlannerConfidence};

    fn stats() -> PhysicalPlanStats {
        PhysicalPlanStats {
            output_row_count: 0.0,
            row_count_confidence: PlannerConfidence::Fallback,
            column_statistics: Default::default(),
            cost_estimate: None,
            broadcast_decision: None,
        }
    }

    fn output_col(id: u32, name: &str) -> AnalysisOutputColumn {
        AnalysisOutputColumn {
            column_id: ColumnId::new_for_test(id),
            name: name.to_string(),
            data_type: DataType::Int64,
            nullable: false,
            is_internal: false,
        }
    }

    fn physical_values_node(
        fragment_id: u32,
        node_id: i32,
        columns: Vec<AnalysisOutputColumn>,
    ) -> DistributedNode {
        DistributedNode {
            node_id,
            fragment_id,
            tuple_ids: vec![node_id],
            nullable_tuple_ids: Vec::new(),
            limit: -1,
            runtime_filter_binding_ids: Vec::new(),
            children: Vec::new(),
            stats: stats(),
            payload: DistributedNodeKind::Values(PlanValuesNode {
                rows: Vec::new(),
                columns,
            }),
        }
    }

    fn stream_exchange_plan(flavor: ExchangeFlavor) -> DistributedPlan {
        let columns = vec![output_col(1, "k")];
        let producer_fragment_id = 1;
        let consumer_fragment_id = 0;
        let exchange_node_id = 20;
        let producer_fragment = PlanFragment {
            fragment_id: producer_fragment_id,
            root: physical_values_node(producer_fragment_id, 10, columns.clone()),
            data_partition: DataPartition::unpartitioned(),
            output_partition: DataPartition::unpartitioned(),
            sink: DataSink::Noop,
            output_exprs: None,
            output_columns: columns.clone(),
            cte_id: None,
            cte_exchange_nodes: Vec::new(),
        };
        let consumer_fragment = PlanFragment {
            fragment_id: consumer_fragment_id,
            root: DistributedNode {
                node_id: exchange_node_id,
                fragment_id: consumer_fragment_id,
                tuple_ids: vec![exchange_node_id],
                nullable_tuple_ids: Vec::new(),
                limit: -1,
                runtime_filter_binding_ids: Vec::new(),
                children: Vec::new(),
                stats: stats(),
                payload: DistributedNodeKind::Exchange(ExchangeReceiver {
                    partition: DataPartition::unpartitioned(),
                    source_fragment_id: producer_fragment_id,
                    output_columns: columns.clone(),
                    output_qualifier: None,
                    flavor,
                }),
            },
            data_partition: DataPartition::unpartitioned(),
            output_partition: DataPartition::unpartitioned(),
            sink: DataSink::Result,
            output_exprs: None,
            output_columns: columns,
            cte_id: None,
            cte_exchange_nodes: Vec::new(),
        };
        distributed_plan_for_test! {
            fragments: vec![producer_fragment, consumer_fragment],
            root_fragment_id: consumer_fragment_id,
            runtime_filter_graph: Default::default(),
            edges: vec![FragmentEdge {
                source_fragment_id: producer_fragment_id,
                target_fragment_id: consumer_fragment_id,
                target_exchange_node_id: exchange_node_id,
                output_partition: DataPartition::unpartitioned(),
                stream_kind: FragmentStreamKind::Gather,
                edge_kind: FragmentEdgeKind::Stream,
                output_slot_ids: vec![1],
            }],
        }
    }

    fn finalized_router_plan() -> DistributedPlan {
        let output_columns = vec![
            output_col(1, "op"),
            output_col(2, "route"),
            output_col(3, "delete_id"),
        ];
        let dp = distributed_plan_draft_builder_for_test! {
            fragments: vec![PlanFragment {
                fragment_id: 0,
                root: physical_values_node(0, 10, output_columns.clone()),
                data_partition: DataPartition::unpartitioned(),
                output_partition: DataPartition::unpartitioned(),
                sink: DataSink::Result,
                output_exprs: None,
                output_columns,
                cte_id: None,
                cte_exchange_nodes: Vec::new(),
            }],
            root_fragment_id: 0,
            runtime_filter_graph: Default::default(),
            edges: Vec::new(),
        };
        let mut branch =
            crate::sql::planner::distributed::write::change_stream::ChangeStreamWriteBranchSpec::delete_dv_for_test(vec![2]);
        branch.output_partition_ordinals = vec![2];
        branch.sink_spec.iceberg.serialized_metadata = Some(
            crate::sql::planner::distributed::write::sink::test_support::unpartitioned_metadata_json(),
        );
        let dag =
            crate::sql::planner::distributed::write::change_stream::ChangeStreamWriteDagSpec::for_test(Some(0), None, vec![branch]);
        crate::sql::planner::distributed::write::plan::finalize_iceberg_change_stream_test_plan(
            dp, "test_db", dag,
        )
        .expect("plan change-stream write")
    }

    fn cte_multicast_plan() -> DistributedPlan {
        let cte_id: CteId = 7;
        let producer_columns = vec![
            output_col(1, "k"),
            output_col(2, "v"),
            output_col(3, "payload"),
        ];
        let receive_columns = vec![producer_columns[0].clone(), producer_columns[2].clone()];
        let receive_producer_column_ids =
            vec![producer_columns[0].column_id, producer_columns[2].column_id];

        let producer_fragment_id = 1;
        let consumer_fragment_id = 0;
        let exchange_node_id = 20;
        let producer_fragment = PlanFragment {
            fragment_id: producer_fragment_id,
            root: physical_values_node(producer_fragment_id, 10, producer_columns.clone()),
            data_partition: DataPartition::unpartitioned(),
            output_partition: DataPartition::unpartitioned(),
            sink: DataSink::Noop,
            output_exprs: None,
            output_columns: producer_columns,
            cte_id: Some(cte_id),
            cte_exchange_nodes: Vec::new(),
        };
        let consumer_fragment = PlanFragment {
            fragment_id: consumer_fragment_id,
            root: DistributedNode {
                node_id: exchange_node_id,
                fragment_id: consumer_fragment_id,
                tuple_ids: vec![exchange_node_id],
                nullable_tuple_ids: Vec::new(),
                limit: -1,
                runtime_filter_binding_ids: Vec::new(),
                children: Vec::new(),
                stats: stats(),
                payload: DistributedNodeKind::Exchange(ExchangeReceiver {
                    partition: DataPartition::unpartitioned(),
                    source_fragment_id: producer_fragment_id,
                    output_columns: receive_columns.clone(),
                    output_qualifier: Some("c".to_string()),
                    flavor: ExchangeFlavor::CteMulticast {
                        cte_id,
                        receive_producer_column_ids: receive_producer_column_ids.clone(),
                    },
                }),
            },
            data_partition: DataPartition::unpartitioned(),
            output_partition: DataPartition::unpartitioned(),
            sink: DataSink::Result,
            output_exprs: None,
            output_columns: receive_columns,
            cte_id: None,
            cte_exchange_nodes: vec![(
                cte_id,
                exchange_node_id,
                receive_producer_column_ids.clone(),
            )],
        };
        distributed_plan_for_test! {
            fragments: vec![producer_fragment, consumer_fragment],
            root_fragment_id: consumer_fragment_id,
            runtime_filter_graph: Default::default(),
            edges: vec![FragmentEdge {
                source_fragment_id: producer_fragment_id,
                target_fragment_id: consumer_fragment_id,
                target_exchange_node_id: exchange_node_id,
                output_partition: DataPartition::unpartitioned(),
                stream_kind: FragmentStreamKind::Gather,
                edge_kind: FragmentEdgeKind::CteMulticast {
                    cte_id,
                    receive_producer_column_ids,
                },
                output_slot_ids: vec![1, 3],
            }],
        }
    }

    #[test]
    fn distributed_plan_rejects_duplicate_node_id_across_fragments() {
        fn values_node(fragment_id: u32, node_id: i32) -> DistributedNode {
            DistributedNode {
                node_id,
                fragment_id,
                tuple_ids: Vec::new(),
                nullable_tuple_ids: Vec::new(),
                limit: -1,
                runtime_filter_binding_ids: Vec::new(),
                children: Vec::new(),
                stats: stats(),
                payload: DistributedNodeKind::Values(PlanValuesNode {
                    rows: Vec::new(),
                    columns: Vec::new(),
                }),
            }
        }

        let fragments = [0, 1]
            .into_iter()
            .map(|fragment_id| PlanFragment {
                fragment_id,
                root: values_node(fragment_id, 7),
                data_partition: DataPartition::unpartitioned(),
                output_partition: DataPartition::unpartitioned(),
                sink: DataSink::Noop,
                output_exprs: None,
                output_columns: Vec::new(),
                cte_id: None,
                cte_exchange_nodes: Vec::new(),
            })
            .collect();
        let builder = distributed_plan_draft_builder_for_test! {
            fragments,
            root_fragment_id: 0,
            runtime_filter_graph: Default::default(),
            edges: Vec::new(),
        };

        let err = builder
            .seal()
            .expect_err("node ids are global descriptor keys and must be unique");
        assert!(err.contains("duplicate node_id=7"), "{err}");
        assert!(err.contains("fragments 0 and 1"), "{err}");
    }

    #[test]
    fn unpartitioned_stream_rejects_other_and_partitioned_with_edge_context() {
        for stream_kind in [FragmentStreamKind::Other, FragmentStreamKind::Partitioned] {
            let mut builder = draft_builder_from_plan(
                &stream_exchange_plan(ExchangeFlavor::Distribution),
                Default::default(),
            );
            builder.edges_mut()[0].stream_kind = stream_kind;

            let err = builder
                .seal()
                .expect_err("Unpartitioned edge with non-gather/broadcast stream must fail");
            assert!(err.contains("Unpartitioned"), "{err}");
            assert!(err.contains(&format!("{stream_kind:?}")), "{err}");
            assert!(err.contains("source_fragment_id=1"), "{err}");
            assert!(err.contains("target_fragment_id=0"), "{err}");
            assert!(err.contains("target_exchange_node_id=20"), "{err}");
        }
    }

    #[test]
    fn finalized_stream_validation_rejects_stale_partition_contracts() {
        let mut empty_hash = draft_builder_from_plan(
            &stream_exchange_plan(ExchangeFlavor::Distribution),
            Default::default(),
        );
        empty_hash.edges_mut()[0].output_partition = DataPartition {
            kind: PartitionKind::Hash,
            exprs: Vec::new(),
        };
        empty_hash.edges_mut()[0].stream_kind = FragmentStreamKind::Partitioned;
        let err = empty_hash
            .seal()
            .expect_err("empty HASH edge partition must fail");
        assert!(
            err.contains(
                "stream edge source_fragment_id=1 target_fragment_id=0 target_exchange_node_id=20"
            ),
            "{err}"
        );
        assert!(
            err.contains("edge.output_partition HASH expressions are empty"),
            "{err}"
        );

        let random = DataPartition {
            kind: PartitionKind::Random,
            exprs: Vec::new(),
        };
        let mut receiver_mismatch = draft_builder_from_plan(
            &stream_exchange_plan(ExchangeFlavor::Distribution),
            Default::default(),
        );
        receiver_mismatch.fragments_mut()[0].output_partition = random.clone();
        receiver_mismatch.edges_mut()[0].output_partition = random.clone();
        receiver_mismatch.edges_mut()[0].stream_kind = FragmentStreamKind::Other;
        let err = receiver_mismatch
            .seal()
            .expect_err("edge and target Exchange partition mismatch must fail");
        assert!(
            err.contains(
                "partition mismatch: edge.output_partition is not equivalent to target Exchange.partition"
            ),
            "{err}"
        );

        let mut source_mismatch = draft_builder_from_plan(
            &stream_exchange_plan(ExchangeFlavor::Distribution),
            Default::default(),
        );
        source_mismatch.fragments_mut()[0].output_partition = DataPartition {
            kind: PartitionKind::Random,
            exprs: Vec::new(),
        };
        let err = source_mismatch
            .seal()
            .expect_err("source fragment and edge partition mismatch must fail");
        assert!(
            err.contains(
                "partition mismatch: source fragment.output_partition is not equivalent to edge.output_partition"
            ),
            "{err}"
        );
    }

    #[test]
    fn finalized_router_validation_rejects_stale_contracts() {
        let planned = finalized_router_plan();
        let source_fragment_id = planned.edges()[0].source_fragment_id;
        let target_fragment_id = planned.edges()[0].target_fragment_id;

        let mut wrong_sink = draft_builder_from_plan(&planned, Default::default());
        wrong_sink
            .fragments_mut()
            .iter_mut()
            .find(|fragment| fragment.fragment_id == source_fragment_id)
            .expect("router source fragment")
            .sink = DataSink::Result;
        let err = wrong_sink
            .seal()
            .expect_err("router edge without router sink must fail");
        assert!(
            err.contains("router source fragment does not use ChangeStreamRouter sink"),
            "{err}"
        );

        let mut route_mismatch = draft_builder_from_plan(&planned, Default::default());
        {
            let FragmentEdgeKind::ChangeStreamRouter { branch_id, .. } =
                &mut route_mismatch.edges_mut()[0].edge_kind
            else {
                panic!("expected router edge");
            };
            *branch_id += 1;
        }
        let err = route_mismatch
            .seal()
            .expect_err("router edge without exact route must fail");
        assert!(
            err.contains("no exact matching router branch route"),
            "{err}"
        );

        let mut slot_mismatch = draft_builder_from_plan(&planned, Default::default());
        slot_mismatch.edges_mut()[0].output_slot_ids = vec![2];
        let err = slot_mismatch
            .seal()
            .expect_err("router output slot mismatch must fail");
        assert!(err.contains("router output_slot_ids mismatch"), "{err}");

        let mut route_partition_mismatch = draft_builder_from_plan(&planned, Default::default());
        route_partition_mismatch.edges_mut()[0].output_partition = DataPartition::unpartitioned();
        route_partition_mismatch.edges_mut()[0].stream_kind = FragmentStreamKind::Gather;
        let err = route_partition_mismatch
            .seal()
            .expect_err("router edge and route partition mismatch must fail");
        assert!(
            err.contains(
                "partition mismatch: edge.output_partition is not equivalent to router route partition"
            ),
            "{err}"
        );

        let mut receiver_partition_mismatch = draft_builder_from_plan(&planned, Default::default());
        {
            let target = receiver_partition_mismatch
                .fragments_mut()
                .iter_mut()
                .find(|fragment| fragment.fragment_id == target_fragment_id)
                .expect("router target fragment");
            let DistributedNodeKind::Exchange(exchange) = &mut target.root.payload else {
                panic!("expected router Exchange receiver");
            };
            exchange.partition = DataPartition::unpartitioned();
        }
        let err = receiver_partition_mismatch
            .seal()
            .expect_err("router edge and receiver partition mismatch must fail");
        assert!(
            err.contains(
                "partition mismatch: edge.output_partition is not equivalent to target Exchange.partition"
            ),
            "{err}"
        );

        let mut stream_mismatch = draft_builder_from_plan(&planned, Default::default());
        stream_mismatch.edges_mut()[0].stream_kind = FragmentStreamKind::Gather;
        let err = stream_mismatch
            .seal()
            .expect_err("router HASH edge with Gather stream must fail");
        assert!(
            err.contains("invalid stream/partition combination"),
            "{err}"
        );
        assert!(err.contains("Iceberg change-stream router edge"), "{err}");
    }

    #[test]
    fn finalized_router_shape_rejects_two_edges_to_one_receiver() {
        // Two router edges in one group point at the same target exchange while
        // differing in branch_id and branch_kind. Full-tuple route uniqueness
        // would accept this and lean on the "conflicting partitions for the same
        // target Exchange" backstop in `validate_finalized_edge`; the seal's
        // per-source target-exchange uniqueness now rejects a second edge to one
        // receiver up front (an exchange node has a single source and a source
        // has one group), so that backstop is subsumed and never reached.
        let planned = finalized_router_plan();
        let first_edge = planned.edges()[0].clone();
        let mut builder = draft_builder_from_plan(&planned, Default::default());
        {
            let source = builder
                .fragments_mut()
                .iter_mut()
                .find(|fragment| fragment.fragment_id == first_edge.source_fragment_id)
                .expect("router source fragment");
            let DataSink::ChangeStreamRouter(router) = &mut source.sink else {
                panic!("expected router sink");
            };
            let mut second_route = router.branches[0].clone();
            second_route.branch_id += 1;
            // A distinct branch_kind clears the branch_id and branch_kind ledgers
            // so the target-exchange ledger is what rejects the duplicate receiver.
            second_route.branch_kind = ChangeStreamBranchKind::ReuseData;
            second_route.output_ordinals = vec![1];
            second_route.output_partition_ordinals = vec![1];
            let router_group_id = router.group_id;
            router.branches.push(second_route.clone());

            builder.edges_mut().push(FragmentEdge {
                source_fragment_id: first_edge.source_fragment_id,
                target_fragment_id: first_edge.target_fragment_id,
                target_exchange_node_id: first_edge.target_exchange_node_id,
                output_partition: DataPartition {
                    kind: PartitionKind::Hash,
                    exprs: vec![TypedExpr {
                        kind: ExprKind::ColumnRef {
                            column_id: ColumnId::new_for_test(2),
                            qualifier: None,
                            column: "route".to_string(),
                        },
                        data_type: DataType::Int64,
                        nullable: false,
                    }],
                },
                stream_kind: FragmentStreamKind::Partitioned,
                edge_kind: FragmentEdgeKind::ChangeStreamRouter {
                    router_group_id,
                    branch_id: second_route.branch_id,
                    branch_kind: second_route.branch_kind,
                },
                output_slot_ids: vec![2],
            });
        }

        let err = builder
            .seal()
            .expect_err("one router receiver cannot accept two router edges");
        assert!(
            err.contains(&format!(
                "repeats target exchange target_fragment_id={} target_exchange_node_id={}",
                first_edge.target_fragment_id, first_edge.target_exchange_node_id
            )),
            "{err}"
        );
        assert!(err.contains("source_fragment_id="), "{err}");
        assert!(err.contains("router_group_id="), "{err}");
    }

    #[test]
    fn shared_edge_validation_rejects_source_mismatch_and_empty_hash_partition() {
        let mut source_mismatch = draft_builder_from_plan(
            &stream_exchange_plan(ExchangeFlavor::Distribution),
            Default::default(),
        );
        {
            let DistributedNodeKind::Exchange(exchange) =
                &mut source_mismatch.fragments_mut()[1].root.payload
            else {
                panic!("consumer must be an exchange");
            };
            exchange.source_fragment_id = 42;
        }
        let err = source_mismatch
            .seal()
            .expect_err("edge and exchange source mismatch must fail");
        assert!(
            err.contains(
                "stream edge source_fragment_id=1 does not match Exchange source_fragment_id=42"
            ),
            "{err}"
        );

        let mut empty_hash = draft_builder_from_plan(
            &stream_exchange_plan(ExchangeFlavor::Distribution),
            Default::default(),
        );
        {
            let DistributedNodeKind::Exchange(exchange) =
                &mut empty_hash.fragments_mut()[1].root.payload
            else {
                panic!("consumer must be an exchange");
            };
            exchange.partition = DataPartition {
                kind: PartitionKind::Hash,
                exprs: Vec::new(),
            };
        }
        let err = empty_hash
            .seal()
            .expect_err("empty HASH exchange partition must fail");
        assert!(
            err.contains("DistributedPlan HASH Exchange has no native partition expressions"),
            "{err}"
        );
    }

    #[test]
    fn finalized_cte_multicast_validation_rejects_stale_contracts() {
        let dp = cte_multicast_plan();

        let mut arity_mismatch = draft_builder_from_plan(&dp, Default::default());
        {
            let FragmentEdgeKind::CteMulticast {
                receive_producer_column_ids,
                ..
            } = &mut arity_mismatch.edges_mut()[0].edge_kind
            else {
                panic!("expected CTE multicast edge");
            };
            receive_producer_column_ids.push(ColumnId::new_for_test(2));
            let DistributedNodeKind::Exchange(exchange) =
                &mut arity_mismatch.fragments_mut()[1].root.payload
            else {
                panic!("expected CTE Exchange receiver");
            };
            let ExchangeFlavor::CteMulticast {
                receive_producer_column_ids,
                ..
            } = &mut exchange.flavor
            else {
                panic!("expected CTE multicast flavor");
            };
            receive_producer_column_ids.push(ColumnId::new_for_test(2));
        }
        let err = arity_mismatch
            .seal()
            .expect_err("CTE receive/output arity mismatch must fail");
        assert!(err.contains("CTE receive/output arity mismatch"), "{err}");

        let mut mapping_mismatch = draft_builder_from_plan(&dp, Default::default());
        {
            let FragmentEdgeKind::CteMulticast {
                receive_producer_column_ids,
                ..
            } = &mut mapping_mismatch.edges_mut()[0].edge_kind
            else {
                panic!("expected CTE multicast edge");
            };
            receive_producer_column_ids[1] = ColumnId::new_for_test(2);
            let DistributedNodeKind::Exchange(exchange) =
                &mut mapping_mismatch.fragments_mut()[1].root.payload
            else {
                panic!("expected CTE Exchange receiver");
            };
            let ExchangeFlavor::CteMulticast {
                receive_producer_column_ids,
                ..
            } = &mut exchange.flavor
            else {
                panic!("expected CTE multicast flavor");
            };
            receive_producer_column_ids[1] = ColumnId::new_for_test(2);
        }
        let err = mapping_mismatch
            .seal()
            .expect_err("CTE receive/output mapping mismatch must fail");
        assert!(
            err.contains("CTE Exchange output mapping mismatch"),
            "{err}"
        );

        let mut slot_mismatch = draft_builder_from_plan(&dp, Default::default());
        slot_mismatch.edges_mut()[0].output_slot_ids = vec![1, 2];
        let err = slot_mismatch
            .seal()
            .expect_err("CTE output slot mismatch must fail");
        assert!(err.contains("CTE output_slot_ids mismatch"), "{err}");
    }

    fn plain_fragment(id: u32, node_id: i32, sink: DataSink) -> PlanFragment {
        PlanFragment {
            fragment_id: id,
            root: physical_values_node(id, node_id, Vec::new()),
            data_partition: DataPartition::unpartitioned(),
            output_partition: DataPartition::unpartitioned(),
            sink,
            output_exprs: None,
            output_columns: Vec::new(),
            cte_id: None,
            cte_exchange_nodes: Vec::new(),
        }
    }

    fn stream_edge(source: u32, target: u32, node: i32) -> FragmentEdge {
        FragmentEdge {
            source_fragment_id: source,
            target_fragment_id: target,
            target_exchange_node_id: node,
            output_partition: DataPartition::unpartitioned(),
            stream_kind: FragmentStreamKind::Gather,
            edge_kind: FragmentEdgeKind::Stream,
            output_slot_ids: Vec::new(),
        }
    }

    fn router_edge(
        source: u32,
        target: u32,
        node: i32,
        group: i32,
        branch: i32,
        kind: ChangeStreamBranchKind,
    ) -> FragmentEdge {
        FragmentEdge {
            source_fragment_id: source,
            target_fragment_id: target,
            target_exchange_node_id: node,
            output_partition: DataPartition::unpartitioned(),
            stream_kind: FragmentStreamKind::Gather,
            edge_kind: FragmentEdgeKind::ChangeStreamRouter {
                router_group_id: group,
                branch_id: branch,
                branch_kind: kind,
            },
            output_slot_ids: Vec::new(),
        }
    }

    fn seal_shape(
        fragments: Vec<PlanFragment>,
        edges: Vec<FragmentEdge>,
    ) -> Result<DistributedPlan, String> {
        DistributedPlanDraftBuilder::new(fragments, Some(0), edges, Default::default()).seal()
    }

    #[test]
    fn structural_validation_rejects_plain_stream_fan_out() {
        // Source fragment 1 drives two plain stream edges: an ambiguous fan-out
        // for a single-sink fragment. Both edges are well-formed on their own,
        // so only the per-source shape check can catch this.
        let fragments = vec![
            plain_fragment(0, 10, DataSink::Result),
            plain_fragment(1, 11, DataSink::Noop),
            plain_fragment(2, 12, DataSink::Noop),
        ];
        let edges = vec![stream_edge(1, 0, 100), stream_edge(1, 2, 101)];

        let err = seal_shape(fragments, edges).expect_err("plain stream fan-out must not seal");
        assert!(
            err.contains("source fragment id=1 has ambiguous plain stream fan-out"),
            "{err}"
        );
    }

    #[test]
    fn structural_validation_rejects_plain_stream_and_router_mix() {
        // Source fragment 1 drives both a plain stream edge and a router edge,
        // mixing two incompatible sink shapes on one fragment.
        let fragments = vec![
            plain_fragment(0, 10, DataSink::Result),
            plain_fragment(1, 11, DataSink::Noop),
            plain_fragment(2, 12, DataSink::Noop),
        ];
        let edges = vec![
            stream_edge(1, 0, 100),
            router_edge(1, 2, 101, 0, 0, ChangeStreamBranchKind::DeleteDv),
        ];

        let err = seal_shape(fragments, edges).expect_err("plain/router mix must not seal");
        assert!(
            err.contains("source fragment id=1 mixes plain stream and router edges"),
            "{err}"
        );
    }

    #[test]
    fn structural_validation_rejects_more_than_one_router_group_per_source() {
        let fragments = vec![
            plain_fragment(0, 10, DataSink::Result),
            plain_fragment(1, 11, DataSink::Noop),
            plain_fragment(2, 12, DataSink::Noop),
        ];
        let edges = vec![
            router_edge(1, 0, 100, 0, 0, ChangeStreamBranchKind::DeleteDv),
            router_edge(1, 2, 101, 1, 0, ChangeStreamBranchKind::DeleteDv),
        ];

        let err = seal_shape(fragments, edges)
            .expect_err("multiple router groups per source must not seal");
        assert!(
            err.contains("source fragment id=1 uses more than one router group"),
            "{err}"
        );
    }

    #[test]
    fn structural_validation_rejects_repeated_router_branch_id() {
        // Two router edges in one group repeat branch_id=0 while differing in
        // branch_kind and target. Full-tuple route uniqueness would accept this;
        // the seal's per-field branch_id uniqueness rejects it (strictly
        // stronger), so the coordinator no longer needs to.
        let fragments = vec![
            plain_fragment(0, 10, DataSink::Result),
            plain_fragment(1, 11, DataSink::Noop),
            plain_fragment(2, 12, DataSink::Noop),
        ];
        let edges = vec![
            router_edge(1, 0, 100, 0, 0, ChangeStreamBranchKind::DeleteDv),
            router_edge(1, 2, 101, 0, 0, ChangeStreamBranchKind::ReuseData),
        ];

        let err =
            seal_shape(fragments, edges).expect_err("repeated router branch_id must not seal");
        assert!(err.contains("repeats branch_id=0"), "{err}");
        assert!(err.contains("source_fragment_id=1"), "{err}");
        assert!(err.contains("router_group_id=0"), "{err}");
    }

    #[test]
    fn structural_validation_rejects_repeated_router_branch_kind() {
        // Distinct branch_ids (0, 1) but a repeated branch_kind within the group:
        // the branch_id ledger passes, the branch_kind ledger rejects. Full-tuple
        // uniqueness would accept this.
        let fragments = vec![
            plain_fragment(0, 10, DataSink::Result),
            plain_fragment(1, 11, DataSink::Noop),
            plain_fragment(2, 12, DataSink::Noop),
        ];
        let edges = vec![
            router_edge(1, 0, 100, 0, 0, ChangeStreamBranchKind::DeleteDv),
            router_edge(1, 2, 101, 0, 1, ChangeStreamBranchKind::DeleteDv),
        ];

        let err =
            seal_shape(fragments, edges).expect_err("repeated router branch_kind must not seal");
        assert!(err.contains("repeats branch_kind=DeleteDv"), "{err}");
        assert!(err.contains("source_fragment_id=1"), "{err}");
        assert!(err.contains("router_group_id=0"), "{err}");
    }

    #[test]
    fn structural_validation_rejects_repeated_router_target_exchange() {
        // Distinct branch_ids and branch_kinds, but both edges target the same
        // exchange (fragment 0, node 100): the target-exchange ledger rejects.
        // Full-tuple uniqueness would accept this.
        let fragments = vec![
            plain_fragment(0, 10, DataSink::Result),
            plain_fragment(1, 11, DataSink::Noop),
        ];
        let edges = vec![
            router_edge(1, 0, 100, 0, 0, ChangeStreamBranchKind::DeleteDv),
            router_edge(1, 0, 100, 0, 1, ChangeStreamBranchKind::ReuseData),
        ];

        let err = seal_shape(fragments, edges)
            .expect_err("repeated router target exchange must not seal");
        assert!(
            err.contains(
                "repeats target exchange target_fragment_id=0 target_exchange_node_id=100"
            ),
            "{err}"
        );
        assert!(err.contains("source_fragment_id=1"), "{err}");
        assert!(err.contains("router_group_id=0"), "{err}");
    }

    #[test]
    fn structural_validation_allows_cte_multicast_fan_out() {
        // One CTE producer feeding two receivers is legal fan-out and must not
        // trip the plain-stream fan-out rule. The shape check exempts it; the
        // edges then fail later for unrelated wiring reasons, never for shape.
        let fragments = vec![
            plain_fragment(0, 10, DataSink::Result),
            plain_fragment(1, 11, DataSink::Noop),
            plain_fragment(2, 12, DataSink::Noop),
        ];
        let cte_id: CteId = 3;
        let multicast = |target: u32, node: i32| FragmentEdge {
            source_fragment_id: 1,
            target_fragment_id: target,
            target_exchange_node_id: node,
            output_partition: DataPartition::unpartitioned(),
            stream_kind: FragmentStreamKind::Gather,
            edge_kind: FragmentEdgeKind::CteMulticast {
                cte_id,
                receive_producer_column_ids: Vec::new(),
            },
            output_slot_ids: Vec::new(),
        };
        let edges = vec![multicast(0, 100), multicast(2, 101)];

        let err = seal_shape(fragments, edges)
            .expect_err("unwired multicast edges still fail later, but not for fan-out");
        assert!(
            !err.contains("fan-out"),
            "CTE multicast must be exempt from the plain-stream fan-out rule: {err}"
        );
    }
}
