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

use super::WriterAuxiliaryPlan;
use super::change_stream::{
    ChangeStreamRoute, ChangeStreamRouterSink, ChangeStreamWriteDagSpec,
    SqlChangeStreamWriteTopology, SqlChangeStreamWriterRoute, route_output_ordinals,
};
use super::contract::{ConnectorWriteInputBinding, SqlWritePlanInput};
use super::node::{
    TableFinishNode, TableWriterNode, table_finish_output_columns, table_writer_output_columns,
    writer_multiplex_output_slot_ids,
};
use super::sink::ConnectorWritePlanInput;
use crate::analysis::{ExprKind, OutputColumn, TypedExpr};
use crate::planner::distributed::fragment::DistributedPlanDraft;
use crate::planner::distributed::{
    DataPartition, DataSink, DistributedNode, DistributedNodeKind, DistributedPlan, ExchangeFlavor,
    ExchangeReceiver, FragmentEdge, FragmentEdgeKind, FragmentId, FragmentStreamKind,
    PartitionKind, PlanFragment,
};
use novarocks_spi::connector::write_stack::{WriteTargetOrdinal, WriterMultiplexSchema};

#[derive(Clone, Debug)]
pub(crate) struct PlannedSqlChangeStreamDistributedPlan {
    pub(crate) distributed_plan: DistributedPlan,
    pub(crate) topology: SqlChangeStreamWriteTopology,
}

#[derive(Debug)]
pub(in crate::planner::distributed) struct PlannedSqlChangeStreamDistributedPlanDraft {
    distributed_plan: DistributedPlanDraft,
    topology: SqlChangeStreamWriteTopology,
}

// ===========================================================================
// NCP-6: table writer / table finish dataflow
// ===========================================================================
//
// The builders below wrap the writer's child plan in a `TableWriter` node that
// emits the frozen write-result relation, gather every writer fragment into ONE
// `TableFinish` fragment on the plan root, and emit the query result through the
// ordinary `DataSink::Result`.

pub(crate) fn build_table_writer_finish_distributed_plan_with_auxiliary(
    physical: &crate::planner::physical::PhysicalPlanNode,
    sink: ConnectorWritePlanInput,
    write_target_ordinal: WriteTargetOrdinal,
    auxiliary: &WriterAuxiliaryPlan,
) -> Result<DistributedPlan, String> {
    let draft = crate::planner::distributed::build::build_distributed_plan_draft(physical)?;
    let draft =
        with_table_writer_finish_with_auxiliary(draft, sink, write_target_ordinal, auxiliary)?;
    crate::planner::distributed::seal::seal_draft(draft).map_err(|error| error.to_string())
}

pub(crate) fn build_sql_table_writer_finish_distributed_plan_with_auxiliary(
    physical: &crate::planner::physical::PhysicalPlanNode,
    sink: SqlWritePlanInput,
    write_target_ordinal: WriteTargetOrdinal,
    auxiliary: &WriterAuxiliaryPlan,
) -> Result<DistributedPlan, String> {
    let draft = crate::planner::distributed::build::build_distributed_plan_draft(physical)?;
    let draft = with_table_writer_finish_with_auxiliary(
        draft,
        ConnectorWritePlanInput::from_sql_write_plan_input(sink),
        write_target_ordinal,
        auxiliary,
    )?;
    crate::planner::distributed::seal::seal_draft(draft).map_err(|error| error.to_string())
}

/// Build a dataflow change-stream write plan: the router root keeps its
/// `ChangeStreamRouter` sink, every route gets its own `Exchange -> TableWriter`
/// fragment, and all of them gather into one Root `TableFinish` fragment.
///
/// Dataflow successor of [`build_sql_change_stream_distributed_plan`].
pub(crate) fn build_sql_change_stream_table_writer_finish_distributed_plan_with_auxiliary(
    physical: &crate::planner::physical::PhysicalPlanNode,
    dag: ChangeStreamWriteDagSpec,
    auxiliary: &WriterAuxiliaryPlan,
) -> Result<PlannedSqlChangeStreamDistributedPlan, String> {
    let draft = crate::planner::distributed::build::build_distributed_plan_draft(physical)?;
    let planned = with_sql_change_stream_table_writer_finish_with_auxiliary(draft, dag, auxiliary)?;
    let distributed_plan = crate::planner::distributed::seal::seal_draft(planned.distributed_plan)
        .map_err(|error| error.to_string())?;
    Ok(PlannedSqlChangeStreamDistributedPlan {
        distributed_plan,
        topology: planned.topology,
    })
}

/// Attach the compiler-owned write contract as a dataflow `TableWriter`.
#[cfg(any(test, feature = "test-support"))]
pub(in crate::planner::distributed) fn with_sql_table_writer_finish(
    plan: DistributedPlanDraft,
    sink: SqlWritePlanInput,
    write_target_ordinal: WriteTargetOrdinal,
) -> Result<DistributedPlanDraft, String> {
    with_table_writer_finish(
        plan,
        ConnectorWritePlanInput::from_sql_write_plan_input(sink),
        write_target_ordinal,
    )
}

/// Rewrite a result-rooted draft into the NCP-6 writer/finish shape.
///
/// The draft root becomes the writer fragment: its existing tree becomes the
/// child of a `TableWriter` carrying the caller's `write_target_ordinal`, which
/// is the target this query writes within its write session -- a session that
/// spans several queries (COW cohorts, distributed rewrite) gives each query a
/// different ordinal, so it must not be derived from this plan. Its sink drops from
/// `Result` to `Noop`, and a freshly created finish fragment (Exchange receiver
/// -> `TableFinish` -> `DataSink::Result`) becomes the plan root. Even a
/// single-fragment INSERT gets its own Root finish fragment; there is
/// deliberately no "the writer fragment is already the root" shortcut.
#[cfg(any(test, feature = "test-support"))]
pub(in crate::planner::distributed) fn with_table_writer_finish(
    plan: DistributedPlanDraft,
    sink: ConnectorWritePlanInput,
    write_target_ordinal: WriteTargetOrdinal,
) -> Result<DistributedPlanDraft, String> {
    let auxiliary = WriterAuxiliaryPlan::without_requirements([write_target_ordinal])?;
    with_table_writer_finish_with_auxiliary(plan, sink, write_target_ordinal, &auxiliary)
}

pub(in crate::planner::distributed) fn with_table_writer_finish_with_auxiliary(
    mut plan: DistributedPlanDraft,
    sink: ConnectorWritePlanInput,
    write_target_ordinal: WriteTargetOrdinal,
    auxiliary: &WriterAuxiliaryPlan,
) -> Result<DistributedPlanDraft, String> {
    let root_fragment_id = plan
        .root_fragment_id
        .ok_or_else(|| "table writer requires a draft root fragment id".to_string())?;
    let root_index = plan
        .fragments
        .iter()
        .position(|fragment| fragment.fragment_id == root_fragment_id)
        .ok_or_else(|| format!("table writer cannot find root fragment id={root_fragment_id}"))?;
    if !matches!(plan.fragments[root_index].sink, DataSink::Result) {
        return Err(format!(
            "table writer expected root fragment id={root_fragment_id} to use result sink"
        ));
    }

    let mut ids = WriteOverlayIds::from_draft(&plan);
    // Freeze the Arrow/SQL write contract against the fragment while its
    // declared output is still the child plan's output; the writer node
    // replaces that output with the write-result relation below.
    let output_contract = crate::planner::distributed::output::finalize_connector_write_output(
        &plan.fragments[root_index],
        &sink,
    )
    .map_err(|error| error.to_string())?;

    let writer_fragment = into_table_writer_fragment(
        plan.fragments.remove(root_index),
        write_target_ordinal,
        sink.input,
        output_contract,
        auxiliary.schema().clone(),
        auxiliary.partial_for(write_target_ordinal)?,
        &mut ids,
    );
    let writer_stats = writer_fragment.root.stats.clone();
    let writer_fragment_id = writer_fragment.fragment_id;
    plan.fragments.insert(root_index, writer_fragment);

    let (finish_fragment, finish_edges) = build_table_finish_fragment(
        &[(writer_fragment_id, writer_stats, write_target_ordinal)],
        auxiliary.schema(),
        auxiliary.final_plan(),
        &mut ids,
    )?;
    plan.root_fragment_id = Some(finish_fragment.fragment_id);
    plan.fragments.push(finish_fragment);
    plan.edges.extend(finish_edges);
    Ok(plan)
}

/// Rewrite a result-rooted change-stream draft into the NCP-6 writer/finish
/// shape: the router fragment keeps its `ChangeStreamRouter` sink, each route's
/// writer fragment becomes `Exchange -> TableWriter` with a `Noop` sink, and
/// every writer streams into the same Root `TableFinish` fragment.
///
/// Write target ordinals are dense from 0 in the route order the DAG spec gives.
#[cfg(any(test, feature = "test-support"))]
pub(in crate::planner::distributed) fn with_sql_change_stream_table_writer_finish(
    plan: DistributedPlanDraft,
    dag: ChangeStreamWriteDagSpec,
) -> Result<PlannedSqlChangeStreamDistributedPlanDraft, String> {
    let auxiliary = WriterAuxiliaryPlan::without_requirements(
        dag.routes.iter().map(|route| route.write_target_ordinal),
    )?;
    with_sql_change_stream_table_writer_finish_with_auxiliary(plan, dag, &auxiliary)
}

pub(in crate::planner::distributed) fn with_sql_change_stream_table_writer_finish_with_auxiliary(
    mut plan: DistributedPlanDraft,
    dag: ChangeStreamWriteDagSpec,
    auxiliary: &WriterAuxiliaryPlan,
) -> Result<PlannedSqlChangeStreamDistributedPlanDraft, String> {
    dag.validate()?;
    if dag.routes.is_empty() {
        return Err("row-mutation router requires at least one route".to_string());
    }

    let root_fragment_id = plan
        .root_fragment_id
        .ok_or_else(|| "row-mutation write requires a draft root fragment id".to_string())?;
    let root_index = plan
        .fragments
        .iter()
        .position(|fragment| fragment.fragment_id == root_fragment_id)
        .ok_or_else(|| {
            format!("row-mutation write cannot find root fragment id={root_fragment_id}")
        })?;
    if !matches!(plan.fragments[root_index].sink, DataSink::Result) {
        return Err(format!(
            "row-mutation write expected root fragment id={root_fragment_id} to use result sink"
        ));
    }

    let source_fragment = plan.fragments[root_index].clone();
    validate_output_ordinal(
        &source_fragment.output_columns,
        dag.effect_output_ordinal,
        "effect",
    )?;

    let mut ids = WriteOverlayIds::from_draft(&plan);
    let mut routes = Vec::with_capacity(dag.routes.len());
    let mut writer_routes = Vec::with_capacity(dag.routes.len());
    let mut writer_fragments = Vec::with_capacity(dag.routes.len());
    let mut router_edges = Vec::with_capacity(dag.routes.len());
    let mut writers = Vec::with_capacity(dag.routes.len());

    // `dag.validate()` already established that route `i` carries write target
    // ordinal `i`, which is what makes the finish node's expected ordinals and
    // this loop's writer order describe the same writers.
    for route in dag.routes.into_iter() {
        let route_write_target_ordinal = route.write_target_ordinal;
        let stream_output_ordinals = route_output_ordinals(&route);
        validate_output_ordinals(
            &source_fragment.output_columns,
            &stream_output_ordinals,
            "route input",
        )?;
        validate_output_ordinals(
            &source_fragment.output_columns,
            &route.output_partition_ordinals,
            "route partition",
        )?;

        let sink = route.sink;

        let writer_columns =
            output_columns_by_ordinals(&source_fragment.output_columns, &stream_output_ordinals)?;
        let output_slot_ids = output_slot_ids_for_ordinals(
            &source_fragment.output_columns,
            &stream_output_ordinals,
            "route input",
        )?;
        if !matches!(sink.input, ConnectorWriteInputBinding::RootOutputByOrdinal) {
            return Err("SQL change-stream writer requires root output input binding".to_string());
        }
        if writer_columns.len() != sink.contract.input_columns.len() {
            return Err(format!(
                "SQL row-mutation route output column count {} does not match write input column count {}",
                writer_columns.len(),
                sink.contract.input_columns.len()
            ));
        }

        let writer_fragment_id = ids.alloc_fragment();
        let exchange_node_id = ids.alloc_node();
        let exchange_tuple_id = ids.alloc_tuple();
        let output_partition = data_partition_for_ordinals(
            &source_fragment.output_columns,
            &route.output_partition_ordinals,
            "route partition",
        )?;
        let stream_kind = stream_kind_for_data_partition(&output_partition);

        let sink_template = ConnectorWritePlanInput::from_sql_write_plan_input(sink.clone());
        // The route's Exchange-rooted fragment is the shape the write contract
        // is frozen against; the writer node then replaces its declared output
        // with the write-result relation.
        let route_input_fragment = PlanFragment {
            fragment_id: writer_fragment_id,
            root: DistributedNode {
                node_id: exchange_node_id,
                fragment_id: writer_fragment_id,
                tuple_ids: vec![exchange_tuple_id],
                nullable_tuple_ids: Vec::new(),
                limit: -1,
                runtime_filter_binding_ids: Vec::new(),
                children: Vec::new(),
                stats: source_fragment.root.stats.clone(),
                payload: DistributedNodeKind::Exchange(ExchangeReceiver {
                    partition: output_partition.clone(),
                    source_fragment_id: root_fragment_id,
                    output_columns: writer_columns.clone(),
                    output_qualifier: None,
                    flavor: ExchangeFlavor::Distribution,
                }),
            },
            data_partition: DataPartition::unpartitioned(),
            output_partition: DataPartition::unpartitioned(),
            sink: DataSink::Noop,
            output_exprs: None,
            output_columns: writer_columns,
            cte_id: None,
            cte_exchange_nodes: Vec::new(),
        };
        let output_contract = crate::planner::distributed::output::finalize_connector_write_output(
            &route_input_fragment,
            &sink_template,
        )
        .map_err(|error| error.to_string())?;
        let writer_fragment = into_table_writer_fragment(
            route_input_fragment,
            route_write_target_ordinal,
            sink_template.input,
            output_contract,
            auxiliary.schema().clone(),
            auxiliary.partial_for(route_write_target_ordinal)?,
            &mut ids,
        );
        writers.push((
            writer_fragment_id,
            writer_fragment.root.stats.clone(),
            route.write_target_ordinal,
        ));
        writer_fragments.push(writer_fragment);

        router_edges.push(FragmentEdge {
            source_fragment_id: root_fragment_id,
            target_fragment_id: writer_fragment_id,
            target_exchange_node_id: exchange_node_id,
            output_partition,
            stream_kind,
            edge_kind: FragmentEdgeKind::ChangeStreamRouter {
                router_group_id: 0,
                route_id: route.route_id,
            },
            output_slot_ids,
        });

        routes.push(ChangeStreamRoute {
            route_id: route.route_id,
            write_target_ordinal: route.write_target_ordinal,
            accepted_effects: route.accepted_effects.clone(),
            input_ordinals: route.input_ordinals,
            target_fragment_id: writer_fragment_id,
            target_exchange_node_id: exchange_node_id,
            output_partition_ordinals: route.output_partition_ordinals,
        });
        writer_routes.push(SqlChangeStreamWriterRoute {
            route_id: route.route_id,
            write_target_ordinal: route.write_target_ordinal,
            accepted_effects: route.accepted_effects,
            writer_fragment_id,
            sink,
        });
    }

    plan.fragments[root_index].sink = DataSink::ChangeStreamRouter(ChangeStreamRouterSink {
        group_id: 0,
        effect_output_ordinal: dag.effect_output_ordinal,
        routes,
    });
    plan.fragments.extend(writer_fragments);
    plan.edges.extend(router_edges);

    let (finish_fragment, finish_edges) = build_table_finish_fragment(
        &writers,
        auxiliary.schema(),
        auxiliary.final_plan(),
        &mut ids,
    )?;
    plan.root_fragment_id = Some(finish_fragment.fragment_id);
    plan.fragments.push(finish_fragment);
    plan.edges.extend(finish_edges);

    Ok(PlannedSqlChangeStreamDistributedPlanDraft {
        distributed_plan: plan,
        topology: SqlChangeStreamWriteTopology { writer_routes },
    })
}

/// The dense, query-local write target ordinal for the writer at `index`.
///
/// The ordinal vocabulary and its bound are owned by SPI; this only projects a
/// planner-side position into it.
/// One monotonic id source shared by every fragment, node, and tuple the write
/// overlay creates. Allocating from a single cursor keeps the overlay's ids
/// globally unique even when it creates several fragments at once.
struct WriteOverlayIds {
    fragment_id: FragmentId,
    node_id: i32,
    tuple_id: i32,
}

impl WriteOverlayIds {
    fn from_draft(plan: &DistributedPlanDraft) -> Self {
        Self {
            fragment_id: next_fragment_id(plan),
            node_id: next_node_id(plan),
            tuple_id: next_tuple_id(plan),
        }
    }

    fn alloc_fragment(&mut self) -> FragmentId {
        let id = self.fragment_id;
        self.fragment_id += 1;
        id
    }

    fn alloc_node(&mut self) -> i32 {
        let id = self.node_id;
        self.node_id += 1;
        id
    }

    fn alloc_tuple(&mut self) -> i32 {
        let id = self.tuple_id;
        self.tuple_id += 1;
        id
    }
}

/// Turn one fragment into an NCP-6 writer fragment: its existing root becomes
/// the `TableWriter`'s only child, its declared output becomes the frozen
/// write-result relation, and its sink becomes the ordinary producer `Noop`
/// sink so a gather stream edge can carry that relation to the finish fragment.
fn into_table_writer_fragment(
    fragment: PlanFragment,
    write_target_ordinal: WriteTargetOrdinal,
    input: ConnectorWriteInputBinding,
    output_contract: crate::planner::distributed::output::ConnectorWriteOutputContract,
    writer_schema: WriterMultiplexSchema,
    partial_aggregate_plan: super::WriterPartialAggregatePlan,
    ids: &mut WriteOverlayIds,
) -> PlanFragment {
    let fragment_id = fragment.fragment_id;
    let node_id = ids.alloc_node();
    let tuple_id = ids.alloc_tuple();
    let stats = fragment.root.stats.clone();
    PlanFragment {
        fragment_id,
        root: DistributedNode {
            node_id,
            fragment_id,
            tuple_ids: vec![tuple_id],
            nullable_tuple_ids: Vec::new(),
            limit: -1,
            runtime_filter_binding_ids: Vec::new(),
            children: vec![fragment.root],
            stats,
            payload: DistributedNodeKind::TableWriter(TableWriterNode::new_with_aggregate_plan(
                write_target_ordinal,
                input,
                output_contract,
                writer_schema.clone(),
                partial_aggregate_plan,
            )),
        },
        data_partition: fragment.data_partition,
        // Every writer gathers into the one finish fragment.
        output_partition: DataPartition::unpartitioned(),
        sink: DataSink::Noop,
        output_exprs: None,
        output_columns: table_writer_output_columns(&writer_schema),
        cte_id: fragment.cte_id,
        cte_exchange_nodes: fragment.cte_exchange_nodes,
    }
}

/// Build the single Root finish fragment plus one gather stream edge per writer
/// fragment. `writers` carries each writer's own target ordinal rather than
/// deriving one from position: `writers[i]` is
/// the writer whose `write_target_ordinal` is `i`.
fn build_table_finish_fragment(
    writers: &[(
        FragmentId,
        crate::planner::physical::PhysicalPlanStats,
        WriteTargetOrdinal,
    )],
    writer_schema: &WriterMultiplexSchema,
    final_aggregate_plan: &super::WriterFinalAggregatePlan,
    ids: &mut WriteOverlayIds,
) -> Result<(PlanFragment, Vec<FragmentEdge>), String> {
    let Some((_, first_stats, _)) = writers.first() else {
        return Err("table finish requires at least one table writer fragment".to_string());
    };
    let finish_fragment_id = ids.alloc_fragment();
    let writer_columns = table_writer_output_columns(writer_schema);
    let output_slot_ids = writer_multiplex_output_slot_ids(writer_schema);

    let mut children = Vec::with_capacity(writers.len());
    let mut edges = Vec::with_capacity(writers.len());
    let mut expected_target_ordinals = Vec::with_capacity(writers.len());
    for (writer_fragment_id, writer_stats, target_ordinal) in writers.iter() {
        expected_target_ordinals.push(*target_ordinal);
        let exchange_node_id = ids.alloc_node();
        let exchange_tuple_id = ids.alloc_tuple();
        children.push(DistributedNode {
            node_id: exchange_node_id,
            fragment_id: finish_fragment_id,
            tuple_ids: vec![exchange_tuple_id],
            nullable_tuple_ids: Vec::new(),
            limit: -1,
            runtime_filter_binding_ids: Vec::new(),
            children: Vec::new(),
            stats: writer_stats.clone(),
            payload: DistributedNodeKind::Exchange(ExchangeReceiver {
                partition: DataPartition::unpartitioned(),
                source_fragment_id: *writer_fragment_id,
                output_columns: writer_columns.clone(),
                output_qualifier: None,
                flavor: ExchangeFlavor::Distribution,
            }),
        });
        edges.push(FragmentEdge {
            source_fragment_id: *writer_fragment_id,
            target_fragment_id: finish_fragment_id,
            target_exchange_node_id: exchange_node_id,
            output_partition: DataPartition::unpartitioned(),
            stream_kind: FragmentStreamKind::Gather,
            edge_kind: FragmentEdgeKind::Stream,
            output_slot_ids: output_slot_ids.clone(),
        });
    }

    let finish_node_id = ids.alloc_node();
    let finish_tuple_id = ids.alloc_tuple();
    let fragment = PlanFragment {
        fragment_id: finish_fragment_id,
        root: DistributedNode {
            node_id: finish_node_id,
            fragment_id: finish_fragment_id,
            tuple_ids: vec![finish_tuple_id],
            nullable_tuple_ids: Vec::new(),
            limit: -1,
            runtime_filter_binding_ids: Vec::new(),
            children,
            stats: first_stats.clone(),
            payload: DistributedNodeKind::TableFinish(
                TableFinishNode::try_new_with_aggregate_plan(
                    expected_target_ordinals,
                    writer_schema.clone(),
                    novarocks_spi::connector::write_stack::RootWriteResultSchema::new(),
                    final_aggregate_plan.clone(),
                )?,
            ),
        },
        data_partition: DataPartition::unpartitioned(),
        output_partition: DataPartition::unpartitioned(),
        sink: DataSink::Result,
        output_exprs: None,
        output_columns: table_finish_output_columns(),
        cte_id: None,
        cte_exchange_nodes: Vec::new(),
    };
    Ok((fragment, edges))
}

/// Build a target ordinal for a fixture. Fixtures are single-target unless a
/// test says otherwise, so this is the one place the literal 0 is spelled.
#[cfg(any(test, feature = "test-support"))]
fn target_ordinal_for_test(value: u32) -> WriteTargetOrdinal {
    WriteTargetOrdinal::try_new(value).expect("test write target ordinal is within the bound")
}

#[cfg(any(test, feature = "test-support"))]
pub(crate) fn finalize_sql_table_writer_finish_test_plan(
    builder: crate::planner::distributed::test_support::DistributedPlanDraftBuilder,
    sink: SqlWritePlanInput,
) -> Result<DistributedPlan, String> {
    // Test fixtures are single-target, so ordinal 0 is the whole target set.
    let draft =
        with_sql_table_writer_finish(builder.into_draft(), sink, target_ordinal_for_test(0))?;
    crate::planner::distributed::seal::seal_draft(draft).map_err(|error| error.to_string())
}

#[cfg(any(test, feature = "test-support"))]
pub(crate) fn finalize_sql_change_stream_table_writer_finish_test_plan(
    builder: crate::planner::distributed::test_support::DistributedPlanDraftBuilder,
    dag: ChangeStreamWriteDagSpec,
) -> Result<DistributedPlan, String> {
    let planned = with_sql_change_stream_table_writer_finish(builder.into_draft(), dag)?;
    crate::planner::distributed::seal::seal_draft(planned.distributed_plan)
        .map_err(|error| error.to_string())
}

fn next_fragment_id(plan: &DistributedPlanDraft) -> FragmentId {
    plan.fragments
        .iter()
        .map(|fragment| fragment.fragment_id)
        .max()
        .unwrap_or_default()
        + 1
}

fn next_node_id(plan: &DistributedPlanDraft) -> i32 {
    plan.fragments
        .iter()
        .flat_map(|fragment| node_ids(&fragment.root))
        .max()
        .unwrap_or_default()
        + 1
}

fn next_tuple_id(plan: &DistributedPlanDraft) -> i32 {
    plan.fragments
        .iter()
        .flat_map(|fragment| node_tuple_ids(&fragment.root))
        .max()
        .unwrap_or_default()
        + 1
}

fn node_ids(node: &DistributedNode) -> Vec<i32> {
    let mut ids = vec![node.node_id];
    for child in &node.children {
        ids.extend(node_ids(child));
    }
    ids
}

fn node_tuple_ids(node: &DistributedNode) -> Vec<i32> {
    let mut ids = node.tuple_ids.clone();
    ids.extend_from_slice(&node.nullable_tuple_ids);
    for child in &node.children {
        ids.extend(node_tuple_ids(child));
    }
    ids
}

fn validate_output_ordinal(
    output_columns: &[crate::analysis::OutputColumn],
    ordinal: usize,
    label: &str,
) -> Result<(), String> {
    if output_columns.get(ordinal).is_none() {
        return Err(format!(
            "row-mutation {label} output ordinal {ordinal} is out of range"
        ));
    }
    Ok(())
}

fn validate_output_ordinals(
    output_columns: &[crate::analysis::OutputColumn],
    ordinals: &[usize],
    label: &str,
) -> Result<(), String> {
    for ordinal in ordinals {
        validate_output_ordinal(output_columns, *ordinal, label)?;
    }
    Ok(())
}

fn output_columns_by_ordinals(
    output_columns: &[crate::analysis::OutputColumn],
    ordinals: &[usize],
) -> Result<Vec<crate::analysis::OutputColumn>, String> {
    ordinals
        .iter()
        .copied()
        .map(|ordinal| {
            output_columns.get(ordinal).cloned().ok_or_else(|| {
                format!("row-mutation route input ordinal {ordinal} is out of range")
            })
        })
        .collect()
}

fn output_slot_ids_for_ordinals(
    output_columns: &[OutputColumn],
    ordinals: &[usize],
    label: &str,
) -> Result<Vec<i32>, String> {
    ordinals
        .iter()
        .copied()
        .map(|ordinal| {
            let column = output_columns.get(ordinal).ok_or_else(|| {
                format!("row-mutation {label} output ordinal {ordinal} is out of range")
            })?;
            i32::try_from(column.column_id.0).map_err(|_| {
                format!(
                    "row-mutation {label} column id {} cannot be encoded as stream output slot id",
                    column.column_id
                )
            })
        })
        .collect()
}

fn data_partition_for_ordinals(
    output_columns: &[OutputColumn],
    ordinals: &[usize],
    label: &str,
) -> Result<DataPartition, String> {
    if ordinals.is_empty() {
        return Ok(DataPartition::unpartitioned());
    }

    let exprs = ordinals
        .iter()
        .copied()
        .map(|ordinal| {
            let column = output_columns.get(ordinal).ok_or_else(|| {
                format!("row-mutation {label} output ordinal {ordinal} is out of range")
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

    Ok(DataPartition::hash(exprs))
}

fn stream_kind_for_data_partition(partition: &DataPartition) -> FragmentStreamKind {
    match partition.kind {
        PartitionKind::Unpartitioned => FragmentStreamKind::Gather,
        PartitionKind::Random => FragmentStreamKind::Other,
        PartitionKind::Hash => FragmentStreamKind::Partitioned,
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::{DataType, Field, Schema};
    use novarocks_functions::{
        AggregateOverloadMetadata, EngineFunctionCatalogBuilder, FunctionDefinition,
        FunctionVisibility, FunctionVolatility,
    };
    use novarocks_spi::connector::{
        StatisticsArtifactIdentity, StatisticsRequiredAggregation, StatisticsScanColumn,
    };

    use super::target_ordinal_for_test;

    use super::super::change_stream::ChangeStreamWriteDagSpec;
    use super::super::contract::{ConnectorWriteInputBinding, test_support};

    use crate::analysis::OutputColumn;
    use crate::column_id::ColumnId;
    use crate::planner::distributed::test_support::DistributedPlanDraftBuilder;
    use crate::planner::distributed::{
        DataPartition, DataSink, DistributedNode, DistributedNodeKind, FragmentEdgeKind,
        FragmentStreamKind, PlanFragment,
    };
    use crate::planner::physical::{PhysicalPlanStats, PlannerConfidence};

    use novarocks_spi::connector::write_stack::{root_write_result_schema, writer_output_schema};

    use super::{
        with_sql_change_stream_table_writer_finish,
        with_sql_change_stream_table_writer_finish_with_auxiliary, with_sql_table_writer_finish,
    };

    fn test_route(
        route_byte: u8,
        write_target_ordinal: u32,
        effects: Vec<novarocks_spi::connector::ConnectorRowMutationEffect>,
        input_ordinal: u32,
        partition_ordinals: Vec<usize>,
    ) -> super::super::change_stream::ChangeStreamWriteRouteSpec {
        super::super::change_stream::ChangeStreamWriteRouteSpec {
            route_id: novarocks_spi::connector::ConnectorWriteRouteId::from_bytes([route_byte; 32]),
            write_target_ordinal:
                novarocks_spi::connector::write_stack::WriteTargetOrdinal::try_new(
                    write_target_ordinal,
                )
                .expect("bounded ordinal"),
            accepted_effects: effects,
            input_ordinals: vec![novarocks_spi::connector::ConnectorMutationRouteInput::new(
                novarocks_spi::connector::ConnectorWriteFieldToken::from_bytes(
                    [route_byte.wrapping_add(2); 32],
                ),
                input_ordinal,
            )],
            output_partition_ordinals: partition_ordinals,
            sink: test_support::simple_sql_write_plan_input(
                ConnectorWriteInputBinding::RootOutputByOrdinal,
            ),
        }
    }

    fn single_fragment_plan_for_test() -> DistributedPlanDraftBuilder {
        single_fragment_plan_for_test_with_columns(vec![("id", DataType::Int32)])
    }

    fn single_fragment_plan_for_test_with_columns(
        columns: Vec<(&str, DataType)>,
    ) -> DistributedPlanDraftBuilder {
        let output_columns = columns
            .into_iter()
            .enumerate()
            .map(|(idx, (name, data_type))| OutputColumn {
                column_id: ColumnId::new_for_test((idx + 1) as u32),
                name: name.to_string(),
                data_type,
                nullable: false,
                is_internal: false,
            })
            .collect::<Vec<_>>();
        DistributedPlanDraftBuilder::new(
            vec![PlanFragment {
                fragment_id: 0,
                root: DistributedNode {
                    node_id: 10,
                    fragment_id: 0,
                    tuple_ids: vec![10],
                    nullable_tuple_ids: vec![],
                    limit: -1,
                    runtime_filter_binding_ids: Vec::new(),
                    children: vec![],
                    stats: stats(),
                    payload: DistributedNodeKind::Values(crate::planner::payload::PlanValuesNode {
                        rows: vec![],
                        columns: output_columns.clone(),
                    }),
                },
                data_partition: DataPartition::unpartitioned(),
                output_partition: DataPartition::unpartitioned(),
                sink: DataSink::Result,
                output_exprs: None,
                output_columns,
                cte_id: None,
                cte_exchange_nodes: Vec::new(),
            }],
            Some(0),
            Vec::new(),
            Default::default(),
        )
    }

    fn stats() -> PhysicalPlanStats {
        PhysicalPlanStats {
            output_row_count: 0.0,
            row_count_confidence: PlannerConfidence::Fallback,
            column_statistics: Default::default(),
            cost_estimate: None,
            broadcast_decision: None,
        }
    }

    // -----------------------------------------------------------------
    // NCP-6 dataflow writer/finish plan shape
    // -----------------------------------------------------------------

    fn assert_matches_schema(columns: &[OutputColumn], schema: &arrow::datatypes::SchemaRef) {
        assert_eq!(columns.len(), schema.fields().len());
        for (column, field) in columns.iter().zip(schema.fields()) {
            assert_eq!(column.name, *field.name());
            assert_eq!(column.data_type, *field.data_type());
            assert_eq!(column.nullable, field.is_nullable());
        }
    }

    fn table_writer_finish_test_plan() -> crate::planner::distributed::DistributedPlan {
        let draft = with_sql_table_writer_finish(
            single_fragment_plan_for_test().into_draft(),
            test_support::simple_sql_write_plan_input(
                ConnectorWriteInputBinding::RootOutputByOrdinal,
            ),
            target_ordinal_for_test(0),
        )
        .expect("attach dataflow table writer");
        crate::planner::distributed::seal::seal_draft(draft).expect("dataflow write draft seals")
    }

    fn change_stream_table_writer_finish_test_plan() -> crate::planner::distributed::DistributedPlan
    {
        let plan = single_fragment_plan_for_test_with_columns(vec![
            ("__row_mutation_effect", DataType::Int8),
            ("delete_id", DataType::Int32),
            ("replacement", DataType::Int32),
        ]);
        let dag = ChangeStreamWriteDagSpec::for_test(
            0,
            vec![
                test_route(
                    7,
                    0,
                    vec![
                        novarocks_spi::connector::ConnectorRowMutationEffect::Delete,
                        novarocks_spi::connector::ConnectorRowMutationEffect::Replace,
                    ],
                    1,
                    vec![1],
                ),
                test_route(
                    8,
                    1,
                    vec![novarocks_spi::connector::ConnectorRowMutationEffect::Replace],
                    2,
                    Vec::new(),
                ),
            ],
        );
        let planned = with_sql_change_stream_table_writer_finish(plan.into_draft(), dag)
            .expect("plan dataflow change-stream write");
        crate::planner::distributed::seal::seal_draft(planned.distributed_plan)
            .expect("dataflow change-stream draft seals")
    }

    #[test]
    fn regular_change_stream_keeps_an_explicit_empty_auxiliary_plan_shape() {
        let sealed = change_stream_table_writer_finish_test_plan();
        let mut writer_count = 0;
        for fragment in sealed.fragments() {
            match &fragment.root.payload {
                DistributedNodeKind::TableWriter(writer) => {
                    writer_count += 1;
                    assert!(writer.partial_aggregate_plan.calls().is_empty());
                    assert!(
                        writer
                            .writer_multiplex_schema
                            .auxiliary_channels()
                            .is_empty()
                    );
                }
                DistributedNodeKind::TableFinish(finish) => {
                    assert!(finish.final_aggregate_plan.calls().is_empty());
                    assert!(finish.final_aggregate_plan.unpivot().is_none());
                }
                _ => {}
            }
        }
        assert_eq!(writer_count, 2);
    }

    fn change_stream_auxiliary_plan() -> super::super::WriterAuxiliaryPlan {
        let definition = FunctionDefinition::try_new_exact_aggregate(
            "$test_change_stream_blob",
            FunctionVisibility::Hidden,
            FunctionVolatility::Immutable,
            [AggregateOverloadMetadata::try_new(
                "test/change-stream-blob/i64/v1",
                [DataType::Int64],
                DataType::Binary,
                DataType::Binary,
                "test/change-stream-blob-state/v1",
            )
            .expect("aggregate overload")],
        )
        .expect("aggregate definition");
        let mut functions = EngineFunctionCatalogBuilder::new();
        functions.register(definition).expect("register aggregate");
        let functions = functions.seal().expect("function catalog");
        let requirements = [0, 1].map(|target| {
            vec![
                StatisticsRequiredAggregation::try_new(
                    StatisticsScanColumn::try_new(0, "order_id", DataType::Int64, false)
                        .expect("scan column"),
                    "$test_change_stream_blob",
                    StatisticsArtifactIdentity::try_new(
                        vec![target + 1],
                        "test-change-stream-blob-v1",
                    )
                    .expect("artifact identity"),
                )
                .expect("requirement"),
            ]
        });
        let schemas = [
            Schema::new(vec![Field::new("order_id", DataType::Int64, false)]),
            Schema::new(vec![Field::new("order_id", DataType::Int64, false)]),
        ];
        super::super::auxiliary::plan_writer_statistics(
            &[
                super::super::auxiliary::WriterStatisticsTargetInput {
                    target: target_ordinal_for_test(0),
                    input_schema: &schemas[0],
                    requirements: &requirements[0],
                },
                super::super::auxiliary::WriterStatisticsTargetInput {
                    target: target_ordinal_for_test(1),
                    input_schema: &schemas[1],
                    requirements: &requirements[1],
                },
            ],
            &functions,
        )
        .expect("change-stream auxiliary plan")
    }

    #[test]
    fn change_stream_writers_and_single_finish_share_one_nonempty_auxiliary_plan() {
        let plan = single_fragment_plan_for_test_with_columns(vec![
            ("__row_mutation_effect", DataType::Int8),
            ("delete_id", DataType::Int32),
            ("replacement", DataType::Int32),
        ]);
        let dag = ChangeStreamWriteDagSpec::for_test(
            0,
            vec![
                test_route(
                    7,
                    0,
                    vec![novarocks_spi::connector::ConnectorRowMutationEffect::Delete],
                    1,
                    Vec::new(),
                ),
                test_route(
                    8,
                    1,
                    vec![novarocks_spi::connector::ConnectorRowMutationEffect::Replace],
                    2,
                    Vec::new(),
                ),
            ],
        );
        let auxiliary = change_stream_auxiliary_plan();
        let planned = with_sql_change_stream_table_writer_finish_with_auxiliary(
            plan.into_draft(),
            dag,
            &auxiliary,
        )
        .expect("plan auxiliary change stream");
        let sealed = crate::planner::distributed::seal::seal_draft(planned.distributed_plan)
            .expect("auxiliary change stream seals");

        let writers = sealed
            .fragments()
            .iter()
            .filter_map(|fragment| match &fragment.root.payload {
                DistributedNodeKind::TableWriter(writer) => Some(writer),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(writers.len(), 2);
        for writer in writers {
            assert_eq!(writer.partial_aggregate_plan.calls.len(), 1);
            assert_eq!(writer.writer_multiplex_schema.auxiliary_channels().len(), 1);
        }

        let finish = sealed
            .fragments()
            .iter()
            .find_map(|fragment| match &fragment.root.payload {
                DistributedNodeKind::TableFinish(finish) => Some(finish),
                _ => None,
            })
            .expect("single finish");
        assert_eq!(finish.final_aggregate_plan.calls().len(), 1);
        assert_eq!(
            finish
                .final_aggregate_plan
                .unpivot()
                .expect("artifact unpivot")
                .mappings()
                .len(),
            2
        );
        assert_eq!(finish.writer_multiplex_schema.auxiliary_channels().len(), 1);
    }

    #[test]
    fn table_writer_finish_moves_the_result_root_onto_a_new_finish_fragment() {
        let planned = table_writer_finish_test_plan();

        // The original single fragment is now the writer fragment, and a brand
        // new fragment is the plan root even though the writer was the root.
        assert_eq!(planned.fragments().len(), 2);
        let writer = planned
            .fragments()
            .iter()
            .find(|fragment| fragment.fragment_id == 0)
            .expect("writer fragment");
        assert_ne!(planned.root_fragment_id(), writer.fragment_id);

        let DistributedNodeKind::TableWriter(table_writer) = &writer.root.payload else {
            panic!("expected the writer fragment root to be a TableWriter");
        };
        assert_eq!(table_writer.write_target_ordinal.get(), 0);
        // The writer keeps the original plan as its only child.
        assert_eq!(writer.root.children.len(), 1);
        assert!(matches!(
            writer.root.children[0].payload,
            DistributedNodeKind::Values(_)
        ));
        assert!(matches!(writer.sink, DataSink::Noop));
        assert_matches_schema(&writer.output_columns, &writer_output_schema());

        let finish = planned
            .fragments()
            .iter()
            .find(|fragment| fragment.fragment_id == planned.root_fragment_id())
            .expect("finish fragment");
        let DistributedNodeKind::TableFinish(table_finish) = &finish.root.payload else {
            panic!("expected the root fragment root to be a TableFinish");
        };
        assert_eq!(
            table_finish
                .expected_target_ordinals
                .iter()
                .map(|ordinal| ordinal.get())
                .collect::<Vec<_>>(),
            vec![0]
        );
        assert!(matches!(finish.sink, DataSink::Result));
        assert_matches_schema(&finish.output_columns, &root_write_result_schema());
        // Exchange receiver -> TableFinish.
        assert_eq!(finish.root.children.len(), 1);
        let DistributedNodeKind::Exchange(receiver) = &finish.root.children[0].payload else {
            panic!("expected an Exchange receiver under TableFinish");
        };
        assert_eq!(receiver.source_fragment_id, writer.fragment_id);
        assert_matches_schema(&receiver.output_columns, &writer_output_schema());
    }

    #[test]
    fn table_writer_finish_uses_one_gather_stream_edge_and_no_connector_terminal_sink() {
        let planned = table_writer_finish_test_plan();

        assert_eq!(planned.edges().len(), 1);
        let edge = &planned.edges()[0];
        assert_eq!(edge.source_fragment_id, 0);
        assert_eq!(edge.target_fragment_id, planned.root_fragment_id());
        assert!(matches!(edge.edge_kind, FragmentEdgeKind::Stream));
        assert_eq!(edge.stream_kind, FragmentStreamKind::Gather);
        assert!(matches!(
            edge.output_partition.kind,
            crate::planner::distributed::PartitionKind::Unpartitioned
        ));
    }

    #[test]
    fn seal_rejects_a_writer_multiplex_schema_that_differs_from_finish() {
        let mut draft = with_sql_table_writer_finish(
            single_fragment_plan_for_test().into_draft(),
            test_support::simple_sql_write_plan_input(
                ConnectorWriteInputBinding::RootOutputByOrdinal,
            ),
            target_ordinal_for_test(0),
        )
        .expect("attach dataflow table writer");
        let writer = draft
            .fragments
            .iter_mut()
            .find_map(|fragment| match &mut fragment.root.payload {
                DistributedNodeKind::TableWriter(writer) => Some(writer),
                _ => None,
            })
            .expect("writer");
        writer.writer_multiplex_schema =
            novarocks_spi::connector::write_stack::WriterMultiplexSchema::try_new(vec![
                novarocks_spi::connector::write_stack::WriterAuxiliaryChannel::try_new(
                    7,
                    "generic_aux",
                    DataType::Int64,
                )
                .expect("channel"),
            ])
            .expect("schema");
        let error = crate::planner::distributed::seal::seal_draft(draft)
            .expect_err("mismatched relation schemas");
        assert!(
            error.to_string().contains("multiplex schema differs"),
            "{error}"
        );
    }

    #[test]
    fn table_writer_stream_edge_carries_a_sealed_projection() {
        for planned in [
            table_writer_finish_test_plan(),
            change_stream_table_writer_finish_test_plan(),
        ] {
            let finish_fragment_id = planned.root_fragment_id();
            let writer_edges = planned
                .edges()
                .iter()
                .filter(|edge| edge.target_fragment_id == finish_fragment_id)
                .collect::<Vec<_>>();
            assert!(!writer_edges.is_empty());
            for edge in writer_edges {
                // The writer fragment is an ordinary intermediate fragment: it
                // must have a finalized fragment output AND a finalized stream
                // edge projection, or the native encoder cannot resolve the
                // sender side of the gather edge.
                let fragment_output = planned
                    .fragment_edge_outputs()
                    .fragment_output_columns(edge.source_fragment_id)
                    .expect("writer fragment output is sealed");
                assert_matches_schema(fragment_output, &writer_output_schema());

                let projection = planned
                    .fragment_edge_outputs()
                    .stream_edge_projection(edge.target_fragment_id, edge.target_exchange_node_id)
                    .expect("writer stream edge projection is sealed");
                assert_matches_schema(projection, &writer_output_schema());

                // Both sides of the edge agree on the one sealed projection.
                let finish = planned
                    .fragments()
                    .iter()
                    .find(|fragment| fragment.fragment_id == finish_fragment_id)
                    .expect("finish fragment");
                let receiver = finish
                    .root
                    .children
                    .iter()
                    .find(|child| child.node_id == edge.target_exchange_node_id)
                    .expect("edge target exchange receiver");
                let DistributedNodeKind::Exchange(receiver) = &receiver.payload else {
                    panic!("edge target must be an Exchange receiver");
                };
                assert_eq!(
                    receiver
                        .output_columns
                        .iter()
                        .map(|column| column.column_id)
                        .collect::<Vec<_>>(),
                    projection
                        .iter()
                        .map(|column| column.column_id)
                        .collect::<Vec<_>>()
                );
                assert_eq!(
                    edge.output_slot_ids,
                    projection
                        .iter()
                        .map(|column| i32::try_from(column.column_id.0).expect("slot id"))
                        .collect::<Vec<_>>()
                );
            }
            // The finish fragment itself is also an ordinary sealed output.
            assert_matches_schema(
                planned
                    .fragment_edge_outputs()
                    .fragment_output_columns(finish_fragment_id)
                    .expect("finish fragment output is sealed"),
                &root_write_result_schema(),
            );
        }
    }

    #[test]
    fn change_stream_table_writer_finish_gathers_every_route_into_one_finish_fragment() {
        let planned = change_stream_table_writer_finish_test_plan();

        // Router fragment (0), two writer fragments, one finish fragment.
        assert_eq!(planned.fragments().len(), 4);
        let router = planned
            .fragments()
            .iter()
            .find(|fragment| fragment.fragment_id == 0)
            .expect("router fragment");
        assert!(matches!(router.sink, DataSink::ChangeStreamRouter(_)));
        // The router is deliberately no longer the plan root.
        assert_ne!(planned.root_fragment_id(), router.fragment_id);

        let mut writer_ordinals = planned
            .fragments()
            .iter()
            .filter_map(|fragment| match &fragment.root.payload {
                DistributedNodeKind::TableWriter(writer) => {
                    assert!(matches!(fragment.sink, DataSink::Noop));
                    assert_matches_schema(&fragment.output_columns, &writer_output_schema());
                    // Exchange -> TableWriter.
                    assert_eq!(fragment.root.children.len(), 1);
                    assert!(matches!(
                        fragment.root.children[0].payload,
                        DistributedNodeKind::Exchange(_)
                    ));
                    Some(writer.write_target_ordinal.get())
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        writer_ordinals.sort_unstable();
        assert_eq!(writer_ordinals, vec![0, 1]);

        let finish_fragments = planned
            .fragments()
            .iter()
            .filter(|fragment| matches!(fragment.root.payload, DistributedNodeKind::TableFinish(_)))
            .collect::<Vec<_>>();
        assert_eq!(finish_fragments.len(), 1);
        let finish = finish_fragments[0];
        assert_eq!(finish.fragment_id, planned.root_fragment_id());
        assert!(matches!(finish.sink, DataSink::Result));
        // One Exchange receiver per writer fragment, all under one TableFinish.
        assert_eq!(finish.root.children.len(), 2);

        // Every writer edge into the finish fragment is a plain gather stream.
        let writer_edges = planned
            .edges()
            .iter()
            .filter(|edge| edge.target_fragment_id == finish.fragment_id)
            .collect::<Vec<_>>();
        assert_eq!(writer_edges.len(), 2);
        for edge in writer_edges {
            assert!(matches!(edge.edge_kind, FragmentEdgeKind::Stream));
            assert_eq!(edge.stream_kind, FragmentStreamKind::Gather);
        }
    }

    #[test]
    fn change_stream_table_writer_finish_assigns_dense_ordinals_in_route_order() {
        let planned = change_stream_table_writer_finish_test_plan();

        let finish = planned
            .fragments()
            .iter()
            .find(|fragment| fragment.fragment_id == planned.root_fragment_id())
            .expect("finish fragment");
        let DistributedNodeKind::TableFinish(table_finish) = &finish.root.payload else {
            panic!("expected TableFinish root");
        };
        assert_eq!(
            table_finish
                .expected_target_ordinals
                .iter()
                .map(|ordinal| ordinal.get())
                .collect::<Vec<_>>(),
            vec![0, 1]
        );

        // Route order decides the ordinal: the first route's writer fragment
        // owns ordinal 0.
        let DataSink::ChangeStreamRouter(router) = &planned
            .fragments()
            .iter()
            .find(|fragment| fragment.fragment_id == 0)
            .expect("router fragment")
            .sink
        else {
            panic!("expected router sink");
        };
        for (route_index, route) in router.routes.iter().enumerate() {
            let writer = planned
                .fragments()
                .iter()
                .find(|fragment| fragment.fragment_id == route.target_fragment_id)
                .expect("route writer fragment");
            let DistributedNodeKind::TableWriter(table_writer) = &writer.root.payload else {
                panic!("expected TableWriter root for a route writer fragment");
            };
            assert_eq!(
                table_writer.write_target_ordinal.get(),
                u32::try_from(route_index).expect("ordinal")
            );
        }
    }

    #[test]
    fn seal_rejects_a_table_writer_fragment_with_no_path_to_the_finish_fragment() {
        let mut draft = with_sql_table_writer_finish(
            single_fragment_plan_for_test().into_draft(),
            test_support::simple_sql_write_plan_input(
                ConnectorWriteInputBinding::RootOutputByOrdinal,
            ),
            target_ordinal_for_test(0),
        )
        .expect("attach dataflow table writer");
        draft.edges.clear();

        let error = crate::planner::distributed::seal::seal_draft(draft)
            .expect_err("a writer with no path to the finish fragment must not seal")
            .to_string();
        assert!(
            error.contains("must drive exactly one outgoing stream edge"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn seal_rejects_a_table_writer_streaming_into_a_non_finish_fragment() {
        let mut draft = with_sql_change_stream_table_writer_finish(
            single_fragment_plan_for_test_with_columns(vec![
                ("__row_mutation_effect", DataType::Int8),
                ("delete_id", DataType::Int32),
                ("replacement", DataType::Int32),
            ])
            .into_draft(),
            ChangeStreamWriteDagSpec::for_test(
                0,
                vec![
                    test_route(
                        7,
                        0,
                        vec![novarocks_spi::connector::ConnectorRowMutationEffect::Delete],
                        1,
                        Vec::new(),
                    ),
                    test_route(
                        8,
                        1,
                        vec![novarocks_spi::connector::ConnectorRowMutationEffect::Replace],
                        2,
                        Vec::new(),
                    ),
                ],
            ),
        )
        .expect("plan dataflow change-stream write")
        .distributed_plan;

        // Retarget the first writer's gather edge at the other writer fragment.
        let finish_fragment_id = draft.root_fragment_id.expect("finish root");
        let other_writer_fragment_id = draft
            .fragments
            .iter()
            .find(|fragment| {
                matches!(fragment.root.payload, DistributedNodeKind::TableWriter(_))
                    && draft.edges.iter().any(|edge| {
                        edge.source_fragment_id != fragment.fragment_id
                            && edge.target_fragment_id == finish_fragment_id
                    })
            })
            .map(|fragment| fragment.fragment_id)
            .expect("a writer fragment");
        let edge = draft
            .edges
            .iter_mut()
            .find(|edge| {
                edge.target_fragment_id == finish_fragment_id
                    && edge.source_fragment_id != other_writer_fragment_id
            })
            .expect("a writer gather edge");
        edge.target_fragment_id = other_writer_fragment_id;

        let error = crate::planner::distributed::seal::seal_draft(draft)
            .expect_err("a writer streaming outside the finish fragment must not seal")
            .to_string();
        assert!(
            error.contains("instead of the TableFinish fragment")
                || error.contains("receives from non-writer fragment")
                || error.contains("not found in target fragment"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn seal_rejects_a_table_finish_whose_expected_ordinals_do_not_match_its_writers() {
        let mut draft = with_sql_table_writer_finish(
            single_fragment_plan_for_test().into_draft(),
            test_support::simple_sql_write_plan_input(
                ConnectorWriteInputBinding::RootOutputByOrdinal,
            ),
            target_ordinal_for_test(0),
        )
        .expect("attach dataflow table writer");
        let finish_fragment_id = draft.root_fragment_id.expect("finish root");
        let finish = draft
            .fragments
            .iter_mut()
            .find(|fragment| fragment.fragment_id == finish_fragment_id)
            .expect("finish fragment");
        let DistributedNodeKind::TableFinish(table_finish) = &mut finish.root.payload else {
            panic!("expected TableFinish root");
        };
        table_finish.expected_target_ordinals.push(
            novarocks_spi::connector::write_stack::WriteTargetOrdinal::try_new(1)
                .expect("bounded ordinal"),
        );

        let error = crate::planner::distributed::seal::seal_draft(draft)
            .expect_err("an unmatched expected ordinal set must not seal")
            .to_string();
        assert!(
            error.contains("expects write target ordinals"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn seal_rejects_a_table_writer_outside_a_fragment_root() {
        let mut draft = with_sql_table_writer_finish(
            single_fragment_plan_for_test().into_draft(),
            test_support::simple_sql_write_plan_input(
                ConnectorWriteInputBinding::RootOutputByOrdinal,
            ),
            target_ordinal_for_test(0),
        )
        .expect("attach dataflow table writer");
        let writer = draft
            .fragments
            .iter_mut()
            .find(|fragment| matches!(fragment.root.payload, DistributedNodeKind::TableWriter(_)))
            .expect("writer fragment");
        // Bury the writer under a passthrough node so it is no longer the root.
        let buried = writer.root.clone();
        writer.root = DistributedNode {
            node_id: 9_000,
            fragment_id: buried.fragment_id,
            tuple_ids: vec![9_000],
            nullable_tuple_ids: Vec::new(),
            limit: -1,
            runtime_filter_binding_ids: Vec::new(),
            children: vec![buried],
            stats: stats(),
            payload: DistributedNodeKind::AssertOneRow(
                crate::planner::payload::PlanAssertOneRowNode::global_at_most_one("write"),
            ),
        };

        let error = crate::planner::distributed::seal::seal_draft(draft)
            .expect_err("a non-root TableWriter must not seal")
            .to_string();
        assert!(
            error.contains("TableWriter") && error.contains("must be the fragment root"),
            "unexpected error: {error}"
        );
    }
}
