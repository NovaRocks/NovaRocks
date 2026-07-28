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

use std::{collections::HashMap, fmt::Write};

use arrow::datatypes::DataType;

use crate::coordinator::profile::correlate::{ActualMetrics, DistributedProfileSummary};
use crate::runtime_filter::model::contract::{
    ConsumerActivation, LateApplyGranularity, NullOrder, NullSemantics, RuntimeFilterLogicalDomain,
    SortDirection,
};
use crate::runtime_filter::model::graph::{ProducerBindingTarget, RuntimeFilterBindingRole};
use crate::sql::analysis::{ExprKind, JoinKind, TypedExpr};
use crate::sql::column_id::ColumnId;
use crate::sql::common::ScanVariantColumn;
use crate::sql::explain::{
    ExplainLevel, PlanNodeExplainStage, format_assert_one_row_header, format_expr,
    format_project_item, format_sort_items, format_window_exprs,
};
use crate::sql::planner::distributed::{
    DistributedNode, DistributedNodeKind, DistributedPlan, ExchangeFlavor, ExchangeReceiver,
    PartitionKind, PlanFragment,
};
use crate::sql::planner::payload::{
    PlanAssertOneRowNode as DistributedAssertOneRowNode, PlanFilterNode as DistributedFilterNode,
    PlanGenerateSeriesNode as DistributedGenerateSeriesNode,
    PlanProjectNode as DistributedProjectNode, PlanRepeatNode as DistributedRepeatNode,
    PlanScanNode as DistributedScanNode, PlanSortNode as DistributedSortNode,
    PlanTableFunctionNode as DistributedTableFunctionNode, PlanValuesNode as DistributedValuesNode,
    PlanWindowNode as DistributedWindowNode,
};
use crate::sql::planner::physical::{
    AggMode, DEFAULT_CPU_COST_WEIGHT, DEFAULT_MEMORY_COST_WEIGHT, DEFAULT_NETWORK_COST_WEIGHT,
    DistributedChangeEventExpandNode, JoinDistribution, JoinExecutionMode, MAX_ROW_COUNT,
    PhysicalHashAggregateNode, PhysicalHashJoinNode, PhysicalNestLoopJoinNode, PhysicalPlanKind,
    PhysicalPlanStats, PhysicalSetOpNode, PhysicalTopNNode, PlanSetOpKind as SetOpKind,
    PlannerBroadcastDecision, PlannerConfidence, PlannerCostEstimate, TopNPhase,
};
use crate::sql::planner::table::{ScanSource, TableDef};

pub(crate) fn explain_distributed_plan(dp: &DistributedPlan, level: ExplainLevel) -> Vec<String> {
    explain_distributed_plan_inner(dp, level, None, None)
}

pub(crate) fn explain_distributed_plan_analyze(
    dp: &DistributedPlan,
    level: ExplainLevel,
    actuals: &HashMap<i32, ActualMetrics>,
    per_fragment: Option<&HashMap<i32, DistributedProfileSummary>>,
) -> Vec<String> {
    explain_distributed_plan_inner(dp, level, Some(actuals), per_fragment)
}

fn explain_distributed_plan_inner(
    dp: &DistributedPlan,
    level: ExplainLevel,
    actuals: Option<&HashMap<i32, ActualMetrics>>,
    per_fragment: Option<&HashMap<i32, DistributedProfileSummary>>,
) -> Vec<String> {
    let mut out = Vec::new();
    let fragments = explain_fragment_order(dp);
    let detailed = is_detailed(level);

    if detailed && !dp.runtime_filter_graph().is_empty() {
        out.push("RUNTIME FILTER GRAPH".to_string());
        for channel in dp.runtime_filter_graph().channels() {
            out.push("  runtime filter".to_string());
            out.push(format!(
                "    domain = {}",
                format_runtime_filter_domain(&channel.logical_domain)
            ));
            for binding in dp
                .runtime_filter_graph()
                .bindings()
                .filter(|binding| binding.channel_id == channel.channel_id)
            {
                match &binding.role {
                    RuntimeFilterBindingRole::Producer(requirement) => out.push(format!(
                        "    producer binding: target = {}, expr = ({})",
                        format_runtime_filter_producer_target(&requirement.target),
                        format_expr(&binding.expression),
                    )),
                    RuntimeFilterBindingRole::Consumer(requirement) => out.push(format!(
                        "    consumer binding: activation = {}, expr = ({})",
                        format_runtime_filter_activation(requirement.activation),
                        format_expr(&binding.expression),
                    )),
                }
            }
        }
    }

    for (display_id, fragment) in fragments.iter().enumerate() {
        if detailed {
            out.push(format!("PLAN FRAGMENT {display_id}"));
            if let Some(per_fragment) = per_fragment {
                // Key by the fragment's root (output) node id — the same id the profile collector
                // derives from the `execute_fragment (plan_node_id=N)` tree root. Unique per
                // fragment and never a cross-fragment-shared exchange id.
                if let Some(s) = per_fragment.get(&fragment.root.node_id) {
                    out.push(format!(
                        "  Profile: active={} blocked={} dep_wait={} exch_wait={} net={} scan_io={}",
                        fmt_time_ns(s.operator_active_time_ns),
                        fmt_time_ns(s.driver_blocked_time_ns),
                        fmt_time_ns(s.dependency_wait_time_ns),
                        fmt_time_ns(s.exchange_wait_time_ns),
                        fmt_time_ns(s.network_time_ns),
                        fmt_time_ns(s.scan_io_time_ns),
                    ));
                }
            }
            out.push(format!(
                "  OUTPUT EXPRS: {}",
                format_output_exprs(fragment.output_exprs.as_deref())
            ));
            out.push(format!(
                "  PARTITION: {}",
                fragment.data_partition.explain_label()
            ));
            if fragment.fragment_id != dp.root_fragment_id() {
                let source_edges = dp
                    .edges()
                    .iter()
                    .filter(|edge| edge.source_fragment_id == fragment.fragment_id)
                    .collect::<Vec<_>>();
                if source_edges.is_empty() {
                    out.push("  STREAM DATA SINK".to_string());
                    out.push(format!(
                        "    PARTITION: {}",
                        fragment.output_partition.explain_label()
                    ));
                } else {
                    for edge in source_edges {
                        out.push("  STREAM DATA SINK".to_string());
                        out.push(format!("    EXCHANGE ID: {}", edge.target_exchange_node_id));
                        out.push(format!(
                            "    PARTITION: {}",
                            fragment.output_partition.explain_label()
                        ));
                    }
                }
            }
        }

        format_distributed_node(&fragment.root, level, 0, actuals, &mut out);
    }

    out
}

fn format_runtime_filter_domain(domain: &RuntimeFilterLogicalDomain) -> String {
    match domain {
        RuntimeFilterLogicalDomain::Membership {
            value_type,
            null_semantics,
        } => format!(
            "Membership(value_type={value_type}, null_semantics={})",
            match null_semantics {
                NullSemantics::NeverMatches => "NeverMatches",
                NullSemantics::NullSafeEqual => "NullSafeEqual",
            }
        ),
        RuntimeFilterLogicalDomain::OrderedBound(order) => {
            let keys = order
                .keys
                .iter()
                .map(|key| {
                    format!(
                        "{} {} NULLS {}",
                        key.data_type,
                        match key.direction {
                            SortDirection::Ascending => "ASC",
                            SortDirection::Descending => "DESC",
                        },
                        match key.null_order {
                            NullOrder::First => "FIRST",
                            NullOrder::Last => "LAST",
                        },
                    )
                })
                .collect::<Vec<_>>();
            match keys.as_slice() {
                [key] => format!("OrderedBound(key={key}, inclusive={})", order.inclusive),
                _ => format!(
                    "OrderedBound(keys=[{}], inclusive={})",
                    keys.join(", "),
                    order.inclusive
                ),
            }
        }
    }
}

fn format_runtime_filter_producer_target(target: &ProducerBindingTarget) -> String {
    match target {
        ProducerBindingTarget::JoinBuildKey { ordinal } => {
            format!("JoinBuildKey(ordinal={ordinal})")
        }
        ProducerBindingTarget::AggregateTopNKey {
            group_key_ordinal,
            limit,
        } => format!(
            "AggregateTopNKey(group_key_ordinal={group_key_ordinal}, limit={})",
            limit.get()
        ),
    }
}

fn format_runtime_filter_activation(activation: ConsumerActivation) -> &'static str {
    match activation {
        ConsumerActivation::BlockingSnapshot => "BlockingSnapshot",
        ConsumerActivation::NonBlockingLive {
            late_apply: LateApplyGranularity::Row,
        } => "NonBlockingLive(Row)",
        ConsumerActivation::NonBlockingLive {
            late_apply: LateApplyGranularity::Batch,
        } => "NonBlockingLive(Batch)",
        ConsumerActivation::NonBlockingLive {
            late_apply: LateApplyGranularity::RowGroup,
        } => "NonBlockingLive(RowGroup)",
        ConsumerActivation::NonBlockingLive {
            late_apply: LateApplyGranularity::Split,
        } => "NonBlockingLive(Split)",
        ConsumerActivation::NonBlockingLive {
            late_apply: LateApplyGranularity::File,
        } => "NonBlockingLive(File)",
    }
}

fn explain_fragment_order(dp: &DistributedPlan) -> Vec<&PlanFragment> {
    let mut ordered = Vec::with_capacity(dp.fragments().len());
    if let Some(root) = dp
        .fragments()
        .iter()
        .find(|fragment| fragment.fragment_id == dp.root_fragment_id())
    {
        ordered.push(root);
    }
    ordered.extend(
        dp.fragments()
            .iter()
            .rev()
            .filter(|fragment| fragment.fragment_id != dp.root_fragment_id()),
    );
    ordered
}

fn format_output_exprs(exprs: Option<&[TypedExpr]>) -> String {
    match exprs {
        Some(exprs) if !exprs.is_empty() => {
            exprs.iter().map(format_expr).collect::<Vec<_>>().join(", ")
        }
        _ => "*".to_string(),
    }
}

fn format_distributed_shared_plan_node_header(
    kind: &PhysicalPlanKind,
    stage: PlanNodeExplainStage,
) -> Option<String> {
    match kind {
        PhysicalPlanKind::Scan(node) => {
            let alias = node
                .alias
                .as_deref()
                .map(|a| format!(" (alias={a})"))
                .unwrap_or_default();
            Some(format!(
                "SCAN {}.{}{}",
                node.database, node.table.name, alias
            ))
        }
        PhysicalPlanKind::Filter(_) => Some("FILTER".to_string()),
        PhysicalPlanKind::Project(node) => {
            let items = node
                .items
                .iter()
                .map(format_project_item)
                .collect::<Vec<_>>();
            Some(format!("PROJECT [{}]", items.join(", ")))
        }
        PhysicalPlanKind::Sort(node) => {
            let items = format_sort_items(&node.items);
            Some(format!("SORT BY [{}]", items.join(", ")))
        }
        PhysicalPlanKind::Window(node) => {
            let fns = format_window_exprs(&node.window_exprs, stage);
            Some(format!("WINDOW [{}]", fns.join("; ")))
        }
        PhysicalPlanKind::Values(node) => Some(format!("VALUES ({} rows)", node.rows.len())),
        PhysicalPlanKind::Repeat(node) => Some(format!(
            "REPEAT ({} grouping sets)",
            node.grouping_ids.len()
        )),
        PhysicalPlanKind::ChangeEventExpand(node) => {
            let branches = node
                .events
                .iter()
                .map(|event| format!("{:?}", event.branch_kind))
                .collect::<Vec<_>>();
            Some(format!(
                "CHANGE_EVENT_EXPAND(events={}, branches=[{}])",
                node.events.len(),
                branches.join(",")
            ))
        }
        PhysicalPlanKind::GenerateSeries(node) => Some(format!(
            "GENERATE_SERIES({}, {}, {})",
            node.start, node.end, node.step
        )),
        PhysicalPlanKind::TableFunction(node) => {
            let join_type = if node.is_left_join { "LEFT" } else { "CROSS" };
            Some(format!(
                "TABLE_FUNCTION [{} {}]",
                join_type,
                node.function_name.to_uppercase()
            ))
        }
        PhysicalPlanKind::AssertOneRow(node) => Some(format_assert_one_row_header(node, stage)),
        _ => None,
    }
}

fn format_distributed_shared_plan_node_detail_lines(
    kind: &PhysicalPlanKind,
    _stage: PlanNodeExplainStage,
) -> Vec<String> {
    match kind {
        PhysicalPlanKind::Filter(node) => {
            vec![format!("predicate: {}", format_expr(&node.predicate))]
        }
        _ => vec![],
    }
}

fn format_distributed_node(
    node: &DistributedNode,
    level: ExplainLevel,
    indent: usize,
    actuals: Option<&HashMap<i32, ActualMetrics>>,
    out: &mut Vec<String>,
) {
    let pad = "  ".repeat(indent);
    let costs_suffix = costs_suffix(&node.stats, level);
    let stats_suffix = format!(
        "{}{}",
        stats_suffix(&node.stats, level),
        actual_suffix(node, actuals)
    );

    match &node.payload {
        DistributedNodeKind::Scan(scan) => {
            format_scan_node(node, scan, level, &pad, &costs_suffix, &stats_suffix, out);
        }
        DistributedNodeKind::Project(project) => {
            format_project_node(node, project, &pad, &costs_suffix, &stats_suffix, out);
            format_children(node, level, indent, actuals, out);
        }
        DistributedNodeKind::Filter(filter) => {
            format_filter_node(node, filter, &pad, &costs_suffix, &stats_suffix, out);
            format_children(node, level, indent, actuals, out);
        }
        DistributedNodeKind::HashJoin(join) => {
            format_hash_join_node(node, join, level, &pad, &costs_suffix, &stats_suffix, out);
            format_children(node, level, indent, actuals, out);
        }
        DistributedNodeKind::NestLoopJoin(join) => {
            format_nest_loop_join_node(node, join, &pad, &costs_suffix, &stats_suffix, out);
            format_children(node, level, indent, actuals, out);
        }
        DistributedNodeKind::HashAggregate(agg) => {
            format_hash_aggregate_node(node, agg, &pad, &costs_suffix, &stats_suffix, out);
            format_children(node, level, indent, actuals, out);
        }
        DistributedNodeKind::Sort(sort) => {
            format_sort_node(node, sort, &pad, &costs_suffix, &stats_suffix, out);
            format_children(node, level, indent, actuals, out);
        }
        DistributedNodeKind::TopN(topn) => {
            format_topn_node(node, topn, &pad, &costs_suffix, &stats_suffix, out);
            format_children(node, level, indent, actuals, out);
        }
        DistributedNodeKind::Exchange(exchange) => {
            format_exchange_node(node, exchange, &pad, &costs_suffix, &stats_suffix, out);
        }
        DistributedNodeKind::Values(values) => {
            format_values_node(node, values, &pad, &costs_suffix, &stats_suffix, out);
        }
        DistributedNodeKind::AssertOneRow(assert) => {
            format_assert_one_row_node(node, assert, &pad, &costs_suffix, &stats_suffix, out);
            format_children(node, level, indent, actuals, out);
        }
        DistributedNodeKind::Repeat(repeat) => {
            format_repeat_node(node, repeat, &pad, &costs_suffix, &stats_suffix, out);
            format_children(node, level, indent, actuals, out);
        }
        DistributedNodeKind::ChangeEventExpand(expand) => {
            format_change_event_expand_node(node, expand, &pad, &costs_suffix, &stats_suffix, out);
            format_children(node, level, indent, actuals, out);
        }
        DistributedNodeKind::SetOp(set_op) => {
            format_set_op_node(node, set_op, &pad, &costs_suffix, &stats_suffix, out);
            format_children(node, level, indent, actuals, out);
        }
        DistributedNodeKind::Window(window) => {
            format_window_node(node, window, &pad, &costs_suffix, &stats_suffix, out);
            format_children(node, level, indent, actuals, out);
        }
        DistributedNodeKind::GenerateSeries(generate) => {
            format_generate_series_node(node, generate, &pad, &costs_suffix, &stats_suffix, out);
        }
        DistributedNodeKind::TableFunction(table_function) => {
            format_table_function_node(
                node,
                table_function,
                &pad,
                &costs_suffix,
                &stats_suffix,
                out,
            );
            format_children(node, level, indent, actuals, out);
        }
    }
}

fn format_children(
    node: &DistributedNode,
    level: ExplainLevel,
    indent: usize,
    actuals: Option<&HashMap<i32, ActualMetrics>>,
    out: &mut Vec<String>,
) {
    for child in &node.children {
        format_distributed_node(child, level, indent + 1, actuals, out);
    }
}

fn node_prefix(node: &DistributedNode) -> String {
    format!("{}:", node.node_id)
}

fn physical_payload(node: &DistributedNode) -> Option<PhysicalPlanKind> {
    match &node.payload {
        DistributedNodeKind::Exchange(_) => None,
        kind => Some(crate::sql::planner::distributed::distributed_kind_to_physical(kind)),
    }
}

#[cfg(test)]
fn physical_kind_name(payload: &DistributedNodeKind) -> &'static str {
    match payload {
        DistributedNodeKind::Scan(_) => "Scan",
        DistributedNodeKind::Filter(_) => "Filter",
        DistributedNodeKind::Project(_) => "Project",
        DistributedNodeKind::Sort(_) => "Sort",
        DistributedNodeKind::Values(_) => "Values",
        DistributedNodeKind::Repeat(_) => "Repeat",
        DistributedNodeKind::Window(_) => "Window",
        DistributedNodeKind::GenerateSeries(_) => "GenerateSeries",
        DistributedNodeKind::TableFunction(_) => "TableFunction",
        DistributedNodeKind::AssertOneRow(_) => "AssertOneRow",
        DistributedNodeKind::TopN(_) => "TopN",
        DistributedNodeKind::HashAggregate(_) => "HashAggregate",
        DistributedNodeKind::HashJoin(_) => "HashJoin",
        DistributedNodeKind::NestLoopJoin(_) => "NestLoopJoin",
        DistributedNodeKind::SetOp(_) => "SetOp",
        DistributedNodeKind::ChangeEventExpand(_) => "ChangeEventExpand",
        DistributedNodeKind::Exchange(_) => "Exchange",
    }
}

fn format_scan_node(
    node: &DistributedNode,
    scan: &DistributedScanNode,
    level: ExplainLevel,
    pad: &str,
    costs_suffix: &str,
    stats_suffix: &str,
    out: &mut Vec<String>,
) {
    let header = format_distributed_shared_plan_node_header(
        &physical_payload(node).expect("Scan is a physical explain node"),
        PlanNodeExplainStage::Distributed,
    )
    .expect("Scan is a shared explain node");
    out.push(format!(
        "{pad}{}{header}{costs_suffix}{stats_suffix}",
        node_prefix(node),
    ));
    out.push(format!(
        "{pad}     TABLE: {}.{}",
        scan.database, scan.table.name
    ));
    if let Some(ref mv) = scan.mv_rewritten_from {
        out.push(format!("{pad}     rewritten with mv: {mv}"));
    }
    if let Some(ref cols) = scan.required_columns
        && is_detailed(level)
    {
        out.push(format!("{pad}     columns: {}", cols.join(", ")));
        if matches!(level, ExplainLevel::Verbose | ExplainLevel::Analyze) {
            for line in scan_pruned_type_lines(scan, cols) {
                out.push(format!("{pad}     {line}"));
            }
        }
    }
    if is_detailed(level)
        && let Some(line) = scan_variant_column_line(scan)
    {
        out.push(format!("{pad}     {line}"));
    }
    let local_hints = explain_hints_for_scan(scan);
    if matches!(level, ExplainLevel::Verbose | ExplainLevel::Analyze)
        && local_hints.has_min_max_stats
    {
        out.push(format!("{pad}     min-max stats"));
    }
    if !scan.predicates.is_empty() {
        let preds = scan
            .predicates
            .iter()
            .map(|expr| format_scan_predicate(expr, scan))
            .collect::<Vec<_>>();
        out.push(format!("{pad}     predicates: {}", preds.join(" AND ")));
    }
}

fn format_scan_predicate(expr: &TypedExpr, scan: &DistributedScanNode) -> String {
    let displays = scan
        .columns
        .iter()
        .map(|column| (column.column_id, (scan.alias.clone(), column.name.clone())))
        .collect::<HashMap<ColumnId, (Option<String>, String)>>();
    let mut expr = expr.clone();
    rewrite_scan_column_display(&mut expr, &displays);
    format_expr(&expr)
}

fn rewrite_scan_column_display(
    expr: &mut TypedExpr,
    displays: &HashMap<ColumnId, (Option<String>, String)>,
) {
    match &mut expr.kind {
        ExprKind::ColumnRef {
            column_id,
            qualifier,
            column,
        } => {
            if let Some((scan_qualifier, scan_column)) = displays.get(column_id) {
                *qualifier = scan_qualifier.clone();
                *column = scan_column.clone();
            }
        }
        ExprKind::BinaryOp { left, right, .. } => {
            rewrite_scan_column_display(left, displays);
            rewrite_scan_column_display(right, displays);
        }
        ExprKind::UnaryOp { expr, .. }
        | ExprKind::Cast { expr, .. }
        | ExprKind::IsNull { expr, .. }
        | ExprKind::IsTruthValue { expr, .. }
        | ExprKind::Nested(expr) => rewrite_scan_column_display(expr, displays),
        ExprKind::FunctionCall { args, .. } | ExprKind::AggregateCall { args, .. } => {
            for arg in args {
                rewrite_scan_column_display(arg, displays);
            }
        }
        ExprKind::LambdaFunction { body, .. } | ExprKind::Lambda { body, .. } => {
            rewrite_scan_column_display(body, displays);
        }
        ExprKind::InList { expr, list, .. } => {
            rewrite_scan_column_display(expr, displays);
            for item in list {
                rewrite_scan_column_display(item, displays);
            }
        }
        ExprKind::Between {
            expr, low, high, ..
        } => {
            rewrite_scan_column_display(expr, displays);
            rewrite_scan_column_display(low, displays);
            rewrite_scan_column_display(high, displays);
        }
        ExprKind::Like { expr, pattern, .. } => {
            rewrite_scan_column_display(expr, displays);
            rewrite_scan_column_display(pattern, displays);
        }
        ExprKind::Case {
            operand,
            when_then,
            else_expr,
        } => {
            if let Some(operand) = operand {
                rewrite_scan_column_display(operand, displays);
            }
            for (when, then) in when_then {
                rewrite_scan_column_display(when, displays);
                rewrite_scan_column_display(then, displays);
            }
            if let Some(else_expr) = else_expr {
                rewrite_scan_column_display(else_expr, displays);
            }
        }
        ExprKind::WindowCall {
            args,
            partition_by,
            order_by,
            ..
        } => {
            for arg in args {
                rewrite_scan_column_display(arg, displays);
            }
            for partition in partition_by {
                rewrite_scan_column_display(partition, displays);
            }
            for item in order_by {
                rewrite_scan_column_display(&mut item.expr, displays);
            }
        }
        ExprKind::Literal(_)
        | ExprKind::LambdaParamRef { .. }
        | ExprKind::SubqueryPlaceholder { .. } => {}
    }
}

fn format_project_node(
    node: &DistributedNode,
    _project: &DistributedProjectNode,
    pad: &str,
    costs_suffix: &str,
    stats_suffix: &str,
    out: &mut Vec<String>,
) {
    let header = format_distributed_shared_plan_node_header(
        &physical_payload(node).expect("Project is a physical explain node"),
        PlanNodeExplainStage::Distributed,
    )
    .expect("Project is a shared explain node");
    out.push(format!(
        "{pad}{}{header}{costs_suffix}{stats_suffix}",
        node_prefix(node),
    ));
}

fn format_filter_node(
    node: &DistributedNode,
    _filter: &DistributedFilterNode,
    pad: &str,
    costs_suffix: &str,
    stats_suffix: &str,
    out: &mut Vec<String>,
) {
    let header = format_distributed_shared_plan_node_header(
        &physical_payload(node).expect("Filter is a physical explain node"),
        PlanNodeExplainStage::Distributed,
    )
    .expect("Filter is a shared explain node");
    out.push(format!(
        "{pad}{}{header}{costs_suffix}{stats_suffix}",
        node_prefix(node)
    ));
    for line in format_distributed_shared_plan_node_detail_lines(
        &physical_payload(node).expect("Filter is a physical explain node"),
        PlanNodeExplainStage::Distributed,
    ) {
        out.push(format!("{pad}  {line}"));
    }
}

fn format_hash_join_node(
    node: &DistributedNode,
    join: &PhysicalHashJoinNode,
    level: ExplainLevel,
    pad: &str,
    costs_suffix: &str,
    stats_suffix: &str,
    out: &mut Vec<String>,
) {
    let dist = join_distribution_label(join.execution_mode, &join.distribution);
    let join_str = join_kind_label(join.join_type);
    let eq = join
        .eq_conditions
        .iter()
        .map(|eq| {
            format!(
                "{} {} {}",
                format_expr(&eq.left),
                if eq.null_safe { "<=>" } else { "=" },
                format_expr(&eq.right)
            )
        })
        .collect::<Vec<_>>();
    let bcast_v = bcast_verbose_suffix(&node.stats.broadcast_decision, level);
    let bcast_c = bcast_costs_suffix(&node.stats.broadcast_decision, level);
    out.push(format!(
        "{pad}{}HASH JOIN ({dist}, {join_str}, eq: [{}]){costs_suffix}{bcast_c}{bcast_v}{stats_suffix}",
        node_prefix(node),
        eq.join(", ")
    ));
    if let Some(ref other) = join.other_condition {
        out.push(format!("{pad}  other: {}", format_expr(other)));
    }
}

fn format_nest_loop_join_node(
    node: &DistributedNode,
    join: &PhysicalNestLoopJoinNode,
    pad: &str,
    costs_suffix: &str,
    stats_suffix: &str,
    out: &mut Vec<String>,
) {
    out.push(format!(
        "{pad}{}NEST LOOP JOIN ({}){costs_suffix}{stats_suffix}",
        node_prefix(node),
        join_kind_label(join.join_type)
    ));
    if let Some(ref cond) = join.condition {
        out.push(format!("{pad}  on: {}", format_expr(cond)));
    }
}

fn format_hash_aggregate_node(
    node: &DistributedNode,
    agg: &PhysicalHashAggregateNode,
    pad: &str,
    costs_suffix: &str,
    stats_suffix: &str,
    out: &mut Vec<String>,
) {
    let mode = match agg.mode {
        AggMode::Single => "SINGLE",
        AggMode::Local => "LOCAL",
        AggMode::Global => "GLOBAL",
        AggMode::DistinctGlobal => "DISTINCT_GLOBAL",
        AggMode::DistinctLocal => "DISTINCT_LOCAL",
    };
    let group_lookup = node
        .children
        .first()
        .map(column_display_lookup)
        .unwrap_or_default();
    let groups = agg
        .group_by
        .iter()
        .map(|expr| format_group_expr(expr, &group_lookup))
        .collect::<Vec<_>>();
    let aggs = agg
        .aggregates
        .iter()
        .map(|a| {
            let args = a.args.iter().map(format_expr).collect::<Vec<_>>();
            let distinct = if a.distinct { "DISTINCT " } else { "" };
            format!("{}({}{})", a.name, distinct, args.join(", "))
        })
        .collect::<Vec<_>>();
    let mut detail = format!("{pad}{}HASH AGGREGATE ({mode}", node_prefix(node));
    if !groups.is_empty() {
        let _ = write!(detail, ", group by: [{}]", groups.join(", "));
    }
    let _ = write!(detail, "){costs_suffix}{stats_suffix}");
    out.push(detail);
    if !aggs.is_empty() {
        out.push(format!("{pad}  aggregations: {}", aggs.join(", ")));
    }
}

fn format_sort_node(
    node: &DistributedNode,
    sort: &DistributedSortNode,
    pad: &str,
    costs_suffix: &str,
    stats_suffix: &str,
    out: &mut Vec<String>,
) {
    let body = format_distributed_shared_plan_node_header(
        &physical_payload(node).expect("Sort is a physical explain node"),
        PlanNodeExplainStage::Distributed,
    )
    .expect("Sort is a shared explain node");
    let mut suffix = String::new();
    if let Some(limit) = sort.partition_limit {
        let topn_type = match sort.topn_type {
            Some(crate::exec::node::sort::SortTopNType::RowNumber) => "ROW_NUMBER",
            Some(crate::exec::node::sort::SortTopNType::Rank) => "RANK",
            Some(crate::exec::node::sort::SortTopNType::DenseRank) => "DENSE_RANK",
            None => "ROW_NUMBER",
        };
        suffix = format!(" partition_limit={limit} topn_type={topn_type}");
    }
    out.push(format!(
        "{pad}{}{body}{suffix}{costs_suffix}{stats_suffix}",
        node_prefix(node),
    ));
}

fn format_topn_node(
    node: &DistributedNode,
    topn: &PhysicalTopNNode,
    pad: &str,
    costs_suffix: &str,
    stats_suffix: &str,
    out: &mut Vec<String>,
) {
    let label = match topn.phase {
        TopNPhase::Partial => "LOCAL TOP-N",
        TopNPhase::Final => "TOP-N",
    };
    let items = format_sort_items(&topn.items);
    let mut parts = Vec::new();
    if let Some(l) = topn.limit {
        parts.push(format!("limit={l}"));
    }
    if let Some(o) = topn.offset {
        parts.push(format!("offset={o}"));
    }
    out.push(format!(
        "{pad}{}{label} ({}) [{}]{costs_suffix}{stats_suffix}",
        node_prefix(node),
        parts.join(", "),
        items.join(", ")
    ));
}

fn format_exchange_node(
    node: &DistributedNode,
    exchange: &ExchangeReceiver,
    pad: &str,
    costs_suffix: &str,
    stats_suffix: &str,
    out: &mut Vec<String>,
) {
    let label = exchange_label(exchange);
    out.push(format!(
        "{pad}{}{label}{costs_suffix}{stats_suffix}",
        node_prefix(node)
    ));
}

fn format_values_node(
    node: &DistributedNode,
    _values: &DistributedValuesNode,
    pad: &str,
    costs_suffix: &str,
    stats_suffix: &str,
    out: &mut Vec<String>,
) {
    let body = format_distributed_shared_plan_node_header(
        &physical_payload(node).expect("Values is a physical explain node"),
        PlanNodeExplainStage::Distributed,
    )
    .expect("Values is a shared explain node");
    out.push(format!(
        "{pad}{}{body}{costs_suffix}{stats_suffix}",
        node_prefix(node),
    ));
}

fn format_assert_one_row_node(
    node: &DistributedNode,
    _assert: &DistributedAssertOneRowNode,
    pad: &str,
    costs_suffix: &str,
    stats_suffix: &str,
    out: &mut Vec<String>,
) {
    let body = format_distributed_shared_plan_node_header(
        &physical_payload(node).expect("AssertOneRow is a physical explain node"),
        PlanNodeExplainStage::Distributed,
    )
    .expect("AssertOneRow is a shared explain node");
    out.push(format!(
        "{pad}{}{body}{costs_suffix}{stats_suffix}",
        node_prefix(node)
    ));
}

fn format_repeat_node(
    node: &DistributedNode,
    _repeat: &DistributedRepeatNode,
    pad: &str,
    costs_suffix: &str,
    stats_suffix: &str,
    out: &mut Vec<String>,
) {
    let body = format_distributed_shared_plan_node_header(
        &physical_payload(node).expect("Repeat is a physical explain node"),
        PlanNodeExplainStage::Distributed,
    )
    .expect("Repeat is a shared explain node");
    out.push(format!(
        "{pad}{}{body}{costs_suffix}{stats_suffix}",
        node_prefix(node),
    ));
}

fn format_change_event_expand_node(
    node: &DistributedNode,
    _expand: &DistributedChangeEventExpandNode,
    pad: &str,
    costs_suffix: &str,
    stats_suffix: &str,
    out: &mut Vec<String>,
) {
    let body = format_distributed_shared_plan_node_header(
        &physical_payload(node).expect("ChangeEventExpand is a physical explain node"),
        PlanNodeExplainStage::Distributed,
    )
    .expect("ChangeEventExpand is a shared explain node");
    out.push(format!(
        "{pad}{}{body}{costs_suffix}{stats_suffix}",
        node_prefix(node),
    ));
}

#[derive(Default)]
struct ColumnDisplayLookup {
    by_id: HashMap<ColumnId, (Option<String>, String)>,
}

fn column_display_lookup(node: &DistributedNode) -> ColumnDisplayLookup {
    let mut lookup = ColumnDisplayLookup::default();
    collect_column_displays(node, &mut lookup);
    lookup
}

fn collect_column_displays(node: &DistributedNode, lookup: &mut ColumnDisplayLookup) {
    if let DistributedNodeKind::Scan(scan) = &node.payload {
        for column in &scan.columns {
            let display = (scan.alias.clone(), column.name.clone());
            lookup
                .by_id
                .entry(column.column_id)
                .or_insert(display.clone());
        }
    }
    for child in &node.children {
        collect_column_displays(child, lookup);
    }
}

fn format_group_expr(expr: &TypedExpr, lookup: &ColumnDisplayLookup) -> String {
    let ExprKind::ColumnRef { column_id, .. } = &expr.kind else {
        return format_expr(expr);
    };

    let display = lookup.by_id.get(column_id);
    let Some((display_qualifier, display_column)) = display else {
        return format_expr(expr);
    };
    match display_qualifier {
        Some(qualifier) => format!("{qualifier}.{display_column}"),
        None => display_column.clone(),
    }
}

fn format_set_op_node(
    node: &DistributedNode,
    set_op: &PhysicalSetOpNode,
    pad: &str,
    costs_suffix: &str,
    stats_suffix: &str,
    out: &mut Vec<String>,
) {
    let label = match set_op.kind {
        SetOpKind::UnionAll => "UNION ALL",
        SetOpKind::UnionDistinct => {
            unreachable!(
                "{}",
                crate::sql::planner::distributed::build::union_distinct_must_be_rewritten_error()
            )
        }
        SetOpKind::Intersect => "INTERSECT",
        SetOpKind::Except => "EXCEPT",
    };
    out.push(format!(
        "{pad}{}{label}{costs_suffix}{stats_suffix}",
        node_prefix(node)
    ));
}

fn format_window_node(
    node: &DistributedNode,
    _window: &DistributedWindowNode,
    pad: &str,
    costs_suffix: &str,
    stats_suffix: &str,
    out: &mut Vec<String>,
) {
    let header = format_distributed_shared_plan_node_header(
        &physical_payload(node).expect("Window is a physical explain node"),
        PlanNodeExplainStage::Distributed,
    )
    .expect("Window is a shared explain node");
    out.push(format!(
        "{pad}{}{header}{costs_suffix}{stats_suffix}",
        node_prefix(node),
    ));
}

fn format_generate_series_node(
    node: &DistributedNode,
    _generate: &DistributedGenerateSeriesNode,
    pad: &str,
    costs_suffix: &str,
    stats_suffix: &str,
    out: &mut Vec<String>,
) {
    let body = format_distributed_shared_plan_node_header(
        &physical_payload(node).expect("GenerateSeries is a physical explain node"),
        PlanNodeExplainStage::Distributed,
    )
    .expect("GenerateSeries is a shared explain node");
    out.push(format!(
        "{pad}{}{body}{costs_suffix}{stats_suffix}",
        node_prefix(node),
    ));
}

fn format_table_function_node(
    node: &DistributedNode,
    _table_function: &DistributedTableFunctionNode,
    pad: &str,
    costs_suffix: &str,
    stats_suffix: &str,
    out: &mut Vec<String>,
) {
    let body = format_distributed_shared_plan_node_header(
        &physical_payload(node).expect("TableFunction is a physical explain node"),
        PlanNodeExplainStage::Distributed,
    )
    .expect("TableFunction is a shared explain node");
    out.push(format!(
        "{pad}{}{body}{costs_suffix}{stats_suffix}",
        node_prefix(node),
    ));
}

fn exchange_label(exchange: &ExchangeReceiver) -> String {
    match exchange.flavor {
        ExchangeFlavor::TopNSplit { .. } => "MERGING-EXCHANGE".to_string(),
        ExchangeFlavor::CteMulticast { .. } => "EXCHANGE".to_string(),
        ExchangeFlavor::Distribution | ExchangeFlavor::LimitOffset { .. } => {
            match exchange.partition.kind {
                PartitionKind::Hash => "HASH EXCHANGE".to_string(),
                PartitionKind::Unpartitioned => "GATHER".to_string(),
                PartitionKind::Random => "EXCHANGE".to_string(),
            }
        }
    }
}

fn join_kind_label(join_type: JoinKind) -> &'static str {
    match join_type {
        JoinKind::Inner => "INNER",
        JoinKind::LeftOuter => "LEFT OUTER",
        JoinKind::RightOuter => "RIGHT OUTER",
        JoinKind::FullOuter => "FULL OUTER",
        JoinKind::Cross => "CROSS",
        JoinKind::LeftSemi => "LEFT SEMI",
        JoinKind::RightSemi => "RIGHT SEMI",
        JoinKind::LeftAnti => "LEFT ANTI",
        JoinKind::RightAnti => "RIGHT ANTI",
        JoinKind::NullAwareLeftAnti => "NULL AWARE LEFT ANTI",
    }
}

fn join_distribution_label(
    execution_mode: Option<JoinExecutionMode>,
    fallback: &JoinDistribution,
) -> &'static str {
    match execution_mode {
        Some(JoinExecutionMode::Broadcast) => "BROADCAST",
        Some(JoinExecutionMode::Partitioned) => "PARTITIONED",
        Some(JoinExecutionMode::Colocate) => "COLOCATE",
        None => match fallback {
            JoinDistribution::Broadcast => "BROADCAST",
            JoinDistribution::Shuffle => "PARTITIONED",
            JoinDistribution::Colocate => "COLOCATE",
            JoinDistribution::Unknown => "UNKNOWN",
        },
    }
}

fn dict_actual_suffix(actual: &ActualMetrics) -> String {
    if actual.dict_input_rows == 0 && actual.dict_input_columns == 0 {
        return String::new();
    }
    format!(
        " dict={{in_rows={}, kept_rows={}, hydrated_rows={}, in_cols={}, kept_cols={}, hydrated_cols={}, unsupported_cols={}}}",
        actual.dict_input_rows,
        actual.dict_kept_rows,
        actual.dict_hydrated_rows,
        actual.dict_input_columns,
        actual.dict_kept_columns,
        actual.dict_hydrated_columns,
        actual.dict_unsupported_columns
    )
}

fn actual_suffix(node: &DistributedNode, actuals: Option<&HashMap<i32, ActualMetrics>>) -> String {
    match actuals.and_then(|actuals| actuals.get(&node.node_id)) {
        Some(metrics) => {
            let total_time_ns = metrics.total_time_ns.max(0);
            let total_time_max_ns = metrics.total_time_max_ns.max(0);
            let total_time_min_ns = metrics.total_time_min_ns.max(0);
            let mut s = format!(
                " act={{rows={} time={}",
                metrics.output_rows,
                fmt_time_ns(total_time_ns)
            );
            if total_time_max_ns > 0 {
                s.push_str(&format!(
                    " (max={} min={})",
                    fmt_time_ns(total_time_max_ns),
                    fmt_time_ns(total_time_min_ns)
                ));
            }
            if metrics.build_ht_ns > 0 {
                s.push_str(&format!(" build_ht={}", fmt_time_ns(metrics.build_ht_ns)));
            }
            if metrics.search_ns > 0 {
                s.push_str(&format!(" search={}", fmt_time_ns(metrics.search_ns)));
            }
            if metrics.out_build_ns > 0 {
                s.push_str(&format!(" out_build={}", fmt_time_ns(metrics.out_build_ns)));
            }
            if metrics.out_probe_ns > 0 {
                s.push_str(&format!(" out_probe={}", fmt_time_ns(metrics.out_probe_ns)));
            }
            s.push_str(&format!(" peak={}}}", fmt_bytes(metrics.peak_mem_bytes)));
            s.push_str(&dict_actual_suffix(metrics));
            s
        }
        None => String::new(),
    }
}

fn fmt_time_ns(ns: i64) -> String {
    let abs = ns.checked_abs().unwrap_or(i64::MAX);
    if abs < 1_000 {
        format!("{ns}ns")
    } else if abs < 1_000_000 {
        let us = ns as f64 / 1_000.0;
        if ns % 1_000 == 0 {
            format!("{us:.0}us")
        } else {
            format!("{us:.1}us")
        }
    } else if abs < 1_000_000_000 {
        format!("{:.1}ms", ns as f64 / 1_000_000.0)
    } else {
        format!("{:.1}s", ns as f64 / 1_000_000_000.0)
    }
}

fn fmt_bytes(bytes: i64) -> String {
    let abs = bytes.checked_abs().unwrap_or(i64::MAX);
    if abs < 1024 {
        format!("{bytes}B")
    } else if abs < 1024 * 1024 {
        format!("{:.1}KB", bytes as f64 / 1024.0)
    } else if abs < 1024 * 1024 * 1024 {
        format!("{:.1}MB", bytes as f64 / (1024.0 * 1024.0))
    } else {
        format!("{:.1}GB", bytes as f64 / (1024.0 * 1024.0 * 1024.0))
    }
}

fn fmt_bytes_f64(bytes: f64) -> String {
    const PB: f64 = 1024.0 * 1024.0 * 1024.0 * 1024.0 * 1024.0;

    if !bytes.is_finite() || bytes.abs() >= PB {
        return format!("{}B", fmt_f64(bytes));
    }
    let abs = bytes.abs();
    if abs < 1024.0 {
        format_scaled_bytes(bytes, "B")
    } else if abs < 1024.0 * 1024.0 {
        format_scaled_bytes(bytes / 1024.0, "KB")
    } else if abs < 1024.0 * 1024.0 * 1024.0 {
        format_scaled_bytes(bytes / (1024.0 * 1024.0), "MB")
    } else if abs < 1024.0 * 1024.0 * 1024.0 * 1024.0 {
        format_scaled_bytes(bytes / (1024.0 * 1024.0 * 1024.0), "GB")
    } else {
        format_scaled_bytes(bytes / (1024.0 * 1024.0 * 1024.0 * 1024.0), "TB")
    }
}

fn format_scaled_bytes(value: f64, unit: &str) -> String {
    if value.fract() == 0.0 {
        format!("{value:.0}{unit}")
    } else {
        format!("{value:.1}{unit}")
    }
}

fn is_detailed(level: ExplainLevel) -> bool {
    matches!(
        level,
        ExplainLevel::Verbose | ExplainLevel::Costs | ExplainLevel::Analyze
    )
}

fn costs_suffix(stats: &PhysicalPlanStats, level: ExplainLevel) -> String {
    if matches!(level, ExplainLevel::Costs) {
        let row_part = format!("rows={:.0}", stats.output_row_count);
        let cost_part = stats
            .cost_estimate
            .as_ref()
            .map(format_cost_estimate)
            .unwrap_or_default();
        let colstats = format_column_stats_costs(stats);
        match (cost_part.is_empty(), colstats.is_empty()) {
            (true, true) => format!(" ({row_part})"),
            (false, true) => format!(" ({row_part} {cost_part})"),
            (true, false) => format!(" ({row_part}) {colstats}"),
            (false, false) => format!(" ({row_part} {cost_part}) {colstats}"),
        }
    } else {
        String::new()
    }
}

fn bcast_verbose_suffix(
    decision: &Option<PlannerBroadcastDecision>,
    level: ExplainLevel,
) -> String {
    let d = match decision {
        Some(d) => d,
        None => return String::new(),
    };
    if !matches!(level, ExplainLevel::Verbose | ExplainLevel::Analyze) {
        return String::new();
    }
    let verdict = if d.feasible { "feasible" } else { "infeasible" };
    let forced = if d.forced { " bcast_forced=true" } else { "" };
    format!(" bcast_verdict={verdict}{forced}")
}

fn bcast_costs_suffix(decision: &Option<PlannerBroadcastDecision>, level: ExplainLevel) -> String {
    let d = match decision {
        Some(d) => d,
        None => return String::new(),
    };
    if !matches!(level, ExplainLevel::Costs) {
        return String::new();
    }
    let breakdown = format!(
        " bcast={{build={} ht={} be={} fanout={} budget={} risk_mult={:.1}x}}",
        fmt_bytes_f64(d.build_bytes),
        fmt_bytes_f64(d.hash_table_bytes),
        fmt_f64(d.effective_backend_count),
        fmt_bytes_f64(d.risk_adj_fanout_bytes),
        fmt_bytes_f64(d.per_node_budget_bytes),
        d.risk_multiplier,
    );
    let reject = match d.reject_reason.as_deref() {
        Some(reason) => format!(" bcast_reject=\"{reason}\""),
        None => String::new(),
    };
    format!("{breakdown}{reject}")
}

fn format_cost_estimate(cost: &PlannerCostEstimate) -> String {
    let total = cost.cpu_cost * DEFAULT_CPU_COST_WEIGHT
        + cost.memory_cost * DEFAULT_MEMORY_COST_WEIGHT
        + cost.network_cost * DEFAULT_NETWORK_COST_WEIGHT;
    format!(
        "cost={{cpu={} memory={} network={} total={}}}",
        fmt_f64(cost.cpu_cost),
        fmt_f64(cost.memory_cost),
        fmt_f64(cost.network_cost),
        fmt_f64(total),
    )
}

fn stats_suffix(stats: &PhysicalPlanStats, level: ExplainLevel) -> String {
    if is_detailed(level) {
        let trailer = format_stats_trailer(
            stats,
            matches!(level, ExplainLevel::Costs | ExplainLevel::Analyze),
        );
        format!(" {trailer}")
    } else {
        String::new()
    }
}

fn format_stats_trailer(stats: &PhysicalPlanStats, show_conf: bool) -> String {
    let rows = stats.output_row_count;
    let rows_str = if rows.is_nan() || rows <= 0.0 {
        "?".to_string()
    } else if rows.is_infinite() || rows >= MAX_ROW_COUNT {
        ">=1e15".to_string()
    } else {
        (rows.round() as i64).to_string()
    };
    let conf = if show_conf {
        match stats.row_count_confidence {
            PlannerConfidence::Estimated => " conf=estimated",
            PlannerConfidence::Fallback => " conf=fallback",
            PlannerConfidence::Exact | PlannerConfidence::Measured => "",
        }
    } else {
        ""
    };
    format!("stats={{rows={rows_str}{conf}}}")
}

fn format_column_stats_costs(stats: &PhysicalPlanStats) -> String {
    if stats.column_statistics.is_empty() {
        return String::new();
    }
    let mut ids: Vec<_> = stats.column_statistics.keys().copied().collect();
    ids.sort_by_key(|id| id.0);
    let parts = ids
        .into_iter()
        .map(|column_id| {
            let c = &stats.column_statistics[&column_id];
            let ndv = if let Some(ndv) = c.ndv.filter(|ndv| ndv.is_finite()) {
                fmt_f64(ndv.round())
            } else {
                "?".to_string()
            };
            format!(
                "col#{}[min={} max={} ndv={ndv} null_frac={}]",
                column_id.0,
                fmt_f64(c.min_value),
                fmt_f64(c.max_value),
                fmt_f64(c.nulls_fraction),
            )
        })
        .collect::<Vec<_>>();
    format!("colstats={{{}}}", parts.join(", "))
}

fn fmt_f64(v: f64) -> String {
    const SAFE_INTEGER_DISPLAY_LIMIT: f64 = 9_007_199_254_740_992.0;

    if v.is_nan() {
        "?".to_string()
    } else if v.is_infinite() {
        if v > 0.0 {
            "+inf".to_string()
        } else {
            "-inf".to_string()
        }
    } else if v.abs() > SAFE_INTEGER_DISPLAY_LIMIT {
        format!("{v:.4e}")
    } else if v.fract() == 0.0 {
        format!("{}", v as i64)
    } else {
        format!("{v:.4}")
    }
}

#[derive(Default)]
struct LocalScanExplainHints {
    has_min_max_stats: bool,
}

fn explain_hints_for_scan(scan: &DistributedScanNode) -> LocalScanExplainHints {
    let Some(required_columns) = scan.required_columns.as_ref() else {
        return LocalScanExplainHints::default();
    };
    if required_columns.is_empty() {
        return LocalScanExplainHints::default();
    }

    LocalScanExplainHints {
        has_min_max_stats: scan_supports_min_max_stats(&scan.table, required_columns),
    }
}

fn scan_pruned_type_lines(scan: &DistributedScanNode, required_columns: &[String]) -> Vec<String> {
    required_columns
        .iter()
        .filter_map(|required| {
            let (slot, data_type) = scan_required_column_type(scan, required)?;
            if !is_complex_type(data_type) {
                return None;
            }
            Some(format!(
                "Pruned type: {slot} <-> [{}]",
                format_scan_pruned_type(data_type, true)
            ))
        })
        .collect()
}

fn scan_variant_column_line(scan: &DistributedScanNode) -> Option<String> {
    if scan.variant_columns.is_empty() {
        return None;
    }
    let columns = scan
        .variant_columns
        .iter()
        .map(format_scan_variant_column)
        .collect::<Vec<_>>();
    Some(format!("variant columns: {}", columns.join(", ")))
}

fn format_scan_variant_column(col: &ScanVariantColumn) -> String {
    let function_name = if col.strict {
        "variant_get"
    } else {
        "try_variant_get"
    };
    format!(
        "{} := {function_name}({}, '{}', '{}')",
        col.synthetic_column,
        col.source_column,
        col.canonical_path,
        format_variant_requested_type(&col.requested_type)
    )
}

fn format_variant_requested_type(data_type: &DataType) -> &'static str {
    match data_type {
        DataType::Boolean => "boolean",
        DataType::Int64 => "bigint",
        DataType::Float64 => "double",
        DataType::Utf8 => "string",
        DataType::Date32 => "date",
        _ => "unsupported",
    }
}

fn scan_required_column_type<'a>(
    scan: &'a DistributedScanNode,
    required: &str,
) -> Option<(usize, &'a DataType)> {
    let table_pos = scan
        .table
        .columns
        .iter()
        .position(|column| column.name.eq_ignore_ascii_case(required))?;
    let data_type = scan
        .columns
        .iter()
        .find(|column| column.name.eq_ignore_ascii_case(required))
        .map(|column| &column.data_type)
        .or_else(|| {
            scan.table
                .columns
                .get(table_pos)
                .map(|column| &column.data_type)
        })?;
    Some((table_pos + 1, data_type))
}

fn is_complex_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::List(_)
            | DataType::LargeList(_)
            | DataType::FixedSizeList(_, _)
            | DataType::Map(_, _)
            | DataType::Struct(_)
    )
}

fn format_scan_pruned_type(data_type: &DataType, top_level: bool) -> String {
    match data_type {
        DataType::Null => "null_type".to_string(),
        DataType::Boolean => "boolean".to_string(),
        DataType::Int8 => "tinyint".to_string(),
        DataType::Int16 => "smallint".to_string(),
        DataType::Int32 => "int".to_string(),
        DataType::Int64 => "bigint".to_string(),
        DataType::UInt8 => "tinyint unsigned".to_string(),
        DataType::UInt16 => "smallint unsigned".to_string(),
        DataType::UInt32 => "int unsigned".to_string(),
        DataType::UInt64 => "bigint unsigned".to_string(),
        DataType::Float32 => "float".to_string(),
        DataType::Float64 => "double".to_string(),
        DataType::Utf8 | DataType::LargeUtf8 => "varchar(1073741824)".to_string(),
        DataType::Binary | DataType::LargeBinary => "varbinary".to_string(),
        DataType::Date32 => "date".to_string(),
        DataType::Timestamp(_, _) => "datetime".to_string(),
        DataType::Time32(_) | DataType::Time64(_) => "time".to_string(),
        DataType::Decimal128(precision, scale) | DataType::Decimal256(precision, scale) => {
            format!("decimal({precision},{scale})")
        }
        DataType::List(field) | DataType::LargeList(field) => {
            let inner = format_scan_pruned_type(field.data_type(), false);
            if top_level {
                format!("ARRAY<{inner}>")
            } else {
                format!("array<{inner}>")
            }
        }
        DataType::FixedSizeList(field, _) => {
            let inner = format_scan_pruned_type(field.data_type(), false);
            if top_level {
                format!("ARRAY<{inner}>")
            } else {
                format!("array<{inner}>")
            }
        }
        DataType::Map(entries, _) => {
            let DataType::Struct(fields) = entries.data_type() else {
                return "map<unknown,unknown>".to_string();
            };
            if fields.len() != 2 {
                return "map<unknown,unknown>".to_string();
            }
            format!(
                "map<{},{}>",
                format_scan_pruned_type(fields[0].data_type(), false),
                format_scan_pruned_type(fields[1].data_type(), false)
            )
        }
        DataType::Struct(fields) => {
            let fields = fields
                .iter()
                .map(|field| {
                    format!(
                        "`{}` {}",
                        field.name(),
                        format_scan_pruned_type(field.data_type(), false)
                    )
                })
                .collect::<Vec<_>>();
            format!("struct<{}>", fields.join(", "))
        }
        other => format!("{other:?}").to_lowercase(),
    }
}

fn scan_supports_min_max_stats(table: &TableDef, required_columns: &[String]) -> bool {
    match &table.source {
        ScanSource::IcebergDataFiles { .. } | ScanSource::StarRocks { .. } => {}
        ScanSource::IcebergMetadataTable { .. } => return false,
        ScanSource::IcebergDeltaTable { .. }
        | ScanSource::IcebergVersionTable { .. }
        | ScanSource::IcebergMvTargetState { .. }
        | ScanSource::IcebergMvTargetLocator { .. } => return false,
    }
    required_columns.iter().all(|required| {
        table
            .columns
            .iter()
            .find(|column| column.name.eq_ignore_ascii_case(required))
            .map(|column| supports_scan_min_max_stats(&column.data_type))
            .unwrap_or(false)
    })
}

fn supports_scan_min_max_stats(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float32
            | DataType::Float64
            | DataType::Decimal128(_, _)
            | DataType::Date32
            | DataType::Timestamp(_, _)
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Binary
            | DataType::LargeBinary
            | DataType::FixedSizeBinary(_)
    )
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow::datatypes::DataType;

    use super::{
        explain_distributed_plan_analyze, explain_fragment_order,
        format_distributed_shared_plan_node_header,
    };
    use crate::coordinator::profile::correlate::{ActualMetrics, DistributedProfileSummary};
    use crate::exec::node::sort::SortTopNType;
    use crate::sql::analysis::{
        ExprKind, JoinKind, OutputColumn, ProjectItem, SortItem, TypedExpr,
    };
    use crate::sql::column_id::ColumnId;
    use crate::sql::explain::distributed::explain_distributed_plan;
    use crate::sql::explain::{ExplainLevel, PlanNodeExplainStage};
    use crate::sql::optimizer::operator::{
        AggMode, AggregateOutputLayout, JoinDistribution, Operator, PhysicalDistributionOp,
        PhysicalHashAggregateOp, PhysicalHashJoinEqCondition, PhysicalHashJoinOp, ProjectOp,
        ScanOp, TopNOp, TopNPhase as OptimizerTopNPhase, ValuesOp,
    };
    use crate::sql::optimizer::optimized_tree::{
        JoinExecutionDistribution, OptimizedOperatorNode, PlanExecutionProps, attach_scalar_arena,
    };
    use crate::sql::optimizer::property::DistributionSpec;
    use crate::sql::optimizer::scalar::ScalarArena;
    use crate::sql::optimizer::statistics::{
        ColumnStatistic, Confidence, CostEstimate, Statistics,
    };
    use crate::sql::planner::distributed::{
        DistributedNode, DistributedNodeKind, ExchangeFlavor, ExchangeReceiver,
    };
    use crate::sql::planner::optimizer_bridge::scalar::{
        intern_aggregate_calls, intern_exprs, intern_project_items, intern_sort_items,
    };
    use crate::sql::planner::payload::AggregateCall;
    use crate::sql::planner::physical::{
        PhysicalPlanStats, PlannerBroadcastDecision, PlannerConfidence,
    };
    use crate::sql::planner::table::{ScanSource, TableDef};
    use novarocks_catalog::schema::ColumnDef;

    fn prepare_bridge2_test_props(node: &mut OptimizedOperatorNode) {
        for child in &mut node.children {
            prepare_bridge2_test_props(child);
        }
        if node.explain_stats.cost_estimate.is_none() {
            node.explain_stats.cost_estimate = Some(CostEstimate {
                cpu_cost: 1.0,
                memory_cost: 2.0,
                network_cost: 3.0,
            });
        }
        if let Operator::PhysicalHashJoin(join) = &node.op {
            node.execution_props.join_distribution =
                join_execution_distribution_for_test(join.distribution.clone());
            if node.explain_stats.broadcast_decision.is_none()
                && matches!(join.distribution, JoinDistribution::Broadcast)
            {
                node.explain_stats.broadcast_decision = Some(test_broadcast_decision());
            }
        }
    }

    fn join_execution_distribution_for_test(
        distribution: JoinDistribution,
    ) -> Option<JoinExecutionDistribution> {
        match distribution {
            JoinDistribution::Broadcast => Some(JoinExecutionDistribution::Broadcast),
            JoinDistribution::Shuffle => Some(JoinExecutionDistribution::Partitioned),
            JoinDistribution::Colocate => Some(JoinExecutionDistribution::Colocate),
            JoinDistribution::Unknown => None,
        }
    }

    fn test_broadcast_decision() -> crate::sql::optimizer::cost::BroadcastDecision {
        crate::sql::optimizer::cost::BroadcastDecision {
            feasible: true,
            forced: false,
            build_bytes: 16.0 * 1024.0 * 1024.0,
            hash_table_bytes: 53_300_000.0,
            effective_backend_count: 10.0,
            risk_adj_fanout_bytes: 144.0 * 1024.0 * 1024.0,
            per_node_budget_bytes: 256.0 * 1024.0 * 1024.0,
            cluster_network_budget_bytes: 2560.0 * 1024.0 * 1024.0,
            risk_multiplier: 1.0,
            reject_reason: None,
        }
    }

    #[test]
    fn normal_scan_project_agg_renders_node_id_prefixes() {
        let mut optimized_tree = aggregate_count_plan(project_plan(scan_plan()));
        prepare_bridge2_test_props(&mut optimized_tree);
        let physical_plan =
            crate::sql::planner::optimizer_bridge::to_physical_plan(&optimized_tree)
                .expect("convert optimizer physical plan");
        let distributed_plan = crate::sql::planner::pipeline::build_distributed_plan(physical_plan)
            .expect("build DistributedPlan");

        let text = explain_distributed_plan(&distributed_plan, ExplainLevel::Normal).join("\n");

        assert!(!text.contains("PLAN FRAGMENT"));
        assert!(text.contains("3:HASH AGGREGATE"));
        assert!(text.contains("2:PROJECT"));
        assert!(text.contains("1:SCAN"));
    }

    #[test]
    fn shared_distributed_formatter_path_covers_scan_and_project() {
        let mut optimized_tree = project_plan(scan_plan());
        prepare_bridge2_test_props(&mut optimized_tree);
        let physical_plan =
            crate::sql::planner::optimizer_bridge::to_physical_plan(&optimized_tree)
                .expect("convert optimizer physical plan");
        let distributed_plan = crate::sql::planner::pipeline::build_distributed_plan(physical_plan)
            .expect("build DistributedPlan");
        let root = &distributed_plan
            .fragments()
            .iter()
            .find(|fragment| fragment.fragment_id == distributed_plan.root_fragment_id())
            .expect("root fragment")
            .root;

        let root_kind =
            crate::sql::planner::distributed::distributed_kind_to_physical(&root.payload);
        assert_eq!(
            format_distributed_shared_plan_node_header(
                &root_kind,
                PlanNodeExplainStage::Distributed
            ),
            Some("PROJECT [t.k AS k]".to_string())
        );
        let scan_kind = crate::sql::planner::distributed::distributed_kind_to_physical(
            &root.children[0].payload,
        );
        assert_eq!(
            format_distributed_shared_plan_node_header(
                &scan_kind,
                PlanNodeExplainStage::Distributed
            ),
            Some("SCAN test_db.t (alias=t)".to_string())
        );

        let text = explain_distributed_plan(&distributed_plan, ExplainLevel::Normal).join("\n");
        assert!(
            text.contains("2:PROJECT [t.k AS k]"),
            "missing shared PROJECT header in distributed explain:\n{text}"
        );
        assert!(
            text.contains("1:SCAN test_db.t (alias=t)"),
            "missing shared SCAN header in distributed explain:\n{text}"
        );
    }

    #[test]
    fn distributed_explain_accepts_exchange_only_in_distributed_kind() {
        let node = DistributedNode {
            node_id: 7,
            fragment_id: 1,
            tuple_ids: vec![],
            nullable_tuple_ids: vec![],
            limit: -1,
            runtime_filter_binding_ids: Vec::new(),
            children: vec![],
            stats: PhysicalPlanStats {
                output_row_count: 0.0,
                row_count_confidence: PlannerConfidence::Fallback,
                column_statistics: HashMap::new(),
                cost_estimate: None,
                broadcast_decision: None,
            },
            payload: DistributedNodeKind::Exchange(ExchangeReceiver {
                partition: crate::sql::planner::distributed::DataPartition::unpartitioned(),
                source_fragment_id: 0,
                output_columns: vec![],
                output_qualifier: None,
                flavor: ExchangeFlavor::Distribution,
            }),
        };

        assert_eq!(super::physical_kind_name(&node.payload), "Exchange");
    }

    #[test]
    fn verbose_shuffle_agg_renders_fragments_and_exchange() {
        let mut optimized_tree = aggregate_count_plan(distribution_plan(
            scan_plan(),
            DistributionSpec::shuffle_agg([ColumnId::new_for_test(1)]),
        ));
        prepare_bridge2_test_props(&mut optimized_tree);
        let physical_plan =
            crate::sql::planner::optimizer_bridge::to_physical_plan(&optimized_tree)
                .expect("convert optimizer physical plan");
        let distributed_plan = crate::sql::planner::pipeline::build_distributed_plan(physical_plan)
            .expect("build DistributedPlan");

        let text = explain_distributed_plan(&distributed_plan, ExplainLevel::Verbose).join("\n");

        assert!(text.contains("PLAN FRAGMENT 0"));
        assert!(text.contains("PLAN FRAGMENT 1"));
        assert!(text.contains("EXCHANGE"));
        assert!(text.contains("HASH_PARTITIONED"));
    }

    #[test]
    fn sort_with_partition_limit_renders_topn_suffix() {
        let mut optimized_tree = sort_with_partition_limit_plan(scan_plan());
        prepare_bridge2_test_props(&mut optimized_tree);
        let physical_plan =
            crate::sql::planner::optimizer_bridge::to_physical_plan(&optimized_tree)
                .expect("convert optimizer physical plan");
        let distributed_plan = crate::sql::planner::pipeline::build_distributed_plan(physical_plan)
            .expect("build DistributedPlan");

        let text = explain_distributed_plan(&distributed_plan, ExplainLevel::Normal).join("\n");

        assert!(
            text.contains("2:SORT BY [t.k ASC NULLS LAST] partition_limit=3 topn_type=RANK"),
            "expected ranking-window sort suffix in IR explain output:\n{text}"
        );
    }

    #[test]
    fn costs_renders_colstats_from_ir_stats_only_at_costs_level() {
        let mut optimized_tree = scan_plan();
        optimized_tree.stats.column_statistics.insert(
            ColumnId::new_for_test(1),
            ColumnStatistic {
                min_value: 0.0,
                max_value: 1000.0,
                nulls_fraction: 0.0,
                average_row_size: 8.0,
                ..ColumnStatistic::for_test_with_ndv(1000.0, Confidence::Exact)
            },
        );
        prepare_bridge2_test_props(&mut optimized_tree);
        let physical_plan =
            crate::sql::planner::optimizer_bridge::to_physical_plan(&optimized_tree)
                .expect("convert optimizer physical plan");
        let distributed_plan = crate::sql::planner::pipeline::build_distributed_plan(physical_plan)
            .expect("build DistributedPlan");

        let normal = explain_distributed_plan(&distributed_plan, ExplainLevel::Normal).join("\n");
        let verbose = explain_distributed_plan(&distributed_plan, ExplainLevel::Verbose).join("\n");
        let costs = explain_distributed_plan(&distributed_plan, ExplainLevel::Costs).join("\n");

        assert!(
            !normal.contains("colstats="),
            "Normal must hide colstats:\n{normal}"
        );
        assert!(
            !verbose.contains("colstats="),
            "Verbose must hide colstats:\n{verbose}"
        );
        assert!(
            costs.contains("colstats={col#1[min=0 max=1000 ndv=1000 null_frac=0]}"),
            "Costs must render colstats copied into PhysicalPlanStats:\n{costs}"
        );
    }

    #[test]
    fn costs_colstats_ndv_uses_scientific_not_i64_saturation_for_huge_values() {
        let mut optimized_tree = scan_plan();
        optimized_tree.stats.column_statistics.insert(
            ColumnId::new_for_test(1),
            ColumnStatistic {
                min_value: 0.0,
                max_value: 1.0e300,
                nulls_fraction: 0.0,
                average_row_size: 8.0,
                ..ColumnStatistic::for_test_with_ndv(1.0e300, Confidence::Exact)
            },
        );
        prepare_bridge2_test_props(&mut optimized_tree);
        let physical_plan =
            crate::sql::planner::optimizer_bridge::to_physical_plan(&optimized_tree)
                .expect("convert optimizer physical plan");
        let distributed_plan = crate::sql::planner::pipeline::build_distributed_plan(physical_plan)
            .expect("build DistributedPlan");

        let costs = explain_distributed_plan(&distributed_plan, ExplainLevel::Costs).join("\n");
        assert!(
            costs.contains("ndv=1.0000e300"),
            "huge NDV should use scientific notation:\n{costs}"
        );
        assert!(
            !costs.contains("9223372036854775807"),
            "huge NDV must not saturate through i64 formatting:\n{costs}"
        );
    }

    #[test]
    fn costs_level_renders_dimensional_costs() {
        let mut optimized_tree = scan_plan();
        prepare_bridge2_test_props(&mut optimized_tree);
        let physical_plan =
            crate::sql::planner::optimizer_bridge::to_physical_plan(&optimized_tree)
                .expect("convert optimizer physical plan");
        let distributed_plan = crate::sql::planner::pipeline::build_distributed_plan(physical_plan)
            .expect("build DistributedPlan");

        let costs = explain_distributed_plan(&distributed_plan, ExplainLevel::Costs).join("\n");
        assert!(costs.contains("1:SCAN test_db.t (alias=t) (rows=3 cost={cpu="));
        assert!(costs.contains("memory="));
        assert!(costs.contains("network="));
        assert!(costs.contains("total="));
    }

    #[test]
    fn fmt_f64_uses_scientific_not_i64_saturation_for_huge_values() {
        let text = super::fmt_f64(1.0e300);

        assert!(
            text.contains('e') || text.contains('E'),
            "huge finite values should use scientific notation, got {text}"
        );
        assert!(
            !text.contains("9223372036854775807"),
            "huge finite values must not saturate through i64 formatting"
        );
    }

    fn decision(feasible: bool, forced: bool, reject: Option<&str>) -> PlannerBroadcastDecision {
        PlannerBroadcastDecision {
            feasible,
            forced,
            build_bytes: 16.0 * 1024.0 * 1024.0,
            hash_table_bytes: 53_300_000.0,
            effective_backend_count: 10.0,
            risk_adj_fanout_bytes: 144.0 * 1024.0 * 1024.0,
            per_node_budget_bytes: 256.0 * 1024.0 * 1024.0,
            cluster_network_budget_bytes: 2560.0 * 1024.0 * 1024.0,
            risk_multiplier: 1.0,
            reject_reason: reject.map(str::to_string),
        }
    }

    #[test]
    fn verbose_shows_verdict_token_normal_and_costs_do_not() {
        let d = decision(true, false, None);
        assert_eq!(
            super::bcast_verbose_suffix(&Some(d.clone()), ExplainLevel::Verbose),
            " bcast_verdict=feasible"
        );
        assert_eq!(
            super::bcast_verbose_suffix(&Some(d.clone()), ExplainLevel::Analyze),
            " bcast_verdict=feasible"
        );
        assert_eq!(
            super::bcast_verbose_suffix(&Some(d.clone()), ExplainLevel::Normal),
            ""
        );
        assert_eq!(
            super::bcast_verbose_suffix(&Some(d), ExplainLevel::Costs),
            ""
        );
    }

    #[test]
    fn forced_infeasible_shows_forced_token() {
        let d = decision(
            false,
            true,
            Some("risk_adj_build 203.3MB > node_budget 256MB"),
        );
        let s = super::bcast_verbose_suffix(&Some(d), ExplainLevel::Verbose);
        assert!(s.contains("bcast_verdict=infeasible"));
        assert!(s.contains("bcast_forced=true"));
    }

    #[test]
    fn costs_shows_breakdown_verbose_does_not() {
        let d = decision(true, false, None);
        assert!(
            super::bcast_costs_suffix(&Some(d.clone()), ExplainLevel::Costs)
                .contains("bcast={build=")
        );
        assert_eq!(
            super::bcast_costs_suffix(&Some(d.clone()), ExplainLevel::Verbose),
            ""
        );
        assert_eq!(
            super::bcast_costs_suffix(&Some(d), ExplainLevel::Analyze),
            ""
        );
    }

    #[test]
    fn costs_reject_reason_rendered() {
        let d = decision(
            false,
            false,
            Some("risk_adj_build 203.3MB > node_budget 256MB"),
        );
        let s = super::bcast_costs_suffix(&Some(d), ExplainLevel::Costs);
        assert!(s.contains("bcast_reject=\"risk_adj_build"));
    }

    #[test]
    fn costs_reject_reason_covers_network_and_uninformative_size() {
        let network = decision(
            false,
            false,
            Some("risk_adj_fanout 144MB > network_budget 2.5GB"),
        );
        let network_text = super::bcast_costs_suffix(&Some(network), ExplainLevel::Costs);
        assert!(network_text.contains("bcast_reject=\"risk_adj_fanout"));
        assert!(network_text.contains("network_budget"));

        let unknown = decision(
            false,
            false,
            Some("build size unknown (fallback default stats) -> partitioned"),
        );
        let unknown_text = super::bcast_costs_suffix(&Some(unknown), ExplainLevel::Costs);
        assert!(unknown_text.contains("build size unknown"));
    }

    #[test]
    fn costs_byte_formatting_keeps_huge_and_nonfinite_values_visible() {
        let mut huge = decision(false, false, Some("risk_adj_build huge > node_budget inf"));
        huge.hash_table_bytes = 1.0e300;
        huge.risk_multiplier = 4.0;
        huge.per_node_budget_bytes = f64::INFINITY;
        let huge_text = super::bcast_costs_suffix(&Some(huge), ExplainLevel::Costs);
        assert!(huge_text.contains("1.0000e300B"));
        assert!(huge_text.contains("+infB"));
        assert!(!huge_text.contains("9223372036854775807"));

        let mut fractional = decision(true, false, None);
        fractional.build_bytes = 1536.0;
        let fractional_text = super::bcast_costs_suffix(&Some(fractional), ExplainLevel::Costs);
        assert!(fractional_text.contains("build=1.5KB"));
    }

    #[test]
    fn hash_join_line_places_broadcast_tokens_at_the_expected_levels() {
        let mut optimized_tree = inner_join_two_values();
        prepare_bridge2_test_props(&mut optimized_tree);
        let physical_plan =
            crate::sql::planner::optimizer_bridge::to_physical_plan(&optimized_tree)
                .expect("convert optimizer physical plan");
        let distributed_plan = crate::sql::planner::pipeline::build_distributed_plan(physical_plan)
            .expect("build DistributedPlan");

        let normal = explain_distributed_plan(&distributed_plan, ExplainLevel::Normal).join("\n");
        assert!(!normal.contains("bcast_"));
        assert!(!normal.contains(" bcast={"));

        let verbose = explain_distributed_plan(&distributed_plan, ExplainLevel::Verbose).join("\n");
        let verbose_line = verbose
            .lines()
            .find(|line| line.contains("HASH JOIN"))
            .expect("verbose hash join line");
        assert!(verbose_line.contains("bcast_verdict="));
        assert!(!verbose_line.contains(" bcast={"));
        assert!(
            verbose_line.find("bcast_verdict").expect("verdict")
                < verbose_line.find("stats={").expect("stats"),
            "verdict should render before stats trailer: {verbose_line}"
        );

        let costs = explain_distributed_plan(&distributed_plan, ExplainLevel::Costs).join("\n");
        let costs_line = costs
            .lines()
            .find(|line| line.contains("HASH JOIN"))
            .expect("costs hash join line");
        assert!(costs_line.contains("cost={"));
        assert!(costs_line.contains(" bcast={"));
        assert!(!costs_line.contains("bcast_verdict"));
        assert!(
            costs_line.find("cost={").expect("cost") < costs_line.find(" bcast={").expect("bcast")
                && costs_line.find(" bcast={").expect("bcast")
                    < costs_line.find("stats={").expect("stats"),
            "Costs HASH JOIN line should render cost, bcast, then stats: {costs_line}"
        );
    }

    #[test]
    fn detailed_ir_explain_shows_graph_channels_and_bindings_but_normal_hides_them() {
        let mut optimized_tree = inner_join_two_values();
        prepare_bridge2_test_props(&mut optimized_tree);
        let physical_plan =
            crate::sql::planner::optimizer_bridge::to_physical_plan(&optimized_tree)
                .expect("convert optimizer physical plan");
        let distributed_plan = crate::sql::planner::pipeline::build_distributed_plan(physical_plan)
            .expect("build DistributedPlan");

        for level in [
            ExplainLevel::Verbose,
            ExplainLevel::Costs,
            ExplainLevel::Analyze,
        ] {
            let text = explain_distributed_plan(&distributed_plan, level).join("\n");
            assert!(
                text.contains("RUNTIME FILTER GRAPH"),
                "missing graph header at {level:?}:\n{text}"
            );
            assert!(
                text.contains("domain = Membership(")
                    && text.contains("target = JoinBuildKey(ordinal=0)")
                    && text.contains("activation = BlockingSnapshot"),
                "missing stable Join runtime-filter contract at {level:?}:\n{text}"
            );
            assert!(
                text.contains("producer binding:") && text.contains("consumer binding:"),
                "missing graph bindings at {level:?}:\n{text}"
            );
        }

        let normal = explain_distributed_plan(&distributed_plan, ExplainLevel::Normal).join("\n");
        assert!(
            !normal.contains("RUNTIME FILTER GRAPH"),
            "Normal must hide RF graph lines:\n{normal}"
        );
    }

    #[test]
    fn detailed_ir_explain_shows_aggregate_topn_runtime_filter_contract() {
        let mut optimized_tree = aggregate_topn_runtime_filter_plan();
        prepare_bridge2_test_props(&mut optimized_tree);
        let physical_plan =
            crate::sql::planner::optimizer_bridge::to_physical_plan(&optimized_tree)
                .expect("convert eligible Aggregate TopN plan");
        let distributed_plan = crate::sql::planner::pipeline::build_distributed_plan(physical_plan)
            .expect("build sealed Aggregate TopN graph");

        for level in [
            ExplainLevel::Verbose,
            ExplainLevel::Costs,
            ExplainLevel::Analyze,
        ] {
            let text = explain_distributed_plan(&distributed_plan, level).join("\n");
            for expected in [
                "RUNTIME FILTER GRAPH",
                "domain = OrderedBound(key=Int32 ASC NULLS LAST, inclusive=true)",
                "target = AggregateTopNKey(group_key_ordinal=0, limit=2)",
                "activation = NonBlockingLive(Batch)",
            ] {
                assert!(
                    text.contains(expected),
                    "missing {expected:?} at {level:?}:\n{text}"
                );
            }
            for forbidden in [
                "ComparatorDigest",
                "coverage_witness",
                "route",
                "participant",
                "runtime filter channel 0",
                "binding 0",
                "fragment =",
                "node =",
            ] {
                assert!(
                    !text.contains(forbidden),
                    "unstable graph detail {forbidden:?} leaked at {level:?}:\n{text}"
                );
            }
        }

        let normal = explain_distributed_plan(&distributed_plan, ExplainLevel::Normal).join("\n");
        assert!(
            !normal.contains("RUNTIME FILTER GRAPH"),
            "Normal must hide RF graph lines:\n{normal}"
        );

        let mut join = inner_join_two_values();
        prepare_bridge2_test_props(&mut join);
        let join_physical = crate::sql::planner::optimizer_bridge::to_physical_plan(&join)
            .expect("convert join plan");
        let join_distributed = crate::sql::planner::pipeline::build_distributed_plan(join_physical)
            .expect("build sealed join graph");
        let join_text =
            explain_distributed_plan(&join_distributed, ExplainLevel::Verbose).join("\n");
        assert!(
            join_text.contains("producer binding") && join_text.contains("consumer binding"),
            "Membership/Join bindings must remain explainable:\n{join_text}"
        );
    }

    #[test]
    fn analyze_renders_actuals_for_nodes_present_in_map_only() {
        let mut optimized_tree = aggregate_count_plan(project_plan(scan_plan()));
        prepare_bridge2_test_props(&mut optimized_tree);
        let physical_plan =
            crate::sql::planner::optimizer_bridge::to_physical_plan(&optimized_tree)
                .expect("convert optimizer physical plan");
        let distributed_plan = crate::sql::planner::pipeline::build_distributed_plan(physical_plan)
            .expect("build DistributedPlan");
        let mut actuals = HashMap::new();
        actuals.insert(
            1,
            ActualMetrics {
                output_rows: 11,
                total_time_ns: 450_000,
                peak_mem_bytes: 64,
                ..ActualMetrics::default()
            },
        );
        actuals.insert(
            3,
            ActualMetrics {
                output_rows: 7,
                total_time_ns: 2_300_000,
                peak_mem_bytes: 4 * 1024 * 1024,
                ..ActualMetrics::default()
            },
        );

        let text = explain_distributed_plan_analyze(
            &distributed_plan,
            ExplainLevel::Analyze,
            &actuals,
            None,
        )
        .join("\n");

        assert!(
            text.contains("3:HASH AGGREGATE (SINGLE, group by: [t.k]) stats={rows=3 conf=fallback} act={rows=7 time=2.3ms peak=4.0MB}"),
            "expected aggregate actuals after estimate trailer:\n{text}"
        );
        assert!(
            text.contains("1:SCAN test_db.t (alias=t) stats={rows=3 conf=fallback} act={rows=11 time=450us peak=64B}"),
            "expected scan actuals after estimate trailer:\n{text}"
        );
        assert!(
            text.contains("2:PROJECT [t.k AS k] stats={rows=3 conf=fallback}"),
            "expected project estimate trailer:\n{text}"
        );
        assert!(
            !text.contains("2:PROJECT [t.k AS k] stats={rows=3 conf=fallback} act="),
            "nodes absent from the actuals map must not print act=:\n{text}"
        );
        assert!(
            !text.contains("dict={"),
            "zero dictionary counters must not render dictionary actuals:\n{text}"
        );
    }

    #[test]
    fn analyze_renders_per_fragment_profile_under_fragment_header() {
        let mut optimized_tree = aggregate_count_plan(project_plan(scan_plan()));
        prepare_bridge2_test_props(&mut optimized_tree);
        let physical_plan =
            crate::sql::planner::optimizer_bridge::to_physical_plan(&optimized_tree)
                .expect("convert optimizer physical plan");
        let distributed_plan = crate::sql::planner::pipeline::build_distributed_plan(physical_plan)
            .expect("build DistributedPlan");
        let actuals = HashMap::new();
        // Key the per-fragment map by the first fragment's root node id, the same key the renderer
        // applies (`fragment.root.node_id`) — so the test never hardcodes node ids.
        let frags = explain_fragment_order(&distributed_plan);
        let rep = frags[0].root.node_id;
        let mut per_fragment = HashMap::new();
        per_fragment.insert(
            rep,
            DistributedProfileSummary {
                operator_active_time_ns: 44_800_000_000,
                driver_blocked_time_ns: 120_000_000,
                ..DistributedProfileSummary::default()
            },
        );

        let text = explain_distributed_plan_analyze(
            &distributed_plan,
            ExplainLevel::Analyze,
            &actuals,
            Some(&per_fragment),
        )
        .join("\n");

        assert!(text.contains("PLAN FRAGMENT 0"), "{text}");
        assert!(
            text.contains("Profile: active=44.8s blocked=120.0ms"),
            "{text}"
        );
        // Without the per-fragment map, no Profile line is rendered.
        let plain = explain_distributed_plan_analyze(
            &distributed_plan,
            ExplainLevel::Analyze,
            &actuals,
            None,
        )
        .join("\n");
        assert!(!plain.contains("Profile: active="), "{plain}");
    }

    #[test]
    fn actual_suffix_renders_phase_timers_and_minmax() {
        let mut optimized_tree = aggregate_count_plan(project_plan(scan_plan()));
        prepare_bridge2_test_props(&mut optimized_tree);
        let physical_plan =
            crate::sql::planner::optimizer_bridge::to_physical_plan(&optimized_tree)
                .expect("convert optimizer physical plan");
        let distributed_plan = crate::sql::planner::pipeline::build_distributed_plan(physical_plan)
            .expect("build DistributedPlan");
        let mut actuals = HashMap::new();
        actuals.insert(
            1,
            ActualMetrics {
                output_rows: 13_502_430,
                total_time_ns: 44_800_000_000,
                peak_mem_bytes: 637_000_000,
                total_time_max_ns: 46_000_000_000,
                total_time_min_ns: 43_000_000_000,
                build_ht_ns: 0,
                search_ns: 20_000_000_000,
                out_build_ns: 6_000_000_000,
                out_probe_ns: 12_000_000_000,
                ..ActualMetrics::default()
            },
        );

        let text = explain_distributed_plan_analyze(
            &distributed_plan,
            ExplainLevel::Analyze,
            &actuals,
            None,
        )
        .join("\n");

        assert!(
            text.contains(
                "act={rows=13502430 time=44.8s (max=46.0s min=43.0s) search=20.0s out_build=6.0s out_probe=12.0s peak=607.5MB}"
            ),
            "expected scan actuals to include phase timers and per-driver min/max in order:\n{text}"
        );
        assert!(
            !text.contains("build_ht="),
            "zero build hash-table timer must not render:\n{text}"
        );
    }

    #[test]
    fn actual_suffix_renders_dictionary_metrics_when_present() {
        let mut optimized_tree = aggregate_count_plan(project_plan(scan_plan()));
        prepare_bridge2_test_props(&mut optimized_tree);
        let physical_plan =
            crate::sql::planner::optimizer_bridge::to_physical_plan(&optimized_tree)
                .expect("convert optimizer physical plan");
        let distributed_plan = crate::sql::planner::pipeline::build_distributed_plan(physical_plan)
            .expect("build DistributedPlan");
        let mut actuals = HashMap::new();
        actuals.insert(
            1,
            ActualMetrics {
                output_rows: 8,
                total_time_ns: 900_000,
                peak_mem_bytes: 128,
                dict_input_rows: 8,
                dict_input_columns: 2,
                dict_kept_rows: 4,
                dict_kept_columns: 1,
                dict_hydrated_rows: 4,
                dict_hydrated_columns: 1,
                dict_unsupported_columns: 1,
                ..ActualMetrics::default()
            },
        );

        let text = explain_distributed_plan_analyze(
            &distributed_plan,
            ExplainLevel::Analyze,
            &actuals,
            None,
        )
        .join("\n");

        assert!(
            text.contains(
                "dict={in_rows=8, kept_rows=4, hydrated_rows=4, in_cols=2, kept_cols=1, hydrated_cols=1, unsupported_cols=1}"
            ),
            "expected dictionary carrier actuals in analyzed output:\n{text}"
        );
    }

    #[test]
    fn actual_suffix_clamps_negative_min_time() {
        let mut optimized_tree = aggregate_count_plan(project_plan(scan_plan()));
        prepare_bridge2_test_props(&mut optimized_tree);
        let physical_plan =
            crate::sql::planner::optimizer_bridge::to_physical_plan(&optimized_tree)
                .expect("convert optimizer physical plan");
        let distributed_plan = crate::sql::planner::pipeline::build_distributed_plan(physical_plan)
            .expect("build DistributedPlan");
        let mut actuals = HashMap::new();
        actuals.insert(
            1,
            ActualMetrics {
                output_rows: 1,
                total_time_ns: 10_000,
                peak_mem_bytes: 0,
                total_time_max_ns: 20_000,
                total_time_min_ns: -5_000,
                ..ActualMetrics::default()
            },
        );

        let text = explain_distributed_plan_analyze(
            &distributed_plan,
            ExplainLevel::Analyze,
            &actuals,
            None,
        )
        .join("\n");

        assert!(
            text.contains("min=0ns"),
            "negative min time must clamp to zero when rendered:\n{text}"
        );
        assert!(
            !text.contains("min=-"),
            "negative min time must not be rendered:\n{text}"
        );
    }

    #[test]
    fn analyze_without_actuals_matches_existing_explain() {
        let mut optimized_tree = aggregate_count_plan(project_plan(scan_plan()));
        prepare_bridge2_test_props(&mut optimized_tree);
        let physical_plan =
            crate::sql::planner::optimizer_bridge::to_physical_plan(&optimized_tree)
                .expect("convert optimizer physical plan");
        let distributed_plan = crate::sql::planner::pipeline::build_distributed_plan(physical_plan)
            .expect("build DistributedPlan");
        let actuals = HashMap::new();

        assert_eq!(
            explain_distributed_plan(&distributed_plan, ExplainLevel::Analyze),
            explain_distributed_plan_analyze(
                &distributed_plan,
                ExplainLevel::Analyze,
                &actuals,
                None,
            )
        );
    }

    #[test]
    fn aggregate_group_display_does_not_match_descendant_scan_by_name_only() {
        let mut optimized_tree = aggregate_count_on_projected_id_plan(
            project_alias_collision_plan(alias_collision_scan_plan()),
        );
        prepare_bridge2_test_props(&mut optimized_tree);
        let physical_plan =
            crate::sql::planner::optimizer_bridge::to_physical_plan(&optimized_tree)
                .expect("convert optimizer physical plan");
        let distributed_plan = crate::sql::planner::pipeline::build_distributed_plan(physical_plan)
            .expect("build DistributedPlan");

        let text = explain_distributed_plan(&distributed_plan, ExplainLevel::Normal).join("\n");

        assert!(
            text.contains("3:HASH AGGREGATE (SINGLE, group by: [id])"),
            "expected projected group key name to remain unqualified:\n{text}"
        );
        assert!(
            !text.contains("group by: [base.id]"),
            "group key display must not fall back to a same-name descendant scan column:\n{text}"
        );
    }

    fn scan_plan() -> OptimizedOperatorNode {
        scan_plan_with_key_type(DataType::Int64)
    }

    fn scan_plan_with_key_type(key_type: DataType) -> OptimizedOperatorNode {
        let k = output_col(1, "k", key_type.clone(), false);
        let v = output_col(2, "v", DataType::Int64, true);
        physical_node(
            Operator::PhysicalScan(ScanOp {
                database: "test_db".to_string(),
                table: TableDef {
                    name: "t".to_string(),
                    columns: vec![
                        column_def("k", key_type, false),
                        column_def("v", DataType::Int64, true),
                    ],
                    iceberg_row_lineage_metadata_columns: vec![],
                    source: ScanSource::StarRocks {
                        db_id: 1,
                        table_id: 2,
                    },
                },
                alias: Some("t".to_string()),
                stats_ref: None,
                columns: vec![k.clone(), v.clone()],
                predicates: vec![],
                required_columns: Some(vec!["k".to_string(), "v".to_string()]),
                variant_columns: vec![],
                mv_rewritten_from: None,
            }),
            vec![],
            vec![k, v],
        )
    }

    fn alias_collision_scan_plan() -> OptimizedOperatorNode {
        let id = output_col(1, "id", DataType::Int64, false);
        let v = output_col(2, "v", DataType::Int64, true);
        physical_node(
            Operator::PhysicalScan(ScanOp {
                database: "test_db".to_string(),
                table: TableDef {
                    name: "t".to_string(),
                    columns: vec![
                        column_def("id", DataType::Int64, false),
                        column_def("v", DataType::Int64, true),
                    ],
                    iceberg_row_lineage_metadata_columns: vec![],
                    source: ScanSource::StarRocks {
                        db_id: 1,
                        table_id: 2,
                    },
                },
                alias: Some("base".to_string()),
                stats_ref: None,
                columns: vec![id.clone(), v.clone()],
                predicates: vec![],
                required_columns: Some(vec!["id".to_string(), "v".to_string()]),
                variant_columns: vec![],
                mv_rewritten_from: None,
            }),
            vec![],
            vec![id, v],
        )
    }

    fn project_alias_collision_plan(child: OptimizedOperatorNode) -> OptimizedOperatorNode {
        let mut scalars = scalars_from_children(std::slice::from_ref(&child));
        let output_columns = vec![output_col(3, "id", DataType::Int64, true)];
        let items = vec![ProjectItem {
            expr: column_ref_expr_with_qualifier(2, "base", "v", DataType::Int64, true),
            output_name: "id".to_string(),
            output_column_id: ColumnId::new_for_test(3),
        }];
        physical_node_with_scalars(
            Operator::PhysicalProject(ProjectOp {
                items: intern_project_items(&mut scalars, &items),
                output_qualifier: None,
            }),
            vec![child],
            output_columns,
            scalars,
        )
    }

    fn project_plan(child: OptimizedOperatorNode) -> OptimizedOperatorNode {
        let mut scalars = scalars_from_children(std::slice::from_ref(&child));
        let output_columns = vec![output_col(1, "k", DataType::Int64, false)];
        let items = vec![ProjectItem {
            expr: column_ref_expr(1, "k", DataType::Int64, false),
            output_name: "k".to_string(),
            output_column_id: ColumnId::new_for_test(1),
        }];
        physical_node_with_scalars(
            Operator::PhysicalProject(ProjectOp {
                items: intern_project_items(&mut scalars, &items),
                output_qualifier: None,
            }),
            vec![child],
            output_columns,
            scalars,
        )
    }

    fn aggregate_count_plan(child: OptimizedOperatorNode) -> OptimizedOperatorNode {
        let mut scalars = scalars_from_children(std::slice::from_ref(&child));
        let k = output_col(1, "k", DataType::Int64, false);
        let count = output_col(3, "count(*)", DataType::Int64, true);
        let aggregate_calls = vec![AggregateCall {
            name: "count".to_string(),
            args: vec![],
            distinct: false,
            result_type: DataType::Int64,
            order_by: vec![],
            output_column_id: ColumnId::new_for_test(3),
        }];
        physical_node_with_scalars(
            Operator::PhysicalHashAggregate(PhysicalHashAggregateOp {
                mode: AggMode::Single,
                group_by: intern_exprs(
                    &mut scalars,
                    &[column_ref_expr(1, "k", DataType::Int64, false)],
                ),
                aggregates: intern_aggregate_calls(&mut scalars, &aggregate_calls),
                output_layout: AggregateOutputLayout::new(vec![k.clone()], vec![count.clone()]),
                output_columns: vec![k.clone(), count.clone()],
                is_merge: vec![false],
            }),
            vec![child],
            vec![k, count],
            scalars,
        )
    }

    fn aggregate_topn_runtime_filter_plan() -> OptimizedOperatorNode {
        let scan = scan_plan_with_key_type(DataType::Int32);
        let mut aggregate_scalars = scalars_from_children(std::slice::from_ref(&scan));
        let key = output_col(1, "k", DataType::Int32, false);
        let key_expr = column_ref_expr(1, "k", DataType::Int32, false);
        let aggregate = physical_node_with_scalars(
            Operator::PhysicalHashAggregate(PhysicalHashAggregateOp {
                mode: AggMode::Local,
                group_by: intern_exprs(&mut aggregate_scalars, std::slice::from_ref(&key_expr)),
                aggregates: vec![],
                output_layout: AggregateOutputLayout::new(vec![key.clone()], vec![]),
                output_columns: vec![key.clone()],
                is_merge: vec![],
            }),
            vec![scan],
            vec![key.clone()],
            aggregate_scalars,
        );
        let mut topn_scalars = scalars_from_children(std::slice::from_ref(&aggregate));
        physical_node_with_scalars(
            Operator::PhysicalTopN(TopNOp {
                items: intern_sort_items(
                    &mut topn_scalars,
                    &[SortItem {
                        expr: key_expr,
                        asc: true,
                        nulls_first: false,
                    }],
                ),
                limit: Some(2),
                offset: Some(0),
                phase: OptimizerTopNPhase::Partial,
                is_split: true,
            }),
            vec![aggregate],
            vec![key],
            topn_scalars,
        )
    }

    fn aggregate_count_on_projected_id_plan(child: OptimizedOperatorNode) -> OptimizedOperatorNode {
        let mut scalars = scalars_from_children(std::slice::from_ref(&child));
        let id = output_col(3, "id", DataType::Int64, true);
        let count = output_col(4, "count(*)", DataType::Int64, true);
        let aggregate_calls = vec![AggregateCall {
            name: "count".to_string(),
            args: vec![],
            distinct: false,
            result_type: DataType::Int64,
            order_by: vec![],
            output_column_id: ColumnId::new_for_test(4),
        }];
        physical_node_with_scalars(
            Operator::PhysicalHashAggregate(PhysicalHashAggregateOp {
                mode: AggMode::Single,
                group_by: intern_exprs(
                    &mut scalars,
                    &[TypedExpr {
                        kind: ExprKind::ColumnRef {
                            column_id: ColumnId::new_for_test(3),
                            qualifier: None,
                            column: "id".to_string(),
                        },
                        data_type: DataType::Int64,
                        nullable: true,
                    }],
                ),
                aggregates: intern_aggregate_calls(&mut scalars, &aggregate_calls),
                output_layout: AggregateOutputLayout::new(vec![id.clone()], vec![count.clone()]),
                output_columns: vec![id.clone(), count.clone()],
                is_merge: vec![false],
            }),
            vec![child],
            vec![id, count],
            scalars,
        )
    }

    fn sort_with_partition_limit_plan(child: OptimizedOperatorNode) -> OptimizedOperatorNode {
        let mut scalars = scalars_from_children(std::slice::from_ref(&child));
        let output_columns = child.output_columns.clone();
        physical_node_with_scalars(
            Operator::PhysicalSort(crate::sql::optimizer::operator::SortOp {
                items: intern_sort_items(
                    &mut scalars,
                    &[SortItem {
                        expr: column_ref_expr(1, "k", DataType::Int64, false),
                        asc: true,
                        nulls_first: false,
                    }],
                ),
                analytic_partition_exprs: vec![],
                partition_limit: Some(3),
                topn_type: Some(SortTopNType::Rank),
            }),
            vec![child],
            output_columns,
            scalars,
        )
    }

    fn distribution_plan(
        child: OptimizedOperatorNode,
        spec: DistributionSpec,
    ) -> OptimizedOperatorNode {
        let output_columns = child.output_columns.clone();
        physical_node(
            Operator::PhysicalDistribution(PhysicalDistributionOp { spec }),
            vec![child],
            output_columns,
        )
    }

    fn inner_join_two_values() -> OptimizedOperatorNode {
        let left_col = output_col(1, "left_key", DataType::Int64, false);
        let right_col = output_col(2, "right_key", DataType::Int64, false);
        let left = physical_node(
            Operator::PhysicalValues(ValuesOp {
                rows: vec![],
                columns: vec![left_col.clone()],
            }),
            vec![],
            vec![left_col.clone()],
        );
        let right = physical_node(
            Operator::PhysicalValues(ValuesOp {
                rows: vec![],
                columns: vec![right_col.clone()],
            }),
            vec![],
            vec![right_col.clone()],
        );
        let mut scalars = scalars_from_children(&[left.clone(), right.clone()]);
        let left_key = intern_exprs(
            &mut scalars,
            &[column_ref_expr(1, "left_key", DataType::Int64, false)],
        )[0];
        let right_key = intern_exprs(
            &mut scalars,
            &[column_ref_expr(2, "right_key", DataType::Int64, false)],
        )[0];
        physical_node_with_scalars(
            Operator::PhysicalHashJoin(PhysicalHashJoinOp {
                join_type: JoinKind::Inner,
                eq_conditions: vec![PhysicalHashJoinEqCondition {
                    left: left_key,
                    right: right_key,
                    null_safe: false,
                }],
                other_condition: None,
                distribution: JoinDistribution::Broadcast,
            }),
            vec![left, right],
            vec![left_col, right_col],
            scalars,
        )
    }

    fn physical_node(
        op: Operator,
        children: Vec<OptimizedOperatorNode>,
        output_columns: Vec<OutputColumn>,
    ) -> OptimizedOperatorNode {
        let scalars = scalars_from_children(&children);
        physical_node_with_scalars(op, children, output_columns, scalars)
    }

    fn physical_node_with_scalars(
        op: Operator,
        children: Vec<OptimizedOperatorNode>,
        output_columns: Vec<OutputColumn>,
        scalars: ScalarArena,
    ) -> OptimizedOperatorNode {
        let mut plan = OptimizedOperatorNode {
            op,
            children,
            stats: Statistics {
                output_row_count: 3.0,
                ..Default::default()
            },
            explain_stats: crate::sql::optimizer::optimized_tree::OptimizerExplainStats::default(),
            output_columns,
            execution_props: PlanExecutionProps::default(),
        };
        attach_scalar_arena(&mut plan, Arc::new(scalars));
        plan
    }

    fn scalars_from_children(children: &[OptimizedOperatorNode]) -> ScalarArena {
        children
            .iter()
            .find_map(|child| child.execution_props.scalar_arena.as_deref().cloned())
            .unwrap_or_else(ScalarArena::new)
    }

    fn table_def() -> TableDef {
        TableDef {
            name: "t".to_string(),
            columns: vec![
                column_def("k", DataType::Int64, false),
                column_def("v", DataType::Int64, true),
            ],
            iceberg_row_lineage_metadata_columns: vec![],
            source: ScanSource::StarRocks {
                db_id: 1,
                table_id: 2,
            },
        }
    }

    fn column_def(name: &str, data_type: DataType, nullable: bool) -> ColumnDef {
        ColumnDef {
            name: name.to_string(),
            data_type,
            nullable,
            write_default: None,
            logical_type: None,
        }
    }

    fn output_col(id: u32, name: &str, data_type: DataType, nullable: bool) -> OutputColumn {
        OutputColumn {
            column_id: ColumnId::new_for_test(id),
            name: name.to_string(),
            data_type,
            nullable,
            is_internal: false,
        }
    }

    fn column_ref_expr(id: u32, column: &str, data_type: DataType, nullable: bool) -> TypedExpr {
        column_ref_expr_with_qualifier(id, "t", column, data_type, nullable)
    }

    fn column_ref_expr_with_qualifier(
        id: u32,
        qualifier: &str,
        column: &str,
        data_type: DataType,
        nullable: bool,
    ) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: ColumnId::new_for_test(id),
                qualifier: Some(qualifier.to_string()),
                column: column.to_string(),
            },
            data_type,
            nullable,
        }
    }
}
