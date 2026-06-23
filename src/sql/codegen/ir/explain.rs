use std::{collections::HashMap, fmt::Write};

use arrow::datatypes::DataType;

use crate::partitions;
use crate::runtime::profile_correlate::ActualMetrics;
use crate::sql::analysis::{ExprKind, JoinKind, TypedExpr};
use crate::sql::catalog::{ScanSource, TableDef};
use crate::sql::codegen::scalar_materialize::materialize;
use crate::sql::column_id::ColumnId;
use crate::sql::explain::{
    ExplainLevel, PlanNodeExplainStage, format_expr, format_shared_plan_node_detail_lines,
    format_shared_plan_node_header, format_sort_items,
};
use crate::sql::optimizer::estimate::arith::MAX_ROW_COUNT;
use crate::sql::optimizer::operator::{AggMode, JoinDistribution, TopNPhase};
use crate::sql::optimizer::physical_plan::JoinExecutionDistribution;
use crate::sql::optimizer::runtime_filter_pass::{RuntimeFilterDesc, RuntimeFilterProbe};
use crate::sql::optimizer::scalar::ScalarArena;
use crate::sql::planner::plan::{
    DistributedExchangeNode, DistributedHashAggregateNode, DistributedHashJoinNode,
    DistributedNestLoopJoinNode, DistributedSetOpNode, DistributedTopNNode, ExchangeFlavor,
    PlanAssertOneRowNode as DistributedAssertOneRowNode, PlanDecodeNode as DistributedDecodeNode,
    PlanFilterNode as DistributedFilterNode,
    PlanGenerateSeriesNode as DistributedGenerateSeriesNode, PlanNodeKind,
    PlanProjectNode as DistributedProjectNode, PlanRepeatNode as DistributedRepeatNode,
    PlanScanNode as DistributedScanNode, PlanSetOpKind as SetOpKind,
    PlanSortNode as DistributedSortNode, PlanTableFunctionNode as DistributedTableFunctionNode,
    PlanValuesNode as DistributedValuesNode, PlanWindowNode as DistributedWindowNode,
    ScanVariantColumn,
};
use crate::sql::planner::{DistributedPlan, DistributedPlanNode, PlanFragment, PlanNodeStats};

pub(crate) fn explain_distributed_plan(dp: &DistributedPlan, level: ExplainLevel) -> Vec<String> {
    explain_distributed_plan_inner(dp, level, None)
}

pub(crate) fn explain_distributed_plan_analyze(
    dp: &DistributedPlan,
    level: ExplainLevel,
    actuals: &HashMap<i32, ActualMetrics>,
) -> Vec<String> {
    explain_distributed_plan_inner(dp, level, Some(actuals))
}

fn explain_distributed_plan_inner(
    dp: &DistributedPlan,
    level: ExplainLevel,
    actuals: Option<&HashMap<i32, ActualMetrics>>,
) -> Vec<String> {
    let mut out = Vec::new();
    let fragments = explain_fragment_order(dp);
    let detailed = is_detailed(level);

    for (display_id, fragment) in fragments.iter().enumerate() {
        if detailed {
            out.push(format!("PLAN FRAGMENT {display_id}"));
            out.push(format!(
                "  OUTPUT EXPRS: {}",
                format_output_exprs(fragment.output_exprs.as_deref())
            ));
            out.push(format!(
                "  PARTITION: {}",
                fragment.data_partition.explain_label()
            ));
            if fragment.fragment_id != dp.root_fragment_id {
                let source_edges = dp
                    .edges
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

        format_distributed_node(
            &fragment.root,
            dp.scalar_arena.as_ref(),
            level,
            0,
            actuals,
            &mut out,
        );
    }

    out
}

fn explain_fragment_order(dp: &DistributedPlan) -> Vec<&PlanFragment> {
    let mut ordered = Vec::with_capacity(dp.fragments.len());
    if let Some(root) = dp
        .fragments
        .iter()
        .find(|fragment| fragment.fragment_id == dp.root_fragment_id)
    {
        ordered.push(root);
    }
    ordered.extend(
        dp.fragments
            .iter()
            .rev()
            .filter(|fragment| fragment.fragment_id != dp.root_fragment_id),
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

fn format_distributed_node(
    node: &DistributedPlanNode,
    arena: &ScalarArena,
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

    match &node.kind {
        PlanNodeKind::Scan(scan) => {
            format_scan_node(
                node,
                scan,
                arena,
                level,
                &pad,
                &costs_suffix,
                &stats_suffix,
                out,
            );
        }
        PlanNodeKind::Project(project) => {
            format_project_node(node, project, &pad, &costs_suffix, &stats_suffix, out);
            push_probe_rf_lines(&node.probe_runtime_filters, arena, level, &pad, out);
            format_children(node, arena, level, indent, actuals, out);
        }
        PlanNodeKind::Filter(filter) => {
            format_filter_node(node, filter, &pad, &costs_suffix, &stats_suffix, out);
            push_probe_rf_lines(&node.probe_runtime_filters, arena, level, &pad, out);
            format_children(node, arena, level, indent, actuals, out);
        }
        PlanNodeKind::HashJoin(join) => {
            format_hash_join_node(
                node,
                join,
                arena,
                level,
                &pad,
                &costs_suffix,
                &stats_suffix,
                out,
            );
            format_children(node, arena, level, indent, actuals, out);
        }
        PlanNodeKind::NestLoopJoin(join) => {
            format_nest_loop_join_node(node, join, &pad, &costs_suffix, &stats_suffix, out);
            format_children(node, arena, level, indent, actuals, out);
        }
        PlanNodeKind::HashAggregate(agg) => {
            format_hash_aggregate_node(node, agg, &pad, &costs_suffix, &stats_suffix, out);
            push_probe_rf_lines(&node.probe_runtime_filters, arena, level, &pad, out);
            format_children(node, arena, level, indent, actuals, out);
        }
        PlanNodeKind::Sort(sort) => {
            format_sort_node(node, sort, &pad, &costs_suffix, &stats_suffix, out);
            format_children(node, arena, level, indent, actuals, out);
        }
        PlanNodeKind::TopN(topn) => {
            format_topn_node(node, topn, &pad, &costs_suffix, &stats_suffix, out);
            format_children(node, arena, level, indent, actuals, out);
        }
        PlanNodeKind::Exchange(exchange) => {
            format_exchange_node(node, exchange, &pad, &costs_suffix, &stats_suffix, out);
        }
        PlanNodeKind::Values(values) => {
            format_values_node(node, values, &pad, &costs_suffix, &stats_suffix, out);
            push_probe_rf_lines(&node.probe_runtime_filters, arena, level, &pad, out);
        }
        PlanNodeKind::AssertOneRow(assert) => {
            format_assert_one_row_node(node, assert, &pad, &costs_suffix, &stats_suffix, out);
            format_children(node, arena, level, indent, actuals, out);
        }
        PlanNodeKind::Decode(decode) => {
            format_decode_node(node, decode, &pad, &costs_suffix, &stats_suffix, out);
            format_children(node, arena, level, indent, actuals, out);
        }
        PlanNodeKind::Repeat(repeat) => {
            format_repeat_node(node, repeat, &pad, &costs_suffix, &stats_suffix, out);
            format_children(node, arena, level, indent, actuals, out);
        }
        PlanNodeKind::SetOp(set_op) => {
            format_set_op_node(node, set_op, &pad, &costs_suffix, &stats_suffix, out);
            format_children(node, arena, level, indent, actuals, out);
        }
        PlanNodeKind::Window(window) => {
            format_window_node(node, window, &pad, &costs_suffix, &stats_suffix, out);
            format_children(node, arena, level, indent, actuals, out);
        }
        PlanNodeKind::GenerateSeries(generate) => {
            format_generate_series_node(node, generate, &pad, &costs_suffix, &stats_suffix, out);
        }
        PlanNodeKind::TableFunction(table_function) => {
            format_table_function_node(
                node,
                table_function,
                &pad,
                &costs_suffix,
                &stats_suffix,
                out,
            );
            format_children(node, arena, level, indent, actuals, out);
        }
        PlanNodeKind::Limit(_)
        | PlanNodeKind::Aggregate(_)
        | PlanNodeKind::Join(_)
        | PlanNodeKind::Union(_)
        | PlanNodeKind::Intersect(_)
        | PlanNodeKind::Except(_)
        | PlanNodeKind::CTEAnchor(_)
        | PlanNodeKind::CTEProduce(_)
        | PlanNodeKind::CTEConsume(_)
        | PlanNodeKind::AggregateStateMerge(_)
        | PlanNodeKind::Apply(_)
        | PlanNodeKind::ImvDelta(_)
        | PlanNodeKind::ImvVersion(_) => {
            panic!(
                "logical plan node {} leaked into distributed explain",
                node.kind.variant_name()
            );
        }
    }
}

fn format_children(
    node: &DistributedPlanNode,
    arena: &ScalarArena,
    level: ExplainLevel,
    indent: usize,
    actuals: Option<&HashMap<i32, ActualMetrics>>,
    out: &mut Vec<String>,
) {
    for child in &node.children {
        format_distributed_node(child, arena, level, indent + 1, actuals, out);
    }
}

fn node_prefix(node: &DistributedPlanNode) -> String {
    format!("{}:", node.node_id)
}

fn format_scan_node(
    node: &DistributedPlanNode,
    scan: &DistributedScanNode,
    arena: &ScalarArena,
    level: ExplainLevel,
    pad: &str,
    costs_suffix: &str,
    stats_suffix: &str,
    out: &mut Vec<String>,
) {
    let header = format_shared_plan_node_header(&node.kind, PlanNodeExplainStage::Distributed)
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
    if matches!(level, ExplainLevel::Costs) && local_hints.has_decode {
        out.push(format!("{pad}     Decode"));
    }
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
    push_probe_rf_lines(&node.probe_runtime_filters, arena, level, pad, out);
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
    node: &DistributedPlanNode,
    _project: &DistributedProjectNode,
    pad: &str,
    costs_suffix: &str,
    stats_suffix: &str,
    out: &mut Vec<String>,
) {
    let header = format_shared_plan_node_header(&node.kind, PlanNodeExplainStage::Distributed)
        .expect("Project is a shared explain node");
    out.push(format!(
        "{pad}{}{header}{costs_suffix}{stats_suffix}",
        node_prefix(node),
    ));
}

fn format_filter_node(
    node: &DistributedPlanNode,
    _filter: &DistributedFilterNode,
    pad: &str,
    costs_suffix: &str,
    stats_suffix: &str,
    out: &mut Vec<String>,
) {
    let header = format_shared_plan_node_header(&node.kind, PlanNodeExplainStage::Distributed)
        .expect("Filter is a shared explain node");
    out.push(format!(
        "{pad}{}{header}{costs_suffix}{stats_suffix}",
        node_prefix(node)
    ));
    for line in format_shared_plan_node_detail_lines(&node.kind, PlanNodeExplainStage::Distributed)
    {
        out.push(format!("{pad}  {line}"));
    }
}

fn format_hash_join_node(
    node: &DistributedPlanNode,
    join: &DistributedHashJoinNode,
    arena: &ScalarArena,
    level: ExplainLevel,
    pad: &str,
    costs_suffix: &str,
    stats_suffix: &str,
    out: &mut Vec<String>,
) {
    let dist = join_distribution_label(node.execution_join_distribution, &join.distribution);
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
    out.push(format!(
        "{pad}{}HASH JOIN ({dist}, {join_str}, eq: [{}]){costs_suffix}{stats_suffix}",
        node_prefix(node),
        eq.join(", ")
    ));
    if let Some(ref other) = join.other_condition {
        out.push(format!("{pad}  other: {}", format_expr(other)));
    }
    push_build_rf_lines(&node.build_runtime_filters, arena, level, pad, out);
}

fn format_nest_loop_join_node(
    node: &DistributedPlanNode,
    join: &DistributedNestLoopJoinNode,
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
    node: &DistributedPlanNode,
    agg: &DistributedHashAggregateNode,
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
    node: &DistributedPlanNode,
    sort: &DistributedSortNode,
    pad: &str,
    costs_suffix: &str,
    stats_suffix: &str,
    out: &mut Vec<String>,
) {
    let body = format_shared_plan_node_header(&node.kind, PlanNodeExplainStage::Distributed)
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
    node: &DistributedPlanNode,
    topn: &DistributedTopNNode,
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
    node: &DistributedPlanNode,
    exchange: &DistributedExchangeNode,
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
    node: &DistributedPlanNode,
    _values: &DistributedValuesNode,
    pad: &str,
    costs_suffix: &str,
    stats_suffix: &str,
    out: &mut Vec<String>,
) {
    let body = format_shared_plan_node_header(&node.kind, PlanNodeExplainStage::Distributed)
        .expect("Values is a shared explain node");
    out.push(format!(
        "{pad}{}{body}{costs_suffix}{stats_suffix}",
        node_prefix(node),
    ));
}

fn format_assert_one_row_node(
    node: &DistributedPlanNode,
    _assert: &DistributedAssertOneRowNode,
    pad: &str,
    costs_suffix: &str,
    stats_suffix: &str,
    out: &mut Vec<String>,
) {
    let body = format_shared_plan_node_header(&node.kind, PlanNodeExplainStage::Distributed)
        .expect("AssertOneRow is a shared explain node");
    out.push(format!(
        "{pad}{}{body}{costs_suffix}{stats_suffix}",
        node_prefix(node)
    ));
}

fn format_decode_node(
    node: &DistributedPlanNode,
    _decode: &DistributedDecodeNode,
    pad: &str,
    costs_suffix: &str,
    stats_suffix: &str,
    out: &mut Vec<String>,
) {
    let body = format_shared_plan_node_header(&node.kind, PlanNodeExplainStage::Distributed)
        .expect("Decode is a shared explain node");
    out.push(format!(
        "{pad}{}{body}{costs_suffix}{stats_suffix}",
        node_prefix(node),
    ));
}

fn format_repeat_node(
    node: &DistributedPlanNode,
    _repeat: &DistributedRepeatNode,
    pad: &str,
    costs_suffix: &str,
    stats_suffix: &str,
    out: &mut Vec<String>,
) {
    let body = format_shared_plan_node_header(&node.kind, PlanNodeExplainStage::Distributed)
        .expect("Repeat is a shared explain node");
    out.push(format!(
        "{pad}{}{body}{costs_suffix}{stats_suffix}",
        node_prefix(node),
    ));
}

#[derive(Default)]
struct ColumnDisplayLookup {
    by_id: HashMap<ColumnId, (Option<String>, String)>,
}

fn column_display_lookup(node: &DistributedPlanNode) -> ColumnDisplayLookup {
    let mut lookup = ColumnDisplayLookup::default();
    collect_column_displays(node, &mut lookup);
    lookup
}

fn collect_column_displays(node: &DistributedPlanNode, lookup: &mut ColumnDisplayLookup) {
    if let PlanNodeKind::Scan(scan) = &node.kind {
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
    node: &DistributedPlanNode,
    set_op: &DistributedSetOpNode,
    pad: &str,
    costs_suffix: &str,
    stats_suffix: &str,
    out: &mut Vec<String>,
) {
    let label = match set_op.kind {
        SetOpKind::UnionAll => "UNION ALL",
        SetOpKind::Intersect => "INTERSECT",
        SetOpKind::Except => "EXCEPT",
    };
    out.push(format!(
        "{pad}{}{label}{costs_suffix}{stats_suffix}",
        node_prefix(node)
    ));
}

fn format_window_node(
    node: &DistributedPlanNode,
    _window: &DistributedWindowNode,
    pad: &str,
    costs_suffix: &str,
    stats_suffix: &str,
    out: &mut Vec<String>,
) {
    let header = format_shared_plan_node_header(&node.kind, PlanNodeExplainStage::Distributed)
        .expect("Window is a shared explain node");
    out.push(format!(
        "{pad}{}{header}{costs_suffix}{stats_suffix}",
        node_prefix(node),
    ));
}

fn format_generate_series_node(
    node: &DistributedPlanNode,
    _generate: &DistributedGenerateSeriesNode,
    pad: &str,
    costs_suffix: &str,
    stats_suffix: &str,
    out: &mut Vec<String>,
) {
    let body = format_shared_plan_node_header(&node.kind, PlanNodeExplainStage::Distributed)
        .expect("GenerateSeries is a shared explain node");
    out.push(format!(
        "{pad}{}{body}{costs_suffix}{stats_suffix}",
        node_prefix(node),
    ));
}

fn format_table_function_node(
    node: &DistributedPlanNode,
    _table_function: &DistributedTableFunctionNode,
    pad: &str,
    costs_suffix: &str,
    stats_suffix: &str,
    out: &mut Vec<String>,
) {
    let body = format_shared_plan_node_header(&node.kind, PlanNodeExplainStage::Distributed)
        .expect("TableFunction is a shared explain node");
    out.push(format!(
        "{pad}{}{body}{costs_suffix}{stats_suffix}",
        node_prefix(node),
    ));
}

fn exchange_label(exchange: &DistributedExchangeNode) -> String {
    match exchange.flavor {
        ExchangeFlavor::TopNSplit { .. } => "MERGING-EXCHANGE".to_string(),
        ExchangeFlavor::CteMulticast { .. } => "EXCHANGE".to_string(),
        ExchangeFlavor::Distribution | ExchangeFlavor::LimitOffset { .. } => {
            match exchange.partition_type {
                partitions::TPartitionType::HASH_PARTITIONED
                | partitions::TPartitionType::BUCKET_SHUFFLE_HASH_PARTITIONED => {
                    "HASH EXCHANGE".to_string()
                }
                partitions::TPartitionType::UNPARTITIONED => "GATHER".to_string(),
                partitions::TPartitionType::RANDOM => "EXCHANGE".to_string(),
                _ => "EXCHANGE".to_string(),
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
    execution_distribution: Option<JoinExecutionDistribution>,
    fallback: &JoinDistribution,
) -> &'static str {
    match execution_distribution {
        Some(JoinExecutionDistribution::Broadcast) => "BROADCAST",
        Some(JoinExecutionDistribution::Partitioned) => "PARTITIONED",
        Some(JoinExecutionDistribution::Colocate) => "COLOCATE",
        None => match fallback {
            JoinDistribution::Broadcast => "BROADCAST",
            JoinDistribution::Shuffle => "PARTITIONED",
            JoinDistribution::Colocate => "COLOCATE",
            JoinDistribution::Unknown => "UNKNOWN",
        },
    }
}

fn actual_suffix(
    node: &DistributedPlanNode,
    actuals: Option<&HashMap<i32, ActualMetrics>>,
) -> String {
    match actuals.and_then(|actuals| actuals.get(&node.node_id)) {
        Some(metrics) => {
            let mut s = format!(
                " act={{rows={} time={}",
                metrics.output_rows,
                fmt_time_ns(metrics.total_time_ns)
            );
            if metrics.total_time_max_ns > 0 {
                s.push_str(&format!(
                    " (max={} min={})",
                    fmt_time_ns(metrics.total_time_max_ns),
                    fmt_time_ns(metrics.total_time_min_ns)
                ));
            }
            if metrics.build_ht_ns > 0 {
                s.push_str(&format!(" build_ht={}", fmt_time_ns(metrics.build_ht_ns)));
            }
            if metrics.search_ns > 0 {
                s.push_str(&format!(" search={}", fmt_time_ns(metrics.search_ns)));
            }
            if metrics.output_ns > 0 {
                s.push_str(&format!(" output={}", fmt_time_ns(metrics.output_ns)));
            }
            s.push_str(&format!(" peak={}}}", fmt_bytes(metrics.peak_mem_bytes)));
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

fn is_detailed(level: ExplainLevel) -> bool {
    matches!(
        level,
        ExplainLevel::Verbose | ExplainLevel::Costs | ExplainLevel::Analyze
    )
}

fn costs_suffix(stats: &PlanNodeStats, level: ExplainLevel) -> String {
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

fn format_cost_estimate(cost: &crate::sql::optimizer::statistics::CostEstimate) -> String {
    let options = crate::sql::optimizer::cost::CostOptions::default();
    format!(
        "cost={{cpu={} memory={} network={} total={}}}",
        fmt_f64(cost.cpu_cost),
        fmt_f64(cost.memory_cost),
        fmt_f64(cost.network_cost),
        fmt_f64(cost.total_with_options(&options)),
    )
}

fn stats_suffix(stats: &PlanNodeStats, level: ExplainLevel) -> String {
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

fn format_stats_trailer(stats: &PlanNodeStats, show_conf: bool) -> String {
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
            crate::sql::optimizer::statistics::Confidence::Estimated => " conf=estimated",
            crate::sql::optimizer::statistics::Confidence::Fallback => " conf=fallback",
            crate::sql::optimizer::statistics::Confidence::Exact
            | crate::sql::optimizer::statistics::Confidence::Measured => "",
        }
    } else {
        ""
    };
    format!("stats={{rows={rows_str}{conf}}}")
}

fn format_column_stats_costs(stats: &PlanNodeStats) -> String {
    if stats.column_statistics.is_empty() {
        return String::new();
    }
    let mut ids: Vec<_> = stats.column_statistics.keys().copied().collect();
    ids.sort_by_key(|id| id.0);
    let parts = ids
        .into_iter()
        .map(|column_id| {
            let c = &stats.column_statistics[&column_id];
            let ndv = if let Some(ndv) = c.ndv_value().filter(|ndv| ndv.is_finite()) {
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

fn push_build_rf_lines(
    filters: &[RuntimeFilterDesc],
    arena: &ScalarArena,
    level: ExplainLevel,
    pad: &str,
    out: &mut Vec<String>,
) {
    if !is_detailed(level) || filters.is_empty() {
        return;
    }
    out.push(format!("{pad}  build runtime filters:"));
    for rf in filters {
        out.push(format!(
            "{pad}  - filter_id = {}, build_expr = ({})",
            rf.filter_id,
            format_expr(&materialize(arena, rf.build_expr)),
        ));
    }
}

fn push_probe_rf_lines(
    filters: &[RuntimeFilterProbe],
    arena: &ScalarArena,
    level: ExplainLevel,
    pad: &str,
    out: &mut Vec<String>,
) {
    if !is_detailed(level) || filters.is_empty() {
        return;
    }
    out.push(format!("{pad}    probe runtime filters:"));
    for rf in filters {
        out.push(format!(
            "{pad}    - filter_id = {}, probe_expr = ({})",
            rf.filter_id,
            format_expr(&materialize(arena, rf.probe_expr)),
        ));
    }
}

#[derive(Default)]
struct LocalScanExplainHints {
    has_decode: bool,
    has_min_max_stats: bool,
}

fn explain_hints_for_scan(scan: &DistributedScanNode) -> LocalScanExplainHints {
    let Some(required_columns) = scan.required_columns.as_ref() else {
        return LocalScanExplainHints::default();
    };
    if required_columns.is_empty() {
        return LocalScanExplainHints::default();
    }

    let resolved = required_columns
        .iter()
        .map(|required| {
            scan.dict_columns
                .iter()
                .find(|d| d.dict_column.eq_ignore_ascii_case(required))
                .map(|d| d.source_column.clone())
                .unwrap_or_else(|| required.clone())
        })
        .collect::<Vec<_>>();

    LocalScanExplainHints {
        has_decode: scan_supports_decode_hint(&scan.table, &resolved),
        has_min_max_stats: scan_supports_min_max_stats(&scan.table, &resolved),
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

fn scan_supports_decode_hint(table: &TableDef, required_columns: &[String]) -> bool {
    match &table.source {
        ScanSource::IcebergDataFiles { .. } | ScanSource::StarRocks { .. } => {
            required_columns.iter().any(|required| {
                table
                    .columns
                    .iter()
                    .find(|column| column.name.eq_ignore_ascii_case(required))
                    .map(|column| supports_scan_decode_hint(&column.data_type))
                    .unwrap_or(false)
            })
        }
        ScanSource::IcebergMetadataTable { .. } => false,
        ScanSource::IcebergDeltaTable { .. }
        | ScanSource::IcebergVersionTable { .. }
        | ScanSource::IcebergMvTargetState { .. } => false,
    }
}

fn scan_supports_min_max_stats(table: &TableDef, required_columns: &[String]) -> bool {
    match &table.source {
        ScanSource::IcebergDataFiles { .. } | ScanSource::StarRocks { .. } => {}
        ScanSource::IcebergMetadataTable { .. } => return false,
        ScanSource::IcebergDeltaTable { .. }
        | ScanSource::IcebergVersionTable { .. }
        | ScanSource::IcebergMvTargetState { .. } => return false,
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

fn supports_scan_decode_hint(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Binary | DataType::LargeBinary
    )
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow::datatypes::DataType;

    use super::explain_distributed_plan_analyze;
    use crate::exec::node::sort::SortTopNType;
    use crate::runtime::profile_correlate::ActualMetrics;
    use crate::sql::analysis::{ExprKind, OutputColumn, ProjectItem, SortItem, TypedExpr};
    use crate::sql::catalog::{ColumnDef, ScanSource, TableDef};
    use crate::sql::codegen::ir::{build_distributed_plan, explain_distributed_plan};
    use crate::sql::column_id::ColumnId;
    use crate::sql::explain::{ExplainLevel, PlanNodeExplainStage, format_shared_plan_node_header};
    use crate::sql::optimizer::operator::{
        AggMode, Operator, PhysicalDistributionOp, PhysicalHashAggregateOp, ProjectOp, ScanOp,
    };
    use crate::sql::optimizer::options::OptimizerOptions;
    use crate::sql::optimizer::physical_plan::{
        PhysicalPlanNode, PlanExecutionProps, attach_scalar_arena,
    };
    use crate::sql::optimizer::property::DistributionSpec;
    use crate::sql::optimizer::runtime_filter_pass::{self, test_support};
    use crate::sql::optimizer::scalar::ScalarArena;
    use crate::sql::optimizer::statistics::{ColumnStatistic, Confidence, Statistics};
    use crate::sql::planner::optimizer_bridge::scalar::{
        intern_aggregate_calls, intern_exprs, intern_project_items, intern_sort_items,
    };
    use crate::sql::planner::plan::AggregateCall;

    #[test]
    fn normal_scan_project_agg_renders_node_id_prefixes() {
        let dp = build_distributed_plan(&aggregate_count_plan(project_plan(scan_plan())))
            .expect("build DistributedPlan");

        let text = explain_distributed_plan(&dp, ExplainLevel::Normal).join("\n");

        assert!(!text.contains("PLAN FRAGMENT"));
        assert!(text.contains("3:HASH AGGREGATE"));
        assert!(text.contains("2:PROJECT"));
        assert!(text.contains("1:SCAN"));
    }

    #[test]
    fn shared_distributed_formatter_path_covers_scan_and_project() {
        let dp = build_distributed_plan(&project_plan(scan_plan())).expect("build DistributedPlan");
        let root = &dp
            .fragments
            .iter()
            .find(|fragment| fragment.fragment_id == dp.root_fragment_id)
            .expect("root fragment")
            .root;

        assert_eq!(
            format_shared_plan_node_header(&root.kind, PlanNodeExplainStage::Distributed),
            Some("PROJECT [t.k AS k]".to_string())
        );
        assert_eq!(
            format_shared_plan_node_header(
                &root.children[0].kind,
                PlanNodeExplainStage::Distributed
            ),
            Some("SCAN test_db.t (alias=t)".to_string())
        );

        let text = explain_distributed_plan(&dp, ExplainLevel::Normal).join("\n");
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
    fn verbose_shuffle_agg_renders_fragments_and_exchange() {
        let dp = build_distributed_plan(&aggregate_count_plan(distribution_plan(
            scan_plan(),
            DistributionSpec::shuffle_agg([ColumnId::new_for_test(1)]),
        )))
        .expect("build DistributedPlan");

        let text = explain_distributed_plan(&dp, ExplainLevel::Verbose).join("\n");

        assert!(text.contains("PLAN FRAGMENT 0"));
        assert!(text.contains("PLAN FRAGMENT 1"));
        assert!(text.contains("EXCHANGE"));
        assert!(text.contains("HASH_PARTITIONED"));
    }

    #[test]
    fn sort_with_partition_limit_renders_topn_suffix() {
        let dp = build_distributed_plan(&sort_with_partition_limit_plan(scan_plan()))
            .expect("build DistributedPlan");

        let text = explain_distributed_plan(&dp, ExplainLevel::Normal).join("\n");

        assert!(
            text.contains("2:SORT BY [t.k ASC NULLS LAST] partition_limit=3 topn_type=RANK"),
            "expected ranking-window sort suffix in IR explain output:\n{text}"
        );
    }

    #[test]
    fn costs_renders_colstats_from_ir_stats_only_at_costs_level() {
        let mut scan = scan_plan();
        scan.stats.column_statistics.insert(
            ColumnId::new_for_test(1),
            ColumnStatistic {
                min_value: 0.0,
                max_value: 1000.0,
                nulls_fraction: 0.0,
                average_row_size: 8.0,
                ..ColumnStatistic::for_test_with_ndv(1000.0, Confidence::Exact)
            },
        );
        let dp = build_distributed_plan(&scan).expect("build DistributedPlan");

        let normal = explain_distributed_plan(&dp, ExplainLevel::Normal).join("\n");
        let verbose = explain_distributed_plan(&dp, ExplainLevel::Verbose).join("\n");
        let costs = explain_distributed_plan(&dp, ExplainLevel::Costs).join("\n");

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
            "Costs must render colstats copied into PlanNodeStats:\n{costs}"
        );
    }

    #[test]
    fn costs_colstats_ndv_uses_scientific_not_i64_saturation_for_huge_values() {
        let mut scan = scan_plan();
        scan.stats.column_statistics.insert(
            ColumnId::new_for_test(1),
            ColumnStatistic {
                min_value: 0.0,
                max_value: 1.0e300,
                nulls_fraction: 0.0,
                average_row_size: 8.0,
                ..ColumnStatistic::for_test_with_ndv(1.0e300, Confidence::Exact)
            },
        );
        let dp = build_distributed_plan(&scan).expect("build DistributedPlan");

        let costs = explain_distributed_plan(&dp, ExplainLevel::Costs).join("\n");
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
        let dp = build_distributed_plan(&scan_plan()).expect("build DistributedPlan");

        let costs = explain_distributed_plan(&dp, ExplainLevel::Costs).join("\n");
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

    #[test]
    fn detailed_ir_explain_shows_build_and_probe_rf_but_normal_hides_them() {
        let mut join = test_support::inner_join_two_scans();
        let scalars = join
            .execution_props
            .scalar_arena
            .as_ref()
            .expect("runtime-filter test plan must carry scalar arena")
            .clone();
        runtime_filter_pass::annotate(
            &mut join,
            scalars.as_ref(),
            &OptimizerOptions::default_settings(),
        );
        let dp = build_distributed_plan(&join).expect("build DistributedPlan");

        for level in [
            ExplainLevel::Verbose,
            ExplainLevel::Costs,
            ExplainLevel::Analyze,
        ] {
            let text = explain_distributed_plan(&dp, level).join("\n");
            assert!(
                text.contains("build runtime filters:"),
                "missing build RF at {level:?}:\n{text}"
            );
            assert!(
                text.contains("filter_id = 0"),
                "missing RF id at {level:?}:\n{text}"
            );
            assert!(
                text.contains("probe runtime filters:"),
                "missing probe RF at {level:?}:\n{text}"
            );
        }

        let normal = explain_distributed_plan(&dp, ExplainLevel::Normal).join("\n");
        assert!(
            !normal.contains("runtime filters:"),
            "Normal must hide RF lines:\n{normal}"
        );
    }

    #[test]
    fn analyze_renders_actuals_for_nodes_present_in_map_only() {
        let dp = build_distributed_plan(&aggregate_count_plan(project_plan(scan_plan())))
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

        let text =
            explain_distributed_plan_analyze(&dp, ExplainLevel::Analyze, &actuals).join("\n");

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
    }

    #[test]
    fn actual_suffix_renders_phase_timers_and_minmax() {
        let dp = build_distributed_plan(&aggregate_count_plan(project_plan(scan_plan())))
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
                output_ns: 18_000_000_000,
            },
        );

        let text =
            explain_distributed_plan_analyze(&dp, ExplainLevel::Analyze, &actuals).join("\n");

        assert!(
            text.contains(
                "act={rows=13502430 time=44.8s (max=46.0s min=43.0s) search=20.0s output=18.0s peak=607.5MB}"
            ),
            "expected scan actuals to include phase timers and per-driver min/max in order:\n{text}"
        );
        assert!(
            !text.contains("build_ht="),
            "zero build hash-table timer must not render:\n{text}"
        );
    }

    #[test]
    fn analyze_without_actuals_matches_existing_explain() {
        let dp = build_distributed_plan(&aggregate_count_plan(project_plan(scan_plan())))
            .expect("build DistributedPlan");
        let actuals = HashMap::new();

        assert_eq!(
            explain_distributed_plan(&dp, ExplainLevel::Analyze),
            explain_distributed_plan_analyze(&dp, ExplainLevel::Analyze, &actuals)
        );
    }

    #[test]
    fn aggregate_group_display_does_not_match_descendant_scan_by_name_only() {
        let dp = build_distributed_plan(&aggregate_count_on_projected_id_plan(
            project_alias_collision_plan(alias_collision_scan_plan()),
        ))
        .expect("build DistributedPlan");

        let text = explain_distributed_plan(&dp, ExplainLevel::Normal).join("\n");

        assert!(
            text.contains("3:HASH AGGREGATE (SINGLE, group by: [id])"),
            "expected projected group key name to remain unqualified:\n{text}"
        );
        assert!(
            !text.contains("group by: [base.id]"),
            "group key display must not fall back to a same-name descendant scan column:\n{text}"
        );
    }

    fn scan_plan() -> PhysicalPlanNode {
        let k = output_col(1, "k", DataType::Int64, false);
        let v = output_col(2, "v", DataType::Int64, true);
        physical_node(
            Operator::PhysicalScan(ScanOp {
                database: "test_db".to_string(),
                table: table_def(),
                alias: Some("t".to_string()),
                stats_ref: None,
                columns: vec![k.clone(), v.clone()],
                predicates: vec![],
                required_columns: Some(vec!["k".to_string(), "v".to_string()]),
                dict_columns: vec![],
                variant_columns: vec![],
                mv_rewritten_from: None,
            }),
            vec![],
            vec![k, v],
        )
    }

    fn alias_collision_scan_plan() -> PhysicalPlanNode {
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
                dict_columns: vec![],
                variant_columns: vec![],
                mv_rewritten_from: None,
            }),
            vec![],
            vec![id, v],
        )
    }

    fn project_alias_collision_plan(child: PhysicalPlanNode) -> PhysicalPlanNode {
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

    fn project_plan(child: PhysicalPlanNode) -> PhysicalPlanNode {
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

    fn aggregate_count_plan(child: PhysicalPlanNode) -> PhysicalPlanNode {
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
                output_columns: vec![k.clone(), count.clone()],
                is_merge: vec![false],
            }),
            vec![child],
            vec![k, count],
            scalars,
        )
    }

    fn aggregate_count_on_projected_id_plan(child: PhysicalPlanNode) -> PhysicalPlanNode {
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
                output_columns: vec![id.clone(), count.clone()],
                is_merge: vec![false],
            }),
            vec![child],
            vec![id, count],
            scalars,
        )
    }

    fn sort_with_partition_limit_plan(child: PhysicalPlanNode) -> PhysicalPlanNode {
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

    fn distribution_plan(child: PhysicalPlanNode, spec: DistributionSpec) -> PhysicalPlanNode {
        let output_columns = child.output_columns.clone();
        physical_node(
            Operator::PhysicalDistribution(PhysicalDistributionOp { spec }),
            vec![child],
            output_columns,
        )
    }

    fn physical_node(
        op: Operator,
        children: Vec<PhysicalPlanNode>,
        output_columns: Vec<OutputColumn>,
    ) -> PhysicalPlanNode {
        let scalars = scalars_from_children(&children);
        physical_node_with_scalars(op, children, output_columns, scalars)
    }

    fn physical_node_with_scalars(
        op: Operator,
        children: Vec<PhysicalPlanNode>,
        output_columns: Vec<OutputColumn>,
        scalars: ScalarArena,
    ) -> PhysicalPlanNode {
        let mut plan = PhysicalPlanNode {
            op,
            children,
            stats: Statistics {
                output_row_count: 3.0,
                ..Default::default()
            },
            output_columns,
            execution_props: PlanExecutionProps::default(),
            build_runtime_filters: vec![],
            probe_runtime_filters: vec![],
        };
        attach_scalar_arena(&mut plan, Arc::new(scalars));
        plan
    }

    fn scalars_from_children(children: &[PhysicalPlanNode]) -> ScalarArena {
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
