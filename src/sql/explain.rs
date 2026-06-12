//! EXPLAIN plan formatter — produces text from LogicalPlan or PhysicalPlan.

use std::fmt::Write;

use arrow::datatypes::DataType;

use crate::sql::analysis::{BinOp, ExprKind, JoinKind, LiteralValue, TypedExpr, UnOp};
use crate::sql::catalog::ScanSource;
use crate::sql::optimizer::estimate::arith::MAX_ROW_COUNT;
use crate::sql::optimizer::operator::{AggMode, JoinDistribution, Operator, PhysicalScanOp};
use crate::sql::optimizer::physical_plan::{JoinExecutionDistribution, PhysicalPlanNode};
use crate::sql::optimizer::property::DistributionSpec;
use crate::sql::planner::plan::{ApplyKind, LogicalPlan, ScanVariantColumn};

/// Build the per-node `stats={...}` trailer surfaced under
/// `Verbose | Costs | Analyze` levels. Future PRs (OPT-3 NDV, OPT-4
/// distribution) append keys after `rows=`; never reorder existing
/// keys — golden files depend on stable ordering.
pub(crate) fn format_stats_trailer(
    stats: &crate::sql::optimizer::statistics::Statistics,
) -> String {
    format_stats_trailer_with_conf(stats, false)
}

pub(crate) fn format_stats_trailer_with_conf(
    stats: &crate::sql::optimizer::statistics::Statistics,
    show_conf: bool,
) -> String {
    let rows = stats.output_row_count;
    let rows_str: String = if rows.is_nan() || rows <= 0.0 {
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
            crate::sql::optimizer::statistics::Confidence::Exact => "",
        }
    } else {
        ""
    };
    format!("stats={{rows={rows_str}{conf}}}")
}

/// Costs-only per-column statistics block. Kept separate from
/// `format_stats_trailer` so Verbose/Analyze output stays focused on row
/// counts — only `EXPLAIN COSTS` shows column stats.
/// Unknown-stat columns (ColumnStatistic::unknown) render as min=-inf max=+inf ndv=1 null_frac=0.
pub(crate) fn format_column_stats_costs(
    stats: &crate::sql::optimizer::statistics::Statistics,
) -> String {
    if stats.column_statistics.is_empty() {
        return String::new();
    }
    let mut ids: Vec<_> = stats.column_statistics.keys().copied().collect();
    ids.sort_by_key(|id| id.0);
    let parts: Vec<String> = ids
        .into_iter()
        .map(|column_id| {
            let c = &stats.column_statistics[&column_id];
            let ndv = if c.distinct_values_count.is_finite() {
                (c.distinct_values_count.round() as i64).to_string()
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
        .collect();
    format!("colstats={{{}}}", parts.join(", "))
}

fn fmt_f64(v: f64) -> String {
    if v.is_nan() {
        "?".to_string()
    } else if v.is_infinite() {
        if v > 0.0 {
            "+inf".to_string()
        } else {
            "-inf".to_string()
        }
    } else if v.fract() == 0.0 {
        format!("{}", v as i64)
    } else {
        format!("{v:.4}")
    }
}

/// Detail level for EXPLAIN output.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ExplainLevel {
    Normal,
    Verbose,
    Costs,
    /// Produce node-level output identical to Verbose; the
    /// Planning/Execution/Rows header is added by
    /// `explain_analyze_query` in `src/engine/mod.rs`.
    Analyze,
}

/// Format a single LogicalPlan tree as EXPLAIN text lines.
#[allow(dead_code)]
pub(crate) fn explain_plan(plan: &LogicalPlan, level: ExplainLevel) -> Vec<String> {
    let mut out = Vec::new();
    format_node(plan, level, 0, &mut out);
    out
}

#[allow(dead_code)]
fn format_node(plan: &LogicalPlan, level: ExplainLevel, indent: usize, out: &mut Vec<String>) {
    let pad = "  ".repeat(indent);
    match plan {
        LogicalPlan::Scan(node) => {
            let alias = node
                .alias
                .as_deref()
                .map(|a| format!(" (alias={a})"))
                .unwrap_or_default();
            out.push(format!(
                "{pad}0:SCAN {db}.{tbl}{alias}",
                db = node.database,
                tbl = node.table.name
            ));
            if let Some(ref cols) = node.required_columns
                && matches!(
                    level,
                    ExplainLevel::Verbose | ExplainLevel::Costs | ExplainLevel::Analyze
                )
            {
                out.push(format!("{pad}     columns: {}", cols.join(", ")));
            }
            if matches!(
                level,
                ExplainLevel::Verbose | ExplainLevel::Costs | ExplainLevel::Analyze
            ) && let Some(source) = logical_scan_source_label(&node.table.source)
            {
                out.push(format!("{pad}     source: {source}"));
            }
            if !node.predicates.is_empty() {
                let preds: Vec<String> = node.predicates.iter().map(format_expr).collect();
                out.push(format!("{pad}     predicates: {}", preds.join(" AND ")));
            }
        }
        LogicalPlan::Filter(node) => {
            out.push(format!("{pad}FILTER"));
            out.push(format!(
                "{pad}  predicate: {}",
                format_expr(&node.predicate)
            ));
            format_node(&node.input, level, indent + 1, out);
        }
        LogicalPlan::Project(node) => {
            let items: Vec<String> = node
                .items
                .iter()
                .map(|item| {
                    let expr_str = format_expr(&item.expr);
                    if item.output_name != expr_str {
                        format!("{expr_str} AS {}", item.output_name)
                    } else {
                        expr_str
                    }
                })
                .collect();
            out.push(format!("{pad}PROJECT [{}]", items.join(", ")));
            format_node(&node.input, level, indent + 1, out);
        }
        LogicalPlan::Aggregate(node) => {
            let groups: Vec<String> = node.group_by.iter().map(format_expr).collect();
            let aggs: Vec<String> = node
                .aggregates
                .iter()
                .map(|a| {
                    let args: Vec<String> = a.args.iter().map(format_expr).collect();
                    let distinct = if a.distinct { "DISTINCT " } else { "" };
                    format!("{}({}{})", a.name, distinct, args.join(", "))
                })
                .collect();
            out.push(format!("{pad}AGGREGATE"));
            if !groups.is_empty() {
                out.push(format!("{pad}  group by: {}", groups.join(", ")));
            }
            if !aggs.is_empty() {
                out.push(format!("{pad}  aggregations: {}", aggs.join(", ")));
            }
            format_node(&node.input, level, indent + 1, out);
        }
        LogicalPlan::Join(node) => {
            let join_str = match node.join_type {
                JoinKind::Inner => "INNER JOIN",
                JoinKind::LeftOuter => "LEFT OUTER JOIN",
                JoinKind::RightOuter => "RIGHT OUTER JOIN",
                JoinKind::FullOuter => "FULL OUTER JOIN",
                JoinKind::Cross => "CROSS JOIN",
                JoinKind::LeftSemi => "LEFT SEMI JOIN",
                JoinKind::RightSemi => "RIGHT SEMI JOIN",
                JoinKind::LeftAnti => "LEFT ANTI JOIN",
                JoinKind::RightAnti => "RIGHT ANTI JOIN",
                JoinKind::NullAwareLeftAnti => "NULL AWARE LEFT ANTI JOIN",
            };
            out.push(format!("{pad}{join_str}"));
            if let Some(ref cond) = node.condition {
                out.push(format!("{pad}  on: {}", format_expr(cond)));
            }
            format_node(&node.left, level, indent + 1, out);
            format_node(&node.right, level, indent + 1, out);
        }
        LogicalPlan::Sort(node) => {
            let items: Vec<String> = node
                .items
                .iter()
                .map(|s| {
                    let dir = if s.asc { "ASC" } else { "DESC" };
                    let nulls = if s.nulls_first {
                        " NULLS FIRST"
                    } else {
                        " NULLS LAST"
                    };
                    format!("{} {dir}{nulls}", format_expr(&s.expr))
                })
                .collect();
            out.push(format!("{pad}SORT BY [{}]", items.join(", ")));
            format_node(&node.input, level, indent + 1, out);
        }
        LogicalPlan::Limit(node) => {
            let mut parts = Vec::new();
            if let Some(limit) = node.limit {
                parts.push(format!("limit={limit}"));
            }
            if let Some(offset) = node.offset {
                parts.push(format!("offset={offset}"));
            }
            out.push(format!("{pad}LIMIT [{}]", parts.join(", ")));
            format_node(&node.input, level, indent + 1, out);
        }
        LogicalPlan::Union(node) => {
            let kind = if node.all { "UNION ALL" } else { "UNION" };
            out.push(format!("{pad}{kind}"));
            for input in &node.inputs {
                format_node(input, level, indent + 1, out);
            }
        }
        LogicalPlan::Intersect(node) => {
            out.push(format!("{pad}INTERSECT"));
            for input in &node.inputs {
                format_node(input, level, indent + 1, out);
            }
        }
        LogicalPlan::Except(node) => {
            out.push(format!("{pad}EXCEPT"));
            for input in &node.inputs {
                format_node(input, level, indent + 1, out);
            }
        }
        LogicalPlan::Window(node) => {
            let fns: Vec<String> = node
                .window_exprs
                .iter()
                .map(|w| {
                    let args: Vec<String> = w.args.iter().map(format_expr).collect();
                    let partition: Vec<String> = w.partition_by.iter().map(format_expr).collect();
                    let order: Vec<String> = w
                        .order_by
                        .iter()
                        .map(|s| {
                            let dir = if s.asc { "ASC" } else { "DESC" };
                            format!("{} {dir}", format_expr(&s.expr))
                        })
                        .collect();
                    let mut over_parts = Vec::new();
                    if !partition.is_empty() {
                        over_parts.push(format!("PARTITION BY {}", partition.join(", ")));
                    }
                    if !order.is_empty() {
                        over_parts.push(format!("ORDER BY {}", order.join(", ")));
                    }
                    format!(
                        "{}({}) OVER ({})",
                        w.name,
                        args.join(", "),
                        over_parts.join(" ")
                    )
                })
                .collect();
            out.push(format!("{pad}WINDOW [{}]", fns.join("; ")));
            format_node(&node.input, level, indent + 1, out);
        }
        LogicalPlan::Values(node) => {
            out.push(format!("{pad}VALUES ({} rows)", node.rows.len()));
        }
        LogicalPlan::GenerateSeries(node) => {
            out.push(format!(
                "{pad}GENERATE_SERIES({}, {}, {})",
                node.start, node.end, node.step
            ));
        }
        LogicalPlan::TableFunction(node) => {
            let join_type = if node.is_left_join { "LEFT" } else { "CROSS" };
            out.push(format!(
                "{pad}TABLE_FUNCTION [{} {}]",
                join_type,
                node.function_name.to_uppercase()
            ));
            format_node(&node.input, level, indent + 1, out);
        }
        LogicalPlan::Repeat(node) => {
            out.push(format!(
                "{pad}REPEAT ({} grouping sets)",
                node.grouping_ids.len()
            ));
            format_node(&node.input, level, indent + 1, out);
        }
        LogicalPlan::CTEAnchor(node) => {
            out.push(format!("{pad}CTE_ANCHOR(cte_id={})", node.cte_id));
            format_node(&node.produce, level, indent + 1, out);
            format_node(&node.consumer, level, indent + 1, out);
        }
        LogicalPlan::CTEProduce(node) => {
            out.push(format!("{pad}CTE_PRODUCE(cte_id={})", node.cte_id));
            format_node(&node.input, level, indent + 1, out);
        }
        LogicalPlan::CTEConsume(node) => {
            out.push(format!("{pad}CTE_CONSUME(cte_id={})", node.cte_id));
        }
        LogicalPlan::Decode(node) => {
            let pairs: Vec<String> = node
                .mappings
                .iter()
                .map(|m| format!("{}->{}", m.dict_column, m.string_column))
                .collect();
            out.push(format!("{pad}DECODE [{}]", pairs.join(", ")));
            format_node(&node.input, level, indent + 1, out);
        }
        LogicalPlan::AggregateStateMerge(node) => {
            out.push(format!(
                "{}AggregateStateMerge keys=[{}] states=[{}] change_op={}",
                pad,
                node.group_key_names.join(","),
                node.aggregate_state_names.join(","),
                node.change_op_column
            ));
            format_node(&node.old_input, level, indent + 1, out);
            format_node(&node.delta_input, level, indent + 1, out);
        }
        LogicalPlan::Apply(node) => {
            let kind = match node.kind {
                ApplyKind::Scalar => "SCALAR",
                ApplyKind::Exists { negated: false } => "EXISTS",
                ApplyKind::Exists { negated: true } => "NOT EXISTS",
                ApplyKind::In { negated: false } => "IN",
                ApplyKind::In { negated: true } => "NOT IN",
            };
            out.push(format!(
                "{pad}APPLY ({kind}, correlated={}, use_semi_anti={})",
                !node.correlation_column_ids.is_empty(),
                node.use_semi_anti
            ));
            format_node(&node.left, level, indent + 1, out);
            format_node(&node.right, level, indent + 1, out);
        }
        LogicalPlan::AssertOneRow(node) => {
            out.push(format!("{pad}ASSERT ONE ROW"));
            format_node(&node.input, level, indent + 1, out);
        }
        LogicalPlan::ImvDelta(_) | LogicalPlan::ImvVersion(_) => {
            panic!("imv marker leaked into non-IMV plan");
        }
    }
}

fn logical_scan_source_label(source: &ScanSource) -> Option<String> {
    match source {
        ScanSource::IcebergVersionTable { snapshot_id, .. } => {
            Some(format!("IcebergVersionTable snapshot_id={snapshot_id}"))
        }
        ScanSource::IcebergMvTargetState(scan) => Some(format!(
            "IcebergMvTargetState target={} keys=[{}] states=[{}] {}",
            scan.fqn(),
            scan.group_key_names.join(","),
            scan.aggregate_state_names.join(","),
            scan.constraint_summary()
        )),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Physical plan formatting
// ---------------------------------------------------------------------------

/// Format a PhysicalPlanNode tree as EXPLAIN text lines.
pub(crate) fn explain_physical_plan(plan: &PhysicalPlanNode, level: ExplainLevel) -> Vec<String> {
    let mut out = Vec::new();
    format_physical_node(plan, level, 0, &mut out);
    out
}

pub(crate) fn format_boundary_schema_reports(
    reports: &[crate::sql::codegen::boundary_schema::BoundarySchemaReport],
) -> Vec<String> {
    if reports.is_empty() {
        return Vec::new();
    }

    let mut out = Vec::new();
    out.push("Boundary Schemas:".to_string());
    for report in reports {
        let fragment = report
            .fragment_id
            .map(|id| id.to_string())
            .unwrap_or_else(|| "root".to_string());
        out.push(format!(
            "  Fragment {} {} node={}:",
            fragment,
            report.boundary_kind.label(),
            report.node_id
        ));
        for column in &report.columns {
            out.push(format!(
                "    slot={} name={} arrow={:?} logical={} nullable={}",
                column.slot_id,
                column.name,
                column.arrow_type,
                column.logical_type.as_deref().unwrap_or("<none>"),
                column.nullable
            ));
        }
    }
    out
}

fn join_distribution_label(node: &PhysicalPlanNode, fallback: &JoinDistribution) -> &'static str {
    match node.execution_props.join_distribution {
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

fn format_physical_node(
    node: &PhysicalPlanNode,
    level: ExplainLevel,
    indent: usize,
    out: &mut Vec<String>,
) {
    let pad = "  ".repeat(indent);
    let costs_suffix = if matches!(level, ExplainLevel::Costs) {
        let colstats = format_column_stats_costs(&node.stats);
        if colstats.is_empty() {
            format!(" (rows={:.0})", node.stats.output_row_count)
        } else {
            format!(" (rows={:.0}) {colstats}", node.stats.output_row_count)
        }
    } else {
        String::new()
    };
    let stats_suffix = if matches!(
        level,
        ExplainLevel::Verbose | ExplainLevel::Costs | ExplainLevel::Analyze
    ) {
        let trailer = if matches!(level, ExplainLevel::Costs | ExplainLevel::Analyze) {
            format_stats_trailer_with_conf(&node.stats, true)
        } else {
            format_stats_trailer(&node.stats)
        };
        format!(" {trailer}")
    } else {
        String::new()
    };

    match &node.op {
        Operator::PhysicalScan(op) => {
            let alias = op
                .alias
                .as_deref()
                .map(|a| format!(" (alias={a})"))
                .unwrap_or_default();
            out.push(format!(
                "{pad}SCAN {}.{}{alias}{costs_suffix}{stats_suffix}",
                op.database, op.table.name
            ));
            out.push(format!(
                "{pad}     TABLE: {}.{}",
                op.database, op.table.name
            ));
            if let Some(ref mv) = op.mv_rewritten_from {
                out.push(format!("{pad}     rewritten with mv: {mv}"));
            }
            if let Some(ref cols) = op.required_columns
                && matches!(
                    level,
                    ExplainLevel::Verbose | ExplainLevel::Costs | ExplainLevel::Analyze
                )
            {
                out.push(format!("{pad}     columns: {}", cols.join(", ")));
                if matches!(level, ExplainLevel::Verbose | ExplainLevel::Analyze) {
                    for line in scan_pruned_type_lines(op, cols) {
                        out.push(format!("{pad}     {line}"));
                    }
                }
            }
            if matches!(
                level,
                ExplainLevel::Verbose | ExplainLevel::Costs | ExplainLevel::Analyze
            ) && let Some(line) = scan_variant_column_line(op)
            {
                out.push(format!("{pad}     {line}"));
            }
            let local_hints = explain_hints_for_scan(op);
            if matches!(level, ExplainLevel::Costs) && local_hints.has_decode {
                out.push(format!("{pad}     Decode"));
            }
            if matches!(level, ExplainLevel::Verbose | ExplainLevel::Analyze)
                && local_hints.has_min_max_stats
            {
                out.push(format!("{pad}     min-max stats"));
            }
            if !op.predicates.is_empty() {
                let preds: Vec<String> = op.predicates.iter().map(format_expr).collect();
                out.push(format!("{pad}     predicates: {}", preds.join(" AND ")));
            }
            push_probe_rf_lines(node, level, &pad, out);
        }
        Operator::PhysicalFilter(op) => {
            out.push(format!("{pad}FILTER{costs_suffix}{stats_suffix}"));
            out.push(format!("{pad}  predicate: {}", format_expr(&op.predicate)));
            push_probe_rf_lines(node, level, &pad, out);
            for child in &node.children {
                format_physical_node(child, level, indent + 1, out);
            }
        }
        Operator::PhysicalProject(op) => {
            let items: Vec<String> = op
                .items
                .iter()
                .map(|item| {
                    let expr_str = format_expr(&item.expr);
                    if item.output_name != expr_str {
                        format!("{expr_str} AS {}", item.output_name)
                    } else {
                        expr_str
                    }
                })
                .collect();
            out.push(format!(
                "{pad}PROJECT [{}]{costs_suffix}{stats_suffix}",
                items.join(", ")
            ));
            push_probe_rf_lines(node, level, &pad, out);
            for child in &node.children {
                format_physical_node(child, level, indent + 1, out);
            }
        }
        Operator::PhysicalHashJoin(op) => {
            let dist = join_distribution_label(node, &op.distribution);
            let join_str = match op.join_type {
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
            };
            let eq: Vec<String> = op
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
                .collect();
            out.push(format!(
                "{pad}HASH JOIN ({dist}, {join_str}, eq: [{}]){costs_suffix}{stats_suffix}",
                eq.join(", ")
            ));
            if let Some(ref other) = op.other_condition {
                out.push(format!("{pad}  other: {}", format_expr(other)));
            }
            if matches!(
                level,
                ExplainLevel::Verbose | ExplainLevel::Costs | ExplainLevel::Analyze
            ) && !node.build_runtime_filters.is_empty()
            {
                out.push(format!("{pad}  build runtime filters:"));
                for rf in &node.build_runtime_filters {
                    out.push(format!(
                        "{pad}  - filter_id = {}, build_expr = ({})",
                        rf.filter_id,
                        format_expr(&rf.build_expr),
                    ));
                }
            }
            for child in &node.children {
                format_physical_node(child, level, indent + 1, out);
            }
        }
        Operator::PhysicalNestLoopJoin(op) => {
            let join_str = match op.join_type {
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
            };
            out.push(format!(
                "{pad}NEST LOOP JOIN ({join_str}){costs_suffix}{stats_suffix}"
            ));
            if let Some(ref cond) = op.condition {
                out.push(format!("{pad}  on: {}", format_expr(cond)));
            }
            for child in &node.children {
                format_physical_node(child, level, indent + 1, out);
            }
        }
        Operator::PhysicalHashAggregate(op) => {
            let mode = match op.mode {
                AggMode::Single => "SINGLE",
                AggMode::Local => "LOCAL",
                AggMode::Global => "GLOBAL",
                AggMode::DistinctGlobal => "DISTINCT_GLOBAL",
                AggMode::DistinctLocal => "DISTINCT_LOCAL",
            };
            let groups: Vec<String> = op.group_by.iter().map(format_expr).collect();
            let aggs: Vec<String> = op
                .aggregates
                .iter()
                .map(|a| {
                    let args: Vec<String> = a.args.iter().map(format_expr).collect();
                    let distinct = if a.distinct { "DISTINCT " } else { "" };
                    format!("{}({}{})", a.name, distinct, args.join(", "))
                })
                .collect();
            let mut detail = format!("{pad}HASH AGGREGATE ({mode}");
            if !groups.is_empty() {
                let _ = write!(detail, ", group by: [{}]", groups.join(", "));
            }
            let _ = write!(detail, "){costs_suffix}{stats_suffix}");
            out.push(detail);
            if !aggs.is_empty() {
                out.push(format!("{pad}  aggregations: {}", aggs.join(", ")));
            }
            push_probe_rf_lines(node, level, &pad, out);
            for child in &node.children {
                format_physical_node(child, level, indent + 1, out);
            }
        }
        Operator::PhysicalSort(op) => {
            let items: Vec<String> = op
                .items
                .iter()
                .map(|s| {
                    let dir = if s.asc { "ASC" } else { "DESC" };
                    let nulls = if s.nulls_first {
                        " NULLS FIRST"
                    } else {
                        " NULLS LAST"
                    };
                    format!("{} {dir}{nulls}", format_expr(&s.expr))
                })
                .collect();
            out.push(format!(
                "{pad}SORT BY [{}]{costs_suffix}{stats_suffix}",
                items.join(", ")
            ));
            for child in &node.children {
                format_physical_node(child, level, indent + 1, out);
            }
        }
        Operator::PhysicalTopN(op) => {
            let label = match op.phase {
                crate::sql::optimizer::operator::TopNPhase::Partial => "LOCAL TOP-N",
                crate::sql::optimizer::operator::TopNPhase::Final => "TOP-N",
            };
            let items: Vec<String> = op
                .items
                .iter()
                .map(|s| {
                    let dir = if s.asc { "ASC" } else { "DESC" };
                    let nulls = if s.nulls_first {
                        " NULLS FIRST"
                    } else {
                        " NULLS LAST"
                    };
                    format!("{} {dir}{nulls}", format_expr(&s.expr))
                })
                .collect();
            let mut parts = Vec::new();
            if let Some(l) = op.limit {
                parts.push(format!("limit={l}"));
            }
            if let Some(o) = op.offset {
                parts.push(format!("offset={o}"));
            }
            out.push(format!(
                "{pad}{label} ({}) [{}]{costs_suffix}{stats_suffix}",
                parts.join(", "),
                items.join(", ")
            ));
            for child in &node.children {
                format_physical_node(child, level, indent + 1, out);
            }
        }
        Operator::PhysicalLimit(op) => {
            let mut parts = Vec::new();
            if let Some(limit) = op.limit {
                parts.push(format!("limit={limit}"));
            }
            if let Some(offset) = op.offset {
                parts.push(format!("offset={offset}"));
            }
            out.push(format!(
                "{pad}LIMIT [{}]{costs_suffix}{stats_suffix}",
                parts.join(", ")
            ));
            for child in &node.children {
                format_physical_node(child, level, indent + 1, out);
            }
        }
        Operator::PhysicalAssertOneRow(_) => {
            out.push(format!(
                "{pad}ASSERT NUM ROWS (<= 1){costs_suffix}{stats_suffix}"
            ));
            for child in &node.children {
                format_physical_node(child, level, indent + 1, out);
            }
        }
        Operator::PhysicalDistribution(op) => {
            let label = match &op.spec {
                DistributionSpec::Any => "ANY EXCHANGE".to_string(),
                DistributionSpec::Gather => "GATHER EXCHANGE".to_string(),
                DistributionSpec::Broadcast => "BROADCAST EXCHANGE".to_string(),
                DistributionSpec::HashPartitioned { cols, source } => {
                    let col_names: Vec<String> = cols.iter().map(|c| format!("{}", c)).collect();
                    format!(
                        "HASH EXCHANGE (source: {:?}, hash: [{}])",
                        source,
                        col_names.join(", ")
                    )
                }
            };
            out.push(format!("{pad}{label}{costs_suffix}{stats_suffix}"));
            for child in &node.children {
                format_physical_node(child, level, indent + 1, out);
            }
        }
        Operator::PhysicalWindow(op) => {
            let fns: Vec<String> = op
                .window_exprs
                .iter()
                .map(|w| {
                    let args: Vec<String> = w.args.iter().map(format_expr).collect();
                    format!("{}({})", w.name, args.join(", "))
                })
                .collect();
            out.push(format!(
                "{pad}WINDOW [{}]{costs_suffix}{stats_suffix}",
                fns.join("; ")
            ));
            for child in &node.children {
                format_physical_node(child, level, indent + 1, out);
            }
        }
        Operator::PhysicalCTEAnchor(op) => {
            out.push(format!(
                "{pad}CTE ANCHOR (cte_id={}){costs_suffix}{stats_suffix}",
                op.cte_id
            ));
            for child in &node.children {
                format_physical_node(child, level, indent + 1, out);
            }
        }
        Operator::PhysicalCTEProduce(op) => {
            out.push(format!(
                "{pad}CTE PRODUCE (cte_id={}){costs_suffix}{stats_suffix}",
                op.cte_id
            ));
            for child in &node.children {
                format_physical_node(child, level, indent + 1, out);
            }
        }
        Operator::PhysicalCTEConsume(op) => {
            out.push(format!(
                "{pad}CTE CONSUME (cte_id={}){costs_suffix}{stats_suffix}",
                op.cte_id
            ));
        }
        Operator::PhysicalRepeat(op) => {
            out.push(format!(
                "{pad}REPEAT ({} grouping sets){costs_suffix}{stats_suffix}",
                op.grouping_ids.len()
            ));
            for child in &node.children {
                format_physical_node(child, level, indent + 1, out);
            }
        }
        Operator::PhysicalUnion(op) => {
            let kind = if op.all { "UNION ALL" } else { "UNION" };
            out.push(format!("{pad}{kind}{costs_suffix}{stats_suffix}"));
            for child in &node.children {
                format_physical_node(child, level, indent + 1, out);
            }
        }
        Operator::PhysicalIntersect(_) => {
            out.push(format!("{pad}INTERSECT{costs_suffix}{stats_suffix}"));
            for child in &node.children {
                format_physical_node(child, level, indent + 1, out);
            }
        }
        Operator::PhysicalExcept(_) => {
            out.push(format!("{pad}EXCEPT{costs_suffix}{stats_suffix}"));
            for child in &node.children {
                format_physical_node(child, level, indent + 1, out);
            }
        }
        Operator::PhysicalValues(op) => {
            out.push(format!(
                "{pad}VALUES ({} rows){costs_suffix}{stats_suffix}",
                op.rows.len()
            ));
            push_probe_rf_lines(node, level, &pad, out);
        }
        Operator::PhysicalGenerateSeries(op) => {
            out.push(format!(
                "{pad}GENERATE_SERIES({}, {}, {}){costs_suffix}{stats_suffix}",
                op.start, op.end, op.step
            ));
        }
        Operator::PhysicalTableFunction(op) => {
            let join_type = if op.is_left_join { "LEFT" } else { "CROSS" };
            out.push(format!(
                "{pad}TABLE_FUNCTION [{} {}]{costs_suffix}{stats_suffix}",
                join_type,
                op.function_name.to_uppercase()
            ));
            for child in &node.children {
                format_physical_node(child, level, indent + 1, out);
            }
        }
        Operator::PhysicalDecode(op) => {
            let pairs: Vec<String> = op
                .mappings
                .iter()
                .map(|m| format!("{}->{}", m.dict_column, m.string_column))
                .collect();
            out.push(format!(
                "{pad}DECODE [{}]{costs_suffix}{stats_suffix}",
                pairs.join(", ")
            ));
            for child in &node.children {
                format_physical_node(child, level, indent + 1, out);
            }
        }
        Operator::PhysicalAggregateStateMerge(op) => {
            out.push(format!(
                "{}AggregateStateMerge keys=[{}] states=[{}] change_op={}{}{}",
                pad,
                op.group_key_names.join(","),
                op.aggregate_state_names.join(","),
                op.change_op_column,
                costs_suffix,
                stats_suffix
            ));
            for child in &node.children {
                format_physical_node(child, level, indent + 1, out);
            }
        }
        // Logical operators should not appear in physical plan
        _ => {
            out.push(format!(
                "{pad}<logical operator>{costs_suffix}{stats_suffix}"
            ));
        }
    }
}

fn push_probe_rf_lines(
    node: &PhysicalPlanNode,
    level: ExplainLevel,
    pad: &str,
    out: &mut Vec<String>,
) {
    if !matches!(
        level,
        ExplainLevel::Verbose | ExplainLevel::Costs | ExplainLevel::Analyze
    ) || node.probe_runtime_filters.is_empty()
    {
        return;
    }
    out.push(format!("{pad}    probe runtime filters:"));
    for rf in &node.probe_runtime_filters {
        out.push(format!(
            "{pad}    - filter_id = {}, probe_expr = ({})",
            rf.filter_id,
            format_expr(&rf.probe_expr),
        ));
    }
}

#[derive(Default)]
struct LocalScanExplainHints {
    has_decode: bool,
    has_min_max_stats: bool,
}

fn explain_hints_for_scan(
    op: &crate::sql::optimizer::operator::PhysicalScanOp,
) -> LocalScanExplainHints {
    let Some(required_columns) = op.required_columns.as_ref() else {
        return LocalScanExplainHints::default();
    };
    if required_columns.is_empty() {
        return LocalScanExplainHints::default();
    }

    // Translate any hidden dict-encoded slot names (`__nr_dict_<table>_<col>`,
    // emitted by `LowCardinalityDictionaryRewrite`) back to their source
    // string columns before checking format-level capabilities. The
    // dict slot is a synthetic Int32 that the scan reader produces *from*
    // the underlying storage column, so the on-disk min-max / decode-hint
    // semantics belong to the source column, not the dict slot.
    let resolved: Vec<String> = required_columns
        .iter()
        .map(|required| {
            op.dict_columns
                .iter()
                .find(|d| d.dict_column.eq_ignore_ascii_case(required))
                .map(|d| d.source_column.clone())
                .unwrap_or_else(|| required.clone())
        })
        .collect();

    LocalScanExplainHints {
        has_decode: scan_supports_decode_hint(&op.table, &resolved),
        has_min_max_stats: scan_supports_min_max_stats(&op.table, &resolved),
    }
}

fn scan_pruned_type_lines(op: &PhysicalScanOp, required_columns: &[String]) -> Vec<String> {
    required_columns
        .iter()
        .filter_map(|required| {
            let (slot, data_type) = scan_required_column_type(op, required)?;
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

fn scan_variant_column_line(op: &PhysicalScanOp) -> Option<String> {
    if op.variant_columns.is_empty() {
        return None;
    }
    let columns = op
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
    op: &'a PhysicalScanOp,
    required: &str,
) -> Option<(usize, &'a DataType)> {
    let table_pos = op
        .table
        .columns
        .iter()
        .position(|column| column.name.eq_ignore_ascii_case(required))?;
    let data_type = op
        .columns
        .iter()
        .find(|column| column.name.eq_ignore_ascii_case(required))
        .map(|column| &column.data_type)
        .or_else(|| {
            op.table
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

fn scan_supports_decode_hint(
    table: &crate::sql::catalog::TableDef,
    required_columns: &[String],
) -> bool {
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
        // Iceberg metadata-table scans are JVM-bridged; the parquet decode
        // hint path does not apply.
        ScanSource::IcebergMetadataTable { .. } => false,
        // IVM delta-scan does not produce stable column-dictionary stats.
        // IMV pinned-version placeholders and target-state scans never
        // produce parquet stats either.
        ScanSource::IcebergDeltaTable { .. }
        | ScanSource::IcebergVersionTable { .. }
        | ScanSource::IcebergMvTargetState { .. } => false,
    }
}

fn scan_supports_min_max_stats(
    table: &crate::sql::catalog::TableDef,
    required_columns: &[String],
) -> bool {
    match &table.source {
        ScanSource::IcebergDataFiles { .. } | ScanSource::StarRocks { .. } => {}
        // Iceberg metadata tables do not produce parquet column statistics.
        ScanSource::IcebergMetadataTable { .. } => return false,
        // IVM delta-scan, IMV pinned-version placeholders, and target-state
        // scans are synthetic; no parquet stats.
        ScanSource::IcebergDeltaTable { .. }
        | ScanSource::IcebergVersionTable { .. }
        | ScanSource::IcebergMvTargetState { .. } => {
            return false;
        }
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

fn format_expr(expr: &TypedExpr) -> String {
    format_expr_kind(&expr.kind)
}

fn format_expr_kind(kind: &ExprKind) -> String {
    match kind {
        ExprKind::ColumnRef {
            qualifier, column, ..
        } => match qualifier {
            Some(q) => format!("{q}.{column}"),
            None => column.clone(),
        },
        ExprKind::LambdaParamRef { name, .. } => name.clone(),
        ExprKind::Literal(lit) => match lit {
            LiteralValue::Null => "NULL".to_string(),
            LiteralValue::Bool(b) => b.to_string(),
            LiteralValue::Int(n) => n.to_string(),
            LiteralValue::LargeInt(n) => n.to_string(),
            LiteralValue::Float(f) => f.to_string(),
            LiteralValue::Decimal(d) => d.clone(),
            LiteralValue::String(s) => format!("'{s}'"),
            LiteralValue::Binary(bytes) => format!("X'{}'", hex::encode_upper(bytes)),
        },
        ExprKind::BinaryOp { left, op, right } => {
            let op_str = match op {
                BinOp::Add => "+",
                BinOp::Sub => "-",
                BinOp::Mul => "*",
                BinOp::Div => "/",
                BinOp::Mod => "%",
                BinOp::Eq => "=",
                BinOp::Ne => "!=",
                BinOp::Lt => "<",
                BinOp::Le => "<=",
                BinOp::Gt => ">",
                BinOp::Ge => ">=",
                BinOp::EqForNull => "<=>",
                BinOp::And => "AND",
                BinOp::Or => "OR",
            };
            format!("{} {op_str} {}", format_expr(left), format_expr(right))
        }
        ExprKind::UnaryOp { op, expr } => {
            let op_str = match op {
                UnOp::Not => "NOT",
                UnOp::Negate => "-",
                UnOp::BitwiseNot => "~",
            };
            format!("{op_str} {}", format_expr(expr))
        }
        ExprKind::FunctionCall {
            name,
            args,
            distinct,
            ..
        } => {
            let args_str: Vec<String> = args.iter().map(format_expr).collect();
            let distinct_str = if *distinct { "DISTINCT " } else { "" };
            format!("{name}({distinct_str}{})", args_str.join(", "))
        }
        ExprKind::LambdaFunction { params, body } => {
            let params = params
                .iter()
                .map(|param| param.name.as_str())
                .collect::<Vec<_>>()
                .join(", ");
            format!("({params}) -> {}", format_expr(body))
        }
        ExprKind::AggregateCall {
            name,
            args,
            distinct,
            ..
        } => {
            let args_str: Vec<String> = args.iter().map(format_expr).collect();
            let distinct_str = if *distinct { "DISTINCT " } else { "" };
            format!("{name}({distinct_str}{})", args_str.join(", "))
        }
        ExprKind::Cast { expr, target } => {
            format!("CAST({} AS {target:?})", format_expr(expr))
        }
        ExprKind::IsNull { expr, negated } => {
            let not = if *negated { " NOT" } else { "" };
            format!("{} IS{not} NULL", format_expr(expr))
        }
        ExprKind::InList {
            expr,
            list,
            negated,
        } => {
            let not = if *negated { " NOT" } else { "" };
            let items: Vec<String> = list.iter().map(format_expr).collect();
            format!("{}{not} IN ({})", format_expr(expr), items.join(", "))
        }
        ExprKind::Between {
            expr,
            low,
            high,
            negated,
        } => {
            let not = if *negated { " NOT" } else { "" };
            format!(
                "{}{not} BETWEEN {} AND {}",
                format_expr(expr),
                format_expr(low),
                format_expr(high)
            )
        }
        ExprKind::Like {
            expr,
            pattern,
            negated,
        } => {
            let not = if *negated { " NOT" } else { "" };
            format!("{}{not} LIKE {}", format_expr(expr), format_expr(pattern))
        }
        ExprKind::Case {
            operand,
            when_then,
            else_expr,
        } => {
            let mut s = String::from("CASE");
            if let Some(op) = operand {
                let _ = write!(s, " {}", format_expr(op));
            }
            for (w, t) in when_then {
                let _ = write!(s, " WHEN {} THEN {}", format_expr(w), format_expr(t));
            }
            if let Some(e) = else_expr {
                let _ = write!(s, " ELSE {}", format_expr(e));
            }
            s.push_str(" END");
            s
        }
        ExprKind::IsTruthValue {
            expr,
            value,
            negated,
        } => {
            let not = if *negated { " NOT" } else { "" };
            let val = if *value { "TRUE" } else { "FALSE" };
            format!("{} IS{not} {val}", format_expr(expr))
        }
        ExprKind::Nested(inner) => format_expr(inner),
        ExprKind::WindowCall { name, args, .. } => {
            let args_str: Vec<String> = args.iter().map(format_expr).collect();
            format!("{name}({})", args_str.join(", "))
        }
        ExprKind::SubqueryPlaceholder { id, .. } => format!("<subquery_{id}>"),
        ExprKind::Lambda { params, body } => match params.as_slice() {
            [single] => format!("{} -> {}", single, format_expr(body)),
            many => format!("({}) -> {}", many.join(", "), format_expr(body)),
        },
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field, Fields};

    use super::{
        ExplainLevel, explain_physical_plan, explain_plan, format_boundary_schema_reports,
        format_physical_node, format_stats_trailer,
    };
    use crate::sql::analysis::{ExprKind, JoinKind, OutputColumn, SortItem, TypedExpr};
    use crate::sql::catalog::{ColumnDef, ScanSource, TableDef};
    use crate::sql::codegen::boundary_schema::{
        BoundaryKind, BoundarySchemaColumn, BoundarySchemaReport,
    };
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::operator::{
        JoinDistribution, Operator, PhysicalDistributionOp, PhysicalHashJoinOp, PhysicalScanOp,
        PhysicalTopNOp, TopNPhase,
    };
    use crate::sql::optimizer::physical_plan::{
        JoinExecutionDistribution, PhysicalPlanNode, PlanExecutionProps,
    };
    use crate::sql::optimizer::property::DistributionSpec;
    use crate::sql::optimizer::property::PhysicalPropertySet;
    use crate::sql::optimizer::statistics::{Confidence, Statistics};
    use crate::sql::planner::plan::{
        AggregateStateMergeNode, LogicalPlan, ScanVariantColumn, ValuesNode,
    };

    fn explain_logical_plan_for_test(plan: &LogicalPlan) -> String {
        explain_plan(plan, ExplainLevel::Normal).join("\n")
    }

    #[test]
    fn format_boundary_schema_reports_includes_root_fragment_and_columns() {
        let reports = vec![BoundarySchemaReport {
            fragment_id: None,
            node_id: -1,
            boundary_kind: BoundaryKind::ResultRoot,
            columns: vec![BoundarySchemaColumn {
                slot_id: 1,
                name: "k1".to_string(),
                arrow_type: DataType::Int64,
                logical_type: None,
                nullable: false,
            }],
        }];

        let lines = format_boundary_schema_reports(&reports);

        assert_eq!(
            lines,
            vec![
                "Boundary Schemas:".to_string(),
                "  Fragment root RESULT_ROOT node=-1:".to_string(),
                "    slot=1 name=k1 arrow=Int64 logical=<none> nullable=false".to_string(),
            ]
        );
    }

    #[test]
    fn explain_hash_join_uses_execution_distribution_metadata() {
        let plan = PhysicalPlanNode {
            op: Operator::PhysicalHashJoin(PhysicalHashJoinOp {
                join_type: JoinKind::Inner,
                eq_conditions: vec![],
                other_condition: None,
                distribution: JoinDistribution::Unknown,
            }),
            children: vec![],
            stats: Statistics::default(),
            output_columns: vec![],
            execution_props: PlanExecutionProps {
                output_property: PhysicalPropertySet::any(),
                child_output_properties: vec![],
                join_distribution: Some(JoinExecutionDistribution::Partitioned),
            },
            build_runtime_filters: vec![],
            probe_runtime_filters: vec![],
        };

        let text = explain_physical_plan(&plan, ExplainLevel::Verbose).join("\n");
        assert!(text.contains("HASH JOIN (PARTITIONED, INNER"), "{text}");
        assert!(!text.contains("UNKNOWN"), "{text}");
    }

    #[test]
    fn explain_prints_aggregate_state_merge_evidence() {
        fn empty_values_for_test() -> LogicalPlan {
            LogicalPlan::Values(ValuesNode {
                rows: vec![],
                columns: vec![],
                required_output_columns: None,
            })
        }

        let plan = LogicalPlan::AggregateStateMerge(AggregateStateMergeNode {
            old_input: Box::new(empty_values_for_test()),
            delta_input: Box::new(empty_values_for_test()),
            group_key_names: vec!["region".to_string()],
            aggregate_state_names: vec!["c".to_string()],
            change_op_column: "__change_op".to_string(),
            output_columns: vec![],
        });
        let text = explain_logical_plan_for_test(&plan);
        assert!(text.contains("AggregateStateMerge"), "{text}");
        assert!(text.contains("keys=[region]"), "{text}");
        assert!(text.contains("states=[c]"), "{text}");
    }

    #[test]
    fn starrocks_scan_verbose_explain_reports_min_max_stats_for_supported_required_columns() {
        let column = ColumnDef {
            name: "c_2_0".to_string(),
            data_type: DataType::FixedSizeBinary(16),
            nullable: false,
            write_default: None,
            logical_type: None,
        };
        let plan = PhysicalPlanNode {
            op: Operator::PhysicalScan(PhysicalScanOp {
                database: "db1".to_string(),
                table: TableDef {
                    name: "t3".to_string(),
                    columns: vec![column.clone()],
                    iceberg_row_lineage_metadata_columns: Vec::new(),
                    source: ScanSource::StarRocks {
                        db_id: 0,
                        table_id: 0,
                    },
                },
                alias: None,
                columns: vec![OutputColumn {
                    column_id: ColumnId::UNSET,
                    name: column.name.clone(),
                    data_type: column.data_type.clone(),
                    nullable: column.nullable,
                    is_internal: false,
                }],
                predicates: Vec::new(),
                required_columns: Some(vec![column.name.clone()]),
                dict_columns: vec![],
                variant_columns: vec![],
                mv_rewritten_from: None,
            }),
            children: Vec::new(),
            stats: Statistics {
                output_row_count: 3.0,
                column_statistics: HashMap::new(),
                ..Default::default()
            },
            output_columns: Vec::new(),
            execution_props: crate::sql::optimizer::physical_plan::PlanExecutionProps::default(),
            build_runtime_filters: Vec::new(),
            probe_runtime_filters: Vec::new(),
        };

        let lines = explain_physical_plan(&plan, ExplainLevel::Verbose);

        assert!(
            lines.iter().any(|line| line.contains("min-max stats")),
            "verbose explain lines: {lines:?}"
        );
    }

    #[test]
    fn starrocks_scan_costs_explain_reports_decode_for_string_required_columns() {
        let column = ColumnDef {
            name: "c8".to_string(),
            data_type: DataType::Utf8,
            nullable: true,
            write_default: None,
            logical_type: None,
        };
        let plan = PhysicalPlanNode {
            op: Operator::PhysicalScan(PhysicalScanOp {
                database: "db1".to_string(),
                table: TableDef {
                    name: "all_t0".to_string(),
                    columns: vec![column.clone()],
                    iceberg_row_lineage_metadata_columns: Vec::new(),
                    source: ScanSource::StarRocks {
                        db_id: 0,
                        table_id: 0,
                    },
                },
                alias: None,
                columns: vec![OutputColumn {
                    column_id: ColumnId::UNSET,
                    name: column.name.clone(),
                    data_type: column.data_type.clone(),
                    nullable: column.nullable,
                    is_internal: false,
                }],
                predicates: Vec::new(),
                required_columns: Some(vec![column.name.clone()]),
                dict_columns: vec![],
                variant_columns: vec![],
                mv_rewritten_from: None,
            }),
            children: Vec::new(),
            stats: Statistics {
                output_row_count: 3.0,
                column_statistics: HashMap::new(),
                ..Default::default()
            },
            output_columns: Vec::new(),
            execution_props: crate::sql::optimizer::physical_plan::PlanExecutionProps::default(),
            build_runtime_filters: Vec::new(),
            probe_runtime_filters: Vec::new(),
        };

        let lines = explain_physical_plan(&plan, ExplainLevel::Costs);

        assert!(
            lines.iter().any(|line| line.contains("Decode")),
            "costs explain lines: {lines:?}"
        );
    }

    #[test]
    fn physical_decode_explain_prints_dict_to_string_mapping() {
        use crate::sql::column_id::ColumnId;
        use crate::sql::optimizer::operator::PhysicalDecodeOp;
        use crate::sql::planner::plan::DecodeMapping;

        let scan = PhysicalPlanNode {
            op: Operator::PhysicalScan(PhysicalScanOp {
                database: "db1".to_string(),
                table: TableDef {
                    name: "t1".to_string(),
                    columns: Vec::new(),
                    iceberg_row_lineage_metadata_columns: Vec::new(),
                    source: ScanSource::StarRocks {
                        db_id: 0,
                        table_id: 0,
                    },
                },
                alias: None,
                columns: Vec::new(),
                predicates: Vec::new(),
                required_columns: None,
                dict_columns: vec![],
                variant_columns: vec![],
                mv_rewritten_from: None,
            }),
            children: Vec::new(),
            stats: Statistics {
                output_row_count: 10.0,
                column_statistics: HashMap::new(),
                ..Default::default()
            },
            output_columns: Vec::new(),
            execution_props: crate::sql::optimizer::physical_plan::PlanExecutionProps::default(),
            build_runtime_filters: Vec::new(),
            probe_runtime_filters: Vec::new(),
        };
        let decode = PhysicalPlanNode {
            op: Operator::PhysicalDecode(PhysicalDecodeOp {
                mappings: vec![DecodeMapping {
                    source_column_id: ColumnId::new_for_test(1),
                    output_column_id: ColumnId::new_for_test(2),
                    dict_column: "d".to_string(),
                    string_column: "s".to_string(),
                }],
                output_columns: Vec::new(),
            }),
            children: vec![scan],
            stats: Statistics {
                output_row_count: 10.0,
                column_statistics: HashMap::new(),
                ..Default::default()
            },
            output_columns: Vec::new(),
            execution_props: crate::sql::optimizer::physical_plan::PlanExecutionProps::default(),
            build_runtime_filters: Vec::new(),
            probe_runtime_filters: Vec::new(),
        };

        let lines = explain_physical_plan(&decode, ExplainLevel::Normal);
        let output = lines.join("\n");
        assert!(
            output.contains("DECODE [d->s]"),
            "expected DECODE [d->s] line, got:\n{output}"
        );
    }

    #[test]
    fn physical_distribution_explain_prints_hash_source() {
        let node = PhysicalPlanNode {
            op: Operator::PhysicalDistribution(PhysicalDistributionOp {
                spec: DistributionSpec::shuffle_agg([ColumnId(1)]),
            }),
            children: Vec::new(),
            stats: Statistics {
                output_row_count: 0.0,
                column_statistics: HashMap::new(),
                ..Default::default()
            },
            output_columns: Vec::new(),
            execution_props: crate::sql::optimizer::physical_plan::PlanExecutionProps::default(),
            build_runtime_filters: Vec::new(),
            probe_runtime_filters: Vec::new(),
        };

        let mut lines = Vec::new();
        format_physical_node(&node, ExplainLevel::Normal, 0, &mut lines);
        let output = lines.join("\n");
        assert!(
            output.contains("HASH EXCHANGE (source: ShuffleAgg, hash: [c1])"),
            "explain output was:\n{output}"
        );
    }

    #[test]
    fn partial_topn_explain_uses_local_label() {
        let scan = build_minimal_scan_plan_for_explain_test();
        let node = PhysicalPlanNode {
            op: Operator::PhysicalTopN(PhysicalTopNOp {
                items: vec![SortItem {
                    expr: TypedExpr {
                        kind: ExprKind::ColumnRef {
                            column_id: ColumnId(1),
                            qualifier: None,
                            column: "id".to_string(),
                        },
                        data_type: DataType::Int64,
                        nullable: false,
                    },
                    asc: true,
                    nulls_first: false,
                }],
                limit: Some(10),
                offset: Some(0),
                phase: TopNPhase::Partial,
                is_split: false,
            }),
            children: vec![scan],
            stats: Statistics {
                output_row_count: 10.0,
                column_statistics: HashMap::new(),
                ..Default::default()
            },
            output_columns: Vec::new(),
            execution_props: crate::sql::optimizer::physical_plan::PlanExecutionProps::default(),
            build_runtime_filters: Vec::new(),
            probe_runtime_filters: Vec::new(),
        };

        let output = explain_physical_plan(&node, ExplainLevel::Verbose).join("\n");

        assert!(
            output.starts_with("LOCAL TOP-N (limit=10, offset=0)"),
            "explain output was:\n{output}"
        );
        assert!(
            !output.starts_with("TOP-N ("),
            "partial TopN must not look like a global TOP-N:\n{output}"
        );
    }

    fn build_minimal_scan_plan_for_explain_test() -> PhysicalPlanNode {
        let column = ColumnDef {
            name: "id".to_string(),
            data_type: DataType::Int64,
            nullable: false,
            write_default: None,
            logical_type: None,
        };
        PhysicalPlanNode {
            op: Operator::PhysicalScan(PhysicalScanOp {
                database: "db1".to_string(),
                table: TableDef {
                    name: "t1".to_string(),
                    columns: vec![column.clone()],
                    iceberg_row_lineage_metadata_columns: Vec::new(),
                    source: ScanSource::StarRocks {
                        db_id: 0,
                        table_id: 0,
                    },
                },
                alias: None,
                columns: vec![OutputColumn {
                    column_id: ColumnId::UNSET,
                    name: column.name.clone(),
                    data_type: column.data_type.clone(),
                    nullable: column.nullable,
                    is_internal: false,
                }],
                predicates: Vec::new(),
                required_columns: Some(vec![column.name.clone()]),
                dict_columns: vec![],
                variant_columns: vec![],
                mv_rewritten_from: None,
            }),
            children: Vec::new(),
            stats: Statistics {
                output_row_count: 1.0,
                column_statistics: HashMap::new(),
                ..Default::default()
            },
            output_columns: Vec::new(),
            execution_props: crate::sql::optimizer::physical_plan::PlanExecutionProps::default(),
            build_runtime_filters: Vec::new(),
            probe_runtime_filters: Vec::new(),
        }
    }

    #[test]
    fn analyze_level_matches_verbose_for_exact_stats() {
        let mut plan = build_minimal_scan_plan_for_explain_test();
        plan.stats.row_count_confidence = Confidence::Exact;
        let verbose = explain_physical_plan(&plan, ExplainLevel::Verbose);
        let analyze = explain_physical_plan(&plan, ExplainLevel::Analyze);
        // With exact row counts, Analyze still shares Verbose body text.
        // Header is added by explain_analyze_query later, not here.
        assert_eq!(
            verbose, analyze,
            "Analyze should match Verbose when confidence has no visible suffix"
        );
    }

    #[test]
    fn explain_variant_path_columns() {
        let mut plan = build_minimal_scan_plan_for_explain_test();
        let Operator::PhysicalScan(op) = &mut plan.op else {
            panic!("expected scan plan");
        };
        op.variant_columns.push(ScanVariantColumn {
            source_column_id: ColumnId::new_for_test(1),
            source_column: "v".to_string(),
            synthetic_column_id: ColumnId::new_for_test(2),
            synthetic_column: "__nr_var_v_0".to_string(),
            canonical_path: "$.a".to_string(),
            requested_type: DataType::Int64,
            strict: true,
        });

        let expected = "variant columns: __nr_var_v_0 := variant_get(v, '$.a', 'bigint')";

        for level in [
            ExplainLevel::Verbose,
            ExplainLevel::Costs,
            ExplainLevel::Analyze,
        ] {
            let output = explain_physical_plan(&plan, level).join("\n");
            assert!(
                output.contains(expected),
                "{level:?} explain should contain variant path columns, got:\n{output}"
            );
        }

        let normal = explain_physical_plan(&plan, ExplainLevel::Normal).join("\n");
        assert!(
            !normal.contains("variant columns:"),
            "Normal explain must hide variant path columns, got:\n{normal}"
        );
    }

    #[test]
    fn verbose_scan_explain_reports_complex_required_column_type() {
        let nested_string_array =
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)));
        let column_type = DataType::List(Arc::new(Field::new(
            "item",
            DataType::Struct(Fields::from(vec![
                Arc::new(Field::new("user", DataType::Utf8, true)),
                Arc::new(Field::new("family", DataType::Utf8, true)),
                Arc::new(Field::new("given", nested_string_array.clone(), true)),
                Arc::new(Field::new("prefix", nested_string_array.clone(), true)),
                Arc::new(Field::new("suffix", nested_string_array, true)),
            ])),
            true,
        )));
        let column = ColumnDef {
            name: "name".to_string(),
            data_type: column_type,
            nullable: true,
            write_default: None,
            logical_type: None,
        };
        let plan = PhysicalPlanNode {
            op: Operator::PhysicalScan(PhysicalScanOp {
                database: "db1".to_string(),
                table: TableDef {
                    name: "ice_tbl".to_string(),
                    columns: vec![column.clone()],
                    iceberg_row_lineage_metadata_columns: Vec::new(),
                    source: ScanSource::StarRocks {
                        db_id: 0,
                        table_id: 0,
                    },
                },
                alias: None,
                columns: vec![OutputColumn {
                    column_id: ColumnId::UNSET,
                    name: column.name.clone(),
                    data_type: column.data_type.clone(),
                    nullable: column.nullable,
                    is_internal: false,
                }],
                predicates: Vec::new(),
                required_columns: Some(vec![column.name.clone()]),
                dict_columns: vec![],
                variant_columns: vec![],
                mv_rewritten_from: None,
            }),
            children: Vec::new(),
            stats: Statistics {
                output_row_count: 3.0,
                column_statistics: HashMap::new(),
                ..Default::default()
            },
            output_columns: Vec::new(),
            execution_props: crate::sql::optimizer::physical_plan::PlanExecutionProps::default(),
            build_runtime_filters: Vec::new(),
            probe_runtime_filters: Vec::new(),
        };

        let lines = explain_physical_plan(&plan, ExplainLevel::Verbose);

        assert!(
            lines.iter().any(|line| line.contains(
                "Pruned type: 1 <-> [ARRAY<struct<`user` varchar(1073741824), `family` varchar(1073741824), `given` array<varchar(1073741824)>, `prefix` array<varchar(1073741824)>, `suffix` array<varchar(1073741824)>>>]"
            )),
            "verbose explain lines: {lines:?}"
        );
    }

    #[test]
    fn stats_trailer_emits_rows_question_mark_for_unset_stats() {
        let stats = Statistics {
            output_row_count: 0.0,
            column_statistics: HashMap::new(),
            ..Default::default()
        };
        assert_eq!(format_stats_trailer(&stats), "stats={rows=?}");
    }

    #[test]
    fn stats_trailer_emits_rows_value_for_positive_estimate() {
        let stats = Statistics {
            output_row_count: 123.7,
            column_statistics: HashMap::new(),
            ..Default::default()
        };
        assert_eq!(format_stats_trailer(&stats), "stats={rows=124}");
    }

    #[test]
    fn stats_trailer_with_conf_emits_estimated_suffix_when_enabled() {
        let stats = Statistics {
            output_row_count: 123.7,
            row_count_confidence: Confidence::Estimated,
            column_statistics: HashMap::new(),
        };
        assert_eq!(
            super::format_stats_trailer_with_conf(&stats, true),
            "stats={rows=124 conf=estimated}"
        );
    }

    #[test]
    fn stats_trailer_with_conf_emits_fallback_suffix_when_enabled() {
        let stats = Statistics {
            output_row_count: 0.0,
            row_count_confidence: Confidence::Fallback,
            column_statistics: HashMap::new(),
        };
        assert_eq!(
            super::format_stats_trailer_with_conf(&stats, true),
            "stats={rows=? conf=fallback}"
        );
    }

    #[test]
    fn stats_trailer_with_conf_omits_exact_suffix_and_wrapper_stays_plain() {
        let stats = Statistics {
            output_row_count: 10.0,
            row_count_confidence: Confidence::Exact,
            column_statistics: HashMap::new(),
        };
        assert_eq!(
            super::format_stats_trailer_with_conf(&stats, true),
            "stats={rows=10}"
        );
        assert_eq!(format_stats_trailer(&stats), "stats={rows=10}");
    }

    #[test]
    fn stats_trailer_emits_question_mark_for_nan() {
        let stats = Statistics {
            output_row_count: f64::NAN,
            column_statistics: HashMap::new(),
            ..Default::default()
        };
        assert_eq!(format_stats_trailer(&stats), "stats={rows=?}");
    }

    #[test]
    fn stats_trailer_emits_question_mark_for_negative() {
        let stats = Statistics {
            output_row_count: -1.0,
            column_statistics: HashMap::new(),
            ..Default::default()
        };
        assert_eq!(format_stats_trailer(&stats), "stats={rows=?}");
    }

    #[test]
    fn stats_trailer_caps_overflow_instead_of_i64_max() {
        let inf = Statistics {
            output_row_count: f64::INFINITY,
            ..Default::default()
        };
        assert_eq!(format_stats_trailer(&inf), "stats={rows=>=1e15}");

        let huge = Statistics {
            output_row_count: 9.5e18,
            ..Default::default()
        };
        assert_eq!(format_stats_trailer(&huge), "stats={rows=>=1e15}");

        let ok = Statistics {
            output_row_count: 1234.0,
            ..Default::default()
        };
        assert_eq!(format_stats_trailer(&ok), "stats={rows=1234}");
    }

    #[test]
    fn verbose_explain_includes_stats_trailer_on_scan() {
        let plan = build_minimal_scan_plan_for_explain_test();
        let lines = explain_physical_plan(&plan, ExplainLevel::Verbose);
        let scan_line = lines
            .iter()
            .find(|l| l.contains("SCAN"))
            .expect("scan line");
        assert!(
            scan_line.contains("stats={rows="),
            "scan node should end with stats trailer: {scan_line}"
        );
    }

    #[test]
    fn costs_and_analyze_include_non_exact_confidence_but_verbose_does_not() {
        let mut plan = build_minimal_scan_plan_for_explain_test();
        plan.stats.row_count_confidence = Confidence::Estimated;

        let verbose = explain_physical_plan(&plan, ExplainLevel::Verbose).join("\n");
        let costs = explain_physical_plan(&plan, ExplainLevel::Costs).join("\n");
        let analyze = explain_physical_plan(&plan, ExplainLevel::Analyze).join("\n");

        assert!(verbose.contains("stats={rows=1}"), "{verbose}");
        assert!(!verbose.contains("conf="), "{verbose}");
        assert!(costs.contains("stats={rows=1 conf=estimated}"), "{costs}");
        assert!(
            analyze.contains("stats={rows=1 conf=estimated}"),
            "{analyze}"
        );
    }

    #[test]
    fn costs_and_analyze_omit_exact_confidence_suffix() {
        let mut plan = build_minimal_scan_plan_for_explain_test();
        plan.stats.row_count_confidence = Confidence::Exact;

        let costs = explain_physical_plan(&plan, ExplainLevel::Costs).join("\n");
        let analyze = explain_physical_plan(&plan, ExplainLevel::Analyze).join("\n");

        assert!(costs.contains("stats={rows=1}"), "{costs}");
        assert!(!costs.contains("conf="), "{costs}");
        assert!(analyze.contains("stats={rows=1}"), "{analyze}");
        assert!(!analyze.contains("conf="), "{analyze}");
    }

    #[test]
    fn normal_level_does_not_include_stats_trailer() {
        let plan = build_minimal_scan_plan_for_explain_test();
        let lines = explain_physical_plan(&plan, ExplainLevel::Normal);
        for line in &lines {
            assert!(
                !line.contains("stats={rows="),
                "Normal level must not include stats trailer: {line}"
            );
        }
    }
}

#[cfg(test)]
mod costs_level_tests {
    use super::*;
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::statistics::{ColumnStatistic, Statistics};
    use std::collections::HashMap;

    #[test]
    fn costs_column_stats_formatting() {
        // Empty map → empty string.
        let empty = Statistics {
            output_row_count: 0.0,
            column_statistics: HashMap::new(),
            ..Default::default()
        };
        assert_eq!(format_column_stats_costs(&empty), "");

        // Normal column: exact format pinned.
        let mut cs = HashMap::new();
        cs.insert(
            ColumnId::new_for_test(1),
            ColumnStatistic {
                min_value: 0.0,
                max_value: 1000.0,
                nulls_fraction: 0.0,
                average_row_size: 8.0,
                distinct_values_count: 1000.0,
                ..Default::default()
            },
        );
        let stats = Statistics {
            output_row_count: 10.0,
            column_statistics: cs,
            ..Default::default()
        };
        let s = format_column_stats_costs(&stats);
        assert_eq!(s, "colstats={col#1[min=0 max=1000 ndv=1000 null_frac=0]}");
    }

    #[test]
    fn fmt_f64_nan_returns_question_mark() {
        assert_eq!(fmt_f64(f64::NAN), "?");
    }

    #[test]
    fn fmt_f64_infinite_returns_inf_labels() {
        assert_eq!(fmt_f64(f64::INFINITY), "+inf");
        assert_eq!(fmt_f64(f64::NEG_INFINITY), "-inf");
    }

    #[test]
    fn format_column_stats_costs_non_finite_ndv_renders_question_mark() {
        let mut cs = HashMap::new();
        cs.insert(
            ColumnId::new_for_test(9),
            ColumnStatistic {
                min_value: 0.0,
                max_value: 1.0,
                nulls_fraction: 0.0,
                average_row_size: 4.0,
                distinct_values_count: f64::INFINITY,
                ..Default::default()
            },
        );
        let stats = Statistics {
            output_row_count: 5.0,
            column_statistics: cs,
            ..Default::default()
        };
        let s = format_column_stats_costs(&stats);
        assert!(
            s.contains("ndv=?"),
            "expected ndv=? for infinite NDV, got: {s}"
        );
    }
}

#[cfg(test)]
mod rf_explain_tests {
    use super::*;
    use crate::sql::optimizer::options::OptimizerOptions;
    use crate::sql::optimizer::runtime_filter_pass::{self, test_support};

    #[test]
    fn explain_shows_build_and_probe_rf() {
        let mut join = test_support::inner_join_two_scans();
        runtime_filter_pass::annotate(&mut join, &OptimizerOptions::default_settings());
        let lines = explain_physical_plan(&join, ExplainLevel::Verbose).join("\n");
        assert!(
            lines.contains("build runtime filters:"),
            "missing build RF; got:\n{lines}"
        );
        assert!(
            lines.contains("filter_id = 0"),
            "missing filter_id; got:\n{lines}"
        );
        assert!(
            lines.contains("probe runtime filters:"),
            "missing probe RF; got:\n{lines}"
        );
    }

    #[test]
    fn explain_normal_level_hides_rf() {
        let mut join = test_support::inner_join_two_scans();
        runtime_filter_pass::annotate(&mut join, &OptimizerOptions::default_settings());
        let lines = explain_physical_plan(&join, ExplainLevel::Normal).join("\n");
        assert!(
            !lines.contains("runtime filters:"),
            "RF must be hidden at Normal level"
        );
    }

    #[test]
    fn logical_explain_formats_apply_and_assert_one_row() {
        use std::collections::HashSet;

        use arrow::datatypes::DataType;

        use crate::sql::analysis::{ExprKind, OutputColumn, TypedExpr};
        use crate::sql::column_id::ColumnId;
        use crate::sql::planner::plan::{
            ApplyKind, ApplyNode, AssertOneRowNode, LogicalPlan, ValuesNode,
        };

        let values = || {
            LogicalPlan::Values(ValuesNode {
                rows: vec![],
                columns: vec![],
                required_output_columns: None,
            })
        };
        let plan = LogicalPlan::Apply(ApplyNode {
            left: Box::new(values()),
            right: Box::new(LogicalPlan::AssertOneRow(AssertOneRowNode {
                input: Box::new(values()),
                subquery_text: "select 1".to_string(),
                required_output_columns: None,
            })),
            kind: ApplyKind::Exists { negated: true },
            subquery_expr: TypedExpr {
                kind: ExprKind::ColumnRef {
                    column_id: ColumnId(5),
                    qualifier: None,
                    column: "sq".to_string(),
                },
                data_type: DataType::Boolean,
                nullable: false,
            },
            output_column: OutputColumn {
                column_id: ColumnId(5),
                name: "sq".to_string(),
                data_type: DataType::Boolean,
                nullable: false,
                is_internal: true,
            },
            inner_output_column_id: ColumnId(5),
            correlation_column_ids: vec![ColumnId(1)],
            correlation_conjuncts: vec![],
            residual_predicate: None,
            need_check_max_rows: false,
            use_semi_anti: true,
            uncorrelated_outer_predicate_columns: HashSet::new(),
            required_output_columns: None,
        });

        let mut out = Vec::new();
        super::format_node(&plan, super::ExplainLevel::Normal, 0, &mut out);
        assert!(
            out.iter().any(
                |line| line.contains("APPLY (NOT EXISTS, correlated=true, use_semi_anti=true)")
            ),
            "missing APPLY line: {out:?}"
        );
        assert!(
            out.iter().any(|line| line.contains("ASSERT ONE ROW")),
            "missing ASSERT ONE ROW line: {out:?}"
        );
    }
}
