//! EXPLAIN plan formatter — produces text from LogicalPlan or PhysicalPlan.

use std::fmt::Write;

use crate::sql::cascades::operator::{AggMode, JoinDistribution, Operator};
use crate::sql::cascades::physical_plan::PhysicalPlanNode;
use crate::sql::cascades::property::DistributionSpec;
use crate::sql::ir::{BinOp, ExprKind, JoinKind, LiteralValue, TypedExpr, UnOp};
use crate::sql::plan::{LogicalPlan, QueryPlan};

/// Detail level for EXPLAIN output.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ExplainLevel {
    Normal,
    Verbose,
    Costs,
}

/// Format a QueryPlan (with optional CTE fragments) as EXPLAIN text lines.
pub(crate) fn explain_query_plan(plan: &QueryPlan, level: ExplainLevel) -> Vec<String> {
    let mut lines = Vec::new();
    if !plan.cte_plans.is_empty() {
        for cte in &plan.cte_plans {
            lines.push(format!("  CTE(cte_id={})", cte.cte_id));
            let sub = explain_plan(&cte.plan, level);
            for l in sub {
                lines.push(format!("    {l}"));
            }
        }
        lines.push(String::new());
    }
    let main_lines = explain_plan(&plan.main_plan, level);
    lines.extend(main_lines);
    lines
}

/// Format a single LogicalPlan tree as EXPLAIN text lines.
pub(crate) fn explain_plan(plan: &LogicalPlan, level: ExplainLevel) -> Vec<String> {
    let mut out = Vec::new();
    format_node(plan, level, 0, &mut out);
    out
}

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
            if let Some(ref cols) = node.required_columns {
                if matches!(level, ExplainLevel::Verbose | ExplainLevel::Costs) {
                    out.push(format!("{pad}     columns: {}", cols.join(", ")));
                }
            }
            if !node.predicates.is_empty() {
                let preds: Vec<String> = node.predicates.iter().map(format_expr).collect();
                out.push(format!("{pad}     predicates: {}", preds.join(" AND ")));
            }
        }
        LogicalPlan::Filter(node) => {
            out.push(format!("{pad}FILTER"));
            out.push(format!("{pad}  predicate: {}", format_expr(&node.predicate)));
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
        LogicalPlan::SubqueryAlias(node) => {
            out.push(format!("{pad}SUBQUERY ALIAS [{}]", node.alias));
            format_node(&node.input, level, indent + 1, out);
        }
        LogicalPlan::Repeat(node) => {
            out.push(format!(
                "{pad}REPEAT ({} grouping sets)",
                node.grouping_ids.len()
            ));
            format_node(&node.input, level, indent + 1, out);
        }
        LogicalPlan::CTEConsume(node) => {
            out.push(format!("{pad}CTE_CONSUME(cte_id={})", node.cte_id));
        }
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

fn format_physical_node(
    node: &PhysicalPlanNode,
    level: ExplainLevel,
    indent: usize,
    out: &mut Vec<String>,
) {
    let pad = "  ".repeat(indent);
    let costs_suffix = if matches!(level, ExplainLevel::Costs) {
        format!(" (rows={:.0})", node.stats.output_row_count)
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
                "{pad}SCAN {}.{}{alias}{costs_suffix}",
                op.database, op.table.name
            ));
            if let Some(ref cols) = op.required_columns {
                if matches!(level, ExplainLevel::Verbose | ExplainLevel::Costs) {
                    out.push(format!("{pad}     columns: {}", cols.join(", ")));
                }
            }
            if !op.predicates.is_empty() {
                let preds: Vec<String> = op.predicates.iter().map(format_expr).collect();
                out.push(format!("{pad}     predicates: {}", preds.join(" AND ")));
            }
        }
        Operator::PhysicalFilter(op) => {
            out.push(format!("{pad}FILTER{costs_suffix}"));
            out.push(format!("{pad}  predicate: {}", format_expr(&op.predicate)));
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
            out.push(format!("{pad}PROJECT [{}]{costs_suffix}", items.join(", ")));
            for child in &node.children {
                format_physical_node(child, level, indent + 1, out);
            }
        }
        Operator::PhysicalHashJoin(op) => {
            let dist = match op.distribution {
                JoinDistribution::Shuffle => "SHUFFLE",
                JoinDistribution::Broadcast => "BROADCAST",
                JoinDistribution::Colocate => "COLOCATE",
            };
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
            };
            let eq: Vec<String> = op
                .eq_conditions
                .iter()
                .map(|(l, r)| format!("{} = {}", format_expr(l), format_expr(r)))
                .collect();
            out.push(format!(
                "{pad}HASH JOIN ({dist}, {join_str}, eq: [{}]){costs_suffix}",
                eq.join(", ")
            ));
            if let Some(ref other) = op.other_condition {
                out.push(format!("{pad}  other: {}", format_expr(other)));
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
            };
            out.push(format!("{pad}NEST LOOP JOIN ({join_str}){costs_suffix}"));
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
            let _ = write!(detail, "){costs_suffix}");
            out.push(detail);
            if !aggs.is_empty() {
                out.push(format!("{pad}  aggregations: {}", aggs.join(", ")));
            }
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
                "{pad}SORT BY [{}]{costs_suffix}",
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
                "{pad}LIMIT [{}]{costs_suffix}",
                parts.join(", ")
            ));
            for child in &node.children {
                format_physical_node(child, level, indent + 1, out);
            }
        }
        Operator::PhysicalDistribution(op) => {
            let label = match &op.spec {
                DistributionSpec::Any => "ANY EXCHANGE".to_string(),
                DistributionSpec::Gather => "GATHER EXCHANGE".to_string(),
                DistributionSpec::HashPartitioned(cols) => {
                    let col_names: Vec<String> = cols
                        .iter()
                        .map(|c| match &c.qualifier {
                            Some(q) => format!("{q}.{}", c.column),
                            None => c.column.clone(),
                        })
                        .collect();
                    format!("HASH EXCHANGE (hash: [{}])", col_names.join(", "))
                }
            };
            out.push(format!("{pad}{label}{costs_suffix}"));
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
                "{pad}WINDOW [{}]{costs_suffix}",
                fns.join("; ")
            ));
            for child in &node.children {
                format_physical_node(child, level, indent + 1, out);
            }
        }
        Operator::PhysicalCTEProduce(op) => {
            out.push(format!("{pad}CTE PRODUCE (cte_id={}){costs_suffix}", op.cte_id));
            for child in &node.children {
                format_physical_node(child, level, indent + 1, out);
            }
        }
        Operator::PhysicalCTEConsume(op) => {
            out.push(format!(
                "{pad}CTE CONSUME (cte_id={}){costs_suffix}",
                op.cte_id
            ));
        }
        Operator::PhysicalRepeat(op) => {
            out.push(format!(
                "{pad}REPEAT ({} grouping sets){costs_suffix}",
                op.grouping_ids.len()
            ));
            for child in &node.children {
                format_physical_node(child, level, indent + 1, out);
            }
        }
        Operator::PhysicalUnion(op) => {
            let kind = if op.all { "UNION ALL" } else { "UNION" };
            out.push(format!("{pad}{kind}{costs_suffix}"));
            for child in &node.children {
                format_physical_node(child, level, indent + 1, out);
            }
        }
        Operator::PhysicalIntersect(_) => {
            out.push(format!("{pad}INTERSECT{costs_suffix}"));
            for child in &node.children {
                format_physical_node(child, level, indent + 1, out);
            }
        }
        Operator::PhysicalExcept(_) => {
            out.push(format!("{pad}EXCEPT{costs_suffix}"));
            for child in &node.children {
                format_physical_node(child, level, indent + 1, out);
            }
        }
        Operator::PhysicalValues(op) => {
            out.push(format!("{pad}VALUES ({} rows){costs_suffix}", op.rows.len()));
        }
        Operator::PhysicalGenerateSeries(op) => {
            out.push(format!(
                "{pad}GENERATE_SERIES({}, {}, {}){costs_suffix}",
                op.start, op.end, op.step
            ));
        }
        Operator::PhysicalSubqueryAlias(op) => {
            out.push(format!("{pad}SUBQUERY ALIAS [{}]{costs_suffix}", op.alias));
            for child in &node.children {
                format_physical_node(child, level, indent + 1, out);
            }
        }
        // Logical operators should not appear in physical plan
        _ => {
            out.push(format!("{pad}<logical operator>{costs_suffix}"));
        }
    }
}

fn format_expr(expr: &TypedExpr) -> String {
    format_expr_kind(&expr.kind)
}

fn format_expr_kind(kind: &ExprKind) -> String {
    match kind {
        ExprKind::ColumnRef { qualifier, column } => match qualifier {
            Some(q) => format!("{q}.{column}"),
            None => column.clone(),
        },
        ExprKind::Literal(lit) => match lit {
            LiteralValue::Null => "NULL".to_string(),
            LiteralValue::Bool(b) => b.to_string(),
            LiteralValue::Int(n) => n.to_string(),
            LiteralValue::Float(f) => f.to_string(),
            LiteralValue::Decimal(d) => d.clone(),
            LiteralValue::String(s) => format!("'{s}'"),
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
            name, args, distinct, ..
        } => {
            let args_str: Vec<String> = args.iter().map(format_expr).collect();
            let distinct_str = if *distinct { "DISTINCT " } else { "" };
            format!("{name}({distinct_str}{})", args_str.join(", "))
        }
        ExprKind::AggregateCall {
            name, args, distinct, ..
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
    }
}
