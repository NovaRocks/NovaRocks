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

//! EXPLAIN plan formatter for logical plans and shared expression formatting.

pub(crate) mod distributed;

use std::fmt::Write;

use crate::sql::analysis::{
    BinOp, ExprKind, JoinKind, LiteralValue, ProjectItem, SortItem, TypedExpr, UnOp,
};
use crate::sql::common::ApplyKind;
use crate::sql::planner::logical::{LogicalPlanKind, LogicalPlanNode};
use crate::sql::planner::payload::{PlanAssertOneRowNode, PlanRowCountAssertion};
use crate::sql::planner::table::ScanSource;

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

/// Format a single LogicalPlanNode tree as EXPLAIN text lines.
#[allow(dead_code)]
pub(crate) fn explain_plan_checked(
    plan: &LogicalPlanNode,
    level: ExplainLevel,
) -> Result<Vec<String>, String> {
    Ok(explain_plan_unchecked(plan, level))
}

#[allow(dead_code)]
pub(crate) fn explain_plan(plan: &LogicalPlanNode, level: ExplainLevel) -> Vec<String> {
    explain_plan_checked(plan, level).expect("invalid logical plan stage")
}

fn explain_plan_unchecked(plan: &LogicalPlanNode, level: ExplainLevel) -> Vec<String> {
    let mut out = Vec::new();
    format_node(plan, level, 0, &mut out);
    out
}

#[allow(dead_code)]
fn format_node(plan: &LogicalPlanNode, level: ExplainLevel, indent: usize, out: &mut Vec<String>) {
    let pad = "  ".repeat(indent);
    match &plan.kind {
        LogicalPlanKind::Scan(node) => {
            let header = format_shared_plan_node_header(&plan.kind, PlanNodeExplainStage::Logical)
                .expect("Scan is a shared explain node");
            out.push(format!("{pad}0:{header}",));
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
        LogicalPlanKind::Filter(_) => {
            let header = format_shared_plan_node_header(&plan.kind, PlanNodeExplainStage::Logical)
                .expect("Filter is a shared explain node");
            out.push(format!("{pad}{header}"));
            for line in
                format_shared_plan_node_detail_lines(&plan.kind, PlanNodeExplainStage::Logical)
            {
                out.push(format!("{pad}  {line}"));
            }
            format_node(plan.unary_input(), level, indent + 1, out);
        }
        LogicalPlanKind::Project(_) => {
            let header = format_shared_plan_node_header(&plan.kind, PlanNodeExplainStage::Logical)
                .expect("Project is a shared explain node");
            out.push(format!("{pad}{header}"));
            format_node(plan.unary_input(), level, indent + 1, out);
        }
        LogicalPlanKind::Aggregate(node) => {
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
            format_node(plan.unary_input(), level, indent + 1, out);
        }
        LogicalPlanKind::Join(node) => {
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
            format_node(plan.left(), level, indent + 1, out);
            format_node(plan.right(), level, indent + 1, out);
        }
        LogicalPlanKind::Sort(_) => {
            let body = format_shared_plan_node_header(&plan.kind, PlanNodeExplainStage::Logical)
                .expect("Sort is a shared explain node");
            out.push(format!("{pad}{body}"));
            format_node(plan.unary_input(), level, indent + 1, out);
        }
        LogicalPlanKind::Limit(node) => {
            let mut parts = Vec::new();
            if let Some(limit) = node.limit {
                parts.push(format!("limit={limit}"));
            }
            if let Some(offset) = node.offset {
                parts.push(format!("offset={offset}"));
            }
            out.push(format!("{pad}LIMIT [{}]", parts.join(", ")));
            format_node(plan.unary_input(), level, indent + 1, out);
        }
        LogicalPlanKind::Union(node) => {
            let kind = if node.all { "UNION ALL" } else { "UNION" };
            out.push(format!("{pad}{kind}"));
            for input in &plan.children {
                format_node(input, level, indent + 1, out);
            }
        }
        LogicalPlanKind::Intersect(_) => {
            out.push(format!("{pad}INTERSECT"));
            for input in &plan.children {
                format_node(input, level, indent + 1, out);
            }
        }
        LogicalPlanKind::Except(_) => {
            out.push(format!("{pad}EXCEPT"));
            for input in &plan.children {
                format_node(input, level, indent + 1, out);
            }
        }
        LogicalPlanKind::Window(_) => {
            let header = format_shared_plan_node_header(&plan.kind, PlanNodeExplainStage::Logical)
                .expect("Window is a shared explain node");
            out.push(format!("{pad}{header}"));
            format_node(plan.unary_input(), level, indent + 1, out);
        }
        LogicalPlanKind::Values(_) => {
            let body = format_shared_plan_node_header(&plan.kind, PlanNodeExplainStage::Logical)
                .expect("Values is a shared explain node");
            out.push(format!("{pad}{body}"));
        }
        LogicalPlanKind::GenerateSeries(_) => {
            let body = format_shared_plan_node_header(&plan.kind, PlanNodeExplainStage::Logical)
                .expect("GenerateSeries is a shared explain node");
            out.push(format!("{pad}{body}"));
        }
        LogicalPlanKind::TableFunction(_) => {
            let body = format_shared_plan_node_header(&plan.kind, PlanNodeExplainStage::Logical)
                .expect("TableFunction is a shared explain node");
            out.push(format!("{pad}{body}"));
            format_node(plan.unary_input(), level, indent + 1, out);
        }
        LogicalPlanKind::Repeat(_) => {
            let body = format_shared_plan_node_header(&plan.kind, PlanNodeExplainStage::Logical)
                .expect("Repeat is a shared explain node");
            out.push(format!("{pad}{body}"));
            format_node(plan.unary_input(), level, indent + 1, out);
        }
        LogicalPlanKind::CTEAnchor(node) => {
            out.push(format!("{pad}CTE_ANCHOR(cte_id={})", node.cte_id));
            format_node(plan.child(0), level, indent + 1, out);
            format_node(plan.child(1), level, indent + 1, out);
        }
        LogicalPlanKind::CTEProduce(node) => {
            out.push(format!("{pad}CTE_PRODUCE(cte_id={})", node.cte_id));
            format_node(plan.unary_input(), level, indent + 1, out);
        }
        LogicalPlanKind::CTEConsume(node) => {
            out.push(format!("{pad}CTE_CONSUME(cte_id={})", node.cte_id));
        }
        LogicalPlanKind::Apply(node) => {
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
            format_node(plan.left(), level, indent + 1, out);
            format_node(plan.right(), level, indent + 1, out);
        }
        LogicalPlanKind::AssertOneRow(_) => {
            let body = format_shared_plan_node_header(&plan.kind, PlanNodeExplainStage::Logical)
                .expect("AssertOneRow is a shared explain node");
            out.push(format!("{pad}{body}"));
            format_node(plan.unary_input(), level, indent + 1, out);
        }
        LogicalPlanKind::ImvDelta(_) | LogicalPlanKind::ImvVersion(_) => {
            panic!("imv marker leaked into non-IMV plan");
        }
    }
}

fn logical_scan_source_label(source: &ScanSource) -> Option<String> {
    match source {
        ScanSource::IcebergDeltaTable {
            from_snapshot_id,
            to_snapshot_id,
            ..
        } => Some(format!(
            "IcebergDeltaTable from_snapshot_id={from_snapshot_id} to_snapshot_id={to_snapshot_id}"
        )),
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
        ScanSource::IcebergMvTargetLocator(scan) => Some(format!(
            "IcebergMvTargetLocator target={} apply_key={}{}",
            scan.fqn(),
            scan.apply_key_column,
            scan.branch_id_column
                .as_deref()
                .map(|column| format!(" branch_id={column}"))
                .unwrap_or_default()
        )),
        _ => None,
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PlanNodeExplainStage {
    Logical,
    Distributed,
}

pub(crate) fn format_shared_plan_node_header(
    kind: &LogicalPlanKind,
    stage: PlanNodeExplainStage,
) -> Option<String> {
    match kind {
        LogicalPlanKind::Scan(node) => {
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
        LogicalPlanKind::Filter(_) => Some("FILTER".to_string()),
        LogicalPlanKind::Project(node) => {
            let items = node
                .items
                .iter()
                .map(format_project_item)
                .collect::<Vec<_>>();
            Some(format!("PROJECT [{}]", items.join(", ")))
        }
        LogicalPlanKind::Sort(node) => {
            let items = format_sort_items(&node.items);
            Some(format!("SORT BY [{}]", items.join(", ")))
        }
        LogicalPlanKind::Window(node) => {
            let fns = format_window_exprs(&node.window_exprs, stage);
            Some(format!("WINDOW [{}]", fns.join("; ")))
        }
        LogicalPlanKind::Values(node) => Some(format!("VALUES ({} rows)", node.rows.len())),
        LogicalPlanKind::Repeat(node) => Some(format!(
            "REPEAT ({} grouping sets)",
            node.grouping_ids.len()
        )),
        LogicalPlanKind::GenerateSeries(node) => Some(format!(
            "GENERATE_SERIES({}, {}, {})",
            node.start, node.end, node.step
        )),
        LogicalPlanKind::TableFunction(node) => {
            let join_type = if node.is_left_join { "LEFT" } else { "CROSS" };
            Some(format!(
                "TABLE_FUNCTION [{} {}]",
                join_type,
                node.function_name.to_uppercase()
            ))
        }
        LogicalPlanKind::AssertOneRow(node) => Some(format_assert_one_row_header(node, stage)),
        _ => None,
    }
}

pub(crate) fn format_assert_one_row_header(
    node: &PlanAssertOneRowNode,
    stage: PlanNodeExplainStage,
) -> String {
    if matches!(stage, PlanNodeExplainStage::Logical) {
        return "ASSERT ONE ROW".to_string();
    }
    let relation = format_row_count_assertion(node.assertion);
    let desired = node.desired_num_rows.unwrap_or(1);
    if node.group_key_column_ids.is_empty() {
        return format!("ASSERT NUM ROWS ({relation} {desired})");
    }
    let labels = if node.group_key_labels.is_empty() {
        node.group_key_column_ids
            .iter()
            .map(|column_id| format!("column_{}", column_id.0))
            .collect::<Vec<_>>()
    } else {
        node.group_key_labels.clone()
    };
    format!(
        "ASSERT NUM ROWS (PER KEY {relation} {desired} BY [{}])",
        labels.join(", ")
    )
}

fn format_row_count_assertion(assertion: PlanRowCountAssertion) -> &'static str {
    match assertion {
        PlanRowCountAssertion::Eq => "=",
        PlanRowCountAssertion::Ne => "!=",
        PlanRowCountAssertion::Lt => "<",
        PlanRowCountAssertion::Le => "<=",
        PlanRowCountAssertion::Gt => ">",
        PlanRowCountAssertion::Ge => ">=",
    }
}

pub(crate) fn format_shared_plan_node_detail_lines(
    kind: &LogicalPlanKind,
    _stage: PlanNodeExplainStage,
) -> Vec<String> {
    match kind {
        LogicalPlanKind::Filter(node) => {
            vec![format!("predicate: {}", format_expr(&node.predicate))]
        }
        _ => vec![],
    }
}

pub(crate) fn format_sort_items(items: &[SortItem]) -> Vec<String> {
    items
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
        .collect()
}

pub(crate) fn format_window_exprs(
    exprs: &[crate::sql::planner::payload::WindowExpr],
    stage: PlanNodeExplainStage,
) -> Vec<String> {
    exprs
        .iter()
        .map(|w| {
            let args = w.args.iter().map(format_expr).collect::<Vec<_>>();
            match stage {
                PlanNodeExplainStage::Logical => {
                    let partition = w.partition_by.iter().map(format_expr).collect::<Vec<_>>();
                    let order = w
                        .order_by
                        .iter()
                        .map(|s| {
                            let dir = if s.asc { "ASC" } else { "DESC" };
                            format!("{} {dir}", format_expr(&s.expr))
                        })
                        .collect::<Vec<_>>();
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
                }
                PlanNodeExplainStage::Distributed => {
                    format!("{}({})", w.name, args.join(", "))
                }
            }
        })
        .collect()
}

pub(crate) fn format_expr(expr: &TypedExpr) -> String {
    format_expr_kind(&expr.kind)
}

pub(crate) fn format_project_item(item: &ProjectItem) -> String {
    let expr_str = format_expr(&item.expr);
    if item.output_name == expr_str {
        expr_str
    } else {
        format!("{expr_str} AS {}", item.output_name)
    }
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
            let (display_left, display_right) = if matches!(op, BinOp::Eq | BinOp::EqForNull)
                && matches!(left.kind, ExprKind::Literal(_))
                && matches!(right.kind, ExprKind::ColumnRef { .. })
            {
                (right.as_ref(), left.as_ref())
            } else {
                (left.as_ref(), right.as_ref())
            };
            format!(
                "{} {op_str} {}",
                format_expr(display_left),
                format_expr(display_right)
            )
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
    use std::collections::HashSet;

    use arrow::datatypes::DataType;

    use super::{
        ExplainLevel, PlanNodeExplainStage, explain_plan, format_expr, format_project_item,
        format_shared_plan_node_header,
    };
    use crate::connector::iceberg::scan_model::{IcebergSchemaDef, IcebergTableInfo};
    use crate::sql::analysis::{
        BinOp, ExprKind, LiteralValue, OutputColumn, ProjectItem, SortItem, TypedExpr,
    };
    use crate::sql::column_id::ColumnId;
    use crate::sql::common::ApplyKind;
    use crate::sql::planner::logical::{LogicalApplyNode, LogicalPlanKind, LogicalPlanNode};
    use crate::sql::planner::payload::{
        PlanAssertOneRowNode, PlanFilterNode, PlanProjectNode, PlanScanNode, PlanValuesNode,
        PlanWindowNode, WindowExpr,
    };
    use crate::sql::planner::table::{IcebergMvTargetLocatorScan, ScanSource, TableDef};
    use novarocks_catalog::schema::ColumnDef;

    fn empty_values_for_test() -> LogicalPlanNode {
        LogicalPlanNode::new(
            LogicalPlanKind::Values(PlanValuesNode {
                rows: vec![],
                columns: vec![],
            }),
            vec![],
            None,
        )
    }

    fn output_column(id: u32, name: &str, data_type: DataType, nullable: bool) -> OutputColumn {
        OutputColumn {
            column_id: ColumnId::new_for_test(id),
            name: name.to_string(),
            data_type,
            nullable,
            is_internal: false,
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

    fn test_table_def() -> TableDef {
        TableDef {
            name: "t".to_string(),
            columns: vec![column_def("k", DataType::Int64, false)],
            iceberg_row_lineage_metadata_columns: vec![],
            source: ScanSource::StarRocks {
                db_id: 1,
                table_id: 2,
            },
        }
    }

    fn iceberg_table_info() -> IcebergTableInfo {
        IcebergTableInfo {
            catalog: "ice".to_string(),
            namespace: "db".to_string(),
            table: "orders".to_string(),
            table_uuid: Some("uuid-orders".to_string()),
            current_snapshot_id: Some(200),
            schema_id: 1,
            location: "file:///tmp/ice/db/orders".to_string(),
            schema: IcebergSchemaDef { fields: vec![] },
            serialized_metadata: None,
            serialized_metadata_rows: None,
        }
    }

    fn scan_plan_with_source(table_name: &str, source: ScanSource) -> LogicalPlanNode {
        LogicalPlanNode::new(
            LogicalPlanKind::Scan(PlanScanNode {
                database: "db".to_string(),
                table: TableDef {
                    name: table_name.to_string(),
                    columns: vec![column_def("k", DataType::Int64, false)],
                    iceberg_row_lineage_metadata_columns: vec![],
                    source,
                },
                alias: None,
                columns: vec![output_column(1, "k", DataType::Int64, false)],
                predicates: vec![],
                required_columns: None,
                variant_columns: vec![],
                mv_rewritten_from: None,
            }),
            vec![],
            None,
        )
    }

    fn column_expr(id: u32, qualifier: Option<&str>, name: &str) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: ColumnId::new_for_test(id),
                qualifier: qualifier.map(str::to_string),
                column: name.to_string(),
            },
            data_type: DataType::Int64,
            nullable: false,
        }
    }

    fn int_literal(value: i64) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::Literal(LiteralValue::Int(value)),
            data_type: DataType::Int64,
            nullable: false,
        }
    }

    #[test]
    fn logical_explain_verbose_prints_refresh_scan_sources() {
        let delta_plan = scan_plan_with_source(
            "orders",
            ScanSource::IcebergDeltaTable {
                table: iceberg_table_info(),
                from_snapshot_id: 101,
                to_snapshot_id: 200,
            },
        );

        let delta_normal = explain_plan(&delta_plan, ExplainLevel::Normal).join("\n");
        assert!(!delta_normal.contains("source:"), "{delta_normal}");
        let delta_verbose = explain_plan(&delta_plan, ExplainLevel::Verbose).join("\n");
        assert!(
            delta_verbose
                .contains("source: IcebergDeltaTable from_snapshot_id=101 to_snapshot_id=200"),
            "{delta_verbose}"
        );

        let locator_plan = scan_plan_with_source(
            "pf_mv",
            ScanSource::IcebergMvTargetLocator(IcebergMvTargetLocatorScan {
                catalog: "ice".to_string(),
                database: "db".to_string(),
                table: "pf_mv".to_string(),
                target_table_uuid: "uuid-pf-mv".to_string(),
                target_snapshot_id: Some(99),
                apply_key_column: "__nova_base_row_id".to_string(),
                branch_id_column: Some("__branch_id".to_string()),
            }),
        );

        let locator_normal = explain_plan(&locator_plan, ExplainLevel::Normal).join("\n");
        assert!(!locator_normal.contains("source:"), "{locator_normal}");
        let locator_verbose = explain_plan(&locator_plan, ExplainLevel::Verbose).join("\n");
        assert!(
            locator_verbose.contains(
                "source: IcebergMvTargetLocator target=ice.db.pf_mv apply_key=__nova_base_row_id branch_id=__branch_id"
            ),
            "{locator_verbose}"
        );
    }

    #[test]
    fn logical_explain_formats_apply_and_assert_one_row() {
        let plan = LogicalPlanNode::new(
            LogicalPlanKind::Apply(LogicalApplyNode {
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
            }),
            vec![
                empty_values_for_test(),
                LogicalPlanNode::new(
                    LogicalPlanKind::AssertOneRow(PlanAssertOneRowNode::global_at_most_one(
                        "select 1",
                    )),
                    vec![empty_values_for_test()],
                    None,
                ),
            ],
            None,
        );

        let out = explain_plan(&plan, ExplainLevel::Normal).join("\n");

        assert!(
            out.contains("APPLY (NOT EXISTS, correlated=true, use_semi_anti=true)"),
            "missing APPLY line: {out}"
        );
        assert!(
            out.contains("ASSERT ONE ROW"),
            "missing ASSERT ONE ROW line: {out}"
        );
    }

    #[test]
    fn shared_plan_node_header_formats_unified_pass_through_nodes() {
        let values = LogicalPlanKind::Values(PlanValuesNode {
            rows: vec![vec![], vec![]],
            columns: vec![],
        });
        let assert =
            LogicalPlanKind::AssertOneRow(PlanAssertOneRowNode::global_at_most_one("select 1"));

        assert_eq!(
            format_shared_plan_node_header(&values, PlanNodeExplainStage::Logical),
            Some("VALUES (2 rows)".to_string())
        );
        assert_eq!(
            format_shared_plan_node_header(&values, PlanNodeExplainStage::Distributed),
            Some("VALUES (2 rows)".to_string())
        );
        assert_eq!(
            format_shared_plan_node_header(&assert, PlanNodeExplainStage::Logical),
            Some("ASSERT ONE ROW".to_string())
        );
        assert_eq!(
            format_shared_plan_node_header(&assert, PlanNodeExplainStage::Distributed),
            Some("ASSERT NUM ROWS (<= 1)".to_string())
        );

        let keyed = LogicalPlanKind::AssertOneRow(PlanAssertOneRowNode::per_key_at_most_one(
            "DML change-stream matched row uniqueness",
            vec![crate::sql::column_id::ColumnId::new_for_test(7)],
            vec!["_row_id".to_string()],
            "MOR UPDATE matched target row",
        ));
        assert_eq!(
            format_shared_plan_node_header(&keyed, PlanNodeExplainStage::Distributed),
            Some("ASSERT NUM ROWS (PER KEY <= 1 BY [_row_id])".to_string())
        );
    }

    #[test]
    fn shared_logical_formatter_path_covers_scan_filter_project_and_window() {
        let scan_columns = vec![output_column(1, "k", DataType::Int64, false)];
        let scan = LogicalPlanNode::new(
            LogicalPlanKind::Scan(PlanScanNode {
                database: "test_db".to_string(),
                table: test_table_def(),
                alias: Some("t".to_string()),
                columns: scan_columns,
                predicates: vec![],
                required_columns: None,
                variant_columns: vec![],
                mv_rewritten_from: None,
            }),
            vec![],
            None,
        );
        let predicate = TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(column_expr(1, Some("t"), "k")),
                op: BinOp::Gt,
                right: Box::new(int_literal(10)),
            },
            data_type: DataType::Boolean,
            nullable: false,
        };
        let filter = LogicalPlanNode::new(
            LogicalPlanKind::Filter(PlanFilterNode { predicate }),
            vec![scan],
            None,
        );
        let project = LogicalPlanNode::new(
            LogicalPlanKind::Project(PlanProjectNode {
                items: vec![ProjectItem {
                    expr: column_expr(1, Some("t"), "k"),
                    output_name: "k".to_string(),
                    output_column_id: ColumnId::new_for_test(1),
                }],
                output_qualifier: None,
            }),
            vec![filter],
            None,
        );
        let window = LogicalPlanNode::new(
            LogicalPlanKind::Window(PlanWindowNode {
                window_exprs: vec![WindowExpr {
                    name: "row_number".to_string(),
                    args: vec![],
                    distinct: false,
                    partition_by: vec![column_expr(1, None, "k")],
                    order_by: vec![SortItem {
                        expr: column_expr(1, None, "k"),
                        asc: true,
                        nulls_first: false,
                    }],
                    window_frame: None,
                    result_type: DataType::Int64,
                    output_name: "rn".to_string(),
                    output_column_id: ColumnId::new_for_test(2),
                    ignore_nulls: false,
                }],
                output_columns: vec![
                    output_column(1, "k", DataType::Int64, false),
                    output_column(2, "rn", DataType::Int64, false),
                ],
            }),
            vec![project],
            None,
        );

        assert_eq!(
            format_shared_plan_node_header(&window.kind, PlanNodeExplainStage::Logical),
            Some("WINDOW [row_number() OVER (PARTITION BY k ORDER BY k ASC)]".to_string())
        );
        assert_eq!(
            explain_plan(&window, ExplainLevel::Normal),
            vec![
                "WINDOW [row_number() OVER (PARTITION BY k ORDER BY k ASC)]".to_string(),
                "  PROJECT [t.k AS k]".to_string(),
                "    FILTER".to_string(),
                "      predicate: t.k > 10".to_string(),
                "      0:SCAN test_db.t (alias=t)".to_string(),
            ]
        );
    }

    #[test]
    fn format_expr_prints_column_before_literal_for_equality() {
        let expr = TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(TypedExpr {
                    kind: ExprKind::Literal(LiteralValue::Int(10)),
                    data_type: DataType::Int64,
                    nullable: false,
                }),
                op: BinOp::Eq,
                right: Box::new(TypedExpr {
                    kind: ExprKind::ColumnRef {
                        column_id: ColumnId(42),
                        qualifier: Some("r".to_string()),
                        column: "rk".to_string(),
                    },
                    data_type: DataType::Int64,
                    nullable: false,
                }),
            },
            data_type: DataType::Boolean,
            nullable: false,
        };

        assert_eq!(format_expr(&expr), "r.rk = 10");
    }

    #[test]
    fn format_project_item_keeps_qualified_column_alias() {
        let item = ProjectItem {
            expr: TypedExpr {
                kind: ExprKind::ColumnRef {
                    column_id: ColumnId(1),
                    qualifier: Some("a".to_string()),
                    column: "k".to_string(),
                },
                data_type: DataType::Int64,
                nullable: false,
            },
            output_name: "k".to_string(),
            output_column_id: ColumnId(1),
        };

        assert_eq!(format_project_item(&item), "a.k AS k");
    }

    #[test]
    fn format_project_item_keeps_real_column_alias() {
        let item = ProjectItem {
            expr: TypedExpr {
                kind: ExprKind::ColumnRef {
                    column_id: ColumnId(1),
                    qualifier: None,
                    column: "id".to_string(),
                },
                data_type: DataType::Int64,
                nullable: false,
            },
            output_name: "alias_id".to_string(),
            output_column_id: ColumnId(1),
        };

        assert_eq!(format_project_item(&item), "id AS alias_id");
    }
}
