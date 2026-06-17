//! EXPLAIN plan formatter for logical plans and shared expression formatting.

use std::fmt::Write;

use crate::sql::analysis::{BinOp, ExprKind, JoinKind, LiteralValue, ProjectItem, TypedExpr, UnOp};
use crate::sql::catalog::ScanSource;
use crate::sql::planner::plan::{ApplyKind, LogicalPlanNode, LogicalPlanNodeKind};

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
pub(crate) fn explain_plan(plan: &LogicalPlanNode, level: ExplainLevel) -> Vec<String> {
    let mut out = Vec::new();
    format_node(plan, level, 0, &mut out);
    out
}

#[allow(dead_code)]
fn format_node(plan: &LogicalPlanNode, level: ExplainLevel, indent: usize, out: &mut Vec<String>) {
    let pad = "  ".repeat(indent);
    match &plan.kind {
        LogicalPlanNodeKind::Scan(node) => {
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
        LogicalPlanNodeKind::Filter(node) => {
            out.push(format!("{pad}FILTER"));
            out.push(format!(
                "{pad}  predicate: {}",
                format_expr(&node.predicate)
            ));
            format_node(plan.unary_input(), level, indent + 1, out);
        }
        LogicalPlanNodeKind::Project(node) => {
            let items: Vec<String> = node.items.iter().map(format_project_item).collect();
            out.push(format!("{pad}PROJECT [{}]", items.join(", ")));
            format_node(plan.unary_input(), level, indent + 1, out);
        }
        LogicalPlanNodeKind::Aggregate(node) => {
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
        LogicalPlanNodeKind::Join(node) => {
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
        LogicalPlanNodeKind::Sort(node) => {
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
            format_node(plan.unary_input(), level, indent + 1, out);
        }
        LogicalPlanNodeKind::Limit(node) => {
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
        LogicalPlanNodeKind::Union(node) => {
            let kind = if node.all { "UNION ALL" } else { "UNION" };
            out.push(format!("{pad}{kind}"));
            for input in &plan.children {
                format_node(input, level, indent + 1, out);
            }
        }
        LogicalPlanNodeKind::Intersect(_) => {
            out.push(format!("{pad}INTERSECT"));
            for input in &plan.children {
                format_node(input, level, indent + 1, out);
            }
        }
        LogicalPlanNodeKind::Except(_) => {
            out.push(format!("{pad}EXCEPT"));
            for input in &plan.children {
                format_node(input, level, indent + 1, out);
            }
        }
        LogicalPlanNodeKind::Window(node) => {
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
            format_node(plan.unary_input(), level, indent + 1, out);
        }
        LogicalPlanNodeKind::Values(node) => {
            out.push(format!("{pad}VALUES ({} rows)", node.rows.len()));
        }
        LogicalPlanNodeKind::GenerateSeries(node) => {
            out.push(format!(
                "{pad}GENERATE_SERIES({}, {}, {})",
                node.start, node.end, node.step
            ));
        }
        LogicalPlanNodeKind::TableFunction(node) => {
            let join_type = if node.is_left_join { "LEFT" } else { "CROSS" };
            out.push(format!(
                "{pad}TABLE_FUNCTION [{} {}]",
                join_type,
                node.function_name.to_uppercase()
            ));
            format_node(plan.unary_input(), level, indent + 1, out);
        }
        LogicalPlanNodeKind::Repeat(node) => {
            out.push(format!(
                "{pad}REPEAT ({} grouping sets)",
                node.grouping_ids.len()
            ));
            format_node(plan.unary_input(), level, indent + 1, out);
        }
        LogicalPlanNodeKind::CTEAnchor(node) => {
            out.push(format!("{pad}CTE_ANCHOR(cte_id={})", node.cte_id));
            format_node(plan.child(0), level, indent + 1, out);
            format_node(plan.child(1), level, indent + 1, out);
        }
        LogicalPlanNodeKind::CTEProduce(node) => {
            out.push(format!("{pad}CTE_PRODUCE(cte_id={})", node.cte_id));
            format_node(plan.unary_input(), level, indent + 1, out);
        }
        LogicalPlanNodeKind::CTEConsume(node) => {
            out.push(format!("{pad}CTE_CONSUME(cte_id={})", node.cte_id));
        }
        LogicalPlanNodeKind::Decode(node) => {
            let pairs: Vec<String> = node
                .mappings
                .iter()
                .map(|m| format!("{}->{}", m.dict_column, m.string_column))
                .collect();
            out.push(format!("{pad}DECODE [{}]", pairs.join(", ")));
            format_node(plan.unary_input(), level, indent + 1, out);
        }
        LogicalPlanNodeKind::AggregateStateMerge(node) => {
            out.push(format!(
                "{}AggregateStateMerge keys=[{}] states=[{}] change_op={}",
                pad,
                node.group_key_names.join(","),
                node.aggregate_state_names.join(","),
                node.change_op_column
            ));
            format_node(plan.left(), level, indent + 1, out);
            format_node(plan.right(), level, indent + 1, out);
        }
        LogicalPlanNodeKind::Apply(node) => {
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
        LogicalPlanNodeKind::AssertOneRow(_) => {
            out.push(format!("{pad}ASSERT ONE ROW"));
            format_node(plan.unary_input(), level, indent + 1, out);
        }
        LogicalPlanNodeKind::ImvDelta(_) | LogicalPlanNodeKind::ImvVersion(_) => {
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

    use super::{ExplainLevel, explain_plan, format_expr, format_project_item};
    use crate::sql::analysis::{
        BinOp, ExprKind, LiteralValue, OutputColumn, ProjectItem, TypedExpr,
    };
    use crate::sql::column_id::ColumnId;
    use crate::sql::planner::plan::{
        ApplyKind, LogicalAggregateStateMergeNode, LogicalApplyNode, LogicalAssertOneRowNode,
        LogicalPlanNode, LogicalPlanNodeKind, LogicalValuesNode,
    };

    fn empty_values_for_test() -> LogicalPlanNode {
        LogicalPlanNode::new(
            LogicalPlanNodeKind::Values(LogicalValuesNode {
                rows: vec![],
                columns: vec![],
            }),
            vec![],
            None,
        )
    }

    #[test]
    fn logical_explain_prints_aggregate_state_merge_evidence() {
        let plan = LogicalPlanNode::new(
            LogicalPlanNodeKind::AggregateStateMerge(LogicalAggregateStateMergeNode {
                group_key_names: vec!["region".to_string()],
                aggregate_state_names: vec!["c".to_string()],
                change_op_column: "__change_op".to_string(),
                output_columns: vec![],
            }),
            vec![empty_values_for_test(), empty_values_for_test()],
            None,
        );

        let text = explain_plan(&plan, ExplainLevel::Normal).join("\n");

        assert!(text.contains("AggregateStateMerge"), "{text}");
        assert!(text.contains("keys=[region]"), "{text}");
        assert!(text.contains("states=[c]"), "{text}");
    }

    #[test]
    fn logical_explain_formats_apply_and_assert_one_row() {
        let plan = LogicalPlanNode::new(
            LogicalPlanNodeKind::Apply(LogicalApplyNode {
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
                    LogicalPlanNodeKind::AssertOneRow(LogicalAssertOneRowNode {
                        subquery_text: "select 1".to_string(),
                    }),
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
