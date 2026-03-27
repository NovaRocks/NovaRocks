//! Logical Planner — converts [`ResolvedQuery`] into [`LogicalPlan`].
//!
//! This is a structural transformation that builds a relational algebra tree
//! from the analyzed query IR.  A future optimizer would rewrite this tree
//! before it reaches the Thrift emitter.

use crate::sql::ir::*;
use crate::sql::plan::*;

// ---------------------------------------------------------------------------
// Public entry
// ---------------------------------------------------------------------------

/// Convert a fully-analyzed query into a logical plan tree.
pub(crate) fn plan(resolved: ResolvedQuery) -> Result<LogicalPlan, String> {
    let mut body_plan = plan_body(resolved.body)?;

    // Wrap with Sort if ORDER BY is present.
    if !resolved.order_by.is_empty() {
        // If ORDER BY references columns not in the projection, we need to
        // extend the project to include them (hidden columns) so the sort can
        // reference them.  After sort, a top-level project strips them.
        let needs_hidden =
            has_non_projected_sort_columns(&resolved.order_by, &resolved.output_columns);
        if needs_hidden {
            // Collect extra columns needed by ORDER BY
            let mut extra_items = Vec::new();
            for sort_item in &resolved.order_by {
                collect_extra_columns(&sort_item.expr, &resolved.output_columns, &mut extra_items);
            }

            // Extend the project node at the top of body_plan
            if let LogicalPlan::Project(ref mut proj) = body_plan {
                for extra in &extra_items {
                    proj.items.push(extra.clone());
                }
            }

            // Sort with extended scope
            body_plan = LogicalPlan::Sort(SortNode {
                input: Box::new(body_plan),
                items: resolved.order_by,
            });

            // Strip extra columns with a final project
            let final_items: Vec<ProjectItem> = resolved
                .output_columns
                .iter()
                .map(|col| ProjectItem {
                    expr: TypedExpr {
                        kind: ExprKind::ColumnRef {
                            qualifier: None,
                            column: col.name.clone(),
                        },
                        data_type: col.data_type.clone(),
                        nullable: col.nullable,
                    },
                    output_name: col.name.clone(),
                })
                .collect();
            body_plan = LogicalPlan::Project(ProjectNode {
                input: Box::new(body_plan),
                items: final_items,
            });
        } else {
            body_plan = LogicalPlan::Sort(SortNode {
                input: Box::new(body_plan),
                items: resolved.order_by,
            });
        }
    }

    // Wrap with Limit if LIMIT/OFFSET is present.
    if resolved.limit.is_some() || resolved.offset.is_some() {
        body_plan = LogicalPlan::Limit(LimitNode {
            input: Box::new(body_plan),
            limit: resolved.limit,
            offset: resolved.offset,
        });
    }

    Ok(body_plan)
}

/// Check if any ORDER BY expression references columns not in the output.
fn has_non_projected_sort_columns(order_by: &[SortItem], output: &[OutputColumn]) -> bool {
    let output_names: std::collections::HashSet<String> =
        output.iter().map(|c| c.name.to_lowercase()).collect();
    for item in order_by {
        if has_non_projected_ref(&item.expr, &output_names) {
            return true;
        }
    }
    false
}

fn has_non_projected_ref(
    expr: &TypedExpr,
    output_names: &std::collections::HashSet<String>,
) -> bool {
    match &expr.kind {
        ExprKind::ColumnRef { column, .. } => !output_names.contains(&column.to_lowercase()),
        ExprKind::BinaryOp { left, right, .. } => {
            has_non_projected_ref(left, output_names) || has_non_projected_ref(right, output_names)
        }
        ExprKind::UnaryOp { expr, .. } => has_non_projected_ref(expr, output_names),
        ExprKind::FunctionCall { args, .. } => {
            args.iter().any(|a| has_non_projected_ref(a, output_names))
        }
        ExprKind::Cast { expr, .. } => has_non_projected_ref(expr, output_names),
        ExprKind::Nested(inner) => has_non_projected_ref(inner, output_names),
        ExprKind::IsNull { expr, .. } => has_non_projected_ref(expr, output_names),
        _ => false,
    }
}

/// Collect ColumnRef items from ORDER BY that aren't in the output,
/// and build extra ProjectItems for them.
fn collect_extra_columns(expr: &TypedExpr, output: &[OutputColumn], extra: &mut Vec<ProjectItem>) {
    let output_names: std::collections::HashSet<String> =
        output.iter().map(|c| c.name.to_lowercase()).collect();
    let already_added: std::collections::HashSet<String> =
        extra.iter().map(|e| e.output_name.to_lowercase()).collect();
    collect_extra_inner(expr, &output_names, &already_added, extra);
}

fn collect_extra_inner(
    expr: &TypedExpr,
    output_names: &std::collections::HashSet<String>,
    already_added: &std::collections::HashSet<String>,
    extra: &mut Vec<ProjectItem>,
) {
    match &expr.kind {
        ExprKind::ColumnRef { column, .. } => {
            let col_lower = column.to_lowercase();
            if !output_names.contains(&col_lower) && !already_added.contains(&col_lower) {
                extra.push(ProjectItem {
                    expr: expr.clone(),
                    output_name: column.clone(),
                });
            }
        }
        ExprKind::BinaryOp { left, right, .. } => {
            collect_extra_inner(left, output_names, already_added, extra);
            collect_extra_inner(right, output_names, already_added, extra);
        }
        ExprKind::UnaryOp { expr: inner, .. } => {
            collect_extra_inner(inner, output_names, already_added, extra);
        }
        ExprKind::FunctionCall { args, .. } => {
            for a in args {
                collect_extra_inner(a, output_names, already_added, extra);
            }
        }
        ExprKind::Cast { expr: inner, .. } => {
            collect_extra_inner(inner, output_names, already_added, extra);
        }
        ExprKind::Nested(inner) => {
            collect_extra_inner(inner, output_names, already_added, extra);
        }
        ExprKind::IsNull { expr: inner, .. } => {
            collect_extra_inner(inner, output_names, already_added, extra);
        }
        _ => {}
    }
}

// ---------------------------------------------------------------------------
// Body planning
// ---------------------------------------------------------------------------

fn plan_body(body: QueryBody) -> Result<LogicalPlan, String> {
    match body {
        QueryBody::Select(select) => plan_select(select),
        QueryBody::SetOperation(set_op) => plan_set_operation(set_op),
        QueryBody::Values(values) => plan_values(values),
    }
}

// ---------------------------------------------------------------------------
// SELECT planning
// ---------------------------------------------------------------------------

/// Produces:  Project( [Aggregate(] [Filter(] from_plan [)] [)] )
fn plan_select(select: ResolvedSelect) -> Result<LogicalPlan, String> {
    // 1. FROM clause → base plan
    let mut current = match select.from {
        Some(relation) => plan_relation(relation)?,
        None => {
            // SELECT without FROM — single-row values node
            LogicalPlan::Values(ValuesNode {
                rows: vec![vec![]],
                columns: vec![],
            })
        }
    };

    // 2. WHERE → Filter
    if let Some(predicate) = select.filter {
        current = LogicalPlan::Filter(FilterNode {
            input: Box::new(current),
            predicate,
        });
    }

    // 3. GROUP BY / aggregation → Aggregate
    if select.has_aggregation || !select.group_by.is_empty() {
        let (project_items, agg_calls, output_columns) =
            split_projection_for_aggregate(&select.projection, &select.group_by);
        current = LogicalPlan::Aggregate(AggregateNode {
            input: Box::new(current),
            group_by: select.group_by,
            aggregates: agg_calls,
            output_columns,
        });
        // HAVING → Filter between Aggregate and Project
        if let Some(having) = select.having {
            current = LogicalPlan::Filter(FilterNode {
                input: Box::new(current),
                predicate: having,
            });
        }

        // The projection after aggregation
        if !project_items.is_empty() {
            current = LogicalPlan::Project(ProjectNode {
                input: Box::new(current),
                items: project_items,
            });
        }
    } else {
        // 4. No aggregation → check for window functions, then Project
        let has_window = select
            .projection
            .iter()
            .any(|item| has_window_call(&item.expr));
        if has_window {
            // Extract window function calls → WindowNode, rewrite Project
            let (window_exprs, rewritten_items) = extract_window_calls(&select.projection);
            let mut output_columns = Vec::new();
            // All input columns pass through
            // (we'll let the emitter figure out the exact scope)
            for item in &select.projection {
                output_columns.push(OutputColumn {
                    name: item.output_name.clone(),
                    data_type: item.expr.data_type.clone(),
                    nullable: item.expr.nullable,
                });
            }
            current = LogicalPlan::Window(WindowNode {
                input: Box::new(current),
                window_exprs,
                output_columns,
            });
            current = LogicalPlan::Project(ProjectNode {
                input: Box::new(current),
                items: rewritten_items,
            });
        } else {
            current = LogicalPlan::Project(ProjectNode {
                input: Box::new(current),
                items: select.projection,
            });
        }
    }

    Ok(current)
}

/// Check if an expression contains any WindowCall.
fn has_window_call(expr: &TypedExpr) -> bool {
    match &expr.kind {
        ExprKind::WindowCall { .. } => true,
        ExprKind::BinaryOp { left, right, .. } => has_window_call(left) || has_window_call(right),
        ExprKind::UnaryOp { expr, .. } => has_window_call(expr),
        ExprKind::Cast { expr, .. } => has_window_call(expr),
        ExprKind::Nested(inner) => has_window_call(inner),
        _ => false,
    }
}

/// Extract window function calls from the projection items.
/// Returns (window_exprs, rewritten_projection_items).
/// Each window call is replaced with a ColumnRef to its output name.
fn extract_window_calls(items: &[ProjectItem]) -> (Vec<WindowExpr>, Vec<ProjectItem>) {
    let mut window_exprs = Vec::new();
    let mut rewritten = Vec::new();

    for item in items {
        if let ExprKind::WindowCall {
            ref name,
            ref args,
            distinct,
            ref partition_by,
            ref order_by,
            ref window_frame,
        } = item.expr.kind
        {
            let win_output_name = item.output_name.clone();
            window_exprs.push(WindowExpr {
                name: name.clone(),
                args: args.clone(),
                distinct,
                partition_by: partition_by.clone(),
                order_by: order_by.clone(),
                window_frame: window_frame.clone(),
                result_type: item.expr.data_type.clone(),
                output_name: win_output_name.clone(),
            });
            // Replace with a column ref to the window output
            rewritten.push(ProjectItem {
                expr: TypedExpr {
                    kind: ExprKind::ColumnRef {
                        qualifier: None,
                        column: win_output_name.clone(),
                    },
                    data_type: item.expr.data_type.clone(),
                    nullable: item.expr.nullable,
                },
                output_name: item.output_name.clone(),
            });
        } else {
            rewritten.push(item.clone());
        }
    }

    (window_exprs, rewritten)
}

/// Split the SELECT list into post-aggregate projection items and aggregate calls.
///
/// For a query like `SELECT a, count(*), sum(b) + 1 FROM t GROUP BY a`:
/// - group_by exprs: [a]
/// - aggregate calls: [count(*), sum(b)]
/// - project items: the full SELECT list (may reference group-by columns and agg results)
fn split_projection_for_aggregate(
    projection: &[ProjectItem],
    _group_by: &[TypedExpr],
) -> (Vec<ProjectItem>, Vec<AggregateCall>, Vec<OutputColumn>) {
    let mut agg_calls = Vec::new();
    let mut output_columns = Vec::new();

    // Collect aggregate calls from projection
    for item in projection {
        collect_aggregates(&item.expr, &mut agg_calls);
        output_columns.push(OutputColumn {
            name: item.output_name.clone(),
            data_type: item.expr.data_type.clone(),
            nullable: item.expr.nullable,
        });
    }

    // The projection items remain as-is; the thrift emitter will handle
    // mapping them to the aggregate output.
    (projection.to_vec(), agg_calls, output_columns)
}

/// Recursively collect AggregateCall from a TypedExpr tree.
fn collect_aggregates(expr: &TypedExpr, out: &mut Vec<AggregateCall>) {
    match &expr.kind {
        ExprKind::AggregateCall {
            name,
            args,
            distinct,
            order_by,
        } => {
            // Avoid duplicates — compare name, distinct, and actual arg content
            let already = out.iter().any(|a| {
                a.name == *name
                    && a.distinct == *distinct
                    && a.args.len() == args.len()
                    && a.args
                        .iter()
                        .zip(args.iter())
                        .all(|(a, b)| format!("{:?}", a.kind) == format!("{:?}", b.kind))
            });
            if !already {
                out.push(AggregateCall {
                    name: name.clone(),
                    args: args.clone(),
                    distinct: *distinct,
                    result_type: expr.data_type.clone(),
                    order_by: order_by.clone(),
                });
            }
        }
        ExprKind::BinaryOp { left, right, .. } => {
            collect_aggregates(left, out);
            collect_aggregates(right, out);
        }
        ExprKind::UnaryOp { expr: inner, .. } => collect_aggregates(inner, out),
        ExprKind::FunctionCall { args, .. } => {
            for arg in args {
                collect_aggregates(arg, out);
            }
        }
        ExprKind::Cast { expr: inner, .. } => collect_aggregates(inner, out),
        ExprKind::Case {
            operand,
            when_then,
            else_expr,
        } => {
            if let Some(op) = operand {
                collect_aggregates(op, out);
            }
            for (w, t) in when_then {
                collect_aggregates(w, out);
                collect_aggregates(t, out);
            }
            if let Some(e) = else_expr {
                collect_aggregates(e, out);
            }
        }
        ExprKind::IsNull { expr: inner, .. } => collect_aggregates(inner, out),
        ExprKind::Nested(inner) => collect_aggregates(inner, out),
        ExprKind::InList { expr, list, .. } => {
            collect_aggregates(expr, out);
            for item in list {
                collect_aggregates(item, out);
            }
        }
        ExprKind::Between {
            expr, low, high, ..
        } => {
            collect_aggregates(expr, out);
            collect_aggregates(low, out);
            collect_aggregates(high, out);
        }
        ExprKind::Like { expr, pattern, .. } => {
            collect_aggregates(expr, out);
            collect_aggregates(pattern, out);
        }
        ExprKind::IsTruthValue { expr: inner, .. } => collect_aggregates(inner, out),
        // Leaves
        ExprKind::ColumnRef { .. } | ExprKind::Literal(_) => {}
        // Window calls are handled separately, not as aggregates
        ExprKind::WindowCall { .. } => {}
    }
}

// ---------------------------------------------------------------------------
// FROM clause planning
// ---------------------------------------------------------------------------

fn plan_relation(relation: Relation) -> Result<LogicalPlan, String> {
    match relation {
        Relation::Scan(scan) => {
            let columns = scan
                .table
                .columns
                .iter()
                .map(|c| OutputColumn {
                    name: c.name.clone(),
                    data_type: c.data_type.clone(),
                    nullable: c.nullable,
                })
                .collect();
            Ok(LogicalPlan::Scan(ScanNode {
                database: scan.database,
                table: scan.table,
                alias: scan.alias,
                columns,
                predicates: vec![],
                required_columns: None,
            }))
        }
        Relation::Subquery { query, alias: _ } => {
            // Recursively plan the subquery
            plan(*query)
        }
        Relation::Join(join_rel) => {
            let left = plan_relation(join_rel.left)?;
            let right = plan_relation(join_rel.right)?;
            Ok(LogicalPlan::Join(JoinNode {
                left: Box::new(left),
                right: Box::new(right),
                join_type: join_rel.join_type,
                condition: join_rel.condition,
            }))
        }
        Relation::GenerateSeries(gs) => Ok(LogicalPlan::GenerateSeries(GenerateSeriesNode {
            start: gs.start,
            end: gs.end,
            step: gs.step,
            column_name: gs.column_name,
            alias: gs.alias,
        })),
    }
}

// ---------------------------------------------------------------------------
// Set operation planning
// ---------------------------------------------------------------------------

fn plan_set_operation(set_op: ResolvedSetOp) -> Result<LogicalPlan, String> {
    let left = plan_body(*set_op.left)?;
    let right = plan_body(*set_op.right)?;

    match set_op.kind {
        SetOpKind::Union => Ok(LogicalPlan::Union(UnionNode {
            inputs: vec![left, right],
            all: set_op.all,
        })),
        SetOpKind::Intersect => Ok(LogicalPlan::Intersect(IntersectNode {
            inputs: vec![left, right],
        })),
        SetOpKind::Except => Ok(LogicalPlan::Except(ExceptNode {
            inputs: vec![left, right],
        })),
    }
}

// ---------------------------------------------------------------------------
// VALUES planning
// ---------------------------------------------------------------------------

fn plan_values(values: ResolvedValues) -> Result<LogicalPlan, String> {
    let columns = values
        .column_types
        .iter()
        .enumerate()
        .map(|(i, dt)| OutputColumn {
            name: format!("column_{}", i),
            data_type: dt.clone(),
            nullable: true,
        })
        .collect();
    Ok(LogicalPlan::Values(ValuesNode {
        rows: values.rows,
        columns,
    }))
}
