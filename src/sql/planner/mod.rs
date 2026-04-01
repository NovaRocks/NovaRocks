//! Logical Planner — converts [`ResolvedQuery`] into [`LogicalPlan`].
//!
//! This is a structural transformation that builds a relational algebra tree
//! from the analyzed query IR.  A future optimizer would rewrite this tree
//! before it reaches the Thrift emitter.

use crate::sql::cte::CTERegistry;
use crate::sql::ir::*;
use crate::sql::plan::*;

// ---------------------------------------------------------------------------
// Public entry
// ---------------------------------------------------------------------------

/// Plan a resolved query with shared CTE subtrees into a [`QueryPlan`].
///
/// Each CTE entry in the registry is planned independently into a
/// [`CTEProducePlan`], and references in the main plan appear as
/// [`LogicalPlan::CTEConsume`] leaf nodes.
pub(crate) fn plan_query(
    resolved: ResolvedQuery,
    cte_registry: CTERegistry,
) -> Result<QueryPlan, String> {
    let output_columns = resolved.output_columns.clone();
    let main_plan = plan(resolved)?;

    // Plan each shared CTE subtree independently.
    let cte_plans = cte_registry
        .entries
        .into_iter()
        .map(|entry| {
            let cte_plan = plan(entry.resolved_query)?;
            Ok(CTEProducePlan {
                cte_id: entry.id,
                plan: cte_plan,
                output_columns: entry.output_columns,
            })
        })
        .collect::<Result<Vec<_>, String>>()?;

    Ok(QueryPlan {
        cte_plans,
        main_plan,
        output_columns,
    })
}

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
fn plan_select(mut select: ResolvedSelect) -> Result<LogicalPlan, String> {
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

    // 2.5: ROLLUP → Insert Repeat node between filter and aggregate
    if let Some(repeat_info) = select.repeat.take() {
        current = LogicalPlan::Repeat(RepeatPlanNode {
            input: Box::new(current),
            repeat_column_ref_list: repeat_info.repeat_column_ref_list,
            grouping_ids: repeat_info.grouping_ids,
            all_rollup_columns: repeat_info.all_rollup_columns,
            grouping_fn_args: repeat_info.grouping_fn_args,
        });
    }

    // 3. GROUP BY / aggregation → Aggregate
    if select.has_aggregation || !select.group_by.is_empty() {
        // Collect non-aggregate column refs from HAVING that aren't already in
        // GROUP BY. These come from scalar subquery CROSS JOINs and must pass
        // through as extra group-by keys so they're available in the
        // post-aggregate HAVING filter.
        if let Some(ref having_expr) = select.having {
            let mut extra_gb = Vec::new();
            collect_non_agg_column_refs(having_expr, &select.group_by, &mut extra_gb);
            for col in extra_gb {
                select.group_by.push(col);
            }
        }

        let (project_items, agg_calls, output_columns) = split_projection_for_aggregate(
            &select.projection,
            &select.group_by,
            select.having.as_ref(),
        );
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

        // The projection after aggregation — may contain window functions
        current = build_window_and_project(current, project_items, &select.projection)?;
    } else {
        // 4. No aggregation → check for window functions, then Project
        current = build_window_and_project(current, select.projection.clone(), &select.projection)?;
    }

    Ok(current)
}

/// Check if an expression contains any WindowCall.
/// Build Window + Project nodes if the projection contains window functions,
/// otherwise just a Project node.
fn build_window_and_project(
    input: LogicalPlan,
    project_items: Vec<ProjectItem>,
    original_projection: &[ProjectItem],
) -> Result<LogicalPlan, String> {
    let has_window = project_items.iter().any(|item| has_window_call(&item.expr));
    if has_window {
        let (window_exprs, rewritten_items) = extract_window_calls(&project_items);
        let mut output_columns = Vec::new();
        for item in original_projection {
            output_columns.push(OutputColumn {
                name: item.output_name.clone(),
                data_type: item.expr.data_type.clone(),
                nullable: item.expr.nullable,
            });
        }
        let windowed = LogicalPlan::Window(WindowNode {
            input: Box::new(input),
            window_exprs,
            output_columns,
        });
        Ok(LogicalPlan::Project(ProjectNode {
            input: Box::new(windowed),
            items: rewritten_items,
        }))
    } else if !project_items.is_empty() {
        Ok(LogicalPlan::Project(ProjectNode {
            input: Box::new(input),
            items: project_items,
        }))
    } else {
        Ok(input)
    }
}

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
/// Window calls may be nested inside expressions (e.g., `sum(x) * 100 / sum(sum(x)) OVER (...)`).
fn extract_window_calls(items: &[ProjectItem]) -> (Vec<WindowExpr>, Vec<ProjectItem>) {
    let mut window_exprs = Vec::new();
    let mut rewritten = Vec::new();
    let mut counter = 0usize;

    for item in items {
        if has_window_call(&item.expr) {
            let new_expr = rewrite_window_calls(
                &item.expr,
                &item.output_name,
                &mut window_exprs,
                &mut counter,
            );
            rewritten.push(ProjectItem {
                expr: new_expr,
                output_name: item.output_name.clone(),
            });
        } else {
            rewritten.push(item.clone());
        }
    }

    (window_exprs, rewritten)
}

/// Recursively rewrite an expression tree, replacing each WindowCall node
/// with a ColumnRef that points to the window function's output column.
fn rewrite_window_calls(
    expr: &TypedExpr,
    base_name: &str,
    window_exprs: &mut Vec<WindowExpr>,
    counter: &mut usize,
) -> TypedExpr {
    match &expr.kind {
        ExprKind::WindowCall {
            name,
            args,
            distinct,
            partition_by,
            order_by,
            window_frame,
        } => {
            let win_output_name = if *counter == 0 {
                base_name.to_string()
            } else {
                format!("{}__win{}", base_name, counter)
            };
            *counter += 1;
            window_exprs.push(WindowExpr {
                name: name.clone(),
                args: args.clone(),
                distinct: *distinct,
                partition_by: partition_by.clone(),
                order_by: order_by.clone(),
                window_frame: window_frame.clone(),
                result_type: expr.data_type.clone(),
                output_name: win_output_name.clone(),
            });
            TypedExpr {
                kind: ExprKind::ColumnRef {
                    qualifier: None,
                    column: win_output_name,
                },
                data_type: expr.data_type.clone(),
                nullable: expr.nullable,
            }
        }
        ExprKind::BinaryOp { left, right, op } => TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(rewrite_window_calls(left, base_name, window_exprs, counter)),
                op: *op,
                right: Box::new(rewrite_window_calls(
                    right,
                    base_name,
                    window_exprs,
                    counter,
                )),
            },
            data_type: expr.data_type.clone(),
            nullable: expr.nullable,
        },
        ExprKind::UnaryOp { op, expr: inner } => TypedExpr {
            kind: ExprKind::UnaryOp {
                op: *op,
                expr: Box::new(rewrite_window_calls(
                    inner,
                    base_name,
                    window_exprs,
                    counter,
                )),
            },
            data_type: expr.data_type.clone(),
            nullable: expr.nullable,
        },
        ExprKind::Cast {
            expr: inner,
            target,
        } => TypedExpr {
            kind: ExprKind::Cast {
                expr: Box::new(rewrite_window_calls(
                    inner,
                    base_name,
                    window_exprs,
                    counter,
                )),
                target: target.clone(),
            },
            data_type: expr.data_type.clone(),
            nullable: expr.nullable,
        },
        ExprKind::Nested(inner) => TypedExpr {
            kind: ExprKind::Nested(Box::new(rewrite_window_calls(
                inner,
                base_name,
                window_exprs,
                counter,
            ))),
            data_type: expr.data_type.clone(),
            nullable: expr.nullable,
        },
        // For any other node types, return as-is (no window calls inside)
        _ => expr.clone(),
    }
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
    having: Option<&TypedExpr>,
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

    // Also collect aggregate calls from HAVING clause so the aggregate node
    // computes them even when they don't appear in SELECT.
    if let Some(having_expr) = having {
        collect_aggregates(having_expr, &mut agg_calls);
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
        // Window calls themselves are not aggregates, but their args may
        // contain aggregate calls that must be collected so the aggregate node
        // computes them (e.g. sum(sum(x)) OVER (...)).
        ExprKind::WindowCall {
            args,
            partition_by,
            order_by,
            ..
        } => {
            for arg in args {
                collect_aggregates(arg, out);
            }
            for expr in partition_by {
                collect_aggregates(expr, out);
            }
            for sort_item in order_by {
                collect_aggregates(&sort_item.expr, out);
            }
        }
        // SubqueryPlaceholder should be rewritten before reaching the planner
        ExprKind::SubqueryPlaceholder { .. } => {}
    }
}

/// Collect ColumnRef expressions from HAVING that appear outside of aggregate calls.
/// These are typically scalar subquery results (from CROSS JOINs) that need to pass
/// through the aggregate node as group-by keys.
fn collect_non_agg_column_refs(expr: &TypedExpr, group_by: &[TypedExpr], out: &mut Vec<TypedExpr>) {
    collect_non_agg_column_refs_inner(expr, group_by, out, false);
}

fn collect_non_agg_column_refs_inner(
    expr: &TypedExpr,
    group_by: &[TypedExpr],
    out: &mut Vec<TypedExpr>,
    inside_agg: bool,
) {
    match &expr.kind {
        ExprKind::AggregateCall { .. } => {
            // Don't recurse into aggregate calls — columns inside aggregates
            // are handled by the aggregate function itself, not as pass-through keys.
        }
        ExprKind::ColumnRef { qualifier, column } => {
            if !inside_agg {
                // Check if this column is already in group_by
                let already_grouped = group_by.iter().any(|gb| {
                    matches!(&gb.kind, ExprKind::ColumnRef { qualifier: gq, column: gc }
                        if gc == column && gq == qualifier)
                });
                // Check if already collected
                let already_collected = out.iter().any(|o| {
                    matches!(&o.kind, ExprKind::ColumnRef { qualifier: oq, column: oc }
                        if oc == column && oq == qualifier)
                });
                if !already_grouped && !already_collected {
                    out.push(expr.clone());
                }
            }
        }
        ExprKind::BinaryOp { left, right, .. } => {
            collect_non_agg_column_refs_inner(left, group_by, out, inside_agg);
            collect_non_agg_column_refs_inner(right, group_by, out, inside_agg);
        }
        ExprKind::UnaryOp { expr: inner, .. } => {
            collect_non_agg_column_refs_inner(inner, group_by, out, inside_agg);
        }
        ExprKind::FunctionCall { args, .. } => {
            for arg in args {
                collect_non_agg_column_refs_inner(arg, group_by, out, inside_agg);
            }
        }
        ExprKind::Cast { expr: inner, .. } => {
            collect_non_agg_column_refs_inner(inner, group_by, out, inside_agg);
        }
        ExprKind::Nested(inner) => {
            collect_non_agg_column_refs_inner(inner, group_by, out, inside_agg);
        }
        ExprKind::IsNull { expr: inner, .. } => {
            collect_non_agg_column_refs_inner(inner, group_by, out, inside_agg);
        }
        ExprKind::Case {
            operand,
            when_then,
            else_expr,
        } => {
            if let Some(op) = operand {
                collect_non_agg_column_refs_inner(op, group_by, out, inside_agg);
            }
            for (w, t) in when_then {
                collect_non_agg_column_refs_inner(w, group_by, out, inside_agg);
                collect_non_agg_column_refs_inner(t, group_by, out, inside_agg);
            }
            if let Some(e) = else_expr {
                collect_non_agg_column_refs_inner(e, group_by, out, inside_agg);
            }
        }
        _ => {}
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
        Relation::Subquery { query, alias } => {
            // Recursively plan the subquery, wrapping with alias metadata
            // so the physical emitter can register qualified columns.
            let output_columns = query.output_columns.clone();
            let inner_plan = plan(*query)?;
            Ok(LogicalPlan::SubqueryAlias(SubqueryAliasNode {
                input: Box::new(inner_plan),
                alias,
                output_columns,
            }))
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
        Relation::CTEConsume {
            cte_id,
            alias,
            output_columns,
        } => Ok(LogicalPlan::CTEConsume(CTEConsumeNode {
            cte_id,
            alias,
            output_columns,
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
