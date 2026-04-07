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

/// Plan a resolved query into a single logical tree, wrapping CTE definitions
/// as nested anchor/produce pairs around the main query subtree.
pub(crate) fn plan_query(
    resolved: ResolvedQuery,
    cte_registry: CTERegistry,
) -> Result<LogicalPlan, String> {
    plan_scoped_query(resolved, &cte_registry)
}

pub(crate) fn plan_query_legacy(
    resolved: ResolvedQuery,
    cte_registry: CTERegistry,
) -> Result<QueryPlan, String> {
    let output_columns = resolved.output_columns.clone();
    let main_plan = plan(resolved)?;

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
    let ResolvedQuery {
        body,
        order_by,
        limit,
        offset,
        output_columns,
        ..
    } = resolved;
    let body_plan = plan_body(body)?;
    Ok(apply_query_modifiers(
        body_plan,
        order_by,
        output_columns,
        limit,
        offset,
    ))
}

fn plan_scoped_query(
    resolved: ResolvedQuery,
    cte_registry: &CTERegistry,
) -> Result<LogicalPlan, String> {
    let ResolvedQuery {
        body,
        order_by,
        limit,
        offset,
        output_columns,
        local_cte_ids,
    } = resolved;
    let mut root = apply_query_modifiers(
        plan_body_scoped(body, cte_registry)?,
        order_by,
        output_columns,
        limit,
        offset,
    );

    for cte_id in local_cte_ids.into_iter().rev() {
        let entry = cte_registry
            .get(cte_id)
            .ok_or_else(|| format!("missing CTE entry for id {cte_id}"))?;
        let produce_input = plan_scoped_query(entry.resolved_query.clone(), cte_registry)?;
        let produce = LogicalPlan::CTEProduce(CTEProduceNode {
            cte_id: entry.id,
            input: Box::new(produce_input),
            output_columns: entry.output_columns.clone(),
        });
        root = LogicalPlan::CTEAnchor(CTEAnchorNode {
            cte_id: entry.id,
            produce: Box::new(produce),
            consumer: Box::new(root),
        });
    }

    Ok(root)
}

fn apply_query_modifiers(
    mut body_plan: LogicalPlan,
    order_by: Vec<SortItem>,
    output_columns: Vec<OutputColumn>,
    limit: Option<i64>,
    offset: Option<i64>,
) -> LogicalPlan {
    // Wrap with Sort if ORDER BY is present.
    if !order_by.is_empty() {
        // If ORDER BY references columns not in the projection, we need to
        // extend the project to include them (hidden columns) so the sort can
        // reference them.  After sort, a top-level project strips them.
        let needs_hidden = has_non_projected_sort_columns(&order_by, &output_columns);
        if needs_hidden {
            // Collect extra columns needed by ORDER BY
            let mut extra_items = Vec::new();
            for sort_item in &order_by {
                collect_extra_columns(&sort_item.expr, &output_columns, &mut extra_items);
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
                items: order_by,
            });

            // Strip extra columns with a final project
            let final_items: Vec<ProjectItem> = output_columns
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
                items: order_by,
            });
        }
    }

    // Wrap with Limit if LIMIT/OFFSET is present.
    if limit.is_some() || offset.is_some() {
        body_plan = LogicalPlan::Limit(LimitNode {
            input: Box::new(body_plan),
            limit,
            offset,
        });
    }

    body_plan
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

fn plan_body_scoped(body: QueryBody, cte_registry: &CTERegistry) -> Result<LogicalPlan, String> {
    match body {
        QueryBody::Select(select) => plan_select_scoped(select, cte_registry),
        QueryBody::SetOperation(set_op) => plan_set_operation_scoped(set_op, cte_registry),
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

    // 5. SELECT DISTINCT → Aggregate on all output columns (deduplication)
    if select.distinct {
        current = build_distinct(current, &select.projection);
    }

    Ok(current)
}

fn plan_select_scoped(
    mut select: ResolvedSelect,
    cte_registry: &CTERegistry,
) -> Result<LogicalPlan, String> {
    let mut current = match select.from {
        Some(relation) => plan_relation_scoped(relation, cte_registry)?,
        None => LogicalPlan::Values(ValuesNode {
            rows: vec![vec![]],
            columns: vec![],
        }),
    };

    if let Some(predicate) = select.filter {
        current = LogicalPlan::Filter(FilterNode {
            input: Box::new(current),
            predicate,
        });
    }

    if let Some(repeat_info) = select.repeat.take() {
        current = LogicalPlan::Repeat(RepeatPlanNode {
            input: Box::new(current),
            repeat_column_ref_list: repeat_info.repeat_column_ref_list,
            grouping_ids: repeat_info.grouping_ids,
            all_rollup_columns: repeat_info.all_rollup_columns,
            grouping_fn_args: repeat_info.grouping_fn_args,
        });
    }

    if select.has_aggregation || !select.group_by.is_empty() {
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
        if let Some(having) = select.having {
            current = LogicalPlan::Filter(FilterNode {
                input: Box::new(current),
                predicate: having,
            });
        }

        current = build_window_and_project(current, project_items, &select.projection)?;
    } else {
        current = build_window_and_project(current, select.projection.clone(), &select.projection)?;
    }

    // SELECT DISTINCT → Aggregate on all output columns (deduplication)
    if select.distinct {
        current = build_distinct(current, &select.projection);
    }

    Ok(current)
}

/// Build a deduplication Aggregate for SELECT DISTINCT.
/// Uses all projection columns as GROUP BY keys with no aggregate functions.
fn build_distinct(input: LogicalPlan, projection: &[ProjectItem]) -> LogicalPlan {
    let mut group_by = Vec::new();
    let mut output_columns = Vec::new();
    for item in projection {
        group_by.push(TypedExpr {
            kind: ExprKind::ColumnRef {
                qualifier: None,
                column: item.output_name.clone(),
            },
            data_type: item.expr.data_type.clone(),
            nullable: item.expr.nullable,
        });
        output_columns.push(OutputColumn {
            name: item.output_name.clone(),
            data_type: item.expr.data_type.clone(),
            nullable: item.expr.nullable,
        });
    }
    LogicalPlan::Aggregate(AggregateNode {
        input: Box::new(input),
        group_by,
        aggregates: vec![],
        output_columns,
    })
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
        // The analytic operator requires input sorted by (partition_by, order_by).
        // Insert a Sort node before the Window node.
        let first_win = &window_exprs[0];
        let mut sort_items = Vec::new();
        for p in &first_win.partition_by {
            sort_items.push(SortItem {
                expr: p.clone(),
                asc: true,
                nulls_first: true,
            });
        }
        for ob in &first_win.order_by {
            sort_items.push(ob.clone());
        }
        let sorted_input = if sort_items.is_empty() {
            input
        } else {
            LogicalPlan::Sort(SortNode {
                input: Box::new(input),
                items: sort_items,
            })
        };

        let windowed = LogicalPlan::Window(WindowNode {
            input: Box::new(sorted_input),
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

fn plan_relation_scoped(
    relation: Relation,
    cte_registry: &CTERegistry,
) -> Result<LogicalPlan, String> {
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
            let output_columns = query.output_columns.clone();
            let inner_plan = plan_scoped_query(*query, cte_registry)?;
            Ok(LogicalPlan::SubqueryAlias(SubqueryAliasNode {
                input: Box::new(inner_plan),
                alias,
                output_columns,
            }))
        }
        Relation::Join(join_rel) => {
            let left = plan_relation_scoped(join_rel.left, cte_registry)?;
            let right = plan_relation_scoped(join_rel.right, cte_registry)?;
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
    let left = plan(*set_op.left)?;
    let right = plan(*set_op.right)?;

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

fn plan_set_operation_scoped(
    set_op: ResolvedSetOp,
    cte_registry: &CTERegistry,
) -> Result<LogicalPlan, String> {
    let left = plan_scoped_query(*set_op.left, cte_registry)?;
    let right = plan_scoped_query(*set_op.right, cte_registry)?;

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::catalog::{CatalogProvider, ColumnDef, TableDef, TableStorage};

    struct TestCatalog;

    impl CatalogProvider for TestCatalog {
        fn get_table(&self, _db: &str, table: &str) -> Result<TableDef, String> {
            match table {
                "orders" => Ok(TableDef {
                    name: "orders".to_string(),
                    columns: vec![
                        ColumnDef {
                            name: "o_orderkey".to_string(),
                            data_type: arrow::datatypes::DataType::Int64,
                            nullable: false,
                        },
                        ColumnDef {
                            name: "o_custkey".to_string(),
                            data_type: arrow::datatypes::DataType::Int64,
                            nullable: false,
                        },
                    ],
                    storage: TableStorage::LocalParquetFile {
                        path: std::path::PathBuf::from("/tmp/orders.parquet"),
                    },
                }),
                other => Err(format!("unknown test table: {other}")),
            }
        }
    }

    fn parse_analyze_and_plan(sql: &str) -> Result<LogicalPlan, String> {
        let dialect = crate::sql::parser::dialect::StarRocksDialect;
        let mut ast =
            sqlparser::parser::Parser::parse_sql(&dialect, sql).map_err(|e| e.to_string())?;
        let stmt = ast
            .pop()
            .ok_or_else(|| "expected a statement".to_string())?;
        let query = match stmt {
            sqlparser::ast::Statement::Query(q) => q,
            _ => return Err("expected query".into()),
        };
        let (resolved, cte_registry) =
            crate::sql::analyzer::analyze(&query, &TestCatalog, "default")?;
        plan_query(resolved, cte_registry)
    }

    fn find_subquery_input(plan: &LogicalPlan) -> Option<&LogicalPlan> {
        match plan {
            LogicalPlan::Project(node) => find_subquery_input(&node.input),
            LogicalPlan::Sort(node) => find_subquery_input(&node.input),
            LogicalPlan::Limit(node) => find_subquery_input(&node.input),
            LogicalPlan::SubqueryAlias(node) => Some(&node.input),
            _ => None,
        }
    }

    #[test]
    fn test_plan_query_wraps_single_cte_in_anchor() {
        let plan = parse_analyze_and_plan(
            "WITH t AS (SELECT o_orderkey AS ok FROM orders) SELECT ok FROM t",
        )
        .expect("planner should succeed");

        match plan {
            LogicalPlan::CTEAnchor(anchor) => {
                assert_eq!(anchor.cte_id, 0);
                assert!(matches!(*anchor.produce, LogicalPlan::CTEProduce(_)));
            }
            other => panic!("expected CTEAnchor, got {other:?}"),
        }
    }

    #[test]
    fn test_plan_query_builds_nested_anchor_chain() {
        let plan = parse_analyze_and_plan(
            "WITH a AS (SELECT o_orderkey AS ok FROM orders), \
                  b AS (SELECT ok FROM a) \
             SELECT ok FROM b",
        )
        .expect("planner should succeed");

        match plan {
            LogicalPlan::CTEAnchor(anchor_a) => match *anchor_a.consumer {
                LogicalPlan::CTEAnchor(anchor_b) => {
                    assert_eq!(anchor_a.cte_id, 0);
                    assert_eq!(anchor_b.cte_id, 1);
                }
                other => panic!("expected nested CTEAnchor, got {other:?}"),
            },
            other => panic!("expected outer CTEAnchor, got {other:?}"),
        }
    }

    #[test]
    fn test_nested_with_in_derived_table_stays_inside_subquery_scope() {
        let plan = parse_analyze_and_plan(
            "WITH outer_t AS (SELECT o_orderkey AS ok FROM orders) \
             SELECT ok FROM (WITH inner_t AS (SELECT o_custkey AS ok FROM orders) \
                             SELECT ok FROM inner_t) s",
        )
        .expect("planner should succeed");

        match plan {
            LogicalPlan::CTEAnchor(outer_anchor) => {
                assert_eq!(outer_anchor.cte_id, 0);
                let subquery_input = find_subquery_input(&outer_anchor.consumer)
                    .expect("expected derived subquery under outer consumer");
                match subquery_input {
                    LogicalPlan::CTEAnchor(inner_anchor) => {
                        assert_eq!(inner_anchor.cte_id, 1);
                    }
                    other => panic!("expected inner CTEAnchor inside subquery, got {other:?}"),
                }
            }
            other => panic!("expected outer CTEAnchor, got {other:?}"),
        }
    }

    #[test]
    fn test_nested_with_in_cte_definition_stays_inside_produce_subtree() {
        let plan = parse_analyze_and_plan(
            "WITH outer_cte AS (WITH inner_cte AS (SELECT o_orderkey AS ok FROM orders) \
                                SELECT ok FROM inner_cte) \
             SELECT ok FROM outer_cte",
        )
        .expect("planner should succeed");

        match plan {
            LogicalPlan::CTEAnchor(outer_anchor) => {
                assert_eq!(outer_anchor.cte_id, 1);
                match *outer_anchor.produce {
                    LogicalPlan::CTEProduce(outer_produce) => match *outer_produce.input {
                        LogicalPlan::CTEAnchor(inner_anchor) => {
                            assert_eq!(inner_anchor.cte_id, 0);
                        }
                        other => {
                            panic!("expected inner CTEAnchor inside produce input, got {other:?}")
                        }
                    },
                    other => panic!("expected outer CTEProduce, got {other:?}"),
                }
            }
            other => panic!("expected outer CTEAnchor, got {other:?}"),
        }
    }

    #[test]
    fn test_explain_keeps_nested_cte_anchor_inside_subquery() {
        let plan = parse_analyze_and_plan(
            "WITH outer_t AS (SELECT o_orderkey AS ok FROM orders) \
             SELECT ok FROM (WITH inner_t AS (SELECT o_custkey AS ok FROM orders) \
                             SELECT ok FROM inner_t) s",
        )
        .expect("planner should succeed");

        let lines =
            crate::sql::explain::explain_plan(&plan, crate::sql::explain::ExplainLevel::Normal);
        let subquery_idx = lines
            .iter()
            .position(|line| line.contains("SUBQUERY ALIAS [s]"))
            .expect("expected subquery alias line");
        let inner_anchor_idx = lines
            .iter()
            .position(|line| line.contains("CTE_ANCHOR(cte_id=1)"))
            .expect("expected nested inner anchor line");

        assert!(
            inner_anchor_idx > subquery_idx,
            "nested inner anchor should appear under subquery: {lines:?}"
        );
    }

    #[test]
    fn test_parenthesized_set_op_branch_keeps_local_cte_anchor_in_branch() {
        let plan = parse_analyze_and_plan(
            "SELECT o_orderkey AS ok FROM orders \
             UNION ALL \
             (WITH t AS (SELECT o_custkey AS ok FROM orders) SELECT ok FROM t)",
        )
        .expect("planner should succeed");

        match plan {
            LogicalPlan::Union(node) => {
                assert_eq!(node.inputs.len(), 2);
                match &node.inputs[1] {
                    LogicalPlan::CTEAnchor(anchor) => assert_eq!(anchor.cte_id, 0),
                    other => {
                        panic!("expected branch-local CTEAnchor in union input, got {other:?}")
                    }
                }
            }
            other => panic!("expected UNION plan, got {other:?}"),
        }
    }

    #[test]
    fn test_explain_keeps_parenthesized_set_op_branch_anchor_in_branch() {
        let plan = parse_analyze_and_plan(
            "SELECT o_orderkey AS ok FROM orders \
             UNION ALL \
             (WITH t AS (SELECT o_custkey AS ok FROM orders) SELECT ok FROM t)",
        )
        .expect("planner should succeed");

        let lines =
            crate::sql::explain::explain_plan(&plan, crate::sql::explain::ExplainLevel::Normal);
        let union_idx = lines
            .iter()
            .position(|line| line.contains("UNION ALL"))
            .expect("expected union line");
        let anchor_idx = lines
            .iter()
            .position(|line| line.contains("CTE_ANCHOR(cte_id=0)"))
            .expect("expected branch-local anchor line");

        assert!(
            anchor_idx > union_idx,
            "branch-local anchor should appear under union: {lines:?}"
        );
    }
}
