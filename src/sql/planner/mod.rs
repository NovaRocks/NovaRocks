//! Logical Planner — converts [`ResolvedQuery`] into [`LogicalPlan`].
//!
//! This is a structural transformation that builds a relational algebra tree
//! from the analyzed query IR.  A future optimizer would rewrite this tree
//! before it reaches the Thrift emitter.

pub(crate) mod plan;

use crate::sql::analysis::cte::CTERegistry;
use crate::sql::analysis::*;
use crate::sql::codegen::helpers::typed_expr_display_name;
use plan::*;

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
    let mut final_projection: Option<Vec<ProjectItem>> = None;

    // Wrap with Sort if ORDER BY is present.
    if !order_by.is_empty() {
        let extra_items = collect_extra_sort_items(&order_by, &output_columns);
        let sort_items = rewrite_sort_items_to_projection_refs(&order_by, &extra_items);
        if !extra_items.is_empty() {
            // We're about to add extra sort-only columns to the inner Project
            // and then strip them with an outer Project after the sort. To
            // make that outer Project's column references unambiguous — even
            // when two SELECT items share an output name (e.g. `t1.c2,
            // t2.c2` both default to `c2`) — rename each inner Project
            // SELECT item to a unique synthetic name (`__nr_sel_<idx>`).
            // The outer strip-projection then references those synthetic
            // names and re-aliases each to the user-visible output name.
            //
            // Extras keep their display-name output_name because
            // `sort_items` (rewritten above by
            // `rewrite_sort_items_to_projection_refs`) references them
            // through that exact name.
            //
            // Sort items that didn't match an extra (and therefore still
            // hold their original ColumnRef into the SELECT projection)
            // would otherwise fail to resolve after the rename, so we
            // remap any `ColumnRef(<select_output_name>)` to the matching
            // `__nr_sel_<idx>` below.
            let user_select: Option<Vec<(String, arrow::datatypes::DataType, bool)>> =
                if let LogicalPlan::Project(ref mut proj) = body_plan {
                    if let LogicalPlan::Aggregate(ref mut agg) = *proj.input {
                        for extra in &extra_items {
                            collect_aggregates(&extra.expr, &mut agg.aggregates);
                        }
                    }
                    let user: Vec<(String, arrow::datatypes::DataType, bool)> = proj
                        .items
                        .iter()
                        .map(|it| {
                            (
                                it.output_name.clone(),
                                it.expr.data_type.clone(),
                                it.expr.nullable,
                            )
                        })
                        .collect();
                    for (idx, item) in proj.items.iter_mut().enumerate() {
                        item.output_name = format!("__nr_sel_{idx}");
                    }
                    for extra in &extra_items {
                        proj.items.push(extra.clone());
                    }
                    Some(user)
                } else {
                    None
                };

            // After renaming, sort items that still hold ColumnRefs to
            // pre-rename SELECT output names must be remapped onto the
            // synthetic `__nr_sel_<idx>` slots. Without this, sort
            // references like `ORDER BY v1` (matching SELECT v1 → renamed
            // to `__nr_sel_1`) would fail to resolve at sort time.
            let sort_items = if let Some(ref user) = user_select {
                let name_to_idx: std::collections::HashMap<String, usize> = user
                    .iter()
                    .enumerate()
                    .map(|(idx, (name, _, _))| (name.to_lowercase(), idx))
                    .collect();
                sort_items
                    .into_iter()
                    .map(|item| remap_sort_to_synthetic(item, &name_to_idx))
                    .collect()
            } else {
                sort_items
            };

            // Sort with extended scope
            body_plan = LogicalPlan::Sort(SortNode {
                input: Box::new(body_plan),
                items: sort_items,
            });

            // Strip synthetic sort-only columns after LIMIT/OFFSET so the
            // limit stays directly above Sort and can be rewritten to TopN.
            final_projection = Some(if let Some(user) = user_select {
                user.into_iter()
                    .enumerate()
                    .map(|(idx, (name, dt, nullable))| ProjectItem {
                        expr: TypedExpr {
                            kind: ExprKind::ColumnRef {
                                qualifier: None,
                                column: format!("__nr_sel_{idx}"),
                            },
                            data_type: dt,
                            nullable,
                        },
                        output_name: name,
                    })
                    .collect()
            } else {
                output_columns
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
                    .collect()
            });
        } else {
            body_plan = LogicalPlan::Sort(SortNode {
                input: Box::new(body_plan),
                items: sort_items,
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

    if let Some(items) = final_projection {
        body_plan = LogicalPlan::Project(ProjectNode {
            input: Box::new(body_plan),
            items,
        });
    }

    body_plan
}

fn collect_extra_sort_items(order_by: &[SortItem], output: &[OutputColumn]) -> Vec<ProjectItem> {
    let output_names: std::collections::HashSet<String> =
        output.iter().map(|c| c.name.to_lowercase()).collect();
    let mut added = std::collections::HashSet::new();
    let mut extra = Vec::new();
    for item in order_by {
        let output_name = crate::sql::codegen::helpers::typed_expr_display_name(&item.expr);
        let output_name_lower = output_name.to_lowercase();
        if !output_names.contains(&output_name_lower) && added.insert(output_name_lower) {
            extra.push(ProjectItem {
                expr: item.expr.clone(),
                output_name,
            });
        }
    }
    extra
}

/// Rewrite a sort item so any unqualified `ColumnRef` pointing at a
/// pre-rename SELECT output name is remapped to the matching
/// `__nr_sel_<idx>`. Used after the inner Project items have been renamed
/// for the sort-extras flow so that simple `ORDER BY <select_alias>`
/// references still resolve.
fn remap_sort_to_synthetic(
    item: SortItem,
    name_to_idx: &std::collections::HashMap<String, usize>,
) -> SortItem {
    let SortItem {
        expr,
        asc,
        nulls_first,
    } = item;
    SortItem {
        expr: remap_select_alias_refs(expr, name_to_idx),
        asc,
        nulls_first,
    }
}

fn remap_select_alias_refs(
    expr: TypedExpr,
    name_to_idx: &std::collections::HashMap<String, usize>,
) -> TypedExpr {
    match expr.kind {
        ExprKind::ColumnRef {
            qualifier: None,
            ref column,
        } => {
            if let Some(idx) = name_to_idx.get(&column.to_lowercase()) {
                TypedExpr {
                    data_type: expr.data_type,
                    nullable: expr.nullable,
                    kind: ExprKind::ColumnRef {
                        qualifier: None,
                        column: format!("__nr_sel_{idx}"),
                    },
                }
            } else {
                expr
            }
        }
        _ => expr,
    }
}

fn rewrite_sort_items_to_projection_refs(
    order_by: &[SortItem],
    extra_items: &[ProjectItem],
) -> Vec<SortItem> {
    let extra_names: std::collections::HashMap<String, &ProjectItem> = extra_items
        .iter()
        .map(|item| {
            (
                crate::sql::codegen::helpers::typed_expr_display_name(&item.expr).to_lowercase(),
                item,
            )
        })
        .collect();

    order_by
        .iter()
        .map(|item| {
            let display =
                crate::sql::codegen::helpers::typed_expr_display_name(&item.expr).to_lowercase();
            if let Some(extra) = extra_names.get(&display) {
                SortItem {
                    expr: TypedExpr {
                        kind: ExprKind::ColumnRef {
                            qualifier: None,
                            column: extra.output_name.clone(),
                        },
                        data_type: item.expr.data_type.clone(),
                        nullable: item.expr.nullable,
                    },
                    asc: item.asc,
                    nulls_first: item.nulls_first,
                }
            } else {
                item.clone()
            }
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Body planning
// ---------------------------------------------------------------------------

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

fn plan_select_scoped(
    mut select: ResolvedSelect,
    cte_registry: &CTERegistry,
) -> Result<LogicalPlan, String> {
    const REPEAT_GROUP_QUALIFIER: &str = "__repeat_group";

    let mut current = match select.from.take() {
        Some(relation) => plan_relation_scoped(relation, cte_registry)?,
        None => LogicalPlan::Values(ValuesNode {
            rows: vec![vec![]],
            columns: vec![],
        }),
    };

    if let Some(predicate) = select.filter.take() {
        current = LogicalPlan::Filter(FilterNode {
            input: Box::new(current),
            predicate,
        });
    }

    if let Some(mut repeat_info) = select.repeat.take() {
        let grouping_key_aliases = prepare_repeat_input(
            &mut current,
            &mut select,
            &mut repeat_info,
            REPEAT_GROUP_QUALIFIER,
        );
        current = LogicalPlan::Repeat(RepeatPlanNode {
            input: Box::new(current),
            repeat_column_ref_list: repeat_info.repeat_column_ref_list,
            grouping_ids: repeat_info.grouping_ids,
            all_rollup_columns: repeat_info.all_rollup_columns,
            grouping_key_aliases,
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

fn prepare_repeat_input(
    current: &mut LogicalPlan,
    select: &mut ResolvedSelect,
    repeat_info: &mut crate::sql::analysis::RepeatInfo,
    repeat_group_qualifier: &str,
) -> Vec<(String, String)> {
    let grouping_key_aliases: Vec<(String, String)> = repeat_info
        .all_rollup_columns
        .iter()
        .enumerate()
        .map(|(idx, name)| (name.clone(), format!("__repeat_group_key_{idx}")))
        .collect();
    if grouping_key_aliases.is_empty() {
        return grouping_key_aliases;
    }

    let mut project_items = Vec::new();
    let mut seen_refs = std::collections::HashSet::new();
    for gb_expr in &select.group_by {
        collect_repeat_input_refs(gb_expr, &mut project_items, &mut seen_refs);
    }
    for item in &select.projection {
        collect_repeat_input_refs(&item.expr, &mut project_items, &mut seen_refs);
    }
    if let Some(having) = &select.having {
        collect_repeat_input_refs(having, &mut project_items, &mut seen_refs);
    }

    for (original_name, alias_name) in &grouping_key_aliases {
        if let Some(source_expr) = select.group_by.iter().find_map(|expr| match &expr.kind {
            ExprKind::ColumnRef { qualifier, column }
                if column.eq_ignore_ascii_case(original_name) =>
            {
                Some(TypedExpr {
                    kind: ExprKind::ColumnRef {
                        qualifier: qualifier.clone(),
                        column: column.clone(),
                    },
                    data_type: expr.data_type.clone(),
                    nullable: expr.nullable,
                })
            }
            _ => None,
        }) {
            project_items.push(ProjectItem {
                expr: source_expr,
                output_name: alias_name.clone(),
            });
        }
    }

    *current = LogicalPlan::Project(ProjectNode {
        input: Box::new(current.clone()),
        items: project_items,
    });

    for gb_expr in &mut select.group_by {
        if let ExprKind::ColumnRef {
            qualifier: _,
            column,
        } = &gb_expr.kind
            && grouping_key_aliases
                .iter()
                .any(|(original_name, _)| column.eq_ignore_ascii_case(original_name))
        {
            gb_expr.kind = ExprKind::ColumnRef {
                qualifier: Some(repeat_group_qualifier.to_string()),
                column: column.clone(),
            };
        }
    }

    for non_null_cols in &mut repeat_info.repeat_column_ref_list {
        for col in non_null_cols {
            if let Some((_, alias_name)) = grouping_key_aliases
                .iter()
                .find(|(original_name, _)| col.eq_ignore_ascii_case(original_name))
            {
                *col = alias_name.clone();
            }
        }
    }
    repeat_info.all_rollup_columns = grouping_key_aliases
        .iter()
        .map(|(_, alias_name)| alias_name.clone())
        .collect();
    for (_fn_name, arg_cols) in &mut repeat_info.grouping_fn_args {
        for col in arg_cols {
            if let Some((_, alias_name)) = grouping_key_aliases
                .iter()
                .find(|(original_name, _)| col.eq_ignore_ascii_case(original_name))
            {
                *col = alias_name.clone();
            }
        }
    }

    grouping_key_aliases
}

fn collect_repeat_input_refs(
    expr: &TypedExpr,
    out: &mut Vec<ProjectItem>,
    seen: &mut std::collections::HashSet<(Option<String>, String)>,
) {
    match &expr.kind {
        ExprKind::ColumnRef { qualifier, column } => {
            if qualifier.is_none() && column.starts_with("__grouping_") {
                return;
            }
            let key = (qualifier.clone(), column.to_lowercase());
            if seen.insert(key) {
                out.push(ProjectItem {
                    expr: expr.clone(),
                    output_name: column.clone(),
                });
            }
        }
        ExprKind::AggregateCall { args, order_by, .. } => {
            for arg in args {
                collect_repeat_input_refs(arg, out, seen);
            }
            for sort_item in order_by {
                collect_repeat_input_refs(&sort_item.expr, out, seen);
            }
        }
        ExprKind::FunctionCall { args, .. } => {
            for arg in args {
                collect_repeat_input_refs(arg, out, seen);
            }
        }
        ExprKind::BinaryOp { left, right, .. } => {
            collect_repeat_input_refs(left, out, seen);
            collect_repeat_input_refs(right, out, seen);
        }
        ExprKind::UnaryOp { expr: inner, .. }
        | ExprKind::Cast { expr: inner, .. }
        | ExprKind::Nested(inner)
        | ExprKind::IsNull { expr: inner, .. }
        | ExprKind::IsTruthValue { expr: inner, .. } => {
            collect_repeat_input_refs(inner, out, seen);
        }
        ExprKind::Case {
            operand,
            when_then,
            else_expr,
        } => {
            if let Some(op) = operand {
                collect_repeat_input_refs(op, out, seen);
            }
            for (when, then) in when_then {
                collect_repeat_input_refs(when, out, seen);
                collect_repeat_input_refs(then, out, seen);
            }
            if let Some(el) = else_expr {
                collect_repeat_input_refs(el, out, seen);
            }
        }
        ExprKind::WindowCall {
            args,
            partition_by,
            order_by,
            ..
        } => {
            for arg in args {
                collect_repeat_input_refs(arg, out, seen);
            }
            for part in partition_by {
                collect_repeat_input_refs(part, out, seen);
            }
            for sort_item in order_by {
                collect_repeat_input_refs(&sort_item.expr, out, seen);
            }
        }
        _ => {}
    }
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
        // Insert a Sort node before the Window node using the first window
        // function's sort keys.  When window functions have different
        // partition/order signatures, the physical emitter splits them into
        // separate Sort + Analytic nodes (see emit_window).
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
        ExprKind::FunctionCall { args, .. } | ExprKind::AggregateCall { args, .. } => {
            args.iter().any(has_window_call)
        }
        ExprKind::Cast { expr, .. } => has_window_call(expr),
        ExprKind::IsNull { expr, .. } | ExprKind::IsTruthValue { expr, .. } => {
            has_window_call(expr)
        }
        ExprKind::InList { expr, list, .. } => {
            has_window_call(expr) || list.iter().any(has_window_call)
        }
        ExprKind::Between {
            expr, low, high, ..
        } => has_window_call(expr) || has_window_call(low) || has_window_call(high),
        ExprKind::Like { expr, pattern, .. } => has_window_call(expr) || has_window_call(pattern),
        ExprKind::Case {
            operand,
            when_then,
            else_expr,
        } => {
            operand.as_deref().is_some_and(has_window_call)
                || when_then
                    .iter()
                    .any(|(when, then)| has_window_call(when) || has_window_call(then))
                || else_expr.as_deref().is_some_and(has_window_call)
        }
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
            ignore_nulls,
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
                ignore_nulls: *ignore_nulls,
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
        ExprKind::FunctionCall {
            name,
            args,
            distinct,
        } => TypedExpr {
            kind: ExprKind::FunctionCall {
                name: name.clone(),
                args: args
                    .iter()
                    .map(|arg| rewrite_window_calls(arg, base_name, window_exprs, counter))
                    .collect(),
                distinct: *distinct,
            },
            data_type: expr.data_type.clone(),
            nullable: expr.nullable,
        },
        ExprKind::AggregateCall {
            name,
            args,
            distinct,
            order_by,
        } => TypedExpr {
            kind: ExprKind::AggregateCall {
                name: name.clone(),
                args: args
                    .iter()
                    .map(|arg| rewrite_window_calls(arg, base_name, window_exprs, counter))
                    .collect(),
                distinct: *distinct,
                order_by: order_by
                    .iter()
                    .map(|item| SortItem {
                        expr: rewrite_window_calls(&item.expr, base_name, window_exprs, counter),
                        asc: item.asc,
                        nulls_first: item.nulls_first,
                    })
                    .collect(),
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
        ExprKind::IsNull {
            expr: inner,
            negated,
        } => TypedExpr {
            kind: ExprKind::IsNull {
                expr: Box::new(rewrite_window_calls(
                    inner,
                    base_name,
                    window_exprs,
                    counter,
                )),
                negated: *negated,
            },
            data_type: expr.data_type.clone(),
            nullable: expr.nullable,
        },
        ExprKind::InList {
            expr: inner,
            list,
            negated,
        } => TypedExpr {
            kind: ExprKind::InList {
                expr: Box::new(rewrite_window_calls(
                    inner,
                    base_name,
                    window_exprs,
                    counter,
                )),
                list: list
                    .iter()
                    .map(|item| rewrite_window_calls(item, base_name, window_exprs, counter))
                    .collect(),
                negated: *negated,
            },
            data_type: expr.data_type.clone(),
            nullable: expr.nullable,
        },
        ExprKind::Between {
            expr: inner,
            low,
            high,
            negated,
        } => TypedExpr {
            kind: ExprKind::Between {
                expr: Box::new(rewrite_window_calls(
                    inner,
                    base_name,
                    window_exprs,
                    counter,
                )),
                low: Box::new(rewrite_window_calls(low, base_name, window_exprs, counter)),
                high: Box::new(rewrite_window_calls(high, base_name, window_exprs, counter)),
                negated: *negated,
            },
            data_type: expr.data_type.clone(),
            nullable: expr.nullable,
        },
        ExprKind::Like {
            expr: inner,
            pattern,
            negated,
        } => TypedExpr {
            kind: ExprKind::Like {
                expr: Box::new(rewrite_window_calls(
                    inner,
                    base_name,
                    window_exprs,
                    counter,
                )),
                pattern: Box::new(rewrite_window_calls(
                    pattern,
                    base_name,
                    window_exprs,
                    counter,
                )),
                negated: *negated,
            },
            data_type: expr.data_type.clone(),
            nullable: expr.nullable,
        },
        ExprKind::Case {
            operand,
            when_then,
            else_expr,
        } => TypedExpr {
            kind: ExprKind::Case {
                operand: operand.as_ref().map(|inner| {
                    Box::new(rewrite_window_calls(
                        inner,
                        base_name,
                        window_exprs,
                        counter,
                    ))
                }),
                when_then: when_then
                    .iter()
                    .map(|(when, then)| {
                        (
                            rewrite_window_calls(when, base_name, window_exprs, counter),
                            rewrite_window_calls(then, base_name, window_exprs, counter),
                        )
                    })
                    .collect(),
                else_expr: else_expr.as_ref().map(|inner| {
                    Box::new(rewrite_window_calls(
                        inner,
                        base_name,
                        window_exprs,
                        counter,
                    ))
                }),
            },
            data_type: expr.data_type.clone(),
            nullable: expr.nullable,
        },
        ExprKind::IsTruthValue {
            expr: inner,
            value,
            negated,
        } => TypedExpr {
            kind: ExprKind::IsTruthValue {
                expr: Box::new(rewrite_window_calls(
                    inner,
                    base_name,
                    window_exprs,
                    counter,
                )),
                value: *value,
                negated: *negated,
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
    group_by: &[TypedExpr],
    having: Option<&TypedExpr>,
) -> (Vec<ProjectItem>, Vec<AggregateCall>, Vec<OutputColumn>) {
    let mut agg_calls = Vec::new();
    let mut output_columns = Vec::new();
    let mut project_items = Vec::with_capacity(projection.len());

    // Collect aggregate calls from projection
    for item in projection {
        collect_aggregates(&item.expr, &mut agg_calls);
        output_columns.push(OutputColumn {
            name: item.output_name.clone(),
            data_type: item.expr.data_type.clone(),
            nullable: item.expr.nullable,
        });
        project_items.push(ProjectItem {
            expr: rewrite_exact_group_by_expr_ref(&item.expr, group_by),
            output_name: item.output_name.clone(),
        });
    }

    // Also collect aggregate calls from HAVING clause so the aggregate node
    // computes them even when they don't appear in SELECT.
    if let Some(having_expr) = having {
        collect_aggregates(having_expr, &mut agg_calls);
    }

    (project_items, agg_calls, output_columns)
}

fn rewrite_exact_group_by_expr_ref(expr: &TypedExpr, group_by: &[TypedExpr]) -> TypedExpr {
    let expr_name = typed_expr_display_name(expr);
    for gb in group_by {
        if typed_expr_display_name(gb) == expr_name {
            return TypedExpr {
                kind: ExprKind::ColumnRef {
                    qualifier: None,
                    column: expr_name,
                },
                data_type: gb.data_type.clone(),
                nullable: gb.nullable,
            };
        }
    }
    expr.clone()
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
            // Avoid duplicates — compare full aggregate semantics, including
            // ORDER BY metadata for ordered aggregates like
            // `array_agg(distinct x order by y desc)`.
            let already = out.iter().any(|a| {
                a.name == *name
                    && a.distinct == *distinct
                    && a.args.len() == args.len()
                    && a.order_by.len() == order_by.len()
                    && a.args
                        .iter()
                        .zip(args.iter())
                        .all(|(a, b)| format!("{:?}", a.kind) == format!("{:?}", b.kind))
                    && a.order_by.iter().zip(order_by.iter()).all(|(left, right)| {
                        left.asc == right.asc
                            && left.nulls_first == right.nulls_first
                            && format!("{:?}", left.expr.kind) == format!("{:?}", right.expr.kind)
                    })
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
        ExprKind::LambdaFunction { body, .. } => collect_aggregates(body, out),
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
        ExprKind::ColumnRef { .. } | ExprKind::LambdaParamRef { .. } | ExprKind::Literal(_) => {}
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
        // Higher-order function body is evaluated per element by array_map etc.;
        // any aggregate inside a lambda body would be a semantic error, so
        // walking is unnecessary. Treat as a leaf for aggregate collection.
        ExprKind::Lambda { .. } => {}
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
        Relation::Subquery {
            query,
            alias,
            output_columns,
        } => {
            let inner_plan = plan_scoped_query(*query, cte_registry)?;
            Ok(LogicalPlan::SubqueryAlias(SubqueryAliasNode {
                input: Box::new(inner_plan),
                alias,
                output_columns,
            }))
        }
        Relation::Join(join_rel) => {
            let JoinRelation {
                left,
                right,
                join_type,
                condition,
            } = *join_rel;
            match right {
                Relation::Unnest(unnest) => {
                    let is_left_join = match join_type {
                        JoinKind::Cross | JoinKind::Inner => false,
                        JoinKind::LeftOuter => true,
                        other => {
                            return Err(format!(
                                "LATERAL UNNEST supports CROSS/INNER/LEFT joins, got {other:?}"
                            ));
                        }
                    };
                    if !is_lateral_unnest_condition_supported(&condition) {
                        return Err(
                            "LATERAL UNNEST currently requires no condition or ON TRUE".into()
                        );
                    }
                    let left = plan_relation_scoped(left, cte_registry)?;
                    Ok(LogicalPlan::TableFunction(TableFunctionNode {
                        input: Box::new(left),
                        function_name: "unnest".to_string(),
                        args: unnest.args,
                        output_columns: unnest.output_columns,
                        alias: unnest.alias,
                        is_left_join,
                    }))
                }
                right => {
                    let left = plan_relation_scoped(left, cte_registry)?;
                    let right = plan_relation_scoped(right, cte_registry)?;
                    Ok(LogicalPlan::Join(JoinNode {
                        left: Box::new(left),
                        right: Box::new(right),
                        join_type,
                        condition,
                    }))
                }
            }
        }
        Relation::GenerateSeries(gs) => Ok(LogicalPlan::GenerateSeries(GenerateSeriesNode {
            start: gs.start,
            end: gs.end,
            step: gs.step,
            column_name: gs.column_name,
            alias: gs.alias,
        })),
        Relation::Unnest(_) => Err("UNNEST is currently supported only in LATERAL JOIN".into()),
        Relation::CTEConsume {
            cte_id,
            alias,
            output_columns,
        } => Ok(LogicalPlan::CTEConsume(CTEConsumeNode {
            cte_id,
            alias,
            output_columns,
        })),
        Relation::IcebergMetadataScan(rel) => plan_iceberg_metadata_scan(rel),
    }
}

fn is_lateral_unnest_condition_supported(condition: &Option<TypedExpr>) -> bool {
    matches!(
        condition,
        None | Some(TypedExpr {
            kind: ExprKind::Literal(LiteralValue::Bool(true)),
            ..
        })
    )
}

/// Lower an analyzer-built `IcebergMetadataScanRelation` into a regular
/// `LogicalPlan::Scan` whose `TableDef` carries the synthetic
/// `TableStorage::IcebergMetadataTable` storage. The optimizer treats it
/// like any other Scan; codegen branches on the storage variant to emit
/// an `HDFS_SCAN_NODE` whose lowering wires up the native-Rust
/// `IcebergMetadataScanOp` (no JNI bridge).
fn plan_iceberg_metadata_scan(rel: IcebergMetadataScanRelation) -> Result<LogicalPlan, String> {
    use crate::sql::analyzer::iceberg_metadata::metadata_table_schema;
    use crate::sql::catalog::{ColumnDef, TableDef, TableStorage};

    let cols = metadata_table_schema(rel.metadata_table_type.clone());
    if cols.is_empty() {
        return Err(format!(
            "iceberg metadata table type {:?} is not supported",
            rel.metadata_table_type
        ));
    }
    let column_defs: Vec<ColumnDef> = cols
        .iter()
        .map(|c| ColumnDef {
            name: c.name.clone(),
            data_type: c.data_type.clone(),
            nullable: c.nullable,
            write_default: None,
        })
        .collect();
    let output_columns: Vec<OutputColumn> = cols
        .iter()
        .map(|c| OutputColumn {
            name: c.name.clone(),
            data_type: c.data_type.clone(),
            nullable: c.nullable,
        })
        .collect();
    let serialized_table = rel
        .table
        .iceberg_table
        .as_ref()
        .and_then(|i| i.serialized_metadata.clone())
        .ok_or_else(|| {
            format!(
                "iceberg metadata table {} requires serialized metadata; \
                 table was not loaded through an iceberg catalog",
                rel.table.name
            )
        })?;
    let cloud_properties = match &rel.table.storage {
        TableStorage::S3ParquetFiles {
            cloud_properties, ..
        } => cloud_properties.clone(),
        _ => Default::default(),
    };
    let synthetic_name = format!("{}__nr_meta__", rel.table.name);
    let synthetic_table = TableDef {
        name: synthetic_name,
        columns: column_defs,
        iceberg_row_lineage_metadata_columns: vec![],
        iceberg_table: rel.table.iceberg_table.clone(),
        storage: TableStorage::IcebergMetadataTable {
            metadata_table_type: rel.metadata_table_type,
            serialized_table,
            cloud_properties,
        },
    };
    Ok(LogicalPlan::Scan(ScanNode {
        database: rel.database,
        table: synthetic_table,
        alias: rel.alias,
        columns: output_columns,
        predicates: vec![],
        required_columns: None,
    }))
}

// ---------------------------------------------------------------------------
// Set operation planning
// ---------------------------------------------------------------------------

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
                            write_default: None,
                        },
                        ColumnDef {
                            name: "o_custkey".to_string(),
                            data_type: arrow::datatypes::DataType::Int64,
                            nullable: false,
                            write_default: None,
                        },
                    ],
                    iceberg_row_lineage_metadata_columns: vec![],
                    iceberg_table: None,
                    storage: TableStorage::LocalParquetFile {
                        path: std::path::PathBuf::from("/tmp/orders.parquet"),
                    },
                }),
                "maps" => Ok(TableDef {
                    name: "maps".to_string(),
                    columns: vec![ColumnDef {
                        name: "m".to_string(),
                        data_type: arrow::datatypes::DataType::Map(
                            std::sync::Arc::new(arrow::datatypes::Field::new(
                                "entries",
                                arrow::datatypes::DataType::Struct(
                                    vec![
                                        std::sync::Arc::new(arrow::datatypes::Field::new(
                                            "key",
                                            arrow::datatypes::DataType::Int32,
                                            true,
                                        )),
                                        std::sync::Arc::new(arrow::datatypes::Field::new(
                                            "value",
                                            arrow::datatypes::DataType::Int32,
                                            true,
                                        )),
                                    ]
                                    .into(),
                                ),
                                false,
                            )),
                            false,
                        ),
                        nullable: true,
                        write_default: None,
                    }],
                    iceberg_row_lineage_metadata_columns: vec![],
                    iceberg_table: None,
                    storage: TableStorage::LocalParquetFile {
                        path: std::path::PathBuf::from("/tmp/maps.parquet"),
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
    fn test_sum_map_subscript_plans_as_aggregate() {
        let plan = parse_analyze_and_plan("SELECT sum_map(m)[1] FROM maps")
            .expect("planner should succeed");

        match plan {
            LogicalPlan::Project(project) => match *project.input {
                LogicalPlan::Aggregate(agg) => {
                    assert_eq!(agg.aggregates.len(), 1);
                    assert_eq!(agg.aggregates[0].name, "sum_map");
                }
                other => panic!("expected Aggregate under Project, got {other:?}"),
            },
            other => panic!("expected Project root, got {other:?}"),
        }
    }

    #[test]
    fn group_by_alias_expression_projects_aggregate_group_key() {
        let plan = parse_analyze_and_plan(
            "SELECT o_orderkey % 2 AS g, count(*) FROM orders GROUP BY g ORDER BY g",
        )
        .expect("planner should succeed");

        let LogicalPlan::Sort(sort) = plan else {
            panic!("expected Sort root");
        };
        let LogicalPlan::Project(project) = *sort.input else {
            panic!("expected Project under Sort");
        };
        let ExprKind::ColumnRef { qualifier, column } = &project.items[0].expr.kind else {
            panic!(
                "expected group key projection to be a ColumnRef, got {:?}",
                project.items[0].expr
            );
        };
        assert!(qualifier.is_none());
        assert_eq!(column, "o_orderkey % 2");
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
