//! Logical Planner — converts [`ResolvedQuery`] into [`LogicalPlan`].
//!
//! This is a structural transformation that builds a relational algebra tree
//! from the analyzed query IR.  A future optimizer would rewrite this tree
//! before it reaches the Thrift emitter.

pub(crate) mod plan;

use crate::sql::analysis::cte::CTERegistry;
use crate::sql::analysis::*;
use crate::sql::catalog::{IcebergDataFileInfo, IcebergDeleteFileContent};
use crate::sql::codegen::helpers::typed_expr_display_name;
use crate::sql::column_id::{ColumnId, ColumnRefFactory};
use plan::*;

/// Extract ColumnId from a TypedExpr, or allocate a new one from the factory.
fn expr_column_id(expr: &TypedExpr, name: &str, factory: &mut ColumnRefFactory) -> ColumnId {
    if let ExprKind::ColumnRef { column_id, .. } = &expr.kind {
        *column_id
    } else {
        factory.create(
            None,
            name.to_string(),
            expr.data_type.clone(),
            expr.nullable,
        )
    }
}

// ---------------------------------------------------------------------------
// Public entry
// ---------------------------------------------------------------------------

/// Plan a resolved query into a single logical tree, wrapping CTE definitions
/// as nested anchor/produce pairs around the main query subtree.
pub(crate) fn plan_query(
    resolved: ResolvedQuery,
    cte_registry: CTERegistry,
    factory: &mut ColumnRefFactory,
) -> Result<LogicalPlan, String> {
    plan_scoped_query(resolved, &cte_registry, factory)
}

fn plan_scoped_query(
    resolved: ResolvedQuery,
    cte_registry: &CTERegistry,
    factory: &mut ColumnRefFactory,
) -> Result<LogicalPlan, String> {
    let ResolvedQuery {
        body,
        order_by,
        limit,
        offset,
        output_columns,
        local_cte_ids,
    } = resolved;

    // Plan the query body first so we can stamp fresh set-op ColumnIds before
    // apply_query_modifiers consumes output_columns.
    let mut body_plan = plan_body_scoped(body, cte_registry, factory)?;

    // Strategy A: if the body produced a set-op node (Union/Intersect/Except),
    // overwrite its output_columns with the fresh ColumnIds that the analyzer
    // allocated for this query's output (stored in `output_columns`).  The
    // planner previously left branch-side ColumnIds in those fields, which
    // disagreed with the fresh IDs that the parent scope (and any wrapping
    // SubqueryAliasNode) uses to reference the set-op output.
    match &mut body_plan {
        LogicalPlan::Union(node) => {
            node.output_columns = output_columns.clone();
        }
        LogicalPlan::Intersect(node) => {
            node.output_columns = output_columns.clone();
        }
        LogicalPlan::Except(node) => {
            node.output_columns = output_columns.clone();
        }
        _ => {}
    }

    let mut root =
        apply_query_modifiers(body_plan, order_by, output_columns, limit, offset, factory);

    for cte_id in local_cte_ids.into_iter().rev() {
        let entry = cte_registry
            .get(cte_id)
            .ok_or_else(|| format!("missing CTE entry for id {cte_id}"))?;
        let produce_input = plan_scoped_query(entry.resolved_query.clone(), cte_registry, factory)?;
        let produce = LogicalPlan::CTEProduce(CTEProduceNode {
            cte_id: entry.id,
            input: Box::new(produce_input),
            output_columns: entry.output_columns.clone(),
            required_output_columns: None,
        });
        root = LogicalPlan::CTEAnchor(CTEAnchorNode {
            cte_id: entry.id,
            produce: Box::new(produce),
            consumer: Box::new(root),
            required_output_columns: None,
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
    factory: &mut ColumnRefFactory,
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
            // Each tuple: (user-visible output name, data type, nullable, inner output_column_id).
            // The inner output_column_id is captured here so the outer strip-project can
            // reference the same ColumnId that the inner Project produces, preserving id
            // continuity through the double-Project barrier for the Phase-1 tagging pass.
            let user_select: Option<Vec<(String, arrow::datatypes::DataType, bool, ColumnId)>> =
                if let LogicalPlan::Project(ref mut proj) = body_plan {
                    if let LogicalPlan::Aggregate(ref mut agg) = *proj.input {
                        for extra in &extra_items {
                            collect_aggregates(&extra.expr, &mut agg.aggregates);
                        }
                    }
                    let user: Vec<(String, arrow::datatypes::DataType, bool, ColumnId)> = proj
                        .items
                        .iter()
                        .map(|it| {
                            (
                                it.output_name.clone(),
                                it.expr.data_type.clone(),
                                it.expr.nullable,
                                it.output_column_id,
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
                    .map(|(idx, (name, _, _, _))| (name.to_lowercase(), idx))
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
                // Top-level ORDER BY — no analytic partition.
                analytic_partition_by: Vec::new(),
                required_output_columns: None,
            });

            // Strip synthetic sort-only columns after LIMIT/OFFSET so the
            // limit stays directly above Sort and can be rewritten to TopN.
            final_projection = Some(if let Some(user) = user_select {
                user.into_iter()
                    .enumerate()
                    .map(|(idx, (name, dt, nullable, inner_cid))| {
                        let syn_name = format!("__nr_sel_{idx}");
                        // Reuse the inner project item's existing ColumnId so
                        // that the Phase-1 tagging pass can thread required
                        // columns through the double-Project barrier without
                        // encountering an id discontinuity. Minting a fresh id
                        // here would make the outer Project's output invisible
                        // to the inner Project's pruning tag.
                        let cid = inner_cid;
                        ProjectItem {
                            expr: TypedExpr {
                                kind: ExprKind::ColumnRef {
                                    column_id: cid,
                                    qualifier: None,
                                    column: syn_name,
                                },
                                data_type: dt,
                                nullable,
                            },
                            output_name: name,
                            output_column_id: cid,
                        }
                    })
                    .collect()
            } else {
                output_columns
                    .iter()
                    .map(|col| ProjectItem {
                        expr: TypedExpr {
                            kind: ExprKind::ColumnRef {
                                column_id: col.column_id,
                                qualifier: None,
                                column: col.name.clone(),
                            },
                            data_type: col.data_type.clone(),
                            nullable: col.nullable,
                        },
                        output_name: col.name.clone(),
                        output_column_id: col.column_id,
                    })
                    .collect()
            });
        } else {
            body_plan = LogicalPlan::Sort(SortNode {
                input: Box::new(body_plan),
                items: sort_items,
                // Top-level ORDER BY — no analytic partition.
                analytic_partition_by: Vec::new(),
                required_output_columns: None,
            });
        }
    }

    // Wrap with Limit if LIMIT/OFFSET is present.
    if limit.is_some() || offset.is_some() {
        body_plan = LogicalPlan::Limit(LimitNode {
            input: Box::new(body_plan),
            limit,
            offset,
            required_output_columns: None,
        });
    }

    if let Some(items) = final_projection {
        body_plan = LogicalPlan::Project(ProjectNode {
            input: Box::new(body_plan),
            items,
            required_output_columns: None,
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
            let output_column_id = if let ExprKind::ColumnRef { column_id, .. } = &item.expr.kind {
                *column_id
            } else {
                ColumnId::UNSET
            };
            extra.push(ProjectItem {
                expr: item.expr.clone(),
                output_name,
                output_column_id,
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
            ..
        } => {
            if let Some(idx) = name_to_idx.get(&column.to_lowercase()) {
                TypedExpr {
                    data_type: expr.data_type,
                    nullable: expr.nullable,
                    kind: ExprKind::ColumnRef {
                        column_id: ColumnId::UNSET,
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
                // Preserve the extra item's output_column_id so that the
                // Phase-1 tagging pass (tag_sort → collect_column_id_refs)
                // can see this sort key's ColumnId and include it in the
                // child's required_output_columns.  Using UNSET here caused
                // tag_sort to silently omit the extra column from the inner
                // project's needed set, which then made PruneProjectColumns
                // drop the extra item → the sort's input no longer had the
                // column → "Column cannot be resolved" at codegen time.
                SortItem {
                    expr: TypedExpr {
                        kind: ExprKind::ColumnRef {
                            column_id: extra.output_column_id,
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

fn plan_body_scoped(
    body: QueryBody,
    cte_registry: &CTERegistry,
    factory: &mut ColumnRefFactory,
) -> Result<LogicalPlan, String> {
    match body {
        QueryBody::Select(select) => plan_select_scoped(select, cte_registry, factory),
        QueryBody::SetOperation(set_op) => plan_set_operation_scoped(set_op, cte_registry, factory),
        QueryBody::Values(values) => plan_values(values, factory),
    }
}

// ---------------------------------------------------------------------------
// SELECT planning
// ---------------------------------------------------------------------------

fn plan_select_scoped(
    mut select: ResolvedSelect,
    cte_registry: &CTERegistry,
    factory: &mut ColumnRefFactory,
) -> Result<LogicalPlan, String> {
    const REPEAT_GROUP_QUALIFIER: &str = "__repeat_group";

    let mut current = match select.from.take() {
        Some(relation) => plan_relation_scoped(relation, cte_registry, factory)?,
        None => LogicalPlan::Values(ValuesNode {
            rows: vec![vec![]],
            columns: vec![],
            required_output_columns: None,
        }),
    };

    if let Some(predicate) = select.filter.take() {
        current = LogicalPlan::Filter(FilterNode {
            input: Box::new(current),
            predicate,
            required_output_columns: None,
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
            required_output_columns: None,
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
            factory,
        );
        current = LogicalPlan::Aggregate(AggregateNode {
            input: Box::new(current),
            group_by: select.group_by,
            aggregates: agg_calls,
            output_columns,
            already_pushed: false,
            required_output_columns: None,
        });
        if let Some(having) = select.having {
            current = LogicalPlan::Filter(FilterNode {
                input: Box::new(current),
                predicate: having,
                required_output_columns: None,
            });
        }

        current = build_window_and_project(current, project_items, &select.projection, factory)?;
    } else {
        current = build_window_and_project(
            current,
            select.projection.clone(),
            &select.projection,
            factory,
        )?;
    }

    // SELECT DISTINCT → Aggregate on all output columns (deduplication)
    if select.distinct {
        current = build_distinct(current, &select.projection, factory);
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

    // Materialize each rollup key expression under its alias and prepare
    // a substitution map. The rule used to only materialize ColumnRef
    // group_by entries (e.g. `GROUP BY ROLLUP(k1)`); a synthetic non-ref
    // expression — most commonly the `COALESCE(left.k, right.k)` introduced
    // by `FULL OUTER JOIN ... USING(k)` — was index-aligned with
    // `all_rollup_columns` but skipped here, so the Repeat node had no
    // slot to null out at higher rollup levels and the per-level null
    // pattern silently devolved into duplicates (see
    // `join_full_outer_with_using` step 40: 39 vs 23 expected rows).
    //
    // Walk index-aligned: `all_rollup_columns[i]` is the AST text of
    // `select.group_by[i]`, so use the analysed group_by expression at
    // the same index as the source of the materialised projection item.
    // Build a substitution table keyed by the original expression's
    // display name so a later pass can rewrite projection / having
    // occurrences of the same expression to a ColumnRef on the alias.
    let mut substitutions: Vec<(String, TypedExpr)> = Vec::new();
    for (idx, (_, alias_name)) in grouping_key_aliases.iter().enumerate() {
        let Some(source_expr) = select.group_by.get(idx).cloned() else {
            continue;
        };
        let data_type = source_expr.data_type.clone();
        let nullable = source_expr.nullable;
        let original_display = typed_expr_display_name(&source_expr);

        // Substitute downstream references to the original expression with a
        // ColumnRef on the alias. For the ColumnRef case the existing
        // `repeat_group_qualifier` form keeps the column name (so
        // visit_repeat's `add_qualified_alias(__repeat_group, k1, …)`
        // wiring still resolves). For non-ColumnRef cases we point at the
        // alias slot directly so the AGGREGATE above REPEAT reads the
        // per-level nullified value, not the pre-REPEAT input.
        let replacement = match &source_expr.kind {
            ExprKind::ColumnRef {
                column_id, column, ..
            } => TypedExpr {
                kind: ExprKind::ColumnRef {
                    column_id: *column_id,
                    qualifier: Some(repeat_group_qualifier.to_string()),
                    column: column.clone(),
                },
                data_type,
                nullable,
            },
            _ => TypedExpr {
                kind: ExprKind::ColumnRef {
                    column_id: ColumnId::UNSET,
                    qualifier: None,
                    column: alias_name.clone(),
                },
                data_type,
                nullable,
            },
        };
        substitutions.push((original_display, replacement));

        let output_column_id = if let ExprKind::ColumnRef { column_id, .. } = &source_expr.kind {
            *column_id
        } else {
            ColumnId::UNSET
        };
        project_items.push(ProjectItem {
            expr: source_expr,
            output_name: alias_name.clone(),
            output_column_id,
        });
    }

    *current = LogicalPlan::Project(ProjectNode {
        input: Box::new(current.clone()),
        items: project_items,
        required_output_columns: None,
    });

    // Apply substitutions to group_by, projection, having so that every
    // place the original rollup-key expression appeared now reads from
    // the materialized alias slot.
    for gb_expr in &mut select.group_by {
        substitute_expr_in_place(gb_expr, &substitutions);
    }
    for item in &mut select.projection {
        substitute_expr_in_place(&mut item.expr, &substitutions);
    }
    if let Some(having_expr) = select.having.as_mut() {
        substitute_expr_in_place(having_expr, &substitutions);
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

/// In-place substitution: when any sub-expression's `typed_expr_display_name`
/// matches an entry's first field, replace that sub-expression with the
/// second field. Walks AggregateCall / FunctionCall / BinaryOp / UnaryOp /
/// IsNull / Cast / Case / InList / Nested children recursively.
///
/// Used after `prepare_repeat_input` to rewrite group-by / projection /
/// having references to the original rollup-key expression into ColumnRefs
/// on the materialised alias slot — so the REPEAT operator's per-level
/// nullification of that slot drives the grouping key, instead of being
/// recomputed from the pre-REPEAT input.
fn substitute_expr_in_place(expr: &mut TypedExpr, substitutions: &[(String, TypedExpr)]) {
    let name = typed_expr_display_name(expr);
    if let Some((_, replacement)) = substitutions.iter().find(|(n, _)| n == &name) {
        *expr = replacement.clone();
        return;
    }
    match &mut expr.kind {
        ExprKind::AggregateCall { args, order_by, .. } => {
            for a in args {
                substitute_expr_in_place(a, substitutions);
            }
            for s in order_by {
                substitute_expr_in_place(&mut s.expr, substitutions);
            }
        }
        ExprKind::FunctionCall { args, .. } => {
            for a in args {
                substitute_expr_in_place(a, substitutions);
            }
        }
        ExprKind::BinaryOp { left, right, .. } => {
            substitute_expr_in_place(left, substitutions);
            substitute_expr_in_place(right, substitutions);
        }
        ExprKind::UnaryOp { expr: inner, .. } => substitute_expr_in_place(inner, substitutions),
        ExprKind::IsNull { expr: inner, .. } => substitute_expr_in_place(inner, substitutions),
        ExprKind::Cast { expr: inner, .. } => substitute_expr_in_place(inner, substitutions),
        ExprKind::Case {
            operand,
            when_then,
            else_expr,
        } => {
            if let Some(op) = operand {
                substitute_expr_in_place(op, substitutions);
            }
            for (w, t) in when_then {
                substitute_expr_in_place(w, substitutions);
                substitute_expr_in_place(t, substitutions);
            }
            if let Some(e) = else_expr {
                substitute_expr_in_place(e, substitutions);
            }
        }
        ExprKind::InList {
            expr: inner, list, ..
        } => {
            substitute_expr_in_place(inner, substitutions);
            for v in list {
                substitute_expr_in_place(v, substitutions);
            }
        }
        ExprKind::Nested(inner) => substitute_expr_in_place(inner, substitutions),
        // ColumnRef, Literal, LambdaParamRef, SubqueryPlaceholder, etc. —
        // either leaves with no sub-exprs or contexts where substitution
        // would change semantics. Top-level match above already handles
        // any whole-expr replacement.
        _ => {}
    }
}

fn collect_repeat_input_refs(
    expr: &TypedExpr,
    out: &mut Vec<ProjectItem>,
    seen: &mut std::collections::HashSet<(Option<String>, String)>,
) {
    match &expr.kind {
        ExprKind::ColumnRef {
            qualifier,
            column,
            column_id,
            ..
        } => {
            if qualifier.is_none() && column.starts_with("__grouping_") {
                return;
            }
            let key = (qualifier.clone(), column.to_lowercase());
            if seen.insert(key) {
                out.push(ProjectItem {
                    expr: expr.clone(),
                    output_name: column.clone(),
                    output_column_id: *column_id,
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
fn build_distinct(
    input: LogicalPlan,
    projection: &[ProjectItem],
    factory: &mut ColumnRefFactory,
) -> LogicalPlan {
    let mut group_by = Vec::new();
    let mut output_columns = Vec::new();
    for item in projection {
        // Prefer the pre-assigned output_column_id (e.g. synthetic __match_N
        // columns from IN/EXISTS subquery rewrites). Falling back to
        // expr_column_id would mint a fresh id, disconnecting the column from
        // any downstream reference that already holds the original id.
        let cid = if item.output_column_id != ColumnId::UNSET {
            item.output_column_id
        } else {
            expr_column_id(&item.expr, &item.output_name, factory)
        };
        group_by.push(TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: cid,
                qualifier: None,
                column: item.output_name.clone(),
            },
            data_type: item.expr.data_type.clone(),
            nullable: item.expr.nullable,
        });
        output_columns.push(OutputColumn {
            column_id: cid,
            name: item.output_name.clone(),
            data_type: item.expr.data_type.clone(),
            nullable: item.expr.nullable,
            is_internal: false,
        });
    }
    LogicalPlan::Aggregate(AggregateNode {
        input: Box::new(input),
        group_by,
        aggregates: vec![],
        output_columns,
        already_pushed: false,
        required_output_columns: None,
    })
}

/// Check if an expression contains any WindowCall.
/// Build Window + Project nodes if the projection contains window functions,
/// otherwise just a Project node.
fn build_window_and_project(
    input: LogicalPlan,
    project_items: Vec<ProjectItem>,
    original_projection: &[ProjectItem],
    factory: &mut ColumnRefFactory,
) -> Result<LogicalPlan, String> {
    let has_window = project_items.iter().any(|item| has_window_call(&item.expr));
    if has_window {
        let (window_exprs, rewritten_items) = extract_window_calls(&project_items);
        let mut output_columns = Vec::new();
        for item in original_projection {
            output_columns.push(OutputColumn {
                column_id: factory.create(
                    None,
                    item.output_name.clone(),
                    item.expr.data_type.clone(),
                    item.expr.nullable,
                ),
                name: item.output_name.clone(),
                data_type: item.expr.data_type.clone(),
                nullable: item.expr.nullable,
                is_internal: false,
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
        // Tag the Sort with the window's partition columns so the optimizer
        // can require Hash(partition_by) distribution from the child instead
        // of forcing Gather — letting the sort run locally per analytic
        // partition. This mirrors StarRocks's
        // `TSortNode.analytic_partition_exprs` mechanism.
        let analytic_partition_by = first_win.partition_by.clone();
        let sorted_input = if sort_items.is_empty() {
            input
        } else {
            LogicalPlan::Sort(SortNode {
                input: Box::new(input),
                items: sort_items,
                analytic_partition_by,
                required_output_columns: None,
            })
        };

        let windowed = LogicalPlan::Window(WindowNode {
            input: Box::new(sorted_input),
            window_exprs,
            output_columns,
            required_output_columns: None,
        });
        Ok(LogicalPlan::Project(ProjectNode {
            input: Box::new(windowed),
            items: rewritten_items,
            required_output_columns: None,
        }))
    } else if !project_items.is_empty() {
        Ok(LogicalPlan::Project(ProjectNode {
            input: Box::new(input),
            items: project_items,
            required_output_columns: None,
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

/// Converse of a window-frame boundary: swap PRECEDING ↔ FOLLOWING (including
/// unbounded variants) and leave CURRENT_ROW alone. Matches StarRocks FE
/// `AnalyticWindowBoundary.BoundaryType.converse()`.
fn converse_window_bound(bound: &WindowBound) -> WindowBound {
    match bound {
        WindowBound::UnboundedPreceding => WindowBound::UnboundedFollowing,
        WindowBound::UnboundedFollowing => WindowBound::UnboundedPreceding,
        WindowBound::Preceding(n) => WindowBound::Following(*n),
        WindowBound::Following(n) => WindowBound::Preceding(*n),
        WindowBound::CurrentRow => WindowBound::CurrentRow,
    }
}

/// Reverse a window frame in place: new_start = converse(old_end),
/// new_end = converse(old_start). Mirrors StarRocks FE
/// `AnalyticWindow.reverse()`.
fn reverse_window_frame(frame: &WindowFrame) -> WindowFrame {
    WindowFrame {
        frame_type: frame.frame_type,
        start: converse_window_bound(&frame.end),
        end: converse_window_bound(&frame.start),
    }
}

/// Normalize a window frame so the BE only sees frames whose start is
/// UNBOUNDED PRECEDING. When the original frame ends at UNBOUNDED FOLLOWING
/// and does not start at UNBOUNDED PRECEDING, we reverse the ORDER BY
/// direction and converse the frame bounds. For FIRST_VALUE / LAST_VALUE we
/// also swap the function name because reversing the iteration flips which
/// row is "first" vs "last".
///
/// Mirrors StarRocks FE `WindowTransformer.visit(AnalyticExpr)`.
fn normalize_window_frame_for_be(
    name: &str,
    order_by: Vec<SortItem>,
    window_frame: Option<WindowFrame>,
) -> (String, Vec<SortItem>, Option<WindowFrame>) {
    let Some(frame) = window_frame else {
        return (name.to_string(), order_by, None);
    };

    let needs_reverse = matches!(frame.end, WindowBound::UnboundedFollowing)
        && !matches!(frame.start, WindowBound::UnboundedPreceding);
    if !needs_reverse {
        return (name.to_string(), order_by, Some(frame));
    }

    let reversed_order_by = order_by
        .into_iter()
        .map(|item| SortItem {
            expr: item.expr,
            asc: !item.asc,
            nulls_first: !item.nulls_first,
        })
        .collect();
    let reversed_frame = reverse_window_frame(&frame);

    let reversed_name = match name.to_ascii_lowercase().as_str() {
        "first_value" => "last_value".to_string(),
        "last_value" => "first_value".to_string(),
        _ => name.to_string(),
    };

    (reversed_name, reversed_order_by, Some(reversed_frame))
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
                output_column_id: item.output_column_id,
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

            // Normalize frames that end at UNBOUNDED FOLLOWING by reversing the
            // ORDER BY direction and converse-ing the frame bounds, so the BE
            // only sees frames whose start is UNBOUNDED PRECEDING. This mirrors
            // StarRocks FE `WindowTransformer.visit(AnalyticExpr)` which reverses
            // such frames before lowering; the BE analytor relies on this
            // invariant (it `DCHECK`s !window_start for RANGE and assumes
            // cumulative processing).
            //
            // For FIRST_VALUE / LAST_VALUE the reversal also swaps the function
            // because reversing the iteration direction inverts which row is
            // "first" vs "last".
            let (rewritten_name, rewritten_order_by, rewritten_frame) =
                normalize_window_frame_for_be(name, order_by.clone(), window_frame.clone());

            window_exprs.push(WindowExpr {
                name: rewritten_name,
                args: args.clone(),
                distinct: *distinct,
                partition_by: partition_by.clone(),
                order_by: rewritten_order_by,
                window_frame: rewritten_frame,
                result_type: expr.data_type.clone(),
                output_name: win_output_name.clone(),
                ignore_nulls: *ignore_nulls,
            });
            TypedExpr {
                kind: ExprKind::ColumnRef {
                    column_id: ColumnId::UNSET,
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
    factory: &mut ColumnRefFactory,
) -> (Vec<ProjectItem>, Vec<AggregateCall>, Vec<OutputColumn>) {
    let mut agg_calls = Vec::new();
    let mut output_columns = Vec::new();
    let mut project_items = Vec::with_capacity(projection.len());

    // Collect aggregate calls from projection
    for item in projection {
        collect_aggregates(&item.expr, &mut agg_calls);
        output_columns.push(OutputColumn {
            column_id: expr_column_id(&item.expr, &item.output_name, factory),
            name: item.output_name.clone(),
            data_type: item.expr.data_type.clone(),
            nullable: item.expr.nullable,
            is_internal: false,
        });
        project_items.push(ProjectItem {
            expr: rewrite_exact_group_by_expr_ref(&item.expr, group_by),
            output_name: item.output_name.clone(),
            output_column_id: item.output_column_id,
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
            // Reuse the group-by expression's ColumnId if it is a ColumnRef.
            let cid = if let ExprKind::ColumnRef { column_id, .. } = &gb.kind {
                *column_id
            } else if let ExprKind::ColumnRef { column_id, .. } = &expr.kind {
                *column_id
            } else {
                ColumnId::UNSET
            };
            return TypedExpr {
                kind: ExprKind::ColumnRef {
                    column_id: cid,
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
        ExprKind::ColumnRef {
            qualifier, column, ..
        } => {
            if !inside_agg {
                // Check if this column is already in group_by
                let already_grouped = group_by.iter().any(|gb| {
                    matches!(&gb.kind, ExprKind::ColumnRef { qualifier: gq, column: gc, .. }
                        if gc == column && gq == qualifier)
                });
                // Check if already collected
                let already_collected = out.iter().any(|o| {
                    matches!(&o.kind, ExprKind::ColumnRef { qualifier: oq, column: oc, .. }
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
    factory: &mut ColumnRefFactory,
) -> Result<LogicalPlan, String> {
    match relation {
        Relation::Scan(scan) => {
            // G1: reuse the ColumnIds the analyzer already minted for this
            // table's columns (carried on `scan.column_ids`). Minting fresh
            // ids here would desync the analyzer-produced `ColumnRef`s in
            // the rest of the plan (Window PARTITION BY, GROUP BY, ORDER BY,
            // join eq keys, etc.) from the scan output, and distribution
            // matching would fail.
            let columns = scan
                .table
                .columns
                .iter()
                .enumerate()
                .map(|(idx, c)| OutputColumn {
                    column_id: scan.column_ids.get(idx).copied().unwrap_or_else(|| {
                        factory.create(
                            scan.alias.as_ref().or(Some(&scan.table.name)).cloned(),
                            c.name.clone(),
                            c.data_type.clone(),
                            c.nullable,
                        )
                    }),
                    name: c.name.clone(),
                    data_type: c.data_type.clone(),
                    nullable: c.nullable,
                    is_internal: false,
                })
                .collect();
            Ok(LogicalPlan::Scan(ScanNode {
                database: scan.database,
                table: scan.table,
                alias: scan.alias,
                columns,
                predicates: vec![],
                required_columns: None,
                dict_columns: vec![],
                required_output_columns: None,
            }))
        }
        Relation::Subquery {
            query,
            alias,
            output_columns,
        } => {
            let inner_plan = plan_scoped_query(*query, cte_registry, factory)?;
            // SubqueryAlias MUST reuse the inner query's ColumnIds.
            // The output_columns from the analyzer already carry the right ids.
            Ok(LogicalPlan::SubqueryAlias(SubqueryAliasNode {
                input: Box::new(inner_plan),
                alias,
                output_columns,
                required_output_columns: None,
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
                    let left = plan_relation_scoped(left, cte_registry, factory)?;
                    Ok(LogicalPlan::TableFunction(TableFunctionNode {
                        input: Box::new(left),
                        function_name: "unnest".to_string(),
                        args: unnest.args,
                        output_columns: unnest.output_columns,
                        alias: unnest.alias,
                        is_left_join,
                        required_output_columns: None,
                    }))
                }
                right => {
                    let left = plan_relation_scoped(left, cte_registry, factory)?;
                    let right = plan_relation_scoped(right, cte_registry, factory)?;
                    Ok(LogicalPlan::Join(JoinNode {
                        left: Box::new(left),
                        right: Box::new(right),
                        join_type,
                        condition,
                        required_output_columns: None,
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
            required_output_columns: None,
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
            required_output_columns: None,
        })),
        Relation::IcebergMetadataScan(rel) => plan_iceberg_metadata_scan(rel, factory),
        Relation::IcebergDeltaScan(rel) => plan_iceberg_delta_scan(rel, factory),
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
/// `ScanSource::IcebergMetadataTable` source. The optimizer treats it
/// like any other Scan; codegen branches on the source variant to emit
/// an `HDFS_SCAN_NODE` whose lowering wires up the native-Rust
/// `IcebergMetadataScanOp` (no JVM / JNI bridge — the embedded-Java
/// path was removed in favor of iceberg-rust).
fn plan_iceberg_metadata_scan(
    rel: IcebergMetadataScanRelation,
    factory: &mut ColumnRefFactory,
) -> Result<LogicalPlan, String> {
    use crate::sql::analyzer::iceberg_metadata::metadata_table_schema;
    use crate::sql::catalog::{ColumnDef, ScanSource, TableDef};

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
            logical_type: None,
        })
        .collect();
    // Reuse the ColumnIds that the analyzer already minted for this metadata
    // table's columns (carried on `rel.column_ids`). Creating fresh ids here
    // would desync the `ColumnRef` ids in the rest of the plan (SELECT list,
    // WHERE, etc.) from the scan's output_columns, causing Phase-2 column
    // pruning to incorrectly prune needed columns (same pattern as Relation::Scan).
    let output_columns: Vec<OutputColumn> = cols
        .iter()
        .enumerate()
        .map(|(idx, c)| OutputColumn {
            column_id: rel.column_ids.get(idx).copied().unwrap_or_else(|| {
                factory.create(None, c.name.clone(), c.data_type.clone(), c.nullable)
            }),
            name: c.name.clone(),
            data_type: c.data_type.clone(),
            nullable: c.nullable,
            is_internal: false,
        })
        .collect();
    let table_info = iceberg_table_info(&rel.table.source)
        .ok_or_else(|| {
            format!(
                "iceberg metadata table {} requires iceberg table identity; \
                 table was not loaded through an iceberg catalog",
                rel.table.name
            )
        })?
        .clone();
    let serialized_table = table_info.serialized_metadata.clone().ok_or_else(|| {
        format!(
            "iceberg metadata table {} requires serialized metadata; \
                 table was not loaded through an iceberg catalog",
            rel.table.name
        )
    })?;
    let cloud_properties = match &rel.table.source {
        ScanSource::IcebergDataFiles {
            cloud_properties, ..
        } => cloud_properties.clone(),
        _ => Default::default(),
    };
    let metadata_payload =
        build_iceberg_metadata_payload(&rel.metadata_table_type, &rel.table.source)?;
    let synthetic_name = format!("{}__nr_meta__", rel.table.name);
    let synthetic_table = TableDef {
        name: synthetic_name,
        columns: column_defs,
        iceberg_row_lineage_metadata_columns: vec![],
        source: ScanSource::IcebergMetadataTable {
            table: table_info,
            metadata_table_type: rel.metadata_table_type,
            serialized_table,
            cloud_properties,
            metadata_payload,
        },
    };
    Ok(LogicalPlan::Scan(ScanNode {
        database: rel.database,
        table: synthetic_table,
        alias: rel.alias,
        columns: output_columns,
        predicates: vec![],
        required_columns: None,
        dict_columns: vec![],
        required_output_columns: None,
    }))
}

#[derive(Default)]
struct PartitionMetadataAgg {
    record_count: i64,
    file_count: i64,
    total_data_file_size_in_bytes: i64,
    position_delete_files: std::collections::BTreeSet<String>,
    equality_delete_files: std::collections::BTreeSet<String>,
}

fn build_iceberg_metadata_payload(
    metadata_table_type: &crate::connector::iceberg::IcebergMetadataTableType,
    storage: &crate::sql::catalog::ScanSource,
) -> Result<Option<String>, String> {
    use crate::connector::iceberg::IcebergMetadataTableType;
    use crate::sql::catalog::ScanSource;
    match metadata_table_type {
        IcebergMetadataTableType::Partitions => {
            let ScanSource::IcebergDataFiles { files, .. } = storage else {
                return Err(
                    "iceberg partitions metadata table requires catalog-resolved data files"
                        .to_string(),
                );
            };
            build_iceberg_partitions_payload(files).map(Some)
        }
        _ => Ok(None),
    }
}

fn build_iceberg_partitions_payload(files: &[IcebergDataFileInfo]) -> Result<String, String> {
    let mut groups = std::collections::BTreeMap::<(i32, String), PartitionMetadataAgg>::new();
    for file in files {
        let spec_id = file.partition_spec_id.ok_or_else(|| {
            format!(
                "iceberg partitions metadata requires partition spec id for data file {}",
                file.path
            )
        })?;
        let record_count = file.row_count.ok_or_else(|| {
            format!(
                "iceberg partitions metadata requires record_count for data file {}",
                file.path
            )
        })?;
        let partition_key = file
            .partition_key
            .clone()
            .unwrap_or_else(|| "Struct([])".to_string());
        let agg = groups.entry((spec_id, partition_key)).or_default();
        agg.record_count = agg
            .record_count
            .checked_add(record_count)
            .ok_or_else(|| "iceberg partitions metadata record_count overflow".to_string())?;
        agg.file_count = agg
            .file_count
            .checked_add(1)
            .ok_or_else(|| "iceberg partitions metadata file_count overflow".to_string())?;
        agg.total_data_file_size_in_bytes = agg
            .total_data_file_size_in_bytes
            .checked_add(file.size)
            .ok_or_else(|| {
                "iceberg partitions metadata total_data_file_size_in_bytes overflow".to_string()
            })?;
        for delete_file in &file.delete_files {
            match delete_file.file_content {
                IcebergDeleteFileContent::Position => {
                    agg.position_delete_files.insert(delete_file.path.clone());
                }
                IcebergDeleteFileContent::Equality => {
                    agg.equality_delete_files.insert(delete_file.path.clone());
                }
            }
        }
    }
    let rows = groups
        .into_iter()
        .map(
            |((spec_id, partition), agg)| -> Result<serde_json::Value, String> {
                let position_delete_file_count = i64::try_from(agg.position_delete_files.len())
                    .map_err(|_| {
                        "iceberg partitions metadata position_delete_file_count overflow"
                            .to_string()
                    })?;
                let equality_delete_file_count = i64::try_from(agg.equality_delete_files.len())
                    .map_err(|_| {
                        "iceberg partitions metadata equality_delete_file_count overflow"
                            .to_string()
                    })?;
                Ok(serde_json::json!({
                    "spec_id": spec_id,
                    "partition": partition,
                    "record_count": agg.record_count,
                    "file_count": agg.file_count,
                    "total_data_file_size_in_bytes": agg.total_data_file_size_in_bytes,
                    "position_delete_file_count": position_delete_file_count,
                    "equality_delete_file_count": equality_delete_file_count,
                }))
            },
        )
        .collect::<Result<Vec<_>, _>>()?;
    serde_json::to_string(&serde_json::json!({
        "version": 1,
        "rows": rows,
    }))
    .map_err(|e| format!("serialize iceberg partitions metadata payload failed: {e}"))
}

/// Lower an analyzer-built `IcebergDeltaScanRelation` into a regular
/// `LogicalPlan::Scan` whose `TableDef` carries the synthetic
/// `ScanSource::IcebergDeltaTable` storage. Codegen recognizes this
/// storage variant and emits `TPlanNodeType::ICEBERG_DELTA_SCAN_NODE`
/// (rather than `HDFS_SCAN_NODE`); the lowering layer resolves the
/// actual change file list via `connector::iceberg::changes::plan_changes`.
fn plan_iceberg_delta_scan(
    rel: IcebergDeltaScanRelation,
    factory: &mut ColumnRefFactory,
) -> Result<LogicalPlan, String> {
    use crate::sql::catalog::{ScanSource, TableDef};

    // Output schema: base columns + iceberg v3 row-lineage metadata columns.
    // The delta scan emits both: scanner-side projection re-uses the same
    // column ordering as the base scan, plus the row-lineage virtual columns
    // for downstream row-identity matching.
    //
    // Reuse the ColumnIds that the analyzer already minted for this delta scan's
    // columns (carried on `rel.column_ids`). Creating fresh ids here would desync
    // the `ColumnRef` ids in the rest of the plan from the scan's output columns,
    // causing Phase-2 column pruning to incorrectly prune needed scan columns.
    let base_col_count = rel.table.columns.len();
    let mut output_columns: Vec<OutputColumn> = rel
        .table
        .columns
        .iter()
        .enumerate()
        .map(|(idx, c)| OutputColumn {
            column_id: rel.column_ids.get(idx).copied().unwrap_or_else(|| {
                factory.create(None, c.name.clone(), c.data_type.clone(), c.nullable)
            }),
            name: c.name.clone(),
            data_type: c.data_type.clone(),
            nullable: c.nullable,
            is_internal: false,
        })
        .collect();
    for (meta_idx, col) in rel
        .table
        .iceberg_row_lineage_metadata_columns
        .iter()
        .enumerate()
    {
        let col_id_idx = base_col_count + meta_idx;
        output_columns.push(OutputColumn {
            column_id: rel.column_ids.get(col_id_idx).copied().unwrap_or_else(|| {
                factory.create(None, col.name.clone(), col.data_type.clone(), col.nullable)
            }),
            name: col.name.clone(),
            data_type: col.data_type.clone(),
            nullable: col.nullable,
            is_internal: false,
        });
    }
    let table_info = iceberg_table_info(&rel.table.source)
        .ok_or_else(|| {
            format!(
                "iceberg delta scan {}.{}.{} requires iceberg table identity",
                rel.catalog, rel.namespace, rel.table_name
            )
        })?
        .clone();
    let synthetic_table = TableDef {
        name: rel.table.name.clone(),
        columns: rel.table.columns.clone(),
        iceberg_row_lineage_metadata_columns: rel
            .table
            .iceberg_row_lineage_metadata_columns
            .clone(),
        source: ScanSource::IcebergDeltaTable {
            table: table_info,
            from_snapshot_id: rel.from_snapshot_id,
            to_snapshot_id: rel.to_snapshot_id,
        },
    };
    Ok(LogicalPlan::Scan(ScanNode {
        database: rel.namespace,
        table: synthetic_table,
        alias: rel.alias,
        columns: output_columns,
        predicates: vec![],
        required_columns: None,
        dict_columns: vec![],
        required_output_columns: None,
    }))
}

fn iceberg_table_info(
    source: &crate::sql::catalog::ScanSource,
) -> Option<&crate::sql::catalog::IcebergTableInfo> {
    match source {
        crate::sql::catalog::ScanSource::IcebergDataFiles { table, .. }
        | crate::sql::catalog::ScanSource::IcebergMetadataTable { table, .. }
        | crate::sql::catalog::ScanSource::IcebergDeltaTable { table, .. }
        | crate::sql::catalog::ScanSource::IcebergVersionTable { table, .. } => Some(table),
        crate::sql::catalog::ScanSource::StarRocks { .. } => None,
    }
}

// ---------------------------------------------------------------------------
// Set operation planning
// ---------------------------------------------------------------------------

fn plan_set_operation_scoped(
    set_op: ResolvedSetOp,
    cte_registry: &CTERegistry,
    factory: &mut ColumnRefFactory,
) -> Result<LogicalPlan, String> {
    // Build position-aligned output schema before consuming the branches.
    // For each position we widen the type across left/right (matching
    // the analyzer's wider_type logic), keep the left branch ColumnId and
    // name, and union the nullable flags. This mirrors what derive_output_columns
    // and visit_set_op_common use as the canonical union output schema.
    let output_columns: Vec<OutputColumn> = set_op
        .left
        .output_columns
        .iter()
        .zip(set_op.right.output_columns.iter())
        .map(|(lc, rc)| {
            let dt = crate::sql::types::wider_type(&lc.data_type, &rc.data_type);
            OutputColumn {
                column_id: lc.column_id,
                name: lc.name.clone(),
                data_type: dt,
                nullable: lc.nullable || rc.nullable,
                is_internal: lc.is_internal && rc.is_internal,
            }
        })
        .collect();

    let left = plan_scoped_query(*set_op.left, cte_registry, factory)?;
    let right = plan_scoped_query(*set_op.right, cte_registry, factory)?;

    match set_op.kind {
        SetOpKind::Union => Ok(LogicalPlan::Union(UnionNode {
            inputs: vec![left, right],
            all: set_op.all,
            output_columns,
            required_output_columns: None,
        })),
        SetOpKind::Intersect => Ok(LogicalPlan::Intersect(IntersectNode {
            inputs: vec![left, right],
            output_columns,
            required_output_columns: None,
        })),
        SetOpKind::Except => Ok(LogicalPlan::Except(ExceptNode {
            inputs: vec![left, right],
            output_columns,
            required_output_columns: None,
        })),
    }
}

// ---------------------------------------------------------------------------
// VALUES planning
// ---------------------------------------------------------------------------

fn plan_values(
    values: ResolvedValues,
    factory: &mut ColumnRefFactory,
) -> Result<LogicalPlan, String> {
    let columns = values
        .column_types
        .iter()
        .enumerate()
        .map(|(i, dt)| {
            let name = format!("column_{}", i);
            OutputColumn {
                column_id: factory.create(None, name.clone(), dt.clone(), true),
                name,
                data_type: dt.clone(),
                nullable: true,
                is_internal: false,
            }
        })
        .collect();
    Ok(LogicalPlan::Values(ValuesNode {
        rows: values.rows,
        columns,
        required_output_columns: None,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::catalog::{CatalogProvider, ColumnDef, ScanSource, TableDef};

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
                            logical_type: None,
                        },
                        ColumnDef {
                            name: "o_custkey".to_string(),
                            data_type: arrow::datatypes::DataType::Int64,
                            nullable: false,
                            write_default: None,
                            logical_type: None,
                        },
                    ],
                    iceberg_row_lineage_metadata_columns: vec![],
                    source: ScanSource::StarRocks {
                        db_id: 0,
                        table_id: 0,
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
                        logical_type: None,
                    }],
                    iceberg_row_lineage_metadata_columns: vec![],
                    source: ScanSource::StarRocks {
                        db_id: 0,
                        table_id: 0,
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
        let (resolved, cte_registry, mut factory) =
            crate::sql::analyzer::analyze(&query, &TestCatalog, "default")?;
        plan_query(resolved, cte_registry, &mut factory)
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
        let ExprKind::ColumnRef {
            qualifier, column, ..
        } = &project.items[0].expr.kind
        else {
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

    /// Regression test for the ColumnId-correctness bug where UnionNode.output_columns
    /// carried left-branch ColumnIds instead of the fresh set-op output ColumnIds.
    ///
    /// After the fix, SubqueryAlias.output_columns (which carries the fresh IDs from
    /// the analyzer's parent scope) must equal child Union.output_columns position by
    /// position.  Before the fix these were two disjoint ID spaces.
    #[test]
    fn union_output_columns_carry_fresh_set_op_ids_matching_subquery_alias() {
        // Plan a derived table that wraps a UNION ALL.  The subquery alias node
        // receives the fresh set-op output ColumnIds from the analyzer scope; the
        // Union node must carry the same IDs (not the left-branch scan IDs).
        let plan = parse_analyze_and_plan(
            "SELECT o_orderkey, o_custkey \
             FROM (SELECT o_orderkey, o_custkey FROM orders \
                   UNION ALL \
                   SELECT o_orderkey, o_custkey FROM orders) sub",
        )
        .expect("planner should succeed");

        // Navigate to SubqueryAlias — it is either the root or directly under a Project.
        let alias_node = match &plan {
            LogicalPlan::SubqueryAlias(n) => n,
            LogicalPlan::Project(p) => match p.input.as_ref() {
                LogicalPlan::SubqueryAlias(n) => n,
                other => panic!("expected SubqueryAlias under Project, got {other:?}"),
            },
            other => panic!("expected SubqueryAlias or Project root, got {other:?}"),
        };
        assert_eq!(alias_node.alias, "sub");

        // The direct child of SubqueryAlias must be the Union node.
        let union_node = match alias_node.input.as_ref() {
            LogicalPlan::Union(n) => n,
            other => panic!("expected Union as SubqueryAlias child, got {other:?}"),
        };

        // Core assertion: fresh IDs must match position-by-position.
        assert_eq!(
            alias_node.output_columns.len(),
            union_node.output_columns.len(),
            "SubqueryAlias and Union output_columns length must match"
        );
        for (i, (alias_col, union_col)) in alias_node
            .output_columns
            .iter()
            .zip(union_node.output_columns.iter())
            .enumerate()
        {
            assert_eq!(
                alias_col.column_id, union_col.column_id,
                "output_columns[{i}]: SubqueryAlias column_id {:?} != Union column_id {:?} \
                 (Union must carry the fresh set-op IDs, not left-branch IDs)",
                alias_col.column_id, union_col.column_id
            );
        }
    }

    /// Same correctness guarantee for INTERSECT and EXCEPT set operations.
    #[test]
    fn intersect_except_output_columns_carry_fresh_set_op_ids() {
        for sql in [
            "SELECT o_orderkey FROM (SELECT o_orderkey FROM orders \
             INTERSECT SELECT o_orderkey FROM orders) sub",
            "SELECT o_orderkey FROM (SELECT o_orderkey FROM orders \
             EXCEPT SELECT o_orderkey FROM orders) sub",
        ] {
            let plan = parse_analyze_and_plan(sql).expect("planner should succeed");

            let alias_node = match &plan {
                LogicalPlan::SubqueryAlias(n) => n,
                LogicalPlan::Project(p) => match p.input.as_ref() {
                    LogicalPlan::SubqueryAlias(n) => n,
                    other => panic!("expected SubqueryAlias under Project, got {other:?}"),
                },
                other => panic!("expected SubqueryAlias or Project root, got {other:?}"),
            };

            let (alias_cols, set_op_cols) = match alias_node.input.as_ref() {
                LogicalPlan::Intersect(n) => (&alias_node.output_columns, &n.output_columns),
                LogicalPlan::Except(n) => (&alias_node.output_columns, &n.output_columns),
                other => panic!("expected Intersect/Except as child, got {other:?}"),
            };

            assert_eq!(alias_cols.len(), set_op_cols.len());
            for (i, (ac, sc)) in alias_cols.iter().zip(set_op_cols.iter()).enumerate() {
                assert_eq!(
                    ac.column_id, sc.column_id,
                    "output_columns[{i}]: SubqueryAlias {:?} != set-op {:?} for SQL: {sql}",
                    ac.column_id, sc.column_id
                );
            }
        }
    }

    // -----------------------------------------------------------------------
    // Bug B regression: build_distinct must preserve item.output_column_id
    // -----------------------------------------------------------------------

    /// Bug B: build_distinct previously called expr_column_id() for every item
    /// in the projection, minting a fresh ColumnId for non-ColumnRef exprs (e.g.
    /// the synthetic `Literal(1)` produced by IN/EXISTS subquery rewriting when
    /// the item had a meaningful pre-assigned `output_column_id`). This broke
    /// downstream references that already held the original id.
    ///
    /// Fix: when item.output_column_id != UNSET, use it directly instead of
    /// calling expr_column_id.
    ///
    /// This test verifies that SELECT DISTINCT over a query with pre-assigned
    /// output ids produces an Aggregate whose group-by ColumnRefs carry the same
    /// ids as the inner projection's output_column_ids.
    #[test]
    fn build_distinct_preserves_output_column_id_from_projection() {
        // Use build_distinct indirectly via the planner: SELECT DISTINCT
        // o_orderkey FROM orders.  The inner Project item will have a
        // non-UNSET output_column_id (assigned by the analyzer), and the outer
        // DISTINCT Aggregate's group-by ColumnRef must carry the same id.
        let plan = parse_analyze_and_plan("SELECT DISTINCT o_orderkey FROM orders")
            .expect("planner should succeed");

        // Expected shape: Aggregate(group_by=[ColumnRef(cid)]) <- Project(item.output_column_id=cid)
        let (agg_group_by_cid, inner_proj_output_cid) = match &plan {
            LogicalPlan::Aggregate(agg) => {
                let gb_cid = match &agg.group_by[0].kind {
                    ExprKind::ColumnRef { column_id, .. } => *column_id,
                    other => panic!("expected ColumnRef group_by, got {other:?}"),
                };
                let inner_proj = match agg.input.as_ref() {
                    LogicalPlan::Project(p) => p,
                    other => panic!("expected Project under Aggregate, got {other:?}"),
                };
                let item_cid = inner_proj.items[0].output_column_id;
                (gb_cid, item_cid)
            }
            other => panic!("expected Aggregate root for SELECT DISTINCT, got {other:?}"),
        };

        assert_ne!(
            agg_group_by_cid,
            ColumnId::UNSET,
            "Aggregate group-by ColumnRef must not be UNSET"
        );
        assert_eq!(
            agg_group_by_cid, inner_proj_output_cid,
            "build_distinct must reuse inner Project item's output_column_id, \
             not mint a fresh id (Bug B)"
        );
    }

    // -----------------------------------------------------------------------
    // Bug C regression: apply_query_modifiers strip-project must reuse inner ids
    // -----------------------------------------------------------------------

    /// Bug C: apply_query_modifiers built a strip-projection by calling
    /// factory.create(...) for each item, minting fresh ColumnIds that
    /// disconnected the outer Project from the inner Project's output ids.
    /// The Phase-1 tagging pass then saw a double-Project barrier where the
    /// outer Project's items used ids that didn't match anything the inner
    /// Project produced, causing it to compute child_needed = {} and drop all
    /// inner columns.
    ///
    /// Fix: reuse the inner project item's existing output_column_id for the
    /// strip-project item instead of minting a fresh one.
    ///
    /// This test uses a query with an ORDER BY column that is NOT in the SELECT
    /// output (triggering extra_items and the strip-projection path), then
    /// verifies that the outer Project's items carry the same ColumnIds as the
    /// inner Project's items at the corresponding positions.
    #[test]
    fn apply_query_modifiers_strip_project_reuses_inner_output_column_ids() {
        // ORDER BY o_custkey is not in the SELECT output (only o_orderkey is),
        // so collect_extra_sort_items returns o_custkey as an extra.
        // apply_query_modifiers then builds:
        //   outer strip-Project (items: [o_orderkey]) <-- Sort <-- inner Project (items: [o_orderkey__nr_sel_0, o_custkey_extra])
        let plan = parse_analyze_and_plan("SELECT o_orderkey FROM orders ORDER BY o_custkey")
            .expect("planner should succeed");

        // Walk down to find the outer and inner Projects.
        // Shape: outer-Project? <- Sort <- inner-Project <- Scan
        let outer_proj = match &plan {
            LogicalPlan::Project(p) => p,
            other => {
                // If there is no outer Project (no extra items triggered), skip.
                // The test is only meaningful when the strip-projection was built.
                let shape = format!("{other:?}");
                if shape.contains("Sort") {
                    return; // no extra items path — test not applicable
                }
                panic!("expected Project or Sort root, got {shape}");
            }
        };

        let inner_proj = match outer_proj.input.as_ref() {
            LogicalPlan::Sort(s) => match s.input.as_ref() {
                LogicalPlan::Project(p) => p,
                other => panic!("expected inner Project under Sort, got {other:?}"),
            },
            other => panic!("expected Sort under outer Project, got {other:?}"),
        };

        // The outer strip-project has one user-visible item (o_orderkey).
        // Its output_column_id and expr ColumnRef column_id must match the
        // corresponding inner project item's output_column_id.
        assert!(
            !outer_proj.items.is_empty(),
            "outer strip-project must have at least one item"
        );
        let outer_item = &outer_proj.items[0];
        let outer_expr_cid = match &outer_item.expr.kind {
            ExprKind::ColumnRef { column_id, .. } => *column_id,
            other => panic!("outer strip-project item must be ColumnRef, got {other:?}"),
        };

        // Find the inner project item that was renamed to __nr_sel_0 and
        // corresponds to position 0.
        let inner_item_0 = &inner_proj.items[0];
        let inner_output_cid = inner_item_0.output_column_id;

        assert_ne!(
            outer_expr_cid,
            ColumnId::UNSET,
            "outer strip-project ColumnRef must not be UNSET"
        );
        assert_eq!(
            outer_expr_cid, inner_output_cid,
            "outer strip-project item's ColumnRef column_id must equal inner project item's \
             output_column_id at the same position (Bug C: no fresh id minting)"
        );
        assert_eq!(
            outer_item.output_column_id, inner_output_cid,
            "outer strip-project item's output_column_id must equal inner project item's \
             output_column_id at the same position (Bug C: no fresh id minting)"
        );
    }
}
