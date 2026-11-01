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

use crate::analysis::cte::CTERegistry;
use crate::analysis::expr_display::typed_expr_display_name;
use crate::analysis::*;
use crate::column_id::{ColumnId, ColumnRefFactory};
use crate::compiler::SqlCompileError;
use crate::planner::logical::*;
use crate::planner::payload::*;
use novarocks_type_contract::PureCompileControl;

use super::aggregate::{
    collect_aggregates, ensure_aggregate_output_columns, planner_aggregate_group_by_targets,
    planner_repeat_original_group_by_targets, rewrite_agg_calls_to_refs, rewrite_expr_children,
    rewrite_group_by_expr_refs, typed_expr_semantically_eq,
};
use super::output::{adapt_plan_output, adapt_plan_output_with_qualifier, plan_output_columns};
use super::relation::{plan_set_operation_scoped, plan_values};
use super::select::{plan_select_scoped, plan_select_scoped_with_source};

// ---------------------------------------------------------------------------
// Public entry
// ---------------------------------------------------------------------------

/// Plan a resolved query into a single logical tree, wrapping CTE definitions
/// as nested anchor/produce pairs around the main query subtree.
pub(crate) fn plan_query(
    resolved: ResolvedQuery,
    cte_registry: CTERegistry,
    factory: &mut ColumnRefFactory,
    control: &dyn PureCompileControl,
) -> Result<LogicalPlanNode, SqlCompileError> {
    let work = novarocks_type_contract::CompileCheckpoints::try_new(
        control,
        novarocks_type_contract::CompilePhase::LowerProgram,
    )?;
    let result = plan_scoped_query(resolved, &cte_registry, factory, control);
    if matches!(
        &result,
        Err(SqlCompileError::Cancelled
            | SqlCompileError::DeadlineExceeded
            | SqlCompileError::ResourceExhausted)
    ) {
        return result;
    }
    work.finish()?;
    result
}

pub(super) fn plan_scoped_query(
    resolved: ResolvedQuery,
    cte_registry: &CTERegistry,
    factory: &mut ColumnRefFactory,
    control: &dyn PureCompileControl,
) -> Result<LogicalPlanNode, SqlCompileError> {
    if resolved.local_cte_ids.is_empty()
        && matches!(&resolved.body, QueryBody::Select(select)
            if matches!(select.from, Some(Relation::Subquery { .. })))
    {
        return plan_nested_subquery_chain(resolved, cte_registry, factory, control);
    }
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
    let mut body_plan = plan_body_scoped(body, &output_columns, cte_registry, factory, control)?;

    // Strategy A: if the body produced a set-op node (Union/Intersect/Except),
    // overwrite its output_columns with the fresh ColumnIds that the analyzer
    // allocated for this query's output (stored in `output_columns`).  The
    // planner previously left branch-side ColumnIds in those fields, which
    // disagreed with the fresh IDs that the parent scope uses to reference
    // the set-op output.
    match &mut body_plan.kind {
        LogicalPlanKind::Union(node) => {
            node.output_columns = output_columns.clone();
        }
        LogicalPlanKind::Intersect(node) => {
            node.output_columns = output_columns.clone();
        }
        LogicalPlanKind::Except(node) => {
            node.output_columns = output_columns.clone();
        }
        _ => {}
    }

    let mut root = apply_query_modifiers(
        body_plan,
        order_by,
        output_columns,
        limit,
        offset,
        factory,
        control,
    )?;

    for cte_id in local_cte_ids.into_iter().rev() {
        let entry = cte_registry
            .get(cte_id)
            .ok_or_else(|| format!("missing CTE entry for id {cte_id}"))?;
        let produce_input =
            plan_scoped_query(entry.resolved_query.clone(), cte_registry, factory, control)?;
        let produce_input = adapt_plan_output(produce_input, &entry.output_columns)?;
        let produce = LogicalPlanNode::new(
            LogicalPlanKind::CTEProduce(PlanCTEProduceNode {
                cte_id: entry.id,
                output_columns: entry.output_columns.clone(),
            }),
            vec![produce_input],
            None,
        );
        root = LogicalPlanNode::new(
            LogicalPlanKind::CTEAnchor(PlanCTEAnchorNode { cte_id: entry.id }),
            vec![produce, root],
            None,
        );
    }

    Ok(root)
}

struct NestedSubqueryLayer {
    select: ResolvedSelect,
    alias: String,
    source_output_columns: Vec<OutputColumn>,
    order_by: Vec<SortItem>,
    output_columns: Vec<OutputColumn>,
    limit: Option<i64>,
    offset: Option<i64>,
}

/// Plan a derived-table chain from the innermost query outward. Each layer
/// still uses the ordinary SELECT planner and output adaptation; only the
/// traversal ownership changes.
fn plan_nested_subquery_chain(
    mut current: ResolvedQuery,
    cte_registry: &CTERegistry,
    factory: &mut ColumnRefFactory,
    control: &dyn PureCompileControl,
) -> Result<LogicalPlanNode, SqlCompileError> {
    let mut layers = Vec::new();
    let base = loop {
        let ResolvedQuery {
            body,
            order_by,
            limit,
            offset,
            output_columns,
            local_cte_ids,
        } = current;
        let QueryBody::Select(mut select) = body else {
            break ResolvedQuery {
                body,
                order_by,
                limit,
                offset,
                output_columns,
                local_cte_ids,
            };
        };
        match select.from.take() {
            Some(Relation::Subquery {
                query,
                alias,
                output_columns: source_output_columns,
            }) if local_cte_ids.is_empty() => {
                layers.push(NestedSubqueryLayer {
                    select,
                    alias,
                    source_output_columns,
                    order_by,
                    output_columns,
                    limit,
                    offset,
                });
                current = *query;
            }
            other => {
                select.from = other;
                break ResolvedQuery {
                    body: QueryBody::Select(select),
                    order_by,
                    limit,
                    offset,
                    output_columns,
                    local_cte_ids,
                };
            }
        }
    };
    let mut plan = plan_scoped_query(base, cte_registry, factory, control)?;
    while let Some(layer) = layers.pop() {
        let source = adapt_plan_output_with_qualifier(
            plan,
            &layer.source_output_columns,
            Some(&layer.alias),
        )?;
        let body = plan_select_scoped_with_source(
            layer.select,
            Some(source),
            cte_registry,
            factory,
            control,
        )?;
        plan = apply_query_modifiers(
            body,
            layer.order_by,
            layer.output_columns,
            layer.limit,
            layer.offset,
            factory,
            control,
        )?;
    }
    Ok(plan)
}

fn apply_query_modifiers(
    mut body_plan: LogicalPlanNode,
    order_by: Vec<SortItem>,
    output_columns: Vec<OutputColumn>,
    limit: Option<i64>,
    offset: Option<i64>,
    factory: &mut ColumnRefFactory,
    control: &dyn PureCompileControl,
) -> Result<LogicalPlanNode, SqlCompileError> {
    let mut final_projection: Option<Vec<ProjectItem>> = None;

    // Wrap with Sort if ORDER BY is present.
    if !order_by.is_empty() {
        let body_output_columns =
            plan_output_columns(&body_plan).unwrap_or_else(|_| output_columns.clone());
        let source_projection: &[ProjectItem] = match &body_plan.kind {
            LogicalPlanKind::Project(project) => &project.items,
            _ => &[],
        };
        let mut extra_items = collect_extra_sort_items(
            &order_by,
            &body_output_columns,
            source_projection,
            factory,
            control,
        )?;
        let sort_items = rewrite_sort_items_to_projection_refs(
            &order_by,
            &extra_items,
            &body_output_columns,
            source_projection,
            control,
        )?;
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
            // Each tuple retains the user-visible name, complete value type and inner ColumnId.
            // The inner output_column_id is captured here so the outer strip-project can
            // reference the same ColumnId that the inner Project produces, preserving id
            // continuity through the double-Project barrier for the Phase-1 tagging pass.
            let user_select: Option<
                Vec<(String, novarocks_type_contract::FunctionValueType, ColumnId)>,
            > = if let LogicalPlanNode {
                kind: LogicalPlanKind::Project(proj),
                children,
                ..
            } = &mut body_plan
            {
                let select_items_for_extra = proj.items.clone();
                for extra in &mut extra_items {
                    extra.expr = rewrite_project_output_refs_to_item_expr(
                        &extra.expr,
                        &select_items_for_extra,
                    );
                }

                if let Some(child) = children.get_mut(0)
                    && matches!(child.kind, LogicalPlanKind::Aggregate(_))
                {
                    if let LogicalPlanKind::Aggregate(agg) = &mut child.kind {
                        for extra in &extra_items {
                            collect_aggregates(&extra.expr, &mut agg.aggregates, factory, control)?;
                        }
                        ensure_aggregate_output_columns(agg, control)?;
                    }
                    // ORDER BY-only aggregates (e.g. `count(v2)` that does
                    // not appear in SELECT) were just folded into the
                    // aggregate node above. Their extra Project items still
                    // carry raw AggregateCall expressions; rewrite them to
                    // reference the aggregate's output columns, exactly as
                    // split_projection_for_aggregate does for SELECT/HAVING.
                    // Without this the post-aggregate Project keeps a
                    // ColumnRef to the aggregate's *input* column (the
                    // aggregate argument), which the id-binding verifier
                    // rejects as "not produced by child scope".
                    // ORDER BY-only group-by *expressions* (e.g. `substr(col, ...)`
                    // that appears in GROUP BY/SELECT but whose ORDER BY display
                    // name didn't match the SELECT output name — most commonly the
                    // `substr`/`substring` alias, where the SELECT output name keeps
                    // the SQL-text spelling but the analyzed expr canonicalizes the
                    // function name) keep a raw expression over the aggregate's
                    // *input* columns. Rewrite them to reference the planner
                    // aggregate's group-key layout, exactly as
                    // split_projection_for_aggregate does for SELECT/HAVING.
                    // Without this the post-aggregate Project re-derives the group
                    // key from a pre-aggregate column that the id-binding verifier
                    // rejects as "not produced by child scope".
                    let repeat_gb_targets =
                        planner_repeat_original_group_by_targets(child, control)?;
                    if let LogicalPlanKind::Aggregate(agg) = &mut child.kind {
                        let mut gb_targets = planner_aggregate_group_by_targets(agg, control)?;
                        gb_targets.extend(repeat_gb_targets);
                        for extra in &mut extra_items {
                            extra.expr =
                                rewrite_agg_calls_to_refs(&extra.expr, &agg.aggregates, control)?;
                            extra.expr =
                                rewrite_group_by_expr_refs(&extra.expr, &gb_targets, control)?;
                        }
                    }
                }
                let user: Vec<(String, novarocks_type_contract::FunctionValueType, ColumnId)> =
                    proj.items
                        .iter()
                        .map(|it| {
                            (
                                it.output_name.clone(),
                                it.expr.value_type.clone(),
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
                let name_to_output: std::collections::HashMap<String, (usize, ColumnId)> = user
                    .iter()
                    .enumerate()
                    .map(|(idx, (name, _, output_id))| (name.to_lowercase(), (idx, *output_id)))
                    .collect();
                let id_to_output: std::collections::HashMap<ColumnId, (usize, ColumnId)> = user
                    .iter()
                    .enumerate()
                    .filter_map(|(idx, (_, _, output_id))| {
                        (*output_id != ColumnId::UNSET).then_some((*output_id, (idx, *output_id)))
                    })
                    .collect();
                sort_items
                    .into_iter()
                    .map(|item| remap_sort_to_synthetic(item, &id_to_output, &name_to_output))
                    .collect()
            } else {
                sort_items
            };

            // Sort with extended scope
            body_plan = LogicalPlanNode::new(
                LogicalPlanKind::Sort(PlanSortNode {
                    items: sort_items,
                    // Top-level ORDER BY — no analytic partition.
                    analytic_partition_by: Vec::new(),
                    output_columns: vec![],
                    offset: None,
                    partition_limit: None,
                    topn_type: None,
                }),
                vec![body_plan],
                None,
            );

            // Strip synthetic sort-only columns after LIMIT/OFFSET so the
            // limit stays directly above Sort and can be rewritten to TopN.
            final_projection = Some(if let Some(user) = user_select {
                user.into_iter()
                    .enumerate()
                    .map(|(idx, (name, value_type, inner_cid))| {
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
                                value_type,
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
                            value_type: col.value_type.clone(),
                        },
                        output_name: col.name.clone(),
                        output_column_id: col.column_id,
                    })
                    .collect()
            });
        } else {
            body_plan = LogicalPlanNode::new(
                LogicalPlanKind::Sort(PlanSortNode {
                    items: sort_items,
                    // Top-level ORDER BY — no analytic partition.
                    analytic_partition_by: Vec::new(),
                    output_columns: vec![],
                    offset: None,
                    partition_limit: None,
                    topn_type: None,
                }),
                vec![body_plan],
                None,
            );
        }
    }

    // Wrap with Limit if LIMIT/OFFSET is present.
    if limit.is_some() || offset.is_some() {
        body_plan = LogicalPlanNode::new(
            LogicalPlanKind::Limit(PlanLimitNode { limit, offset }),
            vec![body_plan],
            None,
        );
    }

    if let Some(items) = final_projection {
        body_plan = LogicalPlanNode::new(
            LogicalPlanKind::Project(PlanProjectNode {
                items,
                output_qualifier: None,
            }),
            vec![body_plan],
            None,
        );
    }

    Ok(body_plan)
}

fn collect_extra_sort_items(
    order_by: &[SortItem],
    output: &[OutputColumn],
    source_projection: &[ProjectItem],
    factory: &mut ColumnRefFactory,
    control: &dyn PureCompileControl,
) -> Result<Vec<ProjectItem>, SqlCompileError> {
    let output_ids: std::collections::HashSet<ColumnId> = output
        .iter()
        .filter_map(|column| (column.column_id != ColumnId::UNSET).then_some(column.column_id))
        .collect();
    let mut extra: Vec<ProjectItem> = Vec::new();
    for item in order_by {
        if let ExprKind::ColumnRef {
            column_id,
            qualifier,
            column,
        } = &item.expr.kind
            && (output_ids.contains(column_id)
                || (*column_id == ColumnId::UNSET
                    && qualifier.is_none()
                    && output
                        .iter()
                        .any(|output| output.name.eq_ignore_ascii_case(column))))
        {
            continue;
        }
        let mut matched = false;
        for projected in source_projection.iter().chain(&extra) {
            if typed_expr_semantically_eq(&item.expr, &projected.expr, control)? {
                matched = true;
                break;
            }
        }
        if matched {
            continue;
        }
        let output_name = typed_expr_display_name(&item.expr, control)?;
        let output_column_id = if let ExprKind::ColumnRef { column_id, .. } = &item.expr.kind {
            *column_id
        } else {
            factory.create(None, output_name.clone(), item.expr.value_type.clone())
        };
        extra.push(ProjectItem {
            expr: item.expr.clone(),
            output_name,
            output_column_id,
        });
    }
    Ok(extra)
}

/// Rewrite a sort item so any unqualified `ColumnRef` pointing at a
/// pre-rename SELECT output name is remapped to the matching
/// `__nr_sel_<idx>`. Used after the inner Project items have been renamed
/// for the sort-extras flow so that simple `ORDER BY <select_alias>`
/// references still resolve.
fn remap_sort_to_synthetic(
    item: SortItem,
    id_to_output: &std::collections::HashMap<ColumnId, (usize, ColumnId)>,
    name_to_output: &std::collections::HashMap<String, (usize, ColumnId)>,
) -> SortItem {
    let SortItem {
        expr,
        asc,
        nulls_first,
    } = item;
    SortItem {
        expr: remap_select_alias_refs(expr, id_to_output, name_to_output),
        asc,
        nulls_first,
    }
}

fn remap_select_alias_refs(
    expr: TypedExpr,
    id_to_output: &std::collections::HashMap<ColumnId, (usize, ColumnId)>,
    name_to_output: &std::collections::HashMap<String, (usize, ColumnId)>,
) -> TypedExpr {
    match expr.kind {
        ExprKind::ColumnRef {
            column_id,
            qualifier: None,
            ref column,
        } => {
            let target = if column_id != ColumnId::UNSET {
                id_to_output.get(&column_id)
            } else {
                None
            }
            .or_else(|| name_to_output.get(&column.to_lowercase()));
            if let Some((idx, output_id)) = target {
                TypedExpr {
                    value_type: expr.value_type,

                    kind: ExprKind::ColumnRef {
                        column_id: *output_id,
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

fn rewrite_project_output_refs_to_item_expr(
    expr: &TypedExpr,
    project_items: &[ProjectItem],
) -> TypedExpr {
    if let ExprKind::ColumnRef {
        column_id,
        qualifier: None,
        column,
    } = &expr.kind
    {
        if *column_id != ColumnId::UNSET
            && let Some(item) = project_items
                .iter()
                .find(|item| item.output_column_id == *column_id)
        {
            return item.expr.clone();
        }
        if let Some(item) = project_items
            .iter()
            .find(|item| item.output_name.eq_ignore_ascii_case(column))
        {
            return item.expr.clone();
        }
    }

    rewrite_expr_children(expr, |child| {
        rewrite_project_output_refs_to_item_expr(child, project_items)
    })
}

fn rewrite_sort_items_to_projection_refs(
    order_by: &[SortItem],
    extra_items: &[ProjectItem],
    output: &[OutputColumn],
    source_projection: &[ProjectItem],
    control: &dyn PureCompileControl,
) -> Result<Vec<SortItem>, SqlCompileError> {
    let mut rewritten = Vec::with_capacity(order_by.len());
    for item in order_by {
        if let ExprKind::ColumnRef { column_id, .. } = &item.expr.kind
            && *column_id != ColumnId::UNSET
            && output.iter().any(|output| output.column_id == *column_id)
        {
            rewritten.push(item.clone());
            continue;
        }
        let mut target = None;
        // Search the actual expressions in source order; labels cannot prove value identity.
        for projected in extra_items.iter().chain(source_projection) {
            if typed_expr_semantically_eq(&item.expr, &projected.expr, control)? {
                target = Some((projected.output_column_id, projected.output_name.clone()));
                break;
            }
        }
        if target.is_none()
            && let ExprKind::ColumnRef {
                column_id: ColumnId::UNSET,
                qualifier: None,
                column,
            } = &item.expr.kind
            && let Some(output) = output.iter().find(|output| {
                output.column_id != ColumnId::UNSET && output.name.eq_ignore_ascii_case(column)
            })
        {
            target = Some((output.column_id, output.name.clone()));
        }
        rewritten.push(if let Some((column_id, column)) = target {
            SortItem {
                expr: TypedExpr {
                    kind: ExprKind::ColumnRef {
                        column_id,
                        qualifier: None,
                        column,
                    },
                    value_type: item.expr.value_type.clone(),
                },
                asc: item.asc,
                nulls_first: item.nulls_first,
            }
        } else {
            item.clone()
        });
    }
    Ok(rewritten)
}

// ---------------------------------------------------------------------------
// Body planning
// ---------------------------------------------------------------------------

fn plan_body_scoped(
    body: QueryBody,
    output_columns: &[OutputColumn],
    cte_registry: &CTERegistry,
    factory: &mut ColumnRefFactory,
    control: &dyn PureCompileControl,
) -> Result<LogicalPlanNode, SqlCompileError> {
    match body {
        QueryBody::Select(select) => plan_select_scoped(select, cte_registry, factory, control),
        QueryBody::SetOperation(set_op) => {
            plan_set_operation_scoped(set_op, output_columns, cte_registry, factory, control)
        }
        QueryBody::Values(values) => {
            plan_values(values, factory).map_err(SqlCompileError::Compilation)
        }
    }
}

#[cfg(test)]
mod constant_identity_tests {
    use super::*;
    use arrow::array::{Array, ArrayRef, Float64Array, Int64Array};
    use arrow::datatypes::Field;
    use novarocks_type_contract::{CompileControlError, CompilePhase, FunctionValueType};
    use std::sync::{Arc, Mutex};

    fn constant(array: ArrayRef, ordinal: u32, metadata: &[(&str, &str)]) -> TypedExpr {
        let value_type = FunctionValueType::new(array.data_type().clone(), false);
        let field = Arc::new(
            Field::new("constant", array.data_type().clone(), false).with_metadata(
                metadata
                    .iter()
                    .map(|(key, value)| (key.to_string(), value.to_string()))
                    .collect::<std::collections::HashMap<String, String>>(),
            ),
        );
        let pool = novarocks_functions::ConstantPool::try_new(
            field,
            value_type.clone(),
            array.to_data(),
            crate::constant::test_constant_policy(),
            CompilePhase::LowerProgram,
            &crate::compiler::SqlCompileControl::unbounded(),
        )
        .unwrap();
        TypedExpr {
            kind: ExprKind::Constant(pool.value(ordinal).unwrap()),
            value_type,
        }
    }
    fn projected(expr: TypedExpr, id: u32) -> ProjectItem {
        ProjectItem {
            expr,
            output_name: "colliding-label".into(),
            output_column_id: ColumnId::new_for_test(id),
        }
    }
    fn output(item: &ProjectItem) -> OutputColumn {
        OutputColumn {
            name: item.output_name.clone(),
            column_id: item.output_column_id,
            value_type: item.expr.value_type.clone(),
            is_internal: false,
        }
    }
    fn sort(expr: TypedExpr, asc: bool, nulls_first: bool) -> SortItem {
        SortItem {
            expr,
            asc,
            nulls_first,
        }
    }
    fn column_id(expr: &TypedExpr) -> ColumnId {
        let ExprKind::ColumnRef { column_id, .. } = &expr.kind else {
            panic!("sort expression must reference a proven output")
        };
        *column_id
    }

    #[test]
    fn sort_projection_matches_selected_values_without_display_key_identity() {
        let control = crate::compiler::SqlCompileControl::unbounded();
        let source = projected(
            constant(Arc::new(Int64Array::from(vec![-99, 42])), 1, &[]),
            901,
        );
        let orders = [
            sort(
                constant(Arc::new(Int64Array::from(vec![42, 7])), 0, &[]),
                false,
                true,
            ),
            sort(
                constant(Arc::new(Int64Array::from(vec![43])), 0, &[]),
                true,
                false,
            ),
            sort(
                constant(Arc::new(Int64Array::from(vec![99, 43])), 1, &[]),
                false,
                false,
            ),
        ];
        let outputs = [output(&source)];
        let mut factory = ColumnRefFactory::new();
        let extra = collect_extra_sort_items(
            &orders,
            &outputs,
            std::slice::from_ref(&source),
            &mut factory,
            &control,
        )
        .unwrap();
        assert_eq!(
            extra.len(),
            1,
            "only the different selected value needs a new output"
        );
        let rewritten = rewrite_sort_items_to_projection_refs(
            &orders,
            &extra,
            &outputs,
            std::slice::from_ref(&source),
            &control,
        )
        .unwrap();
        assert_eq!(column_id(&rewritten[0].expr), source.output_column_id);
        assert_eq!(column_id(&rewritten[1].expr), extra[0].output_column_id);
        assert_eq!(column_id(&rewritten[2].expr), extra[0].output_column_id);
        assert_eq!(
            rewritten
                .iter()
                .map(|item| (item.asc, item.nulls_first))
                .collect::<Vec<_>>(),
            vec![(false, true), (true, false), (false, false)]
        );
    }

    struct Control {
        trace: Mutex<Vec<(CompilePhase, u32)>>,
        refuse: Option<usize>,
        cause: CompileControlError,
    }
    impl PureCompileControl for Control {
        fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
            let mut trace = self.trace.lock().unwrap();
            trace.push((phase, units));
            if self.refuse == Some(trace.len() - 1) {
                Err(self.cause.clone())
            } else {
                Ok(())
            }
        }
    }

    #[test]
    fn sort_projection_keeps_nan_bits_metadata_and_original_control() {
        let bits = 0x7ff0_0000_0000_0001;
        let source = projected(
            constant(
                Arc::new(Float64Array::from(vec![0.0, f64::from_bits(bits)])),
                1,
                &[("provider", "left")],
            ),
            903,
        );
        let orders = [
            sort(
                constant(
                    Arc::new(Float64Array::from(vec![f64::from_bits(bits)])),
                    0,
                    &[("provider", "left")],
                ),
                true,
                true,
            ),
            sort(
                constant(
                    Arc::new(Float64Array::from(vec![f64::from_bits(bits + 1)])),
                    0,
                    &[("provider", "left")],
                ),
                false,
                true,
            ),
            sort(
                constant(
                    Arc::new(Float64Array::from(vec![f64::from_bits(bits)])),
                    0,
                    &[("provider", "right")],
                ),
                true,
                false,
            ),
        ];
        let outputs = [output(&source)];
        let run = |control: &dyn PureCompileControl| {
            let mut factory = ColumnRefFactory::new();
            let extra = collect_extra_sort_items(
                &orders,
                &outputs,
                std::slice::from_ref(&source),
                &mut factory,
                control,
            )?;
            let rewritten = rewrite_sort_items_to_projection_refs(
                &orders,
                &extra,
                &outputs,
                std::slice::from_ref(&source),
                control,
            )?;
            Ok::<_, SqlCompileError>((extra, rewritten))
        };
        let recording = Control {
            trace: Default::default(),
            refuse: None,
            cause: CompileControlError::Cancelled,
        };
        let (extra, rewritten) = run(&recording).unwrap();
        assert_eq!(extra.len(), 1);
        assert_eq!(column_id(&rewritten[0].expr), source.output_column_id);
        assert_eq!(column_id(&rewritten[1].expr), extra[0].output_column_id);
        assert_eq!(column_id(&rewritten[2].expr), source.output_column_id);
        assert_eq!(
            rewritten
                .iter()
                .map(|item| (item.asc, item.nulls_first))
                .collect::<Vec<_>>(),
            orders
                .iter()
                .map(|item| (item.asc, item.nulls_first))
                .collect::<Vec<_>>()
        );
        let trace = recording.trace.into_inner().unwrap();
        assert!(!trace.is_empty());
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            for stop in 0..trace.len() {
                let control = Control {
                    trace: Default::default(),
                    refuse: Some(stop),
                    cause: cause.clone(),
                };
                let error = run(&control).unwrap_err();
                assert!(matches!(
                    (cause.clone(), error),
                    (CompileControlError::Cancelled, SqlCompileError::Cancelled)
                        | (
                            CompileControlError::DeadlineExceeded,
                            SqlCompileError::DeadlineExceeded
                        )
                        | (
                            CompileControlError::ResourceExhausted,
                            SqlCompileError::ResourceExhausted
                        )
                ));
                assert_eq!(control.trace.into_inner().unwrap(), trace[..=stop]);
            }
        }
    }
}
