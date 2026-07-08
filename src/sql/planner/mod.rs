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

//! Planner — converts analyzed SQL into logical plans and distributed Bridge 2 IR.
//!
//! This is a structural transformation that builds a relational algebra tree
//! from the analyzed query IR. It also owns Bridge 2, which materializes
//! physical optimizer plans into planner-side distributed plan fragments before
//! codegen lowers them to Thrift.

pub(crate) mod change_stream_write;
mod distributed_fragment;
mod distributed_node;
mod distributed_plan_build;
pub(crate) mod imv_rewrite;
pub(crate) mod optimizer_bridge;
mod ordering;
mod physical_vocab;
pub(crate) mod plan;
pub(crate) mod runtime_filter;
pub(crate) mod runtime_filter_placement;
pub(crate) mod stats;
pub(crate) mod write_plan;
pub(crate) mod write_sink;

pub(crate) use change_stream_write::{
    ChangeStreamWriteBranchSpec, ChangeStreamWriteDagSpec, IcebergChangeStreamBranchRoute,
    IcebergChangeStreamRouterSink, IcebergChangeStreamWriteTopology,
    IcebergChangeStreamWriterBranch, PlannedIcebergChangeStreamDistributedPlan,
};
pub(crate) use distributed_fragment::{
    DataPartition, DataSink, DistributedPlan, PartitionKind, PlanFragment,
};
pub(crate) use distributed_node::{DistributedNode, DistributedPayload, ExchangeReceiver};
pub(crate) use distributed_plan_build::{
    build_distributed_plan, union_distinct_must_be_rewritten_error,
};
#[allow(unused_imports)]
pub(crate) use ordering::{OrderingSpec, SortKey};
#[allow(unused_imports)]
pub(crate) use physical_vocab::{
    AggMode, AggregateOutputLayout, HashSource, JoinDistribution, TopNPhase,
};
pub(crate) use runtime_filter::{
    JoinExecutionMode, PlannedRuntimeFilter, RuntimeFilterBuildIntent, RuntimeFilterProbeIntent,
};
#[allow(unused_imports)]
pub(crate) use runtime_filter::{WiredRuntimeFilterBuild, WiredRuntimeFilterProbe};
pub(crate) use stats::{
    PhysicalPlanStats, PlannerBroadcastDecision, PlannerColumnStatistic, PlannerConfidence,
    PlannerCostEstimate,
};
pub(crate) use write_plan::{with_iceberg_change_stream_write, with_iceberg_write_sink};
pub(crate) use write_sink::{
    IcebergWriteFragmentSink, IcebergWriteInputBinding, IcebergWriteSinkMode, IcebergWriteSinkSpec,
    synthetic_iceberg_write_table_id,
};

use arrow::datatypes::DataType;

use crate::sql::analysis::cte::CTERegistry;
use crate::sql::analysis::*;
use crate::sql::catalog::{IcebergDataFileInfo, IcebergDeleteFileContent};
use crate::sql::codegen::helpers::typed_expr_display_name;
use crate::sql::column_id::{ColumnId, ColumnRefFactory};
use crate::sql::planner::optimizer_bridge::property::ordering_spec_from_sort_items;
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

#[cfg(test)]
mod bridge2_export_tests {
    use super::{DistributedNode, DistributedPlan, build_distributed_plan};
    use crate::sql::planner::plan::PhysicalPlanNode;

    #[test]
    fn planner_exports_bridge2_distributed_plan_api() {
        fn accepts_builder(_: fn(&PhysicalPlanNode) -> Result<DistributedPlan, String>) {}
        fn accepts_node(_: Option<DistributedNode>) {}

        accepts_builder(build_distributed_plan);
        accepts_node(None);
    }
}

#[cfg(test)]
mod write_export_tests {
    use super::{
        ChangeStreamWriteBranchSpec, ChangeStreamWriteDagSpec, IcebergWriteSinkMode,
        IcebergWriteSinkSpec,
    };

    #[test]
    fn planner_exports_write_sink_dtos() {
        fn accepts_sink_spec(_: Option<IcebergWriteSinkSpec>) {}
        fn accepts_dag(_: Option<ChangeStreamWriteDagSpec>) {}

        accepts_sink_spec(None);
        accepts_dag(None);
        assert_eq!(IcebergWriteSinkMode::Data, IcebergWriteSinkMode::Data);
    }

    #[test]
    fn change_stream_branch_spec_stores_ordinals_not_slots() {
        let branch = ChangeStreamWriteBranchSpec::for_test(
            7,
            crate::sql::common::ChangeStreamBranchKind::ReuseData,
            vec![0, 2],
        );

        assert_eq!(branch.branch_id, 7);
        assert_eq!(branch.stream_output_ordinals, vec![0, 2]);
        assert!(branch.output_partition_ordinals.is_empty());
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
) -> Result<LogicalPlanNode, String> {
    plan_scoped_query(resolved, &cte_registry, factory)
}

fn plan_scoped_query(
    resolved: ResolvedQuery,
    cte_registry: &CTERegistry,
    factory: &mut ColumnRefFactory,
) -> Result<LogicalPlanNode, String> {
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

    let mut root =
        apply_query_modifiers(body_plan, order_by, output_columns, limit, offset, factory);

    for cte_id in local_cte_ids.into_iter().rev() {
        let entry = cte_registry
            .get(cte_id)
            .ok_or_else(|| format!("missing CTE entry for id {cte_id}"))?;
        let produce_input = plan_scoped_query(entry.resolved_query.clone(), cte_registry, factory)?;
        let produce_input = adapt_plan_output(produce_input, &entry.output_columns)?;
        let produce = LogicalPlanNode::new(
            LogicalPlanKind::CTEProduce(LogicalCTEProduceNode {
                cte_id: entry.id,
                output_columns: entry.output_columns.clone(),
            }),
            vec![produce_input],
            None,
        );
        root = LogicalPlanNode::new(
            LogicalPlanKind::CTEAnchor(LogicalCTEAnchorNode { cte_id: entry.id }),
            vec![produce, root],
            None,
        );
    }

    Ok(root)
}

fn apply_query_modifiers(
    mut body_plan: LogicalPlanNode,
    order_by: Vec<SortItem>,
    output_columns: Vec<OutputColumn>,
    limit: Option<i64>,
    offset: Option<i64>,
    factory: &mut ColumnRefFactory,
) -> LogicalPlanNode {
    let mut final_projection: Option<Vec<ProjectItem>> = None;

    // Wrap with Sort if ORDER BY is present.
    if !order_by.is_empty() {
        let body_output_columns =
            plan_output_columns(&body_plan).unwrap_or_else(|_| output_columns.clone());
        let mut extra_items = collect_extra_sort_items(&order_by, &body_output_columns, factory);
        let sort_items =
            rewrite_sort_items_to_projection_refs(&order_by, &extra_items, &body_output_columns);
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
                if let LogicalPlanNode {
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
                                collect_aggregates(&extra.expr, &mut agg.aggregates, factory);
                            }
                            ensure_aggregate_output_columns(agg);
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
                        let repeat_gb_targets = planner_repeat_original_group_by_targets(child);
                        if let LogicalPlanKind::Aggregate(agg) = &mut child.kind {
                            let mut gb_targets = planner_aggregate_group_by_targets(agg);
                            gb_targets.extend(repeat_gb_targets);
                            for extra in &mut extra_items {
                                extra.expr =
                                    rewrite_agg_calls_to_refs(&extra.expr, &agg.aggregates);
                                extra.expr = rewrite_group_by_expr_refs(&extra.expr, &gb_targets);
                            }
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
                let name_to_output: std::collections::HashMap<String, (usize, ColumnId)> = user
                    .iter()
                    .enumerate()
                    .map(|(idx, (name, _, _, output_id))| (name.to_lowercase(), (idx, *output_id)))
                    .collect();
                let id_to_output: std::collections::HashMap<ColumnId, (usize, ColumnId)> = user
                    .iter()
                    .enumerate()
                    .filter_map(|(idx, (_, _, _, output_id))| {
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
                LogicalPlanKind::Sort(LogicalSortNode {
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
            body_plan = LogicalPlanNode::new(
                LogicalPlanKind::Sort(LogicalSortNode {
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
            LogicalPlanKind::Limit(LogicalLimitNode {
                limit: limit,
                offset: offset,
            }),
            vec![body_plan],
            None,
        );
    }

    if let Some(items) = final_projection {
        body_plan = LogicalPlanNode::new(
            LogicalPlanKind::Project(LogicalProjectNode {
                items: items,
                output_qualifier: None,
            }),
            vec![body_plan],
            None,
        );
    }

    body_plan
}

fn collect_extra_sort_items(
    order_by: &[SortItem],
    output: &[OutputColumn],
    factory: &mut ColumnRefFactory,
) -> Vec<ProjectItem> {
    let output_names: std::collections::HashSet<String> =
        output.iter().map(|c| c.name.to_lowercase()).collect();
    let output_ids: std::collections::HashSet<ColumnId> = output
        .iter()
        .filter_map(|c| (c.column_id != ColumnId::UNSET).then_some(c.column_id))
        .collect();
    let mut added = std::collections::HashSet::new();
    let mut extra = Vec::new();
    for item in order_by {
        if let ExprKind::ColumnRef { column_id, .. } = &item.expr.kind
            && output_ids.contains(column_id)
        {
            continue;
        }
        let output_name = crate::sql::codegen::helpers::typed_expr_display_name(&item.expr);
        let output_name_lower = output_name.to_lowercase();
        if !output_names.contains(&output_name_lower) && added.insert(output_name_lower) {
            let output_column_id = if let ExprKind::ColumnRef { column_id, .. } = &item.expr.kind {
                *column_id
            } else {
                factory.create(
                    None,
                    output_name.clone(),
                    item.expr.data_type.clone(),
                    item.expr.nullable,
                )
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
                    data_type: expr.data_type,
                    nullable: expr.nullable,
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
    // SELECT output columns keyed by display name. A non-ColumnRef ORDER BY item
    // (e.g. `sum(x)`) whose display name matches a SELECT output already computed
    // by the aggregate/projection must reference that output column rather than
    // repeat the expression — repeating it keeps a raw AggregateCall whose
    // argument column lives below the aggregate and is not in the sort's input
    // scope ("not produced by child scope").
    let output_by_name: std::collections::HashMap<String, &OutputColumn> = output
        .iter()
        .filter(|c| c.column_id != ColumnId::UNSET)
        .map(|c| (c.name.to_lowercase(), c))
        .collect();
    let output_by_id: std::collections::HashMap<ColumnId, &OutputColumn> = output
        .iter()
        .filter(|c| c.column_id != ColumnId::UNSET)
        .map(|c| (c.column_id, c))
        .collect();

    order_by
        .iter()
        .map(|item| {
            let display =
                crate::sql::codegen::helpers::typed_expr_display_name(&item.expr).to_lowercase();
            if let ExprKind::ColumnRef { column_id, .. } = &item.expr.kind
                && *column_id != ColumnId::UNSET
                && output_by_id.contains_key(column_id)
            {
                // Positional ORDER BY is already resolved to the exact SELECT
                // output ColumnId. Keep that id instead of re-resolving by
                // display name; duplicate output names such as `s.a, t.a`
                // would otherwise collapse both sort keys onto the same slot.
                item.clone()
            } else if let Some(extra) = extra_names.get(&display) {
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
            } else if let ExprKind::ColumnRef {
                qualifier: None,
                column,
                ..
            } = &item.expr.kind
                && let Some(col) = output_by_name.get(&column.to_lowercase())
            {
                SortItem {
                    expr: TypedExpr {
                        kind: ExprKind::ColumnRef {
                            column_id: col.column_id,
                            qualifier: None,
                            column: col.name.clone(),
                        },
                        data_type: item.expr.data_type.clone(),
                        nullable: item.expr.nullable,
                    },
                    asc: item.asc,
                    nulls_first: item.nulls_first,
                }
            } else if !matches!(item.expr.kind, ExprKind::ColumnRef { .. })
                && let Some(col) = output_by_name.get(&display)
            {
                // Non-ColumnRef ORDER BY item (e.g. an aggregate) that names a
                // SELECT output: reference that already-computed output column.
                SortItem {
                    expr: TypedExpr {
                        kind: ExprKind::ColumnRef {
                            column_id: col.column_id,
                            qualifier: None,
                            column: col.name.clone(),
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
) -> Result<LogicalPlanNode, String> {
    match body {
        QueryBody::Select(select) => plan_select_scoped(select, cte_registry, factory),
        QueryBody::SetOperation(set_op) => plan_set_operation_scoped(set_op, cte_registry, factory),
        QueryBody::Values(values) => plan_values(values, factory),
    }
}

pub(crate) fn plan_output_columns(plan: &LogicalPlanNode) -> Result<Vec<OutputColumn>, String> {
    match &plan.kind {
        LogicalPlanKind::Scan(node) => Ok(node.columns.clone()),
        LogicalPlanKind::Filter(_) => plan_output_columns(plan.unary_input()),
        LogicalPlanKind::Project(node) => {
            let input_columns = plan_output_columns(plan.unary_input())?;
            Ok(node
                .items
                .iter()
                .map(|item| OutputColumn {
                    column_id: item.output_column_id,
                    name: item.output_name.clone(),
                    data_type: item.expr.data_type.clone(),
                    nullable: item.expr.nullable,
                    is_internal: project_item_refs_internal_column(item, &input_columns),
                })
                .collect())
        }
        LogicalPlanKind::Aggregate(node) => Ok(node.output_columns.clone()),
        LogicalPlanKind::Join(node) => {
            let left = plan_output_columns(plan.left())?;
            let right = plan_output_columns(plan.right())?;
            Ok(join_output_columns(node.join_type, left, right))
        }
        LogicalPlanKind::Sort(_) => plan_output_columns(plan.unary_input()),
        LogicalPlanKind::Limit(_) => plan_output_columns(plan.unary_input()),
        LogicalPlanKind::Union(node) => Ok(node.output_columns.clone()),
        LogicalPlanKind::Intersect(node) => Ok(node.output_columns.clone()),
        LogicalPlanKind::Except(node) => Ok(node.output_columns.clone()),
        LogicalPlanKind::Values(node) => Ok(node.columns.clone()),
        LogicalPlanKind::GenerateSeries(node) => Ok(vec![OutputColumn {
            column_id: node.output_column_id,
            name: node.column_name.clone(),
            data_type: arrow::datatypes::DataType::Int64,
            nullable: false,
            is_internal: false,
        }]),
        LogicalPlanKind::TableFunction(node) => {
            let mut columns = plan_output_columns(plan.unary_input())?;
            columns.extend(node.output_columns.clone());
            Ok(columns)
        }
        LogicalPlanKind::Window(node) => Ok(node.output_columns.clone()),
        LogicalPlanKind::Repeat(node) => {
            let mut columns = plan_output_columns(plan.unary_input())?;
            columns.extend(
                node.grouping_fn_ids
                    .iter()
                    .map(|(name, column_id)| OutputColumn {
                        column_id: *column_id,
                        name: name.clone(),
                        data_type: arrow::datatypes::DataType::Int64,
                        nullable: false,
                        is_internal: true,
                    }),
            );
            Ok(columns)
        }
        LogicalPlanKind::CTEAnchor(_) => plan_output_columns(plan.child(1)),
        LogicalPlanKind::CTEProduce(node) => Ok(node.output_columns.clone()),
        LogicalPlanKind::CTEConsume(node) => Ok(node.output_columns.clone()),
        LogicalPlanKind::Apply(node) => {
            let mut columns = plan_output_columns(plan.left())?;
            columns.push(node.output_column.clone());
            Ok(columns)
        }
        LogicalPlanKind::AssertOneRow(_) => plan_output_columns(plan.unary_input()),
        LogicalPlanKind::ImvDelta(_) | LogicalPlanKind::ImvVersion(_) => {
            Err("imv marker leaked into non-IMV planner output adaptation".to_string())
        }
    }
}

fn project_item_refs_internal_column(item: &ProjectItem, input_columns: &[OutputColumn]) -> bool {
    expr_refs_internal_column(&item.expr, input_columns)
}

fn expr_refs_internal_column(expr: &TypedExpr, input_columns: &[OutputColumn]) -> bool {
    match &expr.kind {
        ExprKind::ColumnRef {
            column_id, column, ..
        } => input_columns.iter().any(|input| {
            input.is_internal
                && (input.column_id == *column_id || input.name.eq_ignore_ascii_case(column))
        }),
        ExprKind::Cast { expr, .. } => expr_refs_internal_column(expr, input_columns),
        _ => false,
    }
}

pub(crate) fn adapt_plan_output(
    input: LogicalPlanNode,
    target_output_columns: &[OutputColumn],
) -> Result<LogicalPlanNode, String> {
    adapt_plan_output_with_qualifier(input, target_output_columns, None)
}

pub(crate) fn adapt_plan_output_with_qualifier(
    input: LogicalPlanNode,
    target_output_columns: &[OutputColumn],
    output_qualifier: Option<&str>,
) -> Result<LogicalPlanNode, String> {
    let source_output_columns = plan_output_columns(&input)?;
    if source_output_columns.len() != target_output_columns.len() {
        return Err(format!(
            "output column count mismatch while adapting subquery/CTE output: child has {}, target has {}",
            source_output_columns.len(),
            target_output_columns.len()
        ));
    }

    if source_output_columns
        .iter()
        .zip(target_output_columns.iter())
        .all(|(source, target)| output_column_metadata_equal(source, target))
        && output_qualifier.is_none()
    {
        return Ok(input);
    }

    let mut items = Vec::with_capacity(target_output_columns.len());
    for (source, target) in source_output_columns
        .iter()
        .zip(target_output_columns.iter())
    {
        if source.data_type != target.data_type {
            return Err(format!(
                "output type mismatch while adapting subquery/CTE column '{}': child={:?}, target={:?}",
                target.name, source.data_type, target.data_type
            ));
        }
        if source.nullable && !target.nullable {
            return Err(format!(
                "output nullability mismatch while adapting subquery/CTE column '{}': child={}, target={}",
                target.name, source.nullable, target.nullable
            ));
        }
        items.push(ProjectItem {
            expr: TypedExpr {
                kind: ExprKind::ColumnRef {
                    column_id: source.column_id,
                    qualifier: None,
                    column: source.name.clone(),
                },
                data_type: source.data_type.clone(),
                nullable: target.nullable,
            },
            output_name: target.name.clone(),
            output_column_id: target.column_id,
        });
    }

    Ok(LogicalPlanNode::new(
        LogicalPlanKind::Project(LogicalProjectNode {
            items: items,
            output_qualifier: output_qualifier.map(str::to_string),
        }),
        vec![input],
        None,
    ))
}

fn output_column_metadata_equal(left: &OutputColumn, right: &OutputColumn) -> bool {
    left.column_id == right.column_id
        && left.name == right.name
        && left.data_type == right.data_type
        && left.nullable == right.nullable
        && left.is_internal == right.is_internal
}

fn join_output_columns(
    join_type: JoinKind,
    left: Vec<OutputColumn>,
    right: Vec<OutputColumn>,
) -> Vec<OutputColumn> {
    match join_type {
        JoinKind::LeftSemi | JoinKind::LeftAnti | JoinKind::NullAwareLeftAnti => left,
        JoinKind::RightSemi | JoinKind::RightAnti => right,
        JoinKind::LeftOuter => {
            let mut out = left;
            out.extend(make_nullable(right));
            out
        }
        JoinKind::RightOuter => {
            let mut out = make_nullable(left);
            out.extend(right);
            out
        }
        JoinKind::FullOuter => {
            let mut out = make_nullable(left);
            out.extend(make_nullable(right));
            out
        }
        JoinKind::Inner | JoinKind::Cross => {
            let mut out = left;
            out.extend(right);
            out
        }
    }
}

fn make_nullable(mut columns: Vec<OutputColumn>) -> Vec<OutputColumn> {
    for column in &mut columns {
        column.nullable = true;
    }
    columns
}

// ---------------------------------------------------------------------------
// Apply spec wrapping helpers
// ---------------------------------------------------------------------------

/// Wrap `input` in a left-deep chain of `LogicalPlanKind::Apply` nodes, one per
/// spec whose clause matches `clause`. Each Apply's right child is the planned
/// inner subquery. Matching specs are consumed (removed) from `specs`; the
/// remaining specs are preserved for the other clause insertion points.
fn wrap_scalar_applies(
    input: LogicalPlanNode,
    specs: &mut Vec<ApplyScalarSpec>,
    clause: ApplyClause,
    cte_registry: &CTERegistry,
    factory: &mut ColumnRefFactory,
) -> Result<LogicalPlanNode, String> {
    let mut current = input;
    let mut remaining = Vec::new();
    for spec in specs.drain(..) {
        if spec.clause != clause {
            remaining.push(spec);
            continue;
        }
        let right = plan_scoped_query(spec.inner, cte_registry, factory)?;
        // Capture the inner's single scalar output column id before right is
        // moved into the LogicalApplyNode. This id is stable across M1b pushdown rules
        // (which may add group-by keys), so it is the reliable way to find the
        // scalar result in ScalarApplyToJoin (Task 3).
        let inner_output_column_id = plan_output_columns(&right)?
            .first()
            .map(|c| c.column_id)
            .ok_or_else(|| "scalar subquery inner has no output column".to_string())?;
        // Copy output-column fields before spec.output_column is moved into the LogicalApplyNode.
        let col_id = spec.output_column.column_id;
        let col_name = spec.output_column.name.clone();
        let col_type = spec.output_column.data_type.clone();
        current = LogicalPlanNode::new(
            LogicalPlanKind::Apply(LogicalApplyNode {
                kind: ApplyKind::Scalar,
                inner_output_column_id: inner_output_column_id,
                subquery_expr: TypedExpr {
                    kind: ExprKind::ColumnRef {
                        column_id: col_id,
                        qualifier: None,
                        column: col_name,
                    },
                    data_type: col_type,
                    nullable: true,
                },
                output_column: spec.output_column,
                correlation_column_ids: spec.correlation_column_ids,
                correlation_conjuncts: Vec::new(),
                residual_predicate: None,
                need_check_max_rows: spec.need_check_max_rows,
                use_semi_anti: false,
                uncorrelated_outer_predicate_columns: std::collections::HashSet::new(),
            }),
            vec![current, right],
            None,
        );
    }
    *specs = remaining;
    Ok(current)
}

/// Wrap `input` in a left-deep chain of `LogicalPlanKind::Apply` nodes for each
/// EXISTS/IN predicate spec whose clause matches `clause`. Mirrors
/// `wrap_scalar_applies` but builds `ApplyKind::Exists` / `ApplyKind::In`
/// semi/anti-collapsing applies. The M3 to-join rules read correlation and
/// residual predicates directly from the inner Filter, so construction leaves
/// `correlation_conjuncts` empty.
fn wrap_predicate_applies(
    input: LogicalPlanNode,
    specs: &mut Vec<ApplyPredicateSpec>,
    clause: ApplyClause,
    cte_registry: &CTERegistry,
    factory: &mut ColumnRefFactory,
) -> Result<LogicalPlanNode, String> {
    use crate::sql::analysis::SubqueryKind;

    let mut current = input;
    let mut remaining = Vec::new();
    for spec in specs.drain(..) {
        if spec.clause != clause {
            remaining.push(spec);
            continue;
        }
        let right = plan_scoped_query(spec.inner, cte_registry, factory)?;
        let inner_output_column_id = plan_output_columns(&right)?
            .first()
            .map(|c| c.column_id)
            .ok_or_else(|| "EXISTS/IN subquery inner has no output column".to_string())?;

        let kind = match spec.kind {
            SubqueryKind::Exists { negated } => ApplyKind::Exists { negated },
            SubqueryKind::InSubquery { negated } => ApplyKind::In { negated },
            SubqueryKind::Scalar => {
                return Err("scalar spec routed to wrap_predicate_applies".to_string());
            }
        };

        let subquery_expr = match (&kind, spec.in_lhs.clone()) {
            (ApplyKind::In { .. }, Some(lhs)) => lhs,
            (ApplyKind::In { .. }, None) => {
                return Err("IN spec missing analyzed LHS".to_string());
            }
            _ => TypedExpr {
                kind: ExprKind::ColumnRef {
                    column_id: spec.output_column.column_id,
                    qualifier: None,
                    column: spec.output_column.name.clone(),
                },
                data_type: spec.output_column.data_type.clone(),
                nullable: spec.output_column.nullable,
            },
        };

        current = LogicalPlanNode::new(
            LogicalPlanKind::Apply(LogicalApplyNode {
                kind: kind,
                subquery_expr: subquery_expr,
                output_column: spec.output_column,
                inner_output_column_id: inner_output_column_id,
                correlation_column_ids: spec.correlation_column_ids,
                correlation_conjuncts: Vec::new(),
                residual_predicate: None,
                need_check_max_rows: false,
                use_semi_anti: spec.use_semi_anti,
                uncorrelated_outer_predicate_columns: std::collections::HashSet::new(),
            }),
            vec![current, right],
            None,
        );
    }
    *specs = remaining;
    Ok(current)
}

// ---------------------------------------------------------------------------
// SELECT planning
// ---------------------------------------------------------------------------

fn plan_select_scoped(
    mut select: ResolvedSelect,
    cte_registry: &CTERegistry,
    factory: &mut ColumnRefFactory,
) -> Result<LogicalPlanNode, String> {
    const REPEAT_GROUP_QUALIFIER: &str = "__repeat_group";

    // Take ownership of all apply specs up-front. The wrap points below consume
    // them clause by clause.
    let mut apply_specs = std::mem::take(&mut select.apply_specs);
    let mut predicate_apply_specs = std::mem::take(&mut select.predicate_apply_specs);

    let mut current = match select.from.take() {
        Some(relation) => plan_relation_scoped(relation, cte_registry, factory)?,
        None => LogicalPlanNode::new(
            LogicalPlanKind::Values(LogicalValuesNode {
                rows: vec![vec![]],
                columns: vec![],
            }),
            vec![],
            None,
        ),
    };

    // WHERE placement: Apply nodes for WHERE-clause scalar subqueries are
    // inserted between the FROM plan and the WHERE Filter so the output column
    // is visible when the filter expression evaluates.
    current = wrap_scalar_applies(
        current,
        &mut apply_specs,
        ApplyClause::Where,
        cte_registry,
        factory,
    )?;
    current = wrap_predicate_applies(
        current,
        &mut predicate_apply_specs,
        ApplyClause::Where,
        cte_registry,
        factory,
    )?;

    if let Some(predicate) = select.filter.take() {
        current = LogicalPlanNode::new(
            LogicalPlanKind::Filter(LogicalFilterNode {
                predicate: predicate,
            }),
            vec![current],
            None,
        );
    }

    if let Some(mut repeat_info) = select.repeat.take() {
        let grouping_key_aliases = prepare_repeat_input(
            &mut current,
            &mut select,
            &mut repeat_info,
            REPEAT_GROUP_QUALIFIER,
            factory,
        );
        current = LogicalPlanNode::new(
            LogicalPlanKind::Repeat(LogicalRepeatNode {
                repeat_column_ref_list: repeat_info.repeat_column_ref_list,
                repeat_column_ref_ids: repeat_info.repeat_column_ref_ids,
                grouping_ids: repeat_info.grouping_ids,
                all_rollup_columns: repeat_info.all_rollup_columns,
                all_rollup_column_ids: repeat_info.all_rollup_column_ids,
                grouping_key_aliases: grouping_key_aliases,
                grouping_fn_args: repeat_info.grouping_fn_args,
                grouping_fn_arg_ids: repeat_info.grouping_fn_arg_ids,
                grouping_fn_ids: repeat_info.grouping_fn_ids,
                virtual_tuple_id: None,
            }),
            vec![current],
            None,
        );
    }

    if select.has_aggregation || !select.group_by.is_empty() {
        if let Some(ref having_expr) = select.having {
            // Collect the output column ids of HAVING apply specs so they are
            // not mistakenly promoted into the GROUP BY list. Those columns are
            // produced by Apply nodes that sit ABOVE the Aggregate, so the
            // Aggregate must not try to pass them through as group keys.
            let mut having_apply_col_ids: std::collections::HashSet<ColumnId> = apply_specs
                .iter()
                .filter(|s| s.clause == ApplyClause::Having)
                .map(|s| s.output_column.column_id)
                .collect();
            having_apply_col_ids.extend(
                predicate_apply_specs
                    .iter()
                    .filter(|s| s.clause == ApplyClause::Having)
                    .map(|s| s.output_column.column_id),
            );
            let mut extra_gb = Vec::new();
            collect_non_agg_column_refs(having_expr, &select.group_by, &mut extra_gb);
            for col in extra_gb {
                // Skip output columns of HAVING apply specs — they are
                // provided by the Apply node above the Aggregate, not below.
                if let ExprKind::ColumnRef { column_id, .. } = &col.kind
                    && having_apply_col_ids.contains(column_id)
                {
                    continue;
                }
                select.group_by.push(col);
            }
        }

        let aggregate_group_by = dedup_group_by_exprs(&select.group_by);
        let (project_items, agg_calls, output_columns, rewritten_having) =
            split_projection_for_aggregate(
                &select.projection,
                &aggregate_group_by,
                select.having.as_ref(),
                factory,
            );
        current = LogicalPlanNode::new(
            LogicalPlanKind::Aggregate(LogicalAggregateNode {
                group_by: aggregate_group_by,
                aggregates: agg_calls,
                output_columns: output_columns,
                already_pushed: false,
            }),
            vec![current],
            None,
        );

        // HAVING placement: Apply nodes for HAVING-clause scalar subqueries
        // are inserted above the Aggregate and below the HAVING Filter.
        current = wrap_scalar_applies(
            current,
            &mut apply_specs,
            ApplyClause::Having,
            cte_registry,
            factory,
        )?;
        current = wrap_predicate_applies(
            current,
            &mut predicate_apply_specs,
            ApplyClause::Having,
            cte_registry,
            factory,
        )?;

        if let Some(having) = rewritten_having {
            current = LogicalPlanNode::new(
                LogicalPlanKind::Filter(LogicalFilterNode { predicate: having }),
                vec![current],
                None,
            );
        }

        // Projection placement (aggregated branch): Apply nodes for
        // Projection-clause scalar subqueries are inserted before the window
        // and project so the output column is available for the SELECT list.
        current = wrap_scalar_applies(
            current,
            &mut apply_specs,
            ApplyClause::Projection,
            cte_registry,
            factory,
        )?;

        current = build_window_and_project(current, project_items, factory)?;
    } else {
        // Projection placement (non-aggregated branch).
        current = wrap_scalar_applies(
            current,
            &mut apply_specs,
            ApplyClause::Projection,
            cte_registry,
            factory,
        )?;

        current = build_window_and_project(current, select.projection.clone(), factory)?;
    }

    debug_assert!(
        apply_specs.is_empty() && predicate_apply_specs.is_empty(),
        "unplaced apply specs: scalar={:?} predicate={:?}",
        apply_specs.iter().map(|s| s.clause).collect::<Vec<_>>(),
        predicate_apply_specs
            .iter()
            .map(|s| s.clause)
            .collect::<Vec<_>>()
    );

    // SELECT DISTINCT → Aggregate on all output columns (deduplication)
    if select.distinct {
        current = build_distinct(current, &select.projection, factory);
    }

    Ok(current)
}

fn prepare_repeat_input(
    current: &mut LogicalPlanNode,
    select: &mut ResolvedSelect,
    repeat_info: &mut crate::sql::analysis::RepeatInfo,
    repeat_group_qualifier: &str,
    factory: &mut ColumnRefFactory,
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
    let mut substitutions: Vec<RepeatSubstitution> = Vec::new();
    let mut repeat_key_ids_by_name: std::collections::HashMap<String, ColumnId> =
        std::collections::HashMap::new();
    let mut all_rollup_column_ids = Vec::with_capacity(grouping_key_aliases.len());
    for (idx, (_, alias_name)) in grouping_key_aliases.iter().enumerate() {
        let Some(source_expr) = select.group_by.get(idx).cloned() else {
            continue;
        };
        let data_type = source_expr.data_type.clone();
        let nullable = source_expr.nullable;
        let original_display = typed_expr_display_name(&source_expr);
        let materialized_column_id =
            factory.create(None, alias_name.clone(), data_type.clone(), nullable);
        if let Some((original_name, _)) = grouping_key_aliases.get(idx) {
            repeat_key_ids_by_name
                .insert(original_name.to_ascii_lowercase(), materialized_column_id);
        }
        repeat_key_ids_by_name.insert(alias_name.to_ascii_lowercase(), materialized_column_id);
        all_rollup_column_ids.push(materialized_column_id);

        // Substitute downstream grouping-key references with the materialized
        // alias slot so Aggregate reads Repeat's nullified value rather than
        // the pre-Repeat input column.
        let replacement = TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: materialized_column_id,
                qualifier: Some(repeat_group_qualifier.to_string()),
                column: alias_name.clone(),
            },
            data_type,
            nullable,
        };
        substitutions.push(RepeatSubstitution {
            display_name: original_display,
            source_column_id: direct_column_ref_id(&source_expr),
            replacement,
        });

        project_items.push(ProjectItem {
            expr: source_expr,
            output_name: alias_name.clone(),
            output_column_id: materialized_column_id,
        });
    }

    repeat_info.repeat_column_ref_ids = repeat_info
        .repeat_column_ref_list
        .iter()
        .map(|non_null_cols| {
            non_null_cols
                .iter()
                .filter_map(|col| {
                    repeat_key_ids_by_name
                        .get(&col.to_ascii_lowercase())
                        .copied()
                })
                .collect()
        })
        .collect();
    repeat_info.all_rollup_column_ids = all_rollup_column_ids;
    repeat_info.grouping_fn_arg_ids = repeat_info
        .grouping_fn_args
        .iter()
        .map(|(_, arg_cols)| {
            arg_cols
                .iter()
                .filter_map(|col| {
                    repeat_key_ids_by_name
                        .get(&col.to_ascii_lowercase())
                        .copied()
                })
                .collect()
        })
        .collect();

    *current = LogicalPlanNode::new(
        LogicalPlanKind::Project(LogicalProjectNode {
            items: project_items,
            output_qualifier: None,
        }),
        vec![current.clone()],
        None,
    );

    // Apply substitutions to group_by, projection, having so that every
    // place the original rollup-key expression appeared now reads from
    // the materialized alias slot.
    for gb_expr in &mut select.group_by {
        substitute_expr_in_place(gb_expr, &substitutions);
    }
    for item in &mut select.projection {
        substitute_expr_in_place(&mut item.expr, &substitutions);
        if let Some(column_id) = direct_column_ref_id(&item.expr) {
            item.output_column_id = column_id;
        }
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
#[derive(Clone)]
struct RepeatSubstitution {
    display_name: String,
    source_column_id: Option<ColumnId>,
    replacement: TypedExpr,
}

fn substitute_expr_in_place(expr: &mut TypedExpr, substitutions: &[RepeatSubstitution]) {
    let name = typed_expr_display_name(expr);
    if let Some(substitution) = substitutions.iter().find(|substitution| {
        substitution.display_name == name
            || direct_column_ref_id(expr)
                .zip(substitution.source_column_id)
                .is_some_and(|(expr_id, source_id)| expr_id == source_id)
    }) {
        *expr = substitution.replacement.clone();
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
        ExprKind::WindowCall {
            args,
            partition_by,
            order_by,
            ..
        } => {
            for a in args {
                substitute_expr_in_place(a, substitutions);
            }
            for p in partition_by {
                substitute_expr_in_place(p, substitutions);
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
    input: LogicalPlanNode,
    projection: &[ProjectItem],
    factory: &mut ColumnRefFactory,
) -> LogicalPlanNode {
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
    LogicalPlanNode::new(
        LogicalPlanKind::Aggregate(LogicalAggregateNode {
            group_by: group_by,
            aggregates: vec![],
            output_columns: output_columns,
            already_pushed: false,
        }),
        vec![input],
        None,
    )
}

/// Check if an expression contains any WindowCall.
/// Build Window + Project nodes if the projection contains window functions,
/// otherwise just a Project node.
fn build_window_and_project(
    input: LogicalPlanNode,
    project_items: Vec<ProjectItem>,
    factory: &mut ColumnRefFactory,
) -> Result<LogicalPlanNode, String> {
    let project_items = dedup_project_item_output_ids(project_items, factory);
    let has_window = project_items.iter().any(|item| has_window_call(&item.expr));
    if has_window {
        let mut output_columns = plan_output_columns(&input)?;
        let (window_exprs, rewritten_items) =
            extract_window_calls(&project_items, &mut output_columns, factory);
        // The analytic operator requires input sorted by (partition_by, order_by).
        // Insert a Sort node before the Window node using the first window
        // function's sort keys.  When window functions have different
        // partition/order signatures, the physical emitter splits them into
        // separate Sort + Analytic nodes (see fragment_builder.rs::visit_window_multi_group).
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
        let input_already_ordered =
            logical_plan_satisfies_window_ordering(&input, &sort_items, &analytic_partition_by);
        let sorted_input = if sort_items.is_empty() || input_already_ordered {
            input
        } else {
            LogicalPlanNode::new(
                LogicalPlanKind::Sort(LogicalSortNode {
                    items: sort_items,
                    analytic_partition_by: analytic_partition_by,
                    output_columns: vec![],
                    offset: None,
                    partition_limit: None,
                    topn_type: None,
                }),
                vec![input],
                None,
            )
        };

        let windowed = LogicalPlanNode::new(
            LogicalPlanKind::Window(LogicalWindowNode {
                window_exprs: window_exprs,
                output_columns: output_columns,
            }),
            vec![sorted_input],
            None,
        );
        Ok(LogicalPlanNode::new(
            LogicalPlanKind::Project(LogicalProjectNode {
                items: rewritten_items,
                output_qualifier: None,
            }),
            vec![windowed],
            None,
        ))
    } else if !project_items.is_empty() {
        Ok(LogicalPlanNode::new(
            LogicalPlanKind::Project(LogicalProjectNode {
                items: project_items,
                output_qualifier: None,
            }),
            vec![input],
            None,
        ))
    } else {
        Ok(input)
    }
}

fn dedup_project_item_output_ids(
    mut project_items: Vec<ProjectItem>,
    factory: &mut ColumnRefFactory,
) -> Vec<ProjectItem> {
    let mut seen = std::collections::HashSet::new();
    for item in &mut project_items {
        if item.output_column_id != ColumnId::UNSET && seen.insert(item.output_column_id) {
            continue;
        }
        item.output_column_id = factory.create(
            None,
            item.output_name.clone(),
            item.expr.data_type.clone(),
            item.expr.nullable,
        );
        seen.insert(item.output_column_id);
    }
    project_items
}

fn logical_plan_satisfies_window_ordering(
    input: &LogicalPlanNode,
    required_items: &[SortItem],
    partition_by: &[TypedExpr],
) -> bool {
    match &input.kind {
        LogicalPlanKind::Project(project) if project_preserves_column_identity(project) => {
            logical_plan_satisfies_window_ordering(
                input.unary_input(),
                required_items,
                partition_by,
            )
        }
        LogicalPlanKind::Sort(sort) => {
            logical_sort_satisfies_window_ordering(sort, required_items, partition_by)
        }
        _ => false,
    }
}

fn project_preserves_column_identity(project: &LogicalProjectNode) -> bool {
    project.items.iter().all(|item| {
        matches!(
            &item.expr.kind,
            ExprKind::ColumnRef { column_id, .. } if item.output_column_id == *column_id
        )
    })
}

fn logical_sort_satisfies_window_ordering(
    sort: &LogicalSortNode,
    required_items: &[SortItem],
    partition_by: &[TypedExpr],
) -> bool {
    let required = ordering_spec_from_sort_items(required_items);
    let provided = ordering_spec_from_sort_items(&sort.items);
    if matches!(required, OrderingSpec::Any) || !provided.satisfies(&required) {
        return false;
    }
    // A regular ORDER BY Sort gathers globally. That is enough only for
    // non-partitioned windows; partitioned windows need the analytic-partition
    // tag unless the child Sort already has an equivalent tag.
    partition_by.is_empty()
        || ordering_spec_from_sort_items(
            &sort
                .analytic_partition_by
                .iter()
                .map(|expr| SortItem {
                    expr: expr.clone(),
                    asc: true,
                    nulls_first: true,
                })
                .collect::<Vec<_>>(),
        )
        .satisfies(&ordering_spec_from_sort_items(
            &partition_by
                .iter()
                .map(|expr| SortItem {
                    expr: expr.clone(),
                    asc: true,
                    nulls_first: true,
                })
                .collect::<Vec<_>>(),
        ))
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
fn extract_window_calls(
    items: &[ProjectItem],
    output_columns: &mut Vec<OutputColumn>,
    factory: &mut ColumnRefFactory,
) -> (Vec<WindowExpr>, Vec<ProjectItem>) {
    let mut window_exprs = Vec::new();
    let mut rewritten = Vec::new();

    for item in items {
        if has_window_call(&item.expr) {
            let mut counter = 0usize;
            let mut output_ids = WindowOutputIdAllocator {
                factory,
                output_columns,
                visible_output_column_id: item.output_column_id,
                reuse_visible_output_id: is_exact_window_call(&item.expr),
                visible_output_id_used: false,
            };
            let new_expr = rewrite_window_calls(
                &item.expr,
                &item.output_name,
                &mut output_ids,
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

struct WindowOutputIdAllocator<'a> {
    factory: &'a mut ColumnRefFactory,
    output_columns: &'a mut Vec<OutputColumn>,
    visible_output_column_id: ColumnId,
    reuse_visible_output_id: bool,
    visible_output_id_used: bool,
}

impl WindowOutputIdAllocator<'_> {
    fn allocate(&mut self, output_name: &str, data_type: DataType, nullable: bool) -> ColumnId {
        let reuse_visible_output_id = self.reuse_visible_output_id
            && !self.visible_output_id_used
            && self.visible_output_column_id != ColumnId::UNSET;
        let column_id = if reuse_visible_output_id {
            self.visible_output_id_used = true;
            self.visible_output_column_id
        } else {
            self.factory
                .create(None, output_name.to_string(), data_type.clone(), nullable)
        };
        self.output_columns.push(OutputColumn {
            column_id,
            name: output_name.to_string(),
            data_type,
            nullable,
            is_internal: !reuse_visible_output_id,
        });
        column_id
    }
}

fn is_exact_window_call(expr: &TypedExpr) -> bool {
    match &expr.kind {
        ExprKind::WindowCall { .. } => true,
        ExprKind::Nested(inner) => is_exact_window_call(inner),
        _ => false,
    }
}

/// Recursively rewrite an expression tree, replacing each WindowCall node
/// with a ColumnRef that points to the window function's output column.
fn rewrite_window_calls(
    expr: &TypedExpr,
    base_name: &str,
    output_ids: &mut WindowOutputIdAllocator<'_>,
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
            let output_column_id =
                output_ids.allocate(&win_output_name, expr.data_type.clone(), expr.nullable);

            window_exprs.push(WindowExpr {
                name: rewritten_name,
                args: args.clone(),
                distinct: *distinct,
                partition_by: partition_by.clone(),
                order_by: rewritten_order_by,
                window_frame: rewritten_frame,
                result_type: expr.data_type.clone(),
                output_name: win_output_name.clone(),
                output_column_id,
                ignore_nulls: *ignore_nulls,
            });
            TypedExpr {
                kind: ExprKind::ColumnRef {
                    column_id: output_column_id,
                    qualifier: None,
                    column: win_output_name,
                },
                data_type: expr.data_type.clone(),
                nullable: expr.nullable,
            }
        }
        ExprKind::BinaryOp { left, right, op } => TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(rewrite_window_calls(
                    left,
                    base_name,
                    output_ids,
                    window_exprs,
                    counter,
                )),
                op: *op,
                right: Box::new(rewrite_window_calls(
                    right,
                    base_name,
                    output_ids,
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
                    output_ids,
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
                    .map(|arg| {
                        rewrite_window_calls(arg, base_name, output_ids, window_exprs, counter)
                    })
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
                    .map(|arg| {
                        rewrite_window_calls(arg, base_name, output_ids, window_exprs, counter)
                    })
                    .collect(),
                distinct: *distinct,
                order_by: order_by
                    .iter()
                    .map(|item| SortItem {
                        expr: rewrite_window_calls(
                            &item.expr,
                            base_name,
                            output_ids,
                            window_exprs,
                            counter,
                        ),
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
                    output_ids,
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
                    output_ids,
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
                    output_ids,
                    window_exprs,
                    counter,
                )),
                list: list
                    .iter()
                    .map(|item| {
                        rewrite_window_calls(item, base_name, output_ids, window_exprs, counter)
                    })
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
                    output_ids,
                    window_exprs,
                    counter,
                )),
                low: Box::new(rewrite_window_calls(
                    low,
                    base_name,
                    output_ids,
                    window_exprs,
                    counter,
                )),
                high: Box::new(rewrite_window_calls(
                    high,
                    base_name,
                    output_ids,
                    window_exprs,
                    counter,
                )),
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
                    output_ids,
                    window_exprs,
                    counter,
                )),
                pattern: Box::new(rewrite_window_calls(
                    pattern,
                    base_name,
                    output_ids,
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
                        output_ids,
                        window_exprs,
                        counter,
                    ))
                }),
                when_then: when_then
                    .iter()
                    .map(|(when, then)| {
                        (
                            rewrite_window_calls(
                                when,
                                base_name,
                                output_ids,
                                window_exprs,
                                counter,
                            ),
                            rewrite_window_calls(
                                then,
                                base_name,
                                output_ids,
                                window_exprs,
                                counter,
                            ),
                        )
                    })
                    .collect(),
                else_expr: else_expr.as_ref().map(|inner| {
                    Box::new(rewrite_window_calls(
                        inner,
                        base_name,
                        output_ids,
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
                    output_ids,
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
                output_ids,
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
) -> (
    Vec<ProjectItem>,
    Vec<AggregateCall>,
    Vec<OutputColumn>,
    Option<TypedExpr>,
) {
    let mut agg_calls = Vec::new();

    for item in projection {
        collect_aggregates(&item.expr, &mut agg_calls, factory);
    }

    // Also collect aggregate calls from HAVING clause so the aggregate node
    // computes them even when they don't appear in SELECT.
    if let Some(having_expr) = having {
        collect_aggregates(having_expr, &mut agg_calls, factory);
    }

    let mut output_columns = Vec::with_capacity(group_by.len() + agg_calls.len());
    let mut group_by_rewrite_targets = Vec::new();
    for gb in group_by {
        let output_column = group_by_output_column(gb, projection, factory);
        group_by_rewrite_targets.push(GroupByRewriteTarget {
            expr: gb.clone(),
            column_id: output_column.column_id,
            display_name: typed_expr_display_name(gb),
        });
        output_columns.push(output_column);
    }
    output_columns.extend(agg_calls.iter().map(|call| {
        let name = crate::sql::codegen::helpers::agg_call_display_name(call);
        OutputColumn {
            column_id: call.output_column_id,
            name,
            data_type: call.result_type.clone(),
            nullable: true,
            is_internal: false,
        }
    }));

    let project_items = projection
        .iter()
        .map(|item| {
            let expr = rewrite_agg_calls_to_refs(&item.expr, &agg_calls);
            let expr = rewrite_group_by_expr_refs(&expr, &group_by_rewrite_targets);
            let output_column_id = direct_column_ref_id(&expr).unwrap_or(item.output_column_id);
            ProjectItem {
                expr,
                output_name: item.output_name.clone(),
                output_column_id,
            }
        })
        .collect();
    let rewritten_having = having.map(|expr| {
        let expr = rewrite_agg_calls_to_refs(expr, &agg_calls);
        rewrite_group_by_expr_refs(&expr, &group_by_rewrite_targets)
    });

    (project_items, agg_calls, output_columns, rewritten_having)
}

fn direct_column_ref_id(expr: &TypedExpr) -> Option<ColumnId> {
    match &expr.kind {
        ExprKind::ColumnRef { column_id, .. } if *column_id != ColumnId::UNSET => Some(*column_id),
        ExprKind::Nested(inner) => direct_column_ref_id(inner),
        _ => None,
    }
}

fn dedup_group_by_exprs(group_by: &[TypedExpr]) -> Vec<TypedExpr> {
    let mut deduped = Vec::with_capacity(group_by.len());
    for expr in group_by {
        if !deduped
            .iter()
            .any(|existing| typed_expr_semantically_eq(existing, expr))
        {
            deduped.push(expr.clone());
        }
    }
    deduped
}

fn ensure_aggregate_output_columns(agg: &mut LogicalAggregateNode) {
    let mut existing: std::collections::HashSet<ColumnId> = agg
        .output_columns
        .iter()
        .map(|column| column.column_id)
        .filter(|id| *id != ColumnId::UNSET)
        .collect();

    for call in &agg.aggregates {
        if call.output_column_id == ColumnId::UNSET || existing.contains(&call.output_column_id) {
            continue;
        }
        existing.insert(call.output_column_id);
        agg.output_columns.push(OutputColumn {
            column_id: call.output_column_id,
            name: crate::sql::codegen::helpers::agg_call_display_name(call),
            data_type: call.result_type.clone(),
            nullable: true,
            is_internal: true,
        });
    }
}

fn planner_aggregate_group_by_targets(agg: &LogicalAggregateNode) -> Vec<GroupByRewriteTarget> {
    let aggregate_output_ids: std::collections::HashSet<ColumnId> = agg
        .aggregates
        .iter()
        .map(|call| call.output_column_id)
        .filter(|id| *id != ColumnId::UNSET)
        .collect();
    let group_key_outputs = agg
        .output_columns
        .iter()
        .filter(|column| !aggregate_output_ids.contains(&column.column_id));

    agg.group_by
        .iter()
        .zip(group_key_outputs)
        .map(|(gb, output_column)| GroupByRewriteTarget {
            expr: gb.clone(),
            column_id: output_column.column_id,
            display_name: typed_expr_display_name(gb),
        })
        .collect()
}

fn planner_repeat_original_group_by_targets(
    aggregate_plan: &LogicalPlanNode,
) -> Vec<GroupByRewriteTarget> {
    let LogicalPlanKind::Aggregate(agg) = &aggregate_plan.kind else {
        return Vec::new();
    };
    let Some(repeat) = aggregate_plan
        .children
        .first()
        .and_then(|child| match &child.kind {
            LogicalPlanKind::Repeat(repeat) => Some(repeat),
            _ => None,
        })
    else {
        return Vec::new();
    };

    let aggregate_targets = planner_aggregate_group_by_targets(agg);
    repeat
        .grouping_key_aliases
        .iter()
        .enumerate()
        .filter_map(|(idx, (original_name, alias_name))| {
            let alias_id = repeat.all_rollup_column_ids.get(idx).copied();
            let target = aggregate_targets.iter().find(|target| {
                target.display_name.eq_ignore_ascii_case(alias_name)
                    || matches!(
                        &target.expr.kind,
                        ExprKind::ColumnRef { column_id, column, .. }
                            if alias_id.is_some_and(|id| id == *column_id)
                                || column.eq_ignore_ascii_case(alias_name)
                    )
            })?;
            Some(GroupByRewriteTarget {
                expr: TypedExpr {
                    kind: ExprKind::ColumnRef {
                        column_id: ColumnId::UNSET,
                        qualifier: None,
                        column: original_name.clone(),
                    },
                    data_type: target.expr.data_type.clone(),
                    nullable: target.expr.nullable,
                },
                column_id: target.column_id,
                display_name: target.display_name.clone(),
            })
        })
        .collect()
}

fn group_by_output_column(
    group_by: &TypedExpr,
    projection: &[ProjectItem],
    factory: &mut ColumnRefFactory,
) -> OutputColumn {
    let matching_projection = projection
        .iter()
        .find(|item| typed_expr_semantically_eq(&item.expr, group_by));
    if let Some(item) = matching_projection {
        return OutputColumn {
            column_id: expr_column_id(&item.expr, &item.output_name, factory),
            name: item.output_name.clone(),
            data_type: item.expr.data_type.clone(),
            nullable: item.expr.nullable,
            is_internal: false,
        };
    }

    let name = typed_expr_display_name(group_by);
    OutputColumn {
        column_id: expr_column_id(group_by, &name, factory),
        name,
        data_type: group_by.data_type.clone(),
        nullable: group_by.nullable,
        is_internal: true,
    }
}

#[derive(Clone)]
struct GroupByRewriteTarget {
    expr: TypedExpr,
    column_id: ColumnId,
    display_name: String,
}

fn rewrite_agg_calls_to_refs(expr: &TypedExpr, agg_calls: &[AggregateCall]) -> TypedExpr {
    if let ExprKind::AggregateCall {
        name,
        args,
        distinct,
        order_by,
    } = &expr.kind
        && let Some(call) = agg_calls
            .iter()
            .find(|call| aggregate_call_matches(call, name, args, *distinct, order_by))
    {
        let display = crate::sql::codegen::helpers::agg_call_display_name_from_parts(
            name, args, *distinct, order_by,
        );
        return TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: call.output_column_id,
                qualifier: None,
                column: display,
            },
            data_type: expr.data_type.clone(),
            nullable: expr.nullable,
        };
    }
    rewrite_expr_children(expr, |child| rewrite_agg_calls_to_refs(child, agg_calls))
}

fn rewrite_group_by_expr_refs(expr: &TypedExpr, targets: &[GroupByRewriteTarget]) -> TypedExpr {
    for target in targets {
        if typed_expr_semantically_eq(expr, &target.expr) {
            return TypedExpr {
                kind: ExprKind::ColumnRef {
                    column_id: target.column_id,
                    qualifier: None,
                    column: target.display_name.clone(),
                },
                data_type: expr.data_type.clone(),
                nullable: expr.nullable,
            };
        }
    }
    rewrite_expr_children(expr, |child| rewrite_group_by_expr_refs(child, targets))
}

fn rewrite_expr_children(
    expr: &TypedExpr,
    mut rewrite_child: impl FnMut(&TypedExpr) -> TypedExpr,
) -> TypedExpr {
    let kind = match &expr.kind {
        ExprKind::BinaryOp { left, op, right } => ExprKind::BinaryOp {
            left: Box::new(rewrite_child(left)),
            op: *op,
            right: Box::new(rewrite_child(right)),
        },
        ExprKind::UnaryOp { op, expr: inner } => ExprKind::UnaryOp {
            op: *op,
            expr: Box::new(rewrite_child(inner)),
        },
        ExprKind::FunctionCall {
            name,
            args,
            distinct,
        } => ExprKind::FunctionCall {
            name: name.clone(),
            args: args.iter().map(&mut rewrite_child).collect(),
            distinct: *distinct,
        },
        ExprKind::LambdaFunction { params, body } => ExprKind::LambdaFunction {
            params: params.clone(),
            body: Box::new(rewrite_child(body)),
        },
        ExprKind::Cast {
            expr: inner,
            target,
        } => ExprKind::Cast {
            expr: Box::new(rewrite_child(inner)),
            target: target.clone(),
        },
        ExprKind::IsNull {
            expr: inner,
            negated,
        } => ExprKind::IsNull {
            expr: Box::new(rewrite_child(inner)),
            negated: *negated,
        },
        ExprKind::InList {
            expr: inner,
            list,
            negated,
        } => ExprKind::InList {
            expr: Box::new(rewrite_child(inner)),
            list: list.iter().map(&mut rewrite_child).collect(),
            negated: *negated,
        },
        ExprKind::Between {
            expr: inner,
            low,
            high,
            negated,
        } => ExprKind::Between {
            expr: Box::new(rewrite_child(inner)),
            low: Box::new(rewrite_child(low)),
            high: Box::new(rewrite_child(high)),
            negated: *negated,
        },
        ExprKind::Like {
            expr: inner,
            pattern,
            negated,
        } => ExprKind::Like {
            expr: Box::new(rewrite_child(inner)),
            pattern: Box::new(rewrite_child(pattern)),
            negated: *negated,
        },
        ExprKind::Case {
            operand,
            when_then,
            else_expr,
        } => ExprKind::Case {
            operand: operand
                .as_ref()
                .map(|operand| Box::new(rewrite_child(operand))),
            when_then: when_then
                .iter()
                .map(|(when, then)| (rewrite_child(when), rewrite_child(then)))
                .collect(),
            else_expr: else_expr
                .as_ref()
                .map(|else_expr| Box::new(rewrite_child(else_expr))),
        },
        ExprKind::IsTruthValue {
            expr: inner,
            value,
            negated,
        } => ExprKind::IsTruthValue {
            expr: Box::new(rewrite_child(inner)),
            value: *value,
            negated: *negated,
        },
        ExprKind::Nested(inner) => ExprKind::Nested(Box::new(rewrite_child(inner))),
        ExprKind::WindowCall {
            name,
            args,
            distinct,
            partition_by,
            order_by,
            window_frame,
            ignore_nulls,
        } => ExprKind::WindowCall {
            name: name.clone(),
            args: args.iter().map(&mut rewrite_child).collect(),
            distinct: *distinct,
            partition_by: partition_by.iter().map(&mut rewrite_child).collect(),
            order_by: order_by
                .iter()
                .map(|item| SortItem {
                    expr: rewrite_child(&item.expr),
                    asc: item.asc,
                    nulls_first: item.nulls_first,
                })
                .collect(),
            window_frame: window_frame.clone(),
            ignore_nulls: *ignore_nulls,
        },
        ExprKind::Lambda { params, body } => ExprKind::Lambda {
            params: params.clone(),
            body: Box::new(rewrite_child(body)),
        },
        ExprKind::AggregateCall { .. }
        | ExprKind::ColumnRef { .. }
        | ExprKind::LambdaParamRef { .. }
        | ExprKind::Literal(_)
        | ExprKind::SubqueryPlaceholder { .. } => return expr.clone(),
    };
    TypedExpr {
        kind,
        data_type: expr.data_type.clone(),
        nullable: expr.nullable,
    }
}

fn aggregate_call_matches(
    call: &AggregateCall,
    name: &str,
    args: &[TypedExpr],
    distinct: bool,
    order_by: &[SortItem],
) -> bool {
    call.name == name
        && call.distinct == distinct
        && call.args.len() == args.len()
        && call.order_by.len() == order_by.len()
        && call
            .args
            .iter()
            .zip(args.iter())
            .all(|(left, right)| typed_expr_semantically_eq(left, right))
        && call
            .order_by
            .iter()
            .zip(order_by.iter())
            .all(|(left, right)| sort_item_semantically_eq(left, right))
}

fn typed_expr_semantically_eq(left: &TypedExpr, right: &TypedExpr) -> bool {
    match (&left.kind, &right.kind) {
        (
            ExprKind::ColumnRef {
                column_id: left_id,
                qualifier: left_qualifier,
                column: left_column,
            },
            ExprKind::ColumnRef {
                column_id: right_id,
                qualifier: right_qualifier,
                column: right_column,
            },
        ) => {
            if *left_id != ColumnId::UNSET && *right_id != ColumnId::UNSET {
                left_id == right_id
            } else {
                left_qualifier.as_ref().map(|q| q.to_lowercase())
                    == right_qualifier.as_ref().map(|q| q.to_lowercase())
                    && left_column.eq_ignore_ascii_case(right_column)
            }
        }
        (
            ExprKind::LambdaParamRef {
                name: left_name,
                slot_id: left_slot,
            },
            ExprKind::LambdaParamRef {
                name: right_name,
                slot_id: right_slot,
            },
        ) => left_slot == right_slot && left_name.eq_ignore_ascii_case(right_name),
        (ExprKind::Literal(left), ExprKind::Literal(right)) => left == right,
        (
            ExprKind::BinaryOp {
                left: left_left,
                op: left_op,
                right: left_right,
            },
            ExprKind::BinaryOp {
                left: right_left,
                op: right_op,
                right: right_right,
            },
        ) => {
            left_op == right_op
                && typed_expr_semantically_eq(left_left, right_left)
                && typed_expr_semantically_eq(left_right, right_right)
        }
        (
            ExprKind::UnaryOp {
                op: left_op,
                expr: left_expr,
            },
            ExprKind::UnaryOp {
                op: right_op,
                expr: right_expr,
            },
        ) => left_op == right_op && typed_expr_semantically_eq(left_expr, right_expr),
        (
            ExprKind::FunctionCall {
                name: left_name,
                args: left_args,
                distinct: left_distinct,
            },
            ExprKind::FunctionCall {
                name: right_name,
                args: right_args,
                distinct: right_distinct,
            },
        ) => {
            left_name.eq_ignore_ascii_case(right_name)
                && left_distinct == right_distinct
                && typed_expr_slices_semantically_eq(left_args, right_args)
        }
        (
            ExprKind::LambdaFunction {
                params: left_params,
                body: left_body,
            },
            ExprKind::LambdaFunction {
                params: right_params,
                body: right_body,
            },
        ) => {
            left_params.len() == right_params.len()
                && typed_expr_semantically_eq(left_body, right_body)
        }
        (
            ExprKind::AggregateCall {
                name: left_name,
                args: left_args,
                distinct: left_distinct,
                order_by: left_order_by,
            },
            ExprKind::AggregateCall {
                name: right_name,
                args: right_args,
                distinct: right_distinct,
                order_by: right_order_by,
            },
        ) => {
            left_name.eq_ignore_ascii_case(right_name)
                && left_distinct == right_distinct
                && typed_expr_slices_semantically_eq(left_args, right_args)
                && sort_item_slices_semantically_eq(left_order_by, right_order_by)
        }
        (
            ExprKind::Cast {
                expr: left_expr,
                target: left_target,
            },
            ExprKind::Cast {
                expr: right_expr,
                target: right_target,
            },
        ) => left_target == right_target && typed_expr_semantically_eq(left_expr, right_expr),
        (
            ExprKind::IsNull {
                expr: left_expr,
                negated: left_negated,
            },
            ExprKind::IsNull {
                expr: right_expr,
                negated: right_negated,
            },
        ) => left_negated == right_negated && typed_expr_semantically_eq(left_expr, right_expr),
        (
            ExprKind::InList {
                expr: left_expr,
                list: left_list,
                negated: left_negated,
            },
            ExprKind::InList {
                expr: right_expr,
                list: right_list,
                negated: right_negated,
            },
        ) => {
            left_negated == right_negated
                && typed_expr_semantically_eq(left_expr, right_expr)
                && typed_expr_slices_semantically_eq(left_list, right_list)
        }
        (
            ExprKind::Between {
                expr: left_expr,
                low: left_low,
                high: left_high,
                negated: left_negated,
            },
            ExprKind::Between {
                expr: right_expr,
                low: right_low,
                high: right_high,
                negated: right_negated,
            },
        ) => {
            left_negated == right_negated
                && typed_expr_semantically_eq(left_expr, right_expr)
                && typed_expr_semantically_eq(left_low, right_low)
                && typed_expr_semantically_eq(left_high, right_high)
        }
        (
            ExprKind::Like {
                expr: left_expr,
                pattern: left_pattern,
                negated: left_negated,
            },
            ExprKind::Like {
                expr: right_expr,
                pattern: right_pattern,
                negated: right_negated,
            },
        ) => {
            left_negated == right_negated
                && typed_expr_semantically_eq(left_expr, right_expr)
                && typed_expr_semantically_eq(left_pattern, right_pattern)
        }
        (
            ExprKind::Case {
                operand: left_operand,
                when_then: left_when_then,
                else_expr: left_else,
            },
            ExprKind::Case {
                operand: right_operand,
                when_then: right_when_then,
                else_expr: right_else,
            },
        ) => {
            optional_typed_expr_semantically_eq(left_operand.as_deref(), right_operand.as_deref())
                && left_when_then.len() == right_when_then.len()
                && left_when_then.iter().zip(right_when_then.iter()).all(
                    |((left_when, left_then), (right_when, right_then))| {
                        typed_expr_semantically_eq(left_when, right_when)
                            && typed_expr_semantically_eq(left_then, right_then)
                    },
                )
                && optional_typed_expr_semantically_eq(left_else.as_deref(), right_else.as_deref())
        }
        (
            ExprKind::IsTruthValue {
                expr: left_expr,
                value: left_value,
                negated: left_negated,
            },
            ExprKind::IsTruthValue {
                expr: right_expr,
                value: right_value,
                negated: right_negated,
            },
        ) => {
            left_value == right_value
                && left_negated == right_negated
                && typed_expr_semantically_eq(left_expr, right_expr)
        }
        (ExprKind::Nested(left), ExprKind::Nested(right)) => {
            typed_expr_semantically_eq(left, right)
        }
        (
            ExprKind::WindowCall {
                name: left_name,
                args: left_args,
                distinct: left_distinct,
                partition_by: left_partition_by,
                order_by: left_order_by,
                window_frame: left_frame,
                ignore_nulls: left_ignore_nulls,
            },
            ExprKind::WindowCall {
                name: right_name,
                args: right_args,
                distinct: right_distinct,
                partition_by: right_partition_by,
                order_by: right_order_by,
                window_frame: right_frame,
                ignore_nulls: right_ignore_nulls,
            },
        ) => {
            left_name.eq_ignore_ascii_case(right_name)
                && left_distinct == right_distinct
                && left_ignore_nulls == right_ignore_nulls
                && format!("{left_frame:?}") == format!("{right_frame:?}")
                && typed_expr_slices_semantically_eq(left_args, right_args)
                && typed_expr_slices_semantically_eq(left_partition_by, right_partition_by)
                && sort_item_slices_semantically_eq(left_order_by, right_order_by)
        }
        (
            ExprKind::SubqueryPlaceholder {
                id: left_id,
                kind: left_kind,
                data_type: left_type,
            },
            ExprKind::SubqueryPlaceholder {
                id: right_id,
                kind: right_kind,
                data_type: right_type,
            },
        ) => {
            left_id == right_id
                && format!("{left_kind:?}") == format!("{right_kind:?}")
                && left_type == right_type
        }
        (
            ExprKind::Lambda {
                params: left_params,
                body: left_body,
            },
            ExprKind::Lambda {
                params: right_params,
                body: right_body,
            },
        ) => left_params == right_params && typed_expr_semantically_eq(left_body, right_body),
        _ => false,
    }
}

fn optional_typed_expr_semantically_eq(
    left: Option<&TypedExpr>,
    right: Option<&TypedExpr>,
) -> bool {
    match (left, right) {
        (Some(left), Some(right)) => typed_expr_semantically_eq(left, right),
        (None, None) => true,
        _ => false,
    }
}

fn typed_expr_slices_semantically_eq(left: &[TypedExpr], right: &[TypedExpr]) -> bool {
    left.len() == right.len()
        && left
            .iter()
            .zip(right.iter())
            .all(|(left, right)| typed_expr_semantically_eq(left, right))
}

fn sort_item_semantically_eq(left: &SortItem, right: &SortItem) -> bool {
    left.asc == right.asc
        && left.nulls_first == right.nulls_first
        && typed_expr_semantically_eq(&left.expr, &right.expr)
}

fn sort_item_slices_semantically_eq(left: &[SortItem], right: &[SortItem]) -> bool {
    left.len() == right.len()
        && left
            .iter()
            .zip(right.iter())
            .all(|(left, right)| sort_item_semantically_eq(left, right))
}

/// Recursively collect AggregateCall from a TypedExpr tree.
fn collect_aggregates(
    expr: &TypedExpr,
    out: &mut Vec<AggregateCall>,
    factory: &mut ColumnRefFactory,
) {
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
                let display = crate::sql::codegen::helpers::agg_call_display_name_from_parts(
                    name, args, *distinct, order_by,
                );
                let output_column_id =
                    factory.create(None, display, expr.data_type.clone(), expr.nullable);
                out.push(AggregateCall {
                    name: name.clone(),
                    args: args.clone(),
                    distinct: *distinct,
                    result_type: expr.data_type.clone(),
                    order_by: order_by.clone(),
                    output_column_id,
                });
            }
        }
        ExprKind::BinaryOp { left, right, .. } => {
            collect_aggregates(left, out, factory);
            collect_aggregates(right, out, factory);
        }
        ExprKind::UnaryOp { expr: inner, .. } => collect_aggregates(inner, out, factory),
        ExprKind::FunctionCall { args, .. } => {
            for arg in args {
                collect_aggregates(arg, out, factory);
            }
        }
        ExprKind::LambdaFunction { body, .. } => collect_aggregates(body, out, factory),
        ExprKind::Cast { expr: inner, .. } => collect_aggregates(inner, out, factory),
        ExprKind::Case {
            operand,
            when_then,
            else_expr,
        } => {
            if let Some(op) = operand {
                collect_aggregates(op, out, factory);
            }
            for (w, t) in when_then {
                collect_aggregates(w, out, factory);
                collect_aggregates(t, out, factory);
            }
            if let Some(e) = else_expr {
                collect_aggregates(e, out, factory);
            }
        }
        ExprKind::IsNull { expr: inner, .. } => collect_aggregates(inner, out, factory),
        ExprKind::Nested(inner) => collect_aggregates(inner, out, factory),
        ExprKind::InList { expr, list, .. } => {
            collect_aggregates(expr, out, factory);
            for item in list {
                collect_aggregates(item, out, factory);
            }
        }
        ExprKind::Between {
            expr, low, high, ..
        } => {
            collect_aggregates(expr, out, factory);
            collect_aggregates(low, out, factory);
            collect_aggregates(high, out, factory);
        }
        ExprKind::Like { expr, pattern, .. } => {
            collect_aggregates(expr, out, factory);
            collect_aggregates(pattern, out, factory);
        }
        ExprKind::IsTruthValue { expr: inner, .. } => {
            collect_aggregates(inner, out, factory);
        }
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
                collect_aggregates(arg, out, factory);
            }
            for expr in partition_by {
                collect_aggregates(expr, out, factory);
            }
            for sort_item in order_by {
                collect_aggregates(&sort_item.expr, out, factory);
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
    if !inside_agg
        && group_by
            .iter()
            .any(|gb| typed_expr_semantically_eq(expr, gb))
    {
        return;
    }

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
) -> Result<LogicalPlanNode, String> {
    match relation {
        Relation::Scan(scan) => {
            // G1: reuse the ColumnIds the analyzer already minted for this
            // table's columns (carried on `scan.column_ids`). Minting fresh
            // ids here would desync the analyzer-produced `ColumnRef`s in
            // the rest of the plan (Window PARTITION BY, GROUP BY, ORDER BY,
            // join eq keys, etc.) from the scan output, and distribution
            // matching would fail.
            let base_len = scan.table.columns.len();
            let mut columns: Vec<OutputColumn> = scan
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
            for (meta_idx, c) in scan
                .table
                .iceberg_row_lineage_metadata_columns
                .iter()
                .enumerate()
            {
                let col_id_idx = base_len + meta_idx;
                columns.push(OutputColumn {
                    column_id: scan.column_ids.get(col_id_idx).copied().unwrap_or_else(|| {
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
                });
            }
            Ok(LogicalPlanNode::new(
                LogicalPlanKind::Scan(LogicalScanNode {
                    database: scan.database,
                    table: scan.table,
                    alias: scan.alias,
                    columns: columns,
                    predicates: vec![],
                    required_columns: None,
                    variant_columns: vec![],
                    mv_rewritten_from: None,
                }),
                vec![],
                None,
            ))
        }
        Relation::Subquery {
            query,
            alias,
            output_columns,
        } => {
            let inner_plan = plan_scoped_query(*query, cte_registry, factory)?;
            adapt_plan_output_with_qualifier(inner_plan, &output_columns, Some(&alias))
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
                    Ok(LogicalPlanNode::new(
                        LogicalPlanKind::TableFunction(LogicalTableFunctionNode {
                            function_name: "unnest".to_string(),
                            args: unnest.args,
                            output_columns: unnest.output_columns,
                            alias: unnest.alias,
                            is_left_join: is_left_join,
                        }),
                        vec![left],
                        None,
                    ))
                }
                right => {
                    let left = plan_relation_scoped(left, cte_registry, factory)?;
                    let right = plan_relation_scoped(right, cte_registry, factory)?;
                    Ok(LogicalPlanNode::new(
                        LogicalPlanKind::Join(LogicalJoinNode {
                            join_type: join_type,
                            condition: condition,
                        }),
                        vec![left, right],
                        None,
                    ))
                }
            }
        }
        Relation::GenerateSeries(gs) => Ok(LogicalPlanNode::new(
            LogicalPlanKind::GenerateSeries(LogicalGenerateSeriesNode {
                start: gs.start,
                end: gs.end,
                step: gs.step,
                column_name: gs.column_name,
                alias: gs.alias,
                output_column_id: gs.output_column_id,
            }),
            vec![],
            None,
        )),
        Relation::Unnest(_) => Err("UNNEST is currently supported only in LATERAL JOIN".into()),
        Relation::CTEConsume {
            cte_id,
            alias,
            output_columns,
            producer_column_ids,
        } => Ok(LogicalPlanNode::new(
            LogicalPlanKind::CTEConsume(LogicalCTEConsumeNode {
                cte_id: cte_id,
                alias: alias,
                output_columns: output_columns,
                producer_column_ids: producer_column_ids,
            }),
            vec![],
            None,
        )),
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
/// `LogicalPlanKind::Scan` whose `TableDef` carries the synthetic
/// `ScanSource::IcebergMetadataTable` source. The optimizer treats it
/// like any other Scan; codegen branches on the source variant to emit
/// an `HDFS_SCAN_NODE` whose lowering wires up the native-Rust
/// `IcebergMetadataScanOp` (no JVM / JNI bridge — the embedded-Java
/// path was removed in favor of iceberg-rust).
fn plan_iceberg_metadata_scan(
    rel: IcebergMetadataScanRelation,
    factory: &mut ColumnRefFactory,
) -> Result<LogicalPlanNode, String> {
    use crate::sql::analyzer::iceberg_metadata::metadata_table_schema_for_source;
    use crate::sql::catalog::{ColumnDef, ScanSource, TableDef};

    let cols =
        metadata_table_schema_for_source(rel.metadata_table_type.clone(), &rel.table.source)?;
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
    Ok(LogicalPlanNode::new(
        LogicalPlanKind::Scan(LogicalScanNode {
            database: rel.database,
            table: synthetic_table,
            alias: rel.alias,
            columns: output_columns,
            predicates: vec![],
            required_columns: None,
            variant_columns: vec![],
            mv_rewritten_from: None,
        }),
        vec![],
        None,
    ))
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
        IcebergMetadataTableType::Files
        | IcebergMetadataTableType::Manifests
        | IcebergMetadataTableType::LogicalIcebergMetadata => {
            let table_info = iceberg_table_info(storage).ok_or_else(|| {
                "iceberg files/manifests/entries metadata table requires iceberg table identity"
                    .to_string()
            })?;
            table_info
                .serialized_metadata_rows
                .clone()
                .map(Some)
                .ok_or_else(|| {
                    "iceberg metadata rows were not resolved at catalog lookup time".to_string()
                })
        }
        IcebergMetadataTableType::Snapshots
        | IcebergMetadataTableType::History
        | IcebergMetadataTableType::Refs => Ok(None),
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
/// `LogicalPlanKind::Scan` whose `TableDef` carries the synthetic
/// `ScanSource::IcebergDeltaTable` storage. Codegen recognizes this
/// storage variant and emits `TPlanNodeType::ICEBERG_DELTA_SCAN_NODE`
/// (rather than `HDFS_SCAN_NODE`). Refresh/codegen expands the storage
/// variant into a typed explicit payload; lower only consumes that payload.
fn plan_iceberg_delta_scan(
    rel: IcebergDeltaScanRelation,
    factory: &mut ColumnRefFactory,
) -> Result<LogicalPlanNode, String> {
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
    Ok(LogicalPlanNode::new(
        LogicalPlanKind::Scan(LogicalScanNode {
            database: rel.namespace,
            table: synthetic_table,
            alias: rel.alias,
            columns: output_columns,
            predicates: vec![],
            required_columns: None,
            variant_columns: vec![],
            mv_rewritten_from: None,
        }),
        vec![],
        None,
    ))
}

fn iceberg_table_info(
    source: &crate::sql::catalog::ScanSource,
) -> Option<&crate::sql::catalog::IcebergTableInfo> {
    match source {
        crate::sql::catalog::ScanSource::IcebergDataFiles { table, .. }
        | crate::sql::catalog::ScanSource::IcebergMetadataTable { table, .. }
        | crate::sql::catalog::ScanSource::IcebergDeltaTable { table, .. }
        | crate::sql::catalog::ScanSource::IcebergVersionTable { table, .. } => Some(table),
        crate::sql::catalog::ScanSource::StarRocks { .. }
        | crate::sql::catalog::ScanSource::IcebergMvTargetState { .. }
        | crate::sql::catalog::ScanSource::IcebergMvTargetLocator { .. } => None,
    }
}

// ---------------------------------------------------------------------------
// Set operation planning
// ---------------------------------------------------------------------------

fn plan_set_operation_scoped(
    set_op: ResolvedSetOp,
    cte_registry: &CTERegistry,
    factory: &mut ColumnRefFactory,
) -> Result<LogicalPlanNode, String> {
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
            let dt = crate::types::wider_type(&lc.data_type, &rc.data_type);
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
        SetOpKind::Union => Ok(LogicalPlanNode::new(
            LogicalPlanKind::Union(LogicalUnionNode {
                all: set_op.all,
                output_columns: output_columns,
            }),
            vec![left, right],
            None,
        )),
        SetOpKind::Intersect => Ok(LogicalPlanNode::new(
            LogicalPlanKind::Intersect(LogicalIntersectNode {
                output_columns: output_columns,
            }),
            vec![left, right],
            None,
        )),
        SetOpKind::Except => Ok(LogicalPlanNode::new(
            LogicalPlanKind::Except(LogicalExceptNode {
                output_columns: output_columns,
            }),
            vec![left, right],
            None,
        )),
    }
}

// ---------------------------------------------------------------------------
// VALUES planning
// ---------------------------------------------------------------------------

fn plan_values(
    values: ResolvedValues,
    _factory: &mut ColumnRefFactory,
) -> Result<LogicalPlanNode, String> {
    let columns = values.output_columns;
    Ok(LogicalPlanNode::new(
        LogicalPlanKind::Values(LogicalValuesNode {
            rows: values.rows,
            columns: columns,
        }),
        vec![],
        None,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::catalog::{CatalogProvider, ColumnDef, ScanSource, TableDef};
    use crate::sql::planner::plan::*;

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
                "t" => Ok(TableDef {
                    name: "t".to_string(),
                    columns: vec![
                        ColumnDef {
                            name: "a".to_string(),
                            data_type: arrow::datatypes::DataType::Int64,
                            nullable: false,
                            write_default: None,
                            logical_type: None,
                        },
                        ColumnDef {
                            name: "b".to_string(),
                            data_type: arrow::datatypes::DataType::Int64,
                            nullable: true,
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
                "iv_orders" => Ok(TableDef {
                    name: "iv_orders".to_string(),
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
                    iceberg_row_lineage_metadata_columns: vec![
                        ColumnDef {
                            name: "_row_id".to_string(),
                            data_type: arrow::datatypes::DataType::Int64,
                            nullable: false,
                            write_default: None,
                            logical_type: None,
                        },
                        ColumnDef {
                            name: "_last_updated_sequence_number".to_string(),
                            data_type: arrow::datatypes::DataType::Int64,
                            nullable: false,
                            write_default: None,
                            logical_type: None,
                        },
                    ],
                    source: ScanSource::StarRocks {
                        db_id: 0,
                        table_id: 0,
                    },
                }),
                "t1" | "t2" => {
                    let value_col = if table == "t1" { "v1" } else { "v2" };
                    Ok(TableDef {
                        name: table.to_string(),
                        columns: vec![
                            ColumnDef {
                                name: "k1".to_string(),
                                data_type: arrow::datatypes::DataType::Int64,
                                nullable: true,
                                write_default: None,
                                logical_type: None,
                            },
                            ColumnDef {
                                name: "k2".to_string(),
                                data_type: arrow::datatypes::DataType::Int64,
                                nullable: true,
                                write_default: None,
                                logical_type: None,
                            },
                            ColumnDef {
                                name: value_col.to_string(),
                                data_type: arrow::datatypes::DataType::Utf8,
                                nullable: true,
                                write_default: None,
                                logical_type: None,
                            },
                        ],
                        iceberg_row_lineage_metadata_columns: vec![],
                        source: ScanSource::StarRocks {
                            db_id: 0,
                            table_id: 0,
                        },
                    })
                }
                other => Err(format!("unknown test table: {other}")),
            }
        }
    }

    fn parse_analyze_and_plan(sql: &str) -> Result<LogicalPlanNode, String> {
        let (resolved, cte_registry, mut factory) = parse_analyze_query(sql)?;
        plan_query(resolved, cte_registry, &mut factory)
    }

    fn parse_analyze_query(
        sql: &str,
    ) -> Result<(ResolvedQuery, CTERegistry, ColumnRefFactory), String> {
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
        crate::sql::analyzer::analyze(&query, &TestCatalog, "default")
    }

    fn parse_analyze_query_apply(
        sql: &str,
    ) -> Result<(ResolvedQuery, CTERegistry, ColumnRefFactory), String> {
        parse_analyze_query(sql)
    }

    /// Analyze and plan `sql` with the Apply subquery framework.
    fn parse_analyze_and_plan_apply(sql: &str) -> Result<LogicalPlanNode, String> {
        let (resolved, cte_registry, mut factory) = parse_analyze_query_apply(sql)?;
        plan_query(resolved, cte_registry, &mut factory)
    }

    fn plan_test_query(sql: &str) -> LogicalPlanNode {
        parse_analyze_and_plan(sql).expect("planner should succeed")
    }

    fn first_aggregate_calls(plan: &LogicalPlanNode) -> Vec<AggregateCall> {
        fn visit(plan: &LogicalPlanNode) -> Option<Vec<AggregateCall>> {
            match &plan.kind {
                LogicalPlanKind::Aggregate(node) => Some(node.aggregates.clone()),
                _ => plan.children.iter().find_map(visit),
            }
        }

        visit(plan).unwrap_or_default()
    }

    fn first_aggregate_node(plan: &LogicalPlanNode) -> Option<&LogicalAggregateNode> {
        match &plan.kind {
            LogicalPlanKind::Aggregate(node) => Some(node),
            _ => plan.children.iter().find_map(first_aggregate_node),
        }
    }

    fn root_project_over_aggregate(
        plan: &LogicalPlanNode,
    ) -> (&LogicalProjectNode, &LogicalAggregateNode) {
        let LogicalPlanKind::Project(project) = &plan.kind else {
            panic!("expected Project root, got {plan:?}");
        };
        let LogicalPlanKind::Aggregate(aggregate) = &plan.unary_input().kind else {
            panic!(
                "expected Aggregate under Project, got {:?}",
                plan.unary_input()
            );
        };
        (project, aggregate)
    }

    fn root_project_filter_aggregate(
        plan: &LogicalPlanNode,
    ) -> (
        &LogicalProjectNode,
        &LogicalFilterNode,
        &LogicalAggregateNode,
    ) {
        let LogicalPlanKind::Project(project) = &plan.kind else {
            panic!("expected Project root, got {plan:?}");
        };
        let filter_plan = plan.unary_input();
        let LogicalPlanKind::Filter(filter) = &filter_plan.kind else {
            panic!("expected Filter under Project, got {:?}", filter_plan);
        };
        let LogicalPlanKind::Aggregate(aggregate) = &filter_plan.unary_input().kind else {
            panic!(
                "expected Aggregate under Filter, got {:?}",
                filter_plan.unary_input()
            );
        };
        (project, filter, aggregate)
    }

    fn first_repeat_node(plan: &LogicalPlanNode) -> (&LogicalPlanNode, &LogicalRepeatNode) {
        fn visit(plan: &LogicalPlanNode) -> Option<(&LogicalPlanNode, &LogicalRepeatNode)> {
            match &plan.kind {
                LogicalPlanKind::Repeat(node) => Some((plan, node)),
                _ => plan.children.iter().find_map(visit),
            }
        }

        visit(plan).unwrap_or_else(|| panic!("missing Repeat node in {plan:?}"))
    }

    #[test]
    fn planner_deduplicates_repeated_group_by_but_keeps_repeated_projection_outputs() {
        let plan = parse_analyze_and_plan(
            "SELECT o_orderkey, o_orderkey, count(DISTINCT o_orderkey) \
             FROM orders GROUP BY o_orderkey, o_orderkey",
        )
        .expect("planner should succeed");
        let (project, aggregate) = root_project_over_aggregate(&plan);

        assert_eq!(aggregate.group_by.len(), 1);
        assert_eq!(
            aggregate
                .output_columns
                .iter()
                .map(|column| column.column_id)
                .collect::<std::collections::HashSet<_>>()
                .len(),
            aggregate.output_columns.len()
        );
        assert_eq!(project.items.len(), 3);
        assert_ne!(
            project.items[0].output_column_id,
            project.items[1].output_column_id
        );
        assert_eq!(
            column_ref_id(&project.items[0].expr),
            aggregate.output_columns[0].column_id
        );
        assert_eq!(
            column_ref_id(&project.items[1].expr),
            aggregate.output_columns[0].column_id
        );
    }

    fn column_ref_id(expr: &TypedExpr) -> ColumnId {
        let ExprKind::ColumnRef { column_id, .. } = &expr.kind else {
            panic!("expected ColumnRef, got {:?}", expr.kind);
        };
        *column_id
    }

    #[test]
    fn planner_group_by_targets_ignore_aggregate_public_output_order() {
        fn col(id: u32, name: &str) -> TypedExpr {
            TypedExpr {
                kind: ExprKind::ColumnRef {
                    column_id: ColumnId(id),
                    qualifier: None,
                    column: name.to_string(),
                },
                data_type: DataType::Int64,
                nullable: false,
            }
        }

        fn output(id: u32, name: &str) -> OutputColumn {
            OutputColumn {
                column_id: ColumnId(id),
                name: name.to_string(),
                data_type: DataType::Int64,
                nullable: false,
                is_internal: false,
            }
        }

        let aggregate = LogicalAggregateNode {
            group_by: vec![col(1, "k"), col(2, "region")],
            aggregates: vec![AggregateCall {
                name: "sum".to_string(),
                args: Vec::new(),
                distinct: false,
                result_type: DataType::Int64,
                order_by: Vec::new(),
                output_column_id: ColumnId(30),
            }],
            output_columns: vec![output(30, "sum(v)"), output(1, "k"), output(2, "region")],
            already_pushed: false,
        };

        let targets = planner_aggregate_group_by_targets(&aggregate);

        assert_eq!(
            targets
                .iter()
                .map(|target| target.column_id)
                .collect::<Vec<_>>(),
            vec![ColumnId(1), ColumnId(2)]
        );
    }

    fn root_project_over_window(
        plan: &LogicalPlanNode,
    ) -> (&LogicalProjectNode, &LogicalWindowNode) {
        let LogicalPlanKind::Project(project) = &plan.kind else {
            panic!("expected Project root, got {plan:?}");
        };
        let LogicalPlanKind::Window(window) = &plan.unary_input().kind else {
            panic!(
                "expected Window under Project, got {:?}",
                plan.unary_input()
            );
        };
        (project, window)
    }

    fn root_strip_sort_inner_project(
        plan: &LogicalPlanNode,
    ) -> (&LogicalProjectNode, &LogicalSortNode, &LogicalProjectNode) {
        let LogicalPlanKind::Project(outer_proj) = &plan.kind else {
            panic!("expected outer strip Project, got {plan:?}");
        };
        let sort_plan = plan.unary_input();
        let LogicalPlanKind::Sort(sort) = &sort_plan.kind else {
            panic!("expected Sort under outer Project, got {:?}", sort_plan);
        };
        let LogicalPlanKind::Project(inner_proj) = &sort_plan.unary_input().kind else {
            panic!(
                "expected inner Project under Sort, got {:?}",
                sort_plan.unary_input()
            );
        };
        (outer_proj, sort, inner_proj)
    }

    fn binary_left_column_id(expr: &TypedExpr) -> ColumnId {
        let ExprKind::BinaryOp { left, .. } = &expr.kind else {
            panic!("expected BinaryOp, got {:?}", expr.kind);
        };
        let ExprKind::ColumnRef { column_id, .. } = &left.kind else {
            panic!(
                "expected BinaryOp left side to be ColumnRef, got {:?}",
                left.kind
            );
        };
        *column_id
    }

    fn first_window_exprs(plan: &LogicalPlanNode) -> Vec<WindowExpr> {
        fn visit(plan: &LogicalPlanNode) -> Option<Vec<WindowExpr>> {
            match &plan.kind {
                LogicalPlanKind::Window(node) => Some(node.window_exprs.clone()),
                _ => plan.children.iter().find_map(visit),
            }
        }

        visit(plan).unwrap_or_default()
    }

    fn first_window_output_columns(plan: &LogicalPlanNode) -> Vec<OutputColumn> {
        fn visit(plan: &LogicalPlanNode) -> Option<Vec<OutputColumn>> {
            match &plan.kind {
                LogicalPlanKind::Window(node) => Some(node.output_columns.clone()),
                _ => plan.children.iter().find_map(visit),
            }
        }

        visit(plan).unwrap_or_default()
    }

    fn assert_window_expr_ids_are_real_unique_and_backed_by_output_columns(plan: &LogicalPlanNode) {
        let wins = first_window_exprs(plan);
        let output_columns = first_window_output_columns(plan);
        assert!(!wins.is_empty(), "expected at least one WindowExpr");

        let output_ids = output_columns
            .iter()
            .map(|col| col.column_id)
            .collect::<std::collections::HashSet<_>>();
        let mut window_ids = std::collections::HashSet::new();
        for w in &wins {
            assert_ne!(
                w.output_column_id,
                crate::sql::column_id::ColumnId::UNSET,
                "WindowExpr {} must carry a real output_column_id",
                w.output_name
            );
            assert!(
                window_ids.insert(w.output_column_id),
                "WindowExpr {} reuses output_column_id {}",
                w.output_name,
                w.output_column_id
            );
            assert!(
                output_ids.contains(&w.output_column_id),
                "WindowExpr {} output_column_id {} missing from LogicalWindowNode.output_columns",
                w.output_name,
                w.output_column_id
            );
        }
    }

    fn window_expr_by_function_name<'a>(wins: &'a [WindowExpr], name: &str) -> &'a WindowExpr {
        wins.iter()
            .find(|w| w.name.eq_ignore_ascii_case(name))
            .unwrap_or_else(|| panic!("missing WindowExpr function {name}"))
    }

    fn visible_output_column_by_name<'a>(
        output_columns: &'a [OutputColumn],
        name: &str,
    ) -> &'a OutputColumn {
        output_columns
            .iter()
            .find(|col| !col.is_internal && col.name == name)
            .unwrap_or_else(|| panic!("missing visible Window output column {name}"))
    }

    fn strip_project_sort_limit(plan: &LogicalPlanNode) -> &LogicalPlanNode {
        match &plan.kind {
            LogicalPlanKind::Project(_) | LogicalPlanKind::Sort(_) | LogicalPlanKind::Limit(_) => {
                strip_project_sort_limit(plan.unary_input())
            }
            _ => plan,
        }
    }

    fn unwrap_project_input(plan: &LogicalPlanNode) -> &LogicalPlanNode {
        // Peel any chain of Project adapters to reach the underlying logical
        // node. Besides the outer identity adapter, a subquery alias is now
        // represented as a Project carrying `output_qualifier` (added by the
        // predicate-pushdown work), so more than one Project layer may sit
        // above the set-op.
        let mut current = plan;
        while let LogicalPlanKind::Project(_) = &current.kind {
            current = current.unary_input();
        }
        current
    }

    fn contains_identity_project_adapter(
        plan: &LogicalPlanNode,
        source_column: &str,
        output_name: &str,
    ) -> bool {
        match &plan.kind {
            LogicalPlanKind::Project(project) => {
                project.items.iter().any(|item| {
                    item.output_name == output_name
                        && matches!(
                            &item.expr.kind,
                            ExprKind::ColumnRef { column_id, column, .. }
                                if column == source_column && item.output_column_id == *column_id
                        )
                }) || plan.children.iter().any(|child| {
                    contains_identity_project_adapter(child, source_column, output_name)
                })
            }
            _ => plan
                .children
                .iter()
                .any(|child| contains_identity_project_adapter(child, source_column, output_name)),
        }
    }

    #[test]
    fn adapt_plan_output_passthrough_when_outputs_match() {
        let source_id = ColumnId::new_for_test(10);
        let input = LogicalPlanNode::new(
            LogicalPlanKind::Values(LogicalValuesNode {
                rows: vec![],
                columns: vec![OutputColumn {
                    column_id: source_id,
                    name: "k".to_string(),
                    data_type: arrow::datatypes::DataType::Int64,
                    nullable: false,
                    is_internal: false,
                }],
            }),
            vec![],
            None,
        );
        let target = vec![OutputColumn {
            column_id: source_id,
            name: "k".to_string(),
            data_type: arrow::datatypes::DataType::Int64,
            nullable: false,
            is_internal: false,
        }];

        let adapted = adapt_plan_output(input, &target).expect("adapter should succeed");
        assert!(matches!(&adapted.kind, LogicalPlanKind::Values(_)));
    }

    #[test]
    fn adapt_plan_output_renames_and_rebinds_with_project() {
        let source_id = ColumnId::new_for_test(10);
        let target_id = ColumnId::new_for_test(20);
        let input = LogicalPlanNode::new(
            LogicalPlanKind::Values(LogicalValuesNode {
                rows: vec![],
                columns: vec![OutputColumn {
                    column_id: source_id,
                    name: "k".to_string(),
                    data_type: arrow::datatypes::DataType::Int64,
                    nullable: false,
                    is_internal: false,
                }],
            }),
            vec![],
            None,
        );
        let target = vec![OutputColumn {
            column_id: target_id,
            name: "alias_k".to_string(),
            data_type: arrow::datatypes::DataType::Int64,
            nullable: false,
            is_internal: false,
        }];

        let adapted = adapt_plan_output(input, &target).expect("adapter should succeed");
        let LogicalPlanKind::Project(project) = &adapted.kind else {
            panic!("expected Project adapter");
        };
        assert_eq!(project.items.len(), 1);
        assert_eq!(project.items[0].output_name, "alias_k");
        assert_eq!(project.items[0].output_column_id, target_id);
        let ExprKind::ColumnRef {
            column_id, column, ..
        } = &project.items[0].expr.kind
        else {
            panic!("expected adapter item to read child column");
        };
        assert_eq!(*column_id, source_id);
        assert_eq!(column, "k");
    }

    #[test]
    fn adapt_plan_output_with_qualifier_preserves_cte_alias_lookup() {
        let source_id = ColumnId::new_for_test(10);
        let target_id = ColumnId::new_for_test(20);
        let input = LogicalPlanNode::new(
            LogicalPlanKind::Values(LogicalValuesNode {
                rows: vec![],
                columns: vec![OutputColumn {
                    column_id: source_id,
                    name: "k1".to_string(),
                    data_type: arrow::datatypes::DataType::Int64,
                    nullable: false,
                    is_internal: false,
                }],
            }),
            vec![],
            None,
        );
        let target = vec![OutputColumn {
            column_id: target_id,
            name: "k1".to_string(),
            data_type: arrow::datatypes::DataType::Int64,
            nullable: false,
            is_internal: false,
        }];

        let adapted = adapt_plan_output_with_qualifier(input, &target, Some("w1"))
            .expect("adapter should succeed");
        let LogicalPlanKind::Project(project) = &adapted.kind else {
            panic!("expected Project adapter");
        };
        assert_eq!(project.items[0].output_column_id, target_id);
        assert_eq!(project.output_qualifier.as_deref(), Some("w1"));
        let ExprKind::ColumnRef {
            column_id,
            qualifier,
            column,
        } = &project.items[0].expr.kind
        else {
            panic!("expected adapter item to read child column");
        };
        assert_eq!(*column_id, source_id);
        assert_eq!(qualifier.as_deref(), None);
        assert_eq!(column, "k1");
    }

    #[test]
    fn adapt_plan_output_with_qualifier_inserts_project_when_outputs_match() {
        let source_id = ColumnId::new_for_test(10);
        let input = LogicalPlanNode::new(
            LogicalPlanKind::Values(LogicalValuesNode {
                rows: vec![],
                columns: vec![OutputColumn {
                    column_id: source_id,
                    name: "rnk".to_string(),
                    data_type: arrow::datatypes::DataType::Int64,
                    nullable: false,
                    is_internal: false,
                }],
            }),
            vec![],
            None,
        );
        let target = vec![OutputColumn {
            column_id: source_id,
            name: "rnk".to_string(),
            data_type: arrow::datatypes::DataType::Int64,
            nullable: false,
            is_internal: false,
        }];

        let adapted = adapt_plan_output_with_qualifier(input, &target, Some("asceding"))
            .expect("adapter should insert alias project");
        let LogicalPlanKind::Project(project) = &adapted.kind else {
            panic!("expected Project adapter for qualified subquery output");
        };
        assert_eq!(project.items[0].output_name, "rnk");
        assert_eq!(project.output_qualifier.as_deref(), Some("asceding"));
        let ExprKind::ColumnRef {
            column_id,
            qualifier,
            column,
        } = &project.items[0].expr.kind
        else {
            panic!("expected adapter item to read child column");
        };
        assert_eq!(*column_id, source_id);
        assert_eq!(qualifier.as_deref(), None);
        assert_eq!(column, "rnk");
    }

    #[test]
    fn adapt_plan_output_allows_nullable_widening() {
        let source_id = ColumnId::new_for_test(10);
        let target_id = ColumnId::new_for_test(20);
        let input = LogicalPlanNode::new(
            LogicalPlanKind::Values(LogicalValuesNode {
                rows: vec![],
                columns: vec![OutputColumn {
                    column_id: source_id,
                    name: "k".to_string(),
                    data_type: arrow::datatypes::DataType::Int64,
                    nullable: false,
                    is_internal: false,
                }],
            }),
            vec![],
            None,
        );
        let target = vec![OutputColumn {
            column_id: target_id,
            name: "nullable_k".to_string(),
            data_type: arrow::datatypes::DataType::Int64,
            nullable: true,
            is_internal: false,
        }];

        let adapted = adapt_plan_output(input, &target).expect("adapter should widen nullability");
        let LogicalPlanKind::Project(project) = &adapted.kind else {
            panic!("expected Project adapter");
        };
        assert_eq!(project.items.len(), 1);
        assert!(project.items[0].expr.nullable);
        assert_eq!(project.items[0].output_column_id, target_id);
    }

    #[test]
    fn adapt_plan_output_rejects_nullable_narrowing() {
        let input = LogicalPlanNode::new(
            LogicalPlanKind::Values(LogicalValuesNode {
                rows: vec![],
                columns: vec![OutputColumn {
                    column_id: ColumnId::new_for_test(10),
                    name: "k".to_string(),
                    data_type: arrow::datatypes::DataType::Int64,
                    nullable: true,
                    is_internal: false,
                }],
            }),
            vec![],
            None,
        );
        let target = vec![OutputColumn {
            column_id: ColumnId::new_for_test(20),
            name: "not_null_k".to_string(),
            data_type: arrow::datatypes::DataType::Int64,
            nullable: false,
            is_internal: false,
        }];

        let err = adapt_plan_output(input, &target)
            .expect_err("adapter should reject nullable narrowing");
        assert!(
            err.contains("output nullability mismatch"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn adapt_plan_output_rejects_shape_mismatch() {
        let input = LogicalPlanNode::new(
            LogicalPlanKind::Values(LogicalValuesNode {
                rows: vec![],
                columns: vec![],
            }),
            vec![],
            None,
        );
        let target = vec![OutputColumn {
            column_id: ColumnId::new_for_test(20),
            name: "alias_k".to_string(),
            data_type: arrow::datatypes::DataType::Int64,
            nullable: false,
            is_internal: false,
        }];

        let err =
            adapt_plan_output(input, &target).expect_err("adapter should reject arity mismatch");
        assert!(
            err.contains("output column count mismatch"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_plan_query_wraps_single_cte_in_anchor() {
        let plan = parse_analyze_and_plan(
            "WITH t AS (SELECT o_orderkey AS ok FROM orders) SELECT ok FROM t",
        )
        .expect("planner should succeed");

        match &plan.kind {
            LogicalPlanKind::CTEAnchor(anchor) => {
                assert_eq!(anchor.cte_id, 0);
                assert!(matches!(
                    &plan.child(0).kind,
                    LogicalPlanKind::CTEProduce(_)
                ));
            }
            other => panic!("expected CTEAnchor, got {other:?}"),
        }
    }

    #[test]
    fn p1_aggregate_call_gets_output_column_id() {
        let plan = plan_test_query("SELECT a, sum(b) AS s FROM t GROUP BY a");
        let aggs = first_aggregate_calls(&plan);
        assert!(!aggs.is_empty(), "expected at least one AggregateCall");
        for call in &aggs {
            assert_ne!(
                call.output_column_id,
                crate::sql::column_id::ColumnId::UNSET,
                "AggregateCall {} must carry a real output_column_id",
                call.name
            );
        }
    }

    #[test]
    fn p1_aggregate_call_ids_deduplicate_repeated_calls() {
        let plan = plan_test_query("SELECT sum(b) AS s1, count(b) AS c, sum(b) AS s2 FROM t");
        let aggs = first_aggregate_calls(&plan);
        assert_eq!(aggs.len(), 2, "expected repeated sum(b) to deduplicate");
        assert_eq!(
            aggs.iter()
                .filter(|call| call.name.eq_ignore_ascii_case("sum"))
                .count(),
            1,
            "expected exactly one sum(b) AggregateCall"
        );
        assert_eq!(
            aggs.iter()
                .filter(|call| call.name.eq_ignore_ascii_case("count"))
                .count(),
            1,
            "expected one count(b) AggregateCall"
        );

        let ids = aggs
            .iter()
            .map(|call| {
                assert_ne!(
                    call.output_column_id,
                    crate::sql::column_id::ColumnId::UNSET,
                    "AggregateCall {} must carry a real output_column_id",
                    call.name
                );
                call.output_column_id
            })
            .collect::<std::collections::HashSet<_>>();
        assert_eq!(
            ids.len(),
            aggs.len(),
            "distinct AggregateCalls must carry distinct output ids"
        );
    }

    #[test]
    fn p2_aggregate_projection_rewrites_agg_call_to_output_id_ref() {
        let plan = plan_test_query("SELECT sum(b) + 1 AS s1 FROM t");
        let (project, aggregate) = root_project_over_aggregate(&plan);
        let sum_id = aggregate
            .aggregates
            .iter()
            .find(|call| call.name.eq_ignore_ascii_case("sum"))
            .expect("expected sum AggregateCall")
            .output_column_id;
        assert_ne!(
            sum_id,
            ColumnId::UNSET,
            "AggregateCall must have a real output id"
        );

        let ExprKind::BinaryOp { left, .. } = &project.items[0].expr.kind else {
            panic!(
                "expected sum(b)+1 to remain a BinaryOp over the aggregate output, got {:?}",
                project.items[0].expr.kind
            );
        };
        let ExprKind::ColumnRef {
            column_id, column, ..
        } = &left.kind
        else {
            panic!(
                "aggregate child in sum(b)+1 must be rewritten to ColumnRef, got {:?}",
                left.kind
            );
        };
        assert_eq!(
            *column_id, sum_id,
            "project expression must reference the AggregateCall output id"
        );
        assert_eq!(
            column, "sum(b)",
            "project aggregate ColumnRef must preserve the display name for the P2 fallback"
        );
    }

    #[test]
    fn p2_computed_group_key_rewrites_to_group_output_id() {
        let plan = plan_test_query("SELECT a + 1 AS k, sum(b) AS s FROM t GROUP BY a + 1");
        let (project, aggregate) = root_project_over_aggregate(&plan);
        let group_output_id = aggregate
            .output_columns
            .iter()
            .find(|col| col.name == "k")
            .expect("expected aggregate output column for computed key")
            .column_id;
        assert_ne!(
            group_output_id,
            ColumnId::UNSET,
            "computed group output column must have a real id"
        );

        let ExprKind::ColumnRef {
            column_id, column, ..
        } = &project.items[0].expr.kind
        else {
            panic!(
                "computed group key projection must be rewritten to ColumnRef, got {:?}",
                project.items[0].expr.kind
            );
        };
        assert_eq!(
            *column_id, group_output_id,
            "computed group key projection must reference the Aggregate output id"
        );
        assert_eq!(
            column, "a + 1",
            "computed group key ColumnRef must preserve the group expression display name"
        );
    }

    #[test]
    fn p2_having_rewrites_agg_call_to_output_id_ref() {
        let plan = plan_test_query("SELECT sum(b) AS s FROM t HAVING sum(b) > 10");
        let (_project, filter, aggregate) = root_project_filter_aggregate(&plan);
        let sum_id = aggregate
            .aggregates
            .iter()
            .find(|call| call.name.eq_ignore_ascii_case("sum"))
            .expect("expected sum AggregateCall")
            .output_column_id;
        assert_ne!(
            sum_id,
            ColumnId::UNSET,
            "AggregateCall must have a real output id"
        );

        let ExprKind::BinaryOp { left, .. } = &filter.predicate.kind else {
            panic!(
                "expected HAVING sum(b)>10 to remain a BinaryOp over the aggregate output, got {:?}",
                filter.predicate.kind
            );
        };
        let ExprKind::ColumnRef {
            column_id, column, ..
        } = &left.kind
        else {
            panic!(
                "aggregate child in HAVING must be rewritten to ColumnRef, got {:?}",
                left.kind
            );
        };
        assert_eq!(
            *column_id, sum_id,
            "HAVING predicate must reference the AggregateCall output id"
        );
        assert_eq!(
            column, "sum(b)",
            "HAVING aggregate ColumnRef must preserve the display name for the P2 fallback"
        );
    }

    #[test]
    fn order_by_only_aggregates_are_added_to_aggregate_outputs() {
        let plan = plan_test_query(
            "SELECT min(a) AS v1 FROM t GROUP BY b ORDER BY round(count(a) / min(a)), min(a)",
        );
        let aggregate = first_aggregate_node(&plan).expect("expected Aggregate in plan");

        assert_eq!(
            aggregate.output_columns.len(),
            aggregate.group_by.len() + aggregate.aggregates.len(),
            "Aggregate output columns must include ORDER BY-only aggregate calls"
        );
        for call in &aggregate.aggregates {
            assert_ne!(
                call.output_column_id,
                ColumnId::UNSET,
                "AggregateCall {} must have a real output id",
                call.name
            );
            assert!(
                aggregate
                    .output_columns
                    .iter()
                    .any(|col| col.column_id == call.output_column_id),
                "Aggregate output columns must contain {} with id {}",
                call.name,
                call.output_column_id
            );
        }
    }

    #[test]
    fn p2_having_computed_group_key_does_not_append_leaf_group_by() {
        let plan = plan_test_query(
            "SELECT abs(a) AS k, sum(b) AS s FROM t GROUP BY abs(a) HAVING abs(a) > 1",
        );
        let (project, filter, aggregate) = root_project_filter_aggregate(&plan);
        assert_eq!(
            aggregate.group_by.len(),
            1,
            "HAVING group expression must not append its leaf column as an extra group key"
        );
        let group_output_id = aggregate
            .output_columns
            .iter()
            .find(|col| col.name == "k")
            .expect("expected aggregate output column for computed key")
            .column_id;
        assert_ne!(
            group_output_id,
            ColumnId::UNSET,
            "computed group output column must have a real id"
        );
        let ExprKind::ColumnRef {
            column_id, column, ..
        } = &project.items[0].expr.kind
        else {
            panic!(
                "computed group key projection must be rewritten to ColumnRef, got {:?}",
                project.items[0].expr.kind
            );
        };
        assert_eq!(
            *column_id, group_output_id,
            "computed group key projection must reference the Aggregate output id"
        );
        assert_eq!(
            column, "abs(a)",
            "computed group key ColumnRef must preserve the group expression display name"
        );

        let ExprKind::BinaryOp { left, .. } = &filter.predicate.kind else {
            panic!(
                "expected HAVING abs(a)>1 to remain a BinaryOp over the group key output, got {:?}",
                filter.predicate.kind
            );
        };
        let ExprKind::ColumnRef {
            column_id, column, ..
        } = &left.kind
        else {
            panic!(
                "computed group key in HAVING must be rewritten to ColumnRef, got {:?}",
                left.kind
            );
        };
        assert_eq!(
            *column_id, group_output_id,
            "HAVING computed group key must reference the Aggregate output id"
        );
        assert_eq!(
            column, "abs(a)",
            "HAVING computed group key ColumnRef must preserve the group expression display name"
        );
    }

    #[test]
    fn p2_repeat_grouping_aggregate_outputs_follow_group_by_order() {
        let plan = plan_test_query(
            "SELECT grouping(a + 1) AS g, a + 1 AS k, count(*) AS cnt \
             FROM t GROUP BY ROLLUP(a + 1)",
        );
        let (project, aggregate) = root_project_over_aggregate(&plan);
        assert_eq!(
            aggregate.group_by.len(),
            2,
            "ROLLUP with GROUPING() should group by repeat key and grouping marker"
        );
        let group_ids = aggregate
            .group_by
            .iter()
            .map(column_ref_id)
            .collect::<Vec<_>>();
        let output_prefix_ids = aggregate
            .output_columns
            .iter()
            .take(aggregate.group_by.len())
            .map(|col| col.column_id)
            .collect::<Vec<_>>();
        assert_eq!(
            output_prefix_ids, group_ids,
            "Aggregate output_columns prefix must match group_by physical output order"
        );

        let g_id = column_ref_id(&project.items[0].expr);
        let k_id = column_ref_id(&project.items[1].expr);
        assert_eq!(
            g_id, group_ids[1],
            "GROUPING() projection must bind to the grouping marker output"
        );
        assert_eq!(
            k_id, group_ids[0],
            "rollup key projection must bind to the repeat key output"
        );
    }

    #[test]
    fn p3_cube_without_grouping_survives_optimizer_id_binding() {
        let sql = "WITH t AS ( \
                   SELECT 1 AS a, 'x' AS b \
                   UNION ALL SELECT 1, 'y' \
                   UNION ALL SELECT 2, 'z' \
                   ) \
                   SELECT a, b FROM t GROUP BY CUBE(a, b) ORDER BY a, b";
        let (resolved, cte_registry, mut factory) =
            parse_analyze_query(sql).expect("analyzer should succeed");
        let logical =
            plan_query(resolved, cte_registry, &mut factory).expect("planner should succeed");
        let mut scalar_arena = crate::sql::optimizer::scalar::ScalarArena::new();
        let opt_expr = crate::sql::planner::optimizer_bridge::plan::try_logical_plan_to_opt_expr(
            &logical,
            &mut scalar_arena,
        )
        .expect("logical to opt expr");
        let physical = crate::sql::optimizer::optimize_with_legacy_table_stats_for_migration(
            opt_expr,
            scalar_arena,
            &std::collections::HashMap::new(),
            factory,
            None,
            Vec::new(),
        )
        .expect("optimizer should produce a physical plan");

        crate::sql::planner::optimizer_bridge::id_binding::verify_optimizer_id_binding(&physical)
            .expect("CUBE synthetic grouping output must survive optimizer extraction");
    }

    #[test]
    fn p3_rollup_order_by_only_key_survives_optimizer_id_binding() {
        let sql = "SELECT array_agg(DISTINCT b ORDER BY b) \
                   FROM t GROUP BY ROLLUP(a) ORDER BY a";
        let (resolved, cte_registry, mut factory) =
            parse_analyze_query(sql).expect("analyzer should succeed");
        let logical =
            plan_query(resolved, cte_registry, &mut factory).expect("planner should succeed");
        let mut scalar_arena = crate::sql::optimizer::scalar::ScalarArena::new();
        let opt_expr = crate::sql::planner::optimizer_bridge::plan::try_logical_plan_to_opt_expr(
            &logical,
            &mut scalar_arena,
        )
        .expect("logical to opt expr");
        let physical = crate::sql::optimizer::optimize_with_legacy_table_stats_for_migration(
            opt_expr,
            scalar_arena,
            &std::collections::HashMap::new(),
            factory,
            None,
            Vec::new(),
        )
        .expect("optimizer should produce a physical plan");

        crate::sql::planner::optimizer_bridge::id_binding::verify_optimizer_id_binding(&physical)
            .expect("ROLLUP ORDER BY-only key must bind to aggregate repeat-key output");
    }

    #[test]
    fn p3_rollup_window_order_by_alias_extra_survives_optimizer_id_binding() {
        let sql = "SELECT sum(b) AS total_sum, \
                          a, \
                          b, \
                          grouping(a) + grouping(b) AS lochierarchy, \
                          rank() OVER ( \
                            PARTITION BY grouping(a) + grouping(b), \
                                         CASE WHEN grouping(b) = 0 THEN a END \
                            ORDER BY sum(b) DESC \
                          ) AS rank_within_parent \
                   FROM t \
                   GROUP BY ROLLUP(a, b) \
                   ORDER BY lochierarchy DESC, \
                            CASE WHEN lochierarchy = 0 THEN a END, \
                            rank_within_parent \
                   LIMIT 10";
        let (resolved, cte_registry, mut factory) =
            parse_analyze_query(sql).expect("analyzer should succeed");
        let logical =
            plan_query(resolved, cte_registry, &mut factory).expect("planner should succeed");
        let mut scalar_arena = crate::sql::optimizer::scalar::ScalarArena::new();
        let opt_expr = crate::sql::planner::optimizer_bridge::plan::try_logical_plan_to_opt_expr(
            &logical,
            &mut scalar_arena,
        )
        .expect("logical to opt expr");
        let physical = crate::sql::optimizer::optimize_with_legacy_table_stats_for_migration(
            opt_expr,
            scalar_arena,
            &std::collections::HashMap::new(),
            factory,
            None,
            Vec::new(),
        )
        .expect("optimizer should produce a physical plan");

        crate::sql::planner::optimizer_bridge::id_binding::verify_optimizer_id_binding(&physical)
            .expect("ROLLUP window ORDER BY alias extras must bind to child/window outputs");
    }

    #[test]
    fn p3_aggregate_order_by_alias_topn_survives_optimizer_id_binding() {
        let sql = "SELECT a, count(*) AS total_cnt \
                   FROM t \
                   GROUP BY a \
                   ORDER BY total_cnt DESC, a \
                   LIMIT 10";
        let (resolved, cte_registry, mut factory) =
            parse_analyze_query(sql).expect("analyzer should succeed");
        let logical =
            plan_query(resolved, cte_registry, &mut factory).expect("planner should succeed");
        let mut scalar_arena = crate::sql::optimizer::scalar::ScalarArena::new();
        let opt_expr = crate::sql::planner::optimizer_bridge::plan::try_logical_plan_to_opt_expr(
            &logical,
            &mut scalar_arena,
        )
        .expect("logical to opt expr");
        let physical = crate::sql::optimizer::optimize_with_legacy_table_stats_for_migration(
            opt_expr,
            scalar_arena,
            &std::collections::HashMap::new(),
            factory,
            None,
            Vec::new(),
        )
        .expect("optimizer should produce a physical plan");

        crate::sql::planner::optimizer_bridge::id_binding::verify_optimizer_id_binding(&physical)
            .expect("aggregate ORDER BY alias TopN must bind to aggregate project output");
    }

    #[test]
    fn p1_window_expr_gets_output_column_id() {
        let plan =
            plan_test_query("SELECT a, row_number() OVER (PARTITION BY a ORDER BY b) AS rn FROM t");
        assert_window_expr_ids_are_real_unique_and_backed_by_output_columns(&plan);
        let wins = first_window_exprs(&plan);
        let output_columns = first_window_output_columns(&plan);
        let rn = window_expr_by_function_name(&wins, "row_number");
        let visible_rn = visible_output_column_by_name(&output_columns, "rn");
        assert_eq!(
            rn.output_column_id, visible_rn.column_id,
            "single visible window projection must reuse the visible rn output id"
        );
    }

    #[test]
    fn p1_compound_window_exprs_get_distinct_output_column_ids() {
        let plan = plan_test_query(
            "SELECT row_number() OVER (ORDER BY a) + rank() OVER (ORDER BY b) AS x FROM t",
        );
        let wins = first_window_exprs(&plan);
        let output_columns = first_window_output_columns(&plan);
        assert_eq!(wins.len(), 2, "expected two extracted WindowExprs");
        assert_window_expr_ids_are_real_unique_and_backed_by_output_columns(&plan);
        for w in &wins {
            let output_column = output_columns
                .iter()
                .find(|col| col.column_id == w.output_column_id)
                .expect("window output id should be present");
            assert!(
                output_column.is_internal,
                "compound WindowExpr {} should use an internal output column",
                w.output_name
            );
        }
        let visible = plan_output_columns(&plan).expect("plan output should be known");
        assert_eq!(visible.len(), 1);
        assert_eq!(visible[0].name, "x");
        assert!(!visible[0].is_internal);
    }

    #[test]
    fn p1_multiple_projection_window_exprs_get_distinct_output_column_ids() {
        let plan = plan_test_query(
            "SELECT row_number() OVER (ORDER BY a) AS rn, rank() OVER (ORDER BY b) AS rk FROM t",
        );
        let wins = first_window_exprs(&plan);
        let output_columns = first_window_output_columns(&plan);
        assert_eq!(wins.len(), 2, "expected two extracted WindowExprs");
        assert_window_expr_ids_are_real_unique_and_backed_by_output_columns(&plan);
        let rn = window_expr_by_function_name(&wins, "row_number");
        let rk = window_expr_by_function_name(&wins, "rank");
        let visible_rn = visible_output_column_by_name(&output_columns, "rn");
        let visible_rk = visible_output_column_by_name(&output_columns, "rk");
        assert_eq!(
            rn.output_column_id, visible_rn.column_id,
            "single visible window projection must reuse the visible rn output id"
        );
        assert_eq!(
            rk.output_column_id, visible_rk.column_id,
            "single visible window projection must reuse the visible rk output id"
        );
    }

    #[test]
    fn p2_window_output_columns_preserve_passthrough_input_ids() {
        let plan =
            plan_test_query("SELECT a, row_number() OVER (PARTITION BY a ORDER BY b) AS rn FROM t");
        let (project, window) = root_project_over_window(&plan);
        let passthrough_id = column_ref_id(&project.items[0].expr);
        assert!(
            window
                .output_columns
                .iter()
                .any(|col| col.column_id == passthrough_id),
            "LogicalWindowNode output_columns must expose child passthrough ColumnIds"
        );
        let rn = window_expr_by_function_name(&window.window_exprs, "row_number");
        assert!(
            window
                .output_columns
                .iter()
                .any(|col| col.column_id == rn.output_column_id),
            "LogicalWindowNode output_columns must include window result ColumnIds"
        );
    }

    #[test]
    fn p2_window_call_rewrites_to_window_output_id() {
        let plan = plan_test_query(
            "SELECT row_number() OVER (PARTITION BY a ORDER BY b) + 1 AS rn1 FROM t",
        );
        let (project, window) = root_project_over_window(&plan);
        let rn = window_expr_by_function_name(&window.window_exprs, "row_number");
        assert_ne!(
            rn.output_column_id,
            ColumnId::UNSET,
            "WindowExpr must have a real output id"
        );

        let ExprKind::BinaryOp { left, .. } = &project.items[0].expr.kind else {
            panic!(
                "expected row_number()+1 to remain a BinaryOp over the window output, got {:?}",
                project.items[0].expr.kind
            );
        };
        let ExprKind::ColumnRef {
            column_id, column, ..
        } = &left.kind
        else {
            panic!(
                "window child in row_number()+1 must be rewritten to ColumnRef, got {:?}",
                left.kind
            );
        };
        assert_eq!(
            *column_id, rn.output_column_id,
            "project expression must reference the WindowExpr output id"
        );
        assert_eq!(
            column, "rn1",
            "window ColumnRef must preserve the P2 display name"
        );
    }

    #[test]
    fn test_plan_query_builds_nested_anchor_chain() {
        let plan = parse_analyze_and_plan(
            "WITH a AS (SELECT o_orderkey AS ok FROM orders), \
                  b AS (SELECT ok FROM a) \
             SELECT ok FROM b",
        )
        .expect("planner should succeed");

        match &plan.kind {
            LogicalPlanKind::CTEAnchor(anchor_a) => match &plan.child(1).kind {
                LogicalPlanKind::CTEAnchor(anchor_b) => {
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

        match &plan.kind {
            LogicalPlanKind::Project(_) => match &plan.unary_input().kind {
                LogicalPlanKind::Aggregate(agg) => {
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

        let LogicalPlanKind::Sort(sort) = &plan.kind else {
            panic!("expected Sort root");
        };
        let LogicalPlanKind::Project(project) = &plan.unary_input().kind else {
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
    fn derived_table_plans_without_alias_operator() {
        let plan =
            parse_analyze_and_plan("SELECT s.o_orderkey FROM (SELECT o_orderkey FROM orders) s")
                .expect("planner should succeed");

        let debug = format!("{plan:?}");
        assert!(
            !debug.contains("alias operator"),
            "derived table must not create alias operator: {debug}"
        );
    }

    #[test]
    fn derived_table_column_alias_uses_project_adapter() {
        let plan = parse_analyze_and_plan("SELECT s.ok FROM (SELECT o_orderkey FROM orders) s(ok)")
            .expect("planner should succeed");

        let debug = format!("{plan:?}");
        assert!(
            !debug.contains("alias operator"),
            "column alias derived table must not create alias operator: {debug}"
        );

        assert!(
            contains_identity_project_adapter(&plan, "o_orderkey", "ok"),
            "expected identity Project adapter to expose column alias ok: {plan:?}"
        );
    }

    #[test]
    fn test_nested_with_in_derived_table_stays_inside_subquery_scope() {
        let plan = parse_analyze_and_plan(
            "WITH outer_t AS (SELECT o_orderkey AS ok FROM orders) \
             SELECT ok FROM (WITH inner_t AS (SELECT o_custkey AS ok FROM orders) \
                             SELECT ok FROM inner_t) s",
        )
        .expect("planner should succeed");

        match &plan.kind {
            LogicalPlanKind::CTEAnchor(outer_anchor) => {
                assert_eq!(outer_anchor.cte_id, 0);
                let subquery_input = strip_project_sort_limit(plan.child(1));
                match &subquery_input.kind {
                    LogicalPlanKind::CTEAnchor(inner_anchor) => {
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

        match &plan.kind {
            LogicalPlanKind::CTEAnchor(outer_anchor) => {
                assert_eq!(outer_anchor.cte_id, 1);
                match &plan.child(0).kind {
                    LogicalPlanKind::CTEProduce(_) => match &plan.child(0).unary_input().kind {
                        LogicalPlanKind::CTEAnchor(inner_anchor) => {
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
        let outer_anchor_idx = lines
            .iter()
            .position(|line| line.contains("CTE_ANCHOR(cte_id=0)"))
            .expect("expected outer anchor line");
        let inner_anchor_idx = lines
            .iter()
            .position(|line| line.contains("CTE_ANCHOR(cte_id=1)"))
            .expect("expected nested inner anchor line");

        assert!(
            inner_anchor_idx > outer_anchor_idx,
            "nested inner anchor should remain inside derived-table subtree: {lines:?}"
        );
    }

    #[test]
    fn window_reuses_ordering_through_derived_table() {
        let plan = parse_analyze_and_plan(
            "SELECT sum(o_custkey) OVER \
                    (ORDER BY o_orderkey ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) \
                    AS running_sum \
             FROM (SELECT o_orderkey, o_custkey FROM orders ORDER BY o_orderkey) s",
        )
        .expect("planner should succeed");

        let lines =
            crate::sql::explain::explain_plan(&plan, crate::sql::explain::ExplainLevel::Verbose);
        let sort_count = lines
            .iter()
            .filter(|line| line.contains("SORT BY [o_orderkey ASC NULLS FIRST]"))
            .count();
        assert_eq!(
            sort_count, 1,
            "window should reuse the derived table ordering: {lines:?}"
        );
    }

    #[test]
    fn window_reuses_ordering_through_derived_table_column_alias_project() {
        let plan = parse_analyze_and_plan(
            "SELECT sum(ok) OVER \
                    (ORDER BY ok ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) \
                    AS running_sum \
             FROM (SELECT o_orderkey FROM orders ORDER BY o_orderkey) s(ok)",
        )
        .expect("planner should succeed");

        let lines =
            crate::sql::explain::explain_plan(&plan, crate::sql::explain::ExplainLevel::Verbose);
        let sort_lines = lines
            .iter()
            .filter(|line| line.contains("SORT BY ["))
            .collect::<Vec<_>>();
        assert_eq!(
            sort_lines.len(),
            1,
            "window should reuse derived table ordering through identity Project: {lines:?}"
        );
        assert!(
            sort_lines[0].contains("SORT BY [o_orderkey ASC NULLS FIRST]"),
            "expected the preserved derived-table ordering, got {sort_lines:?}"
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

        match &plan.kind {
            LogicalPlanKind::Union(_) => {
                assert_eq!(plan.children.len(), 2);
                match &plan.child(1).kind {
                    LogicalPlanKind::CTEAnchor(anchor) => assert_eq!(anchor.cte_id, 0),
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

    /// Regression test for the ColumnId-correctness bug where LogicalUnionNode.output_columns
    /// carried left-branch ColumnIds instead of the fresh set-op output ColumnIds.
    #[test]
    fn output_columns_carry_fresh_set_op_ids() {
        // Plan a derived table that wraps a UNION ALL. The Union node must carry
        // the analyzer-visible output IDs directly, without an alias wrapper.
        let plan = parse_analyze_and_plan(
            "SELECT o_orderkey, o_custkey \
             FROM (SELECT o_orderkey, o_custkey FROM orders \
                   UNION ALL \
                   SELECT o_orderkey, o_custkey FROM orders) sub",
        )
        .expect("planner should succeed");

        let debug = format!("{plan:?}");
        assert!(
            !debug.contains("alias operator"),
            "set-op derived table must not create alias operator: {debug}"
        );

        let union_node = match &unwrap_project_input(&plan).kind {
            LogicalPlanKind::Union(n) => n,
            other => panic!("expected Union below adapter, got {other:?}"),
        };
        let visible_columns = plan_output_columns(&plan).expect("plan output should be known");

        // Core assertion: fresh IDs must match position-by-position.
        assert_eq!(
            visible_columns.len(),
            union_node.output_columns.len(),
            "visible output and Union output_columns length must match"
        );
        for (i, (visible_col, union_col)) in visible_columns
            .iter()
            .zip(union_node.output_columns.iter())
            .enumerate()
        {
            assert_eq!(
                visible_col.column_id, union_col.column_id,
                "output_columns[{i}]: visible column_id {:?} != Union column_id {:?} \
                 (Union must carry the fresh set-op IDs, not left-branch IDs)",
                visible_col.column_id, union_col.column_id
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

            let debug = format!("{plan:?}");
            assert!(
                !debug.contains("alias operator"),
                "set-op derived table must not create alias operator: {debug}"
            );

            let visible_columns = plan_output_columns(&plan).expect("plan output should be known");
            let set_op_cols = match &unwrap_project_input(&plan).kind {
                LogicalPlanKind::Intersect(n) => &n.output_columns,
                LogicalPlanKind::Except(n) => &n.output_columns,
                other => panic!("expected Intersect/Except below adapter, got {other:?}"),
            };

            assert_eq!(visible_columns.len(), set_op_cols.len());
            for (i, (visible_col, set_op_col)) in
                visible_columns.iter().zip(set_op_cols.iter()).enumerate()
            {
                assert_eq!(
                    visible_col.column_id, set_op_col.column_id,
                    "output_columns[{i}]: visible {:?} != set-op {:?} for SQL: {sql}",
                    visible_col.column_id, set_op_col.column_id
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
        let (agg_group_by_cid, inner_proj_output_cid) = match &plan.kind {
            LogicalPlanKind::Aggregate(agg) => {
                let gb_cid = match &agg.group_by[0].kind {
                    ExprKind::ColumnRef { column_id, .. } => *column_id,
                    other => panic!("expected ColumnRef group_by, got {other:?}"),
                };
                let inner_proj = match &plan.unary_input().kind {
                    LogicalPlanKind::Project(p) => p,
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
        let outer_proj = match &plan.kind {
            LogicalPlanKind::Project(p) => p,
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

        let sort_plan = plan.unary_input();
        let inner_proj = match &sort_plan.kind {
            LogicalPlanKind::Sort(_) => match &sort_plan.unary_input().kind {
                LogicalPlanKind::Project(p) => p,
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

    #[test]
    fn sort_only_expression_extra_uses_traceable_output_column_id() {
        let plan = parse_analyze_and_plan(
            "SELECT o_orderkey FROM orders ORDER BY abs(o_custkey - o_orderkey)",
        )
        .expect("planner should succeed");

        let outer_proj = match &plan.kind {
            LogicalPlanKind::Project(p) => p,
            other => panic!("expected outer strip Project, got {other:?}"),
        };
        let sort_plan = plan.unary_input();
        let sort = match &sort_plan.kind {
            LogicalPlanKind::Sort(s) => s,
            other => panic!("expected Sort under outer Project, got {other:?}"),
        };
        let inner_proj = match &sort_plan.unary_input().kind {
            LogicalPlanKind::Project(p) => p,
            other => panic!("expected inner Project under Sort, got {other:?}"),
        };

        let extra_item = inner_proj
            .items
            .iter()
            .find(|item| item.output_name.starts_with("abs("))
            .expect("expected sort-only expression item");
        assert_ne!(
            extra_item.output_column_id,
            ColumnId::UNSET,
            "sort-only expression extra must have a real output ColumnId so pruning can track it"
        );

        let ExprKind::ColumnRef {
            column_id: sort_key_id,
            ..
        } = sort.items[0].expr.kind
        else {
            panic!("sort key should be rewritten to a ColumnRef");
        };
        assert_eq!(
            sort_key_id, extra_item.output_column_id,
            "sort key must reference the sort-only expression extra by ColumnId"
        );
    }

    #[test]
    fn order_by_computed_select_alias_reuses_project_output_column_id() {
        let plan =
            parse_analyze_and_plan("SELECT o_orderkey * 2 AS revenue FROM orders ORDER BY revenue")
                .expect("planner should succeed");

        let sort = match &plan.kind {
            LogicalPlanKind::Sort(s) => s,
            other => panic!("expected Sort root, got {other:?}"),
        };
        let project = match &plan.unary_input().kind {
            LogicalPlanKind::Project(p) => p,
            other => panic!("expected Project under Sort, got {other:?}"),
        };
        let project_output_id = project.items[0].output_column_id;
        let ExprKind::ColumnRef {
            column_id: sort_key_id,
            ..
        } = sort.items[0].expr.kind
        else {
            panic!("sort key should be a ColumnRef to the select alias");
        };

        assert_ne!(
            project_output_id,
            ColumnId::UNSET,
            "computed select alias must have a real output ColumnId"
        );
        assert_eq!(
            sort_key_id, project_output_id,
            "ORDER BY select alias must point at the Project output ColumnId"
        );
    }

    #[test]
    fn p2_order_by_select_alias_extra_path_preserves_inner_output_id() {
        let plan =
            parse_analyze_and_plan("SELECT o_orderkey AS x FROM orders ORDER BY x, o_custkey")
                .expect("planner should succeed");

        let outer_proj = match &plan.kind {
            LogicalPlanKind::Project(p) => p,
            other => panic!("expected outer strip Project, got {other:?}"),
        };
        let sort_plan = plan.unary_input();
        let sort = match &sort_plan.kind {
            LogicalPlanKind::Sort(s) => s,
            other => panic!("expected Sort under outer Project, got {other:?}"),
        };
        let inner_proj = match &sort_plan.unary_input().kind {
            LogicalPlanKind::Project(p) => p,
            other => panic!("expected inner Project under Sort, got {other:?}"),
        };

        let inner_output_id = inner_proj.items[0].output_column_id;
        assert_ne!(
            inner_output_id,
            ColumnId::UNSET,
            "inner select alias output must have a real ColumnId"
        );
        let ExprKind::ColumnRef {
            column_id, column, ..
        } = &sort.items[0].expr.kind
        else {
            panic!("sort key should be a ColumnRef to the remapped select alias");
        };
        assert_eq!(
            column, "__nr_sel_0",
            "ORDER BY alias should remap to the synthetic inner Project label"
        );
        assert_eq!(
            *column_id, inner_output_id,
            "ORDER BY alias remap must preserve the inner Project output ColumnId"
        );
    }

    #[test]
    fn order_by_positions_preserve_duplicate_output_column_ids() {
        let plan = parse_analyze_and_plan(
            "SELECT l.a, r.a FROM t l FULL JOIN t r ON l.a != r.a ORDER BY 1, 2",
        )
        .expect("planner should succeed");

        let sort = match &plan.kind {
            LogicalPlanKind::Sort(sort) => sort,
            other => panic!("expected Sort root, got {other:?}"),
        };
        let project = match &plan.unary_input().kind {
            LogicalPlanKind::Project(project) => project,
            other => panic!("expected Project under Sort, got {other:?}"),
        };
        assert_eq!(project.items.len(), 2);
        assert_eq!(project.items[0].output_name, "a");
        assert_eq!(project.items[1].output_name, "a");

        let first_output_id = project.items[0].output_column_id;
        let second_output_id = project.items[1].output_column_id;
        assert_ne!(first_output_id, ColumnId::UNSET);
        assert_ne!(second_output_id, ColumnId::UNSET);
        assert_ne!(first_output_id, second_output_id);

        let sort_ids = sort
            .items
            .iter()
            .map(|item| match &item.expr.kind {
                ExprKind::ColumnRef { column_id, .. } => *column_id,
                other => panic!("expected sort key ColumnRef, got {other:?}"),
            })
            .collect::<Vec<_>>();

        assert_eq!(sort_ids, vec![first_output_id, second_output_id]);
    }

    #[test]
    fn p2_order_by_derived_values_extra_preserves_source_column_id() {
        let plan =
            parse_analyze_and_plan("SELECT 1 FROM (VALUES (1, 2)) AS v(a, b) ORDER BY v.b + 1")
                .expect("planner should succeed");

        let (_, _, inner_proj) = root_strip_sort_inner_project(&plan);
        let inner_proj_plan = plan.unary_input().unary_input();
        let child_output_columns = plan_output_columns(inner_proj_plan.unary_input())
            .expect("VALUES child output should be known");
        assert_eq!(
            child_output_columns.len(),
            2,
            "derived VALUES child should expose both columns"
        );
        let b_output_id = child_output_columns[1].column_id;
        assert_ne!(
            b_output_id,
            ColumnId::UNSET,
            "derived VALUES b output must have a real ColumnId"
        );

        let extra_item = inner_proj
            .items
            .last()
            .expect("sort-extra ProjectItem should be appended");
        assert_eq!(
            binary_left_column_id(&extra_item.expr),
            b_output_id,
            "ORDER BY v.b + 1 extra must reference the derived VALUES source ColumnId"
        );
    }

    #[test]
    fn p2_order_by_generate_series_extra_preserves_source_column_id() {
        let plan = parse_analyze_and_plan(
            "SELECT 1 FROM TABLE(generate_series(1, 3, 1)) AS gs(x) ORDER BY gs.x + 1",
        )
        .expect("planner should succeed");

        let (_, _, inner_proj) = root_strip_sort_inner_project(&plan);
        let inner_proj_plan = plan.unary_input().unary_input();
        let child_output_columns = plan_output_columns(inner_proj_plan.unary_input())
            .expect("GenerateSeries child output should be known");
        assert_eq!(
            child_output_columns.len(),
            1,
            "GenerateSeries child should expose one column"
        );
        let source_output_id = child_output_columns[0].column_id;
        assert_ne!(
            source_output_id,
            ColumnId::UNSET,
            "GenerateSeries output must have a real ColumnId"
        );

        let extra_item = inner_proj
            .items
            .last()
            .expect("sort-extra ProjectItem should be appended");
        assert_eq!(
            binary_left_column_id(&extra_item.expr),
            source_output_id,
            "ORDER BY gs.x + 1 extra must reference the GenerateSeries source ColumnId"
        );
    }

    #[test]
    fn p2_values_output_uses_single_column_id() {
        let (resolved, cte_registry, mut factory) =
            parse_analyze_query("VALUES (1, 2), (3, 4)").expect("analyzer should succeed");
        let analyzer_output_columns = resolved.output_columns.clone();
        let plan =
            plan_query(resolved, cte_registry, &mut factory).expect("planner should succeed");
        let LogicalPlanKind::Values(values) = &plan.kind else {
            panic!("expected Values root");
        };
        assert_eq!(
            values.columns.len(),
            analyzer_output_columns.len(),
            "LogicalValuesNode should expose the analyzer output columns"
        );
        for (value_column, analyzer_column) in
            values.columns.iter().zip(analyzer_output_columns.iter())
        {
            assert_ne!(
                value_column.column_id,
                ColumnId::UNSET,
                "VALUES output column must have a real ColumnId"
            );
            assert_eq!(
                value_column.column_id, analyzer_column.column_id,
                "LogicalValuesNode column id must reuse the analyzer query output id"
            );
        }
    }

    #[test]
    fn p2_generate_series_output_has_column_id_through_planner() {
        let plan = parse_analyze_and_plan("SELECT x FROM TABLE(generate_series(1, 3, 1)) AS gs(x)")
            .expect("planner should succeed");
        let LogicalPlanKind::Project(project) = &plan.kind else {
            panic!("expected Project root, got {plan:?}");
        };
        let child_output_columns = plan_output_columns(plan.unary_input())
            .expect("generate_series output should be known");
        assert_eq!(
            child_output_columns.len(),
            1,
            "generate_series should expose one output column"
        );
        let child_output_id = child_output_columns[0].column_id;
        assert_ne!(
            child_output_id,
            ColumnId::UNSET,
            "GenerateSeries output must have a real ColumnId"
        );
        let ExprKind::ColumnRef { column_id, .. } = project.items[0].expr.kind else {
            panic!("project over generate_series should read a ColumnRef");
        };
        assert_eq!(
            column_id, project.items[0].output_column_id,
            "Project item should preserve the generate_series ColumnRef id"
        );
        assert_eq!(
            column_id, child_output_id,
            "GenerateSeries child output id must match the parent Project ColumnRef"
        );
    }

    #[test]
    fn p2_base_scan_row_lineage_metadata_preserves_column_id_through_planner() {
        let plan = parse_analyze_and_plan("SELECT _row_id FROM iv_orders AS t")
            .expect("planner should succeed");
        let LogicalPlanKind::Project(project) = &plan.kind else {
            panic!("expected Project root, got {plan:?}");
        };
        let LogicalPlanKind::Scan(scan) = &plan.unary_input().kind else {
            panic!("expected Scan under Project, got {:?}", plan.unary_input());
        };

        let row_id_output = scan
            .columns
            .iter()
            .find(|col| col.name == "_row_id")
            .expect("LogicalScanNode must expose _row_id metadata output");
        assert_ne!(row_id_output.column_id, ColumnId::UNSET);

        let ExprKind::ColumnRef { column_id, .. } = project.items[0].expr.kind else {
            panic!("Project over _row_id should read a ColumnRef");
        };
        assert_eq!(
            column_id, row_id_output.column_id,
            "Project must read the _row_id ColumnId exposed by the LogicalScanNode"
        );
        assert_eq!(
            project.items[0].output_column_id, row_id_output.column_id,
            "visible _row_id output should preserve the LogicalScanNode metadata ColumnId"
        );
    }

    #[test]
    fn p2_rollup_materialized_key_has_real_id() {
        let plan = parse_analyze_and_plan("SELECT a + 1 AS k FROM t GROUP BY ROLLUP(a + 1)")
            .expect("planner should succeed");
        let (_project, aggregate) = root_project_over_aggregate(&plan);
        let (repeat_plan, repeat) = first_repeat_node(&plan);
        let LogicalPlanKind::Project(repeat_input_project) = &repeat_plan.unary_input().kind else {
            panic!(
                "expected Repeat input Project, got {:?}",
                repeat_plan.unary_input()
            );
        };
        let repeat_key = repeat_input_project
            .items
            .iter()
            .find(|item| item.output_name == "__repeat_group_key_0")
            .expect("computed rollup key should be materialized before Repeat");
        assert_ne!(
            repeat_key.output_column_id,
            ColumnId::UNSET,
            "computed ROLLUP key materialization must have a real ColumnId"
        );
        assert_eq!(
            column_ref_id(&aggregate.group_by[0]),
            repeat_key.output_column_id,
            "Aggregate over Repeat must group by the materialized key ColumnId"
        );
    }

    #[test]
    fn p2_rollup_column_key_uses_distinct_repeat_materialization_id() {
        let plan = parse_analyze_and_plan("SELECT a, count(*) FROM t GROUP BY ROLLUP(a)")
            .expect("planner should succeed");
        let (_project, aggregate) = root_project_over_aggregate(&plan);
        let (repeat_plan, repeat) = first_repeat_node(&plan);
        let LogicalPlanKind::Project(repeat_input_project) = &repeat_plan.unary_input().kind else {
            panic!(
                "expected Repeat input Project, got {:?}",
                repeat_plan.unary_input()
            );
        };
        let source_a = repeat_input_project
            .items
            .iter()
            .find(|item| item.output_name == "a")
            .expect("Repeat input should preserve original source column");
        let repeat_key = repeat_input_project
            .items
            .iter()
            .find(|item| item.output_name == "__repeat_group_key_0")
            .expect("ROLLUP key should be materialized before Repeat");

        assert_ne!(
            source_a.output_column_id, repeat_key.output_column_id,
            "Repeat grouping key must not reuse the source ColumnId"
        );
        assert_eq!(
            repeat.all_rollup_column_ids,
            vec![repeat_key.output_column_id],
            "Repeat metadata must point at the materialized key id"
        );
        assert_eq!(
            column_ref_id(&aggregate.group_by[0]),
            repeat_key.output_column_id,
            "Aggregate over Repeat must group by the nullified materialized key"
        );
    }

    #[test]
    fn p2_repeat_output_columns_include_grouping_function_slots() {
        let plan = parse_analyze_and_plan(
            "SELECT grouping(a) AS g, a, count(*) FROM t GROUP BY ROLLUP(a)",
        )
        .expect("planner should succeed");
        let (repeat_plan, repeat) = first_repeat_node(&plan);
        let grouping_id = repeat
            .grouping_fn_ids
            .iter()
            .find(|(name, _)| name == "__grouping_fn_0")
            .map(|(_, id)| *id)
            .expect("ROLLUP should produce grouping function metadata");

        let repeat_outputs = plan_output_columns(repeat_plan).expect("Repeat output columns");

        assert!(
            repeat_outputs
                .iter()
                .any(|column| column.column_id == grouping_id && column.name == "__grouping_fn_0"),
            "Repeat output columns must expose generated GROUPING() ColumnId"
        );
    }

    #[test]
    fn p2_subquery_alias_reexposes_producing_id() {
        let plan = parse_analyze_and_plan("SELECT x FROM (SELECT a AS x FROM t) s WHERE x > 1")
            .expect("planner should succeed");
        let LogicalPlanKind::Project(project) = &plan.kind else {
            panic!("expected Project root, got {plan:?}");
        };
        let filter_plan = plan.unary_input();
        let LogicalPlanKind::Filter(filter) = &filter_plan.kind else {
            panic!("expected Filter under Project, got {:?}", filter_plan);
        };
        let child_output =
            plan_output_columns(filter_plan.unary_input()).expect("child output columns");
        let producing_id = child_output
            .iter()
            .find(|col| col.name == "x")
            .expect("subquery child should expose x")
            .column_id;
        assert_ne!(
            producing_id,
            ColumnId::UNSET,
            "subquery alias producer must expose a real ColumnId"
        );
        let ExprKind::BinaryOp { left, .. } = &filter.predicate.kind else {
            panic!(
                "expected WHERE x > 1 binary predicate, got {:?}",
                filter.predicate.kind
            );
        };
        assert_eq!(
            column_ref_id(left),
            producing_id,
            "outer WHERE x must reuse the subquery producer ColumnId"
        );
        assert_eq!(
            column_ref_id(&project.items[0].expr),
            producing_id,
            "outer SELECT x must reuse the subquery producer ColumnId"
        );
    }

    #[test]
    fn p2_full_outer_using_order_by_uses_project_output_id() {
        let plan = parse_analyze_and_plan(
            "SELECT a AS merged FROM t l FULL OUTER JOIN t r USING(a) ORDER BY merged",
        )
        .expect("planner should succeed");
        let LogicalPlanKind::Sort(sort) = &plan.kind else {
            panic!("expected Sort root, got {plan:?}");
        };
        let LogicalPlanKind::Project(project) = &plan.unary_input().kind else {
            panic!("expected Project under Sort, got {:?}", plan.unary_input());
        };
        let merged_output_id = project.items[0].output_column_id;
        assert_ne!(
            merged_output_id,
            ColumnId::UNSET,
            "FULL OUTER USING merged projection must have a real output ColumnId"
        );
        assert_eq!(
            column_ref_id(&sort.items[0].expr),
            merged_output_id,
            "ORDER BY merged must reference the FULL OUTER USING project output ColumnId"
        );
    }

    #[test]
    fn qualified_order_by_selected_column_does_not_create_sort_extra() {
        let plan = parse_analyze_and_plan(
            "SELECT s.o_orderkey FROM (SELECT o_orderkey FROM orders) s ORDER BY s.o_orderkey",
        )
        .expect("planner should succeed");

        let sort = match &plan.kind {
            LogicalPlanKind::Sort(s) => s,
            other => {
                panic!("qualified ORDER BY selected column must not add strip Project: {other:?}")
            }
        };
        let project = match &plan.unary_input().kind {
            LogicalPlanKind::Project(p) => p,
            other => panic!("expected SELECT Project under Sort, got {other:?}"),
        };
        assert_eq!(
            project.items.len(),
            1,
            "selected ORDER BY column must not be appended as a sort-only extra"
        );
        let ExprKind::ColumnRef {
            column_id: sort_key_id,
            ..
        } = sort.items[0].expr.kind
        else {
            panic!("sort key should remain a ColumnRef");
        };
        assert_eq!(
            sort_key_id, project.items[0].output_column_id,
            "sort key should reference the selected output column by ColumnId"
        );
    }

    #[test]
    fn apply_output_columns_extend_left_with_output_column() {
        use std::collections::HashSet;

        use arrow::datatypes::DataType;

        use crate::sql::analysis::{ExprKind, OutputColumn, TypedExpr};
        use crate::sql::column_id::ColumnId;
        use crate::sql::planner::plan::{
            ApplyKind, LogicalApplyNode, LogicalPlanKind, LogicalValuesNode,
        };

        let left_col = OutputColumn {
            column_id: ColumnId(11),
            name: "l1".to_string(),
            data_type: DataType::Int64,
            nullable: false,
            is_internal: false,
        };
        let out_col = OutputColumn {
            column_id: ColumnId(12),
            name: "__sq_1".to_string(),
            data_type: DataType::Int64,
            nullable: true,
            is_internal: true,
        };
        let plan = LogicalPlanNode::new(
            LogicalPlanKind::Apply(LogicalApplyNode {
                kind: ApplyKind::Scalar,
                subquery_expr: TypedExpr {
                    kind: ExprKind::ColumnRef {
                        column_id: ColumnId(12),
                        qualifier: None,
                        column: "__sq_1".to_string(),
                    },
                    data_type: DataType::Int64,
                    nullable: true,
                },
                output_column: out_col.clone(),
                inner_output_column_id: out_col.column_id,
                correlation_column_ids: vec![],
                correlation_conjuncts: vec![],
                residual_predicate: None,
                need_check_max_rows: true,
                use_semi_anti: false,
                uncorrelated_outer_predicate_columns: HashSet::new(),
            }),
            vec![
                LogicalPlanNode::new(
                    LogicalPlanKind::Values(LogicalValuesNode {
                        rows: vec![],
                        columns: vec![left_col.clone()],
                    }),
                    vec![],
                    None,
                ),
                LogicalPlanNode::new(
                    LogicalPlanKind::Values(LogicalValuesNode {
                        rows: vec![],
                        columns: vec![],
                    }),
                    vec![],
                    None,
                ),
            ],
            None,
        );

        let columns = plan_output_columns(&plan).expect("apply output columns");
        assert_eq!(columns.len(), 2);
        assert_eq!(columns[0].column_id, left_col.column_id);
        assert_eq!(columns[1].column_id, out_col.column_id);
    }

    #[test]
    fn assert_one_row_output_columns_pass_through() {
        use arrow::datatypes::DataType;

        use crate::sql::analysis::OutputColumn;
        use crate::sql::column_id::ColumnId;
        use crate::sql::planner::plan::{
            LogicalAssertOneRowNode, LogicalPlanKind, LogicalValuesNode,
        };

        let col = OutputColumn {
            column_id: ColumnId(21),
            name: "c1".to_string(),
            data_type: DataType::Int64,
            nullable: false,
            is_internal: false,
        };
        let plan = LogicalPlanNode::new(
            LogicalPlanKind::AssertOneRow(LogicalAssertOneRowNode::global_at_most_one("select 1")),
            vec![LogicalPlanNode::new(
                LogicalPlanKind::Values(LogicalValuesNode {
                    rows: vec![],
                    columns: vec![col.clone()],
                }),
                vec![],
                None,
            )],
            None,
        );

        let columns = plan_output_columns(&plan).expect("assert output columns");
        assert_eq!(columns.len(), 1);
        assert_eq!(columns[0].column_id, col.column_id);
    }

    // -------------------------------------------------------------------
    // Apply spec placement tests (Task 4)
    // -------------------------------------------------------------------

    /// Recursive helper: returns true if `e` (or any sub-expression) contains a
    /// `ColumnRef` with the given `ColumnId`.  Used by placement tests to verify
    /// that filter/projection predicates reference the Apply output column.
    fn expr_references_col(e: &TypedExpr, id: ColumnId) -> bool {
        match &e.kind {
            ExprKind::ColumnRef { column_id, .. } => *column_id == id,
            ExprKind::BinaryOp { left, right, .. } => {
                expr_references_col(left, id) || expr_references_col(right, id)
            }
            ExprKind::UnaryOp { expr, .. } => expr_references_col(expr, id),
            ExprKind::FunctionCall { args, .. } | ExprKind::AggregateCall { args, .. } => {
                args.iter().any(|a| expr_references_col(a, id))
            }
            ExprKind::Cast { expr, .. } => expr_references_col(expr, id),
            _ => false,
        }
    }

    /// WHERE-clause scalar subquery in Apply framework: the plan must contain an
    /// Apply node between the FROM Scan and the WHERE Filter, and the Apply's
    /// output_column must appear in the plan's output column set.
    #[test]
    fn apply_where_spec_emits_apply_below_where_filter() {
        // t1.k1 = (SELECT max(k2) FROM t2 WHERE t2.k1 = t1.k1)
        let sql = "SELECT k1 FROM t1 WHERE k1 = (SELECT max(k2) FROM t2 WHERE t2.k1 = t1.k1)";
        let plan = parse_analyze_and_plan_apply(sql).expect("Apply framework plan must succeed");

        // Root shape: Project → Filter(WHERE) → Apply → Scan
        let LogicalPlanKind::Project(project) = &plan.kind else {
            panic!("expected Project root, got {plan:?}");
        };
        let filter_plan = plan.unary_input();
        let LogicalPlanKind::Filter(filter) = &filter_plan.kind else {
            panic!("expected Filter under Project, got {:?}", filter_plan);
        };
        // The Apply must be directly under the WHERE Filter.
        let apply_plan = filter_plan.unary_input();
        let LogicalPlanKind::Apply(apply) = &apply_plan.kind else {
            panic!("expected Apply under WHERE Filter, got {:?}", apply_plan);
        };
        assert_eq!(
            apply.kind,
            crate::sql::planner::plan::ApplyKind::Scalar,
            "Apply kind must be Scalar"
        );
        // Apply.left must be the FROM Scan.
        assert!(
            matches!(&apply_plan.left().kind, LogicalPlanKind::Scan(_)),
            "Apply.left must be the FROM Scan, got {:?}",
            apply_plan.left()
        );
        // The WHERE Filter's predicate must reference the Apply output column
        // so that the filter can consume the scalar value.
        let apply_col_id = apply.output_column.column_id;
        assert!(
            expr_references_col(&filter.predicate, apply_col_id),
            "WHERE predicate must reference the Apply output column {:?}",
            apply_col_id
        );
    }

    /// HAVING-clause scalar subquery in Apply framework: the Apply must appear
    /// between the Aggregate and the HAVING Filter.
    #[test]
    fn apply_having_spec_emits_apply_above_aggregate() {
        let sql = "SELECT k1, max(k2) FROM t1 GROUP BY k1 \
                   HAVING max(k2) > (SELECT max(k2) FROM t2 WHERE t2.k1 = t1.k1)";
        let plan = parse_analyze_and_plan_apply(sql)
            .expect("Apply framework plan must succeed for HAVING");

        // Walk down: Project → Filter(HAVING) → Apply → Aggregate → ...
        let LogicalPlanKind::Project(project) = &plan.kind else {
            panic!("expected Project root, got {plan:?}");
        };
        let having_filter_plan = plan.unary_input();
        let LogicalPlanKind::Filter(having_filter) = &having_filter_plan.kind else {
            panic!(
                "expected HAVING Filter under Project, got {:?}",
                having_filter_plan
            );
        };
        let apply_plan = having_filter_plan.unary_input();
        let LogicalPlanKind::Apply(apply) = &apply_plan.kind else {
            panic!(
                "expected Apply directly under HAVING Filter, got {:?}",
                apply_plan
            );
        };
        assert_eq!(apply.kind, crate::sql::planner::plan::ApplyKind::Scalar);
        // Apply.left must be the Aggregate.
        assert!(
            matches!(&apply_plan.left().kind, LogicalPlanKind::Aggregate(_)),
            "Apply.left for HAVING spec must be the Aggregate, got {:?}",
            apply_plan.left()
        );
        // The HAVING Filter's predicate must reference the Apply output column.
        let apply_col_id = apply.output_column.column_id;
        assert!(
            expr_references_col(&having_filter.predicate, apply_col_id),
            "HAVING predicate must reference Apply output column {:?}",
            apply_col_id
        );
    }

    /// Projection-clause scalar subquery in Apply framework: the Apply must appear
    /// below the Project node (Project is above Apply).
    #[test]
    fn apply_projection_spec_emits_apply_below_project() {
        let sql = "SELECT k1, (SELECT max(k2) FROM t2 WHERE t2.k1 = t1.k1) AS sub FROM t1";
        let plan = parse_analyze_and_plan_apply(sql)
            .expect("Apply framework plan must succeed for Projection");

        // Root must be Project; its input must be Apply.
        let LogicalPlanKind::Project(project) = &plan.kind else {
            panic!("expected Project root, got {plan:?}");
        };
        let apply_plan = plan.unary_input();
        let LogicalPlanKind::Apply(apply) = &apply_plan.kind else {
            panic!(
                "expected Apply directly under Project, got {:?}",
                apply_plan
            );
        };
        assert_eq!(apply.kind, crate::sql::planner::plan::ApplyKind::Scalar);
        // Apply.left must be the FROM Scan.
        assert!(
            matches!(&apply_plan.left().kind, LogicalPlanKind::Scan(_)),
            "Apply.left for Projection spec must be FROM Scan, got {:?}",
            apply_plan.left()
        );
        // The Apply's output_column must appear in the Project's items.
        let apply_col_id = apply.output_column.column_id;
        let projected = project.items.iter().any(|item| {
            matches!(
                &item.expr.kind,
                ExprKind::ColumnRef { column_id, .. } if *column_id == apply_col_id
            )
        });
        assert!(
            projected,
            "Projection must reference the Apply output column"
        );
    }

    fn plan_with_single_predicate_apply_spec(
        sql: &str,
    ) -> (LogicalPlanNode, crate::sql::analysis::ApplyPredicateSpec) {
        use crate::sql::analysis::QueryBody;

        let (resolved, cte_registry, mut factory) =
            parse_analyze_query_apply(sql).expect("Apply framework analyze must succeed");
        let QueryBody::Select(select) = &resolved.body else {
            panic!("expected SELECT body");
        };
        assert_eq!(
            select.predicate_apply_specs.len(),
            1,
            "test query must record exactly one predicate apply spec"
        );
        let spec = select.predicate_apply_specs[0].clone();
        let plan = plan_query(resolved, cte_registry, &mut factory)
            .expect("planner must consume predicate apply spec");
        (plan, spec)
    }

    fn direct_where_apply(plan: &LogicalPlanNode) -> &LogicalApplyNode {
        let LogicalPlanKind::Project(_) = &plan.kind else {
            panic!("expected Project root, got {plan:?}");
        };
        let project_input = plan.unary_input();
        match &project_input.kind {
            LogicalPlanKind::Filter(_) => {
                let apply_plan = project_input.unary_input();
                let LogicalPlanKind::Apply(apply) = &apply_plan.kind else {
                    panic!(
                        "expected Apply directly below WHERE Filter, got {:?}",
                        apply_plan
                    );
                };
                apply
            }
            LogicalPlanKind::Apply(apply) => apply,
            other => panic!("expected Filter->Apply or Apply below Project, got {other:?}"),
        }
    }

    fn assert_same_column_ref_expr(actual: &TypedExpr, expected: &TypedExpr) {
        assert_eq!(actual.data_type, expected.data_type);
        assert_eq!(actual.nullable, expected.nullable);
        let ExprKind::ColumnRef {
            column_id: actual_id,
            qualifier: actual_qualifier,
            column: actual_column,
        } = &actual.kind
        else {
            panic!("actual expression must be a ColumnRef, got {actual:?}");
        };
        let ExprKind::ColumnRef {
            column_id: expected_id,
            qualifier: expected_qualifier,
            column: expected_column,
        } = &expected.kind
        else {
            panic!("expected expression must be a ColumnRef, got {expected:?}");
        };
        assert_eq!(actual_id, expected_id);
        assert_eq!(actual_qualifier, expected_qualifier);
        assert_eq!(actual_column, expected_column);
    }

    #[test]
    fn plan_exists_builds_apply_exists() {
        let sql = "SELECT k1 FROM t1 WHERE k1 > 0 \
                   AND EXISTS (SELECT 1 FROM t2 WHERE t2.k1 = t1.k1)";
        let (plan, spec) = plan_with_single_predicate_apply_spec(sql);
        assert!(
            !spec.correlation_column_ids.is_empty(),
            "test query must record a correlated EXISTS predicate spec"
        );

        let LogicalPlanKind::Project(project) = &plan.kind else {
            panic!("expected Project root, got {plan:?}");
        };
        let filter_plan = plan.unary_input();
        let LogicalPlanKind::Filter(filter) = &filter_plan.kind else {
            panic!(
                "expected residual WHERE Filter under Project, got {:?}",
                filter_plan
            );
        };
        let apply_plan = filter_plan.unary_input();
        let LogicalPlanKind::Apply(apply) = &apply_plan.kind else {
            panic!(
                "expected Apply directly below WHERE Filter, got {:?}",
                apply_plan
            );
        };
        assert_eq!(
            apply.kind,
            crate::sql::planner::plan::ApplyKind::Exists { negated: false }
        );
        assert_eq!(apply.correlation_column_ids, spec.correlation_column_ids);
        assert!(apply.use_semi_anti);
        assert!(!apply.need_check_max_rows);
        assert!(apply.correlation_conjuncts.is_empty());
    }

    #[test]
    fn plan_not_in_builds_apply_in_negated() {
        let sql = "SELECT k1 FROM t1 WHERE t1.k1 NOT IN (SELECT t2.k2 FROM t2)";
        let (plan, spec) = plan_with_single_predicate_apply_spec(sql);
        assert!(
            spec.correlation_column_ids.is_empty(),
            "test query must record an uncorrelated NOT IN predicate spec"
        );
        let apply = direct_where_apply(&plan);

        assert_eq!(
            apply.kind,
            crate::sql::planner::plan::ApplyKind::In { negated: true }
        );
        assert!(apply.correlation_column_ids.is_empty());
        let expected_lhs = spec
            .in_lhs
            .expect("IN predicate apply spec must carry analyzed LHS");
        assert_same_column_ref_expr(&apply.subquery_expr, &expected_lhs);
    }

    #[test]
    fn plan_exists_subquery_expr_is_boolean_colref() {
        let sql = "SELECT k1 FROM t1 WHERE EXISTS (SELECT 1 FROM t2 WHERE t2.k1 = t1.k1)";
        let (plan, spec) = plan_with_single_predicate_apply_spec(sql);
        let apply = direct_where_apply(&plan);

        assert_eq!(
            apply.subquery_expr.data_type,
            arrow::datatypes::DataType::Boolean
        );
        assert_eq!(
            apply.subquery_expr.nullable, spec.output_column.nullable,
            "EXISTS subquery_expr must mirror the Boolean predicate output nullability"
        );
        let ExprKind::ColumnRef { column_id, .. } = apply.subquery_expr.kind else {
            panic!(
                "EXISTS subquery_expr must be a Boolean ColumnRef, got {:?}",
                apply.subquery_expr
            );
        };
        assert_eq!(
            column_id, spec.output_column.column_id,
            "EXISTS subquery_expr must reference the predicate output column"
        );
    }
}
