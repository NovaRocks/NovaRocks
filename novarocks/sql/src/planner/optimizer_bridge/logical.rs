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

//! Conversion between `LogicalPlanNode` and `OptExpr`.

use crate::analysis::{OutputColumn, SortItem};
use crate::optimizer::operator::{
    AggregateOutputLayout, ApplyOp, AssertOneRowOp, CTEAnchorOp, CTEConsumeOp, CTEProduceOp,
    ExceptOp, FilterOp, GenerateSeriesOp, ImvDeltaOp, ImvVersionOp, IntersectOp, LimitOp,
    LogicalAggregateOp, LogicalJoinOp, Operator, ProjectOp, RepeatOp, ScalarAggregateSpec, ScanOp,
    SortOp, TableFunctionOp, UnionOp, ValuesOp, WindowOp,
};
use crate::optimizer::opt_expr::OptExpr;
use crate::optimizer::scalar::ScalarArena;
use crate::planner::logical::{
    LogicalAggregateNode, LogicalApplyNode, LogicalExceptNode, LogicalImvDeltaNode,
    LogicalImvVersionNode, LogicalIntersectNode, LogicalJoinNode, LogicalPlanKind, LogicalPlanNode,
    LogicalUnionNode,
};
use crate::planner::optimizer_bridge::scalar::{
    intern_aggregate_calls, intern_exprs, intern_project_items, intern_sort_items, intern_typed,
    intern_window_exprs, materialize, materialize_aggregate_calls, materialize_exprs,
    materialize_project_items, materialize_sort_keys, materialize_window_exprs,
};
use crate::planner::payload::{
    PlanAssertOneRowNode, PlanCTEAnchorNode, PlanCTEConsumeNode, PlanCTEProduceNode,
    PlanFilterNode, PlanGenerateSeriesNode, PlanLimitNode, PlanProjectNode, PlanRepeatNode,
    PlanScanNode, PlanSortNode, PlanTableFunctionNode, PlanValuesNode, PlanWindowNode,
};

/// Bridge 1: convert a `LogicalPlanNode` tree into an `OptExpr` tree, interning
/// all scalars into the provided `ScalarArena`. No Memo groups are minted here.
pub(crate) fn try_to_optimizer_expr(
    plan: &LogicalPlanNode,
    scalars: &mut ScalarArena,
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<OptExpr, crate::compiler::SqlCompileError> {
    let mut work = novarocks_type_contract::CompileCheckpoints::try_new(
        control,
        novarocks_type_contract::CompilePhase::LowerProgram,
    )?;
    let expression = to_optimizer_expr_unchecked(plan, scalars, &mut work)?;
    work.finish()?;
    Ok(expression)
}

#[cfg(test)]
pub(crate) fn to_optimizer_expr(plan: &LogicalPlanNode, scalars: &mut ScalarArena) -> OptExpr {
    try_to_optimizer_expr(plan, scalars, crate::optimizer::test_optimizer_control())
        .expect("invalid logical plan stage")
}

fn aggregate_output_layout_from_plan(
    group_by_len: usize,
    aggregates: &[ScalarAggregateSpec],
    output_columns: &[OutputColumn],
) -> AggregateOutputLayout {
    assert!(
        output_columns.len() >= aggregates.len(),
        "planner Aggregate output_columns must contain aggregate outputs"
    );
    let aggregate_output_ids: Vec<_> = aggregates
        .iter()
        .map(|aggregate| aggregate.output_column_id)
        .collect();
    let group_key_columns: Vec<_> = output_columns
        .iter()
        .filter(|column| !aggregate_output_ids.contains(&column.column_id))
        .take(group_by_len)
        .cloned()
        .collect();
    assert!(
        group_key_columns.len() == group_by_len,
        "planner Aggregate output_columns must contain group key outputs"
    );
    let aggregate_columns = aggregates
        .iter()
        .map(|aggregate| {
            output_columns
                .iter()
                .find(|column| column.column_id == aggregate.output_column_id)
                .unwrap_or_else(|| {
                    panic!(
                        "aggregate output column id {} missing from planner Aggregate.output_columns",
                        aggregate.output_column_id.0
                    )
                })
                .clone()
        })
        .collect();
    AggregateOutputLayout::new(group_key_columns, aggregate_columns)
}

fn to_optimizer_expr_unchecked(
    plan: &LogicalPlanNode,
    scalars: &mut ScalarArena,
    work: &mut novarocks_type_contract::CompileCheckpoints<'_>,
) -> Result<OptExpr, crate::compiler::SqlCompileError> {
    work.step()?;
    let control = work.control();
    if matches!(plan.kind, LogicalPlanKind::Project(_)) && plan.children.len() == 1 {
        let mut spine = Vec::new();
        let mut base = plan;
        while matches!(base.kind, LogicalPlanKind::Project(_)) && base.children.len() == 1 {
            work.step()?;
            spine.push(base);
            base = &base.children[0];
        }
        if spine.len() > 1 {
            let mut child = to_optimizer_expr_unchecked(base, scalars, work)?;
            while let Some(node) = spine.pop() {
                work.step()?;
                let LogicalPlanKind::Project(project) = &node.kind else {
                    unreachable!("the collected spine contains only projects")
                };
                child = OptExpr::new(
                    Operator::LogicalProject(ProjectOp {
                        items: intern_project_items(scalars, &project.items, control)?,
                        output_qualifier: project.output_qualifier.clone(),
                    }),
                    vec![child],
                );
                child.required_output_columns = node.required_output_columns.clone();
            }
            return Ok(child);
        }
    }
    if let Some(flattened) = flatten_homogeneous_union_all(plan, scalars, work)? {
        return Ok(flattened);
    }
    // A parsed UNION chain is left-associated. Convert its spine bottom-up so
    // bridge stack depth depends on one branch, not the number of branches.
    if matches!(plan.kind, LogicalPlanKind::Union(_)) && plan.children.len() == 2 {
        let mut spine = Vec::new();
        let mut base = plan;
        while matches!(base.kind, LogicalPlanKind::Union(_)) && base.children.len() == 2 {
            work.step()?;
            spine.push(base);
            base = &base.children[0];
        }
        if spine.len() > 1 {
            let mut left = to_optimizer_expr_unchecked(base, scalars, work)?;
            while let Some(node) = spine.pop() {
                work.step()?;
                let LogicalPlanKind::Union(union) = &node.kind else {
                    unreachable!("the collected spine contains only unions")
                };
                let right = to_optimizer_expr_unchecked(&node.children[1], scalars, work)?;
                let child_output_columns = node
                    .children
                    .iter()
                    .map(|input| crate::planner::plan_output_columns(input).unwrap_or_default())
                    .collect();
                left = OptExpr::new(
                    Operator::LogicalUnion(UnionOp {
                        all: union.all,
                        output_columns: union.output_columns.clone(),
                        child_output_columns,
                    }),
                    vec![left, right],
                );
                left.required_output_columns = node.required_output_columns.clone();
            }
            return Ok(left);
        }
    }
    let mut expr = match &plan.kind {
        LogicalPlanKind::Scan(node) => {
            for column in &node.columns {
                work.step()?;
                scalars.remember_source_column_display(
                    column.column_id,
                    node.alias.clone(),
                    column.name.clone(),
                );
            }
            let op = Operator::LogicalScan(ScanOp {
                database: node.database.clone(),
                table: node.table.clone(),
                alias: node.alias.clone(),
                stats_ref: None,
                columns: node.columns.clone(),
                predicates: intern_exprs(scalars, &node.predicates, control)?,
                required_columns: node.required_columns.clone(),
                variant_columns: node.variant_columns.clone(),
                mv_rewritten_from: None,
            });
            OptExpr::leaf(op)
        }

        LogicalPlanKind::Filter(node) => {
            let child = to_optimizer_expr_unchecked(plan.unary_input(), scalars, work)?;
            let op = Operator::LogicalFilter(FilterOp {
                predicate: intern_typed(scalars, &node.predicate, control)?,
            });
            OptExpr::new(op, vec![child])
        }

        LogicalPlanKind::Project(node) => {
            let child = to_optimizer_expr_unchecked(plan.unary_input(), scalars, work)?;
            let op = Operator::LogicalProject(ProjectOp {
                items: intern_project_items(scalars, &node.items, control)?,
                output_qualifier: node.output_qualifier.clone(),
            });
            OptExpr::new(op, vec![child])
        }

        LogicalPlanKind::Aggregate(node) => {
            let child = to_optimizer_expr_unchecked(plan.unary_input(), scalars, work)?;
            let group_by = intern_exprs(scalars, &node.group_by, control)?;
            let aggregates = intern_aggregate_calls(scalars, &node.aggregates, control)?;
            let output_layout = aggregate_output_layout_from_plan(
                node.group_by.len(),
                &aggregates,
                &node.output_columns,
            );
            for (scalar_id, output) in group_by.iter().zip(output_layout.group_key_columns.iter()) {
                work.step()?;
                scalars.remember_column_display_from_scalar(output.column_id, *scalar_id);
            }
            let op = Operator::LogicalAggregate(LogicalAggregateOp::single(
                group_by,
                aggregates,
                output_layout,
                node.output_columns.clone(),
            ));
            OptExpr::new(op, vec![child])
        }

        LogicalPlanKind::Join(node) => {
            let left = to_optimizer_expr_unchecked(plan.left(), scalars, work)?;
            let right = to_optimizer_expr_unchecked(plan.right(), scalars, work)?;
            let op = Operator::LogicalJoin(LogicalJoinOp {
                naa_author: node
                    .naa_author
                    .as_ref()
                    .map(|facts| {
                        facts
                            .clone()
                            .try_map(|expr| intern_typed(scalars, &expr, control))
                    })
                    .transpose()?,
                join_type: node.join_type,
                condition: node
                    .condition
                    .as_ref()
                    .map(|condition| intern_typed(scalars, condition, control))
                    .transpose()?,
            });
            OptExpr::new(op, vec![left, right])
        }

        LogicalPlanKind::Sort(node) => {
            let child = to_optimizer_expr_unchecked(plan.unary_input(), scalars, work)?;
            let op = Operator::LogicalSort(SortOp {
                items: intern_sort_items(scalars, &node.items, control)?,
                analytic_partition_exprs: intern_exprs(
                    scalars,
                    &node.analytic_partition_by,
                    control,
                )?,
                partition_limit: node.partition_limit,
                topn_type: node.topn_type,
            });
            OptExpr::new(op, vec![child])
        }

        LogicalPlanKind::Limit(node) => {
            let child = to_optimizer_expr_unchecked(plan.unary_input(), scalars, work)?;
            let op = Operator::LogicalLimit(LimitOp {
                limit: node.limit,
                offset: node.offset,
            });
            OptExpr::new(op, vec![child])
        }

        LogicalPlanKind::Union(node) => {
            let child_output_columns = plan
                .children
                .iter()
                .map(|input| crate::planner::plan_output_columns(input).unwrap_or_default())
                .collect();
            let children: Vec<OptExpr> = plan
                .children
                .iter()
                .map(|input| to_optimizer_expr_unchecked(input, scalars, work))
                .collect::<Result<Vec<_>, crate::compiler::SqlCompileError>>()?;
            let op = Operator::LogicalUnion(UnionOp {
                all: node.all,
                output_columns: node.output_columns.clone(),
                child_output_columns,
            });
            OptExpr::new(op, children)
        }

        LogicalPlanKind::Intersect(node) => {
            let child_output_columns = plan
                .children
                .iter()
                .map(|input| crate::planner::plan_output_columns(input).unwrap_or_default())
                .collect();
            let children: Vec<OptExpr> = plan
                .children
                .iter()
                .map(|input| to_optimizer_expr_unchecked(input, scalars, work))
                .collect::<Result<Vec<_>, crate::compiler::SqlCompileError>>()?;
            let op = Operator::LogicalIntersect(IntersectOp {
                output_columns: node.output_columns.clone(),
                child_output_columns,
            });
            OptExpr::new(op, children)
        }

        LogicalPlanKind::Except(node) => {
            let child_output_columns = plan
                .children
                .iter()
                .map(|input| crate::planner::plan_output_columns(input).unwrap_or_default())
                .collect();
            let children: Vec<OptExpr> = plan
                .children
                .iter()
                .map(|input| to_optimizer_expr_unchecked(input, scalars, work))
                .collect::<Result<Vec<_>, crate::compiler::SqlCompileError>>()?;
            let op = Operator::LogicalExcept(ExceptOp {
                output_columns: node.output_columns.clone(),
                child_output_columns,
            });
            OptExpr::new(op, children)
        }

        LogicalPlanKind::Values(node) => {
            let op = Operator::LogicalValues(ValuesOp {
                rows: {
                    let mut rows = Vec::with_capacity(node.rows.len());
                    for row in &node.rows {
                        work.step()?;
                        rows.push(intern_exprs(scalars, row, control)?);
                    }
                    rows
                },
                columns: node.columns.clone(),
            });
            OptExpr::leaf(op)
        }

        LogicalPlanKind::GenerateSeries(node) => {
            let op = Operator::LogicalGenerateSeries(GenerateSeriesOp {
                start: node.start,
                end: node.end,
                step: node.step,
                column_name: node.column_name.clone(),
                alias: node.alias.clone(),
                output_column_id: node.output_column_id,
            });
            OptExpr::leaf(op)
        }

        LogicalPlanKind::TableFunction(node) => {
            let child = to_optimizer_expr_unchecked(plan.unary_input(), scalars, work)?;
            let op = Operator::LogicalTableFunction(TableFunctionOp {
                function_name: node.function_name.clone(),
                args: intern_exprs(scalars, &node.args, control)?,
                binding: node.binding.clone(),
                output_columns: node.output_columns.clone(),
                alias: node.alias.clone(),
                is_left_join: node.is_left_join,
            });
            OptExpr::new(op, vec![child])
        }

        LogicalPlanKind::Window(node) => {
            let child = to_optimizer_expr_unchecked(plan.unary_input(), scalars, work)?;
            let op = Operator::LogicalWindow(WindowOp {
                window_exprs: intern_window_exprs(scalars, &node.window_exprs, control)?,
                output_columns: node.output_columns.clone(),
            });
            OptExpr::new(op, vec![child])
        }

        LogicalPlanKind::Repeat(node) => {
            let child = to_optimizer_expr_unchecked(plan.unary_input(), scalars, work)?;
            let op = Operator::LogicalRepeat(RepeatOp {
                repeat_column_ref_list: node.repeat_column_ref_list.clone(),
                repeat_column_ref_ids: node.repeat_column_ref_ids.clone(),
                grouping_ids: node.grouping_ids.clone(),
                all_rollup_columns: node.all_rollup_columns.clone(),
                all_rollup_column_ids: node.all_rollup_column_ids.clone(),
                grouping_key_aliases: node.grouping_key_aliases.clone(),
                grouping_fn_args: node.grouping_fn_args.clone(),
                grouping_fn_arg_ids: node.grouping_fn_arg_ids.clone(),
                grouping_fn_ids: node.grouping_fn_ids.clone(),
            });
            OptExpr::new(op, vec![child])
        }

        LogicalPlanKind::CTEConsume(node) => {
            let op = Operator::LogicalCTEConsume(CTEConsumeOp {
                cte_id: node.cte_id,
                alias: node.alias.clone(),
                output_columns: node.output_columns.clone(),
                producer_column_ids: node.producer_column_ids.clone(),
            });
            OptExpr::leaf(op)
        }

        LogicalPlanKind::CTEAnchor(node) => {
            let produce = to_optimizer_expr_unchecked(plan.child(0), scalars, work)?;
            let consumer = to_optimizer_expr_unchecked(plan.child(1), scalars, work)?;
            let op = Operator::LogicalCTEAnchor(CTEAnchorOp {
                cte_id: node.cte_id,
            });
            OptExpr::new(op, vec![produce, consumer])
        }

        LogicalPlanKind::CTEProduce(node) => {
            let child = to_optimizer_expr_unchecked(plan.unary_input(), scalars, work)?;
            let op = Operator::LogicalCTEProduce(CTEProduceOp {
                cte_id: node.cte_id,
                output_columns: node.output_columns.clone(),
            });
            OptExpr::new(op, vec![child])
        }

        LogicalPlanKind::AssertOneRow(node) => {
            let child = to_optimizer_expr_unchecked(plan.unary_input(), scalars, work)?;
            let op = Operator::LogicalAssertOneRow(AssertOneRowOp {
                subquery_text: node.subquery_text.clone(),
            });
            OptExpr::new(op, vec![child])
        }

        LogicalPlanKind::Apply(node) => {
            // Apply is consumed by the subquery/imv rewrite rules BEFORE memo
            // conversion. Building an OptExpr here allows the rewrite rules
            // (subquery/ and imv/ dirs) to operate on OptExpr trees. After
            // rewrite the SubqueryRewrite backstop asserts no Apply remains.
            let outer = to_optimizer_expr_unchecked(plan.left(), scalars, work)?;
            let inner = to_optimizer_expr_unchecked(plan.right(), scalars, work)?;
            let op = Operator::LogicalApply(ApplyOp {
                kind: node.kind,
                subquery_expr: intern_typed(scalars, &node.subquery_expr, control)?,
                output_column: node.output_column.clone(),
                inner_output_column_id: node.inner_output_column_id,
                correlation_column_ids: node.correlation_column_ids.clone(),
                correlation_conjuncts: intern_exprs(scalars, &node.correlation_conjuncts, control)?,
                residual_predicate: node
                    .residual_predicate
                    .as_ref()
                    .map(|e| intern_typed(scalars, e, control))
                    .transpose()?,
                need_check_max_rows: node.need_check_max_rows,
                use_semi_anti: node.use_semi_anti,
                uncorrelated_outer_predicate_columns: node
                    .uncorrelated_outer_predicate_columns
                    .clone(),
            });
            OptExpr::new(op, vec![outer, inner])
        }

        LogicalPlanKind::ImvDelta(node) => {
            // ImvDelta wraps a child subtree (the base plan being rewritten).
            let child = to_optimizer_expr_unchecked(plan.unary_input(), scalars, work)?;
            let op = Operator::LogicalImvDelta(ImvDeltaOp {
                is_root: node.is_root,
                action_column: node.action_column,
                branch_scope: node.branch_scope.clone(),
            });
            OptExpr::new(op, vec![child])
        }

        LogicalPlanKind::ImvVersion(node) => {
            // ImvVersion wraps a child plan (the snapshot scan subtree).
            let op = Operator::LogicalImvVersion(ImvVersionOp {
                version_ref: node.version_ref.clone(),
            });
            if plan.children.is_empty() {
                OptExpr::leaf(op)
            } else {
                let child = to_optimizer_expr_unchecked(plan.unary_input(), scalars, work)?;
                OptExpr::new(op, vec![child])
            }
        }
    };
    expr.required_output_columns = plan.required_output_columns.clone();
    Ok(expr)
}

/// Flatten a long UNION ALL only when every branch already has the root's
/// physical column types. Then bypassing intermediate unions cannot change a
/// coercion step, and the optimizer sees one bounded-depth n-ary operator.
fn flatten_homogeneous_union_all(
    plan: &LogicalPlanNode,
    scalars: &mut ScalarArena,
    work: &mut novarocks_type_contract::CompileCheckpoints<'_>,
) -> Result<Option<OptExpr>, crate::compiler::SqlCompileError> {
    work.step()?;
    let control = work.control();
    let LogicalPlanKind::Union(root) = &plan.kind else {
        return Ok(None);
    };
    if !root.all {
        return Ok(None);
    }
    let mut pending = vec![plan];
    let mut leaves = Vec::new();
    let mut flattened = 0;
    while let Some(node) = pending.pop() {
        work.step()?;
        match &node.kind {
            LogicalPlanKind::Union(union)
                if union.all
                    && node.children.len() == 2
                    && (std::ptr::eq(node, plan) || node.required_output_columns.is_none()) =>
            {
                flattened += 1;
                pending.push(&node.children[1]);
                pending.push(&node.children[0]);
            }
            _ => leaves.push(node),
        }
    }
    if flattened < 2 {
        return Ok(None);
    }
    let mut child_output_columns = Vec::with_capacity(leaves.len());
    for leaf in &leaves {
        work.step()?;
        // Existing output derivation/cloning is opaque. Observe its boundary;
        // this does not claim its internal allocations are governed here.
        work.flush()?;
        let columns = crate::planner::plan_output_columns(leaf);
        control.checkpoint(novarocks_type_contract::CompilePhase::LowerProgram, 0)?;
        let Ok(columns) = columns else {
            return Ok(None);
        };
        child_output_columns.push(columns);
    }
    for columns in &child_output_columns {
        work.step()?;
        if columns.len() != root.output_columns.len() {
            return Ok(None);
        }
        for (child, output) in columns.iter().zip(&root.output_columns) {
            work.step()?;
            if !child
                .value_type
                .same_physical_binding_ignoring_root_nullability_observed(
                    &output.value_type,
                    || work.step().map_err(crate::compiler::SqlCompileError::from),
                )?
            {
                return Ok(None);
            }
        }
    }
    let mut children = Vec::with_capacity(leaves.len());
    for leaf in leaves {
        work.step()?;
        children.push(to_optimizer_expr_unchecked(leaf, scalars, work)?);
    }
    let mut expression = OptExpr::new(
        Operator::LogicalUnion(UnionOp {
            all: true,
            output_columns: root.output_columns.clone(),
            child_output_columns,
        }),
        children,
    );
    expression.required_output_columns = plan.required_output_columns.clone();
    Ok(Some(expression))
}

/// Bridge 2 (reverse): convert an `OptExpr` tree back into a `LogicalPlanNode`
/// tree, materializing all `ScalarId` values from the provided arena.
///
/// This is used exclusively by the IMV rewrite pipeline, which operates on
/// `OptExpr` internally but produces output that callers (e.g. `engine/mod.rs`)
/// still consume as `LogicalPlanNode`. Only the operator variants that can
/// appear in the IMV rewrite path are handled; the remainder panic because
/// they cannot arise from a well-formed IMV rewrite output.
pub(crate) fn to_logical_plan(expr: OptExpr, arena: &ScalarArena) -> LogicalPlanNode {
    let children: Vec<LogicalPlanNode> = expr
        .children
        .into_iter()
        .map(|c| to_logical_plan(c, arena))
        .collect();
    let kind = match expr.op {
        Operator::LogicalScan(op) => LogicalPlanKind::scan(PlanScanNode {
            database: op.database,
            table: op.table,
            alias: op.alias,
            columns: op.columns,
            predicates: materialize_exprs(arena, &op.predicates),
            required_columns: op.required_columns,
            variant_columns: op.variant_columns,
            mv_rewritten_from: None,
        }),
        Operator::LogicalFilter(op) => LogicalPlanKind::Filter(PlanFilterNode {
            predicate: materialize(arena, op.predicate),
        }),
        Operator::LogicalProject(op) => LogicalPlanKind::Project(PlanProjectNode {
            items: materialize_project_items(arena, &op.items),
            output_qualifier: op.output_qualifier,
        }),
        Operator::LogicalAggregate(op) => {
            let group_by = materialize_exprs(arena, &op.group_by);
            let aggregates = materialize_aggregate_calls(arena, &op.aggregates, &op.output_layout);
            LogicalPlanKind::Aggregate(LogicalAggregateNode {
                group_by,
                aggregates,
                output_columns: op.output_columns,
                already_pushed: false,
            })
        }
        Operator::LogicalJoin(op) => LogicalPlanKind::Join(LogicalJoinNode {
            naa_author: op
                .naa_author
                .map(|facts| facts.map(|id| materialize(arena, id))),
            join_type: op.join_type,
            condition: op.condition.map(|id| materialize(arena, id)),
        }),
        Operator::LogicalSort(op) => {
            let items: Vec<SortItem> = materialize_sort_keys(arena, &op.items);
            LogicalPlanKind::Sort(PlanSortNode {
                items,
                analytic_partition_by: materialize_exprs(arena, &op.analytic_partition_exprs),
                output_columns: vec![],
                offset: None,
                partition_limit: op.partition_limit,
                topn_type: op.topn_type,
            })
        }
        Operator::LogicalLimit(op) => LogicalPlanKind::Limit(PlanLimitNode {
            limit: op.limit,
            offset: op.offset,
        }),
        Operator::LogicalUnion(op) => LogicalPlanKind::Union(LogicalUnionNode {
            all: op.all,
            output_columns: op.output_columns,
        }),
        Operator::LogicalValues(op) => LogicalPlanKind::Values(PlanValuesNode {
            rows: op
                .rows
                .iter()
                .map(|row| materialize_exprs(arena, row))
                .collect(),
            columns: op.columns,
        }),
        Operator::LogicalImvDelta(op) => LogicalPlanKind::ImvDelta(LogicalImvDeltaNode {
            is_root: op.is_root,
            action_column: op.action_column,
            branch_scope: op.branch_scope,
        }),
        Operator::LogicalImvVersion(op) => LogicalPlanKind::ImvVersion(LogicalImvVersionNode {
            version_ref: op.version_ref,
        }),
        Operator::LogicalAssertOneRow(op) => LogicalPlanKind::AssertOneRow(
            PlanAssertOneRowNode::global_at_most_one(op.subquery_text),
        ),
        Operator::LogicalIntersect(op) => LogicalPlanKind::Intersect(LogicalIntersectNode {
            output_columns: op.output_columns,
        }),
        Operator::LogicalExcept(op) => LogicalPlanKind::Except(LogicalExceptNode {
            output_columns: op.output_columns,
        }),
        Operator::LogicalGenerateSeries(op) => {
            LogicalPlanKind::GenerateSeries(PlanGenerateSeriesNode {
                start: op.start,
                end: op.end,
                step: op.step,
                column_name: op.column_name,
                alias: op.alias,
                output_column_id: op.output_column_id,
            })
        }
        Operator::LogicalTableFunction(op) => {
            LogicalPlanKind::TableFunction(PlanTableFunctionNode {
                function_name: op.function_name,
                args: materialize_exprs(arena, &op.args),
                binding: op.binding,
                output_columns: op.output_columns,
                alias: op.alias,
                is_left_join: op.is_left_join,
            })
        }
        Operator::LogicalWindow(op) => {
            let window_exprs =
                materialize_window_exprs(arena, &op.window_exprs, &op.output_columns);
            LogicalPlanKind::Window(PlanWindowNode {
                window_exprs,
                output_columns: op.output_columns,
            })
        }
        Operator::LogicalRepeat(op) => LogicalPlanKind::Repeat(PlanRepeatNode {
            repeat_column_ref_list: op.repeat_column_ref_list,
            repeat_column_ref_ids: op.repeat_column_ref_ids,
            grouping_ids: op.grouping_ids,
            all_rollup_columns: op.all_rollup_columns,
            all_rollup_column_ids: op.all_rollup_column_ids,
            grouping_key_aliases: op.grouping_key_aliases,
            grouping_fn_args: op.grouping_fn_args,
            grouping_fn_arg_ids: op.grouping_fn_arg_ids,
            grouping_fn_ids: op.grouping_fn_ids,
            virtual_tuple_id: None,
        }),
        Operator::LogicalCTEAnchor(op) => {
            LogicalPlanKind::CTEAnchor(PlanCTEAnchorNode { cte_id: op.cte_id })
        }
        Operator::LogicalCTEProduce(op) => LogicalPlanKind::CTEProduce(PlanCTEProduceNode {
            cte_id: op.cte_id,
            output_columns: op.output_columns,
        }),
        Operator::LogicalCTEConsume(op) => LogicalPlanKind::CTEConsume(PlanCTEConsumeNode {
            cte_id: op.cte_id,
            alias: op.alias,
            output_columns: op.output_columns,
            producer_column_ids: op.producer_column_ids,
        }),
        Operator::LogicalApply(op) => {
            // Apply is expected to be eliminated by the SubqueryRewrite stage
            // before to_logical_plan is called. If it survives, we
            // still need to materialize it correctly for callers that inspect
            // the plan before memo conversion.
            LogicalPlanKind::apply(LogicalApplyNode {
                kind: op.kind,
                subquery_expr: materialize(arena, op.subquery_expr),
                output_column: op.output_column,
                inner_output_column_id: op.inner_output_column_id,
                correlation_column_ids: op.correlation_column_ids,
                correlation_conjuncts: materialize_exprs(arena, &op.correlation_conjuncts),
                residual_predicate: op.residual_predicate.map(|id| materialize(arena, id)),
                need_check_max_rows: op.need_check_max_rows,
                use_semi_anti: op.use_semi_anti,
                uncorrelated_outer_predicate_columns: op.uncorrelated_outer_predicate_columns,
            })
        }
        // Physical operators should never reach to_logical_plan.
        other => panic!(
            "to_logical_plan: unexpected operator kind {:?} — \
             physical/unknown operators cannot be materialized to LogicalPlanNode",
            other
        ),
    };
    LogicalPlanNode::new(kind, children, expr.required_output_columns)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::analysis::{ExprKind, LiteralValue, OutputColumn, TypedExpr};
    use crate::column_id::ColumnId;
    use crate::common::ScanVariantColumn;
    use crate::optimizer::cascades_rules::implement::ScanToPhysical;
    use crate::optimizer::memo::{GroupId, Memo};
    use crate::optimizer::memo_copy::opt_expr_to_memo;
    use crate::optimizer::optimized_tree::{
        OptimizedOperatorNode, OptimizerExplainStats, PlanExecutionProps,
    };
    use crate::optimizer::rule::Rule;
    use crate::optimizer::statistics::Statistics;
    use crate::optimizer::stats_input::StatsRef;
    use crate::planner::logical::*;
    use crate::planner::logical::{LogicalPlanKind, LogicalUnionNode};
    use crate::planner::payload::*;
    use crate::planner::payload::{PlanFilterNode, PlanScanNode, PlanValuesNode};
    use crate::planner::physical::PhysicalPlanKind;
    use crate::planner::table::{ScanSource, SqlScanKind, TableDef};
    use arrow::datatypes::DataType;
    use novarocks_types::schema::ColumnDef;
    use std::sync::Arc;

    fn logical_plan_to_memo_for_test(plan: &LogicalPlanNode, memo: &mut Memo) -> GroupId {
        let opt_expr = try_to_optimizer_expr(
            plan,
            &mut memo.scalars,
            crate::optimizer::test_optimizer_control(),
        )
        .expect("logical plan to opt expr");
        opt_expr_to_memo(&opt_expr, memo, crate::optimizer::test_optimizer_control()).unwrap()
    }

    fn dummy_table_def() -> TableDef {
        TableDef {
            name: "t1".to_string(),
            columns: vec![ColumnDef {
                name: "id".to_string(),
                data_type: DataType::Int32,
                nullable: false,
                write_default: None,
                logical_type: None,
            }],
            iceberg_row_lineage_metadata_columns: vec![],
            source: crate::planner::table::test_sql_scan_source(SqlScanKind::ConnectorRead),
        }
    }

    fn dummy_output_columns() -> Vec<OutputColumn> {
        vec![OutputColumn {
            column_id: ColumnId::UNSET,
            name: "id".to_string(),
            value_type: novarocks_type_contract::FunctionValueType::new(DataType::Int32, false),

            is_internal: false,
        }]
    }

    fn test_output_column(id: u32, name: &str) -> OutputColumn {
        OutputColumn {
            column_id: ColumnId::new_for_test(id),
            name: name.to_string(),
            value_type: novarocks_type_contract::FunctionValueType::new(DataType::Int64, false),

            is_internal: false,
        }
    }

    fn values_with_columns(columns: Vec<OutputColumn>) -> LogicalPlanNode {
        LogicalPlanNode::new(
            LogicalPlanKind::Values(PlanValuesNode {
                rows: vec![],
                columns,
            }),
            vec![],
            None,
        )
    }

    #[test]
    fn to_optimizer_expr_normalizes_scan_mv_rewrite_source() {
        let scan = LogicalPlanNode::new(
            LogicalPlanKind::scan(PlanScanNode {
                database: "db".to_string(),
                table: dummy_table_def(),
                alias: None,
                columns: dummy_output_columns(),
                predicates: vec![],
                required_columns: None,
                variant_columns: vec![],
                mv_rewritten_from: Some(crate::planner::payload::MvRewriteSelection::unverified(
                    "mv_t1".to_string(),
                )),
            }),
            vec![],
            None,
        );

        let mut scalars = ScalarArena::new();
        let opt_expr = try_to_optimizer_expr(
            &scan,
            &mut scalars,
            crate::optimizer::test_optimizer_control(),
        )
        .expect("logical scan to opt expr");

        let Operator::LogicalScan(op) = opt_expr.op else {
            panic!("expected logical scan");
        };
        assert_eq!(op.mv_rewritten_from, None);
    }

    #[test]
    fn to_optimizer_expr_ignores_sort_output_and_offset_sidecars() {
        let sort = LogicalPlanNode::new(
            LogicalPlanKind::Sort(PlanSortNode {
                items: vec![],
                analytic_partition_by: vec![],
                output_columns: dummy_output_columns(),
                offset: Some(2),
                partition_limit: None,
                topn_type: None,
            }),
            vec![values_with_columns(dummy_output_columns())],
            None,
        );

        let mut scalars = ScalarArena::new();
        let opt_expr = try_to_optimizer_expr(
            &sort,
            &mut scalars,
            crate::optimizer::test_optimizer_control(),
        )
        .expect("logical sort to opt expr");

        let Operator::LogicalSort(op) = opt_expr.op else {
            panic!("expected logical sort");
        };
        assert!(op.items.is_empty());
        assert!(op.analytic_partition_exprs.is_empty());
        assert_eq!(op.partition_limit, None);
        assert_eq!(op.topn_type, None);
    }

    #[test]
    fn to_optimizer_expr_ignores_repeat_virtual_tuple_sidecar() {
        let repeat = LogicalPlanNode::new(
            LogicalPlanKind::Repeat(PlanRepeatNode {
                repeat_column_ref_list: vec![],
                repeat_column_ref_ids: vec![],
                grouping_ids: vec![],
                all_rollup_columns: vec![],
                all_rollup_column_ids: vec![],
                grouping_key_aliases: vec![],
                grouping_fn_args: vec![],
                grouping_fn_arg_ids: vec![],
                grouping_fn_ids: vec![],
                virtual_tuple_id: Some(7),
            }),
            vec![values_with_columns(dummy_output_columns())],
            None,
        );

        let mut scalars = ScalarArena::new();
        let opt_expr = try_to_optimizer_expr(
            &repeat,
            &mut scalars,
            crate::optimizer::test_optimizer_control(),
        )
        .expect("logical repeat to opt expr");

        let Operator::LogicalRepeat(op) = opt_expr.op else {
            panic!("expected logical repeat");
        };
        assert!(op.repeat_column_ref_list.is_empty());
        assert!(op.grouping_ids.is_empty());
    }

    #[test]
    fn set_op_output_columns_survive_memo_stats_with_duplicate_names() {
        let target = vec![test_output_column(20, "dup"), test_output_column(21, "dup")];
        let plan = LogicalPlanNode::new(
            LogicalPlanKind::Union(LogicalUnionNode {
                all: true,
                output_columns: target.clone(),
            }),
            vec![
                values_with_columns(vec![
                    test_output_column(10, "dup"),
                    test_output_column(11, "dup"),
                ]),
                values_with_columns(vec![
                    test_output_column(12, "dup"),
                    test_output_column(13, "dup"),
                ]),
            ],
            None,
        );

        let mut memo = Memo::new();
        let root = logical_plan_to_memo_for_test(&plan, &mut memo);
        let stats_input =
            crate::optimizer::stats_input::OptimizerStatsInput::from_test_table_statistics(
                &std::collections::HashMap::new(),
            );
        crate::optimizer::stats::derive_group_statistics(
            &mut memo,
            &stats_input,
            crate::optimizer::test_optimizer_control(),
        )
        .unwrap();

        let output_columns = &memo.groups[root]
            .logical_props
            .as_ref()
            .expect("set-op root should have logical properties")
            .output_columns;
        assert_eq!(output_columns.len(), target.len());
        assert_eq!(output_columns[0].name, "dup");
        assert_eq!(output_columns[1].name, "dup");
        assert_eq!(output_columns[0].column_id, target[0].column_id);
        assert_eq!(output_columns[1].column_id, target[1].column_id);

        let root_expr = memo.groups[root]
            .logical_exprs
            .first()
            .expect("root logical expression");
        let Operator::LogicalUnion(op) = &root_expr.op else {
            panic!("expected logical union");
        };
        assert_eq!(op.child_output_columns.len(), 2);
        assert_eq!(op.child_output_columns[0][0].column_id, ColumnId(10));
        assert_eq!(op.child_output_columns[0][1].column_id, ColumnId(11));
        assert_eq!(op.child_output_columns[1][0].column_id, ColumnId(12));
        assert_eq!(op.child_output_columns[1][1].column_id, ColumnId(13));
    }

    #[test]
    fn test_scan_to_memo() {
        let scan = LogicalPlanNode::new(
            LogicalPlanKind::scan(PlanScanNode {
                database: "db".to_string(),
                table: dummy_table_def(),
                alias: None,
                columns: dummy_output_columns(),
                predicates: vec![],
                required_columns: None,
                variant_columns: vec![],
                mv_rewritten_from: None,
            }),
            vec![],
            None,
        );

        let mut memo = Memo::new();
        let gid = logical_plan_to_memo_for_test(&scan, &mut memo);

        assert_eq!(gid, 0);
        assert_eq!(memo.groups.len(), 1);
        assert_eq!(memo.groups[0].logical_exprs.len(), 1);
        assert!(memo.groups[0].physical_exprs.is_empty());
        assert!(matches!(
            &memo.groups[0].logical_exprs[0].op,
            Operator::LogicalScan(_)
        ));
        assert!(memo.groups[0].logical_exprs[0].children.is_empty());
    }

    #[test]
    fn variant_path_scan_descriptor_survives_physical_conversion() {
        let source_column_id = ColumnId::new_for_test(100);
        let synthetic_column_id = ColumnId::new_for_test(101);
        let source_type =
            novarocks_type_contract::FunctionValueType::new(DataType::LargeBinary, true);
        let variant_descriptor = ScanVariantColumn::test_fixture(
            source_column_id,
            "payload".to_string(),
            synthetic_column_id,
            "__nr_var_payload_0".to_string(),
            "$.user.id".to_string(),
            DataType::Int64,
            "bigint".to_string(),
            true,
            source_type.clone(),
        );

        let scan = LogicalPlanNode::new(
            LogicalPlanKind::scan(PlanScanNode {
                database: "db".to_string(),
                table: dummy_table_def(),
                alias: None,
                columns: vec![
                    OutputColumn {
                        column_id: source_column_id,
                        name: "payload".to_string(),
                        value_type: source_type.clone(),

                        is_internal: false,
                    },
                    OutputColumn {
                        column_id: synthetic_column_id,
                        name: "__nr_var_payload_0".to_string(),
                        value_type: novarocks_type_contract::FunctionValueType::new(
                            DataType::Int64,
                            true,
                        ),

                        is_internal: true,
                    },
                ],
                predicates: vec![],
                required_columns: None,
                variant_columns: vec![variant_descriptor.clone()],
                mv_rewritten_from: None,
            }),
            vec![],
            None,
        );

        let mut memo = Memo::new();
        let gid = logical_plan_to_memo_for_test(&scan, &mut memo);
        let mut logical_expr = memo.groups[gid].logical_exprs[0].clone();
        let stats_ref = StatsRef::new(7);
        let Operator::LogicalScan(logical_scan) = &mut logical_expr.op else {
            panic!("expected LogicalScan");
        };
        logical_scan.stats_ref = Some(stats_ref);

        let physical = ScanToPhysical
            .apply(
                &logical_expr,
                &mut memo,
                &crate::compiler::SqlCompileControl::unbounded(),
            )
            .unwrap();

        assert_eq!(physical.len(), 1);
        let Operator::PhysicalScan(scan) = &physical[0].op else {
            panic!("expected PhysicalScan");
        };
        assert_eq!(scan.stats_ref, Some(stats_ref));
        assert_eq!(scan.variant_columns.len(), 1);
        let actual = &scan.variant_columns[0];
        assert_eq!(
            actual.source_column_id(),
            variant_descriptor.source_column_id()
        );
        assert_eq!(actual.source_column(), variant_descriptor.source_column());
        assert_eq!(
            actual.synthetic_column_id(),
            variant_descriptor.synthetic_column_id()
        );
        assert_eq!(
            actual.synthetic_column(),
            variant_descriptor.synthetic_column()
        );
        assert_eq!(actual.canonical_path(), variant_descriptor.canonical_path());
        assert_eq!(actual.requested_type(), variant_descriptor.requested_type());
        assert_eq!(actual.strict(), variant_descriptor.strict());
        assert!(Arc::ptr_eq(actual.source(), variant_descriptor.source()));
        let request = actual.source().captured().request();
        assert_eq!(request.arguments.len(), 3);
        let novarocks_functions::FunctionArgument::Value {
            value_type,
            constant,
        } = &request.arguments[0]
        else {
            panic!("original variant source channel");
        };
        assert_eq!(value_type, &source_type);
        assert!(constant.is_none());
    }

    #[test]
    fn scan_source_identity_survives_logical_optimizer_physical_roundtrip() {
        let mut table = dummy_table_def();
        table.source = crate::planner::table::test_sql_scan_source(SqlScanKind::ConnectorRead);
        let scan = LogicalPlanNode::new(
            LogicalPlanKind::scan(PlanScanNode {
                database: "db".to_string(),
                table,
                alias: None,
                columns: dummy_output_columns(),
                predicates: vec![],
                required_columns: None,
                variant_columns: vec![],
                mv_rewritten_from: None,
            }),
            vec![],
            None,
        );

        let mut memo = Memo::new();
        let gid = logical_plan_to_memo_for_test(&scan, &mut memo);
        let logical_expr = memo.groups[gid].logical_exprs[0].clone();
        let mut physical = ScanToPhysical
            .apply(
                &logical_expr,
                &mut memo,
                &crate::compiler::SqlCompileControl::unbounded(),
            )
            .unwrap();
        assert_eq!(physical.len(), 1);

        let execution_props = PlanExecutionProps {
            scalar_arena: Some(Arc::new(memo.scalars)),
            ..Default::default()
        };
        let optimized = OptimizedOperatorNode {
            op: physical.remove(0).op,
            children: vec![],
            stats: Statistics::default(),
            explain_stats: OptimizerExplainStats::default(),
            output_columns: dummy_output_columns(),
            execution_props,
        };
        let materialized = super::super::physical::materialize_physical_plan(&optimized)
            .expect("physical scan bridge");
        let PhysicalPlanKind::Scan(scan) = materialized.kind else {
            panic!("expected physical scan");
        };
        let ScanSource::Sql(source) = &scan.table.source;
        assert!(matches!(source.kind, SqlScanKind::ConnectorRead));
    }

    #[test]
    fn test_filter_scan_to_memo() {
        let scan = LogicalPlanNode::new(
            LogicalPlanKind::scan(PlanScanNode {
                database: "db".to_string(),
                table: dummy_table_def(),
                alias: None,
                columns: dummy_output_columns(),
                predicates: vec![],
                required_columns: None,
                variant_columns: vec![],
                mv_rewritten_from: None,
            }),
            vec![],
            None,
        );

        let predicate = TypedExpr {
            kind: ExprKind::Literal(LiteralValue::Bool(true)),
            value_type: novarocks_type_contract::FunctionValueType::new(DataType::Boolean, false),
        };

        let filter = LogicalPlanNode::new(
            LogicalPlanKind::Filter(PlanFilterNode { predicate }),
            vec![scan],
            None,
        );

        let mut memo = Memo::new();
        let gid = logical_plan_to_memo_for_test(&filter, &mut memo);

        // Should produce 2 groups: Scan (group 0) and Filter (group 1).
        assert_eq!(memo.groups.len(), 2);
        assert_eq!(gid, 1);

        // Group 0: Scan, no children.
        assert_eq!(memo.groups[0].logical_exprs.len(), 1);
        assert!(matches!(
            &memo.groups[0].logical_exprs[0].op,
            Operator::LogicalScan(_)
        ));
        assert!(memo.groups[0].logical_exprs[0].children.is_empty());

        // Group 1: Filter, child = group 0.
        assert_eq!(memo.groups[1].logical_exprs.len(), 1);
        assert!(matches!(
            &memo.groups[1].logical_exprs[0].op,
            Operator::LogicalFilter(_)
        ));
        assert_eq!(memo.groups[1].logical_exprs[0].children, vec![0]);
    }

    #[test]
    fn test_cte_anchor_to_memo() {
        let scan = LogicalPlanNode::new(
            LogicalPlanKind::scan(PlanScanNode {
                database: "db".to_string(),
                table: dummy_table_def(),
                alias: None,
                columns: dummy_output_columns(),
                predicates: vec![],
                required_columns: None,
                variant_columns: vec![],
                mv_rewritten_from: None,
            }),
            vec![],
            None,
        );

        let produce = LogicalPlanNode::new(
            LogicalPlanKind::CTEProduce(PlanCTEProduceNode {
                cte_id: 7,
                output_columns: dummy_output_columns(),
            }),
            vec![scan.clone()],
            None,
        );

        let consume = LogicalPlanNode::new(
            LogicalPlanKind::CTEConsume(PlanCTEConsumeNode {
                cte_id: 7,
                alias: "t".to_string(),
                output_columns: dummy_output_columns(),
                producer_column_ids: dummy_output_columns().iter().map(|c| c.column_id).collect(),
            }),
            vec![],
            None,
        );

        let anchor = LogicalPlanNode::new(
            LogicalPlanKind::CTEAnchor(PlanCTEAnchorNode { cte_id: 7 }),
            vec![produce, consume],
            None,
        );

        let mut memo = Memo::new();
        let gid = logical_plan_to_memo_for_test(&anchor, &mut memo);

        assert_eq!(gid, 3);
        assert!(matches!(
            memo.groups[3].logical_exprs[0].op,
            Operator::LogicalCTEAnchor(_)
        ));
        assert_eq!(memo.groups[3].logical_exprs[0].children, vec![1, 2]);
    }

    #[test]
    fn actual_logical_bridge_observes_entry_wide_rows_and_final_tail() {
        use novarocks_type_contract::{CompileControlError, CompilePhase, PureCompileControl};
        use std::sync::Mutex;
        struct Stop {
            reason: CompileControlError,
            entry: bool,
            calls: Mutex<Vec<u32>>,
        }
        impl PureCompileControl for Stop {
            fn checkpoint(
                &self,
                phase: CompilePhase,
                units: u32,
            ) -> Result<(), CompileControlError> {
                if phase != CompilePhase::LowerProgram {
                    return Ok(());
                }
                self.calls.lock().unwrap().push(units);
                if self.entry || units > 0 {
                    Err(self.reason.clone())
                } else {
                    Ok(())
                }
            }
        }
        for reason in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            for (rows, entry) in [(0, true), (320, false), (0, false)] {
                let plan = LogicalPlanNode::new(
                    LogicalPlanKind::Values(PlanValuesNode {
                        rows: vec![vec![]; rows],
                        columns: vec![],
                    }),
                    vec![],
                    None,
                );
                let control = Stop {
                    reason: reason.clone(),
                    entry,
                    calls: Mutex::new(Vec::new()),
                };
                let result = try_to_optimizer_expr(&plan, &mut ScalarArena::new(), &control);
                assert!(
                    matches!(result, Err(ref error) if *error == crate::compiler::SqlCompileError::from(reason.clone()))
                );
                let calls = control.calls.lock().unwrap();
                let positive = calls.iter().find(|&&units| units > 0).copied();
                if entry {
                    assert_eq!(calls.as_slice(), &[0]);
                } else if rows > 256 {
                    assert_eq!(positive, Some(256));
                } else {
                    assert!(positive.is_some_and(|units| units < 256));
                }
            }
        }
    }
    #[test]
    fn actual_typed_filter_bridge_preserves_interner_control_errors() {
        use novarocks_type_contract::{
            CompileControlError, CompilePhase, FunctionValueType, PureCompileControl,
        };
        use std::sync::Mutex;
        struct StopInterner {
            reason: CompileControlError,
            entry: bool,
            calls: Mutex<Vec<u32>>,
        }
        impl PureCompileControl for StopInterner {
            fn checkpoint(
                &self,
                phase: CompilePhase,
                units: u32,
            ) -> Result<(), CompileControlError> {
                if phase != CompilePhase::Validate {
                    return Ok(());
                }
                self.calls.lock().unwrap().push(units);
                // Small child interners complete first. Refuse the parent's
                // actual wide InList traversal at its first full quantum.
                if self.entry || units == 256 {
                    Err(self.reason.clone())
                } else {
                    Ok(())
                }
            }
        }
        let output = test_output_column(9001, "id");
        let child = TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: output.column_id,
                qualifier: None,
                column: output.name.clone(),
            },
            value_type: output.value_type.clone(),
        };
        let predicate = TypedExpr {
            kind: ExprKind::InList {
                expr: Box::new(child.clone()),
                list: vec![child; 320],
                negated: false,
            },
            value_type: FunctionValueType::new(DataType::Boolean, false),
        };
        let plan = LogicalPlanNode::new(
            LogicalPlanKind::Filter(PlanFilterNode { predicate }),
            vec![values_with_columns(vec![output])],
            None,
        );
        for reason in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            for entry in [true, false] {
                let control = StopInterner {
                    reason: reason.clone(),
                    entry,
                    calls: Mutex::new(Vec::new()),
                };
                let mut scalars = ScalarArena::new();
                assert!(
                    matches!(try_to_optimizer_expr(&plan, &mut scalars, &control), Err(error) if error == crate::compiler::SqlCompileError::from(reason.clone()))
                );
                let calls = control.calls.lock().unwrap();
                if entry {
                    assert_eq!(calls.as_slice(), &[0]);
                } else {
                    assert_eq!(calls.iter().find(|&&units| units == 256), Some(&256));
                    assert_eq!(calls.last(), Some(&256));
                }
            }
        }
        // The same real typed input succeeds with the explicit fixture owner.
        let expression = try_to_optimizer_expr(
            &plan,
            &mut ScalarArena::new(),
            crate::optimizer::test_optimizer_control(),
        )
        .unwrap();
        assert!(matches!(expression.op, Operator::LogicalFilter(_)));
        assert!(matches!(
            expression.children[0].op,
            Operator::LogicalValues(_)
        ));
    }
}
