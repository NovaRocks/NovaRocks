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

use crate::analysis::expr_display::{agg_call_display_name_from_parts, typed_expr_display_name};
use crate::analysis::*;
use crate::column_id::{ColumnId, ColumnRefFactory};
use crate::planner::logical::*;
use crate::planner::payload::*;

/// Extract ColumnId from a TypedExpr, or allocate a new one from the factory.
pub(super) fn expr_column_id(
    expr: &TypedExpr,
    name: &str,
    factory: &mut ColumnRefFactory,
) -> ColumnId {
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

pub(super) fn prepare_repeat_input(
    current: &mut LogicalPlanNode,
    select: &mut ResolvedSelect,
    repeat_info: &mut crate::analysis::RepeatInfo,
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
        LogicalPlanKind::Project(PlanProjectNode {
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

/// Split the SELECT list into post-aggregate projection items and aggregate calls.
///
/// For a query like `SELECT a, count(*), sum(b) + 1 FROM t GROUP BY a`:
/// - group_by exprs: [a]
/// - aggregate calls: [count(*), sum(b)]
/// - project items: the full SELECT list (may reference group-by columns and agg results)
pub(super) fn split_projection_for_aggregate(
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
        let name =
            agg_call_display_name_from_parts(&call.name, &call.args, call.distinct, &call.order_by);
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

pub(super) fn dedup_group_by_exprs(group_by: &[TypedExpr]) -> Vec<TypedExpr> {
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

pub(super) fn ensure_aggregate_output_columns(agg: &mut LogicalAggregateNode) {
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
            name: agg_call_display_name_from_parts(
                &call.name,
                &call.args,
                call.distinct,
                &call.order_by,
            ),
            data_type: call.result_type.clone(),
            nullable: true,
            is_internal: true,
        });
    }
}

pub(super) fn planner_aggregate_group_by_targets(
    agg: &LogicalAggregateNode,
) -> Vec<GroupByRewriteTarget> {
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

pub(super) fn planner_repeat_original_group_by_targets(
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
pub(super) struct GroupByRewriteTarget {
    expr: TypedExpr,
    pub(super) column_id: ColumnId,
    display_name: String,
}

pub(super) fn rewrite_agg_calls_to_refs(
    expr: &TypedExpr,
    agg_calls: &[AggregateCall],
) -> TypedExpr {
    if let ExprKind::AggregateCall {
        name,
        args,
        distinct,
        order_by,
        ..
    } = &expr.kind
        && let Some(call) = agg_calls
            .iter()
            .find(|call| aggregate_call_matches(call, name, args, *distinct, order_by))
    {
        let display = agg_call_display_name_from_parts(name, args, *distinct, order_by);
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

pub(super) fn rewrite_group_by_expr_refs(
    expr: &TypedExpr,
    targets: &[GroupByRewriteTarget],
) -> TypedExpr {
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

pub(super) fn rewrite_expr_children(
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
            volatility,
        } => ExprKind::FunctionCall {
            name: name.clone(),
            args: args.iter().map(&mut rewrite_child).collect(),
            distinct: *distinct,
            volatility: *volatility,
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
            function_order_by,
            aggregate_binding,
            partition_by,
            order_by,
            window_frame,
            ignore_nulls,
        } => ExprKind::WindowCall {
            name: name.clone(),
            args: args.iter().map(&mut rewrite_child).collect(),
            distinct: *distinct,
            function_order_by: function_order_by
                .iter()
                .map(|item| SortItem {
                    expr: rewrite_child(&item.expr),
                    asc: item.asc,
                    nulls_first: item.nulls_first,
                })
                .collect(),
            aggregate_binding: aggregate_binding.clone(),
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
                volatility: left_volatility,
            },
            ExprKind::FunctionCall {
                name: right_name,
                args: right_args,
                distinct: right_distinct,
                volatility: right_volatility,
            },
        ) => {
            left_name.eq_ignore_ascii_case(right_name)
                && left_distinct == right_distinct
                && left_volatility == right_volatility
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
                ..
            },
            ExprKind::AggregateCall {
                name: right_name,
                args: right_args,
                distinct: right_distinct,
                order_by: right_order_by,
                ..
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
                function_order_by: left_function_order_by,
                aggregate_binding: left_aggregate_binding,
                partition_by: left_partition_by,
                order_by: left_order_by,
                window_frame: left_frame,
                ignore_nulls: left_ignore_nulls,
            },
            ExprKind::WindowCall {
                name: right_name,
                args: right_args,
                distinct: right_distinct,
                function_order_by: right_function_order_by,
                aggregate_binding: right_aggregate_binding,
                partition_by: right_partition_by,
                order_by: right_order_by,
                window_frame: right_frame,
                ignore_nulls: right_ignore_nulls,
            },
        ) => {
            left_name.eq_ignore_ascii_case(right_name)
                && left_distinct == right_distinct
                && left_aggregate_binding == right_aggregate_binding
                && left_ignore_nulls == right_ignore_nulls
                && format!("{left_frame:?}") == format!("{right_frame:?}")
                && typed_expr_slices_semantically_eq(left_args, right_args)
                && sort_item_slices_semantically_eq(left_function_order_by, right_function_order_by)
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
pub(super) fn collect_aggregates(
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
            resolved,
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
                let display = agg_call_display_name_from_parts(name, args, *distinct, order_by);
                let output_column_id =
                    factory.create(None, display, expr.data_type.clone(), expr.nullable);
                out.push(AggregateCall {
                    name: name.clone(),
                    args: args.clone(),
                    distinct: *distinct,
                    result_type: expr.data_type.clone(),
                    order_by: order_by.clone(),
                    output_column_id,
                    resolved: resolved.clone(),
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
pub(super) fn collect_non_agg_column_refs(
    expr: &TypedExpr,
    group_by: &[TypedExpr],
    out: &mut Vec<TypedExpr>,
) {
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
