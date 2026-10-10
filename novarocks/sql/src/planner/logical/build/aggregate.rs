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
use crate::compiler::SqlCompileError;
use crate::planner::logical::*;
use crate::planner::payload::*;
use novarocks_type_contract::{CompileCheckpoints, CompilePhase, PureCompileControl};

/// Extract ColumnId from a TypedExpr, or allocate a new one from the factory.
pub(super) fn expr_column_id(
    expr: &TypedExpr,
    name: &str,
    factory: &mut ColumnRefFactory,
) -> ColumnId {
    if let ExprKind::ColumnRef { column_id, .. } = &expr.kind {
        *column_id
    } else {
        factory.create(None, name.to_string(), expr.value_type.clone())
    }
}

pub(super) fn prepare_repeat_input(
    current: &mut LogicalPlanNode,
    select: &mut ResolvedSelect,
    repeat_info: &mut crate::analysis::RepeatInfo,
    repeat_group_qualifier: &str,
    factory: &mut ColumnRefFactory,
    control: &dyn PureCompileControl,
) -> Result<Vec<(String, String)>, SqlCompileError> {
    let grouping_key_aliases: Vec<(String, String)> = repeat_info
        .all_rollup_columns
        .iter()
        .enumerate()
        .map(|(idx, name)| (name.clone(), format!("__repeat_group_key_{idx}")))
        .collect();
    if grouping_key_aliases.is_empty() {
        return Ok(grouping_key_aliases);
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
    // Retain the original typed expression so a later pass can rewrite projection / having
    // occurrences of the same expression to a ColumnRef on the alias.
    let mut substitutions: Vec<RepeatSubstitution> = Vec::new();
    let mut repeat_key_ids_by_name: std::collections::HashMap<String, ColumnId> =
        std::collections::HashMap::new();
    let mut all_rollup_column_ids = Vec::with_capacity(grouping_key_aliases.len());
    for (idx, (_, alias_name)) in grouping_key_aliases.iter().enumerate() {
        let Some(source_expr) = select.group_by.get(idx).cloned() else {
            continue;
        };

        // Repeat's whole job is to null this key out on the levels whose
        // grouping set leaves it out, which is why the substitution below
        // points downstream reads at the materialized slot. A key that any
        // level omits therefore holds NULL in that level's rows, however
        // non-null its source was -- `GROUP BY CUBE(a, b)` over `1 AS a` rolls
        // up to a row where `a` is NULL. A single grouping set omits nothing
        // and keeps the source's own nullability.
        let original_name = grouping_key_aliases
            .get(idx)
            .map(|(original_name, _)| original_name.to_ascii_lowercase());
        let nulled_by_some_level = original_name.is_some_and(|name| {
            repeat_info.repeat_column_ref_list.iter().any(|non_null| {
                !non_null
                    .iter()
                    .any(|column| column.to_ascii_lowercase() == name)
            })
        });
        let nullable = source_expr.value_type.nullable || nulled_by_some_level;
        let mut result_type = source_expr.value_type.clone();
        result_type.nullable = nullable;
        let materialized_column_id = factory.create(None, alias_name.clone(), result_type.clone());
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
            value_type: result_type,
        };
        substitutions.push(RepeatSubstitution {
            source: source_expr.clone(),
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

    // Rewrite grouping keys and their post-aggregate uses to materialized
    // Repeat slots. Aggregate arguments and aggregate-local ordering keep
    // original row inputs so rolled-up levels do not aggregate forced NULLs.
    for gb_expr in &mut select.group_by {
        substitute_expr_in_place(gb_expr, &substitutions, control)?;
    }
    for item in &mut select.projection {
        substitute_expr_in_place(&mut item.expr, &substitutions, control)?;
        if let Some(column_id) = direct_column_ref_id(&item.expr) {
            item.output_column_id = column_id;
        }
    }
    if let Some(having_expr) = select.having.as_mut() {
        substitute_expr_in_place(having_expr, &substitutions, control)?;
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

    Ok(grouping_key_aliases)
}

/// Substitute equal typed expressions while preserving the original aggregate-input boundary.
#[derive(Clone)]
struct RepeatSubstitution {
    source: TypedExpr,
    replacement: TypedExpr,
}

fn substitute_expr_in_place(
    expr: &mut TypedExpr,
    substitutions: &[RepeatSubstitution],
    control: &dyn PureCompileControl,
) -> Result<(), SqlCompileError> {
    if matches!(&expr.kind, ExprKind::AggregateCall { .. }) {
        return Ok(());
    }
    for substitution in substitutions {
        validate_group_output_relation(
            &substitution.source.value_type,
            &substitution.replacement.value_type,
            control,
        )?;
        let mapped_column = authored_group_column_matches(
            expr,
            &substitution.source,
            &substitution.replacement.value_type,
            control,
        )?;
        if mapped_column || typed_expr_semantically_eq(expr, &substitution.source, control)? {
            *expr = substitution.replacement.clone();
            return Ok(());
        }
    }
    *expr = rewrite_expr_children_with(expr, |child| {
        let mut rewritten = child.clone();
        substitute_expr_in_place(&mut rewritten, substitutions, control)?;
        Ok::<_, SqlCompileError>(rewritten)
    })?;
    Ok(())
}

fn validate_group_output_relation(
    source: &novarocks_type_contract::FunctionValueType,
    output: &novarocks_type_contract::FunctionValueType,
    control: &dyn PureCompileControl,
) -> Result<(), SqlCompileError> {
    group_target_work(control, |work| {
        let compatible =
            (!source.nullable || output.nullable) && source.logical_type == output.logical_type;
        work.step()?;
        let same_carrier = if compatible {
            novarocks_type_contract::arrow_data_types_exact_observed::<
                novarocks_functions::ConstantError,
            >(&source.data_type, &output.data_type, || {
                work.step().map_err(Into::into)
            })
            .map_err(SqlCompileError::from)?
        } else {
            false
        };
        if !same_carrier {
            return Err(SqlCompileError::InvalidRequest(
                "grouping output must preserve its source domain and may only widen root nullability".into(),
            ));
        }
        Ok(())
    })
}

// A grouping owner maps an existing source identity to its nullifiable output.
// This relation does not relax the shared equality of arbitrary values.
fn authored_group_column_matches(
    actual: &TypedExpr,
    source: &TypedExpr,
    output: &novarocks_type_contract::FunctionValueType,
    control: &dyn PureCompileControl,
) -> Result<bool, SqlCompileError> {
    let same_column = matches!((&actual.kind, &source.kind),
        (ExprKind::ColumnRef { column_id: actual, .. },
         ExprKind::ColumnRef { column_id: source, .. })
            if *actual != ColumnId::UNSET && actual == source);
    if !same_column {
        return Ok(false);
    }
    group_target_work(control, |work| {
        let compatible = (output.nullable
            || (!actual.value_type.nullable && !source.value_type.nullable))
            && actual.value_type.logical_type == source.value_type.logical_type
            && source.value_type.logical_type == output.logical_type;
        work.step()?;
        if !compatible {
            return Ok(false);
        }
        let same_source = novarocks_type_contract::arrow_data_types_exact_observed::<
            novarocks_functions::ConstantError,
        >(
            &actual.value_type.data_type,
            &source.value_type.data_type,
            || work.step().map_err(Into::into),
        )
        .map_err(SqlCompileError::from)?;
        if !same_source {
            return Ok(false);
        }
        novarocks_type_contract::arrow_data_types_exact_observed::<novarocks_functions::ConstantError>(
            &source.value_type.data_type, &output.data_type,
            || work.step().map_err(Into::into),
        ).map_err(Into::into)
    })
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
        _ => {
            // Unlike output substitution, input collection deliberately enters
            // aggregate argument and aggregate-local ordering domains above.
            // All scalar and window children share the complete traversal.
            let _ = rewrite_expr_children(expr, |child| {
                collect_repeat_input_refs(child, out, seen);
                child.clone()
            });
        }
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
    control: &dyn PureCompileControl,
) -> Result<
    (
        Vec<ProjectItem>,
        Vec<AggregateCall>,
        Vec<OutputColumn>,
        Option<TypedExpr>,
    ),
    SqlCompileError,
> {
    let mut agg_calls = Vec::new();

    for item in projection {
        collect_aggregates(&item.expr, &mut agg_calls, factory, control)?;
    }

    // Also collect aggregate calls from HAVING clause so the aggregate node
    // computes them even when they don't appear in SELECT.
    if let Some(having_expr) = having {
        collect_aggregates(having_expr, &mut agg_calls, factory, control)?;
    }

    let mut output_columns = Vec::with_capacity(group_by.len() + agg_calls.len());
    let mut group_by_rewrite_targets = Vec::new();
    for gb in group_by {
        let output_column = group_by_output_column(gb, projection, factory, control)?;
        group_by_rewrite_targets.push(GroupByRewriteTarget {
            expr: gb.clone(),
            column_id: output_column.column_id,
            output_value_type: output_column.value_type.clone(),
            display_name: typed_expr_display_name(gb, control)?,
        });
        output_columns.push(output_column);
    }
    let aggregate_outputs = agg_calls
        .iter()
        .map(|call| {
            let name = agg_call_display_name_from_parts(
                &call.name,
                call.source.arguments(),
                call.distinct,
                call.source.order_by(),
                control,
            )?;
            Ok(OutputColumn {
                column_id: call.output_column_id,
                name,
                value_type: novarocks_type_contract::FunctionValueType::new(
                    call.result_type.clone(),
                    true,
                ),

                is_internal: false,
            })
        })
        .collect::<Result<Vec<_>, SqlCompileError>>()?;
    output_columns.extend(aggregate_outputs);

    let project_items = projection
        .iter()
        .map(|item| {
            let expr = rewrite_agg_calls_to_refs(&item.expr, &agg_calls, control)?;
            let expr = rewrite_group_by_expr_refs(&expr, &group_by_rewrite_targets, control)?;
            // The expression can share one aggregate runtime symbol while
            // each analyzer-authored output retains its own value domain.
            let output_column_id = if item.output_column_id == ColumnId::UNSET {
                direct_column_ref_id(&expr).unwrap_or(ColumnId::UNSET)
            } else {
                item.output_column_id
            };
            Ok(ProjectItem {
                expr,
                output_name: item.output_name.clone(),
                output_column_id,
            })
        })
        .collect::<Result<Vec<_>, SqlCompileError>>()?;
    let rewritten_having = having
        .map(|expr| {
            let expr = rewrite_agg_calls_to_refs(expr, &agg_calls, control)?;
            rewrite_group_by_expr_refs(&expr, &group_by_rewrite_targets, control)
        })
        .transpose()?;

    Ok((project_items, agg_calls, output_columns, rewritten_having))
}

fn direct_column_ref_id(expr: &TypedExpr) -> Option<ColumnId> {
    match &expr.kind {
        ExprKind::ColumnRef { column_id, .. } if *column_id != ColumnId::UNSET => Some(*column_id),
        ExprKind::Nested(inner) => direct_column_ref_id(inner),
        _ => None,
    }
}

pub(super) fn dedup_group_by_exprs(
    group_by: &[TypedExpr],
    control: &dyn PureCompileControl,
) -> Result<Vec<TypedExpr>, SqlCompileError> {
    let mut deduped = Vec::with_capacity(group_by.len());
    for expr in group_by {
        let mut matched = false;
        for existing in &deduped {
            if typed_expr_semantically_eq(existing, expr, control)? {
                matched = true;
                break;
            }
        }
        if !matched {
            deduped.push(expr.clone());
        }
    }
    Ok(deduped)
}

fn group_target_work<T>(
    control: &dyn PureCompileControl,
    body: impl FnOnce(&mut CompileCheckpoints<'_>) -> Result<T, SqlCompileError>,
) -> Result<T, SqlCompileError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::LowerProgram)?;
    let result = body(&mut work);
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

pub(super) fn ensure_aggregate_output_columns(
    agg: &mut LogicalAggregateNode,
    control: &dyn PureCompileControl,
) -> Result<(), SqlCompileError> {
    let additional = group_target_work(control, |work| {
        let mut existing = std::collections::HashSet::new();
        for column in &agg.output_columns {
            if column.column_id != ColumnId::UNSET {
                existing.insert(column.column_id);
            }
            work.step()?;
        }
        let mut additional = Vec::new();
        for call in &agg.aggregates {
            let missing =
                call.output_column_id != ColumnId::UNSET && existing.insert(call.output_column_id);
            work.step()?;
            if !missing {
                continue;
            }
            work.flush()?;
            let name = agg_call_display_name_from_parts(
                &call.name,
                call.source.arguments(),
                call.distinct,
                call.source.order_by(),
                work.control(),
            )?;
            work.flush()?;
            additional.push(OutputColumn {
                column_id: call.output_column_id,
                name,
                value_type: novarocks_type_contract::FunctionValueType::new(
                    call.result_type.clone(),
                    true,
                ),
                is_internal: true,
            });
            work.step()?;
        }
        Ok(additional)
    })?;
    // Publish only after the shared completion checkpoint accepts every label.
    agg.output_columns.extend(additional);
    Ok(())
}

pub(super) fn planner_aggregate_group_by_targets(
    agg: &LogicalAggregateNode,
    control: &dyn PureCompileControl,
) -> Result<Vec<GroupByRewriteTarget>, SqlCompileError> {
    group_target_work(control, |work| {
        let mut aggregate_output_ids = std::collections::HashSet::new();
        for call in &agg.aggregates {
            if call.output_column_id != ColumnId::UNSET {
                aggregate_output_ids.insert(call.output_column_id);
            }
            work.step()?;
        }
        let mut targets = Vec::new();
        let mut group_by = agg.group_by.iter();
        for output_column in &agg.output_columns {
            let is_aggregate = aggregate_output_ids.contains(&output_column.column_id);
            work.step()?;
            if is_aggregate {
                continue;
            }
            let Some(gb) = group_by.next() else { break };
            work.flush()?;
            let display_name = typed_expr_display_name(gb, work.control())?;
            work.flush()?;
            targets.push(GroupByRewriteTarget {
                expr: gb.clone(),
                column_id: output_column.column_id,
                output_value_type: output_column.value_type.clone(),
                display_name,
            });
            work.step()?;
        }
        Ok(targets)
    })
}

fn repeat_alias_matches(
    left: &str,
    right: &str,
    work: &mut CompileCheckpoints<'_>,
) -> Result<bool, SqlCompileError> {
    let same_length = left.len() == right.len();
    work.step()?;
    if !same_length {
        return Ok(false);
    }
    for (left, right) in left
        .as_bytes()
        .chunks(1024)
        .zip(right.as_bytes().chunks(1024))
    {
        let same = left.eq_ignore_ascii_case(right);
        work.step()?;
        if !same {
            return Ok(false);
        }
    }
    Ok(true)
}

pub(super) fn planner_repeat_original_group_by_targets(
    aggregate_plan: &LogicalPlanNode,
    control: &dyn PureCompileControl,
) -> Result<Vec<GroupByRewriteTarget>, SqlCompileError> {
    group_target_work(control, |work| {
        let aggregate = match &aggregate_plan.kind {
            LogicalPlanKind::Aggregate(agg) => Some(agg),
            _ => None,
        };
        work.step()?;
        let Some(agg) = aggregate else {
            return Ok(Vec::new());
        };
        let repeat = aggregate_plan
            .children
            .first()
            .and_then(|child| match &child.kind {
                LogicalPlanKind::Repeat(repeat) => Some(repeat),
                _ => None,
            });
        work.step()?;
        let Some(repeat) = repeat else {
            return Ok(Vec::new());
        };
        let source_project = aggregate_plan
            .children
            .first()
            .and_then(|repeat_plan| repeat_plan.children.first())
            .and_then(|source| match &source.kind {
                LogicalPlanKind::Project(project) => Some(project),
                _ => None,
            });
        work.step()?;
        work.flush()?;
        let aggregate_targets = planner_aggregate_group_by_targets(agg, work.control())?;
        work.flush()?;
        let mut targets = Vec::new();
        for (idx, (original_name, alias_name)) in repeat.grouping_key_aliases.iter().enumerate() {
            let alias_id = repeat
                .all_rollup_column_ids
                .get(idx)
                .copied()
                .filter(|id| *id != ColumnId::UNSET);
            work.step()?;
            let mut matched = None;
            if let Some(alias_id) = alias_id {
                for target in &aggregate_targets {
                    let same = target.column_id == alias_id;
                    work.step()?;
                    if same {
                        matched = Some(target);
                        break;
                    }
                }
            }
            if matched.is_none() {
                for target in &aggregate_targets {
                    let same = if let Some(alias_id) = alias_id {
                        matches!(&target.expr.kind,
                            ExprKind::ColumnRef { column_id, .. } if *column_id == alias_id)
                    } else if let ExprKind::ColumnRef { column, .. } = &target.expr.kind {
                        repeat_alias_matches(column, alias_name, work)?
                    } else {
                        false
                    };
                    work.step()?;
                    if same {
                        matched = Some(target);
                        break;
                    }
                }
            }
            if let Some(target) = matched {
                work.flush()?;
                let mut source = None;
                if let (Some(alias_id), Some(project)) = (alias_id, source_project) {
                    for item in &project.items {
                        let same = item.output_column_id == alias_id;
                        work.step()?;
                        if same {
                            source = Some(&item.expr);
                            break;
                        }
                    }
                }
                work.flush()?;
                targets.push(GroupByRewriteTarget {
                    expr: if let Some(source) = source {
                        source.clone()
                    } else {
                        TypedExpr {
                            kind: ExprKind::ColumnRef {
                                column_id: ColumnId::UNSET,
                                qualifier: None,
                                column: original_name.clone(),
                            },
                            value_type: target.expr.value_type.clone(),
                        }
                    },
                    column_id: target.column_id,
                    output_value_type: target.output_value_type.clone(),
                    display_name: target.display_name.clone(),
                });
                work.flush()?;
                work.step()?;
            }
        }
        Ok(targets)
    })
}

fn group_by_output_column(
    group_by: &TypedExpr,
    projection: &[ProjectItem],
    factory: &mut ColumnRefFactory,
    control: &dyn PureCompileControl,
) -> Result<OutputColumn, SqlCompileError> {
    let mut matching_projection = None;
    for item in projection {
        if typed_expr_semantically_eq(&item.expr, group_by, control)? {
            matching_projection = Some(item);
            break;
        }
    }
    if let Some(item) = matching_projection {
        return Ok(OutputColumn {
            column_id: direct_column_ref_id(&item.expr)
                .or((item.output_column_id != ColumnId::UNSET).then_some(item.output_column_id))
                .unwrap_or_else(|| expr_column_id(&item.expr, &item.output_name, factory)),
            name: item.output_name.clone(),
            value_type: item.expr.value_type.clone(),

            is_internal: false,
        });
    }

    let name = typed_expr_display_name(group_by, control)?;
    Ok(OutputColumn {
        column_id: expr_column_id(group_by, &name, factory),
        name,
        value_type: group_by.value_type.clone(),

        is_internal: true,
    })
}

#[derive(Clone)]
pub(super) struct GroupByRewriteTarget {
    expr: TypedExpr,
    pub(super) column_id: ColumnId,
    output_value_type: novarocks_type_contract::FunctionValueType,
    display_name: String,
}

pub(super) fn rewrite_agg_calls_to_refs(
    expr: &TypedExpr,
    agg_calls: &[AggregateCall],
    control: &dyn PureCompileControl,
) -> Result<TypedExpr, SqlCompileError> {
    if let ExprKind::AggregateCall {
        name,
        args,
        distinct,
        order_by,
        resolved,
    } = &expr.kind
    {
        for call in agg_calls {
            if aggregate_call_matches(call, name, args, *distinct, order_by, resolved, control)? {
                let display =
                    agg_call_display_name_from_parts(name, args, *distinct, order_by, control)?;
                return Ok(TypedExpr {
                    kind: ExprKind::ColumnRef {
                        column_id: call.output_column_id,
                        qualifier: None,
                        column: display,
                    },
                    value_type: expr.value_type.clone(),
                });
            }
        }
    }
    rewrite_expr_children_with(expr, |child| {
        rewrite_agg_calls_to_refs(child, agg_calls, control)
    })
}

pub(super) fn rewrite_group_by_expr_refs(
    expr: &TypedExpr,
    targets: &[GroupByRewriteTarget],
    control: &dyn PureCompileControl,
) -> Result<TypedExpr, SqlCompileError> {
    for target in targets {
        validate_group_output_relation(
            &target.expr.value_type,
            &target.output_value_type,
            control,
        )?;
        if authored_group_column_matches(expr, &target.expr, &target.output_value_type, control)?
            || typed_expr_semantically_eq(expr, &target.expr, control)?
        {
            return Ok(TypedExpr {
                kind: ExprKind::ColumnRef {
                    column_id: target.column_id,
                    qualifier: None,
                    column: target.display_name.clone(),
                },
                value_type: target.output_value_type.clone(),
            });
        }
    }
    rewrite_expr_children_with(expr, |child| {
        rewrite_group_by_expr_refs(child, targets, control)
    })
}

pub(super) fn rewrite_expr_children(
    expr: &TypedExpr,
    mut rewrite_child: impl FnMut(&TypedExpr) -> TypedExpr,
) -> TypedExpr {
    match rewrite_expr_children_with::<std::convert::Infallible>(expr, |child| {
        Ok(rewrite_child(child))
    }) {
        Ok(value) => value,
        Err(never) => match never {},
    }
}

fn rewrite_expr_children_with<E>(
    expr: &TypedExpr,
    mut rewrite_child: impl FnMut(&TypedExpr) -> Result<TypedExpr, E>,
) -> Result<TypedExpr, E> {
    let kind = match &expr.kind {
        ExprKind::BinaryOp {
            left,
            op,
            right,
            decimal_overflow_policy,
        } => ExprKind::BinaryOp {
            left: Box::new(rewrite_child(left)?),
            op: *op,
            right: Box::new(rewrite_child(right)?),
            decimal_overflow_policy: *decimal_overflow_policy,
        },
        ExprKind::UnaryOp { op, expr: inner } => ExprKind::UnaryOp {
            op: *op,
            expr: Box::new(rewrite_child(inner)?),
        },
        ExprKind::FunctionCall {
            name,
            args,
            distinct,
            binding,
            volatility,
        } => ExprKind::FunctionCall {
            name: name.clone(),
            args: args
                .iter()
                .map(&mut rewrite_child)
                .collect::<Result<_, _>>()?,
            distinct: *distinct,
            binding: binding.clone(),
            volatility: *volatility,
        },
        ExprKind::LambdaFunction { params, body } => ExprKind::LambdaFunction {
            params: params.clone(),
            body: Box::new(rewrite_child(body)?),
        },
        ExprKind::Cast {
            expr: inner,
            target,
            decimal_overflow_policy,
        } => ExprKind::Cast {
            expr: Box::new(rewrite_child(inner)?),
            target: target.clone(),
            decimal_overflow_policy: *decimal_overflow_policy,
        },
        ExprKind::IsNull {
            expr: inner,
            negated,
        } => ExprKind::IsNull {
            expr: Box::new(rewrite_child(inner)?),
            negated: *negated,
        },
        ExprKind::InList {
            expr: inner,
            list,
            negated,
        } => ExprKind::InList {
            expr: Box::new(rewrite_child(inner)?),
            list: list
                .iter()
                .map(&mut rewrite_child)
                .collect::<Result<_, _>>()?,
            negated: *negated,
        },
        ExprKind::Between {
            expr: inner,
            low,
            high,
            negated,
        } => ExprKind::Between {
            expr: Box::new(rewrite_child(inner)?),
            low: Box::new(rewrite_child(low)?),
            high: Box::new(rewrite_child(high)?),
            negated: *negated,
        },
        ExprKind::Like {
            expr: inner,
            pattern,
            negated,
        } => ExprKind::Like {
            expr: Box::new(rewrite_child(inner)?),
            pattern: Box::new(rewrite_child(pattern)?),
            negated: *negated,
        },
        ExprKind::Case {
            operand,
            when_then,
            else_expr,
        } => ExprKind::Case {
            operand: operand
                .as_ref()
                .map(|operand| rewrite_child(operand).map(Box::new))
                .transpose()?,
            when_then: when_then
                .iter()
                .map(|(when, then)| Ok((rewrite_child(when)?, rewrite_child(then)?)))
                .collect::<Result<_, E>>()?,
            else_expr: else_expr
                .as_ref()
                .map(|else_expr| rewrite_child(else_expr).map(Box::new))
                .transpose()?,
        },
        ExprKind::IsTruthValue {
            expr: inner,
            value,
            negated,
        } => ExprKind::IsTruthValue {
            expr: Box::new(rewrite_child(inner)?),
            value: *value,
            negated: *negated,
        },
        ExprKind::Nested(inner) => ExprKind::Nested(Box::new(rewrite_child(inner)?)),
        ExprKind::WindowCall {
            name,
            args,
            distinct,
            binding,
            function_order_by,
            aggregate_binding,
            partition_by,
            order_by,
            window_frame,
            ignore_nulls,
        } => ExprKind::WindowCall {
            name: name.clone(),
            args: args
                .iter()
                .map(&mut rewrite_child)
                .collect::<Result<_, _>>()?,
            distinct: *distinct,
            binding: binding.clone(),
            function_order_by: function_order_by
                .iter()
                .map(|item| {
                    Ok(SortItem {
                        expr: rewrite_child(&item.expr)?,
                        asc: item.asc,
                        nulls_first: item.nulls_first,
                    })
                })
                .collect::<Result<_, E>>()?,
            aggregate_binding: aggregate_binding.clone(),
            partition_by: partition_by
                .iter()
                .map(&mut rewrite_child)
                .collect::<Result<_, _>>()?,
            order_by: order_by
                .iter()
                .map(|item| {
                    Ok(SortItem {
                        expr: rewrite_child(&item.expr)?,
                        asc: item.asc,
                        nulls_first: item.nulls_first,
                    })
                })
                .collect::<Result<_, E>>()?,
            window_frame: window_frame.clone(),
            ignore_nulls: *ignore_nulls,
        },
        ExprKind::Lambda { params, body } => ExprKind::Lambda {
            params: params.clone(),
            body: Box::new(rewrite_child(body)?),
        },
        ExprKind::AggregateCall { .. }
        | ExprKind::ColumnRef { .. }
        | ExprKind::LambdaParamRef { .. }
        | ExprKind::Literal(_)
        | ExprKind::Constant(_)
        | ExprKind::SubqueryPlaceholder { .. } => return Ok(expr.clone()),
    };
    Ok(TypedExpr {
        kind,
        value_type: expr.value_type.clone(),
    })
}

fn aggregate_call_matches(
    call: &AggregateCall,
    name: &str,
    args: &[TypedExpr],
    distinct: bool,
    order_by: &[SortItem],
    resolved: &crate::binding::SqlFunctionBinding,
    control: &dyn PureCompileControl,
) -> Result<bool, SqlCompileError> {
    if call.source.binding() != resolved
        || call.name != name
        || call.distinct != distinct
        || call.source.arguments().len() != args.len()
        || call.source.order_by().len() != order_by.len()
    {
        return Ok(false);
    }
    for (left, right) in call.source.arguments().iter().zip(args) {
        if !typed_expr_semantically_eq(left, right, control)? {
            return Ok(false);
        }
    }
    for (left, right) in call.source.order_by().iter().zip(order_by) {
        if left.asc != right.asc
            || left.nulls_first != right.nulls_first
            || !typed_expr_semantically_eq(&left.expr, &right.expr, control)?
        {
            return Ok(false);
        }
    }
    Ok(true)
}

pub(super) use crate::analysis::expr_identity::typed_expr_semantically_eq;

/// Recursively collect AggregateCall from a TypedExpr tree.
pub(super) fn collect_aggregates(
    expr: &TypedExpr,
    out: &mut Vec<AggregateCall>,
    factory: &mut ColumnRefFactory,
    control: &dyn PureCompileControl,
) -> Result<(), SqlCompileError> {
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
            let mut already = false;
            for call in out.iter() {
                if aggregate_call_matches(call, name, args, *distinct, order_by, resolved, control)?
                {
                    already = true;
                    break;
                }
            }
            if !already {
                let display =
                    agg_call_display_name_from_parts(name, args, *distinct, order_by, control)?;
                let output_column_id = factory.create(None, display, expr.value_type.clone());
                out.push(AggregateCall {
                    name: name.clone(),
                    distinct: *distinct,
                    result_type: expr.value_type.data_type.clone(),
                    output_column_id,
                    source: crate::binding::AggregateArgumentSource::logical_update(
                        args.clone(),
                        order_by.clone(),
                        resolved.clone(),
                    ),
                });
            }
        }
        ExprKind::BinaryOp { left, right, .. } => {
            collect_aggregates(left, out, factory, control)?;
            collect_aggregates(right, out, factory, control)?;
        }
        ExprKind::UnaryOp { expr: inner, .. } => collect_aggregates(inner, out, factory, control)?,
        ExprKind::FunctionCall { args, .. } => {
            for arg in args {
                collect_aggregates(arg, out, factory, control)?;
            }
        }
        ExprKind::LambdaFunction { body, .. } => collect_aggregates(body, out, factory, control)?,
        ExprKind::Cast { expr: inner, .. } => collect_aggregates(inner, out, factory, control)?,
        ExprKind::Case {
            operand,
            when_then,
            else_expr,
        } => {
            if let Some(op) = operand {
                collect_aggregates(op, out, factory, control)?;
            }
            for (w, t) in when_then {
                collect_aggregates(w, out, factory, control)?;
                collect_aggregates(t, out, factory, control)?;
            }
            if let Some(e) = else_expr {
                collect_aggregates(e, out, factory, control)?;
            }
        }
        ExprKind::IsNull { expr: inner, .. } => collect_aggregates(inner, out, factory, control)?,
        ExprKind::Nested(inner) => collect_aggregates(inner, out, factory, control)?,
        ExprKind::InList { expr, list, .. } => {
            collect_aggregates(expr, out, factory, control)?;
            for item in list {
                collect_aggregates(item, out, factory, control)?;
            }
        }
        ExprKind::Between {
            expr, low, high, ..
        } => {
            collect_aggregates(expr, out, factory, control)?;
            collect_aggregates(low, out, factory, control)?;
            collect_aggregates(high, out, factory, control)?;
        }
        ExprKind::Like { expr, pattern, .. } => {
            collect_aggregates(expr, out, factory, control)?;
            collect_aggregates(pattern, out, factory, control)?;
        }
        ExprKind::IsTruthValue { expr: inner, .. } => {
            collect_aggregates(inner, out, factory, control)?;
        }
        // Leaves
        ExprKind::ColumnRef { .. }
        | ExprKind::LambdaParamRef { .. }
        | ExprKind::Literal(_)
        | ExprKind::Constant(_) => {}
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
                collect_aggregates(arg, out, factory, control)?;
            }
            for expr in partition_by {
                collect_aggregates(expr, out, factory, control)?;
            }
            for sort_item in order_by {
                collect_aggregates(&sort_item.expr, out, factory, control)?;
            }
        }
        // SubqueryPlaceholder should be rewritten before reaching the planner
        ExprKind::SubqueryPlaceholder { .. } => {}
        // Higher-order function body is evaluated per element by array_map etc.;
        // any aggregate inside a lambda body would be a semantic error, so
        // walking is unnecessary. Treat as a leaf for aggregate collection.
        ExprKind::Lambda { .. } => {}
    }
    Ok(())
}

/// Collect ColumnRef expressions from HAVING that appear outside of aggregate calls.
/// These are typically scalar subquery results (from CROSS JOINs) that need to pass
/// through the aggregate node as group-by keys.
pub(super) fn collect_non_agg_column_refs(
    expr: &TypedExpr,
    group_by: &[TypedExpr],
    out: &mut Vec<TypedExpr>,
    control: &dyn PureCompileControl,
) -> Result<(), SqlCompileError> {
    collect_non_agg_column_refs_inner(expr, group_by, out, false, control)?;
    Ok(())
}

fn collect_non_agg_column_refs_inner(
    expr: &TypedExpr,
    group_by: &[TypedExpr],
    out: &mut Vec<TypedExpr>,
    inside_agg: bool,
    control: &dyn PureCompileControl,
) -> Result<(), SqlCompileError> {
    if !inside_agg {
        for gb in group_by {
            if typed_expr_semantically_eq(expr, gb, control)? {
                return Ok(());
            }
        }
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
            collect_non_agg_column_refs_inner(left, group_by, out, inside_agg, control)?;
            collect_non_agg_column_refs_inner(right, group_by, out, inside_agg, control)?;
        }
        ExprKind::UnaryOp { expr: inner, .. } => {
            collect_non_agg_column_refs_inner(inner, group_by, out, inside_agg, control)?;
        }
        ExprKind::FunctionCall { args, .. } => {
            for arg in args {
                collect_non_agg_column_refs_inner(arg, group_by, out, inside_agg, control)?;
            }
        }
        ExprKind::Cast { expr: inner, .. } => {
            collect_non_agg_column_refs_inner(inner, group_by, out, inside_agg, control)?;
        }
        ExprKind::Nested(inner) => {
            collect_non_agg_column_refs_inner(inner, group_by, out, inside_agg, control)?;
        }
        ExprKind::IsNull { expr: inner, .. } => {
            collect_non_agg_column_refs_inner(inner, group_by, out, inside_agg, control)?;
        }
        ExprKind::Case {
            operand,
            when_then,
            else_expr,
        } => {
            if let Some(op) = operand {
                collect_non_agg_column_refs_inner(op, group_by, out, inside_agg, control)?;
            }
            for (w, t) in when_then {
                collect_non_agg_column_refs_inner(w, group_by, out, inside_agg, control)?;
                collect_non_agg_column_refs_inner(t, group_by, out, inside_agg, control)?;
            }
            if let Some(e) = else_expr {
                collect_non_agg_column_refs_inner(e, group_by, out, inside_agg, control)?;
            }
        }
        _ => {}
    }
    Ok(())
}

#[cfg(test)]
mod call_policy_tests {
    use super::*;
    use arrow::datatypes::DataType;
    use novarocks_type_contract::{DecimalOverflowPolicy, FunctionValueType};

    #[test]
    fn aggregate_collection_and_reference_rewrite_preserve_distinct_call_policies() {
        let args = vec![TypedExpr {
            kind: ExprKind::Literal(LiteralValue::Int(1)),
            value_type: FunctionValueType::new(DataType::Int64, false),
        }];
        let control = crate::compiler::SqlCompileControl::unbounded();
        let selected = crate::functions::resolve_sql_aggregate_binding(
            crate::functions::builtin_sql_function_catalog(),
            "sum",
            &args,
            &[],
            false,
            crate::constant::test_constant_policy(),
            &control,
        )
        .unwrap();
        let result_type = crate::functions::aggregate_result_type(&selected).clone();
        let expression = |policy| TypedExpr {
            kind: ExprKind::AggregateCall {
                name: "sum".into(),
                args: args.clone(),
                distinct: false,
                order_by: vec![],
                resolved: crate::binding::SqlFunctionBinding::new(selected.clone(), policy),
            },
            value_type: result_type.clone(),
        };
        let output_null = expression(DecimalOverflowPolicy::OutputNull);
        let report_error = expression(DecimalOverflowPolicy::ReportError);
        assert!(!typed_expr_semantically_eq(&output_null, &report_error, &control).unwrap());
        let mut factory = ColumnRefFactory::new();
        let mut calls = Vec::new();
        collect_aggregates(&output_null, &mut calls, &mut factory, &control).unwrap();
        collect_aggregates(&report_error, &mut calls, &mut factory, &control).unwrap();
        collect_aggregates(&output_null, &mut calls, &mut factory, &control).unwrap();
        assert_eq!(calls.len(), 2);
        assert_eq!(
            calls[0].source.binding().decimal_overflow_policy(),
            DecimalOverflowPolicy::OutputNull
        );
        assert_eq!(
            calls[1].source.binding().decimal_overflow_policy(),
            DecimalOverflowPolicy::ReportError
        );
        for (expression, call) in [output_null, report_error].iter().zip(&calls) {
            let rewritten = rewrite_agg_calls_to_refs(expression, &calls, &control).unwrap();
            let ExprKind::ColumnRef { column_id, .. } = rewritten.kind else {
                panic!("actual aggregate occurrence must be rewritten to its own output")
            };
            assert_eq!(column_id, call.output_column_id);
            assert_eq!(rewritten.value_type, result_type);
        }
    }
    fn constant_expr(values: Vec<i64>, ordinal: u32) -> TypedExpr {
        use arrow::array::{Array, Int64Array};
        let control = crate::compiler::SqlCompileControl::unbounded();
        let value_type = FunctionValueType::new(DataType::Int64, false);
        let pool = novarocks_functions::ConstantPool::try_new(
            std::sync::Arc::new(arrow::datatypes::Field::new(
                "source",
                DataType::Int64,
                false,
            )),
            value_type.clone(),
            Int64Array::from(values).to_data(),
            crate::constant::test_constant_policy(),
            CompilePhase::LowerProgram,
            &control,
        )
        .unwrap();
        TypedExpr {
            kind: ExprKind::Constant(pool.value(ordinal).unwrap()),
            value_type,
        }
    }

    #[test]
    fn aggregate_constant_candidates_compare_only_selected_values_and_complete_types() {
        let control = crate::compiler::SqlCompileControl::unbounded();
        let first = constant_expr(vec![-99, 42, 71], 1);
        let equivalent = constant_expr(vec![42, -7], 0);
        let different = constant_expr(vec![99, 43, 42], 1);
        assert!(typed_expr_semantically_eq(&first, &equivalent, &control).unwrap());
        assert!(!typed_expr_semantically_eq(&first, &different, &control).unwrap());
        let mut nullable = equivalent.clone();
        nullable.value_type.nullable = true;
        assert!(!typed_expr_semantically_eq(&first, &nullable, &control).unwrap());
        let deduped = dedup_group_by_exprs(
            &[first.clone(), equivalent.clone(), different.clone()],
            &control,
        )
        .unwrap();
        assert_eq!(deduped.len(), 2);
        let ExprKind::Constant(value) = &deduped[0].kind else {
            panic!("constant source must survive deduplication")
        };
        assert_eq!(value.ordinal(), 1);
        assert_eq!(value.try_i64().unwrap(), Some(42));

        let selected = crate::functions::resolve_sql_aggregate_binding(
            crate::functions::builtin_sql_function_catalog(),
            "sum",
            std::slice::from_ref(&first),
            &[],
            false,
            crate::constant::test_constant_policy(),
            &control,
        )
        .unwrap();
        let binding = crate::binding::SqlFunctionBinding::new(
            selected.clone(),
            DecimalOverflowPolicy::OutputNull,
        );
        let aggregate = |argument| TypedExpr {
            kind: ExprKind::AggregateCall {
                name: "sum".into(),
                args: vec![argument],
                distinct: false,
                order_by: vec![],
                resolved: binding.clone(),
            },
            value_type: crate::functions::aggregate_result_type(&selected).clone(),
        };
        let mut factory = ColumnRefFactory::new();
        let mut calls = Vec::new();
        for expression in [
            aggregate(first),
            aggregate(equivalent),
            aggregate(different),
        ] {
            collect_aggregates(&expression, &mut calls, &mut factory, &control).unwrap();
        }
        assert_eq!(
            calls.len(),
            2,
            "different unused rows cannot split equal selected aggregate arguments"
        );
    }

    fn collected_bridge_call(argument: TypedExpr, name: &str) -> AggregateCall {
        let control = crate::compiler::SqlCompileControl::unbounded();
        let selected = crate::functions::resolve_sql_aggregate_binding(
            crate::functions::builtin_sql_function_catalog(),
            name,
            std::slice::from_ref(&argument),
            &[],
            false,
            crate::constant::test_constant_policy(),
            &control,
        )
        .unwrap();
        let expression = TypedExpr {
            value_type: crate::functions::aggregate_result_type(&selected).clone(),
            kind: ExprKind::AggregateCall {
                name: name.into(),
                args: vec![argument],
                distinct: false,
                order_by: vec![],
                resolved: crate::binding::SqlFunctionBinding::new(
                    selected,
                    DecimalOverflowPolicy::ReportError,
                ),
            },
        };
        let mut calls = Vec::new();
        collect_aggregates(
            &expression,
            &mut calls,
            &mut ColumnRefFactory::new(),
            &control,
        )
        .unwrap();
        assert_eq!(calls.len(), 1);
        calls.remove(0)
    }

    fn bridge_round_trip(call: &AggregateCall) -> AggregateCall {
        use crate::optimizer::operator::AggregateOutputLayout;
        use crate::planner::optimizer_bridge::scalar::{
            intern_aggregate_call, materialize_aggregate_call,
        };
        let mut arena = crate::optimizer::scalar::ScalarArena::new();
        let spec = intern_aggregate_call(
            &mut arena,
            call,
            &crate::compiler::SqlCompileControl::unbounded(),
        )
        .unwrap();
        assert_eq!(
            spec.source.logical_parts().is_some(),
            call.source.logical_parts().is_some()
        );
        let layout = AggregateOutputLayout::new(
            vec![],
            vec![crate::analysis::OutputColumn {
                column_id: call.output_column_id,
                name: "original_result".into(),
                value_type: crate::functions::aggregate_result_type(call.source.binding()).clone(),
                is_internal: false,
            }],
        );
        materialize_aggregate_call(&arena, &spec, &layout)
    }

    #[test]
    fn aggregate_logical_source_bridge_and_capture_preserve_shared_pool_ordinals() {
        use std::sync::Arc;
        let first = constant_expr(vec![-99, 42, 71], 1);
        let ExprKind::Constant(original) = &first.kind else {
            unreachable!()
        };
        let second = TypedExpr {
            kind: ExprKind::Constant(original.pool().value(2).unwrap()),
            value_type: first.value_type.clone(),
        };
        let source = collected_bridge_call(first.clone(), "sum");
        // The actual collector is the source author; the bridge must only transfer it.
        assert!(source.source.logical_parts().is_some());
        let mut second_source = source.clone();
        second_source
            .source
            .rewrite_channels(|arguments, _| arguments[0] = second);
        let control = crate::compiler::SqlCompileControl::unbounded();
        for (call, ordinal, expected) in [(&source, 1, 42), (&second_source, 2, 71)] {
            let materialized = bridge_round_trip(call);
            assert!(std::ptr::eq(
                materialized.source.binding().resolved(),
                source.source.binding().resolved()
            ));
            assert!(std::ptr::eq(
                &materialized.source.binding().resolved().selected,
                &source.source.binding().resolved().selected
            ));
            assert_eq!(
                materialized.source.binding().decimal_overflow_policy(),
                DecimalOverflowPolicy::ReportError
            );
            let captured = crate::binding::capture_aggregate_logical_request(
                &materialized.source,
                crate::constant::test_constant_policy(),
                &control,
            )
            .unwrap();
            assert!(std::ptr::eq(
                captured.binding().resolved(),
                source.source.binding().resolved()
            ));
            assert_eq!(
                captured.constant_policy(),
                crate::constant::test_constant_policy()
            );
            let request = captured.request();
            assert_eq!(request.logical_argument_count, 1);
            let [
                novarocks_functions::FunctionArgument::Value {
                    value_type,
                    constant: Some(value),
                },
            ] = request.arguments
            else {
                panic!("the original selected constant must remain a checked handle")
            };
            assert_eq!(value_type, original.value_type());
            assert_eq!(value.ordinal(), ordinal);
            assert_eq!(value.try_i64().unwrap(), Some(expected));
            assert_eq!(
                value.pool().backing_identity(),
                original.pool().backing_identity()
            );
            assert!(Arc::ptr_eq(
                value.pool().field_ref(),
                original.pool().field_ref()
            ));
        }
    }

    #[test]
    fn aggregate_materialization_does_not_certify_equal_state_only_input() {
        let genuine = collected_bridge_call(constant_expr(vec![9, 42], 1), "count");
        let mut uncertified = genuine.clone();
        // Equal channels, complete types and the same selected Arc do not prove origin.
        uncertified.source = crate::binding::AggregateArgumentSource::uncertified(
            genuine.source.arguments().to_vec(),
            genuine.source.order_by().to_vec(),
            genuine.source.binding().clone(),
        );
        let source = bridge_round_trip(&genuine);
        let state_only = bridge_round_trip(&uncertified);
        let control = crate::compiler::SqlCompileControl::unbounded();
        assert!(
            typed_expr_semantically_eq(
                &source.source.arguments()[0],
                &state_only.source.arguments()[0],
                &control
            )
            .unwrap()
        );
        assert!(std::ptr::eq(
            source.source.binding().resolved(),
            state_only.source.binding().resolved()
        ));
        assert!(
            crate::binding::capture_aggregate_logical_request(
                &source.source,
                crate::constant::test_constant_policy(),
                &control
            )
            .is_ok()
        );
        assert!(matches!(
            crate::binding::capture_aggregate_logical_request(
                &state_only.source,
                crate::constant::test_constant_policy(),
                &control
            ),
            Err(crate::binding::AggregateRequestCaptureError::MissingLogicalSource)
        ));
    }

    struct EqualityControl {
        trace: std::sync::Mutex<Vec<(CompilePhase, u32)>>,
        refuse: Option<usize>,
        cause: novarocks_type_contract::CompileControlError,
    }
    impl PureCompileControl for EqualityControl {
        fn checkpoint(
            &self,
            phase: CompilePhase,
            units: u32,
        ) -> Result<(), novarocks_type_contract::CompileControlError> {
            let mut trace = self.trace.lock().unwrap();
            trace.push((phase, units));
            if self.refuse == Some(trace.len() - 1) {
                Err(self.cause)
            } else {
                Ok(())
            }
        }
    }

    #[test]
    fn aggregate_constant_equality_keeps_original_control_and_false_completion_tail() {
        use novarocks_type_contract::CompileControlError as Cause;
        let first = constant_expr(vec![91, 42], 1);
        for right in [constant_expr(vec![42, 9], 0), constant_expr(vec![43], 0)] {
            let recording = EqualityControl {
                trace: Default::default(),
                refuse: None,
                cause: Cause::Cancelled,
            };
            let expected = typed_expr_semantically_eq(&first, &right, &recording).unwrap();
            let trace = recording.trace.into_inner().unwrap();
            assert!(!trace.is_empty());
            for cause in [
                Cause::Cancelled,
                Cause::DeadlineExceeded,
                Cause::ResourceExhausted,
            ] {
                for stop in 0..trace.len() {
                    let control = EqualityControl {
                        trace: Default::default(),
                        refuse: Some(stop),
                        cause,
                    };
                    let error = typed_expr_semantically_eq(&first, &right, &control).unwrap_err();
                    assert!(matches!(
                        (cause, error),
                        (Cause::Cancelled, SqlCompileError::Cancelled)
                            | (Cause::DeadlineExceeded, SqlCompileError::DeadlineExceeded)
                            | (Cause::ResourceExhausted, SqlCompileError::ResourceExhausted)
                    ));
                    assert_eq!(control.trace.into_inner().unwrap(), trace[..=stop]);
                }
            }
            let ExprKind::Constant(value) = &right.kind else {
                panic!("checked constant fixture")
            };
            assert_eq!(expected, value.try_i64().unwrap() == Some(42));
        }
    }
    #[test]
    fn repeat_substitution_uses_selected_constant_identity_and_column_ids() {
        let control = crate::compiler::SqlCompileControl::unbounded();
        let original = constant_expr(vec![-99, 42], 1);
        let replacement = TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: ColumnId::new_for_test(701),
                qualifier: Some("repeat".into()),
                column: "key".into(),
            },
            value_type: original.value_type.clone(),
        };
        let substitutions = [RepeatSubstitution {
            source: original,
            replacement: replacement.clone(),
        }];
        let mut equivalent = constant_expr(vec![42, 99], 0);
        substitute_expr_in_place(&mut equivalent, &substitutions, &control).unwrap();
        assert!(
            matches!(equivalent.kind, ExprKind::ColumnRef { column_id, .. } if column_id == ColumnId::new_for_test(701))
        );
        let mut different = constant_expr(vec![99, 43], 1);
        substitute_expr_in_place(&mut different, &substitutions, &control).unwrap();
        let ExprKind::Constant(value) = &different.kind else {
            panic!("different selected value must not be substituted")
        };
        assert_eq!(value.try_i64().unwrap(), Some(43));

        let source_column = TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: ColumnId::new_for_test(301),
                qualifier: Some("left".into()),
                column: "same-label".into(),
            },
            value_type: replacement.value_type.clone(),
        };
        let columns = [RepeatSubstitution {
            source: source_column.clone(),
            replacement,
        }];
        let mut same_label_other_id = source_column.clone();
        if let ExprKind::ColumnRef { column_id, .. } = &mut same_label_other_id.kind {
            *column_id = ColumnId::new_for_test(302);
        }
        substitute_expr_in_place(&mut same_label_other_id, &columns, &control).unwrap();
        assert!(
            matches!(same_label_other_id.kind, ExprKind::ColumnRef { column_id, .. } if column_id == ColumnId::new_for_test(302))
        );
        let mut same_id_other_qualifier = source_column;
        if let ExprKind::ColumnRef { qualifier, .. } = &mut same_id_other_qualifier.kind {
            *qualifier = Some("derived".into());
        }
        substitute_expr_in_place(&mut same_id_other_qualifier, &columns, &control).unwrap();
        assert!(
            matches!(same_id_other_qualifier.kind, ExprKind::ColumnRef { column_id, .. } if column_id == ColumnId::new_for_test(701))
        );
    }

    #[test]
    fn repeat_targets_use_authored_ids_before_colliding_constant_diagnostics() {
        let control = crate::compiler::SqlCompileControl::unbounded();
        let constant = constant_expr(vec![-99, 42], 1);
        let key = TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: ColumnId::new_for_test(702),
                qualifier: None,
                column: "42".into(),
            },
            value_type: constant.value_type.clone(),
        };
        assert_eq!(
            typed_expr_display_name(&constant, &control).unwrap(),
            typed_expr_display_name(&key, &control).unwrap(),
            "a diagnostic collision cannot prove a grouping-key identity"
        );
        let output = |id| OutputColumn {
            column_id: ColumnId::new_for_test(id),
            name: "42".into(),
            value_type: constant.value_type.clone(),
            is_internal: false,
        };
        let repeat = PlanRepeatNode {
            repeat_column_ref_list: vec![],
            repeat_column_ref_ids: vec![],
            grouping_ids: vec![],
            all_rollup_columns: vec!["original".into()],
            all_rollup_column_ids: vec![ColumnId::new_for_test(702)],
            grouping_key_aliases: vec![("original".into(), "42".into())],
            grouping_fn_args: vec![],
            grouping_fn_arg_ids: vec![],
            grouping_fn_ids: vec![],
            virtual_tuple_id: None,
        };
        let mut plan = LogicalPlanNode::new(
            LogicalPlanKind::Aggregate(LogicalAggregateNode {
                group_by: vec![constant.clone(), key],
                aggregates: vec![],
                output_columns: vec![output(701), output(702)],
                already_pushed: false,
            }),
            vec![LogicalPlanNode::new(
                LogicalPlanKind::Repeat(repeat),
                vec![],
                None,
            )],
            None,
        );
        let targets = planner_repeat_original_group_by_targets(&plan, &control).unwrap();
        assert_eq!(targets.len(), 1);
        assert_eq!(targets[0].column_id, ColumnId::new_for_test(702));

        let source = TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: ColumnId::new_for_test(303),
                qualifier: Some("source".into()),
                column: "original".into(),
            },
            value_type: constant.value_type.clone(),
        };
        plan.children[0].children.push(LogicalPlanNode::new(
            LogicalPlanKind::Project(PlanProjectNode {
                items: vec![ProjectItem {
                    expr: source.clone(),
                    output_name: "42".into(),
                    output_column_id: ColumnId::new_for_test(702),
                }],
                output_qualifier: None,
            }),
            vec![],
            None,
        ));
        let LogicalPlanKind::Aggregate(aggregate) = &mut plan.kind else {
            unreachable!()
        };
        aggregate.group_by[1].value_type.nullable = true;
        aggregate.output_columns[1].value_type.nullable = true;
        let targets = planner_repeat_original_group_by_targets(&plan, &control).unwrap();
        assert!(
            matches!(targets[0].expr.kind, ExprKind::ColumnRef { column_id, .. }
            if column_id == ColumnId::new_for_test(303))
        );
        let rewritten = rewrite_group_by_expr_refs(&source, &targets, &control).unwrap();
        assert!(
            matches!(rewritten.kind, ExprKind::ColumnRef { column_id, .. }
            if column_id == ColumnId::new_for_test(702))
        );
        assert!(rewritten.value_type.nullable);
        let mut wrong_type = source.clone();
        wrong_type.value_type.data_type = DataType::Int32;
        let unchanged = rewrite_group_by_expr_refs(&wrong_type, &targets, &control).unwrap();
        assert!(
            matches!(unchanged.kind, ExprKind::ColumnRef { column_id, .. }
            if column_id == ColumnId::new_for_test(303))
        );
        assert_eq!(unchanged.value_type.data_type, DataType::Int32);

        let LogicalPlanKind::Repeat(repeat) = &mut plan.children[0].kind else {
            unreachable!()
        };
        repeat.all_rollup_column_ids[0] = ColumnId::new_for_test(999);
        assert!(
            planner_repeat_original_group_by_targets(&plan, &control)
                .unwrap()
                .is_empty()
        );

        let LogicalPlanKind::Repeat(repeat) = &mut plan.children[0].kind else {
            unreachable!()
        };
        repeat.all_rollup_column_ids.clear();
        let targets = planner_repeat_original_group_by_targets(&plan, &control).unwrap();
        assert_eq!(targets.len(), 1);
        assert_eq!(targets[0].column_id, ColumnId::new_for_test(702));

        let LogicalPlanKind::Aggregate(aggregate) = &mut plan.kind else {
            unreachable!()
        };
        aggregate.group_by.truncate(1);
        aggregate.output_columns.truncate(1);
        assert!(
            planner_repeat_original_group_by_targets(&plan, &control)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn aggregate_output_rendering_preserves_control_without_partial_publication() {
        use novarocks_type_contract::CompileControlError as Cause;
        let admitted = crate::compiler::SqlCompileControl::unbounded();
        let mut calls = Vec::new();
        let mut factory = ColumnRefFactory::new();
        for argument in [
            constant_expr(vec![-99, 42], 1),
            constant_expr(vec![43, 99], 0),
        ] {
            let selected = crate::functions::resolve_sql_aggregate_binding(
                crate::functions::builtin_sql_function_catalog(),
                "sum",
                std::slice::from_ref(&argument),
                &[],
                false,
                crate::constant::test_constant_policy(),
                &admitted,
            )
            .unwrap();
            let expression = TypedExpr {
                value_type: crate::functions::aggregate_result_type(&selected).clone(),
                kind: ExprKind::AggregateCall {
                    name: "sum".into(),
                    args: vec![argument],
                    distinct: false,
                    order_by: vec![],
                    resolved: crate::binding::SqlFunctionBinding::new(
                        selected,
                        DecimalOverflowPolicy::OutputNull,
                    ),
                },
            };
            collect_aggregates(&expression, &mut calls, &mut factory, &admitted).unwrap();
        }
        let original = LogicalAggregateNode {
            group_by: vec![],
            aggregates: calls,
            output_columns: vec![],
            already_pushed: false,
        };
        let recording = EqualityControl {
            trace: Default::default(),
            refuse: None,
            cause: Cause::Cancelled,
        };
        let mut success = original.clone();
        ensure_aggregate_output_columns(&mut success, &recording).unwrap();
        assert_eq!(success.output_columns.len(), 2);
        assert_eq!(success.output_columns[0].name, "sum(42)");
        assert_eq!(success.output_columns[1].name, "sum(43)");
        assert_eq!(
            success.output_columns[0].column_id,
            success.aggregates[0].output_column_id
        );
        assert_eq!(
            success.output_columns[1].column_id,
            success.aggregates[1].output_column_id
        );
        let success_trace = recording.trace.into_inner().unwrap();
        let mut invalid = original.clone();
        invalid.aggregates[1]
            .source
            .rewrite_channels(|arguments, _| {
                arguments[0].value_type.nullable = true;
            });
        let ordinary = EqualityControl {
            trace: Default::default(),
            refuse: None,
            cause: Cause::Cancelled,
        };
        let mut invalid_output = invalid.clone();
        assert!(matches!(
            ensure_aggregate_output_columns(&mut invalid_output, &ordinary),
            Err(SqlCompileError::InvalidRequest(_))
        ));
        assert!(invalid_output.output_columns.is_empty());
        let ordinary_trace = ordinary.trace.into_inner().unwrap();
        assert_eq!(
            ordinary_trace.last(),
            Some(&(CompilePhase::LowerProgram, 0))
        );
        for (input, trace) in [(original, success_trace), (invalid, ordinary_trace)] {
            assert!(!trace.is_empty());
            for cause in [
                Cause::Cancelled,
                Cause::DeadlineExceeded,
                Cause::ResourceExhausted,
            ] {
                for stop in 0..trace.len() {
                    let control = EqualityControl {
                        trace: Default::default(),
                        refuse: Some(stop),
                        cause,
                    };
                    let mut aggregate = input.clone();
                    let error =
                        ensure_aggregate_output_columns(&mut aggregate, &control).unwrap_err();
                    assert!(matches!(
                        (cause, error),
                        (Cause::Cancelled, SqlCompileError::Cancelled)
                            | (Cause::DeadlineExceeded, SqlCompileError::DeadlineExceeded)
                            | (Cause::ResourceExhausted, SqlCompileError::ResourceExhausted)
                    ));
                    assert!(aggregate.output_columns.is_empty());
                    assert_eq!(control.trace.into_inner().unwrap(), trace[..=stop]);
                }
            }
        }
    }

    #[test]
    fn group_target_helpers_observe_empty_and_nonmatching_repeat_completion() {
        use novarocks_type_contract::CompileControlError as Cause;
        let empty = LogicalAggregateNode {
            group_by: vec![],
            aggregates: vec![],
            output_columns: vec![],
            already_pushed: false,
        };
        let repeat = PlanRepeatNode {
            repeat_column_ref_list: vec![],
            repeat_column_ref_ids: vec![],
            grouping_ids: vec![],
            all_rollup_columns: vec![],
            all_rollup_column_ids: vec![],
            grouping_key_aliases: (0..320)
                .map(|index| (format!("original_{index}"), "absent".into()))
                .collect(),
            grouping_fn_args: vec![],
            grouping_fn_arg_ids: vec![],
            grouping_fn_ids: vec![],
            virtual_tuple_id: None,
        };
        let plan = LogicalPlanNode::new(
            LogicalPlanKind::Aggregate(empty.clone()),
            vec![LogicalPlanNode::new(
                LogicalPlanKind::Repeat(repeat),
                vec![],
                None,
            )],
            None,
        );
        let run = |path, control: &dyn PureCompileControl| -> Result<usize, SqlCompileError> {
            match path {
                0 => {
                    let mut aggregate = empty.clone();
                    ensure_aggregate_output_columns(&mut aggregate, control)?;
                    Ok(aggregate.output_columns.len())
                }
                1 => Ok(planner_aggregate_group_by_targets(&empty, control)?.len()),
                2 => Ok(planner_repeat_original_group_by_targets(&plan, control)?.len()),
                _ => unreachable!(),
            }
        };
        for path in 0..3 {
            let recording = EqualityControl {
                trace: Default::default(),
                refuse: None,
                cause: Cause::Cancelled,
            };
            assert_eq!(run(path, &recording).unwrap(), 0);
            let trace = recording.trace.into_inner().unwrap();
            assert!(trace.len() >= 2);
            if path == 2 {
                assert!(trace.iter().any(|(_, units)| *units == 256));
            }
            for cause in [
                Cause::Cancelled,
                Cause::DeadlineExceeded,
                Cause::ResourceExhausted,
            ] {
                for stop in 0..trace.len() {
                    let control = EqualityControl {
                        trace: Default::default(),
                        refuse: Some(stop),
                        cause,
                    };
                    let error = run(path, &control).unwrap_err();
                    assert!(matches!(
                        (cause, error),
                        (Cause::Cancelled, SqlCompileError::Cancelled)
                            | (Cause::DeadlineExceeded, SqlCompileError::DeadlineExceeded)
                            | (Cause::ResourceExhausted, SqlCompileError::ResourceExhausted)
                    ));
                    assert_eq!(control.trace.into_inner().unwrap(), trace[..=stop]);
                }
            }
        }
    }

    #[test]
    fn grouping_target_relation_refuses_domain_drift_before_any_equal_source_fallback() {
        use novarocks_type_contract::{CompileControlError as Cause, ValueLogicalType};
        let control = crate::compiler::SqlCompileControl::unbounded();
        let selected = constant_expr(vec![-99, 42], 1);
        let mut widened = selected.value_type.clone();
        widened.nullable = true;
        let valid = GroupByRewriteTarget {
            expr: selected.clone(),
            column_id: ColumnId::new_for_test(702),
            output_value_type: widened.clone(),
            display_name: "diagnostic".into(),
        };
        let equivalent = constant_expr(vec![42, 7], 0);
        let rewritten = rewrite_group_by_expr_refs(&equivalent, &[valid], &control).unwrap();
        assert!(
            matches!(rewritten.kind, ExprKind::ColumnRef { column_id, .. }
            if column_id == ColumnId::new_for_test(702))
        );
        assert_eq!(rewritten.value_type, widened);

        let column = |value_type| TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: ColumnId::new_for_test(301),
                qualifier: None,
                column: "source".into(),
            },
            value_type,
        };
        let computed = TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(selected.clone()),
                right: Box::new(constant_expr(vec![1], 0)),
                op: BinOp::Add,
                decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
            },
            value_type: FunctionValueType::new(DataType::Int64, true),
        };
        let nested = |metadata: &str| {
            FunctionValueType::new(
                DataType::List(std::sync::Arc::new(
                    arrow::datatypes::Field::new("item", DataType::Int64, true)
                        .with_metadata([("provider".into(), metadata.into())].into()),
                )),
                false,
            )
        };
        let uuid = FunctionValueType::try_with_logical_type(
            DataType::FixedSizeBinary(16),
            false,
            ValueLogicalType::Uuid,
        )
        .unwrap();
        let cases = [
            (
                column(FunctionValueType::new(DataType::Int64, false)),
                FunctionValueType::new(DataType::Int32, true),
            ),
            (selected, FunctionValueType::new(DataType::Int32, false)),
            (computed, FunctionValueType::new(DataType::Int32, true)),
            (column(nested("source")), nested("changed")),
            (
                column(FunctionValueType::new(DataType::FixedSizeBinary(16), false)),
                uuid,
            ),
            (
                column(FunctionValueType::new(DataType::Int64, true)),
                FunctionValueType::new(DataType::Int64, false),
            ),
        ];
        for (source, output_value_type) in cases {
            let target = GroupByRewriteTarget {
                expr: source.clone(),
                column_id: ColumnId::new_for_test(702),
                output_value_type,
                display_name: "diagnostic".into(),
            };
            let recording = EqualityControl {
                trace: Default::default(),
                refuse: None,
                cause: Cause::Cancelled,
            };
            assert!(matches!(
                rewrite_group_by_expr_refs(&source, std::slice::from_ref(&target), &recording,),
                Err(SqlCompileError::InvalidRequest(_))
            ));
            let trace = recording.trace.into_inner().unwrap();
            assert!(trace.len() >= 2);
            assert!(trace.last().unwrap().1 > 0);
            for cause in [
                Cause::Cancelled,
                Cause::DeadlineExceeded,
                Cause::ResourceExhausted,
            ] {
                for stop in 0..trace.len() {
                    let refusing = EqualityControl {
                        trace: Default::default(),
                        refuse: Some(stop),
                        cause,
                    };
                    let error = rewrite_group_by_expr_refs(
                        &source,
                        std::slice::from_ref(&target),
                        &refusing,
                    )
                    .unwrap_err();
                    assert!(matches!(
                        (cause, error),
                        (Cause::Cancelled, SqlCompileError::Cancelled)
                            | (Cause::DeadlineExceeded, SqlCompileError::DeadlineExceeded)
                            | (Cause::ResourceExhausted, SqlCompileError::ResourceExhausted)
                    ));
                    assert_eq!(refusing.trace.into_inner().unwrap(), trace[..=stop]);
                }
            }
        }
    }
}
