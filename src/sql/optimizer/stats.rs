//! Statistics derivation for Memo groups.
//!
//! Mirrors the logic in `sql::optimizer::cardinality` but operates on
//! Memo operators (`MExpr`) and reads child statistics from group logical
//! properties instead of recursing the `LogicalPlan` tree.

use std::collections::HashMap;

use super::estimate::join_condition::estimate_join_condition;
use super::estimate::ndv::{
    agg_group_rows, cap_ndv_at_rows, get_expr_ndv, get_join_key_ndv_with_confidence,
};
use super::estimate::selectivity::apply_filter;
pub(crate) use super::estimate::selectivity::{estimate_selectivity, extract_column_id};
use super::memo::{MExpr, Memo};
use super::operator::Operator;
use crate::sql::analysis::{BinOp, ExprKind, JoinKind, OutputColumn, TypedExpr};
use crate::sql::column_id::ColumnId;
use crate::sql::optimizer::estimate::arith::sat_add;
use crate::sql::optimizer::estimate::cardinality::{
    JoinCardInput, estimate_join_cardinality, except_rows, intersect_rows, union_all_rows,
    union_distinct_rows,
};
use crate::sql::optimizer::statistics::*;
use crate::sql::planner::plan::LogicalPlan;

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Derive [`Statistics`] for a single `MExpr` using child group statistics
/// already stored in `memo.groups[child].logical_props`.
pub(crate) fn derive_statistics(
    expr: &MExpr,
    memo: &Memo,
    table_stats: &HashMap<String, TableStatistics>,
) -> Statistics {
    match &expr.op {
        // -- Leaf operators (no children) --
        Operator::LogicalScan(scan) => derive_scan(scan, table_stats),
        Operator::LogicalValues(vals) => Statistics {
            output_row_count: vals.rows.len() as f64,
            row_count_confidence: Confidence::Exact,
            column_statistics: HashMap::new(),
        },
        Operator::LogicalGenerateSeries(gs) => Statistics {
            output_row_count: generate_series_row_count_f64(gs.start, gs.end, gs.step),
            row_count_confidence: Confidence::Exact,
            column_statistics: HashMap::new(),
        },
        Operator::LogicalTableFunction(tf) => {
            derive_table_function_stats(tf.is_left_join, expr, memo)
        }
        Operator::LogicalCTEConsume(cte) => {
            // Look up the CTEProduce group's row count from the memo.
            if let Some(&produce_group_id) = memo.cte_produce_groups.get(&cte.cte_id) {
                if let Some(ref props) = memo.groups[produce_group_id].logical_props {
                    Statistics {
                        output_row_count: props.row_count,
                        row_count_confidence: Confidence::Estimated,
                        column_statistics: HashMap::new(),
                    }
                } else {
                    // CTEProduce group not yet derived (should not happen in bottom-up order).
                    Statistics {
                        output_row_count: 10_000.0,
                        row_count_confidence: Confidence::Fallback,
                        column_statistics: HashMap::new(),
                    }
                }
            } else {
                // No mapping found; conservative fallback.
                Statistics {
                    output_row_count: 10_000.0,
                    row_count_confidence: Confidence::Fallback,
                    column_statistics: HashMap::new(),
                }
            }
        }
        Operator::LogicalCTEAnchor(_) => child_statistics(memo, &expr.children, 1),

        // -- Unary operators (single child) --
        Operator::LogicalFilter(filter) => {
            let child_stats = child_statistics(memo, &expr.children, 0);
            let selectivity =
                estimate_selectivity(&filter.predicate, &child_stats.column_statistics);
            let (output_rows, row_count_confidence) = apply_filter(
                child_stats.output_row_count,
                child_stats.row_count_confidence,
                selectivity,
            );
            let mut column_statistics = child_stats.column_statistics;
            for stat in column_statistics.values_mut() {
                stat.distinct_values_count =
                    cap_ndv_at_rows(stat.distinct_values_count, output_rows);
            }
            Statistics {
                output_row_count: output_rows,
                row_count_confidence,
                column_statistics,
            }
        }

        Operator::LogicalProject(proj) => {
            let child_stats = child_statistics(memo, &expr.children, 0);
            let projected: HashMap<ColumnId, ColumnStatistic> = proj
                .items
                .iter()
                .filter_map(|item| {
                    extract_column_id(&item.expr)
                        .and_then(|column_id| child_stats.column_statistics.get(&column_id))
                        .cloned()
                        .map(|cs| (item.output_column_id, cs))
                })
                .collect();
            Statistics {
                output_row_count: child_stats.output_row_count,
                row_count_confidence: Confidence::Estimated,
                column_statistics: projected,
            }
        }

        Operator::LogicalAggregate(agg) => {
            let child_stats = child_statistics(memo, &expr.children, 0);
            if agg.group_by.is_empty() {
                return Statistics {
                    output_row_count: 1.0,
                    row_count_confidence: Confidence::Estimated,
                    column_statistics: HashMap::new(),
                };
            }
            let group_key_ndvs: Vec<f64> = agg
                .group_by
                .iter()
                .map(|gb_expr| get_expr_ndv(gb_expr, &child_stats.column_statistics))
                .collect();
            let output_rows = agg_group_rows(&group_key_ndvs, child_stats.output_row_count);
            let column_statistics = aggregate_group_column_statistics(
                &agg.group_by,
                &agg.output_columns,
                &child_stats,
                output_rows,
            );
            Statistics {
                output_row_count: output_rows,
                row_count_confidence: Confidence::derive(
                    &[child_stats.row_count_confidence],
                    false,
                ),
                column_statistics,
            }
        }

        Operator::LogicalSort(_) => {
            // Sort preserves row count.
            child_statistics(memo, &expr.children, 0)
        }

        Operator::LogicalTopN(topn) => {
            // TopN limits output rows to at most limit+offset.
            let child_stats = child_statistics(memo, &expr.children, 0);
            let limit_rows = match (topn.limit, topn.offset) {
                (Some(l), Some(o)) => ((l as f64) + (o as f64)).min(child_stats.output_row_count),
                (Some(l), None) => (l as f64).min(child_stats.output_row_count),
                _ => child_stats.output_row_count,
            };
            Statistics {
                output_row_count: limit_rows.max(0.0),
                row_count_confidence: Confidence::Estimated,
                column_statistics: child_stats.column_statistics,
            }
        }

        Operator::LogicalLimit(limit) => {
            let child_stats = child_statistics(memo, &expr.children, 0);
            let output_rows = if let Some(lim) = limit.limit {
                (lim as f64).min(child_stats.output_row_count)
            } else {
                child_stats.output_row_count
            };
            Statistics {
                output_row_count: output_rows.max(0.0),
                row_count_confidence: Confidence::Estimated,
                column_statistics: child_stats.column_statistics,
            }
        }

        Operator::LogicalWindow(window) => {
            let child_stats = child_statistics(memo, &expr.children, 0);
            derive_window_statistics(child_stats, &window.window_exprs)
        }

        Operator::LogicalRepeat(repeat) => {
            let child_stats = child_statistics(memo, &expr.children, 0);
            let repeat_times = repeat.repeat_column_ref_list.len() as f64;
            Statistics {
                output_row_count: child_stats.output_row_count * repeat_times,
                row_count_confidence: Confidence::Estimated,
                column_statistics: child_stats.column_statistics,
            }
        }

        Operator::LogicalCTEProduce(_) => {
            // Passthrough child stats.
            child_statistics(memo, &expr.children, 0)
        }

        Operator::LogicalDecode(_) => {
            // Decode preserves row count and column stats.
            child_statistics(memo, &expr.children, 0)
        }

        Operator::LogicalAggregateStateMerge(_) => {
            let old_stats = child_statistics(memo, &expr.children, 0);
            let delta_stats = child_statistics(memo, &expr.children, 1);
            Statistics {
                output_row_count: (old_stats.output_row_count + delta_stats.output_row_count)
                    .max(1.0),
                row_count_confidence: Confidence::Estimated,
                column_statistics: HashMap::new(),
            }
        }

        // -- Binary / multi-child operators --
        Operator::LogicalJoin(join) => {
            let left_stats = child_statistics(memo, &expr.children, 0);
            let right_stats = child_statistics(memo, &expr.children, 1);
            derive_join(join, &left_stats, &right_stats)
        }

        Operator::LogicalUnion(union_op) => derive_set_op_statistics(
            memo,
            &expr.children,
            &union_op.output_columns,
            SetOpKind::Union { all: union_op.all },
        ),

        Operator::LogicalIntersect(intersect_op) => derive_set_op_statistics(
            memo,
            &expr.children,
            &intersect_op.output_columns,
            SetOpKind::Intersect,
        ),

        Operator::LogicalExcept(except_op) => derive_set_op_statistics(
            memo,
            &expr.children,
            &except_op.output_columns,
            SetOpKind::Except,
        ),

        // -- Physical operators: derive the same way as their logical counterparts --
        Operator::PhysicalScan(scan) => derive_scan_statistics(
            &scan.table.name,
            scan.alias.as_deref(),
            &scan.columns,
            &scan.predicates,
            table_stats,
            estimate_default_row_count(&scan.table.name),
        ),

        Operator::PhysicalFilter(filter) => {
            let child_stats = child_statistics(memo, &expr.children, 0);
            let selectivity =
                estimate_selectivity(&filter.predicate, &child_stats.column_statistics);
            let (output_rows, row_count_confidence) = apply_filter(
                child_stats.output_row_count,
                child_stats.row_count_confidence,
                selectivity,
            );
            let mut column_statistics = child_stats.column_statistics;
            for stat in column_statistics.values_mut() {
                stat.distinct_values_count =
                    cap_ndv_at_rows(stat.distinct_values_count, output_rows);
            }
            Statistics {
                output_row_count: output_rows,
                row_count_confidence,
                column_statistics,
            }
        }

        Operator::PhysicalProject(proj) => {
            let child_stats = child_statistics(memo, &expr.children, 0);
            let projected: HashMap<ColumnId, ColumnStatistic> = proj
                .items
                .iter()
                .filter_map(|item| {
                    extract_column_id(&item.expr)
                        .and_then(|column_id| child_stats.column_statistics.get(&column_id))
                        .cloned()
                        .map(|cs| (item.output_column_id, cs))
                })
                .collect();
            Statistics {
                output_row_count: child_stats.output_row_count,
                row_count_confidence: Confidence::Estimated,
                column_statistics: projected,
            }
        }

        Operator::PhysicalHashAggregate(agg) => {
            let child_stats = child_statistics(memo, &expr.children, 0);
            if agg.group_by.is_empty() {
                return Statistics {
                    output_row_count: 1.0,
                    row_count_confidence: Confidence::Estimated,
                    column_statistics: HashMap::new(),
                };
            }
            let group_key_ndvs: Vec<f64> = agg
                .group_by
                .iter()
                .map(|gb_expr| get_expr_ndv(gb_expr, &child_stats.column_statistics))
                .collect();
            let output_rows = agg_group_rows(&group_key_ndvs, child_stats.output_row_count);
            let column_statistics = aggregate_group_column_statistics(
                &agg.group_by,
                &agg.output_columns,
                &child_stats,
                output_rows,
            );
            Statistics {
                output_row_count: output_rows,
                row_count_confidence: Confidence::derive(
                    &[child_stats.row_count_confidence],
                    false,
                ),
                column_statistics,
            }
        }

        Operator::PhysicalHashJoin(join) => {
            let left_stats = child_statistics(memo, &expr.children, 0);
            let right_stats = child_statistics(memo, &expr.children, 1);
            let eq_key_ndvs: Vec<(f64, f64, Confidence, Option<(ColumnId, ColumnId)>)> = join
                .eq_conditions
                .iter()
                .map(|eq| {
                    let eq_key_pair = extract_column_id(&eq.left).zip(extract_column_id(&eq.right));
                    let (left_ndv, left_confidence) = best_join_key_ndv(
                        &eq.left,
                        &left_stats.column_statistics,
                        &right_stats.column_statistics,
                    );
                    let (right_ndv, right_confidence) = best_join_key_ndv(
                        &eq.right,
                        &right_stats.column_statistics,
                        &left_stats.column_statistics,
                    );
                    (
                        left_ndv,
                        right_ndv,
                        left_confidence.combine(right_confidence),
                        eq_key_pair,
                    )
                })
                .collect();
            let mut eq_key_ndvs_for_cardinality = Vec::new();
            let mut eq_key_pairs = Vec::new();
            for (left_ndv, right_ndv, confidence, pair) in eq_key_ndvs {
                eq_key_ndvs_for_cardinality.push((left_ndv, right_ndv, confidence));
                if let Some(pair) = pair {
                    eq_key_pairs.push(pair);
                }
            }

            let (output_rows, row_count_confidence) = estimate_join_cardinality(&JoinCardInput {
                left: (left_stats.output_row_count, left_stats.row_count_confidence),
                right: (
                    right_stats.output_row_count,
                    right_stats.row_count_confidence,
                ),
                kind: join.join_type,
                eq_key_ndvs: eq_key_ndvs_for_cardinality,
                non_equi_selectivity: None,
            });

            let column_statistics = merge_join_column_statistics(
                &left_stats,
                &right_stats,
                output_rows,
                row_count_confidence,
                join.join_type,
                &eq_key_pairs,
            );
            Statistics {
                output_row_count: output_rows,
                row_count_confidence,
                column_statistics,
            }
        }

        Operator::PhysicalNestLoopJoin(join) => {
            let left_stats = child_statistics(memo, &expr.children, 0);
            let right_stats = child_statistics(memo, &expr.children, 1);
            let non_equi_selectivity = join.condition.as_ref().map(|cond| {
                (
                    estimate_selectivity(cond, &left_stats.column_statistics),
                    Confidence::Estimated,
                )
            });
            let eq_key_pairs = collect_equi_join_column_pairs(join.condition.as_ref());

            let (output_rows, row_count_confidence) = estimate_join_cardinality(&JoinCardInput {
                left: (left_stats.output_row_count, left_stats.row_count_confidence),
                right: (
                    right_stats.output_row_count,
                    right_stats.row_count_confidence,
                ),
                kind: join.join_type,
                eq_key_ndvs: Vec::new(),
                non_equi_selectivity,
            });

            let column_statistics = merge_join_column_statistics(
                &left_stats,
                &right_stats,
                output_rows,
                row_count_confidence,
                join.join_type,
                &eq_key_pairs,
            );
            Statistics {
                output_row_count: output_rows,
                row_count_confidence,
                column_statistics,
            }
        }

        Operator::PhysicalSort(_) => child_statistics(memo, &expr.children, 0),

        Operator::PhysicalTopN(topn) => {
            // TopN limits output rows to at most limit+offset.
            let child_stats = child_statistics(memo, &expr.children, 0);
            let limit_rows = match (topn.limit, topn.offset) {
                (Some(l), Some(o)) => ((l as f64) + (o as f64)).min(child_stats.output_row_count),
                (Some(l), None) => (l as f64).min(child_stats.output_row_count),
                _ => child_stats.output_row_count,
            };
            Statistics {
                output_row_count: limit_rows.max(0.0),
                row_count_confidence: Confidence::Estimated,
                column_statistics: child_stats.column_statistics,
            }
        }

        Operator::PhysicalLimit(limit) => {
            let child_stats = child_statistics(memo, &expr.children, 0);
            let output_rows = if let Some(lim) = limit.limit {
                (lim as f64).min(child_stats.output_row_count)
            } else {
                child_stats.output_row_count
            };
            Statistics {
                output_row_count: output_rows.max(0.0),
                row_count_confidence: Confidence::Estimated,
                column_statistics: child_stats.column_statistics,
            }
        }

        Operator::PhysicalWindow(window) => {
            let child_stats = child_statistics(memo, &expr.children, 0);
            derive_window_statistics(child_stats, &window.window_exprs)
        }

        Operator::PhysicalDistribution(_) => {
            // Distribution enforcer preserves row count.
            child_statistics(memo, &expr.children, 0)
        }

        Operator::PhysicalCTEProduce(_) => child_statistics(memo, &expr.children, 0),

        Operator::PhysicalCTEConsume(cte) => {
            // Look up the CTEProduce group's row count from the memo.
            if let Some(&produce_group_id) = memo.cte_produce_groups.get(&cte.cte_id) {
                if let Some(ref props) = memo.groups[produce_group_id].logical_props {
                    Statistics {
                        output_row_count: props.row_count,
                        row_count_confidence: Confidence::Estimated,
                        column_statistics: HashMap::new(),
                    }
                } else {
                    Statistics {
                        output_row_count: 10_000.0,
                        row_count_confidence: Confidence::Fallback,
                        column_statistics: HashMap::new(),
                    }
                }
            } else {
                Statistics {
                    output_row_count: 10_000.0,
                    row_count_confidence: Confidence::Fallback,
                    column_statistics: HashMap::new(),
                }
            }
        }

        Operator::PhysicalCTEAnchor(_) => child_statistics(memo, &expr.children, 1),

        Operator::PhysicalRepeat(repeat) => {
            let child_stats = child_statistics(memo, &expr.children, 0);
            let repeat_times = repeat.repeat_column_ref_list.len() as f64;
            Statistics {
                output_row_count: child_stats.output_row_count * repeat_times,
                row_count_confidence: Confidence::Estimated,
                column_statistics: child_stats.column_statistics,
            }
        }

        Operator::PhysicalUnion(union_op) => derive_set_op_statistics(
            memo,
            &expr.children,
            &union_op.output_columns,
            SetOpKind::Union { all: union_op.all },
        ),

        Operator::PhysicalIntersect(intersect_op) => derive_set_op_statistics(
            memo,
            &expr.children,
            &intersect_op.output_columns,
            SetOpKind::Intersect,
        ),

        Operator::PhysicalExcept(except_op) => derive_set_op_statistics(
            memo,
            &expr.children,
            &except_op.output_columns,
            SetOpKind::Except,
        ),

        Operator::PhysicalValues(vals) => Statistics {
            output_row_count: vals.rows.len() as f64,
            row_count_confidence: Confidence::Exact,
            column_statistics: HashMap::new(),
        },

        Operator::PhysicalGenerateSeries(gs) => Statistics {
            output_row_count: generate_series_row_count_f64(gs.start, gs.end, gs.step),
            row_count_confidence: Confidence::Exact,
            column_statistics: HashMap::new(),
        },
        Operator::PhysicalTableFunction(tf) => {
            derive_table_function_stats(tf.is_left_join, expr, memo)
        }

        Operator::PhysicalDecode(_) => {
            // Decode preserves row count and column stats.
            child_statistics(memo, &expr.children, 0)
        }

        Operator::PhysicalAggregateStateMerge(_) => {
            let old_stats = child_statistics(memo, &expr.children, 0);
            let delta_stats = child_statistics(memo, &expr.children, 1);
            Statistics {
                output_row_count: (old_stats.output_row_count + delta_stats.output_row_count)
                    .max(1.0),
                row_count_confidence: Confidence::Estimated,
                column_statistics: HashMap::new(),
            }
        }
        Operator::LogicalAssertOneRow(_) | Operator::PhysicalAssertOneRow(_) => {
            let child_stats = child_statistics(memo, &expr.children, 0);
            Statistics {
                output_row_count: child_stats.output_row_count.min(1.0),
                row_count_confidence: Confidence::Estimated,
                column_statistics: child_stats.column_statistics,
            }
        }
    }
}

pub(crate) fn derive_logical_plan_statistics(
    plan: &LogicalPlan,
    table_stats: &HashMap<String, TableStatistics>,
) -> Statistics {
    let mut memo = Memo::new();
    let root_group = super::convert::logical_plan_to_memo(plan, &mut memo);
    derive_group_statistics(&mut memo, table_stats);
    memo.groups
        .get(root_group)
        .and_then(|group| group.logical_props.as_ref())
        .map(|props| Statistics {
            output_row_count: props.row_count,
            row_count_confidence: props.row_count_confidence,
            column_statistics: props.column_statistics.clone(),
        })
        .unwrap_or_else(|| Statistics {
            output_row_count: 1.0,
            row_count_confidence: Confidence::Fallback,
            column_statistics: HashMap::new(),
        })
}

fn best_join_key_ndv(
    expr: &TypedExpr,
    primary_stats: &HashMap<ColumnId, ColumnStatistic>,
    secondary_stats: &HashMap<ColumnId, ColumnStatistic>,
) -> (f64, Confidence) {
    let primary = get_join_key_ndv_with_confidence(expr, primary_stats);
    let secondary = get_join_key_ndv_with_confidence(expr, secondary_stats);
    match (
        primary.1 == Confidence::Fallback,
        secondary.1 == Confidence::Fallback,
    ) {
        (false, true) => primary,
        (true, false) => secondary,
        _ if secondary.0 > primary.0 => secondary,
        _ => primary,
    }
}

fn aggregate_group_column_statistics(
    group_by: &[TypedExpr],
    output_columns: &[OutputColumn],
    child_stats: &Statistics,
    output_rows: f64,
) -> HashMap<ColumnId, ColumnStatistic> {
    group_by
        .iter()
        .zip(output_columns.iter())
        .map(|(expr, output)| {
            let mut stat = extract_column_id(expr)
                .and_then(|column_id| child_stats.column_statistics.get(&column_id))
                .cloned()
                .unwrap_or_else(|| {
                    let mut fallback = ColumnStatistic::unknown();
                    fallback.distinct_values_count =
                        get_expr_ndv(expr, &child_stats.column_statistics);
                    fallback
                });
            stat.distinct_values_count = cap_ndv_at_rows(stat.distinct_values_count, output_rows);
            (output.column_id, stat)
        })
        .collect()
}

/// Derive statistics for all groups in the Memo, bottom-up.
///
/// Groups are visited in order (0..N). Since `convert.rs` inserts leaves
/// before their parents, group 0 is the deepest leaf and the last group
/// is the root. This guarantees that all child groups have their
/// `logical_props` set before any parent group is processed.
pub(crate) fn derive_group_statistics(
    memo: &mut Memo,
    table_stats: &HashMap<String, TableStatistics>,
) {
    for group_idx in 0..memo.groups.len() {
        // Derive stats from the first logical expression in the group.
        let stats = if let Some(first_expr) = memo.groups[group_idx].logical_exprs.first() {
            let expr_clone = first_expr.clone();
            derive_statistics(&expr_clone, memo, table_stats)
        } else {
            // No logical expression; fall back to first physical expression.
            if let Some(first_expr) = memo.groups[group_idx].physical_exprs.first() {
                let expr_clone = first_expr.clone();
                derive_statistics(&expr_clone, memo, table_stats)
            } else {
                // Empty group: use defaults.
                Statistics {
                    output_row_count: 1.0,
                    row_count_confidence: Confidence::Fallback,
                    column_statistics: HashMap::new(),
                }
            }
        };

        // Derive output columns from the operator.
        let output_columns = derive_output_columns(memo, group_idx);

        memo.groups[group_idx].logical_props = Some(super::logical_props::derive_for_group(
            memo,
            group_idx,
            output_columns,
            stats.output_row_count,
            stats.row_count_confidence,
            stats.column_statistics,
        ));
    }
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

/// Get child group's statistics as a [`Statistics`] value.
///
/// Reads `logical_props` from the child group. If not yet derived (should
/// not happen when groups are processed in order), returns a default.
fn child_statistics(memo: &Memo, children: &[super::memo::GroupId], index: usize) -> Statistics {
    let group_id = children[index];
    let group = &memo.groups[group_id];
    if let Some(ref props) = group.logical_props {
        // Column statistics now travel on LogicalProperties, so propagate
        // them so parent operators estimate real selectivity / join NDV.
        Statistics {
            output_row_count: props.row_count,
            row_count_confidence: props.row_count_confidence,
            column_statistics: props.column_statistics.clone(),
        }
    } else {
        // Child not yet derived; use conservative default.
        Statistics {
            output_row_count: 10_000.0,
            row_count_confidence: Confidence::Fallback,
            column_statistics: HashMap::new(),
        }
    }
}

fn derive_table_function_stats(is_left_join: bool, expr: &MExpr, memo: &Memo) -> Statistics {
    let child = child_statistics(memo, &expr.children, 0);
    let estimated_rows = child.output_row_count * 3.0;
    Statistics {
        output_row_count: if is_left_join {
            estimated_rows.max(child.output_row_count)
        } else {
            estimated_rows.max(1.0)
        },
        row_count_confidence: Confidence::Estimated,
        column_statistics: HashMap::new(),
    }
}

#[derive(Clone, Copy)]
enum SetOpKind {
    Union { all: bool },
    Intersect,
    Except,
}

fn derive_set_op_statistics(
    memo: &Memo,
    children: &[usize],
    output_columns: &[OutputColumn],
    kind: SetOpKind,
) -> Statistics {
    let child_stats: Vec<_> = children
        .iter()
        .enumerate()
        .map(|(i, _)| child_statistics(memo, children, i))
        .collect();
    let input_rows: Vec<_> = child_stats.iter().map(|s| s.output_row_count).collect();
    let (formula_rows, saturated_or_defaulted) = match kind {
        SetOpKind::Union { all: true } => union_all_rows(&input_rows),
        SetOpKind::Union { all: false } => union_distinct_rows(&input_rows),
        SetOpKind::Intersect => intersect_rows(&input_rows),
        SetOpKind::Except => except_rows(&input_rows),
    };
    let (output_rows, defaulted_output_rows) = positive_set_op_output_rows(formula_rows);
    let row_confidences: Vec<_> = child_stats.iter().map(|s| s.row_count_confidence).collect();
    let column_statistics = merge_set_op_column_statistics(
        memo,
        children,
        output_columns,
        &child_stats,
        output_rows,
        kind,
    );

    Statistics {
        output_row_count: output_rows,
        row_count_confidence: Confidence::derive(
            &row_confidences,
            saturated_or_defaulted || defaulted_output_rows,
        ),
        column_statistics,
    }
}

fn merge_set_op_column_statistics(
    memo: &Memo,
    children: &[usize],
    output_columns: &[OutputColumn],
    child_stats: &[Statistics],
    output_rows: f64,
    kind: SetOpKind,
) -> HashMap<ColumnId, ColumnStatistic> {
    let child_output_columns: Vec<_> = children
        .iter()
        .enumerate()
        .map(|(i, _)| child_output_columns(memo, children, i))
        .collect();
    let mut merged = HashMap::new();

    for (column_idx, output_column) in output_columns.iter().enumerate() {
        let mut stats_for_column = Vec::new();
        let mut missing_child_stat = false;
        for (child_idx, stats) in child_stats.iter().enumerate() {
            let child_column_id = child_output_columns
                .get(child_idx)
                .and_then(|columns| columns.get(column_idx))
                .map(|column| column.column_id);
            let child_stat = child_column_id
                .and_then(|column_id| stats.column_statistics.get(&column_id))
                .or_else(|| stats.column_statistics.get(&output_column.column_id));
            if let Some(stat) = child_stat {
                stats_for_column.push(stat);
            } else {
                missing_child_stat = true;
            }
        }
        let Some(first) = stats_for_column.first().copied() else {
            continue;
        };

        let mut min_value = first.min_value;
        let mut max_value = first.max_value;
        let mut nulls_fraction = first.nulls_fraction;
        let mut average_row_size = positive_row_size(first.average_row_size);
        let mut confidence = first.confidence;
        let mut union_ndv = 0.0;
        let mut min_ndv = first.distinct_values_count;

        for stat in &stats_for_column {
            min_value = min_value.min(stat.min_value);
            max_value = max_value.max(stat.max_value);
            nulls_fraction = nulls_fraction.max(stat.nulls_fraction);
            average_row_size = average_row_size.max(positive_row_size(stat.average_row_size));
            confidence = confidence.combine(stat.confidence);

            let (next_ndv, _) = sat_add(union_ndv, stat.distinct_values_count);
            union_ndv = next_ndv;
            min_ndv = min_ndv.min(stat.distinct_values_count);
        }

        let raw_ndv = match kind {
            SetOpKind::Union { .. } => union_ndv,
            SetOpKind::Intersect | SetOpKind::Except => min_ndv,
        };
        if missing_child_stat {
            confidence = confidence.combine(Confidence::Fallback);
        }

        merged.insert(
            output_column.column_id,
            ColumnStatistic {
                min_value,
                max_value,
                nulls_fraction,
                average_row_size,
                distinct_values_count: bounded_set_op_ndv(raw_ndv, output_rows),
                confidence,
            },
        );
    }

    merged
}

fn positive_row_size(size: f64) -> f64 {
    if size.is_finite() && size > 0.0 {
        size
    } else {
        8.0
    }
}

fn bounded_set_op_ndv(ndv: f64, output_rows: f64) -> f64 {
    let bounded = if ndv.is_finite() {
        ndv.min(output_rows)
    } else {
        output_rows
    };
    bounded.max(1.0)
}

fn cap_column_ndvs(column_statistics: &mut HashMap<ColumnId, ColumnStatistic>, output_rows: f64) {
    for stat in column_statistics.values_mut() {
        stat.distinct_values_count = cap_ndv_at_rows(stat.distinct_values_count, output_rows);
    }
}

fn positive_set_op_output_rows(rows: f64) -> (f64, bool) {
    if !rows.is_finite() || rows < 1.0 {
        (1.0, true)
    } else {
        (rows, false)
    }
}

fn merge_join_column_statistics(
    left_stats: &Statistics,
    right_stats: &Statistics,
    output_rows: f64,
    row_count_confidence: Confidence,
    join_type: JoinKind,
    eq_key_pairs: &[(ColumnId, ColumnId)],
) -> HashMap<ColumnId, ColumnStatistic> {
    let mut column_statistics = match join_type {
        JoinKind::LeftSemi | JoinKind::LeftAnti | JoinKind::NullAwareLeftAnti => {
            left_stats.column_statistics.clone()
        }
        JoinKind::RightSemi | JoinKind::RightAnti => right_stats.column_statistics.clone(),
        _ => {
            let mut stats = left_stats.column_statistics.clone();
            stats.extend(right_stats.column_statistics.clone());
            stats
        }
    };

    for (left_key, right_key) in eq_key_pairs {
        let Some((left_key, right_key)) =
            orient_join_key_pair(*left_key, *right_key, left_stats, right_stats)
        else {
            continue;
        };
        let left = &left_stats.column_statistics[&left_key];
        let right = &right_stats.column_statistics[&right_key];
        let Some((contained_ndv, confidence)) =
            contained_join_key_ndv(left, right, row_count_confidence)
        else {
            continue;
        };
        if let Some(stat) = column_statistics.get_mut(&left_key) {
            stat.distinct_values_count = contained_ndv;
            stat.confidence = confidence;
        }
        if let Some(stat) = column_statistics.get_mut(&right_key) {
            stat.distinct_values_count = contained_ndv;
            stat.confidence = confidence;
        }
    }

    for stat in column_statistics.values_mut() {
        let capped_ndv = cap_ndv_at_rows(stat.distinct_values_count, output_rows);
        if capped_ndv != stat.distinct_values_count {
            stat.distinct_values_count = capped_ndv;
            stat.confidence = Confidence::derive(&[stat.confidence, row_count_confidence], false);
        }
    }

    column_statistics
}

fn orient_join_key_pair(
    first: ColumnId,
    second: ColumnId,
    left_stats: &Statistics,
    right_stats: &Statistics,
) -> Option<(ColumnId, ColumnId)> {
    let forward = left_stats.column_statistics.contains_key(&first)
        && right_stats.column_statistics.contains_key(&second);
    let reverse = left_stats.column_statistics.contains_key(&second)
        && right_stats.column_statistics.contains_key(&first);
    match (forward, reverse) {
        (true, false) => Some((first, second)),
        (false, true) => Some((second, first)),
        _ => None,
    }
}

fn contained_join_key_ndv(
    left: &ColumnStatistic,
    right: &ColumnStatistic,
    row_count_confidence: Confidence,
) -> Option<(f64, Confidence)> {
    let left_ndv = real_column_ndv(left);
    let right_ndv = real_column_ndv(right);
    let ndv = match (left_ndv, right_ndv) {
        (Some(left), Some(right)) => left.min(right),
        (Some(left), None) => left,
        (None, Some(right)) => right,
        (None, None) => return None,
    };
    let confidence = Confidence::derive(
        &[left.confidence, right.confidence, row_count_confidence],
        left_ndv.is_none() || right_ndv.is_none(),
    );
    Some((ndv, confidence))
}

fn real_column_ndv(stat: &ColumnStatistic) -> Option<f64> {
    if stat.distinct_values_count.is_finite() && stat.distinct_values_count > 1.0 {
        Some(stat.distinct_values_count)
    } else {
        None
    }
}

fn collect_equi_join_column_pairs(condition: Option<&TypedExpr>) -> Vec<(ColumnId, ColumnId)> {
    let mut pairs = Vec::new();
    if let Some(condition) = condition {
        collect_equi_join_column_pairs_inner(condition, &mut pairs);
    }
    pairs
}

fn collect_equi_join_column_pairs_inner(expr: &TypedExpr, pairs: &mut Vec<(ColumnId, ColumnId)>) {
    match &expr.kind {
        ExprKind::BinaryOp {
            left,
            op: BinOp::Eq | BinOp::EqForNull,
            right,
        } => {
            if let (Some(left_id), Some(right_id)) =
                (extract_column_id(left), extract_column_id(right))
            {
                pairs.push((left_id, right_id));
            }
        }
        ExprKind::BinaryOp {
            left,
            op: BinOp::And,
            right,
        } => {
            collect_equi_join_column_pairs_inner(left, pairs);
            collect_equi_join_column_pairs_inner(right, pairs);
        }
        ExprKind::Nested(inner) => collect_equi_join_column_pairs_inner(inner, pairs),
        _ => {}
    }
}

fn derive_window_statistics(
    mut child_stats: Statistics,
    window_exprs: &[crate::sql::planner::plan::WindowExpr],
) -> Statistics {
    for window_expr in window_exprs {
        child_stats
            .column_statistics
            .insert(window_expr.output_column_id, ColumnStatistic::unknown());
    }
    child_stats
}

/// Derive scan statistics from a `LogicalScanOp`.
fn derive_scan(
    scan: &super::operator::LogicalScanOp,
    table_stats: &HashMap<String, TableStatistics>,
) -> Statistics {
    derive_scan_statistics(
        &scan.table.name,
        scan.alias.as_deref(),
        &scan.columns,
        &scan.predicates,
        table_stats,
        estimate_default_row_count(&scan.table.name),
    )
}

fn derive_scan_statistics(
    table_name: &str,
    alias: Option<&str>,
    columns: &[OutputColumn],
    predicates: &[TypedExpr],
    table_stats: &HashMap<String, TableStatistics>,
    default_rows: f64,
) -> Statistics {
    // Try alias first, then fall back to the canonical table name.
    // `collect_scan_stats` inserts by table name, but the scan node
    // may have an alias that differs from the table name.
    let alias_key = alias.map(|a| a.to_lowercase());
    let table_key = table_name.to_lowercase();
    let ts_opt = alias_key
        .as_deref()
        .and_then(|k| table_stats.get(k))
        .or_else(|| table_stats.get(&table_key));

    if let Some(ts) = ts_opt {
        let row_count = ts.row_count.max(1) as f64;

        let mut output_rows = row_count;
        let mut row_count_confidence = Confidence::Exact;
        let table_column_statistics = map_table_column_stats_to_ids(columns, ts);
        for pred in predicates {
            let selectivity = estimate_selectivity(pred, &table_column_statistics);
            (output_rows, row_count_confidence) =
                apply_filter(output_rows, row_count_confidence, selectivity);
        }

        let mut column_statistics: HashMap<ColumnId, ColumnStatistic> = columns
            .iter()
            .map(|c| {
                let col_name = c.name.to_lowercase();
                let cs = ts
                    .column_stats
                    .get(&col_name)
                    .cloned()
                    .unwrap_or_else(ColumnStatistic::unknown);
                (c.column_id, cs)
            })
            .collect();
        cap_column_ndvs(&mut column_statistics, output_rows);

        Statistics {
            output_row_count: output_rows,
            row_count_confidence,
            column_statistics,
        }
    } else {
        // No table stats available: use heuristic defaults based on table name.
        let mut column_statistics: HashMap<ColumnId, ColumnStatistic> = columns
            .iter()
            .map(|c| (c.column_id, ColumnStatistic::unknown()))
            .collect();
        let mut output_rows = default_rows;
        let mut row_count_confidence = Confidence::Fallback;
        for pred in predicates {
            let selectivity = estimate_selectivity(pred, &column_statistics);
            (output_rows, row_count_confidence) =
                apply_filter(output_rows, row_count_confidence, selectivity);
        }
        cap_column_ndvs(&mut column_statistics, output_rows);
        Statistics {
            output_row_count: output_rows,
            row_count_confidence,
            column_statistics,
        }
    }
}

fn map_table_column_stats_to_ids(
    columns: &[OutputColumn],
    table_stats: &TableStatistics,
) -> HashMap<ColumnId, ColumnStatistic> {
    columns
        .iter()
        .map(|column| {
            let stat = table_stats
                .column_stats
                .get(&column.name.to_lowercase())
                .cloned()
                .unwrap_or_else(ColumnStatistic::unknown);
            (column.column_id, stat)
        })
        .collect()
}

/// Heuristic row count estimation for tables without real statistics.
///
/// Large fact tables (containing "sales", "returns", "inventory", etc.)
/// get a larger default, while small dimension tables ("customer_demographics",
/// "date_dim", "time_dim", etc.) get a smaller default. This prevents the
/// optimizer from treating all unknown tables equally, which can lead to
/// bad join ordering.
fn estimate_default_row_count(table_name: &str) -> f64 {
    let name = table_name.to_lowercase();

    // Large fact tables: high row count default.
    const FACT_TABLE_PATTERNS: &[&str] = &[
        "store_sales",
        "web_sales",
        "catalog_sales",
        "store_returns",
        "web_returns",
        "catalog_returns",
        "inventory",
        "lineitem",
        "lineorder",
        "orders",
        "partsupp",
    ];
    for pattern in FACT_TABLE_PATTERNS {
        if name == *pattern || name.ends_with(&format!(".{}", pattern)) {
            return 1_000_000.0;
        }
    }

    // Medium dimension tables: moderate row count default.
    const MEDIUM_TABLE_PATTERNS: &[&str] = &[
        "customer",
        "customer_address",
        "item",
        "web_page",
        "catalog_page",
        "store",
        "promotion",
        "household_demographics",
        "part",
        "supplier",
    ];
    for pattern in MEDIUM_TABLE_PATTERNS {
        if name == *pattern || name.ends_with(&format!(".{}", pattern)) {
            return 100_000.0;
        }
    }

    // Small dimension tables: low row count default.
    const SMALL_TABLE_PATTERNS: &[&str] = &[
        "customer_demographics",
        "date_dim",
        "time_dim",
        "income_band",
        "reason",
        "ship_mode",
        "warehouse",
        "web_site",
        "call_center",
        "nation",
        "region",
    ];
    for pattern in SMALL_TABLE_PATTERNS {
        if name == *pattern || name.ends_with(&format!(".{}", pattern)) {
            return 10_000.0;
        }
    }

    // General heuristic: names containing "fact" or "sales" or "returns"
    // suggest a large table.
    if name.contains("sales")
        || name.contains("returns")
        || name.contains("fact")
        || name.contains("lineitem")
    {
        return 1_000_000.0;
    }
    if name.contains("_dim") || name.contains("dimension") {
        return 10_000.0;
    }

    // Default for completely unknown tables.
    100_000.0
}

/// Derive join statistics from a `LogicalJoinOp` and child stats.
fn derive_join(
    join: &super::operator::LogicalJoinOp,
    left_stats: &Statistics,
    right_stats: &Statistics,
) -> Statistics {
    let join_condition = estimate_join_condition(
        join.condition.as_ref(),
        &left_stats.column_statistics,
        &right_stats.column_statistics,
    );
    let (output_rows, row_count_confidence) = estimate_join_cardinality(&JoinCardInput {
        left: (left_stats.output_row_count, left_stats.row_count_confidence),
        right: (
            right_stats.output_row_count,
            right_stats.row_count_confidence,
        ),
        kind: join.join_type,
        eq_key_ndvs: join_condition.eq_key_ndvs,
        non_equi_selectivity: join_condition.residual_selectivity,
    });

    let column_statistics = merge_join_column_statistics(
        left_stats,
        right_stats,
        output_rows,
        row_count_confidence,
        join.join_type,
        &join_condition.eq_key_pairs,
    );

    Statistics {
        output_row_count: output_rows,
        row_count_confidence,
        column_statistics,
    }
}

/// Widen the nullable flags of `left_cols` and `right_cols` according to the
/// join's outer-join semantics and return the concatenated result.
///
///  - Inner, Cross:        no widening
///  - LeftOuter:           widen right columns to nullable
///  - RightOuter:          widen left columns to nullable
///  - FullOuter:           widen both sides to nullable
///  - LeftSemi, LeftAnti:  only left columns survive; no widening needed
///  - RightSemi, RightAnti: only right columns survive; no widening needed
fn widen_for_join_kind(
    join_type: crate::sql::analysis::JoinKind,
    left_cols: Vec<crate::sql::analysis::OutputColumn>,
    right_cols: Vec<crate::sql::analysis::OutputColumn>,
) -> Vec<crate::sql::analysis::OutputColumn> {
    use crate::sql::analysis::JoinKind::*;
    fn widen(
        cols: Vec<crate::sql::analysis::OutputColumn>,
    ) -> Vec<crate::sql::analysis::OutputColumn> {
        cols.into_iter()
            .map(|mut c| {
                c.nullable = true;
                c
            })
            .collect()
    }
    match join_type {
        Inner | Cross => {
            let mut out = left_cols;
            out.extend(right_cols);
            out
        }
        LeftOuter => {
            let mut out = left_cols;
            out.extend(widen(right_cols));
            out
        }
        RightOuter => {
            let mut out = widen(left_cols);
            out.extend(right_cols);
            out
        }
        FullOuter => {
            let mut out = widen(left_cols);
            out.extend(widen(right_cols));
            out
        }
        LeftSemi | LeftAnti | NullAwareLeftAnti => left_cols,
        RightSemi | RightAnti => right_cols,
    }
}

/// Derive output columns for a group from its first expression.
fn derive_output_columns(memo: &Memo, group_idx: usize) -> Vec<crate::sql::analysis::OutputColumn> {
    let group = &memo.groups[group_idx];
    let expr = group.logical_exprs.first().or(group.physical_exprs.first());

    let Some(expr) = expr else {
        return vec![];
    };

    match &expr.op {
        Operator::LogicalScan(s) => s.columns.clone(),
        Operator::LogicalProject(p) => p
            .items
            .iter()
            .map(|item| crate::sql::analysis::OutputColumn {
                column_id: item.output_column_id,
                name: item.output_name.clone(),
                data_type: item.expr.data_type.clone(),
                nullable: item.expr.nullable,
                is_internal: false,
            })
            .collect(),
        Operator::LogicalAggregate(a) => a.output_columns.clone(),
        Operator::LogicalAggregateStateMerge(a) => a.output_columns.clone(),
        Operator::LogicalWindow(w) => w.output_columns.clone(),
        Operator::LogicalValues(v) => v.columns.clone(),
        // Decode renames dict->string and therefore breaks the
        // child-passthrough invariant the rest of the rename-free
        // operators rely on. Return the operator's stored output_columns
        // so consumers see the post-rename string names.
        Operator::LogicalDecode(d) => d.output_columns.clone(),
        Operator::LogicalCTEAnchor(_) => child_output_columns(memo, &expr.children, 1),
        Operator::LogicalCTEProduce(c) => c.output_columns.clone(),
        Operator::LogicalCTEConsume(c) => c.output_columns.clone(),
        Operator::LogicalGenerateSeries(g) => {
            vec![crate::sql::analysis::OutputColumn {
                column_id: g.output_column_id,
                name: g.column_name.clone(),
                data_type: arrow::datatypes::DataType::Int64,
                nullable: false,
                is_internal: false,
            }]
        }
        Operator::LogicalTableFunction(tf) => {
            let mut cols = child_output_columns(memo, &expr.children, 0);
            cols.extend(tf.output_columns.clone());
            cols
        }

        // Passthrough operators: inherit output columns from first child.
        Operator::LogicalFilter(_)
        | Operator::LogicalSort(_)
        | Operator::LogicalLimit(_)
        | Operator::LogicalTopN(_)
        | Operator::LogicalRepeat(_)
        | Operator::LogicalAssertOneRow(_) => {
            if let Some(&child_id) = expr.children.first() {
                memo.groups[child_id]
                    .logical_props
                    .as_ref()
                    .map(|p| p.output_columns.clone())
                    .unwrap_or_default()
            } else {
                vec![]
            }
        }

        // Join: derive output columns with nullable widening per join kind.
        // SEMI/ANTI return only the surviving side; OUTER widens the null-producing side.
        Operator::LogicalJoin(j) => {
            let left_cols = expr
                .children
                .first()
                .and_then(|&id| memo.groups[id].logical_props.as_ref())
                .map(|p| p.output_columns.clone())
                .unwrap_or_default();
            let right_cols = expr
                .children
                .get(1)
                .and_then(|&id| memo.groups[id].logical_props.as_ref())
                .map(|p| p.output_columns.clone())
                .unwrap_or_default();
            widen_for_join_kind(j.join_type, left_cols, right_cols)
        }

        Operator::LogicalUnion(op) => op.output_columns.clone(),
        Operator::LogicalIntersect(op) => op.output_columns.clone(),
        Operator::LogicalExcept(op) => op.output_columns.clone(),

        // Physical operator counterparts.
        Operator::PhysicalScan(s) => s.columns.clone(),
        Operator::PhysicalProject(p) => p
            .items
            .iter()
            .map(|item| crate::sql::analysis::OutputColumn {
                column_id: item.output_column_id,
                name: item.output_name.clone(),
                data_type: item.expr.data_type.clone(),
                nullable: item.expr.nullable,
                is_internal: false,
            })
            .collect(),
        Operator::PhysicalHashAggregate(a) => a.output_columns.clone(),
        Operator::PhysicalAggregateStateMerge(a) => a.output_columns.clone(),
        Operator::PhysicalWindow(w) => w.output_columns.clone(),
        Operator::PhysicalValues(v) => v.columns.clone(),
        // Decode renames dict->string; see the LogicalDecode arm above.
        Operator::PhysicalDecode(d) => d.output_columns.clone(),
        Operator::PhysicalCTEAnchor(_) => child_output_columns(memo, &expr.children, 1),
        Operator::PhysicalCTEProduce(c) => c.output_columns.clone(),
        Operator::PhysicalCTEConsume(c) => c.output_columns.clone(),
        Operator::PhysicalGenerateSeries(g) => {
            vec![crate::sql::analysis::OutputColumn {
                column_id: g.output_column_id,
                name: g.column_name.clone(),
                data_type: arrow::datatypes::DataType::Int64,
                nullable: false,
                is_internal: false,
            }]
        }
        Operator::PhysicalTableFunction(tf) => {
            let mut cols = child_output_columns(memo, &expr.children, 0);
            cols.extend(tf.output_columns.clone());
            cols
        }
        Operator::PhysicalFilter(_)
        | Operator::PhysicalSort(_)
        | Operator::PhysicalLimit(_)
        | Operator::PhysicalTopN(_)
        | Operator::PhysicalDistribution(_)
        | Operator::PhysicalRepeat(_)
        | Operator::PhysicalAssertOneRow(_) => {
            if let Some(&child_id) = expr.children.first() {
                memo.groups[child_id]
                    .logical_props
                    .as_ref()
                    .map(|p| p.output_columns.clone())
                    .unwrap_or_default()
            } else {
                vec![]
            }
        }
        Operator::PhysicalHashJoin(j) => {
            let left_cols = expr
                .children
                .first()
                .and_then(|&id| memo.groups[id].logical_props.as_ref())
                .map(|p| p.output_columns.clone())
                .unwrap_or_default();
            let right_cols = expr
                .children
                .get(1)
                .and_then(|&id| memo.groups[id].logical_props.as_ref())
                .map(|p| p.output_columns.clone())
                .unwrap_or_default();
            widen_for_join_kind(j.join_type, left_cols, right_cols)
        }
        Operator::PhysicalNestLoopJoin(j) => {
            let left_cols = expr
                .children
                .first()
                .and_then(|&id| memo.groups[id].logical_props.as_ref())
                .map(|p| p.output_columns.clone())
                .unwrap_or_default();
            let right_cols = expr
                .children
                .get(1)
                .and_then(|&id| memo.groups[id].logical_props.as_ref())
                .map(|p| p.output_columns.clone())
                .unwrap_or_default();
            widen_for_join_kind(j.join_type, left_cols, right_cols)
        }
        Operator::PhysicalUnion(op) => op.output_columns.clone(),
        Operator::PhysicalIntersect(op) => op.output_columns.clone(),
        Operator::PhysicalExcept(op) => op.output_columns.clone(),
    }
}

fn child_output_columns(
    memo: &Memo,
    children: &[usize],
    child_idx: usize,
) -> Vec<crate::sql::analysis::OutputColumn> {
    children
        .get(child_idx)
        .and_then(|&child_id| memo.groups[child_id].logical_props.as_ref())
        .map(|props| props.output_columns.clone())
        .unwrap_or_default()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::analysis::{
        ExprKind, JoinKind, LiteralValue, OutputColumn, ProjectItem, TypedExpr,
    };
    use crate::sql::catalog::{
        ColumnDef, IcebergDataFileInfo, IcebergSchemaDef, IcebergTableInfo, ScanSource, TableDef,
    };
    use crate::sql::optimizer::convert::logical_plan_to_memo;
    use crate::sql::optimizer::memo::Memo;
    use crate::sql::planner::plan::*;
    use arrow::datatypes::DataType;

    fn test_iceberg_table_info() -> IcebergTableInfo {
        IcebergTableInfo {
            catalog: "test_catalog".to_string(),
            namespace: "test_db".to_string(),
            table: "test_table".to_string(),
            table_uuid: Some("00000000-0000-0000-0000-000000000001".to_string()),
            current_snapshot_id: Some(7),
            schema_id: 1,
            location: "file:///tmp/test_table".to_string(),
            schema: IcebergSchemaDef { fields: vec![] },
            serialized_metadata: None,
            serialized_metadata_rows: None,
        }
    }

    fn make_table_stats(
        name: &str,
        row_count: u64,
        columns: &[(&str, f64)],
    ) -> (String, TableStatistics) {
        let mut cs = HashMap::new();
        for &(col_name, ndv) in columns {
            cs.insert(
                col_name.to_string(),
                ColumnStatistic {
                    min_value: 0.0,
                    max_value: row_count as f64,
                    nulls_fraction: 0.01,
                    average_row_size: 8.0,
                    distinct_values_count: ndv,
                    confidence: Confidence::Exact,
                },
            );
        }
        (
            name.to_string(),
            TableStatistics {
                row_count,
                column_stats: cs,
            },
        )
    }

    fn test_col_id(name: &str) -> ColumnId {
        let id = match name {
            "id" | "a" | "base" | "k1" | "filter_col" | "l_key" | "l_k1" | "l_filter"
            | "l_orderkey" => 1,
            "status" | "payload" | "rn" | "k2" | "r_key" | "r_k1" | "r_filter" | "o_orderkey" => 2,
            "missing" | "k3" | "l_payload" | "l_k2" | "count(v)" => 3,
            "r_payload" | "r_k2" | "count(*)" => 4,
            "k" => 10,
            other => {
                let mut hash = 2_166_136_261u32;
                for byte in other.bytes() {
                    hash ^= byte as u32;
                    hash = hash.wrapping_mul(16_777_619);
                }
                1_000 + (hash % 100_000)
            }
        };
        ColumnId::new_for_test(id)
    }

    fn stat_by_name<'a>(
        stats: &'a HashMap<ColumnId, ColumnStatistic>,
        name: &str,
    ) -> &'a ColumnStatistic {
        stats.get(&test_col_id(name)).unwrap()
    }

    fn has_stat(stats: &HashMap<ColumnId, ColumnStatistic>, name: &str) -> bool {
        stats.contains_key(&test_col_id(name))
    }

    fn scan_plan(name: &str, cols: &[&str]) -> LogicalPlan {
        let columns: Vec<OutputColumn> = cols
            .iter()
            .map(|c| OutputColumn {
                column_id: test_col_id(c),
                name: c.to_string(),
                data_type: DataType::Int32,
                nullable: false,
                is_internal: false,
            })
            .collect();
        let col_defs: Vec<ColumnDef> = cols
            .iter()
            .map(|c| ColumnDef {
                name: c.to_string(),
                data_type: DataType::Int32,
                nullable: false,
                write_default: None,
                logical_type: None,
            })
            .collect();
        LogicalPlan::Scan(ScanNode {
            database: "db".to_string(),
            table: TableDef {
                name: name.to_string(),
                columns: col_defs,
                iceberg_row_lineage_metadata_columns: vec![],
                source: ScanSource::IcebergDataFiles {
                    table: test_iceberg_table_info(),
                    files: vec![IcebergDataFileInfo {
                        path: format!("s3://bucket/{}.parquet", name),
                        size: 1000,
                        row_count: Some(1000),
                        column_stats: None,
                        partition_spec_id: None,
                        partition_key: None,
                        first_row_id: None,
                        data_sequence_number: None,
                        ivm_change_op: None,
                        included_positions: None,
                        delete_files: vec![],
                        manifest_path: None,
                        partition_values: vec![],
                    }],
                    cloud_properties: Default::default(),
                    binding: crate::sql::catalog::IcebergDataFileBinding::CurrentSnapshot,
                },
            },
            alias: None,
            columns,
            predicates: vec![],
            required_columns: None,
            dict_columns: vec![],
            required_output_columns: None,
        })
    }

    fn scan_plan_with_predicates(
        name: &str,
        cols: &[&str],
        predicates: Vec<TypedExpr>,
    ) -> LogicalPlan {
        let LogicalPlan::Scan(mut node) = scan_plan(name, cols) else {
            unreachable!("scan_plan always returns a Scan");
        };
        node.predicates = predicates;
        LogicalPlan::Scan(node)
    }

    #[test]
    fn fallback_scan_applies_predicate_selectivity() {
        // No table stats registered -> derive_scan takes the heuristic
        // fallback. With the fix, the predicate still reduces the row count.
        let table_stats: HashMap<String, TableStatistics> = HashMap::new();
        let pred = eq_expr(col_ref("a"), int_lit(42));
        let plan = scan_plan_with_predicates("unknown_tbl", &["a"], vec![pred]);

        let mut memo = Memo::new();
        logical_plan_to_memo(&plan, &mut memo);
        derive_group_statistics(&mut memo, &table_stats);

        // default_rows("unknown_tbl") = 100000; unknown-column eq selectivity
        // = PREDICATE_UNKNOWN_FILTER (0.25) -> 100000 * 0.25 = 25000.
        let props = memo.groups[0].logical_props.as_ref().unwrap();
        assert!((props.row_count - 25_000.0).abs() < 1.0);
        assert_eq!(props.row_count_confidence, Confidence::Fallback);
    }

    #[test]
    fn scan_with_table_stats_marks_real_stats_exact_and_missing_columns_fallback() {
        let (name, mut ts) = make_table_stats("orders", 100_000, &[("id", 100_000.0)]);
        ts.column_stats.insert(
            "status".to_string(),
            ColumnStatistic {
                min_value: 0.0,
                max_value: 100_000.0,
                nulls_fraction: 0.01,
                average_row_size: 8.0,
                distinct_values_count: 5.0,
                confidence: Confidence::Estimated,
            },
        );
        let mut table_stats = HashMap::new();
        table_stats.insert(name, ts);
        let plan = scan_plan("orders", &["id", "status", "missing"]);

        let mut memo = Memo::new();
        logical_plan_to_memo(&plan, &mut memo);
        derive_group_statistics(&mut memo, &table_stats);

        let props = memo.groups[0].logical_props.as_ref().unwrap();
        assert!((props.row_count - 100_000.0).abs() < 1.0);
        assert_eq!(props.row_count_confidence, Confidence::Exact);
        assert_eq!(
            stat_by_name(&props.column_statistics, "id").confidence,
            Confidence::Exact
        );
        assert_eq!(
            stat_by_name(&props.column_statistics, "status").confidence,
            Confidence::Estimated
        );
        assert_eq!(
            stat_by_name(&props.column_statistics, "missing").confidence,
            Confidence::Fallback
        );
    }

    #[test]
    fn scan_with_table_stats_and_predicate_downgrades_row_confidence() {
        let (name, ts) = make_table_stats("orders", 100_000, &[("id", 100.0)]);
        let mut table_stats = HashMap::new();
        table_stats.insert(name, ts);
        let plan =
            scan_plan_with_predicates("orders", &["id"], vec![eq_expr(col_ref("id"), int_lit(42))]);

        let mut memo = Memo::new();
        logical_plan_to_memo(&plan, &mut memo);
        derive_group_statistics(&mut memo, &table_stats);

        let props = memo.groups[0].logical_props.as_ref().unwrap();
        assert!((props.row_count - 1_000.0).abs() < 1.0);
        assert_eq!(props.row_count_confidence, Confidence::Estimated);
    }

    #[test]
    fn scan_with_table_stats_and_predicate_caps_ndv_at_output_rows() {
        let (name, ts) = make_table_stats("orders", 1_000, &[("id", 1_000_000.0)]);
        let mut table_stats = HashMap::new();
        table_stats.insert(name, ts);
        let plan =
            scan_plan_with_predicates("orders", &["id"], vec![eq_expr(col_ref("id"), int_lit(42))]);

        let mut memo = Memo::new();
        logical_plan_to_memo(&plan, &mut memo);
        derive_group_statistics(&mut memo, &table_stats);

        let props = memo.groups[0].logical_props.as_ref().unwrap();
        assert_eq!(props.row_count, 1.0);
        assert_eq!(props.row_count_confidence, Confidence::Fallback);
        assert_eq!(
            stat_by_name(&props.column_statistics, "id").distinct_values_count,
            1.0
        );
    }

    #[test]
    fn physical_scan_uses_same_confidence_rules_as_logical_scan() {
        use crate::sql::optimizer::memo::MExpr;
        use crate::sql::optimizer::operator::{Operator, PhysicalScanOp};

        let (name, ts) = make_table_stats("orders", 100_000, &[("id", 100.0)]);
        let mut table_stats = HashMap::new();
        table_stats.insert(name, ts);
        let LogicalPlan::Scan(scan) =
            scan_plan_with_predicates("orders", &["id"], vec![eq_expr(col_ref("id"), int_lit(42))])
        else {
            unreachable!("scan_plan_with_predicates always returns a Scan");
        };
        let memo = Memo::new();
        let expr = MExpr {
            id: memo.next_expr_id(),
            op: Operator::PhysicalScan(PhysicalScanOp {
                database: scan.database,
                table: scan.table,
                alias: scan.alias,
                columns: scan.columns,
                predicates: scan.predicates,
                required_columns: scan.required_columns,
                dict_columns: scan.dict_columns,
                mv_rewritten_from: None,
            }),
            children: vec![],
        };

        let stats = derive_statistics(&expr, &memo, &table_stats);

        assert!((stats.output_row_count - 1_000.0).abs() < 1.0);
        assert_eq!(stats.row_count_confidence, Confidence::Estimated);
        assert_eq!(
            stat_by_name(&stats.column_statistics, "id").confidence,
            Confidence::Exact
        );
    }

    #[test]
    fn child_statistics_preserves_logical_props_row_count_confidence() {
        use crate::sql::optimizer::memo::{LogicalProperties, MExpr};
        use crate::sql::optimizer::operator::{LogicalValuesOp, Operator};

        let mut memo = Memo::new();
        let child = memo.new_group(MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalValues(LogicalValuesOp {
                rows: vec![],
                columns: vec![],
            }),
            children: vec![],
        });
        let mut props = LogicalProperties::new(vec![], 42.0);
        props.row_count_confidence = Confidence::Fallback;
        memo.groups[child].logical_props = Some(props);

        let stats = child_statistics(&memo, &[child], 0);
        assert_eq!(stats.output_row_count, 42.0);
        assert_eq!(stats.row_count_confidence, Confidence::Fallback);
    }

    #[test]
    fn aggregate_stats_are_independent_of_split_stage_metadata() {
        use std::collections::HashMap;

        use crate::sql::analysis::{ExprKind, OutputColumn, TypedExpr};
        use crate::sql::column_id::ColumnId;
        use crate::sql::optimizer::memo::{LogicalProperties, MExpr, Memo};
        use crate::sql::optimizer::operator::{
            AggStage, LogicalAggregateOp, LogicalValuesOp, Operator,
        };
        use crate::sql::optimizer::statistics::ColumnStatistic;
        use crate::sql::planner::plan::AggregateCall;

        fn col_ref(id: u32, name: &str) -> TypedExpr {
            TypedExpr {
                kind: ExprKind::ColumnRef {
                    column_id: ColumnId::new_for_test(id),
                    qualifier: Some("t".to_string()),
                    column: name.to_string(),
                },
                data_type: arrow::datatypes::DataType::Int64,
                nullable: false,
            }
        }

        fn output_column(id: u32, name: &str) -> OutputColumn {
            OutputColumn {
                column_id: ColumnId::new_for_test(id),
                name: name.to_string(),
                data_type: arrow::datatypes::DataType::Int64,
                nullable: false,
                is_internal: false,
            }
        }

        fn count_call() -> AggregateCall {
            AggregateCall {
                name: "count".to_string(),
                args: vec![col_ref(2, "v")],
                distinct: false,
                result_type: arrow::datatypes::DataType::Int64,
                order_by: vec![],
                output_column_id: ColumnId::UNSET,
            }
        }

        fn values_group(memo: &mut Memo) -> usize {
            let id = memo.next_expr_id();
            memo.new_group(MExpr {
                id,
                op: Operator::LogicalValues(LogicalValuesOp {
                    rows: vec![],
                    columns: vec![],
                }),
                children: vec![],
            })
        }

        let mut memo = Memo::new();
        let child_group = values_group(&mut memo);
        let mut child_props = LogicalProperties::new(vec![output_column(1, "k")], 10_000.0);
        child_props.column_statistics.insert(
            ColumnId::new_for_test(1),
            ColumnStatistic {
                min_value: 0.0,
                max_value: 10_000.0,
                nulls_fraction: 0.0,
                average_row_size: 8.0,
                distinct_values_count: 100.0,
                ..Default::default()
            },
        );
        memo.groups[child_group].logical_props = Some(child_props);

        fn aggregate_expr(
            memo: &Memo,
            child_group: usize,
            stage: AggStage,
            is_merge: Vec<bool>,
            is_split: bool,
        ) -> MExpr {
            MExpr {
                id: memo.next_expr_id(),
                op: Operator::LogicalAggregate(LogicalAggregateOp::staged(
                    stage,
                    vec![col_ref(1, "k")],
                    vec![count_call()],
                    vec![output_column(1, "k"), output_column(3, "count(v)")],
                    is_merge,
                    is_split,
                )),
                children: vec![child_group],
            }
        }

        let single = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalAggregate(LogicalAggregateOp::single(
                vec![col_ref(1, "k")],
                vec![count_call()],
                vec![output_column(1, "k"), output_column(3, "count(v)")],
            )),
            children: vec![child_group],
        };
        let local = aggregate_expr(&memo, child_group, AggStage::Local, vec![false], true);
        let global = aggregate_expr(&memo, child_group, AggStage::Global, vec![true], true);
        let global_without_split =
            aggregate_expr(&memo, child_group, AggStage::Global, vec![true], false);

        let table_stats = HashMap::new();
        let single_stats = derive_statistics(&single, &memo, &table_stats);
        for alternative in [&local, &global, &global_without_split] {
            let alternative_stats = derive_statistics(alternative, &memo, &table_stats);
            assert_eq!(
                single_stats.output_row_count,
                alternative_stats.output_row_count
            );
            assert_eq!(
                single_stats.column_statistics.len(),
                alternative_stats.column_statistics.len()
            );
        }
    }

    fn aggregate_expected_three_key_rows() -> f64 {
        100.0 * 100.0_f64.sqrt() * 100.0_f64.powf(0.25)
    }

    fn assert_row_count_close(actual: f64, expected: f64) {
        assert!(
            (actual - expected).abs() < 0.000_001,
            "expected row count {expected}, got {actual}"
        );
    }

    fn stats_output_column(id: u32, name: &str) -> OutputColumn {
        OutputColumn {
            column_id: ColumnId::new_for_test(id),
            name: name.to_string(),
            data_type: DataType::Int64,
            nullable: false,
            is_internal: false,
        }
    }

    fn set_op_column_stat(
        min_value: f64,
        max_value: f64,
        nulls_fraction: f64,
        average_row_size: f64,
        ndv: f64,
        confidence: Confidence,
    ) -> ColumnStatistic {
        ColumnStatistic {
            min_value,
            max_value,
            nulls_fraction,
            average_row_size,
            distinct_values_count: ndv,
            confidence,
        }
    }

    fn set_op_child_group(
        memo: &mut Memo,
        rows: f64,
        row_count_confidence: Confidence,
        output_name: &str,
        stat: ColumnStatistic,
    ) -> usize {
        use crate::sql::optimizer::memo::LogicalProperties;
        use crate::sql::optimizer::operator::{LogicalValuesOp, Operator};

        let group = memo.new_group(MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalValues(LogicalValuesOp {
                rows: vec![],
                columns: vec![],
            }),
            children: vec![],
        });
        let mut props = LogicalProperties::new(vec![stats_output_column(1, output_name)], rows);
        props.row_count_confidence = row_count_confidence;
        props
            .column_statistics
            .insert(ColumnId::new_for_test(1), stat);
        memo.groups[group].logical_props = Some(props);
        group
    }

    fn set_op_child_group_without_column_stat(
        memo: &mut Memo,
        rows: f64,
        row_count_confidence: Confidence,
        output_name: &str,
    ) -> usize {
        use crate::sql::optimizer::memo::LogicalProperties;
        use crate::sql::optimizer::operator::{LogicalValuesOp, Operator};

        let group = memo.new_group(MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalValues(LogicalValuesOp {
                rows: vec![],
                columns: vec![],
            }),
            children: vec![],
        });
        let mut props = LogicalProperties::new(vec![stats_output_column(1, output_name)], rows);
        props.row_count_confidence = row_count_confidence;
        memo.groups[group].logical_props = Some(props);
        group
    }

    #[test]
    fn logical_union_all_saturates_rows_and_caps_merged_column_ndv() {
        use crate::sql::optimizer::estimate::arith::MAX_ROW_COUNT;
        use crate::sql::optimizer::operator::{LogicalUnionOp, Operator};

        let mut memo = Memo::new();
        let left = set_op_child_group(
            &mut memo,
            9.0e14,
            Confidence::Exact,
            "left_k",
            set_op_column_stat(0.0, 20.0, 0.10, 8.0, 8.0e14, Confidence::Exact),
        );
        let right = set_op_child_group(
            &mut memo,
            9.0e14,
            Confidence::Exact,
            "right_k",
            set_op_column_stat(-10.0, 10.0, 0.20, 16.0, 8.0e14, Confidence::Estimated),
        );
        let union = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalUnion(LogicalUnionOp {
                all: true,
                output_columns: vec![stats_output_column(10, "k")],
                child_output_columns: vec![],
            }),
            children: vec![left, right],
        };

        let stats = derive_statistics(&union, &memo, &HashMap::new());

        assert_eq!(stats.output_row_count, MAX_ROW_COUNT);
        assert_eq!(stats.row_count_confidence, Confidence::Fallback);
        let col = stat_by_name(&stats.column_statistics, "k");
        assert_eq!(col.min_value, -10.0);
        assert_eq!(col.max_value, 20.0);
        assert_eq!(col.nulls_fraction, 0.20);
        assert_eq!(col.average_row_size, 16.0);
        assert_eq!(col.distinct_values_count, MAX_ROW_COUNT);
        assert_eq!(col.confidence, Confidence::Estimated);
    }

    #[test]
    fn logical_union_distinct_applies_correlation_and_merges_column_ranges() {
        use crate::sql::optimizer::operator::{LogicalUnionOp, Operator};

        let mut memo = Memo::new();
        let left = set_op_child_group(
            &mut memo,
            100.0,
            Confidence::Exact,
            "left_k",
            set_op_column_stat(5.0, 30.0, 0.01, 8.0, 40.0, Confidence::Exact),
        );
        let right = set_op_child_group(
            &mut memo,
            300.0,
            Confidence::Exact,
            "right_k",
            set_op_column_stat(-5.0, 50.0, 0.15, 12.0, 90.0, Confidence::Estimated),
        );
        let union = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalUnion(LogicalUnionOp {
                all: false,
                output_columns: vec![stats_output_column(10, "k")],
                child_output_columns: vec![],
            }),
            children: vec![left, right],
        };

        let stats = derive_statistics(&union, &memo, &HashMap::new());

        assert_eq!(stats.output_row_count, 300.0);
        assert_eq!(stats.row_count_confidence, Confidence::Estimated);
        let col = stat_by_name(&stats.column_statistics, "k");
        assert_eq!(col.min_value, -5.0);
        assert_eq!(col.max_value, 50.0);
        assert_eq!(col.nulls_fraction, 0.15);
        assert_eq!(col.average_row_size, 12.0);
        assert_eq!(col.distinct_values_count, 130.0);
        assert_eq!(col.confidence, Confidence::Estimated);
    }

    #[test]
    fn logical_union_column_stat_missing_child_degrades_confidence() {
        use crate::sql::optimizer::operator::{LogicalUnionOp, Operator};

        let mut memo = Memo::new();
        let left = set_op_child_group(
            &mut memo,
            100.0,
            Confidence::Exact,
            "left_k",
            set_op_column_stat(5.0, 30.0, 0.01, 8.0, 40.0, Confidence::Exact),
        );
        let right =
            set_op_child_group_without_column_stat(&mut memo, 200.0, Confidence::Exact, "right_k");
        let union = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalUnion(LogicalUnionOp {
                all: true,
                output_columns: vec![stats_output_column(10, "k")],
                child_output_columns: vec![],
            }),
            children: vec![left, right],
        };

        let stats = derive_statistics(&union, &memo, &HashMap::new());

        assert_eq!(stats.output_row_count, 300.0);
        let col = stat_by_name(&stats.column_statistics, "k");
        assert_eq!(col.min_value, 5.0);
        assert_eq!(col.max_value, 30.0);
        assert_eq!(col.distinct_values_count, 40.0);
        assert_eq!(col.confidence, Confidence::Fallback);
    }

    #[test]
    fn logical_intersect_halves_min_rows_and_uses_min_column_ndv() {
        use crate::sql::optimizer::operator::{LogicalIntersectOp, Operator};

        let mut memo = Memo::new();
        let left = set_op_child_group(
            &mut memo,
            1_000.0,
            Confidence::Exact,
            "left_k",
            set_op_column_stat(0.0, 100.0, 0.10, 8.0, 80.0, Confidence::Exact),
        );
        let right = set_op_child_group(
            &mut memo,
            200.0,
            Confidence::Exact,
            "right_k",
            set_op_column_stat(-20.0, 80.0, 0.25, 16.0, 30.0, Confidence::Fallback),
        );
        let intersect = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalIntersect(LogicalIntersectOp {
                output_columns: vec![stats_output_column(10, "k")],
                child_output_columns: vec![],
            }),
            children: vec![left, right],
        };

        let stats = derive_statistics(&intersect, &memo, &HashMap::new());

        assert_eq!(stats.output_row_count, 100.0);
        assert_eq!(stats.row_count_confidence, Confidence::Estimated);
        let col = stat_by_name(&stats.column_statistics, "k");
        assert_eq!(col.min_value, -20.0);
        assert_eq!(col.max_value, 100.0);
        assert_eq!(col.nulls_fraction, 0.25);
        assert_eq!(col.average_row_size, 16.0);
        assert_eq!(col.distinct_values_count, 30.0);
        assert_eq!(col.confidence, Confidence::Fallback);
    }

    #[test]
    fn logical_except_halves_first_rows_and_merges_column_stats_with_min_ndv() {
        use crate::sql::optimizer::operator::{LogicalExceptOp, Operator};

        let mut memo = Memo::new();
        let left = set_op_child_group(
            &mut memo,
            1_000.0,
            Confidence::Exact,
            "left_k",
            set_op_column_stat(10.0, 100.0, 0.05, 8.0, 80.0, Confidence::Exact),
        );
        let right = set_op_child_group(
            &mut memo,
            400.0,
            Confidence::Exact,
            "right_k",
            set_op_column_stat(-10.0, 70.0, 0.20, 16.0, 30.0, Confidence::Estimated),
        );
        let except = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalExcept(LogicalExceptOp {
                output_columns: vec![stats_output_column(10, "k")],
                child_output_columns: vec![],
            }),
            children: vec![left, right],
        };

        let stats = derive_statistics(&except, &memo, &HashMap::new());

        assert_eq!(stats.output_row_count, 500.0);
        assert_eq!(stats.row_count_confidence, Confidence::Estimated);
        let col = stat_by_name(&stats.column_statistics, "k");
        assert_eq!(col.min_value, -10.0);
        assert_eq!(col.max_value, 100.0);
        assert_eq!(col.nulls_fraction, 0.20);
        assert_eq!(col.average_row_size, 16.0);
        assert_eq!(col.distinct_values_count, 30.0);
        assert_eq!(col.confidence, Confidence::Estimated);
    }

    fn aggregate_child_stat(ndv: f64) -> ColumnStatistic {
        ColumnStatistic {
            min_value: 0.0,
            max_value: 1_000_000.0,
            nulls_fraction: 0.0,
            average_row_size: 8.0,
            distinct_values_count: ndv,
            confidence: Confidence::Exact,
        }
    }

    fn aggregate_ndv_child_group(
        memo: &mut Memo,
        row_count: f64,
        row_count_confidence: Confidence,
    ) -> usize {
        use crate::sql::optimizer::memo::LogicalProperties;
        use crate::sql::optimizer::operator::{LogicalValuesOp, Operator};

        let id = memo.next_expr_id();
        let group = memo.new_group(MExpr {
            id,
            op: Operator::LogicalValues(LogicalValuesOp {
                rows: vec![],
                columns: vec![],
            }),
            children: vec![],
        });
        let mut props = LogicalProperties::new(
            vec![
                stats_output_column(1, "k1"),
                stats_output_column(2, "k2"),
                stats_output_column(3, "k3"),
            ],
            row_count,
        );
        props.row_count_confidence = row_count_confidence;
        for name in ["k1", "k2", "k3"] {
            props
                .column_statistics
                .insert(test_col_id(name), aggregate_child_stat(100.0));
        }
        memo.groups[group].logical_props = Some(props);
        group
    }

    fn aggregate_group_keys() -> Vec<TypedExpr> {
        vec![col_ref("k1"), col_ref("k2"), col_ref("k3")]
    }

    fn aggregate_output_columns() -> Vec<OutputColumn> {
        vec![
            stats_output_column(1, "k1"),
            stats_output_column(2, "k2"),
            stats_output_column(3, "k3"),
        ]
    }

    #[test]
    fn logical_aggregate_stats_use_damped_group_ndv_and_child_confidence() {
        use crate::sql::optimizer::operator::{LogicalAggregateOp, Operator};

        let expected = aggregate_expected_three_key_rows();
        assert!(expected < 100.0 * 100.0 * 100.0);

        let mut memo = Memo::new();
        let exact_child = aggregate_ndv_child_group(&mut memo, 1_000_000.0, Confidence::Exact);
        let fallback_child =
            aggregate_ndv_child_group(&mut memo, 1_000_000.0, Confidence::Fallback);

        let exact_agg = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalAggregate(LogicalAggregateOp::single(
                aggregate_group_keys(),
                vec![],
                aggregate_output_columns(),
            )),
            children: vec![exact_child],
        };
        let fallback_agg = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalAggregate(LogicalAggregateOp::single(
                aggregate_group_keys(),
                vec![],
                aggregate_output_columns(),
            )),
            children: vec![fallback_child],
        };

        let exact_stats = derive_statistics(&exact_agg, &memo, &HashMap::new());
        assert_row_count_close(exact_stats.output_row_count, expected);
        assert_eq!(exact_stats.row_count_confidence, Confidence::Estimated);

        let fallback_stats = derive_statistics(&fallback_agg, &memo, &HashMap::new());
        assert_row_count_close(fallback_stats.output_row_count, expected);
        assert_eq!(fallback_stats.row_count_confidence, Confidence::Fallback);
    }

    #[test]
    fn physical_hash_aggregate_stats_use_damped_group_ndv_and_child_confidence() {
        use crate::sql::optimizer::operator::{AggMode, Operator, PhysicalHashAggregateOp};

        let expected = aggregate_expected_three_key_rows();
        assert!(expected < 100.0 * 100.0 * 100.0);

        let mut memo = Memo::new();
        let exact_child = aggregate_ndv_child_group(&mut memo, 1_000_000.0, Confidence::Exact);
        let fallback_child =
            aggregate_ndv_child_group(&mut memo, 1_000_000.0, Confidence::Fallback);

        let exact_agg = MExpr {
            id: memo.next_expr_id(),
            op: Operator::PhysicalHashAggregate(PhysicalHashAggregateOp {
                mode: AggMode::Single,
                group_by: aggregate_group_keys(),
                aggregates: vec![],
                output_columns: aggregate_output_columns(),
                is_merge: vec![],
            }),
            children: vec![exact_child],
        };
        let fallback_agg = MExpr {
            id: memo.next_expr_id(),
            op: Operator::PhysicalHashAggregate(PhysicalHashAggregateOp {
                mode: AggMode::Single,
                group_by: aggregate_group_keys(),
                aggregates: vec![],
                output_columns: aggregate_output_columns(),
                is_merge: vec![],
            }),
            children: vec![fallback_child],
        };

        let exact_stats = derive_statistics(&exact_agg, &memo, &HashMap::new());
        assert_row_count_close(exact_stats.output_row_count, expected);
        assert_eq!(exact_stats.row_count_confidence, Confidence::Estimated);

        let fallback_stats = derive_statistics(&fallback_agg, &memo, &HashMap::new());
        assert_row_count_close(fallback_stats.output_row_count, expected);
        assert_eq!(fallback_stats.row_count_confidence, Confidence::Fallback);
    }

    #[test]
    fn grouped_aggregate_preserves_group_key_column_statistics() {
        use crate::sql::optimizer::operator::{
            AggMode, LogicalAggregateOp, Operator, PhysicalHashAggregateOp,
        };

        let expected = aggregate_expected_three_key_rows();
        let mut memo = Memo::new();
        let child = aggregate_ndv_child_group(&mut memo, 1_000_000.0, Confidence::Fallback);

        let logical = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalAggregate(LogicalAggregateOp::single(
                aggregate_group_keys(),
                vec![],
                aggregate_output_columns(),
            )),
            children: vec![child],
        };
        let physical = MExpr {
            id: memo.next_expr_id(),
            op: Operator::PhysicalHashAggregate(PhysicalHashAggregateOp {
                mode: AggMode::Single,
                group_by: aggregate_group_keys(),
                aggregates: vec![],
                output_columns: aggregate_output_columns(),
                is_merge: vec![],
            }),
            children: vec![child],
        };

        for stats in [
            derive_statistics(&logical, &memo, &HashMap::new()),
            derive_statistics(&physical, &memo, &HashMap::new()),
        ] {
            assert_row_count_close(stats.output_row_count, expected);
            assert_eq!(
                stat_by_name(&stats.column_statistics, "k1").distinct_values_count,
                100.0
            );
            assert_eq!(
                stat_by_name(&stats.column_statistics, "k2").distinct_values_count,
                100.0
            );
            assert_eq!(
                stat_by_name(&stats.column_statistics, "k3").distinct_values_count,
                100.0
            );
        }
    }

    #[test]
    fn aggregate_stats_empty_group_by_keeps_scalar_behavior() {
        use crate::sql::optimizer::operator::{
            AggMode, LogicalAggregateOp, Operator, PhysicalHashAggregateOp,
        };

        let mut memo = Memo::new();
        let child = aggregate_ndv_child_group(&mut memo, 1_000_000.0, Confidence::Fallback);
        let logical = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalAggregate(LogicalAggregateOp::single(
                vec![],
                vec![],
                vec![stats_output_column(4, "count(*)")],
            )),
            children: vec![child],
        };
        let physical = MExpr {
            id: memo.next_expr_id(),
            op: Operator::PhysicalHashAggregate(PhysicalHashAggregateOp {
                mode: AggMode::Single,
                group_by: vec![],
                aggregates: vec![],
                output_columns: vec![stats_output_column(4, "count(*)")],
                is_merge: vec![],
            }),
            children: vec![child],
        };

        let logical_stats = derive_statistics(&logical, &memo, &HashMap::new());
        assert_eq!(logical_stats.output_row_count, 1.0);
        assert_eq!(logical_stats.row_count_confidence, Confidence::Estimated);
        assert!(logical_stats.column_statistics.is_empty());

        let physical_stats = derive_statistics(&physical, &memo, &HashMap::new());
        assert_eq!(physical_stats.output_row_count, 1.0);
        assert_eq!(physical_stats.row_count_confidence, Confidence::Estimated);
        assert!(physical_stats.column_statistics.is_empty());
    }

    fn col_ref(name: &str) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: test_col_id(name),
                qualifier: None,
                column: name.to_string(),
            },
            data_type: DataType::Int32,
            nullable: false,
        }
    }

    fn int_lit(v: i64) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::Literal(crate::sql::analysis::LiteralValue::Int(v)),
            data_type: DataType::Int64,
            nullable: false,
        }
    }

    fn eq_expr(left: TypedExpr, right: TypedExpr) -> TypedExpr {
        TypedExpr {
            data_type: DataType::Boolean,
            nullable: false,
            kind: ExprKind::BinaryOp {
                left: Box::new(left),
                op: crate::sql::analysis::BinOp::Eq,
                right: Box::new(right),
            },
        }
    }

    fn and_expr(left: TypedExpr, right: TypedExpr) -> TypedExpr {
        TypedExpr {
            data_type: DataType::Boolean,
            nullable: false,
            kind: ExprKind::BinaryOp {
                left: Box::new(left),
                op: crate::sql::analysis::BinOp::And,
                right: Box::new(right),
            },
        }
    }

    fn nested_expr(expr: TypedExpr) -> TypedExpr {
        TypedExpr {
            data_type: expr.data_type.clone(),
            nullable: expr.nullable,
            kind: ExprKind::Nested(Box::new(expr)),
        }
    }

    fn filter_ndv_child_stat(ndv: f64) -> ColumnStatistic {
        ColumnStatistic {
            min_value: 7.0,
            max_value: 77.0,
            nulls_fraction: 0.0,
            average_row_size: 8.0,
            distinct_values_count: ndv,
            confidence: Confidence::Exact,
        }
    }

    fn filter_ndv_child_group(memo: &mut Memo) -> usize {
        use crate::sql::optimizer::memo::LogicalProperties;
        use crate::sql::optimizer::operator::{LogicalValuesOp, Operator};

        let group = memo.new_group(MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalValues(LogicalValuesOp {
                rows: vec![],
                columns: vec![],
            }),
            children: vec![],
        });
        let mut props = LogicalProperties::new(vec![], 1_000.0);
        props.row_count_confidence = Confidence::Exact;
        props
            .column_statistics
            .insert(test_col_id("filter_col"), filter_ndv_child_stat(10.0));
        props
            .column_statistics
            .insert(test_col_id("payload"), filter_ndv_child_stat(1_000.0));
        memo.groups[group].logical_props = Some(props);
        group
    }

    fn assert_filter_caps_payload_ndv(stats: Statistics) {
        assert!(
            (stats.output_row_count - 100.0).abs() < 0.000_001,
            "expected filter output rows 100, got {}",
            stats.output_row_count
        );
        assert_eq!(stats.row_count_confidence, Confidence::Estimated);
        let payload = stat_by_name(&stats.column_statistics, "payload");
        assert_eq!(payload.distinct_values_count, 100.0);
        assert_eq!(payload.min_value, 7.0);
        assert_eq!(payload.max_value, 77.0);
    }

    #[test]
    fn logical_filter_caps_payload_ndv_at_output_rows() {
        use crate::sql::optimizer::operator::{LogicalFilterOp, Operator};

        let mut memo = Memo::new();
        let child = filter_ndv_child_group(&mut memo);
        let filter = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalFilter(LogicalFilterOp {
                predicate: eq_expr(col_ref("filter_col"), int_lit(1)),
            }),
            children: vec![child],
        };

        assert_filter_caps_payload_ndv(derive_statistics(&filter, &memo, &HashMap::new()));
    }

    #[test]
    fn physical_filter_caps_payload_ndv_at_output_rows() {
        use crate::sql::optimizer::operator::{Operator, PhysicalFilterOp};

        let mut memo = Memo::new();
        let child = filter_ndv_child_group(&mut memo);
        let filter = MExpr {
            id: memo.next_expr_id(),
            op: Operator::PhysicalFilter(PhysicalFilterOp {
                predicate: eq_expr(col_ref("filter_col"), int_lit(1)),
            }),
            children: vec![child],
        };

        assert_filter_caps_payload_ndv(derive_statistics(&filter, &memo, &HashMap::new()));
    }

    #[test]
    fn scan_group_stats() {
        let (name, ts) = make_table_stats("orders", 100_000, &[("id", 100_000.0)]);
        let mut table_stats = HashMap::new();
        table_stats.insert(name, ts);

        let plan = scan_plan("orders", &["id"]);
        let mut memo = Memo::new();
        logical_plan_to_memo(&plan, &mut memo);
        derive_group_statistics(&mut memo, &table_stats);

        let props = memo.groups[0].logical_props.as_ref().unwrap();
        assert!((props.row_count - 100_000.0).abs() < 1.0);
        assert_eq!(props.output_columns.len(), 1);
        assert_eq!(props.output_columns[0].name, "id");
    }

    #[test]
    fn filter_group_stats() {
        let (name, ts) = make_table_stats("t", 10_000, &[("a", 100.0)]);
        let mut table_stats = HashMap::new();
        table_stats.insert(name, ts);

        let scan = scan_plan("t", &["a"]);
        let pred = eq_expr(col_ref("a"), int_lit(42));
        let plan = LogicalPlan::Filter(FilterNode {
            input: Box::new(scan),
            predicate: pred,
            required_output_columns: None,
        });

        let mut memo = Memo::new();
        logical_plan_to_memo(&plan, &mut memo);
        derive_group_statistics(&mut memo, &table_stats);

        // Scan group (0): 10000 rows
        let scan_props = memo.groups[0].logical_props.as_ref().unwrap();
        assert!((scan_props.row_count - 10_000.0).abs() < 1.0);

        // Filter group (1): with column stats now flowing through
        // child_statistics, `a = 42` uses real NDV(a)=100 -> selectivity
        // 1/100 = 0.01 -> 10000 * 0.01 = 100 rows.
        let filter_props = memo.groups[1].logical_props.as_ref().unwrap();
        assert!((filter_props.row_count - 100.0).abs() < 1.0);
    }

    #[test]
    fn physical_hash_join_stats_use_shared_cardinality_estimator() {
        use crate::sql::optimizer::memo::{LogicalProperties, MExpr};
        use crate::sql::optimizer::operator::{
            JoinDistribution, LogicalValuesOp, Operator, PhysicalHashJoinEqCondition,
            PhysicalHashJoinOp,
        };

        fn column_stat(ndv: f64) -> ColumnStatistic {
            ColumnStatistic {
                min_value: 0.0,
                max_value: 1_000.0,
                nulls_fraction: 0.0,
                average_row_size: 8.0,
                distinct_values_count: ndv,
                confidence: Confidence::Exact,
            }
        }

        fn values_group(memo: &mut Memo, stats: &[(&str, f64)]) -> usize {
            let id = memo.next_expr_id();
            let group = memo.new_group(MExpr {
                id,
                op: Operator::LogicalValues(LogicalValuesOp {
                    rows: vec![],
                    columns: vec![],
                }),
                children: vec![],
            });
            let mut props = LogicalProperties::new(vec![], 1_000.0);
            props.row_count_confidence = Confidence::Exact;
            for &(name, ndv) in stats {
                props
                    .column_statistics
                    .insert(test_col_id(name), column_stat(ndv));
            }
            memo.groups[group].logical_props = Some(props);
            group
        }

        let mut memo = Memo::new();
        let left = values_group(&mut memo, &[("l_k1", 100.0), ("l_k2", 100.0)]);
        let right = values_group(&mut memo, &[("r_k1", 100.0), ("r_k2", 100.0)]);
        let join = MExpr {
            id: memo.next_expr_id(),
            op: Operator::PhysicalHashJoin(PhysicalHashJoinOp {
                join_type: JoinKind::Inner,
                eq_conditions: vec![
                    PhysicalHashJoinEqCondition {
                        left: col_ref("l_k1"),
                        right: col_ref("r_k1"),
                        null_safe: false,
                    },
                    PhysicalHashJoinEqCondition {
                        left: col_ref("l_k2"),
                        right: col_ref("r_k2"),
                        null_safe: false,
                    },
                ],
                other_condition: None,
                distribution: JoinDistribution::Broadcast,
            }),
            children: vec![left, right],
        };

        let stats = derive_statistics(&join, &memo, &HashMap::new());

        assert!(
            (stats.output_row_count - 1_000.0).abs() < 1.0,
            "expected shared estimator output, got {}",
            stats.output_row_count
        );
        assert_eq!(stats.row_count_confidence, Confidence::Estimated);
        assert!(has_stat(&stats.column_statistics, "l_k1"));
        assert!(has_stat(&stats.column_statistics, "r_k1"));
    }

    #[test]
    fn physical_hash_join_caps_output_ndv_and_merges_key_equivalence() {
        use crate::sql::optimizer::memo::{LogicalProperties, MExpr};
        use crate::sql::optimizer::operator::{
            JoinDistribution, LogicalValuesOp, Operator, PhysicalHashJoinEqCondition,
            PhysicalHashJoinOp,
        };

        fn column_stat(ndv: f64) -> ColumnStatistic {
            ColumnStatistic {
                min_value: 0.0,
                max_value: 1_000.0,
                nulls_fraction: 0.0,
                average_row_size: 8.0,
                distinct_values_count: ndv,
                confidence: Confidence::Exact,
            }
        }

        fn values_group(memo: &mut Memo, rows: f64, stats: &[(&str, f64)]) -> usize {
            let group = memo.new_group(MExpr {
                id: memo.next_expr_id(),
                op: Operator::LogicalValues(LogicalValuesOp {
                    rows: vec![],
                    columns: vec![],
                }),
                children: vec![],
            });
            let mut props = LogicalProperties::new(vec![], rows);
            props.row_count_confidence = Confidence::Exact;
            for &(name, ndv) in stats {
                props
                    .column_statistics
                    .insert(test_col_id(name), column_stat(ndv));
            }
            memo.groups[group].logical_props = Some(props);
            group
        }

        let mut memo = Memo::new();
        let left = values_group(
            &mut memo,
            100.0,
            &[("l_key", 100.0), ("l_payload", 1_000.0)],
        );
        let right = values_group(&mut memo, 40.0, &[("r_key", 20.0), ("r_payload", 500.0)]);
        let join = MExpr {
            id: memo.next_expr_id(),
            op: Operator::PhysicalHashJoin(PhysicalHashJoinOp {
                join_type: JoinKind::Inner,
                eq_conditions: vec![PhysicalHashJoinEqCondition {
                    left: col_ref("l_key"),
                    right: col_ref("r_key"),
                    null_safe: false,
                }],
                other_condition: None,
                distribution: JoinDistribution::Broadcast,
            }),
            children: vec![left, right],
        };

        let stats = derive_statistics(&join, &memo, &HashMap::new());

        assert_eq!(stats.output_row_count, 40.0);
        assert_eq!(
            stat_by_name(&stats.column_statistics, "l_key").distinct_values_count,
            20.0
        );
        assert_eq!(
            stat_by_name(&stats.column_statistics, "r_key").distinct_values_count,
            20.0
        );
        assert_eq!(
            stat_by_name(&stats.column_statistics, "l_payload").distinct_values_count,
            40.0
        );
        assert_eq!(
            stat_by_name(&stats.column_statistics, "r_payload").distinct_values_count,
            40.0
        );
    }

    #[test]
    fn logical_join_stats_use_shared_cardinality_estimator_for_condition() {
        use crate::sql::optimizer::memo::{LogicalProperties, MExpr};
        use crate::sql::optimizer::operator::{LogicalJoinOp, LogicalValuesOp, Operator};

        fn column_stat(ndv: f64) -> ColumnStatistic {
            ColumnStatistic {
                min_value: 0.0,
                max_value: 1_000.0,
                nulls_fraction: 0.0,
                average_row_size: 8.0,
                distinct_values_count: ndv,
                confidence: Confidence::Exact,
            }
        }

        fn values_group(memo: &mut Memo, rows: f64, stats: &[(&str, f64)]) -> usize {
            let id = memo.next_expr_id();
            let group = memo.new_group(MExpr {
                id,
                op: Operator::LogicalValues(LogicalValuesOp {
                    rows: vec![],
                    columns: vec![],
                }),
                children: vec![],
            });
            let mut props = LogicalProperties::new(vec![], rows);
            props.row_count_confidence = Confidence::Exact;
            for &(name, ndv) in stats {
                props
                    .column_statistics
                    .insert(test_col_id(name), column_stat(ndv));
            }
            memo.groups[group].logical_props = Some(props);
            group
        }

        let mut memo = Memo::new();
        let left = values_group(&mut memo, 1_000.0, &[("l_key", 100.0)]);
        let right = values_group(&mut memo, 50.0, &[("r_key", 50.0)]);
        let join = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalJoin(LogicalJoinOp {
                join_type: JoinKind::Inner,
                condition: Some(eq_expr(col_ref("l_key"), col_ref("r_key"))),
            }),
            children: vec![left, right],
        };

        let stats = derive_statistics(&join, &memo, &HashMap::new());

        assert!(
            (stats.output_row_count - 500.0).abs() < 1.0,
            "expected shared estimator output, got {}",
            stats.output_row_count
        );
        assert_eq!(stats.row_count_confidence, Confidence::Estimated);
        assert!(has_stat(&stats.column_statistics, "l_key"));
        assert!(has_stat(&stats.column_statistics, "r_key"));
    }

    #[test]
    fn logical_join_condition_merges_key_equivalence_and_caps_payload_ndv() {
        use crate::sql::optimizer::memo::{LogicalProperties, MExpr};
        use crate::sql::optimizer::operator::{LogicalJoinOp, LogicalValuesOp, Operator};

        fn column_stat(ndv: f64) -> ColumnStatistic {
            ColumnStatistic {
                min_value: 0.0,
                max_value: 1_000.0,
                nulls_fraction: 0.0,
                average_row_size: 8.0,
                distinct_values_count: ndv,
                confidence: Confidence::Exact,
            }
        }

        fn values_group(memo: &mut Memo, rows: f64, stats: &[(&str, f64)]) -> usize {
            let group = memo.new_group(MExpr {
                id: memo.next_expr_id(),
                op: Operator::LogicalValues(LogicalValuesOp {
                    rows: vec![],
                    columns: vec![],
                }),
                children: vec![],
            });
            let mut props = LogicalProperties::new(vec![], rows);
            props.row_count_confidence = Confidence::Exact;
            for &(name, ndv) in stats {
                props
                    .column_statistics
                    .insert(test_col_id(name), column_stat(ndv));
            }
            memo.groups[group].logical_props = Some(props);
            group
        }

        let mut memo = Memo::new();
        let left = values_group(
            &mut memo,
            10_000.0,
            &[("l_key", 100.0), ("l_payload", 1_000_000.0)],
        );
        let right = values_group(
            &mut memo,
            4_000.0,
            &[("r_key", 20.0), ("r_payload", 500_000.0)],
        );
        let join = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalJoin(LogicalJoinOp {
                join_type: JoinKind::Inner,
                condition: Some(eq_expr(col_ref("l_key"), col_ref("r_key"))),
            }),
            children: vec![left, right],
        };

        let stats = derive_statistics(&join, &memo, &HashMap::new());

        assert_eq!(stats.output_row_count, 400_000.0);
        assert_eq!(
            stat_by_name(&stats.column_statistics, "l_key").distinct_values_count,
            20.0
        );
        assert_eq!(
            stat_by_name(&stats.column_statistics, "r_key").distinct_values_count,
            20.0
        );
        assert_eq!(
            stat_by_name(&stats.column_statistics, "l_payload").distinct_values_count,
            400_000.0
        );
        assert_eq!(
            stat_by_name(&stats.column_statistics, "r_payload").distinct_values_count,
            400_000.0
        );
    }

    #[test]
    fn logical_join_applies_only_residual_non_equi_selectivity() {
        use crate::sql::optimizer::memo::{LogicalProperties, MExpr};
        use crate::sql::optimizer::operator::{LogicalJoinOp, LogicalValuesOp, Operator};

        fn column_stat(ndv: f64) -> ColumnStatistic {
            ColumnStatistic {
                min_value: 0.0,
                max_value: 1_000.0,
                nulls_fraction: 0.0,
                average_row_size: 8.0,
                distinct_values_count: ndv,
                confidence: Confidence::Exact,
            }
        }

        fn values_group(memo: &mut Memo, rows: f64, stats: &[(&str, f64)]) -> usize {
            let group = memo.new_group(MExpr {
                id: memo.next_expr_id(),
                op: Operator::LogicalValues(LogicalValuesOp {
                    rows: vec![],
                    columns: vec![],
                }),
                children: vec![],
            });
            let mut props = LogicalProperties::new(vec![], rows);
            props.row_count_confidence = Confidence::Exact;
            for &(name, ndv) in stats {
                props
                    .column_statistics
                    .insert(test_col_id(name), column_stat(ndv));
            }
            memo.groups[group].logical_props = Some(props);
            group
        }

        let mut memo = Memo::new();
        let left = values_group(
            &mut memo,
            1_000.0,
            &[("l_key", 100.0), ("l_payload", 200.0)],
        );
        let right = values_group(&mut memo, 100.0, &[("r_key", 100.0)]);
        let condition = and_expr(
            eq_expr(col_ref("l_key"), col_ref("r_key")),
            eq_expr(col_ref("l_payload"), int_lit(7)),
        );
        let join = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalJoin(LogicalJoinOp {
                join_type: JoinKind::Inner,
                condition: Some(condition),
            }),
            children: vec![left, right],
        };

        let stats = derive_statistics(&join, &memo, &HashMap::new());

        assert_eq!(stats.output_row_count, 5.0);
    }

    #[test]
    fn logical_join_reversed_condition_merges_key_equivalence() {
        use crate::sql::optimizer::memo::{LogicalProperties, MExpr};
        use crate::sql::optimizer::operator::{LogicalJoinOp, LogicalValuesOp, Operator};

        fn column_stat(ndv: f64) -> ColumnStatistic {
            ColumnStatistic {
                min_value: 0.0,
                max_value: 1_000.0,
                nulls_fraction: 0.0,
                average_row_size: 8.0,
                distinct_values_count: ndv,
                confidence: Confidence::Exact,
            }
        }

        fn values_group(memo: &mut Memo, rows: f64, stats: &[(&str, f64)]) -> usize {
            let group = memo.new_group(MExpr {
                id: memo.next_expr_id(),
                op: Operator::LogicalValues(LogicalValuesOp {
                    rows: vec![],
                    columns: vec![],
                }),
                children: vec![],
            });
            let mut props = LogicalProperties::new(vec![], rows);
            props.row_count_confidence = Confidence::Exact;
            for &(name, ndv) in stats {
                props
                    .column_statistics
                    .insert(test_col_id(name), column_stat(ndv));
            }
            memo.groups[group].logical_props = Some(props);
            group
        }

        let mut memo = Memo::new();
        let left = values_group(&mut memo, 10_000.0, &[("l_key", 100.0)]);
        let right = values_group(&mut memo, 4_000.0, &[("r_key", 20.0)]);
        let join = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalJoin(LogicalJoinOp {
                join_type: JoinKind::Inner,
                condition: Some(eq_expr(col_ref("r_key"), col_ref("l_key"))),
            }),
            children: vec![left, right],
        };

        let stats = derive_statistics(&join, &memo, &HashMap::new());

        assert_eq!(
            stat_by_name(&stats.column_statistics, "l_key").distinct_values_count,
            20.0
        );
        assert_eq!(
            stat_by_name(&stats.column_statistics, "r_key").distinct_values_count,
            20.0
        );
    }

    #[test]
    fn logical_join_nested_and_condition_merges_multiple_key_pairs() {
        use crate::sql::optimizer::memo::{LogicalProperties, MExpr};
        use crate::sql::optimizer::operator::{LogicalJoinOp, LogicalValuesOp, Operator};

        fn column_stat(ndv: f64) -> ColumnStatistic {
            ColumnStatistic {
                min_value: 0.0,
                max_value: 1_000.0,
                nulls_fraction: 0.0,
                average_row_size: 8.0,
                distinct_values_count: ndv,
                confidence: Confidence::Exact,
            }
        }

        fn values_group(memo: &mut Memo, rows: f64, stats: &[(&str, f64)]) -> usize {
            let group = memo.new_group(MExpr {
                id: memo.next_expr_id(),
                op: Operator::LogicalValues(LogicalValuesOp {
                    rows: vec![],
                    columns: vec![],
                }),
                children: vec![],
            });
            let mut props = LogicalProperties::new(vec![], rows);
            props.row_count_confidence = Confidence::Exact;
            for &(name, ndv) in stats {
                props
                    .column_statistics
                    .insert(test_col_id(name), column_stat(ndv));
            }
            memo.groups[group].logical_props = Some(props);
            group
        }

        let mut memo = Memo::new();
        let left = values_group(&mut memo, 10_000.0, &[("l_k1", 100.0), ("l_k2", 500.0)]);
        let right = values_group(&mut memo, 4_000.0, &[("r_k1", 20.0), ("r_k2", 50.0)]);
        let condition = nested_expr(and_expr(
            nested_expr(eq_expr(col_ref("l_k1"), col_ref("r_k1"))),
            nested_expr(eq_expr(col_ref("l_k2"), col_ref("r_k2"))),
        ));
        let join = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalJoin(LogicalJoinOp {
                join_type: JoinKind::Inner,
                condition: Some(condition),
            }),
            children: vec![left, right],
        };

        let stats = derive_statistics(&join, &memo, &HashMap::new());

        assert_eq!(stats.output_row_count, 8_000.0);
        assert_eq!(
            stat_by_name(&stats.column_statistics, "l_k1").distinct_values_count,
            20.0
        );
        assert_eq!(
            stat_by_name(&stats.column_statistics, "r_k1").distinct_values_count,
            20.0
        );
        assert_eq!(
            stat_by_name(&stats.column_statistics, "l_k2").distinct_values_count,
            50.0
        );
        assert_eq!(
            stat_by_name(&stats.column_statistics, "r_k2").distinct_values_count,
            50.0
        );
    }

    #[test]
    fn logical_join_unknown_key_ndv_does_not_collapse_real_side_to_one() {
        use crate::sql::optimizer::memo::{LogicalProperties, MExpr};
        use crate::sql::optimizer::operator::{LogicalJoinOp, LogicalValuesOp, Operator};

        fn values_group(memo: &mut Memo, rows: f64, name: &str, stat: ColumnStatistic) -> usize {
            let group = memo.new_group(MExpr {
                id: memo.next_expr_id(),
                op: Operator::LogicalValues(LogicalValuesOp {
                    rows: vec![],
                    columns: vec![],
                }),
                children: vec![],
            });
            let mut props = LogicalProperties::new(vec![], rows);
            props.row_count_confidence = Confidence::Exact;
            props.column_statistics.insert(test_col_id(name), stat);
            memo.groups[group].logical_props = Some(props);
            group
        }

        let real_key = ColumnStatistic {
            min_value: 0.0,
            max_value: 1_000.0,
            nulls_fraction: 0.0,
            average_row_size: 8.0,
            distinct_values_count: 100.0,
            confidence: Confidence::Exact,
        };

        let mut memo = Memo::new();
        let left = values_group(&mut memo, 10_000.0, "l_key", real_key);
        let right = values_group(&mut memo, 4_000.0, "r_key", ColumnStatistic::unknown());
        let join = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalJoin(LogicalJoinOp {
                join_type: JoinKind::Inner,
                condition: Some(eq_expr(col_ref("l_key"), col_ref("r_key"))),
            }),
            children: vec![left, right],
        };

        let stats = derive_statistics(&join, &memo, &HashMap::new());

        assert_eq!(
            stat_by_name(&stats.column_statistics, "l_key").distinct_values_count,
            100.0
        );
        assert_eq!(
            stat_by_name(&stats.column_statistics, "r_key").distinct_values_count,
            100.0
        );
        assert_eq!(
            stat_by_name(&stats.column_statistics, "l_key").confidence,
            Confidence::Fallback
        );
        assert_eq!(
            stat_by_name(&stats.column_statistics, "r_key").confidence,
            Confidence::Fallback
        );
    }

    #[test]
    fn p4_self_join_same_name_columns_keep_distinct_statistics() {
        use crate::sql::optimizer::memo::{LogicalProperties, MExpr};
        use crate::sql::optimizer::operator::{LogicalJoinOp, LogicalValuesOp, Operator};

        fn column_stat(ndv: f64) -> ColumnStatistic {
            ColumnStatistic {
                min_value: 0.0,
                max_value: 1_000.0,
                nulls_fraction: 0.0,
                average_row_size: 8.0,
                distinct_values_count: ndv,
                confidence: Confidence::Exact,
            }
        }

        fn col_ref_with_id(column_id: ColumnId) -> TypedExpr {
            TypedExpr {
                kind: ExprKind::ColumnRef {
                    column_id,
                    qualifier: None,
                    column: "id".to_string(),
                },
                data_type: DataType::Int32,
                nullable: false,
            }
        }

        fn values_group(memo: &mut Memo, rows: f64, column_id: ColumnId, ndv: f64) -> usize {
            let group = memo.new_group(MExpr {
                id: memo.next_expr_id(),
                op: Operator::LogicalValues(LogicalValuesOp {
                    rows: vec![],
                    columns: vec![],
                }),
                children: vec![],
            });
            let mut props = LogicalProperties::new(vec![], rows);
            props.row_count_confidence = Confidence::Exact;
            props.column_statistics.insert(column_id, column_stat(ndv));
            memo.groups[group].logical_props = Some(props);
            group
        }

        let left_id = ColumnId::new_for_test(101);
        let right_id = ColumnId::new_for_test(201);
        let mut memo = Memo::new();
        let left = values_group(&mut memo, 10_000.0, left_id, 100.0);
        let right = values_group(&mut memo, 4_000.0, right_id, 20.0);
        let join = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalJoin(LogicalJoinOp {
                join_type: JoinKind::Inner,
                condition: Some(eq_expr(col_ref_with_id(left_id), col_ref_with_id(right_id))),
            }),
            children: vec![left, right],
        };

        let stats = derive_statistics(&join, &memo, &HashMap::new());

        assert_eq!(stats.output_row_count, 400_000.0);
        assert_eq!(
            stats.column_statistics[&left_id].distinct_values_count,
            20.0
        );
        assert_eq!(
            stats.column_statistics[&right_id].distinct_values_count,
            20.0
        );
    }

    #[test]
    fn physical_nest_loop_join_stats_use_shared_cardinality_estimator_for_non_equi_semi() {
        use crate::sql::optimizer::memo::{LogicalProperties, MExpr};
        use crate::sql::optimizer::operator::{LogicalValuesOp, Operator, PhysicalNestLoopJoinOp};

        fn column_stat(ndv: f64) -> ColumnStatistic {
            ColumnStatistic {
                min_value: 0.0,
                max_value: 1_000.0,
                nulls_fraction: 0.0,
                average_row_size: 8.0,
                distinct_values_count: ndv,
                confidence: Confidence::Exact,
            }
        }

        fn values_group(memo: &mut Memo, rows: f64, stats: &[(&str, f64)]) -> usize {
            let id = memo.next_expr_id();
            let group = memo.new_group(MExpr {
                id,
                op: Operator::LogicalValues(LogicalValuesOp {
                    rows: vec![],
                    columns: vec![],
                }),
                children: vec![],
            });
            let mut props = LogicalProperties::new(vec![], rows);
            props.row_count_confidence = Confidence::Exact;
            for &(name, ndv) in stats {
                props
                    .column_statistics
                    .insert(test_col_id(name), column_stat(ndv));
            }
            memo.groups[group].logical_props = Some(props);
            group
        }

        let mut memo = Memo::new();
        let left = values_group(&mut memo, 1_000.0, &[("l_filter", 100.0)]);
        let right = values_group(&mut memo, 50.0, &[("r_payload", 50.0)]);
        let join = MExpr {
            id: memo.next_expr_id(),
            op: Operator::PhysicalNestLoopJoin(PhysicalNestLoopJoinOp {
                join_type: JoinKind::LeftSemi,
                condition: Some(eq_expr(col_ref("l_filter"), int_lit(7))),
            }),
            children: vec![left, right],
        };

        let stats = derive_statistics(&join, &memo, &HashMap::new());

        assert!(
            (stats.output_row_count - 10.0).abs() < 1.0,
            "expected shared estimator output, got {}",
            stats.output_row_count
        );
        assert_eq!(stats.row_count_confidence, Confidence::Estimated);
        assert!(has_stat(&stats.column_statistics, "l_filter"));
        assert!(!has_stat(&stats.column_statistics, "r_payload"));
    }

    #[test]
    fn physical_nest_loop_right_anti_keeps_only_right_side_stats() {
        use crate::sql::optimizer::memo::{LogicalProperties, MExpr};
        use crate::sql::optimizer::operator::{LogicalValuesOp, Operator, PhysicalNestLoopJoinOp};

        fn column_stat(ndv: f64) -> ColumnStatistic {
            ColumnStatistic {
                min_value: 0.0,
                max_value: 1_000.0,
                nulls_fraction: 0.0,
                average_row_size: 8.0,
                distinct_values_count: ndv,
                confidence: Confidence::Exact,
            }
        }

        fn values_group(memo: &mut Memo, rows: f64, stats: &[(&str, f64)]) -> usize {
            let id = memo.next_expr_id();
            let group = memo.new_group(MExpr {
                id,
                op: Operator::LogicalValues(LogicalValuesOp {
                    rows: vec![],
                    columns: vec![],
                }),
                children: vec![],
            });
            let mut props = LogicalProperties::new(vec![], rows);
            props.row_count_confidence = Confidence::Exact;
            for &(name, ndv) in stats {
                props
                    .column_statistics
                    .insert(test_col_id(name), column_stat(ndv));
            }
            memo.groups[group].logical_props = Some(props);
            group
        }

        let mut memo = Memo::new();
        let left = values_group(&mut memo, 1_000.0, &[("l_payload", 100.0)]);
        let right = values_group(&mut memo, 50.0, &[("r_filter", 50.0)]);
        let join = MExpr {
            id: memo.next_expr_id(),
            op: Operator::PhysicalNestLoopJoin(PhysicalNestLoopJoinOp {
                join_type: JoinKind::RightAnti,
                condition: Some(eq_expr(col_ref("r_filter"), int_lit(7))),
            }),
            children: vec![left, right],
        };

        let stats = derive_statistics(&join, &memo, &HashMap::new());

        assert!(
            (stats.output_row_count - 20.0).abs() < 1.0,
            "expected right anti output, got {}",
            stats.output_row_count
        );
        assert!(!has_stat(&stats.column_statistics, "l_payload"));
        assert!(has_stat(&stats.column_statistics, "r_filter"));
    }

    #[test]
    fn window_stats_preserve_child_columns_and_mark_window_outputs_fallback() {
        use crate::sql::optimizer::memo::{LogicalProperties, MExpr};
        use crate::sql::optimizer::operator::{
            LogicalValuesOp, LogicalWindowOp, Operator, PhysicalWindowOp,
        };

        fn window_expr(output_name: &str) -> WindowExpr {
            WindowExpr {
                name: "row_number".to_string(),
                args: vec![],
                distinct: false,
                partition_by: vec![],
                order_by: vec![],
                window_frame: None,
                result_type: DataType::Int64,
                output_name: output_name.to_string(),
                output_column_id: test_col_id(output_name),
                ignore_nulls: false,
            }
        }

        fn child_group(memo: &mut Memo) -> usize {
            let group = memo.new_group(MExpr {
                id: memo.next_expr_id(),
                op: Operator::LogicalValues(LogicalValuesOp {
                    rows: vec![],
                    columns: vec![],
                }),
                children: vec![],
            });
            let mut props = LogicalProperties::new(vec![stats_output_column(1, "base")], 25.0);
            props.row_count_confidence = Confidence::Exact;
            props.column_statistics.insert(
                test_col_id("base"),
                ColumnStatistic {
                    min_value: 1.0,
                    max_value: 10.0,
                    nulls_fraction: 0.0,
                    average_row_size: 8.0,
                    distinct_values_count: 10.0,
                    confidence: Confidence::Exact,
                },
            );
            memo.groups[group].logical_props = Some(props);
            group
        }

        fn assert_window_stats(stats: Statistics) {
            assert_eq!(stats.output_row_count, 25.0);
            assert_eq!(stats.row_count_confidence, Confidence::Exact);
            assert_eq!(
                stat_by_name(&stats.column_statistics, "base").distinct_values_count,
                10.0
            );
            let row_number = stat_by_name(&stats.column_statistics, "rn");
            assert_eq!(row_number.confidence, Confidence::Fallback);
            assert_eq!(row_number.distinct_values_count, 1.0);
        }

        let mut memo = Memo::new();
        let child = child_group(&mut memo);
        let logical_window = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalWindow(LogicalWindowOp {
                window_exprs: vec![window_expr("rn")],
                output_columns: vec![stats_output_column(1, "base"), stats_output_column(2, "rn")],
            }),
            children: vec![child],
        };
        assert_window_stats(derive_statistics(&logical_window, &memo, &HashMap::new()));

        let physical_window = MExpr {
            id: memo.next_expr_id(),
            op: Operator::PhysicalWindow(PhysicalWindowOp {
                window_exprs: vec![window_expr("rn")],
                output_columns: vec![stats_output_column(1, "base"), stats_output_column(2, "rn")],
            }),
            children: vec![child],
        };
        assert_window_stats(derive_statistics(&physical_window, &memo, &HashMap::new()));
    }

    #[test]
    fn join_group_stats() {
        let (ln, lt) = make_table_stats("lineitem", 6_000_000, &[("l_orderkey", 1_500_000.0)]);
        let (on, ot) = make_table_stats("orders", 1_500_000, &[("o_orderkey", 1_500_000.0)]);
        let mut table_stats = HashMap::new();
        table_stats.insert(ln, lt);
        table_stats.insert(on, ot);

        let left = scan_plan("lineitem", &["l_orderkey"]);
        let right = scan_plan("orders", &["o_orderkey"]);
        let cond = eq_expr(col_ref("l_orderkey"), col_ref("o_orderkey"));

        let plan = LogicalPlan::Join(JoinNode {
            left: Box::new(left),
            right: Box::new(right),
            join_type: JoinKind::Inner,
            condition: Some(cond),
            required_output_columns: None,
        });

        let mut memo = Memo::new();
        logical_plan_to_memo(&plan, &mut memo);
        derive_group_statistics(&mut memo, &table_stats);

        // Join group should have stats derived.
        let join_props = memo.groups[2].logical_props.as_ref().unwrap();
        assert!(join_props.row_count > 0.0);
        // Output columns should include both sides.
        assert_eq!(join_props.output_columns.len(), 2);
    }

    #[test]
    fn aggregate_group_stats() {
        let (name, ts) = make_table_stats("t", 100_000, &[("status", 5.0)]);
        let mut table_stats = HashMap::new();
        table_stats.insert(name, ts);

        let scan = scan_plan("t", &["status"]);
        let plan = LogicalPlan::Aggregate(AggregateNode {
            input: Box::new(scan),
            group_by: vec![col_ref("status")],
            aggregates: vec![],
            output_columns: vec![OutputColumn {
                column_id: ColumnId::UNSET,
                name: "status".to_string(),
                data_type: DataType::Int32,
                nullable: false,
                is_internal: false,
            }],
            already_pushed: false,
            required_output_columns: None,
        });

        let mut memo = Memo::new();
        logical_plan_to_memo(&plan, &mut memo);
        derive_group_statistics(&mut memo, &table_stats);

        // Agg group: real NDV(status)=5 now flows through child_statistics,
        // so output = min(5, 100000*0.75) = 5.
        let agg_props = memo.groups[1].logical_props.as_ref().unwrap();
        assert!((agg_props.row_count - 5.0).abs() < 1.0);
    }

    #[test]
    fn limit_group_stats() {
        let (name, ts) = make_table_stats("t", 100_000, &[("a", 100.0)]);
        let mut table_stats = HashMap::new();
        table_stats.insert(name, ts);

        let scan = scan_plan("t", &["a"]);
        let plan = LogicalPlan::Limit(LimitNode {
            input: Box::new(scan),
            limit: Some(10),
            offset: None,
            required_output_columns: None,
        });

        let mut memo = Memo::new();
        logical_plan_to_memo(&plan, &mut memo);
        derive_group_statistics(&mut memo, &table_stats);

        let limit_props = memo.groups[1].logical_props.as_ref().unwrap();
        assert!((limit_props.row_count - 10.0).abs() < 0.01);
    }

    #[test]
    fn cte_consume_propagates_produce_row_count() {
        // Create a plan: CTEAnchor(CTEProduce(Scan), CTEConsume)
        // The CTEConsume should inherit the produce group's row count,
        // not use the old hardcoded 1000.
        let (name, ts) = make_table_stats("orders", 250_000, &[("id", 100_000.0)]);
        let mut table_stats = HashMap::new();
        table_stats.insert(name, ts);

        let scan = scan_plan("orders", &["id"]);
        let produce = LogicalPlan::CTEProduce(CTEProduceNode {
            cte_id: 1,
            input: Box::new(scan),
            output_columns: vec![OutputColumn {
                column_id: ColumnId::UNSET,
                name: "id".to_string(),
                data_type: DataType::Int32,
                nullable: false,
                is_internal: false,
            }],
            required_output_columns: None,
        });
        let consume = LogicalPlan::CTEConsume(CTEConsumeNode {
            cte_id: 1,
            alias: "cte_orders".to_string(),
            output_columns: vec![OutputColumn {
                column_id: ColumnId::UNSET,
                name: "id".to_string(),
                data_type: DataType::Int32,
                nullable: false,
                is_internal: false,
            }],
            required_output_columns: None,
        });
        let anchor = LogicalPlan::CTEAnchor(CTEAnchorNode {
            cte_id: 1,
            produce: Box::new(produce),
            consumer: Box::new(consume),
            required_output_columns: None,
        });

        let mut memo = Memo::new();
        logical_plan_to_memo(&anchor, &mut memo);
        derive_group_statistics(&mut memo, &table_stats);

        // Group 0: Scan (250000 rows from table stats)
        let scan_props = memo.groups[0].logical_props.as_ref().unwrap();
        assert!((scan_props.row_count - 250_000.0).abs() < 1.0);

        // Group 1: CTEProduce (passthrough from scan = 250000)
        let produce_props = memo.groups[1].logical_props.as_ref().unwrap();
        assert!((produce_props.row_count - 250_000.0).abs() < 1.0);

        // Group 2: CTEConsume (should propagate produce group's row count)
        let consume_props = memo.groups[2].logical_props.as_ref().unwrap();
        assert!(
            (consume_props.row_count - 250_000.0).abs() < 1.0,
            "CTEConsume should propagate produce group's row count (250000), got {}",
            consume_props.row_count
        );

        // Group 3: CTEAnchor (passthrough from consumer = 250000)
        let anchor_props = memo.groups[3].logical_props.as_ref().unwrap();
        assert!((anchor_props.row_count - 250_000.0).abs() < 1.0);
    }

    #[test]
    fn default_row_count_fact_table() {
        // A fact table without stats should get a large default.
        let rows = estimate_default_row_count("store_sales");
        assert_eq!(rows, 1_000_000.0);
    }

    #[test]
    fn default_row_count_small_dim() {
        // A small dimension table should get a small default.
        let rows = estimate_default_row_count("date_dim");
        assert_eq!(rows, 10_000.0);
    }

    #[test]
    fn default_row_count_medium_table() {
        let rows = estimate_default_row_count("customer");
        assert_eq!(rows, 100_000.0);
    }

    #[test]
    fn default_row_count_unknown_table() {
        // Completely unknown table gets the general default.
        let rows = estimate_default_row_count("my_custom_table");
        assert_eq!(rows, 100_000.0);
    }

    #[test]
    fn values_group_stats() {
        let plan = LogicalPlan::Values(ValuesNode {
            rows: vec![vec![], vec![], vec![]],
            columns: vec![OutputColumn {
                column_id: ColumnId::UNSET,
                name: "x".to_string(),
                data_type: DataType::Int32,
                nullable: false,
                is_internal: false,
            }],
            required_output_columns: None,
        });

        let mut memo = Memo::new();
        logical_plan_to_memo(&plan, &mut memo);
        derive_group_statistics(&mut memo, &HashMap::new());

        let props = memo.groups[0].logical_props.as_ref().unwrap();
        assert!((props.row_count - 3.0).abs() < 0.01);
        assert_eq!(props.output_columns.len(), 1);
    }

    #[test]
    fn project_group_stats_preserve_project_item_output_column_id() {
        let out_id = ColumnId::new_for_test(42);
        let plan = LogicalPlan::Project(ProjectNode {
            input: Box::new(LogicalPlan::Values(ValuesNode {
                rows: vec![vec![]],
                columns: vec![],
                required_output_columns: None,
            })),
            items: vec![ProjectItem {
                expr: TypedExpr {
                    kind: ExprKind::Literal(LiteralValue::Int(1)),
                    data_type: DataType::Int64,
                    nullable: false,
                },
                output_name: "col1".to_string(),
                output_column_id: out_id,
            }],
            output_qualifier: None,
            required_output_columns: None,
        });

        let mut memo = Memo::new();
        logical_plan_to_memo(&plan, &mut memo);
        derive_group_statistics(&mut memo, &HashMap::new());

        let props = memo.groups[1].logical_props.as_ref().unwrap();
        assert_eq!(props.output_columns.len(), 1);
        assert_eq!(props.output_columns[0].column_id, out_id);
    }

    #[test]
    fn decode_group_surfaces_string_output_columns() {
        // Decode wraps a scan that exposes `a` (the dict-encoded slot).
        // The Decode group itself must surface `a_str` (the renamed
        // string output) — passing the child's `a` through would let
        // downstream consumers fail to resolve the post-rename name.
        let (name, ts) = make_table_stats("t", 100, &[("a", 10.0)]);
        let mut table_stats = HashMap::new();
        table_stats.insert(name, ts);

        let scan = scan_plan("t", &["a"]);
        let plan = LogicalPlan::Decode(DecodeNode {
            input: Box::new(scan),
            mappings: vec![DecodeMapping {
                source_column_id: ColumnId::new_for_test(1),
                output_column_id: ColumnId::new_for_test(2),
                dict_column: "a".to_string(),
                string_column: "a_str".to_string(),
            }],
            output_columns: vec![OutputColumn {
                column_id: ColumnId::UNSET,
                name: "a_str".to_string(),
                data_type: DataType::Utf8,
                nullable: false,
                is_internal: false,
            }],
            required_output_columns: None,
        });

        let mut memo = Memo::new();
        logical_plan_to_memo(&plan, &mut memo);
        derive_group_statistics(&mut memo, &table_stats);

        // Group 0 is the scan; group 1 is Decode.
        let decode_props = memo.groups[1].logical_props.as_ref().unwrap();
        assert_eq!(decode_props.output_columns.len(), 1);
        assert_eq!(
            decode_props.output_columns[0].name, "a_str",
            "Decode must surface string_column, not the child's dict_column"
        );
    }

    fn col_stat(min: f64, max: f64, ndv: f64) -> ColumnStatistic {
        ColumnStatistic {
            min_value: min,
            max_value: max,
            nulls_fraction: 0.0,
            average_row_size: 8.0,
            distinct_values_count: ndv,
            ..Default::default()
        }
    }

    fn between_expr(expr: TypedExpr, low: TypedExpr, high: TypedExpr) -> TypedExpr {
        TypedExpr {
            data_type: DataType::Boolean,
            nullable: false,
            kind: ExprKind::Between {
                expr: Box::new(expr),
                low: Box::new(low),
                high: Box::new(high),
                negated: false,
            },
        }
    }

    #[test]
    fn between_uses_range_selectivity() {
        let mut cs = HashMap::new();
        cs.insert(test_col_id("a"), col_stat(0.0, 100.0, 100.0));
        // a BETWEEN 0 AND 50 over [0,100]: ge = clamp((100-0+1)/100) = 0.99,
        // le = (50-0+1)/100 = 0.51, product ≈ 0.5049.
        let pred = between_expr(col_ref("a"), int_lit(0), int_lit(50));
        let sel = estimate_selectivity(&pred, &cs);
        assert!(sel > 0.45 && sel < 0.56, "between selectivity was {sel}");
    }

    #[test]
    fn not_between_is_complement_of_between() {
        let mut cs = HashMap::new();
        cs.insert(test_col_id("a"), col_stat(0.0, 100.0, 100.0));
        // NOT (a BETWEEN 0 AND 10) over [0,100]:
        //   ge(a >= 0) = clamp((100-0+1)/100) = 0.99
        //   le(a <= 10) = (10-0+1)/100 = 0.11
        //   between sel = 0.99 * 0.11 ≈ 0.109
        //   NOT BETWEEN sel = 1 - 0.109 ≈ 0.891
        // The negated value (~0.89) is clearly distinct from the positive (~0.11),
        // so this test genuinely exercises the negated branch.
        let pred = TypedExpr {
            data_type: DataType::Boolean,
            nullable: false,
            kind: ExprKind::Between {
                expr: Box::new(col_ref("a")),
                low: Box::new(int_lit(0)),
                high: Box::new(int_lit(10)),
                negated: true,
            },
        };
        let sel = estimate_selectivity(&pred, &cs);
        assert!(
            sel > 0.85 && sel < 0.93,
            "not-between selectivity was {sel}"
        );
    }
}

#[cfg(test)]
mod join_widening_tests {
    use super::*;
    use crate::sql::analysis::{JoinKind, OutputColumn};
    use arrow::datatypes::DataType;

    fn c(name: &str, nullable: bool) -> OutputColumn {
        OutputColumn {
            column_id: ColumnId::UNSET,
            name: name.into(),
            data_type: DataType::Int32,
            nullable,
            is_internal: false,
        }
    }

    #[test]
    fn inner_preserves_nullability() {
        let out = widen_for_join_kind(JoinKind::Inner, vec![c("a", false)], vec![c("b", false)]);
        assert_eq!(out.len(), 2);
        assert!(!out[0].nullable);
        assert!(!out[1].nullable);
    }

    #[test]
    fn left_outer_widens_right() {
        let out = widen_for_join_kind(
            JoinKind::LeftOuter,
            vec![c("a", false)],
            vec![c("b", false)],
        );
        assert!(!out[0].nullable, "left side preserved");
        assert!(out[1].nullable, "right side widened");
    }

    #[test]
    fn right_outer_widens_left() {
        let out = widen_for_join_kind(
            JoinKind::RightOuter,
            vec![c("a", false)],
            vec![c("b", false)],
        );
        assert!(out[0].nullable, "left side widened");
        assert!(!out[1].nullable, "right side preserved");
    }

    #[test]
    fn full_outer_widens_both() {
        let out = widen_for_join_kind(
            JoinKind::FullOuter,
            vec![c("a", false)],
            vec![c("b", false)],
        );
        assert!(out[0].nullable);
        assert!(out[1].nullable);
    }

    #[test]
    fn left_semi_returns_left_only() {
        let out = widen_for_join_kind(JoinKind::LeftSemi, vec![c("a", false)], vec![c("b", false)]);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].name, "a");
        assert!(!out[0].nullable);
    }

    #[test]
    fn right_anti_returns_right_only() {
        let out = widen_for_join_kind(
            JoinKind::RightAnti,
            vec![c("a", false)],
            vec![c("b", false)],
        );
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].name, "b");
    }

    #[test]
    fn inner_with_nullable_source_column_preserves_flags() {
        // Inner join does not widen. If the source column was nullable, it stays nullable.
        // If not, it stays non-nullable. The Inner arm is a pure concatenation.
        let out = widen_for_join_kind(
            JoinKind::Inner,
            vec![c("a", true), c("a2", false)],
            vec![c("b", false)],
        );
        assert_eq!(out.len(), 3);
        assert!(out[0].nullable, "nullable source stays nullable");
        assert!(!out[1].nullable, "non-nullable source stays non-nullable");
        assert!(
            !out[2].nullable,
            "non-nullable source on right stays non-nullable"
        );
    }
}
