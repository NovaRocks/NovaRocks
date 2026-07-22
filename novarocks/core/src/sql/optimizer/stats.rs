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

//! Statistics derivation for Memo groups.
//!
//! Mirrors the logic in `sql::optimizer::cardinality` but operates on
//! Memo operators (`MExpr`) and reads child statistics from group logical
//! properties instead of recursing the `LogicalPlanNode` tree.

use std::collections::HashMap;

use super::estimate::ndv::{agg_group_rows, cap_ndv_at_rows};
use super::estimate::selectivity::apply_filter;
use super::memo::{GroupId, JoinTree, MExpr, Memo};
use super::operator::Operator;
use crate::sql::column_id::ColumnId;
use crate::sql::common::{BinOp, JoinKind, LiteralValue, OutputColumn, UnOp};
use crate::sql::optimizer::estimate::arith::{damped_conjunction, sat_add};
use crate::sql::optimizer::estimate::cardinality::{
    JoinCardInput, estimate_join_cardinality, except_rows, intersect_rows, union_all_rows,
    union_distinct_rows,
};
use crate::sql::optimizer::opt_expr::OptExpr;
use crate::sql::optimizer::scalar::{ScalarArena, ScalarId, ScalarNode};
use crate::sql::optimizer::statistics::*;
use crate::sql::optimizer::stats_input::{OptimizerStatsInput, StatsSource};

// Neutral fallback used only when no real base table statistics are available.
const MISSING_BASE_ROW_COUNT_FALLBACK: f64 = 100_000.0;

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Remap a CTE producer group's column statistics onto a consume's output
/// columns. Producer and consume output columns line up positionally (the
/// consume re-exposes the producer's projection under fresh column ids), so a
/// producer stat at position `i` is rekeyed to the consume column at the same
/// position. Without this, join keys on a CTE consume carry no NDV and a
/// self-join of the same CTE (tpc-ds q4/q11/q31/q74) degrades to a
/// cross-product, exploding the row-count estimate.
fn remap_cte_consume_column_statistics(
    props: &super::memo::LogicalProperties,
    consume_output_columns: &[OutputColumn],
) -> HashMap<ColumnId, ColumnStatistic> {
    let mut remapped = HashMap::with_capacity(consume_output_columns.len());
    for (producer_col, consume_col) in props
        .output_columns
        .iter()
        .zip(consume_output_columns.iter())
    {
        if let Some(stat) = props.column_statistics.get(&producer_col.column_id) {
            remapped.insert(consume_col.column_id, stat.clone());
        }
    }
    remapped
}

/// Derive [`Statistics`] for a single `MExpr` using child group statistics
/// already stored in `memo.groups[child].logical_props`.
pub(crate) fn derive_statistics(
    expr: &MExpr,
    memo: &Memo,
    stats_input: &OptimizerStatsInput,
) -> Statistics {
    match &expr.op {
        // -- Leaf operators (no children) --
        Operator::LogicalScan(scan) => derive_scan(scan, &memo.scalars, stats_input),
        Operator::LogicalValues(vals) => Statistics {
            output_row_count: vals.rows.len() as f64,
            row_count_confidence: Confidence::Exact,
            column_statistics: values_column_statistics_scalar(
                &memo.scalars,
                &vals.rows,
                &vals.columns,
            ),
        },
        Operator::LogicalGenerateSeries(gs) => Statistics {
            output_row_count: generate_series_row_count_f64(gs.start, gs.end, gs.step),
            row_count_confidence: Confidence::Exact,
            column_statistics: generate_series_column_statistics(
                gs.output_column_id,
                gs.start,
                gs.end,
                gs.step,
            ),
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
                        column_statistics: remap_cte_consume_column_statistics(
                            props,
                            &cte.output_columns,
                        ),
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
            let selectivity = estimate_selectivity_scalar(
                &memo.scalars,
                filter.predicate,
                &child_stats.column_statistics,
            );
            let (output_rows, row_count_confidence) = apply_filter(
                child_stats.output_row_count,
                child_stats.row_count_confidence,
                selectivity,
            );
            let mut column_statistics = child_stats.column_statistics;
            for stat in column_statistics.values_mut() {
                cap_stat_ndv_at_rows(stat, output_rows);
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
                    extract_column_id_scalar(&memo.scalars, item.expr)
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
                .map(|gb_expr| {
                    get_expr_ndv_scalar(&memo.scalars, *gb_expr, &child_stats.column_statistics)
                })
                .collect();
            let output_rows = agg_group_rows(&group_key_ndvs, child_stats.output_row_count);
            let column_statistics = aggregate_group_column_statistics_scalar(
                &memo.scalars,
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
            derive_window_statistics_scalar(
                child_stats,
                window.window_exprs.len(),
                &window.output_columns,
            )
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

        Operator::LogicalChangeEventExpand(expand) => {
            derive_change_event_expand_statistics(expand, expr, memo)
        }

        Operator::LogicalCTEProduce(_) => {
            // Passthrough child stats.
            child_statistics(memo, &expr.children, 0)
        }

        // -- Binary / multi-child operators --
        Operator::LogicalJoin(join) => {
            let left_stats = child_statistics(memo, &expr.children, 0);
            let right_stats = child_statistics(memo, &expr.children, 1);
            derive_join(join, &memo.scalars, &left_stats, &right_stats)
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
        Operator::PhysicalScan(scan) => derive_scan(scan, &memo.scalars, stats_input),

        Operator::PhysicalFilter(filter) => {
            let child_stats = child_statistics(memo, &expr.children, 0);
            let selectivity = estimate_selectivity_scalar(
                &memo.scalars,
                filter.predicate,
                &child_stats.column_statistics,
            );
            let (output_rows, row_count_confidence) = apply_filter(
                child_stats.output_row_count,
                child_stats.row_count_confidence,
                selectivity,
            );
            let mut column_statistics = child_stats.column_statistics;
            for stat in column_statistics.values_mut() {
                cap_stat_ndv_at_rows(stat, output_rows);
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
                    extract_column_id_scalar(&memo.scalars, item.expr)
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
                .map(|gb_expr| {
                    get_expr_ndv_scalar(&memo.scalars, *gb_expr, &child_stats.column_statistics)
                })
                .collect();
            let output_rows = agg_group_rows(&group_key_ndvs, child_stats.output_row_count);
            let column_statistics = aggregate_group_column_statistics_scalar(
                &memo.scalars,
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
                    let eq_key_pair = extract_column_id_scalar(&memo.scalars, eq.left)
                        .zip(extract_column_id_scalar(&memo.scalars, eq.right));
                    let (left_ndv, left_confidence) = best_join_key_ndv_scalar(
                        &memo.scalars,
                        eq.left,
                        &left_stats.column_statistics,
                        &right_stats.column_statistics,
                    );
                    let (right_ndv, right_confidence) = best_join_key_ndv_scalar(
                        &memo.scalars,
                        eq.right,
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
            let non_equi_selectivity = join.condition.map(|cond| {
                (
                    estimate_selectivity_scalar(&memo.scalars, cond, &left_stats.column_statistics),
                    Confidence::Estimated,
                )
            });
            let eq_key_pairs = collect_equi_join_column_pairs_scalar(&memo.scalars, join.condition);

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

        Operator::PhysicalSort(sort) => {
            let child_stats = child_statistics(memo, &expr.children, 0);
            if let Some(k) = sort.partition_limit {
                let output_rows = sort_partition_limit_output_rows_scalar(
                    &memo.scalars,
                    child_stats.output_row_count,
                    &sort.analytic_partition_exprs,
                    &child_stats.column_statistics,
                    k,
                );
                Statistics {
                    output_row_count: output_rows,
                    row_count_confidence: Confidence::Estimated,
                    column_statistics: child_stats.column_statistics,
                }
            } else {
                child_stats
            }
        }

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
            derive_window_statistics_scalar(
                child_stats,
                window.window_exprs.len(),
                &window.output_columns,
            )
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
                        column_statistics: remap_cte_consume_column_statistics(
                            props,
                            &cte.output_columns,
                        ),
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

        Operator::PhysicalChangeEventExpand(expand) => {
            derive_change_event_expand_statistics(expand, expr, memo)
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
            column_statistics: values_column_statistics_scalar(
                &memo.scalars,
                &vals.rows,
                &vals.columns,
            ),
        },

        Operator::PhysicalGenerateSeries(gs) => Statistics {
            output_row_count: generate_series_row_count_f64(gs.start, gs.end, gs.step),
            row_count_confidence: Confidence::Exact,
            column_statistics: generate_series_column_statistics(
                gs.output_column_id,
                gs.start,
                gs.end,
                gs.step,
            ),
        },
        Operator::PhysicalTableFunction(tf) => {
            derive_table_function_stats(tf.is_left_join, expr, memo)
        }

        Operator::LogicalAssertOneRow(_) | Operator::PhysicalAssertOneRow(_) => {
            let child_stats = child_statistics(memo, &expr.children, 0);
            Statistics {
                output_row_count: child_stats.output_row_count.min(1.0),
                row_count_confidence: Confidence::Estimated,
                column_statistics: child_stats.column_statistics,
            }
        }

        // Apply and IMV markers are eliminated by the rewrite stage before
        // statistics derivation. Reaching here indicates a planner bug.
        Operator::LogicalApply(_) => {
            unreachable!(
                "Apply operator must be eliminated by SubqueryRewrite before statistics derivation"
            )
        }
        Operator::LogicalImvDelta(_) | Operator::LogicalImvVersion(_) => {
            unreachable!(
                "IMV marker operators must be eliminated by the IMV rewrite stage before statistics derivation"
            )
        }
    }
}

pub(crate) fn derive_opt_expr_statistics(
    expr: &OptExpr,
    arena: &ScalarArena,
    stats_input: &OptimizerStatsInput,
) -> Statistics {
    let mut memo = Memo::new();
    // Donate a clone of the caller's arena so the memo can materialize scalars.
    memo.scalars = arena.clone();
    let root_group = super::memo_copy::opt_expr_to_memo(expr, &mut memo);
    derive_group_statistics(&mut memo, stats_input);
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

const DEFAULT_EXPR_NDV: f64 = 10.0;
const DEFAULT_JOIN_KEY_NDV: f64 = 40.0;
const UNKNOWN_JOIN_RESIDUAL_EQ_FILTER: f64 = 0.5;

#[derive(Default)]
struct ScalarJoinConditionEstimate {
    eq_key_ndvs: Vec<(f64, f64, Confidence)>,
    eq_key_pairs: Vec<(ColumnId, ColumnId)>,
    residual_selectivity: Option<(f64, Confidence)>,
}

fn best_join_key_ndv_scalar(
    arena: &ScalarArena,
    expr: ScalarId,
    primary_stats: &HashMap<ColumnId, ColumnStatistic>,
    secondary_stats: &HashMap<ColumnId, ColumnStatistic>,
) -> (f64, Confidence) {
    let primary = get_join_key_ndv_with_confidence_scalar(arena, expr, primary_stats);
    let secondary = get_join_key_ndv_with_confidence_scalar(arena, expr, secondary_stats);
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

fn extract_column_id_scalar(arena: &ScalarArena, expr: ScalarId) -> Option<ColumnId> {
    match arena.node(expr) {
        ScalarNode::ColumnRef(column_id) if *column_id != ColumnId::UNSET => Some(*column_id),
        ScalarNode::Cast { child, .. } | ScalarNode::Nested(child) => {
            extract_column_id_scalar(arena, *child)
        }
        _ => None,
    }
}

fn derive_change_event_expand_statistics(
    expand: &super::operator::ChangeEventExpandOp,
    expr: &MExpr,
    memo: &Memo,
) -> Statistics {
    let child_stats = child_statistics(memo, &expr.children, 0);
    let event_count = expand.events.len() as f64;
    let output_rows = child_stats.output_row_count * event_count;
    let mut column_statistics = HashMap::new();
    for event in &expand.events {
        for assignment in &event.assignments {
            let Some(expr) = assignment.expr else {
                continue;
            };
            let Some(input_id) = extract_column_id_scalar(&memo.scalars, expr) else {
                continue;
            };
            if let Some(stat) = child_stats.column_statistics.get(&input_id) {
                column_statistics.insert(assignment.output_column_id, stat.clone());
            }
        }
    }
    Statistics {
        output_row_count: output_rows,
        row_count_confidence: Confidence::Estimated,
        column_statistics,
    }
}

fn get_expr_ndv_scalar(
    arena: &ScalarArena,
    expr: ScalarId,
    column_stats: &HashMap<ColumnId, ColumnStatistic>,
) -> f64 {
    real_expr_ndv_scalar(arena, expr, column_stats)
        .map(|(ndv, _)| ndv)
        .unwrap_or(DEFAULT_EXPR_NDV)
}

fn get_join_key_ndv_with_confidence_scalar(
    arena: &ScalarArena,
    expr: ScalarId,
    column_stats: &HashMap<ColumnId, ColumnStatistic>,
) -> (f64, Confidence) {
    real_expr_ndv_scalar(arena, expr, column_stats)
        .unwrap_or((DEFAULT_JOIN_KEY_NDV, Confidence::Fallback))
}

fn real_expr_ndv_scalar(
    arena: &ScalarArena,
    expr: ScalarId,
    column_stats: &HashMap<ColumnId, ColumnStatistic>,
) -> Option<(f64, Confidence)> {
    if let Some(column_id) = extract_column_id_scalar(arena, expr)
        && let Some(cs) = column_stats.get(&column_id)
        && let Some((ndv, confidence)) = cs.trusted_ndv()
    {
        return Some((ndv, confidence));
    }
    None
}

fn estimate_selectivity_scalar(
    arena: &ScalarArena,
    expr: ScalarId,
    column_stats: &HashMap<ColumnId, ColumnStatistic>,
) -> f64 {
    match arena.node(expr) {
        ScalarNode::BinaryOp { left, op, right } => match *op {
            BinOp::And => {
                let mut conjuncts = Vec::new();
                flatten_and_scalar(arena, expr, &mut conjuncts);
                let sels: Vec<f64> = conjuncts
                    .iter()
                    .map(|conjunct| estimate_selectivity_scalar(arena, *conjunct, column_stats))
                    .collect();
                damped_conjunction(&sels)
            }
            BinOp::Or => {
                let l = estimate_selectivity_scalar(arena, *left, column_stats);
                let r = estimate_selectivity_scalar(arena, *right, column_stats);
                l + r - l * r
            }
            BinOp::Eq | BinOp::EqForNull => {
                estimate_eq_selectivity_scalar(arena, *left, *right, column_stats)
            }
            BinOp::Ne => 1.0 - estimate_eq_selectivity_scalar(arena, *left, *right, column_stats),
            BinOp::Lt | BinOp::Le | BinOp::Gt | BinOp::Ge => {
                estimate_range_selectivity_scalar(arena, *left, *right, *op, column_stats)
            }
            _ => PREDICATE_UNKNOWN_FILTER,
        },
        ScalarNode::IsNull { child, negated } => {
            let col_id = extract_column_id_scalar(arena, *child);
            let null_frac = col_id
                .and_then(|column_id| column_stats.get(&column_id))
                .map(|cs| {
                    if cs.nulls_fraction > 0.0 {
                        cs.nulls_fraction
                    } else {
                        IS_NULL_FILTER
                    }
                })
                .unwrap_or(IS_NULL_FILTER);
            if *negated { 1.0 - null_frac } else { null_frac }
        }
        ScalarNode::InList {
            child,
            list,
            negated,
        } => {
            let col_id = extract_column_id_scalar(arena, *child);
            let ndv = col_id
                .and_then(|column_id| column_stats.get(&column_id))
                .and_then(ColumnStatistic::trusted_ndv_value);

            let sel = if let Some(ndv) = ndv {
                (list.len() as f64 / ndv).min(1.0)
            } else {
                IN_PREDICATE_DEFAULT_FILTER
            };
            if *negated { 1.0 - sel } else { sel }
        }
        ScalarNode::Between {
            child,
            low,
            high,
            negated,
        } => {
            let ge =
                estimate_range_selectivity_scalar(arena, *child, *low, BinOp::Ge, column_stats);
            let le =
                estimate_range_selectivity_scalar(arena, *child, *high, BinOp::Le, column_stats);
            let sel = ge * le;
            if *negated { 1.0 - sel } else { sel }
        }
        ScalarNode::Like { negated, .. } => {
            let sel = PREDICATE_UNKNOWN_FILTER;
            if *negated { 1.0 - sel } else { sel }
        }
        ScalarNode::UnaryOp {
            op: UnOp::Not,
            child,
        } => 1.0 - estimate_selectivity_scalar(arena, *child, column_stats),
        ScalarNode::IsTruthValue { negated, .. } => {
            let base = 0.5;
            if *negated { 1.0 - base } else { base }
        }
        ScalarNode::Nested(inner) => estimate_selectivity_scalar(arena, *inner, column_stats),
        _ => PREDICATE_UNKNOWN_FILTER,
    }
}

fn flatten_and_scalar(arena: &ScalarArena, expr: ScalarId, out: &mut Vec<ScalarId>) {
    match arena.node(expr) {
        ScalarNode::BinaryOp {
            op: BinOp::And,
            left,
            right,
        } => {
            flatten_and_scalar(arena, *left, out);
            flatten_and_scalar(arena, *right, out);
        }
        ScalarNode::Nested(inner) => flatten_and_scalar(arena, *inner, out),
        _ => out.push(expr),
    }
}

fn estimate_eq_selectivity_scalar(
    arena: &ScalarArena,
    left: ScalarId,
    right: ScalarId,
    column_stats: &HashMap<ColumnId, ColumnStatistic>,
) -> f64 {
    if let Some((column_id, column_expr, literal_expr)) =
        extract_column_literal_pair_scalar(arena, left, right)
        && let Some(cs) = column_stats.get(&column_id)
    {
        if let Some(ndv) = cs.trusted_ndv_value() {
            return 1.0 / ndv;
        }
        if let Some(selectivity) =
            discrete_domain_equality_selectivity_scalar(arena, column_expr, literal_expr, cs)
        {
            return selectivity;
        }
    }
    PREDICATE_UNKNOWN_FILTER
}

fn extract_column_literal_pair_scalar(
    arena: &ScalarArena,
    left: ScalarId,
    right: ScalarId,
) -> Option<(ColumnId, ScalarId, ScalarId)> {
    if let Some(column_id) = extract_column_id_scalar(arena, left)
        && scalar_literal_f64(arena, right).is_some()
    {
        return Some((column_id, left, right));
    }
    if let Some(column_id) = extract_column_id_scalar(arena, right)
        && scalar_literal_f64(arena, left).is_some()
    {
        return Some((column_id, right, left));
    }
    None
}

fn discrete_domain_equality_selectivity_scalar(
    arena: &ScalarArena,
    column_expr: ScalarId,
    literal_expr: ScalarId,
    stat: &ColumnStatistic,
) -> Option<f64> {
    if !is_discrete_numeric_domain(arena.data_type(column_expr)) {
        return None;
    }
    let min = stat.min_value;
    let max = stat.max_value;
    if !min.is_finite() || !max.is_finite() || max < min {
        return None;
    }
    let value = scalar_literal_f64(arena, literal_expr)?;
    if value < min || value > max {
        return Some(0.0);
    }
    let domain_width = (max.floor() - min.ceil() + 1.0).max(1.0);
    Some((1.0 / domain_width).clamp(0.0, 1.0))
}

fn is_discrete_numeric_domain(data_type: &arrow::datatypes::DataType) -> bool {
    matches!(
        data_type,
        arrow::datatypes::DataType::Boolean
            | arrow::datatypes::DataType::Int8
            | arrow::datatypes::DataType::Int16
            | arrow::datatypes::DataType::Int32
            | arrow::datatypes::DataType::Int64
            | arrow::datatypes::DataType::UInt8
            | arrow::datatypes::DataType::UInt16
            | arrow::datatypes::DataType::UInt32
            | arrow::datatypes::DataType::UInt64
            | arrow::datatypes::DataType::Date32
    )
}

fn estimate_range_selectivity_scalar(
    arena: &ScalarArena,
    left: ScalarId,
    right: ScalarId,
    op: BinOp,
    column_stats: &HashMap<ColumnId, ColumnStatistic>,
) -> f64 {
    let col_id = extract_column_id_scalar(arena, left);
    let literal_val = scalar_literal_f64(arena, right);

    if let (Some(column_id), Some(val)) = (col_id, literal_val)
        && let Some(cs) = column_stats.get(&column_id)
    {
        let min = cs.min_value;
        let max = cs.max_value;
        if min.is_finite() && max.is_finite() && max > min {
            let range = max - min;
            return match op {
                BinOp::Lt => ((val - min) / range).clamp(0.01, 0.99),
                BinOp::Le => ((val - min + 1.0) / range).clamp(0.01, 0.99),
                BinOp::Gt => ((max - val) / range).clamp(0.01, 0.99),
                BinOp::Ge => ((max - val + 1.0) / range).clamp(0.01, 0.99),
                _ => 0.5,
            };
        }
    }
    0.5
}

fn scalar_literal_f64(arena: &ScalarArena, expr: ScalarId) -> Option<f64> {
    match arena.node(expr) {
        ScalarNode::Literal(value) => match &value.0 {
            LiteralValue::Int(v) => Some(*v as f64),
            LiteralValue::LargeInt(v) => Some(*v as f64),
            LiteralValue::Float(v) => Some(*v),
            LiteralValue::Decimal(s) => s.parse::<f64>().ok(),
            _ => None,
        },
        ScalarNode::Cast { child, .. } | ScalarNode::Nested(child) => {
            scalar_literal_f64(arena, *child)
        }
        _ => None,
    }
}

fn aggregate_group_column_statistics_scalar(
    arena: &ScalarArena,
    group_by: &[ScalarId],
    output_columns: &[OutputColumn],
    child_stats: &Statistics,
    output_rows: f64,
) -> HashMap<ColumnId, ColumnStatistic> {
    group_by
        .iter()
        .zip(output_columns.iter())
        .map(|(expr, output)| {
            let mut stat = extract_column_id_scalar(arena, *expr)
                .and_then(|column_id| child_stats.column_statistics.get(&column_id))
                .cloned()
                .unwrap_or_else(ColumnStatistic::unknown);
            cap_stat_ndv_at_rows(&mut stat, output_rows);
            (output.column_id, stat)
        })
        .collect()
}

/// Derive statistics for all groups in the Memo, bottom-up.
///
/// Groups are visited in order (0..N). Since `memo_copy` inserts leaves
/// before their parents, group 0 is the deepest leaf and the last group
/// is the root. This guarantees that all child groups have their
/// `logical_props` set before any parent group is processed.
pub(crate) fn derive_group_statistics(memo: &mut Memo, stats_input: &OptimizerStatsInput) {
    for group_idx in 0..memo.groups.len() {
        // Memoized derive: a group's logical_props are computed exactly once,
        // when first needed (StarRocks isStatsDerived semantics). Under the
        // per-group argmax collapse (`pick_group_representative`), a group's
        // representative depends on ALL its members, so this skip is only sound
        // while every member appended to an already-derived group has a
        // source_confidence no greater than the current representative's. That
        // holds for every producer today:
        //   - reorder / join commutativity / associativity append logically-
        //     equivalent join members with an EQUAL key (same source
        //     confidence).
        //   - MvRewrite appends an MV-backed member whose TOP operator is the
        //     same SPJG shape (Project/Aggregate) as the incumbent; that top op
        //     derives `Estimated` (the buried MV-scan `Exact` is capped away by
        //     the Project/Aggregate derive), so its key also ties. The collapse
        //     key reads the representative's (top-op) confidence, not a buried
        //     child's, so a deeper Exact does not by itself raise the key.
        // Hence the skip reproduces the identical representative today.
        //
        // INVARIANT for a future producer that appends a member which RAISES the
        // representative's key — i.e. lifts the top-op source confidence above
        // the incumbent (e.g. `Measured` stats stamped at the representative, or
        // runtime feedback): it MUST eagerly re-derive that group by calling
        // `derive_group_statistics_for` at append time. It MUST NOT set
        // `logical_props = None`: `implement()` reads child-group `logical_props`
        // for join-input column ids, and a `None` there silently degrades
        // HashJoin to NestLoop (M1). Eager re-derive keeps `logical_props` Some
        // and updates the representative.
        if memo.groups[group_idx].logical_props.is_some() {
            continue;
        }
        derive_group_statistics_for(memo, group_idx, stats_input);
    }
}

/// Derive and store [`super::memo::LogicalProperties`] for a single group. The
/// group's child groups must already have their `logical_props` set (the
/// bottom-up allocation invariant the bulk pass relies on). Used both by
/// [`derive_group_statistics`] and by [`copy_in_join_tree`] to stamp each
/// newly-created join group immediately, before `implement()` runs — otherwise
/// a bushy join's children have no column ids and `JoinToHashJoin` degrades the
/// join to a NestLoop.
///
/// Calling this function is also the **eager re-derive** mechanism: a producer
/// that appends a strictly-higher-key member to an already-derived group MUST
/// call this function directly at append time so the argmax is re-run against
/// all members. The function always keeps `logical_props` `Some` (never goes to
/// `None`), which preserves the M1 invariant that `implement()` relies on to
/// read child-group column ids.
pub(crate) fn derive_group_statistics_for(
    memo: &mut Memo,
    group_idx: usize,
    stats_input: &OptimizerStatsInput,
) {
    // Output columns are a group-level invariant: all members of a memo group
    // are logically equivalent and expose the identical output columns, so
    // picking them via `first()` here is consistent with the argmax-chosen
    // member. The structural properties that DO vary by member shape
    // (equivalence classes, unique columns) are derived from the chosen member
    // inside `derive_for_expr` below.
    let output_columns = derive_output_columns(memo, group_idx);

    // Collapse the group to a single representative member by lexicographic
    // confidence argmax, then derive BOTH statistics and structural properties
    // from that SAME member (member-consistency). For a non-empty group we call
    // `derive_for_expr` with the chosen member; `derive_for_group` would re-pick
    // `first()` internally and break member-consistency when argmax selects a
    // non-first member.
    if let Some((chosen, stats)) = pick_group_representative(memo, group_idx, stats_input) {
        memo.groups[group_idx].logical_props = Some(super::logical_props::derive_for_expr(
            &chosen,
            memo,
            output_columns,
            stats.output_row_count,
            stats.row_count_confidence,
            stats.column_statistics,
        ));
    } else {
        // Empty group: keep today's behavior — default statistics plus
        // `derive_for_group`, which handles the no-member case correctly.
        memo.groups[group_idx].logical_props = Some(super::logical_props::derive_for_group(
            memo,
            group_idx,
            output_columns,
            1.0,
            Confidence::Fallback,
            HashMap::new(),
        ));
    }
}

/// Pick a group's representative member by source-confidence argmax:
/// key = source_confidence; on tie prefer FFewerConj (inner-join only); on
/// final tie keep the lowest index (canonical-first). Mirrors GPORCA
/// PgexprBestPromise + FBetterPromise (minus the derivability axis, which is
/// redundant under NovaRocks's derive-then-pick model — per-member confidence
/// is already computed, so a shape-based promise proxy added no within-group
/// signal). Strict-greater replacement from the first member, so an all-equal
/// group degenerates to `first()` (zero-regression baseline). Returns the
/// chosen member (cloned) and its already-derived Statistics (reused, not
/// recomputed). Returns None for an empty group.
///
/// Member order is `logical_exprs` then `physical_exprs`, so the lowest index
/// is `logical_exprs[0]` — exactly today's pick
/// (`logical_exprs.first().or(physical_exprs.first())`).
pub(crate) fn pick_group_representative(
    memo: &Memo,
    group_idx: usize,
    stats_input: &OptimizerStatsInput,
) -> Option<(MExpr, Statistics)> {
    // Build the member list as logical_exprs then physical_exprs. Cloning each
    // member releases the borrow on `memo.groups[..]`, so we can pass `memo`
    // immutably to `derive_statistics` below.
    let members: Vec<MExpr> = {
        let group = &memo.groups[group_idx];
        group
            .logical_exprs
            .iter()
            .chain(group.physical_exprs.iter())
            .cloned()
            .collect()
    };

    let mut iter = members.into_iter();
    let first = iter.next()?;
    let first_stats = derive_statistics(&first, memo, stats_input);
    let first_key = first_stats.row_count_confidence;
    let mut best = (first, first_stats, first_key);

    for cand in iter {
        let cand_stats = derive_statistics(&cand, memo, stats_input);
        let cand_key = cand_stats.row_count_confidence;
        // Strict-greater replacement: replace only on a strict improvement so
        // ties keep the lower index (canonical-first / zero-regression).
        let replace = cand_key > best.2
            || (cand_key == best.2
                && inner_join_conjunct_count(&cand.op, &memo.scalars)
                    .zip(inner_join_conjunct_count(&best.0.op, &memo.scalars))
                    .is_some_and(|(cand_conj, best_conj)| cand_conj < best_conj));
        if replace {
            best = (cand, cand_stats, cand_key);
        }
    }

    Some((best.0, best.1))
}

/// Conjunct count of an INNER `LogicalJoin`'s condition (an AND-tree split into
/// conjuncts), used as the FFewerConj sub-tie-break. Returns `None` for any
/// operator that is not an inner LogicalJoin, so the tie-break only fires when
/// BOTH tied members are inner joins (GPORCA FFewerConj semantics).
fn inner_join_conjunct_count(op: &Operator, scalars: &ScalarArena) -> Option<usize> {
    match op {
        Operator::LogicalJoin(join) if join.join_type == JoinKind::Inner => match join.condition {
            Some(sid) => {
                let mut conjuncts = Vec::new();
                flatten_and_scalar(scalars, sid, &mut conjuncts);
                Some(conjuncts.len())
            }
            None => Some(0),
        },
        _ => None,
    }
}

/// Materialize a [`JoinTree`] candidate order bottom-up into the memo, returning
/// the group id of its root. Leaves reuse their existing group; each internal
/// join is deduplicated against `memo.join_group_index` and, when newly
/// created, has its statistics stamped immediately via
/// [`derive_group_statistics_for`].
pub(crate) fn copy_in_join_tree(
    memo: &mut Memo,
    tree: &JoinTree,
    stats_input: &OptimizerStatsInput,
) -> GroupId {
    match tree {
        JoinTree::Leaf(group_id) => *group_id,
        JoinTree::Join { left, right, op } => {
            // Recurse children first so child group ids are always allocated
            // before the parent (the bottom-up invariant M2 relies on).
            let left_id = copy_in_join_tree(memo, left, stats_input);
            let right_id = copy_in_join_tree(memo, right, stats_input);
            let operator = Operator::LogicalJoin(op.clone());
            // Dedup: reuse an existing group for the same operator + child
            // groups, so candidates sharing intermediate sub-joins do not mint
            // duplicate groups.
            let key = (format!("{operator:?}"), vec![left_id, right_id]);
            if let Some(&existing) = memo.join_group_index.get(&key) {
                return existing;
            }
            let id = memo.next_expr_id();
            let group_id = memo.new_group(MExpr {
                id,
                op: operator,
                children: vec![left_id, right_id],
            });
            debug_assert!(
                left_id < group_id && right_id < group_id,
                "copy_in_join_tree must allocate bottom-up (child < parent)"
            );
            // Stamp statistics immediately: implement() runs before the bulk
            // re-derive (mod.rs), and JoinToHashJoin reads child column ids from
            // logical_props — without this a bushy join degrades to NestLoop (M1).
            derive_group_statistics_for(memo, group_id, stats_input);
            memo.join_group_index.insert(key, group_id);
            group_id
        }
    }
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

/// Get child group's statistics as a [`Statistics`] value.
///
/// Reads `logical_props` from the child group. If not yet derived (should
/// not happen when groups are processed in order), returns a default.
/// Returns a conservative default if `index` is out of bounds (malformed memo).
fn child_statistics(memo: &Memo, children: &[super::memo::GroupId], index: usize) -> Statistics {
    let Some(&group_id) = children.get(index) else {
        return Statistics {
            output_row_count: 10_000.0,
            row_count_confidence: Confidence::Fallback,
            column_statistics: HashMap::new(),
        };
    };
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

/// Estimate Sort output rows when `partition_limit = Some(k)`.
///
/// A per-partition TopN truncates each partition group to at most `k` rows, so
/// the total output is bounded by `ndv_partition_keys * k`. If NDV information
/// is available for *any* partition-key expression we use the product of their
/// NDVs (via `get_expr_ndv`); otherwise we fall back to pass-through
/// (`child_rows`) because we have no basis for a reduction estimate.
///
/// The result is capped at `child_rows` — we never inflate.
fn sort_partition_limit_output_rows_scalar(
    arena: &ScalarArena,
    child_rows: f64,
    partition_exprs: &[ScalarId],
    column_stats: &HashMap<ColumnId, ColumnStatistic>,
    partition_limit: usize,
) -> f64 {
    if partition_exprs.is_empty() {
        return child_rows;
    }

    let mut ndv_product = 1.0_f64;
    let mut exp = 1.0_f64;
    let mut ndvs: Vec<f64> = partition_exprs
        .iter()
        .map(|expr| get_expr_ndv_scalar(arena, *expr, column_stats))
        .collect();
    ndvs.sort_by(|a, b| b.partial_cmp(a).unwrap_or(std::cmp::Ordering::Equal));
    for ndv in ndvs {
        let n = if ndv.is_finite() { ndv.max(1.0) } else { 1.0 };
        ndv_product *= n.powf(exp);
        if !ndv_product.is_finite() {
            ndv_product = child_rows;
            break;
        }
        exp *= 0.5;
    }
    let estimated = ndv_product * partition_limit as f64;
    estimated.min(child_rows).max(1.0)
}

/// Exact per-column statistics for a `VALUES` relation, synthesized from the
/// literal rows. A column gets stats only when every value is a numeric literal
/// (or NULL); non-numeric columns are left unknown. NDV is the exact distinct
/// count, bounded by the literal min/max. Without this, a join on a `VALUES`
/// column (e.g. an `IN`-list lowered to a values join) has no NDV.
fn values_column_statistics_scalar(
    arena: &ScalarArena,
    rows: &[Vec<ScalarId>],
    columns: &[OutputColumn],
) -> HashMap<ColumnId, ColumnStatistic> {
    let mut out = HashMap::new();
    let row_count = rows.len() as f64;
    if row_count == 0.0 {
        return out;
    }
    for (col_idx, column) in columns.iter().enumerate() {
        if column.column_id == ColumnId::UNSET {
            continue;
        }
        let mut values: Vec<f64> = Vec::with_capacity(rows.len());
        let mut nulls = 0usize;
        let mut all_numeric_or_null = true;
        for row in rows {
            match row.get(col_idx) {
                Some(expr) if scalar_is_null_literal(arena, *expr) => nulls += 1,
                Some(expr) => match scalar_literal_f64(arena, *expr) {
                    Some(v) => values.push(v),
                    None => {
                        all_numeric_or_null = false;
                        break;
                    }
                },
                None => {
                    all_numeric_or_null = false;
                    break;
                }
            }
        }
        if !all_numeric_or_null {
            continue;
        }
        let non_null = values.len();
        let mut distinct = values.clone();
        distinct.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        distinct.dedup_by(|a, b| (*a - *b).abs() < f64::EPSILON);
        let min = values
            .iter()
            .copied()
            .reduce(f64::min)
            .unwrap_or(f64::NEG_INFINITY);
        let max = values
            .iter()
            .copied()
            .reduce(f64::max)
            .unwrap_or(f64::INFINITY);
        out.insert(
            column.column_id,
            ColumnStatistic {
                min_value: min,
                max_value: max,
                nulls_fraction: nulls as f64 / row_count,
                average_row_size: 8.0,
                confidence: Confidence::Exact,
                ..ColumnStatistic::unknown()
            }
            .with_known_ndv(
                (distinct.len().max(if non_null > 0 { 1 } else { 0 }) as f64).max(1.0),
                Confidence::Exact,
                StatsSource::Derived,
            ),
        );
    }
    out
}

fn scalar_is_null_literal(arena: &ScalarArena, expr: ScalarId) -> bool {
    matches!(
        arena.node(expr),
        ScalarNode::Literal(value) if matches!(&value.0, LiteralValue::Null)
    )
}

/// Exact per-column statistics for a `generate_series` output column. Every
/// value in the arithmetic sequence is distinct, so NDV equals the row count,
/// bounded by `[min(start,end), max(start,end)]`. Without this the series
/// column has no NDV and a join on it falls back to `DEFAULT_JOIN_KEY_NDV`.
fn generate_series_column_statistics(
    output_column_id: ColumnId,
    start: i64,
    end: i64,
    step: i64,
) -> HashMap<ColumnId, ColumnStatistic> {
    let mut column_statistics = HashMap::new();
    if output_column_id == ColumnId::UNSET {
        return column_statistics;
    }
    let rows = generate_series_row_count_f64(start, end, step);
    column_statistics.insert(
        output_column_id,
        ColumnStatistic {
            min_value: start.min(end) as f64,
            max_value: start.max(end) as f64,
            nulls_fraction: 0.0,
            average_row_size: 8.0,
            confidence: Confidence::Exact,
            ..ColumnStatistic::unknown()
        }
        .with_known_ndv(rows.max(1.0), Confidence::Exact, StatsSource::Derived),
    );
    column_statistics
}

fn derive_table_function_stats(is_left_join: bool, expr: &MExpr, memo: &Memo) -> Statistics {
    let child = child_statistics(memo, &expr.children, 0);
    let estimated_rows = child.output_row_count * 3.0;
    let output_row_count = if is_left_join {
        estimated_rows.max(child.output_row_count)
    } else {
        estimated_rows.max(1.0)
    };
    Statistics {
        output_row_count,
        row_count_confidence: Confidence::Estimated,
        // Pass through child column statistics; the table function's generated
        // columns stay unknown (absent). The child columns survive on the
        // output (see `derive_output_columns`), so without this a left-join
        // table function silently drops the left side's NDVs.
        column_statistics: child.column_statistics,
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
        let mut min_ndv: Option<f64> = None;
        let mut missing_ndv = false;

        for stat in &stats_for_column {
            min_value = min_value.min(stat.min_value);
            max_value = max_value.max(stat.max_value);
            nulls_fraction = nulls_fraction.max(stat.nulls_fraction);
            average_row_size = average_row_size.max(positive_row_size(stat.average_row_size));
            confidence = confidence.combine(stat.confidence);

            if let Some((stat_ndv, ndv_confidence)) = stat.trusted_ndv() {
                let (next_ndv, _) = sat_add(union_ndv, stat_ndv);
                union_ndv = next_ndv;
                min_ndv = Some(min_ndv.map_or(stat_ndv, |current| current.min(stat_ndv)));
                confidence = confidence.combine(ndv_confidence);
            } else {
                missing_ndv = true;
            }
        }

        let raw_ndv = match kind {
            SetOpKind::Union { .. } if !missing_child_stat && !missing_ndv => Some(union_ndv),
            SetOpKind::Intersect | SetOpKind::Except if !missing_child_stat && !missing_ndv => {
                min_ndv
            }
            _ => None,
        };
        if missing_child_stat || missing_ndv {
            confidence = confidence.combine(Confidence::Fallback);
        }

        let mut stat = ColumnStatistic {
            min_value,
            max_value,
            nulls_fraction,
            average_row_size,
            confidence,
            ..ColumnStatistic::unknown()
        };
        if let Some(raw_ndv) = raw_ndv {
            stat.set_known_ndv(
                bounded_set_op_ndv(raw_ndv, output_rows),
                confidence,
                StatsSource::Derived,
            );
        }
        merged.insert(output_column.column_id, stat);
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
        cap_stat_ndv_at_rows(stat, output_rows);
    }
}

fn cap_stat_ndv_at_rows(stat: &mut ColumnStatistic, output_rows: f64) {
    let Some(ndv) = stat.ndv_value() else {
        return;
    };
    let capped_ndv = cap_ndv_at_rows(ndv, output_rows);
    if capped_ndv != ndv {
        let source = stat.ndv_source().unwrap_or(StatsSource::Derived);
        stat.set_known_ndv(capped_ndv, stat.confidence, source);
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
            stat.set_known_ndv(contained_ndv, confidence, StatsSource::Derived);
            stat.confidence = confidence;
        }
        if let Some(stat) = column_statistics.get_mut(&right_key) {
            stat.set_known_ndv(contained_ndv, confidence, StatsSource::Derived);
            stat.confidence = confidence;
        }
    }

    for stat in column_statistics.values_mut() {
        let Some(ndv) = stat.ndv_value() else {
            continue;
        };
        let capped_ndv = cap_ndv_at_rows(ndv, output_rows);
        if capped_ndv != ndv {
            let source = stat.ndv_source().unwrap_or(StatsSource::Derived);
            let confidence = Confidence::derive(&[stat.confidence, row_count_confidence], false);
            stat.set_known_ndv(capped_ndv, confidence, source);
            stat.confidence = confidence;
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
    let left_ndv = real_column_ndv(left)?;
    let right_ndv = real_column_ndv(right)?;
    let ndv = left_ndv.min(right_ndv);
    let confidence = Confidence::derive(
        &[left.confidence, right.confidence, row_count_confidence],
        false,
    );
    Some((ndv, confidence))
}

fn real_column_ndv(stat: &ColumnStatistic) -> Option<f64> {
    stat.trusted_ndv_value()
}

fn collect_equi_join_column_pairs_scalar(
    arena: &ScalarArena,
    condition: Option<ScalarId>,
) -> Vec<(ColumnId, ColumnId)> {
    let mut pairs = Vec::new();
    if let Some(condition) = condition {
        collect_equi_join_column_pairs_inner_scalar(arena, condition, &mut pairs);
    }
    pairs
}

fn collect_equi_join_column_pairs_inner_scalar(
    arena: &ScalarArena,
    expr: ScalarId,
    pairs: &mut Vec<(ColumnId, ColumnId)>,
) {
    match arena.node(expr) {
        ScalarNode::BinaryOp {
            left,
            op: BinOp::Eq | BinOp::EqForNull,
            right,
        } => {
            if let (Some(left_id), Some(right_id)) = (
                extract_column_id_scalar(arena, *left),
                extract_column_id_scalar(arena, *right),
            ) {
                pairs.push((left_id, right_id));
            }
        }
        ScalarNode::BinaryOp {
            left,
            op: BinOp::And,
            right,
        } => {
            collect_equi_join_column_pairs_inner_scalar(arena, *left, pairs);
            collect_equi_join_column_pairs_inner_scalar(arena, *right, pairs);
        }
        ScalarNode::Nested(inner) => {
            collect_equi_join_column_pairs_inner_scalar(arena, *inner, pairs)
        }
        _ => {}
    }
}

fn derive_window_statistics_scalar(
    mut child_stats: Statistics,
    window_expr_count: usize,
    output_columns: &[OutputColumn],
) -> Statistics {
    assert!(
        output_columns.len() >= window_expr_count,
        "window output layout must include window result columns"
    );
    let window_output_start = output_columns.len() - window_expr_count;
    for output_column in output_columns.iter().skip(window_output_start) {
        child_stats
            .column_statistics
            .insert(output_column.column_id, ColumnStatistic::unknown());
    }
    child_stats
}

/// Derive scan statistics from a `ScanOp`.
fn derive_scan(
    scan: &super::operator::ScanOp,
    scalars: &ScalarArena,
    stats_input: &OptimizerStatsInput,
) -> Statistics {
    match resolve_scan_table_statistics(scan, stats_input) {
        ScanStatsResolution::Resolved(resolved) => derive_scan_statistics_scalar(
            &scan.columns,
            &scan.predicates,
            scalars,
            Some(&resolved.table_stats),
            resolved.row_count_confidence,
            MISSING_BASE_ROW_COUNT_FALLBACK,
        ),
        ScanStatsResolution::MissingBoundRef => derive_scan_statistics_scalar(
            &scan.columns,
            &scan.predicates,
            scalars,
            None,
            Confidence::Fallback,
            MISSING_BASE_ROW_COUNT_FALLBACK,
        ),
    }
}

struct ResolvedScanTableStatistics {
    table_stats: TableStatistics,
    row_count_confidence: Confidence,
}

enum ScanStatsResolution {
    Resolved(ResolvedScanTableStatistics),
    MissingBoundRef,
}

fn resolve_scan_table_statistics(
    scan: &super::operator::ScanOp,
    stats_input: &OptimizerStatsInput,
) -> ScanStatsResolution {
    let Some(stats_ref) = scan.stats_ref else {
        return ScanStatsResolution::MissingBoundRef;
    };
    match stats_input
        .query_stats()
        .get(stats_ref)
        .and_then(TableStatistics::try_from_base_stats_with_confidence)
    {
        Some((table_stats, row_count_confidence)) => {
            ScanStatsResolution::Resolved(ResolvedScanTableStatistics {
                table_stats,
                row_count_confidence,
            })
        }
        None => ScanStatsResolution::MissingBoundRef,
    }
}

fn derive_scan_statistics_scalar(
    columns: &[OutputColumn],
    predicates: &[ScalarId],
    scalars: &ScalarArena,
    table_stats: Option<&TableStatistics>,
    table_row_count_confidence: Confidence,
    default_rows: f64,
) -> Statistics {
    if let Some(ts) = table_stats {
        let row_count = ts.row_count.max(1) as f64;

        let mut output_rows = row_count;
        let mut row_count_confidence = table_row_count_confidence;
        let table_column_statistics = map_table_column_stats_to_ids(columns, ts);
        for pred in predicates {
            let selectivity = estimate_selectivity_scalar(scalars, *pred, &table_column_statistics);
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
        let mut column_statistics: HashMap<ColumnId, ColumnStatistic> = columns
            .iter()
            .map(|c| (c.column_id, ColumnStatistic::unknown()))
            .collect();
        let mut output_rows = default_rows;
        let mut row_count_confidence = Confidence::Fallback;
        for pred in predicates {
            let selectivity = estimate_selectivity_scalar(scalars, *pred, &column_statistics);
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

/// Derive join statistics from a `LogicalJoinOp` and child stats.
fn estimate_join_condition_scalar(
    arena: &ScalarArena,
    condition: Option<ScalarId>,
    left_stats: &HashMap<ColumnId, ColumnStatistic>,
    right_stats: &HashMap<ColumnId, ColumnStatistic>,
) -> ScalarJoinConditionEstimate {
    let Some(condition) = condition else {
        return ScalarJoinConditionEstimate::default();
    };

    let mut estimate = ScalarJoinConditionEstimate::default();
    let mut residuals = Vec::new();
    collect_join_conjuncts_scalar(
        arena,
        condition,
        left_stats,
        right_stats,
        &mut estimate,
        &mut residuals,
    );

    if !residuals.is_empty() {
        let combined_stats = combined_column_statistics(left_stats, right_stats);
        let selectivities: Vec<_> = residuals
            .iter()
            .map(|expr| estimate_join_residual_selectivity_scalar(arena, *expr, &combined_stats))
            .collect();
        estimate.residual_selectivity =
            Some((damped_conjunction(&selectivities), Confidence::Estimated));
    }

    estimate
}

fn collect_join_conjuncts_scalar(
    arena: &ScalarArena,
    expr: ScalarId,
    left_stats: &HashMap<ColumnId, ColumnStatistic>,
    right_stats: &HashMap<ColumnId, ColumnStatistic>,
    estimate: &mut ScalarJoinConditionEstimate,
    residuals: &mut Vec<ScalarId>,
) {
    match arena.node(expr) {
        ScalarNode::BinaryOp {
            left,
            op: BinOp::And,
            right,
        } => {
            collect_join_conjuncts_scalar(
                arena,
                *left,
                left_stats,
                right_stats,
                estimate,
                residuals,
            );
            collect_join_conjuncts_scalar(
                arena,
                *right,
                left_stats,
                right_stats,
                estimate,
                residuals,
            );
        }
        ScalarNode::Nested(inner) => {
            collect_join_conjuncts_scalar(
                arena,
                *inner,
                left_stats,
                right_stats,
                estimate,
                residuals,
            );
        }
        _ => {
            if !try_collect_equi_key_scalar(arena, expr, left_stats, right_stats, estimate) {
                residuals.push(expr);
            }
        }
    }
}

fn try_collect_equi_key_scalar(
    arena: &ScalarArena,
    expr: ScalarId,
    left_stats: &HashMap<ColumnId, ColumnStatistic>,
    right_stats: &HashMap<ColumnId, ColumnStatistic>,
    estimate: &mut ScalarJoinConditionEstimate,
) -> bool {
    let ScalarNode::BinaryOp {
        left,
        op: BinOp::Eq | BinOp::EqForNull,
        right,
    } = arena.node(expr)
    else {
        return false;
    };

    let Some(left_id) = extract_column_id_scalar(arena, *left) else {
        return false;
    };
    let Some(right_id) = extract_column_id_scalar(arena, *right) else {
        return false;
    };

    let forward = left_stats.contains_key(&left_id) && right_stats.contains_key(&right_id);
    let reverse = left_stats.contains_key(&right_id) && right_stats.contains_key(&left_id);
    let (left_expr, right_expr, left_key, right_key) = match (forward, reverse) {
        (true, false) => (*left, *right, left_id, right_id),
        (false, true) => (*right, *left, right_id, left_id),
        (true, true) if left_id == right_id => {
            let (left_ndv, left_confidence) =
                get_join_key_ndv_with_confidence_scalar(arena, *left, left_stats);
            let (right_ndv, right_confidence) =
                get_join_key_ndv_with_confidence_scalar(arena, *right, right_stats);
            estimate.eq_key_ndvs.push((
                left_ndv,
                right_ndv,
                left_confidence.combine(right_confidence),
            ));
            return true;
        }
        _ => return false,
    };

    estimate.eq_key_pairs.push((left_key, right_key));
    let (left_ndv, left_confidence) =
        get_join_key_ndv_with_confidence_scalar(arena, left_expr, left_stats);
    let (right_ndv, right_confidence) =
        get_join_key_ndv_with_confidence_scalar(arena, right_expr, right_stats);
    estimate.eq_key_ndvs.push((
        left_ndv,
        right_ndv,
        left_confidence.combine(right_confidence),
    ));
    true
}

fn estimate_join_residual_selectivity_scalar(
    arena: &ScalarArena,
    expr: ScalarId,
    column_stats: &HashMap<ColumnId, ColumnStatistic>,
) -> f64 {
    let selectivity = estimate_selectivity_scalar(arena, expr, column_stats);
    if (selectivity - PREDICATE_UNKNOWN_FILTER).abs() < f64::EPSILON
        && is_unknown_column_literal_eq_scalar(arena, expr, column_stats)
    {
        UNKNOWN_JOIN_RESIDUAL_EQ_FILTER
    } else {
        selectivity
    }
}

fn is_unknown_column_literal_eq_scalar(
    arena: &ScalarArena,
    expr: ScalarId,
    column_stats: &HashMap<ColumnId, ColumnStatistic>,
) -> bool {
    let ScalarNode::BinaryOp {
        left,
        op: BinOp::Eq | BinOp::EqForNull,
        right,
    } = arena.node(expr)
    else {
        return false;
    };

    let Some(column_id) =
        extract_column_id_scalar(arena, *left).or_else(|| extract_column_id_scalar(arena, *right))
    else {
        return false;
    };
    if !(scalar_is_literal_like(arena, *left) || scalar_is_literal_like(arena, *right)) {
        return false;
    }
    column_stats
        .get(&column_id)
        .is_none_or(|cs| cs.trusted_ndv().is_none())
}

fn scalar_is_literal_like(arena: &ScalarArena, expr: ScalarId) -> bool {
    match arena.node(expr) {
        ScalarNode::Literal(_) => true,
        ScalarNode::Cast { child, .. } | ScalarNode::Nested(child) => {
            scalar_is_literal_like(arena, *child)
        }
        _ => false,
    }
}

fn combined_column_statistics(
    left_stats: &HashMap<ColumnId, ColumnStatistic>,
    right_stats: &HashMap<ColumnId, ColumnStatistic>,
) -> HashMap<ColumnId, ColumnStatistic> {
    let mut combined = left_stats.clone();
    combined.extend(right_stats.clone());
    combined
}

fn derive_join(
    join: &super::operator::LogicalJoinOp,
    scalars: &ScalarArena,
    left_stats: &Statistics,
    right_stats: &Statistics,
) -> Statistics {
    let join_condition = estimate_join_condition_scalar(
        scalars,
        join.condition,
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
    join_type: crate::sql::common::JoinKind,
    left_cols: Vec<crate::sql::common::OutputColumn>,
    right_cols: Vec<crate::sql::common::OutputColumn>,
) -> Vec<crate::sql::common::OutputColumn> {
    use crate::sql::common::JoinKind::*;
    fn widen(cols: Vec<crate::sql::common::OutputColumn>) -> Vec<crate::sql::common::OutputColumn> {
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
fn derive_output_columns(memo: &Memo, group_idx: usize) -> Vec<crate::sql::common::OutputColumn> {
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
            .map(|item| crate::sql::common::OutputColumn {
                column_id: item.output_column_id,
                name: item.output_name.clone(),
                data_type: memo.scalars.data_type(item.expr).clone(),
                nullable: memo.scalars.nullable(item.expr),
                is_internal: false,
            })
            .collect(),
        Operator::LogicalAggregate(a) => a.output_columns.clone(),
        Operator::LogicalChangeEventExpand(e) => e.output_columns.clone(),
        Operator::LogicalWindow(w) => w.output_columns.clone(),
        Operator::LogicalValues(v) => v.columns.clone(),
        Operator::LogicalCTEAnchor(_) => child_output_columns(memo, &expr.children, 1),
        Operator::LogicalCTEProduce(c) => c.output_columns.clone(),
        Operator::LogicalCTEConsume(c) => c.output_columns.clone(),
        Operator::LogicalGenerateSeries(g) => {
            vec![crate::sql::common::OutputColumn {
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

        Operator::LogicalRepeat(repeat) => repeat_output_columns(memo, &expr.children, repeat),

        // Passthrough operators: inherit output columns from first child.
        Operator::LogicalFilter(_)
        | Operator::LogicalSort(_)
        | Operator::LogicalLimit(_)
        | Operator::LogicalTopN(_)
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
            .map(|item| crate::sql::common::OutputColumn {
                column_id: item.output_column_id,
                name: item.output_name.clone(),
                data_type: memo.scalars.data_type(item.expr).clone(),
                nullable: memo.scalars.nullable(item.expr),
                is_internal: false,
            })
            .collect(),
        Operator::PhysicalHashAggregate(a) => a.output_columns.clone(),
        Operator::PhysicalChangeEventExpand(e) => e.output_columns.clone(),
        Operator::PhysicalWindow(w) => w.output_columns.clone(),
        Operator::PhysicalValues(v) => v.columns.clone(),
        Operator::PhysicalCTEAnchor(_) => child_output_columns(memo, &expr.children, 1),
        Operator::PhysicalCTEProduce(c) => c.output_columns.clone(),
        Operator::PhysicalCTEConsume(c) => c.output_columns.clone(),
        Operator::PhysicalGenerateSeries(g) => {
            vec![crate::sql::common::OutputColumn {
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
        Operator::PhysicalRepeat(repeat) => repeat_output_columns(memo, &expr.children, repeat),
        Operator::PhysicalFilter(_)
        | Operator::PhysicalSort(_)
        | Operator::PhysicalLimit(_)
        | Operator::PhysicalTopN(_)
        | Operator::PhysicalDistribution(_)
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

        // Apply and IMV markers are eliminated before statistics derivation.
        Operator::LogicalApply(_) => {
            unreachable!(
                "Apply operator must be eliminated by SubqueryRewrite before output-column derivation"
            )
        }
        Operator::LogicalImvDelta(_) | Operator::LogicalImvVersion(_) => {
            unreachable!(
                "IMV marker operators must be eliminated by the IMV rewrite stage before output-column derivation"
            )
        }
    }
}

fn child_output_columns(
    memo: &Memo,
    children: &[usize],
    child_idx: usize,
) -> Vec<crate::sql::common::OutputColumn> {
    children
        .get(child_idx)
        .and_then(|&child_id| memo.groups[child_id].logical_props.as_ref())
        .map(|props| props.output_columns.clone())
        .unwrap_or_default()
}

fn repeat_output_columns(
    memo: &Memo,
    children: &[usize],
    repeat: &super::operator::RepeatOp,
) -> Vec<crate::sql::common::OutputColumn> {
    let mut columns = child_output_columns(memo, children, 0);
    columns.extend(repeat.grouping_fn_ids.iter().map(|(name, column_id)| {
        crate::sql::common::OutputColumn {
            column_id: *column_id,
            name: name.clone(),
            data_type: arrow::datatypes::DataType::Int64,
            nullable: false,
            is_internal: true,
        }
    }));
    columns
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connector::iceberg::scan_model::{
        IcebergDataFileInfo, IcebergSchemaDef, IcebergTableInfo,
    };
    use crate::sql::analysis::{
        ExprKind, JoinKind, LiteralValue, OutputColumn, ProjectItem, TypedExpr,
    };
    use crate::sql::optimizer::estimate::selectivity::estimate_selectivity;
    use crate::sql::optimizer::memo::Memo;
    use crate::sql::optimizer::operator::AggregateOutputLayout;
    use crate::sql::optimizer::stats_input::{
        BaseColumnStatistics, BaseTableStatistics, QueryStatsSnapshot, StatValue,
        StatsMissingReason, StatsRef, StatsSource,
    };
    use crate::sql::planner::logical::*;
    use crate::sql::planner::optimizer_bridge::scalar::intern_typed;
    use crate::sql::planner::optimizer_bridge::scalar::{
        intern_aggregate_calls, intern_exprs, intern_window_exprs,
    };
    use crate::sql::planner::payload::*;
    use crate::sql::planner::table::{ScanSource, TableDef};
    use arrow::datatypes::DataType;
    use novarocks_catalog::schema::ColumnDef;

    fn logical_plan_to_memo_for_test(plan: &LogicalPlanNode, memo: &mut Memo) -> GroupId {
        let mut opt_expr = crate::sql::planner::optimizer_bridge::logical::try_to_optimizer_expr(
            plan,
            &mut memo.scalars,
        )
        .expect("logical plan to opt expr");
        bind_test_scan_refs(&mut opt_expr);
        crate::sql::optimizer::memo_copy::opt_expr_to_memo(&opt_expr, memo)
    }

    fn bind_test_scan_refs(expr: &mut OptExpr) {
        match &mut expr.op {
            Operator::LogicalScan(scan) | Operator::PhysicalScan(scan) => {
                scan.stats_ref = Some(test_stats_ref_for_table(&scan.table.name));
            }
            _ => {}
        }
        for child in &mut expr.children {
            bind_test_scan_refs(child);
        }
    }

    fn test_stats_ref_for_table(table: &str) -> StatsRef {
        let mut hash = 2_166_136_261u32;
        for byte in table.to_ascii_lowercase().bytes() {
            hash ^= byte as u32;
            hash = hash.wrapping_mul(16_777_619);
        }
        StatsRef::new(hash)
    }

    fn estimate_selectivity_for_test(
        expr: &TypedExpr,
        column_stats: &HashMap<ColumnId, ColumnStatistic>,
    ) -> f64 {
        let mut arena = ScalarArena::new();
        let id = intern_typed(&mut arena, expr);
        estimate_selectivity(&arena, id, column_stats)
    }

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
                    ..ColumnStatistic::for_test_with_ndv(ndv, Confidence::Exact)
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

    fn query_stats_input_for_test(stats: &HashMap<String, TableStatistics>) -> OptimizerStatsInput {
        let mut snapshot = QueryStatsSnapshot::empty();
        for (name, stats) in stats {
            let stats_ref = test_stats_ref_for_table(name);
            assert!(
                snapshot.get(stats_ref).is_none(),
                "test stats ref collision for table {name}"
            );
            snapshot.insert(
                stats_ref,
                format!("db.{name}"),
                base_stats_from_table_statistics(stats),
            );
        }
        OptimizerStatsInput::from_query_stats(&snapshot)
    }

    fn empty_stats_input() -> OptimizerStatsInput {
        OptimizerStatsInput::from_query_stats(&QueryStatsSnapshot::empty())
    }

    fn base_stats_from_table_statistics(stats: &TableStatistics) -> BaseTableStatistics {
        BaseTableStatistics {
            row_count: StatValue::known(
                stats.row_count,
                Confidence::Exact,
                StatsSource::TestFixture,
            ),
            columns: stats
                .column_stats
                .iter()
                .map(|(name, stat)| {
                    (
                        name.to_ascii_lowercase(),
                        BaseColumnStatistics {
                            nulls_fraction: StatValue::known(
                                stat.nulls_fraction,
                                stat.confidence,
                                StatsSource::TestFixture,
                            ),
                            average_row_size: StatValue::known(
                                stat.average_row_size,
                                stat.confidence,
                                StatsSource::TestFixture,
                            ),
                            min_value: StatValue::known(
                                stat.min_value,
                                stat.confidence,
                                StatsSource::TestFixture,
                            ),
                            max_value: StatValue::known(
                                stat.max_value,
                                stat.confidence,
                                StatsSource::TestFixture,
                            ),
                            ndv: stat.ndv_value().map_or_else(
                                || {
                                    StatValue::missing(StatsMissingReason::ColumnNotReported(
                                        name.to_ascii_lowercase(),
                                    ))
                                },
                                |ndv| {
                                    StatValue::known(ndv, stat.confidence, StatsSource::TestFixture)
                                },
                            ),
                        },
                    )
                })
                .collect(),
            source: StatsSource::TestFixture,
        }
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

    fn scan_plan(name: &str, cols: &[&str]) -> LogicalPlanNode {
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
        LogicalPlanNode::new(
            LogicalPlanKind::Scan(PlanScanNode {
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
                        binding:
                            crate::connector::iceberg::scan_model::IcebergDataFileBinding::CurrentSnapshot,
                    },
                },
                alias: None,
                columns: columns,
                predicates: vec![],
                required_columns: None,
                variant_columns: vec![],
                mv_rewritten_from: None,
            }),
            vec![],
            None,
        )
    }

    fn scan_plan_with_predicates(
        name: &str,
        cols: &[&str],
        predicates: Vec<TypedExpr>,
    ) -> LogicalPlanNode {
        let mut plan = scan_plan(name, cols);
        let LogicalPlanKind::Scan(node) = &mut plan.kind else {
            unreachable!("scan_plan always returns a Scan");
        };
        node.predicates = predicates;
        plan
    }

    fn bound_scan_opt_expr(name: &str, cols: &[&str], stats_ref: StatsRef) -> OptExpr {
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
        OptExpr::leaf(Operator::LogicalScan(super::super::operator::ScanOp {
            database: "db".to_string(),
            table: TableDef {
                name: name.to_string(),
                columns: col_defs,
                iceberg_row_lineage_metadata_columns: vec![],
                source: ScanSource::StarRocks {
                    db_id: 1,
                    table_id: 1,
                },
            },
            alias: None,
            stats_ref: Some(stats_ref),
            columns,
            predicates: vec![],
            required_columns: None,
            variant_columns: vec![],
            mv_rewritten_from: None,
        }))
    }

    #[test]
    fn fallback_scan_applies_predicate_selectivity() {
        // No bound snapshot entry -> derive_scan takes the neutral fallback.
        // The predicate should still reduce the row count.
        let table_stats: HashMap<String, TableStatistics> = HashMap::new();
        let pred = eq_expr(col_ref("a"), int_lit(42));
        let plan = scan_plan_with_predicates("unknown_tbl", &["a"], vec![pred]);

        let mut memo = Memo::new();
        logical_plan_to_memo_for_test(&plan, &mut memo);
        derive_group_statistics(&mut memo, &query_stats_input_for_test(&table_stats));

        // Neutral fallback rows = 100000; unknown-column eq selectivity
        // = PREDICATE_UNKNOWN_FILTER (0.25) -> 100000 * 0.25 = 25000.
        let props = memo.groups[0].logical_props.as_ref().unwrap();
        assert!((props.row_count - 25_000.0).abs() < 1.0);
        assert_eq!(props.row_count_confidence, Confidence::Fallback);
    }

    #[test]
    fn bound_scan_missing_snapshot_ref_uses_neutral_non_name_fallback() {
        let stats_input = OptimizerStatsInput::from_query_stats(&QueryStatsSnapshot::empty());

        for table_name in ["store_sales", "tiny_dim"] {
            let stats_ref = StatsRef::new(42);
            let scan = bound_scan_opt_expr(table_name, &["k"], stats_ref);
            let stats = derive_opt_expr_statistics(&scan, &ScalarArena::new(), &stats_input);

            assert!((stats.output_row_count - MISSING_BASE_ROW_COUNT_FALLBACK).abs() < 1.0);
            assert_eq!(stats.row_count_confidence, Confidence::Fallback);
        }
    }

    #[test]
    fn bound_scan_preserves_snapshot_row_count_despite_misleading_name() {
        let stats_ref = StatsRef::new(7);
        let mut snapshot = QueryStatsSnapshot::empty();
        snapshot.insert(
            stats_ref,
            "db.misleading_sales_table",
            BaseTableStatistics {
                row_count: StatValue::known(
                    2,
                    Confidence::Estimated,
                    StatsSource::ConnectorEstimate,
                ),
                columns: HashMap::new(),
                source: StatsSource::ConnectorEstimate,
            },
        );
        let scan = bound_scan_opt_expr("misleading_sales_table", &["k"], stats_ref);
        let stats_input = OptimizerStatsInput::from_query_stats(&snapshot);
        let stats = derive_opt_expr_statistics(&scan, &ScalarArena::new(), &stats_input);

        assert!((stats.output_row_count - 2.0).abs() < 1.0);
        assert_eq!(stats.row_count_confidence, Confidence::Estimated);
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
                ..ColumnStatistic::for_test_with_ndv(5.0, Confidence::Estimated)
            },
        );
        let mut table_stats = HashMap::new();
        table_stats.insert(name, ts);
        let plan = scan_plan("orders", &["id", "status", "missing"]);

        let mut memo = Memo::new();
        logical_plan_to_memo_for_test(&plan, &mut memo);
        derive_group_statistics(&mut memo, &query_stats_input_for_test(&table_stats));

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
        logical_plan_to_memo_for_test(&plan, &mut memo);
        derive_group_statistics(&mut memo, &query_stats_input_for_test(&table_stats));

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
        logical_plan_to_memo_for_test(&plan, &mut memo);
        derive_group_statistics(&mut memo, &query_stats_input_for_test(&table_stats));

        let props = memo.groups[0].logical_props.as_ref().unwrap();
        assert_eq!(props.row_count, 1.0);
        assert_eq!(props.row_count_confidence, Confidence::Fallback);
        assert_eq!(
            stat_by_name(&props.column_statistics, "id").ndv_or_legacy_unknown_sentinel_for_test(),
            1.0
        );
    }

    #[test]
    fn physical_scan_uses_same_confidence_rules_as_logical_scan() {
        use crate::sql::optimizer::memo::MExpr;
        use crate::sql::optimizer::operator::{Operator, ScanOp};

        let (name, ts) = make_table_stats("orders", 100_000, &[("id", 100.0)]);
        let mut table_stats = HashMap::new();
        table_stats.insert(name, ts);
        let scan_plan =
            scan_plan_with_predicates("orders", &["id"], vec![eq_expr(col_ref("id"), int_lit(42))]);
        let LogicalPlanKind::Scan(scan) = scan_plan.kind else {
            unreachable!("scan_plan_with_predicates always returns a Scan");
        };
        let mut memo = Memo::new();
        let predicates = intern_exprs(&mut memo.scalars, &scan.predicates);
        let expr = MExpr {
            id: memo.next_expr_id(),
            op: Operator::PhysicalScan(ScanOp {
                database: scan.database,
                table: scan.table,
                alias: scan.alias,
                stats_ref: Some(test_stats_ref_for_table("orders")),
                columns: scan.columns,
                predicates,
                required_columns: scan.required_columns,
                variant_columns: scan.variant_columns,
                mv_rewritten_from: None,
            }),
            children: vec![],
        };

        let stats = derive_statistics(&expr, &memo, &query_stats_input_for_test(&table_stats));

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
        use crate::sql::optimizer::operator::{Operator, ValuesOp};

        let mut memo = Memo::new();
        let child = memo.new_group(MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalValues(ValuesOp {
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
    fn change_event_expand_output_columns_are_declared_schema() {
        use crate::sql::common::change_stream::ChangeStreamBranchKind;
        use crate::sql::optimizer::memo::{LogicalProperties, MExpr};
        use crate::sql::optimizer::operator::{
            ChangeEventExpandOp, ChangeEventOutputExpr, ChangeEventSpec, Operator, ValuesOp,
        };

        fn output_column(id: u32, name: &str) -> OutputColumn {
            OutputColumn {
                column_id: ColumnId::new_for_test(id),
                name: name.to_string(),
                data_type: DataType::Int64,
                nullable: false,
                is_internal: name.starts_with('_'),
            }
        }

        let mut memo = Memo::new();
        let child_columns = vec![output_column(1, "payload")];
        let child = memo.new_group(MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalValues(ValuesOp {
                rows: vec![],
                columns: child_columns.clone(),
            }),
            children: vec![],
        });
        memo.groups[child].logical_props = Some(LogicalProperties::new(child_columns, 10.0));

        let output_columns = vec![
            output_column(101, "_file"),
            output_column(102, "_pos"),
            output_column(103, "__change_op"),
        ];
        let expand_group = memo.new_group(MExpr {
            id: memo.next_expr_id(),
            op: Operator::PhysicalChangeEventExpand(ChangeEventExpandOp {
                events: vec![ChangeEventSpec {
                    predicate: None,
                    branch_kind: ChangeStreamBranchKind::DeleteDv,
                    assignments: vec![ChangeEventOutputExpr {
                        output_column_id: ColumnId::new_for_test(101),
                        expr: None,
                    }],
                }],
                output_columns: output_columns.clone(),
                change_op_column_id: ColumnId::new_for_test(103),
                data_route_column_id: None,
            }),
            children: vec![child],
        });

        let derived = derive_output_columns(&memo, expand_group);
        assert_eq!(
            derived
                .iter()
                .map(|column| column.column_id)
                .collect::<Vec<_>>(),
            output_columns
                .iter()
                .map(|column| column.column_id)
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn aggregate_stats_are_independent_of_split_stage_metadata() {
        use std::collections::HashMap;

        use crate::sql::analysis::{ExprKind, OutputColumn, TypedExpr};
        use crate::sql::column_id::ColumnId;
        use crate::sql::optimizer::memo::{LogicalProperties, MExpr, Memo};
        use crate::sql::optimizer::operator::{AggStage, LogicalAggregateOp, Operator, ValuesOp};
        use crate::sql::optimizer::statistics::ColumnStatistic;
        use crate::sql::planner::payload::AggregateCall;

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
                output_column_id: ColumnId::new_for_test(3),
            }
        }

        fn values_group(memo: &mut Memo) -> usize {
            let id = memo.next_expr_id();
            memo.new_group(MExpr {
                id,
                op: Operator::LogicalValues(ValuesOp {
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
                ..ColumnStatistic::for_test_with_ndv(100.0, Confidence::Exact)
            },
        );
        memo.groups[child_group].logical_props = Some(child_props);

        fn aggregate_expr(
            memo: &mut Memo,
            child_group: usize,
            stage: AggStage,
            is_merge: Vec<bool>,
            is_split: bool,
        ) -> MExpr {
            let group_by = intern_exprs(&mut memo.scalars, &[col_ref(1, "k")]);
            let aggregates = intern_aggregate_calls(&mut memo.scalars, &[count_call()]);
            let output_columns = vec![output_column(1, "k"), output_column(3, "count(v)")];
            let output_layout = full_aggregate_output_layout(group_by.len(), &output_columns);
            MExpr {
                id: memo.next_expr_id(),
                op: Operator::LogicalAggregate(LogicalAggregateOp::staged(
                    stage,
                    group_by,
                    aggregates,
                    output_layout,
                    output_columns,
                    is_merge,
                    is_split,
                )),
                children: vec![child_group],
            }
        }

        let single_group_by = intern_exprs(&mut memo.scalars, &[col_ref(1, "k")]);
        let single_aggregates = intern_aggregate_calls(&mut memo.scalars, &[count_call()]);
        let single_output_columns = vec![output_column(1, "k"), output_column(3, "count(v)")];
        let single_output_layout =
            full_aggregate_output_layout(single_group_by.len(), &single_output_columns);
        let single = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalAggregate(LogicalAggregateOp::single(
                single_group_by,
                single_aggregates,
                single_output_layout,
                single_output_columns,
            )),
            children: vec![child_group],
        };
        let local = aggregate_expr(&mut memo, child_group, AggStage::Local, vec![false], true);
        let global = aggregate_expr(&mut memo, child_group, AggStage::Global, vec![true], true);
        let global_without_split =
            aggregate_expr(&mut memo, child_group, AggStage::Global, vec![true], false);

        let table_stats = HashMap::new();
        let single_stats =
            derive_statistics(&single, &memo, &query_stats_input_for_test(&table_stats));
        for alternative in [&local, &global, &global_without_split] {
            let alternative_stats = derive_statistics(
                alternative,
                &memo,
                &query_stats_input_for_test(&table_stats),
            );
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
            ..ColumnStatistic::for_test_with_ndv(ndv, confidence)
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
        use crate::sql::optimizer::operator::{Operator, ValuesOp};

        let group = memo.new_group(MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalValues(ValuesOp {
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
        use crate::sql::optimizer::operator::{Operator, ValuesOp};

        let group = memo.new_group(MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalValues(ValuesOp {
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
        use crate::sql::optimizer::operator::{Operator, UnionOp};

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
            op: Operator::LogicalUnion(UnionOp {
                all: true,
                output_columns: vec![stats_output_column(10, "k")],
                child_output_columns: vec![],
            }),
            children: vec![left, right],
        };

        let stats = derive_statistics(&union, &memo, &empty_stats_input());

        assert_eq!(stats.output_row_count, MAX_ROW_COUNT);
        assert_eq!(stats.row_count_confidence, Confidence::Fallback);
        let col = stat_by_name(&stats.column_statistics, "k");
        assert_eq!(col.min_value, -10.0);
        assert_eq!(col.max_value, 20.0);
        assert_eq!(col.nulls_fraction, 0.20);
        assert_eq!(col.average_row_size, 16.0);
        assert_eq!(col.ndv_or_legacy_unknown_sentinel_for_test(), MAX_ROW_COUNT);
        assert_eq!(col.confidence, Confidence::Estimated);
    }

    #[test]
    fn logical_union_distinct_applies_correlation_and_merges_column_ranges() {
        use crate::sql::optimizer::operator::{Operator, UnionOp};

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
            op: Operator::LogicalUnion(UnionOp {
                all: false,
                output_columns: vec![stats_output_column(10, "k")],
                child_output_columns: vec![],
            }),
            children: vec![left, right],
        };

        let stats = derive_statistics(&union, &memo, &empty_stats_input());

        assert_eq!(stats.output_row_count, 300.0);
        assert_eq!(stats.row_count_confidence, Confidence::Estimated);
        let col = stat_by_name(&stats.column_statistics, "k");
        assert_eq!(col.min_value, -5.0);
        assert_eq!(col.max_value, 50.0);
        assert_eq!(col.nulls_fraction, 0.15);
        assert_eq!(col.average_row_size, 12.0);
        assert_eq!(col.ndv_or_legacy_unknown_sentinel_for_test(), 130.0);
        assert_eq!(col.confidence, Confidence::Estimated);
    }

    #[test]
    fn logical_union_column_stat_missing_child_degrades_confidence() {
        use crate::sql::optimizer::operator::{Operator, UnionOp};

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
            op: Operator::LogicalUnion(UnionOp {
                all: true,
                output_columns: vec![stats_output_column(10, "k")],
                child_output_columns: vec![],
            }),
            children: vec![left, right],
        };

        let stats = derive_statistics(&union, &memo, &empty_stats_input());

        assert_eq!(stats.output_row_count, 300.0);
        let col = stat_by_name(&stats.column_statistics, "k");
        assert_eq!(col.min_value, 5.0);
        assert_eq!(col.max_value, 30.0);
        assert!(col.ndv_value().is_none());
        assert_eq!(col.ndv_or_legacy_unknown_sentinel_for_test(), 1.0);
        assert_eq!(col.confidence, Confidence::Fallback);
    }

    #[test]
    fn logical_intersect_halves_min_rows_and_uses_min_column_ndv() {
        use crate::sql::optimizer::operator::{IntersectOp, Operator};

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
            set_op_column_stat(-20.0, 80.0, 0.25, 16.0, 30.0, Confidence::Estimated),
        );
        let intersect = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalIntersect(IntersectOp {
                output_columns: vec![stats_output_column(10, "k")],
                child_output_columns: vec![],
            }),
            children: vec![left, right],
        };

        let stats = derive_statistics(&intersect, &memo, &empty_stats_input());

        assert_eq!(stats.output_row_count, 100.0);
        assert_eq!(stats.row_count_confidence, Confidence::Estimated);
        let col = stat_by_name(&stats.column_statistics, "k");
        assert_eq!(col.min_value, -20.0);
        assert_eq!(col.max_value, 100.0);
        assert_eq!(col.nulls_fraction, 0.25);
        assert_eq!(col.average_row_size, 16.0);
        assert_eq!(col.ndv_or_legacy_unknown_sentinel_for_test(), 30.0);
        assert_eq!(col.confidence, Confidence::Estimated);
    }

    #[test]
    fn logical_except_halves_first_rows_and_merges_column_stats_with_min_ndv() {
        use crate::sql::optimizer::operator::{ExceptOp, Operator};

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
            op: Operator::LogicalExcept(ExceptOp {
                output_columns: vec![stats_output_column(10, "k")],
                child_output_columns: vec![],
            }),
            children: vec![left, right],
        };

        let stats = derive_statistics(&except, &memo, &empty_stats_input());

        assert_eq!(stats.output_row_count, 500.0);
        assert_eq!(stats.row_count_confidence, Confidence::Estimated);
        let col = stat_by_name(&stats.column_statistics, "k");
        assert_eq!(col.min_value, -10.0);
        assert_eq!(col.max_value, 100.0);
        assert_eq!(col.nulls_fraction, 0.20);
        assert_eq!(col.average_row_size, 16.0);
        assert_eq!(col.ndv_or_legacy_unknown_sentinel_for_test(), 30.0);
        assert_eq!(col.confidence, Confidence::Estimated);
    }

    fn aggregate_child_stat(ndv: f64) -> ColumnStatistic {
        ColumnStatistic {
            min_value: 0.0,
            max_value: 1_000_000.0,
            nulls_fraction: 0.0,
            average_row_size: 8.0,
            ..ColumnStatistic::for_test_with_ndv(ndv, Confidence::Exact)
        }
    }

    fn aggregate_ndv_child_group(
        memo: &mut Memo,
        row_count: f64,
        row_count_confidence: Confidence,
    ) -> usize {
        use crate::sql::optimizer::memo::LogicalProperties;
        use crate::sql::optimizer::operator::{Operator, ValuesOp};

        let id = memo.next_expr_id();
        let group = memo.new_group(MExpr {
            id,
            op: Operator::LogicalValues(ValuesOp {
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

    fn aggregate_group_key_ids(memo: &mut Memo) -> Vec<ScalarId> {
        intern_exprs(&mut memo.scalars, &aggregate_group_keys())
    }

    fn aggregate_output_columns() -> Vec<OutputColumn> {
        vec![
            stats_output_column(1, "k1"),
            stats_output_column(2, "k2"),
            stats_output_column(3, "k3"),
        ]
    }

    fn full_aggregate_output_layout(
        group_by_len: usize,
        output_columns: &[OutputColumn],
    ) -> AggregateOutputLayout {
        AggregateOutputLayout::new(
            output_columns.iter().take(group_by_len).cloned().collect(),
            output_columns.iter().skip(group_by_len).cloned().collect(),
        )
    }

    fn aggregate_output_layout() -> AggregateOutputLayout {
        let output_columns = aggregate_output_columns();
        full_aggregate_output_layout(aggregate_group_keys().len(), &output_columns)
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
                aggregate_group_key_ids(&mut memo),
                vec![],
                aggregate_output_layout(),
                aggregate_output_columns(),
            )),
            children: vec![exact_child],
        };
        let fallback_agg = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalAggregate(LogicalAggregateOp::single(
                aggregate_group_key_ids(&mut memo),
                vec![],
                aggregate_output_layout(),
                aggregate_output_columns(),
            )),
            children: vec![fallback_child],
        };

        let exact_stats = derive_statistics(&exact_agg, &memo, &empty_stats_input());
        assert_row_count_close(exact_stats.output_row_count, expected);
        assert_eq!(exact_stats.row_count_confidence, Confidence::Estimated);

        let fallback_stats = derive_statistics(&fallback_agg, &memo, &empty_stats_input());
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
                group_by: aggregate_group_key_ids(&mut memo),
                aggregates: vec![],
                output_layout: aggregate_output_layout(),
                output_columns: aggregate_output_columns(),
                is_merge: vec![],
            }),
            children: vec![exact_child],
        };
        let fallback_agg = MExpr {
            id: memo.next_expr_id(),
            op: Operator::PhysicalHashAggregate(PhysicalHashAggregateOp {
                mode: AggMode::Single,
                group_by: aggregate_group_key_ids(&mut memo),
                aggregates: vec![],
                output_layout: aggregate_output_layout(),
                output_columns: aggregate_output_columns(),
                is_merge: vec![],
            }),
            children: vec![fallback_child],
        };

        let exact_stats = derive_statistics(&exact_agg, &memo, &empty_stats_input());
        assert_row_count_close(exact_stats.output_row_count, expected);
        assert_eq!(exact_stats.row_count_confidence, Confidence::Estimated);

        let fallback_stats = derive_statistics(&fallback_agg, &memo, &empty_stats_input());
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
                aggregate_group_key_ids(&mut memo),
                vec![],
                aggregate_output_layout(),
                aggregate_output_columns(),
            )),
            children: vec![child],
        };
        let physical = MExpr {
            id: memo.next_expr_id(),
            op: Operator::PhysicalHashAggregate(PhysicalHashAggregateOp {
                mode: AggMode::Single,
                group_by: aggregate_group_key_ids(&mut memo),
                aggregates: vec![],
                output_layout: aggregate_output_layout(),
                output_columns: aggregate_output_columns(),
                is_merge: vec![],
            }),
            children: vec![child],
        };

        for stats in [
            derive_statistics(&logical, &memo, &empty_stats_input()),
            derive_statistics(&physical, &memo, &empty_stats_input()),
        ] {
            assert_row_count_close(stats.output_row_count, expected);
            assert_eq!(
                stat_by_name(&stats.column_statistics, "k1")
                    .ndv_or_legacy_unknown_sentinel_for_test(),
                100.0
            );
            assert_eq!(
                stat_by_name(&stats.column_statistics, "k2")
                    .ndv_or_legacy_unknown_sentinel_for_test(),
                100.0
            );
            assert_eq!(
                stat_by_name(&stats.column_statistics, "k3")
                    .ndv_or_legacy_unknown_sentinel_for_test(),
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
                AggregateOutputLayout::new(vec![stats_output_column(4, "count(*)")], vec![]),
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
                output_layout: AggregateOutputLayout::new(
                    vec![stats_output_column(4, "count(*)")],
                    vec![],
                ),
                output_columns: vec![stats_output_column(4, "count(*)")],
                is_merge: vec![],
            }),
            children: vec![child],
        };

        let logical_stats = derive_statistics(&logical, &memo, &empty_stats_input());
        assert_eq!(logical_stats.output_row_count, 1.0);
        assert_eq!(logical_stats.row_count_confidence, Confidence::Estimated);
        assert!(logical_stats.column_statistics.is_empty());

        let physical_stats = derive_statistics(&physical, &memo, &empty_stats_input());
        assert_eq!(physical_stats.output_row_count, 1.0);
        assert_eq!(physical_stats.row_count_confidence, Confidence::Estimated);
        assert!(physical_stats.column_statistics.is_empty());
    }

    #[test]
    fn derive_statistics_is_child_sensitive_across_groups() {
        use crate::sql::analysis::{ExprKind, OutputColumn, TypedExpr};
        use crate::sql::column_id::ColumnId;
        use crate::sql::optimizer::memo::{LogicalProperties, MExpr, Memo};
        use crate::sql::optimizer::operator::{
            AggMode, Operator, PhysicalHashAggregateOp, ValuesOp,
        };
        use crate::sql::optimizer::statistics::ColumnStatistic;

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
        // A leaf group with the given row_count and a single group-by column (id=1).
        fn child_with_stats(memo: &mut Memo, rows: f64, ndv: f64) -> usize {
            let id = memo.next_expr_id();
            let g = memo.new_group(MExpr {
                id,
                op: Operator::LogicalValues(ValuesOp {
                    rows: vec![],
                    columns: vec![],
                }),
                children: vec![],
            });
            let mut props = LogicalProperties::new(vec![output_column(1, "k")], rows);
            props.column_statistics.insert(
                ColumnId::new_for_test(1),
                ColumnStatistic {
                    min_value: 0.0,
                    max_value: rows,
                    nulls_fraction: 0.0,
                    average_row_size: 8.0,
                    ..ColumnStatistic::for_test_with_ndv(ndv, Confidence::Exact)
                },
            );
            memo.groups[g].logical_props = Some(props);
            g
        }
        fn agg_over(child: usize, memo: &mut Memo) -> MExpr {
            let group_by = intern_exprs(&mut memo.scalars, &[col_ref(1, "k")]);
            let output_columns = vec![output_column(1, "k")];
            MExpr {
                id: memo.next_expr_id(), // id is irrelevant to derive_statistics
                op: Operator::PhysicalHashAggregate(PhysicalHashAggregateOp {
                    mode: AggMode::Single,
                    group_by,
                    aggregates: vec![],
                    output_layout: AggregateOutputLayout::new(output_columns.clone(), vec![]),
                    output_columns,
                    is_merge: vec![],
                }),
                children: vec![child],
            }
        }

        let mut memo = Memo::new();
        // big: rows=200, NDV=100 -> agg_group_rows = min(100, 200) = 100.
        let big = child_with_stats(&mut memo, 200.0, 100.0);
        // small: rows=100, NDV=50 -> agg_group_rows = min(50, 100) = 50.
        let small = child_with_stats(&mut memo, 100.0, 50.0);

        let big_stats = derive_statistics(&agg_over(big, &mut memo), &memo, &empty_stats_input());
        let small_stats =
            derive_statistics(&agg_over(small, &mut memo), &memo, &empty_stats_input());

        // Same op, different children -> different derived statistics. These two
        // aggregates have different children, so they are DIFFERENT memo groups;
        // this asserts derive_statistics is child-sensitive ACROSS groups. It does
        // NOT imply within-group per-expr variation: members of one group are
        // logically equivalent, so the per-group collapsed statistic is correct
        // (search now reads the per-group stat, not a per-expr derivation).
        assert!(
            big_stats.output_row_count > small_stats.output_row_count,
            "per-expr own_stats must differ: big={} small={}",
            big_stats.output_row_count,
            small_stats.output_row_count
        );
        assert_ne!(big_stats.output_row_count, small_stats.output_row_count);
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
            ..ColumnStatistic::for_test_with_ndv(ndv, Confidence::Exact)
        }
    }

    fn filter_ndv_child_group(memo: &mut Memo) -> usize {
        use crate::sql::optimizer::memo::LogicalProperties;
        use crate::sql::optimizer::operator::{Operator, ValuesOp};

        let group = memo.new_group(MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalValues(ValuesOp {
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
        assert_eq!(payload.ndv_or_legacy_unknown_sentinel_for_test(), 100.0);
        assert_eq!(payload.min_value, 7.0);
        assert_eq!(payload.max_value, 77.0);
    }

    #[test]
    fn logical_filter_caps_payload_ndv_at_output_rows() {
        use crate::sql::optimizer::operator::{FilterOp, Operator};

        let mut memo = Memo::new();
        let child = filter_ndv_child_group(&mut memo);
        let filter = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalFilter(FilterOp {
                predicate: intern_typed(
                    &mut memo.scalars,
                    &eq_expr(col_ref("filter_col"), int_lit(1)),
                ),
            }),
            children: vec![child],
        };

        assert_filter_caps_payload_ndv(derive_statistics(&filter, &memo, &empty_stats_input()));
    }

    #[test]
    fn physical_filter_caps_payload_ndv_at_output_rows() {
        use crate::sql::optimizer::operator::{FilterOp, Operator};

        let mut memo = Memo::new();
        let child = filter_ndv_child_group(&mut memo);
        let filter = MExpr {
            id: memo.next_expr_id(),
            op: Operator::PhysicalFilter(FilterOp {
                predicate: intern_typed(
                    &mut memo.scalars,
                    &eq_expr(col_ref("filter_col"), int_lit(1)),
                ),
            }),
            children: vec![child],
        };

        assert_filter_caps_payload_ndv(derive_statistics(&filter, &memo, &empty_stats_input()));
    }

    #[test]
    fn scan_group_stats() {
        let (name, ts) = make_table_stats("orders", 100_000, &[("id", 100_000.0)]);
        let mut table_stats = HashMap::new();
        table_stats.insert(name, ts);

        let plan = scan_plan("orders", &["id"]);
        let mut memo = Memo::new();
        logical_plan_to_memo_for_test(&plan, &mut memo);
        derive_group_statistics(&mut memo, &query_stats_input_for_test(&table_stats));

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
        let plan = LogicalPlanNode::new(
            LogicalPlanKind::Filter(PlanFilterNode { predicate: pred }),
            vec![scan],
            None,
        );

        let mut memo = Memo::new();
        logical_plan_to_memo_for_test(&plan, &mut memo);
        derive_group_statistics(&mut memo, &query_stats_input_for_test(&table_stats));

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
            JoinDistribution, Operator, PhysicalHashJoinEqCondition, PhysicalHashJoinOp, ValuesOp,
        };

        fn column_stat(ndv: f64) -> ColumnStatistic {
            ColumnStatistic {
                min_value: 0.0,
                max_value: 1_000.0,
                nulls_fraction: 0.0,
                average_row_size: 8.0,
                ..ColumnStatistic::for_test_with_ndv(ndv, Confidence::Exact)
            }
        }

        fn values_group(memo: &mut Memo, stats: &[(&str, f64)]) -> usize {
            let id = memo.next_expr_id();
            let group = memo.new_group(MExpr {
                id,
                op: Operator::LogicalValues(ValuesOp {
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
                        left: intern_typed(&mut memo.scalars, &col_ref("l_k1")),
                        right: intern_typed(&mut memo.scalars, &col_ref("r_k1")),
                        null_safe: false,
                    },
                    PhysicalHashJoinEqCondition {
                        left: intern_typed(&mut memo.scalars, &col_ref("l_k2")),
                        right: intern_typed(&mut memo.scalars, &col_ref("r_k2")),
                        null_safe: false,
                    },
                ],
                other_condition: None,
                distribution: JoinDistribution::Broadcast,
            }),
            children: vec![left, right],
        };

        let stats = derive_statistics(&join, &memo, &empty_stats_input());

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
            JoinDistribution, Operator, PhysicalHashJoinEqCondition, PhysicalHashJoinOp, ValuesOp,
        };

        fn column_stat(ndv: f64) -> ColumnStatistic {
            ColumnStatistic {
                min_value: 0.0,
                max_value: 1_000.0,
                nulls_fraction: 0.0,
                average_row_size: 8.0,
                ..ColumnStatistic::for_test_with_ndv(ndv, Confidence::Exact)
            }
        }

        fn values_group(memo: &mut Memo, rows: f64, stats: &[(&str, f64)]) -> usize {
            let group = memo.new_group(MExpr {
                id: memo.next_expr_id(),
                op: Operator::LogicalValues(ValuesOp {
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
                    left: intern_typed(&mut memo.scalars, &col_ref("l_key")),
                    right: intern_typed(&mut memo.scalars, &col_ref("r_key")),
                    null_safe: false,
                }],
                other_condition: None,
                distribution: JoinDistribution::Broadcast,
            }),
            children: vec![left, right],
        };

        let stats = derive_statistics(&join, &memo, &empty_stats_input());

        assert_eq!(stats.output_row_count, 40.0);
        assert_eq!(
            stat_by_name(&stats.column_statistics, "l_key")
                .ndv_or_legacy_unknown_sentinel_for_test(),
            20.0
        );
        assert_eq!(
            stat_by_name(&stats.column_statistics, "r_key")
                .ndv_or_legacy_unknown_sentinel_for_test(),
            20.0
        );
        assert_eq!(
            stat_by_name(&stats.column_statistics, "l_payload")
                .ndv_or_legacy_unknown_sentinel_for_test(),
            40.0
        );
        assert_eq!(
            stat_by_name(&stats.column_statistics, "r_payload")
                .ndv_or_legacy_unknown_sentinel_for_test(),
            40.0
        );
    }

    #[test]
    fn logical_join_stats_use_shared_cardinality_estimator_for_condition() {
        use crate::sql::optimizer::memo::{LogicalProperties, MExpr};
        use crate::sql::optimizer::operator::{LogicalJoinOp, Operator, ValuesOp};

        fn column_stat(ndv: f64) -> ColumnStatistic {
            ColumnStatistic {
                min_value: 0.0,
                max_value: 1_000.0,
                nulls_fraction: 0.0,
                average_row_size: 8.0,
                ..ColumnStatistic::for_test_with_ndv(ndv, Confidence::Exact)
            }
        }

        fn values_group(memo: &mut Memo, rows: f64, stats: &[(&str, f64)]) -> usize {
            let id = memo.next_expr_id();
            let group = memo.new_group(MExpr {
                id,
                op: Operator::LogicalValues(ValuesOp {
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
                condition: Some(intern_typed(
                    &mut memo.scalars,
                    &eq_expr(col_ref("l_key"), col_ref("r_key")),
                )),
            }),
            children: vec![left, right],
        };

        let stats = derive_statistics(&join, &memo, &empty_stats_input());

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
        use crate::sql::optimizer::operator::{LogicalJoinOp, Operator, ValuesOp};

        fn column_stat(ndv: f64) -> ColumnStatistic {
            ColumnStatistic {
                min_value: 0.0,
                max_value: 1_000.0,
                nulls_fraction: 0.0,
                average_row_size: 8.0,
                ..ColumnStatistic::for_test_with_ndv(ndv, Confidence::Exact)
            }
        }

        fn values_group(memo: &mut Memo, rows: f64, stats: &[(&str, f64)]) -> usize {
            let group = memo.new_group(MExpr {
                id: memo.next_expr_id(),
                op: Operator::LogicalValues(ValuesOp {
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
                condition: Some(intern_typed(
                    &mut memo.scalars,
                    &eq_expr(col_ref("l_key"), col_ref("r_key")),
                )),
            }),
            children: vec![left, right],
        };

        let stats = derive_statistics(&join, &memo, &empty_stats_input());

        assert_eq!(stats.output_row_count, 400_000.0);
        assert_eq!(
            stat_by_name(&stats.column_statistics, "l_key")
                .ndv_or_legacy_unknown_sentinel_for_test(),
            20.0
        );
        assert_eq!(
            stat_by_name(&stats.column_statistics, "r_key")
                .ndv_or_legacy_unknown_sentinel_for_test(),
            20.0
        );
        assert_eq!(
            stat_by_name(&stats.column_statistics, "l_payload")
                .ndv_or_legacy_unknown_sentinel_for_test(),
            400_000.0
        );
        assert_eq!(
            stat_by_name(&stats.column_statistics, "r_payload")
                .ndv_or_legacy_unknown_sentinel_for_test(),
            400_000.0
        );
    }

    #[test]
    fn logical_join_applies_only_residual_non_equi_selectivity() {
        use crate::sql::optimizer::memo::{LogicalProperties, MExpr};
        use crate::sql::optimizer::operator::{LogicalJoinOp, Operator, ValuesOp};

        fn column_stat(ndv: f64) -> ColumnStatistic {
            ColumnStatistic {
                min_value: 0.0,
                max_value: 1_000.0,
                nulls_fraction: 0.0,
                average_row_size: 8.0,
                ..ColumnStatistic::for_test_with_ndv(ndv, Confidence::Exact)
            }
        }

        fn values_group(memo: &mut Memo, rows: f64, stats: &[(&str, f64)]) -> usize {
            let group = memo.new_group(MExpr {
                id: memo.next_expr_id(),
                op: Operator::LogicalValues(ValuesOp {
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
                condition: Some(intern_typed(&mut memo.scalars, &condition)),
            }),
            children: vec![left, right],
        };

        let stats = derive_statistics(&join, &memo, &empty_stats_input());

        assert_eq!(stats.output_row_count, 5.0);
    }

    #[test]
    fn logical_join_reversed_condition_merges_key_equivalence() {
        use crate::sql::optimizer::memo::{LogicalProperties, MExpr};
        use crate::sql::optimizer::operator::{LogicalJoinOp, Operator, ValuesOp};

        fn column_stat(ndv: f64) -> ColumnStatistic {
            ColumnStatistic {
                min_value: 0.0,
                max_value: 1_000.0,
                nulls_fraction: 0.0,
                average_row_size: 8.0,
                ..ColumnStatistic::for_test_with_ndv(ndv, Confidence::Exact)
            }
        }

        fn values_group(memo: &mut Memo, rows: f64, stats: &[(&str, f64)]) -> usize {
            let group = memo.new_group(MExpr {
                id: memo.next_expr_id(),
                op: Operator::LogicalValues(ValuesOp {
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
                condition: Some(intern_typed(
                    &mut memo.scalars,
                    &eq_expr(col_ref("r_key"), col_ref("l_key")),
                )),
            }),
            children: vec![left, right],
        };

        let stats = derive_statistics(&join, &memo, &empty_stats_input());

        assert_eq!(
            stat_by_name(&stats.column_statistics, "l_key")
                .ndv_or_legacy_unknown_sentinel_for_test(),
            20.0
        );
        assert_eq!(
            stat_by_name(&stats.column_statistics, "r_key")
                .ndv_or_legacy_unknown_sentinel_for_test(),
            20.0
        );
    }

    #[test]
    fn logical_join_nested_and_condition_merges_multiple_key_pairs() {
        use crate::sql::optimizer::memo::{LogicalProperties, MExpr};
        use crate::sql::optimizer::operator::{LogicalJoinOp, Operator, ValuesOp};

        fn column_stat(ndv: f64) -> ColumnStatistic {
            ColumnStatistic {
                min_value: 0.0,
                max_value: 1_000.0,
                nulls_fraction: 0.0,
                average_row_size: 8.0,
                ..ColumnStatistic::for_test_with_ndv(ndv, Confidence::Exact)
            }
        }

        fn values_group(memo: &mut Memo, rows: f64, stats: &[(&str, f64)]) -> usize {
            let group = memo.new_group(MExpr {
                id: memo.next_expr_id(),
                op: Operator::LogicalValues(ValuesOp {
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
                condition: Some(intern_typed(&mut memo.scalars, &condition)),
            }),
            children: vec![left, right],
        };

        let stats = derive_statistics(&join, &memo, &empty_stats_input());

        assert_eq!(stats.output_row_count, 8_000.0);
        assert_eq!(
            stat_by_name(&stats.column_statistics, "l_k1")
                .ndv_or_legacy_unknown_sentinel_for_test(),
            20.0
        );
        assert_eq!(
            stat_by_name(&stats.column_statistics, "r_k1")
                .ndv_or_legacy_unknown_sentinel_for_test(),
            20.0
        );
        assert_eq!(
            stat_by_name(&stats.column_statistics, "l_k2")
                .ndv_or_legacy_unknown_sentinel_for_test(),
            50.0
        );
        assert_eq!(
            stat_by_name(&stats.column_statistics, "r_k2")
                .ndv_or_legacy_unknown_sentinel_for_test(),
            50.0
        );
    }

    #[test]
    fn logical_join_unknown_key_ndv_does_not_collapse_real_side_to_one() {
        use crate::sql::optimizer::memo::{LogicalProperties, MExpr};
        use crate::sql::optimizer::operator::{LogicalJoinOp, Operator, ValuesOp};

        fn values_group(memo: &mut Memo, rows: f64, name: &str, stat: ColumnStatistic) -> usize {
            let group = memo.new_group(MExpr {
                id: memo.next_expr_id(),
                op: Operator::LogicalValues(ValuesOp {
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
            ..ColumnStatistic::for_test_with_ndv(100.0, Confidence::Exact)
        };

        let mut memo = Memo::new();
        let left = values_group(&mut memo, 10_000.0, "l_key", real_key);
        let right = values_group(&mut memo, 4_000.0, "r_key", ColumnStatistic::unknown());
        let join = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalJoin(LogicalJoinOp {
                join_type: JoinKind::Inner,
                condition: Some(intern_typed(
                    &mut memo.scalars,
                    &eq_expr(col_ref("l_key"), col_ref("r_key")),
                )),
            }),
            children: vec![left, right],
        };

        let stats = derive_statistics(&join, &memo, &empty_stats_input());

        assert_eq!(
            stat_by_name(&stats.column_statistics, "l_key")
                .ndv_or_legacy_unknown_sentinel_for_test(),
            100.0
        );
        assert!(
            stat_by_name(&stats.column_statistics, "r_key")
                .ndv_value()
                .is_none()
        );
        assert_eq!(
            stat_by_name(&stats.column_statistics, "r_key")
                .ndv_or_legacy_unknown_sentinel_for_test(),
            1.0
        );
        assert_eq!(
            stat_by_name(&stats.column_statistics, "l_key").confidence,
            Confidence::Exact
        );
        assert_eq!(
            stat_by_name(&stats.column_statistics, "r_key").confidence,
            Confidence::Fallback
        );
    }

    #[test]
    fn p4_self_join_same_name_columns_keep_distinct_statistics() {
        use crate::sql::optimizer::memo::{LogicalProperties, MExpr};
        use crate::sql::optimizer::operator::{LogicalJoinOp, Operator, ValuesOp};

        fn column_stat(ndv: f64) -> ColumnStatistic {
            ColumnStatistic {
                min_value: 0.0,
                max_value: 1_000.0,
                nulls_fraction: 0.0,
                average_row_size: 8.0,
                ..ColumnStatistic::for_test_with_ndv(ndv, Confidence::Exact)
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
                op: Operator::LogicalValues(ValuesOp {
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
                condition: Some(intern_typed(
                    &mut memo.scalars,
                    &eq_expr(col_ref_with_id(left_id), col_ref_with_id(right_id)),
                )),
            }),
            children: vec![left, right],
        };

        let stats = derive_statistics(&join, &memo, &empty_stats_input());

        assert_eq!(stats.output_row_count, 400_000.0);
        assert_eq!(
            stats.column_statistics[&left_id].ndv_or_legacy_unknown_sentinel_for_test(),
            20.0
        );
        assert_eq!(
            stats.column_statistics[&right_id].ndv_or_legacy_unknown_sentinel_for_test(),
            20.0
        );
    }

    #[test]
    fn physical_nest_loop_join_stats_use_shared_cardinality_estimator_for_non_equi_semi() {
        use crate::sql::optimizer::memo::{LogicalProperties, MExpr};
        use crate::sql::optimizer::operator::{Operator, PhysicalNestLoopJoinOp, ValuesOp};

        fn column_stat(ndv: f64) -> ColumnStatistic {
            ColumnStatistic {
                min_value: 0.0,
                max_value: 1_000.0,
                nulls_fraction: 0.0,
                average_row_size: 8.0,
                ..ColumnStatistic::for_test_with_ndv(ndv, Confidence::Exact)
            }
        }

        fn values_group(memo: &mut Memo, rows: f64, stats: &[(&str, f64)]) -> usize {
            let id = memo.next_expr_id();
            let group = memo.new_group(MExpr {
                id,
                op: Operator::LogicalValues(ValuesOp {
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
                condition: Some(intern_typed(
                    &mut memo.scalars,
                    &eq_expr(col_ref("l_filter"), int_lit(7)),
                )),
            }),
            children: vec![left, right],
        };

        let stats = derive_statistics(&join, &memo, &empty_stats_input());

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
        use crate::sql::optimizer::operator::{Operator, PhysicalNestLoopJoinOp, ValuesOp};

        fn column_stat(ndv: f64) -> ColumnStatistic {
            ColumnStatistic {
                min_value: 0.0,
                max_value: 1_000.0,
                nulls_fraction: 0.0,
                average_row_size: 8.0,
                ..ColumnStatistic::for_test_with_ndv(ndv, Confidence::Exact)
            }
        }

        fn values_group(memo: &mut Memo, rows: f64, stats: &[(&str, f64)]) -> usize {
            let id = memo.next_expr_id();
            let group = memo.new_group(MExpr {
                id,
                op: Operator::LogicalValues(ValuesOp {
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
                condition: Some(intern_typed(
                    &mut memo.scalars,
                    &eq_expr(col_ref("r_filter"), int_lit(7)),
                )),
            }),
            children: vec![left, right],
        };

        let stats = derive_statistics(&join, &memo, &empty_stats_input());

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
        use crate::sql::optimizer::operator::{Operator, ValuesOp, WindowOp};

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
                op: Operator::LogicalValues(ValuesOp {
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
                    ..ColumnStatistic::for_test_with_ndv(10.0, Confidence::Exact)
                },
            );
            memo.groups[group].logical_props = Some(props);
            group
        }

        fn assert_window_stats(stats: Statistics) {
            assert_eq!(stats.output_row_count, 25.0);
            assert_eq!(stats.row_count_confidence, Confidence::Exact);
            assert_eq!(
                stat_by_name(&stats.column_statistics, "base")
                    .ndv_or_legacy_unknown_sentinel_for_test(),
                10.0
            );
            let row_number = stat_by_name(&stats.column_statistics, "rn");
            assert_eq!(row_number.confidence, Confidence::Fallback);
            assert_eq!(row_number.ndv_or_legacy_unknown_sentinel_for_test(), 1.0);
        }

        let mut memo = Memo::new();
        let child = child_group(&mut memo);
        let logical_window_exprs = intern_window_exprs(&mut memo.scalars, &[window_expr("rn")]);
        let logical_window = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalWindow(WindowOp {
                window_exprs: logical_window_exprs,
                output_columns: vec![stats_output_column(1, "base"), stats_output_column(2, "rn")],
            }),
            children: vec![child],
        };
        assert_window_stats(derive_statistics(
            &logical_window,
            &memo,
            &empty_stats_input(),
        ));

        let physical_window_exprs = intern_window_exprs(&mut memo.scalars, &[window_expr("rn")]);
        let physical_window = MExpr {
            id: memo.next_expr_id(),
            op: Operator::PhysicalWindow(WindowOp {
                window_exprs: physical_window_exprs,
                output_columns: vec![stats_output_column(1, "base"), stats_output_column(2, "rn")],
            }),
            children: vec![child],
        };
        assert_window_stats(derive_statistics(
            &physical_window,
            &memo,
            &empty_stats_input(),
        ));
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

        let plan = LogicalPlanNode::new(
            LogicalPlanKind::Join(LogicalJoinNode {
                join_type: JoinKind::Inner,
                condition: Some(cond),
            }),
            vec![left, right],
            None,
        );

        let mut memo = Memo::new();
        logical_plan_to_memo_for_test(&plan, &mut memo);
        derive_group_statistics(&mut memo, &query_stats_input_for_test(&table_stats));

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
        let plan = LogicalPlanNode::new(
            LogicalPlanKind::Aggregate(LogicalAggregateNode {
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
            }),
            vec![scan],
            None,
        );

        let mut memo = Memo::new();
        logical_plan_to_memo_for_test(&plan, &mut memo);
        derive_group_statistics(&mut memo, &query_stats_input_for_test(&table_stats));

        // Agg group: real NDV(status)=5 now flows through child_statistics,
        // so output = min(5, 100000) = 5.
        let agg_props = memo.groups[1].logical_props.as_ref().unwrap();
        assert!((agg_props.row_count - 5.0).abs() < 1.0);
    }

    #[test]
    fn limit_group_stats() {
        let (name, ts) = make_table_stats("t", 100_000, &[("a", 100.0)]);
        let mut table_stats = HashMap::new();
        table_stats.insert(name, ts);

        let scan = scan_plan("t", &["a"]);
        let plan = LogicalPlanNode::new(
            LogicalPlanKind::Limit(PlanLimitNode {
                limit: Some(10),
                offset: None,
            }),
            vec![scan],
            None,
        );

        let mut memo = Memo::new();
        logical_plan_to_memo_for_test(&plan, &mut memo);
        derive_group_statistics(&mut memo, &query_stats_input_for_test(&table_stats));

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
        let produce = LogicalPlanNode::new(
            LogicalPlanKind::CTEProduce(PlanCTEProduceNode {
                cte_id: 1,
                output_columns: vec![OutputColumn {
                    column_id: ColumnId::UNSET,
                    name: "id".to_string(),
                    data_type: DataType::Int32,
                    nullable: false,
                    is_internal: false,
                }],
            }),
            vec![scan],
            None,
        );
        let consume = LogicalPlanNode::new(
            LogicalPlanKind::CTEConsume(PlanCTEConsumeNode {
                cte_id: 1,
                alias: "cte_orders".to_string(),
                output_columns: vec![OutputColumn {
                    column_id: ColumnId::UNSET,
                    name: "id".to_string(),
                    data_type: DataType::Int32,
                    nullable: false,
                    is_internal: false,
                }],
                producer_column_ids: vec![ColumnId::UNSET],
            }),
            vec![],
            None,
        );
        let anchor = LogicalPlanNode::new(
            LogicalPlanKind::CTEAnchor(PlanCTEAnchorNode { cte_id: 1 }),
            vec![produce, consume],
            None,
        );

        let mut memo = Memo::new();
        logical_plan_to_memo_for_test(&anchor, &mut memo);
        derive_group_statistics(&mut memo, &query_stats_input_for_test(&table_stats));

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
    fn cte_consume_remaps_produce_column_statistics_to_consume_columns() {
        use crate::sql::optimizer::memo::{LogicalProperties, MExpr};
        use crate::sql::optimizer::operator::{CTEConsumeOp, Operator, ValuesOp};

        let cte_id: crate::sql::analysis::cte::CteId = 1;
        let mut memo = Memo::new();

        // Producer group: output column id 1 ("customer_id"), NDV 50_000, 100_000 rows.
        let produce_group = memo.new_group(MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalValues(ValuesOp {
                rows: vec![],
                columns: vec![],
            }),
            children: vec![],
        });
        let mut produce_props =
            LogicalProperties::new(vec![stats_output_column(1, "customer_id")], 100_000.0);
        produce_props.row_count_confidence = Confidence::Estimated;
        produce_props.column_statistics.insert(
            ColumnId::new_for_test(1),
            set_op_column_stat(0.0, 50_000.0, 0.0, 8.0, 50_000.0, Confidence::Estimated),
        );
        memo.groups[produce_group].logical_props = Some(produce_props);
        memo.cte_produce_groups.insert(cte_id, produce_group);

        // Consume re-exposes the producer column under a DIFFERENT column id (7),
        // exactly as a second `year_total` reference would in tpc-ds q4.
        let consume = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalCTEConsume(CTEConsumeOp {
                cte_id,
                alias: "t_s_firstyear".to_string(),
                output_columns: vec![stats_output_column(7, "customer_id")],
                producer_column_ids: vec![ColumnId::new_for_test(1)],
            }),
            children: vec![],
        };

        let stats = derive_statistics(&consume, &memo, &empty_stats_input());

        assert!(
            (stats.output_row_count - 100_000.0).abs() < 1.0,
            "row count should still propagate from producer, got {}",
            stats.output_row_count
        );

        // The producer NDV must reach the consume keyed by the CONSUME column id (7).
        // Without this, a self-join on the CTE key sees no NDV and the join estimator
        // falls back to a cross-product, exploding cardinality (tpc-ds q4/q11/q31/q74).
        let stat = stats
            .column_statistics
            .get(&ColumnId::new_for_test(7))
            .expect("consume must carry producer column stats remapped to its own column id");
        assert!(
            (stat.ndv_or_legacy_unknown_sentinel_for_test() - 50_000.0).abs() < 1.0,
            "expected propagated NDV 50000, got {}",
            stat.ndv_or_legacy_unknown_sentinel_for_test()
        );
        // The producer-side column id must not leak into the consume's statistics.
        assert!(
            !stats
                .column_statistics
                .contains_key(&ColumnId::new_for_test(1)),
            "producer column id must not leak into consume stats"
        );
    }

    #[test]
    fn cte_self_join_on_key_does_not_explode_to_cross_product() {
        // Reproduces the tpc-ds q4/q11/q31/q74 shape: a `year_total`-style CTE
        // consumed twice and self-joined on its grouping key (customer_id).
        // Before producer column stats reached the consume, the join keys had
        // no NDV and the estimator cross-producted the two consumes:
        // 250_000 * 250_000 * 0.25 ~= 1.56e10. With the producer NDV propagated,
        // the equi-join collapses to |L|*|R|/ndv ~= 694k.
        use crate::sql::optimizer::memo::{LogicalProperties, MExpr};
        use crate::sql::optimizer::operator::{
            CTEConsumeOp, JoinDistribution, Operator, PhysicalHashJoinEqCondition,
            PhysicalHashJoinOp, ValuesOp,
        };

        fn col_ref_id(id: u32, name: &str) -> TypedExpr {
            TypedExpr {
                kind: ExprKind::ColumnRef {
                    column_id: ColumnId::new_for_test(id),
                    qualifier: None,
                    column: name.to_string(),
                },
                data_type: DataType::Int32,
                nullable: false,
            }
        }

        let cte_id: crate::sql::analysis::cte::CteId = 1;
        let mut memo = Memo::new();

        // Producer: 250_000 rows grouped by customer_id (NDV 90_000), id 100.
        let produce_group = memo.new_group(MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalValues(ValuesOp {
                rows: vec![],
                columns: vec![],
            }),
            children: vec![],
        });
        let mut produce_props =
            LogicalProperties::new(vec![stats_output_column(100, "customer_id")], 250_000.0);
        produce_props.row_count_confidence = Confidence::Estimated;
        produce_props.column_statistics.insert(
            ColumnId::new_for_test(100),
            set_op_column_stat(0.0, 90_000.0, 0.0, 8.0, 90_000.0, Confidence::Estimated),
        );
        memo.groups[produce_group].logical_props = Some(produce_props);
        memo.cte_produce_groups.insert(cte_id, produce_group);

        // Two consumes re-expose customer_id under distinct ids (701, 801),
        // each derived through the real consume path then stored on its group.
        let mut consume_group = |col_id: u32, alias: &str| -> usize {
            let expr = MExpr {
                id: memo.next_expr_id(),
                op: Operator::LogicalCTEConsume(CTEConsumeOp {
                    cte_id,
                    alias: alias.to_string(),
                    output_columns: vec![stats_output_column(col_id, "customer_id")],
                    producer_column_ids: vec![ColumnId::new_for_test(100)],
                }),
                children: vec![],
            };
            let derived = derive_statistics(&expr, &memo, &empty_stats_input());
            let group = memo.new_group(expr);
            let mut props = LogicalProperties::new(
                vec![stats_output_column(col_id, "customer_id")],
                derived.output_row_count,
            );
            props.row_count_confidence = derived.row_count_confidence;
            props.column_statistics = derived.column_statistics;
            memo.groups[group].logical_props = Some(props);
            group
        };
        let left = consume_group(701, "t_s_firstyear");
        let right = consume_group(801, "t_s_secyear");

        let join = MExpr {
            id: memo.next_expr_id(),
            op: Operator::PhysicalHashJoin(PhysicalHashJoinOp {
                join_type: JoinKind::Inner,
                eq_conditions: vec![PhysicalHashJoinEqCondition {
                    left: intern_typed(&mut memo.scalars, &col_ref_id(701, "customer_id")),
                    right: intern_typed(&mut memo.scalars, &col_ref_id(801, "customer_id")),
                    null_safe: false,
                }],
                other_condition: None,
                distribution: JoinDistribution::Broadcast,
            }),
            children: vec![left, right],
        };

        let stats = derive_statistics(&join, &memo, &empty_stats_input());

        // Bounded: nowhere near the ~1.56e10 cross-product. A loose upper bound
        // keeps the test robust to estimator-constant tuning while still failing
        // hard if the cross-product regression returns.
        assert!(
            stats.output_row_count < 2_000_000.0,
            "CTE self-join on a key must not cross-product; got {}",
            stats.output_row_count
        );
        assert!(
            stats.output_row_count > 100_000.0,
            "equi-join on the CTE key should stay in a sane range, got {}",
            stats.output_row_count
        );
    }

    #[test]
    fn cte_consume_propagates_produce_column_statistics_through_real_pipeline() {
        // End-to-end through logical_plan_to_memo + derive_group_statistics:
        // the producer's column NDV must reach the consume group, so a join on
        // the CTE key sees real key statistics instead of a Fallback NDV. The
        // hand-built unit test exercises the remap directly; this one proves the
        // real conversion + derivation pipeline populates and forwards the stats.
        let (name, ts) = make_table_stats("orders", 250_000, &[("id", 100_000.0)]);
        let mut table_stats = HashMap::new();
        table_stats.insert(name, ts);

        let scan = scan_plan("orders", &["id"]);
        let produce = LogicalPlanNode::new(
            LogicalPlanKind::CTEProduce(PlanCTEProduceNode {
                cte_id: 1,
                output_columns: vec![OutputColumn {
                    column_id: test_col_id("id"),
                    name: "id".to_string(),
                    data_type: DataType::Int32,
                    nullable: false,
                    is_internal: false,
                }],
            }),
            vec![scan],
            None,
        );
        let consume = LogicalPlanNode::new(
            LogicalPlanKind::CTEConsume(PlanCTEConsumeNode {
                cte_id: 1,
                alias: "cte_orders".to_string(),
                output_columns: vec![OutputColumn {
                    column_id: test_col_id("consume_id"),
                    name: "id".to_string(),
                    data_type: DataType::Int32,
                    nullable: false,
                    is_internal: false,
                }],
                producer_column_ids: vec![test_col_id("id")],
            }),
            vec![],
            None,
        );
        let anchor = LogicalPlanNode::new(
            LogicalPlanKind::CTEAnchor(PlanCTEAnchorNode { cte_id: 1 }),
            vec![produce, consume],
            None,
        );

        let mut memo = Memo::new();
        logical_plan_to_memo_for_test(&anchor, &mut memo);
        derive_group_statistics(&mut memo, &query_stats_input_for_test(&table_stats));

        // Group 2: CTEConsume — must now carry the producer's column statistics.
        let consume_props = memo.groups[2].logical_props.as_ref().unwrap();
        assert!((consume_props.row_count - 250_000.0).abs() < 1.0);
        assert!(
            !consume_props.column_statistics.is_empty(),
            "consume must carry producer column statistics through the real pipeline"
        );
        // The scan caps NDV(id) at its row count: min(100_000, 250_000) = 100_000.
        let propagated_ndv = consume_props
            .column_statistics
            .values()
            .map(|s| s.ndv_or_legacy_unknown_sentinel_for_test())
            .fold(0.0_f64, f64::max);
        assert!(
            (propagated_ndv - 100_000.0).abs() < 1.0,
            "consume should see the producer NDV (100000), got {}",
            propagated_ndv
        );
    }

    #[test]
    fn values_group_stats() {
        let plan = LogicalPlanNode::new(
            LogicalPlanKind::Values(PlanValuesNode {
                rows: vec![vec![], vec![], vec![]],
                columns: vec![OutputColumn {
                    column_id: ColumnId::UNSET,
                    name: "x".to_string(),
                    data_type: DataType::Int32,
                    nullable: false,
                    is_internal: false,
                }],
            }),
            vec![],
            None,
        );

        let mut memo = Memo::new();
        logical_plan_to_memo_for_test(&plan, &mut memo);
        derive_group_statistics(&mut memo, &empty_stats_input());

        let props = memo.groups[0].logical_props.as_ref().unwrap();
        assert!((props.row_count - 3.0).abs() < 0.01);
        assert_eq!(props.output_columns.len(), 1);
    }

    #[test]
    fn generate_series_synthesizes_exact_column_statistics() {
        use crate::sql::optimizer::memo::MExpr;
        use crate::sql::optimizer::operator::{GenerateSeriesOp, Operator};

        let col = ColumnId::new_for_test(42);
        let memo = Memo::new();
        let expr = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalGenerateSeries(GenerateSeriesOp {
                start: 1,
                end: 1000,
                step: 1,
                column_name: "gs".to_string(),
                alias: None,
                output_column_id: col,
            }),
            children: vec![],
        };

        let stats = derive_statistics(&expr, &memo, &empty_stats_input());

        assert!((stats.output_row_count - 1000.0).abs() < 1.0);
        // start=1, end=1000, step=1 -> 1000 distinct values in [1, 1000], all unique.
        let cs = stats
            .column_statistics
            .get(&col)
            .expect("generate_series output column must carry exact statistics");
        assert!(
            (cs.ndv_or_legacy_unknown_sentinel_for_test() - 1000.0).abs() < 1.0,
            "NDV should equal row count, got {}",
            cs.ndv_or_legacy_unknown_sentinel_for_test()
        );
        assert!((cs.min_value - 1.0).abs() < 1e-9, "min should be start");
        assert!((cs.max_value - 1000.0).abs() < 1e-9, "max should be end");
        assert_eq!(cs.confidence, Confidence::Exact);
    }

    #[test]
    fn values_synthesizes_exact_column_statistics_from_literals() {
        use crate::sql::optimizer::memo::MExpr;
        use crate::sql::optimizer::operator::{Operator, ValuesOp};

        let col = ColumnId::new_for_test(7);
        let mut memo = Memo::new();
        let rows = vec![
            intern_exprs(&mut memo.scalars, &[int_lit(1)]),
            intern_exprs(&mut memo.scalars, &[int_lit(2)]),
            intern_exprs(&mut memo.scalars, &[int_lit(3)]),
        ];
        let expr = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalValues(ValuesOp {
                rows,
                columns: vec![stats_output_column(7, "v")],
            }),
            children: vec![],
        };
        let stats = derive_statistics(&expr, &memo, &empty_stats_input());
        assert!((stats.output_row_count - 3.0).abs() < 1e-9);
        let cs = stats
            .column_statistics
            .get(&col)
            .expect("values column must carry literal-derived statistics");
        assert!(
            (cs.ndv_or_legacy_unknown_sentinel_for_test() - 3.0).abs() < 1e-9,
            "3 distinct literals -> NDV 3, got {}",
            cs.ndv_or_legacy_unknown_sentinel_for_test()
        );
        assert!((cs.min_value - 1.0).abs() < 1e-9);
        assert!((cs.max_value - 3.0).abs() < 1e-9);
        assert_eq!(cs.confidence, Confidence::Exact);
    }

    #[test]
    fn table_function_passes_through_child_column_statistics() {
        use crate::sql::optimizer::memo::{LogicalProperties, MExpr};
        use crate::sql::optimizer::operator::{Operator, TableFunctionOp, ValuesOp};

        let base = ColumnId::new_for_test(3);
        let mut memo = Memo::new();
        let child = memo.new_group(MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalValues(ValuesOp {
                rows: vec![],
                columns: vec![],
            }),
            children: vec![],
        });
        let mut props = LogicalProperties::new(vec![stats_output_column(3, "base")], 1000.0);
        props.row_count_confidence = Confidence::Estimated;
        props.column_statistics.insert(
            base,
            set_op_column_stat(0.0, 50.0, 0.0, 8.0, 50.0, Confidence::Estimated),
        );
        memo.groups[child].logical_props = Some(props);

        let tf = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalTableFunction(TableFunctionOp {
                function_name: "unnest".to_string(),
                args: vec![],
                output_columns: vec![],
                alias: None,
                is_left_join: true,
            }),
            children: vec![child],
        };
        let stats = derive_statistics(&tf, &memo, &empty_stats_input());
        let cs = stats
            .column_statistics
            .get(&base)
            .expect("table function must pass through child column statistics");
        assert!(
            (cs.ndv_or_legacy_unknown_sentinel_for_test() - 50.0).abs() < 1e-9,
            "child NDV should pass through, got {}",
            cs.ndv_or_legacy_unknown_sentinel_for_test()
        );
    }

    /// Build a leaf memo group exposing one column with the given NDV/row count.
    fn join_leaf_group(memo: &mut Memo, col_id: u32, ndv: f64, rows: f64) -> usize {
        use crate::sql::optimizer::memo::{LogicalProperties, MExpr};
        use crate::sql::optimizer::operator::{Operator, ValuesOp};
        let g = memo.new_group(MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalValues(ValuesOp {
                rows: vec![],
                columns: vec![],
            }),
            children: vec![],
        });
        let mut props = LogicalProperties::new(vec![stats_output_column(col_id, "c")], rows);
        props.row_count_confidence = Confidence::Estimated;
        props.column_statistics.insert(
            ColumnId::new_for_test(col_id),
            set_op_column_stat(0.0, ndv, 0.0, 8.0, ndv, Confidence::Estimated),
        );
        memo.groups[g].logical_props = Some(props);
        g
    }

    /// `(A join B) join C` over the three leaf groups, inner joins (no explicit
    /// condition — irrelevant for materialization/dedup tests).
    fn abc_join_tree(a: usize, b: usize, c: usize) -> crate::sql::optimizer::memo::JoinTree {
        use crate::sql::optimizer::memo::JoinTree;
        use crate::sql::optimizer::operator::LogicalJoinOp;
        let inner = || LogicalJoinOp {
            join_type: JoinKind::Inner,
            condition: None,
        };
        JoinTree::Join {
            left: Box::new(JoinTree::Join {
                left: Box::new(JoinTree::Leaf(a)),
                right: Box::new(JoinTree::Leaf(b)),
                op: inner(),
            }),
            right: Box::new(JoinTree::Leaf(c)),
            op: inner(),
        }
    }

    #[test]
    fn copy_in_join_tree_stamps_stats_on_new_groups() {
        let mut memo = Memo::new();
        let a = join_leaf_group(&mut memo, 1, 100.0, 1000.0);
        let b = join_leaf_group(&mut memo, 2, 100.0, 1000.0);
        let c = join_leaf_group(&mut memo, 3, 50.0, 500.0);
        let tree = abc_join_tree(a, b, c);

        let groups_before = memo.groups.len();
        let root = copy_in_join_tree(&mut memo, &tree, &empty_stats_input());

        // Two new groups materialized: (A join B) and the root.
        assert_eq!(memo.groups.len(), groups_before + 2);
        // Each new join group must have stamped logical_props at creation time;
        // otherwise implement() runs before the re-derive and bushy joins
        // degrade to NestLoop (M1).
        for gid in groups_before..memo.groups.len() {
            let props = memo.groups[gid].logical_props.as_ref();
            assert!(
                props.is_some(),
                "new join group {gid} must have stamped logical_props"
            );
            assert!(props.unwrap().row_count > 0.0);
        }
        assert!(memo.groups[root].logical_props.is_some());
    }

    #[test]
    fn copy_in_join_tree_dedups_repeated_subtrees() {
        let mut memo = Memo::new();
        let a = join_leaf_group(&mut memo, 1, 100.0, 1000.0);
        let b = join_leaf_group(&mut memo, 2, 100.0, 1000.0);
        let c = join_leaf_group(&mut memo, 3, 50.0, 500.0);
        let tree = abc_join_tree(a, b, c);

        let r1 = copy_in_join_tree(&mut memo, &tree, &empty_stats_input());
        let after_first = memo.groups.len();
        // Re-materializing the identical tree reuses every group: same root,
        // zero new groups (dedup via join_group_index).
        let r2 = copy_in_join_tree(&mut memo, &tree, &empty_stats_input());
        assert_eq!(r1, r2, "identical tree must dedup to the same root group");
        assert_eq!(
            memo.groups.len(),
            after_first,
            "re-copying an identical tree must not mint new groups"
        );
    }

    #[test]
    fn project_group_stats_preserve_project_item_output_column_id() {
        let out_id = ColumnId::new_for_test(42);
        let plan = LogicalPlanNode::new(
            LogicalPlanKind::Project(PlanProjectNode {
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
            }),
            vec![LogicalPlanNode::new(
                LogicalPlanKind::Values(PlanValuesNode {
                    rows: vec![vec![]],
                    columns: vec![],
                }),
                vec![],
                None,
            )],
            None,
        );

        let mut memo = Memo::new();
        logical_plan_to_memo_for_test(&plan, &mut memo);
        derive_group_statistics(&mut memo, &empty_stats_input());

        let props = memo.groups[1].logical_props.as_ref().unwrap();
        assert_eq!(props.output_columns.len(), 1);
        assert_eq!(props.output_columns[0].column_id, out_id);
    }

    fn col_stat(min: f64, max: f64, ndv: f64) -> ColumnStatistic {
        ColumnStatistic {
            min_value: min,
            max_value: max,
            nulls_fraction: 0.0,
            average_row_size: 8.0,
            ..ColumnStatistic::for_test_with_ndv(ndv, Confidence::Exact)
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
        let sel = estimate_selectivity_for_test(&pred, &cs);
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
        let sel = estimate_selectivity_for_test(&pred, &cs);
        assert!(
            sel > 0.85 && sel < 0.93,
            "not-between selectivity was {sel}"
        );
    }

    #[test]
    fn derive_group_statistics_skips_already_computed_groups() {
        use crate::sql::optimizer::memo::{LogicalProperties, MExpr, Memo};
        use crate::sql::optimizer::operator::{Operator, ValuesOp};

        let mut memo = Memo::new();

        // Group A: simulates a group computed by an earlier derive pass. The
        // sentinel row_count 999_999 is a value the real derivation would never
        // produce for an empty LogicalValues (which derives to 0).
        let group_a = memo.new_group(MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalValues(ValuesOp {
                rows: vec![],
                columns: vec![],
            }),
            children: vec![],
        });
        memo.groups[group_a].logical_props = Some(LogicalProperties::new(vec![], 999_999.0));

        // Group B: simulates a fresh group minted by implement() — logical_props=None.
        let group_b = memo.new_group(MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalValues(ValuesOp {
                rows: vec![],
                columns: vec![],
            }),
            children: vec![],
        });
        assert!(memo.groups[group_b].logical_props.is_none());

        derive_group_statistics(&mut memo, &empty_stats_input());

        // Group A was memoized/skipped — sentinel preserved (NOT recomputed to 0).
        assert_eq!(
            memo.groups[group_a]
                .logical_props
                .as_ref()
                .unwrap()
                .row_count,
            999_999.0,
            "already-computed group must be skipped, not recomputed"
        );
        // Group B (None) must still be derived.
        assert!(
            memo.groups[group_b].logical_props.is_some(),
            "fresh (None) group must still be derived"
        );
    }

    // -----------------------------------------------------------------------
    // pick_group_representative — lexicographic confidence argmax collapse
    // -----------------------------------------------------------------------

    /// Build a leaf-scan `MExpr` for `table` over a single column `col` by
    /// converting a scan plan into a throwaway memo and extracting its sole
    /// logical expr. Scans are leaves (`children: vec![]`), so the resulting
    /// `MExpr` carries no group references and can be inserted into any group.
    fn scan_mexpr(table: &str, col: &str) -> MExpr {
        let mut tmp = Memo::new();
        let g = logical_plan_to_memo_for_test(&scan_plan(table, &[col]), &mut tmp);
        tmp.groups[g].logical_exprs[0].clone()
    }

    /// A scan of a table registered with table stats derives `Exact`
    /// confidence; a scan of an unregistered table derives `Fallback`. This
    /// confirms two members of the SAME group can derive DIFFERENT confidences
    /// via the real `derive_statistics` (the precondition for the argmax tests).
    #[test]
    fn pick_representative_argmax_picks_higher_source_confidence_not_first() {
        // Only `with_stats` is registered, so its scan derives Exact; the
        // `no_stats` scan derives Fallback.
        let (name, ts) = make_table_stats("with_stats", 1_000, &[("a", 50.0)]);
        let mut table_stats = HashMap::new();
        table_stats.insert(name, ts);

        // Member at index 0 = Fallback (no stats); member at index 1 = Exact
        // (with stats). Higher confidence is deliberately NOT first, to prove
        // the pick is argmax and not `first()`.
        let mut memo = Memo::new();
        let group = memo.new_group(scan_mexpr("no_stats", "a"));
        memo.add_expr_to_group(group, scan_mexpr("with_stats", "a"));

        // Sanity: confirm the two members really do derive different confidence.
        let m0 = memo.groups[group].logical_exprs[0].clone();
        let m1 = memo.groups[group].logical_exprs[1].clone();
        assert_eq!(
            derive_statistics(&m0, &memo, &query_stats_input_for_test(&table_stats))
                .row_count_confidence,
            Confidence::Fallback,
            "index-0 member (unregistered table) must derive Fallback"
        );
        assert_eq!(
            derive_statistics(&m1, &memo, &query_stats_input_for_test(&table_stats))
                .row_count_confidence,
            Confidence::Exact,
            "index-1 member (registered table) must derive Exact"
        );

        // argmax must pick the Exact member (index 1), not first().
        let (chosen, stats) =
            pick_group_representative(&memo, group, &query_stats_input_for_test(&table_stats))
                .expect("non-empty group");
        assert_eq!(
            stats.row_count_confidence,
            Confidence::Exact,
            "argmax must pick the higher-confidence (Exact) member"
        );
        assert!(
            (stats.output_row_count - 1_000.0).abs() < 1.0,
            "stats must come from the with_stats scan (1000 rows), got {}",
            stats.output_row_count
        );
        assert!(
            matches!(&chosen.op, Operator::LogicalScan(s) if s.table.name == "with_stats"),
            "chosen member must be the with_stats scan, not first()"
        );

        // The cached logical_props (Site 1) must reflect the same argmax pick.
        derive_group_statistics_for(&mut memo, group, &query_stats_input_for_test(&table_stats));
        let props = memo.groups[group].logical_props.as_ref().unwrap();
        assert_eq!(props.row_count_confidence, Confidence::Exact);
        assert!((props.row_count - 1_000.0).abs() < 1.0);
    }

    /// An all-equal group (every member same source confidence) must keep the
    /// lowest index — `logical_exprs[0]` — exactly reproducing today's
    /// `first()` pick (zero-regression baseline).
    #[test]
    fn pick_representative_all_equal_degenerates_to_first() {
        let (name, ts) = make_table_stats("eq_tbl", 1_000, &[("a", 50.0)]);
        let mut table_stats = HashMap::new();
        table_stats.insert(name, ts);

        // Two scans of the SAME registered table: both Exact → equal key. We
        // distinguish them by column ("a" vs "b") so we can tell which member
        // was picked.
        let mut memo = Memo::new();
        let group = memo.new_group(scan_mexpr("eq_tbl", "a"));
        memo.add_expr_to_group(group, scan_mexpr("eq_tbl", "b"));

        let (chosen, _) =
            pick_group_representative(&memo, group, &query_stats_input_for_test(&table_stats))
                .expect("non-empty group");
        // Lowest index wins the tie → the "a" scan (logical_exprs[0]).
        assert!(
            matches!(&chosen.op, Operator::LogicalScan(s)
                if s.columns.iter().any(|c| c.name == "a")),
            "all-equal group must keep logical_exprs[0] (the 'a' scan)"
        );
    }

    /// Member-consistency: after `derive_group_statistics_for`, the cached
    /// `logical_props` (both row stats AND column stats) must come from the SAME
    /// member the argmax chose — proving Site 1 calls `derive_for_expr` with the
    /// chosen member rather than re-picking `first()` via `derive_for_group`.
    #[test]
    fn pick_representative_member_consistency_props_match_argmax_member() {
        // Distinguishing column stat: the Exact member carries a real NDV for
        // column "a"; the Fallback member carries only `unknown()` stats.
        let (name, ts) = make_table_stats("consistent_tbl", 1_000, &[("a", 50.0)]);
        let mut table_stats = HashMap::new();
        table_stats.insert(name, ts);

        // Index 0 = Fallback (first()); index 1 = Exact (argmax winner).
        let mut memo = Memo::new();
        let group = memo.new_group(scan_mexpr("unregistered_tbl", "a"));
        memo.add_expr_to_group(group, scan_mexpr("consistent_tbl", "a"));

        derive_group_statistics_for(&mut memo, group, &query_stats_input_for_test(&table_stats));
        let props = memo.groups[group].logical_props.as_ref().unwrap();

        // Row-level props come from the argmax (Exact) member.
        assert_eq!(props.row_count_confidence, Confidence::Exact);
        assert!((props.row_count - 1_000.0).abs() < 1.0);

        // Column-level props also come from the argmax member: the Exact scan's
        // column "a" has NDV=50 and Exact confidence. The first() member
        // (unregistered) would have produced only an `unknown()` column stat
        // (NDV not 50, Fallback confidence), so this distinguishes the source.
        let a = stat_by_name(&props.column_statistics, "a");
        assert!(
            (a.ndv_or_legacy_unknown_sentinel_for_test() - 50.0).abs() < 1e-9,
            "column stat NDV must come from the argmax member (50), got {}",
            a.ndv_or_legacy_unknown_sentinel_for_test()
        );
        assert_eq!(
            a.confidence,
            Confidence::Exact,
            "column stat confidence must come from the argmax (Exact) member, not first()"
        );
    }

    /// FFewerConj sub-tie-break: two tied inner-join members (same children,
    /// same source confidence) differing only in the number of join-condition
    /// conjuncts — the member with FEWER conjuncts wins.
    #[test]
    fn pick_representative_ffewerconj_prefers_fewer_join_conjuncts() {
        use crate::sql::analysis::ExprKind;
        use crate::sql::optimizer::memo::LogicalProperties;
        use crate::sql::optimizer::operator::{LogicalJoinOp, ValuesOp};

        let table_stats: HashMap<String, TableStatistics> = HashMap::new();

        let mut memo = Memo::new();

        // Two base leaf children with pre-baked props (so child_statistics reads
        // them and both join members get the SAME derived stats / source
        // confidence).
        let mk_leaf = |memo: &mut Memo| -> GroupId {
            let id = memo.next_expr_id();
            let g = memo.new_group(MExpr {
                id,
                op: Operator::LogicalValues(ValuesOp {
                    rows: vec![],
                    columns: vec![],
                }),
                children: vec![],
            });
            let mut props = LogicalProperties::new(vec![], 1_000.0);
            props.row_count_confidence = Confidence::Estimated;
            memo.groups[g].logical_props = Some(props);
            g
        };
        let left = mk_leaf(&mut memo);
        let right = mk_leaf(&mut memo);

        // Build two AND-tree predicates in the memo's scalar arena: one with a
        // single conjunct, one with two. The literals are arbitrary — only the
        // conjunct COUNT matters to FFewerConj.
        let lit = |v: i64| TypedExpr {
            data_type: DataType::Boolean,
            nullable: false,
            kind: ExprKind::Literal(LiteralValue::Int(v)),
        };
        let one_conjunct = intern_typed(&mut memo.scalars, &lit(1));
        let two_conjuncts = {
            let and = TypedExpr {
                data_type: DataType::Boolean,
                nullable: false,
                kind: ExprKind::BinaryOp {
                    op: BinOp::And,
                    left: Box::new(lit(1)),
                    right: Box::new(lit(2)),
                },
            };
            intern_typed(&mut memo.scalars, &and)
        };

        // Member 0 (first): TWO conjuncts. Member 1: ONE conjunct. Both inner
        // joins over the same children → tied key; FFewerConj must pick the
        // one-conjunct member (index 1), beating first().
        let id0 = memo.next_expr_id();
        let group = memo.new_group(MExpr {
            id: id0,
            op: Operator::LogicalJoin(LogicalJoinOp {
                join_type: JoinKind::Inner,
                condition: Some(two_conjuncts),
            }),
            children: vec![left, right],
        });
        let id1 = memo.next_expr_id();
        memo.add_expr_to_group(
            group,
            MExpr {
                id: id1,
                op: Operator::LogicalJoin(LogicalJoinOp {
                    join_type: JoinKind::Inner,
                    condition: Some(one_conjunct),
                }),
                children: vec![left, right],
            },
        );

        let (chosen, _) =
            pick_group_representative(&memo, group, &query_stats_input_for_test(&table_stats))
                .expect("non-empty group");
        match &chosen.op {
            Operator::LogicalJoin(j) => {
                let sid = j.condition.expect("join has a condition");
                let mut conjuncts = Vec::new();
                flatten_and_scalar(&memo.scalars, sid, &mut conjuncts);
                assert_eq!(
                    conjuncts.len(),
                    1,
                    "FFewerConj must pick the fewer-conjunct member (index 1), not first()"
                );
            }
            other => panic!("expected a LogicalJoin, got {other:?}"),
        }
    }

    /// Documents both the hazard and the fix for the per-group argmax guard.
    ///
    /// **Hazard**: when a strictly-higher-key member is appended to an already-
    /// derived group, the bulk `derive_group_statistics` guard SKIPS the group
    /// and never sees the new member → the cached `logical_props` stays stale.
    ///
    /// **Fix (eager re-derive)**: calling `derive_group_statistics_for` directly
    /// on the group at append time re-runs the argmax over ALL members, picks the
    /// higher-key one, and keeps `logical_props` `Some` throughout — satisfying
    /// the M1 invariant that `implement()` requires to read child-group column ids.
    #[test]
    fn eager_rederive_picks_late_higher_confidence_member() {
        // Only "registered_tbl" is in table_stats, so its scan derives Exact.
        // "unregistered_tbl" is absent, so its scan derives Fallback.
        let (name, ts) = make_table_stats("registered_tbl", 2_000, &[("a", 100.0)]);
        let mut table_stats = HashMap::new();
        table_stats.insert(name, ts);

        // ── Step 1: create a group whose ONLY member is Fallback. ──────────
        let mut memo = Memo::new();
        let group = memo.new_group(scan_mexpr("unregistered_tbl", "a"));

        // ── Step 2: derive via the bulk pass (first derivation). ──────────
        derive_group_statistics(&mut memo, &query_stats_input_for_test(&table_stats));

        // The group now has cached props — single member is Fallback.
        let props = memo.groups[group]
            .logical_props
            .as_ref()
            .expect("bulk pass must have stamped logical_props");
        assert_eq!(
            props.row_count_confidence,
            Confidence::Fallback,
            "initial derivation: only Fallback member present, must cache Fallback"
        );

        // ── Step 3: append a strictly-higher-key member (Exact). ──────────
        // This simulates a future producer (e.g. MV-rewrite) adding a member
        // with a higher source_confidence AFTER the group was already derived.
        memo.add_expr_to_group(group, scan_mexpr("registered_tbl", "a"));

        // ── Step 4: hazard — bulk pass SKIPS the group. ───────────────────
        // The guard sees `logical_props.is_some()` and does not re-run argmax.
        derive_group_statistics(&mut memo, &query_stats_input_for_test(&table_stats));
        let props_after_bulk = memo.groups[group]
            .logical_props
            .as_ref()
            .expect("logical_props must remain Some (M1 invariant)");
        assert_eq!(
            props_after_bulk.row_count_confidence,
            Confidence::Fallback,
            "hazard: bulk pass guard skipped the group — stale Fallback is still cached \
             even though an Exact member was appended"
        );

        // ── Step 5: fix — eager re-derive via derive_group_statistics_for. ─
        // This is what a real producer MUST call at append time.
        derive_group_statistics_for(&mut memo, group, &query_stats_input_for_test(&table_stats));

        let props_after_eager = memo.groups[group]
            .logical_props
            .as_ref()
            .expect("(a) logical_props must remain Some — eager re-derive MUST NOT go to None");
        assert_eq!(
            props_after_eager.row_count_confidence,
            Confidence::Exact,
            "(b) argmax must now select the Exact member appended in step 3"
        );
        assert!(
            (props_after_eager.row_count - 2_000.0).abs() < 1.0,
            "row_count must come from the registered_tbl scan (2000 rows), got {}",
            props_after_eager.row_count
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

#[cfg(test)]
mod sort_partition_limit_tests {
    use super::*;
    use crate::sql::analysis::{ExprKind, TypedExpr};
    use crate::sql::column_id::ColumnId;
    use crate::sql::planner::optimizer_bridge::scalar::intern_typed;
    use arrow::datatypes::DataType;

    fn intern_col_ref(scalars: &mut ScalarArena, col_id: u32, name: &str) -> ScalarId {
        let expr = TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: ColumnId::new_for_test(col_id),
                qualifier: None,
                column: name.to_string(),
            },
            data_type: DataType::Int32,
            nullable: false,
        };
        intern_typed(scalars, &expr)
    }

    fn col_stat_with_ndv(ndv: f64) -> ColumnStatistic {
        ColumnStatistic {
            min_value: 0.0,
            max_value: ndv,
            nulls_fraction: 0.0,
            average_row_size: 8.0,
            ..ColumnStatistic::for_test_with_ndv(ndv, Confidence::Exact)
        }
    }

    /// NDV=10 partition key, k=3 → output capped at 10*3=30 (< child_rows=1000).
    #[test]
    fn sort_partition_limit_caps_output_rows() {
        let partition_col_id = 42u32;
        let mut scalars = ScalarArena::new();
        let partition_expr = intern_col_ref(&mut scalars, partition_col_id, "pk");
        let mut col_stats = HashMap::new();
        col_stats.insert(
            ColumnId::new_for_test(partition_col_id),
            col_stat_with_ndv(10.0),
        );

        let result = sort_partition_limit_output_rows_scalar(
            &scalars,
            1000.0,
            &[partition_expr],
            &col_stats,
            3,
        );
        // ndv=10 * k=3 = 30, which is less than child_rows=1000.
        assert!(
            (result - 30.0).abs() < 1e-9,
            "expected 30.0 rows, got {result}"
        );
    }

    /// No partition_limit (None) → pass-through: child_rows unchanged.
    #[test]
    fn sort_without_partition_limit_is_passthrough() {
        // When partition_limit is None the operator arm returns child_stats directly.
        // We test this indirectly by verifying the helper is NOT called (i.e., it's
        // only invoked inside the Some(k) branch). Here we simply confirm that with
        // an empty partition_exprs and any k, we get child_rows back (the no-partition
        // fallback path inside the helper itself).
        let scalars = ScalarArena::new();
        let result =
            sort_partition_limit_output_rows_scalar(&scalars, 5000.0, &[], &HashMap::new(), 99);
        assert!(
            (result - 5000.0).abs() < 1e-9,
            "expected passthrough 5000.0, got {result}"
        );
    }

    /// cap never exceeds child_rows even when ndv*k > child_rows.
    #[test]
    fn sort_partition_limit_never_inflates_above_child_rows() {
        let partition_col_id = 43u32;
        let mut scalars = ScalarArena::new();
        let partition_expr = intern_col_ref(&mut scalars, partition_col_id, "pk2");
        let mut col_stats = HashMap::new();
        col_stats.insert(
            ColumnId::new_for_test(partition_col_id),
            col_stat_with_ndv(500.0),
        );

        // ndv=500 * k=10 = 5000, but child_rows=200 → must be capped at 200.
        let result = sort_partition_limit_output_rows_scalar(
            &scalars,
            200.0,
            &[partition_expr],
            &col_stats,
            10,
        );
        assert!(
            (result - 200.0).abs() < 1e-9,
            "expected cap at child_rows=200.0, got {result}"
        );
    }

    /// When no column stats are present, get_expr_ndv returns DEFAULT_EXPR_NDV=10.
    /// The output is still bounded by child_rows.
    #[test]
    fn sort_partition_limit_fallback_default_ndv_when_no_stats() {
        let mut scalars = ScalarArena::new();
        let partition_expr = intern_col_ref(&mut scalars, 99, "unknown_col");
        // No stats → get_expr_ndv returns 10.0 (DEFAULT_EXPR_NDV).
        // ndv=10 * k=5 = 50, child_rows=1000 → 50.
        let result = sort_partition_limit_output_rows_scalar(
            &scalars,
            1000.0,
            &[partition_expr],
            &HashMap::new(),
            5,
        );
        assert!(
            (result - 50.0).abs() < 1e-9,
            "expected 50.0 with default NDV, got {result}"
        );
    }
}
