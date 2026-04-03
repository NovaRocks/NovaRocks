//! Statistics derivation for Memo groups.
//!
//! Mirrors the logic in `sql::optimizer::cardinality` but operates on
//! Memo operators (`MExpr`) and reads child statistics from group logical
//! properties instead of recursing the `LogicalPlan` tree.

use std::collections::HashMap;

use super::memo::{LogicalProperties, MExpr, Memo};
use super::operator::Operator;
use crate::sql::optimizer::cardinality::estimate_selectivity;
use crate::sql::statistics::*;

// ---------------------------------------------------------------------------
// Default selectivity constant for scan predicates (simple predicate = 0.3)
// ---------------------------------------------------------------------------

const DEFAULT_FILTER_SELECTIVITY: f64 = 0.3;

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
            column_statistics: HashMap::new(),
        },
        Operator::LogicalGenerateSeries(gs) => {
            let rows = if gs.step != 0 {
                ((gs.end - gs.start) / gs.step + 1).max(0) as f64
            } else {
                1.0
            };
            Statistics {
                output_row_count: rows,
                column_statistics: HashMap::new(),
            }
        }
        // TODO: derive CTEConsume stats from the corresponding CTEProduce group
        // once we add a cte_id -> group_id mapping to Memo.
        Operator::LogicalCTEConsume(_) => Statistics {
            output_row_count: 1000.0,
            column_statistics: HashMap::new(),
        },
        Operator::LogicalCTEAnchor(_) => child_statistics(memo, &expr.children, 1),

        // -- Unary operators (single child) --
        Operator::LogicalFilter(filter) => {
            let child_stats = child_statistics(memo, &expr.children, 0);
            let selectivity =
                estimate_selectivity(&filter.predicate, &child_stats.column_statistics);
            let output_rows = (child_stats.output_row_count * selectivity).max(1.0);
            Statistics {
                output_row_count: output_rows,
                column_statistics: child_stats.column_statistics,
            }
        }

        Operator::LogicalProject(proj) => {
            let child_stats = child_statistics(memo, &expr.children, 0);
            let projected: HashMap<String, ColumnStatistic> = proj
                .items
                .iter()
                .filter_map(|item| {
                    let name = item.output_name.to_lowercase();
                    child_stats
                        .column_statistics
                        .get(&name)
                        .cloned()
                        .map(|cs| (name, cs))
                })
                .collect();
            Statistics {
                output_row_count: child_stats.output_row_count,
                column_statistics: projected,
            }
        }

        Operator::LogicalAggregate(agg) => {
            let child_stats = child_statistics(memo, &expr.children, 0);
            if agg.group_by.is_empty() {
                return Statistics {
                    output_row_count: 1.0,
                    column_statistics: HashMap::new(),
                };
            }
            let mut ndv_product = 1.0f64;
            for gb_expr in &agg.group_by {
                let ndv = get_expr_ndv(gb_expr, &child_stats.column_statistics);
                ndv_product *= ndv;
            }
            let capped = child_stats.output_row_count * UNKNOWN_GROUP_BY_CORRELATION;
            let output_rows = ndv_product.min(capped).max(1.0);
            Statistics {
                output_row_count: output_rows,
                column_statistics: HashMap::new(),
            }
        }

        Operator::LogicalSort(_) => {
            // Sort preserves row count.
            child_statistics(memo, &expr.children, 0)
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
                column_statistics: child_stats.column_statistics,
            }
        }

        Operator::LogicalWindow(_) => {
            // Window preserves row count.
            child_statistics(memo, &expr.children, 0)
        }

        Operator::LogicalSubqueryAlias(_) => {
            // Passthrough child stats.
            child_statistics(memo, &expr.children, 0)
        }

        Operator::LogicalRepeat(repeat) => {
            let child_stats = child_statistics(memo, &expr.children, 0);
            let repeat_times = repeat.repeat_column_ref_list.len() as f64;
            Statistics {
                output_row_count: child_stats.output_row_count * repeat_times,
                column_statistics: child_stats.column_statistics,
            }
        }

        Operator::LogicalCTEProduce(_) => {
            // Passthrough child stats.
            child_statistics(memo, &expr.children, 0)
        }

        // -- Binary / multi-child operators --
        Operator::LogicalJoin(join) => {
            let left_stats = child_statistics(memo, &expr.children, 0);
            let right_stats = child_statistics(memo, &expr.children, 1);
            derive_join(join, &left_stats, &right_stats)
        }

        Operator::LogicalUnion(union_op) => {
            let mut total_rows = 0.0;
            let mut column_statistics = HashMap::new();
            for (i, _) in expr.children.iter().enumerate() {
                let s = child_statistics(memo, &expr.children, i);
                total_rows += s.output_row_count;
                if column_statistics.is_empty() {
                    column_statistics = s.column_statistics;
                }
            }
            if !union_op.all {
                total_rows *= UNKNOWN_GROUP_BY_CORRELATION;
            }
            Statistics {
                output_row_count: total_rows.max(1.0),
                column_statistics,
            }
        }

        Operator::LogicalIntersect(_) => {
            let mut min_rows = f64::MAX;
            let mut column_statistics = HashMap::new();
            for (i, _) in expr.children.iter().enumerate() {
                let s = child_statistics(memo, &expr.children, i);
                if s.output_row_count < min_rows {
                    min_rows = s.output_row_count;
                    column_statistics = s.column_statistics;
                }
            }
            Statistics {
                output_row_count: (min_rows * 0.5).max(1.0),
                column_statistics,
            }
        }

        Operator::LogicalExcept(_) => {
            if !expr.children.is_empty() {
                let s = child_statistics(memo, &expr.children, 0);
                Statistics {
                    output_row_count: (s.output_row_count * 0.5).max(1.0),
                    column_statistics: s.column_statistics,
                }
            } else {
                Statistics {
                    output_row_count: 1.0,
                    column_statistics: HashMap::new(),
                }
            }
        }

        // -- Physical operators: derive the same way as their logical counterparts --
        Operator::PhysicalScan(scan) => {
            let key = scan
                .alias
                .as_deref()
                .unwrap_or(&scan.table.name)
                .to_lowercase();
            if let Some(ts) = table_stats.get(&key) {
                let row_count = ts.row_count.max(1) as f64;
                let mut selectivity = 1.0;
                for pred in &scan.predicates {
                    selectivity *= estimate_selectivity(pred, &ts.column_stats);
                }
                let output_rows = (row_count * selectivity).max(1.0);
                let column_statistics: HashMap<String, ColumnStatistic> = scan
                    .columns
                    .iter()
                    .map(|c| {
                        let col_name = c.name.to_lowercase();
                        let cs = ts
                            .column_stats
                            .get(&col_name)
                            .cloned()
                            .unwrap_or_else(ColumnStatistic::unknown);
                        (col_name, cs)
                    })
                    .collect();
                Statistics {
                    output_row_count: output_rows,
                    column_statistics,
                }
            } else {
                let column_statistics: HashMap<String, ColumnStatistic> = scan
                    .columns
                    .iter()
                    .map(|c| (c.name.to_lowercase(), ColumnStatistic::unknown()))
                    .collect();
                Statistics {
                    output_row_count: 10_000.0,
                    column_statistics,
                }
            }
        }

        Operator::PhysicalFilter(filter) => {
            let child_stats = child_statistics(memo, &expr.children, 0);
            let selectivity =
                estimate_selectivity(&filter.predicate, &child_stats.column_statistics);
            let output_rows = (child_stats.output_row_count * selectivity).max(1.0);
            Statistics {
                output_row_count: output_rows,
                column_statistics: child_stats.column_statistics,
            }
        }

        Operator::PhysicalProject(proj) => {
            let child_stats = child_statistics(memo, &expr.children, 0);
            let projected: HashMap<String, ColumnStatistic> = proj
                .items
                .iter()
                .filter_map(|item| {
                    let name = item.output_name.to_lowercase();
                    child_stats
                        .column_statistics
                        .get(&name)
                        .cloned()
                        .map(|cs| (name, cs))
                })
                .collect();
            Statistics {
                output_row_count: child_stats.output_row_count,
                column_statistics: projected,
            }
        }

        Operator::PhysicalHashAggregate(agg) => {
            let child_stats = child_statistics(memo, &expr.children, 0);
            if agg.group_by.is_empty() {
                return Statistics {
                    output_row_count: 1.0,
                    column_statistics: HashMap::new(),
                };
            }
            let mut ndv_product = 1.0f64;
            for gb_expr in &agg.group_by {
                let ndv = get_expr_ndv(gb_expr, &child_stats.column_statistics);
                ndv_product *= ndv;
            }
            let capped = child_stats.output_row_count * UNKNOWN_GROUP_BY_CORRELATION;
            let output_rows = ndv_product.min(capped).max(1.0);
            Statistics {
                output_row_count: output_rows,
                column_statistics: HashMap::new(),
            }
        }

        Operator::PhysicalHashJoin(join) => {
            let left_stats = child_statistics(memo, &expr.children, 0);
            let right_stats = child_statistics(memo, &expr.children, 1);
            let left_rows = left_stats.output_row_count.max(1.0);
            let right_rows = right_stats.output_row_count.max(1.0);

            // Compute max NDV from equi-join keys.
            let mut max_ndv = 1.0f64;
            for (l_key, r_key) in &join.eq_conditions {
                let l_ndv = get_expr_ndv(l_key, &left_stats.column_statistics)
                    .max(get_expr_ndv(l_key, &right_stats.column_statistics));
                let r_ndv = get_expr_ndv(r_key, &left_stats.column_statistics)
                    .max(get_expr_ndv(r_key, &right_stats.column_statistics));
                max_ndv = max_ndv.max(l_ndv).max(r_ndv);
            }

            use crate::sql::ir::JoinKind;
            let output_rows = match join.join_type {
                JoinKind::Cross => left_rows * right_rows,
                JoinKind::Inner => {
                    if !join.eq_conditions.is_empty() {
                        (left_rows * right_rows / max_ndv).max(1.0)
                    } else {
                        left_rows * right_rows
                    }
                }
                JoinKind::LeftOuter => {
                    if !join.eq_conditions.is_empty() {
                        let inner = left_rows * right_rows / max_ndv;
                        inner.max(left_rows)
                    } else {
                        left_rows * right_rows
                    }
                }
                JoinKind::RightOuter => {
                    if !join.eq_conditions.is_empty() {
                        let inner = left_rows * right_rows / max_ndv;
                        inner.max(right_rows)
                    } else {
                        left_rows * right_rows
                    }
                }
                JoinKind::FullOuter => {
                    if !join.eq_conditions.is_empty() {
                        let inner = left_rows * right_rows / max_ndv;
                        inner.max(left_rows).max(right_rows)
                    } else {
                        left_rows * right_rows
                    }
                }
                JoinKind::LeftSemi => (left_rows * DEFAULT_FILTER_SELECTIVITY).max(1.0),
                JoinKind::RightSemi => (right_rows * DEFAULT_FILTER_SELECTIVITY).max(1.0),
                JoinKind::LeftAnti => (left_rows * ANTI_JOIN_SELECTIVITY).max(1.0),
                JoinKind::RightAnti => (right_rows * ANTI_JOIN_SELECTIVITY).max(1.0),
            };

            let mut column_statistics = left_stats.column_statistics;
            column_statistics.extend(right_stats.column_statistics);
            Statistics {
                output_row_count: output_rows,
                column_statistics,
            }
        }

        Operator::PhysicalNestLoopJoin(join) => {
            let left_stats = child_statistics(memo, &expr.children, 0);
            let right_stats = child_statistics(memo, &expr.children, 1);
            let left_rows = left_stats.output_row_count.max(1.0);
            let right_rows = right_stats.output_row_count.max(1.0);

            use crate::sql::ir::JoinKind;
            let output_rows = match join.join_type {
                JoinKind::Cross => left_rows * right_rows,
                JoinKind::Inner => {
                    if let Some(ref cond) = join.condition {
                        let sel = estimate_selectivity(cond, &left_stats.column_statistics);
                        (left_rows * right_rows * sel).max(1.0)
                    } else {
                        left_rows * right_rows
                    }
                }
                JoinKind::LeftOuter => {
                    let base = if let Some(ref cond) = join.condition {
                        let sel = estimate_selectivity(cond, &left_stats.column_statistics);
                        left_rows * right_rows * sel
                    } else {
                        left_rows * right_rows
                    };
                    base.max(left_rows)
                }
                JoinKind::RightOuter => {
                    let base = if let Some(ref cond) = join.condition {
                        let sel = estimate_selectivity(cond, &right_stats.column_statistics);
                        left_rows * right_rows * sel
                    } else {
                        left_rows * right_rows
                    };
                    base.max(right_rows)
                }
                JoinKind::FullOuter => {
                    let base = left_rows * right_rows;
                    base.max(left_rows).max(right_rows)
                }
                JoinKind::LeftSemi => (left_rows * DEFAULT_FILTER_SELECTIVITY).max(1.0),
                JoinKind::RightSemi => (right_rows * DEFAULT_FILTER_SELECTIVITY).max(1.0),
                JoinKind::LeftAnti => (left_rows * ANTI_JOIN_SELECTIVITY).max(1.0),
                JoinKind::RightAnti => (right_rows * ANTI_JOIN_SELECTIVITY).max(1.0),
            };

            let mut column_statistics = left_stats.column_statistics;
            column_statistics.extend(right_stats.column_statistics);
            Statistics {
                output_row_count: output_rows,
                column_statistics,
            }
        }

        Operator::PhysicalSort(_) => child_statistics(memo, &expr.children, 0),

        Operator::PhysicalLimit(limit) => {
            let child_stats = child_statistics(memo, &expr.children, 0);
            let output_rows = if let Some(lim) = limit.limit {
                (lim as f64).min(child_stats.output_row_count)
            } else {
                child_stats.output_row_count
            };
            Statistics {
                output_row_count: output_rows.max(0.0),
                column_statistics: child_stats.column_statistics,
            }
        }

        Operator::PhysicalWindow(_) => child_statistics(memo, &expr.children, 0),
        Operator::PhysicalSubqueryAlias(_) => child_statistics(memo, &expr.children, 0),

        Operator::PhysicalDistribution(_) => {
            // Distribution enforcer preserves row count.
            child_statistics(memo, &expr.children, 0)
        }

        Operator::PhysicalCTEProduce(_) => child_statistics(memo, &expr.children, 0),

        // TODO: derive from corresponding CTEProduce group (see logical arm above).
        Operator::PhysicalCTEConsume(_) => Statistics {
            output_row_count: 1000.0,
            column_statistics: HashMap::new(),
        },

        Operator::PhysicalCTEAnchor(_) => child_statistics(memo, &expr.children, 1),

        Operator::PhysicalRepeat(repeat) => {
            let child_stats = child_statistics(memo, &expr.children, 0);
            let repeat_times = repeat.repeat_column_ref_list.len() as f64;
            Statistics {
                output_row_count: child_stats.output_row_count * repeat_times,
                column_statistics: child_stats.column_statistics,
            }
        }

        Operator::PhysicalUnion(union_op) => {
            let mut total_rows = 0.0;
            let mut column_statistics = HashMap::new();
            for (i, _) in expr.children.iter().enumerate() {
                let s = child_statistics(memo, &expr.children, i);
                total_rows += s.output_row_count;
                if column_statistics.is_empty() {
                    column_statistics = s.column_statistics;
                }
            }
            if !union_op.all {
                total_rows *= UNKNOWN_GROUP_BY_CORRELATION;
            }
            Statistics {
                output_row_count: total_rows.max(1.0),
                column_statistics,
            }
        }

        Operator::PhysicalIntersect(_) => {
            let mut min_rows = f64::MAX;
            let mut column_statistics = HashMap::new();
            for (i, _) in expr.children.iter().enumerate() {
                let s = child_statistics(memo, &expr.children, i);
                if s.output_row_count < min_rows {
                    min_rows = s.output_row_count;
                    column_statistics = s.column_statistics;
                }
            }
            Statistics {
                output_row_count: (min_rows * 0.5).max(1.0),
                column_statistics,
            }
        }

        Operator::PhysicalExcept(_) => {
            if !expr.children.is_empty() {
                let s = child_statistics(memo, &expr.children, 0);
                Statistics {
                    output_row_count: (s.output_row_count * 0.5).max(1.0),
                    column_statistics: s.column_statistics,
                }
            } else {
                Statistics {
                    output_row_count: 1.0,
                    column_statistics: HashMap::new(),
                }
            }
        }

        Operator::PhysicalValues(vals) => Statistics {
            output_row_count: vals.rows.len() as f64,
            column_statistics: HashMap::new(),
        },

        Operator::PhysicalGenerateSeries(gs) => {
            let rows = if gs.step != 0 {
                ((gs.end - gs.start) / gs.step + 1).max(0) as f64
            } else {
                1.0
            };
            Statistics {
                output_row_count: rows,
                column_statistics: HashMap::new(),
            }
        }
    }
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
                    column_statistics: HashMap::new(),
                }
            }
        };

        // Derive output columns from the operator.
        let output_columns = derive_output_columns(memo, group_idx);

        memo.groups[group_idx].logical_props = Some(LogicalProperties {
            output_columns,
            row_count: stats.output_row_count,
        });
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
        // Reconstruct Statistics from logical properties.
        // Column statistics are not stored in LogicalProperties, so we
        // return an empty map -- the row_count is the critical value.
        Statistics {
            output_row_count: props.row_count,
            column_statistics: HashMap::new(),
        }
    } else {
        // Child not yet derived; use conservative default.
        Statistics {
            output_row_count: 10_000.0,
            column_statistics: HashMap::new(),
        }
    }
}

/// Derive scan statistics from a `LogicalScanOp`.
fn derive_scan(
    scan: &super::operator::LogicalScanOp,
    table_stats: &HashMap<String, TableStatistics>,
) -> Statistics {
    let key = scan
        .alias
        .as_deref()
        .unwrap_or(&scan.table.name)
        .to_lowercase();

    if let Some(ts) = table_stats.get(&key) {
        let row_count = ts.row_count.max(1) as f64;

        // Apply scan-level predicate selectivity.
        let mut selectivity = 1.0;
        for pred in &scan.predicates {
            selectivity *= estimate_selectivity(pred, &ts.column_stats);
        }

        let output_rows = (row_count * selectivity).max(1.0);

        let column_statistics: HashMap<String, ColumnStatistic> = scan
            .columns
            .iter()
            .map(|c| {
                let col_name = c.name.to_lowercase();
                let cs = ts
                    .column_stats
                    .get(&col_name)
                    .cloned()
                    .unwrap_or_else(ColumnStatistic::unknown);
                (col_name, cs)
            })
            .collect();

        Statistics {
            output_row_count: output_rows,
            column_statistics,
        }
    } else {
        // No table stats available: use defaults.
        let column_statistics: HashMap<String, ColumnStatistic> = scan
            .columns
            .iter()
            .map(|c| (c.name.to_lowercase(), ColumnStatistic::unknown()))
            .collect();
        Statistics {
            output_row_count: 10_000.0,
            column_statistics,
        }
    }
}

/// Derive join statistics from a `LogicalJoinOp` and child stats.
fn derive_join(
    join: &super::operator::LogicalJoinOp,
    left_stats: &Statistics,
    right_stats: &Statistics,
) -> Statistics {
    use crate::sql::ir::JoinKind;

    let left_rows = left_stats.output_row_count.max(1.0);
    let right_rows = right_stats.output_row_count.max(1.0);

    let output_rows = match join.join_type {
        JoinKind::Cross => left_rows * right_rows,
        JoinKind::Inner => {
            if let Some(ref cond) = join.condition {
                let key_ndv = get_join_key_ndv(
                    cond,
                    &left_stats.column_statistics,
                    &right_stats.column_statistics,
                );
                (left_rows * right_rows / key_ndv).max(1.0)
            } else {
                left_rows * right_rows
            }
        }
        JoinKind::LeftOuter => {
            if let Some(ref cond) = join.condition {
                let key_ndv = get_join_key_ndv(
                    cond,
                    &left_stats.column_statistics,
                    &right_stats.column_statistics,
                );
                let inner = left_rows * right_rows / key_ndv;
                inner.max(left_rows)
            } else {
                left_rows * right_rows
            }
        }
        JoinKind::RightOuter => {
            if let Some(ref cond) = join.condition {
                let key_ndv = get_join_key_ndv(
                    cond,
                    &left_stats.column_statistics,
                    &right_stats.column_statistics,
                );
                let inner = left_rows * right_rows / key_ndv;
                inner.max(right_rows)
            } else {
                left_rows * right_rows
            }
        }
        JoinKind::FullOuter => {
            if let Some(ref cond) = join.condition {
                let key_ndv = get_join_key_ndv(
                    cond,
                    &left_stats.column_statistics,
                    &right_stats.column_statistics,
                );
                let inner = left_rows * right_rows / key_ndv;
                inner.max(left_rows).max(right_rows)
            } else {
                left_rows * right_rows
            }
        }
        JoinKind::LeftSemi => {
            if let Some(ref cond) = join.condition {
                let sel = estimate_selectivity(cond, &left_stats.column_statistics);
                (left_rows * sel).max(1.0)
            } else {
                left_rows
            }
        }
        JoinKind::RightSemi => {
            if let Some(ref cond) = join.condition {
                let sel = estimate_selectivity(cond, &right_stats.column_statistics);
                (right_rows * sel).max(1.0)
            } else {
                right_rows
            }
        }
        JoinKind::LeftAnti => (left_rows * ANTI_JOIN_SELECTIVITY).max(1.0),
        JoinKind::RightAnti => (right_rows * ANTI_JOIN_SELECTIVITY).max(1.0),
    };

    let mut column_statistics = left_stats.column_statistics.clone();
    column_statistics.extend(right_stats.column_statistics.clone());

    Statistics {
        output_row_count: output_rows,
        column_statistics,
    }
}

/// Derive output columns for a group from its first expression.
fn derive_output_columns(memo: &Memo, group_idx: usize) -> Vec<crate::sql::ir::OutputColumn> {
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
            .map(|item| crate::sql::ir::OutputColumn {
                name: item.output_name.clone(),
                data_type: item.expr.data_type.clone(),
                nullable: item.expr.nullable,
            })
            .collect(),
        Operator::LogicalAggregate(a) => a.output_columns.clone(),
        Operator::LogicalWindow(w) => w.output_columns.clone(),
        Operator::LogicalValues(v) => v.columns.clone(),
        Operator::LogicalSubqueryAlias(s) => s.output_columns.clone(),
        Operator::LogicalCTEAnchor(_) => child_output_columns(memo, &expr.children, 1),
        Operator::LogicalCTEProduce(c) => c.output_columns.clone(),
        Operator::LogicalCTEConsume(c) => c.output_columns.clone(),
        Operator::LogicalGenerateSeries(g) => {
            vec![crate::sql::ir::OutputColumn {
                name: g.column_name.clone(),
                data_type: arrow::datatypes::DataType::Int64,
                nullable: false,
            }]
        }

        // Passthrough operators: inherit output columns from first child.
        Operator::LogicalFilter(_)
        | Operator::LogicalSort(_)
        | Operator::LogicalLimit(_)
        | Operator::LogicalRepeat(_) => {
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

        // Join: concatenate both sides' output columns.
        Operator::LogicalJoin(_) => {
            let mut cols = vec![];
            for &child_id in &expr.children {
                if let Some(ref props) = memo.groups[child_id].logical_props {
                    cols.extend(props.output_columns.clone());
                }
            }
            cols
        }

        // Union/Intersect/Except: use first child's output columns.
        Operator::LogicalUnion(_) | Operator::LogicalIntersect(_) | Operator::LogicalExcept(_) => {
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

        // Physical operator counterparts.
        Operator::PhysicalScan(s) => s.columns.clone(),
        Operator::PhysicalProject(p) => p
            .items
            .iter()
            .map(|item| crate::sql::ir::OutputColumn {
                name: item.output_name.clone(),
                data_type: item.expr.data_type.clone(),
                nullable: item.expr.nullable,
            })
            .collect(),
        Operator::PhysicalHashAggregate(a) => a.output_columns.clone(),
        Operator::PhysicalWindow(w) => w.output_columns.clone(),
        Operator::PhysicalValues(v) => v.columns.clone(),
        Operator::PhysicalSubqueryAlias(s) => s.output_columns.clone(),
        Operator::PhysicalCTEAnchor(_) => child_output_columns(memo, &expr.children, 1),
        Operator::PhysicalCTEProduce(c) => c.output_columns.clone(),
        Operator::PhysicalCTEConsume(c) => c.output_columns.clone(),
        Operator::PhysicalGenerateSeries(g) => {
            vec![crate::sql::ir::OutputColumn {
                name: g.column_name.clone(),
                data_type: arrow::datatypes::DataType::Int64,
                nullable: false,
            }]
        }
        Operator::PhysicalFilter(_)
        | Operator::PhysicalSort(_)
        | Operator::PhysicalLimit(_)
        | Operator::PhysicalDistribution(_)
        | Operator::PhysicalRepeat(_) => {
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
        Operator::PhysicalHashJoin(_) | Operator::PhysicalNestLoopJoin(_) => {
            let mut cols = vec![];
            for &child_id in &expr.children {
                if let Some(ref props) = memo.groups[child_id].logical_props {
                    cols.extend(props.output_columns.clone());
                }
            }
            cols
        }
        Operator::PhysicalUnion(_)
        | Operator::PhysicalIntersect(_)
        | Operator::PhysicalExcept(_) => {
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
    }
}

// ---------------------------------------------------------------------------
// NDV / join-key helpers (mirrored from cardinality.rs since they are private)
// ---------------------------------------------------------------------------

use crate::sql::ir::{ExprKind, TypedExpr};

/// Get the NDV for an expression from column statistics.
fn get_expr_ndv(expr: &TypedExpr, column_stats: &HashMap<String, ColumnStatistic>) -> f64 {
    if let Some(name) = extract_column_name(expr) {
        if let Some(cs) = column_stats.get(&name.to_lowercase()) {
            return cs.distinct_values_count.max(1.0);
        }
    }
    10.0
}

/// For a join condition, extract the max NDV of join keys from both sides.
fn get_join_key_ndv(
    condition: &TypedExpr,
    left_stats: &HashMap<String, ColumnStatistic>,
    right_stats: &HashMap<String, ColumnStatistic>,
) -> f64 {
    use crate::sql::ir::BinOp;
    match &condition.kind {
        ExprKind::BinaryOp {
            left,
            op: BinOp::Eq | BinOp::EqForNull,
            right,
        } => {
            let left_ndv = get_expr_ndv(left, left_stats).max(get_expr_ndv(left, right_stats));
            let right_ndv = get_expr_ndv(right, left_stats).max(get_expr_ndv(right, right_stats));
            left_ndv.max(right_ndv).max(1.0)
        }
        ExprKind::BinaryOp {
            left,
            op: BinOp::And,
            right,
        } => {
            let l = get_join_key_ndv(left, left_stats, right_stats);
            let r = get_join_key_ndv(right, left_stats, right_stats);
            l.max(r)
        }
        _ => 1.0,
    }
}

/// Extract column name from a simple column reference expression.
fn extract_column_name(expr: &TypedExpr) -> Option<&str> {
    match &expr.kind {
        ExprKind::ColumnRef { column, .. } => Some(column.as_str()),
        ExprKind::Cast { expr, .. } => extract_column_name(expr),
        ExprKind::Nested(inner) => extract_column_name(inner),
        _ => None,
    }
}

fn child_output_columns(
    memo: &Memo,
    children: &[usize],
    child_idx: usize,
) -> Vec<crate::sql::ir::OutputColumn> {
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
    use crate::sql::cascades::convert::logical_plan_to_memo;
    use crate::sql::cascades::memo::Memo;
    use crate::sql::catalog::{ColumnDef, S3FileInfo, TableDef, TableStorage};
    use crate::sql::ir::{JoinKind, OutputColumn};
    use crate::sql::plan::*;
    use arrow::datatypes::DataType;

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

    fn scan_plan(name: &str, cols: &[&str]) -> LogicalPlan {
        let columns: Vec<OutputColumn> = cols
            .iter()
            .map(|c| OutputColumn {
                name: c.to_string(),
                data_type: DataType::Int32,
                nullable: false,
            })
            .collect();
        let col_defs: Vec<ColumnDef> = cols
            .iter()
            .map(|c| ColumnDef {
                name: c.to_string(),
                data_type: DataType::Int32,
                nullable: false,
            })
            .collect();
        LogicalPlan::Scan(ScanNode {
            database: "db".to_string(),
            table: TableDef {
                name: name.to_string(),
                columns: col_defs,
                storage: TableStorage::S3ParquetFiles {
                    files: vec![S3FileInfo {
                        path: format!("s3://bucket/{}.parquet", name),
                        size: 1000,
                        row_count: Some(1000),
                        column_stats: None,
                    }],
                    cloud_properties: Default::default(),
                },
            },
            alias: None,
            columns,
            predicates: vec![],
            required_columns: None,
        })
    }

    fn col_ref(name: &str) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                qualifier: None,
                column: name.to_string(),
            },
            data_type: DataType::Int32,
            nullable: false,
        }
    }

    fn int_lit(v: i64) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::Literal(crate::sql::ir::LiteralValue::Int(v)),
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
                op: crate::sql::ir::BinOp::Eq,
                right: Box::new(right),
            },
        }
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
        });

        let mut memo = Memo::new();
        logical_plan_to_memo(&plan, &mut memo);
        derive_group_statistics(&mut memo, &table_stats);

        // Scan group (0): 10000 rows
        let scan_props = memo.groups[0].logical_props.as_ref().unwrap();
        assert!((scan_props.row_count - 10_000.0).abs() < 1.0);

        // Filter group (1): filter selectivity applied to child row_count
        // But column stats are not propagated through child_statistics,
        // so selectivity falls back to PREDICATE_UNKNOWN_FILTER (0.25).
        let filter_props = memo.groups[1].logical_props.as_ref().unwrap();
        assert!(filter_props.row_count < 10_000.0);
        assert!(filter_props.row_count >= 1.0);
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
                name: "status".to_string(),
                data_type: DataType::Int32,
                nullable: false,
            }],
        });

        let mut memo = Memo::new();
        logical_plan_to_memo(&plan, &mut memo);
        derive_group_statistics(&mut memo, &table_stats);

        // Agg group: child_rows * UNKNOWN_GROUP_BY_CORRELATION vs NDV product.
        // NDV = 10 (default, because child_statistics loses column stats),
        // capped = 100000 * 0.75 = 75000.
        // Result = min(10, 75000) = 10.
        let agg_props = memo.groups[1].logical_props.as_ref().unwrap();
        assert!(agg_props.row_count >= 1.0);
        assert!(agg_props.row_count <= 100_000.0);
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
        });

        let mut memo = Memo::new();
        logical_plan_to_memo(&plan, &mut memo);
        derive_group_statistics(&mut memo, &table_stats);

        let limit_props = memo.groups[1].logical_props.as_ref().unwrap();
        assert!((limit_props.row_count - 10.0).abs() < 0.01);
    }

    #[test]
    fn values_group_stats() {
        let plan = LogicalPlan::Values(ValuesNode {
            rows: vec![vec![], vec![], vec![]],
            columns: vec![OutputColumn {
                name: "x".to_string(),
                data_type: DataType::Int32,
                nullable: false,
            }],
        });

        let mut memo = Memo::new();
        logical_plan_to_memo(&plan, &mut memo);
        derive_group_statistics(&mut memo, &HashMap::new());

        let props = memo.groups[0].logical_props.as_ref().unwrap();
        assert!((props.row_count - 3.0).abs() < 0.01);
        assert_eq!(props.output_columns.len(), 1);
    }
}
