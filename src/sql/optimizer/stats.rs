//! Statistics derivation for Memo groups.
//!
//! Mirrors the logic in `sql::optimizer::cardinality` but operates on
//! Memo operators (`MExpr`) and reads child statistics from group logical
//! properties instead of recursing the `LogicalPlan` tree.

use std::collections::HashMap;

use super::memo::{LogicalProperties, MExpr, Memo};
use super::operator::Operator;
use crate::sql::column_id::ColumnId;
use crate::sql::optimizer::statistics::*;

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
        Operator::LogicalGenerateSeries(gs) => Statistics {
            output_row_count: generate_series_row_count_f64(gs.start, gs.end, gs.step),
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
                        column_statistics: HashMap::new(),
                    }
                } else {
                    // CTEProduce group not yet derived (should not happen in bottom-up order).
                    Statistics {
                        output_row_count: 10_000.0,
                        column_statistics: HashMap::new(),
                    }
                }
            } else {
                // No mapping found; conservative fallback.
                Statistics {
                    output_row_count: 10_000.0,
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

        Operator::LogicalDecode(_) => {
            // Decode preserves row count and column stats.
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
            let alias_key = scan.alias.as_deref().map(|a| a.to_lowercase());
            let table_key = scan.table.name.to_lowercase();
            let ts_opt = alias_key
                .as_deref()
                .and_then(|k| table_stats.get(k))
                .or_else(|| table_stats.get(&table_key));
            if let Some(ts) = ts_opt {
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
                let default_rows = estimate_default_row_count(&scan.table.name);
                let column_statistics: HashMap<String, ColumnStatistic> = scan
                    .columns
                    .iter()
                    .map(|c| (c.name.to_lowercase(), ColumnStatistic::unknown()))
                    .collect();
                let mut selectivity = 1.0;
                for pred in &scan.predicates {
                    selectivity *= estimate_selectivity(pred, &column_statistics);
                }
                Statistics {
                    output_row_count: (default_rows * selectivity).max(1.0),
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
            for eq in &join.eq_conditions {
                let l_ndv = get_expr_ndv(&eq.left, &left_stats.column_statistics)
                    .max(get_expr_ndv(&eq.left, &right_stats.column_statistics));
                let r_ndv = get_expr_ndv(&eq.right, &left_stats.column_statistics)
                    .max(get_expr_ndv(&eq.right, &right_stats.column_statistics));
                max_ndv = max_ndv.max(l_ndv).max(r_ndv);
            }

            use crate::sql::analysis::JoinKind;
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
                JoinKind::LeftAnti | JoinKind::NullAwareLeftAnti => {
                    (left_rows * ANTI_JOIN_SELECTIVITY).max(1.0)
                }
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

            use crate::sql::analysis::JoinKind;
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
                JoinKind::LeftAnti | JoinKind::NullAwareLeftAnti => {
                    (left_rows * ANTI_JOIN_SELECTIVITY).max(1.0)
                }
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

        Operator::PhysicalCTEConsume(cte) => {
            // Look up the CTEProduce group's row count from the memo.
            if let Some(&produce_group_id) = memo.cte_produce_groups.get(&cte.cte_id) {
                if let Some(ref props) = memo.groups[produce_group_id].logical_props {
                    Statistics {
                        output_row_count: props.row_count,
                        column_statistics: HashMap::new(),
                    }
                } else {
                    Statistics {
                        output_row_count: 10_000.0,
                        column_statistics: HashMap::new(),
                    }
                }
            } else {
                Statistics {
                    output_row_count: 10_000.0,
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

        Operator::PhysicalGenerateSeries(gs) => Statistics {
            output_row_count: generate_series_row_count_f64(gs.start, gs.end, gs.step),
            column_statistics: HashMap::new(),
        },
        Operator::PhysicalTableFunction(tf) => {
            derive_table_function_stats(tf.is_left_join, expr, memo)
        }

        Operator::PhysicalDecode(_) => {
            // Decode preserves row count and column stats.
            child_statistics(memo, &expr.children, 0)
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

        memo.groups[group_idx].logical_props = Some(super::logical_props::derive_for_group(
            memo,
            group_idx,
            output_columns,
            stats.output_row_count,
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
            column_statistics: props.column_statistics.clone(),
        }
    } else {
        // Child not yet derived; use conservative default.
        Statistics {
            output_row_count: 10_000.0,
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
        column_statistics: HashMap::new(),
    }
}

/// Derive scan statistics from a `LogicalScanOp`.
fn derive_scan(
    scan: &super::operator::LogicalScanOp,
    table_stats: &HashMap<String, TableStatistics>,
) -> Statistics {
    // Try alias first, then fall back to the canonical table name.
    // `collect_scan_stats` inserts by table name, but the scan node
    // may have an alias that differs from the table name.
    let alias_key = scan.alias.as_deref().map(|a| a.to_lowercase());
    let table_key = scan.table.name.to_lowercase();
    let ts_opt = alias_key
        .as_deref()
        .and_then(|k| table_stats.get(k))
        .or_else(|| table_stats.get(&table_key));

    if let Some(ts) = ts_opt {
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
        // No table stats available: use heuristic defaults based on table name.
        let default_rows = estimate_default_row_count(&scan.table.name);
        let column_statistics: HashMap<String, ColumnStatistic> = scan
            .columns
            .iter()
            .map(|c| (c.name.to_lowercase(), ColumnStatistic::unknown()))
            .collect();
        let mut selectivity = 1.0;
        for pred in &scan.predicates {
            selectivity *= estimate_selectivity(pred, &column_statistics);
        }
        Statistics {
            output_row_count: (default_rows * selectivity).max(1.0),
            column_statistics,
        }
    }
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
    use crate::sql::analysis::JoinKind;

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
        JoinKind::LeftAnti | JoinKind::NullAwareLeftAnti => {
            (left_rows * ANTI_JOIN_SELECTIVITY).max(1.0)
        }
        JoinKind::RightAnti => (right_rows * ANTI_JOIN_SELECTIVITY).max(1.0),
    };

    let mut column_statistics = left_stats.column_statistics.clone();
    column_statistics.extend(right_stats.column_statistics.clone());

    Statistics {
        output_row_count: output_rows,
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
        Operator::LogicalWindow(w) => w.output_columns.clone(),
        Operator::LogicalValues(v) => v.columns.clone(),
        Operator::LogicalSubqueryAlias(s) => s.output_columns.clone(),
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
                // GenerateSeries columns don't originate from the analyzer;
                // use UNSET as there is no factory available in this read-only context.
                column_id: ColumnId::UNSET,
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
            .map(|item| {
                let cid = if let crate::sql::analysis::ExprKind::ColumnRef { column_id, .. } =
                    &item.expr.kind
                {
                    *column_id
                } else {
                    ColumnId::UNSET
                };
                crate::sql::analysis::OutputColumn {
                    column_id: cid,
                    name: item.output_name.clone(),
                    data_type: item.expr.data_type.clone(),
                    nullable: item.expr.nullable,
                    is_internal: false,
                }
            })
            .collect(),
        Operator::PhysicalHashAggregate(a) => a.output_columns.clone(),
        Operator::PhysicalWindow(w) => w.output_columns.clone(),
        Operator::PhysicalValues(v) => v.columns.clone(),
        Operator::PhysicalSubqueryAlias(s) => s.output_columns.clone(),
        // Decode renames dict->string; see the LogicalDecode arm above.
        Operator::PhysicalDecode(d) => d.output_columns.clone(),
        Operator::PhysicalCTEAnchor(_) => child_output_columns(memo, &expr.children, 1),
        Operator::PhysicalCTEProduce(c) => c.output_columns.clone(),
        Operator::PhysicalCTEConsume(c) => c.output_columns.clone(),
        Operator::PhysicalGenerateSeries(g) => {
            vec![crate::sql::analysis::OutputColumn {
                column_id: ColumnId::UNSET,
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

// ---------------------------------------------------------------------------
// NDV / join-key helpers (mirrored from cardinality.rs since they are private)
// ---------------------------------------------------------------------------

use crate::sql::analysis::{BinOp, ExprKind, LiteralValue, TypedExpr, UnOp};

// ---------------------------------------------------------------------------
// Selectivity estimation (moved from sql::optimizer::cardinality)
// ---------------------------------------------------------------------------

/// Estimate selectivity of a predicate expression (0.0..1.0).
pub(crate) fn estimate_selectivity(
    expr: &TypedExpr,
    column_stats: &HashMap<String, ColumnStatistic>,
) -> f64 {
    match &expr.kind {
        ExprKind::BinaryOp { left, op, right } => match op {
            BinOp::And => {
                let l = estimate_selectivity(left, column_stats);
                let r = estimate_selectivity(right, column_stats);
                l * r
            }
            BinOp::Or => {
                let l = estimate_selectivity(left, column_stats);
                let r = estimate_selectivity(right, column_stats);
                l + r - l * r
            }
            BinOp::Eq | BinOp::EqForNull => estimate_eq_selectivity(left, right, column_stats),
            BinOp::Ne => 1.0 - estimate_eq_selectivity(left, right, column_stats),
            BinOp::Lt | BinOp::Le | BinOp::Gt | BinOp::Ge => {
                estimate_range_selectivity(left, right, *op, column_stats)
            }
            _ => PREDICATE_UNKNOWN_FILTER,
        },
        ExprKind::IsNull { negated, expr } => {
            let col_name = extract_column_name(expr);
            let null_frac = col_name
                .and_then(|name| column_stats.get(&name.to_lowercase()))
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
        ExprKind::InList {
            expr,
            list,
            negated,
        } => {
            let col_name = extract_column_name(expr);
            let ndv = col_name
                .and_then(|name| column_stats.get(&name.to_lowercase()))
                .map(|cs| cs.distinct_values_count.max(1.0))
                .unwrap_or(0.0);

            let sel = if ndv > 0.0 {
                (list.len() as f64 / ndv).min(1.0)
            } else {
                IN_PREDICATE_DEFAULT_FILTER
            };
            if *negated { 1.0 - sel } else { sel }
        }
        ExprKind::Between {
            negated,
            expr,
            low,
            high,
        } => {
            // a BETWEEN low AND high  ==  a >= low AND a <= high
            // The ge * le product uses the same independence model as the BinOp::And arm;
            // this slightly overestimates selectivity for narrow symmetric ranges.
            let ge = estimate_range_selectivity(expr, low, BinOp::Ge, column_stats);
            let le = estimate_range_selectivity(expr, high, BinOp::Le, column_stats);
            let sel = ge * le;
            if *negated { 1.0 - sel } else { sel }
        }
        ExprKind::Like { negated, .. } => {
            let sel = PREDICATE_UNKNOWN_FILTER;
            if *negated { 1.0 - sel } else { sel }
        }
        ExprKind::UnaryOp {
            op: UnOp::Not,
            expr,
        } => 1.0 - estimate_selectivity(expr, column_stats),
        ExprKind::IsTruthValue { negated, .. } => {
            // IS TRUE / IS NOT TRUE / IS FALSE / IS NOT FALSE
            let base = 0.5;
            if *negated { 1.0 - base } else { base }
        }
        ExprKind::Nested(inner) => estimate_selectivity(inner, column_stats),
        _ => PREDICATE_UNKNOWN_FILTER,
    }
}

fn estimate_eq_selectivity(
    left: &TypedExpr,
    right: &TypedExpr,
    column_stats: &HashMap<String, ColumnStatistic>,
) -> f64 {
    // col = literal: use 1/ndv
    let col_name = extract_column_name(left).or_else(|| extract_column_name(right));

    if let Some(name) = col_name
        && let Some(cs) = column_stats.get(&name.to_lowercase())
        && cs.distinct_values_count > 1.0
    {
        return 1.0 / cs.distinct_values_count;
    }
    PREDICATE_UNKNOWN_FILTER
}

fn estimate_range_selectivity(
    left: &TypedExpr,
    right: &TypedExpr,
    op: BinOp,
    column_stats: &HashMap<String, ColumnStatistic>,
) -> f64 {
    // Try to use min/max range if available.
    let col_name = extract_column_name(left);
    let literal_val = extract_literal_f64(right);

    if let (Some(name), Some(val)) = (col_name, literal_val)
        && let Some(cs) = column_stats.get(&name.to_lowercase())
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
    0.5 // default for range predicates
}

fn extract_literal_f64(expr: &TypedExpr) -> Option<f64> {
    match &expr.kind {
        ExprKind::Literal(LiteralValue::Int(v)) => Some(*v as f64),
        ExprKind::Literal(LiteralValue::LargeInt(v)) => Some(*v as f64),
        ExprKind::Literal(LiteralValue::Float(v)) => Some(*v),
        ExprKind::Literal(LiteralValue::Decimal(s)) => s.parse::<f64>().ok(),
        ExprKind::Cast { expr, .. } => extract_literal_f64(expr),
        ExprKind::Nested(inner) => extract_literal_f64(inner),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// NDV / join-key helpers (mirrored from cardinality.rs since they are private)
// ---------------------------------------------------------------------------

/// Get the NDV for an expression from column statistics.
fn get_expr_ndv(expr: &TypedExpr, column_stats: &HashMap<String, ColumnStatistic>) -> f64 {
    // A column is only useful for cardinality if it carries a real NDV (> 1).
    // ColumnStatistic::unknown() (propagated for no-stats / managed-lake tables)
    // reports distinct_values_count = 1.0; treating that as a true NDV would make
    // get_join_key_ndv divide left*right by ~1 and explode joins to near
    // cross-products. Mirror the `> 1.0` guard estimate_eq_selectivity uses and
    // fall back to the default NDV for unknown/degenerate columns.
    if let Some(name) = extract_column_name(expr)
        && let Some(cs) = column_stats.get(&name.to_lowercase())
        && cs.distinct_values_count > 1.0
    {
        return cs.distinct_values_count;
    }
    10.0
}

/// For a join condition, extract the max NDV of join keys from both sides.
fn get_join_key_ndv(
    condition: &TypedExpr,
    left_stats: &HashMap<String, ColumnStatistic>,
    right_stats: &HashMap<String, ColumnStatistic>,
) -> f64 {
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
pub(crate) fn extract_column_name(expr: &TypedExpr) -> Option<&str> {
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
                column_id: ColumnId::UNSET,
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
                        delete_files: vec![],
                        manifest_path: None,
                        partition_values: vec![],
                    }],
                    cloud_properties: Default::default(),
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
            "k".to_string(),
            ColumnStatistic {
                min_value: 0.0,
                max_value: 10_000.0,
                nulls_fraction: 0.0,
                average_row_size: 8.0,
                distinct_values_count: 100.0,
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

    fn col_ref(name: &str) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: ColumnId::UNSET,
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
    fn get_expr_ndv_ignores_unknown_ndv() {
        // OQ-3 propagates ColumnStatistic::unknown() (distinct_values_count = 1.0)
        // for no-stats / managed-lake tables. get_expr_ndv must treat that as
        // "no information" and return the 10.0 default, otherwise get_join_key_ndv
        // would divide left*right by ~1 and explode joins to near cross-products.
        let mut column_stats: HashMap<String, ColumnStatistic> = HashMap::new();
        column_stats.insert("unknown_col".to_string(), ColumnStatistic::unknown());
        assert_eq!(column_stats["unknown_col"].distinct_values_count, 1.0);
        let unknown_expr = col_ref("unknown_col");
        assert_eq!(get_expr_ndv(&unknown_expr, &column_stats), 10.0);

        // A degenerate ndv of exactly 1.0 (not via unknown()) is also ignored.
        column_stats.insert(
            "degenerate_col".to_string(),
            ColumnStatistic {
                min_value: 0.0,
                max_value: 100.0,
                nulls_fraction: 0.0,
                average_row_size: 8.0,
                distinct_values_count: 1.0,
            },
        );
        let degenerate_expr = col_ref("degenerate_col");
        assert_eq!(get_expr_ndv(&degenerate_expr, &column_stats), 10.0);

        // A real NDV (> 1) is still used verbatim.
        column_stats.insert(
            "real_col".to_string(),
            ColumnStatistic {
                min_value: 0.0,
                max_value: 100.0,
                nulls_fraction: 0.0,
                average_row_size: 8.0,
                distinct_values_count: 50.0,
            },
        );
        let real_expr = col_ref("real_col");
        assert_eq!(get_expr_ndv(&real_expr, &column_stats), 50.0);

        // An unknown column reference (absent from the map) also defaults.
        let missing_expr = col_ref("missing_col");
        assert_eq!(get_expr_ndv(&missing_expr, &column_stats), 10.0);
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
        cs.insert("a".to_string(), col_stat(0.0, 100.0, 100.0));
        // a BETWEEN 0 AND 50 over [0,100]: ge = clamp((100-0+1)/100) = 0.99,
        // le = (50-0+1)/100 = 0.51, product ≈ 0.5049.
        let pred = between_expr(col_ref("a"), int_lit(0), int_lit(50));
        let sel = estimate_selectivity(&pred, &cs);
        assert!(sel > 0.45 && sel < 0.56, "between selectivity was {sel}");
    }

    #[test]
    fn not_between_is_complement_of_between() {
        let mut cs = HashMap::new();
        cs.insert("a".to_string(), col_stat(0.0, 100.0, 100.0));
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
