//! Cost model for physical operators in the Cascades optimizer.
//!
//! Provides a single `compute_cost` function that estimates the self-cost of
//! a physical operator (not including children).  The formulas are aligned with
//! StarRocks conventions and the existing `optimizer/cost.rs` model.

use super::memo::Cost;
use super::operator::{
    AggMode, JoinDistribution, Operator, PhysicalHashAggregateOp, PhysicalHashJoinOp, ScanOp,
};
use super::property::{DistributionSpec, PhysicalPropertySet};
use super::scalar::{ScalarArena, ScalarId, ScalarNode};
use crate::sql::optimizer::derive::PropertyAlternativeKind;
use crate::sql::optimizer::statistics::{CostEstimate, Statistics};

/// Network transfer multiplier applied to data that crosses node boundaries.
/// Single source of truth: `derive` imports this constant.
pub(crate) const NETWORK_COST: f64 = 1.5;
/// Fixed startup cost for distribution/exchange operators and enforcers.
/// Exchange setup and sender synchronization are visible for tiny joins,
/// especially in debug builds, so a pure byte cost makes small shuffles look
/// unrealistically cheap. Single source of truth: `derive::estimate_enforcer_cost`
/// imports this constant (and waives it for ShuffleAgg pre-aggregation shuffles).
pub(crate) const DISTRIBUTION_STARTUP_COST: f64 = 16.0 * 1024.0 * 1024.0;

/// Penalty multiplier for cross joins (matches StarRocks `CROSS_JOIN_COST_PENALTY`).
const CROSS_JOIN_COST_PENALTY: f64 = 10.0;

/// Penalty multiplier for non-equi hash joins (has `other_condition`).
/// Matches StarRocks optimizer's execute-cost penalty coefficient.
const NON_EQUI_JOIN_COST_PENALTY: f64 = 2.0;

/// Penalty multiplier for nest-loop join execution cost.
/// NLJ is O(N*M) and should be heavily penalized relative to hash join.
const NEST_LOOP_COST_PENALTY: f64 = 100.0;

// Deliberately below f64::MAX so downstream weighting can still clamp safely.
const MAX_FINITE_COST: f64 = 1.0e300;
const DEFAULT_ROW_WIDTH: f64 = 8.0;

pub(crate) struct CostInput<'a> {
    pub op: &'a Operator,
    pub own_stats: &'a Statistics,
    pub child_stats: &'a [&'a Statistics],
    pub child_outputs: &'a [&'a PhysicalPropertySet],
    pub required_output: &'a PhysicalPropertySet,
    pub alt_kind: &'a PropertyAlternativeKind,
    pub scalars: Option<&'a ScalarArena>,
    pub options: &'a CostOptions,
}

/// Estimate the self-cost of a single operator.
///
/// `own_stats`   — output statistics of the operator itself.
/// `child_stats` — output statistics of each child, in order
///                  (probe/left first, build/right second for joins).
///
/// Returns `0.0` for logical operators (they should never be costed).
pub(crate) fn compute_cost(
    op: &Operator,
    own_stats: &Statistics,
    child_stats: &[&Statistics],
) -> Cost {
    match op {
        // ------------------------------------------------------------------
        // Logical operators — not costed
        // ------------------------------------------------------------------
        Operator::LogicalScan(_)
        | Operator::LogicalFilter(_)
        | Operator::LogicalProject(_)
        | Operator::LogicalAggregate(_)
        | Operator::LogicalJoin(_)
        | Operator::LogicalSort(_)
        | Operator::LogicalLimit(_)
        | Operator::LogicalTopN(_)
        | Operator::LogicalWindow(_)
        | Operator::LogicalUnion(_)
        | Operator::LogicalIntersect(_)
        | Operator::LogicalExcept(_)
        | Operator::LogicalValues(_)
        | Operator::LogicalGenerateSeries(_)
        | Operator::LogicalTableFunction(_)
        | Operator::LogicalRepeat(_)
        | Operator::LogicalCTEAnchor(_)
        | Operator::LogicalCTEProduce(_)
        | Operator::LogicalCTEConsume(_)
        | Operator::LogicalDecode(_)
        | Operator::LogicalAggregateStateMerge(_)
        | Operator::LogicalAssertOneRow(_)
        // Apply and IMV markers are eliminated before costing; unreachable here.
        | Operator::LogicalApply(_)
        | Operator::LogicalImvDelta(_)
        | Operator::LogicalImvVersion(_) => 0.0,

        // ------------------------------------------------------------------
        // Physical operators
        // ------------------------------------------------------------------
        Operator::PhysicalScan(scan) => scan_cost_size(scan, own_stats),

        Operator::PhysicalFilter(_) => own_stats.output_row_count * own_stats.avg_row_size() * 0.01,

        Operator::PhysicalProject(_) => own_stats.output_row_count * 0.01,

        Operator::PhysicalHashJoin(j) => {
            let probe_size = child_stats.first().map(|s| s.compute_size()).unwrap_or(0.0);
            let build_size = child_stats.get(1).map(|s| s.compute_size()).unwrap_or(0.0);

            let base_cost = match j.distribution {
                JoinDistribution::Shuffle => (build_size + probe_size) * NETWORK_COST + probe_size,
                JoinDistribution::Broadcast => build_size * NETWORK_COST + probe_size,
                JoinDistribution::Colocate => probe_size,
                JoinDistribution::Unknown => {
                    panic!("unknown join distribution should be resolved before costing")
                }
            };

            // Apply cross join penalty (StarRocks: getCrossJoinCostPenalty = 10).
            let cost_after_cross = if j.join_type == crate::sql::common::JoinKind::Cross {
                base_cost * CROSS_JOIN_COST_PENALTY
            } else {
                base_cost
            };

            // Apply non-equi join penalty: if the join has a residual
            // other_condition, hash probing is less efficient (StarRocks:
            // EXECUTE_COST_PENALTY = 100).
            if j.other_condition.is_some() {
                cost_after_cross * NON_EQUI_JOIN_COST_PENALTY
            } else {
                cost_after_cross
            }
        }

        Operator::PhysicalNestLoopJoin(_) => {
            let left_rows = child_stats
                .first()
                .map(|s| s.output_row_count)
                .unwrap_or(0.0);
            let right_rows = child_stats
                .get(1)
                .map(|s| s.output_row_count)
                .unwrap_or(0.0);
            let avg_row_size = own_stats.avg_row_size();
            left_rows * right_rows * avg_row_size * NEST_LOOP_COST_PENALTY
        }

        Operator::PhysicalHashAggregate(a) => {
            let input_size = child_stats.first().map(|s| s.compute_size()).unwrap_or(0.0);
            match a.mode {
                AggMode::Single => input_size,
                AggMode::Local => input_size * 0.5,
                AggMode::Global => input_size * 0.3,
                // DISTINCT multi-phase agg phases use the same reduction factor
                // as Global. This is a rough approximation — DistinctGlobal
                // typically processes more rows than Global (it groups by g+x,
                // not just g), so this may underestimate its cost.
                AggMode::DistinctGlobal | AggMode::DistinctLocal => input_size * 0.3,
            }
        }

        Operator::PhysicalSort(_) => {
            let n = own_stats.output_row_count.max(1.0);
            n * n.log2()
        }

        Operator::PhysicalTopN(t) => {
            // Physical model: TopN scans all input rows (size = child's output row count)
            // and maintains a heap of size k = min(input_rows, limit + offset).
            // Total cost: input_rows * log2(k).
            let input_rows = child_stats
                .first()
                .map(|s| s.output_row_count)
                .unwrap_or(own_stats.output_row_count)
                .max(1.0);
            let k = match (t.limit, t.offset) {
                (Some(l), Some(o)) => ((l as f64) + (o as f64)).min(input_rows).max(1.0),
                (Some(l), None) => (l as f64).min(input_rows).max(1.0),
                _ => input_rows,
            };
            // Guard against log2(1)=0 when limit=1: lower-bound the per-row work at 1.0.
            input_rows * k.log2().max(1.0)
        }

        Operator::PhysicalDistribution(_) => {
            DISTRIBUTION_STARTUP_COST + own_stats.compute_size() * NETWORK_COST
        }

        Operator::PhysicalLimit(_) => 0.01,

        Operator::PhysicalAssertOneRow(_) => 0.01,

        Operator::PhysicalCTEAnchor(_) => 0.0,

        // Window, Repeat, Union, Intersect, Except, Values, GenerateSeries,
        // CTE, Decode — lightweight default.
        Operator::PhysicalWindow(_)
        | Operator::PhysicalRepeat(_)
        | Operator::PhysicalUnion(_)
        | Operator::PhysicalIntersect(_)
        | Operator::PhysicalExcept(_)
        | Operator::PhysicalValues(_)
        | Operator::PhysicalGenerateSeries(_)
        | Operator::PhysicalTableFunction(_)
        | Operator::PhysicalCTEProduce(_)
        | Operator::PhysicalCTEConsume(_)
        | Operator::PhysicalDecode(_)
        | Operator::PhysicalAggregateStateMerge(_) => own_stats.output_row_count * 0.01,
    }
}

#[derive(Clone, Debug)]
pub(crate) struct CostOptions {
    pub cpu_weight: f64,
    pub memory_weight: f64,
    pub network_weight: f64,
    pub backend_factor: f64,
    pub broadcast_row_limit: f64,
    pub broadcast_byte_limit: f64,
    pub broadcast_right_table_scale_factor: f64,
    pub fallback_broadcast_row_limit: f64,
    pub network_cost: f64,
    pub memory_cost_weight: f64,
    pub predicate_cost_factor: f64,
    pub projection_cost_factor: f64,
    pub hash_cost_factor: f64,
    pub sort_cost_factor: f64,
    pub topn_cost_factor: f64,
    pub aggregate_cost_factor: f64,
    pub exchange_startup_cost: f64,
    pub fallback_cpu_factor: f64,
}

impl Default for CostOptions {
    fn default() -> Self {
        Self {
            cpu_weight: 0.5,
            memory_weight: 2.0,
            network_weight: 1.5,
            backend_factor: 3.0,
            broadcast_row_limit: 15_000_000.0,
            broadcast_byte_limit: 512.0 * 1024.0 * 1024.0,
            broadcast_right_table_scale_factor: 10.0,
            fallback_broadcast_row_limit: 500_000.0,
            network_cost: NETWORK_COST,
            memory_cost_weight: 0.25,
            predicate_cost_factor: 0.02,
            projection_cost_factor: 0.01,
            hash_cost_factor: 1.0,
            sort_cost_factor: 1.0,
            topn_cost_factor: 1.0,
            aggregate_cost_factor: 1.0,
            exchange_startup_cost: DISTRIBUTION_STARTUP_COST,
            fallback_cpu_factor: 0.01,
        }
    }
}

fn effective_cost_weight(weight: f64) -> f64 {
    if weight.is_finite() {
        weight.max(0.0)
    } else {
        0.0
    }
}

fn finite_non_negative_cost(value: f64) -> f64 {
    if value.is_finite() {
        if value > 0.0 {
            value.min(MAX_FINITE_COST)
        } else {
            0.0
        }
    } else if value.is_infinite() && value.is_sign_positive() {
        MAX_FINITE_COST
    } else {
        0.0
    }
}

fn cost_row_count(stats: &Statistics) -> f64 {
    let rows = stats.output_row_count;
    if rows.is_finite() {
        if rows > 0.0 {
            finite_non_negative_cost(rows)
        } else {
            1.0
        }
    } else if rows.is_infinite() && rows.is_sign_positive() {
        MAX_FINITE_COST
    } else {
        1.0
    }
}

fn cost_row_width(stats: &Statistics) -> f64 {
    if stats.column_statistics.is_empty() {
        return DEFAULT_ROW_WIDTH;
    }

    let mut total = 0.0;
    for column in stats.column_statistics.values() {
        let width = column.average_row_size;
        let contribution = if width.is_finite() {
            if width > 0.0 {
                finite_non_negative_cost(width)
            } else {
                DEFAULT_ROW_WIDTH
            }
        } else if width.is_infinite() && width.is_sign_positive() {
            return MAX_FINITE_COST;
        } else {
            DEFAULT_ROW_WIDTH
        };

        total = finite_non_negative_cost(total + contribution);
        if total >= MAX_FINITE_COST {
            return MAX_FINITE_COST;
        }
    }
    total
}

fn safe_compute_size(stats: &Statistics) -> f64 {
    finite_non_negative_cost(cost_row_count(stats) * cost_row_width(stats))
}

fn scan_cost_size(scan: &ScanOp, stats: &Statistics) -> f64 {
    let Some(required_columns) = scan
        .required_columns
        .as_ref()
        .filter(|cols| !cols.is_empty())
    else {
        return safe_compute_size(stats);
    };

    let mut column_ids = Vec::new();
    for required_name in required_columns {
        if let Some(column) = scan
            .columns
            .iter()
            .find(|column| column.name.eq_ignore_ascii_case(required_name))
        {
            if !column_ids.contains(&column.column_id) {
                column_ids.push(column.column_id);
            }
        }
    }

    if column_ids.is_empty() {
        safe_compute_size(stats)
    } else {
        finite_non_negative_cost(stats.compute_size_for_columns(&column_ids))
    }
}

fn stats_has_positive_overflow_signal(stats: &Statistics) -> bool {
    safe_compute_size(stats) >= MAX_FINITE_COST
}

fn sanitize_legacy_fallback_cost(
    legacy_cost: Cost,
    own_stats: &Statistics,
    child_stats: &[&Statistics],
) -> Cost {
    if legacy_cost.is_nan()
        && (stats_has_positive_overflow_signal(own_stats)
            || child_stats
                .iter()
                .any(|stats| stats_has_positive_overflow_signal(stats)))
    {
        MAX_FINITE_COST
    } else {
        finite_non_negative_cost(legacy_cost)
    }
}

impl CostEstimate {
    pub(crate) fn total_with_options(&self, options: &CostOptions) -> Cost {
        self.weighted_total(
            effective_cost_weight(options.cpu_weight),
            effective_cost_weight(options.memory_weight),
            effective_cost_weight(options.network_weight),
        )
    }
}

fn scalar_complexity(arena: Option<&ScalarArena>, expr: ScalarId) -> f64 {
    let Some(arena) = arena else {
        return 1.0;
    };
    match arena.node(expr) {
        ScalarNode::ColumnRef(_) | ScalarNode::LambdaParamRef { .. } | ScalarNode::Literal(_) => {
            0.1
        }
        ScalarNode::Nested(child) | ScalarNode::Cast { child, .. } => {
            0.2 + scalar_complexity(Some(arena), *child)
        }
        ScalarNode::UnaryOp { child, .. }
        | ScalarNode::IsNull { child, .. }
        | ScalarNode::IsTruthValue { child, .. } => 0.5 + scalar_complexity(Some(arena), *child),
        ScalarNode::BinaryOp { left, right, .. } => {
            1.0 + scalar_complexity(Some(arena), *left) + scalar_complexity(Some(arena), *right)
        }
        ScalarNode::FunctionCall { args, .. } => {
            3.0 + args
                .iter()
                .map(|arg| scalar_complexity(Some(arena), *arg))
                .sum::<f64>()
        }
        ScalarNode::LambdaFunction { body, .. } | ScalarNode::Lambda { body, .. } => {
            2.0 + scalar_complexity(Some(arena), *body)
        }
        ScalarNode::AggregateCall { args, order_by, .. } => {
            2.0 + args
                .iter()
                .map(|arg| scalar_complexity(Some(arena), *arg))
                .sum::<f64>()
                + order_by.len() as f64
        }
        ScalarNode::InList { child, list, .. } => {
            1.0 + scalar_complexity(Some(arena), *child) + list.len() as f64 * 0.2
        }
        ScalarNode::Between {
            child, low, high, ..
        } => {
            1.0 + scalar_complexity(Some(arena), *child)
                + scalar_complexity(Some(arena), *low)
                + scalar_complexity(Some(arena), *high)
        }
        ScalarNode::Like { child, pattern, .. } => {
            3.0 + scalar_complexity(Some(arena), *child) + scalar_complexity(Some(arena), *pattern)
        }
        ScalarNode::Case {
            operand,
            when_then,
            else_expr,
        } => {
            operand
                .map(|expr| scalar_complexity(Some(arena), expr))
                .unwrap_or(0.0)
                + when_then
                    .iter()
                    .map(|(when, then)| {
                        scalar_complexity(Some(arena), *when)
                            + scalar_complexity(Some(arena), *then)
                    })
                    .sum::<f64>()
                + else_expr
                    .map(|expr| scalar_complexity(Some(arena), expr))
                    .unwrap_or(0.0)
                + 1.0
        }
        ScalarNode::WindowCall {
            args,
            partition_by,
            order_by,
            ..
        } => {
            4.0 + args
                .iter()
                .chain(partition_by.iter())
                .map(|arg| scalar_complexity(Some(arena), *arg))
                .sum::<f64>()
                + order_by.len() as f64
        }
    }
}

fn scalar_list_complexity(arena: Option<&ScalarArena>, exprs: &[ScalarId]) -> f64 {
    exprs
        .iter()
        .map(|expr| scalar_complexity(arena, *expr))
        .sum::<f64>()
        .max(1.0)
}

fn child_output_is_hash_partitioned(output: Option<&&PhysicalPropertySet>) -> bool {
    matches!(
        output.map(|properties| &properties.distribution),
        Some(DistributionSpec::HashPartitioned { .. })
    )
}

fn estimate_hash_join_cost(input: &CostInput<'_>, join: &PhysicalHashJoinOp) -> CostEstimate {
    let probe_stats = input.child_stats.first().copied();
    let build_stats = input.child_stats.get(1).copied();
    let probe_rows = probe_stats.map(cost_row_count).unwrap_or(1.0);
    let build_rows = build_stats.map(cost_row_count).unwrap_or(1.0);
    let probe_size = probe_stats.map(safe_compute_size).unwrap_or(0.0);
    let build_size = build_stats.map(safe_compute_size).unwrap_or(0.0);
    let output_size = safe_compute_size(input.own_stats);
    let key_factor = (join.eq_conditions.len() as f64).max(1.0);

    let is_broadcast = match input.alt_kind {
        PropertyAlternativeKind::BroadcastJoin => true,
        PropertyAlternativeKind::ShuffleJoin => false,
        PropertyAlternativeKind::Default => match join.distribution {
            JoinDistribution::Broadcast => true,
            JoinDistribution::Shuffle | JoinDistribution::Colocate => false,
            JoinDistribution::Unknown => {
                panic!("unknown join distribution should be resolved before costing")
            }
        },
    };
    let is_shuffle = match input.alt_kind {
        PropertyAlternativeKind::ShuffleJoin => true,
        PropertyAlternativeKind::BroadcastJoin => false,
        PropertyAlternativeKind::Default => match join.distribution {
            JoinDistribution::Shuffle => true,
            JoinDistribution::Broadcast | JoinDistribution::Colocate => false,
            JoinDistribution::Unknown => {
                panic!("unknown join distribution should be resolved before costing")
            }
        },
    };

    let mut cpu_cost = finite_non_negative_cost(
        (probe_rows + build_rows) * key_factor * input.options.hash_cost_factor + output_size,
    );
    let mut memory_cost = if is_broadcast {
        finite_non_negative_cost(build_size * input.options.backend_factor)
    } else if is_shuffle {
        finite_non_negative_cost(build_size / input.options.backend_factor.max(1.0))
    } else {
        build_size
    };
    let network_cost = if is_broadcast {
        finite_non_negative_cost(build_size * input.options.backend_factor)
    } else if is_shuffle {
        if child_output_is_hash_partitioned(input.child_outputs.first())
            && child_output_is_hash_partitioned(input.child_outputs.get(1))
        {
            0.0
        } else {
            finite_non_negative_cost(probe_size + build_size)
        }
    } else {
        0.0
    };

    if join.join_type == crate::sql::analysis::JoinKind::Cross {
        cpu_cost = finite_non_negative_cost(cpu_cost * CROSS_JOIN_COST_PENALTY);
        memory_cost = finite_non_negative_cost(memory_cost * CROSS_JOIN_COST_PENALTY);
    }
    if join.other_condition.is_some() {
        cpu_cost = finite_non_negative_cost(cpu_cost * NON_EQUI_JOIN_COST_PENALTY);
    }

    CostEstimate {
        cpu_cost,
        memory_cost,
        network_cost,
    }
}

fn estimate_nested_loop_join_cost(input: &CostInput<'_>) -> CostEstimate {
    let left_rows = input
        .child_stats
        .first()
        .map(|stats| cost_row_count(stats))
        .unwrap_or_else(|| cost_row_count(input.own_stats));
    let right_rows = input
        .child_stats
        .get(1)
        .map(|stats| cost_row_count(stats))
        .unwrap_or(1.0);
    let build_size = input
        .child_stats
        .get(1)
        .map(|stats| safe_compute_size(stats))
        .unwrap_or(0.0);
    CostEstimate {
        cpu_cost: finite_non_negative_cost(
            left_rows * right_rows * cost_row_width(input.own_stats) * NEST_LOOP_COST_PENALTY,
        ),
        memory_cost: finite_non_negative_cost(build_size),
        network_cost: 0.0,
    }
}

fn estimate_aggregate_cost(input: &CostInput<'_>, agg: &PhysicalHashAggregateOp) -> CostEstimate {
    let input_size = input
        .child_stats
        .first()
        .map(|stats| safe_compute_size(stats))
        .unwrap_or_else(|| safe_compute_size(input.own_stats));
    let phase_factor = match agg.mode {
        AggMode::Single => 1.0,
        AggMode::Local => 0.5,
        AggMode::Global | AggMode::DistinctGlobal | AggMode::DistinctLocal => 0.3,
    };
    CostEstimate {
        cpu_cost: finite_non_negative_cost(
            input_size * phase_factor * input.options.aggregate_cost_factor,
        ),
        memory_cost: safe_compute_size(input.own_stats),
        network_cost: 0.0,
    }
}

pub(crate) fn estimate_distribution_cost_estimate(
    stats: &Statistics,
    options: &CostOptions,
) -> CostEstimate {
    let size = safe_compute_size(stats);
    CostEstimate {
        cpu_cost: finite_non_negative_cost(options.exchange_startup_cost),
        memory_cost: finite_non_negative_cost(size * 0.05),
        network_cost: size,
    }
}

pub(crate) fn estimate_sort_cost_estimate(
    stats: &Statistics,
    options: &CostOptions,
) -> CostEstimate {
    let rows = cost_row_count(stats);
    CostEstimate {
        cpu_cost: finite_non_negative_cost(rows * rows.log2().max(1.0) * options.sort_cost_factor),
        memory_cost: safe_compute_size(stats),
        network_cost: 0.0,
    }
}

pub(crate) fn compute_cost_estimate(input: &CostInput<'_>) -> CostEstimate {
    match input.op {
        Operator::PhysicalScan(scan) => CostEstimate {
            cpu_cost: scan_cost_size(scan, input.own_stats),
            memory_cost: 0.0,
            network_cost: 0.0,
        },
        Operator::PhysicalFilter(filter) => {
            let input_rows = input
                .child_stats
                .first()
                .map(|stats| cost_row_count(stats))
                .unwrap_or_else(|| cost_row_count(input.own_stats));
            let complexity = scalar_complexity(input.scalars, filter.predicate);
            CostEstimate {
                cpu_cost: finite_non_negative_cost(
                    input_rows * complexity * input.options.predicate_cost_factor,
                ),
                memory_cost: safe_compute_size(input.own_stats) * 0.05,
                network_cost: 0.0,
            }
        }
        Operator::PhysicalProject(project) => {
            let input_rows = input
                .child_stats
                .first()
                .map(|stats| cost_row_count(stats))
                .unwrap_or_else(|| cost_row_count(input.own_stats));
            let exprs: Vec<_> = project.items.iter().map(|item| item.expr).collect();
            CostEstimate {
                cpu_cost: finite_non_negative_cost(
                    input_rows
                        * scalar_list_complexity(input.scalars, &exprs)
                        * input.options.projection_cost_factor,
                ),
                memory_cost: safe_compute_size(input.own_stats) * 0.02,
                network_cost: 0.0,
            }
        }
        Operator::PhysicalSort(_) => {
            let stats = input
                .child_stats
                .first()
                .copied()
                .unwrap_or(input.own_stats);
            let mut estimate = estimate_sort_cost_estimate(stats, input.options);
            estimate.memory_cost = safe_compute_size(input.own_stats);
            estimate
        }
        Operator::PhysicalTopN(topn) => {
            let input_rows = input
                .child_stats
                .first()
                .map(|stats| cost_row_count(stats))
                .unwrap_or_else(|| cost_row_count(input.own_stats));
            let k = match (topn.limit, topn.offset) {
                (Some(limit), Some(offset)) => {
                    ((limit as f64) + (offset as f64)).min(input_rows).max(1.0)
                }
                (Some(limit), None) => (limit as f64).min(input_rows).max(1.0),
                _ => input_rows,
            };
            CostEstimate {
                cpu_cost: finite_non_negative_cost(
                    input_rows * k.log2().max(1.0) * input.options.topn_cost_factor,
                ),
                memory_cost: safe_compute_size(input.own_stats),
                network_cost: 0.0,
            }
        }
        Operator::PhysicalLimit(_) | Operator::PhysicalAssertOneRow(_) => CostEstimate {
            cpu_cost: finite_non_negative_cost(cost_row_count(input.own_stats) * 0.001),
            memory_cost: 0.0,
            network_cost: 0.0,
        },
        Operator::PhysicalHashJoin(join) => estimate_hash_join_cost(input, join),
        Operator::PhysicalNestLoopJoin(_) => estimate_nested_loop_join_cost(input),
        Operator::PhysicalHashAggregate(agg) => estimate_aggregate_cost(input, agg),
        Operator::PhysicalDistribution(_) => {
            estimate_distribution_cost_estimate(input.own_stats, input.options)
        }
        _ => {
            // Keep the generic fallback independent from the public cost entrypoint.
            // Task 4 can then rebuild that entrypoint on CostInput without recursion.
            let legacy_cost = compute_legacy_cost_with_properties(
                input.op,
                input.own_stats,
                input.child_stats,
                input.child_outputs,
                input.alt_kind,
                input.options,
            );
            let cpu_weight = effective_cost_weight(input.options.cpu_weight);
            let cpu_cost = finite_non_negative_cost(
                sanitize_legacy_fallback_cost(legacy_cost, input.own_stats, input.child_stats)
                    / cpu_weight,
            );
            CostEstimate {
                // Generic fallback stores the legacy scalar total as CPU-equivalent
                // cost until the operator gets a real dimensional kernel.
                cpu_cost,
                memory_cost: 0.0,
                network_cost: 0.0,
            }
        }
    }
}

pub(crate) fn compute_cost_from_input(input: &CostInput<'_>) -> Cost {
    compute_cost_estimate(input).total_with_options(input.options)
}

pub(crate) fn broadcast_gate_passes(
    probe_stats: &Statistics,
    build_stats: &Statistics,
    options: &CostOptions,
) -> bool {
    let build_rows = build_stats.output_row_count;
    let build_bytes = build_stats.compute_size();
    let probe_bytes = probe_stats.compute_size();

    if build_bytes > options.broadcast_byte_limit {
        return false;
    }

    if build_stats.row_count_confidence != crate::sql::optimizer::statistics::Confidence::Exact
        && build_rows > options.fallback_broadcast_row_limit
    {
        return false;
    }

    let build_is_obviously_tiny = probe_bytes
        >= build_bytes * options.backend_factor * options.broadcast_right_table_scale_factor;
    if build_rows > options.broadcast_row_limit && !build_is_obviously_tiny {
        return false;
    }

    true
}

fn compute_legacy_cost_with_properties(
    op: &Operator,
    own_stats: &Statistics,
    child_stats: &[&Statistics],
    _child_outputs: &[&PhysicalPropertySet],
    alt_kind: &PropertyAlternativeKind,
    options: &CostOptions,
) -> Cost {
    match op {
        Operator::PhysicalHashJoin(j) => {
            let probe_stats = child_stats.first().copied();
            let build_stats = child_stats.get(1).copied();
            let probe_size = probe_stats.map(|s| s.compute_size()).unwrap_or(0.0);
            let build_size = build_stats.map(|s| s.compute_size()).unwrap_or(0.0);

            let base_cost = match alt_kind {
                PropertyAlternativeKind::BroadcastJoin => {
                    // The distribution enforcer cost models making the build
                    // child available to the join. The join self-cost still
                    // charges backend fanout and memory pressure during hash
                    // table materialization/probing.
                    probe_size
                        + build_size * options.network_cost * options.backend_factor
                        + build_size * options.memory_cost_weight * options.backend_factor
                }
                PropertyAlternativeKind::ShuffleJoin => {
                    probe_size + build_size / options.backend_factor.max(1.0)
                }
                PropertyAlternativeKind::Default => compute_cost(op, own_stats, child_stats),
            };

            let cost_after_cross = if j.join_type == crate::sql::common::JoinKind::Cross {
                base_cost * CROSS_JOIN_COST_PENALTY
            } else {
                base_cost
            };
            if j.other_condition.is_some() {
                cost_after_cross * NON_EQUI_JOIN_COST_PENALTY
            } else {
                cost_after_cross
            }
        }
        _ => compute_cost(op, own_stats, child_stats),
    }
}

pub(crate) fn compute_cost_with_properties(
    op: &Operator,
    own_stats: &Statistics,
    child_stats: &[&Statistics],
    child_outputs: &[&PhysicalPropertySet],
    alt_kind: &PropertyAlternativeKind,
    options: &CostOptions,
) -> Cost {
    let required_output = PhysicalPropertySet::any();
    let input = CostInput {
        op,
        own_stats,
        child_stats,
        child_outputs,
        required_output: &required_output,
        alt_kind,
        scalars: None,
        options,
    };
    compute_cost_from_input(&input)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::analysis::JoinKind;
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::operator::*;
    use crate::sql::optimizer::property::{DistributionSpec, OrderingSpec};
    use crate::sql::optimizer::scalar::{ScalarArena, intern_typed};
    use crate::sql::optimizer::statistics::{ColumnStatistic, CostEstimate};
    use crate::sql::planner::plan::*;
    use std::collections::HashMap;

    fn stats(rows: f64, avg_size: f64) -> Statistics {
        let mut col = HashMap::new();
        col.insert(
            ColumnId::new_for_test(1),
            ColumnStatistic {
                min_value: 0.0,
                max_value: 100.0,
                nulls_fraction: 0.0,
                average_row_size: avg_size,
                distinct_values_count: rows,
                ..Default::default()
            },
        );
        Statistics {
            output_row_count: rows,
            column_statistics: col,
            ..Default::default()
        }
    }

    fn stats_with_column_widths(rows: f64, widths: &[f64]) -> Statistics {
        let mut col = HashMap::new();
        for (idx, width) in widths.iter().enumerate() {
            col.insert(
                ColumnId::new_for_test(idx as u32 + 1),
                ColumnStatistic {
                    min_value: 0.0,
                    max_value: 100.0,
                    nulls_fraction: 0.0,
                    average_row_size: *width,
                    distinct_values_count: rows,
                    ..Default::default()
                },
            );
        }
        Statistics {
            output_row_count: rows,
            column_statistics: col,
            ..Default::default()
        }
    }

    fn output_column(id: u32, name: &str) -> crate::sql::analysis::OutputColumn {
        crate::sql::analysis::OutputColumn {
            column_id: ColumnId::new_for_test(id),
            name: name.to_string(),
            data_type: arrow::datatypes::DataType::Int64,
            nullable: false,
            is_internal: false,
        }
    }

    fn two_column_scan_op(required_columns: Option<Vec<&str>>) -> Operator {
        Operator::PhysicalScan(ScanOp {
            database: String::new(),
            table: crate::sql::catalog::TableDef {
                name: "t".into(),
                columns: vec![],
                iceberg_row_lineage_metadata_columns: vec![],
                source: crate::sql::catalog::ScanSource::StarRocks {
                    db_id: 0,
                    table_id: 0,
                },
            },
            alias: None,
            columns: vec![output_column(1, "narrow"), output_column(2, "wide")],
            predicates: vec![],
            required_columns: required_columns
                .map(|columns| columns.into_iter().map(str::to_string).collect()),
            dict_columns: vec![],
            variant_columns: vec![],
            mv_rewritten_from: None,
        })
    }

    fn scan_op() -> Operator {
        Operator::PhysicalScan(ScanOp {
            database: String::new(),
            table: crate::sql::catalog::TableDef {
                name: "t".into(),
                columns: vec![],
                iceberg_row_lineage_metadata_columns: vec![],
                source: crate::sql::catalog::ScanSource::StarRocks {
                    db_id: 0,
                    table_id: 0,
                },
            },
            alias: None,
            columns: vec![],
            predicates: vec![],
            required_columns: None,
            dict_columns: vec![],
            variant_columns: vec![],
            mv_rewritten_from: None,
        })
    }

    fn assert_finite_non_negative_dimensions(estimate: &CostEstimate) {
        assert!(estimate.cpu_cost.is_finite() && estimate.cpu_cost >= 0.0);
        assert!(estimate.memory_cost.is_finite() && estimate.memory_cost >= 0.0);
        assert!(estimate.network_cost.is_finite() && estimate.network_cost >= 0.0);
    }

    fn test_eq_condition(
        arena: &mut ScalarArena,
        left_value: i64,
        right_value: i64,
    ) -> PhysicalHashJoinEqCondition {
        let left = intern_typed(
            arena,
            &crate::sql::analysis::TypedExpr {
                kind: crate::sql::analysis::ExprKind::Literal(
                    crate::sql::analysis::LiteralValue::Int(left_value),
                ),
                data_type: arrow::datatypes::DataType::Int64,
                nullable: false,
            },
        );
        let right = intern_typed(
            arena,
            &crate::sql::analysis::TypedExpr {
                kind: crate::sql::analysis::ExprKind::Literal(
                    crate::sql::analysis::LiteralValue::Int(right_value),
                ),
                data_type: arrow::datatypes::DataType::Int64,
                nullable: false,
            },
        );
        PhysicalHashJoinEqCondition {
            left,
            right,
            null_safe: false,
        }
    }

    #[test]
    fn finite_non_negative_cost_saturates_only_positive_overflow() {
        assert_eq!(finite_non_negative_cost(42.0), 42.0);
        assert_eq!(
            finite_non_negative_cost(MAX_FINITE_COST * 10.0),
            MAX_FINITE_COST
        );
        assert_eq!(finite_non_negative_cost(f64::INFINITY), MAX_FINITE_COST);
        assert_eq!(finite_non_negative_cost(0.0), 0.0);
        assert_eq!(finite_non_negative_cost(-1.0), 0.0);
        assert_eq!(finite_non_negative_cost(f64::NEG_INFINITY), 0.0);
        assert_eq!(finite_non_negative_cost(f64::NAN), 0.0);
    }

    #[test]
    fn cost_row_count_saturates_positive_infinity_and_preserves_invalid_fallback() {
        assert_eq!(cost_row_count(&stats(42.0, 8.0)), 42.0);
        assert_eq!(cost_row_count(&stats(f64::MAX, 8.0)), MAX_FINITE_COST);
        assert_eq!(cost_row_count(&stats(f64::INFINITY, 8.0)), MAX_FINITE_COST);
        assert_eq!(cost_row_count(&stats(0.0, 8.0)), 1.0);
        assert_eq!(cost_row_count(&stats(-1.0, 8.0)), 1.0);
        assert_eq!(cost_row_count(&stats(f64::NEG_INFINITY, 8.0)), 1.0);
        assert_eq!(cost_row_count(&stats(f64::NAN, 8.0)), 1.0);
    }

    #[test]
    fn cost_row_width_saturates_positive_infinity_and_preserves_invalid_fallback() {
        assert_eq!(cost_row_width(&stats(1.0, 42.0)), 42.0);
        assert_eq!(cost_row_width(&stats(1.0, f64::MAX)), MAX_FINITE_COST);
        assert_eq!(
            cost_row_width(&stats_with_column_widths(1.0, &[f64::MAX, f64::MAX])),
            MAX_FINITE_COST
        );
        assert_eq!(
            cost_row_width(&stats_with_column_widths(
                1.0,
                &[f64::INFINITY, f64::NEG_INFINITY],
            )),
            MAX_FINITE_COST
        );
        assert_eq!(
            cost_row_width(&stats_with_column_widths(1.0, &[f64::MAX, f64::NAN])),
            MAX_FINITE_COST
        );
        assert_eq!(cost_row_width(&stats(1.0, 0.0)), 8.0);
        assert_eq!(cost_row_width(&stats(1.0, -1.0)), 8.0);
        assert_eq!(cost_row_width(&stats(1.0, f64::NEG_INFINITY)), 8.0);
        assert_eq!(cost_row_width(&stats(1.0, f64::NAN)), 8.0);
    }

    #[test]
    fn compute_cost_estimate_returns_dimensions_for_scan() {
        let s = stats(1000.0, 100.0);
        let op = scan_op();
        let child_stats: [&Statistics; 0] = [];
        let child_outputs: [&PhysicalPropertySet; 0] = [];
        let required = PhysicalPropertySet::any();
        let options = CostOptions::default();
        let input = CostInput {
            op: &op,
            own_stats: &s,
            child_stats: &child_stats,
            child_outputs: &child_outputs,
            required_output: &required,
            alt_kind: &PropertyAlternativeKind::Default,
            scalars: None,
            options: &options,
        };

        let estimate = compute_cost_estimate(&input);
        assert_eq!(estimate.cpu_cost, s.compute_size());
        assert_eq!(estimate.memory_cost, 0.0);
        assert_eq!(estimate.network_cost, 0.0);
    }

    #[test]
    fn filter_cost_uses_input_rows_not_output_rows() {
        let mut arena = ScalarArena::new();
        let predicate = intern_typed(
            &mut arena,
            &crate::sql::analysis::TypedExpr {
                kind: crate::sql::analysis::ExprKind::Literal(
                    crate::sql::analysis::LiteralValue::Bool(true),
                ),
                data_type: arrow::datatypes::DataType::Boolean,
                nullable: false,
            },
        );
        let input_stats = stats(1_000_000.0, 16.0);
        let output_stats = stats(10.0, 16.0);
        let op = Operator::PhysicalFilter(FilterOp { predicate });
        let child_stats = [&input_stats];
        let child_outputs = [PhysicalPropertySet::any()];
        let child_output_refs = [&child_outputs[0]];
        let required = PhysicalPropertySet::any();
        let options = CostOptions::default();
        let input = CostInput {
            op: &op,
            own_stats: &output_stats,
            child_stats: &child_stats,
            child_outputs: &child_output_refs,
            required_output: &required,
            alt_kind: &PropertyAlternativeKind::Default,
            scalars: Some(&arena),
            options: &options,
        };

        let estimate = compute_cost_estimate(&input);
        assert!(estimate.cpu_cost > output_stats.compute_size());
    }

    #[test]
    fn topn_estimate_is_cheaper_than_full_sort_for_small_limit() {
        let input_stats = stats(10_000_000.0, 50.0);
        let output_stats = stats(100.0, 50.0);
        let sort = Operator::PhysicalSort(SortOp {
            items: vec![],
            analytic_partition_exprs: Vec::new(),
            partition_limit: None,
            topn_type: None,
        });
        let topn = Operator::PhysicalTopN(TopNOp {
            items: vec![],
            limit: Some(100),
            offset: None,
            phase: TopNPhase::Final,
            is_split: false,
        });
        let options = CostOptions::default();
        let required = PhysicalPropertySet::any();
        let child_outputs = [PhysicalPropertySet::any()];
        let child_output_refs = [&child_outputs[0]];
        let sort_child_stats = [&input_stats];
        let topn_child_stats = [&input_stats];
        let sort_input = CostInput {
            op: &sort,
            own_stats: &input_stats,
            child_stats: &sort_child_stats,
            child_outputs: &child_output_refs,
            required_output: &required,
            alt_kind: &PropertyAlternativeKind::Default,
            scalars: None,
            options: &options,
        };
        let topn_input = CostInput {
            op: &topn,
            own_stats: &output_stats,
            child_stats: &topn_child_stats,
            child_outputs: &child_output_refs,
            required_output: &required,
            alt_kind: &PropertyAlternativeKind::Default,
            scalars: None,
            options: &options,
        };

        let topn_estimate = compute_cost_estimate(&topn_input);
        assert!(topn_estimate.memory_cost > 0.0);
        assert!(
            topn_estimate.total_with_options(&options)
                < compute_cost_estimate(&sort_input).total_with_options(&options)
        );
    }

    #[test]
    fn cost_estimate_dimensions_are_finite_for_invalid_stats() {
        let invalid_stats = stats(f64::NAN, f64::INFINITY);
        let op = Operator::PhysicalSort(SortOp {
            items: vec![],
            analytic_partition_exprs: Vec::new(),
            partition_limit: None,
            topn_type: None,
        });
        let child_stats = [&invalid_stats];
        let child_outputs = [PhysicalPropertySet::any()];
        let child_output_refs = [&child_outputs[0]];
        let required = PhysicalPropertySet::any();
        let options = CostOptions::default();
        let input = CostInput {
            op: &op,
            own_stats: &invalid_stats,
            child_stats: &child_stats,
            child_outputs: &child_output_refs,
            required_output: &required,
            alt_kind: &PropertyAlternativeKind::Default,
            scalars: None,
            options: &options,
        };

        let estimate = compute_cost_estimate(&input);
        assert_finite_non_negative_dimensions(&estimate);
    }

    #[test]
    fn sort_cost_estimate_cpu_saturates_for_huge_input_rows() {
        let huge_stats = stats(f64::MAX, 8.0);
        let op = Operator::PhysicalSort(SortOp {
            items: vec![],
            analytic_partition_exprs: Vec::new(),
            partition_limit: None,
            topn_type: None,
        });
        let child_stats = [&huge_stats];
        let child_outputs = [PhysicalPropertySet::any()];
        let child_output_refs = [&child_outputs[0]];
        let required = PhysicalPropertySet::any();
        let options = CostOptions::default();
        let input = CostInput {
            op: &op,
            own_stats: &huge_stats,
            child_stats: &child_stats,
            child_outputs: &child_output_refs,
            required_output: &required,
            alt_kind: &PropertyAlternativeKind::Default,
            scalars: None,
            options: &options,
        };

        let estimate = compute_cost_estimate(&input);
        assert_finite_non_negative_dimensions(&estimate);
        assert_eq!(estimate.cpu_cost, MAX_FINITE_COST);
    }

    #[test]
    fn sort_cost_estimate_cpu_saturates_for_infinite_input_rows() {
        let infinite_stats = stats(f64::INFINITY, 8.0);
        let op = Operator::PhysicalSort(SortOp {
            items: vec![],
            analytic_partition_exprs: Vec::new(),
            partition_limit: None,
            topn_type: None,
        });
        let child_stats = [&infinite_stats];
        let child_outputs = [PhysicalPropertySet::any()];
        let child_output_refs = [&child_outputs[0]];
        let required = PhysicalPropertySet::any();
        let options = CostOptions::default();
        let input = CostInput {
            op: &op,
            own_stats: &infinite_stats,
            child_stats: &child_stats,
            child_outputs: &child_output_refs,
            required_output: &required,
            alt_kind: &PropertyAlternativeKind::Default,
            scalars: None,
            options: &options,
        };

        let estimate = compute_cost_estimate(&input);
        assert_finite_non_negative_dimensions(&estimate);
        assert_eq!(estimate.cpu_cost, MAX_FINITE_COST);
    }

    #[test]
    fn scan_cost_estimate_dimensions_are_finite_for_overflow_size() {
        let overflow_stats = stats(f64::MAX, f64::MAX);
        let op = scan_op();
        let child_stats: [&Statistics; 0] = [];
        let child_outputs: [&PhysicalPropertySet; 0] = [];
        let required = PhysicalPropertySet::any();
        let options = CostOptions::default();
        let input = CostInput {
            op: &op,
            own_stats: &overflow_stats,
            child_stats: &child_stats,
            child_outputs: &child_outputs,
            required_output: &required,
            alt_kind: &PropertyAlternativeKind::Default,
            scalars: None,
            options: &options,
        };

        let estimate = compute_cost_estimate(&input);
        assert_finite_non_negative_dimensions(&estimate);
        assert_eq!(estimate.cpu_cost, MAX_FINITE_COST);
    }

    #[test]
    fn scan_cost_estimate_saturates_for_overflowed_row_width() {
        let overflow_stats = stats_with_column_widths(10.0, &[f64::MAX, f64::MAX]);
        let op = scan_op();
        let child_stats: [&Statistics; 0] = [];
        let child_outputs: [&PhysicalPropertySet; 0] = [];
        let required = PhysicalPropertySet::any();
        let options = CostOptions::default();
        let input = CostInput {
            op: &op,
            own_stats: &overflow_stats,
            child_stats: &child_stats,
            child_outputs: &child_outputs,
            required_output: &required,
            alt_kind: &PropertyAlternativeKind::Default,
            scalars: None,
            options: &options,
        };

        let estimate = compute_cost_estimate(&input);
        assert_finite_non_negative_dimensions(&estimate);
        assert_eq!(estimate.cpu_cost, MAX_FINITE_COST);
    }

    #[test]
    fn scan_cost_estimate_saturates_for_infinite_rows() {
        let infinite_stats = stats(f64::INFINITY, 8.0);
        let op = scan_op();
        let child_stats: [&Statistics; 0] = [];
        let child_outputs: [&PhysicalPropertySet; 0] = [];
        let required = PhysicalPropertySet::any();
        let options = CostOptions::default();
        let input = CostInput {
            op: &op,
            own_stats: &infinite_stats,
            child_stats: &child_stats,
            child_outputs: &child_outputs,
            required_output: &required,
            alt_kind: &PropertyAlternativeKind::Default,
            scalars: None,
            options: &options,
        };

        let estimate = compute_cost_estimate(&input);
        assert_finite_non_negative_dimensions(&estimate);
        assert_eq!(estimate.cpu_cost, MAX_FINITE_COST);
    }

    #[test]
    fn topn_cost_estimate_cpu_saturates_for_infinite_input_rows() {
        let infinite_input_stats = stats(f64::INFINITY, 8.0);
        let output_stats = stats(100.0, 8.0);
        let op = Operator::PhysicalTopN(TopNOp {
            items: vec![],
            limit: None,
            offset: None,
            phase: TopNPhase::Final,
            is_split: false,
        });
        let child_stats = [&infinite_input_stats];
        let child_outputs = [PhysicalPropertySet::any()];
        let child_output_refs = [&child_outputs[0]];
        let required = PhysicalPropertySet::any();
        let options = CostOptions::default();
        let input = CostInput {
            op: &op,
            own_stats: &output_stats,
            child_stats: &child_stats,
            child_outputs: &child_output_refs,
            required_output: &required,
            alt_kind: &PropertyAlternativeKind::Default,
            scalars: None,
            options: &options,
        };

        let estimate = compute_cost_estimate(&input);
        assert_finite_non_negative_dimensions(&estimate);
        assert_eq!(estimate.cpu_cost, MAX_FINITE_COST);
    }

    #[test]
    fn fallback_cost_estimate_dimensions_are_finite_for_invalid_child_stats() {
        let invalid_child_stats = stats(f64::NAN, f64::INFINITY);
        let own_stats = stats(10.0, 8.0);
        let op = Operator::PhysicalHashAggregate(PhysicalHashAggregateOp {
            mode: AggMode::Single,
            group_by: vec![],
            aggregates: vec![],
            output_columns: vec![],
            is_merge: vec![],
        });
        let child_stats = [&invalid_child_stats];
        let child_outputs = [PhysicalPropertySet::any()];
        let child_output_refs = [&child_outputs[0]];
        let required = PhysicalPropertySet::any();
        let options = CostOptions::default();
        let input = CostInput {
            op: &op,
            own_stats: &own_stats,
            child_stats: &child_stats,
            child_outputs: &child_output_refs,
            required_output: &required,
            alt_kind: &PropertyAlternativeKind::Default,
            scalars: None,
            options: &options,
        };

        let estimate = compute_cost_estimate(&input);
        assert_finite_non_negative_dimensions(&estimate);
    }

    #[test]
    fn fallback_cost_estimate_saturates_nan_legacy_cost_with_positive_overflow_signal() {
        let mixed_child_stats = stats_with_column_widths(10.0, &[f64::INFINITY, f64::NEG_INFINITY]);
        let own_stats = stats(10.0, 8.0);
        let op = Operator::PhysicalHashAggregate(PhysicalHashAggregateOp {
            mode: AggMode::Single,
            group_by: vec![],
            aggregates: vec![],
            output_columns: vec![],
            is_merge: vec![],
        });
        let child_stats = [&mixed_child_stats];
        let child_outputs = [PhysicalPropertySet::any()];
        let child_output_refs = [&child_outputs[0]];
        let required = PhysicalPropertySet::any();
        let options = CostOptions::default();
        let input = CostInput {
            op: &op,
            own_stats: &own_stats,
            child_stats: &child_stats,
            child_outputs: &child_output_refs,
            required_output: &required,
            alt_kind: &PropertyAlternativeKind::Default,
            scalars: None,
            options: &options,
        };

        let estimate = compute_cost_estimate(&input);
        assert_finite_non_negative_dimensions(&estimate);
        assert_eq!(estimate.cpu_cost, MAX_FINITE_COST);
    }

    #[test]
    fn fallback_cost_from_input_preserves_legacy_total() {
        let s = stats(1000.0, 100.0);
        let op = Operator::PhysicalValues(ValuesOp {
            rows: vec![],
            columns: vec![],
        });
        let child_stats: [&Statistics; 0] = [];
        let child_outputs: [&PhysicalPropertySet; 0] = [];
        let required = PhysicalPropertySet::any();
        let options = CostOptions::default();
        let input = CostInput {
            op: &op,
            own_stats: &s,
            child_stats: &child_stats,
            child_outputs: &child_outputs,
            required_output: &required,
            alt_kind: &PropertyAlternativeKind::Default,
            scalars: None,
            options: &options,
        };

        let estimate_total = compute_cost_from_input(&input);
        let legacy_total = compute_cost(&op, &s, &[]);
        assert!((estimate_total - legacy_total).abs() < f64::EPSILON);
    }

    #[test]
    fn fallback_cost_from_input_uses_property_aware_join_alternative() {
        let probe = stats(100_000.0, 100.0);
        let build = stats(10_000.0, 100.0);
        let own = stats(100_000.0, 200.0);
        let op = Operator::PhysicalHashJoin(PhysicalHashJoinOp {
            join_type: JoinKind::Inner,
            eq_conditions: vec![],
            other_condition: None,
            distribution: JoinDistribution::Unknown,
        });
        let child_stats = [&probe, &build];
        let child_outputs = [PhysicalPropertySet::any(), PhysicalPropertySet::broadcast()];
        let child_output_refs = [&child_outputs[0], &child_outputs[1]];
        let required = PhysicalPropertySet::any();
        let options = CostOptions::default();
        let input = CostInput {
            op: &op,
            own_stats: &own,
            child_stats: &child_stats,
            child_outputs: &child_output_refs,
            required_output: &required,
            alt_kind: &PropertyAlternativeKind::BroadcastJoin,
            scalars: None,
            options: &options,
        };

        let input_cost = compute_cost_from_input(&input);
        let property_cost = compute_cost_with_properties(
            &op,
            &own,
            &child_stats,
            &child_output_refs,
            &PropertyAlternativeKind::BroadcastJoin,
            &options,
        );
        assert!((input_cost - property_cost).abs() < f64::EPSILON);
    }

    #[test]
    fn fallback_cost_estimate_uses_legacy_property_helper_for_unmodeled_operator() {
        let s = stats(1000.0, 100.0);
        let op = Operator::PhysicalValues(ValuesOp {
            rows: vec![],
            columns: vec![],
        });
        let child_stats: [&Statistics; 0] = [];
        let child_outputs: [&PhysicalPropertySet; 0] = [];
        let required = PhysicalPropertySet::any();
        let options = CostOptions::default();
        let input = CostInput {
            op: &op,
            own_stats: &s,
            child_stats: &child_stats,
            child_outputs: &child_outputs,
            required_output: &required,
            alt_kind: &PropertyAlternativeKind::Default,
            scalars: None,
            options: &options,
        };

        let estimate_total = compute_cost_from_input(&input);
        let legacy_cost = compute_legacy_cost_with_properties(
            &op,
            &s,
            &child_stats,
            &child_outputs,
            &PropertyAlternativeKind::Default,
            &options,
        );
        assert!((estimate_total - legacy_cost).abs() < f64::EPSILON);
    }

    #[test]
    fn broadcast_join_estimate_charges_backend_fanout() {
        let probe = stats(1_000_000.0, 64.0);
        let build = stats(10_000.0, 32.0);
        let own = stats(100_000.0, 96.0);
        let op = Operator::PhysicalHashJoin(PhysicalHashJoinOp {
            join_type: JoinKind::Inner,
            eq_conditions: vec![],
            other_condition: None,
            distribution: JoinDistribution::Unknown,
        });
        let options = CostOptions::default();
        let required = PhysicalPropertySet::any();
        let child_outputs = [PhysicalPropertySet::any(), PhysicalPropertySet::broadcast()];
        let input = CostInput {
            op: &op,
            own_stats: &own,
            child_stats: &[&probe, &build],
            child_outputs: &[&child_outputs[0], &child_outputs[1]],
            required_output: &required,
            alt_kind: &PropertyAlternativeKind::BroadcastJoin,
            scalars: None,
            options: &options,
        };

        let estimate = compute_cost_estimate(&input);
        assert!(estimate.memory_cost >= build.compute_size() * options.backend_factor);
        assert!(estimate.network_cost >= build.compute_size() * options.backend_factor);
    }

    #[test]
    fn shuffle_join_estimate_charges_both_sides_network() {
        let probe = stats(1_000_000.0, 64.0);
        let build = stats(1_000_000.0, 64.0);
        let own = stats(100_000.0, 128.0);
        let op = Operator::PhysicalHashJoin(PhysicalHashJoinOp {
            join_type: JoinKind::Inner,
            eq_conditions: vec![],
            other_condition: None,
            distribution: JoinDistribution::Unknown,
        });
        let options = CostOptions::default();
        let required = PhysicalPropertySet::any();
        let child_outputs = [PhysicalPropertySet::any(), PhysicalPropertySet::any()];
        let input = CostInput {
            op: &op,
            own_stats: &own,
            child_stats: &[&probe, &build],
            child_outputs: &[&child_outputs[0], &child_outputs[1]],
            required_output: &required,
            alt_kind: &PropertyAlternativeKind::ShuffleJoin,
            scalars: None,
            options: &options,
        };

        let estimate = compute_cost_estimate(&input);
        assert!(estimate.network_cost >= probe.compute_size() + build.compute_size());
    }

    #[test]
    fn shuffle_join_estimate_waives_network_for_already_hash_partitioned_children() {
        let probe = stats(1_000_000.0, 64.0);
        let build = stats(1_000_000.0, 64.0);
        let own = stats(100_000.0, 128.0);
        let op = Operator::PhysicalHashJoin(PhysicalHashJoinOp {
            join_type: JoinKind::Inner,
            eq_conditions: vec![],
            other_condition: None,
            distribution: JoinDistribution::Unknown,
        });
        let options = CostOptions::default();
        let required = PhysicalPropertySet::any();
        let child_outputs = [
            PhysicalPropertySet {
                distribution: DistributionSpec::shuffle_join([ColumnId(1)]),
                ordering: OrderingSpec::Any,
            },
            PhysicalPropertySet {
                distribution: DistributionSpec::shuffle_join([ColumnId(2)]),
                ordering: OrderingSpec::Any,
            },
        ];
        let input = CostInput {
            op: &op,
            own_stats: &own,
            child_stats: &[&probe, &build],
            child_outputs: &[&child_outputs[0], &child_outputs[1]],
            required_output: &required,
            alt_kind: &PropertyAlternativeKind::ShuffleJoin,
            scalars: None,
            options: &options,
        };

        let estimate = compute_cost_estimate(&input);
        assert_eq!(estimate.network_cost, 0.0);
    }

    #[test]
    fn shuffle_join_estimate_scales_memory_by_backend_factor() {
        let probe = stats(1_000_000.0, 64.0);
        let build = stats(10_000.0, 32.0);
        let own = stats(100_000.0, 96.0);
        let op = Operator::PhysicalHashJoin(PhysicalHashJoinOp {
            join_type: JoinKind::Inner,
            eq_conditions: vec![],
            other_condition: None,
            distribution: JoinDistribution::Unknown,
        });
        let options = CostOptions::default();
        let required = PhysicalPropertySet::any();
        let child_outputs = [
            PhysicalPropertySet {
                distribution: DistributionSpec::shuffle_join([ColumnId(1)]),
                ordering: OrderingSpec::Any,
            },
            PhysicalPropertySet {
                distribution: DistributionSpec::shuffle_join([ColumnId(2)]),
                ordering: OrderingSpec::Any,
            },
        ];
        let input = CostInput {
            op: &op,
            own_stats: &own,
            child_stats: &[&probe, &build],
            child_outputs: &[&child_outputs[0], &child_outputs[1]],
            required_output: &required,
            alt_kind: &PropertyAlternativeKind::ShuffleJoin,
            scalars: None,
            options: &options,
        };

        let estimate = compute_cost_estimate(&input);
        let expected_memory = build.compute_size() / options.backend_factor.max(1.0);
        assert!((estimate.memory_cost - expected_memory).abs() <= f64::EPSILON);
        assert_eq!(estimate.network_cost, 0.0);
    }

    #[test]
    fn hash_join_cpu_increases_with_key_count() {
        let probe = stats(10_000.0, 16.0);
        let build = stats(5_000.0, 16.0);
        let own = stats(1_000.0, 32.0);
        let mut scalars = ScalarArena::new();
        let first_key = test_eq_condition(&mut scalars, 1, 11);
        let second_key = test_eq_condition(&mut scalars, 2, 12);
        let third_key = test_eq_condition(&mut scalars, 3, 13);
        let single_key = Operator::PhysicalHashJoin(PhysicalHashJoinOp {
            join_type: JoinKind::Inner,
            eq_conditions: vec![first_key.clone()],
            other_condition: None,
            distribution: JoinDistribution::Colocate,
        });
        let multi_key = Operator::PhysicalHashJoin(PhysicalHashJoinOp {
            join_type: JoinKind::Inner,
            eq_conditions: vec![first_key, second_key, third_key],
            other_condition: None,
            distribution: JoinDistribution::Colocate,
        });
        let options = CostOptions::default();
        let required = PhysicalPropertySet::any();
        let child_outputs = [PhysicalPropertySet::any(), PhysicalPropertySet::any()];

        let single_input = CostInput {
            op: &single_key,
            own_stats: &own,
            child_stats: &[&probe, &build],
            child_outputs: &[&child_outputs[0], &child_outputs[1]],
            required_output: &required,
            alt_kind: &PropertyAlternativeKind::Default,
            scalars: Some(&scalars),
            options: &options,
        };
        let multi_input = CostInput {
            op: &multi_key,
            own_stats: &own,
            child_stats: &[&probe, &build],
            child_outputs: &[&child_outputs[0], &child_outputs[1]],
            required_output: &required,
            alt_kind: &PropertyAlternativeKind::Default,
            scalars: Some(&scalars),
            options: &options,
        };

        assert!(
            compute_cost_estimate(&multi_input).cpu_cost
                > compute_cost_estimate(&single_input).cpu_cost
        );
    }

    #[test]
    fn hash_join_cpu_includes_output_size() {
        let probe = stats(10_000.0, 16.0);
        let build = stats(5_000.0, 16.0);
        let low_output = stats(10.0, 8.0);
        let high_output = stats(100_000.0, 128.0);
        let mut scalars = ScalarArena::new();
        let op = Operator::PhysicalHashJoin(PhysicalHashJoinOp {
            join_type: JoinKind::Inner,
            eq_conditions: vec![test_eq_condition(&mut scalars, 1, 11)],
            other_condition: None,
            distribution: JoinDistribution::Colocate,
        });
        let options = CostOptions::default();
        let required = PhysicalPropertySet::any();
        let child_outputs = [PhysicalPropertySet::any(), PhysicalPropertySet::any()];

        let low_input = CostInput {
            op: &op,
            own_stats: &low_output,
            child_stats: &[&probe, &build],
            child_outputs: &[&child_outputs[0], &child_outputs[1]],
            required_output: &required,
            alt_kind: &PropertyAlternativeKind::Default,
            scalars: Some(&scalars),
            options: &options,
        };
        let high_input = CostInput {
            op: &op,
            own_stats: &high_output,
            child_stats: &[&probe, &build],
            child_outputs: &[&child_outputs[0], &child_outputs[1]],
            required_output: &required,
            alt_kind: &PropertyAlternativeKind::Default,
            scalars: Some(&scalars),
            options: &options,
        };

        assert!(
            compute_cost_estimate(&high_input).cpu_cost
                > compute_cost_estimate(&low_input).cpu_cost
        );
    }

    #[test]
    fn nested_loop_join_memory_uses_build_side_size() {
        let left = stats(10_000.0, 8.0);
        let build = stats(50_000.0, 256.0);
        let own = stats(1.0, 8.0);
        let op = Operator::PhysicalNestLoopJoin(PhysicalNestLoopJoinOp {
            join_type: JoinKind::Inner,
            condition: None,
        });
        let options = CostOptions::default();
        let required = PhysicalPropertySet::any();
        let child_outputs = [PhysicalPropertySet::any(), PhysicalPropertySet::any()];
        let input = CostInput {
            op: &op,
            own_stats: &own,
            child_stats: &[&left, &build],
            child_outputs: &[&child_outputs[0], &child_outputs[1]],
            required_output: &required,
            alt_kind: &PropertyAlternativeKind::Default,
            scalars: None,
            options: &options,
        };

        let estimate = compute_cost_estimate(&input);
        let expected_memory = build.compute_size();
        assert!((estimate.memory_cost - expected_memory).abs() <= f64::EPSILON);
        assert!(estimate.memory_cost > own.compute_size() * 0.05);
    }

    #[test]
    fn cost_options_weights_drive_total_cost() {
        let options = CostOptions {
            cpu_weight: 1.0,
            memory_weight: 10.0,
            network_weight: 100.0,
            ..Default::default()
        };
        let estimate = CostEstimate {
            cpu_cost: 1.0,
            memory_cost: 2.0,
            network_cost: 3.0,
        };

        assert_eq!(estimate.total_with_options(&options), 321.0);
    }

    #[test]
    fn cost_options_clamp_invalid_weights() {
        let options = CostOptions {
            cpu_weight: 0.0,
            memory_weight: -1.0,
            network_weight: f64::NAN,
            ..Default::default()
        };
        let estimate = CostEstimate {
            cpu_cost: 1.0,
            memory_cost: 2.0,
            network_cost: 3.0,
        };

        let total = estimate.total_with_options(&options);
        assert!(total.is_finite());
        assert_eq!(total, 0.0);
    }

    #[test]
    fn scan_cost_equals_data_size() {
        let s = stats(1000.0, 100.0);
        let op = Operator::PhysicalScan(ScanOp {
            database: String::new(),
            table: crate::sql::catalog::TableDef {
                name: "t".into(),
                columns: vec![],
                iceberg_row_lineage_metadata_columns: vec![],
                source: crate::sql::catalog::ScanSource::StarRocks {
                    db_id: 0,
                    table_id: 0,
                },
            },
            alias: None,
            columns: vec![],
            predicates: vec![],
            required_columns: None,
            dict_columns: vec![],
            variant_columns: vec![],
            mv_rewritten_from: None,
        });
        let cost = compute_cost(&op, &s, &[]);
        assert!((cost - 100_000.0).abs() < 1.0);
    }

    #[test]
    fn scan_cost_uses_required_columns_when_pruned() {
        let s = stats_with_column_widths(1000.0, &[4.0, 128.0]);
        let op = two_column_scan_op(Some(vec!["narrow"]));

        let legacy_cost = compute_cost(&op, &s, &[]);
        let input = CostInput {
            op: &op,
            own_stats: &s,
            child_stats: &[],
            child_outputs: &[],
            required_output: &PhysicalPropertySet::any(),
            alt_kind: &PropertyAlternativeKind::Default,
            scalars: None,
            options: &CostOptions::default(),
        };
        let estimate = compute_cost_estimate(&input);

        assert_eq!(legacy_cost, 4_000.0);
        assert_eq!(estimate.cpu_cost, 4_000.0);
    }

    #[test]
    fn scan_cost_with_required_columns_saturates_infinite_rows() {
        let s = stats_with_column_widths(f64::INFINITY, &[4.0, 128.0]);
        let op = two_column_scan_op(Some(vec!["narrow"]));

        let legacy_cost = compute_cost(&op, &s, &[]);
        let input = CostInput {
            op: &op,
            own_stats: &s,
            child_stats: &[],
            child_outputs: &[],
            required_output: &PhysicalPropertySet::any(),
            alt_kind: &PropertyAlternativeKind::Default,
            scalars: None,
            options: &CostOptions::default(),
        };
        let estimate = compute_cost_estimate(&input);

        assert_eq!(legacy_cost, MAX_FINITE_COST);
        assert_eq!(estimate.cpu_cost, MAX_FINITE_COST);
    }

    #[test]
    fn shuffle_join_more_expensive_than_colocate() {
        let probe = stats(100_000.0, 100.0);
        let build = stats(10_000.0, 100.0);
        let own = stats(100_000.0, 200.0);

        let shuffle = Operator::PhysicalHashJoin(PhysicalHashJoinOp {
            join_type: JoinKind::Inner,
            eq_conditions: vec![],
            other_condition: None,
            distribution: JoinDistribution::Shuffle,
        });
        let colocate = Operator::PhysicalHashJoin(PhysicalHashJoinOp {
            join_type: JoinKind::Inner,
            eq_conditions: vec![],
            other_condition: None,
            distribution: JoinDistribution::Colocate,
        });
        let cs = [&probe, &build];
        let c_shuffle = compute_cost(&shuffle, &own, &cs);
        let c_colocate = compute_cost(&colocate, &own, &cs);
        assert!(c_shuffle > c_colocate);
    }

    #[test]
    fn child_output_aware_shuffle_join_does_not_charge_network_exchange_twice() {
        let probe = stats(100_000.0, 100.0);
        let build = stats(10_000.0, 100.0);
        let own = stats(100_000.0, 200.0);
        let op = Operator::PhysicalHashJoin(PhysicalHashJoinOp {
            join_type: JoinKind::Inner,
            eq_conditions: vec![],
            other_condition: None,
            distribution: JoinDistribution::Unknown,
        });
        let child_stats = [&probe, &build];
        let left_output = PhysicalPropertySet {
            distribution: DistributionSpec::shuffle_join([ColumnId(1)]),
            ordering: OrderingSpec::Any,
        };
        let right_output = PhysicalPropertySet {
            distribution: DistributionSpec::shuffle_join([ColumnId(2)]),
            ordering: OrderingSpec::Any,
        };
        let child_outputs = [&left_output, &right_output];

        let cost = compute_cost_with_properties(
            &op,
            &own,
            &child_stats,
            &child_outputs,
            &PropertyAlternativeKind::ShuffleJoin,
            &CostOptions::default(),
        );
        let unshuffled_outputs = [PhysicalPropertySet::any(), PhysicalPropertySet::any()];
        let unshuffled_child_outputs = [&unshuffled_outputs[0], &unshuffled_outputs[1]];
        let unshuffled_cost = compute_cost_with_properties(
            &op,
            &own,
            &child_stats,
            &unshuffled_child_outputs,
            &PropertyAlternativeKind::ShuffleJoin,
            &CostOptions::default(),
        );

        assert!(cost > 0.0);
        assert!(cost < unshuffled_cost);
    }

    #[test]
    fn broadcast_gate_rejects_fallback_build_above_fallback_limit() {
        let mut build = stats(600_000.0, 100.0);
        build.row_count_confidence = crate::sql::optimizer::statistics::Confidence::Fallback;
        let probe = stats(700_000.0, 100.0);
        let options = CostOptions::default();

        assert!(!broadcast_gate_passes(&probe, &build, &options));
    }

    #[test]
    fn broadcast_gate_rejects_estimated_build_above_fallback_limit() {
        let mut build = stats(648_000.0, 100.0);
        build.row_count_confidence = crate::sql::optimizer::statistics::Confidence::Estimated;
        let probe = stats(3_543_657.0, 100.0);
        let options = CostOptions::default();

        assert!(!broadcast_gate_passes(&probe, &build, &options));
    }

    #[test]
    fn broadcast_join_alternative_charges_fanout_and_memory_pressure() {
        let probe = stats(100_000.0, 100.0);
        let build = stats(10_000.0, 100.0);
        let own = stats(100_000.0, 200.0);
        let op = Operator::PhysicalHashJoin(PhysicalHashJoinOp {
            join_type: JoinKind::Inner,
            eq_conditions: vec![],
            other_condition: None,
            distribution: JoinDistribution::Unknown,
        });
        let child_stats = [&probe, &build];
        let child_outputs = [PhysicalPropertySet::any(), PhysicalPropertySet::broadcast()];
        let child_output_refs = [&child_outputs[0], &child_outputs[1]];
        let options = CostOptions::default();

        let required = PhysicalPropertySet::any();
        let input = CostInput {
            op: &op,
            own_stats: &own,
            child_stats: &child_stats,
            child_outputs: &child_output_refs,
            required_output: &required,
            alt_kind: &PropertyAlternativeKind::BroadcastJoin,
            scalars: None,
            options: &options,
        };
        let estimate = compute_cost_estimate(&input);
        let expected = CostEstimate {
            cpu_cost: finite_non_negative_cost(
                (cost_row_count(&probe) + cost_row_count(&build)) * options.hash_cost_factor
                    + safe_compute_size(&own),
            ),
            memory_cost: build.compute_size() * options.backend_factor,
            network_cost: build.compute_size() * options.backend_factor,
        };

        assert!((estimate.cpu_cost - expected.cpu_cost).abs() < f64::EPSILON);
        assert!((estimate.memory_cost - expected.memory_cost).abs() < f64::EPSILON);
        assert!((estimate.network_cost - expected.network_cost).abs() < f64::EPSILON);
        assert!(
            (compute_cost_with_properties(
                &op,
                &own,
                &child_stats,
                &child_output_refs,
                &PropertyAlternativeKind::BroadcastJoin,
                &options,
            ) - expected.total_with_options(&options))
            .abs()
                < f64::EPSILON
        );
    }

    #[test]
    fn non_equi_hash_join_uses_optimizer_execute_cost_penalty() {
        let probe = stats(100_000.0, 100.0);
        let build = stats(10_000.0, 100.0);
        let own = stats(100_000.0, 200.0);
        let mut scalars = ScalarArena::new();
        let other_condition = intern_typed(
            &mut scalars,
            &crate::sql::analysis::TypedExpr {
                kind: crate::sql::analysis::ExprKind::Literal(
                    crate::sql::analysis::LiteralValue::Bool(true),
                ),
                data_type: arrow::datatypes::DataType::Boolean,
                nullable: false,
            },
        );
        let op = Operator::PhysicalHashJoin(PhysicalHashJoinOp {
            join_type: JoinKind::Inner,
            eq_conditions: vec![],
            other_condition: Some(other_condition),
            distribution: JoinDistribution::Unknown,
        });
        let child_stats = [&probe, &build];
        let child_outputs = [PhysicalPropertySet::any(), PhysicalPropertySet::broadcast()];
        let child_output_refs = [&child_outputs[0], &child_outputs[1]];
        let options = CostOptions::default();

        let required = PhysicalPropertySet::any();
        let input = CostInput {
            op: &op,
            own_stats: &own,
            child_stats: &child_stats,
            child_outputs: &child_output_refs,
            required_output: &required,
            alt_kind: &PropertyAlternativeKind::BroadcastJoin,
            scalars: Some(&scalars),
            options: &options,
        };
        let estimate = compute_cost_estimate(&input);
        let base_cpu = finite_non_negative_cost(
            (cost_row_count(&probe) + cost_row_count(&build)) * options.hash_cost_factor
                + safe_compute_size(&own),
        );
        let broadcast_fanout_size = build.compute_size() * options.backend_factor;

        assert!((estimate.cpu_cost - base_cpu * NON_EQUI_JOIN_COST_PENALTY).abs() < f64::EPSILON);
        assert!((estimate.memory_cost - broadcast_fanout_size).abs() < f64::EPSILON);
        assert!((estimate.network_cost - broadcast_fanout_size).abs() < f64::EPSILON);
    }

    #[test]
    fn local_agg_cheaper_than_single() {
        let input = stats(100_000.0, 50.0);
        let own = stats(100.0, 50.0);

        let single = Operator::PhysicalHashAggregate(PhysicalHashAggregateOp {
            mode: AggMode::Single,
            group_by: vec![],
            aggregates: vec![],
            output_columns: vec![],
            is_merge: vec![],
        });
        let local = Operator::PhysicalHashAggregate(PhysicalHashAggregateOp {
            mode: AggMode::Local,
            group_by: vec![],
            aggregates: vec![],
            output_columns: vec![],
            is_merge: vec![],
        });

        let cs = [&input];
        assert!(compute_cost(&single, &own, &cs) > compute_cost(&local, &own, &cs));
    }

    #[test]
    fn split_agg_total_cost_can_win_or_lose_after_exchange_cost() {
        use crate::sql::optimizer::property::DistributionSpec;

        let single = Operator::PhysicalHashAggregate(PhysicalHashAggregateOp {
            mode: AggMode::Single,
            group_by: vec![],
            aggregates: vec![],
            output_columns: vec![],
            is_merge: vec![],
        });
        let local = Operator::PhysicalHashAggregate(PhysicalHashAggregateOp {
            mode: AggMode::Local,
            group_by: vec![],
            aggregates: vec![],
            output_columns: vec![],
            is_merge: vec![],
        });
        let global = Operator::PhysicalHashAggregate(PhysicalHashAggregateOp {
            mode: AggMode::Global,
            group_by: vec![],
            aggregates: vec![],
            output_columns: vec![],
            is_merge: vec![],
        });
        let gather = Operator::PhysicalDistribution(PhysicalDistributionOp {
            spec: DistributionSpec::Gather,
        });

        let large_input = stats(1_000_000.0, 100.0);
        let reduced_rows = stats(100.0, 16.0);
        let final_rows = stats(100.0, 16.0);
        let single_large_cost = compute_cost(&single, &final_rows, &[&large_input]);
        let split_large_cost = compute_cost(&local, &reduced_rows, &[&large_input])
            + compute_cost(&gather, &reduced_rows, &[])
            + compute_cost(&global, &final_rows, &[&reduced_rows]);
        assert!(split_large_cost < single_large_cost);

        let small_input = stats(10.0, 8.0);
        let unreduced_rows = stats(10.0, 8.0);
        let single_small_cost = compute_cost(&single, &unreduced_rows, &[&small_input]);
        let split_small_cost = compute_cost(&local, &unreduced_rows, &[&small_input])
            + compute_cost(&gather, &unreduced_rows, &[])
            + compute_cost(&global, &unreduced_rows, &[&unreduced_rows]);
        assert!(single_small_cost < split_small_cost);
    }

    #[test]
    fn sort_cost_nlogn() {
        let s = stats(1024.0, 10.0);
        let op = Operator::PhysicalSort(SortOp {
            items: vec![],
            analytic_partition_exprs: Vec::new(),
            partition_limit: None,
            topn_type: None,
        });
        let cost = compute_cost(&op, &s, &[]);
        // 1024 * log2(1024) = 1024 * 10 = 10240
        assert!((cost - 10_240.0).abs() < 1.0);
    }

    #[test]
    fn logical_ops_have_zero_cost() {
        let s = stats(1000.0, 100.0);
        let op = Operator::LogicalScan(ScanOp {
            database: String::new(),
            table: crate::sql::catalog::TableDef {
                name: "t".into(),
                columns: vec![],
                iceberg_row_lineage_metadata_columns: vec![],
                source: crate::sql::catalog::ScanSource::StarRocks {
                    db_id: 0,
                    table_id: 0,
                },
            },
            alias: None,
            columns: vec![],
            predicates: vec![],
            required_columns: None,
            dict_columns: vec![],
            variant_columns: vec![],
            mv_rewritten_from: None,
        });
        assert!((compute_cost(&op, &s, &[]) - 0.0).abs() < f64::EPSILON);
    }

    #[test]
    fn limit_is_nearly_free() {
        let s = stats(1_000_000.0, 100.0);
        let op = Operator::PhysicalLimit(LimitOp {
            limit: Some(10),
            offset: None,
        });
        assert!(compute_cost(&op, &s, &[]) < 1.0);
    }

    #[test]
    fn distribution_has_network_multiplier() {
        let s = stats(1000.0, 100.0);
        let op = Operator::PhysicalDistribution(PhysicalDistributionOp {
            spec: crate::sql::optimizer::property::DistributionSpec::Any,
        });
        let cost = compute_cost(&op, &s, &[]);
        // 16 MiB + 1000 * 100 * 1.5 = 16_927_216
        let expected = DISTRIBUTION_STARTUP_COST + 150_000.0;
        assert!((cost - expected).abs() < 1.0);
    }

    #[test]
    fn top_n_cheaper_than_sort_for_small_limit() {
        // Input of 10M rows; TopN's own_stats is the limited output (k=100 rows),
        // while its child's output (the scan) has 10M rows.
        let input = stats(10_000_000.0, 50.0);
        let own = stats(100.0, 50.0);
        let sort = Operator::PhysicalSort(SortOp {
            items: vec![],
            analytic_partition_exprs: Vec::new(),
            partition_limit: None,
            topn_type: None,
        });
        let top_n = Operator::PhysicalTopN(TopNOp {
            items: vec![],
            limit: Some(100),
            offset: None,
            phase: TopNPhase::Final,
            is_split: false,
        });
        let cost_sort = compute_cost(&sort, &input, &[]);
        let cost_top_n = compute_cost(&top_n, &own, &[&input]);
        // Expected ratio ~ log2(100)/log2(10M) ≈ 0.286.
        assert!(
            cost_top_n < cost_sort * 0.5,
            "expected TOP-N strictly cheaper than Sort; got top_n={} sort={}",
            cost_top_n,
            cost_sort
        );
    }

    #[test]
    fn top_n_falls_back_to_sort_cost_when_limit_exceeds_rows() {
        // When limit >> input rows, TopN's k clamps to input rows, and cost
        // equals Sort's cost (both are n * log2(n)).
        let input = stats(100.0, 10.0);
        let own = stats(100.0, 10.0); // unlimited output (limit exceeds input)
        let sort = Operator::PhysicalSort(SortOp {
            items: vec![],
            analytic_partition_exprs: Vec::new(),
            partition_limit: None,
            topn_type: None,
        });
        let top_n = Operator::PhysicalTopN(TopNOp {
            items: vec![],
            limit: Some(10_000),
            offset: None,
            phase: TopNPhase::Final,
            is_split: false,
        });
        let cost_sort = compute_cost(&sort, &input, &[]);
        let cost_top_n = compute_cost(&top_n, &own, &[&input]);
        assert!((cost_top_n - cost_sort).abs() < 1.0);
    }

    #[test]
    fn top_n_with_offset_and_limit_sums_both() {
        // limit=50 + offset=50 => k=100. Same cost as limit=100, offset=None.
        let input = stats(10_000.0, 10.0);
        let own = stats(100.0, 10.0);
        let top_n = Operator::PhysicalTopN(TopNOp {
            items: vec![],
            limit: Some(50),
            offset: Some(50),
            phase: TopNPhase::Final,
            is_split: false,
        });
        let cost = compute_cost(&top_n, &own, &[&input]);
        // input_rows=10_000, k=100, cost = 10_000 * log2(100) ≈ 66_438.56
        let expected = 10_000.0 * (100f64).log2();
        assert!(
            (cost - expected).abs() < 1.0,
            "got {}, expected {}",
            cost,
            expected
        );
    }
}
