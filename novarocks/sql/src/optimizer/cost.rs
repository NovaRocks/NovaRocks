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

//! Cost model for physical operators in the Cascades optimizer.
//!
//! Provides a single `compute_cost` function that estimates the self-cost of
//! a physical operator (not including children).  The formulas are aligned with
//! StarRocks conventions and the existing `optimizer/cost.rs` model.

use super::memo::TotalCost;
use super::operator::{
    AggMode, JoinDistribution, Operator, PhysicalHashAggregateOp, PhysicalHashJoinOp, ScanOp,
};
use super::property::{DistributionSpec, PhysicalPropertySet};
use super::scalar::{ScalarArena, ScalarId, ScalarNode};
use crate::common::JoinKind;
use crate::optimizer::derive::PropertyAlternativeKind;
use crate::optimizer::statistics::{
    CostEstimate, DEFAULT_CPU_COST_WEIGHT, DEFAULT_MEMORY_COST_WEIGHT, DEFAULT_NETWORK_COST_WEIGHT,
    MAX_FINITE_COST, Statistics, TableStatistics, finite_non_negative_dimension,
};
use crate::optimizer::stats_input::OptimizerStatsInput;

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

const DEFAULT_ROW_WIDTH: f64 = 8.0;

pub(crate) struct CostInput<'a> {
    pub op: &'a Operator,
    pub own_stats: &'a Statistics,
    pub child_stats: &'a [&'a Statistics],
    pub child_outputs: &'a [&'a PhysicalPropertySet],
    pub required_output: &'a PhysicalPropertySet,
    pub alt_kind: &'a PropertyAlternativeKind,
    pub scalars: Option<&'a ScalarArena>,
    pub stats_input: Option<&'a OptimizerStatsInput>,
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
) -> TotalCost {
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
        | Operator::LogicalChangeEventExpand(_)
        | Operator::LogicalCTEAnchor(_)
        | Operator::LogicalCTEProduce(_)
        | Operator::LogicalCTEConsume(_)
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
            let probe_size = child_stats
                .get(j.build_side.probe_ordinal())
                .map(|s| s.compute_size())
                .unwrap_or(0.0);
            let build_size = child_stats
                .get(j.build_side.input_ordinal())
                .map(|s| s.compute_size())
                .unwrap_or(0.0);

            let base_cost = match j.distribution {
                JoinDistribution::Shuffle => (build_size + probe_size) * NETWORK_COST + probe_size,
                JoinDistribution::Broadcast => build_size * NETWORK_COST + probe_size,
                JoinDistribution::Colocate => probe_size,
                JoinDistribution::Singleton => probe_size,
                JoinDistribution::Unknown => {
                    panic!("unknown join distribution should be resolved before costing")
                }
            };

            // Apply cross join penalty (StarRocks: getCrossJoinCostPenalty = 10).
            let cost_after_cross = if j.join_type == JoinKind::Cross {
                base_cost * CROSS_JOIN_COST_PENALTY
            } else {
                base_cost
            };

            // Apply non-equi join penalty: if the join has a residual
            // other_condition, hash probing is less efficient (StarRocks:
            // EXECUTE_COST_PENALTY = 100).
            if j.condition.residual().is_some() {
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
        // and CTE operators use a lightweight default.
        Operator::PhysicalWindow(_)
        | Operator::PhysicalRepeat(_)
        | Operator::PhysicalChangeEventExpand(_)
        | Operator::PhysicalUnion(_)
        | Operator::PhysicalIntersect(_)
        | Operator::PhysicalExcept(_)
        | Operator::PhysicalValues(_)
        | Operator::PhysicalGenerateSeries(_)
        | Operator::PhysicalTableFunction(_)
        | Operator::PhysicalCTEProduce(_)
        | Operator::PhysicalCTEConsume(_) => own_stats.output_row_count * 0.01,
    }
}

/// Default per-query planning-time memory budget (StarRocks maxExecMemByte analog).
const DEFAULT_QUERY_MEM_LIMIT_BYTES: f64 = 2.0 * 1024.0 * 1024.0 * 1024.0;
/// Fraction of the per-query budget a single broadcast build hash table may use.
const BUILD_HASH_TABLE_MEM_FRACTION: f64 = 0.5;
/// CI distributed baseline backend count (1 FE + 3 BE). NOT a standalone assumption.
const DEFAULT_EFFECTIVE_BACKEND_COUNT: f64 = 3.0;

/// Cluster/resource snapshot the cost kernel uses to normalize broadcast risk
/// into real resource units. All `f64` to match `CostOptions` (hot-path, no cast).
/// Populated at the engine boundary from the live BE registry; the cost formulas
/// must NEVER read a global registry directly (keeps cost free of hidden state).
#[derive(Clone, Debug)]
pub(crate) struct ClusterResourceProfile {
    /// Real BE count (live BE registry snapshot); clamped `>= 1.0` at use sites.
    /// Replaces the hardcoded `backend_factor=3.0`. CI baseline = 3.
    pub effective_backend_count: f64,
    /// LAYER 1 per-node OOM floor denominator.
    pub per_node_build_memory_budget_bytes: f64,
    /// Planning-time per-query memory limit (StarRocks maxExecMemByte analog).
    pub query_mem_limit_bytes: f64,
    /// LAYER 2 cluster-wide broadcast network floor (finite default, NOT INF).
    pub cluster_broadcast_network_budget_bytes: f64,
}

impl Default for ClusterResourceProfile {
    fn default() -> Self {
        let backends = DEFAULT_EFFECTIVE_BACKEND_COUNT;
        let per_node = DEFAULT_QUERY_MEM_LIMIT_BYTES * BUILD_HASH_TABLE_MEM_FRACTION;
        Self {
            effective_backend_count: backends,
            per_node_build_memory_budget_bytes: per_node,
            query_mem_limit_bytes: DEFAULT_QUERY_MEM_LIMIT_BYTES,
            cluster_broadcast_network_budget_bytes: per_node,
        }
    }
}

impl ClusterResourceProfile {
    pub(crate) fn apply_query_mem_limit_bytes(&mut self, query_mem_limit_bytes: f64) {
        let query_mem_limit_bytes =
            normalized_positive_bytes(query_mem_limit_bytes, DEFAULT_QUERY_MEM_LIMIT_BYTES);
        let per_node =
            finite_non_negative_cost(query_mem_limit_bytes * BUILD_HASH_TABLE_MEM_FRACTION);
        self.query_mem_limit_bytes = query_mem_limit_bytes;
        self.per_node_build_memory_budget_bytes = per_node;
        self.cluster_broadcast_network_budget_bytes = per_node;
    }
}

#[derive(Clone, Debug)]
pub(crate) struct CostOptions {
    pub cpu_weight: f64,
    pub memory_weight: f64,
    pub network_weight: f64,
    pub backend_factor: f64,
    /// Cluster/resource snapshot used by broadcast feasibility and hash-join
    /// costing. `backend_factor` remains a cached projection for older callers.
    pub profile: ClusterResourceProfile,
    pub hash_table_per_row_overhead_bytes: f64,
    pub hash_table_load_factor: f64,
    pub risk_multiplier_fallback: f64,
    pub risk_multiplier_estimated: f64,
    pub risk_multiplier_exact: f64,
    pub risk_multiplier_measured: f64,
    pub predicate_cost_factor: f64,
    pub projection_cost_factor: f64,
    pub hash_cost_factor: f64,
    pub sort_cost_factor: f64,
    pub topn_cost_factor: f64,
    pub aggregate_cost_factor: f64,
    pub exchange_startup_cost: f64,
}

impl Default for CostOptions {
    fn default() -> Self {
        Self {
            cpu_weight: DEFAULT_CPU_COST_WEIGHT,
            memory_weight: DEFAULT_MEMORY_COST_WEIGHT,
            network_weight: DEFAULT_NETWORK_COST_WEIGHT,
            backend_factor: 3.0,
            profile: ClusterResourceProfile::default(),
            hash_table_per_row_overhead_bytes: 16.0,
            hash_table_load_factor: 0.75,
            risk_multiplier_fallback: 4.0,
            risk_multiplier_estimated: 2.0,
            risk_multiplier_exact: 1.0,
            risk_multiplier_measured: 1.0,
            predicate_cost_factor: 0.02,
            projection_cost_factor: 0.01,
            hash_cost_factor: 1.0,
            sort_cost_factor: 1.0,
            topn_cost_factor: 1.0,
            aggregate_cost_factor: 1.0,
            exchange_startup_cost: DISTRIBUTION_STARTUP_COST,
        }
    }
}

impl CostOptions {
    /// Install a profile and refresh the cached `backend_factor` projection.
    /// `backend_factor` is always the normalized effective backend count, so
    /// existing `options.backend_factor` call sites need no change.
    pub(crate) fn apply_profile(&mut self, mut profile: ClusterResourceProfile) {
        let backends = normalized_effective_backend_count(profile.effective_backend_count);
        profile.effective_backend_count = backends;
        profile.query_mem_limit_bytes =
            normalized_positive_bytes(profile.query_mem_limit_bytes, DEFAULT_QUERY_MEM_LIMIT_BYTES);
        profile.per_node_build_memory_budget_bytes =
            finite_non_negative_cost(profile.per_node_build_memory_budget_bytes);
        profile.cluster_broadcast_network_budget_bytes =
            finite_non_negative_cost(profile.per_node_build_memory_budget_bytes);
        self.backend_factor = backends;
        self.profile = profile;
    }
}

fn normalized_positive_bytes(value: f64, default_value: f64) -> f64 {
    if value.is_finite() && value > 0.0 {
        finite_non_negative_cost(value)
    } else {
        default_value
    }
}

fn normalized_effective_backend_count(value: f64) -> f64 {
    if value.is_finite() && value > 0.0 {
        value.max(1.0)
    } else {
        1.0
    }
}

fn finite_non_negative_cost(value: f64) -> f64 {
    finite_non_negative_dimension(value)
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

/// Degenerate "build size unknown" detector. Triggers on the magnitude axis
/// (non-finite/<=0/overflow) AND on NovaRocks' real fabricated-default
/// fingerprint (Fallback confidence + empty column_statistics), because the
/// fallback constructors and CTE/scan missing-stats paths replace truly unknown
/// tables with a finite default magnitude + Fallback confidence. Without the
/// fingerprint arm the magnitude-only check is dead code. Trino: unknown ->
/// partitioned.
pub(crate) fn build_size_is_uninformative(build_stats: &Statistics) -> bool {
    let rows = build_stats.output_row_count;
    if !rows.is_finite() || rows <= 0.0 || safe_compute_size(build_stats) >= MAX_FINITE_COST {
        return true;
    }
    build_stats.row_count_confidence == crate::optimizer::statistics::Confidence::Fallback
        && build_stats.column_statistics.is_empty()
}

/// Estimated per-node memory of the broadcast build hash table. This feeds the
/// LAYER 1 floor and the hash-join memory dimension; network terms use raw
/// build bytes.
/// `payload / load_factor + rows * per_row_overhead`. The per-row term (16B =
/// 4x Vec<u32> in JoinHashTable) is what makes a narrow build correctly
/// expensive, dissolving the old narrow-build exception.
pub(crate) fn estimated_build_hash_table_bytes(
    build_stats: &Statistics,
    options: &CostOptions,
) -> f64 {
    let payload = safe_compute_size(build_stats);
    let rows = cost_row_count(build_stats);
    let load_factor = if options.hash_table_load_factor.is_nan() {
        0.75
    } else {
        options.hash_table_load_factor.clamp(0.5, 1.0)
    };
    let per_row = options.hash_table_per_row_overhead_bytes.max(0.0);
    finite_non_negative_cost(payload / load_factor + rows * per_row)
}

/// Dimensionless inflation factor applied to the estimated build hash-table
/// bytes BEFORE the LAYER 1 feasibility check. Feasibility-only — it does NOT
/// enter LAYER 2 cost (that would double-count and degrade into a soft gate).
/// Anchored at Exact=1.0, monotone non-increasing as confidence rises.
pub(crate) fn confidence_risk_multiplier(
    confidence: crate::optimizer::statistics::Confidence,
    options: &CostOptions,
) -> f64 {
    use crate::optimizer::statistics::Confidence;
    let multiplier = match confidence {
        Confidence::Fallback => options.risk_multiplier_fallback,
        Confidence::Estimated => options.risk_multiplier_estimated,
        Confidence::Exact => options.risk_multiplier_exact,
        Confidence::Measured => options.risk_multiplier_measured,
    };
    if multiplier.is_finite() && multiplier > 0.0 {
        multiplier
    } else {
        1.0
    }
}

#[allow(dead_code)] // BC-1 Phase 1 staged until search.rs consumes broadcast feasibility.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum BroadcastRejectReason {
    /// `risk_adj_build_bytes` exceeds the per-node memory budget (LAYER 1).
    PerNodeMemory,
    /// `risk_adj_fanout_bytes` exceeds the cluster network budget (LAYER 2).
    ClusterNetwork,
    /// Build size unknown (fabricated default stats) -> partitioned.
    UninformativeSize,
}

#[allow(dead_code)] // BC-1 Phase 1 staged until search.rs consumes broadcast feasibility.
#[derive(Clone, Copy, Debug)]
pub(crate) struct BroadcastFeasibility {
    pub(crate) feasible: bool,
    pub(crate) build_bytes: f64,
    pub(crate) hash_table_bytes: f64,
    pub(crate) effective_backend_count: f64,
    pub(crate) risk_multiplier: f64,
    pub(crate) risk_adj_build_bytes: f64,
    pub(crate) risk_adj_fanout_bytes: f64,
    pub(crate) reject_reason: Option<BroadcastRejectReason>,
}

/// LAYER 1 hard feasibility floor. Per-node memory floor NEVER divides by the
/// backend count (broadcast materializes the full build on every node) plus a
/// cluster network fanout floor. Confidence enters only here, never in cost.
#[allow(dead_code)] // BC-1 Phase 1 staged until search.rs consumes broadcast feasibility.
pub(crate) fn broadcast_is_feasible(
    _probe_stats: &Statistics,
    build_stats: &Statistics,
    options: &CostOptions,
) -> BroadcastFeasibility {
    let backends = normalized_effective_backend_count(options.profile.effective_backend_count);
    let raw = safe_compute_size(build_stats);
    let risk_mult = confidence_risk_multiplier(build_stats.row_count_confidence, options);
    let ht_bytes = estimated_build_hash_table_bytes(build_stats, options);

    if build_size_is_uninformative(build_stats) {
        return BroadcastFeasibility {
            feasible: false,
            build_bytes: raw,
            hash_table_bytes: ht_bytes,
            effective_backend_count: backends,
            risk_multiplier: risk_mult,
            risk_adj_build_bytes: finite_non_negative_cost(ht_bytes * risk_mult),
            risk_adj_fanout_bytes: finite_non_negative_cost(raw * backends * risk_mult),
            reject_reason: Some(BroadcastRejectReason::UninformativeSize),
        };
    }

    // LAYER 1 (per-node memory): NO backend divisor — full build per node.
    let risk_adj_build_bytes = finite_non_negative_cost(ht_bytes * risk_mult);
    // LAYER 2 (cluster network): charge raw bytes * backend count * risk against
    // a fixed cluster-wide budget. The budget intentionally does not scale with
    // backend count; otherwise fanout cancels out and the network floor is dead.
    let risk_adj_fanout_bytes = finite_non_negative_cost(raw * backends * risk_mult);

    let mem_ok = risk_adj_build_bytes <= options.profile.per_node_build_memory_budget_bytes;
    let net_ok = risk_adj_fanout_bytes <= options.profile.cluster_broadcast_network_budget_bytes;

    let reject_reason = if !mem_ok {
        Some(BroadcastRejectReason::PerNodeMemory)
    } else if !net_ok {
        Some(BroadcastRejectReason::ClusterNetwork)
    } else {
        None
    };

    BroadcastFeasibility {
        feasible: mem_ok && net_ok,
        build_bytes: raw,
        hash_table_bytes: ht_bytes,
        effective_backend_count: backends,
        risk_multiplier: risk_mult,
        risk_adj_build_bytes,
        risk_adj_fanout_bytes,
        reject_reason,
    }
}

/// EXPLAIN-facing broadcast decision. Single source of truth: produced only by
/// `broadcast_decision`. None for non-hash-join / non-broadcast / no-build nodes.
#[allow(dead_code)] // BC-1 Phase 2 staged until distributed_build consumes broadcast decisions.
#[derive(Clone, Copy, Debug)]
pub(crate) struct BroadcastDecision {
    pub feasible: bool,
    pub forced: bool,
    pub build_bytes: f64,
    pub hash_table_bytes: f64,
    pub effective_backend_count: f64,
    pub risk_adj_fanout_bytes: f64,
    pub per_node_budget_bytes: f64,
    pub cluster_network_budget_bytes: f64,
    pub risk_multiplier: f64,
    pub reject_reason: Option<BroadcastRejectReason>,
}

/// Produce the broadcast decision for EXPLAIN. Only fires for a hash join with
/// a build child under the BroadcastJoin alternative (or Default+Broadcast).
#[allow(dead_code)] // BC-1 Phase 2 staged until distributed_build consumes broadcast decisions.
pub(crate) fn broadcast_decision(input: &CostInput<'_>) -> Option<BroadcastDecision> {
    let join = match input.op {
        Operator::PhysicalHashJoin(join) => join,
        _ => return None,
    };
    let is_broadcast = match input.alt_kind {
        PropertyAlternativeKind::BroadcastJoin => true,
        PropertyAlternativeKind::ShuffleJoin | PropertyAlternativeKind::SingletonJoin => false,
        PropertyAlternativeKind::Default => {
            matches!(join.distribution, JoinDistribution::Broadcast)
        }
    };
    if !is_broadcast {
        return None;
    }

    let build_stats = input
        .child_stats
        .get(join.build_side.input_ordinal())
        .copied()?;
    let probe_stats = input
        .child_stats
        .get(join.build_side.probe_ordinal())
        .copied()
        .unwrap_or(build_stats);
    let feas = broadcast_is_feasible(probe_stats, build_stats, input.options);
    let forced = broadcast_decision_is_forced(join, input);

    Some(BroadcastDecision {
        feasible: feas.feasible,
        forced,
        build_bytes: feas.build_bytes,
        hash_table_bytes: feas.hash_table_bytes,
        effective_backend_count: feas.effective_backend_count,
        risk_adj_fanout_bytes: feas.risk_adj_fanout_bytes,
        per_node_budget_bytes: input.options.profile.per_node_build_memory_budget_bytes,
        cluster_network_budget_bytes: input.options.profile.cluster_broadcast_network_budget_bytes,
        risk_multiplier: feas.risk_multiplier,
        reject_reason: feas.reject_reason,
    })
}

fn broadcast_decision_is_forced(join: &PhysicalHashJoinOp, input: &CostInput<'_>) -> bool {
    if matches!(
        join.join_type,
        JoinKind::NullAwareLeftAnti | JoinKind::LeftMark { .. }
    ) {
        return true;
    }
    let Some(scalars) = input.scalars else {
        return false;
    };
    let mut unresolved = join.clone();
    unresolved.distribution = JoinDistribution::Unknown;
    let unresolved_op = Operator::PhysicalHashJoin(unresolved);
    let alternatives = super::derive::derive_required_alternatives(
        &unresolved_op,
        scalars,
        input.required_output,
        input.child_stats.len(),
    );
    feasibility_is_advisory_only(&unresolved_op, &alternatives)
}

/// True when broadcast feasibility is advisory only for this operator: broadcast
/// is the only correct distribution, so an infeasible verdict must not prune
/// the alternative.
#[allow(dead_code)] // BC-1 Phase 1 staged until search.rs consumes advisory feasibility.
pub(crate) fn feasibility_is_advisory_only(
    op: &Operator,
    alternatives: &[super::derive::ChildRequirementAlternative],
) -> bool {
    match op {
        Operator::PhysicalHashJoin(join) => {
            join.join_type == JoinKind::NullAwareLeftAnti
                || (!alternatives.is_empty()
                    && alternatives
                        .iter()
                        .all(|alt| alt.kind == PropertyAlternativeKind::BroadcastJoin))
        }
        _ => false,
    }
}

fn scan_cost_size(scan: &ScanOp, stats: &Statistics) -> f64 {
    let Some(required_columns) = scan.required_columns.as_ref() else {
        return safe_compute_size(stats);
    };
    finite_non_negative_cost(stats.compute_size_for_columns(required_columns))
}

fn scan_required_column_names(scan: &ScanOp) -> Vec<String> {
    let column_ids = scan.required_columns.as_deref();
    let mut out = Vec::new();
    for column in scan
        .columns
        .iter()
        .filter(|column| column_ids.is_none_or(|required| required.contains(&column.column_id)))
    {
        let lower = column.name.to_ascii_lowercase();
        if !out.iter().any(|existing| existing == &lower) {
            out.push(lower);
        }
    }
    out
}

fn table_scan_row_count(table_stats: &TableStatistics) -> f64 {
    let rows = table_stats.row_count as f64;
    if rows.is_finite() && rows > 0.0 {
        finite_non_negative_cost(rows)
    } else {
        1.0
    }
}

fn table_column_width(table_stats: &TableStatistics, column_name: &str) -> f64 {
    table_stats
        .column_stats
        .get(&column_name.to_ascii_lowercase())
        .map(|stat| stat.average_row_size)
        .filter(|width| width.is_finite() && *width > 0.0)
        .map(finite_non_negative_cost)
        .unwrap_or(DEFAULT_ROW_WIDTH)
}

fn scan_bound_table_statistics(
    scan: &ScanOp,
    stats_input: Option<&OptimizerStatsInput>,
) -> Option<TableStatistics> {
    let stats_ref = scan.stats_ref?;
    stats_input?
        .query_stats()
        .get(stats_ref)
        .and_then(TableStatistics::try_from_base_stats_with_confidence)
        .map(|(stats, _)| stats)
}

fn scan_input_cost_size(scan: &ScanOp, input: &CostInput<'_>) -> f64 {
    let Some(table_stats) = scan_bound_table_statistics(scan, input.stats_input) else {
        return scan_cost_size(scan, input.own_stats);
    };
    let required_columns = scan_required_column_names(scan);
    if required_columns.is_empty() {
        return scan_cost_size(scan, input.own_stats);
    }
    let row_count = table_scan_row_count(&table_stats);
    let row_width = required_columns.iter().fold(0.0, |acc, column| {
        finite_non_negative_cost(acc + table_column_width(&table_stats, column))
    });
    finite_non_negative_cost(row_count * row_width)
}

fn scan_input_row_count(scan: &ScanOp, input: &CostInput<'_>) -> f64 {
    scan_bound_table_statistics(scan, input.stats_input)
        .map(|stats| table_scan_row_count(&stats))
        .unwrap_or_else(|| cost_row_count(input.own_stats))
}

fn estimate_scan_cost_estimate(input: &CostInput<'_>, scan: &ScanOp) -> CostEstimate {
    let payload_cost = scan_input_cost_size(scan, input);
    let predicate_cost = if scan.predicates.is_empty() {
        0.0
    } else {
        finite_non_negative_cost(
            scan_input_row_count(scan, input)
                * scalar_list_complexity(input.scalars, &scan.predicates)
                * input.options.predicate_cost_factor,
        )
    };
    CostEstimate {
        cpu_cost: finite_non_negative_cost(payload_cost + predicate_cost),
        memory_cost: 0.0,
        network_cost: 0.0,
    }
}

fn stats_has_positive_overflow_signal(stats: &Statistics) -> bool {
    safe_compute_size(stats) >= MAX_FINITE_COST
}

fn sanitize_legacy_fallback_cost(
    legacy_cost: TotalCost,
    own_stats: &Statistics,
    child_stats: &[&Statistics],
) -> TotalCost {
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
    pub(crate) fn total_with_options(&self, options: &CostOptions) -> TotalCost {
        self.weighted_total(
            options.cpu_weight,
            options.memory_weight,
            options.network_weight,
        )
    }
}

fn scalar_complexity(arena: Option<&ScalarArena>, expr: ScalarId) -> f64 {
    let Some(arena) = arena else {
        return 1.0;
    };
    match arena.node(expr) {
        ScalarNode::ColumnRef(_)
        | ScalarNode::LambdaParamRef { .. }
        | ScalarNode::Literal(_)
        | ScalarNode::Constant(_) => 0.1,
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

fn is_passthrough_project(
    arena: Option<&ScalarArena>,
    project: &super::operator::ProjectOp,
) -> bool {
    let Some(arena) = arena else {
        return false;
    };
    project
        .items
        .iter()
        .all(|item| matches!(arena.node(item.expr), ScalarNode::ColumnRef(_)))
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
    let probe_stats = input
        .child_stats
        .get(join.build_side.probe_ordinal())
        .copied();
    let build_stats = input
        .child_stats
        .get(join.build_side.input_ordinal())
        .copied();
    let probe_rows = probe_stats.map(cost_row_count).unwrap_or(1.0);
    let build_rows = build_stats.map(cost_row_count).unwrap_or(1.0);
    let probe_size = probe_stats.map(safe_compute_size).unwrap_or(0.0);
    let build_size = build_stats.map(safe_compute_size).unwrap_or(0.0);
    let output_size = safe_compute_size(input.own_stats);
    let key_factor = (join.condition.keys().len() as f64).max(1.0);

    let is_broadcast = match input.alt_kind {
        PropertyAlternativeKind::BroadcastJoin => true,
        PropertyAlternativeKind::ShuffleJoin | PropertyAlternativeKind::SingletonJoin => false,
        PropertyAlternativeKind::Default => match join.distribution {
            JoinDistribution::Broadcast => true,
            JoinDistribution::Shuffle
            | JoinDistribution::Colocate
            | JoinDistribution::Singleton => false,
            JoinDistribution::Unknown => {
                panic!("unknown join distribution should be resolved before costing")
            }
        },
    };
    let is_shuffle = match input.alt_kind {
        PropertyAlternativeKind::ShuffleJoin => true,
        PropertyAlternativeKind::BroadcastJoin | PropertyAlternativeKind::SingletonJoin => false,
        PropertyAlternativeKind::Default => match join.distribution {
            JoinDistribution::Shuffle => true,
            JoinDistribution::Broadcast
            | JoinDistribution::Colocate
            | JoinDistribution::Singleton => false,
            JoinDistribution::Unknown => {
                panic!("unknown join distribution should be resolved before costing")
            }
        },
    };

    let mut cpu_cost = finite_non_negative_cost(
        (probe_rows + build_rows) * key_factor * input.options.hash_cost_factor + output_size,
    );
    let backends =
        normalized_effective_backend_count(input.options.profile.effective_backend_count);
    let build_hash =
        estimated_build_hash_table_bytes(build_stats.unwrap_or(input.own_stats), input.options);
    let fanout = (backends - 1.0).max(0.0);
    let mut memory_cost = if is_broadcast {
        finite_non_negative_cost(build_hash * backends)
    } else if is_shuffle {
        finite_non_negative_cost(build_hash / backends)
    } else {
        build_hash
    };
    let network_cost = if is_broadcast {
        finite_non_negative_cost(build_size * fanout)
    } else if is_shuffle {
        if backends <= 1.0
            || (child_output_is_hash_partitioned(input.child_outputs.first())
                && child_output_is_hash_partitioned(input.child_outputs.get(1)))
        {
            0.0
        } else {
            finite_non_negative_cost(probe_size + build_size)
        }
    } else {
        0.0
    };

    if join.join_type == JoinKind::Cross {
        cpu_cost = finite_non_negative_cost(cpu_cost * CROSS_JOIN_COST_PENALTY);
        memory_cost = finite_non_negative_cost(memory_cost * CROSS_JOIN_COST_PENALTY);
    }
    if join.condition.residual().is_some() {
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
        Operator::PhysicalScan(scan) => estimate_scan_cost_estimate(input, scan),
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
            let memory_cost = if is_passthrough_project(input.scalars, project) {
                0.0
            } else {
                safe_compute_size(input.own_stats) * 0.02
            };
            CostEstimate {
                cpu_cost: finite_non_negative_cost(
                    input_rows
                        * scalar_list_complexity(input.scalars, &exprs)
                        * input.options.projection_cost_factor,
                ),
                memory_cost,
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
            let cpu_weight =
                if input.options.cpu_weight.is_finite() && input.options.cpu_weight > 0.0 {
                    input.options.cpu_weight.min(MAX_FINITE_COST)
                } else {
                    0.0
                };
            let legacy_cost =
                sanitize_legacy_fallback_cost(legacy_cost, input.own_stats, input.child_stats);
            let cpu_cost = if cpu_weight > 0.0 {
                finite_non_negative_cost(legacy_cost / cpu_weight)
            } else {
                0.0
            };
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

#[allow(
    dead_code,
    reason = "Retained for staged SQL planner migration consumers and test helpers."
)]
pub(crate) fn compute_cost_from_input(input: &CostInput<'_>) -> TotalCost {
    compute_cost_estimate(input).total_with_options(input.options)
}

fn compute_legacy_cost_with_properties(
    op: &Operator,
    own_stats: &Statistics,
    child_stats: &[&Statistics],
    child_outputs: &[&PhysicalPropertySet],
    alt_kind: &PropertyAlternativeKind,
    options: &CostOptions,
) -> TotalCost {
    match op {
        Operator::PhysicalHashJoin(join) => {
            let required_output = PhysicalPropertySet::any();
            let input = CostInput {
                op,
                own_stats,
                child_stats,
                child_outputs,
                required_output: &required_output,
                alt_kind,
                scalars: None,
                stats_input: None,
                options,
            };
            estimate_hash_join_cost(&input, join).total_with_options(options)
        }
        _ => compute_cost(op, own_stats, child_stats),
    }
}

#[allow(
    dead_code,
    reason = "Retained for staged SQL planner migration consumers and test helpers."
)]
pub(crate) fn compute_cost_with_properties(
    op: &Operator,
    own_stats: &Statistics,
    child_stats: &[&Statistics],
    child_outputs: &[&PhysicalPropertySet],
    alt_kind: &PropertyAlternativeKind,
    options: &CostOptions,
) -> TotalCost {
    let required_output = PhysicalPropertySet::any();
    let input = CostInput {
        op,
        own_stats,
        child_stats,
        child_outputs,
        required_output: &required_output,
        alt_kind,
        scalars: None,
        stats_input: None,
        options,
    };
    compute_cost_from_input(&input)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::analysis::JoinKind;
    use crate::column_id::ColumnId;
    use crate::optimizer::operator::PhysicalHashJoinEqCondition as OptimizerPhysicalHashJoinEqCondition;
    use crate::optimizer::operator::*;
    use crate::optimizer::property::{DistributionSpec, OrderingSpec};
    use crate::optimizer::scalar::{ScalarArena, ScalarId, ScalarNode};
    use crate::optimizer::statistics::{ColumnStatistic, Confidence, CostEstimate};

    use crate::planner::optimizer_bridge::scalar::intern_typed;

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
                ..ColumnStatistic::for_test_with_ndv(rows, Confidence::Exact)
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
                    ..ColumnStatistic::for_test_with_ndv(rows, Confidence::Exact)
                },
            );
        }
        Statistics {
            output_row_count: rows,
            column_statistics: col,
            ..Default::default()
        }
    }

    fn output_column(id: u32, name: &str) -> crate::analysis::OutputColumn {
        crate::analysis::OutputColumn {
            column_id: ColumnId::new_for_test(id),
            name: name.to_string(),
            value_type: novarocks_type_contract::FunctionValueType::new(
                arrow::datatypes::DataType::Int64,
                false,
            ),

            is_internal: false,
        }
    }

    fn two_column_scan_op(required_columns: Option<Vec<&str>>) -> Operator {
        let columns = vec![output_column(1, "narrow"), output_column(2, "wide")];
        let required_columns = required_columns.map(|required| {
            required
                .into_iter()
                .map(|name| {
                    columns
                        .iter()
                        .find(|column| column.name == name)
                        .expect("test scan column")
                        .column_id
                })
                .collect()
        });
        Operator::PhysicalScan(ScanOp {
            database: String::new(),
            table: crate::planner::table::TableDef {
                name: "t".into(),
                columns: vec![],
                iceberg_row_lineage_metadata_columns: vec![],
                source: crate::compiler::mv_rewrite::test_scan_source(
                    crate::planner::table::SqlScanKind::ConnectorRead,
                ),
            },
            alias: None,
            stats_ref: None,
            columns,
            predicates: vec![],
            required_columns,
            variant_columns: vec![],
            mv_rewritten_from: None,
        })
    }

    fn scan_op() -> Operator {
        Operator::PhysicalScan(ScanOp {
            database: String::new(),
            table: crate::planner::table::TableDef {
                name: "t".into(),
                columns: vec![],
                iceberg_row_lineage_metadata_columns: vec![],
                source: crate::compiler::mv_rewrite::test_scan_source(
                    crate::planner::table::SqlScanKind::ConnectorRead,
                ),
            },
            alias: None,
            stats_ref: None,
            columns: vec![],
            predicates: vec![],
            required_columns: None,
            variant_columns: vec![],
            mv_rewritten_from: None,
        })
    }

    fn assert_finite_non_negative_dimensions(estimate: &CostEstimate) {
        assert!(estimate.cpu_cost.is_finite() && estimate.cpu_cost >= 0.0);
        assert!(estimate.memory_cost.is_finite() && estimate.memory_cost >= 0.0);
        assert!(estimate.network_cost.is_finite() && estimate.network_cost >= 0.0);
    }

    #[test]
    fn passthrough_project_has_no_materialization_memory_cost() {
        let mut scalars = ScalarArena::new();
        let exprs: Vec<_> = [1, 2, 3]
            .into_iter()
            .map(|id| {
                scalars.intern(
                    ScalarNode::ColumnRef(ColumnId::new_for_test(id)),
                    novarocks_type_contract::FunctionValueType::new(
                        arrow::datatypes::DataType::Int64,
                        false,
                    ),
                )
            })
            .collect();
        let op = Operator::PhysicalProject(ProjectOp {
            items: exprs
                .iter()
                .enumerate()
                .map(|(idx, expr)| ScalarProjectItem {
                    expr: *expr,
                    output_column_id: ColumnId::new_for_test(idx as u32 + 101),
                    output_name: format!("c{idx}"),
                    expr_display: None,
                })
                .collect(),
            output_qualifier: None,
        });
        let own = stats_with_column_widths(1_680.0, &[1024.0, 1024.0, 1024.0]);
        let child = stats_with_column_widths(1_680.0, &[1024.0, 1024.0, 1024.0]);
        let child_stats = [&child];
        let required = PhysicalPropertySet::any();
        let child_outputs = [PhysicalPropertySet::any()];
        let child_output_refs = [&child_outputs[0]];
        let alt_kind = PropertyAlternativeKind::Default;
        let options = CostOptions::default();
        let input = CostInput {
            op: &op,
            own_stats: &own,
            child_stats: &child_stats,
            child_outputs: &child_output_refs,
            required_output: &required,
            alt_kind: &alt_kind,
            scalars: Some(&scalars),
            stats_input: None,
            options: &options,
        };

        let estimate = compute_cost_estimate(&input);

        assert_eq!(estimate.memory_cost, 0.0);
        assert!(estimate.cpu_cost > 0.0);
    }

    fn test_eq_condition(
        arena: &mut ScalarArena,
        left_value: i64,
        right_value: i64,
    ) -> OptimizerPhysicalHashJoinEqCondition {
        let left = intern_typed(
            arena,
            &crate::analysis::TypedExpr {
                kind: crate::analysis::ExprKind::Literal(crate::analysis::LiteralValue::Int(
                    left_value,
                )),
                value_type: novarocks_type_contract::FunctionValueType::new(
                    arrow::datatypes::DataType::Int64,
                    false,
                ),
            },
            crate::optimizer::test_optimizer_control(),
        )
        .unwrap();
        let right = intern_typed(
            arena,
            &crate::analysis::TypedExpr {
                kind: crate::analysis::ExprKind::Literal(crate::analysis::LiteralValue::Int(
                    right_value,
                )),
                value_type: novarocks_type_contract::FunctionValueType::new(
                    arrow::datatypes::DataType::Int64,
                    false,
                ),
            },
            crate::optimizer::test_optimizer_control(),
        )
        .unwrap();
        OptimizerPhysicalHashJoinEqCondition {
            left,
            right,
            null_safe: false,
        }
    }

    fn column_ref(arena: &mut ScalarArena, id: u32) -> ScalarId {
        arena.intern(
            ScalarNode::ColumnRef(ColumnId::new_for_test(id)),
            novarocks_type_contract::FunctionValueType::new(
                arrow::datatypes::DataType::Int64,
                false,
            ),
        )
    }

    fn nested_column_ref(arena: &mut ScalarArena, id: u32) -> ScalarId {
        let child = column_ref(arena, id);
        arena.intern(
            ScalarNode::Nested(child),
            novarocks_type_contract::FunctionValueType::new(
                arrow::datatypes::DataType::Int64,
                false,
            ),
        )
    }

    fn column_eq_condition(
        arena: &mut ScalarArena,
        left_id: u32,
        right_id: u32,
    ) -> OptimizerPhysicalHashJoinEqCondition {
        OptimizerPhysicalHashJoinEqCondition {
            left: column_ref(arena, left_id),
            right: column_ref(arena, right_id),
            null_safe: false,
        }
    }

    fn expression_key_eq_condition(
        arena: &mut ScalarArena,
        left_id: u32,
        right_id: u32,
    ) -> OptimizerPhysicalHashJoinEqCondition {
        OptimizerPhysicalHashJoinEqCondition {
            left: nested_column_ref(arena, left_id),
            right: column_ref(arena, right_id),
            null_safe: false,
        }
    }

    fn join_op(
        kind: JoinKind,
        eq: Vec<crate::optimizer::operator::PhysicalHashJoinEqCondition>,
    ) -> Operator {
        let build_side = match kind {
            JoinKind::Cross => crate::optimizer::operator::HashJoinBuildSide::Right,
            _ => crate::optimizer::operator::exact_hash_join_build_side(kind)
                .expect("cost fixture uses a legal hash join kind"),
        };
        Operator::PhysicalHashJoin(PhysicalHashJoinOp {
            join_type: kind,
            build_side,
            distribution: JoinDistribution::Unknown,
            condition: novarocks_physical_plan::HashJoinCondition::Ordinary {
                keys: eq,
                residual: None,
            },
        })
    }

    fn derived_alternatives(
        op: &Operator,
        arena: &ScalarArena,
    ) -> Vec<super::super::derive::ChildRequirementAlternative> {
        super::super::derive::derive_required_alternatives(
            op,
            arena,
            &PhysicalPropertySet::any(),
            2,
        )
    }

    fn advisory_for_derived_hash_join(op: &Operator, arena: &ScalarArena) -> bool {
        let alternatives = derived_alternatives(op, arena);
        feasibility_is_advisory_only(op, &alternatives)
    }

    fn broadcast_input<'a>(
        op: &'a Operator,
        own: &'a Statistics,
        child: &'a [&'a Statistics],
        outs: &'a [&'a PhysicalPropertySet],
        required: &'a PhysicalPropertySet,
        alt: &'a PropertyAlternativeKind,
        o: &'a CostOptions,
    ) -> CostInput<'a> {
        CostInput {
            op,
            own_stats: own,
            child_stats: child,
            child_outputs: outs,
            required_output: required,
            alt_kind: alt,
            scalars: None,
            stats_input: None,
            options: o,
        }
    }

    fn broadcast_input_with_scalars<'a>(
        op: &'a Operator,
        own: &'a Statistics,
        child: &'a [&'a Statistics],
        outs: &'a [&'a PhysicalPropertySet],
        required: &'a PhysicalPropertySet,
        alt: &'a PropertyAlternativeKind,
        scalars: &'a ScalarArena,
        o: &'a CostOptions,
    ) -> CostInput<'a> {
        CostInput {
            op,
            own_stats: own,
            child_stats: child,
            child_outputs: outs,
            required_output: required,
            alt_kind: alt,
            scalars: Some(scalars),
            stats_input: None,
            options: o,
        }
    }

    #[test]
    fn broadcast_decision_present_for_broadcast_hash_join_absent_otherwise() {
        let probe = stats(1_000_000.0, 64.0);
        let mut build = stats(1_000.0, 4.0);
        build.row_count_confidence = Confidence::Exact;
        let own = stats(1_000.0, 64.0);
        let mut scalars = ScalarArena::new();
        let eq = vec![column_eq_condition(&mut scalars, 1, 2)];
        let op = join_op(JoinKind::Inner, eq.clone());
        let o = CostOptions::default();
        let required = PhysicalPropertySet::any();
        let outs = [PhysicalPropertySet::any(), PhysicalPropertySet::broadcast()];
        let out_refs = [&outs[0], &outs[1]];
        let child = [&probe, &build];
        let broadcast = PropertyAlternativeKind::BroadcastJoin;
        let input = broadcast_input_with_scalars(
            &op, &own, &child, &out_refs, &required, &broadcast, &scalars, &o,
        );

        let decision = broadcast_decision(&input).expect("broadcast decision");
        assert!(decision.feasible);
        assert_eq!(decision.effective_backend_count, 3.0);
        assert!(!decision.forced);

        let shuffle = PropertyAlternativeKind::ShuffleJoin;
        let shuffle_input = broadcast_input(&op, &own, &child, &out_refs, &required, &shuffle, &o);
        assert!(broadcast_decision(&shuffle_input).is_none());

        let default = PropertyAlternativeKind::Default;
        let default_unknown_input =
            broadcast_input(&op, &own, &child, &out_refs, &required, &default, &o);
        assert!(broadcast_decision(&default_unknown_input).is_none());

        let broadcast_op = Operator::PhysicalHashJoin(PhysicalHashJoinOp {
            join_type: JoinKind::Inner,
            build_side: crate::optimizer::operator::HashJoinBuildSide::Right,
            distribution: JoinDistribution::Broadcast,
            condition: novarocks_physical_plan::HashJoinCondition::Ordinary {
                keys: eq.clone(),
                residual: None,
            },
        });
        let default_broadcast_input = broadcast_input_with_scalars(
            &broadcast_op,
            &own,
            &child,
            &out_refs,
            &required,
            &default,
            &scalars,
            &o,
        );
        let default_decision =
            broadcast_decision(&default_broadcast_input).expect("default broadcast decision");
        assert!(!default_decision.forced);

        let no_build_child = [&probe];
        let missing_build_input = broadcast_input(
            &op,
            &own,
            &no_build_child,
            &out_refs,
            &required,
            &broadcast,
            &o,
        );
        assert!(broadcast_decision(&missing_build_input).is_none());

        let scan = scan_op();
        let scan_input = broadcast_input(&scan, &own, &child, &out_refs, &required, &broadcast, &o);
        assert!(broadcast_decision(&scan_input).is_none());
    }

    #[test]
    fn broadcast_decision_marks_correctness_required_broadcast_forced() {
        let probe = stats(1_000_000.0, 64.0);
        let mut build = stats(1_000.0, 4.0);
        build.row_count_confidence = Confidence::Exact;
        let own = stats(1_000.0, 64.0);
        let o = CostOptions::default();
        let required = PhysicalPropertySet::any();
        let outs = [PhysicalPropertySet::any(), PhysicalPropertySet::broadcast()];
        let out_refs = [&outs[0], &outs[1]];
        let child = [&probe, &build];
        let broadcast = PropertyAlternativeKind::BroadcastJoin;

        let scalars = ScalarArena::new();
        for op in [
            join_op(JoinKind::Inner, vec![]),
            join_op(JoinKind::Cross, vec![]),
            join_op(JoinKind::NullAwareLeftAnti, vec![]),
        ] {
            let input = broadcast_input_with_scalars(
                &op, &own, &child, &out_refs, &required, &broadcast, &scalars, &o,
            );
            assert!(broadcast_decision(&input).expect("decision").forced);
        }

        let mut unsupported_scalars = ScalarArena::new();
        let unsupported_key = vec![expression_key_eq_condition(&mut unsupported_scalars, 1, 2)];
        let unsupported_op = join_op(JoinKind::Inner, unsupported_key);
        let input = broadcast_input_with_scalars(
            &unsupported_op,
            &own,
            &child,
            &out_refs,
            &required,
            &broadcast,
            &unsupported_scalars,
            &o,
        );
        assert!(!broadcast_decision(&input).expect("decision").forced);
    }

    #[test]
    #[expect(
        clippy::field_reassign_with_default,
        reason = "The fixture assigns optimizer facts progressively so each test input remains explicit."
    )]
    fn broadcast_decision_keeps_cross_join_forced_when_infeasible() {
        let probe = stats(1_000.0, 64.0);
        let mut build = stats(1_000_000.0, 2048.0);
        build.row_count_confidence = Confidence::Exact;
        let own = stats(1_000_000_000.0, 2112.0);

        let mut options = CostOptions::default();
        let mut profile = ClusterResourceProfile::default();
        profile.effective_backend_count = 3.0;
        profile.per_node_build_memory_budget_bytes = 256.0 * 1024.0 * 1024.0;
        options.apply_profile(profile);

        let required = PhysicalPropertySet::any();
        let outs = [PhysicalPropertySet::any(), PhysicalPropertySet::broadcast()];
        let out_refs = [&outs[0], &outs[1]];
        let child = [&probe, &build];
        let broadcast = PropertyAlternativeKind::BroadcastJoin;
        let scalars = ScalarArena::new();
        let op = join_op(JoinKind::Cross, vec![]);
        let input = broadcast_input_with_scalars(
            &op, &own, &child, &out_refs, &required, &broadcast, &scalars, &options,
        );

        let decision = broadcast_decision(&input).expect("decision");
        assert!(!decision.feasible);
        assert!(decision.forced);
        assert_eq!(
            decision.reject_reason,
            Some(BroadcastRejectReason::PerNodeMemory)
        );
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
    fn estimated_build_hash_table_bytes_inflates_narrow_more_than_wide() {
        let o = CostOptions::default();
        // Narrow: 8B/row, 10M rows => payload 80MB; ht = 80MB/0.75 + 16*10M.
        let narrow = stats(10_000_000.0, 8.0);
        let narrow_ht = estimated_build_hash_table_bytes(&narrow, &o);
        let narrow_payload = safe_compute_size(&narrow);
        let expected_narrow = narrow_payload / 0.75 + 16.0 * 10_000_000.0;
        assert!((narrow_ht - expected_narrow).abs() < 1.0);
        // Index overhead must roughly double the narrow build's footprint.
        assert!(narrow_ht > 2.0 * narrow_payload);

        // Wide: 200B/row, 1M rows => payload 200MB; index ~6% only.
        let wide = stats(1_000_000.0, 200.0);
        let wide_ht = estimated_build_hash_table_bytes(&wide, &o);
        let wide_payload = safe_compute_size(&wide);
        assert!(wide_ht < 1.5 * wide_payload);
    }

    #[test]
    #[expect(
        clippy::field_reassign_with_default,
        reason = "The fixture assigns optimizer facts progressively so each test input remains explicit."
    )]
    fn estimated_build_hash_table_bytes_sanitizes_options() {
        let build = stats(100.0, 8.0);
        let payload = safe_compute_size(&build);
        let rows = cost_row_count(&build);

        let mut nan_load_factor = CostOptions::default();
        nan_load_factor.hash_table_load_factor = f64::NAN;
        let nan_bytes = estimated_build_hash_table_bytes(&build, &nan_load_factor);
        let expected_default =
            payload / 0.75 + rows * nan_load_factor.hash_table_per_row_overhead_bytes;
        assert!(nan_bytes.is_finite());
        assert!(nan_bytes > 0.0);
        assert!((nan_bytes - expected_default).abs() < 1e-9);

        for bad_load_factor in [-1.0, 0.0] {
            let mut options = CostOptions::default();
            options.hash_table_load_factor = bad_load_factor;
            let actual = estimated_build_hash_table_bytes(&build, &options);
            let expected = payload / 0.5 + rows * options.hash_table_per_row_overhead_bytes;
            assert!((actual - expected).abs() < 1e-9);
        }

        let mut oversized_load_factor = CostOptions::default();
        oversized_load_factor.hash_table_load_factor = 1.5;
        let oversized_actual = estimated_build_hash_table_bytes(&build, &oversized_load_factor);
        let oversized_expected =
            payload / 1.0 + rows * oversized_load_factor.hash_table_per_row_overhead_bytes;
        assert!((oversized_actual - oversized_expected).abs() < 1e-9);

        let mut negative_overhead = CostOptions::default();
        negative_overhead.hash_table_per_row_overhead_bytes = -16.0;
        let negative_overhead_actual = estimated_build_hash_table_bytes(&build, &negative_overhead);
        let negative_overhead_expected = payload / negative_overhead.hash_table_load_factor;
        assert!((negative_overhead_actual - negative_overhead_expected).abs() < 1e-9);
    }

    #[test]
    fn confidence_risk_multiplier_is_monotone_and_anchored() {
        let o = CostOptions::default();
        assert_eq!(confidence_risk_multiplier(Confidence::Fallback, &o), 4.0);
        assert_eq!(confidence_risk_multiplier(Confidence::Estimated, &o), 2.0);
        assert_eq!(confidence_risk_multiplier(Confidence::Exact, &o), 1.0);
        // Measured is inert until a producer lands: must equal Exact.
        assert_eq!(confidence_risk_multiplier(Confidence::Measured, &o), 1.0);
        assert!(
            confidence_risk_multiplier(Confidence::Fallback, &o)
                >= confidence_risk_multiplier(Confidence::Estimated, &o)
        );
        assert!(
            confidence_risk_multiplier(Confidence::Estimated, &o)
                >= confidence_risk_multiplier(Confidence::Exact, &o)
        );
    }

    #[test]
    #[expect(
        clippy::field_reassign_with_default,
        reason = "The fixture assigns optimizer facts progressively so each test input remains explicit."
    )]
    fn confidence_risk_multiplier_invalid_values_fall_back_to_neutral() {
        let mut o = CostOptions::default();

        o.risk_multiplier_estimated = f64::NAN;
        assert_eq!(confidence_risk_multiplier(Confidence::Estimated, &o), 1.0);

        o.risk_multiplier_estimated = -2.0;
        assert_eq!(confidence_risk_multiplier(Confidence::Estimated, &o), 1.0);

        o.risk_multiplier_estimated = 3.5;
        assert_eq!(confidence_risk_multiplier(Confidence::Estimated, &o), 3.5);
    }

    #[test]
    fn uninformative_fingerprint_catches_fabricated_defaults_but_allows_small_values() {
        // NovaRocks fabricated-default fingerprint: Fallback + empty column_statistics,
        // finite positive rows (e.g. CTE/scan fallback 10_000 rows). MUST refuse.
        let fabricated = Statistics {
            output_row_count: 10_000.0,
            row_count_confidence: Confidence::Fallback,
            column_statistics: HashMap::new(),
        };
        assert!(build_size_is_uninformative(&fabricated));

        // Small Fallback VALUES with real literal column widths: NOT uninformative.
        let mut small_values = stats(1_000.0, 16.0); // stats() populates column_statistics
        small_values.row_count_confidence = Confidence::Fallback;
        assert!(!build_size_is_uninformative(&small_values));
    }

    #[test]
    fn uninformative_fingerprint_catches_magnitude_unknown_with_real_colstats() {
        let mut populated = stats(10.0, 16.0);
        populated.row_count_confidence = Confidence::Exact;

        // Magnitude truly unknown (defensive). MUST refuse independent of the
        // fabricated-default fingerprint.
        populated.output_row_count = f64::INFINITY;
        assert!(build_size_is_uninformative(&populated));
        populated.output_row_count = f64::NAN;
        assert!(build_size_is_uninformative(&populated));
        populated.output_row_count = 0.0;
        assert!(build_size_is_uninformative(&populated));
        populated.output_row_count = -1.0;
        assert!(build_size_is_uninformative(&populated));

        // Finite size overflow/saturation is uninformative even with real colstats.
        let mut overflow = stats_with_column_widths(10.0, &[f64::MAX, f64::MAX]);
        overflow.row_count_confidence = Confidence::Exact;
        assert!(build_size_is_uninformative(&overflow));
    }

    #[test]
    #[expect(
        clippy::field_reassign_with_default,
        reason = "The fixture assigns optimizer facts progressively so each test input remains explicit."
    )]
    fn invalid_risk_multiplier_never_makes_infeasible_build_feasible() {
        for invalid_multiplier in [f64::NAN, -2.0] {
            let mut o = CostOptions::default();
            o.risk_multiplier_estimated = invalid_multiplier;

            let mut build = stats(1.0, 900.0 * 1024.0 * 1024.0);
            build.row_count_confidence = Confidence::Estimated;
            let probe = stats(100_000.0, 64.0);

            let feas = broadcast_is_feasible(&probe, &build, &o);

            assert_eq!(feas.risk_multiplier, 1.0);
            assert!(!feas.feasible);
            assert_eq!(
                feas.reject_reason,
                Some(BroadcastRejectReason::PerNodeMemory)
            );
        }
    }

    #[test]
    #[expect(
        clippy::field_reassign_with_default,
        reason = "The fixture assigns optimizer facts progressively so each test input remains explicit."
    )]
    fn layer1_per_node_floor_never_divides_by_backend_count() {
        let mut o = CostOptions::default();
        let mut profile = ClusterResourceProfile::default();
        profile.effective_backend_count = 30.0;
        profile.per_node_build_memory_budget_bytes = 1024.0 * 1024.0 * 1024.0;
        o.apply_profile(profile);

        let mut build = stats(3_000_000.0, 2048.0);
        build.row_count_confidence = Confidence::Estimated;
        let probe = stats(100_000.0, 64.0);

        let feas = broadcast_is_feasible(&probe, &build, &o);

        assert!(!feas.feasible);
        assert_eq!(
            feas.reject_reason,
            Some(BroadcastRejectReason::PerNodeMemory)
        );
    }

    #[test]
    #[expect(
        clippy::field_reassign_with_default,
        reason = "The fixture assigns optimizer facts progressively so each test input remains explicit."
    )]
    fn cluster_network_floor_can_reject_when_per_node_memory_fits() {
        let mut o = CostOptions::default();
        let mut profile = ClusterResourceProfile::default();
        profile.effective_backend_count = 10.0;
        profile.per_node_build_memory_budget_bytes = 1024.0 * 1024.0 * 1024.0;
        o.apply_profile(profile);

        let mut build = stats(1_000_000.0, 200.0);
        build.row_count_confidence = Confidence::Exact;
        let probe = stats(100_000.0, 64.0);

        let feas = broadcast_is_feasible(&probe, &build, &o);

        assert!(
            feas.risk_adj_build_bytes <= o.profile.per_node_build_memory_budget_bytes,
            "test shape must fit per-node memory: {:?}",
            feas
        );
        assert!(
            feas.risk_adj_fanout_bytes > o.profile.cluster_broadcast_network_budget_bytes,
            "test shape must exceed cluster network budget: {:?}",
            feas
        );
        assert!(!feas.feasible);
        assert_eq!(
            feas.reject_reason,
            Some(BroadcastRejectReason::ClusterNetwork)
        );
    }

    #[test]
    fn degenerate_build_is_infeasible() {
        let o = CostOptions::default();
        let build = Statistics {
            output_row_count: 10_000.0,
            row_count_confidence: Confidence::Fallback,
            column_statistics: HashMap::new(),
        };
        let probe = stats(5_000_000.0, 100.0);

        let feas = broadcast_is_feasible(&probe, &build, &o);

        assert!(!feas.feasible);
        assert_eq!(
            feas.reject_reason,
            Some(BroadcastRejectReason::UninformativeSize)
        );
    }

    #[test]
    #[expect(
        clippy::field_reassign_with_default,
        reason = "The fixture assigns optimizer facts progressively so each test input remains explicit."
    )]
    fn small_exact_build_is_feasible() {
        let mut o = CostOptions::default();
        let mut profile = ClusterResourceProfile::default();
        profile.effective_backend_count = 3.0;
        profile.per_node_build_memory_budget_bytes = 256.0 * 1024.0 * 1024.0;
        o.apply_profile(profile);

        let mut build = stats(1_000.0, 4.0);
        build.row_count_confidence = Confidence::Exact;
        let probe = stats(1_000_000.0, 64.0);

        let feas = broadcast_is_feasible(&probe, &build, &o);

        assert!(feas.feasible);
        assert_eq!(feas.reject_reason, None);
    }

    #[test]
    #[expect(
        clippy::field_reassign_with_default,
        reason = "The fixture assigns optimizer facts progressively so each test input remains explicit."
    )]
    fn huge_exact_big_memory_allows_wide_but_rejects_extremely_narrow_build() {
        let mut o = CostOptions::default();
        let mut profile = ClusterResourceProfile::default();
        profile.effective_backend_count = 3.0;
        profile.per_node_build_memory_budget_bytes = 32.0 * 1024.0 * 1024.0 * 1024.0;
        o.apply_profile(profile);

        let probe = stats(10_000_000.0, 8.0);
        let mut wide = stats(10_000_000.0, 800.0);
        wide.row_count_confidence = Confidence::Exact;
        let mut narrow = stats(2_500_000_000.0, 8.0);
        narrow.row_count_confidence = Confidence::Exact;

        let wide_feas = broadcast_is_feasible(&probe, &wide, &o);
        assert!(wide_feas.feasible);
        assert_eq!(wide_feas.reject_reason, None);
        assert!(
            wide_feas.hash_table_bytes < o.profile.per_node_build_memory_budget_bytes,
            "wide build hash table should fit: {:?}",
            wide_feas
        );

        let narrow_feas = broadcast_is_feasible(&probe, &narrow, &o);
        assert!(!narrow_feas.feasible);
        assert_eq!(
            narrow_feas.reject_reason,
            Some(BroadcastRejectReason::PerNodeMemory)
        );
        assert!(
            narrow_feas.hash_table_bytes > o.profile.per_node_build_memory_budget_bytes,
            "narrow build hash table should exceed budget: {:?}",
            narrow_feas
        );
    }

    #[test]
    fn feasibility_advisory_for_correctness_required_joins() {
        let arena = ScalarArena::new();
        assert!(advisory_for_derived_hash_join(
            &join_op(JoinKind::Cross, vec![]),
            &arena
        ));
        assert!(advisory_for_derived_hash_join(
            &join_op(JoinKind::Inner, vec![]),
            &arena
        ));

        let mut naaj_arena = ScalarArena::new();
        let naaj = join_op(
            JoinKind::NullAwareLeftAnti,
            vec![test_eq_condition(&mut naaj_arena, 1, 2)],
        );
        assert!(advisory_for_derived_hash_join(&naaj, &naaj_arena));

        let mut expr_arena = ScalarArena::new();
        let unsupported_shuffle_key = join_op(
            JoinKind::Inner,
            vec![expression_key_eq_condition(&mut expr_arena, 10, 20)],
        );
        let alternatives = derived_alternatives(&unsupported_shuffle_key, &expr_arena);
        assert_eq!(alternatives.len(), 2);
        assert_eq!(alternatives[0].kind, PropertyAlternativeKind::BroadcastJoin);
        assert_eq!(alternatives[1].kind, PropertyAlternativeKind::SingletonJoin);
        assert!(!advisory_for_derived_hash_join(
            &unsupported_shuffle_key,
            &expr_arena
        ));
    }

    #[test]
    fn feasibility_not_advisory_for_ordinary_equi_inner_join() {
        let mut arena = ScalarArena::new();
        let ordinary_equi = join_op(
            JoinKind::Inner,
            vec![column_eq_condition(&mut arena, 10, 20)],
        );
        let alternatives = derived_alternatives(&ordinary_equi, &arena);
        assert!(
            alternatives
                .iter()
                .any(|alt| alt.kind == PropertyAlternativeKind::BroadcastJoin)
        );
        assert!(
            alternatives
                .iter()
                .any(|alt| alt.kind == PropertyAlternativeKind::ShuffleJoin)
        );
        assert!(!advisory_for_derived_hash_join(&ordinary_equi, &arena));

        let right_outer = join_op(JoinKind::RightOuter, vec![]);
        let right_outer_alternatives = derived_alternatives(&right_outer, &ScalarArena::new());
        assert!(right_outer_alternatives.is_empty());
        assert!(!advisory_for_derived_hash_join(
            &right_outer,
            &ScalarArena::new()
        ));

        assert!(!feasibility_is_advisory_only(&scan_op(), &[]));
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
            stats_input: None,
            options: &options,
        };

        let estimate = compute_cost_estimate(&input);
        assert_eq!(estimate.cpu_cost, s.compute_size());
        assert_eq!(estimate.memory_cost, 0.0);
        assert_eq!(estimate.network_cost, 0.0);
    }

    #[test]
    fn scan_cost_charges_scan_predicates() {
        let s = stats(1000.0, 100.0);
        let mut arena = ScalarArena::new();
        let predicate = intern_typed(
            &mut arena,
            &crate::analysis::TypedExpr {
                kind: crate::analysis::ExprKind::Literal(crate::analysis::LiteralValue::Bool(true)),
                value_type: novarocks_type_contract::FunctionValueType::new(
                    arrow::datatypes::DataType::Boolean,
                    false,
                ),
            },
            crate::optimizer::test_optimizer_control(),
        )
        .unwrap();
        let mut op = scan_op();
        let Operator::PhysicalScan(scan) = &mut op else {
            panic!("expected scan");
        };
        scan.predicates.push(predicate);

        let child_stats: [&Statistics; 0] = [];
        let child_outputs: [&PhysicalPropertySet; 0] = [];
        let required = PhysicalPropertySet::any();
        let options = CostOptions::default();
        let no_predicate_op = scan_op();
        let no_predicate_input = CostInput {
            op: &no_predicate_op,
            own_stats: &s,
            child_stats: &child_stats,
            child_outputs: &child_outputs,
            required_output: &required,
            alt_kind: &PropertyAlternativeKind::Default,
            scalars: Some(&arena),
            stats_input: None,
            options: &options,
        };
        let predicate_input = CostInput {
            op: &op,
            own_stats: &s,
            child_stats: &child_stats,
            child_outputs: &child_outputs,
            required_output: &required,
            alt_kind: &PropertyAlternativeKind::Default,
            scalars: Some(&arena),
            stats_input: None,
            options: &options,
        };

        let no_predicate_cost = compute_cost_estimate(&no_predicate_input).cpu_cost;
        let predicate_cost = compute_cost_estimate(&predicate_input).cpu_cost;

        assert!(
            predicate_cost > no_predicate_cost,
            "scan predicates must not be cost-free: predicate_cost={predicate_cost}, no_predicate_cost={no_predicate_cost}"
        );
    }

    #[test]
    fn scan_cost_uses_bound_table_rows_before_pushdown_predicates() {
        let filtered_stats = stats(10.0, 8.0);
        let mut arena = ScalarArena::new();
        let predicate = intern_typed(
            &mut arena,
            &crate::analysis::TypedExpr {
                kind: crate::analysis::ExprKind::Literal(crate::analysis::LiteralValue::Bool(true)),
                value_type: novarocks_type_contract::FunctionValueType::new(
                    arrow::datatypes::DataType::Boolean,
                    false,
                ),
            },
            crate::optimizer::test_optimizer_control(),
        )
        .unwrap();
        let mut op = two_column_scan_op(Some(vec!["narrow"]));
        let Operator::PhysicalScan(scan) = &mut op else {
            panic!("expected scan");
        };
        scan.stats_ref = Some(crate::optimizer::stats_input::StatsRef::new(3));
        scan.predicates.push(predicate);

        let mut query_stats = crate::optimizer::stats_input::QueryStatsSnapshot::empty();
        query_stats.insert(
            crate::optimizer::stats_input::StatsRef::new(3),
            "db.t",
            crate::optimizer::stats_input::BaseTableStatistics {
                row_count: crate::optimizer::stats_input::StatValue::known(
                    1000,
                    Confidence::Exact,
                    crate::optimizer::stats_input::StatsSource::TestFixture,
                ),
                columns: std::collections::HashMap::from([(
                    "narrow".to_string(),
                    crate::optimizer::stats_input::BaseColumnStatistics {
                        nulls_fraction: crate::optimizer::stats_input::StatValue::known(
                            0.0,
                            Confidence::Exact,
                            crate::optimizer::stats_input::StatsSource::TestFixture,
                        ),
                        average_row_size: crate::optimizer::stats_input::StatValue::known(
                            2.0,
                            Confidence::Exact,
                            crate::optimizer::stats_input::StatsSource::TestFixture,
                        ),
                        min_value: crate::optimizer::stats_input::StatValue::missing(
                            crate::optimizer::stats_input::StatsMissingReason::ColumnNotReported(
                                "narrow".to_string(),
                            ),
                        ),
                        max_value: crate::optimizer::stats_input::StatValue::missing(
                            crate::optimizer::stats_input::StatsMissingReason::ColumnNotReported(
                                "narrow".to_string(),
                            ),
                        ),
                        ndv: crate::optimizer::stats_input::StatValue::missing(
                            crate::optimizer::stats_input::StatsMissingReason::ColumnNotReported(
                                "narrow".to_string(),
                            ),
                        ),
                    },
                )]),
                source: crate::optimizer::stats_input::StatsSource::TestFixture,
            },
        );
        let stats_input =
            crate::optimizer::stats_input::OptimizerStatsInput::from_query_stats(&query_stats);

        let child_stats: [&Statistics; 0] = [];
        let child_outputs: [&PhysicalPropertySet; 0] = [];
        let required = PhysicalPropertySet::any();
        let options = CostOptions::default();
        let input = CostInput {
            op: &op,
            own_stats: &filtered_stats,
            child_stats: &child_stats,
            child_outputs: &child_outputs,
            required_output: &required,
            alt_kind: &PropertyAlternativeKind::Default,
            scalars: Some(&arena),
            stats_input: Some(&stats_input),
            options: &options,
        };

        let estimate = compute_cost_estimate(&input);

        assert!(
            estimate.cpu_cost >= 2000.0,
            "scan payload must use pre-predicate table rows, not filtered output rows: {estimate:?}"
        );
    }

    #[test]
    fn filter_cost_uses_input_rows_not_output_rows() {
        let mut arena = ScalarArena::new();
        let predicate = intern_typed(
            &mut arena,
            &crate::analysis::TypedExpr {
                kind: crate::analysis::ExprKind::Literal(crate::analysis::LiteralValue::Bool(true)),
                value_type: novarocks_type_contract::FunctionValueType::new(
                    arrow::datatypes::DataType::Boolean,
                    false,
                ),
            },
            crate::optimizer::test_optimizer_control(),
        )
        .unwrap();
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
            stats_input: None,
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
            stats_input: None,
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
            stats_input: None,
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
            stats_input: None,
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
            stats_input: None,
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
            stats_input: None,
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
            stats_input: None,
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
            stats_input: None,
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
            stats_input: None,
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
            stats_input: None,
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
            output_layout: AggregateOutputLayout::new(vec![], vec![]),
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
            stats_input: None,
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
            output_layout: AggregateOutputLayout::new(vec![], vec![]),
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
            stats_input: None,
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
            stats_input: None,
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
            build_side: crate::optimizer::operator::HashJoinBuildSide::Right,
            distribution: JoinDistribution::Unknown,
            condition: novarocks_physical_plan::HashJoinCondition::Ordinary {
                keys: vec![],
                residual: None,
            },
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
            stats_input: None,
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
        let legacy_cost = compute_legacy_cost_with_properties(
            &op,
            &own,
            &child_stats,
            &child_output_refs,
            &PropertyAlternativeKind::BroadcastJoin,
            &options,
        );
        assert!((input_cost - legacy_cost).abs() < f64::EPSILON);
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
            stats_input: None,
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
            build_side: crate::optimizer::operator::HashJoinBuildSide::Right,
            distribution: JoinDistribution::Unknown,
            condition: novarocks_physical_plan::HashJoinCondition::Ordinary {
                keys: vec![],
                residual: None,
            },
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
            stats_input: None,
            options: &options,
        };

        let estimate = compute_cost_estimate(&input);
        let hash_table_bytes = estimated_build_hash_table_bytes(&build, &options);
        let backends = normalized_effective_backend_count(options.profile.effective_backend_count);
        assert!(estimate.memory_cost >= hash_table_bytes * backends - f64::EPSILON);
        assert!(
            estimate.network_cost
                >= safe_compute_size(&build) * (backends - 1.0).max(0.0) - f64::EPSILON
        );
    }

    #[test]
    fn broadcast_memory_uses_hash_table_times_backends() {
        let probe = stats(1_000_000.0, 64.0);
        let build = stats(10_000.0, 32.0);
        let own = stats(100_000.0, 96.0);
        let op = Operator::PhysicalHashJoin(PhysicalHashJoinOp {
            join_type: JoinKind::Inner,
            build_side: crate::optimizer::operator::HashJoinBuildSide::Right,
            distribution: JoinDistribution::Unknown,
            condition: novarocks_physical_plan::HashJoinCondition::Ordinary {
                keys: vec![],
                residual: None,
            },
        });
        let options = CostOptions::default();
        let required = PhysicalPropertySet::any();
        let child_outputs = [PhysicalPropertySet::any(), PhysicalPropertySet::broadcast()];
        let child_output_refs = [&child_outputs[0], &child_outputs[1]];
        let child_stats = [&probe, &build];
        let input = broadcast_input(
            &op,
            &own,
            &child_stats,
            &child_output_refs,
            &required,
            &PropertyAlternativeKind::BroadcastJoin,
            &options,
        );

        let estimate = compute_cost_estimate(&input);
        let hash_table_bytes = estimated_build_hash_table_bytes(&build, &options);
        let backends = normalized_effective_backend_count(options.profile.effective_backend_count);
        let fanout = (backends - 1.0).max(0.0);
        assert!((estimate.memory_cost - hash_table_bytes * backends).abs() < f64::EPSILON);
        assert!((estimate.network_cost - safe_compute_size(&build) * fanout).abs() < f64::EPSILON);
    }

    #[test]
    fn colocate_memory_uses_build_hash_table_bytes() {
        let probe = stats(1_000_000.0, 64.0);
        let build = stats(10_000.0, 4.0);
        let own = stats(100_000.0, 68.0);
        let op = Operator::PhysicalHashJoin(PhysicalHashJoinOp {
            join_type: JoinKind::Inner,
            build_side: crate::optimizer::operator::HashJoinBuildSide::Right,
            distribution: JoinDistribution::Colocate,
            condition: novarocks_physical_plan::HashJoinCondition::Ordinary {
                keys: vec![],
                residual: None,
            },
        });
        let options = CostOptions::default();
        let required = PhysicalPropertySet::any();
        let child_outputs = [PhysicalPropertySet::any(), PhysicalPropertySet::any()];
        let child_output_refs = [&child_outputs[0], &child_outputs[1]];
        let child_stats = [&probe, &build];
        let input = broadcast_input(
            &op,
            &own,
            &child_stats,
            &child_output_refs,
            &required,
            &PropertyAlternativeKind::Default,
            &options,
        );

        let estimate = compute_cost_estimate(&input);
        let hash_table_bytes = estimated_build_hash_table_bytes(&build, &options);

        assert_eq!(estimate.memory_cost, hash_table_bytes);
        assert_eq!(estimate.network_cost, 0.0);
    }

    #[test]
    #[expect(
        clippy::field_reassign_with_default,
        reason = "The fixture assigns optimizer facts progressively so each test input remains explicit."
    )]
    fn single_node_broadcast_and_shuffle_network_are_zero() {
        let mut options = CostOptions::default();
        let mut profile = ClusterResourceProfile::default();
        profile.effective_backend_count = 1.0;
        options.apply_profile(profile);

        let probe = stats(1_000_000.0, 64.0);
        let build = stats(10_000.0, 32.0);
        let own = stats(100_000.0, 96.0);
        let op = Operator::PhysicalHashJoin(PhysicalHashJoinOp {
            join_type: JoinKind::Inner,
            build_side: crate::optimizer::operator::HashJoinBuildSide::Right,
            distribution: JoinDistribution::Unknown,
            condition: novarocks_physical_plan::HashJoinCondition::Ordinary {
                keys: vec![],
                residual: None,
            },
        });
        let required = PhysicalPropertySet::any();
        let child_outputs = [PhysicalPropertySet::any(), PhysicalPropertySet::broadcast()];
        let child_output_refs = [&child_outputs[0], &child_outputs[1]];
        let child_stats = [&probe, &build];

        let broadcast = compute_cost_estimate(&broadcast_input(
            &op,
            &own,
            &child_stats,
            &child_output_refs,
            &required,
            &PropertyAlternativeKind::BroadcastJoin,
            &options,
        ));
        let shuffle = compute_cost_estimate(&broadcast_input(
            &op,
            &own,
            &child_stats,
            &child_output_refs,
            &required,
            &PropertyAlternativeKind::ShuffleJoin,
            &options,
        ));

        assert_eq!(broadcast.network_cost, 0.0);
        assert_eq!(shuffle.network_cost, 0.0);
        assert!(
            (broadcast.total_with_options(&options) - shuffle.total_with_options(&options)).abs()
                < 1.0
        );
    }

    #[test]
    #[expect(
        clippy::field_reassign_with_default,
        reason = "The fixture assigns optimizer facts progressively so each test input remains explicit."
    )]
    fn q9_shape_broadcast_total_below_shuffle_total() {
        let mut options = CostOptions::default();
        options.memory_weight = 0.15;
        let mut profile = ClusterResourceProfile::default();
        profile.effective_backend_count = 10.0;
        profile.per_node_build_memory_budget_bytes = 256.0 * 1024.0 * 1024.0;
        options.apply_profile(profile);

        // Tuned so the pre-1.6 broadcast formula (payload * N for both memory
        // and network) would not beat shuffle, while the new fanout term does.
        let mut probe = stats(15_837_500.0, 80.0);
        probe.row_count_confidence = Confidence::Exact;
        let mut build = stats(4_000_000.0, 32.0);
        build.row_count_confidence = Confidence::Exact;
        let own = stats(4_000_000.0, 80.0);
        let op = Operator::PhysicalHashJoin(PhysicalHashJoinOp {
            join_type: JoinKind::Inner,
            build_side: crate::optimizer::operator::HashJoinBuildSide::Right,
            distribution: JoinDistribution::Unknown,
            condition: novarocks_physical_plan::HashJoinCondition::Ordinary {
                keys: vec![],
                residual: None,
            },
        });
        let required = PhysicalPropertySet::any();
        let child_outputs = [PhysicalPropertySet::any(), PhysicalPropertySet::broadcast()];
        let child_output_refs = [&child_outputs[0], &child_outputs[1]];
        let child_stats = [&probe, &build];

        let broadcast = compute_cost_estimate(&broadcast_input(
            &op,
            &own,
            &child_stats,
            &child_output_refs,
            &required,
            &PropertyAlternativeKind::BroadcastJoin,
            &options,
        ));
        let shuffle = compute_cost_estimate(&broadcast_input(
            &op,
            &own,
            &child_stats,
            &child_output_refs,
            &required,
            &PropertyAlternativeKind::ShuffleJoin,
            &options,
        ));

        let broadcast_total = broadcast.total_with_options(&options);
        let shuffle_total = shuffle.total_with_options(&options);
        let backends = normalized_effective_backend_count(options.profile.effective_backend_count);
        let old_broadcast_total = CostEstimate {
            cpu_cost: broadcast.cpu_cost,
            memory_cost: safe_compute_size(&build) * backends,
            network_cost: safe_compute_size(&build) * backends,
        }
        .total_with_options(&options);
        assert!(
            old_broadcast_total >= shuffle_total,
            "old broadcast {old_broadcast_total} should be >= shuffle {shuffle_total}"
        );
        assert!(
            broadcast_total < shuffle_total,
            "broadcast {broadcast_total} should be < shuffle {shuffle_total}"
        );
    }

    #[test]
    fn shuffle_join_estimate_charges_both_sides_network() {
        let probe = stats(1_000_000.0, 64.0);
        let build = stats(1_000_000.0, 64.0);
        let own = stats(100_000.0, 128.0);
        let op = Operator::PhysicalHashJoin(PhysicalHashJoinOp {
            join_type: JoinKind::Inner,
            build_side: crate::optimizer::operator::HashJoinBuildSide::Right,
            distribution: JoinDistribution::Unknown,
            condition: novarocks_physical_plan::HashJoinCondition::Ordinary {
                keys: vec![],
                residual: None,
            },
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
            stats_input: None,
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
            build_side: crate::optimizer::operator::HashJoinBuildSide::Right,
            distribution: JoinDistribution::Unknown,
            condition: novarocks_physical_plan::HashJoinCondition::Ordinary {
                keys: vec![],
                residual: None,
            },
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
            stats_input: None,
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
            build_side: crate::optimizer::operator::HashJoinBuildSide::Right,
            distribution: JoinDistribution::Unknown,
            condition: novarocks_physical_plan::HashJoinCondition::Ordinary {
                keys: vec![],
                residual: None,
            },
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
            stats_input: None,
            options: &options,
        };

        let estimate = compute_cost_estimate(&input);
        let backends = normalized_effective_backend_count(options.profile.effective_backend_count);
        let expected_memory = estimated_build_hash_table_bytes(&build, &options) / backends;
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
            build_side: crate::optimizer::operator::HashJoinBuildSide::Right,
            distribution: JoinDistribution::Colocate,
            condition: novarocks_physical_plan::HashJoinCondition::Ordinary {
                keys: vec![first_key.clone()],
                residual: None,
            },
        });
        let multi_key = Operator::PhysicalHashJoin(PhysicalHashJoinOp {
            join_type: JoinKind::Inner,
            build_side: crate::optimizer::operator::HashJoinBuildSide::Right,
            distribution: JoinDistribution::Colocate,
            condition: novarocks_physical_plan::HashJoinCondition::Ordinary {
                keys: vec![first_key, second_key, third_key],
                residual: None,
            },
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
            stats_input: None,
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
            stats_input: None,
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
            build_side: crate::optimizer::operator::HashJoinBuildSide::Right,
            distribution: JoinDistribution::Colocate,
            condition: novarocks_physical_plan::HashJoinCondition::Ordinary {
                keys: vec![test_eq_condition(&mut scalars, 1, 11)],
                residual: None,
            },
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
            stats_input: None,
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
            stats_input: None,
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
            stats_input: None,
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
    fn weighted_total_is_linear_over_sanitized_cost_addition() {
        let options = CostOptions::default();
        let a = CostEstimate {
            cpu_cost: 10.0,
            memory_cost: f64::NAN,
            network_cost: 4.0,
        };
        let b = CostEstimate {
            cpu_cost: 3.0,
            memory_cost: 7.0,
            network_cost: -1.0,
        };

        let sum_total = a.add_sanitized(&b).total_with_options(&options);
        let separate_total = a.total_with_options(&options) + b.total_with_options(&options);

        assert!((sum_total - separate_total).abs() <= f64::EPSILON);
    }

    #[test]
    fn scan_cost_equals_data_size() {
        let s = stats(1000.0, 100.0);
        let op = Operator::PhysicalScan(ScanOp {
            database: String::new(),
            table: crate::planner::table::TableDef {
                name: "t".into(),
                columns: vec![],
                iceberg_row_lineage_metadata_columns: vec![],
                source: crate::compiler::mv_rewrite::test_scan_source(
                    crate::planner::table::SqlScanKind::ConnectorRead,
                ),
            },
            alias: None,
            stats_ref: None,
            columns: vec![],
            predicates: vec![],
            required_columns: None,
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
            stats_input: None,
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
            stats_input: None,
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
            build_side: crate::optimizer::operator::HashJoinBuildSide::Right,
            distribution: JoinDistribution::Shuffle,
            condition: novarocks_physical_plan::HashJoinCondition::Ordinary {
                keys: vec![],
                residual: None,
            },
        });
        let colocate = Operator::PhysicalHashJoin(PhysicalHashJoinOp {
            join_type: JoinKind::Inner,
            build_side: crate::optimizer::operator::HashJoinBuildSide::Right,
            distribution: JoinDistribution::Colocate,
            condition: novarocks_physical_plan::HashJoinCondition::Ordinary {
                keys: vec![],
                residual: None,
            },
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
            build_side: crate::optimizer::operator::HashJoinBuildSide::Right,
            distribution: JoinDistribution::Unknown,
            condition: novarocks_physical_plan::HashJoinCondition::Ordinary {
                keys: vec![],
                residual: None,
            },
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
    fn broadcast_join_alternative_charges_fanout_and_memory_pressure() {
        let probe = stats(100_000.0, 100.0);
        let build = stats(10_000.0, 100.0);
        let own = stats(100_000.0, 200.0);
        let op = Operator::PhysicalHashJoin(PhysicalHashJoinOp {
            join_type: JoinKind::Inner,
            build_side: crate::optimizer::operator::HashJoinBuildSide::Right,
            distribution: JoinDistribution::Unknown,
            condition: novarocks_physical_plan::HashJoinCondition::Ordinary {
                keys: vec![],
                residual: None,
            },
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
            stats_input: None,
            options: &options,
        };
        let estimate = compute_cost_estimate(&input);
        let expected = CostEstimate {
            cpu_cost: finite_non_negative_cost(
                (cost_row_count(&probe) + cost_row_count(&build)) * options.hash_cost_factor
                    + safe_compute_size(&own),
            ),
            memory_cost: estimated_build_hash_table_bytes(&build, &options)
                * normalized_effective_backend_count(options.profile.effective_backend_count),
            network_cost: safe_compute_size(&build)
                * (normalized_effective_backend_count(options.profile.effective_backend_count)
                    - 1.0)
                    .max(0.0),
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
            &crate::analysis::TypedExpr {
                kind: crate::analysis::ExprKind::Literal(crate::analysis::LiteralValue::Bool(true)),
                value_type: novarocks_type_contract::FunctionValueType::new(
                    arrow::datatypes::DataType::Boolean,
                    false,
                ),
            },
            crate::optimizer::test_optimizer_control(),
        )
        .unwrap();
        let op = Operator::PhysicalHashJoin(PhysicalHashJoinOp {
            join_type: JoinKind::Inner,
            build_side: crate::optimizer::operator::HashJoinBuildSide::Right,
            distribution: JoinDistribution::Unknown,
            condition: novarocks_physical_plan::HashJoinCondition::Ordinary {
                keys: vec![],
                residual: Some(other_condition),
            },
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
            stats_input: None,
            options: &options,
        };
        let estimate = compute_cost_estimate(&input);
        let base_cpu = finite_non_negative_cost(
            (cost_row_count(&probe) + cost_row_count(&build)) * options.hash_cost_factor
                + safe_compute_size(&own),
        );
        let backends = normalized_effective_backend_count(options.profile.effective_backend_count);
        let expected_memory = estimated_build_hash_table_bytes(&build, &options) * backends;
        let expected_network = safe_compute_size(&build) * (backends - 1.0).max(0.0);

        assert!((estimate.cpu_cost - base_cpu * NON_EQUI_JOIN_COST_PENALTY).abs() < f64::EPSILON);
        assert!((estimate.memory_cost - expected_memory).abs() < f64::EPSILON);
        assert!((estimate.network_cost - expected_network).abs() < f64::EPSILON);
    }

    #[test]
    fn local_agg_cheaper_than_single() {
        let input = stats(100_000.0, 50.0);
        let own = stats(100.0, 50.0);

        let single = Operator::PhysicalHashAggregate(PhysicalHashAggregateOp {
            mode: AggMode::Single,
            group_by: vec![],
            aggregates: vec![],
            output_layout: AggregateOutputLayout::new(vec![], vec![]),
            output_columns: vec![],
            is_merge: vec![],
        });
        let local = Operator::PhysicalHashAggregate(PhysicalHashAggregateOp {
            mode: AggMode::Local,
            group_by: vec![],
            aggregates: vec![],
            output_layout: AggregateOutputLayout::new(vec![], vec![]),
            output_columns: vec![],
            is_merge: vec![],
        });

        let cs = [&input];
        assert!(compute_cost(&single, &own, &cs) > compute_cost(&local, &own, &cs));
    }

    #[test]
    fn split_agg_total_cost_can_win_or_lose_after_exchange_cost() {
        use crate::optimizer::property::DistributionSpec;

        let single = Operator::PhysicalHashAggregate(PhysicalHashAggregateOp {
            mode: AggMode::Single,
            group_by: vec![],
            aggregates: vec![],
            output_layout: AggregateOutputLayout::new(vec![], vec![]),
            output_columns: vec![],
            is_merge: vec![],
        });
        let local = Operator::PhysicalHashAggregate(PhysicalHashAggregateOp {
            mode: AggMode::Local,
            group_by: vec![],
            aggregates: vec![],
            output_layout: AggregateOutputLayout::new(vec![], vec![]),
            output_columns: vec![],
            is_merge: vec![],
        });
        let global = Operator::PhysicalHashAggregate(PhysicalHashAggregateOp {
            mode: AggMode::Global,
            group_by: vec![],
            aggregates: vec![],
            output_layout: AggregateOutputLayout::new(vec![], vec![]),
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
            table: crate::planner::table::TableDef {
                name: "t".into(),
                columns: vec![],
                iceberg_row_lineage_metadata_columns: vec![],
                source: crate::compiler::mv_rewrite::test_scan_source(
                    crate::planner::table::SqlScanKind::ConnectorRead,
                ),
            },
            alias: None,
            stats_ref: None,
            columns: vec![],
            predicates: vec![],
            required_columns: None,
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
            spec: crate::optimizer::property::DistributionSpec::Any,
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

    #[test]
    fn cluster_resource_profile_defaults_match_ci_baseline() {
        let opts = CostOptions::default();
        assert_eq!(opts.profile.effective_backend_count, 3.0);
        assert_eq!(opts.backend_factor, 3.0);
        assert_eq!(
            opts.profile.query_mem_limit_bytes,
            2.0 * 1024.0 * 1024.0 * 1024.0
        );
        assert_eq!(
            opts.profile.per_node_build_memory_budget_bytes,
            1.0 * 1024.0 * 1024.0 * 1024.0
        );
        assert_eq!(
            opts.profile.cluster_broadcast_network_budget_bytes,
            opts.profile.per_node_build_memory_budget_bytes
        );
    }

    #[test]
    #[expect(
        clippy::field_reassign_with_default,
        reason = "The fixture assigns optimizer facts progressively so each test input remains explicit."
    )]
    fn apply_profile_syncs_backend_factor_projection() {
        let mut opts = CostOptions::default();
        let mut profile = ClusterResourceProfile::default();
        profile.effective_backend_count = 16.0;
        opts.apply_profile(profile);
        assert_eq!(opts.profile.effective_backend_count, 16.0);
        assert_eq!(opts.backend_factor, 16.0);
    }

    #[test]
    #[expect(
        clippy::field_reassign_with_default,
        reason = "The fixture assigns optimizer facts progressively so each test input remains explicit."
    )]
    fn apply_profile_clamps_backend_factor_to_one() {
        for backend_count in [0.0, 0.5, f64::NAN, f64::INFINITY, f64::NEG_INFINITY, -2.0] {
            let mut opts = CostOptions::default();
            let mut profile = ClusterResourceProfile::default();
            profile.effective_backend_count = backend_count;
            profile.per_node_build_memory_budget_bytes = 256.0 * 1024.0 * 1024.0;
            profile.cluster_broadcast_network_budget_bytes = f64::NAN;

            opts.apply_profile(profile);

            assert_eq!(opts.profile.effective_backend_count, 1.0);
            assert_eq!(opts.backend_factor, 1.0);
            assert_eq!(
                opts.profile.cluster_broadcast_network_budget_bytes,
                opts.profile.per_node_build_memory_budget_bytes
            );
        }
    }
}
