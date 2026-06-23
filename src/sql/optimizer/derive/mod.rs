//! Per-operator physical-property deriver traits and dispatchers.
//!
//! Top-down Cascades search consults these visitors to:
//!   1. derive_required: what each child group must satisfy (top-down,
//!      before children are optimized — no child visibility).
//!   2. derive_output: what this physical expression actually delivers,
//!      given the children's chosen-winner outputs (after children are
//!      optimized).
//!
//! The asymmetry between (1) and (2) is structural — see the G3 spec
//! `docs/design/specs/2026-05-21-g3-output-properties-visitor-design.md`
//! §1 for the explanation.

use super::cost::{CostOptions, estimate_distribution_cost_estimate, estimate_sort_cost_estimate};
use super::memo::TotalCost;
use super::operator::*;
use super::property::*;
use super::scalar::{ScalarArena, ScalarId, ScalarNode, SortKey as ScalarSortKey};
use super::statistics::{CostEstimate, Statistics};
use crate::sql::column_id::ColumnId;

// ---------------------------------------------------------------------------
// Trait contracts
// ---------------------------------------------------------------------------

pub(crate) trait DeriveOutput {
    /// Compute the physical-property set this operator's chosen physical
    /// expression actually delivers, given the children's chosen-winner
    /// outputs (in child-slot order).
    fn derive_output(
        &self,
        scalars: &ScalarArena,
        children_outputs: &[&PhysicalPropertySet],
    ) -> PhysicalPropertySet;
}

pub(crate) trait DeriveRequired {
    /// Compute the required physical-property set for each child slot.
    ///
    /// Top-down: children outputs are NOT yet known when this is called.
    fn derive_required(
        &self,
        scalars: &ScalarArena,
        parent_required: &PhysicalPropertySet,
        num_children: usize,
    ) -> Vec<PhysicalPropertySet>;
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) enum PropertyAlternativeKind {
    Default,
    BroadcastJoin,
    ShuffleJoin,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) struct ChildRequirementAlternative {
    pub kind: PropertyAlternativeKind,
    pub child_props: Vec<PhysicalPropertySet>,
}

impl ChildRequirementAlternative {
    pub(crate) fn default(child_props: Vec<PhysicalPropertySet>) -> Self {
        Self {
            kind: PropertyAlternativeKind::Default,
            child_props,
        }
    }
}

pub(super) fn scalar_expr_to_column_id(scalars: &ScalarArena, expr: ScalarId) -> Option<ColumnId> {
    match scalars.node(expr) {
        ScalarNode::ColumnRef(column_id) if *column_id != ColumnId::UNSET => Some(*column_id),
        _ => None,
    }
}

pub(super) fn scalar_exprs_to_column_ids(
    scalars: &ScalarArena,
    exprs: &[ScalarId],
) -> Option<Vec<ColumnId>> {
    let mut out = Vec::with_capacity(exprs.len());
    for expr in exprs {
        out.push(scalar_expr_to_column_id(scalars, *expr)?);
    }
    Some(out)
}

pub(super) fn ordering_from_scalar_sort_keys(
    scalars: &ScalarArena,
    sort_keys: &[ScalarSortKey],
) -> OrderingSpec {
    let mut keys = Vec::with_capacity(sort_keys.len());
    for sort_key in sort_keys {
        let Some(column) = scalar_expr_to_column_id(scalars, sort_key.expr) else {
            return OrderingSpec::Any;
        };
        keys.push(SortKey {
            column,
            asc: sort_key.asc,
            nulls_first: sort_key.nulls_first,
        });
    }
    if keys.is_empty() {
        OrderingSpec::Any
    } else {
        OrderingSpec::Required(keys)
    }
}

pub(super) fn window_ordering_from_scalar_keys(
    scalars: &ScalarArena,
    partition_by: &[ScalarId],
    order_by: &[ScalarSortKey],
) -> OrderingSpec {
    let mut keys = Vec::with_capacity(partition_by.len() + order_by.len());
    for expr in partition_by {
        let Some(column) = scalar_expr_to_column_id(scalars, *expr) else {
            return OrderingSpec::Any;
        };
        keys.push(SortKey {
            column,
            asc: true,
            nulls_first: true,
        });
    }
    for sort_key in order_by {
        let Some(column) = scalar_expr_to_column_id(scalars, sort_key.expr) else {
            return OrderingSpec::Any;
        };
        keys.push(SortKey {
            column,
            asc: sort_key.asc,
            nulls_first: sort_key.nulls_first,
        });
    }
    if keys.is_empty() {
        OrderingSpec::Any
    } else {
        OrderingSpec::Required(keys)
    }
}

// ---------------------------------------------------------------------------
// Dispatchers
// ---------------------------------------------------------------------------

/// Dispatch `derive_output` based on the operator's concrete variant.
pub(crate) fn derive_output(
    op: &Operator,
    scalars: &ScalarArena,
    children_outputs: &[&PhysicalPropertySet],
) -> PhysicalPropertySet {
    match op {
        Operator::PhysicalScan(o) => o.derive_output(scalars, children_outputs),
        Operator::PhysicalValues(o) => o.derive_output(scalars, children_outputs),
        Operator::PhysicalGenerateSeries(o) => o.derive_output(scalars, children_outputs),
        Operator::PhysicalCTEConsume(o) => o.derive_output(scalars, children_outputs),
        Operator::PhysicalUnion(o) => o.derive_output(scalars, children_outputs),
        Operator::PhysicalIntersect(o) => o.derive_output(scalars, children_outputs),
        Operator::PhysicalExcept(o) => o.derive_output(scalars, children_outputs),
        Operator::PhysicalCTEAnchor(o) => o.derive_output(scalars, children_outputs),
        Operator::PhysicalDistribution(o) => o.derive_output(scalars, children_outputs),
        Operator::PhysicalFilter(o) => o.derive_output(scalars, children_outputs),
        Operator::PhysicalProject(o) => o.derive_output(scalars, children_outputs),
        Operator::PhysicalDecode(o) => o.derive_output(scalars, children_outputs),
        Operator::PhysicalAggregateStateMerge(o) => o.derive_output(scalars, children_outputs),
        Operator::PhysicalLimit(o) => o.derive_output(scalars, children_outputs),
        Operator::PhysicalAssertOneRow(o) => o.derive_output(scalars, children_outputs),
        Operator::PhysicalCTEProduce(o) => o.derive_output(scalars, children_outputs),
        Operator::PhysicalRepeat(o) => o.derive_output(scalars, children_outputs),
        Operator::PhysicalTableFunction(o) => o.derive_output(scalars, children_outputs),
        Operator::PhysicalWindow(o) => o.derive_output(scalars, children_outputs),
        Operator::PhysicalNestLoopJoin(o) => o.derive_output(scalars, children_outputs),
        Operator::PhysicalHashAggregate(o) => o.derive_output(scalars, children_outputs),
        Operator::PhysicalSort(o) => o.derive_output(scalars, children_outputs),
        Operator::PhysicalTopN(o) => o.derive_output(scalars, children_outputs),
        Operator::PhysicalHashJoin(o) => o.derive_output(scalars, children_outputs),
        op => {
            debug_assert!(
                !op.is_physical(),
                "missing dispatch arm for physical operator: {op:?}"
            );
            unreachable!("derive_output called on logical operator: {op:?}");
        }
    }
}

pub(crate) fn derive_output_for_alternative(
    op: &Operator,
    scalars: &ScalarArena,
    children_outputs: &[&PhysicalPropertySet],
    alt_kind: &PropertyAlternativeKind,
) -> PhysicalPropertySet {
    match op {
        Operator::PhysicalHashJoin(o) => {
            o.derive_output_for_alternative(scalars, children_outputs, alt_kind)
        }
        _ => derive_output(op, scalars, children_outputs),
    }
}

/// Dispatch `derive_required` based on the operator's concrete variant.
pub(crate) fn derive_required(
    op: &Operator,
    scalars: &ScalarArena,
    parent_required: &PhysicalPropertySet,
    num_children: usize,
) -> Vec<PhysicalPropertySet> {
    match op {
        Operator::PhysicalScan(o) => o.derive_required(scalars, parent_required, num_children),
        Operator::PhysicalValues(o) => o.derive_required(scalars, parent_required, num_children),
        Operator::PhysicalGenerateSeries(o) => {
            o.derive_required(scalars, parent_required, num_children)
        }
        Operator::PhysicalCTEConsume(o) => {
            o.derive_required(scalars, parent_required, num_children)
        }
        Operator::PhysicalUnion(o) => o.derive_required(scalars, parent_required, num_children),
        Operator::PhysicalIntersect(o) => o.derive_required(scalars, parent_required, num_children),
        Operator::PhysicalExcept(o) => o.derive_required(scalars, parent_required, num_children),
        Operator::PhysicalCTEAnchor(o) => o.derive_required(scalars, parent_required, num_children),
        Operator::PhysicalDistribution(o) => {
            o.derive_required(scalars, parent_required, num_children)
        }
        Operator::PhysicalFilter(o) => o.derive_required(scalars, parent_required, num_children),
        Operator::PhysicalProject(o) => o.derive_required(scalars, parent_required, num_children),
        Operator::PhysicalDecode(o) => o.derive_required(scalars, parent_required, num_children),
        Operator::PhysicalAggregateStateMerge(o) => {
            o.derive_required(scalars, parent_required, num_children)
        }
        Operator::PhysicalLimit(o) => o.derive_required(scalars, parent_required, num_children),
        Operator::PhysicalAssertOneRow(o) => {
            o.derive_required(scalars, parent_required, num_children)
        }
        Operator::PhysicalCTEProduce(o) => {
            o.derive_required(scalars, parent_required, num_children)
        }
        Operator::PhysicalRepeat(o) => o.derive_required(scalars, parent_required, num_children),
        Operator::PhysicalTableFunction(o) => {
            o.derive_required(scalars, parent_required, num_children)
        }
        Operator::PhysicalWindow(o) => o.derive_required(scalars, parent_required, num_children),
        Operator::PhysicalNestLoopJoin(o) => {
            o.derive_required(scalars, parent_required, num_children)
        }
        Operator::PhysicalHashAggregate(o) => {
            o.derive_required(scalars, parent_required, num_children)
        }
        Operator::PhysicalSort(o) => o.derive_required(scalars, parent_required, num_children),
        Operator::PhysicalTopN(o) => o.derive_required(scalars, parent_required, num_children),
        Operator::PhysicalHashJoin(o) => o.derive_required(scalars, parent_required, num_children),
        op => {
            debug_assert!(
                !op.is_physical(),
                "missing dispatch arm for physical operator: {op:?}"
            );
            unreachable!("derive_required called on logical operator: {op:?}");
        }
    }
}

pub(crate) fn derive_required_alternatives(
    op: &Operator,
    scalars: &ScalarArena,
    parent_required: &PhysicalPropertySet,
    num_children: usize,
) -> Vec<ChildRequirementAlternative> {
    match op {
        Operator::PhysicalHashJoin(o) => {
            o.derive_required_alternatives(scalars, parent_required, num_children)
        }
        _ => vec![ChildRequirementAlternative::default(derive_required(
            op,
            scalars,
            parent_required,
            num_children,
        ))],
    }
}

// ---------------------------------------------------------------------------
// Enforcer helpers — single source of truth (search.rs re-exports EnforcerKind).
// ---------------------------------------------------------------------------

/// Determine what enforcers are needed to bridge `provided` → `required`.
pub(crate) fn needed_enforcers(
    required: &PhysicalPropertySet,
    provided: &PhysicalPropertySet,
) -> Vec<EnforcerKind> {
    let mut enforcers = Vec::new();
    if !provided.distribution.satisfies(&required.distribution) {
        enforcers.push(EnforcerKind::Distribution(required.distribution.clone()));
    }
    if !provided.ordering.satisfies(&required.ordering) {
        enforcers.push(EnforcerKind::Sort(required.ordering.clone()));
    }
    enforcers
}

/// Estimate the dimensional cost of an enforcer given group statistics.
pub(crate) fn estimate_enforcer_cost_estimate(
    enforcer: &EnforcerKind,
    stats: &Statistics,
    options: &CostOptions,
) -> CostEstimate {
    match enforcer {
        EnforcerKind::Distribution(_) => estimate_distribution_cost_estimate(stats, options),
        EnforcerKind::Sort(_) => estimate_sort_cost_estimate(stats, options),
    }
}

/// Estimate the cost of an enforcer given group statistics.
pub(crate) fn estimate_enforcer_cost(enforcer: &EnforcerKind, stats: &Statistics) -> TotalCost {
    let options = CostOptions::default();
    estimate_enforcer_cost_estimate(enforcer, stats, &options).total_with_options(&options)
}

// ---------------------------------------------------------------------------
// EnforcerKind type — single source of truth, re-exported from search.rs.
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub(crate) enum EnforcerKind {
    Distribution(DistributionSpec),
    Sort(OrderingSpec),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::cost::NETWORK_COST;

    #[test]
    fn needed_enforcers_distribution_mismatch() {
        let required = PhysicalPropertySet::gather();
        let provided = PhysicalPropertySet::any();
        let enforcers = needed_enforcers(&required, &provided);
        assert_eq!(enforcers.len(), 1);
        assert!(matches!(
            enforcers[0],
            EnforcerKind::Distribution(DistributionSpec::Gather)
        ));
    }

    #[test]
    fn needed_enforcers_no_mismatch() {
        let required = PhysicalPropertySet::any();
        let provided = PhysicalPropertySet::gather();
        let enforcers = needed_enforcers(&required, &provided);
        assert!(enforcers.is_empty());
    }

    #[test]
    fn default_required_alternative_wraps_legacy_deriver() {
        let scalars = ScalarArena::new();
        let op = Operator::PhysicalLimit(LimitOp {
            limit: Some(10),
            offset: None,
        });
        let parent = PhysicalPropertySet::gather();

        let legacy = derive_required(&op, &scalars, &parent, 1);
        let alternatives = derive_required_alternatives(&op, &scalars, &parent, 1);

        assert_eq!(alternatives.len(), 1);
        assert_eq!(alternatives[0].kind, PropertyAlternativeKind::Default);
        assert_eq!(alternatives[0].child_props, legacy);
    }

    #[test]
    fn distribution_enforcer_cost_includes_startup_overhead() {
        let stats = Statistics {
            output_row_count: 1.0,
            column_statistics: Default::default(),
            ..Default::default()
        };

        let cost = estimate_enforcer_cost(
            &EnforcerKind::Distribution(DistributionSpec::Broadcast),
            &stats,
        );

        assert!(
            cost > stats.compute_size() * NETWORK_COST,
            "distribution enforcers must model startup overhead for tiny exchanges"
        );
    }

    #[test]
    fn distribution_enforcer_cost_estimate_has_network_dimension() {
        let stats = Statistics {
            output_row_count: 1000.0,
            column_statistics: Default::default(),
            ..Default::default()
        };
        let estimate = estimate_enforcer_cost_estimate(
            &EnforcerKind::Distribution(DistributionSpec::Gather),
            &stats,
            &crate::sql::optimizer::cost::CostOptions::default(),
        );

        assert!(estimate.memory_cost > 0.0);
        assert!(estimate.network_cost > 0.0);
    }

    #[test]
    fn sort_enforcer_cost_estimate_saturates_huge_and_sanitizes_invalid_stats() {
        let options = crate::sql::optimizer::cost::CostOptions::default();
        let huge_stats = Statistics {
            output_row_count: f64::MAX,
            column_statistics: Default::default(),
            ..Default::default()
        };
        let huge_estimate = estimate_enforcer_cost_estimate(
            &EnforcerKind::Sort(OrderingSpec::Any),
            &huge_stats,
            &options,
        );

        assert!(huge_estimate.cpu_cost.is_finite());
        assert!(huge_estimate.cpu_cost > 1.0e299);
        assert!(huge_estimate.memory_cost.is_finite());
        assert!(huge_estimate.memory_cost > 1.0e299);
        assert_eq!(huge_estimate.network_cost, 0.0);

        let invalid_stats = Statistics {
            output_row_count: f64::NAN,
            column_statistics: Default::default(),
            ..Default::default()
        };
        let invalid_estimate = estimate_enforcer_cost_estimate(
            &EnforcerKind::Sort(OrderingSpec::Any),
            &invalid_stats,
            &options,
        );

        assert!(invalid_estimate.cpu_cost.is_finite());
        assert!(invalid_estimate.cpu_cost >= 0.0);
        assert!(invalid_estimate.memory_cost.is_finite());
        assert!(invalid_estimate.memory_cost >= 0.0);
        assert_eq!(invalid_estimate.network_cost, 0.0);
    }

    #[test]
    fn shuffle_join_output_needs_enforcer_for_narrower_shuffle_agg_requirement() {
        let required = PhysicalPropertySet {
            distribution: DistributionSpec::shuffle_agg([ColumnId(10)]),
            ordering: OrderingSpec::Any,
        };
        let provided = PhysicalPropertySet {
            distribution: DistributionSpec::shuffle_join([ColumnId(10), ColumnId(20)]),
            ordering: OrderingSpec::Any,
        };

        let enforcers = needed_enforcers(&required, &provided);
        assert_eq!(enforcers.len(), 1);
        match &enforcers[0] {
            EnforcerKind::Distribution(DistributionSpec::HashPartitioned { cols, source }) => {
                assert_eq!(*source, HashSource::ShuffleAgg);
                assert_eq!(cols.as_slice(), &[ColumnId(10)]);
            }
            other => panic!("expected ShuffleAgg([c10]) distribution enforcer, got {other:?}"),
        }
    }
}

// ---------------------------------------------------------------------------
// Sub-modules — populated by Tasks 3–13
// ---------------------------------------------------------------------------

pub(crate) mod aggregate_state_merge;
pub(crate) mod assert_one_row;
pub(crate) mod cte;
pub(crate) mod enforcer;
pub(crate) mod hash_aggregate;
pub(crate) mod hash_join;
pub(crate) mod nest_loop_join;
pub(crate) mod passthrough;
pub(crate) mod scan;
pub(crate) mod set_op;
pub(crate) mod sort;
pub(crate) mod top_n;
pub(crate) mod window;
