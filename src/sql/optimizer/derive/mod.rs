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
//! `docs/superpowers/specs/2026-05-21-g3-output-properties-visitor-design.md`
//! §1 for the explanation.

use super::memo::Cost;
use super::operator::*;
use super::property::*;
use super::statistics::Statistics;

// ---------------------------------------------------------------------------
// Trait contracts
// ---------------------------------------------------------------------------

pub(crate) trait DeriveOutput {
    /// Compute the physical-property set this operator's chosen physical
    /// expression actually delivers, given the children's chosen-winner
    /// outputs (in child-slot order).
    fn derive_output(&self, children_outputs: &[&PhysicalPropertySet]) -> PhysicalPropertySet;
}

pub(crate) trait DeriveRequired {
    /// Compute the required physical-property set for each child slot.
    ///
    /// Top-down: children outputs are NOT yet known when this is called.
    fn derive_required(
        &self,
        parent_required: &PhysicalPropertySet,
        num_children: usize,
    ) -> Vec<PhysicalPropertySet>;
}

// ---------------------------------------------------------------------------
// Dispatchers
// ---------------------------------------------------------------------------

/// Dispatch `derive_output` based on the operator's concrete variant.
pub(crate) fn derive_output(
    op: &Operator,
    children_outputs: &[&PhysicalPropertySet],
) -> PhysicalPropertySet {
    match op {
        Operator::PhysicalScan(o) => o.derive_output(children_outputs),
        Operator::PhysicalValues(o) => o.derive_output(children_outputs),
        Operator::PhysicalGenerateSeries(o) => o.derive_output(children_outputs),
        Operator::PhysicalCTEConsume(o) => o.derive_output(children_outputs),
        Operator::PhysicalUnion(o) => o.derive_output(children_outputs),
        Operator::PhysicalIntersect(o) => o.derive_output(children_outputs),
        Operator::PhysicalExcept(o) => o.derive_output(children_outputs),
        Operator::PhysicalCTEAnchor(o) => o.derive_output(children_outputs),
        Operator::PhysicalDistribution(o) => o.derive_output(children_outputs),
        Operator::PhysicalFilter(o) => o.derive_output(children_outputs),
        Operator::PhysicalProject(o) => o.derive_output(children_outputs),
        Operator::PhysicalLimit(o) => o.derive_output(children_outputs),
        Operator::PhysicalSubqueryAlias(o) => o.derive_output(children_outputs),
        Operator::PhysicalCTEProduce(o) => o.derive_output(children_outputs),
        Operator::PhysicalRepeat(o) => o.derive_output(children_outputs),
        Operator::PhysicalTableFunction(o) => o.derive_output(children_outputs),
        Operator::PhysicalWindow(o) => o.derive_output(children_outputs),
        Operator::PhysicalNestLoopJoin(o) => o.derive_output(children_outputs),
        Operator::PhysicalHashAggregate(o) => o.derive_output(children_outputs),
        Operator::PhysicalSort(o) => o.derive_output(children_outputs),
        Operator::PhysicalTopN(o) => o.derive_output(children_outputs),
        Operator::PhysicalHashJoin(o) => o.derive_output(children_outputs),
        op => {
            debug_assert!(
                !op.is_physical(),
                "missing dispatch arm for physical operator: {op:?}"
            );
            unreachable!("derive_output called on logical operator: {op:?}");
        }
    }
}

/// Dispatch `derive_required` based on the operator's concrete variant.
pub(crate) fn derive_required(
    op: &Operator,
    parent_required: &PhysicalPropertySet,
    num_children: usize,
) -> Vec<PhysicalPropertySet> {
    match op {
        Operator::PhysicalScan(o) => o.derive_required(parent_required, num_children),
        Operator::PhysicalValues(o) => o.derive_required(parent_required, num_children),
        Operator::PhysicalGenerateSeries(o) => o.derive_required(parent_required, num_children),
        Operator::PhysicalCTEConsume(o) => o.derive_required(parent_required, num_children),
        Operator::PhysicalUnion(o) => o.derive_required(parent_required, num_children),
        Operator::PhysicalIntersect(o) => o.derive_required(parent_required, num_children),
        Operator::PhysicalExcept(o) => o.derive_required(parent_required, num_children),
        Operator::PhysicalCTEAnchor(o) => o.derive_required(parent_required, num_children),
        Operator::PhysicalDistribution(o) => o.derive_required(parent_required, num_children),
        Operator::PhysicalFilter(o) => o.derive_required(parent_required, num_children),
        Operator::PhysicalProject(o) => o.derive_required(parent_required, num_children),
        Operator::PhysicalLimit(o) => o.derive_required(parent_required, num_children),
        Operator::PhysicalSubqueryAlias(o) => o.derive_required(parent_required, num_children),
        Operator::PhysicalCTEProduce(o) => o.derive_required(parent_required, num_children),
        Operator::PhysicalRepeat(o) => o.derive_required(parent_required, num_children),
        Operator::PhysicalTableFunction(o) => o.derive_required(parent_required, num_children),
        Operator::PhysicalWindow(o) => o.derive_required(parent_required, num_children),
        Operator::PhysicalNestLoopJoin(o) => o.derive_required(parent_required, num_children),
        Operator::PhysicalHashAggregate(o) => o.derive_required(parent_required, num_children),
        Operator::PhysicalSort(o) => o.derive_required(parent_required, num_children),
        Operator::PhysicalTopN(o) => o.derive_required(parent_required, num_children),
        Operator::PhysicalHashJoin(o) => o.derive_required(parent_required, num_children),
        op => {
            debug_assert!(
                !op.is_physical(),
                "missing dispatch arm for physical operator: {op:?}"
            );
            unreachable!("derive_required called on logical operator: {op:?}");
        }
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

/// Network cost multiplier — must stay in sync with `cost.rs`.
const NETWORK_COST: f64 = 1.5;

/// Estimate the cost of an enforcer given group statistics.
pub(crate) fn estimate_enforcer_cost(enforcer: &EnforcerKind, stats: &Statistics) -> Cost {
    match enforcer {
        EnforcerKind::Distribution(_) => stats.compute_size() * NETWORK_COST,
        EnforcerKind::Sort(_) => {
            let n = stats.output_row_count.max(1.0);
            n * n.log2()
        }
    }
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
}

// ---------------------------------------------------------------------------
// Sub-modules — populated by Tasks 3–13
// ---------------------------------------------------------------------------

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
