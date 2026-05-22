//! Physical properties for Cascades optimizer.

use crate::sql::column_id::ColumnId;

#[derive(Clone, Debug, Hash, Eq, PartialEq)]
pub(crate) struct PhysicalPropertySet {
    pub distribution: DistributionSpec,
    pub ordering: OrderingSpec,
}

impl PhysicalPropertySet {
    pub fn any() -> Self {
        Self {
            distribution: DistributionSpec::Any,
            ordering: OrderingSpec::Any,
        }
    }

    pub fn gather() -> Self {
        Self {
            distribution: DistributionSpec::Gather,
            ordering: OrderingSpec::Any,
        }
    }

    pub fn satisfies(&self, required: &PhysicalPropertySet) -> bool {
        self.distribution.satisfies(&required.distribution)
            && self.ordering.satisfies(&required.ordering)
    }
}

#[derive(Clone, Debug, Hash, Eq, PartialEq)]
pub(crate) enum DistributionSpec {
    Any,
    Gather,
    HashPartitioned(Vec<ColumnId>),
}

impl DistributionSpec {
    pub fn satisfies(&self, required: &DistributionSpec) -> bool {
        match required {
            DistributionSpec::Any => true,
            DistributionSpec::Gather => matches!(self, DistributionSpec::Gather),
            DistributionSpec::HashPartitioned(req_cols) => {
                let DistributionSpec::HashPartitioned(my_cols) = self else {
                    return false;
                };
                // StarRocks-style "containAll" matching: a child that hashes
                // on a SUPERSET of the required columns satisfies the
                // requirement. Rationale: if the child's data is
                // `hash(a, b)`-partitioned, then for any fixed value of
                // `a`, all rows with that `a` are colocated within the
                // bucket determined by `(a, b)` for that `a` — and an
                // operator that only cares about `hash(a)`-locality (e.g.
                // an aggregate / window grouped only on `a`) is happy to
                // accept a finer split, since each bucket still contains
                // only rows of a single `(a, b)` family, and therefore
                // only rows of a single `a`.
                //
                // Concretely: a SHUFFLE_JOIN with eq keys `[l.c0, r.c0]`
                // emits output partitioned by `hash(l.c0, r.c0)`. A
                // downstream Window keyed on `PARTITION BY l.c0` only
                // needs `hash(l.c0)`. Because matched rows share both
                // `l.c0` and `r.c0`, every bucket from the join is
                // homogeneous in `l.c0` — so the window can run locally
                // per bucket without a re-shuffle. (This mirrors
                // `HashDistributionSpec.satisfyContainAll` in
                // StarRocks FE; see PR-F1 spec for the worked example.)
                req_cols.iter().all(|c| my_cols.contains(c))
            }
        }
    }
}

#[derive(Clone, Debug, Hash, Eq, PartialEq)]
pub(crate) enum OrderingSpec {
    Any,
    Required(Vec<SortKey>),
}

impl OrderingSpec {
    pub fn satisfies(&self, required: &OrderingSpec) -> bool {
        match required {
            OrderingSpec::Any => true,
            OrderingSpec::Required(req_keys) => {
                if let OrderingSpec::Required(my_keys) = self {
                    // Provided ordering must be a prefix-or-equal match
                    my_keys.len() >= req_keys.len()
                        && my_keys.iter().zip(req_keys).all(|(m, r)| m == r)
                } else {
                    false
                }
            }
        }
    }
}

#[derive(Clone, Debug, Hash, Eq, PartialEq)]
pub(crate) struct SortKey {
    pub column: ColumnId,
    pub asc: bool,
    pub nulls_first: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hash_partitioned_satisfies_exact_match() {
        let provided = DistributionSpec::HashPartitioned(vec![ColumnId(1), ColumnId(2)]);
        let required = DistributionSpec::HashPartitioned(vec![ColumnId(1), ColumnId(2)]);
        assert!(provided.satisfies(&required));
    }

    #[test]
    fn hash_partitioned_satisfies_when_provider_has_superset() {
        // Child hashes on (c1, c2); a downstream operator that only needs
        // hash(c1) is satisfied because each (c1,c2) bucket is homogeneous
        // in `c1` — the StarRocks `satisfyContainAll` rule.
        let provided = DistributionSpec::HashPartitioned(vec![ColumnId(1), ColumnId(2)]);
        let required = DistributionSpec::HashPartitioned(vec![ColumnId(1)]);
        assert!(provided.satisfies(&required));
    }

    #[test]
    fn hash_partitioned_satisfies_when_required_in_any_position() {
        // Order within the hash key vector doesn't matter — what matters
        // is that the required column is part of the hash.
        let provided =
            DistributionSpec::HashPartitioned(vec![ColumnId(1), ColumnId(2), ColumnId(3)]);
        let required = DistributionSpec::HashPartitioned(vec![ColumnId(2)]);
        assert!(provided.satisfies(&required));
    }

    #[test]
    fn hash_partitioned_does_not_satisfy_disjoint_columns() {
        let provided = DistributionSpec::HashPartitioned(vec![ColumnId(1)]);
        let required = DistributionSpec::HashPartitioned(vec![ColumnId(2)]);
        assert!(!provided.satisfies(&required));
    }

    #[test]
    fn hash_partitioned_does_not_satisfy_when_required_has_extra() {
        // Provided hash(c1) does NOT satisfy required hash(c1, c2) — a
        // single bucket of the provider can contain rows with different
        // `c2` values, so an operator that needs (c1, c2)-locality is not
        // safe.
        let provided = DistributionSpec::HashPartitioned(vec![ColumnId(1)]);
        let required = DistributionSpec::HashPartitioned(vec![ColumnId(1), ColumnId(2)]);
        assert!(!provided.satisfies(&required));
    }

    #[test]
    fn gather_does_not_satisfy_hash_partitioned() {
        let provided = DistributionSpec::Gather;
        let required = DistributionSpec::HashPartitioned(vec![ColumnId(1)]);
        assert!(!provided.satisfies(&required));
    }

    #[test]
    fn any_required_is_satisfied_by_anything() {
        for provided in [
            DistributionSpec::Any,
            DistributionSpec::Gather,
            DistributionSpec::HashPartitioned(vec![ColumnId(1)]),
        ] {
            assert!(provided.satisfies(&DistributionSpec::Any));
        }
    }
}
