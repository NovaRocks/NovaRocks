//! Physical properties for Cascades optimizer.

use crate::sql::column_id::ColumnId;

#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub(crate) struct ColumnIdSet {
    columns: Vec<ColumnId>,
}

impl ColumnIdSet {
    #[allow(dead_code)]
    pub(crate) fn new() -> Self {
        Self {
            columns: Vec::new(),
        }
    }

    #[allow(dead_code)]
    pub(crate) fn single(column: ColumnId) -> Self {
        Self::from_columns([column])
    }

    pub(crate) fn from_columns<I>(columns: I) -> Self
    where
        I: IntoIterator<Item = ColumnId>,
    {
        let mut columns: Vec<ColumnId> = columns
            .into_iter()
            .filter(|id| *id != ColumnId::UNSET)
            .collect();
        columns.sort_unstable();
        columns.dedup();
        Self { columns }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.columns.is_empty()
    }

    pub(crate) fn len(&self) -> usize {
        self.columns.len()
    }

    pub(crate) fn contains(&self, column: ColumnId) -> bool {
        self.columns.binary_search(&column).is_ok()
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = ColumnId> + '_ {
        self.columns.iter().copied()
    }

    pub(crate) fn min_column(&self) -> Option<ColumnId> {
        self.columns.first().copied()
    }

    pub(crate) fn union(&self, other: &Self) -> Self {
        Self::from_columns(self.iter().chain(other.iter()))
    }

    pub(crate) fn is_subset(&self, other: &Self) -> bool {
        self.iter().all(|id| other.contains(id))
    }

    #[allow(dead_code)]
    pub(crate) fn intersects(&self, other: &Self) -> bool {
        self.iter().any(|id| other.contains(id))
    }
}

impl FromIterator<ColumnId> for ColumnIdSet {
    fn from_iter<T: IntoIterator<Item = ColumnId>>(iter: T) -> Self {
        Self::from_columns(iter)
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct EquivalenceClasses {
    classes: Vec<ColumnIdSet>,
}

impl EquivalenceClasses {
    pub(crate) fn classes(&self) -> &[ColumnIdSet] {
        &self.classes
    }

    #[allow(dead_code)]
    pub(crate) fn is_empty(&self) -> bool {
        self.classes.is_empty()
    }

    pub(crate) fn merge_pair(&mut self, left: ColumnId, right: ColumnId) {
        if left == ColumnId::UNSET || right == ColumnId::UNSET || left == right {
            return;
        }

        let mut matched = Vec::new();
        for (idx, class) in self.classes.iter().enumerate() {
            if class.contains(left) || class.contains(right) {
                matched.push(idx);
            }
        }

        match matched.as_slice() {
            [] => self.classes.push(ColumnIdSet::from_columns([left, right])),
            [idx] => {
                let merged = self.classes[*idx].union(&ColumnIdSet::from_columns([left, right]));
                self.classes[*idx] = merged;
            }
            _ => {
                let mut merged = ColumnIdSet::from_columns([left, right]);
                for idx in matched.iter().rev() {
                    let class = self.classes.remove(*idx);
                    merged = merged.union(&class);
                }
                self.classes.push(merged);
                self.normalize();
            }
        }
    }

    pub(crate) fn extend_from(&mut self, other: &Self) {
        for class in other.classes() {
            let ids: Vec<ColumnId> = class.iter().collect();
            if let Some((&first, rest)) = ids.split_first() {
                for id in rest {
                    self.merge_pair(first, *id);
                }
            }
        }
        self.normalize();
    }

    pub(crate) fn class_containing(&self, column: ColumnId) -> Option<&ColumnIdSet> {
        self.classes.iter().find(|class| class.contains(column))
    }

    pub(crate) fn retain_subset_of(&mut self, output_columns: &ColumnIdSet) {
        self.classes = self
            .classes
            .iter()
            .map(|class| {
                ColumnIdSet::from_columns(class.iter().filter(|id| output_columns.contains(*id)))
            })
            .filter(|class| class.len() >= 2)
            .collect();
        self.normalize();
    }

    pub(crate) fn normalize(&mut self) {
        self.classes
            .sort_by_key(|class| class.min_column().unwrap_or(ColumnId::UNSET));
        self.classes.dedup();
    }
}

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

    #[test]
    fn column_id_set_sorts_dedups_and_drops_unset() {
        let set = ColumnIdSet::from_columns([
            ColumnId(3),
            ColumnId::UNSET,
            ColumnId(1),
            ColumnId(3),
            ColumnId(2),
        ]);
        assert_eq!(
            set.iter().collect::<Vec<_>>(),
            vec![ColumnId(1), ColumnId(2), ColumnId(3)]
        );
        assert!(set.contains(ColumnId(2)));
        assert!(!set.contains(ColumnId::UNSET));
    }

    #[test]
    fn column_id_set_union_keeps_stable_order() {
        let left = ColumnIdSet::from_columns([ColumnId(3), ColumnId(1)]);
        let right = ColumnIdSet::from_columns([ColumnId(2), ColumnId(3)]);
        assert_eq!(
            left.union(&right).iter().collect::<Vec<_>>(),
            vec![ColumnId(1), ColumnId(2), ColumnId(3)]
        );
    }

    #[test]
    fn equivalence_classes_merge_transitively() {
        let mut classes = EquivalenceClasses::default();
        classes.merge_pair(ColumnId(1), ColumnId(2));
        classes.merge_pair(ColumnId(2), ColumnId(3));
        let class = classes.class_containing(ColumnId(1)).expect("class for c1");
        assert_eq!(
            class.iter().collect::<Vec<_>>(),
            vec![ColumnId(1), ColumnId(2), ColumnId(3)]
        );
        assert_eq!(classes.classes().len(), 1);
    }

    #[test]
    fn equivalence_classes_extend_merges_overlapping_classes() {
        let mut left = EquivalenceClasses::default();
        left.merge_pair(ColumnId(1), ColumnId(2));
        let mut right = EquivalenceClasses::default();
        right.merge_pair(ColumnId(2), ColumnId(4));
        left.extend_from(&right);
        let class = left.class_containing(ColumnId(4)).expect("class for c4");
        assert_eq!(
            class.iter().collect::<Vec<_>>(),
            vec![ColumnId(1), ColumnId(2), ColumnId(4)]
        );
    }
}
