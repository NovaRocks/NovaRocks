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

//! Physical properties for Cascades optimizer.

use crate::column_id::ColumnId;

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

    pub fn broadcast() -> Self {
        Self {
            distribution: DistributionSpec::Broadcast,
            ordering: OrderingSpec::Any,
        }
    }

    pub fn satisfies(&self, required: &PhysicalPropertySet) -> bool {
        self.distribution.satisfies(&required.distribution)
            && self.ordering.satisfies(&required.ordering)
    }
}

#[derive(Clone, Copy, Debug, Hash, Eq, PartialEq)]
pub(crate) enum HashSource {
    ShuffleAgg,
    ShuffleJoin,
    MapGrouping,
    MapSet,
}

impl HashSource {
    fn uses_canonical_map(self) -> bool {
        matches!(self, Self::MapGrouping | Self::MapSet)
    }

    fn allows_subset(self) -> bool {
        matches!(self, Self::ShuffleAgg | Self::MapGrouping)
    }
}

#[derive(Clone, Debug, Hash, Eq, PartialEq)]
pub(crate) enum DistributionSpec {
    Any,
    Gather,
    Broadcast,
    HashPartitioned {
        cols: Vec<ColumnId>,
        source: HashSource,
    },
}

impl DistributionSpec {
    pub(crate) fn shuffle_agg<I>(cols: I) -> Self
    where
        I: IntoIterator<Item = ColumnId>,
    {
        Self::hash_partitioned(cols, HashSource::ShuffleAgg)
    }

    pub(crate) fn shuffle_join<I>(cols: I) -> Self
    where
        I: IntoIterator<Item = ColumnId>,
    {
        Self::hash_partitioned(cols, HashSource::ShuffleJoin)
    }

    pub(crate) fn hash_partitioned<I>(cols: I, source: HashSource) -> Self
    where
        I: IntoIterator<Item = ColumnId>,
    {
        let mut normalized = Vec::new();
        for col in cols {
            if col == ColumnId::UNSET || normalized.contains(&col) {
                continue;
            }
            normalized.push(col);
        }
        if normalized.is_empty() {
            DistributionSpec::Any
        } else {
            DistributionSpec::HashPartitioned {
                cols: normalized,
                source,
            }
        }
    }

    #[allow(dead_code)]
    pub(crate) fn hash_cols(&self) -> Option<&[ColumnId]> {
        match self {
            DistributionSpec::HashPartitioned { cols, .. } => Some(cols.as_slice()),
            _ => None,
        }
    }

    #[allow(dead_code)]
    pub(crate) fn hash_source(&self) -> Option<HashSource> {
        match self {
            DistributionSpec::HashPartitioned { source, .. } => Some(*source),
            _ => None,
        }
    }

    pub fn satisfies(&self, required: &DistributionSpec) -> bool {
        match required {
            DistributionSpec::Any => true,
            DistributionSpec::Gather => matches!(self, DistributionSpec::Gather),
            DistributionSpec::Broadcast => matches!(self, DistributionSpec::Broadcast),
            DistributionSpec::HashPartitioned {
                cols: required_cols,
                source: required_source,
            } => {
                let DistributionSpec::HashPartitioned {
                    cols: provided_cols,
                    source: provided_source,
                } = self
                else {
                    return false;
                };
                provided_source.uses_canonical_map() == required_source.uses_canonical_map()
                    && if required_source.allows_subset() {
                        hash_cols_subset(provided_cols, required_cols)
                    } else {
                        provided_cols == required_cols
                    }
            }
        }
    }
}

fn hash_cols_subset(left: &[ColumnId], right: &[ColumnId]) -> bool {
    left.iter().all(|col| right.contains(col))
}

#[derive(Clone, Debug, Hash, Eq, PartialEq)]
pub(crate) enum OrderingSpec {
    Any,
    Required(Vec<SortKey>),
}

impl OrderingSpec {
    #[allow(
        dead_code,
        reason = "Retained for staged SQL planner migration consumers and test helpers."
    )]
    pub(crate) fn from_sort_keys<I>(items: I) -> Self
    where
        I: IntoIterator<Item = SortKey>,
    {
        let keys: Vec<SortKey> = items.into_iter().collect();
        if keys.is_empty() {
            OrderingSpec::Any
        } else {
            OrderingSpec::Required(keys)
        }
    }

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
    fn shuffle_agg_subset_satisfies_finer_shuffle_agg_requirement() {
        let provided = DistributionSpec::shuffle_agg([ColumnId(1)]);
        let required = DistributionSpec::shuffle_agg([ColumnId(1), ColumnId(2)]);
        assert!(provided.satisfies(&required));
    }

    #[test]
    fn shuffle_agg_superset_does_not_satisfy_coarser_shuffle_agg_requirement() {
        let provided = DistributionSpec::shuffle_agg([ColumnId(1), ColumnId(2)]);
        let required = DistributionSpec::shuffle_agg([ColumnId(1)]);
        assert!(!provided.satisfies(&required));
    }

    #[test]
    fn shuffle_join_requires_exact_ordered_keys() {
        let provided = DistributionSpec::shuffle_join([ColumnId(1), ColumnId(2)]);
        let exact = DistributionSpec::shuffle_join([ColumnId(1), ColumnId(2)]);
        let reordered = DistributionSpec::shuffle_join([ColumnId(2), ColumnId(1)]);
        let prefix = DistributionSpec::shuffle_join([ColumnId(1)]);

        assert!(provided.satisfies(&exact));
        assert!(!provided.satisfies(&reordered));
        assert!(!provided.satisfies(&prefix));
    }

    #[test]
    fn shuffle_join_does_not_satisfy_narrower_shuffle_agg_requirement() {
        let provided = DistributionSpec::shuffle_join([ColumnId(10), ColumnId(20)]);
        let required = DistributionSpec::shuffle_agg([ColumnId(10)]);
        assert!(!provided.satisfies(&required));
    }

    #[test]
    fn cross_source_rules_are_conservative() {
        let agg_exact = DistributionSpec::shuffle_agg([ColumnId(1), ColumnId(2)]);
        let join_exact = DistributionSpec::shuffle_join([ColumnId(1), ColumnId(2)]);
        let join_finer = DistributionSpec::shuffle_join([ColumnId(1)]);
        let agg_finer_required =
            DistributionSpec::shuffle_agg([ColumnId(1), ColumnId(2), ColumnId(3)]);

        assert!(agg_exact.satisfies(&join_exact));
        assert!(join_finer.satisfies(&agg_finer_required));
        assert!(!join_exact.satisfies(&DistributionSpec::shuffle_agg([ColumnId(1)])));
    }

    #[test]
    fn canonical_map_distribution_cannot_reuse_a_legacy_partition() {
        let keys = [ColumnId(1), ColumnId(2)];
        let legacy_set = DistributionSpec::shuffle_join(keys);
        let map_set = DistributionSpec::hash_partitioned(keys, HashSource::MapSet);
        let map_group = DistributionSpec::hash_partitioned(keys, HashSource::MapGrouping);
        let coarser = DistributionSpec::hash_partitioned([ColumnId(1)], HashSource::MapGrouping);

        assert!(!legacy_set.satisfies(&map_set));
        assert!(!map_group.satisfies(&legacy_set));
        assert!(map_set.satisfies(&map_group));
        assert!(coarser.satisfies(&map_group));
        assert!(!coarser.satisfies(&map_set));
    }

    #[test]
    fn hash_constructors_drop_unset_and_dedup_preserving_first_seen_order() {
        let spec = DistributionSpec::shuffle_agg([
            ColumnId(3),
            ColumnId::UNSET,
            ColumnId(1),
            ColumnId(3),
            ColumnId(2),
        ]);

        assert_eq!(spec.hash_source(), Some(HashSource::ShuffleAgg));
        assert_eq!(
            spec.hash_cols(),
            Some([ColumnId(3), ColumnId(1), ColumnId(2)].as_slice())
        );
        match spec {
            DistributionSpec::HashPartitioned { cols, source } => {
                assert_eq!(source, HashSource::ShuffleAgg);
                assert_eq!(cols, vec![ColumnId(3), ColumnId(1), ColumnId(2)]);
            }
            other => panic!("expected hash distribution, got {other:?}"),
        }
    }

    #[test]
    fn hash_partitioned_satisfies_exact_match() {
        let provided = DistributionSpec::shuffle_agg([ColumnId(1), ColumnId(2)]);
        let required = DistributionSpec::shuffle_agg([ColumnId(1), ColumnId(2)]);
        assert!(provided.satisfies(&required));
    }

    #[test]
    fn shuffle_agg_does_not_satisfy_disjoint_columns() {
        let provided = DistributionSpec::shuffle_agg([ColumnId(1)]);
        let required = DistributionSpec::shuffle_agg([ColumnId(2)]);
        assert!(!provided.satisfies(&required));
    }

    #[test]
    fn shuffle_agg_subset_match_ignores_order() {
        // Order within the hash key vector doesn't matter — what matters
        // is that the required column is part of the hash.
        let provided = DistributionSpec::shuffle_agg([ColumnId(2)]);
        let required = DistributionSpec::shuffle_agg([ColumnId(1), ColumnId(2), ColumnId(3)]);
        assert!(provided.satisfies(&required));
    }

    #[test]
    fn shuffle_join_does_not_satisfy_when_required_has_extra() {
        let provided = DistributionSpec::shuffle_join([ColumnId(1)]);
        let required = DistributionSpec::shuffle_join([ColumnId(1), ColumnId(2)]);
        assert!(!provided.satisfies(&required));
    }

    #[test]
    fn gather_does_not_satisfy_hash_partitioned() {
        let provided = DistributionSpec::Gather;
        let required = DistributionSpec::shuffle_agg([ColumnId(1)]);
        assert!(!provided.satisfies(&required));
    }

    #[test]
    fn broadcast_distribution_satisfies_only_broadcast_and_any() {
        let provided = DistributionSpec::Broadcast;

        assert!(provided.satisfies(&DistributionSpec::Any));
        assert!(provided.satisfies(&DistributionSpec::Broadcast));
        assert!(!provided.satisfies(&DistributionSpec::Gather));
        assert!(!provided.satisfies(&DistributionSpec::shuffle_join([ColumnId(1)])));
        assert!(!DistributionSpec::Gather.satisfies(&DistributionSpec::Broadcast));
        assert!(
            !DistributionSpec::shuffle_join([ColumnId(1)]).satisfies(&DistributionSpec::Broadcast)
        );
    }

    #[test]
    fn physical_property_set_broadcast_uses_any_ordering() {
        let props = PhysicalPropertySet::broadcast();
        assert_eq!(props.distribution, DistributionSpec::Broadcast);
        assert_eq!(props.ordering, OrderingSpec::Any);
    }

    #[test]
    fn ordering_from_sort_keys_returns_any_for_empty_keys() {
        assert_eq!(OrderingSpec::from_sort_keys([]), OrderingSpec::Any);
    }

    #[test]
    fn ordering_from_sort_keys_preserves_native_keys() {
        let required = OrderingSpec::from_sort_keys([SortKey {
            column: ColumnId(1),
            asc: true,
            nulls_first: true,
        }]);

        assert_eq!(
            required,
            OrderingSpec::Required(vec![SortKey {
                column: ColumnId(1),
                asc: true,
                nulls_first: true,
            }])
        );
    }

    #[test]
    fn any_required_is_satisfied_by_anything() {
        for provided in [
            DistributionSpec::Any,
            DistributionSpec::Gather,
            DistributionSpec::shuffle_agg([ColumnId(1)]),
        ] {
            assert!(provided.satisfies(&DistributionSpec::Any));
        }
    }

    #[test]
    fn ordering_prefix_satisfies_shorter_required_ordering() {
        let provided = OrderingSpec::Required(vec![
            SortKey {
                column: ColumnId(1),
                asc: true,
                nulls_first: true,
            },
            SortKey {
                column: ColumnId(2),
                asc: false,
                nulls_first: false,
            },
        ]);
        let required = OrderingSpec::Required(vec![SortKey {
            column: ColumnId(1),
            asc: true,
            nulls_first: true,
        }]);

        assert!(provided.satisfies(&required));
    }

    #[test]
    fn ordering_rejects_direction_or_nulls_mismatch() {
        let provided = OrderingSpec::Required(vec![SortKey {
            column: ColumnId(1),
            asc: true,
            nulls_first: true,
        }]);
        let desc_required = OrderingSpec::Required(vec![SortKey {
            column: ColumnId(1),
            asc: false,
            nulls_first: true,
        }]);
        let nulls_last_required = OrderingSpec::Required(vec![SortKey {
            column: ColumnId(1),
            asc: true,
            nulls_first: false,
        }]);

        assert!(!provided.satisfies(&desc_required));
        assert!(!provided.satisfies(&nulls_last_required));
    }

    #[test]
    fn ordering_prefix_stays_strict_without_equivalence_helper() {
        let provided = OrderingSpec::Required(vec![SortKey {
            column: ColumnId(1),
            asc: true,
            nulls_first: false,
        }]);
        let required = OrderingSpec::Required(vec![SortKey {
            column: ColumnId(2),
            asc: true,
            nulls_first: false,
        }]);

        assert!(!provided.satisfies(&required));
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
