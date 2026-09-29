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

use std::collections::HashSet;
use std::hash::{BuildHasher, RandomState};
use std::sync::Arc;

use super::metrics::delete_may_match;
use super::{BucketKind, CandidateLookupCost, DataFileFact, DeleteFact, FrozenBucket, ReadDomain};

#[derive(Clone, Debug)]
pub struct BucketView {
    bucket: Arc<FrozenBucket>,
    start: usize,
}

impl BucketView {
    pub(crate) fn new(bucket: Arc<FrozenBucket>, start: usize) -> Self {
        Self { bucket, start }
    }
    pub fn bucket(&self) -> &Arc<FrozenBucket> {
        &self.bucket
    }
    pub const fn start(&self) -> usize {
        self.start
    }
    pub fn members(&self) -> &[Arc<DeleteFact>] {
        &self.bucket.members()[self.start..]
    }
    fn descriptor_variable_bytes(&self) -> usize {
        self.bucket.descriptor_variable_suffix_bytes(self.start)
    }
    pub fn same_representation(&self, other: &Self) -> bool {
        self.start == other.start && Arc::ptr_eq(&self.bucket, &other.bucket)
    }
}

#[derive(Clone, Debug)]
pub enum PositionSource {
    None,
    Files(Vec<BucketView>),
    OneDv(Arc<DeleteFact>),
}

/// Logical unpruned content/application view. Construction never walks or
/// hashes all suffix members. Cross-representation comparisons explicitly pay
/// their linear temporary-set cost; no interner retains historical file sets.
#[derive(Clone, Debug)]
pub struct DeleteSet {
    domain: Arc<ReadDomain>,
    data: Arc<DataFileFact>,
    position: PositionSource,
    equality: Vec<BucketView>,
    cost: CandidateLookupCost,
}

impl DeleteSet {
    pub(crate) fn new(
        domain: Arc<ReadDomain>,
        data: Arc<DataFileFact>,
        position: PositionSource,
        equality: Vec<BucketView>,
        cost: CandidateLookupCost,
    ) -> Self {
        Self {
            domain,
            data,
            position,
            equality,
            cost,
        }
    }
    pub fn domain(&self) -> &Arc<ReadDomain> {
        &self.domain
    }
    pub fn data(&self) -> &DataFileFact {
        &self.data
    }
    pub fn position(&self) -> &PositionSource {
        &self.position
    }
    pub fn equality(&self) -> &[BucketView] {
        &self.equality
    }
    pub const fn lookup_cost(&self) -> CandidateLookupCost {
        self.cost
    }
    /// Counts application descriptions, including raw identical entry repeats.
    pub const fn member_count(&self) -> usize {
        self.cost.candidate_members
    }
    pub fn members(&self) -> impl Iterator<Item = &Arc<DeleteFact>> {
        let position: Box<dyn Iterator<Item = &Arc<DeleteFact>> + '_> = match &self.position {
            PositionSource::None => Box::new(std::iter::empty()),
            PositionSource::OneDv(dv) => Box::new(std::iter::once(dv)),
            PositionSource::Files(views) => Box::new(views.iter().flat_map(BucketView::members)),
        };
        position.chain(self.equality.iter().flat_map(BucketView::members))
    }

    pub fn same_representation(&self, other: &Self) -> bool {
        let positions = match (&self.position, &other.position) {
            (PositionSource::None, PositionSource::None) => true,
            (PositionSource::OneDv(a), PositionSource::OneDv(b)) => {
                a.application() == b.application()
            }
            (PositionSource::Files(a), PositionSource::Files(b)) => views_equal(a, b),
            _ => false,
        };
        positions && views_equal(&self.equality, &other.equality)
    }

    /// Exact address-set identity, deliberately ignoring application facts.
    /// This alone is insufficient to skip a From/To visibility difference.
    pub fn same_addresses(&self, other: &Self) -> bool {
        self.same_addresses_with_hasher(other, RandomState::new())
    }

    pub(crate) fn same_addresses_with_hasher<S: BuildHasher + Clone>(
        &self,
        other: &Self,
        hasher: S,
    ) -> bool {
        if self.same_representation(other) {
            return true;
        }
        let mut left = HashSet::with_hasher(hasher.clone());
        let mut right = HashSet::with_hasher(hasher);
        left.extend(self.members().map(|fact| fact.address()));
        right.extend(other.members().map(|fact| fact.address()));
        left == right
    }

    /// Endpoint comparison: equal addresses AND equal execution interpretations.
    /// Domain IDs are intentionally not compared: different snapshots can have
    /// an unchanged logical effect. Metrics/provenance cannot manufacture events.
    pub fn same_applications(&self, other: &Self) -> bool {
        if self.same_representation(other) {
            return true;
        }
        let left = self
            .members()
            .map(|fact| fact.application())
            .collect::<HashSet<_>>();
        let right = other
            .members()
            .map(|fact| fact.application())
            .collect::<HashSet<_>>();
        left == right
    }

    /// Selection uses only known suffix sizes/group widths and data metrics.
    /// No member is visited merely to decide whether statistics are affordable.
    pub fn load_view(&self, policy: StatisticsPolicy) -> LoadView {
        let candidate_members = self.member_count();
        let estimated_comparisons = self
            .equality
            .iter()
            .fold(0usize, |sum, view| {
                let BucketKind::Equality(key) = view.bucket().kind() else {
                    unreachable!("equality view")
                };
                sum.saturating_add(
                    view.members()
                        .len()
                        .saturating_mul(key.fields.fields().len()),
                )
            })
            .saturating_add(match &self.position {
                PositionSource::Files(views) => views.iter().map(|v| v.members().len()).sum(),
                _ => 0,
            });
        let decision = match policy {
            StatisticsPolicy::Disabled => StatisticsDecision::Disabled,
            StatisticsPolicy::MetadataBudget {
                max_candidate_members,
                max_field_comparisons,
            } if candidate_members > max_candidate_members
                || estimated_comparisons > max_field_comparisons =>
            {
                StatisticsDecision::MetadataBudgetExceeded
            }
            StatisticsPolicy::MetadataBudget { .. } => StatisticsDecision::Applied,
        };
        let mut cost = StatisticsCost {
            candidate_members,
            estimated_comparisons,
            ..Default::default()
        };
        let mut excluded_descriptor_variable_bytes = 0usize;
        let exclusions = if decision == StatisticsDecision::Applied {
            let mut excluded = Vec::new();
            for (index, fact) in self.members().enumerate() {
                let (may_match, comparisons) = delete_may_match(&self.data, fact);
                cost.visited_members += 1;
                cost.field_comparisons += comparisons;
                if !may_match {
                    excluded.push(index);
                    excluded_descriptor_variable_bytes = excluded_descriptor_variable_bytes
                        .saturating_add(fact.descriptor_variable_bytes());
                }
            }
            cost.temporary_exclusion_capacity_bytes = excluded
                .capacity()
                .saturating_mul(std::mem::size_of::<usize>());
            Exclusions::new(candidate_members, excluded)
        } else {
            Exclusions::None
        };
        cost.excluded_members = exclusions.len();
        cost.retained_exclusion_bytes = exclusions.retained_bytes();
        LoadView {
            logical: self.clone(),
            exclusions,
            decision,
            cost,
            excluded_descriptor_variable_bytes,
        }
    }
}

fn views_equal(a: &[BucketView], b: &[BucketView]) -> bool {
    a.len() == b.len() && a.iter().zip(b).all(|(a, b)| a.same_representation(b))
}

/// An explicit planner policy, not an admission limit. Exceeding either budget
/// preserves the complete suffix and performs no statistics comparisons.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StatisticsPolicy {
    Disabled,
    MetadataBudget {
        max_candidate_members: usize,
        max_field_comparisons: usize,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StatisticsDecision {
    Disabled,
    MetadataBudgetExceeded,
    Applied,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct StatisticsCost {
    pub candidate_members: usize,
    pub estimated_comparisons: usize,
    pub visited_members: usize,
    pub field_comparisons: usize,
    pub excluded_members: usize,
    pub retained_exclusion_bytes: usize,
    /// Temporary sparse builder capacity; bitmap conversion also allocates
    /// its destination. This is not a total allocator/RSS peak measurement.
    pub temporary_exclusion_capacity_bytes: usize,
}

#[derive(Clone, Debug)]
enum Exclusions {
    None,
    Sparse(Arc<[usize]>),
    Bitmap { words: Arc<[u64]>, count: usize },
}

impl Exclusions {
    fn new(member_count: usize, excluded: Vec<usize>) -> Self {
        if excluded.is_empty() {
            return Self::None;
        }
        let words = member_count.div_ceil(64);
        if excluded.len().saturating_mul(std::mem::size_of::<usize>())
            <= words.saturating_mul(std::mem::size_of::<u64>())
        {
            Self::Sparse(excluded.into())
        } else {
            let mut bitmap = vec![0u64; words];
            for index in &excluded {
                bitmap[index / 64] |= 1 << (index % 64);
            }
            Self::Bitmap {
                words: bitmap.into(),
                count: excluded.len(),
            }
        }
    }
    fn contains(&self, index: usize) -> bool {
        match self {
            Self::None => false,
            Self::Sparse(indices) => indices.binary_search(&index).is_ok(),
            Self::Bitmap { words, .. } => words[index / 64] & (1 << (index % 64)) != 0,
        }
    }
    fn len(&self) -> usize {
        match self {
            Self::None => 0,
            Self::Sparse(indices) => indices.len(),
            Self::Bitmap { count, .. } => *count,
        }
    }
    fn retained_bytes(&self) -> usize {
        match self {
            Self::None => 0,
            Self::Sparse(indices) => std::mem::size_of_val(indices.as_ref()),
            Self::Bitmap { words, .. } => std::mem::size_of_val(words.as_ref()),
        }
    }
}

/// Required members for one file. Extra safely pruned members in its bucket
/// may later be loaded by siblings, but must not affect the file's row result.
#[derive(Clone, Debug)]
pub struct LoadView {
    logical: DeleteSet,
    exclusions: Exclusions,
    decision: StatisticsDecision,
    cost: StatisticsCost,
    excluded_descriptor_variable_bytes: usize,
}

impl LoadView {
    pub fn logical(&self) -> &DeleteSet {
        &self.logical
    }
    pub const fn decision(&self) -> StatisticsDecision {
        self.decision
    }
    pub const fn cost(&self) -> StatisticsCost {
        self.cost
    }
    pub fn member_count(&self) -> usize {
        self.logical.member_count() - self.exclusions.len()
    }
    /// A representation-independent scheduling charge for inline wire expansion.
    /// Prefix aggregates make unpruned suffix accounting proportional to buckets.
    pub fn expanded_descriptor_bytes(&self, descriptor_fixed_bytes: usize) -> usize {
        let positions = match self.logical.position() {
            PositionSource::None => 0,
            PositionSource::OneDv(fact) => fact.descriptor_variable_bytes(),
            PositionSource::Files(views) => views.iter().fold(0usize, |sum, view| {
                sum.saturating_add(view.descriptor_variable_bytes())
            }),
        };
        let variable = self.logical.equality().iter().fold(positions, |sum, view| {
            sum.saturating_add(view.descriptor_variable_bytes())
        });
        variable
            .saturating_sub(self.excluded_descriptor_variable_bytes)
            .saturating_add(self.member_count().saturating_mul(descriptor_fixed_bytes))
    }
    pub fn members(&self) -> impl Iterator<Item = &Arc<DeleteFact>> {
        self.logical
            .members()
            .enumerate()
            .filter_map(|(index, fact)| (!self.exclusions.contains(index)).then_some(fact))
    }
}
