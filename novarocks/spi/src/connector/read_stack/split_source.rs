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

//! Lazy split enumeration.
//!
//! A split source is a per-scan, per-attempt owned resource. It produces at
//! most one outstanding batch at a time, and closing it is idempotent and may
//! race with an outstanding request.

use std::fmt::Debug;

use crate::connector::ConnectorError;

use super::dynamic_filter::DynamicFilterSnapshot;

/// The reason an exact file inventory did not produce splits for a file.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SplitPruningBasis {
    StaticPredicate,
    PartitionSelection,
    DynamicFilter,
    EmptyDemand,
    UnsatisfiablePredicate,
}

impl SplitPruningBasis {
    const ALL: [Self; 5] = [
        Self::StaticPredicate,
        Self::PartitionSelection,
        Self::DynamicFilter,
        Self::EmptyDemand,
        Self::UnsatisfiablePredicate,
    ];

    const fn index(self) -> usize {
        match self {
            Self::StaticPredicate => 0,
            Self::PartitionSelection => 1,
            Self::DynamicFilter => 2,
            Self::EmptyDemand => 3,
            Self::UnsatisfiablePredicate => 4,
        }
    }
}

/// An order-independent digest of the file identities attributed to one reason.
/// The digest describes the inventory. It does not decide file eligibility.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PrunedSplitFiles {
    pub file_count: u64,
    pub identity_digest: [u8; 32],
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SplitPruningLedger {
    files: [PrunedSplitFiles; 5],
    /// Ordinary scans may prove an empty predicate without making an exact
    /// file inventory. Such a proof cannot stand in for the inventory.
    unenumerated_unsatisfiable: bool,
    /// An ordinary scan can prune ranges from an already partially assigned
    /// file. The whole-file ledger cannot describe those remaining ranges.
    unaccounted_split_remainder: bool,
}

impl SplitPruningLedger {
    pub fn record_file(
        &mut self,
        basis: SplitPruningBasis,
        identity_digest: [u8; 32],
    ) -> Result<(), ConnectorError> {
        let files = &mut self.files[basis.index()];
        files.file_count = files.file_count.checked_add(1).ok_or_else(|| {
            ConnectorError::new(
                crate::connector::ConnectorErrorKind::CorruptData,
                "Split pruning file count overflow",
            )
        })?;
        // Addition modulo 2^256 preserves order-independence without
        // retaining all identities or cancelling identical contributions.
        let mut carry = 0_u16;
        for (sum, input) in files.identity_digest.iter_mut().zip(identity_digest).rev() {
            let next = u16::from(*sum) + u16::from(input) + carry;
            *sum = next as u8;
            carry = next >> 8;
        }
        Ok(())
    }

    pub fn complete_file_inventory(&self) -> bool {
        !self.unenumerated_unsatisfiable && !self.unaccounted_split_remainder
    }

    pub fn mark_unenumerated_unsatisfiable(&mut self) {
        self.unenumerated_unsatisfiable = true;
    }

    pub fn mark_unaccounted_split_remainder(&mut self) {
        self.unaccounted_split_remainder = true;
    }

    pub fn entries(&self) -> impl Iterator<Item = (SplitPruningBasis, PrunedSplitFiles)> + '_ {
        SplitPruningBasis::ALL
            .into_iter()
            .zip(self.files)
            .filter(|(basis, files)| {
                files.file_count != 0
                    || (*basis == SplitPruningBasis::UnsatisfiablePredicate
                        && self.unenumerated_unsatisfiable)
            })
    }
}

#[derive(Debug)]
pub enum SplitEnumeration {
    More,
    NaturalEnd(Box<SplitPruningLedger>),
    Closed,
}

/// One batch of splits. Closing a source can settle delivery without proving
/// that the original enumeration reached a natural end.
#[derive(Debug)]
pub struct ConnectorSplitBatch<S> {
    splits: Vec<S>,
    enumeration: SplitEnumeration,
}

impl<S> ConnectorSplitBatch<S> {
    pub fn new(splits: Vec<S>, no_more_splits: bool) -> Self {
        if no_more_splits {
            Self::natural_end(splits, SplitPruningLedger::default())
        } else {
            Self {
                splits,
                enumeration: SplitEnumeration::More,
            }
        }
    }

    /// An empty batch that does not finish the source.
    pub const fn empty() -> Self {
        Self {
            splits: Vec::new(),
            enumeration: SplitEnumeration::More,
        }
    }

    /// The final, empty batch from a naturally exhausted source.
    pub fn finished() -> Self {
        Self::natural_end(Vec::new(), SplitPruningLedger::default())
    }

    pub fn natural_end(splits: Vec<S>, pruning: SplitPruningLedger) -> Self {
        Self {
            splits,
            enumeration: SplitEnumeration::NaturalEnd(Box::new(pruning)),
        }
    }

    pub const fn closed() -> Self {
        Self {
            splits: Vec::new(),
            enumeration: SplitEnumeration::Closed,
        }
    }

    pub fn splits(&self) -> &[S] {
        &self.splits
    }

    pub fn into_splits(self) -> Vec<S> {
        self.splits
    }

    pub fn into_parts(self) -> (Vec<S>, SplitEnumeration) {
        (self.splits, self.enumeration)
    }

    pub fn map_splits<T>(self, map: impl FnMut(S) -> T) -> ConnectorSplitBatch<T> {
        ConnectorSplitBatch {
            splits: self.splits.into_iter().map(map).collect(),
            enumeration: self.enumeration,
        }
    }

    /// An empty batch is not the end of delivery unless this is set.
    pub const fn no_more_splits(&self) -> bool {
        !matches!(self.enumeration, SplitEnumeration::More)
    }

    pub fn is_empty(&self) -> bool {
        self.splits.is_empty()
    }
}

/// A connector-owned, lazily advancing split enumerator.
pub trait ConnectorSplitSource: Send {
    type Split;
    type Column: Ord + Clone + Debug;

    /// Immutable, monotonically increasing enumeration facts.  They describe
    /// only split production, never backend I/O or page-source effects.
    fn profile_snapshot(&self) -> SplitSourceProfile {
        SplitSourceProfile::default()
    }

    /// Produce up to `max_size` splits.
    ///
    /// The caller keeps at most one outstanding request. An empty batch means
    /// "nothing right now" unless it is marked final.
    ///
    /// An explicit change-window read and a prepared delta both enumerate the
    /// complete frozen difference regardless of the dynamic filter. Successful
    /// enumeration is the active attempt's consumption of all batches through
    /// the natural final batch. A proven zero contribution remains a separate
    /// preparation fact.
    fn next_batch(
        &mut self,
        max_size: usize,
        dynamic_filter: &DynamicFilterSnapshot<Self::Column>,
    ) -> Result<ConnectorSplitBatch<Self::Split>, ConnectorError>;

    fn is_finished(&self) -> bool;

    /// Idempotent. A normal batch that completed before the close may still be
    /// delivered to the caller that requested it. Closing or canceling an attempt
    /// provides no evidence that the enumeration was consumed.
    fn close(&mut self) -> Result<(), ConnectorError>;
}

/// Provider-neutral facts collected while a source decides which unexpanded
/// units become splits. `files_pruned` counts only files ruled out by a
/// completed dynamic-filter snapshot, not files the static predicate already
/// excluded. It is an avoided-work estimate, not bytes read.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SplitSourceProfile {
    pub files_considered: u64,
    pub files_pruned: u64,
    pub files_expanded: u64,
    pub splits_emitted: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_empty_batch_is_not_the_end_of_enumeration() {
        let batch = ConnectorSplitBatch::<u32>::empty();
        assert!(batch.is_empty());
        assert!(!batch.no_more_splits());
        let finished = ConnectorSplitBatch::<u32>::finished();
        assert!(finished.is_empty());
        assert!(finished.no_more_splits());
    }

    #[test]
    fn mapping_preserves_natural_end_and_early_close() {
        let mut pruning = SplitPruningLedger::default();
        pruning
            .record_file(SplitPruningBasis::DynamicFilter, [1; 32])
            .unwrap();
        let source = ConnectorSplitBatch::natural_end(vec![1_u32], pruning.clone());
        let (splits, end) = source.map_splits(u64::from).into_parts();
        assert_eq!(splits, vec![1_u64]);
        assert!(matches!(end, SplitEnumeration::NaturalEnd(value) if *value == pruning));
        let closed = ConnectorSplitBatch::<u32>::closed().map_splits(u64::from);
        assert!(closed.no_more_splits());
        assert!(matches!(closed.into_parts().1, SplitEnumeration::Closed));
    }

    #[test]
    fn a_batch_can_carry_splits_and_finish_at_once() {
        let batch = ConnectorSplitBatch::new(vec![1_u32, 2], true);
        assert_eq!(batch.splits().len(), 2);
        assert!(batch.no_more_splits());
    }
}
