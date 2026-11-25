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

/// One batch of splits, plus whether enumeration has finished.
#[derive(Debug)]
pub struct ConnectorSplitBatch<S> {
    splits: Vec<S>,
    no_more_splits: bool,
}

impl<S> ConnectorSplitBatch<S> {
    pub const fn new(splits: Vec<S>, no_more_splits: bool) -> Self {
        Self {
            splits,
            no_more_splits,
        }
    }

    /// An empty batch that does not finish the source.
    pub const fn empty() -> Self {
        Self {
            splits: Vec::new(),
            no_more_splits: false,
        }
    }

    /// The final, empty batch.
    pub const fn finished() -> Self {
        Self {
            splits: Vec::new(),
            no_more_splits: true,
        }
    }

    pub fn splits(&self) -> &[S] {
        &self.splits
    }

    pub fn into_splits(self) -> Vec<S> {
        self.splits
    }

    /// An empty batch is not end of enumeration unless this is set.
    pub const fn no_more_splits(&self) -> bool {
        self.no_more_splits
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
    fn a_batch_can_carry_splits_and_finish_at_once() {
        let batch = ConnectorSplitBatch::new(vec![1_u32, 2], true);
        assert_eq!(batch.splits().len(), 2);
        assert!(batch.no_more_splits());
    }
}
