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

//! The dynamic filter contract shared by split enumeration and page sources.
//!
//! A dynamic filter is live state read through an interface; it never becomes
//! part of a table handle, a split, a scheduled split, or the wire, and it has
//! no identity, version, or digest.

use std::collections::BTreeSet;
use std::fmt::Debug;

use super::predicate::TupleDomain;
use super::value::ConnectorValue;

/// What one column's statistics say about a row group, page, or file.
///
/// Every field is optional because a writer may omit any of them, and the
/// consumer has to stay correct when it does.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ColumnValueBounds {
    pub min: Option<ConnectorValue>,
    pub max: Option<ConnectorValue>,
    pub null_count: Option<u64>,
    pub value_count: Option<u64>,
    /// Whether the min/max are known to be exact and comparable. A deprecated
    /// or unknown-sort-order statistic must set this to false.
    pub bounds_are_exact: bool,
}

/// The answer to "can anything in these bounds satisfy the filter".
///
/// `Unknown` is the safe answer and the default: a filter that cannot decide
/// must never cause a prune, because a wrong prune silently returns fewer rows
/// while a wrong keep only costs work.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BoundsMatch {
    Possible,
    Impossible,
    Unknown,
}

/// An immutable observation of a dynamic filter at one instant.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DynamicFilterSnapshot<C: Ord + Clone + Debug> {
    current_predicate: TupleDomain<C>,
    complete: bool,
    empty_content_demand: bool,
}

impl<C: Ord + Clone + Debug> DynamicFilterSnapshot<C> {
    pub const fn new(current_predicate: TupleDomain<C>, complete: bool) -> Self {
        Self {
            current_predicate,
            complete,
            empty_content_demand: false,
        }
    }

    /// Runtime-filter feedback proved that the content application requests
    /// no target rows. An ordinary contradictory filter cannot author this.
    pub fn empty_content_demand() -> Self {
        Self {
            current_predicate: TupleDomain::none(),
            complete: true,
            empty_content_demand: true,
        }
    }

    pub const fn is_empty_content_demand(&self) -> bool {
        self.empty_content_demand
    }

    /// The unconstrained, final snapshot used when no feedback exists.
    pub fn all_complete() -> Self {
        Self::new(TupleDomain::all(), true)
    }

    pub const fn current_predicate(&self) -> &TupleDomain<C> {
        &self.current_predicate
    }

    pub const fn is_complete(&self) -> bool {
        self.complete
    }
}

/// Live dynamic-filter state a connector may consult repeatedly.
pub trait DynamicFilter<C: Ord + Clone + Debug>: Send + Sync {
    /// The columns this filter can ever constrain.
    fn columns_covered(&self) -> &BTreeSet<C>;

    /// The predicate as of now. Must never become less restrictive over time
    /// in a way that would retroactively invalidate an earlier decision.
    fn current_predicate(&self) -> TupleDomain<C>;

    /// Whether the predicate can still tighten.
    fn is_complete(&self) -> bool;

    /// Whether a caller may usefully wait for a tighter predicate.
    ///
    /// A filter that reports `false` must never be waited on; reporting `true`
    /// while no feedback exists would fabricate a blocked scan.
    fn is_awaitable(&self) -> bool;

    /// Whether the filter is currently withholding a decision.
    fn is_blocked(&self) -> bool {
        false
    }

    fn snapshot(&self) -> DynamicFilterSnapshot<C> {
        DynamicFilterSnapshot::new(self.current_predicate(), self.is_complete())
    }

    /// Ask whether any value within these bounds could satisfy the filter.
    ///
    /// This exists because a runtime filter is not always an enumerable set.
    /// NovaRocks' runtime-filter artifact is a predicate oracle that can answer
    /// this question exactly but cannot produce the values behind it, so a
    /// filter that reported only [`Self::current_predicate`] would have to
    /// widen every column to "all" and could never prune. Asking the question
    /// directly keeps the pruning capability without turning the artifact into
    /// an enumerable domain.
    ///
    /// The default never prunes, which is what an unconstrained filter should
    /// do.
    fn bounds_may_match(&self, column: &C, bounds: &ColumnValueBounds) -> BoundsMatch {
        let _ = (column, bounds);
        BoundsMatch::Unknown
    }
}

/// The truthful filter used where no runtime feedback is produced.
///
/// It reports the real covered columns, an unconstrained predicate, and a
/// complete, non-awaitable, non-blocked state.
#[derive(Clone, Debug)]
pub struct CompleteAllDynamicFilter<C: Ord + Clone + Debug> {
    columns_covered: BTreeSet<C>,
}

impl<C: Ord + Clone + Debug> CompleteAllDynamicFilter<C> {
    pub const fn new(columns_covered: BTreeSet<C>) -> Self {
        Self { columns_covered }
    }
}

impl<C: Ord + Clone + Debug + Send + Sync> DynamicFilter<C> for CompleteAllDynamicFilter<C> {
    fn columns_covered(&self) -> &BTreeSet<C> {
        &self.columns_covered
    }

    fn current_predicate(&self) -> TupleDomain<C> {
        TupleDomain::all()
    }

    fn is_complete(&self) -> bool {
        true
    }

    fn is_awaitable(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_filter_that_cannot_decide_never_prunes() {
        let filter = CompleteAllDynamicFilter::new(BTreeSet::from([1_u32]));
        assert_eq!(
            filter.bounds_may_match(&1, &ColumnValueBounds::default()),
            BoundsMatch::Unknown
        );
    }

    #[test]
    fn the_no_feedback_filter_is_truthful_and_never_awaitable() {
        let mut covered = BTreeSet::new();
        covered.insert(7_u32);
        let filter = CompleteAllDynamicFilter::new(covered);
        assert!(filter.columns_covered().contains(&7));
        assert!(filter.current_predicate().is_all());
        assert!(filter.is_complete());
        assert!(!filter.is_awaitable());
        assert!(!filter.is_blocked());
        assert!(filter.snapshot().is_complete());
    }
}
