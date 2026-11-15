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

//! Query-local association between a logical write target and its writer
//! output.
//!
//! A [`WriteTargetOrdinal`] is not an operation id, a cohort id, a writer uuid,
//! a recovery token, or a catalog authority. It exists only inside one sealed
//! query plan, and it says exactly one thing: which logical writer handle a
//! commit fragment belongs to.

use std::collections::BTreeSet;

pub use novarocks_connector_contract::WriteTargetOrdinal;

use crate::connector::write_stack::limits::MAX_CONNECTOR_WRITE_TARGETS;
use crate::connector::{ConnectorError, ConnectorErrorKind};

/// Validate one *query's* expected target set: non-empty, inside the frozen
/// target bound, and free of duplicates.
///
/// This is deliberately weaker than [`validate_dense_target_ordinals`], and the
/// two must not be merged. Denseness from zero is a property of the *session's*
/// sealed target set, not of any single query. A query may compile exactly one
/// writer at its own ordinal within the same sealed session. Query `k` therefore
/// legitimately expects `[k]`, which is not dense from zero and never will be. What a query can still
/// assert is that it feeds at least one target, names none of them twice, and
/// stays inside the bound the session sealed -- the caller supplies the sealed
/// set that bounds it.
pub fn validate_query_target_ordinals(
    ordinals: &[WriteTargetOrdinal],
) -> Result<(), ConnectorError> {
    validate_target_ordinal_cardinality(ordinals)?;
    if ordinals.iter().copied().collect::<BTreeSet<_>>().len() != ordinals.len() {
        return Err(ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            "connector write query repeats a logical write target ordinal",
        ));
    }
    Ok(())
}

/// Validate that `ordinals` form the dense set `0..ordinals.len()` with no gap
/// and no duplicate. A sparse or duplicated set means the begin session and the
/// sealed plan disagree about which logical writers exist, so it fails closed
/// before any fragment submission.
///
/// This belongs to the *session*: `begin_write` seals every logical target the
/// statement may use at once, so their ordinals are the complete `0..n`. A
/// single query inside that session sees only the subset its own writers feed
/// and must be validated with [`validate_query_target_ordinals`] instead.
pub fn validate_dense_target_ordinals(
    ordinals: &[WriteTargetOrdinal],
) -> Result<(), ConnectorError> {
    validate_target_ordinal_cardinality(ordinals)?;
    let unique = ordinals.iter().copied().collect::<BTreeSet<_>>();
    if unique.len() != ordinals.len() {
        return Err(ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            "connector write session repeats a logical write target ordinal",
        ));
    }
    let dense = unique
        .iter()
        .enumerate()
        .all(|(index, ordinal)| u32::try_from(index).is_ok_and(|index| index == ordinal.get()));
    if !dense {
        return Err(ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            "connector write session target ordinals are not dense from zero",
        ));
    }
    Ok(())
}

/// The cardinality rules both target-set checks share: a write with no target
/// writes nothing, and the frozen bound caps how many a session may seal.
fn validate_target_ordinal_cardinality(
    ordinals: &[WriteTargetOrdinal],
) -> Result<(), ConnectorError> {
    if ordinals.is_empty() {
        return Err(ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            "connector write session requires at least one logical write target",
        ));
    }
    if ordinals.len() > MAX_CONNECTOR_WRITE_TARGETS {
        return Err(ConnectorError::new(
            ConnectorErrorKind::ResourceExhausted,
            "connector write session exceeds the frozen logical write target bound",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ordinals(values: &[u32]) -> Vec<WriteTargetOrdinal> {
        values
            .iter()
            .map(|value| WriteTargetOrdinal::try_new(*value).expect("bounded ordinal"))
            .collect()
    }

    #[test]
    fn ordinal_is_bounded_by_the_frozen_target_limit() {
        assert!(WriteTargetOrdinal::try_new(0).is_ok());
        let last = u32::try_from(MAX_CONNECTOR_WRITE_TARGETS - 1).expect("bounded");
        assert!(WriteTargetOrdinal::try_new(last).is_ok());
        let over = u32::try_from(MAX_CONNECTOR_WRITE_TARGETS).expect("bounded");
        assert!(WriteTargetOrdinal::try_new(over).is_err());
    }

    #[test]
    fn dense_target_ordinals_are_accepted() {
        assert!(validate_dense_target_ordinals(&ordinals(&[0, 1, 2])).is_ok());
        assert!(validate_dense_target_ordinals(&ordinals(&[2, 0, 1])).is_ok());
    }

    #[test]
    fn empty_sparse_and_duplicate_target_ordinals_fail_closed() {
        assert!(validate_dense_target_ordinals(&[]).is_err());
        assert!(validate_dense_target_ordinals(&ordinals(&[0, 2])).is_err());
        assert!(validate_dense_target_ordinals(&ordinals(&[0, 0])).is_err());
        assert!(validate_dense_target_ordinals(&ordinals(&[1, 2])).is_err());
    }

    /// A query within a sealed session may compile exactly one writer at
    /// ordinal `k`. Its expected set is `[k]`, which the session-level check
    /// rejects and the query-level check must accept.
    #[test]
    fn a_single_writer_query_at_a_non_zero_ordinal_is_accepted() {
        assert!(validate_query_target_ordinals(&ordinals(&[3])).is_ok());
        assert!(
            validate_dense_target_ordinals(&ordinals(&[3])).is_err(),
            "the session-level check must stay exactly as strict as it was"
        );
        // A gap is likewise a query fact, not a session one: a merge-on-read
        // statement can feed the delete branch without feeding the data branch.
        assert!(validate_query_target_ordinals(&ordinals(&[0, 2])).is_ok());
    }

    #[test]
    fn an_empty_or_duplicated_query_target_set_still_fails_closed() {
        assert!(validate_query_target_ordinals(&[]).is_err());
        assert!(validate_query_target_ordinals(&ordinals(&[1, 1])).is_err());
    }
}
