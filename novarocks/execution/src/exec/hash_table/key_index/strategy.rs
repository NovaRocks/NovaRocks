// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Whole-input strategy selection precedes routing. Cost models neither grant
//! memory nor recover from real resource/control failures by changing strategy.

use super::{
    NullPolicy,
    bulk::{CompleteStats, NdvHint},
    direct_int::{DirectCosts, DirectGeometry},
    hash_layout::HashLayout,
    table::{IntegerHashCosts, integer_hash_costs},
};
use crate::runtime::fragment::ExecutionResult;
use std::num::NonZeroUsize;

#[derive(Clone, Copy, Debug)]
pub(super) enum StrategyKind {
    Empty,
    Direct(DirectGeometry),
    Hash(HashLayout),
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum StrategyReason {
    ExactEmptyIndexableDomain,
    NonIntegerDomain,
    MissingNdvHint,
    DirectLayoutUnrepresentable,
    CostModelUnavailable,
    DirectKnownStructureCheaper,
    HashKnownStructureCheaper,
}
#[derive(Clone, Copy, Debug)]
pub(super) struct CostComparison {
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "Retains the original selector cost comparison and explicit unmodeled terms; not a physical bound."
        )
    )]
    pub(super) direct: DirectCosts,
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "Retains the original selector cost comparison and explicit unmodeled terms; not a physical bound."
        )
    )]
    pub(super) hash: IntegerHashCosts,
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "Retains the original selector cost comparison and explicit unmodeled terms; not a physical bound."
        )
    )]
    pub(super) direct_modeled_bytes: u128,
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "Retains the original selector cost comparison and explicit unmodeled terms; not a physical bound."
        )
    )]
    pub(super) hash_modeled_bytes: u128,
    /// Deliberately use the least fragmented modeled hash endpoint. This is a
    /// cheap reference, not a claim that all hash keys occupy one actual page.
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "Retains the original selector cost comparison and explicit unmodeled terms; not a physical bound."
        )
    )]
    pub(super) hash_batches_modeled: usize,
    /// Original custody, library padding and capacity slack remain unmodeled
    /// on both sides. The comparison is no physical bound or performance claim.
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "Retains the original selector cost comparison and explicit unmodeled terms; not a physical bound."
        )
    )]
    pub(super) has_unmodeled_terms: bool,
}
#[derive(Clone, Copy, Debug)]
pub(super) struct StrategyDecision {
    pub(super) kind: StrategyKind,
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "Retains why the original selector chose a layout and its actual modeled/unknown cost facts."
        )
    )]
    pub(super) reason: StrategyReason,
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "Retains why the original selector chose a layout and its actual modeled/unknown cost facts."
        )
    )]
    pub(super) comparison: Option<CostComparison>,
}

/// A missing hint means no direct optimization. The present hint must be this
/// complete input's own observation; names, counts or seeds cannot recreate it.
pub(super) fn choose_layout(
    stats: &CompleteStats,
    hint: Option<&NdvHint>,
    workers: NonZeroUsize,
    checkpoint: &dyn Fn() -> ExecutionResult<()>,
) -> ExecutionResult<StrategyDecision> {
    stats.check()?;
    checkpoint()?;
    if let Some(hint) = hint {
        hint.validate(stats.domain(), stats.exact())?;
    }
    let decision = choose_layout_inner(stats, hint, workers)?;
    stats.check()?;
    checkpoint()?;
    Ok(decision)
}
fn choose_layout_inner(
    stats: &CompleteStats,
    hint: Option<&NdvHint>,
    workers: NonZeroUsize,
) -> ExecutionResult<StrategyDecision> {
    let hash = |reason, comparison| StrategyDecision {
        kind: StrategyKind::Hash(HashLayout::for_bulk(
            requested_route_bits(workers),
            stats.exact().indexable_rows(),
        )),
        reason,
        comparison,
    };
    if stats.exact().indexable_rows() == 0 {
        return Ok(StrategyDecision {
            kind: StrategyKind::Empty,
            reason: StrategyReason::ExactEmptyIndexableDomain,
            comparison: None,
        });
    }
    let Some(integer) = stats.exact().integer() else {
        return Ok(hash(StrategyReason::NonIntegerDomain, None));
    };
    let Some(hint) = hint else {
        return Ok(hash(StrategyReason::MissingNdvHint, None));
    };
    let Some(geometry) =
        DirectGeometry::for_selected(stats.domain().spec(), integer, stats.purpose(), workers)
    else {
        return Ok(hash(StrategyReason::DirectLayoutUnrepresentable, None));
    };
    let has_null_class =
        stats.exact().null_rows() != 0 && stats.domain().spec().nulls()[0] == NullPolicy::Equal;
    // This remains an estimated cardinality. Subtract only the independently
    // known NULL class from the estimate; never substitute exact B or B/P.
    let nonnull_estimate = (u128::from(hint.estimate()).saturating_sub(u128::from(has_null_class)))
        .min(stats.exact().indexable_rows() as u128) as usize;
    let Some(direct) = geometry.costs(nonnull_estimate, has_null_class) else {
        return Ok(hash(StrategyReason::CostModelUnavailable, None));
    };
    let Some(hash_cost) = integer_hash_costs(stats)? else {
        return Ok(hash(StrategyReason::CostModelUnavailable, None));
    };
    let batches = *hash_cost.batch_range().start();
    let Some(hash_bytes) = hash_cost.modeled_bytes(batches) else {
        return Ok(hash(StrategyReason::CostModelUnavailable, None));
    };
    let Some(direct_bytes) = direct_total(direct) else {
        return Ok(hash(StrategyReason::CostModelUnavailable, None));
    };
    let comparison = Some(CostComparison {
        direct,
        hash: hash_cost,
        direct_modeled_bytes: direct_bytes,
        hash_modeled_bytes: hash_bytes,
        hash_batches_modeled: batches,
        has_unmodeled_terms: true,
    });
    Ok(if direct_bytes < hash_bytes {
        StrategyDecision {
            kind: StrategyKind::Direct(geometry),
            reason: StrategyReason::DirectKnownStructureCheaper,
            comparison,
        }
    } else {
        hash(StrategyReason::HashKnownStructureCheaper, comparison)
    })
}
fn direct_total(cost: DirectCosts) -> Option<u128> {
    cost.bitmap_bytes
        .checked_add(cost.slot_id_bytes)?
        .checked_add(cost.storage_headers_bytes)?
        .checked_add(cost.count_prefix_peak_bytes)?
        .checked_add(cost.representative_payload_estimate)?
        .checked_add(cost.representative_node_metadata_upper)?
        .checked_add(cost.null_constructor_peak_upper)
}
fn requested_route_bits(workers: NonZeroUsize) -> u32 {
    // Independent partitioning preference, not a data or worker quota. On the
    // representational rounding edge choose the nearest lower power of two.
    workers
        .get()
        .checked_next_power_of_two()
        .map_or(usize::BITS - 1, usize::ilog2)
}
