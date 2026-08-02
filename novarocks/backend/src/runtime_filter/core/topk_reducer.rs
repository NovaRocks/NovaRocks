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

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::sync::Arc;

use novarocks::runtime_filter_transition::model::contract::BindingId;
use novarocks::runtime_filter_transition::port::identity::{ProducerSequence, ProducerStreamId};
use novarocks::runtime_filter_transition::port::ordered_bound::{
    OrderedTuple, RuntimeOrderContract,
};
use novarocks::runtime_filter_transition::port::producer::{
    RuntimeContractViolation, RuntimeContractViolationKind,
};
use novarocks::runtime_filter_transition::port::topk_summary::{
    RuntimeTopKSummaryContract, TopKSummary,
};
use novarocks_types::UniqueId;

use novarocks::runtime_filter_transition::port::value_domain::OrderedBoundDomain;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TopKApplyOutcome {
    Stale,
    Duplicate,
    SequenceAdvancedEqual,
    StreamUpdated,
    GlobalTightened,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TopKCloseOutcome {
    Duplicate,
    PendingFinalSnapshot,
    Satisfied,
}

#[derive(Clone, Debug, Default)]
struct TopKStreamState {
    highest_sequence: Option<ProducerSequence>,
    replay_digest: Option<[u8; 32]>,
    latest_candidates: Option<Arc<[OrderedTuple]>>,
    terminal_sequence: Option<ProducerSequence>,
}

impl TopKStreamState {
    fn terminal_satisfied(&self) -> bool {
        match self.terminal_sequence {
            Some(terminal) if terminal.get() == 0 => self.highest_sequence.is_none(),
            Some(terminal) => terminal.get().checked_sub(1).is_some_and(|final_sequence| {
                self.highest_sequence == Some(ProducerSequence::new(final_sequence))
            }),
            None => false,
        }
    }

    fn estimated_retained_bytes(&self) -> Option<usize> {
        let candidate_bytes = self
            .latest_candidates
            .as_ref()
            .map_or(Some(0), |candidates| {
                candidates.iter().try_fold(0usize, |bytes, candidate| {
                    bytes.checked_add(candidate.estimated_retained_bytes()?)
                })
            })?;
        candidate_bytes
            .checked_add(self.replay_digest.map_or(0, |_| 32))?
            .checked_add(
                self.highest_sequence
                    .map_or(0, |_| size_of::<ProducerSequence>()),
            )?
            .checked_add(
                self.terminal_sequence
                    .map_or(0, |_| size_of::<ProducerSequence>()),
            )
    }
}

#[derive(Debug)]
pub(crate) struct TopKSummaryReducer {
    contract: Arc<RuntimeTopKSummaryContract>,
    streams: BTreeMap<ProducerStreamId, TopKStreamState>,
    global: Option<Arc<OrderedBoundDomain>>,
}

#[derive(Debug)]
pub(crate) struct TopKApplyProjection {
    stream: ProducerStreamId,
    next_stream: TopKStreamState,
    next_global: Option<Arc<OrderedBoundDomain>>,
    outcome: TopKApplyOutcome,
    retained_bytes: usize,
}

impl TopKApplyProjection {
    pub(crate) const fn outcome(&self) -> TopKApplyOutcome {
        self.outcome
    }

    pub(crate) const fn retained_bytes(&self) -> usize {
        self.retained_bytes
    }

    pub(crate) fn global(&self) -> Option<&Arc<OrderedBoundDomain>> {
        self.next_global.as_ref()
    }

    pub(crate) fn stream_covered(&self) -> bool {
        self.next_stream.highest_sequence.is_some() || self.next_stream.terminal_satisfied()
    }
}

#[derive(Debug)]
pub(crate) struct TopKCloseProjection {
    stream: ProducerStreamId,
    next_stream: TopKStreamState,
    outcome: TopKCloseOutcome,
    retained_bytes: usize,
}

impl TopKCloseProjection {
    pub(crate) const fn outcome(&self) -> TopKCloseOutcome {
        self.outcome
    }

    pub(crate) const fn retained_bytes(&self) -> usize {
        self.retained_bytes
    }

    pub(crate) fn stream_covered(&self) -> bool {
        self.next_stream.highest_sequence.is_some() || self.next_stream.terminal_satisfied()
    }
}

impl TopKSummaryReducer {
    pub(crate) fn new(contract: Arc<RuntimeTopKSummaryContract>) -> Self {
        Self {
            contract,
            streams: BTreeMap::new(),
            global: None,
        }
    }

    pub(crate) const fn contract(&self) -> &Arc<RuntimeTopKSummaryContract> {
        &self.contract
    }

    pub(crate) fn preflight_apply(
        &self,
        stream: ProducerStreamId,
        sequence: ProducerSequence,
        summary: &TopKSummary,
    ) -> Result<TopKApplyProjection, RuntimeContractViolation> {
        if summary.contract_digest() != self.contract.digest() {
            return Err(violation(
                RuntimeContractViolationKind::OrderedContractMismatch,
                "top-k summary does not match the installed summary contract",
            ));
        }

        let current = self.streams.get(&stream).cloned().unwrap_or_default();
        if current
            .terminal_sequence
            .is_some_and(|terminal| sequence >= terminal)
        {
            return Err(violation(
                RuntimeContractViolationKind::SequenceOutsideTerminalRange,
                "top-k summary sequence is outside the exclusive terminal range",
            ));
        }

        if let Some(highest) = current.highest_sequence {
            if sequence < highest {
                return self.unchanged_apply_projection(stream, current, TopKApplyOutcome::Stale);
            }
            if sequence == highest {
                if current.replay_digest != Some(summary.replay_digest()) {
                    return Err(violation(
                        RuntimeContractViolationKind::ConflictingReplay,
                        "same top-k producer sequence carried a different cumulative summary",
                    ));
                }
                return self.unchanged_apply_projection(
                    stream,
                    current,
                    TopKApplyOutcome::Duplicate,
                );
            }

            let previous = current.latest_candidates.as_ref().ok_or_else(|| {
                violation(
                    RuntimeContractViolationKind::OrderedBoundLoosened,
                    "top-k candidate state was already released",
                )
            })?;
            validate_transition(
                self.contract.order(),
                self.contract.k().get(),
                previous,
                summary.candidates(),
            )?;
            if previous.as_ref() == summary.candidates() {
                let mut next_stream = current;
                next_stream.highest_sequence = Some(sequence);
                next_stream.replay_digest = Some(summary.replay_digest());
                next_stream.latest_candidates = Some(summary.shared_candidates());
                return self.apply_projection(
                    stream,
                    next_stream,
                    self.global.clone(),
                    TopKApplyOutcome::SequenceAdvancedEqual,
                );
            }
        }

        let mut next_stream = current;
        next_stream.highest_sequence = Some(sequence);
        next_stream.replay_digest = Some(summary.replay_digest());
        next_stream.latest_candidates = Some(summary.shared_candidates());

        let next_bound = self.select_kth(stream, &next_stream)?;
        let (next_global, outcome) = match next_bound {
            None => (None, TopKApplyOutcome::StreamUpdated),
            Some(bound) => {
                let tightened = self.global.as_ref().is_none_or(|previous| {
                    self.contract
                        .order()
                        .compare(&bound, previous.bound())
                        .expect("validated top-k bounds remain comparable")
                        == Ordering::Less
                });
                if tightened {
                    (
                        Some(Arc::new(OrderedBoundDomain::new(
                            self.contract.order().clone(),
                            bound,
                        ))),
                        TopKApplyOutcome::GlobalTightened,
                    )
                } else {
                    (self.global.clone(), TopKApplyOutcome::StreamUpdated)
                }
            }
        };
        self.apply_projection(stream, next_stream, next_global, outcome)
    }

    pub(crate) fn commit_apply(&mut self, projection: TopKApplyProjection) {
        self.streams
            .insert(projection.stream, projection.next_stream);
        self.global = projection.next_global;
    }

    pub(crate) fn preflight_close(
        &self,
        stream: ProducerStreamId,
        terminal: ProducerSequence,
    ) -> Result<TopKCloseProjection, RuntimeContractViolation> {
        let current = self.streams.get(&stream).cloned().unwrap_or_default();
        if let Some(previous) = current.terminal_sequence {
            if previous != terminal {
                return Err(violation(
                    RuntimeContractViolationKind::ConflictingTerminalSequence,
                    "top-k partition close replay changed terminal sequence",
                ));
            }
            return self.close_projection(stream, current, TopKCloseOutcome::Duplicate);
        }
        if current
            .highest_sequence
            .is_some_and(|highest| highest >= terminal)
        {
            return Err(violation(
                RuntimeContractViolationKind::SequenceOutsideTerminalRange,
                "top-k partition already contains a summary outside terminal range",
            ));
        }

        let mut next_stream = current;
        next_stream.terminal_sequence = Some(terminal);
        let outcome = if next_stream.terminal_satisfied() {
            TopKCloseOutcome::Satisfied
        } else {
            TopKCloseOutcome::PendingFinalSnapshot
        };
        self.close_projection(stream, next_stream, outcome)
    }

    pub(crate) fn commit_close(&mut self, projection: TopKCloseProjection) {
        self.streams
            .insert(projection.stream, projection.next_stream);
    }

    pub(crate) fn global(&self) -> Option<&Arc<OrderedBoundDomain>> {
        self.global.as_ref()
    }

    pub(crate) fn estimated_retained_bytes(&self) -> Option<usize> {
        let stream_bytes = self.streams.values().try_fold(0usize, |bytes, stream| {
            bytes.checked_add(stream.estimated_retained_bytes()?)
        })?;
        stream_bytes.checked_add(
            self.global
                .as_ref()
                .map_or(Some(0), |global| global.estimated_retained_bytes())?,
        )
    }

    pub(crate) fn retain_protocol_tombstones(&mut self) -> Option<usize> {
        for stream in self.streams.values_mut() {
            stream.latest_candidates = None;
        }
        self.global = None;
        self.estimated_retained_bytes()
    }

    pub(crate) fn validate_tombstone_summary(
        &self,
        stream: ProducerStreamId,
        sequence: ProducerSequence,
        summary: &TopKSummary,
    ) -> Result<(), RuntimeContractViolation> {
        if summary.contract_digest() != self.contract.digest() {
            return Err(violation(
                RuntimeContractViolationKind::OrderedContractMismatch,
                "top-k summary does not match the installed summary contract",
            ));
        }
        let Some(current) = self.streams.get(&stream) else {
            return Ok(());
        };
        if current
            .terminal_sequence
            .is_some_and(|terminal| sequence >= terminal)
        {
            return Err(violation(
                RuntimeContractViolationKind::SequenceOutsideTerminalRange,
                "top-k summary sequence is outside the exclusive terminal range",
            ));
        }
        if current.highest_sequence == Some(sequence)
            && current.replay_digest != Some(summary.replay_digest())
        {
            return Err(violation(
                RuntimeContractViolationKind::ConflictingReplay,
                "same top-k producer sequence carried a different cumulative summary",
            ));
        }
        Ok(())
    }

    pub(crate) fn submitted_partition_count(
        &self,
        binding_id: BindingId,
        instance: UniqueId,
    ) -> usize {
        self.streams
            .iter()
            .filter(|(stream, state)| {
                stream.binding_id() == binding_id
                    && stream.fragment_instance_id() == instance
                    && state.highest_sequence.is_some()
            })
            .count()
    }

    pub(crate) fn terminal_partition_count(
        &self,
        binding_id: BindingId,
        instance: UniqueId,
    ) -> usize {
        self.streams
            .iter()
            .filter(|(stream, state)| {
                stream.binding_id() == binding_id
                    && stream.fragment_instance_id() == instance
                    && state.terminal_satisfied()
            })
            .count()
    }

    pub(crate) fn covered_partition_count(
        &self,
        binding_id: BindingId,
        instance: UniqueId,
    ) -> usize {
        self.streams
            .iter()
            .filter(|(stream, state)| {
                stream.binding_id() == binding_id
                    && stream.fragment_instance_id() == instance
                    && (state.highest_sequence.is_some() || state.terminal_satisfied())
            })
            .count()
    }

    pub(crate) fn stream_covered(&self, stream: ProducerStreamId) -> bool {
        self.streams
            .get(&stream)
            .is_some_and(|state| state.highest_sequence.is_some() || state.terminal_satisfied())
    }

    fn unchanged_apply_projection(
        &self,
        stream: ProducerStreamId,
        next_stream: TopKStreamState,
        outcome: TopKApplyOutcome,
    ) -> Result<TopKApplyProjection, RuntimeContractViolation> {
        self.apply_projection(stream, next_stream, self.global.clone(), outcome)
    }

    fn apply_projection(
        &self,
        stream: ProducerStreamId,
        next_stream: TopKStreamState,
        next_global: Option<Arc<OrderedBoundDomain>>,
        outcome: TopKApplyOutcome,
    ) -> Result<TopKApplyProjection, RuntimeContractViolation> {
        let retained_bytes =
            self.retained_bytes_with(stream, &next_stream, next_global.as_ref())?;
        Ok(TopKApplyProjection {
            stream,
            next_stream,
            next_global,
            outcome,
            retained_bytes,
        })
    }

    fn close_projection(
        &self,
        stream: ProducerStreamId,
        next_stream: TopKStreamState,
        outcome: TopKCloseOutcome,
    ) -> Result<TopKCloseProjection, RuntimeContractViolation> {
        let retained_bytes =
            self.retained_bytes_with(stream, &next_stream, self.global.as_ref())?;
        Ok(TopKCloseProjection {
            stream,
            next_stream,
            outcome,
            retained_bytes,
        })
    }

    fn retained_bytes_with(
        &self,
        replaced_stream: ProducerStreamId,
        replacement: &TopKStreamState,
        global: Option<&Arc<OrderedBoundDomain>>,
    ) -> Result<usize, RuntimeContractViolation> {
        let mut bytes = 0usize;
        let mut replaced = false;
        for (stream, state) in &self.streams {
            let state = if *stream == replaced_stream {
                replaced = true;
                replacement
            } else {
                state
            };
            bytes = bytes
                .checked_add(
                    state
                        .estimated_retained_bytes()
                        .ok_or_else(size_violation)?,
                )
                .ok_or_else(size_violation)?;
        }
        if !replaced {
            bytes = bytes
                .checked_add(
                    replacement
                        .estimated_retained_bytes()
                        .ok_or_else(size_violation)?,
                )
                .ok_or_else(size_violation)?;
        }
        if let Some(global) = global {
            bytes = bytes
                .checked_add(
                    global
                        .estimated_retained_bytes()
                        .ok_or_else(size_violation)?,
                )
                .ok_or_else(size_violation)?;
        }
        Ok(bytes)
    }

    fn select_kth(
        &self,
        replaced_stream: ProducerStreamId,
        replacement: &TopKStreamState,
    ) -> Result<Option<OrderedTuple>, RuntimeContractViolation> {
        let k = usize::try_from(self.contract.k().get()).map_err(|_| {
            violation(
                RuntimeContractViolationKind::OrderedContractMismatch,
                "top-k contract K does not fit the platform index type",
            )
        })?;
        let mut rank = 0usize;
        let mut previous: Option<&OrderedTuple> = None;
        loop {
            let Some(next) = self.next_distinct(replaced_stream, replacement, previous)? else {
                return Ok(None);
            };
            rank = rank
                .checked_add(self.multiplicity(replaced_stream, replacement, next)?)
                .ok_or_else(size_violation)?;
            if rank >= k {
                return Ok(Some(next.clone()));
            }
            previous = Some(next);
        }
    }

    fn next_distinct<'a>(
        &'a self,
        replaced_stream: ProducerStreamId,
        replacement: &'a TopKStreamState,
        previous: Option<&OrderedTuple>,
    ) -> Result<Option<&'a OrderedTuple>, RuntimeContractViolation> {
        let mut best = None;
        self.for_each_candidates(replaced_stream, replacement, |candidates| {
            for candidate in candidates {
                if previous.is_some_and(|previous| {
                    self.contract
                        .order()
                        .compare(candidate, previous)
                        .expect("validated top-k candidates remain comparable")
                        != Ordering::Greater
                }) {
                    continue;
                }
                if best.is_none_or(|best| {
                    self.contract
                        .order()
                        .compare(candidate, best)
                        .expect("validated top-k candidates remain comparable")
                        == Ordering::Less
                }) {
                    best = Some(candidate);
                }
                break;
            }
        });
        Ok(best)
    }

    fn multiplicity(
        &self,
        replaced_stream: ProducerStreamId,
        replacement: &TopKStreamState,
        selected: &OrderedTuple,
    ) -> Result<usize, RuntimeContractViolation> {
        let mut count = Some(0usize);
        self.for_each_candidates(replaced_stream, replacement, |candidates| {
            let summary_count = candidates
                .iter()
                .filter(|candidate| {
                    self.contract
                        .order()
                        .compare(candidate, selected)
                        .expect("validated top-k candidates remain comparable")
                        == Ordering::Equal
                })
                .count();
            count = count.and_then(|count| count.checked_add(summary_count));
        });
        count.ok_or_else(size_violation)
    }

    fn for_each_candidates<'a>(
        &'a self,
        replaced_stream: ProducerStreamId,
        replacement: &'a TopKStreamState,
        mut visitor: impl FnMut(&'a [OrderedTuple]),
    ) {
        let mut replaced = false;
        for (stream, state) in &self.streams {
            let state = if *stream == replaced_stream {
                replaced = true;
                replacement
            } else {
                state
            };
            if let Some(candidates) = state.latest_candidates.as_ref() {
                visitor(candidates);
            }
        }
        if !replaced && let Some(candidates) = replacement.latest_candidates.as_ref() {
            visitor(candidates);
        }
    }
}

fn validate_transition(
    order: &RuntimeOrderContract,
    k: u32,
    previous: &[OrderedTuple],
    next: &[OrderedTuple],
) -> Result<(), RuntimeContractViolation> {
    if next.len() < previous.len() {
        return Err(loosened(
            "higher top-k sequence shortened its cumulative summary",
        ));
    }

    let k = usize::try_from(k).map_err(|_| {
        violation(
            RuntimeContractViolationKind::OrderedContractMismatch,
            "top-k contract K does not fit the platform index type",
        )
    })?;
    if next.len() < k {
        let mut next_index = 0usize;
        for previous_candidate in previous {
            loop {
                let Some(next_candidate) = next.get(next_index) else {
                    return Err(loosened(
                        "higher top-k sequence dropped a candidate below K",
                    ));
                };
                match order
                    .compare(next_candidate, previous_candidate)
                    .map_err(|_| {
                        violation(
                            RuntimeContractViolationKind::OrderedContractMismatch,
                            "top-k transition tuple does not match the installed order contract",
                        )
                    })? {
                    Ordering::Less => next_index += 1,
                    Ordering::Equal => {
                        next_index += 1;
                        break;
                    }
                    Ordering::Greater => {
                        return Err(loosened(
                            "higher top-k sequence dropped a candidate below K",
                        ));
                    }
                }
            }
        }
    } else {
        for (next_candidate, previous_candidate) in next.iter().zip(previous) {
            if order
                .compare(next_candidate, previous_candidate)
                .map_err(|_| {
                    violation(
                        RuntimeContractViolationKind::OrderedContractMismatch,
                        "top-k transition tuple does not match the installed order contract",
                    )
                })?
                == Ordering::Greater
            {
                return Err(loosened(
                    "higher top-k sequence worsened a retained candidate rank",
                ));
            }
        }
    }
    Ok(())
}

fn loosened(detail: impl Into<String>) -> RuntimeContractViolation {
    violation(RuntimeContractViolationKind::OrderedBoundLoosened, detail)
}

fn size_violation() -> RuntimeContractViolation {
    violation(
        RuntimeContractViolationKind::TypeMismatch,
        "top-k reducer retained byte size overflowed usize",
    )
}

fn violation(
    kind: RuntimeContractViolationKind,
    detail: impl Into<String>,
) -> RuntimeContractViolation {
    RuntimeContractViolation::new(kind, detail)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use arrow::datatypes::DataType;

    use novarocks::runtime_filter_transition::model::contract::{
        BindingId, NullOrder, OrderContract, OrderKeyContract, SortDirection,
        TopKSummaryRequirement,
    };
    use novarocks::runtime_filter_transition::port::identity::{
        PartitionId, ProducerSequence, ProducerStreamId,
    };
    use novarocks::runtime_filter_transition::port::ordered_bound::{
        COMPARATOR_ALGORITHM_VERSION, OrderedScalar, OrderedTuple, comparator_digest_for_test,
    };
    use novarocks::runtime_filter_transition::port::producer::RuntimeContractViolationKind;
    use novarocks::runtime_filter_transition::port::topk_summary::{
        RuntimeTopKSummaryContract, TopKSummary,
    };
    use novarocks_types::UniqueId;

    use super::{TopKApplyOutcome, TopKCloseOutcome, TopKSummaryReducer};

    fn contract(k: u32) -> Arc<RuntimeTopKSummaryContract> {
        contract_with_keys(
            k,
            vec![OrderKeyContract {
                data_type: DataType::Int64,
                direction: SortDirection::Ascending,
                null_order: NullOrder::Last,
            }],
        )
    }

    fn contract_with_keys(k: u32, keys: Vec<OrderKeyContract>) -> Arc<RuntimeTopKSummaryContract> {
        Arc::new(
            RuntimeTopKSummaryContract::try_from_plan(
                &OrderContract {
                    comparator_digest: comparator_digest_for_test(
                        &keys,
                        COMPARATOR_ALGORITHM_VERSION,
                    ),
                    keys,
                    inclusive: true,
                },
                TopKSummaryRequirement::try_new(k).unwrap(),
            )
            .unwrap(),
        )
    }

    fn stream(partition: u32) -> ProducerStreamId {
        ProducerStreamId::new(
            BindingId::new(1),
            UniqueId::new(2, 3),
            PartitionId::new(partition),
        )
    }

    fn summary(contract: &RuntimeTopKSummaryContract, values: &[i64]) -> TopKSummary {
        TopKSummary::try_new(
            contract,
            values
                .iter()
                .map(|value| {
                    OrderedTuple::try_new(contract.order(), [Some(OrderedScalar::Int64(*value))])
                        .unwrap()
                })
                .collect(),
        )
        .unwrap()
    }

    fn bound(reducer: &TopKSummaryReducer) -> Option<i64> {
        reducer
            .global()
            .map(|domain| match domain.bound().values() {
                [Some(OrderedScalar::Int64(value))] => *value,
                _ => panic!("expected int64 bound"),
            })
    }

    fn apply(
        reducer: &mut TopKSummaryReducer,
        partition: u32,
        sequence: u64,
        values: &[i64],
    ) -> Result<
        TopKApplyOutcome,
        novarocks::runtime_filter_transition::port::producer::RuntimeContractViolation,
    > {
        let update = summary(reducer.contract(), values);
        let projection =
            reducer.preflight_apply(stream(partition), ProducerSequence::new(sequence), &update)?;
        let outcome = projection.outcome();
        reducer.commit_apply(projection);
        Ok(outcome)
    }

    fn apply_summary(
        reducer: &mut TopKSummaryReducer,
        partition: u32,
        sequence: u64,
        candidates: Vec<OrderedTuple>,
    ) -> TopKApplyOutcome {
        let summary = TopKSummary::try_new(reducer.contract(), candidates).unwrap();
        let projection = reducer
            .preflight_apply(stream(partition), ProducerSequence::new(sequence), &summary)
            .unwrap();
        let outcome = projection.outcome();
        reducer.commit_apply(projection);
        outcome
    }

    fn close(
        reducer: &mut TopKSummaryReducer,
        partition: u32,
        terminal: u64,
    ) -> Result<
        TopKCloseOutcome,
        novarocks::runtime_filter_transition::port::producer::RuntimeContractViolation,
    > {
        let projection =
            reducer.preflight_close(stream(partition), ProducerSequence::new(terminal))?;
        let outcome = projection.outcome();
        reducer.commit_close(projection);
        Ok(outcome)
    }

    #[test]
    fn exact_merge_counts_duplicates_without_requiring_full_shards() {
        let mut reducer = TopKSummaryReducer::new(contract(4));
        assert_eq!(
            apply(&mut reducer, 0, 0, &[1, 4]).unwrap(),
            TopKApplyOutcome::StreamUpdated
        );
        assert_eq!(bound(&reducer), None);
        assert_eq!(
            apply(&mut reducer, 1, 0, &[2, 2]).unwrap(),
            TopKApplyOutcome::GlobalTightened
        );
        assert_eq!(bound(&reducer), Some(4));
    }

    #[test]
    fn cumulative_transition_preserves_below_k_multiset_and_non_worsening_ranks() {
        let mut reducer = TopKSummaryReducer::new(contract(4));
        apply(&mut reducer, 0, 0, &[1, 3]).unwrap();
        let before = format!("{reducer:?}");
        let error = apply(&mut reducer, 0, 1, &[1, 2]).unwrap_err();
        assert_eq!(
            error.kind(),
            RuntimeContractViolationKind::OrderedBoundLoosened
        );
        assert_eq!(format!("{reducer:?}"), before);

        apply(&mut reducer, 0, 2, &[0, 1, 2, 3]).unwrap();
        let before = format!("{reducer:?}");
        let error = apply(&mut reducer, 0, 3, &[0, 1, 4, 5]).unwrap_err();
        assert_eq!(
            error.kind(),
            RuntimeContractViolationKind::OrderedBoundLoosened
        );
        assert_eq!(format!("{reducer:?}"), before);
    }

    #[test]
    fn replay_gap_and_terminal_rules_are_deterministic() {
        let mut reducer = TopKSummaryReducer::new(contract(2));
        assert_eq!(
            apply(&mut reducer, 0, 3, &[5]).unwrap(),
            TopKApplyOutcome::StreamUpdated
        );
        assert_eq!(
            apply(&mut reducer, 0, 2, &[4]).unwrap(),
            TopKApplyOutcome::Stale
        );
        assert_eq!(
            apply(&mut reducer, 0, 3, &[5]).unwrap(),
            TopKApplyOutcome::Duplicate
        );
        assert_eq!(
            apply(&mut reducer, 0, 3, &[4]).unwrap_err().kind(),
            RuntimeContractViolationKind::ConflictingReplay
        );
        assert_eq!(
            apply(&mut reducer, 0, 7, &[5]).unwrap(),
            TopKApplyOutcome::SequenceAdvancedEqual
        );
        assert_eq!(
            close(&mut reducer, 0, 9).unwrap(),
            TopKCloseOutcome::PendingFinalSnapshot
        );
        assert_eq!(
            apply(&mut reducer, 0, 8, &[4, 5]).unwrap(),
            TopKApplyOutcome::GlobalTightened
        );
        assert_eq!(
            close(&mut reducer, 0, 9).unwrap(),
            TopKCloseOutcome::Duplicate
        );
        assert_eq!(
            apply(&mut reducer, 0, 9, &[3, 4]).unwrap_err().kind(),
            RuntimeContractViolationKind::SequenceOutsideTerminalRange
        );
        assert_eq!(
            close(&mut reducer, 0, 10).unwrap_err().kind(),
            RuntimeContractViolationKind::ConflictingTerminalSequence
        );

        let mut empty = TopKSummaryReducer::new(contract(2));
        assert_eq!(
            close(&mut empty, 1, 0).unwrap(),
            TopKCloseOutcome::Satisfied
        );
        assert_eq!(
            empty.terminal_partition_count(BindingId::new(1), UniqueId::new(2, 3)),
            1
        );
    }

    #[test]
    fn cumulative_summary_length_never_decreases() {
        let mut reducer = TopKSummaryReducer::new(contract(4));
        apply(&mut reducer, 0, 0, &[1, 2, 3]).unwrap();
        assert_eq!(
            apply(&mut reducer, 0, 1, &[1, 2]).unwrap_err().kind(),
            RuntimeContractViolationKind::OrderedBoundLoosened
        );
    }

    #[test]
    fn preflight_is_read_only_and_retained_bytes_release_only_candidate_state() {
        let mut reducer = TopKSummaryReducer::new(contract(2));
        let update = summary(reducer.contract(), &[1, 3]);
        let before = format!("{reducer:?}");
        let projection = reducer
            .preflight_apply(stream(0), ProducerSequence::new(0), &update)
            .unwrap();
        assert_eq!(format!("{reducer:?}"), before);
        assert_eq!(projection.retained_bytes(), 67);
        reducer.commit_apply(projection);
        assert_eq!(reducer.estimated_retained_bytes(), Some(67));
        assert_eq!(
            reducer.submitted_partition_count(BindingId::new(1), UniqueId::new(2, 3)),
            1
        );

        let close_projection = reducer
            .preflight_close(stream(0), ProducerSequence::new(1))
            .unwrap();
        assert_eq!(close_projection.retained_bytes(), 75);
        reducer.commit_close(close_projection);
        assert_eq!(reducer.retain_protocol_tombstones(), Some(48));
        assert_eq!(bound(&reducer), None);
    }

    #[test]
    fn randomized_exact_merge_matches_full_sort_reference_and_never_worsens() {
        const K: usize = 5;
        let mut reducer = TopKSummaryReducer::new(contract(K as u32));
        let mut latest = BTreeMap::<u32, Vec<i64>>::new();
        let mut sequences = [0u64; 4];
        let mut random = 0x5eed_u64;
        let mut previous_bound = None;

        for _ in 0..200 {
            random = random.wrapping_mul(6364136223846793005).wrapping_add(1);
            let partition = ((random >> 32) % 4) as u32;
            let values = latest.entry(partition).or_default();
            for _ in 0..=((random >> 40) % 2) {
                random = random.wrapping_mul(6364136223846793005).wrapping_add(1);
                values.push(((random >> 32) % 50) as i64);
            }
            values.sort_unstable();
            values.truncate(K);

            apply(
                &mut reducer,
                partition,
                sequences[partition as usize],
                values,
            )
            .unwrap();
            sequences[partition as usize] += 1;

            let mut reference = latest.values().flatten().copied().collect::<Vec<_>>();
            reference.sort_unstable();
            let expected = reference.get(K - 1).copied();
            assert_eq!(bound(&reducer), expected);
            if let (Some(previous), Some(current)) = (previous_bound, expected) {
                assert!(current <= previous);
            }
            previous_bound = expected.or(previous_bound);
        }
    }

    #[test]
    fn topk_randomized_reference_merge() {
        const K: usize = 5;
        let matrices = [
            vec![OrderKeyContract {
                data_type: DataType::Int64,
                direction: SortDirection::Ascending,
                null_order: NullOrder::Last,
            }],
            vec![OrderKeyContract {
                data_type: DataType::Int64,
                direction: SortDirection::Descending,
                null_order: NullOrder::First,
            }],
            vec![
                OrderKeyContract {
                    data_type: DataType::Utf8,
                    direction: SortDirection::Ascending,
                    null_order: NullOrder::First,
                },
                OrderKeyContract {
                    data_type: DataType::Int64,
                    direction: SortDirection::Descending,
                    null_order: NullOrder::Last,
                },
            ],
        ];

        for (matrix_index, keys) in matrices.into_iter().enumerate() {
            let contract = contract_with_keys(K as u32, keys);
            let mut reducer = TopKSummaryReducer::new(contract.clone());
            let mut full_streams = BTreeMap::<u32, Vec<OrderedTuple>>::new();
            let mut latest = BTreeMap::<u32, Vec<OrderedTuple>>::new();
            let mut sequences = [0u64; 4];
            let mut random = 0x5eed_u64.wrapping_add(matrix_index as u64);

            for iteration in 0..200 {
                random = random.wrapping_mul(6364136223846793005).wrapping_add(1);
                let partition = ((random >> 32) % 4) as u32;
                let values = full_streams.entry(partition).or_default();
                for duplicate_index in 0..=usize::try_from((random >> 40) % 2).unwrap() {
                    random = random.wrapping_mul(6364136223846793005).wrapping_add(1);
                    let bucket = ((random >> 32) % 9) as i64;
                    let tuple = if contract.order().keys().len() == 1 {
                        OrderedTuple::try_new(
                            contract.order(),
                            [(bucket % 4 != 0).then_some(OrderedScalar::Int64(bucket))],
                        )
                        .unwrap()
                    } else {
                        let word = match bucket % 4 {
                            0 => None,
                            1 => Some(OrderedScalar::Utf8(Arc::from("alpha"))),
                            2 => Some(OrderedScalar::Utf8(Arc::from("beta"))),
                            _ => Some(OrderedScalar::Utf8(Arc::from("gamma"))),
                        };
                        OrderedTuple::try_new(
                            contract.order(),
                            [
                                word,
                                (bucket % 3 != 0).then_some(OrderedScalar::Int64(
                                    bucket + duplicate_index as i64,
                                )),
                            ],
                        )
                        .unwrap()
                    };
                    values.push(tuple);
                    if duplicate_index == 1 {
                        values.push(values.last().unwrap().clone());
                    }
                }
                values.sort_by(|left, right| contract.order().compare(left, right).unwrap());
                let candidates = values.iter().take(K).cloned().collect::<Vec<_>>();
                latest.insert(partition, candidates.clone());
                apply_summary(
                    &mut reducer,
                    partition,
                    sequences[partition as usize],
                    candidates,
                );
                sequences[partition as usize] += 1;

                let mut reference = latest
                    .values()
                    .flat_map(|candidates| candidates.iter().cloned())
                    .collect::<Vec<_>>();
                reference.sort_by(|left, right| contract.order().compare(left, right).unwrap());
                let expected = reference.get(K - 1);
                assert_eq!(
                    reducer.global().map(|domain| domain.bound()),
                    expected,
                    "matrix={matrix_index} iteration={iteration}"
                );
            }
        }
    }
}
