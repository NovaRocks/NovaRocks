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

use crate::runtime_filter::port::identity::{ProducerSequence, ProducerStreamId};
use crate::runtime_filter::port::ordered_bound::{
    OrderedBoundUpdate, OrderedTuple, RuntimeOrderContract,
};
use crate::runtime_filter::port::producer::{
    RuntimeContractViolation, RuntimeContractViolationKind,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct OrderedBoundDomain {
    contract: Arc<RuntimeOrderContract>,
    bound: OrderedTuple,
}

impl OrderedBoundDomain {
    pub(crate) fn new(contract: Arc<RuntimeOrderContract>, bound: OrderedTuple) -> Self {
        Self { contract, bound }
    }

    pub(crate) const fn contract(&self) -> &Arc<RuntimeOrderContract> {
        &self.contract
    }

    pub(crate) const fn bound(&self) -> &OrderedTuple {
        &self.bound
    }

    pub(crate) fn estimated_retained_bytes(&self) -> Option<usize> {
        self.bound.estimated_retained_bytes()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum OrderedApplyOutcome {
    Stale,
    Duplicate,
    SequenceAdvancedEqual,
    StreamTightened,
    GlobalTightened(Arc<OrderedBoundDomain>),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum OrderedCloseOutcome {
    Duplicate,
    PendingFinalSnapshot,
    Satisfied,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct OrderedStreamState {
    highest_sequence: Option<ProducerSequence>,
    replay_digest: Option<[u8; 32]>,
    latest_bound: Option<OrderedTuple>,
    terminal_sequence: Option<ProducerSequence>,
}

impl OrderedStreamState {
    fn terminal_satisfied(&self) -> bool {
        match self.terminal_sequence {
            Some(terminal) if terminal.get() == 0 => self.highest_sequence.is_none(),
            Some(terminal) => terminal.get().checked_sub(1).is_some_and(|final_sequence| {
                self.highest_sequence == Some(ProducerSequence::new(final_sequence))
            }),
            None => false,
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct OrderedReducer {
    contract: Arc<RuntimeOrderContract>,
    streams: BTreeMap<ProducerStreamId, OrderedStreamState>,
    global: Option<Arc<OrderedBoundDomain>>,
}

impl OrderedReducer {
    pub(crate) fn new(contract: Arc<RuntimeOrderContract>) -> Self {
        Self {
            contract,
            streams: BTreeMap::new(),
            global: None,
        }
    }

    pub(crate) const fn contract(&self) -> &Arc<RuntimeOrderContract> {
        &self.contract
    }

    pub(crate) fn global(&self) -> Option<&Arc<OrderedBoundDomain>> {
        self.global.as_ref()
    }

    pub(crate) fn estimated_retained_bytes(&self) -> Option<usize> {
        self.streams.values().try_fold(0usize, |bytes, stream| {
            let bound_bytes = stream
                .latest_bound
                .as_ref()
                .map_or(Some(0), OrderedTuple::estimated_retained_bytes)?;
            bytes
                .checked_add(bound_bytes)?
                .checked_add(32)?
                .checked_add(size_of::<ProducerSequence>())?
                .checked_add(
                    stream
                        .terminal_sequence
                        .map_or(0, |_| size_of::<ProducerSequence>()),
                )
        })
    }

    pub(crate) fn retain_protocol_tombstones(&mut self) -> Option<usize> {
        for stream in self.streams.values_mut() {
            stream.latest_bound = None;
        }
        self.global = None;
        self.estimated_retained_bytes()
    }

    pub(crate) fn validate_tombstone_update(
        &self,
        stream_id: ProducerStreamId,
        sequence: ProducerSequence,
        update: &OrderedBoundUpdate,
    ) -> Result<(), RuntimeContractViolation> {
        if update.order_contract_digest() != self.contract.digest() {
            return Err(violation(
                RuntimeContractViolationKind::OrderedContractMismatch,
                "ordered bound update does not match the installed order contract",
            ));
        }
        let Some(current) = self.streams.get(&stream_id) else {
            return Ok(());
        };
        if current
            .terminal_sequence
            .is_some_and(|terminal| sequence >= terminal)
        {
            return Err(violation(
                RuntimeContractViolationKind::SequenceOutsideTerminalRange,
                "ordered update sequence is outside the exclusive terminal range",
            ));
        }
        if current.highest_sequence == Some(sequence)
            && current.replay_digest != Some(update.replay_digest())
        {
            return Err(violation(
                RuntimeContractViolationKind::ConflictingReplay,
                "same ordered producer sequence carried a different cumulative bound",
            ));
        }
        Ok(())
    }

    pub(crate) fn apply(
        &mut self,
        stream_id: ProducerStreamId,
        sequence: ProducerSequence,
        update: OrderedBoundUpdate,
    ) -> Result<OrderedApplyOutcome, RuntimeContractViolation> {
        if update.order_contract_digest() != self.contract.digest() {
            return Err(violation(
                RuntimeContractViolationKind::OrderedContractMismatch,
                "ordered bound update does not match the installed order contract",
            ));
        }

        let current = self.streams.get(&stream_id).cloned().unwrap_or_default();
        if current
            .terminal_sequence
            .is_some_and(|terminal| sequence >= terminal)
        {
            return Err(violation(
                RuntimeContractViolationKind::SequenceOutsideTerminalRange,
                "ordered update sequence is outside the exclusive terminal range",
            ));
        }
        if let Some(highest) = current.highest_sequence {
            if sequence < highest {
                return Ok(OrderedApplyOutcome::Stale);
            }
            if sequence == highest {
                return if current.replay_digest == Some(update.replay_digest()) {
                    Ok(OrderedApplyOutcome::Duplicate)
                } else {
                    Err(violation(
                        RuntimeContractViolationKind::ConflictingReplay,
                        "same ordered producer sequence carried a different cumulative bound",
                    ))
                };
            }
            let ordering = self
                .contract
                .compare(
                    update.bound(),
                    current.latest_bound.as_ref().expect("sequence owns bound"),
                )
                .map_err(|_| {
                    violation(
                        RuntimeContractViolationKind::OrderedContractMismatch,
                        "ordered bound tuple does not match the installed order contract",
                    )
                })?;
            if ordering == Ordering::Greater {
                return Err(violation(
                    RuntimeContractViolationKind::OrderedBoundLoosened,
                    "higher ordered producer sequence loosened its stream bound",
                ));
            }
            if ordering == Ordering::Equal {
                let stream = self
                    .streams
                    .get_mut(&stream_id)
                    .expect("existing ordered stream");
                stream.highest_sequence = Some(sequence);
                stream.replay_digest = Some(update.replay_digest());
                return Ok(OrderedApplyOutcome::SequenceAdvancedEqual);
            }
        }

        let previous_global = self.global.clone();
        self.streams.insert(
            stream_id,
            OrderedStreamState {
                highest_sequence: Some(sequence),
                replay_digest: Some(update.replay_digest()),
                latest_bound: Some(update.bound().clone()),
                terminal_sequence: current.terminal_sequence,
            },
        );
        let next_global = self
            .streams
            .values()
            .filter_map(|stream| stream.latest_bound.as_ref())
            .try_fold(None::<OrderedTuple>, |best, bound| {
                let Some(best) = best else {
                    return Ok::<_, RuntimeContractViolation>(Some(bound.clone()));
                };
                let ordering = self.contract.compare(bound, &best).map_err(|_| {
                    violation(
                        RuntimeContractViolationKind::OrderedContractMismatch,
                        "retained ordered bound does not match the installed contract",
                    )
                })?;
                Ok(Some(if ordering == Ordering::Less {
                    bound.clone()
                } else {
                    best
                }))
            })?
            .expect("applied ordered stream establishes a global bound");
        let tightened = previous_global.as_ref().is_none_or(|previous| {
            self.contract
                .compare(&next_global, previous.bound())
                .expect("validated ordered bounds remain comparable")
                == Ordering::Less
        });
        if tightened {
            let domain = Arc::new(OrderedBoundDomain::new(self.contract.clone(), next_global));
            self.global = Some(domain.clone());
            Ok(OrderedApplyOutcome::GlobalTightened(domain))
        } else {
            Ok(OrderedApplyOutcome::StreamTightened)
        }
    }

    pub(crate) fn close(
        &mut self,
        stream_id: ProducerStreamId,
        terminal_sequence: ProducerSequence,
    ) -> Result<OrderedCloseOutcome, RuntimeContractViolation> {
        let current = self.streams.get(&stream_id).cloned().unwrap_or_default();
        if let Some(previous) = current.terminal_sequence {
            return if previous == terminal_sequence {
                Ok(OrderedCloseOutcome::Duplicate)
            } else {
                Err(violation(
                    RuntimeContractViolationKind::ConflictingTerminalSequence,
                    "ordered partition close replay changed terminal sequence",
                ))
            };
        }
        if current
            .highest_sequence
            .is_some_and(|highest| highest >= terminal_sequence)
        {
            return Err(violation(
                RuntimeContractViolationKind::SequenceOutsideTerminalRange,
                "ordered partition already contains an update outside terminal range",
            ));
        }
        let stream = self.streams.entry(stream_id).or_default();
        stream.terminal_sequence = Some(terminal_sequence);
        Ok(if stream.terminal_satisfied() {
            OrderedCloseOutcome::Satisfied
        } else {
            OrderedCloseOutcome::PendingFinalSnapshot
        })
    }

    pub(crate) fn stream_terminal_satisfied(&self, stream_id: ProducerStreamId) -> bool {
        self.streams
            .get(&stream_id)
            .is_some_and(OrderedStreamState::terminal_satisfied)
    }

    pub(crate) fn terminal_partition_count(
        &self,
        binding_id: crate::runtime_filter::model::contract::BindingId,
        fragment_instance_id: crate::common::types::UniqueId,
    ) -> usize {
        self.streams
            .iter()
            .filter(|(stream, state)| {
                stream.binding_id() == binding_id
                    && stream.fragment_instance_id() == fragment_instance_id
                    && state.terminal_satisfied()
            })
            .count()
    }
}

fn violation(
    kind: RuntimeContractViolationKind,
    detail: impl Into<String>,
) -> RuntimeContractViolation {
    RuntimeContractViolation::new(kind, detail)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::DataType;

    use crate::common::types::UniqueId;
    use crate::runtime_filter::model::contract::{
        BindingId, NullOrder, OrderContract, OrderKeyContract, SortDirection,
    };
    use crate::runtime_filter::port::identity::{PartitionId, ProducerSequence, ProducerStreamId};
    use crate::runtime_filter::port::ordered_bound::{
        COMPARATOR_ALGORITHM_VERSION, OrderedBoundUpdate, OrderedScalar, OrderedTuple,
        RuntimeOrderContract, comparator_digest_for_test,
    };
    use crate::runtime_filter::port::producer::RuntimeContractViolation;

    use super::{OrderedApplyOutcome, OrderedReducer};

    fn contract() -> Arc<RuntimeOrderContract> {
        let keys = vec![OrderKeyContract {
            data_type: DataType::Int64,
            direction: SortDirection::Ascending,
            null_order: NullOrder::Last,
        }];
        Arc::new(
            RuntimeOrderContract::try_from_plan(&OrderContract {
                comparator_digest: comparator_digest_for_test(&keys, COMPARATOR_ALGORITHM_VERSION),
                keys,
                inclusive: true,
            })
            .unwrap(),
        )
    }

    fn stream(partition: u32) -> ProducerStreamId {
        ProducerStreamId::new(
            BindingId::new(1),
            UniqueId::new(0, 1),
            PartitionId::new(partition),
        )
    }

    fn update(contract: &RuntimeOrderContract, value: i64) -> OrderedBoundUpdate {
        OrderedBoundUpdate::new(
            contract,
            OrderedTuple::try_new(contract, [Some(OrderedScalar::Int64(value))]).unwrap(),
        )
        .unwrap()
    }

    fn apply_for_test(
        reducer: &mut OrderedReducer,
        partition: u32,
        sequence: u64,
        value: i64,
    ) -> Result<OrderedApplyOutcome, RuntimeContractViolation> {
        let update = update(reducer.contract(), value);
        reducer.apply(stream(partition), ProducerSequence::new(sequence), update)
    }

    #[test]
    fn higher_equal_advances_sequence_without_new_version() {
        let mut reducer = OrderedReducer::new(contract());
        assert!(matches!(
            apply_for_test(&mut reducer, 0, 3, 100).unwrap(),
            OrderedApplyOutcome::GlobalTightened(_)
        ));
        assert_eq!(
            apply_for_test(&mut reducer, 0, 7, 100).unwrap(),
            OrderedApplyOutcome::SequenceAdvancedEqual
        );
        assert_eq!(
            reducer.global().unwrap().bound().values(),
            &[Some(OrderedScalar::Int64(100))]
        );
    }

    #[test]
    fn higher_looser_is_contract_violation_and_state_is_unchanged() {
        let mut reducer = OrderedReducer::new(contract());
        apply_for_test(&mut reducer, 0, 3, 100).unwrap();
        let before = format!("{reducer:?}");
        let error = apply_for_test(&mut reducer, 0, 4, 101).unwrap_err();
        assert_eq!(
            error.kind(),
            crate::runtime_filter::port::producer::RuntimeContractViolationKind::OrderedBoundLoosened
        );
        assert_eq!(format!("{reducer:?}"), before);
    }

    #[test]
    fn another_stream_may_be_looser_than_global_without_violation() {
        let mut reducer = OrderedReducer::new(contract());
        assert!(matches!(
            apply_for_test(&mut reducer, 0, 0, 50).unwrap(),
            OrderedApplyOutcome::GlobalTightened(_)
        ));
        assert_eq!(
            apply_for_test(&mut reducer, 1, 0, 90).unwrap(),
            OrderedApplyOutcome::StreamTightened
        );
        assert_eq!(
            reducer.global().unwrap().bound().values(),
            &[Some(OrderedScalar::Int64(50))]
        );
    }
}
