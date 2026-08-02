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

use std::collections::BTreeMap;

use novarocks::runtime_filter_transition::port::identity::{PartitionId, ProducerSequence};
use novarocks::runtime_filter_transition::port::subscription::UnavailableReason;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum LogicalTerminal {
    Completed,
    CompletedWithoutArtifact,
    DegradedLogical(UnavailableReason),
    Unavailable(UnavailableReason),
    Cancelled,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TerminalProgress {
    Pending,
    Satisfied,
    Impossible,
}

#[derive(Debug)]
pub(crate) struct PartitionState {
    pub(crate) seen: BTreeMap<ProducerSequence, [u8; 32]>,
    pub(crate) terminal_sequence: Option<ProducerSequence>,
    pub(crate) progress: TerminalProgress,
}

impl Default for PartitionState {
    fn default() -> Self {
        Self {
            seen: BTreeMap::new(),
            terminal_sequence: None,
            progress: TerminalProgress::Pending,
        }
    }
}

impl PartitionState {
    pub(crate) fn is_gapless(&self) -> bool {
        let Some(terminal) = self.terminal_sequence else {
            return false;
        };
        usize::try_from(terminal.get()) == Ok(self.seen.len())
    }
}

#[derive(Debug)]
pub(crate) struct InstanceState {
    local_partition_count: Option<u32>,
    partitions: BTreeMap<PartitionId, PartitionState>,
    pub(crate) progress: TerminalProgress,
}

impl Default for InstanceState {
    fn default() -> Self {
        Self {
            local_partition_count: None,
            partitions: BTreeMap::new(),
            progress: TerminalProgress::Pending,
        }
    }
}

impl InstanceState {
    pub(crate) fn open(&mut self, count: u32) -> bool {
        if self.local_partition_count.is_some() {
            return false;
        }
        self.local_partition_count = Some(count);
        true
    }

    pub(crate) const fn local_partition_count(&self) -> Option<u32> {
        self.local_partition_count
    }

    #[cfg(test)]
    pub(crate) fn materialized_partition_count(&self) -> usize {
        self.partitions.len()
    }

    pub(crate) fn partition(&self, partition_id: PartitionId) -> Option<&PartitionState> {
        self.partitions.get(&partition_id)
    }

    pub(crate) fn partition_mut_for_commit(
        &mut self,
        partition_id: PartitionId,
    ) -> &mut PartitionState {
        self.partitions.entry(partition_id).or_default()
    }

    pub(crate) fn clear_partitions(&mut self) {
        self.partitions.clear();
    }

    pub(crate) fn refresh_satisfied(&mut self) {
        if self.progress == TerminalProgress::Pending
            && self.local_partition_count.is_some_and(|count| {
                usize::try_from(count) == Ok(self.partitions.len())
                    && self
                        .partitions
                        .values()
                        .all(|partition| partition.progress == TerminalProgress::Satisfied)
            })
        {
            self.progress = TerminalProgress::Satisfied;
        }
    }
}

#[cfg(test)]
mod tests {
    use novarocks::runtime_filter_transition::port::identity::{PartitionId, ProducerSequence};
    use novarocks::runtime_filter_transition::port::value_domain::{
        MembershipValues, ValueDomainDelta,
    };

    use super::*;

    #[test]
    fn max_partition_count_open_is_sparse_and_safe() {
        let mut instance = InstanceState::default();

        assert!(instance.open(u32::MAX));
        assert_eq!(instance.local_partition_count(), Some(u32::MAX));
        assert_eq!(instance.materialized_partition_count(), 0);
        assert!(instance.partition(PartitionId::new(0)).is_none());
    }

    #[test]
    fn gapless_uses_cardinality_after_terminal_range_invariant() {
        let mut partition = PartitionState::default();
        let delta = ValueDomainDelta::new(MembershipValues::int64([1]), false);
        partition
            .seen
            .insert(ProducerSequence::new(999_999), delta.fingerprint().bytes());
        partition.terminal_sequence = Some(ProducerSequence::new(1_000_000));
        assert!(!partition.is_gapless());

        let mut partition = PartitionState::default();
        for sequence in [2_u64, 0, 1] {
            partition
                .seen
                .insert(ProducerSequence::new(sequence), delta.fingerprint().bytes());
        }
        partition.terminal_sequence = Some(ProducerSequence::new(3));
        assert!(partition.is_gapless());
    }
}
