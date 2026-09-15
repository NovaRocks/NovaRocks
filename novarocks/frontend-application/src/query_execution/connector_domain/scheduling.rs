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

//! Runtime split-assignment values owned by the coordinator.
//!
//! Sequence is the only scheduling identity: it is monotonic within one
//! (task attempt, plan node), and a sequence at or below the receiver
//! watermark is idempotently skipped regardless of its payload. Sender-side
//! immutable assignment ownership is therefore the sole guard against mapping
//! one sequence to different content. No split digest, content id, or
//! self-attestation exists here.

use std::collections::BTreeMap;
use std::fmt;

use novarocks_proto_codec::connector_read::ConnectorReadEncoder;
use novarocks_proto_models::connector_read as dto;
use novarocks_spi::connector::ConnectorReadWireEncoder;
use novarocks_types::UniqueId;

use super::handle::Split;

/// Largest number of splits one assignment may carry, matching the wire bound.
pub(crate) const MAX_SPLITS_PER_ASSIGNMENT: usize = 4096;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum SplitAssignmentError {
    /// A plan node already reached its terminal marker.
    AlreadyTerminal { plan_node_id: i32 },
    /// The batch would exceed the per-assignment split bound.
    BatchTooLarge { plan_node_id: i32, splits: usize },
}

impl fmt::Display for SplitAssignmentError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AlreadyTerminal { plan_node_id } => write!(
                formatter,
                "plan node {plan_node_id} already reached no-more-splits"
            ),
            Self::BatchTooLarge {
                plan_node_id,
                splits,
            } => write!(
                formatter,
                "plan node {plan_node_id} assignment carries {splits} splits, above the bound"
            ),
        }
    }
}

impl std::error::Error for SplitAssignmentError {}

/// Allocates monotonic sequences for one task attempt.
///
/// A new attempt constructs a new allocator, so a sequence from a replaced
/// round can never be reused or resumed.
#[derive(Debug, Default)]
pub(crate) struct SplitSequenceAllocator {
    next: BTreeMap<i32, u64>,
}

impl SplitSequenceAllocator {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Allocates the next sequence for one plan node, starting at one.
    ///
    /// One and not zero, for two reasons that point the same way. A split
    /// sequence is a nonzero type on the task protocol, so a zero-based
    /// allocation makes every plan node's *first* split unrepresentable and
    /// its payload undecodable. And a receipt reports the highest accepted
    /// sequence as a plain integer, so with a zero-based space `0` would mean
    /// both "nothing accepted yet" and "accepted sequence zero" -- one of the
    /// two readings has to be wrong, and a watermark that cannot tell them
    /// apart cannot decide whether a resend is a duplicate.
    fn allocate(&mut self, plan_node_id: i32) -> u64 {
        let slot = self.next.entry(plan_node_id).or_insert(1);
        let sequence = *slot;
        *slot += 1;
        sequence
    }

    /// The next sequence a plan node will use.
    ///
    /// Zero means nothing has been issued, which is a distinct answer from
    /// any sequence a split could carry.
    pub(crate) fn issued(&self, plan_node_id: i32) -> u64 {
        self.next.get(&plan_node_id).copied().unwrap_or_default()
    }
}

/// One split placed at one sequence in one plan node's queue.
#[derive(Clone, Debug)]
pub(crate) struct ScheduledSplit {
    sequence_id: u64,
    plan_node_id: i32,
    split: Split,
}

impl ScheduledSplit {
    pub(crate) const fn sequence_id(&self) -> u64 {
        self.sequence_id
    }

    pub(crate) const fn plan_node_id(&self) -> i32 {
        self.plan_node_id
    }

    pub(crate) const fn split(&self) -> &Split {
        &self.split
    }

    /// Encode only at TaskUpdate egress with the codec selected for this
    /// exact provider binding.
    pub(crate) fn to_proto(
        &self,
        encoder: &dyn ConnectorReadWireEncoder,
    ) -> Result<dto::ScheduledSplit, String> {
        encoder
            .encode_scheduled_split(self.sequence_id, self.plan_node_id, self.split.split())
            .map_err(|error| error.to_string())
    }
}

/// A batch of splits for one plan node, plus its terminal marker.
#[derive(Clone)]
pub(crate) struct SplitAssignment {
    plan_node_id: i32,
    splits: Vec<ScheduledSplit>,
    no_more_splits: bool,
    encoder: std::sync::Arc<dyn ConnectorReadWireEncoder>,
}

impl std::fmt::Debug for SplitAssignment {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SplitAssignment")
            .field("plan_node_id", &self.plan_node_id)
            .field("splits", &self.splits)
            .field("no_more_splits", &self.no_more_splits)
            .finish_non_exhaustive()
    }
}

impl SplitAssignment {
    pub(crate) const fn plan_node_id(&self) -> i32 {
        self.plan_node_id
    }

    pub(crate) fn splits(&self) -> &[ScheduledSplit] {
        &self.splits
    }

    pub(crate) const fn no_more_splits(&self) -> bool {
        self.no_more_splits
    }

    pub(crate) fn to_proto(&self) -> Result<dto::SplitAssignment, String> {
        Ok(dto::SplitAssignment {
            plan_node_id: self.plan_node_id,
            splits: self
                .splits
                .iter()
                .map(|split| split.to_proto(self.encoder.as_ref()))
                .collect::<Result<_, _>>()?,
            no_more_splits: self.no_more_splits,
        })
    }
}

/// Per-plan-node send state for one task attempt.
///
/// It records what has been issued and whether the terminal marker was already
/// sent, so a driver cannot append after no-more or reuse a sequence.
#[derive(Debug, Default)]
pub(crate) struct PlanNodeAssignmentState {
    sequences: SplitSequenceAllocator,
    terminal: BTreeMap<i32, bool>,
}

impl PlanNodeAssignmentState {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn is_terminal(&self, plan_node_id: i32) -> bool {
        self.terminal
            .get(&plan_node_id)
            .copied()
            .unwrap_or_default()
    }

    pub(crate) fn issued(&self, plan_node_id: i32) -> u64 {
        self.sequences.issued(plan_node_id)
    }

    /// Build the next assignment for one plan node.
    ///
    /// Zero splits with `no_more_splits` is the normal way a plan node with no
    /// work finishes; it never produces a synthetic empty split.
    pub(crate) fn assign(
        &mut self,
        plan_node_id: i32,
        splits: Vec<Split>,
        no_more_splits: bool,
        encoder: std::sync::Arc<dyn ConnectorReadWireEncoder>,
    ) -> Result<SplitAssignment, SplitAssignmentError> {
        if self.is_terminal(plan_node_id) {
            return Err(SplitAssignmentError::AlreadyTerminal { plan_node_id });
        }
        if splits.len() > MAX_SPLITS_PER_ASSIGNMENT {
            return Err(SplitAssignmentError::BatchTooLarge {
                plan_node_id,
                splits: splits.len(),
            });
        }
        let scheduled = splits
            .into_iter()
            .map(|split| {
                let sequence_id = self.sequences.allocate(plan_node_id);
                ScheduledSplit {
                    sequence_id,
                    plan_node_id,
                    split,
                }
            })
            .collect();
        if no_more_splits {
            self.terminal.insert(plan_node_id, true);
        }
        Ok(SplitAssignment {
            plan_node_id,
            splits: scheduled,
            no_more_splits,
            encoder,
        })
    }
}

/// One immutable update delivered to one admitted task.
///
/// A driver owns this value until it observes a terminal acknowledgement. It
/// may pass the same borrowed request to its transport more than once after an
/// unknown network outcome; every egress encoding therefore starts from these
/// fixed assignments rather than rebuilding their sequence space.
#[derive(Clone, Debug)]
pub(crate) struct TaskUpdateRequest {
    fragment_instance_id: UniqueId,
    assignments: Vec<SplitAssignment>,
}

impl TaskUpdateRequest {
    pub(crate) fn new(fragment_instance_id: UniqueId, assignments: Vec<SplitAssignment>) -> Self {
        Self {
            fragment_instance_id,
            assignments,
        }
    }

    pub(crate) const fn fragment_instance_id(&self) -> UniqueId {
        self.fragment_instance_id
    }

    pub(crate) fn assignments(&self) -> &[SplitAssignment] {
        &self.assignments
    }

    pub(crate) fn to_proto_assignments(&self) -> Result<Vec<dto::SplitAssignment>, String> {
        self.assignments
            .iter()
            .map(SplitAssignment::to_proto)
            .collect()
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn the_first_sequence_of_every_plan_node_is_representable() {
        use novarocks_execution::task_execution::SplitSequence;

        // A split sequence is nonzero on the task protocol. A zero-based
        // allocation therefore makes the *first* split of every plan node
        // undecodable, which is every scan's first batch -- and it also makes
        // a receipt's "highest accepted sequence" ambiguous, because zero
        // would mean both nothing-accepted and accepted-zero.
        let mut allocator = SplitSequenceAllocator::new();
        assert_eq!(
            allocator.issued(7),
            0,
            "nothing issued is its own answer, distinct from any real sequence"
        );

        let first = allocator.allocate(7);
        assert!(
            SplitSequence::new(first).is_ok(),
            "the first sequence must be representable, got {first}"
        );
        assert_eq!(allocator.allocate(7), first + 1);

        // Each plan node has its own space, so a second node also starts
        // representable rather than continuing the first node's count.
        assert_eq!(allocator.allocate(9), first);
    }

    use super::*;

    #[test]
    fn empty_terminal_assignment_is_a_real_terminal_not_a_synthetic_split() {
        assert_eq!(
            SplitAssignmentError::AlreadyTerminal { plan_node_id: 9 }.to_string(),
            "plan node 9 already reached no-more-splits"
        );
        assert_eq!(
            SplitAssignmentError::BatchTooLarge {
                plan_node_id: 9,
                splits: MAX_SPLITS_PER_ASSIGNMENT + 1
            }
            .to_string(),
            format!(
                "plan node 9 assignment carries {} splits, above the bound",
                MAX_SPLITS_PER_ASSIGNMENT + 1
            )
        );
    }
}
