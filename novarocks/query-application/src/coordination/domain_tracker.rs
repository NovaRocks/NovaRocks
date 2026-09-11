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

//! Query-side progression of immutable domain intents and Worker receipts.
//!
//! These trackers record what query coordination chose to send. They do not
//! model Worker state and cannot authorize a Worker mutation. The Worker owns
//! its accepted-domain state; coordination owns only producer ordering and
//! verification that a receipt settles the immutable intent it released.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use novarocks_execution_contract::{
    ContentFingerprint, DomainConflict, DomainProgression, DomainVersion, EdgeOpenVersion,
    ExchangeEdgeId, PlanNodeId, SplitOffer, SplitSequence, TaskDomainReceipt, TaskDomainUpdate,
};

/// The query-side watermark of split intents issued for one plan node.
#[derive(Copy, Clone, Debug, Default, Eq, PartialEq)]
pub struct SentSplitWatermark {
    accepted_through: Option<SplitSequence>,
    no_more: bool,
}

impl SentSplitWatermark {
    pub const fn accepted_through(self) -> Option<SplitSequence> {
        self.accepted_through
    }

    pub const fn no_more_splits(self) -> bool {
        self.no_more
    }

    const fn next_expected(self) -> u64 {
        match self.accepted_through {
            Some(sequence) => sequence.get() + 1,
            None => 1,
        }
    }

    fn classify(self, offer: SplitOffer) -> DomainProgression {
        match offer {
            SplitOffer::Seal => self.classify_seal(None),
            SplitOffer::Batch {
                first,
                last,
                no_more,
            } => {
                let next = self.next_expected();
                if last.get() < next {
                    if no_more && !self.no_more {
                        return self.classify_seal(Some(last));
                    }
                    return DomainProgression::Idempotent;
                }
                if self.no_more {
                    return DomainProgression::Conflict(DomainConflict::AfterSeal);
                }
                if first.get() > next {
                    return DomainProgression::Conflict(DomainConflict::Gap);
                }
                DomainProgression::Apply
            }
        }
    }

    fn classify_seal(self, through: Option<SplitSequence>) -> DomainProgression {
        if self.no_more {
            return match through {
                Some(through) if self.accepted_through.is_some_and(|held| through > held) => {
                    DomainProgression::Conflict(DomainConflict::AfterSeal)
                }
                _ => DomainProgression::Idempotent,
            };
        }
        match (through, self.accepted_through) {
            (Some(through), Some(held)) if through < held => {
                DomainProgression::Conflict(DomainConflict::NotMonotonic)
            }
            (Some(_), None) => DomainProgression::Conflict(DomainConflict::Gap),
            _ => DomainProgression::Apply,
        }
    }

    fn apply(self, offer: SplitOffer) -> Self {
        match offer {
            SplitOffer::Seal => Self {
                no_more: true,
                ..self
            },
            SplitOffer::Batch { last, no_more, .. } if last.get() < self.next_expected() => Self {
                no_more: self.no_more || no_more,
                ..self
            },
            SplitOffer::Batch { last, no_more, .. } => Self {
                accepted_through: Some(last),
                no_more: self.no_more || no_more,
            },
        }
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
struct SentScalar {
    version: DomainVersion,
    fingerprint: ContentFingerprint,
}

/// Query coordination's record of task-domain intents it has accepted from
/// producers. It is initialized from one immutable task descriptor.
#[derive(Clone, Debug, Default)]
pub struct TaskDomainIntentTracker {
    split_members: BTreeSet<PlanNodeId>,
    splits: BTreeMap<PlanNodeId, SentSplitWatermark>,
    dynamic_filter: Option<SentScalar>,
    edge_members: BTreeSet<ExchangeEdgeId>,
    opened_edges: BTreeSet<ExchangeEdgeId>,
    opened_sets: BTreeMap<EdgeOpenVersion, BTreeSet<ExchangeEdgeId>>,
}

/// The exact Worker receipt facts expected for one recorded task-domain
/// intent. Query coordination stores this beside the immutable request so a
/// later queued intent cannot move the expectation forward.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TaskDomainReceiptExpectation {
    SplitAssignment {
        node: PlanNodeId,
        watermark: SentSplitWatermark,
    },
    TaskDynamicFilter {
        version: DomainVersion,
    },
    OpenExchangeEdges {
        version: EdgeOpenVersion,
        edges: BTreeSet<ExchangeEdgeId>,
    },
}

impl TaskDomainIntentTracker {
    pub fn new(
        split_members: impl IntoIterator<Item = PlanNodeId>,
        edge_members: impl IntoIterator<Item = ExchangeEdgeId>,
    ) -> Self {
        Self {
            split_members: split_members.into_iter().collect(),
            edge_members: edge_members.into_iter().collect(),
            ..Self::default()
        }
    }

    pub fn split_watermark(&self, node: PlanNodeId) -> SentSplitWatermark {
        self.splits.get(&node).copied().unwrap_or_default()
    }

    /// The next edge-open version that has not been assigned to an intent.
    pub fn next_edge_open_version(&self) -> Option<EdgeOpenVersion> {
        match self.opened_sets.keys().next_back() {
            Some(highest) => EdgeOpenVersion::new(highest.get().checked_add(1)?).ok(),
            None => Some(EdgeOpenVersion::FIRST),
        }
    }

    /// Classifies and records one immutable task-domain intent.
    pub fn record(&mut self, update: &TaskDomainUpdate) -> DomainProgression {
        match update {
            TaskDomainUpdate::SplitAssignment(intent) => {
                if !self.split_members.contains(&intent.node()) {
                    return DomainProgression::Conflict(DomainConflict::UnknownMember);
                }
                let watermark = self.split_watermark(intent.node());
                let progression = watermark.classify(intent.offer());
                if matches!(progression, DomainProgression::Apply) {
                    self.splits
                        .insert(intent.node(), watermark.apply(intent.offer()));
                }
                progression
            }
            TaskDomainUpdate::TaskDynamicFilter { version, payload } => {
                let fingerprint = payload.fingerprint();
                match self.dynamic_filter {
                    Some(held) if held.version == *version => {
                        if held.fingerprint == fingerprint {
                            DomainProgression::Idempotent
                        } else {
                            DomainProgression::Conflict(DomainConflict::SameTokenDifferentContent)
                        }
                    }
                    Some(held) if *version < held.version => DomainProgression::Older,
                    _ => {
                        self.dynamic_filter = Some(SentScalar {
                            version: *version,
                            fingerprint,
                        });
                        DomainProgression::Apply
                    }
                }
            }
            TaskDomainUpdate::OpenExchangeEdges { version, edges } => {
                self.record_edge_open(*version, edges)
            }
        }
    }

    /// Freezes the receipt expectation after `update` was recorded.
    ///
    /// Callers retain the returned value with that exact immutable request;
    /// consulting the tracker again after another intent is queued would ask
    /// a receipt to prove state that the Worker has not received yet.
    pub fn receipt_expectation(&self, update: &TaskDomainUpdate) -> TaskDomainReceiptExpectation {
        match update {
            TaskDomainUpdate::SplitAssignment(intent) => {
                TaskDomainReceiptExpectation::SplitAssignment {
                    node: intent.node(),
                    watermark: self.split_watermark(intent.node()),
                }
            }
            TaskDomainUpdate::TaskDynamicFilter { version, .. } => {
                TaskDomainReceiptExpectation::TaskDynamicFilter { version: *version }
            }
            TaskDomainUpdate::OpenExchangeEdges { version, edges } => {
                TaskDomainReceiptExpectation::OpenExchangeEdges {
                    version: *version,
                    edges: edges.iter().copied().collect(),
                }
            }
        }
    }

    fn record_edge_open(
        &mut self,
        version: EdgeOpenVersion,
        edges: &[ExchangeEdgeId],
    ) -> DomainProgression {
        if edges.is_empty() {
            return DomainProgression::Conflict(DomainConflict::UnknownMember);
        }
        let requested = edges.iter().copied().collect::<BTreeSet<_>>();
        if requested.len() != edges.len() {
            return DomainProgression::Conflict(DomainConflict::SameTokenDifferentContent);
        }
        if !requested.is_subset(&self.edge_members) {
            return DomainProgression::Conflict(DomainConflict::UnknownMember);
        }
        if let Some(held) = self.opened_sets.get(&version) {
            return if *held == requested {
                DomainProgression::Idempotent
            } else {
                DomainProgression::Conflict(DomainConflict::SameTokenDifferentContent)
            };
        }
        if self
            .opened_sets
            .keys()
            .next_back()
            .is_some_and(|highest| version < *highest)
        {
            return DomainProgression::Older;
        }
        if !requested.is_disjoint(&self.opened_edges) {
            return DomainProgression::Conflict(DomainConflict::SameTokenDifferentContent);
        }
        self.opened_edges.extend(requested.iter().copied());
        self.opened_sets.insert(version, requested);
        DomainProgression::Apply
    }
}

/// Why a Worker receipt does not settle the immutable intent sent by query
/// coordination.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DomainReceiptMismatch {
    detail: String,
}

impl DomainReceiptMismatch {
    fn new(detail: impl Into<String>) -> Self {
        Self {
            detail: detail.into(),
        }
    }

    pub fn detail(&self) -> &str {
        &self.detail
    }
}

impl fmt::Display for DomainReceiptMismatch {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.detail.fmt(formatter)
    }
}

impl std::error::Error for DomainReceiptMismatch {}

/// Verifies that one Worker task-domain receipt settles the exact immutable
/// intent released by query coordination.
pub fn verify_task_domain_receipt(
    intent: &TaskDomainUpdate,
    expected: &TaskDomainReceiptExpectation,
    receipts: &[TaskDomainReceipt],
) -> Result<(), DomainReceiptMismatch> {
    if receipts.len() != 1 {
        return Err(DomainReceiptMismatch::new(format!(
            "the Worker receipt contains {} domains for one sent intent",
            receipts.len()
        )));
    }
    let mut matching = receipts
        .iter()
        .filter(|receipt| receipt.kind() == intent.kind());
    let receipt = matching
        .next()
        .ok_or_else(|| DomainReceiptMismatch::new("the Worker receipt omitted the sent domain"))?;
    if matching.next().is_some() {
        return Err(DomainReceiptMismatch::new(
            "the Worker receipt repeated the sent domain",
        ));
    }
    if !matches!(
        receipt.progression(),
        DomainProgression::Apply | DomainProgression::Idempotent
    ) {
        return Err(DomainReceiptMismatch::new(format!(
            "the Worker receipt reported progression {:?} for an applied operation",
            receipt.progression()
        )));
    }
    match (intent, expected, receipt) {
        (
            TaskDomainUpdate::SplitAssignment(sent),
            TaskDomainReceiptExpectation::SplitAssignment { node, watermark },
            TaskDomainReceipt::SplitAssignment { nodes, .. },
        ) if sent.node() == *node => verify_split_receipt(*node, *watermark, nodes),
        (
            TaskDomainUpdate::TaskDynamicFilter { version, .. },
            TaskDomainReceiptExpectation::TaskDynamicFilter {
                version: expected_version,
            },
            TaskDomainReceipt::TaskDynamicFilter {
                accepted_version, ..
            },
        ) if version == expected_version && *accepted_version == Some(*expected_version) => Ok(()),
        (
            TaskDomainUpdate::OpenExchangeEdges { version, edges },
            TaskDomainReceiptExpectation::OpenExchangeEdges {
                version: expected_version,
                edges: expected_edges,
            },
            TaskDomainReceipt::OpenExchangeEdges {
                accepted_version,
                opened,
                ..
            },
        ) if version == expected_version
            && accepted_version == expected_version
            && as_set(edges) == *expected_edges
            && opened.len() == expected_edges.len()
            && as_set(opened) == *expected_edges =>
        {
            Ok(())
        }
        _ => Err(DomainReceiptMismatch::new(
            "the Worker receipt does not match the sent domain token or members",
        )),
    }
}

fn verify_split_receipt(
    node: PlanNodeId,
    expected: SentSplitWatermark,
    receipts: &[novarocks_execution_contract::PlanNodeSplitReceipt],
) -> Result<(), DomainReceiptMismatch> {
    let [receipt] = receipts else {
        return Err(DomainReceiptMismatch::new(
            "the Worker split receipt must contain the sent plan node exactly once",
        ));
    };
    if receipt.node() != node {
        return Err(DomainReceiptMismatch::new(
            "the Worker split receipt names a different plan node",
        ));
    }
    let watermark = receipt.watermark();
    let covers_issued = expected.accepted_through().is_none_or(|issued| {
        watermark
            .accepted_through()
            .is_some_and(|accepted| accepted >= issued)
    });
    if !covers_issued || watermark.no_more_splits() != expected.no_more_splits() {
        return Err(DomainReceiptMismatch::new(
            "the Worker split watermark does not cover the sent offer",
        ));
    }
    Ok(())
}

fn as_set(edges: &[ExchangeEdgeId]) -> BTreeSet<ExchangeEdgeId> {
    edges.iter().copied().collect()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use novarocks_execution_contract::SplitWatermark;

    use super::*;

    #[derive(Debug)]
    struct Content(ContentFingerprint);

    impl novarocks_execution_contract::CodecOwnedContent for Content {
        fn fingerprint(&self) -> ContentFingerprint {
            self.0
        }

        fn encoded_len(&self) -> usize {
            1
        }
    }

    fn node(value: i32) -> PlanNodeId {
        PlanNodeId::new(value).expect("valid node")
    }

    fn edge(value: u32) -> ExchangeEdgeId {
        ExchangeEdgeId::new(value).expect("valid edge")
    }

    fn version(value: u64) -> DomainVersion {
        DomainVersion::new(value).expect("valid version")
    }

    fn sequence(value: u64) -> SplitSequence {
        SplitSequence::new(value).expect("valid sequence")
    }

    fn payload(byte: u8) -> Arc<dyn novarocks_execution_contract::CodecOwnedContent> {
        Arc::new(Content(ContentFingerprint::from_bytes([byte; 16])))
    }

    #[test]
    fn scalar_same_token_requires_the_same_content() {
        let mut tracker = TaskDomainIntentTracker::new([], []);
        let first = TaskDomainUpdate::TaskDynamicFilter {
            version: version(1),
            payload: payload(1),
        };
        let changed = TaskDomainUpdate::TaskDynamicFilter {
            version: version(1),
            payload: payload(2),
        };
        assert_eq!(tracker.record(&first), DomainProgression::Apply);
        assert_eq!(tracker.record(&first), DomainProgression::Idempotent);
        assert_eq!(
            tracker.record(&changed),
            DomainProgression::Conflict(DomainConflict::SameTokenDifferentContent)
        );
    }

    #[test]
    fn split_watermarks_reject_gaps_and_content_after_seal() {
        let scan = node(7);
        let mut tracker = TaskDomainIntentTracker::new([scan], []);
        let gap = TaskDomainUpdate::SplitAssignment(
            novarocks_execution_contract::SplitAssignmentIntent::new(
                scan,
                SplitOffer::batch(sequence(2), sequence(2), false).expect("range"),
                payload(1),
            ),
        );
        assert_eq!(
            tracker.record(&gap),
            DomainProgression::Conflict(DomainConflict::Gap)
        );
        let seal = TaskDomainUpdate::SplitAssignment(
            novarocks_execution_contract::SplitAssignmentIntent::new(
                scan,
                SplitOffer::Seal,
                payload(2),
            ),
        );
        assert_eq!(tracker.record(&seal), DomainProgression::Apply);
        assert_eq!(tracker.record(&seal), DomainProgression::Idempotent);
        assert!(tracker.split_watermark(scan).no_more_splits());
    }

    #[test]
    fn edge_versions_are_monotonic_and_membership_is_frozen() {
        let mut tracker = TaskDomainIntentTracker::new([], [edge(1), edge(2), edge(3)]);
        let unknown = TaskDomainUpdate::OpenExchangeEdges {
            version: EdgeOpenVersion::FIRST,
            edges: vec![edge(4)],
        };
        assert_eq!(
            tracker.record(&unknown),
            DomainProgression::Conflict(DomainConflict::UnknownMember)
        );
        let first = TaskDomainUpdate::OpenExchangeEdges {
            version: EdgeOpenVersion::FIRST,
            edges: vec![edge(1)],
        };
        assert_eq!(tracker.record(&first), DomainProgression::Apply);
        assert_eq!(tracker.record(&first), DomainProgression::Idempotent);
        let reused = TaskDomainUpdate::OpenExchangeEdges {
            version: EdgeOpenVersion::FIRST,
            edges: vec![edge(2)],
        };
        assert_eq!(
            tracker.record(&reused),
            DomainProgression::Conflict(DomainConflict::SameTokenDifferentContent)
        );
        let skipped = TaskDomainUpdate::OpenExchangeEdges {
            version: EdgeOpenVersion::new(3).expect("valid version"),
            edges: vec![edge(2)],
        };
        assert_eq!(tracker.record(&skipped), DomainProgression::Apply);
        let older_unseen = TaskDomainUpdate::OpenExchangeEdges {
            version: EdgeOpenVersion::new(2).expect("valid version"),
            edges: vec![edge(3)],
        };
        assert_eq!(tracker.record(&older_unseen), DomainProgression::Older);
        assert_eq!(
            tracker.next_edge_open_version().map(EdgeOpenVersion::get),
            Some(4)
        );
    }

    #[test]
    fn receipt_must_cover_the_exact_sent_split_intent() {
        let scan = node(4);
        let offer = SplitOffer::batch(sequence(1), sequence(2), true).expect("range");
        let intent = TaskDomainUpdate::SplitAssignment(
            novarocks_execution_contract::SplitAssignmentIntent::new(scan, offer, payload(1)),
        );
        let mut tracker = TaskDomainIntentTracker::new([scan], []);
        assert_eq!(tracker.record(&intent), DomainProgression::Apply);
        let expected = tracker.receipt_expectation(&intent);
        let incomplete = TaskDomainReceipt::SplitAssignment {
            nodes: vec![novarocks_execution_contract::PlanNodeSplitReceipt::new(
                scan,
                SplitWatermark::empty().apply_batch(sequence(1), false),
            )],
            progression: DomainProgression::Apply,
        };
        assert!(verify_task_domain_receipt(&intent, &expected, &[incomplete]).is_err());

        let complete = TaskDomainReceipt::SplitAssignment {
            nodes: vec![novarocks_execution_contract::PlanNodeSplitReceipt::new(
                scan,
                SplitWatermark::empty().apply_batch(sequence(2), true),
            )],
            progression: DomainProgression::Apply,
        };
        assert!(verify_task_domain_receipt(&intent, &expected, &[complete]).is_ok());
    }

    #[test]
    fn seal_receipt_cannot_discard_the_previously_issued_watermark() {
        let scan = node(4);
        let mut tracker = TaskDomainIntentTracker::new([scan], []);
        let batch = TaskDomainUpdate::SplitAssignment(
            novarocks_execution_contract::SplitAssignmentIntent::new(
                scan,
                SplitOffer::batch(sequence(1), sequence(3), false).expect("range"),
                payload(1),
            ),
        );
        assert_eq!(tracker.record(&batch), DomainProgression::Apply);
        let seal = TaskDomainUpdate::SplitAssignment(
            novarocks_execution_contract::SplitAssignmentIntent::new(
                scan,
                SplitOffer::Seal,
                payload(2),
            ),
        );
        assert_eq!(tracker.record(&seal), DomainProgression::Apply);
        let expected = tracker.receipt_expectation(&seal);

        let lost_floor = TaskDomainReceipt::SplitAssignment {
            nodes: vec![novarocks_execution_contract::PlanNodeSplitReceipt::new(
                scan,
                SplitWatermark::empty().apply_no_more(),
            )],
            progression: DomainProgression::Apply,
        };
        assert!(verify_task_domain_receipt(&seal, &expected, &[lost_floor]).is_err());

        let retained_floor = TaskDomainReceipt::SplitAssignment {
            nodes: vec![novarocks_execution_contract::PlanNodeSplitReceipt::new(
                scan,
                SplitWatermark::empty()
                    .apply_batch(sequence(3), false)
                    .apply_no_more(),
            )],
            progression: DomainProgression::Apply,
        };
        assert!(verify_task_domain_receipt(&seal, &expected, &[retained_floor]).is_ok());
    }

    #[test]
    fn edge_receipt_must_name_the_exact_accepted_version() {
        let mut tracker = TaskDomainIntentTracker::new([], [edge(1)]);
        let intent = TaskDomainUpdate::OpenExchangeEdges {
            version: EdgeOpenVersion::new(3).expect("valid version"),
            edges: vec![edge(1)],
        };
        assert_eq!(tracker.record(&intent), DomainProgression::Apply);
        let expected = tracker.receipt_expectation(&intent);

        let wrong_version = TaskDomainReceipt::OpenExchangeEdges {
            accepted_version: EdgeOpenVersion::new(2).expect("valid version"),
            opened: vec![edge(1)],
            progression: DomainProgression::Apply,
        };
        assert!(
            verify_task_domain_receipt(&intent, &expected, &[wrong_version]).is_err(),
            "edge membership alone must not stand in for the accepted version"
        );

        let exact = TaskDomainReceipt::OpenExchangeEdges {
            accepted_version: EdgeOpenVersion::new(3).expect("valid version"),
            opened: vec![edge(1)],
            progression: DomainProgression::Apply,
        };
        assert!(verify_task_domain_receipt(&intent, &expected, &[exact]).is_ok());
    }
}
