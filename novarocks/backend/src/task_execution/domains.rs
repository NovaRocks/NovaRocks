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

//! One task's accepted domain tokens and the two-pass application of a
//! typed domain update.
//!
//! Classification is pure and happens for every domain in the request before
//! anything is applied, so one operation is either fully accepted or changes
//! no token at all. The execution-side apply runs between the two passes: if
//! the host rejects, no token moves, which leaves the identical request a
//! clean replay rather than a half-advanced domain.

use std::fmt;

use novarocks_execution::task_execution::descriptor::TaskDescriptor;
use novarocks_execution::task_execution::domain::{
    DomainProgression, EdgeOpenVersion, ExchangeEdgeDomain, ExchangeEdgeId, PlanNodeId,
    ScalarDomain, SplitDomain, SplitSequence, SplitWatermark,
};
use novarocks_execution::task_execution::operation::{
    OperationOutcome, PlanNodeSplitReceipt, TaskDomainReceipt, TaskDomainUpdate,
};

use super::host::{HostRejection, TaskExecutionHost};

/// The accepted token state of one task's three domains.
#[derive(Clone, Debug, Default)]
pub(super) struct TaskDomains {
    splits: SplitDomain,
    dynamic_filter: ScalarDomain,
    edges: ExchangeEdgeDomain,
}

impl TaskDomains {
    /// Installs the closed edge set the descriptor froze. Every other domain
    /// starts empty.
    pub(super) fn for_descriptor(descriptor: &TaskDescriptor) -> Self {
        Self {
            splits: SplitDomain::new(),
            dynamic_filter: ScalarDomain::empty(),
            edges: ExchangeEdgeDomain::from_frozen_edges(descriptor.topology().edge_ids()),
        }
    }
}

/// Why one domain update was refused.
#[derive(Clone, Debug)]
pub(super) struct DomainRejection {
    outcome: OperationOutcome,
    detail: String,
}

impl DomainRejection {
    fn new(outcome: OperationOutcome, detail: impl Into<String>) -> Self {
        Self {
            outcome,
            detail: detail.into(),
        }
    }

    pub(super) const fn outcome(&self) -> OperationOutcome {
        self.outcome
    }

    pub(super) fn detail(&self) -> &str {
        &self.detail
    }
}

impl fmt::Display for DomainRejection {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.detail)
    }
}

/// One classified domain update, ready to be applied.
struct Classified<'a> {
    update: &'a TaskDomainUpdate,
    progression: DomainProgression,
}

/// Rejects an update naming something the frozen descriptor does not contain.
///
/// This is the structural half of creation validation: an initial domain that
/// addresses an unknown plan node or an unfrozen edge is refused before any
/// receiver or capability is installed.
pub(super) fn validate_membership(
    descriptor: &TaskDescriptor,
    updates: &[TaskDomainUpdate],
) -> Result<(), DomainRejection> {
    for update in updates {
        match update {
            TaskDomainUpdate::SplitAssignment(intent) => {
                if !descriptor.accepts_split_plan_node(intent.node()) {
                    return Err(DomainRejection::new(
                        OperationOutcome::DomainConflict,
                        format!(
                            "split assignment names plan node {} which the descriptor did not freeze",
                            intent.node()
                        ),
                    ));
                }
            }
            TaskDomainUpdate::TaskDynamicFilter { .. } => {}
            TaskDomainUpdate::OpenExchangeEdges { edges, .. } => {
                for edge in edges {
                    if descriptor.topology().edge(*edge).is_none() {
                        return Err(DomainRejection::new(
                            OperationOutcome::DomainConflict,
                            format!(
                                "edge open names exchange edge {edge} which the descriptor did not freeze"
                            ),
                        ));
                    }
                }
            }
        }
    }
    Ok(())
}

/// Applies one operation's domain updates, or none of them.
pub(super) fn apply_updates(
    host: &dyn TaskExecutionHost,
    descriptor: &TaskDescriptor,
    domains: &mut TaskDomains,
    updates: &[TaskDomainUpdate],
) -> Result<(Vec<TaskDomainReceipt>, bool), DomainRejection> {
    validate_membership(descriptor, updates)?;

    // Pass one classifies against a speculative copy so that two batches for
    // the same plan node inside one request are judged in order, without any
    // of them being committed yet.
    let mut speculative = domains.clone();
    let mut classified = Vec::with_capacity(updates.len());
    for update in updates {
        let progression = classify(&mut speculative, update)?;
        classified.push(Classified {
            update,
            progression,
        });
    }

    // The execution side applies before any token moves. A rejection here
    // leaves every token exactly where it was, so the identical request stays
    // a clean replay instead of a partially advanced domain.
    for entry in &classified {
        if matches!(entry.progression, DomainProgression::Apply) {
            host.apply_task_domain(descriptor, entry.update)
                .map_err(rejection_from_host)?;
        }
    }

    let mut receipts = Vec::with_capacity(classified.len());
    let mut applied_any = false;
    for entry in classified {
        applied_any |= matches!(entry.progression, DomainProgression::Apply);
        receipts.push(commit(domains, entry.update, entry.progression));
    }
    Ok((receipts, applied_any))
}

fn rejection_from_host(rejection: HostRejection) -> DomainRejection {
    use novarocks_execution::task_execution::status::TaskFailureCategory;

    let outcome = match rejection.category() {
        TaskFailureCategory::ResourceExhausted => OperationOutcome::ResourceExhausted,
        TaskFailureCategory::Protocol => OperationOutcome::InvalidStateOrRequest,
        TaskFailureCategory::Exchange => OperationOutcome::DestinationFailure,
        TaskFailureCategory::Execution | TaskFailureCategory::Internal => {
            OperationOutcome::InvalidStateOrRequest
        }
    };
    DomainRejection::new(outcome, rejection.detail().as_str())
}

fn classify(
    domains: &mut TaskDomains,
    update: &TaskDomainUpdate,
) -> Result<DomainProgression, DomainRejection> {
    let progression = match update {
        TaskDomainUpdate::SplitAssignment(intent) => {
            let watermark = domains.splits.watermark(intent.node());
            let progression = classify_split_batch(
                watermark,
                intent.first(),
                intent.last(),
                intent.no_more_splits(),
            );
            if matches!(progression, DomainProgression::Apply) {
                domains.splits.set_watermark(
                    intent.node(),
                    apply_split_batch(watermark, intent.last(), intent.no_more_splits()),
                );
            }
            progression
        }
        TaskDomainUpdate::TaskDynamicFilter { version, payload } => {
            let progression = domains
                .dynamic_filter
                .classify(*version, payload.fingerprint());
            if matches!(progression, DomainProgression::Apply) {
                domains.dynamic_filter = domains
                    .dynamic_filter
                    .apply(*version, payload.fingerprint());
            }
            progression
        }
        TaskDomainUpdate::OpenExchangeEdges { version, edges } => {
            let progression = domains.edges.classify_open(*version, edges);
            if matches!(progression, DomainProgression::Apply) {
                domains.edges.apply_open(*version, edges);
            }
            progression
        }
    };
    if let DomainProgression::Conflict(conflict) = progression {
        return Err(DomainRejection::new(
            OperationOutcome::DomainConflict,
            conflict.to_string(),
        ));
    }
    Ok(progression)
}

/// Classifies one split batch, routing a pure terminal marker to the
/// watermark's own marker rule.
///
/// A batch whose whole range is already accepted is `Idempotent` by the
/// batch rule, which on its own would swallow the seal that the same request
/// also carries. `classify_no_more` is the rule for exactly that fact, so a
/// re-offered range that newly seals the node still seals it.
fn classify_split_batch(
    watermark: SplitWatermark,
    first: SplitSequence,
    last: SplitSequence,
    no_more: bool,
) -> DomainProgression {
    let already_accepted = last.get() < watermark.next_expected();
    if no_more && already_accepted && !watermark.no_more_splits() {
        return watermark.classify_no_more(Some(last));
    }
    watermark.classify_batch(first, last, no_more)
}

fn apply_split_batch(
    watermark: SplitWatermark,
    last: SplitSequence,
    no_more: bool,
) -> SplitWatermark {
    if last.get() < watermark.next_expected() {
        // The range was already accepted; only the terminal marker is new.
        return watermark.apply_no_more();
    }
    watermark.apply_batch(last, no_more)
}

fn commit(
    domains: &mut TaskDomains,
    update: &TaskDomainUpdate,
    progression: DomainProgression,
) -> TaskDomainReceipt {
    match update {
        TaskDomainUpdate::SplitAssignment(intent) => {
            let watermark = domains.splits.watermark(intent.node());
            let watermark = if matches!(progression, DomainProgression::Apply) {
                let next = apply_split_batch(watermark, intent.last(), intent.no_more_splits());
                domains.splits.set_watermark(intent.node(), next);
                next
            } else {
                watermark
            };
            TaskDomainReceipt::SplitAssignment {
                nodes: vec![PlanNodeSplitReceipt::new(intent.node(), watermark)],
                progression,
            }
        }
        TaskDomainUpdate::TaskDynamicFilter { version, payload } => {
            if matches!(progression, DomainProgression::Apply) {
                domains.dynamic_filter = domains
                    .dynamic_filter
                    .apply(*version, payload.fingerprint());
            }
            TaskDomainReceipt::TaskDynamicFilter {
                accepted_version: domains.dynamic_filter.accepted_version(),
                progression,
            }
        }
        TaskDomainUpdate::OpenExchangeEdges { version, edges } => {
            if matches!(progression, DomainProgression::Apply) {
                domains.edges.apply_open(*version, edges);
            }
            TaskDomainReceipt::OpenExchangeEdges {
                opened: opened_edges(&domains.edges, edges),
                progression,
            }
        }
    }
}

fn opened_edges(domain: &ExchangeEdgeDomain, requested: &[ExchangeEdgeId]) -> Vec<ExchangeEdgeId> {
    use novarocks_execution::task_execution::domain::EdgeSendPermission;

    requested
        .iter()
        .copied()
        .filter(|edge| domain.permission(*edge) == Some(EdgeSendPermission::Open))
        .collect()
}

/// A comparable, secret-free key of one initial domain.
///
/// A create replay must be judged on its initial domains as well as its
/// descriptor, and a payload is codec-owned, so this folds each update to its
/// tokens plus a content fingerprint.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum InitialDomainKey {
    SplitAssignment {
        node: PlanNodeId,
        first: SplitSequence,
        last: SplitSequence,
        no_more: bool,
        payload: novarocks_execution::task_execution::domain::ContentFingerprint,
    },
    TaskDynamicFilter {
        version: novarocks_execution::task_execution::domain::DomainVersion,
        payload: novarocks_execution::task_execution::domain::ContentFingerprint,
    },
    OpenExchangeEdges {
        version: EdgeOpenVersion,
        edges: Vec<ExchangeEdgeId>,
    },
}

pub(super) fn initial_domain_keys(updates: &[TaskDomainUpdate]) -> Vec<InitialDomainKey> {
    updates
        .iter()
        .map(|update| match update {
            TaskDomainUpdate::SplitAssignment(intent) => InitialDomainKey::SplitAssignment {
                node: intent.node(),
                first: intent.first(),
                last: intent.last(),
                no_more: intent.no_more_splits(),
                payload: intent.payload().fingerprint(),
            },
            TaskDomainUpdate::TaskDynamicFilter { version, payload } => {
                InitialDomainKey::TaskDynamicFilter {
                    version: *version,
                    payload: payload.fingerprint(),
                }
            }
            TaskDomainUpdate::OpenExchangeEdges { version, edges } => {
                InitialDomainKey::OpenExchangeEdges {
                    version: *version,
                    edges: edges.clone(),
                }
            }
        })
        .collect()
}
