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
            // A filter domain has nothing descriptor-level to check, and that
            // is not an omission. The descriptor freezes plan nodes and
            // exchange edges, so those two can be fenced here; it freezes no
            // filter channel or binding, because a task's filter role comes
            // from the query context's installed contribution rather than from
            // its own plan. ADR-0044 puts that fence one layer down: an
            // envelope is decoded against the *installed* contract before any
            // reduction, by the owner that holds it. Inventing a descriptor
            // field to check here would create a second authority over the
            // same fact.
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

/// Classifies one operation's domain updates without applying anything.
///
/// It runs against a copy, so two batches for the same plan node inside one
/// request are judged in order while nothing is committed. The result decides
/// which updates the execution side is asked to apply.
pub(super) fn plan_updates(
    descriptor: &TaskDescriptor,
    domains: &TaskDomains,
    updates: &[TaskDomainUpdate],
) -> Result<Vec<DomainProgression>, DomainRejection> {
    validate_membership(descriptor, updates)?;
    let mut speculative = domains.clone();
    updates
        .iter()
        .map(|update| classify(&mut speculative, update))
        .collect()
}

/// Applies one operation's domain updates to the authoritative token state.
///
/// Every update is re-classified here, against the tokens as they are now
/// rather than as they were when the plan was made. That is what keeps two
/// concurrent advances from rolling a token backwards: an operation whose
/// version was overtaken while the execution side was applying finds itself
/// `Older` and commits nothing.
pub(super) fn commit_updates(
    domains: &mut TaskDomains,
    updates: &[TaskDomainUpdate],
    queued: &[Option<u64>],
) -> Result<(Vec<TaskDomainReceipt>, bool), DomainRejection> {
    let mut receipts = Vec::with_capacity(updates.len());
    let mut applied_any = false;
    for (index, update) in updates.iter().enumerate() {
        let mut speculative = domains.clone();
        let progression = classify(&mut speculative, update)?;
        applied_any |= matches!(progression, DomainProgression::Apply);
        receipts.push(commit(
            domains,
            update,
            progression,
            queued.get(index).copied().flatten(),
        ));
    }
    Ok((receipts, applied_any))
}

/// Classifies, applies through the execution side, then commits.
///
/// This is the single-threaded form, for a task identity a creation
/// transaction still owns exclusively.
pub(super) fn apply_updates(
    host: &dyn TaskExecutionHost,
    descriptor: &TaskDescriptor,
    domains: &mut TaskDomains,
    updates: &[TaskDomainUpdate],
) -> Result<(Vec<TaskDomainReceipt>, bool), DomainRejection> {
    let plan = plan_updates(descriptor, domains, updates)?;
    let queued = apply_planned(host, descriptor, updates, &plan)?;
    commit_updates(domains, updates, &queued)
}

/// Asks the execution side to apply exactly the updates the plan accepted.
///
/// A rejection here leaves every token where it was, so the identical request
/// stays a clean replay instead of a partially advanced domain.
pub(super) fn apply_planned(
    host: &dyn TaskExecutionHost,
    descriptor: &TaskDescriptor,
    updates: &[TaskDomainUpdate],
    plan: &[DomainProgression],
) -> Result<Vec<Option<u64>>, DomainRejection> {
    let mut queued = Vec::with_capacity(updates.len());
    for (update, progression) in updates.iter().zip(plan) {
        if reaches_execution(update, *progression) {
            match host.apply_task_domain(descriptor, update) {
                Ok(depth) => queued.push(depth),
                Err(rejection) => {
                    // The only place this reason exists. The frontend's
                    // acknowledgement carries the outcome but not the detail,
                    // so a refused domain update reaches the client as a bare
                    // "answered with InvalidStateOrRequest" and an operator
                    // has nothing to act on.
                    tracing::warn!(
                        task = %descriptor.identity(),
                        kind = ?update.kind(),
                        progression = ?progression,
                        category = ?rejection.category(),
                        detail = %rejection.detail(),
                        "task domain update refused by the execution host"
                    );
                    return Err(rejection_from_host(rejection));
                }
            }
        } else {
            // Nothing reached a queue, so nothing was measured. The receipt
            // reports the retained token without a depth rather than the depth
            // of an offer that never landed.
            queued.push(None);
        }
    }
    Ok(queued)
}

/// Whether one classified update still has to reach the execution side.
///
/// A strictly newer token always does. A split assignment the watermark
/// already covers does as well, and that is not a second application:
/// ADR-0123 puts duplicate recognition in the plan node's own queue, whose
/// preflight drops every sequence at or below its watermark before a provider
/// payload is decoded and whose terminal marker only transitions once. Routing
/// the replay there is the only thing that makes the retransmission the retry
/// rule depends on observable at the receiver, and the only thing that can
/// measure the queue depth the sender reads as backpressure. Withholding that
/// depth is not a smaller answer: the sender refuses a duplicate
/// acknowledgement that carries none, which strands every scan queued behind
/// the sealed batch.
///
/// The other two domains have no queue and are not replay-safe down there:
/// opening an exchange edge and installing a filter payload are applications,
/// so only a strictly newer token reaches them.
const fn reaches_execution(update: &TaskDomainUpdate, progression: DomainProgression) -> bool {
    match progression {
        DomainProgression::Apply => true,
        DomainProgression::Idempotent => {
            matches!(update, TaskDomainUpdate::SplitAssignment(_))
        }
        DomainProgression::Older | DomainProgression::Conflict(_) => false,
    }
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
            let progression = SplitWatermark::classify_offer(
                watermark,
                intent.first(),
                intent.last(),
                intent.no_more_splits(),
            );
            if matches!(progression, DomainProgression::Apply) {
                domains.splits.set_watermark(
                    intent.node(),
                    watermark.apply_offer(intent.last(), intent.no_more_splits()),
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
fn commit(
    domains: &mut TaskDomains,
    update: &TaskDomainUpdate,
    progression: DomainProgression,
    queued_splits: Option<u64>,
) -> TaskDomainReceipt {
    match update {
        TaskDomainUpdate::SplitAssignment(intent) => {
            let watermark = domains.splits.watermark(intent.node());
            let watermark = if matches!(progression, DomainProgression::Apply) {
                let next = watermark.apply_offer(intent.last(), intent.no_more_splits());
                domains.splits.set_watermark(intent.node(), next);
                next
            } else {
                watermark
            };
            let mut node = PlanNodeSplitReceipt::new(intent.node(), watermark);
            if let Some(depth) = queued_splits {
                node = node.with_queued_splits(depth);
            }
            TaskDomainReceipt::SplitAssignment {
                nodes: vec![node],
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
