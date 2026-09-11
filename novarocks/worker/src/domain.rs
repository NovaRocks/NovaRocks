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

//! Worker-owned accepted state and progression policy for mutable domains.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::Arc;

use novarocks_execution_contract::{
    ConfidentialContent, ContentFingerprint, CredentialEpoch, CredentialLeaseId, CredentialUpdate,
    DomainConflict, DomainProgression, DomainVersion, EdgeOpenVersion, EdgeSendPermission,
    ExchangeEdgeId, PlanNodeId, PlanNodeSplitReceipt, QueryContextDomainReceipt,
    QueryContextDomainUpdate, SplitOffer, SplitSequence, SplitWatermark, TaskDescriptor,
    TaskDomainReceipt, TaskDomainUpdate,
};

/// Accepted state for a scalar-versioned Worker domain.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
struct ScalarDomain {
    accepted: Option<(DomainVersion, ContentFingerprint)>,
    sealed: bool,
}

impl Default for ScalarDomain {
    fn default() -> Self {
        Self::empty()
    }
}

impl ScalarDomain {
    pub const fn empty() -> Self {
        Self {
            accepted: None,
            sealed: false,
        }
    }

    pub const fn accepted_version(self) -> Option<DomainVersion> {
        match self.accepted {
            Some((version, _)) => Some(version),
            None => None,
        }
    }

    pub fn classify(
        self,
        version: DomainVersion,
        fingerprint: ContentFingerprint,
    ) -> DomainProgression {
        match self.accepted {
            Some((accepted_version, accepted_fingerprint)) if accepted_version == version => {
                if accepted_fingerprint == fingerprint {
                    DomainProgression::Idempotent
                } else {
                    DomainProgression::Conflict(DomainConflict::SameTokenDifferentContent)
                }
            }
            Some((accepted_version, _)) if version < accepted_version => DomainProgression::Older,
            _ if self.sealed => DomainProgression::Conflict(DomainConflict::AfterSeal),
            _ => DomainProgression::Apply,
        }
    }

    pub fn apply(self, version: DomainVersion, fingerprint: ContentFingerprint) -> Self {
        Self {
            accepted: Some((version, fingerprint)),
            sealed: self.sealed,
        }
    }
}

#[derive(Copy, Clone, Debug, Default, Eq, PartialEq)]
struct AcceptedSplitWatermark {
    accepted_through: Option<SplitSequence>,
    no_more: bool,
}

impl AcceptedSplitWatermark {
    const fn next_expected(self) -> u64 {
        match self.accepted_through {
            Some(sequence) => sequence.get() + 1,
            None => 1,
        }
    }

    fn classify(self, offer: SplitOffer) -> DomainProgression {
        let SplitOffer::Batch {
            first,
            last,
            no_more,
        } = offer
        else {
            return self.classify_no_more(None);
        };
        let already_accepted = last.get() < self.next_expected();
        if no_more && already_accepted && !self.no_more {
            return self.classify_no_more(Some(last));
        }
        if last < first {
            return DomainProgression::Conflict(DomainConflict::NotMonotonic);
        }
        if already_accepted {
            return DomainProgression::Idempotent;
        }
        if self.no_more {
            return DomainProgression::Conflict(DomainConflict::AfterSeal);
        }
        if first.get() > self.next_expected() {
            return DomainProgression::Conflict(DomainConflict::Gap);
        }
        DomainProgression::Apply
    }

    fn classify_no_more(self, through: Option<SplitSequence>) -> DomainProgression {
        if self.no_more {
            return match through {
                Some(through) if self.accepted_through.is_some_and(|value| through > value) => {
                    DomainProgression::Conflict(DomainConflict::AfterSeal)
                }
                _ => DomainProgression::Idempotent,
            };
        }
        match (through, self.accepted_through) {
            (Some(through), Some(accepted)) if through < accepted => {
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
                accepted_through: Some(match self.accepted_through {
                    Some(accepted) if accepted > last => accepted,
                    _ => last,
                }),
                no_more: self.no_more || no_more,
            },
        }
    }

    fn receipt(self) -> SplitWatermark {
        let mut receipt = SplitWatermark::empty();
        if let Some(accepted) = self.accepted_through {
            receipt = receipt.apply_batch(accepted, self.no_more);
        } else if self.no_more {
            receipt = receipt.apply_no_more();
        }
        receipt
    }
}

/// Accepted split watermarks, independently keyed by plan node.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct SplitDomain {
    nodes: BTreeMap<PlanNodeId, AcceptedSplitWatermark>,
}

impl SplitDomain {
    pub fn new() -> Self {
        Self::default()
    }

    fn watermark(&self, node: PlanNodeId) -> AcceptedSplitWatermark {
        self.nodes.get(&node).copied().unwrap_or_default()
    }

    fn set_watermark(&mut self, node: PlanNodeId, watermark: AcceptedSplitWatermark) {
        self.nodes.insert(node, watermark);
    }
}

/// Accepted credential epoch and its exact confidential replay material.
#[derive(Clone)]
struct CredentialDomain {
    lease_id: CredentialLeaseId,
    accepted_epoch: CredentialEpoch,
    material: Arc<dyn ConfidentialContent>,
}

impl fmt::Debug for CredentialDomain {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CredentialDomain")
            .field("lease_id", &self.lease_id)
            .field("accepted_epoch", &self.accepted_epoch)
            .field("material", &"[REDACTED]")
            .finish()
    }
}

impl CredentialDomain {
    pub fn install(
        lease_id: CredentialLeaseId,
        epoch: CredentialEpoch,
        material: Arc<dyn ConfidentialContent>,
    ) -> Self {
        Self {
            lease_id,
            accepted_epoch: epoch,
            material,
        }
    }

    pub const fn accepted_epoch(&self) -> CredentialEpoch {
        self.accepted_epoch
    }

    pub fn matches(&self, update: &CredentialUpdate) -> bool {
        self.lease_id == update.lease_id()
            && self.accepted_epoch == update.epoch()
            && update.matches_installed(&*self.material)
    }

    fn classify(&self, update: &CredentialUpdate) -> DomainProgression {
        if update.lease_id() != self.lease_id {
            return DomainProgression::Conflict(DomainConflict::UnknownMember);
        }
        if update.epoch() == self.accepted_epoch {
            return if update.matches_installed(&*self.material) {
                DomainProgression::Idempotent
            } else {
                DomainProgression::Conflict(DomainConflict::SameTokenDifferentContent)
            };
        }
        if update.epoch() < self.accepted_epoch {
            return DomainProgression::Older;
        }
        match self.accepted_epoch.next() {
            Some(next) if next == update.epoch() => DomainProgression::Apply,
            _ => DomainProgression::Conflict(DomainConflict::Gap),
        }
    }

    fn apply(&mut self, update: &CredentialUpdate) {
        self.accepted_epoch = update.epoch();
        self.material = Arc::clone(update.material());
    }
}

/// Accepted edge-open versions and frozen edge permissions.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct ExchangeEdgeDomain {
    edges: BTreeMap<ExchangeEdgeId, EdgeSendPermission>,
    opened_sets: BTreeMap<EdgeOpenVersion, BTreeSet<ExchangeEdgeId>>,
}

impl ExchangeEdgeDomain {
    pub fn from_frozen_edges(edges: impl IntoIterator<Item = ExchangeEdgeId>) -> Self {
        Self {
            edges: edges
                .into_iter()
                .map(|edge| (edge, EdgeSendPermission::Closed))
                .collect(),
            opened_sets: BTreeMap::new(),
        }
    }

    pub fn permission(&self, edge: ExchangeEdgeId) -> Option<EdgeSendPermission> {
        self.edges.get(&edge).copied()
    }

    fn accepted_version(&self) -> Option<EdgeOpenVersion> {
        self.opened_sets.keys().next_back().copied()
    }

    fn classify(
        &self,
        version: EdgeOpenVersion,
        requested: &[ExchangeEdgeId],
    ) -> DomainProgression {
        if requested.is_empty() {
            return DomainProgression::Conflict(DomainConflict::UnknownMember);
        }
        let mut set = BTreeSet::new();
        for edge in requested {
            if !self.edges.contains_key(edge) {
                return DomainProgression::Conflict(DomainConflict::UnknownMember);
            }
            if !set.insert(*edge) {
                return DomainProgression::Conflict(DomainConflict::SameTokenDifferentContent);
            }
        }
        if let Some(applied) = self.opened_sets.get(&version) {
            return if *applied == set {
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
        if set
            .iter()
            .any(|edge| self.edges.get(edge) == Some(&EdgeSendPermission::Open))
        {
            return DomainProgression::Conflict(DomainConflict::SameTokenDifferentContent);
        }
        DomainProgression::Apply
    }

    fn apply(&mut self, version: EdgeOpenVersion, requested: &[ExchangeEdgeId]) {
        for edge in requested {
            self.edges.insert(*edge, EdgeSendPermission::Open);
        }
        self.opened_sets
            .insert(version, requested.iter().copied().collect());
    }
}

/// Worker-owned accepted state for the three task domains.
#[derive(Clone, Debug, Default)]
pub struct TaskDomains {
    splits: SplitDomain,
    dynamic_filter: ScalarDomain,
    edges: ExchangeEdgeDomain,
}

impl TaskDomains {
    pub fn for_descriptor(descriptor: &TaskDescriptor) -> Self {
        Self {
            splits: SplitDomain::new(),
            dynamic_filter: ScalarDomain::empty(),
            edges: ExchangeEdgeDomain::from_frozen_edges(descriptor.topology().edge_ids()),
        }
    }
}

/// Worker-owned shared domain state for one query context.
#[derive(Clone, Debug, Default)]
pub struct QueryContextDomains {
    catalog: ScalarDomain,
    shared_filter: ScalarDomain,
    credential: Option<CredentialDomain>,
}

impl QueryContextDomains {
    pub fn empty() -> Self {
        Self::default()
    }

    pub fn install_initial(
        catalog: ContentFingerprint,
        shared_filter: ContentFingerprint,
        credential_lease: CredentialLeaseId,
        credential_epoch: CredentialEpoch,
        credential_material: Arc<dyn ConfidentialContent>,
    ) -> Self {
        Self {
            catalog: ScalarDomain::empty().apply(DomainVersion::FIRST, catalog),
            shared_filter: ScalarDomain::empty().apply(DomainVersion::FIRST, shared_filter),
            credential: Some(CredentialDomain::install(
                credential_lease,
                credential_epoch,
                credential_material,
            )),
        }
    }

    pub fn initial_credential_matches(&self, update: &CredentialUpdate) -> bool {
        self.credential
            .as_ref()
            .is_none_or(|credential| credential.matches(update))
    }

    pub fn classify(&self, update: &QueryContextDomainUpdate) -> DomainProgression {
        match update {
            QueryContextDomainUpdate::CatalogBinding { version, payload } => {
                self.catalog.classify(*version, payload.fingerprint())
            }
            QueryContextDomainUpdate::SharedDynamicFilter { version, payload } => {
                self.shared_filter.classify(*version, payload.fingerprint())
            }
            QueryContextDomainUpdate::Credential(update) => self.credential.as_ref().map_or(
                DomainProgression::Conflict(DomainConflict::UnknownMember),
                |credential| credential.classify(update),
            ),
        }
    }

    pub fn apply(&mut self, update: &QueryContextDomainUpdate) {
        match update {
            QueryContextDomainUpdate::CatalogBinding { version, payload } => {
                self.catalog = self.catalog.apply(*version, payload.fingerprint());
            }
            QueryContextDomainUpdate::SharedDynamicFilter { version, payload } => {
                self.shared_filter = self.shared_filter.apply(*version, payload.fingerprint());
            }
            QueryContextDomainUpdate::Credential(update) => {
                if let Some(credential) = &mut self.credential {
                    credential.apply(update);
                }
            }
        }
    }

    pub fn receipt(
        &self,
        update: &QueryContextDomainUpdate,
        progression: DomainProgression,
    ) -> QueryContextDomainReceipt {
        match update {
            QueryContextDomainUpdate::CatalogBinding { .. } => {
                QueryContextDomainReceipt::CatalogBinding {
                    accepted_version: self.catalog.accepted_version(),
                    progression,
                }
            }
            QueryContextDomainUpdate::SharedDynamicFilter { .. } => {
                QueryContextDomainReceipt::SharedDynamicFilter {
                    accepted_version: self.shared_filter.accepted_version(),
                    progression,
                }
            }
            QueryContextDomainUpdate::Credential(update) => QueryContextDomainReceipt::Credential {
                lease_id: update.lease_id(),
                accepted_epoch: self
                    .credential
                    .as_ref()
                    .map_or(update.epoch(), CredentialDomain::accepted_epoch),
                progression,
            },
        }
    }
}

/// A secret-free key used to classify exact create replays.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum InitialDomainKey {
    SplitAssignment {
        node: PlanNodeId,
        offer: SplitOffer,
        payload: ContentFingerprint,
    },
    TaskDynamicFilter {
        version: DomainVersion,
        payload: ContentFingerprint,
    },
    OpenExchangeEdges {
        version: EdgeOpenVersion,
        edges: Vec<ExchangeEdgeId>,
    },
}

pub fn initial_domain_keys(updates: &[TaskDomainUpdate]) -> Vec<InitialDomainKey> {
    updates
        .iter()
        .map(|update| match update {
            TaskDomainUpdate::SplitAssignment(intent) => InitialDomainKey::SplitAssignment {
                node: intent.node(),
                offer: intent.offer(),
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

/// A Worker policy refusal before any execution-side effect.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DomainPolicyRejection {
    conflict: DomainConflict,
    detail: String,
}

impl DomainPolicyRejection {
    fn new(conflict: DomainConflict, detail: impl Into<String>) -> Self {
        Self {
            conflict,
            detail: detail.into(),
        }
    }

    pub const fn conflict(&self) -> DomainConflict {
        self.conflict
    }

    pub fn detail(&self) -> &str {
        &self.detail
    }
}

pub fn validate_task_domain_membership(
    descriptor: &TaskDescriptor,
    updates: &[TaskDomainUpdate],
) -> Result<(), DomainPolicyRejection> {
    for update in updates {
        match update {
            TaskDomainUpdate::SplitAssignment(intent)
                if !descriptor.accepts_split_plan_node(intent.node()) =>
            {
                return Err(DomainPolicyRejection::new(
                    DomainConflict::UnknownMember,
                    format!(
                        "split assignment names plan node {} which the descriptor did not freeze",
                        intent.node()
                    ),
                ));
            }
            TaskDomainUpdate::OpenExchangeEdges { edges, .. } => {
                if let Some(edge) = edges
                    .iter()
                    .find(|edge| descriptor.topology().edge(**edge).is_none())
                {
                    return Err(DomainPolicyRejection::new(
                        DomainConflict::UnknownMember,
                        format!(
                            "edge open names exchange edge {edge} which the descriptor did not freeze"
                        ),
                    ));
                }
            }
            _ => {}
        }
    }
    Ok(())
}

pub fn plan_task_domain_updates(
    descriptor: &TaskDescriptor,
    domains: &TaskDomains,
    updates: &[TaskDomainUpdate],
) -> Result<Vec<DomainProgression>, DomainPolicyRejection> {
    validate_task_domain_membership(descriptor, updates)?;
    let mut speculative = domains.clone();
    updates
        .iter()
        .map(|update| classify_task_domain_update(&mut speculative, update))
        .collect()
}

pub fn commit_task_domain_updates(
    domains: &mut TaskDomains,
    updates: &[TaskDomainUpdate],
    queued: &[Option<u64>],
) -> Result<(Vec<TaskDomainReceipt>, bool), DomainPolicyRejection> {
    let mut receipts = Vec::with_capacity(updates.len());
    let mut applied_any = false;
    for (index, update) in updates.iter().enumerate() {
        let mut speculative = domains.clone();
        let progression = classify_task_domain_update(&mut speculative, update)?;
        applied_any |= matches!(progression, DomainProgression::Apply);
        receipts.push(commit_task_domain_update(
            domains,
            update,
            progression,
            queued.get(index).copied().flatten(),
        ));
    }
    Ok((receipts, applied_any))
}

pub const fn task_domain_reaches_execution(
    update: &TaskDomainUpdate,
    progression: DomainProgression,
) -> bool {
    match progression {
        DomainProgression::Apply => true,
        DomainProgression::Idempotent => {
            matches!(update, TaskDomainUpdate::SplitAssignment(_))
        }
        DomainProgression::Older | DomainProgression::Conflict(_) => false,
    }
}

fn classify_task_domain_update(
    domains: &mut TaskDomains,
    update: &TaskDomainUpdate,
) -> Result<DomainProgression, DomainPolicyRejection> {
    let progression = match update {
        TaskDomainUpdate::SplitAssignment(intent) => {
            let watermark = domains.splits.watermark(intent.node());
            let progression = watermark.classify(intent.offer());
            if matches!(progression, DomainProgression::Apply) {
                domains
                    .splits
                    .set_watermark(intent.node(), watermark.apply(intent.offer()));
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
            let progression = domains.edges.classify(*version, edges);
            if matches!(progression, DomainProgression::Apply) {
                domains.edges.apply(*version, edges);
            }
            progression
        }
    };
    if let DomainProgression::Conflict(conflict) = progression {
        return Err(DomainPolicyRejection::new(
            conflict,
            format!("{conflict} ({})", conflicting_token(update)),
        ));
    }
    Ok(progression)
}

fn conflicting_token(update: &TaskDomainUpdate) -> String {
    match update {
        TaskDomainUpdate::SplitAssignment(intent) => format!(
            "domain=split_assignment plan_node={} {}",
            intent.node(),
            intent.offer()
        ),
        TaskDomainUpdate::TaskDynamicFilter { version, .. } => {
            format!("domain=task_dynamic_filter version={}", version.get())
        }
        TaskDomainUpdate::OpenExchangeEdges { version, edges } => format!(
            "domain=open_exchange_edges version={} edges={:?}",
            version.get(),
            edges
        ),
    }
}

fn commit_task_domain_update(
    domains: &mut TaskDomains,
    update: &TaskDomainUpdate,
    progression: DomainProgression,
    queued_splits: Option<u64>,
) -> TaskDomainReceipt {
    match update {
        TaskDomainUpdate::SplitAssignment(intent) => {
            let watermark = domains.splits.watermark(intent.node());
            let watermark = if matches!(progression, DomainProgression::Apply) {
                let next = watermark.apply(intent.offer());
                domains.splits.set_watermark(intent.node(), next);
                next
            } else {
                watermark
            };
            let mut node = PlanNodeSplitReceipt::new(intent.node(), watermark.receipt());
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
                domains.edges.apply(*version, edges);
            }
            TaskDomainReceipt::OpenExchangeEdges {
                accepted_version: domains
                    .edges
                    .accepted_version()
                    .expect("an accepted edge update must retain its version"),
                opened: edges
                    .iter()
                    .copied()
                    .filter(|edge| {
                        domains.edges.permission(*edge) == Some(EdgeSendPermission::Open)
                    })
                    .collect(),
                progression,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{AcceptedSplitWatermark, CredentialDomain, ExchangeEdgeDomain, ScalarDomain};
    use novarocks_execution_contract::{
        ConfidentialContent, ContentFingerprint, CredentialEpoch, CredentialLeaseId,
        CredentialUpdate, DomainConflict, DomainProgression, DomainVersion, EdgeOpenVersion,
        EdgeSendPermission, ExchangeEdgeId, SplitOffer, SplitSequence,
    };
    use std::sync::Arc;

    struct Secret(u8);

    impl ConfidentialContent for Secret {
        fn encoded_len(&self) -> usize {
            1
        }

        fn matches(&self, other: &dyn ConfidentialContent) -> bool {
            other
                .stored_representation()
                .and_then(|value| value.downcast_ref::<Self>())
                .is_some_and(|other| other.0 == self.0)
        }

        fn stored_representation(&self) -> Option<&(dyn std::any::Any + 'static)> {
            Some(self)
        }
    }

    fn secret(value: u8) -> Arc<dyn ConfidentialContent> {
        Arc::new(Secret(value))
    }

    #[test]
    fn scalar_progression_is_monotonic_and_sealed() {
        let first = DomainVersion::FIRST;
        let fingerprint = ContentFingerprint::from_bytes([1; 16]);
        let domain = ScalarDomain {
            accepted: Some((first, fingerprint)),
            sealed: true,
        };
        assert_eq!(
            domain.classify(first, fingerprint),
            DomainProgression::Idempotent
        );
        assert_eq!(
            domain.classify(
                DomainVersion::new(2).expect("nonzero version"),
                ContentFingerprint::from_bytes([2; 16]),
            ),
            DomainProgression::Conflict(DomainConflict::AfterSeal)
        );
    }

    #[test]
    fn credential_equality_stays_with_the_worker_owner() {
        let lease = CredentialLeaseId::new(7);
        let mut domain = CredentialDomain::install(lease, CredentialEpoch::FIRST, secret(1));
        let replay = CredentialUpdate::new(lease, CredentialEpoch::FIRST, secret(1));
        assert_eq!(domain.classify(&replay), DomainProgression::Idempotent);
        let next = CredentialUpdate::new(
            lease,
            CredentialEpoch::new(2).expect("nonzero epoch"),
            secret(2),
        );
        assert_eq!(domain.classify(&next), DomainProgression::Apply);
        domain.apply(&next);
        assert_eq!(domain.accepted_epoch(), next.epoch());
    }

    #[test]
    fn split_progression_enforces_contiguous_watermarks_and_sealing() {
        let sequence = |value| SplitSequence::new(value).expect("nonzero sequence");
        let first = SplitOffer::Batch {
            first: sequence(1),
            last: sequence(2),
            no_more: false,
        };
        let gap = SplitOffer::Batch {
            first: sequence(4),
            last: sequence(4),
            no_more: false,
        };
        let after_seal = SplitOffer::Batch {
            first: sequence(3),
            last: sequence(3),
            no_more: false,
        };

        let empty = AcceptedSplitWatermark::default();
        assert_eq!(
            empty.classify(gap),
            DomainProgression::Conflict(DomainConflict::Gap)
        );
        assert_eq!(empty.classify(first), DomainProgression::Apply);
        let accepted = empty.apply(first);
        assert_eq!(accepted.classify(first), DomainProgression::Idempotent);
        assert_eq!(
            accepted.classify(SplitOffer::Seal),
            DomainProgression::Apply
        );
        let sealed = accepted.apply(SplitOffer::Seal);
        assert_eq!(
            sealed.classify(after_seal),
            DomainProgression::Conflict(DomainConflict::AfterSeal)
        );
    }

    #[test]
    fn exchange_progression_preserves_frozen_membership_and_exact_version_replay() {
        let edge = |value| ExchangeEdgeId::new(value).expect("nonzero edge");
        let version = |value| EdgeOpenVersion::new(value).expect("nonzero version");
        let first = edge(1);
        let second = edge(2);
        let third = edge(3);
        let foreign = edge(4);
        let mut domain = ExchangeEdgeDomain::from_frozen_edges([first, second, third]);

        assert_eq!(
            domain.classify(version(1), &[first]),
            DomainProgression::Apply
        );
        domain.apply(version(1), &[first]);
        assert_eq!(domain.permission(first), Some(EdgeSendPermission::Open));
        assert_eq!(
            domain.classify(version(1), &[first]),
            DomainProgression::Idempotent
        );
        assert_eq!(
            domain.classify(version(1), &[second]),
            DomainProgression::Conflict(DomainConflict::SameTokenDifferentContent)
        );
        assert_eq!(
            domain.classify(version(3), &[second]),
            DomainProgression::Apply
        );
        domain.apply(version(3), &[second]);
        assert_eq!(
            domain.classify(version(2), &[third]),
            DomainProgression::Older,
            "an unseen version below the high watermark cannot roll policy backwards"
        );
        assert_eq!(
            domain.classify(version(2), &[foreign]),
            DomainProgression::Conflict(DomainConflict::UnknownMember)
        );
    }
}
