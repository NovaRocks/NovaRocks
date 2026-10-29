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

//! Pure domain facts. Confidential content remains in driver custody.
use novarocks_execution_contract::{
    ContentFingerprint, CredentialEpoch, CredentialLeaseId, DomainConflict, DomainProgression,
    DomainVersion, EdgeOpenVersion, EdgeSendPermission, ExchangeEdgeId, PlanNodeId,
    PlanNodeSplitReceipt, QueryContextDomainReceipt, SplitOffer, SplitSequence, SplitWatermark,
    TaskDomainReceipt, TaskIdentity,
};
use std::collections::{BTreeMap, BTreeSet};
/// Accepted state for a scalar-versioned Worker domain.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct ScalarDomain {
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
pub struct AcceptedSplitWatermark {
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

/// Accepted edge-open versions and frozen edge permissions.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct ExchangeEdgeDomain {
    members: BTreeSet<ExchangeEdgeId>,
    opened: BTreeSet<ExchangeEdgeId>,
    opened_sets: BTreeMap<EdgeOpenVersion, BTreeSet<ExchangeEdgeId>>,
}

/// One retained verdict per version and frozen destination. Each successful
/// version consumes a previously open member, so this cannot outgrow the
/// producer descriptor's destination count.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct ExchangeDestinationClosureDomain {
    accepted: BTreeMap<DomainVersion, (ExchangeEdgeId, TaskIdentity)>,
    closed: BTreeSet<(ExchangeEdgeId, TaskIdentity)>,
}

impl ExchangeDestinationClosureDomain {
    fn classify(
        &self,
        version: DomainVersion,
        member: (ExchangeEdgeId, TaskIdentity),
    ) -> DomainProgression {
        if let Some(accepted) = self.accepted.get(&version) {
            return if *accepted == member {
                DomainProgression::Idempotent
            } else {
                DomainProgression::Conflict(DomainConflict::SameTokenDifferentContent)
            };
        }
        if self
            .accepted
            .keys()
            .next_back()
            .is_some_and(|highest| version < *highest)
        {
            return DomainProgression::Older;
        }
        if self.closed.contains(&member) {
            return DomainProgression::Conflict(DomainConflict::NotMonotonic);
        }
        DomainProgression::Apply
    }

    fn apply(&mut self, version: DomainVersion, member: (ExchangeEdgeId, TaskIdentity)) {
        self.accepted.insert(version, member);
        self.closed.insert(member);
    }

    fn accepted_version(&self) -> Option<DomainVersion> {
        self.accepted.keys().next_back().copied()
    }
}

impl ExchangeEdgeDomain {
    pub fn permission(&self, edge: ExchangeEdgeId) -> Option<EdgeSendPermission> {
        self.members.contains(&edge).then(|| {
            if self.opened.contains(&edge) {
                EdgeSendPermission::Open
            } else {
                EdgeSendPermission::Closed
            }
        })
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
            if !self.members.contains(edge) {
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
        if set.iter().any(|edge| self.opened.contains(edge)) {
            return DomainProgression::Conflict(DomainConflict::SameTokenDifferentContent);
        }
        DomainProgression::Apply
    }

    fn apply(&mut self, version: EdgeOpenVersion, requested: &[ExchangeEdgeId]) {
        for edge in requested {
            self.opened.insert(*edge);
        }
        self.opened_sets
            .insert(version, requested.iter().copied().collect());
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TaskDomainInput {
    Split {
        node: PlanNodeId,
        offer: SplitOffer,
    },
    Filter {
        version: DomainVersion,
        fingerprint: ContentFingerprint,
    },
    Open {
        version: EdgeOpenVersion,
        edges: Vec<ExchangeEdgeId>,
    },
    Close {
        version: DomainVersion,
        edge: ExchangeEdgeId,
        destination: TaskIdentity,
    },
}
#[derive(Clone, Debug, Default)]
pub struct TaskDomains {
    split_members: BTreeSet<PlanNodeId>,
    destinations: BTreeSet<(ExchangeEdgeId, TaskIdentity)>,
    splits: SplitDomain,
    dynamic_filter: ScalarDomain,
    edges: ExchangeEdgeDomain,
    closed_destinations: ExchangeDestinationClosureDomain,
}
impl TaskDomains {
    /// Retire at most one original neutral member or empty nested row. False
    /// means one real member/row was removed; true means every owned dynamic
    /// collection is empty and dropping the outer value performs no traversal.
    pub(crate) fn retire_one(&mut self) -> bool {
        if self.split_members.pop_first().is_some()
            || self.destinations.pop_first().is_some()
            || self.splits.nodes.pop_first().is_some()
            || self.edges.members.pop_first().is_some()
            || self.edges.opened.pop_first().is_some()
        {
            return false;
        }
        if let Some(mut row) = self.edges.opened_sets.first_entry() {
            if row.get_mut().pop_first().is_none() {
                // The nested original set is already empty, so removing this
                // version row cannot hide another member traversal in Drop.
                row.remove();
            }
            return false;
        }
        if self.closed_destinations.accepted.pop_first().is_some()
            || self.closed_destinations.closed.pop_first().is_some()
        {
            return false;
        }
        true
    }
    pub fn new(
        split_nodes: impl IntoIterator<Item = PlanNodeId>,
        edges: impl IntoIterator<Item = ExchangeEdgeId>,
        destinations: impl IntoIterator<Item = (ExchangeEdgeId, TaskIdentity)>,
    ) -> Self {
        Self::from_frozen_shape(
            split_nodes.into_iter().collect(),
            edges.into_iter().collect(),
            destinations.into_iter().collect(),
        )
    }
    /// Frozen descriptor sets transfer into the actor in constant time. Their
    /// structural decoding, duplicate checks and resource charge precede this
    /// handoff; no initial metadata traversal is hidden in ContextCore.
    pub fn from_frozen_shape(
        split_members: BTreeSet<PlanNodeId>,
        members: BTreeSet<ExchangeEdgeId>,
        destinations: BTreeSet<(ExchangeEdgeId, TaskIdentity)>,
    ) -> Self {
        Self {
            split_members,
            destinations,
            splits: SplitDomain::new(),
            edges: ExchangeEdgeDomain {
                members,
                opened: BTreeSet::new(),
                opened_sets: BTreeMap::new(),
            },
            ..Self::default()
        }
    }
    pub fn classify(&self, update: &TaskDomainInput) -> DomainProgression {
        match update {
            TaskDomainInput::Split { node, offer } => {
                if !self.split_members.contains(node) {
                    return DomainProgression::Conflict(DomainConflict::UnknownMember);
                }
                self.splits.watermark(*node).classify(*offer)
            }
            TaskDomainInput::Filter {
                version,
                fingerprint,
            } => self.dynamic_filter.classify(*version, *fingerprint),
            TaskDomainInput::Open { version, edges } => self.edges.classify(*version, edges),
            TaskDomainInput::Close {
                version,
                edge,
                destination,
            } => {
                if !self.destinations.contains(&(*edge, *destination)) {
                    return DomainProgression::Conflict(DomainConflict::UnknownMember);
                }
                self.closed_destinations
                    .classify(*version, (*edge, *destination))
            }
        }
    }
    /// The caller invokes this only after the corresponding bounded host
    /// effect has been accepted into the step batch, before external publish.
    pub fn apply(&mut self, update: &TaskDomainInput) -> Result<DomainProgression, DomainConflict> {
        let progression = self.classify(update);
        if let DomainProgression::Conflict(conflict) = progression {
            return Err(conflict);
        }
        if progression == DomainProgression::Apply {
            match update {
                TaskDomainInput::Split { node, offer } => {
                    let current = self.splits.watermark(*node);
                    self.splits.set_watermark(*node, current.apply(*offer));
                }
                TaskDomainInput::Filter {
                    version,
                    fingerprint,
                } => {
                    self.dynamic_filter = self.dynamic_filter.apply(*version, *fingerprint);
                }
                TaskDomainInput::Open { version, edges } => self.edges.apply(*version, edges),
                TaskDomainInput::Close {
                    version,
                    edge,
                    destination,
                } => self
                    .closed_destinations
                    .apply(*version, (*edge, *destination)),
            }
        }
        Ok(progression)
    }
    pub fn permission(&self, edge: ExchangeEdgeId) -> Option<EdgeSendPermission> {
        self.edges.permission(edge)
    }
    pub fn receipt(
        &self,
        update: &TaskDomainInput,
        progression: DomainProgression,
        queued: Option<u64>,
    ) -> TaskDomainReceipt {
        match update {
            TaskDomainInput::Split { node, .. } => TaskDomainReceipt::SplitAssignment {
                nodes: vec![match queued {
                    Some(depth) => {
                        PlanNodeSplitReceipt::new(*node, self.splits.watermark(*node).receipt())
                            .with_queued_splits(depth)
                    }
                    None => {
                        PlanNodeSplitReceipt::new(*node, self.splits.watermark(*node).receipt())
                    }
                }],
                progression,
            },
            TaskDomainInput::Filter { .. } => TaskDomainReceipt::TaskDynamicFilter {
                accepted_version: self.dynamic_filter.accepted_version(),
                progression,
            },
            TaskDomainInput::Open { edges, .. } => TaskDomainReceipt::OpenExchangeEdges {
                accepted_version: self
                    .edges
                    .accepted_version()
                    .expect("accepted edge update has a version"),
                opened: edges
                    .iter()
                    .copied()
                    .filter(|edge| self.edges.permission(*edge) == Some(EdgeSendPermission::Open))
                    .collect(),
                progression,
            },
            TaskDomainInput::Close { .. } => TaskDomainReceipt::CloseExchangeDestination {
                accepted_version: self
                    .closed_destinations
                    .accepted_version()
                    .expect("accepted close has a version"),
                progression,
            },
        }
    }
}

/// `same_material` is measured against this exact resource identity by the
/// credential owner. The pure core never hashes or retains secret material.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CredentialInput {
    pub lease: CredentialLeaseId,
    pub epoch: CredentialEpoch,
    pub current_resource: u64,
    pub replacement_resource: u64,
    pub same_material: bool,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CredentialState {
    pub lease: CredentialLeaseId,
    pub epoch: CredentialEpoch,
    pub resource: u64,
}
impl CredentialState {
    pub fn classify(self, update: CredentialInput) -> DomainProgression {
        if update.lease != self.lease || update.current_resource != self.resource {
            return DomainProgression::Conflict(DomainConflict::UnknownMember);
        }
        if update.epoch == self.epoch {
            return if update.same_material {
                DomainProgression::Idempotent
            } else {
                DomainProgression::Conflict(DomainConflict::SameTokenDifferentContent)
            };
        }
        if update.epoch < self.epoch {
            return DomainProgression::Older;
        }
        if self.epoch.next() == Some(update.epoch) {
            DomainProgression::Apply
        } else {
            DomainProgression::Conflict(DomainConflict::Gap)
        }
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ContextDomainInput {
    Catalog {
        version: DomainVersion,
        fingerprint: ContentFingerprint,
    },
    Filter {
        version: DomainVersion,
        fingerprint: ContentFingerprint,
    },
    Credential(CredentialInput),
}
#[derive(Clone, Debug, Default)]
pub struct QueryContextDomains {
    catalog: ScalarDomain,
    shared_filter: ScalarDomain,
    credential: Option<CredentialState>,
}
impl QueryContextDomains {
    pub const fn credential(&self) -> Option<CredentialState> {
        self.credential
    }
    pub fn install_initial(
        catalog: ContentFingerprint,
        filter: ContentFingerprint,
        credential: CredentialState,
    ) -> Self {
        Self {
            catalog: ScalarDomain::empty().apply(DomainVersion::FIRST, catalog),
            shared_filter: ScalarDomain::empty().apply(DomainVersion::FIRST, filter),
            credential: Some(credential),
        }
    }
    pub fn classify(&self, input: ContextDomainInput) -> DomainProgression {
        match input {
            ContextDomainInput::Catalog {
                version,
                fingerprint,
            } => self.catalog.classify(version, fingerprint),
            ContextDomainInput::Filter {
                version,
                fingerprint,
            } => self.shared_filter.classify(version, fingerprint),
            ContextDomainInput::Credential(input) => self.credential.map_or(
                DomainProgression::Conflict(DomainConflict::UnknownMember),
                |current| current.classify(input),
            ),
        }
    }
    pub fn apply(
        &mut self,
        input: ContextDomainInput,
    ) -> Result<DomainProgression, DomainConflict> {
        let progression = self.classify(input);
        if let DomainProgression::Conflict(c) = progression {
            return Err(c);
        }
        if progression == DomainProgression::Apply {
            match input {
                ContextDomainInput::Catalog {
                    version,
                    fingerprint,
                } => self.catalog = self.catalog.apply(version, fingerprint),
                ContextDomainInput::Filter {
                    version,
                    fingerprint,
                } => self.shared_filter = self.shared_filter.apply(version, fingerprint),
                ContextDomainInput::Credential(input) => {
                    self.credential = Some(CredentialState {
                        lease: input.lease,
                        epoch: input.epoch,
                        resource: input.replacement_resource,
                    })
                }
            }
        }
        Ok(progression)
    }
    pub fn receipt(
        &self,
        input: ContextDomainInput,
        progression: DomainProgression,
    ) -> QueryContextDomainReceipt {
        match input {
            ContextDomainInput::Catalog { .. } => QueryContextDomainReceipt::CatalogBinding {
                accepted_version: self.catalog.accepted_version(),
                progression,
            },
            ContextDomainInput::Filter { .. } => QueryContextDomainReceipt::SharedDynamicFilter {
                accepted_version: self.shared_filter.accepted_version(),
                progression,
            },
            ContextDomainInput::Credential(input) => QueryContextDomainReceipt::Credential {
                lease_id: input.lease,
                accepted_epoch: self.credential.map_or(input.epoch, |c| c.epoch),
                progression,
            },
        }
    }
}
/// An owned projection journal. It borrows no host and visits at most one
/// frozen domain member per `advance`. Other operations for the Task queue
/// behind it until its host result commits or its journal rolls back.
/// The journal owns only unpublished speculative tokens of this request. It
/// never rewinds a previously committed request. Partially executed host work
/// keeps its original WorkLease and Task cleanup obligations after rollback;
/// restoring pure tokens is not evidence of host exit or physical convergence.
#[derive(Debug)]
pub struct TaskDomainTransaction {
    domains: TaskDomains,
    updates: std::collections::VecDeque<TaskDomainInput>,
    projection_index: usize,
    progressions: std::sync::Arc<Vec<DomainProgression>>,
    building_progressions: Vec<DomainProgression>,
    undo: Vec<DomainUndo>,
    open: Option<OpenProjection>,
    garbage: Vec<BTreeSet<ExchangeEdgeId>>,
    failure: Option<DomainConflict>,
    projected: bool,
    applied: bool,
    completion: Option<std::collections::VecDeque<Option<u64>>>,
    receipts: Vec<TaskDomainReceipt>,
    receipt_edges: Vec<ExchangeEdgeId>,
    receipt_index: usize,
}
#[derive(Debug)]
enum DomainUndo {
    Split(PlanNodeId, Option<AcceptedSplitWatermark>),
    Filter(ScalarDomain),
    Permission(ExchangeEdgeId, EdgeSendPermission),
    OpenSet(EdgeOpenVersion),
    Close(DomainVersion, (ExchangeEdgeId, TaskIdentity)),
}
#[derive(Debug)]
struct OpenProjection {
    index: usize,
    set: BTreeSet<ExchangeEdgeId>,
    any_open: bool,
    same_set: bool,
    applying: bool,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TaskDomainTransactionProgress {
    Pending,
    Ready,
    RolledBack(DomainConflict),
    Complete,
}
impl TaskDomainTransaction {
    pub fn new(domains: TaskDomains, updates: Vec<TaskDomainInput>) -> Self {
        Self {
            domains,
            updates: updates.into(),
            projection_index: 0,
            progressions: std::sync::Arc::new(Vec::new()),
            building_progressions: Vec::new(),
            undo: Vec::new(),
            open: None,
            garbage: Vec::new(),
            failure: None,
            projected: false,
            applied: false,
            completion: None,
            receipts: Vec::new(),
            receipt_edges: Vec::new(),
            receipt_index: 0,
        }
    }
    pub fn progressions(&self) -> std::sync::Arc<Vec<DomainProgression>> {
        self.progressions.clone()
    }
    /// Host failure requests rollback of the exact already-projected state.
    pub fn rollback(&mut self, conflict: DomainConflict) {
        self.failure = Some(conflict);
        if let Some(open) = self.open.take() {
            self.garbage.push(open.set);
        }
    }
    pub fn begin_completion(&mut self, queued: Vec<Option<u64>>) -> bool {
        if !self.projected || self.failure.is_some() || queued.len() != self.updates.len() {
            return false;
        }
        self.completion = Some(queued.into());
        true
    }
    /// Returns the restored or committed state without copying any full map.
    pub fn into_result(self) -> (TaskDomains, Vec<TaskDomainReceipt>, bool) {
        (self.domains, self.receipts, self.applied)
    }
    pub fn advance(&mut self) -> TaskDomainTransactionProgress {
        // BTreeSet destruction visits every node; dispose scratch sets with
        // this same cursor instead of hiding that traversal in one poller step.
        if let Some(set) = self.garbage.last_mut() {
            if set.pop_first().is_none() {
                self.garbage.pop();
            }
            return TaskDomainTransactionProgress::Pending;
        }
        if let Some(conflict) = self.failure {
            if let Some(undo) = self.undo.pop() {
                match undo {
                    DomainUndo::Split(node, old) => match old {
                        Some(old) => {
                            self.domains.splits.nodes.insert(node, old);
                        }
                        None => {
                            self.domains.splits.nodes.remove(&node);
                        }
                    },
                    DomainUndo::Filter(old) => self.domains.dynamic_filter = old,
                    DomainUndo::Permission(edge, old) => {
                        if old == EdgeSendPermission::Open {
                            self.domains.edges.opened.insert(edge);
                        } else {
                            self.domains.edges.opened.remove(&edge);
                        }
                    }
                    DomainUndo::OpenSet(version) => {
                        if let Some(set) = self.domains.edges.opened_sets.remove(&version) {
                            self.garbage.push(set);
                        }
                    }
                    DomainUndo::Close(version, member) => {
                        self.domains.closed_destinations.accepted.remove(&version);
                        self.domains.closed_destinations.closed.remove(&member);
                    }
                }
                return TaskDomainTransactionProgress::Pending;
            }
            if self.updates.pop_front().is_some() {
                return TaskDomainTransactionProgress::Pending;
            }
            return TaskDomainTransactionProgress::RolledBack(conflict);
        }
        if self.completion.is_some() {
            return self.advance_completion();
        }
        if self.projected {
            return TaskDomainTransactionProgress::Ready;
        }
        let Some(update) = self.updates.get(self.projection_index) else {
            self.progressions =
                std::sync::Arc::new(std::mem::take(&mut self.building_progressions));
            self.projected = true;
            return TaskDomainTransactionProgress::Ready;
        };
        if let TaskDomainInput::Open { version, edges } = update {
            let open = self.open.get_or_insert_with(|| OpenProjection {
                index: 0,
                set: BTreeSet::new(),
                any_open: false,
                same_set: self
                    .domains
                    .edges
                    .opened_sets
                    .get(version)
                    .is_some_and(|set| set.len() == edges.len()),
                applying: false,
            });
            if let Some(edge) = edges.get(open.index).copied() {
                if open.applying {
                    let old = self.domains.edges.permission(edge).expect("validated edge");
                    self.domains.edges.opened.insert(edge);
                    self.undo.push(DomainUndo::Permission(edge, old));
                } else {
                    let Some(permission) = self.domains.edges.permission(edge) else {
                        self.rollback(DomainConflict::UnknownMember);
                        return TaskDomainTransactionProgress::Pending;
                    };
                    if !open.set.insert(edge) {
                        self.rollback(DomainConflict::SameTokenDifferentContent);
                        return TaskDomainTransactionProgress::Pending;
                    }
                    open.any_open |= permission == EdgeSendPermission::Open;
                    open.same_set &= self
                        .domains
                        .edges
                        .opened_sets
                        .get(version)
                        .is_some_and(|set| set.contains(&edge));
                }
                open.index += 1;
                return TaskDomainTransactionProgress::Pending;
            }
            if open.applying {
                let open = self.open.take().expect("active open projection");
                self.domains.edges.opened_sets.insert(*version, open.set);
                self.undo.push(DomainUndo::OpenSet(*version));
                self.building_progressions.push(DomainProgression::Apply);
                self.applied = true;
                self.projection_index += 1;
                return TaskDomainTransactionProgress::Pending;
            }
            let progression = if edges.is_empty() {
                DomainProgression::Conflict(DomainConflict::UnknownMember)
            } else if self.domains.edges.opened_sets.contains_key(version) {
                if open.same_set {
                    DomainProgression::Idempotent
                } else {
                    DomainProgression::Conflict(DomainConflict::SameTokenDifferentContent)
                }
            } else if self
                .domains
                .edges
                .accepted_version()
                .is_some_and(|highest| *version < highest)
            {
                DomainProgression::Older
            } else if open.any_open {
                DomainProgression::Conflict(DomainConflict::SameTokenDifferentContent)
            } else {
                DomainProgression::Apply
            };
            match progression {
                DomainProgression::Conflict(conflict) => self.rollback(conflict),
                DomainProgression::Apply => {
                    open.applying = true;
                    open.index = 0;
                }
                progression => {
                    self.garbage
                        .push(self.open.take().expect("open projection").set);
                    self.building_progressions.push(progression);
                    self.projection_index += 1;
                }
            }
            return TaskDomainTransactionProgress::Pending;
        }
        let progression = self.domains.classify(update);
        if let DomainProgression::Conflict(conflict) = progression {
            self.rollback(conflict);
            return TaskDomainTransactionProgress::Pending;
        }
        if progression == DomainProgression::Apply {
            self.applied = true;
            self.undo.push(match update {
                TaskDomainInput::Split { node, .. } => {
                    DomainUndo::Split(*node, self.domains.splits.nodes.get(node).copied())
                }
                TaskDomainInput::Filter { .. } => DomainUndo::Filter(self.domains.dynamic_filter),
                TaskDomainInput::Close {
                    version,
                    edge,
                    destination,
                } => DomainUndo::Close(*version, (*edge, *destination)),
                TaskDomainInput::Open { .. } => unreachable!("open handled with a member cursor"),
            });
            self.domains.apply(update).expect("classified update");
        }
        self.building_progressions.push(progression);
        self.projection_index += 1;
        TaskDomainTransactionProgress::Pending
    }
    fn advance_completion(&mut self) -> TaskDomainTransactionProgress {
        let Some(update) = self.updates.front() else {
            // Undo records contain only IDs/scalars, so clearing this Vec has
            // no member traversal or user destructor.
            self.undo.clear();
            return TaskDomainTransactionProgress::Complete;
        };
        let progression = self.progressions[self.receipts.len()];
        if let TaskDomainInput::Open { edges, .. } = update {
            if let Some(edge) = edges.get(self.receipt_index) {
                if self.domains.edges.permission(*edge) == Some(EdgeSendPermission::Open) {
                    self.receipt_edges.push(*edge);
                }
                self.receipt_index += 1;
                return TaskDomainTransactionProgress::Pending;
            }
            self.receipts.push(TaskDomainReceipt::OpenExchangeEdges {
                accepted_version: self
                    .domains
                    .edges
                    .accepted_version()
                    .expect("accepted open update"),
                opened: std::mem::take(&mut self.receipt_edges),
                progression,
            });
            self.receipt_index = 0;
        } else {
            let queued = self
                .completion
                .as_ref()
                .expect("completion active")
                .front()
                .copied()
                .flatten();
            self.receipts
                .push(self.domains.receipt(update, progression, queued));
        }
        self.completion
            .as_mut()
            .expect("completion active")
            .pop_front();
        self.updates.pop_front();
        TaskDomainTransactionProgress::Pending
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn scalar_replay_older_and_seal_are_independent() {
        let a = ContentFingerprint::from_bytes([1; 16]);
        let b = ContentFingerprint::from_bytes([2; 16]);
        let d = ScalarDomain::empty().apply(DomainVersion::FIRST, a);
        assert_eq!(
            d.classify(DomainVersion::FIRST, a),
            DomainProgression::Idempotent
        );
        assert_eq!(
            d.classify(DomainVersion::FIRST, b),
            DomainProgression::Conflict(DomainConflict::SameTokenDifferentContent)
        );
    }
    #[test]
    fn credential_equality_is_bound_to_the_exact_installed_resource() {
        let state = CredentialState {
            lease: CredentialLeaseId::new(3),
            epoch: CredentialEpoch::FIRST,
            resource: 7,
        };
        let input = CredentialInput {
            lease: state.lease,
            epoch: state.epoch,
            current_resource: 7,
            replacement_resource: 8,
            same_material: true,
        };
        assert_eq!(state.classify(input), DomainProgression::Idempotent);
        assert_eq!(
            state.classify(CredentialInput {
                current_resource: 9,
                ..input
            }),
            DomainProgression::Conflict(DomainConflict::UnknownMember)
        );
        assert_eq!(
            state.classify(CredentialInput {
                same_material: false,
                ..input
            }),
            DomainProgression::Conflict(DomainConflict::SameTokenDifferentContent)
        );
        assert_eq!(
            state.classify(CredentialInput {
                epoch: CredentialEpoch::new(2).unwrap(),
                same_material: false,
                ..input
            }),
            DomainProgression::Apply
        );
        assert_eq!(
            state.classify(CredentialInput {
                epoch: CredentialEpoch::new(3).unwrap(),
                ..input
            }),
            DomainProgression::Conflict(DomainConflict::Gap)
        );
    }
    #[test]
    fn split_watermarks_do_not_hash_replayed_payloads() {
        let one = SplitSequence::new(1).unwrap();
        let two = SplitSequence::new(2).unwrap();
        let d = AcceptedSplitWatermark::default();
        let offer = SplitOffer::Batch {
            first: one,
            last: two,
            no_more: false,
        };
        assert_eq!(d.classify(offer), DomainProgression::Apply);
        let d = d.apply(offer);
        assert_eq!(d.classify(offer), DomainProgression::Idempotent);
        assert_eq!(d.classify(SplitOffer::Seal), DomainProgression::Apply);
        assert_eq!(
            d.apply(SplitOffer::Seal).classify(offer),
            DomainProgression::Idempotent
        );
    }
    fn projected(transaction: &mut TaskDomainTransaction) -> TaskDomainTransactionProgress {
        for _ in 0..1000 {
            let progress = transaction.advance();
            if progress != TaskDomainTransactionProgress::Pending {
                return progress;
            }
        }
        panic!("bounded test transaction failed to advance");
    }
    #[test]
    fn incremental_domain_projection_matches_single_step_and_builds_final_receipts() {
        let node = PlanNodeId::new(1).unwrap();
        let a = ExchangeEdgeId::new(1).unwrap();
        let b = ExchangeEdgeId::new(2).unwrap();
        let original = TaskDomains::new([node], [a, b], []);
        let updates = vec![
            TaskDomainInput::Split {
                node,
                offer: SplitOffer::Batch {
                    first: SplitSequence::new(1).unwrap(),
                    last: SplitSequence::new(3).unwrap(),
                    no_more: false,
                },
            },
            TaskDomainInput::Open {
                version: EdgeOpenVersion::new(1).unwrap(),
                edges: vec![a, b],
            },
            TaskDomainInput::Split {
                node,
                offer: SplitOffer::Seal,
            },
        ];
        let mut expected = original.clone();
        let expected_progressions: Vec<_> = updates
            .iter()
            .map(|update| expected.apply(update).unwrap())
            .collect();
        let expected_receipts: Vec<_> = updates
            .iter()
            .zip(&expected_progressions)
            .map(|(update, progression)| expected.receipt(update, *progression, None))
            .collect();
        let mut transaction = TaskDomainTransaction::new(original, updates);
        assert_eq!(
            projected(&mut transaction),
            TaskDomainTransactionProgress::Ready
        );
        assert_eq!(transaction.progressions().as_ref(), &expected_progressions);
        assert!(transaction.begin_completion(vec![None; 3]));
        assert_eq!(
            projected(&mut transaction),
            TaskDomainTransactionProgress::Complete
        );
        let (actual, receipts, applied) = transaction.into_result();
        assert!(applied);
        assert_eq!(receipts, expected_receipts);
        assert_eq!(actual.splits, expected.splits);
        assert_eq!(actual.edges, expected.edges);
    }
    #[test]
    fn failed_projection_and_host_failure_restore_every_touched_domain_without_cloning_owner_maps()
    {
        let node = PlanNodeId::new(1).unwrap();
        let a = ExchangeEdgeId::new(1).unwrap();
        let b = ExchangeEdgeId::new(2).unwrap();
        let mut original = TaskDomains::new([node], [a, b], []);
        original
            .apply(&TaskDomainInput::Split {
                node,
                offer: SplitOffer::Batch {
                    first: SplitSequence::new(1).unwrap(),
                    last: SplitSequence::new(2).unwrap(),
                    no_more: false,
                },
            })
            .unwrap();
        // This is a previously committed request, outside the new journal.
        for fail_during_projection in [false, true] {
            let mut updates = vec![
                TaskDomainInput::Split {
                    node,
                    offer: SplitOffer::Seal,
                },
                TaskDomainInput::Open {
                    version: EdgeOpenVersion::new(1).unwrap(),
                    edges: vec![a, b],
                },
            ];
            if fail_during_projection {
                updates.push(TaskDomainInput::Open {
                    version: EdgeOpenVersion::new(2).unwrap(),
                    edges: vec![a],
                });
            }
            let mut transaction = TaskDomainTransaction::new(original.clone(), updates);
            let first = projected(&mut transaction);
            if !fail_during_projection {
                assert_eq!(first, TaskDomainTransactionProgress::Ready);
                transaction.rollback(DomainConflict::SameTokenDifferentContent);
                assert_eq!(
                    projected(&mut transaction),
                    TaskDomainTransactionProgress::RolledBack(
                        DomainConflict::SameTokenDifferentContent
                    )
                );
            } else {
                assert_eq!(
                    first,
                    TaskDomainTransactionProgress::RolledBack(
                        DomainConflict::SameTokenDifferentContent
                    )
                );
            }
            let (restored, receipts, _) = transaction.into_result();
            assert!(receipts.is_empty());
            assert_eq!(restored.splits, original.splits);
            assert_eq!(restored.edges, original.edges);
            assert_eq!(restored.dynamic_filter, original.dynamic_filter);
            assert_eq!(restored.closed_destinations, original.closed_destinations);
        }
    }
}
