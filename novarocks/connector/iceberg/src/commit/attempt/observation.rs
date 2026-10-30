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

//! One journal owns each request's dispatch fact, evidence and finalization.

mod dispatch;
pub(crate) use dispatch::{PendingPublicationFinalization, PreparedObservedPublisher};
#[cfg(test)]
mod lifecycle_contract_tests;

use super::{Publisher, Report};
use crate::catalog::error::CatalogOutcome;
use crate::catalog::transaction::CommitProof;
use crate::commit::model::{
    AttemptToken, CleanupRemainingReason, CleanupScope, FrozenRequest, IcebergCleanupReport,
    ObjectIdentity, OperationToken, PublicationOutcome, RemainingArtifact,
};
use crate::commit::operation::{IcebergCommitAttempt, IcebergCommitOperation};
use crate::commit::publication_descriptor::{
    AttributionPlan, PositiveAttributionObservation, PublicationDescriptor,
};
use crate::commit::staging::StagingBase;
use crate::iceberg::{Error, ErrorKind, Result};
use async_trait::async_trait;
use novarocks_spi::connector::{
    ConnectorError, ConnectorErrorKind, ConnectorMutationFailure, ConnectorMutationFailureKind,
    ConnectorOperationControl, ConnectorProcessLocalPayload, ConnectorProviderBindingKey,
    ConnectorRequestContext, ExternalMutationEffect, ExternalMutationEvidence,
    ExternalMutationFinalization, ExternalMutationOutcome, LakePublicationDisposition,
};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use uuid::Uuid;

/// Mechanisms return dispatch facts. Ordinary error kinds never decide effect.
#[derive(Clone, Debug)]
pub(crate) enum DispatchEvidence {
    NotDispatched {
        failure: ConnectorMutationFailure,
    },
    Rejected {
        failure: ConnectorMutationFailure,
        #[expect(
            dead_code,
            reason = "Retain the exact protocol observation alongside the typed rejection for dispatch diagnostics."
        )]
        observation: ProtocolObservation,
    },
    Applied {
        proof: CommitProof,
    },
    Unknown {
        failure: ConnectorMutationFailure,
        #[cfg_attr(
            not(test),
            expect(
                dead_code,
                reason = "Retain the exact protocol observation alongside an indeterminate dispatch for diagnostics."
            )
        )]
        observation: ProtocolObservation,
    },
}
#[derive(Clone, Debug, Default)]
pub(crate) struct ProtocolObservation {
    #[expect(
        dead_code,
        reason = "The adapter identity is retained as protocol diagnostic evidence."
    )]
    pub adapter: &'static str,
    #[expect(
        dead_code,
        reason = "The dispatch stage is retained as protocol diagnostic evidence."
    )]
    pub stage: &'static str,
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "The response status is retained as protocol diagnostic evidence."
        )
    )]
    pub status_code: Option<u16>,
    #[expect(
        dead_code,
        reason = "Response header names are retained as protocol diagnostic evidence."
    )]
    pub header_names: Vec<String>,
}
/// A narrow capability minted only from one exact Applied journal entry.
/// NoOp, rejection, and raw Unknown cannot authorize object collection.
pub(crate) struct AppliedDropWitness(crate::catalog::catalog_request::DropPublicationScope);
impl AppliedDropWitness {
    pub(crate) fn proves_capture(
        &self,
        target: &str,
        capture: &crate::catalog::CatalogDropTableReceipt,
    ) -> bool {
        self.0.proves_capture(target, capture)
    }
}

/// Both valid receipt projections are built before dispatch. The recorded
/// effect selects facts; receipt projection never decides publication.
#[derive(Clone)]
pub(crate) struct PublicationReceipts<R> {
    applied: R,
    no_op: R,
}
impl<R: Clone> PublicationReceipts<R> {
    pub(crate) fn same(receipt: R) -> Self {
        Self {
            applied: receipt.clone(),
            no_op: receipt,
        }
    }
    pub(crate) fn for_effects(applied: R, no_op: R) -> Self {
        Self { applied, no_op }
    }
    fn for_effect(&self, effect: novarocks_spi::connector::ExternalMutationEffect) -> R {
        match effect {
            novarocks_spi::connector::ExternalMutationEffect::Applied => self.applied.clone(),
            novarocks_spi::connector::ExternalMutationEffect::NoOp => self.no_op.clone(),
        }
    }
}

#[derive(Clone)]
struct Checked<R> {
    evidence: ExternalMutationEvidence,
    receipts: PublicationReceipts<R>,
    retained: Vec<ObjectIdentity>,
    descriptor: PublicationDescriptor,
    attempt: Option<AttemptToken>,
    preparation: Option<ConnectorRequestContext>,
}
#[derive(Clone)]
enum EntryPhase {
    Prepared,
    Claimed,
    Dispatching,
    Concluded(DispatchEvidence),
}
struct JournalEntry<R> {
    checked: Checked<R>,
    phase: EntryPhase,
    // Independent dimensions: attaching finalization or positive observation
    // never replaces the original dispatch evidence.
    finalization: ExternalMutationFinalization,
    cleanup: ExternalMutationFinalization,
    // The original entry, not the public diagnostic, owns exact remaining
    // objects/reasons until its existing operation owner is retired.
    cleanup_report: Option<IcebergCleanupReport>,
    finalization_grant:
        Option<std::result::Result<crate::catalog::publication::EvidenceContext, ConnectorError>>,
    // The original entry captures its caller's necessary-finalization cap
    // before dispatch. It never uses the ordinary business deadline as a cap.
    finalization_deadline_cap: Option<std::time::Instant>,
    positive_proof: Option<CommitProof>,
}
struct JournalState<R> {
    entries: BTreeMap<Uuid, JournalEntry<R>>,
    current: BTreeMap<u64, Uuid>,
    next_frontier: u64,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct JournalEntryRef(Uuid);

/// Exact original failure dimension, independent of the recorded commit fact.
/// Cloning preserves the original typed cause; no Checked graph is projected.
#[derive(Clone)]
pub(crate) enum CatalogPublicationFailure {
    Dispatch(ConnectorMutationFailure),
    RequiredFinalization(ConnectorMutationFailure),
}
impl CatalogPublicationFailure {
    pub(crate) fn connector_error(&self) -> ConnectorError {
        let (failure, detail) = match self {
            Self::Dispatch(failure) => (failure, "original catalog publication failed"),
            Self::RequiredFinalization(failure) => {
                (failure, "original catalog required finalization failed")
            }
        };
        if let Some(original) = failure.connector_source() {
            return original.clone();
        }
        // This is the publication boundary's descriptive kind. The original
        // producer's typed failure remains authoritative for classification.
        ConnectorError::from_domain_static(ConnectorErrorKind::Unavailable, failure.clone(), detail)
    }
}

pub(crate) enum RetainedPublicationProjection<R> {
    Committed {
        effect: ExternalMutationEffect,
        receipt: R,
        finalization: ExternalMutationFinalization,
    },
    Settled(ExternalMutationOutcome<R>),
}
pub(crate) struct PublicationJournal<R> {
    operation: OperationToken,
    frontier: u64,
    activity: Option<Arc<crate::activity::GenerationActivity>>,
    state: Arc<Mutex<JournalState<R>>>,
}
impl<R> Clone for PublicationJournal<R> {
    fn clone(&self) -> Self {
        Self {
            operation: self.operation,
            frontier: self.frontier,
            activity: self.activity.clone(),
            state: Arc::clone(&self.state),
        }
    }
}
// The journal stores only bare evidence. A projected Unknown may retain this
// original owner; the back-reference must never enter JournalState.
struct RetainedJournalEvidence<R> {
    journal: PublicationJournal<R>,
    entry: JournalEntryRef,
    evidence: ExternalMutationEvidence,
}

// A read-only bridge projection, never an independently writable state machine.
#[derive(Clone)]
enum Phase<R> {
    Undispatched,
    Ready,
    Waiting,
    Issued(Checked<R>),
    Committed(CommitProof, Checked<R>, ExternalMutationFinalization),
    Rejected(ConnectorMutationFailure, ExternalMutationFinalization),
    Unknown(Checked<R>, ConnectorMutationFailure),
}
impl<R: Clone + Send + Sync + 'static> PublicationJournal<R> {
    pub(crate) const fn retained_evidence_backing_bytes() -> usize {
        // The original journal evidence plus ProcessLocalPayload's Arc<Arc<T>>.
        std::mem::size_of::<RetainedJournalEvidence<R>>()
            + std::mem::size_of::<Arc<RetainedJournalEvidence<R>>>()
            + 2 * std::mem::size_of::<[usize; 2]>()
    }
    pub(crate) const fn fixed_backing_bytes() -> usize {
        // Exactly one original Arc-backed empty journal state. Variable
        // entries and reports retain their original formation-point receipts.
        std::mem::size_of::<Mutex<JournalState<R>>>() + std::mem::size_of::<[usize; 2]>()
    }

    #[cfg(test)]
    pub(crate) fn new(operation: OperationToken) -> Self {
        Self::new_with_activity(operation, None)
    }
    pub(crate) fn for_runtime(
        operation: OperationToken,
        runtime: &crate::resources::IcebergCatalogRuntime,
    ) -> Self {
        Self::new_with_activity(operation, runtime.activity().cloned())
    }
    fn new_with_activity(
        operation: OperationToken,
        activity: Option<Arc<crate::activity::GenerationActivity>>,
    ) -> Self {
        Self {
            operation,
            frontier: 0,
            activity,
            state: Arc::new(Mutex::new(JournalState {
                entries: BTreeMap::new(),
                current: BTreeMap::new(),
                next_frontier: 1,
            })),
        }
    }
    /// Recover only the original in-process owner, never a fabricated journal
    /// from serializable request facts. Wire-only evidence has no such owner.
    pub(crate) fn from_evidence(
        evidence: &ExternalMutationEvidence,
    ) -> std::result::Result<Option<(Self, JournalEntryRef)>, ConnectorError> {
        let Some(payload) = evidence.process_local_payload() else {
            return Ok(None);
        };
        let retained = payload
            .value::<RetainedJournalEvidence<R>>(evidence.descriptor().instance_id.as_str())?;
        if retained.evidence != *evidence {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "Publication evidence does not match its original journal owner",
            ));
        }
        let state = retained
            .journal
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let exact = state.entries.get(&retained.entry.0).is_some_and(|entry| {
            entry.checked.evidence == *evidence
                && entry.checked.descriptor.key == retained.entry.0
                && evidence.operation_id().to_bytes() == retained.journal.operation.to_bytes()
        });
        if !exact {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "Publication evidence has no exact original journal entry",
            ));
        }
        Ok(Some((retained.journal.clone(), retained.entry)))
    }

    pub(crate) fn entry_evidence(
        &self,
        entry: JournalEntryRef,
    ) -> Option<ExternalMutationEvidence> {
        let checked = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .entries
            .get(&entry.0)?
            .checked
            .clone();
        Some(self.retained_evidence(checked))
    }

    fn retained_evidence(&self, checked: Checked<R>) -> ExternalMutationEvidence {
        let evidence = checked.evidence.without_process_local_payload();
        let owner = ConnectorProviderBindingKey {
            instance_id: evidence.descriptor().instance_id.clone(),
            incarnation: evidence.incarnation(),
        };
        let retained = Arc::new(RetainedJournalEvidence {
            journal: self.clone(),
            entry: JournalEntryRef(checked.descriptor.key),
            evidence: evidence.clone(),
        });
        evidence
            .with_process_local_payload(ConnectorProcessLocalPayload::new(owner, retained))
            .expect("Bare journal evidence accepts its exact in-process owner")
    }

    /// A new publication frontier shares the operation's entries but has no
    /// current request yet. A prior stage proof cannot answer a final publish.
    pub(crate) fn next_frontier(&self) -> Result<Self> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.entries.values().any(|entry| {
            !matches!(
                entry.phase,
                EntryPhase::Concluded(
                    DispatchEvidence::Applied { .. }
                        | DispatchEvidence::Rejected { .. }
                        | DispatchEvidence::NotDispatched { .. }
                )
            )
        }) {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Publication frontier cannot bypass active or unknown evidence",
            ));
        }
        let frontier = state.next_frontier;
        state.next_frontier = frontier.checked_add(1).ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                "Publication frontier identity exhausted",
            )
        })?;
        Ok(Self {
            operation: self.operation,
            frontier,
            activity: self.activity.clone(),
            state: Arc::clone(&self.state),
        })
    }
    fn prepare(&self, mut checked: Checked<R>) -> Result<()> {
        // No strong cycle: projected owner payloads are never journal facts.
        checked.evidence = checked.evidence.without_process_local_payload();
        checked.descriptor.validate()?;
        if checked.evidence.operation_id().to_bytes() != self.operation.to_bytes() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Publication names another operation",
            ));
        }
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.entries.values().any(|entry| {
            !matches!(
                entry.phase,
                EntryPhase::Concluded(
                    DispatchEvidence::NotDispatched { .. }
                        | DispatchEvidence::Rejected { .. }
                        | DispatchEvidence::Applied { .. }
                )
            )
        }) {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Publication cannot prepare while its preceding request is active or unknown",
            ));
        }
        if checked.attempt.is_some_and(|attempt| {
            state
                .entries
                .values()
                .any(|entry| entry.checked.attempt == Some(attempt))
        }) {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Frozen attempt already has a journal entry",
            ));
        }
        let key = checked.descriptor.key;
        state.entries.insert(
            key,
            JournalEntry {
                finalization_deadline_cap: checked
                    .preparation
                    .as_ref()
                    .and_then(ConnectorRequestContext::necessary_finalization_deadline_cap),
                checked,
                phase: EntryPhase::Prepared,
                finalization: ExternalMutationFinalization::Complete,
                cleanup: ExternalMutationFinalization::Complete,
                cleanup_report: None,
                finalization_grant: None,
                positive_proof: None,
            },
        );
        state.current.insert(self.frontier, key);
        Ok(())
    }
    pub(crate) fn record(
        &self,
        evidence: ExternalMutationEvidence,
        receipt: R,
        request: &FrozenRequest,
    ) -> Result<()> {
        self.record_receipts(evidence, PublicationReceipts::same(receipt), request)
    }
    /// The exact dispatch effect, never a receipt/snapshot guess, selects one
    /// of the two receipts frozen before publication.
    pub(crate) fn record_for_effects(
        &self,
        evidence: ExternalMutationEvidence,
        applied: R,
        no_op: R,
        request: &FrozenRequest,
    ) -> Result<()> {
        self.record_receipts(
            evidence,
            PublicationReceipts::for_effects(applied, no_op),
            request,
        )
    }
    fn record_receipts(
        &self,
        evidence: ExternalMutationEvidence,
        receipts: PublicationReceipts<R>,
        request: &FrozenRequest,
    ) -> Result<()> {
        if request.artifacts().attempt().operation() != self.operation {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Frozen artifacts name another operation",
            ));
        }
        let retained = request
            .artifacts()
            .attempt_owned()
            .iter()
            .chain(request.artifacts().operation_references())
            .chain(request.artifacts().session_references())
            .cloned()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        self.prepare(Checked {
            evidence,
            receipts,
            retained,
            descriptor: PublicationDescriptor::frozen(request)?,
            attempt: Some(request.artifacts().attempt()),
            preparation: None,
        })
    }
    /// Namespace/View/DROP vocabulary does not need a table or artifact ledger.
    pub(crate) fn prepare_catalog(
        &self,
        evidence: ExternalMutationEvidence,
        receipt: R,
        targets: Vec<String>,
        request: Vec<u8>,
        attribution: AttributionPlan,
        preparation: ConnectorRequestContext,
    ) -> Result<JournalEntryRef> {
        self.prepare_catalog_with_receipts(
            evidence,
            PublicationReceipts::same(receipt),
            targets,
            request,
            attribution,
            preparation,
        )
    }
    pub(crate) fn prepare_catalog_with_receipts(
        &self,
        evidence: ExternalMutationEvidence,
        receipts: PublicationReceipts<R>,
        targets: Vec<String>,
        request: Vec<u8>,
        attribution: AttributionPlan,
        preparation: ConnectorRequestContext,
    ) -> Result<JournalEntryRef> {
        self.prepare(Checked {
            evidence,
            receipts,
            retained: Vec::new(),
            descriptor: PublicationDescriptor::catalog(
                self.operation.to_bytes(),
                targets,
                request,
                attribution,
            )?,
            attempt: None,
            preparation: Some(preparation),
        })?;
        Ok(self.current_entry().expect("prepared catalog entry exists"))
    }
    fn current_checked(&self) -> Option<Checked<R>> {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state
            .current
            .get(&self.frontier)
            .and_then(|key| state.entries.get(key))
            .map(|entry| entry.checked.clone())
    }
    pub(crate) fn bind_preparation(&self, preparation: ConnectorRequestContext) -> Result<()> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let key = state.current.get(&self.frontier).copied().ok_or_else(|| {
            Error::new(ErrorKind::DataInvalid, "Publication has no checked request")
        })?;
        let entry = state.entries.get_mut(&key).expect("current entry exists");
        if !matches!(entry.phase, EntryPhase::Prepared) {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Publication preparation is closed",
            ));
        }
        if let Some(cap) = preparation.necessary_finalization_deadline_cap() {
            entry.finalization_deadline_cap = Some(
                entry
                    .finalization_deadline_cap
                    .map_or(cap, |original| original.min(cap)),
            );
        }
        entry.checked.preparation = Some(preparation);
        Ok(())
    }
    /// Synchronize the claim before returning a future or yielding to another caller.
    pub(crate) fn claim_dispatch(&self) -> Result<DispatchClaim<R>> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let key = state.current.get(&self.frontier).copied().ok_or_else(|| {
            Error::new(ErrorKind::DataInvalid, "Publication has no checked request")
        })?;
        let entry = state.entries.get_mut(&key).expect("current entry exists");
        if !matches!(entry.phase, EntryPhase::Prepared) {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Publication entry cannot be dispatched twice",
            ));
        }
        entry.phase = EntryPhase::Claimed;
        Ok(DispatchClaim {
            state: Arc::clone(&self.state),
            key,
            completed: false,
        })
    }
    pub(crate) fn applied_drop_witness(
        &self,
        entry: JournalEntryRef,
        scope: &crate::catalog::catalog_request::DropPublicationScope,
    ) -> Option<AppliedDropWitness> {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.current.get(&self.frontier) != Some(&entry.0) {
            return None;
        }
        let entry = state.entries.get(&entry.0)?;
        match &entry.phase {
            EntryPhase::Concluded(DispatchEvidence::Applied { proof })
                if proof.effect == novarocks_spi::connector::ExternalMutationEffect::Applied =>
            {
                scope
                    .matches_targets(&entry.checked.descriptor.targets)
                    .then(|| AppliedDropWitness(scope.clone()))
            }
            _ => None,
        }
    }
    /// Copy-only observation of the original dispatch evidence. This borrows
    /// neither a receipt nor a cloned Checked graph and grants no retirement,
    /// finalization or I/O authority. None means no dispatch conclusion exists.
    pub(crate) fn catalog_dispatch_started(&self) -> bool {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state
            .current
            .get(&self.frontier)
            .and_then(|id| state.entries.get(id))
            .is_some_and(|entry| entry.checked.descriptor.first_dispatch_ms.is_some())
    }
    /// Copy-only conclusion; an in-flight actual dispatch remains None.
    pub(crate) fn catalog_dispatch_fact(&self) -> Option<(LakePublicationDisposition, bool)> {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let entry = state
            .current
            .get(&self.frontier)
            .and_then(|id| state.entries.get(id))?;
        match &entry.phase {
            EntryPhase::Concluded(DispatchEvidence::Applied { proof }) => Some((
                LakePublicationDisposition::KnownCommitted,
                proof.effect == ExternalMutationEffect::Applied,
            )),
            EntryPhase::Concluded(
                DispatchEvidence::NotDispatched { .. } | DispatchEvidence::Rejected { .. },
            ) => Some((LakePublicationDisposition::KnownUncommitted, false)),
            EntryPhase::Concluded(DispatchEvidence::Unknown { .. }) => {
                match &entry.positive_proof {
                    Some(proof) => Some((
                        LakePublicationDisposition::KnownCommitted,
                        proof.effect == ExternalMutationEffect::Applied,
                    )),
                    None => Some((LakePublicationDisposition::CommitUnknown, false)),
                }
            }
            EntryPhase::Prepared | EntryPhase::Claimed | EntryPhase::Dispatching => None,
        }
    }
    /// Copy only the exact original failure needed by a stable receiver before
    /// a later receipt/CPU action can refuse. Commit, Unknown, finalization and
    /// independent object cleanup remain separate dimensions.
    pub(crate) fn catalog_publication_failure(&self) -> Option<CatalogPublicationFailure> {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let entry = state
            .current
            .get(&self.frontier)
            .and_then(|id| state.entries.get(id))?;
        match &entry.phase {
            EntryPhase::Concluded(
                DispatchEvidence::NotDispatched { failure }
                | DispatchEvidence::Rejected { failure, .. },
            ) => Some(CatalogPublicationFailure::Dispatch(failure.clone())),
            EntryPhase::Concluded(DispatchEvidence::Unknown { failure, .. })
                if entry.positive_proof.is_none() =>
            {
                Some(CatalogPublicationFailure::Dispatch(failure.clone()))
            }
            EntryPhase::Concluded(
                DispatchEvidence::Applied { .. } | DispatchEvidence::Unknown { .. },
            ) => match &entry.finalization {
                ExternalMutationFinalization::Complete => None,
                ExternalMutationFinalization::Failed(failure) => Some(
                    CatalogPublicationFailure::RequiredFinalization(failure.clone()),
                ),
            },
            EntryPhase::Prepared | EntryPhase::Claimed | EntryPhase::Dispatching => None,
        }
    }
    pub(crate) fn current_entry(&self) -> Option<JournalEntryRef> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .current
            .get(&self.frontier)
            .copied()
            .map(JournalEntryRef)
    }
    pub(crate) fn conclude_entry(&self, entry_ref: JournalEntryRef, evidence: DispatchEvidence) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let entry = state
            .entries
            .get_mut(&entry_ref.0)
            .expect("dispatch entry remains owned");
        assert!(
            matches!(entry.phase, EntryPhase::Dispatching),
            "Publication conclusion requires its single dispatch permit"
        );
        entry.phase = EntryPhase::Concluded(evidence);
    }
    /// Observes an exact positively-applied entry. This grants no budget or I/O authority.
    pub(crate) fn finalization_activity(
        &self,
        entry_ref: JournalEntryRef,
    ) -> Option<crate::activity::ActivityGuard> {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let entry = state.entries.get(&entry_ref.0)?;
        if matches!(
            entry.phase,
            EntryPhase::Concluded(DispatchEvidence::Applied { .. })
        ) || (matches!(
            entry.phase,
            EntryPhase::Concluded(DispatchEvidence::Unknown { .. })
        ) && entry.positive_proof.is_some())
        {
            self.activity.as_ref().map(|owner| {
                owner.enter(novarocks_spi::connector::CatalogActivityPhase::Finalization)
            })
        } else {
            None
        }
    }
    /// Adapter and producer finalization share one finite phase grant. A
    /// subsequent projection cannot restart the deadline after cache work.
    pub(crate) fn finalization_context(
        &self,
        entry_ref: JournalEntryRef,
        policy: crate::catalog::publication::CatalogEvidencePolicy,
    ) -> std::result::Result<crate::catalog::publication::EvidenceContext, ConnectorError> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let entry = state.entries.get_mut(&entry_ref.0).ok_or_else(|| {
            ConnectorError::new(
                novarocks_spi::connector::ConnectorErrorKind::InvalidRequest,
                "Publication finalization requires an entry owned by this journal",
            )
        })?;
        let positively_applied = matches!(
            entry.phase,
            EntryPhase::Concluded(DispatchEvidence::Applied { .. })
        ) || (matches!(
            entry.phase,
            EntryPhase::Concluded(DispatchEvidence::Unknown { .. })
        ) && entry.positive_proof.is_some());
        if !positively_applied {
            return Err(ConnectorError::new(
                novarocks_spi::connector::ConnectorErrorKind::InvalidRequest,
                "Publication finalization requires positive applied evidence",
            ));
        }
        let cap = entry.finalization_deadline_cap;
        let grant = entry
            .finalization_grant
            .get_or_insert_with(|| {
                policy
                    .begin_finalization()
                    .map(|grant| grant.with_finalization_deadline_cap(cap))
            })
            .clone()?;
        if grant.expired() {
            return Err(ConnectorError::new(
                novarocks_spi::connector::ConnectorErrorKind::DeadlineExceeded,
                "Publication finalization deadline elapsed; no new finalization I/O may start",
            ));
        }
        Ok(grant)
    }

    pub(crate) fn record_entry_finalization(
        &self,
        entry_ref: JournalEntryRef,
        finalization: ExternalMutationFinalization,
    ) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let entry = state
            .entries
            .get_mut(&entry_ref.0)
            .expect("finalization entry remains owned");
        assert!(
            matches!(entry.phase, EntryPhase::Concluded(_)),
            "Finalization follows recorded dispatch evidence"
        );
        entry.finalization = merge_finalization(entry.finalization.clone(), finalization);
    }
    /// Pure observation of the actual final whole-operation cleanup report.
    /// That author covers all prior retry objects except the published set.
    /// An absent report, Unknown, or an unconcluded entry proves no disposal.
    #[cfg(test)]
    pub(crate) fn cleanup_is_complete(&self) -> bool {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(current) = state
            .current
            .get(&self.frontier)
            .and_then(|id| state.entries.get(id))
        else {
            return false;
        };
        matches!(
            &current.cleanup_report,
            Some(IcebergCleanupReport::Complete { .. })
        ) && matches!(&current.cleanup, ExternalMutationFinalization::Complete)
            && state.entries.values().all(|entry| {
                match &entry.phase {
                    EntryPhase::Concluded(
                        DispatchEvidence::Applied { .. }
                        | DispatchEvidence::NotDispatched { .. }
                        | DispatchEvidence::Rejected { .. },
                    ) => true,
                    // Exact positive observation may prove the effect while
                    // preserving the original Unknown dispatch evidence.
                    EntryPhase::Concluded(DispatchEvidence::Unknown { .. }) => {
                        entry.positive_proof.is_some()
                    }
                    _ => false,
                }
            })
    }

    /// Latest actual inventory disposal is independent of cumulative failure
    /// diagnostics. No snapshot state or historical finalization grants it.
    pub(crate) fn cleanup_inventory_is_disposed(&self) -> bool {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(current) = state
            .current
            .get(&self.frontier)
            .and_then(|id| state.entries.get(id))
        else {
            return false;
        };
        matches!(
            &current.cleanup_report,
            Some(IcebergCleanupReport::Complete { .. })
        ) && state.entries.values().all(|entry| match &entry.phase {
            EntryPhase::Concluded(
                DispatchEvidence::Applied { .. }
                | DispatchEvidence::NotDispatched { .. }
                | DispatchEvidence::Rejected { .. },
            ) => true,
            EntryPhase::Concluded(DispatchEvidence::Unknown { .. }) => {
                entry.positive_proof.is_some()
            }
            _ => false,
        })
    }
    /// Only this journal's exact positive proof and retained-object set can
    /// revoke deletion rights for the committed objects before cleanup.
    pub(crate) async fn settle_committed_cleanup_in(
        &self,
        operation: &IcebergCommitOperation,
        current: &ConnectorRequestContext,
    ) -> std::result::Result<(), ConnectorError> {
        if operation.token() != self.operation {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "Committed cleanup has a foreign original operation",
            ));
        }
        let Phase::Committed(_, checked, _) = self.phase() else {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "Committed cleanup requires the original positive publication proof",
            ));
        };
        let entry = self.current_entry().ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "Committed cleanup has no original journal entry",
            )
        })?;
        let report = operation
            .cleanup_after_commit_in(&checked.retained, current)
            .await;
        self.record_entry_cleanup_report(entry, report);
        Ok(())
    }

    /// Diagnostic cleanup is an independent dimension of the SAME entry.
    pub(crate) fn record_entry_cleanup(
        &self,
        entry_ref: JournalEntryRef,
        cleanup: ExternalMutationFinalization,
    ) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let entry = state
            .entries
            .get_mut(&entry_ref.0)
            .expect("cleanup entry remains owned");
        assert!(
            matches!(entry.phase, EntryPhase::Concluded(_)),
            "Cleanup follows recorded dispatch evidence"
        );
        entry.cleanup = merge_finalization(entry.cleanup.clone(), cleanup);
    }

    /// Move the complete report into the original journal before projecting it.
    pub(crate) fn record_entry_cleanup_report(
        &self,
        entry_ref: JournalEntryRef,
        report: IcebergCleanupReport,
    ) {
        let cleanup = report.finalization();
        let retired = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let entry = state
                .entries
                .get_mut(&entry_ref.0)
                .expect("cleanup entry remains owned");
            // A real report may also conclude a pre-dispatch rejection. It
            // records cleanup, never authorizes dispatch or changes its verdict.
            if matches!(report, IcebergCleanupReport::NotAttempted)
                && entry.cleanup_report.is_some()
            {
                // Absence cannot replace either an exact report or its known
                // cleanup verdict. Independently recorded failures stay intact.
                None
            } else {
                entry.cleanup = merge_finalization(entry.cleanup.clone(), cleanup);
                entry.cleanup_report.replace(report)
            }
        };
        drop(retired);
    }

    /// Absence of the actual cleanup report is not a completed cleanup.
    /// A separately recorded failure is itself an actual observed diagnostic.
    pub(crate) fn observed_entry_cleanup(
        &self,
        entry_ref: JournalEntryRef,
    ) -> Option<ExternalMutationFinalization> {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let entry = state.entries.get(&entry_ref.0)?;
        (entry.cleanup_report.is_some()
            || matches!(&entry.cleanup, ExternalMutationFinalization::Failed(_)))
        .then(|| entry.cleanup.clone())
    }

    fn entry_cleanup(&self, entry_ref: JournalEntryRef) -> ExternalMutationFinalization {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .entries
            .get(&entry_ref.0)
            .expect("cleanup entry remains owned")
            .cleanup
            .clone()
    }

    pub(crate) fn entry_descriptor(
        &self,
        entry_ref: JournalEntryRef,
    ) -> Option<PublicationDescriptor> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .entries
            .get(&entry_ref.0)
            .map(|entry| entry.checked.descriptor.clone())
    }

    /// The adapter has returned. No await, cancellation or fallible receipt I/O.
    #[cfg(test)]
    pub(crate) fn conclude_dispatch(&self, evidence: DispatchEvidence) {
        self.conclude_entry(
            self.current_entry()
                .expect("publication has a dispatch entry"),
            evidence,
        );
    }
    #[cfg(test)]
    fn conclude_outcome_entry(
        &self,
        entry_ref: JournalEntryRef,
        outcome: &CatalogOutcome<CommitProof>,
    ) {
        let evidence = match outcome {
            CatalogOutcome::KnownCommitted { receipt, .. } => DispatchEvidence::Applied {
                proof: receipt.clone(),
            },
            CatalogOutcome::KnownUncommitted { failure } => DispatchEvidence::Rejected {
                failure: failure.clone(),
                observation: ProtocolObservation::default(),
            },
            CatalogOutcome::Unsupported(error) => DispatchEvidence::NotDispatched {
                failure: ConnectorMutationFailure::new(
                    ConnectorMutationFailureKind::Unsupported,
                    error.message(),
                ),
            },
            CatalogOutcome::CommitUnknown { failure, .. } => DispatchEvidence::Unknown {
                failure: failure.clone(),
                observation: ProtocolObservation::default(),
            },
        };
        self.conclude_entry(entry_ref, evidence);
        if let CatalogOutcome::KnownCommitted { finalization, .. } = outcome {
            self.record_entry_finalization(entry_ref, finalization.clone());
        }
    }
    // Removed when all adapters use DispatchEvidence directly in T3.
    #[cfg(test)]
    pub(crate) fn conclude(&self, outcome: &CatalogOutcome<CommitProof>) {
        let evidence = match outcome {
            CatalogOutcome::KnownCommitted { receipt, .. } => DispatchEvidence::Applied {
                proof: receipt.clone(),
            },
            CatalogOutcome::KnownUncommitted { failure } => DispatchEvidence::Rejected {
                failure: failure.clone(),
                observation: ProtocolObservation::default(),
            },
            CatalogOutcome::Unsupported(error) => DispatchEvidence::NotDispatched {
                failure: ConnectorMutationFailure::new(
                    ConnectorMutationFailureKind::Unsupported,
                    error.message(),
                ),
            },
            CatalogOutcome::CommitUnknown { failure, .. } => DispatchEvidence::Unknown {
                failure: failure.clone(),
                observation: ProtocolObservation::default(),
            },
        };
        self.conclude_dispatch(evidence);
        if let CatalogOutcome::KnownCommitted { finalization, .. } = outcome {
            self.record_finalization(finalization.clone());
        }
    }
    #[cfg(test)]
    pub(crate) fn record_finalization(&self, finalization: ExternalMutationFinalization) {
        self.record_entry_finalization(
            self.current_entry()
                .expect("finalization has a publication entry"),
            finalization,
        );
    }
    /// Only the frozen request's positive probe may append proof. Unknown's
    /// original observation remains stored, including after this succeeds.
    #[cfg(test)]
    pub(crate) fn append_positive_proof(
        &self,
        key: Uuid,
        observation: PositiveAttributionObservation,
    ) -> Result<()> {
        self.append_positive_proof_with_finalization(
            key,
            observation,
            ExternalMutationFinalization::Complete,
        )
    }

    /// Publish the verified proof and its pure owner finalization fact under
    /// one lock. Concurrent reconciliation cannot observe an Applied projection
    /// with a transient default Complete between these two facts.
    pub(crate) fn append_positive_proof_with_finalization(
        &self,
        key: Uuid,
        observation: PositiveAttributionObservation,
        finalization: ExternalMutationFinalization,
    ) -> Result<()> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let entry = state.entries.get_mut(&key).ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                "Positive proof names another publication",
            )
        })?;
        if !matches!(
            entry.phase,
            EntryPhase::Concluded(DispatchEvidence::Unknown { .. })
        ) || entry.positive_proof.is_some()
        {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Positive proof requires an unresolved Unknown entry",
            ));
        }
        let descriptor = &entry.checked.descriptor;
        if !descriptor.attribution.matches_observation(&observation) {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Positive observation does not match the frozen attribution probe",
            ));
        }
        // Derive the minimal proof from verified probe facts. A caller cannot
        // pair a matching observation with a foreign location or a NoOp proof.
        let proof = match observation {
            PositiveAttributionObservation::SnapshotMarker {
                table_uuid,
                snapshot_id,
                ..
            } => CommitProof::applied(Some(snapshot_id)).with_table_uuid(table_uuid.to_string()),
            PositiveAttributionObservation::PropertyMarker { table_uuid, .. } => {
                CommitProof::applied(None).with_table_uuid(table_uuid.to_string())
            }
            PositiveAttributionObservation::ExactMetadata {
                table_uuid,
                location,
                digest,
            } => CommitProof::applied(None)
                .with_table_uuid(table_uuid.to_string())
                .with_metadata(location, digest),
            PositiveAttributionObservation::ViewOperation { view_uuid, .. } => match view_uuid {
                Some(uuid) => CommitProof::applied(None).with_table_uuid(uuid.to_string()),
                None => CommitProof::applied(None),
            },
        };
        entry.finalization = merge_finalization(entry.finalization.clone(), finalization);
        entry.positive_proof = Some(proof);
        Ok(())
    }
    pub(crate) fn descriptor(&self) -> Option<PublicationDescriptor> {
        self.current_checked().map(|checked| checked.descriptor)
    }
    fn phase(&self) -> Phase<R> {
        self.current_entry()
            .map_or(Phase::Undispatched, |key| self.entry_phase(key))
    }
    fn entry_phase(&self, entry_ref: JournalEntryRef) -> Phase<R> {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(entry) = state.entries.get(&entry_ref.0) else {
            return Phase::Undispatched;
        };
        let checked = entry.checked.clone();
        match &entry.phase {
            EntryPhase::Prepared => Phase::Ready,
            EntryPhase::Claimed => Phase::Waiting,
            EntryPhase::Dispatching => Phase::Issued(checked),
            EntryPhase::Concluded(DispatchEvidence::Applied { proof }) => {
                Phase::Committed(proof.clone(), checked, entry.finalization.clone())
            }
            EntryPhase::Concluded(
                DispatchEvidence::NotDispatched { failure }
                | DispatchEvidence::Rejected { failure, .. },
            ) => Phase::Rejected(failure.clone(), entry.finalization.clone()),
            EntryPhase::Concluded(DispatchEvidence::Unknown { failure, .. }) => match &entry
                .positive_proof
            {
                Some(proof) => Phase::Committed(proof.clone(), checked, entry.finalization.clone()),
                None => Phase::Unknown(checked, failure.clone()),
            },
        }
    }
    #[cfg(test)]
    pub(crate) fn catalog_outcome(&self) -> ExternalMutationOutcome<R> {
        let entry_ref = self
            .current_entry()
            .expect("catalog publication has a prepared entry");
        self.catalog_entry_outcome(entry_ref)
    }
    pub(crate) fn catalog_bridge_failure(&self, message: String) -> ExternalMutationOutcome<R> {
        let failure =
            ConnectorMutationFailure::new(ConnectorMutationFailureKind::Unavailable, message);
        if let Some(entry) = self.current_entry() {
            match self.entry_phase(entry) {
                Phase::Committed(_, _, _) | Phase::Rejected(_, _) => {
                    self.record_entry_finalization(
                        entry,
                        ExternalMutationFinalization::Failed(failure),
                    );
                }
                Phase::Issued(checked) => {
                    return ExternalMutationOutcome::CommitUnknown {
                        failure,
                        evidence: self.retained_evidence(checked),
                    };
                }
                Phase::Unknown(checked, original) => {
                    return ExternalMutationOutcome::CommitUnknown {
                        failure: original,
                        evidence: self.retained_evidence(checked),
                    };
                }
                _ => {}
            }
            self.catalog_entry_outcome(entry)
        } else {
            ExternalMutationOutcome::KnownUncommitted {
                failure,
                cleanup: ExternalMutationFinalization::Complete,
            }
        }
    }

    pub(crate) fn catalog_entry_outcome(
        &self,
        entry_ref: JournalEntryRef,
    ) -> ExternalMutationOutcome<R> {
        match self.entry_phase(entry_ref) {
            Phase::Committed(proof, checked, finalization) => {
                ExternalMutationOutcome::KnownCommitted {
                    cleanup: self.entry_cleanup(entry_ref),
                    effect: proof.effect,
                    receipt: checked.receipts.for_effect(proof.effect),
                    finalization,
                }
            }
            Phase::Rejected(failure, cleanup) => {
                ExternalMutationOutcome::KnownUncommitted { failure, cleanup }
            }
            Phase::Unknown(checked, failure) => ExternalMutationOutcome::CommitUnknown {
                failure,
                evidence: self.retained_evidence(checked),
            },
            Phase::Issued(checked) => ExternalMutationOutcome::CommitUnknown {
                failure: ConnectorMutationFailure::new(
                    ConnectorMutationFailureKind::Unavailable,
                    "Publication dispatch has no conclusion",
                ),
                evidence: self.retained_evidence(checked),
            },
            _ => ExternalMutationOutcome::KnownUncommitted {
                failure: ConnectorMutationFailure::new(
                    ConnectorMutationFailureKind::Unavailable,
                    "Publication has not been dispatched",
                ),
                cleanup: ExternalMutationFinalization::Complete,
            },
        }
    }
    #[cfg(test)]
    pub(crate) fn observe(&self, inner: Arc<dyn Publisher>) -> impl Publisher + use<R> {
        ObservedPublisher {
            inner,
            journal: self.clone(),
        }
    }
    /// Before safe GC there is no report to project. The actual positive
    /// journal owns needed business facts; cleanup remains a real continuation.
    pub(crate) fn project_retained_committed_frontier(
        &self,
        report: Report,
    ) -> RetainedPublicationProjection<R> {
        if matches!(&report.publication, PublicationOutcome::Committed(_))
            && matches!(&report.cleanup, IcebergCleanupReport::NotAttempted)
        {
            let Phase::Committed(proof, checked, finalization) = self.phase() else {
                panic!("committed frontier requires actual positive proof")
            };
            RetainedPublicationProjection::Committed {
                effect: proof.effect,
                receipt: checked.receipts.for_effect(proof.effect),
                finalization,
            }
        } else {
            RetainedPublicationProjection::Settled(self.project(report))
        }
    }
    pub(crate) fn project(&self, report: Report) -> ExternalMutationOutcome<R> {
        let cleanup = report.cleanup.finalization();
        let entry = self.current_entry();
        if let Some(entry) = entry {
            self.record_entry_cleanup_report(entry, report.cleanup);
        }
        match report.publication {
            PublicationOutcome::KnownUncommitted(failure) => {
                ExternalMutationOutcome::KnownUncommitted { failure, cleanup }
            }
            PublicationOutcome::Committed(_) => {
                let Phase::Committed(proof, checked, finalization) = self.phase() else {
                    panic!("Committed publication follows its checked dispatch")
                };
                ExternalMutationOutcome::KnownCommitted {
                    cleanup: self.entry_cleanup(entry.expect("committed publication has an entry")),
                    effect: proof.effect,
                    receipt: checked.receipts.for_effect(proof.effect),
                    finalization,
                }
            }
            PublicationOutcome::Unknown { failure, .. } => match self.phase() {
                Phase::Committed(proof, checked, finalization) => {
                    ExternalMutationOutcome::KnownCommitted {
                        cleanup: self
                            .entry_cleanup(entry.expect("committed publication has an entry")),
                        effect: proof.effect,
                        receipt: checked.receipts.for_effect(proof.effect),
                        finalization,
                    }
                }
                Phase::Issued(checked) | Phase::Unknown(checked, _) => {
                    ExternalMutationOutcome::CommitUnknown {
                        failure,
                        evidence: self.retained_evidence(checked),
                    }
                }
                Phase::Ready | Phase::Waiting => ExternalMutationOutcome::CommitUnknown {
                    failure,
                    evidence: self.retained_evidence(
                        self.current_checked().expect("active publication entry"),
                    ),
                },
                _ => panic!("Unknown publication follows its checked dispatch"),
            },
        }
    }
    pub(crate) async fn recover_bridge(
        &self,
        operation: &IcebergCommitOperation,
        message: String,
    ) -> ExternalMutationOutcome<R> {
        self.recover_bridge_in(operation, operation.request_context(), message)
            .await
    }
    /// The source keeps the same journal and operation across local future
    /// exit. Existing evidence selects cleanup rights; current grants no
    /// publication or object ownership of its own.
    pub(crate) async fn recover_bridge_in(
        &self,
        operation: &IcebergCommitOperation,
        current: &ConnectorRequestContext,
        message: String,
    ) -> ExternalMutationOutcome<R> {
        let failure =
            ConnectorMutationFailure::new(ConnectorMutationFailureKind::Unavailable, message);
        match self.phase() {
            Phase::Unknown(checked, original) => ExternalMutationOutcome::CommitUnknown {
                failure: original,
                evidence: self.retained_evidence(checked),
            },
            Phase::Issued(checked) => ExternalMutationOutcome::CommitUnknown {
                failure,
                evidence: self.retained_evidence(checked),
            },
            Phase::Committed(proof, checked, owner_finalization) => {
                let cleanup = operation
                    .cleanup_after_commit_in(&checked.retained, current)
                    .await;
                let cleanup = if matches!(cleanup, IcebergCleanupReport::NotAttempted) {
                    remaining_after_bridge(operation, &checked.retained, failure.message())
                } else {
                    cleanup
                };
                let entry = self
                    .current_entry()
                    .expect("committed publication has an entry");
                self.record_entry_cleanup_report(entry, cleanup);
                ExternalMutationOutcome::KnownCommitted {
                    effect: proof.effect,
                    receipt: checked.receipts.for_effect(proof.effect),
                    finalization: merge_finalization(
                        owner_finalization,
                        ExternalMutationFinalization::Failed(failure),
                    ),
                    cleanup: self.entry_cleanup(entry),
                }
            }
            Phase::Rejected(original, finalization) => {
                let cleanup = operation
                    .cleanup_in(CleanupScope::EntireOperation, current)
                    .await;
                ExternalMutationOutcome::KnownUncommitted {
                    failure: original,
                    cleanup: merge_finalization(
                        finalization,
                        unconfirmed_cleanup_finalization(operation, cleanup, failure.message()),
                    ),
                }
            }
            Phase::Undispatched | Phase::Ready | Phase::Waiting => {
                let cleanup = operation
                    .cleanup_in(CleanupScope::EntireOperation, current)
                    .await;
                ExternalMutationOutcome::KnownUncommitted {
                    failure: failure.clone(),
                    cleanup: unconfirmed_cleanup_finalization(
                        operation,
                        cleanup,
                        failure.message(),
                    ),
                }
            }
        }
    }
    pub(crate) fn bridge_failure(
        &self,
        operation: &IcebergCommitOperation,
        message: String,
    ) -> ExternalMutationOutcome<R> {
        let failure =
            ConnectorMutationFailure::new(ConnectorMutationFailureKind::Unavailable, message);
        match self.phase() {
            Phase::Unknown(checked, original) => ExternalMutationOutcome::CommitUnknown {
                failure: original,
                evidence: self.retained_evidence(checked),
            },
            Phase::Issued(checked) => ExternalMutationOutcome::CommitUnknown {
                failure,
                evidence: self.retained_evidence(checked),
            },
            Phase::Committed(proof, checked, finalization) => {
                let entry = self
                    .current_entry()
                    .expect("committed publication has an entry");
                self.record_entry_cleanup_report(
                    entry,
                    remaining_after_bridge(operation, &checked.retained, failure.message()),
                );
                ExternalMutationOutcome::KnownCommitted {
                    effect: proof.effect,
                    receipt: checked.receipts.for_effect(proof.effect),
                    finalization: merge_finalization(
                        finalization,
                        ExternalMutationFinalization::Failed(failure),
                    ),
                    cleanup: self.entry_cleanup(entry),
                }
            }
            Phase::Rejected(original, finalization) => ExternalMutationOutcome::KnownUncommitted {
                failure: original,
                cleanup: merge_finalization(
                    finalization,
                    merge_finalization(
                        ExternalMutationFinalization::Failed(failure.clone()),
                        remaining_after_bridge(operation, &[], failure.message()).finalization(),
                    ),
                ),
            },
            _ => ExternalMutationOutcome::KnownUncommitted {
                failure: failure.clone(),
                cleanup: merge_finalization(
                    ExternalMutationFinalization::Failed(failure.clone()),
                    remaining_after_bridge(operation, &[], failure.message()).finalization(),
                ),
            },
        }
    }
}

#[async_trait]
pub(crate) trait DispatchBarrier: Send + Sync {
    async fn wait(&self, preparation: &ConnectorRequestContext) -> Result<()>;
}
pub(crate) struct MemoryDispatchBarrier;
#[async_trait]
impl DispatchBarrier for MemoryDispatchBarrier {
    async fn wait(&self, preparation: &ConnectorRequestContext) -> Result<()> {
        preparation.check_active().map_err(|error| {
            Error::new(ErrorKind::Unexpected, "Publication preparation stopped").with_source(error)
        })
    }
}
pub(crate) struct DispatchClaim<R> {
    state: Arc<Mutex<JournalState<R>>>,
    key: Uuid,
    completed: bool,
}
impl<R> DispatchClaim<R> {
    pub(crate) async fn begin_dispatch(
        mut self,
        preparation: &ConnectorRequestContext,
        barrier: &dyn DispatchBarrier,
    ) -> Result<JournalEntryRef> {
        let admitted: Result<()> = async {
            preparation.check_active().map_err(|error| {
                Error::new(ErrorKind::Unexpected, "Publication preparation stopped").with_source(error)
            })?;
            tokio::select! {
                result=barrier.wait(preparation)=>result?,
                _=preparation.stop().stopped()=>return Err(Error::new(ErrorKind::Unexpected,"Publication barrier cancelled")
                    .with_source(novarocks_spi::connector::ConnectorError::new(novarocks_spi::connector::ConnectorErrorKind::Cancelled,"Publication barrier cancelled"))),
                _=tokio::time::sleep_until(preparation.deadline().into())=>return Err(Error::new(ErrorKind::Unexpected,"Publication barrier deadline elapsed")
                    .with_source(novarocks_spi::connector::ConnectorError::new(novarocks_spi::connector::ConnectorErrorKind::DeadlineExceeded,"Publication barrier deadline elapsed"))),
            }
            preparation.check_active().map_err(|error| {
                Error::new(ErrorKind::Unexpected, "Publication preparation stopped").with_source(error)
            })
        }.await;
        if let Err(error) = admitted {
            self.refuse(super::before_dispatch_failure(&error));
            return Err(error);
        }
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let entry = state
            .entries
            .get_mut(&self.key)
            .expect("claim entry remains owned");
        assert!(
            matches!(entry.phase, EntryPhase::Claimed),
            "Dispatch claim lost its entry"
        );
        if let Some(cap) = preparation.necessary_finalization_deadline_cap() {
            entry.finalization_deadline_cap = Some(
                entry
                    .finalization_deadline_cap
                    .map_or(cap, |original| original.min(cap)),
            );
        }
        entry.phase = EntryPhase::Dispatching;
        self.completed = true;
        Ok(JournalEntryRef(self.key))
    }
}
impl<R> Drop for DispatchClaim<R> {
    fn drop(&mut self) {
        if !self.completed {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(entry) = state.entries.get_mut(&self.key)
                && matches!(entry.phase, EntryPhase::Claimed)
            {
                entry.phase = EntryPhase::Concluded(DispatchEvidence::NotDispatched {
                    failure: ConnectorMutationFailure::new(
                        ConnectorMutationFailureKind::Unavailable,
                        "Publication barrier did not grant external dispatch",
                    ),
                });
            }
        }
    }
}

/// Read-only recovery facts grant no permission to remove an object. A proven
/// request's published roots are excluded from unfinished cleanup reporting.
fn remaining_after_bridge(
    operation: &IcebergCommitOperation,
    retained: &[ObjectIdentity],
    message: &str,
) -> IcebergCleanupReport {
    let retained: BTreeSet<_> = retained.iter().collect();
    let remaining = operation
        .recovery_artifacts()
        .into_iter()
        .filter(|record| !retained.contains(&record.object))
        .map(|record| RemainingArtifact {
            object: record.object,
            reason: CleanupRemainingReason::BridgeInterrupted(message.to_string()),
        })
        .collect::<Vec<_>>();
    if remaining.is_empty() {
        IcebergCleanupReport::Complete { deleted: 0 }
    } else {
        IcebergCleanupReport::Partial {
            deleted: 0,
            remaining,
        }
    }
}

fn unconfirmed_cleanup_finalization(
    operation: &IcebergCommitOperation,
    cleanup: IcebergCleanupReport,
    message: &str,
) -> ExternalMutationFinalization {
    if matches!(cleanup, IcebergCleanupReport::NotAttempted) {
        merge_finalization(
            cleanup.finalization(),
            remaining_after_bridge(operation, &[], message).finalization(),
        )
    } else {
        cleanup.finalization()
    }
}

fn merge_finalization(
    first: ExternalMutationFinalization,
    second: ExternalMutationFinalization,
) -> ExternalMutationFinalization {
    match (first, second) {
        (
            ExternalMutationFinalization::Failed(first),
            ExternalMutationFinalization::Failed(second),
        ) => ExternalMutationFinalization::Failed(ConnectorMutationFailure::new(
            first.kind(),
            format!(
                "{}; additional finalization failure: {}",
                first.message(),
                second.message()
            ),
        )),
        (failed @ ExternalMutationFinalization::Failed(_), _)
        | (_, failed @ ExternalMutationFinalization::Failed(_)) => failed,
        _ => ExternalMutationFinalization::Complete,
    }
}

#[cfg(test)]
struct ObservedPublisher<R> {
    inner: Arc<dyn Publisher>,
    journal: PublicationJournal<R>,
}
#[async_trait]
#[cfg(test)]
impl<R: Clone + Send + Sync + 'static> Publisher for ObservedPublisher<R> {
    async fn load_target_with_snapshots(
        &self,
        attempt: &IcebergCommitAttempt,
        mode: crate::catalog::SnapshotLoading,
    ) -> crate::iceberg::Result<StagingBase> {
        self.inner.load_target_with_snapshots(attempt, mode).await
    }

    fn pinned_expiry_base(&self) -> Option<StagingBase> {
        self.inner.pinned_expiry_base()
    }
    async fn load_target(&self, attempt: &IcebergCommitAttempt) -> Result<StagingBase> {
        self.inner.load_target(attempt).await
    }
    fn preflight_recovery(
        &self,
        request: &FrozenRequest,
        operation: &IcebergCommitOperation,
    ) -> Result<()> {
        self.inner.preflight_recovery(request, operation)?;
        self.journal
            .bind_preparation(operation.request_context().clone())
    }
    fn preflight_recovery_in<'a>(
        &'a self,
        request: &'a mut Option<crate::commit::model::FrozenRequest>,
        operation: &'a IcebergCommitOperation,
    ) -> crate::commit::attempt::RecoveryPreflightFuture<'a> {
        self.inner
            .preflight_recovery_in(request, operation)
            .bind_preparation(&self.journal, operation)
    }
    async fn dispatch_once(&self, request: FrozenRequest) -> CatalogOutcome<CommitProof> {
        let preparation = self
            .journal
            .current_checked()
            .and_then(|checked| checked.preparation);
        let Some(preparation) = preparation else {
            return CatalogOutcome::uncommitted(
                ConnectorMutationFailureKind::Internal,
                "Publication has no preparation context",
            );
        };
        let claim = match self.journal.claim_dispatch() {
            Ok(claim) => claim,
            Err(error) => {
                return CatalogOutcome::uncommitted(
                    ConnectorMutationFailureKind::InvalidRequest,
                    error.to_string(),
                );
            }
        };
        let entry_ref = match claim
            .begin_dispatch(&preparation, &MemoryDispatchBarrier)
            .await
        {
            Ok(entry_ref) => entry_ref,
            Err(error) => {
                return CatalogOutcome::uncommitted(
                    ConnectorMutationFailureKind::Unavailable,
                    error.to_string(),
                );
            }
        };
        self.journal.mark_dispatch_start(entry_ref);
        let outcome = self.inner.dispatch_once(request).await;
        self.journal.conclude_outcome_entry(entry_ref, &outcome);
        outcome
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::error::CatalogCommitEvidence;
    use crate::commit::attempt::{RetryPolicy, run};
    use crate::commit::fast_append::FastAppendPreparer;
    use crate::commit::model::ObjectIdentity;
    use crate::commit::overwrite::preparer_tests::Fixture;
    use crate::commit::recovery::FrozenPublicationFacts;
    use crate::commit::staging::PreparedChange;
    use crate::iceberg::spec::{FormatVersion, TableMetadata};
    use bytes::Bytes;
    use novarocks_spi::connector::{
        ConnectorInstanceDescriptor, ConnectorInstanceId, ConnectorMutationOperationId,
        ConnectorProviderId, ExternalMutationEffect, ProviderBindingEpoch,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Clone, Copy)]
    enum Behavior {
        Commit,
        CommitFinalizationFailure,
        Unknown,
        PanicBeforeLoad,
        PanicAfterIssue,
    }
    struct Fake {
        operation: IcebergCommitOperation,
        metadata: TableMetadata,
        metadata_location: String,
        journal: PublicationJournal<String>,
        dispatches: Arc<AtomicUsize>,
        behavior: Behavior,
    }
    #[async_trait]
    impl Publisher for Fake {
        async fn load_target(&self, _: &IcebergCommitAttempt) -> Result<StagingBase> {
            if matches!(self.behavior, Behavior::PanicBeforeLoad) {
                panic!("injected before-load bridge panic")
            }
            Ok(StagingBase::Existing {
                metadata: self.metadata.clone(),
                metadata_location: self.metadata_location.clone(),
            })
        }
        fn preflight_recovery(
            &self,
            request: &FrozenRequest,
            operation: &IcebergCommitOperation,
        ) -> Result<()> {
            let facts = FrozenPublicationFacts::from_request(request, operation)?;
            let encoded = serde_json::to_vec(&facts).unwrap();
            let decoded: FrozenPublicationFacts = serde_json::from_slice(&encoded).unwrap();
            assert_eq!(facts, decoded);
            decoded.validate(operation.token())?;
            let evidence = ExternalMutationEvidence::try_new(
                2,
                ConnectorInstanceDescriptor {
                    provider_id: ConnectorProviderId::parse("iceberg").unwrap(),
                    instance_id: ConnectorInstanceId::parse("observation-test").unwrap(),
                },
                ProviderBindingEpoch::from_bytes([1; 16]),
                ConnectorMutationOperationId::from_bytes(self.operation.token().to_bytes()),
                "observation-test",
                Bytes::from(encoded),
            )
            .unwrap();
            self.journal
                .record(evidence, "preflight receipt".into(), request)
        }
        async fn dispatch_once(&self, request: FrozenRequest) -> CatalogOutcome<CommitProof> {
            self.dispatches.fetch_add(1, Ordering::SeqCst);
            match self.behavior {
                Behavior::PanicAfterIssue => panic!("injected issued bridge panic"),
                Behavior::Unknown => CatalogOutcome::unknown(
                    "lost response",
                    CatalogCommitEvidence::for_target("db.t"),
                ),
                Behavior::Commit => CatalogOutcome::committed(
                    CommitProof::applied(request.ref_snapshot_after(request.target_ref())),
                    ExternalMutationEffect::Applied,
                ),
                Behavior::CommitFinalizationFailure => CatalogOutcome::KnownCommitted {
                    receipt: CommitProof::applied(request.ref_snapshot_after(request.target_ref())),
                    effect: ExternalMutationEffect::Applied,
                    finalization: ExternalMutationFinalization::Failed(
                        ConnectorMutationFailure::new(
                            ConnectorMutationFailureKind::CorruptData,
                            "injected owner finalization failure",
                        ),
                    ),
                },
                Behavior::PanicBeforeLoad => unreachable!(),
            }
        }
    }

    #[tokio::test]
    async fn prepared_journal_preserves_refs_loading_and_pinned_expiry_observation() {
        use crate::catalog::publication::{CatalogEvidencePolicy, EvidenceContext};
        use crate::catalog::publication_dispatch::{
            DispatchCompletion, PreparationResult, PreparedCatalogDispatch, PreparedPublication,
        };
        use crate::commit::attempt::prepared::{PinnedExpiryPreparer, PublicationPreparer};
        use crate::commit::model::{
            Dependency, FileChanges, IsolationLevel, OperationIntent, OperationIntentParts,
            RequestShape, TableTarget,
        };
        use crate::iceberg::{TableIdent, TableUpdate};

        struct TracedPreparedOwner {
            facts: Fake,
            modes: Arc<Mutex<Vec<crate::catalog::SnapshotLoading>>>,
        }
        struct Mutation {
            calls: Arc<AtomicUsize>,
            snapshot: Option<i64>,
        }
        #[async_trait]
        impl PreparedCatalogDispatch for Mutation {
            async fn dispatch_once(self: Box<Self>, _: EvidenceContext) -> DispatchCompletion {
                self.calls.fetch_add(1, Ordering::SeqCst);
                DispatchCompletion::evidence(DispatchEvidence::Applied {
                    proof: CommitProof::applied(self.snapshot),
                })
            }
        }
        #[async_trait]
        impl PublicationPreparer for TracedPreparedOwner {
            async fn load_target(&self, attempt: &IcebergCommitAttempt) -> Result<StagingBase> {
                self.load_target_with_snapshots(attempt, crate::catalog::SnapshotLoading::All)
                    .await
            }
            async fn load_target_with_snapshots(
                &self,
                attempt: &IcebergCommitAttempt,
                mode: crate::catalog::SnapshotLoading,
            ) -> Result<StagingBase> {
                self.modes.lock().unwrap().push(mode);
                self.facts.load_target(attempt).await
            }
            fn preflight_recovery(
                &self,
                request: &FrozenRequest,
                operation: &IcebergCommitOperation,
            ) -> Result<()> {
                self.facts.preflight_recovery(request, operation)
            }
            fn evidence_policy(&self) -> CatalogEvidencePolicy {
                CatalogEvidencePolicy::default()
            }
            async fn prepare_dispatch(
                self: Arc<Self>,
                request: FrozenRequest,
                descriptor: PublicationDescriptor,
                _: ProviderBindingEpoch,
                preparation: ConnectorRequestContext,
            ) -> PreparationResult<PreparedPublication> {
                preparation.check_active()?;
                descriptor.validate_frozen(&request)?;
                Ok(PreparedPublication::Mutation {
                    binding: None,
                    dispatch: Box::new(Mutation {
                        calls: self.facts.dispatches.clone(),
                        snapshot: request.ref_snapshot_after(request.target_ref()),
                    }),
                })
            }
        }

        for expiry in [false, true] {
            let fixture = Fixture::new();
            let metadata = fixture.metadata(FormatVersion::V3);
            let location = format!(
                "file://{}/original.metadata.json",
                fixture.directory.path().display()
            );
            let operation = fixture.operation.clone();
            let journal = PublicationJournal::new(operation.token());
            let modes = Arc::new(Mutex::new(Vec::new()));
            let dispatches = Arc::new(AtomicUsize::new(0));
            let inner: Arc<dyn PublicationPreparer> = Arc::new(TracedPreparedOwner {
                facts: Fake {
                    operation: operation.clone(),
                    metadata: metadata.clone(),
                    metadata_location: location.clone(),
                    journal: journal.clone(),
                    dispatches: dispatches.clone(),
                    behavior: Behavior::Commit,
                },
                modes: modes.clone(),
            });
            let inner = if expiry {
                Arc::new(PinnedExpiryPreparer {
                    inner,
                    base: StagingBase::Existing {
                        metadata: metadata.clone(),
                        metadata_location: location,
                    },
                }) as Arc<dyn PublicationPreparer>
            } else {
                inner
            };
            let publisher = journal.observe_prepared(inner);
            let intent = if expiry {
                OperationIntent::new(OperationIntentParts {
                    target: TableTarget {
                        ident: TableIdent::from_strs(["db", "t"]).unwrap(),
                        uuid: Some(metadata.uuid()),
                    },
                    target_ref: "main".into(),
                    start: None,
                    changes: FileChanges::default(),
                    dependencies: vec![Dependency::NoRead],
                    isolation: IsolationLevel::Snapshot,
                    shape: RequestShape::DocumentExpiry,
                    summary: Default::default(),
                    token: operation.token(),
                })
                .unwrap()
            } else {
                fixture
                    .intent(&metadata, "main", vec![])
                    .with_snapshot_loading(crate::catalog::SnapshotLoading::Refs)
            };
            let prefix = if expiry {
                PreparedChange {
                    requirements: vec![],
                    updates: vec![TableUpdate::RemoveProperties {
                        removals: vec![format!("novarocks.doc.v2.rev.{}", "0".repeat(64))],
                    }],
                }
            } else {
                PreparedChange::default()
            };
            let preparers: Vec<&dyn crate::commit::staging::Preparer> = if expiry {
                vec![]
            } else {
                vec![&FastAppendPreparer]
            };
            let report = run(
                &operation,
                &intent,
                &prefix,
                &preparers,
                &publisher,
                RetryPolicy::from_properties(metadata.properties()).unwrap(),
            )
            .await;
            assert!(matches!(
                journal.project(report),
                ExternalMutationOutcome::KnownCommitted { .. }
            ));
            assert_eq!(dispatches.load(Ordering::SeqCst), 1);
            assert_eq!(
                *modes.lock().unwrap(),
                if expiry {
                    vec![]
                } else {
                    vec![crate::catalog::SnapshotLoading::Refs]
                }
            );
        }
    }

    #[tokio::test]
    async fn retry_policy_deadline_bounds_delayed_owner_preparation_before_evidence() {
        use crate::catalog::publication::{CatalogEvidencePolicy, EvidenceContext};
        use crate::catalog::publication_dispatch::{
            DispatchCompletion, PreparationResult, PreparedCatalogDispatch, PreparedPublication,
        };
        use crate::commit::attempt::prepared::PublicationPreparer;
        use crate::iceberg::spec::TableProperties;
        use novarocks_spi::connector::{
            CatalogActivityPhase as P, CatalogActivityReader, CatalogScopeIdentity,
        };
        use std::collections::HashMap;
        use std::time::{Duration, Instant};
        use tokio::sync::Notify;

        struct DelayedOwner {
            facts: Fake,
            entered: Arc<Notify>,
            release: Arc<Notify>,
            actual_exits: Arc<AtomicUsize>,
            received_deadline: Arc<Mutex<Option<Instant>>>,
        }
        struct CountedMutation {
            calls: Arc<AtomicUsize>,
            snapshot: Option<i64>,
        }
        #[async_trait]
        impl PreparedCatalogDispatch for CountedMutation {
            async fn dispatch_once(self: Box<Self>, _: EvidenceContext) -> DispatchCompletion {
                self.calls.fetch_add(1, Ordering::SeqCst);
                DispatchCompletion::evidence(DispatchEvidence::Applied {
                    proof: CommitProof::applied(self.snapshot),
                })
            }
        }
        #[async_trait]
        impl PublicationPreparer for DelayedOwner {
            async fn load_target(&self, attempt: &IcebergCommitAttempt) -> Result<StagingBase> {
                self.facts.load_target(attempt).await
            }
            fn preflight_recovery(
                &self,
                request: &FrozenRequest,
                operation: &IcebergCommitOperation,
            ) -> Result<()> {
                self.facts.preflight_recovery(request, operation)
            }
            fn evidence_policy(&self) -> CatalogEvidencePolicy {
                CatalogEvidencePolicy::default()
            }
            async fn prepare_dispatch(
                self: Arc<Self>,
                request: FrozenRequest,
                _: PublicationDescriptor,
                _: ProviderBindingEpoch,
                preparation: ConnectorRequestContext,
            ) -> PreparationResult<PreparedPublication> {
                *self.received_deadline.lock().unwrap() = Some(preparation.deadline());
                self.entered.notify_one();
                self.release.notified().await;
                self.actual_exits.fetch_add(1, Ordering::SeqCst);
                // The real owner checks the context it received after its
                // already-started preparation has actually exited.
                preparation.check_active()?;
                Ok(PreparedPublication::Mutation {
                    binding: None,
                    dispatch: Box::new(CountedMutation {
                        calls: self.facts.dispatches.clone(),
                        snapshot: request.ref_snapshot_after(request.target_ref()),
                    }),
                })
            }
        }

        let root = crate::activity::CatalogActivity::new(CatalogScopeIdentity::new(
            ConnectorProviderId::parse("iceberg").unwrap(),
            ConnectorInstanceId::parse("preparation.policy").unwrap(),
            Uuid::now_v7(),
        ));
        let fixture = Fixture::new_observed(root.generation());
        let operation = fixture.operation.clone();
        let original = operation.request_context().clone();
        let metadata = fixture.metadata(FormatVersion::V3);
        let intent = fixture.intent(&metadata, "main", vec![]);
        let journal = PublicationJournal::for_runtime(operation.token(), operation.runtime());
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let actual_exits = Arc::new(AtomicUsize::new(0));
        let received_deadline = Arc::new(Mutex::new(None));
        let dispatches = Arc::new(AtomicUsize::new(0));
        let publisher = journal.observe_prepared(Arc::new(DelayedOwner {
            facts: Fake {
                operation: operation.clone(),
                metadata,
                metadata_location: format!(
                    "file://{}/base.metadata.json",
                    fixture.directory.path().display()
                ),
                journal: journal.clone(),
                dispatches: dispatches.clone(),
                behavior: Behavior::Commit,
            },
            entered: entered.clone(),
            release: release.clone(),
            actual_exits: actual_exits.clone(),
            received_deadline: received_deadline.clone(),
        }));
        let policy = RetryPolicy::from_properties(&HashMap::from([(
            TableProperties::PROPERTY_COMMIT_TOTAL_RETRY_TIME_MS.to_owned(),
            "2000".to_owned(),
        )]))
        .unwrap();
        let running = operation.clone();
        let task = tokio::spawn(async move {
            run(
                &running,
                &intent,
                &PreparedChange::default(),
                &[&FastAppendPreparer],
                &publisher,
                policy,
            )
            .await
        });
        tokio::time::timeout(Duration::from_secs(5), entered.notified())
            .await
            .expect("owner preparation must start inside the retry window");
        tokio::time::sleep(policy.total_timeout() + Duration::from_millis(20)).await;
        let waiting = root.snapshot();
        assert!(
            !task.is_finished(),
            "policy expiry must not detach the owner"
        );
        assert_eq!(actual_exits.load(Ordering::SeqCst), 0);
        assert_eq!(waiting.phases[P::PreDispatch as usize].inflight, 1);
        assert_eq!(waiting.phases[P::Evidence as usize].started, 0);
        assert!(
            original.check_active().is_ok(),
            "the original request is still active"
        );
        release.notify_one();
        let report = task.await.unwrap();
        let outcome = journal.project(report);
        assert!(matches!(
            outcome,
            ExternalMutationOutcome::KnownUncommitted { failure, .. }
                if failure.kind() == ConnectorMutationFailureKind::DeadlineExceeded
        ));
        assert!(received_deadline.lock().unwrap().unwrap() < original.deadline());
        assert_eq!(actual_exits.load(Ordering::SeqCst), 1);
        assert_eq!(dispatches.load(Ordering::SeqCst), 0);
        let exited = root.snapshot();
        assert_eq!(exited.phases[P::PreDispatch as usize].inflight, 0);
        assert_eq!(exited.phases[P::PreDispatch as usize].started, 2);
        assert_eq!(exited.phases[P::PreDispatch as usize].exited, 2);
        assert_eq!(exited.events.pre_dispatch_timed_out, 1);
        assert_eq!(exited.phases[P::Dispatch as usize].started, 0);
        assert_eq!(exited.phases[P::Evidence as usize].started, 0);
    }

    #[tokio::test]
    async fn committed_bounded_cleanup_retains_exact_residue_without_failing_business_finalize() {
        use crate::commit::model::{ArtifactClass, ArtifactKind, ArtifactWriter};
        use crate::commit::operation::{CleanupBudget, OperationLimits};
        use std::time::Duration;

        for behavior in [Behavior::Commit, Behavior::CommitFinalizationFailure] {
            let fixture = Fixture::new_with_limits(OperationLimits {
                cleanup: CleanupBudget {
                    time: Duration::from_secs(30),
                    objects: 1,
                },
                ..OperationLimits::default()
            });
            let old = fixture.operation.begin_attempt().unwrap();
            for _ in 0..3 {
                let object = old
                    .allocate(ArtifactClass::Attempt, ArtifactKind::Manifest)
                    .unwrap();
                old.file_io()
                    .new_output(object.path())
                    .unwrap()
                    .write(Bytes::from_static(b"old attempt artifact"))
                    .await
                    .unwrap();
            }
            let metadata = fixture.metadata(FormatVersion::V3);
            let intent = fixture.intent(&metadata, "main", vec![]);
            let journal = PublicationJournal::new(fixture.operation.token());
            let dispatches = Arc::new(AtomicUsize::new(0));
            let publisher = journal.observe(Arc::new(Fake {
                operation: fixture.operation.clone(),
                metadata: metadata.clone(),
                metadata_location: format!(
                    "file://{}/base.metadata.json",
                    fixture.directory.path().display()
                ),
                journal: journal.clone(),
                dispatches: dispatches.clone(),
                behavior,
            }));
            let report = run(
                &fixture.operation,
                &intent,
                &PreparedChange::default(),
                &[&FastAppendPreparer],
                &publisher,
                RetryPolicy::from_properties(metadata.properties()).unwrap(),
            )
            .await;
            let expected = report.cleanup.clone();
            let IcebergCleanupReport::Partial { remaining, .. } = &expected else {
                panic!("one cleanup object cannot remove all three abandoned artifacts")
            };
            assert!(!remaining.is_empty());
            // Physical files are independent evidence of unresolved cleanup.
            for item in remaining {
                assert!(
                    std::path::Path::new(item.object.path().trim_start_matches("file://")).exists()
                );
            }
            let ExternalMutationOutcome::KnownCommitted {
                finalization,
                cleanup,
                receipt,
                ..
            } = journal.project(report)
            else {
                panic!("actual publication proof must survive partial cleanup")
            };
            assert_eq!(receipt, "preflight receipt");
            assert!(matches!(cleanup, ExternalMutationFinalization::Failed(_)));
            match behavior {
                Behavior::Commit => {
                    assert_eq!(finalization, ExternalMutationFinalization::Complete)
                }
                Behavior::CommitFinalizationFailure => assert!(
                    matches!(finalization, ExternalMutationFinalization::Failed(ref e) if e.message() == "injected owner finalization failure")
                ),
                _ => unreachable!(),
            }
            let entry = journal.current_entry().unwrap();
            assert_eq!(
                journal.state.lock().unwrap().entries[&entry.0]
                    .cleanup_report
                    .as_ref(),
                Some(&expected)
            );
            for item in remaining {
                assert!(
                    fixture
                        .operation
                        .recovery_artifacts()
                        .iter()
                        .any(|record| record.object == item.object)
                );
            }
            assert_eq!(dispatches.load(Ordering::SeqCst), 1);
            assert!(
                journal.claim_dispatch().is_err(),
                "cleanup failure cannot authorize another publication"
            );
        }
    }

    #[test]
    fn bridge_frontier_preserves_unknown_and_prebuilt_committed_receipts() {
        for behavior in [
            Behavior::Unknown,
            Behavior::PanicAfterIssue,
            Behavior::Commit,
            Behavior::CommitFinalizationFailure,
        ] {
            let executor = tokio::runtime::Runtime::new().unwrap();
            let fixture = executor.block_on(async { Fixture::new() });
            let metadata = fixture.metadata(FormatVersion::V3);
            let intent = fixture.intent(&metadata, "main", vec![]);
            let operation = fixture.operation.clone();
            let provenance = fixture.directory.path().join("publication-provenance.json");
            std::fs::write(&provenance, b"provider-owned").unwrap();
            operation
                .adopt_operation_reference(
                    ObjectIdentity::new(format!("file://{}", provenance.display())).unwrap(),
                )
                .unwrap();
            let journal = PublicationJournal::new(operation.token());
            let dispatches = Arc::new(AtomicUsize::new(0));
            let publisher = journal.observe(Arc::new(Fake {
                operation: operation.clone(),
                metadata: metadata.clone(),
                metadata_location: format!(
                    "file://{}/base.metadata.json",
                    fixture.directory.path().display()
                ),
                journal: journal.clone(),
                dispatches: Arc::clone(&dispatches),
                behavior,
            }));
            let run_operation = operation.clone();
            let result = operation.runtime().block_on(async move {
                run(
                    &run_operation,
                    &intent,
                    &PreparedChange::default(),
                    &[&FastAppendPreparer],
                    &publisher,
                    RetryPolicy::from_properties(metadata.properties()).unwrap(),
                )
                .await
            });
            let outcome = match result {
                Ok(report) => journal.project(report),
                Err(error) => executor.block_on(journal.recover_bridge(&operation, error)),
            };
            assert_eq!(dispatches.load(Ordering::SeqCst), 1);
            assert!(!operation.artifacts().unwrap().is_empty());
            match behavior {
                Behavior::Unknown | Behavior::PanicAfterIssue => {
                    let ExternalMutationOutcome::CommitUnknown { evidence, .. } = outcome else {
                        panic!("issued frontier lost")
                    };
                    let checked = journal.current_checked().expect("checked request retained");
                    assert_eq!(evidence, checked.evidence);
                    let facts: FrozenPublicationFacts =
                        serde_json::from_slice(evidence.provider_payload()).unwrap();
                    facts.validate(operation.token()).unwrap();
                }
                Behavior::Commit | Behavior::CommitFinalizationFailure => {
                    let ExternalMutationOutcome::KnownCommitted {
                        receipt,
                        finalization,
                        ..
                    } = outcome
                    else {
                        panic!("commit proof lost")
                    };
                    assert_eq!(receipt, "preflight receipt");
                    assert!(provenance.exists());
                    assert!(operation.artifacts().unwrap().iter().any(
                        |record| record.class == crate::commit::model::ArtifactClass::Operation
                    ));
                    if matches!(behavior, Behavior::CommitFinalizationFailure) {
                        let ExternalMutationFinalization::Failed(failure) = finalization else {
                            panic!("owner failure lost")
                        };
                        assert_eq!(failure.kind(), ConnectorMutationFailureKind::CorruptData);
                        assert_eq!(failure.message(), "injected owner finalization failure");
                    }
                    let before = operation.artifacts().unwrap();
                    let recovered = executor.block_on(journal.recover_bridge(
                        &operation,
                        "post-proof finalization bridge failed".into(),
                    ));
                    assert!(matches!(
                        recovered,
                        ExternalMutationOutcome::KnownCommitted {
                            finalization: ExternalMutationFinalization::Failed(_),
                            ..
                        }
                    ));
                    assert_eq!(operation.artifacts().unwrap(), before);
                }
                Behavior::PanicBeforeLoad => unreachable!(),
            }
        }
    }
    #[test]
    fn bridge_before_dispatch_cleans_owned_objects_and_keeps_definite_verdict() {
        let executor = tokio::runtime::Runtime::new().unwrap();
        let fixture = executor.block_on(async { Fixture::new() });
        let metadata = fixture.metadata(FormatVersion::V3);
        let intent = fixture.intent(&metadata, "main", vec![]);
        let path = fixture.directory.path().join("owned.parquet");
        std::fs::write(&path, b"owned").unwrap();
        fixture
            .operation
            .adopt_operation_reference(
                ObjectIdentity::new(format!("file://{}", path.display())).unwrap(),
            )
            .unwrap();
        let operation = fixture.operation.clone();
        let journal = PublicationJournal::new(operation.token());
        let dispatches = Arc::new(AtomicUsize::new(0));
        let publisher = journal.observe(Arc::new(Fake {
            operation: operation.clone(),
            metadata: metadata.clone(),
            metadata_location: "file:///base.metadata.json".into(),
            journal: journal.clone(),
            dispatches: Arc::clone(&dispatches),
            behavior: Behavior::PanicBeforeLoad,
        }));
        let run_operation = operation.clone();
        let error = operation
            .runtime()
            .block_on(async move {
                run(
                    &run_operation,
                    &intent,
                    &PreparedChange::default(),
                    &[&FastAppendPreparer],
                    &publisher,
                    RetryPolicy::from_properties(metadata.properties()).unwrap(),
                )
                .await
            })
            .unwrap_err();
        let outcome = executor.block_on(journal.recover_bridge(&operation, error));
        assert!(matches!(
            outcome,
            ExternalMutationOutcome::KnownUncommitted {
                cleanup: ExternalMutationFinalization::Complete,
                ..
            }
        ));
        assert_eq!(dispatches.load(Ordering::SeqCst), 0);
        assert!(!path.exists());
        assert!(operation.artifacts().unwrap().is_empty());
    }

    #[test]
    fn final_publication_bridge_cannot_reuse_a_successful_stages_entry() {
        let executor = tokio::runtime::Runtime::new().unwrap();
        let fixture = executor.block_on(async { Fixture::new() });
        let metadata = fixture.metadata(FormatVersion::V3);
        let intent = fixture.intent(&metadata, "main", vec![]);
        let operation = fixture.operation.clone();
        let path = fixture.directory.path().join("stage-provenance.json");
        std::fs::write(&path, b"owned stage provenance").unwrap();
        operation
            .adopt_operation_reference(
                ObjectIdentity::new(format!("file://{}", path.display())).unwrap(),
            )
            .unwrap();
        let (stage, preparation) =
            catalog_journal_for_operation(operation.token(), AttributionPlan::ObserveOnly);
        let stage_entry = stage.current_entry().unwrap();
        let claim = stage.claim_dispatch().unwrap();
        executor
            .block_on(claim.begin_dispatch(&preparation, &MemoryDispatchBarrier))
            .unwrap();
        stage.conclude_entry(
            stage_entry,
            DispatchEvidence::Applied {
                proof: CommitProof::applied(None),
            },
        );
        let journal = stage.next_frontier().unwrap();
        assert!(journal.current_entry().is_none());
        let dispatches = Arc::new(AtomicUsize::new(0));
        let publisher = journal.observe(Arc::new(Fake {
            operation: operation.clone(),
            metadata: metadata.clone(),
            metadata_location: "file:///base.metadata.json".into(),
            journal: journal.clone(),
            dispatches: dispatches.clone(),
            behavior: Behavior::PanicBeforeLoad,
        }));
        let run_operation = operation.clone();
        let error = operation
            .runtime()
            .block_on(async move {
                run(
                    &run_operation,
                    &intent,
                    &PreparedChange::default(),
                    &[&FastAppendPreparer],
                    &publisher,
                    RetryPolicy::from_properties(metadata.properties()).unwrap(),
                )
                .await
            })
            .unwrap_err();
        let outcome = executor.block_on(journal.recover_bridge(&operation, error));
        assert!(matches!(
            outcome,
            ExternalMutationOutcome::KnownUncommitted {
                cleanup: ExternalMutationFinalization::Complete,
                ..
            }
        ));
        assert_eq!(dispatches.load(Ordering::SeqCst), 0);
        assert!(
            !path.exists(),
            "undispatched final publication cleans its actual owned artifacts"
        );
        assert!(operation.artifacts().unwrap().is_empty());
        assert!(matches!(
            stage.commit_outcome(stage_entry),
            CatalogOutcome::KnownCommitted { .. }
        ));
        assert_eq!(stage.current_entry(), Some(stage_entry));
        assert!(journal.current_entry().is_none());
        assert!(
            Arc::ptr_eq(&stage.state, &journal.state),
            "frontiers retain one journal fact source"
        );
    }

    #[tokio::test]
    async fn postproof_recovery_reports_residue_and_keeps_every_frozen_publication_root() {
        use crate::commit::model::{AddedContent, ArtifactClass, ArtifactKind, ArtifactWriter};
        use crate::commit::operation::{CleanupBudget, OperationLimits};
        use crate::commit::overwrite::preparer_tests::data;
        use crate::commit::staging::StagingEngine;
        use crate::iceberg::spec::Struct;
        use novarocks_spi::connector::{ConnectorRequestContext, ConnectorStopOwner};
        use std::time::{Duration, Instant};

        for behavior in [
            Behavior::Commit,
            Behavior::CommitFinalizationFailure,
            Behavior::Unknown,
        ] {
            let fixture = Fixture::new();
            let metadata = fixture.metadata(FormatVersion::V3);
            let runtime = tokio::runtime::Handle::current();
            let binding = crate::access_binding::IcebergReadBinding::new(
                None,
                novarocks_fs::FsAccessResolver::new(),
                Arc::new(novarocks_fs::TokioFileIoRuntime::new(runtime.clone())),
                Arc::new(novarocks_fs::TokioFileTaskSpawner::new(runtime.clone())),
            );
            let stop = ConnectorStopOwner::new();
            let operation = IcebergCommitOperation::new(
                fixture.operation.token(),
                metadata.location(),
                binding,
                ConnectorRequestContext::try_new(
                    Instant::now() + Duration::from_secs(60),
                    stop.view(),
                    64 * 1024,
                    1024 * 1024,
                )
                .unwrap(),
                crate::resources::IcebergCatalogRuntime::new(runtime),
                OperationLimits {
                    cleanup: CleanupBudget {
                        time: Duration::from_secs(30),
                        objects: 1,
                    },
                    ..OperationLimits::default()
                },
            )
            .unwrap();
            let old = operation.begin_attempt().unwrap();
            let mut abandoned = Vec::new();
            for class in [
                ArtifactClass::Operation,
                ArtifactClass::Operation,
                ArtifactClass::Attempt,
                ArtifactClass::Attempt,
            ] {
                let object = old.allocate(class, ArtifactKind::Manifest).unwrap();
                old.file_io()
                    .new_output(object.path())
                    .unwrap()
                    .write(Bytes::from_static(b"abandoned"))
                    .await
                    .unwrap();
                abandoned.push(object);
            }
            let operation_root = ObjectIdentity::new(format!(
                "{}/publication-provenance.json",
                metadata.location()
            ))
            .unwrap();
            let session_root =
                ObjectIdentity::new(format!("{}/session.parquet", metadata.location())).unwrap();
            std::fs::write(
                operation_root.path().trim_start_matches("file://"),
                b"provenance",
            )
            .unwrap();
            std::fs::write(
                session_root.path().trim_start_matches("file://"),
                b"session rows",
            )
            .unwrap();
            operation
                .adopt_operation_reference(operation_root.clone())
                .unwrap();
            operation.adopt_session_data(session_root.clone()).unwrap();
            let intent = fixture.intent(
                &metadata,
                "main",
                vec![
                    AddedContent::new_logical_data(
                        data(session_root.path(), 3, Struct::empty()),
                        0,
                    )
                    .unwrap(),
                ],
            );
            let attempt = operation.begin_attempt().unwrap();
            let mut engine = StagingEngine::begin(
                StagingBase::Existing {
                    metadata: metadata.clone(),
                    metadata_location: "file:///base.metadata.json".into(),
                },
                &intent,
                &attempt,
            )
            .unwrap();
            engine.stage(&FastAppendPreparer).await.unwrap();
            let request = engine
                .freeze(&[operation_root.clone(), session_root.clone()])
                .unwrap();
            let mut retained = request.artifacts().attempt_owned().to_vec();
            retained.extend_from_slice(request.artifacts().operation_references());
            retained.extend_from_slice(request.artifacts().session_references());
            let journal = PublicationJournal::new(operation.token());
            let dispatches = Arc::new(AtomicUsize::new(0));
            let publisher = journal.observe(Arc::new(Fake {
                operation: operation.clone(),
                metadata,
                metadata_location: "file:///base.metadata.json".into(),
                journal: journal.clone(),
                dispatches: dispatches.clone(),
                behavior,
            }));
            publisher.preflight_recovery(&request, &operation).unwrap();
            publisher.dispatch_once(request).await;
            let before = operation.recovery_artifacts();
            let first = journal
                .recover_bridge(&operation, "first post-dispatch bridge failure".into())
                .await;
            let second =
                journal.bridge_failure(&operation, "second recovery bridge failure".into());
            if matches!(behavior, Behavior::Unknown) {
                let (
                    ExternalMutationOutcome::CommitUnknown {
                        evidence: first, ..
                    },
                    ExternalMutationOutcome::CommitUnknown {
                        evidence: second, ..
                    },
                ) = (first, second)
                else {
                    panic!("unknown bridge lost its issued carrier")
                };
                assert_eq!(first, second);
                let checked = journal.current_checked().expect("checked request retained");
                assert_eq!(first, checked.evidence);
                assert_eq!(operation.recovery_artifacts(), before);
            } else {
                let ExternalMutationOutcome::KnownCommitted {
                    finalization: ExternalMutationFinalization::Failed(first),
                    cleanup: ExternalMutationFinalization::Failed(first_cleanup),
                    ..
                } = first
                else {
                    panic!("first post-proof failure lost its committed verdict")
                };
                let ExternalMutationOutcome::KnownCommitted {
                    finalization: ExternalMutationFinalization::Failed(second),
                    cleanup: ExternalMutationFinalization::Failed(second_cleanup),
                    ..
                } = second
                else {
                    panic!("second post-proof failure lost its committed verdict")
                };
                let residue = operation
                    .recovery_artifacts()
                    .into_iter()
                    .filter(|record| !retained.contains(&record.object))
                    .collect::<Vec<_>>();
                assert_eq!(
                    residue.len(),
                    3,
                    "bounded cleanup deletes only one abandoned object"
                );
                assert!(
                    residue
                        .iter()
                        .any(|record| record.class == ArtifactClass::Operation)
                );
                assert!(
                    residue
                        .iter()
                        .any(|record| record.class == ArtifactClass::Attempt)
                );
                for record in &residue {
                    assert!(abandoned.contains(&record.object));
                    assert!(first_cleanup.message().contains(record.object.path()));
                    assert!(second_cleanup.message().contains(record.object.path()));
                }
                assert!(
                    first
                        .message()
                        .contains("first post-dispatch bridge failure")
                );
                assert!(second.message().contains("second recovery bridge failure"));
                assert!(first_cleanup.message().contains("BudgetExhausted"));
                assert!(second_cleanup.message().contains("BridgeInterrupted"));
                assert!(!second_cleanup.message().contains("DeleteFailed"));
                for object in &retained {
                    assert!(!first_cleanup.message().contains(object.path()));
                    assert!(!second_cleanup.message().contains(object.path()));
                }
                if matches!(behavior, Behavior::CommitFinalizationFailure) {
                    assert!(
                        first
                            .message()
                            .contains("injected owner finalization failure")
                    );
                    assert!(
                        second
                            .message()
                            .contains("injected owner finalization failure")
                    );
                }
            }
            for object in &retained {
                assert!(std::path::Path::new(object.path().trim_start_matches("file://")).exists());
            }
            assert_eq!(dispatches.load(Ordering::SeqCst), 1);
        }
    }

    #[tokio::test]
    async fn undispatched_second_bridge_failure_reports_actual_owned_objects_without_io() {
        let fixture = Fixture::new();
        let path = fixture.directory.path().join("owned.parquet");
        std::fs::write(&path, b"owned").unwrap();
        let object = ObjectIdentity::new(format!("file://{}", path.display())).unwrap();
        fixture
            .operation
            .adopt_operation_reference(object.clone())
            .unwrap();
        let journal = PublicationJournal::<String>::new(fixture.operation.token());
        let before = fixture.operation.recovery_artifacts();
        let outcome = journal.bridge_failure(
            &fixture.operation,
            "second bridge failed before dispatch".into(),
        );
        let ExternalMutationOutcome::KnownUncommitted {
            cleanup: ExternalMutationFinalization::Failed(failure),
            ..
        } = outcome
        else {
            panic!("unattempted cleanup was claimed complete")
        };
        assert!(failure.message().contains(object.path()));
        assert!(failure.message().contains("BridgeInterrupted"));
        assert!(!failure.message().contains("DeleteFailed"));
        assert!(path.exists());
        assert_eq!(fixture.operation.recovery_artifacts(), before);
    }
    pub(super) fn catalog_journal() -> (PublicationJournal<String>, ConnectorRequestContext) {
        catalog_journal_with_attribution(AttributionPlan::ObserveOnly)
    }
    pub(super) fn catalog_journal_with_attribution(
        attribution: AttributionPlan,
    ) -> (PublicationJournal<String>, ConnectorRequestContext) {
        catalog_journal_for_operation(
            OperationToken::from_mutation(ConnectorMutationOperationId::from_bytes([3; 16])),
            attribution,
        )
    }
    fn catalog_journal_for_operation(
        operation: OperationToken,
        attribution: AttributionPlan,
    ) -> (PublicationJournal<String>, ConnectorRequestContext) {
        use novarocks_spi::connector::ConnectorStopOwner;
        let journal = PublicationJournal::new(operation);
        let evidence = ExternalMutationEvidence::try_new(
            2,
            ConnectorInstanceDescriptor {
                provider_id: ConnectorProviderId::parse("iceberg").unwrap(),
                instance_id: ConnectorInstanceId::parse("catalog-journal").unwrap(),
            },
            ProviderBindingEpoch::from_bytes([2; 16]),
            ConnectorMutationOperationId::from_bytes(operation.to_bytes()),
            "create_namespace",
            Bytes::from_static(b"{}"),
        )
        .unwrap();
        let preparation = ConnectorRequestContext::try_new(
            std::time::Instant::now() + std::time::Duration::from_secs(30),
            ConnectorStopOwner::new().view(),
            64,
            128,
        )
        .unwrap();
        journal
            .prepare_catalog(
                evidence,
                "namespace receipt".into(),
                vec!["namespace:db".into()],
                b"create db".to_vec(),
                attribution,
                preparation.clone(),
            )
            .unwrap();
        (journal, preparation)
    }

    struct FailingBarrier;
    #[async_trait]
    impl DispatchBarrier for FailingBarrier {
        async fn wait(&self, _: &ConnectorRequestContext) -> Result<()> {
            Err(Error::new(
                ErrorKind::Unexpected,
                "injected persistence barrier failure",
            ))
        }
    }
    #[tokio::test]
    async fn synchronous_claim_excludes_duplicates_and_failed_barrier_never_issues() {
        let (journal, preparation) = catalog_journal();
        let key = journal.descriptor().unwrap().key;
        let claim = journal.claim_dispatch().unwrap();
        assert!(journal.claim_dispatch().is_err());
        assert!(
            claim
                .begin_dispatch(&preparation, &FailingBarrier)
                .await
                .is_err()
        );
        assert!(matches!(
            journal.catalog_outcome(),
            ExternalMutationOutcome::KnownUncommitted { .. }
        ));
        let descriptor = journal.descriptor().unwrap();
        assert_eq!(descriptor.key, key);
        assert!(descriptor.first_dispatch_ms.is_none());
    }
    #[tokio::test]
    async fn actual_dispatch_start_is_distinct_from_claim_and_pending_finalization() {
        use crate::catalog::publication::{CatalogEvidencePolicy, EvidenceContext};
        use crate::catalog::publication_dispatch::{
            DispatchCompletion, FinalizationWork, PreparedCatalogDispatch, PreparedPublication,
        };
        use std::task::{Context, Poll, Waker};
        struct Mutation {
            journal: PublicationJournal<String>,
            issued: Arc<AtomicUsize>,
            dispatch: tokio::sync::oneshot::Receiver<()>,
            finalize: tokio::sync::oneshot::Receiver<()>,
        }
        struct Finalize(tokio::sync::oneshot::Receiver<()>);
        #[async_trait]
        impl FinalizationWork for Finalize {
            async fn run(self: Box<Self>, _: EvidenceContext) -> ExternalMutationFinalization {
                self.0.await.unwrap();
                ExternalMutationFinalization::Complete
            }
        }
        #[async_trait]
        impl PreparedCatalogDispatch for Mutation {
            async fn dispatch_once(self: Box<Self>, _: EvidenceContext) -> DispatchCompletion {
                assert!(
                    self.journal.catalog_dispatch_started(),
                    "actual dispatch author marked first"
                );
                self.issued.fetch_add(1, Ordering::SeqCst);
                self.dispatch.await.unwrap();
                DispatchCompletion {
                    evidence: DispatchEvidence::Applied {
                        proof: CommitProof::applied(None),
                    },
                    finalization: Some(Box::new(Finalize(self.finalize))),
                }
            }
        }
        let (journal, preparation) = catalog_journal();
        assert!(!journal.catalog_dispatch_started());
        // The old claim/Dispatching state is earlier than physical publication.
        let (other, context) = catalog_journal();
        let _entry = other
            .claim_dispatch()
            .unwrap()
            .begin_dispatch(&context, &MemoryDispatchBarrier)
            .await
            .unwrap();
        assert!(!other.catalog_dispatch_started());
        let (dispatch_tx, dispatch_rx) = tokio::sync::oneshot::channel();
        let (finalize_tx, finalize_rx) = tokio::sync::oneshot::channel();
        let issued = Arc::new(AtomicUsize::new(0));
        let actual = Mutation {
            journal: journal.clone(),
            issued: issued.clone(),
            dispatch: dispatch_rx,
            finalize: finalize_rx,
        };
        let policy = CatalogEvidencePolicy::default();
        let mut original = Box::pin(journal.publish_once_to_finalization(
            preparation,
            policy,
            |_, _, _| async move {
                Ok(PreparedPublication::Mutation {
                    binding: None,
                    dispatch: Box::new(actual),
                })
            },
        ));
        let mut cx = Context::from_waker(Waker::noop());
        assert!(std::future::Future::poll(original.as_mut(), &mut cx).is_pending());
        assert!(journal.catalog_dispatch_started());
        assert!(
            journal.catalog_dispatch_fact().is_none(),
            "no conclusion while request is pending"
        );
        assert_eq!(issued.load(Ordering::SeqCst), 1);
        assert!(journal.claim_dispatch().is_err());
        dispatch_tx.send(()).unwrap();
        let pending = original.await.unwrap();
        assert_eq!(
            journal.catalog_dispatch_fact(),
            Some((LakePublicationDisposition::KnownCommitted, true))
        );
        let mut finish = Box::pin(pending.finish(policy));
        assert!(matches!(
            std::future::Future::poll(finish.as_mut(), &mut cx),
            Poll::Pending
        ));
        assert_eq!(
            journal.catalog_dispatch_fact(),
            Some((LakePublicationDisposition::KnownCommitted, true)),
            "necessary finalization never erases Applied"
        );
        finalize_tx.send(()).unwrap();
        finish.await.unwrap();
        assert!(journal.catalog_dispatch_started());
        assert_eq!(issued.load(Ordering::SeqCst), 1);
    }
    #[tokio::test]
    async fn retained_positive_frontier_does_not_invent_a_cleanup_report() {
        let (journal, preparation) = catalog_journal();
        journal
            .claim_dispatch()
            .unwrap()
            .begin_dispatch(&preparation, &MemoryDispatchBarrier)
            .await
            .unwrap();
        journal.conclude(&CatalogOutcome::committed(
            CommitProof::applied(None),
            ExternalMutationEffect::Applied,
        ));
        let entry = journal.current_entry().unwrap();
        let frontier = journal.project_retained_committed_frontier(Report {
            publication: PublicationOutcome::Committed(CommitProof::applied(None)),
            cleanup: IcebergCleanupReport::NotAttempted,
        });
        assert!(matches!(
            frontier,
            RetainedPublicationProjection::Committed {
                finalization: ExternalMutationFinalization::Complete,
                ..
            }
        ));
        assert!(!journal.cleanup_inventory_is_disposed());
        assert!(
            journal
                .state
                .lock()
                .unwrap()
                .entries
                .get(&entry.0)
                .unwrap()
                .cleanup_report
                .is_none()
        );
        // Only this actual cleanup author can install an inventory report.
        journal.record_entry_cleanup_report(entry, IcebergCleanupReport::Complete { deleted: 0 });
        assert!(journal.cleanup_inventory_is_disposed());
    }

    #[tokio::test]
    async fn catalog_proof_is_recorded_before_finalization_and_cannot_be_overwritten() {
        let (journal, preparation) = catalog_journal();
        journal
            .claim_dispatch()
            .unwrap()
            .begin_dispatch(&preparation, &MemoryDispatchBarrier)
            .await
            .unwrap();
        assert!(journal.descriptor().unwrap().first_dispatch_ms.is_none());
        journal.conclude(&CatalogOutcome::committed(
            CommitProof::applied(None),
            ExternalMutationEffect::Applied,
        ));
        journal.record_finalization(ExternalMutationFinalization::Failed(
            ConnectorMutationFailure::new(
                ConnectorMutationFailureKind::Unavailable,
                "injected conclusion persistence failure",
            ),
        ));
        assert!(matches!(
            journal.catalog_outcome(),
            ExternalMutationOutcome::KnownCommitted {
                finalization: ExternalMutationFinalization::Failed(_),
                ..
            }
        ));
        assert!(journal.claim_dispatch().is_err());
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| journal.conclude(
                &CatalogOutcome::committed(
                    CommitProof::applied(None),
                    ExternalMutationEffect::Applied
                )
            )))
            .is_err()
        );
        assert!(matches!(
            journal.catalog_outcome(),
            ExternalMutationOutcome::KnownCommitted { .. }
        ));
    }
    #[tokio::test]
    async fn latest_disposal_does_not_erase_historical_cleanup_failure() {
        let (journal, preparation) = catalog_journal();
        let entry = journal
            .claim_dispatch()
            .unwrap()
            .begin_dispatch(&preparation, &MemoryDispatchBarrier)
            .await
            .unwrap();
        assert!(!journal.cleanup_inventory_is_disposed());
        journal.conclude(&CatalogOutcome::committed(
            CommitProof::applied(None),
            ExternalMutationEffect::Applied,
        ));
        journal.record_entry_cleanup(
            entry,
            ExternalMutationFinalization::Failed(ConnectorMutationFailure::new(
                ConnectorMutationFailureKind::Unavailable,
                "earlier real cleanup failed",
            )),
        );
        assert!(!journal.cleanup_inventory_is_disposed());
        journal.record_entry_cleanup_report(entry, IcebergCleanupReport::Complete { deleted: 1 });
        assert!(journal.cleanup_inventory_is_disposed());
        assert!(!journal.cleanup_is_complete());
        assert!(matches!(
            journal.catalog_entry_outcome(entry),
            ExternalMutationOutcome::KnownCommitted {
                cleanup: ExternalMutationFinalization::Failed(_),
                ..
            }
        ));
    }
    #[tokio::test]
    async fn absent_cleanup_attempt_preserves_exact_completion_and_independent_failure() {
        let (journal, preparation) = catalog_journal();
        let entry = journal
            .claim_dispatch()
            .unwrap()
            .begin_dispatch(&preparation, &MemoryDispatchBarrier)
            .await
            .unwrap();
        journal.conclude(&CatalogOutcome::committed(
            CommitProof::applied(None),
            ExternalMutationEffect::Applied,
        ));
        assert!(
            journal.observed_entry_cleanup(entry).is_none(),
            "initial Complete is not an observed cleanup report"
        );
        let exact = IcebergCleanupReport::Complete { deleted: 2 };
        journal.record_entry_cleanup_report(entry, exact.clone());
        assert_eq!(
            journal.observed_entry_cleanup(entry),
            Some(ExternalMutationFinalization::Complete)
        );
        assert!(journal.cleanup_is_complete());
        let absent = || Report {
            publication: PublicationOutcome::Committed(CommitProof::applied(None)),
            cleanup: IcebergCleanupReport::NotAttempted,
        };
        let ExternalMutationOutcome::KnownCommitted {
            finalization,
            cleanup,
            ..
        } = journal.project(absent())
        else {
            panic!("absence of cleanup cannot change a proven publication")
        };
        assert_eq!(finalization, ExternalMutationFinalization::Complete);
        assert_eq!(cleanup, ExternalMutationFinalization::Complete);
        assert_eq!(
            journal.state.lock().unwrap().entries[&entry.0]
                .cleanup_report
                .as_ref(),
            Some(&exact)
        );
        // A real, separate cleanup failure is not hidden by the same no-op.
        let failure = ConnectorMutationFailure::new(
            ConnectorMutationFailureKind::Unavailable,
            "independent reclamation failure",
        );
        journal.record_entry_cleanup(entry, ExternalMutationFinalization::Failed(failure.clone()));
        assert!(!journal.cleanup_is_complete());
        let ExternalMutationOutcome::KnownCommitted { cleanup, .. } = journal.project(absent())
        else {
            unreachable!()
        };
        assert_eq!(cleanup, ExternalMutationFinalization::Failed(failure));
        assert_eq!(
            journal.state.lock().unwrap().entries[&entry.0]
                .cleanup_report
                .as_ref(),
            Some(&exact)
        );
        assert!(journal.claim_dispatch().is_err());
    }

    #[tokio::test]
    async fn abandoned_barrier_claim_is_definitely_undispatched() {
        let (journal, _) = catalog_journal();
        let claim = journal.claim_dispatch().unwrap();
        drop(claim);
        assert!(matches!(
            journal.catalog_outcome(),
            ExternalMutationOutcome::KnownUncommitted { .. }
        ));
        assert!(journal.descriptor().unwrap().first_dispatch_ms.is_none());
    }
    #[tokio::test]
    async fn rejected_evidence_keeps_independent_finalization_failure() {
        let (journal, preparation) = catalog_journal();
        journal
            .claim_dispatch()
            .unwrap()
            .begin_dispatch(&preparation, &MemoryDispatchBarrier)
            .await
            .unwrap();
        journal.conclude_dispatch(DispatchEvidence::Rejected {
            failure: ConnectorMutationFailure::new(
                ConnectorMutationFailureKind::Conflict,
                "server rejected CAS",
            ),
            observation: ProtocolObservation {
                adapter: "rest",
                stage: "status",
                status_code: Some(409),
                header_names: vec![],
            },
        });
        journal.record_finalization(ExternalMutationFinalization::Failed(
            ConnectorMutationFailure::new(
                ConnectorMutationFailureKind::Unavailable,
                "conclusion persistence failed",
            ),
        ));
        assert!(
            matches!(journal.catalog_outcome(),ExternalMutationOutcome::KnownUncommitted { failure,cleanup:ExternalMutationFinalization::Failed(_) } if failure.kind()==ConnectorMutationFailureKind::Conflict)
        );
    }
    #[tokio::test]
    async fn positive_proof_and_owner_finalization_are_visible_as_one_fact() {
        let uuid = Uuid::from_u128(42);
        let (journal, preparation) = catalog_journal_with_attribution(AttributionPlan::Property {
            table_uuid: uuid,
            key: "operation".into(),
            value: "original".into(),
        });
        let entry = journal
            .claim_dispatch()
            .unwrap()
            .begin_dispatch(&preparation, &MemoryDispatchBarrier)
            .await
            .unwrap();
        journal.conclude_entry(
            entry,
            DispatchEvidence::Unknown {
                failure: ConnectorMutationFailure::new(
                    ConnectorMutationFailureKind::Unavailable,
                    "lost reply",
                ),
                observation: ProtocolObservation::default(),
            },
        );
        let finalization = ExternalMutationFinalization::Failed(ConnectorMutationFailure::new(
            ConnectorMutationFailureKind::Unavailable,
            "owner finalization was not observed",
        ));
        let observation = PositiveAttributionObservation::PropertyMarker {
            table_uuid: uuid,
            key: "operation".into(),
            value: "original".into(),
        };
        assert!(
            journal
                .append_positive_proof_with_finalization(
                    entry.0,
                    PositiveAttributionObservation::PropertyMarker {
                        table_uuid: uuid,
                        key: "operation".into(),
                        value: "foreign".into(),
                    },
                    finalization.clone()
                )
                .is_err()
        );
        assert!(matches!(
            journal.catalog_entry_outcome(entry),
            ExternalMutationOutcome::CommitUnknown { .. }
        ));
        let start = Arc::new(std::sync::Barrier::new(2));
        let reader_start = start.clone();
        let reader = journal.clone();
        let expected = finalization.clone();
        let task = std::thread::spawn(move || {
            reader_start.wait();
            let until = std::time::Instant::now() + std::time::Duration::from_secs(2);
            loop {
                match reader.catalog_entry_outcome(entry) {
                    ExternalMutationOutcome::CommitUnknown { .. } => {}
                    ExternalMutationOutcome::KnownCommitted { finalization, .. } => {
                        assert_eq!(
                            finalization, expected,
                            "first Applied observation must include the owner fact"
                        );
                        break;
                    }
                    other => panic!("unexpected publication outcome: {other:?}"),
                }
                assert!(
                    std::time::Instant::now() < until,
                    "positive observation did not become visible"
                );
                std::thread::yield_now();
            }
        });
        start.wait();
        journal
            .append_positive_proof_with_finalization(
                entry.0,
                observation.clone(),
                finalization.clone(),
            )
            .unwrap();
        task.join().unwrap();
        assert!(
            journal
                .append_positive_proof_with_finalization(
                    entry.0,
                    observation,
                    ExternalMutationFinalization::Complete
                )
                .is_err()
        );
        assert!(
            matches!(journal.catalog_entry_outcome(entry), ExternalMutationOutcome::KnownCommitted { finalization: actual, .. } if actual == finalization)
        );
        assert!(matches!(
            journal.state.lock().unwrap().entries[&entry.0].phase,
            EntryPhase::Concluded(DispatchEvidence::Unknown { .. })
        ));
    }

    #[tokio::test]
    async fn unknown_accepts_only_its_frozen_positive_probe_and_preserves_original_observation() {
        let uuid = Uuid::from_u128(42);
        let plan = AttributionPlan::Property {
            table_uuid: uuid,
            key: "operation".into(),
            value: "original".into(),
        };
        let (journal, preparation) = catalog_journal_with_attribution(plan);
        let key = journal.descriptor().unwrap().key;
        journal
            .claim_dispatch()
            .unwrap()
            .begin_dispatch(&preparation, &MemoryDispatchBarrier)
            .await
            .unwrap();
        journal.conclude_dispatch(DispatchEvidence::Unknown {
            failure: ConnectorMutationFailure::new(
                ConnectorMutationFailureKind::Unavailable,
                "original lost response",
            ),
            observation: ProtocolObservation {
                adapter: "rest",
                stage: "body",
                status_code: Some(200),
                header_names: vec![],
            },
        });
        let observation = PositiveAttributionObservation::PropertyMarker {
            table_uuid: uuid,
            key: "operation".into(),
            value: "original".into(),
        };
        let foreign = PositiveAttributionObservation::PropertyMarker {
            table_uuid: uuid,
            key: "operation".into(),
            value: "other".into(),
        };
        assert!(journal.append_positive_proof(key, foreign).is_err());
        assert!(matches!(
            journal.catalog_outcome(),
            ExternalMutationOutcome::CommitUnknown { .. }
        ));
        journal
            .append_positive_proof(key, observation.clone())
            .unwrap();
        assert!(matches!(
            journal.catalog_outcome(),
            ExternalMutationOutcome::KnownCommitted { .. }
        ));
        assert!(journal.append_positive_proof(key, observation).is_err());
        let state = journal.state.lock().unwrap();
        let EntryPhase::Concluded(DispatchEvidence::Unknown {
            failure,
            observation,
        }) = &state.entries.get(&key).unwrap().phase
        else {
            panic!("original evidence replaced")
        };
        assert_eq!(failure.message(), "original lost response");
        assert_eq!(observation.status_code, Some(200));
    }
    #[tokio::test]
    async fn cancellation_of_an_actually_pending_barrier_never_enters_dispatch() {
        struct Held {
            entered: tokio::sync::Notify,
            release: tokio::sync::Notify,
        }
        #[async_trait]
        impl DispatchBarrier for Held {
            async fn wait(&self, _: &ConnectorRequestContext) -> Result<()> {
                self.entered.notify_one();
                self.release.notified().await;
                Ok(())
            }
        }
        let (journal, preparation) = catalog_journal();
        let stop = novarocks_spi::connector::ConnectorStopOwner::new();
        let preparation = preparation.with_additional_stop(stop.view());
        let barrier = Arc::new(Held {
            entered: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
        });
        let claim = journal.claim_dispatch().unwrap();
        let task_barrier = Arc::clone(&barrier);
        let task = tokio::spawn(async move {
            claim
                .begin_dispatch(&preparation, task_barrier.as_ref())
                .await
        });
        barrier.entered.notified().await;
        assert!(journal.claim_dispatch().is_err());
        stop.request_stop();
        assert!(task.await.unwrap().is_err());
        assert!(matches!(
            journal.catalog_outcome(),
            ExternalMutationOutcome::KnownUncommitted { .. }
        ));
        assert!(journal.descriptor().unwrap().first_dispatch_ms.is_none());
    }
    #[tokio::test]
    async fn catalog_receipt_seeds_follow_recorded_effect_and_survive_late_bridge_failure() {
        use novarocks_spi::connector::ExternalMutationEffect;
        for effect in [
            ExternalMutationEffect::Applied,
            ExternalMutationEffect::NoOp,
        ] {
            let (template, preparation) = catalog_journal();
            let checked = template.current_checked().unwrap();
            let journal = PublicationJournal::new(template.operation);
            let entry = journal
                .prepare_catalog_with_receipts(
                    checked.evidence,
                    PublicationReceipts::for_effects(
                        "attempt UUID/digest".to_string(),
                        "no attempted metadata published".to_string(),
                    ),
                    vec!["db.t".into()],
                    b"frozen conditional create".to_vec(),
                    AttributionPlan::ObserveOnly,
                    preparation.clone(),
                )
                .unwrap();
            let issued = journal
                .claim_dispatch()
                .unwrap()
                .begin_dispatch(&preparation, &MemoryDispatchBarrier)
                .await
                .unwrap();
            assert_eq!(issued, entry);
            let mut proof = CommitProof::applied(None);
            proof.effect = effect;
            journal.conclude_entry(entry, DispatchEvidence::Applied { proof });
            let expected = if effect == ExternalMutationEffect::Applied {
                "attempt UUID/digest"
            } else {
                "no attempted metadata published"
            };
            assert!(
                matches!(journal.catalog_entry_outcome(entry), ExternalMutationOutcome::KnownCommitted { effect: actual, receipt, .. } if actual == effect && receipt == expected)
            );
            assert!(
                matches!(journal.catalog_bridge_failure("late bridge failure".into()), ExternalMutationOutcome::KnownCommitted { cleanup: _, effect: actual, receipt, finalization: ExternalMutationFinalization::Failed(_) } if actual == effect && receipt == expected)
            );
        }
    }

    #[tokio::test]
    async fn catalog_unknown_never_selects_no_op_receipt_and_positive_proof_selects_applied_facts()
    {
        let (template, preparation) = catalog_journal();
        let checked = template.current_checked().unwrap();
        let journal = PublicationJournal::new(template.operation);
        let uuid = Uuid::from_u128(1);
        let location = "s3://warehouse/db/t/metadata/v1.metadata.json".to_string();
        let digest = "a".repeat(64);
        let entry = journal
            .prepare_catalog_with_receipts(
                checked.evidence,
                PublicationReceipts::for_effects(
                    "attempt UUID/digest".to_string(),
                    "no attempted metadata published".to_string(),
                ),
                vec!["db.t".into()],
                b"frozen conditional create".to_vec(),
                AttributionPlan::ExactMetadata {
                    table_uuid: uuid,
                    location: location.clone(),
                    digest: digest.clone(),
                },
                preparation.clone(),
            )
            .unwrap();
        journal
            .claim_dispatch()
            .unwrap()
            .begin_dispatch(&preparation, &MemoryDispatchBarrier)
            .await
            .unwrap();
        journal.conclude_entry(
            entry,
            DispatchEvidence::Unknown {
                failure: ConnectorMutationFailure::new(
                    ConnectorMutationFailureKind::Unavailable,
                    "lost response",
                ),
                observation: ProtocolObservation {
                    adapter: "hadoop",
                    stage: "conditional-create",
                    ..Default::default()
                },
            },
        );
        assert!(matches!(
            journal.catalog_entry_outcome(entry),
            ExternalMutationOutcome::CommitUnknown { .. }
        ));
        assert!(matches!(
            journal.catalog_bridge_failure("later failure".into()),
            ExternalMutationOutcome::CommitUnknown { .. }
        ));
        journal
            .append_positive_proof(
                entry.0,
                PositiveAttributionObservation::ExactMetadata {
                    table_uuid: uuid,
                    location,
                    digest,
                },
            )
            .unwrap();
        assert!(
            matches!(journal.catalog_entry_outcome(entry), ExternalMutationOutcome::KnownCommitted { effect: novarocks_spi::connector::ExternalMutationEffect::Applied, receipt, .. } if receipt == "attempt UUID/digest")
        );
    }

    #[tokio::test]
    async fn delayed_finalization_is_bound_to_its_entry_after_next_request_is_prepared() {
        let (journal, preparation) = catalog_journal();
        let first = journal
            .claim_dispatch()
            .unwrap()
            .begin_dispatch(&preparation, &MemoryDispatchBarrier)
            .await
            .unwrap();
        journal.conclude_entry(
            first,
            DispatchEvidence::Applied {
                proof: CommitProof::applied(None),
            },
        );
        let checked = journal.current_checked().unwrap();
        let second = journal
            .prepare_catalog(
                checked.evidence,
                "second receipt".into(),
                vec!["namespace:other".into()],
                b"create other".to_vec(),
                AttributionPlan::ObserveOnly,
                preparation.clone(),
            )
            .unwrap();
        journal.record_entry_finalization(
            first,
            ExternalMutationFinalization::Failed(ConnectorMutationFailure::new(
                ConnectorMutationFailureKind::Unavailable,
                "first finalization failed",
            )),
        );
        assert!(
            matches!(journal.catalog_entry_outcome(first),ExternalMutationOutcome::KnownCommitted {receipt,finalization:ExternalMutationFinalization::Failed(_),..} if receipt=="namespace receipt")
        );
        let second_dispatch = journal
            .claim_dispatch()
            .unwrap()
            .begin_dispatch(&preparation, &MemoryDispatchBarrier)
            .await
            .unwrap();
        assert_eq!(second, second_dispatch);
        journal.conclude_entry(
            second,
            DispatchEvidence::Applied {
                proof: CommitProof::applied(None),
            },
        );
        assert!(
            matches!(journal.catalog_entry_outcome(second),ExternalMutationOutcome::KnownCommitted {receipt,finalization:ExternalMutationFinalization::Complete,..} if receipt=="second receipt")
        );
    }
    #[tokio::test]
    async fn projected_unknown_retains_exact_owner_without_a_journal_cycle() {
        let uuid = Uuid::from_u128(1);
        let location = "s3://bucket/table/metadata/v1.metadata.json".to_string();
        let digest = "a".repeat(64);
        let (journal, context) = catalog_journal_with_attribution(AttributionPlan::ExactMetadata {
            table_uuid: uuid,
            location: location.clone(),
            digest: digest.clone(),
        });
        let weak = Arc::downgrade(&journal.state);
        let entry = journal
            .claim_dispatch()
            .unwrap()
            .begin_dispatch(&context, &MemoryDispatchBarrier)
            .await
            .unwrap();
        journal.conclude_entry(
            entry,
            DispatchEvidence::Unknown {
                failure: ConnectorMutationFailure::new(
                    ConnectorMutationFailureKind::Unavailable,
                    "lost reply",
                ),
                observation: ProtocolObservation::default(),
            },
        );
        let ExternalMutationOutcome::CommitUnknown { evidence, .. } =
            journal.catalog_entry_outcome(entry)
        else {
            panic!("unknown");
        };
        assert!(
            journal
                .current_checked()
                .unwrap()
                .evidence
                .process_local_payload()
                .is_none()
        );
        let wire = evidence.try_to_wire_v1().unwrap();
        let decoded = ExternalMutationEvidence::try_from_wire_v1(&wire).unwrap();
        assert_eq!(decoded, evidence);
        assert!(
            PublicationJournal::<String>::from_evidence(&decoded)
                .unwrap()
                .is_none()
        );
        assert!(PublicationJournal::<u8>::from_evidence(&evidence).is_err());
        drop(journal);
        assert!(weak.upgrade().is_some());
        let (original, original_entry) = PublicationJournal::<String>::from_evidence(&evidence)
            .unwrap()
            .unwrap();
        assert_eq!(original_entry, entry);
        let proof = PositiveAttributionObservation::ExactMetadata {
            table_uuid: uuid,
            location,
            digest,
        };
        original
            .append_positive_proof(entry.0, proof.clone())
            .unwrap();
        assert!(original.append_positive_proof(entry.0, proof).is_err());
        assert!(
            matches!(original.catalog_entry_outcome(entry), ExternalMutationOutcome::KnownCommitted { receipt, .. } if receipt == "namespace receipt")
        );
        {
            let state = original.state.lock().unwrap();
            let record = state.entries.get(&entry.0).unwrap();
            assert!(matches!(
                record.phase,
                EntryPhase::Concluded(DispatchEvidence::Unknown { .. })
            ));
            assert!(record.positive_proof.is_some());
        }
        drop(original);
        drop(evidence);
        assert!(
            weak.upgrade().is_none(),
            "no strong owner cycle in journal evidence"
        );
    }

    #[tokio::test]
    async fn evidence_attachment_cannot_be_reused_for_another_operation() {
        let (journal, context) = catalog_journal();
        let entry = journal
            .claim_dispatch()
            .unwrap()
            .begin_dispatch(&context, &MemoryDispatchBarrier)
            .await
            .unwrap();
        journal.conclude_entry(
            entry,
            DispatchEvidence::Unknown {
                failure: ConnectorMutationFailure::new(
                    ConnectorMutationFailureKind::Unavailable,
                    "lost reply",
                ),
                observation: ProtocolObservation::default(),
            },
        );
        let ExternalMutationOutcome::CommitUnknown { evidence, .. } =
            journal.catalog_entry_outcome(entry)
        else {
            panic!("unknown");
        };
        let changed = ExternalMutationEvidence::try_new(
            evidence.schema_version(),
            evidence.descriptor().clone(),
            evidence.incarnation(),
            ConnectorMutationOperationId::new(),
            evidence.operation_kind(),
            evidence.provider_payload().clone(),
        )
        .unwrap()
        .with_process_local_payload(evidence.process_local_payload().unwrap().clone())
        .unwrap();
        assert!(
            matches!(PublicationJournal::<String>::from_evidence(&changed), Err(error) if error.kind() == ConnectorErrorKind::InvalidRequest)
        );
        assert!(matches!(
            journal.catalog_entry_outcome(entry),
            ExternalMutationOutcome::CommitUnknown { .. }
        ));
    }
}
