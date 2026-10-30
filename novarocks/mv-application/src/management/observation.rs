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

use std::collections::HashSet;
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use novarocks_spi::connector::ConnectorCommittedVersion;

use super::{
    CreateIntent, DeploymentOwner, EffectIdentity, ManagedMvTarget, ManagementOwnershipError,
    ProcessIncarnation, ReadmissionPermit, UnsettledEffect,
};

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct ManagementObservationRequestId([u8; 16]);

impl ManagementObservationRequestId {
    pub const fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }
}

#[derive(Clone)]
pub(super) struct ManagementObservationAuthorization(Arc<()>);

impl ManagementObservationAuthorization {
    pub(super) fn new() -> Self {
        Self(Arc::new(()))
    }

    pub(super) fn matches(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

#[derive(Clone, Debug)]
pub(super) struct ManagementObservationLiveness(Arc<AtomicBool>);

impl ManagementObservationLiveness {
    pub(super) fn new() -> Self {
        Self(Arc::new(AtomicBool::new(true)))
    }

    pub(super) fn is_open(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }

    pub(super) fn close(&self) {
        self.0.store(false, Ordering::Release);
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ManagementContinuation {
    SameOwner {
        previous_incarnation: ProcessIncarnation,
    },
    OwnerHandover {
        previous_owner: DeploymentOwner,
        previous_incarnation: ProcessIncarnation,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RegistrationRequirement {
    Incarnation,
    OwnerAndIncarnation,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ManagementObservationPhase {
    AwaitingEffectClosure,
    AwaitingFreshObservation,
    RegistrationRequired(RegistrationRequirement),
    AwaitingRegisteredObservation,
    Ready,
    ClosedForeignOwner,
    ClosedIncarnationMismatch,
    ClosedObjectReplacement,
}

/// Single-use wrapper minted only by completing a controller-issued current
/// observation request. Historical frozen observations have no such ticket.
#[derive(Debug, Eq, PartialEq)]
pub struct FreshManagementObservation {
    request_id: ManagementObservationRequestId,
    target: ManagedMvTarget,
    owner: DeploymentOwner,
    incarnation: ProcessIncarnation,
    metadata_version: ConnectorCommittedVersion,
}

impl FreshManagementObservation {
    pub const fn target(&self) -> &ManagedMvTarget {
        &self.target
    }

    pub const fn owner(&self) -> &DeploymentOwner {
        &self.owner
    }

    pub const fn incarnation(&self) -> &ProcessIncarnation {
        &self.incarnation
    }

    pub const fn metadata_version(&self) -> &ConnectorCommittedVersion {
        &self.metadata_version
    }

    #[cfg(test)]
    pub(super) fn for_test(
        request_id: ManagementObservationRequestId,
        target: ManagedMvTarget,
        owner: DeploymentOwner,
        incarnation: ProcessIncarnation,
        metadata_version: ConnectorCommittedVersion,
    ) -> Self {
        Self {
            request_id,
            target,
            owner,
            incarnation,
            metadata_version,
        }
    }
}

pub struct PendingManagementObservation {
    request_id: ManagementObservationRequestId,
    target: ManagedMvTarget,
}

/// A one-shot observation capability issued only for a CREATE responsibility
/// whose stage request was dispatched before a physical object identity was
/// available. It does not accept a raw target: completion must consume a
/// sealed provider management observation and verify the original logical
/// create intent.
pub struct PendingCreateIntentObservation {
    intent: CreateIntent,
}

impl PendingCreateIntentObservation {
    pub fn complete_keyed(
        self,
        lease: &novarocks_spi::connector::ConnectorDocumentStorageLease,
        request: novarocks_spi::connector::KeyedDocumentObservationRequest,
    ) -> Result<
        (
            FreshCreateIntentObservation,
            novarocks_spi::connector::KeyedDocumentView,
        ),
        ManagementObservationError,
    > {
        if request.catalog_handle != *self.intent.catalog()
            || request.target != *self.intent.table()
        {
            return Err(ManagementObservationError::EffectTargetMismatch);
        }
        let catalog = request.catalog_handle.clone();
        let view = lease
            .observe_keyed_documents(request)
            .map_err(|error| match error {
                novarocks_spi::connector::KeyedDocumentReadError::Invalid(error)
                    if error.table_object_binding_failure()
                        == Some(
                            novarocks_spi::connector::ConnectorTableObjectBindingFailure::Replaced,
                        ) =>
                {
                    ManagementObservationError::Ownership(ManagementOwnershipError::TargetReplaced)
                }
                _ => ManagementObservationError::UnsealedObservation,
            })?;
        let slot = view
            .slots
            .get(
                &crate::persistence::keyed_documents::management_slot_name()
                    .map_err(|_| ManagementObservationError::InvalidProvenance)?,
            )
            .ok_or(ManagementObservationError::InvalidProvenance)?;
        crate::persistence::keyed_documents::decode_management_slot(&slot.value)
            .map_err(|_| ManagementObservationError::InvalidProvenance)?;
        let target = ManagedMvTarget::try_new(catalog, view.table.clone(), view.object_id.clone())?;
        let target = self.intent.bind_target(target)?;
        Ok((
            FreshCreateIntentObservation {
                intent: self.intent,
                target,
            },
            view,
        ))
    }

    pub(super) const fn for_intent(intent: CreateIntent) -> Self {
        Self { intent }
    }
}

/// Exact provider observation that may narrow one previously unbound CREATE
/// intent. It is intentionally consumable and cannot be constructed from a
/// caller-supplied object id.
pub struct FreshCreateIntentObservation {
    intent: CreateIntent,
    target: ManagedMvTarget,
}

impl FreshCreateIntentObservation {
    pub const fn target(&self) -> &ManagedMvTarget {
        &self.target
    }

    pub(crate) fn into_parts(self) -> (CreateIntent, ManagedMvTarget) {
        (self.intent, self.target)
    }
}

impl PendingManagementObservation {
    /// Obtain the fresh observation through the retained lease itself; raw
    /// decoded keyed views cannot mint a management admission capability.
    pub fn complete_keyed(
        self,
        lease: &novarocks_spi::connector::ConnectorDocumentStorageLease,
        request: novarocks_spi::connector::KeyedDocumentObservationRequest,
    ) -> Result<
        (
            FreshManagementObservation,
            novarocks_spi::connector::KeyedDocumentView,
        ),
        ManagementObservationError,
    > {
        self.validate_request(&request)?;
        let view = lease
            .observe_keyed_documents(request)
            .map_err(|error| document_observation_kind(&error))?;
        self.finish_keyed_view(view)
    }

    /// Await the retained lease's actual asynchronous observation. This consumes
    /// the pending capability, but does not own or retire the Provider source.
    pub async fn complete_keyed_async(
        self,
        lease: &novarocks_spi::connector::ConnectorDocumentStorageLease,
        request: novarocks_spi::connector::KeyedDocumentObservationRequest,
    ) -> Result<
        (
            FreshManagementObservation,
            novarocks_spi::connector::KeyedDocumentView,
        ),
        ManagementObservationFailure,
    > {
        self.validate_request(&request)?;
        let view = lease
            .observe_keyed_documents_async(request)
            .await
            .map_err(ManagementObservationFailure::from_document)?;
        self.finish_keyed_view(view).map_err(Into::into)
    }

    fn validate_request(
        &self,
        request: &novarocks_spi::connector::KeyedDocumentObservationRequest,
    ) -> Result<(), ManagementObservationError> {
        if request.catalog_handle != *self.target.catalog()
            || request.target != *self.target.table()
            || request.object_id != *self.target.object_id()
        {
            return Err(ManagementObservationError::EffectTargetMismatch);
        }
        Ok(())
    }

    fn finish_keyed_view(
        self,
        view: novarocks_spi::connector::KeyedDocumentView,
    ) -> Result<
        (
            FreshManagementObservation,
            novarocks_spi::connector::KeyedDocumentView,
        ),
        ManagementObservationError,
    > {
        let slot = view
            .slots
            .get(
                &crate::persistence::keyed_documents::management_slot_name()
                    .map_err(|_| ManagementObservationError::InvalidProvenance)?,
            )
            .ok_or(ManagementObservationError::InvalidProvenance)?;
        let (owner, incarnation) =
            crate::persistence::keyed_documents::decode_management_slot(&slot.value)
                .map_err(|_| ManagementObservationError::InvalidProvenance)?;
        let fresh = FreshManagementObservation {
            request_id: self.request_id,
            target: self.target,
            owner,
            incarnation,
            metadata_version: view.metadata_version.clone(),
        };
        Ok((fresh, view))
    }
}

/// Target-local readmission state. It never records an alternative outcome for
/// an unknown effect and cannot execute provider operations.
pub struct ManagementObservationState {
    target: ManagedMvTarget,
    local_owner: DeploymentOwner,
    local_incarnation: ProcessIncarnation,
    continuation: ManagementContinuation,
    phase: ManagementObservationPhase,
    unsettled: HashSet<EffectIdentity>,
    resolved_effects: HashSet<EffectIdentity>,
    active_request: Option<ManagementObservationRequestId>,
    consumed_requests: HashSet<ManagementObservationRequestId>,
    required_committed_effect: Option<EffectIdentity>,
    authorization: Option<ManagementObservationAuthorization>,
    liveness: ManagementObservationLiveness,
    latest_metadata_version: Option<ConnectorCommittedVersion>,
}

impl ManagementObservationState {
    pub(super) fn try_new(
        target: ManagedMvTarget,
        local_owner: DeploymentOwner,
        local_incarnation: ProcessIncarnation,
        continuation: ManagementContinuation,
        unsettled: Vec<UnsettledEffect>,
        required_committed_effect: Option<EffectIdentity>,
        authorization: Option<ManagementObservationAuthorization>,
    ) -> Result<Self, ManagementObservationError> {
        if !unsettled.is_empty() && required_committed_effect.is_some() {
            return Err(ManagementObservationError::InvalidProvenance);
        }
        let previous_incarnation = match &continuation {
            ManagementContinuation::SameOwner {
                previous_incarnation,
            }
            | ManagementContinuation::OwnerHandover {
                previous_incarnation,
                ..
            } => previous_incarnation,
        };
        let mut identities = HashSet::with_capacity(unsettled.len());
        for effect in &unsettled {
            if effect.responsibility().target() != &target {
                return Err(ManagementObservationError::EffectTargetMismatch);
            }
            if effect.responsibility().dispatching_incarnation() != previous_incarnation {
                return Err(ManagementObservationError::InvalidRecoveryBarrier);
            }
            if !identities.insert(effect.responsibility().identity()) {
                return Err(ManagementObservationError::DuplicateEffect);
            }
        }
        let phase = if identities.is_empty() {
            ManagementObservationPhase::AwaitingFreshObservation
        } else {
            ManagementObservationPhase::AwaitingEffectClosure
        };
        Ok(Self {
            target,
            local_owner,
            local_incarnation,
            continuation,
            phase,
            unsettled: identities,
            resolved_effects: HashSet::new(),
            active_request: None,
            consumed_requests: HashSet::new(),
            required_committed_effect,
            authorization,
            liveness: ManagementObservationLiveness::new(),
            latest_metadata_version: None,
        })
    }

    pub const fn phase(&self) -> ManagementObservationPhase {
        self.phase
    }

    pub const fn target(&self) -> &ManagedMvTarget {
        &self.target
    }

    pub const fn local_owner(&self) -> &DeploymentOwner {
        &self.local_owner
    }

    pub const fn local_incarnation(&self) -> &ProcessIncarnation {
        &self.local_incarnation
    }

    pub(crate) fn resolved_effects(&self) -> &HashSet<EffectIdentity> {
        &self.resolved_effects
    }

    pub(crate) const fn required_committed_effect(&self) -> Option<EffectIdentity> {
        self.required_committed_effect
    }

    pub(super) const fn continuation(&self) -> &ManagementContinuation {
        &self.continuation
    }

    pub(super) const fn authorization(&self) -> Option<&ManagementObservationAuthorization> {
        self.authorization.as_ref()
    }

    pub(super) const fn liveness(&self) -> &ManagementObservationLiveness {
        &self.liveness
    }

    pub fn accept_readmission_permit(
        &mut self,
        permit: ReadmissionPermit,
    ) -> Result<(), ManagementObservationError> {
        if permit.target() != &self.target {
            return Err(ManagementObservationError::EffectTargetMismatch);
        }
        if !permit.preserves_unknown_disposition() || !self.unsettled.remove(&permit.effect()) {
            return Err(ManagementObservationError::UnknownEffect);
        }
        self.resolved_effects.insert(permit.effect());
        if self.unsettled.is_empty() {
            self.phase = ManagementObservationPhase::AwaitingFreshObservation;
        }
        Ok(())
    }

    pub fn begin_current_observation(
        &mut self,
        request_id: ManagementObservationRequestId,
    ) -> Result<PendingManagementObservation, ManagementObservationError> {
        if !matches!(
            self.phase,
            ManagementObservationPhase::AwaitingFreshObservation
                | ManagementObservationPhase::AwaitingRegisteredObservation
                | ManagementObservationPhase::Ready
        ) {
            return Err(ManagementObservationError::ObservationNotAllowed);
        }
        if self.consumed_requests.contains(&request_id) {
            return Err(ManagementObservationError::ReusedObservationRequest);
        }
        if self.active_request.is_some() {
            return Err(ManagementObservationError::ObservationAlreadyPending);
        }
        self.active_request = Some(request_id);
        Ok(PendingManagementObservation {
            request_id,
            target: self.target.clone(),
        })
    }

    pub fn accept_current_observation(
        &mut self,
        observation: FreshManagementObservation,
    ) -> Result<ManagementObservationPhase, ManagementObservationError> {
        self.consume_current_request(observation.request_id)?;
        self.apply_current_observation(observation)
    }

    fn apply_current_observation(
        &mut self,
        observation: FreshManagementObservation,
    ) -> Result<ManagementObservationPhase, ManagementObservationError> {
        if observation.target() != &self.target {
            self.phase = ManagementObservationPhase::ClosedObjectReplacement;
            return Err(ManagementObservationError::Ownership(
                ManagementOwnershipError::TargetReplaced,
            ));
        }
        self.latest_metadata_version = Some(observation.metadata_version().clone());

        let result = match self.phase {
            ManagementObservationPhase::AwaitingFreshObservation => {
                self.accept_pre_registration_observation(&observation)
            }
            ManagementObservationPhase::AwaitingRegisteredObservation
            | ManagementObservationPhase::Ready => {
                self.accept_registered_or_live_observation(&observation)
            }
            _ => Err(ManagementObservationError::ObservationNotAllowed),
        };
        if result.is_err() {
            self.close_for_observation_error(&result);
        }
        result
    }

    /// Consume the current ticket even when a keyed provider observation fails.
    /// The retained lease mints the fresh capability; callers cannot substitute
    /// a historical view or reapply the same observation ticket.
    pub fn complete_keyed_current_observation(
        &mut self,
        pending: PendingManagementObservation,
        lease: &novarocks_spi::connector::ConnectorDocumentStorageLease,
        request: novarocks_spi::connector::KeyedDocumentObservationRequest,
    ) -> Result<
        (
            ManagementObservationPhase,
            novarocks_spi::connector::KeyedDocumentView,
        ),
        ManagementObservationError,
    > {
        self.consume_current_request(pending.request_id)?;
        let (fresh, view) = match pending.complete_keyed(lease, request) {
            Ok(observation) => observation,
            Err(error) => {
                if matches!(
                    error,
                    ManagementObservationError::Ownership(ManagementOwnershipError::TargetReplaced)
                ) {
                    self.phase = ManagementObservationPhase::ClosedObjectReplacement;
                    self.liveness.close();
                }
                return Err(error);
            }
        };
        let phase = self.apply_current_observation(fresh)?;
        Ok((phase, view))
    }

    /// Consume the exact ticket before polling foreign I/O. No State mutex is
    /// held here; the caller retains the same Source while this borrower waits.
    pub async fn complete_keyed_current_observation_async(
        &mut self,
        pending: PendingManagementObservation,
        lease: &novarocks_spi::connector::ConnectorDocumentStorageLease,
        request: novarocks_spi::connector::KeyedDocumentObservationRequest,
    ) -> Result<
        (
            ManagementObservationPhase,
            novarocks_spi::connector::KeyedDocumentView,
        ),
        ManagementObservationFailure,
    > {
        self.consume_current_request(pending.request_id)?;
        let (fresh, view) = match pending.complete_keyed_async(lease, request).await {
            Ok(observation) => observation,
            Err(error) => {
                if matches!(
                    error.kind(),
                    ManagementObservationError::Ownership(ManagementOwnershipError::TargetReplaced)
                ) {
                    self.phase = ManagementObservationPhase::ClosedObjectReplacement;
                    self.liveness.close();
                }
                return Err(error);
            }
        };
        let phase = self.apply_current_observation(fresh)?;
        Ok((phase, view))
    }

    pub fn record_registration_terminal(
        &mut self,
        terminal: super::EffectTerminalFact,
    ) -> Result<(), ManagementObservationError> {
        if !matches!(
            self.phase,
            ManagementObservationPhase::RegistrationRequired(_)
        ) {
            return Err(ManagementObservationError::RegistrationNotAllowed);
        }
        let responsibility = match &terminal {
            super::EffectTerminalFact::KnownCommitted(responsibility)
            | super::EffectTerminalFact::KnownUncommitted(responsibility) => responsibility,
            super::EffectTerminalFact::CommitUnknown(effect) => effect.responsibility(),
        };
        if responsibility.target() != &self.target
            || !responsibility
                .scope()
                .contains(super::EffectPath::CatalogCommit)
        {
            return Err(ManagementObservationError::EffectTargetMismatch);
        }
        match terminal {
            super::EffectTerminalFact::KnownCommitted(_) => {
                self.phase = ManagementObservationPhase::AwaitingRegisteredObservation;
            }
            super::EffectTerminalFact::KnownUncommitted(_) => {}
            super::EffectTerminalFact::CommitUnknown(effect) => {
                let identity = effect.responsibility().identity();
                if !self.unsettled.insert(identity) {
                    return Err(ManagementObservationError::DuplicateEffect);
                }
                self.phase = ManagementObservationPhase::AwaitingEffectClosure;
            }
        }
        Ok(())
    }

    pub const fn latest_metadata_version(&self) -> Option<&ConnectorCommittedVersion> {
        self.latest_metadata_version.as_ref()
    }

    fn accept_pre_registration_observation(
        &mut self,
        observation: &FreshManagementObservation,
    ) -> Result<ManagementObservationPhase, ManagementObservationError> {
        let requirement = match &self.continuation {
            ManagementContinuation::SameOwner {
                previous_incarnation,
            } => {
                if observation.owner() != &self.local_owner {
                    return Err(ManagementObservationError::ForeignOwner);
                }
                if observation.incarnation() == &self.local_incarnation {
                    self.phase = ManagementObservationPhase::Ready;
                    return Ok(self.phase);
                }
                if observation.incarnation() != previous_incarnation {
                    return Err(ManagementObservationError::IncarnationMismatch);
                }
                RegistrationRequirement::Incarnation
            }
            ManagementContinuation::OwnerHandover {
                previous_owner,
                previous_incarnation,
            } => {
                if observation.owner() == &self.local_owner
                    && observation.incarnation() == &self.local_incarnation
                {
                    self.phase = ManagementObservationPhase::Ready;
                    return Ok(self.phase);
                }
                if observation.owner() != previous_owner {
                    return Err(ManagementObservationError::ForeignOwner);
                }
                if observation.incarnation() != previous_incarnation {
                    return Err(ManagementObservationError::IncarnationMismatch);
                }
                RegistrationRequirement::OwnerAndIncarnation
            }
        };
        self.phase = ManagementObservationPhase::RegistrationRequired(requirement);
        Ok(self.phase)
    }

    fn accept_registered_or_live_observation(
        &mut self,
        observation: &FreshManagementObservation,
    ) -> Result<ManagementObservationPhase, ManagementObservationError> {
        if observation.owner() != &self.local_owner {
            return Err(ManagementObservationError::ForeignOwner);
        }
        if observation.incarnation() != &self.local_incarnation {
            return Err(ManagementObservationError::IncarnationMismatch);
        }
        self.phase = ManagementObservationPhase::Ready;
        Ok(self.phase)
    }

    fn close_for_observation_error(
        &mut self,
        result: &Result<ManagementObservationPhase, ManagementObservationError>,
    ) {
        self.phase = match result {
            Err(ManagementObservationError::ForeignOwner) => {
                ManagementObservationPhase::ClosedForeignOwner
            }
            Err(ManagementObservationError::IncarnationMismatch) => {
                ManagementObservationPhase::ClosedIncarnationMismatch
            }
            Err(ManagementObservationError::Ownership(
                ManagementOwnershipError::TargetReplaced,
            )) => ManagementObservationPhase::ClosedObjectReplacement,
            _ => self.phase,
        };
        if matches!(
            self.phase,
            ManagementObservationPhase::ClosedForeignOwner
                | ManagementObservationPhase::ClosedIncarnationMismatch
                | ManagementObservationPhase::ClosedObjectReplacement
        ) {
            self.liveness.close();
        }
    }

    fn consume_current_request(
        &mut self,
        request_id: ManagementObservationRequestId,
    ) -> Result<(), ManagementObservationError> {
        if self.consumed_requests.contains(&request_id) {
            return Err(ManagementObservationError::ReusedObservationRequest);
        }
        if self.active_request != Some(request_id) {
            return Err(ManagementObservationError::StaleObservation);
        }
        self.active_request = None;
        self.consumed_requests.insert(request_id);
        Ok(())
    }
}

/// The existing management decision plus the Provider's originating failure.
/// This sidecar neither changes the domain policy nor grants execution rights.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ManagementObservationFailure {
    kind: ManagementObservationError,
    original_failure: Option<novarocks_failure_contract::SharedFailureCause>,
}

impl ManagementObservationFailure {
    pub const fn kind(&self) -> &ManagementObservationError {
        &self.kind
    }

    pub fn original_failure(&self) -> Option<&novarocks_failure_contract::SharedFailureCause> {
        self.original_failure.as_ref()
    }

    fn from_document(error: novarocks_spi::connector::KeyedDocumentReadError) -> Self {
        let kind = document_observation_kind(&error);
        let original_failure = match error {
            novarocks_spi::connector::KeyedDocumentReadError::Invalid(error) => {
                std::error::Error::source(&error)
                    .and_then(|source| {
                        source.downcast_ref::<novarocks_failure_contract::SharedFailureCause>()
                    })
                    .cloned()
                    .unwrap_or_else(|| novarocks_failure_contract::SharedFailureCause::new(error))
            }
            // The codec now authors the legacy format's own fact; capture the
            // actual typed error instead of maintaining another raw sidecar.
            legacy @ novarocks_spi::connector::KeyedDocumentReadError::LegacyFormat { .. } => {
                novarocks_failure_contract::SharedFailureCause::new(legacy)
            }
        };
        Self {
            kind,
            original_failure: Some(original_failure),
        }
    }
}

impl From<ManagementObservationError> for ManagementObservationFailure {
    fn from(kind: ManagementObservationError) -> Self {
        Self {
            kind,
            original_failure: None,
        }
    }
}

impl fmt::Display for ManagementObservationFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.kind.fmt(formatter)?;
        if let Some(original) = &self.original_failure {
            write!(formatter, ": {}", original.detail())?;
        }
        Ok(())
    }
}

impl std::error::Error for ManagementObservationFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.original_failure
            .as_ref()
            .map(|cause| cause as &(dyn std::error::Error + 'static))
    }
}

fn document_observation_kind(
    error: &novarocks_spi::connector::KeyedDocumentReadError,
) -> ManagementObservationError {
    match error {
        novarocks_spi::connector::KeyedDocumentReadError::Invalid(error)
            if error.table_object_binding_failure()
                == Some(novarocks_spi::connector::ConnectorTableObjectBindingFailure::Replaced) =>
        {
            ManagementObservationError::Ownership(ManagementOwnershipError::TargetReplaced)
        }
        _ => ManagementObservationError::UnsealedObservation,
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ManagementObservationError {
    Ownership(ManagementOwnershipError),
    EffectTargetMismatch,
    DuplicateEffect,
    UnknownEffect,
    ObservationNotAllowed,
    ObservationAlreadyPending,
    ReusedObservationRequest,
    StaleObservation,
    InvalidProvenance,
    InvalidRecoveryBarrier,
    RegistrationNotAllowed,
    ForeignOwner,
    IncarnationMismatch,
    UnsealedObservation,
}

impl fmt::Display for ManagementObservationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Ownership(error) => error.fmt(formatter),
            Self::EffectTargetMismatch => formatter.write_str("effect targets another MV"),
            Self::DuplicateEffect => formatter.write_str("duplicate unsettled effect identity"),
            Self::UnknownEffect => formatter.write_str("readmission permit is stale or unknown"),
            Self::ObservationNotAllowed => {
                formatter.write_str("fresh observation is not allowed in the current phase")
            }
            Self::ObservationAlreadyPending => {
                formatter.write_str("a fresh management observation is already pending")
            }
            Self::ReusedObservationRequest => {
                formatter.write_str("management observation request was already consumed")
            }
            Self::StaleObservation => {
                formatter.write_str("management observation request is stale or mismatched")
            }
            Self::InvalidProvenance => {
                formatter.write_str("management observation provenance is invalid")
            }
            Self::InvalidRecoveryBarrier => formatter
                .write_str("recovery barrier does not cover the isolated previous incarnation"),
            Self::RegistrationNotAllowed => {
                formatter.write_str("owner or incarnation registration is not currently allowed")
            }
            Self::ForeignOwner => formatter.write_str("materialized view has another owner"),
            Self::IncarnationMismatch => {
                formatter.write_str("fresh observation found another management incarnation")
            }
            Self::UnsealedObservation => formatter
                .write_str("management observation was not sealed by its exact storage lease"),
        }
    }
}

impl std::error::Error for ManagementObservationError {}

impl From<ManagementOwnershipError> for ManagementObservationError {
    fn from(value: ManagementOwnershipError) -> Self {
        Self::Ownership(value)
    }
}
