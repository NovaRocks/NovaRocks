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

use std::collections::HashMap;
use std::fmt;
use std::future::Future;
use std::sync::{Arc, Mutex, Weak};

use novarocks_spi::connector::{
    CatalogHandle, ConnectorCommittedVersion, ConnectorControlRuntimeId,
    ConnectorDocumentManagementOperation, ConnectorTableIdentity, ConnectorTableObjectId,
};

use crate::activity::{
    CanonicalMvTarget, MvActivityAdmissionError, MvActivityGate, MvActivityGateError,
    MvActivityLease, MvActivityObservation, MvActivityOwner, MvActivityTicket,
};

/// Single-use proof that the management owner admitted the exact same Current
/// observation after effect closure and owner/incarnation validation.
/// Historical reads and foreign-owner observations cannot construct it.
#[derive(Debug)]
pub struct MvCurrentManagementAdmission {
    target: ManagedMvTarget,
    owner: DeploymentOwner,
    incarnation: ProcessIncarnation,
    metadata_version: ConnectorCommittedVersion,
    liveness: ManagementObservationLiveness,
}

impl MvCurrentManagementAdmission {
    pub(crate) fn matches_keyed(
        &self,
        documents: &crate::persistence::keyed_documents::MvKeyedObservedCurrentDocuments,
    ) -> bool {
        self.liveness.is_open()
            && self.target == documents.management_target
            && self.owner == documents.deployment_owner
            && self.incarnation == documents.process_incarnation
            && self.metadata_version == documents.view.metadata_version
    }

    pub(crate) fn is_open(&self) -> bool {
        self.liveness.is_open()
    }

    #[cfg(test)]
    pub(crate) fn for_test_keyed(
        documents: &crate::persistence::keyed_documents::MvKeyedObservedCurrentDocuments,
    ) -> Self {
        Self {
            target: documents.management_target.clone(),
            owner: documents.deployment_owner.clone(),
            incarnation: documents.process_incarnation.clone(),
            metadata_version: documents.view.metadata_version.clone(),
            liveness: ManagementObservationLiveness::new(),
        }
    }
}

use super::observation::{
    FreshCreateIntentObservation, ManagementObservationAuthorization,
    ManagementObservationLiveness, PendingCreateIntentObservation,
};
use super::{
    CreateIntent, CreateIntentResponsibility, DeploymentOwner, EffectDisposition, EffectIdentity,
    EffectResponsibility, EffectScope, ManagedMvTarget, ManagementContinuation,
    ManagementObservationError, ManagementObservationPhase, ManagementObservationState,
    ProcessIncarnation, UnsettledEffect,
};

/// Fresh deletion-only preparation for an already installed exact target.
/// It cannot authorize a data projection, publication, or readmission.
#[derive(Debug)]
pub struct MvDropManagementObservation {
    target: ManagedMvTarget,
    dependencies: ManagementDependencySet,
    root_binding: novarocks_spi::connector::DocumentIdentity,
    configuration_revision: crate::persistence::identity::DocumentRevision,
}

impl MvDropManagementObservation {
    pub fn target(&self) -> &ManagedMvTarget {
        &self.target
    }

    pub fn request(&self) -> ManagementRequest {
        ManagementRequest {
            catalog: self.target.catalog().clone(),
            table: self.target.table().clone(),
            expected_object_id: Some(self.target.object_id().clone()),
            operation: ConnectorDocumentManagementOperation::Drop,
            expected_dependencies: Some(self.dependencies.clone()),
            effect_scope: EffectScope::CATALOG_AND_OBJECT_DELETION,
            frozen_effect_identity: None,
            create_intent: None,
        }
    }

    /// Recheck the preparation after taking the same target's activity lease.
    pub fn matches(&self, other: &Self) -> bool {
        self.target == other.target
            && self.dependencies == other.dependencies
            && self.root_binding == other.root_binding
            && self.configuration_revision == other.configuration_revision
    }
}

/// Exact MV-domain dependencies frozen before a long computation. Reacquiring
/// the entrance with an older set fails rather than publishing old output.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ManagementDependencySet {
    definition_revision: [u8; 32],
    interpretation_revision: [u8; 32],
    publication_base: Option<[u8; 32]>,
    control_runtime_id: ConnectorControlRuntimeId,
}

impl ManagementDependencySet {
    pub const fn new(
        definition_revision: [u8; 32],
        interpretation_revision: [u8; 32],
        publication_base: Option<[u8; 32]>,
        control_runtime_id: ConnectorControlRuntimeId,
    ) -> Self {
        Self {
            definition_revision,
            interpretation_revision,
            publication_base,
            control_runtime_id,
        }
    }
}

#[derive(Clone, Debug)]
pub struct ManagementRequest {
    catalog: CatalogHandle,
    table: ConnectorTableIdentity,
    expected_object_id: Option<ConnectorTableObjectId>,
    operation: ConnectorDocumentManagementOperation,
    expected_dependencies: Option<ManagementDependencySet>,
    effect_scope: EffectScope,
    frozen_effect_identity: Option<EffectIdentity>,
    create_intent: Option<CreateIntent>,
}

impl ManagementRequest {
    pub fn try_new(
        catalog: CatalogHandle,
        table: ConnectorTableIdentity,
        expected_object_id: Option<ConnectorTableObjectId>,
        operation: ConnectorDocumentManagementOperation,
        expected_dependencies: Option<ManagementDependencySet>,
        effect_scope: EffectScope,
    ) -> Result<Self, ManagementAdmissionError> {
        if operation == ConnectorDocumentManagementOperation::Create
            || catalog.catalog_name() != &table.instance_id
            || expected_object_id.is_none()
            || expected_dependencies.is_none()
        {
            return Err(ManagementAdmissionError::InvalidRequest);
        }
        Ok(Self {
            catalog,
            table,
            expected_object_id,
            operation,
            expected_dependencies,
            effect_scope,
            frozen_effect_identity: None,
            create_intent: None,
        })
    }

    /// Construct the only admission request that can cross the first staged
    /// CREATE side effect. The caller owns the identity before any provider
    /// call; no physical object identity is guessed here.
    pub fn for_create_intent(intent: CreateIntent, effect_scope: EffectScope) -> Self {
        Self {
            catalog: intent.catalog().clone(),
            table: intent.table().clone(),
            expected_object_id: None,
            operation: ConnectorDocumentManagementOperation::Create,
            expected_dependencies: None,
            effect_scope,
            frozen_effect_identity: Some(intent.operation_id()),
            create_intent: Some(intent),
        }
    }

    /// One automatic-maintenance action is one management effect. Its exact
    /// current observation/dependencies and caller-frozen identity must be
    /// reacquired for every action; it cannot be reused as an attempt-wide
    /// composite lease.
    pub fn for_automatic_maintenance(effect: AutomaticMaintenanceEffect) -> Self {
        Self {
            catalog: effect.target.catalog().clone(),
            table: effect.target.table().clone(),
            expected_object_id: Some(effect.target.object_id().clone()),
            operation: ConnectorDocumentManagementOperation::SingleTargetUpdate,
            expected_dependencies: Some(effect.dependencies),
            effect_scope: effect.scope,
            frozen_effect_identity: Some(effect.operation_id),
            create_intent: None,
        }
    }

    pub const fn catalog(&self) -> &CatalogHandle {
        &self.catalog
    }

    pub const fn table(&self) -> &ConnectorTableIdentity {
        &self.table
    }

    pub const fn operation(&self) -> ConnectorDocumentManagementOperation {
        self.operation
    }
}

/// Typed exact input for one automatic durable action. The frontend adapter
/// supplies a newly observed target and dependencies for each action.
#[derive(Clone, Debug)]
pub struct AutomaticMaintenanceEffect {
    target: ManagedMvTarget,
    dependencies: ManagementDependencySet,
    operation_id: EffectIdentity,
    scope: EffectScope,
}

impl AutomaticMaintenanceEffect {
    pub const fn new(
        target: ManagedMvTarget,
        dependencies: ManagementDependencySet,
        operation_id: EffectIdentity,
        scope: EffectScope,
    ) -> Self {
        Self {
            target,
            dependencies,
            operation_id,
            scope,
        }
    }

    pub const fn target(&self) -> &ManagedMvTarget {
        &self.target
    }

    pub const fn operation_id(&self) -> EffectIdentity {
        self.operation_id
    }
}

/// What one target's management admission is doing in this process.
///
/// Only `Manageable` opens a new business write. Every other phase names what
/// is in the way, because "not ready" alone tells an operator nothing about
/// whether to wait, to observe, or to declare.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MvManagementPhase {
    /// Nothing in this process has observed the target yet.
    NotObserved,
    /// Admitted, idle, and open to a new business write.
    Manageable,
    /// A business write holds the target right now.
    Managing,
    /// An exact re-observation is owed before management reopens.
    AwaitingObservation,
    /// A committed effect must be re-observed before management reopens.
    AwaitingConvergence,
    /// One or more effects have an unknown outcome and block admission.
    AwaitingEffectSettlement { unsettled: usize },
    /// A CREATE was dispatched before its object identity existed and its
    /// response was lost.
    AwaitingCreateBinding,
    /// The entrance is stopping and admits nothing further.
    Stopping,
    /// A fresh Current observation found a different process incarnation.
    IncarnationMismatch,
    /// A fresh Current observation transferred this target to another deployment.
    OwnerMismatch,
}

impl MvManagementPhase {
    /// Whether a new business write can be admitted right now.
    pub const fn is_manageable(self) -> bool {
        matches!(self, Self::Manageable)
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NotObserved => "NOT_OBSERVED",
            Self::Manageable => "MANAGEABLE",
            Self::Managing => "MANAGING",
            Self::AwaitingObservation => "AWAITING_OBSERVATION",
            Self::AwaitingConvergence => "AWAITING_CONVERGENCE",
            Self::AwaitingEffectSettlement { .. } => "AWAITING_EFFECT_SETTLEMENT",
            Self::AwaitingCreateBinding => "AWAITING_CREATE_BINDING",
            Self::Stopping => "STOPPING",
            Self::IncarnationMismatch => "INCARNATION_MISMATCH",
            Self::OwnerMismatch => "OWNER_MISMATCH",
        }
    }
}

#[derive(Clone)]
pub struct ManagementEntrance {
    inner: Arc<EntranceInner>,
}

struct EntranceInner {
    owner: DeploymentOwner,
    incarnation: ProcessIncarnation,
    activity: MvActivityGate,
    state: Mutex<HashMap<ConnectorTableIdentity, TargetAdmissionState>>,
    unbound_create: Mutex<HashMap<ConnectorTableIdentity, CreateIntentResponsibility>>,
}

struct TargetAdmissionState {
    target: ManagedMvTarget,
    dependencies: ManagementDependencySet,
    unsettled: HashMap<super::EffectIdentity, UnsettledEffect>,
    pending_slot_changes:
        HashMap<super::EffectIdentity, novarocks_spi::connector::UnknownCommitEvidence>,
    latest_slot_change_conclusion: Option<novarocks_spi::connector::CommitConclusion>,
    pending_committed_effect: Option<super::EffectIdentity>,
    pending_continuation: Option<ManagementContinuation>,
    pending_observation: Option<ManagementObservationAuthorization>,
    installed_observation: Option<ManagementObservationLiveness>,
    ready: bool,
    incarnation_mismatch: bool,
    owner_mismatch: bool,
}

impl ManagementEntrance {
    pub fn new(owner: DeploymentOwner, incarnation: ProcessIncarnation) -> Self {
        Self {
            inner: Arc::new(EntranceInner {
                owner,
                incarnation,
                activity: MvActivityGate::new(),
                state: Mutex::new(HashMap::new()),
                unbound_create: Mutex::new(HashMap::new()),
            }),
        }
    }

    pub fn pending_slot_changes(&self, table: &ConnectorTableIdentity) -> usize {
        lock(&self.inner.state)
            .get(table)
            .map_or(0, |state| state.pending_slot_changes.len())
    }
    pub fn latest_slot_change_conclusion(
        &self,
        table: &ConnectorTableIdentity,
    ) -> Option<novarocks_spi::connector::CommitConclusion> {
        lock(&self.inner.state)
            .get(table)
            .and_then(|state| state.latest_slot_change_conclusion.clone())
    }
    /// Call after every fresh keyed observation. Provider I/O runs without the
    /// admission mutex. Only terminal conclusions retire pending evidence.
    pub fn resolve_pending_slot_changes(
        &self,
        lease: &novarocks_spi::connector::ConnectorDocumentStorageLease,
        request: novarocks_spi::connector::KeyedDocumentObservationRequest,
    ) -> Result<SlotChangeConvergence, novarocks_spi::connector::ConnectorError> {
        self.resolve_slot_changes(
            &request.catalog_handle,
            &request.target,
            &request.object_id,
            |evidence| lease.resolve_keyed_commit(request.clone(), evidence),
        )
    }
    pub(super) fn resolve_slot_changes(
        &self,
        catalog: &CatalogHandle,
        table: &ConnectorTableIdentity,
        object_id: &novarocks_spi::connector::ConnectorTableObjectId,
        mut resolve: impl FnMut(
            novarocks_spi::connector::UnknownCommitEvidence,
        ) -> Result<
            novarocks_spi::connector::CommitConclusion,
            novarocks_spi::connector::ConnectorError,
        >,
    ) -> Result<SlotChangeConvergence, novarocks_spi::connector::ConnectorError> {
        use novarocks_spi::connector::{CommitConclusion, ConnectorError, ConnectorErrorKind};
        let pending = {
            let states = lock(&self.inner.state);
            match states.get(table) {
                Some(state)
                    if state.target.catalog() == catalog
                        && state.target.object_id() == object_id =>
                {
                    state.pending_slot_changes.clone()
                }
                Some(_) => {
                    return Err(ConnectorError::new(
                        ConnectorErrorKind::InvalidRequest,
                        "pending slot change target was replaced",
                    ));
                }
                None => return Ok(SlotChangeConvergence::default()),
            }
        };
        let mut result = SlotChangeConvergence::default();
        for (effect, evidence) in pending {
            let conclusion = resolve(evidence.clone())?;
            if conclusion == CommitConclusion::StillUnknown {
                continue;
            }
            let mut states = lock(&self.inner.state);
            let Some(state) = states.get_mut(table) else {
                continue;
            };
            if state.target.catalog() != catalog
                || state.target.object_id() != object_id
                || state.pending_slot_changes.get(&effect) != Some(&evidence)
            {
                continue;
            }
            state.pending_slot_changes.remove(&effect);
            state.latest_slot_change_conclusion = Some(conclusion.clone());
            result.reinstall_current_projection |=
                matches!(conclusion, CommitConclusion::Committed(_));
            result.conclusions.push(conclusion);
        }
        Ok(result)
    }

    pub async fn resolve_pending_slot_changes_async(
        &self,
        lease: &novarocks_spi::connector::ConnectorDocumentStorageLease,
        request: novarocks_spi::connector::KeyedDocumentObservationRequest,
    ) -> Result<SlotChangeConvergence, novarocks_spi::connector::ConnectorError> {
        use novarocks_spi::connector::{CommitConclusion, ConnectorError, ConnectorErrorKind};
        let catalog = &request.catalog_handle;
        let table = &request.target;
        let object_id = &request.object_id;
        let pending = {
            let states = lock(&self.inner.state);
            match states.get(table) {
                Some(state)
                    if state.target.catalog() == catalog
                        && state.target.object_id() == object_id =>
                {
                    state.pending_slot_changes.clone()
                }
                Some(_) => {
                    return Err(ConnectorError::new(
                        ConnectorErrorKind::InvalidRequest,
                        "pending slot change target was replaced",
                    ));
                }
                None => return Ok(SlotChangeConvergence::default()),
            }
        };
        let mut result = SlotChangeConvergence::default();
        for (effect, evidence) in pending {
            let conclusion = lease
                .resolve_keyed_commit_async(request.clone(), evidence.clone())
                .await?;
            if conclusion == CommitConclusion::StillUnknown {
                continue;
            }
            let mut states = lock(&self.inner.state);
            let Some(state) = states.get_mut(table) else {
                continue;
            };
            if state.target.catalog() != catalog
                || state.target.object_id() != object_id
                || state.pending_slot_changes.get(&effect) != Some(&evidence)
            {
                continue;
            }
            state.pending_slot_changes.remove(&effect);
            state.latest_slot_change_conclusion = Some(conclusion.clone());
            result.reinstall_current_projection |=
                matches!(conclusion, CommitConclusion::Committed(_));
            result.conclusions.push(conclusion);
        }
        Ok(result)
    }

    /// Revoke from the exact actual Current management slot independently of
    /// publication or root decoding. The caller supplies the actual view from
    /// its exact document lease; this negative observation grants no capability.
    pub fn revoke_foreign_current_management(
        &self,
        view: &novarocks_spi::connector::KeyedDocumentView,
        catalog: &CatalogHandle,
    ) -> Result<bool, crate::persistence::documents::MvDocumentError> {
        use crate::persistence::{
            documents::MvDocumentError,
            keyed_documents::{decode_management_slot, management_slot_name},
        };
        let slot = view
            .slots
            .get(&management_slot_name()?)
            .ok_or_else(|| MvDocumentError::Contract("missing MV management slot".into()))?;
        let (owner, incarnation) = decode_management_slot(&slot.value)?;
        let mut states = lock(&self.inner.state);
        if let Some(current) = states.get_mut(&view.table)
            && current.target.catalog() == catalog
            && current.target.object_id() == &view.object_id
            && (owner != self.inner.owner || incarnation != self.inner.incarnation)
        {
            current.ready = false;
            current.owner_mismatch = owner != self.inner.owner;
            current.incarnation_mismatch = incarnation != self.inner.incarnation;
            if let Some(installed) = &current.installed_observation {
                installed.close();
            }
            return Ok(true);
        }
        Ok(false)
    }

    /// Observe only the strict current management/root facts needed to delete
    /// an already installed target. Missing current P never becomes an absent
    /// publication in a usable projection or a new readmission capability.
    pub fn observe_keyed_drop(
        &self,
        lease: &novarocks_spi::connector::ConnectorDocumentStorageLease,
        request: novarocks_spi::connector::KeyedDocumentObservationRequest,
        budget: crate::persistence::validation::PersistenceDecodeBudget,
    ) -> Result<
        (MvDropManagementObservation, SlotChangeConvergence),
        crate::persistence::documents::MvDocumentError,
    > {
        use crate::persistence::{
            documents::MvDocumentError, keyed_documents::decode_keyed_drop_documents,
        };
        let error = |message: &str| MvDocumentError::Contract(message.into());
        self.validate_keyed_drop_before_observation(lease, &request)?;
        let mut view = lease
            .observe_keyed_documents(request.clone())
            .map_err(MvDocumentError::KeyedStorage)?;
        // Revoke foreign exact-target authority before C/root decoding and
        // before consuming any pending slot evidence.
        if self.revoke_foreign_current_management(&view, &request.catalog_handle)? {
            return Err(error(
                "MV DROP Current management slot names another owner or incarnation",
            ));
        }
        let mut documents =
            decode_keyed_drop_documents(&view, request.catalog_handle.clone(), budget)?;
        let convergence = self.resolve_pending_slot_changes(lease, request.clone())?;
        if convergence.reinstall_current_projection {
            view = lease
                .observe_keyed_documents(request.clone())
                .map_err(MvDocumentError::KeyedStorage)?;
            if self.revoke_foreign_current_management(&view, &request.catalog_handle)? {
                return Err(error(
                    "MV DROP Current management slot names another owner or incarnation",
                ));
            }
            documents = decode_keyed_drop_documents(&view, request.catalog_handle.clone(), budget)?;
        }
        self.finish_keyed_drop_observation(lease, &request, documents, convergence)
    }

    /// Observe the same exact deletion facts using the original async leaves.
    /// The outer Source retains this future across borrowed waiter cancellation.
    pub async fn observe_keyed_drop_async(
        &self,
        lease: &novarocks_spi::connector::ConnectorDocumentStorageLease,
        request: novarocks_spi::connector::KeyedDocumentObservationRequest,
        budget: crate::persistence::validation::PersistenceDecodeBudget,
    ) -> Result<
        (MvDropManagementObservation, SlotChangeConvergence),
        crate::persistence::documents::MvDocumentError,
    > {
        use crate::persistence::{
            documents::MvDocumentError, keyed_documents::decode_keyed_drop_documents,
        };
        let error = |message: &str| MvDocumentError::Contract(message.into());
        self.validate_keyed_drop_before_observation(lease, &request)?;
        let mut view = lease
            .observe_keyed_documents_async(request.clone())
            .await
            .map_err(MvDocumentError::KeyedStorage)?;
        // Revoke foreign exact-target authority before C/root decoding and
        // before consuming any pending slot evidence.
        if self.revoke_foreign_current_management(&view, &request.catalog_handle)? {
            return Err(error(
                "MV DROP Current management slot names another owner or incarnation",
            ));
        }
        let mut documents =
            decode_keyed_drop_documents(&view, request.catalog_handle.clone(), budget)?;
        let convergence = self
            .resolve_pending_slot_changes_async(lease, request.clone())
            .await?;
        if convergence.reinstall_current_projection {
            view = lease
                .observe_keyed_documents_async(request.clone())
                .await
                .map_err(MvDocumentError::KeyedStorage)?;
            if self.revoke_foreign_current_management(&view, &request.catalog_handle)? {
                return Err(error(
                    "MV DROP Current management slot names another owner or incarnation",
                ));
            }
            documents = decode_keyed_drop_documents(&view, request.catalog_handle.clone(), budget)?;
        }
        self.finish_keyed_drop_observation(lease, &request, documents, convergence)
    }

    fn validate_keyed_drop_state(
        &self,
        current: &TargetAdmissionState,
        lease: &novarocks_spi::connector::ConnectorDocumentStorageLease,
        request: &novarocks_spi::connector::KeyedDocumentObservationRequest,
    ) -> Result<(), crate::persistence::documents::MvDocumentError> {
        use crate::persistence::documents::MvDocumentError;
        let error = |message: &str| MvDocumentError::Contract(message.into());
        if current.target.catalog() != &request.catalog_handle
            || current.target.table() != &request.target
            || current.target.object_id() != &request.object_id
            || current.dependencies.control_runtime_id != lease.control_runtime_id()
        {
            return Err(error(
                "MV DROP exact installed target or control runtime changed",
            ));
        }
        if !current.ready
            || current.owner_mismatch
            || current.incarnation_mismatch
            || !current.unsettled.is_empty()
            || current.pending_committed_effect.is_some()
            || current.pending_observation.is_some()
            || !current
                .installed_observation
                .as_ref()
                .is_some_and(ManagementObservationLiveness::is_open)
        {
            return Err(error(
                "MV DROP installed management admission is closed or unsettled",
            ));
        }
        Ok(())
    }

    fn validate_keyed_drop_before_observation(
        &self,
        lease: &novarocks_spi::connector::ConnectorDocumentStorageLease,
        request: &novarocks_spi::connector::KeyedDocumentObservationRequest,
    ) -> Result<(), crate::persistence::documents::MvDocumentError> {
        use crate::persistence::documents::MvDocumentError;
        let states = lock(&self.inner.state);
        let current = states.get(&request.target).ok_or_else(|| {
            MvDocumentError::Contract("MV DROP target has not been admitted".into())
        })?;
        self.validate_keyed_drop_state(current, lease, request)
    }

    fn finish_keyed_drop_observation(
        &self,
        lease: &novarocks_spi::connector::ConnectorDocumentStorageLease,
        request: &novarocks_spi::connector::KeyedDocumentObservationRequest,
        documents: crate::persistence::keyed_documents::MvKeyedDropDocuments,
        convergence: SlotChangeConvergence,
    ) -> Result<
        (MvDropManagementObservation, SlotChangeConvergence),
        crate::persistence::documents::MvDocumentError,
    > {
        use crate::persistence::documents::MvDocumentError;
        let error = |message: &str| MvDocumentError::Contract(message.into());
        let mut states = lock(&self.inner.state);
        let current = states
            .get_mut(&request.target)
            .ok_or_else(|| error("MV DROP target admission disappeared"))?;
        self.validate_keyed_drop_state(current, lease, request)?;
        if current.target != documents.target {
            return Err(error("MV DROP target changed during observation"));
        }
        if documents.owner != self.inner.owner || documents.incarnation != self.inner.incarnation {
            current.ready = false;
            current.owner_mismatch = documents.owner != self.inner.owner;
            current.incarnation_mismatch = documents.incarnation != self.inner.incarnation;
            if let Some(liveness) = &current.installed_observation {
                liveness.close();
            }
            return Err(error(
                "MV DROP Current management slot names another owner or incarnation",
            ));
        }
        if current.dependencies.definition_revision
            != *documents.versions.definition_revision().as_bytes()
            || current.dependencies.interpretation_revision
                != *documents.versions.interpretation_revision().as_bytes()
        {
            return Err(error(
                "MV DROP current root definition or interpretation changed",
            ));
        }
        // The publication component is the installed admission's revision gate,
        // not a claim that this externally modified current snapshot has P.
        // Do not update admission or publish a replacement projection here.
        Ok((
            MvDropManagementObservation {
                target: documents.target,
                dependencies: current.dependencies.clone(),
                root_binding: documents.versions.binding.identity(),
                configuration_revision: documents.configuration_revision,
            },
            convergence,
        ))
    }

    /// Refresh the capability of an already admitted target from a real,
    /// retained-lease observation. Pending slot evidence is reconciled here;
    /// no raw view supplied by a caller can mint this capability.
    pub fn observe_keyed_admission(
        &self,
        lease: &novarocks_spi::connector::ConnectorDocumentStorageLease,
        request: novarocks_spi::connector::KeyedDocumentObservationRequest,
        budget: crate::persistence::validation::PersistenceDecodeBudget,
    ) -> Result<
        (
            crate::persistence::keyed_documents::MvKeyedObservedCurrentDocuments,
            MvCurrentManagementAdmission,
            SlotChangeConvergence,
        ),
        crate::persistence::documents::MvDocumentError,
    > {
        use crate::persistence::{
            documents::MvDocumentError,
            keyed_documents::{
                decode_current_keyed_documents, observe_current_keyed_view_with_management,
            },
        };
        let control = lease.control_runtime_id();
        let catalog = request.catalog_handle.clone();
        let (view, convergence) = observe_current_keyed_view_with_management(lease, request, self)?;
        if self.revoke_foreign_current_management(&view, &catalog)? {
            return Err(MvDocumentError::Contract(
                "keyed observation Current management names another owner or incarnation".into(),
            ));
        }
        let documents = decode_current_keyed_documents(view, catalog, budget)?;
        let mut states = lock(&self.inner.state);
        let current = states.get_mut(&documents.view.table).ok_or_else(|| {
            MvDocumentError::Contract("keyed observation target has not been admitted".into())
        })?;
        if current.target != documents.management_target {
            return Err(MvDocumentError::Contract(
                "keyed observation cannot renew another target admission".into(),
            ));
        }
        if documents.deployment_owner != self.inner.owner
            || documents.process_incarnation != self.inner.incarnation
        {
            current.ready = false;
            current.owner_mismatch = documents.deployment_owner != self.inner.owner;
            current.incarnation_mismatch = documents.process_incarnation != self.inner.incarnation;
            if let Some(installed) = &current.installed_observation {
                installed.close();
            }
            return Err(MvDocumentError::Contract(
                "keyed observation Current management names another owner or incarnation".into(),
            ));
        }
        if !current.ready
            || !current.unsettled.is_empty()
            || current.pending_committed_effect.is_some()
            || current.pending_observation.is_some()
        {
            return Err(MvDocumentError::Contract(
                "keyed observation cannot renew this target admission".into(),
            ));
        }
        let liveness = current
            .installed_observation
            .as_ref()
            .filter(|live| live.is_open())
            .cloned()
            .ok_or_else(|| {
                MvDocumentError::Contract("keyed observation target admission is closed".into())
            })?;
        current.dependencies = documents.management_dependencies(control);
        let admission = MvCurrentManagementAdmission {
            target: documents.management_target.clone(),
            owner: documents.deployment_owner.clone(),
            incarnation: documents.process_incarnation.clone(),
            metadata_version: documents.view.metadata_version.clone(),
            liveness,
        };
        Ok((documents, admission, convergence))
    }

    pub async fn observe_keyed_admission_async(
        &self,
        lease: &novarocks_spi::connector::ConnectorDocumentStorageLease,
        request: novarocks_spi::connector::KeyedDocumentObservationRequest,
        budget: crate::persistence::validation::PersistenceDecodeBudget,
    ) -> Result<
        (
            crate::persistence::keyed_documents::MvKeyedObservedCurrentDocuments,
            MvCurrentManagementAdmission,
            SlotChangeConvergence,
        ),
        crate::persistence::documents::MvDocumentError,
    > {
        use crate::persistence::{
            documents::MvDocumentError,
            keyed_documents::{
                decode_current_keyed_documents, observe_current_keyed_view_with_management_async,
            },
        };
        let control = lease.control_runtime_id();
        let catalog = request.catalog_handle.clone();
        let (view, convergence) =
            observe_current_keyed_view_with_management_async(lease, request, self).await?;
        if self.revoke_foreign_current_management(&view, &catalog)? {
            return Err(MvDocumentError::Contract(
                "keyed observation Current management names another owner or incarnation".into(),
            ));
        }
        let documents = decode_current_keyed_documents(view, catalog, budget)?;
        let mut states = lock(&self.inner.state);
        let current = states.get_mut(&documents.view.table).ok_or_else(|| {
            MvDocumentError::Contract("keyed observation target has not been admitted".into())
        })?;
        if current.target != documents.management_target {
            return Err(MvDocumentError::Contract(
                "keyed observation cannot renew another target admission".into(),
            ));
        }
        if documents.deployment_owner != self.inner.owner
            || documents.process_incarnation != self.inner.incarnation
        {
            current.ready = false;
            current.owner_mismatch = documents.deployment_owner != self.inner.owner;
            current.incarnation_mismatch = documents.process_incarnation != self.inner.incarnation;
            if let Some(installed) = &current.installed_observation {
                installed.close();
            }
            return Err(MvDocumentError::Contract(
                "keyed observation Current management names another owner or incarnation".into(),
            ));
        }
        if !current.ready
            || !current.unsettled.is_empty()
            || current.pending_committed_effect.is_some()
            || current.pending_observation.is_some()
        {
            return Err(MvDocumentError::Contract(
                "keyed observation cannot renew this target admission".into(),
            ));
        }
        let liveness = current
            .installed_observation
            .as_ref()
            .filter(|live| live.is_open())
            .cloned()
            .ok_or_else(|| {
                MvDocumentError::Contract("keyed observation target admission is closed".into())
            })?;
        current.dependencies = documents.management_dependencies(control);
        let admission = MvCurrentManagementAdmission {
            target: documents.management_target.clone(),
            owner: documents.deployment_owner.clone(),
            incarnation: documents.process_incarnation.clone(),
            metadata_version: documents.view.metadata_version.clone(),
            liveness,
        };
        Ok((documents, admission, convergence))
    }

    pub fn owner(&self) -> &DeploymentOwner {
        &self.inner.owner
    }

    pub fn incarnation(&self) -> &ProcessIncarnation {
        &self.inner.incarnation
    }

    /// Installs only a state that completed effect closure, exact
    /// re-observation and owner/incarnation registration.
    pub fn install_observed_target(
        &self,
        observation: &ManagementObservationState,
        dependencies: ManagementDependencySet,
    ) -> Result<MvCurrentManagementAdmission, ManagementAdmissionError> {
        if observation.phase() != ManagementObservationPhase::Ready {
            return Err(ManagementAdmissionError::ReadmissionIncomplete);
        }
        if observation.local_owner() != &self.inner.owner
            || observation.local_incarnation() != &self.inner.incarnation
        {
            return Err(ManagementAdmissionError::ReadmissionIncomplete);
        }
        let mut state = lock(&self.inner.state);
        if let Some(current) = state.get(observation.target().table()) {
            if current.incarnation_mismatch || current.owner_mismatch {
                return Err(ManagementAdmissionError::ReadmissionIncomplete);
            }
            let current_effects = current
                .unsettled
                .keys()
                .copied()
                .collect::<std::collections::HashSet<_>>();
            if current.target != *observation.target()
                || current_effects != *observation.resolved_effects()
                || current.pending_committed_effect != observation.required_committed_effect()
                || current.pending_continuation.as_ref() != Some(observation.continuation())
                || !matching_authorization(
                    current.pending_observation.as_ref(),
                    observation.authorization(),
                )
            {
                return Err(ManagementAdmissionError::ReadmissionIncomplete);
            }
        } else if !observation.resolved_effects().is_empty()
            || observation.required_committed_effect().is_some()
            || observation.authorization().is_some()
        {
            return Err(ManagementAdmissionError::ReadmissionIncomplete);
        }
        let admission = MvCurrentManagementAdmission {
            target: observation.target().clone(),
            owner: observation.local_owner().clone(),
            incarnation: observation.local_incarnation().clone(),
            metadata_version: observation
                .latest_metadata_version()
                .cloned()
                .ok_or(ManagementAdmissionError::ReadmissionIncomplete)?,
            liveness: observation.liveness().clone(),
        };
        let (pending_slot_changes, latest_slot_change_conclusion) = state
            .get_mut(observation.target().table())
            .map(|current| {
                (
                    std::mem::take(&mut current.pending_slot_changes),
                    current.latest_slot_change_conclusion.clone(),
                )
            })
            .unwrap_or_default();
        state.insert(
            observation.target().table().clone(),
            TargetAdmissionState {
                target: observation.target().clone(),
                dependencies,
                unsettled: HashMap::new(),
                pending_slot_changes,
                latest_slot_change_conclusion,
                pending_committed_effect: None,
                pending_continuation: None,
                pending_observation: None,
                installed_observation: Some(observation.liveness().clone()),
                ready: true,
                incarnation_mismatch: false,
                owner_mismatch: false,
            },
        );
        Ok(admission)
    }

    /// Close normal writes before acquiring new current evidence. The returned
    /// state is bound to the exact unresolved effect set retained by this
    /// entrance; creating an empty state cannot bypass Unknown.
    pub fn begin_readmission(
        &self,
        table: &ConnectorTableIdentity,
        continuation: ManagementContinuation,
    ) -> Result<ManagementObservationState, ManagementObservationError> {
        let mut state = lock(&self.inner.state);
        let current = state
            .get_mut(table)
            .ok_or(ManagementObservationError::ObservationNotAllowed)?;
        if current.unsettled.is_empty() || current.pending_committed_effect.is_some() {
            return Err(ManagementObservationError::ObservationNotAllowed);
        }
        if current.pending_continuation.as_ref() != Some(&continuation) {
            return Err(ManagementObservationError::InvalidRecoveryBarrier);
        }
        if current.pending_observation.is_some() {
            return Err(ManagementObservationError::ObservationAlreadyPending);
        }
        current.ready = false;
        current.installed_observation = None;
        let authorization = ManagementObservationAuthorization::new();
        current.pending_observation = Some(authorization.clone());
        ManagementObservationState::try_new(
            current.target.clone(),
            self.inner.owner.clone(),
            self.inner.incarnation.clone(),
            continuation,
            current.unsettled.values().cloned().collect(),
            None,
            Some(authorization),
        )
    }

    /// Starts the mandatory exact re-observation after a successful external
    /// commit. The committed effect identity is retained as an unforgeable
    /// entrance-side convergence requirement until installation succeeds.
    pub fn begin_committed_convergence(
        &self,
        table: &ConnectorTableIdentity,
        continuation: ManagementContinuation,
    ) -> Result<ManagementObservationState, ManagementObservationError> {
        let mut state = lock(&self.inner.state);
        let current = state
            .get_mut(table)
            .ok_or(ManagementObservationError::ObservationNotAllowed)?;
        let committed_effect = current
            .pending_committed_effect
            .ok_or(ManagementObservationError::ObservationNotAllowed)?;
        if current.ready || !current.unsettled.is_empty() {
            return Err(ManagementObservationError::ObservationNotAllowed);
        }
        if current.pending_continuation.as_ref() != Some(&continuation) {
            return Err(ManagementObservationError::InvalidRecoveryBarrier);
        }
        if current.pending_observation.is_some() {
            return Err(ManagementObservationError::ObservationAlreadyPending);
        }
        let authorization = ManagementObservationAuthorization::new();
        current.installed_observation = None;
        current.pending_observation = Some(authorization.clone());
        ManagementObservationState::try_new(
            current.target.clone(),
            self.inner.owner.clone(),
            self.inner.incarnation.clone(),
            continuation,
            vec![],
            Some(committed_effect),
            Some(authorization),
        )
    }

    /// Establish the conservative barrier used after process reconstruction.
    /// No durable attempt ledger is loaded: the caller supplies one barrier
    /// covering every operation path that the isolated old incarnation could
    /// have dispatched, and normal management remains closed until it is
    /// resolved and followed by exact re-observation.
    pub fn begin_recovered_target(
        &self,
        target: ManagedMvTarget,
        continuation: ManagementContinuation,
        recovery_barrier: UnsettledEffect,
    ) -> Result<ManagementObservationState, ManagementObservationError> {
        if recovery_barrier.responsibility().target() != &target {
            return Err(ManagementObservationError::EffectTargetMismatch);
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
        if previous_incarnation == &self.inner.incarnation
            || recovery_barrier.responsibility().dispatching_incarnation() != previous_incarnation
            || recovery_barrier.responsibility().scope() != EffectScope::CATALOG_AND_OBJECT_DELETION
        {
            return Err(ManagementObservationError::InvalidRecoveryBarrier);
        }
        let mut state = lock(&self.inner.state);
        if state.contains_key(target.table()) {
            return Err(ManagementObservationError::ObservationAlreadyPending);
        }
        let authorization = ManagementObservationAuthorization::new();
        state.insert(
            target.table().clone(),
            TargetAdmissionState {
                target: target.clone(),
                dependencies: ManagementDependencySet::new(
                    [0; 32],
                    [0; 32],
                    None,
                    ConnectorControlRuntimeId::from_bytes([0; 16]),
                ),
                pending_slot_changes: HashMap::new(),
                latest_slot_change_conclusion: None,
                unsettled: HashMap::from([(
                    recovery_barrier.responsibility().identity(),
                    recovery_barrier.clone(),
                )]),
                pending_committed_effect: None,
                pending_continuation: Some(continuation.clone()),
                pending_observation: Some(authorization.clone()),
                installed_observation: None,
                ready: false,
                incarnation_mismatch: false,
                owner_mismatch: false,
            },
        );
        ManagementObservationState::try_new(
            target,
            self.inner.owner.clone(),
            self.inner.incarnation.clone(),
            continuation,
            vec![recovery_barrier],
            None,
            Some(authorization),
        )
    }

    /// Abandons one entrance-issued observation token without reopening the
    /// target. The unresolved or committed convergence requirement remains in
    /// place, so the caller may explicitly issue a replacement observation.
    pub fn abandon_observation(
        &self,
        observation: ManagementObservationState,
    ) -> Result<(), ManagementObservationError> {
        if observation.local_owner() != &self.inner.owner
            || observation.local_incarnation() != &self.inner.incarnation
        {
            return Err(ManagementObservationError::InvalidProvenance);
        }
        let authorization = observation
            .authorization()
            .ok_or(ManagementObservationError::InvalidProvenance)?;
        let mut state = lock(&self.inner.state);
        let current = state
            .get_mut(observation.target().table())
            .ok_or(ManagementObservationError::ObservationNotAllowed)?;
        if current.target != *observation.target()
            || !matching_authorization(current.pending_observation.as_ref(), Some(authorization))
        {
            return Err(ManagementObservationError::StaleObservation);
        }
        current.pending_observation = None;
        observation.liveness().close();
        Ok(())
    }

    /// Queue one normal mutation on the only management entrance.
    ///
    /// The caller supplies its real activity owner so foreground, scheduler,
    /// and maintenance work share one FIFO without being mislabeled as a
    /// manual refresh. Domain dependencies are checked only after the ticket
    /// reaches the head and owns the target activity lease.
    pub fn request(
        &self,
        request: ManagementRequest,
        owner: MvActivityOwner,
    ) -> Result<ManagementEntranceTicket, ManagementAdmissionError> {
        let activity_target = CanonicalMvTarget::from_parts(
            Some(request.table.instance_id.as_str()),
            &request.table.namespace,
            &request.table.table,
        );
        let activity = self
            .inner
            .activity
            .request(activity_target, owner)
            .map_err(ManagementAdmissionError::from)?;
        Ok(ManagementEntranceTicket {
            entrance: Arc::downgrade(&self.inner),
            request: Some(request),
            activity,
        })
    }

    /// Blocking convenience for foreground callers. Worker adapters should
    /// retain the ticket and use `try_acquire` so their event loop never
    /// blocks behind another owner.
    pub fn acquire(
        &self,
        request: ManagementRequest,
        cancelled: impl Fn() -> bool,
    ) -> Result<ManagementEntranceLease, ManagementAdmissionError> {
        let owner = activity_owner(request.operation);
        self.request(request, owner)?
            .acquire_waiting(cancelled)?
            .ok_or(ManagementAdmissionError::Cancelled)
    }

    /// Async admission on the same management/activity FIFO. The caller lends
    /// its original cancellation/deadline future; no synchronous wait occurs.
    pub async fn acquire_async(
        &self,
        request: ManagementRequest,
        cancelled: impl Future<Output = ()>,
    ) -> Result<ManagementEntranceLease, ManagementAdmissionError> {
        let owner = activity_owner(request.operation);
        self.request(request, owner)?
            .acquire_waiting_async(cancelled)
            .await?
            .ok_or(ManagementAdmissionError::Cancelled)
    }

    /// Acquire one automatic action through the same FIFO as every other MV
    /// management effect. The action carrier is intentionally consumed here so
    /// callers must construct a fresh exact observation and operation identity
    /// before attempting the next durable action.
    pub fn acquire_automatic_maintenance(
        &self,
        effect: AutomaticMaintenanceEffect,
        cancelled: impl Fn() -> bool,
    ) -> Result<ManagementEntranceLease, ManagementAdmissionError> {
        self.request(
            ManagementRequest::for_automatic_maintenance(effect),
            MvActivityOwner::AutomaticMaintenance,
        )?
        .acquire_waiting(cancelled)?
        .ok_or(ManagementAdmissionError::Cancelled)
    }

    /// The actual migrated automatic runner uses the same immutable effect
    /// and activity owner as the temporary legacy entry above.
    pub async fn acquire_automatic_maintenance_async(
        &self,
        effect: AutomaticMaintenanceEffect,
        cancelled: impl Future<Output = ()>,
    ) -> Result<ManagementEntranceLease, ManagementAdmissionError> {
        self.request(
            ManagementRequest::for_automatic_maintenance(effect),
            MvActivityOwner::AutomaticMaintenance,
        )?
        .acquire_waiting_async(cancelled)
        .await?
        .ok_or(ManagementAdmissionError::Cancelled)
    }

    pub fn begin_stopping(&self) {
        self.inner.activity.begin_stopping();
    }

    /// What this entrance can currently do with one target.
    ///
    /// The phases are deliberately separate rather than one ready flag: an
    /// operator has to distinguish "nothing here has observed this target"
    /// from "an effect's outcome is unknown", because those need different
    /// actions and only one of them is recoverable by declaration. This is a
    /// read: it admits nothing, settles nothing, and may be stale the moment
    /// it returns.
    pub fn management_phase(&self, table: &ConnectorTableIdentity) -> MvManagementPhase {
        let activity = self.activity_observation(table);
        if activity.stopping {
            return MvManagementPhase::Stopping;
        }
        if lock(&self.inner.unbound_create).contains_key(table) {
            return MvManagementPhase::AwaitingCreateBinding;
        }
        let state = lock(&self.inner.state);
        let Some(current) = state.get(table) else {
            return MvManagementPhase::NotObserved;
        };
        if current.owner_mismatch {
            return MvManagementPhase::OwnerMismatch;
        }
        if current.incarnation_mismatch {
            return MvManagementPhase::IncarnationMismatch;
        }
        if !current.unsettled.is_empty() {
            return MvManagementPhase::AwaitingEffectSettlement {
                unsettled: current.unsettled.len(),
            };
        }
        if current.pending_committed_effect.is_some() {
            return MvManagementPhase::AwaitingConvergence;
        }
        if current.pending_observation.is_some() {
            return MvManagementPhase::AwaitingObservation;
        }
        if !current.ready {
            return MvManagementPhase::AwaitingObservation;
        }
        if activity.active {
            return MvManagementPhase::Managing;
        }
        MvManagementPhase::Manageable
    }

    /// Close this process's admission when a provider Current observation of
    /// the exact installed target names another owner or incarnation. An already
    /// dispatched effect is not revoked; this only stops future admissions.
    /// Historical observations must never be passed to this method.
    pub fn close_on_keyed_current_management_mismatch(
        &self,
        lease: &novarocks_spi::connector::ConnectorDocumentStorageLease,
        request: novarocks_spi::connector::KeyedDocumentObservationRequest,
    ) -> Result<bool, crate::persistence::documents::MvDocumentError> {
        use crate::persistence::{
            documents::MvDocumentError,
            keyed_documents::{decode_management_slot, management_slot_name},
        };
        let catalog = request.catalog_handle.clone();
        let mut view = lease
            .observe_keyed_documents(request.clone())
            .map_err(MvDocumentError::KeyedStorage)?;
        // A foreign target observation cannot reconcile this target's pending
        // evidence. The exact target is checked again after provider I/O.
        {
            let states = lock(&self.inner.state);
            let Some(current) = states.get(&view.table) else {
                return Ok(false);
            };
            if current.target.catalog() != &catalog || current.target.object_id() != &view.object_id
            {
                return Ok(false);
            }
        }
        let convergence = self.resolve_pending_slot_changes(lease, request.clone())?;
        if convergence.reinstall_current_projection {
            view = lease
                .observe_keyed_documents(request)
                .map_err(MvDocumentError::KeyedStorage)?;
        }
        let slot = view
            .slots
            .get(&management_slot_name()?)
            .ok_or_else(|| MvDocumentError::Contract("missing MV management slot".into()))?;
        let (owner, incarnation) = decode_management_slot(&slot.value)?;
        let mut states = lock(&self.inner.state);
        let Some(current) = states.get_mut(&view.table) else {
            return Ok(false);
        };
        if current.target.catalog() != &catalog
            || current.target.object_id() != &view.object_id
            || (owner == self.inner.owner && incarnation == self.inner.incarnation)
        {
            return Ok(false);
        }
        current.ready = false;
        current.owner_mismatch = owner != self.inner.owner;
        current.incarnation_mismatch = incarnation != self.inner.incarnation;
        if let Some(installed) = &current.installed_observation {
            installed.close();
        }
        Ok(true)
    }

    pub async fn close_on_keyed_current_management_mismatch_async(
        &self,
        lease: &novarocks_spi::connector::ConnectorDocumentStorageLease,
        request: novarocks_spi::connector::KeyedDocumentObservationRequest,
    ) -> Result<bool, crate::persistence::documents::MvDocumentError> {
        use crate::persistence::{
            documents::MvDocumentError,
            keyed_documents::{decode_management_slot, management_slot_name},
        };
        let catalog = request.catalog_handle.clone();
        let mut view = lease
            .observe_keyed_documents_async(request.clone())
            .await
            .map_err(MvDocumentError::KeyedStorage)?;
        // A foreign target observation cannot reconcile this target's pending
        // evidence. The exact target is checked again after provider I/O.
        {
            let states = lock(&self.inner.state);
            let Some(current) = states.get(&view.table) else {
                return Ok(false);
            };
            if current.target.catalog() != &catalog || current.target.object_id() != &view.object_id
            {
                return Ok(false);
            }
        }
        let convergence = self
            .resolve_pending_slot_changes_async(lease, request.clone())
            .await?;
        if convergence.reinstall_current_projection {
            view = lease
                .observe_keyed_documents_async(request)
                .await
                .map_err(MvDocumentError::KeyedStorage)?;
        }
        let slot = view
            .slots
            .get(&management_slot_name()?)
            .ok_or_else(|| MvDocumentError::Contract("missing MV management slot".into()))?;
        let (owner, incarnation) = decode_management_slot(&slot.value)?;
        let mut states = lock(&self.inner.state);
        let Some(current) = states.get_mut(&view.table) else {
            return Ok(false);
        };
        if current.target.catalog() != &catalog
            || current.target.object_id() != &view.object_id
            || (owner == self.inner.owner && incarnation == self.inner.incarnation)
        {
            return Ok(false);
        }
        current.ready = false;
        current.owner_mismatch = owner != self.inner.owner;
        current.incarnation_mismatch = incarnation != self.inner.incarnation;
        if let Some(installed) = &current.installed_observation {
            installed.close();
        }
        Ok(true)
    }

    /// Where one target stands in this process's activity gate. Diagnostic
    /// only: it admits nothing and may be stale the moment it returns.
    pub fn activity_observation(&self, table: &ConnectorTableIdentity) -> MvActivityObservation {
        self.inner.activity.observe(&CanonicalMvTarget::from_parts(
            Some(table.instance_id.as_str()),
            &table.namespace,
            &table.table,
        ))
    }

    /// Snapshot unresolved responsibilities without removing their admission
    /// block. Only a completed readmission observation may replace the state.
    pub fn unsettled_effects(&self, table: &ConnectorTableIdentity) -> Vec<UnsettledEffect> {
        lock(&self.inner.state)
            .get(table)
            .map(|state| state.unsettled.values().cloned().collect())
            .unwrap_or_default()
    }

    /// Issue the only capability that can turn an unbound staged CREATE into
    /// an exact target. The caller must perform a fresh provider observation
    /// and complete this token; a raw object id cannot resolve a lost staged
    /// response.
    pub fn begin_unbound_create_observation(
        &self,
        intent: &CreateIntent,
    ) -> Result<PendingCreateIntentObservation, ManagementObservationError> {
        match lock(&self.inner.unbound_create).get(intent.table()) {
            Some(responsibility) if responsibility.intent() == intent => {
                Ok(PendingCreateIntentObservation::for_intent(intent.clone()))
            }
            _ => Err(ManagementObservationError::ObservationNotAllowed),
        }
    }

    /// Convert a fresh, matching observation of a previously unbound CREATE
    /// into the same exact Unknown responsibility used by normal readmission.
    /// The original operation id, scope, incarnation, and dispatch timestamp
    /// are retained; this does not invent a new create attempt.
    pub fn resolve_unbound_create_as_unknown(
        &self,
        observation: FreshCreateIntentObservation,
    ) -> Result<ManagedMvTarget, ManagementAdmissionError> {
        let (intent, target) = observation.into_parts();
        let responsibility = {
            let unbound = lock(&self.inner.unbound_create);
            match unbound.get(intent.table()) {
                Some(responsibility) if responsibility.intent() == &intent => {
                    responsibility.clone()
                }
                _ => return Err(ManagementAdmissionError::ReadmissionIncomplete),
            }
        };
        let exact = responsibility
            .clone()
            .late_bind(target.clone())
            .map_err(|_| ManagementAdmissionError::TargetReplaced)?;
        let removed = lock(&self.inner.unbound_create).remove(intent.table());
        if removed.as_ref() != Some(&responsibility) {
            return Err(ManagementAdmissionError::ReadmissionIncomplete);
        }
        let weak = Arc::downgrade(&self.inner);
        if let Err(error) = record_unsettled(&weak, exact) {
            lock(&self.inner.unbound_create).insert(intent.table().clone(), responsibility);
            return Err(error);
        }
        Ok(target)
    }
}

/// A queued management request. Dropping it before acquisition removes the
/// request from the shared target FIFO through the embedded activity ticket.
pub struct ManagementEntranceTicket {
    entrance: Weak<EntranceInner>,
    request: Option<ManagementRequest>,
    activity: MvActivityTicket,
}

impl ManagementEntranceTicket {
    /// Acquire only when this request is at the head of the target FIFO.
    pub fn try_acquire(
        &mut self,
    ) -> Result<Option<ManagementEntranceLease>, ManagementAdmissionError> {
        let Some(activity) = self
            .activity
            .try_acquire()
            .map_err(ManagementAdmissionError::from)?
        else {
            return Ok(None);
        };
        self.finish_acquire(activity).map(Some)
    }

    /// Wait for the target FIFO while honoring the caller-owned cancellation
    /// probe. Worker callers normally use `try_acquire` instead.
    pub fn acquire_waiting(
        &mut self,
        cancelled: impl Fn() -> bool,
    ) -> Result<Option<ManagementEntranceLease>, ManagementAdmissionError> {
        let Some(activity) = self
            .activity
            .acquire_waiting(cancelled)
            .map_err(ManagementAdmissionError::from)?
        else {
            return Ok(None);
        };
        self.finish_acquire(activity).map(Some)
    }

    /// Borrow the same queued ticket without moving its request. Cancelling
    /// only this borrow leaves ownership with the original ticket.
    pub async fn acquire_waiting_async(
        &mut self,
        cancelled: impl Future<Output = ()>,
    ) -> Result<Option<ManagementEntranceLease>, ManagementAdmissionError> {
        let Some(activity) = self
            .activity
            .acquire_waiting_async(cancelled)
            .await
            .map_err(ManagementAdmissionError::from)?
        else {
            return Ok(None);
        };
        self.finish_acquire(activity).map(Some)
    }

    fn finish_acquire(
        &mut self,
        activity: MvActivityLease,
    ) -> Result<ManagementEntranceLease, ManagementAdmissionError> {
        let entrance = self
            .entrance
            .upgrade()
            .ok_or(ManagementAdmissionError::EntranceDropped)?;
        let request = self
            .request
            .take()
            .ok_or(ManagementAdmissionError::InvalidRequest)?;
        validate_request_against_state(&entrance, &request)?;
        Ok(ManagementEntranceLease {
            entrance: Arc::downgrade(&entrance),
            request,
            activity: Some(activity),
            dispatched: None,
        })
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SlotChangeConvergence {
    pub reinstall_current_projection: bool,
    pub conclusions: Vec<novarocks_spi::connector::CommitConclusion>,
}

pub struct ManagementEntranceLease {
    entrance: Weak<EntranceInner>,
    request: ManagementRequest,
    activity: Option<MvActivityLease>,
    dispatched: Option<DispatchedEffect>,
}

enum DispatchedEffect {
    Exact(EffectResponsibility),
    CreateIntent(CreateIntentResponsibility),
}

impl ManagementEntranceLease {
    /// Worker-owned admissions receive a shutdown cancellation view from the
    /// shared activity gate. Foreground admissions deliberately return none;
    /// their statement WorkScope remains the cancellation owner.
    pub fn worker_cancellation(
        &self,
    ) -> Option<novarocks_query_application::cancellation::QueryCancellationView> {
        self.activity
            .as_ref()
            .and_then(MvActivityLease::cancellation)
    }

    /// Must be called immediately before crossing the provider side-effect
    /// boundary. Dropping after this point conservatively records Unknown.
    pub fn mark_dispatched(
        &mut self,
        responsibility: EffectResponsibility,
    ) -> Result<(), ManagementAdmissionError> {
        let entrance = self
            .entrance
            .upgrade()
            .ok_or(ManagementAdmissionError::EntranceDropped)?;
        if self.request.create_intent.is_some()
            || self.dispatched.is_some()
            || self.request.effect_scope != responsibility.scope()
            || responsibility.dispatching_incarnation() != &entrance.incarnation
            || self
                .request
                .frozen_effect_identity
                .is_some_and(|identity| identity != responsibility.identity())
        {
            return Err(ManagementAdmissionError::InvalidEffect);
        }
        if responsibility.target().catalog() != &self.request.catalog
            || responsibility.target().table() != &self.request.table
            || self
                .request
                .expected_object_id
                .as_ref()
                .is_some_and(|object_id| responsibility.target().object_id() != object_id)
        {
            return Err(ManagementAdmissionError::InvalidEffect);
        }
        let mut state = lock(&entrance.state);
        match self.request.operation {
            ConnectorDocumentManagementOperation::Create => {
                if state.contains_key(&self.request.table) {
                    return Err(ManagementAdmissionError::ReadmissionIncomplete);
                }
            }
            ConnectorDocumentManagementOperation::SingleTargetUpdate
            | ConnectorDocumentManagementOperation::Publication
            | ConnectorDocumentManagementOperation::Drop => {
                let current = state
                    .get_mut(&self.request.table)
                    .ok_or(ManagementAdmissionError::ReadmissionIncomplete)?;
                if current
                    .installed_observation
                    .as_ref()
                    .is_some_and(|observation| !observation.is_open())
                {
                    current.ready = false;
                    current.installed_observation = None;
                }
                if !current.ready
                    || !current.unsettled.is_empty()
                    || current.pending_committed_effect.is_some()
                    || current.pending_observation.is_some()
                {
                    return Err(ManagementAdmissionError::ReadmissionIncomplete);
                }
                if current.target.catalog() != &self.request.catalog
                    || self.request.expected_object_id.as_ref() != Some(current.target.object_id())
                {
                    return Err(ManagementAdmissionError::TargetReplaced);
                }
                if self.request.expected_dependencies.as_ref() != Some(&current.dependencies) {
                    return Err(ManagementAdmissionError::DependencyChanged);
                }
            }
        }
        self.dispatched = Some(DispatchedEffect::Exact(responsibility));
        Ok(())
    }

    /// Mark the single CREATE responsibility immediately before the first
    /// staged provider call, when no physical object identity exists yet.
    /// The lease remains responsible for the stage, final publish, abort, and
    /// any Unknown outcome; [`Self::late_bind_create_target`] only narrows the
    /// target after the provider supplies the exact object.
    pub fn mark_create_intent_dispatched(
        &mut self,
        last_possible_dispatch_at: super::ManagementTimestamp,
    ) -> Result<(), ManagementAdmissionError> {
        let entrance = self
            .entrance
            .upgrade()
            .ok_or(ManagementAdmissionError::EntranceDropped)?;
        let intent = self
            .request
            .create_intent
            .clone()
            .ok_or(ManagementAdmissionError::InvalidEffect)?;
        if self.dispatched.is_some()
            || self.request.frozen_effect_identity != Some(intent.operation_id())
        {
            return Err(ManagementAdmissionError::InvalidEffect);
        }
        let responsibility = CreateIntentResponsibility::new(
            intent.clone(),
            entrance.incarnation.clone(),
            self.request.effect_scope,
            last_possible_dispatch_at,
        );
        let mut unbound = lock(&entrance.unbound_create);
        if unbound.contains_key(intent.table())
            || lock(&entrance.state).contains_key(intent.table())
        {
            return Err(ManagementAdmissionError::ReadmissionIncomplete);
        }
        unbound.insert(intent.table().clone(), responsibility.clone());
        self.dispatched = Some(DispatchedEffect::CreateIntent(responsibility));
        Ok(())
    }

    /// Bind the provider-returned exact target without changing the already
    /// frozen effect identity, scope, incarnation, or dispatch timestamp.
    pub fn late_bind_create_target(
        &mut self,
        target: ManagedMvTarget,
    ) -> Result<(), ManagementAdmissionError> {
        let entrance = self
            .entrance
            .upgrade()
            .ok_or(ManagementAdmissionError::EntranceDropped)?;
        let Some(DispatchedEffect::CreateIntent(intent_responsibility)) = self.dispatched.take()
        else {
            return Err(ManagementAdmissionError::EffectNotDispatched);
        };
        let exact = intent_responsibility
            .clone()
            .late_bind(target)
            .map_err(|_| ManagementAdmissionError::TargetReplaced)?;
        let mut unbound = lock(&entrance.unbound_create);
        match unbound.get(intent_responsibility.intent().table()) {
            Some(current) if current == &intent_responsibility => {
                unbound.remove(intent_responsibility.intent().table());
            }
            _ => return Err(ManagementAdmissionError::ReadmissionIncomplete),
        }
        self.dispatched = Some(DispatchedEffect::Exact(exact));
        Ok(())
    }

    /// A lost slot-change result retains its provider evidence without closing
    /// admission. Publication/other unknowns keep using record_terminal.
    pub fn record_slot_change_unknown(
        mut self,
        evidence: novarocks_spi::connector::UnknownCommitEvidence,
    ) -> Result<(), ManagementAdmissionError> {
        use novarocks_spi::connector::UnknownCommitIdentity;
        if self.request.operation != ConnectorDocumentManagementOperation::SingleTargetUpdate
            || self.request.effect_scope != EffectScope::CATALOG_COMMIT
            || !matches!(evidence.identity, UnknownCommitIdentity::SlotWriter { .. })
            || evidence.preselected_snapshot <= 0
        {
            return Err(ManagementAdmissionError::InvalidEffect);
        }
        let Some(DispatchedEffect::Exact(responsibility)) = self.dispatched.as_ref() else {
            return Err(ManagementAdmissionError::EffectNotDispatched);
        };
        let entrance = self
            .entrance
            .upgrade()
            .ok_or(ManagementAdmissionError::EntranceDropped)?;
        let mut states = lock(&entrance.state);
        let current = states
            .get_mut(&self.request.table)
            .ok_or(ManagementAdmissionError::ReadmissionIncomplete)?;
        if current.target != *responsibility.target()
            || !current.ready
            || !current.unsettled.is_empty()
            || current.pending_committed_effect.is_some()
        {
            return Err(ManagementAdmissionError::InvalidEffect);
        }
        current
            .pending_slot_changes
            .insert(responsibility.identity(), evidence);
        self.dispatched.take();
        self.activity.take();
        Ok(())
    }

    /// Close one publication that either stayed local or crossed the provider
    /// boundary. A local refusal releases only its activity turn: no external
    /// effect was created and no provider terminal may be invented for it.
    /// All dispatched outcomes retain the ordinary effect settlement rules.
    pub fn record_publication_terminal(
        mut self,
        disposition: EffectDisposition,
    ) -> Result<(), ManagementAdmissionError> {
        if self.request.operation != ConnectorDocumentManagementOperation::Publication {
            return Err(ManagementAdmissionError::InvalidEffect);
        }
        if self.dispatched.is_none() {
            if disposition != EffectDisposition::KnownUncommitted {
                return Err(ManagementAdmissionError::EffectNotDispatched);
            }
            self.activity.take();
            return Ok(());
        }
        self.record_terminal(disposition)
    }

    pub fn record_terminal(
        mut self,
        disposition: EffectDisposition,
    ) -> Result<(), ManagementAdmissionError> {
        self.record_terminal_in_place(disposition)
    }

    /// Settle through the original stable owner without moving its only lease
    /// into a callback frame. The caller must never replay an unproven call.
    pub fn record_terminal_in_place(
        &mut self,
        disposition: EffectDisposition,
    ) -> Result<(), ManagementAdmissionError> {
        if self.request.operation == ConnectorDocumentManagementOperation::Drop {
            return Err(ManagementAdmissionError::InvalidEffect);
        }
        let dispatched = self
            .dispatched
            .take()
            .ok_or(ManagementAdmissionError::EffectNotDispatched)?;
        let responsibility = match dispatched {
            DispatchedEffect::Exact(responsibility) => responsibility,
            DispatchedEffect::CreateIntent(responsibility) => {
                if disposition == EffectDisposition::KnownUncommitted {
                    let entrance = self
                        .entrance
                        .upgrade()
                        .ok_or(ManagementAdmissionError::EntranceDropped)?;
                    let removed =
                        lock(&entrance.unbound_create).remove(responsibility.intent().table());
                    if removed.as_ref() != Some(&responsibility) {
                        return Err(ManagementAdmissionError::ReadmissionIncomplete);
                    }
                    self.activity.take();
                    return Ok(());
                }
                // The physical target is still unknown. Preserve the
                // unbound responsibility on Drop and require an exact
                // provider observation before any terminal other than abort.
                self.dispatched = Some(DispatchedEffect::CreateIntent(responsibility));
                return Err(ManagementAdmissionError::CreateTargetNotBound);
            }
        };
        match disposition {
            EffectDisposition::KnownCommitted => {
                record_committed(&self.entrance, responsibility)?;
            }
            EffectDisposition::KnownUncommitted => {}
            EffectDisposition::CommitUnknown => {
                record_unsettled(&self.entrance, responsibility)?;
            }
        }
        self.activity.take();
        Ok(())
    }

    /// Settle one exact DROP while still holding its target's activity turn.
    /// A committed DROP retires the old management target rather than asking
    /// for an observation of an object that no longer exists.
    pub fn record_drop_terminal(
        mut self,
        disposition: EffectDisposition,
    ) -> Result<(), ManagementAdmissionError> {
        if self.request.operation != ConnectorDocumentManagementOperation::Drop
            || self.activity.is_none()
        {
            return Err(ManagementAdmissionError::InvalidEffect);
        }
        let Some(DispatchedEffect::Exact(responsibility)) = self.dispatched.as_ref() else {
            return Err(ManagementAdmissionError::EffectNotDispatched);
        };
        if responsibility.target().catalog() != &self.request.catalog
            || responsibility.target().table() != &self.request.table
            || self.request.expected_object_id.as_ref() != Some(responsibility.target().object_id())
        {
            return Err(ManagementAdmissionError::InvalidEffect);
        }
        match disposition {
            EffectDisposition::KnownCommitted => {
                retire_committed_drop(&self.entrance, &self.request, responsibility)?;
            }
            EffectDisposition::KnownUncommitted => {}
            EffectDisposition::CommitUnknown => {
                record_unsettled(&self.entrance, responsibility.clone())?;
            }
        }
        self.dispatched.take();
        self.activity.take();
        Ok(())
    }
}

fn retire_committed_drop(
    entrance: &Weak<EntranceInner>,
    request: &ManagementRequest,
    responsibility: &EffectResponsibility,
) -> Result<(), ManagementAdmissionError> {
    let entrance = entrance
        .upgrade()
        .ok_or(ManagementAdmissionError::EntranceDropped)?;
    let mut state = lock(&entrance.state);
    let current = state
        .get(&request.table)
        .ok_or(ManagementAdmissionError::ReadmissionIncomplete)?;
    if current.target != *responsibility.target()
        || request.expected_dependencies.as_ref() != Some(&current.dependencies)
        || !current.ready
        || !current.unsettled.is_empty()
        || current.pending_committed_effect.is_some()
        || current.pending_observation.is_some()
        || !current
            .installed_observation
            .as_ref()
            .is_some_and(ManagementObservationLiveness::is_open)
    {
        return Err(ManagementAdmissionError::ReadmissionIncomplete);
    }
    if let Some(observation) = &current.installed_observation {
        observation.close();
    }
    state.remove(&request.table);
    Ok(())
}

fn record_committed(
    entrance: &Weak<EntranceInner>,
    responsibility: EffectResponsibility,
) -> Result<(), ManagementAdmissionError> {
    let entrance = entrance
        .upgrade()
        .ok_or(ManagementAdmissionError::EntranceDropped)?;
    let table = responsibility.target().table().clone();
    let mut state = lock(&entrance.state);
    let target = state.entry(table).or_insert_with(|| TargetAdmissionState {
        target: responsibility.target().clone(),
        dependencies: ManagementDependencySet::new(
            [0; 32],
            [0; 32],
            None,
            ConnectorControlRuntimeId::from_bytes([0; 16]),
        ),
        unsettled: HashMap::new(),
        pending_slot_changes: HashMap::new(),
        latest_slot_change_conclusion: None,
        pending_committed_effect: None,
        pending_continuation: None,
        pending_observation: None,
        installed_observation: None,
        ready: false,
        incarnation_mismatch: false,
        owner_mismatch: false,
    });
    if target.target != *responsibility.target()
        || !target.unsettled.is_empty()
        || target.pending_committed_effect.is_some()
        || target.pending_observation.is_some()
    {
        return Err(ManagementAdmissionError::InvalidEffect);
    }
    target.ready = false;
    target.pending_committed_effect = Some(responsibility.identity());
    target.pending_continuation = Some(ManagementContinuation::SameOwner {
        previous_incarnation: responsibility.dispatching_incarnation().clone(),
    });
    target.pending_observation = None;
    target.installed_observation = None;
    Ok(())
}

impl Drop for ManagementEntranceLease {
    fn drop(&mut self) {
        if let Some(dispatched) = self.dispatched.take() {
            match dispatched {
                DispatchedEffect::Exact(responsibility) => {
                    let _ = record_unsettled(&self.entrance, responsibility);
                }
                DispatchedEffect::CreateIntent(_) => {
                    // The unbound intent was registered before the provider
                    // call. Leaving it in place is the conservative Unknown
                    // barrier until an exact target can be observed and bound.
                }
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ManagementAdmissionError {
    InvalidRequest,
    Stopping,
    Cancelled,
    TargetAlreadyExists,
    TargetReplaced,
    DependencyChanged,
    EffectUnsettled,
    ReadmissionIncomplete,
    InvalidEffect,
    EffectNotDispatched,
    CreateTargetNotBound,
    EntranceDropped,
}

impl fmt::Display for ManagementAdmissionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let message = match self {
            Self::InvalidRequest => "invalid MV management request",
            Self::Stopping => "MV management admission is stopping",
            Self::Cancelled => "MV management admission wait was cancelled",
            Self::TargetAlreadyExists => "MV target already has an observed object",
            Self::TargetReplaced => "MV target generation changed",
            Self::DependencyChanged => "MV dependencies changed during preparation",
            Self::EffectUnsettled => "a prior external effect may still change this MV",
            Self::ReadmissionIncomplete => "MV management readmission is incomplete",
            Self::InvalidEffect => "external effect does not match its management admission",
            Self::EffectNotDispatched => "external effect was not marked dispatched",
            Self::CreateTargetNotBound => "staged create has no exact provider target to settle",
            Self::EntranceDropped => "MV management entrance no longer exists",
        };
        formatter.write_str(message)
    }
}

impl std::error::Error for ManagementAdmissionError {}

impl From<MvActivityAdmissionError> for ManagementAdmissionError {
    fn from(value: MvActivityAdmissionError) -> Self {
        match value {
            MvActivityAdmissionError::Stopping => Self::Stopping,
            MvActivityAdmissionError::Cancelled => Self::Cancelled,
        }
    }
}

impl From<MvActivityGateError> for ManagementAdmissionError {
    fn from(value: MvActivityGateError) -> Self {
        match value {
            MvActivityGateError::Stopping => Self::Stopping,
        }
    }
}

fn validate_request_against_state(
    entrance: &EntranceInner,
    request: &ManagementRequest,
) -> Result<(), ManagementAdmissionError> {
    if lock(&entrance.unbound_create).contains_key(&request.table) {
        return Err(ManagementAdmissionError::EffectUnsettled);
    }
    let mut state = lock(&entrance.state);
    match request.operation {
        ConnectorDocumentManagementOperation::Create => {
            if let Some(current) = state.get(&request.table) {
                return Err(if current.unsettled.is_empty() {
                    ManagementAdmissionError::TargetAlreadyExists
                } else {
                    ManagementAdmissionError::EffectUnsettled
                });
            }
        }
        ConnectorDocumentManagementOperation::SingleTargetUpdate
        | ConnectorDocumentManagementOperation::Publication
        | ConnectorDocumentManagementOperation::Drop => {
            let current = state
                .get_mut(&request.table)
                .ok_or(ManagementAdmissionError::ReadmissionIncomplete)?;
            if current
                .installed_observation
                .as_ref()
                .is_some_and(|observation| !observation.is_open())
            {
                current.ready = false;
                current.installed_observation = None;
            }
            if !current.ready {
                return Err(if current.unsettled.is_empty() {
                    ManagementAdmissionError::ReadmissionIncomplete
                } else {
                    ManagementAdmissionError::EffectUnsettled
                });
            }
            if current.target.catalog() != &request.catalog
                || request.expected_object_id.as_ref() != Some(current.target.object_id())
            {
                return Err(ManagementAdmissionError::TargetReplaced);
            }
            if request.expected_dependencies.as_ref() != Some(&current.dependencies) {
                return Err(ManagementAdmissionError::DependencyChanged);
            }
            if !current.unsettled.is_empty() {
                return Err(ManagementAdmissionError::EffectUnsettled);
            }
        }
    }
    Ok(())
}

fn activity_owner(operation: ConnectorDocumentManagementOperation) -> MvActivityOwner {
    match operation {
        ConnectorDocumentManagementOperation::Create => MvActivityOwner::Create,
        ConnectorDocumentManagementOperation::SingleTargetUpdate => MvActivityOwner::Alter,
        ConnectorDocumentManagementOperation::Publication => MvActivityOwner::ManualRefresh,
        ConnectorDocumentManagementOperation::Drop => MvActivityOwner::Drop,
    }
}

fn record_unsettled(
    entrance: &Weak<EntranceInner>,
    responsibility: EffectResponsibility,
) -> Result<(), ManagementAdmissionError> {
    let entrance = entrance
        .upgrade()
        .ok_or(ManagementAdmissionError::EntranceDropped)?;
    let table = responsibility.target().table().clone();
    let unknown = match responsibility
        .clone()
        .record_terminal(EffectDisposition::CommitUnknown)
    {
        super::EffectTerminalFact::CommitUnknown(unknown) => unknown,
        _ => unreachable!("explicit CommitUnknown always yields an unsettled effect"),
    };
    let mut state = lock(&entrance.state);
    let target = state.entry(table).or_insert_with(|| TargetAdmissionState {
        target: responsibility.target().clone(),
        dependencies: ManagementDependencySet::new(
            [0; 32],
            [0; 32],
            None,
            ConnectorControlRuntimeId::from_bytes([0; 16]),
        ),
        unsettled: HashMap::new(),
        pending_slot_changes: HashMap::new(),
        latest_slot_change_conclusion: None,
        pending_committed_effect: None,
        pending_continuation: None,
        pending_observation: None,
        installed_observation: None,
        ready: false,
        incarnation_mismatch: false,
        owner_mismatch: false,
    });
    if target.target != *responsibility.target()
        || target.pending_committed_effect.is_some()
        || target.pending_observation.is_some()
    {
        return Err(ManagementAdmissionError::InvalidEffect);
    }
    target.ready = false;
    target.pending_committed_effect = None;
    target.pending_continuation = Some(ManagementContinuation::SameOwner {
        previous_incarnation: responsibility.dispatching_incarnation().clone(),
    });
    target.pending_observation = None;
    target.installed_observation = None;
    target
        .unsettled
        .entry(responsibility.identity())
        .or_insert(unknown);
    Ok(())
}

fn matching_authorization(
    expected: Option<&ManagementObservationAuthorization>,
    actual: Option<&ManagementObservationAuthorization>,
) -> bool {
    match (expected, actual) {
        (Some(expected), Some(actual)) => expected.matches(actual),
        (None, None) => true,
        _ => false,
    }
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}
