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

//! The frontend-only external write session.
//!
//! One `begin_write` atomically returns a frontend-only commit handle and the
//! complete set of logical writer handles the sealed plan may use. There is no
//! separate prepare, activate, or placement-dependent planning step: a writer
//! recipe is a property of the logical target, not of where the plan happens to
//! run.
//!
//! Only the frontend, holding its exact control generation, may finish, abort,
//! or reconcile a write. A backend has no commit handle and no catalog
//! mutation capability at all.

use std::sync::Arc;

use arrow::datatypes::SchemaRef;

use crate::connector::distributed_rewrite::ConnectorDistributedRewriteShape;
use crate::connector::handle::ConnectorPinnedFileSet;
use crate::connector::row_mutation::{
    ConnectorMutationMatchContract, ConnectorMutationSelectionFieldRole,
    ConnectorRowMutationScanBinding, ConnectorRowMutationSelection,
    ConnectorRowMutationSelectionOrdinal, selection_effect,
};
use crate::connector::write_stack::prepared::ConnectorPreparedWriteSet;
use crate::connector::write_stack::runtime::{
    ConnectorWriteBinding, ConnectorWriteCommitHandle, ConnectorWriterHandle,
};
use crate::connector::write_stack::target::{WriteTargetOrdinal, validate_dense_target_ordinals};
use crate::connector::{
    ConnectorDocumentPublicationDeclaration, ConnectorRowMutationEffect, ConnectorTableHandle,
    ConnectorWriteAbortOutcome, ConnectorWriteAdmissionPurpose, ConnectorWriteBaseVersion,
    ConnectorWriteInputRequest, ConnectorWriteInputShape, ConnectorWriteIntent,
    ConnectorWriteReceipt, ConnectorWriteRouteId, ConnectorWriteTargetRef,
};
use crate::connector::{
    ConnectorError, ConnectorErrorKind, ConnectorProviderBindingKey, ConnectorRequestContext,
};
use crate::connector::{ConnectorMutationRouteInput, ConnectorWriteFieldToken};
use crate::connector::{
    ExternalMutationEvidence, ExternalMutationOutcome, MAX_CONNECTOR_STATISTICS_ARTIFACTS,
    StatisticsArtifactDraft, StatisticsRequiredAggregation,
};

/// An actual KnownCommitted receipt after its necessary finalization ended.
/// Safe cleanup has its own original continuation and is absent from this fact.
#[derive(Debug, Eq, PartialEq)]
pub struct ConnectorWriteNeededReady {
    effect: crate::connector::ExternalMutationEffect,
    receipt: ConnectorWriteReceipt,
    finalization: crate::connector::ExternalMutationFinalization,
}
impl ConnectorWriteNeededReady {
    /// Seal the actual provider verdict after its necessary finalization.
    pub fn new(
        effect: crate::connector::ExternalMutationEffect,
        receipt: ConnectorWriteReceipt,
        finalization: crate::connector::ExternalMutationFinalization,
    ) -> Self {
        Self {
            effect,
            receipt,
            finalization,
        }
    }
    /// Only a unique, not-yet-published fact can record a local settle failure.
    pub fn finalization_mut(&mut self) -> &mut crate::connector::ExternalMutationFinalization {
        &mut self.finalization
    }
    pub const fn effect(&self) -> crate::connector::ExternalMutationEffect {
        self.effect
    }
    pub const fn receipt(&self) -> &ConnectorWriteReceipt {
        &self.receipt
    }
    pub const fn finalization(&self) -> &crate::connector::ExternalMutationFinalization {
        &self.finalization
    }
}
/// Pending is an actual retained continuation, never an invented cleanup
/// diagnostic. Only the original cleanup author records Observed.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConnectorWriteCleanupState {
    Pending,
    Observed(crate::connector::ExternalMutationFinalization),
}
/// The canonical committed business fact and its separate actual cleanup.
/// The original Future, receiving and caller share ONE needed-ready Arc.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConnectorWritePublicationCompletion {
    KnownCommitted {
        needed: Arc<ConnectorWriteNeededReady>,
        cleanup: ConnectorWriteCleanupState,
    },
    KnownUncommitted {
        failure: crate::connector::ConnectorMutationFailure,
        cleanup: crate::connector::ExternalMutationFinalization,
    },
    CommitUnknown {
        failure: crate::connector::ConnectorMutationFailure,
        evidence: ExternalMutationEvidence,
    },
}
impl ConnectorWritePublicationCompletion {
    pub fn from_committed_frontier(
        effect: crate::connector::ExternalMutationEffect,
        receipt: ConnectorWriteReceipt,
        finalization: crate::connector::ExternalMutationFinalization,
    ) -> Self {
        Self::KnownCommitted {
            needed: Arc::new(ConnectorWriteNeededReady::new(
                effect,
                receipt,
                finalization,
            )),
            cleanup: ConnectorWriteCleanupState::Pending,
        }
    }
    /// Move the original fact. No receipt, partition Vec or failure is cloned.
    pub fn from_outcome(outcome: ExternalMutationOutcome<ConnectorWriteReceipt>) -> Self {
        match outcome {
            ExternalMutationOutcome::KnownCommitted {
                effect,
                receipt,
                finalization,
                cleanup,
            } => Self::KnownCommitted {
                needed: Arc::new(ConnectorWriteNeededReady::new(
                    effect,
                    receipt,
                    finalization,
                )),
                cleanup: ConnectorWriteCleanupState::Observed(cleanup),
            },
            ExternalMutationOutcome::KnownUncommitted { failure, cleanup } => {
                Self::KnownUncommitted { failure, cleanup }
            }
            ExternalMutationOutcome::CommitUnknown { failure, evidence } => {
                Self::CommitUnknown { failure, evidence }
            }
        }
    }
    /// Explicit legacy projection AFTER the original Future fully drained.
    /// Governed publication Calls consume the shared completion directly and
    /// never call this value-cloning migration entrance.
    pub fn into_legacy_outcome(
        self,
    ) -> Result<ExternalMutationOutcome<ConnectorWriteReceipt>, Self> {
        match self {
            Self::KnownCommitted {
                needed,
                cleanup: ConnectorWriteCleanupState::Observed(cleanup),
            } => {
                let ConnectorWriteNeededReady {
                    effect,
                    receipt,
                    finalization,
                } = match Arc::try_unwrap(needed) {
                    Ok(original) => original,
                    Err(shared) => ConnectorWriteNeededReady {
                        effect: shared.effect,
                        receipt: shared.receipt.clone(),
                        finalization: shared.finalization.clone(),
                    },
                };
                Ok(ExternalMutationOutcome::KnownCommitted {
                    effect,
                    receipt,
                    finalization,
                    cleanup,
                })
            }
            pending @ Self::KnownCommitted {
                cleanup: ConnectorWriteCleanupState::Pending,
                ..
            } => Err(pending),
            Self::KnownUncommitted { failure, cleanup } => {
                Ok(ExternalMutationOutcome::KnownUncommitted { failure, cleanup })
            }
            Self::CommitUnknown { failure, evidence } => {
                Ok(ExternalMutationOutcome::CommitUnknown { failure, evidence })
            }
        }
    }
}
#[derive(Default)]
struct NeededReadyState {
    ready: Option<Arc<ConnectorWriteNeededReady>>,
    cleanup_permitted: bool,
    waiter: Option<std::task::Waker>,
}
/// Original receiving storage. It grants no publication, operation or owner.
#[derive(Default)]
pub struct ConnectorWriteNeededReadyReceiving {
    state: std::sync::Mutex<NeededReadyState>,
}
pub struct ConnectorWriteNeededHandoffNotice {
    waiter: Option<std::task::Waker>,
}
impl ConnectorWriteNeededHandoffNotice {
    pub fn notify(self) {
        if let Some(waiter) = self.waiter {
            waiter.wake();
        }
    }
}
impl ConnectorWriteNeededReadyReceiving {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }
    pub fn publish(&self, ready: Arc<ConnectorWriteNeededReady>) -> Result<(), ConnectorError> {
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        if state.ready.is_some() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "Write needed-ready fact was already published",
            ));
        }
        state.ready = Some(ready);
        Ok(())
    }
    /// Read-only observation is valid even after a poll/Drop failure. It does
    /// not authorize cleanup, another dispatch, or source retirement.
    pub fn observed(&self) -> Option<Arc<ConnectorWriteNeededReady>> {
        self.state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .ready
            .clone()
    }
    pub fn cleanup_is_permitted(&self) -> bool {
        self.state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .cleanup_permitted
    }
    /// Install the original typed fact before silently allowing the SAME
    /// Provider Future to enter cleanup. Notify only outside every owner lock.
    pub fn handoff_in(
        &self,
        target: &mut Option<Arc<ConnectorWriteNeededReady>>,
    ) -> Result<ConnectorWriteNeededHandoffNotice, ConnectorError> {
        if target.is_some() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "Write needed-ready receiving target is occupied",
            ));
        }
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        if state.cleanup_permitted {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "Write needed-ready fact was already handed off",
            ));
        }
        let ready = state
            .ready
            .as_ref()
            .ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "Write needed-ready fact is not published",
                )
            })?
            .clone();
        *target = Some(ready);
        state.cleanup_permitted = true;
        Ok(ConnectorWriteNeededHandoffNotice {
            waiter: state.waiter.take(),
        })
    }
    pub fn poll_cleanup_permitted(&self, cx: &mut std::task::Context<'_>) -> std::task::Poll<()> {
        let registered = cx.waker().clone();
        let retired = {
            let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
            if state.cleanup_permitted {
                return std::task::Poll::Ready(());
            }
            state.waiter.replace(registered)
        };
        drop(retired);
        std::task::Poll::Pending
    }
    pub async fn cleanup_permitted(&self) {
        std::future::poll_fn(|cx| self.poll_cleanup_permitted(cx)).await
    }
    /// Actual SPI creator geometry, without creating a dummy receipt or Future.
    pub fn known_layouts() -> Result<
        [std::alloc::Layout; 2],
        novarocks_type_contract::owned_resources::layout::LayoutResourceError,
    > {
        use novarocks_type_contract::owned_resources::layout::arc_layout;
        Ok([
            arc_layout(std::alloc::Layout::new::<Self>())?,
            arc_layout(std::alloc::Layout::new::<ConnectorWriteNeededReady>())?,
        ])
    }
}

/// Cloneable FE planning facts for collect-on-write statistics on one logical
/// write target.
///
/// This value carries no publication or commit authority and is never encoded
/// into a writer handle. The frontend lowers its ordinary aggregate
/// requirements into the plan; only the resulting resolved generic plan facts
/// cross the native boundary.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct WriteStatisticsContract {
    requirements: Vec<StatisticsRequiredAggregation>,
}

impl WriteStatisticsContract {
    /// Freeze requirements against the exact input shape accepted by this
    /// target. An aggregate input is positional, so every descriptive fact is
    /// checked here before planning rather than trusted independently later.
    pub fn try_new(
        input: &ConnectorWriteInputShape,
        requirements: Vec<StatisticsRequiredAggregation>,
    ) -> Result<Self, ConnectorError> {
        if requirements.len() > MAX_CONNECTOR_STATISTICS_ARTIFACTS {
            return Err(ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "write statistics requirement count exceeds the artifact limit",
            ));
        }
        let fields = input.fields();
        let mut identities = std::collections::BTreeSet::new();
        for requirement in &requirements {
            let declared = requirement.input();
            let actual = fields.get(declared.ordinal()).ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "write statistics aggregate input ordinal is outside the target input",
                )
            })?;
            let actual_value_type = actual.execution_type().ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "write statistics target field has no complete execution declaration",
                )
            })?;
            if declared.name() != actual.field().name()
                || declared.value_type() != actual_value_type
            {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "write statistics aggregate input does not exactly match the target field",
                ));
            }
            if !identities.insert(requirement.artifact().clone()) {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "write statistics requirements contain a duplicate artifact identity",
                ));
            }
        }
        Ok(Self { requirements })
    }

    pub fn requirements(&self) -> &[StatisticsRequiredAggregation] {
        &self.requirements
    }

    pub const fn is_empty(&self) -> bool {
        self.requirements.is_empty()
    }

    fn validate_against(&self, input: &ConnectorWriteInputShape) -> Result<(), ConnectorError> {
        Self::try_new(input, self.requirements.clone()).map(|_| ())
    }
}

/// One generic statistics artifact produced for one exact logical write
/// target. Target membership is explicit because several physical targets may
/// contribute the same artifact identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WriteStatisticsArtifact {
    target: WriteTargetOrdinal,
    draft: StatisticsArtifactDraft,
}

impl WriteStatisticsArtifact {
    pub const fn new(target: WriteTargetOrdinal, draft: StatisticsArtifactDraft) -> Self {
        Self { target, draft }
    }

    pub const fn target(&self) -> WriteTargetOrdinal {
        self.target
    }

    pub const fn draft(&self) -> &StatisticsArtifactDraft {
        &self.draft
    }

    pub fn into_parts(self) -> (WriteTargetOrdinal, StatisticsArtifactDraft) {
        (self.target, self.draft)
    }
}

/// The frozen intent a frontend hands to `begin_write`.
///
/// Every fact here is decided before any external write side effect. The
/// provider completes all local and metadata admission inside `begin_write`,
/// so a sealed plan can never discover mid-execution that its write was never
/// admissible.
#[derive(Clone)]
pub struct ConnectorWriteBeginRequest {
    pub table: Arc<str>,
    pub target_ref: ConnectorWriteTargetRef,
    pub intent: ConnectorWriteIntent,
    pub purpose: ConnectorWriteAdmissionPurpose,
    pub input: ConnectorWriteInputRequest,
    pub base: Option<ConnectorWriteBaseVersion>,
    /// What kind of write this is, and the facts only that kind needs.
    pub flavor: ConnectorWriteSessionFlavor,
    pub context: ConnectorRequestContext,
}

impl ConnectorWriteBeginRequest {
    pub fn validate_document_publication(&self) -> Result<(), ConnectorError> {
        if let ConnectorWriteSessionFlavor::ApplicationDocumentPublication { declaration, .. } =
            &self.flavor
        {
            declaration.validate_for_write(self.base.as_ref())?;
        }
        Ok(())
    }
}

/// The write flavors a session admits.
///
/// This selects how the provider plans its logical branches. It is deliberately
/// not a writer identity and carries no operation, cohort, attempt, or
/// placement: two writes of the same flavor against the same table are the same
/// kind of write, and what distinguishes them belongs to whoever owns their
/// external effect.
#[derive(Clone, Debug)]
pub enum ConnectorWriteSessionFlavor {
    /// One logical target writing data.
    Ordinary,
    /// A write into a target the provider has staged but has not registered.
    ///
    /// Every other flavor names its target and lets the provider look it up.
    /// A staged target has no catalog entry to look up -- that is what makes it
    /// staged -- so the caller hands the session the provider-frozen target
    /// facts a catalog load would otherwise have returned. They stay opaque:
    /// this is the same provider-owned table value the staged-create capability
    /// vends, and it names no publication, operation, or attempt.
    StagedCreate(ConnectorTableHandle),
    /// A publication whose application-owned durable facts are attached by
    /// the same existing write-session commit that publishes its data.
    /// The caller declares `shape`: an incremental `_file` / `_pos` change
    /// stream shares its physical input with a row mutation, while this
    /// declaration binds the session to its publication contract.
    ApplicationDocumentPublication {
        declaration: ConnectorDocumentPublicationDeclaration,
        shape: ConnectorManagedPublicationShape,
    },
    /// A merge-on-read row mutation. The provider decides how many branches
    /// the mutation needs and what each accepts; SQL routes rows to them.
    RowMutation,
    /// A copy-on-write row mutation, which rewrites whole data files.
    ///
    /// It carries the match selection and the exact tokenized contract that
    /// produced it because a copy-on-write session cannot be planned without
    /// either: which files are rewritten, which rows were matched, and where
    /// each writer value came from must remain one provider-owned decision.
    ///
    /// This is a separate flavor from [`Self::RowMutation`] for that reason
    /// alone: the two differ by an input one of them cannot be opened without.
    CopyOnWrite {
        selection: ConnectorRowMutationSelection,
        match_contract: ConnectorMutationMatchContract,
    },
    /// A rewrite whose reads, writers and replacement set consume one provider-owned frozen plan.
    DistributedRewrite {
        shape: ConnectorDistributedRewriteShape,
        frozen: crate::connector::ConnectorProcessLocalPayload,
    },
}

/// How a publication's rows reach it.
///
/// This is the branch structure the publication commits, not a tuning knob.
/// The three shapes differ in whether SQL routes change events at all, and in
/// what the commit must supersede.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectorManagedPublicationShape {
    /// The publication replaces rows wholesale; SQL sends data rows to one
    /// unrouted branch, and there is no change event to route.
    Data,
    /// The publication applies a change stream that only ever inserts.
    ///
    /// It seals one data branch like [`Self::Data`], but a *routed* one, because
    /// its rows arrive as change events and SQL's change-stream compile requires
    /// every branch to declare which effects it accepts. Nothing is superseded,
    /// so no delete artifact is frozen.
    ///
    /// This is a distinct shape rather than a flag on [`Self::Data`] because the
    /// two produce different plans: one is compiled as an ordinary write, the
    /// other through the change-stream router.
    ///
    /// Note what this does NOT do: an effect no branch accepts is dropped by the
    /// router, not refused, so declaring only Insert does not make "no deletes
    /// can appear" an enforced invariant. It remains the caller's precondition.
    InsertOnlyChangeStream,
    /// The publication applies a change stream that supersedes rows; SQL routes
    /// change events to the branches the provider seals, and the session freezes
    /// the old delete artifacts those branches supersede.
    RowMutation,
}

/// One logical write target and its immutable recipe.
///
/// The same `handle` is copied to every physical writer placement serving this
/// target. The frontend charges the unique-handle budget once per target, not
/// once per copy.
#[derive(Clone, Debug)]
pub struct ConnectorWriteTargetPlan {
    ordinal: WriteTargetOrdinal,
    handle: ConnectorWriterHandle,
    input: ConnectorWriteInputShape,
    statistics: WriteStatisticsContract,
    route: Option<ConnectorWriteRouteFacts>,
    routing_proof: Option<ConnectorWriteCohortRoutingProof>,
    rewrite_source: Option<ConnectorWriteRewriteSource>,
    rewrite_group_digest: Option<[u8; 32]>,
}

/// What one copy-on-write target must read to produce its rows.
///
/// A copy-on-write branch rewrites whole data files, so its input is not a
/// projection of the statement's source: it re-reads exactly the files this
/// target replaces. The read contract travels with the target because the two
/// are one decision -- a target that replaced one file set while reading
/// another would silently drop or duplicate rows -- and it is provider-owned
/// throughout: the engine passes it to the read side without interpreting it.
#[derive(Clone, Debug)]
pub struct ConnectorWriteRewriteSource {
    signed: Arc<ConnectorWriteRewriteSourceFacts>,
    frozen_read_source: Option<crate::connector::read_stack::ConnectorFrozenReadSource>,
}
/// Only signed immutable facts are shared. Post-signing read capability stays
/// wrapper-local, so retaining the original source creates no capability cycle.
#[derive(Debug)]
struct ConnectorWriteRewriteSourceFacts {
    source: ConnectorTableHandle,
    pinned_source: ConnectorPinnedFileSet,
    base_version_digest: [u8; 32],
    scan_schema: SchemaRef,
    scan_execution_schema: novarocks_connector_contract::ConnectorReadExecutionSchema,
    scan_bindings: Vec<ConnectorRowMutationScanBinding>,
    match_tokens: Vec<ConnectorWriteFieldToken>,
    written_version_token: Option<ConnectorWriteFieldToken>,
    _storage: novarocks_type_contract::ValueTypeStorageLifetime,
}
impl ConnectorWriteRewriteSource {
    pub fn try_new_typed(
        source: ConnectorTableHandle,
        pinned_source: ConnectorPinnedFileSet,
        base_version_digest: [u8; 32],
        scan_schema: SchemaRef,
        scan_execution_schema: novarocks_connector_contract::ConnectorReadExecutionSchema,
        scan_bindings: Vec<ConnectorRowMutationScanBinding>,
        match_tokens: Vec<ConnectorWriteFieldToken>,
        written_version_token: Option<ConnectorWriteFieldToken>,
        creation: &mut crate::connector::read_stack::ConnectorReadSchemaCreation<'_>,
    ) -> Result<Self, novarocks_connector_contract::PureProviderCompileError<ConnectorError>> {
        crate::connector::distributed_rewrite::validate_scan_source_pair(
            &scan_schema,
            &scan_execution_schema,
            creation,
        )?;
        let signed = creation.arc(ConnectorWriteRewriteSourceFacts {
            source,
            pinned_source,
            base_version_digest,
            scan_schema,
            scan_execution_schema,
            scan_bindings,
            match_tokens,
            written_version_token,
            _storage: creation.storage(),
        })?;
        Ok(Self {
            signed,
            frozen_read_source: None,
        })
    }
    /// Attach the original source observation after signing. This capability
    /// is separate from the immutable source facts and never reauthors them.
    pub fn with_frozen_read_source(
        mut self,
        source: crate::connector::read_stack::ConnectorFrozenReadSource,
    ) -> Self {
        self.frozen_read_source = Some(source);
        self
    }
    pub fn frozen_read_source(
        &self,
    ) -> Option<&crate::connector::read_stack::ConnectorFrozenReadSource> {
        self.frozen_read_source.as_ref()
    }
    pub fn source(&self) -> &ConnectorTableHandle {
        &self.signed.source
    }
    pub fn pinned_source(&self) -> &ConnectorPinnedFileSet {
        &self.signed.pinned_source
    }
    pub fn base_version_digest(&self) -> [u8; 32] {
        self.signed.base_version_digest
    }
    /// Original storage format; this loan grants no execution value semantics.
    pub fn scan_schema(&self) -> &arrow::datatypes::Schema {
        &self.signed.scan_schema
    }
    pub fn scan_execution_schema(
        &self,
    ) -> &novarocks_connector_contract::ConnectorReadExecutionSchema {
        &self.signed.scan_execution_schema
    }
    pub fn scan_bindings(&self) -> &[ConnectorRowMutationScanBinding] {
        &self.signed.scan_bindings
    }
    pub fn match_tokens(&self) -> &[ConnectorWriteFieldToken] {
        &self.signed.match_tokens
    }
    pub fn written_version_token(&self) -> Option<ConnectorWriteFieldToken> {
        self.signed.written_version_token
    }
}

/// What SQL needs to route rows to one row-mutation branch.
///
/// These are routing facts, not identity: they say which change events a branch
/// accepts and where in the input row its columns live. The branch's identity is
/// its [`WriteTargetOrdinal`], and its recipe is the opaque writer handle beside
/// it -- neither is derivable from these facts, and none of them reaches a
/// commit fragment.
/// Correctness colocation is independent of writer instance count and layout.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConnectorWriteColocation {
    None,
    Singleton,
    ByKey(Vec<ConnectorWriteFieldToken>),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConnectorWriteLayoutPreference {
    None,
    ClusterBy(Vec<ConnectorWriteFieldToken>),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorWriteRouteRequirements {
    pub colocation: ConnectorWriteColocation,
    pub layout: ConnectorWriteLayoutPreference,
}

impl ConnectorWriteRouteRequirements {
    pub fn validate(&self) -> Result<(), ConnectorError> {
        let validate_keys = |keys: &[ConnectorWriteFieldToken]| {
            if keys.is_empty()
                || keys
                    .iter()
                    .copied()
                    .collect::<std::collections::BTreeSet<_>>()
                    .len()
                    != keys.len()
            {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "write route key must be nonempty and contain no duplicate fields",
                ));
            }
            Ok(())
        };
        if let ConnectorWriteColocation::ByKey(keys) = &self.colocation {
            validate_keys(keys)?;
        }
        if let ConnectorWriteLayoutPreference::ClusterBy(keys) = &self.layout {
            validate_keys(keys)?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct ConnectorWriteRouteFacts {
    route_id: ConnectorWriteRouteId,
    accepted_effects: Vec<ConnectorRowMutationEffect>,
    input_ordinals: Vec<ConnectorMutationRouteInput>,
    requirements: ConnectorWriteRouteRequirements,
    selection_bindings: Arc<[ConnectorWriteSelectionBinding]>,
}

/// The role of a value read from the provider-signed match selection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectorWriteSelectionBindingRole {
    Identity,
    AfterImage,
}

/// A value whose null representation asks the provider to inherit the value
/// from the physical artifact it is currently writing.
///
/// This is deliberately disjoint from [`ConnectorWriteValueSource::Selection`]:
/// a derived value has no selection token or ordinal that a caller could use
/// to disguise it as an arbitrary identity field.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectorWriteProviderDerivedValue {
    Inherit,
}

/// The exact source of one writer occurrence.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectorWriteValueSource {
    Selection {
        token: ConnectorWriteFieldToken,
        ordinal: u32,
        role: ConnectorWriteSelectionBindingRole,
    },
    ProviderDerived(ConnectorWriteProviderDerivedValue),
}

/// Exact bridge between the independently signed writer and match-selection
/// token spaces. Field names are descriptive only and never participate.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConnectorWriteSelectionBinding {
    writer_token: ConnectorWriteFieldToken,
    source: ConnectorWriteValueSource,
}

impl ConnectorWriteSelectionBinding {
    pub const fn new(
        writer_token: ConnectorWriteFieldToken,
        selection_token: ConnectorWriteFieldToken,
        selection_ordinal: u32,
        role: ConnectorWriteSelectionBindingRole,
    ) -> Self {
        Self {
            writer_token,
            source: ConnectorWriteValueSource::Selection {
                token: selection_token,
                ordinal: selection_ordinal,
                role,
            },
        }
    }

    pub const fn provider_derived(
        writer_token: ConnectorWriteFieldToken,
        value: ConnectorWriteProviderDerivedValue,
    ) -> Self {
        Self {
            writer_token,
            source: ConnectorWriteValueSource::ProviderDerived(value),
        }
    }

    pub const fn writer_token(&self) -> ConnectorWriteFieldToken {
        self.writer_token
    }

    pub const fn source(&self) -> ConnectorWriteValueSource {
        self.source
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectorWriteCohortRoutingBody {
    Rewrite,
    Append,
}

/// Provider-frozen proof that one target owns exactly these selection rows.
#[derive(Clone, Debug)]
pub struct ConnectorWriteCohortRoutingProof {
    route_id: ConnectorWriteRouteId,
    selection_digest: [u8; 32],
    selection_ordinals: Arc<[ConnectorRowMutationSelectionOrdinal]>,
    body: ConnectorWriteCohortRoutingBody,
}

impl ConnectorWriteCohortRoutingProof {
    pub fn try_new(
        route_id: ConnectorWriteRouteId,
        selection_digest: [u8; 32],
        selection_ordinals: impl Into<Arc<[ConnectorRowMutationSelectionOrdinal]>>,
        body: ConnectorWriteCohortRoutingBody,
    ) -> Result<Self, ConnectorError> {
        let selection_ordinals = selection_ordinals.into();
        if selection_ordinals.is_empty()
            || selection_ordinals.windows(2).any(|pair| pair[0] >= pair[1])
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "COW routing proof ordinals must be non-empty and strictly increasing",
            ));
        }
        Ok(Self {
            route_id,
            selection_digest,
            selection_ordinals,
            body,
        })
    }

    pub const fn route_id(&self) -> ConnectorWriteRouteId {
        self.route_id
    }

    pub const fn selection_digest(&self) -> [u8; 32] {
        self.selection_digest
    }

    pub fn selection_ordinals(&self) -> &[ConnectorRowMutationSelectionOrdinal] {
        &self.selection_ordinals
    }

    pub const fn body(&self) -> ConnectorWriteCohortRoutingBody {
        self.body
    }
}

// Same enum order as ConnectorWriteInputShape::fields(), borrowed only.
// Validation must not allocate a projection before the caller's growth gate.
fn borrowed_write_input_fields(
    input: &ConnectorWriteInputShape,
) -> impl Iterator<Item = &crate::connector::ConnectorWriteFieldBinding> {
    let (first, second): (
        &[crate::connector::ConnectorWriteFieldBinding],
        &[crate::connector::ConnectorWriteFieldBinding],
    ) = match input {
        ConnectorWriteInputShape::Data { fields } => (fields, &[]),
        ConnectorWriteInputShape::RowLineage {
            data_fields,
            row_identity_fields,
        } => (data_fields, row_identity_fields),
        ConnectorWriteInputShape::PositionDelete {
            identity_fields,
            partition_source_fields,
        }
        | ConnectorWriteInputShape::DeletionVector {
            identity_fields,
            partition_source_fields,
        } => (identity_fields, partition_source_fields),
        ConnectorWriteInputShape::EqualityDelete { equality_fields } => (equality_fields, &[]),
    };
    first.iter().chain(second.iter())
}

impl ConnectorWriteRouteFacts {
    /// A branch that accepts no change event would silently drop every row
    /// routed to it, so an empty effect set is refused.
    pub fn try_new(
        route_id: ConnectorWriteRouteId,
        accepted_effects: Vec<ConnectorRowMutationEffect>,
        input_ordinals: Vec<ConnectorMutationRouteInput>,
        requirements: ConnectorWriteRouteRequirements,
        selection_bindings: impl Into<Arc<[ConnectorWriteSelectionBinding]>>,
    ) -> Result<Self, ConnectorError> {
        if accepted_effects.is_empty() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "a row-mutation route must accept at least one change event",
            ));
        }
        requirements.validate()?;
        let input_tokens = input_ordinals
            .iter()
            .map(|input| input.token())
            .collect::<std::collections::BTreeSet<_>>();
        let keys = match &requirements.colocation {
            ConnectorWriteColocation::ByKey(keys) => keys.as_slice(),
            ConnectorWriteColocation::None | ConnectorWriteColocation::Singleton => &[],
        };
        let layout_keys = match &requirements.layout {
            ConnectorWriteLayoutPreference::None => &[][..],
            ConnectorWriteLayoutPreference::ClusterBy(keys) => keys.as_slice(),
        };
        if keys
            .iter()
            .chain(layout_keys.iter())
            .any(|key| !input_tokens.contains(key))
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "write route requirement refers to a field outside its input",
            ));
        }
        Ok(Self {
            route_id,
            accepted_effects,
            input_ordinals,
            requirements,
            selection_bindings: selection_bindings.into(),
        })
    }

    pub const fn route_id(&self) -> ConnectorWriteRouteId {
        self.route_id
    }

    pub fn accepted_effects(&self) -> &[ConnectorRowMutationEffect] {
        &self.accepted_effects
    }

    pub fn input_ordinals(&self) -> &[ConnectorMutationRouteInput] {
        &self.input_ordinals
    }

    pub const fn requirements(&self) -> &ConnectorWriteRouteRequirements {
        &self.requirements
    }

    pub fn selection_bindings(&self) -> &[ConnectorWriteSelectionBinding] {
        &self.selection_bindings
    }

    fn validate_against_input(
        &self,
        input: &ConnectorWriteInputShape,
    ) -> Result<(), ConnectorError> {
        let field_count = borrowed_write_input_fields(input).count();
        if self.input_ordinals.len() != field_count
            || self
                .input_ordinals
                .iter()
                .zip(borrowed_write_input_fields(input))
                .any(|(route, field)| route.token() != field.token())
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "row-mutation route input token order differs from its target input",
            ));
        }
        if !self.selection_bindings.is_empty()
            && (self.selection_bindings.len() != field_count
                || self
                    .selection_bindings
                    .iter()
                    .zip(borrowed_write_input_fields(input))
                    .any(|(binding, field)| binding.writer_token() != field.token()))
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "COW selection bindings are incomplete or reordered",
            ));
        }
        Ok(())
    }

    pub fn validate_selection_contract(
        &self,
        input: &ConnectorWriteInputShape,
        contract: &ConnectorMutationMatchContract,
    ) -> Result<(), ConnectorError> {
        self.validate_against_input(input)?;
        if self.selection_bindings.is_empty() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "COW route omits its provider-signed selection bindings",
            ));
        }
        for (binding, writer) in self
            .selection_bindings
            .iter()
            .zip(borrowed_write_input_fields(input))
        {
            match binding.source() {
                ConnectorWriteValueSource::Selection {
                    token,
                    ordinal,
                    role,
                } => {
                    let selection = contract.selection_field(token).ok_or_else(|| {
                        ConnectorError::new(
                            ConnectorErrorKind::InvalidRequest,
                            "COW route selection binding carries a foreign token",
                        )
                    })?;
                    let role_matches = matches!(
                        (role, selection.role()),
                        (
                            ConnectorWriteSelectionBindingRole::Identity,
                            ConnectorMutationSelectionFieldRole::Identity
                        ) | (
                            ConnectorWriteSelectionBindingRole::AfterImage,
                            ConnectorMutationSelectionFieldRole::AfterImage
                        )
                    );
                    if ordinal != selection.ordinal()
                        || !role_matches
                        || writer.field().data_type() != selection.field().data_type()
                        || (selection.field().is_nullable() && !writer.field().is_nullable())
                    {
                        return Err(ConnectorError::new(
                            ConnectorErrorKind::InvalidRequest,
                            "COW route selection binding differs from its signed token, role, ordinal, or field",
                        ));
                    }
                }
                ConnectorWriteValueSource::ProviderDerived(
                    ConnectorWriteProviderDerivedValue::Inherit,
                ) if writer.field().is_nullable() => {}
                ConnectorWriteValueSource::ProviderDerived(_) => {
                    return Err(ConnectorError::new(
                        ConnectorErrorKind::InvalidRequest,
                        "COW provider-derived writer occurrence must admit the null inherit marker",
                    ));
                }
            }
        }
        Ok(())
    }
}

impl ConnectorWriteTargetPlan {
    pub const fn new(
        ordinal: WriteTargetOrdinal,
        handle: ConnectorWriterHandle,
        input: ConnectorWriteInputShape,
    ) -> Self {
        Self {
            ordinal,
            handle,
            input,
            statistics: WriteStatisticsContract {
                requirements: Vec::new(),
            },
            route: None,
            routing_proof: None,
            rewrite_source: None,
            rewrite_group_digest: None,
        }
    }

    pub fn with_statistics_contract(
        mut self,
        statistics: WriteStatisticsContract,
    ) -> Result<Self, ConnectorError> {
        statistics.validate_against(&self.input)?;
        self.statistics = statistics;
        Ok(self)
    }

    /// Attach the routing facts of a row-mutation branch.
    pub fn with_route(mut self, route: ConnectorWriteRouteFacts) -> Self {
        self.route = Some(route);
        self
    }

    pub fn with_routing_proof(mut self, proof: ConnectorWriteCohortRoutingProof) -> Self {
        self.routing_proof = Some(proof);
        self
    }

    /// Attach the read contract of a copy-on-write branch.
    pub fn with_rewrite_source(mut self, source: ConnectorWriteRewriteSource) -> Self {
        self.rewrite_source = Some(source);
        self
    }

    pub fn with_rewrite_group_digest(mut self, digest: [u8; 32]) -> Self {
        self.rewrite_group_digest = Some(digest);
        self
    }
    pub const fn rewrite_group_digest(&self) -> Option<[u8; 32]> {
        self.rewrite_group_digest
    }

    /// Present exactly for a row-mutation branch.
    pub const fn route(&self) -> Option<&ConnectorWriteRouteFacts> {
        self.route.as_ref()
    }

    pub const fn routing_proof(&self) -> Option<&ConnectorWriteCohortRoutingProof> {
        self.routing_proof.as_ref()
    }

    /// Present exactly for a copy-on-write branch that rewrites files.
    pub const fn rewrite_source(&self) -> Option<&ConnectorWriteRewriteSource> {
        self.rewrite_source.as_ref()
    }

    pub const fn ordinal(&self) -> WriteTargetOrdinal {
        self.ordinal
    }

    pub const fn handle(&self) -> &ConnectorWriterHandle {
        &self.handle
    }

    pub const fn input(&self) -> &ConnectorWriteInputShape {
        &self.input
    }

    pub const fn statistics(&self) -> &WriteStatisticsContract {
        &self.statistics
    }
}

/// What `begin_write` returns: the frontend-only commit authority plus the
/// sealed logical target map.
#[derive(Debug)]
pub struct ConnectorWriteSessionPlan {
    commit: ConnectorWriteCommitHandle,
    targets: Vec<ConnectorWriteTargetPlan>,
    copy_on_write: Option<ConnectorWriteCopyOnWriteRoutingPlan>,
}

#[derive(Clone, Debug)]
pub struct ConnectorWriteCopyOnWriteRoutingPlan {
    selection: ConnectorRowMutationSelection,
    match_contract: ConnectorMutationMatchContract,
}

impl ConnectorWriteCopyOnWriteRoutingPlan {
    pub const fn selection(&self) -> &ConnectorRowMutationSelection {
        &self.selection
    }

    pub const fn match_contract(&self) -> &ConnectorMutationMatchContract {
        &self.match_contract
    }
}

impl ConnectorWriteSessionPlan {
    /// Targets must be dense from zero and must all belong to the same exact
    /// provider generation as the commit handle. A disagreement here means the
    /// session and the plan could name different runtimes, so it fails before
    /// any fragment is encoded.
    pub fn try_new(
        commit: ConnectorWriteCommitHandle,
        targets: Vec<ConnectorWriteTargetPlan>,
    ) -> Result<Self, ConnectorError> {
        Self::try_with_copy_on_write(commit, targets, None)
    }

    pub fn try_copy_on_write(
        commit: ConnectorWriteCommitHandle,
        targets: Vec<ConnectorWriteTargetPlan>,
        selection: ConnectorRowMutationSelection,
        match_contract: ConnectorMutationMatchContract,
    ) -> Result<Self, ConnectorError> {
        Self::try_with_copy_on_write(
            commit,
            targets,
            Some(ConnectorWriteCopyOnWriteRoutingPlan {
                selection,
                match_contract,
            }),
        )
    }

    fn try_with_copy_on_write(
        commit: ConnectorWriteCommitHandle,
        targets: Vec<ConnectorWriteTargetPlan>,
        copy_on_write: Option<ConnectorWriteCopyOnWriteRoutingPlan>,
    ) -> Result<Self, ConnectorError> {
        let ordinals = targets
            .iter()
            .map(ConnectorWriteTargetPlan::ordinal)
            .collect::<Vec<_>>();
        validate_dense_target_ordinals(&ordinals)?;
        for target in &targets {
            if target.handle().binding() != commit.binding() {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "connector writer handle does not belong to the begin session's generation",
                ));
            }
            target.input().validate()?;
            if let Some(route) = target.route() {
                route.validate_against_input(target.input())?;
            }
        }
        // Routing is a property of the whole session, not of individual
        // branches: if some branches carry routing facts and others do not, SQL
        // can route rows to part of the write and silently has nowhere to send
        // the rest.
        let routed = targets
            .iter()
            .filter(|target| target.route().is_some())
            .count();
        if routed != 0 && routed != targets.len() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "a connector write session routes either every branch or none",
            ));
        }
        // Two branches sharing a route key would make the router's choice
        // ambiguous, and the loser's rows would vanish.
        let mut seen = std::collections::BTreeSet::new();
        for target in &targets {
            target.statistics.validate_against(&target.input)?;
            if let Some(route) = target.route()
                && !seen.insert(route.route_id())
            {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "a connector write session repeats a row-mutation route key",
                ));
            }
        }
        match &copy_on_write {
            Some(routing) => validate_copy_on_write_routing(routing, &targets)?,
            None if targets
                .iter()
                .any(|target| target.routing_proof().is_some()) =>
            {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "COW routing proof requires its signed selection and match contract",
                ));
            }
            None => {}
        }
        Ok(Self {
            commit,
            targets,
            copy_on_write,
        })
    }

    pub const fn binding(&self) -> &ConnectorWriteBinding {
        self.commit.binding()
    }

    pub const fn commit_handle(&self) -> &ConnectorWriteCommitHandle {
        &self.commit
    }

    pub fn targets(&self) -> &[ConnectorWriteTargetPlan] {
        &self.targets
    }

    pub const fn copy_on_write(&self) -> Option<&ConnectorWriteCopyOnWriteRoutingPlan> {
        self.copy_on_write.as_ref()
    }

    /// The sealed ordinal set a prepared write set must not exceed.
    pub fn expected_targets(&self) -> Vec<WriteTargetOrdinal> {
        self.targets
            .iter()
            .map(ConnectorWriteTargetPlan::ordinal)
            .collect()
    }

    pub fn into_parts(self) -> (ConnectorWriteCommitHandle, Vec<ConnectorWriteTargetPlan>) {
        (self.commit, self.targets)
    }
}

fn validate_copy_on_write_routing(
    routing: &ConnectorWriteCopyOnWriteRoutingPlan,
    targets: &[ConnectorWriteTargetPlan],
) -> Result<(), ConnectorError> {
    routing.selection.validate()?;
    routing
        .match_contract
        .validate_selection(&routing.selection)?;
    if targets.is_empty()
        || targets
            .iter()
            .any(|target| target.route().is_none() || target.routing_proof().is_none())
    {
        return Err(ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            "COW session requires one route and routing proof per target",
        ));
    }
    let row_count = usize::try_from(routing.selection.row_count()).map_err(|_| {
        ConnectorError::new(
            ConnectorErrorKind::ResourceExhausted,
            "COW selection row count does not fit routing validation memory",
        )
    })?;
    let mut covered = vec![false; row_count];
    for target in targets {
        let route = target.route().expect("checked above");
        route.validate_selection_contract(target.input(), &routing.match_contract)?;
        let proof = target.routing_proof().expect("checked above");
        if proof.route_id() != route.route_id()
            || proof.selection_digest() != routing.selection.digest()
            || matches!(proof.body(), ConnectorWriteCohortRoutingBody::Rewrite)
                != target.rewrite_source().is_some()
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "COW routing proof differs from its target, route, or selection",
            ));
        }
        let derived_tokens = route
            .selection_bindings()
            .iter()
            .filter(|binding| {
                matches!(
                    binding.source(),
                    ConnectorWriteValueSource::ProviderDerived(
                        ConnectorWriteProviderDerivedValue::Inherit
                    )
                )
            })
            .map(ConnectorWriteSelectionBinding::writer_token)
            .collect::<Vec<_>>();
        let expected_derived = target
            .rewrite_source()
            .and_then(ConnectorWriteRewriteSource::written_version_token)
            .map(|token| vec![token])
            .unwrap_or_default();
        if derived_tokens != expected_derived {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "COW provider-derived writer source differs from its frozen rewrite role",
            ));
        }
        for ordinal in proof.selection_ordinals() {
            let index = usize::try_from(ordinal.get()).map_err(|_| {
                ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "COW routing proof ordinal does not fit this process",
                )
            })?;
            if index >= row_count || covered[index] {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "COW routing proof coverage is out of bounds or overlapping",
                ));
            }
            covered[index] = true;
            let effect = selection_effect(
                &routing.selection,
                *ordinal,
                routing.match_contract.effect_field().target_ordinal(),
            )?;
            if !route.accepted_effects().contains(&effect)
                || matches!(proof.body(), ConnectorWriteCohortRoutingBody::Append)
                    != (effect == ConnectorRowMutationEffect::Insert)
            {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "COW routing proof does not exactly cover its route effects",
                ));
            }
        }
    }
    if covered.iter().any(|covered| !covered) {
        return Err(ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            "COW routing proofs do not exactly cover the selection",
        ));
    }
    Ok(())
}

/// Commit one complete prepared write set.
///
/// The commit handle is borrowed, never moved: a frontend session keeps it for
/// a possible abort or reconcile, and nothing else in the process can take
/// ownership of it.
pub struct ConnectorWriteFinishRequest<'a> {
    pub commit: &'a ConnectorWriteCommitHandle,
    pub prepared: ConnectorPreparedWriteSet,
    pub statistics: Vec<WriteStatisticsArtifact>,
    pub publication: ConnectorWriteFinishPublication,
    pub context: ConnectorRequestContext,
}

/// Borrowed view of the original stable finish packet. Polling or dropping a
/// Provider future cannot destroy the caller's admitted input or authority.
pub struct ConnectorWriteFinishRequestRef<'a> {
    pub commit: &'a ConnectorWriteCommitHandle,
    pub prepared: &'a ConnectorPreparedWriteSet,
    pub statistics: &'a [WriteStatisticsArtifact],
    pub publication: &'a ConnectorWriteFinishPublication,
    pub context: &'a ConnectorRequestContext,
    pub production: ConnectorWriteAbortProduction,
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[expect(
    clippy::large_enum_variant,
    reason = "The public finish publication union carries the exact keyed intent by value; boxing it would change the shared SPI contract"
)]
pub enum ConnectorWriteFinishPublication {
    None,
    KeyedDocuments(crate::connector::KeyedPublicationIntent),
}

/// Release a begin session that never reached a complete prepared write set.
///
/// This is a known-uncommitted path: it may clean up provider-side staging, and
/// it must never report a commit it did not observe.
/// Original writer-production evidence, not an object-deletion capability.
/// Only the frontend's stable Session can prove that no production was ever
/// registered and that its complete received inventory is still empty.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectorWriteAbortProduction {
    NotStarted,
    /// The original Session accepted every exact End on its own production
    /// origin. This is not granted by constructing a prepared set: the caller
    /// must retain the original received union and its provider binding.
    CompleteReceived,
    Unproven,
}

pub struct ConnectorWriteSessionAbortRequest<'a> {
    pub commit: &'a ConnectorWriteCommitHandle,
    pub context: ConnectorRequestContext,
    pub production: ConnectorWriteAbortProduction,
    /// Borrow only the original stable, complete received union. The Provider
    /// additionally checks its own known-uncommitted state and exact binding.
    pub received: Option<&'a ConnectorPreparedWriteSet>,
}

/// Resolve a commit whose external outcome is unknown.
pub struct ConnectorWriteSessionReconcileRequest<'a> {
    pub commit: &'a ConnectorWriteCommitHandle,
    pub evidence: ExternalMutationEvidence,
    pub context: ConnectorRequestContext,
}

/// The frontend-only external write authority of one exact provider
/// generation.
///
/// Admission and prepared-result projection read exact provider facts; terminal
/// methods own external catalog effects. None is reachable from a backend role
/// binding.
pub trait ConnectorWriteControl: Send + Sync {
    /// Initial selected routes of THIS original Rewrite-begin factory. This
    /// reports no SDK layout, byte allowance or concurrency quota.
    fn rewrite_begin_initial_dependencies(
        &self,
    ) -> Result<Vec<crate::connector::SharedDependency>, ConnectorError> {
        Err(ConnectorError::new(
            crate::connector::ConnectorErrorKind::Unsupported,
            "selected Rewrite begin factory has no original route facts",
        ))
    }

    /// Exact first-party Rewrite begin source of this selected generation.
    /// This observation grants neither CPU nor I/O or payload capacity.
    fn rewrite_begin_cpu_author(
        &self,
    ) -> Option<(
        crate::connector::ConnectorProviderBindingKey,
        std::any::TypeId,
    )> {
        None
    }

    /// Install the original source before any foreign poll. Preserve the SAME
    /// scope, absolute deadline and retention through actual request/plan exit.
    /// Refusal leaves both receiving slots unchanged; unsupported capability
    /// never delegates to an ordinary or synchronous begin path.
    fn prepare_rewrite_begin_source_in(
        self: std::sync::Arc<Self>,
        request: &mut Option<ConnectorWriteBeginRequest>,
        original: crate::connector::ConnectorOriginalResultScope,
        receiving: &mut Option<
            std::sync::Arc<dyn crate::connector::ConnectorRewriteWriteBeginSource>,
        >,
    ) -> Result<(), ConnectorError> {
        let _ = (request, original, receiving);
        Err(ConnectorError::new(
            crate::connector::ConnectorErrorKind::Unsupported,
            "selected write control has no governed Rewrite begin source",
        ))
    }

    /// Pure backing of the actual control factory and this exact generation's
    /// selected routes. The caller adds its own source backing before admission.
    /// This is a quote, never a grant.
    fn io_profile(
        &self,
    ) -> Result<crate::connector::ConnectorMetadataMaintenanceIoProfile, ConnectorError>;

    /// Pure observation of the original session's object responsibility.
    /// Business completion, timeout and a lost waiter never prove disposal.
    fn cleanup_responsibility(
        &self,
        commit: &ConnectorWriteCommitHandle,
    ) -> Result<crate::connector::ConnectorMetadataCleanupResponsibility, ConnectorError>;

    /// Observe only a finite finalization context already received by this
    /// exact original session. This must never begin a grant or derive a new
    /// deadline/stop. None is absence, not permission to use business control.
    fn existing_finalization_context(
        &self,
        _commit: &ConnectorWriteCommitHandle,
    ) -> Result<Option<ConnectorRequestContext>, ConnectorError> {
        Ok(None)
    }

    /// Direct asynchronous control I/O. The original caller retains the
    /// operation future; dropping a waiter is not a cancellation or exit fact.
    fn begin_write_async(
        &self,
        request: ConnectorWriteBeginRequest,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = Result<ConnectorWriteSessionPlan, ConnectorError>>
                + Send
                + '_,
        >,
    >;

    /// Checked COW admission preserves the caller's original result authority
    /// across physical awaits. This is required and never delegates to ordinary begin.
    fn begin_cow_write_checked_async(
        &self,
        request: ConnectorWriteBeginRequest,
        original: crate::connector::ConnectorOriginalResultScope,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<
                        crate::connector::ConnectorCowBeginPlan,
                        crate::connector::ConnectorCowBeginFailure,
                    >,
                > + Send
                + '_,
        >,
    >;

    fn finish_write_async<'a>(
        &'a self,
        request: ConnectorWriteFinishRequestRef<'a>,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<ExternalMutationOutcome<ConnectorWriteReceipt>, ConnectorError>,
                > + Send
                + 'a,
        >,
    >;

    /// Same original finish, with its committed receipt handed off only after
    /// needed finalization. The Future retains its actual cleanup until exit.
    fn finish_write_with_needed_ready_async<'a>(
        &'a self,
        request: ConnectorWriteFinishRequestRef<'a>,
        receiving: &'a ConnectorWriteNeededReadyReceiving,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<ConnectorWritePublicationCompletion, ConnectorError>,
                > + Send
                + 'a,
        >,
    >;
    /// The sole exact reconciliation uses the same needed-ready author.
    fn reconcile_write_with_needed_ready_async<'a>(
        &'a self,
        request: ConnectorWriteSessionReconcileRequest<'a>,
        receiving: &'a ConnectorWriteNeededReadyReceiving,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<ConnectorWritePublicationCompletion, ConnectorError>,
                > + Send
                + 'a,
        >,
    >;

    fn abort_write_async<'a>(
        &'a self,
        request: ConnectorWriteSessionAbortRequest<'a>,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = Result<ConnectorWriteAbortOutcome, ConnectorError>>
                + Send
                + 'a,
        >,
    >;

    fn reconcile_write_async<'a>(
        &'a self,
        request: ConnectorWriteSessionReconcileRequest<'a>,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<ExternalMutationOutcome<ConnectorWriteReceipt>, ConnectorError>,
                > + Send
                + 'a,
        >,
    >;
    fn binding_key(&self) -> &ConnectorProviderBindingKey;

    /// Complete all admission and freeze the write recipe. On return either a
    /// session exists and no external effect has happened yet, or an error was
    /// raised and nothing was started.
    fn begin_write(
        &self,
        request: ConnectorWriteBeginRequest,
    ) -> Result<ConnectorWriteSessionPlan, ConnectorError>;

    /// First-party COW admission under the caller's original Internal window.
    /// Required: no default delegation to ordinary, unchecked begin_write.
    /// Provider capability checks and original request control remain required.
    fn begin_cow_write_checked(
        &self,
        request: ConnectorWriteBeginRequest,
        original: crate::connector::ConnectorOriginalResultScope,
    ) -> Result<crate::connector::ConnectorCowBeginPlan, crate::connector::ConnectorCowBeginFailure>;

    /// Interpret every commit fragment and perform exactly one external commit.
    fn finish_write(
        &self,
        request: ConnectorWriteFinishRequest<'_>,
    ) -> Result<ExternalMutationOutcome<ConnectorWriteReceipt>, ConnectorError>;

    /// Project a complete prepared write's exact visible-row change against
    /// this session's admitted version, without committing or reading data.
    /// None means the provider cannot establish that logical fact; physical
    /// file record totals and writer input counts are not substitutes.
    fn prepared_logical_row_change(
        &self,
        _commit: &ConnectorWriteCommitHandle,
        _prepared: &ConnectorPreparedWriteSet,
        _context: &ConnectorRequestContext,
    ) -> Result<Option<super::prepared::ConnectorWriteRowChange>, ConnectorError> {
        Err(ConnectorError::new(
            ConnectorErrorKind::Unsupported,
            "connector does not project prepared logical row changes",
        ))
    }

    fn abort_write(
        &self,
        request: ConnectorWriteSessionAbortRequest<'_>,
    ) -> Result<ConnectorWriteAbortOutcome, ConnectorError>;

    fn reconcile_write(
        &self,
        request: ConnectorWriteSessionReconcileRequest<'_>,
    ) -> Result<ExternalMutationOutcome<ConnectorWriteReceipt>, ConnectorError>;
}

#[cfg(test)]
mod tests {
    #[test]
    fn rewrite_source_shares_frozen_bindings_and_keeps_independent_complete_value_lifetimes() {
        use super::*;
        use arrow::datatypes::{DataType, Field, Schema};
        use novarocks_connector_contract::ConnectorReadExecutionSchema;
        use novarocks_type_contract::{
            FunctionValueType, ValueKind, ValueLayout, ValueLogicalType, ValueTypeRef,
            ValueTypeStorageHold, ValueTypeStorageLifetime,
        };
        use std::sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        };
        struct Creator(Arc<AtomicUsize>);
        impl ValueTypeStorageHold for Creator {}
        impl Drop for Creator {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }
        let exits = Arc::new(AtomicUsize::new(0));
        let storage = ValueTypeStorageLifetime::held(Arc::new(Creator(exits.clone())));
        let child =
            FunctionValueType::try_with_logical_type(DataType::Utf8, true, ValueLogicalType::Json)
                .unwrap();
        let carrier = DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)));
        let binding = FunctionValueType::try_from_value(
            ValueTypeRef::try_new(
                ValueKind::List(child.value_type().clone()),
                false,
                storage.clone(),
            )
            .unwrap(),
            ValueLayout::try_new(&carrier, storage.clone()).unwrap(),
        )
        .unwrap();
        let execution = ConnectorReadExecutionSchema::try_new(
            Arc::new(Schema::new(vec![Field::new("payload", carrier, false)])),
            vec![binding].into_boxed_slice(),
            storage.clone(),
        )
        .unwrap();
        // The exact original format uses its own anonymous role name. Its
        // generic attributes neither replace nor mint the typed declaration.
        let original = Arc::new(Schema::new(vec![
            Field::new(
                "payload",
                DataType::List(Arc::new(Field::new("element", DataType::Utf8, true))),
                false,
            )
            .with_metadata(std::collections::HashMap::from([(
                novarocks_type_contract::NR_LOGICAL_TYPE_KEY.to_owned(),
                "unknown-source-attribute".to_owned(),
            )])),
        ]));
        let token = ConnectorWriteFieldToken::from_bytes([1; 32]);
        let mut request = |_| Ok(());
        let mut work = |_| Ok(());
        let mut creation = crate::connector::read_stack::ConnectorReadSchemaCreation::new(
            storage.clone(),
            &mut request,
            &mut work,
        );
        let source = ConnectorWriteRewriteSource::try_new_typed(
            ConnectorTableHandle::try_new(
                crate::connector::ConnectorInstanceId::parse("rewrite-source-test").unwrap(),
                bytes::Bytes::from_static(b"source"),
            )
            .unwrap(),
            ConnectorPinnedFileSet::try_new("db", "table", 7, ["source.parquet"]).unwrap(),
            [7; 32],
            original,
            execution,
            vec![ConnectorRowMutationScanBinding::new(token, 0)],
            vec![token],
            None,
            &mut creation,
        )
        .unwrap();
        let clone = source.clone();
        assert_eq!(
            source.scan_bindings().as_ptr(),
            clone.scan_bindings().as_ptr()
        );
        assert_eq!(
            source.match_tokens().as_ptr(),
            clone.match_tokens().as_ptr()
        );
        let late = clone.scan_execution_schema().binding(0).unwrap().clone();
        assert_eq!(
            late.value_type().child_type_at(0).unwrap().domain(),
            Some(ValueLogicalType::Json)
        );
        drop(source);
        drop(clone);
        drop(creation);
        drop(storage);
        assert_eq!(exits.load(Ordering::SeqCst), 0);
        assert_eq!(
            late.value_type().child_type_at(0).unwrap().domain(),
            Some(ValueLogicalType::Json)
        );
        drop(late);
        assert_eq!(exits.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn borrowed_writer_fields_keep_all_original_enum_orders() {
        let field = |id: u8| {
            crate::connector::ConnectorWriteFieldBinding::new(
                ConnectorWriteFieldToken::from_bytes([id; 32]),
                arrow::datatypes::Field::new(
                    format!("field_{id}"),
                    arrow::datatypes::DataType::Int64,
                    true,
                ),
            )
        };
        let first = vec![field(1), field(2)];
        let second = vec![field(3), field(4)];
        let inputs = [
            ConnectorWriteInputShape::Data {
                fields: first.clone(),
            },
            ConnectorWriteInputShape::RowLineage {
                data_fields: first.clone(),
                row_identity_fields: second.clone(),
            },
            ConnectorWriteInputShape::PositionDelete {
                identity_fields: first.clone(),
                partition_source_fields: second.clone(),
            },
            ConnectorWriteInputShape::DeletionVector {
                identity_fields: first.clone(),
                partition_source_fields: second.clone(),
            },
            ConnectorWriteInputShape::EqualityDelete {
                equality_fields: first,
            },
        ];
        for input in inputs {
            let borrowed = borrowed_write_input_fields(&input)
                .map(|f| f.token())
                .collect::<Vec<_>>();
            let public = input
                .fields()
                .into_iter()
                .map(|f| f.token())
                .collect::<Vec<_>>();
            assert_eq!(borrowed, public);
        }
    }

    #[test]
    fn borrowed_route_validation_preserves_original_first_error_and_messages() {
        let input = cow_input();
        let contract = cow_contract();
        let mut wrong = cow_route([1, 0]);
        wrong.input_ordinals.clear();
        wrong.selection_bindings = Arc::from([]);
        let first = wrong
            .validate_selection_contract(&input, &contract)
            .unwrap_err();
        assert_eq!(first.kind(), ConnectorErrorKind::InvalidRequest);
        assert_eq!(
            first.message(),
            "row-mutation route input token order differs from its target input"
        );
        let mut missing = cow_route([1, 0]);
        missing.selection_bindings = Arc::from([]);
        let second = missing
            .validate_selection_contract(&input, &contract)
            .unwrap_err();
        assert_eq!(
            second.message(),
            "COW route omits its provider-signed selection bindings"
        );
    }

    use super::*;
    use crate::connector::write_stack::adapter::{ProviderWriteRuntime, WriteRuntimeAdapter};
    use crate::connector::{
        CatalogHandle, CatalogVersion, ConnectorInstanceDescriptor, ConnectorInstanceId,
        ConnectorMutationEffectField, ConnectorMutationSourceField, ConnectorMutationTargetField,
        ConnectorProviderBindingKey, ConnectorProviderId, ConnectorTableHandle,
        ConnectorWriteBaseVersion, ConnectorWriteFieldBinding, ConnectorWriteFieldToken,
        ProviderBindingEpoch,
    };
    use arrow::array::{Int8Array, Int64Array};
    use arrow::datatypes::Schema;
    use arrow::record_batch::RecordBatch;

    #[derive(Clone, Debug)]
    struct Value;

    struct FakeProvider {
        descriptor: ConnectorInstanceDescriptor,
        catalog_handle: CatalogHandle,
    }

    impl ProviderWriteRuntime for FakeProvider {
        type CommitHandle = Value;
        type WriterHandle = Value;
        type CommitFragment = Value;

        fn descriptor(&self) -> &ConnectorInstanceDescriptor {
            &self.descriptor
        }

        fn catalog_handle(&self) -> &CatalogHandle {
            &self.catalog_handle
        }
    }

    fn adapter() -> WriteRuntimeAdapter<FakeProvider> {
        let instance_id = ConnectorInstanceId::parse("session_unit").expect("instance id");
        WriteRuntimeAdapter::new(std::sync::Arc::new(FakeProvider {
            descriptor: ConnectorInstanceDescriptor {
                provider_id: ConnectorProviderId::parse("fake").expect("provider id"),
                instance_id: instance_id.clone(),
            },
            catalog_handle: CatalogHandle::new(instance_id, CatalogVersion::from_bytes([2; 32])),
        }))
    }

    fn input_shape() -> ConnectorWriteInputShape {
        ConnectorWriteInputShape::Data {
            fields: vec![
                ConnectorWriteFieldBinding::try_new_typed(
                    ConnectorWriteFieldToken::from_bytes([1; 32]),
                    arrow::datatypes::Field::new("v", arrow::datatypes::DataType::Int64, true),
                    novarocks_type_contract::FunctionValueType::new(
                        arrow::datatypes::DataType::Int64,
                        true,
                    ),
                )
                .expect("complete fixture declaration"),
            ],
        }
    }

    fn route(key: u8) -> ConnectorWriteRouteFacts {
        ConnectorWriteRouteFacts::try_new(
            ConnectorWriteRouteId::from_bytes([key; 32]),
            vec![ConnectorRowMutationEffect::Delete],
            vec![ConnectorMutationRouteInput::new(
                ConnectorWriteFieldToken::from_bytes([1; 32]),
                0,
            )],
            crate::connector::write_stack::ConnectorWriteRouteRequirements {
                colocation: crate::connector::write_stack::ConnectorWriteColocation::None,
                layout: crate::connector::write_stack::ConnectorWriteLayoutPreference::None,
            },
            Vec::new(),
        )
        .expect("route facts")
    }

    fn cow_bigint_type() -> novarocks_type_contract::FunctionValueType {
        use novarocks_type_contract::{
            FunctionValueType, ValueKind, ValueLayout, ValueTypeRef, ValueTypeStorageLifetime,
        };
        let storage = ValueTypeStorageLifetime::pure();
        FunctionValueType::try_from_value(
            ValueTypeRef::try_new(
                ValueKind::Scalar(arrow::datatypes::DataType::Int64),
                true,
                storage.clone(),
            )
            .unwrap(),
            ValueLayout::try_new(&arrow::datatypes::DataType::Int64, storage).unwrap(),
        )
        .unwrap()
    }

    fn cow_contract() -> ConnectorMutationMatchContract {
        let instance = ConnectorInstanceId::parse("session_unit").expect("instance");
        let identity = ConnectorMutationSourceField::try_new_typed(
            ConnectorWriteFieldToken::from_bytes([3; 32]),
            arrow::datatypes::Field::new("same", arrow::datatypes::DataType::Int64, true),
            0,
            cow_bigint_type(),
        )
        .expect("explicit nullable BIGINT identity");
        ConnectorMutationMatchContract::try_new(
            ConnectorProviderBindingKey {
                instance_id: instance.clone(),
                incarnation: ProviderBindingEpoch::from_bytes([7; 16]),
            },
            ConnectorTableHandle::try_new(instance, bytes::Bytes::from_static(b"table"))
                .expect("table"),
            ConnectorWriteBaseVersion::try_new(bytes::Bytes::from_static(b"base")).expect("base"),
            vec![identity.clone()],
            Vec::new(),
            vec![
                ConnectorMutationTargetField::try_new_typed(
                    ConnectorWriteFieldToken::from_bytes([4; 32]),
                    arrow::datatypes::Field::new("same", arrow::datatypes::DataType::Int64, true),
                    1,
                    cow_bigint_type(),
                )
                .expect("explicit nullable BIGINT after"),
            ],
            vec![identity.token()],
            ConnectorMutationEffectField::try_new(
                ConnectorWriteFieldToken::from_bytes([5; 32]),
                arrow::datatypes::Field::new("effect", arrow::datatypes::DataType::Int8, false),
                2,
            )
            .expect("effect"),
        )
        .expect("contract")
    }

    fn cow_input() -> ConnectorWriteInputShape {
        ConnectorWriteInputShape::RowLineage {
            data_fields: vec![ConnectorWriteFieldBinding::new(
                ConnectorWriteFieldToken::from_bytes([1; 32]),
                arrow::datatypes::Field::new("same", arrow::datatypes::DataType::Int64, true),
            )],
            row_identity_fields: vec![ConnectorWriteFieldBinding::new(
                ConnectorWriteFieldToken::from_bytes([2; 32]),
                arrow::datatypes::Field::new("same", arrow::datatypes::DataType::Int64, true),
            )],
        }
    }

    fn cow_selection(effects: &[ConnectorRowMutationEffect]) -> ConnectorRowMutationSelection {
        let contract = cow_contract();
        let schema = Arc::new(Schema::new(vec![
            contract.identity_fields()[0].field().clone(),
            contract.after_fields()[0].field().clone(),
            contract.effect_field().field().clone(),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from_iter_values(0..effects.len() as i64)),
                Arc::new(Int64Array::from_iter_values(10..10 + effects.len() as i64)),
                Arc::new(Int8Array::from_iter_values(
                    effects.iter().map(|effect| *effect as i8),
                )),
            ],
        )
        .expect("selection batch");
        ConnectorRowMutationSelection::try_new(schema, vec![batch], 1024, 1 << 20)
            .expect("selection")
    }

    fn cow_append_route() -> ConnectorWriteRouteFacts {
        ConnectorWriteRouteFacts::try_new(
            ConnectorWriteRouteId::from_bytes([18; 32]),
            vec![ConnectorRowMutationEffect::Insert],
            vec![ConnectorMutationRouteInput::new(
                ConnectorWriteFieldToken::from_bytes([1; 32]),
                0,
            )],
            crate::connector::write_stack::ConnectorWriteRouteRequirements {
                colocation: crate::connector::write_stack::ConnectorWriteColocation::None,
                layout: crate::connector::write_stack::ConnectorWriteLayoutPreference::None,
            },
            vec![ConnectorWriteSelectionBinding::new(
                ConnectorWriteFieldToken::from_bytes([1; 32]),
                ConnectorWriteFieldToken::from_bytes([4; 32]),
                1,
                ConnectorWriteSelectionBindingRole::AfterImage,
            )],
        )
        .expect("append route")
    }

    fn cow_route(ordinals: [u32; 2]) -> ConnectorWriteRouteFacts {
        ConnectorWriteRouteFacts::try_new(
            ConnectorWriteRouteId::from_bytes([8; 32]),
            vec![ConnectorRowMutationEffect::Replace],
            vec![
                ConnectorMutationRouteInput::new(
                    ConnectorWriteFieldToken::from_bytes([1; 32]),
                    ordinals[0],
                ),
                ConnectorMutationRouteInput::new(
                    ConnectorWriteFieldToken::from_bytes([2; 32]),
                    ordinals[1],
                ),
            ],
            crate::connector::write_stack::ConnectorWriteRouteRequirements {
                colocation: crate::connector::write_stack::ConnectorWriteColocation::None,
                layout: crate::connector::write_stack::ConnectorWriteLayoutPreference::None,
            },
            vec![
                ConnectorWriteSelectionBinding::new(
                    ConnectorWriteFieldToken::from_bytes([1; 32]),
                    ConnectorWriteFieldToken::from_bytes([4; 32]),
                    1,
                    ConnectorWriteSelectionBindingRole::AfterImage,
                ),
                ConnectorWriteSelectionBinding::new(
                    ConnectorWriteFieldToken::from_bytes([2; 32]),
                    ConnectorWriteFieldToken::from_bytes([3; 32]),
                    0,
                    ConnectorWriteSelectionBindingRole::Identity,
                ),
            ],
        )
        .expect("COW route")
    }

    fn target(
        adapter: &WriteRuntimeAdapter<FakeProvider>,
        ordinal: u32,
    ) -> ConnectorWriteTargetPlan {
        ConnectorWriteTargetPlan::new(
            WriteTargetOrdinal::try_new(ordinal).expect("bounded ordinal"),
            adapter.wrap_writer_handle(Value),
            input_shape(),
        )
    }

    fn statistics_requirement(
        ordinal: usize,
        name: &str,
        value_type: novarocks_type_contract::FunctionValueType,
        field_id: i32,
    ) -> StatisticsRequiredAggregation {
        StatisticsRequiredAggregation::try_new(
            crate::connector::StatisticsScanColumn::try_new(
                ordinal,
                Arc::<str>::from(name),
                value_type,
            )
            .expect("scan column"),
            "$test_stat",
            crate::connector::StatisticsArtifactIdentity::try_new(vec![field_id], "test/blob")
                .expect("identity"),
        )
        .expect("requirement")
    }

    #[test]
    fn statistics_contract_binds_the_exact_target_field() {
        let input = input_shape();
        let contract = WriteStatisticsContract::try_new(
            &input,
            vec![statistics_requirement(
                0,
                "v",
                novarocks_type_contract::FunctionValueType::new(
                    arrow::datatypes::DataType::Int64,
                    true,
                ),
                1,
            )],
        )
        .expect("contract");
        assert_eq!(contract.requirements().len(), 1);

        for mismatch in [
            statistics_requirement(
                0,
                "V",
                novarocks_type_contract::FunctionValueType::new(
                    arrow::datatypes::DataType::Int64,
                    true,
                ),
                1,
            ),
            statistics_requirement(
                0,
                "v",
                novarocks_type_contract::FunctionValueType::new(
                    arrow::datatypes::DataType::Int32,
                    true,
                ),
                1,
            ),
            statistics_requirement(
                0,
                "v",
                novarocks_type_contract::FunctionValueType::new(
                    arrow::datatypes::DataType::Int64,
                    false,
                ),
                1,
            ),
            statistics_requirement(
                1,
                "v",
                novarocks_type_contract::FunctionValueType::new(
                    arrow::datatypes::DataType::Int64,
                    true,
                ),
                1,
            ),
        ] {
            assert_eq!(
                WriteStatisticsContract::try_new(&input, vec![mismatch])
                    .expect_err("mismatched field")
                    .kind(),
                ConnectorErrorKind::InvalidRequest
            );
        }
    }

    fn statistics_input_shape(
        field: arrow::datatypes::Field,
        value: novarocks_type_contract::FunctionValueType,
    ) -> ConnectorWriteInputShape {
        ConnectorWriteInputShape::Data {
            fields: vec![
                ConnectorWriteFieldBinding::try_new_typed(
                    ConnectorWriteFieldToken::from_bytes([1; 32]),
                    field,
                    value,
                )
                .expect("independent statistics declaration"),
            ],
        }
    }

    #[test]
    fn statistics_contract_keeps_uuid_and_variant_root_identity() {
        use novarocks_type_contract::{FunctionValueType, ValueLogicalType};

        for (carrier, logical) in [
            (
                arrow::datatypes::DataType::FixedSizeBinary(16),
                ValueLogicalType::Uuid,
            ),
            (
                arrow::datatypes::DataType::LargeBinary,
                ValueLogicalType::Variant,
            ),
        ] {
            let semantic = FunctionValueType::try_with_logical_type(carrier.clone(), true, logical)
                .expect("authored semantic input");
            let physical = FunctionValueType::new(carrier, true);
            let semantic_input = statistics_input_shape(
                semantic.try_to_field("v").expect("semantic source field"),
                semantic.clone(),
            );
            let contract = WriteStatisticsContract::try_new(
                &semantic_input,
                vec![statistics_requirement(0, "v", semantic.clone(), 1)],
            )
            .expect("exact semantic statistics contract");
            assert_eq!(contract.requirements()[0].input().value_type(), &semantic);

            let physical_input = statistics_input_shape(
                physical.try_to_field("v").expect("physical field"),
                physical.clone(),
            );
            WriteStatisticsContract::try_new(
                &physical_input,
                vec![statistics_requirement(0, "v", physical.clone(), 1)],
            )
            .expect("exact physical statistics contract");

            for (input, declared) in [(&semantic_input, physical), (&physical_input, semantic)] {
                let error = WriteStatisticsContract::try_new(
                    input,
                    vec![statistics_requirement(0, "v", declared, 1)],
                )
                .expect_err("same carrier cannot erase or establish a root identity");
                assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
                assert_eq!(
                    error.message(),
                    "write statistics aggregate input does not exactly match the target field",
                );
            }
        }
    }

    #[test]
    fn statistics_contract_does_not_read_source_attributes_as_value_identity() {
        use novarocks_type_contract::{FunctionValueType, NR_LOGICAL_TYPE_KEY};
        for (carrier, tag) in [
            (arrow::datatypes::DataType::FixedSizeBinary(16), "unknown"),
            (arrow::datatypes::DataType::LargeBinary, "uuid"),
            (arrow::datatypes::DataType::FixedSizeBinary(16), "variant"),
        ] {
            let field = arrow::datatypes::Field::new("v", carrier.clone(), true).with_metadata(
                std::collections::HashMap::<String, String>::from([(
                    NR_LOGICAL_TYPE_KEY.to_owned(),
                    tag.to_owned(),
                )]),
            );
            let value = FunctionValueType::new(carrier, true);
            let input = statistics_input_shape(field, value.clone());
            let contract = WriteStatisticsContract::try_new(
                &input,
                vec![statistics_requirement(0, "v", value.clone(), 1)],
            )
            .expect("source attributes cannot change an explicit physical declaration");
            assert_eq!(contract.requirements()[0].input().value_type(), &value);
            assert_eq!(
                input.fields()[0].field().metadata()[NR_LOGICAL_TYPE_KEY],
                tag
            );
        }
    }

    #[test]
    fn statistics_contract_requires_a_complete_target_declaration() {
        let input = ConnectorWriteInputShape::Data {
            fields: vec![ConnectorWriteFieldBinding::new(
                ConnectorWriteFieldToken::from_bytes([1; 32]),
                arrow::datatypes::Field::new("v", arrow::datatypes::DataType::Int64, true),
            )],
        };
        let error = WriteStatisticsContract::try_new(
            &input,
            vec![statistics_requirement(
                0,
                "v",
                novarocks_type_contract::FunctionValueType::new(
                    arrow::datatypes::DataType::Int64,
                    true,
                ),
                1,
            )],
        )
        .expect_err("source field alone is not an execution author");
        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
        assert_eq!(
            error.message(),
            "write statistics target field has no complete execution declaration"
        );
    }

    #[test]
    fn statistics_contract_rejects_duplicate_artifact_identity() {
        let input = input_shape();
        let requirement = statistics_requirement(
            0,
            "v",
            novarocks_type_contract::FunctionValueType::new(
                arrow::datatypes::DataType::Int64,
                true,
            ),
            1,
        );
        assert_eq!(
            WriteStatisticsContract::try_new(&input, vec![requirement.clone(), requirement])
                .expect_err("duplicate identity")
                .kind(),
            ConnectorErrorKind::InvalidRequest
        );
    }

    #[test]
    fn a_route_that_accepts_nothing_would_silently_drop_its_rows() {
        assert_eq!(
            ConnectorWriteRouteFacts::try_new(
                ConnectorWriteRouteId::from_bytes([1; 32]),
                Vec::new(),
                Vec::new(),
                crate::connector::write_stack::ConnectorWriteRouteRequirements {
                    colocation: crate::connector::write_stack::ConnectorWriteColocation::None,
                    layout: crate::connector::write_stack::ConnectorWriteLayoutPreference::None
                },
                Vec::new(),
            )
            .expect_err("no accepted effects")
            .kind(),
            ConnectorErrorKind::InvalidRequest
        );
    }

    #[test]
    fn a_session_routes_every_branch_or_none() {
        let adapter = adapter();
        let commit = adapter.wrap_commit_handle(Value);

        // None routed: an ordinary write.
        assert!(ConnectorWriteSessionPlan::try_new(commit, vec![target(&adapter, 0)]).is_ok());

        // All routed: a row mutation.
        let commit = adapter.wrap_commit_handle(Value);
        assert!(
            ConnectorWriteSessionPlan::try_new(
                commit,
                vec![
                    target(&adapter, 0).with_route(route(1)),
                    target(&adapter, 1).with_route(route(2)),
                ],
            )
            .is_ok()
        );

        // Half routed: SQL would have nowhere to send the rest.
        let commit = adapter.wrap_commit_handle(Value);
        assert_eq!(
            ConnectorWriteSessionPlan::try_new(
                commit,
                vec![
                    target(&adapter, 0).with_route(route(1)),
                    target(&adapter, 1)
                ],
            )
            .expect_err("partially routed")
            .kind(),
            ConnectorErrorKind::InvalidRequest
        );
    }

    #[test]
    fn two_branches_cannot_share_a_route_key() {
        let adapter = adapter();
        let commit = adapter.wrap_commit_handle(Value);
        assert_eq!(
            ConnectorWriteSessionPlan::try_new(
                commit,
                vec![
                    target(&adapter, 0).with_route(route(1)),
                    target(&adapter, 1).with_route(route(1)),
                ],
            )
            .expect_err("duplicate route key")
            .kind(),
            ConnectorErrorKind::InvalidRequest
        );
    }

    #[test]
    fn cow_route_preserves_provider_order_and_repeated_source_occurrences() {
        let input = cow_input();
        let reversed = cow_route([1, 0]);
        reversed
            .validate_selection_contract(&input, &cow_contract())
            .expect("exact token bridge");
        assert_eq!(
            reversed
                .input_ordinals()
                .iter()
                .map(ConnectorMutationRouteInput::input_ordinal)
                .collect::<Vec<_>>(),
            vec![1, 0]
        );

        let repeated = cow_route([0, 0]);
        repeated
            .validate_selection_contract(&input, &cow_contract())
            .expect("two route occurrences may reuse one producer ordinal");
        assert_eq!(repeated.input_ordinals()[0].input_ordinal(), 0);
        assert_eq!(repeated.input_ordinals()[1].input_ordinal(), 0);
    }

    #[test]
    fn cow_selection_bridge_is_token_exact_even_when_names_match() {
        let input = cow_input();
        let contract = cow_contract();
        let missing = ConnectorWriteRouteFacts::try_new(
            ConnectorWriteRouteId::from_bytes([9; 32]),
            vec![ConnectorRowMutationEffect::Replace],
            cow_route([1, 0]).input_ordinals().to_vec(),
            crate::connector::write_stack::ConnectorWriteRouteRequirements {
                colocation: crate::connector::write_stack::ConnectorWriteColocation::None,
                layout: crate::connector::write_stack::ConnectorWriteLayoutPreference::None,
            },
            Vec::new(),
        )
        .expect("route");
        assert_eq!(
            missing
                .validate_selection_contract(&input, &contract)
                .expect_err("missing bridge")
                .kind(),
            ConnectorErrorKind::InvalidRequest
        );

        let foreign = ConnectorWriteRouteFacts::try_new(
            ConnectorWriteRouteId::from_bytes([10; 32]),
            vec![ConnectorRowMutationEffect::Replace],
            cow_route([1, 0]).input_ordinals().to_vec(),
            crate::connector::write_stack::ConnectorWriteRouteRequirements {
                colocation: crate::connector::write_stack::ConnectorWriteColocation::None,
                layout: crate::connector::write_stack::ConnectorWriteLayoutPreference::None,
            },
            vec![
                ConnectorWriteSelectionBinding::new(
                    ConnectorWriteFieldToken::from_bytes([1; 32]),
                    ConnectorWriteFieldToken::from_bytes([6; 32]),
                    1,
                    ConnectorWriteSelectionBindingRole::AfterImage,
                ),
                cow_route([1, 0]).selection_bindings()[1],
            ],
        )
        .expect("route");
        assert_eq!(
            foreign
                .validate_selection_contract(&input, &contract)
                .expect_err("same field name cannot recover a foreign token")
                .kind(),
            ConnectorErrorKind::InvalidRequest
        );

        let mut tampered_bindings = cow_route([1, 0]).selection_bindings().to_vec();
        tampered_bindings[0] = ConnectorWriteSelectionBinding::new(
            ConnectorWriteFieldToken::from_bytes([1; 32]),
            ConnectorWriteFieldToken::from_bytes([4; 32]),
            0,
            ConnectorWriteSelectionBindingRole::AfterImage,
        );
        let tampered = ConnectorWriteRouteFacts::try_new(
            ConnectorWriteRouteId::from_bytes([13; 32]),
            vec![ConnectorRowMutationEffect::Replace],
            cow_route([1, 0]).input_ordinals().to_vec(),
            crate::connector::write_stack::ConnectorWriteRouteRequirements {
                colocation: crate::connector::write_stack::ConnectorWriteColocation::None,
                layout: crate::connector::write_stack::ConnectorWriteLayoutPreference::None,
            },
            tampered_bindings,
        )
        .expect("route");
        assert_eq!(
            tampered
                .validate_selection_contract(&input, &contract)
                .expect_err("tampered selection ordinal")
                .kind(),
            ConnectorErrorKind::InvalidRequest
        );
    }

    #[test]
    fn cow_selection_occurrence_can_feed_multiple_writer_occurrences() {
        let input = ConnectorWriteInputShape::Data {
            fields: vec![
                ConnectorWriteFieldBinding::new(
                    ConnectorWriteFieldToken::from_bytes([1; 32]),
                    arrow::datatypes::Field::new("first", arrow::datatypes::DataType::Int64, true),
                ),
                ConnectorWriteFieldBinding::new(
                    ConnectorWriteFieldToken::from_bytes([2; 32]),
                    arrow::datatypes::Field::new("second", arrow::datatypes::DataType::Int64, true),
                ),
            ],
        };
        let route = ConnectorWriteRouteFacts::try_new(
            ConnectorWriteRouteId::from_bytes([11; 32]),
            vec![ConnectorRowMutationEffect::Replace],
            vec![
                ConnectorMutationRouteInput::new(ConnectorWriteFieldToken::from_bytes([1; 32]), 0),
                ConnectorMutationRouteInput::new(ConnectorWriteFieldToken::from_bytes([2; 32]), 0),
            ],
            crate::connector::write_stack::ConnectorWriteRouteRequirements {
                colocation: crate::connector::write_stack::ConnectorWriteColocation::None,
                layout: crate::connector::write_stack::ConnectorWriteLayoutPreference::None,
            },
            vec![
                ConnectorWriteSelectionBinding::new(
                    ConnectorWriteFieldToken::from_bytes([1; 32]),
                    ConnectorWriteFieldToken::from_bytes([4; 32]),
                    1,
                    ConnectorWriteSelectionBindingRole::AfterImage,
                ),
                ConnectorWriteSelectionBinding::new(
                    ConnectorWriteFieldToken::from_bytes([2; 32]),
                    ConnectorWriteFieldToken::from_bytes([4; 32]),
                    1,
                    ConnectorWriteSelectionBindingRole::AfterImage,
                ),
            ],
        )
        .expect("route");
        route
            .validate_selection_contract(&input, &cow_contract())
            .expect("one signed selection occurrence may feed two writer occurrences");
    }

    #[test]
    fn provider_derived_value_requires_nullable_inherit_marker() {
        let writer = ConnectorWriteFieldToken::from_bytes([2; 32]);
        let input = |nullable| ConnectorWriteInputShape::RowLineage {
            data_fields: Vec::new(),
            row_identity_fields: vec![ConnectorWriteFieldBinding::new(
                writer,
                arrow::datatypes::Field::new(
                    "new_version",
                    arrow::datatypes::DataType::Int64,
                    nullable,
                ),
            )],
        };
        let route = ConnectorWriteRouteFacts::try_new(
            ConnectorWriteRouteId::from_bytes([12; 32]),
            vec![ConnectorRowMutationEffect::Insert],
            vec![ConnectorMutationRouteInput::new(writer, 0)],
            crate::connector::write_stack::ConnectorWriteRouteRequirements {
                colocation: crate::connector::write_stack::ConnectorWriteColocation::None,
                layout: crate::connector::write_stack::ConnectorWriteLayoutPreference::None,
            },
            vec![ConnectorWriteSelectionBinding::provider_derived(
                writer,
                ConnectorWriteProviderDerivedValue::Inherit,
            )],
        )
        .expect("route");
        route
            .validate_selection_contract(&input(true), &cow_contract())
            .expect("nullable writer admits the inherit marker");
        assert_eq!(
            route
                .validate_selection_contract(&input(false), &cow_contract())
                .expect_err("non-null writer cannot carry the inherit marker")
                .kind(),
            ConnectorErrorKind::InvalidRequest
        );
    }

    #[test]
    fn cow_session_routing_proof_exactly_covers_selection_by_route_identity() {
        let adapter = adapter();
        let selection = cow_selection(&[
            ConnectorRowMutationEffect::Insert,
            ConnectorRowMutationEffect::Insert,
        ]);
        let route = cow_append_route();
        let proof = ConnectorWriteCohortRoutingProof::try_new(
            route.route_id(),
            selection.digest(),
            vec![
                ConnectorRowMutationSelectionOrdinal::new(0),
                ConnectorRowMutationSelectionOrdinal::new(1),
            ],
            ConnectorWriteCohortRoutingBody::Append,
        )
        .expect("proof");
        let input = ConnectorWriteInputShape::Data {
            fields: vec![cow_input().fields()[0].clone()],
        };
        let target = ConnectorWriteTargetPlan::new(
            WriteTargetOrdinal::try_new(0).expect("ordinal"),
            adapter.wrap_writer_handle(Value),
            input,
        )
        .with_route(route)
        .with_routing_proof(proof);
        ConnectorWriteSessionPlan::try_copy_on_write(
            adapter.wrap_commit_handle(Value),
            vec![target],
            selection,
            cow_contract(),
        )
        .expect("exact routing proof");
    }

    #[test]
    fn cow_session_routing_proof_fails_closed_on_missing_or_wrong_effect_coverage() {
        let adapter = adapter();
        let selection = cow_selection(&[
            ConnectorRowMutationEffect::Insert,
            ConnectorRowMutationEffect::Replace,
        ]);
        let route = cow_append_route();
        let proof = ConnectorWriteCohortRoutingProof::try_new(
            route.route_id(),
            selection.digest(),
            vec![ConnectorRowMutationSelectionOrdinal::new(0)],
            ConnectorWriteCohortRoutingBody::Append,
        )
        .expect("partial proof");
        let input = ConnectorWriteInputShape::Data {
            fields: vec![cow_input().fields()[0].clone()],
        };
        let target = ConnectorWriteTargetPlan::new(
            WriteTargetOrdinal::try_new(0).expect("ordinal"),
            adapter.wrap_writer_handle(Value),
            input,
        )
        .with_route(route)
        .with_routing_proof(proof);
        assert_eq!(
            ConnectorWriteSessionPlan::try_copy_on_write(
                adapter.wrap_commit_handle(Value),
                vec![target],
                selection,
                cow_contract(),
            )
            .expect_err("partial coverage")
            .kind(),
            ConnectorErrorKind::InvalidRequest
        );
    }
}

#[cfg(test)]
mod needed_ready_tests {
    use super::*;
    use crate::connector::{ExternalMutationEffect, ExternalMutationFinalization};
    use std::task::{Context, Poll, Wake, Waker};

    fn actual_ready() -> Arc<ConnectorWriteNeededReady> {
        let ConnectorWritePublicationCompletion::KnownCommitted { needed, .. } =
            ConnectorWritePublicationCompletion::from_outcome(
                ExternalMutationOutcome::KnownCommitted {
                    effect: ExternalMutationEffect::Applied,
                    receipt: ConnectorWriteReceipt::try_new(bytes::Bytes::from_static(
                        b"actual receipt",
                    ))
                    .unwrap(),
                    finalization: ExternalMutationFinalization::Complete,
                    cleanup: ExternalMutationFinalization::Complete,
                },
            )
        else {
            unreachable!()
        };
        needed
    }
    #[test]
    fn canonical_frontier_moves_full_failure_and_payload_without_cloning_them() {
        let payload = bytes::Bytes::from(vec![7; 128]);
        let payload_pointer = payload.as_ptr();
        let original_error = ConnectorError::new(
            ConnectorErrorKind::Unavailable,
            String::from("original necessary finalization cause"),
        );
        let cause_pointer = original_error.message().as_ptr();
        let completion = ConnectorWritePublicationCompletion::from_committed_frontier(
            ExternalMutationEffect::Applied,
            ConnectorWriteReceipt::try_new(payload).unwrap(),
            ExternalMutationFinalization::Failed(
                crate::connector::ConnectorMutationFailure::from_connector(original_error),
            ),
        );
        let ConnectorWritePublicationCompletion::KnownCommitted { needed, cleanup } = &completion
        else {
            unreachable!()
        };
        assert_eq!(cleanup, &ConnectorWriteCleanupState::Pending);
        assert_eq!(needed.receipt().payload().as_ptr(), payload_pointer);
        let ExternalMutationFinalization::Failed(failure) = needed.finalization() else {
            unreachable!()
        };
        assert_eq!(
            failure.connector_source().unwrap().message().as_ptr(),
            cause_pointer
        );
        let receiving = ConnectorWriteNeededReadyReceiving::new();
        receiving.publish(Arc::clone(needed)).unwrap();
        let mut stable = None;
        receiving.handoff_in(&mut stable).unwrap().notify();
        assert!(Arc::ptr_eq(needed, stable.as_ref().unwrap()));
        // A retained frontier must never manufacture cleanup completion in a
        // legacy result. Its real typed fact remains with the returned value.
        assert!(matches!(
            completion.into_legacy_outcome(),
            Err(ConnectorWritePublicationCompletion::KnownCommitted {
                cleanup: ConnectorWriteCleanupState::Pending,
                ..
            })
        ));
    }
    struct PanicWake;
    impl Wake for PanicWake {
        fn wake(self: Arc<Self>) {
            panic!("scripted original cleanup wake failed");
        }
    }
    #[test]
    fn handoff_stores_actual_fact_and_permission_before_foreign_wake() {
        let receiving = ConnectorWriteNeededReadyReceiving::new();
        let waker = Waker::from(Arc::new(PanicWake));
        let mut cx = Context::from_waker(&waker);
        receiving.publish(actual_ready()).unwrap();
        assert_eq!(receiving.poll_cleanup_permitted(&mut cx), Poll::Pending);
        let mut stable = None;
        let notice = receiving.handoff_in(&mut stable).unwrap();
        assert!(stable.is_some());
        assert!(receiving.cleanup_is_permitted());
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| notice.notify())).is_err()
        );
        assert!(Arc::ptr_eq(
            stable.as_ref().unwrap(),
            &receiving.observed().unwrap()
        ));
        assert_eq!(receiving.poll_cleanup_permitted(&mut cx), Poll::Ready(()));
        assert!(receiving.handoff_in(&mut None).is_err());
        assert!(receiving.publish(actual_ready()).is_err());
    }
    #[test]
    fn absence_and_occupied_target_cannot_authorize_cleanup() {
        let receiving = ConnectorWriteNeededReadyReceiving::new();
        assert!(receiving.handoff_in(&mut None).is_err());
        assert!(!receiving.cleanup_is_permitted());
        receiving.publish(actual_ready()).unwrap();
        let mut occupied = Some(actual_ready());
        assert!(receiving.handoff_in(&mut occupied).is_err());
        assert!(!receiving.cleanup_is_permitted());
        assert_eq!(
            ConnectorWriteNeededReadyReceiving::known_layouts()
                .unwrap()
                .len(),
            2
        );
    }
}
