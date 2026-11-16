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
//! One `begin_write` atomically returns a frontend-only commit session and the
//! complete set of logical writer handles the sealed plan may use. There is no
//! separate prepare, activate, or placement-dependent planning step: a writer
//! recipe is a property of the logical target, not of where the plan happens to
//! run.
//!
//! Only the frontend, holding its exact control generation, may finish, abort,
//! or reconcile a write. A backend has no commit session and no catalog
//! mutation capability at all.

use std::sync::Arc;

use arrow::datatypes::DataType;
use novarocks_connector_contract::ConnectorMutationContributionGroupId;

use crate::connector::distributed_rewrite::ConnectorDistributedRewriteShape;
use crate::connector::write_stack::prepared::ConnectorPreparedWriteSet;
use crate::connector::write_stack::runtime::{
    ConnectorWriteBinding, ConnectorWriteCommitSession, ConnectorWriterHandle,
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
    ExternalMutationEvidence, ExternalMutationOutcome, StatisticsArtifactDraft,
    StatisticsRequiredAggregation,
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
        declaration: Box<ConnectorDocumentPublicationDeclaration>,
        shape: ConnectorManagedPublicationShape,
    },
    /// A merge-on-read row mutation. The provider decides how many branches
    /// the mutation needs and what each accepts; SQL routes rows to them.
    RowMutation,
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
    position_materialization: Option<ConnectorMutationContributionGroup>,
    rewrite_group_digest: Option<[u8; 32]>,
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
        Ok(())
    }
}

/// The raw position routes that feed one final materialization writer.
/// The target retains its own writer input independently of this shape.
#[derive(Clone, Debug)]
pub struct ConnectorMutationContributionGroup {
    id: ConnectorMutationContributionGroupId,
    raw_input: ConnectorWriteInputShape,
    file_key: ConnectorWriteFieldToken,
    routes: Vec<ConnectorWriteRouteFacts>,
}

impl ConnectorMutationContributionGroup {
    pub fn try_new(
        id: ConnectorMutationContributionGroupId,
        raw_input: ConnectorWriteInputShape,
        file_key: ConnectorWriteFieldToken,
        routes: Vec<ConnectorWriteRouteFacts>,
    ) -> Result<Self, ConnectorError> {
        raw_input.validate()?;
        let fields = raw_input.fields_iter().collect::<Vec<_>>();
        let exact_layout = matches!(fields.as_slice(), [file, position, spec_id, partition]
            if file.token() == file_key
                && required_type(file.field(), &DataType::Utf8)
                && required_type(position.field(), &DataType::Int64)
                && required_type(spec_id.field(), &DataType::Int32)
                && nullable_partition(partition.field()));
        if id.to_bytes() == [0; 32]
            || !matches!(&raw_input, ConnectorWriteInputShape::PositionDelete { .. })
            || !exact_layout
            || routes.is_empty()
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "position materialization requires a group, raw position input and file key",
            ));
        }
        let mut ids = std::collections::BTreeSet::new();
        for route in &routes {
            route.validate_against_input(&raw_input)?;
            if route.route_id().to_bytes() == [0; 32]
                || !ids.insert(route.route_id())
                || route
                    .accepted_effects()
                    .contains(&ConnectorRowMutationEffect::Insert)
                || !matches!(
                    &route.requirements().colocation,
                    ConnectorWriteColocation::ByKey(keys) if keys.as_slice() == [file_key]
                )
            {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "position contribution route differs from its group file key or delete effects",
                ));
            }
        }
        Ok(Self {
            id,
            raw_input,
            file_key,
            routes,
        })
    }

    fn accepts_writer_input(&self, writer: &ConnectorWriteInputShape) -> bool {
        if !matches!(writer, ConnectorWriteInputShape::DeletionVector { .. }) {
            return false;
        }
        let fields = writer.fields_iter().collect::<Vec<_>>();
        let raw = self.raw_input.fields_iter().collect::<Vec<_>>();
        matches!((fields.as_slice(), raw.as_slice()),
            ([file, spec_id, partition, driver, high32, container, index, count, bitmap],
                [raw_file, _, raw_spec, raw_partition])
                if file.field() == raw_file.field()
                    && spec_id.field() == raw_spec.field()
                    && partition.field() == raw_partition.field()
                    && required_type(driver.field(), &DataType::Int64)
                    && required_type(high32.field(), &DataType::Int32)
                    && required_type(container.field(), &DataType::Int32)
                    && required_type(index.field(), &DataType::Int64)
                    && required_type(count.field(), &DataType::Int64)
                    && required_type(bitmap.field(), &DataType::Binary))
    }

    pub const fn id(&self) -> ConnectorMutationContributionGroupId {
        self.id
    }

    pub const fn raw_input(&self) -> &ConnectorWriteInputShape {
        &self.raw_input
    }

    pub const fn file_key(&self) -> ConnectorWriteFieldToken {
        self.file_key
    }

    pub fn routes(&self) -> &[ConnectorWriteRouteFacts] {
        &self.routes
    }
}

fn required_type(field: &arrow::datatypes::Field, ty: &DataType) -> bool {
    !field.is_nullable() && field.data_type() == ty
}

fn nullable_partition(field: &arrow::datatypes::Field) -> bool {
    field.is_nullable() && matches!(field.data_type(), DataType::Struct(_))
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
            position_materialization: None,
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

    pub fn with_position_materialization(
        mut self,
        group: ConnectorMutationContributionGroup,
    ) -> Self {
        self.position_materialization = Some(group);
        self
    }

    pub const fn position_materialization(&self) -> Option<&ConnectorMutationContributionGroup> {
        self.position_materialization.as_ref()
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
    commit: ConnectorWriteCommitSession,
    targets: Vec<ConnectorWriteTargetPlan>,
}

impl ConnectorWriteSessionPlan {
    /// Targets must be dense from zero and must all belong to the same exact
    /// provider generation as the commit session. A disagreement here means the
    /// session and the plan could name different runtimes, so it fails before
    /// any fragment is encoded.
    pub fn try_new(
        commit: ConnectorWriteCommitSession,
        targets: Vec<ConnectorWriteTargetPlan>,
    ) -> Result<Self, ConnectorError> {
        let ordinals = targets
            .iter()
            .map(ConnectorWriteTargetPlan::ordinal)
            .collect::<Vec<_>>();
        validate_dense_target_ordinals(&ordinals)?;
        let mut groups = std::collections::BTreeSet::new();
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
            if let Some(group) = target.position_materialization()
                && (!groups.insert(group.id())
                    || target.route().is_some()
                    || !group.accepts_writer_input(target.input()))
            {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "position contribution group requires a unique materialization target",
                ));
            }
        }
        // Routing is a property of the whole session, not of individual
        // branches: if some branches carry routing facts and others do not, SQL
        // can route rows to part of the write and silently has nowhere to send
        // the rest.
        let routed = targets
            .iter()
            .filter(|target| {
                target.route().is_some() || target.position_materialization().is_some()
            })
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
            let direct = target.route().into_iter();
            let grouped = target
                .position_materialization()
                .into_iter()
                .flat_map(|group| group.routes());
            if direct
                .chain(grouped)
                .any(|route| !seen.insert(route.route_id()))
            {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "a connector write session repeats a row-mutation route key",
                ));
            }
        }
        Ok(Self { commit, targets })
    }

    pub const fn binding(&self) -> &ConnectorWriteBinding {
        self.commit.binding()
    }

    pub const fn commit_session(&self) -> &ConnectorWriteCommitSession {
        &self.commit
    }

    pub fn targets(&self) -> &[ConnectorWriteTargetPlan] {
        &self.targets
    }

    /// The sealed ordinal set a prepared write set must not exceed.
    pub fn expected_targets(&self) -> Vec<WriteTargetOrdinal> {
        self.targets
            .iter()
            .map(ConnectorWriteTargetPlan::ordinal)
            .collect()
    }

    pub fn into_parts(self) -> (ConnectorWriteCommitSession, Vec<ConnectorWriteTargetPlan>) {
        (self.commit, self.targets)
    }
}

/// Commit one complete prepared write set.
///
/// The commit session is borrowed, never moved: a frontend session keeps it for
/// a possible abort or reconcile, and nothing else in the process can take
/// ownership of it.
pub struct ConnectorWriteFinishRequest<'a> {
    pub commit: &'a ConnectorWriteCommitSession,
    pub prepared: ConnectorPreparedWriteSet,
    pub statistics: Vec<WriteStatisticsArtifact>,
    pub publication: ConnectorWriteFinishPublication,
    pub context: ConnectorRequestContext,
}

/// Borrowed view of the original stable finish packet. Polling or dropping a
/// Provider future cannot destroy the caller's admitted input or authority.
pub struct ConnectorWriteFinishRequestRef<'a> {
    pub commit: &'a ConnectorWriteCommitSession,
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
    pub commit: &'a ConnectorWriteCommitSession,
    pub context: ConnectorRequestContext,
    pub production: ConnectorWriteAbortProduction,
    /// Borrow only the original stable, complete received union. The Provider
    /// additionally checks its own known-uncommitted state and exact binding.
    pub received: Option<&'a ConnectorPreparedWriteSet>,
}

/// Resolve a commit whose external outcome is unknown.
pub struct ConnectorWriteSessionReconcileRequest<'a> {
    pub commit: &'a ConnectorWriteCommitSession,
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
        commit: &ConnectorWriteCommitSession,
    ) -> Result<crate::connector::ConnectorMetadataCleanupResponsibility, ConnectorError>;

    /// Observe only a finite finalization context already received by this
    /// exact original session. This must never begin a grant or derive a new
    /// deadline/stop. None is absence, not permission to use business control.
    fn existing_finalization_context(
        &self,
        _commit: &ConnectorWriteCommitSession,
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
        _commit: &ConnectorWriteCommitSession,
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
    fn borrowed_route_validation_preserves_original_input_order_error() {
        let input = input_shape();
        let mut wrong = route(1);
        wrong.input_ordinals.clear();
        let first = wrong.validate_against_input(&input).unwrap_err();
        assert_eq!(first.kind(), ConnectorErrorKind::InvalidRequest);
        assert_eq!(
            first.message(),
            "row-mutation route input token order differs from its target input"
        );
    }

    use super::*;
    use crate::connector::write_stack::adapter::{ProviderWriteRuntime, WriteRuntimeAdapter};
    use crate::connector::{
        CatalogHandle, CatalogVersion, ConnectorInstanceDescriptor, ConnectorInstanceId,
        ConnectorProviderId, ConnectorWriteFieldBinding, ConnectorWriteFieldToken,
    };

    #[derive(Clone, Debug)]
    struct Value;

    struct FakeProvider {
        descriptor: ConnectorInstanceDescriptor,
        catalog_handle: CatalogHandle,
    }

    impl ProviderWriteRuntime for FakeProvider {
        type CommitSession = Value;
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
        )
        .expect("route facts")
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
    fn route_allows_distinct_tokens_to_share_one_producer_ordinal() {
        let ConnectorWriteInputShape::Data { mut fields } = input_shape() else {
            panic!("data input fixture");
        };
        let first = fields[0].token();
        let second = ConnectorWriteFieldToken::from_bytes([32; 32]);
        fields.push(
            ConnectorWriteFieldBinding::try_new_typed(
                second,
                arrow::datatypes::Field::new("alias", DataType::Int64, true),
                novarocks_type_contract::FunctionValueType::new(DataType::Int64, true),
            )
            .expect("complete alias declaration"),
        );
        let input = ConnectorWriteInputShape::Data { fields };
        let requirements = ConnectorWriteRouteRequirements {
            colocation: ConnectorWriteColocation::ByKey(vec![second]),
            layout: ConnectorWriteLayoutPreference::ClusterBy(vec![first, second]),
        };
        let route = ConnectorWriteRouteFacts::try_new(
            ConnectorWriteRouteId::from_bytes([34; 32]),
            vec![ConnectorRowMutationEffect::Delete],
            vec![
                ConnectorMutationRouteInput::new(first, 0),
                ConnectorMutationRouteInput::new(second, 0),
            ],
            requirements.clone(),
        )
        .expect("distinct route tokens may share one producer occurrence");
        let adapter = adapter();
        let target = ConnectorWriteTargetPlan::new(
            WriteTargetOrdinal::try_new(0).expect("bounded target ordinal"),
            adapter.wrap_writer_handle(Value),
            input,
        )
        .with_route(route);
        let session =
            ConnectorWriteSessionPlan::try_new(adapter.wrap_commit_session(Value), vec![target])
                .expect("shared producer occurrence remains a valid routed session");
        let route = session.targets()[0]
            .route()
            .expect("routing facts retained");
        assert_eq!(
            route
                .input_ordinals()
                .iter()
                .map(|input| input.token())
                .collect::<Vec<_>>(),
            vec![first, second]
        );
        assert_eq!(
            route
                .input_ordinals()
                .iter()
                .map(|input| input.input_ordinal())
                .collect::<Vec<_>>(),
            vec![0, 0]
        );
        assert_eq!(route.requirements(), &requirements);
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
            )
            .expect_err("no accepted effects")
            .kind(),
            ConnectorErrorKind::InvalidRequest
        );
    }

    #[test]
    fn position_group_binds_exact_raw_routes_and_a_separate_shard_input() {
        let binding = |id, name, ty: DataType, nullable| {
            ConnectorWriteFieldBinding::try_new_typed(
                ConnectorWriteFieldToken::from_bytes([id; 32]),
                arrow::datatypes::Field::new(name, ty.clone(), nullable),
                novarocks_type_contract::FunctionValueType::new(ty, nullable),
            )
            .unwrap()
        };
        let file = binding(1, "_file", DataType::Utf8, false);
        let pos = binding(2, "_pos", DataType::Int64, false);
        let spec = binding(3, "_spec_id", DataType::Int32, false);
        let part = binding(
            4,
            "_partition",
            DataType::Struct(arrow::datatypes::Fields::empty()),
            true,
        );
        let raw = ConnectorWriteInputShape::PositionDelete {
            identity_fields: vec![file.clone(), pos],
            partition_source_fields: vec![spec.clone(), part.clone()],
        };
        let shards = ConnectorWriteInputShape::DeletionVector {
            identity_fields: vec![file.clone(), spec, part],
            partition_source_fields: vec![
                binding(5, "driver", DataType::Int64, false),
                binding(6, "high32", DataType::Int32, false),
                binding(7, "container", DataType::Int32, false),
                binding(8, "index", DataType::Int64, false),
                binding(9, "count", DataType::Int64, false),
                binding(10, "bitmap", DataType::Binary, false),
            ],
        };
        let route = |id, effect| {
            ConnectorWriteRouteFacts::try_new(
                ConnectorWriteRouteId::from_bytes([id; 32]),
                vec![effect],
                raw.fields_iter()
                    .enumerate()
                    .map(|(ordinal, field)| {
                        ConnectorMutationRouteInput::new(field.token(), ordinal as u32)
                    })
                    .collect(),
                ConnectorWriteRouteRequirements {
                    colocation: ConnectorWriteColocation::ByKey(vec![file.token()]),
                    layout: ConnectorWriteLayoutPreference::None,
                },
            )
            .unwrap()
        };
        let delete = route(21, ConnectorRowMutationEffect::Delete);
        let replace = route(22, ConnectorRowMutationEffect::Replace);
        let group_id = ConnectorMutationContributionGroupId::from_bytes([31; 32]);
        let group = ConnectorMutationContributionGroup::try_new(
            group_id,
            raw.clone(),
            file.token(),
            vec![delete.clone(), replace],
        )
        .unwrap();
        assert_eq!(group.routes().len(), 2);
        assert!(group.accepts_writer_input(&shards));
        assert!(!group.accepts_writer_input(&raw));
        assert!(
            ConnectorMutationContributionGroup::try_new(
                group_id,
                raw.clone(),
                file.token(),
                vec![delete.clone(), delete],
            )
            .is_err()
        );
        assert!(
            ConnectorMutationContributionGroup::try_new(
                group_id,
                raw.clone(),
                file.token(),
                vec![route(23, ConnectorRowMutationEffect::Insert)],
            )
            .is_err()
        );
        let adapter = adapter();
        let target = ConnectorWriteTargetPlan::new(
            WriteTargetOrdinal::try_new(0).unwrap(),
            adapter.wrap_writer_handle(Value),
            shards,
        )
        .with_position_materialization(group);
        assert!(
            ConnectorWriteSessionPlan::try_new(adapter.wrap_commit_session(Value), vec![target])
                .is_ok()
        );
    }

    #[test]
    fn a_session_routes_every_branch_or_none() {
        let adapter = adapter();
        let commit = adapter.wrap_commit_session(Value);

        // None routed: an ordinary write.
        assert!(ConnectorWriteSessionPlan::try_new(commit, vec![target(&adapter, 0)]).is_ok());

        // All routed: a row mutation.
        let commit = adapter.wrap_commit_session(Value);
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
        let commit = adapter.wrap_commit_session(Value);
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
        let commit = adapter.wrap_commit_session(Value);
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
