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

//! The frontend-only write session.
//!
//! One `begin_write` on one exact control generation returns the commit handle
//! and the complete set of logical writer recipes the sealed plan may use.
//! Everything else about a distributed write hangs off that: the plan encodes
//! the recipes, the backends execute them, and this session -- and only this
//! session -- can turn the result into an external commit.
//!
//! Frozen writer/target occurrences pre-register potential output responsibility
//! before dispatch. They do not enumerate unknown BE-local objects or replace
//! complete End/domain validation, local sealing, or Provider disposition.
//!
//! The terminal decision is single-shot. A session that has committed cannot
//! abort, one that has aborted cannot commit, and the invocation counter makes
//! "the connector was never asked to commit" an assertable fact rather than an
//! inference.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use novarocks_proto_codec::connector_write::{
    ConnectorWriteFragmentDecoder, ConnectorWriteHandleEncoder,
};
use novarocks_proto_models::connector_write as write_dto;
use novarocks_spi::connector::write_stack::{
    ConnectorPreparedWriteSet, ConnectorWriteBeginRequest, ConnectorWriteFinishPublication,
    ConnectorWriteSessionAbortRequest, ConnectorWriteSessionFlavor, ConnectorWriteSessionPlan,
    ConnectorWriteSessionReconcileRequest, ConnectorWriteTargetPlan, PreparedWriteSetLedger,
    UniqueWriterHandleLedger, WriteRowCountAccumulator, WriteTargetOrdinal,
};
use novarocks_spi::connector::{
    ConnectorDocumentPublicationDeclaration, ConnectorError, ConnectorErrorKind,
    ConnectorRequestContext, ConnectorStorageResolver, ConnectorWriteAbortOutcome,
    ExternalMutationEvidence, KeyedPublicationIntent,
};

use crate::query_execution::write_result::DecodedPreparedWriteSet;
use novarocks_catalog_application::ConnectorWriteStackLease;
use novarocks_plan_codec::SealedWriteTargets;

#[path = "write_computation.rs"]
mod computation;
#[path = "write_production.rs"]
mod production;
#[path = "write_session/publication_call.rs"]
mod publication_call;
pub(crate) use computation::{
    AcceptedWriteSegment, SessionDecodedWriteSegment, WriteComputationDecode, WriteReceivingPort,
};
pub(crate) use publication_call::WritePublicationCall;

#[cfg(test)]
pub(crate) fn production_work_error_for_test(
    error: novarocks_workload_control::WorkError,
) -> ConnectorError {
    production::production_work_error(error)
}
#[path = "write_session/begin_receiving.rs"]
mod begin_receiving;
pub(crate) use begin_receiving::WriteBeginReceiving;

/// What a session has already decided. Recorded so a second, different
/// decision is refused rather than silently issuing two external effects.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TerminalDecision {
    Committed,
    Aborted,
    /// The commit may or may not have taken effect externally. Neither a
    /// retry nor an abort is safe from here; only reconcile is.
    CommitUnknown,
}

/// The application-document payload is not knowable when writer recipes are
/// admitted: it includes the exact read occurrences and the write outcome.
/// Keep its declaration frozen from begin, then permit one exact late bind
/// before the provider's terminal call.
enum WritePublicationState {
    Ordinary,
    Pending {
        declaration: ConnectorDocumentPublicationDeclaration,
    },
    Bound(KeyedPublicationIntent),
}

/// One distributed write's frontend session.
pub(crate) struct ConnectorWriteSession {
    decode_origin: Arc<()>,
    productions: Mutex<production::WriteProductionReceiving>,
    statistics_expected: Arc<
        std::collections::BTreeSet<(
            WriteTargetOrdinal,
            novarocks_spi::connector::StatisticsArtifactIdentity,
        )>,
    >,
    lease: ConnectorWriteStackLease,
    plan: ConnectorWriteSessionPlan,
    metadata_only_publication: bool,
    /// The application-owned publication attached to the provider's single
    /// external commit. Its declaration is frozen at begin, while its exact
    /// payload may be bound once after the data plane closes.
    finish_publication: Mutex<WritePublicationState>,
    /// The catalog runtime this session's writers execute against, kept whole
    /// rather than reduced to its handle: the backend leases a catalog from its
    /// properties, and a writer node that named a handle the query never leased
    /// cannot resolve a write runtime on the backend at all.
    catalog_properties: novarocks_spi::connector::CatalogProperties,
    accumulated: Mutex<AccumulatedWriteSet>,
    // One stable packet; borrowed Provider futures never uniquely own its input.
    publication_input: Mutex<Option<Arc<publication_call::PublicationInput>>>,
    terminal: Mutex<Option<TerminalDecision>>,
    finish_invocations: AtomicUsize,
    /// The frontend-local terminal router retains every original computation
    /// stage's storage capability through this Session's single publication.
    ///
    /// A distributed write reaches its external effect *after* its lifecycle
    /// attempt finalizes: the commit reloads table metadata and writes
    /// manifests through object storage once every participant has converged.
    /// The attempt's live storage authority already refuses a resolve by then,
    /// so on a vended-credential deployment this capability is the only thing
    /// the commit can read storage through -- and holding it is what defers
    /// the attempt's credential cleanup past finalization.
    ///
    /// It is cleared as soon as no further external decision can need storage,
    /// and dropping the session clears it too, so a write that neither commits
    /// nor reconciles cannot pin credential material.
    terminal_storage:
        Mutex<Option<Arc<crate::query_execution::lifecycle_plan::SessionTerminalCredentialRouter>>>,
    // Last holder retires after every session-owned plan and storage field.
    cow_original: Option<novarocks_spi::connector::ConnectorOriginalResultScope>,
}

/// What a session has collected so far across the queries it drives.
///
/// Most writes are one query, but a copy-on-write mutation and a distributed
/// rewrite drive several against one session and commit once at the end. Each
/// query still produces a set that is complete for its own execution graph;
/// what accumulates here is the statement's union.
///
/// The frozen budgets are charged on this union rather than per query. Charging
/// them per query would let a statement hold an unbounded amount before commit
/// while every individual query looked well inside its limit -- and the limits
/// exist to bound exactly what the frontend holds.
/// One immutable received union, shared by pure projection and publication.
/// Actual buffers and their receipts stay together until the last borrower exits.
struct ReceivedWriteInput {
    prepared: ConnectorPreparedWriteSet,
    statistics: Vec<novarocks_spi::connector::write_stack::WriteStatisticsArtifact>,
    _retention: Option<crate::query_execution::internal_result_cpu::InternalResultRetention>,
}

#[derive(Default)]
struct AccumulatedWriteSet {
    received: Option<Arc<ReceivedWriteInput>>,
    rows: WriteRowCountAccumulator,
    ledger: PreparedWriteSetLedger,
    fragments: Vec<(
        WriteTargetOrdinal,
        novarocks_spi::connector::write_stack::ConnectorCommitFragment,
    )>,
    statistics: Vec<novarocks_spi::connector::write_stack::WriteStatisticsArtifact>,
    statistics_identities: std::collections::BTreeSet<(
        WriteTargetOrdinal,
        novarocks_spi::connector::StatisticsArtifactIdentity,
    )>,
    statistics_body_bytes: usize,
    statistics_property_bytes: usize,
    retention: Option<crate::query_execution::internal_result_cpu::InternalResultRetention>,
}

impl ConnectorWriteSession {
    /// The statement owns `receiving` before polling this borrowed begin.
    /// Adopt the actual Provider result before dropping its original child F.
    pub(crate) async fn begin_async_in(
        lease: ConnectorWriteStackLease,
        catalog_properties: novarocks_spi::connector::CatalogProperties,
        request: ConnectorWriteBeginRequest,
        receiving: &WriteBeginReceiving,
    ) -> Result<Arc<Self>, ConnectorError> {
        receiving.claim()?;
        if matches!(
            &request.flavor,
            ConnectorWriteSessionFlavor::ApplicationDocumentPublication { .. }
        ) {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "application-document writes require an application-document session entrypoint",
            ));
        }
        Self::begin_validated_async_in(
            lease,
            catalog_properties,
            request,
            WritePublicationState::Ordinary,
            receiving,
        )
        .await
    }

    /// Retain the exact pending declaration in the original receiving Source
    /// before constructing the Provider's borrowed application-document begin.
    pub(crate) async fn begin_pending_application_document_publication_async_in(
        lease: ConnectorWriteStackLease,
        catalog_properties: novarocks_spi::connector::CatalogProperties,
        request: ConnectorWriteBeginRequest,
        receiving: &WriteBeginReceiving,
    ) -> Result<Arc<Self>, ConnectorError> {
        receiving.claim()?;
        let declaration = match &request.flavor {
            ConnectorWriteSessionFlavor::ApplicationDocumentPublication { declaration, .. } => {
                declaration.clone()
            }
            _ => {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "pending application-document publication requires an application-document write",
                ));
            }
        };
        Self::begin_validated_async_in(
            lease,
            catalog_properties,
            request,
            WritePublicationState::Pending { declaration },
            receiving,
        )
        .await
    }

    // Both explicit entrypoints claim and validate their flavor before using
    // this one original Provider poll/result-before-child-Drop/adoption path.
    async fn begin_validated_async_in(
        lease: ConnectorWriteStackLease,
        catalog_properties: novarocks_spi::connector::CatalogProperties,
        mut request: ConnectorWriteBeginRequest,
        finish_publication: WritePublicationState,
        receiving: &WriteBeginReceiving,
    ) -> Result<Arc<Self>, ConnectorError> {
        request.context = context_for_write_begin(request.context, &catalog_properties)?;
        receiving.install_publication_input(lease, catalog_properties, finish_publication);
        let control = receiving.control();
        receiving.enter_factory();
        let mut original = control.begin_write_async(request);
        receiving.leave_pending();
        let outcome = std::future::poll_fn(|cx| {
            if !receiving.enter_poll() {
                return std::task::Poll::Pending;
            }
            let polled = std::future::Future::poll(original.as_mut(), cx);
            if polled.is_pending() {
                receiving.leave_pending();
            }
            polled
        })
        .await;
        // The original lease/properties and the exact accepted plan or cause
        // already have independent fixed Source slots before foreign Drop.
        receiving.publish_plan(outcome);
        drop(original);
        receiving.adopt_session();
        receiving.finish_drop();
        receiving.completed_result()
    }

    /// Borrow the selected stable original Rewrite source; no synchronous begin fallback.
    pub(crate) async fn begin_rewrite_scoped_in(
        lease: ConnectorWriteStackLease,
        catalog_properties: novarocks_spi::connector::CatalogProperties,
        request: ConnectorWriteBeginRequest,
        receiving: &WriteBeginReceiving,
        original_source: &mut Option<
            Arc<dyn novarocks_spi::connector::ConnectorRewriteWriteBeginSource>,
        >,
        cpu: &dyn novarocks_spi::connector::RewriteCpuPort,
        metadata_cpu: &Arc<dyn novarocks_spi::connector::ProviderMetadataCpuPort>,
        original: novarocks_spi::connector::ConnectorOriginalResultScope,
    ) -> Result<Arc<Self>, ConnectorError> {
        if original_source.is_some()
            || !matches!(
                &request.flavor,
                ConnectorWriteSessionFlavor::DistributedRewrite { .. }
            )
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "governed Rewrite begin requires an empty original source and exact flavor",
            ));
        }
        receiving.claim()?;
        receiving.install_input(lease, catalog_properties);
        let mut request = Some(request);
        receiving.control().prepare_rewrite_begin_source_in(
            &mut request,
            original,
            original_source,
        )?;
        let source = original_source
            .as_ref()
            .expect("installed original Rewrite begin source");
        let mut driver = source.prepare(cpu, metadata_cpu);
        let status = std::future::poll_fn(|cx| driver.as_mut().poll(cx)).await;
        drop(driver);
        status?;
        let mut plan = None;
        source.take_plan_in(&mut plan)?;
        receiving.publish_plan(plan.expect("same original Rewrite begin result"));
        receiving.adopt_session();
        receiving.finish_drop();
        receiving.completed_result()
    }

    pub(crate) fn cleanup_responsibility(
        &self,
    ) -> Result<novarocks_spi::connector::ConnectorMetadataCleanupResponsibility, ConnectorError>
    {
        let provider = self
            .lease
            .session()
            .cleanup_responsibility(self.plan.commit_handle())?;
        if self.has_unreceived_productions()? {
            return Ok(novarocks_spi::connector::ConnectorMetadataCleanupResponsibility::Retained);
        }
        Ok(provider)
    }

    /// An aborted, never-accepted formation owns no decoded payload in this
    /// session. Unknown production records and original Provider/credential
    /// custody remain intact; this grants neither deletion nor cleanup Complete.
    pub(crate) fn has_no_accepted_formation(&self) -> Result<bool, ConnectorError> {
        let terminal = self.lock_terminal()?;
        if *terminal != Some(TerminalDecision::Aborted) {
            return Ok(false);
        }
        let union = self.lock_accumulated()?;
        let publication = self.publication_input.lock().map_err(|_| {
            ConnectorError::new(
                ConnectorErrorKind::Internal,
                "write publication input is poisoned",
            )
        })?;
        Ok(union.rows.get() == 0
            && union.fragments.is_empty()
            && union.statistics.is_empty()
            && union.statistics_identities.is_empty()
            && union.statistics_body_bytes == 0
            && union.statistics_property_bytes == 0
            && union.retention.is_none()
            && union.received.is_none()
            && publication.is_none())
    }

    fn abort_production(
        &self,
    ) -> Result<novarocks_spi::connector::write_stack::ConnectorWriteAbortProduction, ConnectorError>
    {
        use novarocks_spi::connector::write_stack::ConnectorWriteAbortProduction as P;
        if self.production_fully_received()? {
            return Ok(P::CompleteReceived);
        }
        if !self.production_never_registered()? {
            return Ok(P::Unproven);
        }
        let union = self.lock_accumulated()?;
        let publication = self.publication_input.lock().map_err(|_| {
            ConnectorError::new(
                ConnectorErrorKind::Internal,
                "write publication input is poisoned",
            )
        })?;
        Ok(
            if union.rows.get() == 0
                && union.fragments.is_empty()
                && union.statistics.is_empty()
                && union.received.is_none()
                && publication.is_none()
            {
                P::NotStarted
            } else {
                P::Unproven
            },
        )
    }

    /// Pure observation from the exact session/control that owns publication.
    /// The provider returns no context until its positive finite source exists.
    pub(super) fn existing_finalization_context(
        &self,
    ) -> Result<Option<ConnectorRequestContext>, ConnectorError> {
        self.lease
            .session()
            .existing_finalization_context(self.plan.commit_handle())
    }

    /// Settle the original begin on preparation or Native refusal. The same
    /// Call keeps the actual abort result independently of cancellation.
    pub(crate) fn prepare_abort_call(
        self: &Arc<Self>,
        context: ConnectorRequestContext,
    ) -> Result<WritePublicationCall, ConnectorError> {
        let context = self.terminal_context(context)?;
        self.claim_terminal(TerminalDecision::Aborted)?;
        let production = self.abort_production()?;
        let received = if production == novarocks_spi::connector::write_stack::ConnectorWriteAbortProduction::CompleteReceived {
            Some(self.freeze_publication_input(
                context.clone(), ConnectorWriteFinishPublication::None, production,
            )?)
        } else { None };
        Ok(WritePublicationCall::new_abort(
            Arc::clone(self),
            context,
            production,
            received,
        ))
    }

    /// Retain a pre-claim refusal before beginning the original abort. A
    /// failure after a terminal claim is not permission to issue another call.
    pub(crate) fn prepare_finish_or_abort_call_in(
        self: &Arc<Self>,
        context: ConnectorRequestContext,
        target: &mut Option<WritePublicationCall>,
        refusal: &mut Option<ConnectorError>,
    ) -> Result<(), ConnectorError> {
        if target.is_some() || refusal.is_some() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "write publication preparation target is occupied",
            ));
        }
        match self.prepare_finish_call(context.clone()) {
            Ok(call) => *target = Some(call),
            Err(error) => {
                // The original Source owns the exact refusal before any next
                // factory, formatter or destructor can unwind.
                *refusal = Some(error);
                if self.lock_terminal()?.is_some() {
                    return Err(ConnectorError::new(
                        ConnectorErrorKind::Internal,
                        "write publication preparation failed after a terminal claim",
                    ));
                }
                *target = Some(self.prepare_abort_call(context)?);
            }
        }
        Ok(())
    }

    /// Freeze the actual accepted union before constructing a borrowed Provider
    /// future. This is not evidence that publication has dispatched or committed.
    pub(crate) fn prepare_finish_call(
        self: &Arc<Self>,
        context: ConnectorRequestContext,
    ) -> Result<WritePublicationCall, ConnectorError> {
        ensure_finish_context_active(&context)?;
        self.require_received_productions()?;
        let publication = self.finish_publication_for_commit()?;
        let context = self.terminal_context(context)?;
        self.claim_terminal(TerminalDecision::Committed)?;
        let production = self.abort_production()?;
        let input = self.freeze_publication_input(context, publication, production)?;
        Ok(WritePublicationCall::new(Arc::clone(self), input))
    }

    fn freeze_publication_input(
        &self,
        context: ConnectorRequestContext,
        publication: ConnectorWriteFinishPublication,
        production: novarocks_spi::connector::write_stack::ConnectorWriteAbortProduction,
    ) -> Result<Arc<publication_call::PublicationInput>, ConnectorError> {
        let received = self.freeze_received_input()?;
        let input = Arc::new(publication_call::PublicationInput {
            received,
            publication,
            context,
            production,
        });
        {
            let mut slot = self.publication_input.lock().map_err(|_| {
                ConnectorError::new(
                    ConnectorErrorKind::Internal,
                    "write publication input is poisoned",
                )
            })?;
            if slot.is_some() {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "write publication input is already installed",
                ));
            }
            *slot = Some(Arc::clone(&input));
        }
        Ok(input)
    }

    fn from_begin_plan(
        lease: ConnectorWriteStackLease,
        catalog_properties: novarocks_spi::connector::CatalogProperties,
        plan: ConnectorWriteSessionPlan,
        finish_publication: WritePublicationState,
        metadata_only_publication: bool,
    ) -> Self {
        Self {
            decode_origin: Arc::new(()),
            productions: Mutex::new(production::WriteProductionReceiving::default()),
            statistics_expected: Arc::new(
                plan.targets()
                    .iter()
                    .flat_map(|target| {
                        target
                            .statistics()
                            .requirements()
                            .iter()
                            .map(move |requirement| {
                                (target.ordinal(), requirement.artifact().clone())
                            })
                    })
                    .collect(),
            ),
            lease,
            plan,
            metadata_only_publication,
            finish_publication: Mutex::new(finish_publication),
            catalog_properties,
            accumulated: Mutex::new(AccumulatedWriteSet::default()),
            publication_input: Mutex::new(None),
            terminal: Mutex::new(None),
            finish_invocations: AtomicUsize::new(0),
            terminal_storage: Mutex::new(None),
            cow_original: None,
        }
    }

    fn from_checked_cow_plan(
        lease: ConnectorWriteStackLease,
        catalog_properties: novarocks_spi::connector::CatalogProperties,
        checked: novarocks_spi::connector::ConnectorCowBeginPlan,
    ) -> Result<Self, novarocks_spi::connector::ConnectorCowBeginFailure> {
        let (plan, original) = checked.into_parts();
        // Reject a stop between the original Provider return and FE adoption.
        if let Err(cause) = original.check_active() {
            // Plan destruction precedes return of the guarded original cause.
            drop(plan);
            return Err(novarocks_spi::connector::ConnectorCowBeginFailure::new(
                cause.into(),
                original,
            ));
        }
        Ok(Self {
            decode_origin: Arc::new(()),
            productions: Mutex::new(production::WriteProductionReceiving::default()),
            statistics_expected: Arc::new(
                plan.targets()
                    .iter()
                    .flat_map(|target| {
                        target
                            .statistics()
                            .requirements()
                            .iter()
                            .map(move |requirement| {
                                (target.ordinal(), requirement.artifact().clone())
                            })
                    })
                    .collect(),
            ),
            lease,
            plan,
            metadata_only_publication: false,
            finish_publication: Mutex::new(WritePublicationState::Ordinary),
            catalog_properties,
            accumulated: Mutex::new(AccumulatedWriteSet::default()),
            publication_input: Mutex::new(None),
            terminal: Mutex::new(None),
            finish_invocations: AtomicUsize::new(0),
            terminal_storage: Mutex::new(None),
            cow_original: Some(original),
        })
    }

    /// The declaration this session opened with, for the statement that must
    /// now build the exact payload it promised.
    ///
    /// Only a session still waiting for its payload answers: once bound, the
    /// declaration has done its job, and a second caller asking for it is
    /// about to build a second payload for one publication.
    pub(crate) fn pending_publication_declaration(
        &self,
    ) -> Result<ConnectorDocumentPublicationDeclaration, ConnectorError> {
        match &*self.lock_finish_publication()? {
            WritePublicationState::Pending { declaration } => Ok(declaration.clone()),
            WritePublicationState::Ordinary => Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "ordinary connector write session declared no application-document publication",
            )),
            WritePublicationState::Bound(_) => Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "application-document publication is already bound",
            )),
        }
    }

    /// Bind the exact application-document payload once the statement has the
    /// executed scan facts and write result needed to construct it.
    pub(crate) fn bind_keyed_document_publication(
        &self,
        publication: KeyedPublicationIntent,
    ) -> Result<(), ConnectorError> {
        let mut state = self.lock_finish_publication()?;
        if self.lock_terminal()?.is_some() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector write session already reached a terminal decision",
            ));
        }
        match &*state {
            WritePublicationState::Ordinary => Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "ordinary connector write session cannot bind an application-document publication",
            )),
            WritePublicationState::Pending { declaration } => {
                publication.validate_for(declaration)?;
                *state = WritePublicationState::Bound(publication);
                Ok(())
            }
            WritePublicationState::Bound(_) => Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "application-document publication is already bound",
            )),
        }
    }

    /// The sealed ordinal set a prepared write set may not exceed.
    pub(crate) fn expected_targets(&self) -> Vec<WriteTargetOrdinal> {
        self.plan.expected_targets()
    }

    pub(crate) fn cow_original(
        &self,
    ) -> Option<&novarocks_spi::connector::ConnectorOriginalResultScope> {
        self.cow_original.as_ref()
    }

    pub(crate) fn targets(&self) -> &[ConnectorWriteTargetPlan] {
        self.plan.targets()
    }

    pub(crate) const fn copy_on_write_routing(
        &self,
    ) -> Option<&novarocks_spi::connector::write_stack::ConnectorWriteCopyOnWriteRoutingPlan> {
        self.plan.copy_on_write()
    }

    /// How many times this session actually asked the connector to commit.
    ///
    /// The dual barrier's whole point is that some outcomes must leave this at
    /// zero, and "zero" is only meaningful if it is observable.
    #[cfg(test)]
    pub(crate) fn finish_invocations(&self) -> usize {
        self.finish_invocations.load(Ordering::SeqCst)
    }

    /// Encode every logical recipe once and charge the query's unique-handle
    /// budget.
    ///
    /// A recipe is charged per logical target, not per placement: copying the
    /// same canonical bytes to more backends causes no additional provider
    /// planning, so charging per copy would refuse writes that cost nothing
    /// extra to plan.
    /// The catalog this write executes against, as the backend must materialize
    /// it. It belongs in the query's Init catalog set beside every typed read's.
    pub(crate) const fn catalog_properties(&self) -> &novarocks_spi::connector::CatalogProperties {
        &self.catalog_properties
    }

    /// Return the provider-selected aggregate requirements for exactly one
    /// sealed logical target. SQL resolves these names against the same
    /// immutable function catalog snapshot that analyzed the write query.
    pub(crate) fn statistics_requirements(
        &self,
        target: WriteTargetOrdinal,
    ) -> Result<&[novarocks_spi::connector::StatisticsRequiredAggregation], ConnectorError> {
        self.plan
            .targets()
            .iter()
            .find(|candidate| candidate.ordinal() == target)
            .map(|candidate| candidate.statistics().requirements())
            .ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    format!(
                        "connector write target {} is outside the sealed session",
                        target.get()
                    ),
                )
            })
    }

    /// The plan-carried form of one target's writer handle.
    ///
    /// A completed plan states the handle it writes, and the wire encoder
    /// stamps the same handle in its own form. Both come from this session's
    /// handle encoder, so the plan cannot name a target the commit does not.
    pub(crate) fn encode_writer_handle_payload(
        &self,
        handle: &novarocks_spi::connector::write_stack::ConnectorWriterHandle,
    ) -> Result<novarocks_spi::connector::ConnectorEncodedPayload, ConnectorError> {
        self.lease
            .handle_encoder()
            .encode_writer_handle_payload(handle)
            .map_err(|error| ConnectorError::from_domain(ConnectorErrorKind::Internal, error))
    }

    pub(crate) fn seal_write_targets(&self) -> Result<SealedWriteTargets, ConnectorError> {
        let encoder = self.lease.handle_encoder();
        let mut ledger = UniqueWriterHandleLedger::new();
        let mut handles = std::collections::BTreeMap::new();
        for target in self.plan.targets() {
            let encoded = encoder
                .encode_writer_handle(target.handle())
                .map_err(|error| {
                    ConnectorError::from_domain(ConnectorErrorKind::Internal, error)
                })?;
            let canonical = encoder
                .canonical_writer_handle_bytes(target.handle())
                .map_err(|error| {
                    ConnectorError::from_domain(ConnectorErrorKind::Internal, error)
                })?;
            ledger.charge(target.ordinal(), canonical.len())?;
            if handles.insert(target.ordinal().get(), encoded).is_some() {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "connector write session sealed one logical target twice",
                ));
            }
        }
        Ok(SealedWriteTargets::new(
            self.catalog_properties.handle().clone(),
            handles,
        ))
    }

    pub(crate) fn logical_rows_after_empty_publication(
        &self,
        previous: Option<u64>,
        context: &ConnectorRequestContext,
    ) -> Result<Option<u64>, ConnectorError> {
        if !self.metadata_only_publication {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "only a metadata-only publication may project an implicit empty write",
            ));
        }
        let prepared = ConnectorPreparedWriteSet::try_new(0, Vec::new(), &self.expected_targets())?;
        self.logical_rows_after_prepared(&prepared, previous, context)
    }

    fn logical_rows_after_prepared(
        &self,
        prepared: &ConnectorPreparedWriteSet,
        previous: Option<u64>,
        context: &ConnectorRequestContext,
    ) -> Result<Option<u64>, ConnectorError> {
        ensure_finish_context_active(context)?;
        match self.lease.session().prepared_logical_row_change(
            self.plan.commit_handle(),
            prepared,
            context,
        )? {
            Some(change) => change.apply(previous),
            None => Ok(None),
        }
    }

    /// Project the exact already-received union without decoding it again.
    /// The caller's original CPU Source owns actual call/exit and panic health.
    pub(crate) fn logical_rows_after_received_write(
        &self,
        previous: Option<u64>,
        context: &ConnectorRequestContext,
    ) -> Result<Option<u64>, ConnectorError> {
        ensure_finish_context_active(context)?;
        let received = {
            let terminal = self.lock_terminal()?;
            if terminal.is_some() {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "write row projection must precede the terminal decision",
                ));
            }
            self.require_received_productions()?;
            self.freeze_received_input()?
        };
        // No terminal, production, accumulation or publication mutex crosses
        // the Provider callback. A panic leaves the same packet in the session.
        self.logical_rows_after_prepared(&received.prepared, previous, context)
    }

    fn freeze_received_input(&self) -> Result<Arc<ReceivedWriteInput>, ConnectorError> {
        let mut union = self.lock_accumulated()?;
        if let Some(received) = &union.received {
            return Ok(Arc::clone(received));
        }
        // Validate while all payload/statistics/receipts still occupy their
        // original slots. A typed refusal moves none of that inventory.
        let rows = union.rows.get();
        let prepared = ConnectorPreparedWriteSet::try_new_in(
            rows,
            &mut union.fragments,
            &self.expected_targets(),
        )?;
        let received = Arc::new(ReceivedWriteInput {
            prepared,
            statistics: std::mem::take(&mut union.statistics),
            _retention: union.retention.take(),
        });
        union.received = Some(Arc::clone(&received));
        Ok(received)
    }

    /// Observe the complete original Root count before publication. The first
    /// freeze moves inventory into this session; later publication reuses that
    /// same cached Arc. This neither copies payloads nor proves business success.
    pub(crate) fn freeze_received_row_count(&self) -> Result<u64, ConnectorError> {
        self.require_received_productions()?;
        Ok(self.freeze_received_input()?.prepared.row_count())
    }

    /// The rows accumulated so far. Report them to a client only after the
    /// external commit is known to have succeeded.
    #[cfg(test)]
    pub(crate) fn accumulated_row_count(&self) -> Result<u64, ConnectorError> {
        Ok(self.lock_accumulated()?.rows.get())
    }

    /// Decorate one terminal request with this session's retained capability.
    ///
    /// The vended-credential sink is removed with it: the attempt's credential
    /// descriptors were frozen at Init, so a terminal metadata reload may
    /// consume an existing lease but can never contribute a new one.
    /// Continue the same terminal credential resolver after a preceding sealed
    /// write. This getter performs no I/O and is not a publication proof.
    pub(crate) fn following_terminal_context(
        &self,
        context: ConnectorRequestContext,
    ) -> Result<ConnectorRequestContext, ConnectorError> {
        self.terminal_context(context)
    }

    fn terminal_context(
        &self,
        context: ConnectorRequestContext,
    ) -> Result<ConnectorRequestContext, ConnectorError> {
        let context = context.without_vended_credential_lease_sink();
        let router = self.lock_terminal_storage()?.clone();
        match router {
            Some(router) => {
                // All callers are at the actual terminal Context frontier.
                // Close original collection once and retain the same immutable
                // scope snapshot through cached and following contexts.
                router.freeze()?;
                let resolver: Arc<dyn ConnectorStorageResolver> = router;
                Ok(context.with_storage_resolver(resolver))
            }
            None => Ok(context),
        }
    }

    #[cfg(test)]
    fn retain_terminal_storage_for_test(
        &self,
        resolver: Arc<dyn ConnectorStorageResolver>,
    ) -> Result<(), ConnectorError> {
        let mut storage = self.lock_terminal_storage()?;
        if storage.is_some() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "terminal test observer is occupied",
            ));
        }
        *storage = Some(Arc::new(
            crate::query_execution::lifecycle_plan::SessionTerminalCredentialRouter::for_test(
                resolver,
            ),
        ));
        Ok(())
    }

    /// Drop the retained capability, releasing the hold that deferred the
    /// attempt's credential cleanup.
    fn release_terminal_storage_resolver(&self) {
        let previous = self
            .terminal_storage
            .lock()
            .ok()
            .and_then(|mut slot| slot.take());
        drop(previous);
    }

    fn lock_terminal_storage(
        &self,
    ) -> Result<
        std::sync::MutexGuard<
            '_,
            Option<Arc<crate::query_execution::lifecycle_plan::SessionTerminalCredentialRouter>>,
        >,
        ConnectorError,
    > {
        self.terminal_storage.lock().map_err(|_| {
            ConnectorError::new(
                ConnectorErrorKind::Internal,
                "connector write session terminal storage capability is poisoned",
            )
        })
    }

    fn lock_accumulated(
        &self,
    ) -> Result<std::sync::MutexGuard<'_, AccumulatedWriteSet>, ConnectorError> {
        self.accumulated.lock().map_err(|_| {
            ConnectorError::new(
                ConnectorErrorKind::Internal,
                "connector write session accumulation is poisoned",
            )
        })
    }

    fn finish_publication_for_commit(
        &self,
    ) -> Result<ConnectorWriteFinishPublication, ConnectorError> {
        match &*self.lock_finish_publication()? {
            WritePublicationState::Ordinary => Ok(ConnectorWriteFinishPublication::None),
            WritePublicationState::Pending { .. } => Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "application-document publication must be bound before finish",
            )),
            WritePublicationState::Bound(publication) => Ok(
                ConnectorWriteFinishPublication::KeyedDocuments(publication.clone()),
            ),
        }
    }

    fn lock_finish_publication(
        &self,
    ) -> Result<std::sync::MutexGuard<'_, WritePublicationState>, ConnectorError> {
        self.finish_publication.lock().map_err(|_| {
            ConnectorError::new(
                ConnectorErrorKind::Internal,
                "connector write session publication state is poisoned",
            )
        })
    }

    fn claim_terminal(&self, decision: TerminalDecision) -> Result<(), ConnectorError> {
        let mut terminal = self.lock_terminal()?;
        match *terminal {
            None => {
                *terminal = Some(decision);
                Ok(())
            }
            Some(existing) => Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                format!(
                    "connector write session already reached {existing:?} and cannot also reach {decision:?}"
                ),
            )),
        }
    }

    fn record_terminal(&self, decision: TerminalDecision) {
        if let Ok(mut terminal) = self.terminal.lock() {
            *terminal = Some(decision);
        }
    }

    fn lock_terminal(
        &self,
    ) -> Result<std::sync::MutexGuard<'_, Option<TerminalDecision>>, ConnectorError> {
        self.terminal.lock().map_err(|_| {
            ConnectorError::new(
                ConnectorErrorKind::Internal,
                "connector write session terminal state is poisoned",
            )
        })
    }
}

fn ensure_finish_context_active(context: &ConnectorRequestContext) -> Result<(), ConnectorError> {
    if context.is_cancelled() {
        return Err(ConnectorError::new(
            ConnectorErrorKind::Cancelled,
            "connector write commit was cancelled before the terminal decision",
        ));
    }
    if std::time::Instant::now() >= context.deadline() {
        return Err(ConnectorError::new(
            ConnectorErrorKind::DeadlineExceeded,
            "connector write commit deadline elapsed before the terminal decision",
        ));
    }
    Ok(())
}

/// Bind only an already-admitted attempt sink to the retained write generation.
/// The owned context preserves its deadline, stop and request/storage capabilities.
/// COW callers must cover this properties clone before entering this helper.
pub(super) fn context_for_write_begin(
    context: ConnectorRequestContext,
    catalog_properties: &novarocks_spi::connector::CatalogProperties,
) -> Result<ConnectorRequestContext, ConnectorError> {
    if context.vended_credential_lease_sink().is_some() {
        context.with_vended_credential_lease_collection(catalog_properties.clone())
    } else {
        Ok(context)
    }
}

fn prepare_cow_begin_input(
    write_lease: &novarocks_spi::connector::ConnectorWriteLease,
    mut request: ConnectorWriteBeginRequest,
    original: novarocks_spi::connector::ConnectorOriginalResultScope,
) -> Result<
    (
        novarocks_spi::connector::CatalogProperties,
        ConnectorWriteBeginRequest,
        novarocks_spi::connector::ConnectorOriginalResultScope,
    ),
    crate::query_execution::dml::mutation_flow::CowFailure,
> {
    use crate::query_execution::dml::mutation_flow::CowFailure;
    if let Err(cause) = original.check_active() {
        return Err(CowFailure::checked_begin(
            novarocks_spi::connector::ConnectorCowBeginFailure::new(cause.into(), original),
        ));
    }
    let Some(properties) = write_lease.catalog_properties() else {
        return Err(CowFailure::missing_catalog_identity_bound(original));
    };
    // Original catalog identity and attempt collection stay with the same
    // session. Layout prediction does not authorize these Provider requests.
    request.context = match context_for_write_begin(request.context, properties) {
        Ok(context) => context,
        Err(error) => {
            return Err(CowFailure::checked_begin(
                novarocks_spi::connector::ConnectorCowBeginFailure::new(error.into(), original),
            ));
        }
    };
    Ok((properties.clone(), request, original))
}

/// The exact checked COW plan and typed cause enter the original receiving
/// source before the Provider child Future is destroyed.
pub(crate) async fn begin_cow_connector_write_session_async_in(
    lease: ConnectorWriteStackLease,
    write_lease: &novarocks_spi::connector::ConnectorWriteLease,
    request: ConnectorWriteBeginRequest,
    original: novarocks_spi::connector::ConnectorOriginalResultScope,
    receiving: &WriteBeginReceiving,
) -> Result<Arc<ConnectorWriteSession>, crate::query_execution::dml::mutation_flow::CowFailure> {
    use crate::query_execution::dml::mutation_flow::CowFailure;
    let (catalog_properties, request, original) =
        prepare_cow_begin_input(write_lease, request, original)?;
    receiving.claim().map_err(|cause| {
        CowFailure::checked_begin(novarocks_spi::connector::ConnectorCowBeginFailure::new(
            cause.into(),
            original.clone(),
        ))
    })?;
    receiving.install_input(lease, catalog_properties);
    let control = receiving.control();
    receiving.enter_factory();
    let mut child = control.begin_cow_write_checked_async(request, original);
    receiving.leave_pending();
    let outcome = std::future::poll_fn(|cx| {
        if !receiving.enter_poll() {
            return std::task::Poll::Pending;
        }
        let polled = std::future::Future::poll(child.as_mut(), cx);
        if polled.is_pending() {
            receiving.leave_pending();
        }
        polled
    })
    .await;
    receiving.publish_cow_plan(outcome);
    drop(child);
    receiving.adopt_cow_session();
    receiving.finish_drop();
    receiving
        .completed_cow_result()
        .map_err(CowFailure::checked_begin)
}

/// Borrow the original asynchronous begin while retaining its exact declaration.
/// Preparation and Provider refusals keep the original ConnectorError.
pub(crate) async fn begin_connector_application_document_write_session_pending_async_in(
    lease: ConnectorWriteStackLease,
    write_lease: &novarocks_spi::connector::ConnectorWriteLease,
    request: ConnectorWriteBeginRequest,
    receiving: &WriteBeginReceiving,
) -> Result<Arc<ConnectorWriteSession>, ConnectorError> {
    let catalog_properties = write_lease.catalog_properties().cloned().ok_or_else(|| {
        ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            "connector write lease has no immutable catalog runtime identity",
        )
    })?;
    ConnectorWriteSession::begin_pending_application_document_publication_async_in(
        lease,
        catalog_properties,
        request,
        receiving,
    )
    .await
}

#[cfg(test)]
pub(crate) mod tests {
    use novarocks_spi::connector::write_stack::ConnectorWriteFinishRequest;
    use novarocks_spi::connector::{ConnectorWriteReceipt, ExternalMutationOutcome};
    use std::sync::Arc;

    use novarocks_proto_codec::connector_common::encode_connector_payload_message;
    use novarocks_spi::connector::ConnectorControlWriteBinding;
    use novarocks_spi::connector::document_storage::{
        ConnectorDocumentManagementAdmissionRequest, ConnectorDocumentManagementOperation,
        ConnectorDocumentPublicationDeclaration, ConnectorDocumentStorageBinding,
        ConnectorDocumentStorageManagement, KeyedPublicationIntent,
    };
    use novarocks_spi::connector::write_stack::{
        ConnectorCommitFragment, ConnectorWriterHandle, MAX_CONNECTOR_UNIQUE_WRITER_HANDLE_BYTES,
        ProviderWriteRuntime, WriteRuntimeAdapter, WriteStatisticsArtifact,
    };
    use novarocks_spi::connector::{
        CatalogHandle, CatalogVersion, ConnectorCodecCategory, ConnectorCodecRevision,
        ConnectorEncodedPayload, ConnectorEnvelopeHeader, ConnectorInstanceDescriptor,
        ConnectorInstanceId, ConnectorProviderBindingKey, ConnectorProviderId,
        ConnectorWriteControl as LegacyWriteControl, ConnectorWriteFragmentWireDecoder,
        ConnectorWriteHandleWireEncoder, CredentialLeaseId, ResolvedVendedS3Access,
        StatisticsArtifactDraft, StatisticsArtifactIdentity, StatisticsRequiredAggregation,
        StatisticsScanColumn, StorageAccessDomainId, StorageAccessRequest,
        StorageCredentialScopePrefix,
    };

    use super::*;

    // ---- a minimal provider whose only job is to be recoverable -----------

    #[derive(Clone, Debug, Eq, PartialEq)]
    struct FakeCommit;
    #[derive(Clone, Debug, Eq, PartialEq)]
    struct FakeHandle(u32);
    #[derive(Clone, Debug, Eq, PartialEq)]
    struct FakeFragment(u32, Option<bytes::Bytes>);

    struct FakeProvider {
        descriptor: ConnectorInstanceDescriptor,
        catalog_handle: CatalogHandle,
    }

    impl ProviderWriteRuntime for FakeProvider {
        type CommitHandle = FakeCommit;
        type WriterHandle = FakeHandle;
        type CommitFragment = FakeFragment;

        fn descriptor(&self) -> &ConnectorInstanceDescriptor {
            &self.descriptor
        }

        fn catalog_handle(&self) -> &CatalogHandle {
            &self.catalog_handle
        }
    }

    fn catalog_handle() -> CatalogHandle {
        CatalogHandle::new(
            ConnectorInstanceId::parse("write_session_unit").expect("instance id"),
            CatalogVersion::from_bytes([3; 32]),
        )
    }

    fn catalog_properties() -> novarocks_spi::connector::CatalogProperties {
        novarocks_spi::connector::CatalogProperties::new(
            catalog_handle(),
            novarocks_spi::connector::ConnectorProviderId::parse("iceberg")
                .expect("static provider ID"),
            1,
            Vec::new(),
            Vec::new(),
        )
        .expect("test catalog properties")
    }

    fn adapter() -> WriteRuntimeAdapter<FakeProvider> {
        let handle = catalog_handle();
        WriteRuntimeAdapter::new(Arc::new(FakeProvider {
            descriptor: ConnectorInstanceDescriptor {
                provider_id: ConnectorProviderId::parse("fake").expect("provider id"),
                instance_id: handle.catalog_name().clone(),
            },
            catalog_handle: handle,
        }))
    }

    fn encoded_payload(
        category: ConnectorCodecCategory,
        payload: impl Into<bytes::Bytes>,
    ) -> ConnectorEncodedPayload {
        ConnectorEncodedPayload::new(
            ConnectorEnvelopeHeader::new(
                ConnectorProviderId::parse("fake").expect("provider id"),
                catalog_handle(),
                category,
                ConnectorCodecRevision::try_new(1).expect("codec revision"),
            ),
            payload.into(),
        )
    }

    // ---- the session control under test ----------------------------------

    #[derive(Default)]
    pub(crate) struct Recorded {
        decoded_fragments: usize,
        pub(crate) begin: usize,
        begin_async: usize,
        begin_declaration: Option<ConnectorDocumentPublicationDeclaration>,
        capture_begin_context: bool,
        begin_context: Option<ConnectorRequestContext>,
        pub(crate) finish_future_drops: usize,
        pub(crate) finalization_context:
            Option<Result<Option<ConnectorRequestContext>, ConnectorError>>,
        pub(crate) finalization_context_reads: usize,
        begin_future_drops: usize,
        panic_begin_future_drop: bool,
        source_drops: usize,
        pub(crate) finish_rows: u64,
        panic_finish_future_drop: bool,
        pub(crate) defer_finish_cleanup: bool,
        pub(crate) allow_cleanup_exit: bool,
        pub(crate) cleanup_started: usize,
        pub(crate) fail_safe_cleanup: bool,
        reconcile_committed: bool,
        abort_future_drops: usize,
        panic_abort_future_drop: bool,
        pub(crate) finish: usize,
        pub(crate) abort: usize,
        pub(crate) abort_outcome: Option<ConnectorWriteAbortOutcome>,
        pub(crate) enforce_cleanup_responsibility: bool,
        terminal_committed: Option<(
            Arc<novarocks_spi::connector::write_stack::ConnectorWriteNeededReady>,
            novarocks_spi::connector::write_stack::ConnectorWriteCleanupState,
        )>,
        pub(crate) abort_production:
            Option<novarocks_spi::connector::write_stack::ConnectorWriteAbortProduction>,
        pub(crate) abort_received_rows: Option<u64>,
        pub(crate) abort_received_fragments: Option<usize>,
        pub(crate) reconcile: usize,
        pub(crate) statistics: Vec<WriteStatisticsArtifact>,
        pub(crate) publication: Option<ConnectorWriteFinishPublication>,
        row_change: Option<Option<novarocks_spi::connector::write_stack::ConnectorWriteRowChange>>,
        row_change_queries: usize,
        row_change_session: Option<std::sync::Weak<ConnectorWriteSession>>,
        row_change_failure: Option<ConnectorError>,
        panic_row_change: bool,
        row_change_prepared_address: Option<usize>,
        finish_prepared_address: Option<usize>,
        retain_decoded_payload: bool,
        row_change_payload: Option<bytes::Bytes>,
        /// What the last terminal request could actually read object storage
        /// with. A real provider resolves this before it reloads table metadata
        /// or writes a manifest, so recording it here is recording whether the
        /// commit could have happened at all.
        pub(crate) terminal_storage: Option<Result<String, String>>,
        expected_finish_capacity: Option<novarocks_workload_control::ResultCapacityHandle>,
    }

    struct FakeSession {
        adapter: WriteRuntimeAdapter<FakeProvider>,
        binding_key: ConnectorProviderBindingKey,
        targets: usize,
        statistics: bool,
        recorded: Arc<Mutex<Recorded>>,
        finish_outcome: Mutex<Option<ExternalMutationOutcome<ConnectorWriteReceipt>>>,
    }

    struct FakeFinishFuture<'a, T> {
        original: std::pin::Pin<Box<dyn std::future::Future<Output = T> + Send + 'a>>,
        recorded: Arc<Mutex<Recorded>>,
    }
    impl<T> std::future::Future for FakeFinishFuture<'_, T> {
        type Output = T;
        fn poll(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Self::Output> {
            std::future::Future::poll(self.original.as_mut(), cx)
        }
    }
    impl<T> Drop for FakeFinishFuture<'_, T> {
        fn drop(&mut self) {
            let panic_on_drop = {
                let mut recorded = self.recorded.lock().unwrap();
                recorded.finish_future_drops += 1;
                recorded.panic_finish_future_drop
            };
            assert!(!panic_on_drop, "scripted finish future Drop panic");
        }
    }
    struct FakeBeginFuture<'a, T> {
        original: std::pin::Pin<Box<dyn std::future::Future<Output = T> + Send + 'a>>,
        recorded: Arc<Mutex<Recorded>>,
    }
    impl<T> std::future::Future for FakeBeginFuture<'_, T> {
        type Output = T;
        fn poll(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Self::Output> {
            std::future::Future::poll(self.original.as_mut(), cx)
        }
    }
    impl<T> Drop for FakeBeginFuture<'_, T> {
        fn drop(&mut self) {
            let panic_on_drop = {
                let mut recorded = self.recorded.lock().unwrap();
                recorded.begin_future_drops += 1;
                recorded.panic_begin_future_drop
            };
            assert!(!panic_on_drop, "scripted begin future Drop panic");
        }
    }
    struct FakeAbortFuture<'a> {
        original: std::pin::Pin<
            Box<
                dyn std::future::Future<Output = Result<ConnectorWriteAbortOutcome, ConnectorError>>
                    + Send
                    + 'a,
            >,
        >,
        recorded: Arc<Mutex<Recorded>>,
    }
    impl std::future::Future for FakeAbortFuture<'_> {
        type Output = Result<ConnectorWriteAbortOutcome, ConnectorError>;
        fn poll(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Self::Output> {
            std::future::Future::poll(self.original.as_mut(), cx)
        }
    }
    impl Drop for FakeAbortFuture<'_> {
        fn drop(&mut self) {
            let panic_on_drop = {
                let mut recorded = self.recorded.lock().unwrap();
                recorded.abort_future_drops += 1;
                recorded.panic_abort_future_drop
            };
            assert!(!panic_on_drop, "scripted abort future Drop panic");
        }
    }
    impl FakeSession {
        async fn finish_actual_needed_fixture(
            &self,
            outcome: Result<ExternalMutationOutcome<ConnectorWriteReceipt>, ConnectorError>,
            receiving: &novarocks_spi::connector::write_stack::ConnectorWriteNeededReadyReceiving,
        ) -> Result<
            novarocks_spi::connector::write_stack::ConnectorWritePublicationCompletion,
            ConnectorError,
        > {
            let mut outcome = outcome.map(novarocks_spi::connector::write_stack::ConnectorWritePublicationCompletion::from_outcome);
            if let Ok(committed) = &outcome
                && let novarocks_spi::connector::write_stack::ConnectorWritePublicationCompletion::KnownCommitted { needed, .. } = committed {
                    let deferred = self.recorded.lock().unwrap().defer_finish_cleanup;
                    self.recorded.lock().unwrap().terminal_committed = Some((
                        Arc::clone(needed),
                        if deferred { novarocks_spi::connector::write_stack::ConnectorWriteCleanupState::Pending }
                        else { novarocks_spi::connector::write_stack::ConnectorWriteCleanupState::Observed(novarocks_spi::connector::ExternalMutationFinalization::Complete) },
                    ));
                    receiving.publish(Arc::clone(needed))?;
                    if deferred {
                        let Ok(novarocks_spi::connector::write_stack::ConnectorWritePublicationCompletion::KnownCommitted { cleanup, .. }) = &mut outcome else { unreachable!() };
                        *cleanup = novarocks_spi::connector::write_stack::ConnectorWriteCleanupState::Pending;
                        receiving.cleanup_permitted().await;
                        self.recorded.lock().unwrap().cleanup_started += 1;
                        std::future::poll_fn(|_| {
                            if self.recorded.lock().unwrap().allow_cleanup_exit {
                                std::task::Poll::Ready(())
                            } else { std::task::Poll::Pending }
                        }).await;
                        if self.recorded.lock().unwrap().fail_safe_cleanup {
                            let Ok(novarocks_spi::connector::write_stack::ConnectorWritePublicationCompletion::KnownCommitted { cleanup, .. }) = &mut outcome else { unreachable!() };
                            *cleanup = novarocks_spi::connector::write_stack::ConnectorWriteCleanupState::Observed(novarocks_spi::connector::ExternalMutationFinalization::Failed(
                                novarocks_spi::connector::ConnectorMutationFailure::new(
                                    novarocks_spi::connector::ConnectorMutationFailureKind::Unavailable,
                                    "scripted safe cleanup failed after needed finalization",
                                ),
                            ));
                        } else {
                            let Ok(novarocks_spi::connector::write_stack::ConnectorWritePublicationCompletion::KnownCommitted { cleanup, .. }) = &mut outcome else { unreachable!() };
                            *cleanup = novarocks_spi::connector::write_stack::ConnectorWriteCleanupState::Observed(novarocks_spi::connector::ExternalMutationFinalization::Complete);
                        }
                    }
                }
            if let Ok(novarocks_spi::connector::write_stack::ConnectorWritePublicationCompletion::KnownCommitted { needed, cleanup }) = &outcome {
                self.recorded.lock().unwrap().terminal_committed = Some((Arc::clone(needed), cleanup.clone()));
            }
            outcome
        }
        fn record_finish(
            &self,
            prepared: &ConnectorPreparedWriteSet,
            statistics: &[WriteStatisticsArtifact],
            publication: &ConnectorWriteFinishPublication,
            context: &ConnectorRequestContext,
        ) -> Result<ExternalMutationOutcome<ConnectorWriteReceipt>, ConnectorError> {
            {
                let storage = probe_vended_storage(context);
                let mut recorded = self.recorded.lock().expect("recorded");
                recorded.finish += 1;
                recorded.finish_rows = prepared.row_count();
                recorded.finish_prepared_address = Some(prepared as *const _ as usize);
                if let Some(capacity) = &recorded.expected_finish_capacity {
                    assert_eq!(capacity.snapshot().held_positions, [0, 0, 1, 0]);
                }
                recorded.statistics = statistics.to_vec();
                recorded.publication = Some(publication.clone());
                recorded.terminal_storage = Some(storage);
            }
            self.finish_outcome
                .lock()
                .expect("outcome")
                .take()
                .ok_or_else(|| {
                    ConnectorError::new(ConnectorErrorKind::Internal, "no scripted outcome")
                })
        }
    }
    impl novarocks_spi::connector::write_stack::ConnectorWriteControl for FakeSession {
        fn existing_finalization_context(
            &self,
            _: &novarocks_spi::connector::write_stack::ConnectorWriteCommitHandle,
        ) -> Result<Option<ConnectorRequestContext>, ConnectorError> {
            let mut recorded = self.recorded.lock().unwrap();
            recorded.finalization_context_reads += 1;
            recorded.finalization_context.clone().unwrap_or(Ok(None))
        }
        fn begin_write_async<'a>(
            &'a self,
            request: novarocks_spi::connector::write_stack::ConnectorWriteBeginRequest,
        ) -> std::pin::Pin<
            Box<
                dyn std::future::Future<
                        Output = Result<
                            novarocks_spi::connector::write_stack::ConnectorWriteSessionPlan,
                            novarocks_spi::connector::ConnectorError,
                        >,
                    > + Send
                    + 'a,
            >,
        > {
            self.recorded.lock().unwrap().begin_async += 1;
            Box::pin(FakeBeginFuture {
                original: Box::pin(async move { self.begin_write(request) }),
                recorded: Arc::clone(&self.recorded),
            })
        }

        fn finish_write_async<'a>(
            &'a self,
            request: novarocks_spi::connector::write_stack::ConnectorWriteFinishRequestRef<'a>,
        ) -> std::pin::Pin<
            Box<
                dyn std::future::Future<
                        Output = Result<
                            novarocks_spi::connector::ExternalMutationOutcome<
                                novarocks_spi::connector::ConnectorWriteReceipt,
                            >,
                            novarocks_spi::connector::ConnectorError,
                        >,
                    > + Send
                    + 'a,
            >,
        > {
            Box::pin(FakeFinishFuture {
                original: Box::pin(async move {
                    self.record_finish(
                        request.prepared,
                        request.statistics,
                        request.publication,
                        request.context,
                    )
                }),
                recorded: Arc::clone(&self.recorded),
            })
        }

        fn finish_write_with_needed_ready_async<'a>(
            &'a self,
            request: novarocks_spi::connector::write_stack::ConnectorWriteFinishRequestRef<'a>,

            receiving:&'a novarocks_spi::connector::write_stack::ConnectorWriteNeededReadyReceiving,
        ) -> std::pin::Pin<
            Box<
                dyn std::future::Future<
                        Output = Result<
                            novarocks_spi::connector::write_stack::ConnectorWritePublicationCompletion,
                            novarocks_spi::connector::ConnectorError,
                        >,
                    > + Send
                    + 'a,
            >,
        >{
            Box::pin(FakeFinishFuture {
                original: Box::pin(async move {
                    let outcome = self.record_finish(
                        request.prepared,
                        request.statistics,
                        request.publication,
                        request.context,
                    );
                    self.finish_actual_needed_fixture(outcome, receiving).await
                }),
                recorded: Arc::clone(&self.recorded),
            })
        }

        fn abort_write_async<'a>(
            &'a self,
            request: novarocks_spi::connector::write_stack::ConnectorWriteSessionAbortRequest<'a>,
        ) -> std::pin::Pin<
            Box<
                dyn std::future::Future<
                        Output = Result<
                            novarocks_spi::connector::ConnectorWriteAbortOutcome,
                            novarocks_spi::connector::ConnectorError,
                        >,
                    > + Send
                    + 'a,
            >,
        > {
            Box::pin(FakeAbortFuture {
                original: Box::pin(async move { self.abort_write(request) }),
                recorded: Arc::clone(&self.recorded),
            })
        }

        fn reconcile_write_async<'a>(
            &'a self,
            request: novarocks_spi::connector::write_stack::ConnectorWriteSessionReconcileRequest<
                'a,
            >,
        ) -> std::pin::Pin<
            Box<
                dyn std::future::Future<
                        Output = Result<
                            novarocks_spi::connector::ExternalMutationOutcome<
                                novarocks_spi::connector::ConnectorWriteReceipt,
                            >,
                            novarocks_spi::connector::ConnectorError,
                        >,
                    > + Send
                    + 'a,
            >,
        > {
            Box::pin(async move { self.reconcile_write(request) })
        }

        fn reconcile_write_with_needed_ready_async<'a>(
            &'a self,
            request: novarocks_spi::connector::write_stack::ConnectorWriteSessionReconcileRequest<
                'a,
            >,

            receiving:&'a novarocks_spi::connector::write_stack::ConnectorWriteNeededReadyReceiving,
        ) -> std::pin::Pin<
            Box<
                dyn std::future::Future<
                        Output = Result<
                            novarocks_spi::connector::write_stack::ConnectorWritePublicationCompletion,
                            novarocks_spi::connector::ConnectorError,
                        >,
                    > + Send
                    + 'a,
            >,
        >{
            Box::pin(async move {
                let outcome = self.reconcile_write(request);
                self.finish_actual_needed_fixture(outcome, receiving).await
            })
        }

        fn io_profile(
            &self,
        ) -> Result<novarocks_spi::connector::ConnectorMetadataMaintenanceIoProfile, ConnectorError>
        {
            novarocks_spi::connector::ConnectorMetadataMaintenanceIoProfile::try_new(
                std::num::NonZeroUsize::new(1).unwrap(),
                std::num::NonZeroU64::new(1024).unwrap(),
                std::num::NonZeroU64::new(1024).unwrap(),
                vec![novarocks_spi::connector::SharedDependency::LocalHost],
            )
        }
        fn cleanup_responsibility(
            &self,
            commit: &novarocks_spi::connector::write_stack::ConnectorWriteCommitHandle,
        ) -> Result<novarocks_spi::connector::ConnectorMetadataCleanupResponsibility, ConnectorError>
        {
            self.adapter.commit_handle(commit)?;
            let recorded = self.recorded.lock().unwrap();
            if recorded.enforce_cleanup_responsibility
                && !matches!(
                    &recorded.terminal_committed,
                    Some((
                        _,
                        novarocks_spi::connector::write_stack::ConnectorWriteCleanupState::Observed(
                            novarocks_spi::connector::ExternalMutationFinalization::Complete
                        )
                    ))
                )
            {
                return Ok(
                    novarocks_spi::connector::ConnectorMetadataCleanupResponsibility::Retained,
                );
            }
            Ok(novarocks_spi::connector::ConnectorMetadataCleanupResponsibility::Disposed)
        }

        fn prepared_logical_row_change(
            &self,
            commit: &novarocks_spi::connector::write_stack::ConnectorWriteCommitHandle,
            prepared: &ConnectorPreparedWriteSet,
            _: &ConnectorRequestContext,
        ) -> Result<
            Option<novarocks_spi::connector::write_stack::ConnectorWriteRowChange>,
            ConnectorError,
        > {
            self.adapter.commit_handle(commit)?;
            assert_eq!(prepared.row_count(), 1);
            let mut original_payload = None;
            for (_, fragment) in prepared.fragments() {
                let original = self.adapter.commit_fragment(fragment)?;
                original_payload = original.1.clone();
            }
            let (session, panic, failure, change) = {
                let mut recorded = self.recorded.lock().unwrap();
                recorded.row_change_queries += 1;
                recorded.row_change_prepared_address = Some(prepared as *const _ as usize);
                recorded.row_change_payload = original_payload;
                (
                    recorded.row_change_session.clone(),
                    recorded.panic_row_change,
                    recorded.row_change_failure.take(),
                    recorded.row_change,
                )
            };
            if let Some(session) = session.and_then(|weak| weak.upgrade()) {
                assert!(session.terminal.try_lock().is_ok());
                assert!(session.productions.try_lock().is_ok());
                assert!(session.accumulated.try_lock().is_ok());
                assert!(session.publication_input.try_lock().is_ok());
                assert!(session.finish_publication.try_lock().is_ok());
            }
            assert!(!panic, "scripted logical row projection panic");
            if let Some(failure) = failure {
                return Err(failure);
            }
            change.ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::Unsupported,
                    "scripted row change is unavailable",
                )
            })
        }

        fn begin_cow_write_checked_async(
            &self,
            request: novarocks_spi::connector::write_stack::ConnectorWriteBeginRequest,
            original: novarocks_spi::connector::ConnectorOriginalResultScope,
        ) -> std::pin::Pin<
            Box<
                dyn std::future::Future<
                        Output = Result<
                            novarocks_spi::connector::ConnectorCowBeginPlan,
                            novarocks_spi::connector::ConnectorCowBeginFailure,
                        >,
                    > + Send
                    + '_,
            >,
        > {
            // This original test control's checked author is pure and performs no I/O.
            Box::pin(FakeBeginFuture {
                original: Box::pin(async move { self.begin_cow_write_checked(request, original) }),
                recorded: Arc::clone(&self.recorded),
            })
        }

        fn begin_cow_write_checked(
            &self,
            request: novarocks_spi::connector::write_stack::ConnectorWriteBeginRequest,
            original: novarocks_spi::connector::ConnectorOriginalResultScope,
        ) -> Result<
            novarocks_spi::connector::ConnectorCowBeginPlan,
            novarocks_spi::connector::ConnectorCowBeginFailure,
        > {
            // Test-only scripted provider: the fixture owns its finite graph.
            if let Err(cause) = original.check_active() {
                return Err(novarocks_spi::connector::ConnectorCowBeginFailure::new(
                    cause.into(),
                    original,
                ));
            }
            match self.begin_write(request) {
                Ok(plan) => Ok(novarocks_spi::connector::ConnectorCowBeginPlan::new(
                    plan, original,
                )),
                Err(cause) => Err(novarocks_spi::connector::ConnectorCowBeginFailure::new(
                    cause.into(),
                    original,
                )),
            }
        }

        fn binding_key(&self) -> &ConnectorProviderBindingKey {
            &self.binding_key
        }

        fn begin_write(
            &self,
            request: ConnectorWriteBeginRequest,
        ) -> Result<ConnectorWriteSessionPlan, ConnectorError> {
            {
                let mut recorded = self.recorded.lock().expect("recorded");
                recorded.begin += 1;
                if let ConnectorWriteSessionFlavor::ApplicationDocumentPublication {
                    declaration,
                    ..
                } = &request.flavor
                {
                    recorded.begin_declaration = Some(declaration.clone());
                }
                if recorded.capture_begin_context {
                    recorded.begin_context = Some(request.context);
                }
            }
            let commit = self.adapter.wrap_commit_handle(FakeCommit);
            let targets = (0..self.targets)
                .map(|index| {
                    let ordinal = WriteTargetOrdinal::try_new(
                        u32::try_from(index).expect("bounded ordinal"),
                    )?;
                    let handle = self.adapter.wrap_writer_handle(FakeHandle(ordinal.get()));
                    let input = novarocks_spi::connector::ConnectorWriteInputShape::Data {
                        fields: vec![
                            novarocks_spi::connector::ConnectorWriteFieldBinding::try_new_typed(
                                novarocks_spi::connector::ConnectorWriteFieldToken::from_bytes(
                                    [1; 32],
                                ),
                                arrow::datatypes::Field::new(
                                    "v",
                                    arrow::datatypes::DataType::Int64,
                                    true,
                                ),
                                novarocks_type_contract::FunctionValueType::new(
                                    arrow::datatypes::DataType::Int64,
                                    true,
                                ),
                            )?,
                        ],
                    };
                    let target = ConnectorWriteTargetPlan::new(ordinal, handle, input.clone());
                    if !self.statistics {
                        return Ok(target);
                    }
                    let requirement = StatisticsRequiredAggregation::try_new(
                        StatisticsScanColumn::try_new(
                            0,
                            "v",
                            novarocks_type_contract::FunctionValueType::new(
                                arrow::datatypes::DataType::Int64,
                                true,
                            ),
                        )?,
                        "$test_stat",
                        StatisticsArtifactIdentity::try_new(
                            vec![i32::try_from(index + 1).expect("bounded field id")],
                            "test/blob",
                        )?,
                    )?;
                    target.with_statistics_contract(
                        novarocks_spi::connector::write_stack::WriteStatisticsContract::try_new(
                            &input,
                            vec![requirement],
                        )?,
                    )
                })
                .collect::<Result<Vec<_>, ConnectorError>>()?;
            ConnectorWriteSessionPlan::try_new(commit, targets)
        }

        fn finish_write(
            &self,
            request: ConnectorWriteFinishRequest<'_>,
        ) -> Result<ExternalMutationOutcome<ConnectorWriteReceipt>, ConnectorError> {
            self.record_finish(
                &request.prepared,
                &request.statistics,
                &request.publication,
                &request.context,
            )
        }

        fn abort_write(
            &self,
            request: ConnectorWriteSessionAbortRequest<'_>,
        ) -> Result<ConnectorWriteAbortOutcome, ConnectorError> {
            let mut recorded = self.recorded.lock().expect("recorded");
            recorded.abort += 1;
            recorded.abort_production = Some(request.production);
            recorded.abort_received_rows = request.received.map(|set| set.row_count());
            recorded.abort_received_fragments = request.received.map(|set| set.fragments().len());
            recorded.terminal_storage = Some(probe_vended_storage(&request.context));
            if let Some(outcome) = recorded.abort_outcome.take() {
                return Ok(outcome);
            }
            if let Some((needed, cleanup)) = &recorded.terminal_committed {
                return Ok(ConnectorWriteAbortOutcome::KnownCommitted {
                    needed: Arc::clone(needed),
                    cleanup: cleanup.clone(),
                });
            }
            Ok(ConnectorWriteAbortOutcome::KnownUncommitted {
                cleanup: novarocks_spi::connector::ExternalMutationFinalization::Complete,
            })
        }

        fn reconcile_write(
            &self,
            request: ConnectorWriteSessionReconcileRequest<'_>,
        ) -> Result<ExternalMutationOutcome<ConnectorWriteReceipt>, ConnectorError> {
            let mut recorded = self.recorded.lock().expect("recorded");
            recorded.reconcile += 1;
            recorded.terminal_storage = Some(probe_vended_storage(&request.context));
            if recorded.reconcile_committed {
                Ok(known_committed())
            } else {
                Err(ConnectorError::new(
                    ConnectorErrorKind::Internal,
                    "reconcile outcome is not scripted in this test",
                ))
            }
        }
    }

    // ---- codec facets -----------------------------------------------------

    struct FakeEncoder {
        payload_bytes: usize,
    }

    impl ConnectorWriteHandleWireEncoder for FakeEncoder {
        fn owner(&self) -> &str {
            "fake"
        }

        fn encode_writer_handle_payload(
            &self,
            _handle: &ConnectorWriterHandle,
        ) -> Result<ConnectorEncodedPayload, novarocks_spi::connector::ConnectorCodecError>
        {
            Ok(encoded_payload(
                ConnectorCodecCategory::WriteHandle,
                bytes::Bytes::from(vec![b'u'; self.payload_bytes]),
            ))
        }
    }

    struct FakeDecoder {
        adapter: WriteRuntimeAdapter<FakeProvider>,
        recorded: Arc<Mutex<Recorded>>,
    }

    impl ConnectorWriteFragmentWireDecoder for FakeDecoder {
        fn owner(&self) -> &str {
            "fake"
        }

        fn decode_commit_fragment_payload(
            &self,
            _fragment: &ConnectorEncodedPayload,
        ) -> Result<ConnectorCommitFragment, novarocks_spi::connector::ConnectorCodecError>
        {
            let retain = {
                let mut recorded = self.recorded.lock().unwrap();
                recorded.decoded_fragments += 1;
                recorded.retain_decoded_payload
            };
            Ok(self
                .adapter
                .wrap_commit_fragment(FakeFragment(0, retain.then(|| _fragment.payload().clone()))))
        }
    }

    struct UnusedLegacyControl;

    impl LegacyWriteControl for UnusedLegacyControl {
        fn binding_key(&self) -> &ConnectorProviderBindingKey {
            unreachable!("the legacy control is not exercised by the write session")
        }
    }

    pub(crate) fn begin_for_test(
        lease: ConnectorWriteStackLease,
        properties: novarocks_spi::connector::CatalogProperties,
        request: ConnectorWriteBeginRequest,
    ) -> Result<Arc<ConnectorWriteSession>, ConnectorError> {
        let original = Arc::new(Mutex::new(None));
        let receiving = WriteBeginReceiving::new(Arc::clone(&original));
        let result = futures::executor::block_on(ConnectorWriteSession::begin_async_in(
            lease, properties, request, &receiving,
        ));
        drop(receiving);
        drop(original);
        result
    }

    pub(crate) fn begin_pending_for_test(
        lease: ConnectorWriteStackLease,
        properties: novarocks_spi::connector::CatalogProperties,
        request: ConnectorWriteBeginRequest,
    ) -> Result<Arc<ConnectorWriteSession>, ConnectorError> {
        let original = Arc::new(Mutex::new(None));
        let receiving = WriteBeginReceiving::new(Arc::clone(&original));
        let result = futures::executor::block_on(
            ConnectorWriteSession::begin_pending_application_document_publication_async_in(
                lease, properties, request, &receiving,
            ),
        );
        drop(receiving);
        drop(original);
        result
    }

    pub(crate) fn drain_call_for_test(
        call: &mut WritePublicationCall,
    ) -> Result<
        novarocks_spi::connector::write_stack::ConnectorWritePublicationCompletion,
        ConnectorError,
    > {
        futures::executor::block_on(std::future::poll_fn(|cx| call.poll_completed(cx)));
        let mut result = None;
        call.take_outcome_in(&mut result)?;
        result.expect("same original completed publication")
    }

    impl ConnectorWriteSession {
        pub(crate) fn receive_for_test(
            self: &Arc<Self>,
            prepared: DecodedPreparedWriteSet,
        ) -> Result<(), ConnectorError> {
            let segment = self.computation_decode(&self.expected_targets())?
                .interpret(prepared)
                .map_err(crate::query_execution::write_result::RootWriteDecodeFailure::into_connector_error)?;
            self.receiving_port().accept(segment).map(|_| ())
        }
        pub(crate) fn publish_for_test(
            self: &Arc<Self>,
            context: ConnectorRequestContext,
        ) -> Result<
            novarocks_spi::connector::write_stack::ConnectorWritePublicationCompletion,
            ConnectorError,
        > {
            let mut call = self.prepare_finish_call(context)?;
            drain_call_for_test(&mut call)
        }
        pub(crate) fn publish_prepared_for_test(
            self: &Arc<Self>,
            prepared: DecodedPreparedWriteSet,
            context: ConnectorRequestContext,
        ) -> Result<
            novarocks_spi::connector::write_stack::ConnectorWritePublicationCompletion,
            ConnectorError,
        > {
            self.receive_for_test(prepared)?;
            self.publish_for_test(context)
        }
        pub(crate) fn abort_for_test(
            self: &Arc<Self>,
            context: ConnectorRequestContext,
        ) -> Result<ConnectorWriteAbortOutcome, ConnectorError> {
            let mut call = self.prepare_abort_call(context)?;
            futures::executor::block_on(std::future::poll_fn(|cx| call.poll_completed(cx)));
            let mut result = None;
            call.take_abort_outcome_in(&mut result)?;
            result.expect("same original completed abort")
        }
    }

    pub(crate) struct Fixture {
        pub(crate) session: Arc<ConnectorWriteSession>,
        pub(crate) recorded: Arc<Mutex<Recorded>>,
    }

    impl Fixture {
        pub(crate) async fn release_provider_direct(
            &self,
            context: ConnectorRequestContext,
        ) -> Result<ConnectorWriteAbortOutcome, ConnectorError> {
            let control = self.session.lease.session();
            control.abort_write_async(ConnectorWriteSessionAbortRequest {
                commit: self.session.plan.commit_handle(), context,
                production: novarocks_spi::connector::write_stack::ConnectorWriteAbortProduction::Unproven,
                received: None,
            }).await
        }
    }

    fn fixture(targets: usize, payload_bytes: usize) -> Fixture {
        fixture_with_outcome(
            targets,
            payload_bytes,
            ExternalMutationOutcome::KnownUncommitted {
                failure: novarocks_spi::connector::ConnectorMutationFailure::new(
                    novarocks_spi::connector::ConnectorMutationFailureKind::Unavailable,
                    "scripted",
                ),
                cleanup: novarocks_spi::connector::ExternalMutationFinalization::Complete,
            },
        )
    }

    /// A write session on a scripted control, for tests in the statement flows
    /// that need a real session but no real provider.
    pub(crate) fn fixture_with_outcome(
        targets: usize,
        payload_bytes: usize,
        outcome: ExternalMutationOutcome<ConnectorWriteReceipt>,
    ) -> Fixture {
        fixture_with_outcome_and_publication(targets, payload_bytes, outcome, None)
    }

    fn fixture_with_outcome_and_publication(
        targets: usize,
        payload_bytes: usize,
        outcome: ExternalMutationOutcome<ConnectorWriteReceipt>,
        publication: Option<(
            ConnectorDocumentPublicationDeclaration,
            KeyedPublicationIntent,
        )>,
    ) -> Fixture {
        let (lease, recorded) = unopened_fixture(targets, payload_bytes, outcome);
        let session = match publication {
            Some((declaration, publication)) => {
                let session = begin_pending_for_test(
                    lease,
                    catalog_properties(),
                    application_document_begin_request(declaration),
                )
                .expect("begin pending write");
                session
                    .bind_keyed_document_publication(publication)
                    .expect("bind exact publication");
                session
            }
            None => {
                begin_for_test(lease, catalog_properties(), begin_request()).expect("begin write")
            }
        };
        Fixture { session, recorded }
    }

    fn unopened_fixture(
        targets: usize,
        payload_bytes: usize,
        outcome: ExternalMutationOutcome<ConnectorWriteReceipt>,
    ) -> (ConnectorWriteStackLease, Arc<Mutex<Recorded>>) {
        unopened_fixture_with_encoder(targets, outcome, Arc::new(FakeEncoder { payload_bytes }))
    }

    fn unopened_fixture_with_encoder(
        targets: usize,
        outcome: ExternalMutationOutcome<ConnectorWriteReceipt>,
        encoder: Arc<dyn ConnectorWriteHandleWireEncoder>,
    ) -> (ConnectorWriteStackLease, Arc<Mutex<Recorded>>) {
        let adapter = adapter();
        let binding_key = ConnectorProviderBindingKey {
            instance_id: catalog_handle().catalog_name().clone(),
            incarnation: novarocks_spi::connector::ProviderBindingEpoch::new(),
        };
        let recorded = Arc::new(Mutex::new(Recorded::default()));
        let session_control = Arc::new(FakeSession {
            adapter: adapter.clone(),
            binding_key,
            targets,
            statistics: false,
            recorded: Arc::clone(&recorded),
            finish_outcome: Mutex::new(Some(outcome)),
        });
        let group = ConnectorControlWriteBinding::new(
            Arc::new(UnusedLegacyControl),
            session_control,
            encoder,
            Arc::new(FakeDecoder {
                adapter,
                recorded: Arc::clone(&recorded),
            }),
        );
        let source_retirement = Arc::clone(&recorded);
        let lease = novarocks_catalog_application::test_support::write_stack_lease(
            novarocks_spi::connector::ConnectorControlRuntimeId::new(),
            group,
            move || {
                source_retirement.lock().unwrap().source_drops += 1;
            },
        );
        (lease, recorded)
    }

    fn fixture_with_statistics(targets: usize) -> Fixture {
        let adapter = adapter();
        let binding_key = ConnectorProviderBindingKey {
            instance_id: catalog_handle().catalog_name().clone(),
            incarnation: novarocks_spi::connector::ProviderBindingEpoch::new(),
        };
        let recorded = Arc::new(Mutex::new(Recorded::default()));
        let session_control = Arc::new(FakeSession {
            adapter: adapter.clone(),
            binding_key,
            targets,
            statistics: true,
            recorded: Arc::clone(&recorded),
            finish_outcome: Mutex::new(Some(ExternalMutationOutcome::KnownUncommitted {
                failure: novarocks_spi::connector::ConnectorMutationFailure::new(
                    novarocks_spi::connector::ConnectorMutationFailureKind::Unavailable,
                    "scripted",
                ),
                cleanup: novarocks_spi::connector::ExternalMutationFinalization::Complete,
            })),
        });
        let group = ConnectorControlWriteBinding::new(
            Arc::new(UnusedLegacyControl),
            session_control,
            Arc::new(FakeEncoder { payload_bytes: 16 }),
            Arc::new(FakeDecoder {
                adapter,
                recorded: Arc::clone(&recorded),
            }),
        );
        let lease = novarocks_catalog_application::test_support::write_stack_lease(
            novarocks_spi::connector::ConnectorControlRuntimeId::new(),
            group,
            || {},
        );
        let session =
            begin_for_test(lease, catalog_properties(), begin_request()).expect("begin write");
        Fixture { session, recorded }
    }

    fn statistics_artifact(
        target: u32,
        field_id: i32,
        body: &'static [u8],
    ) -> WriteStatisticsArtifact {
        WriteStatisticsArtifact::new(
            WriteTargetOrdinal::try_new(target).expect("target"),
            StatisticsArtifactDraft::try_new(
                vec![field_id],
                "test/blob",
                bytes::Bytes::from_static(body),
                std::collections::BTreeMap::new(),
            )
            .expect("artifact"),
        )
    }

    fn begin_request() -> ConnectorWriteBeginRequest {
        ConnectorWriteBeginRequest {
            table: Arc::from("db.t"),
            target_ref: novarocks_spi::connector::ConnectorWriteTargetRef::main(),
            intent: novarocks_spi::connector::ConnectorWriteIntent::Append,
            purpose: novarocks_spi::connector::ConnectorWriteAdmissionPurpose::OrdinaryDml,
            input: novarocks_spi::connector::ConnectorWriteInputRequest::Data {
                fields: vec![
                    novarocks_spi::connector::ConnectorWriteFieldRequest::try_new_typed(
                        arrow::datatypes::Field::new("v", arrow::datatypes::DataType::Int64, true),
                        novarocks_type_contract::FunctionValueType::new(
                            arrow::datatypes::DataType::Int64,
                            true,
                        ),
                    )
                    .expect("caller complete input declaration"),
                ],
            },
            base: None,
            flavor: novarocks_spi::connector::write_stack::ConnectorWriteSessionFlavor::Ordinary,
            context: request_context(),
        }
    }

    struct FakeDocumentManagement {
        descriptor: ConnectorInstanceDescriptor,
        incarnation: novarocks_spi::connector::ProviderBindingEpoch,
    }

    impl ConnectorDocumentStorageManagement for FakeDocumentManagement {
        fn descriptor(&self) -> &ConnectorInstanceDescriptor {
            &self.descriptor
        }

        fn incarnation(&self) -> novarocks_spi::connector::ProviderBindingEpoch {
            self.incarnation
        }

        fn admit_management(
            &self,
            _request: ConnectorDocumentManagementAdmissionRequest,
        ) -> Result<bytes::Bytes, ConnectorError> {
            Ok(bytes::Bytes::from_static(b"admitted-publication"))
        }
    }

    fn application_document_publication() -> (
        ConnectorDocumentPublicationDeclaration,
        KeyedPublicationIntent,
    ) {
        application_document_publication_for(
            novarocks_spi::connector::ConnectorManagedPublicationTechnique::Full,
        )
    }

    fn application_document_publication_for(
        technique: novarocks_spi::connector::ConnectorManagedPublicationTechnique,
    ) -> (
        ConnectorDocumentPublicationDeclaration,
        KeyedPublicationIntent,
    ) {
        let instance_id = catalog_handle().catalog_name().clone();
        let incarnation = novarocks_spi::connector::ProviderBindingEpoch::from_bytes([8; 16]);
        let descriptor = ConnectorInstanceDescriptor {
            provider_id: ConnectorProviderId::parse("iceberg").expect("provider id"),
            instance_id: instance_id.clone(),
        };
        let management = Arc::new(FakeDocumentManagement {
            descriptor: descriptor.clone(),
            incarnation,
        });
        let documents = ConnectorDocumentStorageBinding::try_new(
            descriptor,
            incarnation,
            None,
            Some(management),
        )
        .expect("document storage binding");
        let binding = novarocks_catalog_application::test_support::test_control_binding_for(
            instance_id.clone(),
            8,
        )
        .with_catalog_properties(catalog_properties())
        .and_then(|binding| binding.try_with_document_storage(Some(documents)))
        .expect("control binding");
        let documents =
            novarocks_spi::connector::ConnectorControlPlanningLease::new(Arc::new(binding), || {})
                .derive_document_storage_lease()
                .expect("document storage lease");

        let publication_id = novarocks_spi::connector::LakePublicationId::new_v7();
        let target = novarocks_spi::connector::ConnectorTableIdentity {
            instance_id,
            namespace: Arc::from("db"),
            table: Arc::from("t"),
        };
        let object_id = novarocks_spi::connector::ConnectorTableObjectId::try_new(
            bytes::Bytes::from_static(b"table-object"),
        )
        .expect("object id");
        let admission = documents
            .admit_management(
                ConnectorDocumentManagementAdmissionRequest::try_new(
                    documents.owner().clone(),
                    documents.catalog_handle().clone(),
                    novarocks_spi::connector::ConnectorMutationOperationId::from_bytes(
                        publication_id.to_bytes(),
                    ),
                    target,
                    Some(object_id.clone()),
                    ConnectorDocumentManagementOperation::Publication,
                    request_context(),
                )
                .expect("admission request"),
            )
            .expect("admission");
        let base = novarocks_spi::connector::ConnectorWriteBaseVersion::try_new(
            bytes::Bytes::from_static(b"exact-base"),
        )
        .expect("write base");
        let declaration = ConnectorDocumentPublicationDeclaration::try_new(
            publication_id,
            admission,
            object_id,
            base,
            technique,
            novarocks_spi::connector::ConnectorManagedPublicationEmptyInputDisposition::CommitEmptyWrite,
            None,
            None,
        )
        .expect("publication declaration");
        let (catalog, view) =
            novarocks_mv_application::persistence::keyed_documents::keyed_view_fixture(
                Some(17),
                true,
            );
        let observed =
            novarocks_mv_application::persistence::keyed_documents::decode_current_keyed_documents(
                view,
                catalog,
                Default::default(),
            )
            .unwrap();
        let mut p = observed.publication.unwrap();
        p.publication_id =
            novarocks_mv_application::persistence::identity::PublicationIdentity::try_new(
                publication_id.to_bytes().to_vec(),
            )
            .unwrap();
        let keyed =
            novarocks_mv_application::persistence::keyed_documents::keyed_publication_document_set(
                &observed.versions,
                &observed.definition,
                &p,
            )
            .unwrap();
        let publication = KeyedPublicationIntent {
            declaration: declaration.clone(),
            documents: keyed,
        };
        (declaration, publication)
    }

    fn application_document_begin_request(
        declaration: ConnectorDocumentPublicationDeclaration,
    ) -> ConnectorWriteBeginRequest {
        ConnectorWriteBeginRequest {
            table: Arc::from("db.t"),
            target_ref: novarocks_spi::connector::ConnectorWriteTargetRef::main(),
            intent: novarocks_spi::connector::ConnectorWriteIntent::Overwrite,
            purpose:
                novarocks_spi::connector::ConnectorWriteAdmissionPurpose::MaterializedViewRefresh,
            input: novarocks_spi::connector::ConnectorWriteInputRequest::Data {
                fields: vec![novarocks_spi::connector::ConnectorWriteFieldRequest::new(
                    arrow::datatypes::Field::new("v", arrow::datatypes::DataType::Int64, true),
                )],
            },
            base: Some(declaration.expected_base().clone()),
            flavor: ConnectorWriteSessionFlavor::ApplicationDocumentPublication {
                declaration,
                shape:
                    novarocks_spi::connector::write_stack::ConnectorManagedPublicationShape::Data,
            },
            context: request_context(),
        }
    }

    pub(crate) fn request_context() -> ConnectorRequestContext {
        ConnectorRequestContext::try_new(
            std::time::Instant::now() + std::time::Duration::from_secs(60),
            novarocks_spi::connector::ConnectorStopOwner::new().view(),
            novarocks_spi::connector::MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
            novarocks_spi::connector::MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
        )
        .expect("request context")
    }

    // ---- the terminal storage capability ----------------------------------

    /// The table location the commit reads and writes through. The provider
    /// resolves object-store access for it before it can reload metadata or
    /// write a manifest.
    const TABLE_LOCATION: &str = "s3://warehouse/db/t/metadata/v2.metadata.json";
    const TABLE_PREFIX: &str = "s3://warehouse/db/t";

    /// What a provider does at commit time. A terminal request that cannot
    /// answer this cannot commit, whatever else it carries.
    fn probe_vended_storage(context: &ConnectorRequestContext) -> Result<String, String> {
        let resolver = context
            .storage_resolver()
            .ok_or_else(|| "terminal request has no storage resolver".to_string())?;
        let request = StorageAccessRequest::try_new(catalog_handle(), TABLE_LOCATION)
            .map_err(|error| error.to_string())?;
        resolver
            .resolve_vended_s3(&request)
            .map(|access| access.matched_prefix().as_str().to_string())
            .map_err(|error| error.to_string())
    }

    /// The frontend accounting a real query attempt keeps around its vended
    /// credential leases, reduced to what a write session can observe: the
    /// terminal capability resolves only after the attempt finalized, an
    /// outstanding hold defers the lease cleanup that finalization would
    /// otherwise perform, and releasing the last hold performs it.
    #[derive(Default)]
    struct TerminalCredentialAccounting {
        holds: AtomicUsize,
        finalized: std::sync::atomic::AtomicBool,
        leases_cleared: std::sync::atomic::AtomicBool,
    }

    impl TerminalCredentialAccounting {
        fn finalize(&self) {
            self.finalized.store(true, Ordering::SeqCst);
            if self.holds.load(Ordering::SeqCst) == 0 {
                self.leases_cleared.store(true, Ordering::SeqCst);
            }
        }

        fn release_hold(&self) {
            if self.holds.fetch_sub(1, Ordering::SeqCst) == 1
                && self.finalized.load(Ordering::SeqCst)
            {
                self.leases_cleared.store(true, Ordering::SeqCst);
            }
        }

        fn leases_cleared(&self) -> bool {
            self.leases_cleared.load(Ordering::SeqCst)
        }

        fn holds(&self) -> usize {
            self.holds.load(Ordering::SeqCst)
        }
    }

    struct TerminalCredentialCapability {
        accounting: Arc<TerminalCredentialAccounting>,
    }

    impl ConnectorStorageResolver for TerminalCredentialCapability {
        fn resolve_vended_s3(
            &self,
            request: &StorageAccessRequest,
        ) -> Result<ResolvedVendedS3Access, ConnectorError> {
            if !self.accounting.finalized.load(Ordering::SeqCst) {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "vended storage terminal capability is unavailable before finalization",
                ));
            }
            if self.accounting.leases_cleared() {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "vended storage access is unavailable for this query attempt",
                ));
            }
            let prefix =
                StorageCredentialScopePrefix::try_from_normalized(TABLE_PREFIX).expect("prefix");
            if !request.location().starts_with(prefix.as_str()) {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "vended storage access is out of the leased scope",
                ));
            }
            Ok(ResolvedVendedS3Access::new(
                StorageAccessDomainId::from_bytes([9; 32]),
                CredentialLeaseId::try_from_bytes([5; 16]).expect("lease id"),
                1,
                prefix,
                None,
                Some(novarocks_spi::connector::VendedS3SeedGrantId::new()),
                Some(novarocks_spi::connector::VendedS3SeedMaterial::new(
                    u64::MAX,
                    novarocks_secret::SecretValue::new("access"),
                    novarocks_secret::SecretValue::new("secret"),
                    novarocks_secret::SecretValue::new("token"),
                )),
            ))
        }
    }

    impl Drop for TerminalCredentialCapability {
        fn drop(&mut self) {
            self.accounting.release_hold();
        }
    }

    impl TerminalCredentialAccounting {
        /// Hands out the same terminal-only capability a real vended-credential
        /// attempt hands the session, taking a hold for it.
        fn retain_capability(self: &Arc<Self>) -> Arc<dyn ConnectorStorageResolver> {
            self.holds.fetch_add(1, Ordering::SeqCst);
            Arc::new(TerminalCredentialCapability {
                accounting: Arc::clone(self),
            })
        }
    }

    /// Drive the two coordinator steps that stand between an attempt's
    /// credential leases and a write's external commit: retain the terminal
    /// capability while the attempt still owns its leases, then finish the
    /// attempt.
    fn finalize_attempt_retaining_terminal_storage(
        session: &Arc<ConnectorWriteSession>,
    ) -> Arc<TerminalCredentialAccounting> {
        let accounting = Arc::new(TerminalCredentialAccounting::default());
        session
            .retain_terminal_storage_for_test(accounting.retain_capability())
            .unwrap();
        accounting.finalize();
        accounting
    }

    /// A canonical provider-neutral commit-fragment envelope. The fake provider
    /// owns the private payload and the session exercises the real outer codec.
    pub(crate) fn fragment_bytes(path: &str) -> Vec<u8> {
        use prost::Message;
        write_dto::ConnectorCommitFragment {
            provider_payload: Some(encode_connector_payload_message(&encoded_payload(
                ConnectorCodecCategory::CommitFragment,
                bytes::Bytes::copy_from_slice(path.as_bytes()),
            ))),
        }
        .encode_to_vec()
    }

    pub(crate) fn empty_prepared() -> DecodedPreparedWriteSet {
        DecodedPreparedWriteSet::for_test(0, Vec::new())
    }

    fn evidence() -> ExternalMutationEvidence {
        ExternalMutationEvidence::try_new(
            1,
            ConnectorInstanceDescriptor {
                provider_id: ConnectorProviderId::parse("fake").expect("provider id"),
                instance_id: catalog_handle().catalog_name().clone(),
            },
            novarocks_spi::connector::ProviderBindingEpoch::new(),
            novarocks_spi::connector::ConnectorMutationOperationId::new(),
            "write",
            bytes::Bytes::new(),
        )
        .expect("evidence")
    }

    /// Bytes a producer could actually emit, so a test that hands a fragment
    /// to a session exercises the real decode path rather than a stub.
    pub(crate) fn commit_fragment_bytes() -> Vec<u8> {
        fragment_bytes("s3://bucket/db/t/data/new.parquet")
    }

    pub(crate) fn known_committed() -> ExternalMutationOutcome<ConnectorWriteReceipt> {
        ExternalMutationOutcome::KnownCommitted {
            cleanup: novarocks_spi::connector::ExternalMutationFinalization::Complete,
            effect: novarocks_spi::connector::ExternalMutationEffect::Applied,
            receipt: ConnectorWriteReceipt::try_new(bytes::Bytes::from_static(b"receipt"))
                .expect("receipt"),
            finalization: novarocks_spi::connector::ExternalMutationFinalization::Complete,
        }
    }

    pub(crate) fn commit_unknown() -> ExternalMutationOutcome<ConnectorWriteReceipt> {
        ExternalMutationOutcome::CommitUnknown {
            failure: novarocks_spi::connector::ConnectorMutationFailure::new(
                novarocks_spi::connector::ConnectorMutationFailureKind::Unavailable,
                "scripted commit outcome is unknown",
            ),
            evidence: evidence(),
        }
    }

    #[test]
    fn logical_row_projection_uses_provider_effect_without_committing_or_changing_affected_rows() {
        use novarocks_spi::connector::write_stack::ConnectorWriteRowChange;
        let fixture = fixture_with_outcome(1, 16, known_committed());
        fixture
            .session
            .receive_for_test(DecodedPreparedWriteSet::for_test(1, Vec::new()))
            .unwrap();
        fixture.recorded.lock().unwrap().row_change = Some(Some(ConnectorWriteRowChange::Delta {
            inserted: 1,
            deleted: 0,
        }));
        assert_eq!(
            fixture
                .session
                .logical_rows_after_received_write(Some(2), &request_context())
                .unwrap(),
            Some(3)
        );
        assert_eq!(fixture.session.accumulated_row_count().unwrap(), 1);
        assert_eq!(fixture.session.finish_invocations(), 0);
        fixture.recorded.lock().unwrap().row_change = Some(None);
        assert_eq!(
            fixture
                .session
                .logical_rows_after_received_write(Some(2), &request_context())
                .unwrap(),
            None
        );
        fixture.recorded.lock().unwrap().row_change = None;
        assert!(
            fixture
                .session
                .logical_rows_after_received_write(Some(2), &request_context())
                .is_err()
        );
        assert_eq!(fixture.recorded.lock().unwrap().row_change_queries, 3);
        let completed = fixture.session.publish_for_test(request_context()).unwrap();
        assert!(matches!(completed, novarocks_spi::connector::write_stack::ConnectorWritePublicationCompletion::KnownCommitted { .. }));
        assert_eq!(fixture.recorded.lock().unwrap().finish_rows, 1);
        assert_eq!(fixture.session.finish_invocations(), 1);
    }

    #[test]
    fn received_row_projection_and_publication_borrow_one_original_packet() {
        use novarocks_spi::connector::write_stack::ConnectorWriteRowChange;
        let fixture = fixture_with_statistics(1);
        let target = WriteTargetOrdinal::try_new(0).unwrap();
        let decoded = fixture
            .session
            .computation_decode(&[target])
            .unwrap()
            .interpret(DecodedPreparedWriteSet::for_test_with_statistics(
                1,
                vec![(target, commit_fragment_bytes())],
                vec![statistics_artifact(0, 1, b"original-statistics")],
            ))
            .unwrap();
        fixture.session.receiving_port().accept(decoded).unwrap();
        {
            let mut recorded = fixture.recorded.lock().unwrap();
            recorded.row_change = Some(Some(ConnectorWriteRowChange::Delta {
                inserted: 1,
                deleted: 0,
            }));
            recorded.row_change_session = Some(Arc::downgrade(&fixture.session));
        }
        assert_eq!(
            fixture
                .session
                .logical_rows_after_received_write(Some(2), &request_context())
                .unwrap(),
            Some(3)
        );
        let received = fixture
            .session
            .lock_accumulated()
            .unwrap()
            .received
            .clone()
            .unwrap();
        assert_eq!(received.prepared.row_count(), 1);
        assert_eq!(received.prepared.fragments().len(), 1);
        assert_eq!(
            received.statistics[0].draft().body().as_ref(),
            b"original-statistics"
        );
        assert!(fixture.session.lock_terminal().unwrap().is_none());
        assert_eq!(fixture.recorded.lock().unwrap().decoded_fragments, 1);
        assert!(
            fixture
                .session
                .receive_for_test(DecodedPreparedWriteSet::for_test(1, Vec::new()))
                .is_err()
        );
        assert!(fixture.session.lock_terminal().unwrap().is_none());
        let mut call = fixture
            .session
            .prepare_finish_call(request_context())
            .unwrap();
        assert!(
            fixture
                .session
                .prepare_finish_call(request_context())
                .is_err()
        );
        assert_eq!(fixture.recorded.lock().unwrap().finish, 0);
        {
            let input = fixture.session.publication_input.lock().unwrap();
            assert!(Arc::ptr_eq(&received, &input.as_ref().unwrap().received));
        }
        let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
        assert!(call.poll_completed(&mut cx).is_ready());
        let recorded = fixture.recorded.lock().unwrap();
        assert_eq!(recorded.decoded_fragments, 1);
        assert_eq!(recorded.row_change_queries, 1);
        assert_eq!(
            recorded.row_change_prepared_address,
            recorded.finish_prepared_address
        );
        assert_eq!(
            recorded.statistics[0].draft().body().as_ref(),
            b"original-statistics"
        );
        assert_eq!(recorded.finish, 1);
    }

    #[test]
    fn received_projection_refusal_or_panic_keeps_original_payload_for_abort() {
        use novarocks_failure_contract::SharedFailureCause;
        for panic in [false, true] {
            let fixture = fixture_with_outcome(1, 16, known_committed());
            let target = WriteTargetOrdinal::try_new(0).unwrap();
            fixture
                .session
                .receive_for_test(DecodedPreparedWriteSet::for_test(
                    1,
                    vec![(target, commit_fragment_bytes())],
                ))
                .unwrap();
            let failure = ConnectorError::from_domain(
                ConnectorErrorKind::ResourceExhausted,
                novarocks_workload_control::WorkError::Capacity("original row projection refusal"),
            );
            let original = std::error::Error::source(&failure)
                .unwrap()
                .downcast_ref::<SharedFailureCause>()
                .unwrap()
                .clone();
            {
                let mut recorded = fixture.recorded.lock().unwrap();
                recorded.row_change_failure = Some(failure);
                recorded.panic_row_change = panic;
                recorded.row_change_session = Some(Arc::downgrade(&fixture.session));
            }
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                fixture
                    .session
                    .logical_rows_after_received_write(Some(2), &request_context())
            }));
            if panic {
                assert!(result.is_err());
            } else {
                let failure = result.unwrap().unwrap_err();
                assert_eq!(failure.kind(), ConnectorErrorKind::ResourceExhausted);
                assert_eq!(
                    std::error::Error::source(&failure)
                        .unwrap()
                        .downcast_ref::<SharedFailureCause>()
                        .unwrap(),
                    &original
                );
            }
            let received = fixture
                .session
                .lock_accumulated()
                .unwrap()
                .received
                .clone()
                .unwrap();
            assert_eq!(received.prepared.fragments().len(), 1);
            assert!(fixture.session.lock_terminal().unwrap().is_none());
            let mut call = fixture
                .session
                .prepare_abort_call(request_context())
                .unwrap();
            let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
            assert!(call.poll_completed(&mut cx).is_ready());
            assert_eq!(fixture.recorded.lock().unwrap().row_change_queries, 1);
            assert_eq!(fixture.recorded.lock().unwrap().finish, 0);
            assert_eq!(received.prepared.fragments().len(), 1);
        }
    }

    #[test]
    fn received_input_validation_refusal_moves_no_original_inventory() {
        let (_control, _root, binding, _capacity) =
            crate::query_execution::internal_result_cpu::admitted_internal_fixture();
        let fixture = fixture_with_statistics(1);
        let target = WriteTargetOrdinal::try_new(0).unwrap();
        fixture
            .session
            .receive_for_test(
                DecodedPreparedWriteSet::for_test_with_statistics(
                    1,
                    vec![(target, commit_fragment_bytes())],
                    vec![statistics_artifact(0, 1, b"retained-statistics")],
                )
                .with_test_capacity(&binding),
            )
            .unwrap();
        let original_address = {
            let mut union = fixture.session.lock_accumulated().unwrap();
            // Exercise the SPI receiving validator on the original unique Vec;
            // a foreign target cannot make it consume the deletion inventory.
            union.fragments[0].0 = WriteTargetOrdinal::try_new(1).unwrap();
            union.fragments.as_ptr() as usize
        };
        let failure = fixture
            .session
            .logical_rows_after_received_write(Some(2), &request_context())
            .unwrap_err();
        assert_eq!(failure.kind(), ConnectorErrorKind::InvalidRequest);
        let mut union = fixture.session.lock_accumulated().unwrap();
        assert!(union.received.is_none());
        assert_eq!(union.fragments.as_ptr() as usize, original_address);
        assert_eq!(union.fragments.len(), 1);
        assert_eq!(
            union.statistics[0].draft().body().as_ref(),
            b"retained-statistics"
        );
        assert!(union.retention.is_some());
        assert_eq!(union.rows.get(), 1);
        union.fragments[0].0 = target;
        assert_eq!(fixture.recorded.lock().unwrap().row_change_queries, 0);
    }

    #[test]
    fn unknown_publication_retains_the_same_projected_packet() {
        let fixture = fixture_with_outcome(1, 16, commit_unknown());
        let target = WriteTargetOrdinal::try_new(0).unwrap();
        fixture
            .session
            .receive_for_test(DecodedPreparedWriteSet::for_test(
                1,
                vec![(target, commit_fragment_bytes())],
            ))
            .unwrap();
        fixture.recorded.lock().unwrap().row_change = Some(None);
        assert_eq!(
            fixture
                .session
                .logical_rows_after_received_write(Some(2), &request_context())
                .unwrap(),
            None
        );
        let original = fixture
            .session
            .lock_accumulated()
            .unwrap()
            .received
            .clone()
            .unwrap();
        let mut call = fixture
            .session
            .prepare_finish_call(request_context())
            .unwrap();
        let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
        assert!(call.poll_completed(&mut cx).is_ready());
        assert!(matches!(call.outcome(), Some(Ok(novarocks_spi::connector::write_stack::ConnectorWritePublicationCompletion::CommitUnknown { .. }))));
        assert!(call.retains_original_unknown());
        assert!(Arc::ptr_eq(
            &original,
            fixture
                .session
                .lock_accumulated()
                .unwrap()
                .received
                .as_ref()
                .unwrap()
        ));
        assert_eq!(original.prepared.fragments().len(), 1);
        assert_eq!(fixture.recorded.lock().unwrap().decoded_fragments, 1);
        assert_eq!(fixture.recorded.lock().unwrap().finish, 1);
        assert!(
            fixture
                .session
                .logical_rows_after_received_write(Some(2), &request_context())
                .is_err()
        );
        assert_eq!(fixture.recorded.lock().unwrap().row_change_queries, 1);
    }

    #[test]
    fn received_packet_and_last_buffer_alias_keep_only_the_original_receipts() {
        let (_control, root, legacy_binding, capacity) =
            crate::query_execution::internal_result_cpu::admitted_internal_fixture();
        drop(legacy_binding);
        let binding = novarocks_query_application::admitted_query_context::QueryResultCapacityBinding::from_root_owner(&root.owner).unwrap();
        let observed = binding.window_alias();
        assert_eq!(capacity.snapshot().held_positions, [0; 4]);
        let fixture = fixture_with_outcome(1, 16, known_committed());
        let target = WriteTargetOrdinal::try_new(0).unwrap();
        let encoded = commit_fragment_bytes();
        let encoded_bytes = encoded.len() as u64;
        fixture.recorded.lock().unwrap().retain_decoded_payload = true;
        fixture
            .session
            .receive_for_test(
                DecodedPreparedWriteSet::for_test(1, vec![(target, encoded)])
                    .with_test_capacity(&binding),
            )
            .unwrap();
        // The canonical backing exited. Its one real outer copy remains in
        // the Provider's actual Bytes, together with only that copy's receipt.
        assert_eq!(observed.retained_payload_bytes(), encoded_bytes);
        fixture.recorded.lock().unwrap().row_change = Some(None);
        assert_eq!(
            fixture
                .session
                .logical_rows_after_received_write(Some(2), &request_context())
                .unwrap(),
            None
        );
        let received = fixture
            .session
            .lock_accumulated()
            .unwrap()
            .received
            .clone()
            .unwrap();
        let payload = fixture
            .recorded
            .lock()
            .unwrap()
            .row_change_payload
            .take()
            .unwrap();
        drop(binding);
        root.owner.complete();
        root.business.release();
        assert_eq!(capacity.snapshot().held_positions, [0; 4]);
        let weak = Arc::downgrade(&fixture.session);
        drop(fixture);
        assert!(weak.upgrade().is_none());
        assert_eq!(capacity.snapshot().held_positions, [0; 4]);
        drop(received);
        assert_eq!(capacity.snapshot().held_positions, [0; 4]);
        assert_eq!(observed.retained_payload_bytes(), encoded_bytes);
        drop(payload);
        assert_eq!(observed.retained_payload_bytes(), 0);
    }

    #[test]
    fn ordinary_begin_finishes_without_an_application_publication() {
        let fixture = fixture_with_statistics(1);

        let _ = fixture
            .session
            .publish_prepared_for_test(empty_prepared(), request_context());

        assert_eq!(
            fixture.recorded.lock().expect("recorded").publication,
            Some(ConnectorWriteFinishPublication::None)
        );
    }

    #[test]
    fn finish_sends_the_exact_application_publication_frozen_at_begin() {
        let (declaration, publication) = application_document_publication();
        let expected = ConnectorWriteFinishPublication::KeyedDocuments(publication.clone());
        let fixture = fixture_with_outcome_and_publication(
            1,
            16,
            known_committed(),
            Some((declaration, publication)),
        );

        fixture
            .session
            .publish_prepared_for_test(empty_prepared(), request_context())
            .expect("finish");

        assert_eq!(
            fixture.recorded.lock().expect("recorded").publication,
            Some(expected)
        );
    }

    #[tokio::test]
    async fn pending_document_async_begin_retains_exact_declaration_and_one_late_bind() {
        struct BindingOnlyControl(ConnectorProviderBindingKey);
        impl LegacyWriteControl for BindingOnlyControl {
            fn binding_key(&self) -> &ConnectorProviderBindingKey {
                &self.0
            }
        }
        let (declaration, publication) = application_document_publication();
        let expected = ConnectorWriteFinishPublication::KeyedDocuments(publication.clone());
        let (lease, recorded) = unopened_fixture(1, 16, known_committed());
        let key = lease.session().binding_key().clone();
        let write_lease = novarocks_spi::connector::ConnectorWriteLease::new(
            key.clone(),
            Arc::new(BindingOnlyControl(key)),
            || {},
        )
        .unwrap()
        .with_catalog_properties(catalog_properties())
        .unwrap();
        let original = Arc::new(Mutex::new(None));
        let receiving = WriteBeginReceiving::new(Arc::clone(&original));
        let session = begin_connector_application_document_write_session_pending_async_in(
            lease,
            &write_lease,
            application_document_begin_request(declaration.clone()),
            &receiving,
        )
        .await
        .unwrap();
        assert!(Arc::ptr_eq(
            &session,
            original.lock().unwrap().as_ref().unwrap()
        ));
        assert!(Arc::ptr_eq(&session, &receiving.session().unwrap()));
        assert_eq!(
            session.pending_publication_declaration().unwrap(),
            declaration
        );
        assert!(!session.metadata_only_publication);
        assert!(session.publish_for_test(request_context()).is_err());
        assert_eq!(recorded.lock().unwrap().finish, 0);
        session.receive_for_test(empty_prepared()).unwrap();
        session
            .bind_keyed_document_publication(publication.clone())
            .unwrap();
        assert!(
            session
                .bind_keyed_document_publication(publication)
                .is_err()
        );
        session.publish_for_test(request_context()).unwrap();
        let recorded = recorded.lock().unwrap();
        assert_eq!(recorded.begin, 1);
        assert_eq!(recorded.begin_async, 1);
        assert_eq!(recorded.begin_future_drops, 1);
        assert_eq!(recorded.begin_declaration, Some(declaration));
        assert_eq!(recorded.finish, 1);
        assert_eq!(recorded.publication, Some(expected));
        assert!(receiving.local_health_failure().is_none());
    }

    #[tokio::test]
    async fn pending_document_async_metadata_only_permission_comes_from_the_declaration() {
        let (declaration, publication) = application_document_publication_for(
            novarocks_spi::connector::ConnectorManagedPublicationTechnique::MetadataOnly,
        );
        let expected = ConnectorWriteFinishPublication::KeyedDocuments(publication.clone());
        let (lease, recorded) = unopened_fixture(1, 16, known_committed());
        let original = Arc::new(Mutex::new(None));
        let receiving = WriteBeginReceiving::new(Arc::clone(&original));
        let mut request = application_document_begin_request(declaration.clone());
        request.intent = novarocks_spi::connector::ConnectorWriteIntent::Append;
        let session =
            ConnectorWriteSession::begin_pending_application_document_publication_async_in(
                lease,
                catalog_properties(),
                request,
                &receiving,
            )
            .await
            .unwrap();
        assert!(Arc::ptr_eq(
            &session,
            original.lock().unwrap().as_ref().unwrap()
        ));
        assert_eq!(
            session.pending_publication_declaration().unwrap(),
            declaration
        );
        assert!(session.metadata_only_publication);
        assert!(session.publish_for_test(request_context()).is_err());
        assert_eq!(
            recorded.lock().unwrap().finish,
            0,
            "pending declaration is not a bound publication"
        );
        session
            .bind_keyed_document_publication(publication)
            .unwrap();
        session.publish_for_test(request_context()).unwrap();
        let recorded = recorded.lock().unwrap();
        assert_eq!(recorded.begin_async, 1);
        assert_eq!(recorded.begin_declaration, Some(declaration));
        assert_eq!(recorded.finish, 1);
        assert_eq!(recorded.finish_rows, 0);
        assert_eq!(recorded.publication, Some(expected));
    }

    #[test]
    fn pending_document_async_drop_panic_retains_plan_input_and_exact_declaration() {
        let (declaration, _) = application_document_publication();
        let (lease, recorded) = unopened_fixture(1, 16, known_committed());
        recorded.lock().unwrap().panic_begin_future_drop = true;
        let original = Arc::new(Mutex::new(None));
        let receiving = WriteBeginReceiving::new(Arc::clone(&original));
        let mut borrowed = Box::pin(
            ConnectorWriteSession::begin_pending_application_document_publication_async_in(
                lease,
                catalog_properties(),
                application_document_begin_request(declaration.clone()),
                &receiving,
            ),
        );
        let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                std::future::Future::poll(borrowed.as_mut(), &mut cx)
            }))
            .is_err()
        );
        assert!(receiving.retains_plan());
        assert!(receiving.retains_input());
        assert!(receiving.retains_pending_declaration(&declaration));
        assert!(receiving.local_health_failure().is_some());
        assert!(receiving.completed_result().is_err());
        assert!(receiving.session().is_none());
        assert!(original.lock().unwrap().is_none());
        let recorded = recorded.lock().unwrap();
        assert_eq!(recorded.begin, 1);
        assert_eq!(recorded.begin_async, 1);
        assert_eq!(recorded.begin_future_drops, 1);
        assert_eq!(recorded.begin_declaration, Some(declaration));
        assert_eq!(recorded.finish, 0);
    }

    #[tokio::test]
    async fn async_document_and_ordinary_entries_reject_the_other_flavor_before_begin() {
        let (lease, recorded) = unopened_fixture(1, 16, known_committed());
        let receiving = WriteBeginReceiving::new(Arc::new(Mutex::new(None)));
        let failure =
            ConnectorWriteSession::begin_pending_application_document_publication_async_in(
                lease,
                catalog_properties(),
                begin_request(),
                &receiving,
            )
            .await
            .err()
            .expect("ordinary request cannot acquire an application-document session");
        assert_eq!(failure.kind(), ConnectorErrorKind::InvalidRequest);
        assert_eq!(recorded.lock().unwrap().begin, 0);
        assert_eq!(recorded.lock().unwrap().begin_async, 0);
        assert!(receiving.session().is_none());
        assert!(!receiving.retains_input());

        let (declaration, _) = application_document_publication();
        let (lease, recorded) = unopened_fixture(1, 16, known_committed());
        let receiving = WriteBeginReceiving::new(Arc::new(Mutex::new(None)));
        let failure = ConnectorWriteSession::begin_async_in(
            lease,
            catalog_properties(),
            application_document_begin_request(declaration),
            &receiving,
        )
        .await
        .err()
        .expect("ordinary async entry cannot erase an application-document declaration");
        assert_eq!(failure.kind(), ConnectorErrorKind::InvalidRequest);
        assert_eq!(recorded.lock().unwrap().begin, 0);
        assert_eq!(recorded.lock().unwrap().begin_async, 0);
        assert!(receiving.session().is_none());
    }

    #[test]
    fn pending_application_publication_binds_once_after_execution_and_reaches_finish() {
        let (declaration, publication) = application_document_publication();
        let expected = ConnectorWriteFinishPublication::KeyedDocuments(publication.clone());
        let (lease, recorded) = unopened_fixture(1, 16, known_committed());
        let session = begin_pending_for_test(
            lease,
            catalog_properties(),
            application_document_begin_request(declaration),
        )
        .expect("begin pending application-document publication");

        session
            .receive_for_test(empty_prepared())
            .expect("execution result");
        session
            .bind_keyed_document_publication(publication)
            .expect("bind exact publication");
        session
            .publish_for_test(request_context())
            .expect("finish bound publication");

        let recorded = recorded.lock().expect("recorded");
        assert_eq!(recorded.begin, 1);
        assert_eq!(recorded.finish, 1);
        assert_eq!(recorded.publication, Some(expected));
    }

    #[test]
    fn pending_application_publication_refuses_finish_before_provider_invocation() {
        let (declaration, publication) = application_document_publication();
        let (lease, recorded) = unopened_fixture(1, 16, known_committed());
        let session = begin_pending_for_test(
            lease,
            catalog_properties(),
            application_document_begin_request(declaration),
        )
        .expect("begin pending application-document publication");

        let error = session
            .publish_prepared_for_test(empty_prepared(), request_context())
            .expect_err("pending publication must not finish");
        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
        assert_eq!(session.finish_invocations(), 0);
        assert_eq!(recorded.lock().expect("recorded").finish, 0);

        session
            .bind_keyed_document_publication(publication)
            .expect("failed finish must leave publication bindable");
        session
            .publish_for_test(request_context())
            .expect("finish after bind");
        assert_eq!(recorded.lock().expect("recorded").finish, 1);
    }

    #[test]
    fn application_document_publication_rejects_a_second_bind() {
        let (declaration, publication) = application_document_publication();
        let (lease, recorded) = unopened_fixture(1, 16, known_committed());
        let session = begin_pending_for_test(
            lease,
            catalog_properties(),
            application_document_begin_request(declaration),
        )
        .expect("begin pending application-document publication");

        session
            .bind_keyed_document_publication(publication.clone())
            .expect("first bind");
        let error = session
            .bind_keyed_document_publication(publication)
            .expect_err("second bind must be rejected");

        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
        assert_eq!(recorded.lock().expect("recorded").finish, 0);
    }

    #[test]
    fn pending_application_publication_rejects_a_mismatched_bind() {
        let (declaration, publication) = application_document_publication();
        let (_, other_publication) = application_document_publication();
        let (lease, recorded) = unopened_fixture(1, 16, known_committed());
        let session = begin_pending_for_test(
            lease,
            catalog_properties(),
            application_document_begin_request(declaration),
        )
        .expect("begin pending application-document publication");

        let error = session
            .bind_keyed_document_publication(other_publication)
            .expect_err("mismatched publication must be rejected");
        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
        assert_eq!(recorded.lock().expect("recorded").finish, 0);

        session
            .bind_keyed_document_publication(publication)
            .expect("mismatched bind must leave the exact bind available");
    }

    #[test]
    fn ordinary_write_session_rejects_application_document_bind() {
        let (_, publication) = application_document_publication();
        let fixture = fixture(1, 16);

        let error = fixture
            .session
            .bind_keyed_document_publication(publication)
            .expect_err("ordinary write must reject document publication");

        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
        assert_eq!(fixture.recorded.lock().expect("recorded").finish, 0);
    }

    #[test]
    fn ordinary_begin_rejects_an_application_document_flavor_before_provider_begin() {
        let (declaration, _) = application_document_publication();
        let (lease, recorded) = unopened_fixture(1, 16, known_committed());

        let error = begin_for_test(
            lease,
            catalog_properties(),
            application_document_begin_request(declaration),
        )
        .err()
        .expect("application-document flavor must use the exact publication entrypoint");

        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
        let recorded = recorded.lock().expect("recorded");
        assert_eq!(recorded.begin, 0);
        assert_eq!(recorded.finish, 0);
    }

    #[test]
    fn application_document_begin_rejects_an_ordinary_flavor_before_provider_begin() {
        let (lease, recorded) = unopened_fixture(1, 16, known_committed());

        let error = begin_pending_for_test(lease, catalog_properties(), begin_request())
            .err()
            .expect("publication intent must require an application-document flavor");

        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
        let recorded = recorded.lock().expect("recorded");
        assert_eq!(recorded.begin, 0);
        assert_eq!(recorded.finish, 0);
    }

    #[test]
    fn pending_application_document_begin_rejects_an_ordinary_flavor_before_provider_begin() {
        let (lease, recorded) = unopened_fixture(1, 16, known_committed());

        let error = begin_pending_for_test(lease, catalog_properties(), begin_request())
            .err()
            .expect("pending publication requires an application-document flavor");

        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
        let recorded = recorded.lock().expect("recorded");
        assert_eq!(recorded.begin, 0);
        assert_eq!(recorded.finish, 0);
    }

    #[test]
    fn application_document_pending_bind_rejects_a_mismatched_intent_without_finishing() {
        let (declaration, publication) = application_document_publication();
        let (_, other_publication) = application_document_publication();
        let (lease, recorded) = unopened_fixture(1, 16, known_committed());
        let session = begin_pending_for_test(
            lease,
            catalog_properties(),
            application_document_begin_request(declaration),
        )
        .unwrap();
        let error = session
            .bind_keyed_document_publication(other_publication)
            .expect_err("publication intent must match the exact begin declaration");
        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
        assert_eq!(recorded.lock().expect("recorded").begin, 1);
        assert_eq!(recorded.lock().expect("recorded").finish, 0);
        session
            .bind_keyed_document_publication(publication)
            .expect("mismatched intent leaves the exact bind available");
    }

    #[test]
    fn session_encoding_and_sealing_retain_the_original_codec_cause() {
        use novarocks_failure_contract::{FailureProjection, SharedFailureCause};
        use novarocks_spi::connector::{
            ConnectorCodecError, ConnectorDecodeLedger, ConnectorDecodeLimits, ConnectorFieldPath,
        };

        struct RefusingEncoder(ConnectorCodecError);
        impl ConnectorWriteHandleWireEncoder for RefusingEncoder {
            fn owner(&self) -> &str {
                "fake"
            }
            fn encode_writer_handle_payload(
                &self,
                _handle: &ConnectorWriterHandle,
            ) -> Result<ConnectorEncodedPayload, ConnectorCodecError> {
                Err(self.0.clone())
            }
        }

        let mut ledger =
            ConnectorDecodeLedger::new(ConnectorDecodeLimits::try_new(8, 8, 8, 8, 8).unwrap());
        ledger.charge_raw(7).unwrap();
        let original = ConnectorCodecError::from_domain(
            ConnectorFieldPath::root("writer"),
            ledger.charge_raw(2).unwrap_err(),
        );
        let fact = original.failure_fact();
        let (lease, recorded) = unopened_fixture_with_encoder(
            1,
            known_committed(),
            Arc::new(RefusingEncoder(original.clone())),
        );
        let session = begin_for_test(lease, catalog_properties(), begin_request()).unwrap();
        let payload_error = session
            .encode_writer_handle_payload(session.targets()[0].handle())
            .expect_err("actual provider encoder refuses the payload");
        let sealed_error = session
            .seal_write_targets()
            .expect_err("the same encoder refuses the sealed carrier");
        for error in [&payload_error, &sealed_error] {
            assert_eq!(error.failure_fact(), fact);
            let mut source: &(dyn std::error::Error + 'static) = error;
            loop {
                if let Some(codec) = source.downcast_ref::<ConnectorCodecError>() {
                    assert_eq!(codec, &original);
                    let cause = std::error::Error::source(codec)
                        .unwrap()
                        .downcast_ref::<SharedFailureCause>()
                        .unwrap();
                    let expected = std::error::Error::source(&original)
                        .unwrap()
                        .downcast_ref::<SharedFailureCause>()
                        .unwrap();
                    assert_eq!(cause, expected);
                    break;
                }
                source = source.source().expect("retain the concrete original codec");
            }
        }
        session
            .abort_for_test(request_context())
            .expect("abort the same unstarted write session");
        assert_eq!(recorded.lock().unwrap().abort, 1);
    }

    #[test]
    fn a_session_seals_one_recipe_per_logical_target() {
        let fixture = fixture(3, 16);
        let sealed = fixture
            .session
            .seal_write_targets()
            .expect("sealed targets");
        assert_eq!(sealed.ordinals().collect::<Vec<_>>(), vec![0, 1, 2]);
        assert_eq!(fixture.session.expected_targets().len(), 3);
    }

    #[test]
    fn the_unique_handle_budget_refuses_a_query_whose_recipes_do_not_fit() {
        // Each target's recipe is deliberately enormous, so a handful of
        // logical targets is enough to exceed the whole-query budget.
        let per_handle = 4 * 1024 * 1024;
        let targets = MAX_CONNECTOR_UNIQUE_WRITER_HANDLE_BYTES / per_handle + 1;
        let fixture = fixture(targets, per_handle);
        let error = fixture
            .session
            .seal_write_targets()
            .expect_err("over the unique handle budget");
        assert_eq!(error.kind(), ConnectorErrorKind::ResourceExhausted);
    }

    #[test]
    fn a_session_reaches_exactly_one_terminal_decision() {
        let fixture = fixture(1, 16);
        assert_eq!(fixture.session.finish_invocations(), 0);

        let prepared = empty_prepared();
        let _ = fixture
            .session
            .publish_prepared_for_test(prepared, request_context());
        assert_eq!(fixture.session.finish_invocations(), 1);
        assert_eq!(fixture.recorded.lock().expect("recorded").finish, 1);

        // A second, different decision is refused, and the connector is not
        // asked again.
        let error = fixture
            .session
            .abort_for_test(request_context())
            .expect_err("abort after commit");
        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
        assert_eq!(fixture.recorded.lock().expect("recorded").abort, 0);
    }

    /// The frontend half of the write, composed the way production composes
    /// it: begin, seal the recipes into the plan, read the root result back,
    /// gate on both facts, commit.
    ///
    /// Each piece has its own tests; this one exists because they have to fit
    /// together, and a mismatch between them -- a target the encoder cannot
    /// find, a relation the decoder cannot read, a set the session refuses --
    /// is exactly the kind of defect no single unit test can see.
    #[test]
    fn computation_receiving_decodes_once_and_retains_original_window_until_commit() {
        let (_control, root, binding, capacity) =
            crate::query_execution::internal_result_cpu::admitted_internal_fixture();
        let fixture = fixture(1, 16);
        let target = WriteTargetOrdinal::try_new(0).unwrap();
        let encoded = commit_fragment_bytes();
        let encoded_bytes = encoded.len() as u64;
        let prepared = DecodedPreparedWriteSet::for_test(7, vec![(target, encoded)])
            .with_test_capacity(&binding);
        let original_window = binding.window_alias();
        assert_eq!(original_window.retained_payload_bytes(), encoded_bytes);
        let decoded = fixture
            .session
            .computation_decode(&[target])
            .unwrap()
            .interpret(prepared)
            .unwrap();
        // FakeDecoder retains no original payload alias. Both its temporary
        // decode copy and the now-destroyed canonical backing have exited.
        assert_eq!(original_window.retained_payload_bytes(), 0);
        drop(original_window);
        assert_eq!(fixture.recorded.lock().unwrap().decoded_fragments, 1);
        assert_eq!(fixture.session.finish_invocations(), 0);
        let accepted = fixture.session.receiving_port().accept(decoded).unwrap();
        assert!(accepted.belongs_to(&fixture.session));
        assert_eq!(accepted.row_count(), 7);
        assert!(!accepted.is_empty());
        drop(binding);
        root.owner.complete();
        root.business.release();
        assert_eq!(capacity.snapshot().held_positions[2], 1);
        let _ = fixture.session.publish_for_test(request_context()).unwrap();
        assert_eq!(
            fixture.recorded.lock().unwrap().decoded_fragments,
            1,
            "publication cannot decode again"
        );
        assert_eq!(fixture.session.finish_invocations(), 1);
        drop(fixture);
        assert_eq!(capacity.snapshot().held_positions, [0; 4]);
    }

    #[test]
    fn computation_receiving_rejects_a_different_original_session_before_union_mutation() {
        let first = fixture(1, 16);
        let second = fixture(1, 16);
        let target = WriteTargetOrdinal::try_new(0).unwrap();
        let decoded = first
            .session
            .computation_decode(&[target])
            .unwrap()
            .interpret(DecodedPreparedWriteSet::for_test(
                7,
                vec![(target, commit_fragment_bytes())],
            ))
            .unwrap();
        assert!(second.session.receiving_port().accept(decoded).is_err());
        assert_eq!(second.session.accumulated_row_count().unwrap(), 0);
        assert_eq!(second.session.finish_invocations(), 0);
    }

    #[test]
    fn the_frontend_write_path_composes_from_begin_to_commit() {
        use novarocks_plan_codec::SealedWriteTargets;

        let fixture = fixture_with_outcome(
            1,
            16,
            ExternalMutationOutcome::KnownUncommitted {
                failure: novarocks_spi::connector::ConnectorMutationFailure::new(
                    novarocks_spi::connector::ConnectorMutationFailureKind::Unavailable,
                    "scripted",
                ),
                cleanup: novarocks_spi::connector::ExternalMutationFinalization::Complete,
            },
        );

        // 1. The session seals one recipe per logical target, and the sealed
        //    targets are what the plan encoder consumes.
        let sealed: SealedWriteTargets = fixture
            .session
            .seal_write_targets()
            .expect("sealed targets");
        assert_eq!(sealed.ordinals().collect::<Vec<_>>(), vec![0]);

        // 2. The backends report their fragments through the root relation.
        //    Round-trip a canonical outer fragment envelope so the decoder is
        //    exercised against bytes a producer could emit.
        let fragment_bytes = commit_fragment_bytes();
        let prepared = DecodedPreparedWriteSet::for_test(
            7,
            vec![(
                WriteTargetOrdinal::try_new(0).expect("ordinal"),
                fragment_bytes,
            )],
        );

        // 3. The same session interprets and accepts this exact generation;
        //    Task readiness belongs to the registered consumer component tests.
        let decoded = fixture
            .session
            .computation_decode(&[WriteTargetOrdinal::try_new(0).expect("ordinal")])
            .unwrap()
            .interpret(prepared)
            .unwrap();
        let accepted = fixture.session.receiving_port().accept(decoded).unwrap();
        assert!(accepted.belongs_to(&fixture.session));
        assert_eq!(accepted.row_count(), 7);
        let mut call = fixture
            .session
            .prepare_finish_call(request_context())
            .unwrap();
        let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
        assert!(call.poll_completed(&mut cx).is_ready());
        let mut outcome = None;
        call.take_outcome_in(&mut outcome).unwrap();
        let outcome = outcome.unwrap().expect("finish reaches the connector");
        assert!(matches!(
            outcome,
            novarocks_spi::connector::write_stack::ConnectorWritePublicationCompletion::KnownUncommitted { .. }
        ));
        assert_eq!(fixture.session.finish_invocations(), 1);
    }

    /// A copy-on-write mutation and a distributed rewrite drive several
    /// queries against one session and commit once. Each query's set is
    /// complete for its own graph; the statement commits their union.
    #[test]
    fn write_union_reuses_one_exact_window_and_rejects_foreign_admission() {
        let (_control, root, binding, capacity) =
            crate::query_execution::internal_result_cpu::admitted_internal_fixture();
        let (_foreign_control, foreign_root, foreign_binding, foreign_capacity) =
            crate::query_execution::internal_result_cpu::admitted_internal_fixture();
        let fixture = fixture(1, 16);
        let target = WriteTargetOrdinal::try_new(0).unwrap();
        for path in ["s3://b/a.parquet", "s3://b/b.parquet"] {
            fixture
                .session
                .receive_for_test(
                    DecodedPreparedWriteSet::for_test(4, vec![(target, fragment_bytes(path))])
                        .with_test_capacity(&binding),
                )
                .unwrap();
        }
        let error = fixture
            .session
            .receive_for_test(
                DecodedPreparedWriteSet::for_test(
                    4,
                    vec![(target, fragment_bytes("s3://b/c.parquet"))],
                )
                .with_test_capacity(&foreign_binding),
            )
            .unwrap_err();
        assert!(error.to_string().contains("allowance"));
        assert_eq!(fixture.session.accumulated_row_count().unwrap(), 8);
        drop(foreign_binding);
        foreign_root.owner.complete();
        foreign_root.business.release();
        assert_eq!(foreign_capacity.snapshot().held_positions, [0; 4]);
        drop(binding);
        root.owner.complete();
        root.business.release();
        assert_eq!(capacity.snapshot().held_positions, [0, 0, 1, 0]);
        let _ = fixture.session.publish_for_test(request_context());
        assert_eq!(fixture.session.finish_invocations(), 1);
        drop(fixture);
        assert_eq!(capacity.snapshot().held_positions, [0; 4]);
    }

    #[test]
    fn a_session_commits_the_union_of_every_query_it_drove() {
        let fixture = fixture(1, 16);
        let target = WriteTargetOrdinal::try_new(0).expect("ordinal");

        fixture
            .session
            .receive_for_test(DecodedPreparedWriteSet::for_test(
                4,
                vec![(target, fragment_bytes("s3://b/a.parquet"))],
            ))
            .expect("first query");
        fixture
            .session
            .receive_for_test(DecodedPreparedWriteSet::for_test(
                6,
                vec![
                    (target, fragment_bytes("s3://b/b.parquet")),
                    (target, fragment_bytes("s3://b/c.parquet")),
                ],
            ))
            .expect("second query");

        assert_eq!(
            fixture
                .session
                .accumulated_row_count()
                .expect("accumulated rows"),
            10
        );
        assert_eq!(fixture.session.finish_invocations(), 0);

        let _ = fixture.session.publish_for_test(request_context());
        // One commit for the whole statement, not one per query.
        assert_eq!(fixture.session.finish_invocations(), 1);
        assert_eq!(fixture.recorded.lock().expect("recorded").finish, 1);
    }

    #[test]
    fn finish_callback_retains_fragment_only_capacity_after_root_logical_exit() {
        let (_control, root, binding, capacity) =
            crate::query_execution::internal_result_cpu::admitted_internal_fixture();
        let fixture = fixture(1, 1);
        fixture.recorded.lock().unwrap().expected_finish_capacity = Some(capacity.clone());
        fixture
            .session
            .receive_for_test(
                DecodedPreparedWriteSet::for_test(
                    7,
                    vec![(
                        WriteTargetOrdinal::try_new(0).unwrap(),
                        commit_fragment_bytes(),
                    )],
                )
                .with_test_capacity(&binding),
            )
            .expect("accumulate admitted fragments");
        drop(binding);
        root.owner.complete();
        assert_eq!(capacity.snapshot().held_positions, [0, 0, 1, 0]);
        fixture
            .session
            .publish_for_test(request_context())
            .expect("provider callback returned");
        assert_eq!(fixture.recorded.lock().unwrap().finish, 1);
        drop(fixture);
        assert_eq!(capacity.snapshot().held_positions, [0; 4]);
    }

    #[test]
    fn statistics_artifacts_follow_the_prepared_set_into_the_single_commit() {
        let fixture = fixture_with_statistics(2);
        fixture
            .session
            .receive_for_test(DecodedPreparedWriteSet::for_test_with_statistics(
                4,
                Vec::new(),
                vec![statistics_artifact(0, 1, b"zero")],
            ))
            .expect("first query statistics");
        fixture
            .session
            .receive_for_test(DecodedPreparedWriteSet::for_test_with_statistics(
                6,
                Vec::new(),
                vec![statistics_artifact(1, 2, b"one")],
            ))
            .expect("second query statistics");

        let _ = fixture.session.publish_for_test(request_context());
        let recorded = fixture.recorded.lock().expect("recorded");
        assert_eq!(recorded.finish, 1);
        assert_eq!(recorded.statistics.len(), 2);
        assert_eq!(recorded.statistics[0].target().get(), 0);
        assert_eq!(recorded.statistics[0].draft().body().as_ref(), b"zero");
        assert_eq!(recorded.statistics[1].target().get(), 1);
        assert_eq!(recorded.statistics[1].draft().body().as_ref(), b"one");
    }

    #[test]
    fn rejecting_an_unknown_statistics_artifact_is_transactional() {
        let fixture = fixture_with_statistics(1);
        let error = fixture
            .session
            .receive_for_test(DecodedPreparedWriteSet::for_test_with_statistics(
                9,
                Vec::new(),
                vec![statistics_artifact(0, 99, b"foreign")],
            ))
            .expect_err("foreign artifact identity");
        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
        assert_eq!(
            fixture
                .session
                .accumulated_row_count()
                .expect("unchanged row count"),
            0
        );

        fixture
            .session
            .receive_for_test(DecodedPreparedWriteSet::for_test_with_statistics(
                3,
                Vec::new(),
                vec![statistics_artifact(0, 1, b"valid")],
            ))
            .expect("valid set after rejection");
        let _ = fixture.session.publish_for_test(request_context());
        let recorded = fixture.recorded.lock().expect("recorded");
        assert_eq!(recorded.statistics.len(), 1);
        assert_eq!(recorded.statistics[0].draft().body().as_ref(), b"valid");
    }

    #[test]
    fn the_frozen_budgets_bound_the_union_rather_than_each_query() {
        use novarocks_spi::connector::write_stack::MAX_CONNECTOR_PREPARED_WRITE_SET_ENTRIES;

        let fixture = fixture(1, 16);
        let target = WriteTargetOrdinal::try_new(0).expect("ordinal");
        // Each query stays far inside the entry budget; together they exceed
        // it. Charging per query would have accepted every one of them.
        let per_query = MAX_CONNECTOR_PREPARED_WRITE_SET_ENTRIES / 2;
        let fragment = commit_fragment_bytes();
        for _ in 0..2 {
            fixture
                .session
                .receive_for_test(DecodedPreparedWriteSet::for_test(
                    0,
                    vec![(target, fragment.clone()); per_query],
                ))
                .expect("within the union budget");
        }
        assert_eq!(
            fixture.session.lock_accumulated().unwrap().fragments.len(),
            MAX_CONNECTOR_PREPARED_WRITE_SET_ENTRIES,
        );
        let error = fixture
            .session
            .receive_for_test(DecodedPreparedWriteSet::for_test(
                0,
                vec![(target, fragment)],
            ))
            .expect_err("over the union budget");
        assert_eq!(error.kind(), ConnectorErrorKind::ResourceExhausted);
        assert_eq!(
            fixture.session.lock_accumulated().unwrap().fragments.len(),
            MAX_CONNECTOR_PREPARED_WRITE_SET_ENTRIES,
            "a refused next segment cannot mutate the existing union",
        );
    }

    #[test]
    fn a_set_arriving_after_the_terminal_decision_is_refused() {
        let fixture = fixture(1, 16);
        let _ = fixture
            .session
            .publish_prepared_for_test(empty_prepared(), request_context());
        let error = fixture
            .session
            .receive_for_test(empty_prepared())
            .expect_err("accumulate after terminal");
        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
        assert_eq!(fixture.session.finish_invocations(), 1);
    }

    #[test]
    fn reconcile_is_unreachable_until_a_commit_reported_an_unknown_outcome() {
        let fixture = fixture(1, 16);
        let mut call = fixture
            .session
            .prepare_finish_call(request_context())
            .unwrap();
        let error = call
            .start_reconciliation(evidence(), request_context())
            .expect_err("nothing to reconcile");
        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
        assert_eq!(fixture.recorded.lock().expect("recorded").reconcile, 0);
    }

    /// The commit happens after the lifecycle attempt finalizes -- that is the
    /// whole reason the capability is retained rather than read from the live
    /// attempt. A session that took the hold commits through storage the
    /// attempt no longer serves; on a vended-credential deployment, a session
    /// that took no hold would have watched the credential leases be cleared at
    /// finalization and then reached the object store with nothing to
    /// authenticate.
    #[test]
    fn a_commit_after_finalization_still_resolves_vended_storage() {
        let fixture = fixture_with_outcome(1, 16, known_committed());
        let accounting = finalize_attempt_retaining_terminal_storage(&fixture.session);

        // The outstanding hold is what kept the leases alive across finalize.
        assert_eq!(accounting.holds(), 1);
        assert!(!accounting.leases_cleared());

        fixture
            .session
            .receive_for_test(DecodedPreparedWriteSet::for_test(7, Vec::new()))
            .unwrap();
        let outcome = fixture
            .session
            .publish_for_test(request_context())
            .expect("finish");
        assert!(matches!(
            outcome,
            novarocks_spi::connector::write_stack::ConnectorWritePublicationCompletion::KnownCommitted { .. }
        ));
        assert_eq!(fixture.recorded.lock().unwrap().finish_rows, 7);
        assert_eq!(
            fixture.recorded.lock().expect("recorded").terminal_storage,
            Some(Ok(TABLE_PREFIX.to_string()))
        );
        // The original publication input still belongs to the Session. Retire
        // that real holder before observing the final credential release.
        drop(fixture);
        assert_eq!(accounting.holds(), 0);
        assert!(accounting.leases_cleared());
    }

    /// A staged CTAS has two terminal actions: sealing the writer receipt, then
    /// publishing the catalog-side create. The latter still writes manifests
    /// and metadata, so sealing must transfer (not release) the capability.
    #[test]
    fn a_following_terminal_action_keeps_vended_storage_until_its_context_drops() {
        let fixture = fixture_with_outcome(1, 16, known_committed());
        let accounting = finalize_attempt_retaining_terminal_storage(&fixture.session);
        fixture
            .session
            .receive_for_test(DecodedPreparedWriteSet::for_test(7, Vec::new()))
            .unwrap();
        // The scripted Provider returns this same retained capability through
        // its real finalization getter; a following context is not an effect.
        let provider_context = fixture.session.terminal_context(request_context()).unwrap();
        fixture.recorded.lock().unwrap().finalization_context = Some(Ok(Some(provider_context)));
        let mut call = fixture
            .session
            .prepare_finish_call(request_context())
            .expect("seal write");
        let completed = drain_call_for_test(&mut call).unwrap();
        assert!(matches!(completed, novarocks_spi::connector::write_stack::ConnectorWritePublicationCompletion::KnownCommitted { .. }));
        let terminal_context = call
            .existing_finalization_context()
            .unwrap()
            .expect("actual Provider finalization context")
            .clone();
        assert_eq!(
            fixture.recorded.lock().unwrap().finalization_context_reads,
            1
        );
        assert_eq!(fixture.recorded.lock().unwrap().finish_rows, 7);
        drop(call);
        let provider_context = {
            let mut recorded = fixture.recorded.lock().unwrap();
            recorded.finalization_context.take()
        };
        drop(provider_context);
        // Leave the following context as the actual last capability holder.
        drop(fixture);
        assert_eq!(
            probe_vended_storage(&terminal_context),
            Ok(TABLE_PREFIX.to_string())
        );
        assert_eq!(accounting.holds(), 1);
        assert!(!accounting.leases_cleared());
        drop(terminal_context);
        assert_eq!(accounting.holds(), 0);
        assert!(accounting.leases_cleared());
    }

    #[test]
    fn an_implicit_empty_seal_reaches_an_invisible_staged_create() {
        let (lease, recorded) = unopened_fixture(1, 16, known_committed());
        let mut request = begin_request();
        request.flavor =
            novarocks_spi::connector::write_stack::ConnectorWriteSessionFlavor::StagedCreate(
                novarocks_spi::connector::ConnectorTableHandle::try_new(
                    catalog_handle().catalog_name().clone(),
                    bytes::Bytes::from_static(b"invisible-staged-target"),
                )
                .expect("staged target handle"),
            );
        let session =
            begin_for_test(lease, catalog_properties(), request).expect("begin staged write");

        let outcome = session
            .publish_for_test(request_context())
            .expect("seal provider-managed empty staged write");

        assert!(matches!(
            outcome,
            novarocks_spi::connector::write_stack::ConnectorWritePublicationCompletion::KnownCommitted { .. }
        ));
        assert_eq!(recorded.lock().unwrap().finish_rows, 0);
        assert_eq!(session.finish_invocations(), 1);
    }

    /// A commit whose external outcome is unknown is not finished with storage:
    /// reconciliation reads the same object store, and it is the only decision
    /// still reachable.
    #[test]
    fn a_commit_with_an_unknown_outcome_keeps_its_capability_for_reconciliation() {
        let fixture = fixture_with_outcome(1, 16, commit_unknown());
        let accounting = finalize_attempt_retaining_terminal_storage(&fixture.session);

        fixture.session.receive_for_test(empty_prepared()).unwrap();
        let mut call = fixture
            .session
            .prepare_finish_call(request_context())
            .unwrap();
        futures::executor::block_on(std::future::poll_fn(|cx| call.poll_completed(cx)));
        assert_eq!(
            fixture.recorded.lock().expect("recorded").terminal_storage,
            Some(Ok(TABLE_PREFIX.to_string()))
        );
        assert_eq!(accounting.holds(), 1);
        assert!(!accounting.leases_cleared());

        // Reconciliation reaches the provider with the same capability. This
        // fixture scripts no reconcile outcome, so the session stays
        // reconcilable and keeps holding.
        call.start_reconciliation(evidence(), request_context())
            .unwrap();
        futures::executor::block_on(std::future::poll_fn(|cx| call.poll_completed(cx)));
        assert_eq!(fixture.recorded.lock().expect("recorded").reconcile, 1);
        assert_eq!(
            fixture.recorded.lock().expect("recorded").terminal_storage,
            Some(Ok(TABLE_PREFIX.to_string()))
        );
        assert!(!accounting.leases_cleared());
    }

    /// An abort is a terminal decision too: the provider may clean up staged
    /// objects, and nothing can follow it.
    #[test]
    fn an_abort_carries_the_capability_and_then_releases_it() {
        let fixture = fixture(1, 16);
        let accounting = finalize_attempt_retaining_terminal_storage(&fixture.session);

        fixture
            .session
            .abort_for_test(request_context())
            .expect("abort");

        assert_eq!(
            fixture.recorded.lock().expect("recorded").terminal_storage,
            Some(Ok(TABLE_PREFIX.to_string()))
        );
        assert_eq!(accounting.holds(), 0);
        assert!(accounting.leases_cleared());
    }

    /// A query that neither commits nor reconciles must not pin credential
    /// material for as long as anything happens to keep a reference to it.
    #[test]
    fn a_session_that_never_decides_releases_its_hold_when_dropped() {
        let fixture = fixture(1, 16);
        let accounting = finalize_attempt_retaining_terminal_storage(&fixture.session);
        assert!(!accounting.leases_cleared());

        drop(fixture);

        assert_eq!(accounting.holds(), 0);
        assert!(accounting.leases_cleared());
    }
    mod begin_context_tests {
        include!("write_session_begin_context_tests.rs");
    }

    #[tokio::test]
    async fn async_begin_uses_the_original_session_plan_and_context() {
        let (lease, recorded) = unopened_fixture(1, 16, known_committed());
        recorded.lock().unwrap().capture_begin_context = true;
        let original = Arc::new(Mutex::new(None));
        let receiving = WriteBeginReceiving::new(Arc::clone(&original));
        let session = ConnectorWriteSession::begin_async_in(
            lease,
            catalog_properties(),
            begin_request(),
            &receiving,
        )
        .await
        .unwrap();
        assert_eq!(session.targets().len(), 1);
        assert_eq!(recorded.lock().unwrap().begin, 1);
        assert!(recorded.lock().unwrap().begin_context.is_some());
    }

    fn publication_fixture() -> Fixture {
        publication_fixture_with_outcome(known_committed())
    }

    fn publication_fixture_with_outcome(
        outcome: ExternalMutationOutcome<ConnectorWriteReceipt>,
    ) -> Fixture {
        let fixture = fixture_with_outcome(1, 16, outcome);
        let target = WriteTargetOrdinal::try_new(0).unwrap();
        fixture
            .session
            .receive_for_test(DecodedPreparedWriteSet::for_test(
                7,
                vec![(target, commit_fragment_bytes())],
            ))
            .unwrap();
        fixture
    }

    #[test]
    fn publication_call_preserves_real_input_and_transfers_only_after_child_drop() {
        let fixture = publication_fixture();
        let weak = Arc::downgrade(&fixture.session);
        let mut call = fixture
            .session
            .prepare_finish_call(request_context())
            .unwrap();
        assert_eq!(
            fixture
                .session
                .publication_input
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .received
                .prepared
                .row_count(),
            7
        );
        assert_eq!(
            fixture
                .session
                .publication_input
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .received
                .prepared
                .fragments()
                .len(),
            1
        );
        let mut result = None;
        assert!(call.take_outcome_in(&mut result).is_err());
        let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
        assert!(call.poll_completed(&mut cx).is_ready());
        assert_eq!(fixture.recorded.lock().unwrap().finish_future_drops, 1);
        call.take_outcome_in(&mut result).unwrap();
        assert!(matches!(
            result,
            Some(Ok(novarocks_spi::connector::write_stack::ConnectorWritePublicationCompletion::KnownCommitted { .. }))
        ));
        assert_eq!(fixture.recorded.lock().unwrap().finish_rows, 7);
        let recorded = Arc::clone(&fixture.recorded);
        drop(fixture);
        assert_eq!(recorded.lock().unwrap().source_drops, 0);
        assert!(
            weak.upgrade().is_some(),
            "the original Call still retains its source"
        );
        drop(call);
        assert!(weak.upgrade().is_none());
        assert_eq!(recorded.lock().unwrap().source_drops, 1);
    }

    #[test]
    fn needed_ready_precedes_actual_safe_cleanup_and_preserves_its_success() {
        let fixture = publication_fixture();
        fixture.recorded.lock().unwrap().defer_finish_cleanup = true;
        let mut call = fixture
            .session
            .prepare_finish_call(request_context())
            .unwrap();
        let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
        assert!(call.poll_needed_ready(&mut cx).is_ready());
        assert_eq!(fixture.recorded.lock().unwrap().cleanup_started, 0);
        assert_eq!(fixture.recorded.lock().unwrap().finish_future_drops, 0);
        let mut ready = None;
        call.take_needed_ready_in(&mut ready).unwrap();
        let actual = ready.as_ref().unwrap();
        assert_eq!(
            actual.effect(),
            novarocks_spi::connector::ExternalMutationEffect::Applied
        );
        assert_eq!(
            actual.finalization(),
            &novarocks_spi::connector::ExternalMutationFinalization::Complete
        );
        assert!(call.poll_retirement(&mut cx).is_pending());
        assert_eq!(fixture.recorded.lock().unwrap().cleanup_started, 1);
        assert_eq!(fixture.recorded.lock().unwrap().finish_future_drops, 0);
        let mut duplicate = None;
        assert!(call.take_needed_ready_in(&mut duplicate).is_err());
        {
            let mut recorded = fixture.recorded.lock().unwrap();
            recorded.fail_safe_cleanup = true;
            recorded.allow_cleanup_exit = true;
        }
        assert!(call.poll_retirement(&mut cx).is_ready());
        assert_eq!(fixture.recorded.lock().unwrap().finish_future_drops, 1);
        assert_eq!(
            actual.finalization(),
            &novarocks_spi::connector::ExternalMutationFinalization::Complete
        );
        let outcome = call.outcome().unwrap().as_ref().unwrap();
        let novarocks_spi::connector::write_stack::ConnectorWritePublicationCompletion::KnownCommitted { needed, cleanup } = outcome
        else {
            panic!("actual committed result")
        };
        assert!(Arc::ptr_eq(needed, actual));
        assert_eq!(needed.finalization(), actual.finalization());
        assert!(matches!(
            cleanup,
            novarocks_spi::connector::write_stack::ConnectorWriteCleanupState::Observed(
                novarocks_spi::connector::ExternalMutationFinalization::Failed(_)
            )
        ));
    }

    #[test]
    fn cancelled_close_retains_real_ready_and_continues_same_cleanup_future() {
        let fixture = publication_fixture();
        fixture.recorded.lock().unwrap().defer_finish_cleanup = true;
        let mut call = fixture
            .session
            .prepare_finish_call(request_context())
            .unwrap();
        let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
        // Close never takes the success but must not deadlock the receiving.
        assert!(call.poll_completed(&mut cx).is_pending());
        assert_eq!(fixture.recorded.lock().unwrap().cleanup_started, 0);
        assert!(call.poll_retirement(&mut cx).is_pending());
        assert_eq!(fixture.recorded.lock().unwrap().cleanup_started, 1);
        fixture.recorded.lock().unwrap().allow_cleanup_exit = true;
        fixture.recorded.lock().unwrap().panic_finish_future_drop = true;
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(
                || call.poll_retirement(&mut cx)
            ))
            .is_err()
        );
        assert_eq!(
            call.publication_disposition(),
            Some(novarocks_spi::connector::LakePublicationDisposition::KnownCommitted)
        );
        assert!(call.retains_unreceived_outcome());
        assert!(call.local_health_failure().is_some());
        assert!(call.poll_retirement(&mut cx).is_pending());
        assert_eq!(fixture.recorded.lock().unwrap().finish_future_drops, 1);
    }

    #[test]
    fn exact_reconciliation_has_the_same_needed_ready_cleanup_boundary() {
        let fixture = fixture_with_outcome(1, 16, commit_unknown());
        fixture
            .session
            .receive_for_test(DecodedPreparedWriteSet::for_test(
                7,
                vec![(
                    WriteTargetOrdinal::try_new(0).unwrap(),
                    commit_fragment_bytes(),
                )],
            ))
            .unwrap();
        let mut call = fixture
            .session
            .prepare_finish_call(request_context())
            .unwrap();
        let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
        assert!(call.poll_needed_ready(&mut cx).is_ready());
        assert!(call.needed_ready().is_none());
        let evidence = match call.outcome().unwrap() {
            Ok(novarocks_spi::connector::write_stack::ConnectorWritePublicationCompletion::CommitUnknown { evidence, .. }) => evidence.clone(),
            _ => panic!("actual original unknown"),
        };
        {
            let mut recorded = fixture.recorded.lock().unwrap();
            recorded.reconcile_committed = true;
            recorded.defer_finish_cleanup = true;
        }
        call.start_reconciliation(evidence.clone(), request_context())
            .unwrap();
        assert!(call.poll_needed_ready(&mut cx).is_ready());
        assert!(call.retains_original_unknown());
        assert_eq!(fixture.recorded.lock().unwrap().cleanup_started, 0);
        let mut ready = None;
        call.take_needed_ready_in(&mut ready).unwrap();
        assert!(call.poll_retirement(&mut cx).is_pending());
        fixture.recorded.lock().unwrap().allow_cleanup_exit = true;
        assert!(call.poll_retirement(&mut cx).is_ready());
        assert_eq!(fixture.recorded.lock().unwrap().reconcile, 1);
        assert!(
            call.start_reconciliation(evidence, request_context())
                .is_err()
        );
    }

    #[test]
    fn child_drop_panic_retains_original_result_and_forbids_repoll_or_transfer() {
        let fixture = publication_fixture();
        let context = request_context();
        struct OriginalFinalizationScope;
        let scope = context.request_scope_extension_or_insert_with(|| OriginalFinalizationScope);
        {
            let mut recorded = fixture.recorded.lock().unwrap();
            recorded.panic_finish_future_drop = true;
            recorded.finalization_context = Some(Ok(Some(context.clone())));
        }
        let mut call = fixture
            .session
            .prepare_finish_call(request_context())
            .unwrap();
        let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| call.poll_completed(&mut cx)))
                .is_err()
        );
        assert!(
            call.retains_unreceived_outcome(),
            "the actual Provider result preceded its failing destructor"
        );
        assert_eq!(
            call.publication_disposition(),
            Some(novarocks_spi::connector::LakePublicationDisposition::KnownCommitted)
        );
        assert!(call.local_health_failure().is_some());
        assert_eq!(
            call.received_committed_effect(),
            Some(novarocks_spi::connector::ExternalMutationEffect::Applied)
        );
        // The getter ran before the actual child destructor, not lazily after
        // this unwind. Its original context is still observable with no repoll.
        assert_eq!(
            fixture.recorded.lock().unwrap().finalization_context_reads,
            1
        );
        let received = call.existing_finalization_context().unwrap().unwrap();
        assert_eq!(received.deadline(), context.deadline());
        assert!(Arc::ptr_eq(
            &scope,
            &received.request_scope_extension_or_insert_with(|| OriginalFinalizationScope)
        ));
        assert!(call.poll_completed(&mut cx).is_pending());
        let mut result = None;
        assert!(call.take_outcome_in(&mut result).is_err());
        assert_eq!(fixture.recorded.lock().unwrap().finish, 1);
        assert_eq!(fixture.recorded.lock().unwrap().finish_future_drops, 1);
        assert_eq!(
            fixture.recorded.lock().unwrap().finalization_context_reads,
            1
        );
    }

    #[test]
    fn finalization_getter_refusal_keeps_commit_and_original_cause_without_health_failure() {
        use novarocks_failure_contract::{FailureProjection, SharedFailureCause};
        let fixture = publication_fixture();
        let error = ConnectorError::from_domain(
            ConnectorErrorKind::Unavailable,
            novarocks_workload_control::WorkError::Released,
        );
        let fact = error.failure_fact();
        let original = std::error::Error::source(&error)
            .unwrap()
            .downcast_ref::<SharedFailureCause>()
            .unwrap()
            .downcast_ref::<novarocks_workload_control::WorkError>()
            .unwrap();
        fixture.recorded.lock().unwrap().finalization_context = Some(Err(error.clone()));
        let mut call = fixture
            .session
            .prepare_finish_call(request_context())
            .unwrap();
        let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
        assert!(call.poll_completed(&mut cx).is_ready());
        assert!(call.local_health_failure().is_none());
        assert_eq!(
            call.publication_disposition(),
            Some(novarocks_spi::connector::LakePublicationDisposition::KnownCommitted)
        );
        fixture.recorded.lock().unwrap().finalization_context = Some(Ok(None));
        for _ in 0..2 {
            let observed = match call.existing_finalization_context() {
                Err(error) => error,
                Ok(_) => panic!("the original getter refusal remains independent of commit"),
            };
            assert_eq!(observed.failure_fact(), fact);
            let actual = std::error::Error::source(&observed)
                .unwrap()
                .downcast_ref::<SharedFailureCause>()
                .unwrap()
                .downcast_ref::<novarocks_workload_control::WorkError>()
                .unwrap();
            assert!(std::ptr::eq(actual, original));
        }
        assert_eq!(
            fixture.recorded.lock().unwrap().finalization_context_reads,
            1
        );
        assert_eq!(fixture.recorded.lock().unwrap().finish_future_drops, 1);
        let mut result = None;
        call.take_outcome_in(&mut result).unwrap();
        assert!(matches!(result, Some(Ok(novarocks_spi::connector::write_stack::ConnectorWritePublicationCompletion::KnownCommitted { .. }))));
    }

    #[test]
    fn no_op_child_drop_panic_keeps_the_canonical_absence_of_effect() {
        let mut outcome = known_committed();
        let ExternalMutationOutcome::KnownCommitted { effect, .. } = &mut outcome else {
            unreachable!()
        };
        *effect = novarocks_spi::connector::ExternalMutationEffect::NoOp;
        let fixture = publication_fixture_with_outcome(outcome);
        fixture.recorded.lock().unwrap().panic_finish_future_drop = true;
        let mut call = fixture
            .session
            .prepare_finish_call(request_context())
            .unwrap();
        let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| call.poll_completed(&mut cx)))
                .is_err()
        );
        assert_eq!(
            call.publication_disposition(),
            Some(novarocks_spi::connector::LakePublicationDisposition::KnownCommitted)
        );
        assert_eq!(
            call.received_committed_effect(),
            Some(novarocks_spi::connector::ExternalMutationEffect::NoOp)
        );
        assert!(call.local_health_failure().is_some());
        assert!(call.poll_completed(&mut cx).is_pending());
        assert_eq!(fixture.recorded.lock().unwrap().finish, 1);
    }

    #[test]
    fn committed_abort_receives_context_before_its_actual_child_drop_panics() {
        let fixture = publication_fixture();
        let context = request_context();
        struct OriginalAbortFinalizationScope;
        let scope =
            context.request_scope_extension_or_insert_with(|| OriginalAbortFinalizationScope);
        let novarocks_spi::connector::write_stack::ConnectorWritePublicationCompletion::KnownCommitted { needed, cleanup } =
            novarocks_spi::connector::write_stack::ConnectorWritePublicationCompletion::from_outcome(known_committed())
        else { unreachable!() };
        {
            let mut recorded = fixture.recorded.lock().unwrap();
            recorded.abort_outcome =
                Some(ConnectorWriteAbortOutcome::KnownCommitted { needed, cleanup });
            recorded.finalization_context = Some(Ok(Some(context.clone())));
            recorded.panic_abort_future_drop = true;
        }
        let mut call = fixture
            .session
            .prepare_abort_call(request_context())
            .unwrap();
        let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| call.poll_completed(&mut cx)))
                .is_err()
        );
        assert!(call.retains_unreceived_outcome());
        assert_eq!(
            call.publication_disposition(),
            Some(novarocks_spi::connector::LakePublicationDisposition::KnownCommitted)
        );
        assert!(call.local_health_failure().is_some());
        assert_eq!(
            call.received_committed_effect(),
            Some(novarocks_spi::connector::ExternalMutationEffect::Applied)
        );
        assert_eq!(
            fixture.recorded.lock().unwrap().finalization_context_reads,
            1
        );
        let received = call.existing_finalization_context().unwrap().unwrap();
        assert_eq!(received.deadline(), context.deadline());
        assert!(Arc::ptr_eq(
            &scope,
            &received.request_scope_extension_or_insert_with(|| OriginalAbortFinalizationScope)
        ));
        assert!(call.poll_completed(&mut cx).is_pending());
        let mut result = None;
        assert!(call.take_abort_outcome_in(&mut result).is_err());
        assert_eq!(fixture.recorded.lock().unwrap().abort, 1);
        assert_eq!(fixture.recorded.lock().unwrap().abort_future_drops, 1);
        assert_eq!(
            fixture.recorded.lock().unwrap().finalization_context_reads,
            1
        );
    }

    #[test]
    fn cancellation_after_publication_ready_does_not_erase_the_commit() {
        let fixture = publication_fixture();
        let stop = novarocks_spi::connector::ConnectorStopOwner::new();
        let context = ConnectorRequestContext::try_new(
            std::time::Instant::now() + std::time::Duration::from_secs(60),
            stop.view(),
            novarocks_spi::connector::MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
            novarocks_spi::connector::MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
        )
        .unwrap();
        let mut call = fixture.session.prepare_finish_call(context).unwrap();
        let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
        assert!(call.poll_completed(&mut cx).is_ready());
        stop.request_stop();
        let mut result = None;
        call.take_outcome_in(&mut result).unwrap();
        assert!(matches!(
            result,
            Some(Ok(novarocks_spi::connector::write_stack::ConnectorWritePublicationCompletion::KnownCommitted { .. }))
        ));
    }

    #[test]
    fn reconciliation_keeps_the_original_unknown_until_the_secondary_result_is_stable() {
        let fixture = fixture_with_outcome(1, 16, commit_unknown());
        let target = WriteTargetOrdinal::try_new(0).unwrap();
        fixture
            .session
            .receive_for_test(DecodedPreparedWriteSet::for_test(
                7,
                vec![(target, commit_fragment_bytes())],
            ))
            .unwrap();
        let mut call = fixture
            .session
            .prepare_finish_call(request_context())
            .unwrap();
        let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
        assert!(call.poll_completed(&mut cx).is_ready());
        let evidence = match call.outcome().unwrap() {
            Ok(novarocks_spi::connector::write_stack::ConnectorWritePublicationCompletion::CommitUnknown { evidence, .. }) => evidence.clone(),
            _ => panic!("original unknown result"),
        };
        call.start_reconciliation(evidence.clone(), request_context())
            .unwrap();
        assert!(call.retains_original_unknown());
        let mut result = None;
        assert!(call.take_outcome_in(&mut result).is_err());
        assert!(call.poll_completed(&mut cx).is_ready());
        assert!(call.retains_original_unknown());
        assert!(
            call.start_reconciliation(evidence, request_context())
                .is_err()
        );
        call.take_outcome_in(&mut result).unwrap();
        assert!(matches!(result, Some(Err(_))));
        assert_eq!(fixture.recorded.lock().unwrap().reconcile, 1);
    }

    #[test]
    fn cancellation_between_precheck_and_call_claim_retains_refusal_and_aborts_once() {
        let fixture = publication_fixture();
        let stop = novarocks_spi::connector::ConnectorStopOwner::new();
        let context = ConnectorRequestContext::try_new(
            std::time::Instant::now() + std::time::Duration::from_secs(60),
            stop.view(),
            novarocks_spi::connector::MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
            novarocks_spi::connector::MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
        )
        .unwrap();
        // This is the real precheck/claim interval, not a timer race or a
        // second request. The same context reaches the actual Session factory.
        ensure_finish_context_active(&context).unwrap();
        stop.request_stop();
        let mut call = None;
        let mut refusal = None;
        fixture
            .session
            .prepare_finish_or_abort_call_in(context, &mut call, &mut refusal)
            .unwrap();
        assert_eq!(
            refusal.as_ref().unwrap().kind(),
            ConnectorErrorKind::Cancelled
        );
        assert_eq!(
            *fixture.session.lock_terminal().unwrap(),
            Some(TerminalDecision::Aborted)
        );
        assert!(fixture.session.publication_input.lock().unwrap().is_none());
        assert_eq!(fixture.session.accumulated_row_count().unwrap(), 7);
        let mut call = call.unwrap();
        let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
        assert!(call.poll_completed(&mut cx).is_ready());
        let mut outcome = None;
        call.take_abort_outcome_in(&mut outcome).unwrap();
        assert!(matches!(
            outcome,
            Some(Ok(ConnectorWriteAbortOutcome::KnownUncommitted { .. }))
        ));
        assert_eq!(fixture.recorded.lock().unwrap().finish, 0);
        assert_eq!(fixture.recorded.lock().unwrap().abort, 1);
        assert_eq!(
            fixture.recorded.lock().unwrap().abort_production,
            Some(novarocks_spi::connector::write_stack::ConnectorWriteAbortProduction::Unproven)
        );
        assert_eq!(
            refusal.as_ref().unwrap().kind(),
            ConnectorErrorKind::Cancelled
        );
        assert!(
            fixture
                .session
                .prepare_finish_or_abort_call_in(request_context(), &mut None, &mut refusal,)
                .is_err()
        );
        assert_eq!(fixture.recorded.lock().unwrap().abort, 1);
    }

    #[test]
    fn abort_evidence_requires_no_registered_production_and_an_empty_inventory() {
        use novarocks_spi::connector::write_stack::ConnectorWriteAbortProduction as P;
        let fixture = fixture_with_outcome(1, 16, known_committed());
        let mut call = fixture
            .session
            .prepare_abort_call(request_context())
            .unwrap();
        let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
        assert!(call.poll_completed(&mut cx).is_ready());
        assert_eq!(
            fixture.recorded.lock().unwrap().abort_production,
            Some(P::NotStarted)
        );
        let received = publication_fixture();
        let mut call = received
            .session
            .prepare_abort_call(request_context())
            .unwrap();
        assert!(call.poll_completed(&mut cx).is_ready());
        assert_eq!(
            received.recorded.lock().unwrap().abort_production,
            Some(P::Unproven)
        );
        assert_eq!(received.session.accumulated_row_count().unwrap(), 7);
    }

    #[test]
    fn claimed_session_preparation_error_does_not_authorize_a_second_abort() {
        let fixture = publication_fixture();
        let original_call = fixture
            .session
            .prepare_finish_call(request_context())
            .unwrap();
        let mut refused_call = None;
        let mut refusal = None;
        assert!(fixture.session.prepare_finish_or_abort_call_in(
            request_context(), &mut refused_call, &mut refusal,
        ).is_err());
        assert!(refused_call.is_none());
        assert!(
            refusal.is_some(),
            "the exact factory refusal remains in its original slot"
        );
        assert_eq!(
            *fixture.session.lock_terminal().unwrap(),
            Some(TerminalDecision::Committed)
        );
        assert_eq!(fixture.recorded.lock().unwrap().abort, 0);
        assert_eq!(fixture.recorded.lock().unwrap().finish, 0);
        drop(original_call);
    }

    #[test]
    fn stable_abort_after_refusal_uses_no_publication_or_empty_prepared_set() {
        let fixture = publication_fixture();
        let mut call = fixture
            .session
            .prepare_abort_call(request_context())
            .unwrap();
        assert!(fixture.session.publication_input.lock().unwrap().is_none());
        assert_eq!(fixture.session.accumulated_row_count().unwrap(), 7);
        let mut result = None;
        assert!(call.take_abort_outcome_in(&mut result).is_err());
        let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
        assert!(call.poll_completed(&mut cx).is_ready());
        call.take_abort_outcome_in(&mut result).unwrap();
        assert!(matches!(
            result,
            Some(Ok(ConnectorWriteAbortOutcome::KnownUncommitted { .. }))
        ));
        assert_eq!(fixture.recorded.lock().unwrap().abort, 1);
        assert_eq!(fixture.recorded.lock().unwrap().abort_future_drops, 1);
        assert_eq!(fixture.recorded.lock().unwrap().finish, 0);
    }

    #[test]
    fn abort_child_drop_panic_retains_the_original_abort_result() {
        let fixture = publication_fixture();
        fixture.recorded.lock().unwrap().panic_abort_future_drop = true;
        let mut call = fixture
            .session
            .prepare_abort_call(request_context())
            .unwrap();
        let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| call.poll_completed(&mut cx)))
                .is_err()
        );
        assert!(call.retains_unreceived_outcome());
        assert_eq!(
            call.publication_disposition(),
            Some(novarocks_spi::connector::LakePublicationDisposition::KnownUncommitted)
        );
        assert!(call.local_health_failure().is_some());
        assert_eq!(call.received_committed_effect(), None);
        assert!(call.poll_completed(&mut cx).is_pending());
        let mut result = None;
        assert!(call.take_abort_outcome_in(&mut result).is_err());
        assert_eq!(fixture.recorded.lock().unwrap().abort, 1);
        assert_eq!(fixture.recorded.lock().unwrap().abort_future_drops, 1);
    }

    /// The existing scripted checked Provider returns a real plan and original
    /// scope. The actual receiving author must adopt both before returning.
    #[tokio::test]
    async fn checked_begin_receiving_preserves_the_same_scope_and_session() {
        let (_control, _root, binding, _capacity) =
            crate::query_execution::internal_result_cpu::admitted_internal_fixture();
        let context = request_context();
        let original = crate::query_execution::original_cow_result_scope::from_original_binding(
            &binding, &context,
        )
        .unwrap();
        let same_original = original.clone();
        let (lease, recorded) = unopened_fixture(1, 16, known_committed());
        let session_receiving = Arc::new(Mutex::new(None));
        let receiving = WriteBeginReceiving::new(Arc::clone(&session_receiving));
        receiving.claim().unwrap();
        receiving.install_input(lease, catalog_properties());
        let control = receiving.control();
        receiving.enter_factory();
        let mut child = control.begin_cow_write_checked_async(begin_request(), original);
        receiving.leave_pending();
        let outcome = std::future::poll_fn(|cx| {
            assert!(receiving.enter_poll());
            let result = std::future::Future::poll(child.as_mut(), cx);
            if result.is_pending() {
                receiving.leave_pending();
            }
            result
        })
        .await;
        receiving.publish_cow_plan(outcome);
        assert!(receiving.retains_cow_plan());
        assert!(session_receiving.lock().unwrap().is_none());
        drop(child);
        receiving.adopt_cow_session();
        receiving.finish_drop();
        let session = receiving.completed_cow_result().unwrap();
        assert!(Arc::ptr_eq(
            &session,
            session_receiving.lock().unwrap().as_ref().unwrap()
        ));
        assert!(
            session
                .cow_original()
                .unwrap()
                .is_same_original(&same_original)
        );
        assert_eq!(recorded.lock().unwrap().begin_future_drops, 1);
    }

    #[test]
    fn checked_begin_drop_panic_retains_the_actual_checked_plan() {
        let (_control, _root, binding, _capacity) =
            crate::query_execution::internal_result_cpu::admitted_internal_fixture();
        let context = request_context();
        let original = crate::query_execution::original_cow_result_scope::from_original_binding(
            &binding, &context,
        )
        .unwrap();
        let (lease, recorded) = unopened_fixture(1, 16, known_committed());
        recorded.lock().unwrap().panic_begin_future_drop = true;
        let slot = Arc::new(Mutex::new(None));
        let receiving = WriteBeginReceiving::new(Arc::clone(&slot));
        receiving.claim().unwrap();
        receiving.install_input(lease, catalog_properties());
        let control = receiving.control();
        receiving.enter_factory();
        let mut child = control.begin_cow_write_checked_async(begin_request(), original);
        receiving.leave_pending();
        let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
        assert!(receiving.enter_poll());
        let std::task::Poll::Ready(outcome) = std::future::Future::poll(child.as_mut(), &mut cx)
        else {
            panic!("finite checked fixture must be ready");
        };
        receiving.publish_cow_plan(outcome);
        assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(child))).is_err());
        assert!(receiving.retains_cow_plan());
        assert!(receiving.retains_input());
        assert!(receiving.local_health_failure().is_some());
        assert!(slot.lock().unwrap().is_none());
        assert_eq!(recorded.lock().unwrap().begin_future_drops, 1);
    }

    #[test]
    fn checked_begin_failure_survives_the_original_child_drop_panic() {
        let (_control, _root, binding, _capacity) =
            crate::query_execution::internal_result_cpu::admitted_internal_fixture();
        let stop = novarocks_spi::connector::ConnectorStopOwner::new();
        let context = ConnectorRequestContext::try_new(
            std::time::Instant::now() + std::time::Duration::from_secs(60),
            stop.view(),
            4096,
            65536,
        )
        .unwrap();
        let original = crate::query_execution::original_cow_result_scope::from_original_binding(
            &binding, &context,
        )
        .unwrap();
        let (lease, recorded) = unopened_fixture(1, 16, known_committed());
        recorded.lock().unwrap().panic_begin_future_drop = true;
        let receiving = WriteBeginReceiving::new(Arc::new(Mutex::new(None)));
        receiving.claim().unwrap();
        receiving.install_input(lease, catalog_properties());
        let control = receiving.control();
        receiving.enter_factory();
        // The same original control authors the typed stop failure.
        stop.request_stop();
        let mut child = control.begin_cow_write_checked_async(begin_request(), original);
        receiving.leave_pending();
        let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
        assert!(receiving.enter_poll());
        let std::task::Poll::Ready(outcome) = std::future::Future::poll(child.as_mut(), &mut cx)
        else {
            panic!("finite checked failure fixture must be ready");
        };
        assert!(matches!(&outcome, Err(error) if matches!(error.cause(),
            novarocks_spi::connector::ConnectorCowBeginCause::OriginalResult(_))));
        receiving.publish_cow_plan(outcome);
        assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(child))).is_err());
        assert!(receiving.retains_cow_error());
        assert!(receiving.retains_input());
        assert!(receiving.local_health_failure().is_some());
        assert_eq!(recorded.lock().unwrap().begin_future_drops, 1);
    }

    #[tokio::test]
    async fn begin_in_installs_the_same_accepted_session_before_return() {
        let (lease, recorded) = unopened_fixture(1, 16, known_committed());
        let session_receiving = Arc::new(Mutex::new(None));
        let receiving = WriteBeginReceiving::new(Arc::clone(&session_receiving));
        let session = ConnectorWriteSession::begin_async_in(
            lease,
            catalog_properties(),
            begin_request(),
            &receiving,
        )
        .await
        .unwrap();
        assert!(Arc::ptr_eq(
            &session,
            session_receiving.lock().unwrap().as_ref().unwrap()
        ));
        assert!(Arc::ptr_eq(&session, &receiving.session().unwrap()));
        assert_eq!(recorded.lock().unwrap().begin_future_drops, 1);
        assert!(receiving.local_health_failure().is_none());
    }

    #[test]
    fn begin_child_drop_panic_keeps_the_actual_plan_as_original_session() {
        let (lease, recorded) = unopened_fixture(1, 16, known_committed());
        recorded.lock().unwrap().panic_begin_future_drop = true;
        let original = Arc::new(Mutex::new(None));
        let receiving = WriteBeginReceiving::new(Arc::clone(&original));
        let mut future = Box::pin(ConnectorWriteSession::begin_async_in(
            lease,
            catalog_properties(),
            begin_request(),
            &receiving,
        ));
        let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| std::future::Future::poll(
                future.as_mut(),
                &mut cx
            )))
            .is_err()
        );
        assert!(receiving.local_health_failure().is_some());
        assert!(receiving.retains_plan());
        assert!(receiving.retains_input());
        assert!(receiving.session().is_none());
        assert!(original.lock().unwrap().is_none());
        assert_eq!(recorded.lock().unwrap().begin, 1);
        assert_eq!(recorded.lock().unwrap().begin_future_drops, 1);
        assert!(
            receiving.completed_result().is_err(),
            "unproven child exit cannot be returned as a completed begin"
        );
    }

    #[test]
    fn begin_child_drop_panic_keeps_the_original_rejection_cause() {
        let (lease, recorded) = unopened_fixture(0, 16, known_committed());
        recorded.lock().unwrap().panic_begin_future_drop = true;
        let receiving = WriteBeginReceiving::new(Arc::new(Mutex::new(None)));
        let mut future = Box::pin(ConnectorWriteSession::begin_async_in(
            lease,
            catalog_properties(),
            begin_request(),
            &receiving,
        ));
        let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| std::future::Future::poll(
                future.as_mut(),
                &mut cx
            )))
            .is_err()
        );
        assert!(receiving.retains_error());
        assert!(receiving.session().is_none());
        assert!(receiving.local_health_failure().is_some());
        assert_eq!(recorded.lock().unwrap().begin_future_drops, 1);
    }
}

#[cfg(test)]
mod command_count_tests {
    use super::tests::{fixture_with_outcome, known_committed};
    use super::*;
    use crate::query_execution::write_result::DecodedPreparedWriteSet;
    #[test]
    fn original_complete_root_count_reuses_freeze_without_publishing() {
        let fixture = fixture_with_outcome(1, 16, known_committed());
        fixture
            .session
            .receive_for_test(DecodedPreparedWriteSet::for_test(19, Vec::new()))
            .unwrap();
        assert_eq!(fixture.session.freeze_received_row_count().unwrap(), 19);
        let original = fixture
            .session
            .lock_accumulated()
            .unwrap()
            .received
            .clone()
            .unwrap();
        assert_eq!(fixture.session.freeze_received_row_count().unwrap(), 19);
        let again = fixture
            .session
            .lock_accumulated()
            .unwrap()
            .received
            .clone()
            .unwrap();
        assert!(
            Arc::ptr_eq(&original, &again),
            "the original cached inventory must be reused"
        );
        assert_eq!(fixture.session.finish_invocations(), 0);
        assert_eq!(fixture.recorded.lock().unwrap().finish, 0);
    }
}
