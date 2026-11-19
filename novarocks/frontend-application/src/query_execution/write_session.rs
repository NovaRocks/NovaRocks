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
//! One `begin_write` on one exact control generation returns the commit session
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
    ConnectorWriteBeginRequest, ConnectorWriteFinishPublication, ConnectorWriteQueryIdentity,
    ConnectorWriteSessionAbortRequest, ConnectorWriteSessionFlavor, ConnectorWriteSessionPlan,
    ConnectorWriteSessionReconcileRequest, ConnectorWriteTargetPlan, UniqueWriterHandleLedger,
    WriteRowCountAccumulator, WriteTargetOrdinal,
};
use novarocks_spi::connector::{
    ConnectorDocumentPublicationDeclaration, ConnectorError, ConnectorErrorKind,
    ConnectorRequestContext, ConnectorStorageResolver, ConnectorWriteAbortOutcome,
    ExternalMutationEvidence, KeyedPublicationIntent,
};

#[cfg(test)]
use crate::query_execution::write_result::DecodedPreparedWriteSet;
use novarocks_catalog_application::ConnectorWriteStackPin;
use novarocks_plan_codec::SealedWriteTargets;

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
    write_stack_pin: ConnectorWriteStackPin,
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
}

/// What a session has collected so far across the queries it drives.
///
/// Most writes are one query, but a distributed
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
    row_count: u64,
    statistics: Vec<novarocks_spi::connector::write_stack::WriteStatisticsArtifact>,
    _retention: Option<crate::query_execution::internal_result_cpu::InternalResultRetention>,
}

#[derive(Default)]
struct AccumulatedWriteSet {
    received: Option<Arc<ReceivedWriteInput>>,
    rows: WriteRowCountAccumulator,
    has_fragments: bool,
    statistics: Vec<novarocks_spi::connector::write_stack::WriteStatisticsArtifact>,
    statistics_identities: std::collections::BTreeSet<(
        WriteTargetOrdinal,
        novarocks_spi::connector::StatisticsArtifactIdentity,
    )>,
    statistics_property_bytes: usize,
    retention: Option<crate::query_execution::internal_result_cpu::InternalResultRetention>,
}

impl ConnectorWriteSession {
    /// The statement owns `receiving` before polling this borrowed begin.
    /// Adopt the actual Provider result before dropping its original child F.
    pub(crate) async fn begin_async_in(
        write_stack_pin: ConnectorWriteStackPin,
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
            write_stack_pin,
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
        write_stack_pin: ConnectorWriteStackPin,
        catalog_properties: novarocks_spi::connector::CatalogProperties,
        request: ConnectorWriteBeginRequest,
        receiving: &WriteBeginReceiving,
    ) -> Result<Arc<Self>, ConnectorError> {
        receiving.claim()?;
        let declaration = match &request.flavor {
            ConnectorWriteSessionFlavor::ApplicationDocumentPublication { declaration, .. } => {
                declaration.as_ref().clone()
            }
            _ => {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "pending application-document publication requires an application-document write",
                ));
            }
        };
        Self::begin_validated_async_in(
            write_stack_pin,
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
        write_stack_pin: ConnectorWriteStackPin,
        catalog_properties: novarocks_spi::connector::CatalogProperties,
        mut request: ConnectorWriteBeginRequest,
        finish_publication: WritePublicationState,
        receiving: &WriteBeginReceiving,
    ) -> Result<Arc<Self>, ConnectorError> {
        request.context = context_for_write_begin(request.context, &catalog_properties)?;
        receiving.bind_catalog_session(&mut request, &write_stack_pin, &catalog_properties);
        receiving.install_publication_input(
            write_stack_pin,
            catalog_properties,
            finish_publication,
        );
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
        write_stack_pin: ConnectorWriteStackPin,
        catalog_properties: novarocks_spi::connector::CatalogProperties,
        mut request: ConnectorWriteBeginRequest,
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
        receiving.bind_catalog_session(&mut request, &write_stack_pin, &catalog_properties);
        receiving.install_input(write_stack_pin, catalog_properties);
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
            .write_stack_pin
            .session()
            .cleanup_responsibility(self.plan.commit_session())?;
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
            && !union.has_fragments
            && union.statistics.is_empty()
            && union.statistics_identities.is_empty()
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
                && !union.has_fragments
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
        self.write_stack_pin
            .session()
            .existing_finalization_context(self.plan.commit_session())
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

        Ok(WritePublicationCall::new_abort(
            Arc::clone(self),
            context,
            production,
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
        write_stack_pin: ConnectorWriteStackPin,
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
            write_stack_pin,
            plan,
            metadata_only_publication,
            finish_publication: Mutex::new(finish_publication),
            catalog_properties,
            accumulated: Mutex::new(AccumulatedWriteSet::default()),
            publication_input: Mutex::new(None),
            terminal: Mutex::new(None),
            finish_invocations: AtomicUsize::new(0),
            terminal_storage: Mutex::new(None),
        }
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

    /// The session target set reused by explicit relation and packet fixtures.
    #[cfg(test)]
    pub(crate) fn expected_targets(&self) -> Vec<WriteTargetOrdinal> {
        self.plan.expected_targets()
    }

    pub(crate) fn targets(&self) -> &[ConnectorWriteTargetPlan] {
        self.plan.targets()
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
        self.write_stack_pin
            .handle_encoder()
            .encode_writer_handle_payload(handle)
            .map_err(|error| ConnectorError::from_domain(ConnectorErrorKind::Internal, error))
    }

    pub(crate) fn seal_write_targets(&self) -> Result<SealedWriteTargets, ConnectorError> {
        let encoder = self.write_stack_pin.handle_encoder();
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
        self.logical_rows_after_provider_receiving(previous, context)
    }

    fn logical_rows_after_provider_receiving(
        &self,
        previous: Option<u64>,
        context: &ConnectorRequestContext,
    ) -> Result<Option<u64>, ConnectorError> {
        ensure_finish_context_active(context)?;
        match self
            .write_stack_pin
            .session()
            .received_logical_row_change(self.plan.commit_session(), context)?
        {
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
        {
            let terminal = self.lock_terminal()?;
            if terminal.is_some() {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "write row projection must precede the terminal decision",
                ));
            }
            self.require_received_productions()?;
            self.freeze_received_input()?;
        }
        // The Provider owns the fragment payload. Keep its projection outside
        // the FE terminal and statistics locks, including on unwind.
        self.logical_rows_after_provider_receiving(previous, context)
    }

    fn freeze_received_input(&self) -> Result<Arc<ReceivedWriteInput>, ConnectorError> {
        let mut union = self.lock_accumulated()?;
        if let Some(received) = &union.received {
            return Ok(Arc::clone(received));
        }
        let received = Arc::new(ReceivedWriteInput {
            row_count: union.rows.get(),
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
        Ok(self.freeze_received_input()?.row_count)
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

pub(super) fn write_query_identity(
    execution: novarocks_types::QueryExecutionId,
    plan: novarocks_query_application::api::PlanSeal,
) -> Result<ConnectorWriteQueryIdentity, ConnectorError> {
    let query = execution.query_id();
    let mut query_bytes = [0; 16];
    query_bytes[..8].copy_from_slice(&query.high().to_be_bytes());
    query_bytes[8..].copy_from_slice(&query.low().to_be_bytes());
    let novarocks_query_application::api::PlanSeal::Version(plan) = plan;
    ConnectorWriteQueryIdentity::try_new(
        query_bytes,
        execution.attempt_id().get(),
        *plan.as_bytes(),
    )
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

/// Borrow the original asynchronous begin while retaining its exact declaration.
/// Preparation and Provider refusals keep the original ConnectorError.
pub(crate) async fn begin_connector_application_document_write_session_pending_async_in(
    write_stack_pin: ConnectorWriteStackPin,
    write_lease: &novarocks_spi::connector::ConnectorWrite,
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
        write_stack_pin,
        catalog_properties,
        request,
        receiving,
    )
    .await
}

#[cfg(test)]
pub(crate) mod tests;

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
