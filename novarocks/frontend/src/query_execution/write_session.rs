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
//! What it deliberately is not: there is no operation id, no cohort, no
//! execution attempt, no expected-physical-writer manifest, and no report
//! coverage. Those existed because a writer handle used to be bound to a
//! placement. It no longer is, so completeness comes from the execution graph
//! closing rather than from a pre-enumerated identity tree.
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
    ConnectorPreparedWriteSet, ConnectorWriteBeginRequest, ConnectorWriteFinishRequest,
    ConnectorWriteSessionAbortRequest, ConnectorWriteSessionPlan,
    ConnectorWriteSessionReconcileRequest, ConnectorWriteTargetPlan, PreparedWriteSetLedger,
    UniqueWriterHandleLedger, WriteRowCountAccumulator, WriteTargetOrdinal,
};
use novarocks_spi::connector::{
    ConnectorError, ConnectorErrorKind, ConnectorRequestContext, ConnectorStorageResolver,
    ConnectorWriteAbortOutcome, ConnectorWriteReceipt, ExternalMutationEvidence,
    ExternalMutationOutcome,
};

use crate::connector::control_host::ConnectorWriteStackLease;
use crate::query_execution::write_result::DecodedPreparedWriteSet;
use novarocks_plan_codec::SealedWriteTargets;

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

/// One distributed write's frontend session.
pub(crate) struct ConnectorWriteSession {
    lease: ConnectorWriteStackLease,
    plan: ConnectorWriteSessionPlan,
    /// The catalog runtime this session's writers execute against, kept whole
    /// rather than reduced to its handle: the backend leases a catalog from its
    /// properties, and a writer node that named a handle the query never leased
    /// cannot resolve a write runtime on the backend at all.
    catalog_properties: novarocks_spi::connector::CatalogProperties,
    accumulated: Mutex<AccumulatedWriteSet>,
    terminal: Mutex<Option<TerminalDecision>>,
    finish_invocations: AtomicUsize,
    /// The frontend-local, terminal-only storage capability of the query
    /// attempt whose fragments this session commits.
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
    terminal_storage: Mutex<Option<Arc<dyn ConnectorStorageResolver>>>,
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
#[derive(Default)]
struct AccumulatedWriteSet {
    rows: WriteRowCountAccumulator,
    ledger: PreparedWriteSetLedger,
    fragments: Vec<(WriteTargetOrdinal, Vec<u8>)>,
    statistics: Vec<novarocks_spi::connector::write_stack::WriteStatisticsArtifact>,
    statistics_body_bytes: usize,
    statistics_property_bytes: usize,
}

impl ConnectorWriteSession {
    /// Admit the write and freeze its recipes. On return either a session
    /// exists and nothing external has happened yet, or an error was raised and
    /// nothing was started.
    pub(crate) fn begin(
        lease: ConnectorWriteStackLease,
        catalog_properties: novarocks_spi::connector::CatalogProperties,
        request: ConnectorWriteBeginRequest,
    ) -> Result<Self, ConnectorError> {
        let plan = lease.session().begin_write(request)?;
        Ok(Self {
            lease,
            plan,
            catalog_properties,
            accumulated: Mutex::new(AccumulatedWriteSet::default()),
            terminal: Mutex::new(None),
            finish_invocations: AtomicUsize::new(0),
            terminal_storage: Mutex::new(None),
        })
    }

    /// Retain the query attempt's terminal-only storage capability for this
    /// session's external decision.
    ///
    /// The caller takes the capability from a lifecycle lease that has not yet
    /// been finalized, because the hold it carries is what tells finalization
    /// that a connector write still needs the attempt's credential leases.
    ///
    /// A statement that drives several rounds against one session installs the
    /// newest round's capability. The one it replaces releases its hold right
    /// away: the round that produced it can no longer be the round that
    /// commits, so its credential material is nothing this session still needs.
    pub(crate) fn retain_terminal_storage_resolver(
        &self,
        resolver: Arc<dyn ConnectorStorageResolver>,
    ) {
        if let Ok(mut terminal_storage) = self.terminal_storage.lock() {
            *terminal_storage = Some(resolver);
        }
    }

    /// The sealed ordinal set a prepared write set may not exceed.
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

    pub(crate) fn seal_write_targets(&self) -> Result<SealedWriteTargets, ConnectorError> {
        let encoder = self.lease.handle_encoder();
        let mut ledger = UniqueWriterHandleLedger::new();
        let mut handles = std::collections::BTreeMap::new();
        for target in self.plan.targets() {
            let encoded = encoder
                .encode_writer_handle(target.handle())
                .map_err(|error| {
                    ConnectorError::new(ConnectorErrorKind::Internal, error.to_string())
                })?;
            let canonical = encoder
                .canonical_writer_handle_bytes(target.handle())
                .map_err(|error| {
                    ConnectorError::new(ConnectorErrorKind::Internal, error.to_string())
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

    /// Turn canonical fragments into provider values this generation owns.
    fn interpret_parts(
        &self,
        row_count: u64,
        fragments: Vec<(WriteTargetOrdinal, Vec<u8>)>,
    ) -> Result<ConnectorPreparedWriteSet, ConnectorError> {
        use novarocks_proto_codec::FieldPath;
        use novarocks_proto_codec::connector_write::ValidatedCommitFragment;
        use prost::Message;

        let decoder = self.lease.fragment_decoder();
        let mut decoded = Vec::with_capacity(fragments.len());
        for (index, (target, bytes)) in fragments.into_iter().enumerate() {
            let raw =
                write_dto::ConnectorCommitFragment::decode(bytes.as_slice()).map_err(|error| {
                    ConnectorError::new(
                        ConnectorErrorKind::CorruptData,
                        format!(
                            "prepared write set fragment {index} is not a commit fragment: {error}"
                        ),
                    )
                })?;
            // Re-validated at the trust boundary even though the producer and
            // the root already did: a frontend that trusted a backend's
            // validation could not notice a backend that got it wrong.
            let validated = ValidatedCommitFragment::parse(
                raw,
                FieldPath::root("prepared_write_set").index(index),
            )
            .map_err(|error| {
                ConnectorError::new(ConnectorErrorKind::CorruptData, error.to_string())
            })?;
            let fragment = decoder
                .decode_commit_fragment(&validated)
                .map_err(|error| {
                    ConnectorError::new(ConnectorErrorKind::CorruptData, error.to_string())
                })?;
            decoded.push((target, fragment));
        }
        ConnectorPreparedWriteSet::try_new(row_count, decoded, &self.expected_targets())
    }

    /// Collect one query's complete prepared write set into this session.
    ///
    /// A statement that drives several queries against one session -- a
    /// copy-on-write mutation, a distributed rewrite -- calls this once per
    /// query and commits once at the end. Each set is complete for its own
    /// execution graph; what accumulates is the statement's union.
    ///
    /// The frozen budgets are charged here, on the union. Charging them per
    /// query would let a statement hold an unbounded amount before commit while
    /// every individual query looked well inside its limit, and the limits
    /// exist to bound exactly what the frontend holds.
    pub(crate) fn accumulate(
        &self,
        prepared: DecodedPreparedWriteSet,
    ) -> Result<(), ConnectorError> {
        // Refuse after a terminal decision: a set arriving then belongs to work
        // this session already answered for.
        if self.lock_terminal()?.is_some() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector write session already reached a terminal decision",
            ));
        }
        let mut accumulated = self.lock_accumulated()?;
        let (row_count, fragments, statistics) = prepared.into_parts();
        let mut next_rows = accumulated.rows;
        next_rows.add(row_count)?;
        let mut next_ledger = accumulated.ledger;
        for (_, bytes) in &fragments {
            next_ledger.reserve_fragment(bytes.len())?;
        }
        let expected = self
            .plan
            .targets()
            .iter()
            .flat_map(|target| {
                target
                    .statistics()
                    .requirements()
                    .iter()
                    .map(move |requirement| (target.ordinal(), requirement.artifact().clone()))
            })
            .collect::<std::collections::BTreeSet<_>>();
        let mut observed = accumulated
            .statistics
            .iter()
            .map(|artifact| (artifact.target(), artifact.draft().identity().clone()))
            .collect::<std::collections::BTreeSet<_>>();
        if accumulated
            .statistics
            .len()
            .checked_add(statistics.len())
            .is_none_or(|count| {
                count > novarocks_spi::connector::MAX_CONNECTOR_STATISTICS_ARTIFACTS
            })
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "write session statistics artifact count exceeded its limit",
            ));
        }
        let mut next_body_bytes = accumulated.statistics_body_bytes;
        let mut next_property_bytes = accumulated.statistics_property_bytes;
        for artifact in &statistics {
            let key = (artifact.target(), artifact.draft().identity().clone());
            if !expected.contains(&key) || !observed.insert(key) {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "write session accumulated an unknown or duplicate statistics artifact",
                ));
            }
            next_body_bytes = next_body_bytes
                .checked_add(artifact.draft().body().len())
                .filter(|bytes| {
                    *bytes <= novarocks_spi::connector::MAX_CONNECTOR_STATISTICS_RESULT_BODY_BYTES
                })
                .ok_or_else(|| {
                    ConnectorError::new(
                        ConnectorErrorKind::ResourceExhausted,
                        "write session statistics body bytes exceeded their limit",
                    )
                })?;
            let property_bytes =
                artifact
                    .draft()
                    .properties()
                    .iter()
                    .try_fold(0usize, |total, (key, value)| {
                        total
                            .checked_add(key.len())
                            .and_then(|sum| sum.checked_add(value.len()))
                    });
            next_property_bytes = property_bytes
                .and_then(|bytes| next_property_bytes.checked_add(bytes))
                .filter(|bytes| {
                    *bytes <= novarocks_spi::connector::MAX_CONNECTOR_STATISTICS_PAYLOAD_BYTES
                })
                .ok_or_else(|| {
                    ConnectorError::new(
                        ConnectorErrorKind::ResourceExhausted,
                        "write session statistics property bytes exceeded their limit",
                    )
                })?;
        }
        accumulated.rows = next_rows;
        accumulated.ledger = next_ledger;
        accumulated.fragments.extend(fragments);
        accumulated.statistics_body_bytes = next_body_bytes;
        accumulated.statistics_property_bytes = next_property_bytes;
        accumulated.statistics.extend(statistics);
        Ok(())
    }

    /// Perform the one external commit over everything accumulated so far.
    ///
    /// The caller must already have established BOTH halves of the barrier for
    /// every query it drove: a complete prepared write set each time, and a
    /// lifecycle terminal set in which every participant succeeded. Neither
    /// implies the other, so neither is checked here -- this method exists to be
    /// un-callable until both hold.
    pub(crate) fn finish_accumulated(
        &self,
        context: ConnectorRequestContext,
    ) -> Result<ExternalMutationOutcome<ConnectorWriteReceipt>, ConnectorError> {
        ensure_finish_context_active(&context)?;
        self.claim_terminal(TerminalDecision::Committed)?;
        let outcome = self.commit_accumulated(context);
        // Reconciliation is the one decision that may still follow a commit,
        // and it reads the same object store. Every other way out of here --
        // committed, uncommitted, or an error that leaves neither a retry nor
        // an abort reachable -- settles the session.
        if !self.awaits_reconciliation() {
            self.release_terminal_storage_resolver();
        }
        outcome
    }

    fn commit_accumulated(
        &self,
        context: ConnectorRequestContext,
    ) -> Result<ExternalMutationOutcome<ConnectorWriteReceipt>, ConnectorError> {
        let context = self.terminal_context(context)?;
        self.commit_accumulated_with_context(context)
    }

    fn commit_accumulated_with_context(
        &self,
        context: ConnectorRequestContext,
    ) -> Result<ExternalMutationOutcome<ConnectorWriteReceipt>, ConnectorError> {
        let (row_count, fragments, statistics) = {
            let mut accumulated = self.lock_accumulated()?;
            (
                accumulated.rows.get(),
                std::mem::take(&mut accumulated.fragments),
                std::mem::take(&mut accumulated.statistics),
            )
        };
        let prepared = self.interpret_parts(row_count, fragments)?;
        self.finish_invocations.fetch_add(1, Ordering::SeqCst);
        let outcome = self
            .lease
            .session()
            .finish_write(ConnectorWriteFinishRequest {
                commit: self.plan.commit_handle(),
                prepared,
                statistics,
                context,
            })?;
        if matches!(outcome, ExternalMutationOutcome::CommitUnknown { .. }) {
            self.record_terminal(TerminalDecision::CommitUnknown);
        }
        Ok(outcome)
    }

    /// The rows accumulated so far. Report them to a client only after the
    /// external commit is known to have succeeded.
    #[cfg(test)]
    pub(crate) fn accumulated_row_count(&self) -> Result<u64, ConnectorError> {
        Ok(self.lock_accumulated()?.rows.get())
    }

    /// Accumulate one query's set and commit immediately. The shape almost
    /// every write has.
    pub(crate) fn finish(
        &self,
        prepared: DecodedPreparedWriteSet,
        context: ConnectorRequestContext,
    ) -> Result<ExternalMutationOutcome<ConnectorWriteReceipt>, ConnectorError> {
        self.accumulate(prepared)?;
        self.finish_accumulated(context)
    }

    /// Seal a write whose statement-level owner still has one terminal action
    /// to perform with the same query-attempt storage capability.
    ///
    /// A staged CTAS first seals its writer reports and only then publishes
    /// the catalog-side staged-create action. The seal itself has no external
    /// catalog effect, so releasing the capability here would leave the
    /// following manifest and metadata I/O with the already-revoked planning
    /// route. Transfer one cloned terminal context to that owner instead.
    pub(crate) fn finish_for_following_terminal_action(
        &self,
        prepared: DecodedPreparedWriteSet,
        context: ConnectorRequestContext,
    ) -> Result<
        (
            ExternalMutationOutcome<ConnectorWriteReceipt>,
            ConnectorRequestContext,
        ),
        ConnectorError,
    > {
        self.accumulate(prepared)?;
        self.claim_terminal(TerminalDecision::Committed)?;
        let terminal_context = self.terminal_context(context)?;
        let outcome = self.commit_accumulated_with_context(terminal_context.clone());
        // The returned context owns the terminal capability from here. On an
        // error it drops below, so the session never leaves an orphaned hold.
        self.release_terminal_storage_resolver();
        outcome.map(|outcome| (outcome, terminal_context))
    }

    /// Release a session that never reached a complete prepared write set.
    pub(crate) fn abort(
        &self,
        context: ConnectorRequestContext,
    ) -> Result<ConnectorWriteAbortOutcome, ConnectorError> {
        self.claim_terminal(TerminalDecision::Aborted)?;
        let outcome = self.terminal_context(context).and_then(|context| {
            self.lease
                .session()
                .abort_write(ConnectorWriteSessionAbortRequest {
                    commit: self.plan.commit_handle(),
                    context,
                })
        });
        // An aborted session can reach nothing else, whether the provider's
        // cleanup succeeded or not.
        self.release_terminal_storage_resolver();
        outcome
    }

    /// Resolve a commit whose external outcome is unknown. Only reachable after
    /// a commit that reported exactly that.
    pub(crate) fn reconcile(
        &self,
        evidence: ExternalMutationEvidence,
        context: ConnectorRequestContext,
    ) -> Result<ExternalMutationOutcome<ConnectorWriteReceipt>, ConnectorError> {
        {
            let terminal = self.lock_terminal()?;
            if *terminal != Some(TerminalDecision::CommitUnknown) {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "connector write session has no unknown commit outcome to reconcile",
                ));
            }
        }
        let outcome = self.terminal_context(context).and_then(|context| {
            self.lease
                .session()
                .reconcile_write(ConnectorWriteSessionReconcileRequest {
                    commit: self.plan.commit_handle(),
                    evidence,
                    context,
                })
        });
        // A reconciliation that came back unknown, or that failed to reach the
        // provider at all, may be asked again and needs the same capability.
        // Anything else is the session's last external decision.
        let settled = matches!(
            &outcome,
            Ok(outcome) if !matches!(outcome, ExternalMutationOutcome::CommitUnknown { .. })
        );
        if settled {
            self.release_terminal_storage_resolver();
        }
        outcome
    }

    /// Decorate one terminal request with this session's retained capability.
    ///
    /// The vended-credential sink is removed with it: the attempt's credential
    /// descriptors were frozen at Init, so a terminal metadata reload may
    /// consume an existing lease but can never contribute a new one.
    fn terminal_context(
        &self,
        context: ConnectorRequestContext,
    ) -> Result<ConnectorRequestContext, ConnectorError> {
        let context = context.without_vended_credential_lease_sink();
        Ok(match self.lock_terminal_storage()?.as_ref() {
            Some(resolver) => context.with_storage_resolver(Arc::clone(resolver)),
            None => context,
        })
    }

    /// Whether a commit reported an unknown outcome that reconciliation has
    /// not answered yet.
    fn awaits_reconciliation(&self) -> bool {
        self.lock_terminal()
            .is_ok_and(|terminal| *terminal == Some(TerminalDecision::CommitUnknown))
    }

    /// Drop the retained capability, releasing the hold that deferred the
    /// attempt's credential cleanup.
    fn release_terminal_storage_resolver(&self) {
        if let Ok(mut terminal_storage) = self.terminal_storage.lock() {
            *terminal_storage = None;
        }
    }

    fn lock_terminal_storage(
        &self,
    ) -> Result<std::sync::MutexGuard<'_, Option<Arc<dyn ConnectorStorageResolver>>>, ConnectorError>
    {
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
    if context.cancellation().is_cancelled() {
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

/// Open one distributed write's session on a write stack already pinned to the
/// generation that planned it.
///
/// The catalog identity comes from the same generation's write lease rather
/// than from a fresh catalog resolution: the recipes the session seals are
/// stamped into fragments beside this handle, and a handle from a different
/// incarnation would name a runtime that never admitted them.
pub(crate) fn begin_connector_write_session(
    lease: ConnectorWriteStackLease,
    write_lease: &novarocks_spi::connector::ConnectorWriteLease,
    request: ConnectorWriteBeginRequest,
) -> Result<std::sync::Arc<ConnectorWriteSession>, String> {
    let catalog_properties = write_lease.catalog_properties().cloned().ok_or_else(|| {
        "connector write lease has no immutable catalog runtime identity".to_string()
    })?;
    ConnectorWriteSession::begin(lease, catalog_properties, request)
        .map(std::sync::Arc::new)
        .map_err(|error| format!("begin connector write session: {error}"))
}

/// The external commit of one completed write session, and the rows it made
/// visible.
///
/// `affected_rows` exists only on `KnownCommitted`. That is the whole point of
/// the type: the row count is known as soon as the data plane closes, but
/// reporting it to a client before the commit succeeded would name rows that
/// may never become visible -- and on `CommitUnknown` nobody yet knows whether
/// they did.
pub(crate) struct CommittedWriteSession {
    outcome: ExternalMutationOutcome<ConnectorWriteReceipt>,
    affected_rows: Option<u64>,
}

impl CommittedWriteSession {
    /// The rows a client may be told about, present only after a commit that
    /// is known to have succeeded.
    pub(crate) const fn affected_rows(&self) -> Option<u64> {
        self.affected_rows
    }

    pub(crate) fn into_outcome(self) -> ExternalMutationOutcome<ConnectorWriteReceipt> {
        self.outcome
    }
}

/// A sealed write whose statement owner must perform one following terminal
/// action using the same attempt-scoped storage capability.
///
/// This is frontend-local and carries only a request context. CTAS is its
/// sole consumer: its staged writer receipt becomes catalog metadata during
/// the subsequent publication step.
pub(crate) struct FollowupTerminalWriteSession {
    outcome: ExternalMutationOutcome<ConnectorWriteReceipt>,
    affected_rows: Option<u64>,
    context: ConnectorRequestContext,
}

impl FollowupTerminalWriteSession {
    pub(crate) fn into_parts(
        self,
    ) -> (
        ExternalMutationOutcome<ConnectorWriteReceipt>,
        Option<u64>,
        ConnectorRequestContext,
    ) {
        (self.outcome, self.affected_rows, self.context)
    }
}

/// Perform the one external commit for a write whose data plane closed and
/// whose execution succeeded, then gate its affected-row count on the result.
pub(crate) fn finish_write_session(
    completion: crate::query_execution::outcome::ConnectorWriteSessionCompletion,
    context: ConnectorRequestContext,
) -> Result<CommittedWriteSession, ConnectorError> {
    let row_count = completion.row_count();
    let (session, prepared) = completion.into_parts();
    let outcome = session.finish(prepared, context)?;
    let affected_rows =
        matches!(outcome, ExternalMutationOutcome::KnownCommitted { .. }).then_some(row_count);
    Ok(CommittedWriteSession {
        outcome,
        affected_rows,
    })
}

/// Seal a write while transferring its terminal storage capability to the
/// immediately following statement-owned action.
pub(crate) fn finish_write_session_for_following_terminal_action(
    completion: crate::query_execution::outcome::ConnectorWriteSessionCompletion,
    context: ConnectorRequestContext,
) -> Result<FollowupTerminalWriteSession, ConnectorError> {
    let row_count = completion.row_count();
    let (session, prepared) = completion.into_parts();
    let (outcome, context) = session.finish_for_following_terminal_action(prepared, context)?;
    let affected_rows =
        matches!(outcome, ExternalMutationOutcome::KnownCommitted { .. }).then_some(row_count);
    Ok(FollowupTerminalWriteSession {
        outcome,
        affected_rows,
        context,
    })
}

#[cfg(test)]
pub(crate) mod tests {
    use std::sync::Arc;

    use novarocks_proto_codec::connector_common::encode_connector_payload_message;
    use novarocks_spi::connector::ConnectorControlWriteBinding;
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
    struct FakeFragment(u32);

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
        pub(crate) finish: usize,
        pub(crate) abort: usize,
        pub(crate) reconcile: usize,
        pub(crate) statistics: Vec<WriteStatisticsArtifact>,
        /// What the last terminal request could actually read object storage
        /// with. A real provider resolves this before it reloads table metadata
        /// or writes a manifest, so recording it here is recording whether the
        /// commit could have happened at all.
        pub(crate) terminal_storage: Option<Result<String, String>>,
    }

    struct FakeSession {
        adapter: WriteRuntimeAdapter<FakeProvider>,
        binding_key: ConnectorProviderBindingKey,
        targets: usize,
        statistics: bool,
        recorded: Arc<Mutex<Recorded>>,
        finish_outcome: Mutex<Option<ExternalMutationOutcome<ConnectorWriteReceipt>>>,
    }

    impl novarocks_spi::connector::write_stack::ConnectorWriteControl for FakeSession {
        fn binding_key(&self) -> &ConnectorProviderBindingKey {
            &self.binding_key
        }

        fn begin_write(
            &self,
            _request: ConnectorWriteBeginRequest,
        ) -> Result<ConnectorWriteSessionPlan, ConnectorError> {
            let commit = self.adapter.wrap_commit_handle(FakeCommit);
            let targets = (0..self.targets)
                .map(|index| {
                    let ordinal = WriteTargetOrdinal::try_new(
                        u32::try_from(index).expect("bounded ordinal"),
                    )?;
                    let handle = self.adapter.wrap_writer_handle(FakeHandle(ordinal.get()));
                    let input = novarocks_spi::connector::ConnectorWriteInputShape::Data {
                        fields: vec![novarocks_spi::connector::ConnectorWriteFieldBinding::new(
                            novarocks_spi::connector::ConnectorWriteFieldToken::from_bytes([1; 32]),
                            arrow::datatypes::Field::new(
                                "v",
                                arrow::datatypes::DataType::Int64,
                                true,
                            ),
                        )],
                    };
                    let target = ConnectorWriteTargetPlan::new(ordinal, handle, input.clone());
                    if !self.statistics {
                        return Ok(target);
                    }
                    let requirement = StatisticsRequiredAggregation::try_new(
                        StatisticsScanColumn::try_new(
                            0,
                            "v",
                            arrow::datatypes::DataType::Int64,
                            true,
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
            {
                let mut recorded = self.recorded.lock().expect("recorded");
                recorded.finish += 1;
                recorded.statistics = request.statistics.clone();
                recorded.terminal_storage = Some(probe_vended_storage(&request.context));
            }
            self.finish_outcome
                .lock()
                .expect("outcome")
                .take()
                .ok_or_else(|| {
                    ConnectorError::new(ConnectorErrorKind::Internal, "no scripted outcome")
                })
        }

        fn abort_write(
            &self,
            request: ConnectorWriteSessionAbortRequest<'_>,
        ) -> Result<ConnectorWriteAbortOutcome, ConnectorError> {
            let mut recorded = self.recorded.lock().expect("recorded");
            recorded.abort += 1;
            recorded.terminal_storage = Some(probe_vended_storage(&request.context));
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
            Err(ConnectorError::new(
                ConnectorErrorKind::Internal,
                "reconcile outcome is not scripted in this test",
            ))
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
            Ok(self.adapter.wrap_commit_fragment(FakeFragment(0)))
        }
    }

    struct UnusedLegacyControl;

    impl LegacyWriteControl for UnusedLegacyControl {
        fn binding_key(&self) -> &ConnectorProviderBindingKey {
            unreachable!("the legacy control is not exercised by the write session")
        }
    }

    pub(crate) struct Fixture {
        pub(crate) session: Arc<ConnectorWriteSession>,
        pub(crate) recorded: Arc<Mutex<Recorded>>,
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
            Arc::new(FakeEncoder { payload_bytes }),
            Arc::new(FakeDecoder { adapter }),
        );
        let lease = ConnectorWriteStackLease::new(
            novarocks_spi::connector::ConnectorControlRuntimeId::new(),
            group,
            || {},
        );
        let session = Arc::new(
            ConnectorWriteSession::begin(lease, catalog_properties(), begin_request())
                .expect("begin write"),
        );
        Fixture { session, recorded }
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
            })),
        });
        let group = ConnectorControlWriteBinding::new(
            Arc::new(UnusedLegacyControl),
            session_control,
            Arc::new(FakeEncoder { payload_bytes: 16 }),
            Arc::new(FakeDecoder { adapter }),
        );
        let lease = ConnectorWriteStackLease::new(
            novarocks_spi::connector::ConnectorControlRuntimeId::new(),
            group,
            || {},
        );
        let session = Arc::new(
            ConnectorWriteSession::begin(lease, catalog_properties(), begin_request())
                .expect("begin write"),
        );
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
                fields: vec![novarocks_spi::connector::ConnectorWriteFieldRequest::new(
                    arrow::datatypes::Field::new("v", arrow::datatypes::DataType::Int64, true),
                )],
            },
            base: None,
            flavor: novarocks_spi::connector::write_stack::ConnectorWriteSessionFlavor::Ordinary,
            context: request_context(),
        }
    }

    pub(crate) fn request_context() -> ConnectorRequestContext {
        struct NotCancelled;
        impl novarocks_spi::connector::ConnectorCancellation for NotCancelled {
            fn is_cancelled(&self) -> bool {
                false
            }
        }
        ConnectorRequestContext::try_new(
            std::time::Instant::now() + std::time::Duration::from_secs(60),
            Arc::new(NotCancelled),
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
                u64::MAX,
                novarocks_secret::SecretValue::new("access"),
                novarocks_secret::SecretValue::new("secret"),
                novarocks_secret::SecretValue::new("token"),
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
        session.retain_terminal_storage_resolver(accounting.retain_capability());
        accounting.finalize();
        accounting
    }

    /// A canonical provider-neutral commit-fragment envelope. The fake provider
    /// owns the private payload and the session exercises the real outer codec.
    fn fragment_bytes(path: &str) -> Vec<u8> {
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

    fn completion(
        session: &Arc<ConnectorWriteSession>,
        row_count: u64,
    ) -> crate::query_execution::outcome::ConnectorWriteSessionCompletion {
        crate::query_execution::outcome::ConnectorWriteSessionCompletion::for_test(
            Arc::clone(session),
            DecodedPreparedWriteSet::for_test(row_count, Vec::new()),
        )
    }

    #[test]
    fn affected_rows_are_reported_only_after_a_known_successful_commit() {
        let fixture = fixture_with_outcome(1, 16, known_committed());
        let committed = finish_write_session(completion(&fixture.session, 7), request_context())
            .expect("finish");

        assert_eq!(committed.affected_rows(), Some(7));
        assert_eq!(fixture.session.finish_invocations(), 1);
        assert_eq!(fixture.recorded.lock().expect("recorded").finish, 1);
    }

    #[test]
    fn a_commit_unknown_outcome_reports_no_affected_rows() {
        // The rows were accepted by every writer, so the count is known -- but
        // whether they became visible is not, and reporting success here would
        // tell a client about rows that may not exist.
        let fixture = fixture_with_outcome(1, 16, commit_unknown());
        let committed = finish_write_session(completion(&fixture.session, 7), request_context())
            .expect("finish");

        assert!(committed.affected_rows().is_none());
        assert!(matches!(
            committed.into_outcome(),
            ExternalMutationOutcome::CommitUnknown { .. }
        ));
        assert_eq!(fixture.session.finish_invocations(), 1);
    }

    #[test]
    fn a_known_uncommitted_commit_reports_no_affected_rows() {
        let fixture = fixture(1, 16);
        let committed = finish_write_session(completion(&fixture.session, 7), request_context())
            .expect("finish");

        assert!(committed.affected_rows().is_none());
        assert!(matches!(
            committed.into_outcome(),
            ExternalMutationOutcome::KnownUncommitted { .. }
        ));
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
        let _ = fixture.session.finish(prepared, request_context());
        assert_eq!(fixture.session.finish_invocations(), 1);
        assert_eq!(fixture.recorded.lock().expect("recorded").finish, 1);

        // A second, different decision is refused, and the connector is not
        // asked again.
        let error = fixture
            .session
            .abort(request_context())
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
    fn the_frontend_write_path_composes_from_begin_to_commit() {
        use crate::query_execution::write_barrier::WriteCommitBarrier;
        use novarocks_plan_codec::SealedWriteTargets;

        let fixture = fixture_with_outcome(
            1,
            16,
            ExternalMutationOutcome::KnownUncommitted {
                failure: novarocks_spi::connector::ConnectorMutationFailure::new(
                    novarocks_spi::connector::ConnectorMutationFailureKind::Unavailable,
                    "scripted",
                ),
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

        // 3. Both facts, then and only then the commit.
        let mut barrier = WriteCommitBarrier::new();
        barrier.observe_prepared_write_set(prepared);
        barrier.observe_execution_terminals(true);
        let committable = barrier.into_committable().expect("both facts hold");
        assert_eq!(committable.row_count(), 7);

        let outcome = fixture
            .session
            .finish(committable, request_context())
            .expect("finish reaches the connector");
        assert!(matches!(
            outcome,
            ExternalMutationOutcome::KnownUncommitted { .. }
        ));
        assert_eq!(fixture.session.finish_invocations(), 1);
    }

    /// The same composition, stopped by a failed participant. The connector is
    /// never asked to commit, which is the whole point of splitting the gate.
    #[test]
    fn a_failed_participant_stops_the_composed_path_before_the_connector() {
        use crate::query_execution::write_barrier::WriteCommitBarrier;

        let fixture = fixture(1, 16);
        let mut barrier = WriteCommitBarrier::new();
        barrier.observe_prepared_write_set(empty_prepared());
        barrier.observe_execution_terminals(false);
        assert!(barrier.into_committable().is_err());
        assert_eq!(fixture.session.finish_invocations(), 0);
        assert_eq!(fixture.recorded.lock().expect("recorded").finish, 0);
    }

    /// A copy-on-write mutation and a distributed rewrite drive several
    /// queries against one session and commit once. Each query's set is
    /// complete for its own graph; the statement commits their union.
    #[test]
    fn a_session_commits_the_union_of_every_query_it_drove() {
        let fixture = fixture(1, 16);
        let target = WriteTargetOrdinal::try_new(0).expect("ordinal");

        fixture
            .session
            .accumulate(DecodedPreparedWriteSet::for_test(
                4,
                vec![(target, fragment_bytes("s3://b/a.parquet"))],
            ))
            .expect("first query");
        fixture
            .session
            .accumulate(DecodedPreparedWriteSet::for_test(
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

        let _ = fixture.session.finish_accumulated(request_context());
        // One commit for the whole statement, not one per query.
        assert_eq!(fixture.session.finish_invocations(), 1);
        assert_eq!(fixture.recorded.lock().expect("recorded").finish, 1);
    }

    #[test]
    fn statistics_artifacts_follow_the_prepared_set_into_the_single_commit() {
        let fixture = fixture_with_statistics(2);
        fixture
            .session
            .accumulate(DecodedPreparedWriteSet::for_test_with_statistics(
                4,
                Vec::new(),
                vec![statistics_artifact(0, 1, b"zero")],
            ))
            .expect("first query statistics");
        fixture
            .session
            .accumulate(DecodedPreparedWriteSet::for_test_with_statistics(
                6,
                Vec::new(),
                vec![statistics_artifact(1, 2, b"one")],
            ))
            .expect("second query statistics");

        let _ = fixture.session.finish_accumulated(request_context());
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
            .accumulate(DecodedPreparedWriteSet::for_test_with_statistics(
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
            .accumulate(DecodedPreparedWriteSet::for_test_with_statistics(
                3,
                Vec::new(),
                vec![statistics_artifact(0, 1, b"valid")],
            ))
            .expect("valid set after rejection");
        let _ = fixture.session.finish_accumulated(request_context());
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
        for _ in 0..2 {
            fixture
                .session
                .accumulate(DecodedPreparedWriteSet::for_test(
                    0,
                    vec![(target, Vec::new()); per_query],
                ))
                .expect("within the union budget");
        }
        let error = fixture
            .session
            .accumulate(DecodedPreparedWriteSet::for_test(
                0,
                vec![(target, Vec::new())],
            ))
            .expect_err("over the union budget");
        assert_eq!(error.kind(), ConnectorErrorKind::ResourceExhausted);
    }

    #[test]
    fn a_set_arriving_after_the_terminal_decision_is_refused() {
        let fixture = fixture(1, 16);
        let _ = fixture.session.finish(empty_prepared(), request_context());
        let error = fixture
            .session
            .accumulate(empty_prepared())
            .expect_err("accumulate after terminal");
        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
        assert_eq!(fixture.session.finish_invocations(), 1);
    }

    #[test]
    fn reconcile_is_unreachable_until_a_commit_reported_an_unknown_outcome() {
        let fixture = fixture(1, 16);
        let error = fixture
            .session
            .reconcile(evidence(), request_context())
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

        let committed = finish_write_session(completion(&fixture.session, 7), request_context())
            .expect("finish");

        assert_eq!(committed.affected_rows(), Some(7));
        assert_eq!(
            fixture.recorded.lock().expect("recorded").terminal_storage,
            Some(Ok(TABLE_PREFIX.to_string()))
        );
        // The decision is made, so the credentials are not pinned any longer.
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

        let committed = finish_write_session_for_following_terminal_action(
            completion(&fixture.session, 7),
            request_context(),
        )
        .expect("seal write");
        let (outcome, affected_rows, terminal_context) = committed.into_parts();

        assert!(matches!(
            outcome,
            ExternalMutationOutcome::KnownCommitted { .. }
        ));
        assert_eq!(affected_rows, Some(7));
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

    /// A commit whose external outcome is unknown is not finished with storage:
    /// reconciliation reads the same object store, and it is the only decision
    /// still reachable.
    #[test]
    fn a_commit_with_an_unknown_outcome_keeps_its_capability_for_reconciliation() {
        let fixture = fixture_with_outcome(1, 16, commit_unknown());
        let accounting = finalize_attempt_retaining_terminal_storage(&fixture.session);

        let _ = fixture.session.finish(empty_prepared(), request_context());
        assert_eq!(
            fixture.recorded.lock().expect("recorded").terminal_storage,
            Some(Ok(TABLE_PREFIX.to_string()))
        );
        assert_eq!(accounting.holds(), 1);
        assert!(!accounting.leases_cleared());

        // Reconciliation reaches the provider with the same capability. This
        // fixture scripts no reconcile outcome, so the session stays
        // reconcilable and keeps holding.
        let _ = fixture.session.reconcile(evidence(), request_context());
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

        fixture.session.abort(request_context()).expect("abort");

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
}
