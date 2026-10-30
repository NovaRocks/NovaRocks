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

//! The sole publication executor. Prepared mechanisms report evidence; only
//! this journal owns the dispatch fact and its independently recorded cleanup.

use super::*;
use crate::catalog::publication::CatalogEvidencePolicy;
use crate::catalog::publication_dispatch::{PreparationResult, PreparedPublication};
use crate::commit::publication_descriptor::RecoveryBinding;
use std::future::Future;

/// Move-only continuation of the original dispatch, before its ready child
/// and activity guards exit and before necessary finalization starts.
pub(crate) struct PendingPublicationFinalization<R> {
    journal: PublicationJournal<R>,
    entry: JournalEntryRef,
    policy: CatalogEvidencePolicy,
    dispatch: Option<ReceivedCatalogDispatch>,
}
struct ReceivedCatalogDispatch {
    future: std::pin::Pin<
        Box<dyn Future<Output = crate::catalog::publication_dispatch::DispatchCompletion> + Send>,
    >,
    finalization: Option<Box<dyn crate::catalog::publication_dispatch::FinalizationWork>>,
    dispatching: Option<crate::activity::ActivityGuard>,
    evidence_wait: Option<crate::activity::ActivityGuard>,
}
impl<R: Clone + Send + Sync + 'static> PendingPublicationFinalization<R> {
    fn settled(
        journal: PublicationJournal<R>,
        entry: JournalEntryRef,
        policy: CatalogEvidencePolicy,
    ) -> Self {
        Self {
            journal,
            entry,
            policy,
            dispatch: None,
        }
    }
    pub(crate) fn entry(&self) -> JournalEntryRef {
        self.entry
    }
    pub(crate) fn evidence_policy(&self) -> CatalogEvidencePolicy {
        self.policy
    }
    /// Continue only this journal's already-received dispatch. No second
    /// preparation, dispatch claim, deadline, or finalization grant is minted.
    pub(crate) async fn finish(self, policy: CatalogEvidencePolicy) -> Result<JournalEntryRef> {
        let entry_ref = self.entry;
        let journal = self.journal;
        let Some(ReceivedCatalogDispatch {
            future,
            finalization,
            dispatching,
            evidence_wait,
        }) = self.dispatch
        else {
            return Ok(entry_ref);
        };
        // The original positive fact is already in the journal and, for a
        // catalog mutation, in its Source receiving before this foreign Drop.
        drop(future);
        if let Some(activity) = &journal.activity {
            let state = journal
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if matches!(&state.entries[&entry_ref.0].phase,
                EntryPhase::Concluded(DispatchEvidence::Unknown { failure, .. } | DispatchEvidence::NotDispatched { failure } | DispatchEvidence::Rejected { failure, .. })
                if failure.kind() == ConnectorMutationFailureKind::DeadlineExceeded)
            {
                activity.evidence_expired();
            }
        }
        drop(evidence_wait);
        drop(dispatching);
        if let Some(work) = finalization {
            let finalization = match journal.finalization_context(entry_ref, policy) {
                Ok(context) => {
                    let _finalizing = journal.finalization_activity(entry_ref);
                    work.run(context).await
                }
                Err(error) => ExternalMutationFinalization::Failed(ConnectorMutationFailure::new(
                    ConnectorMutationFailureKind::Internal,
                    error.message(),
                )),
            };
            journal.record_entry_finalization(entry_ref, finalization);
        }
        Ok(entry_ref)
    }
}

impl<R: Clone + Send + Sync + 'static> PublicationJournal<R> {
    pub(crate) async fn publish_once<F, Fut>(
        &self,
        preparation: ConnectorRequestContext,
        policy: CatalogEvidencePolicy,
        prepare: F,
    ) -> Result<JournalEntryRef>
    where
        F: FnOnce(
                PublicationDescriptor,
                novarocks_spi::connector::ProviderBindingEpoch,
                ConnectorRequestContext,
            ) -> Fut
            + Send,
        Fut: Future<Output = PreparationResult<PreparedPublication>> + Send,
    {
        let pending = self
            .publish_once_to_finalization(preparation, policy, prepare)
            .await?;
        pending.finish(policy).await
    }

    pub(crate) async fn publish_once_to_finalization<F, Fut>(
        &self,
        preparation: ConnectorRequestContext,
        policy: CatalogEvidencePolicy,
        prepare: F,
    ) -> Result<PendingPublicationFinalization<R>>
    where
        F: FnOnce(
                PublicationDescriptor,
                novarocks_spi::connector::ProviderBindingEpoch,
                ConnectorRequestContext,
            ) -> Fut
            + Send,
        Fut: Future<Output = PreparationResult<PreparedPublication>> + Send,
    {
        // The claim is acquired before polling any preparation or barrier.
        // A duplicate invocation is an independent call error. It cannot
        // manufacture a rejection or cleanup authority for the live entry.
        let claim = self.claim_dispatch()?;
        let preparing = self
            .activity
            .as_ref()
            .map(|owner| owner.enter(novarocks_spi::connector::CatalogActivityPhase::PreDispatch));
        let entry_ref = JournalEntryRef(claim.key);
        let (descriptor, generation, recorded_cap) = {
            let state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let entry = state
                .entries
                .get(&entry_ref.0)
                .expect("claimed entry exists");
            (
                entry.checked.descriptor.clone(),
                entry.checked.evidence.incarnation(),
                entry.finalization_deadline_cap,
            )
        };
        // A nested publication cannot use a later invocation or an independent
        // cleanup projection to restart its necessary preparation budget.
        let preparation = match recorded_cap {
            Some(cap) => preparation.with_necessary_finalization_deadline_cap(cap),
            None => preparation,
        };
        let preparation = match preparation.necessary_finalization_deadline_cap() {
            Some(cap) => preparation.with_earlier_deadline(cap),
            None => preparation,
        };
        if let Err(error) = preparation.check_active() {
            let failure = super::super::before_dispatch_failure(
                &Error::new(ErrorKind::Unexpected, "Publication preparation stopped")
                    .with_source(error),
            );
            if let Some(activity) = &self.activity {
                activity.preparation_failed(&failure);
            }
            claim.refuse(failure);
            return Ok(PendingPublicationFinalization::settled(
                self.clone(),
                entry_ref,
                policy,
            ));
        }
        // Every adapter receives the original preparation control and owns
        // any started, non-abortable I/O to actual exit. The generic executor
        // must not drop that work in an outer timeout/select.
        let prepared = prepare(descriptor, generation, preparation.clone()).await;
        let prepared = match prepared {
            Ok(prepared) => prepared,
            Err(error) => {
                if let Some(activity) = &self.activity {
                    activity.preparation_failed(&error.0);
                }
                claim.refuse(error.0);
                return Ok(PendingPublicationFinalization::settled(
                    self.clone(),
                    entry_ref,
                    policy,
                ));
            }
        };
        let (binding, dispatch) = match prepared {
            PreparedPublication::Mutation { binding, dispatch } => (binding, dispatch),
            PreparedPublication::NoOp => {
                match claim
                    .begin_dispatch(&preparation, &MemoryDispatchBarrier)
                    .await
                {
                    Ok(_) => self.conclude_entry(
                        entry_ref,
                        DispatchEvidence::Applied {
                            proof: CommitProof::no_op(),
                        },
                    ),
                    Err(error) => {
                        if let Some(activity) = &self.activity {
                            activity
                                .preparation_failed(&super::super::before_dispatch_failure(&error));
                        }
                    }
                }
                return Ok(PendingPublicationFinalization::settled(
                    self.clone(),
                    entry_ref,
                    policy,
                ));
            }
        };
        if let Err(error) = claim.bind(binding) {
            claim.refuse(super::super::before_dispatch_failure(&error));
            return Ok(PendingPublicationFinalization::settled(
                self.clone(),
                entry_ref,
                policy,
            ));
        }
        if let Err(error) = claim
            .begin_dispatch(&preparation, &MemoryDispatchBarrier)
            .await
        {
            if let Some(activity) = &self.activity {
                activity.preparation_failed(&super::super::before_dispatch_failure(&error));
            }
            return Ok(PendingPublicationFinalization::settled(
                self.clone(),
                entry_ref,
                policy,
            ));
        }
        let Some(evidence_context) = self.begin_entry_evidence(entry_ref, &preparation, policy)
        else {
            return Ok(PendingPublicationFinalization::settled(
                self.clone(),
                entry_ref,
                policy,
            ));
        };
        drop(preparing);
        let dispatching = self
            .activity
            .as_ref()
            .map(|owner| owner.enter(novarocks_spi::connector::CatalogActivityPhase::Dispatch));
        let evidence_wait = self
            .activity
            .as_ref()
            .map(|owner| owner.enter(novarocks_spi::connector::CatalogActivityPhase::Evidence));
        self.mark_dispatch_start(entry_ref);
        // The journal has entered its original non-abortable evidence interval;
        // subsequent real requests continue as existing Settlement obligations.
        preparation
            .publication_obligation_started()
            .map_err(|error| {
                Error::new(
                    ErrorKind::Unexpected,
                    "Original publication I/O source is unavailable",
                )
                .with_source(error)
            })?;
        // No statement cancellation branch may drop a dispatched mutation.
        let mut future = dispatch.dispatch_once(evidence_context);
        let completion = std::future::poll_fn(|cx| future.as_mut().poll(cx)).await;
        // Receive canonical dispatch evidence before the ready child's Drop.
        // The caller may now preserve Applied without invoking foreign code.
        self.conclude_entry(entry_ref, completion.evidence);
        Ok(PendingPublicationFinalization {
            journal: self.clone(),
            entry: entry_ref,
            policy,
            dispatch: Some(ReceivedCatalogDispatch {
                future,
                finalization: completion.finalization,
                dispatching,
                evidence_wait,
            }),
        })
    }

    fn begin_entry_evidence(
        &self,
        entry_ref: JournalEntryRef,
        preparation: &ConnectorRequestContext,
        policy: CatalogEvidencePolicy,
    ) -> Option<crate::catalog::publication::EvidenceContext> {
        match policy.begin_evidence(preparation) {
            Ok(context) => Some(context),
            Err(error) => {
                let failure = super::super::before_dispatch_failure(
                    &Error::new(ErrorKind::Unexpected, "Publication evidence grant refused")
                        .with_source(error),
                );
                self.conclude_entry(
                    entry_ref,
                    DispatchEvidence::NotDispatched {
                        failure: failure.clone(),
                    },
                );
                if let Some(activity) = &self.activity {
                    activity.preparation_failed(&failure);
                }
                None
            }
        }
    }

    pub(super) fn mark_dispatch_start(&self, entry_ref: JournalEntryRef) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let entry = state
            .entries
            .get_mut(&entry_ref.0)
            .expect("dispatch entry exists");
        assert!(matches!(entry.phase, EntryPhase::Dispatching));
        assert!(entry.checked.descriptor.first_dispatch_ms.is_none());
        entry.checked.descriptor.first_dispatch_ms = Some(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock is after Unix epoch")
                .as_millis()
                .try_into()
                .expect("timestamp fits i64"),
        );
    }

    pub(crate) fn commit_outcome(&self, entry_ref: JournalEntryRef) -> CatalogOutcome<CommitProof> {
        match self.entry_phase(entry_ref) {
            Phase::Committed(proof, _, finalization) => CatalogOutcome::KnownCommitted {
                effect: proof.effect,
                receipt: proof,
                finalization,
            },
            Phase::Rejected(failure, _) => CatalogOutcome::KnownUncommitted { failure },
            Phase::Unknown(checked, _) | Phase::Issued(checked) => {
                // CatalogOutcome is a projection, not another stored verdict.
                // The external recovery envelope remains owned by the journal.
                let failure = match self.entry_phase(entry_ref) {
                    Phase::Unknown(_, failure) => failure,
                    _ => ConnectorMutationFailure::new(
                        ConnectorMutationFailureKind::Unavailable,
                        "Publication has no dispatch conclusion",
                    ),
                };
                CatalogOutcome::CommitUnknown {
                    failure,
                    evidence: crate::catalog::error::CatalogCommitEvidence::for_target(
                        checked.descriptor.targets.join(","),
                    ),
                }
            }
            _ => CatalogOutcome::uncommitted(
                ConnectorMutationFailureKind::Internal,
                "Publication entry has no conclusion",
            ),
        }
    }
}

impl<R: Clone + Send + Sync + 'static> PublicationJournal<R> {
    pub(crate) fn prepared_publisher_backing_bytes() -> usize {
        std::mem::size_of::<PreparedObservedPublisher<R>>() + std::mem::size_of::<[usize; 2]>()
    }
    pub(crate) fn observe_prepared(
        &self,
        inner: Arc<dyn super::super::prepared::PublicationPreparer>,
    ) -> PreparedObservedPublisher<R> {
        PreparedObservedPublisher {
            inner,
            journal: self.clone(),
        }
    }
    fn invocation_error(&self, message: &str) -> CatalogOutcome<CommitProof> {
        match self.phase() {
            Phase::Committed(_, _, _)
            | Phase::Rejected(_, _)
            | Phase::Unknown(_, _)
            | Phase::Issued(_) => {
                self.commit_outcome(self.current_entry().expect("active publication entry"))
            }
            Phase::Ready | Phase::Waiting => CatalogOutcome::unknown(
                message,
                crate::catalog::error::CatalogCommitEvidence::for_target(
                    self.descriptor()
                        .expect("active publication descriptor")
                        .targets
                        .join(","),
                ),
            ),
            Phase::Undispatched => {
                CatalogOutcome::uncommitted(ConnectorMutationFailureKind::Internal, message)
            }
        }
    }
}
pub(crate) struct PreparedObservedPublisher<R> {
    inner: Arc<dyn super::super::prepared::PublicationPreparer>,
    journal: PublicationJournal<R>,
}
impl<R: Clone + Send + Sync + 'static> PreparedObservedPublisher<R> {
    /// The same preparation and one dispatch, stopping before the ready child
    /// Drop. Concrete original adapters receive their facts at this cut.
    pub(crate) async fn dispatch_to_finalization(
        &self,
        request: FrozenRequest,
    ) -> Result<PendingPublicationFinalization<R>> {
        let preparation = self
            .journal
            .current_checked()
            .and_then(|checked| checked.preparation)
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    "Publication has no admitted preparation context",
                )
            })?;
        let inner = Arc::clone(&self.inner);
        let policy = inner.evidence_policy();
        self.journal
            .publish_once_to_finalization(
                preparation,
                policy,
                move |descriptor, generation, preparation| {
                    inner.prepare_dispatch(request, descriptor, generation, preparation)
                },
            )
            .await
    }
    async fn finish_pending(
        &self,
        pending: PendingPublicationFinalization<R>,
    ) -> CatalogOutcome<CommitProof> {
        let policy = pending.evidence_policy();
        match pending.finish(policy).await {
            Ok(entry) => self.journal.commit_outcome(entry),
            Err(_) => self.journal.invocation_error("Duplicate publication invocation refused; the original journal remains authoritative"),
        }
    }
}

impl PendingPublicationFinalization<novarocks_spi::connector::ConnectorCatalogMutationReceipt> {
    /// This concrete catalog adapter stores only actual Applied. A NoOp may
    /// complete successfully, but cannot expose a FE continuation capability.
    pub(crate) fn receive_catalog_applied(
        &self,
        receiving: &novarocks_spi::connector::ConnectorCatalogMutationReceiving,
        original: &ConnectorRequestContext,
    ) -> std::result::Result<(), ConnectorError> {
        if let ExternalMutationOutcome::KnownCommitted {
            effect: ExternalMutationEffect::Applied,
            receipt,
            ..
        } = self.journal.catalog_entry_outcome(self.entry)
        {
            receiving.record_applied_receipt(&receipt)?;
            let context = self
                .journal
                .finalization_context(self.entry, self.policy)
                .and_then(|grant| grant.for_io(original));
            receiving.record_finalization_context(&receipt, context)?;
        }
        Ok(())
    }
}

impl PublicationJournal<novarocks_spi::connector::ConnectorCatalogMutationReceipt> {
    pub(crate) fn observe_catalog_prepared<'a>(
        &self,
        inner: Arc<dyn super::super::prepared::PublicationPreparer>,
        receiving: &'a novarocks_spi::connector::ConnectorCatalogMutationReceiving,
        context: &'a ConnectorRequestContext,
    ) -> impl Publisher + 'a {
        CatalogReceivingPublisher {
            inner: self.observe_prepared(inner),
            receiving,
            context,
        }
    }
}
struct CatalogReceivingPublisher<'a> {
    inner: PreparedObservedPublisher<novarocks_spi::connector::ConnectorCatalogMutationReceipt>,
    receiving: &'a novarocks_spi::connector::ConnectorCatalogMutationReceiving,
    context: &'a ConnectorRequestContext,
}
#[async_trait]
impl Publisher for CatalogReceivingPublisher<'_> {
    async fn load_target(&self, attempt: &IcebergCommitAttempt) -> Result<StagingBase> {
        self.inner.load_target(attempt).await
    }
    async fn load_target_with_snapshots(
        &self,
        attempt: &IcebergCommitAttempt,
        mode: crate::catalog::SnapshotLoading,
    ) -> Result<StagingBase> {
        self.inner.load_target_with_snapshots(attempt, mode).await
    }
    fn pinned_expiry_base(&self) -> Option<StagingBase> {
        self.inner.pinned_expiry_base()
    }
    fn preflight_recovery(
        &self,
        request: &FrozenRequest,
        operation: &IcebergCommitOperation,
    ) -> Result<()> {
        self.inner.preflight_recovery(request, operation)
    }
    fn preflight_recovery_in<'a>(
        &'a self,
        request: &'a mut Option<FrozenRequest>,
        operation: &'a IcebergCommitOperation,
    ) -> crate::commit::attempt::RecoveryPreflightFuture<'a> {
        self.inner.preflight_recovery_in(request, operation)
    }
    async fn dispatch_once(&self, request: FrozenRequest) -> CatalogOutcome<CommitProof> {
        let pending = match self.inner.dispatch_to_finalization(request).await {
            Ok(pending) => pending,
            Err(_) => return self.inner.journal.invocation_error("Original catalog publication invocation refused; the journal remains authoritative"),
        };
        let entry = pending.entry();
        let receiving_failure = pending
            .receive_catalog_applied(self.receiving, self.context)
            .err();
        let outcome = self.inner.finish_pending(pending).await;
        if let Some(failure) = receiving_failure {
            // A receiving refusal cannot erase the real publication or detach
            // its ready child. Keep its typed cause as necessary finalization.
            if !matches!(
                &outcome,
                CatalogOutcome::KnownCommitted {
                    finalization: ExternalMutationFinalization::Failed(_),
                    ..
                }
            ) {
                self.inner.journal.record_entry_finalization(
                    entry,
                    ExternalMutationFinalization::Failed(ConnectorMutationFailure::from_connector(
                        failure,
                    )),
                );
            }
            return self.inner.journal.commit_outcome(entry);
        }
        outcome
    }
}

#[async_trait]
impl<R: Clone + Send + Sync + 'static> Publisher for PreparedObservedPublisher<R> {
    async fn load_target_with_snapshots(
        &self,
        attempt: &IcebergCommitAttempt,
        mode: crate::catalog::SnapshotLoading,
    ) -> Result<StagingBase> {
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
        if self
            .journal
            .current_checked()
            .and_then(|checked| checked.preparation)
            .is_none()
        {
            return self
                .journal
                .invocation_error("Publication has no admitted preparation context");
        }
        match self.dispatch_to_finalization(request).await {
            Ok(pending) => self.finish_pending(pending).await,
            Err(_) => self.journal.invocation_error("Duplicate publication invocation refused; the original journal remains authoritative"),
        }
    }
}

impl<R> DispatchClaim<R> {
    fn bind(&self, binding: Option<RecoveryBinding>) -> Result<()> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let entry = state
            .entries
            .get_mut(&self.key)
            .expect("claimed entry exists");
        if !matches!(entry.phase, EntryPhase::Claimed) || entry.checked.descriptor.binding.is_some()
        {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Publication binding requires its unconsumed claim",
            ));
        }
        if let Some(binding) = &binding {
            binding.validate_generation(entry.checked.evidence.incarnation().to_bytes())?;
        }
        let mut candidate = entry.checked.descriptor.clone();
        candidate.binding = binding;
        candidate.validate()?;
        entry.checked.descriptor = candidate;
        Ok(())
    }

    pub(super) fn refuse(mut self, failure: ConnectorMutationFailure) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let entry = state
            .entries
            .get_mut(&self.key)
            .expect("claimed entry exists");
        assert!(
            matches!(entry.phase, EntryPhase::Claimed),
            "Only preparation can refuse a claim"
        );
        entry.phase = EntryPhase::Concluded(DispatchEvidence::NotDispatched { failure });
        self.completed = true;
    }
}

impl<R: Clone + Send + Sync + 'static> PublicationJournal<R> {
    /// Actual Box pointee of the original observed preflight wrapper. The
    /// inner producer's actual Box pointee is quoted by that producer.
    pub(crate) fn prepared_preflight_future_bytes() -> usize {
        crate::commit::attempt::RecoveryPreflightFuture::binding_future_bytes::<R>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::publication::EvidenceContext;
    use crate::catalog::publication_dispatch::{
        DispatchCompletion, FinalizationWork, PreparationFailure, PreparedCatalogDispatch,
    };
    use crate::commit::publication_descriptor::RecoveryPrincipalScope;
    use novarocks_spi::connector::{
        ConnectorInstanceDescriptor, ConnectorMutationOperationId, ProviderBindingEpoch,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::sync::Notify;

    struct DelayedDispatch {
        entered: Arc<Notify>,
        exit: Arc<Notify>,
        dispatches: Arc<AtomicUsize>,
        journal: PublicationJournal<String>,
    }
    #[async_trait]
    impl PreparedCatalogDispatch for DelayedDispatch {
        async fn dispatch_once(self: Box<Self>, _: EvidenceContext) -> DispatchCompletion {
            self.dispatches.fetch_add(1, Ordering::SeqCst);
            self.entered.notify_one();
            self.exit.notified().await;
            DispatchCompletion {
                evidence: DispatchEvidence::Applied {
                    proof: CommitProof::applied(None),
                },
                finalization: Some(Box::new(CheckFinalization(self.journal.clone()))),
            }
        }
    }
    struct CheckFinalization(PublicationJournal<String>);
    #[async_trait]
    impl FinalizationWork for CheckFinalization {
        async fn run(self: Box<Self>, _: EvidenceContext) -> ExternalMutationFinalization {
            assert!(
                matches!(self.0.phase(), Phase::Committed(_, _, _)),
                "Applied must precede finalization's first poll"
            );
            ExternalMutationFinalization::Failed(ConnectorMutationFailure::new(
                ConnectorMutationFailureKind::Internal,
                "injected independent finalization failure",
            ))
        }
    }
    fn binding(generation: [u8; 16]) -> RecoveryBinding {
        RecoveryBinding {
            generation,
            method: "POST".into(),
            endpoint: "https://catalog.example/v1/namespaces".into(),
            warehouse: None,
            query: vec![],
            principal_scope: RecoveryPrincipalScope::CatalogGeneration { generation },
            deduplication_domain: "exact-test-domain".into(),
            lifetime: None,
        }
    }

    #[tokio::test]
    async fn duplicate_call_and_statement_stop_do_not_replace_the_live_requests_evidence() {
        let (journal, preparation) = super::super::tests::catalog_journal();
        let stop = novarocks_spi::connector::ConnectorStopOwner::new();
        let preparation = preparation.with_additional_stop(stop.view());
        let entered = Arc::new(Notify::new());
        let exit = Arc::new(Notify::new());
        let dispatches = Arc::new(AtomicUsize::new(0));
        let task_journal = journal.clone();
        let task_entered = entered.clone();
        let task_exit = exit.clone();
        let task_dispatches = dispatches.clone();
        let duplicate_context = preparation.clone();
        let task = tokio::spawn(async move {
            let finalization_journal = task_journal.clone();
            task_journal
                .publish_once(
                    preparation,
                    CatalogEvidencePolicy::default(),
                    move |_, generation, _| async move {
                        Ok(PreparedPublication::Mutation {
                            binding: Some(binding(generation.to_bytes())),
                            dispatch: Box::new(DelayedDispatch {
                                entered: task_entered,
                                exit: task_exit,
                                dispatches: task_dispatches,
                                journal: finalization_journal,
                            }),
                        })
                    },
                )
                .await
        });
        entered.notified().await;
        let duplicate = journal
            .publish_once(
                duplicate_context,
                CatalogEvidencePolicy::default(),
                |_, _, _| async {
                    panic!("duplicate must fail before polling preparation");
                    #[allow(unreachable_code)]
                    Ok::<PreparedPublication, PreparationFailure>(unreachable!())
                },
            )
            .await;
        assert!(duplicate.is_err());
        assert!(matches!(journal.phase(), Phase::Issued(_)));
        assert!(journal.descriptor().unwrap().first_dispatch_ms.is_some());
        stop.request_stop();
        tokio::task::yield_now().await;
        assert!(
            !task.is_finished(),
            "the publication owner must await actual adapter exit"
        );
        exit.notify_one();
        let entry = task.await.unwrap().unwrap();
        assert_eq!(dispatches.load(Ordering::SeqCst), 1);
        assert!(matches!(
            journal.commit_outcome(entry),
            CatalogOutcome::KnownCommitted {
                finalization: ExternalMutationFinalization::Failed(_),
                ..
            }
        ));
        assert!(matches!(
            journal.catalog_entry_outcome(entry),
            ExternalMutationOutcome::KnownCommitted { .. }
        ));
    }

    #[tokio::test]
    async fn foreign_generation_is_refused_without_partial_binding_or_dispatch() {
        let (journal, preparation) = super::super::tests::catalog_journal();
        let dispatches = Arc::new(AtomicUsize::new(0));
        let counter = dispatches.clone();
        let observed = journal.clone();
        let entry = journal
            .publish_once(
                preparation,
                CatalogEvidencePolicy::default(),
                move |_, _, _| async move {
                    Ok(PreparedPublication::Mutation {
                        binding: Some(binding([9; 16])),
                        dispatch: Box::new(DelayedDispatch {
                            entered: Arc::new(Notify::new()),
                            exit: Arc::new(Notify::new()),
                            dispatches: counter,
                            journal: observed,
                        }),
                    })
                },
            )
            .await
            .unwrap();
        assert_eq!(dispatches.load(Ordering::SeqCst), 0);
        let descriptor = journal.entry_descriptor(entry).unwrap();
        assert!(descriptor.binding.is_none());
        assert!(descriptor.first_dispatch_ms.is_none());
        assert!(matches!(
            journal.commit_outcome(entry),
            CatalogOutcome::KnownUncommitted { .. }
        ));
        assert!(matches!(
            journal.state.lock().unwrap().entries[&entry.0].phase,
            EntryPhase::Concluded(DispatchEvidence::NotDispatched { .. })
        ));
    }

    #[tokio::test]
    async fn preparation_waits_for_started_work_to_exit_and_preserves_cancellation_kind() {
        let (journal, preparation) = super::super::tests::catalog_journal();
        let stop = novarocks_spi::connector::ConnectorStopOwner::new();
        let preparation = preparation.with_additional_stop(stop.view());
        let entered = Arc::new(Notify::new());
        let exit = Arc::new(Notify::new());
        let observed = journal.clone();
        let work_entered = entered.clone();
        let work_exit = exit.clone();
        let task = tokio::spawn(async move {
            observed
                .publish_once(
                    preparation,
                    CatalogEvidencePolicy::default(),
                    move |_, _, preparation| async move {
                        work_entered.notify_one();
                        preparation.stop().stopped().await;
                        work_exit.notified().await;
                        Err(PreparationFailure(ConnectorMutationFailure::new(
                            ConnectorMutationFailureKind::Cancelled,
                            "cancelled after actual preparation work exit",
                        )))
                    },
                )
                .await
        });
        entered.notified().await;
        stop.request_stop();
        tokio::task::yield_now().await;
        assert!(!task.is_finished());
        exit.notify_one();
        let entry = task.await.unwrap().unwrap();
        let CatalogOutcome::KnownUncommitted { failure } = journal.commit_outcome(entry) else {
            panic!("preparation cannot dispatch")
        };
        assert_eq!(failure.kind(), ConnectorMutationFailureKind::Cancelled);
        assert!(
            journal
                .entry_descriptor(entry)
                .unwrap()
                .first_dispatch_ms
                .is_none()
        );
    }
    #[tokio::test]
    async fn local_noop_concludes_without_a_dispatch_timestamp_or_binding() {
        let (journal, preparation) = super::super::tests::catalog_journal();
        let entry = journal
            .publish_once(
                preparation,
                CatalogEvidencePolicy::default(),
                |_, _, _| async { Ok(PreparedPublication::NoOp) },
            )
            .await
            .unwrap();
        assert!(matches!(
            journal.commit_outcome(entry),
            CatalogOutcome::KnownCommitted {
                effect: novarocks_spi::connector::ExternalMutationEffect::NoOp,
                ..
            }
        ));
        let descriptor = journal.entry_descriptor(entry).unwrap();
        assert!(descriptor.first_dispatch_ms.is_none());
        assert!(descriptor.binding.is_none());
    }
    #[tokio::test]
    async fn an_unknown_entry_blocks_every_preallocated_frontier() {
        let (stage, preparation) = super::super::tests::catalog_journal();
        stage
            .publish_once(
                preparation.clone(),
                CatalogEvidencePolicy::default(),
                |_, _, _| async { Ok(PreparedPublication::NoOp) },
            )
            .await
            .unwrap();
        let first = stage.next_frontier().unwrap();
        let second = stage.next_frontier().unwrap();
        let checked = stage.current_checked().unwrap();
        first
            .prepare_catalog(
                checked.evidence.clone(),
                "final seed".into(),
                vec!["db.t".into()],
                b"first".to_vec(),
                crate::commit::publication_descriptor::AttributionPlan::ObserveOnly,
                preparation.clone(),
            )
            .unwrap();
        let claim = first.claim_dispatch().unwrap();
        claim
            .begin_dispatch(&preparation, &MemoryDispatchBarrier)
            .await
            .unwrap();
        first.conclude_entry(
            first.current_entry().unwrap(),
            DispatchEvidence::Unknown {
                failure: ConnectorMutationFailure::new(
                    ConnectorMutationFailureKind::Unavailable,
                    "injected unknown",
                ),
                observation: Default::default(),
            },
        );
        for frontier in [&first, &second, &stage] {
            assert!(
                frontier
                    .prepare_catalog(
                        checked.evidence.clone(),
                        "forbidden seed".into(),
                        vec!["db.t".into()],
                        b"again".to_vec(),
                        crate::commit::publication_descriptor::AttributionPlan::ObserveOnly,
                        preparation.clone()
                    )
                    .is_err()
            );
            assert!(frontier.next_frontier().is_err());
        }
        assert_eq!(stage.state.lock().unwrap().entries.len(), 2);
        assert!(second.current_entry().is_none());
    }

    struct ActivityDispatch {
        entered: Arc<Notify>,
        exit: Arc<Notify>,
        fin_entered: Arc<Notify>,
        fin_exit: Arc<Notify>,
    }
    struct ActivityFinalization {
        entered: Arc<Notify>,
        exit: Arc<Notify>,
    }
    #[async_trait]
    impl PreparedCatalogDispatch for ActivityDispatch {
        async fn dispatch_once(self: Box<Self>, _: EvidenceContext) -> DispatchCompletion {
            self.entered.notify_one();
            self.exit.notified().await;
            DispatchCompletion {
                evidence: DispatchEvidence::Applied {
                    proof: CommitProof::applied(None),
                },
                finalization: Some(Box::new(ActivityFinalization {
                    entered: self.fin_entered,
                    exit: self.fin_exit,
                })),
            }
        }
    }
    #[async_trait]
    impl FinalizationWork for ActivityFinalization {
        async fn run(self: Box<Self>, _: EvidenceContext) -> ExternalMutationFinalization {
            self.entered.notify_one();
            self.exit.notified().await;
            ExternalMutationFinalization::Complete
        }
    }
    fn activity_scope() -> Arc<crate::activity::CatalogActivity> {
        crate::activity::CatalogActivity::new(novarocks_spi::connector::CatalogScopeIdentity::new(
            novarocks_spi::connector::ConnectorProviderId::parse("iceberg").unwrap(),
            novarocks_spi::connector::ConnectorInstanceId::parse("catalog.activity").unwrap(),
            Uuid::now_v7(),
        ))
    }
    #[tokio::test]
    async fn activity_tracks_actual_evidence_exit_then_independent_finalization() {
        use novarocks_spi::connector::{CatalogActivityPhase as P, CatalogActivityReader};
        let (mut journal, preparation) = super::super::tests::catalog_journal();
        let root = activity_scope();
        journal.activity = Some(root.generation());
        let stop = novarocks_spi::connector::ConnectorStopOwner::new();
        let preparation = preparation.with_additional_stop(stop.view());
        let entered = Arc::new(Notify::new());
        let exit = Arc::new(Notify::new());
        let fin_entered = Arc::new(Notify::new());
        let fin_exit = Arc::new(Notify::new());
        let dispatch = ActivityDispatch {
            entered: entered.clone(),
            exit: exit.clone(),
            fin_entered: fin_entered.clone(),
            fin_exit: fin_exit.clone(),
        };
        let active = journal.clone();
        let duplicate_preparation = preparation.clone();
        let task = tokio::spawn(async move {
            active
                .publish_once(
                    preparation,
                    CatalogEvidencePolicy::default(),
                    |_, _, _| async {
                        Ok(PreparedPublication::Mutation {
                            binding: None,
                            dispatch: Box::new(dispatch),
                        })
                    },
                )
                .await
        });
        entered.notified().await;
        stop.request_stop();
        let snapshot = root.snapshot();
        assert_eq!(snapshot.phases[P::PreDispatch as usize].inflight, 0);
        assert_eq!(snapshot.phases[P::Dispatch as usize].inflight, 1);
        assert_eq!(snapshot.phases[P::Evidence as usize].inflight, 1);
        assert!(
            journal
                .publish_once(
                    duplicate_preparation,
                    CatalogEvidencePolicy::default(),
                    |_, _, _| async {
                        panic!("duplicate preparation");
                        #[allow(unreachable_code)]
                        Ok(PreparedPublication::NoOp)
                    }
                )
                .await
                .is_err()
        );
        assert_eq!(root.snapshot().phases[P::Dispatch as usize].started, 1);
        exit.notify_one();
        fin_entered.notified().await;
        assert!(matches!(journal.phase(), Phase::Committed(_, _, _)));
        let snapshot = root.snapshot();
        assert_eq!(snapshot.phases[P::Dispatch as usize].inflight, 0);
        assert_eq!(snapshot.phases[P::Evidence as usize].exited, 1);
        assert_eq!(snapshot.phases[P::Finalization as usize].inflight, 1);
        assert!(!task.is_finished());
        fin_exit.notify_one();
        task.await.unwrap().unwrap();
        assert_eq!(root.snapshot().phases[P::Finalization as usize].exited, 1);
        assert_eq!(root.snapshot().events.pre_dispatch_cancelled, 0);
    }
    #[tokio::test]
    async fn activity_noop_and_original_preparation_stop_never_count_dispatch() {
        use novarocks_spi::connector::{CatalogActivityPhase as P, CatalogActivityReader};
        for cancelled in [false, true] {
            let (mut journal, preparation) = super::super::tests::catalog_journal();
            let root = activity_scope();
            journal.activity = Some(root.generation());
            let stop = novarocks_spi::connector::ConnectorStopOwner::new();
            let preparation = preparation.with_additional_stop(stop.view());
            if cancelled {
                stop.request_stop();
            }
            journal
                .publish_once(
                    preparation,
                    CatalogEvidencePolicy::default(),
                    |_, _, _| async { Ok(PreparedPublication::NoOp) },
                )
                .await
                .unwrap();
            let snapshot = root.snapshot();
            assert_eq!(snapshot.phases[P::Dispatch as usize].started, 0);
            assert_eq!(snapshot.phases[P::Evidence as usize].started, 0);
            assert_eq!(snapshot.phases[P::PreDispatch as usize].exited, 1);
            assert_eq!(snapshot.events.pre_dispatch_cancelled, u64::from(cancelled));
        }
    }

    #[tokio::test]
    async fn activity_stop_between_barrier_and_evidence_grant_is_undispatched_and_counted() {
        use novarocks_spi::connector::{CatalogActivityPhase as P, CatalogActivityReader};
        let (mut journal, preparation) = super::super::tests::catalog_journal();
        let root = activity_scope();
        journal.activity = Some(root.generation());
        let stop = novarocks_spi::connector::ConnectorStopOwner::new();
        let preparation = preparation.with_additional_stop(stop.view());
        let claim = journal.claim_dispatch().unwrap();
        let entry = claim
            .begin_dispatch(&preparation, &MemoryDispatchBarrier)
            .await
            .unwrap();
        stop.request_stop();
        assert!(
            journal
                .begin_entry_evidence(entry, &preparation, CatalogEvidencePolicy::default())
                .is_none()
        );
        assert!(
            matches!(journal.commit_outcome(entry), CatalogOutcome::KnownUncommitted { failure } if failure.kind() == ConnectorMutationFailureKind::Cancelled)
        );
        let snapshot = root.snapshot();
        assert_eq!(snapshot.events.pre_dispatch_cancelled, 1);
        assert_eq!(snapshot.phases[P::Dispatch as usize].started, 0);
        assert_eq!(snapshot.phases[P::Evidence as usize].started, 0);
        assert!(
            journal
                .entry_descriptor(entry)
                .unwrap()
                .first_dispatch_ms
                .is_none()
        );
    }

    struct RealRestActivityDispatch(crate::rest_protocol::mutation::prepared::PreparedRestMutation);
    #[async_trait]
    impl PreparedCatalogDispatch for RealRestActivityDispatch {
        async fn dispatch_once(self: Box<Self>, context: EvidenceContext) -> DispatchCompletion {
            let observed = match self.0.dispatch(context).await {
                Ok(observed) => observed.dispatch_evidence(),
                Err(original) => DispatchEvidence::NotDispatched {
                    failure: novarocks_spi::connector::ConnectorMutationFailure::from_connector(
                        original,
                    ),
                },
            };
            DispatchCompletion::evidence(observed)
        }
    }
    #[tokio::test]
    async fn activity_real_blackhole_keeps_evidence_after_stop_and_counts_only_explicit_expiry() {
        use crate::rest_protocol::mutation::prepared::{MutationRoute, PreparedRestMutation};
        use crate::rest_protocol::test_server::{Reply, Script, TestServer};
        use novarocks_spi::connector::{CatalogActivityPhase as P, CatalogActivityReader};
        let server = TestServer::start(
            Script::new()
                .on(
                    "GET",
                    "/v1/config",
                    Reply::json(200, &serde_json::json!({"defaults":{},"overrides":{}})),
                )
                .on("POST", "/v1/namespaces/db/tables/t", Reply::NeverAnswer),
        );
        let endpoints = crate::catalog_runtime::build_vended_rest_generation_for_test(
            std::collections::HashMap::from([("uri".into(), server.url())]),
            None,
            crate::iceberg::Runtime::try_current().unwrap(),
        )
        .await
        .unwrap();
        let (mut journal, preparation) = super::super::tests::catalog_journal();
        let root = activity_scope();
        journal.activity = Some(root.generation());
        endpoints
            .publication_generation
            .bind(journal.current_checked().unwrap().evidence.incarnation())
            .unwrap();
        let stop = novarocks_spi::connector::ConnectorStopOwner::new();
        let preparation = preparation.with_additional_stop(stop.view());
        let active = journal.clone();
        let task = tokio::spawn(async move {
            active
                .publish_once(
                    preparation,
                    CatalogEvidencePolicy {
                        evidence: std::time::Duration::from_secs(2),
                        ..Default::default()
                    },
                    move |descriptor, generation, preparation| async move {
                        let prepared = PreparedRestMutation::prepare(
                            endpoints,
                            MutationRoute::CommitTable(
                                crate::iceberg::TableIdent::from_strs(["db", "t"]).unwrap(),
                            ),
                            &descriptor,
                            generation,
                            &preparation,
                            crate::rest_protocol::evidence::AttributionExpectation::table_commit(
                                Uuid::nil(),
                                None,
                            )
                            .unwrap(),
                            false,
                            None,
                        )
                        .await?;
                        Ok(PreparedPublication::Mutation {
                            binding: Some(prepared.binding().clone()),
                            dispatch: Box::new(RealRestActivityDispatch(prepared)),
                        })
                    },
                )
                .await
        });
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while server.count("POST", "/v1/namespaces/db/tables/t") == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("real mutation arrived");
        stop.request_stop();
        tokio::task::yield_now().await;
        assert_eq!(root.snapshot().phases[P::Dispatch as usize].inflight, 1);
        assert_eq!(root.snapshot().phases[P::Evidence as usize].inflight, 1);
        assert!(!task.is_finished());
        let entry = tokio::time::timeout(std::time::Duration::from_secs(5), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(matches!(
            journal.commit_outcome(entry),
            CatalogOutcome::CommitUnknown { .. }
        ));
        let snapshot = root.snapshot();
        assert_eq!(snapshot.phases[P::Evidence as usize].inflight, 0);
        assert_eq!(snapshot.phases[P::Evidence as usize].exited, 1);
        assert_eq!(snapshot.events.evidence_expired, 1);
        assert_eq!(snapshot.events.pre_dispatch_cancelled, 0);
        assert_eq!(server.count("POST", "/v1/namespaces/db/tables/t"), 1);
    }

    struct CapturedGrantDispatch {
        journal: PublicationJournal<String>,
        grants: Arc<std::sync::Mutex<Vec<EvidenceContext>>>,
    }
    struct CapturedGrantFinalization(CapturedGrantDispatch);
    #[async_trait]
    impl PreparedCatalogDispatch for CapturedGrantDispatch {
        async fn dispatch_once(self: Box<Self>, context: EvidenceContext) -> DispatchCompletion {
            self.grants.lock().unwrap().push(context);
            DispatchCompletion {
                evidence: DispatchEvidence::Applied {
                    proof: CommitProof::applied(None),
                },
                finalization: Some(Box::new(CapturedGrantFinalization(*self))),
            }
        }
    }
    #[async_trait]
    impl FinalizationWork for CapturedGrantFinalization {
        async fn run(self: Box<Self>, context: EvidenceContext) -> ExternalMutationFinalization {
            assert!(matches!(self.0.journal.phase(), Phase::Committed(_, _, _)));
            self.0.grants.lock().unwrap().push(context);
            ExternalMutationFinalization::Complete
        }
    }

    #[tokio::test]
    async fn nested_publication_cannot_extend_the_original_positive_finalization() {
        let (parent, preparation) = super::super::tests::catalog_journal();
        let business_stop = novarocks_spi::connector::ConnectorStopOwner::new();
        let preparation = preparation.with_additional_stop(business_stop.view());
        let parent_grants = Arc::new(std::sync::Mutex::new(Vec::new()));
        let dispatch = CapturedGrantDispatch {
            journal: parent.clone(),
            grants: parent_grants,
        };
        let parent_entry = parent
            .publish_once(
                preparation.clone(),
                CatalogEvidencePolicy::default(),
                move |_, _, _| async move {
                    Ok(PreparedPublication::Mutation {
                        binding: None,
                        dispatch: Box::new(dispatch),
                    })
                },
            )
            .await
            .unwrap();
        let grant = parent
            .finalization_context(parent_entry, CatalogEvidencePolicy::default())
            .unwrap();
        let necessary = grant.for_io(&preparation).unwrap();
        business_stop.request_stop();
        assert!(necessary.check_active().is_ok());

        let (nested, _) = super::super::tests::catalog_journal();
        let grants = Arc::new(std::sync::Mutex::new(Vec::new()));
        let dispatch = CapturedGrantDispatch {
            journal: nested.clone(),
            grants: grants.clone(),
        };
        let longer = CatalogEvidencePolicy {
            evidence: std::time::Duration::from_secs(600),
            finalization: std::time::Duration::from_secs(600),
            ..Default::default()
        };
        let cap = grant.deadline();
        let entry = nested
            .publish_once(necessary, longer, move |_, _, context| async move {
                assert_eq!(
                    context.deadline(),
                    cap,
                    "preparation uses the same upper bound"
                );
                Ok(PreparedPublication::Mutation {
                    binding: None,
                    dispatch: Box::new(dispatch),
                })
            })
            .await
            .unwrap();
        let observed = grants.lock().unwrap();
        assert_eq!(
            observed.len(),
            2,
            "one dispatch and one actual finalization"
        );
        assert_eq!(observed[0].deadline(), cap);
        assert_eq!(observed[1].deadline(), cap);
        let cached = nested.finalization_context(entry, longer).unwrap();
        assert_eq!(cached.started(), observed[1].started());
        assert_eq!(cached.deadline(), cap);
        assert!(matches!(
            nested.commit_outcome(entry),
            CatalogOutcome::KnownCommitted {
                effect: ExternalMutationEffect::Applied,
                finalization: ExternalMutationFinalization::Complete,
                ..
            }
        ));
    }

    #[tokio::test]
    async fn expired_parent_cap_refuses_nested_preparation_even_from_cleanup_projection() {
        let (journal, original) = super::super::tests::catalog_journal();
        let cap = std::time::Instant::now() - std::time::Duration::from_secs(1);
        let necessary = original.with_necessary_finalization_deadline_cap(cap);
        let cleanup = CatalogEvidencePolicy::default()
            .begin_reclamation()
            .unwrap()
            .for_io(&necessary)
            .unwrap();
        assert!(
            cleanup.check_active().is_ok(),
            "cleanup retains its independent budget"
        );
        let entry = journal
            .publish_once(cleanup, CatalogEvidencePolicy::default(), |_, _, _| async {
                panic!("expired parent cap must refuse before preparation I/O");
                #[allow(unreachable_code)]
                Ok(PreparedPublication::NoOp)
            })
            .await
            .unwrap();
        assert!(
            matches!(journal.commit_outcome(entry), CatalogOutcome::KnownUncommitted { failure }
            if failure.kind() == ConnectorMutationFailureKind::DeadlineExceeded)
        );
        assert!(
            journal
                .entry_descriptor(entry)
                .unwrap()
                .first_dispatch_ms
                .is_none()
        );
    }

    struct BudgetDispatch {
        entered: Arc<Notify>,
        exit: Arc<Notify>,
        grant: Arc<std::sync::Mutex<Option<EvidenceContext>>>,
    }
    struct UnexpectedFinalization(Arc<AtomicUsize>);
    #[async_trait]
    impl FinalizationWork for UnexpectedFinalization {
        async fn run(self: Box<Self>, _: EvidenceContext) -> ExternalMutationFinalization {
            self.0.fetch_add(1, Ordering::SeqCst);
            ExternalMutationFinalization::Complete
        }
    }
    struct NonAppliedDispatch {
        evidence: DispatchEvidence,
        polls: Arc<AtomicUsize>,
    }
    #[async_trait]
    impl PreparedCatalogDispatch for NonAppliedDispatch {
        async fn dispatch_once(self: Box<Self>, _: EvidenceContext) -> DispatchCompletion {
            DispatchCompletion {
                evidence: self.evidence,
                finalization: Some(Box::new(UnexpectedFinalization(self.polls))),
            }
        }
    }
    #[tokio::test]
    async fn finalization_requires_positive_evidence_and_never_polls_non_applied_work() {
        let failure = ConnectorMutationFailure::new(
            ConnectorMutationFailureKind::Unavailable,
            "injected non-applied evidence",
        );
        for evidence in [
            DispatchEvidence::Unknown {
                failure: failure.clone(),
                observation: Default::default(),
            },
            DispatchEvidence::Rejected {
                failure: failure.clone(),
                observation: Default::default(),
            },
            DispatchEvidence::NotDispatched {
                failure: failure.clone(),
            },
        ] {
            let (journal, preparation) = super::super::tests::catalog_journal();
            let entry = journal.current_entry().unwrap();
            assert_eq!(
                journal
                    .finalization_context(entry, CatalogEvidencePolicy::default())
                    .unwrap_err()
                    .kind(),
                novarocks_spi::connector::ConnectorErrorKind::InvalidRequest,
            );
            let polls = Arc::new(AtomicUsize::new(0));
            let dispatch = NonAppliedDispatch {
                evidence,
                polls: polls.clone(),
            };
            journal
                .publish_once(
                    preparation,
                    CatalogEvidencePolicy::default(),
                    move |_, _, _| async move {
                        Ok(PreparedPublication::Mutation {
                            binding: None,
                            dispatch: Box::new(dispatch),
                        })
                    },
                )
                .await
                .unwrap();
            assert_eq!(polls.load(Ordering::SeqCst), 0);
            assert!(
                journal.state.lock().unwrap().entries[&entry.0]
                    .finalization_grant
                    .is_none()
            );
            assert_eq!(
                journal
                    .finalization_context(entry, CatalogEvidencePolicy::default())
                    .unwrap_err()
                    .kind(),
                novarocks_spi::connector::ConnectorErrorKind::InvalidRequest,
            );
        }
    }
    #[tokio::test]
    async fn repeated_finalization_requests_reuse_the_exact_grant() {
        let (journal, preparation) = super::super::tests::catalog_journal();
        let entry = journal
            .publish_once(
                preparation,
                CatalogEvidencePolicy::default(),
                |_, _, _| async { Ok(PreparedPublication::NoOp) },
            )
            .await
            .unwrap();
        let first = journal
            .finalization_context(entry, CatalogEvidencePolicy::default())
            .unwrap();
        let second = journal
            .finalization_context(
                entry,
                CatalogEvidencePolicy {
                    finalization: std::time::Duration::from_secs(60),
                    ..Default::default()
                },
            )
            .unwrap();
        assert_eq!(first.started(), second.started());
        assert_eq!(first.deadline(), second.deadline());
        let (foreign, _) = super::super::tests::catalog_journal();
        assert!(
            foreign
                .finalization_context(entry, CatalogEvidencePolicy::default())
                .is_err()
        );
    }
    #[async_trait]
    impl PreparedCatalogDispatch for BudgetDispatch {
        async fn dispatch_once(self: Box<Self>, _: EvidenceContext) -> DispatchCompletion {
            DispatchCompletion {
                evidence: DispatchEvidence::Applied {
                    proof: CommitProof::applied(None),
                },
                finalization: Some(Box::new(BudgetFinalization {
                    entered: self.entered,
                    exit: self.exit,
                    grant: self.grant,
                })),
            }
        }
    }
    struct BudgetFinalization {
        entered: Arc<Notify>,
        exit: Arc<Notify>,
        grant: Arc<std::sync::Mutex<Option<EvidenceContext>>>,
    }
    #[async_trait]
    impl FinalizationWork for BudgetFinalization {
        async fn run(self: Box<Self>, context: EvidenceContext) -> ExternalMutationFinalization {
            *self.grant.lock().unwrap() = Some(context);
            self.entered.notify_one();
            // Simulate already started local work: retain its owner to actual
            // exit even when no further finalization I/O may be started.
            self.exit.notified().await;
            assert!(context.expired());
            ExternalMutationFinalization::Failed(ConnectorMutationFailure::new(
                ConnectorMutationFailureKind::DeadlineExceeded,
                "injected finalization completed after its phase deadline",
            ))
        }
    }
    #[tokio::test]
    async fn producer_projection_cannot_restart_an_adapters_finalization_budget() {
        let (journal, preparation) = super::super::tests::catalog_journal();
        let entered = Arc::new(Notify::new());
        let exit = Arc::new(Notify::new());
        let grant = Arc::new(std::sync::Mutex::new(None));
        let policy = CatalogEvidencePolicy {
            finalization: std::time::Duration::from_millis(5),
            ..Default::default()
        };
        let observed = journal.clone();
        let mechanism = BudgetDispatch {
            entered: entered.clone(),
            exit: exit.clone(),
            grant: grant.clone(),
        };
        let task = tokio::spawn(async move {
            observed
                .publish_once(preparation, policy, move |_, _, _| async move {
                    Ok(PreparedPublication::Mutation {
                        binding: None,
                        dispatch: Box::new(mechanism),
                    })
                })
                .await
        });
        tokio::time::timeout(std::time::Duration::from_secs(1), entered.notified())
            .await
            .expect("finalization must enter its controlled work before the readiness bound");
        let original = grant.lock().unwrap().unwrap();
        tokio::time::sleep_until(
            (original.deadline() + std::time::Duration::from_millis(5)).into(),
        )
        .await;
        let entry = journal.current_entry().unwrap();
        assert!(matches!(
            journal.commit_outcome(entry),
            CatalogOutcome::KnownCommitted { .. }
        ));
        assert!(
            !task.is_finished(),
            "finalization still owns its started local work"
        );
        let error = journal
            .finalization_context(
                entry,
                CatalogEvidencePolicy {
                    finalization: std::time::Duration::from_secs(60),
                    ..Default::default()
                },
            )
            .unwrap_err();
        assert_eq!(
            error.kind(),
            novarocks_spi::connector::ConnectorErrorKind::DeadlineExceeded
        );
        exit.notify_one();
        task.await.unwrap().unwrap();
        assert!(matches!(
            journal.commit_outcome(entry),
            CatalogOutcome::KnownCommitted {
                finalization: ExternalMutationFinalization::Failed(_),
                ..
            }
        ));
    }
    struct CutDispatch {
        child_drops: Arc<AtomicUsize>,
        finalizations: Arc<AtomicUsize>,
        panic_on_drop: bool,
    }
    struct CutChild {
        completion: Option<DispatchCompletion>,
        drops: Arc<AtomicUsize>,
        panic_on_drop: bool,
    }
    impl Future for CutChild {
        type Output = DispatchCompletion;
        fn poll(
            mut self: std::pin::Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Self::Output> {
            std::task::Poll::Ready(self.completion.take().expect("one actual dispatch poll"))
        }
    }
    impl Drop for CutChild {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::SeqCst);
            assert!(!self.panic_on_drop, "original dispatch child Drop panic");
        }
    }
    struct CutFinalization(Arc<AtomicUsize>);
    #[async_trait]
    impl FinalizationWork for CutFinalization {
        async fn run(self: Box<Self>, _: EvidenceContext) -> ExternalMutationFinalization {
            self.0.fetch_add(1, Ordering::SeqCst);
            ExternalMutationFinalization::Complete
        }
    }
    impl PreparedCatalogDispatch for CutDispatch {
        fn dispatch_once<'async_trait>(
            self: Box<Self>,
            _: EvidenceContext,
        ) -> std::pin::Pin<Box<dyn Future<Output = DispatchCompletion> + Send + 'async_trait>>
        where
            Self: 'async_trait,
        {
            Box::pin(CutChild {
                completion: Some(DispatchCompletion {
                    evidence: DispatchEvidence::Applied {
                        proof: CommitProof::applied(None),
                    },
                    finalization: Some(Box::new(CutFinalization(self.finalizations.clone()))),
                }),
                drops: self.child_drops.clone(),
                panic_on_drop: self.panic_on_drop,
            })
        }
    }

    struct CatalogCutCapability {
        descriptor: ConnectorInstanceDescriptor,
        epoch: ProviderBindingEpoch,
        journal: PublicationJournal<novarocks_spi::connector::ConnectorCatalogMutationReceipt>,
        child_drops: Arc<AtomicUsize>,
        finalizations: Arc<AtomicUsize>,
        panic_on_drop: bool,
        no_op: bool,
    }
    impl novarocks_spi::connector::ConnectorCatalogMutation for CatalogCutCapability {
        fn descriptor(&self) -> &ConnectorInstanceDescriptor {
            &self.descriptor
        }
        fn incarnation(&self) -> ProviderBindingEpoch {
            self.epoch
        }
        fn execute(
            &self,
            _: novarocks_spi::connector::ConnectorCatalogMutationRequest,
        ) -> std::result::Result<
            ExternalMutationOutcome<novarocks_spi::connector::ConnectorCatalogMutationReceipt>,
            ConnectorError,
        > {
            panic!("the original catalog fixture must not use synchronous execution")
        }
        fn reconcile(
            &self,
            _: novarocks_spi::connector::ConnectorCatalogMutationReconcileRequest,
        ) -> std::result::Result<
            ExternalMutationOutcome<novarocks_spi::connector::ConnectorCatalogMutationReceipt>,
            ConnectorError,
        > {
            panic!("the original catalog fixture must not replay publication")
        }
        fn execute_async<'a>(
            &'a self,
            request: novarocks_spi::connector::ConnectorCatalogMutationRequest,
            receiving: &'a novarocks_spi::connector::ConnectorCatalogMutationReceiving,
        ) -> std::pin::Pin<
            Box<
                dyn Future<
                        Output = std::result::Result<
                            ExternalMutationOutcome<
                                novarocks_spi::connector::ConnectorCatalogMutationReceipt,
                            >,
                            ConnectorError,
                        >,
                    > + Send
                    + 'a,
            >,
        > {
            Box::pin(async move {
                use novarocks_spi::connector::{
                    ConnectorCatalogMutationReceipt, ConnectorProcessLocalPayload,
                };
                let receipt = ConnectorCatalogMutationReceipt::try_new(
                    self.descriptor.clone(),
                    self.epoch,
                    request.operation_id,
                    request.operation.kind(),
                    None,
                )
                .unwrap();
                let evidence = ExternalMutationEvidence::try_new(
                    2,
                    self.descriptor.clone(),
                    self.epoch,
                    request.operation_id,
                    request.operation.kind(),
                    bytes::Bytes::from_static(b"{}"),
                )
                .unwrap();
                self.journal
                    .prepare_catalog(
                        evidence,
                        receipt,
                        vec!["namespace:db".into()],
                        b"drop db".to_vec(),
                        crate::commit::publication_descriptor::AttributionPlan::ObserveOnly,
                        request.context.clone(),
                    )
                    .unwrap();
                receiving.retain_original_owner(ConnectorProcessLocalPayload::new(
                    request.target.clone(),
                    Arc::new(self.journal.clone()),
                ))?;
                let pending = self
                    .journal
                    .publish_once_to_finalization(
                        request.context.clone(),
                        CatalogEvidencePolicy::default(),
                        |_, generation, _| async move {
                            if self.no_op {
                                Ok(PreparedPublication::NoOp)
                            } else {
                                Ok(PreparedPublication::Mutation {
                                    binding: Some(binding(generation.to_bytes())),
                                    dispatch: Box::new(CutDispatch {
                                        child_drops: self.child_drops.clone(),
                                        finalizations: self.finalizations.clone(),
                                        panic_on_drop: self.panic_on_drop,
                                    }),
                                })
                            }
                        },
                    )
                    .await
                    .unwrap();
                let entry = pending.entry();
                // This is the same receiving author used by the Keyed publisher
                // and the existing-object adapter, not a second fact projection.
                pending.receive_catalog_applied(receiving, &request.context)?;
                assert_eq!(self.child_drops.load(Ordering::SeqCst), 0);
                assert_eq!(self.finalizations.load(Ordering::SeqCst), 0);
                {
                    let observed = receiving.observation();
                    if self.no_op {
                        assert!(observed.applied_receipt().is_none());
                        assert!(observed.finalization_context().is_none());
                    } else {
                        let context = observed.finalization_context().unwrap().as_ref().unwrap();
                        assert_eq!(
                            context.deadline(),
                            self.journal
                                .finalization_context(entry, pending.evidence_policy(),)
                                .unwrap()
                                .deadline()
                        );
                        assert!(observed.applied_receipt().is_some());
                        context.request_scope_extension_or_insert_with(|| 593_u64);
                    }
                    assert!(!observed.provider_exited());
                }
                let policy = pending.evidence_policy();
                pending.finish(policy).await.unwrap();
                let outcome = self.journal.catalog_entry_outcome(entry);
                if let ExternalMutationOutcome::KnownCommitted {
                    effect,
                    receipt,
                    finalization,
                    ..
                } = &outcome
                {
                    receiving.record_needed_committed(*effect, receipt, finalization)?;
                }
                Ok(outcome)
            })
        }
    }

    #[tokio::test]
    async fn catalog_applied_receiving_survives_ready_child_drop_and_noop_grants_nothing() {
        use novarocks_spi::connector::{
            ConnectorCatalogMutationLease, ConnectorCatalogMutationOperation,
            ConnectorCatalogMutationReceiving, ConnectorCatalogMutationRequest,
            ConnectorControlRuntimeId, ConnectorNamespaceIdentity, ConnectorProviderBindingKey,
            ConnectorStopOwner, DropPolicy,
        };
        for (no_op, panic_on_drop) in [(false, false), (false, true), (true, false)] {
            let descriptor = ConnectorInstanceDescriptor {
                provider_id: novarocks_spi::connector::ConnectorProviderId::parse("iceberg")
                    .unwrap(),
                instance_id: novarocks_spi::connector::ConnectorInstanceId::parse("catalog-cut")
                    .unwrap(),
            };
            let epoch = ProviderBindingEpoch::from_bytes([2; 16]);
            let operation_id = ConnectorMutationOperationId::new();
            let journal = PublicationJournal::new(OperationToken::from_mutation(operation_id));
            let capability = Arc::new(CatalogCutCapability {
                descriptor: descriptor.clone(),
                epoch,
                journal: journal.clone(),
                child_drops: Arc::new(AtomicUsize::new(0)),
                finalizations: Arc::new(AtomicUsize::new(0)),
                panic_on_drop,
                no_op,
            });
            let lease = ConnectorCatalogMutationLease::new(
                descriptor.clone(),
                ConnectorControlRuntimeId::new(),
                epoch,
                capability.clone(),
                || {},
            )
            .unwrap();
            let context = ConnectorRequestContext::try_new(
                std::time::Instant::now() + std::time::Duration::from_secs(30),
                ConnectorStopOwner::new().view(),
                1024,
                4096,
            )
            .unwrap();
            let request = ConnectorCatalogMutationRequest {
                operation_id,
                target: ConnectorProviderBindingKey {
                    instance_id: descriptor.instance_id.clone(),
                    incarnation: epoch,
                },
                operation: ConnectorCatalogMutationOperation::DropNamespace {
                    namespace: ConnectorNamespaceIdentity {
                        instance_id: descriptor.instance_id.clone(),
                        namespace: "db".into(),
                    },
                    policy: DropPolicy::NoOpIfMissing,
                },
                context,
            };
            let receiving = Arc::new(ConnectorCatalogMutationReceiving::new());
            let original = receiving.clone();
            let joined =
                tokio::spawn(async move { lease.execute_async_in(request, &original).await }).await;
            assert_eq!(
                capability.child_drops.load(Ordering::SeqCst),
                usize::from(!no_op)
            );
            assert_eq!(
                capability.finalizations.load(Ordering::SeqCst),
                usize::from(!no_op && !panic_on_drop)
            );
            let observed = receiving.observation();
            assert!(observed.original_owner().is_some());
            // Unwinding may finish the outer child's destructor, but it never
            // turns the original foreign panic into proven healthy exit.
            assert_eq!(receiving.foreign_exit_unproven(), panic_on_drop);
            assert!(observed.provider_exited());
            if no_op {
                joined.unwrap().unwrap();
                assert!(observed.applied_receipt().is_none());
                assert!(observed.finalization_context().is_none());
                assert_eq!(
                    observed.needed_committed().unwrap().effect(),
                    ExternalMutationEffect::NoOp
                );
            } else {
                let receipt = observed.applied_receipt().unwrap();
                assert_eq!(receipt.operation_id(), operation_id);
                let context = observed.finalization_context().unwrap().as_ref().unwrap();
                let retained = context
                    .request_scope_extension_or_insert_with::<u64>(|| panic!("same request scope"));
                assert_eq!(*retained, 593);
                assert_eq!(
                    context.deadline(),
                    journal
                        .finalization_context(
                            journal.current_entry().unwrap(),
                            CatalogEvidencePolicy::default(),
                        )
                        .unwrap()
                        .deadline()
                );
                if panic_on_drop {
                    let panic = joined.unwrap_err().into_panic();
                    assert_eq!(
                        panic.downcast_ref::<&'static str>(),
                        Some(&"original dispatch child Drop panic")
                    );
                    assert!(observed.raw_outcome().is_none());
                    assert!(observed.needed_committed().is_none());
                } else {
                    joined.unwrap().unwrap();
                    assert!(observed.needed_committed().is_some());
                }
            }
            assert!(
                matches!(journal.catalog_outcome(), ExternalMutationOutcome::KnownCommitted { effect, .. }
                if effect == if no_op { ExternalMutationEffect::NoOp } else { ExternalMutationEffect::Applied })
            );
        }
    }

    #[tokio::test]
    async fn original_dispatch_cut_keeps_canonical_applied_before_child_drop_and_finalization() {
        for panic_on_drop in [false, true] {
            let (journal, preparation) = super::super::tests::catalog_journal();
            let child_drops = Arc::new(AtomicUsize::new(0));
            let finalizations = Arc::new(AtomicUsize::new(0));
            let drops = child_drops.clone();
            let finishes = finalizations.clone();
            let policy = CatalogEvidencePolicy::default();
            let pending = journal
                .publish_once_to_finalization(
                    preparation,
                    policy,
                    move |_, generation, _| async move {
                        Ok(PreparedPublication::Mutation {
                            binding: Some(binding(generation.to_bytes())),
                            dispatch: Box::new(CutDispatch {
                                child_drops: drops,
                                finalizations: finishes,
                                panic_on_drop,
                            }),
                        })
                    },
                )
                .await
                .unwrap();
            let entry = pending.entry();
            assert!(matches!(
                journal.commit_outcome(entry),
                CatalogOutcome::KnownCommitted {
                    effect: ExternalMutationEffect::Applied,
                    ..
                }
            ));
            assert_eq!(child_drops.load(Ordering::SeqCst), 0);
            assert_eq!(finalizations.load(Ordering::SeqCst), 0);
            let grant = journal.finalization_context(entry, policy).unwrap();
            let deadline = grant.deadline();
            assert_eq!(
                journal
                    .finalization_context(entry, policy)
                    .unwrap()
                    .deadline(),
                deadline
            );
            // The original proof is observable before any opaque child destructor.
            let task = tokio::spawn(pending.finish(policy));
            let result = task.await;
            assert_eq!(child_drops.load(Ordering::SeqCst), 1);
            assert!(matches!(
                journal.commit_outcome(entry),
                CatalogOutcome::KnownCommitted {
                    effect: ExternalMutationEffect::Applied,
                    ..
                }
            ));
            if panic_on_drop {
                assert!(result.unwrap_err().is_panic());
                assert_eq!(finalizations.load(Ordering::SeqCst), 0);
            } else {
                assert_eq!(result.unwrap().unwrap(), entry);
                assert_eq!(finalizations.load(Ordering::SeqCst), 1);
                assert_eq!(
                    journal
                        .finalization_context(entry, policy)
                        .unwrap()
                        .deadline(),
                    deadline
                );
            }
        }
    }
}
