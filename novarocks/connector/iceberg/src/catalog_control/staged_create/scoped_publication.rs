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

//! Stable original for the final staged CREATE frontier. Preparation and
//! abort retain their existing entrances until their callers migrate.
use super::*;
use crate::catalog::error::CatalogOutcome;
use crate::catalog::transaction::CommitProof;
use crate::commit::attempt::observation::{
    PreparedObservedPublisher, PublicationJournal, RetainedPublicationProjection,
};
use crate::commit::attempt::{self, Publisher};
use crate::commit::model::FrozenRequest;
use crate::commit::operation::{IcebergCommitAttempt, IcebergCommitOperation};
use crate::commit::staging::StagingBase;
use async_trait::async_trait;
use novarocks_spi::connector::{
    ConnectorStagedCreatePublicationRecorder, ConnectorStagedCreatePublicationSource,
    ExternalMutationOutcome,
};
use std::{
    future::Future,
    mem::ManuallyDrop,
    pin::Pin,
    task::{Context, Poll},
};

type OriginalFuture = Pin<Box<dyn Future<Output = ()> + Send>>;
struct OpaqueOriginal {
    future: ManuallyDrop<OriginalFuture>,
    dropping: bool,
}
impl OpaqueOriginal {
    fn new(future: OriginalFuture) -> Self {
        Self {
            future: ManuallyDrop::new(future),
            dropping: false,
        }
    }
    fn actual_drop(&mut self) {
        assert!(
            !self.dropping,
            "original publication destructor cannot be retried"
        );
        self.dropping = true;
        // If this panics, the same allocation remains in its original slot.
        unsafe { ManuallyDrop::drop(&mut self.future) };
    }
}
// The enclosing Source only retires an original after its observed Drop. An
// unresolved/panicking original must not be implicitly destroyed a second time.
struct Body {
    prepared: Option<PreparedOperation>,
    shared: Option<Arc<PreparedOperation>>,
    original: Option<OpaqueOriginal>,
    started: bool,
    closed: bool,
}
pub(super) struct PublicationOriginal {
    receiver: std::sync::Weak<Self>,
    final_journal: std::sync::OnceLock<PublicationJournal<ConnectorStagedCreateReceipt>>,
    cleanup_result: Mutex<Option<Result<(), ConnectorError>>>,
    adapter: IcebergStagedCreateAdapter,
    request: ConnectorStagedCreatePublishRequest,
    observation: ConnectorStagedCreatePublicationRecorder,
    body: Mutex<Option<Body>>,
    report: Mutex<Option<attempt::Report>>,
}
struct Loan<'a> {
    source: &'a PublicationOriginal,
    body: Option<Body>,
}
impl<'a> Loan<'a> {
    fn take(source: &'a PublicationOriginal) -> Self {
        let body = source.body.lock().unwrap_or_else(|p| p.into_inner()).take();
        assert!(body.is_some(), "one original staged publication poll");
        Self { source, body }
    }
    fn body(&mut self) -> &mut Body {
        self.body.as_mut().expect("same original publication body")
    }
}
impl Drop for Loan<'_> {
    fn drop(&mut self) {
        let mut target = self.source.body.lock().unwrap_or_else(|p| p.into_inner());
        assert!(
            target.is_none(),
            "same original staged publication body returns once"
        );
        *target = self.body.take();
    }
}
impl PublicationOriginal {
    pub(super) fn new(
        adapter: IcebergStagedCreateAdapter,
        request: ConnectorStagedCreatePublishRequest,
        observation: ConnectorStagedCreatePublicationRecorder,
    ) -> Result<Arc<Self>, ConnectorError> {
        if request.operation_id != request.handle.operation_id() {
            return Err(invalid(
                "Create publication must preserve the original staged operation token",
            ));
        }
        if request.handle.owner() != &adapter.owner() {
            return Err(invalid("Iceberg staged-create publish has a foreign owner"));
        }
        // Validate the borrowed original before removing it. There is no
        // Provider F, grant, or I/O in this factory.
        let properties = publication_document_properties(&request.handle, &request.payload)?;
        let mut prepared = {
            let mut operations = adapter
                .operations
                .lock()
                .map_err(|_| internal("staged-create operation lock is poisoned"))?;
            let Some(slot) = operations.get_mut(&request.handle.operation_id()) else {
                return Err(invalid(
                    "Iceberg staged-create operation is not an unpublished prepared target",
                ));
            };
            let OperationState::Prepared(prepared) = slot else {
                return Err(invalid(
                    "Iceberg staged-create operation is not an unpublished prepared target",
                ));
            };
            if prepared.publication_closed || prepared.handle_digest != request.handle.digest() {
                return Err(invalid(
                    "Iceberg staged-create publish handle digest mismatch",
                ));
            }
            if prepared
                .write
                .as_ref()
                .is_none_or(|write| write.write != request.write)
            {
                return Err(invalid(
                    "Iceberg staged-create publish requires the exact bound write",
                ));
            }
            // Keep the ORIGINAL operation slot occupied while the original
            // aggregate lives in Source. Another prepare cannot reuse its ID.
            match std::mem::replace(
                slot,
                OperationState::Publishing {
                    handle_digest: request.handle.digest(),
                },
            ) {
                OperationState::Prepared(prepared) => prepared,
                _ => unreachable!("validated prepared slot is replaced under the same lock"),
            }
        };
        prepared.document_properties = properties;
        // No foreign call follows the unique move before this header exists.
        Ok(Arc::new_cyclic(|receiver| Self {
            receiver: receiver.clone(),
            final_journal: std::sync::OnceLock::new(),
            cleanup_result: Mutex::new(None),
            adapter,
            request,
            observation,
            body: Mutex::new(Some(Body {
                prepared: Some(prepared),
                shared: None,
                original: None,
                started: false,
                closed: false,
            })),
            report: Mutex::new(None),
        }))
    }
    #[cfg(test)]
    pub(super) fn body_for_test(&self) -> (bool, bool) {
        let guard = self.body.lock().unwrap();
        let body = guard.as_ref().unwrap();
        (
            body.prepared.is_some() || body.shared.is_some(),
            body.original.is_some(),
        )
    }
    #[cfg(test)]
    pub(super) fn install_ready_original_for_test(
        &self,
        future: Pin<Box<dyn Future<Output = ()> + Send>>,
    ) {
        let mut loan = Loan::take(self);
        let body = loan.body();
        assert!(!body.started);
        body.shared = Some(Arc::new(body.prepared.take().unwrap()));
        body.original = Some(OpaqueOriginal::new(future));
        body.started = true;
    }
    fn initialize(&self, body: &mut Body) -> Result<(), ConnectorError> {
        let prepared = body
            .prepared
            .as_mut()
            .expect("same original prepared operation");
        let operation = publication::ensure_operation(
            &self.adapter,
            prepared,
            self.request.operation_id,
            &self.request.context,
        )?;
        let intent = publication::create_intent(prepared, operation.token())
            .map_err(|error| corrupt(error.to_string()))?;
        let prefix = publication::create_prefix(prepared);
        let policy =
            attempt::RetryPolicy::from_properties(prepared.staged.table.metadata().properties())
                .map_err(|error| invalid(error.to_string()))?;
        let journal = prepared
            .stage_publication
            .journal
            .next_frontier()
            .map_err(|error| internal(error.to_string()))?;
        prepared.final_publication = Some(journal.clone());
        // Publish the same final frontier before the first opaque child poll.
        assert!(
            self.final_journal.set(journal.clone()).is_ok(),
            "one original final publication journal"
        );
        let prepared = Arc::new(
            body.prepared
                .take()
                .expect("original prepared operation moves into its stable shared field"),
        );
        body.shared = Some(prepared.clone());
        let publisher = journal.observe_prepared(Arc::new(publication::CreatePublisher {
            adapter: self.adapter.clone(),
            prepared: prepared.clone(),
            operation_id: self.request.operation_id,
            journal: journal.clone(),
        }));
        let publisher = OriginalPublisher {
            adapter: self.adapter.clone(),
            prepared: prepared.clone(),
            inner: publisher,
            journal: journal.clone(),
            observation: self.observation.clone(),
            context: self.request.context.clone(),
        };
        let receiving = self.receiver.clone();
        let observation = self.observation.clone();
        let cleanup_receiving = self.receiver.clone();
        let current = self.request.context.clone();
        let dispatch = self.request.operation_id;
        body.original = Some(OpaqueOriginal::new(Box::pin(async move {
            let append = crate::commit::FastAppendPreparer;
            let preparers: Vec<&dyn crate::commit::staging::Preparer> =
                if intent.changes().added.is_empty() {
                    vec![]
                } else {
                    vec![&append]
                };
            let mut child = Box::pin(attempt::run_to_committed_frontier(
                &operation, &intent, &prefix, &preparers, &publisher, policy,
            ));
            // Both the raw report and its typed projection are received before
            // the ready attempt Future is destroyed.
            std::future::poll_fn(|cx| match child.as_mut().poll(cx) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(report) => {
                    let source = receiving
                        .upgrade()
                        .expect("original staged publication receiver remains owned");
                    {
                        let mut slot = source.report.lock().unwrap_or_else(|p| p.into_inner());
                        assert!(slot.is_none(), "one original staged publication report");
                        *slot = Some(report);
                    }
                    let report = source
                        .report
                        .lock()
                        .unwrap_or_else(|p| p.into_inner())
                        .take()
                        .expect("received original staged report");
                    let projected = journal.project_retained_committed_frontier(report);
                    let outcome = match projected {
                        RetainedPublicationProjection::Committed {
                            effect,
                            receipt,
                            finalization,
                        } => committed(effect, receipt, finalization),
                        RetainedPublicationProjection::Settled(outcome) => {
                            project_outcome(&source.adapter, &prepared, dispatch, outcome)
                        }
                    };
                    observation.record_raw(outcome);
                    Poll::Ready(())
                }
            })
            .await;
            drop(child);
            // The first positive frontier deliberately did not run safe GC.
            // This is the SAME operation and journal's exact retained set.
            if (observation.applied_receipt().is_some()
                || matches!(
                    observation.raw_outcome(),
                    Some(Ok(ConnectorStagedCreatePublishOutcome::NoOp { .. }))
                ))
                && matches!(
                    journal.catalog_entry_outcome(
                        journal.current_entry().expect("final publication entry")
                    ),
                    ExternalMutationOutcome::KnownCommitted { .. }
                )
            {
                let result = journal
                    .settle_committed_cleanup_in(&operation, &current)
                    .await;
                let source = cleanup_receiving
                    .upgrade()
                    .expect("same original cleanup receiver");
                let mut slot = source
                    .cleanup_result
                    .lock()
                    .unwrap_or_else(|p| p.into_inner());
                assert!(slot.is_none(), "one original staged cleanup call result");
                *slot = Some(result);
            }
        })));
        body.started = true;
        Ok(())
    }
    fn finish_original(&self, body: &mut Body) {
        let shared = body
            .shared
            .take()
            .expect("stable original prepared aggregate");
        let prepared = match Arc::try_unwrap(shared) {
            Ok(prepared) => prepared,
            Err(shared) => {
                body.shared = Some(shared);
                panic!(
                    "staged publication still has an actual original borrower after its Future exit"
                )
            }
        };
        match self
            .observation
            .raw_outcome()
            .expect("same original staged outcome")
        {
            Ok(ConnectorStagedCreatePublishOutcome::Applied { .. }) => {
                self.adapter
                    .record_published(self.request.handle.operation_id(), prepared);
            }
            Ok(ConnectorStagedCreatePublishOutcome::NoOp {
                ref finalization, ..
            }) => {
                // NoOpIfExists is a definite non-publication. Its original
                // cleanup report, never an empty slot, chooses retained state.
                if prepared
                    .final_publication
                    .as_ref()
                    .and_then(|journal| {
                        journal
                            .current_entry()
                            .map(|entry| journal.catalog_entry_outcome(entry))
                    })
                    .is_some_and(|outcome| {
                        matches!(
                            outcome,
                            ExternalMutationOutcome::KnownCommitted {
                                effect: ExternalMutationEffect::NoOp,
                                ..
                            }
                        )
                    })
                {
                    self.adapter
                        .record_published(self.request.handle.operation_id(), prepared);
                    return;
                }
                let mut prepared = prepared;
                prepared.publication_closed = true;
                self.adapter.record_terminal(
                    self.request.handle.operation_id(),
                    if matches!(finalization, ExternalMutationFinalization::Complete) {
                        OperationState::Published {
                            handle_digest: prepared.handle_digest,
                        }
                    } else {
                        OperationState::CleanupPending(prepared)
                    },
                );
            }
            Ok(
                ConnectorStagedCreatePublishOutcome::Conflict { ref cleanup, .. }
                | ConnectorStagedCreatePublishOutcome::KnownUncommitted { ref cleanup, .. },
            ) => self.adapter.record_cleanup_state(
                self.request.handle.operation_id(),
                prepared,
                cleanup,
            ),
            Ok(ConnectorStagedCreatePublishOutcome::CommitUnknown { ref evidence, .. }) => self
                .adapter
                .set_publication_unknown(self.request.handle.operation_id(), evidence, prepared),
            Err(_) => self.adapter.record_terminal(
                self.request.handle.operation_id(),
                OperationState::Prepared(prepared),
            ),
        }
    }
    fn poll_original(&self, cx: &mut Context<'_>) -> Poll<()> {
        if self.observation.foreign_exit_unproven() {
            // A prior unwind is a sticky original health frontier. The caller
            // owns that raw panic; no new poll or destructor may replace it.
            return Poll::Pending;
        }
        let mut loan = Loan::take(self);
        let body = loan.body();
        if body.closed {
            return Poll::Ready(());
        }
        if !body.started {
            self.observation.enter_foreign();
            if let Err(error) = self.initialize(body) {
                // No publication Future was started. The original staged
                // target remains abortable, not falsely cleaned or published.
                self.observation.record_preparation_refusal(error);
                if let Some(prepared) = body.prepared.take() {
                    self.adapter.record_terminal(
                        self.request.handle.operation_id(),
                        OperationState::Prepared(prepared),
                    );
                }
                body.closed = true;
                self.observation.record_provider_exit();
                return Poll::Ready(());
            }
            self.observation.leave_pending();
        }
        self.observation.enter_foreign();
        let polled = body
            .original
            .as_mut()
            .expect("same original publication Future")
            .future
            .as_mut()
            .poll(cx);
        if polled.is_pending() {
            self.observation.leave_pending();
            return Poll::Pending;
        }
        // Keep the original allocation in its slot if its destructor unwinds.
        body.original.as_mut().unwrap().actual_drop();
        body.original = None;
        self.finish_original(body);
        body.closed = true;
        self.observation.record_provider_exit();
        Poll::Ready(())
    }
}
impl ConnectorStagedCreatePublicationSource for PublicationOriginal {
    fn dispatch_started(&self) -> bool {
        self.final_journal
            .get()
            .is_some_and(PublicationJournal::catalog_dispatch_started)
    }
    fn poll_progress(&self, cx: &mut Context<'_>) -> Poll<()> {
        self.poll_original(cx)
    }
    fn poll_closed(&self, cx: &mut Context<'_>) -> Poll<()> {
        self.poll_original(cx)
    }
}
impl Drop for PublicationOriginal {
    fn drop(&mut self) {
        // An unpolled publication has not created a physical Future. Return
        // its original prepared target rather than abandoning that inventory.
        let body = self.body.get_mut().unwrap_or_else(|p| p.into_inner());
        if let Some(body) = body
            && !body.started
            && !body.closed
            && let Some(prepared) = body.prepared.take()
        {
            self.adapter.record_terminal(
                self.request.handle.operation_id(),
                OperationState::Prepared(prepared),
            );
        }
    }
}
pub(super) fn begin(
    adapter: IcebergStagedCreateAdapter,
    request: ConnectorStagedCreatePublishRequest,
    observation: ConnectorStagedCreatePublicationRecorder,
) -> Result<Arc<dyn ConnectorStagedCreatePublicationSource>, ConnectorError> {
    Ok(PublicationOriginal::new(adapter, request, observation)?)
}
fn committed(
    effect: ExternalMutationEffect,
    receipt: ConnectorStagedCreateReceipt,
    finalization: ExternalMutationFinalization,
) -> Result<ConnectorStagedCreatePublishOutcome, ConnectorError> {
    Ok(match effect {
        ExternalMutationEffect::Applied => ConnectorStagedCreatePublishOutcome::Applied {
            receipt,
            finalization,
        },
        ExternalMutationEffect::NoOp => ConnectorStagedCreatePublishOutcome::NoOp {
            receipt,
            finalization,
        },
    })
}
fn project_outcome(
    adapter: &IcebergStagedCreateAdapter,
    prepared: &PreparedOperation,
    dispatch: ConnectorStagedCreateOperationId,
    outcome: ExternalMutationOutcome<ConnectorStagedCreateReceipt>,
) -> Result<ConnectorStagedCreatePublishOutcome, ConnectorError> {
    match outcome {
        ExternalMutationOutcome::KnownCommitted {
            effect,
            receipt,
            finalization,
            ..
        } => committed(effect, receipt, finalization),
        ExternalMutationOutcome::KnownUncommitted { failure, cleanup }
            if failure.kind() == ConnectorMutationFailureKind::Conflict
                && prepared.policy == CreatePolicy::NoOpIfExists =>
        {
            Ok(ConnectorStagedCreatePublishOutcome::NoOp {
                receipt: adapter.receipt(
                    dispatch,
                    ConnectorStagedCreateReceiptPhase::Published,
                    ExternalMutationEffect::NoOp,
                    Bytes::new(),
                )?,
                finalization: cleanup,
            })
        }
        ExternalMutationOutcome::KnownUncommitted { failure, cleanup }
            if failure.kind() == ConnectorMutationFailureKind::Conflict =>
        {
            Ok(ConnectorStagedCreatePublishOutcome::Conflict { failure, cleanup })
        }
        ExternalMutationOutcome::KnownUncommitted { failure, cleanup } => {
            Ok(ConnectorStagedCreatePublishOutcome::KnownUncommitted { failure, cleanup })
        }
        ExternalMutationOutcome::CommitUnknown { failure, evidence } => {
            Ok(ConnectorStagedCreatePublishOutcome::CommitUnknown { failure, evidence })
        }
    }
}
struct OriginalPublisher {
    adapter: IcebergStagedCreateAdapter,
    prepared: Arc<PreparedOperation>,
    inner: PreparedObservedPublisher<ConnectorStagedCreateReceipt>,
    journal: PublicationJournal<ConnectorStagedCreateReceipt>,
    observation: ConnectorStagedCreatePublicationRecorder,
    context: ConnectorRequestContext,
}
#[async_trait]
impl Publisher for OriginalPublisher {
    async fn load_target(
        &self,
        attempt: &IcebergCommitAttempt,
    ) -> crate::iceberg::Result<StagingBase> {
        self.inner.load_target(attempt).await
    }
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
    fn preflight_recovery(
        &self,
        request: &FrozenRequest,
        operation: &IcebergCommitOperation,
    ) -> crate::iceberg::Result<()> {
        self.inner.preflight_recovery(request, operation)
    }
    fn preflight_recovery_in<'a>(
        &'a self,
        request: &'a mut Option<FrozenRequest>,
        operation: &'a IcebergCommitOperation,
    ) -> attempt::RecoveryPreflightFuture<'a> {
        self.inner.preflight_recovery_in(request, operation)
    }
    async fn dispatch_once(&self, request: FrozenRequest) -> CatalogOutcome<CommitProof> {
        let pending = match self.inner.dispatch_to_finalization(request).await {
            Ok(pending) => pending,
            Err(error) => {
                return match self.journal.current_entry() {
                    Some(entry) => self.journal.commit_outcome(entry),
                    None => CatalogOutcome::KnownUncommitted {
                        failure: attempt::before_dispatch_failure(&error),
                    },
                };
            }
        };
        let entry = pending.entry();
        let policy = pending.evidence_policy();
        let mut reception = None;
        if let ExternalMutationOutcome::KnownCommitted {
            effect: ExternalMutationEffect::Applied,
            receipt,
            ..
        } = self.journal.catalog_entry_outcome(entry)
        {
            reception = self.observation.record_applied_receipt(&receipt).err();
            if reception.is_none() {
                let context = self
                    .journal
                    .finalization_context(entry, policy)
                    .and_then(|grant| grant.for_io(&self.context));
                reception = self
                    .observation
                    .record_finalization_context(&receipt, context)
                    .err();
            }
        }
        // Positive facts and the original absolute finite Context are stable
        // before finish destroys the ready dispatch child or runs finalization.
        if let Err(error) = pending.finish(policy).await {
            self.journal.record_entry_finalization(
                entry,
                ExternalMutationFinalization::Failed(attempt::before_dispatch_failure(&error)),
            );
        }
        if let Some(error) = reception {
            self.journal.record_entry_finalization(
                entry,
                ExternalMutationFinalization::Failed(ConnectorMutationFailure::from_connector(
                    error,
                )),
            );
        }
        if let ExternalMutationOutcome::KnownCommitted {
            effect,
            receipt,
            finalization,
            ..
        } = self.journal.catalog_entry_outcome(entry)
        {
            // Necessary cache invalidation precedes Ready, independently of GC.
            invalidate_prepared(&self.adapter.runtime, &self.prepared);
            if let Err(error) =
                self.observation
                    .record_needed_committed(effect, &receipt, &finalization)
            {
                self.journal.record_entry_finalization(
                    entry,
                    ExternalMutationFinalization::Failed(ConnectorMutationFailure::from_connector(
                        error,
                    )),
                );
            }
        }
        self.journal.commit_outcome(entry)
    }
}
