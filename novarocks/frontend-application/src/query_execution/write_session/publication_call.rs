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

//! Stable publication input and outcome custody for one original write session.
use super::*;
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use novarocks_spi::connector::write_stack::{
    ConnectorWriteNeededReady, ConnectorWriteNeededReadyReceiving,
};
type Outcome = Result<
    novarocks_spi::connector::write_stack::ConnectorWritePublicationCompletion,
    ConnectorError,
>;
type AbortOutcome = Result<ConnectorWriteAbortOutcome, ConnectorError>;
enum CallCompletion {
    Publication(Outcome),
    Abort(AbortOutcome),
}
type PublicationFuture = Pin<Box<dyn Future<Output = CallCompletion> + Send>>;

/// The original receiving allocation outlives every borrowed Provider child.
/// The Context result is independent of the business verdict and actual exit.
#[derive(Default)]
struct CallReceiving {
    outcome: Mutex<Option<CallCompletion>>,
    finalization_context:
        std::sync::OnceLock<Result<Option<ConnectorRequestContext>, ConnectorError>>,
}
impl CallReceiving {
    fn receive_finalization_context(
        &self,
        session: &ConnectorWriteSession,
        effect: novarocks_spi::connector::ExternalMutationEffect,
    ) {
        if self.finalization_context.get().is_some() {
            return;
        }
        // The single original poll author serializes this with early NeededReady.
        // Invoke the foreign getter outside the outcome mutex, and retain None
        // or an ordinary error just as permanently as a successful observation.
        let received = match effect {
            novarocks_spi::connector::ExternalMutationEffect::Applied => {
                session.existing_finalization_context()
            }
            novarocks_spi::connector::ExternalMutationEffect::NoOp => Ok(None),
        };
        let _ = self.finalization_context.set(received);
    }

    fn receive_completion(
        &self,
        session: &ConnectorWriteSession,
        outcome: CallCompletion,
        needed_receiving: Option<&ConnectorWriteNeededReadyReceiving>,
    ) {
        let effect = needed_receiving
            .and_then(|receiving| receiving.observed())
            .map(|needed| needed.effect())
            .or_else(|| match &outcome {
            CallCompletion::Publication(Ok(
                novarocks_spi::connector::write_stack::ConnectorWritePublicationCompletion::KnownCommitted { needed, .. },
            )) => Some(needed.effect()),
            CallCompletion::Abort(Ok(ConnectorWriteAbortOutcome::KnownCommitted { needed, .. })) => {
                Some(needed.effect())
            }
            _ => None,
        });
        {
            let mut receiving = self.outcome.lock().unwrap_or_else(|p| p.into_inner());
            assert!(receiving.is_none(), "original completion is received once");
            *receiving = Some(outcome);
        }
        // The canonical result is stable before the getter can fail or panic.
        // Both observations precede the actual Provider child destructor.
        if let Some(effect) = effect {
            self.receive_finalization_context(session, effect);
        }
    }
}

pub(super) struct PublicationInput {
    pub(super) received: Arc<ReceivedWriteInput>,
    pub(super) publication: ConnectorWriteFinishPublication,
    pub(super) context: ConnectorRequestContext,
    pub(super) production: novarocks_spi::connector::write_stack::ConnectorWriteAbortProduction,
}
struct AbortInput {
    context: ConnectorRequestContext,
    production: novarocks_spi::connector::write_stack::ConnectorWriteAbortProduction,
    received: Option<Arc<PublicationInput>>,
}
struct ReconciliationInput {
    evidence: ExternalMutationEvidence,
    context: ConnectorRequestContext,
}

#[derive(Debug)]
struct PublicationExitUnproven;
impl std::fmt::Display for PublicationExitUnproven {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("write publication poll or future Drop did not return normally")
    }
}
impl std::error::Error for PublicationExitUnproven {}

/// The outer statement Source must retain this call through actual local exit.
/// A ready business result is recorded before any foreign future destructor runs.
pub(crate) struct WritePublicationCall {
    future: Option<PublicationFuture>,
    needed_receiving: Arc<ConnectorWriteNeededReadyReceiving>,
    close_needed_ready: Option<Arc<ConnectorWriteNeededReady>>,
    needed_polled: bool,
    outcome: Option<Outcome>,
    inflight_outcome: Arc<CallReceiving>,
    secondary_outcome: Option<Outcome>,
    abort_outcome: Option<AbortOutcome>,
    abort_input: Option<Arc<AbortInput>>,
    reconciliation: Option<Arc<ReconciliationInput>>,
    reconciling: bool,
    reconciliation_started: bool,
    poll_poisoned: bool,
    drop_completed: bool,
    taken: bool,
    production_settled: bool,
    health: novarocks_query_application::api::StatementNativeHealthFailure,
    // Retires last, after both outcomes and the actual future.
    session: Arc<ConnectorWriteSession>,
}
impl WritePublicationCall {
    pub(super) fn new(session: Arc<ConnectorWriteSession>, input: Arc<PublicationInput>) -> Self {
        let slot = Arc::new(CallReceiving::default());
        let needed_receiving = ConnectorWriteNeededReadyReceiving::new();
        let future = Box::pin(finish_future((
            Arc::clone(&session),
            input,
            Arc::clone(&slot),
            Arc::clone(&needed_receiving),
        )));
        Self::from_future(session, future, slot, None, needed_receiving)
    }
    pub(super) fn new_abort(
        session: Arc<ConnectorWriteSession>,
        context: ConnectorRequestContext,
        production: novarocks_spi::connector::write_stack::ConnectorWriteAbortProduction,
        received: Option<Arc<PublicationInput>>,
    ) -> Self {
        let input = Arc::new(AbortInput {
            context,
            production,
            received,
        });
        let slot = Arc::new(CallReceiving::default());
        let future = Box::pin(abort_future((
            Arc::clone(&session),
            Arc::clone(&input),
            Arc::clone(&slot),
        )));
        Self::from_future(
            session,
            future,
            slot,
            Some(input),
            ConnectorWriteNeededReadyReceiving::new(),
        )
    }
    fn from_future(
        session: Arc<ConnectorWriteSession>,
        future: PublicationFuture,
        slot: Arc<CallReceiving>,
        abort_input: Option<Arc<AbortInput>>,
        needed_receiving: Arc<ConnectorWriteNeededReadyReceiving>,
    ) -> Self {
        Self {
            future: Some(future),
            needed_receiving,
            close_needed_ready: None,
            needed_polled: false,
            inflight_outcome: slot,
            outcome: None,
            secondary_outcome: None,
            abort_outcome: None,
            abort_input,
            reconciliation: None,
            reconciling: false,
            reconciliation_started: false,
            poll_poisoned: false,
            drop_completed: false,
            taken: false,
            production_settled: false,
            health:
                novarocks_query_application::api::StatementNativeHealthFailure::actual_exit_unproven(
                    "write publication local exit is unproven",
                    Arc::new(PublicationExitUnproven),
                ),
            session,
        }
    }

    fn poll_actual_completion(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        if self.poll_poisoned {
            return Poll::Pending;
        }
        if self.drop_completed {
            return Poll::Ready(());
        }
        let Some(future) = self.future.as_mut() else {
            return Poll::Pending;
        };
        // Set before crossing the original Provider poll. An unwind leaves the
        // same F in its fixed slot; it must never be polled again.
        self.poll_poisoned = true;
        let polled = future.as_mut().poll(cx);
        match polled {
            Poll::Pending => {
                self.poll_poisoned = false;
                Poll::Pending
            }
            Poll::Ready(outcome) => {
                match outcome {
                    CallCompletion::Publication(outcome) if self.reconciling => {
                        self.secondary_outcome = Some(outcome)
                    }
                    CallCompletion::Publication(outcome) => self.outcome = Some(outcome),
                    CallCompletion::Abort(outcome) => self.abort_outcome = Some(outcome),
                }
                self.receive_finalization_context();
                // The business cause already belongs to this fixed Call. A
                // destructor unwind cannot erase it or manufacture local exit.
                let completed = self.future.take();
                drop(completed);
                if self.abort_input.is_some() && !self.reconciling {
                    // Observe outstanding production responsibility while the
                    // original Provider result is already in the fixed slot.
                    let pending = !matches!(self.session.has_unreceived_productions(), Ok(false));
                    if let Some(Ok(ConnectorWriteAbortOutcome::KnownUncommitted { cleanup })) =
                        &mut self.abort_outcome
                        && pending
                        && matches!(
                            cleanup,
                            novarocks_spi::connector::ExternalMutationFinalization::Complete
                        )
                    {
                        *cleanup = novarocks_spi::connector::ExternalMutationFinalization::Failed(
                            novarocks_spi::connector::ConnectorMutationFailure::new(
                                novarocks_spi::connector::ConnectorMutationFailureKind::Unavailable,
                                "unreceived writer outputs retain an unknown object inventory",
                            ),
                        );
                    }
                    let unknown = matches!(
                        self.abort_outcome,
                        Some(Ok(ConnectorWriteAbortOutcome::CommitUnknown { .. }))
                    );
                    if unknown {
                        self.session
                            .record_terminal(TerminalDecision::CommitUnknown);
                    }
                    if !pending && !unknown {
                        self.session.release_terminal_storage_resolver();
                    }
                } else {
                    let settled = self
                        .secondary_outcome
                        .as_ref()
                        .or(self.outcome.as_ref())
                        .unwrap();
                    if matches!(settled, Ok(novarocks_spi::connector::write_stack::ConnectorWritePublicationCompletion::CommitUnknown { .. })) {
                        self.session
                            .record_terminal(TerminalDecision::CommitUnknown);
                    } else if settled.is_ok() || !self.reconciling {
                        self.session.release_terminal_storage_resolver();
                    }
                }
                if self.settle_actual_productions().is_pending() {
                    return Poll::Pending;
                }
                self.drop_completed = true;
                self.poll_poisoned = false;
                Poll::Ready(())
            }
        }
    }

    fn settle_actual_productions(&mut self) -> Poll<()> {
        if !self.production_settled
            && matches!(
                self.publication_disposition(),
                Some(
                    novarocks_spi::connector::LakePublicationDisposition::KnownCommitted
                        | novarocks_spi::connector::LakePublicationDisposition::KnownUncommitted
                )
            )
        {
            let receivable = match self.session.has_unreceived_productions() {
                Ok(receivable) => !receivable,
                Err(cause) => {
                    self.health = novarocks_query_application::api::StatementNativeHealthFailure::actual_exit_unproven(
                                "write production observation failed", Arc::new(cause),
                            );
                    return Poll::Pending;
                }
            };
            let notice = match if receivable {
                self.session.settle_received_productions_in()
            } else {
                Ok(None)
            } {
                Ok(notice) => notice,
                Err(cause) => {
                    self.health = novarocks_query_application::api::StatementNativeHealthFailure::actual_exit_unproven(
                                "write production disposition could not be installed", Arc::new(cause),
                            );
                    return Poll::Pending;
                }
            };
            // The actual verdict and cohort release are stable before
            // any foreign progress callback, including wake panic.
            self.production_settled = true;
            if let Some(notice) = notice {
                notice.notify();
            }
        }
        Poll::Ready(())
    }
    fn receive_finalization_context(&mut self) {
        if self.inflight_outcome.finalization_context.get().is_some() {
            return;
        }
        let needed = self.needed_receiving.observed().or_else(|| {
            match (self.secondary_outcome.as_ref().or(self.outcome.as_ref()), self.abort_outcome.as_ref()) {
                (Some(Ok(novarocks_spi::connector::write_stack::ConnectorWritePublicationCompletion::KnownCommitted { needed, .. })), _) => Some(Arc::clone(needed)),
                (_, Some(Ok(ConnectorWriteAbortOutcome::KnownCommitted { needed, .. }))) => Some(Arc::clone(needed)),
                _ => None,
            }
        });
        let Some(needed) = needed else { return };
        self.inflight_outcome
            .receive_finalization_context(&self.session, needed.effect());
    }
    pub(crate) fn existing_finalization_context(
        &self,
    ) -> Result<Option<&ConnectorRequestContext>, ConnectorError> {
        match self.inflight_outcome.finalization_context.get() {
            Some(Ok(context)) => Ok(context.as_ref()),
            Some(Err(error)) => Err(error.clone()),
            None => Ok(None),
        }
    }

    fn retain_needed_for_close(&mut self) -> Result<(), ConnectorError> {
        if self.needed_receiving.observed().is_none()
            || self.needed_receiving.cleanup_is_permitted()
        {
            return Ok(());
        }
        self.poll_poisoned = true;
        self.receive_finalization_context();
        let notice = self
            .needed_receiving
            .handoff_in(&mut self.close_needed_ready)?;
        notice.notify();
        self.poll_poisoned = false;
        Ok(())
    }
    /// Whole original F/Drop completion. A failure or cancelled caller still
    /// retains the ACTUAL committed fact before permitting its same GC tail.
    pub(crate) fn poll_completed(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        if self.poll_poisoned {
            return Poll::Pending;
        }
        if let Err(cause) = self.retain_needed_for_close() {
            self.health=novarocks_query_application::api::StatementNativeHealthFailure::actual_exit_unproven("write needed-ready close handoff failed",Arc::new(cause));
            return Poll::Pending;
        }
        let polled = self.poll_actual_completion(cx);
        if !self.poll_poisoned
            && polled.is_pending()
            && let Err(cause) = self.retain_needed_for_close()
        {
            self.health=novarocks_query_application::api::StatementNativeHealthFailure::actual_exit_unproven("write needed-ready close handoff failed",Arc::new(cause));
            return Poll::Pending;
        }
        polled
    }
    /// Business receipt readiness is independent of safe GC. This polls the
    /// original F only until it has yielded with its typed fact installed.
    pub(crate) fn poll_needed_ready(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        if self.poll_poisoned {
            return Poll::Pending;
        }
        if self.needed_receiving.observed().is_none() {
            let polled = self.poll_actual_completion(cx);
            if self.poll_poisoned {
                return Poll::Pending;
            }
            if self.needed_receiving.observed().is_none() {
                return polled;
            }
        }
        self.poll_poisoned = true;
        self.receive_finalization_context();
        if self.settle_actual_productions().is_pending() {
            return Poll::Pending;
        }
        self.needed_polled = true;
        self.poll_poisoned = false;
        Poll::Ready(())
    }
    pub(crate) fn needed_ready(&self) -> Option<Arc<ConnectorWriteNeededReady>> {
        if self.poll_poisoned || !self.needed_polled {
            return None;
        }
        self.needed_receiving.observed()
    }
    /// Only a fact transfer and the original cleanup permission occur here.
    /// Source retirement still requires poll_retirement and Provider disposal.
    pub(crate) fn take_needed_ready_in(
        &mut self,
        target: &mut Option<Arc<ConnectorWriteNeededReady>>,
    ) -> Result<(), ConnectorError> {
        if target.is_some()
            || self.needed_ready().is_none()
            || self.needed_receiving.cleanup_is_permitted()
        {
            return Err(invalid("write needed-ready fact is not transferable"));
        }
        self.poll_poisoned = true;
        let notice = self.needed_receiving.handoff_in(target)?;
        notice.notify();
        self.poll_poisoned = false;
        Ok(())
    }
    pub(crate) fn poll_retirement(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        self.poll_completed(cx)
    }

    #[cfg(test)]
    pub(super) fn retains_original_unknown(&self) -> bool {
        matches!(
            self.outcome,
            Some(Ok(novarocks_spi::connector::write_stack::ConnectorWritePublicationCompletion::CommitUnknown { .. }))
        )
    }
    #[cfg(test)]
    pub(super) fn retains_unreceived_outcome(&self) -> bool {
        self.inflight_outcome
            .outcome
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .is_some()
    }

    /// Publication evidence is independent of local future/port exit. In
    /// particular the child may have published a real verdict before Drop
    /// panicked; observing that verdict does not authorize repoll or release.
    pub(crate) fn publication_disposition(
        &self,
    ) -> Option<novarocks_spi::connector::LakePublicationDisposition> {
        use novarocks_spi::connector::LakePublicationDisposition as D;
        fn publication(outcome: &Outcome) -> D {
            match outcome {
                Ok(novarocks_spi::connector::write_stack::ConnectorWritePublicationCompletion::KnownCommitted { .. }) => D::KnownCommitted,
                Ok(novarocks_spi::connector::write_stack::ConnectorWritePublicationCompletion::KnownUncommitted { .. }) => D::KnownUncommitted,
                Ok(novarocks_spi::connector::write_stack::ConnectorWritePublicationCompletion::CommitUnknown { .. }) | Err(_) => D::CommitUnknown,
            }
        }
        fn abort(outcome: &AbortOutcome) -> D {
            match outcome {
                Ok(ConnectorWriteAbortOutcome::KnownCommitted { .. }) => D::KnownCommitted,
                Ok(ConnectorWriteAbortOutcome::KnownUncommitted { .. }) => D::KnownUncommitted,
                Ok(ConnectorWriteAbortOutcome::CommitUnknown { .. }) | Err(_) => D::CommitUnknown,
            }
        }
        if self.needed_receiving.observed().is_some() {
            return Some(D::KnownCommitted);
        }
        let pending = self
            .inflight_outcome
            .outcome
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        if let Some(outcome) = pending.as_ref() {
            return Some(match outcome {
                CallCompletion::Publication(outcome) => publication(outcome),
                CallCompletion::Abort(outcome) => abort(outcome),
            });
        }
        self.secondary_outcome
            .as_ref()
            .or(self.outcome.as_ref())
            .map(publication)
            .or_else(|| self.abort_outcome.as_ref().map(abort))
    }

    /// Observe only the canonical received effect, even if child Drop or
    /// handoff notification panicked. This grants no cleanup or replay right.
    pub(crate) fn received_committed_effect(
        &self,
    ) -> Option<novarocks_spi::connector::ExternalMutationEffect> {
        fn publication(
            outcome: &Outcome,
        ) -> Option<novarocks_spi::connector::ExternalMutationEffect> {
            match outcome {
                Ok(novarocks_spi::connector::write_stack::ConnectorWritePublicationCompletion::KnownCommitted { needed, .. }) => Some(needed.effect()),
                _ => None,
            }
        }
        fn abort(
            outcome: &AbortOutcome,
        ) -> Option<novarocks_spi::connector::ExternalMutationEffect> {
            match outcome {
                Ok(ConnectorWriteAbortOutcome::KnownCommitted { needed, .. }) => {
                    Some(needed.effect())
                }
                _ => None,
            }
        }
        if let Some(needed) = self.needed_receiving.observed() {
            return Some(needed.effect());
        }
        {
            let pending = self
                .inflight_outcome
                .outcome
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            if let Some(outcome) = pending.as_ref() {
                return match outcome {
                    CallCompletion::Publication(outcome) => publication(outcome),
                    CallCompletion::Abort(outcome) => abort(outcome),
                };
            }
        }
        self.secondary_outcome
            .as_ref()
            .or(self.outcome.as_ref())
            .and_then(publication)
            .or_else(|| self.abort_outcome.as_ref().and_then(abort))
    }

    pub(crate) fn local_health_failure(
        &self,
    ) -> Option<novarocks_query_application::api::StatementNativeHealthFailure> {
        self.poll_poisoned.then(|| self.health.clone())
    }
    pub(crate) fn outcome(&self) -> Option<&Outcome> {
        if !self.drop_completed || self.poll_poisoned || self.taken {
            return None;
        }
        self.secondary_outcome.as_ref().or(self.outcome.as_ref())
    }
    pub(crate) fn abort_outcome(&self) -> Option<&AbortOutcome> {
        if !self.drop_completed || self.poll_poisoned || self.taken || self.reconciliation_started {
            return None;
        }
        self.abort_outcome.as_ref()
    }
    pub(crate) fn take_abort_outcome_in(
        &mut self,
        target: &mut Option<AbortOutcome>,
    ) -> Result<(), ConnectorError> {
        if target.is_some() || self.abort_outcome().is_none() {
            return Err(invalid("write abort outcome is not transferable"));
        }
        *target = self.abort_outcome.take();
        self.taken = true;
        Ok(())
    }
    pub(crate) fn take_outcome_in(
        &mut self,
        target: &mut Option<Outcome>,
    ) -> Result<(), ConnectorError> {
        if target.is_some() || self.outcome().is_none() {
            return Err(invalid("write publication outcome is not transferable"));
        }
        *target = if self.reconciliation_started {
            self.secondary_outcome.take()
        } else {
            self.outcome.take()
        };
        self.taken = true;
        Ok(())
    }
    pub(crate) fn start_reconciliation(
        &mut self,
        evidence: ExternalMutationEvidence,
        context: ConnectorRequestContext,
    ) -> Result<(), ConnectorError> {
        if self.reconciliation_started
            || self.taken
            || !(matches!(
                self.outcome(),
                Some(Ok(novarocks_spi::connector::write_stack::ConnectorWritePublicationCompletion::CommitUnknown { .. }))
            ) || matches!(
                self.abort_outcome(),
                Some(Ok(ConnectorWriteAbortOutcome::CommitUnknown { .. }))
            ))
        {
            return Err(invalid(
                "write publication has no retained unknown result to reconcile",
            ));
        }
        let input = Arc::new(ReconciliationInput {
            evidence,
            context: self.session.terminal_context(context)?,
        });
        let future = Box::pin(reconcile_future((
            Arc::clone(&self.session),
            Arc::clone(&input),
            Arc::clone(&self.inflight_outcome),
            Arc::clone(&self.needed_receiving),
        )));
        self.reconciliation = Some(input);
        self.future = Some(future);
        self.reconciliation_started = true;
        self.reconciling = true;
        self.drop_completed = false;
        Ok(())
    }
}

async fn finish_future(
    (session, input, slot, needed_receiving): (
        Arc<ConnectorWriteSession>,
        Arc<PublicationInput>,
        Arc<CallReceiving>,
        Arc<ConnectorWriteNeededReadyReceiving>,
    ),
) -> CallCompletion {
    session.finish_invocations.fetch_add(1, Ordering::SeqCst);
    let control = session.lease.session();
    let mut original = control.finish_write_with_needed_ready_async(
        novarocks_spi::connector::write_stack::ConnectorWriteFinishRequestRef {
            commit: session.plan.commit_handle(),
            prepared: &input.received.prepared,
            statistics: &input.received.statistics,
            publication: &input.publication,
            context: &input.context,
            production: input.production,
        },
        &needed_receiving,
    );
    // Await a borrow of the actual child F. Awaiting it by value would Drop
    // that child before its original outcome could enter this stable slot.
    let outcome = std::future::poll_fn(|cx| original.as_mut().poll(cx)).await;
    slot.receive_completion(
        &session,
        CallCompletion::Publication(outcome),
        Some(&needed_receiving),
    );
    drop(original);
    drop(control);

    slot.outcome
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .take()
        .expect("same stored publication result")
}
async fn reconcile_future(
    (session, input, slot, needed_receiving): (
        Arc<ConnectorWriteSession>,
        Arc<ReconciliationInput>,
        Arc<CallReceiving>,
        Arc<ConnectorWriteNeededReadyReceiving>,
    ),
) -> CallCompletion {
    let control = session.lease.session();
    let mut original = control.reconcile_write_with_needed_ready_async(
        ConnectorWriteSessionReconcileRequest {
            commit: session.plan.commit_handle(),
            evidence: input.evidence.clone(),
            context: input.context.clone(),
        },
        &needed_receiving,
    );
    let outcome = std::future::poll_fn(|cx| original.as_mut().poll(cx)).await;
    slot.receive_completion(
        &session,
        CallCompletion::Publication(outcome),
        Some(&needed_receiving),
    );
    drop(original);
    drop(control);

    slot.outcome
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .take()
        .expect("same stored reconciliation result")
}
async fn abort_future(
    (session, input, slot): (
        Arc<ConnectorWriteSession>,
        Arc<AbortInput>,
        Arc<CallReceiving>,
    ),
) -> CallCompletion {
    let control = session.lease.session();
    let mut original = control.abort_write_async(ConnectorWriteSessionAbortRequest {
        commit: session.plan.commit_handle(),
        context: input.context.clone(),
        production: input.production,
        received: input
            .received
            .as_ref()
            .map(|input| &input.received.prepared),
    });
    let outcome = std::future::poll_fn(|cx| original.as_mut().poll(cx)).await;
    slot.receive_completion(&session, CallCompletion::Abort(outcome), None);
    drop(original);
    drop(control);

    slot.outcome
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .take()
        .expect("same stored abort result")
}
fn invalid(detail: &'static str) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::InvalidRequest, detail)
}
