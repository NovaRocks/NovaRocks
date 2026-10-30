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

//! The original write Call and publication evidence across statement phases.

use super::write_action::{
    WriteOutcome, disposition, select_needed_publication, select_publication,
};
use super::*;
use crate::dml::attempt::{
    DmlPublicationAdjudication, DmlPublicationAdjudicationOutcome, DmlPublicationAttempt,
    DmlPublicationFinalization,
};
use crate::query_execution::write_session::{ConnectorWriteSession, WritePublicationCall};
use novarocks_query_application::api::StatementNativeHealthFailure;
use novarocks_spi::connector::write_stack::{
    ConnectorWriteNeededReady, ConnectorWritePublicationCompletion,
};
use novarocks_spi::connector::{
    ConnectorError, ConnectorRequestContext, ConnectorWriteAbortOutcome,
    ExternalMutationFinalization, LakePublicationDisposition,
};
use std::task::{Context, Poll};

/// Borrowed exact effect of the original committed business fact.
pub(crate) enum CommittedWriteEffect<'a> {
    Applied(&'a novarocks_spi::connector::ConnectorWriteReceipt),
    NoOp,
}

pub(crate) struct StatementPublication {
    pub(crate) attempt: DmlPublicationAttempt,
    pub(crate) call: Option<WritePublicationCall>,
    pub(crate) refusal: Option<ConnectorError>,
    pub(crate) outcome: Option<WriteOutcome>,
    needed: Option<Arc<ConnectorWriteNeededReady>>,
    pub(crate) abort_outcome: Option<Result<ConnectorWriteAbortOutcome, ConnectorError>>,
    pub(crate) local_finalization: Option<Result<(), String>>,
    adjudication: Option<DmlPublicationAdjudication>,
    reconciled: bool,
    finished: bool,
    poisoned: bool,
    verdict: Option<LakePublicationDisposition>,
    health: Option<StatementNativeHealthFailure>,
}
impl StatementPublication {
    pub(crate) fn new(attempt: DmlPublicationAttempt) -> Self {
        Self {
            attempt,
            call: None,
            refusal: None,
            outcome: None,
            needed: None,
            abort_outcome: None,
            local_finalization: None,
            adjudication: None,
            reconciled: false,
            finished: false,
            poisoned: false,
            verdict: None,
            health: None,
        }
    }
    pub(crate) fn disposition(&self) -> Option<LakePublicationDisposition> {
        match self.verdict {
            Some(
                LakePublicationDisposition::KnownCommitted
                | LakePublicationDisposition::KnownUncommitted,
            ) => self.verdict,
            _ => self
                .call
                .as_ref()
                .and_then(WritePublicationCall::publication_disposition)
                .or(self.verdict),
        }
    }
    pub(crate) fn local_health_failure(&self) -> Option<StatementNativeHealthFailure> {
        self.health.clone().or_else(|| {
            self.call
                .as_ref()
                .and_then(WritePublicationCall::local_health_failure)
        })
    }
    fn poison(&mut self, error: impl std::error::Error + Send + Sync + 'static) {
        self.poisoned = true;
        self.health = Some(StatementNativeHealthFailure::actual_exit_unproven(
            "statement publication transition did not complete",
            Arc::new(error),
        ));
    }
    /// Caller has already observed local CPU exit and stores every refusal in
    /// its original source. This never invents another session or operation.
    pub(crate) fn poll_settle(
        &mut self,
        session: Option<&Arc<ConnectorWriteSession>>,
        context: &ConnectorRequestContext,
        abort: bool,
        cx: &mut Context<'_>,
    ) -> Poll<()> {
        if self.poisoned {
            return Poll::Pending;
        }
        if self.finished {
            return Poll::Ready(());
        }
        if self.call.is_none() {
            let Some(session) = session else {
                self.finished = true;
                return Poll::Ready(());
            };
            let prepared = if abort {
                session
                    .prepare_abort_call(context.clone())
                    .map(|call| self.call = Some(call))
            } else {
                session.prepare_finish_or_abort_call_in(
                    context.clone(),
                    &mut self.call,
                    &mut self.refusal,
                )
            };
            if let Err(error) = prepared {
                self.poison(error);
                return Poll::Pending;
            }
        }
        let call = self.call.as_mut().expect("original publication Call");
        if call.poll_needed_ready(cx).is_pending() {
            return Poll::Pending;
        }
        if call.needed_ready().is_some() {
            if let Err(error) = call.take_needed_ready_in(&mut self.needed) {
                self.poison(error);
                return Poll::Pending;
            }
            self.verdict = Some(LakePublicationDisposition::KnownCommitted);
            self.finished = true;
            return Poll::Ready(());
        }
        if !self.reconciled {
            let evidence = match (call.outcome(), call.abort_outcome()) {
                (
                    Some(Ok(ConnectorWritePublicationCompletion::CommitUnknown {
                        evidence, ..
                    })),
                    _,
                ) => Some(evidence.clone()),
                (_, Some(Ok(ConnectorWriteAbortOutcome::CommitUnknown { evidence, .. }))) => {
                    Some(evidence.clone())
                }
                _ => None,
            };
            if let Some(evidence) = evidence {
                self.verdict = Some(LakePublicationDisposition::CommitUnknown);
                if let Err(error) = self.attempt.mark_dispatch_possible() {
                    self.poison(error);
                    return Poll::Pending;
                }
                match self.attempt.begin_adjudication() {
                    Ok(token) => self.adjudication = Some(token),
                    Err(error) => {
                        self.poison(error);
                        return Poll::Pending;
                    }
                }
                if let Err(error) = call.start_reconciliation(evidence, context.clone()) {
                    self.poison(error);
                    return Poll::Pending;
                }
                self.reconciled = true;
                if call.poll_needed_ready(cx).is_pending() {
                    return Poll::Pending;
                }
            }
        }
        if call.needed_ready().is_some() {
            if let Err(error) = call.take_needed_ready_in(&mut self.needed) {
                self.poison(error);
                return Poll::Pending;
            }
            self.verdict = Some(LakePublicationDisposition::KnownCommitted);
            self.finished = true;
            return Poll::Ready(());
        }
        let transferred = if call.outcome().is_some() {
            self.verdict = Some(disposition(call.outcome().unwrap()));
            call.take_outcome_in(&mut self.outcome)
        } else if call.abort_outcome().is_some() {
            self.verdict = Some(match call.abort_outcome().unwrap() {
                Ok(ConnectorWriteAbortOutcome::KnownCommitted { .. }) => {
                    LakePublicationDisposition::KnownCommitted
                }
                Ok(ConnectorWriteAbortOutcome::KnownUncommitted { .. }) => {
                    LakePublicationDisposition::KnownUncommitted
                }
                _ => LakePublicationDisposition::CommitUnknown,
            });
            call.take_abort_outcome_in(&mut self.abort_outcome)
        } else {
            return Poll::Pending;
        };
        if let Err(error) = transferred {
            self.poison(error);
            return Poll::Pending;
        }
        self.finished = true;
        Poll::Ready(())
    }
    /// The business fact does not retire the original Call, cleanup or Drop.
    pub(crate) fn poll_retirement(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        if self.poisoned {
            return Poll::Pending;
        }
        let Some(call) = self.call.as_mut() else {
            return Poll::Ready(());
        };
        if call.poll_retirement(cx).is_pending() {
            return Poll::Pending;
        }
        if self.outcome.is_none()
            && call.outcome().is_some()
            && let Err(error) = call.take_outcome_in(&mut self.outcome)
        {
            self.poison(error);
            return Poll::Pending;
        }
        Poll::Ready(())
    }
    /// Exact original canonical business frontier, independent of actual GC.
    pub(crate) fn committed_needed(&self) -> Option<&ConnectorWriteNeededReady> {
        self.needed
            .as_deref()
            .or_else(|| match (&self.outcome, &self.abort_outcome) {
                (
                    Some(Ok(ConnectorWritePublicationCompletion::KnownCommitted {
                        needed, ..
                    })),
                    _,
                ) => Some(needed.as_ref()),
                (_, Some(Ok(ConnectorWriteAbortOutcome::KnownCommitted { needed, .. }))) => {
                    Some(needed.as_ref())
                }
                _ => None,
            })
    }
    /// Clone only the same received fact's Arc; this does not settle the Call
    /// or mint another publication/cleanup authority.
    pub(crate) fn committed_needed_arc(&self) -> Option<Arc<ConnectorWriteNeededReady>> {
        self.needed
            .clone()
            .or_else(|| match (&self.outcome, &self.abort_outcome) {
                (
                    Some(Ok(ConnectorWritePublicationCompletion::KnownCommitted {
                        needed, ..
                    })),
                    _,
                ) => Some(needed.clone()),
                (_, Some(Ok(ConnectorWriteAbortOutcome::KnownCommitted { needed, .. }))) => {
                    Some(needed.clone())
                }
                _ => None,
            })
    }

    /// Borrow the original Provider's already-received finite context. This
    /// observation grants no work and does not extend its absolute deadline.
    pub(crate) fn existing_finalization_context(
        &self,
    ) -> Result<Option<&ConnectorRequestContext>, ConnectorError> {
        match self.call.as_ref() {
            Some(call) => call.existing_finalization_context(),
            None => Ok(None),
        }
    }

    /// None means no committed fact; only actual NoOp proves no external effect.
    pub(crate) fn committed_effect(&self) -> Option<CommittedWriteEffect<'_>> {
        self.committed_needed().map(|needed| match needed.effect() {
            novarocks_spi::connector::ExternalMutationEffect::Applied => {
                CommittedWriteEffect::Applied(needed.receipt())
            }
            novarocks_spi::connector::ExternalMutationEffect::NoOp => CommittedWriteEffect::NoOp,
        })
    }
    /// Same canonical effect before a borrowed child/notice has actually exited.
    /// This observation is independent of custody transfer and cleanup permission.
    pub(crate) fn received_committed_effect(
        &self,
    ) -> Option<novarocks_spi::connector::ExternalMutationEffect> {
        self.committed_effect()
            .map(|effect| match effect {
                CommittedWriteEffect::Applied(_) => {
                    novarocks_spi::connector::ExternalMutationEffect::Applied
                }
                CommittedWriteEffect::NoOp => {
                    novarocks_spi::connector::ExternalMutationEffect::NoOp
                }
            })
            .or_else(|| {
                self.call
                    .as_ref()
                    .and_then(WritePublicationCall::received_committed_effect)
            })
    }
    pub(crate) fn committed(&self) -> bool {
        self.disposition() == Some(LakePublicationDisposition::KnownCommitted)
    }
    pub(crate) fn select(
        &self,
        cancellation: Option<QueryCancellationReason>,
    ) -> Result<(), QueryServiceError> {
        if let Some(error) = &self.refusal {
            return Err(super::write_action::publication_failure(error));
        }
        if let Some(needed) = &self.needed {
            return select_needed_publication(
                needed,
                self.local_finalization.as_ref(),
                cancellation,
            );
        }
        if let Some(outcome) = &self.outcome {
            return select_publication(outcome, self.local_finalization.as_ref(), cancellation);
        }
        if let Some(Err(error)) = &self.local_finalization {
            return Err(internal_error(error.clone()));
        }
        match &self.abort_outcome {
            Some(Err(error)) => Err(super::write_action::publication_failure(error)),
            Some(Ok(ConnectorWriteAbortOutcome::CommitUnknown { failure, .. })) => {
                Err(super::write_action::publication_failure(failure))
            }
            Some(Ok(ConnectorWriteAbortOutcome::KnownCommitted { needed, .. })) => {
                select_needed_publication(needed, self.local_finalization.as_ref(), cancellation)
            }
            _ => cancellation.map(cancellation_error).map_or(Ok(()), Err),
        }
    }
    pub(crate) fn freeze_terminal(&mut self) -> Result<(), QueryServiceError> {
        if self.attempt.terminal().is_some() {
            return Ok(());
        }
        let verdict = if self.needed.is_some() {
            Some(LakePublicationDisposition::KnownCommitted)
        } else if let Some(outcome) = self.outcome.as_ref() {
            Some(disposition(outcome))
        } else {
            self.abort_outcome.as_ref().map(|outcome| match outcome {
                Ok(ConnectorWriteAbortOutcome::KnownUncommitted { .. }) => {
                    LakePublicationDisposition::KnownUncommitted
                }
                Ok(ConnectorWriteAbortOutcome::KnownCommitted { .. }) => {
                    LakePublicationDisposition::KnownCommitted
                }
                _ => LakePublicationDisposition::CommitUnknown,
            })
        };
        let Some(verdict) = verdict else {
            // A refusal before any provider publication call has an exact
            // pre-dispatch verdict, even after local preparation was observed.
            if self.call.is_none() && self.finished {
                self.attempt
                    .terminal_pre_dispatch_uncommitted()
                    .map_err(|e| internal_error(e.to_string()))?;
                self.verdict = Some(LakePublicationDisposition::KnownUncommitted);
            }
            return Ok(());
        };
        let provider_finalization = self
            .needed
            .as_ref()
            .map(|needed| needed.finalization())
            .or_else(|| match (&self.outcome, &self.abort_outcome) {
                (
                    Some(Ok(ConnectorWritePublicationCompletion::KnownCommitted {
                        needed, ..
                    })),
                    _,
                ) => Some(needed.finalization()),
                (_, Some(Ok(ConnectorWriteAbortOutcome::KnownCommitted { needed, .. }))) => {
                    Some(needed.finalization())
                }
                _ => None,
            });
        let finalized = if provider_finalization
            .is_some_and(|f| matches!(f, ExternalMutationFinalization::Failed(_)))
            || self.local_finalization.as_ref().is_some_and(Result::is_err)
        {
            DmlPublicationFinalization::Failed
        } else {
            DmlPublicationFinalization::Succeeded
        };
        if let Some(token) = self.adjudication.take() {
            let fact = match verdict {
                LakePublicationDisposition::KnownCommitted => {
                    DmlPublicationAdjudicationOutcome::KnownCommitted
                }
                LakePublicationDisposition::KnownUncommitted => {
                    DmlPublicationAdjudicationOutcome::KnownUncommitted
                }
                LakePublicationDisposition::CommitUnknown => {
                    DmlPublicationAdjudicationOutcome::CommitUnknown
                }
            };
            self.attempt
                .finish_adjudication(
                    token,
                    fact,
                    if verdict == LakePublicationDisposition::KnownCommitted {
                        finalized
                    } else {
                        DmlPublicationFinalization::NotApplicable
                    },
                )
                .map_err(|e| internal_error(e.to_string()))?;
        } else {
            match verdict {
                LakePublicationDisposition::KnownUncommitted => {
                    self.attempt
                        .terminal_known_uncommitted()
                        .map_err(|e| internal_error(e.to_string()))?;
                }
                LakePublicationDisposition::KnownCommitted => {
                    self.attempt
                        .mark_dispatch_possible()
                        .map_err(|e| internal_error(e.to_string()))?;
                    self.attempt
                        .terminal_known_committed(finalized)
                        .map_err(|e| internal_error(e.to_string()))?;
                }
                LakePublicationDisposition::CommitUnknown => {
                    self.attempt
                        .mark_dispatch_possible()
                        .map_err(|e| internal_error(e.to_string()))?;
                    self.attempt
                        .terminal_commit_unknown()
                        .map_err(|e| internal_error(e.to_string()))?;
                }
            }
        }
        self.verdict = Some(verdict);
        Ok(())
    }
}

#[cfg(all(test, novarocks_dev_controls))]
mod tests {
    use super::*;
    use crate::query_execution::write_result::{RootWriteDecodeContract, RootWriteResultDecoder};
    use crate::query_execution::write_session::tests::{
        commit_fragment_bytes, fixture_with_outcome, known_committed, request_context,
    };
    use novarocks_spi::connector::ExternalMutationOutcome;
    use novarocks_spi::connector::write_stack::WriteTargetOrdinal;

    #[test]
    fn complete_receiving_segments_feed_one_original_session_and_one_actual_call() {
        let (_control, root, binding, _capacity) =
            crate::query_execution::internal_result_cpu::admitted_internal_fixture();
        let fixture = fixture_with_outcome(1, 16, known_committed());
        let target = WriteTargetOrdinal::try_new(0).unwrap();
        for _ in 0..2 {
            let mut decoder = RootWriteResultDecoder::new_with_capacity(
                RootWriteDecodeContract::try_new(&[target], fixture.session.targets()).unwrap(),
                &binding,
            )
            .unwrap();
            decoder
                .apply_relay_body(
                    &crate::query_execution::write_result::tests::component_write_body(
                        commit_fragment_bytes(),
                    ),
                )
                .unwrap();
            // A body/summary prefix cannot authorize a publication.
            assert_eq!(fixture.recorded.lock().unwrap().finish, 0);
            decoder.observe_root_eof().unwrap();
            let decoded = fixture
                .session
                .computation_decode(&[target])
                .unwrap()
                .interpret(decoder.finish().unwrap())
                .unwrap();
            let accepted = fixture.session.receiving_port().accept(decoded).unwrap();
            assert!(accepted.belongs_to(&fixture.session));
            assert_eq!(accepted.row_count(), 3);
        }
        assert_eq!(fixture.session.accumulated_row_count().unwrap(), 6);
        let mut publication = StatementPublication::new(original_attempt());
        let mut cx = Context::from_waker(std::task::Waker::noop());
        assert!(
            publication
                .poll_settle(Some(&fixture.session), &request_context(), false, &mut cx)
                .is_ready()
        );
        assert!(
            publication
                .select(Some(QueryCancellationReason::ClientDisconnected))
                .is_ok(),
            "an actual Applied result survives late cancel"
        );
        publication.freeze_terminal().unwrap();
        assert_eq!(
            publication.attempt.terminal().unwrap().disposition(),
            LakePublicationDisposition::KnownCommitted
        );
        assert_eq!(fixture.recorded.lock().unwrap().finish, 1);
        assert_eq!(fixture.session.accumulated_row_count().unwrap(), 6);
        assert!(
            publication
                .poll_settle(Some(&fixture.session), &request_context(), false, &mut cx)
                .is_ready()
        );
        assert_eq!(
            fixture.recorded.lock().unwrap().finish,
            1,
            "settled original Call is never reissued"
        );
        drop(publication);
        drop(fixture);
        drop(binding);
        root.owner.complete();
    }
    #[test]
    fn actual_needed_ready_returns_before_gc_and_same_publication_retires_later() {
        use crate::query_execution::write_result::DecodedPreparedWriteSet;
        let fixture = fixture_with_outcome(1, 16, known_committed());
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
        fixture.recorded.lock().unwrap().defer_finish_cleanup = true;
        let context = request_context();
        struct OriginalFinalizationScope;
        let scope = context.request_scope_extension_or_insert_with(|| OriginalFinalizationScope);
        fixture.recorded.lock().unwrap().finalization_context = Some(Ok(Some(context.clone())));
        let mut publication = StatementPublication::new(original_attempt());
        let mut cx = Context::from_waker(std::task::Waker::noop());
        assert!(
            publication
                .poll_settle(Some(&fixture.session), &request_context(), false, &mut cx)
                .is_ready()
        );
        assert!(
            publication.outcome.is_none(),
            "whole Call is still owned and pending"
        );
        let ready = publication.needed.as_ref().unwrap().clone();
        let received = publication
            .existing_finalization_context()
            .unwrap()
            .unwrap();
        assert_eq!(received.deadline(), context.deadline());
        assert!(Arc::ptr_eq(
            &scope,
            &received.request_scope_extension_or_insert_with(|| OriginalFinalizationScope)
        ));
        assert_eq!(
            fixture.recorded.lock().unwrap().finalization_context_reads,
            1
        );
        fixture.recorded.lock().unwrap().finalization_context = Some(Err(ConnectorError::new(
            novarocks_spi::connector::ConnectorErrorKind::Internal,
            "a second finalization-context lookup must not occur",
        )));
        assert_eq!(fixture.recorded.lock().unwrap().cleanup_started, 0);
        assert_eq!(fixture.recorded.lock().unwrap().finish_future_drops, 0);
        publication.freeze_terminal().unwrap();
        assert!(
            publication
                .select(Some(QueryCancellationReason::ClientDisconnected))
                .is_ok()
        );
        assert!(publication.poll_retirement(&mut cx).is_pending());
        assert_eq!(fixture.recorded.lock().unwrap().cleanup_started, 1);
        assert!(
            publication
                .poll_settle(Some(&fixture.session), &request_context(), true, &mut cx)
                .is_ready()
        );
        assert_eq!(fixture.recorded.lock().unwrap().finish, 1);
        {
            let mut state = fixture.recorded.lock().unwrap();
            state.fail_safe_cleanup = true;
            state.allow_cleanup_exit = true;
        }
        assert!(publication.poll_retirement(&mut cx).is_ready());
        assert_eq!(fixture.recorded.lock().unwrap().finish_future_drops, 1);
        assert_eq!(
            fixture.recorded.lock().unwrap().finalization_context_reads,
            1
        );
        let received = publication
            .existing_finalization_context()
            .unwrap()
            .unwrap();
        assert!(Arc::ptr_eq(
            &scope,
            &received.request_scope_extension_or_insert_with(|| OriginalFinalizationScope)
        ));
        let Some(Ok(ConnectorWritePublicationCompletion::KnownCommitted { needed, cleanup })) =
            publication.outcome.as_ref()
        else {
            panic!("actual original completion");
        };
        assert!(Arc::ptr_eq(&ready, needed));
        assert!(matches!(
            cleanup,
            novarocks_spi::connector::write_stack::ConnectorWriteCleanupState::Observed(
                ExternalMutationFinalization::Failed(_)
            )
        ));
        assert!(
            publication
                .select(Some(QueryCancellationReason::ClientDisconnected))
                .is_ok()
        );
        assert_eq!(
            publication.attempt.terminal().unwrap().disposition(),
            LakePublicationDisposition::KnownCommitted
        );
    }
    #[test]
    fn original_pre_dispatch_refusal_aborts_same_session_once() {
        let fixture = fixture_with_outcome(1, 16, known_committed());
        let mut publication = StatementPublication::new(original_attempt());
        let mut cx = Context::from_waker(std::task::Waker::noop());
        assert!(
            publication
                .poll_settle(Some(&fixture.session), &request_context(), true, &mut cx)
                .is_ready()
        );
        publication.freeze_terminal().unwrap();
        assert_eq!(fixture.recorded.lock().unwrap().finish, 0);
        assert_eq!(fixture.recorded.lock().unwrap().abort, 1);
        assert_eq!(
            publication.attempt.terminal().unwrap().disposition(),
            LakePublicationDisposition::KnownUncommitted
        );
        assert!(
            publication
                .poll_settle(Some(&fixture.session), &request_context(), true, &mut cx)
                .is_ready()
        );
        assert_eq!(fixture.recorded.lock().unwrap().abort, 1);
    }
    #[test]
    fn original_abort_call_preserves_exact_applied_or_noop_fact() {
        use novarocks_spi::connector::ExternalMutationEffect;
        for effect in [
            ExternalMutationEffect::Applied,
            ExternalMutationEffect::NoOp,
        ] {
            let fixture = fixture_with_outcome(1, 16, known_committed());
            let ExternalMutationOutcome::KnownCommitted {
                receipt,
                finalization,
                ..
            } = known_committed()
            else {
                panic!("actual scripted committed fact");
            };
            let needed = Arc::new(ConnectorWriteNeededReady::new(
                effect,
                receipt,
                finalization,
            ));
            fixture.recorded.lock().unwrap().abort_outcome =
                Some(ConnectorWriteAbortOutcome::KnownCommitted {
                    needed: Arc::clone(&needed),
                    cleanup:
                        novarocks_spi::connector::write_stack::ConnectorWriteCleanupState::Observed(
                            ExternalMutationFinalization::Complete,
                        ),
                });
            let mut publication = StatementPublication::new(original_attempt());
            assert!(publication.committed_effect().is_none());
            let mut cx = Context::from_waker(std::task::Waker::noop());
            assert!(
                publication
                    .poll_settle(Some(&fixture.session), &request_context(), true, &mut cx)
                    .is_ready()
            );
            let Some(Ok(ConnectorWriteAbortOutcome::KnownCommitted {
                needed: returned, ..
            })) = &publication.abort_outcome
            else {
                panic!("actual original abort Call result");
            };
            assert!(Arc::ptr_eq(&needed, returned));
            match (effect, publication.committed_effect()) {
                (ExternalMutationEffect::Applied, Some(CommittedWriteEffect::Applied(receipt))) => {
                    assert!(std::ptr::eq(receipt, needed.receipt()));
                    assert!(
                        publication
                            .select(Some(QueryCancellationReason::ClientDisconnected))
                            .is_ok()
                    );
                }
                (ExternalMutationEffect::NoOp, Some(CommittedWriteEffect::NoOp)) => {
                    assert!(
                        publication
                            .select(Some(QueryCancellationReason::ClientDisconnected))
                            .is_err()
                    );
                }
                _ => panic!("exact effect erased"),
            }
            publication.freeze_terminal().unwrap();
            assert!(publication.poll_retirement(&mut cx).is_ready());
            assert!(
                publication
                    .poll_settle(Some(&fixture.session), &request_context(), true, &mut cx)
                    .is_ready()
            );
            assert_eq!(fixture.recorded.lock().unwrap().abort, 1);
            assert_eq!(fixture.recorded.lock().unwrap().finish, 0);
        }
    }

    #[test]
    fn direct_provider_release_keeps_pending_until_same_original_gc_exits() {
        use crate::query_execution::write_result::DecodedPreparedWriteSet;
        use novarocks_spi::connector::write_stack::ConnectorWriteCleanupState;
        use novarocks_spi::connector::{
            ConnectorMetadataCleanupResponsibility, ExternalMutationEffect,
        };
        use std::future::Future;
        for effect in [
            ExternalMutationEffect::Applied,
            ExternalMutationEffect::NoOp,
        ] {
            let (_control, root, binding, _capacity) =
                crate::query_execution::internal_result_cpu::admitted_internal_fixture();
            let ExternalMutationOutcome::KnownCommitted {
                receipt,
                finalization,
                cleanup,
                ..
            } = known_committed()
            else {
                unreachable!()
            };
            let fixture = fixture_with_outcome(
                1,
                16,
                ExternalMutationOutcome::KnownCommitted {
                    effect,
                    receipt,
                    finalization,
                    cleanup,
                },
            );
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
            {
                let mut state = fixture.recorded.lock().unwrap();
                state.defer_finish_cleanup = true;
                state.enforce_cleanup_responsibility = true;
            }
            let context = request_context();
            let mut publication = StatementPublication::new(original_attempt());
            let mut cx = Context::from_waker(std::task::Waker::noop());
            assert!(
                publication
                    .poll_settle(Some(&fixture.session), &context, false, &mut cx)
                    .is_ready()
            );
            let needed = Arc::clone(publication.needed.as_ref().unwrap());
            assert_eq!(needed.effect(), effect);
            assert_eq!(
                fixture.session.cleanup_responsibility().unwrap(),
                ConnectorMetadataCleanupResponsibility::Retained
            );
            let mut release = std::pin::pin!(fixture.release_provider_direct(context.clone()));
            let Poll::Ready(Ok(ConnectorWriteAbortOutcome::KnownCommitted {
                needed: released,
                cleanup,
            })) = release.as_mut().poll(&mut cx)
            else {
                panic!("actual provider release result");
            };
            assert!(Arc::ptr_eq(&needed, &released));
            assert!(matches!(cleanup, ConnectorWriteCleanupState::Pending));
            assert!(publication.poll_retirement(&mut cx).is_pending());
            assert_eq!(fixture.recorded.lock().unwrap().finish, 1);
            fixture.recorded.lock().unwrap().allow_cleanup_exit = true;
            assert!(publication.poll_retirement(&mut cx).is_ready());
            let mut release = std::pin::pin!(fixture.release_provider_direct(context.clone()));
            let Poll::Ready(Ok(ConnectorWriteAbortOutcome::KnownCommitted {
                needed: released,
                cleanup,
            })) = release.as_mut().poll(&mut cx)
            else {
                panic!("actual provider release result");
            };
            assert!(Arc::ptr_eq(&needed, &released));
            assert!(matches!(
                cleanup,
                ConnectorWriteCleanupState::Observed(ExternalMutationFinalization::Complete)
            ));
            assert_eq!(
                fixture.session.cleanup_responsibility().unwrap(),
                ConnectorMetadataCleanupResponsibility::Disposed
            );
            assert_eq!(fixture.recorded.lock().unwrap().finish, 1);
            drop(binding);
            root.owner.complete();
        }
    }
}

#[cfg(test)]
fn original_attempt() -> DmlPublicationAttempt {
    DmlPublicationAttempt::new(
        novarocks_spi::connector::LakePublicationId::new_v7(),
        novarocks_spi::connector::LakePublicationFamily::DataMutation,
        novarocks_spi::connector::LakePublicationTarget::try_new(
            "catalog".to_owned(),
            "namespace".to_owned(),
            Some("table".to_owned()),
            None,
        )
        .unwrap(),
        None,
    )
}

#[cfg(test)]
mod selection_migration_tests {
    use super::*;
    use crate::query_execution::write_session::tests::{
        fixture_with_outcome, known_committed, request_context,
    };
    fn assert_original_rows_reach_actual_selection(
        outcome: novarocks_spi::connector::ExternalMutationOutcome<
            novarocks_spi::connector::ConnectorWriteReceipt,
        >,
        expected: LakePublicationDisposition,
        selected_rows: Option<u64>,
        cancellation: Option<QueryCancellationReason>,
    ) {
        let fixture = fixture_with_outcome(1, 16, outcome);
        let prepared =
            crate::query_execution::write_result::DecodedPreparedWriteSet::for_test(7, Vec::new());
        let decoded = fixture
            .session
            .computation_decode(&fixture.session.expected_targets())
            .unwrap()
            .interpret(prepared)
            .unwrap();
        let accepted = fixture.session.receiving_port().accept(decoded).unwrap();
        assert!(accepted.belongs_to(&fixture.session));
        assert_eq!(accepted.row_count(), 7);
        assert_eq!(fixture.recorded.lock().unwrap().finish, 0);
        let rows = fixture.session.freeze_received_row_count().unwrap();
        assert_eq!(rows, accepted.row_count());
        assert_eq!(fixture.session.finish_invocations(), 0);
        let mut publication = StatementPublication::new(original_attempt());
        let mut cx = Context::from_waker(std::task::Waker::noop());
        assert!(
            publication
                .poll_settle(Some(&fixture.session), &request_context(), false, &mut cx)
                .is_ready()
        );
        assert_eq!(publication.disposition(), Some(expected));
        let completion = super::super::write_action::selected_command_completion(
            publication.select(cancellation),
            Some(rows),
        );
        match selected_rows {
            Some(expected_rows) => {
                let novarocks_query_application::session_control::StatementActionValue::Ok(rows) =
                    completion.expect("only a known committed write selects success")
                else {
                    panic!("publication success must preserve command metadata")
                };
                assert_eq!(rows.affected_rows(), expected_rows);
            }
            _ => assert!(
                completion.is_err(),
                "an uncommitted or unknown write cannot expose successful row metadata"
            ),
        }
        assert_eq!(
            fixture.recorded.lock().unwrap().finish_rows,
            accepted.row_count()
        );
        assert_eq!(fixture.session.finish_invocations(), 1);
        assert_eq!(fixture.recorded.lock().unwrap().finish, 1);
    }
    #[test]
    fn affected_rows_are_reported_only_after_a_known_successful_commit() {
        assert_original_rows_reach_actual_selection(
            known_committed(),
            LakePublicationDisposition::KnownCommitted,
            Some(7),
            None,
        );
    }
    #[test]
    fn a_commit_unknown_outcome_reports_no_affected_rows() {
        assert_original_rows_reach_actual_selection(
            crate::query_execution::write_session::tests::commit_unknown(),
            LakePublicationDisposition::CommitUnknown,
            None,
            None,
        );
    }
    #[test]
    fn a_known_uncommitted_commit_reports_no_affected_rows() {
        assert_original_rows_reach_actual_selection(
            novarocks_spi::connector::ExternalMutationOutcome::KnownUncommitted {
                failure: novarocks_spi::connector::ConnectorMutationFailure::new(
                    novarocks_spi::connector::ConnectorMutationFailureKind::Unavailable,
                    "scripted rejection",
                ),
                cleanup: ExternalMutationFinalization::Complete,
            },
            LakePublicationDisposition::KnownUncommitted,
            None,
            None,
        );
    }
    #[test]
    fn applied_late_cancel_keeps_the_original_count() {
        assert_original_rows_reach_actual_selection(
            known_committed(),
            LakePublicationDisposition::KnownCommitted,
            Some(7),
            Some(QueryCancellationReason::ClientDisconnected),
        );
    }
    #[test]
    fn committed_necessary_finalization_failure_exposes_no_successful_count() {
        let mut outcome = known_committed();
        let novarocks_spi::connector::ExternalMutationOutcome::KnownCommitted {
            finalization, ..
        } = &mut outcome
        else {
            unreachable!()
        };
        *finalization = ExternalMutationFinalization::Failed(
            novarocks_spi::connector::ConnectorMutationFailure::new(
                novarocks_spi::connector::ConnectorMutationFailureKind::Unavailable,
                "original necessary finalization failed",
            ),
        );
        assert_original_rows_reach_actual_selection(
            outcome,
            LakePublicationDisposition::KnownCommitted,
            None,
            None,
        );
    }
}
