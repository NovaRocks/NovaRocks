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

//! Nonempty decoded payloads use the original statistics publication author.
//! The finite session authors explicit verdicts; missing receipts are not facts.

use super::*;
use crate::statistics::attempt_executor::publication_fixture::OriginalStatisticsPublication;
use crate::statistics::publication::StatisticsPublicationOutcome;
use novarocks_failure_contract::FailureProjection;
use novarocks_spi::connector::{
    AsyncStatisticsCollectionSession, ConnectorError, ConnectorErrorKind,
    ConnectorInstanceDescriptor, ConnectorMutationFailure, ConnectorMutationOperationId,
    ConnectorProviderId, ConnectorRequestContext, ExternalMutationEffect, ExternalMutationEvidence,
    ExternalMutationFinalization, ExternalMutationOutcome, ProviderBindingEpoch,
    StatisticsArtifactDraft, StatisticsDataVersion, StatisticsEvidenceRevision,
    StatisticsPublicationClose, StatisticsPublicationReady, StatisticsReceipt,
    StatisticsSessionFuture,
};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PublicationOutcomeCase {
    CommittedFinalizationFailed,
    Uncommitted,
    Unknown,
}
#[derive(Default)]
struct Observation {
    prepares: AtomicUsize,
    dispatches: AtomicUsize,
    finishes: AtomicUsize,
    closes: AtomicUsize,
    disposed: AtomicBool,
}
struct Session {
    descriptor: ConnectorInstanceDescriptor,
    expected: Vec<StatisticsArtifactIdentity>,
    original_body: usize,
    artifacts: Option<Vec<StatisticsArtifactDraft>>,
    ready: StatisticsPublicationReady,
    close: StatisticsPublicationClose,
    finish_gate: Option<tokio::sync::oneshot::Receiver<()>>,
    close_gate: Option<tokio::sync::oneshot::Receiver<()>>,
    observation: Arc<Observation>,
}
impl AsyncStatisticsCollectionSession for Session {
    fn descriptor(&self) -> &ConnectorInstanceDescriptor {
        &self.descriptor
    }
    fn incarnation(&self) -> ProviderBindingEpoch {
        ProviderBindingEpoch::from_bytes([41; 16])
    }
    fn operation_id(&self) -> ConnectorMutationOperationId {
        ConnectorMutationOperationId::from_bytes([42; 16])
    }
    fn expectations(&self) -> &[StatisticsArtifactIdentity] {
        &self.expected
    }
    fn prepare_publication_async<'a>(
        &'a mut self,
        artifacts: &'a mut Option<Vec<StatisticsArtifactDraft>>,
    ) -> StatisticsSessionFuture<'a, ()> {
        Box::pin(async move {
            assert_eq!(self.observation.prepares.fetch_add(1, Ordering::AcqRel), 0);
            let original = artifacts.as_ref().expect("actual decoded statistics");
            assert_eq!(original.len(), 1);
            assert_eq!(original[0].identity(), &self.expected[0]);
            assert_eq!(original[0].body().as_ptr() as usize, self.original_body);
            assert_eq!(original[0].body().as_ref(), b"original artifact");
            assert!(self.artifacts.is_none());
            self.artifacts = artifacts.take();
            Ok(())
        })
    }
    fn finish_publication_async(
        &mut self,
    ) -> StatisticsSessionFuture<'_, StatisticsPublicationReady> {
        Box::pin(async move {
            assert!(self.artifacts.is_some());
            assert_eq!(
                self.observation.dispatches.fetch_add(1, Ordering::AcqRel),
                0
            );
            self.finish_gate
                .take()
                .expect("same first dispatch")
                .await
                .unwrap();
            assert_eq!(self.observation.finishes.fetch_add(1, Ordering::AcqRel), 0);
            Ok(self.ready.clone())
        })
    }
    fn close_publication_async<'a>(
        &'a mut self,
        _: &'a ConnectorRequestContext,
    ) -> StatisticsSessionFuture<'a, StatisticsPublicationClose> {
        Box::pin(async move {
            assert_eq!(self.observation.finishes.load(Ordering::Acquire), 1);
            assert_eq!(self.observation.closes.fetch_add(1, Ordering::AcqRel), 0);
            self.close_gate
                .take()
                .expect("same publication close")
                .await
                .unwrap();
            // These are the finite session's actual retained decoded artifacts.
            // This makes no claim about a catalog file or CompletionSource exit.
            drop(
                self.artifacts
                    .take()
                    .expect("same retained artifact vector"),
            );
            self.observation.disposed.store(true, Ordering::Release);
            Ok(self.close.clone())
        })
    }
}

fn original_verdicts(
    case: PublicationOutcomeCase,
    descriptor: &ConnectorInstanceDescriptor,
    cause: &ConnectorMutationFailure,
) -> (StatisticsPublicationReady, StatisticsPublicationClose) {
    let outcome = match case {
        PublicationOutcomeCase::CommittedFinalizationFailed => {
            ExternalMutationOutcome::KnownCommitted {
                effect: ExternalMutationEffect::Applied,
                receipt: StatisticsReceipt::try_new(
                    descriptor.clone(),
                    ProviderBindingEpoch::from_bytes([41; 16]),
                    ConnectorMutationOperationId::from_bytes([42; 16]),
                    StatisticsDataVersion::try_new(Bytes::from_static(b"original-version-7"))
                        .unwrap(),
                    StatisticsEvidenceRevision::try_new(Bytes::from_static(
                        b"original-finalization-1",
                    ))
                    .unwrap(),
                    Bytes::from_static(b"same committed receipt"),
                )
                .unwrap(),
                finalization: ExternalMutationFinalization::Failed(cause.clone()),
                cleanup: ExternalMutationFinalization::Complete,
            }
        }
        PublicationOutcomeCase::Uncommitted => ExternalMutationOutcome::KnownUncommitted {
            failure: cause.clone(),
            cleanup: ExternalMutationFinalization::Complete,
        },
        PublicationOutcomeCase::Unknown => ExternalMutationOutcome::CommitUnknown {
            failure: cause.clone(),
            evidence: ExternalMutationEvidence::try_new(
                1,
                descriptor.clone(),
                ProviderBindingEpoch::from_bytes([41; 16]),
                ConnectorMutationOperationId::from_bytes([42; 16]),
                "statistics-publish",
                Bytes::from_static(b"same unresolved original publication"),
            )
            .unwrap(),
        },
    };
    let ready = match &outcome {
        ExternalMutationOutcome::KnownCommitted {
            effect,
            receipt,
            finalization,
            ..
        } => StatisticsPublicationReady::Committed {
            effect: *effect,
            receipt: receipt.clone(),
            finalization: finalization.clone(),
        },
        _ => StatisticsPublicationReady::Settled(outcome.clone()),
    };
    (
        ready,
        StatisticsPublicationClose::Observed {
            outcome,
            owned_objects_disposed: true,
        },
    )
}

pub(super) async fn publish_outcome(run: &mut ActualStatisticsRun, case: PublicationOutcomeCase) {
    let gate = run.publication_cleanup_gate.as_ref().unwrap().clone();
    drive_until_with_original_clock(
        &run.process,
        &mut run.typed.as_mut().unwrap().driver,
        std::future::poll_fn(|_| {
            if gate.entered() {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        }),
        Instant::now() + Duration::from_secs(5),
        None,
    )
    .await
    .expect("original Native read close entered");
    assert_eq!(gate.calls(), 1);
    assert!(
        !*run
            .installed
            .as_ref()
            .unwrap()
            .fixture_exit
            .observe_actual_exit()
            .borrow()
    );

    let observation = Arc::new(Observation::default());
    let (finish, finish_gate) = tokio::sync::oneshot::channel();
    let (close, close_gate) = tokio::sync::oneshot::channel();
    let descriptor = ConnectorInstanceDescriptor {
        provider_id: ConnectorProviderId::parse("iceberg").unwrap(),
        instance_id: ConnectorInstanceId::parse("statistics_component").unwrap(),
    };
    let cause = ConnectorMutationFailure::from_connector(ConnectorError::new(
        ConnectorErrorKind::Unavailable,
        "original statistics publication cause",
    ));
    assert!(cause.connector_source().is_some());
    let (expected_ready, expected_close) = original_verdicts(case, &descriptor, &cause);
    let ready = run
        .ready
        .take()
        .expect("same real nonempty statistics Ready");
    let session = Session {
        descriptor,
        expected: vec![ready.artifacts[0].identity().clone()],
        original_body: ready.artifacts[0].body().as_ptr() as usize,
        artifacts: None,
        ready: expected_ready.clone(),
        close: expected_close.clone(),
        finish_gate: Some(finish_gate),
        close_gate: Some(close_gate),
        observation: observation.clone(),
    };
    run.publication = Some(OriginalStatisticsPublication::new(Box::new(session), ready));
    let context = crate::connector::connector_request_context_for_query_on_runtime(
        &tokio::runtime::Handle::current(),
        None,
        QueryCancellationView::governed(
            run.typed
                .as_ref()
                .unwrap()
                .root
                .owner
                .scope()
                .cancellation()
                .unwrap(),
            None,
        ),
    )
    .unwrap();
    run.publication_future = Some(run.publication.as_ref().unwrap().start(context.clone()));
    assert!(futures::poll!(run.publication_future.as_mut().unwrap().as_mut()).is_pending());
    assert_eq!(observation.prepares.load(Ordering::Acquire), 1);
    assert_eq!(observation.dispatches.load(Ordering::Acquire), 1);
    assert_eq!(observation.finishes.load(Ordering::Acquire), 0);
    assert_eq!(observation.closes.load(Ordering::Acquire), 0);
    finish.send(()).unwrap();
    run.publication_future
        .as_mut()
        .unwrap()
        .as_mut()
        .await
        .unwrap();
    drop(run.publication_future.take());
    let original = run.publication.as_ref().unwrap();
    assert_eq!(original.publication().await, Some(expected_ready.clone()));
    let selected = original
        .outcome()
        .await
        .expect("same production outcome projection");
    match (&selected, case) {
        (
            StatisticsPublicationOutcome::KnownCommitted {
                finalization_failure: Some(original),
                cleanup_failure: None,
            },
            PublicationOutcomeCase::CommittedFinalizationFailed,
        )
        | (
            StatisticsPublicationOutcome::KnownUncommitted {
                failure: original,
                cleanup_failure: None,
            },
            PublicationOutcomeCase::Uncommitted,
        )
        | (
            StatisticsPublicationOutcome::CommitUnknown { failure: original },
            PublicationOutcomeCase::Unknown,
        ) => {
            assert_eq!(original.fact(), cause.failure_fact());
            assert_eq!(original.local_diagnostic(), cause.to_string());
        }
        _ => panic!("publication keeps the exact original verdict and finalization"),
    }
    assert!(!observation.disposed.load(Ordering::Acquire));
    run.session_publication_close_future = Some(original.start_session_publication_close(context));
    assert!(
        futures::poll!(
            run.session_publication_close_future
                .as_mut()
                .unwrap()
                .as_mut()
        )
        .is_pending()
    );
    assert_eq!(observation.closes.load(Ordering::Acquire), 1);
    assert!(!observation.disposed.load(Ordering::Acquire));
    assert!(
        !*run
            .installed
            .as_ref()
            .unwrap()
            .fixture_exit
            .observe_actual_exit()
            .borrow()
    );
    assert!(
        futures::poll!(std::future::poll_fn(|cx| {
            run.logical
                .as_mut()
                .unwrap()
                .exit
                .as_mut()
                .unwrap()
                .poll_exit(cx)
        }))
        .is_pending()
    );
    close.send(()).unwrap();
    let actual_close = run
        .session_publication_close_future
        .as_mut()
        .unwrap()
        .as_mut()
        .await
        .unwrap();
    drop(run.session_publication_close_future.take());
    assert_eq!(actual_close, expected_close);
    assert!(observation.disposed.load(Ordering::Acquire));
    let original = run.publication.as_ref().unwrap();
    assert_eq!(original.publication().await, Some(expected_ready));
    assert_eq!(
        original.outcome().await,
        Some(selected),
        "local disposal cannot rewrite the selected verdict"
    );
    assert_eq!(observation.dispatches.load(Ordering::Acquire), 1);
    assert_eq!(observation.finishes.load(Ordering::Acquire), 1);
    assert_eq!(observation.closes.load(Ordering::Acquire), 1);
    run.ready = Some(original.take_ready().await);
    assert!(run.ready.as_ref().unwrap().artifacts.is_empty());
    drop(run.ready.take());
    gate.release();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn actual_statistics_committed_publication_keeps_original_finalization_failure() {
    run_actual_statistics_case(StatisticsRecipeCase::PublicationOutcome(
        PublicationOutcomeCase::CommittedFinalizationFailed,
    ))
    .await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn actual_statistics_uncommitted_publication_requires_its_original_positive_verdict() {
    run_actual_statistics_case(StatisticsRecipeCase::PublicationOutcome(
        PublicationOutcomeCase::Uncommitted,
    ))
    .await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn actual_statistics_unknown_publication_keeps_original_evidence_after_local_disposal() {
    run_actual_statistics_case(StatisticsRecipeCase::PublicationOutcome(
        PublicationOutcomeCase::Unknown,
    ))
    .await;
}
