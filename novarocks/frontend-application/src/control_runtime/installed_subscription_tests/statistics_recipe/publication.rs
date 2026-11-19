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

//! One real decoded artifact enters the production publication author.
//! The provider is a finite session, not a Worker or catalog implementation.

use super::*;
use crate::fixtures::native_recipe::ReadCloseGate;
use crate::statistics::attempt_executor::publication_fixture::OriginalStatisticsPublication;
use novarocks_spi::connector::{
    AsyncStatisticsCollectionSession, ConnectorError, ConnectorInstanceDescriptor,
    ConnectorMutationOperationId, ConnectorProviderId, ConnectorRequestContext,
    ExternalMutationEffect, ExternalMutationFinalization, ProviderBindingEpoch,
    StatisticsArtifactDraft, StatisticsDataVersion, StatisticsEvidenceRevision,
    StatisticsPublicationClose, StatisticsPublicationReady, StatisticsReceipt,
    StatisticsSessionFuture,
};
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Default)]
struct PublicationObservation {
    prepares: AtomicUsize,
    may_dispatch: AtomicUsize,
    commits: AtomicUsize,
    closes: AtomicUsize,
}

struct ArtifactSession {
    descriptor: ConnectorInstanceDescriptor,
    expected: Vec<StatisticsArtifactIdentity>,
    original_body: usize,
    artifacts: Option<Vec<StatisticsArtifactDraft>>,
    observation: Arc<PublicationObservation>,
    finish_gate: Option<tokio::sync::oneshot::Receiver<()>>,
}
impl ArtifactSession {
    fn receipt(&self) -> StatisticsReceipt {
        StatisticsReceipt::try_new(
            self.descriptor.clone(),
            self.incarnation(),
            self.operation_id(),
            StatisticsDataVersion::try_new(Bytes::from_static(b"original-version-7")).unwrap(),
            StatisticsEvidenceRevision::try_new(Bytes::from_static(b"published-artifact-1"))
                .unwrap(),
            Bytes::from_static(b"original publication receipt"),
        )
        .unwrap()
    }
}
impl AsyncStatisticsCollectionSession for ArtifactSession {
    fn descriptor(&self) -> &ConnectorInstanceDescriptor {
        &self.descriptor
    }
    fn incarnation(&self) -> ProviderBindingEpoch {
        ProviderBindingEpoch::from_bytes([31; 16])
    }
    fn operation_id(&self) -> ConnectorMutationOperationId {
        ConnectorMutationOperationId::from_bytes([32; 16])
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
            let original = artifacts.as_ref().expect("actual decoded artifacts");
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
                self.observation.may_dispatch.fetch_add(1, Ordering::AcqRel),
                0
            );
            self.finish_gate
                .take()
                .expect("same first dispatch gate")
                .await
                .unwrap();
            assert_eq!(self.observation.commits.fetch_add(1, Ordering::AcqRel), 0);
            Ok(StatisticsPublicationReady::Committed {
                effect: ExternalMutationEffect::Applied,
                receipt: self.receipt(),
                finalization: ExternalMutationFinalization::Complete,
            })
        })
    }
    fn close_publication_async<'a>(
        &'a mut self,
        _: &'a ConnectorRequestContext,
    ) -> StatisticsSessionFuture<'a, StatisticsPublicationClose> {
        // This first slice never calls provider close. Production close_session
        // additionally needs its actual PlanSlot and completion custody.
        Box::pin(async move {
            self.observation.closes.fetch_add(1, Ordering::AcqRel);
            Err(ConnectorError::new(
                novarocks_spi::connector::ConnectorErrorKind::Unsupported,
                "the publication-only fixture has no provider close owner",
            ))
        })
    }
}

pub(super) fn hold_cleanup(
    run: &mut ActualStatisticsRun,
) -> crate::fixtures::native_recipe::ReadCloseRelease {
    let gate = Arc::new(ReadCloseGate::default());
    *run.logical
        .as_ref()
        .unwrap()
        .probe
        .read_close_gate
        .lock()
        .unwrap() = Some(gate.clone());
    run.publication_cleanup_gate = Some(gate.clone());
    gate.release_guard()
}

pub(super) async fn publish_while_cleanup_pending(run: &mut ActualStatisticsRun) {
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
    .expect("original Native read close reached its actual callback");
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

    let observation = Arc::new(PublicationObservation::default());
    let (release, finish_gate) = tokio::sync::oneshot::channel();
    let ready = run.ready.take().expect("actual statistics Ready");
    let session = ArtifactSession {
        descriptor: ConnectorInstanceDescriptor {
            provider_id: ConnectorProviderId::parse("iceberg").unwrap(),
            instance_id: ConnectorInstanceId::parse("statistics_component").unwrap(),
        },
        expected: vec![ready.artifacts[0].identity().clone()],
        original_body: ready.artifacts[0].body().as_ptr() as usize,
        artifacts: None,
        observation: observation.clone(),
        finish_gate: Some(finish_gate),
    };
    let expected_receipt = session.receipt();
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
    run.publication_future = Some(run.publication.as_ref().unwrap().start(context));
    assert!(futures::poll!(run.publication_future.as_mut().unwrap().as_mut()).is_pending());
    assert_eq!(observation.prepares.load(Ordering::Acquire), 1);
    assert_eq!(observation.may_dispatch.load(Ordering::Acquire), 1);
    assert_eq!(observation.commits.load(Ordering::Acquire), 0);
    // publish_session holds the original session lock across this borrowed
    // provider future; do not read its slot until that future has returned.
    release.send(()).unwrap();
    run.publication_future
        .as_mut()
        .unwrap()
        .as_mut()
        .await
        .unwrap();
    drop(run.publication_future.take());
    let original = run.publication.as_ref().unwrap();
    assert_eq!(
        original.publication().await,
        Some(StatisticsPublicationReady::Committed {
            effect: ExternalMutationEffect::Applied,
            receipt: expected_receipt,
            finalization: ExternalMutationFinalization::Complete,
        })
    );
    assert!(matches!(
        original.outcome().await,
        Some(
            crate::statistics::publication::StatisticsPublicationOutcome::KnownCommitted {
                finalization_failure: None,
                cleanup_failure: None,
            }
        )
    ));
    assert_eq!(observation.commits.load(Ordering::Acquire), 1);
    assert_eq!(observation.closes.load(Ordering::Acquire), 0);
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
    run.ready = Some(original.take_ready().await);
    assert!(run.ready.as_ref().unwrap().artifacts.is_empty());
    drop(run.ready.take());
    // Releasing this callback is permission to finish the SAME original
    // cleanup, not a fabricated exit. The parent completes all actual joins.
    gate.release();
}
