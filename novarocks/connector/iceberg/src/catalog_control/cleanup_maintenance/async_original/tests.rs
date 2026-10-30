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

//! Source/custody regressions; these do not qualify physical CPU or I/O grants.
use super::*;
use novarocks_spi::connector::{
    ConnectorCleanupOperationId, ConnectorOriginalResultScopeCheck, ConnectorTableIdentity,
    OriginalResultCheckError, ProviderMetadataCpuInput,
};
use std::{
    sync::atomic::{AtomicUsize, Ordering},
    time::Instant,
};
struct UnreachedScope;
impl ConnectorOriginalResultScopeCheck for UnreachedScope {
    fn original_deadline(&self) -> Instant {
        panic!("negative fixture has no deadline authority")
    }
    fn check_active(&self) -> Result<(), OriginalResultCheckError> {
        panic!("negative fixture has no Work authority")
    }
}
#[derive(Default)]
struct NoPhysicalCpu {
    closes: AtomicUsize,
}
impl ProviderMetadataCpuPort for NoPhysicalCpu {
    fn submit_metadata_in(
        &self,
        _: &mut Option<ProviderMetadataCpuInput>,
    ) -> Result<(), ConnectorError> {
        panic!("negative fixture cannot dispatch CPU")
    }
    fn poll_finished(&self, _: &mut Context<'_>) -> Poll<Result<(), ConnectorError>> {
        panic!("negative fixture has no running CPU")
    }
    fn poll_closed(&self, _: &mut Context<'_>) -> Poll<Result<(), ConnectorError>> {
        self.closes.fetch_add(1, Ordering::SeqCst);
        panic!("completed source cannot close its shared port")
    }
    fn local_failure(&self) -> Option<Arc<dyn std::error::Error + Send + Sync>> {
        None
    }
}
fn values() -> (ConnectorInstanceDescriptor, ConnectorProviderBindingKey) {
    let (d, k, _, _) = super::super::tests::contract_values();
    (d, k)
}
#[test]
fn missing_original_io_refuses_before_consuming_start_or_dispatching_cpu() {
    let (_executor, _warehouse, runtime) = super::super::tests::local_runtime();
    let (descriptor, key) = values();
    let metadata = Arc::new(crate::metadata::IcebergMetadata::new(
        descriptor.clone(),
        key.incarnation,
        runtime.clone(),
    ));
    let adapter = IcebergCleanupMaintenanceAdapter::new(key.clone(), runtime).unwrap();
    let cpu = Arc::new(NoPhysicalCpu::default());
    let operation = ConnectorCleanupOperationId::new();
    let mut start = Some(Start {
        original: ConnectorOriginalResultScope::from_original(Arc::new(UnreachedScope)),
        table: ConnectorTableIdentity {
            instance_id: key.instance_id,
            namespace: Arc::from("db"),
            table: Arc::from("t"),
        },
        operation_id: operation,
        older_than_ms: 1,
        selection: None,
        context: super::super::tests::context(),
    });
    let mut target = None;
    let error = install_in(&adapter, &mut start, metadata, cpu.clone(), &mut target).unwrap_err();
    assert_eq!(error.kind(), ConnectorErrorKind::Unsupported);
    assert_eq!(start.as_ref().unwrap().operation_id, operation);
    assert!(target.is_none());
    assert_eq!(cpu.closes.load(Ordering::SeqCst), 0);
}
#[test]
fn completed_discovery_source_does_not_close_the_next_plans_original_cpu_port() {
    let (_executor, _warehouse, runtime) = super::super::tests::local_runtime();
    let (descriptor, key) = values();
    let metadata = Arc::new(crate::metadata::IcebergMetadata::new(
        descriptor,
        key.incarnation,
        runtime.clone(),
    ));
    let adapter = IcebergCleanupMaintenanceAdapter::new(key, runtime).unwrap();
    let cpu = Arc::new(NoPhysicalCpu::default());
    let original = completed_original(adapter, metadata, cpu.clone());
    let waker = futures::task::noop_waker();
    let mut cx = Context::from_waker(&waker);
    assert!(matches!(original.poll_closed(&mut cx), Poll::Ready(Ok(()))));
    assert_eq!(cpu.closes.load(Ordering::SeqCst), 0);
}
struct DropPanic;
impl Future for DropPanic {
    type Output = Result<Output, ConnectorError>;
    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Ready(Ok(Output::Finalized))
    }
}
impl Drop for DropPanic {
    fn drop(&mut self) {
        panic!("original F Drop did not return")
    }
}
#[test]
fn original_f_drop_unwind_keeps_its_output_and_cannot_become_an_exit_receipt() {
    let mut body = Body {
        future: Some(Box::pin(DropPanic)),
        result: None,
        closed: false,
        unproven: false,
    };
    let waker = futures::task::noop_waker();
    let mut cx = Context::from_waker(&waker);
    assert!(
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| poll_step(
            &mut body, &mut cx
        )))
        .is_err()
    );
    assert!(matches!(body.result, Some(Ok(Output::Finalized))));
    assert!(body.future.is_none());
    assert!(body.unproven);
    assert!(matches!(poll_step(&mut body, &mut cx), Poll::Ready(Err(_))));
}
#[test]
fn cleanup_cpu_factory_uses_the_selected_metadata_original_not_statistics_product() {
    let (_executor, _warehouse, runtime) = super::super::tests::local_runtime();
    let (descriptor, key, _, _) = super::super::tests::contract_values();
    let adapter = IcebergCleanupMaintenanceAdapter::new(key.clone(), runtime).unwrap();
    let payload = PlanPayload {
        version: ARTIFACT_VERSION,
        namespace: "db".into(),
        table: "t".into(),
        table_uuid: uuid::Uuid::from_u128(31).to_string(),
    };
    let request = ConnectorCleanupPlanningRequest::try_new(
        ConnectorCleanupOperationId::new(),
        key.clone(),
        novarocks_spi::connector::ConnectorCleanupOperation::remove_unreferenced_objects(
            novarocks_spi::connector::ConnectorTableHandle::try_new(
                key.instance_id.clone(),
                bytes::Bytes::from_static(b"table"),
            )
            .unwrap(),
            1,
        )
        .unwrap(),
        super::super::tests::context(),
    )
    .unwrap();
    let plan = ConnectorCleanupPlan::try_new(
        &request,
        [6; 32],
        [7; 32],
        novarocks_spi::connector::ConnectorCleanupPlanSummary::try_new(3, 30, 1).unwrap(),
        canonical(&serde_json::to_value(&payload).unwrap()).unwrap(),
        novarocks_spi::connector::ConnectorProcessLocalPayload::new(key.clone(), Arc::new(())),
    )
    .unwrap();
    let source = IcebergMetadataCpuOriginal::cleanup_source(
        descriptor,
        key.incarnation,
        CpuWork {
            input: CpuInput::Payload {
                adapter: adapter.clone(),
                plan,
            },
            result: None,
        },
    );
    let input = ProviderMetadataCpuInput(source.clone());
    assert_eq!(
        input.original_source_type_id(),
        adapter.metadata_cpu_source_type_id().unwrap()
    );
    input.run_original_phase();
    let CpuOutput::Payload(decoded) = source.take_cleanup_output().unwrap() else {
        panic!("selected metadata original must run the Cleanup payload phase");
    };
    assert_eq!(decoded.version, payload.version);
    assert_eq!(decoded.namespace, payload.namespace);
    assert_eq!(decoded.table, payload.table);
    assert_eq!(decoded.table_uuid, payload.table_uuid);
}

#[test]
fn cleanup_metadata_original_preserves_corrupt_provider_payload_after_neutral_validation() {
    use novarocks_failure_contract::FailureProjection;
    let (_executor, _warehouse, runtime) = super::super::tests::local_runtime();
    let (descriptor, key, plan, _) = super::super::tests::contract_values();
    let adapter = IcebergCleanupMaintenanceAdapter::new(key.clone(), runtime).unwrap();
    assert_eq!(plan.owner(), &key);
    plan.validate().unwrap();
    // The neutral contract fixture has a valid digest and exact owner, but
    // its opaque `plan` bytes are intentionally not an Iceberg JSON payload.
    assert_eq!(plan.provider_payload().as_ref(), b"plan");
    assert!(serde_json::from_slice::<serde_json::Value>(plan.provider_payload()).is_err());
    let expected = match adapter.plan_payload(&plan) {
        Err(error) => error,
        Ok(_) => panic!("malformed provider bytes must fail canonical decoding"),
    };
    assert_eq!(expected.kind(), ConnectorErrorKind::CorruptData);
    let source = IcebergMetadataCpuOriginal::cleanup_source(
        descriptor,
        key.incarnation,
        CpuWork {
            input: CpuInput::Payload { adapter, plan },
            result: None,
        },
    );
    ProviderMetadataCpuInput(source.clone()).run_original_phase();
    let actual = match source.take_cleanup_output() {
        Err(error) => error,
        Ok(_) => panic!("Cleanup CPU must preserve the provider decoder refusal"),
    };
    assert_eq!(actual, expected);
    assert_eq!(actual.failure_fact(), expected.failure_fact());
}

#[test]
fn background_hadoop_policy_refuses_in_the_original_cpu_phase_before_catalog_io() {
    let (_executor, warehouse, runtime) = super::super::tests::local_runtime();
    let (descriptor, key) = values();
    let adapter = IcebergCleanupMaintenanceAdapter::new(key.clone(), runtime).unwrap();
    let context = super::super::tests::context()
        .with_initiation(novarocks_spi::connector::ConnectorRequestInitiation::Background);
    let source = IcebergMetadataCpuOriginal::cleanup_source(
        descriptor,
        key.incarnation,
        CpuWork {
            input: CpuInput::Admission {
                adapter,
                table: ConnectorTableIdentity {
                    instance_id: key.instance_id,
                    namespace: Arc::from("absent"),
                    table: Arc::from("absent"),
                },
                context,
            },
            result: None,
        },
    );
    let entries = std::fs::read_dir(warehouse.path()).unwrap().count();
    ProviderMetadataCpuInput(source.clone()).run_original_phase();
    let error = match source.take_cleanup_output() {
        Err(error) => error,
        Ok(_) => panic!("background policy must refuse"),
    };
    assert_eq!(error.kind(), ConnectorErrorKind::Unsupported);
    assert_eq!(
        std::fs::read_dir(warehouse.path()).unwrap().count(),
        entries
    );
}

fn completed_original(
    adapter: IcebergCleanupMaintenanceAdapter,
    metadata: Arc<dyn ConnectorMetadata>,
    cpu: Arc<dyn ProviderMetadataCpuPort>,
) -> Original {
    Original {
        shared: Arc::new(Shared {
            adapter,
            metadata,
            cpu,
            original: ConnectorOriginalResultScope::from_original(Arc::new(UnreachedScope)),
            plan_stamp: Mutex::new(None),
            executed: Mutex::new(Default::default()),
            cpu_original: Mutex::new(None),
            metadata_call_unproven: std::sync::atomic::AtomicBool::new(false),
            journals: Mutex::new(Vec::new()),
            first_failure: Mutex::new(None),
            receipt_cpu_failure: Mutex::new(None),
            facts: Mutex::new(Default::default()),
        }),
        body: Mutex::new(Some(Body {
            future: None,
            result: Some(Ok(Output::Finalized)),
            closed: false,
            unproven: false,
        })),
    }
}

// This port refuses before consuming the input. It cannot mint a physical
// grant; close proves only that this negative fixture never started CPU work.
#[derive(Default)]
struct RefusingReceiptCpu {
    submitted: AtomicUsize,
    closed: AtomicUsize,
}
impl ProviderMetadataCpuPort for RefusingReceiptCpu {
    fn submit_metadata_in(
        &self,
        input: &mut Option<ProviderMetadataCpuInput>,
    ) -> Result<(), ConnectorError> {
        assert!(input.is_some(), "same stable original receipt input");
        self.submitted.fetch_add(1, Ordering::SeqCst);
        Err(exhausted("receipt CPU capacity refused after publication"))
    }
    fn poll_finished(&self, _: &mut Context<'_>) -> Poll<Result<(), ConnectorError>> {
        panic!("refused input never started physical work")
    }
    fn poll_closed(&self, _: &mut Context<'_>) -> Poll<Result<(), ConnectorError>> {
        self.closed.fetch_add(1, Ordering::SeqCst);
        Poll::Ready(Ok(()))
    }
    fn local_failure(&self) -> Option<Arc<dyn std::error::Error + Send + Sync>> {
        None
    }
}

use crate::{
    catalog::{
        publication::{CatalogEvidencePolicy, EvidenceContext},
        publication_dispatch::{
            DispatchCompletion, FinalizationWork, PreparationFailure, PreparedCatalogDispatch,
            PreparedPublication,
        },
        transaction::CommitProof,
    },
    commit::{
        attempt::observation::{DispatchEvidence, ProtocolObservation, PublicationReceipts},
        model::OperationToken,
        publication_descriptor::{AttributionPlan, PositiveAttributionObservation},
    },
};
use novarocks_spi::connector::{
    ConnectorMutationFailure, ConnectorMutationFailureKind, ConnectorMutationOperationId,
    ExternalMutationEffect, ExternalMutationEvidence, ExternalMutationFinalization,
};
struct ReturnedDispatch {
    evidence: DispatchEvidence,
    finalization: Option<Box<dyn FinalizationWork>>,
}
#[async_trait::async_trait]
impl PreparedCatalogDispatch for ReturnedDispatch {
    async fn dispatch_once(self: Box<Self>, _: EvidenceContext) -> DispatchCompletion {
        DispatchCompletion {
            evidence: self.evidence,
            finalization: self.finalization,
        }
    }
}
// Rejected evidence has no positive-applied finalization authority. Hold the
// actual dispatch return instead of inventing a required finalize for it.
struct HeldRejectedDispatch {
    held: tokio::sync::oneshot::Receiver<()>,
    failure: ConnectorMutationFailure,
}
#[async_trait::async_trait]
impl PreparedCatalogDispatch for HeldRejectedDispatch {
    async fn dispatch_once(self: Box<Self>, _: EvidenceContext) -> DispatchCompletion {
        self.held.await.expect("original rejected dispatch returns");
        DispatchCompletion {
            evidence: DispatchEvidence::Rejected {
                failure: self.failure,
                observation: ProtocolObservation::default(),
            },
            finalization: None,
        }
    }
}
struct HeldFinalization(tokio::sync::oneshot::Receiver<()>);
#[async_trait::async_trait]
impl FinalizationWork for HeldFinalization {
    async fn run(self: Box<Self>, _: EvidenceContext) -> ExternalMutationFinalization {
        self.0
            .await
            .expect("original finalization returns normally");
        ExternalMutationFinalization::Complete
    }
}
fn publication_journal() -> (Journal, ConnectorRequestContext) {
    let (descriptor, key) = values();
    let operation = ConnectorMutationOperationId::from_bytes([31; 16]);
    let journal = Journal::new(OperationToken::from_mutation(operation));
    let evidence = ExternalMutationEvidence::try_new(
        2,
        descriptor,
        key.incarnation,
        operation,
        "drop_branch",
        bytes::Bytes::from_static(b"{}"),
    )
    .unwrap();
    let preparation = super::super::tests::context();
    journal
        .prepare_catalog_with_receipts(
            evidence,
            PublicationReceipts::for_effects(
                ReceiptRecord {
                    ordinal: 0,
                    outcome: ObjectOutcome::Deleted,
                    reason: None,
                },
                ReceiptRecord {
                    ordinal: 0,
                    outcome: ObjectOutcome::AlreadyAbsent,
                    reason: None,
                },
            ),
            vec!["db.t:owned-ref".into()],
            b"exact drop owned-ref".to_vec(),
            AttributionPlan::Property {
                table_uuid: uuid::Uuid::from_u128(31),
                key: "operation".into(),
                value: "original".into(),
            },
            preparation.clone(),
        )
        .unwrap();
    (journal, preparation)
}
fn install_test_publication(
    original: &Original,
    journal: Journal,
    context: ConnectorRequestContext,
    prepared: Result<PreparedPublication, PreparationFailure>,
) {
    original
        .shared
        .journals
        .lock()
        .unwrap()
        .push(journal.clone());
    let shared = original.shared.clone();
    let mut body = original.body.lock().unwrap();
    let body = body.as_mut().unwrap();
    body.result = None;
    body.future = Some(Box::pin(async move {
        let reply = journal
            .publish_once(
                context,
                CatalogEvidencePolicy::default(),
                move |_, _, _| async move { prepared },
            )
            .await;
        receive_publication(&shared, journal, reply, 0).await?;
        Ok(Output::Finalized)
    }));
}
fn returned_evidence(
    evidence: DispatchEvidence,
    finalization: Option<Box<dyn FinalizationWork>>,
) -> Result<PreparedPublication, PreparationFailure> {
    Ok(PreparedPublication::Mutation {
        binding: None,
        dispatch: Box::new(ReturnedDispatch {
            evidence,
            finalization,
        }),
    })
}

#[test]
fn applied_and_noop_are_visible_while_finalize_waits_and_survive_receipt_cpu_refusal() {
    for effect in [
        ExternalMutationEffect::Applied,
        ExternalMutationEffect::NoOp,
    ] {
        let (_executor, _warehouse, runtime) = super::super::tests::local_runtime();
        // Journal dispatch constructs its real Tokio deadline/barrier futures.
        let _entered = _executor.enter();
        let (descriptor, key) = values();
        let metadata = Arc::new(crate::metadata::IcebergMetadata::new(
            descriptor,
            key.incarnation,
            runtime.clone(),
        ));
        let adapter = IcebergCleanupMaintenanceAdapter::new(key, runtime).unwrap();
        let cpu = Arc::new(RefusingReceiptCpu::default());
        let original = completed_original(adapter, metadata, cpu.clone());
        let (journal, context) = publication_journal();
        let (release, held) = tokio::sync::oneshot::channel();
        let mut proof = CommitProof::applied(None);
        proof.effect = effect;
        install_test_publication(
            &original,
            journal,
            context,
            returned_evidence(
                DispatchEvidence::Applied { proof },
                Some(Box::new(HeldFinalization(held))),
            ),
        );
        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        assert_eq!(
            original.publication_fact(),
            (Some(LakePublicationDisposition::CommitUnknown), false)
        );
        assert!(matches!(original.poll_output(&mut cx), Poll::Pending));
        let expected = (
            Some(LakePublicationDisposition::KnownCommitted),
            effect == ExternalMutationEffect::Applied,
        );
        assert_eq!(original.publication_fact(), expected);
        // Evidence is not a finalization or original F exit receipt.
        assert!(matches!(original.poll_closed(&mut cx), Poll::Pending));
        assert_eq!(original.shared.journals.lock().unwrap().len(), 1);
        assert_eq!(cpu.submitted.load(Ordering::SeqCst), 0);
        release.send(()).unwrap();
        let Poll::Ready(Err(error)) = original.poll_output(&mut cx) else {
            panic!("receipt CPU must refuse");
        };
        assert_eq!(error.kind(), ConnectorErrorKind::ResourceExhausted);
        assert_eq!(original.publication_fact(), expected);
        assert!(original.shared.journals.lock().unwrap().is_empty());
        assert!(original.shared.cpu_original.lock().unwrap().is_some());
        assert!(matches!(original.poll_closed(&mut cx), Poll::Ready(Ok(()))));
        assert_eq!(cpu.submitted.load(Ordering::SeqCst), 1);
        assert_eq!(cpu.closed.load(Ordering::SeqCst), 1);
        assert!(original.shared.cpu_original.lock().unwrap().is_none());
    }
}

#[test]
fn original_not_dispatched_survives_receipt_cpu_refusal_and_closes() {
    let (_executor, _warehouse, runtime) = super::super::tests::local_runtime();
    let (descriptor, key) = values();
    let metadata = Arc::new(crate::metadata::IcebergMetadata::new(
        descriptor,
        key.incarnation,
        runtime.clone(),
    ));
    let adapter = IcebergCleanupMaintenanceAdapter::new(key, runtime).unwrap();
    let cpu = Arc::new(RefusingReceiptCpu::default());
    let original = completed_original(adapter, metadata, cpu.clone());
    let (journal, context) = publication_journal();
    install_test_publication(
        &original,
        journal.clone(),
        context,
        Err(PreparationFailure(ConnectorMutationFailure::new(
            ConnectorMutationFailureKind::Unavailable,
            "original adapter refused before RPC",
        ))),
    );
    let waker = futures::task::noop_waker();
    let mut cx = Context::from_waker(&waker);
    assert!(matches!(original.poll_output(&mut cx), Poll::Ready(Err(_))));
    assert!(journal.descriptor().unwrap().first_dispatch_ms.is_none());
    assert_eq!(
        original.publication_fact(),
        (Some(LakePublicationDisposition::KnownUncommitted), false)
    );
    assert!(matches!(original.poll_closed(&mut cx), Poll::Ready(Ok(()))));
    assert_eq!(cpu.submitted.load(Ordering::SeqCst), 1);
    assert_eq!(cpu.closed.load(Ordering::SeqCst), 1);
}

#[test]
fn unknown_is_retained_until_the_same_original_positive_proof_exists() {
    let (_executor, _warehouse, runtime) = super::super::tests::local_runtime();
    // Journal dispatch constructs its real Tokio deadline/barrier futures.
    let _entered = _executor.enter();
    let (descriptor, key) = values();
    let metadata = Arc::new(crate::metadata::IcebergMetadata::new(
        descriptor,
        key.incarnation,
        runtime.clone(),
    ));
    let adapter = IcebergCleanupMaintenanceAdapter::new(key, runtime).unwrap();
    let cpu = Arc::new(RefusingReceiptCpu::default());
    let original = completed_original(adapter, metadata, cpu.clone());
    let (journal, context) = publication_journal();
    install_test_publication(
        &original,
        journal.clone(),
        context,
        returned_evidence(
            DispatchEvidence::Unknown {
                failure: ConnectorMutationFailure::new(
                    ConnectorMutationFailureKind::Unavailable,
                    "original lost reply",
                ),
                observation: ProtocolObservation::default(),
            },
            None,
        ),
    );
    let waker = futures::task::noop_waker();
    let mut cx = Context::from_waker(&waker);
    assert!(matches!(original.poll_output(&mut cx), Poll::Ready(Err(_))));
    assert_eq!(
        original.publication_fact(),
        (Some(LakePublicationDisposition::CommitUnknown), false)
    );
    // The closing tag creates no new work and cannot turn Unknown into an
    // exit receipt, even when foreground time has already expired.
    let (_, _, plan, _) = super::super::tests::contract_values();
    *original.shared.plan_stamp.lock().unwrap() =
        Some((plan.plan_digest(), plan.manifest_digest()));
    let mut closing = Some(Step::Finalize(
        novarocks_spi::connector::ConnectorCleanupFinalizeRequest {
            plan,
            context: super::super::tests::context()
                .with_earlier_deadline(Instant::now() - std::time::Duration::from_secs(1)),
        },
    ));
    original.install_step_in(&mut closing).unwrap();
    assert!(matches!(
        original.poll_output(&mut cx),
        Poll::Ready(Ok(Output::Finalized))
    ));
    assert_eq!(
        original.publication_fact(),
        (Some(LakePublicationDisposition::CommitUnknown), false)
    );
    assert!(matches!(original.poll_closed(&mut cx), Poll::Pending));
    assert!(original.shared.cpu_original.lock().unwrap().is_none());
    assert_eq!(original.shared.journals.lock().unwrap().len(), 1);
    journal
        .append_positive_proof_with_finalization(
            journal.descriptor().unwrap().key,
            PositiveAttributionObservation::PropertyMarker {
                table_uuid: uuid::Uuid::from_u128(31),
                key: "operation".into(),
                value: "original".into(),
            },
            ExternalMutationFinalization::Complete,
        )
        .unwrap();
    assert_eq!(
        original.publication_fact(),
        (Some(LakePublicationDisposition::KnownCommitted), true)
    );
    assert!(matches!(original.poll_closed(&mut cx), Poll::Ready(Ok(()))));
    assert!(original.shared.journals.lock().unwrap().is_empty());
}

#[test]
fn original_rejected_dispatch_survives_receipt_cpu_refusal_and_closes() {
    let (_executor, _warehouse, runtime) = super::super::tests::local_runtime();
    // Journal dispatch constructs its real Tokio deadline/barrier futures.
    let _entered = _executor.enter();
    let (descriptor, key) = values();
    let metadata = Arc::new(crate::metadata::IcebergMetadata::new(
        descriptor,
        key.incarnation,
        runtime.clone(),
    ));
    let adapter = IcebergCleanupMaintenanceAdapter::new(key, runtime).unwrap();
    let cpu = Arc::new(RefusingReceiptCpu::default());
    let original = completed_original(adapter, metadata, cpu.clone());
    let (journal, context) = publication_journal();
    install_test_publication(
        &original,
        journal,
        context,
        returned_evidence(
            DispatchEvidence::Rejected {
                failure: ConnectorMutationFailure::new(
                    ConnectorMutationFailureKind::Unavailable,
                    "original catalog rejected mutation",
                ),
                observation: ProtocolObservation::default(),
            },
            None,
        ),
    );
    let waker = futures::task::noop_waker();
    let mut cx = Context::from_waker(&waker);
    assert!(matches!(original.poll_output(&mut cx), Poll::Ready(Err(_))));
    assert_eq!(
        original.publication_fact(),
        (Some(LakePublicationDisposition::KnownUncommitted), false)
    );
    assert!(original.shared.journals.lock().unwrap().is_empty());
    assert!(matches!(original.poll_closed(&mut cx), Poll::Ready(Ok(()))));
    assert_eq!(cpu.submitted.load(Ordering::SeqCst), 1);
    assert_eq!(cpu.closed.load(Ordering::SeqCst), 1);
}

#[derive(Debug)]
struct OriginalPublicationCause(&'static str);
impl std::fmt::Display for OriginalPublicationCause {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0)
    }
}
impl std::error::Error for OriginalPublicationCause {}
impl novarocks_failure_contract::FailureProjection for OriginalPublicationCause {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        ConnectorErrorKind::PermissionDenied.failure_fact()
    }
}
fn original_publication_failure(detail: &'static str) -> ConnectorError {
    ConnectorError::from_domain_static(
        ConnectorErrorKind::PermissionDenied,
        OriginalPublicationCause(detail),
        detail,
    )
}
fn assert_same_original_cause(actual: &ConnectorError, expected: &ConnectorError) {
    use novarocks_failure_contract::{FailureProjection, SharedFailureCause};
    assert_eq!(actual, expected);
    assert_eq!(actual.failure_fact(), expected.failure_fact());
    let actual = std::error::Error::source(actual)
        .unwrap()
        .downcast_ref::<SharedFailureCause>()
        .unwrap();
    let expected = std::error::Error::source(expected)
        .unwrap()
        .downcast_ref::<SharedFailureCause>()
        .unwrap();
    // SharedFailureCause equality is exact Arc identity, not equal text.
    assert_eq!(actual, expected);
}
struct FailedFinalization {
    held: tokio::sync::oneshot::Receiver<()>,
    failure: ConnectorMutationFailure,
}
#[async_trait::async_trait]
impl FinalizationWork for FailedFinalization {
    async fn run(self: Box<Self>, _: EvidenceContext) -> ExternalMutationFinalization {
        self.held
            .await
            .expect("original finalization returns its failure");
        ExternalMutationFinalization::Failed(self.failure)
    }
}

#[test]
fn rejected_original_cause_precedes_receipt_cpu_refusal_and_survives_actual_close() {
    let (_executor, _warehouse, runtime) = super::super::tests::local_runtime();
    // Journal dispatch constructs its real Tokio deadline/barrier futures.
    let _entered = _executor.enter();
    let (descriptor, key) = values();
    let metadata = Arc::new(crate::metadata::IcebergMetadata::new(
        descriptor,
        key.incarnation,
        runtime.clone(),
    ));
    let adapter = IcebergCleanupMaintenanceAdapter::new(key, runtime).unwrap();
    let cpu = Arc::new(RefusingReceiptCpu::default());
    let original = completed_original(adapter, metadata, cpu.clone());
    let (journal, context) = publication_journal();
    let primary = original_publication_failure("original exact catalog rejection");
    let failure = ConnectorMutationFailure::from_connector(primary.clone());
    let (release, held) = tokio::sync::oneshot::channel();
    install_test_publication(
        &original,
        journal,
        context,
        Ok(PreparedPublication::Mutation {
            binding: None,
            dispatch: Box::new(HeldRejectedDispatch { held, failure }),
        }),
    );
    let waker = futures::task::noop_waker();
    let mut cx = Context::from_waker(&waker);
    assert!(matches!(original.poll_output(&mut cx), Poll::Pending));
    assert_eq!(
        original.publication_fact(),
        (Some(LakePublicationDisposition::CommitUnknown), false)
    );
    assert!(ConnectorCleanupAsyncSource::original_failure(&original).is_none());
    assert!(matches!(original.poll_closed(&mut cx), Poll::Pending));
    assert_eq!(cpu.submitted.load(Ordering::SeqCst), 0);
    assert_eq!(cpu.closed.load(Ordering::SeqCst), 0);
    release.send(()).unwrap();
    let Poll::Ready(Err(actual)) = original.poll_output(&mut cx) else {
        panic!("receipt CPU refusal must return the original rejection");
    };
    assert_same_original_cause(&actual, &primary);
    assert_eq!(
        original.publication_fact(),
        (Some(LakePublicationDisposition::KnownUncommitted), false)
    );
    let (observed, required) = ConnectorCleanupAsyncSource::original_failure(&original).unwrap();
    assert_same_original_cause(&observed, &primary);
    assert!(!required);
    assert!(matches!(
        original.shared.first_failure.lock().unwrap().as_ref(),
        Some(OriginalFailure::Publication(
            CatalogPublicationFailure::Dispatch(_)
        ))
    ));
    assert_eq!(
        original
            .shared
            .receipt_cpu_failure
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .kind(),
        ConnectorErrorKind::ResourceExhausted
    );
    assert!(original.shared.cpu_original.lock().unwrap().is_some());
    assert_eq!(cpu.closed.load(Ordering::SeqCst), 0);
    assert!(matches!(original.poll_closed(&mut cx), Poll::Ready(Ok(()))));
    assert_eq!(cpu.closed.load(Ordering::SeqCst), 1);
    assert!(original.shared.cpu_original.lock().unwrap().is_none());
    assert_same_original_cause(&original.shared.original_failure().unwrap(), &primary);
    let (observed, required) = ConnectorCleanupAsyncSource::original_failure(&original).unwrap();
    assert_same_original_cause(&observed, &primary);
    assert!(!required);
    assert_eq!(
        original.publication_fact(),
        (Some(LakePublicationDisposition::KnownUncommitted), false)
    );
}

#[test]
fn applied_required_finalize_cause_precedes_receipt_cpu_refusal_and_survives_actual_close() {
    let (_executor, _warehouse, runtime) = super::super::tests::local_runtime();
    // Journal dispatch constructs its real Tokio deadline/barrier futures.
    let _entered = _executor.enter();
    let (descriptor, key) = values();
    let metadata = Arc::new(crate::metadata::IcebergMetadata::new(
        descriptor,
        key.incarnation,
        runtime.clone(),
    ));
    let adapter = IcebergCleanupMaintenanceAdapter::new(key, runtime).unwrap();
    let cpu = Arc::new(RefusingReceiptCpu::default());
    let original = completed_original(adapter, metadata, cpu.clone());
    let (journal, context) = publication_journal();
    let primary = original_publication_failure("original required finalization rejection");
    let failure = ConnectorMutationFailure::from_connector(primary.clone());
    let (release, held) = tokio::sync::oneshot::channel();
    install_test_publication(
        &original,
        journal,
        context,
        returned_evidence(
            DispatchEvidence::Applied {
                proof: CommitProof::applied(None),
            },
            Some(Box::new(FailedFinalization { held, failure })),
        ),
    );
    let waker = futures::task::noop_waker();
    let mut cx = Context::from_waker(&waker);
    assert!(matches!(original.poll_output(&mut cx), Poll::Pending));
    assert_eq!(
        original.publication_fact(),
        (Some(LakePublicationDisposition::KnownCommitted), true)
    );
    assert!(matches!(original.poll_closed(&mut cx), Poll::Pending));
    assert_eq!(cpu.submitted.load(Ordering::SeqCst), 0);
    assert_eq!(cpu.closed.load(Ordering::SeqCst), 0);
    release.send(()).unwrap();
    let Poll::Ready(Err(actual)) = original.poll_output(&mut cx) else {
        panic!("receipt CPU refusal must return the original required-finalize failure");
    };
    assert_same_original_cause(&actual, &primary);
    let (observed, required) = ConnectorCleanupAsyncSource::original_failure(&original).unwrap();
    assert_same_original_cause(&observed, &primary);
    assert!(required);
    assert!(matches!(
        original.shared.first_failure.lock().unwrap().as_ref(),
        Some(OriginalFailure::Publication(
            CatalogPublicationFailure::RequiredFinalization(_)
        ))
    ));
    assert_eq!(
        original
            .shared
            .receipt_cpu_failure
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .kind(),
        ConnectorErrorKind::ResourceExhausted
    );
    assert!(original.shared.cpu_original.lock().unwrap().is_some());
    assert_eq!(cpu.closed.load(Ordering::SeqCst), 0);
    assert!(matches!(original.poll_closed(&mut cx), Poll::Ready(Ok(()))));
    assert_eq!(cpu.closed.load(Ordering::SeqCst), 1);
    assert!(original.shared.cpu_original.lock().unwrap().is_none());
    assert_same_original_cause(&original.shared.original_failure().unwrap(), &primary);
    let (observed, required) = ConnectorCleanupAsyncSource::original_failure(&original).unwrap();
    assert_same_original_cause(&observed, &primary);
    assert!(required);
    assert_eq!(
        original.publication_fact(),
        (Some(LakePublicationDisposition::KnownCommitted), true)
    );
}

#[test]
fn pure_finalize_after_deadline_preserves_owner_and_starts_no_cpu_or_io() {
    let (_executor, _warehouse, runtime) = super::super::tests::local_runtime();
    let (descriptor, key, plan, _) = super::super::tests::contract_values();
    let metadata = Arc::new(crate::metadata::IcebergMetadata::new(
        descriptor,
        key.incarnation,
        runtime.clone(),
    ));
    let adapter = IcebergCleanupMaintenanceAdapter::new(key, runtime).unwrap();
    let cpu = Arc::new(NoPhysicalCpu::default());
    let original = completed_original(adapter, metadata, cpu.clone());
    *original.shared.plan_stamp.lock().unwrap() =
        Some((plan.plan_digest(), plan.manifest_digest()));
    original.body.lock().unwrap().as_mut().unwrap().result = None;
    let expired = super::super::tests::context()
        .with_earlier_deadline(Instant::now() - std::time::Duration::from_secs(1));
    assert!(
        validate_context(&expired).is_err(),
        "foreground calls remain stopped"
    );
    let mut step = Some(Step::Finalize(
        novarocks_spi::connector::ConnectorCleanupFinalizeRequest {
            plan,
            context: expired,
        },
    ));
    original.install_step_in(&mut step).unwrap();
    assert!(step.is_none());
    let mut cx = Context::from_waker(std::task::Waker::noop());
    assert!(matches!(
        original.poll_output(&mut cx),
        Poll::Ready(Ok(Output::Finalized))
    ));
    assert_eq!(
        original.publication_fact(),
        (None, false),
        "Finalize creates no publication or effect proof"
    );
    assert!(matches!(original.poll_closed(&mut cx), Poll::Ready(Ok(()))));
    assert_eq!(cpu.closes.load(Ordering::SeqCst), 0);
}
