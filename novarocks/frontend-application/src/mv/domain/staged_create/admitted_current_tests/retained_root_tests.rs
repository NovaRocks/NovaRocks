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

//! Current registration, real local footers and exact Catalog lease retention.
//! This is not a complete CREATE Action, Iceberg commit or deletion proof.

use super::*;
use crate::{
    query::read_action::error_residual_tests::{Gate, IdleOriginal, PauseResultWake, Rescue},
    state_store::execution::StateStoreOriginalOwner,
};
use novarocks_catalog_application::ConnectorControlHost;
use novarocks_mv_application::management::{
    ActualCompletionEvidence, EffectTerminalFact, IsolationEvidence, ProcessIncarnation,
    ReadmissionEvaluator,
};
use novarocks_query_application::cpu::{QueryBlockingExecutorConfig, QueryBlockingExecutorOwner};
use novarocks_state_store_api::{
    InstanceScope, StateStoreBlockingOriginal, StateStoreBlockingSourceVocabulary,
    StateStoreProviderId,
};
use novarocks_state_store_runtime::StateStoreRunPolicy;
use std::{
    future::poll_fn,
    num::NonZeroUsize,
    sync::atomic::{AtomicBool, AtomicUsize, Ordering},
    task::{Context, Poll, Waker},
    time::{Duration, Instant},
};

// Pure fixture admission; the production lease validates and creates the
// actual one-operation document token before the original mutation request.
impl ConnectorDocumentStorageManagement for OriginalRegistrationChildFixture {
    fn descriptor(&self) -> &ConnectorInstanceDescriptor {
        &self.descriptor
    }
    fn incarnation(&self) -> ProviderBindingEpoch {
        self.epoch
    }
    fn admit_management(
        &self,
        request: ConnectorDocumentManagementAdmissionRequest,
    ) -> Result<Bytes, ConnectorError> {
        assert_eq!(request.owner().instance_id, self.descriptor.instance_id);
        assert_eq!(request.owner().incarnation, self.epoch);
        assert_eq!(
            request.operation(),
            ConnectorDocumentManagementOperation::SingleTargetUpdate
        );
        assert!(request.expected_object_id().is_some());
        Ok(Bytes::from_static(b"original-registration-admission"))
    }
}

fn actual_generation(
    fixture: &Fixture,
    mutation: Arc<OriginalRegistrationChildFixture>,
) -> ConnectorControlBinding {
    let template = novarocks_catalog_application::test_support::test_control_binding_for(
        fixture.caps.descriptor.instance_id.clone(),
        3,
    );
    ConnectorControlBinding::try_new(
        fixture.caps.descriptor.clone(),
        fixture.caps.epoch,
        fixture.caps.clone(),
        Arc::clone(template.planning()),
        Arc::clone(template.execution_distribution()),
        Some(mutation.clone()),
    )
    .unwrap()
    .with_catalog_properties(
        CatalogProperties::new(
            fixture.catalog.clone(),
            ConnectorProviderId::parse("iceberg").unwrap(),
            1,
            vec![],
            vec![],
        )
        .unwrap(),
    )
    .unwrap()
    .try_with_document_storage(Some(
        ConnectorDocumentStorageBinding::try_new(
            fixture.caps.descriptor.clone(),
            fixture.caps.epoch,
            Some(fixture.caps.clone()),
            Some(mutation),
        )
        .unwrap(),
    ))
    .unwrap()
}

#[tokio::test(flavor = "current_thread")]
async fn current_registration_retains_exact_g1_after_same_root_cpu_and_qb_footers() {
    use super::super::{CurrentProjectionRegistration, OriginalManagedTargetCurrentSource};
    let fixture = Fixture::new(vec![]);
    let mut cpu = CurrentCpuFixture::new();
    let scope = cpu.root.owner.scope();
    let mut workers = QueryBlockingExecutorOwner::try_new(QueryBlockingExecutorConfig::new(
        NonZeroUsize::new(1).unwrap(),
        NonZeroUsize::new(1).unwrap(),
    ))
    .unwrap();
    let instance = InstanceScope::new();
    let mut store =
        StateStoreOriginalOwner::for_root(
            &workers.executor(),
            StateStoreBlockingSourceVocabulary::for_provider::<
                IdleOriginal,
                IdleOriginal,
                IdleOriginal,
            >(StateStoreProviderId::new(
                "current-generation-footer-fixture",
            )),
            instance,
            scope.clone(),
            StateStoreRunPolicy::default(),
        )
        .unwrap();
    let read_gate = Gate::new();
    let footer_gate = Gate::new();
    let close_gate = Gate::new();
    let _read_rescue = Rescue(read_gate.clone());
    let _footer_rescue = Rescue(footer_gate.clone());
    let _close_rescue = Rescue(close_gate.clone());
    let local = Arc::new(IdleOriginal {
        instance,
        slot: Mutex::new(None),
        read: read_gate.clone(),
        read_returned: Arc::new(AtomicBool::new(false)),
        close: close_gate.clone(),
        closing: AtomicBool::new(false),
        results: AtomicUsize::new(0),
        exits: AtomicUsize::new(0),
    });
    let local_context = store.context(Instant::now() + Duration::from_secs(30));
    let original: Arc<dyn StateStoreBlockingOriginal> = local.clone();
    let mut local_slot = None;
    local_context
        .bind_original_in(&original, &mut local_slot)
        .unwrap();
    *local.slot.lock().unwrap() = local_slot;
    local_context.request_progress();
    drop(original);

    let (finish_cleanup, cleanup) = tokio::sync::oneshot::channel();
    let payload = Arc::new(());
    let payload_weak = Arc::downgrade(&payload);
    let provider_exits = Arc::new(AtomicUsize::new(0));
    let cleanup_error = ConnectorError::new(
        ConnectorErrorKind::Unavailable,
        "same Current registration cleanup remains retained",
    );
    let provider = Arc::new(OriginalRegistrationChildFixture {
        descriptor: fixture.caps.descriptor.clone(),
        epoch: fixture.caps.epoch,
        cleanup: Mutex::new(Some(cleanup)),
        cleanup_outcome: ExternalMutationFinalization::Failed(
            ConnectorMutationFailure::from_connector(cleanup_error.clone()),
        ),
        calls: AtomicUsize::new(0),
        exits: provider_exits.clone(),
        owner: payload,
    });
    let host = ConnectorControlHost::new();
    host.register(actual_generation(&fixture, provider.clone()))
        .unwrap();
    let instance_id = fixture.caps.descriptor.instance_id.clone();
    let planning = host.acquire_current(&instance_id).unwrap();
    let g1 = planning.control_runtime_id();
    let g1_key = ConnectorProviderBindingKey {
        instance_id: instance_id.clone(),
        incarnation: planning.binding().incarnation(),
    };
    host.record_installed_backend(&g1_key, "original-be")
        .unwrap();
    let registration = CurrentProjectionRegistration::new();
    let request = ConnectorTableObjectCaptureRequest {
        table: fixture.managed.table().clone(),
        resolution: ConnectorTableResolution::StrictBaseTable,
        selector: ConnectorTableObjectSelector::Current,
        context: context(),
    };
    // Use the actual registered factory and original CPU job, including join.
    let binding = current_with_driver(
        &cpu.control,
        &mut cpu.driver,
        registration
            .capture
            .capture(&planning, &cpu.factory, request.clone()),
    )
    .await
    .unwrap();
    assert_eq!(binding.object_id, *fixture.managed.object_id());

    // Establish RegistrationRequired through the existing management authors.
    // This fixture's recovery evidence is not a cleanup/physical-exit receipt.
    let observed = decode_current_keyed_documents(
        fixture.caps.current.lock().unwrap().clone(),
        fixture.catalog.clone(),
        Default::default(),
    )
    .unwrap();
    let entrance = ManagementEntrance::new(
        observed.deployment_owner.clone(),
        ProcessIncarnation::parse("current-root-generation-successor").unwrap(),
    );
    let old_incarnation = observed.process_incarnation.clone();
    let barrier = match EffectResponsibility::new(
        EffectIdentity::from_bytes([61; 16]),
        fixture.managed.clone(),
        old_incarnation.clone(),
        EffectScope::CATALOG_AND_OBJECT_DELETION,
        ManagementTimestamp::from_unix_millis(1_000),
    )
    .record_terminal(EffectDisposition::CommitUnknown)
    {
        EffectTerminalFact::CommitUnknown(effect) => effect,
        _ => unreachable!(),
    };
    let isolation = IsolationEvidence::try_new(
        fixture.managed.clone(),
        old_incarnation.clone(),
        ManagementTimestamp::from_unix_millis(1_100),
        "fixture previous management writer was isolated",
    )
    .unwrap();
    let completion = ActualCompletionEvidence::try_new(
        barrier.responsibility().identity(),
        fixture.managed.clone(),
        EffectScope::CATALOG_AND_OBJECT_DELETION,
        ManagementTimestamp::from_unix_millis(1_200),
        "fixture prior management effect audit",
    )
    .unwrap();
    let permit = ReadmissionEvaluator::default()
        .from_actual_completion(&barrier, &isolation, &completion)
        .unwrap();
    let mut state = entrance
        .begin_recovered_target(
            fixture.managed.clone(),
            ManagementContinuation::SameOwner {
                previous_incarnation: old_incarnation,
            },
            barrier,
        )
        .unwrap();
    state.accept_readmission_permit(permit).unwrap();
    let pending = state
        .begin_current_observation(ManagementObservationRequestId::from_bytes([62; 16]))
        .unwrap();
    let documents = planning.derive_document_storage_lease().unwrap();
    let (phase, _) = state
        .complete_keyed_current_observation_async(
            pending,
            &documents,
            keyed_request(
                &documents,
                fixture.managed.table().clone(),
                fixture.managed.object_id().clone(),
                context(),
            ),
        )
        .await
        .unwrap();
    assert!(matches!(
        phase,
        ManagementObservationPhase::RegistrationRequired(_)
    ));
    {
        let source = OriginalManagedTargetCurrentSource {
            entrance: &entrance,
            connector_control: &host,
            published: None,
            convergence: MvConvergence::Readmission {
                previous_incarnation: observed.process_incarnation.clone(),
                permits: vec![],
            },
            metadata_cpu: &cpu.factory,
            registration: &registration,
        };
        source
            .register_incarnation(&mut state, &planning, &documents, &observed, &context())
            .await
            .unwrap();
    }
    // register_incarnation installed this same lease in its normal preparation
    // slot before the Provider borrower. The test never writes that slot.
    assert_eq!(
        registration
            .preparation
            .get()
            .unwrap()
            .planning
            .control_runtime_id(),
        g1
    );
    let operation = registration.observation().operation_id().unwrap();
    assert_eq!(provider.calls.load(Ordering::Acquire), 1);
    assert_eq!(provider_exits.load(Ordering::Acquire), 0);
    assert!(registration.observation().needed_committed().is_some());
    assert!(registration.observation().applied_receipt().is_none());
    drop((planning, documents, observed, binding, state));

    host.retire_current(&instance_id).unwrap();
    let g2_catalog = CatalogHandle::new(instance_id.clone(), CatalogVersion::from_bytes([2; 32]));
    let g2_binding = novarocks_catalog_application::test_support::test_control_binding_for(
        instance_id.clone(),
        89,
    )
    .with_catalog_properties(
        CatalogProperties::new(
            g2_catalog.clone(),
            ConnectorProviderId::parse("iceberg").unwrap(),
            2,
            vec![],
            vec![],
        )
        .unwrap(),
    )
    .unwrap();
    host.register(g2_binding).unwrap();
    let g2 = host.acquire_current(&instance_id).unwrap();
    assert_ne!(g2.control_runtime_id(), g1);
    assert_eq!(g2.binding().catalog_handle().unwrap(), &g2_catalog);
    drop(g2);
    assert!(
        host.reachable_catalog_handles()
            .unwrap()
            .contains(&fixture.catalog)
    );
    assert!(host.take_ready_retires().unwrap().is_empty());

    let pause = Arc::new(PauseResultWake {
        caller: std::thread::current().id(),
        caller_wakes: AtomicUsize::new(0),
        read_returned: local.read_returned.clone(),
        once: AtomicBool::new(false),
        footer: footer_gate.clone(),
    });
    let wake = Waker::from(pause.clone());
    assert!(
        !store
            .poll_progress(&mut Context::from_waker(&wake))
            .unwrap()
    );
    read_gate.reached().await;
    read_gate.release();
    footer_gate.reached().await;
    let mut cx = Context::from_waker(Waker::noop());
    assert!(!store.poll_actual_quiescent(&mut cx).unwrap());
    assert_eq!(local.results.load(Ordering::Acquire), 1);
    assert_eq!(local.exits.load(Ordering::Acquire), 0);
    assert_eq!(cpu.control.snapshot().root_responsibilities, 1);
    assert!(
        registration
            .poll_registration_physical_closed(&mut cx)
            .is_pending()
    );

    footer_gate.release();
    tokio::time::timeout(
        Duration::from_secs(5),
        poll_fn(|cx| {
            if store.poll_actual_quiescent(cx).unwrap() {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        }),
    )
    .await
    .expect("the actual QB footer, not the Get result, proves quiescence");
    assert_eq!(local.exits.load(Ordering::Acquire), 1);
    finish_cleanup.send(()).unwrap();
    tokio::time::timeout(
        Duration::from_secs(5),
        poll_fn(|cx| registration.poll_registration_physical_closed(cx)),
    )
    .await
    .expect("the original Current child must actually exit");
    assert_eq!(provider_exits.load(Ordering::Acquire), 1);
    assert!(registration.poll_registration_closed(&mut cx).is_pending());
    assert!(registration.local_health_failure().is_none());
    {
        let original = registration.observation();
        assert_eq!(original.operation_id(), Some(operation));
        assert!(original.provider_exited());
        assert!(original.original_owner().is_some());
        assert!(original.applied_receipt().is_none());
        match original.validated_outcome().unwrap().unwrap() {
            ExternalMutationOutcome::KnownCommitted {
                effect: ExternalMutationEffect::NoOp,
                cleanup: ExternalMutationFinalization::Failed(error),
                ..
            } => assert_eq!(error.connector_source(), Some(&cleanup_error)),
            other => panic!("the original business/cleanup facts changed: {other:?}"),
        }
    }

    // The single physical QB worker really runs a new Close while the exact
    // G1 publication inventory remains retained. No footer is injected.
    assert!(store.poll_closed(&mut cx).is_pending());
    close_gate.reached().await;
    assert_eq!(local.exits.load(Ordering::Acquire), 1);
    close_gate.release();
    tokio::time::timeout(Duration::from_secs(5), poll_fn(|cx| store.poll_closed(cx)))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(local.results.load(Ordering::Acquire), 2);
    assert_eq!(local.exits.load(Ordering::Acquire), 2);
    assert!(local.is_complete());

    // The SAME Root can also run another actual registered metadata CPU job.
    // The original preparation is the only strong planning-lease holder.
    // Borrow it directly rather than adding a test-local G1 lifetime.
    let exact = &registration.preparation.get().unwrap().planning;
    let next = CurrentProjectionRegistration::new();
    current_with_driver(
        &cpu.control,
        &mut cpu.driver,
        next.capture.capture(exact, &cpu.factory, request),
    )
    .await
    .unwrap();
    assert!(next.poll_registration_physical_closed(&mut cx).is_ready());
    drop(next);
    let exact_mutation = host.acquire_exact_mutation(g1).unwrap();
    assert_eq!(exact_mutation.control_runtime_id(), g1);
    drop((exact_mutation, provider));
    assert!(payload_weak.upgrade().is_some());
    assert!(host.take_ready_retires().unwrap().is_empty());
    let reachable = host.reachable_catalog_handles().unwrap();
    assert!(reachable.contains(&fixture.catalog) && reachable.contains(&g2_catalog));
    assert!(registration.poll_registration_closed(&mut cx).is_pending());
    assert_eq!(cpu.control.snapshot().root_responsibilities, 1);
    assert!(scope.check().is_ok());

    // Test teardown is NOT a disposition or a production retirement verdict.
    // The assertions above deliberately leave full Current closure Pending.
    drop((
        registration,
        entrance,
        local_context,
        local,
        store,
        pause,
        wake,
        scope,
    ));
    workers
        .shutdown_until(Instant::now() + Duration::from_secs(5))
        .await
        .unwrap();
    cpu.close().await;
}
