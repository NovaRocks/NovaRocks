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

use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use novarocks_spi::connector::{
    CatalogHandle, CatalogVersion, ConnectorCommittedVersion, ConnectorControlRuntimeId,
    ConnectorDocumentManagementOperation, ConnectorInstanceId, ConnectorProviderBindingKey,
    ConnectorRequestContext, ConnectorStopOwner, ConnectorTableIdentity, ConnectorTableObjectId,
    MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES, MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES, ProviderBindingEpoch,
};

use super::*;

fn owner(value: &str) -> DeploymentOwner {
    DeploymentOwner::parse(value).unwrap()
}

fn incarnation(value: &str) -> ProcessIncarnation {
    ProcessIncarnation::parse(value).unwrap()
}

fn runtime_id(value: u8) -> ConnectorControlRuntimeId {
    ConnectorControlRuntimeId::from_bytes([value; 16])
}

fn table(name: &str) -> ConnectorTableIdentity {
    ConnectorTableIdentity {
        instance_id: ConnectorInstanceId::parse("iceberg").unwrap(),
        namespace: Arc::from("db"),
        table: Arc::from(name),
    }
}

fn catalog(version: u8) -> CatalogHandle {
    CatalogHandle::new(
        ConnectorInstanceId::parse("iceberg").unwrap(),
        CatalogVersion::from_bytes([version; 32]),
    )
}

fn object(value: &'static [u8]) -> ConnectorTableObjectId {
    ConnectorTableObjectId::try_new(Bytes::from_static(value)).unwrap()
}

fn target(name: &str, object_value: &'static [u8]) -> ManagedMvTarget {
    ManagedMvTarget::try_new(catalog(1), table(name), object(object_value)).unwrap()
}

fn request_context() -> ConnectorRequestContext {
    ConnectorRequestContext::try_new(
        Instant::now() + Duration::from_secs(30),
        ConnectorStopOwner::new().view(),
        MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
        MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
    )
    .unwrap()
}

/// The native retained lease validates the reply's exact scope; tests do not
/// write a seal or construct a FreshManagementObservation on this boundary.
struct KeyedManagementObservationCaps {
    descriptor: novarocks_spi::connector::ConnectorInstanceDescriptor,
    epoch: ProviderBindingEpoch,
    reply: std::sync::Mutex<
        Result<
            novarocks_spi::connector::KeyedDocumentView,
            novarocks_spi::connector::KeyedDocumentReadError,
        >,
    >,
    after_resolution: std::sync::Mutex<Option<novarocks_spi::connector::KeyedDocumentView>>,
    conclusions:
        std::sync::Mutex<std::collections::VecDeque<novarocks_spi::connector::CommitConclusion>>,
    calls: Arc<std::sync::atomic::AtomicUsize>,
    async_calls: std::sync::atomic::AtomicUsize,
    async_exits: std::sync::atomic::AtomicUsize,
    async_gate: std::sync::Mutex<Option<tokio::sync::oneshot::Receiver<()>>>,
}

fn unused_keyed_management_test_capability() -> novarocks_spi::connector::ConnectorError {
    novarocks_spi::connector::ConnectorError::new(
        novarocks_spi::connector::ConnectorErrorKind::Unsupported,
        "not used by keyed management observation tests",
    )
}

impl novarocks_spi::connector::ConnectorDocumentStorageObservation
    for KeyedManagementObservationCaps
{
    fn descriptor(&self) -> &novarocks_spi::connector::ConnectorInstanceDescriptor {
        &self.descriptor
    }
    fn incarnation(&self) -> ProviderBindingEpoch {
        self.epoch
    }
    fn observe_keyed_documents(
        &self,
        _request: novarocks_spi::connector::KeyedDocumentObservationRequest,
    ) -> Result<
        novarocks_spi::connector::KeyedDocumentView,
        novarocks_spi::connector::KeyedDocumentReadError,
    > {
        self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.reply.lock().unwrap().clone()
    }
    fn observe_keyed_documents_async(
        &self,
        _request: novarocks_spi::connector::KeyedDocumentObservationRequest,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<
                        novarocks_spi::connector::KeyedDocumentView,
                        novarocks_spi::connector::KeyedDocumentReadError,
                    >,
                > + Send
                + '_,
        >,
    > {
        self.async_calls
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let gate = self.async_gate.lock().unwrap().take();
        Box::pin(async move {
            struct Exit<'a>(&'a std::sync::atomic::AtomicUsize);
            impl Drop for Exit<'_> {
                fn drop(&mut self) {
                    self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                }
            }
            let _exit = Exit(&self.async_exits);
            if let Some(gate) = gate {
                gate.await.expect("test releases the original observation");
            }
            self.reply.lock().unwrap().clone()
        })
    }
    fn observe_full_keyed_documents(
        &self,
        _request: novarocks_spi::connector::KeyedDocumentObservationRequest,
    ) -> Result<
        novarocks_spi::connector::FullKeyedDocumentView,
        novarocks_spi::connector::KeyedDocumentReadError,
    > {
        Err(unused_keyed_management_test_capability().into())
    }
    fn decode_keyed_slot_change_evidence(
        &self,
        _request: novarocks_spi::connector::KeyedDocumentObservationRequest,
        _evidence: novarocks_spi::connector::ExternalMutationEvidence,
    ) -> Result<
        novarocks_spi::connector::UnknownCommitEvidence,
        novarocks_spi::connector::ConnectorError,
    > {
        Err(unused_keyed_management_test_capability())
    }
    fn resolve_keyed_commit(
        &self,
        _request: novarocks_spi::connector::KeyedDocumentObservationRequest,
        _evidence: novarocks_spi::connector::UnknownCommitEvidence,
    ) -> Result<novarocks_spi::connector::CommitConclusion, novarocks_spi::connector::ConnectorError>
    {
        let conclusion = self
            .conclusions
            .lock()
            .unwrap()
            .pop_front()
            .ok_or_else(unused_keyed_management_test_capability)?;
        if matches!(
            conclusion,
            novarocks_spi::connector::CommitConclusion::Committed(_)
        ) && let Some(view) = self.after_resolution.lock().unwrap().take()
        {
            *self.reply.lock().unwrap() = Ok(view);
        }
        Ok(conclusion)
    }
    fn discover_documents(
        &self,
        _request: novarocks_spi::connector::ConnectorDocumentDiscoveryRequest,
    ) -> Result<
        novarocks_spi::connector::ConnectorDocumentDiscoveryPage,
        novarocks_spi::connector::ConnectorError,
    > {
        Err(unused_keyed_management_test_capability())
    }
}

fn keyed_management_view(
    target: &ManagedMvTarget,
    marker_owner: &str,
    marker_incarnation: &str,
    metadata: u8,
) -> novarocks_spi::connector::KeyedDocumentView {
    use crate::persistence::test_support::{KeyedProjectionFixture, observed_current_keyed};
    let mut fixture = KeyedProjectionFixture::new(
        crate::product::MvTarget::from_parts(
            Some(target.table().instance_id.as_str()),
            &target.table().namespace,
            &target.table().table,
        ),
        None,
    );
    fixture.object_id = target.object_id().clone();
    fixture.metadata_version =
        ConnectorCommittedVersion::try_new(Bytes::from(vec![metadata]), Some(metadata as i64 + 1))
            .unwrap();
    let facts = fixture
        .build()
        .expect("canonical exact keyed management fixture");
    let mut view = observed_current_keyed(&facts, target.catalog().clone()).view;
    view.owner = ConnectorProviderBindingKey {
        instance_id: target.table().instance_id.clone(),
        incarnation: ProviderBindingEpoch::from_bytes([7; 16]),
    };
    view.slots
        .get_mut(&crate::persistence::keyed_documents::management_slot_name().unwrap())
        .unwrap()
        .value = crate::persistence::keyed_documents::management_slot(
        &owner(marker_owner),
        &incarnation(marker_incarnation),
    )
    .unwrap();
    view
}

fn keyed_management_lease(
    target: &ManagedMvTarget,
    reply: Result<
        novarocks_spi::connector::KeyedDocumentView,
        novarocks_spi::connector::KeyedDocumentReadError,
    >,
) -> (
    novarocks_spi::connector::ConnectorDocumentStorageLease,
    novarocks_spi::connector::KeyedDocumentObservationRequest,
    Arc<std::sync::atomic::AtomicUsize>,
) {
    let (lease, request, caps) = keyed_management_lease_with_caps(target, reply);
    (lease, request, Arc::clone(&caps.calls))
}

fn keyed_management_lease_with_caps(
    target: &ManagedMvTarget,
    reply: Result<
        novarocks_spi::connector::KeyedDocumentView,
        novarocks_spi::connector::KeyedDocumentReadError,
    >,
) -> (
    novarocks_spi::connector::ConnectorDocumentStorageLease,
    novarocks_spi::connector::KeyedDocumentObservationRequest,
    Arc<KeyedManagementObservationCaps>,
) {
    use novarocks_spi::connector::*;
    let epoch = ProviderBindingEpoch::from_bytes([7; 16]);
    let descriptor = ConnectorInstanceDescriptor {
        provider_id: ConnectorProviderId::parse("iceberg").unwrap(),
        instance_id: target.table().instance_id.clone(),
    };
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let observation = Arc::new(KeyedManagementObservationCaps {
        descriptor: descriptor.clone(),
        epoch,
        reply: std::sync::Mutex::new(reply),
        after_resolution: std::sync::Mutex::new(None),
        conclusions: std::sync::Mutex::new(std::collections::VecDeque::new()),
        calls: Arc::clone(&calls),
        async_calls: std::sync::atomic::AtomicUsize::new(0),
        async_exits: std::sync::atomic::AtomicUsize::new(0),
        async_gate: std::sync::Mutex::new(None),
    });
    let documents = ConnectorDocumentStorageBinding::try_new(
        descriptor,
        epoch,
        Some(observation.clone()),
        None,
    )
    .unwrap();
    let properties = CatalogProperties::new(
        target.catalog().clone(),
        ConnectorProviderId::parse("iceberg").unwrap(),
        1,
        vec![],
        vec![],
    )
    .unwrap();
    let binding = novarocks_catalog_application::test_support::test_control_binding_for(
        target.table().instance_id.clone(),
        7,
    )
    .with_catalog_properties(properties)
    .and_then(|binding| binding.try_with_document_storage(Some(documents)))
    .unwrap();
    let lease = ConnectorControlPlanningLease::new(Arc::new(binding), || {})
        .derive_document_storage_lease()
        .unwrap();
    let request = KeyedDocumentObservationRequest {
        owner: lease.owner().clone(),
        catalog_handle: target.catalog().clone(),
        target: target.table().clone(),
        object_id: target.object_id().clone(),
        context: request_context(),
    };
    (lease, request, observation)
}

fn observe_keyed_management_mismatch(
    entrance: &ManagementEntrance,
    target: &ManagedMvTarget,
    marker_owner: &str,
    marker_incarnation: &str,
    metadata: u8,
) -> bool {
    let (lease, request, calls) = keyed_management_lease(
        target,
        Ok(keyed_management_view(
            target,
            marker_owner,
            marker_incarnation,
            metadata,
        )),
    );
    let result = entrance
        .close_on_keyed_current_management_mismatch(&lease, request)
        .unwrap();
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    result
}

fn fresh_observation(
    request_id: [u8; 16],
    target: &ManagedMvTarget,
    marker_owner: &str,
    marker_incarnation: &str,
    metadata: u8,
) -> FreshManagementObservation {
    FreshManagementObservation::for_test(
        ManagementObservationRequestId::from_bytes(request_id),
        target.clone(),
        owner(marker_owner),
        incarnation(marker_incarnation),
        ConnectorCommittedVersion::try_new(Bytes::from(vec![metadata]), Some(metadata as i64 + 1))
            .unwrap(),
    )
}

fn unknown_effect(
    target: &ManagedMvTarget,
    effect_byte: u8,
    old_incarnation: &str,
    scope: EffectScope,
    dispatched_at: u64,
) -> UnsettledEffect {
    match EffectResponsibility::new(
        EffectIdentity::from_bytes([effect_byte; 16]),
        target.clone(),
        incarnation(old_incarnation),
        scope,
        ManagementTimestamp::from_unix_millis(dispatched_at),
    )
    .record_terminal(EffectDisposition::CommitUnknown)
    {
        EffectTerminalFact::CommitUnknown(effect) => effect,
        _ => unreachable!(),
    }
}

fn isolation(target: &ManagedMvTarget, old_incarnation: &str, at: u64) -> IsolationEvidence {
    IsolationEvidence::try_new(
        target.clone(),
        incarnation(old_incarnation),
        ManagementTimestamp::from_unix_millis(at),
        "deployment controller isolated the old writer",
    )
    .unwrap()
}

fn catalog_terminal(
    target: &ManagedMvTarget,
    effect_byte: u8,
    dispatching_incarnation: &str,
    disposition: EffectDisposition,
) -> EffectTerminalFact {
    EffectResponsibility::new(
        EffectIdentity::from_bytes([effect_byte; 16]),
        target.clone(),
        incarnation(dispatching_incarnation),
        EffectScope::CATALOG_COMMIT,
        ManagementTimestamp::from_unix_millis(1_300),
    )
    .record_terminal(disposition)
}

fn ready_observation_state(
    target: ManagedMvTarget,
    local_owner: &str,
    local_incarnation: &str,
) -> ManagementObservationState {
    let mut state = ManagementObservationState::try_new(
        target.clone(),
        owner(local_owner),
        incarnation(local_incarnation),
        ManagementContinuation::SameOwner {
            previous_incarnation: incarnation(local_incarnation),
        },
        vec![],
        None,
        None,
    )
    .unwrap();
    let _pending = state
        .begin_current_observation(ManagementObservationRequestId::from_bytes([1; 16]))
        .unwrap();
    assert_eq!(
        state
            .accept_current_observation(fresh_observation(
                [1; 16],
                &target,
                local_owner,
                local_incarnation,
                1,
            ))
            .unwrap(),
        ManagementObservationPhase::Ready
    );
    state
}

#[test]
fn policy_window_opens_exactly_at_the_deadline_without_reclassifying_unknown() {
    let target = target("mv", b"object-a");
    let effect = unknown_effect(&target, 3, "old", EffectScope::CATALOG_COMMIT, 1_000);
    let isolation = isolation(&target, "old", 1_100);
    let guarantee = RemoteEffectLifetimeGuarantee::try_new(
        EffectScope::CATALOG_COMMIT,
        Duration::from_millis(500),
        Duration::from_millis(50),
        RemoteEffectGuaranteeBasis::ProviderServiceContract,
        "catalog service contract",
    )
    .unwrap();
    let clock = VirtualManagementClock::new(ManagementTimestamp::from_unix_millis(1_649));
    let mut evaluator = ReadmissionEvaluator::default();

    assert!(matches!(
        evaluator.from_policy_window(
            &effect,
            &isolation,
            &guarantee,
            ReadmissionMode::AutomaticWhenGuaranteed,
            &clock,
        ),
        Err(ReadmissionError::WindowNotElapsed { .. })
    ));
    clock.set(ManagementTimestamp::from_unix_millis(1_650));
    let permit = evaluator
        .from_policy_window(
            &effect,
            &isolation,
            &guarantee,
            ReadmissionMode::AutomaticWhenGuaranteed,
            &clock,
        )
        .unwrap();
    assert!(permit.preserves_unknown_disposition());
    assert_eq!(
        effect.original_disposition(),
        EffectDisposition::CommitUnknown
    );
}

#[test]
fn local_timeout_and_idempotency_retention_cannot_mint_a_remote_guarantee() {
    for basis in [
        RemoteEffectGuaranteeBasis::ClientRequestTimeout,
        RemoteEffectGuaranteeBasis::GatewayTimeout,
        RemoteEffectGuaranteeBasis::CredentialExpiry,
        RemoteEffectGuaranteeBasis::IdempotencyRetention,
        RemoteEffectGuaranteeBasis::EmpiricalPercentile,
    ] {
        assert_eq!(
            RemoteEffectLifetimeGuarantee::try_new(
                EffectScope::CATALOG_COMMIT,
                Duration::from_secs(30),
                Duration::ZERO,
                basis,
                "non-authoritative timeout",
            )
            .unwrap_err(),
            ReadmissionError::InvalidGuarantee
        );
    }
}

#[test]
fn deadline_rounds_sub_millisecond_guarantees_up_and_rejects_overflow() {
    let target = target("mv", b"object-a");
    let effect = unknown_effect(&target, 36, "old", EffectScope::CATALOG_COMMIT, 1_000);
    let isolation_evidence = isolation(&target, "old", 1_000);
    let guarantee = RemoteEffectLifetimeGuarantee::try_new(
        EffectScope::CATALOG_COMMIT,
        Duration::from_nanos(1),
        Duration::ZERO,
        RemoteEffectGuaranteeBasis::DeploymentEnforcedBound,
        "deployment network and service bound",
    )
    .unwrap();
    let clock = VirtualManagementClock::new(ManagementTimestamp::from_unix_millis(1_000));
    assert!(matches!(
        ReadmissionEvaluator::default().from_policy_window(
            &effect,
            &isolation_evidence,
            &guarantee,
            ReadmissionMode::AutomaticWhenGuaranteed,
            &clock,
        ),
        Err(ReadmissionError::WindowNotElapsed { .. })
    ));

    let overflow_effect = unknown_effect(&target, 37, "old", EffectScope::CATALOG_COMMIT, u64::MAX);
    let overflow_isolation = isolation(&target, "old", u64::MAX);
    let clock = VirtualManagementClock::new(ManagementTimestamp::from_unix_millis(u64::MAX));
    assert_eq!(
        ReadmissionEvaluator::default()
            .from_policy_window(
                &overflow_effect,
                &overflow_isolation,
                &guarantee,
                ReadmissionMode::AutomaticWhenGuaranteed,
                &clock,
            )
            .unwrap_err(),
        ReadmissionError::Overflow
    );
}

#[test]
fn unsealed_management_observation_is_rejected_before_owner_policy() {
    let target = target("mv", b"object-a");
    let mut state = ManagementObservationState::try_new(
        target.clone(),
        owner("deployment-a"),
        incarnation("new"),
        ManagementContinuation::SameOwner {
            previous_incarnation: incarnation("old"),
        },
        vec![],
        None,
        None,
    )
    .unwrap();
    let pending = state
        .begin_current_observation(ManagementObservationRequestId::from_bytes([38; 16]))
        .unwrap();
    let mut escaped = keyed_management_view(&target, "deployment-a", "old", 1);
    escaped.owner.incarnation = ProviderBindingEpoch::from_bytes([9; 16]);
    let (lease, request, calls) = keyed_management_lease(&target, Ok(escaped));
    assert_eq!(
        state
            .complete_keyed_current_observation(pending, &lease, request)
            .unwrap_err(),
        ManagementObservationError::UnsealedObservation
    );
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert_eq!(
        state.phase(),
        ManagementObservationPhase::AwaitingFreshObservation
    );
    assert_eq!(
        state
            .begin_current_observation(ManagementObservationRequestId::from_bytes([38; 16]))
            .err()
            .unwrap(),
        ManagementObservationError::ReusedObservationRequest
    );
}

#[test]
fn stale_and_replayed_observations_preserve_the_active_request() {
    let target = target("mv", b"object-a");
    let mut state = ready_observation_state(target.clone(), "deployment-a", "inc-a");
    state
        .begin_current_observation(ManagementObservationRequestId::from_bytes([39; 16]))
        .unwrap();
    assert_eq!(
        state
            .accept_current_observation(fresh_observation(
                [40; 16],
                &target,
                "deployment-a",
                "inc-a",
                2,
            ))
            .unwrap_err(),
        ManagementObservationError::StaleObservation
    );
    assert_eq!(
        state
            .begin_current_observation(ManagementObservationRequestId::from_bytes([41; 16]))
            .err()
            .unwrap(),
        ManagementObservationError::ObservationAlreadyPending
    );
    state
        .accept_current_observation(fresh_observation(
            [39; 16],
            &target,
            "deployment-a",
            "inc-a",
            2,
        ))
        .unwrap();
    assert_eq!(
        state
            .begin_current_observation(ManagementObservationRequestId::from_bytes([39; 16]))
            .err()
            .unwrap(),
        ManagementObservationError::ReusedObservationRequest
    );

    state
        .begin_current_observation(ManagementObservationRequestId::from_bytes([41; 16]))
        .unwrap();
    assert_eq!(
        state
            .begin_current_observation(ManagementObservationRequestId::from_bytes([39; 16]))
            .err()
            .unwrap(),
        ManagementObservationError::ReusedObservationRequest
    );
    assert_eq!(
        state
            .accept_current_observation(fresh_observation(
                [39; 16],
                &target,
                "deployment-a",
                "inc-a",
                2,
            ))
            .unwrap_err(),
        ManagementObservationError::ReusedObservationRequest
    );
    state
        .accept_current_observation(fresh_observation(
            [41; 16],
            &target,
            "deployment-a",
            "inc-a",
            3,
        ))
        .unwrap();
}

#[test]
fn isolation_cutoff_restarts_the_full_policy_window() {
    let target = target("mv", b"object-a");
    let effect = unknown_effect(&target, 4, "old", EffectScope::CATALOG_COMMIT, 1_000);
    let isolation = isolation(&target, "old", 2_000);
    let guarantee = RemoteEffectLifetimeGuarantee::try_new(
        EffectScope::CATALOG_COMMIT,
        Duration::from_millis(500),
        Duration::ZERO,
        RemoteEffectGuaranteeBasis::ProviderServiceContract,
        "catalog service contract",
    )
    .unwrap();
    let clock = VirtualManagementClock::new(ManagementTimestamp::from_unix_millis(2_499));
    let mut evaluator = ReadmissionEvaluator::default();
    assert!(matches!(
        evaluator.from_policy_window(
            &effect,
            &isolation,
            &guarantee,
            ReadmissionMode::AutomaticWhenGuaranteed,
            &clock,
        ),
        Err(ReadmissionError::WindowNotElapsed { .. })
    ));
}

#[test]
fn manual_mode_requires_one_current_exact_challenge_and_real_evidence_text() {
    let target = target("mv", b"object-a");
    let effect = unknown_effect(&target, 5, "old", EffectScope::CATALOG_COMMIT, 1_000);
    let isolation = isolation(&target, "old", 1_100);
    let clock = VirtualManagementClock::new(ManagementTimestamp::from_unix_millis(99_000));
    let guarantee = RemoteEffectLifetimeGuarantee::try_new(
        EffectScope::CATALOG_COMMIT,
        Duration::from_millis(1),
        Duration::ZERO,
        RemoteEffectGuaranteeBasis::ProviderServiceContract,
        "catalog service contract",
    )
    .unwrap();
    let challenge = ReadmissionChallenge::from_bytes([9; 16]);
    let mut evaluator = ReadmissionEvaluator::default();
    assert_eq!(
        evaluator
            .from_policy_window(
                &effect,
                &isolation,
                &guarantee,
                ReadmissionMode::OperatorDeclarationOnly,
                &clock,
            )
            .unwrap_err(),
        ReadmissionError::ManualMode
    );
    evaluator.issue_challenge(challenge).unwrap();
    let declaration = ManualReadmissionDeclaration::try_new(
        challenge,
        effect.responsibility().identity(),
        target,
        incarnation("old"),
        EffectScope::CATALOG_COMMIT,
        "operator@example.com",
        "provider audit confirms the request reached a terminal state",
        ManagementTimestamp::from_unix_millis(1_200),
    )
    .unwrap();
    assert!(
        evaluator
            .from_manual_declaration(&effect, &isolation, &declaration)
            .unwrap()
            .preserves_unknown_disposition()
    );
    assert_eq!(
        evaluator
            .from_manual_declaration(&effect, &isolation, &declaration)
            .unwrap_err(),
        ReadmissionError::MissingChallenge
    );
    assert_eq!(
        evaluator.issue_challenge(challenge).unwrap_err(),
        ReadmissionError::ReusedChallenge
    );
}

#[test]
fn catalog_guarantee_cannot_cover_deletion_or_enable_automatic_gc() {
    let target = target("mv", b"object-a");
    let effect = unknown_effect(&target, 6, "old", EffectScope::OBJECT_DELETION, 1_000);
    let isolation = isolation(&target, "old", 1_100);
    let catalog_only = RemoteEffectLifetimeGuarantee::try_new(
        EffectScope::CATALOG_COMMIT,
        Duration::from_secs(10),
        Duration::from_secs(1),
        RemoteEffectGuaranteeBasis::ProviderServiceContract,
        "catalog service contract",
    )
    .unwrap();
    let clock = VirtualManagementClock::new(ManagementTimestamp::from_unix_millis(99_000));
    assert_eq!(
        ReadmissionEvaluator::default()
            .from_policy_window(
                &effect,
                &isolation,
                &catalog_only,
                ReadmissionMode::AutomaticWhenGuaranteed,
                &clock,
            )
            .unwrap_err(),
        ReadmissionError::GuaranteeScopeMismatch
    );
    let gc = GarbageCollectionSafetyPolicy::new(
        Duration::from_secs(30),
        Some(catalog_only.clone()),
        Some(catalog_only),
        Duration::from_secs(1),
        Duration::from_secs(2),
        Duration::from_secs(3),
    );
    assert_eq!(
        gc.minimum_safe_age().unwrap_err(),
        ReadmissionError::MissingDeleteGuarantee
    );
}

#[test]
fn gc_safe_age_uses_catalog_reference_tail_and_validates_a_separate_delete_tail() {
    let catalog = RemoteEffectLifetimeGuarantee::try_new(
        EffectScope::CATALOG_COMMIT,
        Duration::from_secs(13),
        Duration::from_secs(4),
        RemoteEffectGuaranteeBasis::ProviderServiceContract,
        "catalog commit service contract",
    )
    .unwrap();
    let delete = RemoteEffectLifetimeGuarantee::try_new(
        EffectScope::OBJECT_DELETION,
        Duration::from_secs(11),
        Duration::from_secs(2),
        RemoteEffectGuaranteeBasis::ProviderServiceContract,
        "object store service contract",
    )
    .unwrap();
    let gc = GarbageCollectionSafetyPolicy::new(
        Duration::from_secs(30),
        Some(catalog),
        Some(delete),
        Duration::from_secs(3),
        Duration::from_secs(5),
        Duration::from_secs(7),
    );
    assert_eq!(gc.minimum_safe_age().unwrap(), Duration::from_secs(62));
    assert_eq!(
        gc.deletion_effect_window().unwrap(),
        Duration::from_secs(13)
    );
}

#[test]
fn object_deletion_guarantee_cannot_replace_the_catalog_reference_tail() {
    let delete = RemoteEffectLifetimeGuarantee::try_new(
        EffectScope::OBJECT_DELETION,
        Duration::from_secs(11),
        Duration::from_secs(2),
        RemoteEffectGuaranteeBasis::ProviderServiceContract,
        "object store service contract",
    )
    .unwrap();
    let gc = GarbageCollectionSafetyPolicy::new(
        Duration::from_secs(30),
        Some(delete.clone()),
        Some(delete),
        Duration::from_secs(3),
        Duration::from_secs(5),
        Duration::from_secs(7),
    );
    assert_eq!(
        gc.minimum_safe_age().unwrap_err(),
        ReadmissionError::MissingReferenceGuarantee
    );
}

#[test]
fn clock_regression_permanently_closes_automatic_readmission_for_the_evaluator() {
    let target = target("mv", b"object-a");
    let effect = unknown_effect(&target, 7, "old", EffectScope::CATALOG_COMMIT, 1_000);
    let isolation = isolation(&target, "old", 1_100);
    let guarantee = RemoteEffectLifetimeGuarantee::try_new(
        EffectScope::CATALOG_COMMIT,
        Duration::from_millis(500),
        Duration::ZERO,
        RemoteEffectGuaranteeBasis::ProviderServiceContract,
        "catalog service contract",
    )
    .unwrap();
    let clock = VirtualManagementClock::new(ManagementTimestamp::from_unix_millis(1_200));
    let mut evaluator = ReadmissionEvaluator::default();
    let _ = evaluator.from_policy_window(
        &effect,
        &isolation,
        &guarantee,
        ReadmissionMode::AutomaticWhenGuaranteed,
        &clock,
    );
    clock.set(ManagementTimestamp::from_unix_millis(1_100));
    assert_eq!(
        evaluator
            .from_policy_window(
                &effect,
                &isolation,
                &guarantee,
                ReadmissionMode::AutomaticWhenGuaranteed,
                &clock,
            )
            .unwrap_err(),
        ReadmissionError::ClockRegressed
    );
    clock.set(ManagementTimestamp::from_unix_millis(99_000));
    assert_eq!(
        evaluator
            .from_policy_window(
                &effect,
                &isolation,
                &guarantee,
                ReadmissionMode::AutomaticWhenGuaranteed,
                &clock,
            )
            .unwrap_err(),
        ReadmissionError::ClockRegressed
    );
}

#[test]
fn same_owner_restart_requires_effect_closure_reobservation_and_registration() {
    let target = target("mv", b"object-a");
    let effect = unknown_effect(&target, 8, "old", EffectScope::CATALOG_COMMIT, 1_000);
    let isolation = isolation(&target, "old", 1_100);
    let completion = ActualCompletionEvidence::try_new(
        effect.responsibility().identity(),
        target.clone(),
        EffectScope::CATALOG_COMMIT,
        ManagementTimestamp::from_unix_millis(1_200),
        "catalog audit terminal receipt",
    )
    .unwrap();
    let permit = ReadmissionEvaluator::default()
        .from_actual_completion(&effect, &isolation, &completion)
        .unwrap();
    let mut state = ManagementObservationState::try_new(
        target.clone(),
        owner("deployment-a"),
        incarnation("new"),
        ManagementContinuation::SameOwner {
            previous_incarnation: incarnation("old"),
        },
        vec![effect],
        None,
        None,
    )
    .unwrap();
    assert_eq!(
        state.phase(),
        ManagementObservationPhase::AwaitingEffectClosure
    );
    state.accept_readmission_permit(permit).unwrap();
    let _pending = state
        .begin_current_observation(ManagementObservationRequestId::from_bytes([10; 16]))
        .unwrap();
    assert_eq!(
        state
            .accept_current_observation(fresh_observation(
                [10; 16],
                &target,
                "deployment-a",
                "old",
                2,
            ))
            .unwrap(),
        ManagementObservationPhase::RegistrationRequired(RegistrationRequirement::Incarnation)
    );
    state
        .record_registration_terminal(catalog_terminal(
            &target,
            30,
            "new",
            EffectDisposition::KnownCommitted,
        ))
        .unwrap();
    let _pending = state
        .begin_current_observation(ManagementObservationRequestId::from_bytes([11; 16]))
        .unwrap();
    assert_eq!(
        state
            .accept_current_observation(fresh_observation(
                [11; 16],
                &target,
                "deployment-a",
                "new",
                3,
            ))
            .unwrap(),
        ManagementObservationPhase::Ready
    );
}

#[test]
fn owner_handover_never_opens_before_owner_and_incarnation_are_reobserved() {
    let target = target("mv", b"object-a");
    let mut state = ManagementObservationState::try_new(
        target.clone(),
        owner("deployment-b"),
        incarnation("new-b"),
        ManagementContinuation::OwnerHandover {
            previous_owner: owner("deployment-a"),
            previous_incarnation: incarnation("old-a"),
        },
        vec![],
        None,
        None,
    )
    .unwrap();
    let _pending = state
        .begin_current_observation(ManagementObservationRequestId::from_bytes([12; 16]))
        .unwrap();
    assert_eq!(
        state
            .accept_current_observation(fresh_observation(
                [12; 16],
                &target,
                "deployment-a",
                "old-a",
                1,
            ))
            .unwrap(),
        ManagementObservationPhase::RegistrationRequired(
            RegistrationRequirement::OwnerAndIncarnation
        )
    );
    state
        .record_registration_terminal(catalog_terminal(
            &target,
            31,
            "new-b",
            EffectDisposition::KnownCommitted,
        ))
        .unwrap();
    let _pending = state
        .begin_current_observation(ManagementObservationRequestId::from_bytes([13; 16]))
        .unwrap();
    assert_eq!(
        state
            .accept_current_observation(fresh_observation(
                [13; 16],
                &target,
                "deployment-a",
                "old-a",
                2,
            ))
            .unwrap_err(),
        ManagementObservationError::ForeignOwner
    );
    assert_eq!(
        state.phase(),
        ManagementObservationPhase::ClosedForeignOwner
    );
}

#[test]
fn registration_unknown_returns_to_effect_closure_without_changing_its_result() {
    let target = target("mv", b"object-a");
    let mut state = ManagementObservationState::try_new(
        target.clone(),
        owner("deployment-a"),
        incarnation("new"),
        ManagementContinuation::SameOwner {
            previous_incarnation: incarnation("old"),
        },
        vec![],
        None,
        None,
    )
    .unwrap();
    let _pending = state
        .begin_current_observation(ManagementObservationRequestId::from_bytes([32; 16]))
        .unwrap();
    state
        .accept_current_observation(fresh_observation(
            [32; 16],
            &target,
            "deployment-a",
            "old",
            1,
        ))
        .unwrap();
    let terminal = catalog_terminal(&target, 33, "new", EffectDisposition::CommitUnknown);
    state
        .record_registration_terminal(terminal.clone())
        .unwrap();
    assert_eq!(
        state.phase(),
        ManagementObservationPhase::AwaitingEffectClosure
    );
    assert!(matches!(
        terminal,
        EffectTerminalFact::CommitUnknown(ref effect)
            if effect.original_disposition() == EffectDisposition::CommitUnknown
    ));
    assert!(
        state
            .begin_current_observation(ManagementObservationRequestId::from_bytes([34; 16]))
            .is_err()
    );
}

#[test]
fn process_rebuild_installs_a_barrier_instead_of_defaulting_to_ready() {
    let target = target("mv", b"object-a");
    let barrier = unknown_effect(
        &target,
        35,
        "old",
        EffectScope::CATALOG_AND_OBJECT_DELETION,
        1_000,
    );
    let entrance = ManagementEntrance::new(owner("deployment-a"), incarnation("new"));
    let readmission = entrance
        .begin_recovered_target(
            target.clone(),
            ManagementContinuation::SameOwner {
                previous_incarnation: incarnation("old"),
            },
            barrier,
        )
        .unwrap();
    assert_eq!(
        readmission.phase(),
        ManagementObservationPhase::AwaitingEffectClosure
    );
    let request = ManagementRequest::try_new(
        target.catalog().clone(),
        target.table().clone(),
        Some(target.object_id().clone()),
        ConnectorDocumentManagementOperation::SingleTargetUpdate,
        Some(ManagementDependencySet::new(
            [1; 32],
            [2; 32],
            None,
            runtime_id(1),
        )),
        EffectScope::CATALOG_COMMIT,
    )
    .unwrap();
    assert_eq!(
        entrance.acquire(request, || false).err().unwrap(),
        ManagementAdmissionError::EffectUnsettled
    );
}

#[test]
fn recovered_target_rejects_a_narrow_or_foreign_incarnation_barrier() {
    let target = target("mv", b"object-a");
    let entrance = ManagementEntrance::new(owner("deployment-a"), incarnation("new"));
    let continuation = || ManagementContinuation::SameOwner {
        previous_incarnation: incarnation("old"),
    };

    assert_eq!(
        entrance
            .begin_recovered_target(
                target.clone(),
                continuation(),
                unknown_effect(
                    &target,
                    36,
                    "foreign-old",
                    EffectScope::CATALOG_AND_OBJECT_DELETION,
                    1_000,
                ),
            )
            .err()
            .unwrap(),
        ManagementObservationError::InvalidRecoveryBarrier
    );
    assert_eq!(
        entrance
            .begin_recovered_target(
                target.clone(),
                ManagementContinuation::SameOwner {
                    previous_incarnation: incarnation("new"),
                },
                unknown_effect(
                    &target,
                    37,
                    "new",
                    EffectScope::CATALOG_AND_OBJECT_DELETION,
                    1_000,
                ),
            )
            .err()
            .unwrap(),
        ManagementObservationError::InvalidRecoveryBarrier
    );
    assert_eq!(
        entrance
            .begin_recovered_target(
                target.clone(),
                continuation(),
                unknown_effect(&target, 38, "old", EffectScope::CATALOG_COMMIT, 1_000,),
            )
            .err()
            .unwrap(),
        ManagementObservationError::InvalidRecoveryBarrier
    );

    let recovered = entrance
        .begin_recovered_target(
            target.clone(),
            continuation(),
            unknown_effect(
                &target,
                40,
                "old",
                EffectScope::CATALOG_AND_OBJECT_DELETION,
                1_000,
            ),
        )
        .unwrap();
    assert_eq!(
        recovered.phase(),
        ManagementObservationPhase::AwaitingEffectClosure
    );
}

#[test]
fn abandoned_recovery_observation_can_be_reissued_without_opening_admission() {
    let target = target("mv", b"object-a");
    let entrance = ManagementEntrance::new(owner("deployment-a"), incarnation("new"));
    let continuation = || ManagementContinuation::SameOwner {
        previous_incarnation: incarnation("old"),
    };
    let recovery = entrance
        .begin_recovered_target(
            target.clone(),
            continuation(),
            unknown_effect(
                &target,
                39,
                "old",
                EffectScope::CATALOG_AND_OBJECT_DELETION,
                1_000,
            ),
        )
        .unwrap();
    entrance.abandon_observation(recovery).unwrap();

    match entrance.begin_readmission(
        target.table(),
        ManagementContinuation::SameOwner {
            previous_incarnation: incarnation("different-old"),
        },
    ) {
        Err(error) => assert_eq!(error, ManagementObservationError::InvalidRecoveryBarrier),
        Ok(_) => panic!("a different continuation must not replace the recovery barrier"),
    }

    let replacement = entrance
        .begin_readmission(target.table(), continuation())
        .unwrap();
    assert_eq!(
        replacement.phase(),
        ManagementObservationPhase::AwaitingEffectClosure
    );
    let request = ManagementRequest::try_new(
        target.catalog().clone(),
        target.table().clone(),
        Some(target.object_id().clone()),
        ConnectorDocumentManagementOperation::SingleTargetUpdate,
        Some(ManagementDependencySet::new(
            [1; 32],
            [2; 32],
            None,
            runtime_id(1),
        )),
        EffectScope::CATALOG_COMMIT,
    )
    .unwrap();
    assert_eq!(
        entrance.acquire(request, || false).err().unwrap(),
        ManagementAdmissionError::EffectUnsettled
    );
}

#[test]
fn fresh_incarnation_mismatch_closes_the_installed_entrance_target() {
    let target = target("mv", b"object-a");
    let dependencies = ManagementDependencySet::new([1; 32], [2; 32], None, runtime_id(1));
    let entrance = ManagementEntrance::new(owner("deployment-a"), incarnation("new"));
    let mut state = ready_observation_state(target.clone(), "deployment-a", "new");
    entrance
        .install_observed_target(&state, dependencies.clone())
        .unwrap();
    let request = ManagementRequest::try_new(
        target.catalog().clone(),
        target.table().clone(),
        Some(target.object_id().clone()),
        ConnectorDocumentManagementOperation::SingleTargetUpdate,
        Some(dependencies.clone()),
        EffectScope::CATALOG_COMMIT,
    )
    .unwrap();
    let mut acquired_before_mismatch = entrance.acquire(request.clone(), || false).unwrap();
    let _pending = state
        .begin_current_observation(ManagementObservationRequestId::from_bytes([14; 16]))
        .unwrap();
    assert_eq!(
        state
            .accept_current_observation(fresh_observation(
                [14; 16],
                &target,
                "deployment-a",
                "other",
                2,
            ))
            .unwrap_err(),
        ManagementObservationError::IncarnationMismatch
    );
    assert_eq!(
        state.phase(),
        ManagementObservationPhase::ClosedIncarnationMismatch
    );
    assert_eq!(
        acquired_before_mismatch
            .mark_dispatched(EffectResponsibility::new(
                EffectIdentity::from_bytes([49; 16]),
                target.clone(),
                incarnation("new"),
                EffectScope::CATALOG_COMMIT,
                ManagementTimestamp::from_unix_millis(1_000),
            ))
            .unwrap_err(),
        ManagementAdmissionError::ReadmissionIncomplete
    );
    drop(acquired_before_mismatch);
    assert!(entrance.unsettled_effects(target.table()).is_empty());
    assert_eq!(
        entrance.acquire(request, || false).err().unwrap(),
        ManagementAdmissionError::ReadmissionIncomplete
    );
}

#[test]
fn current_marker_mismatch_closes_only_the_exact_local_management_target() {
    let mv_target = target("mv", b"object-a");
    let dependencies = ManagementDependencySet::new([1; 32], [2; 32], None, runtime_id(1));
    let entrance = ManagementEntrance::new(owner("deployment-a"), incarnation("inc-a"));
    let state = ready_observation_state(mv_target.clone(), "deployment-a", "inc-a");
    entrance
        .install_observed_target(&state, dependencies.clone())
        .unwrap();
    assert_eq!(
        entrance.management_phase(mv_target.table()),
        MvManagementPhase::Manageable
    );
    assert!(!observe_keyed_management_mismatch(
        &entrance,
        &mv_target,
        "deployment-a",
        "inc-a",
        1
    ));
    assert!(!observe_keyed_management_mismatch(
        &entrance,
        &target("mv", b"object-b"),
        "deployment-a",
        "inc-b",
        2
    ));
    let other_catalog = ManagedMvTarget::try_new(
        catalog(2),
        mv_target.table().clone(),
        mv_target.object_id().clone(),
    )
    .unwrap();
    assert!(!observe_keyed_management_mismatch(
        &entrance,
        &other_catalog,
        "deployment-a",
        "inc-b",
        2
    ));
    assert_eq!(
        entrance.management_phase(mv_target.table()),
        MvManagementPhase::Manageable
    );
    assert!(observe_keyed_management_mismatch(
        &entrance,
        &mv_target,
        "deployment-a",
        "inc-b",
        2
    ));
    assert_eq!(
        entrance.management_phase(mv_target.table()),
        MvManagementPhase::IncarnationMismatch
    );
    assert_eq!(
        entrance
            .install_observed_target(&state, dependencies.clone())
            .err()
            .unwrap(),
        ManagementAdmissionError::ReadmissionIncomplete
    );
    let request = ManagementRequest::try_new(
        mv_target.catalog().clone(),
        mv_target.table().clone(),
        Some(mv_target.object_id().clone()),
        ConnectorDocumentManagementOperation::SingleTargetUpdate,
        Some(dependencies),
        EffectScope::CATALOG_COMMIT,
    )
    .unwrap();
    assert_eq!(
        entrance.acquire(request, || false).err().unwrap(),
        ManagementAdmissionError::ReadmissionIncomplete
    );
}

#[test]
fn object_replacement_closes_readmission_instead_of_becoming_a_miss() {
    let original = target("mv", b"object-a");
    let replacement = target("mv", b"object-b");
    let mut state = ManagementObservationState::try_new(
        original,
        owner("deployment-a"),
        incarnation("new"),
        ManagementContinuation::SameOwner {
            previous_incarnation: incarnation("old"),
        },
        vec![],
        None,
        None,
    )
    .unwrap();
    let _pending = state
        .begin_current_observation(ManagementObservationRequestId::from_bytes([15; 16]))
        .unwrap();
    assert!(matches!(
        state.accept_current_observation(fresh_observation(
            [15; 16],
            &replacement,
            "deployment-a",
            "old",
            1,
        )),
        Err(ManagementObservationError::Ownership(
            ManagementOwnershipError::TargetReplaced
        ))
    ));
    assert_eq!(
        state.phase(),
        ManagementObservationPhase::ClosedObjectReplacement
    );
}

#[test]
fn entrance_rechecks_long_computation_dependencies_and_isolates_targets() {
    let target_a = target("mv_a", b"object-a");
    let target_b = target("mv_b", b"object-b");
    let observation_a = ready_observation_state(target_a.clone(), "deployment-a", "inc-a");
    let observation_b = ready_observation_state(target_b.clone(), "deployment-a", "inc-a");
    let dependencies = ManagementDependencySet::new([1; 32], [2; 32], Some([3; 32]), runtime_id(4));
    let entrance = ManagementEntrance::new(owner("deployment-a"), incarnation("inc-a"));
    entrance
        .install_observed_target(&observation_a, dependencies.clone())
        .unwrap();
    entrance
        .install_observed_target(&observation_b, dependencies.clone())
        .unwrap();

    let stale = ManagementRequest::try_new(
        target_a.catalog().clone(),
        target_a.table().clone(),
        Some(target_a.object_id().clone()),
        ConnectorDocumentManagementOperation::Publication,
        Some(ManagementDependencySet::new(
            [9; 32],
            [2; 32],
            Some([3; 32]),
            runtime_id(4),
        )),
        EffectScope::CATALOG_COMMIT,
    )
    .unwrap();
    assert_eq!(
        entrance.acquire(stale, || false).err().unwrap(),
        ManagementAdmissionError::DependencyChanged
    );

    let mut different_runtime_bytes = runtime_id(4).to_bytes();
    different_runtime_bytes[15] = 9;
    let stale_runtime = ManagementRequest::try_new(
        target_a.catalog().clone(),
        target_a.table().clone(),
        Some(target_a.object_id().clone()),
        ConnectorDocumentManagementOperation::Publication,
        Some(ManagementDependencySet::new(
            [1; 32],
            [2; 32],
            Some([3; 32]),
            ConnectorControlRuntimeId::from_bytes(different_runtime_bytes),
        )),
        EffectScope::CATALOG_COMMIT,
    )
    .unwrap();
    assert_eq!(
        entrance.acquire(stale_runtime, || false).err().unwrap(),
        ManagementAdmissionError::DependencyChanged
    );

    let lease_a = entrance
        .acquire(
            ManagementRequest::try_new(
                target_a.catalog().clone(),
                target_a.table().clone(),
                Some(target_a.object_id().clone()),
                ConnectorDocumentManagementOperation::Publication,
                Some(dependencies.clone()),
                EffectScope::CATALOG_COMMIT,
            )
            .unwrap(),
            || false,
        )
        .unwrap();
    let lease_b = entrance
        .acquire(
            ManagementRequest::try_new(
                target_b.catalog().clone(),
                target_b.table().clone(),
                Some(target_b.object_id().clone()),
                ConnectorDocumentManagementOperation::SingleTargetUpdate,
                Some(dependencies),
                EffectScope::CATALOG_COMMIT,
            )
            .unwrap(),
            || false,
        )
        .unwrap();
    drop((lease_a, lease_b));
}

fn publication_terminal_fixture(
    operation: ConnectorDocumentManagementOperation,
) -> (ManagementEntrance, ManagedMvTarget, ManagementRequest) {
    let target = target("mv", b"object-a");
    let observation = ready_observation_state(target.clone(), "deployment-a", "inc-a");
    let dependencies = ManagementDependencySet::new([1; 32], [2; 32], None, runtime_id(4));
    let entrance = ManagementEntrance::new(owner("deployment-a"), incarnation("inc-a"));
    entrance
        .install_observed_target(&observation, dependencies.clone())
        .unwrap();
    let request = ManagementRequest::try_new(
        target.catalog().clone(),
        target.table().clone(),
        Some(target.object_id().clone()),
        operation,
        Some(dependencies),
        EffectScope::CATALOG_COMMIT,
    )
    .unwrap();
    (entrance, target, request)
}

#[test]
fn publication_refused_before_dispatch_releases_only_its_local_activity() {
    let (entrance, target, request) =
        publication_terminal_fixture(ConnectorDocumentManagementOperation::Publication);
    let mut first = entrance
        .request(
            request.clone(),
            crate::activity::MvActivityOwner::ScheduledRefresh,
        )
        .unwrap();
    let first = first.try_acquire().unwrap().unwrap();
    let mut next = entrance
        .request(request, crate::activity::MvActivityOwner::ManualRefresh)
        .unwrap();
    assert!(next.try_acquire().unwrap().is_none());
    assert_eq!(
        entrance.management_phase(target.table()),
        MvManagementPhase::Managing
    );
    first
        .record_publication_terminal(EffectDisposition::KnownUncommitted)
        .unwrap();
    assert!(entrance.unsettled_effects(target.table()).is_empty());
    assert_eq!(
        entrance.management_phase(target.table()),
        MvManagementPhase::Manageable
    );
    let next = next
        .try_acquire()
        .unwrap()
        .expect("local refusal releases the FIFO turn");
    drop(next);
    assert_eq!(
        entrance.management_phase(target.table()),
        MvManagementPhase::Manageable
    );
}

#[test]
fn local_publication_cannot_claim_an_unissued_commit_or_unknown_effect() {
    for disposition in [
        EffectDisposition::KnownCommitted,
        EffectDisposition::CommitUnknown,
    ] {
        let (entrance, target, request) =
            publication_terminal_fixture(ConnectorDocumentManagementOperation::Publication);
        let lease = entrance.acquire(request.clone(), || false).unwrap();
        assert_eq!(
            lease.record_publication_terminal(disposition),
            Err(ManagementAdmissionError::EffectNotDispatched)
        );
        assert!(entrance.unsettled_effects(target.table()).is_empty());
        assert_eq!(
            entrance.management_phase(target.table()),
            MvManagementPhase::Manageable
        );
        // The original general effect API remains strict even for Publication.
        assert_eq!(
            entrance
                .acquire(request, || false)
                .unwrap()
                .record_terminal(EffectDisposition::KnownUncommitted),
            Err(ManagementAdmissionError::EffectNotDispatched)
        );
    }
    let (entrance, target, request) =
        publication_terminal_fixture(ConnectorDocumentManagementOperation::SingleTargetUpdate);
    assert_eq!(
        entrance
            .acquire(request, || false)
            .unwrap()
            .record_publication_terminal(EffectDisposition::KnownUncommitted),
        Err(ManagementAdmissionError::InvalidEffect)
    );
    assert_eq!(
        entrance.management_phase(target.table()),
        MvManagementPhase::Manageable
    );
}

#[test]
fn dispatched_publication_preserves_the_providers_actual_terminal() {
    for (disposition, expected_phase) in [
        (
            EffectDisposition::KnownUncommitted,
            MvManagementPhase::Manageable,
        ),
        (
            EffectDisposition::KnownCommitted,
            MvManagementPhase::AwaitingConvergence,
        ),
        (
            EffectDisposition::CommitUnknown,
            MvManagementPhase::AwaitingEffectSettlement { unsettled: 1 },
        ),
    ] {
        let (entrance, target, request) =
            publication_terminal_fixture(ConnectorDocumentManagementOperation::Publication);
        let mut lease = entrance.acquire(request.clone(), || false).unwrap();
        let identity = EffectIdentity::from_bytes([19; 16]);
        lease
            .mark_dispatched(EffectResponsibility::new(
                identity,
                target.clone(),
                incarnation("inc-a"),
                EffectScope::CATALOG_COMMIT,
                ManagementTimestamp::from_unix_millis(1_000),
            ))
            .unwrap();
        lease.record_publication_terminal(disposition).unwrap();
        assert_eq!(entrance.management_phase(target.table()), expected_phase);
        let unsettled = entrance.unsettled_effects(target.table());
        if disposition == EffectDisposition::CommitUnknown {
            assert_eq!(unsettled.len(), 1);
            assert_eq!(unsettled[0].responsibility().identity(), identity);
            assert_eq!(
                unsettled[0].original_disposition(),
                EffectDisposition::CommitUnknown
            );
            assert_eq!(
                entrance.acquire(request, || false).err().unwrap(),
                ManagementAdmissionError::EffectUnsettled
            );
        } else {
            assert!(unsettled.is_empty());
        }
    }
}

#[test]
fn unknown_or_dropped_after_dispatch_keeps_conflicting_management_closed() {
    let target = target("mv", b"object-a");
    let observation = ready_observation_state(target.clone(), "deployment-a", "inc-a");
    let dependencies = ManagementDependencySet::new([1; 32], [2; 32], Some([3; 32]), runtime_id(4));
    let entrance = ManagementEntrance::new(owner("deployment-a"), incarnation("inc-a"));
    entrance
        .install_observed_target(&observation, dependencies.clone())
        .unwrap();
    let request = || {
        ManagementRequest::try_new(
            target.catalog().clone(),
            target.table().clone(),
            Some(target.object_id().clone()),
            ConnectorDocumentManagementOperation::Publication,
            Some(dependencies.clone()),
            EffectScope::CATALOG_COMMIT,
        )
        .unwrap()
    };
    let mut lease = entrance.acquire(request(), || false).unwrap();
    lease
        .mark_dispatched(EffectResponsibility::new(
            EffectIdentity::from_bytes([20; 16]),
            target.clone(),
            incarnation("inc-a"),
            EffectScope::CATALOG_COMMIT,
            ManagementTimestamp::from_unix_millis(1_000),
        ))
        .unwrap();
    drop(lease);

    let unsettled = entrance.unsettled_effects(target.table());
    assert_eq!(unsettled.len(), 1);
    assert_eq!(
        entrance.acquire(request(), || false).err().unwrap(),
        ManagementAdmissionError::EffectUnsettled
    );

    let effect = &unsettled[0];
    let completion = ActualCompletionEvidence::try_new(
        effect.responsibility().identity(),
        target.clone(),
        EffectScope::CATALOG_COMMIT,
        ManagementTimestamp::from_unix_millis(1_200),
        "provider audit terminal receipt",
    )
    .unwrap();
    let permit = ReadmissionEvaluator::default()
        .from_actual_completion(effect, &isolation(&target, "inc-a", 1_100), &completion)
        .unwrap();
    let mut readmission = entrance
        .begin_readmission(
            target.table(),
            ManagementContinuation::SameOwner {
                previous_incarnation: incarnation("inc-a"),
            },
        )
        .unwrap();
    readmission.accept_readmission_permit(permit).unwrap();
    let _pending = readmission
        .begin_current_observation(ManagementObservationRequestId::from_bytes([21; 16]))
        .unwrap();
    readmission
        .accept_current_observation(fresh_observation(
            [21; 16],
            &target,
            "deployment-a",
            "inc-a",
            5,
        ))
        .unwrap();
    entrance
        .install_observed_target(&readmission, dependencies.clone())
        .unwrap();
    assert!(entrance.acquire(request(), || false).is_ok());
    assert_eq!(
        effect.original_disposition(),
        EffectDisposition::CommitUnknown
    );
}

#[test]
fn fabricated_empty_readmission_cannot_clear_an_unknown_effect() {
    let target = target("mv", b"object-a");
    let observation = ready_observation_state(target.clone(), "deployment-a", "inc-a");
    let dependencies = ManagementDependencySet::new([1; 32], [2; 32], None, runtime_id(4));
    let entrance = ManagementEntrance::new(owner("deployment-a"), incarnation("inc-a"));
    entrance
        .install_observed_target(&observation, dependencies.clone())
        .unwrap();
    let request = ManagementRequest::try_new(
        target.catalog().clone(),
        target.table().clone(),
        Some(target.object_id().clone()),
        ConnectorDocumentManagementOperation::Publication,
        Some(dependencies.clone()),
        EffectScope::CATALOG_COMMIT,
    )
    .unwrap();
    let mut lease = entrance.acquire(request, || false).unwrap();
    lease
        .mark_dispatched(EffectResponsibility::new(
            EffectIdentity::from_bytes([22; 16]),
            target.clone(),
            incarnation("inc-a"),
            EffectScope::CATALOG_COMMIT,
            ManagementTimestamp::from_unix_millis(1_000),
        ))
        .unwrap();
    drop(lease);

    let fabricated = ready_observation_state(target, "deployment-a", "inc-a");
    assert_eq!(
        entrance
            .install_observed_target(&fabricated, dependencies)
            .unwrap_err(),
        ManagementAdmissionError::ReadmissionIncomplete
    );
}

#[test]
fn dispatched_effect_requires_exact_scope_and_the_current_incarnation() {
    let target = target("mv", b"object-a");
    let observation = ready_observation_state(target.clone(), "deployment-a", "inc-a");
    let dependencies = ManagementDependencySet::new([1; 32], [2; 32], None, runtime_id(4));
    let entrance = ManagementEntrance::new(owner("deployment-a"), incarnation("inc-a"));
    entrance
        .install_observed_target(&observation, dependencies.clone())
        .unwrap();
    let request = ManagementRequest::try_new(
        target.catalog().clone(),
        target.table().clone(),
        Some(target.object_id().clone()),
        ConnectorDocumentManagementOperation::Publication,
        Some(dependencies),
        EffectScope::CATALOG_AND_OBJECT_DELETION,
    )
    .unwrap();
    let mut lease = entrance.acquire(request, || false).unwrap();
    assert_eq!(
        lease
            .mark_dispatched(EffectResponsibility::new(
                EffectIdentity::from_bytes([42; 16]),
                target.clone(),
                incarnation("foreign-incarnation"),
                EffectScope::CATALOG_AND_OBJECT_DELETION,
                ManagementTimestamp::from_unix_millis(1_000),
            ))
            .unwrap_err(),
        ManagementAdmissionError::InvalidEffect
    );
    assert_eq!(
        lease
            .mark_dispatched(EffectResponsibility::new(
                EffectIdentity::from_bytes([43; 16]),
                target.clone(),
                incarnation("inc-a"),
                EffectScope::CATALOG_COMMIT,
                ManagementTimestamp::from_unix_millis(1_000),
            ))
            .unwrap_err(),
        ManagementAdmissionError::InvalidEffect
    );
    lease
        .mark_dispatched(EffectResponsibility::new(
            EffectIdentity::from_bytes([44; 16]),
            target,
            incarnation("inc-a"),
            EffectScope::CATALOG_AND_OBJECT_DELETION,
            ManagementTimestamp::from_unix_millis(1_000),
        ))
        .unwrap();
    lease
        .record_terminal(EffectDisposition::KnownUncommitted)
        .unwrap();
}

#[test]
fn committed_update_stays_closed_until_exact_observation_installs_new_dependencies() {
    let target = target("mv", b"object-a");
    let observation = ready_observation_state(target.clone(), "deployment-a", "inc-a");
    let old_dependencies = ManagementDependencySet::new([1; 32], [2; 32], None, runtime_id(4));
    let new_dependencies = ManagementDependencySet::new([5; 32], [6; 32], None, runtime_id(7));
    let entrance = ManagementEntrance::new(owner("deployment-a"), incarnation("inc-a"));
    entrance
        .install_observed_target(&observation, old_dependencies.clone())
        .unwrap();
    let request = |dependencies| {
        ManagementRequest::try_new(
            target.catalog().clone(),
            target.table().clone(),
            Some(target.object_id().clone()),
            ConnectorDocumentManagementOperation::SingleTargetUpdate,
            Some(dependencies),
            EffectScope::CATALOG_COMMIT,
        )
        .unwrap()
    };
    let mut lease = entrance
        .acquire(request(old_dependencies.clone()), || false)
        .unwrap();
    lease
        .mark_dispatched(EffectResponsibility::new(
            EffectIdentity::from_bytes([45; 16]),
            target.clone(),
            incarnation("inc-a"),
            EffectScope::CATALOG_COMMIT,
            ManagementTimestamp::from_unix_millis(1_000),
        ))
        .unwrap();
    lease
        .record_terminal(EffectDisposition::KnownCommitted)
        .unwrap();
    assert_eq!(
        entrance
            .acquire(request(old_dependencies.clone()), || false)
            .err()
            .unwrap(),
        ManagementAdmissionError::ReadmissionIncomplete
    );

    let fabricated = ready_observation_state(target.clone(), "deployment-a", "inc-a");
    assert_eq!(
        entrance
            .install_observed_target(&fabricated, new_dependencies.clone())
            .unwrap_err(),
        ManagementAdmissionError::ReadmissionIncomplete
    );
    let mut convergence = entrance
        .begin_committed_convergence(
            target.table(),
            ManagementContinuation::SameOwner {
                previous_incarnation: incarnation("inc-a"),
            },
        )
        .unwrap();
    assert_eq!(
        entrance
            .begin_committed_convergence(
                target.table(),
                ManagementContinuation::SameOwner {
                    previous_incarnation: incarnation("inc-a"),
                },
            )
            .err()
            .unwrap(),
        ManagementObservationError::ObservationAlreadyPending
    );
    convergence
        .begin_current_observation(ManagementObservationRequestId::from_bytes([46; 16]))
        .unwrap();
    convergence
        .accept_current_observation(fresh_observation(
            [46; 16],
            &target,
            "deployment-a",
            "inc-a",
            2,
        ))
        .unwrap();
    entrance
        .install_observed_target(&convergence, new_dependencies.clone())
        .unwrap();
    assert_eq!(
        entrance
            .install_observed_target(&convergence, new_dependencies.clone())
            .unwrap_err(),
        ManagementAdmissionError::ReadmissionIncomplete
    );
    assert_eq!(
        entrance
            .acquire(request(old_dependencies), || false)
            .err()
            .unwrap(),
        ManagementAdmissionError::DependencyChanged
    );
    assert!(
        entrance
            .acquire(request(new_dependencies), || false)
            .is_ok()
    );
}

#[test]
fn abandoned_committed_convergence_preserves_exact_reissue_requirement() {
    let target = target("mv", b"object-a");
    let dependencies = ManagementDependencySet::new([1; 32], [2; 32], None, runtime_id(4));
    let observation = ready_observation_state(target.clone(), "deployment-a", "inc-a");
    let entrance = ManagementEntrance::new(owner("deployment-a"), incarnation("inc-a"));
    entrance
        .install_observed_target(&observation, dependencies.clone())
        .unwrap();
    let request = || {
        ManagementRequest::try_new(
            target.catalog().clone(),
            target.table().clone(),
            Some(target.object_id().clone()),
            ConnectorDocumentManagementOperation::SingleTargetUpdate,
            Some(dependencies.clone()),
            EffectScope::CATALOG_COMMIT,
        )
        .unwrap()
    };
    let mut lease = entrance.acquire(request(), || false).unwrap();
    lease
        .mark_dispatched(EffectResponsibility::new(
            EffectIdentity::from_bytes([50; 16]),
            target.clone(),
            incarnation("inc-a"),
            EffectScope::CATALOG_COMMIT,
            ManagementTimestamp::from_unix_millis(1_000),
        ))
        .unwrap();
    lease
        .record_terminal(EffectDisposition::KnownCommitted)
        .unwrap();

    let convergence = entrance
        .begin_committed_convergence(
            target.table(),
            ManagementContinuation::SameOwner {
                previous_incarnation: incarnation("inc-a"),
            },
        )
        .unwrap();
    entrance.abandon_observation(convergence).unwrap();
    match entrance.begin_committed_convergence(
        target.table(),
        ManagementContinuation::SameOwner {
            previous_incarnation: incarnation("different-incarnation"),
        },
    ) {
        Err(error) => assert_eq!(error, ManagementObservationError::InvalidRecoveryBarrier),
        Ok(_) => panic!("a different continuation must not replace committed convergence"),
    }
    match entrance.acquire(request(), || false) {
        Err(error) => assert_eq!(error, ManagementAdmissionError::ReadmissionIncomplete),
        Ok(_) => panic!("abandoning convergence must retain the committed effect barrier"),
    }

    let mut replacement = entrance
        .begin_committed_convergence(
            target.table(),
            ManagementContinuation::SameOwner {
                previous_incarnation: incarnation("inc-a"),
            },
        )
        .unwrap();
    replacement
        .begin_current_observation(ManagementObservationRequestId::from_bytes([51; 16]))
        .unwrap();
    replacement
        .accept_current_observation(fresh_observation(
            [51; 16],
            &target,
            "deployment-a",
            "inc-a",
            3,
        ))
        .unwrap();
    entrance
        .install_observed_target(&replacement, dependencies.clone())
        .unwrap();
    assert!(entrance.acquire(request(), || false).is_ok());
}

#[test]
fn committed_create_reserves_the_target_until_observation_converges() {
    let target = target("created_mv", b"created-object");
    let dependencies = ManagementDependencySet::new([1; 32], [2; 32], None, runtime_id(4));
    let entrance = ManagementEntrance::new(owner("deployment-a"), incarnation("inc-a"));
    let create = |identity| {
        ManagementRequest::for_create_intent(
            CreateIntent::try_new(target.catalog().clone(), target.table().clone(), identity)
                .unwrap(),
            EffectScope::CATALOG_COMMIT,
        )
    };
    let mut lease = entrance
        .acquire(create(EffectIdentity::from_bytes([47; 16])), || false)
        .unwrap();
    lease
        .mark_create_intent_dispatched(ManagementTimestamp::from_unix_millis(1_000))
        .unwrap();
    lease.late_bind_create_target(target.clone()).unwrap();
    lease
        .record_terminal(EffectDisposition::KnownCommitted)
        .unwrap();
    assert_eq!(
        entrance
            .acquire(create(EffectIdentity::from_bytes([48; 16])), || false)
            .err()
            .unwrap(),
        ManagementAdmissionError::TargetAlreadyExists
    );

    let mut convergence = entrance
        .begin_committed_convergence(
            target.table(),
            ManagementContinuation::SameOwner {
                previous_incarnation: incarnation("inc-a"),
            },
        )
        .unwrap();
    convergence
        .begin_current_observation(ManagementObservationRequestId::from_bytes([48; 16]))
        .unwrap();
    convergence
        .accept_current_observation(fresh_observation(
            [48; 16],
            &target,
            "deployment-a",
            "inc-a",
            1,
        ))
        .unwrap();
    entrance
        .install_observed_target(&convergence, dependencies.clone())
        .unwrap();
    assert!(
        entrance
            .acquire(
                ManagementRequest::try_new(
                    target.catalog().clone(),
                    target.table().clone(),
                    Some(target.object_id().clone()),
                    ConnectorDocumentManagementOperation::SingleTargetUpdate,
                    Some(dependencies),
                    EffectScope::CATALOG_COMMIT,
                )
                .unwrap(),
                || false,
            )
            .is_ok()
    );
}

#[test]
fn create_known_uncommitted_can_retry_but_create_unknown_cannot() {
    let exact_target = target("new_mv", b"created-object");
    let entrance = ManagementEntrance::new(owner("deployment-a"), incarnation("inc-a"));
    let create = |identity| {
        ManagementRequest::for_create_intent(
            CreateIntent::try_new(
                exact_target.catalog().clone(),
                exact_target.table().clone(),
                identity,
            )
            .unwrap(),
            EffectScope::CATALOG_COMMIT,
        )
    };
    let mut first = entrance
        .acquire(create(EffectIdentity::from_bytes([23; 16])), || false)
        .unwrap();
    first
        .mark_create_intent_dispatched(ManagementTimestamp::from_unix_millis(1_000))
        .unwrap();
    first.late_bind_create_target(exact_target.clone()).unwrap();
    first
        .record_terminal(EffectDisposition::KnownUncommitted)
        .unwrap();
    let mut second = entrance
        .acquire(create(EffectIdentity::from_bytes([24; 16])), || false)
        .unwrap();
    second
        .mark_create_intent_dispatched(ManagementTimestamp::from_unix_millis(2_000))
        .unwrap();
    second
        .late_bind_create_target(exact_target.clone())
        .unwrap();
    second
        .record_terminal(EffectDisposition::CommitUnknown)
        .unwrap();
    assert_eq!(
        entrance
            .acquire(create(EffectIdentity::from_bytes([25; 16])), || false)
            .err()
            .unwrap(),
        ManagementAdmissionError::EffectUnsettled
    );
    assert_eq!(
        entrance
            .begin_readmission(
                exact_target.table(),
                ManagementContinuation::SameOwner {
                    previous_incarnation: incarnation("inc-a"),
                },
            )
            .unwrap()
            .phase(),
        ManagementObservationPhase::AwaitingEffectClosure
    );
}

#[test]
fn create_intent_marks_before_stage_binds_once_and_blocks_a_lost_response() {
    let logical = table("created_mv");
    let exact = target("created_mv", b"created-object");
    let mismatch = target("other_mv", b"created-object");
    let entrance = ManagementEntrance::new(owner("deployment-a"), incarnation("inc-a"));
    let lost_intent = CreateIntent::try_new(
        catalog(1),
        logical.clone(),
        EffectIdentity::from_bytes([71; 16]),
    )
    .unwrap();
    let request = |identity| {
        ManagementRequest::for_create_intent(
            CreateIntent::try_new(catalog(1), logical.clone(), identity).unwrap(),
            EffectScope::CATALOG_COMMIT,
        )
    };

    let mut lease = entrance
        .acquire(
            ManagementRequest::for_create_intent(lost_intent.clone(), EffectScope::CATALOG_COMMIT),
            || false,
        )
        .unwrap();
    assert_eq!(
        lease
            .mark_dispatched(EffectResponsibility::new(
                EffectIdentity::from_bytes([71; 16]),
                exact.clone(),
                incarnation("inc-a"),
                EffectScope::CATALOG_COMMIT,
                ManagementTimestamp::from_unix_millis(1_000),
            ))
            .unwrap_err(),
        ManagementAdmissionError::InvalidEffect
    );
    lease
        .mark_create_intent_dispatched(ManagementTimestamp::from_unix_millis(1_000))
        .unwrap();
    assert_eq!(
        lease.late_bind_create_target(mismatch).unwrap_err(),
        ManagementAdmissionError::TargetReplaced
    );
    drop(lease);

    // The staged response was lost before an exact target could be bound. The
    // old intent remains an Unknown barrier rather than being treated as an
    // absent table or retried with another caller-generated UUID.
    assert_eq!(
        entrance
            .acquire(request(EffectIdentity::from_bytes([72; 16])), || false)
            .err()
            .unwrap(),
        ManagementAdmissionError::EffectUnsettled
    );

    let mut escaped = keyed_management_view(&exact, "deployment-a", "inc-a", 1);
    escaped.owner.incarnation = ProviderBindingEpoch::from_bytes([9; 16]);
    let (bad_lease, bad_request, _) = keyed_management_lease(&exact, Ok(escaped));
    assert_eq!(
        entrance
            .begin_unbound_create_observation(&lost_intent)
            .unwrap()
            .complete_keyed(&bad_lease, bad_request)
            .err()
            .unwrap(),
        ManagementObservationError::UnsealedObservation
    );
    let (lease, request, calls) = keyed_management_lease(
        &exact,
        Ok(keyed_management_view(&exact, "deployment-a", "inc-a", 1)),
    );
    let (fresh, _) = entrance
        .begin_unbound_create_observation(&lost_intent)
        .unwrap()
        .complete_keyed(&lease, request)
        .unwrap();
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert_eq!(fresh.target(), &exact);
    assert_eq!(
        entrance.resolve_unbound_create_as_unknown(fresh).unwrap(),
        exact
    );
    assert_eq!(entrance.unsettled_effects(exact.table()).len(), 1);
    assert_eq!(
        entrance
            .begin_readmission(
                exact.table(),
                ManagementContinuation::SameOwner {
                    previous_incarnation: incarnation("inc-a"),
                },
            )
            .unwrap()
            .phase(),
        ManagementObservationPhase::AwaitingEffectClosure
    );
}

#[test]
fn automatic_actions_are_single_frozen_effects_and_require_convergence() {
    let target = target("mv", b"object-a");
    let dependencies = ManagementDependencySet::new([1; 32], [2; 32], None, runtime_id(4));
    let entrance = ManagementEntrance::new(owner("deployment-a"), incarnation("inc-a"));
    let observation = ready_observation_state(target.clone(), "deployment-a", "inc-a");
    entrance
        .install_observed_target(&observation, dependencies.clone())
        .unwrap();
    let action = |identity| {
        AutomaticMaintenanceEffect::new(
            target.clone(),
            dependencies.clone(),
            identity,
            EffectScope::CATALOG_COMMIT,
        )
    };

    let mut first = entrance
        .acquire_automatic_maintenance(action(EffectIdentity::from_bytes([81; 16])), || false)
        .unwrap();
    assert_eq!(
        first
            .mark_dispatched(EffectResponsibility::new(
                EffectIdentity::from_bytes([82; 16]),
                target.clone(),
                incarnation("inc-a"),
                EffectScope::CATALOG_COMMIT,
                ManagementTimestamp::from_unix_millis(1_000),
            ))
            .unwrap_err(),
        ManagementAdmissionError::InvalidEffect
    );
    first
        .mark_dispatched(EffectResponsibility::new(
            EffectIdentity::from_bytes([81; 16]),
            target.clone(),
            incarnation("inc-a"),
            EffectScope::CATALOG_COMMIT,
            ManagementTimestamp::from_unix_millis(1_000),
        ))
        .unwrap();
    first
        .record_terminal(EffectDisposition::KnownCommitted)
        .unwrap();

    // A next action needs a fresh installation; it cannot share the completed
    // action's lease or stale management observation.
    assert_eq!(
        entrance
            .acquire_automatic_maintenance(action(EffectIdentity::from_bytes([83; 16])), || false)
            .err()
            .unwrap(),
        ManagementAdmissionError::ReadmissionIncomplete
    );
    let mut convergence = entrance
        .begin_committed_convergence(
            target.table(),
            ManagementContinuation::SameOwner {
                previous_incarnation: incarnation("inc-a"),
            },
        )
        .unwrap();
    convergence
        .begin_current_observation(ManagementObservationRequestId::from_bytes([84; 16]))
        .unwrap();
    convergence
        .accept_current_observation(fresh_observation(
            [84; 16],
            &target,
            "deployment-a",
            "inc-a",
            2,
        ))
        .unwrap();
    entrance
        .install_observed_target(&convergence, dependencies.clone())
        .unwrap();
    assert!(
        entrance
            .acquire_automatic_maintenance(action(EffectIdentity::from_bytes([83; 16])), || false)
            .is_ok()
    );
}

#[test]
fn same_target_wait_honors_work_scope_cancellation() {
    let target = target("mv", b"object-a");
    let observation = ready_observation_state(target.clone(), "deployment-a", "inc-a");
    let dependencies = ManagementDependencySet::new([1; 32], [2; 32], None, runtime_id(4));
    let entrance = ManagementEntrance::new(owner("deployment-a"), incarnation("inc-a"));
    entrance
        .install_observed_target(&observation, dependencies.clone())
        .unwrap();
    let request = || {
        ManagementRequest::try_new(
            target.catalog().clone(),
            target.table().clone(),
            Some(target.object_id().clone()),
            ConnectorDocumentManagementOperation::SingleTargetUpdate,
            Some(dependencies.clone()),
            EffectScope::CATALOG_COMMIT,
        )
        .unwrap()
    };
    let first = entrance.acquire(request(), || false).unwrap();
    assert_eq!(
        entrance.acquire(request(), || true).err().unwrap(),
        ManagementAdmissionError::Cancelled
    );
    drop(first);
}

#[test]
fn management_ticket_preserves_worker_owner_and_nonblocking_fifo() {
    let target = target("mv", b"object-a");
    let observation = ready_observation_state(target.clone(), "deployment-a", "inc-a");
    let dependencies = ManagementDependencySet::new([1; 32], [2; 32], None, runtime_id(4));
    let entrance = ManagementEntrance::new(owner("deployment-a"), incarnation("inc-a"));
    entrance
        .install_observed_target(&observation, dependencies.clone())
        .unwrap();
    let request = || {
        ManagementRequest::try_new(
            target.catalog().clone(),
            target.table().clone(),
            Some(target.object_id().clone()),
            ConnectorDocumentManagementOperation::Publication,
            Some(dependencies.clone()),
            EffectScope::CATALOG_COMMIT,
        )
        .unwrap()
    };

    let mut scheduled = entrance
        .request(
            request(),
            crate::activity::MvActivityOwner::ScheduledRefresh,
        )
        .unwrap();
    let scheduled = scheduled
        .try_acquire()
        .unwrap()
        .expect("first worker ticket acquires without blocking");
    assert!(scheduled.worker_cancellation().is_some());

    let mut manual = entrance
        .request(request(), crate::activity::MvActivityOwner::ManualRefresh)
        .unwrap();
    assert!(manual.try_acquire().unwrap().is_none());
    drop(scheduled);

    let manual = manual
        .try_acquire()
        .unwrap()
        .expect("next ticket acquires after the worker releases the target");
    assert!(manual.worker_cancellation().is_none());
}

#[test]
fn stopping_cancels_worker_acquired_through_management_entrance() {
    let target = target("mv", b"object-a");
    let observation = ready_observation_state(target.clone(), "deployment-a", "inc-a");
    let dependencies = ManagementDependencySet::new([1; 32], [2; 32], None, runtime_id(4));
    let entrance = ManagementEntrance::new(owner("deployment-a"), incarnation("inc-a"));
    entrance
        .install_observed_target(&observation, dependencies.clone())
        .unwrap();
    let request = ManagementRequest::try_new(
        target.catalog().clone(),
        target.table().clone(),
        Some(target.object_id().clone()),
        ConnectorDocumentManagementOperation::SingleTargetUpdate,
        Some(dependencies),
        EffectScope::CATALOG_COMMIT,
    )
    .unwrap();
    let mut ticket = entrance
        .request(
            request,
            crate::activity::MvActivityOwner::AutomaticMaintenance,
        )
        .unwrap();
    let lease = ticket.try_acquire().unwrap().expect("worker lease");
    let cancellation = lease.worker_cancellation().expect("worker cancellation");

    entrance.begin_stopping();

    assert!(cancellation.is_cancelled());
}

fn installed_entrance(
    target: &ManagedMvTarget,
    dependencies: &ManagementDependencySet,
) -> ManagementEntrance {
    let observation = ready_observation_state(target.clone(), "deployment-a", "inc-a");
    let entrance = ManagementEntrance::new(owner("deployment-a"), incarnation("inc-a"));
    entrance
        .install_observed_target(&observation, dependencies.clone())
        .unwrap();
    entrance
}

fn drop_request(
    target: &ManagedMvTarget,
    dependencies: &ManagementDependencySet,
) -> ManagementRequest {
    ManagementRequest::try_new(
        target.catalog().clone(),
        target.table().clone(),
        Some(target.object_id().clone()),
        ConnectorDocumentManagementOperation::Drop,
        Some(dependencies.clone()),
        EffectScope::CATALOG_AND_OBJECT_DELETION,
    )
    .unwrap()
}

fn mark_drop_dispatched(lease: &mut ManagementEntranceLease, target: &ManagedMvTarget) {
    lease
        .mark_dispatched(EffectResponsibility::new(
            EffectIdentity::from_bytes([61; 16]),
            target.clone(),
            incarnation("inc-a"),
            EffectScope::CATALOG_AND_OBJECT_DELETION,
            ManagementTimestamp::from_unix_millis(1_000),
        ))
        .unwrap();
}

#[test]
fn committed_drop_retires_only_exact_target_and_allows_same_name_create() {
    let old = target("mv", b"object-a");
    let replacement = target("mv", b"object-b");
    let dependencies = ManagementDependencySet::new([1; 32], [2; 32], None, runtime_id(4));
    let observation = ready_observation_state(old.clone(), "deployment-a", "inc-a");
    let entrance = ManagementEntrance::new(owner("deployment-a"), incarnation("inc-a"));
    let admission = entrance
        .install_observed_target(&observation, dependencies.clone())
        .unwrap();
    assert_eq!(
        entrance
            .acquire(drop_request(&replacement, &dependencies), || false)
            .err(),
        Some(ManagementAdmissionError::TargetReplaced)
    );

    let mut lease = entrance
        .acquire(drop_request(&old, &dependencies), || false)
        .unwrap();
    assert_eq!(
        entrance.management_phase(old.table()),
        MvManagementPhase::Managing
    );
    mark_drop_dispatched(&mut lease, &old);
    lease
        .record_drop_terminal(EffectDisposition::KnownCommitted)
        .unwrap();
    assert_eq!(
        entrance.management_phase(old.table()),
        MvManagementPhase::NotObserved
    );
    assert!(!admission.is_open());
    let intent = CreateIntent::try_new(
        replacement.catalog().clone(),
        replacement.table().clone(),
        EffectIdentity::from_bytes([62; 16]),
    )
    .unwrap();
    let create = entrance
        .acquire(
            ManagementRequest::for_create_intent(intent, EffectScope::CATALOG_COMMIT),
            || false,
        )
        .unwrap();
    drop(create);
}

#[test]
fn unknown_drop_blocks_same_name_create_and_retains_exact_effect() {
    let old = target("mv", b"object-a");
    let dependencies = ManagementDependencySet::new([1; 32], [2; 32], None, runtime_id(4));
    let entrance = installed_entrance(&old, &dependencies);
    let mut lease = entrance
        .acquire(drop_request(&old, &dependencies), || false)
        .unwrap();
    mark_drop_dispatched(&mut lease, &old);
    lease
        .record_drop_terminal(EffectDisposition::CommitUnknown)
        .unwrap();
    assert_eq!(
        entrance.management_phase(old.table()),
        MvManagementPhase::AwaitingEffectSettlement { unsettled: 1 }
    );
    let effect = entrance.unsettled_effects(old.table()).pop().unwrap();
    assert_eq!(effect.responsibility().target(), &old);
    assert_eq!(
        effect.responsibility().identity(),
        EffectIdentity::from_bytes([61; 16])
    );
    let intent = CreateIntent::try_new(
        old.catalog().clone(),
        old.table().clone(),
        EffectIdentity::from_bytes([62; 16]),
    )
    .unwrap();
    assert_eq!(
        entrance
            .acquire(
                ManagementRequest::for_create_intent(intent, EffectScope::CATALOG_COMMIT),
                || false,
            )
            .err(),
        Some(ManagementAdmissionError::EffectUnsettled)
    );
}

#[test]
fn uncommitted_drop_preserves_manageable_target() {
    let old = target("mv", b"object-a");
    let dependencies = ManagementDependencySet::new([1; 32], [2; 32], None, runtime_id(4));
    let entrance = installed_entrance(&old, &dependencies);
    let mut lease = entrance
        .acquire(drop_request(&old, &dependencies), || false)
        .unwrap();
    mark_drop_dispatched(&mut lease, &old);
    lease
        .record_drop_terminal(EffectDisposition::KnownUncommitted)
        .unwrap();
    assert_eq!(
        entrance.management_phase(old.table()),
        MvManagementPhase::Manageable
    );
    assert!(
        entrance
            .acquire(drop_request(&old, &dependencies), || false)
            .is_ok()
    );
}

#[test]
fn rejected_drop_dispatch_does_not_retire_or_unsettle_target() {
    let old = target("mv", b"object-a");
    let dependencies = ManagementDependencySet::new([1; 32], [2; 32], None, runtime_id(4));
    let entrance = installed_entrance(&old, &dependencies);
    let mut lease = entrance
        .acquire(drop_request(&old, &dependencies), || false)
        .unwrap();
    assert_eq!(
        lease.mark_dispatched(EffectResponsibility::new(
            EffectIdentity::from_bytes([61; 16]),
            old.clone(),
            incarnation("inc-a"),
            EffectScope::OBJECT_DELETION,
            ManagementTimestamp::from_unix_millis(1_000),
        )),
        Err(ManagementAdmissionError::InvalidEffect)
    );
    drop(lease);
    assert_eq!(
        entrance.management_phase(old.table()),
        MvManagementPhase::Manageable
    );
    assert!(entrance.unsettled_effects(old.table()).is_empty());
}

#[test]
fn dispatched_drop_without_terminal_result_keeps_new_create_closed() {
    let old = target("mv", b"object-a");
    let dependencies = ManagementDependencySet::new([1; 32], [2; 32], None, runtime_id(4));
    let entrance = installed_entrance(&old, &dependencies);
    let mut lease = entrance
        .acquire(drop_request(&old, &dependencies), || false)
        .unwrap();
    mark_drop_dispatched(&mut lease, &old);
    drop(lease);
    assert_eq!(
        entrance.management_phase(old.table()),
        MvManagementPhase::AwaitingEffectSettlement { unsettled: 1 }
    );
}

#[test]
fn ordinary_terminal_api_cannot_retire_a_dispatched_drop() {
    let old = target("mv", b"object-a");
    let dependencies = ManagementDependencySet::new([1; 32], [2; 32], None, runtime_id(4));
    let entrance = installed_entrance(&old, &dependencies);
    let mut lease = entrance
        .acquire(drop_request(&old, &dependencies), || false)
        .unwrap();
    mark_drop_dispatched(&mut lease, &old);
    assert_eq!(
        lease.record_terminal(EffectDisposition::KnownCommitted),
        Err(ManagementAdmissionError::InvalidEffect)
    );
    assert_eq!(
        entrance.management_phase(old.table()),
        MvManagementPhase::AwaitingEffectSettlement { unsettled: 1 }
    );
}

#[test]
fn expired_drop_observation_cannot_retire_management_state() {
    let old = target("mv", b"object-a");
    let dependencies = ManagementDependencySet::new([1; 32], [2; 32], None, runtime_id(4));
    let observation = ready_observation_state(old.clone(), "deployment-a", "inc-a");
    let entrance = ManagementEntrance::new(owner("deployment-a"), incarnation("inc-a"));
    entrance
        .install_observed_target(&observation, dependencies.clone())
        .unwrap();
    let mut lease = entrance
        .acquire(drop_request(&old, &dependencies), || false)
        .unwrap();
    mark_drop_dispatched(&mut lease, &old);
    observation.liveness().close();
    assert_eq!(
        lease.record_drop_terminal(EffectDisposition::KnownCommitted),
        Err(ManagementAdmissionError::ReadmissionIncomplete)
    );
    assert_eq!(
        entrance.management_phase(old.table()),
        MvManagementPhase::AwaitingEffectSettlement { unsettled: 1 }
    );
}

fn unsettle(
    entrance: &ManagementEntrance,
    target: &ManagedMvTarget,
    dependencies: &ManagementDependencySet,
) {
    let mut lease = entrance
        .acquire(
            ManagementRequest::try_new(
                target.catalog().clone(),
                target.table().clone(),
                Some(target.object_id().clone()),
                ConnectorDocumentManagementOperation::Publication,
                Some(dependencies.clone()),
                EffectScope::CATALOG_COMMIT,
            )
            .unwrap(),
            || false,
        )
        .unwrap();
    lease
        .mark_dispatched(EffectResponsibility::new(
            EffectIdentity::from_bytes([20; 16]),
            target.clone(),
            incarnation("inc-a"),
            EffectScope::CATALOG_COMMIT,
            ManagementTimestamp::from_unix_millis(1_000),
        ))
        .unwrap();
    drop(lease);
}

fn catalog_guarantee() -> RemoteEffectLifetimeGuarantee {
    RemoteEffectLifetimeGuarantee::try_new(
        EffectScope::CATALOG_COMMIT,
        Duration::from_secs(60),
        Duration::from_secs(5),
        RemoteEffectGuaranteeBasis::ProviderServiceContract,
        "iceberg rest service contract",
    )
    .unwrap()
}

#[test]
fn a_manageable_target_offers_a_handover_and_asks_for_no_evidence() {
    let target = target("mv", b"object-a");
    let dependencies = ManagementDependencySet::new([1; 32], [2; 32], Some([3; 32]), runtime_id(4));
    let service = ManagementContinuationService::new(
        installed_entrance(&target, &dependencies),
        RemoteEffectPolicy::default(),
    );

    let status = service
        .status(
            target.table(),
            Some(target.object_id().clone()),
            ReadmissionChallenge::from_bytes([7; 16]),
        )
        .unwrap();

    assert_eq!(status.phase, MvManagementPhase::Manageable);
    assert!(status.unsettled.is_empty());
    assert!(
        status.handover_available,
        "a settled target is exactly the one whose owner may be handed over"
    );
    assert_eq!(
        status.challenge,
        Some(ReadmissionChallenge::from_bytes([7; 16])),
        "the handover statement has to quote a challenge, so one is issued"
    );
    assert_eq!(
        status.required_evidence, None,
        "a handover asserts nothing about a writer elsewhere"
    );
    assert_eq!(status.local_owner.as_str(), "deployment-a");
}

#[test]
fn a_target_with_a_write_in_flight_offers_no_handover_and_no_challenge() {
    let target = target("mv", b"object-a");
    let dependencies = ManagementDependencySet::new([1; 32], [2; 32], Some([3; 32]), runtime_id(4));
    let entrance = installed_entrance(&target, &dependencies);
    let _lease = entrance
        .acquire(
            ManagementRequest::try_new(
                target.catalog().clone(),
                target.table().clone(),
                Some(target.object_id().clone()),
                ConnectorDocumentManagementOperation::SingleTargetUpdate,
                Some(dependencies.clone()),
                EffectScope::CATALOG_COMMIT,
            )
            .unwrap(),
            || false,
        )
        .expect("an installed target admits one write");
    let service = ManagementContinuationService::new(entrance, RemoteEffectPolicy::default());

    let status = service
        .status(
            target.table(),
            Some(target.object_id().clone()),
            ReadmissionChallenge::from_bytes([8; 16]),
        )
        .unwrap();

    assert_eq!(status.phase, MvManagementPhase::Managing);
    assert!(
        !status.handover_available,
        "a target another statement is writing cannot be handed away underneath it"
    );
    assert_eq!(
        status.challenge, None,
        "a challenge never suggests an action that does not exist"
    );
}

#[test]
fn an_unsettled_target_asks_for_the_evidence_its_policy_actually_needs() {
    let target = target("mv", b"object-a");
    let dependencies = ManagementDependencySet::new([1; 32], [2; 32], Some([3; 32]), runtime_id(4));
    let entrance = installed_entrance(&target, &dependencies);
    unsettle(&entrance, &target, &dependencies);
    let service = ManagementContinuationService::new(entrance, RemoteEffectPolicy::default());

    let status = service
        .status(
            target.table(),
            Some(target.object_id().clone()),
            ReadmissionChallenge::from_bytes([7; 16]),
        )
        .unwrap();

    assert_eq!(
        status.phase,
        MvManagementPhase::AwaitingEffectSettlement { unsettled: 1 }
    );
    assert_eq!(status.unsettled.len(), 1);
    assert_eq!(
        status.unsettled[0].mode,
        ReadmissionMode::OperatorDeclarationOnly,
        "no configured guarantee means only an operator can continue this"
    );
    assert_eq!(
        status.challenge,
        Some(ReadmissionChallenge::from_bytes([7; 16]))
    );
    let required = status.required_evidence.expect("operator evidence named");
    assert!(required.contains("isolated"), "{required}");
}

#[test]
fn a_guaranteed_path_waits_for_its_window_instead_of_an_operator() {
    let target = target("mv", b"object-a");
    let dependencies = ManagementDependencySet::new([1; 32], [2; 32], Some([3; 32]), runtime_id(4));
    let entrance = installed_entrance(&target, &dependencies);
    unsettle(&entrance, &target, &dependencies);
    let service = ManagementContinuationService::new(
        entrance,
        RemoteEffectPolicy::try_new(Some(catalog_guarantee()), None).unwrap(),
    );

    let status = service
        .status(
            target.table(),
            Some(target.object_id().clone()),
            ReadmissionChallenge::from_bytes([8; 16]),
        )
        .unwrap();

    assert_eq!(
        status.unsettled[0].mode,
        ReadmissionMode::AutomaticWhenGuaranteed
    );
    let required = status.required_evidence.expect("window evidence named");
    assert!(required.contains("elapsed"), "{required}");
}

#[test]
fn a_catalog_guarantee_cannot_be_declared_for_object_deletion() {
    assert_eq!(
        RemoteEffectPolicy::try_new(None, Some(catalog_guarantee())).unwrap_err(),
        ReadmissionError::GuaranteeScopeMismatch
    );
}

#[test]
fn an_effect_spanning_both_paths_needs_both_guarantees() {
    let both = RemoteEffectPolicy::try_new(Some(catalog_guarantee()), None).unwrap();
    assert!(
        both.guarantee_for(EffectScope::CATALOG_AND_OBJECT_DELETION)
            .is_none()
    );
    assert_eq!(
        both.mode_for(EffectScope::CATALOG_AND_OBJECT_DELETION),
        ReadmissionMode::OperatorDeclarationOnly
    );
    assert!(both.guarantee_for(EffectScope::CATALOG_COMMIT).is_some());
}

#[test]
fn a_challenge_is_issued_once_and_never_reissued() {
    let target = target("mv", b"object-a");
    let dependencies = ManagementDependencySet::new([1; 32], [2; 32], Some([3; 32]), runtime_id(4));
    let entrance = installed_entrance(&target, &dependencies);
    unsettle(&entrance, &target, &dependencies);
    let service = ManagementContinuationService::new(entrance, RemoteEffectPolicy::default());
    let challenge = ReadmissionChallenge::from_bytes([9; 16]);

    service
        .status(target.table(), None, challenge)
        .expect("first status issues the challenge");

    assert_eq!(
        service.status(target.table(), None, challenge).unwrap_err(),
        ReadmissionError::ReusedChallenge
    );
}

#[test]
fn an_unguaranteed_effect_refuses_a_window_resume_rather_than_timing_out() {
    let target = target("mv", b"object-a");
    let effect = unknown_effect(&target, 21, "inc-a", EffectScope::CATALOG_COMMIT, 1_000);
    let dependencies = ManagementDependencySet::new([1; 32], [2; 32], Some([3; 32]), runtime_id(4));
    let service = ManagementContinuationService::new(
        installed_entrance(&target, &dependencies),
        RemoteEffectPolicy::default(),
    );

    assert_eq!(
        service
            .resume_on_policy_window(
                &effect,
                &isolation(&target, "inc-a", 1_100),
                &VirtualManagementClock::new(ManagementTimestamp::from_unix_millis(9_999_999)),
            )
            .unwrap_err(),
        ReadmissionError::ManualMode
    );
}

#[test]
fn an_operator_declaration_permits_reobservation_without_deciding_the_outcome() {
    let target = target("mv", b"object-a");
    let dependencies = ManagementDependencySet::new([1; 32], [2; 32], Some([3; 32]), runtime_id(4));
    let entrance = installed_entrance(&target, &dependencies);
    unsettle(&entrance, &target, &dependencies);
    let service = ManagementContinuationService::new(entrance, RemoteEffectPolicy::default());
    let challenge = ReadmissionChallenge::from_bytes([11; 16]);
    let status = service.status(target.table(), None, challenge).unwrap();
    let effect = unknown_effect(&target, 20, "inc-a", EffectScope::CATALOG_COMMIT, 1_000);

    let permit = service
        .resume_on_declaration(
            &effect,
            &isolation(&target, "inc-a", 1_100),
            &ManualReadmissionDeclaration::try_new(
                status.challenge.expect("challenge issued"),
                effect.responsibility().identity(),
                target.clone(),
                incarnation("inc-a"),
                EffectScope::CATALOG_COMMIT,
                "operator@example",
                "deployment controller confirmed the old FE exited",
                ManagementTimestamp::from_unix_millis(1_200),
            )
            .unwrap(),
        )
        .expect("declaration permits re-observation");

    assert!(
        permit.preserves_unknown_disposition(),
        "a declaration must not decide what the effect did"
    );
}

#[test]
fn one_statement_settles_every_effect_the_declared_writer_left() {
    let target = target("mv", b"object-a");
    let dependencies = ManagementDependencySet::new([1; 32], [2; 32], Some([3; 32]), runtime_id(4));
    let entrance = installed_entrance(&target, &dependencies);
    unsettle(&entrance, &target, &dependencies);
    let service = ManagementContinuationService::new(entrance, RemoteEffectPolicy::default());
    let challenge = ReadmissionChallenge::from_bytes([31; 16]);
    service
        .status(target.table(), None, challenge)
        .expect("status issues the challenge");

    let permits = service
        .resume_target_on_declaration(
            target.table(),
            &MvResumeDeclaration {
                challenge,
                old_incarnation: incarnation("inc-a"),
                operator: "operator@example".to_string(),
                evidence: "deployment controller confirmed the old FE exited".to_string(),
                declared_at: ManagementTimestamp::from_unix_millis(2_000),
            },
        )
        .expect("the declaration covers this target");

    assert_eq!(permits.len(), 1);
    assert!(
        permits[0].preserves_unknown_disposition(),
        "a declaration permits re-observation; it does not decide what happened"
    );
}

#[test]
fn a_statement_about_another_writer_settles_nothing() {
    let target = target("mv", b"object-a");
    let dependencies = ManagementDependencySet::new([1; 32], [2; 32], Some([3; 32]), runtime_id(4));
    let entrance = installed_entrance(&target, &dependencies);
    unsettle(&entrance, &target, &dependencies);
    let service = ManagementContinuationService::new(entrance, RemoteEffectPolicy::default());
    let challenge = ReadmissionChallenge::from_bytes([32; 16]);
    service
        .status(target.table(), None, challenge)
        .expect("status issues the challenge");

    assert_eq!(
        service
            .resume_target_on_declaration(
                target.table(),
                &MvResumeDeclaration {
                    challenge,
                    old_incarnation: incarnation("someone-else"),
                    operator: "operator@example".to_string(),
                    evidence: "an unrelated writer was isolated".to_string(),
                    declared_at: ManagementTimestamp::from_unix_millis(2_000),
                },
            )
            .unwrap_err(),
        ReadmissionError::IncarnationMismatch,
    );
}

#[test]
fn a_spent_challenge_cannot_resume_a_second_time() {
    let target = target("mv", b"object-a");
    let dependencies = ManagementDependencySet::new([1; 32], [2; 32], Some([3; 32]), runtime_id(4));
    let entrance = installed_entrance(&target, &dependencies);
    unsettle(&entrance, &target, &dependencies);
    let service = ManagementContinuationService::new(entrance, RemoteEffectPolicy::default());
    let challenge = ReadmissionChallenge::from_bytes([33; 16]);
    service
        .status(target.table(), None, challenge)
        .expect("status issues the challenge");
    let declaration = MvResumeDeclaration {
        challenge,
        old_incarnation: incarnation("inc-a"),
        operator: "operator@example".to_string(),
        evidence: "deployment controller confirmed the old FE exited".to_string(),
        declared_at: ManagementTimestamp::from_unix_millis(2_000),
    };
    service
        .resume_target_on_declaration(target.table(), &declaration)
        .expect("the first statement is admitted");

    assert_eq!(
        service
            .resume_target_on_declaration(target.table(), &declaration)
            .unwrap_err(),
        ReadmissionError::MissingChallenge,
    );
}

#[test]
fn a_target_with_nothing_unresolved_has_no_declaration_to_admit() {
    let target = target("mv", b"object-a");
    let dependencies = ManagementDependencySet::new([1; 32], [2; 32], Some([3; 32]), runtime_id(4));
    let service = ManagementContinuationService::new(
        installed_entrance(&target, &dependencies),
        RemoteEffectPolicy::default(),
    );

    assert_eq!(
        service
            .resume_target_on_declaration(
                target.table(),
                &MvResumeDeclaration {
                    challenge: ReadmissionChallenge::from_bytes([34; 16]),
                    old_incarnation: incarnation("inc-a"),
                    operator: "operator@example".to_string(),
                    evidence: "nothing to settle".to_string(),
                    declared_at: ManagementTimestamp::from_unix_millis(2_000),
                },
            )
            .unwrap_err(),
        ReadmissionError::ManualMode,
    );
}

fn deletion_guarantee() -> RemoteEffectLifetimeGuarantee {
    RemoteEffectLifetimeGuarantee::try_new(
        EffectScope::OBJECT_DELETION,
        Duration::from_secs(60),
        Duration::from_secs(5),
        RemoteEffectGuaranteeBasis::ProviderServiceContract,
        "iceberg rest service contract",
    )
    .unwrap()
}

/// One target this process rediscovered from the lake, closed behind the
/// barrier a previous incarnation's possible dispatch leaves.
fn recovered_entrance(target: &ManagedMvTarget, previous: &str) -> ManagementEntrance {
    let entrance = ManagementEntrance::new(owner("deployment-a"), incarnation("inc-new"));
    let responsibility = EffectResponsibility::new(
        EffectIdentity::from_bytes([31; 16]),
        target.clone(),
        incarnation(previous),
        EffectScope::CATALOG_AND_OBJECT_DELETION,
        ManagementTimestamp::from_unix_millis(0),
    );
    let EffectTerminalFact::CommitUnknown(barrier) =
        responsibility.record_terminal(EffectDisposition::CommitUnknown)
    else {
        panic!("CommitUnknown produces an unsettled effect");
    };
    let observation = entrance
        .begin_recovered_target(
            target.clone(),
            ManagementContinuation::SameOwner {
                previous_incarnation: incarnation(previous),
            },
            barrier,
        )
        .unwrap();
    entrance.abandon_observation(observation).unwrap();
    entrance
}

fn startup_evidence(isolated: &str, isolated_at_ms: u64) -> StartupIsolationEvidence {
    StartupIsolationEvidence::try_accept(
        StartupIsolationDeclaration {
            deployment: owner("deployment-a"),
            for_incarnation: incarnation("inc-new"),
            nonce: StartupNonce::parse("launch-1").unwrap(),
            isolated_incarnations: [incarnation(isolated)].into_iter().collect(),
            scope: StartupIsolationScope::Deployment,
            isolated_at: ManagementTimestamp::from_unix_millis(isolated_at_ms),
            source: "supervisor recorded the previous process exited".to_string(),
        },
        &owner("deployment-a"),
        &incarnation("inc-new"),
        &StartupNonce::parse("launch-1").unwrap(),
        ManagementTimestamp::from_unix_millis(isolated_at_ms),
    )
    .unwrap()
}

#[test]
fn a_startup_statement_retires_a_recovery_barrier_only_after_its_window() {
    let target = target("mv", b"object-a");
    let service = ManagementContinuationService::new(
        recovered_entrance(&target, "inc-old"),
        RemoteEffectPolicy::try_new(Some(catalog_guarantee()), Some(deletion_guarantee())).unwrap(),
    );
    let evidence = startup_evidence("inc-old", 1_000);
    let clock = VirtualManagementClock::new(ManagementTimestamp::from_unix_millis(60_000));

    let waiting = service.resume_target_on_startup_isolation(target.table(), &evidence, &clock);
    let StartupContinuation::Waiting { deadline } = waiting else {
        panic!("the window runs from the declared isolation, not from the barrier: {waiting:?}");
    };
    assert_eq!(
        deadline,
        ManagementTimestamp::from_unix_millis(1_000 + 60_000 + 5_000),
        "the conservative T is the declared isolation moment"
    );

    clock.set(deadline);
    let ready = service.resume_target_on_startup_isolation(target.table(), &evidence, &clock);
    let StartupContinuation::Ready {
        previous_incarnation,
        permits,
    } = ready
    else {
        panic!("an elapsed window retires the barrier: {ready:?}");
    };
    assert_eq!(previous_incarnation, incarnation("inc-old"));
    assert_eq!(permits.len(), 1);
}

#[test]
fn a_startup_statement_without_a_guarantee_leaves_the_target_operator_only() {
    let target = target("mv", b"object-a");
    let service = ManagementContinuationService::new(
        recovered_entrance(&target, "inc-old"),
        RemoteEffectPolicy::default(),
    );
    let clock = VirtualManagementClock::new(ManagementTimestamp::from_unix_millis(u64::MAX / 2));

    assert!(matches!(
        service.resume_target_on_startup_isolation(
            target.table(),
            &startup_evidence("inc-old", 1_000),
            &clock,
        ),
        StartupContinuation::OperatorOnly
    ));
}

#[test]
fn a_startup_statement_says_nothing_about_a_writer_it_does_not_name() {
    let target = target("mv", b"object-a");
    let service = ManagementContinuationService::new(
        recovered_entrance(&target, "inc-old"),
        RemoteEffectPolicy::try_new(Some(catalog_guarantee()), Some(deletion_guarantee())).unwrap(),
    );
    let clock = VirtualManagementClock::new(ManagementTimestamp::from_unix_millis(u64::MAX / 2));

    assert!(matches!(
        service.resume_target_on_startup_isolation(
            target.table(),
            &startup_evidence("inc-someone-else", 1_000),
            &clock,
        ),
        StartupContinuation::NotCovered
    ));
}

#[test]
fn a_target_with_no_barrier_needs_no_startup_statement() {
    let target = target("mv", b"object-a");
    let dependencies = ManagementDependencySet::new([1; 32], [2; 32], Some([3; 32]), runtime_id(4));
    let service = ManagementContinuationService::new(
        installed_entrance(&target, &dependencies),
        RemoteEffectPolicy::try_new(Some(catalog_guarantee()), Some(deletion_guarantee())).unwrap(),
    );
    let clock = VirtualManagementClock::new(ManagementTimestamp::from_unix_millis(u64::MAX / 2));

    assert!(matches!(
        service.resume_target_on_startup_isolation(
            target.table(),
            &startup_evidence("inc-old", 1_000),
            &clock,
        ),
        StartupContinuation::Nothing
    ));
}

#[test]
fn a_bound_on_both_paths_covers_an_effect_that_spans_them() {
    let policy =
        RemoteEffectPolicy::try_new(Some(catalog_guarantee()), Some(deletion_guarantee())).unwrap();

    let guarantee = policy
        .guarantee_for(EffectScope::CATALOG_AND_OBJECT_DELETION)
        .expect("both paths are bounded, so the spanning scope is too");

    assert_eq!(
        guarantee.scope(),
        EffectScope::CATALOG_AND_OBJECT_DELETION,
        "a bound reported for a spanning scope has to cover it, or the window it \
         selects can never be evaluated"
    );
    assert!(
        guarantee
            .scope()
            .covers(EffectScope::CATALOG_AND_OBJECT_DELETION)
    );
    assert_eq!(
        policy.mode_for(EffectScope::CATALOG_AND_OBJECT_DELETION),
        ReadmissionMode::AutomaticWhenGuaranteed
    );
}

#[test]
fn one_bounded_path_does_not_bound_an_effect_that_spans_two() {
    let policy = RemoteEffectPolicy::try_new(Some(catalog_guarantee()), None).unwrap();

    assert!(
        policy
            .guarantee_for(EffectScope::CATALOG_AND_OBJECT_DELETION)
            .is_none()
    );
    assert_eq!(
        policy.mode_for(EffectScope::CATALOG_AND_OBJECT_DELETION),
        ReadmissionMode::OperatorDeclarationOnly
    );
}

#[test]
fn slot_change_unknown_remains_manageable_until_a_terminal_conclusion() {
    use novarocks_spi::connector::{
        CommitConclusion, CommitProof, ConnectorSlotName, UnknownCommitEvidence,
        UnknownCommitIdentity,
    };
    for conclusion in [
        CommitConclusion::StillUnknown,
        CommitConclusion::Committed(CommitProof::SlotWriter),
        CommitConclusion::NotCommitted(CommitProof::NegativeProof),
        CommitConclusion::InvalidatedUndetermined,
    ] {
        let target = target("mv-slot", b"object-slot");
        let observation = ready_observation_state(target.clone(), "deployment-a", "inc-a");
        let dependencies =
            ManagementDependencySet::new([1; 32], [2; 32], Some([3; 32]), runtime_id(4));
        let entrance = ManagementEntrance::new(owner("deployment-a"), incarnation("inc-a"));
        entrance
            .install_observed_target(&observation, dependencies.clone())
            .unwrap();
        let request = || {
            ManagementRequest::try_new(
                target.catalog().clone(),
                target.table().clone(),
                Some(target.object_id().clone()),
                ConnectorDocumentManagementOperation::SingleTargetUpdate,
                Some(dependencies.clone()),
                EffectScope::CATALOG_COMMIT,
            )
            .unwrap()
        };
        let mut lease = entrance.acquire(request(), || false).unwrap();
        lease
            .mark_dispatched(EffectResponsibility::new(
                EffectIdentity::from_bytes([31; 16]),
                target.clone(),
                incarnation("inc-a"),
                EffectScope::CATALOG_COMMIT,
                ManagementTimestamp::from_unix_millis(1_000),
            ))
            .unwrap();
        let evidence = UnknownCommitEvidence {
            asserted_parent: Some(17),
            preselected_snapshot: 18,
            identity: UnknownCommitIdentity::SlotWriter {
                slot: ConnectorSlotName::try_new("novarocks.mv", "configuration").unwrap(),
            },
        };
        lease.record_slot_change_unknown(evidence.clone()).unwrap();
        assert_eq!(entrance.pending_slot_changes(target.table()), 1);
        assert!(entrance.unsettled_effects(target.table()).is_empty());
        assert_eq!(
            entrance.management_phase(target.table()),
            MvManagementPhase::Manageable
        );
        drop(entrance.acquire(request(), || false).unwrap());
        for foreign in [
            ManagedMvTarget::try_new(
                catalog(2),
                target.table().clone(),
                target.object_id().clone(),
            )
            .unwrap(),
            ManagedMvTarget::try_new(
                target.catalog().clone(),
                target.table().clone(),
                object(b"another-object"),
            )
            .unwrap(),
        ] {
            assert!(!observe_keyed_management_mismatch(
                &entrance,
                &foreign,
                "deployment-a",
                "inc-b",
                2,
            ));
            let mut resolver_called = false;
            let error = entrance
                .resolve_slot_changes(
                    foreign.catalog(),
                    foreign.table(),
                    foreign.object_id(),
                    |_| {
                        resolver_called = true;
                        Ok(conclusion.clone())
                    },
                )
                .unwrap_err();
            assert_eq!(
                error.kind(),
                novarocks_spi::connector::ConnectorErrorKind::InvalidRequest
            );
            assert!(
                !resolver_called,
                "foreign target evidence must not reach the provider"
            );
            assert_eq!(entrance.pending_slot_changes(target.table()), 1);
            assert_eq!(entrance.latest_slot_change_conclusion(target.table()), None);
            assert_eq!(
                entrance.management_phase(target.table()),
                MvManagementPhase::Manageable
            );
        }
        let result = entrance
            .resolve_slot_changes(
                target.catalog(),
                target.table(),
                target.object_id(),
                |actual| {
                    assert_eq!(actual, evidence);
                    Ok(conclusion.clone())
                },
            )
            .unwrap();
        if conclusion == CommitConclusion::StillUnknown {
            assert_eq!(entrance.pending_slot_changes(target.table()), 1);
            assert_eq!(entrance.latest_slot_change_conclusion(target.table()), None);
        } else {
            assert_eq!(entrance.pending_slot_changes(target.table()), 0);
            assert_eq!(
                entrance.latest_slot_change_conclusion(target.table()),
                Some(conclusion.clone())
            );
        }
        assert_eq!(
            result.reinstall_current_projection,
            matches!(conclusion, CommitConclusion::Committed(_))
        );
        assert_eq!(
            entrance.management_phase(target.table()),
            MvManagementPhase::Manageable
        );
    }
}

#[test]
fn keyed_management_wrong_kind_is_rejected_before_owner_policy() {
    let target = target("mv", b"object-a");
    let mut state = ready_observation_state(target.clone(), "deployment-a", "inc-a");
    let pending = state
        .begin_current_observation(ManagementObservationRequestId::from_bytes([90; 16]))
        .unwrap();
    let mut view = keyed_management_view(&target, "deployment-b", "inc-b", 2);
    let mut content = Vec::new();
    for field in ["ordinary-table", "deployment-b", "inc-b"] {
        content.extend_from_slice(&(field.len() as u32).to_be_bytes());
        content.extend_from_slice(field.as_bytes());
    }
    view.slots
        .get_mut(&crate::persistence::keyed_documents::management_slot_name().unwrap())
        .unwrap()
        .value
        .content = Bytes::from(content);
    let (lease, request, calls) = keyed_management_lease(&target, Ok(view));
    assert_eq!(
        state
            .complete_keyed_current_observation(pending, &lease, request)
            .unwrap_err(),
        ManagementObservationError::InvalidProvenance
    );
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    // No valid foreign-owner observation was installed and the ticket is spent.
    assert_eq!(state.phase(), ManagementObservationPhase::Ready);
    assert_eq!(
        state
            .begin_current_observation(ManagementObservationRequestId::from_bytes([90; 16]))
            .err()
            .unwrap(),
        ManagementObservationError::ReusedObservationRequest
    );
}

#[test]
fn typed_keyed_replacement_closes_the_state_after_consuming_the_request() {
    use novarocks_spi::connector::{ConnectorError, ConnectorTableObjectBindingFailure};
    let target = target("mv", b"object-a");
    let mut state = ready_observation_state(target.clone(), "deployment-a", "inc-a");
    let pending = state
        .begin_current_observation(ManagementObservationRequestId::from_bytes([91; 16]))
        .unwrap();
    let (lease, request, calls) = keyed_management_lease(
        &target,
        Err(ConnectorError::table_object_binding(
            ConnectorTableObjectBindingFailure::Replaced,
            "test object was replaced",
        )
        .into()),
    );
    assert_eq!(
        state
            .complete_keyed_current_observation(pending, &lease, request)
            .unwrap_err(),
        ManagementObservationError::Ownership(ManagementOwnershipError::TargetReplaced)
    );
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert_eq!(
        state.phase(),
        ManagementObservationPhase::ClosedObjectReplacement
    );
    assert!(!state.liveness().is_open());
}

#[test]
fn keyed_foreign_owner_closes_the_exact_management_target() {
    let target = target("mv", b"object-a");
    let dependencies = ManagementDependencySet::new([1; 32], [2; 32], None, runtime_id(1));
    let entrance = installed_entrance(&target, &dependencies);
    assert!(observe_keyed_management_mismatch(
        &entrance,
        &target,
        "deployment-b",
        "inc-b",
        2
    ));
    assert_eq!(
        entrance.management_phase(target.table()),
        MvManagementPhase::OwnerMismatch
    );
}

#[test]
fn keyed_create_narrowing_rejects_another_logical_target_before_provider_io() {
    let exact = target("new_mv", b"created-object");
    let intent = CreateIntent::try_new(
        exact.catalog().clone(),
        exact.table().clone(),
        EffectIdentity::from_bytes([92; 16]),
    )
    .unwrap();
    let other = target("another_mv", b"another-object");
    let (lease, request, calls) = keyed_management_lease(
        &other,
        Ok(keyed_management_view(&other, "deployment-a", "inc-a", 1)),
    );
    assert_eq!(
        PendingCreateIntentObservation::for_intent(intent)
            .complete_keyed(&lease, request)
            .err()
            .unwrap(),
        ManagementObservationError::EffectTargetMismatch
    );
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 0);
}

/// Seed admission through a retained native observation, without constructing
/// FreshManagementObservation or a full-observation token in the test.
fn installed_keyed_drop_fixture() -> (
    ManagementEntrance,
    ManagedMvTarget,
    ManagementDependencySet,
    novarocks_spi::connector::ConnectorDocumentStorageLease,
    novarocks_spi::connector::KeyedDocumentObservationRequest,
    Arc<KeyedManagementObservationCaps>,
) {
    use crate::persistence::{
        keyed_documents::{decode_current_keyed_documents, management_slot, management_slot_name},
        test_support::{KeyedProjectionFixture, observed_current_keyed},
    };
    let target = target("drop-mv", b"drop-object");
    let mut fixture = KeyedProjectionFixture::new(
        crate::product::MvTarget::from_parts(Some("iceberg"), "db", "drop-mv"),
        Some(2),
    );
    fixture.object_id = target.object_id().clone();
    let facts = fixture.build().unwrap();
    let mut view = observed_current_keyed(&facts, target.catalog().clone()).view;
    view.owner.incarnation = ProviderBindingEpoch::from_bytes([7; 16]);
    view.slots
        .get_mut(&management_slot_name().unwrap())
        .unwrap()
        .value = management_slot(&owner("deployment-a"), &incarnation("inc-a")).unwrap();
    let (lease, request, caps) = keyed_management_lease_with_caps(&target, Ok(view));
    let mut observation = ManagementObservationState::try_new(
        target.clone(),
        owner("deployment-a"),
        incarnation("inc-a"),
        ManagementContinuation::SameOwner {
            previous_incarnation: incarnation("inc-a"),
        },
        vec![],
        None,
        None,
    )
    .unwrap();
    let pending = observation
        .begin_current_observation(ManagementObservationRequestId::from_bytes([93; 16]))
        .unwrap();
    let (phase, view) = observation
        .complete_keyed_current_observation(pending, &lease, request.clone())
        .unwrap();
    assert_eq!(phase, ManagementObservationPhase::Ready);
    let documents =
        decode_current_keyed_documents(view, target.catalog().clone(), Default::default()).unwrap();
    assert!(documents.publication.is_some());
    let dependencies = documents.management_dependencies(lease.control_runtime_id());
    let entrance = ManagementEntrance::new(owner("deployment-a"), incarnation("inc-a"));
    entrance
        .install_observed_target(&observation, dependencies.clone())
        .unwrap();
    (entrance, target, dependencies, lease, request, caps)
}

#[test]
fn keyed_drop_of_external_data_snapshot_preserves_current_projection_refusal() {
    use crate::persistence::keyed_documents::decode_current_keyed_documents;
    let (entrance, target, _, lease, request, caps) = installed_keyed_drop_fixture();
    let original = caps.reply.lock().unwrap().as_ref().unwrap().clone();
    let mut external = original.clone();
    external.current_snapshot = Some(3);
    external.metadata_version =
        ConnectorCommittedVersion::try_new(Bytes::from_static(b"external"), Some(3)).unwrap();
    external.current_records.clear();
    *caps.reply.lock().unwrap() = Ok(external.clone());
    assert!(
        decode_current_keyed_documents(external, target.catalog().clone(), Default::default())
            .is_err()
    );
    assert!(
        entrance
            .observe_keyed_admission(&lease, request.clone(), Default::default())
            .is_err()
    );
    let (first, _) = entrance
        .observe_keyed_drop(&lease, request.clone(), Default::default())
        .unwrap();
    assert_eq!(first.target(), &target);
    let mut management = entrance.acquire(first.request(), || false).unwrap();
    let (second, _) = entrance
        .observe_keyed_drop(&lease, request, Default::default())
        .unwrap();
    assert!(first.matches(&second));
    management
        .mark_dispatched(EffectResponsibility::new(
            EffectIdentity::from_bytes([94; 16]),
            target.clone(),
            incarnation("inc-a"),
            EffectScope::CATALOG_AND_OBJECT_DELETION,
            ManagementTimestamp::from_unix_millis(1_000),
        ))
        .unwrap();
    management
        .record_drop_terminal(EffectDisposition::KnownCommitted)
        .unwrap();
    assert_eq!(
        entrance.management_phase(target.table()),
        MvManagementPhase::NotObserved
    );
    // This management-only operation did not change the provider's data facts.
    let retained = caps.reply.lock().unwrap().as_ref().unwrap().clone();
    assert!(retained.current_records.is_empty());
    assert!(
        decode_current_keyed_documents(retained, target.catalog().clone(), Default::default())
            .is_err()
    );
    assert!(!original.current_records.is_empty());
}

#[test]
fn keyed_drop_refuses_foreign_owner_and_incarnation_without_renewing_admission() {
    use crate::persistence::keyed_documents::{management_slot, management_slot_name};
    for (marker_owner, marker_incarnation, expected) in [
        ("deployment-b", "inc-a", MvManagementPhase::OwnerMismatch),
        (
            "deployment-a",
            "inc-b",
            MvManagementPhase::IncarnationMismatch,
        ),
    ] {
        let (entrance, target, _, lease, request, caps) = installed_keyed_drop_fixture();
        caps.reply
            .lock()
            .unwrap()
            .as_mut()
            .unwrap()
            .slots
            .get_mut(&management_slot_name().unwrap())
            .unwrap()
            .value =
            management_slot(&owner(marker_owner), &incarnation(marker_incarnation)).unwrap();
        assert!(
            entrance
                .observe_keyed_drop(&lease, request, Default::default())
                .is_err()
        );
        assert_eq!(entrance.management_phase(target.table()), expected);
    }
}

#[test]
fn keyed_drop_refuses_exact_scope_changes_before_observation() {
    for wrong_catalog in [false, true] {
        let (entrance, target, _, lease, mut request, caps) = installed_keyed_drop_fixture();
        let calls = caps.calls.load(std::sync::atomic::Ordering::SeqCst);
        if wrong_catalog {
            request.catalog_handle = catalog(2);
        } else {
            request.object_id = object(b"replacement");
        }
        assert!(
            entrance
                .observe_keyed_drop(&lease, request, Default::default())
                .is_err()
        );
        assert_eq!(caps.calls.load(std::sync::atomic::Ordering::SeqCst), calls);
        assert_eq!(
            entrance.management_phase(target.table()),
            MvManagementPhase::Manageable
        );
    }
    let (entrance, target, _, _lease, request, caps) = installed_keyed_drop_fixture();
    let view = caps.reply.lock().unwrap().clone();
    let (another_runtime, _, _) = keyed_management_lease(&target, view);
    assert!(
        entrance
            .observe_keyed_drop(&another_runtime, request, Default::default())
            .is_err()
    );
}

#[test]
fn keyed_drop_does_not_bypass_publication_unknown_or_committed_convergence() {
    for disposition in [
        EffectDisposition::CommitUnknown,
        EffectDisposition::KnownCommitted,
    ] {
        let (entrance, target, dependencies, lease, request, _) = installed_keyed_drop_fixture();
        let mut publication = entrance
            .acquire(
                ManagementRequest::try_new(
                    target.catalog().clone(),
                    target.table().clone(),
                    Some(target.object_id().clone()),
                    ConnectorDocumentManagementOperation::Publication,
                    Some(dependencies),
                    EffectScope::CATALOG_COMMIT,
                )
                .unwrap(),
                || false,
            )
            .unwrap();
        publication
            .mark_dispatched(EffectResponsibility::new(
                EffectIdentity::from_bytes([95; 16]),
                target.clone(),
                incarnation("inc-a"),
                EffectScope::CATALOG_COMMIT,
                ManagementTimestamp::from_unix_millis(1_000),
            ))
            .unwrap();
        publication.record_terminal(disposition).unwrap();
        assert!(
            entrance
                .observe_keyed_drop(&lease, request, Default::default())
                .is_err()
        );
        assert_ne!(
            entrance.management_phase(target.table()),
            MvManagementPhase::Manageable
        );
    }
}

#[test]
fn keyed_drop_reconciles_soft_slot_evidence_through_the_retained_lease() {
    use novarocks_spi::connector::{
        CommitConclusion, CommitProof, ConnectorSlotName, UnknownCommitEvidence,
        UnknownCommitIdentity,
    };
    let (entrance, target, dependencies, lease, request, caps) = installed_keyed_drop_fixture();
    let mut slot = entrance
        .acquire(
            ManagementRequest::try_new(
                target.catalog().clone(),
                target.table().clone(),
                Some(target.object_id().clone()),
                ConnectorDocumentManagementOperation::SingleTargetUpdate,
                Some(dependencies),
                EffectScope::CATALOG_COMMIT,
            )
            .unwrap(),
            || false,
        )
        .unwrap();
    slot.mark_dispatched(EffectResponsibility::new(
        EffectIdentity::from_bytes([96; 16]),
        target.clone(),
        incarnation("inc-a"),
        EffectScope::CATALOG_COMMIT,
        ManagementTimestamp::from_unix_millis(1_000),
    ))
    .unwrap();
    slot.record_slot_change_unknown(UnknownCommitEvidence {
        asserted_parent: Some(2),
        preselected_snapshot: 3,
        identity: UnknownCommitIdentity::SlotWriter {
            slot: ConnectorSlotName::try_new("novarocks.mv", "configuration").unwrap(),
        },
    })
    .unwrap();
    caps.conclusions.lock().unwrap().extend([
        CommitConclusion::StillUnknown,
        CommitConclusion::Committed(CommitProof::SlotWriter),
    ]);
    let (first, unknown) = entrance
        .observe_keyed_drop(&lease, request.clone(), Default::default())
        .unwrap();
    assert!(unknown.conclusions.is_empty());
    assert_eq!(entrance.pending_slot_changes(target.table()), 1);
    assert_eq!(
        entrance.management_phase(target.table()),
        MvManagementPhase::Manageable
    );
    let before = caps.calls.load(std::sync::atomic::Ordering::SeqCst);
    let (second, committed) = entrance
        .observe_keyed_drop(&lease, request, Default::default())
        .unwrap();
    assert!(first.matches(&second));
    assert!(committed.reinstall_current_projection);
    assert_eq!(
        caps.calls.load(std::sync::atomic::Ordering::SeqCst),
        before + 2
    );
    assert_eq!(entrance.pending_slot_changes(target.table()), 0);
    assert_eq!(
        entrance.latest_slot_change_conclusion(target.table()),
        Some(CommitConclusion::Committed(CommitProof::SlotWriter))
    );
    assert_eq!(
        entrance.management_phase(target.table()),
        MvManagementPhase::Manageable
    );
}

#[test]
fn keyed_drop_requires_an_intact_current_root_closure() {
    let (entrance, target, _, lease, request, caps) = installed_keyed_drop_fixture();
    {
        let mut reply = caps.reply.lock().unwrap();
        let view = reply.as_mut().unwrap();
        let root = view.roots["novarocks.mv"].identity;
        view.versions.remove(&root);
    }
    assert!(
        entrance
            .observe_keyed_drop(&lease, request, Default::default())
            .is_err()
    );
    assert_eq!(
        entrance.management_phase(target.table()),
        MvManagementPhase::Manageable
    );
}

#[test]
fn fresh_foreign_keyed_admission_closes_old_authority_even_without_current_p() {
    use crate::persistence::keyed_documents::{management_slot, management_slot_name};
    for publication in ["valid", "missing", "malformed"] {
        let (entrance, target, dependencies, lease, request, caps) = installed_keyed_drop_fixture();
        let (_, prior_admission, _) = entrance
            .observe_keyed_admission(&lease, request.clone(), Default::default())
            .unwrap();
        assert!(prior_admission.is_open());
        {
            let mut reply = caps.reply.lock().unwrap();
            let view = reply.as_mut().unwrap();
            view.slots
                .get_mut(&management_slot_name().unwrap())
                .unwrap()
                .value = management_slot(&owner("deployment-b"), &incarnation("inc-b")).unwrap();
            match publication {
                "missing" => view.current_records.clear(),
                "malformed" => {
                    view.current_records[0].payload = Bytes::from_static(b"invalid MV P")
                }
                _ => {}
            }
        }
        assert!(
            entrance
                .observe_keyed_admission(&lease, request, Default::default())
                .is_err()
        );
        assert!(!prior_admission.is_open());
        assert_eq!(
            entrance.management_phase(target.table()),
            MvManagementPhase::OwnerMismatch
        );
        let request = ManagementRequest::try_new(
            target.catalog().clone(),
            target.table().clone(),
            Some(target.object_id().clone()),
            ConnectorDocumentManagementOperation::Publication,
            Some(dependencies),
            EffectScope::CATALOG_COMMIT,
        )
        .unwrap();
        assert!(entrance.acquire(request, || false).is_err());
    }
}

#[test]
fn keyed_drop_freezes_configuration_content_and_rejects_changed_root_semantics() {
    use crate::persistence::{
        codec::decode_configuration,
        keyed_documents::{
            configuration_slot_name, keyed_configuration_slot_change, management_slot_name,
        },
        test_support::{KeyedProjectionFixture, observed_current_keyed},
    };
    let (entrance, target, _, lease, request, caps) = installed_keyed_drop_fixture();
    let (first, _) = entrance
        .observe_keyed_drop(&lease, request.clone(), Default::default())
        .unwrap();
    {
        let mut reply = caps.reply.lock().unwrap();
        let view = reply.as_mut().unwrap();
        let slot = view
            .slots
            .get_mut(&configuration_slot_name().unwrap())
            .unwrap();
        let mut configuration =
            decode_configuration(&slot.value.content, Default::default()).unwrap();
        configuration.paused = !configuration.paused;
        slot.value = keyed_configuration_slot_change(&configuration)
            .unwrap()
            .slots
            .into_values()
            .next()
            .unwrap();
    }
    let (second, _) = entrance
        .observe_keyed_drop(&lease, request.clone(), Default::default())
        .unwrap();
    assert!(!first.matches(&second));
    let mut changed = KeyedProjectionFixture::new(
        crate::product::MvTarget::from_parts(Some("iceberg"), "db", "drop-mv"),
        Some(2),
    )
    .with_retraction_count();
    changed.object_id = target.object_id().clone();
    let mut view = observed_current_keyed(&changed.build().unwrap(), target.catalog().clone()).view;
    let old = caps.reply.lock().unwrap().as_ref().unwrap().clone();
    view.owner = old.owner;
    view.slots.insert(
        management_slot_name().unwrap(),
        old.slots[&management_slot_name().unwrap()].clone(),
    );
    *caps.reply.lock().unwrap() = Ok(view);
    assert!(
        entrance
            .observe_keyed_drop(&lease, request, Default::default())
            .is_err()
    );
}

#[test]
fn keyed_drop_revokes_foreign_management_before_corrupt_configuration_or_root() {
    use crate::persistence::keyed_documents::{
        configuration_slot_name, management_slot, management_slot_name,
    };
    use novarocks_spi::connector::{
        CommitConclusion, CommitProof, ConnectorSlotName, UnknownCommitEvidence,
        UnknownCommitIdentity,
    };
    for after_resolution in [false, true] {
        for corrupt_configuration in [false, true] {
            let (entrance, target, dependencies, lease, request, caps) =
                installed_keyed_drop_fixture();
            let (_, prior, _) = entrance
                .observe_keyed_admission(&lease, request.clone(), Default::default())
                .unwrap();
            let old_request = ManagementRequest::try_new(
                target.catalog().clone(),
                target.table().clone(),
                Some(target.object_id().clone()),
                ConnectorDocumentManagementOperation::Publication,
                Some(dependencies.clone()),
                EffectScope::CATALOG_COMMIT,
            )
            .unwrap();
            let mut slot = entrance
                .acquire(
                    ManagementRequest::try_new(
                        target.catalog().clone(),
                        target.table().clone(),
                        Some(target.object_id().clone()),
                        ConnectorDocumentManagementOperation::SingleTargetUpdate,
                        Some(dependencies),
                        EffectScope::CATALOG_COMMIT,
                    )
                    .unwrap(),
                    || false,
                )
                .unwrap();
            slot.mark_dispatched(EffectResponsibility::new(
                EffectIdentity::from_bytes([97; 16]),
                target.clone(),
                incarnation("inc-a"),
                EffectScope::CATALOG_COMMIT,
                ManagementTimestamp::from_unix_millis(1_000),
            ))
            .unwrap();
            slot.record_slot_change_unknown(UnknownCommitEvidence {
                asserted_parent: Some(2),
                preselected_snapshot: 3,
                identity: UnknownCommitIdentity::SlotWriter {
                    slot: ConnectorSlotName::try_new("novarocks.mv", "configuration").unwrap(),
                },
            })
            .unwrap();
            let mut foreign = caps.reply.lock().unwrap().as_ref().unwrap().clone();
            foreign
                .slots
                .get_mut(&management_slot_name().unwrap())
                .unwrap()
                .value = management_slot(&owner("deployment-b"), &incarnation("inc-b")).unwrap();
            if corrupt_configuration {
                foreign
                    .slots
                    .get_mut(&configuration_slot_name().unwrap())
                    .unwrap()
                    .value
                    .content = Bytes::new();
            } else {
                foreign.roots.get_mut("novarocks.mv").unwrap().dangling = true;
            }
            caps.conclusions
                .lock()
                .unwrap()
                .push_back(CommitConclusion::Committed(CommitProof::SlotWriter));
            if after_resolution {
                *caps.after_resolution.lock().unwrap() = Some(foreign);
            } else {
                *caps.reply.lock().unwrap() = Ok(foreign);
            }
            assert!(
                entrance
                    .observe_keyed_drop(&lease, request, Default::default())
                    .is_err()
            );
            assert!(!prior.is_open());
            assert_eq!(
                entrance.management_phase(target.table()),
                MvManagementPhase::OwnerMismatch
            );
            assert!(entrance.acquire(old_request, || false).is_err());
            assert_eq!(
                entrance.pending_slot_changes(target.table()),
                usize::from(!after_resolution)
            );
            assert_eq!(
                caps.conclusions.lock().unwrap().len(),
                usize::from(!after_resolution)
            );
        }
    }
}

#[test]
fn keyed_drop_corrupt_configuration_or_root_still_refuses_for_the_matching_owner() {
    use crate::persistence::keyed_documents::configuration_slot_name;
    for corrupt_configuration in [false, true] {
        let (entrance, _, _, lease, request, caps) = installed_keyed_drop_fixture();
        let (_, prior, _) = entrance
            .observe_keyed_admission(&lease, request.clone(), Default::default())
            .unwrap();
        {
            let mut reply = caps.reply.lock().unwrap();
            let view = reply.as_mut().unwrap();
            if corrupt_configuration {
                view.slots
                    .get_mut(&configuration_slot_name().unwrap())
                    .unwrap()
                    .value
                    .content = Bytes::new();
            } else {
                view.roots.get_mut("novarocks.mv").unwrap().dangling = true;
            }
        }
        assert!(
            entrance
                .observe_keyed_drop(&lease, request, Default::default())
                .is_err()
        );
        assert!(prior.is_open());
    }
}

#[test]
fn keyed_drop_foreign_scope_does_not_revoke_another_targets_authority() {
    use crate::persistence::keyed_documents::{management_slot, management_slot_name};
    for foreign_catalog in [false, true] {
        let (entrance, target, dependencies, lease, mut request, caps) =
            installed_keyed_drop_fixture();
        let (_, prior, _) = entrance
            .observe_keyed_admission(&lease, request.clone(), Default::default())
            .unwrap();
        {
            let mut reply = caps.reply.lock().unwrap();
            let view = reply.as_mut().unwrap();
            view.slots
                .get_mut(&management_slot_name().unwrap())
                .unwrap()
                .value = management_slot(&owner("deployment-b"), &incarnation("inc-b")).unwrap();
            if foreign_catalog {
                request.catalog_handle = catalog(2);
            } else {
                view.object_id = object(b"another-current-object");
            }
        }
        assert!(
            entrance
                .observe_keyed_drop(&lease, request, Default::default())
                .is_err()
        );
        assert!(prior.is_open());
        assert_eq!(
            entrance.management_phase(target.table()),
            MvManagementPhase::Manageable
        );
        drop(
            entrance
                .acquire(
                    ManagementRequest::try_new(
                        target.catalog().clone(),
                        target.table().clone(),
                        Some(target.object_id().clone()),
                        ConnectorDocumentManagementOperation::Publication,
                        Some(dependencies),
                        EffectScope::CATALOG_COMMIT,
                    )
                    .unwrap(),
                    || false,
                )
                .unwrap(),
        );
    }
}

#[test]
fn shared_current_reader_revokes_foreign_authority_before_publication_decoding() {
    use crate::persistence::keyed_documents::{
        management_slot, management_slot_name, observe_current_keyed_documents_with_management,
    };
    for publication in ["valid", "missing", "malformed"] {
        let (entrance, target, dependencies, lease, request, caps) = installed_keyed_drop_fixture();
        let (_, prior, _) = entrance
            .observe_keyed_admission(&lease, request.clone(), Default::default())
            .unwrap();
        {
            let mut reply = caps.reply.lock().unwrap();
            let view = reply.as_mut().unwrap();
            view.slots
                .get_mut(&management_slot_name().unwrap())
                .unwrap()
                .value = management_slot(&owner("deployment-b"), &incarnation("inc-b")).unwrap();
            match publication {
                "missing" => view.current_records.clear(),
                "malformed" => view.current_records[0].payload = Bytes::from_static(b"invalid P"),
                _ => {}
            }
        }
        let before = caps.calls.load(std::sync::atomic::Ordering::SeqCst);
        let result = observe_current_keyed_documents_with_management(
            &lease,
            request,
            Default::default(),
            &entrance,
        );
        assert_eq!(
            caps.calls.load(std::sync::atomic::Ordering::SeqCst),
            before + 1
        );
        if publication == "valid" {
            let (foreign, _) = result.unwrap();
            assert_eq!(foreign.deployment_owner, owner("deployment-b"));
            assert_eq!(foreign.process_incarnation, incarnation("inc-b"));
            assert!(foreign.publication.is_some());
        } else {
            assert!(result.is_err());
        }
        assert!(!prior.is_open());
        assert_eq!(
            entrance.management_phase(target.table()),
            MvManagementPhase::OwnerMismatch
        );
        assert!(
            entrance
                .acquire(
                    ManagementRequest::try_new(
                        target.catalog().clone(),
                        target.table().clone(),
                        Some(target.object_id().clone()),
                        ConnectorDocumentManagementOperation::Publication,
                        Some(dependencies),
                        EffectScope::CATALOG_COMMIT,
                    )
                    .unwrap(),
                    || false
                )
                .is_err()
        );
    }
}

#[test]
fn shared_current_reader_revokes_from_the_actual_post_resolution_reload() {
    use crate::persistence::keyed_documents::{
        management_slot, management_slot_name, observe_current_keyed_documents_with_management,
    };
    use novarocks_spi::connector::{
        CommitConclusion, CommitProof, ConnectorSlotName, UnknownCommitEvidence,
        UnknownCommitIdentity,
    };
    let (entrance, target, dependencies, lease, request, caps) = installed_keyed_drop_fixture();
    let (_, prior, _) = entrance
        .observe_keyed_admission(&lease, request.clone(), Default::default())
        .unwrap();
    let old_request = ManagementRequest::try_new(
        target.catalog().clone(),
        target.table().clone(),
        Some(target.object_id().clone()),
        ConnectorDocumentManagementOperation::Publication,
        Some(dependencies.clone()),
        EffectScope::CATALOG_COMMIT,
    )
    .unwrap();
    let mut slot = entrance
        .acquire(
            ManagementRequest::try_new(
                target.catalog().clone(),
                target.table().clone(),
                Some(target.object_id().clone()),
                ConnectorDocumentManagementOperation::SingleTargetUpdate,
                Some(dependencies),
                EffectScope::CATALOG_COMMIT,
            )
            .unwrap(),
            || false,
        )
        .unwrap();
    slot.mark_dispatched(EffectResponsibility::new(
        EffectIdentity::from_bytes([98; 16]),
        target.clone(),
        incarnation("inc-a"),
        EffectScope::CATALOG_COMMIT,
        ManagementTimestamp::from_unix_millis(1_000),
    ))
    .unwrap();
    slot.record_slot_change_unknown(UnknownCommitEvidence {
        asserted_parent: Some(2),
        preselected_snapshot: 3,
        identity: UnknownCommitIdentity::SlotWriter {
            slot: ConnectorSlotName::try_new("novarocks.mv", "configuration").unwrap(),
        },
    })
    .unwrap();
    let mut foreign = caps.reply.lock().unwrap().as_ref().unwrap().clone();
    foreign
        .slots
        .get_mut(&management_slot_name().unwrap())
        .unwrap()
        .value = management_slot(&owner("deployment-b"), &incarnation("inc-b")).unwrap();
    foreign.current_records[0].payload = Bytes::from_static(b"invalid P after resolver");
    *caps.after_resolution.lock().unwrap() = Some(foreign);
    caps.conclusions
        .lock()
        .unwrap()
        .push_back(CommitConclusion::Committed(CommitProof::SlotWriter));
    let before = caps.calls.load(std::sync::atomic::Ordering::SeqCst);
    assert!(
        observe_current_keyed_documents_with_management(
            &lease,
            request,
            Default::default(),
            &entrance
        )
        .is_err()
    );
    assert_eq!(
        caps.calls.load(std::sync::atomic::Ordering::SeqCst),
        before + 2
    );
    assert!(!prior.is_open());
    assert_eq!(
        entrance.management_phase(target.table()),
        MvManagementPhase::OwnerMismatch
    );
    assert!(entrance.acquire(old_request, || false).is_err());
    assert_eq!(entrance.pending_slot_changes(target.table()), 0);
}

#[test]
fn shared_current_reader_foreign_scope_does_not_revoke_another_target() {
    use crate::persistence::keyed_documents::{
        management_slot, management_slot_name, observe_current_keyed_documents_with_management,
    };
    let (entrance, target, dependencies, lease, request, caps) = installed_keyed_drop_fixture();
    let (_, prior, _) = entrance
        .observe_keyed_admission(&lease, request.clone(), Default::default())
        .unwrap();
    {
        let mut reply = caps.reply.lock().unwrap();
        let view = reply.as_mut().unwrap();
        view.object_id = object(b"unrelated-object");
        view.slots
            .get_mut(&management_slot_name().unwrap())
            .unwrap()
            .value = management_slot(&owner("deployment-b"), &incarnation("inc-b")).unwrap();
    }
    assert!(
        observe_current_keyed_documents_with_management(
            &lease,
            request,
            Default::default(),
            &entrance
        )
        .is_err()
    );
    assert!(prior.is_open());
    assert_eq!(
        entrance.management_phase(target.table()),
        MvManagementPhase::Manageable
    );
    drop(
        entrance
            .acquire(
                ManagementRequest::try_new(
                    target.catalog().clone(),
                    target.table().clone(),
                    Some(target.object_id().clone()),
                    ConnectorDocumentManagementOperation::Publication,
                    Some(dependencies),
                    EffectScope::CATALOG_COMMIT,
                )
                .unwrap(),
                || false,
            )
            .unwrap(),
    );
}

#[test]
fn borrowed_terminal_releases_real_activity_before_the_stable_lease_is_destroyed() {
    let (entrance, target, request) =
        publication_terminal_fixture(ConnectorDocumentManagementOperation::Publication);
    let mut first = entrance
        .request(
            request.clone(),
            crate::activity::MvActivityOwner::ScheduledRefresh,
        )
        .unwrap();
    let mut first = first.try_acquire().unwrap().unwrap();
    first
        .mark_dispatched(EffectResponsibility::new(
            EffectIdentity::from_bytes([91; 16]),
            target.clone(),
            incarnation("inc-a"),
            EffectScope::CATALOG_COMMIT,
            ManagementTimestamp::from_unix_millis(1_000),
        ))
        .unwrap();
    let mut next = entrance
        .request(request, crate::activity::MvActivityOwner::ManualRefresh)
        .unwrap();
    assert!(next.try_acquire().unwrap().is_none());
    first
        .record_terminal_in_place(EffectDisposition::KnownUncommitted)
        .unwrap();
    let next = next
        .try_acquire()
        .unwrap()
        .expect("actual terminal releases the original FIFO turn");
    // Keep the old stable lease alive through the proof. Its later Drop must
    // neither pin the turn nor invent an Unknown effect after real settlement.
    drop(first);
    assert!(entrance.unsettled_effects(target.table()).is_empty());
    drop(next);
    assert_eq!(
        entrance.management_phase(target.table()),
        MvManagementPhase::Manageable
    );
}

#[test]
fn actual_provider_view_revokes_foreign_management_without_projection_or_another_observation() {
    use crate::persistence::keyed_documents::{management_slot, management_slot_name};
    for publication in ["valid", "missing", "malformed"] {
        let (entrance, target, dependencies, lease, request, caps) = installed_keyed_drop_fixture();
        let (_, prior, _) = entrance
            .observe_keyed_admission(&lease, request.clone(), Default::default())
            .unwrap();
        {
            let mut reply = caps.reply.lock().unwrap();
            let view = reply.as_mut().unwrap();
            view.slots
                .get_mut(&management_slot_name().unwrap())
                .unwrap()
                .value = management_slot(&owner("deployment-b"), &incarnation("inc-b")).unwrap();
            match publication {
                "missing" => view.current_records.clear(),
                "malformed" => view.current_records[0].payload = Bytes::from_static(b"invalid P"),
                _ => {}
            }
        }
        let before = caps.calls.load(std::sync::atomic::Ordering::SeqCst);
        // This is the same provider observation the guard already obtained.
        // The negative author does not decode P or issue another storage call.
        let view = lease.observe_keyed_documents(request.clone()).unwrap();
        assert!(
            entrance
                .revoke_foreign_current_management(&view, &request.catalog_handle)
                .unwrap()
        );
        assert_eq!(
            caps.calls.load(std::sync::atomic::Ordering::SeqCst),
            before + 1
        );
        assert!(!prior.is_open());
        assert_eq!(
            entrance.management_phase(target.table()),
            MvManagementPhase::OwnerMismatch
        );
        assert!(
            entrance
                .acquire(
                    ManagementRequest::try_new(
                        target.catalog().clone(),
                        target.table().clone(),
                        Some(target.object_id().clone()),
                        ConnectorDocumentManagementOperation::Publication,
                        Some(dependencies),
                        EffectScope::CATALOG_COMMIT,
                    )
                    .unwrap(),
                    || false
                )
                .is_err()
        );
    }
}

#[test]
fn actual_foreign_provider_scope_cannot_revoke_another_management_target() {
    let (entrance, target, _, lease, request, caps) = installed_keyed_drop_fixture();
    let (_, prior, _) = entrance
        .observe_keyed_admission(&lease, request.clone(), Default::default())
        .unwrap();
    {
        use crate::persistence::keyed_documents::{management_slot, management_slot_name};
        caps.reply
            .lock()
            .unwrap()
            .as_mut()
            .unwrap()
            .slots
            .get_mut(&management_slot_name().unwrap())
            .unwrap()
            .value = management_slot(&owner("deployment-b"), &incarnation("inc-b")).unwrap();
    }
    let view = lease.observe_keyed_documents(request.clone()).unwrap();
    let before = caps.calls.load(std::sync::atomic::Ordering::SeqCst);
    let mut foreign = view.clone();
    foreign.object_id =
        ConnectorTableObjectId::try_new(Bytes::from_static(b"another-object")).unwrap();
    assert!(
        !entrance
            .revoke_foreign_current_management(&foreign, &request.catalog_handle)
            .unwrap()
    );
    let other_catalog = CatalogHandle::new(
        target.table().instance_id.clone(),
        CatalogVersion::from_bytes([91; 32]),
    );
    assert!(
        !entrance
            .revoke_foreign_current_management(&view, &other_catalog)
            .unwrap()
    );
    assert!(prior.is_open());
    assert_eq!(
        entrance.management_phase(target.table()),
        MvManagementPhase::Manageable
    );
    assert_eq!(caps.calls.load(std::sync::atomic::Ordering::SeqCst), before);
}

#[tokio::test(flavor = "current_thread")]
async fn async_keyed_pending_observation_mints_the_same_exact_fresh_capability() {
    use std::sync::atomic::Ordering::SeqCst;
    let target = target("mv", b"async-object");
    let mut state = ready_observation_state(target.clone(), "deployment-a", "inc-a");
    let id = ManagementObservationRequestId::from_bytes([101; 16]);
    let pending = state.begin_current_observation(id).unwrap();
    let view = keyed_management_view(&target, "deployment-a", "inc-a", 2);
    let expected_version = view.metadata_version.clone();
    let (lease, request, caps) = keyed_management_lease_with_caps(&target, Ok(view));
    let (release, gate) = tokio::sync::oneshot::channel();
    *caps.async_gate.lock().unwrap() = Some(gate);
    let mut observation = Box::pin(pending.complete_keyed_async(&lease, request));
    assert!(futures::poll!(observation.as_mut()).is_pending());
    assert_eq!(caps.async_calls.load(SeqCst), 1);
    assert_eq!(caps.calls.load(SeqCst), 0, "no synchronous fallback");
    assert_eq!(caps.async_exits.load(SeqCst), 0);
    release.send(()).unwrap();
    let (fresh, actual) = observation.await.unwrap();
    assert_eq!(fresh.target(), &target);
    assert_eq!(fresh.owner(), &owner("deployment-a"));
    assert_eq!(fresh.incarnation(), &incarnation("inc-a"));
    assert_eq!(fresh.metadata_version(), &expected_version);
    assert_eq!(actual.object_id, *target.object_id());
    assert_eq!(
        state.accept_current_observation(fresh).unwrap(),
        ManagementObservationPhase::Ready,
    );
    assert_eq!(caps.async_calls.load(SeqCst), 1);
    assert_eq!(caps.async_exits.load(SeqCst), 1);
    assert_eq!(
        state.begin_current_observation(id).err().unwrap(),
        ManagementObservationError::ReusedObservationRequest,
    );
    let id = ManagementObservationRequestId::from_bytes([110; 16]);
    let pending = state.begin_current_observation(id).unwrap();
    let (lease, request, caps) = keyed_management_lease_with_caps(
        &target,
        Ok(keyed_management_view(&target, "deployment-a", "inc-a", 3)),
    );
    let (phase, view) = state
        .complete_keyed_current_observation_async(pending, &lease, request)
        .await
        .unwrap();
    assert_eq!(phase, ManagementObservationPhase::Ready);
    assert_eq!(view.object_id, *target.object_id());
    assert_eq!(caps.async_calls.load(SeqCst), 1);
    assert_eq!(caps.async_exits.load(SeqCst), 1);
    assert_eq!(caps.calls.load(SeqCst), 0);
    assert_eq!(
        state.begin_current_observation(id).err().unwrap(),
        ManagementObservationError::ReusedObservationRequest
    );
}

#[tokio::test(flavor = "current_thread")]
async fn async_state_consumes_the_ticket_before_foreign_pending_or_borrower_drop() {
    use std::sync::atomic::Ordering::SeqCst;
    let target = target("mv", b"async-object");
    let mut state = ready_observation_state(target.clone(), "deployment-a", "inc-a");
    let id = ManagementObservationRequestId::from_bytes([102; 16]);
    let pending = state.begin_current_observation(id).unwrap();
    let (lease, request, caps) = keyed_management_lease_with_caps(
        &target,
        Ok(keyed_management_view(&target, "deployment-a", "inc-a", 2)),
    );
    let (release, gate) = tokio::sync::oneshot::channel();
    *caps.async_gate.lock().unwrap() = Some(gate);
    let mut observation =
        Box::pin(state.complete_keyed_current_observation_async(pending, &lease, request));
    assert!(futures::poll!(observation.as_mut()).is_pending());
    assert_eq!(caps.async_calls.load(SeqCst), 1);
    assert_eq!(caps.async_exits.load(SeqCst), 0);
    drop(observation);
    assert!(
        release.send(()).is_err(),
        "the original borrowed future exited"
    );
    assert_eq!(caps.async_exits.load(SeqCst), 1);
    assert_eq!(state.phase(), ManagementObservationPhase::Ready);
    assert!(state.liveness().is_open());
    assert_eq!(
        state.begin_current_observation(id).err().unwrap(),
        ManagementObservationError::ReusedObservationRequest,
    );
    state
        .begin_current_observation(ManagementObservationRequestId::from_bytes([103; 16]))
        .expect("the spent ticket does not leave an active request behind");
    assert_eq!(caps.calls.load(SeqCst), 0);
}

#[tokio::test(flavor = "current_thread")]
async fn async_keyed_failure_preserves_the_original_shared_cause_and_fact() {
    use novarocks_failure_contract::{FailureProjection, SharedFailureCause};
    use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind};
    use std::sync::atomic::Ordering::SeqCst;
    let target = target("mv", b"async-object");
    let mut state = ready_observation_state(target.clone(), "deployment-a", "inc-a");
    let id = ManagementObservationRequestId::from_bytes([104; 16]);
    let pending = state.begin_current_observation(id).unwrap();
    let error = ConnectorError::compile_interruption(
        novarocks_type_contract::CompileControlError::Cancelled,
    );
    let original = std::error::Error::source(&error)
        .and_then(|source| source.downcast_ref::<SharedFailureCause>())
        .unwrap()
        .clone();
    let original_fact = original.failure_fact();
    let (lease, request, caps) = keyed_management_lease_with_caps(&target, Err(error.into()));
    let failure = state
        .complete_keyed_current_observation_async(pending, &lease, request)
        .await
        .unwrap_err();
    assert_eq!(
        failure.kind(),
        &ManagementObservationError::UnsealedObservation
    );
    assert_eq!(failure.original_failure(), Some(&original));
    assert_eq!(
        failure.original_failure().unwrap().failure_fact(),
        original_fact
    );
    assert_eq!(
        std::error::Error::source(&failure)
            .and_then(|source| source.downcast_ref::<SharedFailureCause>()),
        Some(&original),
    );
    assert_eq!(failure.clone(), failure);
    assert_eq!(caps.async_calls.load(SeqCst), 1);
    assert_eq!(caps.calls.load(SeqCst), 0);
    assert!(state.liveness().is_open());
    assert_eq!(
        state.begin_current_observation(id).err().unwrap(),
        ManagementObservationError::ReusedObservationRequest,
    );
    // A plain Provider error is retained as that actual error, not a message.
    let id = ManagementObservationRequestId::from_bytes([105; 16]);
    let pending = state.begin_current_observation(id).unwrap();
    let (lease, request, _) = keyed_management_lease_with_caps(
        &target,
        Err(
            ConnectorError::new(ConnectorErrorKind::Unavailable, "original provider offline")
                .into(),
        ),
    );
    let failure = state
        .complete_keyed_current_observation_async(pending, &lease, request)
        .await
        .unwrap_err();
    assert_eq!(
        failure
            .original_failure()
            .unwrap()
            .downcast_ref::<ConnectorError>()
            .unwrap()
            .kind(),
        ConnectorErrorKind::Unavailable,
    );
}

#[tokio::test(flavor = "current_thread")]
async fn async_keyed_replacement_closes_the_original_liveness_and_retains_the_source() {
    use novarocks_spi::connector::{ConnectorError, ConnectorTableObjectBindingFailure};
    use std::sync::atomic::Ordering::SeqCst;
    let target = target("mv", b"async-object");
    let mut state = ready_observation_state(target.clone(), "deployment-a", "inc-a");
    let original_liveness = state.liveness().clone();
    let id = ManagementObservationRequestId::from_bytes([106; 16]);
    let pending = state.begin_current_observation(id).unwrap();
    let (lease, request, caps) = keyed_management_lease_with_caps(
        &target,
        Err(ConnectorError::table_object_binding(
            ConnectorTableObjectBindingFailure::Replaced,
            "original exact target was replaced",
        )
        .into()),
    );
    let failure = state
        .complete_keyed_current_observation_async(pending, &lease, request)
        .await
        .unwrap_err();
    assert_eq!(
        failure.kind(),
        &ManagementObservationError::Ownership(ManagementOwnershipError::TargetReplaced)
    );
    assert_eq!(
        state.phase(),
        ManagementObservationPhase::ClosedObjectReplacement
    );
    assert!(!original_liveness.is_open());
    assert_eq!(caps.async_calls.load(SeqCst), 1);
    assert_eq!(caps.async_exits.load(SeqCst), 1);
    assert_eq!(
        failure
            .original_failure()
            .unwrap()
            .downcast_ref::<ConnectorError>()
            .unwrap()
            .table_object_binding_failure(),
        Some(ConnectorTableObjectBindingFailure::Replaced),
    );
    assert_eq!(
        state.begin_current_observation(id).err().unwrap(),
        ManagementObservationError::ObservationNotAllowed,
    );
}

#[tokio::test(flavor = "current_thread")]
async fn async_keyed_request_and_provenance_refusals_cannot_install_foreign_management() {
    use std::sync::atomic::Ordering::SeqCst;
    let target = target("mv", b"async-object");
    let mut state = ready_observation_state(target.clone(), "deployment-a", "inc-a");
    let id = ManagementObservationRequestId::from_bytes([107; 16]);
    let pending = state.begin_current_observation(id).unwrap();
    let mut foreign = keyed_management_view(&target, "deployment-b", "inc-b", 2);
    foreign.slots.clear();
    let (lease, mut request, caps) = keyed_management_lease_with_caps(&target, Ok(foreign));
    let original_request = request.clone();
    request.object_id = object(b"another-object");
    let failure = state
        .complete_keyed_current_observation_async(pending, &lease, request)
        .await
        .unwrap_err();
    assert_eq!(
        failure.kind(),
        &ManagementObservationError::EffectTargetMismatch
    );
    assert!(failure.original_failure().is_none());
    assert_eq!(
        caps.async_calls.load(SeqCst),
        0,
        "request is checked before foreign I/O"
    );
    assert_eq!(state.phase(), ManagementObservationPhase::Ready);
    assert!(state.liveness().is_open());
    assert_eq!(
        state.begin_current_observation(id).err().unwrap(),
        ManagementObservationError::ReusedObservationRequest
    );
    let id = ManagementObservationRequestId::from_bytes([108; 16]);
    let pending = state.begin_current_observation(id).unwrap();
    let failure = state
        .complete_keyed_current_observation_async(pending, &lease, original_request.clone())
        .await
        .unwrap_err();
    assert_eq!(
        failure.kind(),
        &ManagementObservationError::InvalidProvenance
    );
    assert!(failure.original_failure().is_none());
    assert_eq!(caps.async_calls.load(SeqCst), 1);
    assert_eq!(state.phase(), ManagementObservationPhase::Ready);
    assert!(
        state.liveness().is_open(),
        "unproven foreign marker cannot revoke the target"
    );
    assert_eq!(caps.calls.load(SeqCst), 0);
    let mut escaped = keyed_management_view(&target, "deployment-a", "inc-a", 3);
    escaped.object_id = object(b"another-provider-object");
    *caps.reply.lock().unwrap() = Ok(escaped);
    let pending = state
        .begin_current_observation(ManagementObservationRequestId::from_bytes([111; 16]))
        .unwrap();
    let failure = state
        .complete_keyed_current_observation_async(pending, &lease, original_request)
        .await
        .unwrap_err();
    assert_eq!(
        failure.kind(),
        &ManagementObservationError::UnsealedObservation
    );
    assert_eq!(
        failure
            .original_failure()
            .unwrap()
            .downcast_ref::<novarocks_spi::connector::ConnectorError>()
            .unwrap()
            .kind(),
        novarocks_spi::connector::ConnectorErrorKind::InvalidRequest,
    );
    assert_eq!(caps.async_calls.load(SeqCst), 2);
    assert_eq!(state.phase(), ManagementObservationPhase::Ready);
    assert!(state.liveness().is_open());
}

#[tokio::test(flavor = "current_thread")]
async fn async_keyed_legacy_refusal_retains_the_actual_codec_source_and_fact() {
    use novarocks_failure_contract::{FailureClass, FailureProjection, SharedFailureCause, domain};
    use novarocks_spi::connector::KeyedDocumentReadError;
    let target = target("mv", b"async-object");
    let mut state = ready_observation_state(target.clone(), "deployment-a", "inc-a");
    let pending = state
        .begin_current_observation(ManagementObservationRequestId::from_bytes([109; 16]))
        .unwrap();
    let legacy = KeyedDocumentReadError::LegacyFormat {
        key: "original-legacy-key".into(),
    };
    let (lease, request, _) = keyed_management_lease_with_caps(&target, Err(legacy.clone()));
    let failure = state
        .complete_keyed_current_observation_async(pending, &lease, request)
        .await
        .unwrap_err();
    assert_eq!(
        failure.kind(),
        &ManagementObservationError::UnsealedObservation
    );
    let original = failure.original_failure().expect("actual codec refusal");
    assert_eq!(
        original.downcast_ref::<KeyedDocumentReadError>(),
        Some(&legacy)
    );
    assert_eq!(original.failure_fact(), legacy.failure_fact());
    assert_eq!(original.failure_fact().class(), FailureClass::Unsupported);
    assert_eq!(
        original.failure_fact().reason().domain(),
        domain::METADATA.get()
    );
    assert_eq!(original.failure_fact().reason().code(), 916);
    assert_eq!(
        std::error::Error::source(&failure)
            .and_then(|source| source.downcast_ref::<SharedFailureCause>()),
        Some(original),
    );
    assert_eq!(failure.clone().original_failure(), Some(original));
    assert!(failure.to_string().contains("original-legacy-key"));
    assert_eq!(failure.clone(), failure);
    assert!(state.liveness().is_open());
}

#[tokio::test]
async fn async_keyed_drop_preserves_the_original_pending_observation_and_exact_guard() {
    use std::future::Future;
    use std::sync::atomic::Ordering::SeqCst;
    let (entrance, target, _, lease, request, caps) = installed_keyed_drop_fixture();
    let (release, gate) = tokio::sync::oneshot::channel();
    *caps.async_gate.lock().unwrap() = Some(gate);
    let mut original =
        Box::pin(entrance.observe_keyed_drop_async(&lease, request.clone(), Default::default()));
    let mut borrowed = Box::pin(std::future::poll_fn(|cx| original.as_mut().poll(cx)));
    assert!(futures::poll!(borrowed.as_mut()).is_pending());
    assert_eq!(caps.async_calls.load(SeqCst), 1);
    assert_eq!(caps.async_exits.load(SeqCst), 0);
    drop(borrowed);
    assert_eq!(caps.async_exits.load(SeqCst), 0);
    release.send(()).unwrap();
    let (first, _) = original.await.unwrap();
    assert_eq!(first.target(), &target);
    assert_eq!(caps.async_calls.load(SeqCst), 1);
    assert_eq!(caps.async_exits.load(SeqCst), 1);
    let management = entrance
        .acquire_async(first.request(), std::future::pending::<()>())
        .await
        .unwrap();
    let (second, _) = entrance
        .observe_keyed_drop_async(&lease, request, Default::default())
        .await
        .unwrap();
    assert!(first.matches(&second));
    drop(management);
}
