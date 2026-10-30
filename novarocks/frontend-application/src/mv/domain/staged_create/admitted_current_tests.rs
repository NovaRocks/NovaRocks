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

//! Cross-layer Current-source regression tests using native retained leases.

use super::{ManagedTargetCurrentSource, MvConvergence, keyed_request};
use bytes::Bytes;
use novarocks_mv_application::management::{
    CreateIntent, DeploymentOwner, EffectDisposition, EffectIdentity, EffectResponsibility,
    EffectScope, ManagedMvTarget, ManagementContinuation, ManagementEntrance,
    ManagementObservationPhase, ManagementObservationRequestId, ManagementRequest,
    ManagementTimestamp, MvManagementPhase,
};
use novarocks_mv_application::persistence::keyed_documents::{
    MvKeyedObservedCurrentDocuments, decode_current_keyed_documents, keyed_view_fixture,
    management_slot, management_slot_name,
};
use novarocks_mv_application::persistence::test_support::{
    KeyedProjectionFixture, observed_current_keyed,
};
use novarocks_mv_application::product::MvTarget;
use novarocks_mv_application::readiness::{
    MvCurrentProjectionRequest, MvKeyedCurrentProjectionSource, MvProjectionError,
    MvProjectionErrorKind,
};
use novarocks_spi::connector::*;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

type CurrentResult = Result<
    (
        MvKeyedObservedCurrentDocuments,
        novarocks_mv_application::management::MvCurrentManagementAdmission,
    ),
    MvProjectionError,
>;

fn unused() -> ConnectorError {
    ConnectorError::new(
        ConnectorErrorKind::Unsupported,
        "unused admitted Current test capability",
    )
}

struct TargetCaps {
    descriptor: ConnectorInstanceDescriptor,
    epoch: ProviderBindingEpoch,
    current: Mutex<KeyedDocumentView>,
    captured_object: Mutex<ConnectorTableObjectId>,
    conclusions: Mutex<VecDeque<CommitConclusion>>,
    events: Mutex<Vec<&'static str>>,
    async_capture_gate: Mutex<Option<tokio::sync::oneshot::Receiver<()>>>,
    async_refusal: Mutex<Option<ConnectorError>>,
    cpu_tail: Mutex<Option<Arc<CaptureCpuTail>>>,
}

impl TargetCaps {
    fn assert_scope(&self, request: &KeyedDocumentObservationRequest) {
        let view = self.current.lock().unwrap();
        assert_eq!(request.owner, view.owner);
        assert_eq!(request.target, view.table);
        assert_eq!(request.object_id, view.object_id);
        assert_eq!(
            request.catalog_handle,
            CatalogHandle::new(
                view.table.instance_id.clone(),
                CatalogVersion::from_bytes([1; 32])
            )
        );
    }

    fn advance_to_committed_slot(&self) {
        let mut view = self.current.lock().unwrap();
        view.current_snapshot = Some(18);
        view.metadata_version =
            ConnectorCommittedVersion::try_new(Bytes::from_static(b"committed-slot-18"), Some(18))
                .unwrap();
        view.slots
            .get_mut(&configuration_slot_name())
            .unwrap()
            .writer = Some(18);
    }
}

fn configuration_slot_name() -> ConnectorSlotName {
    novarocks_mv_application::persistence::keyed_documents::configuration_slot_name().unwrap()
}

struct CaptureCpuTail {
    entered: Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
    released: Mutex<bool>,
    changed: std::sync::Condvar,
    calls: std::sync::atomic::AtomicUsize,
    source_drops: std::sync::atomic::AtomicUsize,
}
impl CaptureCpuTail {
    fn block(&self) {
        self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if let Some(entered) = self.entered.lock().unwrap().take() {
            let _ = entered.send(());
        }
        let mut open = self.released.lock().unwrap();
        while !*open {
            open = self.changed.wait(open).unwrap();
        }
    }
    fn release(&self) {
        *self.released.lock().unwrap() = true;
        self.changed.notify_all();
    }
}
struct CaptureTailRescue(Arc<CaptureCpuTail>);
impl Drop for CaptureTailRescue {
    fn drop(&mut self) {
        self.0.release();
    }
}

struct PureCurrentCpuSource {
    descriptor: ConnectorInstanceDescriptor,
    epoch: ProviderBindingEpoch,
    binding: Mutex<Option<ConnectorTableObjectBinding>>,
    result: Mutex<Option<ConnectorTableObjectBinding>>,
    tail: Option<Arc<CaptureCpuTail>>,
}
impl ProviderMetadataCpuSource for PureCurrentCpuSource {
    fn descriptor(&self) -> &ConnectorInstanceDescriptor {
        &self.descriptor
    }
    fn incarnation(&self) -> ProviderBindingEpoch {
        self.epoch
    }
    fn run_metadata(&self) {
        let binding = self
            .binding
            .lock()
            .unwrap()
            .take()
            .expect("one original pure metadata input");
        *self.result.lock().unwrap() = Some(binding);
        if let Some(tail) = &self.tail {
            tail.block();
        }
    }
}
impl Drop for PureCurrentCpuSource {
    fn drop(&mut self) {
        if let Some(tail) = &self.tail {
            tail.source_drops
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
    }
}

impl ConnectorMetadata for TargetCaps {
    fn provider_metadata_cpu_author(
        &self,
    ) -> Option<(ConnectorProviderBindingKey, std::any::TypeId)> {
        Some((
            ConnectorProviderBindingKey {
                instance_id: self.descriptor.instance_id.clone(),
                incarnation: self.epoch,
            },
            std::any::TypeId::of::<PureCurrentCpuSource>(),
        ))
    }
    fn capture_table_object_with_snapshot_cpu_in<'a>(
        &'a self,
        request: ConnectorTableObjectCaptureRequest,
        cpu: &'a Arc<dyn ProviderMetadataCpuPort>,
        owner: &'a mut Option<ConnectorProcessLocalPayload>,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<(ConnectorTableObjectBinding, Option<i64>), ConnectorError>,
                > + Send
                + 'a,
        >,
    > {
        assert!(
            owner.is_none(),
            "fixture uses the actual original receiving"
        );
        let source = Arc::new(PureCurrentCpuSource {
            descriptor: self.descriptor.clone(),
            epoch: self.epoch,
            binding: Mutex::new(None),
            result: Mutex::new(None),
            tail: self.cpu_tail.lock().unwrap().clone(),
        });
        *owner = Some(ConnectorProcessLocalPayload::new(
            ConnectorProviderBindingKey {
                instance_id: self.descriptor.instance_id.clone(),
                incarnation: self.epoch,
            },
            source.clone(),
        ));
        Box::pin(async move {
            self.events.lock().unwrap().push("async-capture");
            let gate = self.async_capture_gate.lock().unwrap().take();
            if let Some(gate) = gate {
                gate.await.expect("original fixture capture gate");
            }
            // This explicit fixture is a pure fact producer. It is not an
            // Iceberg schema/loader placement or network qualification.
            *source.binding.lock().unwrap() = Some(self.capture_table_object_binding(request)?);
            let mut input = Some(ProviderMetadataCpuInput(source.clone()));
            cpu.submit_metadata_in(&mut input)?;
            let finished = std::future::poll_fn(|cx| cpu.poll_finished(cx)).await;
            let closed = std::future::poll_fn(|cx| cpu.poll_closed(cx)).await;
            finished?;
            closed?;
            let binding = source
                .result
                .lock()
                .unwrap()
                .take()
                .expect("actual pure metadata CPU result");
            Ok((binding, Some(17)))
        })
    }

    fn capture_table_object_binding_async(
        &self,
        request: ConnectorTableObjectCaptureRequest,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = Result<ConnectorTableObjectBinding, ConnectorError>>
                + Send
                + '_,
        >,
    > {
        Box::pin(async move {
            self.events.lock().unwrap().push("async-capture");
            let gate = self.async_capture_gate.lock().unwrap().take();
            if let Some(gate) = gate {
                gate.await.expect("original fixture capture gate");
            }
            // This capability is explicitly a pure in-memory fact producer.
            // Reuse its original exact schema author; no SDK I/O or fallback.
            self.capture_table_object_binding(request)
        })
    }

    fn instance_id(&self) -> &ConnectorInstanceId {
        &self.descriptor.instance_id
    }

    fn namespace_exists(&self, _: ConnectorNamespaceRequest) -> Result<bool, ConnectorError> {
        Err(unused())
    }

    fn table_exists(&self, _: ConnectorTableRequest) -> Result<bool, ConnectorError> {
        Err(unused())
    }

    fn list_tables(
        &self,
        _: ConnectorListTablesRequest,
    ) -> Result<Vec<ConnectorTableIdentity>, ConnectorError> {
        Err(unused())
    }

    fn load_table(
        &self,
        _: ConnectorTableRequest,
        _: &mut read_stack::ConnectorReadSchemaCreation<'_>,
    ) -> Result<
        ConnectorTableMetadata,
        novarocks_connector_contract::PureProviderCompileError<ConnectorError>,
    > {
        Err(unused().into())
    }

    fn capture_table_object_binding(
        &self,
        request: ConnectorTableObjectCaptureRequest,
    ) -> Result<ConnectorTableObjectBinding, ConnectorError> {
        self.events.lock().unwrap().push("capture");
        assert_eq!(request.table, self.current.lock().unwrap().table);
        assert!(matches!(
            request.resolution,
            ConnectorTableResolution::StrictBaseTable
        ));
        assert!(matches!(
            request.selector,
            ConnectorTableObjectSelector::Current
        ));
        let header = ConnectorTableMetadataHeader {
            identity: request.table,
            planning_facts: Default::default(),
            definition_facts: Default::default(),
            version: None,
            statistics_data_version: None,
            table: ConnectorTableHandle::try_new(
                self.descriptor.instance_id.clone(),
                Bytes::from_static(b"captured-current-target"),
            )?,
        };
        // This source consumes identity only; the fixture explicitly declares
        // a zero-column provider table through the real schema creation path.
        let metadata =
            crate::connector::schema_creation::with_schema_creation(&request.context, |creation| {
                crate::connector::schema_creation::declared_test_metadata_with_creation(
                    header,
                    Arc::new(arrow::datatypes::Schema::empty()),
                    vec![],
                    creation,
                )
            })
            .map_err(crate::connector::schema_creation::connector_schema_error)?;
        Ok(ConnectorTableObjectBinding {
            metadata,
            object_id: self.captured_object.lock().unwrap().clone(),
        })
    }
}

impl ConnectorDocumentStorageObservation for TargetCaps {
    fn observe_keyed_documents_async(
        &self,
        request: KeyedDocumentObservationRequest,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = Result<KeyedDocumentView, KeyedDocumentReadError>>
                + Send
                + '_,
        >,
    > {
        Box::pin(async move {
            self.events.lock().unwrap().push("async-observe");
            let refusal = self.async_refusal.lock().unwrap().clone();
            if let Some(refusal) = refusal {
                return Err(refusal.into());
            }
            // Same explicit pure fixture and original lease sealing boundary.
            self.observe_keyed_documents(request)
        })
    }

    fn descriptor(&self) -> &ConnectorInstanceDescriptor {
        &self.descriptor
    }

    fn incarnation(&self) -> ProviderBindingEpoch {
        self.epoch
    }

    fn observe_keyed_documents(
        &self,
        request: KeyedDocumentObservationRequest,
    ) -> Result<KeyedDocumentView, KeyedDocumentReadError> {
        self.assert_scope(&request);
        self.events.lock().unwrap().push("observe");
        // Only ConnectorDocumentStorageLease validates this reply and grants
        // the observation authority used by the MV state machine.
        Ok(self.current.lock().unwrap().clone())
    }

    fn observe_full_keyed_documents(
        &self,
        _: KeyedDocumentObservationRequest,
    ) -> Result<FullKeyedDocumentView, KeyedDocumentReadError> {
        Err(unused().into())
    }

    fn decode_keyed_slot_change_evidence(
        &self,
        _: KeyedDocumentObservationRequest,
        _: ExternalMutationEvidence,
    ) -> Result<UnknownCommitEvidence, ConnectorError> {
        Err(unused())
    }

    fn resolve_keyed_commit(
        &self,
        request: KeyedDocumentObservationRequest,
        evidence: UnknownCommitEvidence,
    ) -> Result<CommitConclusion, ConnectorError> {
        self.assert_scope(&request);
        assert_eq!(evidence, slot_evidence());
        self.events.lock().unwrap().push("resolve");
        let conclusion = self
            .conclusions
            .lock()
            .unwrap()
            .pop_front()
            .expect("only the predeclared slot resolution calls are allowed");
        if matches!(conclusion, CommitConclusion::Committed(_)) {
            self.advance_to_committed_slot();
        }
        Ok(conclusion)
    }

    fn discover_documents(
        &self,
        _: ConnectorDocumentDiscoveryRequest,
    ) -> Result<ConnectorDocumentDiscoveryPage, ConnectorError> {
        Err(unused())
    }
}

struct Resolver {
    binding: Arc<ConnectorControlBinding>,
}

impl Resolver {
    fn lease(&self) -> ConnectorControlPlanningLease {
        ConnectorControlPlanningLease::new(Arc::clone(&self.binding), || {})
    }
}

impl ConnectorControlResolver for Resolver {
    fn observe_current_binding(
        &self,
        instance: &ConnectorInstanceId,
    ) -> Result<ConnectorProviderBindingKey, ConnectorError> {
        assert_eq!(instance, &self.binding.descriptor().instance_id);
        Ok(ConnectorProviderBindingKey {
            instance_id: self.binding.descriptor().instance_id.clone(),
            incarnation: self.binding.incarnation(),
        })
    }

    fn observe_current_control_runtime(
        &self,
        instance: &ConnectorInstanceId,
    ) -> Result<ConnectorControlRuntimeId, ConnectorError> {
        assert_eq!(instance, &self.binding.descriptor().instance_id);
        Ok(self.binding.control_runtime_id())
    }

    fn acquire_current(
        &self,
        instance: &ConnectorInstanceId,
    ) -> Result<ConnectorControlPlanningLease, ConnectorError> {
        assert_eq!(instance, &self.binding.descriptor().instance_id);
        Ok(self.lease())
    }
}

struct Fixture {
    entrance: Arc<ManagementEntrance>,
    resolver: Resolver,
    caps: Arc<TargetCaps>,
    catalog: CatalogHandle,
    target: MvTarget,
    managed: ManagedMvTarget,
}

impl Fixture {
    fn new(conclusions: Vec<CommitConclusion>) -> Self {
        let (catalog, _) = keyed_view_fixture(Some(17), true);
        let target = MvTarget::from_parts(Some("ice"), "sales", "mv");
        let facts = KeyedProjectionFixture::new(target.clone(), Some(17))
            .build()
            .unwrap();
        let observed = observed_current_keyed(&facts, catalog.clone());
        let descriptor = ConnectorInstanceDescriptor {
            provider_id: ConnectorProviderId::parse("iceberg").unwrap(),
            instance_id: observed.view.table.instance_id.clone(),
        };
        let epoch = observed.view.owner.incarnation;
        let caps = Arc::new(TargetCaps {
            descriptor: descriptor.clone(),
            epoch,
            captured_object: Mutex::new(observed.view.object_id.clone()),
            current: Mutex::new(observed.view),
            conclusions: Mutex::new(conclusions.into()),
            events: Mutex::new(vec![]),
            async_capture_gate: Mutex::new(None),
            async_refusal: Mutex::new(None),
            cpu_tail: Mutex::new(None),
        });
        let template = novarocks_catalog_application::test_support::test_control_binding_for(
            descriptor.instance_id.clone(),
            3,
        );
        let documents = ConnectorDocumentStorageBinding::try_new(
            descriptor.clone(),
            epoch,
            Some(caps.clone()),
            None,
        )
        .unwrap();
        let binding = ConnectorControlBinding::try_new(
            descriptor,
            epoch,
            caps.clone(),
            Arc::clone(template.planning()),
            Arc::clone(template.execution_distribution()),
            None,
        )
        .unwrap()
        .with_catalog_properties(
            CatalogProperties::new(
                catalog.clone(),
                ConnectorProviderId::parse("iceberg").unwrap(),
                1,
                vec![],
                vec![],
            )
            .unwrap(),
        )
        .and_then(|binding| binding.try_with_document_storage(Some(documents)))
        .unwrap();
        let resolver = Resolver {
            binding: Arc::new(binding),
        };
        let lease = resolver.lease().derive_document_storage_lease().unwrap();
        let view = caps.current.lock().unwrap().clone();
        let current =
            decode_current_keyed_documents(view, catalog.clone(), Default::default()).unwrap();
        let entrance = ManagementEntrance::new(
            current.deployment_owner.clone(),
            current.process_incarnation.clone(),
        );
        let managed = current.management_target.clone();
        // Seed only fixture CREATE responsibility facts, not a native table
        // creation receipt. Admission itself still requires the real lease's
        // exact Current observation; no Fresh value or seal is fabricated.
        let intent = CreateIntent::try_new(
            catalog.clone(),
            managed.table().clone(),
            EffectIdentity::from_bytes([40; 16]),
        )
        .unwrap();
        let mut creation = entrance
            .acquire(
                ManagementRequest::for_create_intent(intent, EffectScope::CATALOG_COMMIT),
                || false,
            )
            .unwrap();
        creation
            .mark_create_intent_dispatched(ManagementTimestamp::from_unix_millis(900))
            .unwrap();
        creation.late_bind_create_target(managed.clone()).unwrap();
        creation
            .record_terminal(EffectDisposition::KnownCommitted)
            .unwrap();
        let mut state = entrance
            .begin_committed_convergence(
                managed.table(),
                ManagementContinuation::SameOwner {
                    previous_incarnation: entrance.incarnation().clone(),
                },
            )
            .unwrap();
        let pending = state
            .begin_current_observation(ManagementObservationRequestId::from_bytes([41; 16]))
            .unwrap();
        let (phase, view) = state
            .complete_keyed_current_observation(
                pending,
                &lease,
                keyed_request(
                    &lease,
                    managed.table().clone(),
                    managed.object_id().clone(),
                    context(),
                ),
            )
            .unwrap();
        assert_eq!(phase, ManagementObservationPhase::Ready);
        let observed =
            decode_current_keyed_documents(view, catalog.clone(), Default::default()).unwrap();
        entrance
            .install_observed_target(
                &state,
                observed.management_dependencies(resolver.binding.control_runtime_id()),
            )
            .unwrap();
        caps.events.lock().unwrap().clear();
        Self {
            entrance: Arc::new(entrance),
            resolver,
            caps,
            catalog,
            target,
            managed,
        }
    }

    fn request(&self) -> MvCurrentProjectionRequest {
        MvCurrentProjectionRequest::try_new(
            self.catalog.clone(),
            self.target.clone(),
            context(),
            Default::default(),
        )
        .unwrap()
    }

    async fn observe(&self, convergence: MvConvergence) -> CurrentResult {
        self.observe_request(convergence, self.request()).await
    }

    async fn observe_request(
        &self,
        convergence: MvConvergence,
        request: MvCurrentProjectionRequest,
    ) -> CurrentResult {
        ManagedTargetCurrentSource {
            entrance: &self.entrance,
            connector_control: &self.resolver,
            published: None,
            convergence,
        }
        .observe_current_keyed(request)
        .await
    }

    fn management_request(&self) -> ManagementRequest {
        let current = decode_current_keyed_documents(
            self.caps.current.lock().unwrap().clone(),
            self.catalog.clone(),
            Default::default(),
        )
        .unwrap();
        ManagementRequest::try_new(
            self.catalog.clone(),
            self.managed.table().clone(),
            Some(self.managed.object_id().clone()),
            ConnectorDocumentManagementOperation::SingleTargetUpdate,
            Some(current.management_dependencies(self.resolver.binding.control_runtime_id())),
            EffectScope::CATALOG_COMMIT,
        )
        .unwrap()
    }

    fn dispatched(&self) -> novarocks_mv_application::management::ManagementEntranceLease {
        let mut lease = self
            .entrance
            .acquire(self.management_request(), || false)
            .unwrap();
        lease
            .mark_dispatched(EffectResponsibility::new(
                EffectIdentity::from_bytes([51; 16]),
                self.managed.clone(),
                self.entrance.incarnation().clone(),
                EffectScope::CATALOG_COMMIT,
                ManagementTimestamp::from_unix_millis(1000),
            ))
            .unwrap();
        lease
    }

    fn assert_manageable(&self) {
        assert_eq!(
            self.entrance.management_phase(self.managed.table()),
            MvManagementPhase::Manageable
        );
        assert!(
            self.entrance
                .unsettled_effects(self.managed.table())
                .is_empty()
        );
    }
}

fn context() -> ConnectorRequestContext {
    crate::connector::test_request_context()
}

fn slot_evidence() -> UnknownCommitEvidence {
    UnknownCommitEvidence {
        asserted_parent: Some(17),
        preselected_snapshot: 18,
        identity: UnknownCommitIdentity::SlotWriter {
            slot: configuration_slot_name(),
        },
    }
}

#[tokio::test]
async fn healthy_admitted_current_does_not_require_a_local_committed_effect() {
    let fixture = Fixture::new(vec![]);
    fixture.assert_manageable();
    let error = fixture
        .observe(MvConvergence::CommittedEffect)
        .await
        .err()
        .unwrap();
    assert_eq!(error.kind(), MvProjectionErrorKind::SourceConflict);
    assert!(error.to_string().contains("ObservationNotAllowed"));
    assert_eq!(*fixture.caps.events.lock().unwrap(), vec!["capture"]);
    fixture.caps.events.lock().unwrap().clear();

    let (observed, _admission) = fixture
        .observe(MvConvergence::AdmittedCurrent)
        .await
        .unwrap();
    assert_eq!(observed.management_target, fixture.managed);
    assert_eq!(observed.view.current_snapshot, Some(17));
    assert_eq!(
        *fixture.caps.events.lock().unwrap(),
        vec!["capture", "observe"]
    );
    fixture.assert_manageable();
    drop(
        fixture
            .entrance
            .acquire(fixture.management_request(), || false)
            .unwrap(),
    );
}

async fn resolved_slot_current(still_unknown_first: bool) {
    let committed = CommitConclusion::Committed(CommitProof::SlotWriter);
    let conclusions = if still_unknown_first {
        vec![CommitConclusion::StillUnknown, committed.clone()]
    } else {
        vec![committed.clone()]
    };
    let fixture = Fixture::new(conclusions);
    fixture
        .dispatched()
        .record_slot_change_unknown(slot_evidence())
        .unwrap();
    assert_eq!(
        fixture
            .entrance
            .pending_slot_changes(fixture.managed.table()),
        1
    );
    fixture.assert_manageable();
    if still_unknown_first {
        let documents = fixture
            .resolver
            .lease()
            .derive_document_storage_lease()
            .unwrap();
        let result = fixture
            .entrance
            .resolve_pending_slot_changes(
                &documents,
                keyed_request(
                    &documents,
                    fixture.managed.table().clone(),
                    fixture.managed.object_id().clone(),
                    context(),
                ),
            )
            .unwrap();
        assert!(!result.reinstall_current_projection);
        assert!(result.conclusions.is_empty());
        assert_eq!(
            fixture
                .entrance
                .pending_slot_changes(fixture.managed.table()),
            1
        );
        assert_eq!(
            fixture
                .entrance
                .latest_slot_change_conclusion(fixture.managed.table()),
            None
        );
        fixture.assert_manageable();
        assert_eq!(*fixture.caps.events.lock().unwrap(), vec!["resolve"]);
        fixture.caps.events.lock().unwrap().clear();
    }

    let (observed, _admission) = fixture
        .observe(MvConvergence::AdmittedCurrent)
        .await
        .unwrap();
    assert_eq!(observed.management_target, fixture.managed);
    assert_eq!(observed.view.current_snapshot, Some(18));
    assert_eq!(
        observed.view.metadata_version.payload().as_ref(),
        b"committed-slot-18"
    );
    assert_eq!(
        *fixture.caps.events.lock().unwrap(),
        vec!["capture", "observe", "resolve", "observe"]
    );
    assert_eq!(
        fixture
            .entrance
            .pending_slot_changes(fixture.managed.table()),
        0
    );
    assert_eq!(
        fixture
            .entrance
            .latest_slot_change_conclusion(fixture.managed.table()),
        Some(committed)
    );
    assert!(fixture.caps.conclusions.lock().unwrap().is_empty());
    fixture.assert_manageable();
    drop(
        fixture
            .entrance
            .acquire(fixture.management_request(), || false)
            .unwrap(),
    );
    assert!(
        fixture
            .observe(MvConvergence::CommittedEffect)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn admitted_current_consumes_a_committed_slot_without_creating_a_barrier() {
    resolved_slot_current(false).await;
}

#[tokio::test]
async fn admitted_current_retries_resolution_after_a_still_unknown_observation() {
    resolved_slot_current(true).await;
}

#[tokio::test]
async fn an_actual_local_committed_effect_still_uses_committed_convergence() {
    let fixture = Fixture::new(vec![]);
    let publication = fixture.dispatched();
    // A Provider publication verdict alone does not install the management
    // entrance's original committed responsibility. Its actual terminal must
    // be received before Current can consume that exact convergence cut.
    let premature = fixture
        .observe(MvConvergence::CommittedEffect)
        .await
        .expect_err("Current must not bypass the original publication terminal");
    assert!(premature.to_string().contains("ObservationNotAllowed"));
    assert_eq!(*fixture.caps.events.lock().unwrap(), vec!["capture"]);
    fixture.caps.events.lock().unwrap().clear();
    publication
        .record_terminal(EffectDisposition::KnownCommitted)
        .unwrap();
    assert_ne!(
        fixture.entrance.management_phase(fixture.managed.table()),
        MvManagementPhase::Manageable
    );
    fixture.caps.advance_to_committed_slot();
    let (observed, _admission) = fixture
        .observe(MvConvergence::CommittedEffect)
        .await
        .unwrap();
    assert_eq!(observed.view.current_snapshot, Some(18));
    assert_eq!(
        *fixture.caps.events.lock().unwrap(),
        vec!["capture", "observe"]
    );
    fixture.assert_manageable();
    drop(
        fixture
            .entrance
            .acquire(fixture.management_request(), || false)
            .unwrap(),
    );
}

#[tokio::test]
async fn admitted_current_does_not_issue_authority_for_a_foreign_owner() {
    for metadata_renewal in [false, true] {
        let fixture = Fixture::new(vec![]);
        let prior_request = fixture.management_request();
        let (prior_documents, _prior_admission) = fixture
            .observe(MvConvergence::AdmittedCurrent)
            .await
            .unwrap();
        let convergence = if metadata_renewal {
            MvConvergence::MetadataOnlyCurrent {
            expected: Arc::new(novarocks_mv_application::persistence::keyed_documents::MvKeyedDocumentProjection::try_from_current(prior_documents).unwrap()),
        }
        } else {
            MvConvergence::AdmittedCurrent
        };
        fixture.caps.events.lock().unwrap().clear();
        fixture
            .caps
            .current
            .lock()
            .unwrap()
            .slots
            .get_mut(&management_slot_name().unwrap())
            .unwrap()
            .value = management_slot(
            &DeploymentOwner::parse("foreign-deployment").unwrap(),
            fixture.entrance.incarnation(),
        )
        .unwrap();
        let error = fixture.observe(convergence).await.err().unwrap();
        assert_eq!(error.kind(), MvProjectionErrorKind::SourceConflict);
        assert!(
            error
                .to_string()
                .contains("Current management names another owner or incarnation")
        );
        assert_eq!(
            *fixture.caps.events.lock().unwrap(),
            vec!["capture", "observe"]
        );
        assert_eq!(
            fixture
                .entrance
                .pending_slot_changes(fixture.managed.table()),
            0
        );

        assert_eq!(
            fixture.entrance.management_phase(fixture.managed.table()),
            MvManagementPhase::OwnerMismatch
        );
        assert!(fixture.entrance.acquire(prior_request, || false).is_err());
    }
}

#[tokio::test]
async fn admitted_current_does_not_issue_authority_for_a_replaced_exact_object() {
    let fixture = Fixture::new(vec![]);
    let mut replacement = KeyedProjectionFixture::new(fixture.target.clone(), Some(17));
    replacement.object_id =
        ConnectorTableObjectId::try_new(Bytes::from_static(b"replacement-object")).unwrap();
    let replacement =
        observed_current_keyed(&replacement.build().unwrap(), fixture.catalog.clone()).view;
    *fixture.caps.captured_object.lock().unwrap() = replacement.object_id.clone();
    *fixture.caps.current.lock().unwrap() = replacement;
    let error = fixture
        .observe(MvConvergence::AdmittedCurrent)
        .await
        .err()
        .unwrap();
    assert_eq!(error.kind(), MvProjectionErrorKind::SourceConflict);
    assert!(error.to_string().contains("target was replaced"));
    assert_eq!(
        *fixture.caps.events.lock().unwrap(),
        vec!["capture", "observe"]
    );
    fixture.assert_manageable();
}

#[tokio::test]
async fn admitted_current_rejects_a_foreign_catalog_before_metadata_or_document_io() {
    let fixture = Fixture::new(vec![]);
    let request = MvCurrentProjectionRequest::try_new(
        CatalogHandle::new(
            fixture.catalog.catalog_name().clone(),
            CatalogVersion::from_bytes([9; 32]),
        ),
        fixture.target.clone(),
        context(),
        Default::default(),
    )
    .unwrap();
    let error = fixture
        .observe_request(MvConvergence::AdmittedCurrent, request)
        .await
        .err()
        .unwrap();
    assert_eq!(error.kind(), MvProjectionErrorKind::SourceConflict);
    assert!(error.to_string().contains("catalog generation changed"));
    assert!(fixture.caps.events.lock().unwrap().is_empty());
    fixture.assert_manageable();
}

// The existing pure StateStore provider implements original transactions.
// This guard cannot bind physical work or grant an executor position.
struct NoCurrentRepositoryPhysicalPort;
impl novarocks_state_store_api::StateStoreExecutionPort for NoCurrentRepositoryPhysicalPort {
    fn bind_original_in(
        &self,
        _: &Arc<dyn novarocks_state_store_api::StateStoreBlockingOriginal>,
        _: std::time::Instant,
        _: &mut Option<Box<dyn novarocks_state_store_api::StateStoreBlockingSlot>>,
    ) -> Result<(), novarocks_state_store_api::StateStoreExecutionFailure> {
        panic!("pure Current repository fixture must not dispatch physical work")
    }
    fn request_progress(&self) {}
}
struct OriginalCurrentRepositoryFixture {
    host: novarocks_state_store_runtime::StateStoreHost,
    port: Arc<dyn novarocks_state_store_api::StateStoreExecutionPort>,
    repository: Arc<novarocks_mv_application::state_store_repository::StateStoreMvRepository>,
}
impl OriginalCurrentRepositoryFixture {
    async fn new() -> Self {
        let host = crate::state_store::testing::open_persistent(format!(
            "metadata-only-current-{}",
            uuid::Uuid::now_v7(),
        ))
        .await;
        let repository =
            novarocks_mv_application::state_store_repository::StateStoreMvRepository::open(
                host.state_store().unwrap(),
                Default::default(),
            )
            .await
            .unwrap();
        Self {
            host,
            port: Arc::new(NoCurrentRepositoryPhysicalPort),
            repository,
        }
    }
    fn requests(
        &self,
        context: &ConnectorRequestContext,
    ) -> Arc<dyn novarocks_state_store_api::StateStoreExecutionRequests> {
        Arc::new(novarocks_state_store_api::StateStoreExecutionContext::new(
            &self.port,
            context.deadline(),
        ))
    }
    async fn close(mut self) {
        drop(self.repository);
        drop(self.port);
        self.host
            .shutdown(std::time::Instant::now() + std::time::Duration::from_secs(5))
            .await
            .unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn metadata_only_current_renewal_installs_fresh_generation_in_the_same_repository() {
    use novarocks_mv_application::process_runtime::ProcessRuntime;
    use novarocks_mv_application::readiness::MvKeyedReadinessService;
    let fixture = Fixture::new(vec![]);
    let repository = OriginalCurrentRepositoryFixture::new().await;
    let mut cpu = CurrentCpuFixture::new();
    let service = MvKeyedReadinessService::new(
        repository.repository.clone(),
        Arc::new(ProcessRuntime::default()),
    );
    let readiness = crate::mv::domain::readiness::MvReadinessPort::from_product(
        service.clone(),
        tokio::runtime::Handle::current(),
        fixture.entrance.clone(),
    );
    let view = readiness.in_requests(repository.requests(&context()));
    let initial_registration = super::CurrentProjectionRegistration::new();
    current_with_driver(
        &cpu.control,
        &mut cpu.driver,
        super::install_admitted_current_projection_async_in(
            &fixture.entrance,
            &view,
            &fixture.resolver,
            fixture.catalog.clone(),
            fixture.target.clone(),
            uuid::Uuid::now_v7(),
            context(),
            &cpu.factory,
            &initial_registration,
        ),
    )
    .await
    .unwrap();
    current_with_driver(
        &cpu.control,
        &mut cpu.driver,
        std::future::poll_fn(|cx| initial_registration.poll_registration_closed(cx)),
    )
    .await;
    drop(initial_registration);
    let before = service.load_ready(&fixture.target).await.unwrap().unwrap();
    let next = ConnectorCommittedVersion::try_new(
        Bytes::from_static(b"expired-history-same-current"),
        Some(17),
    )
    .unwrap();
    fixture.caps.current.lock().unwrap().metadata_version = next.clone();
    assert_ne!(before.projection.facts.metadata_version(), &next);
    let registration = super::CurrentProjectionRegistration::new();
    current_with_driver(
        &cpu.control,
        &mut cpu.driver,
        super::install_metadata_only_current_projection_async_in(
            &fixture.entrance,
            &view,
            &fixture.resolver,
            fixture.catalog.clone(),
            Arc::new(before.projection.facts.clone()),
            context(),
            &cpu.factory,
            &registration,
        ),
    )
    .await
    .unwrap();
    let after = service.load_ready(&fixture.target).await.unwrap().unwrap();
    assert_eq!(after.projection.mv_id, before.projection.mv_id);
    assert_eq!(after.projection.facts.metadata_version(), &next);
    assert_eq!(after.projection.facts.publication_snapshot(), Some(17));
    assert_eq!(
        after.projection.facts.definition(),
        before.projection.facts.definition()
    );
    assert_eq!(
        after.projection.facts.interpretation(),
        before.projection.facts.interpretation()
    );
    assert_eq!(
        after.projection.facts.versions(),
        before.projection.facts.versions()
    );
    assert_eq!(
        after.projection.facts.publication(),
        before.projection.facts.publication()
    );
    fixture.assert_manageable();
    drop(
        fixture
            .entrance
            .acquire(fixture.management_request(), || false)
            .unwrap(),
    );
    current_with_driver(
        &cpu.control,
        &mut cpu.driver,
        std::future::poll_fn(|cx| registration.poll_registration_closed(cx)),
    )
    .await;
    drop(registration);
    drop(view);
    drop((readiness, service));
    cpu.close().await;
    repository.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn metadata_only_current_renewal_refuses_a_changed_carrier_on_its_actual_read() {
    use novarocks_mv_application::process_runtime::ProcessRuntime;
    use novarocks_mv_application::readiness::MvKeyedReadinessService;
    let fixture = Fixture::new(vec![]);
    let repository = OriginalCurrentRepositoryFixture::new().await;
    let mut cpu = CurrentCpuFixture::new();
    let service = MvKeyedReadinessService::new(
        repository.repository.clone(),
        Arc::new(ProcessRuntime::default()),
    );
    let readiness = crate::mv::domain::readiness::MvReadinessPort::from_product(
        service.clone(),
        tokio::runtime::Handle::current(),
        fixture.entrance.clone(),
    );
    let view = readiness.in_requests(repository.requests(&context()));
    let initial_registration = super::CurrentProjectionRegistration::new();
    current_with_driver(
        &cpu.control,
        &mut cpu.driver,
        super::install_admitted_current_projection_async_in(
            &fixture.entrance,
            &view,
            &fixture.resolver,
            fixture.catalog.clone(),
            fixture.target.clone(),
            uuid::Uuid::now_v7(),
            context(),
            &cpu.factory,
            &initial_registration,
        ),
    )
    .await
    .unwrap();
    current_with_driver(
        &cpu.control,
        &mut cpu.driver,
        std::future::poll_fn(|cx| initial_registration.poll_registration_closed(cx)),
    )
    .await;
    drop(initial_registration);
    let before = service.load_ready(&fixture.target).await.unwrap().unwrap();
    fixture.caps.advance_to_committed_slot();
    let registration = super::CurrentProjectionRegistration::new();
    let error = current_with_driver(
        &cpu.control,
        &mut cpu.driver,
        super::install_metadata_only_current_projection_async_in(
            &fixture.entrance,
            &view,
            &fixture.resolver,
            fixture.catalog.clone(),
            Arc::new(before.projection.facts.clone()),
            context(),
            &cpu.factory,
            &registration,
        ),
    )
    .await
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("carrier changed during metadata-only Current renewal"),
        "{error}"
    );
    assert!(service.load_ready(&fixture.target).await.is_err());
    assert_eq!(
        *fixture.caps.events.lock().unwrap(),
        vec![
            "async-capture",
            "capture",
            "async-observe",
            "observe",
            "async-capture",
            "capture",
            "async-observe",
            "observe"
        ]
    );
    current_with_driver(
        &cpu.control,
        &mut cpu.driver,
        std::future::poll_fn(|cx| registration.poll_registration_closed(cx)),
    )
    .await;
    drop(registration);
    drop(view);
    drop((readiness, service));
    cpu.close().await;
    repository.close().await;
}

struct CurrentCpuFixture {
    owner: crate::query_execution::internal_result_cpu::InternalResultCpuOwner,
    control: novarocks_workload_control::WorkloadControl,
    driver: novarocks_workload_control::joint_admission::JointProgressDriver,
    root: novarocks_workload_control::RootWork,
    factory: super::CurrentProjectionMetadataCpu,
}
impl CurrentCpuFixture {
    fn new() -> Self {
        use crate::query_execution::internal_consumer::domain_cpu::root_action_tests::{
            bind, root_control,
        };
        let mut owner =
            crate::query_execution::internal_result_cpu::InternalResultCpuOwner::try_new_for_test()
                .unwrap();
        let (control, _, mut driver) = root_control(&mut owner);
        let root = control
            .try_begin_root(novarocks_workload_control::WorkRequest::new(
                novarocks_workload_control::WorkClass::Query,
            ))
            .unwrap();
        bind(&root.owner.scope(), &mut driver);
        let window =
            novarocks_workload_control::ResultWindowAlias::from_root_owner(&root.owner).unwrap();
        let cancel = novarocks_query_application::cancellation::QueryCancellationSource::new();
        let factory = super::CurrentProjectionMetadataCpu::business(
            owner.runtime(),
            root.owner.scope(),
            window,
            cancel.view(),
            tokio::runtime::Handle::current(),
            Arc::new(crate::task_execution::status_intake::TestIntakeWake::default()),
        );
        Self {
            owner,
            control,
            driver,
            root,
            factory,
        }
    }
    async fn close(self) {
        let Self {
            mut owner,
            control,
            driver,
            root,
            factory,
        } = self;
        drop(factory);
        root.owner.complete();
        root.business.release();
        drop(driver);
        drop(control);
        owner
            .shutdown_until(std::time::Instant::now() + std::time::Duration::from_secs(5))
            .await
            .unwrap();
    }
}
async fn current_with_driver<F: std::future::Future>(
    control: &novarocks_workload_control::WorkloadControl,
    driver: &mut novarocks_workload_control::joint_admission::JointProgressDriver,
    future: F,
) -> F::Output {
    // Same event-driven original driver pattern as domain_cpu::root_action_tests.
    tokio::pin!(future);
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            match driver.step() {
                novarocks_workload_control::joint_admission::JointProgress::More => {
                    tokio::task::yield_now().await
                }
                novarocks_workload_control::joint_admission::JointProgress::Idle(revision) => {
                    tokio::select! {
                        biased;
                        result=&mut future=>return result,
                        _=control.wait_progress(revision)=>{},
                    }
                }
            }
        }
    })
    .await
    .expect("original Current CPU/component watchdog")
}

#[tokio::test]
async fn original_current_source_borrows_the_same_pending_capture_and_exact_admission() {
    use super::{CurrentProjectionRegistration, OriginalManagedTargetCurrentSource};
    use std::future::Future;
    let fixture = Fixture::new(vec![]);
    let mut cpu = CurrentCpuFixture::new();
    let registration = CurrentProjectionRegistration::new();
    let (release, gate) = tokio::sync::oneshot::channel();
    *fixture.caps.async_capture_gate.lock().unwrap() = Some(gate);
    let source = OriginalManagedTargetCurrentSource {
        entrance: &fixture.entrance,
        connector_control: &fixture.resolver,
        published: None,
        convergence: MvConvergence::AdmittedCurrent,
        metadata_cpu: &cpu.factory,
        registration: &registration,
    };
    let mut original = Box::pin(source.observe_current_keyed(fixture.request()));
    let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
    {
        let mut borrowed = std::pin::pin!(std::future::poll_fn(|cx| original.as_mut().poll(cx)));
        assert!(borrowed.as_mut().poll(&mut cx).is_pending());
    }
    assert_eq!(*fixture.caps.events.lock().unwrap(), vec!["async-capture"]);
    assert!(registration.observation().operation_id().is_none());
    // No registration was dispatched, but its original capture child is
    // still live. The combined receiving must now remain Pending as well.
    assert!(registration.poll_registration_closed(&mut cx).is_pending());
    assert!(original.as_mut().poll(&mut cx).is_pending());
    assert_eq!(*fixture.caps.events.lock().unwrap(), vec!["async-capture"]);
    release.send(()).unwrap();
    let (observed, _original_admission) =
        current_with_driver(&cpu.control, &mut cpu.driver, original.as_mut())
            .await
            .unwrap();
    assert_eq!(observed.management_target, fixture.managed);
    assert_eq!(observed.view.current_snapshot, Some(17));
    assert_eq!(
        *fixture.caps.events.lock().unwrap(),
        vec!["async-capture", "capture", "async-observe", "observe"]
    );
    fixture.assert_manageable();
    // A new F cannot reinterpret this original receiving as a fresh attempt.
    let refused = source
        .observe_current_keyed(fixture.request())
        .await
        .err()
        .unwrap();
    assert!(refused.to_string().contains("already claimed"));
    assert_eq!(fixture.caps.events.lock().unwrap().len(), 4);
    drop(original);
    drop(source);
    drop(registration);
    cpu.close().await;
}

#[tokio::test]
async fn original_current_source_keeps_the_actual_keyed_provider_cause() {
    use super::{CurrentProjectionRegistration, OriginalManagedTargetCurrentSource};
    use novarocks_failure_contract::{FailureProjection, SharedFailureCause};
    let fixture = Fixture::new(vec![]);
    let mut cpu = CurrentCpuFixture::new();
    let refusal = ConnectorError::from_domain_static(
        ConnectorErrorKind::Interrupted,
        novarocks_type_contract::CompileControlError::ResourceExhausted,
        "original Current document refusal",
    );
    let original = std::error::Error::source(&refusal)
        .unwrap()
        .downcast_ref::<SharedFailureCause>()
        .unwrap()
        .clone();
    let fact = refusal.failure_fact();
    *fixture.caps.async_refusal.lock().unwrap() = Some(refusal);
    let registration = CurrentProjectionRegistration::new();
    let source = OriginalManagedTargetCurrentSource {
        entrance: &fixture.entrance,
        connector_control: &fixture.resolver,
        published: None,
        convergence: MvConvergence::AdmittedCurrent,
        metadata_cpu: &cpu.factory,
        registration: &registration,
    };
    let error = current_with_driver(
        &cpu.control,
        &mut cpu.driver,
        source.observe_current_keyed(fixture.request()),
    )
    .await
    .err()
    .unwrap();
    assert_eq!(error.original_failure(), Some(&original));
    assert_eq!(error.original_failure().unwrap().failure_fact(), fact);
    assert!(registration.observation().operation_id().is_none());
    assert!(registration.local_health_failure().is_none());
    fixture.assert_manageable();
    drop(source);
    drop(registration);
    cpu.close().await;
}

#[tokio::test(flavor = "current_thread")]
async fn original_current_capture_uses_each_selected_epoch_and_retains_the_actual_cpu_tail() {
    use std::sync::atomic::Ordering;
    let fixture = Fixture::new(vec![]);
    let mut cpu = CurrentCpuFixture::new();
    let registration = super::CurrentProjectionRegistration::new();
    let request = || ConnectorTableObjectCaptureRequest {
        table: fixture.caps.current.lock().unwrap().table.clone(),
        resolution: ConnectorTableResolution::StrictBaseTable,
        selector: ConnectorTableObjectSelector::Current,
        context: fixture.request().context().clone(),
    };
    let first = fixture.resolver.lease();
    current_with_driver(
        &cpu.control,
        &mut cpu.driver,
        registration
            .capture
            .capture(&first, &cpu.factory, request()),
    )
    .await
    .unwrap();
    // Same catalog instance, actually selected replacement generation. Reusing
    // the first generation's port would refuse this real private input.
    let epoch = ProviderBindingEpoch::from_bytes([93; 16]);
    assert_ne!(epoch, first.binding().incarnation());
    let caps = Arc::new(TargetCaps {
        descriptor: fixture.caps.descriptor.clone(),
        epoch,
        current: Mutex::new(fixture.caps.current.lock().unwrap().clone()),
        captured_object: Mutex::new(fixture.caps.captured_object.lock().unwrap().clone()),
        conclusions: Mutex::new(VecDeque::new()),
        events: Mutex::new(vec![]),
        async_capture_gate: Mutex::new(None),
        async_refusal: Mutex::new(None),
        cpu_tail: Mutex::new(None),
    });
    let template = novarocks_catalog_application::test_support::test_control_binding_for(
        caps.descriptor.instance_id.clone(),
        3,
    );
    let binding = ConnectorControlBinding::try_new(
        caps.descriptor.clone(),
        epoch,
        caps.clone(),
        Arc::clone(template.planning()),
        Arc::clone(template.execution_distribution()),
        None,
    )
    .unwrap();
    let selected = Resolver {
        binding: Arc::new(binding),
    }
    .lease();
    current_with_driver(
        &cpu.control,
        &mut cpu.driver,
        registration
            .capture
            .capture(&selected, &cpu.factory, request()),
    )
    .await
    .unwrap();
    assert_eq!(
        *caps.events.lock().unwrap(),
        vec!["async-capture", "capture"]
    );
    let (signal, entered) = tokio::sync::oneshot::channel();
    let tail = Arc::new(CaptureCpuTail {
        entered: Mutex::new(Some(signal)),
        released: Mutex::new(false),
        changed: std::sync::Condvar::new(),
        calls: std::sync::atomic::AtomicUsize::new(0),
        source_drops: std::sync::atomic::AtomicUsize::new(0),
    });
    let rescue = CaptureTailRescue(tail.clone());
    *fixture.caps.cpu_tail.lock().unwrap() = Some(tail.clone());
    let mut borrower = Box::pin(
        registration
            .capture
            .capture(&first, &cpu.factory, request()),
    );
    current_with_driver(&cpu.control,&mut cpu.driver,async {
        tokio::select! {
            result=&mut borrower=>panic!("metadata handed off before actual CPU tail: {}",result.is_ok()),
            _=entered=>{},
        }
    }).await;
    assert_eq!(tail.calls.load(Ordering::SeqCst), 1);
    assert_eq!(tail.source_drops.load(Ordering::SeqCst), 0);
    assert!(
        registration
            .poll_registration_closed(&mut std::task::Context::from_waker(std::task::Waker::noop()))
            .is_pending()
    );
    // Cancelling this borrowed helper does not cancel/drop the original F,
    // Source, accepted QB job or its actual worker/forwarder receipt.
    drop(borrower);
    tail.release();
    current_with_driver(
        &cpu.control,
        &mut cpu.driver,
        std::future::poll_fn(|cx| registration.poll_registration_closed(cx)),
    )
    .await;
    assert_eq!(tail.calls.load(Ordering::SeqCst), 1);
    assert_eq!(tail.source_drops.load(Ordering::SeqCst), 1);
    assert!(registration.local_health_failure().is_none());
    drop(registration);
    drop(rescue);
    cpu.close().await;
}

// An explicit pure SPI provider exercises the original child/receiving/footer,
// not an Iceberg commit or a Statement/Root qualification. It acknowledges a
// no-op and deliberately holds its same necessary-to-cleanup continuation.
struct OriginalRegistrationChildFixture {
    descriptor: ConnectorInstanceDescriptor,
    epoch: ProviderBindingEpoch,
    cleanup: Mutex<Option<tokio::sync::oneshot::Receiver<()>>>,
    cleanup_outcome: ExternalMutationFinalization,
    calls: std::sync::atomic::AtomicUsize,
    exits: Arc<std::sync::atomic::AtomicUsize>,
    owner: Arc<()>,
}
impl ConnectorCatalogMutation for OriginalRegistrationChildFixture {
    fn descriptor(&self) -> &ConnectorInstanceDescriptor {
        &self.descriptor
    }
    fn incarnation(&self) -> ProviderBindingEpoch {
        self.epoch
    }
    fn execute(
        &self,
        _: ConnectorCatalogMutationRequest,
    ) -> Result<ExternalMutationOutcome<ConnectorCatalogMutationReceipt>, ConnectorError> {
        panic!("original async fixture has no synchronous execution")
    }
    fn reconcile(
        &self,
        _: ConnectorCatalogMutationReconcileRequest,
    ) -> Result<ExternalMutationOutcome<ConnectorCatalogMutationReceipt>, ConnectorError> {
        panic!("original child must not reconcile or replay")
    }
    fn execute_async<'a>(
        &'a self,
        request: ConnectorCatalogMutationRequest,
        receiving: &'a ConnectorCatalogMutationReceiving,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<
                        ExternalMutationOutcome<ConnectorCatalogMutationReceipt>,
                        ConnectorError,
                    >,
                > + Send
                + 'a,
        >,
    > {
        struct ActualFixtureExit(Arc<std::sync::atomic::AtomicUsize>);
        impl Drop for ActualFixtureExit {
            fn drop(&mut self) {
                self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
        }
        self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Box::pin(async move {
            let _actual_exit = ActualFixtureExit(Arc::clone(&self.exits));
            receiving.retain_original_owner(ConnectorProcessLocalPayload::new(
                request.target.clone(),
                Arc::clone(&self.owner),
            ))?;
            let receipt = ConnectorCatalogMutationReceipt::try_new(
                self.descriptor.clone(),
                self.epoch,
                request.operation_id,
                request.operation.kind(),
                None,
            )?;
            receiving.record_needed_committed(
                ExternalMutationEffect::NoOp,
                &receipt,
                &ExternalMutationFinalization::Complete,
            )?;
            let gate = self
                .cleanup
                .lock()
                .unwrap()
                .take()
                .expect("one original cleanup turn");
            gate.await.expect("same original child cleanup gate");
            Ok(ExternalMutationOutcome::KnownCommitted {
                effect: ExternalMutationEffect::NoOp,
                receipt,
                finalization: ExternalMutationFinalization::Complete,
                cleanup: self.cleanup_outcome.clone(),
            })
        })
    }
}

#[tokio::test]
async fn original_registration_needed_does_not_drop_its_child_or_opaque_owner() {
    use super::CurrentProjectionRegistration;
    use std::sync::atomic::{AtomicUsize, Ordering};
    let fixture = Fixture::new(vec![]);
    let (finish, cleanup) = tokio::sync::oneshot::channel();
    let owner = Arc::new(());
    let owner_pin = Arc::downgrade(&owner);
    let exits = Arc::new(AtomicUsize::new(0));
    let provider = Arc::new(OriginalRegistrationChildFixture {
        descriptor: fixture.caps.descriptor.clone(),
        epoch: fixture.caps.epoch,
        cleanup: Mutex::new(Some(cleanup)),
        cleanup_outcome: ExternalMutationFinalization::Complete,
        calls: AtomicUsize::new(0),
        exits: Arc::clone(&exits),
        owner,
    });
    let lease = ConnectorCatalogMutationLease::new(
        provider.descriptor.clone(),
        fixture.resolver.binding.control_runtime_id(),
        provider.epoch,
        provider.clone(),
        || {},
    )
    .unwrap();
    let registration = CurrentProjectionRegistration::new();
    let receiving = Arc::clone(&registration.receiving);
    let operation_id = ConnectorMutationOperationId::new();
    let instance = provider.descriptor.instance_id.clone();
    let future = Box::pin(async move {
        lease
            .execute_operation_async_in(
                operation_id,
                ConnectorCatalogMutationOperation::DropNamespace {
                    namespace: ConnectorNamespaceIdentity {
                        instance_id: instance,
                        namespace: "unused".into(),
                    },
                    policy: DropPolicy::FailIfMissing,
                },
                context(),
                receiving.as_ref(),
            )
            .await
    });
    {
        let mut call = registration.call.lock().unwrap();
        call.call_installed = true;
        call.future = Some(future);
    }
    let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
    let (disposition, business) = match registration.poll_needed(&mut cx) {
        std::task::Poll::Ready(value) => value,
        std::task::Poll::Pending => panic!("the actual source already published Needed"),
    };
    assert_eq!(disposition, EffectDisposition::KnownCommitted);
    business.unwrap();
    {
        let observed = registration.observation();
        assert_eq!(observed.operation_id(), Some(operation_id));
        assert!(observed.applied_receipt().is_none());
        assert_eq!(
            observed.needed_committed().unwrap().effect(),
            ExternalMutationEffect::NoOp
        );
        assert!(observed.raw_outcome().is_none());
        assert!(!observed.provider_exited());
        assert!(observed.original_owner().is_some());
    }
    assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
    assert_eq!(exits.load(Ordering::SeqCst), 0);
    assert!(registration.poll_registration_closed(&mut cx).is_pending());
    finish.send(()).unwrap();
    std::future::poll_fn(|cx| registration.poll_registration_closed(cx)).await;
    assert!(registration.observation().provider_exited());
    assert!(
        registration
            .observation()
            .validated_outcome()
            .unwrap()
            .is_ok()
    );
    assert_eq!(exits.load(Ordering::SeqCst), 1);
    assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
    drop(provider);
    assert!(owner_pin.upgrade().is_some());
    drop(registration);
    assert!(owner_pin.upgrade().is_none());
}

#[tokio::test]
async fn original_registration_physical_exit_keeps_failed_cleanup_and_same_owner() {
    use super::CurrentProjectionRegistration;
    use std::sync::atomic::{AtomicUsize, Ordering};
    let fixture = Fixture::new(vec![]);
    let (finish, cleanup) = tokio::sync::oneshot::channel();
    let owner = Arc::new(());
    let owner_pin = Arc::downgrade(&owner);
    let exits = Arc::new(AtomicUsize::new(0));
    let cleanup_error = ConnectorError::new(
        ConnectorErrorKind::Unavailable,
        "original registration cleanup remains retained",
    );
    let provider = Arc::new(OriginalRegistrationChildFixture {
        descriptor: fixture.caps.descriptor.clone(),
        epoch: fixture.caps.epoch,
        cleanup: Mutex::new(Some(cleanup)),
        cleanup_outcome: ExternalMutationFinalization::Failed(
            ConnectorMutationFailure::from_connector(cleanup_error.clone()),
        ),
        calls: AtomicUsize::new(0),
        exits: Arc::clone(&exits),
        owner,
    });
    let lease = ConnectorCatalogMutationLease::new(
        provider.descriptor.clone(),
        fixture.resolver.binding.control_runtime_id(),
        provider.epoch,
        provider.clone(),
        || {},
    )
    .unwrap();
    let registration = CurrentProjectionRegistration::new();
    let receiving = Arc::clone(&registration.receiving);
    let operation_id = ConnectorMutationOperationId::new();
    let instance = provider.descriptor.instance_id.clone();
    let future = Box::pin(async move {
        lease
            .execute_operation_async_in(
                operation_id,
                ConnectorCatalogMutationOperation::DropNamespace {
                    namespace: ConnectorNamespaceIdentity {
                        instance_id: instance,
                        namespace: "unused".into(),
                    },
                    policy: DropPolicy::FailIfMissing,
                },
                context(),
                receiving.as_ref(),
            )
            .await
    });
    {
        let mut call = registration.call.lock().unwrap();
        call.call_installed = true;
        call.future = Some(future);
    }
    let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
    let (disposition, business) = match registration.poll_needed(&mut cx) {
        std::task::Poll::Ready(value) => value,
        std::task::Poll::Pending => panic!("actual source must publish Needed before its tail"),
    };
    assert_eq!(disposition, EffectDisposition::KnownCommitted);
    business.unwrap();
    assert!(
        registration
            .poll_registration_physical_closed(&mut cx)
            .is_pending()
    );
    assert!(registration.poll_registration_closed(&mut cx).is_pending());
    assert!(!registration.observation().provider_exited());
    assert_eq!(exits.load(Ordering::SeqCst), 0);

    finish.send(()).unwrap();
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        std::future::poll_fn(|cx| registration.poll_registration_physical_closed(cx)),
    )
    .await
    .expect("the original Provider child must physically exit without disposing retained cleanup");
    assert_eq!(exits.load(Ordering::SeqCst), 1);
    assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
    assert!(registration.observation().provider_exited());
    assert!(registration.local_health_failure().is_none());
    assert!(registration.poll_registration_closed(&mut cx).is_pending());
    assert!(
        registration
            .poll_registration_physical_closed(&mut cx)
            .is_ready()
    );
    {
        let observed = registration.observation();
        assert_eq!(observed.operation_id(), Some(operation_id));
        assert!(observed.applied_receipt().is_none());
        assert!(observed.original_owner().is_some());
        match observed.validated_outcome().unwrap().unwrap() {
            ExternalMutationOutcome::KnownCommitted {
                effect: ExternalMutationEffect::NoOp,
                finalization: ExternalMutationFinalization::Complete,
                cleanup: ExternalMutationFinalization::Failed(failure),
                ..
            } => assert_eq!(failure.connector_source(), Some(&cleanup_error)),
            other => panic!("physical exit changed the original outcome: {other:?}"),
        }
    }
    assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
    drop(provider);
    assert!(owner_pin.upgrade().is_some());
    // This fixture teardown is not a production retirement/disposal verdict.
    // The full-close oracle above must remain Pending while this owner exists.
    drop(registration);
    assert!(owner_pin.upgrade().is_none());
}

#[path = "admitted_current_tests/retained_root_tests.rs"]
mod retained_root_tests;
