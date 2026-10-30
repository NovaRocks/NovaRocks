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

//! Observe the real MV guard's provider boundary before any document lookup.
//! A query's sink alone cannot consume a vended table response: its collection
//! port must also carry this exact retained catalog's execution properties.
use super::{IcebergMvUserMutation, reject_if_iceberg_mv_table_with_planning_lease_and_context};
use crate::catalog_application::resolver::TargetBackend;
use novarocks_spi::connector::*;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

fn missing_table() -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::NotFound, "guard test table is absent")
}

struct RecordingMetadata {
    instance: ConnectorInstanceId,
    captured: Mutex<Option<ConnectorTableObjectCaptureRequest>>,
    result: Result<ConnectorTableObjectBinding, ConnectorError>,
}

impl ConnectorMetadata for RecordingMetadata {
    fn instance_id(&self) -> &ConnectorInstanceId {
        &self.instance
    }

    fn namespace_exists(&self, _: ConnectorNamespaceRequest) -> Result<bool, ConnectorError> {
        Err(missing_table())
    }

    fn table_exists(&self, _: ConnectorTableRequest) -> Result<bool, ConnectorError> {
        Err(missing_table())
    }

    fn list_tables(
        &self,
        _: ConnectorListTablesRequest,
    ) -> Result<Vec<ConnectorTableIdentity>, ConnectorError> {
        Err(missing_table())
    }

    fn load_table(
        &self,
        _: ConnectorTableRequest,
        _: &mut read_stack::ConnectorReadSchemaCreation<'_>,
    ) -> Result<
        ConnectorTableMetadata,
        novarocks_connector_contract::PureProviderCompileError<ConnectorError>,
    > {
        Err(missing_table().into())
    }

    fn capture_table_object_binding(
        &self,
        request: ConnectorTableObjectCaptureRequest,
    ) -> Result<ConnectorTableObjectBinding, ConnectorError> {
        *self.captured.lock().unwrap() = Some(request);
        // Refusal fixtures stop before document lookup. Positive fixtures
        // return their exact binding, then use the selected document capability.
        self.result.clone()
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
            *self.captured.lock().unwrap() = Some(request);
            self.result.clone()
        })
    }

    fn capture_table_object_with_snapshot_cpu(
        &self,
        request: ConnectorTableObjectCaptureRequest,
        _: Arc<dyn ProviderMetadataCpuPort>,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<(ConnectorTableObjectBinding, Option<i64>), ConnectorError>,
                > + Send
                + '_,
        >,
    > {
        Box::pin(async move {
            *self.captured.lock().unwrap() = Some(request);
            self.result.clone().map(|binding| (binding, None))
        })
    }
}

struct UnusedSink;

impl ConnectorVendedCredentialLeaseSink for UnusedSink {
    fn offer_vended_s3_credential_lease(
        &self,
        _: &CatalogProperties,
        _: VendedS3CredentialLeaseContribution,
    ) -> Result<(), ConnectorError> {
        panic!("the absent test table cannot contribute credentials")
    }
}

fn fixture(with_properties: bool) -> (ConnectorControlPlanningLease, Arc<RecordingMetadata>) {
    refusal_fixture(with_properties, missing_table())
}

fn refusal_fixture(
    with_properties: bool,
    refusal: ConnectorError,
) -> (ConnectorControlPlanningLease, Arc<RecordingMetadata>) {
    let instance = ConnectorInstanceId::parse("guard_catalog").unwrap();
    let original =
        novarocks_catalog_application::test_support::test_control_binding_for(instance.clone(), 8);
    let metadata = Arc::new(RecordingMetadata {
        instance: instance.clone(),
        captured: Mutex::new(None),
        result: Err(refusal),
    });
    let mut binding = ConnectorControlBinding::try_new(
        original.descriptor().clone(),
        original.incarnation(),
        metadata.clone(),
        original.planning().clone(),
        original.execution_distribution().clone(),
        None,
    )
    .unwrap();
    if with_properties {
        binding = binding
            .with_catalog_properties(super::super::test_admission::catalog_properties(&instance))
            .unwrap();
    }
    (
        ConnectorControlPlanningLease::new(Arc::new(binding), || {}),
        metadata,
    )
}

fn context(stop: &ConnectorStopOwner) -> ConnectorRequestContext {
    ConnectorRequestContext::try_new(
        Instant::now() + Duration::from_secs(30),
        stop.view(),
        4096,
        64 * 1024,
    )
    .unwrap()
}

fn invoke(
    lease: &ConnectorControlPlanningLease,
    context: ConnectorRequestContext,
    catalog: &str,
) -> String {
    let target = TargetBackend {
        provider_id: ConnectorProviderId::parse("iceberg").unwrap(),
        catalog: catalog.into(),
        namespace: "db".into(),
        table: "table".into(),
    };
    reject_if_iceberg_mv_table_with_planning_lease_and_context(
        &UnavailableMvStorageObservationPort,
        lease,
        super::super::test_admission::test_readiness().as_ref(),
        &target,
        IcebergMvUserMutation::Insert,
        context,
    )
    .unwrap_err()
}

#[test]
fn attempt_guard_collects_with_retained_catalog_and_original_sink() {
    let (lease, provider) = fixture(true);
    let stop = ConnectorStopOwner::new();
    let sink: Arc<dyn ConnectorVendedCredentialLeaseSink> = Arc::new(UnusedSink);
    let original = context(&stop).with_vended_credential_lease_sink(sink.clone());
    assert!(original.vended_credential_lease_collection().is_none());
    let error = invoke(&lease, original.clone(), "guard_catalog");
    assert!(error.contains("guard test table is absent"));
    let recorded = provider.captured.lock().unwrap();
    let request = recorded.as_ref().unwrap();
    let collection = request.context.vended_credential_lease_collection().expect(
        "MV guard must bind the attempt sink to its exact retained catalog before provider I/O",
    );
    assert_eq!(
        collection.catalog_properties(),
        lease.binding().catalog_properties().unwrap()
    );
    assert!(Arc::ptr_eq(
        request.context.vended_credential_lease_sink().unwrap(),
        &sink
    ));
    assert_eq!(request.context.deadline(), original.deadline());
    assert_eq!(
        request.table.instance_id,
        lease.binding().descriptor().instance_id
    );
    assert!(matches!(
        request.selector,
        ConnectorTableObjectSelector::Current
    ));
    assert!(matches!(
        request.resolution,
        ConnectorTableResolution::StrictBaseTable
    ));
    stop.request_stop();
    assert!(request.context.is_cancelled());
    assert!(original.vended_credential_lease_collection().is_none());
}

#[test]
fn control_guard_does_not_require_execution_properties_or_mint_an_attempt() {
    let (lease, provider) = fixture(false);
    assert!(lease.binding().catalog_properties().is_err());
    let original = context(&ConnectorStopOwner::new());
    assert!(
        invoke(&lease, original.clone(), "guard_catalog").contains("guard test table is absent")
    );
    let recorded = provider.captured.lock().unwrap();
    let request = recorded.as_ref().unwrap();
    assert!(request.context.vended_credential_lease_sink().is_none());
    assert!(
        request
            .context
            .vended_credential_lease_collection()
            .is_none()
    );
    assert_eq!(request.context.deadline(), original.deadline());
}

#[test]
fn mismatched_catalog_is_rejected_before_provider_capture() {
    let (lease, provider) = fixture(true);
    let original =
        context(&ConnectorStopOwner::new()).with_vended_credential_lease_sink(Arc::new(UnusedSink));
    assert!(
        invoke(&lease, original, "another_catalog").contains("does not match the target connector")
    );
    assert!(provider.captured.lock().unwrap().is_none());
}

// The provider refuses before creating a metadata CPU input. This port must
// never be used as a fabricated positive admission or an actual-exit proof.
struct UnusedMetadataCpu;
impl ProviderMetadataCpuPort for UnusedMetadataCpu {
    fn submit_metadata_in(
        &self,
        _: &mut Option<ProviderMetadataCpuInput>,
    ) -> Result<(), ConnectorError> {
        panic!("provider already refused before CPU input")
    }
    fn poll_finished(
        &self,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), ConnectorError>> {
        panic!("no CPU input was dispatched")
    }
    fn poll_closed(
        &self,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), ConnectorError>> {
        panic!("the fixture claims no CPU exit")
    }
    fn local_failure(&self) -> Option<Arc<dyn std::error::Error + Send + Sync>> {
        None
    }
}

struct GuardRefusalPort {
    error: novarocks_query_application::session_error::QueryServiceError,
}
impl novarocks_table_maintenance::product::TableMaintenanceEffectPort for GuardRefusalPort {
    fn observe_cleanup_owned_ref<'a>(
        &'a self,
        _: &'a novarocks_table_maintenance::gc_observation::GcOwnedRefObservationAccelerator,
        _: novarocks_table_maintenance::gc_observation::GcOwnedRefObservation,
        _: i64,
        _: i64,
    ) -> novarocks_table_maintenance::product::MaintenanceFuture<
        'a,
        Result<
            novarocks_table_maintenance::gc_observation::GcOwnedRefObservationDecision,
            novarocks_table_maintenance::runtime::TerminalError,
        >,
    > {
        Box::pin(async {
            Err(
                novarocks_table_maintenance::runtime::TerminalError::pre_dispatch_failed(
                    "this effect source cannot observe owned-ref cleanup",
                ),
            )
        })
    }
    fn reject_user_action_on_mv<'a>(
        &'a self,
        _: &'a novarocks_table_maintenance::MaintenanceTarget,
    ) -> novarocks_table_maintenance::product::MaintenanceFuture<
        'a,
        Result<(), novarocks_table_maintenance::runtime::TerminalError>,
    > {
        Box::pin(async move {
            Err(
                crate::query_execution::maintenance::rewrite_job::guard_terminal_error(
                    self.error.clone(),
                ),
            )
        })
    }
    fn execute_metadata<'a>(
        &'a self,
        _: novarocks_table_maintenance::MaintenanceActionRequest,
    ) -> novarocks_table_maintenance::product::MaintenanceFuture<
        'a,
        Result<novarocks_table_maintenance::MaintenanceActionOutcome, String>,
    > {
        Box::pin(async { panic!("MV guard refusal precedes metadata effects") })
    }
    fn begin_rewrite<'a>(
        &'a self,
        _: &'a novarocks_table_maintenance::MaintenanceTarget,
        _: novarocks_table_maintenance::product::RewriteIntent,
    ) -> novarocks_table_maintenance::product::MaintenanceFuture<
        'a,
        Result<
            Box<dyn novarocks_table_maintenance::product::DistributedRewriteSession + 'a>,
            String,
        >,
    > {
        Box::pin(async { panic!("MV guard refusal precedes Rewrite") })
    }
    fn begin_cleanup<'a>(
        &'a self,
        _: &'a novarocks_table_maintenance::MaintenanceTarget,
        _: i64,
    ) -> novarocks_table_maintenance::product::MaintenanceFuture<
        'a,
        Result<
            Box<dyn novarocks_table_maintenance::product::CleanupSession + 'a>,
            novarocks_table_maintenance::runtime::TerminalError,
        >,
    > {
        Box::pin(async { panic!("MV guard refusal precedes Cleanup session creation") })
    }
}

#[tokio::test]
async fn asynchronous_guard_provider_cause_survives_cleanup_product_and_service() {
    use novarocks_failure_contract::{FailureProjection, SharedFailureCause};
    use novarocks_table_maintenance::{
        MaintenanceActionRequest, MaintenanceTarget, product::TableMaintenanceProduct,
        runtime::MaintenanceJobState,
    };
    for control in [
        novarocks_type_contract::CompileControlError::ResourceExhausted,
        novarocks_type_contract::CompileControlError::Cancelled,
    ] {
        for governed in [false, true] {
            let original = ConnectorError::from_domain_static(
                ConnectorErrorKind::Interrupted,
                control.clone(),
                "actual guard provider refusal",
            );
            let cause = std::error::Error::source(&original)
                .unwrap()
                .downcast_ref::<SharedFailureCause>()
                .unwrap()
                .clone();
            let expected = original.failure_fact();
            let (lease, provider) = refusal_fixture(true, original);
            let target = TargetBackend {
                provider_id: ConnectorProviderId::parse("iceberg").unwrap(),
                catalog: "guard_catalog".into(),
                namespace: "db".into(),
                table: "table".into(),
            };
            let stop = ConnectorStopOwner::new();
            let readiness = super::super::test_admission::test_readiness();
            let error = if governed {
                super::reject_if_iceberg_mv_table_with_governed_planning_lease_and_context_async(
                    &UnavailableMvStorageObservationPort,
                    &lease,
                    &readiness,
                    &target,
                    IcebergMvUserMutation::Maintenance,
                    context(&stop),
                    Arc::new(UnusedMetadataCpu),
                )
                .await
                .unwrap_err()
            } else {
                super::reject_if_iceberg_mv_table_with_planning_lease_and_context_async(
                    &UnavailableMvStorageObservationPort,
                    &lease,
                    &readiness,
                    &target,
                    IcebergMvUserMutation::Insert,
                    context(&stop),
                )
                .await
                .unwrap_err()
            };
            assert!(
                provider.captured.lock().unwrap().is_some(),
                "actual selected provider capture was called"
            );
            assert_eq!(error.failure_cause(), Some(&cause));
            assert_eq!(error.execution_failure().unwrap().fact(), expected);
            let terminal = TableMaintenanceProduct::new(None)
                .execute_user_action(
                    &GuardRefusalPort { error },
                    MaintenanceActionRequest::RemoveOrphanFiles {
                        target: MaintenanceTarget {
                            catalog: target.catalog,
                            namespace: target.namespace,
                            table: target.table,
                        },
                        older_than_ms: 1,
                    },
                )
                .await
                .unwrap_err();
            assert_eq!(terminal.state, MaintenanceJobState::PreDispatchFailed);
            assert_eq!(
                terminal.effect,
                Some(novarocks_coordination_contract::ExecutionEffect::None)
            );
            assert_eq!(terminal.failure_cause.as_ref(), Some(&cause));
            assert_eq!(terminal.selected_failure_fact(), Some(expected));
            let service =
                crate::query_execution::maintenance::rewrite_job::computation_service_error(
                    terminal,
                );
            assert_eq!(service.failure_cause(), Some(&cause));
            assert_eq!(service.execution_failure().unwrap().fact(), expected);
        }
    }
}

#[tokio::test]
async fn ordinary_mv_policy_denial_has_no_invented_source_or_failure_fact() {
    use novarocks_table_maintenance::{
        MaintenanceActionRequest, MaintenanceTarget, product::TableMaintenanceProduct,
    };
    let message = "table guard_catalog.db.table is a materialized view; use ALTER MATERIALIZED VIEW or DROP MATERIALIZED VIEW";
    let error = super::materialized_view_denial(
        &TargetBackend {
            provider_id: ConnectorProviderId::parse("iceberg").unwrap(),
            catalog: "guard_catalog".into(),
            namespace: "db".into(),
            table: "table".into(),
        },
        IcebergMvUserMutation::Maintenance,
    );
    let terminal = TableMaintenanceProduct::new(None)
        .execute_user_action(
            &GuardRefusalPort { error },
            MaintenanceActionRequest::RemoveOrphanFiles {
                target: MaintenanceTarget {
                    catalog: "guard_catalog".into(),
                    namespace: "db".into(),
                    table: "table".into(),
                },
                older_than_ms: 1,
            },
        )
        .await
        .unwrap_err();
    assert!(terminal.failure_cause.is_none());
    assert!(terminal.selected_failure_fact().is_none());
    let service =
        crate::query_execution::maintenance::rewrite_job::computation_service_error(terminal);
    assert_eq!(service.message(), message);
    assert!(service.failure_cause().is_none());
    assert!(service.execution_failure().is_none());
}

// A fixed actual Provider view, with no execution/CPU or mutation capability.
// Re-reading or resolving here is not required to deny a user mutation.
struct GuardDocuments {
    descriptor: ConnectorInstanceDescriptor,
    epoch: ProviderBindingEpoch,
    reply: Result<KeyedDocumentView, KeyedDocumentReadError>,
    observed: std::sync::atomic::AtomicUsize,
}
impl ConnectorDocumentStorageObservation for GuardDocuments {
    fn descriptor(&self) -> &ConnectorInstanceDescriptor {
        &self.descriptor
    }
    fn incarnation(&self) -> ProviderBindingEpoch {
        self.epoch
    }
    fn observe_keyed_documents(
        &self,
        _: KeyedDocumentObservationRequest,
    ) -> Result<KeyedDocumentView, KeyedDocumentReadError> {
        self.observed
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.reply.clone()
    }
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
        Box::pin(async move { self.observe_keyed_documents(request) })
    }
    fn observe_full_keyed_documents(
        &self,
        _: KeyedDocumentObservationRequest,
    ) -> Result<FullKeyedDocumentView, KeyedDocumentReadError> {
        panic!("a rejected mutation does not need full document observation")
    }
    fn decode_keyed_slot_change_evidence(
        &self,
        _: KeyedDocumentObservationRequest,
        _: ExternalMutationEvidence,
    ) -> Result<UnknownCommitEvidence, ConnectorError> {
        panic!("a rejected mutation does not decode another operation's evidence")
    }
    fn resolve_keyed_commit(
        &self,
        _: KeyedDocumentObservationRequest,
        _: UnknownCommitEvidence,
    ) -> Result<CommitConclusion, ConnectorError> {
        panic!("a rejected mutation does not settle another operation")
    }
    fn discover_documents(
        &self,
        _: ConnectorDocumentDiscoveryRequest,
    ) -> Result<ConnectorDocumentDiscoveryPage, ConnectorError> {
        panic!("the exact guard does not discover other targets")
    }
}

// Opening this repository is pure. Any actual transaction is a test-visible
// failure, so successful policy denial cannot be attributed to a healthy cache.
struct UnavailableGuardStore {
    inner: novarocks_state_store_testkit::testing::InMemoryStateStore,
    transactions: std::sync::atomic::AtomicUsize,
}
impl UnavailableGuardStore {
    fn refused(&self) -> novarocks_state_store_api::StateStoreError {
        self.transactions
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        novarocks_state_store_api::StateStoreError::new(
            novarocks_state_store_api::StateStoreErrorKind::ProviderUnavailable,
            "guard repository is deliberately unavailable",
        )
    }
}
#[async_trait::async_trait]
impl novarocks_state_store_api::StateStore for UnavailableGuardStore {
    fn limits(&self) -> &novarocks_state_store_api::StateStoreLimits {
        novarocks_state_store_api::StateStore::limits(&self.inner)
    }
    fn attempts(&self) -> &novarocks_state_store_api::AttemptSupervisor {
        novarocks_state_store_api::StateStore::attempts(&self.inner)
    }
    async fn begin_read(
        &self,
    ) -> Result<
        Box<dyn novarocks_state_store_api::ReadTransaction>,
        novarocks_state_store_api::StateStoreError,
    > {
        Err(self.refused())
    }
    async fn begin_write(
        &self,
        _: novarocks_state_store_api::WriteAttempt,
        _: &str,
    ) -> Result<
        Box<dyn novarocks_state_store_api::WriteTransaction>,
        novarocks_state_store_api::StateStoreError,
    > {
        Err(self.refused())
    }
    async fn identity(
        &self,
    ) -> Result<novarocks_state_store_api::StoreIdentity, novarocks_state_store_api::StateStoreError>
    {
        novarocks_state_store_api::StateStore::identity(&self.inner).await
    }
}

fn observed_guard_fixture() -> (ConnectorControlBinding, KeyedDocumentView) {
    use novarocks_mv_application::persistence::test_support::{
        KeyedProjectionFixture, observed_current_keyed,
    };
    let instance = ConnectorInstanceId::parse("guard_catalog").unwrap();
    let properties = super::super::test_admission::catalog_properties(&instance);
    let projection = KeyedProjectionFixture::new(
        novarocks_mv_application::product::MvTarget::from_parts(
            Some("guard_catalog"),
            "db",
            "table",
        ),
        Some(2),
    )
    .build()
    .unwrap();
    let mut view = observed_current_keyed(&projection, properties.handle().clone()).view;
    let original =
        novarocks_catalog_application::test_support::test_control_binding_for(instance.clone(), 8);
    view.owner = ConnectorProviderBindingKey {
        instance_id: instance.clone(),
        incarnation: original.incarnation(),
    };
    let metadata = crate::connector::schema_creation::declared_test_metadata(
        ConnectorTableMetadataHeader {
            identity: view.table.clone(),
            planning_facts: ConnectorTablePlanningFacts::empty(),
            definition_facts: ConnectorTableDefinitionFacts::empty(),
            version: None,
            statistics_data_version: None,
            table: ConnectorTableHandle::try_new(
                instance.clone(),
                bytes::Bytes::from_static(b"guard-table"),
            )
            .unwrap(),
        },
        Arc::new(arrow::datatypes::Schema::empty()),
        Vec::new(),
    );
    let metadata = Arc::new(RecordingMetadata {
        instance,
        captured: Mutex::new(None),
        result: Ok(ConnectorTableObjectBinding {
            metadata,
            object_id: view.object_id.clone(),
        }),
    });
    let binding = ConnectorControlBinding::try_new(
        original.descriptor().clone(),
        original.incarnation(),
        metadata,
        original.planning().clone(),
        original.execution_distribution().clone(),
        None,
    )
    .unwrap()
    .with_catalog_properties(properties)
    .unwrap();
    (binding, view)
}
fn guard_documents_lease(
    binding: ConnectorControlBinding,
    reply: Result<KeyedDocumentView, KeyedDocumentReadError>,
) -> (ConnectorControlPlanningLease, Arc<GuardDocuments>) {
    let documents = Arc::new(GuardDocuments {
        descriptor: binding.descriptor().clone(),
        epoch: binding.incarnation(),
        reply,
        observed: std::sync::atomic::AtomicUsize::new(0),
    });
    let storage = ConnectorDocumentStorageBinding::try_new(
        binding.descriptor().clone(),
        binding.incarnation(),
        Some(documents.clone()),
        None,
    )
    .unwrap();
    (
        ConnectorControlPlanningLease::new(
            Arc::new(binding.try_with_document_storage(Some(storage)).unwrap()),
            || {},
        ),
        documents,
    )
}
async fn unavailable_guard_readiness() -> (
    crate::mv::domain::readiness::MvReadinessPort,
    Arc<UnavailableGuardStore>,
) {
    let store = Arc::new(UnavailableGuardStore {
        inner: novarocks_state_store_testkit::testing::InMemoryStateStore::new("guard-unavailable"),
        transactions: std::sync::atomic::AtomicUsize::new(0),
    });
    let repository =
        novarocks_mv_application::state_store_repository::StateStoreMvRepository::open(
            store.clone(),
            Default::default(),
        )
        .await
        .unwrap();
    assert!(
        repository.list_keyed_projections().await.is_err(),
        "the same actual repository really refuses reads"
    );
    store
        .transactions
        .store(0, std::sync::atomic::Ordering::SeqCst);
    let service = novarocks_mv_application::readiness::MvKeyedReadinessService::new(
        repository,
        Arc::default(),
    );
    (
        crate::mv::domain::readiness::MvReadinessPort::from_product(
            service,
            tokio::runtime::Handle::current(),
            super::super::test_admission::management_entrance(),
        ),
        store,
    )
}
async fn guard_with_observed_documents(
    asynchronous: bool,
    lease: &ConnectorControlPlanningLease,
    readiness: &crate::mv::domain::readiness::MvReadinessPort,
    mutation: IcebergMvUserMutation,
) -> Result<(), String> {
    let target = TargetBackend {
        provider_id: ConnectorProviderId::parse("iceberg").unwrap(),
        catalog: "guard_catalog".into(),
        namespace: "db".into(),
        table: "table".into(),
    };
    let stop = ConnectorStopOwner::new();
    if asynchronous {
        super::reject_if_iceberg_mv_table_with_governed_planning_lease_and_context_async(
            &UnavailableMvStorageObservationPort,
            lease,
            readiness,
            &target,
            mutation,
            context(&stop),
            Arc::new(UnusedMetadataCpu),
        )
        .await
        .map_err(|error| error.message().to_owned())
    } else {
        reject_if_iceberg_mv_table_with_planning_lease_and_context(
            &UnavailableMvStorageObservationPort,
            lease,
            readiness,
            &target,
            mutation,
            context(&stop),
        )
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn marked_table_guard_denies_without_repository_or_second_provider_observation() {
    for asynchronous in [false, true] {
        for publication in ["valid", "missing", "malformed"] {
            let (binding, mut view) = observed_guard_fixture();
            match publication {
                "missing" => view.current_records.clear(),
                "malformed" => {
                    view.current_records[0].payload = bytes::Bytes::from_static(b"invalid P")
                }
                _ => {}
            }
            let (lease, documents) = guard_documents_lease(binding, Ok(view));
            let (readiness, store) = unavailable_guard_readiness().await;
            let error = guard_with_observed_documents(
                asynchronous,
                &lease,
                &readiness,
                IcebergMvUserMutation::Maintenance,
            )
            .await
            .unwrap_err();
            assert_eq!(
                error,
                "table guard_catalog.db.table is a materialized view; use ALTER MATERIALIZED VIEW or DROP MATERIALIZED VIEW"
            );
            assert_eq!(
                documents.observed.load(std::sync::atomic::Ordering::SeqCst),
                1
            );
            assert_eq!(
                store.transactions.load(std::sync::atomic::Ordering::SeqCst),
                0
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ordinary_table_and_legacy_drop_keep_their_original_guard_boundary_without_store() {
    for asynchronous in [false, true] {
        let (binding, mut view) = observed_guard_fixture();
        view.management = None;
        view.slots.clear();
        view.versions.clear();
        view.roots.clear();
        view.current_records.clear();
        let (lease, documents) = guard_documents_lease(binding, Ok(view));
        let (readiness, store) = unavailable_guard_readiness().await;
        guard_with_observed_documents(
            asynchronous,
            &lease,
            &readiness,
            IcebergMvUserMutation::Maintenance,
        )
        .await
        .unwrap();
        assert_eq!(
            documents.observed.load(std::sync::atomic::Ordering::SeqCst),
            1
        );
        let (binding, _) = observed_guard_fixture();
        let (legacy, legacy_documents) = guard_documents_lease(
            binding,
            Err(KeyedDocumentReadError::LegacyFormat {
                key: "legacy-mv".into(),
            }),
        );
        guard_with_observed_documents(
            asynchronous,
            &legacy,
            &readiness,
            IcebergMvUserMutation::DropTable,
        )
        .await
        .unwrap();
        assert!(
            guard_with_observed_documents(
                asynchronous,
                &legacy,
                &readiness,
                IcebergMvUserMutation::Maintenance
            )
            .await
            .unwrap_err()
            .contains("LegacyFormat")
        );
        assert_eq!(
            legacy_documents
                .observed
                .load(std::sync::atomic::Ordering::SeqCst),
            2
        );
        assert_eq!(
            store.transactions.load(std::sync::atomic::Ordering::SeqCst),
            0
        );
    }
}
