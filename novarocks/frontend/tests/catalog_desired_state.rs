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

//! The catalog desired-state source contract and its two failure scopes.
//!
//! These are the contracts a later refactor is most likely to erode silently,
//! because the wrong behaviour still looks like a working frontend:
//!
//! * A failed enumeration that degrades into "a snapshot with zero catalogs"
//!   retires every catalog in the deployment while reporting success.
//! * A source mode that quietly falls back to the dynamic StateStore accepts
//!   SQL writes into a store nothing reads.
//! * A per-catalog provider failure that escalates to the whole reconcile takes
//!   healthy catalogs down with one broken one.
//!
//! Each test below pins one of those, by observing behaviour rather than shape.

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use common::state_store_fixture;
use novarocks_frontend::catalog_application::desired_state::{
    CatalogDesiredStateSource, CatalogDesiredStateSourceInput, CatalogDesiredStateSourceMode,
    CatalogSqlMutationAdmission,
};
use novarocks_frontend::catalog_application::{
    CatalogAdmission, CatalogApplicationErrorKind, CatalogApplicationPort, CatalogCreateCommand,
    CatalogDropCommand, CatalogRuntimeProjection, FrontendCatalogApplicationPort,
};
use novarocks_frontend::catalog_attachment::{CatalogAttachment, CatalogAttachmentRepository};
use novarocks_frontend::catalog_controller::{CatalogProjectionConfig, FrontendCatalogController};
use novarocks_frontend::connector::ConnectorControlHost;
use novarocks_frontend::{
    ClusterBackendOpenConfig, FrontendApplicationErrorKind, FrontendApplicationHost,
    FrontendExecutionConfig, FrontendNativeTransport,
};
use novarocks_native_trust::{
    DeploymentId, NativeCallerSubject, NativeTransportMode, NativeTrust, ValidatedSharedSecret,
};
use novarocks_secret::SecretValue;
use novarocks_spi::connector::{
    ConnectorBeginScanRequest, ConnectorControlBinding, ConnectorError, ConnectorErrorKind,
    ConnectorExecutionDistribution, ConnectorInstanceDescriptor, ConnectorInstanceId,
    ConnectorListTablesRequest, ConnectorMetadata, ConnectorNamespaceRequest,
    ConnectorProviderBinding, ConnectorProviderId, ConnectorScan, ConnectorScanHandle,
    ConnectorScanPlanning, ConnectorSplitPlanningRequest, ConnectorTableHandle,
    ConnectorTableMetadata, ConnectorTableRequest, ProviderBindingEpoch,
};
use novarocks_state_store_api::{
    AttemptSupervisor, Key, RangePage, RangeRequest, ReadTransaction, StateRecord, StateStore,
    StateStoreError, StateStoreErrorKind, StateStoreLimits, StoreIdentity, WriteAttempt,
    WriteTransaction,
};
use tokio::sync::Notify;
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Connector fixtures
// ---------------------------------------------------------------------------

fn unsupported() -> ConnectorError {
    ConnectorError::new(
        ConnectorErrorKind::Unsupported,
        "test-only control capability",
    )
}

struct TestControl {
    instance_id: ConnectorInstanceId,
    incarnation: ProviderBindingEpoch,
}

impl ConnectorMetadata for TestControl {
    fn instance_id(&self) -> &ConnectorInstanceId {
        &self.instance_id
    }

    fn namespace_exists(
        &self,
        _request: ConnectorNamespaceRequest,
    ) -> Result<bool, ConnectorError> {
        Err(unsupported())
    }

    fn table_exists(&self, _request: ConnectorTableRequest) -> Result<bool, ConnectorError> {
        Err(unsupported())
    }

    fn list_tables(
        &self,
        _request: ConnectorListTablesRequest,
    ) -> Result<Vec<novarocks_spi::connector::ConnectorTableIdentity>, ConnectorError> {
        Err(unsupported())
    }

    fn load_table(
        &self,
        _request: ConnectorTableRequest,
    ) -> Result<ConnectorTableMetadata, ConnectorError> {
        Err(unsupported())
    }
}

impl ConnectorScanPlanning for TestControl {
    fn instance_id(&self) -> &ConnectorInstanceId {
        &self.instance_id
    }

    fn begin_scan(
        &self,
        _table: &ConnectorTableHandle,
        _request: ConnectorBeginScanRequest,
    ) -> Result<ConnectorScan, ConnectorError> {
        Err(unsupported())
    }

    fn plan_splits(
        &self,
        _scan: &ConnectorScanHandle,
        _request: ConnectorSplitPlanningRequest,
    ) -> Result<novarocks_spi::connector::ConnectorSplitPlanningResult, ConnectorError> {
        Err(unsupported())
    }
}

impl ConnectorExecutionDistribution for TestControl {
    fn declaration(
        &self,
        _context: &novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<ConnectorProviderBinding, ConnectorError> {
        ConnectorProviderBinding::iceberg(
            self.instance_id.as_str(),
            self.incarnation.to_bytes(),
            "default",
        )
        .map_err(|error| ConnectorError::new(ConnectorErrorKind::InvalidRequest, error.to_string()))
    }
}

fn binding(
    instance_id: ConnectorInstanceId,
    incarnation: ProviderBindingEpoch,
) -> ConnectorControlBinding {
    let provider = Arc::new(TestControl {
        instance_id,
        incarnation,
    });
    ConnectorControlBinding::try_new(
        ConnectorInstanceDescriptor {
            provider_id: ConnectorProviderId::parse("iceberg").expect("provider ID"),
            instance_id: provider.instance_id.clone(),
        },
        provider.incarnation,
        provider.clone(),
        provider.clone(),
        provider,
        None,
    )
    .expect("control binding")
}

/// Mints a distinct control generation per materialization, like a real provider
/// role factory: reusing an incarnation would trip the retired-generation guard on a
/// same-name recreate. `poisoned` names the one catalog whose materialization
/// fails, so a single-catalog failure scope can be observed alongside healthy
/// catalogs in one frontend.
struct SelectivelyFailingFactory {
    incarnations: AtomicU64,
    poisoned: Option<&'static str>,
    hanging: Option<HangingCatalog>,
}

struct HangingCatalog {
    name: &'static str,
    entered: Arc<Notify>,
    release: Arc<Notify>,
}

impl SelectivelyFailingFactory {
    fn ready() -> Self {
        Self {
            incarnations: AtomicU64::new(0),
            poisoned: None,
            hanging: None,
        }
    }

    fn failing_for(catalog: &'static str) -> Self {
        Self {
            incarnations: AtomicU64::new(0),
            poisoned: Some(catalog),
            hanging: None,
        }
    }

    fn hanging_for(catalog: &'static str, entered: Arc<Notify>, release: Arc<Notify>) -> Self {
        Self {
            incarnations: AtomicU64::new(0),
            poisoned: None,
            hanging: Some(HangingCatalog {
                name: catalog,
                entered,
                release,
            }),
        }
    }
}

impl novarocks_spi::connector::ConnectorControlRoleBindingFactory for SelectivelyFailingFactory {
    fn provider_id(&self) -> ConnectorProviderId {
        ConnectorProviderId::parse("iceberg").expect("static provider ID")
    }

    fn normalize_and_validate(
        &self,
        properties: novarocks_spi::connector::CatalogProperties,
    ) -> Result<
        novarocks_spi::connector::NormalizedCatalogProperties,
        novarocks_spi::connector::ConnectorMaterializationError,
    > {
        novarocks_spi::connector::NormalizedCatalogProperties::try_new(properties).map_err(|detail| novarocks_spi::connector::ConnectorMaterializationError::new(
            novarocks_spi::connector::ConnectorMaterializationErrorClass::InvalidDefinition,
            novarocks_spi::connector::ConnectorMaterializationRetryDisposition::UntilDefinitionChanges,
            detail,
        ))
    }

    fn materialize(
        &self,
        properties: novarocks_spi::connector::NormalizedCatalogProperties,
        _context: novarocks_spi::connector::MaterializationContext,
    ) -> futures::future::BoxFuture<
        'static,
        Result<
            novarocks_spi::connector::ConnectorControlRoleBinding,
            novarocks_spi::connector::ConnectorMaterializationError,
        >,
    > {
        use futures::FutureExt;

        let poisoned = self.poisoned;
        let hanging = self.hanging.as_ref().and_then(|hanging| {
            (hanging.name == properties.handle().catalog_name().as_str())
                .then(|| (Arc::clone(&hanging.entered), Arc::clone(&hanging.release)))
        });
        let sequence = self.incarnations.fetch_add(1, Ordering::Relaxed) + 1;
        let mut incarnation = [0_u8; 16];
        incarnation[8..].copy_from_slice(&sequence.to_be_bytes());
        async move {
            if let Some((entered, release)) = hanging {
                entered.notify_one();
                release.notified().await;
                return Err(novarocks_spi::connector::ConnectorMaterializationError::new(
                    novarocks_spi::connector::ConnectorMaterializationErrorClass::Unavailable,
                    novarocks_spi::connector::ConnectorMaterializationRetryDisposition::UntilDefinitionChanges,
                    "injected hanging provider materialization",
                ));
            }
            if poisoned == Some(properties.handle().catalog_name().as_str()) {
                return Err(novarocks_spi::connector::ConnectorMaterializationError::new(
                    novarocks_spi::connector::ConnectorMaterializationErrorClass::Unavailable,
                    novarocks_spi::connector::ConnectorMaterializationRetryDisposition::UntilDefinitionChanges,
                    "injected provider materialization failure",
                ));
            }
            let binding = binding(
                properties.handle().catalog_name().clone(),
                ProviderBindingEpoch::from_bytes(incarnation),
            )
                .with_catalog_properties(properties.as_catalog_properties().clone())
                .map_err(novarocks_spi::connector::ConnectorMaterializationError::from)?;
            novarocks_spi::connector::ConnectorControlRoleBinding::try_new(
                properties,
                Arc::new(binding),
                None,
                None,
            )
            .map_err(novarocks_spi::connector::ConnectorMaterializationError::from)
        }
        .boxed()
    }
}

// ---------------------------------------------------------------------------
// StateStore fixture that can lose the ability to enumerate
// ---------------------------------------------------------------------------

/// A store whose range scan fails while targeted `get` reads keep working.
///
/// This is what "the enumeration cannot be completed" looks like from the
/// repository's point of view, and it is deliberately not the same thing as
/// "the prefix is empty": the records are all still there.
struct ScanFailingStore {
    inner: Arc<dyn StateStore>,
    scans_available: AtomicBool,
}

struct ScanFailingRead {
    inner: Box<dyn ReadTransaction>,
    scans_available: bool,
}

#[async_trait::async_trait]
impl ReadTransaction for ScanFailingRead {
    async fn get(&mut self, key: &Key) -> Result<Option<StateRecord>, StateStoreError> {
        self.inner.get(key).await
    }

    async fn range(&mut self, request: &RangeRequest) -> Result<RangePage, StateStoreError> {
        if !self.scans_available {
            return Err(StateStoreError::new(
                StateStoreErrorKind::ProviderUnavailable,
                "injected catalog enumeration failure",
            ));
        }
        self.inner.range(request).await
    }

    async fn abort(self: Box<Self>) -> Result<(), StateStoreError> {
        self.inner.abort().await
    }
}

#[async_trait::async_trait]
impl StateStore for ScanFailingStore {
    fn limits(&self) -> &StateStoreLimits {
        self.inner.limits()
    }

    /// Attempts belong to the wrapped instance: an identity minted here has to
    /// be one the store that will run the write actually issued.
    fn attempts(&self) -> &AttemptSupervisor {
        self.inner.attempts()
    }

    async fn begin_read(&self) -> Result<Box<dyn ReadTransaction>, StateStoreError> {
        Ok(Box::new(ScanFailingRead {
            inner: self.inner.begin_read().await?,
            scans_available: self.scans_available.load(Ordering::Acquire),
        }))
    }

    async fn begin_write(
        &self,
        attempt: WriteAttempt,
        purpose: &str,
    ) -> Result<Box<dyn WriteTransaction>, StateStoreError> {
        self.inner.begin_write(attempt, purpose).await
    }

    async fn identity(&self) -> Result<StoreIdentity, StateStoreError> {
        self.inner.identity().await
    }
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

fn catalog(name: &str) -> ConnectorInstanceId {
    ConnectorInstanceId::parse(name).expect("instance ID")
}

fn create_command(name: &str) -> CatalogCreateCommand {
    CatalogCreateCommand {
        instance_id: catalog(name),
        display_name: name.to_string(),
        properties: vec![("type".to_string(), "iceberg".to_string())],
        if_not_exists: false,
    }
}

fn port_with(
    source: CatalogDesiredStateSource,
    factory: SelectivelyFailingFactory,
) -> (
    Arc<ConnectorControlHost>,
    Arc<FrontendCatalogApplicationPort>,
) {
    let control = Arc::new(
        ConnectorControlHost::with_role_factories(vec![Arc::new(factory)]).expect("control host"),
    );
    let port = Arc::new(FrontendCatalogApplicationPort::new(
        source,
        Arc::clone(&control),
        CatalogRuntimeProjection::new().publisher(),
        tokio::runtime::Handle::current(),
    ));
    (control, port)
}

fn controller(
    store: Arc<dyn StateStore>,
    port: &Arc<FrontendCatalogApplicationPort>,
) -> Arc<FrontendCatalogController> {
    FrontendCatalogController::new(store, Arc::clone(port), CatalogProjectionConfig::default())
        .expect("catalog controller")
}

async fn ready_attachment_id(port: &FrontendCatalogApplicationPort, name: &str) -> Uuid {
    for _ in 0..100 {
        if let CatalogAdmission::Ready(observation) = port.admit_catalog(&catalog(name)) {
            return observation.attachment_id;
        }
        tokio::task::yield_now().await;
    }
    panic!("catalog `{name}` did not become Ready after scheduler materialization")
}

async fn shutdown(mut host: novarocks_frontend::StateStoreHost) {
    host.shutdown(Instant::now() + Duration::from_secs(5))
        .await
        .expect("state store shutdown");
}

async fn seed_ready_catalogs(
    repository: &CatalogAttachmentRepository,
    count: usize,
) -> Vec<String> {
    let mut names = Vec::with_capacity(count);
    for index in 0..count {
        let name = format!("catalog.scale-{count}-{index:04}");
        repository
            .create(CatalogAttachment {
                attachment_id: Uuid::now_v7(),
                instance_id: catalog(&name),
                provider_id: ConnectorProviderId::parse("iceberg").expect("provider ID"),
                display_name: name.clone(),
                durable_properties: vec![("type".to_string(), "iceberg".to_string())],
                credential_bindings: Vec::new(),
                created_at_ms: 1,
            })
            .await
            .expect("seed desired catalog");
        names.push(name);
    }
    names
}

// ---------------------------------------------------------------------------
// 1. Dynamic mode: SQL writes desired state and a restart rediscovers it
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dynamic_state_store_mode_survives_a_frontend_restart_through_one_snapshot_path() {
    let cluster = format!("catalog-desired-state-restart-{}", Uuid::now_v7());
    let first_host = state_store_fixture::open(cluster.clone()).await;
    let store = first_host.state_store().expect("test StateStore");
    let repository = CatalogAttachmentRepository::open(Arc::clone(&store), first_host.run_policy())
        .await
        .expect("open attachment repository");
    let source = CatalogDesiredStateSource::dynamic_state_store(repository.clone());
    assert_eq!(
        source.sql_mutation_admission(),
        CatalogSqlMutationAdmission::Accepted,
        "the dynamic StateStore mode is the mode SQL may write"
    );
    let (_control, port) = port_with(source, SelectivelyFailingFactory::ready());

    let created = port
        .create_catalog(create_command("catalog.analytics"))
        .expect("CREATE CATALOG");
    port.create_catalog(create_command("catalog.raw"))
        .expect("CREATE CATALOG");

    // The snapshot the source enumerates is the deployment's complete desired
    // state, and it carries only logical configuration: no attachment id, no
    // CAS version, no readiness.
    let snapshot = CatalogDesiredStateSource::dynamic_state_store(repository.clone())
        .enumerate(256)
        .await
        .expect("enumerate desired state");
    assert_eq!(snapshot.identity().catalog_count(), 2);
    assert_eq!(
        snapshot.mode(),
        CatalogDesiredStateSourceMode::DynamicStateStore
    );
    let exported = snapshot
        .logical_configs()
        .map(|config| {
            (
                config.instance_id().as_str().to_string(),
                config.provider_id().as_str().to_string(),
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(
        exported,
        vec![
            ("catalog.analytics".to_string(), "iceberg".to_string()),
            ("catalog.raw".to_string(), "iceberg".to_string()),
        ]
    );

    drop(port);
    drop(_control);
    drop(repository);
    drop(store);
    shutdown(first_host).await;

    // Restart: a brand new frontend composition over the same durable store.
    let second_host = state_store_fixture::open(cluster).await;
    let store = second_host.state_store().expect("test StateStore");
    let repository =
        CatalogAttachmentRepository::open(Arc::clone(&store), second_host.run_policy())
            .await
            .expect("reopen attachment repository");
    let (_control, port) = port_with(
        CatalogDesiredStateSource::dynamic_state_store(repository.clone()),
        SelectivelyFailingFactory::ready(),
    );
    let restarted = controller(Arc::clone(&store), &port);
    restarted
        .bootstrap()
        .await
        .expect("bootstrap after restart");

    assert_eq!(
        ready_attachment_id(&port, "catalog.analytics").await,
        created.attachment_id,
        "a restart rediscovers the same durable catalog identity"
    );
    let _ = ready_attachment_id(&port, "catalog.raw").await;

    // DROP removes desired state, so the restart after it must not rediscover
    // the catalog.
    port.drop_catalog(CatalogDropCommand {
        instance_id: catalog("catalog.raw"),
        if_exists: false,
    })
    .expect("DROP CATALOG");
    restarted.bootstrap().await.expect("bootstrap after drop");
    assert!(matches!(
        port.admit_catalog(&catalog("catalog.raw")),
        CatalogAdmission::Absent
    ));
    let _ = ready_attachment_id(&port, "catalog.analytics").await;

    drop(restarted);
    drop(port);
    drop(_control);
    drop(repository);
    drop(store);
    shutdown(second_host).await;
}

// ---------------------------------------------------------------------------
// 2. An unimplemented mode rejects SQL mutation, and never falls back
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_unimplemented_source_mode_rejects_sql_catalog_mutation_without_falling_back() {
    let host = state_store_fixture::open(format!(
        "catalog-desired-state-mode-reject-{}",
        Uuid::now_v7()
    ))
    .await;
    let store = host.state_store().expect("test StateStore");
    let repository = CatalogAttachmentRepository::open(Arc::clone(&store), host.run_policy())
        .await
        .expect("open attachment repository");

    for mode in [CatalogDesiredStateSourceMode::ManagedController] {
        // Handing the StateStore authority to a non-dynamic mode is the exact
        // shape a silent fallback would take, so the source is built *with* a
        // usable repository and must still refuse.
        let source = CatalogDesiredStateSource::select(mode, Some(repository.clone()))
            .expect("select a frozen source mode");
        assert_eq!(source.mode(), mode);
        assert_eq!(
            source.sql_mutation_admission(),
            CatalogSqlMutationAdmission::Rejected
        );
        assert_eq!(
            source
                .enumerate(256)
                .await
                .expect_err("an unimplemented mode must not enumerate")
                .kind(),
            CatalogApplicationErrorKind::UnsupportedSourceMode,
            "an unimplemented mode must not serve the dynamic StateStore's records"
        );

        let (_control, port) = port_with(source, SelectivelyFailingFactory::ready());
        assert_eq!(
            port.create_catalog(create_command("catalog.analytics"))
                .expect_err("CREATE CATALOG must be rejected")
                .kind(),
            CatalogApplicationErrorKind::UnsupportedSourceMode
        );
        assert_eq!(
            port.drop_catalog(CatalogDropCommand {
                instance_id: catalog("catalog.analytics"),
                if_exists: true,
            })
            .expect_err("DROP CATALOG must be rejected even with IF EXISTS")
            .kind(),
            CatalogApplicationErrorKind::UnsupportedSourceMode
        );
        assert!(
            repository
                .list_with_page_size(256)
                .await
                .expect("list attachments")
                .is_empty(),
            "a rejected mutation must not have written desired state anywhere"
        );

        drop(port);
        drop(_control);
    }

    drop(repository);
    drop(store);
    shutdown(host).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn selecting_an_unimplemented_source_mode_fails_before_any_startup_side_effect() {
    let registry = state_store_fixture::registry();
    for mode in [CatalogDesiredStateSourceMode::ManagedController] {
        // No StateStore input at all: if the rejection depended on anything the
        // frontend opens, this could not fail here.
        let error = FrontendApplicationHost::open_with_role_factories_and_state_store_registry(
            None,
            &registry,
            FrontendExecutionConfig::new(
                "127.0.0.1",
                19090,
                std::num::NonZeroUsize::new(1).expect("one worker"),
                novarocks_types::NativeCompatibilityId::new([0x71; 32]),
                std::sync::Arc::new(
                    novarocks_sql::compiler::build_builtin_engine_function_catalog()
                        .expect("builtin function catalog"),
                ),
            )
            .with_catalog_desired_state_source(
                CatalogDesiredStateSourceInput::ManagedControllerUnsupported,
            ),
            ClusterBackendOpenConfig::new(
                novarocks_types::ClusterRole::Fe,
                novarocks_types::NativeCompatibilityId::new([0x71; 32]),
                Duration::from_secs(1),
                1,
                Duration::from_secs(1),
            )
            .expect("valid FE backend config"),
            Vec::new(),
            tokio::runtime::Handle::current(),
            Arc::new(NativeTrust::new(
                DeploymentId::parse("catalog-desired-state-test").expect("deployment"),
                ValidatedSharedSecret::new(SecretValue::new("0123456789abcdef0123456789abcdef"))
                    .expect("secret"),
                NativeCallerSubject::parse("fe@127.0.0.1:19040").expect("subject"),
                NativeTransportMode::Disabled,
            )),
            FrontendNativeTransport::plaintext(),
        )
        .await
        .err()
        .expect("an unimplemented catalog source mode must not open a frontend");
        assert_eq!(
            error.kind(),
            FrontendApplicationErrorKind::CatalogApplicationServiceOpen
        );
        assert!(
            error.to_string().contains(mode.as_str()),
            "the rejection must name the mode it refused: {error}"
        );
    }
}

// ---------------------------------------------------------------------------
// 3. An incomplete enumeration is a global failure, never an empty snapshot
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_incomplete_enumeration_blocks_bootstrap_instead_of_becoming_an_empty_snapshot() {
    let host = state_store_fixture::open(format!(
        "catalog-desired-state-enumeration-{}",
        Uuid::now_v7()
    ))
    .await;
    let inner = host.state_store().expect("test StateStore");
    let scanning = Arc::new(ScanFailingStore {
        inner: Arc::clone(&inner),
        scans_available: AtomicBool::new(true),
    });
    let store = Arc::clone(&scanning) as Arc<dyn StateStore>;
    let repository = CatalogAttachmentRepository::open(Arc::clone(&store), host.run_policy())
        .await
        .expect("open attachment repository");
    let (_control, port) = port_with(
        CatalogDesiredStateSource::dynamic_state_store(repository.clone()),
        SelectivelyFailingFactory::ready(),
    );
    let catalog_controller = controller(Arc::clone(&store), &port);

    port.create_catalog(create_command("catalog.analytics"))
        .expect("CREATE CATALOG");
    catalog_controller
        .bootstrap()
        .await
        .expect("bootstrap with a healthy source");
    let admitted = ready_attachment_id(&port, "catalog.analytics").await;

    // From here the source can no longer be enumerated. The records are all
    // still present — only the scan fails.
    scanning.scans_available.store(false, Ordering::Release);

    let error = CatalogDesiredStateSource::dynamic_state_store(repository.clone())
        .enumerate(256)
        .await
        .expect_err("a scan that cannot complete must not produce a snapshot");
    assert_eq!(
        error.kind(),
        CatalogApplicationErrorKind::DesiredStateEnumerationIncomplete,
        "an unreadable source must be typed as an incomplete enumeration, \
         never as a snapshot that happens to hold zero catalogs: {error}"
    );

    catalog_controller
        .bootstrap()
        .await
        .expect_err("an incomplete enumeration must block frontend bootstrap");

    // A cold frontend has no local projection to preserve. It must still fail
    // closed before scheduling any provider work from the incomplete source.
    let (cold_control, cold_port) = port_with(
        CatalogDesiredStateSource::dynamic_state_store(repository.clone()),
        SelectivelyFailingFactory::ready(),
    );
    controller(Arc::clone(&store), &cold_port)
        .bootstrap()
        .await
        .expect_err("an incomplete enumeration must block a cold frontend too");
    assert!(
        matches!(
            cold_port.admit_catalog(&catalog("catalog.analytics")),
            CatalogAdmission::Absent
        ),
        "an incomplete enumeration must not schedule a partial projection on a cold frontend"
    );

    assert_eq!(
        ready_attachment_id(&port, "catalog.analytics").await,
        admitted,
        "an enumeration failure proves nothing about desired state, so it must \
         not retire the catalogs a working enumeration had found"
    );

    drop(catalog_controller);
    drop(cold_port);
    drop(cold_control);
    drop(port);
    drop(_control);
    drop(repository);
    drop(store);
    drop(scanning);
    drop(inner);
    shutdown(host).await;
}

// ---------------------------------------------------------------------------
// 4. One catalog's provider failure is scoped to that catalog
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn one_catalogs_materialization_failure_leaves_every_other_catalog_serving() {
    let host = state_store_fixture::open(format!(
        "catalog-desired-state-materialize-{}",
        Uuid::now_v7()
    ))
    .await;
    let store = host.state_store().expect("test StateStore");
    let repository = CatalogAttachmentRepository::open(Arc::clone(&store), host.run_policy())
        .await
        .expect("open attachment repository");

    // Both catalogs enter desired state while every provider still works, so
    // the failure below is a materialization failure and not a rejected CREATE.
    let (healthy_control, seeding_port) = port_with(
        CatalogDesiredStateSource::dynamic_state_store(repository.clone()),
        SelectivelyFailingFactory::ready(),
    );
    seeding_port
        .create_catalog(create_command("catalog.analytics"))
        .expect("CREATE healthy catalog");
    seeding_port
        .create_catalog(create_command("catalog.broken"))
        .expect("CREATE catalog that will fail to materialize");
    drop(seeding_port);
    drop(healthy_control);

    let (_control, port) = port_with(
        CatalogDesiredStateSource::dynamic_state_store(repository.clone()),
        SelectivelyFailingFactory::failing_for("catalog.broken"),
    );
    let catalog_controller = controller(Arc::clone(&store), &port);
    catalog_controller
        .bootstrap()
        .await
        .expect("one provider failure must not fail the whole bootstrap");

    let _ = ready_attachment_id(&port, "catalog.analytics").await;
    let broken = port.admit_catalog(&catalog("catalog.broken"));
    assert!(
        matches!(broken, CatalogAdmission::Unavailable { .. }),
        "the failed catalog must be Unavailable, not Absent: {broken:?}"
    );
    // Fail closed: work that depends on the broken catalog is refused rather
    // than being served from a guessed configuration or a stale generation.
    assert_eq!(
        broken
            .require_ready(&catalog("catalog.broken"))
            .expect_err("an unavailable catalog must fail closed")
            .kind(),
        CatalogApplicationErrorKind::Unavailable
    );

    // Retrying needs nothing beyond another successful global enumeration: the
    // same source, re-enumerated against a working provider, is enough.
    let (_retry_control, retry_port) = port_with(
        CatalogDesiredStateSource::dynamic_state_store(repository.clone()),
        SelectivelyFailingFactory::ready(),
    );
    controller(Arc::clone(&store), &retry_port)
        .bootstrap()
        .await
        .expect("retry bootstrap");
    let _ = ready_attachment_id(&retry_port, "catalog.broken").await;
    let _ = ready_attachment_id(&retry_port, "catalog.analytics").await;

    drop(retry_port);
    drop(_retry_control);
    drop(catalog_controller);
    drop(port);
    drop(_control);
    drop(repository);
    drop(store);
    shutdown(host).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bootstrap_submits_63_healthy_catalogs_without_waiting_for_one_hanging_provider() {
    const HANGING: &str = "catalog.hang";
    let host = state_store_fixture::open(format!(
        "catalog-desired-state-bootstrap-liveness-{}",
        Uuid::now_v7()
    ))
    .await;
    let store = host.state_store().expect("test StateStore");
    let repository = CatalogAttachmentRepository::open(Arc::clone(&store), host.run_policy())
        .await
        .expect("open attachment repository");

    // Seed desired state through the normal CREATE path, then rebuild a fresh
    // frontend whose one provider attempt intentionally never completes.
    let (seeding_control, seeding_port) = port_with(
        CatalogDesiredStateSource::dynamic_state_store(repository.clone()),
        SelectivelyFailingFactory::ready(),
    );
    seeding_port
        .create_catalog(create_command(HANGING))
        .expect("seed hanging catalog");
    let healthy = (1..=63)
        .map(|index| format!("catalog.z{index:03}"))
        .collect::<Vec<_>>();
    for name in &healthy {
        seeding_port
            .create_catalog(create_command(name))
            .expect("seed healthy catalog");
    }
    drop(seeding_port);
    drop(seeding_control);

    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let (_control, port) = port_with(
        CatalogDesiredStateSource::dynamic_state_store(repository.clone()),
        SelectivelyFailingFactory::hanging_for(HANGING, Arc::clone(&entered), Arc::clone(&release)),
    );
    let mut projection_config = CatalogProjectionConfig::default();
    projection_config.worker_count = 1;
    let catalog_controller =
        FrontendCatalogController::new(Arc::clone(&store), Arc::clone(&port), projection_config)
            .expect("one-worker catalog controller");

    // The provider is held on an explicit Notify gate. The timeout prevents a
    // regression from hanging the test; it is not used to order successful
    // work or to paper over a race with a sleep.
    tokio::time::timeout(Duration::from_secs(1), catalog_controller.bootstrap())
        .await
        .expect("bootstrap must complete after scheduler ownership")
        .expect("provider failure is local, not a bootstrap failure");
    entered.notified().await;
    for name in &healthy {
        let _ = ready_attachment_id(&port, name).await;
    }

    // Release the fixture before teardown so no background task is left
    // intentionally waiting after the test has observed liveness.
    release.notify_waiters();

    drop(catalog_controller);
    drop(port);
    drop(_control);
    drop(repository);
    drop(store);
    shutdown(host).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bootstrap_enqueues_complete_no_io_snapshots_through_1024_catalogs() {
    // These are the intentional cardinality boundaries for the bootstrap
    // scheduler. The fixture's factory constructs bindings locally, so this
    // characterizes submission/completeness without remote metadata latency.
    for catalog_count in [1, 64, 256, 1024] {
        let host = state_store_fixture::open(format!(
            "catalog-desired-state-bootstrap-scale-{catalog_count}-{}",
            Uuid::now_v7()
        ))
        .await;
        let store = host.state_store().expect("test StateStore");
        let repository = CatalogAttachmentRepository::open(Arc::clone(&store), host.run_policy())
            .await
            .expect("open attachment repository");
        let names = seed_ready_catalogs(&repository, catalog_count).await;
        let (_control, port) = port_with(
            CatalogDesiredStateSource::dynamic_state_store(repository.clone()),
            SelectivelyFailingFactory::ready(),
        );
        let mut projection_config = CatalogProjectionConfig::default();
        projection_config.worker_count = 1;
        let catalog_controller = FrontendCatalogController::new(
            Arc::clone(&store),
            Arc::clone(&port),
            projection_config,
        )
        .expect("one-worker catalog controller");

        catalog_controller
            .bootstrap()
            .await
            .expect("complete no-I/O snapshot must submit successfully");
        for name in &names {
            let _ = ready_attachment_id(&port, name).await;
        }

        drop(catalog_controller);
        drop(port);
        drop(_control);
        drop(repository);
        drop(store);
        shutdown(host).await;
    }
}

// ---------------------------------------------------------------------------
// 5. The snapshot is total truth, not additive seeds
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_catalog_removed_from_the_source_is_not_revived_by_the_next_bootstrap() {
    let cluster = format!("catalog-desired-state-removal-{}", Uuid::now_v7());
    let first_host = state_store_fixture::open(cluster.clone()).await;
    let store = first_host.state_store().expect("test StateStore");
    let repository = CatalogAttachmentRepository::open(Arc::clone(&store), first_host.run_policy())
        .await
        .expect("open attachment repository");
    let (_control, port) = port_with(
        CatalogDesiredStateSource::dynamic_state_store(repository.clone()),
        SelectivelyFailingFactory::ready(),
    );
    port.create_catalog(create_command("catalog.analytics"))
        .expect("CREATE surviving catalog");
    port.create_catalog(create_command("catalog.retired"))
        .expect("CREATE catalog to remove");

    // Removed directly from the source, the way an operator editing a file or a
    // controller withdrawing an entry would remove it — not through SQL.
    let retired = repository
        .get(&catalog("catalog.retired"))
        .await
        .expect("read desired state")
        .expect("the catalog is present before removal");
    repository
        .drop_exact(retired)
        .await
        .expect("remove the catalog from the source");

    drop(port);
    drop(_control);
    drop(repository);
    drop(store);
    shutdown(first_host).await;

    let second_host = state_store_fixture::open(cluster).await;
    let store = second_host.state_store().expect("test StateStore");
    let repository =
        CatalogAttachmentRepository::open(Arc::clone(&store), second_host.run_policy())
            .await
            .expect("reopen attachment repository");
    let (_control, port) = port_with(
        CatalogDesiredStateSource::dynamic_state_store(repository.clone()),
        SelectivelyFailingFactory::ready(),
    );
    controller(Arc::clone(&store), &port)
        .bootstrap()
        .await
        .expect("bootstrap after removal");

    assert!(
        matches!(
            port.admit_catalog(&catalog("catalog.retired")),
            CatalogAdmission::Absent
        ),
        "a snapshot is total truth: a catalog the source no longer declares \
         must not come back as an additive seed"
    );
    let _ = ready_attachment_id(&port, "catalog.analytics").await;

    drop(port);
    drop(_control);
    drop(repository);
    drop(store);
    shutdown(second_host).await;
}
