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

//! `DROP CATALOG` versus the MV Accelerator: an operational check, and a
//! durable delete that does not depend on it.
//!
//! The reference check used to run inside the catalog attachment delete
//! transaction, which read as a cross-family serializability fence. It was not
//! one: the family it scans is a rebuildable Accelerator that may be wiped in
//! whole, and the parties that actually race — MV DDL on another frontend, an
//! external catalog desired-state controller — were never participants in that
//! transaction. This file pins the downgrade honestly in both directions:
//!
//! * an observed reference still refuses the drop, and says in the error that
//!   it is an operational check rather than a guarantee;
//! * a wiped Accelerator, and an Accelerator that cannot be read at all, do
//!   *not* stop the durable delete. That is the behavioural proof the delete
//!   transaction no longer reads MV prefixes, and the honest cost of the
//!   downgrade: a real reference can slip past.

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use common::state_store_fixture;
use novarocks_frontend::catalog_application::desired_state::CatalogDesiredStateSource;
use novarocks_frontend::catalog_application::{
    CatalogAdmission, CatalogApplicationErrorKind, CatalogApplicationPort, CatalogCreateCommand,
    CatalogDropCommand, CatalogRuntimeProjection, FrontendCatalogApplicationPort,
};
use novarocks_frontend::catalog_attachment::{
    CatalogAttachmentRepository, CatalogAttachmentVersioned,
};
use novarocks_frontend::connector::ConnectorControlHost;
use novarocks_frontend::mv::domain::dependency::model::{
    MvDependencyObjectRef, MvDependencyObjectType, MvDependencyStorageEngine,
};
use novarocks_frontend::mv::domain::repository::MvRepository;
use novarocks_frontend::mv::repository::StateStoreMvRepository;
use novarocks_frontend::mv::repository::key::{dependency_by_upstream_key, target_lookup_key};
use novarocks_frontend::state_family::StateFamily;
use novarocks_spi::connector::{
    ConnectorBeginScanRequest, ConnectorControlBinding, ConnectorControlResolver, ConnectorError,
    ConnectorErrorKind, ConnectorExecutionDistribution, ConnectorInstanceDescriptor,
    ConnectorInstanceId, ConnectorListTablesRequest, ConnectorMetadata, ConnectorNamespaceRequest,
    ConnectorProviderBinding, ConnectorProviderId, ConnectorScan, ConnectorScanHandle,
    ConnectorScanPlanning, ConnectorSplitPlanningRequest, ConnectorTableHandle,
    ConnectorTableMetadata, ConnectorTableRequest, ProviderBindingEpoch,
};
use novarocks_state_store_api::{
    ChangePage, ChangePollRequest, CommitOutcome, CommitResolution, Direction, Key, KeyRange,
    Precondition, RangePage, RangeRequest, ReadTransaction, StateRecord, StateStore,
    StateStoreError, StateStoreErrorKind, StateStoreLimits, StateStoreMetricsSnapshot,
    StoreIdentity, TransactionId, Value, WriteTransaction,
};
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Minimal connector control fixture
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

/// Mints a distinct control generation per materialization, like a real
/// provider role factory would: reusing an incarnation trips the retired-generation guard.
struct ReadyFactory {
    incarnations: AtomicU8,
}

impl ReadyFactory {
    fn new() -> Self {
        Self {
            incarnations: AtomicU8::new(0),
        }
    }
}

impl novarocks_spi::connector::ConnectorControlRoleBindingFactory for ReadyFactory {
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

        let incarnation = self.incarnations.fetch_add(1, Ordering::Relaxed) + 1;
        async move {
            let provider = Arc::new(TestControl {
                instance_id: properties.handle().catalog_name().clone(),
                incarnation: ProviderBindingEpoch::from_bytes([incarnation; 16]),
            });
            let binding = ConnectorControlBinding::try_new(
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
// A store whose MV Accelerator prefix cannot be scanned
// ---------------------------------------------------------------------------

/// Range scans that start inside the MV Accelerator prefix fail; every other
/// read, and every write, passes straight through.
///
/// This is the sharp form of "the Accelerator cannot be consulted", and it is
/// deliberately narrower than a broken store: the catalog attachment family
/// stays fully readable and writable, so a failing `DROP CATALOG` here could
/// only mean the delete still depends on reading MV prefixes.
struct AcceleratorUnreadableStore {
    inner: Arc<dyn StateStore>,
}

fn accelerator_prefix_bytes() -> &'static [u8] {
    StateFamily::MvAccelerator
        .persistent_prefix()
        .expect("the MV accelerator is a durable family")
        .as_bytes()
}

struct AcceleratorUnreadableRead {
    inner: Box<dyn ReadTransaction>,
}

#[async_trait::async_trait]
impl ReadTransaction for AcceleratorUnreadableRead {
    async fn get(&mut self, key: &Key) -> Result<Option<StateRecord>, StateStoreError> {
        self.inner.get(key).await
    }

    async fn range(&mut self, request: &RangeRequest) -> Result<RangePage, StateStoreError> {
        if request
            .range
            .start
            .as_bytes()
            .starts_with(accelerator_prefix_bytes())
        {
            return Err(StateStoreError::new(
                StateStoreErrorKind::ProviderUnavailable,
                "injected MV accelerator scan failure",
            ));
        }
        self.inner.range(request).await
    }

    async fn abort(self: Box<Self>) -> Result<(), StateStoreError> {
        self.inner.abort().await
    }
}

#[async_trait::async_trait]
impl StateStore for AcceleratorUnreadableStore {
    fn limits(&self) -> &StateStoreLimits {
        self.inner.limits()
    }

    fn metrics_snapshot(&self) -> StateStoreMetricsSnapshot {
        self.inner.metrics_snapshot()
    }

    async fn begin_read(&self) -> Result<Box<dyn ReadTransaction>, StateStoreError> {
        Ok(Box::new(AcceleratorUnreadableRead {
            inner: self.inner.begin_read().await?,
        }))
    }

    async fn begin_write(
        &self,
        transaction_id: TransactionId,
        purpose: &str,
    ) -> Result<Box<dyn WriteTransaction>, StateStoreError> {
        self.inner.begin_write(transaction_id, purpose).await
    }

    async fn poll_changes(
        &self,
        request: &ChangePollRequest,
    ) -> Result<ChangePage, StateStoreError> {
        self.inner.poll_changes(request).await
    }

    async fn identity(&self) -> Result<StoreIdentity, StateStoreError> {
        self.inner.identity().await
    }

    async fn resolve_commit(
        &self,
        transaction_id: &TransactionId,
    ) -> Result<CommitResolution, StateStoreError> {
        self.inner.resolve_commit(transaction_id).await
    }
}

// ---------------------------------------------------------------------------
// A store that records MV Accelerator scans issued inside write transactions
// ---------------------------------------------------------------------------

/// Counts range requests over the MV Accelerator prefix that are issued on a
/// *write* transaction.
///
/// This separates the two things an empty prefix cannot: the best-effort check
/// reads the same prefixes, but it reads them on its own read transaction, so
/// only a scan inside a write transaction means the attachment delete is still
/// coupled to the MV family. Counting is what makes the wiped-prefix case
/// decisive — an emptied prefix returns "no references" to a transactional
/// fence just as happily as to a check that never ran.
struct WriteScanRecordingStore {
    inner: Arc<dyn StateStore>,
    accelerator_write_scans: Arc<AtomicUsize>,
}

struct RecordingWrite {
    inner: Box<dyn WriteTransaction>,
    accelerator_write_scans: Arc<AtomicUsize>,
}

#[async_trait::async_trait]
impl ReadTransaction for RecordingWrite {
    async fn get(&mut self, key: &Key) -> Result<Option<StateRecord>, StateStoreError> {
        self.inner.get(key).await
    }

    async fn range(&mut self, request: &RangeRequest) -> Result<RangePage, StateStoreError> {
        if request
            .range
            .start
            .as_bytes()
            .starts_with(accelerator_prefix_bytes())
        {
            self.accelerator_write_scans.fetch_add(1, Ordering::Relaxed);
        }
        self.inner.range(request).await
    }

    async fn abort(self: Box<Self>) -> Result<(), StateStoreError> {
        self.inner.abort().await
    }
}

#[async_trait::async_trait]
impl WriteTransaction for RecordingWrite {
    fn transaction_id(&self) -> &TransactionId {
        self.inner.transaction_id()
    }

    async fn put(
        &mut self,
        key: Key,
        value: Value,
        precondition: Precondition,
    ) -> Result<(), StateStoreError> {
        self.inner.put(key, value, precondition).await
    }

    async fn delete(
        &mut self,
        key: Key,
        precondition: Precondition,
    ) -> Result<(), StateStoreError> {
        self.inner.delete(key, precondition).await
    }

    async fn commit(self: Box<Self>) -> CommitOutcome {
        self.inner.commit().await
    }
}

#[async_trait::async_trait]
impl StateStore for WriteScanRecordingStore {
    fn limits(&self) -> &StateStoreLimits {
        self.inner.limits()
    }

    fn metrics_snapshot(&self) -> StateStoreMetricsSnapshot {
        self.inner.metrics_snapshot()
    }

    async fn begin_read(&self) -> Result<Box<dyn ReadTransaction>, StateStoreError> {
        self.inner.begin_read().await
    }

    async fn begin_write(
        &self,
        transaction_id: TransactionId,
        purpose: &str,
    ) -> Result<Box<dyn WriteTransaction>, StateStoreError> {
        Ok(Box::new(RecordingWrite {
            inner: self.inner.begin_write(transaction_id, purpose).await?,
            accelerator_write_scans: Arc::clone(&self.accelerator_write_scans),
        }))
    }

    async fn poll_changes(
        &self,
        request: &ChangePollRequest,
    ) -> Result<ChangePage, StateStoreError> {
        self.inner.poll_changes(request).await
    }

    async fn identity(&self) -> Result<StoreIdentity, StateStoreError> {
        self.inner.identity().await
    }

    async fn resolve_commit(
        &self,
        transaction_id: &TransactionId,
    ) -> Result<CommitResolution, StateStoreError> {
        self.inner.resolve_commit(transaction_id).await
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

fn drop_command(name: &str) -> CatalogDropCommand {
    CatalogDropCommand {
        instance_id: catalog(name),
        if_exists: false,
    }
}

fn port_with(
    source: CatalogDesiredStateSource,
) -> (
    Arc<ConnectorControlHost>,
    Arc<FrontendCatalogApplicationPort>,
) {
    let control = Arc::new(
        ConnectorControlHost::with_role_factories(vec![Arc::new(ReadyFactory::new())])
            .expect("control host"),
    );
    let port = Arc::new(FrontendCatalogApplicationPort::new(
        source,
        Arc::clone(&control),
        CatalogRuntimeProjection::new().publisher(),
        tokio::runtime::Handle::current(),
    ));
    (control, port)
}

/// Writes one MV Accelerator index key naming `name` as an MV target.
async fn seed_target_reference(store: &Arc<dyn StateStore>, name: &str) {
    let key = target_lookup_key(name, "sales", "orders_mv").expect("MV target lookup key");
    put_marker(store, key, "target lookup marker").await;
}

/// Writes one MV Accelerator index key naming `name` as an MV upstream.
async fn seed_dependency_reference(store: &Arc<dyn StateStore>, name: &str) {
    let upstream = MvDependencyObjectRef {
        catalog: Some(name.to_string()),
        database_or_namespace: "sales".to_string(),
        name: "orders".to_string(),
        object_type: MvDependencyObjectType::Table,
        storage_engine: MvDependencyStorageEngine::Iceberg,
    };
    let key = dependency_by_upstream_key(&upstream, 1).expect("MV upstream dependency key");
    put_marker(store, key, "dependency index marker").await;
}

async fn put_marker(store: &Arc<dyn StateStore>, key: Key, purpose: &str) {
    let mut transaction = store
        .begin_write(TransactionId::from(Uuid::now_v7()), purpose)
        .await
        .expect("begin seed transaction");
    transaction
        .put(
            key,
            Bytes::copy_from_slice(purpose.as_bytes())
                .try_into()
                .expect("StateStore value"),
            Precondition::Absent,
        )
        .await
        .expect("write MV accelerator index marker");
    assert!(matches!(
        transaction.commit().await,
        CommitOutcome::Committed(_)
    ));
}

async fn accelerator_key_count(store: &Arc<dyn StateStore>) -> usize {
    let range = KeyRange::for_prefix(
        StateFamily::MvAccelerator
            .persistent_prefix()
            .expect("the MV accelerator is a durable family")
            .key()
            .expect("accelerator prefix key"),
    )
    .expect("accelerator prefix range");
    let mut read = store.begin_read().await.expect("begin read transaction");
    let mut count = 0;
    let mut continuation = None;
    loop {
        let page = read
            .range(&RangeRequest {
                range: range.clone(),
                direction: Direction::Forward,
                page_size: store.limits().max_page_size.min(256),
                continuation,
            })
            .await
            .expect("scan the accelerator prefix");
        count += page.records.len();
        continuation = page.continuation;
        if continuation.is_none() {
            break;
        }
    }
    read.abort().await.expect("abort read transaction");
    count
}

async fn attachment(
    repository: &CatalogAttachmentRepository,
    name: &str,
) -> Option<CatalogAttachmentVersioned> {
    repository
        .get(&catalog(name))
        .await
        .expect("read catalog attachment")
}

async fn shutdown(mut host: novarocks_frontend::StateStoreHost) {
    host.shutdown(Instant::now() + Duration::from_secs(5))
        .await
        .expect("state store shutdown");
}

// ---------------------------------------------------------------------------
// 1. An observed reference still refuses the drop, as an operational check
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_observed_materialized_view_reference_refuses_the_drop_as_an_operational_check() {
    let host =
        state_store_fixture::open(format!("catalog-drop-guard-refuse-{}", Uuid::now_v7())).await;
    let store = host.state_store().expect("test StateStore");
    let repository = CatalogAttachmentRepository::open(Arc::clone(&store))
        .await
        .expect("open attachment repository");
    let (_control, port) = port_with(CatalogDesiredStateSource::dynamic_state_store(
        repository.clone(),
    ));
    port.create_catalog(create_command("catalog.analytics"))
        .expect("CREATE CATALOG");
    let before = attachment(&repository, "catalog.analytics")
        .await
        .expect("the attachment exists before the refused drop");

    seed_dependency_reference(&store, "catalog.analytics").await;

    let error = port
        .drop_catalog(drop_command("catalog.analytics"))
        .expect_err("a referenced catalog must not drop");
    assert_eq!(error.kind(), CatalogApplicationErrorKind::Conflict);
    // The wording is load-bearing: an operator reading this must not conclude
    // that the engine serializes DROP CATALOG against MV DDL.
    assert!(
        error.to_string().contains("best-effort operational check"),
        "the refusal must name itself an operational check: {error}"
    );
    assert!(
        error
            .to_string()
            .contains("not a cross-system serializability guarantee"),
        "the refusal must disclaim the guarantee it no longer provides: {error}"
    );
    assert!(
        error.to_string().contains("catalog.analytics"),
        "the refusal must name the catalog it refused: {error}"
    );

    assert_eq!(
        attachment(&repository, "catalog.analytics").await,
        Some(before),
        "a refused drop must leave the durable attachment record byte-identical"
    );
    assert!(
        matches!(
            port.admit_catalog(&catalog("catalog.analytics")),
            CatalogAdmission::Ready(_)
        ),
        "a refused drop must not retire the local projection either"
    );

    drop(port);
    drop(_control);
    drop(repository);
    drop(store);
    shutdown(host).await;
}

// ---------------------------------------------------------------------------
// 2. A wiped Accelerator does not stop the durable delete
// ---------------------------------------------------------------------------

/// The decisive case for the downgrade. The MV Accelerator is a rebuildable
/// family whose declared wipe entry deletes its whole prefix, so a `DROP
/// CATALOG` that needs those prefixes readable is a delete that stops working
/// after a legitimate wipe. Here the references are real first, then the
/// family is wiped through its own destructive entry point, and the drop must
/// still commit as a single-family transaction.
///
/// The scan counter is what makes an empty prefix prove something: an emptied
/// prefix answers "no references" to a transactional fence exactly as it does
/// to a check that no longer runs there, so success alone would not separate
/// the two. Zero MV scans inside the delete's write transaction does.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_entirely_wiped_accelerator_still_lets_the_catalog_drop() {
    let host =
        state_store_fixture::open(format!("catalog-drop-guard-wiped-{}", Uuid::now_v7())).await;
    let inner = host.state_store().expect("test StateStore");
    let accelerator_write_scans = Arc::new(AtomicUsize::new(0));
    let store = Arc::new(WriteScanRecordingStore {
        inner: Arc::clone(&inner),
        accelerator_write_scans: Arc::clone(&accelerator_write_scans),
    }) as Arc<dyn StateStore>;
    let repository = CatalogAttachmentRepository::open(Arc::clone(&store))
        .await
        .expect("open attachment repository");
    let (_control, port) = port_with(CatalogDesiredStateSource::dynamic_state_store(
        repository.clone(),
    ));
    port.create_catalog(create_command("catalog.analytics"))
        .expect("CREATE CATALOG");

    // Both index shapes, so the wipe below removes content the check really
    // would have observed rather than an already-empty prefix.
    seed_target_reference(&store, "catalog.analytics").await;
    seed_dependency_reference(&store, "catalog.analytics").await;
    assert_eq!(accelerator_key_count(&store).await, 2);
    assert_eq!(
        port.drop_catalog(drop_command("catalog.analytics"))
            .expect_err("while the references are observable the drop is refused")
            .kind(),
        CatalogApplicationErrorKind::Conflict
    );

    let mv_repository =
        StateStoreMvRepository::open(Arc::clone(&store), tokio::runtime::Handle::current())
            .await
            .expect("open MV accelerator repository");
    mv_repository
        .wipe_accelerator(Uuid::now_v7())
        .expect("wipe the whole MV accelerator family");
    assert_eq!(
        accelerator_key_count(&store).await,
        0,
        "the wipe must leave no key under the accelerator prefix"
    );

    let scans_before_drop = accelerator_write_scans.load(Ordering::Relaxed);
    port.drop_catalog(drop_command("catalog.analytics"))
        .expect("a wiped accelerator must not block the durable delete");
    assert_eq!(
        accelerator_write_scans.load(Ordering::Relaxed),
        scans_before_drop,
        "the attachment delete transaction must not scan any MV prefix"
    );
    assert_eq!(
        attachment(&repository, "catalog.analytics").await,
        None,
        "the durable delete is authoritative"
    );
    assert!(matches!(
        port.admit_catalog(&catalog("catalog.analytics")),
        CatalogAdmission::Absent
    ));

    drop(mv_repository);
    drop(port);
    drop(_control);
    drop(repository);
    drop(store);
    drop(inner);
    shutdown(host).await;
}

// ---------------------------------------------------------------------------
// 3. An unreadable Accelerator does not stop the durable delete either
// ---------------------------------------------------------------------------

/// The same property from the other side, and the honest cost of the
/// downgrade: the reference here is present and real, the Accelerator simply
/// cannot be scanned, so the check produces no observation and the catalog
/// drops out from under a live MV reference. That is the accepted bound — the
/// MV then names a catalog that is gone, which its own unavailable/fail-closed
/// paths refuse; it is not a wrong lake publication.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_unreadable_accelerator_does_not_block_the_durable_delete() {
    let host =
        state_store_fixture::open(format!("catalog-drop-guard-unreadable-{}", Uuid::now_v7()))
            .await;
    let inner = host.state_store().expect("test StateStore");
    seed_target_reference(&inner, "catalog.analytics").await;

    let store = Arc::new(AcceleratorUnreadableStore {
        inner: Arc::clone(&inner),
    }) as Arc<dyn StateStore>;
    let repository = CatalogAttachmentRepository::open(Arc::clone(&store))
        .await
        .expect("open attachment repository");
    let (_control, port) = port_with(CatalogDesiredStateSource::dynamic_state_store(
        repository.clone(),
    ));
    port.create_catalog(create_command("catalog.analytics"))
        .expect("CREATE CATALOG");

    port.drop_catalog(drop_command("catalog.analytics"))
        .expect("an unreadable accelerator must not block the durable delete");
    assert_eq!(
        attachment(&repository, "catalog.analytics").await,
        None,
        "the durable delete is authoritative"
    );
    assert_eq!(
        accelerator_key_count(&inner).await,
        1,
        "the delete transaction must not have touched the MV family at all"
    );

    drop(port);
    drop(_control);
    drop(repository);
    drop(store);
    drop(inner);
    shutdown(host).await;
}

// ---------------------------------------------------------------------------
// 4. An unreferenced catalog drops, and its projection is retired
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_unreferenced_catalog_drops_and_retires_its_projection() {
    let host =
        state_store_fixture::open(format!("catalog-drop-guard-plain-{}", Uuid::now_v7())).await;
    let store = host.state_store().expect("test StateStore");
    let repository = CatalogAttachmentRepository::open(Arc::clone(&store))
        .await
        .expect("open attachment repository");
    let (control, port) = port_with(CatalogDesiredStateSource::dynamic_state_store(
        repository.clone(),
    ));
    port.create_catalog(create_command("catalog.analytics"))
        .expect("CREATE CATALOG");
    port.create_catalog(create_command("catalog.raw"))
        .expect("CREATE CATALOG");
    // A reference to a *different* catalog must not be read as a reference to
    // this one: the check is scoped to the exact catalog identity.
    seed_dependency_reference(&store, "catalog.raw").await;

    port.drop_catalog(drop_command("catalog.analytics"))
        .expect("an unreferenced catalog drops");

    assert_eq!(
        attachment(&repository, "catalog.analytics").await,
        None,
        "the durable record is deleted"
    );
    assert!(matches!(
        port.admit_catalog(&catalog("catalog.analytics")),
        CatalogAdmission::Absent
    ));
    assert!(
        control
            .observe_current_binding(&catalog("catalog.analytics"))
            .is_err(),
        "the local Connector control projection is retired after the durable delete"
    );
    // The still-referenced catalog is untouched by its neighbour's drop.
    assert!(matches!(
        port.admit_catalog(&catalog("catalog.raw")),
        CatalogAdmission::Ready(_)
    ));

    drop(port);
    drop(control);
    drop(repository);
    drop(store);
    shutdown(host).await;
}
