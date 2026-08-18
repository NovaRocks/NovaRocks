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

//! StateStore change-hint controller for local catalog runtime projections.
//!
//! Change pages are intentionally only wakeups. Every relevant hint and every
//! retention gap triggers a complete authoritative attachment reread.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use novarocks_spi::state_store::{ChangeCursor, ChangePollRequest, StateStore, StoreIdentity};
use tokio::task::JoinHandle;

use crate::catalog_application::FrontendCatalogApplicationPort;
use crate::catalog_attachment::attachment_prefix;

#[derive(Default)]
struct CatalogProjectionMetrics {
    successful_polls: AtomicU64,
    failed_polls: AtomicU64,
    resyncs: AtomicU64,
    freshness_expiries: AtomicU64,
}

#[derive(Clone, Debug)]
pub struct CatalogProjectionConfig {
    pub page_size: usize,
    pub poll_interval: Duration,
    pub freshness_budget: Duration,
    pub retry_initial: Duration,
    pub retry_max: Duration,
    pub worker_count: usize,
    pub shutdown_deadline: Duration,
}

impl Default for CatalogProjectionConfig {
    fn default() -> Self {
        Self {
            page_size: 256,
            poll_interval: Duration::from_millis(250),
            freshness_budget: Duration::from_secs(30),
            retry_initial: Duration::from_millis(100),
            retry_max: Duration::from_secs(5),
            worker_count: 8,
            shutdown_deadline: Duration::from_secs(5),
        }
    }
}

pub struct FrontendCatalogController {
    store: Arc<dyn StateStore>,
    projection: Arc<FrontendCatalogApplicationPort>,
    config: CatalogProjectionConfig,
    stopping: AtomicBool,
    bootstrap_state: Mutex<Option<(StoreIdentity, ChangeCursor)>>,
    metrics: CatalogProjectionMetrics,
    worker: Mutex<Option<JoinHandle<()>>>,
}

impl FrontendCatalogController {
    pub fn new(
        store: Arc<dyn StateStore>,
        projection: Arc<FrontendCatalogApplicationPort>,
        config: CatalogProjectionConfig,
    ) -> Result<Arc<Self>, String> {
        if config.page_size == 0 || config.page_size > store.limits().max_page_size {
            return Err("catalog controller page size is outside StateStore limits".to_string());
        }
        if config.poll_interval.is_zero()
            || config.freshness_budget.is_zero()
            || config.retry_initial.is_zero()
            || config.retry_max < config.retry_initial
            || config.worker_count == 0
            || config.shutdown_deadline.is_zero()
        {
            return Err(
                "catalog controller config contains an invalid zero or retry bound".to_string(),
            );
        }
        Ok(Arc::new(Self {
            store,
            projection,
            config,
            stopping: AtomicBool::new(false),
            bootstrap_state: Mutex::new(None),
            metrics: CatalogProjectionMetrics::default(),
            worker: Mutex::new(None),
        }))
    }

    /// Captures a polling HWM before the first authoritative attachment scan.
    pub async fn bootstrap(&self) -> Result<ChangeCursor, String> {
        let identity = self
            .store
            .identity()
            .await
            .map_err(|error| error.to_string())?;
        let page = self
            .store
            .poll_changes(&ChangePollRequest {
                after: None,
                page_size: self.config.page_size,
            })
            .await
            .map_err(|error| error.to_string())?;
        self.projection
            .reconcile_with_page_size(self.config.page_size, self.config.worker_count)
            .await
            .map_err(|error| error.to_string())?;
        self.metrics.resyncs.fetch_add(1, Ordering::Relaxed);
        self.publish_metrics();
        let cursor = ChangeCursor::new(identity.store_id, page.high_watermark, u32::MAX)
            .map_err(|error| error.to_string())?;
        cursor
            .decode(identity.store_id)
            .map_err(|error| error.to_string())?;
        *self
            .bootstrap_state
            .lock()
            .map_err(|_| "catalog controller bootstrap state lock is poisoned".to_string())? =
            Some((identity, cursor.clone()));
        Ok(cursor)
    }

    pub fn start(self: &Arc<Self>) -> Result<(), String> {
        let mut worker = self
            .worker
            .lock()
            .map_err(|_| "catalog controller worker lock is poisoned".to_string())?;
        if worker.is_some() {
            return Err("catalog controller is already running".to_string());
        }
        self.stopping.store(false, Ordering::Release);
        let controller = Arc::clone(self);
        *worker = Some(tokio::spawn(async move {
            controller.run().await;
        }));
        Ok(())
    }

    pub async fn shutdown(&self) -> Result<(), String> {
        self.stopping.store(true, Ordering::Release);
        let handle = self
            .worker
            .lock()
            .map_err(|_| "catalog controller worker lock is poisoned".to_string())?
            .take();
        if let Some(mut handle) = handle {
            if tokio::time::timeout(self.config.shutdown_deadline, &mut handle)
                .await
                .is_err()
            {
                handle.abort();
                let _ = handle.await;
            }
        }
        self.projection.unpublish_all();
        self.publish_metrics();
        Ok(())
    }

    pub fn metrics_snapshot(&self) -> crate::catalog_application::CatalogProjectionMetricsSnapshot {
        crate::catalog_application::CatalogProjectionMetricsSnapshot {
            projected_catalogs: self.projection.projection_count(),
            successful_polls: self.metrics.successful_polls.load(Ordering::Relaxed),
            failed_polls: self.metrics.failed_polls.load(Ordering::Relaxed),
            resyncs: self.metrics.resyncs.load(Ordering::Relaxed),
            freshness_expiries: self.metrics.freshness_expiries.load(Ordering::Relaxed),
        }
    }

    fn publish_metrics(&self) {
        crate::catalog_application::publish_catalog_projection_metrics(self.metrics_snapshot());
    }

    async fn run(&self) {
        let bootstrap = match self.bootstrap_state.lock() {
            Ok(mut state) => state.take(),
            Err(_) => {
                tracing::warn!("catalog controller bootstrap state lock is poisoned");
                None
            }
        };
        let (mut identity, mut cursor) = bootstrap.map_or((None, None), |(identity, cursor)| {
            (Some(identity), Some(cursor))
        });
        let mut last_fresh = Instant::now();
        let mut retry = self.config.retry_initial;
        let mut force_resync = identity.is_none();
        let mut fail_closed = false;

        while !self.stopping.load(Ordering::Acquire) {
            let outcome = self
                .poll_once(&mut identity, &mut cursor, &mut force_resync)
                .await;
            match outcome {
                Ok(()) => {
                    last_fresh = Instant::now();
                    retry = self.config.retry_initial;
                    fail_closed = false;
                    self.metrics
                        .successful_polls
                        .fetch_add(1, Ordering::Relaxed);
                    self.publish_metrics();
                    tokio::time::sleep(self.config.poll_interval).await;
                }
                Err(error) => {
                    tracing::warn!(%error, "catalog attachment projection poll failed");
                    self.metrics.failed_polls.fetch_add(1, Ordering::Relaxed);
                    if !fail_closed && last_fresh.elapsed() >= self.config.freshness_budget {
                        self.projection.unpublish_all();
                        self.metrics
                            .freshness_expiries
                            .fetch_add(1, Ordering::Relaxed);
                        force_resync = true;
                        fail_closed = true;
                    }
                    self.publish_metrics();
                    tokio::time::sleep(retry).await;
                    retry = retry.saturating_mul(2).min(self.config.retry_max);
                }
            }
        }
    }

    async fn poll_once(
        &self,
        known_identity: &mut Option<StoreIdentity>,
        cursor: &mut Option<ChangeCursor>,
        force_resync: &mut bool,
    ) -> Result<(), String> {
        let identity = self
            .store
            .identity()
            .await
            .map_err(|error| error.to_string())?;
        if known_identity.as_ref() != Some(&identity) {
            *known_identity = Some(identity.clone());
            *cursor = None;
            *force_resync = true;
        }
        let page = self
            .store
            .poll_changes(&ChangePollRequest {
                after: cursor.clone(),
                page_size: self.config.page_size,
            })
            .await
            .map_err(|error| error.to_string())?;
        page.next_cursor
            .decode(identity.store_id)
            .map_err(|error| error.to_string())?;
        *cursor = Some(page.next_cursor);

        let prefix = attachment_prefix()?;
        let relevant = page
            .hints
            .iter()
            .any(|hint| hint.key.as_bytes().starts_with(prefix.as_bytes()));
        if *force_resync || page.resync_required || relevant {
            self.projection
                .reconcile_with_page_size(self.config.page_size, self.config.worker_count)
                .await
                .map_err(|error| error.to_string())?;
            self.metrics.resyncs.fetch_add(1, Ordering::Relaxed);
            self.publish_metrics();
            *force_resync = false;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;
    use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};

    use crate::catalog_application::{
        CatalogAdmission, CatalogApplicationError, CatalogApplicationErrorKind,
        CatalogApplicationPort, CatalogCreateCommand, CatalogDropCommand,
        CatalogRuntimeObservation, CatalogRuntimePublisherSink,
    };
    use bytes::Bytes;
    use novarocks_spi::connector::{
        ConnectorControlCreation, ConnectorControlFactory, ConnectorControlFactoryRequest,
        ConnectorControlResolver, ConnectorError, ConnectorProviderId,
    };
    use novarocks_spi::state_store::{
        ChangePage, ChangePollRequest, CommitResolution, FeDeploymentView, ReadTransaction,
        StateStore, StateStoreError, StateStoreErrorKind, StateStoreLimits,
        StateStoreMetricsSnapshot, StoreIdentity, TransactionId, WriteTransaction,
        conformance::FaultInjectingStateStore,
    };
    use novarocks_state_store::{
        SQLITE_STATE_STORE_PROVIDER_ID, StateStoreAppConfig, StateStoreConfig, StateStoreHost,
        StateStoreHostConfig, StateStoreLimitOverrides, StateStoreProviderConfig,
        builtin_state_store_provider_registry,
    };

    use super::*;
    use crate::catalog_attachment::{CatalogAttachment, CatalogAttachmentRepository};
    use crate::connector::ConnectorControlHost;

    /// Mints a distinct control generation per creation, like a real provider
    /// factory: reusing an incarnation would trip the retired-generation guard
    /// on a same-name recreate.
    #[derive(Default)]
    struct ReadyFactory {
        incarnations: AtomicU8,
    }

    /// Fails local materialization without touching durable truth, so a test
    /// can observe the durable-first ordering of CREATE.
    struct UnavailableFactory;

    struct RejectingPublisher;

    impl CatalogRuntimePublisherSink for RejectingPublisher {
        fn publish_catalog_runtime(
            &self,
            _observation: CatalogRuntimeObservation,
        ) -> Result<(), CatalogApplicationError> {
            Err(CatalogApplicationError::new(
                CatalogApplicationErrorKind::Unavailable,
                "injected query runtime publication failure",
            ))
        }

        fn unpublish_catalog_runtime(
            &self,
            _instance_id: &novarocks_spi::connector::ConnectorInstanceId,
            _generation: u64,
        ) -> Result<(), CatalogApplicationError> {
            Ok(())
        }
    }

    struct PollUnavailableStore {
        inner: Arc<dyn StateStore>,
    }

    struct ToggleReadStore {
        inner: Arc<dyn StateStore>,
        reads_available: AtomicBool,
    }

    struct IdentityChangedStore {
        inner: Arc<dyn StateStore>,
        identity: StoreIdentity,
        page: ChangePage,
    }

    #[async_trait::async_trait]
    impl StateStore for PollUnavailableStore {
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
            self.inner.begin_write(transaction_id, purpose).await
        }

        async fn poll_changes(
            &self,
            _request: &ChangePollRequest,
        ) -> Result<ChangePage, StateStoreError> {
            Err(StateStoreError::new(
                StateStoreErrorKind::ProviderUnavailable,
                "injected catalog controller outage",
            ))
        }

        async fn identity(
            &self,
        ) -> Result<novarocks_spi::state_store::StoreIdentity, StateStoreError> {
            self.inner.identity().await
        }

        async fn resolve_commit(
            &self,
            transaction_id: &TransactionId,
        ) -> Result<CommitResolution, StateStoreError> {
            self.inner.resolve_commit(transaction_id).await
        }
    }

    #[async_trait::async_trait]
    impl StateStore for ToggleReadStore {
        fn limits(&self) -> &StateStoreLimits {
            self.inner.limits()
        }

        fn metrics_snapshot(&self) -> StateStoreMetricsSnapshot {
            self.inner.metrics_snapshot()
        }

        async fn begin_read(&self) -> Result<Box<dyn ReadTransaction>, StateStoreError> {
            if !self.reads_available.load(Ordering::Acquire) {
                return Err(StateStoreError::new(
                    StateStoreErrorKind::ProviderUnavailable,
                    "injected catalog admission read failure",
                ));
            }
            self.inner.begin_read().await
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

    #[async_trait::async_trait]
    impl StateStore for IdentityChangedStore {
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
            self.inner.begin_write(transaction_id, purpose).await
        }

        async fn poll_changes(
            &self,
            _request: &ChangePollRequest,
        ) -> Result<ChangePage, StateStoreError> {
            Ok(self.page.clone())
        }

        async fn identity(&self) -> Result<StoreIdentity, StateStoreError> {
            Ok(self.identity.clone())
        }

        async fn resolve_commit(
            &self,
            transaction_id: &TransactionId,
        ) -> Result<CommitResolution, StateStoreError> {
            self.inner.resolve_commit(transaction_id).await
        }
    }

    impl ConnectorControlFactory for ReadyFactory {
        fn provider_id(&self) -> &ConnectorProviderId {
            static PROVIDER: std::sync::OnceLock<ConnectorProviderId> = std::sync::OnceLock::new();
            PROVIDER.get_or_init(|| ConnectorProviderId::parse("iceberg").expect("provider ID"))
        }

        fn create_control(
            &self,
            request: ConnectorControlFactoryRequest,
        ) -> Result<ConnectorControlCreation, ConnectorError> {
            let incarnation = self.incarnations.fetch_add(1, Ordering::Relaxed) + 1;
            ConnectorControlCreation::try_new(
                &request,
                crate::connector::control_host::tests::test_control_binding_for(
                    request.instance_id().clone(),
                    incarnation,
                ),
                Vec::new(),
            )
        }
    }

    impl ConnectorControlFactory for UnavailableFactory {
        fn provider_id(&self) -> &ConnectorProviderId {
            static PROVIDER: std::sync::OnceLock<ConnectorProviderId> = std::sync::OnceLock::new();
            PROVIDER.get_or_init(|| ConnectorProviderId::parse("iceberg").expect("provider ID"))
        }

        fn create_control(
            &self,
            _request: ConnectorControlFactoryRequest,
        ) -> Result<ConnectorControlCreation, ConnectorError> {
            Err(ConnectorError::new(
                novarocks_spi::connector::ConnectorErrorKind::Unavailable,
                "injected provider materialization failure",
            ))
        }
    }

    /// A store whose range scan silently omits every attachment, modelling a
    /// scan that read before a concurrent CREATE committed. Targeted `get`
    /// reads are untouched, exactly as a real snapshot boundary behaves.
    struct ScanMissesAttachmentsStore {
        inner: Arc<dyn StateStore>,
        hide_from_scan: AtomicBool,
    }

    struct ScanMissesAttachmentsRead {
        inner: Box<dyn ReadTransaction>,
        hide_from_scan: bool,
    }

    #[async_trait::async_trait]
    impl ReadTransaction for ScanMissesAttachmentsRead {
        async fn get(
            &mut self,
            key: &novarocks_spi::state_store::Key,
        ) -> Result<Option<novarocks_spi::state_store::StateRecord>, StateStoreError> {
            self.inner.get(key).await
        }

        async fn range(
            &mut self,
            request: &novarocks_spi::state_store::RangeRequest,
        ) -> Result<novarocks_spi::state_store::RangePage, StateStoreError> {
            let mut page = self.inner.range(request).await?;
            if self.hide_from_scan {
                page.records.clear();
            }
            Ok(page)
        }

        async fn abort(self: Box<Self>) -> Result<(), StateStoreError> {
            self.inner.abort().await
        }
    }

    #[async_trait::async_trait]
    impl StateStore for ScanMissesAttachmentsStore {
        fn limits(&self) -> &StateStoreLimits {
            self.inner.limits()
        }

        fn metrics_snapshot(&self) -> StateStoreMetricsSnapshot {
            self.inner.metrics_snapshot()
        }

        async fn begin_read(&self) -> Result<Box<dyn ReadTransaction>, StateStoreError> {
            Ok(Box::new(ScanMissesAttachmentsRead {
                inner: self.inner.begin_read().await?,
                hide_from_scan: self.hide_from_scan.load(Ordering::Acquire),
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

    /// A reconcile whose scan began before a CREATE committed must not retire
    /// that catalog's projection.
    ///
    /// `create_catalog` commits the attachment and only then installs the
    /// projection, so a projection can exist while the in-flight scan has not
    /// observed its attachment. Treating "absent from the scan" as proof the
    /// attachment is gone made the statement right after
    /// CREATE EXTERNAL CATALOG fail with "unknown catalog" whenever a reconcile
    /// cycle straddled it — a create that reported success.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_reconcile_scan_that_missed_a_fresh_create_keeps_its_projection() {
        let (_directory, mut host, inner) = open_store().await;
        let scanning = Arc::new(ScanMissesAttachmentsStore {
            inner: Arc::clone(&inner),
            hide_from_scan: AtomicBool::new(false),
        });
        let store = Arc::clone(&scanning) as Arc<dyn StateStore>;
        let repository = CatalogAttachmentRepository::open(Arc::clone(&store))
            .await
            .expect("open attachment repository");
        let (_control, port) = projection(repository);

        let command = create_command(false);
        let instance_id = command.instance_id.clone();
        port.create_catalog(command).expect("create catalog");
        assert!(
            matches!(port.admit_catalog(&instance_id), CatalogAdmission::Ready(_)),
            "CREATE installs the projection synchronously"
        );

        // From here the scan behaves as though it read before the commit.
        scanning.hide_from_scan.store(true, Ordering::Release);
        port.reconcile_with_page_size(256, 1)
            .await
            .expect("reconcile");

        match port.admit_catalog(&instance_id) {
            CatalogAdmission::Ready(_) => {}
            other => panic!(
                "a scan that did not observe the attachment is not proof it is gone;                  retiring on that alone is what surfaces as `unknown catalog`: {other:?}"
            ),
        }

        // A genuinely dropped attachment is still retired: the targeted reread
        // is what distinguishes the two, so it must not blanket-preserve.
        scanning.hide_from_scan.store(false, Ordering::Release);
        port.drop_catalog(CatalogDropCommand {
            instance_id: instance_id.clone(),
            if_exists: false,
        })
        .expect("drop catalog");
        port.reconcile_with_page_size(256, 1)
            .await
            .expect("reconcile after drop");
        assert!(matches!(
            port.admit_catalog(&instance_id),
            CatalogAdmission::Absent
        ));

        // The provider cannot close while this test still holds store handles.
        drop(port);
        drop(_control);
        drop(store);
        drop(scanning);
        drop(inner);
        host.shutdown(Instant::now() + Duration::from_secs(5))
            .await
            .expect("state store shutdown");
    }

    async fn open_store() -> (tempfile::TempDir, StateStoreHost, Arc<dyn StateStore>) {
        let directory = tempfile::tempdir().expect("temporary SQLite StateStore directory");
        let registry =
            builtin_state_store_provider_registry().expect("builtin StateStore registry");
        let host = StateStoreHost::open(
            &registry,
            StateStoreHostConfig {
                state_store: StateStoreAppConfig {
                    store: StateStoreConfig {
                        cluster_id: "catalog-controller-test".to_string(),
                        limits: StateStoreLimitOverrides::default(),
                        provider: StateStoreProviderConfig::Sqlite {
                            path: directory.path().join("state-store.sqlite"),
                            deployment_owner: "catalog-controller-test".to_string(),
                        },
                    },
                    mysql_client: None,
                },
                foundationdb_client: None,
            },
            FeDeploymentView {
                // SQLite is a single-FE StateStore provider. This fixture
                // still instantiates two independent local controller hosts
                // over its shared StateStore surface; it is not a production
                // multi-process deployment claim.
                active_fe_count: NonZeroUsize::new(1).expect("non-zero FE count"),
                topology_revision: Bytes::from_static(b"catalog-controller-test-r1"),
            },
            Instant::now() + Duration::from_secs(5),
        )
        .await
        .expect("open SQLite StateStore");
        assert_eq!(host.provider_id(), SQLITE_STATE_STORE_PROVIDER_ID);
        let store = host.state_store().expect("ready StateStore");
        (directory, host, store)
    }

    fn attachment() -> CatalogAttachment {
        CatalogAttachment {
            attachment_id: uuid::Uuid::now_v7(),
            instance_id: novarocks_spi::connector::ConnectorInstanceId::parse("catalog.analytics")
                .expect("instance ID"),
            provider_id: ConnectorProviderId::parse("iceberg").expect("provider ID"),
            display_name: "catalog.analytics".to_string(),
            durable_properties: Vec::new(),
            created_at_ms: 1,
        }
    }

    fn projection(
        repository: CatalogAttachmentRepository,
    ) -> (
        Arc<ConnectorControlHost>,
        Arc<FrontendCatalogApplicationPort>,
    ) {
        let control = Arc::new(
            ConnectorControlHost::with_factories(vec![Arc::new(ReadyFactory::default())])
                .expect("control host"),
        );
        let port = Arc::new(FrontendCatalogApplicationPort::new(
            repository,
            Arc::clone(&control),
            crate::catalog_application::CatalogRuntimeProjection::new().publisher(),
            tokio::runtime::Handle::current(),
        ));
        (control, port)
    }

    fn create_command(if_not_exists: bool) -> CatalogCreateCommand {
        CatalogCreateCommand {
            instance_id: novarocks_spi::connector::ConnectorInstanceId::parse("catalog.analytics")
                .expect("instance ID"),
            display_name: "catalog.analytics".to_string(),
            properties: vec![("type".to_string(), "iceberg".to_string())],
            if_not_exists,
        }
    }

    /// Name uniqueness is arbitrated by the absent-precondition commit, not by a
    /// local lock: two independent frontend hosts racing the same SQL name
    /// produce exactly one durable attachment identity, and both converge on it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_create_of_one_catalog_name_yields_a_single_attachment_identity() {
        let (_directory, mut host, store) = open_store().await;
        let repository = CatalogAttachmentRepository::open(Arc::clone(&store))
            .await
            .expect("open catalog attachment repository");
        let (_first_control, first_port) = projection(repository.clone());
        let (_second_control, second_port) = projection(repository.clone());

        // Plain OS threads, so each CREATE drives the port's blocking StateStore
        // path from outside the runtime and the two commits genuinely race.
        let (first_result, second_result) = std::thread::scope(|scope| {
            let first = scope.spawn(|| first_port.create_catalog(create_command(false)));
            let second = scope.spawn(|| second_port.create_catalog(create_command(false)));
            (
                first.join().expect("first CREATE thread"),
                second.join().expect("second CREATE thread"),
            )
        });

        let attachments = repository
            .list_with_page_size(256)
            .await
            .expect("list attachments");
        assert_eq!(
            attachments.len(),
            1,
            "one SQL name must own exactly one durable attachment"
        );
        let winner = attachments[0].attachment.attachment_id;
        let (winner_result, loser_result) = match (&first_result, &second_result) {
            (Ok(_), Err(_)) => (&first_result, &second_result),
            (Err(_), Ok(_)) => (&second_result, &first_result),
            other => panic!("exactly one concurrent CREATE must win: {other:?}"),
        };
        assert_eq!(
            winner_result
                .as_ref()
                .expect("winning CREATE observation")
                .attachment_id,
            winner
        );
        assert_eq!(
            loser_result
                .as_ref()
                .expect_err("the losing CREATE reports the existing attachment")
                .kind(),
            CatalogApplicationErrorKind::AlreadyExists
        );

        // The loser converges on the winner's identity through an authoritative
        // reread, and each host owns its own live control generation.
        first_port
            .reconcile_with_page_size(256, 1)
            .await
            .expect("first host reconcile");
        second_port
            .reconcile_with_page_size(256, 1)
            .await
            .expect("second host reconcile");
        let instance_id = create_command(false).instance_id;
        for port in [&first_port, &second_port] {
            match port.admit_catalog(&instance_id) {
                CatalogAdmission::Ready(observation) => {
                    assert_eq!(observation.attachment_id, winner)
                }
                other => panic!("both hosts must admit the surviving attachment: {other:?}"),
            }
        }
        assert!(
            _first_control.observe_current_binding(&instance_id).is_ok()
                && _second_control
                    .observe_current_binding(&instance_id)
                    .is_ok(),
            "each host materializes its own local control generation"
        );

        drop(first_port);
        drop(second_port);
        drop(repository);
        drop(store);
        host.shutdown(Instant::now() + Duration::from_secs(5))
            .await
            .expect("shutdown SQLite StateStore");
    }

    /// The factory validates provider input before the attachment commit, so a
    /// provider that cannot be materialized leaves no durable trace at all.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn factory_preflight_failure_commits_no_attachment() {
        let (_directory, mut host, store) = open_store().await;
        let repository = CatalogAttachmentRepository::open(Arc::clone(&store))
            .await
            .expect("open catalog attachment repository");
        let control = Arc::new(
            ConnectorControlHost::with_factories(vec![Arc::new(UnavailableFactory)])
                .expect("control host"),
        );
        let port = Arc::new(FrontendCatalogApplicationPort::new(
            repository.clone(),
            control,
            crate::catalog_application::CatalogRuntimeProjection::new().publisher(),
            tokio::runtime::Handle::current(),
        ));

        assert_eq!(
            port.create_catalog(create_command(false))
                .expect_err("provider preflight failure must reject CREATE")
                .kind(),
            CatalogApplicationErrorKind::Unavailable
        );
        assert!(
            repository
                .list_with_page_size(256)
                .await
                .expect("list attachments")
                .is_empty(),
            "a preflight failure must not commit durable truth"
        );

        drop(port);
        drop(repository);
        drop(store);
        host.shutdown(Instant::now() + Duration::from_secs(5))
            .await
            .expect("shutdown SQLite StateStore");
    }

    /// Durable commit is the linearization point. Once it succeeds, a local
    /// publication failure is reported as an unavailable runtime and must not
    /// roll back the cluster-wide fact.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn create_keeps_the_committed_attachment_when_local_publication_fails() {
        let (_directory, mut host, store) = open_store().await;
        let repository = CatalogAttachmentRepository::open(Arc::clone(&store))
            .await
            .expect("open catalog attachment repository");
        let control = Arc::new(
            ConnectorControlHost::with_factories(vec![Arc::new(ReadyFactory::default())])
                .expect("control host"),
        );
        let port = Arc::new(FrontendCatalogApplicationPort::new(
            repository.clone(),
            Arc::clone(&control),
            Arc::new(RejectingPublisher),
            tokio::runtime::Handle::current(),
        ));

        assert_eq!(
            port.create_catalog(create_command(false))
                .expect_err("a failed local publication is reported to the client")
                .kind(),
            CatalogApplicationErrorKind::Unavailable
        );
        let attachments = repository
            .list_with_page_size(256)
            .await
            .expect("list attachments");
        assert_eq!(
            attachments.len(),
            1,
            "the committed attachment survives a local publication failure"
        );
        let instance_id = create_command(false).instance_id;
        assert!(
            matches!(
                port.admit_catalog(&instance_id),
                CatalogAdmission::Unavailable { .. }
            ),
            "the durable attachment exists but this host cannot serve it"
        );
        assert!(
            control.observe_current_binding(&instance_id).is_err(),
            "an unpublished generation must not stay locally live"
        );

        drop(port);
        drop(control);
        drop(repository);
        drop(store);
        host.shutdown(Instant::now() + Duration::from_secs(5))
            .await
            .expect("shutdown SQLite StateStore");
    }

    /// User-visible existence semantics, and the identity rule that stops a
    /// same-name recreate from resurrecting the dropped attachment.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn existence_semantics_and_recreate_mint_a_fresh_attachment_identity() {
        let (_directory, mut host, store) = open_store().await;
        let repository = CatalogAttachmentRepository::open(Arc::clone(&store))
            .await
            .expect("open catalog attachment repository");
        let (control, port) = projection(repository.clone());
        let instance_id = create_command(false).instance_id;

        assert_eq!(
            port.drop_catalog(CatalogDropCommand {
                instance_id: instance_id.clone(),
                if_exists: false,
            })
            .expect_err("dropping an absent catalog is an error")
            .kind(),
            CatalogApplicationErrorKind::NotFound
        );
        port.drop_catalog(CatalogDropCommand {
            instance_id: instance_id.clone(),
            if_exists: true,
        })
        .expect("DROP IF EXISTS on an absent catalog is a no-op");

        let first = port
            .create_catalog(create_command(false))
            .expect("first CREATE");
        assert_eq!(
            port.create_catalog(create_command(false))
                .expect_err("a duplicate CREATE is rejected")
                .kind(),
            CatalogApplicationErrorKind::AlreadyExists
        );
        // IF NOT EXISTS returns the current runtime instead of minting a second
        // lifecycle identity for the same SQL name.
        assert_eq!(
            port.create_catalog(create_command(true))
                .expect("CREATE IF NOT EXISTS resolves the existing attachment")
                .attachment_id,
            first.attachment_id
        );

        port.drop_catalog(CatalogDropCommand {
            instance_id: instance_id.clone(),
            if_exists: false,
        })
        .expect("DROP removes the durable attachment");
        assert!(matches!(
            port.admit_catalog(&instance_id),
            CatalogAdmission::Absent
        ));
        assert!(
            control.observe_current_binding(&instance_id).is_err(),
            "DROP stops local admission before the generation is gone"
        );

        let recreated = port
            .create_catalog(create_command(false))
            .expect("recreate the same SQL name");
        assert_ne!(
            recreated.attachment_id, first.attachment_id,
            "a recreated catalog must not reuse the dropped lifecycle identity"
        );

        drop(port);
        drop(control);
        drop(repository);
        drop(store);
        host.shutdown(Instant::now() + Duration::from_secs(5))
            .await
            .expect("shutdown SQLite StateStore");
    }

    #[test]
    fn defaults_match_the_cp2_operational_contract() {
        let config = CatalogProjectionConfig::default();
        assert_eq!(config.page_size, 256);
        assert_eq!(config.poll_interval, Duration::from_millis(250));
        assert_eq!(config.freshness_budget, Duration::from_secs(30));
        assert_eq!(config.retry_initial, Duration::from_millis(100));
        assert_eq!(config.retry_max, Duration::from_secs(5));
        assert_eq!(config.worker_count, 8);
        assert_eq!(config.shutdown_deadline, Duration::from_secs(5));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn publication_failure_keeps_durable_attachment_unavailable_and_retires_control() {
        let (_directory, mut host, store) = open_store().await;
        let repository = CatalogAttachmentRepository::open(Arc::clone(&store))
            .await
            .expect("open catalog attachment repository");
        let created = repository
            .create(attachment())
            .await
            .expect("create attachment");
        let control = Arc::new(
            ConnectorControlHost::with_factories(vec![Arc::new(ReadyFactory::default())])
                .expect("control host"),
        );
        let port = Arc::new(FrontendCatalogApplicationPort::new(
            repository.clone(),
            Arc::clone(&control),
            Arc::new(RejectingPublisher),
            tokio::runtime::Handle::current(),
        ));
        let controller = FrontendCatalogController::new(
            Arc::clone(&store),
            Arc::clone(&port),
            CatalogProjectionConfig::default(),
        )
        .expect("controller");

        controller
            .bootstrap()
            .await
            .expect("one provider failure does not fail the full bootstrap");
        assert!(matches!(
            port.admit_catalog(&created.attachment.instance_id),
            CatalogAdmission::Unavailable { .. }
        ));
        assert_eq!(port.projection_count(), 0);
        assert!(
            control
                .observe_current_binding(&created.attachment.instance_id)
                .is_err()
        );

        drop(controller);
        drop(port);
        drop(control);
        drop(repository);
        drop(store);
        host.shutdown(Instant::now() + Duration::from_secs(5))
            .await
            .expect("shutdown SQLite StateStore");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ready_admission_uses_only_the_local_projection_when_store_reads_fail() {
        let (_directory, mut host, store) = open_store().await;
        let durable_repository = CatalogAttachmentRepository::open(Arc::clone(&store))
            .await
            .expect("open durable repository");
        let created = durable_repository
            .create(attachment())
            .await
            .expect("create attachment");
        let toggle_store = Arc::new(ToggleReadStore {
            inner: Arc::clone(&store),
            reads_available: AtomicBool::new(true),
        });
        let repository = CatalogAttachmentRepository::open(toggle_store.clone())
            .await
            .expect("open toggle repository");
        let (_control, port) = projection(repository.clone());
        let controller = FrontendCatalogController::new(
            toggle_store.clone(),
            Arc::clone(&port),
            CatalogProjectionConfig::default(),
        )
        .expect("controller");
        controller.bootstrap().await.expect("bootstrap projection");
        toggle_store.reads_available.store(false, Ordering::Release);
        assert!(
            repository
                .get(&created.attachment.instance_id)
                .await
                .is_err(),
            "the injected StateStore read failure must be active"
        );
        assert!(matches!(
            port.admit_catalog(&created.attachment.instance_id),
            CatalogAdmission::Ready(_)
        ));

        drop(controller);
        drop(port);
        drop(repository);
        drop(durable_repository);
        drop(toggle_store);
        drop(store);
        host.shutdown(Instant::now() + Duration::from_secs(5))
            .await
            .expect("shutdown SQLite StateStore");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn two_frontend_controllers_converge_after_change_gap_and_catalog_removal() {
        let (_directory, mut host, store) = open_store().await;
        let repository = CatalogAttachmentRepository::open(Arc::clone(&store))
            .await
            .expect("open catalog attachment repository");
        let created = repository
            .create(attachment())
            .await
            .expect("create attachment");

        let (_first_control, first_port) = projection(repository.clone());
        let (_second_control, second_port) = projection(repository.clone());
        let first = FrontendCatalogController::new(
            Arc::clone(&store),
            Arc::clone(&first_port),
            CatalogProjectionConfig::default(),
        )
        .expect("first controller");
        let second = FrontendCatalogController::new(
            Arc::clone(&store),
            Arc::clone(&second_port),
            CatalogProjectionConfig::default(),
        )
        .expect("second controller");
        let first_cursor = first.bootstrap().await.expect("first bootstrap");
        let second_cursor = second.bootstrap().await.expect("second bootstrap");
        let high_watermark = store
            .poll_changes(&ChangePollRequest {
                after: None,
                page_size: 256,
            })
            .await
            .expect("read bootstrap high watermark")
            .high_watermark;
        assert_eq!(
            first_cursor
                .decode(store.identity().await.expect("store identity").store_id)
                .expect("decode bootstrap cursor")
                .0,
            high_watermark
        );
        let (bootstrap_identity, bootstrap_cursor) = first
            .bootstrap_state
            .lock()
            .expect("bootstrap state")
            .clone()
            .expect("bootstrap HWM retained for the worker");
        assert_eq!(
            bootstrap_identity,
            store.identity().await.expect("store identity")
        );
        assert_eq!(bootstrap_cursor, first_cursor);
        assert!(matches!(
            first_port.admit_catalog(&created.attachment.instance_id),
            CatalogAdmission::Ready(_)
        ));
        assert!(matches!(
            second_port.admit_catalog(&created.attachment.instance_id),
            CatalogAdmission::Ready(_)
        ));

        repository
            .drop_exact(created.clone())
            .await
            .expect("remove durable attachment");

        // A retention gap is only a wakeup: each controller rereads the
        // attachment repository instead of trusting the synthetic page state.
        let identity = store.identity().await.expect("store identity");
        let change = store
            .poll_changes(&ChangePollRequest {
                after: Some(first_cursor.clone()),
                page_size: 256,
            })
            .await
            .expect("read change page");
        let fault = FaultInjectingStateStore::new(Arc::clone(&store));
        fault.script_next_change_page(novarocks_spi::state_store::ChangePage {
            resync_required: true,
            ..change
        });
        let fault_store: Arc<dyn StateStore> = fault.clone();
        let fault_controller = FrontendCatalogController::new(
            fault_store,
            Arc::clone(&first_port),
            CatalogProjectionConfig::default(),
        )
        .expect("fault controller");
        let mut first_identity = Some(identity.clone());
        let mut first_cursor = Some(first_cursor);
        let mut force_resync = false;
        fault_controller
            .poll_once(&mut first_identity, &mut first_cursor, &mut force_resync)
            .await
            .expect("retention gap resync");
        assert!(!force_resync);

        let mut second_identity = Some(identity);
        let mut second_cursor = Some(second_cursor);
        let mut second_force_resync = true;
        second
            .poll_once(
                &mut second_identity,
                &mut second_cursor,
                &mut second_force_resync,
            )
            .await
            .expect("second controller authoritative resync");
        assert!(matches!(
            first_port.admit_catalog(&created.attachment.instance_id),
            CatalogAdmission::Absent
        ));
        assert!(matches!(
            second_port.admit_catalog(&created.attachment.instance_id),
            CatalogAdmission::Absent
        ));

        drop(fault_controller);
        drop(fault);
        drop(first);
        drop(second);
        drop(first_port);
        drop(second_port);
        drop(repository);
        drop(store);
        host.shutdown(Instant::now() + Duration::from_secs(5))
            .await
            .expect("shutdown SQLite StateStore");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn freshness_expiry_unpublishes_ready_catalogs_before_retrying() {
        let (_directory, mut host, store) = open_store().await;
        let repository = CatalogAttachmentRepository::open(Arc::clone(&store))
            .await
            .expect("open catalog attachment repository");
        let created = repository
            .create(attachment())
            .await
            .expect("create attachment");
        let (_control, port) = projection(repository.clone());
        let bootstrap = FrontendCatalogController::new(
            Arc::clone(&store),
            Arc::clone(&port),
            CatalogProjectionConfig::default(),
        )
        .expect("bootstrap controller");
        bootstrap.bootstrap().await.expect("bootstrap projection");
        assert!(matches!(
            port.admit_catalog(&created.attachment.instance_id),
            CatalogAdmission::Ready(_)
        ));

        let mut config = CatalogProjectionConfig::default();
        config.poll_interval = Duration::from_millis(1);
        config.freshness_budget = Duration::from_millis(10);
        config.retry_initial = Duration::from_millis(1);
        config.retry_max = Duration::from_millis(2);
        let unavailable_store: Arc<dyn StateStore> = Arc::new(PollUnavailableStore {
            inner: Arc::clone(&store),
        });
        let controller =
            FrontendCatalogController::new(unavailable_store, Arc::clone(&port), config)
                .expect("outage controller");
        controller.start().expect("start outage controller");
        tokio::time::sleep(Duration::from_millis(80)).await;
        assert!(matches!(
            port.admit_catalog(&created.attachment.instance_id),
            CatalogAdmission::Unavailable { .. }
        ));
        let metrics = controller.metrics_snapshot();
        assert_eq!(metrics.projected_catalogs, 0);
        assert!(metrics.failed_polls > 0);
        assert_eq!(metrics.freshness_expiries, 1);
        controller.shutdown().await.expect("shutdown controller");

        drop(controller);
        drop(bootstrap);
        drop(port);
        drop(repository);
        drop(store);
        host.shutdown(Instant::now() + Duration::from_secs(5))
            .await
            .expect("shutdown SQLite StateStore");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn store_identity_change_discards_the_cursor_and_forces_authoritative_resync() {
        let (_directory, mut host, store) = open_store().await;
        let repository = CatalogAttachmentRepository::open(Arc::clone(&store))
            .await
            .expect("open catalog attachment repository");
        let created = repository
            .create(attachment())
            .await
            .expect("create attachment");
        let (_control, port) = projection(repository.clone());
        let controller = FrontendCatalogController::new(
            Arc::clone(&store),
            Arc::clone(&port),
            CatalogProjectionConfig::default(),
        )
        .expect("controller");
        let cursor = controller.bootstrap().await.expect("bootstrap projection");
        assert!(matches!(
            port.admit_catalog(&created.attachment.instance_id),
            CatalogAdmission::Ready(_)
        ));
        repository
            .drop_exact(created.clone())
            .await
            .expect("remove durable attachment");

        let original_identity = store.identity().await.expect("original identity");
        let high_watermark = store
            .poll_changes(&ChangePollRequest {
                after: None,
                page_size: 256,
            })
            .await
            .expect("read current high watermark")
            .high_watermark;
        let changed_identity = StoreIdentity {
            store_id: uuid::Uuid::now_v7(),
            cluster_id: original_identity.cluster_id.clone(),
            initial_incarnation: original_identity.initial_incarnation + 1,
        };
        let changed_store: Arc<dyn StateStore> = Arc::new(IdentityChangedStore {
            inner: Arc::clone(&store),
            page: ChangePage {
                hints: Vec::new(),
                next_cursor: ChangeCursor::new(
                    changed_identity.store_id,
                    high_watermark.clone(),
                    u32::MAX,
                )
                .expect("changed identity cursor"),
                high_watermark,
                resync_required: false,
            },
            identity: changed_identity.clone(),
        });
        let changed_controller = FrontendCatalogController::new(
            changed_store,
            Arc::clone(&port),
            CatalogProjectionConfig::default(),
        )
        .expect("changed-identity controller");
        let mut known_identity = Some(original_identity);
        let mut cursor = Some(cursor);
        let mut force_resync = false;
        changed_controller
            .poll_once(&mut known_identity, &mut cursor, &mut force_resync)
            .await
            .expect("identity-change resync");
        assert_eq!(known_identity, Some(changed_identity));
        assert!(!force_resync);
        assert!(matches!(
            port.admit_catalog(&created.attachment.instance_id),
            CatalogAdmission::Absent
        ));

        drop(changed_controller);
        drop(controller);
        drop(port);
        drop(repository);
        drop(store);
        host.shutdown(Instant::now() + Duration::from_secs(5))
            .await
            .expect("shutdown SQLite StateStore");
    }
}
