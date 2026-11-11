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

//! Real SQLite original requests feed the actual keyed inventory and Source.
//! The gate holds a real range callback, not a physical QB footer; generic
//! StateStore execution tests own the latter proof.
use super::*;
use crate::state_store::execution::{StateStoreOriginalBinding, StateStoreOriginalOwner};
use novarocks_mv_application::{
    readiness::{MvKeyedProjectionInstallOutcome, MvKeyedReadinessService},
    state_store_repository::StateStoreMvRepository,
};
use novarocks_state_store_api::*;
use novarocks_state_store_runtime::{
    StateStoreHost, StateStoreHostInput, StateStoreProviderRegistration,
    StateStoreProviderRegistry, StateStoreRunPolicy,
};
use novarocks_state_store_sqlite::{
    SQLITE_STATE_STORE_CAPACITY, SQLITE_STATE_STORE_PROVIDER_ID, SqliteStateStoreProviderFactory,
};
use std::sync::atomic::AtomicUsize;

struct PageGate {
    expected_key: Key,
    reached: tokio::sync::oneshot::Sender<()>,
    release: tokio::sync::oneshot::Receiver<()>,
    error: StateStoreExecutionFailure,
}
#[derive(Default)]
struct ReadTrace {
    begins: AtomicUsize,
    closes: AtomicUsize,
    drops: AtomicUsize,
    gate: Mutex<Option<PageGate>>,
}
struct HeldStore {
    inner: Arc<dyn StateStore>,
    trace: Arc<ReadTrace>,
}
struct HeldRead {
    inner: Box<dyn OriginalReadTransaction>,
    trace: Arc<ReadTrace>,
}
impl Drop for HeldRead {
    fn drop(&mut self) {
        self.trace.drops.fetch_add(1, Ordering::SeqCst);
    }
}
#[async_trait]
impl OriginalReadTransaction for HeldRead {
    async fn get_in(
        &mut self,
        key: &Key,
    ) -> Result<Option<StateRecord>, StateStoreExecutionFailure> {
        self.inner.get_in(key).await
    }
    async fn range_in(
        &mut self,
        request: &RangeRequest,
    ) -> Result<RangePage, StateStoreExecutionFailure> {
        let page = self.inner.range_in(request).await?;
        let gate = self.trace.gate.lock().unwrap().take();
        if let Some(gate) = gate {
            let prefix = novarocks_mv_application::state_family::MV_ACCELERATOR_STATE_FAMILY
                .key_with_suffix("/projection/by-id/")
                .unwrap();
            assert_eq!(request.range, KeyRange::for_prefix(prefix).unwrap());
            assert_eq!(request.direction, Direction::Forward);
            assert_eq!(request.page_size, 1);
            assert!(request.continuation.is_none());
            assert!(page.continuation.is_none());
            assert_eq!(page.records.len(), 1);
            assert_eq!(page.records[0].key, gate.expected_key);
            let _ = gate.reached.send(());
            // Dropping the release sender also releases the test gate on panic.
            let _ = gate.release.await;
            return Err(gate.error);
        }
        Ok(page)
    }
    async fn close_in(&mut self) -> Result<(), StateStoreExecutionFailure> {
        let result = self.inner.close_in().await;
        self.trace.closes.fetch_add(1, Ordering::SeqCst);
        result
    }
}
#[async_trait]
impl StateStore for HeldStore {
    fn limits(&self) -> &StateStoreLimits {
        self.inner.limits()
    }
    fn attempts(&self) -> &AttemptSupervisor {
        self.inner.attempts()
    }
    async fn begin_read(&self) -> Result<Box<dyn ReadTransaction>, StateStoreError> {
        panic!("inventory must use original read requests")
    }
    async fn begin_write(
        &self,
        _: WriteAttempt,
        _: &str,
    ) -> Result<Box<dyn WriteTransaction>, StateStoreError> {
        panic!("seed must use original write requests")
    }
    async fn identity(&self) -> Result<StoreIdentity, StateStoreError> {
        self.inner.identity().await
    }
    async fn begin_original_read_in(
        &self,
        execution: &StateStoreExecutionContext,
    ) -> Result<Box<dyn OriginalReadTransaction>, StateStoreExecutionFailure> {
        let inner = self.inner.begin_original_read_in(execution).await?;
        self.trace.begins.fetch_add(1, Ordering::SeqCst);
        Ok(Box::new(HeldRead {
            inner,
            trace: self.trace.clone(),
        }))
    }
    async fn begin_original_write_in(
        &self,
        attempt: WriteAttempt,
        purpose: &str,
        execution: &StateStoreExecutionContext,
    ) -> Result<Box<dyn OriginalWriteTransaction>, StateStoreExecutionFailure> {
        self.inner
            .begin_original_write_in(attempt, purpose, execution)
            .await
    }
}
struct InventoryFixture {
    resources: Fixture,
    host: StateStoreHost,
    directory: tempfile::TempDir,
    readiness: Option<MvKeyedReadinessService>,
    trace: Arc<ReadTrace>,
    store: std::sync::Weak<HeldStore>,
    stop: novarocks_spi::connector::ConnectorStopOwner,
    key: Key,
}
impl InventoryFixture {
    async fn new() -> Self {
        let listing = ListingFixture::new(FailurePoint::Views, ConnectorErrorKind::Unavailable);
        let mut resources = Fixture::new(&listing, ResultWindowClass::Client).await;
        let executor = resources.physical.blocking_executor();
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("system-source.sqlite");
        let mut registry = StateStoreProviderRegistry::new();
        registry
            .register(StateStoreProviderRegistration::new(
                SqliteStateStoreProviderFactory::new(path.clone())
                    .descriptor()
                    .to_owned(),
                move |_| Ok(Box::new(SqliteStateStoreProviderFactory::new(path.clone()))),
            ))
            .unwrap();
        let mut prepared = StateStoreHost::prepare_original_open(
            &registry,
            StateStoreHostInput {
                cluster_id: "system-inventory-original".into(),
                provider_id: SQLITE_STATE_STORE_PROVIDER_ID,
                limits: SQLITE_STATE_STORE_CAPACITY.limits(),
                run_policy: StateStoreRunPolicy::default(),
            },
            Instant::now() + Duration::from_secs(5),
        )
        .unwrap();
        let mut opening = StateStoreOriginalOwner::opening(
            &executor,
            prepared.vocabulary(),
            prepared.run_policy(),
        )
        .unwrap();
        prepared
            .start_in(&opening.context(prepared.deadline()))
            .unwrap();
        tokio::time::timeout(
            Duration::from_secs(5),
            std::future::poll_fn(|cx| {
                opening.poll_progress(cx).unwrap();
                prepared.poll_ready(cx)
            }),
        )
        .await
        .unwrap()
        .unwrap();
        let mut ready = None;
        prepared.install_ready_in(&mut ready).unwrap();
        std::future::poll_fn(|cx| opening.poll_closed(cx))
            .await
            .unwrap();
        drop(opening);
        drop(prepared);
        let host = ready.unwrap();
        let (vocabulary, instance) = host.original_blocking_sources().unwrap();
        let binding = StateStoreOriginalBinding {
            transaction_deadline: host.state_store().unwrap().limits().transaction_deadline,
            dependencies: host.original_blocking_dependency_facts().unwrap().to_vec(),
            vocabulary,
            instance,
            executor,
            policy: host.run_policy(),
        };
        let trace = Arc::new(ReadTrace::default());
        let store = Arc::new(HeldStore {
            inner: host.state_store().unwrap(),
            trace: trace.clone(),
        });
        let weak_store = Arc::downgrade(&store);
        let repository = StateStoreMvRepository::open(store, host.run_policy())
            .await
            .unwrap();
        let readiness = MvKeyedReadinessService::new(repository, Arc::default());
        let readiness_port = Arc::new(crate::mv::domain::readiness::MvReadinessPort::from_product(
            readiness.clone(),
            tokio::runtime::Handle::current(),
            crate::mv::domain::test_admission::management_entrance(),
        ));
        // Reuse the resource fixture before any operation starts, replacing its
        // empty product with this exact repository. There is one tested Source,
        // one admitted Root and the SAME already-installed ordinary QB pool.
        let old = resources.source.take().unwrap();
        let mut owners = old.owners.clone();
        owners.materialized_views = readiness_port.candidate_reader();
        owners.mv_readiness = readiness_port;
        owners.state_store = Some(binding);
        let stop = novarocks_spi::connector::ConnectorStopOwner::new();
        let mut scope = old.scope.clone();
        scope.connector_context = scope.connector_context.with_additional_stop(stop.view());
        old.close().await.unwrap();
        drop(old);
        let source = FrontendSystemTableFacts::new(owners, scope).unwrap();
        resources.source = Some(source.clone());
        let requests = source.store_requests().unwrap();
        assert!(Arc::ptr_eq(&requests, &source.store_requests().unwrap()));
        assert!(
            source
                .store
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .owner
                .is_some()
        );
        let read = readiness.in_requests(requests);
        let facts = novarocks_mv_application::persistence::test_support::sample_keyed_projection(
            novarocks_mv_application::product::MvTarget::from_parts(
                Some("ice.main"),
                "db",
                "mv_inventory",
            ),
            Some(9),
        );
        let mut seed = Box::pin(read.seed_projection(uuid::Uuid::now_v7(), facts));
        let loaded = tokio::time::timeout(
            Duration::from_secs(5),
            driven(
                &resources.control,
                &mut resources.driver,
                std::future::poll_fn(|cx| {
                    source.poll_store_progress(cx);
                    seed.as_mut().poll(cx)
                }),
            ),
        )
        .await
        .unwrap()
        .unwrap();
        drop(seed);
        drop(read);
        let MvKeyedProjectionInstallOutcome::Installed(loaded) = loaded else {
            panic!("real seeded keyed projection")
        };
        let key = novarocks_mv_application::state_store_repository::key::projection_by_id_key(
            loaded.projection.mv_id,
        )
        .unwrap();
        drop(loaded);
        drop(source);
        Self {
            resources,
            host,
            directory,
            readiness: Some(readiness),
            trace,
            store: weak_store,
            stop,
            key,
        }
    }
    async fn finish(mut self, cancelled: bool) {
        self.resources.retire_source().await;
        drop(self.readiness.take());
        assert!(
            self.store.upgrade().is_none(),
            "all actual repository aliases retired"
        );
        let (vocabulary, instance) = self.host.original_blocking_sources().unwrap();
        let mut closing = StateStoreOriginalOwner::closing_instance(
            &self.resources.physical.blocking_executor(),
            vocabulary,
            instance,
            self.host.run_policy(),
        )
        .unwrap();
        let mut original = None;
        self.host
            .prepare_original_close_in(
                &closing.context(Instant::now() + Duration::from_secs(5)),
                &mut original,
            )
            .unwrap();
        tokio::time::timeout(
            Duration::from_secs(5),
            std::future::poll_fn(|cx| {
                closing.poll_progress(cx).unwrap();
                self.host
                    .poll_original_closed(original.as_deref_mut().unwrap(), cx)
            }),
        )
        .await
        .unwrap()
        .unwrap();
        std::future::poll_fn(|cx| closing.poll_closed(cx))
            .await
            .unwrap();
        drop(original);
        drop(closing);
        drop(self.host);
        self.resources.finish(cancelled).await;
        drop(self.directory);
    }
}
#[tokio::test]
async fn actual_sqlite_inventory_is_listable_through_the_same_source_owned_requests() {
    let mut fixture = InventoryFixture::new().await;
    let source = fixture.resources.source();
    let mut inventory = tokio::time::timeout(
        Duration::from_secs(5),
        driven(
            &fixture.resources.control,
            &mut fixture.resources.driver,
            source.materialized_views(false, false, &source.scope.connector_context),
        ),
    )
    .await
    .unwrap()
    .unwrap();
    let row = driven(
        &fixture.resources.control,
        &mut fixture.resources.driver,
        inventory.next(),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(
        (row.catalog.as_str(), row.schema.as_str(), row.name.as_str()),
        ("ice.main", "db", "mv_inventory")
    );
    assert!(
        driven(
            &fixture.resources.control,
            &mut fixture.resources.driver,
            inventory.next()
        )
        .await
        .unwrap()
        .is_none()
    );
    drop(inventory);
    driven(
        &fixture.resources.control,
        &mut fixture.resources.driver,
        source.close(),
    )
    .await
    .unwrap();
    assert!(source.io.lock().unwrap().joined);
    assert!(source.local_health_failure().is_none());
    assert!(fixture.trace.begins.load(Ordering::SeqCst) > 0);
    assert_eq!(
        fixture.trace.begins.load(Ordering::SeqCst),
        fixture.trace.closes.load(Ordering::SeqCst)
    );
    assert_eq!(
        fixture.trace.begins.load(Ordering::SeqCst),
        fixture.trace.drops.load(Ordering::SeqCst)
    );
    drop(source);
    fixture.finish(false).await;
}
#[tokio::test]
async fn cancelled_actual_sqlite_inventory_retains_operation_and_original_range_error_until_close()
{
    let mut fixture = InventoryFixture::new().await;
    let source = fixture.resources.source();
    let (signal, reached) = tokio::sync::oneshot::channel();
    let (release, held) = tokio::sync::oneshot::channel();
    let original = StateStoreExecutionFailure::new(StateStoreError::new(
        StateStoreErrorKind::ProviderUnavailable,
        "original held SQLite inventory range failure",
    ));
    *fixture.trace.gate.lock().unwrap() = Some(PageGate {
        expected_key: fixture.key.clone(),
        reached: signal,
        release: held,
        error: original.clone(),
    });
    let mut open =
        Box::pin(source.materialized_views(false, false, &source.scope.connector_context));
    tokio::time::timeout(Duration::from_secs(5), driven(&fixture.resources.control, &mut fixture.resources.driver, async {
        tokio::select! { result = &mut open => panic!("inventory completed before actual range release: {}", result.is_ok()), result = reached => result.unwrap() }
    })).await.unwrap();
    drop(open);
    fixture
        .resources
        .root
        .owner
        .cancel(CancellationReason::ExplicitKill {
            requester_session: 41,
        });
    fixture.stop.request_stop();
    for _ in 0..2 {
        let mut close = Box::pin(source.close());
        assert!(futures::poll!(close.as_mut()).is_pending());
        drop(close);
    }
    assert!(source.io.lock().unwrap().future.is_some());
    assert!(!source.io.lock().unwrap().joined);
    assert!(source.local_health_failure().is_none());
    release.send(()).unwrap();
    let Response::Open(Err(SystemTableError::Provider(error))) = driven(
        &fixture.resources.control,
        &mut fixture.resources.driver,
        source.receive(),
    )
    .await
    else {
        panic!("original inventory refusal")
    };
    let mut chain: Option<&(dyn std::error::Error + 'static)> = Some(&error);
    let mut found = false;
    while let Some(cause) = chain {
        if let Some(actual) = cause.downcast_ref::<novarocks_failure_contract::SharedFailureCause>()
        {
            found |= actual == &original;
        }
        chain = cause.source();
    }
    assert!(found, "same actual range source cause retained");
    driven(
        &fixture.resources.control,
        &mut fixture.resources.driver,
        source.close(),
    )
    .await
    .unwrap();
    assert!(source.io.lock().unwrap().joined);
    assert!(source.local_health_failure().is_none());
    assert_eq!(
        fixture.trace.begins.load(Ordering::SeqCst),
        fixture.trace.closes.load(Ordering::SeqCst)
    );
    assert_eq!(
        fixture.trace.begins.load(Ordering::SeqCst),
        fixture.trace.drops.load(Ordering::SeqCst)
    );
    drop(source);
    fixture.finish(true).await;
}
