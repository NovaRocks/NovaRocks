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

use super::StateStoreMvRepository;
use super::keyed::LoadedKeyedMvProjection;
use super::tests_definition::{projection_facts, repository, target};
use crate::repository::MvRepositoryErrorKind;
use novarocks_state_store_api::{
    AttemptSupervisor, Key, RangePage, RangeRequest, ReadTransaction, StateRecord, StateStore,
    StateStoreError, StateStoreLimits, StoreIdentity, WriteAttempt, WriteTransaction,
};
use novarocks_state_store_runtime::StateStoreRunPolicy;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::oneshot;
use uuid::Uuid;

#[derive(Default)]
struct ReadTrace {
    begins: usize,
    aborts: usize,
    ranges: Vec<RangeRequest>,
}

struct PagePause {
    reached: oneshot::Sender<()>,
    resume: oneshot::Receiver<()>,
}

enum PageFault {
    WrongContinuation,
    ProviderFailure,
    CloseFailure,
}

struct RecordingStore {
    inner: Arc<dyn StateStore>,
    trace: Arc<Mutex<ReadTrace>>,
    pause: Arc<Mutex<Option<PagePause>>>,
    fault: Arc<Mutex<Option<PageFault>>>,
}

impl RecordingStore {
    fn new(inner: Arc<dyn StateStore>) -> Arc<Self> {
        Arc::new(Self {
            inner,
            trace: Arc::new(Mutex::new(ReadTrace::default())),
            pause: Arc::new(Mutex::new(None)),
            fault: Arc::new(Mutex::new(None)),
        })
    }

    fn reset(&self) {
        *self.trace.lock().expect("read trace") = ReadTrace::default();
    }

    fn pause_next_page(&self) -> (oneshot::Receiver<()>, oneshot::Sender<()>) {
        let (reached, observed) = oneshot::channel();
        let (resume, released) = oneshot::channel();
        *self.pause.lock().expect("page pause") = Some(PagePause {
            reached,
            resume: released,
        });
        (observed, resume)
    }

    fn assert_scan_closed(&self, ranges: usize) {
        let trace = self.trace.lock().expect("read trace");
        assert_eq!(trace.begins, 1, "inventory must use one read snapshot");
        assert_eq!(trace.aborts, 1, "inventory must close its read snapshot");
        assert_eq!(trace.ranges.len(), ranges, "no range after refusal or EOF");
        assert!(trace.ranges.iter().all(|request| request.page_size == 1));
        assert!(trace.ranges[0].continuation.is_none());
        assert!(
            trace
                .ranges
                .iter()
                .skip(1)
                .all(|request| request.continuation.is_some())
        );
    }
}

struct RecordingRead {
    inner: Box<dyn ReadTransaction>,
    trace: Arc<Mutex<ReadTrace>>,
    pause: Arc<Mutex<Option<PagePause>>>,
    fault: Arc<Mutex<Option<PageFault>>>,
}

#[async_trait::async_trait]
impl ReadTransaction for RecordingRead {
    async fn get(&mut self, key: &Key) -> Result<Option<StateRecord>, StateStoreError> {
        self.inner.get(key).await
    }

    async fn range(&mut self, request: &RangeRequest) -> Result<RangePage, StateStoreError> {
        self.trace
            .lock()
            .expect("read trace")
            .ranges
            .push(request.clone());
        let mut page = self.inner.range(request).await?;
        let fault = {
            let mut fault = self.fault.lock().unwrap();
            if matches!(*fault, Some(PageFault::CloseFailure)) {
                None
            } else {
                fault.take()
            }
        };
        match fault {
            Some(PageFault::WrongContinuation) => {
                page.continuation = Some(request.continuation_after(&request.range.start)?);
            }
            Some(PageFault::ProviderFailure) => {
                return Err(StateStoreError::new(
                    novarocks_state_store_api::StateStoreErrorKind::ProviderUnavailable,
                    "injected bounded dependency range failure",
                ));
            }
            _ => {}
        }
        // Pause after the real store has fixed and read the first snapshot.
        let pause = self.pause.lock().expect("page pause").take();
        if let Some(pause) = pause {
            let _ = pause.reached.send(());
            pause.resume.await.expect("release snapshot page");
        }
        Ok(page)
    }

    async fn abort(self: Box<Self>) -> Result<(), StateStoreError> {
        self.trace.lock().expect("read trace").aborts += 1;
        self.inner.abort().await?;
        if self.fault.lock().unwrap().take().is_some() {
            return Err(StateStoreError::new(
                novarocks_state_store_api::StateStoreErrorKind::ProviderUnavailable,
                "injected bounded dependency close failure",
            ));
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl StateStore for RecordingStore {
    fn limits(&self) -> &StateStoreLimits {
        self.inner.limits()
    }

    fn attempts(&self) -> &AttemptSupervisor {
        self.inner.attempts()
    }

    async fn begin_read(&self) -> Result<Box<dyn ReadTransaction>, StateStoreError> {
        let inner = self.inner.begin_read().await?;
        self.trace.lock().expect("read trace").begins += 1;
        Ok(Box::new(RecordingRead {
            inner,
            trace: Arc::clone(&self.trace),
            pause: Arc::clone(&self.pause),
            fault: Arc::clone(&self.fault),
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

async fn recorded_repository() -> (Arc<RecordingStore>, Arc<StateStoreMvRepository>) {
    let (store, _) = repository().await;
    let store = RecordingStore::new(store);
    let repository = StateStoreMvRepository::open(store.clone(), StateStoreRunPolicy::default())
        .await
        .expect("open recorded repository");
    (store, repository)
}

async fn seed(repository: &StateStoreMvRepository, name: &str) -> LoadedKeyedMvProjection {
    repository
        .install_keyed_projection(
            Uuid::now_v7(),
            projection_facts(name, name.as_bytes(), 1, "base"),
            None,
        )
        .await
        .expect("seed canonical projection")
}

#[tokio::test]
async fn inventory_reads_one_snapshot_in_single_record_pages_and_returns_thin_identities() {
    let (store, repository) = recorded_repository().await;
    let first = seed(&repository, "first").await;
    let second = seed(&repository, "second").await;
    store.reset();
    let inventory = repository.list_keyed_projection_inventory().await.unwrap();
    store.assert_scan_closed(2);
    assert_eq!(inventory.len(), 2);
    for (entry, loaded) in inventory.iter().zip([&first, &second]) {
        assert_eq!(entry.mv_id, loaded.projection.mv_id);
        assert_eq!(entry.target, loaded.projection.facts.target());
        assert_eq!(
            &entry.object_id,
            &loaded.projection.facts.source_revision().target_object_id
        );
    }
    // Snapshot locators do not serve as a cached projection/version read.
    assert_eq!(
        repository
            .find_keyed_by_target(&target("first"))
            .await
            .unwrap(),
        Some(first),
    );
}

#[tokio::test]
async fn inventory_and_exact_lookup_accept_a_stored_projection_above_one_mib() {
    use crate::persistence::test_support::KeyedProjectionFixture;
    use novarocks_state_store_api::StateStoreCapacity;
    use novarocks_state_store_testkit::testing::InMemoryStateStore;

    // The test store explicitly admits the payload; its ordinary small default
    // is a separate provider fixture policy and must not mask MV acceptance.
    let inner = Arc::new(InMemoryStateStore::with_limits(
        format!("large-inventory-{}", Uuid::now_v7()),
        StateStoreCapacity::new(8 * 1024, usize::MAX, usize::MAX, 10_000).limits(),
    ));
    let store = RecordingStore::new(inner);
    let repository = StateStoreMvRepository::open(store.clone(), StateStoreRunPolicy::default())
        .await
        .unwrap();
    let mut fixture = KeyedProjectionFixture::new(target("large"), Some(17));
    fixture.definition.query.effective_sql.push_str(" /*");
    fixture
        .definition
        .query
        .effective_sql
        .push_str(&"x".repeat(1024 * 1024 + 1));
    fixture.definition.query.effective_sql.push_str("*/");
    let expected_sql = fixture.definition.query.effective_sql.clone();
    // Build reauthors computation identity, revisions and the actual keyed view.
    let loaded = repository
        .install_keyed_projection(Uuid::now_v7(), fixture.build().unwrap(), None)
        .await
        .unwrap();
    let root = super::key::projection_by_id_key(loaded.projection.mv_id).unwrap();
    let mut raw_read = store.inner.begin_read().await.unwrap();
    let persisted = raw_read.get(&root).await.unwrap().unwrap();
    assert!(persisted.value.as_bytes().len() > 1024 * 1024);
    raw_read
        .abort()
        .await
        .expect("close actual encoded-value read");

    store.reset();
    let inventory = repository.list_keyed_projection_inventory().await.unwrap();
    store.assert_scan_closed(1);
    assert_eq!(inventory.len(), 1);
    assert_eq!(inventory[0].mv_id, loaded.projection.mv_id);
    assert_eq!(inventory[0].target, loaded.projection.facts.target());
    assert_eq!(
        &inventory[0].object_id,
        &loaded.projection.facts.source_revision().target_object_id
    );

    store.reset();
    let exact = repository
        .find_keyed_by_target(&inventory[0].target)
        .await
        .unwrap()
        .expect("exact large projection");
    assert_eq!(
        exact
            .projection
            .facts
            .definition()
            .query
            .effective_sql
            .as_bytes(),
        expected_sql.as_bytes()
    );
    assert_eq!(exact, loaded);
    let trace = store.trace.lock().unwrap();
    assert_eq!(
        trace.begins, 2,
        "lookup and root reads keep their existing snapshots"
    );
    assert_eq!(trace.aborts, 2, "both exact reads must close");
    assert!(trace.ranges.is_empty());
}

#[tokio::test]
async fn inventory_continuation_keeps_its_first_snapshot_across_concurrent_insert_and_delete() {
    let (store, repository) = recorded_repository().await;
    let first = seed(&repository, "first").await;
    let removed = seed(&repository, "removed").await;
    store.reset();
    let (reached, resume) = store.pause_next_page();
    let scanner = Arc::clone(&repository);
    let task = tokio::spawn(async move { scanner.list_keyed_projection_inventory().await });
    tokio::time::timeout(Duration::from_secs(5), reached)
        .await
        .expect("first snapshot page must be reached")
        .expect("first page signal");
    // Concurrent writers use the same underlying store, outside the scanner's
    // recording wrapper: create returns a fresh read of its committed result.
    let mutator =
        StateStoreMvRepository::open(Arc::clone(&store.inner), StateStoreRunPolicy::default())
            .await
            .expect("concurrent mutation repository");
    seed(&mutator, "inserted").await;
    mutator
        .delete_keyed_projection(Uuid::now_v7(), {
            let mut expected = removed.clone();
            expected.version = (removed.version.clone()).clone();
            expected.projection.facts.source =
                (removed.projection.facts.source_revision().clone()).clone();
            expected
        })
        .await
        .expect("delete after snapshot was fixed");
    resume.send(()).expect("release snapshot");
    let inventory = tokio::time::timeout(Duration::from_secs(5), task)
        .await
        .expect("inventory must finish")
        .expect("scanner task")
        .expect("snapshot inventory");
    store.assert_scan_closed(2);
    assert_eq!(
        inventory
            .iter()
            .map(|entry| entry.mv_id)
            .collect::<Vec<_>>(),
        [first.projection.mv_id, removed.projection.mv_id],
    );
    assert_eq!(inventory[1].target, target("removed"));
    assert!(
        repository
            .find_keyed_by_target(&target("removed"))
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        repository
            .find_keyed_by_target(&target("inserted"))
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn bounded_target_lookup_returns_the_current_exact_version_after_replacement() {
    let (_, repository) = recorded_repository().await;
    let created = seed(&repository, "first").await;
    let inventory = repository.list_keyed_projection_inventory().await.unwrap();
    let replacement = repository
        .install_keyed_projection(
            Uuid::now_v7(),
            projection_facts("first", b"first", 2, "changed-base"),
            Some({
                let mut expected = created.clone();
                expected.version = (created.version.clone()).clone();
                expected
            }),
        )
        .await
        .expect("replace the scanned target");
    assert_ne!(created.version, replacement.version);
    let fresh = repository
        .find_keyed_by_target(&inventory[0].target)
        .await
        .unwrap()
        .expect("fresh target");
    assert_eq!(fresh, replacement);
    assert_eq!(
        repository
            .find_keyed_by_target(&inventory[0].target)
            .await
            .unwrap(),
        Some(fresh),
    );
}

fn assert_dependency_scan_closed(store: &RecordingStore, ranges: usize) {
    let trace = store.trace.lock().unwrap();
    assert_eq!(trace.begins, 1);
    assert_eq!(trace.aborts, 1);
    assert_eq!(trace.ranges.len(), ranges);
    assert!(trace.ranges.iter().all(|range| range.page_size == 1));
}

#[tokio::test]
async fn dependency_validation_preserve_the_exact_classification_and_sort_oracle() {
    use crate::dependency::MvDependencyObjectType;
    let (store, repository) = recorded_repository().await;
    repository
        .install_keyed_projection(
            Uuid::now_v7(),
            projection_facts("renamed_base", b"base-orders", 1, "other"),
            None,
        )
        .await
        .unwrap();
    let downstream = repository
        .install_keyed_projection(
            Uuid::now_v7(),
            projection_facts("downstream", b"downstream", 2, "orders"),
            None,
        )
        .await
        .unwrap();
    let oracle = repository
        .list_keyed_dependencies_by_downstream(&downstream)
        .await
        .unwrap();
    store.reset();
    let rows = repository
        .list_keyed_dependencies_by_downstream(&downstream)
        .await
        .unwrap();
    assert_eq!(rows, oracle);
    assert!(rows.iter().all(|row| row.upstream.object_type
        == MvDependencyObjectType::MaterializedView
        && row.upstream.display_name() == "mv:ice.sales.orders"));
    assert_dependency_scan_closed(&store, 4);
    let trace = store.trace.lock().unwrap();
    assert!(trace.ranges[0].continuation.is_none());
    assert!(trace.ranges[1].continuation.is_some());
    assert!(trace.ranges[2].continuation.is_none());
    assert!(trace.ranges[3].continuation.is_some());
}

#[tokio::test]
async fn dependency_validation_require_the_fresh_exact_downstream_version() {
    let (store, repository) = recorded_repository().await;
    let downstream = seed(&repository, "downstream").await;
    let replaced = repository
        .install_keyed_projection(
            Uuid::now_v7(),
            projection_facts("downstream", b"downstream", 2, "changed"),
            Some({
                let mut expected = downstream.clone();
                expected.version = (downstream.version.clone()).clone();
                expected
            }),
        )
        .await
        .unwrap();
    store.reset();
    let error = repository
        .list_keyed_dependencies_by_downstream(&downstream)
        .await
        .unwrap_err();
    assert_eq!(error.kind(), MvRepositoryErrorKind::Conflict);
    assert_dependency_scan_closed(&store, 0);
    assert_eq!(
        repository
            .list_keyed_dependencies_by_downstream(&replaced)
            .await
            .unwrap()[0]
            .upstream
            .name,
        "changed"
    );
}

#[tokio::test]
async fn dependency_index_and_classification_share_the_first_read_snapshot() {
    use crate::dependency::MvDependencyObjectType;
    let (store, repository) = recorded_repository().await;
    let upstream = repository
        .install_keyed_projection(
            Uuid::now_v7(),
            projection_facts("orders", b"base-orders", 1, "other"),
            None,
        )
        .await
        .unwrap();
    let downstream = repository
        .install_keyed_projection(
            Uuid::now_v7(),
            projection_facts("downstream", b"downstream", 2, "orders"),
            None,
        )
        .await
        .unwrap();
    let mutator = StateStoreMvRepository::open(store.inner.clone(), StateStoreRunPolicy::default())
        .await
        .unwrap();
    store.reset();
    let (observed, resume) = store.pause_next_page();
    let reader = repository.clone();
    let captured = downstream.clone();
    let running = tokio::spawn(async move {
        reader
            .list_keyed_dependencies_by_downstream(&captured)
            .await
    });
    observed.await.unwrap();
    mutator
        .delete_keyed_projection(Uuid::now_v7(), {
            let mut expected = upstream.clone();
            expected.version = (upstream.version).clone();
            expected.projection.facts.source =
                (upstream.projection.facts.source_revision().clone()).clone();
            expected
        })
        .await
        .unwrap();
    resume.send(()).unwrap();
    let rows = running.await.unwrap().unwrap();
    assert!(
        rows.iter()
            .all(|row| row.upstream.object_type == MvDependencyObjectType::MaterializedView)
    );
    assert_dependency_scan_closed(&store, 4);
    let rows = repository
        .list_keyed_dependencies_by_downstream(&downstream)
        .await
        .unwrap();
    assert!(
        rows.iter()
            .all(|row| row.upstream.object_type == MvDependencyObjectType::Table)
    );
}

#[tokio::test]
async fn dependency_validation_reject_a_missing_canonical_occurrence_before_classifying() {
    use novarocks_state_store_api::{CommitOutcome, Precondition};
    let (store, repository) = recorded_repository().await;
    let downstream = seed(&repository, "downstream").await;
    // Read the real current index before removing one canonical occurrence.
    let canonical = repository
        .list_keyed_dependencies_by_downstream(&downstream)
        .await
        .unwrap();
    let row = &canonical[0];
    let key = super::key::dependency_by_downstream_key(
        row.downstream_mv_id,
        &row.upstream,
        row.occurrence_id,
    )
    .unwrap();
    let (attempt, _) = store.attempts().reserve().unwrap();
    let mut write = store
        .begin_write(attempt, "remove canonical occurrence for bounded display")
        .await
        .unwrap();
    let record = write.get(&key).await.unwrap().unwrap();
    write
        .delete(key, Precondition::Version(record.version))
        .await
        .unwrap();
    assert!(matches!(write.commit().await, CommitOutcome::Committed(_)));
    store.reset();
    let error = repository
        .list_keyed_dependencies_by_downstream(&downstream)
        .await
        .unwrap_err();
    assert_eq!(error.kind(), MvRepositoryErrorKind::Corruption);
    assert!(error.to_string().contains("incomplete"));
    assert_dependency_scan_closed(&store, 1);
}

#[tokio::test]
async fn dependency_range_or_close_failures_never_publish_partial_facts() {
    let (store, repository) = recorded_repository().await;
    let downstream = seed(&repository, "downstream").await;
    for (fault, ranges, message) in [
        (PageFault::WrongContinuation, 1, "continuation differs"),
        (PageFault::ProviderFailure, 1, "range failure"),
        (PageFault::CloseFailure, 3, "close failure"),
    ] {
        store.reset();
        *store.fault.lock().unwrap() = Some(fault);
        let error = repository
            .list_keyed_dependencies_by_downstream(&downstream)
            .await
            .unwrap_err();
        assert!(error.to_string().contains(message), "{error}");
        assert_dependency_scan_closed(&store, ranges);
    }
}
