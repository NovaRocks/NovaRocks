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

use super::*;
use crate::persistence::keyed_documents::MvKeyedDocumentProjection;
use crate::persistence::test_support::{
    KeyedProjectionFixture, observed_current_keyed, sample_keyed_projection,
};
use crate::state_store_repository::StateStoreMvRepository;
use novarocks_spi::connector::{CatalogVersion, ConnectorInstanceId, ConnectorStopOwner};
use std::time::{Duration, Instant};

fn target() -> MvTarget {
    MvTarget::from_parts(Some("ice"), "sales", "mv")
}
async fn service() -> (Arc<StateStoreMvRepository>, MvKeyedReadinessService) {
    let repository = StateStoreMvRepository::open(
        Arc::new(
            novarocks_state_store_testkit::testing::InMemoryStateStore::new(format!(
                "keyed-readiness-behavior-{}",
                Uuid::now_v7()
            )),
        ),
        novarocks_state_store_runtime::StateStoreRunPolicy::default(),
    )
    .await
    .unwrap();
    let service =
        MvKeyedReadinessService::new(repository.clone(), Arc::new(ProcessRuntime::default()));
    (repository, service)
}
type Cancellation = ConnectorStopOwner;
fn request(version: u8, cancellation: Arc<Cancellation>) -> MvCurrentProjectionRequest {
    MvCurrentProjectionRequest::try_new(
        CatalogHandle::new(
            ConnectorInstanceId::parse("ice").unwrap(),
            CatalogVersion::from_bytes([version; 32]),
        ),
        target(),
        ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(30),
            cancellation.view(),
            novarocks_spi::connector::MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
            novarocks_spi::connector::MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
        )
        .unwrap(),
        PersistenceDecodeBudget::default(),
    )
    .unwrap()
}
struct Source {
    facts: MvKeyedDocumentProjection,
    entered: Option<Arc<tokio::sync::Semaphore>>,
    release: Option<Arc<tokio::sync::Semaphore>>,
    fail: bool,
}
impl Source {
    async fn observe_documents(
        &self,
        request: &MvCurrentProjectionRequest,
    ) -> Result<
        crate::persistence::keyed_documents::MvKeyedObservedCurrentDocuments,
        MvProjectionError,
    > {
        if let Some(entered) = &self.entered {
            entered.add_permits(1);
        }
        if let Some(release) = &self.release {
            release.acquire().await.unwrap().forget();
        }
        if self.fail {
            return Err(MvProjectionError::new(
                MvProjectionErrorKind::Unavailable,
                "injected source failure",
            ));
        }
        Ok(observed_current_keyed(
            &self.facts,
            request.catalog().clone(),
        ))
    }
}
#[async_trait::async_trait]
impl MvKeyedCurrentProjectionSource for Source {
    async fn observe_current_keyed(
        &self,
        request: MvCurrentProjectionRequest,
    ) -> Result<
        (
            crate::persistence::keyed_documents::MvKeyedObservedCurrentDocuments,
            crate::management::MvCurrentManagementAdmission,
        ),
        MvProjectionError,
    > {
        let documents = self.observe_documents(&request).await?;
        let admission = crate::management::MvCurrentManagementAdmission::for_test_keyed(&documents);
        Ok((documents, admission))
    }
}
#[async_trait::async_trait]
impl MvKeyedReadOnlyCurrentProjectionSource for Source {
    async fn observe_current_keyed_read_only(
        &self,
        request: MvCurrentProjectionRequest,
    ) -> Result<
        crate::persistence::keyed_documents::MvKeyedObservedCurrentDocuments,
        MvProjectionError,
    > {
        self.observe_documents(&request).await
    }
}
fn source(snapshot: i64) -> Source {
    Source {
        facts: sample_keyed_projection(target(), Some(snapshot)),
        entered: None,
        release: None,
        fail: false,
    }
}

#[tokio::test]
async fn restored_cache_is_query_candidate_but_not_current_management_ready() {
    let (repository, service) = service().await;
    repository
        .install_keyed_projection(
            Uuid::now_v7(),
            sample_keyed_projection(target(), Some(1)),
            None,
        )
        .await
        .unwrap();
    assert!(service.load_ready(&target()).await.is_err());
    assert_eq!(
        service
            .candidate_reader()
            .list_candidate_definitions()
            .await
            .unwrap()
            .len(),
        1
    );
    service
        .observe_and_install(Uuid::now_v7(), request(1, Arc::default()), &source(1))
        .await
        .unwrap();
    assert!(service.load_ready(&target()).await.unwrap().is_some());
}

#[tokio::test]
async fn sealed_read_only_current_rebuilds_inventory_without_granting_management_readiness() {
    let (_, service) = service().await;
    service
        .observe_current_read_only_and_install(
            Uuid::now_v7(),
            request(1, Arc::default()),
            &source(1),
            "test read-only Current".into(),
        )
        .await
        .unwrap();

    assert!(service.load_ready(&target()).await.is_err());
    assert_eq!(
        service
            .candidate_reader()
            .list_candidate_definitions()
            .await
            .unwrap()
            .len(),
        1
    );

    service
        .observe_and_install(Uuid::now_v7(), request(1, Arc::default()), &source(1))
        .await
        .unwrap();
    assert!(service.load_ready(&target()).await.unwrap().is_some());
}

#[tokio::test]
async fn changed_c_only_is_installed_as_a_whole_new_source_revision() {
    let (_, service) = service().await;
    let first = sample_keyed_projection(target(), Some(1));
    service
        .seed_projection(Uuid::now_v7(), first.clone())
        .await
        .unwrap();
    let mut fixture = KeyedProjectionFixture::new(target(), Some(1));
    fixture.configuration.paused = true;
    let next = fixture.build().unwrap();
    assert_ne!(first.source_revision(), next.source_revision());
    service
        .seed_projection(Uuid::now_v7(), next.clone())
        .await
        .unwrap();
    assert_eq!(
        service
            .load_ready(&target())
            .await
            .unwrap()
            .unwrap()
            .projection
            .facts,
        next
    );
}

#[tokio::test]
async fn shared_logical_target_orders_different_catalog_handles_and_late_failures() {
    for late_failure in [false, true] {
        let (_, service) = service().await;
        let entered = Arc::new(tokio::sync::Semaphore::new(0));
        let release = Arc::new(tokio::sync::Semaphore::new(0));
        let mut slow = source(1);
        slow.entered = Some(entered.clone());
        slow.release = Some(release.clone());
        slow.fail = late_failure;
        let a = service.clone();
        let old = tokio::spawn(async move {
            a.observe_and_install(Uuid::now_v7(), request(1, Arc::default()), &slow)
                .await
        });
        entered.acquire().await.unwrap().forget();
        assert!(service.load_ready(&target()).await.unwrap().is_none());
        service
            .observe_and_install(Uuid::now_v7(), request(2, Arc::default()), &source(2))
            .await
            .unwrap();
        let newer = service.load_ready(&target()).await.unwrap();
        release.add_permits(1);
        assert!(matches!(
            old.await.unwrap().unwrap(),
            MvKeyedProjectionInstallOutcome::Superseded
        ));
        assert_eq!(service.load_ready(&target()).await.unwrap(), newer);
    }
}

/// A management read that arrives while an observation is in flight waits for
/// it instead of reporting the absence of one. A background refresh running
/// beside a user statement makes that arrival routine, and without the wait
/// the statement is told the target has no successful fresh observation --
/// about a target whose observation is succeeding as it asks.
#[tokio::test]
async fn a_management_read_waits_for_the_observation_already_in_flight() {
    let (_, service) = service().await;
    let entered = Arc::new(tokio::sync::Semaphore::new(0));
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    let mut slow = source(1);
    slow.entered = Some(entered.clone());
    slow.release = Some(release.clone());
    let installer = service.clone();
    let install = tokio::spawn(async move {
        installer
            .observe_and_install(Uuid::now_v7(), request(1, Arc::default()), &slow)
            .await
    });
    entered.acquire().await.unwrap().forget();

    // The inventory read answers from what the process knows now, which while
    // an observation is in flight is nothing. That is what it is for.
    assert!(service.load_ready(&target()).await.unwrap().is_none());

    let reader = service.clone();
    let waiting = tokio::spawn(async move { reader.load_ready_settled(&target()).await });
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        !waiting.is_finished(),
        "the management read answered before the observation it is waiting for"
    );

    release.add_permits(1);
    install.await.unwrap().unwrap();
    assert!(waiting.await.unwrap().unwrap().is_some());
}

/// The wait is bounded. A reservation whose owner was dropped without settling
/// would otherwise hold every later management read on that target forever;
/// past the bound the read answers exactly as it did before the wait existed.
#[tokio::test(start_paused = true)]
async fn a_management_read_stops_waiting_for_an_abandoned_observation() {
    let (repository, service) = service().await;
    repository
        .install_keyed_projection(
            Uuid::now_v7(),
            sample_keyed_projection(target(), Some(1)),
            None,
        )
        .await
        .unwrap();
    let entered = Arc::new(tokio::sync::Semaphore::new(0));
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    let mut abandoned_source = source(2);
    abandoned_source.entered = Some(entered.clone());
    abandoned_source.release = Some(release);
    let installer = service.clone();
    let abandoned = tokio::spawn(async move {
        installer
            .observe_and_install(
                Uuid::now_v7(),
                request(1, Arc::default()),
                &abandoned_source,
            )
            .await
    });
    // The public operation has already reserved this target and reached the
    // actual source before its owner is dropped without settling that generation.
    entered.acquire().await.unwrap().forget();
    abandoned.abort();
    assert!(abandoned.await.unwrap_err().is_cancelled());
    let began = tokio::time::Instant::now();
    assert!(service.load_ready_settled(&target()).await.is_err());
    assert!(tokio::time::Instant::now().duration_since(began) >= OBSERVATION_SETTLE_WAIT);
}

/// A materialized view is the target object it publishes into, not the
/// catalog attachment a discovery happened to see it through. Two attachments
/// over one catalog are an ordinary deployment; registering the view once per
/// attachment would give one object several projections, make its own
/// `DROP CATALOG` refuse, and leave the real target competing with its own
/// aliases for management readiness.
#[tokio::test]
async fn one_target_object_is_projected_once_however_many_attachments_see_it() {
    let (repository, service) = service().await;
    service
        .observe_and_install(Uuid::now_v7(), request(1, Arc::default()), &source(1))
        .await
        .unwrap();

    // The same object, reached through a second attachment: same namespace and
    // name, a different catalog.
    let alias = MvTarget::from_parts(Some("ice_other"), "sales", "mv");
    let mut through_alias = source(1);
    through_alias.facts = sample_keyed_projection(alias.clone(), Some(1));
    let request = MvCurrentProjectionRequest::try_new(
        CatalogHandle::new(
            ConnectorInstanceId::parse("ice_other").unwrap(),
            CatalogVersion::from_bytes([1; 32]),
        ),
        alias.clone(),
        ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(30),
            ConnectorStopOwner::new().view(),
            novarocks_spi::connector::MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
            novarocks_spi::connector::MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
        )
        .unwrap(),
        PersistenceDecodeBudget::default(),
    )
    .unwrap();

    let outcome = service
        .observe_and_install(Uuid::now_v7(), request, &through_alias)
        .await
        .unwrap();
    let MvKeyedProjectionInstallOutcome::AlreadyProjectedElsewhere(owner) = outcome else {
        panic!("a second attachment must not mint a second projection: {outcome:?}");
    };
    assert_eq!(owner, target());
    assert_eq!(repository.list_keyed_projections().await.unwrap().len(), 1);
    // The alias is not an MV of its own, so nothing guards a catalog on its
    // behalf and nothing reads it as a target.
    assert!(service.load_ready(&alias).await.unwrap().is_none());
    assert!(service.load_ready(&target()).await.unwrap().is_some());
}

/// Rediscovery runs whenever a catalog is admitted, not only at startup, and
/// it observes read-only. Re-observing a view this process created and
/// refreshes must not close management on it: the next REFRESH or DROP would
/// be told the target has no successful fresh observation, about a view this
/// very process manages. A restart still closes management -- a fresh process
/// manages nothing yet, which is what the read-only path is for.
#[tokio::test]
async fn a_read_only_re_observation_does_not_revoke_management_this_process_holds() {
    let (_, service) = service().await;
    service
        .observe_and_install(Uuid::now_v7(), request(1, Arc::default()), &source(1))
        .await
        .unwrap();
    let managed = service.load_ready(&target()).await.unwrap();
    assert!(managed.is_some());

    service
        .observe_current_read_only_and_install(
            Uuid::now_v7(),
            request(1, Arc::default()),
            &source(1),
            "test read-only Current".into(),
        )
        .await
        .unwrap();
    assert_eq!(service.load_ready(&target()).await.unwrap(), managed);

    // A read-only observation that finds the view changed underneath is a
    // different matter: what this process managed is gone, so management
    // closes until a readmission establishes it again.
    service
        .observe_current_read_only_and_install(
            Uuid::now_v7(),
            request(1, Arc::default()),
            &source(2),
            "test read-only Current".into(),
        )
        .await
        .unwrap();
    assert!(service.load_ready(&target()).await.is_err());
}

#[tokio::test]
async fn newer_failure_does_not_reauthorize_older_success() {
    let (_, service) = service().await;
    let entered = Arc::new(tokio::sync::Semaphore::new(0));
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    let mut older_source = source(1);
    older_source.entered = Some(entered.clone());
    older_source.release = Some(release.clone());
    let installer = service.clone();
    let older = tokio::spawn(async move {
        installer
            .observe_and_install(Uuid::now_v7(), request(1, Arc::default()), &older_source)
            .await
    });
    entered.acquire().await.unwrap().forget();
    let mut newer_source = source(2);
    newer_source.fail = true;
    assert_eq!(
        service
            .observe_and_install(Uuid::now_v7(), request(2, Arc::default()), &newer_source,)
            .await
            .unwrap_err()
            .kind(),
        MvProjectionErrorKind::Unavailable,
    );
    release.add_permits(1);
    assert!(matches!(
        older.await.unwrap().unwrap(),
        MvKeyedProjectionInstallOutcome::Superseded,
    ));
    assert_eq!(service.list_ready_projections().await.unwrap().len(), 0);
}

#[tokio::test]
async fn fixed_repository_cas_is_captured_before_source_observation() {
    let (repository, service) = service().await;
    let entered = Arc::new(tokio::sync::Semaphore::new(0));
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    let mut older_source = source(1);
    older_source.entered = Some(entered.clone());
    older_source.release = Some(release.clone());
    let installer = service.clone();
    let older = tokio::spawn(async move {
        installer
            .observe_and_install(Uuid::now_v7(), request(1, Arc::default()), &older_source)
            .await
    });
    // Reaching source proves the absent StateStore CAS baseline was captured.
    entered.acquire().await.unwrap().forget();
    let external = repository
        .install_keyed_projection(
            Uuid::now_v7(),
            sample_keyed_projection(target(), Some(2)),
            None,
        )
        .await
        .unwrap();
    release.add_permits(1);
    assert!(matches!(
        older.await.unwrap().unwrap(),
        MvKeyedProjectionInstallOutcome::Superseded,
    ));
    assert_eq!(
        repository
            .find_keyed_by_target(&target(), PersistenceDecodeBudget::default())
            .await
            .unwrap(),
        Some(external),
    );
    assert!(service.load_ready(&target()).await.is_err());
}

#[tokio::test]
async fn late_cancellation_and_delete_cannot_revoke_new_ready_generation() {
    let (_, service) = service().await;
    service
        .seed_projection(Uuid::now_v7(), sample_keyed_projection(target(), Some(1)))
        .await
        .unwrap();
    let deletion = service.reserve_projection_delete(target()).await.unwrap();
    let cancellation = Arc::new(Cancellation::default());
    let entered = Arc::new(tokio::sync::Semaphore::new(0));
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    let mut slow = source(2);
    slow.entered = Some(entered.clone());
    slow.release = Some(release.clone());
    let req = request(1, cancellation.clone());
    let a = service.clone();
    let old = tokio::spawn(async move { a.observe_and_install(Uuid::now_v7(), req, &slow).await });
    entered.acquire().await.unwrap().forget();
    service
        .observe_and_install(Uuid::now_v7(), request(2, Arc::default()), &source(3))
        .await
        .unwrap();
    cancellation.request_stop();
    release.add_permits(1);
    assert!(matches!(
        old.await.unwrap().unwrap(),
        MvKeyedProjectionInstallOutcome::Superseded
    ));
    assert!(matches!(
        service
            .delete_after_provider_drop(Uuid::now_v7(), deletion)
            .await
            .unwrap(),
        MvKeyedProjectionInstallOutcome::Superseded
    ));
    assert!(service.load_ready(&target()).await.unwrap().is_some());
}

#[test]
fn missing_p_with_existing_output_is_not_never_published() {
    let mut fixture = KeyedProjectionFixture::new(target(), Some(7));
    fixture.publication = None;
    assert!(
        fixture
            .build()
            .unwrap_err()
            .contains("data but no MV publication record")
    );
    assert!(sample_keyed_projection(target(), None).never_published());

    // An empty slot-change snapshot remains never published only with the
    // provider's explicit zero data-file and delete-file totals.
    let mut empty = KeyedProjectionFixture::new(target(), Some(7));
    empty.publication = None;
    empty.file_totals = novarocks_spi::connector::SnapshotFileTotals {
        data_files: Some(0),
        delete_files: Some(0),
    };
    assert!(empty.build().unwrap().never_published());
    let mut unknown = KeyedProjectionFixture::new(target(), Some(7));
    unknown.publication = None;
    unknown.file_totals = novarocks_spi::connector::SnapshotFileTotals {
        data_files: None,
        delete_files: Some(0),
    };
    assert!(unknown.build().is_err());
}

#[tokio::test]
async fn cancellation_during_repository_commit_leaves_cache_but_never_ready() {
    use novarocks_state_store_runtime::StateStoreRunPolicy;
    use novarocks_state_store_testkit::conformance::{FaultGate, FaultInjectingStateStore};
    use novarocks_state_store_testkit::testing::InMemoryStateStore;
    let store = Arc::new(InMemoryStateStore::new("mv-installer-cancel-after-commit"));
    let fault = FaultInjectingStateStore::new(store);
    let repository = crate::state_store_repository::StateStoreMvRepository::open(
        fault.clone(),
        StateStoreRunPolicy::default(),
    )
    .await
    .unwrap();
    let service =
        MvKeyedReadinessService::new(repository.clone(), Arc::new(ProcessRuntime::default()));
    let gate = FaultGate::new();
    fault.pause_next_post_dispatch(gate.clone());
    let cancellation = Arc::new(Cancellation::default());
    let req = request(1, cancellation.clone());
    let installer = service.clone();
    let task = tokio::spawn(async move {
        installer
            .observe_and_install(Uuid::now_v7(), req, &source(1))
            .await
    });
    tokio::time::timeout(Duration::from_secs(10), gate.wait_reached())
        .await
        .unwrap();
    cancellation.request_stop();
    gate.release().await;
    assert_eq!(
        task.await.unwrap().unwrap_err().kind(),
        MvProjectionErrorKind::Cancelled
    );
    assert!(
        repository
            .find_keyed_by_target(&target(), PersistenceDecodeBudget::default())
            .await
            .unwrap()
            .is_some()
    );
    assert!(service.load_ready(&target()).await.is_err());
}

#[tokio::test]
async fn dependency_guard_uses_catalog_and_exact_object_not_locator_name() {
    let (_, service) = service().await;
    let facts = sample_keyed_projection(target(), Some(1));
    let occurrence = facts.definition().relation_occurrences[0].clone();
    service
        .seed_projection(Uuid::now_v7(), facts)
        .await
        .unwrap();

    // The probe carries the provider's own identity, which is what a DROP
    // observes from the provider. The document stores that identity inside the
    // application's fact envelope, so the guard has to open the envelope --
    // comparing the two as bytes is what made it never match.
    let observed =
        crate::persistence::exact_revision::restore_persisted_object(&occurrence.object_id)
            .expect("fixture occurrence carries a persisted provider object");
    let renamed =
        MvDependencyObjectIdentity::new(occurrence.catalog_at_binding.clone(), observed.clone());
    assert!(
        service
            .ensure_no_ready_downstream_dependencies(&renamed)
            .await
            .is_err()
    );

    let replacement = MvDependencyObjectIdentity::new(
        occurrence.catalog_at_binding.clone(),
        novarocks_spi::connector::ConnectorTableObjectId::try_new(bytes::Bytes::from_static(&[99]))
            .unwrap(),
    );
    service
        .ensure_no_ready_downstream_dependencies(&replacement)
        .await
        .unwrap();

    let other_catalog = MvDependencyObjectIdentity::new("other", observed);
    service
        .ensure_no_ready_downstream_dependencies(&other_catalog)
        .await
        .unwrap();
}

#[tokio::test]
async fn a_read_only_rebuild_is_readable_while_its_management_stays_closed() {
    let (_, service) = service().await;
    service
        .observe_current_read_only_and_install(
            Uuid::now_v7(),
            request(1, Arc::default()),
            &source(1),
            "test read-only Current".into(),
        )
        .await
        .unwrap();

    assert_eq!(
        service.query_admission(&target()).await.unwrap(),
        MvQueryAdmission::Admitted,
        "reading an MV is not a management operation"
    );
    assert!(
        service.load_ready(&target()).await.is_err(),
        "management stays closed until readmission completes"
    );
}

#[tokio::test]
async fn a_quarantined_projection_is_not_readable() {
    let (_, service) = service().await;
    service
        .observe_current_read_only_and_install(
            Uuid::now_v7(),
            request(1, Arc::default()),
            &source(1),
            "test read-only Current".into(),
        )
        .await
        .unwrap();
    service
        .invalidate_current(target(), "corrupt document set".to_string())
        .await
        .unwrap();

    assert_eq!(
        service.query_admission(&target()).await.unwrap(),
        MvQueryAdmission::Quarantined("corrupt document set".to_string()),
    );
}

#[tokio::test]
async fn a_table_this_process_holds_no_projection_for_is_not_an_mv() {
    let (_, service) = service().await;

    assert_eq!(
        service.query_admission(&target()).await.unwrap(),
        MvQueryAdmission::NotAnMv,
    );
}

fn local_inventory_bound() -> crate::repository::MvProjectionInventoryBound {
    crate::repository::MvProjectionInventoryBound {
        entries: 64,
        snapshot_bytes: 16 * 1024 * 1024,
        raw_page_bytes: 1024 * 1024,
        single_name_bytes: 65_536,
        continuation_token_bytes: 4096,
        decode: PersistenceDecodeBudget {
            max_working_set_bytes: 4 * 1024 * 1024,
            ..Default::default()
        },
    }
}

#[tokio::test]
async fn bounded_inventory_reloads_replacement_and_requires_its_exact_installed_version() {
    let (repository, service) = service().await;
    service
        .observe_and_install(Uuid::now_v7(), request(1, Arc::default()), &source(1))
        .await
        .unwrap();
    let mut inventory = service
        .bounded_projection_inventory(local_inventory_bound())
        .await
        .unwrap();
    let old = repository
        .find_keyed_by_target(&target(), PersistenceDecodeBudget::default())
        .await
        .unwrap()
        .unwrap();
    let changed = repository
        .install_keyed_projection(
            Uuid::now_v7(),
            sample_keyed_projection(target(), Some(2)),
            Some(old),
        )
        .await
        .unwrap();
    assert!(inventory.next_ready().await.unwrap().is_none());
    let mut inventory = service
        .bounded_projection_inventory(local_inventory_bound())
        .await
        .unwrap();
    assert!(inventory.next_listable().await.unwrap().is_none());
    service
        .observe_and_install(Uuid::now_v7(), request(2, Arc::default()), &source(2))
        .await
        .unwrap();
    let mut inventory = service
        .bounded_projection_inventory(local_inventory_bound())
        .await
        .unwrap();
    let fresh = inventory.next_ready().await.unwrap().unwrap();
    assert_eq!(fresh.version, changed.version);
    assert_eq!(
        fresh.projection.facts.metadata_version().snapshot_id(),
        Some(2)
    );
    assert!(inventory.next_ready().await.unwrap().is_none());
}

#[tokio::test]
async fn bounded_inventory_never_displays_a_projection_deleted_after_enumeration() {
    let (repository, service) = service().await;
    service
        .observe_and_install(Uuid::now_v7(), request(1, Arc::default()), &source(1))
        .await
        .unwrap();
    let mut ready = service
        .bounded_projection_inventory(local_inventory_bound())
        .await
        .unwrap();
    let mut listable = service
        .bounded_projection_inventory(local_inventory_bound())
        .await
        .unwrap();
    let old = repository
        .find_keyed_by_target(&target(), PersistenceDecodeBudget::default())
        .await
        .unwrap()
        .unwrap();
    repository
        .delete_keyed_projection(Uuid::now_v7(), old)
        .await
        .unwrap();
    assert!(ready.next_ready().await.unwrap().is_none());
    assert!(listable.next_listable().await.unwrap().is_none());
}

#[tokio::test]
async fn bounded_listable_inventory_preserves_exact_read_only_and_quarantine_reasons() {
    let (_, service) = service().await;
    service
        .observe_current_read_only_and_install(
            Uuid::now_v7(),
            request(1, Arc::default()),
            &source(1),
            "test read-only Current".into(),
        )
        .await
        .unwrap();
    let mut listable = service
        .bounded_projection_inventory(local_inventory_bound())
        .await
        .unwrap();
    let first = listable.next_listable().await.unwrap().unwrap();
    let expected = match service.runtime.readiness(&target()) {
        TargetReadiness::ReadOnly(reason) => reason,
        other => panic!("expected read-only, got {other:?}"),
    };
    assert_eq!(
        first.manageability,
        MvListedManageability::ReadOnly(expected)
    );
    let mut ready = service
        .bounded_projection_inventory(local_inventory_bound())
        .await
        .unwrap();
    assert!(ready.next_ready().await.unwrap().is_none());
    service
        .invalidate_current(target(), "exact quarantine reason".into())
        .await
        .unwrap();
    let mut listable = service
        .bounded_projection_inventory(local_inventory_bound())
        .await
        .unwrap();
    assert_eq!(
        listable
            .next_listable()
            .await
            .unwrap()
            .unwrap()
            .manageability,
        MvListedManageability::Unavailable("exact quarantine reason".into())
    );
}

#[tokio::test]
async fn bounded_listing_refuses_a_large_reason_before_cloning_it_into_a_row() {
    let (_, service) = service().await;
    service
        .observe_and_install(Uuid::now_v7(), request(1, Arc::default()), &source(1))
        .await
        .unwrap();
    service
        .invalidate_current(target(), "x".repeat(4 * 1024 * 1024))
        .await
        .unwrap();
    let mut inventory = service
        .bounded_projection_inventory(local_inventory_bound())
        .await
        .unwrap();
    let error = inventory.next_listable().await.unwrap_err();
    assert_eq!(error.kind(), MvRepositoryErrorKind::InvalidRequest);
    assert!(error.to_string().contains("readiness reason"));
}

#[tokio::test]
async fn bounded_dependency_display_requires_ready_and_exact_installed_root_version() {
    use crate::repository::MvDependencyReadBound;
    let (repository, service) = service().await;
    service
        .observe_and_install(Uuid::now_v7(), request(1, Arc::default()), &source(1))
        .await
        .unwrap();
    let loaded = service.load_ready(&target()).await.unwrap().unwrap();
    let bound = MvDependencyReadBound {
        inventory: local_inventory_bound(),
        entries: 4096,
        collection_bytes: 4 * 1024 * 1024,
    };
    let rows = service
        .list_ready_dependencies_by_downstream_bounded(&loaded, bound)
        .await
        .unwrap();
    assert_eq!(
        rows,
        service
            .list_ready_dependencies_by_downstream(&loaded)
            .await
            .unwrap()
    );
    let tiny = MvDependencyReadBound {
        collection_bytes: 1,
        ..bound
    };
    assert_eq!(
        service
            .list_ready_dependencies_by_downstream_bounded(&loaded, tiny)
            .await
            .unwrap_err()
            .kind(),
        MvRepositoryErrorKind::InvalidRequest
    );
    let fresh = repository
        .install_keyed_projection(
            Uuid::now_v7(),
            sample_keyed_projection(target(), Some(2)),
            Some(loaded.clone()),
        )
        .await
        .unwrap();
    assert_eq!(
        service
            .list_ready_dependencies_by_downstream_bounded(&loaded, bound)
            .await
            .unwrap_err()
            .kind(),
        MvRepositoryErrorKind::Conflict
    );
    assert_eq!(
        service
            .list_ready_dependencies_by_downstream_bounded(&fresh, bound)
            .await
            .unwrap_err()
            .kind(),
        MvRepositoryErrorKind::Unavailable
    );
    service
        .observe_and_install(Uuid::now_v7(), request(2, Arc::default()), &source(2))
        .await
        .unwrap();
    assert!(
        service
            .list_ready_dependencies_by_downstream_bounded(&fresh, bound)
            .await
            .is_ok()
    );
    assert_eq!(
        service
            .list_ready_dependencies_by_downstream_bounded(&loaded, bound)
            .await
            .unwrap_err()
            .kind(),
        MvRepositoryErrorKind::Unavailable
    );
}

#[path = "readiness_original_tests.rs"]
mod original;
