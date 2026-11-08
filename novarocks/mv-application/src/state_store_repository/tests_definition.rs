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
use crate::persistence::keyed_documents::MvKeyedDocumentProjection;
use crate::persistence::test_support::KeyedProjectionFixture;
use crate::product::MvTarget;
use crate::repository::MvRepositoryErrorKind;
use bytes::Bytes;
use novarocks_spi::connector::ConnectorTableObjectId;
use novarocks_state_store_api::{CommitOutcome, Key, Precondition, StateStore, Value};
use novarocks_state_store_runtime::StateStoreRunPolicy;
use novarocks_state_store_testkit::testing::InMemoryStateStore;
use std::sync::Arc;
pub(crate) async fn repository() -> (Arc<InMemoryStateStore>, Arc<StateStoreMvRepository>) {
    let store = Arc::new(InMemoryStateStore::new(format!(
        "mv-accelerator-test-{}",
        uuid::Uuid::now_v7()
    )));
    let repository = StateStoreMvRepository::open(
        Arc::clone(&store) as Arc<dyn novarocks_state_store_api::StateStore>,
        StateStoreRunPolicy::default(),
    )
    .await
    .expect("open in-memory MV Accelerator repository");
    (store, repository)
}

pub(crate) fn object_id(bytes: &[u8]) -> ConnectorTableObjectId {
    ConnectorTableObjectId::try_new(Bytes::copy_from_slice(bytes)).expect("bounded object ID")
}

pub(crate) fn target(table: &str) -> MvTarget {
    MvTarget::from_parts(Some("ice"), "sales", table)
}

pub(crate) fn projection_facts(
    table: &str,
    object: &[u8],
    snapshot_id: i64,
    dependency: &str,
) -> MvKeyedDocumentProjection {
    let mut fixture = KeyedProjectionFixture::new(target(table), Some(snapshot_id));
    fixture.object_id = object_id(object);
    // Encoded the way the documents encode one, so a fixture dependency and a
    // fixture projection disagree in exactly the ways production ones do.
    let upstream = crate::persistence::test_support::source_object_identity_bytes(
        format!("base-{dependency}").as_bytes(),
    );
    for relation in &mut fixture.definition.relation_occurrences {
        relation.relation_at_binding = dependency.into();
        relation.object_id = upstream.clone();
    }
    for source in &mut fixture.publication.as_mut().unwrap().inputs {
        source.object_id = upstream.clone();
    }
    fixture.build().unwrap()
}

#[tokio::test]
async fn reopening_the_repository_retains_the_exact_lake_source_projection() {
    let (store, repository) = repository().await;
    let created = repository
        .install_keyed_projection(
            uuid::Uuid::now_v7(),
            projection_facts("retained", b"retained-object", 9, "orders"),
            None,
        )
        .await
        .unwrap();
    drop(repository);

    let reopened = StateStoreMvRepository::open(
        store as Arc<dyn novarocks_state_store_api::StateStore>,
        StateStoreRunPolicy::default(),
    )
    .await
    .expect("reopen MV Accelerator repository");
    assert_eq!(
        reopened
            .find_keyed_by_target(&created.projection.facts.target())
            .await
            .unwrap()
            .unwrap()
            .projection,
        created.projection
    );
}

#[tokio::test]
async fn whole_keyed_projection_cas_replaces_root_and_dependency_indexes() {
    let (_host, repository) = repository().await;
    let created = repository
        .install_keyed_projection(
            uuid::Uuid::now_v7(),
            projection_facts("orders_mv", b"object-a", 11, "orders"),
            None,
        )
        .await
        .expect("create projection");
    let stale_version = created.version.clone();

    let replaced = repository
        .install_keyed_projection(
            uuid::Uuid::now_v7(),
            projection_facts("orders_mv", b"object-a", 12, "customers"),
            Some({
                let mut expected = created.clone();
                expected.version = (created.version).clone();
                expected
            }),
        )
        .await
        .expect("replace projection");

    assert_eq!(
        repository
            .find_keyed_by_target(&target("orders_mv"))
            .await
            .unwrap()
            .unwrap(),
        replaced
    );
    let dependencies = repository
        .list_keyed_dependencies_by_downstream(&replaced)
        .await
        .unwrap();
    assert_eq!(dependencies.len(), 2);
    assert_eq!(dependencies[0].upstream.name, "customers");

    let error = repository
        .install_keyed_projection(
            uuid::Uuid::now_v7(),
            projection_facts("orders_mv", b"object-a", 13, "orders"),
            Some({
                let mut expected = replaced.clone();
                expected.version = (stale_version).clone();
                expected
            }),
        )
        .await
        .expect_err("stale CAS must fail");
    assert_eq!(error.kind(), MvRepositoryErrorKind::Conflict);
    assert_eq!(
        repository
            .find_keyed_by_target(&target("orders_mv"))
            .await
            .unwrap(),
        Some(replaced.clone())
    );
    assert!(
        repository
            .list_keyed_dependencies_by_downstream(&replaced)
            .await
            .unwrap()
            .iter()
            .all(|row| row.upstream.name == "customers")
    );
}

#[tokio::test]
async fn different_target_install_refuses_without_changing_either_projection() {
    let (_, repository) = repository().await;
    let first = repository
        .install_keyed_projection(
            uuid::Uuid::now_v7(),
            projection_facts("first", b"object-first", 21, "orders"),
            None,
        )
        .await
        .unwrap();
    let second = repository
        .install_keyed_projection(
            uuid::Uuid::now_v7(),
            projection_facts("second", b"object-second", 22, "customers"),
            None,
        )
        .await
        .unwrap();
    for target_name in ["second", "unoccupied"] {
        let error = repository
            .install_keyed_projection(
                uuid::Uuid::now_v7(),
                projection_facts(target_name, b"object-first", 23, "lineitem"),
                Some(first.clone()),
            )
            .await
            .unwrap_err();
        assert_eq!(error.kind(), MvRepositoryErrorKind::InvalidRequest);
    }
    assert_eq!(
        repository
            .find_keyed_by_target(&target("first"))
            .await
            .unwrap(),
        Some(first.clone())
    );
    assert_eq!(
        repository
            .find_keyed_by_target(&target("second"))
            .await
            .unwrap(),
        Some(second.clone())
    );
    assert!(
        repository
            .find_keyed_by_target(&target("unoccupied"))
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        repository
            .list_keyed_dependencies_by_downstream(&first)
            .await
            .unwrap()
            .iter()
            .all(|row| row.upstream.name == "orders")
    );
    assert!(
        repository
            .list_keyed_dependencies_by_downstream(&second)
            .await
            .unwrap()
            .iter()
            .all(|row| row.upstream.name == "customers")
    );
    let error = repository
        .install_keyed_projection(
            uuid::Uuid::now_v7(),
            projection_facts("second", b"another-object", 24, "lineitem"),
            None,
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), MvRepositoryErrorKind::Conflict);
    assert_eq!(
        repository
            .find_keyed_by_target(&target("second"))
            .await
            .unwrap(),
        Some(second)
    );
}

#[tokio::test]
async fn delete_requires_exact_object_source_and_version() {
    let (_host, repository) = repository().await;
    let created = repository
        .install_keyed_projection(
            uuid::Uuid::now_v7(),
            projection_facts("delete_me", b"object-live", 31, "orders"),
            None,
        )
        .await
        .unwrap();
    let mut stale_source = created.projection.facts.source_revision().clone();
    stale_source.target_object_id = object_id(b"object-recreated");
    let error = repository
        .delete_keyed_projection(uuid::Uuid::now_v7(), {
            let mut expected = created.clone();
            expected.version = (created.version.clone()).clone();
            expected.projection.facts.source = (stale_source).clone();
            expected
        })
        .await
        .expect_err("logical name cannot authorize deletion");
    assert_eq!(error.kind(), MvRepositoryErrorKind::Conflict);
    assert!(
        repository
            .find_keyed_by_target(&created.projection.facts.target())
            .await
            .unwrap()
            .is_some()
    );

    repository
        .delete_keyed_projection(uuid::Uuid::now_v7(), {
            let mut expected = created.clone();
            expected.version = (created.version).clone();
            expected.projection.facts.source =
                (created.projection.facts.source_revision().clone()).clone();
            expected
        })
        .await
        .expect("exact guarded delete");
    assert!(
        repository
            .find_keyed_by_target(&target("delete_me"))
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn whole_family_wipe_allows_internal_id_reallocation() {
    let (_host, repository) = repository().await;
    let first = repository
        .install_keyed_projection(
            uuid::Uuid::now_v7(),
            projection_facts("before_wipe", b"object-before", 41, "orders"),
            None,
        )
        .await
        .unwrap();
    repository
        .wipe_accelerator(uuid::Uuid::now_v7())
        .await
        .expect("wipe current Accelerator family");
    let rebuilt = repository
        .install_keyed_projection(
            uuid::Uuid::now_v7(),
            projection_facts("after_wipe", b"object-after", 42, "orders"),
            None,
        )
        .await
        .unwrap();
    assert_eq!(first.projection.mv_id, rebuilt.projection.mv_id);
}

#[tokio::test]
async fn whole_family_wipe_removes_an_unknown_current_record_without_decoding_it() {
    let (store, repository) = repository().await;
    let key = Key::try_from(Bytes::from_static(
        b"novarocks/frontend/mv/accelerator/v3/unknown/future-record",
    ))
    .unwrap();
    let value = Value::try_from(Bytes::from_static(b"opaque-corrupt-record")).unwrap();
    // The store issues the attempt this write is authorised by; a test cannot
    // mint one, which is exactly the property the new contract is after.
    let (attempt, _observation) = store
        .attempts()
        .reserve()
        .expect("reserve one write attempt");
    let mut transaction = store
        .begin_write(attempt, "inject unknown MV Accelerator record")
        .await
        .unwrap();
    transaction
        .put(key.clone(), value, Precondition::Absent)
        .await
        .unwrap();
    assert!(matches!(
        transaction.commit().await,
        CommitOutcome::Committed(_)
    ));

    repository
        .wipe_accelerator(uuid::Uuid::now_v7())
        .await
        .expect("wipe opaque current record");
    let mut read = store.begin_read().await.unwrap();
    assert!(read.get(&key).await.unwrap().is_none());
    read.abort().await.unwrap();
}
