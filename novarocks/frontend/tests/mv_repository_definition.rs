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
use novarocks_frontend::common::persisted_query_definition::{
    PersistedQueryDefinition, PersistedQueryDialect,
};
use novarocks_frontend::mv::domain::dependency::model::{
    MvDependencyObjectRef, MvDependencyObjectType, MvDependencyStorageEngine,
};
use novarocks_frontend::mv::domain::persistence::definition::{
    CreateMvDefinitionRequest, MvAcceleratorSourceRevision, MvDesiredRefreshPolicy,
};
use novarocks_frontend::mv::domain::persistence::dependency::CreateMvDependencyRequest;
use novarocks_frontend::mv::domain::repository::{
    DeleteMvProjectionRequest, InitialMvRefreshConfiguration, MvProjectionRequest,
    MvPublishedProjection, MvPublishedWaterline, MvRepository, MvRepositoryErrorKind, MvTarget,
    ReplaceMvProjectionRequest,
};
use novarocks_frontend::mv::repository::StateStoreMvRepository;
use novarocks_frontend::state_store::StateStoreRunPolicy;
use novarocks_spi::connector::ConnectorTableObjectId;
use novarocks_state_store_api::{CommitOutcome, Key, Precondition, Value};
#[path = "common/mod.rs"]
mod common;
use common::state_store_fixture::{
    StateStoreAppConfig, StateStoreConfig, StateStoreHost, StateStoreHostConfig,
    StateStoreLimitOverrides, StateStoreProviderConfig, builtin_state_store_provider_registry,
};

pub(crate) async fn repository() -> (
    tempfile::TempDir,
    StateStoreHost,
    Arc<StateStoreMvRepository>,
) {
    let temp = tempfile::tempdir().expect("temporary StateStore directory");
    let cluster_id = format!("mv-accelerator-test-{}", temp.path().display());
    let registry = builtin_state_store_provider_registry().expect("built-in StateStore providers");
    let host = StateStoreHost::open(
        &registry,
        StateStoreHostConfig {
            state_store: StateStoreAppConfig {
                store: StateStoreConfig {
                    cluster_id,
                    limits: StateStoreLimitOverrides::default(),
                    provider: StateStoreProviderConfig::Sqlite {
                        path: temp.path().join("state-store.sqlite"),
                    },
                },
                mysql_client: None,
            },
            foundationdb_client: None,
        },
        Instant::now() + Duration::from_secs(5),
    )
    .await
    .expect("open SQLite StateStore host");
    let store = host.state_store().expect("host exposes StateStore");
    let repository = StateStoreMvRepository::open(store, StateStoreRunPolicy::default())
        .await
        .expect("open MV Accelerator repository");
    (temp, host, repository)
}

pub(crate) fn object_id(bytes: &[u8]) -> ConnectorTableObjectId {
    ConnectorTableObjectId::try_new(Bytes::copy_from_slice(bytes)).expect("bounded object ID")
}

pub(crate) fn target(table: &str) -> MvTarget {
    MvTarget {
        catalog: Some("ice".to_string()),
        database: "sales".to_string(),
        name: table.to_string(),
    }
}

pub(crate) fn projection_request(
    table: &str,
    object: &[u8],
    snapshot_id: i64,
    dependency: &str,
) -> MvProjectionRequest {
    MvProjectionRequest {
        definition: CreateMvDefinitionRequest {
            query_definition: PersistedQueryDefinition::new(
                format!("SELECT * FROM ice.sales.{dependency}"),
                PersistedQueryDialect::StarRocks,
                "ice",
                "sales",
            )
            .unwrap(),
            base_table_refs: vec![format!("ice.sales.{dependency}")],
            primary_key_columns: vec![],
            storage_engine: "iceberg".to_string(),
            target_catalog: Some("ice".to_string()),
            target_namespace: Some("sales".to_string()),
            target_table: Some(table.to_string()),
            schema_contract: None,
            partition_spec: None,
            created_at_ms: 1,
        },
        refresh: InitialMvRefreshConfiguration {
            policy: MvDesiredRefreshPolicy::Manual,
            ..Default::default()
        },
        publication: MvPublishedProjection::Published(MvPublishedWaterline {
            last_refresh_ms: 10,
            last_refresh_rows: 20,
            last_refreshed_iceberg_snapshot_id: snapshot_id,
            base_snapshots: [(format!("ice.sales.{dependency}"), 7)]
                .into_iter()
                .collect(),
            base_table_object_ids: [(
                format!("ice.sales.{dependency}"),
                object_id(format!("base-{dependency}").as_bytes()),
            )]
            .into_iter()
            .collect(),
        }),
        source_revision: MvAcceleratorSourceRevision {
            target_object_id: object_id(object),
            descriptor_content_hash: format!("descriptor-{table}-{snapshot_id}"),
            current_target_snapshot_id: Some(snapshot_id),
        },
        dependencies: vec![CreateMvDependencyRequest {
            upstream: MvDependencyObjectRef {
                catalog: Some("ice".to_string()),
                database_or_namespace: "sales".to_string(),
                name: dependency.to_string(),
                object_type: MvDependencyObjectType::Table,
                storage_engine: MvDependencyStorageEngine::Iceberg,
            },
            created_at_ms: 1,
        }],
    }
}

#[tokio::test]
async fn sqlite_reopen_retains_the_exact_lake_source_projection() {
    let (_temp, host, repository) = repository().await;
    let created = repository
        .create_projection(
            uuid::Uuid::now_v7(),
            projection_request("retained", b"retained-object", 9, "orders"),
        )
        .await
        .unwrap();
    drop(repository);

    let reopened = StateStoreMvRepository::open(
        host.state_store().expect("reopen StateStore"),
        StateStoreRunPolicy::default(),
    )
    .await
    .expect("reopen MV Accelerator repository");
    assert_eq!(
        reopened
            .load_by_id(created.definition.mv_id)
            .await
            .unwrap()
            .unwrap()
            .definition,
        created.definition
    );
}

#[tokio::test]
async fn whole_projection_cas_replaces_root_target_and_dependency_indexes() {
    let (_temp, _host, repository) = repository().await;
    let created = repository
        .create_projection(
            uuid::Uuid::now_v7(),
            projection_request("orders_mv", b"object-a", 11, "orders"),
        )
        .await
        .expect("create projection");
    let stale_version = created.version.clone();

    let replaced = repository
        .replace_projection(
            uuid::Uuid::now_v7(),
            ReplaceMvProjectionRequest {
                mv_id: created.definition.mv_id,
                expected_version: created.version,
                projection: projection_request("orders_mv_v2", b"object-a", 12, "customers"),
            },
        )
        .await
        .expect("replace projection");

    assert!(
        repository
            .find_by_target(&target("orders_mv"))
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        repository
            .find_by_target(&target("orders_mv_v2"))
            .await
            .unwrap()
            .unwrap(),
        replaced
    );
    let dependencies = repository
        .list_dependencies_by_downstream(replaced.definition.mv_id)
        .await
        .unwrap();
    assert_eq!(dependencies.len(), 1);
    assert_eq!(dependencies[0].upstream.name, "customers");

    let error = repository
        .replace_projection(
            uuid::Uuid::now_v7(),
            ReplaceMvProjectionRequest {
                mv_id: replaced.definition.mv_id,
                expected_version: stale_version,
                projection: projection_request("stale", b"object-a", 13, "orders"),
            },
        )
        .await
        .expect_err("stale CAS must fail");
    assert_eq!(error.kind(), MvRepositoryErrorKind::Conflict);
}

#[tokio::test]
async fn replacement_target_conflict_rolls_back_the_whole_projection() {
    let (_temp, _host, repository) = repository().await;
    let first = repository
        .create_projection(
            uuid::Uuid::now_v7(),
            projection_request("first", b"object-first", 21, "orders"),
        )
        .await
        .unwrap();
    repository
        .create_projection(
            uuid::Uuid::now_v7(),
            projection_request("second", b"object-second", 22, "customers"),
        )
        .await
        .unwrap();

    assert!(
        repository
            .replace_projection(
                uuid::Uuid::now_v7(),
                ReplaceMvProjectionRequest {
                    mv_id: first.definition.mv_id,
                    expected_version: first.version.clone(),
                    projection: projection_request("second", b"object-first", 23, "lineitem",),
                },
            )
            .await
            .is_err()
    );
    assert_eq!(
        repository.load_by_id(first.definition.mv_id).await.unwrap(),
        Some(first)
    );
}

#[tokio::test]
async fn delete_requires_exact_object_source_and_version() {
    let (_temp, _host, repository) = repository().await;
    let created = repository
        .create_projection(
            uuid::Uuid::now_v7(),
            projection_request("delete_me", b"object-live", 31, "orders"),
        )
        .await
        .unwrap();
    let mut stale_source = created.definition.source_revision.clone();
    stale_source.target_object_id = object_id(b"object-recreated");
    let error = repository
        .delete_projection(
            uuid::Uuid::now_v7(),
            DeleteMvProjectionRequest {
                mv_id: created.definition.mv_id,
                expected_version: created.version.clone(),
                expected_source_revision: stale_source,
            },
        )
        .await
        .expect_err("logical name cannot authorize deletion");
    assert_eq!(error.kind(), MvRepositoryErrorKind::Conflict);
    assert!(
        repository
            .load_by_id(created.definition.mv_id)
            .await
            .unwrap()
            .is_some()
    );

    repository
        .delete_projection(
            uuid::Uuid::now_v7(),
            DeleteMvProjectionRequest {
                mv_id: created.definition.mv_id,
                expected_version: created.version,
                expected_source_revision: created.definition.source_revision,
            },
        )
        .await
        .expect("exact guarded delete");
    assert!(
        repository
            .find_by_target(&target("delete_me"))
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn whole_family_wipe_allows_internal_id_reallocation() {
    let (_temp, _host, repository) = repository().await;
    let first = repository
        .create_projection(
            uuid::Uuid::now_v7(),
            projection_request("before_wipe", b"object-before", 41, "orders"),
        )
        .await
        .unwrap();
    repository
        .wipe_accelerator(uuid::Uuid::now_v7())
        .await
        .expect("wipe current Accelerator family");
    let rebuilt = repository
        .create_projection(
            uuid::Uuid::now_v7(),
            projection_request("after_wipe", b"object-after", 42, "orders"),
        )
        .await
        .unwrap();
    assert_eq!(first.definition.mv_id, rebuilt.definition.mv_id);
}

#[tokio::test]
async fn whole_family_wipe_removes_an_unknown_current_record_without_decoding_it() {
    let (_temp, host, repository) = repository().await;
    let store = host.state_store().expect("StateStore");
    let key = Key::try_from(Bytes::from_static(
        b"novarocks/frontend/mv/accelerator/v1/unknown/future-record",
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
