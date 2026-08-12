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

use std::collections::{BTreeMap, BTreeSet};

use novarocks::mv::dependency::model::{
    MvDependencyObjectRef, MvDependencyObjectType, MvDependencyStorageEngine,
};
use novarocks::mv::persistence::definition::CreateMvDefinitionRequest;
use novarocks::mv::persistence::partition::ReplaceMvPartitionStatesRequest;
use novarocks::mv::persistence::refresh::MvRefreshFinalizeRequest;
use novarocks::mv::repository::{
    CreateMvDependencyRequest, CreateMvRepositoryRequest, InitialMvRefreshConfiguration,
    MvRepository, MvTarget,
};
use novarocks::mv::test_repository::InMemoryMvRepository;
use uuid::Uuid;

fn target() -> MvTarget {
    MvTarget {
        catalog: Some("ice".to_string()),
        database: "analytics".to_string(),
        name: "orders_mv".to_string(),
    }
}

fn dependency() -> MvDependencyObjectRef {
    MvDependencyObjectRef {
        catalog: Some("ice".to_string()),
        database_or_namespace: "sales".to_string(),
        name: "orders".to_string(),
        object_type: MvDependencyObjectType::Table,
        storage_engine: MvDependencyStorageEngine::Iceberg,
    }
}

#[test]
fn provider_neutral_repository_preserves_create_refresh_partition_and_dependency_contracts() {
    let repository = InMemoryMvRepository::default();
    let definition = repository
        .create(
            Uuid::now_v7(),
            CreateMvRepositoryRequest {
                definition: CreateMvDefinitionRequest {
                    select_sql: "SELECT id FROM ice.sales.orders".to_string(),
                    base_table_refs: vec!["ice.sales.orders".to_string()],
                    primary_key_columns: vec!["id".to_string()],
                    storage_engine: "iceberg".to_string(),
                    target_catalog: target().catalog,
                    target_namespace: Some("analytics".to_string()),
                    target_table: Some("orders_mv".to_string()),
                    schema_contract: None,
                    partition_spec: None,
                    created_at_ms: 1,
                },
                refresh: InitialMvRefreshConfiguration::default(),
                dependencies: vec![CreateMvDependencyRequest {
                    upstream: dependency(),
                    created_at_ms: 1,
                }],
            },
        )
        .expect("create definition through port");

    assert_eq!(
        repository.find_by_target(&target()).expect("find"),
        Some(definition.clone())
    );
    assert_eq!(
        repository
            .list_dependencies_by_downstream(definition.mv_id)
            .expect("dependencies")
            .len(),
        1
    );

    let refresh = repository
        .begin_refresh_intent(definition.mv_id, BTreeMap::new())
        .expect("begin refresh through port");
    repository
        .finalize_refresh_with_partitions(
            novarocks::mv::repository::FinalizeMvRefreshWithPartitionsRequest {
                refresh: MvRefreshFinalizeRequest {
                    refresh_id: refresh.refresh_id,
                    rows: 3,
                    base_snapshots: BTreeMap::new(),
                    base_table_uuids: BTreeMap::new(),
                    target_snapshot_id: Some(7),
                    partition_spec: None,
                },
                partitions: Some(ReplaceMvPartitionStatesRequest {
                    mv_id: definition.mv_id,
                    partition_keys: BTreeSet::from(["spec=0".to_string()]),
                    last_refresh_ms: 2,
                    base_snapshots: BTreeMap::new(),
                    target_snapshot_id: Some(7),
                    last_refresh_id: refresh.refresh_id,
                    max_entries: 10,
                }),
            },
        )
        .expect("finalize through port");

    assert_eq!(
        repository
            .list_partition_states(definition.mv_id)
            .expect("partitions")
            .len(),
        1
    );
    assert!(
        repository
            .load_refresh(refresh.refresh_id)
            .expect("refresh")
            .is_some()
    );
}
