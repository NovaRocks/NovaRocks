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

mod mv_repository_definition;

use novarocks_frontend::mv::domain::repository::{
    DeleteMvProjectionRequest, MvRepository, MvRepositoryErrorKind, ReplaceMvProjectionRequest,
};
use novarocks_frontend::mv::domain::test_repository::InMemoryMvRepository;

#[tokio::test]
async fn provider_neutral_port_exposes_only_whole_projection_cas_and_guarded_delete() {
    let repository = InMemoryMvRepository::default();
    let created = repository
        .create_projection(
            uuid::Uuid::now_v7(),
            mv_repository_definition::projection_request(
                "orders_mv",
                b"target-object",
                61,
                "orders",
            ),
        )
        .await
        .expect("create projection through port");
    assert_eq!(
        repository
            .find_by_target(&mv_repository_definition::target("orders_mv"))
            .await
            .unwrap(),
        Some(created.clone())
    );

    let replaced = repository
        .replace_projection(
            uuid::Uuid::now_v7(),
            ReplaceMvProjectionRequest {
                mv_id: created.definition.mv_id,
                expected_version: created.version.clone(),
                projection: mv_repository_definition::projection_request(
                    "orders_mv",
                    b"target-object",
                    62,
                    "customers",
                ),
            },
        )
        .await
        .unwrap();
    let stale_delete = repository
        .delete_projection(
            uuid::Uuid::now_v7(),
            DeleteMvProjectionRequest {
                mv_id: created.definition.mv_id,
                expected_version: created.version,
                expected_source_revision: created.definition.source_revision,
            },
        )
        .await
        .expect_err("stale root version must not delete the replacement");
    assert_eq!(stale_delete.kind(), MvRepositoryErrorKind::Conflict);

    repository
        .delete_projection(
            uuid::Uuid::now_v7(),
            DeleteMvProjectionRequest {
                mv_id: replaced.definition.mv_id,
                expected_version: replaced.version,
                expected_source_revision: replaced.definition.source_revision,
            },
        )
        .await
        .expect("exact guarded delete");
    assert!(repository.list_projections().await.unwrap().is_empty());
}
