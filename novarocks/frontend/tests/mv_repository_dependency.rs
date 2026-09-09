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

use novarocks_frontend::mv::domain::repository::{MvRepository, ReplaceMvProjectionRequest};

#[tokio::test]
async fn dependency_indexes_are_replaced_only_with_the_root_projection_cas() {
    let (_temp, _host, repository) = mv_repository_definition::repository().await;
    let created = repository
        .create_projection(
            uuid::Uuid::now_v7(),
            mv_repository_definition::projection_request(
                "dependency",
                b"dependency-object",
                51,
                "orders",
            ),
        )
        .await
        .unwrap();
    let upstream = created.definition.base_table_refs[0].clone();
    assert_eq!(
        repository
            .list_dependencies_by_downstream(created.definition.mv_id)
            .await
            .unwrap()
            .len(),
        1
    );

    let replacement = mv_repository_definition::projection_request(
        "dependency",
        b"dependency-object",
        52,
        "customers",
    );
    let replaced = repository
        .replace_projection(
            uuid::Uuid::now_v7(),
            ReplaceMvProjectionRequest {
                mv_id: created.definition.mv_id,
                expected_version: created.version,
                projection: replacement,
            },
        )
        .await
        .unwrap();
    let dependencies = repository
        .list_dependencies_by_downstream(replaced.definition.mv_id)
        .await
        .unwrap();
    assert_eq!(dependencies.len(), 1);
    assert_eq!(dependencies[0].upstream.name, "customers");
    assert_ne!(replaced.definition.base_table_refs[0], upstream);
    repository
        .ensure_no_downstream_dependencies(&dependencies[0].upstream)
        .await
        .expect_err("upstream guard must observe the symmetric index");
}
