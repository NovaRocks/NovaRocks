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

use super::tests_definition;

use crate::persistence::validation::PersistenceDecodeBudget;
use crate::repository::MvRepositoryErrorKind;

#[tokio::test]
async fn keyed_owner_exposes_whole_projection_cas_and_guarded_delete() {
    let (_, repository) = tests_definition::repository().await;
    let created = repository
        .install_keyed_projection(
            uuid::Uuid::now_v7(),
            tests_definition::projection_facts("orders_mv", b"target-object", 61, "orders"),
            None,
        )
        .await
        .expect("create projection through port");
    assert_eq!(
        repository
            .find_keyed_by_target(
                &tests_definition::target("orders_mv"),
                PersistenceDecodeBudget::default()
            )
            .await
            .unwrap(),
        Some(created.clone())
    );

    let replaced = repository
        .install_keyed_projection(
            uuid::Uuid::now_v7(),
            tests_definition::projection_facts("orders_mv", b"target-object", 62, "customers"),
            Some({
                let mut expected = created.clone();
                expected.version = (created.version.clone()).clone();
                expected
            }),
        )
        .await
        .unwrap();
    let stale_delete = repository
        .delete_keyed_projection(uuid::Uuid::now_v7(), {
            let mut expected = created.clone();
            expected.version = (created.version).clone();
            expected.projection.facts.source =
                (created.projection.facts.source_revision().clone()).clone();
            expected
        })
        .await
        .expect_err("stale root version must not delete the replacement");
    assert_eq!(stale_delete.kind(), MvRepositoryErrorKind::Conflict);

    repository
        .delete_keyed_projection(uuid::Uuid::now_v7(), {
            let mut expected = replaced.clone();
            expected.version = (replaced.version).clone();
            expected.projection.facts.source =
                (replaced.projection.facts.source_revision().clone()).clone();
            expected
        })
        .await
        .expect("exact guarded delete");
    assert!(
        repository
            .list_keyed_projections()
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn keyed_owner_reports_a_lost_committed_response_without_retry_or_rollback() {
    use novarocks_state_store_runtime::StateStoreRunPolicy;
    use novarocks_state_store_testkit::conformance::{FaultGate, FaultInjectingStateStore};
    let (store, reader) = tests_definition::repository().await;
    let fault = FaultInjectingStateStore::new(store);
    let writer = super::StateStoreMvRepository::open(fault.clone(), StateStoreRunPolicy::default())
        .await
        .unwrap();
    let expected = tests_definition::projection_facts("unknown", b"unknown-object", 71, "orders");
    let candidate = expected.clone();
    let gate = FaultGate::new();
    fault.lose_next_post_dispatch_response(gate.clone());
    let task = tokio::spawn(async move {
        writer
            .install_keyed_projection(uuid::Uuid::now_v7(), candidate, None)
            .await
    });
    tokio::time::timeout(std::time::Duration::from_secs(5), gate.wait_armed())
        .await
        .expect("real commit reaches response-loss gate");
    gate.release().await;
    let error = tokio::time::timeout(std::time::Duration::from_secs(5), task)
        .await
        .expect("unknown result is bounded")
        .unwrap()
        .unwrap_err();
    assert_eq!(error.kind(), MvRepositoryErrorKind::CommitUnknown);
    assert!(
        error
            .original_failure()
            .unwrap()
            .downcast_ref::<novarocks_state_store_api::StateStoreError>()
            .is_some()
    );
    // Explicitly adjudicate the exact observation returned by this original
    // legacy-provider fault fixture. No operation-id lookup or write replay.
    let observation = error
        .commit_observation()
        .expect("retain issued unknown observation");
    assert!(matches!(
        observation.outcome().await.unwrap(),
        novarocks_state_store_api::AttemptOutcome::Committed(_)
    ));

    let committed = reader
        .find_keyed_by_target(
            &tests_definition::target("unknown"),
            PersistenceDecodeBudget::default(),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(committed.projection.facts, expected);
    assert_eq!(
        reader.list_keyed_projections().await.unwrap(),
        vec![committed.clone()]
    );
    assert_eq!(
        reader
            .list_keyed_dependencies_by_downstream(&committed)
            .await
            .unwrap()
            .len(),
        2
    );
    // A hidden retry would turn the unknown create into a conflict; no second
    // root/sequence allocation or compensating delete may be hidden here.
    let next = reader
        .install_keyed_projection(
            uuid::Uuid::now_v7(),
            tests_definition::projection_facts("after_unknown", b"after-object", 72, "customers"),
            None,
        )
        .await
        .unwrap();
    assert_eq!(next.projection.mv_id, committed.projection.mv_id + 1);
}

#[tokio::test]
async fn keyed_owner_pre_dispatch_failure_is_unavailable_with_no_projection() {
    use novarocks_state_store_api::{StateStoreError, StateStoreErrorKind};
    use novarocks_state_store_runtime::StateStoreRunPolicy;
    use novarocks_state_store_testkit::conformance::FaultInjectingStateStore;
    let (store, reader) = tests_definition::repository().await;
    let fault = FaultInjectingStateStore::new(store);
    let writer = super::StateStoreMvRepository::open(fault.clone(), StateStoreRunPolicy::default())
        .await
        .unwrap();
    fault.fail_next_begin(StateStoreError::new(
        StateStoreErrorKind::ProviderUnavailable,
        "injected pre-dispatch failure",
    ));
    let error = writer
        .install_keyed_projection(
            uuid::Uuid::now_v7(),
            tests_definition::projection_facts("failed", b"failed-object", 73, "orders"),
            None,
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), MvRepositoryErrorKind::Unavailable);
    assert!(
        reader
            .find_keyed_by_target(
                &tests_definition::target("failed"),
                PersistenceDecodeBudget::default()
            )
            .await
            .unwrap()
            .is_none()
    );
    assert!(reader.list_keyed_projections().await.unwrap().is_empty());
}
