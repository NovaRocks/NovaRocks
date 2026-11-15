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

//! End-to-end OCC retry through the real attempt loop, journal, prepared
//! catalog publisher and Hadoop owner. The recorder injects a late competing
//! version before dispatch; Hadoop's own baseline recheck decides Conflict.
//! Conditional-slot evidence itself is covered by install_tests. These tests
//! never synthesize a publication verdict or manually replay a frozen request.
use super::*;
use crate::catalog::admission::CatalogOperation;
use crate::catalog::admission_test_support::all_admitted_recording;
use crate::commit::attempt::observation::PublicationJournal;
use crate::commit::attempt::prepared::CatalogPublisher;
use crate::commit::attempt::{self, RetryPolicy};
use crate::commit::model::{
    BaseIdentity, FileChanges, IcebergCleanupReport, IsolationLevel, OperationIntent,
    OperationIntentParts, OperationToken, PublicationOutcome, RequestShape, TableTarget,
};
use crate::commit::operation::{IcebergCommitOperation, OperationLimits};
use crate::commit::staging::PreparedChange;
use novarocks_spi::connector::{
    ConnectorInstanceDescriptor, ConnectorInstanceId, ConnectorMutationOperationId,
    ConnectorProviderId, ConnectorStopOwner, ExternalMutationEvidence, ProviderBindingEpoch,
};
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

#[derive(Clone, Copy)]
enum Scenario {
    Reprepare,
    RetryLimit,
    Stop,
    ReplacedTable,
}

fn retry_scenario(scenario: Scenario) {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let bridge = crate::resources::IcebergCatalogRuntime::new(runtime.handle().clone());
    let operation_bridge = bridge.clone();
    // The outer owner outlives every joined provider call, including failures.
    let directory = tempfile::tempdir().unwrap();
    let catalog = {
        let _entered = runtime.enter();
        Arc::new(super::super::tests::test_catalog(
            directory.path().to_str().unwrap(),
        ))
    };
    bridge
        .block_on(async move {
            let table = catalog
                .create_fixture_table(
                    &NamespaceIdent::new("db".into()),
                    super::super::tests::test_creation("t"),
                )
                .await
                .unwrap();
            let metadata_dir = std::path::Path::new(table.metadata().location()).join("metadata");
            let v1 = metadata_dir.join("v1.metadata.json");
            let original_v1 = std::fs::read(&v1).unwrap();
            let original_uuid = table.metadata().uuid();
            let base_metadata = table.metadata().clone();
            let stop = ConnectorStopOwner::new();
            let competitor_stop = stop.clone();
            let dispatches = Arc::new(AtomicUsize::new(0));
            let observed_dispatches = dispatches.clone();
            let competitor_dir = metadata_dir.clone();
            let owner = all_admitted_recording(
                crate::catalog::factory::NovaRocksCatalogFactory::adopt_hadoop_for_test(
                    catalog.clone(),
                ),
                Arc::new(move |_, updates| {
                    // This executes only after the real owner prepared one exact
                    // dispatch. Seed a supported same-table concurrent commit.
                    assert!(updates.iter().any(|update| matches!(update,
                    crate::iceberg::TableUpdate::SetProperties { updates }
                    if updates.get("test.retry.ours").is_some_and(|value| value == "requested"))));
                    let ordinal = observed_dispatches.fetch_add(1, Ordering::SeqCst);
                    if ordinal > 0 && !matches!(scenario, Scenario::RetryLimit) {
                        return;
                    }
                    let mut foreign = serde_json::to_value(&base_metadata).unwrap();
                    foreign["properties"]["test.retry.competitor"] =
                        format!("commit-{}", ordinal + 1).into();
                    if matches!(scenario, Scenario::ReplacedTable) {
                        foreign["table-uuid"] =
                            uuid::Uuid::from_bytes([0x6d; 16]).to_string().into();
                    }
                    let bytes = serde_json::to_vec(&foreign).unwrap();
                    let parsed: TableMetadata = serde_json::from_slice(&bytes).unwrap();
                    assert_eq!(
                        parsed.uuid() != original_uuid,
                        matches!(scenario, Scenario::ReplacedTable)
                    );
                    let target = competitor_dir.join(format!("v{}.metadata.json", ordinal + 2));
                    assert!(!target.exists(), "fault must occupy a fresh version slot");
                    std::fs::write(target, bytes).unwrap();
                    if matches!(scenario, Scenario::Stop) {
                        competitor_stop.request_stop();
                    }
                }),
            );
            let generation = ProviderBindingEpoch::from_bytes([0x6c; 16]);
            owner.bind_publication_generation(generation).unwrap();
            let operation_id = ConnectorMutationOperationId::from_bytes([0x6c; 16]);
            let token = OperationToken::from_mutation(operation_id);
            let context = ConnectorRequestContext::try_new(
                Instant::now() + Duration::from_secs(30),
                stop.view(),
                2 * 1024 * 1024,
                4 * 1024 * 1024,
            )
            .unwrap();
            let operation = IcebergCommitOperation::new(
                token,
                table.metadata().location(),
                catalog.binding.clone().unwrap(),
                context,
                operation_bridge,
                OperationLimits::default(),
            )
            .unwrap();
            let intent = OperationIntent::new(OperationIntentParts {
                target: TableTarget {
                    ident: table.identifier().clone(),
                    uuid: Some(original_uuid),
                },
                target_ref: "main".into(),
                start: None,
                changes: FileChanges::default(),
                dependencies: vec![],
                isolation: IsolationLevel::Snapshot,
                shape: RequestShape::MetadataOnly,
                summary: Default::default(),
                token,
            })
            .unwrap();
            assert!(intent.permits_conflict_retry());
            let change = PreparedChange {
                requirements: vec![],
                updates: vec![crate::iceberg::TableUpdate::SetProperties {
                    updates: HashMap::from([("test.retry.ours".into(), "requested".into())]),
                }],
            };
            let evidence = ExternalMutationEvidence::try_new(
                2,
                ConnectorInstanceDescriptor {
                    provider_id: ConnectorProviderId::parse("iceberg").unwrap(),
                    instance_id: ConnectorInstanceId::parse("hadoop-retry-test").unwrap(),
                },
                generation,
                operation_id,
                "alter_properties",
                Bytes::from_static(b"{}"),
            )
            .unwrap();
            let journal = PublicationJournal::new(token);
            let preflight_journal = journal.clone();
            let preparations = Arc::new(Mutex::new(Vec::new()));
            let captured_preparations = preparations.clone();
            let publisher = journal.observe_prepared(Arc::new(CatalogPublisher {
                expected_table_uuid: None,
                catalog_operation: CatalogOperation::AlterProperties,
                catalog: owner,
                ident: table.identifier().clone(),
                target_ref: "main".into(),
                operation: token,
                recovery_preflight: Arc::new(move |request, operation| {
                    assert_eq!(request.artifacts().attempt().operation(), operation.token());
                    let facts = crate::commit::recovery::FrozenPublicationFacts::from_request(
                        request, operation,
                    )?;
                    facts.validate(operation.token())?;
                    let BaseIdentity::Existing {
                        uuid,
                        metadata_location,
                        ..
                    } = request.base()
                    else {
                        panic!("an UPDATE retry must retain an existing table identity")
                    };
                    captured_preparations.lock().unwrap().push((
                        request.artifacts().attempt().ordinal(),
                        metadata_location.clone(),
                        *uuid,
                    ));
                    preflight_journal.record(evidence.clone(), (), request)
                }),
            }));
            let policy = RetryPolicy::from_properties(&HashMap::from([
                ("commit.retry.num-retries".into(), "1".into()),
                ("commit.retry.min-wait-ms".into(), "0".into()),
                ("commit.retry.max-wait-ms".into(), "0".into()),
                ("commit.retry.total-timeout-ms".into(), "30000".into()),
            ]))
            .unwrap();
            let report = attempt::run(&operation, &intent, &change, &[], &publisher, policy).await;
            assert!(
                matches!(report.cleanup, IcebergCleanupReport::Complete { .. }),
                "actual operation cleanup must finish"
            );
            let prepared = preparations.lock().unwrap();
            let expected_attempts = if matches!(scenario, Scenario::Stop | Scenario::ReplacedTable)
            {
                1
            } else {
                2
            };
            assert_eq!(dispatches.load(Ordering::SeqCst), expected_attempts);
            assert_eq!(prepared.len(), expected_attempts);
            for (index, (ordinal, location, uuid)) in prepared.iter().enumerate() {
                assert_eq!(*ordinal, index as u32);
                assert_eq!(
                    *uuid, original_uuid,
                    "retries cannot adopt another table UUID"
                );
                let expected_location = metadata_dir
                    .join(format!("v{}.metadata.json", index + 1))
                    .to_string_lossy()
                    .into_owned();
                assert_eq!(location, &expected_location);
            }
            assert_eq!(std::fs::read(v1).unwrap(), original_v1);
            let v2: TableMetadata = serde_json::from_slice(
                &std::fs::read(metadata_dir.join("v2.metadata.json")).unwrap(),
            )
            .unwrap();
            assert_eq!(
                v2.properties()
                    .get("test.retry.competitor")
                    .map(String::as_str),
                Some("commit-1")
            );
            assert!(!v2.properties().contains_key("test.retry.ours"));
            match scenario {
                Scenario::Reprepare => {
                    let PublicationOutcome::Committed(proof) = report.publication else {
                        panic!("one definite conflict must reload and commit against v2")
                    };
                    assert_eq!(
                        proof.metadata_location.as_deref(),
                        Some(metadata_dir.join("v3.metadata.json").to_str().unwrap())
                    );
                    assert_eq!(
                        proof.table_uuid.as_deref(),
                        Some(original_uuid.to_string().as_str())
                    );
                    let v3: TableMetadata = serde_json::from_slice(
                        &std::fs::read(metadata_dir.join("v3.metadata.json")).unwrap(),
                    )
                    .unwrap();
                    assert_eq!(v3.uuid(), original_uuid);
                    assert_eq!(
                        v3.properties().get("test.retry.ours").map(String::as_str),
                        Some("requested")
                    );
                    assert_eq!(
                        v3.properties()
                            .get("test.retry.competitor")
                            .map(String::as_str),
                        Some("commit-1"),
                        "the new request must inherit the newly loaded base"
                    );
                    assert_eq!(
                        std::fs::read_to_string(metadata_dir.join("version-hint.text"))
                            .unwrap()
                            .trim(),
                        "3"
                    );
                }
                Scenario::RetryLimit => {
                    let PublicationOutcome::KnownUncommitted(failure) = report.publication else {
                        panic!("exhausted conflict retries must remain definitely uncommitted")
                    };
                    assert_eq!(failure.kind(), ConnectorMutationFailureKind::Conflict);
                    let v3: TableMetadata = serde_json::from_slice(
                        &std::fs::read(metadata_dir.join("v3.metadata.json")).unwrap(),
                    )
                    .unwrap();
                    assert_eq!(
                        v3.properties()
                            .get("test.retry.competitor")
                            .map(String::as_str),
                        Some("commit-2")
                    );
                    assert!(!v3.properties().contains_key("test.retry.ours"));
                }
                Scenario::Stop => {
                    let PublicationOutcome::KnownUncommitted(failure) = report.publication else {
                        panic!("stop must prohibit a new preparation/dispatch after known conflict")
                    };
                    assert_eq!(failure.kind(), ConnectorMutationFailureKind::Cancelled);
                    assert!(!metadata_dir.join("v3.metadata.json").exists());
                }
                Scenario::ReplacedTable => {
                    let PublicationOutcome::KnownUncommitted(failure) = report.publication else {
                        panic!("a replaced table UUID must stop re-preparation")
                    };
                    assert_eq!(failure.kind(), ConnectorMutationFailureKind::Conflict);
                    assert!(failure.message().contains("UUID was replaced"));
                    assert!(!metadata_dir.join("v3.metadata.json").exists());
                }
            }
            assert!(
                !metadata_dir.join("v4.metadata.json").exists(),
                "no dispatch may escape the retry/stop/identity bounds"
            );
        })
        .expect("actual provider bridge must join before directory cleanup");
}

#[test]
fn hadoop_conflict_reloads_v2_and_prepares_v3_without_replaying_old_frozen_request() {
    retry_scenario(Scenario::Reprepare);
}

#[test]
fn hadoop_conflict_retry_count_exhaustion_does_not_dispatch_a_third_attempt() {
    retry_scenario(Scenario::RetryLimit);
}

#[test]
fn hadoop_stop_after_known_conflict_prevents_new_preparation_and_dispatch() {
    retry_scenario(Scenario::Stop);
}

#[test]
fn hadoop_retry_cannot_cross_a_replaced_table_uuid() {
    retry_scenario(Scenario::ReplacedTable);
}

#[test]
fn rewrite_captured_uuid_rejects_same_name_replacement_before_any_commit_dispatch() {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let bridge = crate::resources::IcebergCatalogRuntime::new(runtime.handle().clone());
    let operation_bridge = bridge.clone();
    let directory = tempfile::tempdir().unwrap();
    let catalog = {
        let _entered = runtime.enter();
        Arc::new(super::super::tests::test_catalog(
            directory.path().to_str().unwrap(),
        ))
    };
    bridge
        .block_on(async move {
            let table = catalog
                .create_fixture_table(
                    &NamespaceIdent::new("db".into()),
                    super::super::tests::test_creation("t"),
                )
                .await
                .unwrap();
            let captured = table.metadata().uuid();
            let metadata_dir = std::path::Path::new(table.metadata().location()).join("metadata");
            let mut replaced = serde_json::to_value(table.metadata()).unwrap();
            replaced["table-uuid"] = uuid::Uuid::from_u128(901).to_string().into();
            let replacement_bytes = serde_json::to_vec(&replaced).unwrap();
            std::fs::write(metadata_dir.join("v2.metadata.json"), &replacement_bytes).unwrap();
            let dispatches = Arc::new(AtomicUsize::new(0));
            let recorded = dispatches.clone();
            let owner = all_admitted_recording(
                crate::catalog::factory::NovaRocksCatalogFactory::adopt_hadoop_for_test(
                    catalog.clone(),
                ),
                Arc::new(move |_, _| {
                    recorded.fetch_add(1, Ordering::SeqCst);
                }),
            );
            owner
                .bind_publication_generation(ProviderBindingEpoch::from_bytes([0x71; 16]))
                .unwrap();
            let token =
                OperationToken::from_mutation(ConnectorMutationOperationId::from_bytes([0x71; 16]));
            let stop = ConnectorStopOwner::new();
            let context = ConnectorRequestContext::try_new(
                Instant::now() + Duration::from_secs(30),
                stop.view(),
                2 * 1024 * 1024,
                4 * 1024 * 1024,
            )
            .unwrap();
            let operation = IcebergCommitOperation::new(
                token,
                table.metadata().location(),
                catalog.binding.clone().unwrap(),
                context,
                operation_bridge,
                OperationLimits::default(),
            )
            .unwrap();
            let intent = OperationIntent::new(OperationIntentParts {
                target: TableTarget {
                    ident: table.identifier().clone(),
                    uuid: Some(captured),
                },
                target_ref: "main".into(),
                start: None,
                changes: FileChanges::default(),
                dependencies: vec![],
                isolation: IsolationLevel::Snapshot,
                shape: RequestShape::MetadataOnly,
                summary: Default::default(),
                token,
            })
            .unwrap();
            let publisher =
                PublicationJournal::<()>::new(token).observe_prepared(Arc::new(CatalogPublisher {
                    expected_table_uuid: Some(captured),
                    catalog_operation: CatalogOperation::RewriteDataFiles,
                    catalog: owner,
                    ident: table.identifier().clone(),
                    target_ref: "main".into(),
                    operation: token,
                    recovery_preflight: Arc::new(|_, _| {
                        panic!("replaced target cannot reach publication preflight")
                    }),
                }));
            let report = attempt::run(
                &operation,
                &intent,
                &PreparedChange {
                    requirements: vec![],
                    updates: vec![crate::iceberg::TableUpdate::SetProperties {
                        updates: HashMap::from([(
                            "test.rewrite.must_not_publish".into(),
                            "true".into(),
                        )]),
                    }],
                },
                &[],
                &publisher,
                RetryPolicy::from_properties(&HashMap::new()).unwrap(),
            )
            .await;
            let PublicationOutcome::KnownUncommitted(failure) = report.publication else {
                panic!("the actual replaced UUID must reject before dispatch")
            };
            assert!(failure.message().contains("TARGET_REPLACED"), "{failure:?}");
            assert!(matches!(
                report.cleanup,
                IcebergCleanupReport::Complete { .. }
            ));
            assert_eq!(dispatches.load(Ordering::SeqCst), 0);
            assert!(!metadata_dir.join("v3.metadata.json").exists());
            assert_eq!(
                std::fs::read(metadata_dir.join("v2.metadata.json")).unwrap(),
                replacement_bytes
            );
        })
        .expect("actual catalog owner and operation cleanup must join");
}
