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

use novarocks::meta::keys::{NS_ICEBERG_OPERATION, NS_JOB};
use novarocks::meta::repository::iceberg_operation::{
    CreateIcebergOperationRequest, IcebergCleanupOutcomeRecord, IcebergCommitOutcomeRecord,
    IcebergOperationFactUpdate, IcebergOperationFailureKind, IcebergOperationFailureRecord,
    IcebergOperationKind, IcebergOperationNextAction, IcebergOperationRepository,
    IcebergOperationState, IcebergOperationTarget, IcebergRecoveryEvidenceRecord,
    StoredIcebergOperation,
};
use novarocks::meta::repository::job::{CreateEraseJobRequest, JobMetaRepository, JobState};
use novarocks::meta::repository::{
    RepositoryError, RepositoryErrorKind, decode_payload_for_kind, encode_record_payload, id_scopes,
};
use novarocks::meta::{MetaStoreProvider, SqliteMetaStoreProvider};
use std::collections::BTreeMap;

#[test]
fn repository_avro_payload_round_trips_iceberg_operation_payload()
-> Result<(), Box<dyn std::error::Error>> {
    let value = StoredIcebergOperation {
        operation_id: 42,
        operation_kind: IcebergOperationKind::Maintenance,
        operation_subkind: Some("MV_REPARTITION".to_string()),
        target: IcebergOperationTarget {
            catalog: "ice".to_string(),
            namespace: "analytics".to_string(),
            table: "mv_sales".to_string(),
            ref_name: Some("main".to_string()),
        },
        state: IcebergOperationState::CommitUnknown,
        attempt_id: "attempt-1".to_string(),
        base_snapshot_id: Some(7),
        base_snapshot_map: BTreeMap::from([("ice.sales.orders".to_string(), 3)]),
        staged_artifacts: vec!["s3://warehouse/mv/_staging/a.parquet".to_string()],
        commit_request: Some("commit-request-json".to_string()),
        commit_outcome: Some(IcebergCommitOutcomeRecord {
            snapshot_id: 1001,
            written_manifest_paths: vec![
                "s3://warehouse/mv/metadata/manifest-a.avro".to_string(),
                "s3://warehouse/mv/metadata/manifest-b.avro".to_string(),
            ],
        }),
        cleanup_outcome: Some(IcebergCleanupOutcomeRecord {
            attempted: true,
            error_count: 1,
            error_paths: vec!["s3://warehouse/mv/_staging/orphan.parquet".to_string()],
        }),
        recovery_evidence: Some(IcebergRecoveryEvidenceRecord {
            table_ident: "ice.analytics.mv_sales".to_string(),
            commit_op_kind: "fast_append".to_string(),
            base_snapshot_id: Some(7),
            base_sequence_number: Some(11),
            staging_dir: "s3://warehouse/mv/_staging/attempt-1".to_string(),
        }),
        failure: Some(IcebergOperationFailureRecord {
            kind: IcebergOperationFailureKind::Unknown,
            message: "commit status is unknown".to_string(),
            next_action: IcebergOperationNextAction::ManualInspect,
        }),
        created_at_ms: 1000,
        updated_at_ms: 1200,
        finished_at_ms: None,
    };

    let payload = encode_record_payload("iceberg.operation", &value)?;
    assert_eq!(payload.encoding, novarocks::meta::MetaPayloadEncoding::Avro);
    assert_eq!(payload.schema_id, 2);
    assert_eq!(payload.schema_fingerprint.len(), 16);

    let decoded: StoredIcebergOperation = decode_payload_for_kind("iceberg.operation", &payload)?;
    assert_eq!(decoded.operation_kind, IcebergOperationKind::Maintenance);
    assert_eq!(decoded.operation_subkind.as_deref(), Some("MV_REPARTITION"));
    assert_eq!(
        decoded
            .commit_outcome
            .as_ref()
            .expect("commit outcome should round-trip")
            .snapshot_id,
        1001
    );
    assert_eq!(
        decoded
            .cleanup_outcome
            .as_ref()
            .expect("cleanup outcome should round-trip")
            .error_paths[0],
        "s3://warehouse/mv/_staging/orphan.parquet"
    );
    assert_eq!(
        decoded
            .recovery_evidence
            .as_ref()
            .expect("recovery evidence should round-trip")
            .base_sequence_number,
        Some(11)
    );
    assert_eq!(
        decoded
            .failure
            .as_ref()
            .expect("failure should round-trip")
            .next_action,
        IcebergOperationNextAction::ManualInspect
    );
    assert_eq!(decoded, value);
    Ok(())
}

#[test]
fn repository_id_scopes_are_stable_strings() {
    assert_eq!(id_scopes::mv_id().as_str(), "mv.id");
    assert_eq!(id_scopes::refresh_id().as_str(), "refresh.id");
    assert_eq!(id_scopes::erase_job().as_str(), "job.erase");
    assert_eq!(id_scopes::iceberg_operation().as_str(), "iceberg.operation");
}

#[test]
fn repository_namespaces_are_stable_strings() {
    assert_eq!(NS_JOB, "job");
    assert_eq!(NS_ICEBERG_OPERATION, "iceberg.operation");
}

#[test]
fn repository_error_display_is_domain_facing() {
    let err = RepositoryError::conflict("operation state changed");
    assert_eq!(
        err.to_string(),
        "metadata repository conflict: operation state changed"
    );
}

fn create_committing_iceberg_operation(
    provider: &SqliteMetaStoreProvider,
    repository: &IcebergOperationRepository,
) -> Result<i64, Box<dyn std::error::Error>> {
    let operation_id = {
        let mut txn = provider.begin_write("create iceberg operation")?;
        let stored = repository.create_operation(
            txn.as_mut(),
            CreateIcebergOperationRequest {
                operation_kind: IcebergOperationKind::InsertAppend,
                operation_subkind: None,
                target: IcebergOperationTarget {
                    catalog: "ice".to_string(),
                    namespace: "sales".to_string(),
                    table: "orders".to_string(),
                    ref_name: None,
                },
                attempt_id: "attempt-1".to_string(),
                base_snapshot_id: Some(10),
                base_snapshot_map: BTreeMap::new(),
                staged_artifacts: vec!["s3://warehouse/orders/_staging/a.parquet".to_string()],
                created_at_ms: 1000,
            },
        )?;
        txn.commit()?;
        stored.operation_id
    };

    {
        let mut txn = provider.begin_write("transition iceberg operation to committing")?;
        repository.transition_operation(
            txn.as_mut(),
            operation_id,
            IcebergOperationState::Committing,
            1100,
        )?;
        txn.commit()?;
    }

    Ok(operation_id)
}

#[test]
fn iceberg_operation_repository_create_load_and_list_unfinished()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let provider = SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite"))?;
    let repository = IcebergOperationRepository::default();

    let operation_id = {
        let mut txn = provider.begin_write("create iceberg operation")?;
        let stored = repository.create_operation(
            txn.as_mut(),
            CreateIcebergOperationRequest {
                operation_kind: IcebergOperationKind::MvRefresh,
                operation_subkind: None,
                target: IcebergOperationTarget {
                    catalog: "ice".to_string(),
                    namespace: "analytics".to_string(),
                    table: "mv_sales".to_string(),
                    ref_name: Some("main".to_string()),
                },
                attempt_id: "attempt-1".to_string(),
                base_snapshot_id: Some(42),
                base_snapshot_map: BTreeMap::from([("ice.sales.orders".to_string(), 7)]),
                staged_artifacts: vec!["s3://warehouse/mv/_staging/a.parquet".to_string()],
                created_at_ms: 1000,
            },
        )?;
        assert_eq!(stored.state, IcebergOperationState::Preparing);
        assert_eq!(stored.created_at_ms, 1000);
        assert_eq!(stored.updated_at_ms, 1000);
        assert_eq!(stored.finished_at_ms, None);
        txn.commit()?;
        stored.operation_id
    };

    let read = provider.begin_read()?;
    let loaded = repository
        .load_operation(read.as_ref(), operation_id)?
        .expect("operation should exist");
    assert_eq!(loaded.operation_id, operation_id);
    assert_eq!(loaded.operation_kind, IcebergOperationKind::MvRefresh);
    assert_eq!(loaded.target.catalog, "ice");
    assert_eq!(loaded.target.namespace, "analytics");
    assert_eq!(loaded.target.table, "mv_sales");
    assert_eq!(loaded.target.ref_name.as_deref(), Some("main"));
    assert_eq!(loaded.base_snapshot_id, Some(42));
    assert_eq!(loaded.base_snapshot_map["ice.sales.orders"], 7);
    assert_eq!(loaded.staged_artifacts.len(), 1);

    let unfinished = repository.list_unfinished_operations(read.as_ref())?;
    assert_eq!(unfinished.len(), 1);
    assert_eq!(unfinished[0].operation_id, operation_id);

    Ok(())
}

#[test]
fn iceberg_operation_repository_records_commit_request() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let provider = SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite"))?;
    let repository = IcebergOperationRepository::default();

    let operation_id = {
        let mut txn = provider.begin_write("create iceberg repartition operation")?;
        let stored = repository.create_operation(
            txn.as_mut(),
            CreateIcebergOperationRequest {
                operation_kind: IcebergOperationKind::Maintenance,
                operation_subkind: Some("MV_REPARTITION".to_string()),
                target: IcebergOperationTarget {
                    catalog: "ice".to_string(),
                    namespace: "analytics".to_string(),
                    table: "mv_orders".to_string(),
                    ref_name: Some("__nova_mv_repartition_1".to_string()),
                },
                attempt_id: "mv-repartition-1".to_string(),
                base_snapshot_id: Some(42),
                base_snapshot_map: BTreeMap::from([("ice.sales.orders".to_string(), 7)]),
                staged_artifacts: vec!["branch:__nova_mv_repartition_1".to_string()],
                created_at_ms: 1000,
            },
        )?;
        txn.commit()?;
        stored.operation_id
    };

    let commit_request = r#"{"kind":"MV_REPARTITION"}"#.to_string();
    {
        let mut txn = provider.begin_write("record iceberg operation commit request")?;
        repository.record_commit_request(
            txn.as_mut(),
            operation_id,
            commit_request.clone(),
            1200,
        )?;
        txn.commit()?;
    }

    let read = provider.begin_read()?;
    let loaded = repository
        .load_operation(read.as_ref(), operation_id)?
        .expect("operation should exist");
    assert_eq!(
        loaded.commit_request.as_deref(),
        Some(commit_request.as_str())
    );
    assert_eq!(loaded.updated_at_ms, 1200);
    assert_eq!(loaded.state, IcebergOperationState::Preparing);

    Ok(())
}

#[test]
fn iceberg_operation_repository_records_commit_unknown_fact_without_finishing()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let provider = SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite"))?;
    let repository = IcebergOperationRepository::default();
    let operation_id = create_committing_iceberg_operation(&provider, &repository)?;
    let recovery = IcebergRecoveryEvidenceRecord {
        table_ident: "ice.sales.orders".to_string(),
        commit_op_kind: "fast_append".to_string(),
        base_snapshot_id: Some(10),
        base_sequence_number: Some(33),
        staging_dir: "s3://warehouse/orders/_staging".to_string(),
    };
    let failure = IcebergOperationFailureRecord {
        kind: IcebergOperationFailureKind::Unknown,
        message: "commit status is unknown".to_string(),
        next_action: IcebergOperationNextAction::ManualInspect,
    };

    {
        let mut txn = provider.begin_write("record commit unknown iceberg operation fact")?;
        repository.record_operation_fact(
            txn.as_mut(),
            IcebergOperationFactUpdate {
                operation_id,
                state: IcebergOperationState::CommitUnknown,
                commit_outcome: None,
                cleanup_outcome: None,
                recovery_evidence: Some(recovery.clone()),
                failure: Some(failure.clone()),
                now_ms: 1200,
            },
        )?;
        txn.commit()?;
    }

    let read = provider.begin_read()?;
    let loaded = repository
        .load_operation(read.as_ref(), operation_id)?
        .expect("operation should exist");
    assert_eq!(loaded.state, IcebergOperationState::CommitUnknown);
    assert_eq!(loaded.commit_outcome, None);
    assert_eq!(loaded.cleanup_outcome, None);
    assert_eq!(loaded.recovery_evidence, Some(recovery));
    assert_eq!(loaded.failure, Some(failure));
    assert_eq!(loaded.updated_at_ms, 1200);
    assert_eq!(loaded.finished_at_ms, None);

    Ok(())
}

#[test]
fn iceberg_operation_repository_preserves_commit_unknown_evidence_when_recovered_to_committed()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let provider = SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite"))?;
    let repository = IcebergOperationRepository::default();
    let operation_id = create_committing_iceberg_operation(&provider, &repository)?;
    let recovery = IcebergRecoveryEvidenceRecord {
        table_ident: "ice.sales.orders".to_string(),
        commit_op_kind: "fast_append".to_string(),
        base_snapshot_id: Some(10),
        base_sequence_number: Some(33),
        staging_dir: "s3://warehouse/orders/_staging".to_string(),
    };
    let failure = IcebergOperationFailureRecord {
        kind: IcebergOperationFailureKind::Unknown,
        message: "commit status is unknown".to_string(),
        next_action: IcebergOperationNextAction::ManualInspect,
    };

    {
        let mut txn = provider.begin_write("record commit unknown iceberg operation fact")?;
        repository.record_operation_fact(
            txn.as_mut(),
            IcebergOperationFactUpdate {
                operation_id,
                state: IcebergOperationState::CommitUnknown,
                commit_outcome: None,
                cleanup_outcome: None,
                recovery_evidence: Some(recovery.clone()),
                failure: Some(failure.clone()),
                now_ms: 1200,
            },
        )?;
        txn.commit()?;
    }

    let commit_outcome = IcebergCommitOutcomeRecord {
        snapshot_id: 55,
        written_manifest_paths: vec!["s3://warehouse/orders/metadata/m0.avro".to_string()],
    };

    {
        let mut txn = provider.begin_write("recover commit unknown as committed")?;
        repository.record_operation_fact(
            txn.as_mut(),
            IcebergOperationFactUpdate {
                operation_id,
                state: IcebergOperationState::Committed,
                commit_outcome: Some(commit_outcome.clone()),
                cleanup_outcome: None,
                recovery_evidence: None,
                failure: None,
                now_ms: 1300,
            },
        )?;
        txn.commit()?;
    }

    let read = provider.begin_read()?;
    let loaded = repository
        .load_operation(read.as_ref(), operation_id)?
        .expect("operation should exist");
    assert_eq!(loaded.state, IcebergOperationState::Committed);
    assert_eq!(loaded.commit_outcome, Some(commit_outcome));
    assert_eq!(loaded.cleanup_outcome, None);
    assert_eq!(loaded.recovery_evidence, Some(recovery));
    assert_eq!(loaded.failure, Some(failure));
    assert_eq!(loaded.updated_at_ms, 1300);
    assert_eq!(loaded.finished_at_ms, None);

    Ok(())
}

#[test]
fn iceberg_operation_repository_records_known_uncommitted_cleanup_and_finishes()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let provider = SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite"))?;
    let repository = IcebergOperationRepository::default();
    let operation_id = create_committing_iceberg_operation(&provider, &repository)?;
    let cleanup = IcebergCleanupOutcomeRecord {
        attempted: true,
        error_count: 2,
        error_paths: vec![
            "s3://warehouse/orders/_staging/a.parquet".to_string(),
            "s3://warehouse/orders/_staging/b.parquet".to_string(),
        ],
    };
    let failure = IcebergOperationFailureRecord {
        kind: IcebergOperationFailureKind::KnownUncommitted,
        message: "commit rejected before metadata update".to_string(),
        next_action: IcebergOperationNextAction::RetryAbort,
    };

    {
        let mut txn = provider.begin_write("record known uncommitted iceberg operation fact")?;
        repository.record_operation_fact(
            txn.as_mut(),
            IcebergOperationFactUpdate {
                operation_id,
                state: IcebergOperationState::FailedKnownUncommitted,
                commit_outcome: None,
                cleanup_outcome: Some(cleanup.clone()),
                recovery_evidence: None,
                failure: Some(failure.clone()),
                now_ms: 1200,
            },
        )?;
        txn.commit()?;
    }

    let read = provider.begin_read()?;
    let loaded = repository
        .load_operation(read.as_ref(), operation_id)?
        .expect("operation should exist");
    assert_eq!(loaded.state, IcebergOperationState::FailedKnownUncommitted);
    assert_eq!(loaded.commit_outcome, None);
    assert_eq!(loaded.cleanup_outcome, Some(cleanup));
    assert_eq!(loaded.recovery_evidence, None);
    assert_eq!(loaded.failure, Some(failure));
    assert_eq!(loaded.updated_at_ms, 1200);
    assert_eq!(loaded.finished_at_ms, Some(1200));

    Ok(())
}

#[test]
fn iceberg_operation_repository_refines_cleanup_on_committed_and_commit_unknown_states()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let provider = SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite"))?;
    let repository = IcebergOperationRepository::default();

    let committed_operation_id = create_committing_iceberg_operation(&provider, &repository)?;
    let commit_outcome = IcebergCommitOutcomeRecord {
        snapshot_id: 55,
        written_manifest_paths: vec!["s3://warehouse/orders/metadata/m0.avro".to_string()],
    };
    {
        let mut txn = provider.begin_write("record committed iceberg operation fact")?;
        repository.record_operation_fact(
            txn.as_mut(),
            IcebergOperationFactUpdate {
                operation_id: committed_operation_id,
                state: IcebergOperationState::Committed,
                commit_outcome: Some(commit_outcome.clone()),
                cleanup_outcome: None,
                recovery_evidence: None,
                failure: None,
                now_ms: 1200,
            },
        )?;
        txn.commit()?;
    }

    let committed_cleanup = IcebergCleanupOutcomeRecord {
        attempted: true,
        error_count: 0,
        error_paths: Vec::new(),
    };
    {
        let mut txn = provider.begin_write("record committed cleanup refinement")?;
        repository.record_operation_fact(
            txn.as_mut(),
            IcebergOperationFactUpdate {
                operation_id: committed_operation_id,
                state: IcebergOperationState::Committed,
                commit_outcome: None,
                cleanup_outcome: Some(committed_cleanup.clone()),
                recovery_evidence: None,
                failure: None,
                now_ms: 1300,
            },
        )?;
        txn.commit()?;
    }

    {
        let mut txn = provider.begin_write("reject committed failure injection")?;
        let err = repository
            .record_operation_fact(
                txn.as_mut(),
                IcebergOperationFactUpdate {
                    operation_id: committed_operation_id,
                    state: IcebergOperationState::Committed,
                    commit_outcome: None,
                    cleanup_outcome: Some(committed_cleanup.clone()),
                    recovery_evidence: None,
                    failure: Some(IcebergOperationFailureRecord {
                        kind: IcebergOperationFailureKind::FinalizeKnownCommitted,
                        message: "unexpected failure injection".to_string(),
                        next_action: IcebergOperationNextAction::RetryFinalize,
                    }),
                    now_ms: 1350,
                },
            )
            .expect_err("cleanup refinement must not inject a new failure");
        assert_eq!(err.kind(), RepositoryErrorKind::Conflict);
    }

    let unknown_operation_id = create_committing_iceberg_operation(&provider, &repository)?;
    let recovery = IcebergRecoveryEvidenceRecord {
        table_ident: "ice.sales.orders".to_string(),
        commit_op_kind: "fast_append".to_string(),
        base_snapshot_id: Some(10),
        base_sequence_number: Some(33),
        staging_dir: "s3://warehouse/orders/_staging".to_string(),
    };
    let failure = IcebergOperationFailureRecord {
        kind: IcebergOperationFailureKind::Unknown,
        message: "commit status is unknown".to_string(),
        next_action: IcebergOperationNextAction::ManualInspect,
    };
    {
        let mut txn = provider.begin_write("record commit unknown iceberg operation fact")?;
        repository.record_operation_fact(
            txn.as_mut(),
            IcebergOperationFactUpdate {
                operation_id: unknown_operation_id,
                state: IcebergOperationState::CommitUnknown,
                commit_outcome: None,
                cleanup_outcome: None,
                recovery_evidence: Some(recovery.clone()),
                failure: Some(failure.clone()),
                now_ms: 1400,
            },
        )?;
        txn.commit()?;
    }

    let unknown_cleanup = IcebergCleanupOutcomeRecord {
        attempted: true,
        error_count: 1,
        error_paths: vec!["s3://warehouse/orders/_staging/orphan.parquet".to_string()],
    };
    {
        let mut txn = provider.begin_write("record commit unknown cleanup refinement")?;
        repository.record_operation_fact(
            txn.as_mut(),
            IcebergOperationFactUpdate {
                operation_id: unknown_operation_id,
                state: IcebergOperationState::CommitUnknown,
                commit_outcome: None,
                cleanup_outcome: Some(unknown_cleanup.clone()),
                recovery_evidence: None,
                failure: None,
                now_ms: 1500,
            },
        )?;
        txn.commit()?;
    }

    let read = provider.begin_read()?;
    let committed = repository
        .load_operation(read.as_ref(), committed_operation_id)?
        .expect("committed operation should exist");
    assert_eq!(committed.state, IcebergOperationState::Committed);
    assert_eq!(committed.commit_outcome, Some(commit_outcome));
    assert_eq!(committed.cleanup_outcome, Some(committed_cleanup));
    assert_eq!(committed.updated_at_ms, 1300);

    let unknown = repository
        .load_operation(read.as_ref(), unknown_operation_id)?
        .expect("unknown operation should exist");
    assert_eq!(unknown.state, IcebergOperationState::CommitUnknown);
    assert_eq!(unknown.cleanup_outcome, Some(unknown_cleanup));
    assert_eq!(unknown.recovery_evidence, Some(recovery));
    assert_eq!(unknown.failure, Some(failure));
    assert_eq!(unknown.updated_at_ms, 1500);

    Ok(())
}

#[test]
fn iceberg_operation_repository_records_same_state_fact_replay_and_cleanup_refinement()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let provider = SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite"))?;
    let repository = IcebergOperationRepository::default();
    let operation_id = create_committing_iceberg_operation(&provider, &repository)?;
    let cleanup = IcebergCleanupOutcomeRecord {
        attempted: false,
        error_count: 0,
        error_paths: Vec::new(),
    };
    let failure = IcebergOperationFailureRecord {
        kind: IcebergOperationFailureKind::KnownUncommitted,
        message: "commit failed before metadata update".to_string(),
        next_action: IcebergOperationNextAction::RetryAbort,
    };

    {
        let mut txn = provider.begin_write("record known uncommitted fact")?;
        repository.record_operation_fact(
            txn.as_mut(),
            IcebergOperationFactUpdate {
                operation_id,
                state: IcebergOperationState::FailedKnownUncommitted,
                commit_outcome: None,
                cleanup_outcome: Some(cleanup.clone()),
                recovery_evidence: None,
                failure: Some(failure.clone()),
                now_ms: 1200,
            },
        )?;
        txn.commit()?;
    }

    {
        let mut txn = provider.begin_write("replay known uncommitted fact")?;
        repository.record_operation_fact(
            txn.as_mut(),
            IcebergOperationFactUpdate {
                operation_id,
                state: IcebergOperationState::FailedKnownUncommitted,
                commit_outcome: None,
                cleanup_outcome: Some(cleanup.clone()),
                recovery_evidence: None,
                failure: Some(failure.clone()),
                now_ms: 1300,
            },
        )?;
        txn.commit()?;
    }

    {
        let read = provider.begin_read()?;
        let loaded = repository
            .load_operation(read.as_ref(), operation_id)?
            .expect("operation should exist");
        assert_eq!(loaded.updated_at_ms, 1200);
        assert_eq!(loaded.finished_at_ms, Some(1200));
    }

    let refined_cleanup = IcebergCleanupOutcomeRecord {
        attempted: true,
        error_count: 0,
        error_paths: Vec::new(),
    };
    let refined_failure = IcebergOperationFailureRecord {
        next_action: IcebergOperationNextAction::None,
        ..failure.clone()
    };

    {
        let mut txn = provider.begin_write("refine known uncommitted cleanup fact")?;
        repository.record_operation_fact(
            txn.as_mut(),
            IcebergOperationFactUpdate {
                operation_id,
                state: IcebergOperationState::FailedKnownUncommitted,
                commit_outcome: None,
                cleanup_outcome: Some(refined_cleanup.clone()),
                recovery_evidence: None,
                failure: Some(refined_failure.clone()),
                now_ms: 1400,
            },
        )?;
        txn.commit()?;
    }

    {
        let mut txn = provider.begin_write("reject conflicting known uncommitted failure fact")?;
        let err = repository
            .record_operation_fact(
                txn.as_mut(),
                IcebergOperationFactUpdate {
                    operation_id,
                    state: IcebergOperationState::FailedKnownUncommitted,
                    commit_outcome: None,
                    cleanup_outcome: Some(refined_cleanup.clone()),
                    recovery_evidence: None,
                    failure: Some(IcebergOperationFailureRecord {
                        message: "different primary failure".to_string(),
                        ..refined_failure.clone()
                    }),
                    now_ms: 1500,
                },
            )
            .expect_err("same-state refinement must not replace the primary failure");
        assert_eq!(err.kind(), RepositoryErrorKind::Conflict);
        assert!(
            err.to_string()
                .contains("conflicting Iceberg operation fact replay"),
            "{err}"
        );
    }

    let read = provider.begin_read()?;
    let loaded = repository
        .load_operation(read.as_ref(), operation_id)?
        .expect("operation should exist");
    assert_eq!(loaded.state, IcebergOperationState::FailedKnownUncommitted);
    assert_eq!(loaded.cleanup_outcome, Some(refined_cleanup));
    assert_eq!(loaded.failure, Some(refined_failure));
    assert_eq!(loaded.updated_at_ms, 1400);
    assert_eq!(loaded.finished_at_ms, Some(1200));

    Ok(())
}

#[test]
fn iceberg_operation_repository_preserves_commit_outcome_on_finalize_failure()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let provider = SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite"))?;
    let repository = IcebergOperationRepository::default();
    let operation_id = create_committing_iceberg_operation(&provider, &repository)?;
    let commit_outcome = IcebergCommitOutcomeRecord {
        snapshot_id: 55,
        written_manifest_paths: vec!["s3://warehouse/orders/metadata/m0.avro".to_string()],
    };

    {
        let mut txn = provider.begin_write("record committed iceberg operation fact")?;
        repository.record_operation_fact(
            txn.as_mut(),
            IcebergOperationFactUpdate {
                operation_id,
                state: IcebergOperationState::Committed,
                commit_outcome: Some(commit_outcome.clone()),
                cleanup_outcome: None,
                recovery_evidence: None,
                failure: None,
                now_ms: 1200,
            },
        )?;
        txn.commit()?;
    }

    {
        let mut txn = provider.begin_write("transition iceberg operation to finalizing")?;
        repository.transition_operation(
            txn.as_mut(),
            operation_id,
            IcebergOperationState::Finalizing,
            1300,
        )?;
        txn.commit()?;
    }

    let finalize_failure = IcebergOperationFailureRecord {
        kind: IcebergOperationFailureKind::FinalizeKnownCommitted,
        message: "mv metadata update failed".to_string(),
        next_action: IcebergOperationNextAction::RetryFinalize,
    };

    {
        let mut txn = provider.begin_write("record finalize failure iceberg operation fact")?;
        repository.record_operation_fact(
            txn.as_mut(),
            IcebergOperationFactUpdate {
                operation_id,
                state: IcebergOperationState::FinalizeFailedKnownCommitted,
                commit_outcome: None,
                cleanup_outcome: None,
                recovery_evidence: None,
                failure: Some(finalize_failure.clone()),
                now_ms: 1400,
            },
        )?;
        txn.commit()?;
    }

    let read = provider.begin_read()?;
    let loaded = repository
        .load_operation(read.as_ref(), operation_id)?
        .expect("operation should exist");
    assert_eq!(
        loaded.state,
        IcebergOperationState::FinalizeFailedKnownCommitted
    );
    assert_eq!(loaded.commit_outcome, Some(commit_outcome));
    assert_eq!(loaded.cleanup_outcome, None);
    assert_eq!(loaded.failure, Some(finalize_failure));
    assert_eq!(loaded.updated_at_ms, 1400);
    assert_eq!(loaded.finished_at_ms, None);

    Ok(())
}

#[test]
fn iceberg_operation_repository_finished_operations_are_not_unfinished()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let provider = SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite"))?;
    let repository = IcebergOperationRepository::default();

    let operation_id = {
        let mut txn = provider.begin_write("create iceberg operation")?;
        let stored = repository.create_operation(
            txn.as_mut(),
            CreateIcebergOperationRequest {
                operation_kind: IcebergOperationKind::InsertAppend,
                operation_subkind: None,
                target: IcebergOperationTarget {
                    catalog: "ice".to_string(),
                    namespace: "sales".to_string(),
                    table: "orders".to_string(),
                    ref_name: None,
                },
                attempt_id: "attempt-1".to_string(),
                base_snapshot_id: None,
                base_snapshot_map: BTreeMap::new(),
                staged_artifacts: Vec::new(),
                created_at_ms: 1000,
            },
        )?;
        txn.commit()?;
        stored.operation_id
    };

    {
        let mut txn = provider.begin_write("transition iceberg operation")?;
        repository.transition_operation(
            txn.as_mut(),
            operation_id,
            IcebergOperationState::Committing,
            1100,
        )?;
        repository.transition_operation(
            txn.as_mut(),
            operation_id,
            IcebergOperationState::Committed,
            1200,
        )?;
        repository.transition_operation(
            txn.as_mut(),
            operation_id,
            IcebergOperationState::Finalized,
            1300,
        )?;
        txn.commit()?;
    }

    {
        let mut txn = provider.begin_write("replay finalized iceberg operation")?;
        repository.transition_operation(
            txn.as_mut(),
            operation_id,
            IcebergOperationState::Finalized,
            1400,
        )?;
        txn.commit()?;
    }

    let read = provider.begin_read()?;
    let loaded = repository
        .load_operation(read.as_ref(), operation_id)?
        .expect("operation should exist");
    assert_eq!(loaded.state, IcebergOperationState::Finalized);
    assert_eq!(loaded.updated_at_ms, 1300);
    assert_eq!(loaded.finished_at_ms, Some(1300));
    assert!(
        repository
            .list_unfinished_operations(read.as_ref())?
            .is_empty()
    );

    Ok(())
}

#[test]
fn job_repository_claim_finish_and_fail_are_state_checked() -> Result<(), Box<dyn std::error::Error>>
{
    let dir = tempfile::tempdir()?;
    let provider = SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite"))?;
    let repository = JobMetaRepository::default();

    let job_id = {
        let mut txn = provider.begin_write("create erase job")?;
        let job = repository.create_erase_job(
            txn.as_mut(),
            CreateEraseJobRequest {
                table_id: 10,
                partition_id: Some(20),
                root_path: "s3://bucket/db/table/partition".to_string(),
                now_ms: 1000,
            },
        )?;
        assert_eq!(job.table_id, 10);
        assert_eq!(job.partition_id, Some(20));
        assert_eq!(job.root_path, "s3://bucket/db/table/partition");
        assert_eq!(job.state, JobState::Pending);
        assert_eq!(job.retry_at_ms, None);
        assert_eq!(job.updated_at_ms, 1000);
        assert_eq!(job.last_error, None);
        txn.commit()?;
        job.job_id
    };

    {
        let mut txn = provider.begin_write("claim and fail erase job")?;
        assert!(repository.claim_erase_job(txn.as_mut(), job_id, 1100)?);
        repository.fail_erase_job(
            txn.as_mut(),
            job_id,
            "object delete failed".to_string(),
            Some(1150),
            1120,
        )?;
        txn.commit()?;
    }

    {
        let mut txn = provider.begin_write("retry erase job")?;
        assert!(repository.claim_erase_job(txn.as_mut(), job_id, 1150)?);
        repository.finish_erase_job(txn.as_mut(), job_id, 1200)?;
        repository.finish_erase_job(txn.as_mut(), job_id, 1210)?;
        txn.commit()?;
    }

    let read = provider.begin_read()?;
    let job = repository
        .load_erase_job(read.as_ref(), job_id)?
        .expect("erase job should exist");
    assert_eq!(job.state, JobState::Finished);
    assert_eq!(job.retry_at_ms, None);
    assert_eq!(job.updated_at_ms, 1200);
    assert_eq!(job.last_error, None);

    Ok(())
}

#[test]
fn job_repository_fail_requires_running_and_can_update_failed_retry()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let provider = SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite"))?;
    let repository = JobMetaRepository::default();

    let pending_id = {
        let mut txn = provider.begin_write("create pending erase job")?;
        let job = repository.create_erase_job(
            txn.as_mut(),
            CreateEraseJobRequest {
                table_id: 10,
                partition_id: Some(20),
                root_path: "s3://bucket/db/table/partition".to_string(),
                now_ms: 1000,
            },
        )?;
        let err = repository
            .fail_erase_job(
                txn.as_mut(),
                job.job_id,
                "not running".to_string(),
                Some(1300),
                1200,
            )
            .expect_err("pending erase job should not fail");
        assert_eq!(err.kind(), RepositoryErrorKind::Conflict);
        txn.commit()?;
        job.job_id
    };

    {
        let read = provider.begin_read()?;
        let job = repository
            .load_erase_job(read.as_ref(), pending_id)?
            .expect("pending job should exist");
        assert_eq!(job.state, JobState::Pending);
        assert_eq!(job.updated_at_ms, 1000);
    }

    let failed_id = {
        let mut txn = provider.begin_write("fail erase job")?;
        let job = repository.create_erase_job(
            txn.as_mut(),
            CreateEraseJobRequest {
                table_id: 11,
                partition_id: None,
                root_path: "s3://bucket/db/table".to_string(),
                now_ms: 1000,
            },
        )?;
        assert!(repository.claim_erase_job(txn.as_mut(), job.job_id, 1100)?);
        repository.fail_erase_job(
            txn.as_mut(),
            job.job_id,
            "first failure".to_string(),
            Some(1300),
            1200,
        )?;
        repository.fail_erase_job(
            txn.as_mut(),
            job.job_id,
            "retry later".to_string(),
            Some(1400),
            1250,
        )?;
        txn.commit()?;
        job.job_id
    };

    let read = provider.begin_read()?;
    let job = repository
        .load_erase_job(read.as_ref(), failed_id)?
        .expect("failed job should exist");
    assert_eq!(job.state, JobState::Failed);
    assert_eq!(job.retry_at_ms, Some(1400));
    assert_eq!(job.updated_at_ms, 1250);
    assert_eq!(job.last_error.as_deref(), Some("retry later"));

    Ok(())
}

#[test]
fn job_repository_claim_failed_honors_retry_at() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let provider = SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite"))?;
    let repository = JobMetaRepository::default();

    let job_id = {
        let mut txn = provider.begin_write("create failed erase job")?;
        let job = repository.create_erase_job(
            txn.as_mut(),
            CreateEraseJobRequest {
                table_id: 10,
                partition_id: Some(20),
                root_path: "s3://bucket/db/table/partition".to_string(),
                now_ms: 1000,
            },
        )?;
        assert!(repository.claim_erase_job(txn.as_mut(), job.job_id, 1100)?);
        repository.fail_erase_job(
            txn.as_mut(),
            job.job_id,
            "retry later".to_string(),
            Some(1500),
            1200,
        )?;
        txn.commit()?;
        job.job_id
    };

    {
        let mut txn = provider.begin_write("claim failed job before retry")?;
        assert!(!repository.claim_erase_job(txn.as_mut(), job_id, 1400)?);
        txn.commit()?;
    }
    {
        let read = provider.begin_read()?;
        let job = repository
            .load_erase_job(read.as_ref(), job_id)?
            .expect("failed job should exist");
        assert_eq!(job.state, JobState::Failed);
        assert_eq!(job.retry_at_ms, Some(1500));
        assert_eq!(job.updated_at_ms, 1200);
        assert_eq!(job.last_error.as_deref(), Some("retry later"));
    }

    {
        let mut txn = provider.begin_write("claim failed job after retry")?;
        assert!(repository.claim_erase_job(txn.as_mut(), job_id, 1500)?);
        txn.commit()?;
    }
    let read = provider.begin_read()?;
    let job = repository
        .load_erase_job(read.as_ref(), job_id)?
        .expect("running job should exist");
    assert_eq!(job.state, JobState::Running);
    assert_eq!(job.retry_at_ms, None);
    assert_eq!(job.updated_at_ms, 1500);
    assert_eq!(job.last_error, None);

    Ok(())
}

#[test]
fn job_repository_lists_pending_and_due_failed_jobs() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let provider = SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite"))?;
    let repository = JobMetaRepository::default();

    let (
        pending_id,
        failed_none_retry_id,
        failed_due_id,
        failed_future_id,
        running_id,
        finished_id,
    ) = {
        let mut txn = provider.begin_write("create runnable erase jobs")?;
        let pending = repository.create_erase_job(
            txn.as_mut(),
            CreateEraseJobRequest {
                table_id: 10,
                partition_id: Some(20),
                root_path: "s3://bucket/db/table/pending".to_string(),
                now_ms: 1000,
            },
        )?;
        let failed_none_retry = repository.create_erase_job(
            txn.as_mut(),
            CreateEraseJobRequest {
                table_id: 11,
                partition_id: Some(21),
                root_path: "s3://bucket/db/table/failed-none".to_string(),
                now_ms: 1000,
            },
        )?;
        assert!(repository.claim_erase_job(txn.as_mut(), failed_none_retry.job_id, 1010)?);
        repository.fail_erase_job(
            txn.as_mut(),
            failed_none_retry.job_id,
            "retry immediately".to_string(),
            None,
            1020,
        )?;
        let failed_due = repository.create_erase_job(
            txn.as_mut(),
            CreateEraseJobRequest {
                table_id: 12,
                partition_id: Some(22),
                root_path: "s3://bucket/db/table/failed-due".to_string(),
                now_ms: 1000,
            },
        )?;
        assert!(repository.claim_erase_job(txn.as_mut(), failed_due.job_id, 1010)?);
        repository.fail_erase_job(
            txn.as_mut(),
            failed_due.job_id,
            "due".to_string(),
            Some(1100),
            1020,
        )?;
        let failed_future = repository.create_erase_job(
            txn.as_mut(),
            CreateEraseJobRequest {
                table_id: 13,
                partition_id: Some(23),
                root_path: "s3://bucket/db/table/failed-future".to_string(),
                now_ms: 1000,
            },
        )?;
        assert!(repository.claim_erase_job(txn.as_mut(), failed_future.job_id, 1010)?);
        repository.fail_erase_job(
            txn.as_mut(),
            failed_future.job_id,
            "future".to_string(),
            Some(1300),
            1020,
        )?;
        let running = repository.create_erase_job(
            txn.as_mut(),
            CreateEraseJobRequest {
                table_id: 14,
                partition_id: Some(24),
                root_path: "s3://bucket/db/table/running".to_string(),
                now_ms: 1000,
            },
        )?;
        assert!(repository.claim_erase_job(txn.as_mut(), running.job_id, 1010)?);
        let finished = repository.create_erase_job(
            txn.as_mut(),
            CreateEraseJobRequest {
                table_id: 15,
                partition_id: Some(25),
                root_path: "s3://bucket/db/table/finished".to_string(),
                now_ms: 1000,
            },
        )?;
        assert!(repository.claim_erase_job(txn.as_mut(), finished.job_id, 1010)?);
        repository.finish_erase_job(txn.as_mut(), finished.job_id, 1020)?;
        txn.commit()?;
        (
            pending.job_id,
            failed_none_retry.job_id,
            failed_due.job_id,
            failed_future.job_id,
            running.job_id,
            finished.job_id,
        )
    };

    let read = provider.begin_read()?;
    let runnable_ids = repository
        .list_runnable_erase_jobs(read.as_ref(), 1200)?
        .into_iter()
        .map(|job| job.job_id)
        .collect::<Vec<_>>();
    assert_eq!(
        runnable_ids,
        vec![pending_id, failed_none_retry_id, failed_due_id]
    );
    assert!(!runnable_ids.contains(&failed_future_id));
    assert!(!runnable_ids.contains(&running_id));
    assert!(!runnable_ids.contains(&finished_id));

    Ok(())
}

#[test]
fn job_repository_claim_finished_returns_false_without_change()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let provider = SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite"))?;
    let repository = JobMetaRepository::default();

    let job_id = {
        let mut txn = provider.begin_write("create and finish erase job")?;
        let job = repository.create_erase_job(
            txn.as_mut(),
            CreateEraseJobRequest {
                table_id: 10,
                partition_id: Some(20),
                root_path: "s3://bucket/db/table/partition".to_string(),
                now_ms: 1000,
            },
        )?;
        assert!(repository.claim_erase_job(txn.as_mut(), job.job_id, 1100)?);
        repository.finish_erase_job(txn.as_mut(), job.job_id, 1200)?;
        txn.commit()?;
        job.job_id
    };

    {
        let mut txn = provider.begin_write("claim finished erase job")?;
        assert!(!repository.claim_erase_job(txn.as_mut(), job_id, 1300)?);
        txn.commit()?;
    }

    let read = provider.begin_read()?;
    let job = repository
        .load_erase_job(read.as_ref(), job_id)?
        .expect("erase job should exist");
    assert_eq!(job.state, JobState::Finished);
    assert_eq!(job.updated_at_ms, 1200);

    Ok(())
}

#[test]
fn job_repository_finish_pending_returns_conflict() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let provider = SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite"))?;
    let repository = JobMetaRepository::default();

    let mut txn = provider.begin_write("finish pending erase job")?;
    let job = repository.create_erase_job(
        txn.as_mut(),
        CreateEraseJobRequest {
            table_id: 10,
            partition_id: Some(20),
            root_path: "s3://bucket/db/table/partition".to_string(),
            now_ms: 1000,
        },
    )?;
    let err = repository
        .finish_erase_job(txn.as_mut(), job.job_id, 1200)
        .expect_err("pending erase job should not finish");
    assert_eq!(err.kind(), RepositoryErrorKind::Conflict);

    Ok(())
}
