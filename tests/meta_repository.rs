use std::collections::BTreeMap;

use bytes::Bytes;
use novarocks::meta::keys::{NS_JOB, NS_MANAGED_TXN};
use novarocks::meta::repository::iceberg_catalog::{
    IcebergCatalogMetaRepository, IcebergCatalogProperties,
};
use novarocks::meta::repository::job::{
    CreateEraseJobRequest, CreateIcebergOptimizeJobRequest, IcebergOptimizeJobOutcome,
    IcebergOptimizeJobState, JobMetaRepository, JobState,
};
use novarocks::meta::repository::managed_lake::{
    CreateManagedColumnRequest, CreateManagedDatabaseRequest, CreateManagedTableLayoutRequest,
    CreateManagedTableRequest, ManagedIndexState, ManagedLakeMetaRepository, ManagedPartitionState,
    ManagedTableKind, ManagedTableState, StageManagedMvRefreshRequest, StageManagedTruncateRequest,
};
use novarocks::meta::repository::managed_txn::{
    ManagedLakeTxnRepository, ManagedTxnState, StoredManagedTxn,
};
use novarocks::meta::repository::mv::{
    BeginIcebergMvRefreshRequest, CreateMvDefinitionRequest, MvMetaRepository,
    MvRefreshFinalizeRequest, MvRefreshState, MvTargetLookup, RecordPublishCommitRequest,
    RecordStagingCommitRequest, RefreshCommitMarker, RefreshExternalOutcome,
    UpdateManagedMvRefreshSummaryRequest,
};
use novarocks::meta::repository::{
    RepositoryError, RepositoryErrorKind, decode_json_payload, encode_json_payload, id_scopes,
};
use novarocks::meta::{
    ExpectedRevision, MetaKey, MetaRecordKind, MetaRecordPut, MetaStoreProvider,
    SqliteMetaStoreProvider,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
struct SamplePayload {
    id: i64,
    name: String,
}

fn create_managed_table_with_partition(
    provider: &SqliteMetaStoreProvider,
    repository: &ManagedLakeMetaRepository,
) -> Result<(i64, i64), Box<dyn std::error::Error>> {
    create_named_managed_table_with_partition(provider, repository, "orders")
}

fn create_named_managed_table_with_partition(
    provider: &SqliteMetaStoreProvider,
    repository: &ManagedLakeMetaRepository,
    table_name: &str,
) -> Result<(i64, i64), Box<dyn std::error::Error>> {
    let mut txn = provider.begin_write("create managed lake objects")?;
    let database = repository.create_database(
        txn.as_mut(),
        CreateManagedDatabaseRequest {
            name: format!("{table_name}_db"),
        },
    )?;
    let table = repository.create_table(
        txn.as_mut(),
        CreateManagedTableRequest {
            db_id: database.db_id,
            name: table_name.to_string(),
            keys_type: "DUP_KEYS".to_string(),
            bucket_num: 2,
            current_schema_id: 10,
            state: ManagedTableState::Active,
            kind: ManagedTableKind::Table,
        },
    )?;
    let partition = repository.create_partition(txn.as_mut(), table.table_id, table_name, 1)?;
    txn.commit()?;
    Ok((table.table_id, partition.partition_id))
}

fn put_managed_txn_record(
    txn: &mut dyn novarocks::meta::MetaWriteTxn,
    managed_txn: StoredManagedTxn,
) -> Result<(), Box<dyn std::error::Error>> {
    txn.put(MetaRecordPut::new(
        MetaKey::new(NS_MANAGED_TXN, [managed_txn.txn_id.to_string()])?,
        MetaRecordKind::new("managed.txn")?,
        ExpectedRevision::NotExists,
        encode_json_payload(1, &managed_txn)?,
    ))?;
    Ok(())
}

fn sample_mv_definition_request(select_sql: &str) -> CreateMvDefinitionRequest {
    CreateMvDefinitionRequest {
        select_sql: select_sql.to_string(),
        base_table_refs: vec!["ice.sales.orders".to_string()],
        primary_key_columns: vec!["id".to_string()],
        storage_engine: "managed_lake".to_string(),
        target_catalog: None,
        target_namespace: None,
        target_table: None,
        target_apply_key: None,
        created_at_ms: 7,
    }
}

#[test]
fn repository_payload_json_round_trips() {
    let payload = SamplePayload {
        id: 7,
        name: "orders".to_string(),
    };
    let encoded = encode_json_payload(1, &payload).expect("encode payload");
    assert_eq!(encoded.schema_version, 1);
    assert_eq!(
        encoded.bytes,
        Bytes::from_static(br#"{"id":7,"name":"orders"}"#)
    );

    let decoded: SamplePayload = decode_json_payload(&encoded).expect("decode payload");
    assert_eq!(decoded, payload);
}

#[test]
fn repository_id_scopes_are_stable_strings() {
    assert_eq!(id_scopes::managed_db().as_str(), "managed.db");
    assert_eq!(id_scopes::managed_table().as_str(), "managed.table");
    assert_eq!(id_scopes::managed_partition().as_str(), "managed.partition");
    assert_eq!(id_scopes::managed_index().as_str(), "managed.index");
    assert_eq!(id_scopes::managed_tablet().as_str(), "managed.tablet");
    assert_eq!(id_scopes::managed_txn().as_str(), "managed.txn");
    assert_eq!(id_scopes::mv_id().as_str(), "mv.id");
    assert_eq!(id_scopes::refresh_id().as_str(), "refresh.id");
    assert_eq!(id_scopes::erase_job().as_str(), "job.erase");
    assert_eq!(
        id_scopes::iceberg_optimize_job().as_str(),
        "job.iceberg_optimize"
    );
}

#[test]
fn repository_namespaces_are_stable_strings() {
    assert_eq!(NS_MANAGED_TXN, "managed.txn");
    assert_eq!(NS_JOB, "job");
}

#[test]
fn repository_error_display_is_domain_facing() {
    let err = RepositoryError::conflict("managed txn state changed");
    assert_eq!(
        err.to_string(),
        "metadata repository conflict: managed txn state changed"
    );
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
fn job_repository_runs_iceberg_optimize_lifecycle() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let provider = SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite"))?;
    let repository = JobMetaRepository::default();
    let outcome = IcebergOptimizeJobOutcome {
        target_snapshot_id: Some(42),
        rewritten_data_files: 3,
        deleted_data_files: 2,
        added_data_files: 1,
        output_record_count: 99,
    };

    let job_id = {
        let mut txn = provider.begin_write("create iceberg optimize job")?;
        let job = repository.create_iceberg_optimize_job(
            txn.as_mut(),
            CreateIcebergOptimizeJobRequest {
                catalog: "ice".to_string(),
                namespace: "sales".to_string(),
                table: "orders".to_string(),
                base_snapshot_id: 7,
                now_ms: 1000,
            },
        )?;
        assert_eq!(job.state, IcebergOptimizeJobState::Pending);
        assert_eq!(job.created_at_ms, 1000);
        assert_eq!(job.started_at_ms, None);
        assert_eq!(job.finished_at_ms, None);
        let duplicate = repository
            .create_iceberg_optimize_job(
                txn.as_mut(),
                CreateIcebergOptimizeJobRequest {
                    catalog: "ice".to_string(),
                    namespace: "sales".to_string(),
                    table: "orders".to_string(),
                    base_snapshot_id: 8,
                    now_ms: 1001,
                },
            )
            .expect_err("active optimize job should block duplicate");
        assert_eq!(duplicate.kind(), RepositoryErrorKind::Conflict);
        txn.commit()?;
        job.id
    };

    {
        let read = provider.begin_read()?;
        let pending = repository.list_pending_iceberg_optimize_jobs(read.as_ref())?;
        assert_eq!(
            pending.iter().map(|job| job.id).collect::<Vec<_>>(),
            vec![job_id]
        );
    }

    {
        let mut txn = provider.begin_write("claim and record iceberg optimize")?;
        let claimed = repository.claim_iceberg_optimize_job(txn.as_mut(), job_id, 1100)?;
        assert_eq!(claimed.state, IcebergOptimizeJobState::Running);
        assert_eq!(claimed.started_at_ms, Some(1100));
        let recorded = repository.record_iceberg_optimize_job_outcome(
            txn.as_mut(),
            job_id,
            1200,
            outcome.clone(),
        )?;
        assert_eq!(recorded.state, IcebergOptimizeJobState::Running);
        assert_eq!(recorded.finished_at_ms, Some(1200));
        assert_eq!(recorded.outcome, Some(outcome.clone()));
        let finished =
            repository.finish_iceberg_optimize_job(txn.as_mut(), job_id, 1300, outcome.clone())?;
        assert_eq!(finished.state, IcebergOptimizeJobState::Finished);
        assert_eq!(finished.finished_at_ms, Some(1300));
        assert_eq!(finished.outcome, Some(outcome));
        txn.commit()?;
    }

    {
        let mut txn = provider.begin_write("create optimize job after finish")?;
        let next = repository.create_iceberg_optimize_job(
            txn.as_mut(),
            CreateIcebergOptimizeJobRequest {
                catalog: "ice".to_string(),
                namespace: "sales".to_string(),
                table: "orders".to_string(),
                base_snapshot_id: 9,
                now_ms: 1400,
            },
        )?;
        assert_ne!(next.id, job_id);
        txn.commit()?;
    }

    let read = provider.begin_read()?;
    let shown = repository.show_iceberg_optimize_jobs(read.as_ref())?;
    assert_eq!(shown.len(), 2);
    assert_eq!(shown[0].id, job_id);
    assert_eq!(shown[0].state, IcebergOptimizeJobState::Finished);
    assert_eq!(shown[1].state, IcebergOptimizeJobState::Pending);

    Ok(())
}

#[test]
fn job_repository_fails_running_iceberg_optimize_jobs_on_startup()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let provider = SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite"))?;
    let repository = JobMetaRepository::default();

    let (running_id, pending_id) = {
        let mut txn = provider.begin_write("seed iceberg optimize jobs")?;
        let running = repository.create_iceberg_optimize_job(
            txn.as_mut(),
            CreateIcebergOptimizeJobRequest {
                catalog: "ice".to_string(),
                namespace: "sales".to_string(),
                table: "orders".to_string(),
                base_snapshot_id: 7,
                now_ms: 1000,
            },
        )?;
        repository.claim_iceberg_optimize_job(txn.as_mut(), running.id, 1100)?;
        let pending = repository.create_iceberg_optimize_job(
            txn.as_mut(),
            CreateIcebergOptimizeJobRequest {
                catalog: "ice".to_string(),
                namespace: "sales".to_string(),
                table: "lineitem".to_string(),
                base_snapshot_id: 8,
                now_ms: 1001,
            },
        )?;
        txn.commit()?;
        (running.id, pending.id)
    };

    {
        let mut txn = provider.begin_write("fail running optimize jobs on startup")?;
        let changed =
            repository.fail_running_iceberg_optimize_jobs_on_startup(txn.as_mut(), 2000)?;
        assert_eq!(changed, 1);
        txn.commit()?;
    }

    let read = provider.begin_read()?;
    let shown = repository.show_iceberg_optimize_jobs(read.as_ref())?;
    let running = shown
        .iter()
        .find(|job| job.id == running_id)
        .expect("running job");
    assert_eq!(running.state, IcebergOptimizeJobState::Failed);
    assert_eq!(running.finished_at_ms, Some(2000));
    assert!(
        running
            .error_message
            .as_deref()
            .is_some_and(|message| message.contains("running during metadata store startup"))
    );
    let pending = shown
        .iter()
        .find(|job| job.id == pending_id)
        .expect("pending job");
    assert_eq!(pending.state, IcebergOptimizeJobState::Pending);

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

#[test]
fn job_repository_rejects_schema_version_mismatch() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let provider = SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite"))?;
    let repository = JobMetaRepository::default();
    let key = MetaKey::new(NS_JOB, ["erase", "1"])?;
    let payload = serde_json::json!({
        "job_id": 1,
        "table_id": 10,
        "partition_id": 20,
        "root_path": "s3://bucket/db/table/partition",
        "state": "PENDING",
        "retry_at_ms": null,
        "updated_at_ms": 1000,
        "last_error": null
    });

    {
        let mut txn = provider.begin_write("write mismatched erase job")?;
        txn.put(MetaRecordPut::new(
            key,
            MetaRecordKind::new("job.erase")?,
            ExpectedRevision::NotExists,
            encode_json_payload(999, &payload)?,
        ))?;
        txn.commit()?;
    }

    let read = provider.begin_read()?;
    let err = repository
        .load_erase_job(read.as_ref(), 1)
        .expect_err("schema version mismatch should fail");
    assert!(
        err.to_string()
            .contains("metadata record erase/1 has schema version 999")
    );

    Ok(())
}

#[test]
fn managed_lake_repository_creates_database_table_and_active_partition()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let provider = SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite"))?;
    let repository = ManagedLakeMetaRepository::default();

    {
        let mut txn = provider.begin_write("create managed lake objects")?;
        let database = repository.create_database(
            txn.as_mut(),
            CreateManagedDatabaseRequest {
                name: "db1".to_string(),
            },
        )?;
        let table = repository.create_table(
            txn.as_mut(),
            CreateManagedTableRequest {
                db_id: database.db_id,
                name: "orders".to_string(),
                keys_type: "DUP_KEYS".to_string(),
                bucket_num: 2,
                current_schema_id: 10,
                state: ManagedTableState::Creating,
                kind: ManagedTableKind::MaterializedView,
            },
        )?;
        repository.create_partition(txn.as_mut(), table.table_id, "orders", 1)?;
        txn.commit()?;
    }

    let read = provider.begin_read()?;
    let snapshot = repository.load_snapshot(read.as_ref())?;
    assert_eq!(snapshot.databases.len(), 1);
    assert_eq!(snapshot.tables.len(), 1);
    assert_eq!(snapshot.partitions.len(), 1);
    assert!(snapshot.schemas.is_empty());
    assert!(snapshot.columns.is_empty());
    assert!(snapshot.indexes.is_empty());
    assert!(snapshot.tablets.is_empty());

    assert_eq!(snapshot.databases[0].name, "db1");
    assert_eq!(snapshot.tables[0].db_id, snapshot.databases[0].db_id);
    assert_eq!(snapshot.tables[0].name, "orders");
    assert_eq!(snapshot.tables[0].keys_type, "DUP_KEYS");
    assert_eq!(snapshot.tables[0].bucket_num, 2);
    assert_eq!(snapshot.tables[0].current_schema_id, 10);
    assert_eq!(snapshot.tables[0].state, ManagedTableState::Creating);
    assert_eq!(snapshot.tables[0].kind, ManagedTableKind::MaterializedView);
    assert_eq!(snapshot.partitions[0].table_id, snapshot.tables[0].table_id);
    assert_eq!(snapshot.partitions[0].name, "orders");
    assert_eq!(snapshot.partitions[0].state, ManagedPartitionState::Active);
    assert_eq!(snapshot.partitions[0].visible_version, 1);
    assert_eq!(snapshot.partitions[0].next_version, 2);

    Ok(())
}

#[test]
fn managed_lake_repository_rejects_duplicate_table_name() -> Result<(), Box<dyn std::error::Error>>
{
    let dir = tempfile::tempdir()?;
    let provider = SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite"))?;
    let repository = ManagedLakeMetaRepository::default();

    let err = {
        let mut txn = provider.begin_write("create duplicate managed lake table")?;
        let database = repository.create_database(
            txn.as_mut(),
            CreateManagedDatabaseRequest {
                name: "db1".to_string(),
            },
        )?;
        repository.create_table(
            txn.as_mut(),
            CreateManagedTableRequest {
                db_id: database.db_id,
                name: "orders".to_string(),
                keys_type: "DUP_KEYS".to_string(),
                bucket_num: 2,
                current_schema_id: 10,
                state: ManagedTableState::Active,
                kind: ManagedTableKind::Table,
            },
        )?;
        repository
            .create_table(
                txn.as_mut(),
                CreateManagedTableRequest {
                    db_id: database.db_id,
                    name: "ORDERS".to_string(),
                    keys_type: "DUP_KEYS".to_string(),
                    bucket_num: 2,
                    current_schema_id: 10,
                    state: ManagedTableState::Active,
                    kind: ManagedTableKind::Table,
                },
            )
            .expect_err("case-insensitive duplicate table name should fail")
    };

    assert!(err.to_string().contains("already exists"));

    Ok(())
}

#[test]
fn managed_lake_repository_drops_table_and_purges_owned_rows()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let provider = SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite"))?;
    let managed_repo = ManagedLakeMetaRepository::default();
    let txn_repo = ManagedLakeTxnRepository::default();
    let job_repo = JobMetaRepository::default();

    let (table_id, _partition_id, bootstrap_txn_id) = {
        let mut txn = provider.begin_write("create managed table layout")?;
        let database = managed_repo.get_or_create_database(txn.as_mut(), "analytics")?;
        let created = managed_repo.create_table_layout(
            txn.as_mut(),
            CreateManagedTableLayoutRequest {
                db_id: database.db_id,
                table_name: "orders".to_string(),
                keys_type: "DUP_KEYS".to_string(),
                bucket_num: 2,
                kind: ManagedTableKind::Table,
                schema_version: 0,
                tablet_schema_pb: vec![1, 2, 3],
                columns: vec![CreateManagedColumnRequest {
                    column_name: "id".to_string(),
                    logical_type: "INT".to_string(),
                    nullable: false,
                    visible: true,
                    is_key: true,
                }],
                partition_name: "p0".to_string(),
                warehouse_uri: "s3://bucket/warehouse".to_string(),
            },
        )?;
        let bootstrap_txn = txn_repo.record_visible_bootstrap(
            txn.as_mut(),
            created.table.table_id,
            created.partition.partition_id,
        )?;
        txn.commit()?;
        (
            created.table.table_id,
            created.partition.partition_id,
            bootstrap_txn.txn_id,
        )
    };

    {
        let mut txn = provider.begin_write("drop managed table")?;
        txn_repo.ensure_no_inflight_for_table(txn.as_ref(), table_id)?;
        managed_repo.mark_table_dropping(txn.as_mut(), table_id)?;
        job_repo.create_erase_job(
            txn.as_mut(),
            CreateEraseJobRequest {
                table_id,
                partition_id: None,
                root_path: "s3://bucket/warehouse/db_1/table_1".to_string(),
                now_ms: 1000,
            },
        )?;
        txn.commit()?;
    }

    {
        let read = provider.begin_read()?;
        let snapshot = managed_repo.load_snapshot(read.as_ref())?;
        assert_eq!(snapshot.tables[0].state, ManagedTableState::Dropping);
        assert_eq!(snapshot.partitions[0].state, ManagedPartitionState::Retired);
        assert_eq!(snapshot.indexes.len(), 1);
        let jobs = job_repo.list_runnable_erase_jobs(read.as_ref(), 1000)?;
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].table_id, table_id);
        assert_eq!(jobs[0].partition_id, None);
    }

    {
        let mut txn = provider.begin_write("purge dropped managed table")?;
        txn_repo.delete_for_table(txn.as_mut(), table_id)?;
        managed_repo.purge_retired_table_metadata(txn.as_mut(), table_id)?;
        txn.commit()?;
    }

    let read = provider.begin_read()?;
    let snapshot = managed_repo.load_snapshot(read.as_ref())?;
    assert!(snapshot.tables.is_empty());
    assert!(snapshot.schemas.is_empty());
    assert!(snapshot.columns.is_empty());
    assert!(snapshot.partitions.is_empty());
    assert!(snapshot.indexes.is_empty());
    assert!(snapshot.tablets.is_empty());
    assert!(txn_repo.load(read.as_ref(), bootstrap_txn_id)?.is_none());

    Ok(())
}

#[test]
fn managed_lake_repository_stages_activates_and_purges_truncate_partition()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let provider = SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite"))?;
    let managed_repo = ManagedLakeMetaRepository::default();
    let txn_repo = ManagedLakeTxnRepository::default();
    let job_repo = JobMetaRepository::default();

    let (table_id, db_id, old_partition_id) = {
        let mut txn = provider.begin_write("create managed table layout")?;
        let database = managed_repo.get_or_create_database(txn.as_mut(), "analytics")?;
        let created = managed_repo.create_table_layout(
            txn.as_mut(),
            CreateManagedTableLayoutRequest {
                db_id: database.db_id,
                table_name: "orders".to_string(),
                keys_type: "DUP_KEYS".to_string(),
                bucket_num: 2,
                kind: ManagedTableKind::Table,
                schema_version: 0,
                tablet_schema_pb: vec![1, 2, 3],
                columns: vec![CreateManagedColumnRequest {
                    column_name: "id".to_string(),
                    logical_type: "INT".to_string(),
                    nullable: false,
                    visible: true,
                    is_key: true,
                }],
                partition_name: "p0".to_string(),
                warehouse_uri: "s3://bucket/warehouse".to_string(),
            },
        )?;
        txn.commit()?;
        (
            created.table.table_id,
            database.db_id,
            created.partition.partition_id,
        )
    };

    let staged = {
        let mut txn = provider.begin_write("stage truncate partition")?;
        txn_repo.ensure_no_inflight_for_table(txn.as_ref(), table_id)?;
        let staged = managed_repo.stage_truncate_partition(
            txn.as_mut(),
            StageManagedTruncateRequest {
                table_id,
                db_id,
                bucket_num: 2,
                partition_name: "p0".to_string(),
                warehouse_uri: "s3://bucket/warehouse".to_string(),
            },
        )?;
        txn.commit()?;
        staged
    };

    {
        let read = provider.begin_read()?;
        let snapshot = managed_repo.load_snapshot(read.as_ref())?;
        assert!(snapshot.partitions.iter().any(|partition| {
            partition.partition_id == staged.partition_id
                && partition.state == ManagedPartitionState::Creating
        }));
        assert_eq!(staged.tablet_ids.len(), 2);
        assert_eq!(
            staged.partition_root_path,
            format!(
                "s3://bucket/warehouse/db_{db_id}/table_{table_id}/partition_{}",
                staged.partition_id
            )
        );
    }

    {
        let mut txn = provider.begin_write("activate truncate partition")?;
        managed_repo.activate_truncate_partition(
            txn.as_mut(),
            table_id,
            old_partition_id,
            staged.partition_id,
            staged.index_id,
        )?;
        job_repo.create_erase_job(
            txn.as_mut(),
            CreateEraseJobRequest {
                table_id,
                partition_id: Some(old_partition_id),
                root_path: format!(
                    "s3://bucket/warehouse/db_{db_id}/table_{table_id}/partition_{old_partition_id}"
                ),
                now_ms: 1100,
            },
        )?;
        txn.commit()?;
    }

    {
        let read = provider.begin_read()?;
        let snapshot = managed_repo.load_snapshot(read.as_ref())?;
        let old_partition = snapshot
            .partitions
            .iter()
            .find(|partition| partition.partition_id == old_partition_id)
            .expect("old partition");
        let new_partition = snapshot
            .partitions
            .iter()
            .find(|partition| partition.partition_id == staged.partition_id)
            .expect("new partition");
        assert_eq!(old_partition.state, ManagedPartitionState::Retired);
        assert_eq!(new_partition.state, ManagedPartitionState::Active);
        assert_eq!(new_partition.visible_version, 1);
        let jobs = job_repo.list_runnable_erase_jobs(read.as_ref(), 1100)?;
        assert_eq!(jobs[0].partition_id, Some(old_partition_id));
    }

    {
        let mut txn = provider.begin_write("purge retired truncate partition")?;
        txn_repo.delete_for_partition(txn.as_mut(), old_partition_id)?;
        managed_repo.purge_retired_partition_metadata(txn.as_mut(), old_partition_id)?;
        txn.commit()?;
    }

    let read = provider.begin_read()?;
    let snapshot = managed_repo.load_snapshot(read.as_ref())?;
    assert!(
        snapshot
            .partitions
            .iter()
            .all(|partition| partition.partition_id != old_partition_id)
    );
    assert!(
        snapshot
            .tablets
            .iter()
            .all(|tablet| tablet.partition_id != old_partition_id)
    );
    assert!(
        snapshot
            .indexes
            .iter()
            .all(|index| index.partition_id != old_partition_id)
    );

    Ok(())
}

#[test]
fn managed_lake_repository_stages_and_activates_mv_refresh_partition()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let provider = SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite"))?;
    let managed_repo = ManagedLakeMetaRepository::default();
    let job_repo = JobMetaRepository::default();

    let (table_id, db_id, old_partition_id) = {
        let mut txn = provider.begin_write("create managed mv layout")?;
        let database = managed_repo.get_or_create_database(txn.as_mut(), "analytics")?;
        let created = managed_repo.create_table_layout(
            txn.as_mut(),
            CreateManagedTableLayoutRequest {
                db_id: database.db_id,
                table_name: "orders_mv".to_string(),
                keys_type: "DUP_KEYS".to_string(),
                bucket_num: 2,
                kind: ManagedTableKind::MaterializedView,
                schema_version: 0,
                tablet_schema_pb: vec![1, 2, 3],
                columns: vec![CreateManagedColumnRequest {
                    column_name: "id".to_string(),
                    logical_type: "INT".to_string(),
                    nullable: false,
                    visible: true,
                    is_key: true,
                }],
                partition_name: "orders_mv".to_string(),
                warehouse_uri: "s3://bucket/warehouse".to_string(),
            },
        )?;
        txn.commit()?;
        (
            created.table.table_id,
            database.db_id,
            created.partition.partition_id,
        )
    };

    let staged = {
        let mut txn = provider.begin_write("stage managed mv refresh partition")?;
        let staged = managed_repo.stage_mv_refresh_partition(
            txn.as_mut(),
            StageManagedMvRefreshRequest {
                table_id,
                db_id,
                bucket_num: 2,
                partition_name: "orders_mv".to_string(),
                warehouse_uri: "s3://bucket/warehouse".to_string(),
            },
        )?;
        txn.commit()?;
        staged
    };

    {
        let mut txn = provider.begin_write("reject overlapping managed mv refresh")?;
        let err = managed_repo
            .stage_mv_refresh_partition(
                txn.as_mut(),
                StageManagedMvRefreshRequest {
                    table_id,
                    db_id,
                    bucket_num: 2,
                    partition_name: "orders_mv".to_string(),
                    warehouse_uri: "s3://bucket/warehouse".to_string(),
                },
            )
            .expect_err("creating partition should block overlapping refresh");
        assert_eq!(err.kind(), RepositoryErrorKind::Conflict);
    }

    {
        let read = provider.begin_read()?;
        let snapshot = managed_repo.load_snapshot(read.as_ref())?;
        let staged_partition = snapshot
            .partitions
            .iter()
            .find(|partition| partition.partition_id == staged.partition_id)
            .expect("staged partition");
        assert_eq!(staged_partition.state, ManagedPartitionState::Creating);
        assert_eq!(staged.tablet_ids.len(), 2);
        assert_eq!(
            staged.partition_root_path,
            format!(
                "s3://bucket/warehouse/db_{db_id}/table_{table_id}/partition_{}",
                staged.partition_id
            )
        );
    }

    {
        let mut txn = provider.begin_write("activate managed mv refresh partition")?;
        managed_repo.activate_mv_refresh_partition(
            txn.as_mut(),
            table_id,
            old_partition_id,
            staged.partition_id,
            staged.index_id,
        )?;
        job_repo.create_erase_job(
            txn.as_mut(),
            CreateEraseJobRequest {
                table_id,
                partition_id: Some(old_partition_id),
                root_path: format!(
                    "s3://bucket/warehouse/db_{db_id}/table_{table_id}/partition_{old_partition_id}"
                ),
                now_ms: 1100,
            },
        )?;
        txn.commit()?;
    }

    let read = provider.begin_read()?;
    let snapshot = managed_repo.load_snapshot(read.as_ref())?;
    let old_partition = snapshot
        .partitions
        .iter()
        .find(|partition| partition.partition_id == old_partition_id)
        .expect("old partition");
    let new_partition = snapshot
        .partitions
        .iter()
        .find(|partition| partition.partition_id == staged.partition_id)
        .expect("new partition");
    assert_eq!(old_partition.state, ManagedPartitionState::Retired);
    assert_eq!(new_partition.state, ManagedPartitionState::Active);
    assert_eq!(new_partition.visible_version, 2);
    assert_eq!(new_partition.next_version, 3);
    let new_index = snapshot
        .indexes
        .iter()
        .find(|index| index.index_id == staged.index_id)
        .expect("new index");
    assert_eq!(new_index.state, ManagedIndexState::Active);
    let jobs = job_repo.list_runnable_erase_jobs(read.as_ref(), 1100)?;
    assert_eq!(jobs[0].partition_id, Some(old_partition_id));

    Ok(())
}

#[test]
fn managed_txn_repository_prepare_written_visible_advances_partition()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let provider = SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite"))?;
    let meta_repo = ManagedLakeMetaRepository::default();
    let txn_repo = ManagedLakeTxnRepository::default();

    let (table_id, partition_id) = {
        let mut txn = provider.begin_write("create managed lake objects")?;
        let database = meta_repo.create_database(
            txn.as_mut(),
            CreateManagedDatabaseRequest {
                name: "db1".to_string(),
            },
        )?;
        let table = meta_repo.create_table(
            txn.as_mut(),
            CreateManagedTableRequest {
                db_id: database.db_id,
                name: "orders".to_string(),
                keys_type: "DUP_KEYS".to_string(),
                bucket_num: 2,
                current_schema_id: 10,
                state: ManagedTableState::Active,
                kind: ManagedTableKind::Table,
            },
        )?;
        let partition = meta_repo.create_partition(txn.as_mut(), table.table_id, "orders", 1)?;
        txn.commit()?;
        (table.table_id, partition.partition_id)
    };

    let txn_id = {
        let mut txn = provider.begin_write("commit managed lake txn")?;
        let managed_txn = txn_repo.prepare(&meta_repo, txn.as_mut(), table_id, partition_id)?;
        assert_eq!(managed_txn.table_id, table_id);
        assert_eq!(managed_txn.partition_id, partition_id);
        assert_eq!(managed_txn.base_version, 1);
        assert_eq!(managed_txn.commit_version, 2);
        assert_eq!(managed_txn.state, ManagedTxnState::Prepared);
        txn_repo.mark_written(txn.as_mut(), managed_txn.txn_id)?;
        txn_repo.mark_visible(&meta_repo, txn.as_mut(), managed_txn.txn_id)?;
        txn.commit()?;
        managed_txn.txn_id
    };

    let read = provider.begin_read()?;
    let loaded = txn_repo
        .load(read.as_ref(), txn_id)?
        .expect("managed txn should persist");
    assert_eq!(loaded.state, ManagedTxnState::Visible);

    let partition = meta_repo
        .load_partition(read.as_ref(), partition_id)?
        .expect("partition should persist");
    assert_eq!(partition.visible_version, 2);
    assert_eq!(partition.next_version, 3);

    Ok(())
}

#[test]
fn managed_txn_repository_abort_does_not_advance_partition()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let provider = SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite"))?;
    let meta_repo = ManagedLakeMetaRepository::default();
    let txn_repo = ManagedLakeTxnRepository::default();

    let (table_id, partition_id) = {
        let mut txn = provider.begin_write("create managed lake objects")?;
        let database = meta_repo.create_database(
            txn.as_mut(),
            CreateManagedDatabaseRequest {
                name: "db1".to_string(),
            },
        )?;
        let table = meta_repo.create_table(
            txn.as_mut(),
            CreateManagedTableRequest {
                db_id: database.db_id,
                name: "orders".to_string(),
                keys_type: "DUP_KEYS".to_string(),
                bucket_num: 2,
                current_schema_id: 10,
                state: ManagedTableState::Active,
                kind: ManagedTableKind::Table,
            },
        )?;
        let partition = meta_repo.create_partition(txn.as_mut(), table.table_id, "orders", 1)?;
        txn.commit()?;
        (table.table_id, partition.partition_id)
    };

    let txn_id = {
        let mut txn = provider.begin_write("abort managed lake txn")?;
        let managed_txn = txn_repo.prepare(&meta_repo, txn.as_mut(), table_id, partition_id)?;
        txn_repo.mark_aborted(txn.as_mut(), managed_txn.txn_id)?;
        txn.commit()?;
        managed_txn.txn_id
    };

    let read = provider.begin_read()?;
    let loaded = txn_repo
        .load(read.as_ref(), txn_id)?
        .expect("managed txn should persist");
    assert_eq!(loaded.state, ManagedTxnState::Aborted);

    let partition = meta_repo
        .load_partition(read.as_ref(), partition_id)?
        .expect("partition should persist");
    assert_eq!(partition.visible_version, 1);

    Ok(())
}

#[test]
fn managed_txn_repository_mark_written_is_retry_safe() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let provider = SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite"))?;
    let meta_repo = ManagedLakeMetaRepository::default();
    let txn_repo = ManagedLakeTxnRepository::default();
    let (table_id, partition_id) = create_managed_table_with_partition(&provider, &meta_repo)?;

    let txn_id = {
        let mut txn = provider.begin_write("retry mark written")?;
        let managed_txn = txn_repo.prepare(&meta_repo, txn.as_mut(), table_id, partition_id)?;
        txn_repo.mark_written(txn.as_mut(), managed_txn.txn_id)?;
        txn_repo.mark_written(txn.as_mut(), managed_txn.txn_id)?;
        txn.commit()?;
        managed_txn.txn_id
    };

    let read = provider.begin_read()?;
    let loaded = txn_repo
        .load(read.as_ref(), txn_id)?
        .expect("managed txn should persist");
    assert_eq!(loaded.state, ManagedTxnState::Written);

    Ok(())
}

#[test]
fn managed_txn_repository_mark_visible_is_retry_safe() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let provider = SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite"))?;
    let meta_repo = ManagedLakeMetaRepository::default();
    let txn_repo = ManagedLakeTxnRepository::default();
    let (table_id, partition_id) = create_managed_table_with_partition(&provider, &meta_repo)?;

    let txn_id = {
        let mut txn = provider.begin_write("retry mark visible")?;
        let managed_txn = txn_repo.prepare(&meta_repo, txn.as_mut(), table_id, partition_id)?;
        txn_repo.mark_written(txn.as_mut(), managed_txn.txn_id)?;
        txn_repo.mark_visible(&meta_repo, txn.as_mut(), managed_txn.txn_id)?;
        txn_repo.mark_visible(&meta_repo, txn.as_mut(), managed_txn.txn_id)?;
        txn_repo.mark_written(txn.as_mut(), managed_txn.txn_id)?;
        txn.commit()?;
        managed_txn.txn_id
    };

    let read = provider.begin_read()?;
    let loaded = txn_repo
        .load(read.as_ref(), txn_id)?
        .expect("managed txn should persist");
    assert_eq!(loaded.state, ManagedTxnState::Visible);
    let partition = meta_repo
        .load_partition(read.as_ref(), partition_id)?
        .expect("partition should persist");
    assert_eq!(partition.visible_version, 2);
    assert_eq!(partition.next_version, 3);

    Ok(())
}

#[test]
fn managed_txn_repository_rejects_illegal_commit_version() -> Result<(), Box<dyn std::error::Error>>
{
    let dir = tempfile::tempdir()?;
    let provider = SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite"))?;
    let meta_repo = ManagedLakeMetaRepository::default();
    let txn_repo = ManagedLakeTxnRepository::default();
    let (table_id, partition_id) = create_managed_table_with_partition(&provider, &meta_repo)?;

    let txn_id = {
        let mut txn = provider.begin_write("create invalid managed txn")?;
        let txn_id = txn.allocate_id(id_scopes::managed_txn())?;
        put_managed_txn_record(
            txn.as_mut(),
            StoredManagedTxn {
                txn_id,
                table_id,
                partition_id,
                base_version: 1,
                commit_version: 3,
                state: ManagedTxnState::Written,
                retry_at_ms: None,
                updated_at_ms: 0,
            },
        )?;
        txn.commit()?;
        txn_id
    };

    let mut txn = provider.begin_write("mark invalid managed txn visible")?;
    let err = txn_repo
        .mark_visible(&meta_repo, txn.as_mut(), txn_id)
        .expect_err("illegal commit version should fail");
    assert_eq!(err.kind(), RepositoryErrorKind::Provider);

    Ok(())
}

#[test]
fn managed_txn_repository_rejects_partition_table_mismatch()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let provider = SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite"))?;
    let meta_repo = ManagedLakeMetaRepository::default();
    let txn_repo = ManagedLakeTxnRepository::default();

    let (table_id, other_partition_id) = {
        let (table_id, _) = create_managed_table_with_partition(&provider, &meta_repo)?;
        let (other_table_id, other_partition_id) =
            create_named_managed_table_with_partition(&provider, &meta_repo, "lineitem")?;
        assert_ne!(table_id, other_table_id);
        (table_id, other_partition_id)
    };

    let txn_id = {
        let mut txn = provider.begin_write("create mismatched managed txn")?;
        let txn_id = txn.allocate_id(id_scopes::managed_txn())?;
        put_managed_txn_record(
            txn.as_mut(),
            StoredManagedTxn {
                txn_id,
                table_id,
                partition_id: other_partition_id,
                base_version: 1,
                commit_version: 2,
                state: ManagedTxnState::Written,
                retry_at_ms: None,
                updated_at_ms: 0,
            },
        )?;
        txn.commit()?;
        txn_id
    };

    let mut txn = provider.begin_write("mark mismatched managed txn visible")?;
    let err = txn_repo
        .mark_visible(&meta_repo, txn.as_mut(), txn_id)
        .expect_err("partition table mismatch should fail");
    assert_eq!(err.kind(), RepositoryErrorKind::Conflict);

    Ok(())
}

#[test]
fn managed_txn_repository_rejects_partition_next_version_mismatch()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let provider = SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite"))?;
    let meta_repo = ManagedLakeMetaRepository::default();
    let txn_repo = ManagedLakeTxnRepository::default();
    let (table_id, partition_id) = create_managed_table_with_partition(&provider, &meta_repo)?;

    let txn_id = {
        let mut txn = provider.begin_write("prepare managed txn with stale partition next")?;
        let managed_txn = txn_repo.prepare(&meta_repo, txn.as_mut(), table_id, partition_id)?;
        txn_repo.mark_written(txn.as_mut(), managed_txn.txn_id)?;
        let (revision, mut partition) = meta_repo
            .load_versioned_partition(txn.as_ref(), partition_id)?
            .expect("partition should persist");
        partition.next_version = 99;
        meta_repo.update_partition_exact(txn.as_mut(), &partition, revision)?;
        txn.commit()?;
        managed_txn.txn_id
    };

    let mut txn = provider.begin_write("mark managed txn visible with stale partition next")?;
    let err = txn_repo
        .mark_visible(&meta_repo, txn.as_mut(), txn_id)
        .expect_err("partition next_version mismatch should fail");
    assert_eq!(err.kind(), RepositoryErrorKind::Conflict);

    Ok(())
}

#[test]
fn key_helpers_reject_unescaped_path_separators() {
    let err = MetaKey::new("managed", ["table", "bad/name"]).expect_err("slash must fail");
    assert!(
        err.to_string()
            .contains("invalid metadata key path segment")
    );
}

#[test]
fn mv_repository_creates_definition_and_target_lookup() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let provider = SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite"))?;
    let repository = MvMetaRepository::default();

    let created = {
        let mut txn = provider.begin_write("create mv definition")?;
        let definition = repository.create_definition(
            txn.as_mut(),
            CreateMvDefinitionRequest {
                select_sql: "SELECT id, amount FROM iceberg.sales.orders".to_string(),
                base_table_refs: vec!["iceberg.sales.orders".to_string()],
                primary_key_columns: vec!["id".to_string()],
                storage_engine: "iceberg".to_string(),
                target_catalog: Some("ice".to_string()),
                target_namespace: Some("ns".to_string()),
                target_table: Some("orders_mv".to_string()),
                target_apply_key: None,
                created_at_ms: 11,
            },
        )?;
        txn.commit()?;
        definition
    };

    let read = provider.begin_read()?;
    let loaded = repository
        .load_by_id(read.as_ref(), created.mv_id)?
        .expect("definition should exist");
    assert_eq!(loaded.mv_id, created.mv_id);
    assert_eq!(loaded.select_sql, created.select_sql);

    let target = repository
        .find_by_target(read.as_ref(), "ICE", "Ns", "ORDERS_MV")?
        .expect("target lookup should be case-insensitive");
    assert_eq!(target.mv_id, created.mv_id);

    Ok(())
}

#[test]
fn mv_repository_creates_and_drops_definition_with_explicit_id()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let provider = SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite"))?;
    let repository = MvMetaRepository::default();

    {
        let mut txn = provider.begin_write("create explicit mv definition")?;
        let definition = repository.create_definition_with_id(
            txn.as_mut(),
            42,
            CreateMvDefinitionRequest {
                select_sql: "SELECT id FROM ice.sales.orders".to_string(),
                base_table_refs: vec!["ice.sales.orders".to_string()],
                primary_key_columns: vec!["id".to_string()],
                storage_engine: "managed_lake".to_string(),
                target_catalog: None,
                target_namespace: None,
                target_table: None,
                target_apply_key: None,
                created_at_ms: 7,
            },
        )?;
        assert_eq!(definition.mv_id, 42);
        txn.commit()?;
    }

    {
        let read = provider.begin_read()?;
        let definition = repository
            .load_by_id(read.as_ref(), 42)?
            .expect("explicit definition");
        assert_eq!(definition.select_sql, "SELECT id FROM ice.sales.orders");
        assert_eq!(definition.base_table_refs, vec!["ice.sales.orders"]);
    }

    {
        let mut txn = provider.begin_write("drop explicit mv definition")?;
        assert!(repository.drop_by_id(txn.as_mut(), 42)?);
        txn.commit()?;
    }

    let read = provider.begin_read()?;
    assert!(repository.load_by_id(read.as_ref(), 42)?.is_none());

    Ok(())
}

#[test]
fn mv_repository_reserves_explicit_ids_for_future_allocation()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let provider = SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite"))?;
    let repository = MvMetaRepository::default();

    {
        let mut txn = provider.begin_write("reserve explicit mv definition id")?;
        repository.reserve_definition_id(txn.as_mut(), 42)?;
        let definition = repository.create_definition_with_id(
            txn.as_mut(),
            42,
            sample_mv_definition_request("SELECT id FROM ice.sales.orders"),
        )?;
        assert_eq!(definition.mv_id, 42);
        txn.commit()?;
    }

    {
        let mut txn = provider.begin_write("create auto mv definition after reservation")?;
        let definition = repository.create_definition(
            txn.as_mut(),
            sample_mv_definition_request("SELECT id FROM ice.sales.lineitem"),
        )?;
        assert_eq!(definition.mv_id, 43);
        txn.commit()?;
    }

    let read = provider.begin_read()?;
    assert!(repository.load_by_id(read.as_ref(), 42)?.is_some());
    assert!(repository.load_by_id(read.as_ref(), 43)?.is_some());

    Ok(())
}

#[test]
fn mv_repository_shares_id_allocator_with_managed_tables() -> Result<(), Box<dyn std::error::Error>>
{
    let dir = tempfile::tempdir()?;
    let provider = SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite"))?;
    let mv_repository = MvMetaRepository::default();
    let managed_repository = ManagedLakeMetaRepository::default();

    {
        let mut txn = provider.begin_write("create iceberg mv then managed mv")?;
        let iceberg_mv = mv_repository.create_definition(
            txn.as_mut(),
            CreateMvDefinitionRequest {
                select_sql: "SELECT id FROM ice.sales.orders".to_string(),
                base_table_refs: vec!["ice.sales.orders".to_string()],
                primary_key_columns: Vec::new(),
                storage_engine: "iceberg".to_string(),
                target_catalog: Some("ice".to_string()),
                target_namespace: Some("analytics".to_string()),
                target_table: Some("orders_mv".to_string()),
                target_apply_key: None,
                created_at_ms: 7,
            },
        )?;
        assert_eq!(iceberg_mv.mv_id, 1);

        let database = managed_repository.create_database(
            txn.as_mut(),
            CreateManagedDatabaseRequest {
                name: "analytics".to_string(),
            },
        )?;
        let managed_mv_table = managed_repository.create_table(
            txn.as_mut(),
            CreateManagedTableRequest {
                db_id: database.db_id,
                name: "managed_orders_mv".to_string(),
                keys_type: "DUP_KEYS".to_string(),
                bucket_num: 2,
                current_schema_id: 10,
                state: ManagedTableState::Active,
                kind: ManagedTableKind::MaterializedView,
            },
        )?;
        assert_eq!(managed_mv_table.table_id, 2);

        let managed_mv = mv_repository.create_definition_with_id(
            txn.as_mut(),
            managed_mv_table.table_id,
            sample_mv_definition_request("SELECT id FROM ice.sales.lineitem"),
        )?;
        assert_eq!(managed_mv.mv_id, managed_mv_table.table_id);
        txn.commit()?;
    }

    let read = provider.begin_read()?;
    assert!(mv_repository.load_by_id(read.as_ref(), 1)?.is_some());
    assert!(mv_repository.load_by_id(read.as_ref(), 2)?.is_some());

    Ok(())
}

#[test]
fn mv_repository_refresh_intent_finalizes_once() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let provider = SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite"))?;
    let repository = MvMetaRepository::default();

    let mv_id = {
        let mut txn = provider.begin_write("create mv definition")?;
        let definition = repository.create_definition(
            txn.as_mut(),
            CreateMvDefinitionRequest {
                select_sql: "SELECT id, amount FROM iceberg.sales.orders".to_string(),
                base_table_refs: vec!["iceberg.sales.orders".to_string()],
                primary_key_columns: vec!["id".to_string()],
                storage_engine: "iceberg".to_string(),
                target_catalog: Some("ice".to_string()),
                target_namespace: Some("ns".to_string()),
                target_table: Some("orders_mv".to_string()),
                target_apply_key: None,
                created_at_ms: 11,
            },
        )?;
        txn.commit()?;
        definition.mv_id
    };

    let refresh_id = {
        let mut txn = provider.begin_write("begin mv refresh")?;
        let mut target_snapshots = BTreeMap::new();
        target_snapshots.insert("ice.ns.orders_mv".to_string(), 100);
        let refresh = repository.begin_refresh_intent(txn.as_mut(), mv_id, target_snapshots)?;
        assert!(refresh.refresh_id > 0);
        assert_eq!(refresh.mv_id, mv_id);
        assert_eq!(refresh.state, MvRefreshState::IntentCreated);
        assert_eq!(refresh.target_snapshots["ice.ns.orders_mv"], 100);
        txn.commit()?;
        refresh.refresh_id
    };

    {
        let read = provider.begin_read()?;
        let refresh = repository
            .load_refresh(read.as_ref(), refresh_id)?
            .expect("refresh intent should persist");
        assert_eq!(refresh.state, MvRefreshState::IntentCreated);
        assert_eq!(refresh.target_snapshots["ice.ns.orders_mv"], 100);
    }

    {
        let mut txn = provider.begin_write("record external mv commit")?;
        repository.record_external_commit_outcome(
            txn.as_mut(),
            refresh_id,
            RefreshExternalOutcome {
                target_snapshot_id: Some(200),
                commit_id: "commit-1".to_string(),
            },
        )?;
        txn.commit()?;
    }

    {
        let read = provider.begin_read()?;
        let refresh = repository
            .load_refresh(read.as_ref(), refresh_id)?
            .expect("refresh should exist after external commit");
        assert_eq!(refresh.state.as_str(), "PUBLISH_COMMITTED");
        let outcome = refresh
            .external_outcome
            .expect("external outcome should persist");
        assert_eq!(outcome.target_snapshot_id, Some(200));
        assert_eq!(outcome.commit_id, "commit-1");
    }

    {
        let mut txn = provider.begin_write("finalize mv refresh")?;
        let mut base_snapshots = BTreeMap::new();
        base_snapshots.insert("iceberg.sales.orders".to_string(), 50);
        let mut base_table_uuids = BTreeMap::new();
        base_table_uuids.insert(
            "iceberg.sales.orders".to_string(),
            "uuid-orders".to_string(),
        );
        repository.finalize_refresh(
            txn.as_mut(),
            MvRefreshFinalizeRequest {
                refresh_id,
                rows: 3,
                base_snapshots,
                base_table_uuids,
                target_snapshot_id: Some(200),
            },
        )?;
        txn.commit()?;
    }

    {
        let mut txn = provider.begin_write("finalize mv refresh again")?;
        repository.finalize_refresh(
            txn.as_mut(),
            MvRefreshFinalizeRequest {
                refresh_id,
                rows: 3,
                base_snapshots: BTreeMap::new(),
                base_table_uuids: BTreeMap::new(),
                target_snapshot_id: Some(200),
            },
        )?;
        txn.commit()?;
    }

    let read = provider.begin_read()?;
    let refresh = repository
        .load_refresh(read.as_ref(), refresh_id)?
        .expect("refresh should exist");
    assert_eq!(refresh.state, MvRefreshState::Finalized);

    let definition = repository
        .load_by_id(read.as_ref(), mv_id)?
        .expect("definition should exist");
    assert_eq!(definition.last_refresh_rows, Some(3));
    assert_eq!(definition.last_refreshed_iceberg_snapshot_id, Some(200));
    assert!(!definition.refresh_in_progress);
    assert_eq!(definition.active_refresh_id, None);
    assert!(definition.refresh_target_snapshots.is_empty());

    Ok(())
}

#[test]
fn mv_repository_branch_staged_refresh_lifecycle() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let provider = SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite"))?;
    let repository = MvMetaRepository::default();

    let mv_id = {
        let mut txn = provider.begin_write("create mv definition")?;
        let definition = repository.create_definition(
            txn.as_mut(),
            CreateMvDefinitionRequest {
                select_sql: "SELECT id, amount FROM iceberg.sales.orders".to_string(),
                base_table_refs: vec!["iceberg.sales.orders".to_string()],
                primary_key_columns: vec!["id".to_string()],
                storage_engine: "iceberg".to_string(),
                target_catalog: Some("ice".to_string()),
                target_namespace: Some("analytics".to_string()),
                target_table: Some("orders_mv".to_string()),
                target_apply_key: None,
                created_at_ms: 11,
            },
        )?;
        txn.commit()?;
        definition.mv_id
    };

    let marker_token = "marker-token-1001".to_string();

    let refresh_id = {
        let mut txn = provider.begin_write("begin branch staged refresh")?;
        let mut base_snapshots = BTreeMap::new();
        base_snapshots.insert("iceberg.sales.orders".to_string(), 50);
        let refresh = repository.begin_iceberg_refresh_intent(
            txn.as_mut(),
            BeginIcebergMvRefreshRequest {
                mv_id,
                target_catalog: "ice".to_string(),
                target_namespace: "analytics".to_string(),
                target_table: "orders_mv".to_string(),
                staging_branch: "__nova_mv_refresh_1_1001".to_string(),
                expected_main_snapshot_id: Some(200),
                base_snapshots,
                marker_token: marker_token.clone(),
            },
        )?;
        assert_eq!(refresh.state, MvRefreshState::IntentCreated);
        assert_eq!(
            refresh.staging_branch.as_deref(),
            Some("__nova_mv_refresh_1_1001")
        );
        assert_eq!(refresh.expected_main_snapshot_id, Some(200));
        txn.commit()?;
        refresh.refresh_id
    };

    {
        let mut txn = provider.begin_write("record staging commit")?;
        repository.record_staging_commit(
            txn.as_mut(),
            RecordStagingCommitRequest {
                refresh_id,
                staging_snapshot_id: 300,
                rows: 3,
                base_table_uuids: BTreeMap::from([(
                    "iceberg.sales.orders".to_string(),
                    "uuid-orders".to_string(),
                )]),
            },
        )?;
        txn.commit()?;
    }

    {
        let mut txn = provider.begin_write("record publish commit")?;
        repository.record_publish_commit(
            txn.as_mut(),
            RecordPublishCommitRequest {
                refresh_id,
                published_snapshot_id: 300,
            },
        )?;
        txn.commit()?;
    }

    {
        let mut txn = provider.begin_write("finalize branch staged refresh")?;
        repository.finalize_refresh(
            txn.as_mut(),
            MvRefreshFinalizeRequest {
                refresh_id,
                rows: 3,
                base_snapshots: BTreeMap::from([("iceberg.sales.orders".to_string(), 50)]),
                base_table_uuids: BTreeMap::from([(
                    "iceberg.sales.orders".to_string(),
                    "uuid-orders".to_string(),
                )]),
                target_snapshot_id: Some(300),
            },
        )?;
        txn.commit()?;
    }

    let read = provider.begin_read()?;
    let refresh = repository
        .load_refresh(read.as_ref(), refresh_id)?
        .expect("refresh should persist");
    assert_eq!(refresh.state, MvRefreshState::Finalized);
    assert_eq!(refresh.staging_snapshot_id, Some(300));
    assert_eq!(refresh.published_snapshot_id, Some(300));
    assert_eq!(refresh.rows, Some(3));
    assert_eq!(
        refresh.marker,
        Some(RefreshCommitMarker {
            refresh_id,
            mv_id,
            token: marker_token,
        })
    );

    let definition = repository
        .load_by_id(read.as_ref(), mv_id)?
        .expect("definition should persist");
    assert_eq!(definition.last_refreshed_iceberg_snapshot_id, Some(300));
    assert_eq!(definition.last_refresh_rows, Some(3));
    assert_eq!(definition.active_refresh_id, None);
    assert!(!definition.refresh_in_progress);
    Ok(())
}

#[test]
fn mv_repository_lists_unfinished_refreshes() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let provider = SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite"))?;
    let repository = MvMetaRepository::default();
    let mv_id = create_test_iceberg_mv(&provider, &repository)?;

    let mut txn = provider.begin_write("begin refresh")?;
    let refresh = repository.begin_iceberg_refresh_intent(
        txn.as_mut(),
        BeginIcebergMvRefreshRequest {
            mv_id,
            target_catalog: "ice".to_string(),
            target_namespace: "analytics".to_string(),
            target_table: "orders_mv".to_string(),
            staging_branch: "__nova_mv_refresh_1_1".to_string(),
            expected_main_snapshot_id: Some(10),
            base_snapshots: BTreeMap::new(),
            marker_token: "token".to_string(),
        },
    )?;
    txn.commit()?;

    let read = provider.begin_read()?;
    let unfinished = repository.list_unfinished_refreshes(read.as_ref())?;
    assert_eq!(unfinished.len(), 1);
    assert_eq!(unfinished[0].refresh_id, refresh.refresh_id);
    Ok(())
}

#[test]
fn mv_repository_branch_staged_recovery_scan_filters_plain_refresh()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let provider = SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite"))?;
    let repository = MvMetaRepository::default();
    let mv_id = create_test_iceberg_mv(&provider, &repository)?;

    let mut txn = provider.begin_write("begin plain refresh")?;
    repository.begin_refresh_intent(txn.as_mut(), mv_id, BTreeMap::new())?;
    txn.commit()?;

    let read = provider.begin_read()?;
    let unfinished = repository.list_unfinished_branch_staged_iceberg_refreshes(read.as_ref())?;
    assert!(unfinished.is_empty());
    Ok(())
}

#[test]
fn mv_repository_branch_staged_recovery_scan_returns_branch_staged()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let provider = SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite"))?;
    let repository = MvMetaRepository::default();
    let mv_id = create_test_iceberg_mv(&provider, &repository)?;
    let refresh_id = begin_test_branch_staged_refresh(&provider, &repository, mv_id)?;

    let read = provider.begin_read()?;
    let unfinished = repository.list_unfinished_branch_staged_iceberg_refreshes(read.as_ref())?;
    assert_eq!(unfinished.len(), 1);
    assert_eq!(unfinished[0].refresh_id, refresh_id);
    Ok(())
}

#[test]
fn mv_repository_branch_staged_recovery_scan_excludes_terminal_states()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let provider = SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite"))?;
    let repository = MvMetaRepository::default();
    let aborted_mv_id = create_named_test_iceberg_mv(&provider, &repository, "aborted_mv")?;
    let finalized_mv_id = create_named_test_iceberg_mv(&provider, &repository, "finalized_mv")?;
    let aborted_refresh_id = begin_named_test_branch_staged_refresh(
        &provider,
        &repository,
        aborted_mv_id,
        "aborted_mv",
    )?;
    let finalized_refresh_id = begin_named_test_branch_staged_refresh(
        &provider,
        &repository,
        finalized_mv_id,
        "finalized_mv",
    )?;

    {
        let mut txn = provider.begin_write("abort branch staged refresh")?;
        repository.clear_refresh_progress(txn.as_mut(), aborted_mv_id)?;
        txn.commit()?;
    }

    {
        let mut txn = provider.begin_write("finalize branch staged refresh")?;
        repository.record_staging_commit(
            txn.as_mut(),
            RecordStagingCommitRequest {
                refresh_id: finalized_refresh_id,
                staging_snapshot_id: 300,
                rows: 3,
                base_table_uuids: BTreeMap::new(),
            },
        )?;
        repository.record_publish_commit(
            txn.as_mut(),
            RecordPublishCommitRequest {
                refresh_id: finalized_refresh_id,
                published_snapshot_id: 300,
            },
        )?;
        repository.finalize_refresh(
            txn.as_mut(),
            MvRefreshFinalizeRequest {
                refresh_id: finalized_refresh_id,
                rows: 3,
                base_snapshots: BTreeMap::new(),
                base_table_uuids: BTreeMap::new(),
                target_snapshot_id: Some(300),
            },
        )?;
        txn.commit()?;
    }

    let read = provider.begin_read()?;
    let unfinished = repository.list_unfinished_branch_staged_iceberg_refreshes(read.as_ref())?;
    assert!(unfinished.is_empty());
    let aborted = repository
        .load_refresh(read.as_ref(), aborted_refresh_id)?
        .expect("aborted refresh");
    assert_eq!(aborted.state, MvRefreshState::Aborted);
    let finalized = repository
        .load_refresh(read.as_ref(), finalized_refresh_id)?
        .expect("finalized refresh");
    assert_eq!(finalized.state, MvRefreshState::Finalized);
    Ok(())
}

fn create_test_iceberg_mv(
    provider: &SqliteMetaStoreProvider,
    repository: &MvMetaRepository,
) -> Result<i64, Box<dyn std::error::Error>> {
    create_named_test_iceberg_mv(provider, repository, "orders_mv")
}

fn create_named_test_iceberg_mv(
    provider: &SqliteMetaStoreProvider,
    repository: &MvMetaRepository,
    target_table: &str,
) -> Result<i64, Box<dyn std::error::Error>> {
    let mut txn = provider.begin_write("create test iceberg mv")?;
    let definition = repository.create_definition(
        txn.as_mut(),
        CreateMvDefinitionRequest {
            select_sql: "SELECT id, amount FROM iceberg.sales.orders".to_string(),
            base_table_refs: vec!["iceberg.sales.orders".to_string()],
            primary_key_columns: vec!["id".to_string()],
            storage_engine: "iceberg".to_string(),
            target_catalog: Some("ice".to_string()),
            target_namespace: Some("analytics".to_string()),
            target_table: Some(target_table.to_string()),
            target_apply_key: None,
            created_at_ms: 11,
        },
    )?;
    txn.commit()?;
    Ok(definition.mv_id)
}

fn begin_test_branch_staged_refresh(
    provider: &SqliteMetaStoreProvider,
    repository: &MvMetaRepository,
    mv_id: i64,
) -> Result<i64, Box<dyn std::error::Error>> {
    begin_named_test_branch_staged_refresh(provider, repository, mv_id, "orders_mv")
}

fn begin_named_test_branch_staged_refresh(
    provider: &SqliteMetaStoreProvider,
    repository: &MvMetaRepository,
    mv_id: i64,
    target_table: &str,
) -> Result<i64, Box<dyn std::error::Error>> {
    let mut txn = provider.begin_write("begin test branch staged refresh")?;
    let refresh = repository.begin_iceberg_refresh_intent(
        txn.as_mut(),
        BeginIcebergMvRefreshRequest {
            mv_id,
            target_catalog: "ice".to_string(),
            target_namespace: "analytics".to_string(),
            target_table: target_table.to_string(),
            staging_branch: format!("__nova_mv_refresh_{mv_id}_1001"),
            expected_main_snapshot_id: Some(200),
            base_snapshots: BTreeMap::from([("iceberg.sales.orders".to_string(), 50)]),
            marker_token: "marker-token-1001".to_string(),
        },
    )?;
    txn.commit()?;
    Ok(refresh.refresh_id)
}

fn overwrite_refresh_with_legacy_external_committed_payload(
    provider: &SqliteMetaStoreProvider,
    refresh_id: i64,
    mv_id: i64,
    target_snapshot_id: i64,
) -> Result<(), Box<dyn std::error::Error>> {
    let payload = serde_json::json!({
        "refresh_id": refresh_id,
        "mv_id": mv_id,
        "state": "EXTERNAL_COMMITTED",
        "target_snapshots": {},
        "external_outcome": {
            "target_snapshot_id": target_snapshot_id,
            "commit_id": format!("legacy-snapshot-{target_snapshot_id}")
        }
    });
    let mut txn = provider.begin_write("write legacy external committed refresh")?;
    txn.put(MetaRecordPut::new(
        MetaKey::new("mv", ["refresh".to_string(), refresh_id.to_string()])?,
        MetaRecordKind::new("mv.refresh")?,
        ExpectedRevision::Any,
        encode_json_payload(1, &payload)?,
    ))?;
    txn.commit()?;
    Ok(())
}

#[test]
fn mv_repository_staging_commit_retry_is_value_checked() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let provider = SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite"))?;
    let repository = MvMetaRepository::default();
    let mv_id = create_test_iceberg_mv(&provider, &repository)?;
    let refresh_id = begin_test_branch_staged_refresh(&provider, &repository, mv_id)?;
    let base_table_uuids = BTreeMap::from([(
        "iceberg.sales.orders".to_string(),
        "uuid-orders".to_string(),
    )]);

    {
        let mut txn = provider.begin_write("record staging commit")?;
        repository.record_staging_commit(
            txn.as_mut(),
            RecordStagingCommitRequest {
                refresh_id,
                staging_snapshot_id: 300,
                rows: 3,
                base_table_uuids: base_table_uuids.clone(),
            },
        )?;
        txn.commit()?;
    }

    {
        let mut txn = provider.begin_write("retry same staging commit")?;
        repository.record_staging_commit(
            txn.as_mut(),
            RecordStagingCommitRequest {
                refresh_id,
                staging_snapshot_id: 300,
                rows: 3,
                base_table_uuids: base_table_uuids.clone(),
            },
        )?;
        txn.commit()?;
    }

    {
        let mut txn = provider.begin_write("retry different staging commit")?;
        let err = repository
            .record_staging_commit(
                txn.as_mut(),
                RecordStagingCommitRequest {
                    refresh_id,
                    staging_snapshot_id: 301,
                    rows: 3,
                    base_table_uuids,
                },
            )
            .expect_err("different staging retry should conflict");
        assert_eq!(err.kind(), RepositoryErrorKind::Conflict);
    }

    Ok(())
}

#[test]
fn mv_repository_publish_commit_retry_is_value_checked() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let provider = SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite"))?;
    let repository = MvMetaRepository::default();
    let mv_id = create_test_iceberg_mv(&provider, &repository)?;
    let refresh_id = begin_test_branch_staged_refresh(&provider, &repository, mv_id)?;

    {
        let mut txn = provider.begin_write("record staging commit")?;
        repository.record_staging_commit(
            txn.as_mut(),
            RecordStagingCommitRequest {
                refresh_id,
                staging_snapshot_id: 300,
                rows: 3,
                base_table_uuids: BTreeMap::new(),
            },
        )?;
        txn.commit()?;
    }

    {
        let mut txn = provider.begin_write("record publish commit")?;
        repository.record_publish_commit(
            txn.as_mut(),
            RecordPublishCommitRequest {
                refresh_id,
                published_snapshot_id: 300,
            },
        )?;
        txn.commit()?;
    }

    {
        let mut txn = provider.begin_write("retry same publish commit")?;
        repository.record_publish_commit(
            txn.as_mut(),
            RecordPublishCommitRequest {
                refresh_id,
                published_snapshot_id: 300,
            },
        )?;
        txn.commit()?;
    }

    {
        let mut txn = provider.begin_write("retry different publish commit")?;
        let err = repository
            .record_publish_commit(
                txn.as_mut(),
                RecordPublishCommitRequest {
                    refresh_id,
                    published_snapshot_id: 301,
                },
            )
            .expect_err("different publish retry should conflict");
        assert_eq!(err.kind(), RepositoryErrorKind::Conflict);
    }

    Ok(())
}

#[test]
fn mv_repository_finalize_rejects_mismatched_published_snapshot()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let provider = SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite"))?;
    let repository = MvMetaRepository::default();
    let mv_id = create_test_iceberg_mv(&provider, &repository)?;
    let refresh_id = begin_test_branch_staged_refresh(&provider, &repository, mv_id)?;

    {
        let mut txn = provider.begin_write("record staging commit")?;
        repository.record_staging_commit(
            txn.as_mut(),
            RecordStagingCommitRequest {
                refresh_id,
                staging_snapshot_id: 300,
                rows: 3,
                base_table_uuids: BTreeMap::new(),
            },
        )?;
        repository.record_publish_commit(
            txn.as_mut(),
            RecordPublishCommitRequest {
                refresh_id,
                published_snapshot_id: 300,
            },
        )?;
        txn.commit()?;
    }

    {
        let mut txn = provider.begin_write("finalize with mismatched snapshot")?;
        let err = repository
            .finalize_refresh(
                txn.as_mut(),
                MvRefreshFinalizeRequest {
                    refresh_id,
                    rows: 3,
                    base_snapshots: BTreeMap::new(),
                    base_table_uuids: BTreeMap::new(),
                    target_snapshot_id: Some(301),
                },
            )
            .expect_err("mismatched final snapshot should conflict");
        assert_eq!(err.kind(), RepositoryErrorKind::Conflict);
    }

    Ok(())
}

#[test]
fn mv_repository_finalizes_legacy_external_committed_refresh()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let provider = SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite"))?;
    let repository = MvMetaRepository::default();
    let mv_id = create_test_iceberg_mv(&provider, &repository)?;
    let refresh_id = begin_test_branch_staged_refresh(&provider, &repository, mv_id)?;
    overwrite_refresh_with_legacy_external_committed_payload(&provider, refresh_id, mv_id, 300)?;

    {
        let mut txn = provider.begin_write("finalize with mismatched legacy snapshot")?;
        let err = repository
            .finalize_refresh(
                txn.as_mut(),
                MvRefreshFinalizeRequest {
                    refresh_id,
                    rows: 3,
                    base_snapshots: BTreeMap::new(),
                    base_table_uuids: BTreeMap::new(),
                    target_snapshot_id: Some(301),
                },
            )
            .expect_err("mismatched legacy snapshot should conflict");
        assert_eq!(err.kind(), RepositoryErrorKind::Conflict);
    }

    {
        let mut txn = provider.begin_write("finalize with legacy snapshot")?;
        repository.finalize_refresh(
            txn.as_mut(),
            MvRefreshFinalizeRequest {
                refresh_id,
                rows: 3,
                base_snapshots: BTreeMap::new(),
                base_table_uuids: BTreeMap::new(),
                target_snapshot_id: Some(300),
            },
        )?;
        txn.commit()?;
    }

    let read = provider.begin_read()?;
    let refresh = repository
        .load_refresh(read.as_ref(), refresh_id)?
        .expect("refresh should persist");
    assert_eq!(refresh.state, MvRefreshState::Finalized);
    assert_eq!(refresh.published_snapshot_id, None);
    assert_eq!(
        refresh
            .external_outcome
            .as_ref()
            .and_then(|outcome| outcome.target_snapshot_id),
        Some(300)
    );
    let definition = repository
        .load_by_id(read.as_ref(), mv_id)?
        .expect("definition should persist");
    assert_eq!(definition.last_refreshed_iceberg_snapshot_id, Some(300));
    assert_eq!(definition.active_refresh_id, None);
    assert!(!definition.refresh_in_progress);

    Ok(())
}

#[test]
fn mv_repository_managed_summary_rejects_active_commit_unknown()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let provider = SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite"))?;
    let repository = MvMetaRepository::default();
    let mv_id = create_test_iceberg_mv(&provider, &repository)?;
    let refresh_id = begin_test_branch_staged_refresh(&provider, &repository, mv_id)?;

    {
        let mut txn = provider.begin_write("mark commit unknown")?;
        repository.mark_refresh_commit_unknown(txn.as_mut(), refresh_id)?;
        txn.commit()?;
    }

    {
        let mut txn = provider.begin_write("update managed summary")?;
        let err = repository
            .update_managed_refresh_summary_if_present(
                txn.as_mut(),
                UpdateManagedMvRefreshSummaryRequest {
                    mv_id,
                    last_refresh_ms: 20,
                    last_refresh_rows: 3,
                    base_snapshots: BTreeMap::new(),
                    base_table_uuids: BTreeMap::new(),
                },
            )
            .expect_err("commit-unknown refresh must not be finalized");
        assert_eq!(err.kind(), RepositoryErrorKind::Conflict);
    }

    let read = provider.begin_read()?;
    let refresh = repository
        .load_refresh(read.as_ref(), refresh_id)?
        .expect("refresh should persist");
    assert_eq!(refresh.state, MvRefreshState::CommitUnknown);
    let definition = repository
        .load_by_id(read.as_ref(), mv_id)?
        .expect("definition should persist");
    assert_eq!(definition.active_refresh_id, Some(refresh_id));
    assert!(definition.refresh_in_progress);

    Ok(())
}

#[test]
fn mv_repository_clear_refresh_progress_rejects_active_commit_unknown()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let provider = SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite"))?;
    let repository = MvMetaRepository::default();
    let mv_id = create_test_iceberg_mv(&provider, &repository)?;
    let refresh_id = begin_test_branch_staged_refresh(&provider, &repository, mv_id)?;

    {
        let mut txn = provider.begin_write("mark commit unknown")?;
        repository.mark_refresh_commit_unknown(txn.as_mut(), refresh_id)?;
        txn.commit()?;
    }

    {
        let mut txn = provider.begin_write("clear commit unknown refresh")?;
        let err = repository
            .clear_refresh_progress(txn.as_mut(), mv_id)
            .expect_err("commit-unknown refresh must not be cleared");
        assert_eq!(err.kind(), RepositoryErrorKind::Conflict);
    }

    let read = provider.begin_read()?;
    let refresh = repository
        .load_refresh(read.as_ref(), refresh_id)?
        .expect("refresh should persist");
    assert_eq!(refresh.state, MvRefreshState::CommitUnknown);
    let definition = repository
        .load_by_id(read.as_ref(), mv_id)?
        .expect("definition should persist");
    assert_eq!(definition.active_refresh_id, Some(refresh_id));
    assert!(definition.refresh_in_progress);

    Ok(())
}

#[test]
fn mv_repository_rejects_second_refresh_intent() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let provider = SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite"))?;
    let repository = MvMetaRepository::default();

    let mv_id = {
        let mut txn = provider.begin_write("create mv definition")?;
        let definition = repository.create_definition(
            txn.as_mut(),
            CreateMvDefinitionRequest {
                select_sql: "SELECT id, amount FROM iceberg.sales.orders".to_string(),
                base_table_refs: vec!["iceberg.sales.orders".to_string()],
                primary_key_columns: vec!["id".to_string()],
                storage_engine: "iceberg".to_string(),
                target_catalog: Some("ice".to_string()),
                target_namespace: Some("ns".to_string()),
                target_table: Some("orders_mv".to_string()),
                target_apply_key: None,
                created_at_ms: 11,
            },
        )?;
        txn.commit()?;
        definition.mv_id
    };

    let refresh_a = {
        let mut txn = provider.begin_write("begin mv refresh a")?;
        let mut target_snapshots = BTreeMap::new();
        target_snapshots.insert("ice.ns.orders_mv".to_string(), 100);
        let refresh = repository.begin_refresh_intent(txn.as_mut(), mv_id, target_snapshots)?;
        txn.commit()?;
        refresh
    };

    {
        let mut txn = provider.begin_write("begin mv refresh b")?;
        let mut target_snapshots = BTreeMap::new();
        target_snapshots.insert("ice.ns.orders_mv".to_string(), 999);
        let err = repository
            .begin_refresh_intent(txn.as_mut(), mv_id, target_snapshots)
            .expect_err("second refresh should be rejected");
        assert_eq!(err.kind(), RepositoryErrorKind::Conflict);
    }

    let read = provider.begin_read()?;
    let definition = repository
        .load_by_id(read.as_ref(), mv_id)?
        .expect("definition should exist");
    assert!(definition.refresh_in_progress);
    assert_eq!(definition.active_refresh_id, Some(refresh_a.refresh_id));
    assert_eq!(definition.refresh_target_snapshots["ice.ns.orders_mv"], 100);

    let persisted_refresh_a = repository
        .load_refresh(read.as_ref(), refresh_a.refresh_id)?
        .expect("refresh a should persist");
    assert_eq!(
        persisted_refresh_a.target_snapshots["ice.ns.orders_mv"],
        100
    );

    Ok(())
}

#[test]
fn mv_repository_rejects_definition_schema_version_mismatch()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let provider = SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite"))?;
    let repository = MvMetaRepository::default();
    let key = MetaKey::new("mv", ["by-id", "1"])?;
    let payload = serde_json::json!({
        "mv_id": 1,
        "select_sql": "SELECT id FROM iceberg.sales.orders",
        "base_table_refs": ["iceberg.sales.orders"],
        "primary_key_columns": ["id"],
        "storage_engine": "iceberg",
        "target_catalog": "ice",
        "target_namespace": "ns",
        "target_table": "orders_mv",
        "last_refresh_ms": null,
        "last_refresh_rows": null,
        "last_refresh_snapshots": {},
        "last_refresh_table_uuids": {},
        "last_refreshed_iceberg_snapshot_id": null,
        "refresh_in_progress": false,
        "active_refresh_id": null,
        "refresh_target_snapshots": {},
        "created_at_ms": 11
    });

    {
        let mut txn = provider.begin_write("write mismatched mv definition")?;
        txn.put(MetaRecordPut::new(
            key,
            MetaRecordKind::new("mv.definition")?,
            ExpectedRevision::NotExists,
            encode_json_payload(999, &payload)?,
        ))?;
        txn.commit()?;
    }

    let read = provider.begin_read()?;
    let err = repository
        .load_by_id(read.as_ref(), 1)
        .expect_err("schema version mismatch should fail");
    assert!(
        err.to_string()
            .contains("metadata record by-id/1 has schema version 999")
    );

    Ok(())
}

#[test]
fn iceberg_catalog_repository_registers_catalog_namespace_and_table()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let provider = SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite"))?;
    let repository = IcebergCatalogMetaRepository::default();

    {
        let mut txn = provider.begin_write("register iceberg table")?;
        repository.upsert_catalog(
            txn.as_mut(),
            "ice",
            IcebergCatalogProperties {
                properties: vec![("type".to_string(), "rest".to_string())],
            },
        )?;
        repository.upsert_namespace(txn.as_mut(), "ice", "ns")?;
        repository.upsert_table(txn.as_mut(), "ice", "ns", "orders")?;
        txn.commit()?;
    }

    let read = provider.begin_read()?;
    assert!(repository.catalog_exists(read.as_ref(), "ICE")?);
    assert!(repository.namespace_exists(read.as_ref(), "ice", "NS")?);
    assert!(repository.table_exists(read.as_ref(), "ICE", "ns", "ORDERS")?);

    Ok(())
}

#[test]
fn iceberg_catalog_repository_lists_registered_catalog_objects()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let provider = SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite"))?;
    let repository = IcebergCatalogMetaRepository::default();

    {
        let mut txn = provider.begin_write("register iceberg objects")?;
        repository.upsert_catalog(
            txn.as_mut(),
            "ice",
            IcebergCatalogProperties {
                properties: vec![("type".to_string(), "rest".to_string())],
            },
        )?;
        repository.upsert_catalog(
            txn.as_mut(),
            "warehouse",
            IcebergCatalogProperties {
                properties: vec![("type".to_string(), "hadoop".to_string())],
            },
        )?;
        repository.upsert_namespace(txn.as_mut(), "ice", "sales")?;
        repository.upsert_namespace(txn.as_mut(), "ice", "finance")?;
        repository.upsert_table(txn.as_mut(), "ice", "sales", "orders")?;
        repository.upsert_table(txn.as_mut(), "ice", "finance", "payments")?;
        txn.commit()?;
    }

    let read = provider.begin_read()?;
    let catalogs = repository.list_catalogs(read.as_ref())?;
    assert_eq!(catalogs.len(), 2);
    assert_eq!(catalogs[0].catalog, "ice");
    assert_eq!(
        catalogs[0].properties.properties,
        vec![("type".to_string(), "rest".to_string())]
    );
    assert_eq!(catalogs[1].catalog, "warehouse");

    let namespaces = repository.list_namespaces(read.as_ref())?;
    assert_eq!(
        namespaces
            .iter()
            .map(|namespace| (namespace.catalog.as_str(), namespace.namespace.as_str()))
            .collect::<Vec<_>>(),
        vec![("ice", "finance"), ("ice", "sales")]
    );

    let tables = repository.list_tables(read.as_ref())?;
    assert_eq!(
        tables
            .iter()
            .map(|table| (
                table.catalog.as_str(),
                table.namespace.as_str(),
                table.table.as_str()
            ))
            .collect::<Vec<_>>(),
        vec![("ice", "finance", "payments"), ("ice", "sales", "orders")]
    );

    Ok(())
}

#[test]
fn iceberg_catalog_repository_deletes_table_and_related_mv_lookup()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let provider = SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite"))?;
    let catalog_repo = IcebergCatalogMetaRepository::default();
    let mv_repo = MvMetaRepository::default();

    {
        let mut txn = provider.begin_write("seed iceberg table and mv target")?;
        catalog_repo.upsert_catalog(
            txn.as_mut(),
            "ice",
            IcebergCatalogProperties {
                properties: vec![("type".to_string(), "rest".to_string())],
            },
        )?;
        catalog_repo.upsert_namespace(txn.as_mut(), "ice", "ns")?;
        catalog_repo.upsert_table(txn.as_mut(), "ice", "ns", "orders_mv")?;
        mv_repo.create_definition(
            txn.as_mut(),
            CreateMvDefinitionRequest {
                select_sql: "SELECT id, amount FROM iceberg.sales.orders".to_string(),
                base_table_refs: vec!["iceberg.sales.orders".to_string()],
                primary_key_columns: vec!["id".to_string()],
                storage_engine: "iceberg".to_string(),
                target_catalog: Some("ice".to_string()),
                target_namespace: Some("ns".to_string()),
                target_table: Some("orders_mv".to_string()),
                target_apply_key: None,
                created_at_ms: 11,
            },
        )?;
        txn.commit()?;
    }

    {
        let mut txn = provider.begin_write("delete iceberg table and mv lookup")?;
        catalog_repo.delete_table_and_mv_relationships(
            txn.as_mut(),
            &mv_repo,
            "ICE",
            "NS",
            "ORDERS_MV",
        )?;
        txn.commit()?;
    }

    let read = provider.begin_read()?;
    assert!(!catalog_repo.table_exists(read.as_ref(), "ICE", "ns", "ORDERS_MV")?);
    assert!(
        mv_repo
            .find_by_target(read.as_ref(), "ice", "ns", "orders_mv")?
            .is_none()
    );

    Ok(())
}

#[test]
fn iceberg_catalog_repository_rejects_delete_when_target_mv_refresh_is_active()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let provider = SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite"))?;
    let catalog_repo = IcebergCatalogMetaRepository::default();
    let mv_repo = MvMetaRepository::default();

    let mv_id = {
        let mut txn = provider.begin_write("seed active mv target")?;
        catalog_repo.upsert_catalog(
            txn.as_mut(),
            "ice",
            IcebergCatalogProperties {
                properties: vec![("type".to_string(), "rest".to_string())],
            },
        )?;
        catalog_repo.upsert_namespace(txn.as_mut(), "ice", "ns")?;
        catalog_repo.upsert_table(txn.as_mut(), "ice", "ns", "orders_mv")?;
        let definition = mv_repo.create_definition(
            txn.as_mut(),
            CreateMvDefinitionRequest {
                select_sql: "SELECT id, amount FROM iceberg.sales.orders".to_string(),
                base_table_refs: vec!["iceberg.sales.orders".to_string()],
                primary_key_columns: vec!["id".to_string()],
                storage_engine: "iceberg".to_string(),
                target_catalog: Some("ice".to_string()),
                target_namespace: Some("ns".to_string()),
                target_table: Some("orders_mv".to_string()),
                target_apply_key: None,
                created_at_ms: 11,
            },
        )?;
        txn.commit()?;
        definition.mv_id
    };

    {
        let mut txn = provider.begin_write("begin mv refresh")?;
        let mut target_snapshots = BTreeMap::new();
        target_snapshots.insert("ice.ns.orders_mv".to_string(), 100);
        mv_repo.begin_refresh_intent(txn.as_mut(), mv_id, target_snapshots)?;
        txn.commit()?;
    }

    {
        let mut txn = provider.begin_write("delete active mv target")?;
        let err = catalog_repo
            .delete_table_and_mv_relationships(txn.as_mut(), &mv_repo, "ICE", "NS", "ORDERS_MV")
            .expect_err("active refresh should block target deletion");
        assert_eq!(err.kind(), RepositoryErrorKind::Conflict);
    }

    let read = provider.begin_read()?;
    assert!(catalog_repo.table_exists(read.as_ref(), "ICE", "ns", "ORDERS_MV")?);
    assert!(
        mv_repo
            .find_by_target(read.as_ref(), "ice", "ns", "orders_mv")?
            .is_some()
    );
    let definition = mv_repo
        .load_by_id(read.as_ref(), mv_id)?
        .expect("definition should be preserved");
    assert!(definition.refresh_in_progress);
    assert!(definition.active_refresh_id.is_some());

    Ok(())
}

#[test]
fn mv_repository_rejects_stale_target_lookup_without_deleting_wrong_definition()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let provider = SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite"))?;
    let repository = MvMetaRepository::default();

    let mv_id = {
        let mut txn = provider.begin_write("seed mismatched mv lookup")?;
        let definition = repository.create_definition(
            txn.as_mut(),
            CreateMvDefinitionRequest {
                select_sql: "SELECT id, amount FROM iceberg.sales.orders".to_string(),
                base_table_refs: vec!["iceberg.sales.orders".to_string()],
                primary_key_columns: vec!["id".to_string()],
                storage_engine: "iceberg".to_string(),
                target_catalog: Some("ice".to_string()),
                target_namespace: Some("ns".to_string()),
                target_table: Some("other_mv".to_string()),
                target_apply_key: None,
                created_at_ms: 11,
            },
        )?;
        txn.put(MetaRecordPut::new(
            MetaKey::new("mv", ["by-target", "ice", "ns", "orders_mv"])?,
            MetaRecordKind::new("mv.target_lookup")?,
            ExpectedRevision::NotExists,
            encode_json_payload(
                1,
                &MvTargetLookup {
                    mv_id: definition.mv_id,
                },
            )?,
        ))?;
        txn.commit()?;
        definition.mv_id
    };

    {
        let mut txn = provider.begin_write("drop stale target lookup")?;
        let err = repository
            .drop_by_target(txn.as_mut(), "ice", "ns", "orders_mv")
            .expect_err("mismatched lookup should be rejected");
        assert_eq!(err.kind(), RepositoryErrorKind::Provider);
    }

    let read = provider.begin_read()?;
    let err = repository
        .find_by_target(read.as_ref(), "ice", "ns", "orders_mv")
        .expect_err("mismatched lookup read should be rejected");
    assert_eq!(err.kind(), RepositoryErrorKind::Provider);
    assert!(repository.load_by_id(read.as_ref(), mv_id)?.is_some());
    assert!(
        repository
            .find_by_target(read.as_ref(), "ice", "ns", "other_mv")?
            .is_some()
    );

    Ok(())
}

#[test]
fn mv_repository_lists_definitions() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let provider = SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite"))?;
    let repository = MvMetaRepository::default();

    {
        let mut txn = provider.begin_write("create mv definitions")?;
        repository.create_definition(
            txn.as_mut(),
            CreateMvDefinitionRequest {
                select_sql: "SELECT * FROM iceberg.sales.orders".to_string(),
                base_table_refs: vec!["iceberg.sales.orders".to_string()],
                primary_key_columns: vec!["id".to_string()],
                storage_engine: "iceberg".to_string(),
                target_catalog: Some("ice".to_string()),
                target_namespace: Some("sales".to_string()),
                target_table: Some("orders_mv".to_string()),
                target_apply_key: None,
                created_at_ms: 11,
            },
        )?;
        repository.create_definition(
            txn.as_mut(),
            CreateMvDefinitionRequest {
                select_sql: "SELECT * FROM local_table".to_string(),
                base_table_refs: vec!["local_table".to_string()],
                primary_key_columns: vec!["id".to_string()],
                storage_engine: "managed".to_string(),
                target_catalog: None,
                target_namespace: None,
                target_table: None,
                target_apply_key: None,
                created_at_ms: 12,
            },
        )?;
        txn.commit()?;
    }

    let read = provider.begin_read()?;
    let definitions = repository.list_definitions(read.as_ref())?;
    assert_eq!(definitions.len(), 2);
    assert_eq!(
        definitions[0].select_sql,
        "SELECT * FROM iceberg.sales.orders"
    );
    assert_eq!(definitions[1].select_sql, "SELECT * FROM local_table");

    Ok(())
}

#[test]
fn iceberg_catalog_repository_rejects_wrong_kind_and_schema_in_exists_apis()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let provider = SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite"))?;
    let repository = IcebergCatalogMetaRepository::default();

    {
        let mut txn = provider.begin_write("write invalid iceberg metadata records")?;
        txn.put(MetaRecordPut::new(
            MetaKey::new("iceberg_catalog", ["catalog", "ice"])?,
            MetaRecordKind::new("iceberg.namespace")?,
            ExpectedRevision::NotExists,
            encode_json_payload(
                1,
                &IcebergCatalogProperties {
                    properties: vec![("type".to_string(), "rest".to_string())],
                },
            )?,
        ))?;
        txn.put(MetaRecordPut::new(
            MetaKey::new("iceberg_catalog", ["namespace", "ice", "bad_schema"])?,
            MetaRecordKind::new("iceberg.namespace")?,
            ExpectedRevision::NotExists,
            encode_json_payload(
                999,
                &serde_json::json!({
                    "catalog": "ice",
                    "namespace": "bad_schema"
                }),
            )?,
        ))?;
        txn.put(MetaRecordPut::new(
            MetaKey::new("iceberg_catalog", ["table", "ice", "ns", "orders"])?,
            MetaRecordKind::new("iceberg.catalog")?,
            ExpectedRevision::NotExists,
            encode_json_payload(
                1,
                &serde_json::json!({
                    "catalog": "ice",
                    "namespace": "ns",
                    "table": "orders"
                }),
            )?,
        ))?;
        txn.put(MetaRecordPut::new(
            MetaKey::new("iceberg_catalog", ["table", "ice", "ns", "bad_schema"])?,
            MetaRecordKind::new("iceberg.table_registration")?,
            ExpectedRevision::NotExists,
            encode_json_payload(
                999,
                &serde_json::json!({
                    "catalog": "ice",
                    "namespace": "ns",
                    "table": "bad_schema"
                }),
            )?,
        ))?;
        txn.commit()?;
    }

    let read = provider.begin_read()?;
    let err = repository
        .catalog_exists(read.as_ref(), "ice")
        .expect_err("wrong catalog kind should fail");
    assert!(
        err.to_string()
            .contains("metadata record catalog/ice has kind iceberg.namespace")
    );

    let err = repository
        .namespace_exists(read.as_ref(), "ice", "bad_schema")
        .expect_err("wrong namespace schema should fail");
    assert!(
        err.to_string()
            .contains("metadata record namespace/ice/bad_schema has schema version 999")
    );

    let err = repository
        .table_exists(read.as_ref(), "ice", "ns", "orders")
        .expect_err("wrong table kind should fail");
    assert!(
        err.to_string()
            .contains("metadata record table/ice/ns/orders has kind iceberg.catalog")
    );

    let err = repository
        .table_exists(read.as_ref(), "ice", "ns", "bad_schema")
        .expect_err("wrong table schema should fail");
    assert!(
        err.to_string()
            .contains("metadata record table/ice/ns/bad_schema has schema version 999")
    );

    Ok(())
}
