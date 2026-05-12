use std::sync::{Arc, Weak};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::connector::starrocks::managed::config::ManagedLakeConfig;
use crate::engine::StandaloneState;
use crate::fs::oss::{oss_block_on, resolve_oss_operator_and_path_with_config};
use crate::novarocks_logging::warn;

const ERASE_RETRY_DELAY_MS: i64 = 5_000;
const ERASE_WORKER_POLL_INTERVAL: Duration = Duration::from_secs(2);

pub(crate) fn run_erase_jobs_once(state: &StandaloneState) -> Result<(), String> {
    let config = state
        .managed_lake_config
        .as_ref()
        .ok_or_else(|| "managed lake erase worker requires config".to_string())?;
    run_erase_jobs_once_with(state, |root_path| erase_root(root_path, config))
}

fn run_erase_jobs_once_with<F>(state: &StandaloneState, mut erase_root_fn: F) -> Result<(), String>
where
    F: FnMut(&str) -> Result<(), String>,
{
    let provider = state
        .metadata_provider
        .as_ref()
        .ok_or_else(|| "managed lake erase worker requires metadata provider".to_string())?;
    let now_ms = current_time_ms();
    let read = provider
        .begin_read()
        .map_err(|e| format!("open erase job read transaction failed: {e}"))?;
    let jobs = state
        .job_repo
        .list_runnable_erase_jobs(read.as_ref(), now_ms)
        .map_err(|e| format!("list erase jobs failed: {e}"))?;
    drop(read);

    for job in jobs {
        let claimed = {
            let mut txn = provider
                .begin_write("claim managed lake erase job")
                .map_err(|e| format!("open erase job claim transaction failed: {e}"))?;
            let claimed = state
                .job_repo
                .claim_erase_job(txn.as_mut(), job.job_id, current_time_ms())
                .map_err(|e| format!("claim erase job {} failed: {e}", job.job_id))?;
            txn.commit()
                .map_err(|e| format!("commit erase job claim failed: {e}"))?;
            claimed
        };
        if !claimed {
            continue;
        }

        let result: Result<(), String> = (|| {
            erase_root_fn(&job.root_path)?;
            let mut txn = provider
                .begin_write("finish managed lake erase job")
                .map_err(|e| format!("open erase job finish transaction failed: {e}"))?;
            match job.partition_id {
                None => {
                    state
                        .managed_txn_repo
                        .delete_for_table(txn.as_mut(), job.table_id)
                        .map_err(|e| format!("delete erased table txns failed: {e}"))?;
                    state
                        .managed_repo
                        .purge_retired_table_metadata(txn.as_mut(), job.table_id)
                        .map_err(|e| format!("purge erased table metadata failed: {e}"))?;
                }
                Some(partition_id) => {
                    state
                        .managed_txn_repo
                        .delete_for_partition(txn.as_mut(), partition_id)
                        .map_err(|e| format!("delete erased partition txns failed: {e}"))?;
                    state
                        .managed_repo
                        .purge_retired_partition_metadata(txn.as_mut(), partition_id)
                        .map_err(|e| format!("purge erased partition metadata failed: {e}"))?;
                }
            }
            state
                .job_repo
                .finish_erase_job(txn.as_mut(), job.job_id, current_time_ms())
                .map_err(|e| format!("finish erase job {} failed: {e}", job.job_id))?;
            txn.commit()
                .map_err(|e| format!("commit erase job finish failed: {e}"))?;
            Ok(())
        })();

        if let Err(err) = result {
            let retry_at_ms = current_time_ms() + ERASE_RETRY_DELAY_MS;
            let mut txn = provider
                .begin_write("fail managed lake erase job")
                .map_err(|e| format!("open erase job failure transaction failed: {e}"))?;
            state
                .job_repo
                .fail_erase_job(
                    txn.as_mut(),
                    job.job_id,
                    err.clone(),
                    Some(retry_at_ms),
                    current_time_ms(),
                )
                .map_err(|persist_err| {
                    format!(
                        "record erase failure for job {} failed after `{err}`: {persist_err}",
                        job.job_id
                    )
                })?;
            txn.commit()
                .map_err(|e| format!("commit erase job failure failed: {e}"))?;
        }
    }
    Ok(())
}

pub(crate) fn spawn_erase_worker(state: Arc<StandaloneState>) {
    let weak = Arc::downgrade(&state);
    thread::spawn(move || erase_worker_loop(weak));
}

fn erase_worker_loop(state: Weak<StandaloneState>) {
    loop {
        let Some(strong) = state.upgrade() else {
            return;
        };
        if strong.metadata_provider.is_none() {
            return;
        }
        if strong.managed_lake_config.is_none() {
            return;
        }

        if let Err(err) = run_erase_jobs_once(&strong) {
            warn!("managed lake erase worker iteration failed: {err}");
        }
        drop(strong);
        thread::sleep(ERASE_WORKER_POLL_INTERVAL);
    }
}

fn erase_root(root_path: &str, config: &ManagedLakeConfig) -> Result<(), String> {
    let object_store_cfg = config.s3.to_object_store_config();
    let (operator, rel_path) =
        resolve_oss_operator_and_path_with_config(root_path, &object_store_cfg)
            .map_err(|e| format!("resolve erase root `{root_path}` failed: {e}"))?;
    let erase_prefix = erase_prefix_path(&rel_path)
        .map_err(|e| format!("refuse to erase managed lake root `{root_path}`: {e}"))?;
    let remove_result = oss_block_on(operator.remove_all(&erase_prefix))
        .map_err(|e| format!("run erase root `{root_path}` failed: {e}"))?;
    remove_result.map_err(|e| format!("erase root `{root_path}` failed: {e}"))?;
    Ok(())
}

fn erase_prefix_path(rel_path: &str) -> Result<String, String> {
    let trimmed = rel_path.trim_matches('/');
    if trimmed.is_empty() {
        return Err("empty managed lake root".to_string());
    }
    Ok(format!("{trimmed}/"))
}

fn current_time_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::runtime::starlet_shard_registry::S3StoreConfig;

    use super::run_erase_jobs_once_with;
    use crate::connector::starrocks::managed::config::ManagedLakeConfig;
    use crate::connector::starrocks::managed::model::{
        ManagedEraseJobKind, ManagedEraseJobState, ManagedGlobalMeta, ManagedIndexState,
        ManagedPartitionState, ManagedSnapshot, ManagedTableKind, ManagedTableState,
        ManagedTxnState, StoredManagedDatabase, StoredManagedEraseJob, StoredManagedIndex,
        StoredManagedPartition, StoredManagedSchema, StoredManagedTable, StoredManagedTablet,
        StoredManagedTxn,
    };
    use crate::engine::StandaloneState;
    use crate::meta::repository::encode_json_payload;
    use crate::meta::{
        ExpectedRevision, MetaKey, MetaRecordKind, MetaRecordPut, MetaStoreProvider,
        SqliteMetaStoreProvider,
    };

    fn test_managed_config() -> ManagedLakeConfig {
        ManagedLakeConfig {
            warehouse_uri: "s3://test/warehouse".to_string(),
            s3: S3StoreConfig {
                endpoint: "http://127.0.0.1:9000".to_string(),
                bucket: "test".to_string(),
                root: "warehouse".to_string(),
                access_key_id: "ak".to_string(),
                access_key_secret: "sk".to_string(),
                region: Some("us-east-1".to_string()),
                enable_path_style_access: Some(true),
            },
            mv_default_storage_engine: "managed_lake".to_string(),
        }
    }

    fn test_state_with_snapshot(snapshot: ManagedSnapshot) -> (tempfile::TempDir, StandaloneState) {
        let dir = tempfile::tempdir().expect("tempdir");
        let provider = SqliteMetaStoreProvider::open(dir.path().join("standalone.sqlite"))
            .expect("open provider");
        {
            let mut txn = provider
                .begin_write("seed managed erase test")
                .expect("txn");
            seed_repository_snapshot(txn.as_mut(), &snapshot);
            txn.commit().expect("commit seed");
        }
        (
            dir,
            StandaloneState {
                managed_lake_config: Some(test_managed_config()),
                metadata_provider: Some(Arc::new(provider)),
                ..StandaloneState::default()
            },
        )
    }

    fn seed_repository_snapshot(
        txn: &mut dyn crate::meta::MetaWriteTxn,
        snapshot: &ManagedSnapshot,
    ) {
        for database in &snapshot.databases {
            put_record(
                txn,
                "managed",
                vec!["database".to_string(), database.db_id.to_string()],
                "managed.database",
                serde_json::json!({
                    "db_id": database.db_id,
                    "name": database.name,
                }),
            );
        }
        for table in &snapshot.tables {
            put_record(
                txn,
                "managed",
                vec!["table".to_string(), table.table_id.to_string()],
                "managed.table",
                serde_json::json!({
                    "table_id": table.table_id,
                    "db_id": table.db_id,
                    "name": table.name,
                    "keys_type": table.keys_type,
                    "bucket_num": table.bucket_num,
                    "current_schema_id": table.current_schema_id,
                    "state": table_state(table.state),
                    "kind": table_kind(table.kind),
                }),
            );
        }
        for schema in &snapshot.schemas {
            put_record(
                txn,
                "managed",
                vec!["schema".to_string(), schema.schema_id.to_string()],
                "managed.schema",
                serde_json::json!({
                    "schema_id": schema.schema_id,
                    "table_id": schema.table_id,
                    "schema_version": schema.schema_version,
                    "tablet_schema_pb": schema.tablet_schema_pb,
                }),
            );
        }
        for partition in &snapshot.partitions {
            put_record(
                txn,
                "managed",
                vec!["partition".to_string(), partition.partition_id.to_string()],
                "managed.partition",
                serde_json::json!({
                    "partition_id": partition.partition_id,
                    "table_id": partition.table_id,
                    "name": partition.name,
                    "visible_version": partition.visible_version,
                    "next_version": partition.next_version,
                    "state": partition_state(partition.state),
                }),
            );
        }
        for index in &snapshot.indexes {
            put_record(
                txn,
                "managed",
                vec!["index".to_string(), index.index_id.to_string()],
                "managed.index",
                serde_json::json!({
                    "index_id": index.index_id,
                    "table_id": index.table_id,
                    "partition_id": index.partition_id,
                    "index_type": index.index_type,
                    "state": index_state(index.state),
                }),
            );
        }
        for tablet in &snapshot.tablets {
            put_record(
                txn,
                "managed",
                vec!["tablet".to_string(), tablet.tablet_id.to_string()],
                "managed.tablet",
                serde_json::json!({
                    "tablet_id": tablet.tablet_id,
                    "partition_id": tablet.partition_id,
                    "index_id": tablet.index_id,
                    "bucket_seq": tablet.bucket_seq,
                    "tablet_root_path": tablet.tablet_root_path,
                }),
            );
        }
        for managed_txn in &snapshot.txns {
            put_record(
                txn,
                "managed.txn",
                vec![managed_txn.txn_id.to_string()],
                "managed.txn",
                serde_json::json!({
                    "txn_id": managed_txn.txn_id,
                    "table_id": managed_txn.table_id,
                    "partition_id": managed_txn.partition_id,
                    "base_version": managed_txn.base_version,
                    "commit_version": managed_txn.commit_version,
                    "state": txn_state(managed_txn.state),
                    "retry_at_ms": managed_txn.retry_at_ms,
                    "updated_at_ms": managed_txn.updated_at_ms,
                }),
            );
        }
        for job in &snapshot.erase_jobs {
            put_record(
                txn,
                "job",
                vec!["erase".to_string(), job.job_id.to_string()],
                "job.erase",
                serde_json::json!({
                    "job_id": job.job_id,
                    "table_id": job.table_id,
                    "partition_id": job.partition_id,
                    "root_path": job.root_path,
                    "state": erase_job_state(job.state),
                    "retry_at_ms": job.retry_at_ms,
                    "updated_at_ms": job.updated_at_ms,
                    "last_error": job.last_error,
                }),
            );
        }
    }

    fn put_record(
        txn: &mut dyn crate::meta::MetaWriteTxn,
        namespace: &str,
        path: Vec<String>,
        kind: &str,
        payload: serde_json::Value,
    ) {
        txn.put(MetaRecordPut::new(
            MetaKey::new(namespace, path).expect("key"),
            MetaRecordKind::new(kind).expect("kind"),
            ExpectedRevision::NotExists,
            encode_json_payload(1, &payload).expect("payload"),
        ))
        .expect("put record");
    }

    fn table_state(state: ManagedTableState) -> &'static str {
        match state {
            ManagedTableState::Creating => "CREATING",
            ManagedTableState::Active => "ACTIVE",
            ManagedTableState::Dropping => "DROPPING",
            ManagedTableState::Failed => "FAILED",
        }
    }

    fn table_kind(kind: ManagedTableKind) -> &'static str {
        match kind {
            ManagedTableKind::Table => "TABLE",
            ManagedTableKind::MaterializedView => "MATERIALIZED_VIEW",
        }
    }

    fn partition_state(state: ManagedPartitionState) -> &'static str {
        match state {
            ManagedPartitionState::Creating => "CREATING",
            ManagedPartitionState::Active => "ACTIVE",
            ManagedPartitionState::Retired => "RETIRED",
            ManagedPartitionState::Failed => "FAILED",
        }
    }

    fn index_state(state: ManagedIndexState) -> &'static str {
        match state {
            ManagedIndexState::Creating => "CREATING",
            ManagedIndexState::Active => "ACTIVE",
            ManagedIndexState::Retired => "RETIRED",
            ManagedIndexState::Failed => "FAILED",
        }
    }

    fn txn_state(state: ManagedTxnState) -> &'static str {
        match state {
            ManagedTxnState::Prepared => "PREPARED",
            ManagedTxnState::Written => "WRITTEN",
            ManagedTxnState::Visible => "VISIBLE",
            ManagedTxnState::Aborted => "ABORTED",
        }
    }

    fn erase_job_state(state: ManagedEraseJobState) -> &'static str {
        match state {
            ManagedEraseJobState::Pending => "PENDING",
            ManagedEraseJobState::Running => "RUNNING",
            ManagedEraseJobState::Failed => "FAILED",
            ManagedEraseJobState::Finished => "FINISHED",
        }
    }

    #[test]
    fn run_erase_jobs_once_finishes_drop_partition_job_and_purges_metadata() {
        let snapshot = ManagedSnapshot {
            global: ManagedGlobalMeta {
                warehouse_uri: "s3://test/warehouse".to_string(),
                next_db_id: 2,
                next_table_id: 11,
                next_partition_id: 22,
                next_index_id: 32,
                next_tablet_id: 42,
                next_txn_id: 62,
            },
            databases: vec![StoredManagedDatabase {
                db_id: 1,
                name: "analytics".to_string(),
            }],
            tables: vec![StoredManagedTable {
                table_id: 10,
                db_id: 1,
                name: "orders".to_string(),
                keys_type: "DUP_KEYS".to_string(),
                bucket_num: 1,
                current_schema_id: 100,
                state: ManagedTableState::Active,
                kind: ManagedTableKind::Table,
            }],
            schemas: vec![StoredManagedSchema {
                schema_id: 100,
                table_id: 10,
                schema_version: 0,
                tablet_schema_pb: vec![],
            }],
            columns: Vec::new(),
            partitions: vec![
                StoredManagedPartition {
                    partition_id: 20,
                    table_id: 10,
                    name: "p0".to_string(),
                    visible_version: 2,
                    next_version: 3,
                    state: ManagedPartitionState::Retired,
                },
                StoredManagedPartition {
                    partition_id: 21,
                    table_id: 10,
                    name: "p0".to_string(),
                    visible_version: 1,
                    next_version: 2,
                    state: ManagedPartitionState::Active,
                },
            ],
            indexes: vec![
                StoredManagedIndex {
                    index_id: 30,
                    table_id: 10,
                    partition_id: 20,
                    index_type: "BASE".to_string(),
                    state: ManagedIndexState::Retired,
                },
                StoredManagedIndex {
                    index_id: 31,
                    table_id: 10,
                    partition_id: 21,
                    index_type: "BASE".to_string(),
                    state: ManagedIndexState::Active,
                },
            ],
            tablets: vec![
                StoredManagedTablet {
                    tablet_id: 40,
                    partition_id: 20,
                    index_id: 30,
                    bucket_seq: 0,
                    tablet_root_path: "s3://test/warehouse/db_1/table_10/partition_20".to_string(),
                },
                StoredManagedTablet {
                    tablet_id: 41,
                    partition_id: 21,
                    index_id: 31,
                    bucket_seq: 0,
                    tablet_root_path: "s3://test/warehouse/db_1/table_10/partition_21".to_string(),
                },
            ],
            txns: vec![
                StoredManagedTxn {
                    txn_id: 60,
                    table_id: 10,
                    partition_id: 20,
                    base_version: 1,
                    commit_version: 2,
                    state: ManagedTxnState::Visible,
                    retry_at_ms: None,
                    updated_at_ms: 0,
                },
                StoredManagedTxn {
                    txn_id: 61,
                    table_id: 10,
                    partition_id: 21,
                    base_version: 0,
                    commit_version: 1,
                    state: ManagedTxnState::Visible,
                    retry_at_ms: None,
                    updated_at_ms: 0,
                },
            ],
            erase_jobs: vec![StoredManagedEraseJob {
                job_id: 1,
                job_kind: ManagedEraseJobKind::DropPartition,
                table_id: 10,
                partition_id: Some(20),
                root_path: "s3://test/warehouse/db_1/table_10/partition_20".to_string(),
                state: ManagedEraseJobState::Pending,
                retry_at_ms: None,
                updated_at_ms: 0,
                last_error: None,
            }],
            materialized_views: Vec::new(),
        };
        let (_dir, state) = test_state_with_snapshot(snapshot);

        run_erase_jobs_once_with(&state, |_| Ok(())).expect("run erase jobs once");

        let provider = state.metadata_provider.as_ref().expect("provider");
        let read = provider.begin_read().expect("read");
        let loaded = state
            .managed_repo
            .load_snapshot(read.as_ref())
            .expect("load snapshot");
        assert_eq!(loaded.partitions.len(), 1);
        assert_eq!(loaded.partitions[0].partition_id, 21);
        assert_eq!(loaded.indexes.len(), 1);
        assert_eq!(loaded.indexes[0].partition_id, 21);
        assert_eq!(loaded.tablets.len(), 1);
        assert_eq!(loaded.tablets[0].partition_id, 21);
        let txns = state
            .managed_txn_repo
            .list_all(read.as_ref())
            .expect("load txns");
        assert_eq!(txns.len(), 1);
        assert_eq!(txns[0].partition_id, 21);
        let job = state
            .job_repo
            .load_erase_job(read.as_ref(), 1)
            .expect("load job")
            .expect("job");
        assert_eq!(job.state, crate::meta::repository::job::JobState::Finished);
    }

    #[test]
    fn run_erase_jobs_once_marks_job_failed_and_preserves_metadata_on_error() {
        let snapshot = ManagedSnapshot {
            global: ManagedGlobalMeta {
                warehouse_uri: "s3://test/warehouse".to_string(),
                next_db_id: 2,
                next_table_id: 11,
                next_partition_id: 21,
                next_index_id: 31,
                next_tablet_id: 41,
                next_txn_id: 51,
            },
            databases: vec![StoredManagedDatabase {
                db_id: 1,
                name: "analytics".to_string(),
            }],
            tables: vec![StoredManagedTable {
                table_id: 10,
                db_id: 1,
                name: "orders".to_string(),
                keys_type: "DUP_KEYS".to_string(),
                bucket_num: 1,
                current_schema_id: 100,
                state: ManagedTableState::Dropping,
                kind: ManagedTableKind::Table,
            }],
            schemas: vec![StoredManagedSchema {
                schema_id: 100,
                table_id: 10,
                schema_version: 0,
                tablet_schema_pb: vec![],
            }],
            columns: Vec::new(),
            partitions: vec![StoredManagedPartition {
                partition_id: 20,
                table_id: 10,
                name: "p0".to_string(),
                visible_version: 1,
                next_version: 2,
                state: ManagedPartitionState::Retired,
            }],
            indexes: vec![StoredManagedIndex {
                index_id: 30,
                table_id: 10,
                partition_id: 20,
                index_type: "BASE".to_string(),
                state: ManagedIndexState::Retired,
            }],
            tablets: vec![StoredManagedTablet {
                tablet_id: 40,
                partition_id: 20,
                index_id: 30,
                bucket_seq: 0,
                tablet_root_path: "s3://test/warehouse/db_1/table_10/partition_20".to_string(),
            }],
            txns: vec![StoredManagedTxn {
                txn_id: 50,
                table_id: 10,
                partition_id: 20,
                base_version: 0,
                commit_version: 1,
                state: ManagedTxnState::Visible,
                retry_at_ms: None,
                updated_at_ms: 0,
            }],
            erase_jobs: vec![StoredManagedEraseJob {
                job_id: 1,
                job_kind: ManagedEraseJobKind::DropTable,
                table_id: 10,
                partition_id: None,
                root_path: "s3://test/warehouse/db_1/table_10".to_string(),
                state: ManagedEraseJobState::Pending,
                retry_at_ms: None,
                updated_at_ms: 0,
                last_error: None,
            }],
            materialized_views: Vec::new(),
        };
        let (_dir, state) = test_state_with_snapshot(snapshot);

        run_erase_jobs_once_with(&state, |_| Err("injected erase failure".to_string()))
            .expect("run erase jobs once");

        let provider = state.metadata_provider.as_ref().expect("provider");
        let read = provider.begin_read().expect("read");
        let loaded = state
            .managed_repo
            .load_snapshot(read.as_ref())
            .expect("load snapshot");
        assert_eq!(loaded.tables.len(), 1);
        assert_eq!(
            loaded.tables[0].state,
            crate::meta::repository::managed_lake::ManagedTableState::Dropping
        );
        assert_eq!(loaded.partitions.len(), 1);
        let job = state
            .job_repo
            .load_erase_job(read.as_ref(), 1)
            .expect("load job")
            .expect("job");
        assert_eq!(job.state, crate::meta::repository::job::JobState::Failed);
        assert!(
            job.last_error
                .as_deref()
                .is_some_and(|msg| msg.contains("injected erase failure"))
        );
    }

    #[test]
    fn erase_prefix_path_keeps_directory_boundary() {
        assert_eq!(
            super::erase_prefix_path("warehouse/db_70/table_124").expect("prefix"),
            "warehouse/db_70/table_124/"
        );
        assert_eq!(
            super::erase_prefix_path("warehouse/db_70/table_124/").expect("prefix"),
            "warehouse/db_70/table_124/"
        );
        assert!(
            super::erase_prefix_path("/")
                .expect_err("empty root must be rejected")
                .contains("empty")
        );
    }
}
