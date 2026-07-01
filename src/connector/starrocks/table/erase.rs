use std::sync::{Arc, Weak};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::connector::starrocks::fs_access::resolve_tablet_root;
use crate::connector::starrocks::table::config::StarRocksTableConfig;
use crate::engine::StandaloneState;
use crate::fs::object_store::oss_block_on;
use crate::novarocks_logging::warn;

const ERASE_RETRY_DELAY_MS: i64 = 5_000;
const ERASE_WORKER_POLL_INTERVAL: Duration = Duration::from_secs(2);

pub(crate) fn run_erase_jobs_once(state: &StandaloneState) -> Result<(), String> {
    let config = state
        .starrocks_table_config
        .as_ref()
        .ok_or_else(|| "StarRocks table erase worker requires config".to_string())?;
    run_erase_jobs_once_with(state, |root_path| erase_root(root_path, config))
}

fn run_erase_jobs_once_with<F>(state: &StandaloneState, mut erase_root_fn: F) -> Result<(), String>
where
    F: FnMut(&str) -> Result<(), String>,
{
    let provider = state
        .metadata_provider
        .as_ref()
        .ok_or_else(|| "StarRocks table erase worker requires metadata provider".to_string())?;
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
                .begin_write("claim StarRocks table erase job")
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
                .begin_write("finish StarRocks table erase job")
                .map_err(|e| format!("open erase job finish transaction failed: {e}"))?;
            match job.partition_id {
                None => {
                    state
                        .starrocks_txn_repo
                        .delete_for_table(txn.as_mut(), job.table_id)
                        .map_err(|e| format!("delete erased table txns failed: {e}"))?;
                    state
                        .starrocks_table_repo
                        .purge_retired_table_metadata(txn.as_mut(), job.table_id)
                        .map_err(|e| format!("purge erased table metadata failed: {e}"))?;
                }
                Some(partition_id) => {
                    state
                        .starrocks_txn_repo
                        .delete_for_partition(txn.as_mut(), partition_id)
                        .map_err(|e| format!("delete erased partition txns failed: {e}"))?;
                    state
                        .starrocks_table_repo
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
                .begin_write("fail StarRocks table erase job")
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
        if strong.starrocks_table_config.is_none() {
            return;
        }

        if let Err(err) = run_erase_jobs_once(&strong) {
            warn!("StarRocks table erase worker iteration failed: {err}");
        }
        drop(strong);
        thread::sleep(ERASE_WORKER_POLL_INTERVAL);
    }
}

fn erase_root(root_path: &str, config: &StarRocksTableConfig) -> Result<(), String> {
    let root_access = resolve_tablet_root(root_path, Some(&config.s3))
        .map_err(|e| format!("resolve erase root `{root_path}` failed: {e}"))?;
    let rel_path = root_access
        .single_relative_path()
        .map_err(|e| format!("resolve erase root `{root_path}` failed: {e}"))?;
    let warehouse_access =
        resolve_tablet_root(&config.warehouse_uri, Some(&config.s3)).map_err(|e| {
            format!(
                "resolve StarRocks table warehouse `{}` failed: {e}",
                config.warehouse_uri
            )
        })?;
    let warehouse_rel = warehouse_access.single_relative_path().map_err(|e| {
        format!(
            "resolve StarRocks table warehouse `{}` failed: {e}",
            config.warehouse_uri
        )
    })?;
    let erase_prefix = erase_prefix_path(&rel_path, &warehouse_rel)
        .map_err(|e| format!("refuse to erase StarRocks table root `{root_path}`: {e}"))?;
    let operator = root_access.operator();
    let remove_result = oss_block_on(operator.remove_all(&erase_prefix))
        .map_err(|e| format!("run erase root `{root_path}` failed: {e}"))?;
    remove_result.map_err(|e| format!("erase root `{root_path}` failed: {e}"))?;
    Ok(())
}

fn erase_prefix_path(rel_path: &str, warehouse_rel: &str) -> Result<String, String> {
    let trimmed = rel_path.trim_matches('/');
    let warehouse_trimmed = warehouse_rel.trim_matches('/');
    // Refuse erasing the bucket root or the entire StarRocks warehouse —
    // these would otherwise wipe data belonging to other StarRocks tables
    // or even the entire bucket.
    if trimmed.is_empty() || trimmed == warehouse_trimmed {
        return Err("empty StarRocks table root".to_string());
    }
    if !warehouse_trimmed.is_empty() && !trimmed.starts_with(&format!("{warehouse_trimmed}/")) {
        return Err(format!(
            "StarRocks table root `{trimmed}` is outside warehouse `{warehouse_trimmed}`"
        ));
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
    use crate::connector::starrocks::table::config::StarRocksTableConfig;
    use crate::connector::starrocks::table::model::{
        StarRocksEraseJobKind, StarRocksEraseJobState, StarRocksGlobalMeta, StarRocksIndexState,
        StarRocksPartitionState, StarRocksTableKind, StarRocksTableSnapshot, StarRocksTableState,
        StarRocksTxnState, StoredStarRocksDatabase, StoredStarRocksEraseJob, StoredStarRocksIndex,
        StoredStarRocksPartition, StoredStarRocksSchema, StoredStarRocksTable,
        StoredStarRocksTablet, StoredStarRocksTxn,
    };
    use crate::engine::StandaloneState;
    use crate::meta::repository::test_avro_seed::encode_seed_payload;
    use crate::meta::{
        ExpectedRevision, MetaKey, MetaRecordKind, MetaRecordPut, MetaStoreProvider,
        SqliteMetaStoreProvider,
    };

    fn test_starrocks_table_config() -> StarRocksTableConfig {
        StarRocksTableConfig {
            warehouse_uri: "s3://test/warehouse".to_string(),
            s3: S3StoreConfig {
                endpoint: "http://127.0.0.1:9000".to_string(),
                bucket: "test".to_string(),
                access_key_id: "ak".to_string(),
                access_key_secret: "sk".to_string(),
                region: Some("us-east-1".to_string()),
                enable_path_style_access: Some(true),
            },
            mv_default_storage_engine: "starrocks".to_string(),
        }
    }

    fn test_state_with_snapshot(
        snapshot: StarRocksTableSnapshot,
    ) -> (tempfile::TempDir, StandaloneState) {
        let dir = tempfile::tempdir().expect("tempdir");
        let provider = SqliteMetaStoreProvider::open(dir.path().join("standalone.sqlite"))
            .expect("open provider");
        {
            let mut txn = provider
                .begin_write("seed StarRocks erase test")
                .expect("txn");
            seed_repository_snapshot(txn.as_mut(), &snapshot);
            txn.commit().expect("commit seed");
        }
        (
            dir,
            StandaloneState {
                starrocks_table_config: Some(test_starrocks_table_config()),
                metadata_provider: Some(Arc::new(provider)),
                ..StandaloneState::default()
            },
        )
    }

    fn seed_repository_snapshot(
        txn: &mut dyn crate::meta::MetaWriteTxn,
        snapshot: &StarRocksTableSnapshot,
    ) {
        for database in &snapshot.databases {
            put_record(
                txn,
                "starrocks",
                vec!["database".to_string(), database.db_id.to_string()],
                "starrocks.database",
                serde_json::json!({
                    "db_id": database.db_id,
                    "name": database.name,
                }),
            );
        }
        for table in &snapshot.tables {
            put_record(
                txn,
                "starrocks",
                vec!["table".to_string(), table.table_id.to_string()],
                "starrocks.table",
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
                "starrocks",
                vec!["schema".to_string(), schema.schema_id.to_string()],
                "starrocks.schema",
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
                "starrocks",
                vec!["partition".to_string(), partition.partition_id.to_string()],
                "starrocks.partition",
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
                "starrocks",
                vec!["index".to_string(), index.index_id.to_string()],
                "starrocks.index",
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
                "starrocks",
                vec!["tablet".to_string(), tablet.tablet_id.to_string()],
                "starrocks.tablet",
                serde_json::json!({
                    "tablet_id": tablet.tablet_id,
                    "partition_id": tablet.partition_id,
                    "index_id": tablet.index_id,
                    "bucket_seq": tablet.bucket_seq,
                    "tablet_root_path": tablet.tablet_root_path,
                }),
            );
        }
        for starrocks_txn in &snapshot.txns {
            put_record(
                txn,
                "starrocks.txn",
                vec![starrocks_txn.txn_id.to_string()],
                "starrocks.txn",
                serde_json::json!({
                    "txn_id": starrocks_txn.txn_id,
                    "table_id": starrocks_txn.table_id,
                    "partition_id": starrocks_txn.partition_id,
                    "base_version": starrocks_txn.base_version,
                    "commit_version": starrocks_txn.commit_version,
                    "state": txn_state(starrocks_txn.state),
                    "retry_at_ms": starrocks_txn.retry_at_ms,
                    "updated_at_ms": starrocks_txn.updated_at_ms,
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
            encode_seed_payload(kind, &payload).expect("payload"),
        ))
        .expect("put record");
    }

    fn table_state(state: StarRocksTableState) -> &'static str {
        match state {
            StarRocksTableState::Creating => "CREATING",
            StarRocksTableState::Active => "ACTIVE",
            StarRocksTableState::Dropping => "DROPPING",
            StarRocksTableState::Failed => "FAILED",
        }
    }

    fn table_kind(kind: StarRocksTableKind) -> &'static str {
        match kind {
            StarRocksTableKind::Table => "TABLE",
            StarRocksTableKind::MaterializedView => "MATERIALIZED_VIEW",
        }
    }

    fn partition_state(state: StarRocksPartitionState) -> &'static str {
        match state {
            StarRocksPartitionState::Creating => "CREATING",
            StarRocksPartitionState::Active => "ACTIVE",
            StarRocksPartitionState::Retired => "RETIRED",
            StarRocksPartitionState::Failed => "FAILED",
        }
    }

    fn index_state(state: StarRocksIndexState) -> &'static str {
        match state {
            StarRocksIndexState::Creating => "CREATING",
            StarRocksIndexState::Active => "ACTIVE",
            StarRocksIndexState::Retired => "RETIRED",
            StarRocksIndexState::Failed => "FAILED",
        }
    }

    fn txn_state(state: StarRocksTxnState) -> &'static str {
        match state {
            StarRocksTxnState::Prepared => "PREPARED",
            StarRocksTxnState::Written => "WRITTEN",
            StarRocksTxnState::Visible => "VISIBLE",
            StarRocksTxnState::Aborted => "ABORTED",
        }
    }

    fn erase_job_state(state: StarRocksEraseJobState) -> &'static str {
        match state {
            StarRocksEraseJobState::Pending => "PENDING",
            StarRocksEraseJobState::Running => "RUNNING",
            StarRocksEraseJobState::Failed => "FAILED",
            StarRocksEraseJobState::Finished => "FINISHED",
        }
    }

    #[test]
    fn run_erase_jobs_once_finishes_drop_partition_job_and_purges_metadata() {
        let snapshot = StarRocksTableSnapshot {
            global: StarRocksGlobalMeta {
                warehouse_uri: "s3://test/warehouse".to_string(),
                next_db_id: 2,
                next_table_id: 11,
                next_partition_id: 22,
                next_index_id: 32,
                next_tablet_id: 42,
                next_txn_id: 62,
            },
            databases: vec![StoredStarRocksDatabase {
                db_id: 1,
                name: "analytics".to_string(),
            }],
            tables: vec![StoredStarRocksTable {
                table_id: 10,
                db_id: 1,
                name: "orders".to_string(),
                keys_type: "DUP_KEYS".to_string(),
                bucket_num: 1,
                current_schema_id: 100,
                state: StarRocksTableState::Active,
                kind: StarRocksTableKind::Table,
            }],
            schemas: vec![StoredStarRocksSchema {
                schema_id: 100,
                table_id: 10,
                schema_version: 0,
                tablet_schema_pb: vec![],
            }],
            columns: Vec::new(),
            partitions: vec![
                StoredStarRocksPartition {
                    partition_id: 20,
                    table_id: 10,
                    name: "p0".to_string(),
                    visible_version: 2,
                    next_version: 3,
                    state: StarRocksPartitionState::Retired,
                },
                StoredStarRocksPartition {
                    partition_id: 21,
                    table_id: 10,
                    name: "p0".to_string(),
                    visible_version: 1,
                    next_version: 2,
                    state: StarRocksPartitionState::Active,
                },
            ],
            indexes: vec![
                StoredStarRocksIndex {
                    index_id: 30,
                    table_id: 10,
                    partition_id: 20,
                    index_type: "BASE".to_string(),
                    state: StarRocksIndexState::Retired,
                },
                StoredStarRocksIndex {
                    index_id: 31,
                    table_id: 10,
                    partition_id: 21,
                    index_type: "BASE".to_string(),
                    state: StarRocksIndexState::Active,
                },
            ],
            tablets: vec![
                StoredStarRocksTablet {
                    tablet_id: 40,
                    partition_id: 20,
                    index_id: 30,
                    bucket_seq: 0,
                    tablet_root_path: "s3://test/warehouse/db_1/table_10/partition_20".to_string(),
                },
                StoredStarRocksTablet {
                    tablet_id: 41,
                    partition_id: 21,
                    index_id: 31,
                    bucket_seq: 0,
                    tablet_root_path: "s3://test/warehouse/db_1/table_10/partition_21".to_string(),
                },
            ],
            txns: vec![
                StoredStarRocksTxn {
                    txn_id: 60,
                    table_id: 10,
                    partition_id: 20,
                    base_version: 1,
                    commit_version: 2,
                    state: StarRocksTxnState::Visible,
                    retry_at_ms: None,
                    updated_at_ms: 0,
                },
                StoredStarRocksTxn {
                    txn_id: 61,
                    table_id: 10,
                    partition_id: 21,
                    base_version: 0,
                    commit_version: 1,
                    state: StarRocksTxnState::Visible,
                    retry_at_ms: None,
                    updated_at_ms: 0,
                },
            ],
            erase_jobs: vec![StoredStarRocksEraseJob {
                job_id: 1,
                job_kind: StarRocksEraseJobKind::DropPartition,
                table_id: 10,
                partition_id: Some(20),
                root_path: "s3://test/warehouse/db_1/table_10/partition_20".to_string(),
                state: StarRocksEraseJobState::Pending,
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
            .starrocks_table_repo
            .load_snapshot(read.as_ref())
            .expect("load snapshot");
        assert_eq!(loaded.partitions.len(), 1);
        assert_eq!(loaded.partitions[0].partition_id, 21);
        assert_eq!(loaded.indexes.len(), 1);
        assert_eq!(loaded.indexes[0].partition_id, 21);
        assert_eq!(loaded.tablets.len(), 1);
        assert_eq!(loaded.tablets[0].partition_id, 21);
        let txns = state
            .starrocks_txn_repo
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
        let snapshot = StarRocksTableSnapshot {
            global: StarRocksGlobalMeta {
                warehouse_uri: "s3://test/warehouse".to_string(),
                next_db_id: 2,
                next_table_id: 11,
                next_partition_id: 21,
                next_index_id: 31,
                next_tablet_id: 41,
                next_txn_id: 51,
            },
            databases: vec![StoredStarRocksDatabase {
                db_id: 1,
                name: "analytics".to_string(),
            }],
            tables: vec![StoredStarRocksTable {
                table_id: 10,
                db_id: 1,
                name: "orders".to_string(),
                keys_type: "DUP_KEYS".to_string(),
                bucket_num: 1,
                current_schema_id: 100,
                state: StarRocksTableState::Dropping,
                kind: StarRocksTableKind::Table,
            }],
            schemas: vec![StoredStarRocksSchema {
                schema_id: 100,
                table_id: 10,
                schema_version: 0,
                tablet_schema_pb: vec![],
            }],
            columns: Vec::new(),
            partitions: vec![StoredStarRocksPartition {
                partition_id: 20,
                table_id: 10,
                name: "p0".to_string(),
                visible_version: 1,
                next_version: 2,
                state: StarRocksPartitionState::Retired,
            }],
            indexes: vec![StoredStarRocksIndex {
                index_id: 30,
                table_id: 10,
                partition_id: 20,
                index_type: "BASE".to_string(),
                state: StarRocksIndexState::Retired,
            }],
            tablets: vec![StoredStarRocksTablet {
                tablet_id: 40,
                partition_id: 20,
                index_id: 30,
                bucket_seq: 0,
                tablet_root_path: "s3://test/warehouse/db_1/table_10/partition_20".to_string(),
            }],
            txns: vec![StoredStarRocksTxn {
                txn_id: 50,
                table_id: 10,
                partition_id: 20,
                base_version: 0,
                commit_version: 1,
                state: StarRocksTxnState::Visible,
                retry_at_ms: None,
                updated_at_ms: 0,
            }],
            erase_jobs: vec![StoredStarRocksEraseJob {
                job_id: 1,
                job_kind: StarRocksEraseJobKind::DropTable,
                table_id: 10,
                partition_id: None,
                root_path: "s3://test/warehouse/db_1/table_10".to_string(),
                state: StarRocksEraseJobState::Pending,
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
            .starrocks_table_repo
            .load_snapshot(read.as_ref())
            .expect("load snapshot");
        assert_eq!(loaded.tables.len(), 1);
        assert_eq!(
            loaded.tables[0].state,
            crate::meta::repository::starrocks_table::StarRocksTableState::Dropping
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
            super::erase_prefix_path("warehouse/db_70/table_124", "warehouse").expect("prefix"),
            "warehouse/db_70/table_124/"
        );
        assert_eq!(
            super::erase_prefix_path("warehouse/db_70/table_124/", "warehouse").expect("prefix"),
            "warehouse/db_70/table_124/"
        );
        assert!(
            super::erase_prefix_path("/", "warehouse")
                .expect_err("empty root must be rejected")
                .contains("empty")
        );
    }

    #[test]
    fn erase_prefix_path_rejects_warehouse_root_itself() {
        // After the cluster-level S3 refactor the OpenDAL operator always
        // runs from the bucket root, so the warehouse path component lives
        // in the relative key. The safety check must therefore explicitly
        // refuse keys that resolve back to the warehouse root.
        let err = super::erase_prefix_path("warehouse", "warehouse")
            .expect_err("warehouse root itself must be rejected");
        assert!(err.contains("empty"), "err={err}");
        let err = super::erase_prefix_path("/warehouse/", "warehouse")
            .expect_err("warehouse root with surrounding slashes must be rejected");
        assert!(err.contains("empty"), "err={err}");
    }

    #[test]
    fn erase_prefix_path_rejects_paths_outside_warehouse_prefix() {
        let err = super::erase_prefix_path("other-prefix/db_70/table_124", "warehouse")
            .expect_err("unrelated same-bucket prefix must be rejected");
        assert!(err.contains("outside warehouse"), "err={err}");

        let err = super::erase_prefix_path("warehouse2/db_70/table_124", "warehouse")
            .expect_err("lookalike warehouse prefix must be rejected");
        assert!(err.contains("outside warehouse"), "err={err}");
    }

    #[test]
    fn erase_root_rejects_bucket_mismatch_before_delete() {
        let config = test_starrocks_table_config();
        let err = super::erase_root("s3://other/warehouse/db_1/table_10", &config)
            .expect_err("erase root must reject bucket mismatch");

        assert!(err.contains("other"), "err={err}");
        assert!(err.contains("test"), "err={err}");
        assert!(err.contains("resolve erase root"), "err={err}");
    }
}
