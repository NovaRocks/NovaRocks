use std::sync::{Arc, Weak};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::connector::starrocks::managed::config::ManagedLakeConfig;
use crate::connector::starrocks::managed::store::{ManagedEraseJobKind, SqliteMetadataStore};
use crate::fs::oss::{oss_block_on, resolve_oss_operator_and_path_with_config};
use crate::novarocks_logging::warn;
use crate::standalone::engine::StandaloneState;

const ERASE_RETRY_DELAY_MS: i64 = 5_000;
const ERASE_WORKER_POLL_INTERVAL: Duration = Duration::from_secs(2);

pub(crate) fn run_erase_jobs_once(
    store: &SqliteMetadataStore,
    config: &ManagedLakeConfig,
) -> Result<(), String> {
    run_erase_jobs_once_with(store, config, |root_path| erase_root(root_path, config))
}

fn run_erase_jobs_once_with<F>(
    store: &SqliteMetadataStore,
    _config: &ManagedLakeConfig,
    mut erase_root_fn: F,
) -> Result<(), String>
where
    F: FnMut(&str) -> Result<(), String>,
{
    let now_ms = current_time_ms();
    for job in store.list_runnable_erase_jobs(now_ms)? {
        if !store.claim_erase_job(job.job_id)? {
            continue;
        }

        let result: Result<(), String> = (|| {
            erase_root_fn(&job.root_path)?;
            match job.job_kind {
                ManagedEraseJobKind::DropTable => {
                    store.purge_retired_table_metadata(job.table_id)?;
                }
                ManagedEraseJobKind::DropPartition => {
                    let partition_id = job.partition_id.ok_or_else(|| {
                        format!(
                            "drop-partition erase job {} is missing partition_id",
                            job.job_id
                        )
                    })?;
                    store.purge_retired_partition_metadata(partition_id)?;
                }
            }
            store.finish_erase_job(job.job_id)?;
            Ok(())
        })();

        if let Err(err) = result {
            let retry_at_ms = current_time_ms() + ERASE_RETRY_DELAY_MS;
            store
                .fail_erase_job(job.job_id, &err, retry_at_ms)
                .map_err(|persist_err| {
                    format!(
                        "record erase failure for job {} failed after `{err}`: {persist_err}",
                        job.job_id
                    )
                })?;
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
        let Some(store) = strong.metadata_store.clone() else {
            return;
        };
        let Some(config) = strong.managed_lake_config.clone() else {
            return;
        };
        drop(strong);

        if let Err(err) = run_erase_jobs_once(&store, &config) {
            warn!("managed lake erase worker iteration failed: {err}");
        }
        thread::sleep(ERASE_WORKER_POLL_INTERVAL);
    }
}

fn erase_root(root_path: &str, config: &ManagedLakeConfig) -> Result<(), String> {
    let object_store_cfg = config.s3.to_object_store_config();
    let (operator, rel_path) =
        resolve_oss_operator_and_path_with_config(root_path, &object_store_cfg)
            .map_err(|e| format!("resolve erase root `{root_path}` failed: {e}"))?;
    if rel_path.trim_matches('/').is_empty() {
        return Err(format!(
            "refuse to erase empty managed lake root resolved from `{root_path}`"
        ));
    }
    let remove_result = oss_block_on(operator.remove_all(&rel_path))
        .map_err(|e| format!("run erase root `{root_path}` failed: {e}"))?;
    remove_result.map_err(|e| format!("erase root `{root_path}` failed: {e}"))?;
    Ok(())
}

fn current_time_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

#[cfg(test)]
mod tests {
    use crate::runtime::starlet_shard_registry::S3StoreConfig;

    use super::run_erase_jobs_once_with;
    use crate::connector::starrocks::managed::config::ManagedLakeConfig;
    use crate::connector::starrocks::managed::store::{
        ManagedEraseJobKind, ManagedEraseJobState, ManagedGlobalMeta, ManagedIndexState,
        ManagedPartitionState, ManagedSnapshot, ManagedTableKind, ManagedTableState,
        ManagedTxnState, SqliteMetadataStore, StoredManagedDatabase, StoredManagedEraseJob,
        StoredManagedIndex, StoredManagedPartition, StoredManagedSchema, StoredManagedTable,
        StoredManagedTablet, StoredManagedTxn,
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
        }
    }

    fn test_store_with_snapshot(
        snapshot: ManagedSnapshot,
    ) -> (tempfile::TempDir, SqliteMetadataStore) {
        let dir = tempfile::tempdir().expect("tempdir");
        let store =
            SqliteMetadataStore::open(dir.path().join("standalone.sqlite")).expect("open store");
        store
            .replace_managed_snapshot(&snapshot)
            .expect("persist snapshot");
        (dir, store)
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
        let (_dir, store) = test_store_with_snapshot(snapshot);

        run_erase_jobs_once_with(&store, &test_managed_config(), |_| Ok(()))
            .expect("run erase jobs once");

        let loaded = store.load_snapshot().expect("load snapshot");
        assert_eq!(loaded.managed.partitions.len(), 1);
        assert_eq!(loaded.managed.partitions[0].partition_id, 21);
        assert_eq!(loaded.managed.indexes.len(), 1);
        assert_eq!(loaded.managed.indexes[0].partition_id, 21);
        assert_eq!(loaded.managed.tablets.len(), 1);
        assert_eq!(loaded.managed.tablets[0].partition_id, 21);
        assert_eq!(loaded.managed.txns.len(), 1);
        assert_eq!(loaded.managed.txns[0].partition_id, 21);
        assert_eq!(loaded.managed.erase_jobs.len(), 1);
        assert_eq!(
            loaded.managed.erase_jobs[0].state,
            ManagedEraseJobState::Finished
        );
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
        let (_dir, store) = test_store_with_snapshot(snapshot);

        run_erase_jobs_once_with(&store, &test_managed_config(), |_| {
            Err("injected erase failure".to_string())
        })
        .expect("run erase jobs once");

        let loaded = store.load_snapshot().expect("load snapshot");
        assert_eq!(loaded.managed.tables.len(), 1);
        assert_eq!(loaded.managed.tables[0].state, ManagedTableState::Dropping);
        assert_eq!(loaded.managed.partitions.len(), 1);
        assert_eq!(loaded.managed.erase_jobs.len(), 1);
        assert_eq!(
            loaded.managed.erase_jobs[0].state,
            ManagedEraseJobState::Failed
        );
        assert!(
            loaded.managed.erase_jobs[0]
                .last_error
                .as_deref()
                .is_some_and(|msg| msg.contains("injected erase failure"))
        );
    }
}
