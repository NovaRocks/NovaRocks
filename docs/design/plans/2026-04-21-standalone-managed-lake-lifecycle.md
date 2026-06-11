# Standalone Managed Lake Lifecycle Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 为 standalone managed lake table 落地第一阶段 lifecycle 闭环，支持 StarRocks 风格的 `DROP TABLE`、`TRUNCATE TABLE`、异步 erase 和重启恢复。

**Architecture:** 继续以 SQLite 作为 standalone 控制面真相源，managed runtime/catalog 只暴露 `Active` 对象；`DROP TABLE` 通过 `Dropping + erase_jobs` 让表先从 catalog 消失，`TRUNCATE TABLE` 通过 staged partition replacement 切换到新的空 partition。对象存储删除由后台 erase worker 异步完成，启动恢复负责清理 `Creating` 中间态并续跑未完成的 erase job。

**Tech Stack:** Rust, rusqlite, OpenDAL object store, StarRocks lake metadata/runtime, Cargo unit tests, standalone managed lake integration tests

---

## File Structure

- Modify: `/Users/harbor/project/NovaRocks/src/standalone/store.rs`
  - 扩展 managed state enums
  - 新增 `erase_jobs` schema、row types 和 CRUD
  - 提供 `DROP/TRUNCATE` 所需的事务接口
  - 把 managed metadata schema 提升到 lifecycle 版本
- Create: `/Users/harbor/project/NovaRocks/src/standalone/lake_erase.rs`
  - 后台 erase worker
  - 对象存储 root 删除
  - job 状态推进与 purge retired metadata
- Modify: `/Users/harbor/project/NovaRocks/src/standalone/lake_recovery.rs`
  - 把 root layout 改到 `partition_<id>`
  - active-only rebuild
  - reconcile `Creating` partition/table 和 lifecycle 中间态
- Modify: `/Users/harbor/project/NovaRocks/src/standalone/lake_ddl.rs`
  - managed `DROP TABLE`
  - managed `TRUNCATE TABLE`
  - truncate 的 stage/bootstrap/activate 三段流程
- Modify: `/Users/harbor/project/NovaRocks/src/standalone/engine.rs`
  - 把 managed `DROP/TRUNCATE` 从报错改成委派到 lifecycle 路径
  - 在 `StandaloneNovaRocks::open()` 启动 erase worker
- Modify: `/Users/harbor/project/NovaRocks/src/standalone/mod.rs`
  - 注册 `lake_erase` 模块
- Modify: `/Users/harbor/project/NovaRocks/tests/standalone_managed_lake.rs`
  - 增加 object-store-backed lifecycle integration tests
- Modify: `/Users/harbor/project/NovaRocks/tests/standalone_mysql_server.rs`
  - 增加 MySQL 协议下的 managed lifecycle smoke test

### 实现边界

- 不做旧 metadata 兼容迁移
- 不做 `RECOVER TABLE`
- 不做 `DROP TABLE FORCE`
- 不做复杂 recycle bin SQL 面
- 继续保持“每表一个 active partition”的用户语义

### Schema Version 约束

因为已经明确“不需要考虑兼容性”，实现时直接把 standalone managed metadata schema 提升到一个新的 `user_version`，并在打开旧库时 fail fast，而不是编写 v2 -> v3 migration。

---

### Task 1: Lock Metadata Schema and Partition-Scoped Paths

**Files:**
- Modify: `/Users/harbor/project/NovaRocks/src/standalone/store.rs`
- Modify: `/Users/harbor/project/NovaRocks/src/standalone/lake_recovery.rs`
- Test: `/Users/harbor/project/NovaRocks/src/standalone/store.rs`
- Test: `/Users/harbor/project/NovaRocks/src/standalone/lake_recovery.rs`

- [ ] **Step 1: 写出会失败的 store/lake_recovery 单元测试**

在 `store.rs` 增加 schema/layout round-trip 测试：

```rust
#[test]
fn standalone_store_round_trips_lifecycle_states_and_erase_jobs() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = SqliteMetadataStore::open(dir.path().join("standalone.sqlite")).expect("open");
    let snapshot = ManagedSnapshot {
        global: ManagedGlobalMeta {
            warehouse_uri: "s3://bucket/warehouse".to_string(),
            next_db_id: 2,
            next_table_id: 3,
            next_partition_id: 4,
            next_index_id: 5,
            next_tablet_id: 6,
            next_txn_id: 7,
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
            bucket_num: 2,
            current_schema_id: 10,
            state: ManagedTableState::Dropping,
        }],
        schemas: vec![],
        columns: vec![],
        partitions: vec![StoredManagedPartition {
            partition_id: 20,
            table_id: 10,
            name: "p0".to_string(),
            visible_version: 3,
            next_version: 4,
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
            tablet_root_path: "s3://bucket/warehouse/db_1/table_10/partition_20".to_string(),
        }],
        txns: vec![],
        erase_jobs: vec![StoredManagedEraseJob {
            job_id: 50,
            job_kind: ManagedEraseJobKind::DropTable,
            table_id: 10,
            partition_id: None,
            root_path: "s3://bucket/warehouse/db_1/table_10".to_string(),
            state: ManagedEraseJobState::Pending,
            retry_at_ms: None,
            updated_at_ms: 0,
            last_error: None,
        }],
    };

    store.replace_managed_snapshot(&snapshot).expect("persist");
    let loaded = store.load_snapshot().expect("load");
    assert_eq!(loaded.managed, snapshot);
}
```

在 `lake_recovery.rs` 增加路径测试：

```rust
#[test]
fn managed_lake_config_uses_partition_scoped_root() {
    let config = ManagedLakeConfig {
        warehouse_uri: "s3://bucket/warehouse".to_string(),
        s3: S3StoreConfig {
            endpoint: "http://127.0.0.1:9000".to_string(),
            bucket: "bucket".to_string(),
            root: "warehouse".to_string(),
            access_key_id: "ak".to_string(),
            access_key_secret: "sk".to_string(),
            region: Some("us-east-1".to_string()),
            enable_path_style_access: true,
        },
    };

    assert_eq!(
        config.tablet_root_path(1, 10, 20),
        "s3://bucket/warehouse/db_1/table_10/partition_20"
    );
}
```

- [ ] **Step 2: 运行测试并确认它们因缺少新状态/字段而失败**

Run:

```bash
cd /Users/harbor/project/NovaRocks
cargo test standalone_store_round_trips_lifecycle_states_and_erase_jobs --lib -- --nocapture
cargo test managed_lake_config_uses_partition_scoped_root --lib -- --nocapture
```

Expected: 编译失败，报 `Dropping` / `Retired` / `StoredManagedEraseJob` / 新的 `tablet_root_path` 签名不存在。

- [ ] **Step 3: 实现 lifecycle metadata 结构和新的 root layout**

在 `store.rs` 增加新类型并把 `ManagedSnapshot` 扩展到包含 `erase_jobs`：

```rust
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct ManagedSnapshot {
    pub global: ManagedGlobalMeta,
    pub databases: Vec<StoredManagedDatabase>,
    pub tables: Vec<StoredManagedTable>,
    pub schemas: Vec<StoredManagedSchema>,
    pub columns: Vec<StoredManagedColumn>,
    pub partitions: Vec<StoredManagedPartition>,
    pub indexes: Vec<StoredManagedIndex>,
    pub tablets: Vec<StoredManagedTablet>,
    pub txns: Vec<StoredManagedTxn>,
    pub erase_jobs: Vec<StoredManagedEraseJob>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ManagedEraseJobKind {
    DropTable,
    DropPartition,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ManagedEraseJobState {
    Pending,
    Running,
    Failed,
    Finished,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct StoredManagedEraseJob {
    pub job_id: i64,
    pub job_kind: ManagedEraseJobKind,
    pub table_id: i64,
    pub partition_id: Option<i64>,
    pub root_path: String,
    pub state: ManagedEraseJobState,
    pub retry_at_ms: Option<i64>,
    pub updated_at_ms: i64,
    pub last_error: Option<String>,
}
```

把状态枚举扩展为：

```rust
pub(crate) enum ManagedTableState {
    Creating,
    Active,
    Dropping,
    Failed,
}

pub(crate) enum ManagedPartitionState {
    Creating,
    Active,
    Retired,
    Failed,
}

pub(crate) enum ManagedIndexState {
    Creating,
    Active,
    Retired,
    Failed,
}
```

在 `lake_recovery.rs` 把 root helper 改成 partition-scoped：

```rust
pub(crate) fn tablet_root_path(&self, db_id: i64, table_id: i64, partition_id: i64) -> String {
    format!(
        "{}/db_{db_id}/table_{table_id}/partition_{partition_id}",
        self.warehouse_uri
    )
}
```

- [ ] **Step 4: 更新 SQLite schema 到 lifecycle 版本并加 fail-fast version gate**

把 `init_schema()` 调整为新的 schema 版本，新增 `erase_jobs` 表并在打开旧库时直接报错：

```rust
fn init_schema(&self) -> Result<(), String> {
    let conn = self.connection()?;
    let current_version: i64 = conn
        .query_row("PRAGMA user_version", [], |row| row.get(0))
        .map_err(|e| format!("read user_version failed: {e}"))?;
    if current_version != 0 && current_version != 3 {
        return Err(format!(
            "unsupported standalone metadata schema version {current_version}; delete the metadata db and reopen"
        ));
    }
    conn.execute_batch(
        "
        PRAGMA journal_mode = WAL;
        PRAGMA foreign_keys = ON;
        PRAGMA synchronous = NORMAL;
        CREATE TABLE IF NOT EXISTS erase_jobs (
            job_id INTEGER PRIMARY KEY,
            job_kind TEXT NOT NULL,
            table_id INTEGER NOT NULL,
            partition_id INTEGER,
            root_path TEXT NOT NULL,
            state TEXT NOT NULL,
            retry_at_ms INTEGER,
            updated_at_ms INTEGER NOT NULL,
            last_error TEXT
        );
        PRAGMA user_version = 3;
        "
    )?;
    Ok(())
}
```

同时把 `replace_managed_snapshot()` / `load_managed_snapshot()` 扩展到读写 `erase_jobs`。

- [ ] **Step 5: 重新运行 targeted unit tests**

Run:

```bash
cd /Users/harbor/project/NovaRocks
cargo test standalone_store_round_trips_lifecycle_states_and_erase_jobs --lib -- --nocapture
cargo test managed_lake_config_uses_partition_scoped_root --lib -- --nocapture
```

Expected: PASS

- [ ] **Step 6: Commit**

```bash
cd /Users/harbor/project/NovaRocks
git add src/standalone/store.rs src/standalone/lake_recovery.rs
git commit -m "feat: add managed lake lifecycle metadata model"
```

### Task 2: Add Store APIs for Drop, Truncate, and Erase Jobs

**Files:**
- Modify: `/Users/harbor/project/NovaRocks/src/standalone/store.rs`
- Test: `/Users/harbor/project/NovaRocks/src/standalone/store.rs`

- [ ] **Step 1: 写出 store transaction API 的失败测试**

在 `store.rs` 增加三个事务级测试：

```rust
#[test]
fn drop_managed_table_rejects_inflight_txns() {
    let mut snapshot = snapshot_seed();
    snapshot.txns.push(StoredManagedTxn {
        txn_id: 99,
        table_id: 10,
        partition_id: 20,
        base_version: 1,
        commit_version: 2,
        state: ManagedTxnState::Prepared,
        retry_at_ms: None,
        updated_at_ms: 0,
    });
    let (_dir, store) = test_store_with_snapshot(&snapshot);

    let err = store
        .drop_managed_table(10, "s3://bucket/warehouse/db_1/table_10")
        .expect_err("drop should reject inflight txn");
    assert!(err.contains("inflight managed txns"), "err={err}");
}

#[test]
fn drop_managed_table_marks_metadata_and_enqueues_drop_job() {
    let (_dir, store) = test_store_with_snapshot(&snapshot_seed());

    store
        .drop_managed_table(10, "s3://bucket/warehouse/db_1/table_10")
        .expect("drop managed table");

    let loaded = store.load_snapshot().expect("load snapshot");
    assert_eq!(loaded.managed.tables[0].state, ManagedTableState::Dropping);
    assert_eq!(loaded.managed.partitions[0].state, ManagedPartitionState::Retired);
    assert_eq!(loaded.managed.erase_jobs.len(), 1);
    assert_eq!(loaded.managed.erase_jobs[0].job_kind, ManagedEraseJobKind::DropTable);
}

#[test]
fn activate_truncate_partition_switches_active_partition_and_enqueues_erase() {
    let (_dir, store) = test_store_with_snapshot(&snapshot_seed());
    let staged = store
        .stage_truncate_partition(StageManagedTruncateRequest {
            table_id: 10,
            db_id: 1,
            bucket_num: 2,
            partition_name: "p0".to_string(),
            warehouse_uri: "s3://bucket/warehouse".to_string(),
        })
        .expect("stage truncate");

    store
        .activate_truncate_partition(10, 20, 21, staged.index_id, "s3://bucket/warehouse/db_1/table_10/partition_20")
        .expect("activate truncate");

    let loaded = store.load_snapshot().expect("load snapshot");
    assert!(loaded.managed.partitions.iter().any(|p| p.partition_id == 21 && p.state == ManagedPartitionState::Active));
    assert!(loaded.managed.partitions.iter().any(|p| p.partition_id == 20 && p.state == ManagedPartitionState::Retired));
    assert!(loaded.managed.erase_jobs.iter().any(|job| job.job_kind == ManagedEraseJobKind::DropPartition));
}
```

- [ ] **Step 2: 运行这些测试，确认缺少 store API**

Run:

```bash
cd /Users/harbor/project/NovaRocks
cargo test drop_managed_table_rejects_inflight_txns --lib -- --nocapture
cargo test drop_managed_table_marks_metadata_and_enqueues_drop_job --lib -- --nocapture
cargo test activate_truncate_partition_switches_active_partition_and_enqueues_erase --lib -- --nocapture
```

Expected: 编译失败，提示 `drop_managed_table` / `stage_truncate_partition` / `activate_truncate_partition` 未定义。

- [ ] **Step 3: 实现 drop/truncate store transaction helpers**

在 `store.rs` 增加 request/result types 和事务 API：

```rust
pub(crate) struct StageManagedTruncateRequest {
    pub table_id: i64,
    pub db_id: i64,
    pub bucket_num: i64,
    pub partition_name: String,
    pub warehouse_uri: String,
}

pub(crate) struct StagedManagedTruncate {
    pub partition_id: i64,
    pub index_id: i64,
    pub tablet_ids: Vec<i64>,
}

pub(crate) fn drop_managed_table(&self, table_id: i64, root_path: &str) -> Result<(), String> {
    let conn = self.connection()?;
    let tx = conn.unchecked_transaction().map_err(|e| format!("begin drop_managed_table failed: {e}"))?;
    let inflight_txn_count: i64 = tx.query_row(
        "SELECT COUNT(*) FROM txns WHERE table_id = ?1 AND state IN ('PREPARED', 'WRITTEN')",
        params![table_id],
        |row| row.get(0),
    )?;
    if inflight_txn_count > 0 {
        return Err(format!("cannot drop managed table {table_id}: inflight managed txns exist"));
    }
    let next_job_id: i64 = tx.query_row(
        "SELECT COALESCE(MAX(job_id), 0) + 1 FROM erase_jobs",
        [],
        |row| row.get(0),
    )?;
    tx.execute("UPDATE tables SET state = 'DROPPING' WHERE table_id = ?1", params![table_id])?;
    tx.execute("UPDATE partitions SET state = 'RETIRED' WHERE table_id = ?1 AND state = 'ACTIVE'", params![table_id])?;
    tx.execute("UPDATE indexes SET state = 'RETIRED' WHERE table_id = ?1 AND state = 'ACTIVE'", params![table_id])?;
    tx.execute(
        "INSERT INTO erase_jobs(job_id, job_kind, table_id, partition_id, root_path, state, retry_at_ms, updated_at_ms, last_error)
         VALUES (?1, 'DROP_TABLE', ?2, NULL, ?3, 'PENDING', NULL, strftime('%s','now') * 1000, NULL)",
        params![next_job_id, table_id, root_path],
    )?;
    tx.commit().map_err(|e| format!("commit drop_managed_table failed: {e}"))?;
    Ok(())
}
```

把 truncate 切成两个事务接口：

```rust
pub(crate) fn stage_truncate_partition(
    &self,
    req: StageManagedTruncateRequest,
) -> Result<StagedManagedTruncate, String> {
    let conn = self.connection()?;
    let tx = conn
        .unchecked_transaction()
        .map_err(|e| format!("begin stage_truncate_partition failed: {e}"))?;
    let inflight_txn_count: i64 = tx.query_row(
        "SELECT COUNT(*) FROM txns WHERE table_id = ?1 AND state IN ('PREPARED', 'WRITTEN')",
        params![req.table_id],
        |row| row.get(0),
    )?;
    if inflight_txn_count > 0 {
        return Err(format!(
            "cannot truncate managed table {} while inflight managed txns exist",
            req.table_id
        ));
    }
    let partition_id: i64 = tx.query_row(
        "SELECT next_partition_id FROM global_meta WHERE singleton = 1",
        [],
        |row| row.get(0),
    )?;
    let index_id: i64 = tx.query_row(
        "SELECT next_index_id FROM global_meta WHERE singleton = 1",
        [],
        |row| row.get(0),
    )?;
    let first_tablet_id: i64 = tx.query_row(
        "SELECT next_tablet_id FROM global_meta WHERE singleton = 1",
        [],
        |row| row.get(0),
    )?;
    tx.execute(
        "UPDATE global_meta
         SET next_partition_id = ?1, next_index_id = ?2, next_tablet_id = ?3
         WHERE singleton = 1",
        params![partition_id + 1, index_id + 1, first_tablet_id + req.bucket_num],
    )?;
    tx.execute(
        "INSERT INTO partitions(partition_id, table_id, name, visible_version, next_version, state)
         VALUES (?1, ?2, ?3, 1, 2, 'CREATING')",
        params![partition_id, req.table_id, req.partition_name],
    )?;
    tx.execute(
        "INSERT INTO indexes(index_id, table_id, partition_id, index_type, state)
         VALUES (?1, ?2, ?3, 'BASE', 'CREATING')",
        params![index_id, req.table_id, partition_id],
    )?;
    let partition_root_path = format!(
        "{}/db_{}/table_{}/partition_{}",
        req.warehouse_uri, req.db_id, req.table_id, partition_id
    );
    for bucket_seq in 0..req.bucket_num {
        let tablet_id = first_tablet_id + bucket_seq;
        tx.execute(
            "INSERT INTO tablets(tablet_id, partition_id, index_id, bucket_seq, tablet_root_path)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![tablet_id, partition_id, index_id, bucket_seq, partition_root_path],
        )?;
    }
    tx.commit()
        .map_err(|e| format!("commit stage_truncate_partition failed: {e}"))?;
    Ok(StagedManagedTruncate {
        partition_id,
        index_id,
        tablet_ids: (0..req.bucket_num).map(|offset| first_tablet_id + offset).collect(),
    })
}

pub(crate) fn activate_truncate_partition(
    &self,
    table_id: i64,
    old_partition_id: i64,
    new_partition_id: i64,
    new_index_id: i64,
    retired_root_path: &str,
) -> Result<(), String> {
    let conn = self.connection()?;
    let tx = conn
        .unchecked_transaction()
        .map_err(|e| format!("begin activate_truncate_partition failed: {e}"))?;
    let next_job_id: i64 = tx.query_row(
        "SELECT COALESCE(MAX(job_id), 0) + 1 FROM erase_jobs",
        [],
        |row| row.get(0),
    )?;
    tx.execute(
        "UPDATE partitions
         SET state = CASE
             WHEN partition_id = ?1 THEN 'ACTIVE'
             WHEN partition_id = ?2 THEN 'RETIRED'
             ELSE state
         END
         WHERE table_id = ?3",
        params![new_partition_id, old_partition_id, table_id],
    )?;
    tx.execute(
        "UPDATE indexes
         SET state = CASE
             WHEN index_id = ?1 THEN 'ACTIVE'
             WHEN partition_id = ?2 THEN 'RETIRED'
             ELSE state
         END
         WHERE table_id = ?3",
        params![new_index_id, old_partition_id, table_id],
    )?;
    tx.execute(
        "INSERT INTO erase_jobs(job_id, job_kind, table_id, partition_id, root_path, state, retry_at_ms, updated_at_ms, last_error)
         VALUES (?1, 'DROP_PARTITION', ?2, ?3, ?4, 'PENDING', NULL, strftime('%s','now') * 1000, NULL)",
        params![next_job_id, table_id, old_partition_id, retired_root_path],
    )?;
    tx.commit()
        .map_err(|e| format!("commit activate_truncate_partition failed: {e}"))?;
    Ok(())
}
```

- [ ] **Step 4: 实现 erase job queue helpers**

在 `store.rs` 增加 worker 所需接口：

```rust
pub(crate) fn list_runnable_erase_jobs(
    &self,
    now_ms: i64,
) -> Result<Vec<StoredManagedEraseJob>, String> {
    let conn = self.connection()?;
    let mut stmt = conn.prepare(
        "SELECT job_id, job_kind, table_id, partition_id, root_path, state, retry_at_ms, updated_at_ms, last_error
         FROM erase_jobs
         WHERE (state = 'PENDING' OR state = 'FAILED')
           AND (retry_at_ms IS NULL OR retry_at_ms <= ?1)
         ORDER BY job_id",
    )?;
    let rows = stmt.query_map(params![now_ms], |row| {
        Ok(StoredManagedEraseJob {
            job_id: row.get(0)?,
            job_kind: ManagedEraseJobKind::from_sql_str(row.get::<_, String>(1)?.as_str())
                .map_err(json_to_sql_error)?,
            table_id: row.get(2)?,
            partition_id: row.get(3)?,
            root_path: row.get(4)?,
            state: ManagedEraseJobState::from_sql_str(row.get::<_, String>(5)?.as_str())
                .map_err(json_to_sql_error)?,
            retry_at_ms: row.get(6)?,
            updated_at_ms: row.get(7)?,
            last_error: row.get(8)?,
        })
    })?;
    rows.collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("read erase jobs failed: {e}"))
}

pub(crate) fn claim_erase_job(&self, job_id: i64) -> Result<bool, String> {
    let changed = self.connection()?.execute(
        "UPDATE erase_jobs
         SET state = 'RUNNING', updated_at_ms = strftime('%s','now') * 1000
         WHERE job_id = ?1 AND state IN ('PENDING', 'FAILED')",
        params![job_id],
    )?;
    Ok(changed == 1)
}

pub(crate) fn finish_erase_job(&self, job_id: i64) -> Result<(), String> {
    self.connection()?.execute(
        "UPDATE erase_jobs
         SET state = 'FINISHED', updated_at_ms = strftime('%s','now') * 1000, retry_at_ms = NULL, last_error = NULL
         WHERE job_id = ?1",
        params![job_id],
    )?;
    Ok(())
}

pub(crate) fn fail_erase_job(
    &self,
    job_id: i64,
    last_error: &str,
    retry_at_ms: i64,
) -> Result<(), String> {
    self.connection()?.execute(
        "UPDATE erase_jobs
         SET state = 'FAILED', updated_at_ms = strftime('%s','now') * 1000, retry_at_ms = ?2, last_error = ?3
         WHERE job_id = ?1",
        params![job_id, retry_at_ms, last_error],
    )?;
    Ok(())
}

pub(crate) fn purge_retired_table_metadata(&self, table_id: i64) -> Result<(), String> {
    let conn = self.connection()?;
    let tx = conn.unchecked_transaction()?;
    tx.execute("DELETE FROM tablets WHERE partition_id IN (SELECT partition_id FROM partitions WHERE table_id = ?1)", params![table_id])?;
    tx.execute("DELETE FROM indexes WHERE table_id = ?1 AND state = 'RETIRED'", params![table_id])?;
    tx.execute("DELETE FROM partitions WHERE table_id = ?1 AND state = 'RETIRED'", params![table_id])?;
    tx.execute("DELETE FROM tables WHERE table_id = ?1 AND state = 'DROPPING'", params![table_id])?;
    tx.commit().map_err(|e| format!("commit purge_retired_table_metadata failed: {e}"))?;
    Ok(())
}

pub(crate) fn purge_retired_partition_metadata(&self, partition_id: i64) -> Result<(), String> {
    let conn = self.connection()?;
    let tx = conn.unchecked_transaction()?;
    tx.execute("DELETE FROM tablets WHERE partition_id = ?1", params![partition_id])?;
    tx.execute("DELETE FROM indexes WHERE partition_id = ?1 AND state = 'RETIRED'", params![partition_id])?;
    tx.execute("DELETE FROM partitions WHERE partition_id = ?1 AND state = 'RETIRED'", params![partition_id])?;
    tx.commit().map_err(|e| format!("commit purge_retired_partition_metadata failed: {e}"))?;
    Ok(())
}

pub(crate) fn delete_creating_partition(&self, partition_id: i64) -> Result<(), String> {
    let conn = self.connection()?;
    let tx = conn.unchecked_transaction()?;
    tx.execute("DELETE FROM tablets WHERE partition_id = ?1", params![partition_id])?;
    tx.execute("DELETE FROM indexes WHERE partition_id = ?1 AND state = 'CREATING'", params![partition_id])?;
    tx.execute("DELETE FROM partitions WHERE partition_id = ?1 AND state = 'CREATING'", params![partition_id])?;
    tx.commit().map_err(|e| format!("commit delete_creating_partition failed: {e}"))?;
    Ok(())
}
```

- [ ] **Step 5: 重新运行 store unit tests**

Run:

```bash
cd /Users/harbor/project/NovaRocks
cargo test drop_managed_table_rejects_inflight_txns --lib -- --nocapture
cargo test drop_managed_table_marks_metadata_and_enqueues_drop_job --lib -- --nocapture
cargo test activate_truncate_partition_switches_active_partition_and_enqueues_erase --lib -- --nocapture
cargo test mark_txn_written_and_visible_advances_partition_version --lib -- --nocapture
```

Expected: PASS

- [ ] **Step 6: Commit**

```bash
cd /Users/harbor/project/NovaRocks
git add src/standalone/store.rs
git commit -m "feat: add managed lake lifecycle store transactions"
```

### Task 3: Keep Only Active Metadata Visible During Rebuild and Recovery

**Files:**
- Modify: `/Users/harbor/project/NovaRocks/src/standalone/lake_recovery.rs`
- Modify: `/Users/harbor/project/NovaRocks/src/standalone/store.rs`
- Test: `/Users/harbor/project/NovaRocks/src/standalone/lake_recovery.rs`

- [ ] **Step 1: 写出 recovery/catalog 的失败测试**

在 `lake_recovery.rs` 增加两个测试：

```rust
#[test]
fn rebuild_ignores_dropping_tables_and_retired_partitions() {
    let mut snapshot = snapshot_seed();
    snapshot.tables[0].state = ManagedTableState::Dropping;
    snapshot.partitions[0].state = ManagedPartitionState::Retired;

    let rebuilt = ManagedLakeCatalog::rebuild(
        Some(ManagedLakeConfig {
            warehouse_uri: "s3://test/warehouse".to_string(),
            s3: S3StoreConfig {
                endpoint: "http://127.0.0.1:9000".to_string(),
                bucket: "test".to_string(),
                root: "warehouse".to_string(),
                access_key_id: "ak".to_string(),
                access_key_secret: "sk".to_string(),
                region: Some("us-east-1".to_string()),
                enable_path_style_access: true,
            },
        }),
        snapshot,
    )
    .expect("rebuild");
    assert!(!rebuilt.contains_table("analytics", "orders").expect("contains table"));
}

#[test]
fn reconcile_on_open_drops_incomplete_creating_partition_rows() {
    let mut snapshot = snapshot_seed();
    snapshot.partitions.push(StoredManagedPartition {
        partition_id: 21,
        table_id: 10,
        name: "p0".to_string(),
        visible_version: 1,
        next_version: 2,
        state: ManagedPartitionState::Creating,
    });
    snapshot.indexes.push(StoredManagedIndex {
        index_id: 31,
        table_id: 10,
        partition_id: 21,
        index_type: "BASE".to_string(),
        state: ManagedIndexState::Creating,
    });

    let (_dir, store) = test_store_with_snapshot(&snapshot);
    reconcile_on_open(&store, &mut snapshot, |_, _| Ok(())).expect("reconcile");

    assert!(!snapshot.partitions.iter().any(|partition| partition.partition_id == 21));
    assert!(!snapshot.indexes.iter().any(|index| index.partition_id == 21));
}
```

- [ ] **Step 2: 运行 targeted tests，确认当前 rebuild/reconcile 语义不够**

Run:

```bash
cd /Users/harbor/project/NovaRocks
cargo test rebuild_ignores_dropping_tables_and_retired_partitions --lib -- --nocapture
cargo test reconcile_on_open_drops_incomplete_creating_partition_rows --lib -- --nocapture
```

Expected: FAIL，现有 rebuild 仍会把这些对象放进 runtime，reconcile 也不会清理 `Creating` partition。

- [ ] **Step 3: 实现 active-only rebuild 和 staging cleanup**

在 `ManagedLakeCatalog::rebuild()` 里只装配 active lifecycle 对象：

```rust
let tables = snapshot
    .tables
    .iter()
    .filter(|table| table.state == ManagedTableState::Active)
    .cloned()
    .collect::<Vec<_>>();
```

对 partition/index/tablet 只保留 active 链：

```rust
let active_partition_ids = runtime
    .partitions
    .iter()
    .filter(|partition| partition.state == ManagedPartitionState::Active)
    .map(|partition| partition.partition_id)
    .collect::<HashSet<_>>();
```

在 `reconcile_on_open()` 中加入 staging cleanup：

```rust
let dangling_partition_ids = snapshot
    .partitions
    .iter()
    .filter(|partition| partition.state == ManagedPartitionState::Creating)
    .map(|partition| partition.partition_id)
    .collect::<Vec<_>>();

for partition_id in &dangling_partition_ids {
    store.delete_creating_partition(*partition_id)?;
}

snapshot.partitions.retain(|partition| !dangling_partition_ids.contains(&partition.partition_id));
snapshot.indexes.retain(|index| !dangling_partition_ids.contains(&index.partition_id));
snapshot.tablets.retain(|tablet| !dangling_partition_ids.contains(&tablet.partition_id));
```

- [ ] **Step 4: 重新运行 targeted unit tests**

Run:

```bash
cd /Users/harbor/project/NovaRocks
cargo test rebuild_ignores_dropping_tables_and_retired_partitions --lib -- --nocapture
cargo test reconcile_on_open_drops_incomplete_creating_partition_rows --lib -- --nocapture
cargo test reconcile_on_open_replays_written_txns_and_advances_partition --lib -- --nocapture
```

Expected: PASS

- [ ] **Step 5: Commit**

```bash
cd /Users/harbor/project/NovaRocks
git add src/standalone/lake_recovery.rs src/standalone/store.rs
git commit -m "feat: hide retired managed lake metadata on recovery"
```

### Task 4: Add the Background Erase Worker

**Files:**
- Create: `/Users/harbor/project/NovaRocks/src/standalone/lake_erase.rs`
- Modify: `/Users/harbor/project/NovaRocks/src/standalone/mod.rs`
- Modify: `/Users/harbor/project/NovaRocks/src/standalone/engine.rs`
- Modify: `/Users/harbor/project/NovaRocks/tests/standalone_managed_lake.rs`

- [ ] **Step 1: 写出 worker 的失败 integration test**

在 `tests/standalone_managed_lake.rs` 增加：

```rust
#[test]
fn erase_worker_finishes_drop_partition_job_and_purges_metadata() {
    let _guard = managed_lake_test_lock();
    let Some(harness) = ManagedLakeTestHarness::maybe_new("erase_worker_finishes_drop_partition_job_and_purges_metadata").expect("harness") else {
        eprintln!("skipping managed lake object-store test: AWS_S3_ENDPOINT is not set");
        return;
    };
    let engine = StandaloneNovaRocks::open(StandaloneOptions {
        config_path: Some(harness.config_path.clone()),
        metadata_db_path: None,
    }).expect("open engine");
    let session = engine.session();
    session
        .execute("create table orders (k1 int, v1 string) duplicate key(k1) distributed by hash(k1) buckets 2")
        .expect("create table");
    session
        .execute("insert into orders values (1, 'a'), (2, 'b')")
        .expect("insert");
    session.execute("truncate table orders").expect("truncate");
    drop(engine);

    let reopened = StandaloneNovaRocks::open(StandaloneOptions {
        config_path: Some(harness.config_path.clone()),
        metadata_db_path: None,
    }).expect("reopen engine");
    std::thread::sleep(std::time::Duration::from_secs(3));
    drop(reopened);

    let conn = Connection::open(&harness.metadata_db_path).expect("sqlite");
    let finished_jobs: i64 = conn
        .query_row("SELECT COUNT(*) FROM erase_jobs WHERE job_kind = 'DROP_PARTITION' AND state = 'FINISHED'", [], |row| row.get(0))
        .expect("finished jobs");
    assert_eq!(finished_jobs, 1);
}
```

- [ ] **Step 2: 运行 test，确认 worker 相关实现尚不存在**

Run:

```bash
cd /Users/harbor/project/NovaRocks
cargo test --test standalone_managed_lake erase_worker_finishes_drop_partition_job_and_purges_metadata -- --nocapture
```

Expected: FAIL，因为 truncate lifecycle 和 erase worker 还没落地。

- [ ] **Step 3: 实现最小 erase worker**

在 `lake_erase.rs` 新建执行入口：

```rust
pub(crate) fn run_erase_jobs_once(
    store: &SqliteMetadataStore,
    config: &ManagedLakeConfig,
) -> Result<(), String> {
    let now_ms = current_time_ms();
    for job in store.list_runnable_erase_jobs(now_ms)? {
        if !store.claim_erase_job(job.job_id)? {
            continue;
        }
        match erase_root(&job.root_path, config) {
            Ok(()) => {
                match job.job_kind {
                    ManagedEraseJobKind::DropTable => store.purge_retired_table_metadata(job.table_id)?,
                    ManagedEraseJobKind::DropPartition => {
                        let partition_id = job.partition_id.ok_or_else(|| "drop partition erase job missing partition_id".to_string())?;
                        store.purge_retired_partition_metadata(partition_id)?;
                    }
                }
                store.finish_erase_job(job.job_id)?;
            }
            Err(err) => {
                store.fail_erase_job(job.job_id, &err, now_ms + 5_000)?;
            }
        }
    }
    Ok(())
}
```

用 OpenDAL 删除 prefix 下的对象：

```rust
fn erase_root(root_path: &str, config: &ManagedLakeConfig) -> Result<(), String> {
    let object_store_config = ObjectStoreConfig {
        endpoint: config.s3.endpoint.clone(),
        bucket: config.s3.bucket.clone(),
        root: config.s3.root.clone(),
        access_key_id: config.s3.access_key_id.clone(),
        access_key_secret: config.s3.access_key_secret.clone(),
        session_token: None,
        enable_path_style_access: Some(config.s3.enable_path_style_access),
        region: config.s3.region.clone(),
        retry_max_times: Some(2),
        retry_min_delay_ms: Some(50),
        retry_max_delay_ms: Some(200),
        timeout_ms: Some(10_000),
        io_timeout_ms: Some(10_000),
    };
    let operator = build_oss_operator(&object_store_config)
        .map_err(|e| format!("build object store operator failed: {e}"))?;
    let prefix = root_path
        .strip_prefix(&format!("s3://{}/", config.s3.bucket))
        .ok_or_else(|| format!("erase root is outside configured bucket: {root_path}"))?
        .trim_matches('/')
        .to_string();
    let mut entries = data_block_on(async {
        operator
            .lister_with(&prefix)
            .recursive(true)
            .await
            .map_err(|e| format!("list erase prefix `{prefix}` failed: {e}"))
    })?;
    let mut paths = Vec::new();
    while let Some(entry) = data_block_on(async {
        entries
            .try_next()
            .await
            .map_err(|e| format!("read erase listing `{prefix}` failed: {e}"))
    })? {
        if entry.metadata().mode() == EntryMode::FILE {
            paths.push(entry.path().to_string());
        }
    }
    for path in paths.into_iter().rev() {
        data_block_on(async {
            operator
                .delete(&path)
                .await
                .map_err(|e| format!("delete object `{path}` failed: {e}"))
        })?;
    }
    Ok(())
}
```

- [ ] **Step 4: 在 open 路径挂上后台线程**

在 `mod.rs` 注册模块：

```rust
pub(crate) mod lake_erase;
```

在 `engine.rs` 的 `StandaloneNovaRocks::open()` 末尾启动轮询线程：

```rust
if inner.managed_lake_config.is_some() && inner.metadata_store.is_some() {
    super::lake_erase::spawn_erase_worker(Arc::clone(&inner));
}
```

线程逻辑保持简单：

```rust
pub(crate) fn spawn_erase_worker(state: Arc<StandaloneState>) {
    std::thread::spawn(move || loop {
        let Some(store) = state.metadata_store.as_ref() else {
            return;
        };
        let Some(config) = state.managed_lake_config.as_ref() else {
            return;
        };
        let _ = run_erase_jobs_once(store, config);
        std::thread::sleep(std::time::Duration::from_secs(2));
    });
}
```

- [ ] **Step 5: 重新运行 worker integration test**

Run:

```bash
cd /Users/harbor/project/NovaRocks
cargo test --test standalone_managed_lake erase_worker_finishes_drop_partition_job_and_purges_metadata -- --nocapture
```

Expected: PASS

- [ ] **Step 6: Commit**

```bash
cd /Users/harbor/project/NovaRocks
git add src/standalone/lake_erase.rs src/standalone/mod.rs src/standalone/engine.rs tests/standalone_managed_lake.rs
git commit -m "feat: add managed lake erase worker"
```

### Task 5: Implement Managed `DROP TABLE`

**Files:**
- Modify: `/Users/harbor/project/NovaRocks/src/standalone/lake_ddl.rs`
- Modify: `/Users/harbor/project/NovaRocks/src/standalone/engine.rs`
- Modify: `/Users/harbor/project/NovaRocks/tests/standalone_managed_lake.rs`

- [ ] **Step 1: 先写 integration test**

在 `tests/standalone_managed_lake.rs` 增加：

```rust
#[test]
fn managed_lake_drop_table_hides_table_and_schedules_erase() {
    let _guard = managed_lake_test_lock();
    let Some(harness) = ManagedLakeTestHarness::maybe_new("managed_lake_drop_table_hides_table_and_schedules_erase").expect("harness") else {
        eprintln!("skipping managed lake object-store test: AWS_S3_ENDPOINT is not set");
        return;
    };
    let engine = StandaloneNovaRocks::open(StandaloneOptions {
        config_path: Some(harness.config_path.clone()),
        metadata_db_path: None,
    }).expect("open engine");
    let session = engine.session();

    session
        .execute("create table orders (k1 int, v1 string) duplicate key(k1) distributed by hash(k1) buckets 2")
        .expect("create table");
    session
        .execute("insert into orders values (1, 'a'), (2, 'b')")
        .expect("insert");

    session.execute("drop table orders").expect("drop table");

    let err = session.query("select * from orders").expect_err("query after drop should fail");
    assert!(err.contains("unknown table"), "err={err}");

    let conn = Connection::open(&harness.metadata_db_path).expect("sqlite");
    let table_state: String = conn
        .query_row("SELECT state FROM tables WHERE name = 'orders'", [], |row| row.get(0))
        .expect("table state");
    assert_eq!(table_state, "DROPPING");
    let erase_jobs: i64 = conn
        .query_row("SELECT COUNT(*) FROM erase_jobs WHERE table_id = (SELECT table_id FROM tables WHERE name = 'orders')", [], |row| row.get(0))
        .expect("erase jobs");
    assert_eq!(erase_jobs, 1);
}
```

- [ ] **Step 2: 运行 integration test，确认当前路径仍然报 unsupported**

Run:

```bash
cd /Users/harbor/project/NovaRocks
cargo test --test standalone_managed_lake managed_lake_drop_table_hides_table_and_schedules_erase -- --nocapture
```

Expected: FAIL，错误里包含 `DROP TABLE is not supported for managed standalone lake tables yet`。

- [ ] **Step 3: 在 `lake_ddl.rs` 实现 managed drop helper**

新增 lifecycle 入口：

```rust
pub(crate) fn drop_managed_table(
    state: &Arc<StandaloneState>,
    database_name: &str,
    table_name: &str,
) -> Result<StatementResult, String> {
    let metadata_store = state
        .metadata_store
        .as_ref()
        .ok_or_else(|| "managed lake drop requires sqlite metadata store".to_string())?;

    let mut managed = state
        .managed_lake
        .write()
        .expect("standalone managed lake write lock");
    let runtime = managed.table(database_name, table_name)?.clone();
    let table_root = runtime
        .tablets
        .first()
        .map(|tablet| {
            let trimmed = tablet.tablet_root_path.trim_end_matches('/');
            let (_, tail) = trimmed.rsplit_once("/partition_").unwrap_or((trimmed, ""));
            if tail.is_empty() {
                trimmed.to_string()
            } else {
                trimmed.rsplit_once("/partition_").map(|(head, _)| head.to_string()).unwrap_or_else(|| trimmed.to_string())
            }
        })
        .ok_or_else(|| format!("managed table {database_name}.{table_name} has no tablets"))?;

    metadata_store.drop_managed_table(runtime.table.table_id, &table_root)?;
    let snapshot = metadata_store.load_snapshot()?.managed;
    let rebuilt = ManagedLakeCatalog::rebuild(state.managed_lake_config.clone(), snapshot)?;
    *managed = rebuilt;
    drop(managed);

    let mut catalog = state.catalog.write().expect("standalone catalog write lock");
    let _ = catalog.drop_table(database_name, table_name);
    Ok(StatementResult::Ok)
}
```

- [ ] **Step 4: 在 `engine.rs` 接线 managed drop**

把 managed 分支从报错改为调用新 helper：

```rust
if state
    .managed_lake
    .read()
    .expect("standalone managed lake read lock")
    .contains_table(&resolved.database, &resolved.table)?
{
    return super::lake_ddl::drop_managed_table(state, &resolved.database, &resolved.table);
}
```

- [ ] **Step 5: 重新运行 drop integration test**

Run:

```bash
cd /Users/harbor/project/NovaRocks
cargo test --test standalone_managed_lake managed_lake_drop_table_hides_table_and_schedules_erase -- --nocapture
```

Expected: PASS

- [ ] **Step 6: Commit**

```bash
cd /Users/harbor/project/NovaRocks
git add src/standalone/lake_ddl.rs src/standalone/engine.rs tests/standalone_managed_lake.rs
git commit -m "feat: support managed lake drop table"
```

### Task 6: Implement Managed `TRUNCATE TABLE` with Partition Replacement

**Files:**
- Modify: `/Users/harbor/project/NovaRocks/src/standalone/lake_ddl.rs`
- Modify: `/Users/harbor/project/NovaRocks/src/standalone/engine.rs`
- Modify: `/Users/harbor/project/NovaRocks/src/standalone/lake_recovery.rs`
- Modify: `/Users/harbor/project/NovaRocks/tests/standalone_managed_lake.rs`

- [ ] **Step 1: 写出 truncate integration test**

在 `tests/standalone_managed_lake.rs` 增加：

```rust
#[test]
fn managed_lake_truncate_replaces_partition_and_clears_visible_rows() {
    let _guard = managed_lake_test_lock();
    let Some(harness) = ManagedLakeTestHarness::maybe_new("managed_lake_truncate_replaces_partition_and_clears_visible_rows").expect("harness") else {
        eprintln!("skipping managed lake object-store test: AWS_S3_ENDPOINT is not set");
        return;
    };
    let engine = StandaloneNovaRocks::open(StandaloneOptions {
        config_path: Some(harness.config_path.clone()),
        metadata_db_path: None,
    }).expect("open engine");
    let session = engine.session();

    session
        .execute("create table orders (k1 int, v1 string) duplicate key(k1) distributed by hash(k1) buckets 2")
        .expect("create table");
    session
        .execute("insert into orders values (1, 'a'), (2, 'b')")
        .expect("insert");

    let before = engine.managed_table_info("default", "orders").expect("table info before truncate");
    session.execute("truncate table orders").expect("truncate table");
    let after = engine.managed_table_info("default", "orders").expect("table info after truncate");

    let result = session.query("select * from orders").expect("query after truncate");
    assert_eq!(result.row_count(), 0);
    assert_ne!(before.tablets[0].tablet_root_path, after.tablets[0].tablet_root_path);

    let conn = Connection::open(&harness.metadata_db_path).expect("sqlite");
    let active_partition_count: i64 = conn
        .query_row("SELECT COUNT(*) FROM partitions WHERE table_id = (SELECT table_id FROM tables WHERE name = 'orders') AND state = 'ACTIVE'", [], |row| row.get(0))
        .expect("active partition count");
    let retired_partition_count: i64 = conn
        .query_row("SELECT COUNT(*) FROM partitions WHERE table_id = (SELECT table_id FROM tables WHERE name = 'orders') AND state = 'RETIRED'", [], |row| row.get(0))
        .expect("retired partition count");
    assert_eq!(active_partition_count, 1);
    assert_eq!(retired_partition_count, 1);
}
```

- [ ] **Step 2: 运行 integration test，确认当前 managed truncate 仍然报 unsupported**

Run:

```bash
cd /Users/harbor/project/NovaRocks
cargo test --test standalone_managed_lake managed_lake_truncate_replaces_partition_and_clears_visible_rows -- --nocapture
```

Expected: FAIL，错误里包含 `TRUNCATE TABLE is not supported for managed standalone lake tables yet`。

- [ ] **Step 3: 在 `lake_ddl.rs` 实现 truncate 的 stage/bootstrap/activate**

增加 managed truncate helper：

```rust
pub(crate) fn truncate_managed_table(
    state: &Arc<StandaloneState>,
    database_name: &str,
    table_name: &str,
) -> Result<StatementResult, String> {
    let metadata_store = state
        .metadata_store
        .as_ref()
        .ok_or_else(|| "managed lake truncate requires sqlite metadata store".to_string())?;
    let managed_config = state
        .managed_lake_config
        .clone()
        .ok_or_else(|| "standalone managed lake config is missing".to_string())?;

    let runtime = state
        .managed_lake
        .read()
        .expect("standalone managed lake read lock")
        .table(database_name, table_name)?
        .clone();
    let active_partition = runtime
        .partitions
        .iter()
        .find(|partition| partition.state == ManagedPartitionState::Active)
        .cloned()
        .ok_or_else(|| format!("managed table {database_name}.{table_name} has no active partition"))?;

    let staged = metadata_store.stage_truncate_partition(StageManagedTruncateRequest {
        table_id: runtime.table.table_id,
        db_id: runtime.table.db_id,
        bucket_num: runtime.table.bucket_num,
        partition_name: active_partition.name.clone(),
        warehouse_uri: managed_config.warehouse_uri.clone(),
    })?;

    bootstrap_empty_partition(&runtime, &staged, &managed_config)?;
    let retired_root_path = runtime
        .tablets
        .iter()
        .find(|tablet| tablet.partition_id == active_partition.partition_id)
        .map(|tablet| tablet.tablet_root_path.clone())
        .ok_or_else(|| format!("managed table {database_name}.{table_name} is missing active partition root"))?;
    metadata_store.activate_truncate_partition(
        runtime.table.table_id,
        active_partition.partition_id,
        staged.partition_id,
        staged.index_id,
        &retired_root_path,
    )?;
    let snapshot = metadata_store.load_snapshot()?.managed;
    let rebuilt = ManagedLakeCatalog::rebuild(state.managed_lake_config.clone(), snapshot)?;
    {
        let mut managed = state
            .managed_lake
            .write()
            .expect("standalone managed lake write lock");
        *managed = rebuilt.clone();
    }
    {
        let mut catalog = state.catalog.write().expect("standalone catalog write lock");
        catalog.drop_table(database_name, table_name)?;
        register_managed_table_in_catalog(
            &mut catalog,
            rebuilt.table(database_name, table_name)?,
        )?;
    }
    Ok(StatementResult::Ok)
}
```

把 bootstrap helper 复用到 `create_lake_tablet_from_req`：

```rust
fn bootstrap_empty_partition(
    runtime: &ManagedTableRuntime,
    staged: &StagedManagedTruncate,
    managed_config: &ManagedLakeConfig,
) -> Result<(), String> {
    for tablet_id in &staged.tablet_ids {
        let req = crate::agent_service::TCreateTabletReq {
            tablet_id: *tablet_id,
            table_id: Some(runtime.table.table_id),
            partition_id: Some(staged.partition_id),
            tablet_schema: runtime.tablet_schema.clone(),
            ..Default::default()
        };
        create_lake_tablet_from_req(
            &req,
            &managed_config.tablet_root_path(runtime.table.db_id, runtime.table.table_id, staged.partition_id),
            Some(managed_config.s3.clone()),
        )?;
    }
    Ok(())
}
```

- [ ] **Step 4: 在 `engine.rs` 接线 managed truncate**

把 managed truncate 分支改为：

```rust
if state
    .managed_lake
    .read()
    .expect("standalone managed lake read lock")
    .contains_table(&resolved.database, &resolved.table)?
{
    return super::lake_ddl::truncate_managed_table(state, &resolved.database, &resolved.table);
}
```

- [ ] **Step 5: 重新运行 truncate integration test**

Run:

```bash
cd /Users/harbor/project/NovaRocks
cargo test --test standalone_managed_lake managed_lake_truncate_replaces_partition_and_clears_visible_rows -- --nocapture
```

Expected: PASS

- [ ] **Step 6: Commit**

```bash
cd /Users/harbor/project/NovaRocks
git add src/standalone/lake_ddl.rs src/standalone/engine.rs src/standalone/lake_recovery.rs tests/standalone_managed_lake.rs
git commit -m "feat: support managed lake truncate table"
```

### Task 7: Verify Restart Recovery and MySQL Surface for Lifecycle Paths

**Files:**
- Modify: `/Users/harbor/project/NovaRocks/tests/standalone_managed_lake.rs`
- Modify: `/Users/harbor/project/NovaRocks/tests/standalone_mysql_server.rs`

- [ ] **Step 1: 写出 restart recovery test for staged truncate / pending drop erase**

在 `tests/standalone_managed_lake.rs` 增加两个恢复测试：

```rust
#[test]
fn reopen_cleans_incomplete_truncate_stage_partition() {
    let _guard = managed_lake_test_lock();
    let Some(harness) = ManagedLakeTestHarness::maybe_new("reopen_cleans_incomplete_truncate_stage_partition").expect("harness") else {
        eprintln!("skipping managed lake object-store test: AWS_S3_ENDPOINT is not set");
        return;
    };
    let engine = StandaloneNovaRocks::open(StandaloneOptions {
        config_path: Some(harness.config_path.clone()),
        metadata_db_path: None,
    }).expect("open engine");
    engine.session()
        .execute("create table orders (k1 int, v1 string) duplicate key(k1) distributed by hash(k1) buckets 2")
        .expect("create table");
    drop(engine);

    let conn = Connection::open(&harness.metadata_db_path).expect("sqlite");
    conn.execute(
        "INSERT INTO partitions(partition_id, table_id, name, visible_version, next_version, state)
         VALUES (999, (SELECT table_id FROM tables WHERE name = 'orders'), 'p0', 1, 2, 'CREATING')",
        [],
    ).expect("insert creating partition");

    let reopened = StandaloneNovaRocks::open(StandaloneOptions {
        config_path: Some(harness.config_path.clone()),
        metadata_db_path: None,
    }).expect("reopen engine");
    let result = reopened.session().query("select * from orders").expect("query after reopen");
    assert_eq!(result.row_count(), 0);
}

#[test]
fn reopen_runs_pending_drop_erase_job() {
    let _guard = managed_lake_test_lock();
    let Some(harness) = ManagedLakeTestHarness::maybe_new("reopen_runs_pending_drop_erase_job").expect("harness") else {
        eprintln!("skipping managed lake object-store test: AWS_S3_ENDPOINT is not set");
        return;
    };
    let engine = StandaloneNovaRocks::open(StandaloneOptions {
        config_path: Some(harness.config_path.clone()),
        metadata_db_path: None,
    }).expect("open engine");
    let session = engine.session();
    session.execute("create table orders (k1 int, v1 string) duplicate key(k1) distributed by hash(k1) buckets 2").expect("create");
    session.execute("drop table orders").expect("drop");
    drop(engine);

    let reopened = StandaloneNovaRocks::open(StandaloneOptions {
        config_path: Some(harness.config_path.clone()),
        metadata_db_path: None,
    }).expect("reopen engine");
    drop(reopened);

    let conn = Connection::open(&harness.metadata_db_path).expect("sqlite");
    let finished_jobs: i64 = conn
        .query_row("SELECT COUNT(*) FROM erase_jobs WHERE state = 'FINISHED'", [], |row| row.get(0))
        .expect("finished jobs");
    assert_eq!(finished_jobs, 1);
}
```

- [ ] **Step 2: 扩展 MySQL lifecycle smoke test**

在 `tests/standalone_mysql_server.rs` 的 managed lake case 末尾追加：

```rust
conn.query_drop("truncate table analytics.orders")
    .expect("truncate managed table");
let empty_rows: Vec<(i32, Option<String>)> = conn
    .query("select k1, v1 from analytics.orders order by k1")
    .expect("select after truncate");
assert!(empty_rows.is_empty(), "rows after truncate: {empty_rows:?}");

conn.query_drop("drop table analytics.orders")
    .expect("drop managed table");
let err = conn
    .query::<(i32,), _>("select k1 from analytics.orders")
    .expect_err("query after drop should fail");
assert!(
    err.to_string().contains("Unknown table") || err.to_string().contains("unknown table"),
    "unexpected error after managed drop: {err}"
);
```

- [ ] **Step 3: 运行 lifecycle integration tests**

Run:

```bash
cd /Users/harbor/project/NovaRocks
cargo test --test standalone_managed_lake managed_lake_drop_table_hides_table_and_schedules_erase -- --nocapture
cargo test --test standalone_managed_lake managed_lake_truncate_replaces_partition_and_clears_visible_rows -- --nocapture
cargo test --test standalone_managed_lake reopen_cleans_incomplete_truncate_stage_partition -- --nocapture
cargo test --test standalone_managed_lake reopen_runs_pending_drop_erase_job -- --nocapture
```

Expected: PASS

- [ ] **Step 4: 运行 MySQL managed lifecycle smoke test**

Run:

```bash
cd /Users/harbor/project/NovaRocks
cargo test --test standalone_mysql_server standalone_mysql_server_managed_lake_round_trip -- --nocapture
```

Expected: PASS

- [ ] **Step 5: Commit**

```bash
cd /Users/harbor/project/NovaRocks
git add tests/standalone_managed_lake.rs tests/standalone_mysql_server.rs
git commit -m "test: cover managed lake lifecycle flows"
```

### Task 8: Final Verification Sweep

**Files:**
- Inspect: `/Users/harbor/project/NovaRocks/src/standalone/store.rs`
- Inspect: `/Users/harbor/project/NovaRocks/src/standalone/lake_erase.rs`
- Inspect: `/Users/harbor/project/NovaRocks/src/standalone/lake_recovery.rs`
- Inspect: `/Users/harbor/project/NovaRocks/src/standalone/lake_ddl.rs`
- Inspect: `/Users/harbor/project/NovaRocks/src/standalone/engine.rs`
- Inspect: `/Users/harbor/project/NovaRocks/tests/standalone_managed_lake.rs`
- Inspect: `/Users/harbor/project/NovaRocks/tests/standalone_mysql_server.rs`

- [ ] **Step 1: 运行格式化**

Run:

```bash
cd /Users/harbor/project/NovaRocks
cargo fmt
```

Expected: no output or only rustfmt progress

- [ ] **Step 2: 运行 targeted library tests**

Run:

```bash
cd /Users/harbor/project/NovaRocks
cargo test --lib standalone_store_round_trips_lifecycle_states_and_erase_jobs -- --nocapture
cargo test --lib drop_managed_table_marks_metadata_and_enqueues_drop_job -- --nocapture
cargo test --lib activate_truncate_partition_switches_active_partition_and_enqueues_erase -- --nocapture
cargo test --lib rebuild_ignores_dropping_tables_and_retired_partitions -- --nocapture
cargo test --lib erase_worker_finishes_drop_partition_job_and_purges_metadata -- --nocapture
```

Expected: PASS

- [ ] **Step 3: 运行 targeted integration tests**

Run:

```bash
cd /Users/harbor/project/NovaRocks
cargo test --test standalone_managed_lake managed_lake_drop_table_hides_table_and_schedules_erase -- --nocapture
cargo test --test standalone_managed_lake managed_lake_truncate_replaces_partition_and_clears_visible_rows -- --nocapture
cargo test --test standalone_managed_lake reopen_cleans_incomplete_truncate_stage_partition -- --nocapture
cargo test --test standalone_managed_lake reopen_runs_pending_drop_erase_job -- --nocapture
cargo test --test standalone_mysql_server standalone_mysql_server_managed_lake_round_trip -- --nocapture
```

Expected: PASS

- [ ] **Step 4: 总结未覆盖风险**

在最终交付说明里明确列出：

```text
- `RECOVER TABLE` still unsupported by design
- `DROP TABLE FORCE` still unsupported by design
- erase worker uses fixed retry/backoff and no admin visibility yet
```

- [ ] **Step 5: Commit**

```bash
cd /Users/harbor/project/NovaRocks
git status --short
git add src/standalone/store.rs src/standalone/lake_erase.rs src/standalone/lake_recovery.rs src/standalone/lake_ddl.rs src/standalone/engine.rs src/standalone/mod.rs tests/standalone_managed_lake.rs tests/standalone_mysql_server.rs
git commit -m "feat: add standalone managed lake lifecycle"
```
