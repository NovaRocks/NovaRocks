# Materialized Views on Iceberg — Phase 1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Deliver a working, restart-safe `CREATE / REFRESH / DROP / SHOW MATERIALIZED VIEW` surface in standalone mode where MV base tables are Iceberg tables and each MV is materialized as a kind-tagged managed lake table.

**Architecture:** MV metadata lives in a new `materialized_views` SQLite table; the MV's physical data lives in a normal managed lake `tables` row tagged `kind='MATERIALIZED_VIEW'`. `REFRESH` is a synchronous stage → execute → activate partition swap that reuses the existing `TRUNCATE` lifecycle machinery. `SELECT * FROM mv` goes through the normal managed-lake read path.

**Tech Stack:** Rust, rusqlite, Arrow `RecordBatch` / `Chunk`, custom sqlparser dialect at `src/sql/parser/dialect/`, existing managed-lake lifecycle (`src/standalone/lake/*`), Iceberg registry (`src/standalone/iceberg/registry.rs`), `cargo test` (lib + `tests/standalone_mysql_server.rs`), SQL-tests runner.

**Spec:** `docs/superpowers/specs/2026-04-23-mv-on-iceberg-phase1-design.md`

---

## File Structure

**Create**
- `src/standalone/lake/mv_ddl.rs` — CREATE / DROP / SHOW helpers at the engine boundary.
- `src/standalone/lake/mv_refresh.rs` — synchronous full REFRESH orchestration (stage → execute → activate with failure cleanup).

**Modify**
- `src/standalone/lake/store.rs` — schema v4 bump, new types, new transactions for MV create / stage / activate / drop enhancements.
- `src/standalone/lake/mod.rs` — `pub(crate) mod mv_ddl; pub(crate) mod mv_refresh;`
- `src/standalone/lake/catalog.rs` — thread `kind` through rebuild and the `StoredManagedTable` → `ManagedTableRuntime` translation.
- `src/standalone/lake/ddl.rs` — `create_managed_table` / `drop_managed_table` helpers must preserve `kind='TABLE'` for non-MV paths; DROP path gains a CREATING-partition rejection.
- `src/standalone/lake/txn.rs` — generalize the INSERT writer so it can target a specific staged partition + consume arbitrary `Chunk`s rather than only `InsertSource::Values`.
- `src/sql/parser/dialect/mod.rs` (or a new submodule `src/sql/parser/dialect/materialized_view.rs`) — parse four new statements into four new `ast` variants.
- `src/sql/parser/ast/mod.rs` — four new `Statement` variants.
- `src/standalone/engine/mod.rs` — dispatch on the new AST variants; delete the in-memory MV registry and the text-tokenizing stub dispatch.
- `src/standalone/engine/sqlparse/mod.rs` — drop the `pub(crate) mod materialized_view;` declaration.
- `tests/standalone_mysql_server.rs` — four new MV integration tests.

**Delete**
- `src/standalone/engine/sqlparse/materialized_view.rs` — its tokenize-the-SQL-text helpers are entirely superseded by real dialect parsing.

**SQL-tests**
- `tests/sql-test-runner/conf/standalone_managed_lake_cases/managed_lake_mv_basic.sql` (+ `.result`) — one happy-path regression case.

---

## Execution Notes

- **Throughout: verify before claiming completion.** Every "run test → PASS" step requires actually running the listed command and confirming the expected output before ticking the checkbox.
- **Build mode:** debug (`cargo build`, `cargo test`) for every task in this plan. No release build needed.
- **sqlite schema bump:** any pre-existing `standalone.sqlite` on disk must be deleted before running integration tests after Task 1. This is intentional (spec §5.1: no migration).

---

## Task 1: Bump SQLite schema to v4 (`kind` column + `materialized_views` table)

**Files:**
- Modify: `src/standalone/lake/store.rs`
- Test: `src/standalone/lake/store.rs` (inline `#[cfg(test)]`)

- [ ] **Step 1: Write the failing test**

Add inline at the bottom of `src/standalone/lake/store.rs`'s `tests` module:

```rust
#[test]
fn init_schema_v4_creates_tables_with_kind_and_materialized_views_table() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = SqliteMetadataStore::open(dir.path().join("standalone.sqlite"))
        .expect("open fresh store");
    let conn = store.connection().expect("connection");

    let version: i64 = conn
        .query_row("PRAGMA user_version", [], |row| row.get(0))
        .expect("user_version");
    assert_eq!(version, 4);

    // `tables` must have the new `kind` column with the expected default and check.
    let kind_col_exists: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM pragma_table_info('tables') WHERE name = 'kind'",
            [],
            |row| row.get(0),
        )
        .expect("pragma_table_info tables");
    assert_eq!(kind_col_exists, 1);

    // `materialized_views` must exist with mv_id primary key and the declared columns.
    let mv_cols: Vec<(String, String, i64)> = {
        let mut stmt = conn
            .prepare("SELECT name, type, \"notnull\" FROM pragma_table_info('materialized_views') ORDER BY cid")
            .expect("prepare");
        let rows = stmt
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                ))
            })
            .expect("query")
            .collect::<Result<Vec<_>, _>>()
            .expect("collect");
        rows
    };
    let names: Vec<&str> = mv_cols.iter().map(|(n, _, _)| n.as_str()).collect();
    assert_eq!(
        names,
        vec![
            "mv_id",
            "select_sql",
            "refresh_mode",
            "base_table_refs_json",
            "last_refresh_ms",
            "last_refresh_rows",
            "last_refresh_snapshots_json",
            "created_at_ms",
        ],
    );
}

#[test]
fn init_schema_rejects_pre_v4_database() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("old.sqlite");
    {
        let conn = rusqlite::Connection::open(&path).expect("open");
        conn.execute_batch("PRAGMA user_version = 3;")
            .expect("set old version");
    }
    let err = SqliteMetadataStore::open(&path).expect_err("open on v3 must fail");
    assert!(err.contains("schema version 3"), "err={err}");
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
cd /Users/harbor/project/NovaRocks
cargo test --lib standalone::lake::store::tests::init_schema_v4_creates_tables_with_kind_and_materialized_views_table -- --nocapture
cargo test --lib standalone::lake::store::tests::init_schema_rejects_pre_v4_database -- --nocapture
```

Expected: FAIL — current schema is v3, `tables.kind` doesn't exist, `materialized_views` doesn't exist.

- [ ] **Step 3: Update `init_schema()`**

In `src/standalone/lake/store.rs`, replace the `fn init_schema(&self)` body so that:

1. The version gate accepts `0` (fresh) and `4` (current). Anything else fails with `unsupported standalone metadata schema version {current_version}; delete the metadata db and reopen`.
2. The `tables` CREATE TABLE grows a `kind` column with the declared check constraint.
3. A new `CREATE TABLE IF NOT EXISTS materialized_views (...)` is appended.
4. `PRAGMA user_version = 3;` becomes `PRAGMA user_version = 4;`.

Concretely:

```rust
fn init_schema(&self) -> Result<(), String> {
    let conn = self.connection()?;
    let current_version: i64 = conn
        .query_row("PRAGMA user_version", [], |row| row.get(0))
        .map_err(|e| format!("read standalone metadata schema version failed: {e}"))?;
    if current_version != 0 && current_version != 4 {
        return Err(format!(
            "unsupported standalone metadata schema version {current_version}; delete the metadata db and reopen"
        ));
    }
    conn.execute_batch(
        "
        PRAGMA journal_mode = WAL;
        PRAGMA foreign_keys = ON;
        PRAGMA synchronous = NORMAL;
        CREATE TABLE IF NOT EXISTS local_databases (
            name TEXT PRIMARY KEY
        );
        DROP TABLE IF EXISTS local_tables;
        CREATE TABLE IF NOT EXISTS global_meta (
            singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
            warehouse_uri TEXT NOT NULL,
            next_db_id INTEGER NOT NULL,
            next_table_id INTEGER NOT NULL,
            next_partition_id INTEGER NOT NULL,
            next_index_id INTEGER NOT NULL,
            next_tablet_id INTEGER NOT NULL,
            next_txn_id INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS databases (
            db_id INTEGER PRIMARY KEY,
            name TEXT NOT NULL UNIQUE
        );
        CREATE TABLE IF NOT EXISTS tables (
            table_id INTEGER PRIMARY KEY,
            db_id INTEGER NOT NULL,
            name TEXT NOT NULL,
            keys_type TEXT NOT NULL,
            bucket_num INTEGER NOT NULL,
            current_schema_id INTEGER NOT NULL,
            state TEXT NOT NULL,
            kind TEXT NOT NULL DEFAULT 'TABLE'
                CHECK (kind IN ('TABLE', 'MATERIALIZED_VIEW')),
            UNIQUE(db_id, name)
        );
        CREATE TABLE IF NOT EXISTS table_schemas (
            schema_id INTEGER PRIMARY KEY,
            table_id INTEGER NOT NULL,
            schema_version INTEGER NOT NULL,
            tablet_schema_pb BLOB NOT NULL
        );
        CREATE TABLE IF NOT EXISTS table_columns (
            schema_id INTEGER NOT NULL,
            ordinal INTEGER NOT NULL,
            column_name TEXT NOT NULL,
            logical_type TEXT NOT NULL,
            nullable INTEGER NOT NULL,
            PRIMARY KEY (schema_id, ordinal)
        );
        CREATE TABLE IF NOT EXISTS partitions (
            partition_id INTEGER PRIMARY KEY,
            table_id INTEGER NOT NULL,
            name TEXT NOT NULL,
            visible_version INTEGER NOT NULL,
            next_version INTEGER NOT NULL,
            state TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS indexes (
            index_id INTEGER PRIMARY KEY,
            table_id INTEGER NOT NULL,
            partition_id INTEGER NOT NULL,
            index_type TEXT NOT NULL,
            state TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS tablets (
            tablet_id INTEGER PRIMARY KEY,
            partition_id INTEGER NOT NULL,
            index_id INTEGER NOT NULL,
            bucket_seq INTEGER NOT NULL,
            tablet_root_path TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS txns (
            txn_id INTEGER PRIMARY KEY,
            table_id INTEGER NOT NULL,
            partition_id INTEGER NOT NULL,
            base_version INTEGER NOT NULL,
            commit_version INTEGER NOT NULL,
            state TEXT NOT NULL,
            retry_at_ms INTEGER,
            updated_at_ms INTEGER NOT NULL
        );
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
        CREATE TABLE IF NOT EXISTS materialized_views (
            mv_id INTEGER PRIMARY KEY REFERENCES tables(table_id),
            select_sql TEXT NOT NULL,
            refresh_mode TEXT NOT NULL DEFAULT 'DEFERRED_MANUAL'
                CHECK (refresh_mode IN ('DEFERRED_MANUAL')),
            base_table_refs_json TEXT NOT NULL,
            last_refresh_ms INTEGER,
            last_refresh_rows INTEGER,
            last_refresh_snapshots_json TEXT,
            created_at_ms INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS iceberg_catalogs (
            name TEXT PRIMARY KEY,
            properties_json TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS iceberg_namespaces (
            catalog_name TEXT NOT NULL,
            namespace_name TEXT NOT NULL,
            PRIMARY KEY (catalog_name, namespace_name)
        );
        CREATE TABLE IF NOT EXISTS iceberg_tables (
            catalog_name TEXT NOT NULL,
            namespace_name TEXT NOT NULL,
            table_name TEXT NOT NULL,
            PRIMARY KEY (catalog_name, namespace_name, table_name)
        );
        PRAGMA user_version = 4;
        ",
    )
    .map_err(|e| format!("initialize standalone metadata schema failed: {e}"))?;
    Ok(())
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run:

```bash
cd /Users/harbor/project/NovaRocks
cargo test --lib standalone::lake::store::tests::init_schema_v4_creates_tables_with_kind_and_materialized_views_table -- --nocapture
cargo test --lib standalone::lake::store::tests::init_schema_rejects_pre_v4_database -- --nocapture
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cd /Users/harbor/project/NovaRocks
git add src/standalone/lake/store.rs
git commit -m "feat: bump managed lake schema to v4 with kind and materialized_views"
```

---

## Task 2: Add stored types and extend `ManagedSnapshot` round-trip

**Files:**
- Modify: `src/standalone/lake/store.rs`
- Test: `src/standalone/lake/store.rs` (inline)

- [ ] **Step 1: Write the failing test**

Append to the `tests` module in `store.rs`:

```rust
#[test]
fn managed_snapshot_round_trips_mv_rows_and_kind_column() {
    use std::collections::BTreeMap;

    let dir = tempfile::tempdir().expect("tempdir");
    let store = SqliteMetadataStore::open(dir.path().join("standalone.sqlite"))
        .expect("open");

    let mut snapshot = ManagedSnapshot {
        global: ManagedGlobalMeta {
            warehouse_uri: "s3://bucket/warehouse".to_string(),
            next_db_id: 2,
            next_table_id: 11,
            next_partition_id: 21,
            next_index_id: 31,
            next_tablet_id: 43,
            next_txn_id: 1,
        },
        databases: vec![StoredManagedDatabase {
            db_id: 1,
            name: "analytics".to_string(),
        }],
        tables: vec![
            StoredManagedTable {
                table_id: 10,
                db_id: 1,
                name: "orders_mv".to_string(),
                keys_type: "DUP_KEYS".to_string(),
                bucket_num: 2,
                current_schema_id: 10,
                state: ManagedTableState::Active,
                kind: ManagedTableKind::MaterializedView,
            },
        ],
        schemas: vec![],
        columns: vec![],
        partitions: vec![StoredManagedPartition {
            partition_id: 20,
            table_id: 10,
            name: "p0".to_string(),
            visible_version: 1,
            next_version: 2,
            state: ManagedPartitionState::Active,
        }],
        indexes: vec![StoredManagedIndex {
            index_id: 30,
            table_id: 10,
            partition_id: 20,
            index_type: "BASE".to_string(),
            state: ManagedIndexState::Active,
        }],
        tablets: vec![
            StoredManagedTablet {
                tablet_id: 40,
                partition_id: 20,
                index_id: 30,
                bucket_seq: 0,
                tablet_root_path: "s3://bucket/warehouse/db_1/table_10/partition_20".to_string(),
            },
            StoredManagedTablet {
                tablet_id: 41,
                partition_id: 20,
                index_id: 30,
                bucket_seq: 1,
                tablet_root_path: "s3://bucket/warehouse/db_1/table_10/partition_20".to_string(),
            },
        ],
        txns: vec![],
        erase_jobs: vec![],
        materialized_views: vec![StoredMaterializedView {
            mv_id: 10,
            select_sql: "SELECT k1, sum(v2) FROM iceberg_cat.ns.orders GROUP BY k1".to_string(),
            refresh_mode: ManagedMvRefreshMode::DeferredManual,
            base_table_refs: vec![IcebergTableRef {
                catalog: "iceberg_cat".to_string(),
                namespace: "ns".to_string(),
                table: "orders".to_string(),
            }],
            last_refresh_ms: Some(1_700_000_000_000),
            last_refresh_rows: Some(123),
            last_refresh_snapshots: {
                let mut map = BTreeMap::new();
                map.insert("iceberg_cat.ns.orders".to_string(), 7_391_842_i64);
                map
            },
            created_at_ms: 1_699_999_999_000,
        }],
    };

    store
        .replace_managed_snapshot(&mut snapshot)
        .expect("persist");
    let loaded = store.load_snapshot().expect("reload").managed;
    assert_eq!(loaded, snapshot);
}

#[test]
fn managed_snapshot_round_trips_kind_table_default() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = SqliteMetadataStore::open(dir.path().join("standalone.sqlite"))
        .expect("open");

    let mut snapshot = ManagedSnapshot::default();
    snapshot.global.warehouse_uri = "s3://bucket/warehouse".to_string();
    snapshot.global.next_table_id = 2;
    snapshot.tables.push(StoredManagedTable {
        table_id: 1,
        db_id: 1,
        name: "orders".to_string(),
        keys_type: "DUP_KEYS".to_string(),
        bucket_num: 2,
        current_schema_id: 1,
        state: ManagedTableState::Active,
        kind: ManagedTableKind::Table,
    });
    snapshot.databases.push(StoredManagedDatabase {
        db_id: 1,
        name: "analytics".to_string(),
    });

    store
        .replace_managed_snapshot(&mut snapshot)
        .expect("persist");
    let loaded = store.load_snapshot().expect("reload").managed;
    assert_eq!(loaded.tables[0].kind, ManagedTableKind::Table);
    assert!(loaded.materialized_views.is_empty());
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
cd /Users/harbor/project/NovaRocks
cargo test --lib standalone::lake::store::tests::managed_snapshot_round_trips_mv_rows_and_kind_column -- --nocapture
cargo test --lib standalone::lake::store::tests::managed_snapshot_round_trips_kind_table_default -- --nocapture
```

Expected: FAIL — compile errors for `ManagedTableKind`, `StoredMaterializedView`, `IcebergTableRef`, `ManagedMvRefreshMode`, and the `kind` / `materialized_views` fields.

- [ ] **Step 3: Add the types**

In `src/standalone/lake/store.rs`, near the other enums (after `ManagedTableState`):

```rust
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) enum ManagedTableKind {
    #[default]
    Table,
    MaterializedView,
}

impl ManagedTableKind {
    fn as_sql_str(self) -> &'static str {
        match self {
            Self::Table => "TABLE",
            Self::MaterializedView => "MATERIALIZED_VIEW",
        }
    }

    fn from_sql_str(value: &str) -> Result<Self, String> {
        match value {
            "TABLE" => Ok(Self::Table),
            "MATERIALIZED_VIEW" => Ok(Self::MaterializedView),
            _ => Err(format!("unknown managed table kind `{value}`")),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) enum ManagedMvRefreshMode {
    #[default]
    DeferredManual,
}

impl ManagedMvRefreshMode {
    fn as_sql_str(self) -> &'static str {
        match self {
            Self::DeferredManual => "DEFERRED_MANUAL",
        }
    }

    fn from_sql_str(value: &str) -> Result<Self, String> {
        match value {
            "DEFERRED_MANUAL" => Ok(Self::DeferredManual),
            _ => Err(format!("unknown managed mv refresh mode `{value}`")),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct IcebergTableRef {
    pub catalog: String,
    pub namespace: String,
    pub table: String,
}

impl IcebergTableRef {
    pub(crate) fn fqn(&self) -> String {
        format!("{}.{}.{}", self.catalog, self.namespace, self.table)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct StoredMaterializedView {
    pub mv_id: i64,
    pub select_sql: String,
    pub refresh_mode: ManagedMvRefreshMode,
    pub base_table_refs: Vec<IcebergTableRef>,
    pub last_refresh_ms: Option<i64>,
    pub last_refresh_rows: Option<i64>,
    pub last_refresh_snapshots: std::collections::BTreeMap<String, i64>,
    pub created_at_ms: i64,
}
```

Add `pub kind: ManagedTableKind` to `StoredManagedTable`. Update its `Default` derive or add a manual `Default` impl if present.

Extend `ManagedSnapshot` with `pub materialized_views: Vec<StoredMaterializedView>` and update `is_empty()` to also require `materialized_views.is_empty()`.

- [ ] **Step 4: Extend read/write paths in `store.rs`**

Update these specific locations in `src/standalone/lake/store.rs`:

1. **`load_managed_snapshot`'s `tables` section** (around the existing `SELECT table_id, db_id, name, keys_type, bucket_num, current_schema_id, state FROM tables` statement): add `kind` to the column list, map it via `ManagedTableKind::from_sql_str`, populate the new `StoredManagedTable` field.

2. **Add a new block in `load_managed_snapshot`** that loads `materialized_views`:

    ```rust
    let materialized_views = {
        let mut stmt = conn
            .prepare(
                "SELECT
                    mv_id,
                    select_sql,
                    refresh_mode,
                    base_table_refs_json,
                    last_refresh_ms,
                    last_refresh_rows,
                    last_refresh_snapshots_json,
                    created_at_ms
                 FROM materialized_views
                 ORDER BY mv_id",
            )
            .map_err(|e| format!("prepare materialized_views query failed: {e}"))?;
        let rows = stmt
            .query_map([], |row| {
                let refresh_mode = ManagedMvRefreshMode::from_sql_str(
                    &row.get::<_, String>(2)?,
                )
                .map_err(invalid_state_sql_error)?;
                let base_json: String = row.get(3)?;
                let base_table_refs: Vec<IcebergTableRef> =
                    serde_json::from_str(&base_json).map_err(json_to_sql_error)?;
                let snapshots: std::collections::BTreeMap<String, i64> =
                    match row.get::<_, Option<String>>(6)? {
                        Some(s) => serde_json::from_str(&s).map_err(json_to_sql_error)?,
                        None => std::collections::BTreeMap::new(),
                    };
                Ok(StoredMaterializedView {
                    mv_id: row.get(0)?,
                    select_sql: row.get(1)?,
                    refresh_mode,
                    base_table_refs,
                    last_refresh_ms: row.get(4)?,
                    last_refresh_rows: row.get(5)?,
                    last_refresh_snapshots: snapshots,
                    created_at_ms: row.get(7)?,
                })
            })
            .map_err(|e| format!("query materialized_views failed: {e}"))?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(|e| format!("read materialized_views failed: {e}"))?
    };
    ```

    Return this vector in the `ManagedSnapshot` literal.

3. **`replace_managed_snapshot`**:
    - Update the INSERT for `tables` so it writes the `kind` column, using `snapshot.tables[i].kind.as_sql_str()`.
    - After the existing `erase_jobs` loop, add:

    ```rust
    tx.execute("DELETE FROM materialized_views", [])
        .map_err(|e| format!("clear materialized_views failed: {e}"))?;
    for mv in &snapshot.materialized_views {
        let base_json = serde_json::to_string(&mv.base_table_refs)
            .map_err(|e| format!("serialize mv base refs failed: {e}"))?;
        let snapshots_json = if mv.last_refresh_snapshots.is_empty() {
            None
        } else {
            Some(
                serde_json::to_string(&mv.last_refresh_snapshots)
                    .map_err(|e| format!("serialize mv snapshots failed: {e}"))?,
            )
        };
        tx.execute(
            "INSERT INTO materialized_views(
                mv_id,
                select_sql,
                refresh_mode,
                base_table_refs_json,
                last_refresh_ms,
                last_refresh_rows,
                last_refresh_snapshots_json,
                created_at_ms
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![
                mv.mv_id,
                mv.select_sql,
                mv.refresh_mode.as_sql_str(),
                base_json,
                mv.last_refresh_ms,
                mv.last_refresh_rows,
                snapshots_json,
                mv.created_at_ms,
            ],
        )
        .map_err(|e| format!("insert materialized_view failed: {e}"))?;
    }
    ```

4. **Any other callsite that builds `StoredManagedTable` inline** (search with Grep for `StoredManagedTable {` under `src/standalone/lake/`): add `kind: ManagedTableKind::Table` to the literal. Tests and production code.

- [ ] **Step 5: Run the two new tests and the full `standalone::lake::store` test module**

Run:

```bash
cd /Users/harbor/project/NovaRocks
cargo test --lib standalone::lake::store::tests -- --nocapture
```

Expected: PASS (including the two new tests and all pre-existing `store::tests`).

- [ ] **Step 6: Commit**

```bash
cd /Users/harbor/project/NovaRocks
git add src/standalone/lake/store.rs
git commit -m "feat: persist materialized_view rows and table kind in managed snapshot"
```

---

## Task 3: Store transactions for MV create, refresh stage/activate, and drop enhancement

**Files:**
- Modify: `src/standalone/lake/store.rs`
- Test: `src/standalone/lake/store.rs` (inline)

- [ ] **Step 1: Write failing tests**

Append to the `tests` module:

```rust
fn empty_mv_refresh_snapshot(warehouse: &str) -> ManagedSnapshot {
    let mut snapshot = ManagedSnapshot::default();
    snapshot.global.warehouse_uri = warehouse.to_string();
    snapshot.global.next_db_id = 2;
    snapshot.global.next_table_id = 11;
    snapshot.global.next_partition_id = 21;
    snapshot.global.next_index_id = 31;
    snapshot.global.next_tablet_id = 43;
    snapshot.global.next_txn_id = 1;
    snapshot.databases.push(StoredManagedDatabase {
        db_id: 1,
        name: "analytics".to_string(),
    });
    snapshot.tables.push(StoredManagedTable {
        table_id: 10,
        db_id: 1,
        name: "orders_mv".to_string(),
        keys_type: "DUP_KEYS".to_string(),
        bucket_num: 2,
        current_schema_id: 10,
        state: ManagedTableState::Active,
        kind: ManagedTableKind::MaterializedView,
    });
    snapshot.partitions.push(StoredManagedPartition {
        partition_id: 20,
        table_id: 10,
        name: "p0".to_string(),
        visible_version: 1,
        next_version: 2,
        state: ManagedPartitionState::Active,
    });
    snapshot.indexes.push(StoredManagedIndex {
        index_id: 30,
        table_id: 10,
        partition_id: 20,
        index_type: "BASE".to_string(),
        state: ManagedIndexState::Active,
    });
    for bucket_seq in 0..2 {
        snapshot.tablets.push(StoredManagedTablet {
            tablet_id: 40 + bucket_seq,
            partition_id: 20,
            index_id: 30,
            bucket_seq,
            tablet_root_path: "s3://bucket/warehouse/db_1/table_10/partition_20".to_string(),
        });
    }
    snapshot.materialized_views.push(StoredMaterializedView {
        mv_id: 10,
        select_sql: "SELECT k1 FROM iceberg_cat.ns.orders".to_string(),
        refresh_mode: ManagedMvRefreshMode::DeferredManual,
        base_table_refs: vec![IcebergTableRef {
            catalog: "iceberg_cat".to_string(),
            namespace: "ns".to_string(),
            table: "orders".to_string(),
        }],
        last_refresh_ms: None,
        last_refresh_rows: None,
        last_refresh_snapshots: std::collections::BTreeMap::new(),
        created_at_ms: 1_700_000_000_000,
    });
    snapshot
}

#[test]
fn stage_mv_refresh_partition_rejects_when_refresh_already_in_progress() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = SqliteMetadataStore::open(dir.path().join("standalone.sqlite"))
        .expect("open");
    let mut snapshot = empty_mv_refresh_snapshot("s3://bucket/warehouse");
    snapshot.partitions.push(StoredManagedPartition {
        partition_id: 22,
        table_id: 10,
        name: "p0".to_string(),
        visible_version: 1,
        next_version: 2,
        state: ManagedPartitionState::Creating,
    });
    store
        .replace_managed_snapshot(&mut snapshot)
        .expect("persist");

    let err = store
        .stage_mv_refresh_partition(StageMvRefreshRequest {
            table_id: 10,
            db_id: 1,
            bucket_num: 2,
            partition_name: "p0".to_string(),
            warehouse_uri: "s3://bucket/warehouse".to_string(),
        })
        .expect_err("stage should reject");
    assert!(err.contains("refresh already in progress"), "err={err}");
}

#[test]
fn stage_mv_refresh_partition_rejects_when_mv_not_active() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = SqliteMetadataStore::open(dir.path().join("standalone.sqlite"))
        .expect("open");
    let mut snapshot = empty_mv_refresh_snapshot("s3://bucket/warehouse");
    snapshot.tables[0].state = ManagedTableState::Dropping;
    store
        .replace_managed_snapshot(&mut snapshot)
        .expect("persist");

    let err = store
        .stage_mv_refresh_partition(StageMvRefreshRequest {
            table_id: 10,
            db_id: 1,
            bucket_num: 2,
            partition_name: "p0".to_string(),
            warehouse_uri: "s3://bucket/warehouse".to_string(),
        })
        .expect_err("stage should reject");
    assert!(err.contains("is not active"), "err={err}");
}

#[test]
fn activate_mv_refresh_partition_swaps_and_writes_last_refresh_fields() {
    use std::collections::BTreeMap;

    let dir = tempfile::tempdir().expect("tempdir");
    let store = SqliteMetadataStore::open(dir.path().join("standalone.sqlite"))
        .expect("open");
    let mut snapshot = empty_mv_refresh_snapshot("s3://bucket/warehouse");
    store
        .replace_managed_snapshot(&mut snapshot)
        .expect("persist");

    let staged = store
        .stage_mv_refresh_partition(StageMvRefreshRequest {
            table_id: 10,
            db_id: 1,
            bucket_num: 2,
            partition_name: "p0".to_string(),
            warehouse_uri: "s3://bucket/warehouse".to_string(),
        })
        .expect("stage");

    let mut snapshots_map = BTreeMap::new();
    snapshots_map.insert("iceberg_cat.ns.orders".to_string(), 9_999_i64);

    store
        .activate_mv_refresh_partition(ActivateMvRefreshRequest {
            table_id: 10,
            old_partition_id: 20,
            new_partition_id: staged.partition_id,
            new_index_id: staged.index_id,
            retired_root_path: "s3://bucket/warehouse/db_1/table_10/partition_20".to_string(),
            rows_written: 42,
            snapshots: snapshots_map.clone(),
        })
        .expect("activate");

    let loaded = store.load_snapshot().expect("reload").managed;
    let active_pids: Vec<i64> = loaded
        .partitions
        .iter()
        .filter(|p| p.state == ManagedPartitionState::Active)
        .map(|p| p.partition_id)
        .collect();
    assert_eq!(active_pids, vec![staged.partition_id]);
    assert!(
        loaded
            .partitions
            .iter()
            .any(|p| p.partition_id == 20 && p.state == ManagedPartitionState::Retired)
    );
    let erase_jobs: Vec<&StoredManagedEraseJob> = loaded
        .erase_jobs
        .iter()
        .filter(|j| j.partition_id == Some(20))
        .collect();
    assert_eq!(erase_jobs.len(), 1);
    assert_eq!(erase_jobs[0].job_kind, ManagedEraseJobKind::DropPartition);
    let mv = loaded
        .materialized_views
        .iter()
        .find(|mv| mv.mv_id == 10)
        .expect("mv row");
    assert_eq!(mv.last_refresh_rows, Some(42));
    assert_eq!(mv.last_refresh_snapshots, snapshots_map);
    assert!(mv.last_refresh_ms.is_some());
}

#[test]
fn drop_managed_table_rejects_mv_with_inflight_refresh() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = SqliteMetadataStore::open(dir.path().join("standalone.sqlite"))
        .expect("open");
    let mut snapshot = empty_mv_refresh_snapshot("s3://bucket/warehouse");
    snapshot.partitions.push(StoredManagedPartition {
        partition_id: 22,
        table_id: 10,
        name: "p0".to_string(),
        visible_version: 1,
        next_version: 2,
        state: ManagedPartitionState::Creating,
    });
    store
        .replace_managed_snapshot(&mut snapshot)
        .expect("persist");

    let err = store
        .drop_managed_table(10, "s3://bucket/warehouse/db_1/table_10")
        .expect_err("drop should reject");
    assert!(err.contains("refresh in progress"), "err={err}");
}

#[test]
fn purge_retired_table_metadata_removes_mv_row() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = SqliteMetadataStore::open(dir.path().join("standalone.sqlite"))
        .expect("open");
    let mut snapshot = empty_mv_refresh_snapshot("s3://bucket/warehouse");
    snapshot.tables[0].state = ManagedTableState::Dropping;
    for partition in &mut snapshot.partitions {
        partition.state = ManagedPartitionState::Retired;
    }
    for index in &mut snapshot.indexes {
        index.state = ManagedIndexState::Retired;
    }
    store
        .replace_managed_snapshot(&mut snapshot)
        .expect("persist");

    store.purge_retired_table_metadata(10).expect("purge");
    let loaded = store.load_snapshot().expect("reload").managed;
    assert!(loaded.tables.is_empty());
    assert!(loaded.materialized_views.is_empty());
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
cd /Users/harbor/project/NovaRocks
cargo test --lib standalone::lake::store::tests::stage_mv_refresh_partition_rejects_when_refresh_already_in_progress -- --nocapture
cargo test --lib standalone::lake::store::tests::stage_mv_refresh_partition_rejects_when_mv_not_active -- --nocapture
cargo test --lib standalone::lake::store::tests::activate_mv_refresh_partition_swaps_and_writes_last_refresh_fields -- --nocapture
cargo test --lib standalone::lake::store::tests::drop_managed_table_rejects_mv_with_inflight_refresh -- --nocapture
cargo test --lib standalone::lake::store::tests::purge_retired_table_metadata_removes_mv_row -- --nocapture
```

Expected: FAIL — compile errors for the new request/response types and methods.

- [ ] **Step 3: Add request/response types and the `stage_mv_refresh_partition` transaction**

Append near `StageManagedTruncateRequest`:

```rust
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct StageMvRefreshRequest {
    pub table_id: i64,
    pub db_id: i64,
    pub bucket_num: i64,
    pub partition_name: String,
    pub warehouse_uri: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct StagedMvRefresh {
    pub partition_id: i64,
    pub index_id: i64,
    pub tablet_ids: Vec<i64>,
    pub partition_root_path: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ActivateMvRefreshRequest {
    pub table_id: i64,
    pub old_partition_id: i64,
    pub new_partition_id: i64,
    pub new_index_id: i64,
    pub retired_root_path: String,
    pub rows_written: i64,
    pub snapshots: std::collections::BTreeMap<String, i64>,
}
```

Add the staging method to `impl SqliteMetadataStore`:

```rust
pub(crate) fn stage_mv_refresh_partition(
    &self,
    req: StageMvRefreshRequest,
) -> Result<StagedMvRefresh, String> {
    let conn = self.connection()?;
    let tx = conn
        .unchecked_transaction()
        .map_err(|e| format!("begin stage_mv_refresh_partition transaction failed: {e}"))?;

    // Reject if MV is not active.
    let (table_state, table_kind): (String, String) = tx
        .query_row(
            "SELECT state, kind FROM tables WHERE table_id = ?1",
            params![req.table_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .map_err(|e| format!("lookup mv table {} failed: {e}", req.table_id))?;
    if table_kind != "MATERIALIZED_VIEW" {
        return Err(format!("table {} is not a materialized view", req.table_id));
    }
    if table_state != "ACTIVE" {
        return Err(format!(
            "materialized view {} is not active (state={table_state})",
            req.table_id
        ));
    }

    // Reject if a refresh is already in progress (any CREATING partition).
    let creating_count: i64 = tx
        .query_row(
            "SELECT COUNT(*) FROM partitions
             WHERE table_id = ?1 AND state = 'CREATING'",
            params![req.table_id],
            |row| row.get(0),
        )
        .map_err(|e| format!("count creating partitions failed: {e}"))?;
    if creating_count > 0 {
        return Err(format!(
            "cannot refresh materialized view {}: refresh already in progress",
            req.table_id
        ));
    }

    let partition_id: i64 = tx
        .query_row(
            "SELECT next_partition_id FROM global_meta WHERE singleton = 1",
            [],
            |row| row.get(0),
        )
        .map_err(|e| format!("read next_partition_id failed: {e}"))?;
    let index_id: i64 = tx
        .query_row(
            "SELECT next_index_id FROM global_meta WHERE singleton = 1",
            [],
            |row| row.get(0),
        )
        .map_err(|e| format!("read next_index_id failed: {e}"))?;
    let first_tablet_id: i64 = tx
        .query_row(
            "SELECT next_tablet_id FROM global_meta WHERE singleton = 1",
            [],
            |row| row.get(0),
        )
        .map_err(|e| format!("read next_tablet_id failed: {e}"))?;

    tx.execute(
        "UPDATE global_meta
         SET next_partition_id = ?1, next_index_id = ?2, next_tablet_id = ?3
         WHERE singleton = 1",
        params![
            partition_id + 1,
            index_id + 1,
            first_tablet_id + req.bucket_num
        ],
    )
    .map_err(|e| format!("bump mv refresh ids failed: {e}"))?;

    tx.execute(
        "INSERT INTO partitions(partition_id, table_id, name, visible_version, next_version, state)
         VALUES (?1, ?2, ?3, 1, 2, 'CREATING')",
        params![partition_id, req.table_id, req.partition_name],
    )
    .map_err(|e| format!("insert mv creating partition failed: {e}"))?;
    tx.execute(
        "INSERT INTO indexes(index_id, table_id, partition_id, index_type, state)
         VALUES (?1, ?2, ?3, 'BASE', 'CREATING')",
        params![index_id, req.table_id, partition_id],
    )
    .map_err(|e| format!("insert mv creating index failed: {e}"))?;

    let partition_root_path = format!(
        "{}/db_{}/table_{}/partition_{}",
        req.warehouse_uri.trim_end_matches('/'),
        req.db_id,
        req.table_id,
        partition_id
    );
    let mut tablet_ids = Vec::new();
    for bucket_seq in 0..req.bucket_num {
        let tablet_id = first_tablet_id + bucket_seq;
        tx.execute(
            "INSERT INTO tablets(tablet_id, partition_id, index_id, bucket_seq, tablet_root_path)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                tablet_id,
                partition_id,
                index_id,
                bucket_seq,
                partition_root_path,
            ],
        )
        .map_err(|e| format!("insert mv creating tablet failed: {e}"))?;
        tablet_ids.push(tablet_id);
    }

    tx.commit()
        .map_err(|e| format!("commit stage_mv_refresh_partition failed: {e}"))?;

    Ok(StagedMvRefresh {
        partition_id,
        index_id,
        tablet_ids,
        partition_root_path,
    })
}
```

- [ ] **Step 4: Add `activate_mv_refresh_partition`**

```rust
pub(crate) fn activate_mv_refresh_partition(
    &self,
    req: ActivateMvRefreshRequest,
) -> Result<(), String> {
    let conn = self.connection()?;
    let tx = conn
        .unchecked_transaction()
        .map_err(|e| format!("begin activate_mv_refresh_partition transaction failed: {e}"))?;

    let next_job_id: i64 = tx
        .query_row(
            "SELECT COALESCE(MAX(job_id), 0) + 1 FROM erase_jobs",
            [],
            |row| row.get(0),
        )
        .map_err(|e| format!("allocate erase job id failed: {e}"))?;

    tx.execute(
        "UPDATE partitions
         SET state = CASE
             WHEN partition_id = ?1 THEN 'ACTIVE'
             WHEN partition_id = ?2 THEN 'RETIRED'
             ELSE state
         END,
             visible_version = CASE
             WHEN partition_id = ?1 THEN 2
             ELSE visible_version
         END,
             next_version = CASE
             WHEN partition_id = ?1 THEN 3
             ELSE next_version
         END
         WHERE table_id = ?3",
        params![req.new_partition_id, req.old_partition_id, req.table_id],
    )
    .map_err(|e| format!("switch mv partition states failed: {e}"))?;
    tx.execute(
        "UPDATE indexes
         SET state = CASE
             WHEN index_id = ?1 THEN 'ACTIVE'
             WHEN partition_id = ?2 THEN 'RETIRED'
             ELSE state
         END
         WHERE table_id = ?3",
        params![req.new_index_id, req.old_partition_id, req.table_id],
    )
    .map_err(|e| format!("switch mv index states failed: {e}"))?;

    tx.execute(
        "INSERT INTO erase_jobs(
            job_id,
            job_kind,
            table_id,
            partition_id,
            root_path,
            state,
            retry_at_ms,
            updated_at_ms,
            last_error
         ) VALUES (?1, 'DROP_PARTITION', ?2, ?3, ?4, 'PENDING', NULL, strftime('%s','now') * 1000, NULL)",
        params![
            next_job_id,
            req.table_id,
            req.old_partition_id,
            req.retired_root_path,
        ],
    )
    .map_err(|e| format!("insert mv refresh erase job failed: {e}"))?;

    let snapshots_json = if req.snapshots.is_empty() {
        None
    } else {
        Some(
            serde_json::to_string(&req.snapshots)
                .map_err(|e| format!("serialize mv activate snapshots failed: {e}"))?,
        )
    };
    tx.execute(
        "UPDATE materialized_views
         SET last_refresh_ms = strftime('%s','now') * 1000,
             last_refresh_rows = ?1,
             last_refresh_snapshots_json = ?2
         WHERE mv_id = ?3",
        params![req.rows_written, snapshots_json, req.table_id],
    )
    .map_err(|e| format!("update materialized_view last_refresh fields failed: {e}"))?;

    tx.commit()
        .map_err(|e| format!("commit activate_mv_refresh_partition failed: {e}"))?;
    Ok(())
}
```

- [ ] **Step 5: Augment `drop_managed_table` with the CREATING-partition check**

Locate the existing `pub(crate) fn drop_managed_table(&self, table_id: i64, root_path: &str)` and insert the new reject, after the inflight-txn check:

```rust
let creating_count: i64 = tx
    .query_row(
        "SELECT COUNT(*) FROM partitions
         WHERE table_id = ?1 AND state = 'CREATING'",
        params![table_id],
        |row| row.get(0),
    )
    .map_err(|e| format!("count creating partitions for drop failed: {e}"))?;
if creating_count > 0 {
    return Err(format!(
        "cannot drop table {table_id}: refresh in progress"
    ));
}
```

- [ ] **Step 6: Extend `purge_retired_table_metadata` to remove the MV row**

In the existing transaction, before deleting from `tables`, add:

```rust
tx.execute(
    "DELETE FROM materialized_views WHERE mv_id = ?1",
    params![table_id],
)
.map_err(|e| format!("delete materialized_view row failed: {e}"))?;
```

- [ ] **Step 7: Run the new tests**

```bash
cd /Users/harbor/project/NovaRocks
cargo test --lib standalone::lake::store::tests::stage_mv_refresh_partition_rejects_when_refresh_already_in_progress -- --nocapture
cargo test --lib standalone::lake::store::tests::stage_mv_refresh_partition_rejects_when_mv_not_active -- --nocapture
cargo test --lib standalone::lake::store::tests::activate_mv_refresh_partition_swaps_and_writes_last_refresh_fields -- --nocapture
cargo test --lib standalone::lake::store::tests::drop_managed_table_rejects_mv_with_inflight_refresh -- --nocapture
cargo test --lib standalone::lake::store::tests::purge_retired_table_metadata_removes_mv_row -- --nocapture
cargo test --lib standalone::lake::store::tests -- --nocapture
```

Expected: PASS.

- [ ] **Step 8: Commit**

```bash
cd /Users/harbor/project/NovaRocks
git add src/standalone/lake/store.rs
git commit -m "feat: add managed lake store transactions for mv refresh and drop"
```

---

## Task 4: Thread `kind` through `ManagedTableRuntime` / `ManagedLakeCatalog`

**Files:**
- Modify: `src/standalone/lake/catalog.rs`
- Modify: `src/standalone/lake/ddl.rs` (set `kind = Table` on the existing CREATE TABLE path)

- [ ] **Step 1: Add a read-the-kind assertion to an existing catalog rebuild test**

Find the existing test `rebuild_assembles_active_tables_and_partitions` (or similar) in `src/standalone/lake/catalog.rs`. If no test currently inspects `kind`, add a new test named `rebuild_preserves_kind_column`:

```rust
#[test]
fn rebuild_preserves_kind_column() {
    let mut snapshot = snapshot_seed();
    // snapshot_seed creates a kind='TABLE' row by default; spot-check.
    let rebuilt = ManagedLakeCatalog::rebuild(Some(test_managed_config()), snapshot.clone())
        .expect("rebuild");
    let runtime = rebuilt
        .table("analytics", "orders")
        .expect("runtime")
        .clone();
    assert_eq!(runtime.table.kind, ManagedTableKind::Table);

    snapshot.tables[0].kind = ManagedTableKind::MaterializedView;
    let rebuilt_mv = ManagedLakeCatalog::rebuild(Some(test_managed_config()), snapshot)
        .expect("rebuild mv");
    let runtime_mv = rebuilt_mv
        .table("analytics", "orders")
        .expect("runtime")
        .clone();
    assert_eq!(
        runtime_mv.table.kind,
        ManagedTableKind::MaterializedView
    );
}
```

(Adjust the import of `ManagedTableKind` if needed: `use super::store::ManagedTableKind;`.)

- [ ] **Step 2: Run the test, confirm it fails**

```bash
cd /Users/harbor/project/NovaRocks
cargo test --lib standalone::lake::catalog::tests::rebuild_preserves_kind_column -- --nocapture
```

Expected: FAIL — compile error because existing literal constructions of `StoredManagedTable` don't include `kind`, or a test-helper `snapshot_seed` doesn't supply it.

- [ ] **Step 3: Update every `StoredManagedTable` literal**

Run:

```bash
cd /Users/harbor/project/NovaRocks
rg -l "StoredManagedTable \{" src/
```

For each file in the result, add `kind: ManagedTableKind::Table,` to the literal (or `ManagedTableKind::MaterializedView` where intentional). Expected touch points include:

- `src/standalone/lake/store.rs` test helpers (`snapshot_seed`).
- `src/standalone/lake/catalog.rs` test helpers if any inline.
- `src/standalone/lake/ddl.rs`'s `create_managed_table` (this path is `ManagedTableKind::Table`).

In `src/standalone/lake/ddl.rs::create_managed_table`, locate the `snapshot.tables.push(StoredManagedTable { ... })` or equivalent and add `kind: ManagedTableKind::Table,`.

- [ ] **Step 4: If `ManagedTableRuntime` exposes `table: StoredManagedTable`, nothing else is needed; verify**

Grep:

```bash
cd /Users/harbor/project/NovaRocks
rg "pub(?:\\(crate\\))? (?:struct|table:) ManagedTableRuntime" src/standalone/lake/catalog.rs
```

If the struct stores the full `StoredManagedTable`, then `runtime.table.kind` is available downstream without further changes. If it stores flattened fields, add a `pub(crate) kind: ManagedTableKind` mirror and populate it from the snapshot in rebuild.

- [ ] **Step 5: Run the test to confirm it passes**

```bash
cd /Users/harbor/project/NovaRocks
cargo test --lib standalone::lake::catalog::tests::rebuild_preserves_kind_column -- --nocapture
cargo test --lib standalone::lake -- --nocapture
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
cd /Users/harbor/project/NovaRocks
git add src/standalone/lake/catalog.rs src/standalone/lake/store.rs src/standalone/lake/ddl.rs
git commit -m "feat: thread managed table kind through rebuild and ddl"
```

---

## Task 5: Parse `CREATE MATERIALIZED VIEW` statement

**Files:**
- Modify: `src/sql/parser/ast/mod.rs` (new `Statement` variant)
- Create: `src/sql/parser/dialect/materialized_view.rs`
- Modify: `src/sql/parser/dialect/mod.rs` (route `CREATE MATERIALIZED VIEW` to the new submodule)
- Test: `src/sql/parser/dialect/materialized_view.rs` inline

- [ ] **Step 1: Write failing parser tests**

In `src/sql/parser/dialect/materialized_view.rs` (created later in Step 3), stub the module reference in `dialect/mod.rs` and write these tests (place them in a `#[cfg(test)] mod tests` block that references the parse entry point):

```rust
#[cfg(test)]
mod tests {
    use crate::sql::parser::ast::Statement;
    use crate::sql::parser::parse_sql;

    fn parse_one(sql: &str) -> Statement {
        let mut stmts = parse_sql(sql).expect("parse ok");
        assert_eq!(stmts.len(), 1, "exactly one stmt");
        stmts.pop().unwrap()
    }

    #[test]
    fn parse_create_mv_with_distributed_by_and_refresh_deferred_manual() {
        let stmt = parse_one(
            "CREATE MATERIALIZED VIEW analytics.orders_mv \
             DISTRIBUTED BY HASH(k1) BUCKETS 4 \
             REFRESH DEFERRED MANUAL \
             AS SELECT k1, sum(v2) AS total \
                 FROM iceberg_cat.ns.orders \
                 GROUP BY k1",
        );
        let mv = match stmt {
            Statement::CreateMaterializedView(mv) => mv,
            other => panic!("unexpected stmt: {other:?}"),
        };
        assert_eq!(mv.name.parts, vec!["analytics", "orders_mv"]);
        assert!(!mv.if_not_exists);
        assert_eq!(
            mv.distribution
                .as_ref()
                .expect("distribution clause")
                .hash_columns,
            vec!["k1".to_string()],
        );
        assert_eq!(
            mv.distribution
                .as_ref()
                .expect("distribution clause")
                .bucket_count,
            Some(4)
        );
        assert!(mv.refresh_manual_explicit);
    }

    #[test]
    fn parse_create_mv_with_if_not_exists_and_comment_and_properties_ignored() {
        let stmt = parse_one(
            "CREATE MATERIALIZED VIEW IF NOT EXISTS mv1 \
             COMMENT 'demo' \
             DISTRIBUTED BY HASH(k1) BUCKETS 2 \
             PROPERTIES('storage_volume' = 'svc', 'replication_num' = '1') \
             AS SELECT k1 FROM iceberg_cat.ns.orders",
        );
        let mv = match stmt {
            Statement::CreateMaterializedView(mv) => mv,
            other => panic!("unexpected stmt: {other:?}"),
        };
        assert!(mv.if_not_exists);
        assert_eq!(mv.name.parts, vec!["mv1"]);
        // Comment and properties are parsed-then-dropped, so the AST exposes
        // no storage slot for them in Phase 1.
    }

    #[test]
    fn parse_create_mv_rejects_partition_by() {
        let err = crate::sql::parser::parse_sql(
            "CREATE MATERIALIZED VIEW mv1 \
             PARTITION BY k1 \
             DISTRIBUTED BY HASH(k1) BUCKETS 1 \
             AS SELECT k1 FROM iceberg_cat.ns.orders",
        )
        .expect_err("should reject");
        assert!(
            err.to_lowercase().contains("partition by"),
            "unexpected err: {err}"
        );
    }

    #[test]
    fn parse_create_mv_rejects_order_by() {
        let err = crate::sql::parser::parse_sql(
            "CREATE MATERIALIZED VIEW mv1 \
             DISTRIBUTED BY HASH(k1) BUCKETS 1 \
             ORDER BY (k1) \
             AS SELECT k1 FROM iceberg_cat.ns.orders",
        )
        .expect_err("should reject");
        assert!(
            err.to_lowercase().contains("order by"),
            "unexpected err: {err}"
        );
    }

    #[test]
    fn parse_create_mv_rejects_refresh_async() {
        let err = crate::sql::parser::parse_sql(
            "CREATE MATERIALIZED VIEW mv1 \
             DISTRIBUTED BY HASH(k1) BUCKETS 1 \
             REFRESH ASYNC \
             AS SELECT k1 FROM iceberg_cat.ns.orders",
        )
        .expect_err("should reject");
        assert!(
            err.to_lowercase().contains("refresh async")
                || err.to_lowercase().contains("not supported"),
            "unexpected err: {err}"
        );
    }

    #[test]
    fn parse_create_mv_rejects_refresh_immediate() {
        let err = crate::sql::parser::parse_sql(
            "CREATE MATERIALIZED VIEW mv1 \
             DISTRIBUTED BY HASH(k1) BUCKETS 1 \
             REFRESH IMMEDIATE \
             AS SELECT k1 FROM iceberg_cat.ns.orders",
        )
        .expect_err("should reject");
        assert!(
            err.to_lowercase().contains("immediate")
                || err.to_lowercase().contains("not supported"),
            "unexpected err: {err}"
        );
    }

    #[test]
    fn parse_create_mv_requires_distributed_by() {
        let err = crate::sql::parser::parse_sql(
            "CREATE MATERIALIZED VIEW mv1 AS SELECT k1 FROM iceberg_cat.ns.orders",
        )
        .expect_err("should reject");
        assert!(
            err.to_lowercase().contains("distributed by"),
            "unexpected err: {err}"
        );
    }
}
```

- [ ] **Step 2: Run these tests, confirm they fail**

```bash
cd /Users/harbor/project/NovaRocks
cargo test --lib sql::parser::dialect::materialized_view::tests -- --nocapture
```

Expected: FAIL — compile error, neither `Statement::CreateMaterializedView` nor the module exists.

- [ ] **Step 3: Add the AST variant**

In `src/sql/parser/ast/mod.rs`, near other `Statement` variants, add:

```rust
#[derive(Clone, Debug, PartialEq)]
pub struct MaterializedViewDistribution {
    pub hash_columns: Vec<String>,
    pub bucket_count: Option<u32>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct CreateMaterializedViewStmt {
    pub name: ObjectName,
    pub if_not_exists: bool,
    pub distribution: Option<MaterializedViewDistribution>,
    pub refresh_manual_explicit: bool,
    pub select_sql: String,       // the raw body after `AS`, normalized (trimmed, single-spaced)
    pub select_query: sqlparser::ast::Query,
}

#[derive(Clone, Debug, PartialEq)]
pub struct DropMaterializedViewStmt {
    pub name: ObjectName,
    pub if_exists: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub struct RefreshMaterializedViewStmt {
    pub name: ObjectName,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ShowMaterializedViewsStmt {
    pub database: Option<String>,
}
```

Inside the main `Statement` enum add four variants:

```rust
Statement::CreateMaterializedView(CreateMaterializedViewStmt),
Statement::DropMaterializedView(DropMaterializedViewStmt),
Statement::RefreshMaterializedView(RefreshMaterializedViewStmt),
Statement::ShowMaterializedViews(ShowMaterializedViewsStmt),
```

Re-export the structs at the top of the AST module per the file's existing convention.

- [ ] **Step 4: Implement the parser for CREATE MATERIALIZED VIEW**

Create `src/sql/parser/dialect/materialized_view.rs` with:

```rust
//! Parsing for `CREATE / DROP / REFRESH / SHOW MATERIALIZED VIEW[S]` statements.
//!
//! Only the Phase 1 subset is accepted; unsupported clauses are rejected with
//! an explicit error so that users pasting StarRocks DDL see a clear signal.

use sqlparser::dialect::Dialect;
use sqlparser::keywords::Keyword;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::Token;

use crate::sql::parser::ast::{
    CreateMaterializedViewStmt, DropMaterializedViewStmt, MaterializedViewDistribution,
    ObjectName, RefreshMaterializedViewStmt, ShowMaterializedViewsStmt, Statement,
};

pub(crate) fn try_parse_create_materialized_view(
    parser: &mut Parser,
) -> Result<Option<Statement>, String> {
    // Caller has already consumed CREATE. Decide whether the next tokens are
    // `MATERIALIZED VIEW`.
    let savepoint = parser.index();
    if !parser.parse_keyword(Keyword::MATERIALIZED) {
        parser.reset(savepoint);
        return Ok(None);
    }
    if !parser.parse_keyword(Keyword::VIEW) {
        return Err("expected VIEW after MATERIALIZED".to_string());
    }

    let if_not_exists = parser.parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
    let name = parse_object_name(parser)?;

    // Optional COMMENT 'string' (parsed and dropped).
    if parser.parse_keyword(Keyword::COMMENT) {
        parser
            .parse_literal_string()
            .map_err(|e| format!("parse MV comment failed: {e}"))?;
    }

    // Reject PARTITION BY up-front.
    if parser.parse_keywords(&[Keyword::PARTITION, Keyword::BY]) {
        return Err(
            "PARTITION BY is not supported on materialized views yet".to_string(),
        );
    }

    // Required DISTRIBUTED BY clause.
    let distribution = parse_distributed_by(parser)?;
    if distribution.is_none() {
        return Err(
            "CREATE MATERIALIZED VIEW requires a DISTRIBUTED BY HASH(...) BUCKETS n clause"
                .to_string(),
        );
    }

    // Optional REFRESH clause.
    let refresh_manual_explicit = if parser.parse_keyword(Keyword::REFRESH) {
        parse_refresh_clause(parser)?
    } else {
        false
    };

    // Reject ORDER BY (after refresh, mirroring StarRocks ordering).
    if parser.parse_keywords(&[Keyword::ORDER, Keyword::BY]) {
        return Err("ORDER BY is not supported on materialized views yet".to_string());
    }

    // Optional PROPERTIES(...) — parsed and dropped.
    if parser.parse_keyword(Keyword::PROPERTIES) {
        parser
            .expect_token(&Token::LParen)
            .map_err(|e| format!("expected ( after PROPERTIES: {e}"))?;
        loop {
            if parser.consume_token(&Token::RParen) {
                break;
            }
            let _key = parser
                .parse_literal_string()
                .map_err(|e| format!("parse MV property key failed: {e}"))?;
            parser
                .expect_token(&Token::Eq)
                .map_err(|e| format!("expected = in MV property: {e}"))?;
            let _val = parser
                .parse_literal_string()
                .map_err(|e| format!("parse MV property value failed: {e}"))?;
            if !parser.consume_token(&Token::Comma) {
                parser
                    .expect_token(&Token::RParen)
                    .map_err(|e| format!("expected , or ) in MV properties: {e}"))?;
                break;
            }
        }
    }

    parser
        .expect_keyword(Keyword::AS)
        .map_err(|e| format!("expected AS before MV query: {e}"))?;
    let start_index = parser.index();
    let query = parser
        .parse_query()
        .map_err(|e| format!("parse MV query failed: {e}"))?;
    let end_index = parser.index();
    let select_sql = slice_tokens_as_string(parser, start_index, end_index);

    Ok(Some(Statement::CreateMaterializedView(
        CreateMaterializedViewStmt {
            name,
            if_not_exists,
            distribution,
            refresh_manual_explicit,
            select_sql,
            select_query: *query,
        },
    )))
}

fn parse_distributed_by(
    parser: &mut Parser,
) -> Result<Option<MaterializedViewDistribution>, String> {
    if !parser.parse_keywords(&[Keyword::DISTRIBUTED, Keyword::BY]) {
        return Ok(None);
    }
    parser
        .expect_keyword(Keyword::HASH)
        .map_err(|e| format!("expected HASH after DISTRIBUTED BY: {e}"))?;
    parser
        .expect_token(&Token::LParen)
        .map_err(|e| format!("expected ( after HASH: {e}"))?;
    let mut hash_columns = Vec::new();
    loop {
        let ident = parser
            .parse_identifier()
            .map_err(|e| format!("parse hash column failed: {e}"))?;
        hash_columns.push(ident.value);
        if parser.consume_token(&Token::RParen) {
            break;
        }
        parser
            .expect_token(&Token::Comma)
            .map_err(|e| format!("expected , or ) in hash column list: {e}"))?;
    }
    let bucket_count = if parser.parse_keyword(Keyword::BUCKETS) {
        let value = parser
            .parse_literal_uint()
            .map_err(|e| format!("parse BUCKETS count failed: {e}"))?;
        Some(value as u32)
    } else {
        None
    };
    Ok(Some(MaterializedViewDistribution {
        hash_columns,
        bucket_count,
    }))
}

fn parse_refresh_clause(parser: &mut Parser) -> Result<bool, String> {
    // `REFRESH` already consumed.
    if parser.parse_keyword(Keyword::IMMEDIATE) {
        return Err("REFRESH IMMEDIATE is not supported yet".to_string());
    }
    if parser.parse_keyword(Keyword::ASYNC) {
        return Err("REFRESH ASYNC is not supported yet".to_string());
    }
    parser
        .expect_keyword(Keyword::DEFERRED)
        .map_err(|e| format!("expected REFRESH DEFERRED MANUAL: {e}"))?;
    parser
        .expect_keyword(Keyword::MANUAL)
        .map_err(|e| format!("expected REFRESH DEFERRED MANUAL: {e}"))?;
    Ok(true)
}

fn parse_object_name(parser: &mut Parser) -> Result<ObjectName, String> {
    let parts = parser
        .parse_object_name(false)
        .map_err(|e| format!("parse mv name failed: {e}"))?
        .0
        .into_iter()
        .map(|ident| ident.value)
        .collect();
    Ok(ObjectName { parts })
}

fn slice_tokens_as_string(parser: &Parser, start: usize, end: usize) -> String {
    // Reconstruct a whitespace-normalized SQL fragment from the token slice.
    let mut out = String::new();
    for i in start..end {
        let token = parser.token_at(i);
        if let Some(tok) = token {
            if !out.is_empty() {
                out.push(' ');
            }
            out.push_str(&tok.to_string());
        }
    }
    out
}
```

Notes:

- `Parser::index()`, `Parser::reset(idx)`, `Parser::token_at(i)` may or may not exist in the local sqlparser vendor; if not, use the existing "raw SQL slice" strategy already applied in `src/sql/parser/dialect/mod.rs` (see how `CREATE TABLE ... PROPERTIES(...)` slices its body). The goal is: preserve the raw SELECT body text from `AS` to end-of-statement so we can store it in `select_sql`.
- If the local sqlparser does not expose the needed parser hooks, fall back to a regex-lite "find AS, take everything else" approach consistent with existing parser handling elsewhere in the dialect.

- [ ] **Step 5: Wire the new parser into `src/sql/parser/dialect/mod.rs`**

Locate the `CREATE` dispatch in the dialect's top-level `parse_statement` (or equivalent function that routes to `parse_create_table`). Before trying `parse_create_table`, attempt the MV path:

```rust
if let Some(stmt) = super::materialized_view::try_parse_create_materialized_view(parser)? {
    return Ok(stmt);
}
```

Add a `mod materialized_view;` declaration.

- [ ] **Step 6: Run the parser tests to confirm they pass**

```bash
cd /Users/harbor/project/NovaRocks
cargo test --lib sql::parser::dialect::materialized_view::tests -- --nocapture
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
cd /Users/harbor/project/NovaRocks
git add src/sql/parser/
git commit -m "feat: parse CREATE MATERIALIZED VIEW with phase-1 clause restrictions"
```

---

## Task 6: Parse `DROP / REFRESH / SHOW MATERIALIZED VIEW[S]` statements

**Files:**
- Modify: `src/sql/parser/dialect/materialized_view.rs`
- Modify: `src/sql/parser/dialect/mod.rs` (dispatch the DROP / REFRESH / SHOW keywords)
- Test: inline in `src/sql/parser/dialect/materialized_view.rs`

- [ ] **Step 1: Append failing parser tests**

```rust
#[test]
fn parse_drop_mv_with_if_exists() {
    let stmt = parse_one("DROP MATERIALIZED VIEW IF EXISTS analytics.mv1");
    let drop = match stmt {
        Statement::DropMaterializedView(d) => d,
        other => panic!("unexpected: {other:?}"),
    };
    assert!(drop.if_exists);
    assert_eq!(drop.name.parts, vec!["analytics", "mv1"]);
}

#[test]
fn parse_drop_mv_rejects_force() {
    let err = crate::sql::parser::parse_sql("DROP MATERIALIZED VIEW mv1 FORCE")
        .expect_err("should reject");
    assert!(err.to_lowercase().contains("force"), "err={err}");
}

#[test]
fn parse_refresh_mv() {
    let stmt = parse_one("REFRESH MATERIALIZED VIEW analytics.mv1");
    match stmt {
        Statement::RefreshMaterializedView(r) => {
            assert_eq!(r.name.parts, vec!["analytics", "mv1"]);
        }
        other => panic!("unexpected: {other:?}"),
    }
}

#[test]
fn parse_refresh_mv_rejects_partition_range() {
    let err = crate::sql::parser::parse_sql(
        "REFRESH MATERIALIZED VIEW mv1 PARTITION START ('2024-01-01') END ('2024-02-01')",
    )
    .expect_err("should reject");
    assert!(
        err.to_lowercase().contains("partition") || err.to_lowercase().contains("not supported"),
        "err={err}"
    );
}

#[test]
fn parse_refresh_mv_rejects_async_modifier() {
    let err = crate::sql::parser::parse_sql(
        "REFRESH MATERIALIZED VIEW mv1 WITH ASYNC MODE",
    )
    .expect_err("should reject");
    assert!(err.to_lowercase().contains("async") || err.to_lowercase().contains("not supported"),
        "err={err}");
}

#[test]
fn parse_show_materialized_views_no_filters() {
    let stmt = parse_one("SHOW MATERIALIZED VIEWS");
    match stmt {
        Statement::ShowMaterializedViews(s) => assert!(s.database.is_none()),
        other => panic!("unexpected: {other:?}"),
    }
}

#[test]
fn parse_show_materialized_views_from_db() {
    let stmt = parse_one("SHOW MATERIALIZED VIEWS FROM analytics");
    match stmt {
        Statement::ShowMaterializedViews(s) => {
            assert_eq!(s.database, Some("analytics".to_string()))
        }
        other => panic!("unexpected: {other:?}"),
    }
}

#[test]
fn parse_show_materialized_views_rejects_like_and_where() {
    let err_like = crate::sql::parser::parse_sql("SHOW MATERIALIZED VIEWS LIKE '%mv%'")
        .expect_err("should reject LIKE");
    assert!(err_like.to_lowercase().contains("like") || err_like.to_lowercase().contains("not supported"),
        "err={err_like}");
    let err_where = crate::sql::parser::parse_sql("SHOW MATERIALIZED VIEWS WHERE name = 'mv1'")
        .expect_err("should reject WHERE");
    assert!(err_where.to_lowercase().contains("where") || err_where.to_lowercase().contains("not supported"),
        "err={err_where}");
}
```

- [ ] **Step 2: Run tests to verify failure**

```bash
cd /Users/harbor/project/NovaRocks
cargo test --lib sql::parser::dialect::materialized_view::tests -- --nocapture
```

Expected: FAIL — compile or runtime errors for the new variants' parsing.

- [ ] **Step 3: Implement DROP / REFRESH / SHOW parsers**

Append to `src/sql/parser/dialect/materialized_view.rs`:

```rust
pub(crate) fn try_parse_drop_materialized_view(
    parser: &mut Parser,
) -> Result<Option<Statement>, String> {
    let savepoint = parser.index();
    if !parser.parse_keyword(Keyword::MATERIALIZED) {
        parser.reset(savepoint);
        return Ok(None);
    }
    if !parser.parse_keyword(Keyword::VIEW) {
        return Err("expected VIEW after MATERIALIZED".to_string());
    }
    let if_exists = parser.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);
    let name = parse_object_name(parser)?;
    if parser.parse_keyword(Keyword::FORCE) {
        return Err("DROP MATERIALIZED VIEW ... FORCE is not supported".to_string());
    }
    Ok(Some(Statement::DropMaterializedView(
        DropMaterializedViewStmt { name, if_exists },
    )))
}

pub(crate) fn try_parse_refresh_materialized_view(
    parser: &mut Parser,
) -> Result<Option<Statement>, String> {
    // Caller has already consumed REFRESH.
    if !parser.parse_keyword(Keyword::MATERIALIZED) {
        return Err("expected MATERIALIZED after REFRESH".to_string());
    }
    if !parser.parse_keyword(Keyword::VIEW) {
        return Err("expected VIEW after MATERIALIZED".to_string());
    }
    let name = parse_object_name(parser)?;
    if parser.parse_keyword(Keyword::PARTITION) {
        return Err(
            "REFRESH MATERIALIZED VIEW ... PARTITION START(...) END(...) is not supported yet"
                .to_string(),
        );
    }
    if parser.parse_keyword(Keyword::WITH) {
        return Err(
            "REFRESH MATERIALIZED VIEW ... WITH {SYNC|ASYNC} MODE is not supported yet"
                .to_string(),
        );
    }
    Ok(Some(Statement::RefreshMaterializedView(
        RefreshMaterializedViewStmt { name },
    )))
}

pub(crate) fn try_parse_show_materialized_views(
    parser: &mut Parser,
) -> Result<Option<Statement>, String> {
    // Caller has already consumed SHOW.
    if !parser.parse_keyword(Keyword::MATERIALIZED) {
        return Ok(None);
    }
    if !parser.parse_keyword(Keyword::VIEWS) {
        return Err("expected VIEWS after MATERIALIZED".to_string());
    }
    let database = if parser.parse_keyword(Keyword::FROM) {
        let ident = parser
            .parse_identifier()
            .map_err(|e| format!("parse database name after FROM: {e}"))?;
        Some(ident.value)
    } else {
        None
    };
    if parser.parse_keyword(Keyword::LIKE) {
        return Err(
            "SHOW MATERIALIZED VIEWS LIKE '...' is not supported yet".to_string(),
        );
    }
    if parser.parse_keyword(Keyword::WHERE) {
        return Err(
            "SHOW MATERIALIZED VIEWS WHERE ... is not supported yet".to_string(),
        );
    }
    Ok(Some(Statement::ShowMaterializedViews(
        ShowMaterializedViewsStmt { database },
    )))
}
```

- [ ] **Step 4: Wire the dispatches in `src/sql/parser/dialect/mod.rs`**

- In the `DROP` branch of `parse_statement`, before falling through to the existing `DROP TABLE`/`DROP DATABASE` handling, call `try_parse_drop_materialized_view`.
- In the `REFRESH` branch (if it does not exist, add one that peeks for `REFRESH`), call `try_parse_refresh_materialized_view`.
- In the `SHOW` branch, before the existing `SHOW TABLES` handling, call `try_parse_show_materialized_views`.

- [ ] **Step 5: Run parser tests, confirm PASS**

```bash
cd /Users/harbor/project/NovaRocks
cargo test --lib sql::parser::dialect::materialized_view::tests -- --nocapture
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
cd /Users/harbor/project/NovaRocks
git add src/sql/parser/
git commit -m "feat: parse DROP/REFRESH/SHOW MATERIALIZED VIEW statements"
```

---

## Task 7: `lake/mv_ddl.rs` — CREATE / DROP / SHOW helpers

**Files:**
- Create: `src/standalone/lake/mv_ddl.rs`
- Modify: `src/standalone/lake/mod.rs` (register new module)
- Test: inline in `src/standalone/lake/mv_ddl.rs`

- [ ] **Step 1: Write failing unit tests**

Create `src/standalone/lake/mv_ddl.rs`:

```rust
//! Engine-boundary helpers for CREATE / DROP / SHOW MATERIALIZED VIEW.
//!
//! REFRESH lives in `mv_refresh.rs` because it needs the query executor.

// The actual implementation lives below the tests.

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_base_table_refs_rejects_non_iceberg_tables() {
        // Simulate an analyzer output where the FROM list contains a managed
        // lake table. extract_base_table_refs should error out with a clear
        // message.
        let err = extract_base_table_refs(&[ResolvedTableRef::ManagedLake {
            database: "analytics".to_string(),
            table: "orders_raw".to_string(),
        }])
        .expect_err("should reject non-iceberg");
        assert!(err.contains("Iceberg"), "err={err}");
    }

    #[test]
    fn extract_base_table_refs_returns_iceberg_fqns() {
        let refs = extract_base_table_refs(&[
            ResolvedTableRef::Iceberg {
                catalog: "iceberg_cat".to_string(),
                namespace: "ns".to_string(),
                table: "orders".to_string(),
            },
            ResolvedTableRef::Iceberg {
                catalog: "iceberg_cat".to_string(),
                namespace: "ns".to_string(),
                table: "items".to_string(),
            },
        ])
        .expect("ok");
        assert_eq!(refs.len(), 2);
        assert_eq!(refs[0].fqn(), "iceberg_cat.ns.orders");
    }
}
```

- [ ] **Step 2: Run the failing tests**

```bash
cd /Users/harbor/project/NovaRocks
cargo test --lib standalone::lake::mv_ddl -- --nocapture
```

Expected: FAIL — module not registered, types undefined.

- [ ] **Step 3: Implement `mv_ddl.rs`**

Replace the stub above with the real implementation:

```rust
use std::sync::Arc;

use crate::sql::parser::ast::{
    CreateMaterializedViewStmt, DropMaterializedViewStmt, MaterializedViewDistribution,
    ObjectName, ShowMaterializedViewsStmt,
};

use super::super::engine::{
    QueryResult, QueryResultColumn, StandaloneState, StatementResult,
    build_string_query_result,
};
use super::catalog::register_managed_table_in_catalog;
use super::config::ManagedLakeConfig;
use super::ddl::drop_managed_table as drop_managed_table_impl;
use super::store::{
    IcebergTableRef, ManagedMvRefreshMode, ManagedSnapshot, ManagedTableKind,
    StoredMaterializedView,
};

/// Resolved base-table reference as the MV analyzer stage returns it.
/// Only the `Iceberg` variant is allowed; anything else fails validation.
#[derive(Clone, Debug)]
pub(crate) enum ResolvedTableRef {
    Iceberg {
        catalog: String,
        namespace: String,
        table: String,
    },
    ManagedLake {
        database: String,
        table: String,
    },
}

pub(crate) fn extract_base_table_refs(
    resolved: &[ResolvedTableRef],
) -> Result<Vec<IcebergTableRef>, String> {
    let mut out = Vec::new();
    for r in resolved {
        match r {
            ResolvedTableRef::Iceberg {
                catalog,
                namespace,
                table,
            } => out.push(IcebergTableRef {
                catalog: catalog.clone(),
                namespace: namespace.clone(),
                table: table.clone(),
            }),
            ResolvedTableRef::ManagedLake { database, table } => {
                return Err(format!(
                    "materialized view base tables must be Iceberg tables; \
                     found managed lake table `{database}.{table}`"
                ));
            }
        }
    }
    Ok(out)
}

pub(crate) fn create_mv(
    state: &Arc<StandaloneState>,
    current_database: &str,
    stmt: &CreateMaterializedViewStmt,
) -> Result<StatementResult, String> {
    // 1. Resolve the MV target (db, name).
    let (db_name, mv_name) = resolve_mv_name(&stmt.name, current_database)?;

    // 2. Analyze the SELECT: resolve base tables, derive output schema.
    //    The analyzer is reused via the existing `crate::sql::analyzer::analyze`
    //    entrypoint; `analyze_mv_select` below adapts its output by walking the
    //    resolved plan for scan leaves and pulling the top-level output
    //    columns.
    let analysis = analyze_mv_select(&stmt.select_query, state, current_database)?;
    let base_refs = extract_base_table_refs(&analysis.resolved_refs)?;

    // 3. Validate distribution clause against output schema.
    let distribution = stmt
        .distribution
        .as_ref()
        .ok_or_else(|| "CREATE MATERIALIZED VIEW requires DISTRIBUTED BY".to_string())?;
    let bucket_count = distribution.bucket_count.ok_or_else(|| {
        "DISTRIBUTED BY HASH(...) BUCKETS n is required (BUCKETS <n> is mandatory in phase 1)"
            .to_string()
    })?;
    validate_distribution_columns(distribution, &analysis.output_columns)?;

    // 4. Reject IF NOT EXISTS collision / non-collision via the existing
    //    catalog path. If the name exists, short-circuit per spec §6.2.
    if state
        .catalog
        .read()
        .expect("standalone catalog read lock")
        .get(&db_name, &mv_name)
        .is_ok()
    {
        if stmt.if_not_exists {
            return Ok(StatementResult::Ok);
        }
        return Err(format!(
            "materialized view or table already exists: {db_name}.{mv_name}"
        ));
    }

    // 5. Allocate & persist in one sqlite txn; rebuild managed-lake catalog.
    let managed_config = state
        .managed_lake_config
        .clone()
        .ok_or_else(|| "standalone managed lake config is missing".to_string())?;
    let metadata_store = state
        .metadata_store
        .as_ref()
        .ok_or_else(|| "managed lake create materialized view requires sqlite metadata store"
            .to_string())?;

    let mut snapshot = metadata_store.load_snapshot()?.managed;
    let new_snapshot = plan_create_mv_snapshot(
        &mut snapshot,
        &managed_config,
        &db_name,
        &mv_name,
        bucket_count,
        &distribution.hash_columns,
        &analysis.output_columns,
        &base_refs,
        &stmt.select_sql,
    )?;
    metadata_store.replace_managed_snapshot(&mut snapshot)?;

    // 6. Bootstrap empty tablets on object storage for p0 (reuse existing
    //    bootstrap_empty_partition-style helper in ddl.rs).
    bootstrap_mv_initial_tablets(state, &new_snapshot, &managed_config)?;

    // 7. Rebuild in-process catalog so SELECT sees the MV.
    refresh_catalog_view(state)?;

    Ok(StatementResult::Ok)
}

pub(crate) fn drop_mv(
    state: &Arc<StandaloneState>,
    current_database: &str,
    stmt: &DropMaterializedViewStmt,
) -> Result<StatementResult, String> {
    let (db_name, mv_name) = resolve_mv_name(&stmt.name, current_database)?;

    // Verify the target is a materialized view; a kind='TABLE' row with the
    // same name must not be deletable via this path.
    let managed = state
        .managed_lake
        .read()
        .expect("standalone managed lake read lock");
    let runtime = match managed.table(&db_name, &mv_name) {
        Ok(r) => r.clone(),
        Err(_) => {
            if stmt.if_exists {
                return Ok(StatementResult::Ok);
            }
            return Err(format!(
                "materialized view does not exist: {db_name}.{mv_name}"
            ));
        }
    };
    drop(managed);

    if runtime.table.kind != ManagedTableKind::MaterializedView {
        return Err(format!(
            "`{db_name}.{mv_name}` is not a materialized view; use DROP TABLE instead"
        ));
    }

    drop_managed_table_impl(state, &db_name, &mv_name)?;
    Ok(StatementResult::Ok)
}

pub(crate) fn list_mvs(
    state: &Arc<StandaloneState>,
    stmt: &ShowMaterializedViewsStmt,
) -> Result<StatementResult, String> {
    let metadata_store = state
        .metadata_store
        .as_ref()
        .ok_or_else(|| "managed lake show mvs requires sqlite metadata store".to_string())?;
    let snapshot = metadata_store.load_snapshot()?.managed;

    let rows = snapshot
        .materialized_views
        .iter()
        .filter_map(|mv| {
            let table = snapshot
                .tables
                .iter()
                .find(|t| t.table_id == mv.mv_id && t.kind == ManagedTableKind::MaterializedView)?;
            if table.state != super::store::ManagedTableState::Active {
                return None;
            }
            let database = snapshot
                .databases
                .iter()
                .find(|d| d.db_id == table.db_id)
                .map(|d| d.name.clone())
                .unwrap_or_default();
            if let Some(filter) = stmt.database.as_deref() {
                if !database.eq_ignore_ascii_case(filter) {
                    return None;
                }
            }
            let base_fqns = mv
                .base_table_refs
                .iter()
                .map(|r| r.fqn())
                .collect::<Vec<_>>()
                .join(", ");
            Some(vec![
                table.name.clone(),
                database,
                mv.refresh_mode.as_sql_str_owned(),
                mv.last_refresh_ms
                    .map(|ms| ms.to_string())
                    .unwrap_or_default(),
                mv.last_refresh_rows
                    .map(|n| n.to_string())
                    .unwrap_or_default(),
                base_fqns,
                mv.select_sql.clone(),
            ])
        })
        .collect::<Vec<_>>();

    let columns = [
        "Name",
        "Database",
        "RefreshMode",
        "LastRefreshTime",
        "LastRefreshRows",
        "BaseTables",
        "SelectText",
    ];
    let query_result = build_mv_rows_result(&columns, rows)?;
    Ok(StatementResult::Query(query_result))
}

// --- helpers below; their concrete shape depends on the analyzer API that
// --- this plan's spec leaves open. Either extend the analyzer to expose the
// --- resolved refs + output columns, or add a small adapter in this file.

struct MvAnalysis {
    resolved_refs: Vec<ResolvedTableRef>,
    output_columns: Vec<MvOutputColumn>,
}

struct MvOutputColumn {
    name: String,
    logical_type: String,
    nullable: bool,
}

fn analyze_mv_select(
    query: &sqlparser::ast::Query,
    state: &Arc<StandaloneState>,
    current_database: &str,
) -> Result<MvAnalysis, String> {
    // Use the existing analyzer. The function returns a resolved plan from
    // which we can walk the scan leaves and read their table kinds.
    let catalog = state.catalog.read().expect("standalone catalog read lock");
    let (resolved, _cte_registry) =
        crate::sql::analyzer::analyze(query, &*catalog, current_database)?;

    // Extract the output schema of the top-level select.
    let output_columns = resolved
        .output_columns()
        .iter()
        .map(|col| MvOutputColumn {
            name: col.name.clone(),
            logical_type: col.logical_type_string(),
            nullable: col.nullable,
        })
        .collect();

    // Walk resolved scans to classify base-table kinds.
    let resolved_refs = resolved
        .scans()
        .iter()
        .map(|scan| classify_scan(scan))
        .collect::<Result<Vec<_>, _>>()?;

    Ok(MvAnalysis {
        resolved_refs,
        output_columns,
    })
}

fn classify_scan(
    scan: &crate::sql::analyzer::ResolvedScan,
) -> Result<ResolvedTableRef, String> {
    match scan.source() {
        crate::sql::analyzer::ResolvedScanSource::IcebergTable {
            catalog,
            namespace,
            table,
        } => Ok(ResolvedTableRef::Iceberg {
            catalog: catalog.clone(),
            namespace: namespace.clone(),
            table: table.clone(),
        }),
        crate::sql::analyzer::ResolvedScanSource::ManagedLakeTable { database, table } => {
            Ok(ResolvedTableRef::ManagedLake {
                database: database.clone(),
                table: table.clone(),
            })
        }
        other => Err(format!(
            "materialized view base table kind not yet supported: {other:?}"
        )),
    }
}

fn resolve_mv_name(
    name: &ObjectName,
    current_database: &str,
) -> Result<(String, String), String> {
    use super::super::engine::catalog::normalize_identifier;
    match name.parts.as_slice() {
        [table] => Ok((
            normalize_identifier(current_database)?,
            normalize_identifier(table)?,
        )),
        [database, table] => Ok((
            normalize_identifier(database)?,
            normalize_identifier(table)?,
        )),
        _ => Err(format!(
            "materialized view name must be `<name>` or `<db>.<name>`; got `{}`",
            name.parts.join(".")
        )),
    }
}

fn validate_distribution_columns(
    distribution: &MaterializedViewDistribution,
    output_columns: &[MvOutputColumn],
) -> Result<(), String> {
    for col in &distribution.hash_columns {
        let exists = output_columns
            .iter()
            .any(|c| c.name.eq_ignore_ascii_case(col));
        if !exists {
            return Err(format!(
                "DISTRIBUTED BY column `{col}` not in MV output schema"
            ));
        }
    }
    Ok(())
}

// The rest of the helpers (plan_create_mv_snapshot, bootstrap_mv_initial_tablets,
// refresh_catalog_view, build_mv_rows_result, as_sql_str_owned,
// ManagedMvRefreshMode::as_sql_str_owned) are straightforward mirrors of
// equivalent managed-lake ddl.rs helpers. Implement them by pattern-matching
// against how create_managed_table allocates ids, inserts schema + columns,
// calls bootstrap_empty_partition-style helpers, and registers in catalog.
```

**Guidance for the implementer:**

- The `analyze_mv_select` function assumes the analyzer exposes `resolved.output_columns()` and `resolved.scans()`. If the current analyzer API differs, either extend the analyzer to expose these in a `#[derive(Debug)]`-friendly form, or add a small inspector. Document any analyzer-side additions in a short code comment.
- `build_mv_rows_result` should produce a `QueryResult` with the seven columns listed in spec §6.4. Reuse the same Arrow-string-array pattern as `build_string_query_result` in `engine/mod.rs`.

- [ ] **Step 4: Register `mv_ddl` in `src/standalone/lake/mod.rs`**

```rust
pub(crate) mod mv_ddl;
```

- [ ] **Step 5: Run unit tests, confirm PASS**

```bash
cd /Users/harbor/project/NovaRocks
cargo test --lib standalone::lake::mv_ddl -- --nocapture
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
cd /Users/harbor/project/NovaRocks
git add src/standalone/lake/mv_ddl.rs src/standalone/lake/mod.rs
git commit -m "feat: add managed lake mv_ddl module for CREATE/DROP/SHOW"
```

---

## Task 8: Generalize the managed-lake INSERT writer to target a staged partition

**Files:**
- Modify: `src/standalone/lake/txn.rs`
- Test: `src/standalone/lake/txn.rs` inline

- [ ] **Step 1: Identify the refactor boundaries**

Read `src/standalone/lake/txn.rs` end-to-end. The Phase 1 goal is:

- Extract the "given `Chunk`s + a resolved `ManagedInsertPlan`, run prepare/write/publish/visible" portion into a public helper:

  ```rust
  pub(crate) fn write_chunks_into_managed_partition(
      state: &Arc<StandaloneState>,
      plan: ManagedInsertPlan,
      chunks: &[Chunk],
  ) -> Result<i64 /* rows_written */, String>;
  ```

- Extend `load_insert_plan` to accept an optional staged partition override:

  ```rust
  enum PartitionTarget {
      Active,
      Staged { partition_id: i64, index_id: i64, tablet_ids: Vec<i64> },
  }

  fn load_insert_plan(
      state: &Arc<StandaloneState>,
      resolved: &ResolvedLocalTableName,
      target: PartitionTarget,
  ) -> Result<ManagedInsertPlan, String>;
  ```

  `PartitionTarget::Active` preserves today's behaviour; `PartitionTarget::Staged { .. }` swaps the partition/index/tablets used to assemble `ManagedInsertPlan`.

- The existing `insert_into_managed_lake_table` path passes `PartitionTarget::Active` explicitly so behaviour is unchanged.

- [ ] **Step 2: Write the failing test**

Append to `src/standalone/lake/txn.rs`:

```rust
#[cfg(test)]
mod mv_target_tests {
    use super::*;

    #[test]
    fn write_chunks_into_managed_partition_routes_rows_to_staged_tablets() {
        // Given a test StandaloneState with one MV-like table (2 buckets) and a
        // staged `CREATING` partition with 2 empty tablets on local fs, feeding
        // two rows whose bucket hash lands in different tablets produces a
        // per-tablet row count of {1,1}, advances visible_version to 2, and
        // returns rows_written = 2.
        //
        // Use the same in-memory MinIO-less harness as other txn.rs tests if
        // one exists; otherwise, add a local-filesystem adapter via
        // build_oss_operator with an s3://... URI that points at a temp dir.
        // If no such harness exists in the repo, mark this as a unit-level
        // contract test that only exercises the plan-building branch and put
        // end-to-end validation in Task 11 integration tests.

        // NOTE TO IMPLEMENTER: if mocking the object store is non-trivial in
        // this test harness, keep this test focused on the plan shape:
        // verify that load_insert_plan(..., PartitionTarget::Staged { .. })
        // returns a ManagedInsertPlan whose partition_id / index_id /
        // tablet_ids match the requested staged values. End-to-end write
        // correctness is covered by tests/standalone_mysql_server.rs (Task 11).
        let state = crate::standalone::lake::test_helpers::standalone_state_with_seed_mv();
        let staged = crate::standalone::lake::test_helpers::stage_mv_refresh_for_seed_mv(&state);

        let plan = load_insert_plan(
            &state,
            &ResolvedLocalTableName {
                database: "analytics".to_string(),
                table: "orders_mv".to_string(),
            },
            PartitionTarget::Staged {
                partition_id: staged.partition_id,
                index_id: staged.index_id,
                tablet_ids: staged.tablet_ids.clone(),
            },
        )
        .expect("plan");

        assert_eq!(plan.partition_id, staged.partition_id);
        assert_eq!(
            plan.tablets.iter().map(|t| t.tablet_id).collect::<Vec<_>>(),
            staged.tablet_ids,
        );
    }
}
```

(If `crate::standalone::lake::test_helpers` does not exist, add the smallest possible stub in a new `#[cfg(test)] mod test_helpers` block at the crate root of `lake/mod.rs` or a dedicated test-support module — include only the specific seeders used here.)

- [ ] **Step 3: Run the failing test**

```bash
cd /Users/harbor/project/NovaRocks
cargo test --lib standalone::lake::txn::mv_target_tests -- --nocapture
```

Expected: FAIL — `PartitionTarget` unknown, `load_insert_plan` doesn't accept the third argument.

- [ ] **Step 4: Implement the refactor**

- Add the `PartitionTarget` enum near `ManagedInsertPlan`.
- Change `load_insert_plan`'s signature to take `target: PartitionTarget`. The `Active` branch keeps today's logic (`find active partition/index, then filter tablets`). The `Staged { .. }` branch picks partition/index/tablets straight from the runtime by id.
- Update `insert_into_managed_lake_table` to pass `PartitionTarget::Active`.
- Add `pub(crate) fn write_chunks_into_managed_partition(state, plan, chunks) -> Result<i64, String>`:
  - For each chunk, call `write_routed_chunks(state, &plan, chunk, prepared.txn_id)` after a single `prepare_txn`.
  - After all writes, call `mark_txn_written`, `publish_managed_txn`, `mark_txn_visible`, and `commit_catalog_visible_version` — identical to the existing VALUES-insert tail.
  - Sum row counts across chunks and return as `rows_written`.

- [ ] **Step 5: Run the test, confirm PASS**

```bash
cd /Users/harbor/project/NovaRocks
cargo test --lib standalone::lake::txn -- --nocapture
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
cd /Users/harbor/project/NovaRocks
git add src/standalone/lake/txn.rs src/standalone/lake/mod.rs
git commit -m "feat: allow managed lake writer to target a staged partition"
```

---

## Task 9: `lake/mv_refresh.rs` — synchronous full REFRESH

**Files:**
- Create: `src/standalone/lake/mv_refresh.rs`
- Modify: `src/standalone/lake/mod.rs`
- Test: inline in `src/standalone/lake/mv_refresh.rs`

- [ ] **Step 1: Write failing unit test**

Create `src/standalone/lake/mv_refresh.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn refresh_mv_full_cleans_staged_partition_when_executor_fails() {
        // Build a state with a seed MV + a failing executor stub. Invoke
        // refresh_mv_full. Assert that:
        // 1. It returns the executor error.
        // 2. No CREATING partitions remain under the MV's table_id.
        // 3. An erase job targeting the staged partition root was enqueued.
        let state = crate::standalone::lake::test_helpers::standalone_state_with_seed_mv();

        let err = refresh_mv_full_with_executor(
            &state,
            "analytics",
            "orders_mv",
            |_plan, _chunks_out| Err("boom".to_string()),
        )
        .expect_err("should fail");
        assert!(err.contains("boom"), "err={err}");

        let snap = state
            .metadata_store
            .as_ref()
            .unwrap()
            .load_snapshot()
            .expect("load snapshot")
            .managed;
        let creating: Vec<&_> = snap
            .partitions
            .iter()
            .filter(|p| p.state == super::super::store::ManagedPartitionState::Creating)
            .collect();
        assert!(creating.is_empty(), "staged partition not cleaned");
        let erase_exists = snap
            .erase_jobs
            .iter()
            .any(|j| j.job_kind == super::super::store::ManagedEraseJobKind::DropPartition);
        assert!(erase_exists, "no cleanup erase job enqueued");
    }
}
```

- [ ] **Step 2: Run the test, confirm failure**

```bash
cd /Users/harbor/project/NovaRocks
cargo test --lib standalone::lake::mv_refresh -- --nocapture
```

Expected: FAIL — module not yet registered.

- [ ] **Step 3: Implement `mv_refresh.rs`**

```rust
use std::sync::Arc;

use arrow::record_batch::RecordBatch;

use crate::exec::chunk::Chunk;
use crate::sql::parser::ast::RefreshMaterializedViewStmt;

use super::super::engine::{QueryResult, StandaloneState, StatementResult};
use super::store::{
    ActivateMvRefreshRequest, ManagedEraseJobKind, ManagedPartitionState,
    StageMvRefreshRequest,
};
use super::txn::{PartitionTarget, load_insert_plan, write_chunks_into_managed_partition};

pub(crate) fn refresh_mv(
    state: &Arc<StandaloneState>,
    current_database: &str,
    stmt: &RefreshMaterializedViewStmt,
) -> Result<StatementResult, String> {
    let (db_name, mv_name) = resolve_mv_name(&stmt.name, current_database)?;
    refresh_mv_full_with_executor(state, &db_name, &mv_name, run_mv_select_and_chunks)
}

/// Test seam: lets tests supply an executor that returns canned (or error-ing)
/// Chunks without running the full pipeline.
pub(crate) fn refresh_mv_full_with_executor<F>(
    state: &Arc<StandaloneState>,
    database: &str,
    mv_name: &str,
    executor: F,
) -> Result<StatementResult, String>
where
    F: FnOnce(MvRefreshContext) -> Result<Vec<Chunk>, String>,
{
    // 1. Gather runtime + store.
    let managed_config = state
        .managed_lake_config
        .clone()
        .ok_or_else(|| "standalone managed lake config is missing".to_string())?;
    let metadata_store = state
        .metadata_store
        .as_ref()
        .ok_or_else(|| "managed lake mv refresh requires sqlite metadata store".to_string())?
        .clone();

    let runtime = state
        .managed_lake
        .read()
        .expect("standalone managed lake read lock")
        .table(database, mv_name)?
        .clone();
    if runtime.table.kind != super::store::ManagedTableKind::MaterializedView {
        return Err(format!(
            "`{database}.{mv_name}` is not a materialized view"
        ));
    }
    let mv_row = metadata_store
        .load_snapshot()?
        .managed
        .materialized_views
        .into_iter()
        .find(|mv| mv.mv_id == runtime.table.table_id)
        .ok_or_else(|| format!(
            "materialized view {database}.{mv_name} has no materialized_views row"
        ))?;
    let active_partition = runtime
        .partitions
        .iter()
        .find(|p| p.state == ManagedPartitionState::Active)
        .cloned()
        .ok_or_else(|| format!(
            "materialized view {database}.{mv_name} has no active partition"
        ))?;
    let old_root_path = runtime
        .tablets
        .iter()
        .find(|t| t.partition_id == active_partition.partition_id)
        .map(|t| t.tablet_root_path.clone())
        .ok_or_else(|| format!(
            "materialized view {database}.{mv_name} has no active tablets"
        ))?;

    // 2. Stage the new partition.
    let staged = metadata_store.stage_mv_refresh_partition(StageMvRefreshRequest {
        table_id: runtime.table.table_id,
        db_id: runtime.table.db_id,
        bucket_num: runtime.table.bucket_num,
        partition_name: active_partition.name.clone(),
        warehouse_uri: managed_config.warehouse_uri.clone(),
    })?;

    // Rebuild managed-lake catalog so the staged partition's tablets are
    // addressable by the write path.
    refresh_managed_catalog(state, &metadata_store)?;

    // 3. Bootstrap empty tablets for the staged partition.
    if let Err(err) =
        super::ddl::bootstrap_empty_partition_for_mv(state, &runtime, &staged, &managed_config)
    {
        cleanup_on_failure(state, &metadata_store, runtime.table.table_id, &staged)?;
        return Err(format!("mv refresh bootstrap failed: {err}"));
    }

    // 4. Execute the SELECT; sink chunks into the staged partition.
    let context = MvRefreshContext {
        state: Arc::clone(state),
        database: database.to_string(),
        mv_name: mv_name.to_string(),
        select_sql: mv_row.select_sql.clone(),
    };
    let chunks = match executor(context) {
        Ok(chunks) => chunks,
        Err(err) => {
            cleanup_on_failure(state, &metadata_store, runtime.table.table_id, &staged)?;
            return Err(format!("mv refresh execute failed: {err}"));
        }
    };

    let plan = match load_insert_plan(
        state,
        &super::super::engine::ResolvedLocalTableName {
            database: database.to_string(),
            table: mv_name.to_string(),
        },
        PartitionTarget::Staged {
            partition_id: staged.partition_id,
            index_id: staged.index_id,
            tablet_ids: staged.tablet_ids.clone(),
        },
    ) {
        Ok(plan) => plan,
        Err(err) => {
            cleanup_on_failure(state, &metadata_store, runtime.table.table_id, &staged)?;
            return Err(format!("mv refresh plan load failed: {err}"));
        }
    };

    let rows_written =
        match write_chunks_into_managed_partition(state, plan, &chunks) {
            Ok(n) => n,
            Err(err) => {
                cleanup_on_failure(state, &metadata_store, runtime.table.table_id, &staged)?;
                return Err(format!("mv refresh write failed: {err}"));
            }
        };

    // 5. Capture Iceberg current snapshots for each base table (best-effort;
    //    missing snapshots become absent entries rather than failures).
    let snapshots = collect_current_snapshots(state, &mv_row.base_table_refs)?;

    // 6. Activate.
    metadata_store.activate_mv_refresh_partition(ActivateMvRefreshRequest {
        table_id: runtime.table.table_id,
        old_partition_id: active_partition.partition_id,
        new_partition_id: staged.partition_id,
        new_index_id: staged.index_id,
        retired_root_path: old_root_path,
        rows_written,
        snapshots,
    })?;

    refresh_managed_catalog(state, &metadata_store)?;
    Ok(StatementResult::Ok)
}

pub(crate) struct MvRefreshContext {
    pub(crate) state: Arc<StandaloneState>,
    pub(crate) database: String,
    pub(crate) mv_name: String,
    pub(crate) select_sql: String,
}

fn run_mv_select_and_chunks(ctx: MvRefreshContext) -> Result<Vec<Chunk>, String> {
    // Parse the stored select_sql → sqlparser::ast::Query, invoke the same
    // execute_query path used for user SELECTs, then convert the returned
    // QueryResult into Arrow Chunks matching the MV's tablet schema.
    use crate::standalone::engine::execute_query_for_mv_refresh;

    let result: QueryResult = execute_query_for_mv_refresh(
        &ctx.state,
        &ctx.database,
        &ctx.select_sql,
    )?;
    query_result_to_chunks(result)
}

fn query_result_to_chunks(result: QueryResult) -> Result<Vec<Chunk>, String> {
    use std::sync::Arc;

    use arrow::datatypes::{Field, Schema};

    use crate::common::ids::SlotId;
    use crate::exec::chunk::ChunkSchema;

    if result.row_count() == 0 {
        return Ok(Vec::new());
    }

    let fields: Vec<Field> = result
        .columns
        .iter()
        .map(|col| {
            Field::new(
                col.name.clone(),
                col.array.data_type().clone(),
                col.array.null_count() > 0,
            )
        })
        .collect();
    let schema = Arc::new(Schema::new(fields));
    let arrays = result
        .columns
        .iter()
        .map(|col| col.array.clone())
        .collect::<Vec<_>>();
    let record_batch = RecordBatch::try_new(schema.clone(), arrays)
        .map_err(|e| format!("build mv refresh record batch failed: {e}"))?;
    let slot_ids: Vec<SlotId> = (1..=result.columns.len() as u32)
        .map(SlotId::new)
        .collect();
    let chunk_schema =
        ChunkSchema::try_ref_from_schema_and_slot_ids(schema.as_ref(), &slot_ids)?;
    Ok(vec![Chunk::new_with_chunk_schema(record_batch, chunk_schema)])
}

fn collect_current_snapshots(
    state: &Arc<StandaloneState>,
    refs: &[super::store::IcebergTableRef],
) -> Result<std::collections::BTreeMap<String, i64>, String> {
    let registry = state
        .iceberg_catalogs
        .read()
        .expect("iceberg registry read lock");
    let mut out = std::collections::BTreeMap::new();
    for r in refs {
        let Ok(entry) = registry.get(&r.catalog) else {
            continue;
        };
        let loaded = match crate::standalone::iceberg::load_table(
            entry,
            &r.namespace,
            &r.table,
        ) {
            Ok(loaded) => loaded,
            Err(_) => continue,
        };
        if let Some(snap) = loaded.metadata.current_snapshot() {
            out.insert(r.fqn(), snap.snapshot_id());
        }
    }
    Ok(out)
}

fn cleanup_on_failure(
    state: &Arc<StandaloneState>,
    metadata_store: &super::store::SqliteMetadataStore,
    table_id: i64,
    staged: &super::store::StagedMvRefresh,
) -> Result<(), String> {
    metadata_store.delete_creating_partition(staged.partition_id)?;
    metadata_store
        .enqueue_erase_job_for_partition_root(
            table_id,
            staged.partition_id,
            &staged.partition_root_path,
        )?;
    refresh_managed_catalog(state, metadata_store)?;
    Ok(())
}

fn resolve_mv_name(
    name: &crate::sql::parser::ast::ObjectName,
    current_database: &str,
) -> Result<(String, String), String> {
    use super::super::engine::catalog::normalize_identifier;
    match name.parts.as_slice() {
        [table] => Ok((
            normalize_identifier(current_database)?,
            normalize_identifier(table)?,
        )),
        [database, table] => Ok((
            normalize_identifier(database)?,
            normalize_identifier(table)?,
        )),
        _ => Err(format!(
            "materialized view name must be `<name>` or `<db>.<name>`; got `{}`",
            name.parts.join(".")
        )),
    }
}

fn refresh_managed_catalog(
    state: &Arc<StandaloneState>,
    metadata_store: &super::store::SqliteMetadataStore,
) -> Result<(), String> {
    let snapshot = metadata_store.load_snapshot()?.managed;
    let rebuilt = super::catalog::ManagedLakeCatalog::rebuild(
        state.managed_lake_config.clone(),
        snapshot,
    )?;
    let mut managed = state
        .managed_lake
        .write()
        .expect("standalone managed lake write lock");
    *managed = rebuilt;
    Ok(())
}
```

**Implementer notes:**

- `enqueue_erase_job_for_partition_root` is a small new helper on `SqliteMetadataStore`. It is a one-row `INSERT INTO erase_jobs(...) VALUES ('DROP_PARTITION', ...)`; same shape as the body already inside `activate_truncate_partition`. Add it in this task if not already present.
- `bootstrap_empty_partition_for_mv` in `super::ddl` is the managed-lake helper that creates empty lake tablet metadata for each tablet in `staged.tablet_ids`. The equivalent private helper is already used by `stage_truncate_partition`'s follow-up; either rename it to `pub(crate)` or add a thin wrapper.
- `execute_query_for_mv_refresh` in `engine/mod.rs` is a thin `pub(crate)` wrapper that wraps `execute_query(...)` with the right session defaults and returns a `QueryResult`.
- `query_result_to_chunks` reuses `build_local_insert_batch` (already re-exported from `engine/mod.rs`) to build the Arrow `RecordBatch`.

- [ ] **Step 4: Register `mv_refresh` in `src/standalone/lake/mod.rs`**

```rust
pub(crate) mod mv_refresh;
```

- [ ] **Step 5: Run the unit test, confirm PASS**

```bash
cd /Users/harbor/project/NovaRocks
cargo test --lib standalone::lake::mv_refresh -- --nocapture
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
cd /Users/harbor/project/NovaRocks
git add src/standalone/lake/mv_refresh.rs src/standalone/lake/mod.rs \
        src/standalone/lake/store.rs src/standalone/lake/ddl.rs \
        src/standalone/engine/mod.rs
git commit -m "feat: add synchronous full mv refresh orchestrator"
```

---

## Task 10: Wire new AST dispatch into engine; delete the old stub

**Files:**
- Modify: `src/standalone/engine/mod.rs`
- Modify: `src/standalone/engine/sqlparse/mod.rs`
- Delete: `src/standalone/engine/sqlparse/materialized_view.rs`

- [ ] **Step 1: Add a failing integration-style unit test**

Place in the existing `src/standalone/engine/mod.rs` test module (or create one if none exists):

```rust
#[cfg(test)]
mod mv_dispatch_tests {
    use super::*;

    #[test]
    fn engine_dispatches_create_materialized_view_ast_variant() {
        // Compiling the engine with the new AST dispatch branches requires
        // that `Statement::CreateMaterializedView` / `DropMaterializedView` /
        // `RefreshMaterializedView` / `ShowMaterializedViews` are all
        // handled without a catch-all `unreachable!()`. The unit test
        // constructs each AST variant manually and feeds it through the
        // dispatch entry point, asserting that we reach the relevant helper
        // module (observable via the error string).

        let state = crate::standalone::lake::test_helpers::standalone_state_with_seed_mv();
        let err = dispatch_statement(
            &state,
            "analytics",
            Statement::RefreshMaterializedView(
                crate::sql::parser::ast::RefreshMaterializedViewStmt {
                    name: crate::sql::parser::ast::ObjectName {
                        parts: vec!["analytics".to_string(), "does_not_exist".to_string()],
                    },
                },
            ),
        )
        .expect_err("should error on missing mv");
        assert!(
            err.contains("materialized view")
                || err.contains("does_not_exist"),
            "unexpected dispatch error: {err}"
        );
    }
}
```

`dispatch_statement` is a convenience wrapper you add at the same time; it is the single path that matches on `Statement::*` and delegates to existing helpers. If a different name is already used internally (e.g. `Session::execute_statement`), swap the name accordingly.

- [ ] **Step 2: Run the test to confirm failure**

```bash
cd /Users/harbor/project/NovaRocks
cargo test --lib standalone::engine::mv_dispatch_tests -- --nocapture
```

Expected: FAIL — enum match non-exhaustive or missing branches.

- [ ] **Step 3: Add AST dispatch branches in `engine/mod.rs`**

- Locate the current `execute` / `execute_statement` fan-out (the one that currently calls `parse_create_materialized_view_name`, `parse_drop_materialized_view_name`, etc.).
- Replace the text-tokenizing dispatch with a match on the `Statement` variants:

  ```rust
  match statement {
      Statement::CreateMaterializedView(stmt) => {
          super::lake::mv_ddl::create_mv(state, current_database, &stmt)
      }
      Statement::DropMaterializedView(stmt) => {
          super::lake::mv_ddl::drop_mv(state, current_database, &stmt)
      }
      Statement::RefreshMaterializedView(stmt) => {
          super::lake::mv_refresh::refresh_mv(state, current_database, &stmt)
      }
      Statement::ShowMaterializedViews(stmt) => {
          super::lake::mv_ddl::list_mvs(state, &stmt)
      }
      // existing variants (CREATE TABLE, INSERT, SELECT, ...) continue to
      // follow their existing branches.
      _ => ...,
  }
  ```

- Remove the fields `materialized_views` and `materialized_view_seq` from `StandaloneState`, together with their initializers and all dead callers inside `engine/mod.rs`. Delete `StandaloneMaterializedView` entirely.

- [ ] **Step 4: Drop the text-tokenizing module**

- Remove `pub(crate) mod materialized_view;` from `src/standalone/engine/sqlparse/mod.rs`.
- Delete `src/standalone/engine/sqlparse/materialized_view.rs`.

- [ ] **Step 5: Run the test, confirm PASS**

```bash
cd /Users/harbor/project/NovaRocks
cargo test --lib standalone::engine -- --nocapture
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
cd /Users/harbor/project/NovaRocks
git add src/standalone/engine/ src/standalone/engine/sqlparse/
git commit -m "feat: dispatch materialized view AST variants and drop text-tokenizing stub"
```

---

## Task 11: MySQL-protocol integration tests

**Files:**
- Modify: `tests/standalone_mysql_server.rs`

- [ ] **Step 1: Add the happy-path round-trip test**

Append to `tests/standalone_mysql_server.rs`:

```rust
#[test]
fn standalone_mysql_server_mv_create_and_manual_refresh_round_trip() {
    let port = alloc_port();
    let Some((_config_dir, config_path)) = maybe_write_managed_lake_config(port) else {
        return;
    };

    let args = vec![
        "standalone-server".to_string(),
        "--config".to_string(),
        config_path.display().to_string(),
    ];
    let mut server = ServerGuard::spawn(&args);
    let mut conn = server.connect_root(port);

    // 1. Prepare an Iceberg base table via the existing Iceberg DDL path.
    conn.query_drop("create external catalog iceberg_cat properties(\"type\" = \"hadoop\", \"warehouse\" = \"s3://novarocks/sql-tests-managed-lake/iceberg_cat\")")
        .expect("create iceberg_cat");
    conn.query_drop("create database iceberg_cat.ns")
        .expect("create ns");
    conn.query_drop(
        "create table iceberg_cat.ns.orders (k1 int, v2 bigint) \
         using iceberg \
         tblproperties('format-version' = '2')",
    )
    .expect("create iceberg orders");
    conn.query_drop("insert into iceberg_cat.ns.orders values (1, 10), (2, 20), (1, 30), (3, 50)")
        .expect("seed iceberg rows");

    // 2. Create an MV.
    conn.query_drop("create database analytics")
        .expect("create analytics db");
    conn.query_drop("use analytics").expect("use analytics");
    conn.query_drop(
        "create materialized view orders_mv \
         distributed by hash(k1) buckets 2 \
         as select k1, sum(v2) as total from iceberg_cat.ns.orders group by k1",
    )
    .expect("create mv");

    // 3. Before first REFRESH: SELECT returns empty.
    let pre_rows: Vec<(Option<i32>, Option<i64>)> = conn
        .query("select k1, total from orders_mv")
        .expect("select before refresh");
    assert!(pre_rows.is_empty(), "pre-refresh rows: {pre_rows:?}");

    // 4. REFRESH + verify rows.
    conn.query_drop("refresh materialized view orders_mv")
        .expect("refresh mv");
    let mut rows: Vec<(Option<i32>, Option<i64>)> = conn
        .query("select k1, total from orders_mv order by k1")
        .expect("select after refresh");
    rows.sort();
    assert_eq!(
        rows,
        vec![
            (Some(1), Some(40)),
            (Some(2), Some(20)),
            (Some(3), Some(50)),
        ]
    );

    // 5. Insert more rows; MV stays at first-refresh snapshot.
    conn.query_drop("insert into iceberg_cat.ns.orders values (4, 70)")
        .expect("second iceberg write");
    let stable: Vec<(Option<i32>, Option<i64>)> = conn
        .query("select k1, total from orders_mv order by k1")
        .expect("select post-write pre-refresh");
    assert_eq!(
        stable,
        vec![
            (Some(1), Some(40)),
            (Some(2), Some(20)),
            (Some(3), Some(50)),
        ],
        "MV should not see new rows until next REFRESH"
    );

    // 6. REFRESH again, now should include the new row.
    conn.query_drop("refresh materialized view orders_mv")
        .expect("second refresh mv");
    let post: Vec<(Option<i32>, Option<i64>)> = conn
        .query("select k1, total from orders_mv order by k1")
        .expect("select after second refresh");
    assert_eq!(
        post,
        vec![
            (Some(1), Some(40)),
            (Some(2), Some(20)),
            (Some(3), Some(50)),
            (Some(4), Some(70)),
        ]
    );

    // 7. DROP MV; subsequent query fails.
    conn.query_drop("drop materialized view orders_mv")
        .expect("drop mv");
    let err = conn
        .query::<(i32,), _>("select k1 from orders_mv")
        .expect_err("query after drop should fail");
    assert!(
        err.to_string()
            .to_ascii_lowercase()
            .contains("unknown table")
            || err.to_string().to_ascii_lowercase().contains("does not exist"),
        "unexpected error: {err}"
    );
}
```

- [ ] **Step 2: Add the SHOW output test**

```rust
#[test]
fn standalone_mysql_server_mv_show_output_matches_expected_columns() {
    let port = alloc_port();
    let Some((_config_dir, config_path)) = maybe_write_managed_lake_config(port) else {
        return;
    };
    let args = vec![
        "standalone-server".to_string(),
        "--config".to_string(),
        config_path.display().to_string(),
    ];
    let mut server = ServerGuard::spawn(&args);
    let mut conn = server.connect_root(port);

    conn.query_drop("create external catalog iceberg_cat properties(\"type\" = \"hadoop\", \"warehouse\" = \"s3://novarocks/sql-tests-managed-lake/iceberg_cat\")")
        .expect("create iceberg_cat");
    conn.query_drop("create database iceberg_cat.ns").expect("ns");
    conn.query_drop(
        "create table iceberg_cat.ns.orders (k1 int, v2 bigint) \
         using iceberg tblproperties('format-version' = '2')",
    ).expect("iceberg table");
    conn.query_drop("create database analytics").expect("db");
    conn.query_drop("use analytics").expect("use");
    conn.query_drop(
        "create materialized view orders_mv distributed by hash(k1) buckets 2 \
         as select k1 from iceberg_cat.ns.orders",
    ).expect("mv");

    let rows: Vec<(String, String, String, Option<String>, Option<String>, String, String)> =
        conn.query("show materialized views from analytics")
            .expect("show mvs");
    assert_eq!(rows.len(), 1);
    let row = &rows[0];
    assert_eq!(row.0, "orders_mv");
    assert_eq!(row.1, "analytics");
    assert_eq!(row.2, "DEFERRED_MANUAL");
    assert_eq!(row.3, None);
    assert_eq!(row.4, None);
    assert_eq!(row.5, "iceberg_cat.ns.orders");
    assert!(row.6.to_lowercase().contains("select"));
}
```

- [ ] **Step 3: Add the non-Iceberg base-table rejection test**

```rust
#[test]
fn standalone_mysql_server_mv_create_rejects_non_iceberg_base_table() {
    let port = alloc_port();
    let Some((_config_dir, config_path)) = maybe_write_managed_lake_config(port) else {
        return;
    };
    let args = vec![
        "standalone-server".to_string(),
        "--config".to_string(),
        config_path.display().to_string(),
    ];
    let mut server = ServerGuard::spawn(&args);
    let mut conn = server.connect_root(port);

    conn.query_drop("create database analytics").expect("db");
    conn.query_drop("use analytics").expect("use");
    conn.query_drop(
        "create table base_table (k1 int, v2 bigint) \
         duplicate key(k1) distributed by hash(k1) buckets 2",
    ).expect("lake table");

    let err = conn
        .query_drop(
            "create materialized view mv1 distributed by hash(k1) buckets 2 \
             as select k1 from base_table",
        )
        .expect_err("should reject");
    assert!(
        err.to_string().to_lowercase().contains("iceberg"),
        "unexpected error: {err}"
    );
}
```

- [ ] **Step 4: Add the reopen-recovery test**

```rust
#[test]
fn standalone_mysql_server_mv_reopen_recovers_after_crashed_refresh() {
    use rusqlite::Connection as SqliteConn;

    let port = alloc_port();
    let Some((_config_dir, config_path)) = maybe_write_managed_lake_config(port) else {
        return;
    };
    let args = vec![
        "standalone-server".to_string(),
        "--config".to_string(),
        config_path.display().to_string(),
    ];
    {
        let mut server = ServerGuard::spawn(&args);
        let mut conn = server.connect_root(port);
        conn.query_drop("create external catalog iceberg_cat properties(\"type\" = \"hadoop\", \"warehouse\" = \"s3://novarocks/sql-tests-managed-lake/iceberg_cat\")")
            .expect("iceberg_cat");
        conn.query_drop("create database iceberg_cat.ns").expect("ns");
        conn.query_drop(
            "create table iceberg_cat.ns.orders (k1 int) using iceberg \
             tblproperties('format-version' = '2')",
        ).expect("iceberg table");
        conn.query_drop("insert into iceberg_cat.ns.orders values (1), (2)")
            .expect("seed iceberg rows");
        conn.query_drop("create database analytics").expect("db");
        conn.query_drop("use analytics").expect("use");
        conn.query_drop(
            "create materialized view orders_mv distributed by hash(k1) buckets 1 \
             as select k1 from iceberg_cat.ns.orders",
        ).expect("mv");
        conn.query_drop("refresh materialized view orders_mv").expect("first refresh");
        // server drops here
    }

    // Inject a dangling CREATING partition to simulate a crashed refresh.
    let metadata_db_path = metadata_db_path_for_managed_config(&config_path);
    {
        let sqlite = SqliteConn::open(&metadata_db_path).expect("sqlite open");
        let mv_id: i64 = sqlite
            .query_row(
                "SELECT table_id FROM tables WHERE name = 'orders_mv' AND kind = 'MATERIALIZED_VIEW'",
                [],
                |row| row.get(0),
            )
            .expect("mv id");
        let staged_pid: i64 = {
            let next: i64 = sqlite
                .query_row(
                    "SELECT next_partition_id FROM global_meta WHERE singleton = 1",
                    [],
                    |row| row.get(0),
                )
                .expect("next_partition_id");
            sqlite
                .execute(
                    "UPDATE global_meta SET next_partition_id = ?1 WHERE singleton = 1",
                    rusqlite::params![next + 1],
                )
                .expect("bump next pid");
            next
        };
        sqlite
            .execute(
                "INSERT INTO partitions(partition_id, table_id, name, visible_version, next_version, state) \
                 VALUES (?1, ?2, 'p0', 1, 2, 'CREATING')",
                rusqlite::params![staged_pid, mv_id],
            )
            .expect("inject dangling partition");
    }

    // Reopen and assert MV still works + CREATING residue is cleaned.
    {
        let mut server = ServerGuard::spawn(&args);
        let mut conn = server.connect_root(port);
        conn.query_drop("use analytics").expect("use analytics");
        let mut rows: Vec<(Option<i32>,)> = conn
            .query("select k1 from orders_mv order by k1")
            .expect("select after reopen");
        rows.sort();
        assert_eq!(rows, vec![(Some(1),), (Some(2),)]);

        let sqlite = SqliteConn::open(&metadata_db_path).expect("sqlite reopen");
        let creating_count: i64 = sqlite
            .query_row(
                "SELECT COUNT(*) FROM partitions WHERE state = 'CREATING'",
                [],
                |row| row.get(0),
            )
            .expect("count creating");
        assert_eq!(creating_count, 0);
    }
}
```

- [ ] **Step 5: Add `metadata_db_path_for_managed_config` helper (if missing)**

If not already present in `tests/standalone_mysql_server.rs`, add a helper that reads the `metadata_db_path` out of the TOML at `config_path`, mirroring how the other managed-lake tests locate the sqlite file.

- [ ] **Step 6: Run all four new integration tests**

```bash
cd /Users/harbor/project/NovaRocks
cargo test --test standalone_mysql_server standalone_mysql_server_mv_create_and_manual_refresh_round_trip -- --nocapture
cargo test --test standalone_mysql_server standalone_mysql_server_mv_show_output_matches_expected_columns -- --nocapture
cargo test --test standalone_mysql_server standalone_mysql_server_mv_create_rejects_non_iceberg_base_table -- --nocapture
cargo test --test standalone_mysql_server standalone_mysql_server_mv_reopen_recovers_after_crashed_refresh -- --nocapture
```

Expected: PASS (all four).

*If `AWS_S3_ENDPOINT` or MinIO isn't reachable locally, the tests return early with the existing `maybe_write_managed_lake_config` skip behaviour; this is correct and the tests do not need to run in that environment.*

- [ ] **Step 7: Commit**

```bash
cd /Users/harbor/project/NovaRocks
git add tests/standalone_mysql_server.rs
git commit -m "test: cover materialized view on iceberg happy path and reopen recovery"
```

---

## Task 12: SQL-tests suite — happy-path MV case

**Files:**
- Create: `tests/sql-test-runner/cases/managed_lake/managed_lake_mv_basic.sql` (or wherever the existing managed-lake cases live — grep for `managed_lake` in `tests/sql-test-runner/cases/`)
- Create: `tests/sql-test-runner/cases/managed_lake/managed_lake_mv_basic.result`

- [ ] **Step 1: Locate the existing managed-lake suite layout**

```bash
cd /Users/harbor/project/NovaRocks
rg --files tests/sql-test-runner/ | rg -i "managed_lake" | head -20
```

Use the surrounding files to determine the exact case directory and result-file naming convention for the managed-lake suite (`standalone_managed_lake.conf` / `.toml`).

- [ ] **Step 2: Write the case file**

Example contents (adapt file extensions and `-- result:` comment style to the existing suite convention):

```sql
create external catalog iceberg_cat properties(
    "type" = "hadoop",
    "warehouse" = "s3://novarocks/sql-tests-managed-lake/iceberg_cat"
);
create database iceberg_cat.ns;
create table iceberg_cat.ns.orders (k1 int, v2 bigint) using iceberg
    tblproperties('format-version' = '2');
insert into iceberg_cat.ns.orders values (1, 10), (2, 20), (1, 30);

create database analytics;
use analytics;
create materialized view orders_mv distributed by hash(k1) buckets 2
    as select k1, sum(v2) as total from iceberg_cat.ns.orders group by k1;
refresh materialized view orders_mv;
select k1, total from orders_mv order by k1;

drop materialized view orders_mv;
```

- [ ] **Step 3: Record expected output**

```bash
cd /Users/harbor/project/NovaRocks
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --suite standalone-managed-lake --only managed_lake_mv_basic --mode record
```

Inspect the diff in `git diff tests/sql-test-runner/cases/managed_lake/managed_lake_mv_basic.result` and confirm it matches the intent (3 rows: k1=1 → total=40, k1=2 → total=20).

- [ ] **Step 4: Verify the case now passes in `verify` mode**

```bash
cd /Users/harbor/project/NovaRocks
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --suite standalone-managed-lake --only managed_lake_mv_basic --mode verify
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cd /Users/harbor/project/NovaRocks
git add tests/sql-test-runner/
git commit -m "test: record managed lake mv basic case"
```

---

## Task 13: Final verification sweep

**Files:**
- Inspect: all files touched in Tasks 1–12.

- [ ] **Step 1: Formatting and lint**

```bash
cd /Users/harbor/project/NovaRocks
cargo fmt
cargo clippy --all-targets -- -D warnings
```

Expected: no diff from `fmt`; clippy clean (or only pre-existing warnings unrelated to this plan).

- [ ] **Step 2: Targeted library tests**

```bash
cd /Users/harbor/project/NovaRocks
cargo test --lib standalone::lake::store::tests -- --nocapture
cargo test --lib standalone::lake::catalog::tests -- --nocapture
cargo test --lib standalone::lake::mv_ddl -- --nocapture
cargo test --lib standalone::lake::mv_refresh -- --nocapture
cargo test --lib standalone::lake::txn -- --nocapture
cargo test --lib sql::parser::dialect::materialized_view -- --nocapture
cargo test --lib standalone::engine -- --nocapture
```

Expected: PASS.

- [ ] **Step 3: Full integration tests**

```bash
cd /Users/harbor/project/NovaRocks
cargo test --test standalone_mysql_server -- --nocapture
```

Expected: PASS (MV tests run if MinIO is up, otherwise skip gracefully).

- [ ] **Step 4: SQL-tests suite regression**

```bash
cd /Users/harbor/project/NovaRocks
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --suite standalone-managed-lake --mode verify
```

Expected: PASS (new MV case plus all previously-recorded managed-lake cases).

- [ ] **Step 5: Confirm the deprecated stub is gone**

```bash
cd /Users/harbor/project/NovaRocks
rg "src/standalone/engine/sqlparse/materialized_view.rs" || true
rg "supports_bitmap_count_rewrite|parse_create_materialized_view_name" src/
```

Expected: zero matches (file deleted, callers gone).

- [ ] **Step 6: Final summary note**

In the final commit message or PR description, explicitly call out what Phase 1 does **not** cover (copy the list from spec §3 / §10), so reviewers don't expect Phase 2 behaviour.

- [ ] **Step 7: Commit any remaining cleanup**

```bash
cd /Users/harbor/project/NovaRocks
git status --short
# If anything surfaces from fmt/clippy fixups:
git add -u
git commit -m "chore: final cleanup for mv on iceberg phase 1"
```

---
