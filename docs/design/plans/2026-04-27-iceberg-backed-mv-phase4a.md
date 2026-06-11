# Iceberg-Backed Materialized View Phase 4a Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Land projection/filter materialized views whose physical storage is an Iceberg table hosted in a NovaRocks-internal `__nova_mv__` catalog, with append-only first/incremental refresh, while keeping the existing managed-lake MV path (phase1-3) fully intact.

**Architecture:** Add a `storage_engine` PROPERTIES selector on `CREATE MATERIALIZED VIEW`. When `storage_engine = 'iceberg'`, route CREATE / REFRESH / DROP through a new `mv_refresh_iceberg` module that uses an in-process `HadoopFileSystemCatalog` instance scoped to a NovaRocks-private warehouse. MV lineage and refresh metadata stay in NovaRocks SQLite (a new v6 migration adds three columns); Iceberg snapshot commits handle atomic data publish.

**Tech Stack:** Rust, `iceberg` 0.9.0 (`HadoopFileSystemCatalog`, `Transaction::fast_append`, `IcebergTableSinkFactory`), `rusqlite`, NovaRocks managed-lake metadata store, `sqlparser` AST.

---

## File Structure

- Modify `src/sql/parser/ast/mod.rs`: add `properties` field to `CreateMaterializedViewStmt`.
- Modify `src/sql/parser/dialect/materialized_view.rs`: rename `parse_and_drop_properties` to `parse_properties` and return parsed pairs into the AST node.
- Modify `src/connector/starrocks/managed/store.rs`: SQLite v5 → v6 migration adds three columns to `materialized_views`; extend `StoredMaterializedView`; update INSERT/SELECT.
- Modify `src/common/app_config.rs`: add `mv_default_storage_engine` and `mv_iceberg_warehouse_location` fields.
- Modify `src/connector/starrocks/managed/config.rs`: forward new app-config fields into `ManagedLakeConfig`.
- Create `src/connector/starrocks/managed/mv_iceberg_catalog.rs`: lazily build a process-singleton `HadoopFileSystemCatalog` rooted at the NovaRocks-private MV warehouse.
- Create `src/connector/starrocks/managed/mv_refresh_iceberg.rs`: phase4a CREATE / first refresh / incremental refresh / DROP for the Iceberg storage path.
- Modify `src/connector/starrocks/managed/mod.rs`: re-export the two new modules.
- Modify `src/connector/starrocks/managed/mv_ddl.rs`: `create_mv` and `drop_mv` dispatch on resolved `storage_engine`.
- Modify `src/connector/starrocks/managed/mv_refresh.rs`: `refresh_mv` dispatches on the persisted `storage_engine`.
- Modify the `SHOW MATERIALIZED VIEWS` formatting site (located inside `mv_ddl.rs::list_mvs` or its caller) to include the new column.
- Add `sql-tests/write-path/sql/iceberg_backed_mv_projection_filter.sql` and `sql-tests/write-path/result/iceberg_backed_mv_projection_filter.result`.

## Implementation Constraints

- The new `__nova_mv__` catalog must NOT be reachable through user-facing `CREATE EXTERNAL CATALOG` syntax; the parser MUST reject `__nova_mv__` as a user catalog name.
- Phase 1-3 SQL regression cases continue to pass with no edits — `mv_default_storage_engine` defaults to `managed_lake` so existing CREATE statements without a PROPERTIES clause behave identically.
- Iceberg `Transaction::fast_append().commit()` is the only publish primitive in phase4a; do not attempt overwrite, branch, or row-level operations.
- Reuse `run_mv_select_and_chunks` (`mv_refresh.rs:476`) and `plan_append_delta` (`mv_refresh.rs`) instead of forking new query / delta planners.
- All new code is `pub(crate)` only; no module exports leak outside the standalone connector tree.
- Only projection/filter shape is supported in phase4a. Aggregate shape under `storage_engine = 'iceberg'` returns an explicit `not yet supported in phase4a` error.

---

## Task 1: SQLite v6 Migration For storage_engine Columns

**Files:**
- Modify: `src/connector/starrocks/managed/store.rs`

- [ ] **Step 1: Write the failing v6 schema and migration tests**

Add these tests at the end of the existing `#[cfg(test)] mod tests` in `store.rs`:

```rust
#[test]
fn init_schema_v6_creates_storage_engine_columns() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = SqliteMetadataStore::open(dir.path().join("standalone.sqlite"))
        .expect("open fresh store");
    let conn = store.connection().expect("connection");

    let version: i64 = conn
        .query_row("PRAGMA user_version", [], |row| row.get(0))
        .expect("user_version");
    assert_eq!(version, 6);

    let cols: Vec<(String, String, i64)> = {
        let mut stmt = conn
            .prepare(
                "SELECT name, type, \"notnull\"
                 FROM pragma_table_info('materialized_views')
                 WHERE name IN ('storage_engine','iceberg_table_identifier','last_refreshed_iceberg_snapshot_id')
                 ORDER BY cid",
            )
            .expect("prepare");
        stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)?,
            ))
        })
        .expect("query")
        .collect::<Result<Vec<_>, _>>()
        .expect("collect")
    };
    assert_eq!(
        cols,
        vec![
            ("storage_engine".to_string(), "TEXT".to_string(), 1),
            ("iceberg_table_identifier".to_string(), "TEXT".to_string(), 0),
            ("last_refreshed_iceberg_snapshot_id".to_string(), "INTEGER".to_string(), 0),
        ],
    );
}

#[test]
fn init_schema_migrates_v5_materialized_views_storage_engine() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("old.sqlite");
    {
        let conn = rusqlite::Connection::open(&path).expect("open");
        conn.execute_batch(
            "
            CREATE TABLE materialized_views (
                mv_id INTEGER PRIMARY KEY,
                select_sql TEXT NOT NULL,
                refresh_mode TEXT NOT NULL DEFAULT 'DEFERRED_MANUAL',
                base_table_refs_json TEXT NOT NULL,
                last_refresh_ms INTEGER,
                last_refresh_rows INTEGER,
                last_refresh_snapshots_json TEXT,
                created_at_ms INTEGER NOT NULL
            );
            INSERT INTO materialized_views(
                mv_id, select_sql, refresh_mode, base_table_refs_json, created_at_ms
            ) VALUES (
                42, 'SELECT 1', 'DEFERRED_MANUAL', '[]', 0
            );
            PRAGMA user_version = 5;
            ",
        )
        .expect("seed v5");
    }

    let store = SqliteMetadataStore::open(&path).expect("open migrates v5");
    let conn = store.connection().expect("connection");
    let version: i64 = conn
        .query_row("PRAGMA user_version", [], |row| row.get(0))
        .expect("user_version");
    assert_eq!(version, 6);

    let storage_engine: String = conn
        .query_row(
            "SELECT storage_engine FROM materialized_views WHERE mv_id = 42",
            [],
            |row| row.get(0),
        )
        .expect("storage_engine value");
    assert_eq!(storage_engine, "managed_lake");
}
```

- [ ] **Step 2: Run the new schema tests and verify failure**

Run:

```bash
cargo test --lib connector::starrocks::managed::store::tests::init_schema_v6_creates_storage_engine_columns -- --nocapture
cargo test --lib connector::starrocks::managed::store::tests::init_schema_migrates_v5_materialized_views_storage_engine -- --nocapture
```

Expected: both fail because the schema version is still `5` and the new columns do not exist.

- [ ] **Step 3: Extend `StoredMaterializedView`**

In `src/connector/starrocks/managed/store.rs` change the struct to:

```rust
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
    pub storage_engine: ManagedMvStorageEngine,
    pub iceberg_table_identifier: Option<String>,
    pub last_refreshed_iceberg_snapshot_id: Option<i64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ManagedMvStorageEngine {
    ManagedLake,
    Iceberg,
}

impl ManagedMvStorageEngine {
    pub(crate) fn as_sql_str(self) -> &'static str {
        match self {
            Self::ManagedLake => "managed_lake",
            Self::Iceberg => "iceberg",
        }
    }

    pub(crate) fn parse_sql_str(value: &str) -> Result<Self, String> {
        match value {
            "managed_lake" => Ok(Self::ManagedLake),
            "iceberg" => Ok(Self::Iceberg),
            other => Err(format!("unknown materialized view storage_engine `{other}`")),
        }
    }
}
```

- [ ] **Step 4: Add v5 to v6 migration and update fresh DDL**

In `SqliteMetadataStore::init_schema` accept versions `0`, `4`, `5`, and `6`. Replace the existing version guard with:

```rust
if current_version != 0 && current_version != 4 && current_version != 5 && current_version != 6 {
    return Err(format!(
        "unsupported standalone metadata schema version {current_version}; delete the metadata db and reopen"
    ));
}
if current_version == 4 {
    migrate_schema_v4_to_v5(&conn)?;
}
if current_version == 4 || current_version == 5 {
    migrate_schema_v5_to_v6(&conn)?;
}
```

Add the migration helper at the bottom of `store.rs`:

```rust
fn migrate_schema_v5_to_v6(conn: &Connection) -> Result<(), String> {
    if !table_column_exists(conn, "materialized_views", "storage_engine")? {
        conn.execute_batch(
            "ALTER TABLE materialized_views ADD COLUMN storage_engine TEXT NOT NULL DEFAULT 'managed_lake';",
        )
        .map_err(|e| format!("migrate standalone metadata schema v5 to v6 failed adding storage_engine: {e}"))?;
    }
    if !table_column_exists(conn, "materialized_views", "iceberg_table_identifier")? {
        conn.execute_batch(
            "ALTER TABLE materialized_views ADD COLUMN iceberg_table_identifier TEXT;",
        )
        .map_err(|e| format!("migrate standalone metadata schema v5 to v6 failed adding iceberg_table_identifier: {e}"))?;
    }
    if !table_column_exists(conn, "materialized_views", "last_refreshed_iceberg_snapshot_id")? {
        conn.execute_batch(
            "ALTER TABLE materialized_views ADD COLUMN last_refreshed_iceberg_snapshot_id INTEGER;",
        )
        .map_err(|e| format!("migrate standalone metadata schema v5 to v6 failed adding last_refreshed_iceberg_snapshot_id: {e}"))?;
    }
    conn.execute_batch("PRAGMA user_version = 6;")
        .map_err(|e| format!("migrate standalone metadata schema v5 to v6 failed setting version: {e}"))?;
    Ok(())
}
```

Update the fresh-schema DDL in `init_schema`:

```sql
CREATE TABLE IF NOT EXISTS materialized_views (
    mv_id INTEGER PRIMARY KEY REFERENCES tables(table_id),
    select_sql TEXT NOT NULL,
    refresh_mode TEXT NOT NULL DEFAULT 'DEFERRED_MANUAL'
        CHECK (refresh_mode IN ('DEFERRED_MANUAL')),
    base_table_refs_json TEXT NOT NULL,
    last_refresh_ms INTEGER,
    last_refresh_rows INTEGER,
    last_refresh_snapshots_json TEXT,
    created_at_ms INTEGER NOT NULL,
    storage_engine TEXT NOT NULL DEFAULT 'managed_lake',
    iceberg_table_identifier TEXT,
    last_refreshed_iceberg_snapshot_id INTEGER
);
...
PRAGMA user_version = 6;
```

Rename the existing `init_schema_v5_creates_tables_with_kind_and_materialized_views_table` test to `init_schema_v6_creates_tables_with_kind_and_materialized_views_table` and change `assert_eq!(version, 5)` to `assert_eq!(version, 6)`. Do the same for `init_schema_v5_creates_tables_with_kind_and_materialized_views_table` if it asserts version 5 directly. Update `init_schema_v5_creates_table_column_flags` similarly so its version assertion reads `6` (the column-flags assertion stays the same).

- [ ] **Step 5: Update `replace_managed_snapshot` INSERT and `load_managed_snapshot` SELECT**

In the INSERT into `materialized_views`, change the column list and add three positional binds for `storage_engine`, `iceberg_table_identifier`, and `last_refreshed_iceberg_snapshot_id`.

In the SELECT used inside `load_managed_snapshot` (around line 2253), add the three columns to the column list and to the row mapper. Use `ManagedMvStorageEngine::parse_sql_str` for the engine value, default to `ManagedLake` only when SQL guarantees `NOT NULL` (which it does after migration).

In `update_mv_refresh_metadata` (around line 1289), preserve the existing `UPDATE materialized_views SET ...` behavior. Add a sibling helper `update_mv_iceberg_refresh_metadata` for phase4 paths:

```rust
pub(crate) struct UpdateMvIcebergRefreshMetadataRequest {
    pub table_id: i64,
    pub last_refresh_rows: i64,
    pub snapshots: std::collections::BTreeMap<String, i64>,
    pub iceberg_snapshot_id: i64,
}

pub(crate) fn update_mv_iceberg_refresh_metadata(
    &self,
    request: UpdateMvIcebergRefreshMetadataRequest,
) -> Result<(), String> {
    let conn = self.connection()?;
    let snapshots_json = serde_json::to_string(&request.snapshots)
        .map_err(|e| format!("serialize iceberg refresh snapshots failed: {e}"))?;
    let now_ms = current_unix_ms();
    conn.execute(
        "UPDATE materialized_views
         SET last_refresh_ms = ?1,
             last_refresh_rows = ?2,
             last_refresh_snapshots_json = ?3,
             last_refreshed_iceberg_snapshot_id = ?4
         WHERE mv_id = ?5",
        rusqlite::params![
            now_ms,
            request.last_refresh_rows,
            snapshots_json,
            request.iceberg_snapshot_id,
            request.table_id
        ],
    )
    .map_err(|e| format!("update mv iceberg refresh metadata failed: {e}"))?;
    Ok(())
}
```

Update existing test fixtures (at lines 3664, 3765, 2837, 2879) that build `StoredMaterializedView` literally — add the three new fields with default values:

```rust
storage_engine: ManagedMvStorageEngine::ManagedLake,
iceberg_table_identifier: None,
last_refreshed_iceberg_snapshot_id: None,
```

- [ ] **Step 6: Run store tests**

Run:

```bash
cargo test --lib connector::starrocks::managed::store::tests -- --nocapture
```

Expected: all tests pass, including the two new ones from Step 1.

- [ ] **Step 7: Commit**

```bash
git add src/connector/starrocks/managed/store.rs
git commit -m "feat: persist mv storage engine and iceberg lineage"
```

---

## Task 2: Retain MV PROPERTIES In Parser

**Files:**
- Modify: `src/sql/parser/ast/mod.rs`
- Modify: `src/sql/parser/dialect/materialized_view.rs`

- [ ] **Step 1: Write the failing parser test**

Append to the test module in `src/sql/parser/dialect/materialized_view.rs`:

```rust
#[test]
fn parse_create_materialized_view_keeps_storage_engine_property() {
    let sql = "CREATE MATERIALIZED VIEW mv1 \
        DISTRIBUTED BY HASH(k) BUCKETS 2 \
        PROPERTIES('storage_engine' = 'iceberg', 'comment' = 'demo') \
        AS SELECT k, v FROM ice.ns.t";
    let stmt = crate::sql::parser::parse_sql(sql).expect("parse").unwrap();
    let crate::sql::parser::ast::Statement::CreateMaterializedView(create) = stmt else {
        panic!("expected CREATE MATERIALIZED VIEW");
    };
    assert_eq!(
        create.properties,
        vec![
            ("storage_engine".to_string(), "iceberg".to_string()),
            ("comment".to_string(), "demo".to_string()),
        ],
    );
}
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
cargo test --lib sql::parser::dialect::materialized_view::tests::parse_create_materialized_view_keeps_storage_engine_property -- --nocapture
```

Expected: compilation fails because `CreateMaterializedViewStmt` does not have a `properties` field yet.

- [ ] **Step 3: Add `properties` to the AST node**

In `src/sql/parser/ast/mod.rs` change `CreateMaterializedViewStmt`:

```rust
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct CreateMaterializedViewStmt {
    pub name: ObjectName,
    pub if_not_exists: bool,
    pub distribution: Option<MaterializedViewDistribution>,
    pub refresh_manual_explicit: bool,
    pub select_sql: String,
    pub select_query: sqlparser::ast::Query,
    pub properties: Vec<(String, String)>,
}
```

- [ ] **Step 4: Replace `parse_and_drop_properties` with a parser that returns pairs**

In `src/sql/parser/dialect/materialized_view.rs` rename and rewrite the helper:

```rust
fn parse_properties(parser: &mut Parser<'_>) -> Result<Vec<(String, String)>, String> {
    parser
        .expect_token(&Token::LParen)
        .map_err(|e| format!("expected ( after PROPERTIES: {e}"))?;
    let mut out = Vec::new();
    loop {
        if parser.consume_token(&Token::RParen) {
            break;
        }
        let key = parser
            .parse_literal_string()
            .map_err(|e| format!("parse MV property key failed: {e}"))?;
        parser
            .expect_token(&Token::Eq)
            .map_err(|e| format!("expected = in MV property: {e}"))?;
        let value = parser
            .parse_literal_string()
            .map_err(|e| format!("parse MV property value failed: {e}"))?;
        out.push((key, value));
        if parser.consume_token(&Token::Comma) {
            continue;
        }
        parser
            .expect_token(&Token::RParen)
            .map_err(|e| format!("expected ) or , in PROPERTIES: {e}"))?;
        break;
    }
    Ok(out)
}
```

Update the call site inside `parse_create_materialized_view`:

```rust
let properties = if peek_word_eq(parser, 0, "PROPERTIES") {
    parser.next_token(); // PROPERTIES
    parse_properties(parser)?
} else {
    Vec::new()
};
```

And include `properties` when constructing the `CreateMaterializedViewStmt`:

```rust
Ok(Statement::CreateMaterializedView(
    CreateMaterializedViewStmt {
        name,
        if_not_exists,
        distribution,
        refresh_manual_explicit,
        select_sql,
        select_query: *query,
        properties,
    },
))
```

- [ ] **Step 5: Update fixtures that build `CreateMaterializedViewStmt`**

Search for any test or helper that constructs `CreateMaterializedViewStmt` literally and add `properties: Vec::new()`:

```bash
grep -rn "CreateMaterializedViewStmt" src/ --include="*.rs"
```

Add the field to each literal initializer.

- [ ] **Step 6: Run parser tests**

Run:

```bash
cargo test --lib sql::parser::dialect::materialized_view::tests -- --nocapture
```

Expected: all tests pass, including the new property test from Step 1.

- [ ] **Step 7: Commit**

```bash
git add src/sql/parser/ast/mod.rs src/sql/parser/dialect/materialized_view.rs
git commit -m "feat: retain mv properties in parser ast"
```

---

## Task 3: System Config For Default Storage Engine

**Files:**
- Modify: `src/common/app_config.rs`
- Modify: `src/connector/starrocks/managed/config.rs`

- [ ] **Step 1: Write the failing config default test**

Append to the existing `#[cfg(test)] mod tests` in `src/connector/starrocks/managed/config.rs`:

```rust
#[test]
fn managed_lake_config_propagates_default_storage_engine() {
    let app = StandaloneManagedLakeConfig {
        warehouse_uri: "s3://bucket/wh/".to_string(),
        endpoint: "http://localhost:9000".to_string(),
        access_key_id: "ak".to_string(),
        access_key_secret: "sk".to_string(),
        region: None,
        enable_path_style_access: true,
        mv_default_storage_engine: Some("iceberg".to_string()),
        mv_iceberg_warehouse_location: None,
    };
    let cfg = ManagedLakeConfig::from_app_config(app).expect("config");
    assert_eq!(cfg.mv_default_storage_engine, "iceberg");
    assert!(cfg.mv_iceberg_warehouse_location.is_none());
}

#[test]
fn managed_lake_config_defaults_storage_engine_to_managed_lake() {
    let app = StandaloneManagedLakeConfig {
        warehouse_uri: "s3://bucket/wh/".to_string(),
        endpoint: "http://localhost:9000".to_string(),
        access_key_id: "ak".to_string(),
        access_key_secret: "sk".to_string(),
        region: None,
        enable_path_style_access: true,
        mv_default_storage_engine: None,
        mv_iceberg_warehouse_location: None,
    };
    let cfg = ManagedLakeConfig::from_app_config(app).expect("config");
    assert_eq!(cfg.mv_default_storage_engine, "managed_lake");
}
```

- [ ] **Step 2: Run test to verify failure**

Run:

```bash
cargo test --lib connector::starrocks::managed::config::tests::managed_lake_config_propagates_default_storage_engine -- --nocapture
```

Expected: compile error — fields do not exist.

- [ ] **Step 3: Add fields to `StandaloneManagedLakeConfig`**

In `src/common/app_config.rs` find `StandaloneManagedLakeConfig`. Add:

```rust
#[serde(default)]
pub mv_default_storage_engine: Option<String>,
#[serde(default)]
pub mv_iceberg_warehouse_location: Option<String>,
```

- [ ] **Step 4: Forward fields into `ManagedLakeConfig`**

In `src/connector/starrocks/managed/config.rs` change the struct and constructor:

```rust
#[derive(Clone, Debug)]
pub(crate) struct ManagedLakeConfig {
    pub(crate) warehouse_uri: String,
    pub(crate) s3: S3StoreConfig,
    pub(crate) mv_default_storage_engine: String,
    pub(crate) mv_iceberg_warehouse_location: Option<String>,
}

impl ManagedLakeConfig {
    pub(crate) fn from_app_config(config: AppManagedLakeConfig) -> Result<Self, String> {
        let warehouse_uri = config.warehouse_uri.trim().trim_end_matches('/').to_string();
        if warehouse_uri.is_empty() {
            return Err("standalone managed lake warehouse_uri is empty".to_string());
        }
        let (bucket, root) = parse_s3_path(&warehouse_uri)?;
        let mv_default_storage_engine = config
            .mv_default_storage_engine
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .unwrap_or("managed_lake")
            .to_string();
        if mv_default_storage_engine != "managed_lake" && mv_default_storage_engine != "iceberg" {
            return Err(format!(
                "invalid mv_default_storage_engine `{mv_default_storage_engine}`; allowed: managed_lake, iceberg"
            ));
        }
        Ok(Self {
            warehouse_uri,
            s3: S3StoreConfig {
                endpoint: config.endpoint.trim().to_string(),
                bucket,
                root: root.trim_matches('/').to_string(),
                access_key_id: config.access_key_id.trim().to_string(),
                access_key_secret: config.access_key_secret.trim().to_string(),
                region: config.region.as_ref().map(|value| value.trim().to_string()),
                enable_path_style_access: config.enable_path_style_access,
            },
            mv_default_storage_engine,
            mv_iceberg_warehouse_location: config
                .mv_iceberg_warehouse_location
                .as_deref()
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_string),
        })
    }

    pub(crate) fn mv_iceberg_warehouse(&self) -> String {
        self.mv_iceberg_warehouse_location
            .clone()
            .unwrap_or_else(|| format!("{}/_nova_iceberg_mv", self.warehouse_uri))
    }

    pub(crate) fn tablet_root_path(&self, db_id: i64, table_id: i64, partition_id: i64) -> String {
        format!(
            "{}/db_{db_id}/table_{table_id}/partition_{partition_id}",
            self.warehouse_uri
        )
    }
}
```

- [ ] **Step 5: Run config tests**

Run:

```bash
cargo test --lib connector::starrocks::managed::config::tests -- --nocapture
```

Expected: pass.

- [ ] **Step 6: Commit**

```bash
git add src/common/app_config.rs src/connector/starrocks/managed/config.rs
git commit -m "feat: add mv_default_storage_engine config"
```

---

## Task 4: NovaRocks-Internal `__nova_mv__` Catalog

**Files:**
- Create: `src/connector/starrocks/managed/mv_iceberg_catalog.rs`
- Modify: `src/connector/starrocks/managed/mod.rs`
- Modify: `src/sql/parser/dialect/external_catalog.rs` (or wherever `CREATE EXTERNAL CATALOG` is parsed; locate via `grep -rn "CREATE EXTERNAL CATALOG" src/sql/parser/ --include='*.rs'`).

- [ ] **Step 1: Write failing tests for the internal catalog**

Create `src/connector/starrocks/managed/mv_iceberg_catalog.rs` with the test stubs first (file-scoped tests):

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::connector::starrocks::managed::config::ManagedLakeConfig;
    use crate::runtime::starlet_shard_registry::S3StoreConfig;

    fn local_config(tmp: &std::path::Path) -> ManagedLakeConfig {
        let warehouse = format!("file://{}/wh", tmp.display());
        ManagedLakeConfig {
            warehouse_uri: warehouse,
            s3: S3StoreConfig::default(),
            mv_default_storage_engine: "iceberg".to_string(),
            mv_iceberg_warehouse_location: None,
        }
    }

    #[tokio::test]
    async fn nova_mv_catalog_creates_namespace_and_table_round_trip() {
        let dir = tempfile::tempdir().expect("tempdir");
        let cfg = local_config(dir.path());
        let catalog = build_nova_mv_catalog(&cfg).expect("catalog");
        let ns = iceberg::NamespaceIdent::from_strs(["mydb"]).unwrap();
        catalog
            .create_namespace(&ns, std::collections::HashMap::new())
            .await
            .expect("ns");
        let schema = iceberg::spec::Schema::builder()
            .with_fields(vec![
                std::sync::Arc::new(iceberg::spec::NestedField::required(
                    1,
                    "k",
                    iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::Int),
                )),
            ])
            .build()
            .expect("schema");
        let creation = iceberg::TableCreation::builder()
            .name("mv1".to_string())
            .schema(schema)
            .build();
        let table = catalog.create_table(&ns, creation).await.expect("create");
        assert_eq!(table.identifier().name(), "mv1");
        assert!(catalog.table_exists(&iceberg::TableIdent::from_strs(["mydb", "mv1"]).unwrap()).await.unwrap());
    }
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run:

```bash
cargo test --lib connector::starrocks::managed::mv_iceberg_catalog::tests::nova_mv_catalog_creates_namespace_and_table_round_trip -- --nocapture
```

Expected: compile error — `build_nova_mv_catalog` does not exist yet.

- [ ] **Step 3: Implement `build_nova_mv_catalog`**

Replace the test-only file with:

```rust
//! Builds the NovaRocks-internal `__nova_mv__` Iceberg catalog used as the
//! physical store for materialized views with `storage_engine = 'iceberg'`.
//!
//! This catalog is private — it is never registered with the user-visible
//! `IcebergCatalogRegistry` and the parser rejects `__nova_mv__` as a user
//! catalog name.

use std::sync::Arc;

use iceberg::Catalog;
use iceberg::io::FileIO;

use crate::connector::iceberg::catalog::hadoop_catalog::HadoopFileSystemCatalog;
use crate::connector::starrocks::managed::config::ManagedLakeConfig;

/// Reserved catalog name. The parser must reject any user attempt to use this
/// identifier in `CREATE EXTERNAL CATALOG`.
pub(crate) const NOVA_MV_CATALOG_NAME: &str = "__nova_mv__";

/// Build a fresh `HadoopFileSystemCatalog` rooted at the NovaRocks-private MV
/// warehouse derived from `cfg`. Each call returns a new `Arc` — callers are
/// expected to memoize per `StandaloneState` (see Task 5 dispatcher).
pub(crate) fn build_nova_mv_catalog(
    cfg: &ManagedLakeConfig,
) -> Result<Arc<dyn Catalog>, String> {
    let warehouse = cfg.mv_iceberg_warehouse();
    let file_io = build_file_io(cfg, &warehouse)?;
    let catalog = HadoopFileSystemCatalog::new(file_io, warehouse);
    Ok(Arc::new(catalog))
}

fn build_file_io(cfg: &ManagedLakeConfig, warehouse: &str) -> Result<FileIO, String> {
    if let Some(scheme_end) = warehouse.find("://") {
        let scheme = &warehouse[..scheme_end];
        match scheme {
            "file" => FileIO::from_path(warehouse)
                .and_then(|builder| builder.build())
                .map_err(|e| format!("build local FileIO for nova_mv catalog failed: {e}")),
            "s3" | "s3a" | "oss" => {
                let mut builder = FileIO::from_path(warehouse)
                    .map_err(|e| format!("build s3 FileIO for nova_mv catalog failed: {e}"))?;
                builder = builder
                    .with_prop("s3.endpoint", &cfg.s3.endpoint)
                    .with_prop("s3.access-key-id", &cfg.s3.access_key_id)
                    .with_prop("s3.secret-access-key", &cfg.s3.access_key_secret)
                    .with_prop(
                        "s3.path-style-access",
                        if cfg.s3.enable_path_style_access { "true" } else { "false" },
                    );
                if let Some(region) = &cfg.s3.region {
                    builder = builder.with_prop("s3.region", region);
                }
                builder.build().map_err(|e| format!("build s3 FileIO failed: {e}"))
            }
            other => Err(format!("unsupported nova_mv warehouse scheme `{other}`")),
        }
    } else {
        Err(format!("nova_mv warehouse `{warehouse}` is not a URI"))
    }
}
```

Re-add the `#[cfg(test)] mod tests` block from Step 1 underneath the implementation.

- [ ] **Step 4: Export the module**

In `src/connector/starrocks/managed/mod.rs` add:

```rust
pub(crate) mod mv_iceberg_catalog;
```

next to the existing module declarations (alphabetical order if present).

- [ ] **Step 5: Reject `__nova_mv__` as a user catalog name**

Find the `CREATE EXTERNAL CATALOG` parser entry. Use:

```bash
grep -rnE "create.*external.*catalog|parse_create_external_catalog|CREATE EXTERNAL CATALOG" src/sql/parser/ --include="*.rs"
```

In whichever module owns that parser (likely `src/sql/parser/dialect/external_catalog.rs` or `src/standalone/engine/sqlparse/...`), after the catalog name is parsed, add:

```rust
use crate::connector::starrocks::managed::mv_iceberg_catalog::NOVA_MV_CATALOG_NAME;

if normalized_name.eq_ignore_ascii_case(NOVA_MV_CATALOG_NAME) {
    return Err(format!(
        "`{NOVA_MV_CATALOG_NAME}` is reserved for NovaRocks internal materialized view storage"
    ));
}
```

Add a parser test in the same module:

```rust
#[test]
fn create_external_catalog_rejects_reserved_nova_mv_name() {
    let sql = "CREATE EXTERNAL CATALOG __nova_mv__ PROPERTIES('type' = 'hadoop')";
    let err = crate::sql::parser::parse_sql(sql)
        .err()
        .or_else(|| crate::sql::parser::parse_sql(sql).ok().and_then(|s| s.map(|_| "no err".to_string())))
        .expect("error");
    assert!(format!("{err}").contains("__nova_mv__"));
}
```

If no `CREATE EXTERNAL CATALOG` parser exists yet (NovaRocks may handle it via the legacy `parse_sql_raw` path), instead reject the name in whichever runtime point creates the catalog (e.g., `IcebergCatalogRegistry::create_catalog` in `src/connector/iceberg/catalog/registry.rs` line 66) — same message, same module-level test against that helper.

- [ ] **Step 6: Run catalog tests**

Run:

```bash
cargo test --lib connector::starrocks::managed::mv_iceberg_catalog::tests -- --nocapture
```

Run the parser/registry test added in Step 5 (substitute the path you used).

Expected: all pass.

- [ ] **Step 7: Commit**

```bash
git add src/connector/starrocks/managed/mv_iceberg_catalog.rs \
        src/connector/starrocks/managed/mod.rs \
        # add the file you modified in Step 5
git commit -m "feat: add nova_mv internal iceberg catalog"
```

---

## Task 5: storage_engine Dispatch Stubs

**Files:**
- Modify: `src/connector/starrocks/managed/mv_ddl.rs`
- Modify: `src/connector/starrocks/managed/mv_refresh.rs`
- Modify: `src/connector/starrocks/managed/mod.rs`
- Create: `src/connector/starrocks/managed/mv_refresh_iceberg.rs` (stub only)

This task wires the dispatch points but leaves the iceberg path returning an explicit `not yet implemented` error so the next tasks can fill it in incrementally.

- [ ] **Step 1: Write a failing dispatch test**

Append to the existing `#[cfg(test)] mod tests` in `src/connector/starrocks/managed/mv_ddl.rs`:

```rust
#[test]
fn create_mv_routes_iceberg_storage_engine_to_phase4_path() {
    let stmt_sql = "CREATE MATERIALIZED VIEW analytics.mv1 \
        DISTRIBUTED BY HASH(k) BUCKETS 2 \
        PROPERTIES('storage_engine' = 'iceberg') \
        AS SELECT k FROM ice.ns.t";
    let stmt = parse_create_mv(stmt_sql);
    // resolve_storage_engine takes (PROPERTIES, default_from_config) and returns the resolved enum.
    let resolved = resolve_mv_storage_engine(&stmt.properties, "managed_lake")
        .expect("resolve");
    assert_eq!(resolved, ManagedMvStorageEngine::Iceberg);
}

#[test]
fn create_mv_uses_default_when_property_missing() {
    let stmt_sql = "CREATE MATERIALIZED VIEW analytics.mv1 \
        DISTRIBUTED BY HASH(k) BUCKETS 2 \
        AS SELECT k FROM ice.ns.t";
    let stmt = parse_create_mv(stmt_sql);
    let resolved = resolve_mv_storage_engine(&stmt.properties, "iceberg")
        .expect("resolve");
    assert_eq!(resolved, ManagedMvStorageEngine::Iceberg);
}

#[test]
fn create_mv_rejects_unknown_storage_engine() {
    let stmt_sql = "CREATE MATERIALIZED VIEW analytics.mv1 \
        DISTRIBUTED BY HASH(k) BUCKETS 2 \
        PROPERTIES('storage_engine' = 'duckdb') \
        AS SELECT k FROM ice.ns.t";
    let stmt = parse_create_mv(stmt_sql);
    let err = resolve_mv_storage_engine(&stmt.properties, "managed_lake").unwrap_err();
    assert!(err.contains("duckdb"));
}

// Re-uses the existing `parse_create_mv` helper at the top of the
// `mv_ddl::tests` module (see `mv_ddl.rs:875`). No new helper is added.
```

- [ ] **Step 2: Run the failing dispatch tests**

```bash
cargo test --lib connector::starrocks::managed::mv_ddl::tests::create_mv_routes_iceberg_storage_engine_to_phase4_path -- --nocapture
```

Expected: compile error — `resolve_mv_storage_engine` does not exist.

- [ ] **Step 3: Add `resolve_mv_storage_engine` and dispatcher in `mv_ddl.rs`**

Near the top of `src/connector/starrocks/managed/mv_ddl.rs` (after existing `use` imports), add:

```rust
use crate::connector::starrocks::managed::store::ManagedMvStorageEngine;

pub(crate) fn resolve_mv_storage_engine(
    properties: &[(String, String)],
    default_from_config: &str,
) -> Result<ManagedMvStorageEngine, String> {
    let property = properties
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case("storage_engine"))
        .map(|(_, v)| v.as_str());
    let raw = property.unwrap_or(default_from_config);
    ManagedMvStorageEngine::parse_sql_str(raw)
}
```

Inside `create_mv` (line 91), replace the body (after the early validation but before any managed-lake-specific work) with a dispatcher. Concretely, insert this branch right after `if managed.contains_table(&db_name, &mv_name)? { ... }` (line 143) is checked but **before** any tablet allocation:

```rust
let default_engine = state
    .managed_lake_config
    .as_ref()
    .map(|c| c.mv_default_storage_engine.as_str())
    .unwrap_or("managed_lake");
let storage_engine = resolve_mv_storage_engine(&stmt.properties, default_engine)?;
if storage_engine == ManagedMvStorageEngine::Iceberg {
    return crate::connector::starrocks::managed::mv_refresh_iceberg::create_iceberg_mv(
        state,
        current_database,
        stmt,
    );
}
```

Apply the same pattern to `drop_mv` (line 352): after locating the MV row, branch on `mv_row.storage_engine` and dispatch to `mv_refresh_iceberg::drop_iceberg_mv` for the iceberg path.

- [ ] **Step 4: Add the stub iceberg module**

Create `src/connector/starrocks/managed/mv_refresh_iceberg.rs` with stubs:

```rust
//! Phase4a: projection/filter materialized views backed by Iceberg tables in
//! the NovaRocks-internal `__nova_mv__` catalog. Aggregate shapes (phase4b)
//! and any unsupported MV definitions are rejected here.

use std::sync::Arc;

use crate::sql::parser::ast::{
    CreateMaterializedViewStmt, DropMaterializedViewStmt, RefreshMaterializedViewStmt,
};
use crate::standalone::engine::StandaloneState;
use crate::standalone::engine::statement::StatementResult;

pub(crate) fn create_iceberg_mv(
    _state: &Arc<StandaloneState>,
    _current_database: &str,
    _stmt: &CreateMaterializedViewStmt,
) -> Result<StatementResult, String> {
    Err("CREATE MATERIALIZED VIEW with storage_engine='iceberg' is not yet implemented (phase4a in progress)".to_string())
}

pub(crate) fn refresh_iceberg_mv(
    _state: &Arc<StandaloneState>,
    _current_database: &str,
    _stmt: &RefreshMaterializedViewStmt,
) -> Result<StatementResult, String> {
    Err("REFRESH MATERIALIZED VIEW with storage_engine='iceberg' is not yet implemented (phase4a in progress)".to_string())
}

pub(crate) fn drop_iceberg_mv(
    _state: &Arc<StandaloneState>,
    _current_database: &str,
    _stmt: &DropMaterializedViewStmt,
) -> Result<StatementResult, String> {
    Err("DROP MATERIALIZED VIEW with storage_engine='iceberg' is not yet implemented (phase4a in progress)".to_string())
}
```

In `src/connector/starrocks/managed/mod.rs` add:

```rust
pub(crate) mod mv_refresh_iceberg;
```

- [ ] **Step 5: Add refresh dispatch**

In `src/connector/starrocks/managed/mv_refresh.rs::refresh_mv` (line 65), after `let mv_row = ...` at line 89-96, add:

```rust
if mv_row.storage_engine == crate::connector::starrocks::managed::store::ManagedMvStorageEngine::Iceberg {
    drop(_refresh_guard);
    return crate::connector::starrocks::managed::mv_refresh_iceberg::refresh_iceberg_mv(
        state,
        current_database,
        stmt,
    );
}
```

(The `drop(_refresh_guard)` releases the global mv-refresh lock so `mv_refresh_iceberg` can acquire it under its own scoping.)

- [ ] **Step 6: Run the dispatch tests and verify they pass**

Run:

```bash
cargo test --lib connector::starrocks::managed::mv_ddl::tests::create_mv_routes_iceberg_storage_engine_to_phase4_path -- --nocapture
cargo test --lib connector::starrocks::managed::mv_ddl::tests::create_mv_uses_default_when_property_missing -- --nocapture
cargo test --lib connector::starrocks::managed::mv_ddl::tests::create_mv_rejects_unknown_storage_engine -- --nocapture
```

Expected: all pass.

- [ ] **Step 7: Build the entire crate**

Run:

```bash
cargo build
```

Expected: build succeeds; the `not yet implemented` errors are runtime errors only, not compile errors.

- [ ] **Step 8: Commit**

```bash
git add src/connector/starrocks/managed/mod.rs \
        src/connector/starrocks/managed/mv_ddl.rs \
        src/connector/starrocks/managed/mv_refresh.rs \
        src/connector/starrocks/managed/mv_refresh_iceberg.rs
git commit -m "feat: dispatch mv operations on storage_engine"
```

---

## Task 6: CREATE Iceberg-Backed MV (Projection/Filter)

**Files:**
- Modify: `src/connector/starrocks/managed/mv_refresh_iceberg.rs`
- Modify: `src/connector/starrocks/managed/mv_ddl.rs` (extract `allocate_managed_mv_table_row` helper)
- Modify: `src/connector/starrocks/managed/store.rs` (add `insert_iceberg_mv_row` helper)

> **Test scope:** Phase 1-3 mv tests are all helper-level (no end-to-end test harness exists in `mv_ddl::tests` or `mv_refresh::tests`). Phase 4a follows that convention: each helper added in this task gets a small unit test, while end-to-end CREATE → REFRESH → DROP verification is the SQL regression case in Task 11.

- [ ] **Step 1: Write failing helper-level tests**

Append to `src/connector/starrocks/managed/mv_refresh_iceberg.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::DataType;
    use crate::sql::analysis::OutputColumn;

    fn output_col(name: &str, ty: DataType, nullable: bool) -> OutputColumn {
        OutputColumn { name: name.to_string(), data_type: ty, nullable }
    }

    #[test]
    fn build_iceberg_schema_maps_int_bigint_string() {
        let cols = vec![
            output_col("k", DataType::Int32, false),
            output_col("v", DataType::Int64, true),
            output_col("s", DataType::Utf8, true),
        ];
        let schema = build_iceberg_schema_from_outputs(&cols).expect("schema");
        assert_eq!(schema.as_struct().fields().len(), 3);
        assert_eq!(schema.as_struct().fields()[0].name, "k");
        assert!(schema.as_struct().fields()[0].required);
        assert_eq!(schema.as_struct().fields()[1].name, "v");
        assert!(!schema.as_struct().fields()[1].required);
    }

    #[test]
    fn arrow_data_type_to_iceberg_rejects_unsupported_types() {
        let err = arrow_data_type_to_iceberg_primitive(
            &DataType::Map(std::sync::Arc::new(arrow::datatypes::Field::new(
                "entries",
                DataType::Struct(arrow::datatypes::Fields::empty()),
                false,
            )), false),
        )
        .unwrap_err();
        assert!(err.to_lowercase().contains("unsupported"));
    }
}
```

- [ ] **Step 2: Run the failing helper tests**

```bash
cargo test --lib connector::starrocks::managed::mv_refresh_iceberg::tests::build_iceberg_schema_maps_int_bigint_string -- --nocapture
cargo test --lib connector::starrocks::managed::mv_refresh_iceberg::tests::arrow_data_type_to_iceberg_rejects_unsupported_types -- --nocapture
```

Expected: both fail because `build_iceberg_schema_from_outputs` and `arrow_data_type_to_iceberg_primitive` do not yet exist.

- [ ] **Step 3: Add `insert_iceberg_mv_row` to the metadata store**

In `src/connector/starrocks/managed/store.rs`, add a helper next to `update_mv_iceberg_refresh_metadata` (introduced in Task 1):

```rust
pub(crate) struct InsertIcebergMvRowRequest {
    pub mv_id: i64,
    pub select_sql: String,
    pub base_table_refs: Vec<IcebergTableRef>,
    pub iceberg_table_identifier: String,
}

pub(crate) fn insert_iceberg_mv_row(
    &self,
    request: InsertIcebergMvRowRequest,
) -> Result<(), String> {
    let conn = self.connection()?;
    let base_refs_json = serde_json::to_string(&request.base_table_refs)
        .map_err(|e| format!("serialize base_table_refs failed: {e}"))?;
    let now_ms = current_unix_ms();
    conn.execute(
        "INSERT INTO materialized_views(
            mv_id, select_sql, refresh_mode, base_table_refs_json,
            last_refresh_ms, last_refresh_rows, last_refresh_snapshots_json,
            created_at_ms, storage_engine, iceberg_table_identifier,
            last_refreshed_iceberg_snapshot_id
        ) VALUES (
            ?1, ?2, 'DEFERRED_MANUAL', ?3,
            NULL, NULL, NULL,
            ?4, 'iceberg', ?5, NULL
        )",
        rusqlite::params![
            request.mv_id,
            request.select_sql,
            base_refs_json,
            now_ms,
            request.iceberg_table_identifier,
        ],
    )
    .map_err(|e| format!("insert iceberg mv row failed: {e}"))?;
    Ok(())
}
```

- [ ] **Step 4: Implement `create_iceberg_mv`**

Replace the stub in `src/connector/starrocks/managed/mv_refresh_iceberg.rs`:

```rust
use std::collections::HashMap;
use std::sync::Arc;

use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
use iceberg::{Catalog, NamespaceIdent, TableCreation, TableIdent};

use crate::connector::starrocks::managed::config::ManagedLakeConfig;
use crate::connector::starrocks::managed::mv_ddl::{analyze_mv_select, extract_base_table_refs};
use crate::connector::starrocks::managed::mv_iceberg_catalog::{
    NOVA_MV_CATALOG_NAME, build_nova_mv_catalog,
};
use crate::connector::starrocks::managed::mv_shape::{IncrementalMvShape, classify_incremental_mv_query};
use crate::connector::starrocks::managed::store::InsertIcebergMvRowRequest;
use crate::runtime::global_async_runtime::data_block_on;
use crate::sql::parser::ast::{
    CreateMaterializedViewStmt, DropMaterializedViewStmt, RefreshMaterializedViewStmt,
};
use crate::standalone::engine::StandaloneState;
use crate::standalone::engine::statement::StatementResult;

pub(crate) fn create_iceberg_mv(
    state: &Arc<StandaloneState>,
    current_database: &str,
    stmt: &CreateMaterializedViewStmt,
) -> Result<StatementResult, String> {
    let (db_name, mv_name) = crate::connector::starrocks::managed::mv_ddl::resolve_mv_name(
        &stmt.name,
        current_database,
    )?;
    let cfg = state
        .managed_lake_config
        .as_ref()
        .ok_or_else(|| "managed lake config required for iceberg mv".to_string())?
        .clone();
    let metadata_store = state
        .metadata_store
        .as_ref()
        .ok_or_else(|| "sqlite metadata store required for iceberg mv".to_string())?;

    // 1. Analyze and classify shape — phase4a only accepts projection/filter.
    let analysis = analyze_mv_select(state, current_database, &stmt.select_query)?;
    let base_refs = extract_base_table_refs(&analysis.resolved_refs)?;
    let shape = classify_incremental_mv_query(&stmt.select_query)?;
    if !matches!(shape, IncrementalMvShape::ProjectionFilter(_)) {
        return Err(
            "phase4a iceberg-backed materialized views support only projection/filter shapes; aggregates are phase4b"
                .to_string(),
        );
    }

    // 2. Allocate a managed-lake table_id for the MV (we still register it
    //    in the managed-lake `tables` row so SHOW MATERIALIZED VIEWS works
    //    uniformly, but no tablets are allocated for iceberg storage).
    let mv_id = allocate_iceberg_mv_table_row(state, &db_name, &mv_name)?;

    // 3. Build Iceberg schema from analyzed output columns.
    let schema = build_iceberg_schema_from_outputs(&analysis.output_columns)?;

    // 4. Create namespace + table in __nova_mv__.
    let catalog = build_nova_mv_catalog(&cfg)?;
    let ns = NamespaceIdent::from_strs([&db_name])
        .map_err(|e| format!("namespace ident `{db_name}` failed: {e}"))?;
    data_block_on(async {
        if !catalog.namespace_exists(&ns).await.map_err(|e| e.to_string())? {
            catalog
                .create_namespace(&ns, HashMap::new())
                .await
                .map_err(|e| format!("create namespace `{db_name}` in __nova_mv__ failed: {e}"))?;
        }
        let creation = TableCreation::builder()
            .name(mv_name.clone())
            .schema(schema)
            .build();
        catalog
            .create_table(&ns, creation)
            .await
            .map_err(|e| format!("create iceberg mv table failed: {e}"))?;
        Ok::<_, String>(())
    })?;

    // 5. Persist mv row in SQLite.
    metadata_store.insert_iceberg_mv_row(InsertIcebergMvRowRequest {
        mv_id,
        select_sql: stmt.select_sql.clone(),
        base_table_refs: base_refs,
        iceberg_table_identifier: format!(
            "{NOVA_MV_CATALOG_NAME}.{db_name}.{mv_name}"
        ),
    })?;

    Ok(StatementResult::Ok)
}

fn build_iceberg_schema_from_outputs(
    output_columns: &[crate::sql::analysis::OutputColumn],
) -> Result<Schema, String> {
    let mut fields = Vec::with_capacity(output_columns.len());
    for (idx, col) in output_columns.iter().enumerate() {
        let id = (idx + 1) as i32;
        let primitive = arrow_data_type_to_iceberg_primitive(&col.data_type)?;
        let field = if col.nullable {
            NestedField::optional(id, &col.name, Type::Primitive(primitive))
        } else {
            NestedField::required(id, &col.name, Type::Primitive(primitive))
        };
        fields.push(Arc::new(field));
    }
    Schema::builder()
        .with_fields(fields)
        .build()
        .map_err(|e| format!("build iceberg mv schema failed: {e}"))
}

fn arrow_data_type_to_iceberg_primitive(
    arrow_type: &arrow::datatypes::DataType,
) -> Result<PrimitiveType, String> {
    use arrow::datatypes::{DataType, TimeUnit};
    Ok(match arrow_type {
        DataType::Boolean => PrimitiveType::Boolean,
        DataType::Int32 => PrimitiveType::Int,
        DataType::Int64 => PrimitiveType::Long,
        DataType::Float32 => PrimitiveType::Float,
        DataType::Float64 => PrimitiveType::Double,
        DataType::Date32 => PrimitiveType::Date,
        DataType::Timestamp(TimeUnit::Microsecond, _) => PrimitiveType::Timestamp,
        DataType::Utf8 | DataType::LargeUtf8 => PrimitiveType::String,
        DataType::Binary | DataType::LargeBinary => PrimitiveType::Binary,
        DataType::Decimal128(precision, scale) => PrimitiveType::Decimal {
            precision: *precision as u32,
            scale: *scale as u32,
        },
        other => return Err(format!("iceberg-backed mv: unsupported column type `{other:?}`")),
    })
}

fn allocate_iceberg_mv_table_row(
    state: &Arc<StandaloneState>,
    db_name: &str,
    mv_name: &str,
) -> Result<i64, String> {
    let cfg = state
        .managed_lake_config
        .as_ref()
        .ok_or_else(|| "managed lake config required".to_string())?;
    let mut managed = state
        .managed_lake
        .write()
        .expect("standalone managed lake write lock");
    let mut snapshot = managed.snapshot.clone();
    crate::connector::starrocks::managed::mv_ddl::initialize_global_meta_if_needed(
        &mut snapshot, cfg,
    );
    let database = crate::connector::starrocks::managed::mv_ddl::find_or_create_managed_database(
        &mut snapshot, db_name,
    );
    crate::connector::starrocks::managed::mv_ddl::reclaim_dropping_table_for_reuse(
        &mut snapshot, database.db_id, mv_name,
    )?;
    let table_id = crate::connector::starrocks::managed::mv_ddl::alloc_id(
        &mut snapshot.global.next_table_id,
    );
    snapshot.tables.push(
        crate::connector::starrocks::managed::store::StoredManagedTable {
            table_id,
            db_id: database.db_id,
            name: mv_name.to_string(),
            keys_type: "DUP_KEYS".to_string(),
            bucket_num: 0,
            current_schema_id: 0,
            state: crate::connector::starrocks::managed::store::ManagedTableState::Active,
            kind: crate::connector::starrocks::managed::store::ManagedTableKind::MaterializedView,
        },
    );
    let metadata_store = state
        .metadata_store
        .as_ref()
        .ok_or_else(|| "sqlite metadata store required".to_string())?;
    metadata_store.replace_managed_snapshot(&snapshot)?;
    managed.snapshot = snapshot;
    Ok(table_id)
}
```

> NOTE: This helper assumes `initialize_global_meta_if_needed`, `find_or_create_managed_database`, `reclaim_dropping_table_for_reuse`, and `alloc_id` are `pub(crate)` exports of `mv_ddl.rs`. They are currently `fn` (private) — lift their visibility to `pub(crate)` as part of this step. Adding `pub(crate)` to four free functions does not change behavior. The MV is registered with `bucket_num = 0` and `current_schema_id = 0` because no managed-lake tablets, schemas, partitions, indexes, or txns are allocated for the iceberg storage path; the existing `tables` row only exists so that `SHOW MATERIALIZED VIEWS` and the catalog name-collision check at `mv_ddl.rs:143` work uniformly across both storage engines.

- [ ] **Step 5: Run the CREATE test**

```bash
cargo test --lib connector::starrocks::managed::mv_refresh_iceberg::tests::create_iceberg_mv_creates_namespace_table_and_sqlite_row -- --nocapture
```

Expected: pass.

- [ ] **Step 6: Commit**

```bash
git add src/connector/starrocks/managed/store.rs \
        src/connector/starrocks/managed/mv_ddl.rs \
        src/connector/starrocks/managed/mv_refresh_iceberg.rs
git commit -m "feat: create iceberg-backed mv table and metadata"
```

---

## Task 7: First Refresh Writes Iceberg Snapshot

**Files:**
- Modify: `src/connector/starrocks/managed/mv_refresh_iceberg.rs`
- Modify: `src/connector/starrocks/managed/mv_refresh.rs` (lift `run_mv_select_and_chunks`, `single_snapshot_map`, `load_current_iceberg_base_table`, `plan_append_delta`, `execute_query_for_mv_incremental_refresh`, `query_result_to_chunks` to `pub(crate)` if not already)

> **Test scope:** End-to-end CREATE → REFRESH verification is covered by Task 11's SQL regression. This task adds a helper-level round-trip test for the iceberg writer + commit path that does not require any MV-specific harness.

- [ ] **Step 1: Write the failing writer round-trip test**

Append in `mv_refresh_iceberg::tests`:

```rust
#[test]
fn write_chunks_round_trip_through_iceberg_table() {
    use arrow::array::{Int32Array, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc as StdArc;

    let dir = tempfile::tempdir().expect("tempdir");
    let warehouse = format!("file://{}/wh", dir.path().display());
    let file_io = iceberg::io::FileIO::from_path(&warehouse).unwrap().build().unwrap();
    let catalog = StdArc::new(crate::connector::iceberg::catalog::hadoop_catalog::HadoopFileSystemCatalog::new(
        file_io.clone(),
        warehouse.clone(),
    ));

    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        let ns = iceberg::NamespaceIdent::from_strs(["test_ns"]).unwrap();
        catalog.create_namespace(&ns, std::collections::HashMap::new()).await.unwrap();
        let schema = iceberg::spec::Schema::builder()
            .with_fields(vec![
                StdArc::new(iceberg::spec::NestedField::required(
                    1, "k", iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::Int),
                )),
                StdArc::new(iceberg::spec::NestedField::optional(
                    2, "v", iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::Long),
                )),
            ])
            .build()
            .unwrap();
        let creation = iceberg::TableCreation::builder()
            .name("t".to_string())
            .schema(schema)
            .build();
        let table = catalog.create_table(&ns, creation).await.unwrap();

        let arrow_schema = StdArc::new(ArrowSchema::new(vec![
            Field::new("k", DataType::Int32, false),
            Field::new("v", DataType::Int64, true),
        ]));
        let batch = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![
                StdArc::new(Int32Array::from(vec![1, 2, 3])),
                StdArc::new(Int64Array::from(vec![Some(10), Some(20), None])),
            ],
        ).unwrap();
        let chunk = crate::exec::chunk::Chunk::from_record_batch(batch);

        let written = write_chunks_as_iceberg_data_files(&table, &[chunk]).await.unwrap();
        assert!(!written.is_empty());
        let snapshot_id = commit_fast_append(&table, written).await.unwrap();
        assert!(snapshot_id != 0);

        // Reload through catalog and confirm the snapshot is current.
        let reloaded = catalog
            .load_table(&iceberg::TableIdent::from_strs(["test_ns", "t"]).unwrap())
            .await
            .unwrap();
        assert_eq!(
            reloaded.metadata().current_snapshot().map(|s| s.snapshot_id()),
            Some(snapshot_id),
        );
    });
}
```

> NOTE: `Chunk::from_record_batch` is the existing constructor in `src/exec/chunk/mod.rs`. If its signature differs (e.g., requires additional context), use the closest existing constructor and adjust.

- [ ] **Step 2: Run test to verify failure**

```bash
cargo test --lib connector::starrocks::managed::mv_refresh_iceberg::tests::write_chunks_round_trip_through_iceberg_table -- --nocapture
```

Expected: fail because `write_chunks_as_iceberg_data_files` and `commit_fast_append` do not yet exist.

- [ ] **Step 3: Implement `refresh_iceberg_mv` first-refresh path**

Replace the `refresh_iceberg_mv` stub with:

```rust
pub(crate) fn refresh_iceberg_mv(
    state: &Arc<StandaloneState>,
    current_database: &str,
    stmt: &RefreshMaterializedViewStmt,
) -> Result<StatementResult, String> {
    let (db_name, mv_name) = crate::connector::starrocks::managed::mv_ddl::resolve_mv_name(
        &stmt.name,
        current_database,
    )?;
    let cfg = state
        .managed_lake_config
        .as_ref()
        .ok_or_else(|| "managed lake config required".to_string())?
        .clone();
    let metadata_store = state
        .metadata_store
        .as_ref()
        .ok_or_else(|| "sqlite metadata store required".to_string())?;

    let snapshot = metadata_store.load_snapshot()?.managed;
    let mv_row = snapshot
        .materialized_views
        .iter()
        .find(|m| {
            m.iceberg_table_identifier.as_deref()
                == Some(&format!("__nova_mv__.{db_name}.{mv_name}"))
        })
        .cloned()
        .ok_or_else(|| format!("iceberg mv `{db_name}.{mv_name}` has no metadata row"))?;

    let [base_ref] = mv_row.base_table_refs.as_slice() else {
        return Err("iceberg mv refresh requires a single base table".to_string());
    };
    let loaded = crate::connector::starrocks::managed::mv_refresh::load_current_iceberg_base_table(state, base_ref)?;
    let current_snapshot_id = loaded
        .table
        .metadata()
        .current_snapshot()
        .map(|s| s.snapshot_id());
    let previous_snapshot_id = mv_row.last_refresh_snapshots.get(&base_ref.fqn()).copied();

    match (previous_snapshot_id, current_snapshot_id) {
        (None, None) => Ok(StatementResult::Ok), // base empty, nothing to refresh
        (None, Some(cur)) => first_refresh_iceberg_mv(
            state,
            &cfg,
            metadata_store,
            &db_name,
            &mv_name,
            &mv_row,
            base_ref,
            cur,
        ),
        (Some(prev), Some(cur)) if prev == cur => {
            // No-op metadata refresh: just bump last_refresh_ms.
            metadata_store.update_mv_iceberg_refresh_metadata(
                crate::connector::starrocks::managed::store::UpdateMvIcebergRefreshMetadataRequest {
                    table_id: mv_row.mv_id,
                    last_refresh_rows: mv_row.last_refresh_rows.unwrap_or(0),
                    snapshots: crate::connector::starrocks::managed::mv_refresh::single_snapshot_map(base_ref, cur),
                    iceberg_snapshot_id: mv_row.last_refreshed_iceberg_snapshot_id.unwrap_or(0),
                },
            )?;
            Ok(StatementResult::Ok)
        }
        (Some(prev), Some(cur)) => incremental_refresh_iceberg_mv(
            state,
            &cfg,
            metadata_store,
            &db_name,
            &mv_name,
            &mv_row,
            base_ref,
            prev,
            cur,
            &loaded.table,
        ),
        (Some(_), None) => Err(
            "iceberg mv refresh: base table has no current snapshot but mv has lineage".to_string(),
        ),
    }
}

fn first_refresh_iceberg_mv(
    state: &Arc<StandaloneState>,
    cfg: &ManagedLakeConfig,
    metadata_store: &crate::connector::starrocks::managed::store::SqliteMetadataStore,
    db_name: &str,
    mv_name: &str,
    mv_row: &crate::connector::starrocks::managed::store::StoredMaterializedView,
    base_ref: &crate::connector::starrocks::managed::store::IcebergTableRef,
    base_snapshot_id: i64,
) -> Result<StatementResult, String> {
    let chunks = crate::connector::starrocks::managed::mv_refresh::run_mv_full_select_chunks(
        state, db_name, &mv_row.select_sql,
    )?;
    let total_rows = chunks.iter().map(|c| c.num_rows() as i64).sum();

    let catalog = build_nova_mv_catalog(cfg)?;
    let ident = TableIdent::from_strs([db_name, mv_name])
        .map_err(|e| format!("table ident failed: {e}"))?;

    let new_snapshot_id = data_block_on(async {
        let table = catalog
            .load_table(&ident)
            .await
            .map_err(|e| format!("load mv iceberg table failed: {e}"))?;
        let written = write_chunks_as_iceberg_data_files(&table, &chunks).await?;
        let snapshot_id = commit_fast_append(&table, written).await?;
        Ok::<_, String>(snapshot_id)
    });

    let new_snapshot_id = match new_snapshot_id {
        Ok(id) => id,
        Err(e) => {
            // Rollback by dropping the empty/partial iceberg table.
            let _ = data_block_on(async {
                catalog.drop_table(&ident).await.map_err(|e| e.to_string())
            });
            return Err(format!("first refresh failed (rolled back): {e}"));
        }
    };

    metadata_store.update_mv_iceberg_refresh_metadata(
        crate::connector::starrocks::managed::store::UpdateMvIcebergRefreshMetadataRequest {
            table_id: mv_row.mv_id,
            last_refresh_rows: total_rows,
            snapshots: crate::connector::starrocks::managed::mv_refresh::single_snapshot_map(
                base_ref,
                base_snapshot_id,
            ),
            iceberg_snapshot_id: new_snapshot_id,
        },
    )?;
    Ok(StatementResult::Ok)
}

async fn write_chunks_as_iceberg_data_files(
    table: &iceberg::table::Table,
    chunks: &[crate::exec::chunk::Chunk],
) -> Result<Vec<iceberg::spec::DataFile>, String> {
    use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
    use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
    use iceberg::writer::file_writer::ParquetWriterBuilder;
    use iceberg::writer::file_writer::location_generator::{
        DefaultFileNameGenerator, DefaultLocationGenerator,
    };
    use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
    use parquet::file::properties::WriterProperties;

    let location_gen = DefaultLocationGenerator::new(table.metadata().clone())
        .map_err(|e| format!("location generator: {e}"))?;
    let file_name_gen = DefaultFileNameGenerator::new(
        "novarocks".to_string(),
        None,
        iceberg::spec::DataFileFormat::Parquet,
    );
    let parquet_builder = ParquetWriterBuilder::new(
        WriterProperties::default(),
        table.metadata().current_schema().clone(),
    );
    let rolling_builder = RollingFileWriterBuilder::new_with_default_file_size(parquet_builder)
        .with_file_io(table.file_io().clone())
        .with_location_generator(location_gen)
        .with_file_name_generator(file_name_gen);
    let data_builder = DataFileWriterBuilder::new(rolling_builder);
    let mut writer = data_builder
        .build()
        .await
        .map_err(|e| format!("build iceberg writer: {e}"))?;

    for chunk in chunks {
        let batch = chunk.record_batch().clone();
        writer
            .write(batch)
            .await
            .map_err(|e| format!("iceberg write chunk: {e}"))?;
    }
    let files = writer
        .close()
        .await
        .map_err(|e| format!("close iceberg writer: {e}"))?;
    Ok(files)
}

async fn commit_fast_append(
    table: &iceberg::table::Table,
    data_files: Vec<iceberg::spec::DataFile>,
) -> Result<i64, String> {
    use iceberg::transaction::{ApplyTransactionAction, Transaction};
    let mut txn = Transaction::new(table);
    let mut append = txn.fast_append(None, vec![]).map_err(|e| format!("fast_append: {e}"))?;
    append
        .add_data_files(data_files)
        .map_err(|e| format!("add data files: {e}"))?;
    txn = append.apply().map_err(|e| format!("apply append: {e}"))?;
    let updated = txn.commit().await.map_err(|e| format!("commit append: {e}"))?;
    let snapshot_id = updated
        .metadata()
        .current_snapshot()
        .map(|s| s.snapshot_id())
        .ok_or_else(|| "commit produced no current snapshot".to_string())?;
    Ok(snapshot_id)
}
```

> NOTE: `run_mv_full_select_chunks` is a small public wrapper around `run_mv_select_and_chunks` (`mv_refresh.rs:476`). The implementer must export `run_mv_select_and_chunks` (or wrap it as `pub(crate) fn run_mv_full_select_chunks(state, db_name, select_sql) -> Result<Vec<Chunk>>`) so the iceberg path can reuse the exact query logic without duplication. Same for `single_snapshot_map` and `load_current_iceberg_base_table` — these should already be `pub(crate)`; if not, lift visibility.

- [ ] **Step 4: Run the writer round-trip test**

```bash
cargo test --lib connector::starrocks::managed::mv_refresh_iceberg::tests::write_chunks_round_trip_through_iceberg_table -- --nocapture
```

Expected: pass. End-to-end first-refresh acceptance is covered by Task 11.

- [ ] **Step 5: Commit**

```bash
git add src/connector/starrocks/managed/mv_refresh.rs \
        src/connector/starrocks/managed/mv_refresh_iceberg.rs
git commit -m "feat: first refresh writes iceberg-backed mv data files"
```

---

## Task 8: Incremental Refresh Appends Iceberg Delta

**Files:**
- Modify: `src/connector/starrocks/managed/mv_refresh_iceberg.rs`

> **Test scope:** End-to-end incremental refresh + inconsistent-state detection acceptance is covered by Task 11 SQL regression. This task only adds the implementation; the writer round-trip test from Task 7 already exercises the inner `commit_fast_append` path.

- [ ] **Step 1: Skip — implementation only, no new helper-level test required**

The incremental flow uses the same writer/commit helpers tested in Task 7 plus existing phase2 helpers (`plan_append_delta`, `execute_query_for_mv_incremental_refresh`, `query_result_to_chunks`). The new logic is the snapshot-divergence guard, which has no isolatable side-effect-free helper to unit-test cheaply; SQL regression covers it end-to-end.

- [ ] **Step 2: Implement `incremental_refresh_iceberg_mv`**

Add below `first_refresh_iceberg_mv`:

```rust
fn incremental_refresh_iceberg_mv(
    state: &Arc<StandaloneState>,
    cfg: &ManagedLakeConfig,
    metadata_store: &crate::connector::starrocks::managed::store::SqliteMetadataStore,
    db_name: &str,
    mv_name: &str,
    mv_row: &crate::connector::starrocks::managed::store::StoredMaterializedView,
    base_ref: &crate::connector::starrocks::managed::store::IcebergTableRef,
    previous_snapshot_id: i64,
    current_snapshot_id: i64,
    base_table: &iceberg::table::Table,
) -> Result<StatementResult, String> {
    let delta = crate::connector::starrocks::managed::mv_refresh::plan_append_delta(
        base_table,
        previous_snapshot_id,
    )?;
    if delta.current_snapshot_id != current_snapshot_id {
        return Err(format!(
            "iceberg mv incremental refresh: delta snapshot mismatch (expected {current_snapshot_id}, got {})",
            delta.current_snapshot_id,
        ));
    }

    let chunks = crate::connector::starrocks::managed::mv_refresh::execute_query_for_mv_incremental_refresh(
        state,
        db_name,
        &mv_row.select_sql,
        base_ref,
        delta.added_files,
    )
    .and_then(crate::connector::starrocks::managed::mv_refresh::query_result_to_chunks)?;
    let added_rows = chunks.iter().map(|c| c.num_rows() as i64).sum::<i64>();

    let catalog = build_nova_mv_catalog(cfg)?;
    let ident = TableIdent::from_strs([db_name, mv_name])
        .map_err(|e| format!("table ident: {e}"))?;
    let new_snapshot_id = data_block_on(async {
        let table = catalog.load_table(&ident).await.map_err(|e| e.to_string())?;
        let prior_snapshot = table.metadata().current_snapshot().map(|s| s.snapshot_id());
        if prior_snapshot != mv_row.last_refreshed_iceberg_snapshot_id {
            return Err(format!(
                "iceberg mv `{db_name}.{mv_name}` is in inconsistent state: \
                 sqlite recorded snapshot {:?} but iceberg current is {:?}; \
                 manual reconcile required (drop and recreate)",
                mv_row.last_refreshed_iceberg_snapshot_id, prior_snapshot,
            ));
        }
        let written = write_chunks_as_iceberg_data_files(&table, &chunks).await?;
        commit_fast_append(&table, written).await
    })?;

    let new_total_rows = mv_row.last_refresh_rows.unwrap_or(0) + added_rows;
    metadata_store.update_mv_iceberg_refresh_metadata(
        crate::connector::starrocks::managed::store::UpdateMvIcebergRefreshMetadataRequest {
            table_id: mv_row.mv_id,
            last_refresh_rows: new_total_rows,
            snapshots: crate::connector::starrocks::managed::mv_refresh::single_snapshot_map(
                base_ref,
                current_snapshot_id,
            ),
            iceberg_snapshot_id: new_snapshot_id,
        },
    )?;
    Ok(StatementResult::Ok)
}
```

> NOTE: `execute_query_for_mv_incremental_refresh`, `query_result_to_chunks`, and `plan_append_delta` are already `pub(crate)` (or close to) in `mv_refresh.rs` — the phase3 aggregate path uses them. If any are private, lift to `pub(crate)`.

- [ ] **Step 3: Build verification**

```bash
cargo build
```

Expected: succeeds. End-to-end acceptance (incremental refresh + inconsistent-state guard) is covered by Task 11 SQL regression queries 4-6.

- [ ] **Step 4: Commit**

```bash
git add src/connector/starrocks/managed/mv_refresh.rs \
        src/connector/starrocks/managed/mv_refresh_iceberg.rs
git commit -m "feat: incremental refresh appends iceberg mv delta"
```

---

## Task 9: DROP Iceberg-Backed MV

**Files:**
- Modify: `src/connector/starrocks/managed/mv_refresh_iceberg.rs`
- Modify: `src/connector/starrocks/managed/store.rs` (add `delete_iceberg_mv_row` if not subsumed by existing helpers)

> **Test scope:** End-to-end DROP acceptance is covered by Task 11 SQL regression query 8. This task implements the path; no isolated helper-level test is needed because the implementation is composed entirely of two existing primitives (`purge_retired_table_metadata` + `Catalog::drop_table`).

- [ ] **Step 1: Implement `drop_iceberg_mv`**

Replace the stub:

```rust
pub(crate) fn drop_iceberg_mv(
    state: &Arc<StandaloneState>,
    current_database: &str,
    stmt: &DropMaterializedViewStmt,
) -> Result<StatementResult, String> {
    let (db_name, mv_name) = crate::connector::starrocks::managed::mv_ddl::resolve_mv_name(
        &stmt.name,
        current_database,
    )?;
    let cfg = state
        .managed_lake_config
        .as_ref()
        .ok_or_else(|| "managed lake config required".to_string())?
        .clone();
    let metadata_store = state
        .metadata_store
        .as_ref()
        .ok_or_else(|| "sqlite metadata store required".to_string())?;

    let snapshot = metadata_store.load_snapshot()?.managed;
    let mv_row = snapshot
        .materialized_views
        .iter()
        .find(|m| {
            m.iceberg_table_identifier.as_deref()
                == Some(&format!("__nova_mv__.{db_name}.{mv_name}"))
        })
        .cloned();
    let Some(mv_row) = mv_row else {
        if stmt.if_exists {
            return Ok(StatementResult::Ok);
        }
        return Err(format!("materialized view `{db_name}.{mv_name}` does not exist"));
    };

    // 1. Delete SQLite row first (single-statement, atomic).
    metadata_store.purge_retired_table_metadata(mv_row.mv_id)?;
    // The above already removes the materialized_views row + the tables row
    // (it is the same helper used by managed-lake DROP).

    // 2. Drop the iceberg table. If this fails, log and return error;
    //    operator gets to clean up via reconcile (out of scope).
    let catalog = build_nova_mv_catalog(&cfg)?;
    let ident = TableIdent::from_strs([&db_name, &mv_name])
        .map_err(|e| format!("table ident: {e}"))?;
    data_block_on(async {
        catalog
            .drop_table(&ident)
            .await
            .map_err(|e| format!("drop iceberg mv table failed: {e}"))
    })?;
    Ok(StatementResult::Ok)
}
```

> NOTE: If `purge_retired_table_metadata` does not delete the `materialized_views` row + the `tables` row in a single transaction, add a sibling helper `delete_iceberg_mv_row(table_id)` to `store.rs` that runs both deletes inside `BEGIN ... COMMIT`.

- [ ] **Step 2: Build verification**

```bash
cargo build
```

Expected: succeeds. End-to-end DROP acceptance is covered by Task 11 SQL regression query 8.

- [ ] **Step 3: Commit**

```bash
git add src/connector/starrocks/managed/mv_refresh_iceberg.rs \
        src/connector/starrocks/managed/store.rs
git commit -m "feat: drop iceberg-backed mv removes sqlite and iceberg state"
```

---

## Task 10: SHOW MATERIALIZED VIEWS Includes storage_engine

**Files:**
- Modify: `src/connector/starrocks/managed/mv_ddl.rs` (`list_mvs` at line 393)
- Modify: any `ShowMaterializedViews` formatter — locate via `grep -rn "ShowMaterializedViews" src/ --include='*.rs'`

> **Test scope:** End-to-end SHOW output is covered by Task 11 SQL regression query 7. This task only adds the column to the row struct + formatter.

- [ ] **Step 1: Add the column to the list-row struct**

Find the row struct returned by `list_mvs`:

```bash
grep -nE "pub struct.*[Mm]aterialized.*Entry|fn list_mvs" src/connector/starrocks/managed/mv_ddl.rs
```

Add:

```rust
pub storage_engine: String,
```

In `list_mvs` body, when constructing each entry, set:

```rust
storage_engine: mv.storage_engine.as_sql_str().to_string(),
```

- [ ] **Step 2: Update the SHOW formatter**

Locate the formatter that turns `MaterializedViewListEntry` into the rows returned to the client:

```bash
grep -rnE "SHOW MATERIALIZED VIEWS|show_materialized_views|StatementResult::Rows" src/ --include="*.rs" | head -20
```

Append a `storage_engine` column header and corresponding cell. Match the column ordering used by phase1-3 (typically: `name`, `storage_engine`, `last_refresh_ms`, `last_refresh_rows`).

- [ ] **Step 3: Build verification**

```bash
cargo build
```

Expected: succeeds.

- [ ] **Step 4: Commit**

```bash
git add src/connector/starrocks/managed/mv_ddl.rs \
        # any show formatter you touched
git commit -m "feat: show materialized views includes storage_engine"
```

---

## Task 11: SQL Regression For Iceberg-Backed MV

**Files:**
- Create: `sql-tests/write-path/sql/iceberg_backed_mv_projection_filter.sql`
- Create: `sql-tests/write-path/result/iceberg_backed_mv_projection_filter.result`

- [ ] **Step 1: Add SQL case**

Create `sql-tests/write-path/sql/iceberg_backed_mv_projection_filter.sql`:

```sql
-- @order_sensitive=true
-- @tags=write_path,managed_lake,mv,iceberg,phase4a
-- Test Objective:
-- 1. CREATE MATERIALIZED VIEW with PROPERTIES('storage_engine' = 'iceberg') succeeds.
-- 2. First REFRESH writes visible projection/filter result into the iceberg-backed MV.
-- 3. Append-only incremental REFRESH appends only new rows.
-- 4. DROP cleans up sqlite + iceberg.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG mv_phase4a_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "hadoop",
  "iceberg.catalog.warehouse" = "${managed_lake_warehouse}/iceberg_mv_phase4a_${uuid0}",
  "aws.s3.endpoint" = "${oss_endpoint}",
  "aws.s3.access_key" = "${oss_ak}",
  "aws.s3.secret_key" = "${oss_sk}",
  "aws.s3.enable_path_style_access" = "true"
);
CREATE DATABASE mv_phase4a_${uuid0}.ns_${uuid0};
CREATE TABLE mv_phase4a_${uuid0}.ns_${uuid0}.orders (
  k1 INT,
  v2 BIGINT
);
INSERT INTO mv_phase4a_${uuid0}.ns_${uuid0}.orders VALUES
  (1, 10), (1, 20), (2, 40), (3, 0);

CREATE MATERIALIZED VIEW ${case_db}.proj_mv
DISTRIBUTED BY HASH(k1) BUCKETS 2
PROPERTIES ('storage_engine' = 'iceberg')
AS SELECT k1, v2 FROM mv_phase4a_${uuid0}.ns_${uuid0}.orders WHERE v2 > 0;

-- query 2
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW ${case_db}.proj_mv;

-- query 3
SELECT k1, v2 FROM ${case_db}.proj_mv ORDER BY k1, v2;

-- query 4
-- @skip_result_check=true
INSERT INTO mv_phase4a_${uuid0}.ns_${uuid0}.orders VALUES
  (1, 70), (4, 5);

-- query 5
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW ${case_db}.proj_mv;

-- query 6
SELECT k1, v2 FROM ${case_db}.proj_mv ORDER BY k1, v2;

-- query 7
-- SHOW MATERIALIZED VIEWS includes storage_engine column.
SHOW MATERIALIZED VIEWS FROM ${case_db};

-- query 8
-- @skip_result_check=true
DROP MATERIALIZED VIEW ${case_db}.proj_mv;
DROP TABLE mv_phase4a_${uuid0}.ns_${uuid0}.orders FORCE;
DROP DATABASE mv_phase4a_${uuid0}.ns_${uuid0};
DROP CATALOG mv_phase4a_${uuid0};
```

- [ ] **Step 2: Add expected result file**

Create `sql-tests/write-path/result/iceberg_backed_mv_projection_filter.result`:

```text
-- query 3
1	10
1	20
2	40
-- query 6
1	10
1	20
1	70
2	40
4	5
-- query 7
proj_mv	iceberg	...
```

> NOTE: The query 7 expected row uses `...` because `SHOW MATERIALIZED VIEWS` includes columns (last refresh time, row count) whose values are non-deterministic. The implementing engineer should run the case once with `--mode record` to capture the exact row, then trim the timestamp with the same wildcard convention used by other phase1-3 cases. Look at how `managed_lake_mv_basic.result` handles `SHOW`.

- [ ] **Step 3: Run unit and SQL tests**

Start standalone server in another terminal:

```bash
NO_PROXY=127.0.0.1,localhost cargo run -- standalone-server --port 9030
```

Run the new case:

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --suite write-path --only iceberg_backed_mv_projection_filter --mode verify --query-timeout 60
```

Expected: pass.

- [ ] **Step 4: Run phase1-3 regression with the same server**

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --suite write-path --only managed_lake_mv_basic,managed_lake_mv_aggregate_ivm --mode verify --query-timeout 60
```

Expected: both pass with no edits required (default `mv_default_storage_engine = managed_lake`).

- [ ] **Step 5: Commit**

```bash
git add sql-tests/write-path/sql/iceberg_backed_mv_projection_filter.sql \
        sql-tests/write-path/result/iceberg_backed_mv_projection_filter.result
git commit -m "test: cover iceberg-backed projection mv refresh"
```

---

## Task 12: Final Verification And Cleanup

- [ ] **Step 1: Run formatter**

```bash
cargo fmt
```

Expected: no output.

- [ ] **Step 2: Build**

```bash
cargo build
```

Expected: succeeds with no warnings introduced by phase4a code.

- [ ] **Step 3: Run focused unit test set**

```bash
cargo test --lib connector::starrocks::managed::store::tests -- --nocapture
cargo test --lib connector::starrocks::managed::config::tests -- --nocapture
cargo test --lib connector::starrocks::managed::mv_iceberg_catalog::tests -- --nocapture
cargo test --lib connector::starrocks::managed::mv_refresh_iceberg::tests -- --nocapture
cargo test --lib sql::parser::dialect::materialized_view::tests -- --nocapture
```

Expected: all pass.

- [ ] **Step 4: Run SQL regression triple**

With standalone server running:

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --suite write-path \
  --only managed_lake_mv_basic,managed_lake_mv_aggregate_ivm,iceberg_backed_mv_projection_filter \
  --mode verify --query-timeout 60
```

Expected: all three pass.

- [ ] **Step 5: Run clippy**

```bash
cargo clippy --all-targets --no-deps -- -D warnings
```

Expected: no new clippy errors.

- [ ] **Step 6: Inspect git diff**

```bash
git status --short
git diff --check
```

Expected: only intended files; no whitespace damage.

- [ ] **Step 7: Final commit if anything was tweaked during verification**

Only if Steps 1-5 caused changes (formatter / clippy fixes):

```bash
git add -u
git commit -m "chore: phase4a final verification fixes"
```
