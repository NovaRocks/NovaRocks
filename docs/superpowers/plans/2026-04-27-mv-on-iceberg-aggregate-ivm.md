# MV On Iceberg Aggregate IVM Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build single-Iceberg-table aggregate materialized-view incremental refresh using hidden aggregate state and internal primary-key upsert semantics.

**Architecture:** Keep projection/filter MV refresh on the current append path, and add a separate aggregate-MV path that creates a primary-key managed table keyed by hidden `__ROW_ID__`. Full refresh writes the same hidden physical layout that incremental refresh consumes. Incremental refresh scans append-only Iceberg delta files, aggregates only the delta rows, merges scalar count/sum states with the active MV rows, then writes the touched groups through the existing primary-key publish/delete-vector machinery.

**Tech Stack:** Rust, Arrow `RecordBatch`/`ArrayRef`, NovaRocks managed-lake metadata store, StarRocks native segment reader/writer, Iceberg append delta planner, `sqlparser` AST.

---

## File Structure

- Modify `src/connector/starrocks/managed/store.rs`: persist managed column `visible` and `is_key` flags; migrate metadata schema from v4 to v5.
- Modify `src/connector/starrocks/managed/catalog.rs`: expose only stored visible columns to the public catalog while keeping physical layout unchanged.
- Modify `src/connector/starrocks/managed/ddl.rs`: rebuild managed tablet schemas with hidden/key flags and include hidden primary-key columns when bootstrapping MV partitions.
- Modify `src/connector/starrocks/lake/schema.rs`: add a create-tablet helper that lets managed DDL patch the generated `TabletSchemaPb` before runtime registration and metadata write.
- Modify `src/connector/starrocks/managed/txn.rs`: add physical-column insert-plan loading and aggregate-MV internal upsert write.
- Modify `src/connector/starrocks/managed/mv_shape.rs`: split projection/filter and aggregate MV shape classification.
- Create `src/connector/starrocks/managed/mv_agg_state.rs`: aggregate MV layout, hidden physical columns, row-id encoding, scalar state materialization and merge.
- Modify `src/connector/starrocks/managed/mv_ddl.rs`: create aggregate MV as a primary-key physical table with hidden row id/state columns.
- Modify `src/connector/starrocks/managed/mv_refresh.rs`: route aggregate full refresh and incremental refresh through the aggregate-state path.
- Modify `src/connector/starrocks/managed/mod.rs`: export the new `mv_agg_state` module.
- Add SQL regression case `sql-tests/write-path/sql/managed_lake_mv_aggregate_ivm.sql` and result file `sql-tests/write-path/result/managed_lake_mv_aggregate_ivm.result`.

## Implementation Constraints

- Do not expose user-facing generic `PRIMARY KEY` or upsert syntax as part of this work.
- Keep Iceberg delete, overwrite, equality-delete, and position-delete snapshots fail-fast through the existing append-delta planner.
- Do not implement reverse/recovery logic for aggregate states.
- Reuse the primary-key publish path for replacement semantics: a full-schema write into a `PRIMARY_KEYS` tablet appends the new rowset and marks older rows with the same primary key deleted through delete vectors.
- Hidden columns are internal: public catalog registration must not expose `__ROW_ID__` or `__AGG_STATE_*`; an explicit user query for those columns should resolve as unknown column.

## Task 1: Persist Hidden And Key Column Flags

**Files:**
- Modify: `src/connector/starrocks/managed/store.rs`

- [ ] **Step 1: Write metadata schema and round-trip tests**

Add these tests in the existing `#[cfg(test)] mod tests` in `store.rs`:

```rust
#[test]
fn init_schema_v5_creates_table_column_flags() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = SqliteMetadataStore::open(dir.path().join("standalone.sqlite"))
        .expect("open fresh store");
    let conn = store.connection().expect("connection");

    let version: i64 = conn
        .query_row("PRAGMA user_version", [], |row| row.get(0))
        .expect("user_version");
    assert_eq!(version, 5);

    let cols: Vec<(String, String, i64)> = {
        let mut stmt = conn
            .prepare(
                "SELECT name, type, \"notnull\"
                 FROM pragma_table_info('table_columns')
                 WHERE name IN ('visible', 'is_key')
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
            ("visible".to_string(), "INTEGER".to_string(), 1),
            ("is_key".to_string(), "INTEGER".to_string(), 1),
        ],
    );
}

#[test]
fn init_schema_migrates_v4_table_column_flags() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("old.sqlite");
    {
        let conn = rusqlite::Connection::open(&path).expect("open");
        conn.execute_batch(
            "
            CREATE TABLE table_columns (
                schema_id INTEGER NOT NULL,
                ordinal INTEGER NOT NULL,
                column_name TEXT NOT NULL,
                logical_type TEXT NOT NULL,
                nullable INTEGER NOT NULL,
                PRIMARY KEY (schema_id, ordinal)
            );
            PRAGMA user_version = 4;
            ",
        )
        .expect("seed v4");
    }

    let store = SqliteMetadataStore::open(&path).expect("open migrates v4");
    let conn = store.connection().expect("connection");
    let version: i64 = conn
        .query_row("PRAGMA user_version", [], |row| row.get(0))
        .expect("user_version");
    assert_eq!(version, 5);

    let visible_col_exists: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM pragma_table_info('table_columns') WHERE name = 'visible'",
            [],
            |row| row.get(0),
        )
        .expect("visible column");
    let is_key_col_exists: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM pragma_table_info('table_columns') WHERE name = 'is_key'",
            [],
            |row| row.get(0),
        )
        .expect("is_key column");
    assert_eq!(visible_col_exists, 1);
    assert_eq!(is_key_col_exists, 1);
}
```

- [ ] **Step 2: Run the new schema tests and verify failure**

Run:

```bash
cargo test --lib connector::starrocks::managed::store::tests::init_schema_v5_creates_table_column_flags -- --nocapture
cargo test --lib connector::starrocks::managed::store::tests::init_schema_migrates_v4_table_column_flags -- --nocapture
```

Expected: both fail because the schema version is still `4` and `table_columns.visible` / `table_columns.is_key` do not exist.

- [ ] **Step 3: Extend `StoredManagedColumn`**

Change the struct to:

```rust
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct StoredManagedColumn {
    pub schema_id: i64,
    pub ordinal: i64,
    pub column_name: String,
    pub logical_type: String,
    pub nullable: bool,
    pub visible: bool,
    pub is_key: bool,
}
```

- [ ] **Step 4: Add v4 to v5 migration**

In `SqliteMetadataStore::init_schema`, accept versions `0`, `4`, and `5`. Before the main `CREATE TABLE IF NOT EXISTS` batch, run this migration for v4:

```rust
if current_version == 4 {
    conn.execute_batch(
        "
        ALTER TABLE table_columns ADD COLUMN visible INTEGER NOT NULL DEFAULT 1;
        ALTER TABLE table_columns ADD COLUMN is_key INTEGER NOT NULL DEFAULT 0;
        PRAGMA user_version = 5;
        ",
    )
    .map_err(|e| format!("migrate standalone metadata schema v4 to v5 failed: {e}"))?;
}
```

Then update the fresh-schema DDL:

```sql
CREATE TABLE IF NOT EXISTS table_columns (
    schema_id INTEGER NOT NULL,
    ordinal INTEGER NOT NULL,
    column_name TEXT NOT NULL,
    logical_type TEXT NOT NULL,
    nullable INTEGER NOT NULL,
    visible INTEGER NOT NULL DEFAULT 1,
    is_key INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (schema_id, ordinal)
);
PRAGMA user_version = 5;
```

Rename the existing `init_schema_v4_creates_tables_with_kind_and_materialized_views_table` test to `init_schema_v5_creates_tables_with_kind_and_materialized_views_table` and change `assert_eq!(version, 4)` to `assert_eq!(version, 5)`.

- [ ] **Step 5: Persist and load the new column flags**

Update `replace_managed_snapshot` insert SQL:

```rust
tx.execute(
    "INSERT INTO table_columns(
        schema_id,
        ordinal,
        column_name,
        logical_type,
        nullable,
        visible,
        is_key
    ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
    params![
        column.schema_id,
        column.ordinal,
        column.column_name,
        column.logical_type,
        bool_to_sql_int(column.nullable),
        bool_to_sql_int(column.visible),
        bool_to_sql_int(column.is_key),
    ],
)
.map_err(|e| format!("persist managed column failed: {e}"))?;
```

Update `load_managed_snapshot` column query:

```rust
"SELECT schema_id, ordinal, column_name, logical_type, nullable, visible, is_key
 FROM table_columns
 ORDER BY schema_id, ordinal"
```

Map rows as:

```rust
Ok(StoredManagedColumn {
    schema_id: row.get(0)?,
    ordinal: row.get(1)?,
    column_name: row.get(2)?,
    logical_type: row.get(3)?,
    nullable: row.get::<_, i64>(4)? != 0,
    visible: row.get::<_, i64>(5)? != 0,
    is_key: row.get::<_, i64>(6)? != 0,
})
```

- [ ] **Step 6: Update all `StoredManagedColumn` construction sites**

For normal tables and existing projection/filter MVs, use:

```rust
visible: true,
is_key: key_desc.columns.iter().any(|key| {
    normalize_identifier(key)
        .map(|key| key == normalize_identifier(&column.name).unwrap_or_default())
        .unwrap_or(false)
}),
```

For test fixtures that do not care about key metadata, set:

```rust
visible: true,
is_key: false,
```

- [ ] **Step 7: Run store tests**

Run:

```bash
cargo test --lib connector::starrocks::managed::store::tests::init_schema_v5_creates_table_column_flags -- --nocapture
cargo test --lib connector::starrocks::managed::store::tests::init_schema_migrates_v4_table_column_flags -- --nocapture
cargo test --lib connector::starrocks::managed::store::tests::managed_snapshot_round_trips_mv_rows_and_kind_column -- --nocapture
```

Expected: all pass.

- [ ] **Step 8: Commit**

```bash
git add src/connector/starrocks/managed/store.rs
git commit -m "feat: persist managed column visibility flags"
```

## Task 2: Keep Hidden Columns Out Of Public Catalog

**Files:**
- Modify: `src/connector/starrocks/managed/catalog.rs`
- Modify: `src/connector/starrocks/managed/ddl.rs`
- Modify: `src/connector/starrocks/managed/txn.rs`

- [ ] **Step 1: Add a catalog test for hidden columns**

Add this test to `catalog.rs` tests:

```rust
#[test]
fn register_managed_table_hides_invisible_columns() {
    let mut schema = test_tablet_schema();
    schema.column.insert(
        0,
        crate::service::grpc_client::proto::starrocks::ColumnPb {
            unique_id: 99,
            name: Some("__row_id__".to_string()),
            r#type: "VARCHAR".to_string(),
            is_key: Some(true),
            aggregation: Some("NONE".to_string()),
            is_nullable: Some(false),
            default_value: None,
            precision: None,
            frac: None,
            length: Some(65533),
            index_length: Some(65533),
            is_bf_column: None,
            referenced_column_id: None,
            referenced_column: None,
            has_bitmap_index: None,
            visible: Some(false),
            children_columns: Vec::new(),
            is_auto_increment: Some(false),
            agg_state_desc: None,
        },
    );

    let mut runtime = test_runtime("analytics", "orders_mv");
    runtime.tablet_schema = schema;
    runtime.columns.insert(
        0,
        StoredManagedColumn {
            schema_id: runtime.table.current_schema_id,
            ordinal: 0,
            column_name: "__row_id__".to_string(),
            logical_type: "STRING".to_string(),
            nullable: false,
            visible: false,
            is_key: true,
        },
    );

    let table = managed_table_def(&runtime).expect("table def");
    let names = table.columns.iter().map(|col| col.name.as_str()).collect::<Vec<_>>();
    assert!(!names.contains(&"__row_id__"));
}
```

- [ ] **Step 2: Run the hidden catalog test and verify failure**

Run:

```bash
cargo test --lib connector::starrocks::managed::catalog::tests::register_managed_table_hides_invisible_columns -- --nocapture
```

Expected: fails because `managed_table_def` iterates all stored columns.

- [ ] **Step 3: Filter public catalog columns by stored visibility**

In `managed_table_def`, change the loop to skip invisible stored columns:

```rust
for column in runtime.columns.iter().filter(|column| column.visible) {
    let schema_column = schema_columns.get(&column.column_name).ok_or_else(|| {
        format!(
            "managed table {}.{} is missing schema metadata for column `{}`",
            runtime.database_name, runtime.table.name, column.column_name
        )
    })?;
    columns.push(ColumnDef {
        name: column.column_name.clone(),
        data_type: arrow_type_from_tablet_column(schema_column)?,
        nullable: column.nullable,
    });
}
```

- [ ] **Step 4: Rebuild managed schemas with hidden key columns included**

In `ddl.rs::request_schema_from_runtime`, do not filter key columns by visibility:

```rust
let key_columns = runtime
    .columns
    .iter()
    .filter(|column| column.is_key)
    .map(|column| Ok(column.column_name.clone()))
    .collect::<Result<Vec<_>, String>>()?;
```

The `columns` vector must still include all `runtime.columns`, not only visible columns.

- [ ] **Step 5: Add physical column derivation in `txn.rs`**

Add this enum near `ManagedInsertPlan`:

```rust
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ManagedInsertColumnMode {
    VisibleOnly,
    Physical,
}
```

Change `load_insert_plan` to call a new internal helper:

```rust
pub(crate) fn load_insert_plan(
    state: &Arc<StandaloneState>,
    resolved: &ResolvedLocalTableName,
    target: PartitionTarget,
) -> Result<ManagedInsertPlan, String> {
    load_insert_plan_with_column_mode(state, resolved, target, ManagedInsertColumnMode::VisibleOnly)
}

pub(crate) fn load_physical_insert_plan(
    state: &Arc<StandaloneState>,
    resolved: &ResolvedLocalTableName,
    target: PartitionTarget,
) -> Result<ManagedInsertPlan, String> {
    load_insert_plan_with_column_mode(state, resolved, target, ManagedInsertColumnMode::Physical)
}
```

Add a physical column derivation helper:

```rust
fn derive_column_defs_from_runtime(
    runtime: &super::catalog::ManagedTableRuntime,
    mode: ManagedInsertColumnMode,
) -> Result<Vec<ColumnDef>, String> {
    runtime
        .columns
        .iter()
        .filter(|column| mode == ManagedInsertColumnMode::Physical || column.visible)
        .map(|column| {
            let schema_column = runtime
                .tablet_schema
                .column
                .iter()
                .find(|schema_column| {
                    schema_column
                        .name
                        .as_deref()
                        .is_some_and(|name| name.eq_ignore_ascii_case(&column.column_name))
                })
                .ok_or_else(|| {
                    format!(
                        "managed table {}.{} is missing tablet schema column `{}`",
                        runtime.database_name, runtime.table.name, column.column_name
                    )
                })?;
            Ok(ColumnDef {
                name: column.column_name.clone(),
                data_type: crate::connector::starrocks::managed::catalog::arrow_type_from_tablet_column(schema_column)?,
                nullable: column.nullable,
            })
        })
        .collect()
}
```

If `arrow_type_from_tablet_column` is private, change it to `pub(crate)`.

- [ ] **Step 6: Run targeted checks**

Run:

```bash
cargo test --lib connector::starrocks::managed::catalog::tests::register_managed_table_hides_invisible_columns -- --nocapture
cargo test --lib connector::starrocks::managed::txn::tests::load_insert_plan -- --nocapture
```

Expected: catalog test passes; if the second command reports no exact test with that name, run `cargo test --lib connector::starrocks::managed::txn -- --nocapture`.

- [ ] **Step 7: Commit**

```bash
git add src/connector/starrocks/managed/catalog.rs src/connector/starrocks/managed/ddl.rs src/connector/starrocks/managed/txn.rs
git commit -m "feat: separate managed visible and physical columns"
```

## Task 3: Patch Managed Tablet Schema Visibility At Creation

**Files:**
- Modify: `src/connector/starrocks/lake/schema.rs`
- Modify: `src/connector/starrocks/managed/ddl.rs`
- Modify: `src/connector/starrocks/managed/mv_ddl.rs`

- [ ] **Step 1: Add a patched create-tablet helper**

In `lake/schema.rs`, keep `create_lake_tablet_from_req` as the public compatibility wrapper and add:

```rust
pub(crate) fn create_lake_tablet_from_req_with_schema_patch<P>(
    request: &crate::agent_service::TCreateTabletReq,
    tablet_root_path: &str,
    s3_config: Option<S3StoreConfig>,
    patch: P,
) -> Result<(), String>
where
    P: FnOnce(&mut TabletSchemaPb) -> Result<(), String>,
{
    let tablet_id = request.tablet_id;
    if tablet_id <= 0 {
        return Err(format!("create_tablet has non-positive tablet_id={tablet_id}"));
    }

    let mut tablet_schema = build_create_tablet_schema(request)?;
    patch(&mut tablet_schema)?;

    let runtime_ctx = TabletWriteContext {
        db_id: 0,
        table_id: request.table_id.unwrap_or(0),
        tablet_id,
        tablet_root_path: tablet_root_path.to_string(),
        tablet_schema: tablet_schema.clone(),
        s3_config,
        partial_update: Default::default(),
    };
    register_tablet_runtime(&runtime_ctx)?;

    let standalone_v1_path = standalone_meta_file_path(tablet_root_path, tablet_id, 1)?;
    if read_bytes_if_exists(&standalone_v1_path)?.is_some() {
        return Ok(());
    }

    let latest_version = match load_latest_tablet_metadata(tablet_root_path, tablet_id) {
        Ok((version, _)) => version,
        Err(err) if is_missing_tablet_page_in_bundle_error(&err) => 0,
        Err(err) => return Err(err),
    };
    if latest_version > 1 {
        return Ok(());
    }

    let persistent_index_type = match request.persistent_index_type {
        Some(v) => Some(map_create_tablet_persistent_index_type(v)? as i32),
        None => None,
    };
    let compaction_strategy = request
        .compaction_strategy
        .map(map_create_tablet_compaction_strategy)
        .transpose()?
        .or(Some(CompactionStrategyPb::Default as i32));
    let flat_json_config = request.flat_json_config.as_ref().map(|cfg| FlatJsonConfigPb {
        flat_json_enable: cfg.flat_json_enable,
        flat_json_null_factor: cfg.flat_json_null_factor.map(|v| v.0),
        flat_json_sparsity_factor: cfg.flat_json_sparsity_factor.map(|v| v.0),
        flat_json_max_column_max: cfg.flat_json_column_max,
    });

    let mut tablet_meta = empty_tablet_metadata(tablet_id);
    tablet_meta.version = Some(1);
    tablet_meta.enable_persistent_index = request.enable_persistent_index;
    tablet_meta.persistent_index_type = persistent_index_type;
    tablet_meta.gtid = Some(request.gtid.unwrap_or(0));
    tablet_meta.compaction_strategy = compaction_strategy;
    tablet_meta.flat_json_config = flat_json_config;
    seed_tablet_metadata_schema(&mut tablet_meta, &tablet_schema);
    if request.enable_tablet_creation_optimization.unwrap_or(false) {
        let initial_path = initial_meta_file_path(tablet_root_path)?;
        if read_bytes_if_exists(&initial_path)?.is_some() {
            write_standalone_meta_file(tablet_root_path, tablet_id, 1, &tablet_meta)
        } else {
            write_initial_meta_file(tablet_root_path, &tablet_meta)
        }
    } else {
        write_standalone_meta_file(tablet_root_path, tablet_id, 1, &tablet_meta)
    }
}

pub(crate) fn create_lake_tablet_from_req(
    request: &crate::agent_service::TCreateTabletReq,
    tablet_root_path: &str,
    s3_config: Option<S3StoreConfig>,
) -> Result<(), String> {
    create_lake_tablet_from_req_with_schema_patch(request, tablet_root_path, s3_config, |_| Ok(()))
}
```

- [ ] **Step 2: Add managed physical column metadata type**

In `ddl.rs`, add:

```rust
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ManagedPhysicalColumn {
    pub(crate) column: TableColumnDef,
    pub(crate) visible: bool,
    pub(crate) is_key: bool,
}

pub(crate) fn managed_physical_column(
    name: String,
    data_type: SqlType,
    nullable: bool,
    visible: bool,
    is_key: bool,
) -> ManagedPhysicalColumn {
    ManagedPhysicalColumn {
        column: TableColumnDef {
            name,
            data_type,
            nullable,
            aggregation: None,
        },
        visible,
        is_key,
    }
}
```

Add helpers:

```rust
pub(crate) fn table_columns_from_physical_columns(
    columns: &[ManagedPhysicalColumn],
) -> Vec<TableColumnDef> {
    columns.iter().map(|column| column.column.clone()).collect()
}

pub(crate) fn stored_columns_from_physical_columns(
    schema_id: i64,
    key_desc: &TableKeyDesc,
    columns: &[ManagedPhysicalColumn],
) -> Vec<StoredManagedColumn> {
    columns
        .iter()
        .enumerate()
        .map(|(ordinal, column)| StoredManagedColumn {
            schema_id,
            ordinal: ordinal as i64,
            column_name: normalize_identifier(&column.column.name)
                .unwrap_or_else(|_| column.column.name.to_ascii_lowercase()),
            logical_type: logical_type_name(&column.column.data_type),
            nullable: column.column.nullable,
            visible: column.visible,
            is_key: column.is_key
                || key_desc.columns.iter().any(|key| {
                    normalize_identifier(key)
                        .ok()
                        .as_deref()
                        == normalize_identifier(&column.column.name).ok().as_deref()
                }),
        })
        .collect()
}

pub(crate) fn patch_tablet_schema_column_flags(
    schema: &mut crate::service::grpc_client::proto::starrocks::TabletSchemaPb,
    columns: &[ManagedPhysicalColumn],
) -> Result<(), String> {
    if schema.column.len() != columns.len() {
        return Err(format!(
            "managed schema column flag count mismatch: schema_columns={} physical_columns={}",
            schema.column.len(),
            columns.len()
        ));
    }
    for (schema_column, physical) in schema.column.iter_mut().zip(columns) {
        schema_column.visible = Some(physical.visible);
        schema_column.is_key = Some(physical.is_key);
    }
    Ok(())
}
```

- [ ] **Step 3: Route existing normal table creation through physical columns**

In `create_managed_table`, build:

```rust
let physical_columns = columns
    .iter()
    .map(|column| {
        let normalized = normalize_identifier(&column.name)
            .unwrap_or_else(|_| column.name.to_ascii_lowercase());
        let is_key = defaults.key_desc.columns.iter().any(|key| {
            normalize_identifier(key).ok().as_deref() == Some(normalized.as_str())
        });
        ManagedPhysicalColumn {
            column: column.clone(),
            visible: true,
            is_key,
        }
    })
    .collect::<Vec<_>>();
let table_columns = table_columns_from_physical_columns(&physical_columns);
let request_schema = build_tablet_schema(&table_columns, &defaults.key_desc, schema_id)?;
```

When creating tablets, replace `create_lake_tablet_from_req` with:

```rust
create_lake_tablet_from_req_with_schema_patch(
    &request,
    &tablet_root_path,
    Some(managed_config.s3.clone()),
    |schema| patch_tablet_schema_column_flags(schema, &physical_columns),
)?;
```

Persist columns with:

```rust
snapshot.columns.extend(stored_columns_from_physical_columns(
    schema_id,
    &defaults.key_desc,
    &physical_columns,
));
```

- [ ] **Step 4: Run managed DDL tests**

Run:

```bash
cargo test --lib connector::starrocks::managed::ddl -- --nocapture
```

Expected: existing managed DDL behavior passes with all columns visible.

- [ ] **Step 5: Commit**

```bash
git add src/connector/starrocks/lake/schema.rs src/connector/starrocks/managed/ddl.rs src/connector/starrocks/managed/mv_ddl.rs
git commit -m "feat: support hidden managed tablet columns"
```

## Task 4: Classify Aggregate MV Shapes

**Files:**
- Modify: `src/connector/starrocks/managed/mv_shape.rs`

- [ ] **Step 1: Replace the shape struct with shape variants**

At the top of `mv_shape.rs`, replace `IncrementalMvShape` with:

```rust
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum IncrementalMvShape {
    ProjectionFilter(ProjectionFilterMvShape),
    Aggregate(AggregateMvShape),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ProjectionFilterMvShape {
    pub(crate) base_table: sqlparser::ast::ObjectName,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct AggregateMvShape {
    pub(crate) base_table: sqlparser::ast::ObjectName,
    pub(crate) group_keys: Vec<GroupKeyShape>,
    pub(crate) aggregates: Vec<AggregateCallShape>,
    pub(crate) visible_outputs: Vec<VisibleAggregateOutput>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct GroupKeyShape {
    pub(crate) output_name: String,
    pub(crate) expr: sqlparser::ast::Expr,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct AggregateCallShape {
    pub(crate) output_name: String,
    pub(crate) function: AggregateFunctionKind,
    pub(crate) input: AggregateInput,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum AggregateFunctionKind {
    Count,
    Sum,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum AggregateInput {
    Star,
    Expr(sqlparser::ast::Expr),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum VisibleAggregateOutput {
    GroupKey(usize),
    Aggregate(usize),
}
```

- [ ] **Step 2: Keep projection/filter behavior unchanged**

Move the existing classifier body into:

```rust
fn classify_projection_filter_mv_query(
    query: &sqlparser::ast::Query,
) -> Result<ProjectionFilterMvShape, String> {
    // existing logic, returning ProjectionFilterMvShape { base_table: name.clone() }
}
```

Then implement:

```rust
pub(crate) fn classify_incremental_mv_query(
    query: &sqlparser::ast::Query,
) -> Result<IncrementalMvShape, String> {
    match classify_aggregate_mv_query(query) {
        Ok(shape) => return Ok(IncrementalMvShape::Aggregate(shape)),
        Err(err) if is_probably_aggregate_query(query) => return Err(err),
        Err(_) => {}
    }
    classify_projection_filter_mv_query(query).map(IncrementalMvShape::ProjectionFilter)
}
```

- [ ] **Step 3: Implement aggregate shape accept/reject rules**

Add:

```rust
fn classify_aggregate_mv_query(
    query: &sqlparser::ast::Query,
) -> Result<AggregateMvShape, String> {
    reject_unsupported_query_clauses(query)?;
    let sqlparser::ast::SetExpr::Select(select) = query.body.as_ref() else {
        return Err(aggregate_mv_error());
    };
    reject_unsupported_aggregate_select_clauses(select)?;

    let [from] = select.from.as_slice() else {
        return Err(single_base_table_error());
    };
    if !from.joins.is_empty() {
        return Err(single_base_table_error());
    }
    let sqlparser::ast::TableFactor::Table { name, args, with_hints, version, with_ordinality, partitions, json_path, sample, index_hints, .. } = &from.relation else {
        return Err(single_base_table_error());
    };
    if args.is_some()
        || !with_hints.is_empty()
        || version.is_some()
        || *with_ordinality
        || !partitions.is_empty()
        || json_path.is_some()
        || sample.is_some()
        || !index_hints.is_empty()
        || !is_three_part_object_name(name)
    {
        return Err(single_base_table_error());
    }

    if let Some(selection) = &select.selection {
        reject_unsupported_expr(selection)?;
    }
    let group_exprs = group_by_exprs(&select.group_by)?;
    if group_exprs.is_empty() {
        return Err("incremental aggregate MV requires non-empty GROUP BY".to_string());
    }

    let mut group_keys = Vec::new();
    let mut aggregates = Vec::new();
    let mut visible_outputs = Vec::new();
    for item in &select.projection {
        match item {
            sqlparser::ast::SelectItem::UnnamedExpr(expr) => {
                if let Some(group_idx) = find_group_key(expr, &group_exprs) {
                    let name = output_name_for_expr(expr)?;
                    group_keys.push(GroupKeyShape { output_name: name, expr: expr.clone() });
                    visible_outputs.push(VisibleAggregateOutput::GroupKey(group_idx));
                    continue;
                }
                let aggregate = parse_supported_aggregate_call(expr, None)?;
                visible_outputs.push(VisibleAggregateOutput::Aggregate(aggregates.len()));
                aggregates.push(aggregate);
            }
            sqlparser::ast::SelectItem::ExprWithAlias { expr, alias } => {
                if let Some(group_idx) = find_group_key(expr, &group_exprs) {
                    group_keys.push(GroupKeyShape {
                        output_name: alias.value.clone(),
                        expr: expr.clone(),
                    });
                    visible_outputs.push(VisibleAggregateOutput::GroupKey(group_idx));
                    continue;
                }
                let aggregate = parse_supported_aggregate_call(expr, Some(alias.value.clone()))?;
                visible_outputs.push(VisibleAggregateOutput::Aggregate(aggregates.len()));
                aggregates.push(aggregate);
            }
            _ => return Err(aggregate_mv_error()),
        }
    }
    if group_keys.len() != group_exprs.len() {
        return Err("incremental aggregate MV projection must include every GROUP BY key".to_string());
    }
    if aggregates.is_empty() {
        return Err("incremental aggregate MV requires count or sum aggregate output".to_string());
    }
    Ok(AggregateMvShape {
        base_table: name.clone(),
        group_keys,
        aggregates,
        visible_outputs,
    })
}
```

Support helpers for `GROUP BY` expression extraction and function parsing. Accepted aggregates are only `count(*)`, `count(expr)`, and `sum(expr)` with no `DISTINCT`, `FILTER`, `ORDER BY`, `OVER`, or aggregate predicate. Reuse the existing `reject_unsupported_expr` recursion for scalar subexpressions.

- [ ] **Step 4: Add aggregate classifier tests**

Add tests:

```rust
#[test]
fn accepts_single_table_count_sum_group_by() {
    let shape = classify_sql(
        "select k1, count(*) as c, count(v2) as cv, sum(v2) as s \
         from ice.ns.orders where v2 > 0 group by k1",
    )
    .expect("query should be accepted");
    let IncrementalMvShape::Aggregate(agg) = shape else {
        panic!("expected aggregate shape: {shape:?}");
    };
    assert_eq!(agg.base_table.to_string(), "ice.ns.orders");
    assert_eq!(agg.group_keys.len(), 1);
    assert_eq!(agg.aggregates.len(), 3);
}

#[test]
fn rejects_scalar_aggregate_without_group_by() {
    assert_rejects_with(
        "select count(*) from ice.ns.orders",
        "non-empty GROUP BY",
    );
}

#[test]
fn rejects_unsupported_aggregate_functions() {
    for sql in [
        "select k1, avg(v2) from ice.ns.orders group by k1",
        "select k1, min(v2) from ice.ns.orders group by k1",
        "select k1, count(distinct v2) from ice.ns.orders group by k1",
        "select k1, sum(v2) filter (where v2 > 0) from ice.ns.orders group by k1",
    ] {
        assert_rejects_with(sql, "incremental aggregate MV");
    }
}
```

Update the existing `accepts_single_table_projection_filter` test to match:

```rust
let IncrementalMvShape::ProjectionFilter(shape) = shape else {
    panic!("expected projection/filter shape");
};
```

- [ ] **Step 5: Run classifier tests**

Run:

```bash
cargo test --lib connector::starrocks::managed::mv_shape::tests -- --nocapture
```

Expected: all classifier tests pass.

- [ ] **Step 6: Commit**

```bash
git add src/connector/starrocks/managed/mv_shape.rs
git commit -m "feat: classify aggregate incremental mv shapes"
```

## Task 5: Add Aggregate MV Physical Layout And State Merge

**Files:**
- Create: `src/connector/starrocks/managed/mv_agg_state.rs`
- Modify: `src/connector/starrocks/managed/mod.rs`

- [ ] **Step 1: Add the module export**

In `managed/mod.rs`:

```rust
pub(crate) mod mv_agg_state;
```

- [ ] **Step 2: Create layout and constants**

Create `mv_agg_state.rs`:

```rust
use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BooleanArray, Date32Array, Decimal128Array, Int8Array, Int16Array,
    Int32Array, Int64Array, StringArray, TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;

use crate::connector::starrocks::managed::ddl::{managed_physical_column, ManagedPhysicalColumn};
use crate::connector::starrocks::managed::mv_shape::{
    AggregateCallShape, AggregateFunctionKind, AggregateInput, AggregateMvShape,
};
use crate::exec::chunk::Chunk;
use crate::sql::parser::ast::SqlType;
use crate::standalone::engine::{record_batch_to_chunk, QueryResult};

pub(crate) const ROW_ID_COLUMN: &str = "__row_id__";
pub(crate) const AGG_STATE_PREFIX: &str = "__agg_state_";

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct AggregateMvLayout {
    pub(crate) row_id_column: String,
    pub(crate) visible_columns: Vec<AggregateVisibleColumn>,
    pub(crate) state_columns: Vec<AggregateStateColumn>,
    pub(crate) physical_columns: Vec<ManagedPhysicalColumn>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct AggregateVisibleColumn {
    pub(crate) name: String,
    pub(crate) data_type: SqlType,
    pub(crate) nullable: bool,
    pub(crate) source_index: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct AggregateStateColumn {
    pub(crate) name: String,
    pub(crate) data_type: SqlType,
    pub(crate) nullable: bool,
    pub(crate) visible_source_index: usize,
    pub(crate) function: AggregateFunctionKind,
}
```

- [ ] **Step 3: Build physical columns from analyzed output**

Add:

```rust
pub(crate) fn build_aggregate_mv_layout(
    shape: &AggregateMvShape,
    output_columns: &[crate::sql::analysis::OutputColumn],
) -> Result<AggregateMvLayout, String> {
    if output_columns.len() != shape.visible_outputs.len() {
        return Err(format!(
            "aggregate MV output count mismatch: analyzed={} shape={}",
            output_columns.len(),
            shape.visible_outputs.len()
        ));
    }

    let mut visible_columns = Vec::with_capacity(output_columns.len());
    for (idx, output) in output_columns.iter().enumerate() {
        visible_columns.push(AggregateVisibleColumn {
            name: output.name.clone(),
            data_type: crate::connector::starrocks::managed::mv_ddl::arrow_data_type_to_sql_type(
                &output.data_type,
            )?,
            nullable: output.nullable,
            source_index: idx,
        });
    }

    let mut physical_columns = Vec::new();
    physical_columns.push(managed_physical_column(
        ROW_ID_COLUMN.to_string(),
        SqlType::String,
        false,
        false,
        true,
    ));
    for column in &visible_columns {
        physical_columns.push(managed_physical_column(
            column.name.clone(),
            column.data_type.clone(),
            column.nullable,
            true,
            false,
        ));
    }

    let mut state_columns = Vec::new();
    for (agg_idx, aggregate) in shape.aggregates.iter().enumerate() {
        let visible_source_index = aggregate_visible_source_index(shape, agg_idx)?;
        let visible = visible_columns.get(visible_source_index).ok_or_else(|| {
            format!("aggregate visible output index out of range: {visible_source_index}")
        })?;
        let state_name = format!("{AGG_STATE_PREFIX}{}", sanitize_state_suffix(&aggregate.output_name));
        let state = AggregateStateColumn {
            name: state_name,
            data_type: visible.data_type.clone(),
            nullable: visible.nullable,
            visible_source_index,
            function: aggregate.function,
        };
        physical_columns.push(managed_physical_column(
            state.name.clone(),
            state.data_type.clone(),
            state.nullable,
            false,
            false,
        ));
        state_columns.push(state);
    }

    Ok(AggregateMvLayout {
        row_id_column: ROW_ID_COLUMN.to_string(),
        visible_columns,
        state_columns,
        physical_columns,
    })
}
```

Make `arrow_data_type_to_sql_type` in `mv_ddl.rs` `pub(crate)` so this module can use the same mapping.

- [ ] **Step 4: Materialize visible aggregate query output into physical MV chunks**

Add:

```rust
pub(crate) fn materialize_aggregate_result_chunks(
    result: QueryResult,
    layout: &AggregateMvLayout,
) -> Result<Vec<Chunk>, String> {
    result
        .chunks
        .into_iter()
        .map(|chunk| materialize_aggregate_batch(chunk.batch, layout).and_then(record_batch_to_chunk))
        .collect()
}

fn materialize_aggregate_batch(
    visible_batch: RecordBatch,
    layout: &AggregateMvLayout,
) -> Result<RecordBatch, String> {
    if visible_batch.num_columns() != layout.visible_columns.len() {
        return Err(format!(
            "aggregate MV visible batch column count mismatch: batch={} layout={}",
            visible_batch.num_columns(),
            layout.visible_columns.len()
        ));
    }

    let mut fields = Vec::new();
    let mut columns = Vec::new();
    let row_ids = build_row_id_array(&visible_batch, group_key_indexes(layout)?)?;
    fields.push(Field::new(&layout.row_id_column, DataType::Utf8, false));
    columns.push(row_ids);

    for (idx, visible) in layout.visible_columns.iter().enumerate() {
        fields.push(Field::new(
            &visible.name,
            visible_batch.schema().field(idx).data_type().clone(),
            visible.nullable,
        ));
        columns.push(visible_batch.column(idx).clone());
    }

    for state in &layout.state_columns {
        let source = visible_batch.column(state.visible_source_index).clone();
        fields.push(Field::new(
            &state.name,
            source.data_type().clone(),
            state.nullable,
        ));
        columns.push(source);
    }

    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
        .map_err(|e| format!("build aggregate MV physical batch failed: {e}"))
}
```

For v1, `group_key_indexes(layout)` returns leading visible group columns. The number of group columns is `layout.visible_columns.len() - layout.state_columns.len()`.

- [ ] **Step 5: Implement stable row-id encoding**

Add:

```rust
fn build_row_id_array(batch: &RecordBatch, group_key_indexes: Vec<usize>) -> Result<ArrayRef, String> {
    let mut values = Vec::with_capacity(batch.num_rows());
    for row_idx in 0..batch.num_rows() {
        let mut encoded = Vec::new();
        for col_idx in &group_key_indexes {
            encode_row_id_cell(batch.column(*col_idx).as_ref(), row_idx, &mut encoded)?;
        }
        values.push(hex::encode(encoded));
    }
    Ok(Arc::new(StringArray::from(values)))
}

fn encode_row_id_cell(array: &dyn Array, row_idx: usize, out: &mut Vec<u8>) -> Result<(), String> {
    out.extend_from_slice(array.data_type().to_string().as_bytes());
    out.push(0xff);
    if array.is_null(row_idx) {
        out.push(0);
        out.push(0xfe);
        return Ok(());
    }
    out.push(1);
    match array.data_type() {
        DataType::Boolean => {
            let arr = array.as_any().downcast_ref::<BooleanArray>().ok_or_else(|| "downcast BooleanArray failed".to_string())?;
            out.push(u8::from(arr.value(row_idx)));
        }
        DataType::Int8 => out.push(array.as_any().downcast_ref::<Int8Array>().ok_or_else(|| "downcast Int8Array failed".to_string())?.value(row_idx) as u8),
        DataType::Int16 => out.extend_from_slice(&array.as_any().downcast_ref::<Int16Array>().ok_or_else(|| "downcast Int16Array failed".to_string())?.value(row_idx).to_be_bytes()),
        DataType::Int32 | DataType::Date32 => out.extend_from_slice(&array.as_any().downcast_ref::<Int32Array>().ok_or_else(|| "downcast Int32Array failed".to_string())?.value(row_idx).to_be_bytes()),
        DataType::Int64 => out.extend_from_slice(&array.as_any().downcast_ref::<Int64Array>().ok_or_else(|| "downcast Int64Array failed".to_string())?.value(row_idx).to_be_bytes()),
        DataType::Timestamp(TimeUnit::Microsecond, None) => out.extend_from_slice(&array.as_any().downcast_ref::<TimestampMicrosecondArray>().ok_or_else(|| "downcast TimestampMicrosecondArray failed".to_string())?.value(row_idx).to_be_bytes()),
        DataType::Utf8 => {
            let arr = array.as_any().downcast_ref::<StringArray>().ok_or_else(|| "downcast StringArray failed".to_string())?;
            let bytes = arr.value(row_idx).as_bytes();
            out.extend_from_slice(&(bytes.len() as u64).to_be_bytes());
            out.extend_from_slice(bytes);
        }
        DataType::Decimal128(_, _) => out.extend_from_slice(&array.as_any().downcast_ref::<Decimal128Array>().ok_or_else(|| "downcast Decimal128Array failed".to_string())?.value(row_idx).to_be_bytes()),
        other => return Err(format!("unsupported aggregate MV row-id group key type: {other:?}")),
    }
    out.push(0xfe);
    Ok(())
}
```

- [ ] **Step 6: Add state merge helper**

Add:

```rust
pub(crate) fn merge_aggregate_state_batches(
    old_rows: &HashMap<String, AggregatePhysicalRow>,
    delta_chunks: &[Chunk],
    layout: &AggregateMvLayout,
) -> Result<Vec<Chunk>, String> {
    let mut merged: HashMap<String, AggregatePhysicalRow> = HashMap::new();
    for chunk in delta_chunks {
        let rows = physical_rows_from_batch(&chunk.batch, layout)?;
        for row in rows {
            match merged.get_mut(&row.row_id) {
                Some(existing) => existing.add_delta(&row, layout)?,
                None => {
                    let mut base = old_rows.get(&row.row_id).cloned().unwrap_or_else(|| row.zero_base(layout));
                    base.add_delta(&row, layout)?;
                    merged.insert(base.row_id.clone(), base);
                }
            }
        }
    }
    rows_to_chunks(merged.into_values().collect(), layout)
}
```

Implement `AggregatePhysicalRow` using `arrow::array::ArrayRef` row slices for visible columns and scalar enum values for state columns. For `count` and integer `sum`, use checked `i64` addition and return:

```rust
Err("aggregate MV state merge overflow for column `<state_name>`".to_string())
```

For decimal sum, use checked `i128` addition and preserve precision/scale from the Arrow type.

- [ ] **Step 7: Add unit tests**

Add tests in `mv_agg_state.rs`:

```rust
#[test]
fn materialize_physical_chunks_adds_row_id_and_state_columns() {
    // Build RecordBatch(k1 INT, c BIGINT, s BIGINT), materialize it, and assert
    // physical names are __row_id__, k1, c, s, __agg_state_c, __agg_state_s.
}

#[test]
fn merge_count_sum_state_adds_delta_to_old_state() {
    // old: k1=1 c=2 s=30; delta: k1=1 c=3 s=70; result: c=5 s=100.
}

#[test]
fn merge_rejects_duplicate_old_row_id() {
    // Build old active rows with duplicate __row_id__ and assert the loader returns state corruption.
}
```

Use concrete Arrow arrays in those tests:

```rust
let batch = RecordBatch::try_new(
    Arc::new(Schema::new(vec![
        Field::new("k1", DataType::Int32, false),
        Field::new("c", DataType::Int64, false),
        Field::new("s", DataType::Int64, true),
    ])),
    vec![
        Arc::new(Int32Array::from(vec![1])) as ArrayRef,
        Arc::new(Int64Array::from(vec![2])) as ArrayRef,
        Arc::new(Int64Array::from(vec![Some(30)])) as ArrayRef,
    ],
)
.expect("batch");
```

Do not use external services in these tests.

- [ ] **Step 8: Run state tests**

Run:

```bash
cargo test --lib connector::starrocks::managed::mv_agg_state::tests -- --nocapture
```

Expected: all pass.

- [ ] **Step 9: Commit**

```bash
git add src/connector/starrocks/managed/mod.rs src/connector/starrocks/managed/mv_agg_state.rs src/connector/starrocks/managed/mv_ddl.rs
git commit -m "feat: add aggregate mv state layout"
```

## Task 6: Create Aggregate MV As Primary-Key Physical Table

**Files:**
- Modify: `src/connector/starrocks/managed/mv_ddl.rs`
- Modify: `src/connector/starrocks/managed/ddl.rs`

- [ ] **Step 1: Branch `create_mv` by classified shape**

In `create_mv`, keep `analysis` and `base_refs` extraction, then classify once:

```rust
let mv_shape = super::mv_shape::classify_incremental_mv_query(&stmt.select_query)?;
let (physical_columns, key_desc) = match &mv_shape {
    super::mv_shape::IncrementalMvShape::ProjectionFilter(_) => {
        let table_columns = analysis
            .output_columns
            .iter()
            .map(output_column_to_table_column)
            .collect::<Result<Vec<_>, _>>()?;
        let key_desc = TableKeyDesc {
            kind: TableKeyKind::Duplicate,
            columns: distribution.hash_columns.clone(),
        };
        let physical_columns = table_columns
            .into_iter()
            .map(|column| {
                let normalized = normalize_identifier(&column.name)
                    .unwrap_or_else(|_| column.name.to_ascii_lowercase());
                let is_key = key_desc.columns.iter().any(|key| {
                    normalize_identifier(key).ok().as_deref() == Some(normalized.as_str())
                });
                crate::connector::starrocks::managed::ddl::ManagedPhysicalColumn {
                    column,
                    visible: true,
                    is_key,
                }
            })
            .collect::<Vec<_>>();
        (physical_columns, key_desc)
    }
    super::mv_shape::IncrementalMvShape::Aggregate(shape) => {
        let layout = super::mv_agg_state::build_aggregate_mv_layout(shape, &analysis.output_columns)?;
        let key_desc = TableKeyDesc {
            kind: TableKeyKind::Primary,
            columns: vec![super::mv_agg_state::ROW_ID_COLUMN.to_string()],
        };
        (layout.physical_columns, key_desc)
    }
};
```

For aggregate MVs, validate `DISTRIBUTED BY HASH(...)` columns are group-key output columns:

```rust
validate_aggregate_distribution_columns(distribution, shape)?;
```

This preserves the user syntax while internally routing by `__row_id__`.

- [ ] **Step 2: Build and patch tablet schema from physical columns**

Replace the old `table_columns` usage:

```rust
let table_columns =
    crate::connector::starrocks::managed::ddl::table_columns_from_physical_columns(
        &physical_columns,
    );
let request_schema = build_tablet_schema(&table_columns, &key_desc, schema_id)?;
```

When creating tablets, use:

```rust
crate::connector::starrocks::lake::schema::create_lake_tablet_from_req_with_schema_patch(
    &request,
    &tablet_root_path,
    Some(managed_config.s3.clone()),
    |schema| {
        crate::connector::starrocks::managed::ddl::patch_tablet_schema_column_flags(
            schema,
            &physical_columns,
        )
    },
)?;
```

Persist columns with:

```rust
snapshot.columns.extend(
    crate::connector::starrocks::managed::ddl::stored_columns_from_physical_columns(
        schema_id,
        &key_desc,
        &physical_columns,
    ),
);
```

- [ ] **Step 3: Add DDL tests**

Add a unit test in `mv_ddl.rs`:

```rust
#[test]
fn aggregate_mv_physical_schema_has_hidden_row_id_and_state_columns() {
    // Create an aggregate MV over a test Iceberg table using the existing MV DDL test harness.
    // Load the managed runtime and assert:
    // - keys_type is PRIMARY_KEYS
    // - first tablet schema column is __row_id__, is_key=true, visible=false
    // - __agg_state_c exists with visible=false
    // - public catalog table columns do not include __row_id__ or __agg_state_c
}
```

Use the local `create_mv` test setup in `mv_ddl.rs` and keep the SQL exactly:

```sql
CREATE MATERIALIZED VIEW analytics.orders_mv
DISTRIBUTED BY HASH(k1) BUCKETS 2
AS SELECT k1, count(*) AS c, sum(v2) AS s
FROM ice.ns.orders
GROUP BY k1
```

- [ ] **Step 4: Run MV DDL tests**

Run:

```bash
cargo test --lib connector::starrocks::managed::mv_ddl -- --nocapture
```

Expected: aggregate physical schema test passes and existing projection/filter MV tests still pass.

- [ ] **Step 5: Commit**

```bash
git add src/connector/starrocks/managed/mv_ddl.rs src/connector/starrocks/managed/ddl.rs
git commit -m "feat: create aggregate mv primary-key layout"
```

## Task 7: Full Refresh Writes Aggregate Physical State

**Files:**
- Modify: `src/connector/starrocks/managed/mv_refresh.rs`
- Modify: `src/connector/starrocks/managed/txn.rs`

- [ ] **Step 1: Add aggregate full-refresh branch**

In `refresh_mv`, after `mv_shape` classification:

```rust
match (&mv_shape, choose_refresh_strategy(previous_snapshot_id, current_snapshot_id)?) {
    (super::mv_shape::IncrementalMvShape::ProjectionFilter(_), MvRefreshStrategy::Full) => {
        refresh_mv_full_with_executor(state, &db_name, &mv_name, run_mv_select_and_chunks)
    }
    (super::mv_shape::IncrementalMvShape::Aggregate(shape), MvRefreshStrategy::Full) => {
        refresh_aggregate_mv_full(state, &db_name, &mv_name, shape)
    }
    (_, MvRefreshStrategy::NoOp { current_snapshot_id }) => {
        let snapshots = single_snapshot_map(base_ref, current_snapshot_id);
        metadata_store.update_mv_refresh_metadata(UpdateMvRefreshMetadataRequest {
            table_id: runtime.table.table_id,
            last_refresh_rows: mv_row.last_refresh_rows.unwrap_or(0),
            snapshots,
        })?;
        refresh_managed_catalog(state)?;
        Ok(StatementResult::Ok)
    }
    (super::mv_shape::IncrementalMvShape::ProjectionFilter(_), MvRefreshStrategy::Incremental { previous_snapshot_id, current_snapshot_id }) => {
        let delta = plan_append_delta(&loaded.table, previous_snapshot_id)?;
        if delta.current_snapshot_id != current_snapshot_id {
            return Err(format!(
                "iceberg append delta current snapshot mismatch: expected {current_snapshot_id}, got {}",
                delta.current_snapshot_id
            ));
        }

        let result = execute_query_for_mv_incremental_refresh(
            state,
            &db_name,
            &mv_row.select_sql,
            base_ref,
            delta.added_files,
        )?;
        let chunks = query_result_to_chunks(result)?;
        let plan = load_insert_plan(
            state,
            &crate::standalone::engine::ResolvedLocalTableName {
                database: db_name.clone(),
                table: mv_name.clone(),
            },
            PartitionTarget::Active,
        )?;
        let previous_rows = mv_row.last_refresh_rows.unwrap_or(0);
        let snapshots = single_snapshot_map(base_ref, current_snapshot_id);
        write_chunks_into_managed_partition_for_mv_refresh(
            state,
            plan,
            &chunks,
            MvRefreshWriteMetadata {
                table_id: runtime.table.table_id,
                previous_refresh_rows: previous_rows,
                snapshots,
            },
        )?;
        refresh_managed_catalog(state)?;
        Ok(StatementResult::Ok)
    }
    (super::mv_shape::IncrementalMvShape::Aggregate(_), MvRefreshStrategy::Incremental { .. }) => {
        Err("aggregate MV incremental refresh requires aggregate upsert merge path from Task 8".to_string())
    }
}
```

- [ ] **Step 2: Implement aggregate full refresh**

Add:

```rust
fn refresh_aggregate_mv_full(
    state: &Arc<StandaloneState>,
    database: &str,
    mv_name: &str,
    shape: &super::mv_shape::AggregateMvShape,
) -> Result<StatementResult, String> {
    refresh_mv_full_with_executor(state, database, mv_name, |ctx| {
        let result = execute_query_for_mv_refresh(&ctx.state, &ctx.database, &ctx.select_sql)?;
        let output_columns = result.columns.iter().map(query_result_column_to_output_column).collect::<Result<Vec<_>, String>>()?;
        let layout = super::mv_agg_state::build_aggregate_mv_layout(shape, &output_columns)?;
        super::mv_agg_state::materialize_aggregate_result_chunks(result, &layout)
    })
}
```

Add:

```rust
fn query_result_column_to_output_column(
    column: &crate::standalone::engine::QueryResultColumn,
) -> Result<crate::sql::analysis::OutputColumn, String> {
    Ok(crate::sql::analysis::OutputColumn {
        name: column.name.clone(),
        data_type: column.data_type.clone(),
        nullable: column.nullable,
    })
}
```

- [ ] **Step 3: Load physical insert plans for full refresh**

In `refresh_mv_full_with_executor`, replace `load_insert_plan` with:

```rust
let plan = match load_physical_insert_plan(
    state,
    &crate::standalone::engine::ResolvedLocalTableName {
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
        cleanup_staged_partition(state, metadata_store, runtime.table.table_id, &staged, true)?;
        return Err(format!("mv refresh plan load failed: {err}"));
    }
};
```

Projection/filter MVs have all columns visible, so physical mode is equivalent for them.

- [ ] **Step 4: Add full-refresh test**

Add a unit test in `mv_refresh.rs`:

```rust
#[test]
fn aggregate_full_refresh_executor_writes_physical_columns() {
    // Use refresh_mv_full_with_executor with a fake executor returning visible
    // aggregate rows k1,c,s. Load the active tablet metadata after activation
    // and assert the tablet schema still contains hidden __row_id__ and state columns.
}
```

- [ ] **Step 5: Run full refresh tests**

Run:

```bash
cargo test --lib connector::starrocks::managed::mv_refresh::tests::aggregate_full_refresh_executor_writes_physical_columns -- --nocapture
cargo test --lib connector::starrocks::managed::mv_refresh::tests::refresh_mv_full_cleans_staged_partition_when_executor_fails -- --nocapture
```

Expected: both pass.

- [ ] **Step 6: Commit**

```bash
git add src/connector/starrocks/managed/mv_refresh.rs src/connector/starrocks/managed/txn.rs
git commit -m "feat: write aggregate mv full refresh state"
```

## Task 8: Incremental Refresh Merges Delta State And Upserts

**Files:**
- Modify: `src/connector/starrocks/managed/txn.rs`
- Modify: `src/connector/starrocks/managed/mv_refresh.rs`
- Modify: `src/connector/starrocks/managed/mv_agg_state.rs`

- [ ] **Step 1: Add active MV physical-row reader**

In `txn.rs`, add:

```rust
pub(crate) fn read_active_managed_physical_chunks(
    state: &Arc<StandaloneState>,
    plan: &ManagedInsertPlan,
) -> Result<Vec<Chunk>, String> {
    let managed_config = state
        .managed_lake_config
        .as_ref()
        .ok_or_else(|| "standalone managed lake config is missing during managed physical read".to_string())?
        .clone();
    let output_schema = Arc::new(arrow::datatypes::Schema::new(
        plan.columns
            .iter()
            .map(|column| {
                arrow::datatypes::Field::new(
                    &column.name,
                    crate::standalone::engine::parquet::normalize_map_entries_nullability(&column.data_type),
                    column.nullable,
                )
            })
            .collect::<Vec<_>>(),
    ));
    let mut chunks = Vec::new();
    for tablet in &plan.tablets {
        let object_store_profile = match s3_config_for_tablet_path(&tablet.tablet_root_path, &managed_config.s3)? {
            Some(s3) => Some(crate::connector::starrocks::ObjectStoreProfile::from_s3_store_config(&s3)?),
            None => None,
        };
        let snapshot = crate::formats::starrocks::metadata::load_tablet_snapshot(
            tablet.tablet_id,
            plan.base_version,
            &tablet.tablet_root_path,
            object_store_profile.as_ref(),
        )?;
        let segment_footers = crate::formats::starrocks::metadata::load_bundle_segment_footers(
            &snapshot,
            &tablet.tablet_root_path,
            object_store_profile.as_ref(),
        )?;
        let read_plan = crate::formats::starrocks::plan::build_native_read_plan(
            &snapshot,
            &segment_footers,
            &output_schema,
            None,
        )?;
        let batch = crate::formats::starrocks::reader::build_native_record_batch(
            &read_plan,
            &segment_footers,
            &tablet.tablet_root_path,
            object_store_profile.as_ref(),
            &output_schema,
            &[],
        )?;
        if batch.num_rows() > 0 {
            chunks.push(record_batch_to_chunk(batch)?);
        }
    }
    Ok(chunks)
}
```

Import `Arc` and the native read helpers. The native primary-key reader already applies delete vectors, so this reads only visible active MV rows.

- [ ] **Step 2: Add aggregate upsert writer**

In `txn.rs`, add:

```rust
pub(crate) fn write_chunks_into_managed_partition_for_aggregate_mv_upsert(
    state: &Arc<StandaloneState>,
    plan: ManagedInsertPlan,
    delta_chunks: &[Chunk],
    layout: &super::mv_agg_state::AggregateMvLayout,
    metadata: MvRefreshWriteMetadata,
) -> Result<i64, String> {
    if plan.tablet_schema.keys_type != Some(crate::service::grpc_client::proto::starrocks::KeysType::PrimaryKeys as i32) {
        return Err("aggregate MV internal upsert requires PRIMARY_KEYS managed table".to_string());
    }
    let old_chunks = read_active_managed_physical_chunks(state, &plan)?;
    let old_rows = super::mv_agg_state::build_old_state_map(&old_chunks, layout)?;
    let merged_chunks = super::mv_agg_state::merge_aggregate_state_batches(&old_rows, delta_chunks, layout)?;
    write_chunks_into_managed_partition_for_mv_refresh(state, plan, &merged_chunks, metadata)
}
```

No `__op` column is needed for upsert-only writes: the existing lake writer treats a full-schema batch into a `PRIMARY_KEYS` tablet as upsert, and `apply_primary_key_write_log_to_metadata` replaces older rows with delete vectors at publish time.

- [ ] **Step 3: Route aggregate incremental refresh**

In `mv_refresh.rs`, add a branch:

```rust
(super::mv_shape::IncrementalMvShape::Aggregate(shape), MvRefreshStrategy::Incremental { previous_snapshot_id, current_snapshot_id }) => {
    let delta = plan_append_delta(&loaded.table, previous_snapshot_id)?;
    if delta.current_snapshot_id != current_snapshot_id {
        return Err(format!(
            "iceberg append delta current snapshot mismatch: expected {current_snapshot_id}, got {}",
            delta.current_snapshot_id
        ));
    }
    let result = execute_query_for_mv_incremental_refresh(
        state,
        &db_name,
        &mv_row.select_sql,
        base_ref,
        delta.added_files,
    )?;
    let output_columns = result.columns.iter().map(query_result_column_to_output_column).collect::<Result<Vec<_>, String>>()?;
    let layout = super::mv_agg_state::build_aggregate_mv_layout(shape, &output_columns)?;
    let delta_chunks = super::mv_agg_state::materialize_aggregate_result_chunks(result, &layout)?;
    let plan = load_physical_insert_plan(
        state,
        &crate::standalone::engine::ResolvedLocalTableName {
            database: db_name.clone(),
            table: mv_name.clone(),
        },
        PartitionTarget::Active,
    )?;
    let previous_rows = mv_row.last_refresh_rows.unwrap_or(0);
    let snapshots = single_snapshot_map(base_ref, current_snapshot_id);
    write_chunks_into_managed_partition_for_aggregate_mv_upsert(
        state,
        plan,
        &delta_chunks,
        &layout,
        MvRefreshWriteMetadata {
            table_id: runtime.table.table_id,
            previous_refresh_rows: previous_rows,
            snapshots,
        },
    )?;
    refresh_managed_catalog(state)?;
    Ok(StatementResult::Ok)
}
```

Projection/filter incremental refresh keeps the current append path.

- [ ] **Step 4: Add duplicate old-row guard**

In `mv_agg_state.rs`, implement:

```rust
pub(crate) fn build_old_state_map(
    chunks: &[Chunk],
    layout: &AggregateMvLayout,
) -> Result<HashMap<String, AggregatePhysicalRow>, String> {
    let mut out = HashMap::new();
    for chunk in chunks {
        for row in physical_rows_from_batch(&chunk.batch, layout)? {
            if out.insert(row.row_id.clone(), row).is_some() {
                return Err(format!(
                    "aggregate MV state corruption: duplicate {} in active MV",
                    layout.row_id_column
                ));
            }
        }
    }
    Ok(out)
}
```

- [ ] **Step 5: Add transaction-level upsert test**

Add a unit test in `txn.rs`:

```rust
#[test]
fn aggregate_mv_upsert_replaces_existing_primary_key_row() {
    // Create a primary-key managed MV table with physical columns:
    // __row_id__, k1, c, s, __agg_state_c, __agg_state_s.
    // Write full old row: k1=1 c=2 s=30.
    // Call write_chunks_into_managed_partition_for_aggregate_mv_upsert with delta k1=1 c=3 s=70.
    // Read active physical chunks and assert exactly one row for __row_id__ with c=5 and s=100.
}
```

Use the managed-lake tempdir test harness already present in `txn.rs`. The assertion must read through `read_active_managed_physical_chunks` so delete vectors are applied.

- [ ] **Step 6: Run aggregate upsert tests**

Run:

```bash
cargo test --lib connector::starrocks::managed::txn::tests::aggregate_mv_upsert_replaces_existing_primary_key_row -- --nocapture
cargo test --lib connector::starrocks::managed::mv_agg_state::tests -- --nocapture
```

Expected: all pass.

- [ ] **Step 7: Commit**

```bash
git add src/connector/starrocks/managed/txn.rs src/connector/starrocks/managed/mv_refresh.rs src/connector/starrocks/managed/mv_agg_state.rs
git commit -m "feat: upsert aggregate mv incremental state"
```

## Task 9: SQL Regression For Aggregate MV Incremental Refresh

**Files:**
- Create: `sql-tests/write-path/sql/managed_lake_mv_aggregate_ivm.sql`
- Create: `sql-tests/write-path/result/managed_lake_mv_aggregate_ivm.result`

- [ ] **Step 1: Add SQL case**

Create `sql-tests/write-path/sql/managed_lake_mv_aggregate_ivm.sql`:

```sql
-- @order_sensitive=true
-- @tags=write_path,managed_lake,mv,iceberg,aggregate
-- Test Objective:
-- 1. Validate aggregate MV full refresh writes visible count/sum results.
-- 2. Validate append-only incremental refresh merges delta state instead of appending duplicate groups.
-- 3. Validate hidden MV state columns are not query-visible.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG mv_agg_ice_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "hadoop",
  "iceberg.catalog.warehouse" = "${managed_lake_warehouse}/iceberg_mv_agg_${uuid0}",
  "aws.s3.endpoint" = "${oss_endpoint}",
  "aws.s3.access_key" = "${oss_ak}",
  "aws.s3.secret_key" = "${oss_sk}",
  "aws.s3.enable_path_style_access" = "true"
);
CREATE DATABASE mv_agg_ice_${uuid0}.ns_${uuid0};
CREATE TABLE mv_agg_ice_${uuid0}.ns_${uuid0}.orders (
  k1 INT,
  v2 BIGINT
);
INSERT INTO mv_agg_ice_${uuid0}.ns_${uuid0}.orders VALUES
  (1, 10),
  (1, 20),
  (2, 40),
  (3, NULL);
CREATE MATERIALIZED VIEW ${case_db}.orders_agg_mv
DISTRIBUTED BY HASH(k1) BUCKETS 2
AS SELECT k1, count(*) AS c_all, count(v2) AS c_v2, sum(v2) AS s_v2
FROM mv_agg_ice_${uuid0}.ns_${uuid0}.orders
GROUP BY k1;

-- query 2
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW ${case_db}.orders_agg_mv;

-- query 3
SELECT k1, c_all, c_v2, s_v2
FROM ${case_db}.orders_agg_mv
ORDER BY k1;

-- query 4
-- @skip_result_check=true
INSERT INTO mv_agg_ice_${uuid0}.ns_${uuid0}.orders VALUES
  (1, 70),
  (2, 60),
  (4, 5);

-- query 5
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW ${case_db}.orders_agg_mv;

-- query 6
SELECT k1, c_all, c_v2, s_v2
FROM ${case_db}.orders_agg_mv
ORDER BY k1;

-- query 7
-- @error_contains=Column '__row_id__' cannot be resolved
SELECT __row_id__ FROM ${case_db}.orders_agg_mv;

-- query 8
-- @skip_result_check=true
DROP MATERIALIZED VIEW ${case_db}.orders_agg_mv;
DROP TABLE mv_agg_ice_${uuid0}.ns_${uuid0}.orders FORCE;
DROP DATABASE mv_agg_ice_${uuid0}.ns_${uuid0};
DROP CATALOG mv_agg_ice_${uuid0};
```

- [ ] **Step 2: Add expected result file**

Create `sql-tests/write-path/result/managed_lake_mv_aggregate_ivm.result`:

```text
-- query 3
1	2	2	30
2	1	1	40
3	1	0	NULL
-- query 6
1	3	3	100
2	2	2	100
3	1	0	NULL
4	1	1	5
```

- [ ] **Step 3: Run unit and SQL tests**

Run unit tests:

```bash
cargo test --lib connector::starrocks::managed::mv_shape::tests -- --nocapture
cargo test --lib connector::starrocks::managed::mv_agg_state::tests -- --nocapture
cargo test --lib connector::starrocks::managed::txn::tests::aggregate_mv_upsert_replaces_existing_primary_key_row -- --nocapture
```

Start standalone server on the configured SQL-test port in a separate terminal:

```bash
NO_PROXY=127.0.0.1,localhost cargo run -- standalone-server --port 9030
```

Run the SQL case:

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --suite write-path --only managed_lake_mv_aggregate_ivm --mode verify --query-timeout 60
```

Expected: unit tests pass and the SQL case passes.

- [ ] **Step 4: Run projection/filter regression case**

Run:

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --suite write-path --only managed_lake_mv_basic --mode verify --query-timeout 60
```

Expected: existing projection/filter MV behavior still passes.

- [ ] **Step 5: Commit**

```bash
git add sql-tests/write-path/sql/managed_lake_mv_aggregate_ivm.sql sql-tests/write-path/result/managed_lake_mv_aggregate_ivm.result
git commit -m "test: cover aggregate mv incremental refresh"
```

## Task 10: Final Verification And Cleanup

**Files:**
- Verify and format all files changed by Tasks 1-9.

- [ ] **Step 1: Run formatter**

Run:

```bash
cargo fmt
```

Expected: completes with no output.

- [ ] **Step 2: Run focused build**

Run:

```bash
cargo build
```

Expected: build succeeds.

- [ ] **Step 3: Run focused unit test set**

Run:

```bash
cargo test --lib connector::starrocks::managed::store::tests::init_schema_v5_creates_table_column_flags -- --nocapture
cargo test --lib connector::starrocks::managed::catalog::tests::register_managed_table_hides_invisible_columns -- --nocapture
cargo test --lib connector::starrocks::managed::mv_shape::tests -- --nocapture
cargo test --lib connector::starrocks::managed::mv_agg_state::tests -- --nocapture
cargo test --lib connector::starrocks::managed::txn::tests::aggregate_mv_upsert_replaces_existing_primary_key_row -- --nocapture
```

Expected: all pass.

- [ ] **Step 4: Run SQL-test regression pair**

With standalone server running:

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --suite write-path --only managed_lake_mv_basic,managed_lake_mv_aggregate_ivm --mode verify --query-timeout 60
```

Expected: both cases pass.

- [ ] **Step 5: Inspect git diff**

Run:

```bash
git status --short
git diff --check
```

Expected: `git status --short` lists only the implementation, tests, and this plan's intentional local documentation files; `git diff --check` prints nothing.

- [ ] **Step 6: Final commit**

When implementing in one batch, commit once:

```bash
git add src/connector/starrocks/lake/schema.rs \
        src/connector/starrocks/managed/store.rs \
        src/connector/starrocks/managed/catalog.rs \
        src/connector/starrocks/managed/ddl.rs \
        src/connector/starrocks/managed/txn.rs \
        src/connector/starrocks/managed/mv_shape.rs \
        src/connector/starrocks/managed/mv_agg_state.rs \
        src/connector/starrocks/managed/mv_ddl.rs \
        src/connector/starrocks/managed/mv_refresh.rs \
        src/connector/starrocks/managed/mod.rs \
        sql-tests/write-path/sql/managed_lake_mv_aggregate_ivm.sql \
        sql-tests/write-path/result/managed_lake_mv_aggregate_ivm.result
git commit -m "feat: support aggregate mv incremental refresh"
```

Expected: one final commit containing the aggregate MV implementation when the implementation was done in one batch.
