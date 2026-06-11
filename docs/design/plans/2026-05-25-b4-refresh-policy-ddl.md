# B4 Refresh Policy DDL Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 让用户能通过 SQL 声明 materialized view refresh policy，并把 CREATE / ALTER 的声明写入已有 MV metadata；不启动后台 scheduler，不改变手动 `REFRESH MATERIALIZED VIEW` 执行语义。

**Architecture:** Parser AST 增加 refresh policy 和 ALTER MATERIALIZED VIEW action。CREATE MV 在现有 managed-lake / iceberg-backed 创建事务中写入 B4-1 metadata。ALTER MV 只更新 repository metadata 字段，保留 refresh execution path 与 coordinator 行为为空。

**Tech Stack:** Rust, `sqlparser`, NovaRocks custom parser, `MvMetaRepository`, standalone MySQL server tests, `cargo test`.

---

## Scope

Included:

- `CREATE MATERIALIZED VIEW` 支持：
  - `REFRESH DEFERRED MANUAL`
  - `REFRESH MANUAL`
  - `REFRESH ASYNC ON CHANGE`
  - `REFRESH ASYNC EVERY INTERVAL <n> <unit>`
- `ALTER MATERIALIZED VIEW` 支持：
  - `SET REFRESH MANUAL`
  - `SET REFRESH ASYNC ON CHANGE`
  - `SET REFRESH ASYNC EVERY INTERVAL <n> <unit>`
  - `PAUSE REFRESH`
  - `RESUME REFRESH`
- CREATE / ALTER 写入 B4-1 的 persisted metadata。
- SHOW 可观察到 CREATE / ALTER 后的 policy 和 paused state。

Not included:

- `REFRESH IMMEDIATE`
- `REFRESH ASYNC` without trigger
- `MAX_STALENESS` grammar
- Background coordinator, queue, timer, snapshot watch, or rewrite staleness behavior

Supported interval units for this phase:

- `SECOND` / `SECONDS`
- `MINUTE` / `MINUTES`
- `HOUR` / `HOURS`
- `DAY` / `DAYS`

All interval values must be positive and fit in `i64` milliseconds.

## File Structure

- Modify `src/sql/parser/ast/mod.rs`
  - Adds `MaterializedViewRefreshPolicy`, `AlterMaterializedViewAction`, and `AlterMaterializedViewStmt`.
  - Adds `refresh_policy` to `CreateMaterializedViewStmt`.
  - Adds `Statement::AlterMaterializedView`.

- Modify `src/sql/parser/dialect/materialized_view.rs`
  - Parses CREATE refresh policy.
  - Parses ALTER refresh policy / pause / resume.
  - Keeps unsupported combinations fail-fast with explicit errors.

- Modify `src/sql/parser/mod.rs`
  - Dispatches `ALTER MATERIALIZED VIEW` before generic ALTER paths.

- Modify `src/engine/mod.rs`
  - Routes `Statement::AlterMaterializedView` to `mv_flow`.

- Modify `src/engine/mv_flow.rs`
  - Converts AST refresh policy to repository metadata.
  - Resolves managed-lake and iceberg-backed MV definitions.
  - Applies ALTER policy / pause / resume via `MvMetaRepository::update_refresh_metadata`.

- Modify `src/connector/starrocks/managed/mv_ddl.rs`
  - Persists CREATE refresh policy for managed-lake MV creation.

- Modify `src/engine/mv/iceberg_refresh.rs`
  - Persists CREATE refresh policy for iceberg-backed MV creation.

- Modify `tests/standalone_mysql_server.rs`
  - Adds end-to-end CREATE / ALTER / SHOW verification through MySQL protocol.

---

### Task 1: Parser AST and CREATE Refresh Policy Grammar

**Files:**

- Modify `src/sql/parser/ast/mod.rs`
- Modify `src/sql/parser/dialect/materialized_view.rs`

- [ ] **Step 1: Write failing CREATE parser tests**

In `src/sql/parser/dialect/materialized_view.rs`, update the test imports:

```rust
use crate::sql::parser::ast::{IcebergPartitionFieldExpr, MaterializedViewRefreshPolicy, Statement};
```

Add these tests near the existing CREATE MV refresh tests:

```rust
#[test]
fn parse_create_mv_accepts_refresh_async_on_change() {
    let stmt = parse_one(
        "CREATE MATERIALIZED VIEW mv1 \
         DISTRIBUTED BY HASH(k1) BUCKETS 4 \
         REFRESH ASYNC ON CHANGE \
         AS SELECT k1 FROM iceberg_cat.ns.orders",
    );
    let Statement::CreateMaterializedView(mv) = stmt else {
        panic!("expected CREATE MATERIALIZED VIEW");
    };
    assert_eq!(mv.refresh_policy, MaterializedViewRefreshPolicy::AsyncOnChange);
}

#[test]
fn parse_create_mv_accepts_refresh_async_every_interval() {
    let stmt = parse_one(
        "CREATE MATERIALIZED VIEW mv1 \
         DISTRIBUTED BY HASH(k1) BUCKETS 4 \
         REFRESH ASYNC EVERY INTERVAL 5 MINUTE \
         AS SELECT k1 FROM iceberg_cat.ns.orders",
    );
    let Statement::CreateMaterializedView(mv) = stmt else {
        panic!("expected CREATE MATERIALIZED VIEW");
    };
    assert_eq!(
        mv.refresh_policy,
        MaterializedViewRefreshPolicy::AsyncInterval { interval_ms: 300_000 }
    );
}

#[test]
fn parse_create_mv_rejects_refresh_async_without_trigger() {
    let err = crate::sql::parser::parse_sql(
        "CREATE MATERIALIZED VIEW mv1 \
         DISTRIBUTED BY HASH(k1) BUCKETS 1 \
         REFRESH ASYNC \
         AS SELECT k1 FROM iceberg_cat.ns.orders",
    )
    .expect_err("should reject");
    assert!(
        err.contains("REFRESH ASYNC requires ON CHANGE or EVERY INTERVAL"),
        "unexpected err: {err}"
    );
}
```

Update the existing manual tests so they assert `mv.refresh_policy == MaterializedViewRefreshPolicy::Manual` instead of `refresh_manual_explicit`.

- [ ] **Step 2: Run CREATE parser tests and verify RED**

Run:

```bash
cargo test --lib sql::parser::dialect::materialized_view::tests::parse_create_mv_accepts_refresh_async -- --nocapture
```

Expected: compile failure mentioning missing `MaterializedViewRefreshPolicy` and `CreateMaterializedViewStmt.refresh_policy`.

- [ ] **Step 3: Add AST refresh policy type**

In `src/sql/parser/ast/mod.rs`, add after `MaterializedViewDistribution`:

```rust
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum MaterializedViewRefreshPolicy {
    Manual,
    AsyncOnChange,
    AsyncInterval { interval_ms: i64 },
}

impl Default for MaterializedViewRefreshPolicy {
    fn default() -> Self {
        Self::Manual
    }
}
```

In `CreateMaterializedViewStmt`, replace:

```rust
pub refresh_manual_explicit: bool,
```

with:

```rust
pub refresh_policy: MaterializedViewRefreshPolicy,
```

- [ ] **Step 4: Parse CREATE refresh policy**

In `src/sql/parser/dialect/materialized_view.rs`, import `MaterializedViewRefreshPolicy`.

Change the optional refresh clause in `parse_create_materialized_view` to:

```rust
let refresh_policy = if parser.parse_keyword(Keyword::REFRESH) {
    parse_refresh_clause(parser)?
} else {
    MaterializedViewRefreshPolicy::Manual
};
```

Set the AST field:

```rust
refresh_policy,
```

Replace `parse_refresh_clause` with:

```rust
fn parse_refresh_clause(parser: &mut Parser<'_>) -> Result<MaterializedViewRefreshPolicy, String> {
    if parser.parse_keyword(Keyword::IMMEDIATE) {
        return Err("REFRESH IMMEDIATE is not supported yet".to_string());
    }
    let _ = parser.parse_keyword(Keyword::DEFERRED);
    if peek_word_eq(parser, 0, "ASYNC") {
        parser.next_token();
        return parse_refresh_async_tail(parser);
    }
    if !peek_word_eq(parser, 0, "MANUAL") {
        return Err("expected REFRESH [DEFERRED] MANUAL or REFRESH ASYNC ON CHANGE or REFRESH ASYNC EVERY INTERVAL <n> <unit>".to_string());
    }
    parser.next_token();
    Ok(MaterializedViewRefreshPolicy::Manual)
}

fn parse_refresh_async_tail(parser: &mut Parser<'_>) -> Result<MaterializedViewRefreshPolicy, String> {
    if parser.parse_keywords(&[Keyword::ON, Keyword::CHANGE]) {
        return Ok(MaterializedViewRefreshPolicy::AsyncOnChange);
    }
    if peek_word_eq(parser, 0, "EVERY") {
        parser.next_token();
        if !peek_word_eq(parser, 0, "INTERVAL") {
            return Err("REFRESH ASYNC EVERY requires INTERVAL <n> <unit>".to_string());
        }
        parser.next_token();
        let value = parser
            .parse_literal_uint()
            .map_err(|e| format!("parse REFRESH ASYNC interval failed: {e}"))?;
        let unit = parser.next_token();
        let unit = match &unit.token {
            Token::Word(word) => word.value.as_str(),
            other => return Err(format!("expected interval unit after REFRESH ASYNC EVERY INTERVAL, got {other:?}")),
        };
        let interval_ms = refresh_interval_ms(value, unit)?;
        return Ok(MaterializedViewRefreshPolicy::AsyncInterval { interval_ms });
    }
    Err("REFRESH ASYNC requires ON CHANGE or EVERY INTERVAL <n> <unit>".to_string())
}

fn refresh_interval_ms(value: u64, unit: &str) -> Result<i64, String> {
    if value == 0 {
        return Err("REFRESH ASYNC interval must be positive".to_string());
    }
    let multiplier = match unit.to_ascii_uppercase().as_str() {
        "SECOND" | "SECONDS" => 1_000_u64,
        "MINUTE" | "MINUTES" => 60_000_u64,
        "HOUR" | "HOURS" => 3_600_000_u64,
        "DAY" | "DAYS" => 86_400_000_u64,
        _ => {
            return Err(format!(
                "unsupported REFRESH ASYNC interval unit `{unit}`; expected SECOND, MINUTE, HOUR, or DAY"
            ));
        }
    };
    let ms = value
        .checked_mul(multiplier)
        .ok_or_else(|| "REFRESH ASYNC interval is too large".to_string())?;
    i64::try_from(ms).map_err(|_| "REFRESH ASYNC interval is too large".to_string())
}
```

- [ ] **Step 5: Run CREATE parser tests and commit**

Run:

```bash
cargo test --lib sql::parser::dialect::materialized_view::tests::parse_create_mv -- --nocapture
```

Expected: all CREATE MV parser tests pass.

Commit:

```bash
git add src/sql/parser/ast/mod.rs src/sql/parser/dialect/materialized_view.rs
git commit -m "feat: parse MV refresh policy DDL"
```

---

### Task 2: ALTER MATERIALIZED VIEW Refresh Policy Grammar and Dispatch

**Files:**

- Modify `src/sql/parser/ast/mod.rs`
- Modify `src/sql/parser/dialect/materialized_view.rs`
- Modify `src/sql/parser/mod.rs`
- Modify `src/engine/mod.rs`

- [ ] **Step 1: Write failing ALTER parser tests**

In `src/sql/parser/dialect/materialized_view.rs`, add:

```rust
#[test]
fn parse_alter_mv_set_refresh_async_interval() {
    let stmt = parse_one("ALTER MATERIALIZED VIEW analytics.mv1 SET REFRESH ASYNC EVERY INTERVAL 2 HOURS");
    let Statement::AlterMaterializedView(alter) = stmt else {
        panic!("expected ALTER MATERIALIZED VIEW");
    };
    assert_eq!(alter.name.parts, vec!["analytics", "mv1"]);
    assert_eq!(
        alter.action,
        crate::sql::parser::ast::AlterMaterializedViewAction::SetRefresh(
            MaterializedViewRefreshPolicy::AsyncInterval { interval_ms: 7_200_000 }
        )
    );
}

#[test]
fn parse_alter_mv_pause_and_resume_refresh() {
    let pause = parse_one("ALTER MATERIALIZED VIEW mv1 PAUSE REFRESH");
    let Statement::AlterMaterializedView(pause) = pause else {
        panic!("expected ALTER MATERIALIZED VIEW");
    };
    assert_eq!(
        pause.action,
        crate::sql::parser::ast::AlterMaterializedViewAction::PauseRefresh
    );

    let resume = parse_one("ALTER MATERIALIZED VIEW mv1 RESUME REFRESH");
    let Statement::AlterMaterializedView(resume) = resume else {
        panic!("expected ALTER MATERIALIZED VIEW");
    };
    assert_eq!(
        resume.action,
        crate::sql::parser::ast::AlterMaterializedViewAction::ResumeRefresh
    );
}

#[test]
fn parse_alter_mv_rejects_refresh_immediate() {
    let err = crate::sql::parser::parse_sql("ALTER MATERIALIZED VIEW mv1 SET REFRESH IMMEDIATE")
        .expect_err("should reject");
    assert!(err.contains("REFRESH IMMEDIATE is not supported yet"), "err={err}");
}
```

- [ ] **Step 2: Run ALTER parser tests and verify RED**

Run:

```bash
cargo test --lib sql::parser::dialect::materialized_view::tests::parse_alter_mv -- --nocapture
```

Expected: compile failure mentioning missing `AlterMaterializedViewAction`, `AlterMaterializedViewStmt`, or `Statement::AlterMaterializedView`.

- [ ] **Step 3: Add ALTER AST**

In `src/sql/parser/ast/mod.rs`, add after `DropMaterializedViewStmt`:

```rust
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum AlterMaterializedViewAction {
    SetRefresh(MaterializedViewRefreshPolicy),
    PauseRefresh,
    ResumeRefresh,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct AlterMaterializedViewStmt {
    pub name: ObjectName,
    pub action: AlterMaterializedViewAction,
}
```

Add a statement variant:

```rust
AlterMaterializedView(AlterMaterializedViewStmt),
```

- [ ] **Step 4: Parse ALTER MATERIALIZED VIEW**

In `src/sql/parser/dialect/materialized_view.rs`, add imports for `AlterMaterializedViewAction` and `AlterMaterializedViewStmt`.

Add parser probe and parser:

```rust
pub(crate) fn looks_like_alter_materialized_view(parser: &Parser<'_>) -> bool {
    parser.peek_keyword(Keyword::ALTER)
        && peek_word_eq(parser, 1, "MATERIALIZED")
        && peek_word_eq(parser, 2, "VIEW")
}

pub(crate) fn parse_alter_materialized_view(parser: &mut Parser<'_>) -> Result<Statement, String> {
    parser.expect_keyword(Keyword::ALTER).map_err(|e| e.to_string())?;
    parser.expect_keyword(Keyword::MATERIALIZED).map_err(|e| e.to_string())?;
    parser.expect_keyword(Keyword::VIEW).map_err(|e| e.to_string())?;
    let name = convert_object_name(parser.parse_object_name(false).map_err(|e| e.to_string())?)?;

    let action = if parser.parse_keyword(Keyword::SET) {
        parser
            .expect_keyword(Keyword::REFRESH)
            .map_err(|e| format!("expected REFRESH after ALTER MATERIALIZED VIEW ... SET: {e}"))?;
        AlterMaterializedViewAction::SetRefresh(parse_refresh_clause(parser)?)
    } else if peek_word_eq(parser, 0, "PAUSE") {
        parser.next_token();
        parser
            .expect_keyword(Keyword::REFRESH)
            .map_err(|e| format!("expected REFRESH after PAUSE: {e}"))?;
        AlterMaterializedViewAction::PauseRefresh
    } else if peek_word_eq(parser, 0, "RESUME") {
        parser.next_token();
        parser
            .expect_keyword(Keyword::REFRESH)
            .map_err(|e| format!("expected REFRESH after RESUME: {e}"))?;
        AlterMaterializedViewAction::ResumeRefresh
    } else {
        return Err("expected SET REFRESH, PAUSE REFRESH, or RESUME REFRESH after ALTER MATERIALIZED VIEW".to_string());
    };

    Ok(Statement::AlterMaterializedView(AlterMaterializedViewStmt { name, action }))
}
```

In `src/sql/parser/mod.rs`, dispatch this before `alter_iceberg_ref`:

```rust
if dialect::materialized_view::looks_like_alter_materialized_view(&parser) {
    let stmt = dialect::materialized_view::parse_alter_materialized_view(&mut parser)?;
    return Ok(vec![stmt]);
}
```

In `src/engine/mod.rs`, add match dispatch:

```rust
Statement::AlterMaterializedView(stmt) => {
    crate::engine::mv_flow::alter_mv(state, current_catalog, current_database, &stmt)
}
```

For this task, add a temporary stub in `src/engine/mv_flow.rs` so parser tests compile:

```rust
pub(crate) fn alter_mv(
    _state: &Arc<StandaloneState>,
    _current_catalog: Option<&str>,
    _db: &str,
    _stmt: &crate::sql::parser::ast::AlterMaterializedViewStmt,
) -> Result<StatementResult, String> {
    Err("ALTER MATERIALIZED VIEW refresh policy execution is not implemented yet".to_string())
}
```

- [ ] **Step 5: Run ALTER parser tests and commit**

Run:

```bash
cargo test --lib sql::parser::dialect::materialized_view::tests::parse_alter_mv -- --nocapture
```

Expected: ALTER parser tests pass.

Commit:

```bash
git add src/sql/parser/ast/mod.rs src/sql/parser/dialect/materialized_view.rs src/sql/parser/mod.rs src/engine/mod.rs src/engine/mv_flow.rs
git commit -m "feat: parse MV refresh policy ALTER"
```

---

### Task 3: Persist CREATE and ALTER Refresh Metadata

**Files:**

- Modify `src/engine/mv_flow.rs`
- Modify `src/connector/starrocks/managed/mv_ddl.rs`
- Modify `src/engine/mv/iceberg_refresh.rs`
- Modify `tests/standalone_mysql_server.rs`

- [ ] **Step 1: Write failing end-to-end MySQL test**

In `tests/standalone_mysql_server.rs`, add after `standalone_mysql_server_mv_show_output_matches_expected_columns`:

```rust
#[test]
fn standalone_mysql_server_mv_refresh_policy_ddl_updates_show_metadata() {
    let port = alloc_port();
    let Some((_config_dir, config_path)) = maybe_write_managed_lake_config(port) else {
        return;
    };
    let iceberg_warehouse = unique_iceberg_warehouse("mv_refresh_policy_ddl");

    let args = vec![
        "standalone-server".to_string(),
        "--config".to_string(),
        config_path.display().to_string(),
    ];
    let mut server = ServerGuard::spawn(&args);
    let mut conn = server.connect_root(port);

    conn.query_drop(create_s3_iceberg_catalog_sql("ice", &iceberg_warehouse))
        .expect("create iceberg catalog");
    conn.query_drop("create database ice.ns")
        .expect("create iceberg namespace");
    conn.query_drop("create table ice.ns.orders (k1 int, v2 bigint)")
        .expect("create iceberg orders");
    conn.query_drop("create database analytics")
        .expect("create analytics db");
    conn.query_drop("use analytics").expect("use analytics");
    conn.query_drop(
        "create materialized view orders_mv \
         distributed by hash(k1) buckets 2 \
         refresh async every interval 5 minute \
         as select k1 from ice.ns.orders",
    )
    .expect("create mv with refresh policy");

    let created: Vec<Row> = conn
        .query("show materialized views from analytics")
        .expect("show mvs after create");
    assert_eq!(created.len(), 1);
    assert_eq!(created[0].get::<String, _>(3), Some("ASYNC_INTERVAL".to_string()));
    assert_eq!(created[0].get::<String, _>(9), Some("false".to_string()));

    conn.query_drop("alter materialized view orders_mv pause refresh")
        .expect("pause refresh");
    let paused: Vec<Row> = conn
        .query("show materialized views from analytics")
        .expect("show mvs after pause");
    assert_eq!(paused[0].get::<String, _>(3), Some("ASYNC_INTERVAL".to_string()));
    assert_eq!(paused[0].get::<String, _>(9), Some("true".to_string()));

    conn.query_drop("alter materialized view orders_mv set refresh async on change")
        .expect("set refresh on change");
    conn.query_drop("alter materialized view orders_mv resume refresh")
        .expect("resume refresh");
    let resumed: Vec<Row> = conn
        .query("show materialized views from analytics")
        .expect("show mvs after resume");
    assert_eq!(resumed[0].get::<String, _>(3), Some("ASYNC_ON_CHANGE".to_string()));
    assert_eq!(resumed[0].get::<String, _>(9), Some("false".to_string()));
    assert_eq!(resumed[0].get::<Option<String>, _>(10), Some(None));
}
```

- [ ] **Step 2: Run MySQL test and verify RED**

Run:

```bash
cargo test --test standalone_mysql_server standalone_mysql_server_mv_refresh_policy_ddl_updates_show_metadata -- --nocapture
```

Expected before implementation: CREATE parser or ALTER execution fails because metadata persistence / ALTER execution is not implemented.

- [ ] **Step 3: Add refresh metadata conversion helpers**

In `src/engine/mv_flow.rs`, import:

```rust
use crate::meta::repository::mv::{StoredMvDefinition, StoredMvRefreshPolicy, UpdateMvRefreshMetadataRequest};
use crate::sql::parser::ast::{AlterMaterializedViewAction, AlterMaterializedViewStmt, MaterializedViewRefreshPolicy};
```

Add helpers:

```rust
pub(crate) fn refresh_metadata_request_for_policy(
    definition: &StoredMvDefinition,
    policy: &MaterializedViewRefreshPolicy,
    refresh_paused: bool,
) -> UpdateMvRefreshMetadataRequest {
    let (refresh_policy, refresh_interval_ms) = match policy {
        MaterializedViewRefreshPolicy::Manual => (StoredMvRefreshPolicy::Manual, None),
        MaterializedViewRefreshPolicy::AsyncOnChange => (StoredMvRefreshPolicy::AsyncOnChange, None),
        MaterializedViewRefreshPolicy::AsyncInterval { interval_ms } => {
            (StoredMvRefreshPolicy::AsyncInterval, Some(*interval_ms))
        }
    };
    UpdateMvRefreshMetadataRequest {
        mv_id: definition.mv_id,
        refresh_policy,
        refresh_paused,
        refresh_interval_ms,
        max_staleness_ms: definition.max_staleness_ms,
        last_scheduler_error: None,
        next_refresh_after_ms: None,
    }
}

pub(crate) fn refresh_metadata_request_for_create(
    mv_id: i64,
    policy: &MaterializedViewRefreshPolicy,
) -> UpdateMvRefreshMetadataRequest {
    let seed = StoredMvDefinition {
        mv_id,
        select_sql: String::new(),
        base_table_refs: Vec::new(),
        primary_key_columns: Vec::new(),
        storage_engine: String::new(),
        target_catalog: None,
        target_namespace: None,
        target_table: None,
        schema_contract: None,
        partition_spec: None,
        created_at_ms: 0,
        last_refresh_ms: None,
        last_refresh_rows: None,
        refresh_target_snapshots: std::collections::BTreeMap::new(),
        refresh_policy: StoredMvRefreshPolicy::Manual,
        refresh_paused: false,
        refresh_interval_ms: None,
        max_staleness_ms: None,
        last_scheduler_error: None,
        next_refresh_after_ms: None,
    };
    refresh_metadata_request_for_policy(&seed, policy, false)
}
```

- [ ] **Step 4: Implement ALTER metadata update**

Replace the temporary `alter_mv` stub in `src/engine/mv_flow.rs` with:

```rust
pub(crate) fn alter_mv(
    state: &Arc<StandaloneState>,
    current_catalog: Option<&str>,
    db: &str,
    stmt: &AlterMaterializedViewStmt,
) -> Result<StatementResult, String> {
    let provider = state
        .metadata_provider
        .as_ref()
        .ok_or_else(|| "ALTER MATERIALIZED VIEW requires metadata provider".to_string())?;
    let mut txn = provider
        .begin_write("alter materialized view refresh metadata")
        .map_err(|e| format!("open MV metadata write transaction failed: {e}"))?;
    let definition = load_definition_for_alter(state, txn.as_ref(), current_catalog, db, &stmt.name)?;
    let req = match &stmt.action {
        AlterMaterializedViewAction::SetRefresh(policy) => {
            refresh_metadata_request_for_policy(&definition, policy, definition.refresh_paused)
        }
        AlterMaterializedViewAction::PauseRefresh => UpdateMvRefreshMetadataRequest {
            mv_id: definition.mv_id,
            refresh_policy: definition.refresh_policy,
            refresh_paused: true,
            refresh_interval_ms: definition.refresh_interval_ms,
            max_staleness_ms: definition.max_staleness_ms,
            last_scheduler_error: definition.last_scheduler_error,
            next_refresh_after_ms: definition.next_refresh_after_ms,
        },
        AlterMaterializedViewAction::ResumeRefresh => UpdateMvRefreshMetadataRequest {
            mv_id: definition.mv_id,
            refresh_policy: definition.refresh_policy,
            refresh_paused: false,
            refresh_interval_ms: definition.refresh_interval_ms,
            max_staleness_ms: definition.max_staleness_ms,
            last_scheduler_error: definition.last_scheduler_error,
            next_refresh_after_ms: definition.next_refresh_after_ms,
        },
    };
    state
        .mv_repo
        .update_refresh_metadata(txn.as_mut(), req)
        .map_err(|e| format!("update MV refresh metadata failed: {e}"))?;
    txn.commit()
        .map_err(|e| format!("commit MV refresh metadata failed: {e}"))?;
    Ok(StatementResult::Ok)
}
```

Add `load_definition_for_alter` near `existing_mv_storage_engine_by_target`:

```rust
fn load_definition_for_alter(
    state: &Arc<StandaloneState>,
    txn: &dyn crate::meta::MetaReadTxn,
    current_catalog: Option<&str>,
    db: &str,
    name: &crate::sql::parser::ast::ObjectName,
) -> Result<StoredMvDefinition, String> {
    if current_catalog.is_some() {
        let target = crate::engine::mv::iceberg_refresh::resolve_refresh_target(
            current_catalog,
            db,
            name,
        )?;
        if let Some(definition) = state
            .mv_repo
            .find_by_target(
                txn,
                &target.catalog,
                &target.namespace,
                &target.table,
            )
            .map_err(|e| format!("load MV definition by target failed: {e}"))?
        {
            if MvStorageEngine::from_sql_str(&definition.storage_engine)?
                == MvStorageEngine::Iceberg
            {
                return Ok(definition);
            }
        }
    }

    let (database, mv_name) =
        crate::connector::starrocks::managed::mv_ddl::resolve_mv_name(name, db)?;
    let runtime = {
        let managed = state
            .managed_lake
            .read()
            .expect("standalone managed lake read lock");
        managed.table(&database, &mv_name).ok().cloned()
    };
    let Some(runtime) = runtime else {
        return Err(format!("materialized view does not exist: {database}.{mv_name}"));
    };
    if runtime.table.kind != crate::connector::starrocks::managed::model::ManagedTableKind::MaterializedView {
        return Err(format!(
            "`{database}.{mv_name}` is not a materialized view"
        ));
    }
    state
        .mv_repo
        .load_by_id(txn, runtime.table.table_id)
        .map_err(|e| format!("load MV definition failed: {e}"))?
        .ok_or_else(|| format!("MV definition {} not found", runtime.table.table_id))
}
```

- [ ] **Step 5: Persist CREATE refresh policy**

In `src/connector/starrocks/managed/mv_ddl.rs`, after `create_definition_with_id(...)` returns `mv_definition`, call:

```rust
state
    .mv_repo
    .update_refresh_metadata(
        txn.as_mut(),
        crate::engine::mv_flow::refresh_metadata_request_for_create(
            mv_definition.mv_id,
            &stmt.refresh_policy,
        ),
    )
    .map_err(|e| format!("persist materialized view refresh metadata failed: {e}"))?;
```

In `src/engine/mv/iceberg_refresh.rs`, after iceberg MV `create_definition(...)` returns `mv_definition`, call the same helper inside the same write transaction:

```rust
state
    .mv_repo
    .update_refresh_metadata(
        txn.as_mut(),
        crate::engine::mv_flow::refresh_metadata_request_for_create(
            mv_definition.mv_id,
            &stmt.refresh_policy,
        ),
    )
    .map_err(|e| format!("create iceberg MV refresh metadata failed: {e}"))?;
```

- [ ] **Step 6: Run MySQL test and focused parser tests**

Run:

```bash
cargo test --test standalone_mysql_server standalone_mysql_server_mv_refresh_policy_ddl_updates_show_metadata -- --nocapture
cargo test --lib sql::parser::dialect::materialized_view::tests::parse_alter_mv -- --nocapture
cargo test --lib sql::parser::dialect::materialized_view::tests::parse_create_mv_accepts_refresh_async -- --nocapture
```

Expected: all pass.

- [ ] **Step 7: Commit metadata persistence**

Run:

```bash
git add src/engine/mv_flow.rs src/connector/starrocks/managed/mv_ddl.rs src/engine/mv/iceberg_refresh.rs tests/standalone_mysql_server.rs
git commit -m "feat: persist MV refresh policy DDL"
```

---

### Task 4: Final Verification

**Files:**

- No source edits expected.

- [ ] **Step 1: Run formatter**

Run:

```bash
cargo fmt
```

Expected: formatting succeeds. Do not stage unrelated pre-existing formatting changes unless they were caused by B4-2 edits.

- [ ] **Step 2: Run focused verification**

Run:

```bash
cargo fmt --check
cargo check --lib
cargo test --lib sql::parser::dialect::materialized_view::tests::parse_create_mv -- --nocapture
cargo test --lib sql::parser::dialect::materialized_view::tests::parse_alter_mv -- --nocapture
cargo test --test standalone_mysql_server standalone_mysql_server_mv_refresh_policy_ddl_updates_show_metadata -- --nocapture
cargo test --test standalone_mysql_server standalone_mysql_server_mv_show_output_matches_expected_columns -- --nocapture
git diff --check origin/main..HEAD
```

Expected: all commands exit 0. Existing repository warnings are acceptable if they are not introduced by B4-2.

- [ ] **Step 3: Commit formatting-only changes if needed**

If `cargo fmt` changed files touched by B4-2 after the previous commits:

```bash
git add <B4-2 touched files only>
git commit -m "style: format MV refresh policy DDL"
```

Do not stage unrelated dirty files.
