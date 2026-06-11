# Iceberg V3 Compact Data Files Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add Iceberg v3 data-file compaction support in NovaRocks standalone: external `replace` compaction compatibility plus persisted asynchronous `ALTER TABLE ... OPTIMIZE` whole-table rewrite.

**Architecture:** Implement the user-visible `OPTIMIZE` path as a persisted Iceberg maintenance job in the standalone metadata store, exposed through `ALTER TABLE ... OPTIMIZE` and `SHOW ALTER TABLE OPTIMIZE`. The worker runs a whole-table visible-row rewrite, writes new data files, and commits an Iceberg `Operation::Replace` snapshot via a dedicated `RewriteDataFilesCommit` action that retires old data and delete manifests while preserving strict OCC. Existing MV/Iceberg change planning continues to treat validated `replace` compaction snapshots as no-op lineage steps.

**Tech Stack:** Rust, rusqlite metadata store, sqlparser, iceberg-rust 0.9 vendor patch, Arrow `RecordBatch`/`Chunk`, OpenDAL, `tests/sql-test-runner`.

---

## File Structure

- Modify: `src/connector/starrocks/managed/store.rs`
  - Add `StoredIcebergOptimizeJob`, `IcebergOptimizeJobState`, `IcebergOptimizeJobOutcome`.
  - Add `iceberg_optimize_jobs` schema, load/save support, job create/claim/finish/fail/show helpers, and startup RUNNING-to-FAILED recovery.
- Modify: `src/engine/statement.rs`
  - Add parser helpers for `ALTER TABLE ... OPTIMIZE`.
  - Add parser tests for accepted and rejected optimize syntax.
- Modify: `src/engine/mod.rs`
  - Dispatch `ALTER TABLE ... OPTIMIZE` before generic sqlparser parsing.
  - Dispatch `SHOW ALTER TABLE OPTIMIZE` before generic sqlparser parsing.
  - Spawn the Iceberg optimize worker during `StandaloneNovaRocks::open`.
- Create: `src/connector/iceberg/compact.rs`
  - Own the worker loop and single-job executor.
  - Resolve table metadata, validate base snapshot, run visible-row scan, write compacted data files, run commit, and finish/fail the persisted job.
- Modify: `src/connector/iceberg/mod.rs`
  - Export the new `compact` module.
- Modify: `src/connector/mod.rs`
  - Re-export `spawn_iceberg_optimize_worker`; `src/engine/mod.rs` will call the connector-level re-export.
- Create: `src/connector/iceberg/commit/rewrite_data_files.rs`
  - Implement `RewriteDataFilesCommit`.
  - Enumerate live data/delete entries from the base snapshot and write DELETED/ADDED manifests for an Iceberg `replace` snapshot.
- Modify: `src/connector/iceberg/commit/mod.rs`
  - Export `RewriteDataFilesCommit`.
- Modify: `src/connector/iceberg/commit/run.rs`
  - Dispatch `CommitOpKind::RewriteDataFiles`.
- Modify: `src/connector/iceberg/commit/types.rs`
  - Add `CommitOpKind::RewriteDataFiles`.
- Modify: `src/connector/iceberg/changes.rs`
  - Add tests proving validated external `replace` compaction snapshots are skipped and invalid ones fail fast.
- Modify: `src/engine/iceberg_writer.rs`
  - Make `run_select_to_chunks` `pub(crate)` so `compact.rs` can reuse the exact SELECT execution path.
- Create: `sql-tests/iceberg/sql/iceberg_v3_optimize_compact_data_files.sql`
  - End-to-end SQL coverage for v3 row lineage, DV deletion, `OPTIMIZE`, `SHOW ALTER TABLE OPTIMIZE`, and MV refresh stability.

## Task 1: Persist Iceberg OPTIMIZE Jobs

**Files:**
- Modify: `src/connector/starrocks/managed/store.rs`

- [ ] **Step 1: Add failing metadata-store tests**

Add these tests to the existing `#[cfg(test)] mod tests` in `src/connector/starrocks/managed/store.rs`:

```rust
#[test]
fn iceberg_optimize_job_lifecycle_round_trips() {
    let store = test_store();
    let job_id = store
        .create_iceberg_optimize_job("ice", "ns1", "orders", Some(10))
        .expect("create optimize job");

    assert_eq!(job_id, 1);
    let jobs = store
        .show_iceberg_optimize_jobs(Some("ns1"), Some("orders"), 10)
        .expect("show optimize jobs");
    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].catalog, "ice");
    assert_eq!(jobs[0].namespace, "ns1");
    assert_eq!(jobs[0].table, "orders");
    assert_eq!(jobs[0].base_snapshot_id, Some(10));
    assert_eq!(jobs[0].state, IcebergOptimizeJobState::Pending);

    assert!(store.claim_iceberg_optimize_job(job_id).expect("claim"));
    let running = store
        .show_iceberg_optimize_jobs(Some("ns1"), Some("orders"), 10)
        .expect("show running");
    assert_eq!(running[0].state, IcebergOptimizeJobState::Running);

    store
        .finish_iceberg_optimize_job(
            job_id,
            IcebergOptimizeJobOutcome {
                target_snapshot_id: Some(11),
                input_data_files: 3,
                output_data_files: 1,
                input_delete_files: 2,
                output_delete_files: 0,
                message: "compacted 3 data files and 2 delete files into 1 data file".to_string(),
            },
        )
        .expect("finish");

    let finished = store
        .show_iceberg_optimize_jobs(Some("ns1"), Some("orders"), 10)
        .expect("show finished");
    assert_eq!(finished[0].state, IcebergOptimizeJobState::Finished);
    assert_eq!(finished[0].target_snapshot_id, Some(11));
    assert_eq!(finished[0].input_data_files, 3);
    assert_eq!(finished[0].output_data_files, 1);
    assert_eq!(finished[0].input_delete_files, 2);
    assert_eq!(finished[0].output_delete_files, 0);
    assert_eq!(
        finished[0].message.as_deref(),
        Some("compacted 3 data files and 2 delete files into 1 data file")
    );
}

#[test]
fn iceberg_optimize_rejects_active_job_for_same_table() {
    let store = test_store();
    store
        .create_iceberg_optimize_job("ice", "ns1", "orders", Some(10))
        .expect("create first job");
    let err = store
        .create_iceberg_optimize_job("ice", "ns1", "orders", Some(10))
        .expect_err("same table active job should be rejected");
    assert!(err.contains("active OPTIMIZE job already exists"), "{err}");

    store
        .create_iceberg_optimize_job("ice", "ns1", "lineitem", Some(10))
        .expect("different table can create job");
}

#[test]
fn iceberg_optimize_startup_marks_running_jobs_failed() {
    let store = test_store();
    let job_id = store
        .create_iceberg_optimize_job("ice", "ns1", "orders", Some(10))
        .expect("create optimize job");
    assert!(store.claim_iceberg_optimize_job(job_id).expect("claim"));

    let changed = store
        .fail_running_iceberg_optimize_jobs_on_startup()
        .expect("fail running jobs");
    assert_eq!(changed, 1);

    let jobs = store
        .show_iceberg_optimize_jobs(Some("ns1"), Some("orders"), 10)
        .expect("show jobs");
    assert_eq!(jobs[0].state, IcebergOptimizeJobState::Failed);
    assert_eq!(
        jobs[0].message.as_deref(),
        Some("server restarted while optimize job was running")
    );
}
```

- [ ] **Step 2: Run tests and verify they fail**

Run:

```bash
cargo test --lib connector::starrocks::managed::store::tests::iceberg_optimize_job_lifecycle_round_trips -- --exact
```

Expected: compile fails because `create_iceberg_optimize_job`, `IcebergOptimizeJobState`, and `IcebergOptimizeJobOutcome` do not exist.

- [ ] **Step 3: Add job structs and state enum**

In `src/connector/starrocks/managed/store.rs`, near `StoredIcebergTable`, add:

```rust
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum IcebergOptimizeJobState {
    Pending,
    Running,
    Failed,
    Finished,
}

impl IcebergOptimizeJobState {
    fn as_sql_str(self) -> &'static str {
        match self {
            Self::Pending => "PENDING",
            Self::Running => "RUNNING",
            Self::Failed => "FAILED",
            Self::Finished => "FINISHED",
        }
    }

    fn from_sql_str(value: &str) -> Result<Self, String> {
        match value {
            "PENDING" => Ok(Self::Pending),
            "RUNNING" => Ok(Self::Running),
            "FAILED" => Ok(Self::Failed),
            "FINISHED" => Ok(Self::Finished),
            _ => Err(format!("unknown iceberg optimize job state `{value}`")),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct StoredIcebergOptimizeJob {
    pub job_id: i64,
    pub catalog: String,
    pub namespace: String,
    pub table: String,
    pub base_snapshot_id: Option<i64>,
    pub target_snapshot_id: Option<i64>,
    pub state: IcebergOptimizeJobState,
    pub input_data_files: i64,
    pub output_data_files: i64,
    pub input_delete_files: i64,
    pub output_delete_files: i64,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
    pub finished_at_ms: Option<i64>,
    pub message: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct IcebergOptimizeJobOutcome {
    pub target_snapshot_id: Option<i64>,
    pub input_data_files: i64,
    pub output_data_files: i64,
    pub input_delete_files: i64,
    pub output_delete_files: i64,
    pub message: String,
}
```

Add `pub iceberg_optimize_jobs: Vec<StoredIcebergOptimizeJob>` to `MetadataSnapshot`.

- [ ] **Step 4: Add schema migration**

In `init_schema`, add this table before `PRAGMA user_version` and change the version to `8`:

```sql
CREATE TABLE IF NOT EXISTS iceberg_optimize_jobs (
    job_id INTEGER PRIMARY KEY,
    catalog_name TEXT NOT NULL,
    namespace_name TEXT NOT NULL,
    table_name TEXT NOT NULL,
    base_snapshot_id INTEGER,
    target_snapshot_id INTEGER,
    state TEXT NOT NULL
        CHECK (state IN ('PENDING', 'RUNNING', 'FAILED', 'FINISHED')),
    input_data_files INTEGER NOT NULL DEFAULT 0,
    output_data_files INTEGER NOT NULL DEFAULT 0,
    input_delete_files INTEGER NOT NULL DEFAULT 0,
    output_delete_files INTEGER NOT NULL DEFAULT 0,
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    finished_at_ms INTEGER,
    message TEXT
);
PRAGMA user_version = 8;
```

The existing `CREATE TABLE IF NOT EXISTS` style handles existing metadata stores when `init_schema` runs.

- [ ] **Step 5: Load jobs in `load_snapshot`**

In `load_snapshot`, query the new table and include it in `MetadataSnapshot`:

```rust
let iceberg_optimize_jobs = {
    let mut stmt = conn
        .prepare(
            "SELECT
                job_id,
                catalog_name,
                namespace_name,
                table_name,
                base_snapshot_id,
                target_snapshot_id,
                state,
                input_data_files,
                output_data_files,
                input_delete_files,
                output_delete_files,
                created_at_ms,
                updated_at_ms,
                finished_at_ms,
                message
             FROM iceberg_optimize_jobs
             ORDER BY job_id",
        )
        .map_err(|e| format!("prepare iceberg_optimize_jobs query failed: {e}"))?;
    let rows = stmt
        .query_map([], |row| {
            let state = IcebergOptimizeJobState::from_sql_str(&row.get::<_, String>(6)?)
                .map_err(invalid_state_sql_error)?;
            Ok(StoredIcebergOptimizeJob {
                job_id: row.get(0)?,
                catalog: row.get(1)?,
                namespace: row.get(2)?,
                table: row.get(3)?,
                base_snapshot_id: row.get(4)?,
                target_snapshot_id: row.get(5)?,
                state,
                input_data_files: row.get(7)?,
                output_data_files: row.get(8)?,
                input_delete_files: row.get(9)?,
                output_delete_files: row.get(10)?,
                created_at_ms: row.get(11)?,
                updated_at_ms: row.get(12)?,
                finished_at_ms: row.get(13)?,
                message: row.get(14)?,
            })
        })
        .map_err(|e| format!("query iceberg_optimize_jobs failed: {e}"))?;
    rows.collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("read iceberg_optimize_jobs failed: {e}"))?
};
```

- [ ] **Step 6: Add job mutation methods**

Add these `SqliteMetadataStore` methods:

```rust
pub(crate) fn create_iceberg_optimize_job(
    &self,
    catalog: &str,
    namespace: &str,
    table: &str,
    base_snapshot_id: Option<i64>,
) -> Result<i64, String> {
    let mut conn = self.connection()?;
    let tx = begin_write_transaction(&mut conn, "create_iceberg_optimize_job")?;
    let active_count: i64 = tx
        .query_row(
            "SELECT COUNT(*)
             FROM iceberg_optimize_jobs
             WHERE catalog_name = ?1
               AND namespace_name = ?2
               AND table_name = ?3
               AND state IN ('PENDING', 'RUNNING')",
            params![catalog, namespace, table],
            |row| row.get(0),
        )
        .map_err(|e| format!("count active iceberg optimize jobs failed: {e}"))?;
    if active_count > 0 {
        return Err(format!(
            "active OPTIMIZE job already exists for iceberg table {catalog}.{namespace}.{table}"
        ));
    }
    let job_id: i64 = tx
        .query_row(
            "SELECT COALESCE(MAX(job_id), 0) + 1 FROM iceberg_optimize_jobs",
            [],
            |row| row.get(0),
        )
        .map_err(|e| format!("allocate iceberg optimize job id failed: {e}"))?;
    tx.execute(
        "INSERT INTO iceberg_optimize_jobs(
            job_id,
            catalog_name,
            namespace_name,
            table_name,
            base_snapshot_id,
            target_snapshot_id,
            state,
            input_data_files,
            output_data_files,
            input_delete_files,
            output_delete_files,
            created_at_ms,
            updated_at_ms,
            finished_at_ms,
            message
        ) VALUES (
            ?1, ?2, ?3, ?4, ?5, NULL, 'PENDING',
            0, 0, 0, 0,
            strftime('%s','now') * 1000,
            strftime('%s','now') * 1000,
            NULL,
            NULL
        )",
        params![job_id, catalog, namespace, table, base_snapshot_id],
    )
    .map_err(|e| format!("insert iceberg optimize job failed: {e}"))?;
    tx.commit()
        .map_err(|e| format!("commit create_iceberg_optimize_job failed: {e}"))?;
    Ok(job_id)
}

pub(crate) fn list_pending_iceberg_optimize_jobs(
    &self,
) -> Result<Vec<StoredIcebergOptimizeJob>, String> {
    Ok(self
        .show_iceberg_optimize_jobs(None, None, i64::MAX as usize)?
        .into_iter()
        .filter(|job| job.state == IcebergOptimizeJobState::Pending)
        .collect())
}

pub(crate) fn claim_iceberg_optimize_job(&self, job_id: i64) -> Result<bool, String> {
    let changed = self
        .connection()?
        .execute(
            "UPDATE iceberg_optimize_jobs
             SET state = 'RUNNING',
                 updated_at_ms = strftime('%s','now') * 1000
             WHERE job_id = ?1 AND state = 'PENDING'",
            params![job_id],
        )
        .map_err(|e| format!("claim iceberg optimize job failed: {e}"))?;
    Ok(changed == 1)
}

pub(crate) fn finish_iceberg_optimize_job(
    &self,
    job_id: i64,
    outcome: IcebergOptimizeJobOutcome,
) -> Result<(), String> {
    self.connection()?
        .execute(
            "UPDATE iceberg_optimize_jobs
             SET state = 'FINISHED',
                 target_snapshot_id = ?2,
                 input_data_files = ?3,
                 output_data_files = ?4,
                 input_delete_files = ?5,
                 output_delete_files = ?6,
                 updated_at_ms = strftime('%s','now') * 1000,
                 finished_at_ms = strftime('%s','now') * 1000,
                 message = ?7
             WHERE job_id = ?1",
            params![
                job_id,
                outcome.target_snapshot_id,
                outcome.input_data_files,
                outcome.output_data_files,
                outcome.input_delete_files,
                outcome.output_delete_files,
                outcome.message,
            ],
        )
        .map_err(|e| format!("finish iceberg optimize job failed: {e}"))?;
    Ok(())
}

pub(crate) fn fail_iceberg_optimize_job(&self, job_id: i64, message: &str) -> Result<(), String> {
    self.connection()?
        .execute(
            "UPDATE iceberg_optimize_jobs
             SET state = 'FAILED',
                 updated_at_ms = strftime('%s','now') * 1000,
                 finished_at_ms = strftime('%s','now') * 1000,
                 message = ?2
             WHERE job_id = ?1",
            params![job_id, message],
        )
        .map_err(|e| format!("fail iceberg optimize job failed: {e}"))?;
    Ok(())
}

pub(crate) fn fail_running_iceberg_optimize_jobs_on_startup(&self) -> Result<usize, String> {
    let changed = self
        .connection()?
        .execute(
            "UPDATE iceberg_optimize_jobs
             SET state = 'FAILED',
                 updated_at_ms = strftime('%s','now') * 1000,
                 finished_at_ms = strftime('%s','now') * 1000,
                 message = 'server restarted while optimize job was running'
             WHERE state = 'RUNNING'",
            [],
        )
        .map_err(|e| format!("fail running iceberg optimize jobs failed: {e}"))?;
    Ok(changed)
}

pub(crate) fn show_iceberg_optimize_jobs(
    &self,
    namespace: Option<&str>,
    table: Option<&str>,
    limit: usize,
) -> Result<Vec<StoredIcebergOptimizeJob>, String> {
    let mut jobs = self.load_snapshot()?.iceberg_optimize_jobs;
    if let Some(namespace) = namespace {
        jobs.retain(|job| job.namespace.eq_ignore_ascii_case(namespace));
    }
    if let Some(table) = table {
        jobs.retain(|job| job.table.eq_ignore_ascii_case(table));
    }
    jobs.sort_by(|a, b| b.job_id.cmp(&a.job_id));
    jobs.truncate(limit);
    Ok(jobs)
}
```

- [ ] **Step 7: Run metadata-store tests**

Run:

```bash
cargo test --lib connector::starrocks::managed::store::tests::iceberg_optimize_job_lifecycle_round_trips -- --exact
cargo test --lib connector::starrocks::managed::store::tests::iceberg_optimize_rejects_active_job_for_same_table -- --exact
cargo test --lib connector::starrocks::managed::store::tests::iceberg_optimize_startup_marks_running_jobs_failed -- --exact
```

Expected: all three pass.

- [ ] **Step 8: Commit Task 1**

Run:

```bash
git add src/connector/starrocks/managed/store.rs
git commit -m "feat: persist iceberg optimize jobs"
```

## Task 2: Add OPTIMIZE SQL and SHOW ALTER TABLE OPTIMIZE

**Files:**
- Modify: `src/engine/statement.rs`
- Modify: `src/engine/mod.rs`
- Test: `src/engine/statement.rs`
- Test: `src/engine/mod.rs`

- [ ] **Step 1: Add parser tests**

In `src/engine/statement.rs` tests, add:

```rust
#[test]
fn parse_alter_table_optimize_accepts_three_part_table() {
    let stmt = parse_alter_table_optimize_sql("ALTER TABLE ice.ns.orders OPTIMIZE").unwrap();
    assert_eq!(
        stmt.table.parts,
        vec!["ice".to_string(), "ns".to_string(), "orders".to_string()]
    );
}

#[test]
fn parse_alter_table_optimize_rejects_partition_clause() {
    let err = parse_alter_table_optimize_sql("ALTER TABLE ice.ns.orders OPTIMIZE PARTITION (p1)")
        .expect_err("partition optimize is out of scope");
    assert!(err.contains("OPTIMIZE only supports whole-table compaction"), "{err}");
}

#[test]
fn parse_show_alter_table_optimize_extracts_filter() {
    let stmt = parse_show_alter_table_optimize_sql(
        "SHOW ALTER TABLE OPTIMIZE FROM ns WHERE TableName = 'orders' ORDER BY CreateTime DESC LIMIT 1",
    )
    .unwrap();
    assert_eq!(stmt.database.as_deref(), Some("ns"));
    assert_eq!(stmt.table_name.as_deref(), Some("orders"));
    assert_eq!(stmt.limit, 1);
}
```

- [ ] **Step 2: Run parser tests and verify they fail**

Run:

```bash
cargo test --lib engine::statement::tests::parse_alter_table_optimize_accepts_three_part_table -- --exact
```

Expected: compile fails because the parser functions and structs do not exist.

- [ ] **Step 3: Add statement structs and lookahead helpers**

In `src/engine/statement.rs`, near the ad hoc DDL structs, add:

```rust
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct AlterTableOptimizeStmt {
    pub(crate) table: ObjectName,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ShowAlterTableOptimizeStmt {
    pub(crate) database: Option<String>,
    pub(crate) table_name: Option<String>,
    pub(crate) limit: usize,
}

pub(crate) fn looks_like_alter_table_optimize(sql: &str) -> bool {
    let upper = sql.trim().to_ascii_uppercase();
    upper.starts_with("ALTER TABLE") && upper.contains(" OPTIMIZE")
}

pub(crate) fn looks_like_show_alter_table_optimize(sql: &str) -> bool {
    let upper = sql.trim().to_ascii_uppercase();
    upper.starts_with("SHOW ALTER TABLE OPTIMIZE")
}
```

- [ ] **Step 4: Add optimize parsers**

Add:

```rust
pub(crate) fn parse_alter_table_optimize_sql(
    sql: &str,
) -> Result<AlterTableOptimizeStmt, String> {
    let normalized = crate::sql::parser::dialect::normalize_for_raw_parse(sql)?;
    let mut parser = Parser::new(&StarRocksDialect)
        .try_with_sql(&normalized)
        .map_err(|e| format!("parse ALTER TABLE OPTIMIZE: {e}"))?;
    parser.expect_keyword(Keyword::ALTER).map_err(|e| e.to_string())?;
    parser.expect_keyword(Keyword::TABLE).map_err(|e| e.to_string())?;
    let table = crate::sql::parser::dialect::convert_object_name(
        parser.parse_object_name(false).map_err(|e| e.to_string())?,
    )?;
    if !crate::sql::parser::dialect::peek_word_eq(&parser, 0, "OPTIMIZE") {
        return Err("ALTER TABLE OPTIMIZE requires OPTIMIZE keyword".to_string());
    }
    parser.next_token();
    if parser.peek_token_ref().token == Token::SemiColon {
        parser.next_token();
    }
    if parser.peek_token_ref().token != Token::EOF {
        return Err(format!(
            "OPTIMIZE only supports whole-table compaction; unsupported trailing tokens starting at {}",
            parser.peek_token_ref().token
        ));
    }
    Ok(AlterTableOptimizeStmt { table })
}

pub(crate) fn parse_show_alter_table_optimize_sql(
    sql: &str,
) -> Result<ShowAlterTableOptimizeStmt, String> {
    let normalized = crate::sql::parser::dialect::normalize_for_raw_parse(sql)?;
    let mut parser = Parser::new(&StarRocksDialect)
        .try_with_sql(&normalized)
        .map_err(|e| format!("parse SHOW ALTER TABLE OPTIMIZE: {e}"))?;
    parser.expect_keyword(Keyword::SHOW).map_err(|e| e.to_string())?;
    parser.expect_keyword(Keyword::ALTER).map_err(|e| e.to_string())?;
    parser.expect_keyword(Keyword::TABLE).map_err(|e| e.to_string())?;
    if !crate::sql::parser::dialect::peek_word_eq(&parser, 0, "OPTIMIZE") {
        return Err("SHOW ALTER TABLE OPTIMIZE requires OPTIMIZE keyword".to_string());
    }
    parser.next_token();

    let mut database = None;
    if parser.parse_keyword(Keyword::FROM) {
        database = Some(parser.parse_identifier().map_err(|e| e.to_string())?.value);
    }

    let mut table_name = None;
    if parser.parse_keyword(Keyword::WHERE) {
        let expr = parser.parse_expr().map_err(|e| e.to_string())?;
        table_name = parse_show_optimize_table_filter(&expr)?;
    }

    if parser.parse_keyword(Keyword::ORDER) {
        parser.expect_keyword(Keyword::BY).map_err(|e| e.to_string())?;
        let _ = parser.parse_expr().map_err(|e| e.to_string())?;
        let _ = parser.parse_keyword(Keyword::DESC) || parser.parse_keyword(Keyword::ASC);
    }

    let mut limit = 100usize;
    if parser.parse_keyword(Keyword::LIMIT) {
        let token = parser.next_token();
        let Token::Number(value, _) = token.token else {
            return Err(format!("SHOW ALTER TABLE OPTIMIZE LIMIT requires a number, got {}", token.token));
        };
        limit = value
            .parse::<usize>()
            .map_err(|e| format!("invalid SHOW ALTER TABLE OPTIMIZE LIMIT `{value}`: {e}"))?;
    }

    if parser.peek_token_ref().token == Token::SemiColon {
        parser.next_token();
    }
    if parser.peek_token_ref().token != Token::EOF {
        return Err(format!(
            "unsupported SHOW ALTER TABLE OPTIMIZE trailing tokens starting at {}",
            parser.peek_token_ref().token
        ));
    }

    Ok(ShowAlterTableOptimizeStmt {
        database,
        table_name,
        limit,
    })
}

fn parse_show_optimize_table_filter(expr: &sqlparser::ast::Expr) -> Result<Option<String>, String> {
    use sqlparser::ast::{BinaryOperator, Expr as SqlExpr, Value};
    let SqlExpr::BinaryOp { left, op, right } = expr else {
        return Err(format!("SHOW ALTER TABLE OPTIMIZE only supports TableName = 'name', got {expr}"));
    };
    if *op != BinaryOperator::Eq {
        return Err(format!("SHOW ALTER TABLE OPTIMIZE only supports equality filter, got {op}"));
    }
    let column_name = match left.as_ref() {
        SqlExpr::Identifier(ident) => ident.value.as_str(),
        _ => return Err(format!("SHOW ALTER TABLE OPTIMIZE filter left side must be TableName, got {left}")),
    };
    if !column_name.eq_ignore_ascii_case("TableName") {
        return Err(format!("SHOW ALTER TABLE OPTIMIZE only supports TableName filter, got {column_name}"));
    }
    match right.as_ref() {
        SqlExpr::Value(Value::SingleQuotedString(value))
        | SqlExpr::Value(Value::DoubleQuotedString(value)) => Ok(Some(value.clone())),
        _ => Err(format!("SHOW ALTER TABLE OPTIMIZE filter right side must be a string literal, got {right}")),
    }
}
```

- [ ] **Step 5: Wire `ALTER TABLE ... OPTIMIZE` dispatch**

In `src/engine/mod.rs`, import the new helpers:

```rust
looks_like_alter_table_optimize, looks_like_show_alter_table_optimize,
parse_alter_table_optimize_sql, parse_show_alter_table_optimize_sql,
```

Add dispatch before `looks_like_alter_iceberg_schema`:

```rust
if looks_like_alter_table_optimize(&normalized) {
    let stmt = parse_alter_table_optimize_sql(&normalized)?;
    return self.handle_alter_table_optimize(&stmt, current_catalog, current_database);
}

if looks_like_show_alter_table_optimize(&normalized) {
    let stmt = parse_show_alter_table_optimize_sql(&normalized)?;
    return self.handle_show_alter_table_optimize(&stmt, current_database);
}
```

Add methods:

```rust
fn handle_alter_table_optimize(
    &self,
    stmt: &crate::engine::statement::AlterTableOptimizeStmt,
    current_catalog: Option<&str>,
    current_database: &str,
) -> Result<StatementResult, String> {
    let target = crate::engine::backend_resolver::resolve_table_target(
        &self.inner,
        &stmt.table,
        current_catalog,
        current_database,
    )?;
    if target.backend_name != "iceberg" {
        return Err(format!(
            "ALTER TABLE OPTIMIZE only supports iceberg backends, got `{}`",
            target.backend_name
        ));
    }
    let entry = {
        let registry = self
            .inner
            .iceberg_catalogs
            .read()
            .map_err(|e| format!("iceberg catalog registry read lock: {e}"))?;
        registry.get(&target.catalog)?
    };
    let catalog = crate::connector::iceberg::catalog::registry::build_hadoop_catalog(&entry)?;
    let table_ident = iceberg::TableIdent::new(
        iceberg::NamespaceIdent::new(target.namespace.clone()),
        target.table.clone(),
    );
    let table = crate::connector::iceberg::catalog::registry::block_on_iceberg(async {
        catalog.load_table(&table_ident).await
    })?
    .map_err(|e| format!("load iceberg table {}.{}.{} for OPTIMIZE: {e}", target.catalog, target.namespace, target.table))?;
    let base_snapshot_id = table.metadata().current_snapshot().map(|s| s.snapshot_id());
    let store = self
        .inner
        .metadata_store
        .as_ref()
        .ok_or_else(|| "ALTER TABLE OPTIMIZE requires standalone metadata store".to_string())?;
    store.create_iceberg_optimize_job(
        &target.catalog,
        &target.namespace,
        &target.table,
        base_snapshot_id,
    )?;
    Ok(StatementResult::Ok)
}

fn handle_show_alter_table_optimize(
    &self,
    stmt: &crate::engine::statement::ShowAlterTableOptimizeStmt,
    current_database: &str,
) -> Result<StatementResult, String> {
    let store = self
        .inner
        .metadata_store
        .as_ref()
        .ok_or_else(|| "SHOW ALTER TABLE OPTIMIZE requires standalone metadata store".to_string())?;
    let namespace = stmt.database.as_deref().unwrap_or(current_database);
    let jobs = store.show_iceberg_optimize_jobs(
        Some(namespace),
        stmt.table_name.as_deref(),
        stmt.limit,
    )?;
    build_show_alter_table_optimize_result(jobs).map(StatementResult::Query)
}
```

Add a local `build_show_alter_table_optimize_result` that returns columns `JobId`, `TableName`, `State`, `CreateTime`, `FinishTime`, `Msg`, `BaseSnapshotId`, `TargetSnapshotId`, `InputDataFiles`, `OutputDataFiles`, `InputDeleteFiles`, `OutputDeleteFiles`. Use Arrow `StringArray` for all columns to keep formatting simple and stable for sql-tests.

- [ ] **Step 6: Add engine-level SQL smoke test**

In `src/engine/mod.rs` tests, add a test that opens a metadata-backed engine and proves `SHOW ALTER TABLE OPTIMIZE` returns a row after inserting a synthetic job through the store:

```rust
#[test]
fn show_alter_table_optimize_reads_persisted_jobs() {
    let tmp = tempfile::tempdir().unwrap();
    let metadata = tmp.path().join("metadata.db");
    let engine = StandaloneNovaRocks::open(StandaloneOptions {
        metadata_db_path: Some(metadata),
        ..Default::default()
    })
    .unwrap();
    let store = engine.inner.metadata_store.as_ref().unwrap();
    store
        .create_iceberg_optimize_job("ice", "db1", "orders", Some(10))
        .unwrap();

    let result = engine
        .session()
        .execute("SHOW ALTER TABLE OPTIMIZE FROM db1 WHERE TableName = 'orders' ORDER BY CreateTime DESC LIMIT 1")
        .unwrap();
    let StatementResult::Query(query) = result else {
        panic!("expected query result");
    };
    let text = format!("{:?}", query.chunks);
    assert!(text.contains("orders"), "{text}");
    assert!(text.contains("PENDING"), "{text}");
}
```

- [ ] **Step 7: Run parser and engine tests**

Run:

```bash
cargo test --lib engine::statement::tests::parse_alter_table_optimize_accepts_three_part_table -- --exact
cargo test --lib engine::statement::tests::parse_alter_table_optimize_rejects_partition_clause -- --exact
cargo test --lib engine::statement::tests::parse_show_alter_table_optimize_extracts_filter -- --exact
cargo test --lib engine::tests::show_alter_table_optimize_reads_persisted_jobs -- --exact
```

Expected: all pass.

- [ ] **Step 8: Commit Task 2**

Run:

```bash
git add src/engine/statement.rs src/engine/mod.rs
git commit -m "feat: add iceberg optimize sql job surface"
```

## Task 3: Strengthen External Replace-Compaction Compatibility

**Files:**
- Modify: `src/connector/iceberg/changes.rs`

- [ ] **Step 1: Add replace-compaction classifier tests**

In `src/connector/iceberg/changes.rs`, add tests near the existing `classify_snapshot_replace_compaction_is_skipped` tests:

```rust
#[test]
fn classify_lineage_skips_delete_eliminating_replace_compaction() {
    let parent = snap(
        1,
        None,
        Operation::Append,
        &[("total-records", "3"), ("added-data-files", "2")],
        0,
    );
    let owned = replace_props_with_delete_counts(3, 1, 2, 2, 0);
    let props = owned
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect::<Vec<_>>();
    let replace = snap(2, Some(1), Operation::Replace, &props, 0);
    assert_eq!(classify_snapshot(&replace, Some(&parent)).unwrap(), None);
}

#[test]
fn classify_lineage_rejects_replace_that_changes_total_records() {
    let parent = snap(1, None, Operation::Append, &[("total-records", "3")], 0);
    let owned = replace_props_with_delete_counts(2, 1, 2, 1, 0);
    let props = owned
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect::<Vec<_>>();
    let replace = snap(2, Some(1), Operation::Replace, &props, 0);
    let err = classify_snapshot(&replace, Some(&parent)).expect_err("records changed");
    assert!(err.to_string().contains("total-records changed"), "{err}");
}

fn replace_props_with_delete_counts(
    total_records: i64,
    added_data_files: i64,
    deleted_data_files: i64,
    deleted_delete_files: i64,
    added_delete_files: i64,
) -> Vec<(String, String)> {
    vec![
        ("total-records".to_string(), total_records.to_string()),
        ("added-data-files".to_string(), added_data_files.to_string()),
        ("deleted-data-files".to_string(), deleted_data_files.to_string()),
        ("removed-delete-files".to_string(), deleted_delete_files.to_string()),
        ("added-delete-files".to_string(), added_delete_files.to_string()),
    ]
}
```

- [ ] **Step 2: Run tests and inspect current behavior**

Run:

```bash
cargo test --lib connector::iceberg::changes::tests::classify_lineage_skips_delete_eliminating_replace_compaction -- --exact
cargo test --lib connector::iceberg::changes::tests::classify_lineage_rejects_replace_that_changes_total_records -- --exact
```

Expected: the second test passes. The first test either passes immediately or fails because the validator treats delete-file count changes too strictly; Step 3 defines the exact invariant set that makes the first test pass.

- [ ] **Step 3: Keep replace validator scoped to logical-data invariants**

Ensure `validate_replace_snapshot` only rejects:

```rust
// Required invariants:
// - total-records exists on parent and replace, and is unchanged
// - added-data-files > 0
// - deleted-data-files > 0
// - schema id unchanged
```

Do not require `added-delete-files > 0`; delete-eliminating compaction must allow `added-delete-files = 0` and `removed-delete-files > 0`.

- [ ] **Step 4: Run change-planning tests**

Run:

```bash
cargo test --lib connector::iceberg::changes -- --nocapture
```

Expected: all `changes` tests pass.

- [ ] **Step 5: Commit Task 3**

Run:

```bash
git add src/connector/iceberg/changes.rs
git commit -m "test: cover iceberg replace compaction classification"
```

## Task 4: Implement RewriteDataFilesCommit

**Files:**
- Create: `src/connector/iceberg/commit/rewrite_data_files.rs`
- Modify: `src/connector/iceberg/commit/mod.rs`
- Modify: `src/connector/iceberg/commit/run.rs`
- Modify: `src/connector/iceberg/commit/types.rs`

- [ ] **Step 1: Add failing commit dispatch test**

In `src/connector/iceberg/commit/types.rs`, extend the existing `op_kind_variants_are_distinct` test:

```rust
assert_ne!(CommitOpKind::RewriteDataFiles, CommitOpKind::FastAppend);
assert_ne!(CommitOpKind::RewriteDataFiles, CommitOpKind::Overwrite);
assert_ne!(CommitOpKind::RewriteDataFiles, CommitOpKind::RowDelta);
assert_ne!(CommitOpKind::RewriteDataFiles, CommitOpKind::RowDeltaDv);
```

Run:

```bash
cargo test --lib connector::iceberg::commit::types::tests::op_kind_variants_are_distinct -- --exact
```

Expected: compile fails because `RewriteDataFiles` does not exist.

- [ ] **Step 2: Add commit op variant and dispatch**

In `src/connector/iceberg/commit/types.rs`:

```rust
pub enum CommitOpKind {
    FastAppend,
    Overwrite,
    RowDelta,
    RowDeltaDv,
    RewriteDataFiles,
}
```

In `src/connector/iceberg/commit/mod.rs`:

```rust
mod rewrite_data_files;
pub use rewrite_data_files::RewriteDataFilesCommit;
```

In `src/connector/iceberg/commit/run.rs`:

```rust
use super::rewrite_data_files::RewriteDataFilesCommit;

// match arm
CommitOpKind::RewriteDataFiles => Box::new(RewriteDataFilesCommit),
```

- [ ] **Step 3: Create rewrite commit module with skeleton**

Create `src/connector/iceberg/commit/rewrite_data_files.rs`:

```rust
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0.

//! Iceberg data-file compaction commit action.
//!
//! This action commits an Iceberg `replace` snapshot that retires the current
//! live data/delete files and adds freshly compacted data files. Unlike
//! `OverwriteCommit`, it is only valid after the executor has proven the
//! rewritten rows are logically identical to the current visible table state.

use async_trait::async_trait;

use super::action::{CommitCtx, IcebergCommitAction};
use super::types::CommitOutcome;

pub struct RewriteDataFilesCommit;

#[async_trait]
impl IcebergCommitAction for RewriteDataFilesCommit {
    async fn commit(&self, ctx: CommitCtx<'_>) -> Result<CommitOutcome, String> {
        let written = ctx.collector.take_written_files()?;
        if written.is_empty() {
            let id = ctx
                .table
                .metadata()
                .current_snapshot()
                .map(|s| s.snapshot_id())
                .unwrap_or(0);
            return Ok(CommitOutcome {
                new_snapshot_id: id,
                written_manifest_paths: vec![],
            });
        }
        Err(format!(
            "RewriteDataFilesCommit is not implemented for {} written files",
            written.len()
        ))
    }
}
```

Run the op-kind test again. Expected: test passes; later executor tests still fail until the action is implemented.

- [ ] **Step 4: Add unit tests for summary construction**

In `rewrite_data_files.rs`, add a small pure helper first:

```rust
fn rewrite_summary(
    added_records: u64,
    added_data_files: usize,
    deleted_records: u64,
    deleted_data_files: usize,
    deleted_delete_files: usize,
) -> std::collections::HashMap<String, String> {
    let mut p = std::collections::HashMap::new();
    p.insert("added-records".to_string(), added_records.to_string());
    p.insert("added-data-files".to_string(), added_data_files.to_string());
    p.insert("deleted-records".to_string(), deleted_records.to_string());
    p.insert("deleted-data-files".to_string(), deleted_data_files.to_string());
    p.insert(
        "removed-delete-files".to_string(),
        deleted_delete_files.to_string(),
    );
    let total_records = added_records;
    p.insert("total-records".to_string(), total_records.to_string());
    p
}
```

Add test:

```rust
#[test]
fn rewrite_summary_reports_replace_compaction_counts() {
    let summary = rewrite_summary(7, 1, 7, 3, 2);
    assert_eq!(summary.get("total-records").unwrap(), "7");
    assert_eq!(summary.get("added-data-files").unwrap(), "1");
    assert_eq!(summary.get("deleted-data-files").unwrap(), "3");
    assert_eq!(summary.get("removed-delete-files").unwrap(), "2");
}
```

Run:

```bash
cargo test --lib connector::iceberg::commit::rewrite_data_files::tests::rewrite_summary_reports_replace_compaction_counts -- --exact
```

Expected: pass.

- [ ] **Step 5: Implement live file enumeration helpers**

In `rewrite_data_files.rs`, add:

```rust
use iceberg::io::FileIO;
use iceberg::spec::{
    DataContentType, DataFile, ManifestContentType, ManifestFile, ManifestStatus,
};
use iceberg::table::Table;

#[derive(Clone, Debug)]
struct LiveManifestEntry {
    data_file: DataFile,
    sequence_number: i64,
    file_sequence_number: Option<i64>,
}

#[derive(Clone, Debug, Default)]
struct LiveFiles {
    data_files: Vec<LiveManifestEntry>,
    delete_files: Vec<LiveManifestEntry>,
}

async fn enumerate_live_files(table: &Table, file_io: &FileIO) -> Result<LiveFiles, String> {
    let metadata = table.metadata();
    let snapshot = match metadata.current_snapshot() {
        Some(snapshot) => snapshot,
        None => return Ok(LiveFiles::default()),
    };
    let manifest_list = snapshot
        .load_manifest_list(file_io, metadata)
        .await
        .map_err(|e| format!("load current manifest list failed: {e}"))?;
    let mut out = LiveFiles::default();
    for manifest_file in manifest_list.entries() {
        let manifest = manifest_file
            .load_manifest(file_io)
            .await
            .map_err(|e| format!("load manifest {} failed: {e}", manifest_file.manifest_path))?;
        for entry in manifest.entries() {
            if entry.status == ManifestStatus::Deleted || !entry.is_alive() {
                continue;
            }
            let live = LiveManifestEntry {
                data_file: entry.data_file().clone(),
                sequence_number: entry.sequence_number().unwrap_or(manifest_file.sequence_number),
                file_sequence_number: entry.file_sequence_number,
            };
            match manifest_file.content {
                ManifestContentType::Data => {
                    if live.data_file.content_type() == DataContentType::Data {
                        out.data_files.push(live);
                    }
                }
                ManifestContentType::Deletes => out.delete_files.push(live),
            }
        }
    }
    Ok(out)
}
```

- [ ] **Step 6: Implement manifest writers**

Add functions modeled on `overwrite.rs::write_overwrite_deletes_manifest` and `row_delta.rs::write_delete_manifest`:

```rust
async fn write_deleted_manifest(
    file_io: &FileIO,
    out_path: &str,
    entries: &[LiveManifestEntry],
    manifest_content: ManifestContentType,
    partition_spec: iceberg::spec::PartitionSpecRef,
    schema: iceberg::spec::SchemaRef,
    new_snapshot_id: i64,
    format_version: iceberg::spec::FormatVersion,
) -> Result<ManifestFile, String> {
    let output_file = file_io
        .new_output(out_path)
        .map_err(|e| format!("FileIO::new_output({out_path}) failed: {e}"))?;
    let builder = iceberg::spec::ManifestWriterBuilder::new(
        output_file,
        Some(new_snapshot_id),
        None,
        schema,
        (*partition_spec).clone(),
    );
    let mut writer = match (format_version, manifest_content) {
        (iceberg::spec::FormatVersion::V2, ManifestContentType::Data) => builder.build_v2_data(),
        (iceberg::spec::FormatVersion::V3, ManifestContentType::Data) => builder.build_v3_data(),
        (iceberg::spec::FormatVersion::V2, ManifestContentType::Deletes) => {
            builder.build_v2_deletes()
        }
        (iceberg::spec::FormatVersion::V3, ManifestContentType::Deletes) => {
            builder.build_v3_deletes()
        }
        (iceberg::spec::FormatVersion::V1, _) => {
            return Err("RewriteDataFilesCommit does not support V1 tables".to_string());
        }
    };
    for entry in entries {
        writer
            .add_delete_file(
                entry.data_file.clone(),
                entry.sequence_number,
                entry.file_sequence_number,
            )
            .map_err(|e| format!("ManifestWriter::add_delete_file failed: {e}"))?;
    }
    writer
        .write_manifest_file()
        .await
        .map_err(|e| format!("ManifestWriter::write_manifest_file failed: {e}"))
}
```

Use existing `overwrite::write_added_data_manifest` for added data files.

- [ ] **Step 7: Implement transaction action**

Follow `OverwriteTxnAction` and `FastAppendV3TxnAction` patterns:

- Create `RewriteDataFilesTxnAction`.
- In `commit`, compute `new_seq = metadata.last_sequence_number() + 1`.
- Generate `new_snapshot_id`.
- Enumerate live files.
- Write deleted-data manifest when `live.data_files` is non-empty.
- Write deleted-delete manifest when `live.delete_files` is non-empty.
- Write added-data manifest when `written` is non-empty.
- Write manifest list with `first_row_id = Some(table.metadata().next_row_id())` only for `IcebergWriteMode::RowLineageV3`.
- Build `Summary { operation: Operation::Replace, additional_properties: rewrite_summary(...) }`.
- Build snapshot with `with_row_range(first_row_id, added_rows)` for row-lineage tables.
- Return `ActionCommit` with:

```rust
vec![
    TableUpdate::AddSnapshot { snapshot },
    TableUpdate::SetSnapshotRef {
        ref_name: MAIN_BRANCH.to_string(),
        reference: SnapshotReference {
            snapshot_id: new_snapshot_id,
            retention: SnapshotRetention::Branch {
                min_snapshots_to_keep: None,
                max_snapshot_age_ms: None,
                max_ref_age_ms: None,
            },
        },
    },
]
```

and requirements:

```rust
vec![
    TableRequirement::CurrentSchemaIdMatch {
        current_schema_id: metadata.current_schema_id(),
    },
    TableRequirement::DefaultSpecIdMatch {
        default_spec_id: metadata.default_partition_spec_id(),
    },
    TableRequirement::RefSnapshotIdMatch {
        r#ref: MAIN_BRANCH.to_string(),
        snapshot_id: metadata.current_snapshot().map(|s| s.snapshot_id()),
    },
]
```

- [ ] **Step 8: Run commit module tests**

Run:

```bash
cargo test --lib connector::iceberg::commit -- --nocapture
```

Expected: all commit tests pass.

- [ ] **Step 9: Commit Task 4**

Run:

```bash
git add src/connector/iceberg/commit
git commit -m "feat: add iceberg rewrite data files commit"
```

## Task 5: Add OPTIMIZE Worker and Whole-Table Executor

**Files:**
- Create: `src/connector/iceberg/compact.rs`
- Modify: `src/connector/iceberg/mod.rs`
- Modify: `src/connector/mod.rs`
- Modify: `src/engine/mod.rs`
- Modify: `src/engine/iceberg_writer.rs`

- [ ] **Step 1: Make SELECT-to-chunks reusable**

In `src/engine/iceberg_writer.rs`, change:

```rust
fn run_select_to_chunks(...)
```

to:

```rust
pub(crate) fn run_select_to_chunks(...)
```

Run:

```bash
cargo test --lib engine::iceberg_writer -- --nocapture
```

Expected: existing tests pass or no tests are found.

- [ ] **Step 2: Create compact module skeleton**

Create `src/connector/iceberg/compact.rs`:

```rust
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0.

use std::sync::Arc;
use std::time::Duration;

use iceberg::{NamespaceIdent, TableIdent};

use crate::connector::iceberg::catalog::registry::{block_on_iceberg, build_hadoop_catalog};
use crate::connector::iceberg::commit::{
    CommitOpKind, IcebergCommitCollector, RunInput, run_iceberg_commit,
};
use crate::connector::starrocks::managed::store::{
    IcebergOptimizeJobOutcome, SqliteMetadataStore, StoredIcebergOptimizeJob,
};
use crate::engine::StandaloneState;

pub(crate) fn spawn_optimize_worker(state: Arc<StandaloneState>) {
    let Some(store) = state.metadata_store.clone() else {
        return;
    };
    if let Err(err) = store.fail_running_iceberg_optimize_jobs_on_startup() {
        tracing::warn!(error = %err, "failed to mark stale iceberg optimize jobs failed");
    }
    std::thread::Builder::new()
        .name("iceberg-optimize-worker".to_string())
        .spawn(move || loop {
            if let Err(err) = run_optimize_jobs_once(&state, &store) {
                tracing::warn!(error = %err, "iceberg optimize worker iteration failed");
            }
            std::thread::sleep(Duration::from_millis(500));
        })
        .expect("spawn iceberg optimize worker");
}

pub(crate) fn run_optimize_jobs_once(
    state: &Arc<StandaloneState>,
    store: &SqliteMetadataStore,
) -> Result<(), String> {
    for job in store.list_pending_iceberg_optimize_jobs()? {
        if !store.claim_iceberg_optimize_job(job.job_id)? {
            continue;
        }
        match run_one_optimize_job(state, &job) {
            Ok(outcome) => store.finish_iceberg_optimize_job(job.job_id, outcome)?,
            Err(err) => store.fail_iceberg_optimize_job(job.job_id, &err)?,
        }
    }
    Ok(())
}

fn run_one_optimize_job(
    state: &Arc<StandaloneState>,
    job: &StoredIcebergOptimizeJob,
) -> Result<IcebergOptimizeJobOutcome, String> {
    execute_whole_table_rewrite(state, job)
}

fn execute_whole_table_rewrite(
    _state: &Arc<StandaloneState>,
    job: &StoredIcebergOptimizeJob,
) -> Result<IcebergOptimizeJobOutcome, String> {
    Err(format!(
        "iceberg OPTIMIZE executor is not implemented for {}.{}.{}",
        job.catalog, job.namespace, job.table
    ))
}
```

In `src/connector/iceberg/mod.rs`, add:

```rust
pub(crate) mod compact;
```

Run:

```bash
cargo check --lib
```

Expected: compile succeeds after removing unused imports from the skeleton.

- [ ] **Step 3: Spawn worker from engine open**

In `src/connector/mod.rs`, add:

```rust
pub(crate) use iceberg::compact::spawn_optimize_worker as spawn_iceberg_optimize_worker;
```

In `StandaloneNovaRocks::open` after metadata restore:

```rust
if inner.metadata_store.is_some() {
    crate::connector::spawn_iceberg_optimize_worker(Arc::clone(&inner));
}
```

Run:

```bash
cargo check --lib
```

Expected: compile succeeds.

- [ ] **Step 4: Implement table load and base snapshot validation**

In `execute_whole_table_rewrite`, implement:

```rust
let entry = {
    let registry = state
        .iceberg_catalogs
        .read()
        .map_err(|e| format!("iceberg catalog registry read lock: {e}"))?;
    registry.get(&job.catalog)?
};
let hadoop_catalog = build_hadoop_catalog(&entry)?;
let catalog: Arc<dyn iceberg::Catalog> = Arc::new(hadoop_catalog);
let table_ident = TableIdent::new(
    NamespaceIdent::new(job.namespace.clone()),
    job.table.clone(),
);
let table = block_on_iceberg(async { catalog.load_table(&table_ident).await })?
    .map_err(|e| format!("load iceberg table {}.{}.{} for OPTIMIZE: {e}", job.catalog, job.namespace, job.table))?;
let current_snapshot_id = table.metadata().current_snapshot().map(|s| s.snapshot_id());
if current_snapshot_id != job.base_snapshot_id {
    return Err(format!(
        "iceberg OPTIMIZE snapshot changed before rewrite: job base={:?}, current={:?}",
        job.base_snapshot_id, current_snapshot_id
    ));
}
if current_snapshot_id.is_none() {
    return Ok(IcebergOptimizeJobOutcome {
        target_snapshot_id: None,
        input_data_files: 0,
        output_data_files: 0,
        input_delete_files: 0,
        output_delete_files: 0,
        message: "empty iceberg table has no snapshot; optimize no-op".to_string(),
    });
}
```

- [ ] **Step 5: Implement visible-row SELECT**

Build a query:

```rust
let select_sql = format!(
    "SELECT * FROM {}.{}.{}",
    quote_ident(&job.catalog),
    quote_ident(&job.namespace),
    quote_ident(&job.table)
);
let query = match crate::sql::parser::parse_normalized_sql_raw(&select_sql)
    .map_err(|e| format!("parse optimize SELECT: {e}"))?
{
    sqlparser::ast::Statement::Query(query) => query,
    other => return Err(format!("optimize SELECT parsed as non-query: {other}")),
};
let target = crate::engine::backend_resolver::TargetBackend {
    backend_name: "iceberg".to_string(),
    catalog: job.catalog.clone(),
    namespace: job.namespace.clone(),
    table: job.table.clone(),
};
let chunks = crate::engine::iceberg_writer::run_select_to_chunks(
    state,
    &target,
    query.as_ref(),
)?;
let visible_rows = chunks.iter().map(|chunk| chunk.batch.num_rows() as u64).sum::<u64>();
```

Add helper:

```rust
fn quote_ident(value: &str) -> String {
    format!("`{}`", value.replace('`', "``"))
}
```

- [ ] **Step 6: Write compacted data files and validate row count**

Add:

```rust
let data_files = if visible_rows == 0 {
    Vec::new()
} else {
    block_on_iceberg(async {
        crate::connector::starrocks::managed::mv_refresh_iceberg::write_chunks_as_iceberg_data_files(
            &table,
            &chunks,
        )
        .await
    })??
};
let output_rows = data_files.iter().map(|f| f.record_count()).sum::<u64>();
if output_rows != visible_rows {
    return Err(format!(
        "iceberg OPTIMIZE row-count mismatch: visible_rows={visible_rows}, output_rows={output_rows}"
    ));
}
```

- [ ] **Step 7: Add live input file counters**

Expose a pure helper from `rewrite_data_files.rs`:

```rust
pub(crate) async fn count_current_live_files(
    table: &iceberg::table::Table,
    file_io: &iceberg::io::FileIO,
) -> Result<(i64, i64), String> {
    let live = enumerate_live_files(table, file_io).await?;
    Ok((live.data_files.len() as i64, live.delete_files.len() as i64))
}
```

Use it in `execute_whole_table_rewrite` after the second table reload and before building the collector:

```rust
let (input_data_files, input_delete_files) =
    block_on_iceberg(async { count_current_live_files(&table, table.file_io()).await })??;
```

- [ ] **Step 8: Commit rewrite action**

Before commit, reload and validate snapshot again:

```rust
let table = block_on_iceberg(async { catalog.load_table(&table_ident).await })?
    .map_err(|e| format!("reload iceberg table before OPTIMIZE commit: {e}"))?;
let current_snapshot_id = table.metadata().current_snapshot().map(|s| s.snapshot_id());
if current_snapshot_id != job.base_snapshot_id {
    return Err(format!(
        "iceberg OPTIMIZE snapshot changed before commit: job base={:?}, current={:?}",
        job.base_snapshot_id, current_snapshot_id
    ));
}
```

Build collector:

```rust
let metadata = table.metadata();
let staging_dir = format!(
    "{}/data/_staging/{}",
    metadata.location(),
    uuid::Uuid::new_v4()
);
let collector = Arc::new(IcebergCommitCollector::new(
    CommitOpKind::RewriteDataFiles,
    table_ident,
    metadata.current_snapshot().map(|s| s.snapshot_id()),
    metadata.last_sequence_number(),
    metadata.current_schema().clone(),
    metadata.default_partition_spec().clone(),
    staging_dir,
    crate::common::types::UniqueId { hi: 0, lo: 0 },
));
let default_spec_id = metadata.default_partition_spec_id();
for df in &data_files {
    collector.inject_written_file(crate::engine::iceberg_writer::data_file_to_written_file(
        df,
        default_spec_id,
    )?);
}
let abort_cleanup = crate::engine::iceberg_writer::build_abort_cleanup_for_catalog_entry(&entry)?;
let file_io = table.file_io().clone();
let outcome = block_on_iceberg(async {
    run_iceberg_commit(RunInput {
        collector: collector.clone(),
        catalog: catalog.clone(),
        table,
        fs: abort_cleanup.fs,
        file_io,
        cleanup_path_mapper: abort_cleanup.path_mapper,
    })
    .await
})??;
```

Invalidate caches:

```rust
crate::engine::iceberg_writer::invalidate_iceberg_caches(state, &target)?;
```

Return:

```rust
Ok(IcebergOptimizeJobOutcome {
    target_snapshot_id: Some(outcome.new_snapshot_id),
    input_data_files,
    output_data_files: data_files.len() as i64,
    input_delete_files,
    output_delete_files: 0,
    message: format!("iceberg OPTIMIZE finished at snapshot {}", outcome.new_snapshot_id),
})
```

- [ ] **Step 9: Run focused compile and worker tests**

Run:

```bash
cargo check --lib
cargo test --lib connector::iceberg::compact -- --nocapture
```

Expected: both commands exit 0.

- [ ] **Step 10: Commit Task 5**

Run:

```bash
git add src/connector/iceberg/compact.rs src/connector/iceberg/mod.rs src/connector/mod.rs src/engine/mod.rs src/engine/iceberg_writer.rs src/connector/iceberg/commit/rewrite_data_files.rs
git commit -m "feat: run iceberg optimize rewrite jobs"
```

## Task 6: Add End-to-End SQL Coverage

**Files:**
- Create: `sql-tests/iceberg/sql/iceberg_v3_optimize_compact_data_files.sql`
- Test: `sql-tests/iceberg/sql/iceberg_v3_optimize_compact_data_files.sql`

- [ ] **Step 1: Add SQL test case**

Create `sql-tests/iceberg/sql/iceberg_v3_optimize_compact_data_files.sql`:

```sql
-- name: iceberg_v3_optimize_compact_data_files
-- Test Point: Iceberg v3 OPTIMIZE rewrites visible rows into compacted data files and removes live delete files.
-- Method: create a v3 row-lineage table, write multiple files, delete rows to create DV state, run ALTER TABLE OPTIMIZE, then verify SELECT and SHOW output.
-- Scope: standalone Iceberg v3 DDL, INSERT, DELETE, OPTIMIZE, SHOW ALTER TABLE OPTIMIZE

CREATE CATALOG opt_ice_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "hadoop",
  "warehouse" = "${managed_lake_warehouse}/opt_ice_${uuid0}"
);

SET catalog opt_ice_${uuid0};
CREATE DATABASE ns_${uuid0};
USE ns_${uuid0};

CREATE TABLE orders (
  id INT,
  user_id INT,
  amount INT
)
PROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true"
);

INSERT INTO orders VALUES (1, 10, 100);
INSERT INTO orders VALUES (2, 20, 200);
INSERT INTO orders VALUES (3, 30, 300);
INSERT INTO orders VALUES (4, 40, 400);

DELETE FROM orders WHERE id IN (2, 4);

SELECT id, user_id, amount FROM orders ORDER BY id;

-- @wait_alter_optimize=orders
ALTER TABLE orders OPTIMIZE;

-- @result_contains=FINISHED
SHOW ALTER TABLE OPTIMIZE FROM ns_${uuid0}
WHERE TableName = 'orders'
ORDER BY CreateTime DESC
LIMIT 1;

SELECT id, user_id, amount FROM orders ORDER BY id;
```

Expected result for both SELECT statements:

```text
1	10	100
3	30	300
```

Record mode will create the exact `R/` result file.

- [ ] **Step 2: Run SQL test in record mode**

Start a private standalone server on a non-reserved port with the managed-lake config used by the Iceberg suite. Do not use port `9030`.

Run:

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --suite iceberg \
  --only iceberg_v3_optimize_compact_data_files \
  --config tests/sql-test-runner/conf/standalone_managed_lake.conf \
  --port 19045 \
  --mode record \
  --query-timeout 120
```

Expected: `total=1 pass=1 fail=0`, and a result file is recorded under the Iceberg result directory.

- [ ] **Step 3: Run SQL test in verify mode**

Run:

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --suite iceberg \
  --only iceberg_v3_optimize_compact_data_files \
  --config tests/sql-test-runner/conf/standalone_managed_lake.conf \
  --port 19045 \
  --mode verify \
  --query-timeout 120
```

Expected: `total=1 pass=1 fail=0`.

- [ ] **Step 4: Run full focused Rust validation**

Run:

```bash
cargo test --lib connector::starrocks::managed::store::tests::iceberg_optimize_job_lifecycle_round_trips -- --exact
cargo test --lib engine::statement::tests::parse_alter_table_optimize_accepts_three_part_table -- --exact
cargo test --lib connector::iceberg::changes -- --nocapture
cargo test --lib connector::iceberg::commit -- --nocapture
cargo check --lib
cargo fmt --check
git diff --check
```

Expected: all commands exit 0.

- [ ] **Step 5: Commit Task 6**

Run:

```bash
git add sql-tests/iceberg/sql/iceberg_v3_optimize_compact_data_files.sql
git add sql-tests/iceberg/R
git commit -m "test: cover iceberg v3 optimize compaction"
```

## Final Verification

- [ ] **Step 1: Run focused SQL verification**

Run:

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --suite iceberg \
  --only iceberg_v3_optimize_compact_data_files \
  --config tests/sql-test-runner/conf/standalone_managed_lake.conf \
  --port 19045 \
  --mode verify \
  --query-timeout 120
```

Expected: `total=1 pass=1 fail=0`.

- [ ] **Step 2: Run focused Rust verification**

Run:

```bash
cargo test --lib connector::starrocks::managed::store::tests::iceberg_optimize_job_lifecycle_round_trips -- --exact
cargo test --lib connector::starrocks::managed::store::tests::iceberg_optimize_rejects_active_job_for_same_table -- --exact
cargo test --lib connector::starrocks::managed::store::tests::iceberg_optimize_startup_marks_running_jobs_failed -- --exact
cargo test --lib engine::statement::tests::parse_alter_table_optimize_accepts_three_part_table -- --exact
cargo test --lib engine::statement::tests::parse_show_alter_table_optimize_extracts_filter -- --exact
cargo test --lib connector::iceberg::changes -- --nocapture
cargo test --lib connector::iceberg::commit -- --nocapture
cargo check --lib
cargo fmt --check
git diff --check
```

Expected: all commands exit 0.

- [ ] **Step 3: Inspect final diff**

Run:

```bash
git status --short
git log --oneline --decorate -6
```

Expected: the worktree is clean after the task commits, and recent commits are the six task commits from this plan.
