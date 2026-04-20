# FE-Facing Thrift Compatibility Audit Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Restore NovaRocks FE-facing thrift compatibility with the current StarRocks FE for runtime-critical paths, starting with `information_schema.task_runs`, while producing a scoped audit of additional drift in `FrontendService.thrift` and its directly referenced types.

**Architecture:** Diff NovaRocks thrift IDLs against the current StarRocks sources, classify each difference by runtime risk, then patch the minimum FE-facing thrift surface needed to eliminate deserialization and field-misalignment bugs. Keep the implementation centered on `FrontendService.thrift`, with narrowly-scoped follow-up changes in dependent IDLs and schema-row builders when a new field is already consumed by runtime code.

**Tech Stack:** Rust, C++ shim, Thrift IDL, Cargo test/build, NovaRocks runtime, StarRocks FE SQL regression tests

---

### Task 1: Produce Drift Inventory

**Files:**
- Modify: `/Users/harbor/worktree/NovaRocks/main/docs/superpowers/plans/2026-04-18-fe-facing-thrift-compat-audit.md`
- Inspect: `/Users/harbor/worktree/NovaRocks/main/idl/thrift/FrontendService.thrift`
- Inspect: `/Users/harbor/worktree/NovaRocks/main/idl/thrift/Types.thrift`
- Inspect: `/Users/harbor/worktree/NovaRocks/main/idl/thrift/InternalService.thrift`
- Inspect: `/Users/harbor/.codex/worktrees/68b9/starrocks/gensrc/thrift/FrontendService.thrift`
- Inspect: `/Users/harbor/.codex/worktrees/68b9/starrocks/gensrc/thrift/Types.thrift`
- Inspect: `/Users/harbor/.codex/worktrees/68b9/starrocks/gensrc/thrift/InternalService.thrift`

- [ ] **Step 1: Diff the FE-facing thrift files**

Run:

```bash
diff -u /Users/harbor/worktree/NovaRocks/main/idl/thrift/FrontendService.thrift /Users/harbor/.codex/worktrees/68b9/starrocks/gensrc/thrift/FrontendService.thrift
diff -u /Users/harbor/worktree/NovaRocks/main/idl/thrift/Types.thrift /Users/harbor/.codex/worktrees/68b9/starrocks/gensrc/thrift/Types.thrift
diff -u /Users/harbor/worktree/NovaRocks/main/idl/thrift/InternalService.thrift /Users/harbor/.codex/worktrees/68b9/starrocks/gensrc/thrift/InternalService.thrift
```

Expected: visible drift including `TTaskRunInfo.warehouse`, `TGetLoadsParams` filters, request/response additions, and several enum/field additions in dependent files.

- [ ] **Step 2: Classify each diff by risk**

Add a short section to this plan file listing:

```markdown
## Drift Classification

### Runtime-breaking now
- `FrontendService.TTaskRunInfo`: StarRocks FE inserts `warehouse` at field 15, which shifts `job_id` and `process_time` and breaks `getTaskRuns` deserialization.

### Runtime-breaking soon
- `FrontendService.TGetLoadsParams`: extra optional filters can break callers if NovaRocks later forwards or consumes newer FE requests.

### Compile-only / dormant
- `FrontendService.TAuditStatistics` additions
- `InternalService` session variable / enum additions not yet consumed by current failing path
- `Types` enum additions not used in the current FE-facing regression
```

- [ ] **Step 3: Re-read the classification and remove speculative items**

Keep only items backed by a concrete diff or runtime symptom. Do not list guesses without evidence.

### Task 2: Patch Runtime-Critical Drift

**Files:**
- Modify: `/Users/harbor/worktree/NovaRocks/main/idl/thrift/FrontendService.thrift`
- Modify: `/Users/harbor/worktree/NovaRocks/main/src/connector/schema/fe_tables.rs`
- Test: `/Users/harbor/worktree/NovaRocks/main/src/connector/schema/fe_tables.rs`

- [ ] **Step 1: Align `TTaskRunInfo` field numbering with StarRocks**

Update the thrift struct to match the current FE layout:

```thrift
struct TTaskRunInfo {
    1: optional string query_id
    2: optional string task_name
    3: optional i64 create_time
    4: optional i64 finish_time
    5: optional string state
    6: optional string database
    7: optional string definition
    8: optional i64 expire_time
    9: optional i32 error_code
    10: optional string error_message
    11: optional string progress
    12: optional string extra_message
    13: optional string properties
    14: optional string catalog
    15: optional string warehouse
    16: optional string job_id
    17: optional i64 process_time
}
```

- [ ] **Step 2: Teach schema-row conversion to surface `WAREHOUSE`**

Update `build_task_run_row` so it preserves the newly aligned field:

```rust
fn build_task_run_row(info: &frontend_service::TTaskRunInfo) -> SchemaRow {
    let mut row = SchemaRow::new();
    put_opt_str(&mut row, "QUERY_ID", info.query_id.as_deref());
    put_opt_str(&mut row, "TASK_NAME", info.task_name.as_deref());
    put_ts_seconds_positive(&mut row, "CREATE_TIME", info.create_time);
    put_ts_seconds_positive(&mut row, "FINISH_TIME", info.finish_time);
    put_opt_str(&mut row, "STATE", info.state.as_deref());
    put_opt_str(&mut row, "CATALOG", info.catalog.as_deref());
    put_opt_str(&mut row, "DATABASE", info.database.as_deref());
    put_opt_str(&mut row, "DEFINITION", info.definition.as_deref());
    put_ts_seconds_positive(&mut row, "EXPIRE_TIME", info.expire_time);
    put_i32(&mut row, "ERROR_CODE", info.error_code);
    put_opt_str(&mut row, "ERROR_MESSAGE", info.error_message.as_deref());
    put_opt_str(&mut row, "PROGRESS", info.progress.as_deref());
    put_opt_str(&mut row, "EXTRA_MESSAGE", info.extra_message.as_deref());
    put_opt_str(&mut row, "PROPERTIES", info.properties.as_deref());
    put_opt_str(&mut row, "WAREHOUSE", info.warehouse.as_deref());
    put_i64_from_string(&mut row, "JOB_ID", info.job_id.as_deref());
    put_ts_seconds_positive(&mut row, "PROCESS_TIME", info.process_time);
    row
}
```

- [ ] **Step 3: Update the existing unit test to prove layout alignment**

Extend `build_task_run_row_matches_starrocks_task_runs_layout` with the new field:

```rust
let info = frontend_service::TTaskRunInfo {
    query_id: Some("query-1".to_string()),
    task_name: Some("task-1".to_string()),
    create_time: Some(1_700_000_000),
    finish_time: Some(1_700_000_100),
    state: Some("SUCCESS".to_string()),
    database: Some("db1".to_string()),
    definition: Some("submit task".to_string()),
    expire_time: Some(1_700_000_200),
    error_code: Some(0),
    error_message: Some(String::new()),
    progress: Some("100%".to_string()),
    extra_message: Some("done".to_string()),
    properties: Some("{\"k\":\"v\"}".to_string()),
    catalog: Some("default_catalog".to_string()),
    warehouse: Some("default_warehouse".to_string()),
    job_id: Some("42".to_string()),
    process_time: Some(1_700_000_300),
};

assert_eq!(
    row.get(&normalize_column_key("WAREHOUSE")),
    Some(&SchemaValue::Utf8("default_warehouse".to_string()))
);
assert_eq!(
    row.get(&normalize_column_key("JOB_ID")),
    Some(&SchemaValue::Int64(42))
);
```

- [ ] **Step 4: Add the remaining low-risk `FrontendService.thrift` field additions that do not require code changes**

Copy over the new optional fields and request/response structs that are protocol-safe and self-contained:
- `TGetLoadsParams` extra filters
- `TAuthenticateParams.is_arrow_flight_sql` and `user_groups`
- `TStatus` additions like `sql_digest` and `maxStarMgrJournalId`
- `TRLTaskTxnCommitAttachment.nonRetryable`
- `TCreatePartitionRequest.timeout_s`
- `notifyForwardDeploymentFinished` request/response and service method

Do not widen scope into unrelated logic changes; only add the thrift surface so generated bindings stay compatible.

### Task 3: Patch Dependent IDLs Required by the Frontend Drift

**Files:**
- Modify: `/Users/harbor/worktree/NovaRocks/main/idl/thrift/Types.thrift`
- Modify: `/Users/harbor/worktree/NovaRocks/main/idl/thrift/InternalService.thrift`
- Modify: `/Users/harbor/worktree/NovaRocks/main/idl/thrift/Descriptors.thrift` (only if compile forces it)
- Modify: `/Users/harbor/worktree/NovaRocks/main/idl/thrift/PlanNodes.thrift` (only if compile forces it)

- [ ] **Step 1: Add only the dependent definitions referenced by the Frontend patch**

Carry over the minimum definitions required for the updated `FrontendService.thrift` to compile:

```thrift
// Types.thrift
include "CloudConfiguration.thrift"

// InternalService.thrift
enum TArrowFlightSQLVersion {
  V0 = 0,
  V1 = 1,
}
```

If the updated `FrontendService.thrift` references other missing symbols during code generation, add only those exact symbols next.

- [ ] **Step 2: Re-run code generation / build once to discover mandatory follow-up symbols**

Run:

```bash
cd /Users/harbor/worktree/NovaRocks/main
cargo test build_task_run_row_matches_starrocks_task_runs_layout --lib -- --nocapture
```

Expected: either the targeted test compiles and runs, or the compiler points to the next missing thrift symbol. Patch only the missing symbol set and repeat.

### Task 4: Regression Verification

**Files:**
- Test: `/Users/harbor/worktree/NovaRocks/main/src/connector/schema/fe_tables.rs`
- Test: `/Users/harbor/worktree/NovaRocks/main/sql-tests/materialized-view/sql/test_materialized_view_with_sync_mode.sql`
- Test: `/Users/harbor/.codex/worktrees/68b9/starrocks/test/sql/test_mv_on_iceberg/R/test_create_mv_with_params_iceberg`
- Test: `/Users/harbor/.codex/worktrees/68b9/starrocks/test/sql/test_mv_on_iceberg/R/test_mv_iceberg_rewrite`

- [ ] **Step 1: Run the targeted Rust unit test**

Run:

```bash
cd /Users/harbor/worktree/NovaRocks/main
cargo test build_task_run_row_matches_starrocks_task_runs_layout --lib -- --nocapture
```

Expected: PASS

- [ ] **Step 2: Build and package NovaRocks with compat**

Run:

```bash
cd /Users/harbor/worktree/NovaRocks/main
./build.sh --release --package --output /Users/harbor/starrocks-on-novarocks/novarocks --features compat
```

Expected: package succeeds and refreshes the deployed runtime

- [ ] **Step 3: Restart the NovaRocks BE runtime and validate `information_schema.task_runs` manually**

Run:

```bash
cd /Users/harbor/starrocks-on-novarocks/novarocks
./bin/novarocksctl restart --daemon
mysql -h 127.0.0.1 -P19030 -uroot -e "select state from information_schema.task_runs limit 5"
```

Expected: no `bad data` error; query returns rows or an empty successful result

- [ ] **Step 4: Re-run the StarRocks SQL cases that were blocked by `task_runs`**

Run:

```bash
cd /Users/harbor/.codex/worktrees/68b9/starrocks/test
.venv/bin/python run.py -v -c 1 --skip_reruns --config /tmp/starrocks-mv-on-iceberg.local.conf -d sql/test_mv_on_iceberg/R/test_create_mv_with_params_iceberg
.venv/bin/python run.py -v -c 1 --skip_reruns --config /tmp/starrocks-mv-on-iceberg.local.conf -d sql/test_mv_on_iceberg/R/test_mv_iceberg_rewrite
```

Expected: the previous `task_runs`-driven failures disappear; any remaining failures are from unrelated case expectations

### Task 5: Report Remaining Drift

**Files:**
- Modify: `/Users/harbor/worktree/NovaRocks/main/docs/superpowers/plans/2026-04-18-fe-facing-thrift-compat-audit.md`

- [ ] **Step 1: Add an audit residue section**

Record any remaining diffs not patched in this change:

```markdown
## Remaining Drift

- `Types.thrift`: enum additions not referenced by the updated FE-facing path
- `InternalService.thrift`: session variable additions not required by current FE queries
- `FrontendService.TGetTableSchemaRequest`: structural change deferred until a concrete runtime caller requires it
```

- [ ] **Step 2: Add verification evidence**

Append the exact commands and pass/fail outcomes you observed for:
- unit tests
- NovaRocks packaging
- manual `information_schema.task_runs` query
- StarRocks SQL regression reruns
