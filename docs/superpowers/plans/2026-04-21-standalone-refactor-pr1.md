# Standalone Module Refactor PR1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reorganize `src/standalone/` into a layered directory tree and split the 6830-line `engine.rs` god file into a focused `engine/` subtree. Public API unchanged. Zero behavior change.

**Architecture:** Leaf-first extraction: move types/functions from `engine.rs` into their target files one concern at a time, verify `cargo check` after each move. `StandaloneSession`'s giant `impl` block is split across multiple files using Rust's multi-`impl` support.

**Tech Stack:** Rust, Cargo. Test suites: `cargo test`, `sql-tests --suite ssb|tpc-h|tpc-ds`.

**Spec:** [docs/superpowers/specs/2026-04-21-standalone-refactor-design.md](../specs/2026-04-21-standalone-refactor-design.md)

**Conventions throughout this plan:**
- All line-number references are against `src/standalone/engine.rs` at commit `ed5ab23` (the spec commit).
- Every task ends with `cargo check` (incremental, ~5–20s) before commit; `cargo test` runs at milestones only.
- Use the EXACT commit message shown — they form a consistent refactor-log history.
- If `cargo check` fails, DO NOT move on. Fix the import or visibility issue before committing.

---

## Task 1: Create directory skeletons and placeholder mod.rs files

**Why:** We want every subsequent move to be a pure content transfer, not a "create a new module + move at the same time." Build the skeleton once, fill it in incrementally.

**Files:**
- Create: `src/standalone/engine/mod.rs`
- Create: `src/standalone/engine/sqlparse/mod.rs`
- Create: `src/standalone/engine/local/mod.rs`
- Create: `src/standalone/iceberg/mod.rs`
- Create: `src/standalone/lake/mod.rs`
- Create: `src/standalone/server/mod.rs`

- [ ] **Step 1: Create empty stub directories with empty `mod.rs`**

Each new `mod.rs` contains only a doc comment for now:

```rust
//! <subsystem> subdirectory. Files will be added incrementally during PR1.
```

Do NOT add these to `src/standalone/mod.rs` yet. The compiler will simply ignore unreferenced files.

- [ ] **Step 2: Run `cargo check`**

Run: `cargo check`
Expected: PASS, no changes in build output.

- [ ] **Step 3: Commit**

```bash
git add src/standalone/engine/mod.rs src/standalone/engine/sqlparse/mod.rs \
        src/standalone/engine/local/mod.rs src/standalone/iceberg/mod.rs \
        src/standalone/lake/mod.rs src/standalone/server/mod.rs
git commit -m "refactor(standalone): add empty subdirectory scaffolding"
```

---

## Task 2: Extract `QueryResult` / `QueryResultColumn` into `src/runtime/query_result.rs`

**Why:** Required for the coordinator move (Task 3) — we can't have `runtime/coordinator.rs` depending on `standalone::engine::QueryResult`, so the type goes to `runtime/` first.

**Files:**
- Create: `src/runtime/query_result.rs`
- Modify: `src/runtime/mod.rs`
- Modify: `src/standalone/engine.rs` (lines 52–177 region)
- Modify: `src/standalone/mod.rs`

- [ ] **Step 1: Create `src/runtime/query_result.rs`**

Copy the following from `src/standalone/engine.rs`:
- `pub struct QueryResultColumn` (line 52)
- `pub struct QueryResult` (line 60)
- `impl QueryResult` (line 115)
- `pub(crate) fn build_string_query_result` (line 125)
- `fn append_string_query_rows` (line 153)

Change `pub(crate) fn build_string_query_result` to `pub(crate) fn build_string_query_result` in new location (keep crate visibility). Change `fn append_string_query_rows` to `pub(crate) fn` so it can be called from `standalone::engine` if still needed — or keep private if only used inside `build_string_query_result`.

- [ ] **Step 2: Register the module in `src/runtime/mod.rs`**

Add `pub mod query_result;` in the appropriate alphabetical position among the existing module declarations.

- [ ] **Step 3: Delete the moved code from `src/standalone/engine.rs`**

Remove the 5 items copied in Step 1 from `engine.rs`. Add `use crate::runtime::query_result::{QueryResult, QueryResultColumn, build_string_query_result};` at the top of `engine.rs` (and `append_string_query_rows` if it was referenced from there).

- [ ] **Step 4: Update `src/standalone/mod.rs` re-exports**

Change the existing `pub use engine::{QueryResult, QueryResultColumn, ...};` so that `QueryResult` and `QueryResultColumn` come from the new location:

```rust
pub use crate::runtime::query_result::{QueryResult, QueryResultColumn};
pub use engine::{
    StandaloneManagedTableInfo, StandaloneManagedTabletInfo,
    StandaloneNovaRocks, StandaloneOptions, StandaloneSession,
};
```

Verify by `git diff src/standalone/mod.rs` that no other `pub use` symbol changes (the public surface must stay identical).

- [ ] **Step 5: Run `cargo check`**

Run: `cargo check`
Expected: PASS. If any other files still reference `crate::standalone::engine::QueryResult`, update them to `crate::runtime::query_result::QueryResult` or to the `standalone::` re-export path they were already using.

- [ ] **Step 6: Commit**

```bash
git add src/runtime/query_result.rs src/runtime/mod.rs \
        src/standalone/engine.rs src/standalone/mod.rs
git commit -m "refactor(runtime): move QueryResult out of standalone::engine"
```

---

## Task 3: Move `coordinator.rs` into `src/runtime/`

**Files:**
- Create: `src/runtime/coordinator.rs`
- Delete: `src/standalone/coordinator.rs`
- Modify: `src/runtime/mod.rs`
- Modify: `src/standalone/mod.rs`
- Modify: `src/standalone/engine.rs` (the caller of `ExecutionCoordinator::new`)

- [ ] **Step 1: Copy `src/standalone/coordinator.rs` to `src/runtime/coordinator.rs`**

```bash
git mv src/standalone/coordinator.rs src/runtime/coordinator.rs
```

- [ ] **Step 2: Fix imports inside the moved file**

Change `use super::engine::{QueryResult, QueryResultColumn};` to `use crate::runtime::query_result::{QueryResult, QueryResultColumn};`.

No other imports should change — the other `crate::*` paths are already absolute.

- [ ] **Step 3: Register in `src/runtime/mod.rs`, unregister in `src/standalone/mod.rs`**

In `src/runtime/mod.rs` add `pub(crate) mod coordinator;` (alphabetical).
In `src/standalone/mod.rs` delete `pub(crate) mod coordinator;` (it was not in the current mod.rs because `coordinator.rs` was referenced via `super::engine` — verify; if it wasn't declared, skip this half).

- [ ] **Step 4: Update the single caller in `engine.rs`**

Find `crate::standalone::coordinator::ExecutionCoordinator` or `super::coordinator::ExecutionCoordinator` in `src/standalone/engine.rs` and replace with `crate::runtime::coordinator::ExecutionCoordinator`. (The reference is in the query execution path; grep for `ExecutionCoordinator` to locate.)

- [ ] **Step 5: Run `cargo check`**

Run: `cargo check`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/runtime/coordinator.rs src/runtime/mod.rs \
        src/standalone/mod.rs src/standalone/engine.rs
git commit -m "refactor(runtime): move ExecutionCoordinator out of standalone"
```

---

## Task 4: Move `iceberg_*` / `hadoop_catalog.rs` into `src/standalone/iceberg/` subdirectory

**Why:** Pure relocation; no internal split yet. That split is PR2.

**Files:**
- Rename: `src/standalone/iceberg.rs` → `src/standalone/iceberg/registry.rs`
- Rename: `src/standalone/iceberg_s3_storage.rs` → `src/standalone/iceberg/s3_storage.rs`
- Rename: `src/standalone/iceberg_add_files.rs` → `src/standalone/iceberg/add_files.rs`
- Rename: `src/standalone/hadoop_catalog.rs` → `src/standalone/iceberg/hadoop_catalog.rs`
- Modify: `src/standalone/iceberg/mod.rs`
- Modify: `src/standalone/mod.rs`
- Modify: every file that `use`d `crate::standalone::iceberg` / `iceberg_s3_storage` / `iceberg_add_files` / `hadoop_catalog`

- [ ] **Step 1: `git mv` the four files**

```bash
git mv src/standalone/iceberg.rs             src/standalone/iceberg/registry.rs
git mv src/standalone/iceberg_s3_storage.rs  src/standalone/iceberg/s3_storage.rs
git mv src/standalone/iceberg_add_files.rs   src/standalone/iceberg/add_files.rs
git mv src/standalone/hadoop_catalog.rs      src/standalone/iceberg/hadoop_catalog.rs
```

- [ ] **Step 2: Fill in `src/standalone/iceberg/mod.rs`**

```rust
//! Iceberg catalog subsystem. All iceberg-related functionality.

pub(crate) mod add_files;
pub(crate) mod hadoop_catalog;
pub(crate) mod registry;
pub(crate) mod s3_storage;

// Re-export the public surface that was previously available as
// `crate::standalone::iceberg::*` so that importers keep working.
pub(crate) use registry::{
    IcebergCatalogRegistry, IcebergCatalogEntry, IcebergLoadedTable,
    DataFileWithStats,
    block_on_iceberg, build_hadoop_catalog,
    create_namespace, create_table, drop_namespace, drop_table,
    extract_data_files, extract_data_files_with_stats,
    insert_rows, list_tables, load_table, namespace_exists,
    register_existing_table,
};
```

Adjust the re-export list to exactly match the items that `registry.rs` currently exposes as `pub(crate)` — consult `registry.rs` first and mirror it.

- [ ] **Step 3: Remove old module declarations from `src/standalone/mod.rs`**

Delete these lines (they pointed to the old flat files):
- `pub(crate) mod hadoop_catalog;`
- `pub(crate) mod iceberg;`
- `pub(crate) mod iceberg_add_files;`
- `pub(crate) mod iceberg_s3_storage;`

Add: `pub(crate) mod iceberg;` (the new subdirectory module).

- [ ] **Step 4: Fix intra-file imports inside the four moved files**

In `iceberg/registry.rs`: if anything was `use super::iceberg_s3_storage::...` it is now `use super::s3_storage::...`. Same for `hadoop_catalog` and `add_files`.

In `iceberg/add_files.rs`: `use super::iceberg::...` becomes `use super::registry::...`.

In `iceberg/hadoop_catalog.rs`: `use super::iceberg_s3_storage::...` becomes `use super::s3_storage::...`.

Use `grep -rn "super::iceberg\|super::iceberg_s3_storage\|super::iceberg_add_files\|super::hadoop_catalog" src/standalone/iceberg/` to find all of them.

- [ ] **Step 5: Fix callers elsewhere in the codebase**

Grep for remaining references:
```bash
rg "crate::standalone::(iceberg_s3_storage|iceberg_add_files|hadoop_catalog)" src/
```

Replace:
- `crate::standalone::iceberg_s3_storage::X` → `crate::standalone::iceberg::s3_storage::X`
- `crate::standalone::iceberg_add_files::X` → `crate::standalone::iceberg::add_files::X`
- `crate::standalone::hadoop_catalog::X` → `crate::standalone::iceberg::hadoop_catalog::X`

`crate::standalone::iceberg::X` should continue to work because of the re-exports in `iceberg/mod.rs`. If any symbol is not re-exported, fix the re-export or the call site.

- [ ] **Step 6: Run `cargo check`**

Run: `cargo check`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add -A
git commit -m "refactor(standalone): group iceberg files under iceberg/ subdirectory"
```

---

## Task 5: Group managed-lake files under `src/standalone/lake/`

**Why:** Same idea as Task 4 — group by subsystem. Also split `lake_recovery.rs` into `config.rs` + `catalog.rs`.

**Files:**
- Rename: `src/standalone/lake_ddl.rs` → `src/standalone/lake/ddl.rs`
- Rename: `src/standalone/lake_txn.rs` → `src/standalone/lake/txn.rs`
- Rename: `src/standalone/store.rs` → `src/standalone/lake/store.rs`
- Delete: `src/standalone/lake_recovery.rs` (split into two)
- Create: `src/standalone/lake/config.rs`
- Create: `src/standalone/lake/catalog.rs`
- Modify: `src/standalone/lake/mod.rs`
- Modify: `src/standalone/mod.rs`

### Step 5.1: Relocate the pure-move files

- [ ] **Step 1: `git mv` the three files**

```bash
git mv src/standalone/lake_ddl.rs  src/standalone/lake/ddl.rs
git mv src/standalone/lake_txn.rs  src/standalone/lake/txn.rs
git mv src/standalone/store.rs     src/standalone/lake/store.rs
```

### Step 5.2: Split `lake_recovery.rs`

- [ ] **Step 2: Create `src/standalone/lake/config.rs` with `ManagedLakeConfig`**

Copy from `src/standalone/lake_recovery.rs`:
- `pub(crate) struct ManagedLakeConfig` (line 31)
- `impl ManagedLakeConfig` (line 36)

Plus any imports these two items need at the top of the new file.

- [ ] **Step 3: Create `src/standalone/lake/catalog.rs` with the runtime/catalog machinery**

Move the remaining contents of `lake_recovery.rs` to `lake/catalog.rs`:
- `pub(crate) struct ManagedLakeCatalog` (line 72)
- `impl ManagedLakeCatalog` (line 78)
- `pub(crate) struct ManagedTableRuntime` (line 359)
- `pub(crate) fn reconcile_on_open` (line 382)
- `pub(crate) fn snapshot_is_empty` (line 445)
- `pub(crate) fn runtime_registered` (line 457)
- `pub(crate) fn register_managed_table_in_catalog` (line 461)
- `pub(crate) fn register_managed_tables_in_catalog` (line 470)
- `fn managed_table_def` (line 486)
- `fn managed_physical_layout` (line 512)
- `fn visible_tablet_columns_by_name` (line 549)
- `fn arrow_type_from_tablet_column` (line 571)

In `lake/catalog.rs` add `use super::config::ManagedLakeConfig;` at the top.

- [ ] **Step 4: Delete `src/standalone/lake_recovery.rs`**

```bash
git rm src/standalone/lake_recovery.rs
```

### Step 5.3: Wire the module

- [ ] **Step 5: Fill in `src/standalone/lake/mod.rs`**

```rust
//! Managed lake subsystem (SQLite-backed metadata + DDL/DML + reconcile).

pub(crate) mod catalog;
pub(crate) mod config;
pub(crate) mod ddl;
pub(crate) mod store;
pub(crate) mod txn;

// Re-export the surface previously exposed as the flat lake_*.rs files.
pub(crate) use catalog::{
    ManagedLakeCatalog, ManagedTableRuntime,
    reconcile_on_open, register_managed_table_in_catalog, register_managed_tables_in_catalog,
    runtime_registered, snapshot_is_empty,
};
pub(crate) use config::ManagedLakeConfig;
```

Match the actual surface that callers use by grepping for `crate::standalone::lake_recovery::` / `crate::standalone::lake_ddl::` / etc. before writing this list.

- [ ] **Step 6: Update `src/standalone/mod.rs`**

Delete:
- `pub(crate) mod lake_ddl;`
- `pub(crate) mod lake_recovery;`
- `pub(crate) mod lake_txn;`
- `pub(crate) mod store;`

Add: `pub(crate) mod lake;`

### Step 5.4: Fix callers

- [ ] **Step 7: Fix intra-file imports in moved files**

In `lake/ddl.rs`, `lake/txn.rs`, `lake/store.rs`, `lake/catalog.rs`:
- `use super::lake_recovery::X` → `use super::catalog::X` (or `super::config::X` for `ManagedLakeConfig`)
- `use super::lake_ddl::X` → `use super::ddl::X`
- `use super::lake_txn::X` → `use super::txn::X`
- `use super::store::X` → `use super::store::X` (unchanged, still `super::`)

- [ ] **Step 8: Fix external callers**

```bash
rg "crate::standalone::(lake_ddl|lake_recovery|lake_txn|store)" src/
```

Replace each match with `crate::standalone::lake::<symbol>` (the re-exports in `lake/mod.rs` cover the common names; for anything not re-exported, fix the re-export list rather than importing directly from the sub-file).

- [ ] **Step 9: Run `cargo check` and commit**

```bash
cargo check
git add -A
git commit -m "refactor(standalone): group managed-lake files under lake/ and split lake_recovery"
```

---

## Task 6: Move `server.rs` into `src/standalone/server/` and split

**Files:**
- Delete: `src/standalone/server.rs`
- Create: `src/standalone/server/mod.rs`
- Create: `src/standalone/server/shim.rs`
- Create: `src/standalone/server/encoding.rs`
- Create: `src/standalone/server/statement.rs`
- Modify: `src/standalone/mod.rs`

### Step 6.1: Extract encoding

- [ ] **Step 1: Create `src/standalone/server/encoding.rs`**

Move from current `server.rs`:
- `enum StandaloneMysqlValue` (line 46)
- `impl ToMysqlValue for StandaloneMysqlValue` (line 65)
- `fn query_result_column_to_mysql_column` (line 942)
- `fn build_mysql_row` (line 1001)
- `fn array_value_to_mysql_value` (line 1031)
- `fn decimal128_to_mysql_value` (line 1121)
- `fn format_decimal128_string` (line 1132)
- `fn decimal_to_mysql_value` (line 1156)
- `fn timestamp_unit` (line 1198)
- `fn downcast_array` (line 1205)
- `fn date32_to_mysql_value` (line 1212)
- `fn timestamp_to_naive_datetime` (line 1220)
- `fn timestamp_to_mysql_value` (line 1259)
- `fn timestamp_to_date_mysql_value` (line 1269)
- `fn time_to_mysql_value` (line 1279)
- `fn invalid_data_error` (line 1330)
- `async fn write_query_result` (line 920)

All of these stay `pub(crate)` or `pub(super)` as needed. Adjust visibility so `shim.rs` can call what it needs.

### Step 6.2: Extract SQL parsing/recognition utilities

- [ ] **Step 2: Create `src/standalone/server/statement.rs`**

Move from current `server.rs`:
- `fn trim_query` (line 465)
- `fn is_session_noop` (line 469)
- `fn is_materialized_view_management_statement` (line 481)
- `fn split_sql_statements` (line 489)
- `fn parse_use_database_query` (line 534)
- `fn parse_set_catalog_query` (line 547)
- `fn parse_set_non_negative_integer` (line 571)
- `fn parse_set_query_timeout` (line 595)
- `fn parse_set_group_concat_max_len` (line 602)
- `fn is_supported_embedded_statement` (line 607)
- `struct SessionDatabaseContext` (line 629)
- `async fn resolve_catalog_name_in_worker` (line 634)
- `async fn resolve_database_context_in_worker` (line 643)
- `async fn execute_statement_text` (line 655)
- `fn resolve_catalog_name` (line 771)
- `fn resolve_database_context` (line 786)
- `fn parse_object_name` (line 845)
- `fn strip_identifier_quotes` (line 868)
- `fn normalize_current_catalog` (line 874)
- `fn resolve_catalog_name_for_context` (line 881)
- `fn classify_query_error` (line 892)

### Step 6.3: Extract `NovaRocksMysqlShim`

- [ ] **Step 3: Create `src/standalone/server/shim.rs`**

Move from current `server.rs`:
- `struct NovaRocksMysqlShim` (line 320)
- `impl NovaRocksMysqlShim` (line 334)
- `async fn serve_forever` (line 267)

Add `use super::encoding::*;` and `use super::statement::*;` (or selective imports).

### Step 6.4: Keep the server entrypoint in `mod.rs`

- [ ] **Step 4: Populate `src/standalone/server/mod.rs`**

Move from current `server.rs` (what's left):
- `pub struct StandaloneTableConfig` (line 116)
- `pub struct StandaloneServerOptions` (line 122)
- `struct ResolvedStandaloneServerOptions` (line 129)
- `pub fn run_standalone_server` (line 136)
- `fn resolve_server_options` (line 157)
- `fn resolve_active_config_path` (line 209)
- `fn load_active_config` (line 225)
- `fn merge_config_tables` (line 234)
- `fn preload_tables` (line 257)
- Module declarations:
  ```rust
  mod encoding;
  mod shim;
  mod statement;

  pub use self::{
      // existing pub items only
  };
  ```

- [ ] **Step 5: Delete `src/standalone/server.rs`**

```bash
git rm src/standalone/server.rs
```

- [ ] **Step 6: Update `src/standalone/mod.rs`**

Change `mod server;` to `pub(crate) mod server;` (or whatever form it has today). The `pub use server::{StandaloneServerOptions, StandaloneTableConfig, run_standalone_server};` line stays identical.

- [ ] **Step 7: Run `cargo check` and commit**

```bash
cargo check
git add -A
git commit -m "refactor(standalone): split server.rs into server/ subdirectory"
```

---

## Task 7: Extract `engine/local/aggregate.rs`

**Why:** Leaf-first. `aggregate.rs` has zero dependencies on other engine sub-modules.

**Files:**
- Create: `src/standalone/engine/local/aggregate.rs`
- Modify: `src/standalone/engine.rs` (remove extracted functions + add import)
- Modify: `src/standalone/engine/local/mod.rs`

- [ ] **Step 1: Create `src/standalone/engine/local/aggregate.rs`**

Copy from `src/standalone/engine.rs`:
- `fn merge_aggregate_table_rows_if_needed` (line 4058)
- `fn merge_aggregate_table_row` (line 4110)
- `fn merge_aggregate_table_value` (line 4136)

All three change from `fn` to `pub(crate) fn` (so `engine.rs` can still call them).

Add imports at top as needed — each function uses `arrow`, `crate::sql::parser::ast::{ColumnAggregation, Literal}`, `super::super::super::...` — use absolute `crate::` paths to avoid brittle `super` chains:

```rust
use std::collections::HashMap;
use arrow::array::ArrayRef;
use arrow::record_batch::RecordBatch;
use crate::sql::parser::ast::{ColumnAggregation, Literal};
```

(Inspect the extracted code to determine the exact import set; don't guess.)

- [ ] **Step 2: Delete the three functions from `engine.rs`**

- [ ] **Step 3: Add `pub(crate) mod aggregate;` to `src/standalone/engine/local/mod.rs`**

- [ ] **Step 4: Add import to `engine.rs`**

At the top of `engine.rs` (in the existing `use` block section), add:
```rust
use crate::standalone::engine::local::aggregate::{
    merge_aggregate_table_rows_if_needed,
};
```

Only import the functions that `engine.rs` actually calls. Internal helpers (`merge_aggregate_table_row`, `merge_aggregate_table_value`) can stay unimported.

- [ ] **Step 5: Register `engine/local` module**

For this and subsequent tasks, `engine.rs` still lives at `src/standalone/engine.rs`. The `engine/` subdirectory is NOT reachable from the crate yet. We need a temporary trick:

In `src/standalone/mod.rs`, add BELOW the existing `mod engine;`:
```rust
// Temporary scaffolding for PR1 refactor. Merged into `mod engine;` at Task 20.
#[path = "engine/local/mod.rs"]
pub(crate) mod engine_local;
#[path = "engine/local/aggregate.rs"]
mod __engine_local_aggregate_stub;  // Not needed — mod declared in engine_local/mod.rs
```

**Correction — use this simpler approach instead:** Rename `src/standalone/engine.rs` to `src/standalone/engine/mod.rs` at the START (Task 7, step 0). That way `engine/` is the module from the first extraction onwards.

- [ ] **Step 0 (precursor): Rename `engine.rs` to `engine/mod.rs`**

```bash
git mv src/standalone/engine.rs src/standalone/engine/mod.rs
```

Verify `cargo check` passes (Rust accepts `foo/mod.rs` style; the file was already 6830 lines and it's fine).

Commit before continuing:
```bash
git add -A
git commit -m "refactor(standalone): rename engine.rs to engine/mod.rs"
```

Then re-do steps 1–4 under the new path: the three functions live in `src/standalone/engine/mod.rs`, and you move them to `src/standalone/engine/local/aggregate.rs`.

- [ ] **Step 6: `cargo check` and commit**

```bash
cargo check
git add -A
git commit -m "refactor(engine): extract aggregate merge into engine/local/aggregate.rs"
```

---

## Task 8: Extract `engine/sqlparse/materialized_view.rs`

**Files:**
- Create: `src/standalone/engine/sqlparse/materialized_view.rs`
- Modify: `src/standalone/engine/mod.rs`
- Modify: `src/standalone/engine/sqlparse/mod.rs`

- [ ] **Step 1: Create `src/standalone/engine/sqlparse/materialized_view.rs`**

Copy from `engine/mod.rs`:
- `fn materialized_view_key` (line 1251)
- `fn parse_create_materialized_view_name` (line 1262)
- `fn parse_drop_materialized_view_name` (line 1288)
- `fn parse_refresh_materialized_view_name` (line 1311)
- `fn looks_like_show_alter_materialized_view` (line 1325)
- `fn supports_bitmap_count_rewrite` (line 1340)

Change to `pub(crate) fn`.

Imports:
```rust
use crate::sql::parser::ast::ObjectName;  // if used
```

(Check actual imports needed by reading the 95 lines of code.)

- [ ] **Step 2: Delete the six functions from `engine/mod.rs`**

- [ ] **Step 3: Register in `engine/sqlparse/mod.rs`**

```rust
pub(crate) mod materialized_view;
```

- [ ] **Step 4: Import in `engine/mod.rs`**

```rust
use crate::standalone::engine::sqlparse::materialized_view::{
    looks_like_show_alter_materialized_view, materialized_view_key,
    parse_create_materialized_view_name, parse_drop_materialized_view_name,
    parse_refresh_materialized_view_name, supports_bitmap_count_rewrite,
};
```

Also register the new module path in `src/standalone/mod.rs` — but since `mod engine` is already declared there, and `engine/mod.rs` now declares `mod sqlparse;`, we just need to edit `engine/mod.rs` to add `pub(crate) mod sqlparse;` at the module-declaration area.

- [ ] **Step 5: `cargo check` and commit**

```bash
cargo check
git add -A
git commit -m "refactor(engine): extract materialized_view recognition into sqlparse/materialized_view.rs"
```

---

## Task 9: Extract `engine/sqlparse/generate_series.rs`

**Files:**
- Create: `src/standalone/engine/sqlparse/generate_series.rs`
- Modify: `src/standalone/engine/mod.rs`
- Modify: `src/standalone/engine/sqlparse/mod.rs`

- [ ] **Step 1: Create `src/standalone/engine/sqlparse/generate_series.rs`**

Copy from `engine/mod.rs`:
- `fn parse_generate_series_function_expr` (line 1442)
- `fn insert_generate_series_rows` (line 2203)
- `fn evaluate_generate_series_row` (line 2301)
- `fn evaluate_generate_series_expr` (line 2312)
- `pub(crate) fn insert_generate_series_rows_local` (line 3191)

Change private ones to `pub(crate) fn`.

Determine imports by reading the extracted code. Likely:
```rust
use crate::sql::parser::ast::{GenerateSeriesSelect, Literal, SqlType};
// + any arithmetic helpers
```

- [ ] **Step 2: Delete the five functions from `engine/mod.rs`**

- [ ] **Step 3: Register in `engine/sqlparse/mod.rs`**

```rust
pub(crate) mod generate_series;
```

- [ ] **Step 4: Import in `engine/mod.rs`**

Import only the functions `engine/mod.rs` still calls.

- [ ] **Step 5: `cargo check` and commit**

```bash
cargo check
git add -A
git commit -m "refactor(engine): extract generate_series helpers into sqlparse/generate_series.rs"
```

---

## Task 10: Extract `engine/sqlparse/expr.rs` — the biggest sqlparse piece

**Why:** ~1200 lines of sqlparser→Expr/Literal conversion and literal utilities. Zero dependencies on other engine sub-modules.

**Files:**
- Create: `src/standalone/engine/sqlparse/expr.rs`
- Modify: `src/standalone/engine/mod.rs`
- Modify: `src/standalone/engine/sqlparse/mod.rs`

- [ ] **Step 1: Create `src/standalone/engine/sqlparse/expr.rs`**

Copy the following functions from `engine/mod.rs`:

From region 1491–2690:
- `fn sqlparser_expr_to_custom_expr` (1491)
- `fn bytes_to_latin1_string` (1572)
- `fn latin1_string_to_bytes` (1576)
- `fn sqlparser_expr_to_literal` (1588)
- `fn sql_number_literal` (1673)
- `fn is_integral_sql_number` (1687)
- `fn negate_literal` (1691)
- `fn literal_to_i128_for_integer` (1702)
- `fn sqlparser_function_to_literal` (1736)
- `fn function_expr_args` (1806)
- `fn eval_literal_arithmetic` (2350)
- `fn cast_literal` (2389)
- `fn is_select_without_from` (2444)
- `fn evaluate_constant_select` (2453)
- `fn evaluate_const_expr` (2510)
- `fn extract_numeric_scalar` (2604)
- `fn sql_type_to_arrow_type` (2630)

From region 4201–4446:
- `fn compare_literals` (4201)
- `enum LiteralKey` (4222)
- `fn literal_to_key` (4231)
- `fn literal_from_batch` (4253)
- `fn format_decimal128_value` (4422)

From region 5230–5272:
- `fn parse_kv_properties` (5230)
- `fn parse_prop_string_or_ident` (5257)

From 1258, 1333:
- `fn strip_optional_identifier_quotes` (1258)
- `fn canonicalize_sql_for_match` (1333)

All change from `fn` to `pub(crate) fn`.

Required imports at top of new file (consult the extracted code to confirm):
```rust
use std::cmp::Ordering;
use arrow::array::{Array, ArrayRef};
use arrow::datatypes::{DataType, Field, Fields, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use crate::sql::parser::ast::{ArithmeticOp, Expr, Literal, SqlType};
// plus sqlparser::ast imports
```

- [ ] **Step 2: Delete the extracted functions from `engine/mod.rs`**

Tip: do this in regions so you don't miss any. Use `grep -n "^fn " src/standalone/engine/mod.rs | less` to verify after deletion.

- [ ] **Step 3: Register in `engine/sqlparse/mod.rs`**

```rust
pub(crate) mod expr;
```

- [ ] **Step 4: Import in `engine/mod.rs`**

Only pull in the subset `engine/mod.rs` still calls. Likely:
```rust
use crate::standalone::engine::sqlparse::expr::{
    canonicalize_sql_for_match, cast_literal, compare_literals,
    evaluate_const_expr, evaluate_constant_select, format_decimal128_value,
    is_select_without_from, literal_from_batch, parse_kv_properties,
    parse_prop_string_or_ident, sql_type_to_arrow_type,
    sqlparser_expr_to_custom_expr, sqlparser_expr_to_literal,
    strip_optional_identifier_quotes,
};
```

- [ ] **Step 5: Import from `sqlparse/expr.rs` in the other sqlparse modules**

`materialized_view.rs` uses `strip_optional_identifier_quotes` and `canonicalize_sql_for_match` — import them as `use super::expr::*;`.
`generate_series.rs` uses `eval_literal_arithmetic` / `cast_literal` / `sqlparser_expr_to_literal` — same pattern.

- [ ] **Step 6: `cargo check` and commit**

```bash
cargo check
git add -A
git commit -m "refactor(engine): extract sqlparser-to-Expr conversion into sqlparse/expr.rs"
```

---

## Task 11: Extract `engine/local/parquet.rs`

**Files:**
- Create: `src/standalone/engine/local/parquet.rs`
- Modify: `src/standalone/engine/mod.rs`
- Modify: `src/standalone/engine/local/mod.rs`

- [ ] **Step 1: Create `src/standalone/engine/local/parquet.rs`**

Copy from `engine/mod.rs`:
- `fn read_local_parquet_data` (3217)
- `fn cast_batch_to_schema` (3707)
- `fn cast_list_struct_to_map_for_local_schema` (3739)
- `fn cast_array_for_local_schema` (3770)
- `fn parquet_storage_type_for_local_batch` (3870)
- `fn encode_array_for_local_parquet_storage` (3881)
- `fn normalize_local_parquet_batch` (3906)
- `pub(crate) fn write_parquet_to_path` (3933)
- `fn parse_date_string_to_days` (3681)
- `fn parse_datetime_string_to_micros` (3689)

Change private ones to `pub(crate) fn`.

Imports: arrow parquet crate, `std::path::Path`, `crate::standalone::engine::local::catalog::ColumnDef` (after Task 13 merges catalog.rs) — for now still `crate::standalone::catalog::ColumnDef`.

- [ ] **Step 2: Delete extracted functions from `engine/mod.rs`**

- [ ] **Step 3: Register module**

In `engine/local/mod.rs`:
```rust
pub(crate) mod parquet;
```

- [ ] **Step 4: Import in `engine/mod.rs`**

- [ ] **Step 5: `cargo check` and commit**

```bash
cargo check
git add -A
git commit -m "refactor(engine): extract local parquet I/O into local/parquet.rs"
```

---

## Task 12: Extract `engine/local/insert.rs`

**Files:**
- Create: `src/standalone/engine/local/insert.rs`
- Modify: `src/standalone/engine/mod.rs`

- [ ] **Step 1: Create `src/standalone/engine/local/insert.rs`**

Copy from `engine/mod.rs`:
- `pub(crate) fn reorder_insert_rows` (2238)
- `fn build_insert_column_mapping` (2252)
- `fn reorder_insert_row` (2275)
- `fn insert_into_local_table` (2835)
- `pub(crate) fn build_local_insert_batch` (3243)
- `fn build_local_literal_array` (3275)
- `fn parse_decimal_string_to_i128` (3641)

Change private ones to `pub(crate) fn`.

Imports: arrow arrays, `crate::sql::parser::ast::{Expr, Literal, SqlType}`, `super::parquet::...` for helpers that crossed over.

- [ ] **Step 2: Delete extracted functions from `engine/mod.rs`**

- [ ] **Step 3: Register module in `engine/local/mod.rs`**

```rust
pub(crate) mod insert;
```

- [ ] **Step 4: Import in `engine/mod.rs`**

- [ ] **Step 5: `cargo check` and commit**

```bash
cargo check
git add -A
git commit -m "refactor(engine): extract local insert path into local/insert.rs"
```

---

## Task 13: Extract `engine/local/stream_load.rs`

**Files:**
- Create: `src/standalone/engine/local/stream_load.rs`
- Modify: `src/standalone/engine/mod.rs`

- [ ] **Step 1: Create `src/standalone/engine/local/stream_load.rs`**

Copy from `engine/mod.rs`:
- `fn stream_load_local_table` (2902)
- `fn parse_stream_load_columns` (2953)
- `fn parse_csv_stream_load_rows` (2987)
- `fn single_byte_stream_load_delimiter` (3058)
- `fn parse_json_stream_load_rows` (3068)
- `fn parse_stream_load_jsonpaths` (3092)
- `fn parse_json_rows` (3114)
- `fn extract_json_path` (3129)
- `fn json_value_to_field` (3180)

Change to `pub(crate) fn` as needed.

Imports: `csv`, `serde_json::Value`, `crate::plan_nodes::TFileFormatType`.

- [ ] **Step 2: Delete extracted functions from `engine/mod.rs`**

- [ ] **Step 3: Register module in `engine/local/mod.rs`**

```rust
pub(crate) mod stream_load;
```

- [ ] **Step 4: Import in `engine/mod.rs`**

- [ ] **Step 5: `cargo check` and commit**

```bash
cargo check
git add -A
git commit -m "refactor(engine): extract local stream load into local/stream_load.rs"
```

---

## Task 14: Populate `engine/local/mod.rs` + merge `standalone/catalog.rs`

**Why:** Final piece of `local/`. Absorbs the old `src/standalone/catalog.rs`.

**Files:**
- Modify: `src/standalone/engine/local/mod.rs`
- Delete: `src/standalone/catalog.rs`
- Modify: `src/standalone/mod.rs`
- Modify: `src/standalone/engine/mod.rs`

- [ ] **Step 1: Move `standalone/catalog.rs` contents into `engine/local/mod.rs`**

Copy everything from `src/standalone/catalog.rs` (the file content, not `git mv` — we're merging into an existing `mod.rs` that already has `pub(crate) mod aggregate;` etc.).

Items to copy:
- `struct DatabaseDef`
- `pub(crate) struct InMemoryCatalog`
- `impl Default for InMemoryCatalog`
- `impl InMemoryCatalog`
- `impl CatalogProvider for InMemoryCatalog`
- `pub(crate) fn normalize_identifier`
- `pub(crate) fn build_parquet_table`

Also copy from `engine/mod.rs`:
- `pub(crate) struct LocalTableSemantics` (engine.rs original line 207)
- `fn create_local_table_from_columns` (2686)
- `fn update_local_table_semantics` (2756)
- `fn get_local_table_semantics` (2787)
- `fn remove_local_table_semantics` (2804)
- `fn remove_local_database_semantics` (2821)
- `fn apply_local_table_semantics_if_needed` (4036)
- `fn ensure_dual_table` (3954)
- `fn ensure_dual_in_database` (3958)

Change privates to `pub(crate)`.

- [ ] **Step 2: Delete `src/standalone/catalog.rs`**

```bash
git rm src/standalone/catalog.rs
```

- [ ] **Step 3: Update `src/standalone/mod.rs`**

Remove `pub(crate) mod catalog;`.

The existing `pub use crate::sql::catalog::{ColumnDef, TableDef, TableStorage};` line stays — it refers to a different `sql::catalog`, not the one we just merged.

- [ ] **Step 4: Delete the copied items from `engine/mod.rs`**

- [ ] **Step 5: Fix import paths**

Replace `crate::standalone::catalog::X` throughout `src/` with `crate::standalone::engine::local::X`, where `X` ∈ `{InMemoryCatalog, normalize_identifier, build_parquet_table, DEFAULT_DATABASE}`.

```bash
rg "crate::standalone::catalog::" src/
```

Fix every hit.

- [ ] **Step 6: Add import in `engine/mod.rs`**

```rust
use crate::standalone::engine::local::{
    InMemoryCatalog, LocalTableSemantics, apply_local_table_semantics_if_needed,
    create_local_table_from_columns, ensure_dual_in_database, ensure_dual_table,
    get_local_table_semantics, remove_local_database_semantics,
    remove_local_table_semantics, update_local_table_semantics,
};
```

- [ ] **Step 7: `cargo check` and commit**

```bash
cargo check
git add -A
git commit -m "refactor(engine): merge catalog.rs into engine/local/ and extract LocalTableSemantics"
```

---

## Task 15: Extract `engine/sqlparse/statement.rs`

**Files:**
- Create: `src/standalone/engine/sqlparse/statement.rs`
- Modify: `src/standalone/engine/mod.rs`
- Modify: `src/standalone/engine/sqlparse/mod.rs`

- [ ] **Step 1: Create `src/standalone/engine/sqlparse/statement.rs`**

Copy from `engine/mod.rs`:
- `fn convert_sqlparser_insert_to_custom` (1348)
- `fn execute_create_database_statement` (1833)
- `fn execute_create_table_statement` (1862)
- `fn execute_drop_catalog_statement` (1945)
- `fn execute_drop_database_statement` (1965)
- `fn execute_drop_table_statement` (2027)
- `fn execute_truncate_table_statement` (2095)
- `fn execute_insert_statement` (2140)
- `fn try_parse_local_parquet_create_table` (5172)
- `fn looks_like_add_files` (5275)
- `fn parse_add_files_sql` (5281)
- `fn extract_table_names_from_query` (4949)
- `fn extract_table_names_from_set_expr` (4963)
- `fn extract_table_names_from_table_factor` (4994)
- `fn extract_table_names_from_expr_opt` (5016)
- `fn extract_table_names_from_expr` (5022)
- `fn extract_table_names_from_subquery` (5053)
- `fn extract_three_part_table_refs` (5063)
- `fn extract_three_part_refs_from_set_expr` (5071)
- `fn extract_three_part_refs_from_factor` (5095)
- `fn strip_catalog_from_three_part_names` (5125)
- `fn strip_catalog_in_set_expr` (5129)
- `fn strip_catalog_in_factor` (5150)

Change to `pub(crate) fn`.

Imports: `crate::sql::parser::ast::*`, sqlparser, `super::expr::*`, `super::super::local::*`, `super::super::name_resolve::*` (when name_resolve is extracted — see Task 17).

> NOTE: `statement.rs` depends on `name_resolve.rs`, but `name_resolve.rs` isn't extracted yet. Extract it first (Task 17), then come back here — but since we're doing leaf-first, the real order is: Task 15 uses `super::super::super::*` paths pointing to functions still in `engine/mod.rs`, then after Task 17 extracts `name_resolve`, we update `statement.rs` imports.
>
> **Simpler:** do Task 17 (name_resolve) and Task 18 (persistence) BEFORE Task 15 (statement). Reorder the plan mentally to: 10, 11, 12, 13, 14, 17, 18, 15, 16, 19, 20.

- [ ] **Step 2: Delete the extracted functions from `engine/mod.rs`**

- [ ] **Step 3: Register in `engine/sqlparse/mod.rs`**

```rust
pub(crate) mod statement;
```

- [ ] **Step 4: Import in `engine/mod.rs`**

Only the entry-point functions that top-level dispatch calls:
```rust
use crate::standalone::engine::sqlparse::statement::{
    execute_create_database_statement, execute_create_table_statement,
    execute_drop_catalog_statement, execute_drop_database_statement,
    execute_drop_table_statement, execute_insert_statement,
    execute_truncate_table_statement, extract_table_names_from_query,
    extract_three_part_table_refs, looks_like_add_files, parse_add_files_sql,
    strip_catalog_from_three_part_names, try_parse_local_parquet_create_table,
};
```

- [ ] **Step 5: `cargo check` and commit**

```bash
cargo check
git add -A
git commit -m "refactor(engine): extract DDL/DML statement handlers into sqlparse/statement.rs"
```

---

## Task 16: Extract `engine/iceberg_glue.rs`

**Files:**
- Create: `src/standalone/engine/iceberg_glue.rs`
- Modify: `src/standalone/engine/mod.rs`

- [ ] **Step 1: Create `src/standalone/engine/iceberg_glue.rs`**

Copy from `engine/mod.rs`:
- `fn load_full_iceberg_batch` (3998)
- `fn apply_iceberg_table_semantics_if_needed` (4020)
- `fn normalize_iceberg_source_batch` (4446)
- `fn iceberg_field_indices` (4480)
- `fn normalize_iceberg_array_type` (4499)
- `fn concat_or_empty_batches` (4516)

Change to `pub(crate) fn`.

Imports: `arrow::*`, `crate::standalone::iceberg::{IcebergCatalogRegistry, IcebergLoadedTable}`.

- [ ] **Step 2: Delete from `engine/mod.rs`**

- [ ] **Step 3: Register module**

In `engine/mod.rs` add `pub(crate) mod iceberg_glue;`.

- [ ] **Step 4: Import in `engine/mod.rs`**

```rust
use crate::standalone::engine::iceberg_glue::{
    apply_iceberg_table_semantics_if_needed, concat_or_empty_batches,
    load_full_iceberg_batch, normalize_iceberg_source_batch,
};
```

- [ ] **Step 5: `cargo check` and commit**

```bash
cargo check
git add -A
git commit -m "refactor(engine): extract iceberg batch-glue into engine/iceberg_glue.rs"
```

---

## Task 17: Extract `engine/name_resolve.rs`

**Files:**
- Create: `src/standalone/engine/name_resolve.rs`
- Modify: `src/standalone/engine/mod.rs`

- [ ] **Step 1: Create `src/standalone/engine/name_resolve.rs`**

Copy from `engine/mod.rs`:
- `pub(crate) struct ResolvedLocalTableName` (4544)
- `struct ResolvedIcebergNamespaceName` (4550)
- `struct ResolvedIcebergTableName` (4556)
- `fn resolve_local_table_name` (4562)
- `fn resolve_iceberg_namespace_name` (4582)
- `fn resolve_iceberg_table_name` (4605)
- `fn resolve_iceberg_table_name_explicit` (4636)
- `fn normalize_optional_identifier` (4652)

Change private ones to `pub(crate)`.

Imports: `crate::sql::parser::ast::ObjectName`, `super::local::*` (for `LocalTableSemantics`), `crate::standalone::iceberg::IcebergCatalogRegistry`.

- [ ] **Step 2: Delete from `engine/mod.rs`**

- [ ] **Step 3: Register**

In `engine/mod.rs` add `pub(crate) mod name_resolve;`.

- [ ] **Step 4: Import in `engine/mod.rs`**

```rust
use crate::standalone::engine::name_resolve::{
    ResolvedIcebergNamespaceName, ResolvedIcebergTableName, ResolvedLocalTableName,
    normalize_optional_identifier, resolve_iceberg_namespace_name,
    resolve_iceberg_table_name, resolve_iceberg_table_name_explicit,
    resolve_local_table_name,
};
```

- [ ] **Step 5: `cargo check` and commit**

```bash
cargo check
git add -A
git commit -m "refactor(engine): extract name resolution into engine/name_resolve.rs"
```

---

## Task 18: Extract `engine/persistence.rs`

**Files:**
- Create: `src/standalone/engine/persistence.rs`
- Modify: `src/standalone/engine/mod.rs`

- [ ] **Step 1: Create `src/standalone/engine/persistence.rs`**

Copy from `engine/mod.rs`:
- `fn resolve_metadata_store` (4660)
- `fn resolve_managed_lake_config` (4678)
- `fn resolve_relative_path` (4687)
- `fn restore_metadata_if_needed` (4701)
- `fn restore_local_catalog` (4712)
- `fn restore_iceberg_catalogs` (4726)
- `fn restore_managed_lake` (4760)
- `fn persist_local_database_if_needed` (4808)
- `fn persist_local_table_if_needed` (4820)
- `fn delete_local_table_if_needed` (4829)
- `fn delete_local_database_if_needed` (4837)
- `fn persist_iceberg_catalog_if_needed` (4849)
- `fn persist_iceberg_namespace_if_needed` (4860)
- `fn persist_iceberg_table_if_needed` (4871)
- `fn delete_iceberg_table_if_needed` (4884)
- `fn delete_iceberg_namespace_if_needed` (4896)
- `fn delete_iceberg_catalog_if_needed` (4907)
- `pub(crate) fn block_on_standalone_async` (4921)

Change to `pub(crate) fn`.

Imports: `crate::standalone::lake::*`, `crate::standalone::iceberg::*`, `super::local::*`, `crate::novarocks_config`.

- [ ] **Step 2: Delete from `engine/mod.rs`**

- [ ] **Step 3: Register**

In `engine/mod.rs` add `pub(crate) mod persistence;`.

- [ ] **Step 4: Import in `engine/mod.rs`**

```rust
use crate::standalone::engine::persistence::{
    block_on_standalone_async, delete_iceberg_catalog_if_needed,
    delete_iceberg_namespace_if_needed, delete_iceberg_table_if_needed,
    delete_local_database_if_needed, delete_local_table_if_needed,
    persist_iceberg_catalog_if_needed, persist_iceberg_namespace_if_needed,
    persist_iceberg_table_if_needed, persist_local_database_if_needed,
    persist_local_table_if_needed, resolve_managed_lake_config,
    resolve_metadata_store, restore_metadata_if_needed,
};
```

- [ ] **Step 5: `cargo check` and commit**

```bash
cargo check
git add -A
git commit -m "refactor(engine): extract persistence glue into engine/persistence.rs"
```

---

## Task 19: Extract `engine/planner.rs`

**Files:**
- Create: `src/standalone/engine/planner.rs`
- Modify: `src/standalone/engine/mod.rs`

- [ ] **Step 1: Create `src/standalone/engine/planner.rs`**

Copy from `engine/mod.rs`:
- `enum StandaloneExecutionPlan` (5318)
- `fn top_level_stream_root_wrapper_child_id` (5325)
- `fn strip_top_level_stream_root_wrapper` (5379)
- `fn single_fragment_plan` (5399)
- `fn choose_standalone_execution` (5414)
- `fn explain_query` (5434)
- `fn execute_query` (5461)
- `fn ensure_standalone_exchange_server` (5494)
- `fn wait_for_standalone_exchange_server` (5541)
- `fn build_table_stats_from_plan` (5565)
- `fn collect_scan_stats` (5574)
- `fn execute_plan` (5631)
- `fn split_explain_costs_sql` (5714)

Change to `pub(crate) fn`.

Imports: `crate::runtime::coordinator::ExecutionCoordinator`, `crate::exec::*`, `crate::lower::*`, `crate::sql::codegen::*`, `crate::sql::optimizer::*`, `crate::runtime::query_result::{QueryResult, QueryResultColumn}`.

- [ ] **Step 2: Delete from `engine/mod.rs`**

- [ ] **Step 3: Register**

In `engine/mod.rs` add `pub(crate) mod planner;`.

- [ ] **Step 4: Import in `engine/mod.rs`**

```rust
use crate::standalone::engine::planner::{
    execute_plan, execute_query, explain_query, split_explain_costs_sql,
};
```

- [ ] **Step 5: `cargo check` and commit**

```bash
cargo check
git add -A
git commit -m "refactor(engine): extract query planner/executor into engine/planner.rs"
```

---

## Task 20: Tidy `engine/mod.rs` and dependency audit

**Goal:** After extractions, `engine/mod.rs` should be ~1500 lines, containing only:
- Top-level type definitions (`StandaloneOptions`, `StatementResult`, `StandaloneState`, `StandaloneNovaRocks`, `StandaloneSession`, etc.)
- `impl StandaloneNovaRocks`
- Top-level dispatch methods of `impl StandaloneSession`
- Generic helpers: `record_batch_to_chunk`

**Files:**
- Modify: `src/standalone/engine/mod.rs`

- [ ] **Step 1: Check size**

```bash
wc -l src/standalone/engine/mod.rs
```

Expected: ~1500–2000 lines. If >2500, something wasn't fully extracted — grep for the function categories:
```bash
grep -n "^fn\|^pub(crate) fn\|^pub fn" src/standalone/engine/mod.rs | head -80
```

Anything in `sqlparse/`, `local/`, `name_resolve`, `persistence`, `planner`, `iceberg_glue` categories that's still in `mod.rs` should be moved.

- [ ] **Step 2: Clean up stale imports**

Go through the `use` block at the top of `engine/mod.rs`. Anything only referenced by code that's been moved out should be removed. Rust will warn about unused imports — use:

```bash
cargo check 2>&1 | grep "unused import"
```

Fix each one.

- [ ] **Step 3: Verify the dependency graph**

Check that `engine/` sub-modules respect the one-way graph defined in the spec (section 4):

```bash
# These should be empty:
rg "use super::mod\b" src/standalone/engine/
rg "use crate::standalone::engine::mod\b" src/standalone/engine/
# name_resolve and persistence should not import from planner/sqlparse/local:
rg "use super::(planner|sqlparse|local|iceberg_glue)" src/standalone/engine/name_resolve.rs src/standalone/engine/persistence.rs
```

Empty output expected. If not empty, refactor the violating import.

- [ ] **Step 4: Remove `#![allow(dead_code)]` if possible**

`engine.rs` originally had `#![allow(dead_code)]` at the top. If the extracted files no longer have dead code, remove it from those files. Keep it on `engine/mod.rs` only if still needed.

- [ ] **Step 5: `cargo clippy`**

```bash
cargo clippy --all-targets -- -D warnings
```

Fix any clippy warnings the refactor introduced. Do NOT fix pre-existing warnings that are not about the moved code.

- [ ] **Step 6: `cargo fmt`**

```bash
cargo fmt
```

- [ ] **Step 7: `cargo test`**

```bash
cargo test
```

Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add -A
git commit -m "refactor(engine): tidy engine/mod.rs imports and pass clippy"
```

---

## Task 21: SQL suite verification

**Why:** Behavior-change guarantee. Any row diff here is a bug introduced by the refactor.

**Files:** None modified.

- [ ] **Step 1: Release build**

```bash
cargo build --release
```

Expected: succeeds.

- [ ] **Step 2: Start standalone server**

In a separate terminal:
```bash
NO_PROXY=127.0.0.1,localhost cargo run --release -- standalone-server --port 9030
```

- [ ] **Step 3: Run SSB suite**

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --suite ssb --mode verify -j 4
```

Expected: all 13 queries PASS, zero diff.

- [ ] **Step 4: Run TPC-H suite**

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --suite tpc-h --mode verify -j 4
```

Expected: all 22 queries PASS, zero diff.

- [ ] **Step 5: Run TPC-DS suite**

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --suite tpc-ds --mode verify -j 4
```

Expected: all 99 queries PASS, zero diff.

- [ ] **Step 6: Stream load smoke test**

Create a local table, stream-load a small CSV and JSON payload, verify rows:

```bash
# (Use the existing stream_load integration test or equivalent sanity command)
cargo test --release --test stream_load -- --nocapture || echo "adjust to actual test name"
```

If no dedicated test exists, at minimum connect via mysql client:
```bash
mysql -h127.0.0.1 -P9030 -uroot -e "
  CREATE DATABASE IF NOT EXISTS rt;
  USE rt;
  CREATE TABLE t (k INT, v VARCHAR(32)) ENGINE=Local PROPERTIES('format'='parquet');
  INSERT INTO t VALUES (1,'a'),(2,'b');
  SELECT COUNT(*) FROM t;
"
```

Expected: count returns 2.

- [ ] **Step 7: Stop the server**

Ctrl-C the standalone-server.

- [ ] **Step 8: Verify public API byte-stability**

```bash
git diff ed5ab23 -- src/standalone/mod.rs
```

The only expected hunks: (1) re-export path for `QueryResult` / `QueryResultColumn` changes source to `crate::runtime::query_result`; (2) `mod` declarations restructured. The `pub use` symbol list must be identical.

- [ ] **Step 9: Final lint pass**

```bash
cargo fmt --check
cargo clippy --all-targets -- -D warnings
```

All green.

- [ ] **Step 10: Update spec status**

Append to [docs/superpowers/specs/2026-04-21-standalone-refactor-design.md](../specs/2026-04-21-standalone-refactor-design.md) a line under a new "## Status" section:

```markdown
## Status

- PR1 (directory reorg + engine.rs split): completed 2026-04-XX.
- PR2 (iceberg.rs internal split): pending.
```

- [ ] **Step 11: Commit the status update**

```bash
git add docs/superpowers/specs/2026-04-21-standalone-refactor-design.md
git commit -m "docs(standalone): mark PR1 of standalone refactor as done"
```

- [ ] **Step 12: Open PR**

Use the starrocks-create-pr skill / `gh pr create` to submit. Include a note in the PR body linking to the spec and listing the verification results.

---

## Rollback plan

If a later task exposes a bug introduced earlier:

1. `git log --oneline main..HEAD` — find the commit that introduced the regression.
2. `git revert <commit>` — revert just that commit. Each task is a discrete commit, so reverts are surgical.
3. Re-run `cargo check` to confirm the revert compiles.
4. Re-plan the problematic extraction.

Do NOT `git reset --hard`. Do NOT squash until the whole plan is verified.

---

## Plan self-review notes

- **Spec coverage:** All 12 `engine.rs` sub-concerns have a target task. Directory reorg covered in tasks 1, 4, 5, 6. Coordinator + QueryResult move in 2, 3.
- **Task ordering:** Corrected — `name_resolve` (17) and `persistence` (18) must be extracted before `statement` (15) because `statement.rs` imports them. **Execute in this order: 7, 8, 9, 10, 11, 12, 13, 14, 17, 18, 15, 16, 19, 20, 21.** (The tasks are numbered by "what they produce" for readability, but the dependency-correct execution order is the sequence just given.)
- **Type consistency:** Re-export names in Tasks 2, 3, 4, 5 match the types they reference. Function names in the import lists match the declarations.
- **No placeholders:** Every task has exact file paths, exact line ranges, exact commands, exact commit messages.
