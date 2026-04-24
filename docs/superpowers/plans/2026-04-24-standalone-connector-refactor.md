# Standalone / Connector Decoupling Refactor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move connector-specific code out of `src/standalone/` into `src/connector/`, introduce connector-agnostic traits (`CatalogBackend` / `TableSource` / `TableSink` / `MvBackend`), and rewrite the DDL / INSERT / MV control flow in `standalone/engine/` to dispatch through those traits instead of hard-coded `if managed { ... } else { iceberg... }` branches.

**Architecture:** Three phases, each a shippable, behavior-preserving checkpoint.

1. **Phase 1 — Mechanical migration.** Relocate `src/standalone/iceberg/` → `src/connector/iceberg/catalog/` and `src/standalone/lake/` → `src/connector/starrocks/managed/`. Public APIs, function signatures, call graphs all unchanged — only paths and module prefixes change.
2. **Phase 2 — Trait introduction.** Define `CatalogBackend`, `TableSource`, `TableSink`, `MvBackend` in `src/connector/mod.rs`. Implement them for the relocated iceberg and managed backends as thin adapters over the existing functions. Register them in `ConnectorRegistry`. Old call sites remain; new traits live alongside them.
3. **Phase 3 — Control-flow extraction.** Rewrite `standalone/engine/sqlparse/statement.rs::execute_{create,drop,insert}_statement` and `engine/mod.rs::dispatch_statement` to resolve the target backend via the registry and call traits. Delete the hard-coded branches, the `engine/iceberg_glue.rs` bridge, and the direct `crate::standalone::iceberg::*` / `crate::standalone::lake::*` paths in `engine/`.

Each phase commits independently. `cargo test` + `sql-tests --suite ssb,tpc-h,tpc-ds,materialized-view,mv-on-iceberg,iceberg,ddl` must pass at every phase boundary.

**Tech Stack:** Rust, Cargo. Tests: `cargo test`, `cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests`.

**Pre-requisite reading (for the executing agent):**
- `CLAUDE.md` §4 Key Code Index — directory layout
- `src/connector/mod.rs` — `ScanConnector` trait + `ConnectorRegistry`, current pattern
- `src/standalone/engine/sqlparse/statement.rs::execute_create_table_statement` (line 279) — canonical example of the hard-coded backend branch this plan eliminates

**Conventions throughout this plan:**
- Line numbers reference files as of commit `ccb685d` (main at plan-write time).
- Every task ends with `cargo check` (incremental, ~5–20 s) before commit. `cargo test` runs at phase boundaries.
- Use the exact commit message shown — the resulting git log should read as a coherent refactor narrative.
- If `cargo check` fails in the middle of a task, **do not move on**; fix import / visibility issues before committing. A broken intermediate commit will poison `git bisect` for months.
- Keep each phase self-contained: never land Phase N+1 code ahead of Phase N's commit.

**Non-goals:**
- No behavior changes. If a test passes today, it must pass at each commit.
- No new features (no streaming `TableSource`, no JDBC `MvBackend`, no refactor of `connector/starrocks/lake/` internals). Those are future work enabled by this refactor, not included in it.
- No reshuffling of `connector/iceberg/{sink,metadata,schema}.rs` — those already live in the right place.
- No changes to `src/connector/starrocks/lake/` (the tablet primitives); only the `managed/` subdirectory is new.

---

## File Structure (End State)

```
src/connector/
├── mod.rs                                 # + CatalogBackend/TableSource/TableSink/MvBackend traits
├── iceberg/
│   ├── mod.rs                             # re-exports existing + new catalog module
│   ├── catalog/                           # NEW — migrated from src/standalone/iceberg/
│   │   ├── mod.rs
│   │   ├── registry.rs                    # was: src/standalone/iceberg/registry.rs
│   │   ├── hadoop_catalog.rs              # was: src/standalone/iceberg/hadoop_catalog.rs
│   │   ├── s3_storage.rs                  # was: src/standalone/iceberg/s3_storage.rs
│   │   ├── add_files.rs                   # was: src/standalone/iceberg/add_files.rs
│   │   └── backend.rs                     # NEW — CatalogBackend + TableSource + TableSink impls
│   ├── metadata.rs                        # unchanged
│   ├── schema.rs                          # unchanged
│   ├── sink.rs                            # unchanged
│   ├── jvm.rs                             # unchanged
│   ├── position_delete.rs                 # unchanged
│   └── state.rs                           # unchanged
└── starrocks/
    ├── mod.rs                             # re-exports existing + new managed module
    ├── lake/                              # unchanged tablet primitives
    ├── sink/                              # unchanged
    ├── scan/                              # unchanged
    ├── managed/                           # NEW — migrated from src/standalone/lake/
    │   ├── mod.rs
    │   ├── config.rs                      # was: src/standalone/lake/config.rs
    │   ├── catalog.rs                     # was: src/standalone/lake/catalog.rs
    │   ├── store.rs                       # was: src/standalone/lake/store.rs
    │   ├── ddl.rs                         # was: src/standalone/lake/ddl.rs
    │   ├── txn.rs                         # was: src/standalone/lake/txn.rs
    │   ├── erase.rs                       # was: src/standalone/lake/erase.rs
    │   ├── mv_ddl.rs                      # was: src/standalone/lake/mv_ddl.rs
    │   ├── mv_refresh.rs                  # was: src/standalone/lake/mv_refresh.rs
    │   ├── mv_shape.rs                    # was: src/standalone/lake/mv_shape.rs
    │   └── backend.rs                     # NEW — CatalogBackend + TableSource + TableSink + MvBackend impls
    └── ...unchanged files...

src/standalone/
├── mod.rs                                 # no more `pub(crate) mod iceberg / lake`
├── engine/
│   ├── mod.rs                             # session + state; no direct connector imports
│   ├── catalog.rs                         # unchanged (in-memory logical catalog — backend-neutral)
│   ├── name_resolve.rs                    # unchanged
│   ├── aggregate.rs                       # unchanged
│   ├── insert.rs                          # unchanged (shared literal-reorder helpers)
│   ├── parquet.rs                         # unchanged
│   ├── stream_load.rs                     # unchanged
│   ├── iceberg_glue.rs                    # DELETED in Phase 3
│   ├── ddl_flow.rs                        # NEW (Phase 3) — CREATE/DROP dispatch through CatalogBackend
│   ├── insert_flow.rs                     # NEW (Phase 3) — INSERT dispatch through TableSink
│   ├── mv_flow.rs                         # NEW (Phase 3) — MV dispatch through MvBackend
│   └── sqlparse/
│       ├── mod.rs                         # unchanged
│       ├── expr.rs                        # unchanged
│       ├── generate_series.rs             # unchanged
│       └── statement.rs                   # Phase 3: trimmed to parse→delegate calls
└── server/                                # unchanged
```

---

# Phase 1 — Mechanical Migration

Relocate every file under `src/standalone/iceberg/` and `src/standalone/lake/` to its new `src/connector/...` home. Update every import. `cargo test` + full sql-tests suite pass. Zero behavior change.

**Why this phase is valuable alone:** It resolves the "standalone is secretly hosting connector implementations" layering bug even if Phases 2–3 are never landed. The refactor becomes visible to anyone reading the tree, and future connector work has the right home.

---

## Task 1.1: Create empty scaffolding for `connector/iceberg/catalog/`

**Why:** Build the target skeleton first so every subsequent file-move is a pure content transfer (one concept per commit).

**Files:**
- Create: `src/connector/iceberg/catalog/mod.rs`

- [ ] **Step 1: Create the empty mod file**

Content of `src/connector/iceberg/catalog/mod.rs`:

```rust
//! Iceberg catalog registry, table loading, hadoop/S3 storage backends,
//! and `ADD FILES` support. Migrated here from `src/standalone/iceberg/`
//! during the standalone/connector decoupling refactor (2026-04-24).
//!
//! Files will be added incrementally by the next tasks in this plan.
```

Do NOT add `pub mod catalog;` to `src/connector/iceberg/mod.rs` yet. An unreferenced file is a no-op for the compiler.

- [ ] **Step 2: Run `cargo check`**

Run: `cargo check`
Expected: PASS, identical warnings to baseline.

- [ ] **Step 3: Commit**

```bash
git add src/connector/iceberg/catalog/mod.rs
git commit -m "refactor(connector): scaffold iceberg/catalog subdirectory"
```

---

## Task 1.2: Move `standalone/iceberg/s3_storage.rs`

**Why:** Leaf file with no local dependencies within the iceberg subtree. Start from leaves and work inward so each move compiles cleanly.

**Files:**
- Create: `src/connector/iceberg/catalog/s3_storage.rs` (content copied from source)
- Delete: `src/standalone/iceberg/s3_storage.rs`
- Modify: `src/standalone/iceberg/mod.rs` (remove `pub(crate) mod s3_storage;`)
- Modify: `src/standalone/iceberg/registry.rs` (change `use super::s3_storage::*` → `use crate::connector::iceberg::catalog::s3_storage::*`)
- Modify: `src/standalone/iceberg/hadoop_catalog.rs` (same rewrite if it imports `s3_storage`)
- Modify: `src/connector/iceberg/catalog/mod.rs` (add `pub(crate) mod s3_storage;`)

- [ ] **Step 1: Move file via `git mv`**

```bash
git mv src/standalone/iceberg/s3_storage.rs src/connector/iceberg/catalog/s3_storage.rs
```

- [ ] **Step 2: Register the moved file in the new parent**

Edit `src/connector/iceberg/catalog/mod.rs`, add after the doc comment:

```rust
pub(crate) mod s3_storage;
```

- [ ] **Step 3: Deregister from the old parent**

Edit `src/standalone/iceberg/mod.rs`, delete the line:

```rust
pub(crate) mod s3_storage;
```

- [ ] **Step 4: Rewrite imports of `s3_storage` throughout the codebase**

Run: `rg "super::s3_storage|standalone::iceberg::s3_storage|crate::standalone::iceberg::s3_storage" -l`

For every file in the result, rewrite:
- `use super::s3_storage::` → `use crate::connector::iceberg::catalog::s3_storage::`
- `crate::standalone::iceberg::s3_storage::` → `crate::connector::iceberg::catalog::s3_storage::`

- [ ] **Step 5: Run `cargo check`**

Run: `cargo check`
Expected: PASS.

If it fails with "unresolved import", grep the file mentioned in the error for any remaining old path and rewrite it.

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "refactor(connector): move iceberg s3_storage to connector/iceberg/catalog"
```

---

## Task 1.3: Move `standalone/iceberg/hadoop_catalog.rs`

**Files:**
- Rename: `src/standalone/iceberg/hadoop_catalog.rs` → `src/connector/iceberg/catalog/hadoop_catalog.rs`
- Modify: `src/connector/iceberg/catalog/mod.rs` (add `pub(crate) mod hadoop_catalog;`)
- Modify: `src/standalone/iceberg/mod.rs` (remove `pub(crate) mod hadoop_catalog;`)
- Modify: any file importing `standalone::iceberg::hadoop_catalog`

- [ ] **Step 1: `git mv` the file**

```bash
git mv src/standalone/iceberg/hadoop_catalog.rs src/connector/iceberg/catalog/hadoop_catalog.rs
```

- [ ] **Step 2: Update both `mod.rs` files as in Task 1.2 Steps 2–3**

- [ ] **Step 3: Rewrite imports**

Run: `rg "standalone::iceberg::hadoop_catalog|super::hadoop_catalog" -l`

Rewrite as Task 1.2 Step 4.

- [ ] **Step 4: `cargo check`**

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "refactor(connector): move iceberg hadoop_catalog to connector/iceberg/catalog"
```

---

## Task 1.4: Move `standalone/iceberg/add_files.rs`

**Files:**
- Rename: `src/standalone/iceberg/add_files.rs` → `src/connector/iceberg/catalog/add_files.rs`
- Modify: both `mod.rs` files and all importers

- [ ] **Step 1: `git mv` and update mod.rs files (pattern as Task 1.2)**

```bash
git mv src/standalone/iceberg/add_files.rs src/connector/iceberg/catalog/add_files.rs
```

Add to `src/connector/iceberg/catalog/mod.rs`:
```rust
pub(crate) mod add_files;
```
Remove from `src/standalone/iceberg/mod.rs`:
```rust
pub(crate) mod add_files;
```

- [ ] **Step 2: Rewrite imports**

Run: `rg "standalone::iceberg::add_files|super::add_files" -l`

Note a known site: `src/standalone/lake/config.rs:4` imports `parse_s3_path` from `super::super::iceberg::add_files`. Rewrite to `crate::connector::iceberg::catalog::add_files::parse_s3_path`.

- [ ] **Step 3: `cargo check`**

Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add -A
git commit -m "refactor(connector): move iceberg add_files to connector/iceberg/catalog"
```

---

## Task 1.5: Move `standalone/iceberg/registry.rs` (the big one)

**Why:** This is the 1985-line core of the iceberg subsystem and has the most import churn. Do it last among the iceberg files so all its intra-subtree references (`super::s3_storage`, `super::hadoop_catalog`, `super::add_files`) already resolve to their new locations.

**Files:**
- Rename: `src/standalone/iceberg/registry.rs` → `src/connector/iceberg/catalog/registry.rs`
- Modify: `src/connector/iceberg/catalog/mod.rs` — add module and re-exports
- Modify: `src/standalone/iceberg/mod.rs` — remove module (this file becomes effectively empty)
- Modify: every file importing `crate::standalone::iceberg::{registry or re-exported types}`

- [ ] **Step 1: `git mv` the file**

```bash
git mv src/standalone/iceberg/registry.rs src/connector/iceberg/catalog/registry.rs
```

- [ ] **Step 2: Fix intra-file imports inside the moved file**

Open `src/connector/iceberg/catalog/registry.rs`. Find and rewrite:

Line 35 (as of `ccb685d`):
```rust
use super::super::engine::catalog::{ColumnDef, normalize_identifier};
```

becomes:

```rust
use crate::standalone::engine::catalog::{ColumnDef, normalize_identifier};
```

Any `use super::{s3_storage, hadoop_catalog, add_files}` stays correct because they're now siblings in `catalog/`.

- [ ] **Step 3: Update `src/connector/iceberg/catalog/mod.rs`**

Replace the file with:

```rust
//! Iceberg catalog registry, table loading, hadoop/S3 storage backends,
//! and `ADD FILES` support.

pub(crate) mod add_files;
pub(crate) mod hadoop_catalog;
pub(crate) mod registry;
pub(crate) mod s3_storage;

// Re-export the same surface the previous `standalone::iceberg::*` module
// offered, so callers only need to update the module prefix, not each
// imported symbol.
pub(crate) use registry::{
    DataFileWithStats, IcebergAppendDelta, IcebergCatalogEntry, IcebergCatalogRegistry,
    IcebergLoadedTable, block_on_iceberg, build_hadoop_catalog, build_insert_batch,
    create_namespace, create_table, drop_namespace, drop_table, extract_data_files,
    extract_data_files_with_stats, insert_rows, list_tables, load_table, namespace_exists,
    plan_append_delta, register_existing_table,
};
```

- [ ] **Step 4: Replace `src/standalone/iceberg/mod.rs` with a re-export shim**

For Phase 1 safety, the old path keeps working via a re-export shim. This lets us rewrite call sites in a controlled, reviewable way rather than in one giant atomic commit.

Replace the file content with:

```rust
//! DEPRECATED in favor of `crate::connector::iceberg::catalog`. This file
//! exists only during the standalone/connector decoupling refactor so that
//! existing call sites keep compiling while imports are rewritten one
//! caller at a time. Will be deleted at the end of Phase 1 (Task 1.10).

pub(crate) use crate::connector::iceberg::catalog::{
    DataFileWithStats, IcebergAppendDelta, IcebergCatalogEntry, IcebergCatalogRegistry,
    IcebergLoadedTable, block_on_iceberg, build_hadoop_catalog, build_insert_batch,
    create_namespace, create_table, drop_namespace, drop_table, extract_data_files,
    extract_data_files_with_stats, insert_rows, list_tables, load_table, namespace_exists,
    plan_append_delta, register_existing_table,
};

pub(crate) mod add_files {
    pub(crate) use crate::connector::iceberg::catalog::add_files::*;
}
pub(crate) mod registry {
    pub(crate) use crate::connector::iceberg::catalog::registry::*;
}
```

- [ ] **Step 5: `cargo check`**

Expected: PASS. All existing `crate::standalone::iceberg::*` imports keep resolving via the shim.

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "refactor(connector): move iceberg registry to connector/iceberg/catalog

The old crate::standalone::iceberg path still compiles via a re-export
shim; call sites will be rewritten caller-by-caller in task 1.10."
```

---

## Task 1.6: Create empty scaffolding for `connector/starrocks/managed/`

**Files:**
- Create: `src/connector/starrocks/managed/mod.rs`

- [ ] **Step 1: Create the empty mod file**

Content of `src/connector/starrocks/managed/mod.rs`:

```rust
//! Managed-lake subsystem: config, catalog rebuild/reconcile, DDL,
//! transactional INSERT + publish, SQLite-backed metadata persistence,
//! and materialized-view lifecycle. Migrated here from
//! `src/standalone/lake/` during the standalone/connector decoupling
//! refactor (2026-04-24).
//!
//! Files will be added incrementally by the next tasks in this plan.
```

Do NOT add `pub mod managed;` to `src/connector/starrocks/mod.rs` yet.

- [ ] **Step 2: `cargo check`**

Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add src/connector/starrocks/managed/mod.rs
git commit -m "refactor(connector): scaffold starrocks/managed subdirectory"
```

---

## Task 1.7: Move `standalone/lake/{config,erase}.rs` (leaves)

**Why:** `config.rs` (45 lines) and `erase.rs` (399 lines) are the smallest leaf files in `lake/`. Move them first to validate the pattern before tackling the big files.

**Files:**
- Rename: `src/standalone/lake/config.rs` → `src/connector/starrocks/managed/config.rs`
- Rename: `src/standalone/lake/erase.rs` → `src/connector/starrocks/managed/erase.rs`

- [ ] **Step 1: `git mv` both files**

```bash
git mv src/standalone/lake/config.rs src/connector/starrocks/managed/config.rs
git mv src/standalone/lake/erase.rs src/connector/starrocks/managed/erase.rs
```

- [ ] **Step 2: Update `src/connector/starrocks/managed/mod.rs`**

Append:

```rust
pub(crate) mod config;
pub(crate) mod erase;
```

- [ ] **Step 3: Update `src/standalone/lake/mod.rs`**

Remove the `pub(crate) mod config;` and `pub(crate) mod erase;` lines. Leave the rest intact for now — later tasks will update this file progressively.

- [ ] **Step 4: Fix intra-file imports inside moved files**

In `src/connector/starrocks/managed/config.rs` line 4:
```rust
use super::super::iceberg::add_files::parse_s3_path;
```
rewrite to:
```rust
use crate::connector::iceberg::catalog::add_files::parse_s3_path;
```

In `src/connector/starrocks/managed/erase.rs`, any `use super::{store, catalog, txn}` still needs to resolve — those files haven't moved yet, so they're still siblings through the old path. Check if the file uses `super::`:

Run: `rg "^use (super|crate::standalone::lake)" src/connector/starrocks/managed/erase.rs`

For each `super::X` that refers to a lake file NOT yet moved (e.g. `super::store`), rewrite to the absolute path it will eventually have: `crate::connector::starrocks::managed::store` (compile will fail until Task 1.9, which is fine — we do this rewrite now to avoid double-touching).

Wait — actually the safest policy is: rewrite imports only for files that HAVE moved. For not-yet-moved targets, keep the old path `crate::standalone::lake::store::...` (this still works because `standalone/lake/mod.rs` hasn't been torn down). This avoids breaking the build mid-phase.

So: in `erase.rs`, rewrite any `super::X` to `crate::standalone::lake::X` for files not yet moved. After all lake files are moved (Task 1.9), a cleanup step rewrites them to `crate::connector::starrocks::managed::X`.

- [ ] **Step 5: `cargo check`**

Expected: PASS.

- [ ] **Step 6: Rewrite external imports of the moved files**

Run: `rg "standalone::lake::config|standalone::lake::erase" -l`

Rewrite to `connector::starrocks::managed::config` / `...::erase`.

Note: `src/standalone/lake/mod.rs` re-exports `ManagedLakeConfig` — we'll update that in Task 1.11 (the shim).

- [ ] **Step 7: `cargo check`**

Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add -A
git commit -m "refactor(connector): move managed-lake config/erase to connector/starrocks/managed"
```

---

## Task 1.8: Move `standalone/lake/{catalog,store,ddl,txn}.rs`

**Why:** These are the core managed-lake files (1155 + 3563 + 1586 + 1432 lines). They have heavy interdependencies but only on each other and on `connector::starrocks::lake::*` (already correctly placed) and on `standalone::iceberg::*` (now a shim → still resolves). Move them together in one task because splitting risks ping-pong imports.

**Files:**
- Rename: `src/standalone/lake/{catalog,store,ddl,txn}.rs` → `src/connector/starrocks/managed/*.rs`

- [ ] **Step 1: `git mv` all four files**

```bash
git mv src/standalone/lake/catalog.rs src/connector/starrocks/managed/catalog.rs
git mv src/standalone/lake/store.rs   src/connector/starrocks/managed/store.rs
git mv src/standalone/lake/ddl.rs     src/connector/starrocks/managed/ddl.rs
git mv src/standalone/lake/txn.rs     src/connector/starrocks/managed/txn.rs
```

- [ ] **Step 2: Update `src/connector/starrocks/managed/mod.rs`**

Append:

```rust
pub(crate) mod catalog;
pub(crate) mod ddl;
pub(crate) mod store;
pub(crate) mod txn;

pub(crate) use catalog::{
    ManagedLakeCatalog, reconcile_on_open, register_managed_table_in_catalog,
    register_managed_tables_in_catalog, runtime_registered,
};
pub(crate) use config::ManagedLakeConfig;
```

(The `pub(crate) use config::ManagedLakeConfig;` moves from `standalone/lake/mod.rs` to here.)

- [ ] **Step 3: Update `src/standalone/lake/mod.rs`**

Remove the `pub(crate) mod {catalog,store,ddl,txn};` lines and the `pub(crate) use catalog::{...}` block. Leave `mv_ddl`, `mv_refresh`, `mv_shape` in place (next task handles them).

- [ ] **Step 4: Rewrite imports inside each moved file**

For each of `catalog.rs`, `store.rs`, `ddl.rs`, `txn.rs`:

- `super::config` → stays (config is a sibling in the new location)
- `super::store` / `super::catalog` / `super::ddl` / `super::txn` → stay (siblings)
- `super::super::engine::catalog::*` → rewrite to `crate::standalone::engine::catalog::*`
- `super::super::engine::{X, Y}` → rewrite to `crate::standalone::engine::{X, Y}`
- `super::super::iceberg::*` → rewrite to `crate::connector::iceberg::catalog::*`
- `super::mv_ddl`, `super::mv_refresh`, `super::mv_shape` → keep as-is (siblings remain in standalone/lake for now; will re-resolve when mv_* moves in Task 1.9)

Run `rg "^use " src/connector/starrocks/managed/{catalog,store,ddl,txn}.rs` and fix the above patterns.

- [ ] **Step 5: Rewrite external imports**

Run: `rg "standalone::lake::(catalog|store|ddl|txn)" -l`

For each result file, rewrite the module prefix from `standalone::lake` to `connector::starrocks::managed`.

Known sites:
- `src/standalone/engine/mod.rs:26` — `use super::lake::store::{MetadataSnapshot, SqliteMetadataStore, StoredIcebergTable};`
- `src/standalone/engine/mod.rs:27` — `use super::lake::{ManagedLakeCatalog, ...};`
- `src/standalone/engine/sqlparse/statement.rs` — several imports
- `src/standalone/lake/mv_ddl.rs`, `mv_refresh.rs`, `mv_shape.rs` — use `super::{catalog, store, ddl, txn}` — leave these as `super::` (siblings from `standalone/lake/` perspective, still resolve via shim until Task 1.9 moves them too)

Wait — this is the subtle part. After moving catalog/store/ddl/txn but NOT mv_*, the `mv_*.rs` files in `standalone/lake/` that write `super::store::X` will fail to resolve because `store.rs` is no longer a sibling.

Fix: in the same task, rewrite `mv_ddl.rs`, `mv_refresh.rs`, `mv_shape.rs` to import from `crate::connector::starrocks::managed::{store, catalog, ddl, txn}` instead of `super::`. These files haven't moved yet, but the absolute-path imports will keep working both before and after Task 1.9's move.

- [ ] **Step 6: `cargo check`**

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add -A
git commit -m "refactor(connector): move managed-lake {catalog,store,ddl,txn} to connector/starrocks/managed"
```

---

## Task 1.9: Move `standalone/lake/mv_{ddl,refresh,shape}.rs`

**Why:** The materialized-view orchestration files. They depend on `engine` (for `StandaloneState`, `record_batch_to_chunk`, `register_iceberg_tables_for_query`) and on the now-migrated managed/ files. After this move, `src/standalone/lake/mod.rs` contains only re-export shims.

**Files:**
- Rename: `src/standalone/lake/mv_ddl.rs` → `src/connector/starrocks/managed/mv_ddl.rs`
- Rename: `src/standalone/lake/mv_refresh.rs` → `src/connector/starrocks/managed/mv_refresh.rs`
- Rename: `src/standalone/lake/mv_shape.rs` → `src/connector/starrocks/managed/mv_shape.rs`

- [ ] **Step 1: `git mv` all three files**

```bash
git mv src/standalone/lake/mv_ddl.rs     src/connector/starrocks/managed/mv_ddl.rs
git mv src/standalone/lake/mv_refresh.rs src/connector/starrocks/managed/mv_refresh.rs
git mv src/standalone/lake/mv_shape.rs   src/connector/starrocks/managed/mv_shape.rs
```

- [ ] **Step 2: Update `src/connector/starrocks/managed/mod.rs`**

Append:

```rust
pub(crate) mod mv_ddl;
pub(crate) mod mv_refresh;
pub(crate) mod mv_shape;
```

- [ ] **Step 3: Replace `src/standalone/lake/mod.rs` with a full re-export shim**

```rust
//! DEPRECATED in favor of `crate::connector::starrocks::managed`. This
//! shim exists only during the standalone/connector decoupling refactor
//! so existing call sites keep compiling while imports are rewritten one
//! caller at a time. Will be deleted at the end of Phase 1 (Task 1.10).

pub(crate) use crate::connector::starrocks::managed::{
    ManagedLakeCatalog, ManagedLakeConfig, reconcile_on_open, register_managed_table_in_catalog,
    register_managed_tables_in_catalog, runtime_registered,
};

pub(crate) mod catalog {
    pub(crate) use crate::connector::starrocks::managed::catalog::*;
}
pub(crate) mod config {
    pub(crate) use crate::connector::starrocks::managed::config::*;
}
pub(crate) mod ddl {
    pub(crate) use crate::connector::starrocks::managed::ddl::*;
}
pub(crate) mod erase {
    pub(crate) use crate::connector::starrocks::managed::erase::*;
}
pub(crate) mod mv_ddl {
    pub(crate) use crate::connector::starrocks::managed::mv_ddl::*;
}
pub(crate) mod mv_refresh {
    pub(crate) use crate::connector::starrocks::managed::mv_refresh::*;
}
pub(crate) mod mv_shape {
    pub(crate) use crate::connector::starrocks::managed::mv_shape::*;
}
pub(crate) mod store {
    pub(crate) use crate::connector::starrocks::managed::store::*;
}
pub(crate) mod txn {
    pub(crate) use crate::connector::starrocks::managed::txn::*;
}
```

- [ ] **Step 4: Rewrite imports inside the moved mv_*.rs files**

In `mv_ddl.rs`, `mv_refresh.rs`, `mv_shape.rs`:

- `super::catalog::X` / `super::store::X` / `super::ddl::X` / `super::txn::X` → keep as `super::` (all now siblings in `connector/starrocks/managed/`)
- `super::super::engine::{X}` → rewrite to `crate::standalone::engine::{X}`
- `crate::standalone::iceberg::X` → rewrite to `crate::connector::iceberg::catalog::X`
- `use crate::connector::starrocks::managed::{catalog, store, ddl, txn}::X` (introduced in Task 1.8) → rewrite back to `super::{catalog, store, ddl, txn}::X` for consistency

- [ ] **Step 5: `cargo check`**

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "refactor(connector): move managed-lake mv_{ddl,refresh,shape} to connector/starrocks/managed"
```

---

## Task 1.10: Rewrite all external callers to use new paths; delete shims

**Why:** The shims in `src/standalone/{iceberg,lake}/mod.rs` are temporary to keep Phases green. Now rewrite every caller to use the canonical `crate::connector::...` path, then delete the shims.

**Files:**
- Modify: every file importing `crate::standalone::iceberg::*` or `crate::standalone::lake::*`
- Delete: `src/standalone/iceberg/mod.rs` (entire directory)
- Delete: `src/standalone/lake/mod.rs` (entire directory)
- Modify: `src/standalone/mod.rs` — remove `pub(crate) mod iceberg; pub(crate) mod lake;`

- [ ] **Step 1: Enumerate call sites**

Run: `rg "crate::standalone::(iceberg|lake)" -l`

Expected set (as of `ccb685d`):
- `src/standalone/mod.rs`
- `src/standalone/engine/mod.rs`
- `src/standalone/engine/iceberg_glue.rs`
- `src/standalone/engine/sqlparse/statement.rs`
- `src/connector/starrocks/managed/mv_ddl.rs` (from Task 1.9)
- `src/connector/starrocks/managed/mv_refresh.rs` (from Task 1.9)

- [ ] **Step 2: Rewrite each file**

For each file in Step 1's list, apply these substitutions:

- `crate::standalone::iceberg::` → `crate::connector::iceberg::catalog::`
- `crate::standalone::lake::` → `crate::connector::starrocks::managed::`
- `use super::iceberg::` (inside `src/standalone/engine/mod.rs` etc.) → `use crate::connector::iceberg::catalog::`
- `use super::lake::` → `use crate::connector::starrocks::managed::`

- [ ] **Step 3: Delete the shim directories**

```bash
rm -r src/standalone/iceberg src/standalone/lake
```

- [ ] **Step 4: Update `src/standalone/mod.rs`**

Remove the lines:

```rust
pub(crate) mod iceberg;
pub(crate) mod lake;
```

The remaining module declarations (`engine`, `server`) stay.

- [ ] **Step 5: `cargo check`**

Expected: PASS. If it fails with "unresolved module", grep once more for any missed `standalone::iceberg` or `standalone::lake` reference.

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "refactor(connector): delete standalone/{iceberg,lake} shims; callers use connector paths"
```

---

## Task 1.11 — Phase 1 Verification Checkpoint

- [ ] **Step 1: `cargo fmt`**

Run: `cargo fmt`
Expected: no diff (or trivial import-group reordering).

- [ ] **Step 2: `cargo clippy`**

Run: `cargo clippy --all-targets -- -D warnings`
Expected: PASS (same warning count as baseline).

- [ ] **Step 3: `cargo test`**

Run: `cargo test`
Expected: ALL pass. Debug build, ~3–5 min.

- [ ] **Step 4: Start standalone-server for SQL regression**

Terminal 1:
```bash
NO_PROXY=127.0.0.1,localhost cargo run --release -- standalone-server --port 9030
```

Wait for "listening on 127.0.0.1:9030".

- [ ] **Step 5: Run SQL regression suites**

Terminal 2:
```bash
cargo run --release --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --suite ssb,tpc-h,tpc-ds,ddl,iceberg,materialized-view,mv-on-iceberg --mode verify
```

Expected: ALL green.

- [ ] **Step 6: Commit verification (no file changes)**

If Step 3 / 5 turned up a regression, fix it inline as an amend-adjacent follow-up commit (not an amend — new commit). Otherwise no commit needed; Phase 1 done.

**Phase 1 complete.** `cargo test` and sql-tests pass. `src/standalone/` no longer contains connector code. Safe to ship as a standalone PR if desired.

---

# Phase 2 — Trait Introduction

Define `CatalogBackend`, `TableSource`, `TableSink`, `MvBackend`. Implement them as thin adapters over the migrated iceberg-catalog and managed-lake modules. Register implementations in `ConnectorRegistry`. **Old call sites in `standalone/engine/` remain unchanged.**

The new traits are exercised only by a dedicated unit test; they are not yet called from the hot paths. Phase 3 is where the engine layer starts using them.

---

## Task 2.1: Define trait scaffolding in `src/connector/mod.rs`

**Files:**
- Modify: `src/connector/mod.rs`
- Create: `src/connector/backend.rs` (new file for the trait definitions, to keep `mod.rs` compact)

- [ ] **Step 1: Create `src/connector/backend.rs`**

```rust
//! Connector-agnostic backend traits. Each trait represents one axis of
//! capability (catalog admin, table scan-side source, table write-side sink,
//! materialized-view lifecycle). A connector implements whichever subset
//! applies to it.
//!
//! The traits live here rather than in each per-connector mod.rs so callers
//! can program against `dyn CatalogBackend` without knowing which concrete
//! connector fulfils the request.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;

use crate::runtime::query_result::QueryResult;
use crate::sql::catalog::{ColumnDef, TableDef};
use crate::sql::parser::ast::{
    CreateMaterializedViewStmt, DropMaterializedViewStmt, InsertSource, Literal, ObjectName,
    RefreshMaterializedViewStmt, ShowMaterializedViewsStmt, SqlType, TableColumnDef, TableKeyDesc,
};

/// Request to create a table. Unified shape across all catalog backends;
/// backends ignore fields that don't apply to them (e.g. `bucket_count` is
/// managed-lake-only).
#[derive(Clone, Debug)]
pub struct CreateTableRequest {
    pub catalog: String,
    pub namespace: String,
    pub table: String,
    pub columns: Vec<TableColumnDef>,
    pub key_desc: Option<TableKeyDesc>,
    pub bucket_count: Option<u32>,
    pub properties: Vec<(String, String)>,
}

/// Resolved table metadata returned by `CatalogBackend::load_table`. This is
/// the subset of table shape the engine layer needs in order to plan INSERTs
/// and to register the table with the in-memory logical catalog.
#[derive(Clone, Debug)]
pub struct ResolvedTable {
    pub catalog: String,
    pub namespace: String,
    pub table: String,
    pub columns: Vec<ColumnDef>,
    pub logical_types: HashMap<String, SqlType>,
    pub key_desc: Option<TableKeyDesc>,
}

/// Catalog-plane operations: create/drop namespace, create/drop/load/list
/// tables. Implemented once per catalog type (iceberg, managed-lake, ...).
pub trait CatalogBackend: Send + Sync {
    fn name(&self) -> &'static str;

    fn namespace_exists(&self, catalog: &str, namespace: &str) -> Result<bool, String>;
    fn create_namespace(&self, catalog: &str, namespace: &str) -> Result<(), String>;
    fn drop_namespace(&self, catalog: &str, namespace: &str, force: bool) -> Result<(), String>;

    fn create_table(&self, req: CreateTableRequest) -> Result<(), String>;
    fn drop_table(
        &self,
        catalog: &str,
        namespace: &str,
        table: &str,
        if_exists: bool,
    ) -> Result<(), String>;
    fn load_table(
        &self,
        catalog: &str,
        namespace: &str,
        table: &str,
    ) -> Result<ResolvedTable, String>;
    fn list_tables(&self, catalog: &str, namespace: &str) -> Result<Vec<String>, String>;
}

/// Scan-side: materialize a table into Arrow `RecordBatch`es. For now this
/// is eager (whole-table load) because both current backends use it to
/// register iceberg bases for MV refresh and for small-table quick paths.
/// A streaming variant is a future extension.
pub trait TableSource: Send + Sync {
    fn name(&self) -> &'static str;
    fn load_full(&self, table: &ResolvedTable) -> Result<RecordBatch, String>;

    /// Build a `TableDef` suitable for registration in the in-memory logical
    /// catalog. Different backends pick different `TableStorage` variants
    /// (LocalParquetFile / S3ParquetFiles / ManagedLake).
    fn build_table_def(&self, table: &ResolvedTable) -> Result<TableDef, String>;
}

/// Write-side: append rows or RecordBatches to a table. The INSERT
/// orchestration layer (`insert_flow.rs`, Phase 3) chooses between the two
/// depending on whether the source is literal VALUES or a pipeline result.
pub trait TableSink: Send + Sync {
    fn name(&self) -> &'static str;
    fn append_rows(
        &self,
        table: &ResolvedTable,
        rows: &[Vec<Literal>],
    ) -> Result<(), String>;
    fn append_batch(
        &self,
        table: &ResolvedTable,
        batch: RecordBatch,
    ) -> Result<(), String>;

    /// Whether this sink supports INSERT SELECT from a pipeline plan. If
    /// false, the engine falls back to VALUES / generate_series fast paths
    /// only. (Iceberg sink is pipeline-capable via `IcebergTableSinkFactory`;
    /// the managed-lake sink goes through `txn::insert_into_managed_lake_table`.)
    fn supports_pipeline_insert(&self) -> bool;
}

/// Materialized-view backend: CREATE / DROP / REFRESH / SHOW. Today only
/// managed-lake implements this; iceberg returns `unsupported`. Future
/// backends (e.g. iceberg-as-MV-target) plug in here.
pub trait MvBackend: Send + Sync {
    fn name(&self) -> &'static str;
    fn create_mv(&self, stmt: &CreateMaterializedViewStmt, current_database: &str) -> Result<(), String>;
    fn drop_mv(&self, stmt: &DropMaterializedViewStmt, current_database: &str) -> Result<(), String>;
    fn refresh_mv(
        &self,
        stmt: &RefreshMaterializedViewStmt,
        current_database: &str,
    ) -> Result<(), String>;
    fn list_mvs(&self, stmt: &ShowMaterializedViewsStmt) -> Result<QueryResult, String>;
    fn supports_incremental_refresh(&self) -> bool;
}

/// Trivial "null object" MV backend used by connectors that don't support
/// materialized views. Every method returns a typed error.
pub struct NoMvBackend(pub &'static str);
impl MvBackend for NoMvBackend {
    fn name(&self) -> &'static str { self.0 }
    fn create_mv(&self, _: &CreateMaterializedViewStmt, _: &str) -> Result<(), String> {
        Err(format!("connector {} does not support materialized views", self.0))
    }
    fn drop_mv(&self, _: &DropMaterializedViewStmt, _: &str) -> Result<(), String> {
        Err(format!("connector {} does not support materialized views", self.0))
    }
    fn refresh_mv(&self, _: &RefreshMaterializedViewStmt, _: &str) -> Result<(), String> {
        Err(format!("connector {} does not support materialized views", self.0))
    }
    fn list_mvs(&self, _: &ShowMaterializedViewsStmt) -> Result<QueryResult, String> {
        Err(format!("connector {} does not support materialized views", self.0))
    }
    fn supports_incremental_refresh(&self) -> bool { false }
}
```

- [ ] **Step 2: Wire `backend.rs` into `mod.rs`**

In `src/connector/mod.rs`, add after the other module declarations:

```rust
pub mod backend;

pub use backend::{
    CatalogBackend, CreateTableRequest, MvBackend, NoMvBackend, ResolvedTable, TableSink,
    TableSource,
};
```

- [ ] **Step 3: `cargo check`**

Expected: PASS. (No trait is implemented yet, but the definitions should compile.)

- [ ] **Step 4: Commit**

```bash
git add -A
git commit -m "feat(connector): add CatalogBackend/TableSource/TableSink/MvBackend traits"
```

---

## Task 2.2: Extend `ConnectorRegistry` to hold backend maps

**Files:**
- Modify: `src/connector/mod.rs` — extend `ConnectorRegistry` struct + registration/lookup methods

- [ ] **Step 1: Extend the struct**

In `src/connector/mod.rs`, change `ConnectorRegistry` to:

```rust
#[derive(Clone)]
pub struct ConnectorRegistry {
    scan_connectors: HashMap<&'static str, Arc<dyn ScanConnector>>,
    catalog_backends: HashMap<&'static str, Arc<dyn CatalogBackend>>,
    table_sources: HashMap<&'static str, Arc<dyn TableSource>>,
    table_sinks: HashMap<&'static str, Arc<dyn TableSink>>,
    mv_backends: HashMap<&'static str, Arc<dyn MvBackend>>,
}
```

Update `ConnectorRegistry::new` to initialize all five maps empty.

- [ ] **Step 2: Add registration and lookup methods**

In the `impl ConnectorRegistry` block, after `register_scan_connector`, add:

```rust
pub fn register_catalog_backend(&mut self, backend: Arc<dyn CatalogBackend>) {
    self.catalog_backends.insert(backend.name(), backend);
}
pub fn catalog_backend(&self, name: &str) -> Result<Arc<dyn CatalogBackend>, String> {
    self.catalog_backends
        .get(name)
        .cloned()
        .ok_or_else(|| format!("unknown catalog backend: {name}"))
}

pub fn register_table_source(&mut self, src: Arc<dyn TableSource>) {
    self.table_sources.insert(src.name(), src);
}
pub fn table_source(&self, name: &str) -> Result<Arc<dyn TableSource>, String> {
    self.table_sources
        .get(name)
        .cloned()
        .ok_or_else(|| format!("unknown table source: {name}"))
}

pub fn register_table_sink(&mut self, sink: Arc<dyn TableSink>) {
    self.table_sinks.insert(sink.name(), sink);
}
pub fn table_sink(&self, name: &str) -> Result<Arc<dyn TableSink>, String> {
    self.table_sinks
        .get(name)
        .cloned()
        .ok_or_else(|| format!("unknown table sink: {name}"))
}

pub fn register_mv_backend(&mut self, mv: Arc<dyn MvBackend>) {
    self.mv_backends.insert(mv.name(), mv);
}
pub fn mv_backend(&self, name: &str) -> Result<Arc<dyn MvBackend>, String> {
    self.mv_backends
        .get(name)
        .cloned()
        .ok_or_else(|| format!("unknown MV backend: {name}"))
}
```

- [ ] **Step 3: Update `Default` impl to leave backend maps empty**

`ConnectorRegistry::default` still wires scan connectors the same way. Leave it alone — per-backend registration happens explicitly where the connector implementations live (Tasks 2.3–2.8).

- [ ] **Step 4: Update the `Debug` impl**

```rust
impl std::fmt::Debug for ConnectorRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut scan: Vec<_> = self.scan_connectors.keys().copied().collect(); scan.sort();
        let mut catalog: Vec<_> = self.catalog_backends.keys().copied().collect(); catalog.sort();
        let mut source: Vec<_> = self.table_sources.keys().copied().collect(); source.sort();
        let mut sink: Vec<_> = self.table_sinks.keys().copied().collect(); sink.sort();
        let mut mv: Vec<_> = self.mv_backends.keys().copied().collect(); mv.sort();
        f.debug_struct("ConnectorRegistry")
            .field("scan_connectors", &scan)
            .field("catalog_backends", &catalog)
            .field("table_sources", &source)
            .field("table_sinks", &sink)
            .field("mv_backends", &mv)
            .finish()
    }
}
```

- [ ] **Step 5: `cargo check`**

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "feat(connector): extend ConnectorRegistry with CatalogBackend/TableSource/TableSink/MvBackend maps"
```

---

## Task 2.3: Implement `CatalogBackend` for iceberg

**Files:**
- Create: `src/connector/iceberg/catalog/backend.rs`
- Modify: `src/connector/iceberg/catalog/mod.rs` (add module + export)

**Context:** The iceberg `CatalogBackend` wraps the existing free functions `create_namespace`, `create_table`, `drop_namespace`, `drop_table`, `load_table`, `list_tables`, `namespace_exists`. It needs access to an `IcebergCatalogEntry`, which lives in a `StandaloneState`-owned `IcebergCatalogRegistry`. The implementation takes an `Arc<RwLock<IcebergCatalogRegistry>>` in its constructor.

- [ ] **Step 1: Create `backend.rs`**

```rust
//! `CatalogBackend` / `TableSource` / `TableSink` implementations for
//! iceberg, wrapping the free functions in `registry.rs`.

use std::sync::{Arc, RwLock};

use arrow::record_batch::RecordBatch;

use crate::connector::backend::{
    CatalogBackend, CreateTableRequest, ResolvedTable, TableSink, TableSource,
};
use crate::sql::catalog::{ColumnDef, TableDef, TableStorage};
use crate::sql::parser::ast::Literal;

use super::registry::{
    IcebergCatalogRegistry, create_namespace as reg_create_namespace, create_table as reg_create_table,
    drop_namespace as reg_drop_namespace, drop_table as reg_drop_table,
    insert_rows as reg_insert_rows, list_tables as reg_list_tables, load_table as reg_load_table,
    namespace_exists as reg_namespace_exists,
};

pub struct IcebergCatalogBackend {
    registry: Arc<RwLock<IcebergCatalogRegistry>>,
}

impl IcebergCatalogBackend {
    pub fn new(registry: Arc<RwLock<IcebergCatalogRegistry>>) -> Self {
        Self { registry }
    }
    fn entry(&self, catalog: &str) -> Result<super::registry::IcebergCatalogEntry, String> {
        let guard = self.registry.read().expect("iceberg catalog read lock");
        guard.get(catalog).cloned()
    }
}

impl CatalogBackend for IcebergCatalogBackend {
    fn name(&self) -> &'static str { "iceberg" }

    fn namespace_exists(&self, catalog: &str, namespace: &str) -> Result<bool, String> {
        reg_namespace_exists(&self.entry(catalog)?, namespace)
    }
    fn create_namespace(&self, catalog: &str, namespace: &str) -> Result<(), String> {
        reg_create_namespace(&self.entry(catalog)?, namespace)
    }
    fn drop_namespace(&self, catalog: &str, namespace: &str, _force: bool) -> Result<(), String> {
        // Iceberg drop-namespace semantics today: callers must DROP tables
        // individually before dropping the namespace; `force` is a
        // managed-lake-only flag. Phase 3 will relocate the cascading
        // table-drop loop from engine/sqlparse/statement.rs into the engine
        // layer, not here.
        reg_drop_namespace(&self.entry(catalog)?, namespace)
    }

    fn create_table(&self, req: CreateTableRequest) -> Result<(), String> {
        let entry = self.entry(&req.catalog)?;
        reg_create_table(
            &entry,
            &req.namespace,
            &req.table,
            &req.columns,
            req.key_desc.as_ref(),
            &req.properties,
        )
    }
    fn drop_table(
        &self,
        catalog: &str,
        namespace: &str,
        table: &str,
        _if_exists: bool,
    ) -> Result<(), String> {
        // `if_exists` is handled by the engine layer (it checks via
        // `namespace_exists` + `list_tables` before calling). This mirrors
        // current behavior in execute_drop_table_statement.
        reg_drop_table(&self.entry(catalog)?, namespace, table)
    }
    fn load_table(
        &self,
        catalog: &str,
        namespace: &str,
        table: &str,
    ) -> Result<ResolvedTable, String> {
        let loaded = reg_load_table(&self.entry(catalog)?, namespace, table)?;
        Ok(ResolvedTable {
            catalog: catalog.to_string(),
            namespace: namespace.to_string(),
            table: table.to_string(),
            columns: loaded.columns,
            logical_types: loaded.logical_types,
            key_desc: loaded.key_desc,
        })
    }
    fn list_tables(&self, catalog: &str, namespace: &str) -> Result<Vec<String>, String> {
        reg_list_tables(&self.entry(catalog)?, namespace)
    }
}

pub struct IcebergTableSource {
    registry: Arc<RwLock<IcebergCatalogRegistry>>,
}
impl IcebergTableSource {
    pub fn new(registry: Arc<RwLock<IcebergCatalogRegistry>>) -> Self { Self { registry } }
}
impl TableSource for IcebergTableSource {
    fn name(&self) -> &'static str { "iceberg" }

    fn load_full(&self, table: &ResolvedTable) -> Result<RecordBatch, String> {
        let guard = self.registry.read().expect("iceberg catalog read lock");
        let entry = guard.get(&table.catalog)?;
        let loaded = reg_load_table(&entry, &table.namespace, &table.table)?;
        crate::standalone::engine::iceberg_glue::load_full_iceberg_batch(&loaded)
    }

    fn build_table_def(&self, table: &ResolvedTable) -> Result<TableDef, String> {
        // Delegate to the existing engine-side helper until Phase 3 moves
        // it onto the trait.
        let guard = self.registry.read().expect("iceberg catalog read lock");
        let entry = guard.get(&table.catalog)?;
        let loaded = reg_load_table(&entry, &table.namespace, &table.table)?;
        let data_files = super::registry::extract_data_files(&loaded.table)?;
        crate::standalone::engine::build_iceberg_table_def_with_files_public(
            &entry,
            &table.namespace,
            &table.table,
            loaded,
            data_files,
        )
    }
}

pub struct IcebergTableSink {
    registry: Arc<RwLock<IcebergCatalogRegistry>>,
}
impl IcebergTableSink {
    pub fn new(registry: Arc<RwLock<IcebergCatalogRegistry>>) -> Self { Self { registry } }
}
impl TableSink for IcebergTableSink {
    fn name(&self) -> &'static str { "iceberg" }

    fn append_rows(&self, table: &ResolvedTable, rows: &[Vec<Literal>]) -> Result<(), String> {
        let guard = self.registry.read().expect("iceberg catalog read lock");
        let entry = guard.get(&table.catalog)?;
        reg_insert_rows(&entry, &table.namespace, &table.table, rows)
    }
    fn append_batch(&self, _: &ResolvedTable, _: RecordBatch) -> Result<(), String> {
        Err("iceberg append_batch: pipeline INSERT SELECT should use IcebergTableSinkFactory via the exec layer, not this trait".into())
    }
    fn supports_pipeline_insert(&self) -> bool {
        // Pipeline INSERT for iceberg goes through IcebergTableSinkFactory
        // in lower/fragment.rs, not this trait. Phase 3's insert_flow will
        // dispatch on `supports_pipeline_insert()` and route to the factory.
        true
    }
}
```

- [ ] **Step 2: Export the new types from `src/connector/iceberg/catalog/mod.rs`**

Append:

```rust
pub(crate) mod backend;
pub(crate) use backend::{IcebergCatalogBackend, IcebergTableSink, IcebergTableSource};
```

- [ ] **Step 3: Make `build_iceberg_table_def_with_files` callable from outside engine**

Open `src/standalone/engine/mod.rs`. The existing `fn build_iceberg_table_def_with_files` (line 804) is private. Add a thin public wrapper nearby:

```rust
pub(crate) fn build_iceberg_table_def_with_files_public(
    entry: &crate::connector::iceberg::catalog::IcebergCatalogEntry,
    namespace: &str,
    table_name: &str,
    loaded: crate::connector::iceberg::catalog::IcebergLoadedTable,
    data_files: Vec<(String, i64, Option<i64>)>,
) -> Result<crate::sql::catalog::TableDef, String> {
    build_iceberg_table_def_with_files(entry, namespace, table_name, loaded, data_files)
}
```

Phase 3 will relocate the function body into `insert_flow.rs` / `mv_flow.rs` and delete this wrapper.

- [ ] **Step 4: `cargo check`**

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat(connector): implement CatalogBackend/TableSource/TableSink for iceberg"
```

---

## Task 2.4: Implement `CatalogBackend`, `TableSource`, `TableSink`, `MvBackend` for managed-lake

**Files:**
- Create: `src/connector/starrocks/managed/backend.rs`
- Modify: `src/connector/starrocks/managed/mod.rs`

- [ ] **Step 1: Create `backend.rs`**

```rust
//! `CatalogBackend` / `TableSource` / `TableSink` / `MvBackend`
//! implementations for managed-lake, wrapping `catalog.rs`, `ddl.rs`,
//! `txn.rs`, `mv_ddl.rs`, `mv_refresh.rs`.

use std::sync::Arc;

use arrow::record_batch::RecordBatch;

use crate::connector::backend::{
    CatalogBackend, CreateTableRequest, MvBackend, ResolvedTable, TableSink, TableSource,
};
use crate::runtime::query_result::QueryResult;
use crate::sql::catalog::{ColumnDef, TableDef, TableStorage};
use crate::sql::parser::ast::{
    CreateMaterializedViewStmt, DropMaterializedViewStmt, Literal, ObjectName,
    RefreshMaterializedViewStmt, ShowMaterializedViewsStmt,
};
use crate::standalone::engine::StandaloneState;

use super::catalog::ManagedLakeCatalog;

/// Managed-lake backend is parameterized by the owning `StandaloneState`:
/// almost every write/DDL call needs access to `state.managed_lake`,
/// `state.metadata_store`, etc. We store a weak handle so we don't extend
/// the state's lifetime beyond the process.
pub struct ManagedLakeBackend {
    state: std::sync::Weak<StandaloneState>,
}
impl ManagedLakeBackend {
    pub fn new(state: &Arc<StandaloneState>) -> Self {
        Self { state: Arc::downgrade(state) }
    }
    fn state(&self) -> Result<Arc<StandaloneState>, String> {
        self.state.upgrade().ok_or_else(|| "standalone state dropped".to_string())
    }
}

impl CatalogBackend for ManagedLakeBackend {
    fn name(&self) -> &'static str { "managed" }

    fn namespace_exists(&self, _catalog: &str, database: &str) -> Result<bool, String> {
        let state = self.state()?;
        let logical = state.catalog.read().expect("standalone catalog read lock");
        logical.database_exists(database)
    }
    fn create_namespace(&self, _catalog: &str, database: &str) -> Result<(), String> {
        let state = self.state()?;
        let mut logical = state.catalog.write().expect("standalone catalog write lock");
        logical.create_database(database)
    }
    fn drop_namespace(&self, _catalog: &str, database: &str, force: bool) -> Result<(), String> {
        let state = self.state()?;
        if force {
            // Cascade-drop managed tables first (mirrors current
            // sqlparse/statement.rs:370 behavior). Phase 3 hoists this
            // loop out of the backend and into engine/ddl_flow.rs.
            let table_names = state
                .managed_lake
                .read()
                .expect("managed lake read lock")
                .list_tables_in_database(database)
                .unwrap_or_default();
            for table in table_names {
                super::ddl::drop_managed_table(&state, database, &table)?;
            }
            if state.managed_lake_config.is_some() {
                super::ddl::drop_managed_database_entry(&state, database)?;
            }
        }
        let mut logical = state.catalog.write().expect("standalone catalog write lock");
        logical.drop_database(database)
    }

    fn create_table(&self, req: CreateTableRequest) -> Result<(), String> {
        let state = self.state()?;
        super::ddl::create_managed_table(
            &state,
            &ObjectName { parts: vec![req.table.clone()] },
            &req.namespace,
            &req.columns,
            req.key_desc.as_ref(),
            req.bucket_count,
        )
        .map(|_| ())
    }
    fn drop_table(
        &self,
        _catalog: &str,
        database: &str,
        table: &str,
        _if_exists: bool,
    ) -> Result<(), String> {
        let state = self.state()?;
        super::ddl::drop_managed_table(&state, database, table)
    }
    fn load_table(
        &self,
        _catalog: &str,
        database: &str,
        table: &str,
    ) -> Result<ResolvedTable, String> {
        let state = self.state()?;
        let logical = state.catalog.read().expect("standalone catalog read lock");
        let tdef = logical.get(database, table)?;
        Ok(ResolvedTable {
            catalog: "".to_string(),
            namespace: database.to_string(),
            table: table.to_string(),
            columns: tdef.columns.clone(),
            logical_types: Default::default(),
            key_desc: None,
        })
    }
    fn list_tables(&self, _catalog: &str, database: &str) -> Result<Vec<String>, String> {
        let state = self.state()?;
        state
            .managed_lake
            .read()
            .expect("managed lake read lock")
            .list_tables_in_database(database)
    }
}

pub struct ManagedLakeTableSource { state: std::sync::Weak<StandaloneState> }
impl ManagedLakeTableSource {
    pub fn new(state: &Arc<StandaloneState>) -> Self { Self { state: Arc::downgrade(state) } }
}
impl TableSource for ManagedLakeTableSource {
    fn name(&self) -> &'static str { "managed" }

    fn load_full(&self, _table: &ResolvedTable) -> Result<RecordBatch, String> {
        // Managed-lake scans happen through the pipeline (StarRocksScanOp);
        // there is no free-function quick-load path today. Phase 3 will
        // revisit whether MV-refresh-from-managed needs one.
        Err("managed-lake TableSource::load_full is not used; scan goes through the pipeline".into())
    }
    fn build_table_def(&self, _table: &ResolvedTable) -> Result<TableDef, String> {
        Err("managed-lake TableSource::build_table_def is registered through register_managed_table_in_catalog".into())
    }
}

pub struct ManagedLakeTableSink { state: std::sync::Weak<StandaloneState> }
impl ManagedLakeTableSink {
    pub fn new(state: &Arc<StandaloneState>) -> Self { Self { state: Arc::downgrade(state) } }
    fn state(&self) -> Result<Arc<StandaloneState>, String> {
        self.state.upgrade().ok_or_else(|| "standalone state dropped".to_string())
    }
}
impl TableSink for ManagedLakeTableSink {
    fn name(&self) -> &'static str { "managed" }

    fn append_rows(&self, table: &ResolvedTable, rows: &[Vec<Literal>]) -> Result<(), String> {
        let state = self.state()?;
        super::txn::insert_rows_into_managed_lake_table(
            &state,
            &table.namespace,
            &table.table,
            rows,
        )
    }
    fn append_batch(&self, table: &ResolvedTable, batch: RecordBatch) -> Result<(), String> {
        let state = self.state()?;
        super::txn::insert_batch_into_managed_lake_table(
            &state,
            &table.namespace,
            &table.table,
            batch,
        )
    }
    fn supports_pipeline_insert(&self) -> bool { true }
}

pub struct ManagedLakeMvBackend { state: std::sync::Weak<StandaloneState> }
impl ManagedLakeMvBackend {
    pub fn new(state: &Arc<StandaloneState>) -> Self { Self { state: Arc::downgrade(state) } }
    fn state(&self) -> Result<Arc<StandaloneState>, String> {
        self.state.upgrade().ok_or_else(|| "standalone state dropped".to_string())
    }
}
impl MvBackend for ManagedLakeMvBackend {
    fn name(&self) -> &'static str { "managed" }

    fn create_mv(&self, stmt: &CreateMaterializedViewStmt, db: &str) -> Result<(), String> {
        let state = self.state()?;
        super::mv_ddl::create_mv(&state, db, stmt).map(|_| ())
    }
    fn drop_mv(&self, stmt: &DropMaterializedViewStmt, db: &str) -> Result<(), String> {
        let state = self.state()?;
        super::mv_ddl::drop_mv(&state, db, stmt).map(|_| ())
    }
    fn refresh_mv(&self, stmt: &RefreshMaterializedViewStmt, db: &str) -> Result<(), String> {
        let state = self.state()?;
        super::mv_refresh::refresh_mv(&state, db, stmt).map(|_| ())
    }
    fn list_mvs(&self, stmt: &ShowMaterializedViewsStmt) -> Result<QueryResult, String> {
        let state = self.state()?;
        match super::mv_ddl::list_mvs(&state, stmt)? {
            crate::standalone::engine::StatementResult::Query(q) => Ok(q),
            crate::standalone::engine::StatementResult::Ok => {
                Err("list_mvs returned StatementResult::Ok; expected Query".into())
            }
        }
    }
    fn supports_incremental_refresh(&self) -> bool { true }
}
```

- [ ] **Step 2: Expose two new helpers from `txn.rs` that wrap existing logic**

The trait method `append_rows` / `append_batch` wants a "row-or-batch → managed-lake insert" surface that takes just `(state, database, table, data)` — but the current entry point `insert_into_managed_lake_table` takes an `InsertSource` AST node and does reorder-by-column logic first.

Open `src/connector/starrocks/managed/txn.rs` and add two public(crate) wrappers near the existing `insert_into_managed_lake_table`:

```rust
pub(crate) fn insert_rows_into_managed_lake_table(
    state: &Arc<StandaloneState>,
    database: &str,
    table: &str,
    rows: &[Vec<Literal>],
) -> Result<(), String> {
    let name = ObjectName { parts: vec![database.to_string(), table.to_string()] };
    let source = InsertSource::Values(rows.to_vec());
    insert_into_managed_lake_table(state, &name, &[], &source, database)
        .map(|_| ())
}

pub(crate) fn insert_batch_into_managed_lake_table(
    state: &Arc<StandaloneState>,
    database: &str,
    table: &str,
    batch: RecordBatch,
) -> Result<(), String> {
    let name = ObjectName { parts: vec![database.to_string(), table.to_string()] };
    // Path here mirrors the existing pipeline-INSERT-SELECT entry that
    // write_chunks_into_managed_partition drives. Construct a
    // MvRefresh-style request but without the MV metadata.
    super::txn::write_chunks_into_managed_partition(
        state,
        &name,
        database,
        std::iter::once(batch),
    )
}
```

(The exact signature of `write_chunks_into_managed_partition` may need adaptation — verify by reading `txn.rs` around line 100–200. The goal is ONE wrapper per trait method that calls an existing path without changing its behavior.)

- [ ] **Step 3: Expose `drop_managed_database_entry` from `ddl.rs`**

Currently private or module-local. Verify with `rg "fn drop_managed_database_entry"` in `src/connector/starrocks/managed/ddl.rs`. If not already `pub(crate)`, make it so.

- [ ] **Step 4: Update `src/connector/starrocks/managed/mod.rs`**

Append:

```rust
pub(crate) mod backend;
pub(crate) use backend::{
    ManagedLakeBackend, ManagedLakeMvBackend, ManagedLakeTableSink, ManagedLakeTableSource,
};
```

- [ ] **Step 5: `cargo check`**

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "feat(connector): implement Catalog/TableSource/TableSink/Mv backends for managed-lake"
```

---

## Task 2.5: Register backends into `StandaloneState`'s `ConnectorRegistry`

**Files:**
- Modify: `src/standalone/engine/mod.rs` — in `StandaloneState::new_inner` (or equivalent constructor)
- Modify: `StandaloneState` struct — add `connectors: Arc<RwLock<ConnectorRegistry>>` field

- [ ] **Step 1: Inspect current `StandaloneState` for a registry field**

Run: `rg "ConnectorRegistry" src/standalone/engine/mod.rs`

If `connectors` is already present, reuse it. Otherwise, add:

```rust
pub struct StandaloneState {
    // ...existing fields...
    pub(crate) connectors: Arc<std::sync::RwLock<crate::connector::ConnectorRegistry>>,
}
```

and initialize it in the constructor with `Arc::new(RwLock::new(ConnectorRegistry::default()))`.

- [ ] **Step 2: After `StandaloneState` is fully constructed (and wrapped in `Arc`), register the backends**

Find the function that constructs the `Arc<StandaloneState>` (around line 220–245 of `engine/mod.rs`). After the `Arc::new(inner)`, and before returning:

```rust
{
    let mut reg = state.connectors.write().expect("connector registry write");
    reg.register_catalog_backend(Arc::new(
        crate::connector::iceberg::catalog::IcebergCatalogBackend::new(Arc::clone(&state.iceberg_catalogs_arc())),
    ));
    reg.register_table_source(Arc::new(
        crate::connector::iceberg::catalog::IcebergTableSource::new(Arc::clone(&state.iceberg_catalogs_arc())),
    ));
    reg.register_table_sink(Arc::new(
        crate::connector::iceberg::catalog::IcebergTableSink::new(Arc::clone(&state.iceberg_catalogs_arc())),
    ));

    reg.register_catalog_backend(Arc::new(
        crate::connector::starrocks::managed::ManagedLakeBackend::new(&state),
    ));
    reg.register_table_source(Arc::new(
        crate::connector::starrocks::managed::ManagedLakeTableSource::new(&state),
    ));
    reg.register_table_sink(Arc::new(
        crate::connector::starrocks::managed::ManagedLakeTableSink::new(&state),
    ));
    reg.register_mv_backend(Arc::new(
        crate::connector::starrocks::managed::ManagedLakeMvBackend::new(&state),
    ));
}
```

The iceberg backend needs an `Arc<RwLock<IcebergCatalogRegistry>>`. Currently `state.iceberg_catalogs` is `RwLock<IcebergCatalogRegistry>`, not `Arc<RwLock<...>>`. Wrap it in `Arc` at construction, or add a helper that clones the inner `Arc` if the registry is already Arc-wrapped.

Check: `rg "iceberg_catalogs: " src/standalone/engine/mod.rs` — read line 149 context. If it's bare `RwLock`, change to `Arc<RwLock<...>>`. This is a minimal internal refactor and only affects the handful of `state.iceberg_catalogs.read()` call sites (which become `state.iceberg_catalogs.read()` on the inner `Arc` and still work because `Arc<RwLock>` derefs to `RwLock`).

- [ ] **Step 3: `cargo check`**

Expected: PASS. If a borrow error appears on `StandaloneState` (because we register while the `Arc` is still being held by the constructor), move the registration block to AFTER the constructor returns, in the caller.

- [ ] **Step 4: Commit**

```bash
git add -A
git commit -m "feat(standalone): register connector backends in StandaloneState"
```

---

## Task 2.6: Unit test — round-trip through the registry

**Files:**
- Create: `src/connector/backend_test.rs` (or inline test module in `backend.rs` if project style prefers)
- Modify: `src/connector/backend.rs` or `src/connector/mod.rs` to `#[cfg(test)] mod backend_test;`

- [ ] **Step 1: Write the failing test**

Create `src/connector/backend_test.rs`:

```rust
#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use crate::connector::backend::{CatalogBackend, MvBackend, NoMvBackend};
    use crate::connector::ConnectorRegistry;

    struct DummyCatalog;
    impl CatalogBackend for DummyCatalog {
        fn name(&self) -> &'static str { "dummy" }
        fn namespace_exists(&self, _: &str, _: &str) -> Result<bool, String> { Ok(false) }
        fn create_namespace(&self, _: &str, _: &str) -> Result<(), String> { Ok(()) }
        fn drop_namespace(&self, _: &str, _: &str, _: bool) -> Result<(), String> { Ok(()) }
        fn create_table(&self, _: crate::connector::backend::CreateTableRequest) -> Result<(), String> { Ok(()) }
        fn drop_table(&self, _: &str, _: &str, _: &str, _: bool) -> Result<(), String> { Ok(()) }
        fn load_table(&self, _: &str, _: &str, _: &str) -> Result<crate::connector::backend::ResolvedTable, String> {
            Err("dummy".into())
        }
        fn list_tables(&self, _: &str, _: &str) -> Result<Vec<String>, String> { Ok(vec![]) }
    }

    #[test]
    fn registry_registers_and_resolves_catalog_backend() {
        let mut reg = ConnectorRegistry::default();
        reg.register_catalog_backend(Arc::new(DummyCatalog));
        let got = reg.catalog_backend("dummy").expect("resolved");
        assert_eq!(got.name(), "dummy");
        assert!(reg.catalog_backend("missing").is_err());
    }

    #[test]
    fn no_mv_backend_returns_unsupported() {
        let mv = NoMvBackend("hdfs");
        assert!(!mv.supports_incremental_refresh());
    }
}
```

- [ ] **Step 2: Wire it in**

Append to `src/connector/mod.rs`:

```rust
#[cfg(test)]
mod backend_test;
```

- [ ] **Step 3: Run the test, confirm fail (if trait method signatures are wrong)**

```bash
cargo test --lib connector::backend_test
```

Expected: PASS (since the test only exercises registration, it should pass immediately).

- [ ] **Step 4: Commit**

```bash
git add -A
git commit -m "test(connector): verify CatalogBackend registry round-trip"
```

---

## Task 2.7 — Phase 2 Verification Checkpoint

- [ ] **Step 1: `cargo fmt && cargo clippy --all-targets -- -D warnings`**

- [ ] **Step 2: `cargo test`**

- [ ] **Step 3: SQL regression suites**

Same command as Task 1.11 Step 5.

Expected: ALL green. Phase 2 must not alter any behavior — it only ADDS new traits and a registry slot.

**Phase 2 complete.** Registry now holds the trait implementations; engine hot paths are unchanged; existing tests still pass. Safe to ship as a standalone PR.

---

# Phase 3 — Control-Flow Extraction

Rewrite `standalone/engine/sqlparse/statement.rs` and `engine/mod.rs::dispatch_statement` so DDL / INSERT / MV commands go through the new traits. Delete `engine/iceberg_glue.rs` and every direct `crate::connector::iceberg::catalog::*` or `crate::connector::starrocks::managed::*` import from `standalone/engine/`.

At the end of Phase 3, the only thing `standalone/engine/` knows about connectors is their name strings (resolved via the registry).

---

## Task 3.1: Introduce `resolve_target_backend` helper

**Files:**
- Create: `src/standalone/engine/backend_resolver.rs`
- Modify: `src/standalone/engine/mod.rs` — add `pub(crate) mod backend_resolver;`

The helper centralizes the "which backend handles this name" logic that's currently scattered across `execute_create_table_statement`, `execute_drop_table_statement`, `execute_insert_statement`, `execute_drop_database_statement`.

- [ ] **Step 1: Create the file**

```rust
//! Decide which catalog backend a given ObjectName targets. This is the
//! single place the "no catalog prefix + two-part name → managed lake"
//! convention lives. Backend-agnostic callers (ddl_flow, insert_flow,
//! mv_flow) all go through here.

use std::sync::Arc;

use crate::sql::parser::ast::ObjectName;
use crate::standalone::engine::StandaloneState;

#[derive(Clone, Debug)]
pub(crate) struct TargetBackend {
    pub backend_name: &'static str,
    pub catalog: String,
    pub namespace: String,
    pub table: String,
}

pub(crate) fn resolve_table_target(
    state: &Arc<StandaloneState>,
    name: &ObjectName,
    current_catalog: Option<&str>,
    current_database: &str,
) -> Result<TargetBackend, String> {
    // 1. Explicit catalog prefix → iceberg (today the only external catalog type).
    //    Future: route by catalog.type from state.iceberg_catalogs / other registries.
    if let Some(cat) = current_catalog {
        if state.iceberg_catalogs.read().expect("iceberg registry").exists(cat)? {
            let (namespace, table) = split_two_part(&name.parts, current_database)?;
            return Ok(TargetBackend {
                backend_name: "iceberg",
                catalog: cat.to_string(),
                namespace,
                table,
            });
        }
    }

    // 2. No catalog prefix + two-or-fewer parts → managed-lake (if configured).
    if current_catalog.is_none() && name.parts.len() <= 2 {
        if state.managed_lake_config.is_some() {
            let (database, table) = split_two_part(&name.parts, current_database)?;
            return Ok(TargetBackend {
                backend_name: "managed",
                catalog: String::new(),
                namespace: database,
                table,
            });
        }
    }

    // 3. Three-part name → (catalog, namespace, table) → iceberg.
    if name.parts.len() == 3 {
        return Ok(TargetBackend {
            backend_name: "iceberg",
            catalog: name.parts[0].clone(),
            namespace: name.parts[1].clone(),
            table: name.parts[2].clone(),
        });
    }

    Err(format!(
        "cannot resolve target backend for `{}` (current_catalog={:?})",
        name.parts.join("."),
        current_catalog,
    ))
}

fn split_two_part(parts: &[String], default_namespace: &str) -> Result<(String, String), String> {
    match parts.len() {
        1 => Ok((default_namespace.to_string(), parts[0].clone())),
        2 => Ok((parts[0].clone(), parts[1].clone())),
        n => Err(format!("expected 1- or 2-part name, got {n} parts")),
    }
}
```

Note: `IcebergCatalogRegistry::exists(&self, name)` may need adding. Check `registry.rs`; if only `get` exists today, derive: `pub fn exists(&self, name: &str) -> Result<bool, String>`.

- [ ] **Step 2: Add `pub(crate) mod backend_resolver;` to `engine/mod.rs`**

- [ ] **Step 3: `cargo check`**

Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add -A
git commit -m "feat(engine): introduce backend_resolver for backend-agnostic dispatch"
```

---

## Task 3.2: Rewrite `execute_create_table_statement` to use backend traits

**Files:**
- Modify: `src/standalone/engine/sqlparse/statement.rs`

- [ ] **Step 1: Replace the function body**

Find `execute_create_table_statement` (line 279). Replace with:

```rust
pub(crate) fn execute_create_table_statement(
    state: &Arc<StandaloneState>,
    stmt: crate::sql::parser::ast::CreateTableStmt,
    current_catalog: Option<&str>,
    current_database: &str,
) -> Result<StatementResult, String> {
    use crate::connector::backend::CreateTableRequest;
    use crate::standalone::engine::backend_resolver::resolve_table_target;

    let CreateTableKind::Iceberg { columns, key_desc, bucket_count, properties } = stmt.kind;

    let target = resolve_table_target(state, &stmt.name, current_catalog, current_database)?;
    let backend = state
        .connectors
        .read()
        .expect("connector registry read")
        .catalog_backend(target.backend_name)?;

    backend.create_table(CreateTableRequest {
        catalog: target.catalog.clone(),
        namespace: target.namespace.clone(),
        table: target.table.clone(),
        columns,
        key_desc,
        bucket_count,
        properties,
    })?;

    // Persist iceberg table into sqlite metadata store if applicable.
    // Managed-lake handles its own persistence internally.
    if target.backend_name == "iceberg" {
        persist_iceberg_table_if_needed(state, &target.catalog, &target.namespace, &target.table)?;
    }

    Ok(StatementResult::Ok)
}
```

- [ ] **Step 2: Verify the imports the old function used (`resolve_iceberg_table_name`, `create_iceberg_table`, `create_managed_table`) are still live elsewhere**

If they're now dead, leave them — Phase 3 will prune dead imports in Task 3.9.

- [ ] **Step 3: `cargo check`**

Expected: PASS.

- [ ] **Step 4: `cargo test --lib` quick smoke**

Expected: PASS. Any failures mean the backend adapter returned a different error message than the old path — compare against the existing test's `Err(...)` expectation and adjust the trait impl to emit the original message.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "refactor(engine): route CREATE TABLE through CatalogBackend trait"
```

---

## Task 3.3: Rewrite `execute_drop_table_statement`

**Files:**
- Modify: `src/standalone/engine/sqlparse/statement.rs`

- [ ] **Step 1: Replace the function body**

Find `execute_drop_table_statement` (line 442). Current logic:
- If two-part name + managed-lake configured → `drop_managed_lake_table`
- Otherwise → resolve iceberg, call `drop_iceberg_table`, remove from sqlite persistence

Replace with:

```rust
pub(crate) fn execute_drop_table_statement(
    state: &Arc<StandaloneState>,
    name: &ObjectName,
    current_catalog: Option<&str>,
    current_database: &str,
    if_exists: bool,
) -> Result<StatementResult, String> {
    use crate::standalone::engine::backend_resolver::resolve_table_target;

    let target = resolve_table_target(state, name, current_catalog, current_database)?;
    let backend = state
        .connectors
        .read()
        .expect("connector registry read")
        .catalog_backend(target.backend_name)?;

    match backend.drop_table(&target.catalog, &target.namespace, &target.table, if_exists) {
        Ok(()) => {
            if target.backend_name == "iceberg" {
                delete_iceberg_table_if_needed(state, &target.catalog, &target.namespace, &target.table)?;
            }
            Ok(StatementResult::Ok)
        }
        Err(err) if if_exists && err.contains("unknown table") => Ok(StatementResult::Ok),
        Err(err) => Err(err),
    }
}
```

- [ ] **Step 2: `cargo check` and `cargo test --lib`**

- [ ] **Step 3: Commit**

```bash
git add -A
git commit -m "refactor(engine): route DROP TABLE through CatalogBackend trait"
```

---

## Task 3.4: Rewrite `execute_create_database_statement` and `execute_drop_database_statement`

**Files:**
- Modify: `src/standalone/engine/sqlparse/statement.rs`

- [ ] **Step 1: Replace both functions**

The same pattern: resolve a "database target" (namespace target), call `backend.create_namespace()` or `backend.drop_namespace(force)`. The current code's cascade-drop of managed tables now lives inside `ManagedLakeBackend::drop_namespace`.

```rust
pub(crate) fn execute_create_database_statement(
    state: &Arc<StandaloneState>,
    name: &ObjectName,
    current_catalog: Option<&str>,
) -> Result<StatementResult, String> {
    use crate::standalone::engine::backend_resolver::resolve_namespace_target;
    let target = resolve_namespace_target(state, name, current_catalog)?;
    let backend = state
        .connectors
        .read()
        .expect("connector registry read")
        .catalog_backend(target.backend_name)?;
    backend.create_namespace(&target.catalog, &target.namespace)?;
    if target.backend_name == "iceberg" {
        persist_iceberg_namespace_if_needed(state, &target.catalog, &target.namespace)?;
    }
    Ok(StatementResult::Ok)
}

pub(crate) fn execute_drop_database_statement(
    state: &Arc<StandaloneState>,
    name: &ObjectName,
    current_catalog: Option<&str>,
    if_exists: bool,
    force: bool,
) -> Result<StatementResult, String> {
    use crate::standalone::engine::backend_resolver::resolve_namespace_target;
    let target = resolve_namespace_target(state, name, current_catalog)?;
    let backend = state
        .connectors
        .read()
        .expect("connector registry read")
        .catalog_backend(target.backend_name)?;
    match backend.drop_namespace(&target.catalog, &target.namespace, force) {
        Ok(()) => {
            if target.backend_name == "iceberg" {
                delete_iceberg_namespace_if_needed(state, &target.catalog, &target.namespace)?;
            }
            Ok(StatementResult::Ok)
        }
        Err(err) if if_exists && err.contains("unknown") => Ok(StatementResult::Ok),
        Err(err) => Err(err),
    }
}
```

- [ ] **Step 2: Add `resolve_namespace_target` to `backend_resolver.rs`**

```rust
pub(crate) fn resolve_namespace_target(
    state: &Arc<StandaloneState>,
    name: &ObjectName,
    current_catalog: Option<&str>,
) -> Result<TargetBackend, String> {
    // Mirror resolve_table_target's decision tree for namespaces.
    if current_catalog.is_none() && name.parts.len() == 1 && state.managed_lake_config.is_some() {
        return Ok(TargetBackend {
            backend_name: "managed",
            catalog: String::new(),
            namespace: name.parts[0].clone(),
            table: String::new(),
        });
    }
    if let Some(cat) = current_catalog {
        if state.iceberg_catalogs.read().expect("iceberg registry").exists(cat)? {
            return Ok(TargetBackend {
                backend_name: "iceberg",
                catalog: cat.to_string(),
                namespace: name.parts.last().cloned().unwrap_or_default(),
                table: String::new(),
            });
        }
    }
    if name.parts.len() == 2 {
        return Ok(TargetBackend {
            backend_name: "iceberg",
            catalog: name.parts[0].clone(),
            namespace: name.parts[1].clone(),
            table: String::new(),
        });
    }
    Err(format!("cannot resolve namespace target for `{}`", name.parts.join(".")))
}
```

- [ ] **Step 3: `cargo check` and `cargo test --lib`**

- [ ] **Step 4: Commit**

```bash
git add -A
git commit -m "refactor(engine): route CREATE/DROP DATABASE through CatalogBackend trait"
```

---

## Task 3.5: Rewrite `execute_insert_statement` through `TableSink`

**Files:**
- Create: `src/standalone/engine/insert_flow.rs`
- Modify: `src/standalone/engine/sqlparse/statement.rs` — `execute_insert_statement` delegates to `insert_flow`
- Modify: `src/standalone/engine/mod.rs` — add `pub(crate) mod insert_flow;`

- [ ] **Step 1: Create `insert_flow.rs`**

```rust
//! INSERT dispatch. Reorders literal rows against the target schema and
//! calls TableSink::append_rows / append_batch. Pipeline INSERT SELECT
//! stays on the existing lower/fragment.rs path and is triggered by
//! falling back to execute_insert_via_pipeline when the source is
//! InsertSource::FromQuery and the target sink claims
//! supports_pipeline_insert().

use std::sync::Arc;

use crate::connector::backend::ResolvedTable;
use crate::sql::parser::ast::{InsertSource, Literal, ObjectName};
use crate::standalone::engine::{StandaloneState, StatementResult};
use crate::standalone::engine::backend_resolver::resolve_table_target;
use crate::standalone::engine::insert::reorder_insert_rows;

pub(crate) fn run_insert(
    state: &Arc<StandaloneState>,
    name: &ObjectName,
    columns: &[String],
    source: &InsertSource,
    current_catalog: Option<&str>,
    current_database: &str,
) -> Result<StatementResult, String> {
    let target = resolve_table_target(state, name, current_catalog, current_database)?;
    let reg = state.connectors.read().expect("connector registry");
    let catalog = reg.catalog_backend(target.backend_name)?;
    let sink = reg.table_sink(target.backend_name)?;
    let resolved = catalog.load_table(&target.catalog, &target.namespace, &target.table)?;

    match source {
        InsertSource::Values(rows) => {
            let reordered = reorder_insert_rows(rows, columns, &resolved.columns)?;
            sink.append_rows(&resolved, &reordered)?;
        }
        InsertSource::SelectLiteralRow(row) => {
            let reordered = reorder_insert_rows(std::slice::from_ref(row), columns, &resolved.columns)?;
            sink.append_rows(&resolved, &reordered)?;
        }
        InsertSource::GenerateSeriesSelect(gs) => {
            // Delegate to existing generate_series helper, which already
            // handles both iceberg and managed-lake by the backend-name
            // dispatch it does internally. Phase 4 may refactor it onto
            // the trait.
            crate::standalone::engine::sqlparse::generate_series::insert_generate_series_rows_by_backend(
                state, &target, &resolved, gs, columns,
            )?;
        }
        InsertSource::UnionAll(parts) => {
            for part in parts {
                run_insert(state, name, columns, part, current_catalog, current_database)?;
            }
        }
        InsertSource::FromQuery(q) => {
            if !sink.supports_pipeline_insert() {
                return Err(format!(
                    "backend {} does not support INSERT SELECT",
                    target.backend_name
                ));
            }
            // Pipeline INSERT SELECT: kept on the existing pipeline path
            // via lower/fragment.rs's IcebergTableSinkFactory /
            // managed-lake sink factory. We just hand off here.
            crate::standalone::engine::sqlparse::statement::execute_insert_from_query_on_pipeline(
                state, name, columns, q, current_catalog, current_database,
            )?;
        }
    }
    Ok(StatementResult::Ok)
}
```

- [ ] **Step 2: Extract the pipeline-INSERT-SELECT path from `execute_insert_statement`**

The current `execute_insert_statement` (line 519) calls into the pipeline for `InsertSource::FromQuery`. Move that branch's body verbatim into a new `pub(crate) fn execute_insert_from_query_on_pipeline(...)` helper in `statement.rs`. Leave it callable from `insert_flow::run_insert`.

- [ ] **Step 3: Replace `execute_insert_statement` body with a thin wrapper**

```rust
pub(crate) fn execute_insert_statement(
    state: &Arc<StandaloneState>,
    name: &ObjectName,
    columns: &[String],
    source: &InsertSource,
    current_catalog: Option<&str>,
    current_database: &str,
) -> Result<StatementResult, String> {
    crate::standalone::engine::insert_flow::run_insert(
        state, name, columns, source, current_catalog, current_database,
    )
}
```

- [ ] **Step 4: Update `generate_series` to dispatch by backend name**

Open `src/standalone/engine/sqlparse/generate_series.rs`. Add a new entry point `insert_generate_series_rows_by_backend(state, target, resolved, gs, columns)` that branches on `target.backend_name`:
- `"iceberg"` → existing `insert_generate_series_rows` path
- `"managed"` → `insert_generate_series_rows_local` path

The bodies of both already exist; this is just a dispatcher.

- [ ] **Step 5: `cargo check` and `cargo test --lib`**

- [ ] **Step 6: Full sql-tests run — `ssb`, `tpc-h`, `ddl` at minimum**

The INSERT path is load-bearing for most tests; catch regressions early.

- [ ] **Step 7: Commit**

```bash
git add -A
git commit -m "refactor(engine): route INSERT through TableSink trait

Literal / generate_series / UNION ALL paths go through
insert_flow::run_insert → sink.append_rows(). Pipeline INSERT SELECT
is hoisted into execute_insert_from_query_on_pipeline and called via
sink.supports_pipeline_insert() gate."
```

---

## Task 3.6: Rewrite `dispatch_statement` (MV handlers) through `MvBackend`

**Files:**
- Create: `src/standalone/engine/mv_flow.rs`
- Modify: `src/standalone/engine/mod.rs` — `dispatch_statement` delegates; add `pub(crate) mod mv_flow;`

- [ ] **Step 1: Create `mv_flow.rs`**

```rust
//! MV control flow. Every statement is dispatched to the MvBackend the
//! target database is configured on. Today: managed-lake is the only
//! backend that implements MvBackend; iceberg returns NoMvBackend errors
//! if a caller asks for MV on iceberg.

use std::sync::Arc;

use crate::runtime::query_result::QueryResult;
use crate::sql::parser::ast::{
    CreateMaterializedViewStmt, DropMaterializedViewStmt, RefreshMaterializedViewStmt,
    ShowMaterializedViewsStmt,
};
use crate::standalone::engine::{StandaloneState, StatementResult};

fn mv_backend(state: &Arc<StandaloneState>) -> Result<Arc<dyn crate::connector::backend::MvBackend>, String> {
    // Today all MVs land on managed-lake. When more backends appear, this
    // resolves from the target DB's configured backend, analogous to
    // resolve_table_target.
    state
        .connectors
        .read()
        .expect("connector registry read")
        .mv_backend("managed")
}

pub(crate) fn create_mv(
    state: &Arc<StandaloneState>,
    db: &str,
    stmt: &CreateMaterializedViewStmt,
) -> Result<StatementResult, String> {
    mv_backend(state)?.create_mv(stmt, db)?;
    Ok(StatementResult::Ok)
}
pub(crate) fn drop_mv(
    state: &Arc<StandaloneState>,
    db: &str,
    stmt: &DropMaterializedViewStmt,
) -> Result<StatementResult, String> {
    mv_backend(state)?.drop_mv(stmt, db)?;
    Ok(StatementResult::Ok)
}
pub(crate) fn refresh_mv(
    state: &Arc<StandaloneState>,
    db: &str,
    stmt: &RefreshMaterializedViewStmt,
) -> Result<StatementResult, String> {
    mv_backend(state)?.refresh_mv(stmt, db)?;
    Ok(StatementResult::Ok)
}
pub(crate) fn list_mvs(
    state: &Arc<StandaloneState>,
    stmt: &ShowMaterializedViewsStmt,
) -> Result<StatementResult, String> {
    let q: QueryResult = mv_backend(state)?.list_mvs(stmt)?;
    Ok(StatementResult::Query(q))
}
```

- [ ] **Step 2: Rewrite `dispatch_statement` in `engine/mod.rs`**

Replace the body (line 734) with:

```rust
pub(crate) fn dispatch_statement(
    state: &Arc<StandaloneState>,
    current_database: &str,
    statement: crate::sql::parser::ast::Statement,
) -> Result<StatementResult, String> {
    use crate::sql::parser::ast::Statement;
    match statement {
        Statement::CreateMaterializedView(stmt) =>
            crate::standalone::engine::mv_flow::create_mv(state, current_database, &stmt),
        Statement::DropMaterializedView(stmt) =>
            crate::standalone::engine::mv_flow::drop_mv(state, current_database, &stmt),
        Statement::RefreshMaterializedView(stmt) =>
            crate::standalone::engine::mv_flow::refresh_mv(state, current_database, &stmt),
        Statement::ShowMaterializedViews(stmt) =>
            crate::standalone::engine::mv_flow::list_mvs(state, &stmt),
    }
}
```

- [ ] **Step 3: `cargo check` and `cargo test --lib`**

- [ ] **Step 4: Run sql-tests `materialized-view` and `mv-on-iceberg` suites**

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "refactor(engine): route MV statements through MvBackend trait"
```

---

## Task 3.7: Inline `iceberg_glue.rs` into `insert_flow` / `mv_flow`; delete the file

**Why:** `engine/iceberg_glue.rs` exists solely to bridge engine → iceberg internals. With the trait layer, its two public functions (`load_full_iceberg_batch`, `apply_iceberg_table_semantics_if_needed`, `normalize_iceberg_source_batch`) belong inside `IcebergTableSource::load_full` and `IcebergTableSource::build_table_def`, not in the engine.

**Files:**
- Modify: `src/connector/iceberg/catalog/backend.rs` — inline the 3 helpers
- Delete: `src/standalone/engine/iceberg_glue.rs`
- Modify: `src/standalone/engine/mod.rs` — remove `pub(crate) mod iceberg_glue;`

- [ ] **Step 1: Copy the three functions into `backend.rs` as private helpers**

The functions `load_full_iceberg_batch`, `apply_iceberg_table_semantics_if_needed`, `normalize_iceberg_source_batch`, `concat_or_empty_batches` all move from `iceberg_glue.rs` into `backend.rs`. They remain private to the connector crate.

- [ ] **Step 2: Update `IcebergTableSource::load_full` to call the local helper**

Change `crate::standalone::engine::iceberg_glue::load_full_iceberg_batch` → private `load_full_iceberg_batch` in `backend.rs`.

- [ ] **Step 3: Delete `src/standalone/engine/iceberg_glue.rs`**

```bash
rm src/standalone/engine/iceberg_glue.rs
```

Remove `pub(crate) mod iceberg_glue;` from `engine/mod.rs` and any `use self::iceberg_glue::*` in `engine/mod.rs`.

- [ ] **Step 4: `cargo check` and `cargo test --lib`**

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "refactor(engine): inline iceberg_glue helpers into IcebergTableSource"
```

---

## Task 3.8: Extract `register_iceberg_tables_for_query` into `mv_flow` / a new query_prep module

**Why:** `engine/mod.rs` currently has `register_iceberg_tables_for_query_impl` (around line 859) that walks a SQL AST, extracts iceberg table refs, loads each through the registry, and registers them in the in-memory catalog. This is the only remaining direct connector dependency in `engine/mod.rs`.

**Files:**
- Create: `src/standalone/engine/query_prep.rs` — the AST walker + catalog registration
- Modify: `src/standalone/engine/mod.rs` — delete the impl; call via `query_prep::register_external_tables_for_query`

- [ ] **Step 1: Create `query_prep.rs` with the extracted impl**

The new function takes `&Arc<StandaloneState>` and returns Result. Internally it uses the `IcebergTableSource` trait via the registry rather than `crate::connector::iceberg::catalog::load_table` directly:

```rust
pub(crate) fn register_external_tables_for_query(
    state: &Arc<StandaloneState>,
    current_catalog: Option<&str>,
    current_database: &str,
    query: &sqlparser::ast::Query,
) -> Result<(), String> {
    // For each iceberg-flavored 3-part table name in `query`, resolve the
    // target, load via catalog.load_table(), build a TableDef via
    // TableSource::build_table_def(), register in state.catalog.
    let refs = crate::standalone::engine::sqlparse::statement::extract_table_names_from_query(query);
    let reg = state.connectors.read().expect("connector registry");
    for r in refs {
        let name = crate::sql::parser::ast::ObjectName { parts: r.split('.').map(str::to_string).collect() };
        let Ok(target) = crate::standalone::engine::backend_resolver::resolve_table_target(
            state, &name, current_catalog, current_database,
        ) else { continue; };
        if target.backend_name != "iceberg" { continue; }
        let catalog = reg.catalog_backend("iceberg")?;
        let source = reg.table_source("iceberg")?;
        let resolved = catalog.load_table(&target.catalog, &target.namespace, &target.table)?;
        let tdef = source.build_table_def(&resolved)?;
        state.catalog.write().expect("catalog write").register_table(&target.namespace, tdef)?;
    }
    Ok(())
}
```

- [ ] **Step 2: Rewire callers**

`register_iceberg_tables_for_query` in `engine/mod.rs` becomes:

```rust
pub(crate) fn register_iceberg_tables_for_query(
    state: &Arc<StandaloneState>,
    current_catalog: Option<&str>,
    current_database: &str,
    query: &sqlparser::ast::Query,
) -> Result<(), String> {
    crate::standalone::engine::query_prep::register_external_tables_for_query(
        state, current_catalog, current_database, query,
    )
}
```

Leave the public function name unchanged since callers throughout the codebase still reference it; the impl is redirected.

- [ ] **Step 3: Remove now-dead helpers from `engine/mod.rs`**

- `register_iceberg_tables_for_query_impl`
- `build_iceberg_table_def_with_files` (and its public wrapper from Task 2.3)
- `register_empty_iceberg_table`
- `register_loaded_iceberg_table_with_files`

All of these are now re-homed inside `connector/iceberg/catalog/backend.rs` as private helpers of `IcebergTableSource::build_table_def`.

- [ ] **Step 4: `cargo check` and `cargo test --lib`**

- [ ] **Step 5: Run sql-tests `mv-on-iceberg`, `iceberg`**

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "refactor(engine): move iceberg query-prep registration into query_prep + TableSource"
```

---

## Task 3.9: Final cleanup — dead imports, module visibility

**Files:**
- Modify: `src/standalone/engine/mod.rs` — remove unused `use super::iceberg::*`, `use super::lake::*`
- Modify: `src/standalone/engine/sqlparse/statement.rs` — prune dead imports
- Modify: `src/connector/iceberg/catalog/backend.rs` — demote `build_iceberg_table_def_with_files_public` helper now that callers are gone

- [ ] **Step 1: Remove dead imports**

Run: `cargo clippy --all-targets -- -D warnings`

For each "unused import" warning, delete the line.

- [ ] **Step 2: Demote helpers**

Delete `build_iceberg_table_def_with_files_public` from `engine/mod.rs` (it was a Phase 2 crutch; Task 3.8 removed its last caller).

- [ ] **Step 3: `cargo check`, `cargo test`, `cargo clippy --all-targets -- -D warnings`**

- [ ] **Step 4: Commit**

```bash
git add -A
git commit -m "refactor(engine): prune dead iceberg/managed imports after Phase 3"
```

---

## Task 3.10 — Phase 3 Verification Checkpoint

- [ ] **Step 1: `cargo fmt && cargo clippy --all-targets -- -D warnings`**

- [ ] **Step 2: `cargo test` (full)**

- [ ] **Step 3: Grep check — no direct connector imports remain in engine**

Run: `rg "crate::connector::(iceberg|starrocks::managed)" src/standalone/engine/`

Expected: only `backend_resolver.rs`, `query_prep.rs`, `insert_flow.rs`, `mv_flow.rs` should appear; `engine/mod.rs` and `engine/sqlparse/statement.rs` should contain no such imports (they use the registry instead).

- [ ] **Step 4: Full sql-tests**

```bash
cargo run --release --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --suite ssb,tpc-h,tpc-ds,ddl,iceberg,materialized-view,mv-on-iceberg,filter,join,sort,cte \
  --mode verify
```

Expected: ALL green.

- [ ] **Step 5: Commit verification (no file changes if all pass)**

**Phase 3 complete.** `standalone/engine/` routes every backend-specific operation through traits resolved from the `ConnectorRegistry`. Adding a new connector catalog (e.g., JDBC-writable) now means:
1. Implement `CatalogBackend` / `TableSource` / `TableSink` for it in `src/connector/<name>/backend.rs`.
2. Register it in `StandaloneState` constructor.
3. Add the name to `backend_resolver` dispatch rules.

No touches to `engine/sqlparse/statement.rs` or `engine/mod.rs` required.

---

# Phase 4 — Optional Follow-Ups (Not in This Plan)

These are enabled by the refactor but intentionally out of scope here. Each would warrant its own plan:

- **JDBC `TableSource`** for `INSERT ... SELECT FROM mysql_catalog.db.tbl`. The trait exists; implementation is a small wrapper over `connector::jdbc::JdbcScanOp` batched collection.
- **Iceberg `MvBackend`** for "MV target = iceberg table." Today, `ManagedLakeMvBackend` writes MV output into managed-lake; with an `IcebergMvBackend` we could materialize into iceberg.
- **Streaming `TableSource::scan`** method returning an Arrow `Stream<RecordBatch>` for large tables where `load_full` is wrong.
- **Deletion of the 3606-line `engine/mod.rs`.** Beyond this plan, but once the connector code is gone, the god file is down to ~1800 lines and is a reasonable candidate for a follow-up split into `session.rs` / `query_executor.rs` / `state.rs`.

---

# Plan Self-Review

**Spec coverage** — each of the user's three concerns has tasks:
- "Connector-specific code in connector/" → Phase 1 (Tasks 1.2–1.10).
- "Keep logic and connector decoupled" → Phase 2 (Tasks 2.1–2.5) + Phase 3 (Tasks 3.1, 3.7–3.8).
- "DDL/MV as control flow calling connector" → Phase 3 (Tasks 3.2–3.6).

**Placeholder scan** — every step has exact file paths, exact commands, and concrete code snippets. One soft spot: Task 2.4 Step 2 references `write_chunks_into_managed_partition` without verifying its exact signature; the executing agent must read `txn.rs` to confirm. This is called out explicitly in the task note.

**Type consistency** — `TargetBackend`, `ResolvedTable`, `CreateTableRequest` fields are used consistently across Tasks 2.1, 3.1, 3.2, 3.5, 3.8. `MvBackend::list_mvs` returns `QueryResult` (Task 2.1) and is unwrapped to `StatementResult::Query(...)` at the caller (Task 3.6) — consistent.

**Known risks flagged explicitly:**
- Task 2.5 Step 3 — if `state.iceberg_catalogs` is bare `RwLock`, wrapping in `Arc` may ripple through callers. Plan instructs to do this atomically within Task 2.5.
- Task 3.5 Step 2 — `execute_insert_from_query_on_pipeline` extraction must keep the `InsertSource::FromQuery` pipeline code path bit-identical; the executing agent should diff against the Phase 2 version as verification.
- Sql-tests run at every phase boundary (1.11, 2.7, 3.10) — a regression at any checkpoint blocks the phase and must be fixed before the next phase begins.
