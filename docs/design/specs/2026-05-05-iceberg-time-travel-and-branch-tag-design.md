# Iceberg Time Travel & Branch / Tag — Phase 1 Design

- Date: 2026-05-05
- Scope: NovaRocks standalone-server path (FE path interface left open)
- Status: design approved, awaiting implementation plan

## 1. Goals & Non-Goals

### Goals

1. `SELECT … FOR VERSION AS OF <id>` / `FOR VERSION AS OF '<ref>'` /
   `FOR TIMESTAMP AS OF '<ts>'` queries return data at the requested
   snapshot, branch, or tag.
2. `ALTER TABLE t CREATE BRANCH … AS OF VERSION <id>` /
   `ALTER TABLE t CREATE TAG … AS OF VERSION <id>` /
   `ALTER TABLE t DROP BRANCH …` / `ALTER TABLE t DROP TAG …` mutate
   the Iceberg `refs` map.
3. `INSERT / UPDATE / DELETE` against `t.branch_<name>` commits to the
   requested branch and leaves `main` untouched.
4. Cross-ref scans (`SELECT a.x, b.x FROM t FOR VERSION AS OF '<dev>' a
   JOIN t FOR VERSION AS OF '<main>' b ON …`) work in a single query.
5. OCC isolates writes to different refs — concurrent writes on `main`
   and `dev` do not block one another.

### Non-Goals (Phase 2)

- Branch / tag retention semantics (`WITH SNAPSHOT RETENTION …`,
  `refRetain`). Phase 1 parses the syntax and ignores it.
- `SHOW BRANCHES` / `SHOW TAGS`, `ALTER TABLE … REPLACE BRANCH` (other
  than via `CREATE OR REPLACE`), `cherry-pick`, `fast-forward`.
- Materialized-view base-table refs other than `main`. MV refresh / IVM
  always reads `main` and writes `main` in phase 1.
- FE → BE wire integration. The Rust read / write paths are designed to
  accept a snapshot binding from a thrift field, but the FE path itself
  is phase B (no FE changes in this design).
- `AS OF SNAPSHOT <id>` legacy syntax. The numeric form is expressed via
  `FOR VERSION AS OF <int>`; covering both adds parse work without
  semantic gain.
- Branch-aware automatic retry on OCC conflict. Phase 1 returns the
  conflict to the client.

## 2. Architecture Overview

The new axis is a single core type, `IcebergRefBinding`, that flows
from the SQL text to the read and commit code paths.

```
SELECT … FOR VERSION AS OF '<branch>'         ─┐
SELECT … FOR TIMESTAMP AS OF '<ts>'            │
INSERT/UPDATE/DELETE  t.branch_<name>          │     parser
ALTER TABLE t CREATE/DROP BRANCH|TAG …         │  (sqlparser-rs +
                                               │   StarRocksDialect)
                                               ▼
                                  ┌──────────────────────────┐
                                  │ AST: IcebergTimeTravelClause │
                                  │ AST: IcebergRefSuffix     │
                                  │ AST: AlterIcebergRefAction│
                                  └─────────────┬────────────┘
                                                ▼
                                  ┌──────────────────────────┐
                                  │ analyzer/iceberg_ref.rs   │
                                  │  · ref / ts → snapshot_id │
                                  │  · validate ref existence │
                                  │  · reject DML on tag      │
                                  │  · produce binding        │
                                  └─────────────┬────────────┘
                                                ▼
                                  ┌──────────────────────────┐
                                  │ ExecPlan / lower          │
                                  │  · IcebergScan.binding    │
                                  │  · IcebergSink.dml_target │
                                  │  · IcebergRefAction node  │
                                  └─────────────┬────────────┘
                                                ▼
        ┌────────────────────┬───────────────────┬────────────────────┐
        ▼                    ▼                   ▼                    ▼
read.rs / registry.rs  commit/*.rs        commit/ref_action.rs  (FE path stub)
build_read_snapshot_at target_ref aware   SetSnapshotRef        TIcebergTable
extract_data_files_at  OCC by ref head    RemoveSnapshotRef     .snapshot_id
                                                                .target_ref
```

### Invariants

1. **Resolution is single-shot at planning time.** All ref / timestamp →
   `snapshot_id` resolution happens in the analyzer. ExecPlan carries
   only `snapshot_id`. The same query never re-resolves a ref at run
   time, guaranteeing all fragments observe one snapshot.
2. **`ref_name` is metadata.** Read execution uses only `snapshot_id`.
   `ref_name` is preserved for (a) `SetSnapshotRef` target on commit,
   (b) error messages, (c) EXPLAIN.
3. **Default binding is explicit `main`.** When no time-travel clause
   is present and the table has a current snapshot, the binding is
   `IcebergRefBinding { snapshot_id: current, ref_name: Some("main"),
   ref_kind: Branch }`. Read and write paths handle "no clause" and
   "explicit main" identically.
4. **Tag write rejection happens in the analyzer.** Lower / commit code
   never has to special-case tags.
5. **OCC is per-ref via `AssertRefSnapshotId { ref, snapshot_id }`.**
   Iceberg's existing OCC primitive already isolates by ref; this
   design parameterises the `ref` field rather than introducing new
   conflict logic.

## 3. AST & Parser Changes

### 3.1 New AST types — `src/sql/parser/ast/iceberg_ref.rs`

```rust
pub enum IcebergTimeTravelClause {
    SnapshotId(i64),         // FOR? VERSION AS OF <int>
    RefName(String),         // FOR? VERSION AS OF '<str>'
    Timestamp(Expr),          // FOR? TIMESTAMP AS OF <expr>
}

pub enum IcebergRefSuffix {
    Branch(String),           // t.branch_<name>
    Tag(String),              // t.tag_<name>
}

pub enum AlterIcebergRefAction {
    CreateBranch {
        name: String,
        anchor: SnapshotAnchor,
        if_not_exists: bool,
        replace: bool,
        ignored_options: Vec<String>,
    },
    CreateTag {
        name: String,
        anchor: SnapshotAnchor,
        if_not_exists: bool,
        replace: bool,
        ignored_options: Vec<String>,
    },
    DropBranch { name: String, if_exists: bool },
    DropTag    { name: String, if_exists: bool },
}

pub enum SnapshotAnchor {
    SnapshotId(i64),    // AS OF VERSION <id>
    CurrentMain,        // anchor omitted → use head of `main`
}
```

`ignored_options` carries the raw retention / refRetain text so phase 2
can implement retention without re-parsing. Phase 1 emits a
`tracing::warn!` and otherwise ignores the field.

### 3.2 SELECT / DML — reuse sqlparser-rs `TableVersion`

sqlparser-rs 0.61 (`maybe_parse_table_version`) already parses
`VERSION AS OF`, `TIMESTAMP AS OF`, and `FOR SYSTEM_TIME AS OF` after a
`TableFactor`. The standalone-server `parse_sql_raw` path therefore
needs no parser code change; `Statement::AlterIcebergRef` is the only
new statement.

In the analyzer (`src/sql/analyzer/resolve_from.rs`), each
`TableFactor::Table` is post-processed:

1. Read `version: Option<TableVersion>`:
   - `VersionAsOf(Number(n))`     → `IcebergTimeTravelClause::SnapshotId(n)`
   - `VersionAsOf(SingleQuoted(s))` → `IcebergTimeTravelClause::RefName(s)`
   - `VersionAsOf(_other_)` → fail-fast.
   - `TimestampAsOf(expr)` / `ForSystemTimeAsOf(expr)` →
     `IcebergTimeTravelClause::Timestamp(expr)`.
2. Read the qualified name's tail segment. If it matches
   `^(branch|tag)_(.+)$`, strip it and attach as `target_ref:
   IcebergRefSuffix`. SELECT must not carry `target_ref`; the analyzer
   rejects branch / tag suffixes on read-only queries to prevent the
   `t.branch_dev` ↔ multi-segment table name ambiguity.

### 3.3 ALTER TABLE … BRANCH / TAG

sqlparser-rs 0.61's `AlterTableOperation` does not include BRANCH / TAG.
A new probe in `src/sql/parser/dialect/alter_iceberg_ref.rs` inspects
the token stream after `ALTER TABLE <qualified-name>`; if the next
tokens match `(CREATE|DROP) (OR REPLACE)? (IF (NOT)? EXISTS)?
(BRANCH|TAG)` the entire statement is parsed by NovaRocks into
`Statement::AlterIcebergRef { table, action }`. Otherwise the statement
is left to sqlparser as today. This mirrors the existing
`materialized_view`, `create_catalog`, and `drop` probes in the same
directory.

### 3.4 File layout

| File | Change |
| --- | --- |
| `src/sql/parser/ast/iceberg_ref.rs` | new (§3.1) |
| `src/sql/parser/ast/mod.rs` | add `Statement::AlterIcebergRef` variant |
| `src/sql/parser/dialect/alter_iceberg_ref.rs` | new (probe + parse) |
| `src/sql/parser/dialect/mod.rs` | register new module |
| `src/sql/parser/mod.rs` | add `looks_like_alter_iceberg_ref` probe |
| `src/sql/analyzer/iceberg_ref.rs` | new (binding resolution, §4) |
| `src/sql/analyzer/resolve_from.rs` | post-process `TableFactor::Table` |
| `src/sql/analyzer/mod.rs` | dispatch `Statement::AlterIcebergRef` |
| `src/sql/analyzer/alter_iceberg_ref.rs` | new (DDL validation → ExecPlan node) |

## 4. Analyzer: Binding Resolution

### 4.1 Types

```rust
pub struct IcebergRefBinding {
    pub snapshot_id: i64,
    pub ref_name:    Option<String>,
    pub ref_kind:    Option<IcebergRefKind>,
}

pub enum IcebergRefKind { Branch, Tag }

pub struct IcebergDmlTarget {
    pub read_binding: IcebergRefBinding,
    pub write_ref:    String,   // ref_name on commit's SetSnapshotRef
}
```

For empty tables (no current snapshot, no clause), the read path
carries `Option<IcebergRefBinding>::None`, which preserves the existing
`build_read_snapshot` empty-table semantics.

### 4.2 Read-side resolution

| Input | Resolution | Output binding |
| --- | --- | --- |
| no clause | `metadata.current_snapshot()` | `(id, Some("main"), Branch)` |
| no clause + empty table | — | `None` |
| `SnapshotId(n)` | `metadata.snapshot_by_id(n)` must exist | `(n, None, None)` |
| `RefName(s)` | `metadata.refs().get(s)` must exist | `(ref.snapshot_id, Some(s), Branch/Tag)` |
| `Timestamp(expr)` | constant-fold expr to `epoch_ms`; `metadata.snapshot_log()` lookup of latest snapshot ≤ `epoch_ms` | `(found_id, None, None)` |

`Timestamp(expr)` in phase 1 accepts only literals: ISO-8601 string
(parsed via `chrono::DateTime::parse_from_rfc3339`) or integer
epoch-ms. Other expressions (`now()`, `cast(...)`, function calls)
fail-fast with a message pointing at the phase-1 limitation.

If metadata is mutated between planning and execution and the chosen
`snapshot_id` no longer exists, the run-time read path fails fast with
a message that includes the original `ref_repr` (see §8.2). No
fallback to "current snapshot".

### 4.3 Write-side resolution

| Statement | `read_binding` | `write_ref` |
| --- | --- | --- |
| `INSERT/UPDATE/DELETE INTO t` | head of `main` | `"main"` |
| `... INTO t.branch_<name>` | head of `<name>` (must be Branch) | `<name>` |
| `... INTO t.tag_<name>` | rejected by analyzer | n/a |

Combining `t.branch_<name>` with `FOR VERSION AS OF` is rejected. For
`UPDATE … FROM source`, the source table participates in §4.2 read
resolution independently and may carry its own binding.

### 4.4 DDL resolution (`AlterIcebergRefAction`)

`CreateBranch` / `CreateTag` resolve `SnapshotAnchor`:

- `SnapshotId(n)` → `n`; must exist or fail.
- `CurrentMain` → head of `main`; if `main` has no snapshot
  (empty table), fail.

Then validate against existing refs (see §6 idempotency table).
`DropBranch` / `DropTag` only need to validate kind and existence /
`if_exists`.

`name == "main"` is rejected for all four actions.

### 4.5 Error messages (consolidated in §8.1).

## 5. Read Path

### 5.1 Function signatures

| Old | New | Notes |
| --- | --- | --- |
| `build_read_snapshot(table)` | `build_read_snapshot_at(table, snapshot_id: i64)` | the old function survives as a thin wrapper that resolves to current snapshot for callers that explicitly want "main" |
| `extract_data_files_with_stats(table)` | `extract_data_files_with_stats_at(table, snapshot_id)` | thin wrapper kept |
| `IcebergTableSource::build_table_def(&self, table)` | `build_table_def_at(&self, table, binding: Option<IcebergRefBinding>)` on the `TableSource` trait, with a default impl that delegates to `build_table_def`. Iceberg overrides. | other connectors are unaffected |

### 5.2 `IcebergTableSource::build_table_def_at`

Resolves a `snapshot_id` from the binding (or falls back to
`metadata.current_snapshot_id()`), reuses the existing
`IcebergCatalogEntry::cached_data_files(namespace, table, snapshot_id)`
cache (already keyed by snapshot_id, no change needed), and otherwise
preserves today's behaviour. Empty tables → empty file list, identical
to today.

### 5.3 Call-site migration

| Site | Action |
| --- | --- |
| `src/connector/iceberg/catalog/backend.rs:140` (`build_table_def`) | move to `_at` |
| `src/connector/iceberg/changes.rs:732` (IVM changelog) | keep `build_read_snapshot` (main-only invariant) |
| `src/connector/starrocks/managed/mv_refresh_iceberg.rs:910` (MV refresh) | keep `build_read_snapshot` |
| `src/engine/delete_flow.rs:510, 574` | move to `_at`, pass DML target's read_binding |
| `src/engine/mutation_flow.rs:830` | move to `_at`, pass DML target's read_binding |

### 5.4 ExecPlan plumbing

| Node | Field |
| --- | --- |
| `ExecNode::IcebergScan` | `read_binding: Option<IcebergRefBinding>` |
| `ExecNode::IcebergSink` | `dml_target: IcebergDmlTarget` |
| `ExecNode::IcebergRefAction` (new, see §7) | `plan: RefActionPlan` |

The lower stage threads the binding into the thrift `TIcebergTable`
representation when applicable. Phase 1 uses a Rust-side struct field;
phase B (FE integration) adds the corresponding thrift field.

### 5.5 EXPLAIN output

```
IcebergScan
  table: catalog.db.t
  snapshot_id: 1734829112341
  ref: branch 'dev' (resolved at planning time)
  files: 12
```

If `ref_name = None`, the `ref:` line shows `(no ref name)`.

### 5.6 Out of scope (read path)

`IcebergReadDeleteFile` / `IcebergReadFile` structs, v3 row-lineage,
Puffin DV, manifest caching are all already snapshot-scoped and need no
changes.

## 6. Write Path

### 6.1 Shared seam — `CommitCtx`

`src/connector/iceberg/commit/mod.rs` (or `helpers.rs`) extends
`CommitCtx` with `target_ref: &'a str`. The dispatch site fills it
from `IcebergDmlTarget.write_ref`. Default `"main"`.

### 6.2 Mechanical changes in six commit modules

For each of `commit/fast_append.rs`, `commit/row_delta.rs`,
`commit/row_delta_dv.rs`, `commit/overwrite.rs`, `commit/update_cow.rs`,
`commit/rewrite_data_files.rs`:

1. Replace `m.current_snapshot()...` with `m.refs().get(ctx.target_ref)
   .and_then(|r| m.snapshot_by_id(r.snapshot_id))` for parent-snapshot
   computation.
2. Replace `ref_name: MAIN_BRANCH.to_string()` with
   `ref_name: ctx.target_ref.to_string()`.
3. Replace the `AssertRefSnapshotId { r#ref: MAIN_BRANCH.to_string(),
   ... }` requirement with `ctx.target_ref`.

Exact line numbers (current code base):

- `fast_append.rs`: 269, 288 (+ parent at 181)
- `row_delta.rs`: 240, 259
- `row_delta_dv.rs`: 377, 396
- `overwrite.rs`: 280, 299
- `update_cow.rs`: 310, 329
- `rewrite_data_files.rs`: 278, 297

### 6.3 OCC

`AssertRefSnapshotId { ref, snapshot_id }` is already per-ref;
parameterising `ref` automatically gives:

- Concurrent writes to different refs: independent.
- Concurrent writes to the same ref: existing OCC behaviour.
- Ref dropped between planning and commit: catalog `apply` rejects with
  Iceberg's built-in error; we surface it as
  `iceberg ref: branch '<name>' was dropped between planning and
  commit`.

No new conflict-detection logic is added.

### 6.4 Race-on-branch-write

If branch `dev` advances between planning (head = S1) and commit (head
= S2 ≠ S1), the OCC requirement fails. Phase 1 returns the error
verbatim; auto-retry is not implemented. This matches the existing
main-only behaviour.

### 6.5 Out of scope

- v3 row-lineage sequence-number derivation (parent-relative) — works
  unchanged once the parent comes from the per-ref head.
- Puffin DV layout — unaffected by ref selection.
- Java `iceberg-metadata-bridge` — phase 1 does not pass `target_ref`
  through the bridge.

## 7. DDL Execution Path

### 7.1 New module — `src/connector/iceberg/commit/ref_action.rs`

```rust
pub struct RefActionPlan {
    pub catalog: String,
    pub namespace: String,
    pub table: String,
    pub action: RefAction,
}

pub enum RefAction {
    CreateBranch { name: String, snapshot_id: i64, replace: bool, if_not_exists: bool },
    CreateTag    { name: String, snapshot_id: i64, replace: bool, if_not_exists: bool },
    DropBranch   { name: String, if_exists: bool },
    DropTag      { name: String, if_exists: bool },
}

pub async fn execute_ref_action(plan: &RefActionPlan)
    -> Result<RefActionOutcome, String>;
```

The implementation loads metadata, calls `handle_*_idempotency` to
determine whether to commit or short-circuit, then issues `TableUpdate`
+ `TableRequirement` lists through the existing catalog dispatch (same
path as the six data-commit modules).

### 7.2 Updates / requirements

| Action | TableUpdate(s) | TableRequirement |
| --- | --- | --- |
| `CreateBranch dev anchor=S` (new) | `SetSnapshotRef { ref_name: "dev", reference: { snapshot_id: S, retention: Branch{} } }` | `AssertRefSnapshotId { ref: "dev", snapshot_id: None }` |
| `CreateBranch dev` (REPLACE, existing head=H1) | as above | `AssertRefSnapshotId { ref: "dev", snapshot_id: Some(H1) }` |
| `CreateTag t1 anchor=S` | `SetSnapshotRef { ref_name: "t1", reference: { snapshot_id: S, retention: Tag{} } }` | analogous |
| `DropBranch dev` (head=H1) | `RemoveSnapshotRef { ref_name: "dev" }` | `AssertRefSnapshotId { ref: "dev", snapshot_id: Some(H1) }` |
| `DropTag t1` (head=H1) | `RemoveSnapshotRef { ref_name: "t1" }` | analogous |

`commit/mod.rs` dispatch is adjusted so that `AddSnapshot` becomes
optional (ref-action commits do not produce a new snapshot).

### 7.3 Idempotency matrix

| Scenario | Behaviour |
| --- | --- |
| `CREATE BRANCH dev` and ref exists, no `IF NOT EXISTS` / `OR REPLACE` | error `branch '<name>' already exists` |
| `CREATE BRANCH dev` exists + `IF NOT EXISTS` | OK, no commit |
| `CREATE OR REPLACE BRANCH dev` exists | commit, replaces head |
| `CREATE BRANCH dev` does not exist + `IF NOT EXISTS` | commit |
| `DROP BRANCH dev` does not exist, no `IF EXISTS` | error |
| `DROP BRANCH dev` does not exist + `IF EXISTS` | OK, no commit |
| name == `main` | reject (CREATE and DROP) |
| `CREATE/DROP TAG x` but `x` is a branch (kind mismatch) | reject |

### 7.4 ExecPlan / executor integration

A new `ExecNodeKind::IcebergRefAction(IcebergRefActionNode { plan })`
runs as a single driver that calls `execute_ref_action` (block_on),
returns an empty result chunk plus a status row indicating success.

Lower wiring: `src/lower/node/mod.rs` adds dispatch; new
`src/lower/ddl/iceberg_ref.rs` translates the analyzer node to the
ExecNode.

### 7.5 Out of scope

Retention semantics, `SHOW BRANCHES` / `SHOW TAGS`, dedicated
`REPLACE BRANCH` syntax, cherry-pick / fast-forward.

## 8. Errors & Edge Cases

### 8.1 Analyzer-time errors

| Trigger | Message |
| --- | --- |
| `FOR VERSION AS OF <int>` snapshot missing | `iceberg time travel: snapshot {id} not found in {catalog}.{ns}.{table}` |
| `FOR VERSION AS OF '<str>'` ref missing | `iceberg time travel: ref '{name}' not found; existing refs: {list}` |
| `FOR TIMESTAMP AS OF` predates first snapshot | `iceberg time travel: timestamp {ts} predates first snapshot of {table}` |
| `FOR TIMESTAMP AS OF` non-literal | `iceberg time travel: phase 1 only accepts literal timestamp; got expression: {repr}` |
| DML on `t.tag_<name>` | `iceberg ref: tag '{name}' is read-only; use a branch as DML target` |
| `t.branch_<a>` + `FOR VERSION AS OF '<b>'` | `iceberg ref: branch suffix '.branch_{a}' conflicts with FOR VERSION AS OF '{b}'` |
| CREATE/DROP on `main` | `iceberg ref: 'main' is reserved` |
| `CREATE TAG x` but x is branch | `iceberg ref: '{name}' is a branch, not a tag` |
| `CREATE BRANCH x` already exists | `iceberg ref: branch '{name}' already exists` |
| `DROP BRANCH x` does not exist | `iceberg ref: branch '{name}' does not exist` |
| `CREATE BRANCH x AS OF VERSION <id>` id missing | `iceberg ref: snapshot {id} not found; cannot anchor branch '{name}'` |
| Empty table, anchor omitted | `iceberg ref: cannot create branch on table without a current snapshot` |

### 8.2 Run-time errors

| Trigger | Message |
| --- | --- |
| Plan-time snapshot expired during execution | `iceberg time travel: snapshot {id} (resolved from {ref_repr}) was expired between planning and execution; retry the query` |
| Branch head moved on commit | `iceberg ref: branch '{name}' head changed between planning and commit ({plan_id} → {head_id}); retry the write` |
| Branch dropped during commit | `iceberg ref: branch '{name}' was dropped between planning and commit` |
| `CREATE OR REPLACE BRANCH` race | `iceberg ref: branch '{name}' state changed during CREATE OR REPLACE; retry` |

`ref_repr` is `branch '<name>'` or `tag '<name>'` when a ref name is
known, otherwise the raw snapshot id.

### 8.3 Edge cases (explicitly defined)

- **Empty table + any time-travel clause**: analyzer error, never an
  empty result.
- **Same table referenced multiple times in one query at different
  refs**: each `TableFactor` resolves independently — this is the
  cross-ref demo path.
- **CTE referencing a time-traveled table**: CTE inlining preserves the
  binding on each reference; no metadata re-load.
- **Prepared statements**: rebuilt on each execution; no plan-time
  snapshot cached.

## 9. Testing Strategy

### 9.1 SQL regression — `sql-tests/iceberg/sql/`

| File | Coverage |
| --- | --- |
| `iceberg_time_travel_select.sql` | `FOR VERSION AS OF <id>`, `FOR VERSION AS OF '<branch>'`, `FOR VERSION AS OF '<tag>'`, `FOR TIMESTAMP AS OF '<ISO>'`, default main, plus a single query with two refs joined |
| `iceberg_branch_tag_ddl.sql` | `CREATE BRANCH/TAG`, `IF NOT EXISTS`, `OR REPLACE`, `DROP BRANCH/TAG`, `IF EXISTS` |
| `iceberg_branch_write.sql` | `INSERT/UPDATE/DELETE` against `t.branch_dev`; verify main untouched and dev advanced; cross-ref SELECT |
| `iceberg_ref_negative.sql` | every error in §8.1, expected output style `-- error: ...` |
| `iceberg_branch_concurrent_occ.sql` | parallel writes to different refs; uses runner's `--concurrent` mode (extension to sql-tests runner is the last item in the implementation plan) |

Snapshots are generated via `--mode record`.

### 9.2 Rust unit tests

- `src/sql/analyzer/iceberg_ref.rs::tests`: input table → expected
  binding; rejection cases.
- `src/connector/iceberg/commit/ref_action.rs::tests`: 4 actions ×
  idempotency matrix; kind mismatch; reserved name `main`.
- `src/connector/iceberg/commit/fast_append.rs::tests` and the other
  five commit modules: per-module fast-path (≈20 lines each) writing
  to a non-main branch fixture and verifying head advancement.
- `src/connector/iceberg/read.rs::tests`: `build_read_snapshot_at`
  with valid id, historical id, missing id.

### 9.3 Out of scope (testing)

- Cross-binary stress / performance.
- FE↔BE thrift end-to-end (deferred to phase B).
- Retention behaviour.

### 9.4 Acceptance gate

- `cargo build`, `cargo clippy`, `cargo fmt`, `cargo test` clean.
- `cargo run --release -- standalone-server --port 9030` runs.
- Existing `iceberg`, `ssb`, `tpc-h`, `tpc-ds`, `mv-on-iceberg` SQL
  suites still pass `--mode verify`.
- New iceberg sql-tests pass `--mode verify`.
- EXPLAIN shows `ref:` field on `IcebergScan`.
- Demo path runs end to end:
  `CREATE BRANCH dev` → `INSERT INTO t.branch_dev …` →
  `SELECT … FOR VERSION AS OF '<dev>'` differs from
  `… FOR VERSION AS OF '<main>'`.
