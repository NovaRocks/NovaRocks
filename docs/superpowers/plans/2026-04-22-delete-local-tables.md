# Delete Local-Table Backend Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:subagent-driven-development` or `superpowers:executing-plans` to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Design:** `docs/superpowers/specs/2026-04-22-delete-local-tables-design.md`

**Goal:** Collapse standalone to a single managed-lake backend. Delete ~2k lines under `engine/local/` (parquet / insert / stream_load / aggregate / LocalTableSemantics), add StarRocks-style CREATE TABLE defaults so existing `sql-tests` keep passing unmodified, rewrite `FROM dual` in the parser, and route HTTP stream load to managed lake.

**Tech Stack:** Rust, SQLite control plane, OpenDAL + MinIO, StarRocks lake format

**Branch:** `feat/delete-local-tables` (from `upstream/main`)

**Hard prerequisite for CI / local dev:** a reachable MinIO on `http://127.0.0.1:9000` with credentials `admin/admin123`. Document in `sql-tests/README.md` before merging.

---

## Sequencing

Tasks run in order. Each ends with `cargo check` and a commit. Tasks 1-3 are purely additive (new DDL defaults, parser rewrite, stream-load helper re-homing); they land without removing old code so tests still pass. Task 4 flips `sql-tests` to rely on the defaults (no `.sql` edits). Tasks 5-8 delete the old local-table code now that nothing depends on it.

| # | Title | Delta | Risk |
|---|---|---|---|
| 1 | StarRocks-style DDL defaults | +200 lines, 0 deletions | Medium (semantic change in DDL) |
| 2 | `FROM dual` parser rewrite | +50 lines | Low |
| 3 | Re-home stream-load parsers | Moves, 0 net | Low |
| 4 | Switch sql-tests runner default DB to managed | Small runner change | Medium (CI MinIO) |
| 5 | Delete HTTP `stream_load_local_table` path | Code rewire | Medium |
| 6 | Delete `LocalTableSemantics` + `TableStorage::Parquet` | Large, mechanical | Medium |
| 7 | Delete `engine/local/{parquet,insert,stream_load,aggregate}.rs` | Large | Low (everything above unhooked already) |
| 8 | Promote A-tier utilities out of `engine/local/` and remove directory | Moves + re-imports | Low |
| 9 | Final verification sweep | Tests + `cargo fmt` + docs | — |

---

## Task 1: StarRocks-style CREATE TABLE defaults

**Why:** All follow-up tasks depend on "bare `CREATE TABLE`" succeeding on managed lake.

**Files:**
- Modify: `src/standalone/lake/ddl.rs`
- Modify: `src/standalone/engine/sqlparse/statement.rs`
- Test: `src/standalone/lake/ddl.rs` (unit tests)

### Step 1: Fail-first unit tests

Add to `ddl.rs::tests`:

```rust
#[test]
fn create_managed_table_defaults_dup_key_first_non_float_column() {
    // CREATE TABLE t (k BIGINT, v STRING) with no KEY / DISTRIBUTED / BUCKETS.
    // Expect: DUP KEY (k), HASH(k), 1 bucket.
}

#[test]
fn create_managed_table_defaults_skip_float_as_leading_key() {
    // CREATE TABLE t (f FLOAT, k INT, v VARCHAR(8)).
    // First key-eligible column is `k`; expect DUP KEY (k), HASH(k).
}

#[test]
fn create_managed_table_defaults_short_key_length_cap() {
    // Long VARCHAR(200) columns — expect key cap at 3 cols or 36 bytes.
}

#[test]
fn create_managed_table_defaults_first_column_must_be_keyable() {
    // CREATE TABLE t (d DOUBLE, v INT). No explicit KEY — error because no leading
    // column can be a short key (matches StarRocks "data type of first column cannot be key").
}
```

These must fail before Step 2.

### Step 2: Implement `choose_default_dup_key_columns`

In `src/standalone/lake/ddl.rs` (or a new private helper module):

```rust
fn choose_default_dup_key_columns(columns: &[TableColumnDef]) -> Result<Vec<String>, String> {
    const SHORT_KEY_MAX_COLS: usize = 3;
    const SHORT_KEY_MAX_BYTES: usize = 36;

    let mut keys = Vec::new();
    let mut bytes_used = 0usize;
    for col in columns {
        if col.is_generated { break; }
        if !key_eligible_type(&col.data_type) { break; }
        let col_bytes = index_byte_size(&col.data_type);
        if keys.len() >= SHORT_KEY_MAX_COLS || bytes_used + col_bytes > SHORT_KEY_MAX_BYTES {
            break;
        }
        keys.push(col.name.clone());
        bytes_used += col_bytes;
        if is_varchar(&col.data_type) { break; }   // include and stop
    }
    if keys.is_empty() {
        return Err(format!(
            "data type of first column `{}` cannot be a key column",
            columns.first().map(|c| c.name.as_str()).unwrap_or("")
        ));
    }
    Ok(keys)
}

fn key_eligible_type(ty: &SqlType) -> bool {
    !matches!(ty, SqlType::Float | SqlType::Double | SqlType::Array(_) | SqlType::Map(_, _) | SqlType::Struct(_))
}

fn index_byte_size(ty: &SqlType) -> usize {
    match ty {
        SqlType::Boolean | SqlType::TinyInt => 1,
        SqlType::SmallInt => 2,
        SqlType::Int => 4,
        SqlType::BigInt | SqlType::DateTime => 8,
        SqlType::LargeInt | SqlType::Decimal { .. } => 16,
        SqlType::Date => 4,
        SqlType::String | SqlType::Varchar(_) | SqlType::Char(_) => 20, // StarRocks uses length+4 capped
        SqlType::Binary => 20,
        _ => 255,
    }
}
```

### Step 3: Absorb `None` inputs in `create_managed_table`

Change the `key_desc: Option<&TableKeyDesc>` and
`bucket_count: Option<u32>` handling:

- If `key_desc` is `None`, synthesise a `TableKeyDesc {
    kind: TableKeyKind::Duplicate, columns: choose_default_dup_key_columns(columns)? }`.
- If `bucket_count` is `None`, default to `1`.

Drop the existing "requires explicit DISTRIBUTED BY" / "requires explicit
key description" errors in these cases. (Keep PRIMARY / AGGREGATE
rejection.)

### Step 4: Apply defaults at the sqlparse layer OR pass `None` through

Two options; pick one:

**Option A (in statement.rs):** pre-fill defaults in
`execute_create_table_statement` before calling `create_managed_table`.
This keeps `create_managed_table` strict; easier to reason about DDL
semantics.

**Option B (in ddl.rs):** let `create_managed_table` accept `Option`
and apply defaults itself.

Pick **Option B** — the DDL layer is the single source of truth for
StarRocks-alignment, and downstream callers (including future FE-driven
paths, stream load → table-not-exists auto-create, etc.) all benefit
without needing to duplicate the default logic.

### Step 5: `cargo check` + unit tests + commit

```bash
cargo check
cargo test --lib lake::ddl
git add src/standalone/lake/ddl.rs src/standalone/engine/sqlparse/statement.rs
git commit -m "feat(lake): default DUP KEY / HASH / BUCKETS in managed-lake DDL"
```

---

## Task 2: `FROM dual` parser rewrite

**Files:**
- Modify: `src/sql/parser/dialect/mod.rs` (add `rewrite_from_dual` inside `normalize_for_raw_parse`)
- Test: same file

### Step 1: Tests

```rust
#[test]
fn normalize_for_raw_parse_strips_bare_from_dual() {
    let out = normalize_for_raw_parse("SELECT 1 FROM dual").unwrap();
    assert_eq!(out.trim(), "SELECT 1");
}

#[test]
fn normalize_for_raw_parse_strips_from_dual_with_trailing_semicolon() {
    let out = normalize_for_raw_parse("SELECT now() FROM dual;").unwrap();
    assert_eq!(out.trim(), "SELECT now();");
}

#[test]
fn normalize_for_raw_parse_keeps_dual_when_where_present() {
    // Not matched — let downstream fail loudly.
    let out = normalize_for_raw_parse("SELECT 1 FROM dual WHERE 1=1").unwrap();
    assert!(out.contains("FROM dual"));
}
```

### Step 2: Implementation

Case-insensitive regex-free scan: match `\bFROM\s+dual\b` only when the
remainder (after eating optional whitespace) is empty, `;`, or starts
with a comment. Keep the rewrite narrow.

### Step 3: `cargo check` + commit

```bash
cargo check
cargo test --lib sql::parser::dialect::tests::normalize_for_raw_parse_strips
git add src/sql/parser/dialect/mod.rs
git commit -m "feat(sql): strip bare FROM dual in normalize_for_raw_parse"
```

---

## Task 3: Re-home stream-load parsers

**Files:**
- Create: `src/standalone/engine/stream_load.rs` (new neutral location — not under `local/`)
- Modify: `src/standalone/engine/mod.rs` (declare `mod stream_load;`)
- Modify: `src/standalone/engine/local/stream_load.rs` (leave the
  `stream_load_local_table` shell, but move parse helpers out)

### Step 1: Move helpers

Move `parse_csv_stream_load_rows`, `parse_json_stream_load_rows`,
`parse_stream_load_columns`, `parse_stream_load_jsonpaths`,
`parse_json_rows`, `extract_json_path`, `json_value_to_field`,
`single_byte_stream_load_delimiter` into
`src/standalone/engine/stream_load.rs`. They become `pub(crate)`.

### Step 2: Update `local/stream_load.rs`

Keep only `stream_load_local_table` in `local/stream_load.rs`. Re-import
the parsers from the new neutral module. Task 5 will rip this file.

### Step 3: `cargo check` + commit

```bash
cargo check
git add -A  # staged selectively (avoid untracked .codex/meta as always)
git commit -m "refactor(engine): move stream-load parsers out of local/"
```

(Add specific files by name; no `-A`.)

---

## Task 4: Make sql-tests runner rely on managed defaults

**Files:**
- Modify: `tests/sql-test-runner/src/**/*.rs` (the default database
  bootstrap: ensure every test session has a warehouse URI configured)
- Modify: `sql-tests/README.md` — document MinIO requirement
- (Optional) Add: `tests/sql-test-runner/scripts/check-minio.sh`

### Step 1: Audit current runner

Read `tests/sql-test-runner/src/main.rs` / `lib.rs`. Identify where it
spins up the standalone server and with what config. If today it points
to a config without `warehouse_uri`, every `CREATE TABLE` test fell
through to the local-table path.

### Step 2: Provide a default managed warehouse

Either:
- Write `tests/sql-test-runner/` to emit a `novarocks.toml` with
  `warehouse_uri = "s3://novarocks/codex-sql-tests/<run_id>"` per run
- Or require the user to pre-set env vars (`AWS_S3_ENDPOINT` etc.)
  matching `maybe_managed_lake_config` style

Pick the first (self-contained).

### Step 3: MinIO pre-check

Add a fast startup probe in the runner: fail with a clear actionable
error if `http://127.0.0.1:9000` is unreachable:

```
error: MinIO at http://127.0.0.1:9000 is unreachable.
hint: start it with:
  mkdir -p ~/minio-data && minio server ~/minio-data --console-address :9001 &
```

### Step 4: Smoke-run a small suite

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --suite filter --mode verify
```

Must pass unchanged `.sql` files.

### Step 5: Commit

```bash
git add tests/sql-test-runner/ sql-tests/README.md
git commit -m "test(sql-tests): route all cases through managed lake by default"
```

---

## Task 5: Delete HTTP `stream_load_local_table` path

**Files:**
- Modify: `src/service/stream_load.rs`
- Modify: `src/standalone/engine/mod.rs`
  (`StandaloneNovaRocks::stream_load_local_table`)
- Add: `stream_load_managed_lake_table` routing to
  `insert_into_managed_lake_table`

### Step 1: Fail-first integration test

Extend `tests/standalone_mysql_server.rs` (or a sibling) — an HTTP
stream-load test that expects a managed table to receive rows. Skip if
MinIO unreachable (reuse `maybe_managed_lake_config` gate).

### Step 2: Rewire

`StandaloneNovaRocks::stream_load_local_table` → rename
`stream_load_managed_lake_table`. Body:

```rust
let rows = parse_stream_load_payload(&request)?;  // moved parsers
insert_into_managed_lake_table(&self.inner, &request.database, &request.table, rows)?
```

### Step 3: Update `service/stream_load.rs`

Replace the call site. Remove references to local-path stream load.

### Step 4: `cargo check` + commit

```bash
cargo check
cargo test --test standalone_mysql_server <new-test-name>
git add src/service/stream_load.rs src/standalone/engine/mod.rs
git commit -m "feat(stream_load): route HTTP stream load to managed lake"
```

---

## Task 6: Delete `LocalTableSemantics` + `TableStorage::Parquet`

**Files:**
- Modify: `src/standalone/engine/mod.rs` (remove
  `local_table_semantics` field and all interactions)
- Modify: `src/sql/catalog.rs` — remove `TableStorage::Parquet` variant
- Modify: every match on `TableStorage` — delete the `Parquet` arm
- Modify: `src/standalone/engine/sqlparse/statement.rs` — remove
  branches that dispatch to `create_local_table_from_columns`,
  `insert_into_local_table`, `build_parquet_table`
- Modify: `src/standalone/engine/local/mod.rs` — delete the semantics
  helpers (they become unreachable)

### Step 1: Follow the compile errors

Remove `TableStorage::Parquet` → rustc errors list every dependent
site. Walk through each match, delete the parquet arm. For sites that
relied on reading parquet data (e.g., some `SELECT`-on-local-table
codegen), reroute to the managed lake path or delete the branch if the
callsite only made sense for local tables.

### Step 2: `cargo check` + `cargo test --lib` + commit

```bash
cargo check
cargo test --lib
git add <listed files>
git commit -m "refactor(standalone): drop LocalTableSemantics and TableStorage::Parquet"
```

---

## Task 7: Delete `engine/local/{parquet,insert,stream_load,aggregate}.rs`

At this point nothing imports these files. `git rm` them and remove the
`pub(crate) mod ...` declarations in `engine/local/mod.rs`.

Any helper functions still referenced (e.g., `normalize_map_entries_nullability`
from `local/insert.rs`) → either delete if unused, or relocate to a
shared utility under `engine/` (likely the managed-lake insert path
already has or needs a similar helper).

### Step 1: `cargo check` + commit

```bash
cargo check
git rm src/standalone/engine/local/{parquet,insert,stream_load,aggregate}.rs
git commit -m "refactor(engine): remove local parquet/insert/stream_load/aggregate modules"
```

---

## Task 8: Promote A-tier utilities, remove `engine/local/` directory

`normalize_identifier`, `InMemoryCatalog`, `CatalogProvider`,
`ColumnDef`, `TableDef`, `TableStorage::{Iceberg, ManagedLake}`,
`PhysicalTableLayout`, `ManagedTabletRef`, `DEFAULT_DATABASE` —
move to `src/standalone/engine/catalog.rs` (or back to
`src/standalone/catalog.rs`; pick whichever keeps import graph cleanest
— likely `engine/catalog.rs` since the types are engine-scoped now).

Update every `use crate::standalone::engine::local::{...}` → new path.

Delete the empty `src/standalone/engine/local/` directory.

### Step 1: Move + mass-rewrite imports

Use `rg -l 'engine::local'` to find all call sites (code + tests).

### Step 2: `cargo check` + `cargo test --lib` + commit

```bash
cargo check
cargo test --lib
git add <files>
git rm -r src/standalone/engine/local/
git commit -m "refactor(engine): relocate catalog utilities out of engine/local/"
```

---

## Task 9: Final verification sweep

- [ ] `cargo fmt` clean
- [ ] `cargo clippy --lib --tests -- -D warnings` (or at least no new warnings)
- [ ] `cargo test --lib` — all pass
- [ ] `cargo test --test standalone_mysql_server` — pass
- [ ] Full sql-test suite representative run: `filter`, `join`, `aggregate` pick one each and run `--mode verify`
- [ ] `git grep -n "engine::local\|LocalTableSemantics\|stream_load_local_table\|TableStorage::Parquet\|build_parquet_table\|ensure_dual"` returns zero application hits (only docs/plan references)
- [ ] Update `CLAUDE.md` if the file-layout section is affected
- [ ] Final commit:

```bash
git add -p  # curated
git commit -m "docs: update CLAUDE.md after local-table removal"
```

---

## Rollback strategy

Each task lands in a single commit. If any task breaks CI after merging
individual commits, `git revert <commit>` brings the previous state
back. The biggest blast-radius commit is Task 6 (drops
`LocalTableSemantics`); that commit should be isolated so a revert is
clean.

## Post-merge follow-ups (new spec/plan docs)

- Random distribution physical implementation
- Auto-bucket count
- Primary-key managed lake
- `CREATE TABLE AS SELECT` on managed lake
