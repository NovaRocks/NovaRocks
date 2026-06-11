# mv-on-iceberg → iceberg-ivm Consolidation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Migrate 9 unique-coverage cases from `sql-tests/mv-on-iceberg/` to `sql-tests/iceberg-ivm/` with OLAP-target → Iceberg-target MV semantic conversion; delete 8 redundant cases; delete the source directory.

**Architecture:** Per KEEP case: rewrite the `CREATE MATERIALIZED VIEW` to use `PROPERTIES('storage_engine'='iceberg')`, ensure base table uses `format-version=3` + `write.row-lineage=true`, re-record `.result`. Per DROP case: `git rm` source. After both pass, `git rm -r sql-tests/mv-on-iceberg`.

**Tech Stack:** sql-tests/ layout, Iceberg hadoop catalog over MinIO via `docker/iceberg-rest`, NovaRocks `standalone-server`.

**Spec:** [docs/design/specs/2026-05-25-mv-on-iceberg-consolidation-design.md](../specs/2026-05-25-mv-on-iceberg-consolidation-design.md)

**No-fallback rule:** if any KEEP case fails after conversion, stop and report the exact failing statement + error. Fix the NovaRocks gap in this PR (preferred) or defer the case with a follow-up fix task.

---

## Catalog / property conversion mapping

For each KEEP case, the following edits apply:

| Edit | Before (OLAP-target) | After (Iceberg-target) |
|---|---|---|
| MV storage | (default — no `storage_engine`) | `PROPERTIES('storage_engine'='iceberg')` |
| Base format | (varies — sometimes v2 default) | `TBLPROPERTIES ("format-version"="3", "write.row-lineage"="true")` |
| Base lineage | not required for OLAP target | required for Iceberg target IVM |
| OLAP-only MV props | e.g. `replication_num` | drop |
| `.result` | mv-on-iceberg recording | re-record on iceberg-ivm |

---

## Prerequisites

Same as previous PRs in this branch:

```bash
docker/iceberg-rest/up.sh
source docker/iceberg-rest/runtime/current/env.sh
cargo build
# Start standalone-server (kill any existing first)
lsof -ti :9055 2>/dev/null | xargs -I {} kill -9 {} 2>/dev/null; sleep 2
LOG=/tmp/novarocks-icecon.log
NO_PROXY=127.0.0.1,localhost target/debug/novarocks standalone-server \
  --config "$NOVAROCKS_STANDALONE_CONFIG" >"$LOG" 2>&1 &
SRV_PID=$!
for i in $(seq 1 60); do
  grep -q '^NOVAROCKS_READY ' "$LOG" && break
  if ! kill -0 "$SRV_PID" 2>/dev/null; then tail -20 "$LOG"; exit 1; fi
  sleep 1
done
grep '^NOVAROCKS_READY ' "$LOG" || { tail -20 "$LOG"; exit 1; }
echo "$SRV_PID" > /tmp/novarocks-icecon.pid

SQLT="cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests --"
```

---

## Task 1: Migrate basic + AVG + strategy + PK (4 cases)

The 4 "DDL / lifecycle / strategy / validation" cases. None drive row-level mutation directly; the conversion is mostly MV property + base v3 properties.

**Files:**
- Create: `sql-tests/iceberg-ivm/sql/iceberg_backed_mv_basic_lifecycle.sql`
- Create: `sql-tests/iceberg-ivm/sql/iceberg_backed_mv_aggregate_avg_min_max.sql`
- Create: `sql-tests/iceberg-ivm/sql/iceberg_backed_mv_refresh_strategy.sql`
- Create: `sql-tests/iceberg-ivm/sql/iceberg_backed_mv_pk_invalid.sql`
- Create: matching `.result` files via `--mode record`

For each, read the source `sql-tests/mv-on-iceberg/sql/<source>.sql`, apply the conversion, write the destination file, record, verify.

- [ ] **Step 1: Convert `managed_lake_mv_basic.sql` → `iceberg_backed_mv_basic_lifecycle.sql`**

Read `sql-tests/mv-on-iceberg/sql/managed_lake_mv_basic.sql`. Apply:
- `CREATE TABLE` for base: add `TBLPROPERTIES ("format-version"="3", "write.row-lineage"="true")` if absent.
- `CREATE MATERIALIZED VIEW` for MV: add `PROPERTIES('storage_engine'='iceberg')`. Drop any OLAP-only props like `replication_num`.
- Keep all other queries (refresh, select, append, SHOW, DROP) unchanged.
- Rename to `iceberg_backed_mv_basic_lifecycle.sql`.

Record + verify:
```bash
$SQLT --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-ivm --only iceberg_backed_mv_basic_lifecycle \
  --mode record --record-from target
cat sql-tests/iceberg-ivm/result/iceberg_backed_mv_basic_lifecycle.result
$SQLT --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-ivm --only iceberg_backed_mv_basic_lifecycle \
  --mode verify
```

If any failure surfaces (e.g., a property combination is rejected, or SHOW MATERIALIZED VIEW outputs differ in a way that breaks the original assertions), stop and report.

- [ ] **Step 2: Convert `managed_lake_mv_aggregate_avg_min_max.sql` → `iceberg_backed_mv_aggregate_avg_min_max.sql`**

Same conversion pattern. Verify the AVG state-type behavior and DDL rejection messages still hold.

```bash
$SQLT --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-ivm --only iceberg_backed_mv_aggregate_avg_min_max \
  --mode record --record-from target
$SQLT --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-ivm --only iceberg_backed_mv_aggregate_avg_min_max \
  --mode verify
```

- [ ] **Step 3: Convert `managed_lake_mv_iceberg_ivm_strategy.sql` → `iceberg_backed_mv_refresh_strategy.sql`**

This case exercises INSERT OVERWRITE refresh fallback. The OLAP-target MV may have different fallback rules than the Iceberg-target MV — if the assertions about strategy choices diverge, capture the actual behavior and decide:
- If Iceberg target also falls back to full refresh on INSERT OVERWRITE → keep the assertion form.
- If Iceberg target behavior differs (e.g., refuses INSERT OVERWRITE entirely, or doesn't fall back) → stop and report; this is a strategy-policy decision that needs documentation in the case header.

```bash
$SQLT --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-ivm --only iceberg_backed_mv_refresh_strategy \
  --mode record --record-from target
$SQLT --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-ivm --only iceberg_backed_mv_refresh_strategy \
  --mode verify
```

- [ ] **Step 4: Convert `managed_lake_mv_ivm_pk_invalid.sql` → `iceberg_backed_mv_pk_invalid.sql`**

PK DDL validation is at MV-create time. Iceberg-target MV PK validation might use different error strings than OLAP-target; tighten `@expect_error` substrings after recording.

```bash
$SQLT --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-ivm --only iceberg_backed_mv_pk_invalid \
  --mode record --record-from target
# Update @expect_error substrings if Iceberg-target errors differ; re-record
$SQLT --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-ivm --only iceberg_backed_mv_pk_invalid \
  --mode verify
```

- [ ] **Step 5: Commit batch 1**

```bash
git add sql-tests/iceberg-ivm/sql/iceberg_backed_mv_basic_lifecycle.sql \
        sql-tests/iceberg-ivm/sql/iceberg_backed_mv_aggregate_avg_min_max.sql \
        sql-tests/iceberg-ivm/sql/iceberg_backed_mv_refresh_strategy.sql \
        sql-tests/iceberg-ivm/sql/iceberg_backed_mv_pk_invalid.sql \
        sql-tests/iceberg-ivm/result/iceberg_backed_mv_basic_lifecycle.result \
        sql-tests/iceberg-ivm/result/iceberg_backed_mv_aggregate_avg_min_max.result \
        sql-tests/iceberg-ivm/result/iceberg_backed_mv_refresh_strategy.result \
        sql-tests/iceberg-ivm/result/iceberg_backed_mv_pk_invalid.result 2>/dev/null
git commit -m "test(iceberg-ivm): migrate 4 unique mv-on-iceberg cases (basic/avg/strategy/pk_invalid) with Iceberg-target conversion"
```

Note: some cases may have no `.result` (negative-only with `@expect_error`); the `2>/dev/null` swallows missing-file errors.

---

## Task 2: Migrate MERGE INTO COW + MOR (2 cases)

Source cases drive `MERGE INTO` against v3 row-lineage base tables with COW or MOR update mode. They're already on v3; just need the MV target conversion.

**Files:**
- Create: `sql-tests/iceberg-ivm/sql/iceberg_backed_mv_merge_cow.sql`
- Create: `sql-tests/iceberg-ivm/sql/iceberg_backed_mv_merge_mor.sql`
- Create: matching `.result` files

- [ ] **Step 1: Convert `managed_lake_mv_merge_cow.sql`**

Apply: MV gets `PROPERTIES('storage_engine'='iceberg')`. Base v3 + row-lineage should already be set in the source.

```bash
$SQLT --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-ivm --only iceberg_backed_mv_merge_cow \
  --mode record --record-from target
$SQLT --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-ivm --only iceberg_backed_mv_merge_cow \
  --mode verify
```

If `MERGE INTO` against Iceberg-target MV (i.e., MV needs to refresh after MERGE on base) hits a NovaRocks gap, stop and report — Iceberg-target MV refresh from MERGE-driven snapshots may be a new code path not previously exercised in iceberg-ivm.

- [ ] **Step 2: Convert `managed_lake_mv_merge_mor.sql`**

Same conversion. The MOR variant uses NovaRocks update-marker snapshots — those should already work for Iceberg-target IVM via iceberg-ivm's UPDATE handling.

```bash
$SQLT --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-ivm --only iceberg_backed_mv_merge_mor \
  --mode record --record-from target
$SQLT --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-ivm --only iceberg_backed_mv_merge_mor \
  --mode verify
```

- [ ] **Step 3: Commit batch 2**

```bash
git add sql-tests/iceberg-ivm/sql/iceberg_backed_mv_merge_cow.sql \
        sql-tests/iceberg-ivm/sql/iceberg_backed_mv_merge_mor.sql \
        sql-tests/iceberg-ivm/result/iceberg_backed_mv_merge_cow.result \
        sql-tests/iceberg-ivm/result/iceberg_backed_mv_merge_mor.result
git commit -m "test(iceberg-ivm): migrate MERGE INTO COW + MOR cases from mv-on-iceberg"
```

---

## Task 3: Migrate projection hidden PK + partition evolution + recreate (3 cases)

The three remaining KEEP cases that exercise more specialized planner shapes.

**Files:**
- Create: `sql-tests/iceberg-ivm/sql/iceberg_backed_mv_projection_hidden_pk_delete.sql`
- Create: `sql-tests/iceberg-ivm/sql/iceberg_backed_mv_projection_partition_evolution_delete.sql`
- Create: `sql-tests/iceberg-ivm/sql/iceberg_backed_mv_base_recreate_and_rewrite.sql`
- Create: matching `.result` files

- [ ] **Step 1: Convert `managed_lake_mv_projection_hidden_pk_delete.sql`**

MV with user-declared PK that's hidden from the SELECT output. Apply Iceberg-target conversion. Verify delete refresh still applies through the hidden PK.

```bash
$SQLT --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-ivm --only iceberg_backed_mv_projection_hidden_pk_delete \
  --mode record --record-from target
$SQLT --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-ivm --only iceberg_backed_mv_projection_hidden_pk_delete \
  --mode verify
```

- [ ] **Step 2: Convert `managed_lake_mv_read_semantics_partition_v3_delete.sql` → `iceberg_backed_mv_projection_partition_evolution_delete.sql`**

Partition spec evolution (DROP PARTITION COLUMN) + cross-spec DELETE on projection MV. Apply Iceberg-target conversion.

```bash
$SQLT --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-ivm --only iceberg_backed_mv_projection_partition_evolution_delete \
  --mode record --record-from target
$SQLT --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-ivm --only iceberg_backed_mv_projection_partition_evolution_delete \
  --mode verify
```

- [ ] **Step 3: Convert `test_mv_with_iceberg_recreate.sql` → `iceberg_backed_mv_base_recreate_and_rewrite.sql`**

Apply Iceberg-target conversion. The case exercises information_schema.materialized_views inspection — verify the output format stays similar (Iceberg-target MV should still appear in this virtual table).

```bash
$SQLT --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-ivm --only iceberg_backed_mv_base_recreate_and_rewrite \
  --mode record --record-from target
$SQLT --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-ivm --only iceberg_backed_mv_base_recreate_and_rewrite \
  --mode verify
```

- [ ] **Step 4: Commit batch 3**

```bash
git add sql-tests/iceberg-ivm/sql/iceberg_backed_mv_projection_hidden_pk_delete.sql \
        sql-tests/iceberg-ivm/sql/iceberg_backed_mv_projection_partition_evolution_delete.sql \
        sql-tests/iceberg-ivm/sql/iceberg_backed_mv_base_recreate_and_rewrite.sql \
        sql-tests/iceberg-ivm/result/iceberg_backed_mv_projection_hidden_pk_delete.result \
        sql-tests/iceberg-ivm/result/iceberg_backed_mv_projection_partition_evolution_delete.result \
        sql-tests/iceberg-ivm/result/iceberg_backed_mv_base_recreate_and_rewrite.result
git commit -m "test(iceberg-ivm): migrate hidden PK + partition evolution + recreate cases from mv-on-iceberg"
```

---

## Task 4: Delete `sql-tests/mv-on-iceberg/`

All 17 mv-on-iceberg cases are either migrated (9) or redundant with iceberg-ivm (8). Delete the source directory.

- [ ] **Step 1: Confirm no external references**

```bash
grep -rn '"mv-on-iceberg"\|--suite mv-on-iceberg' tests/sql-test-runner/src/ docs/design/ 2>/dev/null \
  | grep -v "2026-05-25-mv-on-iceberg\|2026-04-\|2026-05-0" | head -5
```

Expected: no functional references (historical plan docs are OK).

- [ ] **Step 2: Remove the directory**

```bash
git rm -r sql-tests/mv-on-iceberg
```

- [ ] **Step 3: Confirm runner no longer discovers it**

```bash
source docker/iceberg-rest/runtime/current/env.sh
$SQLT --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite mv-on-iceberg --mode verify 2>&1 | grep -E "unknown suite|ERROR" | head -1
```

Expected: `unknown suite 'mv-on-iceberg'`.

- [ ] **Step 4: Commit**

```bash
git commit -m "test: delete sql-tests/mv-on-iceberg/ suite; 9 unique cases migrated to iceberg-ivm, 8 redundant cases dropped"
```

---

## Task 5: Whole-suite verify

- [ ] **Step 1: Verify iceberg-ivm (60 cases expected)**

```bash
source docker/iceberg-rest/runtime/current/env.sh
$SQLT --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-ivm --mode verify 2>&1 | grep -E "total=|pass=|fail=" | tail -3
```

Expected: total=60, pass=60, fail=0.

- [ ] **Step 2: Confirm mv-on-iceberg is gone**

```bash
$SQLT --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite mv-on-iceberg --mode verify 2>&1 | grep "unknown suite"
```

- [ ] **Step 3: Verify other iceberg-* sub-suites still green**

```bash
for s in iceberg iceberg-ddl iceberg-dml; do
  echo "--- $s ---"
  $SQLT --config "$NOVAROCKS_SQL_TEST_CONFIG" \
    --suite "$s" --mode verify 2>&1 | grep -E "total=|pass=|fail=" | tail -3
done
```

All three baseline counts from PR #173 should hold (iceberg=24, iceberg-ddl=47, iceberg-dml=37).

---

## Task 6: Final hygiene

- [ ] **Step 1: `cargo fmt`**

```bash
cargo fmt
git status
# Commit any fmt fixups
git add -A
git commit -m "chore: cargo fmt after mv-on-iceberg consolidation" 2>/dev/null || echo "nothing to format"
```

- [ ] **Step 2: Stop standalone-server**

```bash
kill "$(cat /tmp/novarocks-icecon.pid)" 2>/dev/null || true
rm -f /tmp/novarocks-icecon.pid
```

- [ ] **Step 3: Print final commit log**

```bash
git log --oneline main..HEAD
```

Expected: a clean stack of focused commits (~6 commits: 3 migration batches + delete dir + maybe fmt).

---

## Appendix: What the engineer must remember

1. **OLAP-target → Iceberg-target conversion**: MV gets `PROPERTIES('storage_engine'='iceberg')`, base gets `format-version=3` + `write.row-lineage=true`.
2. **No silent fallback**: any failure → stop and report. Don't paper over by removing tests.
3. **Re-record `.result`**: row format differs between OLAP-target and Iceberg-target MVs. Always `--mode record` fresh.
4. **8 DROP cases**: `managed_lake_mv_aggregate_ivm`, `managed_lake_mv_equality_delete`, `managed_lake_mv_incremental`, `managed_lake_mv_projection_delete`, `managed_lake_mv_projection_v3_delete`, `managed_lake_mv_read_semantics_equality_delete`, `managed_lake_mv_update_cow`, `managed_lake_mv_update_mor` — these vanish with the source directory in Task 4.
5. **Cross-suite invariants**: existing iceberg / iceberg-ddl / iceberg-dml counts from PR #173 must stay (24 / 47 / 37).
