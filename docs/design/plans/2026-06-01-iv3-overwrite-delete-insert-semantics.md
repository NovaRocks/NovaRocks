# IV3 Overwrite Delete-Insert Semantics Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 让 NovaRocks `INSERT OVERWRITE` 在 Iceberg v3 row-lineage 表上与 Spark/Flink replacement 语义对齐：覆盖范围内旧 rows 是 DELETE，新输出 rows 是 INSERT，并且 IVM 不再把 overwrite 中业务列相同的 rows 当作 unchanged no-op。

**Architecture:** 保持现有 commit path 的文件替换模型：`OverwriteCommit` 整表替换，`OverwritePartitionsCommit` 只替换 touched partitions，正常新写 data files 继续从 snapshot row range 分配新 `_row_id`。主要实现点在 change planning：删除 ordinary `Operation::Overwrite` 的 unchanged-row allow-list 优化，让 IVM 始终看到 added data files 和 deleted data files 组成的 delete+insert delta。SQL 回归覆盖 full-table overwrite 和 dynamic partition overwrite 的 row-id 行为，现有 IVM overwrite suite 作为 delete-bearing delta 的端到端保护。

**Tech Stack:** Rust, Apache Iceberg v3 row lineage, NovaRocks standalone SQL engine, SQL test runner, Cargo.

---

## 文件结构

- Modify: `src/connector/iceberg/changes.rs`
  - 删除 `plan_changes` 中对 ordinary overwrite 的 `compute_overwrite_unchanged_rows` 调用。
  - 删除 `compute_overwrite_unchanged_rows` 和它的私有 reader helper `read_stored_row_ids_from_file`。
  - 更新 `DataFileRef::row_id_allow_list` 注释，明确普通 overwrite 不会填充该字段。
  - 强化已有 overwrite diff unit test，断言 overwrite added files 不携带 `row_id_allow_list`。
- Create: `sql-tests/iceberg-dml/sql/overwrite_row_lineage_delete_insert.sql`
  - 新增整表 `INSERT OVERWRITE` row-id 回归：业务列完全相同的 rows 覆盖后 `_row_id` 必须变化。
- Create: `sql-tests/iceberg-dml/result/overwrite_row_lineage_delete_insert.result`
  - 新 SQL 的 golden result。
- Create: `sql-tests/iceberg-dml/sql/overwrite_partitions_row_lineage.sql`
  - 新增 dynamic partition overwrite row-id 回归：touched partition 的同业务行获得新 `_row_id`，untouched partition 保留旧 `_row_id`。
- Create: `sql-tests/iceberg-dml/result/overwrite_partitions_row_lineage.result`
  - 新 SQL 的 golden result。
- Verify only: `sql-tests/iceberg-rest/sql/iceberg_rest_ivm_change_op_delta_source.sql`
  - 不需要改 SQL；它已经覆盖 projection MV 和 aggregate MV 在 overwrite 后保留业务列不变的 Bob 行，并覆盖整组 retraction。
- Verify only: `sql-tests/iceberg-ivm/sql/iceberg_backed_mv_refresh_strategy.sql`
  - 不需要改 SQL；它覆盖 Iceberg-backed aggregate MV 在 overwrite 后的 refresh 结果。
- No expected code change: `src/connector/iceberg/commit/overwrite.rs`
  - 当前已经使用 `effective_next_row_id` 和 `with_row_range(first_row_id, added_rows)` 给 normal overwrite added rows 分配新 row-id range。
- No expected code change: `src/connector/iceberg/commit/overwrite_partitions.rs`
  - 当前已经只 carry forward untouched partitions，并给 touched partition 的 new files 分配新 row-id range。

### Task 1: Change Planner 删除 overwrite unchanged-row shortcut

**Files:**
- Modify: `src/connector/iceberg/changes.rs`

- [ ] **Step 1: 先记录当前不符合目标语义的 shortcut**

Run:

```bash
rg -n "compute_overwrite_unchanged_rows|shares stored `_row_id`s|row_id_allow_list.*Overwrite" src/connector/iceberg/changes.rs
```

Expected before implementation: output includes the `plan_changes` call to `compute_overwrite_unchanged_rows`, the helper definition, and the comment that says overwrite shared `_row_id`s can be skipped.

- [ ] **Step 2: 强化已有 Rust unit test**

In `src/connector/iceberg/changes.rs`, inside `plan_changes_collects_overwrite_added_and_deleted_data_files`, change the `create_table` properties argument from:

```rust
            &[],
```

to:

```rust
            &[
                ("format-version".to_string(), "3".to_string()),
                ("write.row-lineage".to_string(), "true".to_string()),
            ],
```

At the end of the same test, after the existing deleted row-count assertion, add:

```rust
        assert!(
            batch.inserts.iter().all(|f| f.row_id_allow_list.is_none()),
            "ordinary overwrite must expose every added row as an insert; inserts={:?}",
            batch.inserts
        );
```

- [ ] **Step 3: Run the focused Rust test before implementation**

Run:

```bash
cargo test --lib connector::iceberg::changes::tests::plan_changes_collects_overwrite_added_and_deleted_data_files -- --nocapture
```

Expected before implementation: the test must compile and exercise the v3 overwrite diff path. If it fails with a `row_id_allow_list` assertion, that is the red signal for this task. If it passes because the fixture's deleted data files inherit `first_row_id` only at manifest level, keep the assertion anyway; Step 1 is still the deterministic red signal proving the shortcut remains in code.

- [ ] **Step 4: Update `DataFileRef::row_id_allow_list` documentation**

Replace the existing `row_id_allow_list` doc comment in `DataFileRef` with:

```rust
    /// Optional IVM-only scan-time filter for explicitly row-preserving
    /// mutation paths. Ordinary `Operation::Overwrite` snapshots must leave
    /// this as `None` so IVM observes overwrite as delete+insert.
    pub row_id_allow_list: Option<std::collections::BTreeSet<i64>>,
```

- [ ] **Step 5: Remove the call from `plan_changes`**

In `plan_changes`, delete this block:

```rust
    // V3 row-lineage optimisation: when an Overwrite snapshot's added file
    // shares stored `_row_id`s with a deleted file in the same partition,
    // those rows are unchanged and IVM can skip them. Populate per-file
    // `row_id_allow_list`s describing the rows that ARE actually new.
    // Conservative on errors: silently leave the allow list as `None`,
    // which preserves the current (correct but over-counted) behaviour.
    if !deleted_data_files.is_empty() && !inserts.is_empty() {
        compute_overwrite_unchanged_rows(table, &mut inserts, &deleted_data_files, None)?;
    }
```

The code after collection should go directly to:

```rust
    Ok(IcebergChangeBatch {
        previous_snapshot_id,
        current_snapshot_id,
        inserts,
        deletes,
        equality_deletes,
        deleted_data_files,
    })
```

- [ ] **Step 6: Delete the obsolete helper functions**

Delete both obsolete functions completely:

```rust
pub(crate) fn compute_overwrite_unchanged_rows(
    table: &iceberg::table::Table,
    added_files: &mut [DataFileRef],
    deleted_files: &[DeletedDataFileRef],
    object_store_config: Option<&crate::fs::object_store::ObjectStoreConfig>,
) -> Result<(), ChangeError>
```

and:

```rust
fn read_stored_row_ids_from_file(
    table: &iceberg::table::Table,
    path: &str,
    size: i64,
    first_row_id: i64,
    object_store_config: Option<&crate::fs::object_store::ObjectStoreConfig>,
) -> Result<Vec<i64>, String>
```

Do not delete `build_factory_for_table`; it is still used by deleted-row reverse projection.

- [ ] **Step 7: Verify the shortcut is gone**

Run:

```bash
! rg -n "compute_overwrite_unchanged_rows|shares stored `_row_id`s" src/connector/iceberg/changes.rs
```

Expected after implementation: command exits successfully because `rg` finds no matches.

- [ ] **Step 8: Run focused Rust verification**

Run:

```bash
cargo test --lib connector::iceberg::changes::tests::plan_changes_collects_overwrite_added_and_deleted_data_files -- --nocapture
```

Expected: PASS.

- [ ] **Step 9: Commit Task 1**

```bash
git add src/connector/iceberg/changes.rs
git commit -m "fix: treat iceberg overwrite changes as delete insert"
```

### Task 2: Full-table overwrite row-lineage SQL regression

**Files:**
- Create: `sql-tests/iceberg-dml/sql/overwrite_row_lineage_delete_insert.sql`
- Create: `sql-tests/iceberg-dml/result/overwrite_row_lineage_delete_insert.result`

- [ ] **Step 1: Create the SQL case**

Create `sql-tests/iceberg-dml/sql/overwrite_row_lineage_delete_insert.sql` with:

```sql
-- @order_sensitive=true
-- @tags=write_path,iceberg,row_lineage,overwrite
-- Test Point:
--   Iceberg v3 full-table INSERT OVERWRITE is delete+insert, not
--   row-lineage carry-forward. Even when business columns are identical,
--   overwritten rows receive fresh _row_id values.

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.t_full_overwrite_lineage (
  id INT,
  v INT
)
TBLPROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true"
);
INSERT INTO ${case_db}.t_full_overwrite_lineage VALUES
  (1, 10),
  (2, 20);
ALTER TABLE ${case_db}.t_full_overwrite_lineage CREATE TAG before_overwrite;

-- query 2
SELECT id, v
FROM ${case_db}.t_full_overwrite_lineage
ORDER BY id;

-- query 3
-- @skip_result_check=true
INSERT OVERWRITE ${case_db}.t_full_overwrite_lineage VALUES
  (1, 10),
  (2, 20);

-- query 4
SELECT id, v
FROM ${case_db}.t_full_overwrite_lineage
ORDER BY id;

-- query 5
SELECT
  (
    SELECT COUNT(*)
    FROM (
      SELECT id, _row_id
      FROM ${case_db}.t_full_overwrite_lineage
    ) cur
    JOIN (
      SELECT id, _row_id
      FROM ${case_db}.t_full_overwrite_lineage FOR VERSION AS OF 'before_overwrite'
    ) old
    ON cur.id = old.id
    WHERE cur._row_id = old._row_id
  ) AS same_row_ids,
  (
    SELECT COUNT(*)
    FROM (
      SELECT id, _row_id
      FROM ${case_db}.t_full_overwrite_lineage
    ) cur
    JOIN (
      SELECT id, _row_id
      FROM ${case_db}.t_full_overwrite_lineage FOR VERSION AS OF 'before_overwrite'
    ) old
    ON cur.id = old.id
    WHERE cur._row_id <> old._row_id
  ) AS changed_row_ids,
  (
    SELECT COUNT(*)
    FROM ${case_db}.t_full_overwrite_lineage
  ) AS current_rows,
  (
    SELECT COUNT(*)
    FROM ${case_db}.t_full_overwrite_lineage FOR VERSION AS OF 'before_overwrite'
  ) AS historical_rows;

-- query 6
-- @skip_result_check=true
ALTER TABLE ${case_db}.t_full_overwrite_lineage DROP TAG before_overwrite;
DROP TABLE ${case_db}.t_full_overwrite_lineage FORCE;
```

- [ ] **Step 2: Create the expected result**

Create `sql-tests/iceberg-dml/result/overwrite_row_lineage_delete_insert.result` with:

```text
-- query 2
id	v
1	10
2	20

-- query 4
id	v
1	10
2	20

-- query 5
same_row_ids	changed_row_ids	current_rows	historical_rows
0	2	2	2
```

- [ ] **Step 3: Run the new SQL case**

Use an already running generated standalone environment, or start one as shown in Task 5. Then run:

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-dml \
  --only overwrite_row_lineage_delete_insert \
  --mode verify \
  -j 1
```

Expected: PASS. A failure with `same_row_ids = 2` means the commit path preserved row identity for full overwrite and must be fixed before continuing.

- [ ] **Step 4: Commit Task 2**

```bash
git add sql-tests/iceberg-dml/sql/overwrite_row_lineage_delete_insert.sql \
        sql-tests/iceberg-dml/result/overwrite_row_lineage_delete_insert.result
git commit -m "test: cover iceberg overwrite row lineage replacement"
```

### Task 3: Dynamic partition overwrite row-lineage SQL regression

**Files:**
- Create: `sql-tests/iceberg-dml/sql/overwrite_partitions_row_lineage.sql`
- Create: `sql-tests/iceberg-dml/result/overwrite_partitions_row_lineage.result`

- [ ] **Step 1: Create the SQL case**

Create `sql-tests/iceberg-dml/sql/overwrite_partitions_row_lineage.sql` with:

```sql
-- @order_sensitive=true
-- @tags=write_path,iceberg,row_lineage,overwrite_partitions
-- Test Point:
--   Dynamic partition overwrite is delete+insert inside touched
--   partitions and carry-forward only outside touched partitions.

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.t_partition_overwrite_lineage (
  id INT,
  region VARCHAR(8)
)
PARTITION BY (region)
TBLPROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true"
);
INSERT INTO ${case_db}.t_partition_overwrite_lineage VALUES
  (1, 'us'),
  (2, 'us'),
  (3, 'eu'),
  (4, 'eu');
ALTER TABLE ${case_db}.t_partition_overwrite_lineage CREATE TAG before_overwrite;

-- query 2
SELECT region, COUNT(*) AS n
FROM ${case_db}.t_partition_overwrite_lineage
GROUP BY region
ORDER BY region;

-- query 3
-- @skip_result_check=true
INSERT OVERWRITE PARTITIONS ${case_db}.t_partition_overwrite_lineage VALUES
  (1, 'us'),
  (99, 'us');

-- query 4
SELECT id, region
FROM ${case_db}.t_partition_overwrite_lineage
ORDER BY id;

-- query 5
SELECT
  (
    SELECT COUNT(*)
    FROM (
      SELECT id, region, _row_id
      FROM ${case_db}.t_partition_overwrite_lineage
    ) cur
    JOIN (
      SELECT id, region, _row_id
      FROM ${case_db}.t_partition_overwrite_lineage FOR VERSION AS OF 'before_overwrite'
    ) old
    ON cur.id = old.id AND cur.region = old.region
    WHERE cur.region = 'us' AND cur._row_id = old._row_id
  ) AS same_touched_rows,
  (
    SELECT COUNT(*)
    FROM (
      SELECT id, region, _row_id
      FROM ${case_db}.t_partition_overwrite_lineage
    ) cur
    JOIN (
      SELECT id, region, _row_id
      FROM ${case_db}.t_partition_overwrite_lineage FOR VERSION AS OF 'before_overwrite'
    ) old
    ON cur.id = old.id AND cur.region = old.region
    WHERE cur.region = 'us' AND cur._row_id <> old._row_id
  ) AS changed_touched_rows,
  (
    SELECT COUNT(*)
    FROM (
      SELECT id, region, _row_id
      FROM ${case_db}.t_partition_overwrite_lineage
    ) cur
    JOIN (
      SELECT id, region, _row_id
      FROM ${case_db}.t_partition_overwrite_lineage FOR VERSION AS OF 'before_overwrite'
    ) old
    ON cur.id = old.id AND cur.region = old.region
    WHERE cur.region = 'eu' AND cur._row_id = old._row_id
  ) AS same_untouched_rows,
  (
    SELECT COUNT(*)
    FROM ${case_db}.t_partition_overwrite_lineage
    WHERE region = 'us'
  ) AS current_us_rows,
  (
    SELECT COUNT(*)
    FROM ${case_db}.t_partition_overwrite_lineage
    WHERE region = 'eu'
  ) AS current_eu_rows;

-- query 6
-- @skip_result_check=true
ALTER TABLE ${case_db}.t_partition_overwrite_lineage DROP TAG before_overwrite;
DROP TABLE ${case_db}.t_partition_overwrite_lineage FORCE;
```

- [ ] **Step 2: Create the expected result**

Create `sql-tests/iceberg-dml/result/overwrite_partitions_row_lineage.result` with:

```text
-- query 2
region	n
eu	2
us	2

-- query 4
id	region
1	us
3	eu
4	eu
99	us

-- query 5
same_touched_rows	changed_touched_rows	same_untouched_rows	current_us_rows	current_eu_rows
0	1	2	2	2
```

- [ ] **Step 3: Run the new SQL case**

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-dml \
  --only overwrite_partitions_row_lineage \
  --mode verify \
  -j 1
```

Expected: PASS. A failure with `same_touched_rows = 1` means touched partitions incorrectly carried row identity forward. A failure with `same_untouched_rows = 0` means untouched partition carry-forward was broken.

- [ ] **Step 4: Commit Task 3**

```bash
git add sql-tests/iceberg-dml/sql/overwrite_partitions_row_lineage.sql \
        sql-tests/iceberg-dml/result/overwrite_partitions_row_lineage.result
git commit -m "test: cover partition overwrite row lineage scope"
```

### Task 4: IVM overwrite regression verification

**Files:**
- Verify only: `sql-tests/iceberg-rest/sql/iceberg_rest_ivm_change_op_delta_source.sql`
- Verify only: `sql-tests/iceberg-rest/result/iceberg_rest_ivm_change_op_delta_source.result`
- Verify only: `sql-tests/iceberg-ivm/sql/iceberg_backed_mv_refresh_strategy.sql`
- Verify only: `sql-tests/iceberg-ivm/result/iceberg_backed_mv_refresh_strategy.result`

- [ ] **Step 1: Run REST IVM overwrite delta-source regression**

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-rest \
  --only iceberg_rest_ivm_change_op_delta_source \
  --mode verify \
  -j 1
```

Expected: PASS. This case must still show Bob after the first overwrite refresh and after the second overwrite refresh:

```text
-- query 8
id	amount
1	80
2	40

-- query 9
customer	total_amount
Alice	80
Bob	40

-- query 13
id	amount
2	40

-- query 14
customer	total_amount
Bob	40
```

- [ ] **Step 2: Run Iceberg-backed MV refresh strategy regression**

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-ivm \
  --only iceberg_backed_mv_refresh_strategy \
  --mode verify \
  -j 1
```

Expected: PASS. The overwrite refresh result must remain:

```text
-- query 9
customer	c	s
A	2	320
B	1	130
```

- [ ] **Step 3: Commit Task 4 only if files changed**

If Task 4 only ran verification, do not create a commit. If a golden result changes because behavior is now correctly delete+insert, inspect the row-level delta first, then commit the minimal SQL/result update:

```bash
git add sql-tests/iceberg-rest/sql/iceberg_rest_ivm_change_op_delta_source.sql \
        sql-tests/iceberg-rest/result/iceberg_rest_ivm_change_op_delta_source.result \
        sql-tests/iceberg-ivm/sql/iceberg_backed_mv_refresh_strategy.sql \
        sql-tests/iceberg-ivm/result/iceberg_backed_mv_refresh_strategy.result
git commit -m "test: verify ivm overwrite delete insert refresh"
```

### Task 5: Full verification

**Files:**
- Verify working tree and all changed files.

- [ ] **Step 1: Format Rust code**

```bash
cargo fmt
```

Expected: exits 0.

- [ ] **Step 2: Run focused Rust tests**

```bash
cargo test --lib connector::iceberg::changes::tests::plan_changes_collects_overwrite_added_and_deleted_data_files -- --nocapture
```

Expected: PASS.

- [ ] **Step 3: Prepare local Iceberg REST environment**

```bash
source docker/iceberg-rest/runtime/current/env.sh
docker/iceberg-rest/up.sh
cargo build --profile dev-opt
```

Expected: Docker fixture is up and `target/dev-opt/novarocks` exists.

- [ ] **Step 4: Start standalone-server using generated config**

```bash
source docker/iceberg-rest/runtime/current/env.sh
LOG=/tmp/novarocks-iv3-overwrite-delete-insert.log
NO_PROXY=127.0.0.1,localhost target/dev-opt/novarocks standalone-server \
  --config "$NOVAROCKS_STANDALONE_CONFIG" >"$LOG" 2>&1 &
SRV_PID=$!
for i in $(seq 1 60); do
  if grep -q '^NOVAROCKS_READY ' "$LOG"; then break; fi
  if ! kill -0 "$SRV_PID" 2>/dev/null; then
    echo "standalone-server died during startup; tail of $LOG:" >&2
    tail -20 "$LOG" >&2
    exit 1
  fi
  sleep 1
done
grep -q '^NOVAROCKS_READY ' "$LOG" || {
  echo "timed out waiting for NOVAROCKS_READY" >&2
  kill -9 "$SRV_PID"
  exit 1
}
```

Expected: log contains `NOVAROCKS_READY mysql_port=<port>`.

- [ ] **Step 5: Run all targeted SQL cases**

```bash
source docker/iceberg-rest/runtime/current/env.sh
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-dml \
  --only overwrite_row_lineage_delete_insert,overwrite_partitions_row_lineage \
  --mode verify \
  -j 1

cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-rest \
  --only iceberg_rest_ivm_change_op_delta_source \
  --mode verify \
  -j 1

cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-ivm \
  --only iceberg_backed_mv_refresh_strategy \
  --mode verify \
  -j 1
```

Expected: all three runner commands PASS.

- [ ] **Step 6: Stop the standalone-server started by this task**

```bash
kill "$SRV_PID"
wait "$SRV_PID" 2>/dev/null || true
```

Expected: process exits.

- [ ] **Step 7: Final source scan**

```bash
! rg -n "compute_overwrite_unchanged_rows|shares stored `_row_id`s" src/connector/iceberg/changes.rs
rg -n "row_id_allow_list" src/connector/iceberg/changes.rs src/engine src/exec src/lower
git status --short
```

Expected:

```text
# first command prints nothing and exits 0
# second command still shows the field/consumer plumbing, but no ordinary overwrite producer
# git status shows only intentional files before final commit, then clean after commit
```

- [ ] **Step 8: Final commit if Task 5 introduced formatting-only changes**

If `cargo fmt` changed files not included in earlier commits, commit those formatting changes with the relevant task files:

```bash
git add src/connector/iceberg/changes.rs \
        sql-tests/iceberg-dml/sql/overwrite_row_lineage_delete_insert.sql \
        sql-tests/iceberg-dml/result/overwrite_row_lineage_delete_insert.result \
        sql-tests/iceberg-dml/sql/overwrite_partitions_row_lineage.sql \
        sql-tests/iceberg-dml/result/overwrite_partitions_row_lineage.result
git commit -m "test: verify iceberg overwrite delete insert semantics"
```

## 自检清单

- Spec coverage:
  - Full-table overwrite delete+insert: Task 2.
  - Dynamic partition overwrite touched/untouched scope: Task 3.
  - IVM projection and aggregate over overwrite delta: Task 4.
  - No ordinary overwrite `row_id_allow_list`: Task 1 and Task 5 final scan.
  - Commit path fresh row-id allocation: Task 2 and Task 3 SQL assertions.
- Placeholder scan:
  - No placeholder markers are present in this plan.
- Type consistency:
  - `DataFileRef::row_id_allow_list` remains `Option<std::collections::BTreeSet<i64>>`.
  - `plan_changes` still returns `IcebergChangeBatch` with `inserts` and `deleted_data_files`.
  - SQL test names match the `--only` runner arguments exactly.
