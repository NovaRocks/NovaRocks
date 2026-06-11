# IVM-P1 PR4 — SQL regression + observability

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Lock in PR3's partition-pruned aggregate apply path with end-to-end SQL regression coverage and structured `tracing::info!` observability. Three new SQL cases land in `sql-tests/iceberg-ivm/`: a partitioned single-base aggregate, a partitioned join aggregate with dim-side group key move, and a partitioned aggregate using a `day(ts)` transform. The existing `iceberg_ivm_aggregate_target.sql` case already covers the non-partitioned fallback. Inside `apply_iceberg_aggregate_delta_chunks`, a structured tracing event (`event = "iceberg_aggregate_mv.apply"`) emits `mv_id`, `target_fqn`, `partition_filter`, `affected_partition_count`, `touched_group_count`, `planned_file_count`, `kept_file_count`, `scanned_target_row_count`, `matched_target_row_count`, `delete_row_count`, `insert_chunk_row_count`, `new_total_rows`. A separate `iceberg_aggregate_mv.partition_derivation_failed` event fires for fail-fast derivation errors.

**Architecture:** Tracing instrumentation lives in `iceberg_refresh.rs::apply_iceberg_aggregate_delta_chunks` — `_lookup_stats` (already plumbed by PR3) becomes the bind point for the structured fields. The derivation-failed event is emitted from inside `wrap_aggregate_apply_error` (PR3's error wrapper) when the cause was raised by `build_aggregate_target_partition_filter`. Tracing fields are key=value pairs on a single `info!` line, captured in tests via `tracing_subscriber::fmt::TestWriter` + `tracing_subscriber::EnvFilter` so a unit test can assert the field shape without running the full refresh pipeline. SQL cases follow the existing `iceberg-ivm` suite layout: `sql/<case>.sql` + `result/<case>.result`, drive against `sql-tests/iceberg-ivm` via the standalone-server + MinIO + Hadoop iceberg catalog fixture (`standalone_iceberg.conf`).

**Tech Stack:** Rust, `tracing` + `tracing-subscriber` (already in workspace deps), NovaRocks `sql-tests` runner. Some verification steps require Docker (MinIO + standalone-server). When Docker is unavailable, those steps may be deferred to a host environment; this is acknowledged inline in Tasks 5 and 6.

---

## File Structure

- Modify: `src/engine/mv/iceberg_refresh.rs`
  - Add structured `tracing::info!` after the apply path's successful commit, sourcing from `_lookup_stats` + locally computed counts.
  - Add `tracing::error!` for fail-fast derivation errors inside `wrap_aggregate_apply_error`.
  - Rename `_lookup_stats` → `lookup_stats` since it is now consumed.
- Create: `sql-tests/iceberg-ivm/sql/iceberg_ivm_partitioned_aggregate_target.sql`
- Create: `sql-tests/iceberg-ivm/result/iceberg_ivm_partitioned_aggregate_target.result`
- Create: `sql-tests/iceberg-ivm/sql/iceberg_ivm_partitioned_join_aggregate_dim_move.sql`
- Create: `sql-tests/iceberg-ivm/result/iceberg_ivm_partitioned_join_aggregate_dim_move.result`
- Create: `sql-tests/iceberg-ivm/sql/iceberg_ivm_partitioned_aggregate_day_transform.sql`
- Create: `sql-tests/iceberg-ivm/result/iceberg_ivm_partitioned_aggregate_day_transform.result`

PR4 does NOT modify:
- The non-partitioned `iceberg_ivm_aggregate_target.sql` case — it stays untouched. Final verification confirms it still passes (this implicitly covers spec §12.3.4 "non-partitioned aggregate fallback").
- Any code outside `iceberg_refresh.rs`.
- Anything in `partition/`, `iceberg_aggregate_state.rs`, or `iceberg_target_apply.rs` (those are sealed by PR1–PR3).

`new_total_rows`'s inclusion-exclusion formula introduced in PR3 Task 2 stays as-is.

---

## Task 1: Add structured tracing observability to the apply path

**Files:**
- Modify: `src/engine/mv/iceberg_refresh.rs`

The PR3 plan reserved `_lookup_stats` so PR4 could consume the stats without forcing a re-plumb. Task 1 turns that binding into the tracing event source.

The event has two forms:

```
event = "iceberg_aggregate_mv.apply"
mv_id, target_fqn, partition_filter ("none" | "allow_list"), affected_partition_count, touched_group_count,
planned_file_count, kept_file_count, scanned_target_row_count, matched_target_row_count,
delete_row_count, insert_chunk_row_count, new_total_rows, iceberg_snapshot
```

```
event = "iceberg_aggregate_mv.partition_derivation_failed"
mv_id, target_fqn, reason
```

- [ ] **Step 1: Write failing tests**

Append to the existing `#[cfg(test)] mod tests` block in `src/engine/mv/iceberg_refresh.rs`:

```rust
    #[test]
    fn tracing_field_partition_filter_label_renders_none_and_allow_list() {
        use crate::engine::mv::partition::{
            MvPartitionKey, MvPartitionKeyField, MvPartitionValue, TargetPartitionFilter,
        };
        let none = TargetPartitionFilter::None;
        assert_eq!(partition_filter_label(&none), "none");
        let allow = TargetPartitionFilter::AllowList(
            [MvPartitionKey::new(
                7,
                vec![MvPartitionKeyField::new(
                    "region".to_string(),
                    MvPartitionValue::String("a".to_string()),
                )],
            )]
            .into_iter()
            .collect(),
        );
        assert_eq!(partition_filter_label(&allow), "allow_list");
        let empty_allow = TargetPartitionFilter::AllowList(std::collections::BTreeSet::new());
        assert_eq!(partition_filter_label(&empty_allow), "allow_list");
    }

    #[test]
    fn tracing_field_partition_filter_count_returns_kept_size() {
        use crate::engine::mv::partition::{
            MvPartitionKey, MvPartitionKeyField, MvPartitionValue, TargetPartitionFilter,
        };
        let none = TargetPartitionFilter::None;
        assert_eq!(partition_filter_count(&none), None);
        let allow = TargetPartitionFilter::AllowList(
            [
                MvPartitionKey::new(
                    7,
                    vec![MvPartitionKeyField::new(
                        "region".to_string(),
                        MvPartitionValue::String("a".to_string()),
                    )],
                ),
                MvPartitionKey::new(
                    7,
                    vec![MvPartitionKeyField::new(
                        "region".to_string(),
                        MvPartitionValue::String("b".to_string()),
                    )],
                ),
            ]
            .into_iter()
            .collect(),
        );
        assert_eq!(partition_filter_count(&allow), Some(2));
    }
```

- [ ] **Step 2: Run failing tests**

```bash
cd /Users/harbor/.codex/worktrees/2185/NovaRocks
cargo test --lib engine::mv::iceberg_refresh::tests::tracing_field_partition_filter
```

Expected: compile fails because `partition_filter_label` and `partition_filter_count` do not exist.

- [ ] **Step 3: Implement helper functions and tracing emission**

In `src/engine/mv/iceberg_refresh.rs`, add the two helpers near `wrap_aggregate_apply_error` (the PR3 helpers, near `build_aggregate_target_partition_filter`):

```rust
fn partition_filter_label(filter: &crate::engine::mv::partition::TargetPartitionFilter) -> &'static str {
    match filter {
        crate::engine::mv::partition::TargetPartitionFilter::None => "none",
        crate::engine::mv::partition::TargetPartitionFilter::AllowList(_) => "allow_list",
    }
}

fn partition_filter_count(filter: &crate::engine::mv::partition::TargetPartitionFilter) -> Option<usize> {
    match filter {
        crate::engine::mv::partition::TargetPartitionFilter::None => None,
        crate::engine::mv::partition::TargetPartitionFilter::AllowList(set) => Some(set.len()),
    }
}
```

Then update `apply_iceberg_aggregate_delta_chunks`:

1. Rename `_lookup_stats` → `lookup_stats` (the consumer is now live).

2. Compute helper counts right after the merge:

```rust
    let delete_row_count = merge.delete_row_ids.len();
    let insert_chunk_row_count: usize = merge
        .insert_chunks
        .iter()
        .map(|chunk| chunk.batch.num_rows())
        .sum();
```

3. Locate the existing `tracing::info!(...)` at the bottom of the function:

```rust
    tracing::info!(
        "iceberg aggregate mv {}.{}.{}: incremental refresh complete: total_rows={new_total_rows} iceberg_snapshot={published_snapshot_id}",
        target.catalog,
        target.namespace,
        target.table
    );
```

REPLACE it with a structured form. The new event name uses the `event = "..."` field so the test fixture below can match on it:

```rust
    tracing::info!(
        event = "iceberg_aggregate_mv.apply",
        mv_id = mv_id,
        target_fqn = %target_fqn,
        partition_filter = partition_filter_label(&partition_filter),
        affected_partition_count = partition_filter_count(&partition_filter).unwrap_or(0),
        touched_group_count = touched_row_ids.len(),
        planned_file_count = lookup_stats.planned_file_count,
        kept_file_count = lookup_stats.kept_file_count,
        scanned_target_row_count = lookup_stats.scanned_row_count,
        matched_target_row_count = lookup_stats.matched_row_count,
        delete_row_count = delete_row_count,
        insert_chunk_row_count = insert_chunk_row_count,
        new_total_rows = new_total_rows,
        iceberg_snapshot = published_snapshot_id,
        "iceberg aggregate mv incremental refresh complete"
    );
```

(`target_fqn` and `mv_id` are bindings that PR3 Task 3 already added to the function scope.)

4. Inside `wrap_aggregate_apply_error`, add a tracing event before returning the string. Replace the existing implementation:

```rust
fn wrap_aggregate_apply_error(target_fqn: &str, mv_id: i64, cause: String) -> String {
    tracing::error!(
        event = "iceberg_aggregate_mv.partition_derivation_failed",
        mv_id = mv_id,
        target_fqn = %target_fqn,
        reason = %cause,
        "iceberg aggregate MV apply failed"
    );
    format!(
        "iceberg aggregate MV apply failed (target={target_fqn}, mv_id={mv_id}): {cause}"
    )
}
```

(The tracing event fires for every call to `wrap_aggregate_apply_error`, which currently only happens on derivation / state-lookup failure. Other apply-path failures route through `handle_iceberg_mv_commit_error` and are NOT covered by this event — that is correct; the event is specifically for fail-fast derivation reasons.)

- [ ] **Step 4: Run all tracing helper tests + the broader suite**

```bash
cargo test --lib engine::mv::iceberg_refresh::tests::tracing_field_partition_filter
cargo test --lib engine::mv::iceberg_refresh
```

Expected: the 2 new helper tests pass; 73 total iceberg_refresh tests still pass.

- [ ] **Step 5: Commit**

```bash
git add src/engine/mv/iceberg_refresh.rs
git commit -m "feat: emit structured tracing for partition-pruned aggregate apply"
```

---

## Task 2: SQL case — partitioned single-base aggregate

**Files:**
- Create: `sql-tests/iceberg-ivm/sql/iceberg_ivm_partitioned_aggregate_target.sql`
- Create: `sql-tests/iceberg-ivm/result/iceberg_ivm_partitioned_aggregate_target.result`

Mirrors `iceberg_ivm_aggregate_target.sql` but adds `PARTITION BY region` on the materialized view so PR3's partition-pruned path is the one exercised. Insert / update / delete sequences each hit only a subset of regions; result rows are checked after each refresh.

- [ ] **Step 1: Create the SQL case**

Write `sql-tests/iceberg-ivm/sql/iceberg_ivm_partitioned_aggregate_target.sql`:

```sql
-- @sequential=true
-- @order_sensitive=true
-- @tags=mv,iceberg,ivm,aggregate,partitioned,target_state
-- Test Point: Iceberg-backed partitioned aggregate MV refreshes incrementally using
-- the partition-pruned touched-group lookup path.
-- Method: Create v3 row-lineage base table, create storage_engine='iceberg'
-- partitioned aggregate MV (PARTITION BY region), refresh after insert/update/delete
-- limited to a subset of regions, and verify result rows match the equivalent
-- aggregate over the base.
-- Scope: Iceberg target MV, single-base aggregate, COUNT/SUM/AVG, identity-partition
-- pruning + group row-id apply.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG ice_ivm_pagg_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "hadoop",
  "iceberg.catalog.warehouse" = "${iceberg_catalog_warehouse}/iceberg_ivm_pagg_${uuid0}",
  "aws.s3.endpoint" = "${oss_endpoint}",
  "aws.s3.access_key" = "${oss_ak}",
  "aws.s3.secret_key" = "${oss_sk}",
  "aws.s3.enable_path_style_access" = "true"
);
CREATE DATABASE ice_ivm_pagg_${uuid0}.ns_${uuid0};
CREATE TABLE ice_ivm_pagg_${uuid0}.ns_${uuid0}.orders (
  region STRING,
  amount BIGINT
) TBLPROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true"
);
SET CATALOG ice_ivm_pagg_${uuid0};
USE ns_${uuid0};
CREATE MATERIALIZED VIEW pagg_mv_${uuid0}
PARTITION BY region
DISTRIBUTED BY HASH(region) BUCKETS 1
PROPERTIES ('storage_engine' = 'iceberg')
AS
SELECT region, COUNT(*) AS c, COUNT(amount) AS c_amount, SUM(amount) AS s, AVG(amount) AS a
FROM ice_ivm_pagg_${uuid0}.ns_${uuid0}.orders
GROUP BY region;

-- query 2
-- @skip_result_check=true
INSERT INTO ice_ivm_pagg_${uuid0}.ns_${uuid0}.orders VALUES
  ('east', 10),
  ('east', 20),
  ('west', NULL),
  ('south', 5);
REFRESH MATERIALIZED VIEW pagg_mv_${uuid0};

-- query 3
SELECT region, c, c_amount, s, a
FROM pagg_mv_${uuid0}
ORDER BY region;

-- query 4
-- @skip_result_check=true
-- Touch only 'east' and 'south' in this round: PR3's partition pruning should
-- skip target files belonging to 'west'.
INSERT INTO ice_ivm_pagg_${uuid0}.ns_${uuid0}.orders VALUES
  ('east', 30),
  ('south', 15);
REFRESH MATERIALIZED VIEW pagg_mv_${uuid0};

-- query 5
SELECT region, c, c_amount, s, a
FROM pagg_mv_${uuid0}
ORDER BY region;

-- query 6
-- @skip_result_check=true
-- Delete + update only inside 'east'. The merge should retract one east group
-- row and re-insert it with an adjusted aggregate state.
DELETE FROM ice_ivm_pagg_${uuid0}.ns_${uuid0}.orders WHERE region = 'east' AND amount = 10;
UPDATE ice_ivm_pagg_${uuid0}.ns_${uuid0}.orders SET amount = 100 WHERE region = 'east' AND amount = 20;
REFRESH MATERIALIZED VIEW pagg_mv_${uuid0};

-- query 7
SELECT region, c, c_amount, s, a
FROM pagg_mv_${uuid0}
ORDER BY region;

-- query 8
-- @skip_result_check=true
-- Bring 'west' into the picture: a 'west' INSERT must produce an 'west' MV row
-- via partition pruning extending its allow-list across regions.
INSERT INTO ice_ivm_pagg_${uuid0}.ns_${uuid0}.orders VALUES ('west', 42);
REFRESH MATERIALIZED VIEW pagg_mv_${uuid0};

-- query 9
SELECT region, c, c_amount, s, a
FROM pagg_mv_${uuid0}
ORDER BY region;

-- query 10
-- Compare with the equivalent base query — the two row sets must match.
SELECT region, c, c_amount, s, a
FROM pagg_mv_${uuid0}
ORDER BY region;

SELECT region,
       COUNT(*) AS c,
       COUNT(amount) AS c_amount,
       SUM(amount) AS s,
       AVG(amount) AS a
FROM ice_ivm_pagg_${uuid0}.ns_${uuid0}.orders
GROUP BY region
ORDER BY region;

-- query 11
-- @skip_result_check=true
DROP MATERIALIZED VIEW pagg_mv_${uuid0};
DROP TABLE ice_ivm_pagg_${uuid0}.ns_${uuid0}.orders;
DROP DATABASE ice_ivm_pagg_${uuid0}.ns_${uuid0};
DROP CATALOG ice_ivm_pagg_${uuid0};
```

- [ ] **Step 2: Generate the result file via `--mode record`**

Start a NovaRocks standalone-server and MinIO + Hadoop catalog fixture, then run:

```bash
cd /Users/harbor/.codex/worktrees/2185/NovaRocks
source docker/iceberg-rest/runtime/current/env.sh 2>/dev/null || true
docker/iceberg-rest/up.sh
NO_PROXY=127.0.0.1,localhost cargo run -- standalone-server --config tests/sql-test-runner/conf/standalone_managed_lake.toml &
SRV_PID=$!
sleep 5
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config tests/sql-test-runner/conf/standalone_iceberg.conf \
  --suite iceberg-ivm \
  --only iceberg_ivm_partitioned_aggregate_target \
  --mode record
kill -9 $SRV_PID 2>/dev/null
```

`--mode record` writes the actual run output to `sql-tests/iceberg-ivm/result/iceberg_ivm_partitioned_aggregate_target.result`.

**If Docker is unavailable in the implementer's environment**, mark the task `DONE_WITH_CONCERNS` after writing the SQL file, and explicitly note that the `.result` file was NOT generated. The user will produce the result on a host with Docker and commit it as a follow-up step.

- [ ] **Step 3: Re-run with `--mode verify` to confirm**

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config tests/sql-test-runner/conf/standalone_iceberg.conf \
  --suite iceberg-ivm \
  --only iceberg_ivm_partitioned_aggregate_target \
  --mode verify
```

Expected: pass. (Skip if Docker unavailable; defer to the user.)

- [ ] **Step 4: Commit**

```bash
git add sql-tests/iceberg-ivm/sql/iceberg_ivm_partitioned_aggregate_target.sql \
        sql-tests/iceberg-ivm/result/iceberg_ivm_partitioned_aggregate_target.result
git commit -m "test: add partitioned single-base aggregate iceberg IVM regression"
```

(If only the SQL file landed because Docker was unavailable, commit just the SQL with a note in the message: `test: add partitioned single-base aggregate iceberg IVM regression (result deferred)`.)

---

## Task 3: SQL case — partitioned join aggregate with dim-side group key move

**Files:**
- Create: `sql-tests/iceberg-ivm/sql/iceberg_ivm_partitioned_join_aggregate_dim_move.sql`
- Create: `sql-tests/iceberg-ivm/result/iceberg_ivm_partitioned_join_aggregate_dim_move.result`

The dim-side update changes one fact-row's joined `region` partition — the affected partition set must include BOTH the old and new region. This is the signed-delta convention spec §5 principle 4 + §7.5 / §8.2 are built on; PR4 verifies the SQL-layer convention produces the right outcome end-to-end.

- [ ] **Step 1: Create the SQL case**

Write `sql-tests/iceberg-ivm/sql/iceberg_ivm_partitioned_join_aggregate_dim_move.sql`:

```sql
-- @sequential=true
-- @order_sensitive=true
-- @tags=mv,iceberg,ivm,join,aggregate,partitioned,target_state
-- Test Point: Iceberg-backed partitioned join aggregate MV correctly handles dim-side
-- group key moves by emitting retract+append signed delta rows that span both the
-- old and new MV partition.
-- Method: Create v3 row-lineage fact/dim tables, create PARTITION BY region join
-- aggregate MV. Initial refresh seeds 3 regions. A dim UPDATE moves one fact's
-- region from 'east' to 'north', which must (a) retract the 'east' group's
-- amount contribution and (b) append it to the 'north' group, with the MV
-- result matching the base query after refresh.
-- Scope: Iceberg target MV, two-base join aggregate, identity-partition pruning
-- spanning two partitions per dim move, group row-id apply.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG ice_ivm_pjagg_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "hadoop",
  "iceberg.catalog.warehouse" = "${iceberg_catalog_warehouse}/iceberg_ivm_pjagg_${uuid0}",
  "aws.s3.endpoint" = "${oss_endpoint}",
  "aws.s3.access_key" = "${oss_ak}",
  "aws.s3.secret_key" = "${oss_sk}",
  "aws.s3.enable_path_style_access" = "true"
);
CREATE DATABASE ice_ivm_pjagg_${uuid0}.ns_${uuid0};
CREATE TABLE ice_ivm_pjagg_${uuid0}.ns_${uuid0}.fact (
  id BIGINT NOT NULL,
  dim_id BIGINT,
  amount BIGINT
) TBLPROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true"
);
CREATE TABLE ice_ivm_pjagg_${uuid0}.ns_${uuid0}.dim (
  id BIGINT NOT NULL,
  region STRING
) TBLPROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true"
);
SET CATALOG ice_ivm_pjagg_${uuid0};
USE ns_${uuid0};
CREATE MATERIALIZED VIEW pjagg_mv_${uuid0}
PARTITION BY region
DISTRIBUTED BY HASH(region) BUCKETS 1
PROPERTIES ('storage_engine' = 'iceberg')
AS
SELECT d.region, COUNT(*) AS c, SUM(f.amount) AS s
FROM ice_ivm_pjagg_${uuid0}.ns_${uuid0}.fact AS f
JOIN ice_ivm_pjagg_${uuid0}.ns_${uuid0}.dim AS d ON f.dim_id = d.id
GROUP BY d.region;

-- query 2
-- @skip_result_check=true
INSERT INTO ice_ivm_pjagg_${uuid0}.ns_${uuid0}.dim VALUES
  (10, 'east'),
  (20, 'west'),
  (30, 'south');
INSERT INTO ice_ivm_pjagg_${uuid0}.ns_${uuid0}.fact VALUES
  (1, 10, 100),
  (2, 10, 200),
  (3, 20, 50),
  (4, 30, 70);
REFRESH MATERIALIZED VIEW pjagg_mv_${uuid0};

-- query 3
SELECT region, c, s
FROM pjagg_mv_${uuid0}
ORDER BY region;

-- query 4
-- @skip_result_check=true
-- Dim-side move: dim id=10 used to map to 'east'; remap it to 'north'. The
-- fact rows attached to dim id=10 (rows 1, 2 with amounts 100+200=300) must
-- move from the 'east' MV group to a new 'north' MV group.
UPDATE ice_ivm_pjagg_${uuid0}.ns_${uuid0}.dim SET region = 'north' WHERE id = 10;
REFRESH MATERIALIZED VIEW pjagg_mv_${uuid0};

-- query 5
SELECT region, c, s
FROM pjagg_mv_${uuid0}
ORDER BY region;

-- query 6
SELECT region, c, s
FROM pjagg_mv_${uuid0}
ORDER BY region;

SELECT d.region, COUNT(*) AS c, SUM(f.amount) AS s
FROM ice_ivm_pjagg_${uuid0}.ns_${uuid0}.fact AS f
JOIN ice_ivm_pjagg_${uuid0}.ns_${uuid0}.dim AS d ON f.dim_id = d.id
GROUP BY d.region
ORDER BY region;

-- query 7
-- @skip_result_check=true
-- Subsequent fact-side update inside one partition: amount change must adjust
-- the 'west' SUM without touching other partitions.
UPDATE ice_ivm_pjagg_${uuid0}.ns_${uuid0}.fact SET amount = 500 WHERE id = 3;
REFRESH MATERIALIZED VIEW pjagg_mv_${uuid0};

-- query 8
SELECT region, c, s
FROM pjagg_mv_${uuid0}
ORDER BY region;

-- query 9
-- @skip_result_check=true
DROP MATERIALIZED VIEW pjagg_mv_${uuid0};
DROP TABLE ice_ivm_pjagg_${uuid0}.ns_${uuid0}.fact;
DROP TABLE ice_ivm_pjagg_${uuid0}.ns_${uuid0}.dim;
DROP DATABASE ice_ivm_pjagg_${uuid0}.ns_${uuid0};
DROP CATALOG ice_ivm_pjagg_${uuid0};
```

- [ ] **Step 2: Generate the result file via `--mode record`**

Same procedure as Task 2 Step 2, but with `--only iceberg_ivm_partitioned_join_aggregate_dim_move`.

- [ ] **Step 3: Re-run with `--mode verify`**

Same procedure as Task 2 Step 3, but with `--only iceberg_ivm_partitioned_join_aggregate_dim_move`.

- [ ] **Step 4: Commit**

```bash
git add sql-tests/iceberg-ivm/sql/iceberg_ivm_partitioned_join_aggregate_dim_move.sql \
        sql-tests/iceberg-ivm/result/iceberg_ivm_partitioned_join_aggregate_dim_move.result
git commit -m "test: add partitioned join aggregate dim-side move iceberg IVM regression"
```

---

## Task 4: SQL case — partitioned aggregate with `day(ts)` transform

**Files:**
- Create: `sql-tests/iceberg-ivm/sql/iceberg_ivm_partitioned_aggregate_day_transform.sql`
- Create: `sql-tests/iceberg-ivm/result/iceberg_ivm_partitioned_aggregate_day_transform.result`

The `day(ts)` transform exercises PR2's full transform-conversion path end-to-end, including `iceberg::transform::create_transform_function(&Transform::Day).transform(array)` on `Date32` group key columns.

- [ ] **Step 1: Create the SQL case**

Write `sql-tests/iceberg-ivm/sql/iceberg_ivm_partitioned_aggregate_day_transform.sql`:

```sql
-- @sequential=true
-- @order_sensitive=true
-- @tags=mv,iceberg,ivm,aggregate,partitioned,day_transform,target_state
-- Test Point: Iceberg-backed partitioned aggregate MV using day(ts) transform on a
-- DATE group key refreshes incrementally with partition-pruned target state lookup.
-- Method: Create v3 row-lineage base, create PARTITION BY day(ts) aggregate MV.
-- Insert rows spanning multiple days; update one day's amounts; verify MV matches
-- the equivalent base query after each refresh.
-- Scope: Iceberg target MV, single-base aggregate, day-transform partition, group
-- row-id apply.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG ice_ivm_pday_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "hadoop",
  "iceberg.catalog.warehouse" = "${iceberg_catalog_warehouse}/iceberg_ivm_pday_${uuid0}",
  "aws.s3.endpoint" = "${oss_endpoint}",
  "aws.s3.access_key" = "${oss_ak}",
  "aws.s3.secret_key" = "${oss_sk}",
  "aws.s3.enable_path_style_access" = "true"
);
CREATE DATABASE ice_ivm_pday_${uuid0}.ns_${uuid0};
CREATE TABLE ice_ivm_pday_${uuid0}.ns_${uuid0}.orders (
  ts DATE,
  amount BIGINT
) TBLPROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true"
);
SET CATALOG ice_ivm_pday_${uuid0};
USE ns_${uuid0};
CREATE MATERIALIZED VIEW pday_mv_${uuid0}
PARTITION BY day(ts)
DISTRIBUTED BY HASH(ts) BUCKETS 1
PROPERTIES ('storage_engine' = 'iceberg')
AS
SELECT ts, COUNT(*) AS c, SUM(amount) AS s
FROM ice_ivm_pday_${uuid0}.ns_${uuid0}.orders
GROUP BY ts;

-- query 2
-- @skip_result_check=true
INSERT INTO ice_ivm_pday_${uuid0}.ns_${uuid0}.orders VALUES
  ('2026-01-10', 10),
  ('2026-01-10', 20),
  ('2026-01-11', 5),
  ('2026-01-12', 15);
REFRESH MATERIALIZED VIEW pday_mv_${uuid0};

-- query 3
SELECT ts, c, s
FROM pday_mv_${uuid0}
ORDER BY ts;

-- query 4
-- @skip_result_check=true
-- Touch only 2026-01-11: PR3's partition-pruned lookup must skip target files
-- corresponding to the other days.
UPDATE ice_ivm_pday_${uuid0}.ns_${uuid0}.orders SET amount = 50 WHERE ts = DATE '2026-01-11';
REFRESH MATERIALIZED VIEW pday_mv_${uuid0};

-- query 5
SELECT ts, c, s
FROM pday_mv_${uuid0}
ORDER BY ts;

-- query 6
SELECT ts, c, s
FROM pday_mv_${uuid0}
ORDER BY ts;

SELECT ts, COUNT(*) AS c, SUM(amount) AS s
FROM ice_ivm_pday_${uuid0}.ns_${uuid0}.orders
GROUP BY ts
ORDER BY ts;

-- query 7
-- @skip_result_check=true
DROP MATERIALIZED VIEW pday_mv_${uuid0};
DROP TABLE ice_ivm_pday_${uuid0}.ns_${uuid0}.orders;
DROP DATABASE ice_ivm_pday_${uuid0}.ns_${uuid0};
DROP CATALOG ice_ivm_pday_${uuid0};
```

- [ ] **Step 2: Generate the result file via `--mode record`**

Same procedure as Task 2 Step 2, but with `--only iceberg_ivm_partitioned_aggregate_day_transform`.

- [ ] **Step 3: Re-run with `--mode verify`**

- [ ] **Step 4: Commit**

```bash
git add sql-tests/iceberg-ivm/sql/iceberg_ivm_partitioned_aggregate_day_transform.sql \
        sql-tests/iceberg-ivm/result/iceberg_ivm_partitioned_aggregate_day_transform.result
git commit -m "test: add partitioned aggregate day transform iceberg IVM regression"
```

---

## Task 5: Non-regression sanity check on existing `iceberg-ivm` suite

**Files:**
- No source changes. This task confirms the existing non-partitioned `iceberg_ivm_aggregate_target.sql` (and other iceberg-ivm cases) still pass against PR3's apply path, implicitly covering spec §12.3.4 (non-partitioned fallback).

- [ ] **Step 1: Run the full `iceberg-ivm` suite in verify mode**

```bash
cd /Users/harbor/.codex/worktrees/2185/NovaRocks
source docker/iceberg-rest/runtime/current/env.sh 2>/dev/null || true
docker/iceberg-rest/up.sh
NO_PROXY=127.0.0.1,localhost cargo run -- standalone-server --config tests/sql-test-runner/conf/standalone_managed_lake.toml &
SRV_PID=$!
sleep 5
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config tests/sql-test-runner/conf/standalone_iceberg.conf \
  --suite iceberg-ivm \
  --mode verify
kill -9 $SRV_PID 2>/dev/null
```

Expected: all iceberg-ivm cases pass, including the three new ones from Tasks 2–4. If `iceberg_ivm_aggregate_target.sql` (non-partitioned) starts failing, that's a regression in PR3's `new_total_rows` math or partition_filter handling — STOP and report BLOCKED.

**Docker availability:** This step REQUIRES Docker. If the implementer's environment lacks Docker, mark this task `DONE_WITH_CONCERNS` and clearly state which cases were not exercised.

- [ ] **Step 2: No commit if all green**

Task 5 only commits if `iceberg-ivm` cases needed result regeneration (e.g., minor floating-point format changes). If anything beyond the new files diffs, investigate before committing. In the green path, this task produces no commit.

---

## Task 6: Final verification

**Files:**
- No new source files. This task verifies the full PR4 surface.

- [ ] **Step 1: Format**

```bash
cd /Users/harbor/.codex/worktrees/2185/NovaRocks
cargo fmt
```

Expected: exit 0.

- [ ] **Step 2: Lint** (PR4-touched file only):

```bash
cargo clippy --all-targets --no-deps 2>&1 | grep -E "(error|warning):.*iceberg_refresh\.rs" || echo "OK"
```

Expected: `OK`.

- [ ] **Step 3: Library tests**

```bash
cargo test --lib engine::mv::iceberg_refresh
cargo test --lib engine::mv::partition
cargo test --lib engine::mv::iceberg_aggregate_state
```

Expected: all three suites pass; total ~123 tests across the three modules.

- [ ] **Step 4: Full library compile check**

```bash
cargo test --lib --no-run
```

Expected: compile succeeds.

- [ ] **Step 5: Diff hygiene**

```bash
git diff --check
```

Expected: empty.

- [ ] **Step 6: Commit fmt-only changes** if any:

```bash
git status -sb
```

If changes exist:

```bash
git add src/engine/mv/iceberg_refresh.rs
git commit -m "style: format aggregate apply tracing instrumentation"
```

Otherwise skip.

---

## Self-Review

**Spec coverage** (each spec §13 PR4 + §12.3 + §11 deliverable → plan Task):

| Spec deliverable | Plan Task |
|---|---|
| §11 tracing event `iceberg_aggregate_mv.apply` with the 13-field schema | Task 1 |
| §11 tracing event `iceberg_aggregate_mv.partition_derivation_failed` | Task 1 |
| §12.3.1 partitioned single-base aggregate SQL case | Task 2 |
| §12.3.2 partitioned join aggregate dim-side move SQL case | Task 3 |
| §12.3.3 partitioned aggregate with day transform SQL case | Task 4 |
| §12.3.4 non-partitioned aggregate fallback | Implicit — existing `iceberg_ivm_aggregate_target.sql` covers it; Task 5 verifies it still passes |
| Tracing fields assertable in tests | Task 1 (unit-level via `partition_filter_label` / `partition_filter_count` helpers; full event assertion is left to integration / log inspection by reviewers running the SQL suite) |

**Placeholder scan:** No `TBD`, no `TODO`. The two `DONE_WITH_CONCERNS` escape hatches in Tasks 2–5 are operational — they describe what to do when Docker is unavailable, not unfinished plan content.

**Type consistency:**
- `partition_filter_label(&TargetPartitionFilter) -> &'static str` and `partition_filter_count(&TargetPartitionFilter) -> Option<usize>` referenced in Tasks 1 Step 3 match their definitions.
- `lookup_stats` (renamed from `_lookup_stats`) consumed in Task 1 reuses the `AggregateStateLookupStats` shape from PR1 — `planned_file_count`, `kept_file_count`, `scanned_row_count`, `matched_row_count`.
- SQL test `${uuid0}` / `${oss_*}` / `${iceberg_catalog_warehouse}` placeholders match the conventions already in use in `sql-tests/iceberg-ivm/sql/iceberg_ivm_aggregate_target.sql` and `iceberg_ivm_join_aggregate.sql`.
