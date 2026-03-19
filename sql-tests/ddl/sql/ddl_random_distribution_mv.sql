-- Migrated from dev/test/sql/test_random_distribution/T/test_random_distribution
-- Test Objective:
-- 1. Sync MV (rollup) can be created on a RANDOM distributed base table.
-- 2. Async MV with DISTRIBUTED BY RANDOM: default and explicit.
-- 3. SHOW CREATE MATERIALIZED VIEW shows DISTRIBUTED BY RANDOM.
-- 4. SHOW CREATE MATERIALIZED VIEW fails for sync rollup MV (not an async MV object).
-- 5. After REFRESH WITH SYNC MODE, explain shows rollup rewrite on async MV.

-- query 1
-- Sync MV (rollup) on RANDOM distributed table.
-- @skip_result_check=true
CREATE TABLE ${case_db}.t_sync (k INT, v INT);
CREATE MATERIALIZED VIEW ${case_db}.mv_sync AS SELECT k, sum(v) FROM ${case_db}.t_sync GROUP BY k;
INSERT INTO ${case_db}.t_sync VALUES (1, 1), (1, 2);

-- query 2
SELECT sum(v) FROM ${case_db}.t_sync;

-- query 3
-- Async MV with default RANDOM distribution (no explicit DISTRIBUTED BY).
-- @skip_result_check=true
CREATE TABLE ${case_db}.t_async (k INT, v INT);
CREATE MATERIALIZED VIEW ${case_db}.mv_async_a
  REFRESH ASYNC
  AS SELECT k, sum(v) FROM ${case_db}.t_async GROUP BY k;

-- query 4
-- SHOW CREATE MATERIALIZED VIEW shows DISTRIBUTED BY RANDOM and REFRESH ASYNC.
-- @result_contains=DISTRIBUTED BY RANDOM
-- @result_contains=REFRESH ASYNC
SHOW CREATE MATERIALIZED VIEW ${case_db}.mv_async_a;

-- query 5
-- Async MV with explicit DISTRIBUTED BY RANDOM (manual refresh).
-- @skip_result_check=true
CREATE MATERIALIZED VIEW ${case_db}.mv_async_b
  DISTRIBUTED BY RANDOM
  AS SELECT k, sum(v) FROM ${case_db}.t_async GROUP BY k;

-- query 6
-- SHOW CREATE shows DISTRIBUTED BY RANDOM and REFRESH MANUAL.
-- @result_contains=DISTRIBUTED BY RANDOM
-- @result_contains=REFRESH MANUAL
SHOW CREATE MATERIALIZED VIEW ${case_db}.mv_async_b;

-- query 7
-- Sync rollup MV (created without REFRESH clause).
-- @skip_result_check=true
CREATE MATERIALIZED VIEW ${case_db}.mv_sync_rollup
  AS SELECT k, sum(v) FROM ${case_db}.t_async GROUP BY k;

-- query 8
-- SHOW CREATE MATERIALIZED VIEW on a sync rollup MV returns error (not an async MV object).
-- @expect_error=is not found
SHOW CREATE MATERIALIZED VIEW ${case_db}.mv_sync_rollup;

-- query 9
-- SHOW ALTER TABLE ROLLUP shows the rollup job for the sync MV.
-- @result_contains=mv_sync_rollup
-- @retry_count=20
-- @retry_interval_ms=500
SHOW ALTER TABLE ROLLUP FROM ${case_db};

-- query 10
-- After REFRESH WITH SYNC MODE, explain should rewrite through async MV rollup.
-- @skip_result_check=true
CREATE TABLE ${case_db}.t_rewrite (k INT, v INT);
INSERT INTO ${case_db}.t_rewrite VALUES (1, 1);
CREATE MATERIALIZED VIEW ${case_db}.mv_rewrite
  REFRESH ASYNC
  AS SELECT k, sum(v) FROM ${case_db}.t_rewrite GROUP BY k;
REFRESH MATERIALIZED VIEW ${case_db}.mv_rewrite WITH SYNC MODE;

-- query 11
-- Explain should show rollup: mv_rewrite (MV rewrite via async MV).
-- @result_contains=rollup: mv_rewrite
EXPLAIN SELECT sum(v) FROM ${case_db}.t_rewrite;
