-- BITMAP / HLL type misuse fail-fast checks.
--
-- This case verifies that the analyzer / DDL layer rejects the BITMAP / HLL
-- misuse patterns documented in the BITMAP / HLL plan (PR-B2):
--   1. ORDER BY  on a BITMAP / HLL column
--   2. GROUP BY  on a BITMAP / HLL column
--   3. comparison operators against a BITMAP / HLL column
--   4. IN / BETWEEN / NOT IN predicates against a BITMAP / HLL column
--   5. DISTRIBUTED BY HASH(...)  over a BITMAP / HLL column
--
-- Each rejection is asserted via the runner's `@expect_error` directive
-- so the matched substring documents the user-facing error message.

-- query 1: baseline table creation must succeed
-- @skip_result_check=true
CREATE DATABASE IF NOT EXISTS ${case_db};
CREATE TABLE ${case_db}.t_bm_hll (
  k INT,
  bm BITMAP,
  hv HLL
)
TBLPROPERTIES ("format-version" = "3");

-- query 2: ORDER BY on a BITMAP column must be rejected
-- @expect_error=BITMAP/HLL columns cannot appear in ORDER BY
SELECT k FROM ${case_db}.t_bm_hll ORDER BY bm;

-- query 3: ORDER BY on an HLL column must be rejected
-- @expect_error=BITMAP/HLL columns cannot appear in ORDER BY
SELECT k FROM ${case_db}.t_bm_hll ORDER BY hv;

-- query 4: GROUP BY on a BITMAP column must be rejected
-- @expect_error=BITMAP/HLL columns cannot appear in GROUP BY
SELECT bm FROM ${case_db}.t_bm_hll GROUP BY bm;

-- query 5: GROUP BY on an HLL column must be rejected
-- @expect_error=BITMAP/HLL columns cannot appear in GROUP BY
SELECT hv FROM ${case_db}.t_bm_hll GROUP BY hv;

-- query 6: equality comparison against a BITMAP column must be rejected
-- @expect_error=comparison operator
SELECT k FROM ${case_db}.t_bm_hll WHERE bm = bm;

-- query 7: equality comparison against an HLL column must be rejected
-- @expect_error=comparison operator
SELECT k FROM ${case_db}.t_bm_hll WHERE hv = hv;

-- query 8: DISTRIBUTED BY HASH over a BITMAP column must be rejected
-- @expect_error=BITMAP/HLL columns cannot be used as distribution key
CREATE TABLE ${case_db}.t_dist_bm (k INT, bm BITMAP)
DISTRIBUTED BY HASH(bm) BUCKETS 1
TBLPROPERTIES ("format-version" = "3");

-- query 9: DISTRIBUTED BY HASH over an HLL column must be rejected
-- @expect_error=BITMAP/HLL columns cannot be used as distribution key
CREATE TABLE ${case_db}.t_dist_hll (k INT, hv HLL)
DISTRIBUTED BY HASH(hv) BUCKETS 1
TBLPROPERTIES ("format-version" = "3");

-- query 10: IN list against a BITMAP column must be rejected
-- @expect_error=BITMAP/HLL columns cannot appear in IN
SELECT k FROM ${case_db}.t_bm_hll WHERE bm IN (bm);

-- query 11: BETWEEN against a BITMAP column must be rejected
-- @expect_error=BITMAP/HLL columns cannot appear in BETWEEN
SELECT k FROM ${case_db}.t_bm_hll WHERE bm BETWEEN bm AND bm;

-- query 12: NOT IN against a BITMAP column must be rejected
-- @expect_error=BITMAP/HLL columns cannot appear in NOT IN
SELECT k FROM ${case_db}.t_bm_hll WHERE bm NOT IN (bm);

-- query 13: cleanup probe table
-- @skip_result_check=true
DROP TABLE ${case_db}.t_bm_hll;
