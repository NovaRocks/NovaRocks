-- @tags=join,right_outer,bucket_shuffle
-- Test Objective:
-- 1. Validate RIGHT OUTER JOIN with [bucket] and [colocate] hints produces correct row counts.
-- 2. Prevent regressions where pruned right-outer local bucket-shuffle joins lose unmatched rows.
-- Test Flow:
-- 1. Create a table with hash-distributed BIGINT keys and insert 10000 rows.
-- 2. Run a CTE chain: w1 filters to a single key, w2 does right-outer bucket join,
--    w3 does right-outer colocate join of w1 and w2.
-- 3. Assert count is 10000 (all right-side rows preserved).
-- 4. Run the same query multiple times to ensure stability.

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.t1 (
  k1 bigint NULL,
  c1 bigint
) ENGINE=OLAP
DUPLICATE KEY(`k1`)
DISTRIBUTED BY HASH(`k1`) BUCKETS 6
PROPERTIES (
    "replication_num" = "1"
);

-- query 2
-- @skip_result_check=true
INSERT INTO ${case_db}.t1 SELECT generate_series, generate_series FROM TABLE(generate_series(0, 10000 - 1));

-- query 3
WITH
  w1 AS (SELECT k1 FROM ${case_db}.t1 WHERE k1 = 10),
  w2 AS (SELECT k1 FROM ${case_db}.t1 tt1 RIGHT OUTER JOIN [bucket] ${case_db}.t1 tt2 USING(k1)),
  w3 AS (SELECT k1 FROM w1 RIGHT OUTER JOIN [colocate] w2 USING(k1))
SELECT count(1) FROM w3;

-- query 4
WITH
  w1 AS (SELECT k1 FROM ${case_db}.t1 WHERE k1 = 10),
  w2 AS (SELECT k1 FROM ${case_db}.t1 tt1 RIGHT OUTER JOIN [bucket] ${case_db}.t1 tt2 USING(k1)),
  w3 AS (SELECT k1 FROM w1 RIGHT OUTER JOIN [colocate] w2 USING(k1))
SELECT count(1) FROM w3;
