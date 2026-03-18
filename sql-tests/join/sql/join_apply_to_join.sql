-- @tags=join,apply,subquery,correlated
-- Test Objective:
-- 1. Validate LEFT JOIN with correlated subqueries (IN, EXISTS, scalar) in ON clause.
-- 2. Prevent regressions where apply-to-join transformation produces wrong row counts.
-- Test Flow:
-- 1. Create three tables (t0, t1, t2) with nullable BIGINT columns, insert 9 rows each
--    including NULLs.
-- 2. Execute various LEFT JOIN queries with correlated IN subqueries, EXISTS subqueries,
--    and scalar subqueries in the ON clause.
-- 3. Assert correct count(*) for each variant.

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.t0 (
  `v1` bigint NULL COMMENT "",
  `v2` bigint NULL COMMENT "",
  `v3` bigint NULL
) ENGINE=OLAP
DUPLICATE KEY(`v1`, `v2`, v3)
DISTRIBUTED BY HASH(`v1`) BUCKETS 3
PROPERTIES (
"replication_num" = "1"
);

-- query 2
-- @skip_result_check=true
CREATE TABLE ${case_db}.t1 (
  `v4` bigint NULL COMMENT "",
  `v5` bigint NULL COMMENT "",
  `v6` bigint NULL
) ENGINE=OLAP
DUPLICATE KEY(`v4`, `v5`, v6)
DISTRIBUTED BY HASH(`v4`) BUCKETS 3
PROPERTIES (
"replication_num" = "1"
);

-- query 3
-- @skip_result_check=true
CREATE TABLE ${case_db}.t2 (
  `v7` bigint NULL COMMENT "",
  `v8` bigint NULL COMMENT "",
  `v9` bigint NULL
) ENGINE=OLAP
DUPLICATE KEY(`v7`, `v8`, v9)
DISTRIBUTED BY HASH(`v7`) BUCKETS 3
PROPERTIES (
"replication_num" = "1"
);

-- query 4
-- @skip_result_check=true
INSERT INTO ${case_db}.t0 VALUES (null, 1, 1), (2, null, null), (3, 3, null), (4, 4, 4), (5, 5, 5), (6, 6, 6), (null, null, null), (7, 7, 7), (8, 8, 8);

-- query 5
-- @skip_result_check=true
INSERT INTO ${case_db}.t1 VALUES (null, 1, 1), (2, null, null), (3, 3, null), (4, 4, 4), (5, 5, 5), (6, 6, 6), (null, null, null), (7, 7, 7), (8, 8, 8);

-- query 6
-- @skip_result_check=true
INSERT INTO ${case_db}.t2 VALUES (null, 1, 1), (2, null, null), (3, 3, null), (4, 4, 4), (5, 5, 5), (6, 6, 6), (null, null, null), (7, 7, 7), (8, 8, 8);

-- query 7
SELECT count(*) FROM ${case_db}.t0 LEFT JOIN ${case_db}.t1 ON v1 IN (SELECT v7 FROM ${case_db}.t2) OR v1 < v5;

-- query 8
SELECT count(*) FROM ${case_db}.t0 LEFT JOIN ${case_db}.t1 ON v1 IN (SELECT v7 FROM ${case_db}.t2 WHERE v2 = v8) OR v1 < v5;

-- query 9
SELECT count(*) FROM ${case_db}.t0 LEFT JOIN ${case_db}.t1 ON v4 IN (SELECT v7 FROM ${case_db}.t2) OR v1 < v5;

-- query 10
SELECT count(*) FROM ${case_db}.t0 LEFT JOIN ${case_db}.t1 ON v4 IN (SELECT v7 FROM ${case_db}.t2 WHERE v5 = v8) OR v1 < v5;

-- query 11
SELECT count(*) FROM ${case_db}.t0 LEFT JOIN ${case_db}.t1 ON EXISTS (SELECT v7 FROM ${case_db}.t2) OR v1 < v5;

-- query 12
SELECT count(*) FROM ${case_db}.t0 LEFT JOIN ${case_db}.t1 ON EXISTS (SELECT v7 FROM ${case_db}.t2 WHERE v8 = v1) OR v1 < v5;

-- query 13
SELECT count(*) FROM ${case_db}.t0 LEFT JOIN ${case_db}.t1 ON EXISTS (SELECT v7 FROM ${case_db}.t2 WHERE v8 = v4) OR v1 < v5;

-- query 14
SELECT count(*) FROM ${case_db}.t0 LEFT JOIN ${case_db}.t1 ON (SELECT count(*) FROM ${case_db}.t2 WHERE v4 = v7) > 0;

-- query 15
SELECT count(*) FROM ${case_db}.t0 LEFT JOIN ${case_db}.t1 ON (SELECT count(*) FROM ${case_db}.t2 WHERE v4 = v7) > 0 OR v1 > v4;

-- query 16
SELECT count(*) FROM ${case_db}.t0 LEFT JOIN ${case_db}.t1 ON (SELECT count(*) FROM ${case_db}.t2 WHERE v4 = v7) > v1 + v5 OR v1 > v4;

-- query 17
SELECT count(*) FROM ${case_db}.t0 LEFT JOIN ${case_db}.t1 ON (SELECT count(*) FROM ${case_db}.t2) > 0 OR v1 + v4 = v2 + v5;

-- query 18
SELECT count(*) FROM ${case_db}.t0 LEFT JOIN ${case_db}.t1 ON (SELECT count(*) FROM ${case_db}.t2) > v1 + v4 OR v1 < v4;
