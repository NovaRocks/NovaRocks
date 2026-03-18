-- @tags=join,other_predicate,like
-- Test Objective:
-- 1. Validate INNER JOIN with LIKE-based other (residual) predicates.
-- 2. Prevent regressions where LIKE with concat patterns on join output fails or returns wrong counts.
-- Test Flow:
-- 1. Create two tables and insert 40960 rows plus one all-NULL row.
-- 2. Execute joins with various LIKE patterns (exact, prefix, contains, suffix) as residual predicates.
-- 3. Assert count is 40960 for each (NULL row is excluded by the equi-join).

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.t0 (
  `c0` int(11) NULL COMMENT "",
  `c1` varchar(20) NULL COMMENT "",
  `c2` varchar(200) NULL COMMENT "",
  `c3` int(11) NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`c0`, `c1`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`c0`, `c1`) BUCKETS 1
PROPERTIES (
"replication_num" = "1",
"in_memory" = "false",
"storage_format" = "DEFAULT",
"enable_persistent_index" = "true",
"replicated_storage" = "true",
"compression" = "LZ4"
);

-- query 2
-- @skip_result_check=true
CREATE TABLE ${case_db}.t1 (
  `c0` int(11) NULL COMMENT "",
  `c1` varchar(20) NULL COMMENT "",
  `c2` varchar(200) NULL COMMENT "",
  `c3` int(11) NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`c0`, `c1`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`c0`, `c1`) BUCKETS 48
PROPERTIES (
"replication_num" = "1",
"in_memory" = "false",
"storage_format" = "DEFAULT",
"enable_persistent_index" = "true",
"replicated_storage" = "true",
"compression" = "LZ4"
);

-- query 3
-- @skip_result_check=true
INSERT INTO ${case_db}.t0 SELECT generate_series, generate_series, generate_series, generate_series FROM TABLE(generate_series(1, 40960));

-- query 4
-- @skip_result_check=true
INSERT INTO ${case_db}.t0 VALUES (null, null, null, null);

-- query 5
-- @skip_result_check=true
INSERT INTO ${case_db}.t1 SELECT * FROM ${case_db}.t0;

-- query 6
SELECT count(*) FROM ${case_db}.t0 JOIN ${case_db}.t1 ON ${case_db}.t0.c0=${case_db}.t1.c0 WHERE ${case_db}.t0.c1 LIKE ${case_db}.t1.c1;

-- query 7
SELECT count(*) FROM ${case_db}.t0 JOIN ${case_db}.t1 ON ${case_db}.t0.c0=${case_db}.t1.c0 WHERE ${case_db}.t0.c1 LIKE concat(${case_db}.t1.c1, '%');

-- query 8
SELECT count(*) FROM ${case_db}.t0 JOIN ${case_db}.t1 ON ${case_db}.t0.c0=${case_db}.t1.c0 WHERE ${case_db}.t0.c1 LIKE concat('%', ${case_db}.t1.c1, '%');

-- query 9
SELECT count(*) FROM ${case_db}.t0 JOIN ${case_db}.t1 ON ${case_db}.t0.c0=${case_db}.t1.c0 WHERE ${case_db}.t0.c1 LIKE concat('%', ${case_db}.t1.c1);
