-- @tags=join,exception,failpoint
-- Test Objective:
-- 1. Validate hash join returns correct results under normal execution.
-- 2. Validate that hash join build/append memory allocation failures produce proper error messages.
-- Test Flow:
-- 1. Create two colocated tables and insert 40960 rows plus one all-NULL row.
-- 2. Run a colocate join and assert correct aggregate results.
-- 3. Enable failpoints for hash_join_append_bad_alloc and hash_join_build_bad_alloc,
--    verifying that the expected memory error is raised.

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
DISTRIBUTED BY HASH(`c0`, `c1`) BUCKETS 48
PROPERTIES (
"replication_num" = "1"
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
"replication_num" = "1"
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
SELECT count(l.c0), avg(l.c0), count(l.c1), count(l.c0), count(r.c1) FROM ${case_db}.t0 l JOIN [colocate] ${case_db}.t1 r ON l.c0 = r.c0 AND l.c1 = r.c1;

-- Failpoint queries (hash_join_append_bad_alloc, hash_join_build_bad_alloc) removed:
-- NovaRocks does not support C++ failpoint injection (ADMIN ENABLE/DISABLE FAILPOINT).
