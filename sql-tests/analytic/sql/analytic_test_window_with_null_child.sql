-- Migrated from: dev/test/sql/test_window_function/T/test_window_with_null_child
-- Test Objective:
-- 1. Validate row_number() window function when child input has NULL values from a LEFT JOIN.
-- 2. Test that the window function correctly handles sparse join results.

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.`test_window_with_null_child` (
  `c0` string NOT NULL,
  `c1` bigint NOT NULL,
  `c2` bigint NOT NULL
) ENGINE=OLAP
DUPLICATE KEY(`c0`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`c0`) BUCKETS 16
PROPERTIES ("replication_num" = "1");

CREATE TABLE ${case_db}.`tsmall` (
  `c0` string NOT NULL,
  `c1` bigint NOT NULL,
  `c2` bigint NOT NULL
) ENGINE=OLAP
DUPLICATE KEY(`c0`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`c0`) BUCKETS 16
PROPERTIES ("replication_num" = "1");

INSERT INTO ${case_db}.test_window_with_null_child SELECT generate_series, 4096 - generate_series, generate_series FROM TABLE(generate_series(1, 40960));
INSERT INTO ${case_db}.tsmall SELECT generate_series, 4096 - generate_series, generate_series FROM TABLE(generate_series(1, 1));

-- query 2
SELECT sum(rn) FROM ( SELECT rn FROM ( SELECT row_number() OVER (PARTITION BY c0 ORDER BY c1, c2) AS rn FROM ( SELECT c0, c1, c2 FROM ( SELECT l.c0 AS c0, l.c1 AS c1, r.c1 AS c2 FROM ${case_db}.test_window_with_null_child l LEFT JOIN ${case_db}.tsmall r ON l.c1 = r.c1 ) tx GROUP BY c0, c1, c2 ) ty ) t WHERE rn <= 2 ) t1;
