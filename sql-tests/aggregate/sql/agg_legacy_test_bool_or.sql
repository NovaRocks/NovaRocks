-- Migrated from dev/test/sql/test_agg_function/R/test_bool_or
-- Test Objective:
-- Preserve legacy aggregate coverage in a self-contained sql-tests case.
-- query 1
-- @skip_result_check=true
DROP DATABASE IF EXISTS sql_tests_test_bool_or FORCE;
CREATE DATABASE sql_tests_test_bool_or;
USE sql_tests_test_bool_or;

-- name: test_bool_or
-- query 2
-- @skip_result_check=true
USE sql_tests_test_bool_or;
CREATE TABLE `t1` (
  `c0` bigint NOT NULL,
  `c1` bigint DEFAULT NULL,
  `c2` bigint DEFAULT NULL,
  `c3` bigint DEFAULT NULL
) ENGINE=OLAP
DUPLICATE KEY(`c0`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`c0`) BUCKETS 4
PROPERTIES (
"replication_num" = "1"
);

-- query 3
USE sql_tests_test_bool_or;
select bool_or(c0), boolor_agg(c1), bool_or(c2), bool_or(c3), bool_or(null) from t1;

-- query 4
-- @skip_result_check=true
USE sql_tests_test_bool_or;
insert into t1 SELECT generate_series, generate_series, generate_series, null FROM TABLE(generate_series(1,  40960));

-- query 5
USE sql_tests_test_bool_or;
select bool_or(c0), boolor_agg(c1), bool_or(c2), bool_or(c3), bool_or(null) from t1;

-- query 6
-- @skip_result_check=true
USE sql_tests_test_bool_or;
set streaming_preaggregation_mode="force_streaming";

-- query 7
USE sql_tests_test_bool_or;
select sum (a), sum(b), sum(c),sum(d), sum(e) from (select bool_or(c0) a, boolor_agg(c1) b, bool_or(c2) c, bool_or(c3) d, bool_or(null) e from t1 group by c0) t;

-- query 8
USE sql_tests_test_bool_or;
select sum(a) from ( select bool_or(c0) over (partition by c2 order by c3) a from t1) t;
