-- Migrated from dev/test/sql/test_agg/R/test_orderby_agg
-- Test Objective:
-- Preserve legacy aggregate coverage in a self-contained sql-tests case.
-- query 1
-- @skip_result_check=true
DROP DATABASE IF EXISTS sql_tests_test_orderby_agg FORCE;
CREATE DATABASE sql_tests_test_orderby_agg;
USE sql_tests_test_orderby_agg;

-- name: test_orderby_agg @mac
-- query 2
-- @skip_result_check=true
USE sql_tests_test_orderby_agg;
CREATE TABLE `t0` (
  `v1` bigint(20) NULL COMMENT "",
  `v2` bigint(20) NULL COMMENT "",
  `v3` bigint(20) NULL COMMENT "",
  `v4` varchar NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`v1`, `v2`, `v3`)
DISTRIBUTED BY HASH(`v1`) BUCKETS 3
PROPERTIES (
"replication_num" = "1",
"enable_persistent_index" = "true",
"replicated_storage" = "true",
"compression" = "LZ4"
);

-- query 3
-- @skip_result_check=true
USE sql_tests_test_orderby_agg;
insert into t0 values(1, 2, 3, 'a'), (1, 3, 4, 'b'), (2, 3, 4, 'a'), (null, 1, null, 'c'), (4, null, 1 , null),
(5, 1 , 3, 'c'), (2, 2, null, 'a'), (4, null, 4, 'c'), (null, null, 2, null);

-- query 4
USE sql_tests_test_orderby_agg;
select min(v1) v1 from t0 group by v3 order by round(count(v2) / min(v1)), min(v1);

-- query 5
USE sql_tests_test_orderby_agg;
select min(v1) v1, round(count(v2) / min(v1)) round_col from t0 group by v3 order by abs(min(v1)) + abs(v1) asc;

-- query 6
USE sql_tests_test_orderby_agg;
select min(v1) v1 from t0 group by v3 order by round(count(v2) / min(v1)), abs(v1);

-- query 7
USE sql_tests_test_orderby_agg;
select min(v1) v11 from t0 group by v3 order by round(count(v2) / min(v1)), abs(v11);

-- query 8
USE sql_tests_test_orderby_agg;
select min(v1) v11, min(v1) v1 from t0 group by v3 order by round(count(v2) / min(v1)), abs(v11), abs(v1);

-- query 9
USE sql_tests_test_orderby_agg;
select round(count(v1) * 100.0 / min(v2), 4) as potential_customer_rate, min(v2) v2 from t0 group by v4 order by round(count(v1) * 100.0 / min(v2), 4), min(v2);

-- query 10
USE sql_tests_test_orderby_agg;
select round(count(v1) * 100.0 / min(v2), 4) as potential_customer_rate, min(v2) v2 from t0 group by v4 order by round(count(v1) * 100.0 / min(v2), 4), abs(v2);
