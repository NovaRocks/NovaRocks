-- Migrated from dev/test/sql/test_array_fn/R/test_array_filter
-- Test Objective:
-- Preserve array test coverage migrated from dev/test.
-- query 1
-- @skip_result_check=true
DROP DATABASE IF EXISTS sql_tests_complex_test_array_filter FORCE;
CREATE DATABASE sql_tests_complex_test_array_filter;
USE sql_tests_complex_test_array_filter;

-- name: test_array_filter @mac
-- query 2
-- @skip_result_check=true
USE sql_tests_complex_test_array_filter;
CREATE TABLE `t` (
  `k` bigint(20) NOT NULL COMMENT "",
  `arr_0` array<bigint(20)> NOT NULL COMMENT "",
  `arr_1` array<bigint(20)> NULL COMMENT "",
  `arr_2` array<bigint(20)> NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`k`)
DISTRIBUTED BY RANDOM BUCKETS 1
PROPERTIES (
"replication_num" = "1"
);

-- query 3
-- @skip_result_check=true
USE sql_tests_complex_test_array_filter;
insert into t values (1,[1,2],[1,2],[0,1]),(2,[1,2],null,[1,1]),(3,[1,2],[1,2],null),(4,[1,2],[null,null],[null,null]),(5,[1],[1,2],[0,0,1]);

-- query 4
USE sql_tests_complex_test_array_filter;
select array_filter(arr_0, arr_2) from t order by k;

-- query 5
USE sql_tests_complex_test_array_filter;
select array_filter(arr_1, arr_2) from t order by k;

-- query 6
USE sql_tests_complex_test_array_filter;
select array_filter(arr_0,[0,0,0,1]) from t order by k;

-- query 7
USE sql_tests_complex_test_array_filter;
select array_filter(arr_0,[1,0,1]) from t order by k;

-- query 8
USE sql_tests_complex_test_array_filter;
select array_filter(arr_0,null) from t order by k;

-- query 9
USE sql_tests_complex_test_array_filter;
select array_filter(arr_1,null) from t order by k;

-- query 10
USE sql_tests_complex_test_array_filter;
select array_filter([1,2,3,4],arr_2) from t order by k;

-- query 11
USE sql_tests_complex_test_array_filter;
select array_filter(null, arr_2) from t order by k;

-- query 12
USE sql_tests_complex_test_array_filter;
select array_filter([1,2,3,4],[0,0,1,1]) from t;

-- query 13
USE sql_tests_complex_test_array_filter;
select array_filter(null, null) from t;

-- query 14
USE sql_tests_complex_test_array_filter;
select array_filter([1,2,3,4],null) from t;

-- query 15
USE sql_tests_complex_test_array_filter;
select array_filter(null, [1,0,1,null]) from t;

-- query 16
-- @skip_result_check=true
DROP DATABASE IF EXISTS sql_tests_complex_test_array_filter FORCE;
