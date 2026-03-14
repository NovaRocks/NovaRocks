-- Migrated from dev/test/sql/test_agg_function/R/test_hll
-- Test Objective:
-- Preserve legacy aggregate coverage in a self-contained sql-tests case.
-- query 1
-- @skip_result_check=true
DROP DATABASE IF EXISTS sql_tests_test_hll FORCE;
CREATE DATABASE sql_tests_test_hll;
USE sql_tests_test_hll;

-- name: test_hll_function
-- query 2
-- @skip_result_check=true
USE sql_tests_test_hll;
CREATE TABLE t1 (
    c1 int,
    c2 int
    )
DUPLICATE KEY(c1)
DISTRIBUTED BY HASH(c1)
BUCKETS 1
PROPERTIES ("replication_num" = "1");

-- query 3
-- @skip_result_check=true
USE sql_tests_test_hll;
insert into t1 select generate_series, generate_series from table(generate_series(1, 1000));

-- query 4
-- @skip_result_check=true
USE sql_tests_test_hll;
set pipeline_dop=1;

-- query 5
USE sql_tests_test_hll;
select ndv(c1) from t1;

-- query 6
USE sql_tests_test_hll;
select c2, ndv(c1) from t1 group by c2 order by c2 limit 10;

-- query 7
USE sql_tests_test_hll;
select approx_count_distinct(c1) from t1;

-- query 8
USE sql_tests_test_hll;
select c2,approx_count_distinct(c1) from t1 group by c2 order by c2 limit 10;

-- query 9
USE sql_tests_test_hll;
select approx_count_distinct_hll_sketch(c1) from t1;

-- query 10
USE sql_tests_test_hll;
select c2,approx_count_distinct_hll_sketch(c1) from t1 group by c2 order by c2 limit 10;

-- query 11
USE sql_tests_test_hll;
select hll_union(hll_hash(c1)) from t1;

-- query 12
USE sql_tests_test_hll;
select c2, hll_union(hll_hash(c1)) from t1 group by c2 order by c2 limit 10;

-- query 13
USE sql_tests_test_hll;
select hll_raw_agg(hll_hash(c1)) from t1;

-- query 14
USE sql_tests_test_hll;
select c2, hll_raw_agg(hll_hash(c1)) from t1 group by c2 order by c2 limit 10;

-- query 15
USE sql_tests_test_hll;
select hll_union_agg(hll_hash(c1)) from t1;

-- query 16
USE sql_tests_test_hll;
select c2, hll_union_agg(hll_hash(c1)) from t1 group by c2 order by c2 limit 10;
