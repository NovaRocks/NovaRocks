-- Migrated from dev/test/sql/test_agg_function/R/test_percentile_union
-- Test Objective:
-- Preserve legacy aggregate coverage in a self-contained sql-tests case.
-- query 1
-- @skip_result_check=true
DROP DATABASE IF EXISTS sql_tests_test_percentile_union FORCE;
CREATE DATABASE sql_tests_test_percentile_union;
USE sql_tests_test_percentile_union;

-- name: test_percentile_union
-- query 2
-- @skip_result_check=true
USE sql_tests_test_percentile_union;
CREATE TABLE t1 (
    c1 int,
    c2 double
    )
DUPLICATE KEY(c1)
DISTRIBUTED BY HASH(c1)
BUCKETS 1
PROPERTIES ("replication_num" = "1");

-- query 3
-- @skip_result_check=true
USE sql_tests_test_percentile_union;
insert into t1 select generate_series, generate_series from table(generate_series(1, 1000));

-- query 4
-- @skip_result_check=true
USE sql_tests_test_percentile_union;
set pipeline_dop=1;

-- query 5
USE sql_tests_test_percentile_union;
select percentile_approx_raw(percentile_union(percentile_hash(c2)), 0.99) from t1;
