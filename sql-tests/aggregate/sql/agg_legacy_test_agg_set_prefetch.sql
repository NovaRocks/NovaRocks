-- Migrated from dev/test/sql/test_agg/R/test_agg_set_prefetch
-- Test Objective:
-- Preserve legacy aggregate coverage in a self-contained sql-tests case.
-- query 1
-- @skip_result_check=true
DROP DATABASE IF EXISTS sql_tests_test_agg_set_prefetch FORCE;
CREATE DATABASE sql_tests_test_agg_set_prefetch;
USE sql_tests_test_agg_set_prefetch;

-- name: test_agg_set_prefetch @mac
-- query 2
-- @skip_result_check=true
USE sql_tests_test_agg_set_prefetch;
create table t0 (
    c0 STRING,
    c1 STRING NOT NULL,
    c2 int,
    c3 int NOT NULL
) DUPLICATE KEY(c0) DISTRIBUTED BY HASH(c0) BUCKETS 3 PROPERTIES('replication_num' = '1');

-- query 3
-- @skip_result_check=true
USE sql_tests_test_agg_set_prefetch;
insert into t0 SELECT generate_series, generate_series, generate_series, generate_series FROM TABLE(generate_series(1,  30000));

-- query 4
-- @skip_result_check=true
USE sql_tests_test_agg_set_prefetch;
set pipeline_dop = 1;

-- query 5
USE sql_tests_test_agg_set_prefetch;
select count(distinct c0) from t0;

-- query 6
USE sql_tests_test_agg_set_prefetch;
select count(distinct c1) from t0;

-- query 7
USE sql_tests_test_agg_set_prefetch;
select count(distinct c2) from t0;

-- query 8
USE sql_tests_test_agg_set_prefetch;
select count(distinct c3) from t0;

-- query 9
USE sql_tests_test_agg_set_prefetch;
select count(distinct c0) from t0 group by c2 order by c2 limit 1;

-- query 10
USE sql_tests_test_agg_set_prefetch;
select count(distinct c2) from t0 group by c3 order by c3 limit 1;
