-- Migrated from dev/test/sql/test_agg/R/test_streaming_agg
-- Test Objective:
-- Preserve legacy aggregate coverage in a self-contained sql-tests case.
-- query 1
-- @skip_result_check=true
DROP DATABASE IF EXISTS sql_tests_test_streaming_agg FORCE;
CREATE DATABASE sql_tests_test_streaming_agg;
USE sql_tests_test_streaming_agg;
admin disable failpoint 'force_reset_aggregator_after_agg_streaming_sink_finish';

-- name: test_streaming_agg @sequential
-- query 2
-- @skip_result_check=true
USE sql_tests_test_streaming_agg;
create table t0(
    c0 INT,
    c1 BIGINT
) DUPLICATE KEY(c0) DISTRIBUTED BY HASH(c0) BUCKETS 3 PROPERTIES('replication_num' = '1');

-- query 3
-- @skip_result_check=true
USE sql_tests_test_streaming_agg;
insert into t0 values (1,1),(2,2),(3,3),(4,4),(5,5);

-- query 4
-- @skip_result_check=true
USE sql_tests_test_streaming_agg;
set pipeline_dop=1;

-- query 5
-- @skip_result_check=true
USE sql_tests_test_streaming_agg;
set new_planner_agg_stage=2;

-- query 6
USE sql_tests_test_streaming_agg;
select c0, sum(c1) from t0 group by c0 order by c0;

-- query 7
-- @skip_result_check=true
USE sql_tests_test_streaming_agg;
admin enable failpoint 'force_reset_aggregator_after_agg_streaming_sink_finish';

-- query 8
USE sql_tests_test_streaming_agg;
select c0, sum(c1) from t0 group by c0 order by c0;

-- query 9
-- @skip_result_check=true
USE sql_tests_test_streaming_agg;
admin disable failpoint 'force_reset_aggregator_after_agg_streaming_sink_finish';

-- query 10
-- @skip_result_check=true
USE sql_tests_test_streaming_agg;
create table t1 (
    c0 INT,
    c1 BIGINT
) DUPLICATE KEY(c0) DISTRIBUTED BY HASH(c0) BUCKETS 3 PROPERTIES('replication_num' = '1');

-- query 11
-- @skip_result_check=true
USE sql_tests_test_streaming_agg;
insert into t1 SELECT generate_series, 4096 - generate_series FROM TABLE(generate_series(1,  4096)) union all select null,null;

-- query 12
USE sql_tests_test_streaming_agg;
select c0, sum(c1) from t1 group by c0 order by 2 desc limit 10;

-- Legacy cleanup step.
-- query 13
-- @skip_result_check=true
USE sql_tests_test_streaming_agg;
admin disable failpoint 'force_reset_aggregator_after_agg_streaming_sink_finish';
