-- Migrated from dev/test/sql/test_agg_function/R/test_percentile_approx
-- Test Objective:
-- Preserve legacy aggregate coverage in a self-contained sql-tests case.
-- query 1
-- @skip_result_check=true
DROP DATABASE IF EXISTS sql_tests_test_percentile_approx FORCE;
CREATE DATABASE sql_tests_test_percentile_approx;
USE sql_tests_test_percentile_approx;

-- name: test_percentile_approx
-- query 2
-- @skip_result_check=true
USE sql_tests_test_percentile_approx;
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
USE sql_tests_test_percentile_approx;
insert into t1 select generate_series, generate_series from table(generate_series(1, 50000, 3));

-- query 4
-- @skip_result_check=true
USE sql_tests_test_percentile_approx;
set pipeline_dop=1;

-- query 5
USE sql_tests_test_percentile_approx;
select cast(percentile_approx(c2, 0.5) as int) from t1;

-- query 6
USE sql_tests_test_percentile_approx;
select cast(percentile_approx(c2, 0.9) as int) from t1;

-- query 7
USE sql_tests_test_percentile_approx;
select cast(percentile_approx(c2, 0.9, 2048) as int) from t1;

-- query 8
USE sql_tests_test_percentile_approx;
select cast(percentile_approx(c2, 0.9, 5000) as int) from t1;

-- query 9
USE sql_tests_test_percentile_approx;
select cast(percentile_approx(c2, 0.9, 10000) as int) from t1;

-- query 10
USE sql_tests_test_percentile_approx;
with tt as (select @v1 as v1, c1, c2 from t1) select /*+ set_user_variable(@v1 = 0.5) */ cast(percentile_approx(c2, v1) as int) from tt;

-- query 11
USE sql_tests_test_percentile_approx;
with tt as (select @v1 as v1, @v2 as v2, c1, c2 from t1) select /*+ set_user_variable(@v1= 0.5, @v2 = 4096) */ cast(percentile_approx(c2, v1, v2 + 1) as int) from tt;

-- query 12
USE sql_tests_test_percentile_approx;
select /*+ SET_VAR (new_planner_agg_stage = '2') */ percentile_approx(c2, array<double>[0.25, 0.5, 0.75]) from t1;

-- query 13
USE sql_tests_test_percentile_approx;
select /*+ SET_VAR (new_planner_agg_stage = '2') */ percentile_approx(c2, array<double>[0.0, 0.5, 1.0]) from t1;

-- query 14
USE sql_tests_test_percentile_approx;
select /*+ SET_VAR (new_planner_agg_stage = '2') */ percentile_approx(c2, [0.1, 0.25, 0.5, 0.75, 0.9]) from t1;

-- query 15
USE sql_tests_test_percentile_approx;
select /*+ SET_VAR (new_planner_agg_stage = '2') */ percentile_approx(c2, [0.25, 0.5, 0.75], 2048) from t1;

-- query 16
USE sql_tests_test_percentile_approx;
select percentile_approx(c2, array<double>[0.5, 0.9], 5000) from t1;

-- query 17
USE sql_tests_test_percentile_approx;
select percentile_approx(c2, [0.1, 0.5, 0.9], 10000) from t1;

-- query 18
-- @skip_result_check=true
USE sql_tests_test_percentile_approx;
CREATE TABLE `test_sorted_streaming_agg_percentile`
(
    `id_int` int(11) NOT NULL COMMENT "",
    `value` double NOT NULL COMMENT ""
)
ENGINE=OLAP
DUPLICATE KEY(`id_int`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`id_int`)
BUCKETS 1
PROPERTIES (
"replication_num" = "1"
);

-- query 19
-- @skip_result_check=true
USE sql_tests_test_percentile_approx;
insert into test_sorted_streaming_agg_percentile values(2,1),(2,6),(4,3),(4,8);

-- query 20
USE sql_tests_test_percentile_approx;
select /*+ SET_VAR (enable_sort_aggregate = 'true') */ percentile_approx(value, array<double>[0.5, 0.9], 5000) from test_sorted_streaming_agg_percentile group by id_int;

-- query 21
-- @skip_result_check=true
USE sql_tests_test_percentile_approx;
CREATE TABLE `t0_convert_to_serialize_format` (
  `v1` bigint(20) NULL COMMENT "",
  `v2` bigint(20) NULL COMMENT "",
  `v3` bigint(20) NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`v1`, `v2`, `v3`)
DISTRIBUTED BY HASH(`v1`) BUCKETS 3
PROPERTIES (
"compression" = "LZ4",
"fast_schema_evolution" = "true",
"replicated_storage" = "true",
"replication_num" = "1"
);

-- query 22
-- @skip_result_check=true
USE sql_tests_test_percentile_approx;
CREATE TABLE `t1_convert_to_serialize_format` (
  `v4` bigint(20) NULL COMMENT "",
  `v5` bigint(20) NULL COMMENT "",
  `v6` bigint(20) NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`v4`, `v5`, `v6`)
DISTRIBUTED BY HASH(`v4`) BUCKETS 3
PROPERTIES (
"compression" = "LZ4",
"fast_schema_evolution" = "true",
"replicated_storage" = "true",
"replication_num" = "1"
);

-- query 23
-- @skip_result_check=true
USE sql_tests_test_percentile_approx;
insert into t0_convert_to_serialize_format values(1,2,3),(4,5,6),(7,8,9);

-- query 24
-- @skip_result_check=true
USE sql_tests_test_percentile_approx;
insert into t1_convert_to_serialize_format values(1,2,3),(4,5,6),(7,8,9);

-- query 25
USE sql_tests_test_percentile_approx;
select /*+ SET_VAR (streaming_preaggregation_mode = 'force_streaming') */ percentile_approx(v2, array<double>[0.1, 0.5, 0.9], 5000) from t0_convert_to_serialize_format join t1_convert_to_serialize_format group by 'a';
