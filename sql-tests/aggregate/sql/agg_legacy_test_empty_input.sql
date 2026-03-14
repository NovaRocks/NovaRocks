-- Migrated from dev/test/sql/test_agg/R/test_empty_input
-- Test Objective:
-- Preserve legacy aggregate coverage in a self-contained sql-tests case.
-- query 1
-- @skip_result_check=true
DROP DATABASE IF EXISTS sql_tests_test_empty_input FORCE;
CREATE DATABASE sql_tests_test_empty_input;
USE sql_tests_test_empty_input;

-- name: test_empty_input @mac
-- query 2
-- @skip_result_check=true
USE sql_tests_test_empty_input;
CREATE TABLE `t0` (
  `v1` bigint(20) COMMENT "",
  `v2` bigint(20) COMMENT "",
  `v3` bigint(20) COMMENT "",
  `v4` varchar COMMENT ""
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
USE sql_tests_test_empty_input;
select percentile_disc(v1,0.5) from t0;

-- query 4
USE sql_tests_test_empty_input;
select percentile_cont(v1,0.5) from t0;

-- query 5
USE sql_tests_test_empty_input;
select percentile_disc_lc(v1,0.5) from t0;

-- query 6
USE sql_tests_test_empty_input;
select max(v1),min(v1) from t0;

-- query 7
USE sql_tests_test_empty_input;
select max(v1),count(*) from t0;

-- query 8
USE sql_tests_test_empty_input;
select count(v1) from t0;

-- query 9
USE sql_tests_test_empty_input;
select count(*) from t0;

-- query 10
USE sql_tests_test_empty_input;
select count(distinct v1) from t0;

-- name: test_empty_input_not_null_string
-- query 11
-- @skip_result_check=true
USE sql_tests_test_empty_input;
CREATE TABLE `tt0` (
  `c0` int(11) NULL COMMENT "",
  `c1` varchar(20) not NULL COMMENT "",
  `c2` varchar(200) not NULL COMMENT "",
  `c3` int(11) NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`c0`, `c1`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`c0`, `c1`) BUCKETS 4
PROPERTIES (
"compression" = "LZ4",
"fast_schema_evolution" = "true",
"replicated_storage" = "true",
"replication_num" = "1"
);

-- query 12
-- @skip_result_check=true
USE sql_tests_test_empty_input;
insert into tt0 values (1,"test","test",1);

-- query 13
USE sql_tests_test_empty_input;
select max(c2) from tt0 where 1=0;

-- query 14
USE sql_tests_test_empty_input;
select min(c2) from tt0 where 1=0;
