-- Migrated from dev/test/sql/test_agg_function/R/test_lc_percentile
-- Test Objective:
-- Preserve legacy aggregate coverage in a self-contained sql-tests case.
-- query 1
-- @skip_result_check=true
DROP DATABASE IF EXISTS sql_tests_test_lc_percentile FORCE;
CREATE DATABASE sql_tests_test_lc_percentile;
USE sql_tests_test_lc_percentile;

-- name: test_lc_percentile
-- query 2
-- @skip_result_check=true
USE sql_tests_test_lc_percentile;
CREATE TABLE `test_pc` (
  `date` date NULL COMMENT "",
  `datetime` datetime NULL COMMENT "",
  `db` double NULL COMMENT "",
  `id` int(11) NULL COMMENT "",
  `name` varchar(255) NULL COMMENT "",
  `subject` varchar(255) NULL COMMENT "",
  `score` int(11) NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`date`)
DISTRIBUTED BY HASH(`id`) BUCKETS 4
PROPERTIES (
"replication_num" = "1",
"enable_persistent_index" = "true",
"replicated_storage" = "true",
"compression" = "LZ4"
);

-- query 3
-- @skip_result_check=true
USE sql_tests_test_lc_percentile;
insert into test_pc values ("2018-01-01","2018-01-01 00:00:01",11.1,1,"Tom","English",90);

-- query 4
-- @skip_result_check=true
USE sql_tests_test_lc_percentile;
insert into test_pc values ("2019-01-01","2019-01-01 00:00:01",11.2,1,"Tom","English",91);

-- query 5
-- @skip_result_check=true
USE sql_tests_test_lc_percentile;
insert into test_pc values ("2020-01-01","2020-01-01 00:00:01",11.3,1,"Tom","English",92);

-- query 6
-- @skip_result_check=true
USE sql_tests_test_lc_percentile;
insert into test_pc values ("2021-01-01","2021-01-01 00:00:01",11.4,1,"Tom","English",93);

-- query 7
-- @skip_result_check=true
USE sql_tests_test_lc_percentile;
insert into test_pc values ("2022-01-01","2022-01-01 00:00:01",11.5,1,"Tom","English",94);

-- query 8
-- @skip_result_check=true
USE sql_tests_test_lc_percentile;
insert into test_pc values (NULL,NULL,NULL,NULL,"Tom","English",NULL);

-- query 9
USE sql_tests_test_lc_percentile;
select percentile_disc_lc(score, 0) from test_pc;

-- query 10
USE sql_tests_test_lc_percentile;
select percentile_disc_lc(score, 0.25) from test_pc;

-- query 11
USE sql_tests_test_lc_percentile;
select percentile_disc_lc(score, 0.5) from test_pc;

-- query 12
USE sql_tests_test_lc_percentile;
select percentile_disc_lc(score, 0.75) from test_pc;

-- query 13
USE sql_tests_test_lc_percentile;
select percentile_disc_lc(score, 1) from test_pc;

-- query 14
USE sql_tests_test_lc_percentile;
select percentile_disc_lc(date, 0) from test_pc;

-- query 15
USE sql_tests_test_lc_percentile;
select percentile_disc_lc(date, 0.25) from test_pc;

-- query 16
USE sql_tests_test_lc_percentile;
select percentile_disc_lc(date, 0.5) from test_pc;

-- query 17
USE sql_tests_test_lc_percentile;
select percentile_disc_lc(date, 0.75) from test_pc;

-- query 18
USE sql_tests_test_lc_percentile;
select percentile_disc_lc(date, 1) from test_pc;

-- query 19
USE sql_tests_test_lc_percentile;
select percentile_disc_lc(datetime, 0) from test_pc;

-- query 20
USE sql_tests_test_lc_percentile;
select percentile_disc_lc(datetime, 0.25) from test_pc;

-- query 21
USE sql_tests_test_lc_percentile;
select percentile_disc_lc(datetime, 0.5) from test_pc;

-- query 22
USE sql_tests_test_lc_percentile;
select percentile_disc_lc(datetime, 0.75) from test_pc;

-- query 23
USE sql_tests_test_lc_percentile;
select percentile_disc_lc(datetime, 1) from test_pc;

-- query 24
USE sql_tests_test_lc_percentile;
select percentile_disc_lc(db, 0) from test_pc;

-- query 25
USE sql_tests_test_lc_percentile;
select percentile_disc_lc(db, 0.25) from test_pc;

-- query 26
USE sql_tests_test_lc_percentile;
select percentile_disc_lc(db, 0.5) from test_pc;

-- query 27
USE sql_tests_test_lc_percentile;
select percentile_disc_lc(db, 0.75) from test_pc;

-- query 28
USE sql_tests_test_lc_percentile;
select percentile_disc_lc(db, 1) from test_pc;

-- query 29
-- @skip_result_check=true
USE sql_tests_test_lc_percentile;
set new_planner_agg_stage=2;

-- query 30
-- @skip_result_check=true
USE sql_tests_test_lc_percentile;
set streaming_preaggregation_mode="force_streaming";

-- query 31
USE sql_tests_test_lc_percentile;
select `date`, percentile_disc_lc(score, 0) from test_pc group by `date` order by 1,2;

-- query 32
-- @skip_result_check=true
USE sql_tests_test_lc_percentile;
set streaming_preaggregation_mode="force_preaggregation";

-- query 33
USE sql_tests_test_lc_percentile;
select `date`, percentile_disc_lc(score, 0) from test_pc group by `date` order by 1,2;

-- query 34
-- @expect_error=between 0 and 1
USE sql_tests_test_lc_percentile;
select percentile_disc_lc(score, 1.1) from test_pc;
