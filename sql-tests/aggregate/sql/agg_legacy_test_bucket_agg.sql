-- Migrated from dev/test/sql/test_agg/R/test_bucket_agg
-- Test Objective:
-- Preserve legacy aggregate coverage in a self-contained sql-tests case.
-- query 1
-- @skip_result_check=true
DROP DATABASE IF EXISTS sql_tests_test_bucket_agg FORCE;
CREATE DATABASE sql_tests_test_bucket_agg;
USE sql_tests_test_bucket_agg;

-- name: test_bucket_agg @mac
-- query 2
-- @skip_result_check=true
USE sql_tests_test_bucket_agg;
set pipeline_dop=1;

-- query 3
-- @skip_result_check=true
USE sql_tests_test_bucket_agg;
CREATE TABLE `t0` (
  `c0` bigint DEFAULT NULL,
  `c1` bigint DEFAULT NULL,
  `c2` bigint DEFAULT NULL
) ENGINE=OLAP
DUPLICATE KEY(`c0`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`c0`) BUCKETS 2
PROPERTIES (
"replication_num" = "1"
);

-- query 4
-- @skip_result_check=true
USE sql_tests_test_bucket_agg;
insert into t0 SELECT generate_series, 4096 - generate_series, generate_series FROM TABLE(generate_series(1,  4096));

-- query 5
-- @skip_result_check=true
USE sql_tests_test_bucket_agg;
insert into t0 select * from t0;

-- query 6
-- @skip_result_check=true
USE sql_tests_test_bucket_agg;
set tablet_internal_parallel_mode="force_split";

-- query 7
USE sql_tests_test_bucket_agg;
select distinct c0 from t0 order by 1 limit 5;

-- query 8
USE sql_tests_test_bucket_agg;
select distinct c0, c1 from t0 order by 1, 2 limit 5;

-- query 9
USE sql_tests_test_bucket_agg;
select distinct c0, c1, c2 from t0 order by 1, 2, 3 limit 5;

-- query 10
USE sql_tests_test_bucket_agg;
select sum(c1) from t0 group by c0, c2 order by 1 limit 5;

-- query 11
USE sql_tests_test_bucket_agg;
select sum(c2) from t0 group by c0, c1 order by 1 limit 5;

-- query 12
USE sql_tests_test_bucket_agg;
select sum(c1), max(c2) from t0 group by c0 order by 1, 2 limit 5;

-- query 13
USE sql_tests_test_bucket_agg;
select count(*) from (select distinct c0 from t0) tb;

-- query 14
USE sql_tests_test_bucket_agg;
select count(*) from (select c0, c1, sum(c2) from t0 group by c0, c1) tb;

-- query 15
USE sql_tests_test_bucket_agg;
select count(*) from (select distinct c0 from t0 limit 100) tb;

-- query 16
-- @skip_result_check=true
USE sql_tests_test_bucket_agg;
set tablet_internal_parallel_mode="auto";

-- query 17
USE sql_tests_test_bucket_agg;
select distinct c0 from t0 order by 1 limit 5;

-- query 18
USE sql_tests_test_bucket_agg;
select distinct c0, c1 from t0 order by 1, 2 limit 5;

-- query 19
USE sql_tests_test_bucket_agg;
select distinct c0, c1, c2 from t0 order by 1, 2, 3 limit 5;

-- query 20
USE sql_tests_test_bucket_agg;
select sum(c1) from t0 group by c0, c2 order by 1 limit 5;

-- query 21
USE sql_tests_test_bucket_agg;
select sum(c2) from t0 group by c0, c1 order by 1 limit 5;

-- query 22
USE sql_tests_test_bucket_agg;
select sum(c1), max(c2) from t0 group by c0 order by 1, 2 limit 5;

-- query 23
USE sql_tests_test_bucket_agg;
select count(*) from (select distinct c0 from t0) tb;

-- query 24
USE sql_tests_test_bucket_agg;
select count(*) from (select c0, c1, sum(c2) from t0 group by c0, c1) tb;

-- query 25
USE sql_tests_test_bucket_agg;
select count(*) from (select distinct c0 from t0 limit 100) tb;

-- query 26
-- @skip_result_check=true
USE sql_tests_test_bucket_agg;
CREATE TABLE `t1` (
  `c0` string DEFAULT NULL,
  `c1` bigint DEFAULT NULL,
  `c2` bigint DEFAULT NULL
) ENGINE=OLAP
DUPLICATE KEY(`c0`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`c0`) BUCKETS 96
PROPERTIES (
"replication_num" = "1"
);

-- query 27
-- @skip_result_check=true
USE sql_tests_test_bucket_agg;
insert into t1 SELECT generate_series, 4096 - generate_series, generate_series FROM TABLE(generate_series(1,  4096));

-- query 28
-- @skip_result_check=true
USE sql_tests_test_bucket_agg;
insert into t1 select * from t1;

-- query 29
USE sql_tests_test_bucket_agg;
select distinct c0 from t1 order by 1 limit 3;

-- query 30
USE sql_tests_test_bucket_agg;
select sum(c1), max(c1), min(c1), avg(c1) from t1 group by c0, c2 order by 1 limit 5;

-- query 31
USE sql_tests_test_bucket_agg;
select count(*) from (select distinct c0 from t1 limit 100) tb;

-- query 32
-- @skip_result_check=true
USE sql_tests_test_bucket_agg;
CREATE TABLE `t2` (
  `c0` string DEFAULT NULL,
  `c1` bigint DEFAULT NULL,
  `c2` bigint DEFAULT NULL
) ENGINE=OLAP
DUPLICATE KEY(`c0`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`c0`) BUCKETS 96
PROPERTIES (
"replication_num" = "1"
);

-- query 33
-- @skip_result_check=true
USE sql_tests_test_bucket_agg;
insert into t2 select * from t1 where crc32(c0) % 96 = 95;

-- query 34
USE sql_tests_test_bucket_agg;
select distinct c0 from t2 order by 1 limit 3;

-- query 35
USE sql_tests_test_bucket_agg;
select sum(c1), max(c1), min(c1), avg(c1) from t2 group by c0, c2 order by 1 limit 5;

-- query 36
USE sql_tests_test_bucket_agg;
select count(*) from (select distinct c0 from t2 limit 100) tb;
