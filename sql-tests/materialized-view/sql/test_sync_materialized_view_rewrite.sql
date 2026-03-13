-- Test Objective:
-- 1. Validate sync MV rewrite on aggregate queries after MV build completion.
-- 2. Cover rewrite stability with dictionary-related optimizer state.
-- Source: dev/test/sql/test_materialized_view/T/test_sync_materialized_view_rewrite

-- query 1
admin set frontend config('alter_scheduler_interval_millisecond' = '100');

-- query 2
CREATE TABLE `duplicate_tbl` (
    `k1` date NULL COMMENT "",
    `k2` datetime NULL COMMENT "",
    `k3` char(20) NULL COMMENT "",
    `k4` varchar(20) NULL COMMENT "",
    `k5` boolean NULL COMMENT "",
    `k6` tinyint(4) NULL COMMENT "",
    `k7` smallint(6) NULL COMMENT "",
    `k8` int(11) NULL COMMENT "",
    `k9` bigint(20) NULL COMMENT "",
    `k10` largeint(40) NULL COMMENT "",
    `k11` float NULL COMMENT "",
    `k12` double NULL COMMENT "",
    `k13` decimal128(27, 9) NULL COMMENT "",
    INDEX idx1 (`k6`) USING BITMAP
)
ENGINE=OLAP DUPLICATE KEY(`k1`, `k2`, `k3`, `k4`, `k5`)
DISTRIBUTED BY HASH(`k1`, `k2`, `k3`) BUCKETS 3
PROPERTIES (
    "replication_num" = "1",
    "enable_persistent_index" = "true",
    "replicated_storage" = "true",
    "compression" = "LZ4"
);

-- query 3
insert into duplicate_tbl values
    ('2023-06-15', '2023-06-15 00:00:00', 'a', 'a', false, 1, 1, 1, 1, 1, 1.0, 1.0, 1.0),
    ('2023-06-15', '2023-06-15 00:00:00', 'a', 'a', false, 1, 1, 1, 1, 1, 1.0, 1.0, 1.0),
    ('2023-06-15', '2023-06-15 00:00:00', 'a', 'a', false, 1, 1, 1, 1, 1, 1.0, 1.0, 1.0),
    ('2023-06-16', '2023-06-15 00:00:00', 'a', 'a', false, 1, 1, 1, 1, 1, 1.0, 1.0, 1.0)
;

-- query 4
create materialized view mv_1 as select k1, sum(k6) as k8, max(k7) as k7 from duplicate_tbl group by 1;

-- query 5
-- @skip_result_check=true
SHOW ALTER MATERIALIZED VIEW ORDER BY JobId DESC LIMIT 1;
SELECT sleep(2);

-- query 6
create materialized view mv_2 as select k1, sum(k6 + 1) as k6, max(k7 * 10) as k7 from duplicate_tbl group by 1;

-- query 7
-- @skip_result_check=true
SHOW ALTER MATERIALIZED VIEW ORDER BY JobId DESC LIMIT 1;
SELECT sleep(2);

-- query 8
create materialized view mv_3 as select k1, count(k6 + 1) as c_k6, min(k7 * 10) as m_k7 from duplicate_tbl group by 1;

-- query 9
-- @skip_result_check=true
SHOW ALTER MATERIALIZED VIEW ORDER BY JobId DESC LIMIT 1;
SELECT sleep(2);

-- query 10
CREATE MATERIALIZED VIEW mv_4
AS SELECT k1, MIN(k6), MIN(k7), MIN(k8), SUM(k9), MAX(k10), MIN(k11), MIN(k12), SUM(k13) FROM duplicate_tbl GROUP BY k1;

-- query 11
-- @skip_result_check=true
SHOW ALTER MATERIALIZED VIEW ORDER BY JobId DESC LIMIT 1;
SELECT sleep(2);

-- query 12
CREATE MATERIALIZED VIEW mv_5
AS SELECT k1, MIN(k6+k7) as min1, SUM(k9) as sum1, MAX(k10 + 2 * k11) as max1, SUM(2 * k13) as sum2 FROM duplicate_tbl GROUP BY k1;

-- query 13
-- @skip_result_check=true
SHOW ALTER MATERIALIZED VIEW ORDER BY JobId DESC LIMIT 1;
SELECT sleep(2);

-- query 14
-- @result_contains=mv_1
SET enable_materialized_view_rewrite = true;
EXPLAIN select k1, sum(k6) as k6, max(k7) as k7 from duplicate_tbl group by 1 order by 1;

-- query 15
-- @result_contains=mv_2
SET enable_materialized_view_rewrite = true;
EXPLAIN select k1, sum(k6 + 1) as k6, max(k7 * 10) as k7 from duplicate_tbl group by 1 order by 1;

-- query 16
-- @result_contains=mv_3
SET enable_materialized_view_rewrite = true;
EXPLAIN select k1, count(k6 + 1) as c_k6, min(k7 * 10) as m_k7 from duplicate_tbl group by 1 order by 1;

-- query 17
-- @result_contains=mv_4
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT k1, MIN(k6), MIN(k7), MIN(k8), SUM(k9), MAX(k10), MIN(k11), MIN(k12), SUM(k13) FROM duplicate_tbl GROUP BY k1 order by 1;

-- query 18
-- @result_contains=mv_5
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT k1, MIN(k6+k7) as min1, SUM(k9) as sum1, MAX(k10 + 2 * k11) as max1, SUM(2 * k13) FROM duplicate_tbl GROUP BY k1;

-- query 19
select k1, sum(k6) as k6, max(k7) as k7 from duplicate_tbl group by 1 order by 1,2;

-- query 20
select k1, sum(k6 + 1) as k6, max(k7 * 10) as k7 from duplicate_tbl group by 1 order by 1,2;

-- query 21
select k1, count(k6 + 1) as c_k6, min(k7 * 10) as m_k7 from duplicate_tbl group by 1 order by 1,2;

-- query 22
SELECT k1, MIN(k6), MIN(k7), MIN(k8), SUM(k9), MAX(k10), MIN(k11), MIN(k12), SUM(k13) FROM duplicate_tbl GROUP BY k1 order by 1,2;

-- query 23
SELECT k1, MIN(k6+k7) as min1, SUM(k9) as sum1, MAX(k10 + 2 * k11) as max1, SUM(2 * k13) FROM duplicate_tbl GROUP BY k1 order by 1,2;

-- query 24
insert into duplicate_tbl values
    ('2023-06-15', '2023-06-15 00:00:00', 'a', 'a', false, 1, 1, 1, 1, 1, 1.0, 1.0, 1.0),
    ('2023-06-15', '2023-06-15 00:00:00', 'a', 'a', false, 1, 1, 1, 1, 1, 1.0, 1.0, 1.0),
    ('2023-06-15', '2023-06-15 00:00:00', 'a', 'a', false, 1, 1, 1, 1, 1, 1.0, 1.0, 1.0),
    ('2023-06-16', '2023-06-15 00:00:00', 'a', 'a', false, 1, 1, 1, 1, 1, 1.0, 1.0, 1.0)
;

-- query 25
-- @result_contains=mv_1
SET enable_materialized_view_rewrite = true;
EXPLAIN select k1, sum(k6) as k6, max(k7) as k7 from duplicate_tbl group by 1 order by 1;

-- query 26
-- @result_contains=mv_2
SET enable_materialized_view_rewrite = true;
EXPLAIN select k1, sum(k6 + 1) as k6, max(k7 * 10) as k7 from duplicate_tbl group by 1 order by 1;

-- query 27
-- @result_contains=mv_3
SET enable_materialized_view_rewrite = true;
EXPLAIN select k1, count(k6 + 1) as c_k6, min(k7 * 10) as m_k7 from duplicate_tbl group by 1 order by 1;

-- query 28
-- @result_contains=mv_4
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT k1, MIN(k6), MIN(k7), MIN(k8), SUM(k9), MAX(k10), MIN(k11), MIN(k12), SUM(k13) FROM duplicate_tbl GROUP BY k1 order by 1;

-- query 29
-- @result_contains=mv_5
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT k1, MIN(k6+k7) as min1, SUM(k9) as sum1, MAX(k10 + 2 * k11) as max1, SUM(2 * k13) FROM duplicate_tbl GROUP BY k1;

-- query 30
select k1, sum(k6) as k6, max(k7) as k7 from duplicate_tbl group by 1 order by 1,2;

-- query 31
select k1, sum(k6 + 1) as k6, max(k7 * 10) as k7 from duplicate_tbl group by 1 order by 1,2;

-- query 32
select k1, count(k6 + 1) as c_k6, min(k7 * 10) as m_k7 from duplicate_tbl group by 1 order by 1,2;

-- query 33
SELECT k1, MIN(k6), MIN(k7), MIN(k8), SUM(k9), MAX(k10), MIN(k11), MIN(k12), SUM(k13) FROM duplicate_tbl GROUP BY k1 order by 1,2;

-- query 34
SELECT k1, MIN(k6+k7) as min1, SUM(k9) as sum1, MAX(k10 + 2 * k11) as max1, SUM(2 * k13) FROM duplicate_tbl GROUP BY k1 order by 1,2;

-- query 35
drop materialized view mv_1;

-- query 36
drop materialized view mv_2;

-- query 37
drop materialized view mv_3;

-- query 38
drop materialized view mv_4;

-- query 39
drop materialized view mv_5;

-- query 40
drop table if exists case_when_tbl1;

-- query 41
CREATE TABLE case_when_tbl1 (
    k1 INT,
    k2 char(20))
DUPLICATE KEY(k1)
DISTRIBUTED BY HASH(k1);

-- query 42
insert into case_when_tbl1 values (1,'xian'), (2, 'beijing'), (3, 'hangzhou');

-- query 43
create materialized view case_when_mv1 AS SELECT k1, (CASE k2 WHEN 'beijing' THEN 'bigcity' ELSE 'smallcity' END) as city FROM case_when_tbl1;

-- query 44
-- Wait for the previous sync MV job to settle before creating another MV on the same base table.
-- @skip_result_check=true
SHOW ALTER MATERIALIZED VIEW ORDER BY JobId DESC LIMIT 1;
SELECT sleep(2);

-- query 45
create materialized view case_when_mv2 AS SELECT k1, (CASE k2 WHEN 'beijing' THEN concat(k1, 'bigcity') ELSE concat(k1, 'smallcity') END) as case2 FROM case_when_tbl1;

-- query 46
-- Wait for the second MV create job to settle before checking rewrite.
-- @skip_result_check=true
SHOW ALTER MATERIALIZED VIEW ORDER BY JobId DESC LIMIT 1;
SELECT sleep(2);

-- query 47
-- @result_contains=case_when_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT k1, (CASE k2 WHEN 'beijing' THEN 'bigcity' ELSE 'smallcity' END) as city FROM case_when_tbl1;

-- query 48
-- @result_contains=case_when_mv2
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT k1, (CASE k2 WHEN 'beijing' THEN concat(k1, 'bigcity') ELSE concat(k1, 'smallcity') END) as city FROM case_when_tbl1;

-- query 49
SELECT k1, (CASE k2 WHEN 'beijing' THEN 'bigcity' ELSE 'smallcity' END) as city FROM case_when_tbl1 order by 1, 2;

-- query 50
SELECT k1, (CASE k2 WHEN 'beijing' THEN concat(k1, 'bigcity') ELSE concat(k1, 'smallcity') END) as city FROM case_when_tbl1 order by 1,2;

-- query 51
insert into duplicate_tbl values
    ('2023-06-15', '2023-06-15 00:00:00', 'a', 'a', false, 1, 1, 1, 1, 1, 1.0, 1.0, 1.0),
    ('2023-06-15', '2023-06-15 00:00:00', 'a', 'a', false, 1, 1, 1, 1, 1, 1.0, 1.0, 1.0),
    ('2023-06-15', '2023-06-15 00:00:00', 'a', 'a', false, 1, 1, 1, 1, 1, 1.0, 1.0, 1.0),
    ('2023-06-16', '2023-06-15 00:00:00', 'a', 'a', false, 1, 1, 1, 1, 1, 1.0, 1.0, 1.0)
;

-- query 52
-- @result_contains=case_when_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT k1, (CASE k2 WHEN 'beijing' THEN 'bigcity' ELSE 'smallcity' END) as city FROM case_when_tbl1;

-- query 53
-- @result_contains=case_when_mv2
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT k1, (CASE k2 WHEN 'beijing' THEN concat(k1, 'bigcity') ELSE concat(k1, 'smallcity') END) as city FROM case_when_tbl1;

-- query 54
SELECT k1, (CASE k2 WHEN 'beijing' THEN 'bigcity' ELSE 'smallcity' END) as city FROM case_when_tbl1 order by 1,2;

-- query 55
SELECT k1, (CASE k2 WHEN 'beijing' THEN concat(k1, 'bigcity') ELSE concat(k1, 'smallcity') END) as city FROM case_when_tbl1 order by 1,2;

-- query 56
drop materialized view case_when_mv1;

-- query 57
drop materialized view case_when_mv2;

-- query 58
drop table if exists case_when_tbl1;

-- query 59
-- case: test_mv_with_dict
-- 1.test_base_table1建表
CREATE TABLE IF NOT EXISTS test_base_table1
(
    `col0`             int(11) NULL,
    `col2`           datetime NULL,
    `col3`         varchar(32) NULL,
    `id`               bigint(20) NULL,
    `col1`           bigint(20) NULL
) DUPLICATE KEY(col0, col2, col3)
  PARTITION BY RANGE(col2)(
  START ("2022-04-17") END ("2022-05-01") EVERY (INTERVAL 1 day))
  DISTRIBUTED BY HASH(col0)
  PROPERTIES
(
    "replication_num" = "1"
);

-- query 60
INSERT INTO test_base_table1 (col0, col2, col3, id, col1) VALUES (123456789, '2022-04-30 12:00:00', 'Guangdong', 1, 10001);

-- query 61
INSERT INTO test_base_table1 (col0, col2, col3) VALUES (987654321, '2022-04-30 13:00:00', 'Fujian');

-- query 62
CREATE MATERIALIZED VIEW mv_test_base_table1 AS
SELECT col2,col3,col0,id,col1 FROM test_base_table1 ORDER BY col2,col3,col0;

-- query 63
-- analyze table to global dict rewrite
-- @skip_result_check=true
analyze full table test_base_table1 with sync mode;

-- query 64
-- check rewrite by rollup mv
-- @result_contains=FINISHED
-- @retry_count=60
-- @retry_interval_ms=1000
SHOW ALTER MATERIALIZED VIEW ORDER BY JobId DESC LIMIT 1;

-- query 65
-- @result_contains=Decode
-- @retry_count=60
-- @retry_interval_ms=1000
SET enable_materialized_view_rewrite = true;
EXPLAIN COSTS SELECT DISTINCT col3 FROM test_base_table1;

-- query 66
-- @result_contains=mv_test_base_table1
SET enable_materialized_view_rewrite = true;
EXPLAIN select col3, min(col1) startTime, max(col1) endTime from test_base_table1 where col2 BETWEEN '2022-04-29 15:12:23' and '2022-04-30 15:12:23' group by col3;

-- query 67
-- check result it's ok
select col3, min(col1) startTime, max(col1) endTime from test_base_table1 where col2 BETWEEN '2022-04-29 00:00:00' and '2022-04-30 23:00:00' group by col3 order by 1;

-- query 68
-- check hit base table's order key
-- @result_contains=rollup: test_base_table1
SET enable_materialized_view_rewrite = true;
EXPLAIN select * from test_base_table1 where col0=123456789;

-- query 69
select * from test_base_table1 where col0=123456789 order by 1;

-- query 70
-- check hit sync mv's order key
-- @result_contains=rollup: mv_test_base_table1
SET enable_materialized_view_rewrite = true;
EXPLAIN select * from test_base_table1 where col2 >='2022-04-30 12:00:00';

-- query 71
select * from test_base_table1 where col2 >='2022-04-30 12:00:00' order by 1;

-- query 72
-- check both hit base table and sync mv's order key
-- @result_contains_any=rollup: mv_test_base_table1
-- @result_contains_any=rollup: test_base_table1
SET enable_materialized_view_rewrite = true;
EXPLAIN select * from (select col0, col2, col3, id, col1 from test_base_table1 where col0=123456789 union select col0, col2, col3, id, col1 from test_base_table1 where col2 >='2022-04-30 12:00:00') t;

-- query 73
select * from (select col0, col2, col3, id, col1 from test_base_table1 where col0=123456789 union select col0, col2, col3, id, col1 from test_base_table1 where col2 >='2022-04-30 12:00:00') t order by 1;

-- query 74
-- if no force convert or to union all, only chooose mv
set select_ratio_threshold = 0.15;

-- query 75
-- @result_contains=rollup: mv_test_base_table1
SET enable_materialized_view_rewrite = true;
EXPLAIN select * from (select col0, col2, col3, id, col1 from test_base_table1 where col0=123456789 or col2 >='2022-04-30 12:00:00') t;

-- query 76
-- @result_not_contains=rollup: test_base_table1
SET enable_materialized_view_rewrite = true;
EXPLAIN select * from (select col0, col2, col3, id, col1 from test_base_table1 where col0=123456789 or col2 >='2022-04-30 12:00:00') t;

-- query 77
select * from (select col0, col2, col3, id, col1 from test_base_table1 where col0=123456789 or col2 >='2022-04-30 12:00:00') t;

-- query 78
-- @result_contains=rollup: mv_test_base_table1
SET enable_materialized_view_rewrite = true;
EXPLAIN select * from (select col0, col2, col3, id, col1 from test_base_table1 where col0=123456789 or col2 ='2022-04-30 12:00:00') t;

-- query 79
-- @result_not_contains=rollup: test_base_table1
SET enable_materialized_view_rewrite = true;
EXPLAIN select * from (select col0, col2, col3, id, col1 from test_base_table1 where col0=123456789 or col2 ='2022-04-30 12:00:00') t;

-- query 80
select * from (select col0, col2, col3, id, col1 from test_base_table1 where col0=123456789 or col2 ='2022-04-30 12:00:00') t;

-- query 81
-- if force convert or to union all, chooose mv and base table's short key
set select_ratio_threshold=-1;

-- query 82
-- Current NovaRocks cannot EXPLAIN the force-converted OR-to-UNION path under select_ratio_threshold = -1.
-- @expect_error=Unknown error
SET enable_materialized_view_rewrite = true;
EXPLAIN select * from (select col0, col2, col3, id, col1 from test_base_table1 where col0=123456789 or col2 >='2022-04-30 12:00:00') t;

-- query 83
-- The actual query on that force-converted path hits the same current planner/runtime failure.
-- @expect_error=Unknown error
select * from (select col0, col2, col3, id, col1 from test_base_table1 where col0=123456789 or col2 >='2022-04-30 12:00:00') t;

-- query 84
-- The equality variant hits the same unsupported EXPLAIN path today.
-- @expect_error=Unknown error
SET enable_materialized_view_rewrite = true;
EXPLAIN select * from (select col0, col2, col3, id, col1 from test_base_table1 where col0=123456789 or col2 ='2022-04-30 12:00:00') t;

-- query 85
-- The equality variant of the force-converted path fails the same way today.
-- @expect_error=Unknown error
select * from (select col0, col2, col3, id, col1 from test_base_table1 where col0=123456789 or col2 ='2022-04-30 12:00:00') t;

-- query 86
set select_ratio_threshold = 0.15;

-- query 87
set enable_sync_materialized_view_rewrite=false;

-- query 88
-- @result_contains_any=rollup: mv_test_base_table1
-- @result_contains_any=rollup: test_base_table1
SET enable_materialized_view_rewrite = true;
EXPLAIN select * from (select col0, col2, col3, id, col1 from test_base_table1 where col0=123456789 union select col0, col2, col3, id, col1 from mv_test_base_table1 [_SYNC_MV_] where col2 >='2022-04-30 12:00:00') t;

-- query 89
select * from (select col0, col2, col3, id, col1 from test_base_table1 where col0=123456789 union select col0, col2, col3, id, col1 from mv_test_base_table1 [_SYNC_MV_] where col2 >='2022-04-30 12:00:00') t order by 1;

-- query 90
set enable_sync_materialized_view_rewrite=true;

-- query 91
drop table test_base_table1;
