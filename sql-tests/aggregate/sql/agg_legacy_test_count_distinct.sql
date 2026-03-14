-- Migrated from dev/test/sql/test_agg_function/R/test_count_distinct
-- Test Objective:
-- Preserve legacy aggregate coverage in a self-contained sql-tests case.
-- query 1
-- @skip_result_check=true
DROP DATABASE IF EXISTS sql_tests_test_count_distinct FORCE;
CREATE DATABASE sql_tests_test_count_distinct;
USE sql_tests_test_count_distinct;

-- name: test_count_distinct
-- query 2
-- @skip_result_check=true
USE sql_tests_test_count_distinct;
CREATE TABLE `test_cc` (
  `v1` varchar(65533) NULL COMMENT "",
  `v2` varchar(65533) NULL COMMENT "",
  `v3` datetime NULL COMMENT "",
  `v4` int null,
  `v5` decimal(32, 2) null,
  `v6` array<int> null,
  `v7` struct<a bigint(20), b char(20)>  NULL
) ENGINE=OLAP
DUPLICATE KEY(v1, v2, v3)
PARTITION BY RANGE(`v3`)
(PARTITION p20220418 VALUES [("2022-04-18 00:00:00"), ("2022-04-19 00:00:00")),
PARTITION p20220419 VALUES [("2022-04-19 00:00:00"), ("2022-04-20 00:00:00")),
PARTITION p20220420 VALUES [("2022-04-20 00:00:00"), ("2022-04-21 00:00:00")),
PARTITION p20220421 VALUES [("2022-04-21 00:00:00"), ("2022-04-22 00:00:00")))
DISTRIBUTED BY HASH(`v1`) BUCKETS 4
PROPERTIES (
"replication_num" = "1",
"enable_persistent_index" = "true",
"replicated_storage" = "true",
"compression" = "LZ4"
);

-- query 3
-- @skip_result_check=true
USE sql_tests_test_count_distinct;
insert into test_cc values('a','a', '2022-04-18 01:01:00', 1, 1.2,  [1, 2, 3], row(1, 'a'));

-- query 4
-- @skip_result_check=true
USE sql_tests_test_count_distinct;
insert into test_cc values('a','b', '2022-04-18 02:01:00', 2, 1.3,  [2, 1, 3], row(2, 'a'));

-- query 5
-- @skip_result_check=true
USE sql_tests_test_count_distinct;
insert into test_cc values('a','a', '2022-04-18 02:05:00', 1, 2.3,  [2, 2, 3], row(3, 'a'));

-- query 6
-- @skip_result_check=true
USE sql_tests_test_count_distinct;
insert into test_cc values('a','b', '2022-04-18 02:15:00', 3, 3.31,  [2, 2, 3], row(4, 'a'));

-- query 7
-- @skip_result_check=true
USE sql_tests_test_count_distinct;
insert into test_cc values('a','b', '2022-04-18 03:15:00', 1, 100.3,  [3, 1, 3], row(2, 'a'));

-- query 8
-- @skip_result_check=true
USE sql_tests_test_count_distinct;
insert into test_cc values('c','a', '2022-04-18 03:45:00', 1, 200.3,  [2, 2, 3], row(3, 'a'));

-- query 9
-- @skip_result_check=true
USE sql_tests_test_count_distinct;
insert into test_cc values('c','a', '2022-04-18 03:25:00', 2, 300.3,  null, row(2, 'a'));

-- query 10
-- @skip_result_check=true
USE sql_tests_test_count_distinct;
insert into test_cc values('c','a', '2022-04-18 03:27:00', 3, 400.3,  [3, 1, 3], null);

-- query 11
USE sql_tests_test_count_distinct;
select v2, count(1), count(distinct v1) from test_cc group by v2 order by v2;

-- query 12
USE sql_tests_test_count_distinct;
select v2, bitmap_union_count(to_bitmap(v4)), count(distinct v1) from test_cc group by v2 order by v2;

-- query 13
USE sql_tests_test_count_distinct;
select v2, hll_union_agg(hll_hash(v4)), count(distinct v1) from test_cc group by v2 order by v2;

-- query 14
USE sql_tests_test_count_distinct;
select count(distinct 1, 2, 3, 4) from test_cc;

-- query 15
USE sql_tests_test_count_distinct;
select /*+ new_planner_agg_stage = 3 */ count(distinct 1, 2, 3, 4) from test_cc group by v2;

-- query 16
USE sql_tests_test_count_distinct;
select count(distinct 1, v2) from test_cc;

-- query 17
USE sql_tests_test_count_distinct;
select /*+ new_planner_agg_stage = 2 */ count(distinct 1, v2) from test_cc;

-- query 18
USE sql_tests_test_count_distinct;
select count(distinct 1, 2, 3, 4), sum(distinct 1), avg(distinct 1), group_concat(distinct 1, 2 order by 1), array_agg(distinct 1.3 order by null) from test_cc;

-- query 19
USE sql_tests_test_count_distinct;
select count(distinct 1, 2, 3, 4), sum(distinct 1), avg(distinct 1), group_concat(distinct 1, 2 order by 1), array_agg(distinct 1.3 order by null) from test_cc group by v2 order by v2;

-- query 20
USE sql_tests_test_count_distinct;
select v2, count(distinct v1), sum(distinct v1), avg(distinct v1), group_concat(distinct 1, 2), array_agg(distinct 1.3 order by null)  from test_cc group by v2 order by v2;

-- query 21
USE sql_tests_test_count_distinct;
select v2, count(distinct v4), sum(distinct v4), avg(distinct v4), group_concat(distinct 1, 2), array_agg(distinct 1.3 order by null) from test_cc group by v2 order by v2;

-- query 22
USE sql_tests_test_count_distinct;
select v2, count(distinct v5), sum(distinct v5), avg(distinct v5), group_concat(distinct 1, 2), array_agg(distinct 1.3 order by null) from test_cc group by v2 order by v2;

-- query 23
USE sql_tests_test_count_distinct;
select v2, count(distinct v6), array_agg(distinct v6 order by 1), group_concat(distinct 1, 2), array_agg(distinct 1.3 order by null) from test_cc group by v2 order by v2;

-- query 24
USE sql_tests_test_count_distinct;
select v2, count(distinct v7), array_agg(distinct v7 order by 1), group_concat(distinct 1, 2 order by 1), array_agg(distinct 1.3 order by null) from test_cc group by v2 order by v2;

-- query 25
USE sql_tests_test_count_distinct;
select v2, count(distinct v1, v3, v6), sum(distinct v1), avg(distinct v1), array_agg(v5 order by 1), group_concat(distinct 1, 2), array_agg(distinct 1.3 order by null)  from test_cc group by v2 order by v2;

-- query 26
USE sql_tests_test_count_distinct;
select count(distinct v4, v5), sum(distinct v4), avg(distinct v4), group_concat(distinct v4, v5, 2 order by 1,2), array_agg(distinct 1.456 order by 1) from test_cc;

-- query 27
USE sql_tests_test_count_distinct;
select v2, count(distinct v4, v5), sum(distinct v5), avg(distinct v5), group_concat(distinct v4, v5, 2 order by 1,2), array_agg(distinct 1.456 order by 1) from test_cc group by v2 order by v2;

-- query 28
USE sql_tests_test_count_distinct;
select v2, count(distinct v3, v5), sum(distinct v4), avg(distinct v5), group_concat(distinct v4, v5, 2 order by 1,2), array_agg(distinct 1.456 order by 1) from test_cc group by v2, v3 order by v2, v3;

-- query 29
USE sql_tests_test_count_distinct;
select count(distinct v4, v5), sum(distinct v4), avg(distinct v4), group_concat(distinct v4, v5, 2 order by 1,2), array_agg(distinct v4 order by 1) from test_cc;

-- query 30
USE sql_tests_test_count_distinct;
select v2, count(distinct v4, v5), sum(distinct v5), avg(distinct v5), group_concat(distinct v4, v5, 2 order by 1,2), array_agg(distinct v5 order by 1) from test_cc group by v2 order by v2;

-- query 31
USE sql_tests_test_count_distinct;
select v2, count(distinct v3, v5), sum(distinct v4), avg(distinct v5), group_concat(distinct v4, v5, 2 order by 1,2), array_agg(distinct v5 order by 1) from test_cc group by v2, v3 order by v2, v3;
