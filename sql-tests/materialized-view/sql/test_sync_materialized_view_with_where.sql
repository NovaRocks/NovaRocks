-- Test Objective:
-- 1. Validate sync MV rewrite when the MV definition includes WHERE predicates.
-- 2. Cover predicate subsumption between base query and MV definition.
-- Source: dev/test/sql/test_materialized_view/T/test_sync_materialized_view_with_where

-- query 1
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
    "replication_num" = "1"
);

-- query 2
insert into duplicate_tbl values
    ('2023-06-15', '2023-06-15 00:00:00', 'a', 'a', false, 1, 1, 1, 1, 1, 1.0, 1.0, 1.0),
    ('2023-06-15', '2023-06-15 00:00:00', 'a', 'a', false, 2, 2, 2, 2, 2, 2.0, 2.0, 2.0),
    ('2023-06-15', '2023-06-15 00:00:00', 'a', 'a', false, 3, 3, 3, 3, 3, 3.0, 3.0, 3.0),
    ('2023-06-16', '2023-06-15 00:00:00', 'a', 'a', false, 4, 4, 4, 4, 4, 4.0, 4.0, 4.0),
    ('2023-06-15', '2023-06-15 00:00:00', 'a', 'a', false, 5, 1, 1, 1, 1, 1.0, 1.0, 1.0),
    ('2023-06-15', '2023-06-15 00:00:00', 'a', 'a', false, 6, 7, 2, 2, 2, 2.0, 2.0, 2.0),
    ('2023-06-15', '2023-06-15 00:00:00', 'a', 'a', false, 7, 8, 3, 3, 3, 3.0, 3.0, 3.0),
    ('2023-06-15', '2023-06-15 00:00:00', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
    ('2023-06-16', '2023-06-15 00:00:00', 'a', 'a', false, 8, 10, 4, 4, 4, 4.0, 4.0, 4.0)
;

-- query 3
-- test agg with filter
create materialized view mv_1 as select k1, sum(k6) as sum_k6, max(k7) as max from duplicate_tbl where k7 > 2  group by 1;

-- query 4
-- Wait for the first MV create job to settle before issuing rewrite checks.
-- @skip_result_check=true
SHOW ALTER MATERIALIZED VIEW ORDER BY JobId DESC LIMIT 1;
SELECT sleep(2);

-- query 5
-- @result_contains=mv_1
SET enable_materialized_view_rewrite = true;
EXPLAIN select k1, sum(k6), max(k7) as max from duplicate_tbl where k7 > 2  group by 1;

-- query 6
-- @result_contains=mv_1
SET enable_materialized_view_rewrite = true;
EXPLAIN select k1, sum(k6), max(k7) as max from duplicate_tbl where k7 > 2 and k1 > 1 group by 1;

-- query 7
-- @result_not_contains=mv_1
SET enable_materialized_view_rewrite = true;
EXPLAIN select k1, sum(k6), max(k7 + 1) as max from duplicate_tbl where k7 > 2  group by 1;

-- query 8
-- @result_not_contains=mv_1
SET enable_materialized_view_rewrite = true;
EXPLAIN select k1, sum(k6), max(k7) as max from duplicate_tbl where k7 > 3  group by 1;

-- query 9
select k1, sum(k6), max(k7) as max from duplicate_tbl where k7 > 2  group by 1 order by 1;

-- query 10
select k1, sum(k6), max(k7) as max from duplicate_tbl where k7 > 2 and k1 > '2023-06-15' group by 1 order by 1;

-- query 11
-- test project with filter
create materialized view mv_2 as select k1, k2, k3, k4, k5, k6, k7 from duplicate_tbl where k7 > 2 ;

-- query 12
-- Wait for the second MV create job to settle before later drop/create operations.
-- @skip_result_check=true
SHOW ALTER MATERIALIZED VIEW ORDER BY JobId DESC LIMIT 1;
SELECT sleep(2);

-- query 13
-- The exact predicate match rewrites through the projection MV.
-- @result_contains=mv_2
SET enable_materialized_view_rewrite = true;
EXPLAIN select k1, k2, k3, k4, k5, k6, k7 from duplicate_tbl where k7 > 2;

-- query 14
-- A stronger filter still qualifies for predicate subsumption rewrite.
-- @result_contains=mv_2
SET enable_materialized_view_rewrite = true;
EXPLAIN select k1, k2, k3, k4, k5, k6, k7 from duplicate_tbl where k7 > 3;

-- query 15
-- Additional residual predicates still preserve the rewrite through mv_2.
-- @result_contains=mv_2
SET enable_materialized_view_rewrite = true;
EXPLAIN select k1, k2, k3, k4, k5, k6, k7 from duplicate_tbl where k7 > 3 and k1 > 1;

-- query 16
select k1, k2, k3, k4, k5, k6, k7 from duplicate_tbl where k7 > 2 order by k1;

-- query 17
insert into duplicate_tbl values
    ('2023-06-15', '2023-06-15 00:00:00', 'a', 'a', false, 9, 9, 3, 3, 3, 3.0, 3.0, 3.0),
    ('2023-06-15', '2023-06-15 00:00:00', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
    ('2023-06-16', '2023-06-15 00:00:00', 'a', 'a', false, 10, 10, 4, 4, 4, 4.0, 4.0, 4.0)
;

-- query 18
select k1, sum(k6), max(k7) as max from duplicate_tbl where k7 > 2  group by 1 order by 1;

-- query 19
select k1, sum(k6), max(k7) as max from duplicate_tbl where k7 > 2 and k1 > '2023-06-15' group by 1 order by 1;

-- query 20
select k1, k2, k3, k4, k5, k6, k7 from duplicate_tbl where k7 > 2 order by k1;

-- query 21
drop materialized view mv_1;

-- query 22
drop materialized view mv_2;

-- query 23
-- test agg with multi filter
create materialized view mv_1 as select k1, k8, sum(k6) as sum_k6 , max(k7) as max_k7 from duplicate_tbl where k7 > 2 and k8 < 4 group by 1,2;

-- query 24
-- Wait for the aggregate MV rebuild to settle before continuing.
-- @skip_result_check=true
SHOW ALTER MATERIALIZED VIEW ORDER BY JobId DESC LIMIT 1;
SELECT sleep(2);

-- query 25
-- @result_contains=mv_1
SET enable_materialized_view_rewrite = true;
EXPLAIN select k1, sum(k6), max(k7) from duplicate_tbl where k7 > 2 and k8 < 4 group by 1;

-- query 26
-- @result_not_contains=mv_1
SET enable_materialized_view_rewrite = true;
EXPLAIN select k1, sum(k6), max(k7) from duplicate_tbl where k7 > 3 and k8 < 4 group by 1;

-- query 27
select k1, sum(k6), max(k7) from duplicate_tbl where k7 > 2 and k8 < 4 group by 1 order by 1;

-- query 28
select k1, sum(k6), max(k7) from duplicate_tbl where k7 > 3 and k8 < 4 group by 1 order by 1;

-- query 29
-- test project with filter (with multi predicates)
create materialized view mv_2 as select  k1, k2, k3, k4, k5, k6, k7,k8  from duplicate_tbl where k7 > 2 and k8 < 4;

-- query 30
-- Wait for the projection MV rebuild to settle before rewrite checks.
-- @skip_result_check=true
SHOW ALTER MATERIALIZED VIEW ORDER BY JobId DESC LIMIT 1;
SELECT sleep(2);

-- query 31
-- @result_contains=mv_2
SET enable_materialized_view_rewrite = true;
EXPLAIN select  k1, k2, k3, k4, k5, k6, k7  from duplicate_tbl where k7 > 2 and k8 < 4;

-- query 32
-- @result_contains=mv_2
SET enable_materialized_view_rewrite = true;
EXPLAIN select  k1, k2, k3, k4, k5, k6, k7  from duplicate_tbl where k7 > 3 and k8 < 2;

-- query 33
-- @result_not_contains=mv_2
SET enable_materialized_view_rewrite = true;
EXPLAIN select  k1, k2, k3, k4, k5, k6, k7  from duplicate_tbl where k7 > 0 and k8 < 10;

-- query 34
select  k1, k2, k3, k4, k5, k6, k7  from duplicate_tbl where k7 > 2 and k8 < 4 order by k1;

-- query 35
select  k1, k2, k3, k4, k5, k6, k7  from duplicate_tbl where k7 > 3 and k8 < 10 order by k1;

-- query 36
insert into duplicate_tbl values
    ('2023-06-15', '2023-06-15 00:00:00', 'a', 'a', false, 9, 9, 3, 3, 3, 3.0, 3.0, 3.0),
    ('2023-06-15', '2023-06-15 00:00:00', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
    ('2023-06-16', '2023-06-15 00:00:00', 'a', 'a', false, 10, 10, 4, 4, 4, 4.0, 4.0, 4.0)
;

-- query 37
select k1, sum(k6), max(k7) as max_k7 from duplicate_tbl where k7 > 2 and k8 < 4 group by 1 order by 1;

-- query 38
select k1, sum(k6), max(k7) as max_k7 from duplicate_tbl where k7 > 3 and k8 < 4 group by 1 order by 1;

-- query 39
select k1, k2, k3, k4, k5, k6, k7  from duplicate_tbl where k7 > 2 and k8 < 4 order by k1;

-- query 40
select k1, k2, k3, k4, k5, k6, k7  from duplicate_tbl where k7 > 3 and k8 < 10 order by k1;

-- query 41
drop materialized view mv_1;

-- query 42
drop materialized view mv_2;

-- query 43
-- test where with complex expressions
create materialized view mv_1 as select k1, k8, sum(k6) as sum_k6, max(k7) as max_k7 from duplicate_tbl where k7 + 1 > 2 and k8 * 2 < 4 group by 1, 2;

-- query 44
-- Wait for the complex-expression aggregate MV create to settle before dependent checks.
-- @skip_result_check=true
SHOW ALTER MATERIALIZED VIEW ORDER BY JobId DESC LIMIT 1;
SELECT sleep(2);

-- query 45
-- @result_contains=mv_1
SET enable_materialized_view_rewrite = true;
EXPLAIN select k1, k8, sum(k6) as sum_k6, max(k7) as max_k7 from duplicate_tbl where k7 + 1 > 2 and k8 * 2 < 4 group by 1, 2;

-- query 46
-- @result_not_contains=mv_1
SET enable_materialized_view_rewrite = true;
EXPLAIN select k1, sum(k6), max(k7) from duplicate_tbl where k7 > 3 and k8 < 4 group by 1;

-- query 47
select k1, sum(k6), max(k7) from duplicate_tbl where k7 > 2 and k8 < 4 group by 1 order by 1;

-- query 48
select k1, sum(k6), max(k7) from duplicate_tbl where k7 > 3 and k8 < 4 group by 1 order by 1;

-- query 49
-- test project with filter (with multi predicates)
create materialized view mv_2 as select k1, k2, k3, k4, k5, k6, k7,k8 from duplicate_tbl where k7 + 1 > 2 and k8 * 2 > 4;

-- query 50
-- Wait for the complex-expression projection MV create to settle before rewrite checks.
-- @skip_result_check=true
SHOW ALTER MATERIALIZED VIEW ORDER BY JobId DESC LIMIT 1;
SELECT sleep(2);

-- query 51
-- @result_contains=mv_2
SET enable_materialized_view_rewrite = true;
EXPLAIN select k1, k2, k3, k4, k5, k6, k7,k8 from duplicate_tbl where k7 + 1 > 2 and k8 * 2 > 4;

-- query 52
-- @result_not_contains=mv_2
SET enable_materialized_view_rewrite = true;
EXPLAIN select k1, k2, k3, k4, k5, k6, k7 from duplicate_tbl where k7 > 3 and k8 < 10;

-- query 53
-- @result_not_contains=mv_2
SET enable_materialized_view_rewrite = true;
EXPLAIN select k1, k2, k3, k4, k5, k6, k7 from duplicate_tbl where k7 > 0 and k8 < 10;

-- query 54
select k1, k2, k3, k4, k5, k6, k7 from duplicate_tbl where k7 > 2 and k8 < 4 order by k1;

-- query 55
select k1, k2, k3, k4, k5, k6, k7 from duplicate_tbl where k7 > 3 and k8 < 10 order by k1;

-- query 56
insert into duplicate_tbl values
    ('2023-06-15', '2023-06-15 00:00:00', 'a', 'a', false, 9, 9, 3, 3, 3, 3.0, 3.0, 3.0),
    ('2023-06-15', '2023-06-15 00:00:00', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
    ('2023-06-16', '2023-06-15 00:00:00', 'a', 'a', false, 10, 10, 4, 4, 4, 4.0, 4.0, 4.0)
;

-- query 57
select k1, sum(k6), max(k7) from duplicate_tbl where k7 > 2 and k8 < 4 group by 1 order by 1;

-- query 58
select k1, sum(k6), max(k7) from duplicate_tbl where k7 > 3 and k8 < 4 group by 1 order by 1;

-- query 59
select k1, k2, k3, k4, k5, k6, k7 from duplicate_tbl where k7 > 2 and k8 < 4 order by k1;

-- query 60
select k1, k2, k3, k4, k5, k6, k7 from duplicate_tbl where k7 > 3 and k8 < 10 order by k1;

-- query 61
drop materialized view mv_1;

-- query 62
drop materialized view mv_2;
