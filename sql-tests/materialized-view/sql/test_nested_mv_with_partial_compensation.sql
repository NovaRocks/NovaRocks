-- Test Objective:
-- 1. Validate nested materialized views rewrite correctly across refresh boundaries.
-- 2. Cover partial compensation and nested rewrite plan selection.
-- Source: dev/test/sql/test_materialized_view/T/test_nested_mv_rewrite

-- query 1
CREATE TABLE t1 (
    `k1` INT,
    `v1` INT,
    `v2` INT)
DUPLICATE KEY(`k1`)
PARTITION BY RANGE(`k1`)
(
PARTITION `p1` VALUES LESS THAN ('2'),
PARTITION `p2` VALUES LESS THAN ('3'),
PARTITION `p3` VALUES LESS THAN ('4'),
PARTITION `p4` VALUES LESS THAN ('5'),
PARTITION `p5` VALUES LESS THAN ('6'),
PARTITION `p6` VALUES LESS THAN ('7')
)
DISTRIBUTED BY HASH(k1);

-- query 2
insert into t1 values (1,1,1),(1,1,2),(1,1,3),(1,2,1),(1,2,2),(1,2,3),(1,3,1),(1,3,2),(1,3,3)
    ,(2,1,1),(2,1,2),(2,1,3),(2,2,1),(2,2,2),(2,2,3),(2,3,1),(2,3,2),(2,3,3)
    ,(3,1,1),(3,1,2),(3,1,3),(3,2,1),(3,2,2),(3,2,3),(3,3,1),(3,3,2),(3,3,3);

-- query 3
CREATE MATERIALIZED VIEW mv1 PARTITION BY k1 DISTRIBUTED BY HASH(k1) BUCKETS 10
REFRESH DEFERRED MANUAL AS SELECT k1, v1 as k2, v2 as k3 from t1;

-- query 4
CREATE MATERIALIZED VIEW mv2 PARTITION BY k1 DISTRIBUTED BY HASH(k1) BUCKETS 10
REFRESH DEFERRED MANUAL AS SELECT k1, count(k2) as count_k2, sum(k3) as sum_k3 from mv1 group by k1;

-- query 5
-- first refresh mv2
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW mv2 WITH SYNC MODE;

-- query 6
select count(1) from (
    select k1 from mv1 where k1 = 1
    union all
    select k1 from t1 where k1 = 2
    union all
    select k1 from mv1 where k1 = 3
    union all
    select k1 from mv1 where k1 = 4
) as t;

-- query 7
-- @result_not_contains=MaterializedView: true
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT k1, count(v1), sum(v2) from t1 group by k1;

-- query 8
SELECT k1, count(v1), sum(v2) from t1 group by k1 order by 1;

-- query 9
-- then refresh mv1
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW mv1 WITH SYNC MODE;

-- query 10
select count(1) from (
    select k1 from mv1 where k1 = 1
    union all
    select k1 from t1 where k1 = 2
    union all
    select k1 from mv1 where k1 = 3
    union all
    select k1 from mv1 where k1 = 4
) as t;

-- query 11
-- After refreshing mv1, the nested aggregate query is currently rewritten through the MV chain.
-- @result_contains=MaterializedView: true
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT k1, count(v1), sum(v2) from t1 group by k1;

-- query 12
SELECT k1, count(v1), sum(v2) from t1 group by k1 order by 1;

-- query 13
insert into t1 values (4,1,1);

-- query 14
-- first refresh mv2
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW mv2 WITH SYNC MODE;

-- query 15
select count(1) from (
    select k1 from mv1 where k1 = 1
    union all
    select k1 from t1 where k1 = 2
    union all
    select k1 from mv1 where k1 = 3
    union all
    select k1 from mv1 where k1 = 4
) as t;

-- query 16
-- Current NovaRocks still rewrites through the nested MV chain after refreshing mv2 only.
-- @result_contains=MaterializedView: true
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT k1, count(v1), sum(v2) from t1 group by k1;

-- query 17
SELECT k1, count(v1), sum(v2) from t1 group by k1 order by 1;

-- query 18
-- then refresh mv1
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW mv1 WITH SYNC MODE;

-- query 19
select count(1) from (
    select k1 from mv1 where k1 = 1
    union all
    select k1 from t1 where k1 = 2
    union all
    select k1 from mv1 where k1 = 3
    union all
    select k1 from mv1 where k1 = 4
) as t;

-- query 20
-- After refreshing mv1 again, the nested query still rewrites through the MV chain.
-- @result_contains=MaterializedView: true
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT k1, count(v1), sum(v2) from t1 group by k1;

-- query 21
SELECT k1, count(v1), sum(v2) from t1 group by k1 order by 1;

-- query 22
drop materialized view mv1;

-- query 23
drop materialized view mv2;

-- query 24
drop table t1;
