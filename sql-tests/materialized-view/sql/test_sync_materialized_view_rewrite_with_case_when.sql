-- Test Objective:
-- 1. Validate sync MV rewrite when aggregates include CASE WHEN expressions.
-- 2. Cover conditional aggregate rewrite correctness.
-- Source: dev/test/sql/test_materialized_view/T/test_sync_materialized_view_rewrite_with_case_when

-- query 1
admin set frontend config('alter_scheduler_interval_millisecond' = '100');

-- query 2
CREATE TABLE `t1` (
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
DUPLICATE KEY(`k1`, `k2`, `k3`, `k4`, `k5`)
DISTRIBUTED BY HASH(`k1`, `k2`, `k3`) BUCKETS 3;

-- query 3
-- add duplicated rows
insert into t1 values
    ('2023-06-15', '2023-06-15 00:00:00', 'a', 'a', false, 1, 1, 1, 1, 1, 1.0, 1.0, 1.0),
    ('2023-06-15', '2023-06-15 01:00:00', 'b', 'a', true,  1, 2, 2, 2, 2, 2.0, 2.0, 1.0),
    ('2023-06-16', '2023-06-16 00:00:00', 'c', 'a', false, 3, 1, 3, 3, 3, 3.0, 3.0, 1.0),
    ('2023-06-17', '2023-06-17 00:00:00', 'd', 'a', true,  4, 1, 4, 4, 4, 4.0, 4.0, 1.0),
    ('2023-06-15', '2023-06-15 00:00:00', 'a', 'a', false, 1, 1, 1, 1, 1, 1.0, 1.0, 1.0),
    ('2023-06-15', '2023-06-15 01:00:00', 'b', 'a', true,  1, 2, 2, 2, 2, 2.0, 2.0, 1.0),
    ('2023-06-16', '2023-06-16 00:00:00', 'c', 'a', false, 3, 1, 3, 3, 3, 3.0, 3.0, 1.0),
    ('2023-06-17', '2023-06-17 00:00:00', 'd', 'a', true,  4, 1, 4, 4, 4, 4.0, 4.0, 1.0)
;

-- query 4
CREATE MATERIALIZED VIEW test_mv1
AS SELECT k1, k6, SUM(k7) as sum1, SUM(k9) as sum2, SUM(k8) as sum3 FROM t1 GROUP BY k1, k6;

-- query 5
-- @result_contains=FINISHED
-- @retry_count=60
-- @retry_interval_ms=1000
SHOW ALTER MATERIALIZED VIEW ORDER BY JobId DESC LIMIT 1;

-- query 6
-- @result_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT k1, sum(case when k6 > 1 then k9 else 0 end) from t1 group by k1 order by k1;

-- query 7
-- @result_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT k1, sum(case when k6 > 1 then k9 + 1 else 0 end) from t1 group by k1 order by k1;

-- query 8
-- @result_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT k1, sum(case when k6 = 1 then k9 else 0 end) from t1 group by k1 order by k1;

-- query 9
-- @result_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT k1, sum(case when k6 = 1 then k9 + 1 else 0 end) from t1 group by k1 order by k1;

-- query 10
-- @result_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT k1, sum(k9), sum(if(k6=0, k9, 0)) as cnt0, sum(if(k6=1, k9, 0)) as cnt1,  sum(if(k6=2, k9, 0)) as cnt2 from t1 group by k1 order by k1;

-- query 11
-- @result_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT k1, sum(if(k6 > 1, k9, 0)) as cnt0 from t1 group by k1 order by k1;

-- query 12
SELECT k1, sum(case when k6 > 1 then k9 else 0 end) from t1 group by k1 order by k1;

-- query 13
SELECT k1, sum(case when k6 > 1 then k9 + 1 else 0 end) from t1 group by k1 order by k1;

-- query 14
SELECT k1, sum(case when k6 = 1 then k9 else 0 end) from t1 group by k1 order by k1;

-- query 15
SELECT k1, sum(case when k6 = 1 then k9 + 1 else 0 end) from t1 group by k1 order by k1;

-- query 16
SELECT k1, sum(k9), sum(if(k6=0, k9, 0)) as cnt0, sum(if(k6=1, k9, 0)) as cnt1,  sum(if(k6=2, k9, 0)) as cnt2 from t1 group by k1 order by k1;

-- query 17
SELECT k1, sum(if(k6 > 1, k9, 0)) as cnt0 from t1 group by k1 order by k1;

-- query 18
DROP MATERIALIZED VIEW test_mv1;

-- query 19
CREATE MATERIALIZED VIEW test_mv1
AS SELECT k1, k6, SUM(k9) as sum1, MAX(k10 + 2 * k11) as max1, SUM(2 * k13) as sum2 FROM t1 GROUP BY k1, k6;

-- query 20
-- @result_contains=FINISHED
-- @retry_count=60
-- @retry_interval_ms=1000
SHOW ALTER MATERIALIZED VIEW ORDER BY JobId DESC LIMIT 1;

-- query 21
-- @result_not_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT k1, sum(case when k6 > 1 then k9 else 0 end) from t1 group by k1 order by k1;

-- query 22
-- @result_not_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT k1, sum(case when k6 > 1 then k9 + 1 else 0 end) from t1 group by k1 order by k1;

-- query 23
-- @result_not_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT k1, sum(case when k6 = 1 then k9 else 0 end) from t1 group by k1 order by k1;

-- query 24
-- @result_not_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT k1, sum(case when k6 = 1 then k9 + 1 else 0 end) from t1 group by k1 order by k1;

-- query 25
-- @result_not_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT k1, sum(k9), sum(if(k6=0, k9, 0)) as cnt0, sum(if(k6=1, k9, 0)) as cnt1,  sum(if(k6=2, k9, 0)) as cnt2 from t1 group by k1 order by k1;

-- query 26
-- @result_not_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT k1, sum(if(k6 > 1, k9, 0)) as cnt0 from t1 group by k1 order by k1;

-- query 27
SELECT k1, sum(case when k6 > 1 then k9 else 0 end) from t1 group by k1 order by k1;

-- query 28
SELECT k1, sum(case when k6 > 1 then k9 + 1 else 0 end) from t1 group by k1 order by k1;

-- query 29
SELECT k1, sum(case when k6 = 1 then k9 else 0 end) from t1 group by k1 order by k1;

-- query 30
SELECT k1, sum(case when k6 = 1 then k9 + 1 else 0 end) from t1 group by k1 order by k1;

-- query 31
SELECT k1, sum(k9), sum(if(k6=0, k9, 0)) as cnt0, sum(if(k6=1, k9, 0)) as cnt1,  sum(if(k6=2, k9, 0)) as cnt2 from t1 group by k1 order by k1;

-- query 32
SELECT k1, sum(if(k6 > 1, k9, 0)) as cnt0 from t1 group by k1 order by k1;

-- query 33
DROP MATERIALIZED VIEW IF EXISTS test_mv1;

-- query 34
CREATE MATERIALIZED VIEW test_mv1
DISTRIBUTED BY RANDOM
AS SELECT k1, k6, SUM(k7) as sum1, SUM(k9) as sum2, SUM(k8) as sum3 FROM t1 GROUP BY k1, k6;

-- query 35
refresh materialized view test_mv1 with sync mode;

-- query 36
-- @result_not_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT count(1) from t1;

-- query 37
-- @result_not_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT count(*) from t1;

-- query 38
-- @result_not_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT count(k6) from t1;

-- query 39
-- @result_not_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT k1, sum(case when k6 > 1 then k6 else 0 end) from t1 group by k1 order by k1;

-- query 40
-- @result_not_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT k1, sum(case when k6 > 1 then k6 + 1 else 0 end) from t1 group by k1 order by k1;

-- query 41
-- @result_not_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT k1, sum(case when k6 = 1 then k6 else 0 end) from t1 group by k1 order by k1;

-- query 42
-- @result_not_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT k1, sum(case when k6 = 1 then k6 + 1 else 0 end) from t1 group by k1 order by k1;

-- query 43
-- @result_not_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT k1, sum(if(k6 > 1, k6, 0)) as cnt0 from t1 group by k1 order by k1;

-- query 44
SELECT count(1) from t1;

-- query 45
SELECT count(*) from t1;

-- query 46
SELECT count(k6) from t1;

-- query 47
SELECT k1, sum(case when k6 > 1 then k6 else 0 end) from t1 group by k1 order by k1;

-- query 48
SELECT k1, sum(case when k6 > 1 then k6 + 1 else 0 end) from t1 group by k1 order by k1;

-- query 49
SELECT k1, sum(case when k6 = 1 then k6 else 0 end) from t1 group by k1 order by k1;

-- query 50
SELECT k1, sum(case when k6 = 1 then k6 + 1 else 0 end) from t1 group by k1 order by k1;

-- query 51
SELECT k1, sum(if(k6 > 1, k6, 0)) as cnt0 from t1 group by k1 order by k1;

-- query 52
drop table t1;
