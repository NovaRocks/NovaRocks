-- Test Objective:
-- 1. Validate refreshing MVs across databases keeps rewrite behavior correct.
-- 2. Cover cross-database object resolution during refresh and query rewrite.
-- Source: dev/test/sql/test_materialized_view/T/test_refresh_mv_with_different_dbs

-- query 1
-- db1
create database db_${uuid0}_1;

-- query 2
use db_${uuid0}_1;

-- query 3
CREATE TABLE `t1` (
`k1` int,
`k2` int,
`k3` int
)
DUPLICATE KEY(`k1`)
DISTRIBUTED BY HASH(`k1`) BUCKETS 3;

-- query 4
-- db2
create database db_${uuid0}_2 ;

-- query 5
CREATE TABLE db_${uuid0}_2.t1 (
`k1` int,
`k2` int,
`k3` int
)
DUPLICATE KEY(`k1`)
DISTRIBUTED BY HASH(`k1`) BUCKETS 3;

-- query 6
INSERT INTO t1 VALUES (1,1,1);

-- query 7
INSERT INTO db_${uuid0}_2.t1 VALUES (1,2,2);

-- query 8
-- @skip_result_check=true
analyze full table t1;

-- query 9
-- @skip_result_check=true
analyze full table db_${uuid0}_2.t1;

-- query 10
-- create db with different dbs
CREATE MATERIALIZED VIEW mv1
DISTRIBUTED BY HASH(k1) BUCKETS 10
REFRESH ASYNC
AS SELECT t1.k1 as k1, t1.k2 as k2, t2.k1 as k3, t2.k2 as k4
FROM t1 join db_${uuid0}_2.t1 t2 on t1.k1=t2.k1;

-- query 11
-- @result_contains=ready
-- @retry_count=180
-- @retry_interval_ms=1000
SELECT IF(
    COUNT(*) > 0
    AND SUM(CASE WHEN a.STATE IN ('SUCCESS', 'MERGED', 'SKIPPED') THEN 0 ELSE 1 END) = 0,
    'ready',
    'pending'
) AS status
FROM information_schema.task_runs a
JOIN information_schema.materialized_views b
  ON a.task_name = b.task_name
WHERE b.table_name = 'mv1'
  AND a.`database` = 'db_${uuid0}_1';

-- query 12
-- @result_contains=mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT t1.k1 as k1, t1.k2 as k2, t2.k1 as k3, t2.k2 as k4 FROM t1 join db_${uuid0}_2.t1 t2 on t1.k1=t2.k1;

-- query 13
SELECT t1.k1 as k1, t1.k2 as k2, t2.k1 as k3, t2.k2 as k4
FROM t1 join db_${uuid0}_2.t1 t2 on t1.k1=t2.k1 order by 1, 2, 3, 4;

-- query 14
drop database db_${uuid0}_1 force;

-- query 15
drop database db_${uuid0}_2 force;
