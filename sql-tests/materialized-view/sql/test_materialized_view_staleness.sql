-- Test Objective:
-- 1. Validate MV staleness configuration gates rewrite eligibility.
-- 2. Cover stale and fresh rewrite behavior on the same MV.
-- Source: dev/test/sql/test_materialized_view/T/test_materialized_view_staleness

-- query 1
drop table if exists t1;

-- query 2
CREATE TABLE t1 (
    k1 int,
    k2 int
) DUPLICATE KEY(k1)
properties (
    "replication_num" = "1"
);

-- query 3
INSERT INTO t1 VALUES (1,1),(1,2),(null,null);

-- query 4
drop materialized view if exists mv1;

-- query 5
CREATE MATERIALIZED VIEW mv1 REFRESH MANUAL
properties (
    "replication_num" = "1",
    "mv_rewrite_staleness_second" = "10"
)
 AS SELECT k1,sum(k2) FROM t1 group by k1;

-- query 6
REFRESH MATERIALIZED VIEW mv1 with sync mode;

-- query 7
select * from mv1 order by k1;

-- query 8
INSERT INTO t1 VALUES (2,2);

-- query 9
REFRESH MATERIALIZED VIEW mv1 with sync mode;

-- query 10
select * from mv1 order by k1;
