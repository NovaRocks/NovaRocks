-- Test Objective:
-- 1. Validate inactive/active MV transitions after base-view or base-table changes.
-- 2. Cover rewrite eligibility while the MV is inactive.
-- Source: dev/test/sql/test_materialized_view/T/test_mv_inactive_list

-- query 1
create database db_${uuid0};

-- query 2
use db_${uuid0};

-- query 3
CREATE TABLE t1 (
    k1 date,
    k2 int,
    k3 int
)
DUPLICATE KEY(k1)
COMMENT "OLAP"
PARTITION BY (k1, k2)
PROPERTIES (
    "replication_num" = "1"
);

-- query 4
INSERT INTO t1 VALUES ('2020-06-02',1,1),('2020-06-02',2,2),('2020-07-02',3,3);

-- query 5
CREATE VIEW v1 AS SELECT k1,min(k2) as k2,min(k3) as k3 FROM t1 GROUP BY k1;

-- query 6
CREATE MATERIALIZED VIEW mv1 REFRESH MANUAL AS select k1, k2 from v1;

-- query 7
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW mv1 with sync mode;

-- query 8
SELECT k1,min(k2) as k2 FROM t1 GROUP BY k1 order by 1;

-- query 9
-- @result_contains=mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT k1,min(k2) as k2 FROM t1 GROUP BY k1 order by 1;

-- query 10
-- @result_contains=mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT k1,min(k2) as k2 FROM t1 GROUP BY k1 order by 1;

-- query 11
-- @skip_result_check=true
ALTER MATERIALIZED VIEW mv1 inactive;

-- query 12
-- @result_not_contains=mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT k1,min(k2) as k2 FROM t1 GROUP BY k1 order by 1;

-- query 13
-- @result_not_contains=mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT k1,min(k2) as k2 FROM t1 GROUP BY k1 order by 1;

-- query 14
-- Current NovaRocks refuses to refresh an inactive MV until it is explicitly activated.
-- @expect_error=Refresh materialized view failed because [mv1] is not active
REFRESH MATERIALIZED VIEW mv1 with sync mode;

-- query 15
-- @skip_result_check=true
ALTER MATERIALIZED VIEW mv1 active;

-- query 16
-- @result_contains=mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT k1,min(k2) as k2 FROM t1 GROUP BY k1 order by 1;

-- query 17
-- @result_contains=mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT k1,min(k2) as k2 FROM t1 GROUP BY k1 order by 1;

-- query 18
SELECT k1,min(k2) as k2 FROM t1 GROUP BY k1 order by 1;

-- query 19
SELECT k1,max(k2) FROM t1 GROUP BY k1 order by 1;

-- query 20
drop database db_${uuid0} force;
