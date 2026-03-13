-- Test Objective:
-- 1. Validate multi-partition-column MV behavior under DDL, refresh, and rewrite changes.
-- 2. Cover activation, rename, swap, and optimize flows for multi-column partition MVs.
-- Source: dev/test/sql/test_materialized_view/T/test_mv_with_multi_partition_columns_optimize

-- query 1
CREATE TABLE t1 (
    k1 int,
    k2 date,
    k3 string
)
DUPLICATE KEY(k1)
PARTITION BY date_trunc("day", k2);

-- query 2
INSERT INTO t1 VALUES (1,'2020-06-02','BJ'),(3,'2020-06-02','SZ'),(2,'2020-07-02','SH');

-- query 3
CREATE MATERIALIZED VIEW mv1
partition by (date_trunc("day", k2))
REFRESH MANUAL
AS select sum(k1), k2, k3 from t1 group by k2, k3;

-- query 4
REFRESH MATERIALIZED VIEW mv1 with sync mode;

-- query 5
-- @result_contains=mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN select k2, k3, sum(k1) from t1 group by k2, k3 order by 1,2;

-- query 6
select k2, k3, sum(k1) from t1 group by k2, k3 order by 1,2;

-- query 7
alter table t1 partition by date_trunc("month", k2);

-- query 8
-- The alter-table job state text is not stable across engines; wait for it to settle before checking rewrite fallback.
-- @skip_result_check=true
SHOW ALTER TABLE COLUMN ORDER BY JobId DESC LIMIT 1;
SELECT sleep(3);

-- query 9
-- @result_not_contains=mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN select k2, k3, sum(k1) from t1 group by k2, k3 order by 1,2;

-- query 10
select k2, k3, sum(k1) from t1 group by k2, k3 order by 1,2;

-- query 11
-- refresh should not re-active the materialized view
INSERT INTO t1 VALUES (1,'2020-06-02','BJ'),(3,'2020-06-02','SZ'),(2,'2020-07-02','SH');

-- query 12
-- Refresh must fail because optimize changed the base table partitioning and left the MV inactive.
-- @expect_error=not active due to base-table optimized:t1
REFRESH MATERIALIZED VIEW mv1 with sync mode;

-- query 13
-- @result_not_contains=mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN select k2, k3, sum(k1) from t1 group by k2, k3 order by 1,2;
