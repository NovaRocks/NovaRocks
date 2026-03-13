-- Test Objective:
-- 1. Validate multi-partition-column MV behavior under DDL, refresh, and rewrite changes.
-- 2. Cover activation, rename, swap, and optimize flows for multi-column partition MVs.
-- Source: dev/test/sql/test_materialized_view/T/test_mv_with_multi_partition_columns_basic

-- query 1
CREATE TABLE t1 (
    k1 int,
    k2 date,
    k3 string
)
DUPLICATE KEY(k1)
PARTITION BY date_trunc("day", k2), k3;

-- query 2
INSERT INTO t1 VALUES (1,'2020-06-02','BJ'),(3,'2020-06-02','SZ'),(2,'2020-07-02','SH');

-- query 3
-- Create Fail: mv's partition columns are not the same as table's partition columns
CREATE MATERIALIZED VIEW test_mv1
partition by (date_trunc("day", k2))
REFRESH MANUAL
AS select sum(k1), k2, k3 from t1 group by k2, k3;

-- query 4
REFRESH MATERIALIZED VIEW test_mv1 WITH SYNC MODE;

-- query 5
-- @result_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN select sum(k1), k2, k3 from t1 group by k2, k3;

-- query 6
select sum(k1), k2, k3 from t1 group by k2, k3;
