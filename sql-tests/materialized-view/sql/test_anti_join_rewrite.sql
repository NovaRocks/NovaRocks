-- Test Objective:
-- 1. Validate MV rewrite coverage across different join semantics.
-- 2. Cover join-type-specific rewrite planning and final query correctness.
-- Source: dev/test/sql/test_materialized_view/T/test_mv_join_derivabllity_rewrite

-- query 1
CREATE TABLE t1 (
                                    k1 int,
                                    k2 int
                                )
                                DUPLICATE KEY(k1);

-- query 2
CREATE TABLE t2 (
                  a int,
                  b int
              )
              DUPLICATE KEY(a);

-- query 3
INSERT INTO t1 VALUES (1,1),(3,2),(1,null),(null,null);

-- query 4
INSERT INTO t2 VALUES (1,1),(2,2),(null,1),(null,null);

-- query 5
CREATE MATERIALIZED VIEW mv1 REFRESH MANUAL AS select * from t1 right outer join t2 on k1=a;

-- query 6
REFRESH MATERIALIZED VIEW mv1 with sync mode;

-- query 7
select * from t1 right anti join t2 on k1=a order by 1,2;
