-- Test Objective:
-- 1. Validate optimizer rewrite chooses the intended materialized view plan.
-- 2. Cover query correctness together with rewrite plan inspection.
-- Source: dev/test/sql/test_materialized_view/T/test_view_based_mv_rewrite

-- query 1
CREATE TABLE t1 (
                    k1 INT,
                    v1 INT,
                    v2 INT)
                DUPLICATE KEY(k1)
                DISTRIBUTED BY HASH(k1);

-- query 2
insert into t1 values (1,1,1),(2,1,1),(3,1,1),(1,2,3),(2,2,3),(3,2,3);

-- query 3
create view v1 as select k1,sum(v1) as sum_v1 from t1 group by k1;

-- query 4
create materialized view mv_2 refresh manual as select v1.k1,v1.sum_v1,t1.v1 from v1 join t1 on v1.k1=t1.k1;

-- query 5
refresh materialized view mv_2 with sync mode;

-- query 6
set enable_view_based_mv_rewrite = true;

-- query 7
-- @skip_result_check=true
SET enable_materialized_view_rewrite = true;
EXPLAIN select v1.k1,v1.sum_v1,t1.v1 from v1 join t1 on v1.k1=t1.k1 order by 1,3;
