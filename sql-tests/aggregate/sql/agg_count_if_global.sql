-- @order_sensitive=true
-- @tags=aggregate,count_if
-- Test Objective:
-- 1. Validate global count_if semantics after FE rewrite to count_if(1, predicate).
-- 2. Prevent regressions where rewrite constants are incorrectly counted as data input.
-- Test Flow:
-- 1. Create/reset the source table.
-- 2. Insert deterministic rows including NULL.
-- 3. Assert global count_if outputs.
CREATE DATABASE IF NOT EXISTS sql_tests_d06;
DROP TABLE IF EXISTS sql_tests_d06.t_agg_count_if_global;
CREATE TABLE sql_tests_d06.t_agg_count_if_global (
    k INT,
    v INT
);

INSERT INTO sql_tests_d06.t_agg_count_if_global VALUES
    (1, 10),
    (2, 20),
    (3, NULL),
    (4, 30),
    (5, 15);

SELECT
    count_if(v > 15) AS c_gt_15,
    count_if(v IS NULL) AS c_null
FROM sql_tests_d06.t_agg_count_if_global;
