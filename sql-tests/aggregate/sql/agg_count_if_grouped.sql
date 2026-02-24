-- @order_sensitive=true
-- @tags=aggregate,count_if
-- Test Objective:
-- 1. Validate grouped count_if semantics with deterministic ordering.
-- 2. Prevent regressions in partial/final merge handling for count_if.
-- Test Flow:
-- 1. Create/reset grouped source table.
-- 2. Insert rows that exercise true/false/null predicate outcomes.
-- 3. Group by key and assert ordered count_if outputs.
CREATE DATABASE IF NOT EXISTS sql_tests_d06;
DROP TABLE IF EXISTS sql_tests_d06.t_agg_count_if_grouped;
CREATE TABLE sql_tests_d06.t_agg_count_if_grouped (
    k INT,
    v INT
);

INSERT INTO sql_tests_d06.t_agg_count_if_grouped VALUES
    (1, 10),
    (2, 20),
    (3, NULL),
    (4, 30),
    (5, 15);

SELECT
    MOD(k, 2) AS g,
    count_if(v > 15) AS c_gt_15
FROM sql_tests_d06.t_agg_count_if_grouped
GROUP BY g
ORDER BY g;
