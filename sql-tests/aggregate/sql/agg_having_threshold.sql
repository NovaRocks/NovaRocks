-- @order_sensitive=true
-- @tags=aggregate,having
-- Test Objective:
-- 1. Validate HAVING filtering on aggregate outputs.
-- 2. Prevent regressions in post-aggregation predicate evaluation.
-- Test Flow:
-- 1. Create/reset source table.
-- 2. Insert deterministic rows across groups.
-- 3. Apply GROUP BY + HAVING and assert ordered output.
CREATE DATABASE IF NOT EXISTS sql_tests_d06;
DROP TABLE IF EXISTS sql_tests_d06.t_agg_having_threshold;
CREATE TABLE sql_tests_d06.t_agg_having_threshold (
    g INT,
    v INT
);

INSERT INTO sql_tests_d06.t_agg_having_threshold VALUES
    (1, 5),
    (1, 7),
    (2, 9),
    (2, 15),
    (3, 30);

SELECT
    g,
    SUM(v) AS s_v
FROM sql_tests_d06.t_agg_having_threshold
GROUP BY g
HAVING SUM(v) >= 20
ORDER BY g;
