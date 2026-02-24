-- @order_sensitive=true
-- @tags=aggregate,bool_or
-- Test Objective:
-- 1. Validate BOOL_OR aggregation over grouped predicates.
-- 2. Prevent regressions in boolean aggregation with NULL rows.
-- Test Flow:
-- 1. Create/reset source table.
-- 2. Insert deterministic rows including NULL.
-- 3. Aggregate predicate truth values by group and assert output.
CREATE DATABASE IF NOT EXISTS sql_tests_d06;
DROP TABLE IF EXISTS sql_tests_d06.t_agg_bool_or_grouped;
CREATE TABLE sql_tests_d06.t_agg_bool_or_grouped (
    g INT,
    v INT
);

INSERT INTO sql_tests_d06.t_agg_bool_or_grouped VALUES
    (1, 10),
    (1, 20),
    (2, NULL),
    (2, 5),
    (3, NULL);

SELECT
    g,
    BOOL_OR(v > 15) AS has_gt_15,
    BOOL_OR(v IS NULL) AS has_null
FROM sql_tests_d06.t_agg_bool_or_grouped
GROUP BY g
ORDER BY g;
