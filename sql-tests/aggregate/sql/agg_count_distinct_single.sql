-- @order_sensitive=true
-- @tags=aggregate,count_distinct
-- Test Objective:
-- 1. Validate COUNT(DISTINCT) on grouped string keys.
-- 2. Prevent regressions in distinct-state aggregation across groups.
-- Test Flow:
-- 1. Create/reset source table.
-- 2. Insert duplicate and NULL-contained rows.
-- 3. Compute grouped COUNT(DISTINCT) and assert deterministic order.
CREATE DATABASE IF NOT EXISTS sql_tests_d06;
DROP TABLE IF EXISTS sql_tests_d06.t_agg_count_distinct_single;
CREATE TABLE sql_tests_d06.t_agg_count_distinct_single (
    g INT,
    s VARCHAR(20)
);

INSERT INTO sql_tests_d06.t_agg_count_distinct_single VALUES
    (1, 'a'),
    (1, 'a'),
    (1, 'b'),
    (2, 'a'),
    (2, NULL),
    (2, 'c');

SELECT
    g,
    COUNT(DISTINCT CAST(s AS VARCHAR)) AS cd_s
FROM sql_tests_d06.t_agg_count_distinct_single
GROUP BY g
ORDER BY g;
