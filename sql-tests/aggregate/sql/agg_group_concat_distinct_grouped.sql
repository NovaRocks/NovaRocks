-- @order_sensitive=true
-- @tags=aggregate,group_concat
-- Test Objective:
-- 1. Validate DISTINCT + ORDER BY group_concat behavior per group.
-- 2. Prevent regressions in group_concat merge semantics for distributed aggregation.
-- Test Flow:
-- 1. Create/reset grouped source table.
-- 2. Insert deterministic rows including duplicates and NULL output values.
-- 3. Group by key and assert ordered group_concat outputs.
CREATE DATABASE IF NOT EXISTS sql_tests_d06;
DROP TABLE IF EXISTS sql_tests_d06.t_agg_group_concat_distinct_grouped;
CREATE TABLE sql_tests_d06.t_agg_group_concat_distinct_grouped (
    k INT,
    s STRING
);

INSERT INTO sql_tests_d06.t_agg_group_concat_distinct_grouped VALUES
    (1, 'b'),
    (2, 'a'),
    (3, 'b'),
    (4, NULL),
    (5, 'c');

SELECT
    MOD(k, 2) AS g,
    group_concat(DISTINCT s ORDER BY s SEPARATOR ',') AS gc
FROM sql_tests_d06.t_agg_group_concat_distinct_grouped
GROUP BY g
ORDER BY g;
