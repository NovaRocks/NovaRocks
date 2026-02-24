-- @order_sensitive=true
-- @tags=aggregate,group_concat,distinct,null
-- Test Objective:
-- 1. Validate group_concat DISTINCT+ORDER output when one group has only NULL values.
-- 2. Prevent regressions where all-NULL groups return empty string instead of NULL.
-- Test Flow:
-- 1. Create/reset grouped source table.
-- 2. Insert deterministic rows with one all-NULL group and one mixed group.
-- 3. Group and assert ordered group_concat outputs.
CREATE DATABASE IF NOT EXISTS sql_tests_d06;
DROP TABLE IF EXISTS sql_tests_d06.t_agg_group_concat_distinct_all_null_group;
CREATE TABLE sql_tests_d06.t_agg_group_concat_distinct_all_null_group (
    g INT,
    s STRING
);

INSERT INTO sql_tests_d06.t_agg_group_concat_distinct_all_null_group VALUES
    (1, NULL),
    (1, NULL),
    (2, 'a'),
    (2, 'c'),
    (2, 'a'),
    (2, NULL),
    (2, 'b');

SELECT
    g,
    group_concat(DISTINCT s ORDER BY s DESC SEPARATOR '|') AS gc
FROM sql_tests_d06.t_agg_group_concat_distinct_all_null_group
GROUP BY g
ORDER BY g;
