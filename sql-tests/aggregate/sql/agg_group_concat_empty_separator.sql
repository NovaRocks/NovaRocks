-- @order_sensitive=true
-- @tags=aggregate,group_concat,separator
-- Test Objective:
-- 1. Validate group_concat ORDER BY behavior with empty-string separator.
-- 2. Prevent regressions where empty separator falls back to default comma.
-- Test Flow:
-- 1. Create/reset grouped source table.
-- 2. Insert deterministic ordered rows including NULL values.
-- 3. Group and assert separator-sensitive concatenation outputs.
CREATE DATABASE IF NOT EXISTS sql_tests_d06;
DROP TABLE IF EXISTS sql_tests_d06.t_agg_group_concat_empty_separator;
CREATE TABLE sql_tests_d06.t_agg_group_concat_empty_separator (
    g INT,
    ord INT,
    s STRING
);

INSERT INTO sql_tests_d06.t_agg_group_concat_empty_separator VALUES
    (1, 1, 'a'),
    (1, 2, 'b'),
    (1, 3, NULL),
    (1, 4, 'c'),
    (2, 1, NULL),
    (2, 2, 'x'),
    (2, 3, 'y');

SELECT
    g,
    group_concat(s ORDER BY ord SEPARATOR '') AS gc
FROM sql_tests_d06.t_agg_group_concat_empty_separator
GROUP BY g
ORDER BY g;
