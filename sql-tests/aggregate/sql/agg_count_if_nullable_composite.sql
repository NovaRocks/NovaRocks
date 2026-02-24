-- @order_sensitive=true
-- @tags=aggregate,count_if,null,composite
-- Test Objective:
-- 1. Validate count_if over nullable composite boolean expressions.
-- 2. Prevent regressions where NULL predicate values are counted as TRUE.
-- Test Flow:
-- 1. Create/reset source table with nullable inputs.
-- 2. Insert rows that yield TRUE/FALSE/NULL predicate outcomes.
-- 3. Aggregate by group and assert deterministic count_if outputs.
CREATE DATABASE IF NOT EXISTS sql_tests_d06;
DROP TABLE IF EXISTS sql_tests_d06.t_agg_count_if_nullable_composite;
CREATE TABLE sql_tests_d06.t_agg_count_if_nullable_composite (
    g INT,
    x INT,
    y INT
);

INSERT INTO sql_tests_d06.t_agg_count_if_nullable_composite VALUES
    (1, 3, 1),
    (1, 3, 0),
    (1, NULL, 2),
    (1, 5, NULL),
    (2, 2, 1),
    (2, 1, 5),
    (2, NULL, NULL);

SELECT
    g,
    count_if(x > y AND y > 0) AS cnt_gt,
    count_if(x IS NULL OR y IS NULL) AS cnt_has_null
FROM sql_tests_d06.t_agg_count_if_nullable_composite
GROUP BY g
ORDER BY g;
