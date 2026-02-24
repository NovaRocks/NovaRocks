-- @order_sensitive=true
-- @tags=analytic,row_number,filter
-- Test Objective:
-- 1. Validate filtering over window outputs via subquery.
-- 2. Prevent regressions in window + outer predicate integration.
-- Test Flow:
-- 1. Create/reset source table.
-- 2. Insert deterministic rows per partition.
-- 3. Apply ROW_NUMBER in subquery and keep top-2 rows per partition.
CREATE DATABASE IF NOT EXISTS sql_tests_d07;
DROP TABLE IF EXISTS sql_tests_d07.t_analytic_filter_topn_with_window;
CREATE TABLE sql_tests_d07.t_analytic_filter_topn_with_window (
    grp VARCHAR(10),
    id INT,
    score INT
);

INSERT INTO sql_tests_d07.t_analytic_filter_topn_with_window VALUES
    ('A', 1, 70),
    ('A', 2, 90),
    ('A', 3, 80),
    ('B', 4, 60),
    ('B', 5, 50),
    ('B', 6, 40);

SELECT grp, id, score, rn
FROM (
    SELECT
        grp,
        id,
        score,
        ROW_NUMBER() OVER (PARTITION BY grp ORDER BY score DESC, id) AS rn
    FROM sql_tests_d07.t_analytic_filter_topn_with_window
) t
WHERE rn <= 2
ORDER BY grp, rn;
