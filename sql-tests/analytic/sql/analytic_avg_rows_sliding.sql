-- @order_sensitive=true
-- @tags=analytic,avg,rows_frame
-- Test Objective:
-- 1. Validate AVG over sliding ROWS frame.
-- 2. Prevent regressions in bounded frame aggregation.
-- Test Flow:
-- 1. Create/reset source table.
-- 2. Insert deterministic ordered rows.
-- 3. Compute AVG with 1-preceding/1-following frame and assert output.
CREATE DATABASE IF NOT EXISTS sql_tests_d07;
DROP TABLE IF EXISTS sql_tests_d07.t_analytic_avg_rows_sliding;
CREATE TABLE sql_tests_d07.t_analytic_avg_rows_sliding (
    grp VARCHAR(10),
    ts INT,
    v INT
);

INSERT INTO sql_tests_d07.t_analytic_avg_rows_sliding VALUES
    ('A', 1, 10),
    ('A', 2, 20),
    ('A', 3, 40),
    ('A', 4, 80);

SELECT
    grp,
    ts,
    v,
    AVG(v) OVER (
        PARTITION BY grp ORDER BY ts
        ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING
    ) AS avg_win
FROM sql_tests_d07.t_analytic_avg_rows_sliding
ORDER BY grp, ts;
