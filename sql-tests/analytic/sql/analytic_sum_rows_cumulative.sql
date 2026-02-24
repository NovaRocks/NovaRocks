-- @order_sensitive=true
-- @tags=analytic,sum,rows_frame
-- Test Objective:
-- 1. Validate cumulative SUM over ROWS frame.
-- 2. Prevent regressions in running-aggregate window updates.
-- Test Flow:
-- 1. Create/reset source table.
-- 2. Insert deterministic rows per partition.
-- 3. Compute cumulative SUM and assert ordered output.
CREATE DATABASE IF NOT EXISTS sql_tests_d07;
DROP TABLE IF EXISTS sql_tests_d07.t_analytic_sum_rows_cumulative;
CREATE TABLE sql_tests_d07.t_analytic_sum_rows_cumulative (
    grp VARCHAR(10),
    ts INT,
    v INT
);

INSERT INTO sql_tests_d07.t_analytic_sum_rows_cumulative VALUES
    ('A', 1, 2),
    ('A', 2, 3),
    ('A', 3, 5),
    ('B', 1, 4),
    ('B', 2, 6);

SELECT
    grp,
    ts,
    v,
    SUM(v) OVER (
        PARTITION BY grp ORDER BY ts
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS running_sum
FROM sql_tests_d07.t_analytic_sum_rows_cumulative
ORDER BY grp, ts;
