-- @order_sensitive=true
-- @tags=analytic,count,null
-- Test Objective:
-- 1. Validate COUNT(*) vs COUNT(expr) in window context.
-- 2. Prevent regressions in NULL-aware window counting.
-- Test Flow:
-- 1. Create/reset source table.
-- 2. Insert deterministic rows with NULL values.
-- 3. Compute window counts and assert ordered output.
CREATE DATABASE IF NOT EXISTS sql_tests_d07;
DROP TABLE IF EXISTS sql_tests_d07.t_analytic_count_window_nulls;
CREATE TABLE sql_tests_d07.t_analytic_count_window_nulls (
    grp VARCHAR(10),
    ts INT,
    v INT
);

INSERT INTO sql_tests_d07.t_analytic_count_window_nulls VALUES
    ('A', 1, 10),
    ('A', 2, NULL),
    ('A', 3, 30),
    ('B', 1, NULL);

SELECT
    grp,
    ts,
    v,
    COUNT(*) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cnt_all,
    COUNT(v) OVER (PARTITION BY grp ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cnt_v
FROM sql_tests_d07.t_analytic_count_window_nulls
ORDER BY grp, ts;
