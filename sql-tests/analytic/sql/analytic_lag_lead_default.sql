-- @order_sensitive=true
-- @tags=analytic,lag,lead
-- Test Objective:
-- 1. Validate LAG/LEAD default-value behavior.
-- 2. Prevent regressions in offset window navigation.
-- Test Flow:
-- 1. Create/reset source table.
-- 2. Insert deterministic ordered rows.
-- 3. Compute LAG/LEAD and assert ordered output.
CREATE DATABASE IF NOT EXISTS sql_tests_d07;
DROP TABLE IF EXISTS sql_tests_d07.t_analytic_lag_lead_default;
CREATE TABLE sql_tests_d07.t_analytic_lag_lead_default (
    grp VARCHAR(10),
    ts INT,
    v INT
);

INSERT INTO sql_tests_d07.t_analytic_lag_lead_default VALUES
    ('A', 1, 10),
    ('A', 2, 20),
    ('A', 3, 30),
    ('B', 1, 7),
    ('B', 2, NULL);

SELECT
    grp,
    ts,
    v,
    LAG(v, 1, -1) OVER (PARTITION BY grp ORDER BY ts) AS prev_v,
    LEAD(v, 1, -1) OVER (PARTITION BY grp ORDER BY ts) AS next_v
FROM sql_tests_d07.t_analytic_lag_lead_default
ORDER BY grp, ts;
