-- @order_sensitive=true
-- @tags=analytic,ntile,percent_rank,cume_dist
-- Test Objective:
-- 1. Validate NTILE/PERCENT_RANK/CUME_DIST outputs.
-- 2. Prevent regressions in distribution-based window functions.
-- Test Flow:
-- 1. Create/reset source table.
-- 2. Insert deterministic ordered values.
-- 3. Compute distribution windows and assert ordered output.
CREATE DATABASE IF NOT EXISTS sql_tests_d07;
DROP TABLE IF EXISTS sql_tests_d07.t_analytic_ntile_percentile;
CREATE TABLE sql_tests_d07.t_analytic_ntile_percentile (
    grp VARCHAR(10),
    v INT
);

INSERT INTO sql_tests_d07.t_analytic_ntile_percentile VALUES
    ('A', 10),
    ('A', 20),
    ('A', 30),
    ('A', 40);

SELECT
    grp,
    v,
    NTILE(2) OVER (PARTITION BY grp ORDER BY v) AS nt,
    ROUND(PERCENT_RANK() OVER (PARTITION BY grp ORDER BY v), 6) AS pr,
    ROUND(CUME_DIST() OVER (PARTITION BY grp ORDER BY v), 6) AS cd
FROM sql_tests_d07.t_analytic_ntile_percentile
ORDER BY grp, v;
