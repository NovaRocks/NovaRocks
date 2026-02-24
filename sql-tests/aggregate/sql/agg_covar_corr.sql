-- @order_sensitive=true
-- @tags=aggregate,covar,corr
-- Test Objective:
-- 1. Validate covariance/correlation aggregates.
-- 2. Prevent regressions in pairwise statistical aggregations.
-- Test Flow:
-- 1. Create/reset source table.
-- 2. Insert deterministic (x,y) pairs by group.
-- 3. Compute rounded covar/corr metrics and assert ordered output.
CREATE DATABASE IF NOT EXISTS sql_tests_d06;
DROP TABLE IF EXISTS sql_tests_d06.t_agg_covar_corr;
CREATE TABLE sql_tests_d06.t_agg_covar_corr (
    g INT,
    x INT,
    y INT
);

INSERT INTO sql_tests_d06.t_agg_covar_corr VALUES
    (1, 1, 2),
    (1, 2, 4),
    (1, 3, 6),
    (2, 1, 9),
    (2, 2, 7),
    (2, 3, 5);

SELECT
    g,
    ROUND(COVAR_POP(x, y), 6) AS cov_pop,
    ROUND(COVAR_SAMP(x, y), 6) AS cov_samp,
    ROUND(CORR(x, y), 6) AS corr_xy
FROM sql_tests_d06.t_agg_covar_corr
GROUP BY g
ORDER BY g;
