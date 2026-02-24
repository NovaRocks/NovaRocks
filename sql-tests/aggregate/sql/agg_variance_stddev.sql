-- @order_sensitive=true
-- @tags=aggregate,variance,stddev
-- Test Objective:
-- 1. Validate variance/stddev family aggregates on grouped data.
-- 2. Prevent regressions in statistical aggregate formulas.
-- Test Flow:
-- 1. Create/reset source table.
-- 2. Insert deterministic numeric rows for two groups.
-- 3. Compute rounded variance/stddev metrics and assert ordered output.
CREATE DATABASE IF NOT EXISTS sql_tests_d06;
DROP TABLE IF EXISTS sql_tests_d06.t_agg_variance_stddev;
CREATE TABLE sql_tests_d06.t_agg_variance_stddev (
    g INT,
    v INT
);

INSERT INTO sql_tests_d06.t_agg_variance_stddev VALUES
    (1, 10),
    (1, 20),
    (1, 30),
    (2, 3),
    (2, 7),
    (2, 11);

SELECT
    g,
    ROUND(VAR_POP(v), 6) AS var_pop_v,
    ROUND(VAR_SAMP(v), 6) AS var_samp_v,
    ROUND(STDDEV_POP(v), 6) AS std_pop_v,
    ROUND(STDDEV_SAMP(v), 6) AS std_samp_v
FROM sql_tests_d06.t_agg_variance_stddev
GROUP BY g
ORDER BY g;
