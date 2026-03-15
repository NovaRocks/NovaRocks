-- @order_sensitive=true
-- @tags=aggregate,statistics,null,window
-- Test Objective:
-- 1. Validate corr/covar and variance/stddev families on empty, nullable, and windowed inputs.
-- 2. Preserve legacy null/phase-sensitive semantics from test_statistic and test_always_null_statistic_funcs.
-- Test Flow:
-- 1. Create/reset a nullable statistics source table.
-- 2. Insert deterministic rows with one null-only group and one populated group.
-- 3. Snapshot scalar, staged, and windowed statistics outputs.
-- query 1
DROP TABLE IF EXISTS ${case_db}.t_agg_statistic_null_window;
CREATE TABLE ${case_db}.t_agg_statistic_null_window (
    no INT,
    k DECIMAL(10, 2),
    v DECIMAL(10, 2)
);

INSERT INTO ${case_db}.t_agg_statistic_null_window VALUES
    (1, 10, NULL),
    (2, 10, 11),
    (2, 20, 22),
    (2, 25, NULL),
    (2, 30, 35);
SELECT
    corr(k, v) IS NULL AS corr_empty_is_null,
    covar_samp(k, v) IS NULL AS covar_samp_empty_is_null,
    covar_pop(k, v) IS NULL AS covar_pop_empty_is_null
FROM ${case_db}.t_agg_statistic_null_window
WHERE no = 999;

-- query 2
SELECT
    ABS(corr(k, v) - 0.9988445981121532) / 0.9988445981121532 < 0.00001 AS corr_match,
    ABS(covar_samp(k, v) - 120) / 120 < 0.00001 AS covar_samp_match,
    ABS(covar_pop(k, v) - 80) / 80 < 0.00001 AS covar_pop_match
FROM ${case_db}.t_agg_statistic_null_window
WHERE no = 2;

-- query 3
SELECT
    ABS(co - 0.9988445981121532) / 0.9988445981121532 < 0.00001 AS corr_match,
    total = 4 AS pair_count_match
FROM (
    SELECT
        /*+ SET_VAR (new_planner_agg_stage='3') */
        corr(k, v) AS co,
        COUNT(DISTINCT k) AS total
    FROM ${case_db}.t_agg_statistic_null_window
    WHERE no = 2
) t;

-- query 4
SELECT
    ROUND(var_samp(x), 3) AS var_samp_v,
    ROUND(variance_samp(x), 3) AS variance_samp_v,
    ROUND(stddev_samp(x), 3) AS stddev_samp_v
FROM (
    SELECT 1 AS x
    UNION ALL
    SELECT 2
    UNION ALL
    SELECT 3
) t;

-- query 5
SELECT
    ROUND(var_samp(x), 3) IS NULL AS var_samp_is_null,
    ROUND(variance_samp(x), 3) IS NULL AS variance_samp_is_null,
    ROUND(stddev_samp(x), 3) IS NULL AS stddev_samp_is_null
FROM (
    SELECT 1 AS x
) t;

-- query 6
SELECT
    no,
    k,
    ROUND(covar_samp(k, v) OVER (
        PARTITION BY no
        ORDER BY k
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ), 3) AS covar_samp_window,
    ROUND(var_samp(k) OVER (
        PARTITION BY no
        ORDER BY k
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ), 3) AS var_samp_window
FROM ${case_db}.t_agg_statistic_null_window
ORDER BY no, k;
