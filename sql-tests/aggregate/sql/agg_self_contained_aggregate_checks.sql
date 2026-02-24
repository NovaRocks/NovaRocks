-- @order_sensitive=true
-- @tags=aggregate,regression,self_contained
-- Test Objective:
-- 1. Validate mixed aggregate metrics over customer and lineorder-like data.
-- 2. Prevent regressions where this case depends on preloaded SSB tables.
-- Test Flow:
-- 1. Create/reset minimal source tables for customer and lineorder metrics.
-- 2. Insert deterministic rows that include both filter-hit and filter-miss branches.
-- 3. Aggregate metrics and order by metric name for stable comparison.
CREATE DATABASE IF NOT EXISTS sql_tests_d06;
DROP TABLE IF EXISTS sql_tests_d06.t_agg_regression_customer_metrics;
DROP TABLE IF EXISTS sql_tests_d06.t_agg_regression_lineorder_metrics;
CREATE TABLE sql_tests_d06.t_agg_regression_customer_metrics (
    c_custkey INT,
    c_region VARCHAR(32)
);
CREATE TABLE sql_tests_d06.t_agg_regression_lineorder_metrics (
    lo_revenue BIGINT,
    lo_quantity INT,
    lo_discount INT
);

INSERT INTO sql_tests_d06.t_agg_regression_customer_metrics VALUES
    (1, 'ASIA'),
    (2, 'AMERICA'),
    (3, 'EUROPE'),
    (4, 'AFRICA'),
    (5, 'MIDDLE EAST'),
    (6, 'ASIA');

INSERT INTO sql_tests_d06.t_agg_regression_lineorder_metrics VALUES
    (100, 10, 1),
    (200, 24, 3),
    (50, 25, 2),
    (400, 5, 4),
    (300, 20, 2),
    (10, 1, 0);

SELECT metric, value
FROM (
    SELECT 'customer_count' AS metric, CAST(COUNT(*) AS BIGINT) AS value
    FROM sql_tests_d06.t_agg_regression_customer_metrics
    UNION ALL
    SELECT 'lineorder_filtered_sum_revenue', CAST(SUM(lo_revenue) AS BIGINT)
    FROM sql_tests_d06.t_agg_regression_lineorder_metrics
    WHERE lo_quantity < 25
      AND lo_discount BETWEEN 1 AND 3
    UNION ALL
    SELECT 'lineorder_sum_qty', CAST(SUM(lo_quantity) AS BIGINT)
    FROM sql_tests_d06.t_agg_regression_lineorder_metrics
) t
ORDER BY metric;
