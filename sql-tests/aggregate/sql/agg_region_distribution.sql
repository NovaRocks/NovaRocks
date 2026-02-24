-- @order_sensitive=true
-- @tags=aggregate,group_by,self_contained
-- Test Objective:
-- 1. Validate GROUP BY region distribution counting behavior.
-- 2. Prevent regressions where this case assumes pre-existing SSB customer data.
-- Test Flow:
-- 1. Create/reset a minimal customer-like table.
-- 2. Insert deterministic rows spanning all expected regions.
-- 3. Aggregate by region and order output for stable comparison.
CREATE DATABASE IF NOT EXISTS sql_tests_d06;
DROP TABLE IF EXISTS sql_tests_d06.t_agg_region_distribution;
CREATE TABLE sql_tests_d06.t_agg_region_distribution (
    c_custkey INT,
    c_region VARCHAR(32)
);

INSERT INTO sql_tests_d06.t_agg_region_distribution VALUES
    (1, 'AFRICA'),
    (2, 'ASIA'),
    (3, 'ASIA'),
    (4, 'EUROPE'),
    (5, 'AMERICA'),
    (6, 'MIDDLE EAST'),
    (7, 'AMERICA'),
    (8, 'AFRICA');

SELECT c_region, COUNT(*) AS customer_count
FROM sql_tests_d06.t_agg_region_distribution
GROUP BY c_region
ORDER BY c_region;
